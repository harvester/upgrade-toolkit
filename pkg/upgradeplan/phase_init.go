package upgradeplan

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	harvesterv1beta1 "github.com/harvester/harvester/pkg/apis/harvesterhci.io/v1beta1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	managementv1beta1 "github.com/harvester/upgrade-toolkit/api/v1beta1"
)

const (
	// defaultNewImageSize is the estimated size of new images loaded during
	// upgrade, used for disk space projection. 13GB aggregates all tarball
	// image sizes. It may change in the future.
	defaultNewImageSize uint64 = 13 * 1024 * 1024 * 1024

	// defaultImageGCHighThresholdPercent is the default kubelet image GC
	// high threshold percentage.
	defaultImageGCHighThresholdPercent float64 = 85.0

	// defaultMinCertsExpirationInDay is the default minimum number of days
	// before certificate expiration to allow upgrades.
	defaultMinCertsExpirationInDay = 7

	// kubernetesAPIPort is the standard Kubernetes API server port.
	kubernetesAPIPort = "6443"

	// serviceAccountTokenPath is the standard path to the service account
	// token in a Kubernetes pod.
	serviceAccountTokenPath = "/var/run/secrets/kubernetes.io/serviceaccount/token"
)

// kubeletStatsSummary mirrors essential fields from the kubelet /stats/summary
// response (k8s.io/kubelet/pkg/apis/stats/v1alpha1.Summary).
type kubeletStatsSummary struct {
	Node kubeletNodeStats `json:"node"`
}

type kubeletNodeStats struct {
	Fs *kubeletFsStats `json:"fs"`
}

type kubeletFsStats struct {
	AvailableBytes *uint64 `json:"availableBytes"`
	CapacityBytes  *uint64 `json:"capacityBytes"`
	UsedBytes      *uint64 `json:"usedBytes"`
}

// kubeletConfigzResponse mirrors the /configz endpoint wrapper.
// The kubelet wraps its configuration in {"kubeletconfig": {...}}.
type kubeletConfigzResponse struct {
	KubeletConfig kubeletConfigPartial `json:"kubeletconfig"`
}

// kubeletConfigPartial contains only the fields we need from
// KubeletConfiguration.
type kubeletConfigPartial struct {
	ImageGCHighThresholdPercent *int32 `json:"imageGCHighThresholdPercent,omitempty"`
}

// InitPhase sets up the UpgradePlan resource's conditions and essential status fields.
// It implements Runnable and PostRunnable (for upgrade pre-flight checks).
type InitPhase struct {
	*PhaseDeps

	// httpClient is used for kubelet API calls (TLS-insecure in production).
	httpClient *http.Client
	// bearerToken authenticates requests to the kubelet API.
	bearerToken string
	// certDialer retrieves the earliest expiring TLS certificate from an
	// address. Injectable for testing.
	certDialer func(addr string) (*x509.Certificate, error)
}

func NewInitPhase(deps *PhaseDeps) *InitPhase {
	return &InitPhase{
		PhaseDeps:   deps,
		httpClient:  defaultHTTPClient(),
		bearerToken: readServiceAccountToken(),
		certDialer:  defaultCertDialer,
	}
}

func (p *InitPhase) Name() string { return "Initialize" }

func (p *InitPhase) Run(
	ctx context.Context,
	upgradePlan *managementv1beta1.UpgradePlan,
) (ctrl.Result, error) {
	p.Log.V(1).Info("handle initialize status")

	if upgradePlan.Status.CurrentPhase == managementv1beta1.UpgradePlanPhaseInitializing {
		upgradePlan.SetCondition(managementv1beta1.UpgradePlanAvailable, metav1.ConditionTrue, "Executable", "")

		if err := p.loadVersion(ctx, upgradePlan); err != nil {
			return ctrl.Result{}, err
		}

		if err := p.loadPreviousVersion(ctx, upgradePlan); err != nil {
			return ctrl.Result{}, err
		}

		if err := p.detectSingleNode(ctx, upgradePlan); err != nil {
			return ctrl.Result{}, err
		}

		updateProgressingPhase(upgradePlan, managementv1beta1.UpgradePlanPhaseInitialized, "")
		return ctrl.Result{}, nil
	}

	updateProgressingPhase(upgradePlan, managementv1beta1.UpgradePlanPhaseInitializing, "")
	return ctrl.Result{}, nil
}

// PostRun performs upgrade pre-flight checks after initialization completes.
// Pre-flight check failures are terminal: insufficient disk space or expiring
// certificates will not resolve by retrying, so the plan transitions to
// Failed immediately.
func (p *InitPhase) PostRun(
	ctx context.Context,
	upgradePlan *managementv1beta1.UpgradePlan,
) error {
	if err := p.checkDiskSpace(ctx, upgradePlan); err != nil {
		p.Log.Error(err, "disk space pre-flight check failed")
		updateProgressingPhase(upgradePlan, managementv1beta1.UpgradePlanPhaseFailed, err.Error())
		return nil
	}
	if err := p.checkCerts(ctx, upgradePlan); err != nil {
		p.Log.Error(err, "certificate pre-flight check failed")
		updateProgressingPhase(upgradePlan, managementv1beta1.UpgradePlanPhaseFailed, err.Error())
		return nil
	}
	return nil
}

// checkDiskSpace verifies that each node has sufficient disk space to load new
// upgrade images without exceeding the kubelet image GC threshold.
func (p *InitPhase) checkDiskSpace(
	ctx context.Context,
	upgradePlan *managementv1beta1.UpgradePlan,
) error {
	if v, ok := upgradePlan.Annotations[AnnotationSkipGCThresholdCheck]; ok {
		skip, err := strconv.ParseBool(v)
		if err != nil {
			return fmt.Errorf("invalid value %q for annotation %s", v, AnnotationSkipGCThresholdCheck)
		}
		if skip {
			p.Log.Info("skipping disk space check per annotation")
			return nil
		}
	}

	var nodeList corev1.NodeList
	if err := p.Client.List(ctx, &nodeList); err != nil {
		return fmt.Errorf("failed to list nodes: %w", err)
	}

	for i := range nodeList.Items {
		if err := p.checkNodeDiskSpace(&nodeList.Items[i]); err != nil {
			return err
		}
	}
	return nil
}

// checkNodeDiskSpace projects the disk usage of a single node after loading new
// images and compares it against the kubelet's imageGCHighThresholdPercent.
func (p *InitPhase) checkNodeDiskSpace(node *corev1.Node) error {
	internalIP, ok := node.Annotations[RKE2InternalIPAnnotation]
	if !ok {
		return fmt.Errorf("node %s doesn't have %s annotation", node.Name, RKE2InternalIPAnnotation)
	}

	kubeletPort := node.Status.DaemonEndpoints.KubeletEndpoint.Port
	kubeletURL := fmt.Sprintf("https://%s:%d", internalIP, kubeletPort)

	summary, err := p.getKubeletStatsSummary(node.Name, kubeletURL)
	if err != nil {
		return err
	}

	if summary.Node.Fs == nil || summary.Node.Fs.AvailableBytes == nil ||
		summary.Node.Fs.CapacityBytes == nil || summary.Node.Fs.UsedBytes == nil {
		return fmt.Errorf("can't get node %s filesystem stats from %s", node.Name, kubeletURL)
	}

	configz, err := p.getKubeletConfigz(node.Name, kubeletURL)
	if err != nil {
		return err
	}

	imageGCHighThresholdPercent := defaultImageGCHighThresholdPercent
	if configz.ImageGCHighThresholdPercent != nil {
		imageGCHighThresholdPercent = float64(*configz.ImageGCHighThresholdPercent)
	}

	usedPercent := (float64(*summary.Node.Fs.UsedBytes+defaultNewImageSize) /
		float64(*summary.Node.Fs.CapacityBytes)) * 100.0

	if usedPercent > imageGCHighThresholdPercent {
		return fmt.Errorf("node %q will reach %.2f%% storage space after loading new images, "+
			"higher than kubelet image garbage collection threshold %s%%",
			node.Name, usedPercent, strconv.FormatFloat(imageGCHighThresholdPercent, 'f', -1, 64))
	}

	return nil
}

func (p *InitPhase) getKubeletStatsSummary(nodeName, kubeletURL string) (*kubeletStatsSummary, error) {
	url := kubeletURL + "/stats/summary"
	body, err := p.doKubeletRequest(nodeName, url)
	if err != nil {
		return nil, err
	}

	var summary kubeletStatsSummary
	if err := json.Unmarshal(body, &summary); err != nil {
		return nil, fmt.Errorf("node %s, can't parse json response from %s: %w", nodeName, url, err)
	}
	return &summary, nil
}

func (p *InitPhase) getKubeletConfigz(nodeName, kubeletURL string) (*kubeletConfigPartial, error) {
	url := kubeletURL + "/configz"
	body, err := p.doKubeletRequest(nodeName, url)
	if err != nil {
		return nil, err
	}

	var resp kubeletConfigzResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("node %s, can't parse json response from %s: %w", nodeName, url, err)
	}
	return &resp.KubeletConfig, nil
}

func (p *InitPhase) doKubeletRequest(nodeName, url string) ([]byte, error) {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("node %s, can't create HTTP request for %s: %w", nodeName, url, err)
	}
	if p.bearerToken != "" {
		req.Header.Set("Authorization", "Bearer "+p.bearerToken)
	}

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("node %s, can't make HTTP request to %s: %w", nodeName, url, err)
	}
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("node %s, HTTP response from %s is %d", nodeName, url, resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("node %s, can't read response from %s: %w", nodeName, url, err)
	}
	return body, nil
}

// checkCerts verifies that the Kubernetes API server TLS certificates will not
// expire within the configured threshold.
func (p *InitPhase) checkCerts(
	ctx context.Context,
	upgradePlan *managementv1beta1.UpgradePlan,
) error {
	var epsList discoveryv1.EndpointSliceList
	if err := p.Client.List(ctx, &epsList, &client.ListOptions{
		Namespace: metav1.NamespaceDefault,
		LabelSelector: labels.SelectorFromSet(labels.Set{
			serviceNameLabel: "kubernetes",
		}),
	}); err != nil {
		return fmt.Errorf("can't list kubernetes endpointslices: %w", err)
	}

	var kubernetesIPs []string
	for _, eps := range epsList.Items {
		for _, ep := range eps.Endpoints {
			for _, addr := range ep.Addresses {
				kubernetesIPs = append(kubernetesIPs, addr+":"+kubernetesAPIPort)
			}
		}
	}
	if len(kubernetesIPs) == 0 {
		return fmt.Errorf("cluster IP is empty in the default/kubernetes endpointslices")
	}

	earliestExpiringCert := p.getAddrsEarliestExpiringCert(kubernetesIPs)
	if earliestExpiringCert == nil {
		return fmt.Errorf("no certificates found for kubernetes API server")
	}

	minCertsExpirationInDay := defaultMinCertsExpirationInDay
	if value, ok := upgradePlan.Annotations[AnnotationMinCertsExpirationInDay]; ok {
		var err error
		minCertsExpirationInDay, err = strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("invalid value %q for annotation %s", value, AnnotationMinCertsExpirationInDay)
		}
		if minCertsExpirationInDay <= 0 {
			return fmt.Errorf(
				"invalid value %q for annotation %s, must be greater than 0",
				value, AnnotationMinCertsExpirationInDay)
		}
	}

	expirationDate := time.Now().AddDate(0, 0, minCertsExpirationInDay)
	if earliestExpiringCert.NotAfter.Before(expirationDate) {
		return fmt.Errorf(
			"earliest expiring cert for default/kubernetes ClusterIP is %s, "+
				"it will expire in %d days. Please rotate RKE2 certificates",
			earliestExpiringCert.NotAfter, minCertsExpirationInDay)
	}

	return nil
}

func (p *InitPhase) getAddrsEarliestExpiringCert(addrs []string) *x509.Certificate {
	var earliestExpiringCert *x509.Certificate
	for _, addr := range addrs {
		cert, err := p.certDialer(addr)
		if err != nil {
			p.Log.V(1).Info("failed to get cert from address, continuing", "addr", addr, "error", err)
			continue
		}
		if cert != nil && (earliestExpiringCert == nil || earliestExpiringCert.NotAfter.After(cert.NotAfter)) {
			earliestExpiringCert = cert
		}
	}
	return earliestExpiringCert
}

func defaultCertDialer(addr string) (*x509.Certificate, error) {
	conn, err := tls.Dial("tcp", addr, &tls.Config{
		InsecureSkipVerify: true, //nolint:gosec
	})
	if err != nil {
		return nil, fmt.Errorf("TLS dial to %s failed: %w", addr, err)
	}
	defer conn.Close() //nolint:errcheck

	var earliest *x509.Certificate
	for _, cert := range conn.ConnectionState().PeerCertificates {
		if earliest == nil || earliest.NotAfter.After(cert.NotAfter) {
			earliest = cert
		}
	}
	return earliest, nil
}

func defaultHTTPClient() *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true, //nolint:gosec
			},
		},
		Timeout: 30 * time.Second,
	}
}

func readServiceAccountToken() string {
	token, err := os.ReadFile(serviceAccountTokenPath)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(token))
}

func (p *InitPhase) loadVersion(
	ctx context.Context,
	upgradePlan *managementv1beta1.UpgradePlan,
) error {
	var version managementv1beta1.Version
	if err := p.Client.Get(ctx, types.NamespacedName{Name: upgradePlan.Spec.Version}, &version); err != nil {
		return err
	}
	upgradePlan.Status.Version = &version.Spec
	return nil
}

func (p *InitPhase) detectSingleNode(
	ctx context.Context,
	upgradePlan *managementv1beta1.UpgradePlan,
) error {
	nodes, err := listManagedNodes(ctx, p.Client)
	if err != nil {
		return err
	}

	if len(nodes) == 1 {
		upgradePlan.Status.SingleNode = ptr.To(nodes[0].Name)
	}

	return nil
}

func (p *InitPhase) loadPreviousVersion(
	ctx context.Context,
	upgradePlan *managementv1beta1.UpgradePlan,
) error {
	var setting harvesterv1beta1.Setting
	if err := p.Client.Get(ctx, types.NamespacedName{Name: serverVersionSettingName}, &setting); err != nil {
		return err
	}
	upgradePlan.Status.PreviousVersion = ptr.To(setting.Value)
	return nil
}
