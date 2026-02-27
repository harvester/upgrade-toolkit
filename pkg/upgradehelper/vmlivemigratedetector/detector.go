package vmlivemigratedetector

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/client-go/kubernetes"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"

	kubevirtv1 "kubevirt.io/api/core/v1"
	"kubevirt.io/client-go/kubecli"

	managementv1beta1 "github.com/harvester/upgrade-toolkit/api/v1beta1"
	"github.com/harvester/upgrade-toolkit/pkg/upgradehelper/vmi"
)

const (
	harvesterSystemNamespace = "harvester-system"
	restoreVMConfigMapPrefix = "restore-vm"
	upgradeLabel             = "harvesterhci.io/upgrade"

	RestoreVMConfigMapFailed  = "RestoreVMConfigMapFailed"
	RestoreVMConfigMapCreated = "RestoreVMConfigMapCreated"
	VMShutdownFailed          = "VMShutdownFailed"
	VMShutdownCompleted       = "VMShutdownCompleted"
)

// DetectorOptions holds the configuration for the VMLiveMigrateDetector.
type DetectorOptions struct {
	KubeConfigPath string
	KubeContext    string
	Shutdown       bool
	NodeName       string
	Upgrade        string
}

// VMLiveMigrateDetector detects non-live-migratable VMs on a node and optionally shuts them down.
type VMLiveMigrateDetector struct {
	kubeConfig  string
	kubeContext string

	nodeName    string
	shutdown    bool
	upgradeName string

	virtClient kubecli.KubevirtClient
	k8sClient  kubernetes.Interface
	recorder   record.EventRecorder
}

// NewVMLiveMigrateDetector creates a new VMLiveMigrateDetector.
func NewVMLiveMigrateDetector(options DetectorOptions) *VMLiveMigrateDetector {
	return &VMLiveMigrateDetector{
		kubeConfig:  options.KubeConfigPath,
		kubeContext: options.KubeContext,
		nodeName:    options.NodeName,
		shutdown:    options.Shutdown,
		upgradeName: options.Upgrade,
	}
}

// Init initializes the clients needed by the detector.
func (d *VMLiveMigrateDetector) Init() error {
	if d.nodeName == "" {
		logrus.Fatal("please specify a node name")
	}

	clientConfig := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		&clientcmd.ClientConfigLoadingRules{
			ExplicitPath: d.kubeConfig,
		},
		&clientcmd.ConfigOverrides{
			ClusterInfo:    clientcmdapi.Cluster{},
			CurrentContext: d.kubeContext,
		},
	)

	var err error
	d.virtClient, err = kubecli.GetKubevirtClientFromClientConfig(clientConfig)
	if err != nil {
		logrus.Fatalf("cannot obtain KubeVirt client: %v", err)
	}

	restConfig, err := clientConfig.ClientConfig()
	if err != nil {
		logrus.Fatalf("cannot obtain rest config: %v", err)
	}

	d.k8sClient, err = kubernetes.NewForConfig(restConfig)
	if err != nil {
		logrus.Fatalf("cannot obtain Kubernetes client: %v", err)
	}

	s := runtime.NewScheme()
	_ = managementv1beta1.AddToScheme(s)

	broadcaster := record.NewBroadcaster()
	broadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: d.k8sClient.CoreV1().Events(harvesterSystemNamespace)})
	d.recorder = broadcaster.NewRecorder(
		s,
		corev1.EventSource{Component: "vm-live-migrate-detector", Host: d.nodeName},
	)

	return nil
}

// Run executes the detection and optional shutdown logic.
func (d *VMLiveMigrateDetector) Run(ctx context.Context) error {
	defer func() {
		// wait for events to be flushed
		time.Sleep(10 * time.Second)
	}()

	vmis, err := d.getVMIs(ctx)
	if err != nil {
		return err
	}

	nodes, err := d.k8sClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to get nodes: %w", err)
	}
	nodePointers := make([]*corev1.Node, 0, len(nodes.Items))
	for i := range nodes.Items {
		nodePointers = append(nodePointers, &nodes.Items[i])
	}

	nonLiveMigratableVMNames, err := vmi.GetAllNonLiveMigratableVMINames(vmis, nodePointers)
	if err != nil {
		return err
	}

	logrus.Infof("Non-migratable VM(s): %v", nonLiveMigratableVMNames)

	if d.upgradeName != "" {
		vmNames := getRestoreVMNames(vmis, nonLiveMigratableVMNames)
		logrus.Infof("Store vm info to configmap: %v", vmNames)
		err = d.createOrUpdateConfigMap(ctx, vmNames)
		if err != nil {
			d.recordUpgradeEvent(corev1.EventTypeWarning, RestoreVMConfigMapFailed, err.Error())
			return err
		}
	}

	vmSuccessCnt := 0
	vmFailedCnt := 0
	if d.shutdown {
		for _, namespacedName := range nonLiveMigratableVMNames {
			namespace, name := splitNamespacedName(namespacedName)
			if err := d.virtClient.VirtualMachine(namespace).Stop(ctx, name, &kubevirtv1.StopOptions{}); err != nil {
				d.recordUpgradeEvent(corev1.EventTypeNormal, VMShutdownFailed,
					fmt.Sprintf("Shutdown failed for VM %s on node %s, error: %v", namespacedName, d.nodeName, err))
				logrus.Errorf("failed to stop VM %s: %v", namespacedName, err)
				vmFailedCnt++
			} else {
				vmSuccessCnt++
			}
			logrus.Infof("vm %s was administratively stopped", namespacedName)
		}
	}

	d.recordUpgradeEvent(corev1.EventTypeNormal, VMShutdownCompleted,
		fmt.Sprintf("Shutdown completed for %d VM(s) on node %s, success: %d, failed: %d ", len(nonLiveMigratableVMNames), d.nodeName, vmSuccessCnt, vmFailedCnt))

	return nil
}

func (d *VMLiveMigrateDetector) getVMIs(ctx context.Context) ([]*kubevirtv1.VirtualMachineInstance, error) {
	nodeReq, err := labels.NewRequirement("kubevirt.io/nodeName", selection.Equals, []string{d.nodeName})
	if err != nil {
		return nil, fmt.Errorf("failed to create node label requirement: %w", err)
	}
	options := metav1.ListOptions{LabelSelector: labels.NewSelector().Add(*nodeReq).String()}

	vmiList, err := d.virtClient.VirtualMachineInstance("").List(ctx, options)
	if err != nil {
		return nil, err
	}
	vmis := make([]*kubevirtv1.VirtualMachineInstance, 0, len(vmiList.Items))
	for i := range vmiList.Items {
		vmis = append(vmis, &vmiList.Items[i])
	}
	return vmis, nil
}

// getRestoreVMNames filters out paused VMs and upgrade-related VMs from the candidate list.
func getRestoreVMNames(vmis []*kubevirtv1.VirtualMachineInstance, candidateVMNames []string) []string {
	restoreVMs := make([]string, 0)

	excludeVMs := make(map[string]struct{})
	for _, vmi := range vmis {
		// Exclude paused VMs and upgrade repo VMs
		isPaused := false
		for _, cond := range vmi.Status.Conditions {
			if string(cond.Type) == "Paused" && cond.Status == corev1.ConditionTrue {
				isPaused = true
				break
			}
		}
		if isPaused || vmi.Labels[upgradeLabel] != "" {
			namespacedName := fmt.Sprintf("%s/%s", vmi.Namespace, vmi.Name)
			excludeVMs[namespacedName] = struct{}{}
		}
	}
	for _, name := range candidateVMNames {
		if _, exist := excludeVMs[name]; !exist {
			restoreVMs = append(restoreVMs, name)
		}
	}
	return restoreVMs
}

func (d *VMLiveMigrateDetector) createOrUpdateConfigMap(ctx context.Context, restoreVMNames []string) error {
	vmNames := strings.Join(restoreVMNames, ",")
	name := GetRestoreVMConfigMapName(d.upgradeName)
	namespace := harvesterSystemNamespace

	// Fetch the UpgradePlan to use as owner reference
	upgradePlan, err := d.getUpgradePlan(ctx, d.upgradeName)
	if err != nil {
		return fmt.Errorf("failed to get UpgradePlan %s: %w", d.upgradeName, err)
	}

	return retry.OnError(
		retry.DefaultBackoff,
		func(err error) bool {
			return errors.IsConflict(err) || errors.IsServerTimeout(err)
		},
		func() error {
			configMap, err := d.k8sClient.CoreV1().ConfigMaps(namespace).Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				if errors.IsNotFound(err) {
					newConfigMap := &corev1.ConfigMap{
						ObjectMeta: metav1.ObjectMeta{
							Name:      name,
							Namespace: namespace,
							OwnerReferences: []metav1.OwnerReference{
								{
									Name:       upgradePlan.Name,
									Kind:       "UpgradePlan",
									UID:        upgradePlan.UID,
									APIVersion: managementv1beta1.GroupVersion.String(),
								},
							},
						},
						Data: map[string]string{
							d.nodeName: vmNames,
						},
					}
					_, createErr := d.k8sClient.CoreV1().ConfigMaps(namespace).Create(ctx, newConfigMap, metav1.CreateOptions{})
					if createErr != nil && !errors.IsAlreadyExists(createErr) {
						return fmt.Errorf("failed to create ConfigMap: %w", createErr)
					}
					d.recordUpgradeEvent(corev1.EventTypeNormal, RestoreVMConfigMapCreated,
						fmt.Sprintf("ConfigMap %s/%s created", namespace, name))
					return nil
				}
				return fmt.Errorf("failed to get ConfigMap: %w", err)
			}

			if configMap.Data == nil {
				configMap.Data = map[string]string{}
			}
			configMap.Data[d.nodeName] = vmNames

			_, updateErr := d.k8sClient.CoreV1().ConfigMaps(namespace).Update(ctx, configMap, metav1.UpdateOptions{})
			if updateErr != nil {
				return fmt.Errorf("failed to update ConfigMap: %w", updateErr)
			}
			return nil
		})
}

func (d *VMLiveMigrateDetector) getUpgradePlan(ctx context.Context, name string) (*managementv1beta1.UpgradePlan, error) {
	upgradePlan := &managementv1beta1.UpgradePlan{}
	result := d.k8sClient.CoreV1().RESTClient().Get().
		AbsPath("/apis", managementv1beta1.GroupVersion.Group, managementv1beta1.GroupVersion.Version, "upgradeplans", name).
		Do(ctx)
	if err := result.Error(); err != nil {
		return nil, err
	}
	if err := result.Into(upgradePlan); err != nil {
		return nil, err
	}
	return upgradePlan, nil
}

func (d *VMLiveMigrateDetector) recordUpgradeEvent(eventType, reason, message string) {
	if d.upgradeName == "" {
		return
	}

	upgradePlan, err := d.getUpgradePlan(context.Background(), d.upgradeName)
	if err != nil {
		logrus.Warnf("record event failed to get UpgradePlan %s: %v", d.upgradeName, err)
		return
	}

	logrus.Info("Recording event for upgrade ", d.upgradeName, ": ", eventType, " ", reason, " ", message)
	d.recorder.Event(upgradePlan, eventType, reason, message)
}

// GetRestoreVMConfigMapName returns the ConfigMap name used to store VM names for restoration.
// This mirrors upstream's name.SafeConcatName behavior: join parts with "-", and if the
// result exceeds 63 characters, truncate and append a short hash for uniqueness.
func GetRestoreVMConfigMapName(upgradeName string) string {
	return safeConcatName(upgradeName, restoreVMConfigMapPrefix)
}

const maxNameLength = 63

// safeConcatName concatenates name parts with "-" and ensures the result is at most 63
// characters. If truncation is needed, a 5-character hex hash of the full name is appended
// to preserve uniqueness. This matches the behavior of wrangler's name.SafeConcatName.
func safeConcatName(parts ...string) string {
	fullName := strings.Join(parts, "-")
	if len(fullName) <= maxNameLength {
		return fullName
	}
	hash := sha256.Sum256([]byte(fullName))
	hashStr := hex.EncodeToString(hash[:])[:8]
	return fullName[:maxNameLength-9] + "-" + hashStr
}

// splitNamespacedName splits "namespace/name" into its parts.
func splitNamespacedName(namespacedName string) (string, string) {
	parts := strings.SplitN(namespacedName, "/", 2)
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return "", namespacedName
}
