package restorevm

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"k8s.io/client-go/tools/record"

	kubevirtv1 "kubevirt.io/api/core/v1"
	"kubevirt.io/client-go/kubecli"

	managementv1beta1 "github.com/harvester/upgrade-toolkit/api/v1beta1"
	"github.com/harvester/upgrade-toolkit/pkg/upgradehelper/vmlivemigratedetector"
)

const (
	harvesterSystemNamespace = "harvester-system"

	RestoreVMCompleted = "RestoreVMCompleted"
	RestoreVMFailed    = "RestoreVMFailed"
)

var healthzPath = "/apis/" + kubevirtv1.SubresourceGroupName + "/" + kubevirtv1.ApiLatestVersion + "/healthz"

// RestoreVMHandler restores VMs that were shut down during upgrade.
type RestoreVMHandler struct {
	kubeConfig  string
	kubeContext string

	nodeName    string
	upgradeName string

	virtClient   kubecli.KubevirtClient
	vmRestClient *rest.RESTClient
	k8sClient    kubernetes.Interface
	recorder     record.EventRecorder
}

// NewRestoreVMHandler creates a new RestoreVMHandler.
func NewRestoreVMHandler(kubeConfig, kubeContext, nodeName, upgrade string) (*RestoreVMHandler, error) {
	clientConfig := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		&clientcmd.ClientConfigLoadingRules{
			ExplicitPath: kubeConfig,
		},
		&clientcmd.ConfigOverrides{
			ClusterInfo:    clientcmdapi.Cluster{},
			CurrentContext: kubeContext,
		},
	)

	restConfig, err := clientConfig.ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to build REST config: %w", err)
	}

	virtClient, err := kubecli.GetKubevirtClientFromRESTConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to get kubevirt client: %w", err)
	}

	k8sClient, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kubernetes client: %w", err)
	}

	s := runtime.NewScheme()
	_ = managementv1beta1.AddToScheme(s)

	broadcaster := record.NewBroadcaster()
	eventSink := &typedcorev1.EventSinkImpl{
		Interface: k8sClient.CoreV1().Events(harvesterSystemNamespace),
	}
	broadcaster.StartRecordingToSink(eventSink)
	recorder := broadcaster.NewRecorder(
		s,
		corev1.EventSource{Component: "restore-vm", Host: nodeName},
	)

	return &RestoreVMHandler{
		kubeConfig:   kubeConfig,
		kubeContext:  kubeContext,
		nodeName:     nodeName,
		upgradeName:  upgrade,
		virtClient:   virtClient,
		vmRestClient: virtClient.RestClient(),
		k8sClient:    k8sClient,
		recorder:     recorder,
	}, nil
}

// Run executes the VM restoration logic.
func (h *RestoreVMHandler) Run(ctx context.Context) error {
	defer func() {
		// wait for events to be flushed
		time.Sleep(10 * time.Second)
	}()

	vmNames, err := h.getVMNamesFromConfigMap(ctx)
	if err != nil {
		if errors.IsNotFound(err) {
			logrus.Warn("ConfigMap not found")
			h.recordUpgradeEvent(corev1.EventTypeWarning, RestoreVMFailed, "ConfigMap not found")
			return nil
		}
		return err
	}
	if len(vmNames) == 0 {
		logrus.Info("No VMs to restore")
		h.recordUpgradeEvent(corev1.EventTypeNormal, RestoreVMCompleted,
			fmt.Sprintf("Restored 0 VM for node %s during upgrade %s", h.nodeName, h.upgradeName))
		return nil
	}

	if err := h.checkKubeVirtHealth(ctx); err != nil {
		return fmt.Errorf("KubeVirt not ready: %w", err)
	}

	vmSuccessCnt := 0
	vmFailedCnt := 0
	for _, vmFullName := range vmNames {
		parts := strings.SplitN(vmFullName, "/", 2)
		if len(parts) != 2 {
			logrus.Errorf("Invalid VM name: %s, should be namespace/name", vmFullName)
			continue
		}
		ns, name := parts[0], parts[1]
		logrus.Infof("Starting VM %s/%s...", ns, name)
		if err := h.startVM(ctx, ns, name); err != nil {
			logrus.Errorf("Failed to start VM %s/%s: %v", ns, name, err)
			msg := fmt.Sprintf(
				"Failed to restore VM %s/%s for node %s during upgrade %s: %v",
				ns, name, h.nodeName, h.upgradeName, err,
			)
			h.recordUpgradeEvent(corev1.EventTypeWarning, RestoreVMFailed, msg)
			vmFailedCnt++
		} else {
			vmSuccessCnt++
		}
	}

	msg := fmt.Sprintf(
		"Restored %d VMs for node %s during upgrade %s, success: %d, failed: %d",
		len(vmNames), h.nodeName, h.upgradeName, vmSuccessCnt, vmFailedCnt,
	)
	h.recordUpgradeEvent(corev1.EventTypeNormal, RestoreVMCompleted, msg)
	return nil
}

func (h *RestoreVMHandler) checkKubeVirtHealth(ctx context.Context) error {
	logrus.Infof("Waiting for KubeVirt to be ready...")
	return wait.PollUntilContextTimeout(ctx, 5*time.Second, 30*time.Minute, true, func(ctx context.Context) (bool, error) {
		res := h.vmRestClient.Get().AbsPath(healthzPath).Do(ctx)
		if res.Error() != nil {
			logrus.Errorf("KubeVirt health check failed: %v, retry...", res.Error())
			return false, nil
		}
		return true, nil
	})
}

func (h *RestoreVMHandler) getVMNamesFromConfigMap(ctx context.Context) ([]string, error) {
	cmName := vmlivemigratedetector.GetRestoreVMConfigMapName(h.upgradeName)
	cm, err := h.k8sClient.CoreV1().ConfigMaps(harvesterSystemNamespace).Get(ctx, cmName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get configmap: %w", err)
	}
	vmNamesStr, ok := cm.Data[h.nodeName]
	if !ok || strings.TrimSpace(vmNamesStr) == "" {
		return []string{}, nil
	}
	rawNames := strings.Split(vmNamesStr, ",")
	vmNames := make([]string, 0, len(rawNames))
	for _, name := range rawNames {
		name = strings.TrimSpace(name)
		if name != "" {
			vmNames = append(vmNames, name)
		}
	}
	return vmNames, nil
}

func (h *RestoreVMHandler) startVM(ctx context.Context, namespace, name string) error {
	return h.virtClient.VirtualMachine(namespace).Start(ctx, name, &kubevirtv1.StartOptions{})
}

func (h *RestoreVMHandler) getUpgradePlan(ctx context.Context, name string) (*managementv1beta1.UpgradePlan, error) {
	upgradePlan := &managementv1beta1.UpgradePlan{}
	result := h.k8sClient.CoreV1().RESTClient().Get().
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

func (h *RestoreVMHandler) recordUpgradeEvent(eventType, reason, message string) {
	upgradePlan, err := h.getUpgradePlan(context.Background(), h.upgradeName)
	if err != nil {
		logrus.Warnf("Record upgrade events failed: %v", err)
		return
	}

	logrus.Info("Recording event for upgrade ", h.upgradeName, ": ", eventType, " ", reason, " ", message)
	h.recorder.Event(upgradePlan.ObjectReference(harvesterSystemNamespace), eventType, reason, message)
}
