package upgradeplan

import (
	"context"
	"fmt"
	"strings"

	harvesterv1beta1 "github.com/harvester/harvester/pkg/apis/harvesterhci.io/v1beta1"
	upgradev1 "github.com/rancher/system-upgrade-controller/pkg/apis/upgrade.cattle.io/v1"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	managementv1beta1 "github.com/harvester/upgrade-toolkit/api/v1beta1"
)

const (
	harvesterSystemNamespace       = "harvester-system"
	cattleSystemNamespace          = "cattle-system"
	kubeSystemNamespace            = "kube-system"
	harvesterName                  = "harvester"
	serverVersionSettingName       = "server-version"
	sucName                        = "system-upgrade-controller"
	caName                         = "serving-ca"
	longhornStaticStorageClassName = "longhorn-static"

	harvesterManagedLabel = "harvesterhci.io/managed"
	serviceNameLabel      = "kubernetes.io/service-name"
	imageComponent        = "iso"
	repoComponent         = "repo"

	defaultTTLSecondsAfterFinished = 604800 // 7 days

	rke2UpgradeImage    = "rancher/rke2-upgrade"
	upgradeToolkitImage = "starbops/harvester-upgrade-toolkit"
)

var (
	isoDownloaderScriptTemplate = `
#!/usr/bin/env sh
set -e

WORK_DIR="/iso"
LOCK_FILE="leader.lock"
READY_FLAG="harvester.iso.ready"

if mkdir "$WORK_DIR"/"$LOCK_FILE" 2>/dev/null; then
  trap "rmdir $WORK_DIR/$LOCK_FILE; rm -vf $WORK_DIR/$READY_FLAG; exit 1" EXIT

  echo "$POD_NAME is the leader, start preparing the ISO image..."
  CA_FILE=$(mktemp)
  echo "$CA_CERT" > "$CA_FILE"
  curl -sSfL --cacert "$CA_FILE" \
    "https://harvester:8443/v1/harvester/harvesterhci.io.virtualmachineimages/$VM_IMAGE_NS/$VM_IMAGE_NAME/download" \
    -o harvester.iso.gz

  echo "Download completed, extracting harvester.iso..."
  gzip -dc harvester.iso.gz > "$WORK_DIR"/harvester.iso

  echo "harvester.iso is ready"
  touch "$WORK_DIR"/"$READY_FLAG"

  trap - EXIT
else
  echo "$POD_NAME is a follower, waiting for harvester.iso downloaded..."

  if [ -f "$WORK_DIR"/"$READY_FLAG" ]; then
    echo "harvester.iso already exists"
  else
    until [ -f "$WORK_DIR"/"$READY_FLAG" ]; do
      echo "harvester.iso is not ready yet, waiting..."
      sleep 10
    done
    echo "harvester.iso is ready"
  fi
fi
`
	preloaderScript = `
#!/usr/bin/env sh
set -e

HOST_DIR="${HOST_DIR:-/host}"
export CONTAINER_RUNTIME_ENDPOINT=unix:///$HOST_DIR/run/k3s/containerd/containerd.sock
export CONTAINERD_ADDRESS=$HOST_DIR/run/k3s/containerd/containerd.sock

CTR="$HOST_DIR/$(readlink $HOST_DIR/var/lib/rancher/rke2/bin)/ctr"
if [ -z "$CTR" ];then
  echo "Fail to get host ctr binary."
  exit 1
fi

mount -o loop,ro /iso/harvester.iso /mnt
echo "harvester.iso mounted successfully"

echo "Start preloading images to containerd..."
for archive in $(yq e '.images.common[].archive' /mnt/bundle/metadata.yaml); do
  echo "Importing $archive"
  zstd -dc /mnt/bundle/"$archive" | $CTR -n k8s.io images import --no-unpack -
done
`
	isoMounterScript = `
#!/usr/bin/env sh
set -e

mount -o loop,ro /iso/harvester.iso /share-mount
echo "harvester.iso mounted successfully"
trap "umount -v /iso/harvester.iso; exit 0" EXIT
while true; do sleep 30; done
`
	repoScript = `
#!/usr/bin/env sh
set -e

echo "Starting Nginx..."
nginx -g "daemon off;"
`
)

// Version helpers

func getPreviousVersion(upgradePlan *managementv1beta1.UpgradePlan) string {
	if upgradePlan == nil {
		return "nonexistent"
	}

	if upgradePlan.Spec.Upgrade != nil {
		return *upgradePlan.Spec.Upgrade
	}

	if upgradePlan.Status.PreviousVersion != nil {
		return *upgradePlan.Status.PreviousVersion
	}

	return upgradePlan.Spec.Version
}

func getUpgradeVersion(upgradePlan *managementv1beta1.UpgradePlan) string {
	if upgradePlan == nil {
		return "nonexistent"
	}

	if upgradePlan.Spec.Upgrade != nil {
		return *upgradePlan.Spec.Upgrade
	}

	return upgradePlan.Spec.Version
}

func sanitizedVersion(version string) string {
	return strings.Replace(version, "+", "-", 1)
}

func getKubernetesVersion(upgradePlan *managementv1beta1.UpgradePlan) string {
	if upgradePlan != nil && upgradePlan.Status.ReleaseMetadata != nil {
		return sanitizedVersion(upgradePlan.Status.ReleaseMetadata.Kubernetes)
	}
	return ""
}

// Toleration helpers

func getDefaultTolerations() []corev1.Toleration {
	return []corev1.Toleration{
		{
			Key:      corev1.TaintNodeUnschedulable,
			Operator: corev1.TolerationOpExists,
			Effect:   corev1.TaintEffectNoSchedule,
		},
		{
			Key:      corev1.TaintNodeUnreachable,
			Operator: corev1.TolerationOpExists,
			Effect:   corev1.TaintEffectNoExecute,
		},
		{
			Key:      "node-role.kubernetes.io/control-plane",
			Operator: corev1.TolerationOpExists,
			Effect:   corev1.TaintEffectNoExecute,
		},
		{
			Key:      "node-role.kubernetes.io/etcd",
			Operator: corev1.TolerationOpExists,
			Effect:   corev1.TaintEffectNoExecute,
		},
		{
			Key:      "kubernetes.io/arch",
			Operator: corev1.TolerationOpEqual,
			Effect:   corev1.TaintEffectNoSchedule,
			Value:    "amd64",
		},
		{
			Key:      "kubernetes.io/arch",
			Operator: corev1.TolerationOpEqual,
			Effect:   corev1.TaintEffectNoSchedule,
			Value:    "arm64",
		},
		{
			Key:      "kubernetes.io/arch",
			Operator: corev1.TolerationOpEqual,
			Effect:   corev1.TaintEffectNoSchedule,
			Value:    "arm",
		},
		{
			Key:      "kubevirt.io/drain",
			Operator: corev1.TolerationOpExists,
			Effect:   corev1.TaintEffectNoSchedule,
		},
		{
			Key:      "CriticalAddonsOnly",
			Operator: corev1.TolerationOpExists,
		},
	}
}

// Phase and status helpers

func updateProgressingPhase(
	upgradePlan *managementv1beta1.UpgradePlan,
	phase managementv1beta1.UpgradePlanPhase,
	message string,
) {
	upgradePlan.Status.CurrentPhase = phase

	if !upgradePlan.ConditionExists(managementv1beta1.UpgradePlanProgressing) {
		upgradePlan.SetCondition(managementv1beta1.UpgradePlanProgressing, metav1.ConditionTrue, string(phase), "")
	} else {
		cond := upgradePlan.LookupCondition(managementv1beta1.UpgradePlanProgressing)
		upgradePlan.SetCondition(managementv1beta1.UpgradePlanProgressing, cond.Status, string(phase), message)
	}
}

func isTerminalPhase(phase managementv1beta1.UpgradePlanPhase) bool {
	return phase == managementv1beta1.UpgradePlanPhaseSucceeded ||
		phase == managementv1beta1.UpgradePlanPhaseFailed
}

func markUpgradePlanComplete(upgradePlan *managementv1beta1.UpgradePlan) {
	if isTerminalPhase(upgradePlan.Status.CurrentPhase) {
		upgradePlan.SetCondition(
			managementv1beta1.UpgradePlanAvailable,
			metav1.ConditionFalse,
			"Executed",
			"Entered one of the terminal phases",
		)
		upgradePlan.SetCondition(
			managementv1beta1.UpgradePlanProgressing,
			metav1.ConditionFalse,
			string(upgradePlan.Status.CurrentPhase),
			"UpgradePlan has completed",
		)
	}
}

// Resource readiness checks

func isVirtualMachineImageImported(vmImage *harvesterv1beta1.VirtualMachineImage) (finished, success bool) {
	for _, condition := range vmImage.Status.Conditions {
		if condition.Type == harvesterv1beta1.ImageImported && condition.Status == corev1.ConditionTrue {
			finished, success = true, true
			return
		}
		if condition.Type == harvesterv1beta1.ImageImported && condition.Status == corev1.ConditionFalse {
			finished, success = true, false
			return
		}
	}
	return
}

func isPersistentVolumeClaimBound(pvc *corev1.PersistentVolumeClaim) bool {
	return pvc.Status.Phase == corev1.ClaimBound
}

func isDaemonSetReady(ds *appsv1.DaemonSet) bool {
	return ds.Status.DesiredNumberScheduled > 0 && ds.Status.NumberReady == ds.Status.DesiredNumberScheduled
}

func isServiceReady(ctx context.Context, c client.Client, svc *corev1.Service) bool {
	if svc.Spec.ClusterIP == "" {
		return false
	}
	return isAnyEndpointReady(ctx, c, svc)
}

func isAnyEndpointReady(ctx context.Context, c client.Client, svc *corev1.Service) bool {
	var epsList discoveryv1.EndpointSliceList
	if err := c.List(ctx, &epsList, &client.ListOptions{
		LabelSelector: labels.SelectorFromSet(labels.Set{
			serviceNameLabel: svc.Name,
		}),
	}); err != nil {
		return false
	}

	if len(epsList.Items) == 0 {
		return false
	}

	for _, ep := range epsList.Items[0].Endpoints {
		if ep.Conditions.Ready != nil && *ep.Conditions.Ready {
			return true
		}
	}

	return false
}

func isJobFinished(job *batchv1.Job) (finished, success bool) {
	for _, condition := range job.Status.Conditions {
		if condition.Type == batchv1.JobComplete && condition.Status == corev1.ConditionTrue {
			finished, success = true, true
			return
		}
		if condition.Type == batchv1.JobFailed && condition.Status == corev1.ConditionTrue {
			finished, success = true, false
			return
		}
	}
	return
}

func isPlanFinished(plan *upgradev1.Plan) bool {
	for _, condition := range plan.Status.Conditions {
		if condition.Type == string(upgradev1.PlanComplete) && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func isAnyPlanJobFailed(plan *upgradev1.Plan) bool {
	for _, condition := range plan.Status.Conditions {
		if condition.Type == string(upgradev1.PlanComplete) &&
			condition.Status == corev1.ConditionFalse &&
			condition.Reason == string(batchv1.JobFailed) {
			return true
		}
	}
	return false
}

func isSkipOSUpgrade(upgradePlan *managementv1beta1.UpgradePlan) bool {
	return upgradePlan.Spec.SkipOSUpgrade != nil && *upgradePlan.Spec.SkipOSUpgrade
}

func isTerminalState(status managementv1beta1.NodeUpgradeStatus, skipOSUpgrade *bool) bool {
	if skipOSUpgrade != nil && *skipOSUpgrade {
		return status.State == managementv1beta1.NodeStateKubernetesUpgraded
	}
	return status.State == managementv1beta1.NodeStateOSUpgraded
}

// Generic resource helpers

func createOwnedAndFetch[T client.Object](
	ctx context.Context,
	c client.Client,
	scheme *runtime.Scheme,
	owner client.Object,
	obj T,
) (T, error) {
	if err := controllerutil.SetControllerReference(owner, obj, scheme); err != nil {
		var zero T
		return zero, err
	}
	if err := c.Create(ctx, obj, &client.CreateOptions{}); err != nil {
		var zero T
		return zero, err
	}
	nn := types.NamespacedName{Namespace: obj.GetNamespace(), Name: obj.GetName()}
	if err := c.Get(ctx, nn, obj); err != nil {
		var zero T
		return zero, err
	}
	return obj, nil
}

func getOrCreate[T client.Object](
	ctx context.Context,
	c client.Client,
	scheme *runtime.Scheme,
	nn types.NamespacedName,
	newObj func() T,
	build func() T,
	owner client.Object,
) (T, error) {
	obj := newObj()
	if err := c.Get(ctx, nn, obj); err != nil {
		if apierrors.IsNotFound(err) {
			return createOwnedAndFetch(ctx, c, scheme, owner, build())
		}
		var zero T
		return zero, err
	}
	return obj, nil
}

// constructPlan builds a generic system-upgrade-controller Plan resource.
func constructPlan(
	upgradePlanName, componentName string,
	concurrency int,
	nodeSelector *metav1.LabelSelector,
	maintenance bool,
	prepare *upgradev1.ContainerSpec,
	container *upgradev1.ContainerSpec,
	version string,
) *upgradev1.Plan {
	planName := fmt.Sprintf("%s-%s", upgradePlanName, componentName)

	plan := &upgradev1.Plan{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				HarvesterUpgradePlanLabel:      upgradePlanName,
				HarvesterUpgradeComponentLabel: componentName,
			},
			Name:      planName,
			Namespace: cattleSystemNamespace,
		},
		Spec: upgradev1.PlanSpec{
			Concurrency:           int64(concurrency),
			JobActiveDeadlineSecs: ptr.To[int64](0),
			NodeSelector:          nodeSelector,
			ServiceAccountName:    sucName,
			Tolerations:           getDefaultTolerations(),
			Prepare:               prepare,
			Upgrade:               container,
			Version:               version,
		},
	}

	if maintenance {
		plan.Spec.Cordon = true
		plan.Spec.Drain = &upgradev1.DrainSpec{
			Force: true,
		}
	}

	return plan
}
