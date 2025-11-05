package upgradeplan

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"text/template"

	"github.com/go-logr/logr"
	harvesterv1beta1 "github.com/harvester/harvester/pkg/apis/harvesterhci.io/v1beta1"
	upgradev1 "github.com/rancher/system-upgrade-controller/pkg/apis/upgrade.cattle.io/v1"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
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

type UpgradePlanPhaseHandler struct {
	client client.Client
	scheme *runtime.Scheme
	log    logr.Logger
}

func NewUpgradePlanPhaseHandler(c client.Client, s *runtime.Scheme, log logr.Logger) *UpgradePlanPhaseHandler {
	return &UpgradePlanPhaseHandler{
		client: c,
		scheme: s,
		log:    log,
	}
}

func (h *UpgradePlanPhaseHandler) initialize(
	ctx context.Context,
	upgradePlan *managementv1beta1.UpgradePlan,
) (ctrl.Result, error) {
	h.log.V(1).Info("handle initialize status")

	if upgradePlan.Status.CurrentPhase == managementv1beta1.UpgradePlanPhaseInitializing {
		upgradePlan.SetCondition(managementv1beta1.UpgradePlanAvailable, metav1.ConditionTrue, "Executable", "")

		if err := h.loadVersion(ctx, upgradePlan); err != nil {
			return ctrl.Result{}, err
		}

		if err := h.loadPreviousVersion(ctx, upgradePlan); err != nil {
			return ctrl.Result{}, err
		}

		updateProgressingPhase(upgradePlan, managementv1beta1.UpgradePlanPhaseInitialized, "")
		return ctrl.Result{}, nil
	}

	updateProgressingPhase(upgradePlan, managementv1beta1.UpgradePlanPhaseInitializing, "")
	return ctrl.Result{}, nil
}

func (h *UpgradePlanPhaseHandler) isoDownload(
	ctx context.Context,
	upgradePlan *managementv1beta1.UpgradePlan,
) (ctrl.Result, error) {
	h.log.V(1).Info("handle iso download")

	vmImage, err := h.getOrCreateVirtualMachineImageForRepo(ctx, upgradePlan)
	if err != nil {
		h.log.Error(err, "unable to retrieve iso vmimage from upgradeplan")
		return ctrl.Result{}, err
	}
	if upgradePlan.Status.ISOImageID == nil {
		upgradePlan.Status.ISOImageID = ptr.To(fmt.Sprintf("%s/%s", vmImage.Namespace, vmImage.Name))
	}

	imported, success := isVirtualMachineImageImported(vmImage)

	// vmImage download still ongoing
	if !imported {
		h.log.V(1).Info("iso image downloading")
		updateProgressingPhase(upgradePlan, managementv1beta1.UpgradePlanPhaseISODownloading, "")
		return ctrl.Result{}, nil
	}

	// vmImage download finished but failed
	if !success {
		h.log.V(0).Info("iso image download failed")
		updateProgressingPhase(upgradePlan, managementv1beta1.UpgradePlanPhaseFailed, "ISO image download failed")
		return ctrl.Result{}, nil
	}

	// vmImage download finished successfully
	updateProgressingPhase(upgradePlan, managementv1beta1.UpgradePlanPhaseISODownloaded, "")
	return ctrl.Result{}, nil
}

func (h *UpgradePlanPhaseHandler) repoCreate(
	ctx context.Context,
	upgradePlan *managementv1beta1.UpgradePlan,
) (ctrl.Result, error) {
	h.log.V(1).Info("handle repo create")

	pvc, err := h.getOrCreatePersistentVolumeClaimForRepo(ctx, upgradePlan)
	if err != nil {
		h.log.Error(err, "unable to retrieve repo persistentvolumeclaim from upgradeplan")
		return ctrl.Result{}, err
	}

	bound := isPersistentVolumeClaimBound(pvc)

	if !bound {
		h.log.V(1).Info("upgrade-repo persistentvolumeclaim not bound")
		updateProgressingPhase(upgradePlan, managementv1beta1.UpgradePlanPhaseRepoCreating, "upgrade-repo pvc not bound")
		return ctrl.Result{}, nil
	}

	updateProgressingPhase(upgradePlan, managementv1beta1.UpgradePlanPhaseRepoCreating, "upgrade-repo pvc bound")

	repo, err := h.getOrCreateDaemonSetForRepo(ctx, upgradePlan, pvc)
	if err != nil {
		h.log.Error(err, "unable to retrieve repo daemonset from upgradeplan")
		return ctrl.Result{}, err
	}

	ready := isDaemonSetReady(repo)

	if !ready {
		h.log.V(1).Info("upgrade-repo daemonset not ready")
		updateProgressingPhase(
			upgradePlan,
			managementv1beta1.UpgradePlanPhaseRepoCreating,
			"upgrade-repo daemonset not ready",
		)
		return ctrl.Result{}, nil
	}

	svc, err := h.getOrCreateServiceForRepo(ctx, upgradePlan)
	if err != nil {
		h.log.Error(err, "unable to retrieve repo service from upgradeplan")
		return ctrl.Result{}, err
	}

	ready = isServiceReady(ctx, h.client, svc)

	if !ready {
		h.log.V(1).Info("upgrade-repo service/endpoints not ready")
		updateProgressingPhase(upgradePlan, managementv1beta1.UpgradePlanPhaseRepoCreating, "upgrade-repo svc/ep not ready")
		return ctrl.Result{}, nil
	}

	updateProgressingPhase(upgradePlan, managementv1beta1.UpgradePlanPhaseRepoCreated, "")
	return ctrl.Result{}, nil
}

// metadataPopulate populates UpgradePlan with release metadata retrieved from the Upgrade Repo
func (h *UpgradePlanPhaseHandler) metadataPopulate(upgradePlan *managementv1beta1.UpgradePlan) (ctrl.Result, error) {
	h.log.V(1).Info("handle metadata populate")

	if upgradePlan.Status.CurrentPhase == managementv1beta1.UpgradePlanPhaseMetadataPopulating {
		harvesterRelease := newHarvesterRelease(upgradePlan)
		if err := harvesterRelease.loadReleaseMetadata(); err != nil {
			updateProgressingPhase(upgradePlan, managementv1beta1.UpgradePlanPhaseMetadataPopulating, err.Error())
			return ctrl.Result{}, err
		}
		upgradePlan.Status.ReleaseMetadata = harvesterRelease.ReleaseMetadata
		updateProgressingPhase(upgradePlan, managementv1beta1.UpgradePlanPhaseMetadataPopulated, "")
		return ctrl.Result{}, nil
	}

	updateProgressingPhase(upgradePlan, managementv1beta1.UpgradePlanPhaseMetadataPopulating, "")
	return ctrl.Result{}, nil
}

func (h *UpgradePlanPhaseHandler) imagePreload(
	ctx context.Context,
	upgradePlan *managementv1beta1.UpgradePlan,
) (ctrl.Result, error) {
	h.log.V(1).Info("handle image preload")

	imagePreloadPlan, err := h.getOrCreatePlanForImagePreload(ctx, upgradePlan)
	if err != nil {
		h.log.Error(err, "unable to retrieve image-preload plan from upgradeplan")
		return ctrl.Result{}, err
	}

	if !isPlanFinished(imagePreloadPlan) {
		if isAnyPlanJobFailed(imagePreloadPlan) {
			updateProgressingPhase(upgradePlan, managementv1beta1.UpgradePlanPhaseFailed, "image-preload plan job(s) failed")
			return ctrl.Result{}, nil
		}

		h.log.V(1).Info("image-preload plan running")
		updateProgressingPhase(upgradePlan, managementv1beta1.UpgradePlanPhaseImagePreloading, "")
		return ctrl.Result{}, nil
	}

	// Plan finished successfully
	updateProgressingPhase(upgradePlan, managementv1beta1.UpgradePlanPhaseImagePreloaded, "")
	return ctrl.Result{}, nil
}

func (h *UpgradePlanPhaseHandler) clusterUpgrade(
	ctx context.Context,
	upgradePlan *managementv1beta1.UpgradePlan,
) (ctrl.Result, error) {
	h.log.V(1).Info("handle cluster upgrade")

	clusterUpgradeJob, err := h.getOrCreateJobForClusterUpgrade(ctx, upgradePlan)
	if err != nil {
		h.log.Error(err, "unable to retrieve cluster-upgrade job from upgradeplan")
		return ctrl.Result{}, err
	}

	finished, success := isJobFinished(clusterUpgradeJob)

	// Job still running
	if !finished {
		h.log.V(1).Info("cluster-upgrade job running")
		updateProgressingPhase(upgradePlan, managementv1beta1.UpgradePlanPhaseClusterUpgrading, "")
		return ctrl.Result{}, nil
	}

	// Job finished but failed
	if !success {
		h.log.V(0).Info("cluster-upgrade job failed")
		updateProgressingPhase(upgradePlan, managementv1beta1.UpgradePlanPhaseFailed, "cluster-upgrade job failed")
		return ctrl.Result{}, nil
	}

	// Job finished successfully
	updateProgressingPhase(upgradePlan, managementv1beta1.UpgradePlanPhaseClusterUpgraded, "")
	return ctrl.Result{}, nil
}

func (h *UpgradePlanPhaseHandler) nodeUpgrade(
	ctx context.Context,
	upgradePlan *managementv1beta1.UpgradePlan,
) (ctrl.Result, error) {
	h.log.V(1).Info("handle node upgrade")

	nodeUpgradePlan, err := h.getOrCreatePlanForNodeUpgrade(ctx, upgradePlan)
	if err != nil {
		h.log.Error(err, "unable to retrieve node-upgrade plan from upgradeplan")
		return ctrl.Result{}, err
	}

	if !isPlanFinished(nodeUpgradePlan) {
		if isAnyPlanJobFailed(nodeUpgradePlan) {
			updateProgressingPhase(upgradePlan, managementv1beta1.UpgradePlanPhaseFailed, "node-upgrade plan job(s) failed")
			return ctrl.Result{}, nil
		}

		h.log.V(1).Info("node-upgrade plan running")
		updateProgressingPhase(upgradePlan, managementv1beta1.UpgradePlanPhaseNodeUpgrading, "")
		return ctrl.Result{}, nil
	}

	// Plan finished successfully
	updateProgressingPhase(upgradePlan, managementv1beta1.UpgradePlanPhaseNodeUpgraded, "")
	return ctrl.Result{}, nil
}

func (h *UpgradePlanPhaseHandler) resourceCleanup(
	ctx context.Context,
	upgradePlan *managementv1beta1.UpgradePlan,
) (ctrl.Result, error) {
	h.log.V(1).Info("handle resource cleanup")

	// Ensure the following resources are wiped out upon successful upgrades:
	// - iso VirtualMachineImage
	// - upgrade-repo PersistentVolumeClaim, DaemonSet, and Service
	// - image-preload Plan
	// - cluster-upgrade Job
	// - node-upgrade Plan
	if upgradePlan.Status.CurrentPhase == managementv1beta1.UpgradePlanPhaseCleaningUp {
		resourcesToDelete := []struct {
			obj       client.Object
			namespace string
			component string
		}{
			{&harvesterv1beta1.VirtualMachineImage{}, harvesterSystemNamespace, imageComponent},
			{&corev1.PersistentVolumeClaim{}, harvesterSystemNamespace, repoComponent},
			{&appsv1.DaemonSet{}, harvesterSystemNamespace, repoComponent},
			{&corev1.Service{}, harvesterSystemNamespace, repoComponent},
			{&upgradev1.Plan{}, cattleSystemNamespace, PrepareComponent},
			{&batchv1.Job{}, harvesterSystemNamespace, ClusterComponent},
			{&upgradev1.Plan{}, cattleSystemNamespace, NodeComponent},
		}

		for _, r := range resourcesToDelete {
			namespacedName := types.NamespacedName{
				Namespace: r.namespace,
				Name:      fmt.Sprintf("%s-%s", upgradePlan.Name, r.component),
			}
			if err := h.deleteResource(ctx, r.obj, namespacedName); err != nil {
				return ctrl.Result{}, err
			}
		}

		updateProgressingPhase(upgradePlan, managementv1beta1.UpgradePlanPhaseCleanedUp, "")
		return ctrl.Result{}, nil
	}

	updateProgressingPhase(upgradePlan, managementv1beta1.UpgradePlanPhaseCleaningUp, "")
	return ctrl.Result{}, nil
}

func (h *UpgradePlanPhaseHandler) loadVersion(
	ctx context.Context,
	upgradePlan *managementv1beta1.UpgradePlan,
) error {
	var version managementv1beta1.Version
	if err := h.client.Get(ctx, types.NamespacedName{Name: upgradePlan.Spec.Version}, &version); err != nil {
		return err
	}
	upgradePlan.Status.Version = &version.Spec
	return nil
}

func (h *UpgradePlanPhaseHandler) loadPreviousVersion(
	ctx context.Context,
	upgradePlan *managementv1beta1.UpgradePlan,
) error {
	var setting harvesterv1beta1.Setting
	if err := h.client.Get(ctx, types.NamespacedName{Name: serverVersionSettingName}, &setting); err != nil {
		return err
	}
	upgradePlan.Status.PreviousVersion = ptr.To(setting.Value)
	return nil
}

func (h *UpgradePlanPhaseHandler) getCA(ctx context.Context) (string, error) {
	var caSecret corev1.Secret
	if err := h.client.Get(
		ctx,
		types.NamespacedName{Namespace: kubeSystemNamespace, Name: caName},
		&caSecret,
	); err != nil {
		return "", err
	}

	caPem, ok := caSecret.Data[corev1.TLSCertKey]
	if !ok {
		return "nil", fmt.Errorf("tls.crt not found")
	}

	return string(caPem), nil
}

func (h *UpgradePlanPhaseHandler) isSingleNodeCluster(ctx context.Context) (bool, error) {
	var nodeList corev1.NodeList
	if err := h.client.List(ctx, &nodeList, &client.ListOptions{
		LabelSelector: labels.SelectorFromSet(labels.Set{
			harvesterManagedLabel: "true",
		}),
	}); err != nil {
		return false, err
	}
	return len(nodeList.Items) == 1, nil
}

func (h *UpgradePlanPhaseHandler) getOrCreateVirtualMachineImageForRepo(
	ctx context.Context,
	up *managementv1beta1.UpgradePlan,
) (*harvesterv1beta1.VirtualMachineImage, error) {
	nn := types.NamespacedName{
		Namespace: harvesterSystemNamespace,
		Name:      fmt.Sprintf("%s-%s", up.Name, imageComponent),
	}
	return getOrCreate(
		ctx, h.client, h.scheme, nn,
		func() *harvesterv1beta1.VirtualMachineImage { return &harvesterv1beta1.VirtualMachineImage{} },
		func() *harvesterv1beta1.VirtualMachineImage { return constructVirtualMachineImage(up) },
		up,
	)
}

func (h *UpgradePlanPhaseHandler) getOrCreatePersistentVolumeClaimForRepo(
	ctx context.Context,
	up *managementv1beta1.UpgradePlan,
) (*corev1.PersistentVolumeClaim, error) {
	nn := types.NamespacedName{
		Namespace: harvesterSystemNamespace,
		Name:      fmt.Sprintf("%s-%s", up.Name, repoComponent),
	}
	return getOrCreate(
		ctx, h.client, h.scheme, nn,
		func() *corev1.PersistentVolumeClaim { return &corev1.PersistentVolumeClaim{} },
		func() *corev1.PersistentVolumeClaim { return constructPersistentVolumeClaim(up) },
		up,
	)
}

func (h *UpgradePlanPhaseHandler) getOrCreateDaemonSetForRepo(
	ctx context.Context,
	up *managementv1beta1.UpgradePlan,
	pvc *corev1.PersistentVolumeClaim,
) (*appsv1.DaemonSet, error) {
	nn := types.NamespacedName{
		Namespace: harvesterSystemNamespace,
		Name:      fmt.Sprintf("%s-%s", up.Name, repoComponent),
	}
	ca, err := h.getCA(ctx)
	if err != nil {
		return nil, err
	}
	return getOrCreate(
		ctx, h.client, h.scheme, nn,
		func() *appsv1.DaemonSet { return &appsv1.DaemonSet{} },
		func() *appsv1.DaemonSet { return constructDaemonSet(up, pvc, ca) },
		up,
	)
}

func (h *UpgradePlanPhaseHandler) getOrCreateServiceForRepo(
	ctx context.Context,
	up *managementv1beta1.UpgradePlan,
) (*corev1.Service, error) {
	nn := types.NamespacedName{
		Namespace: harvesterSystemNamespace,
		Name:      fmt.Sprintf("%s-%s", up.Name, repoComponent),
	}
	return getOrCreate(
		ctx, h.client, h.scheme, nn,
		func() *corev1.Service { return &corev1.Service{} },
		func() *corev1.Service { return constructService(up) },
		up,
	)
}

func (h *UpgradePlanPhaseHandler) getOrCreatePlanForImagePreload(
	ctx context.Context,
	up *managementv1beta1.UpgradePlan,
) (*upgradev1.Plan, error) {
	nn := types.NamespacedName{
		Namespace: cattleSystemNamespace,
		Name:      fmt.Sprintf("%s-%s", up.Name, PrepareComponent),
	}
	return getOrCreate(
		ctx, h.client, h.scheme, nn,
		func() *upgradev1.Plan { return &upgradev1.Plan{} },
		func() *upgradev1.Plan { return constructPlanForImagePreload(up) },
		up,
	)
}

func (h *UpgradePlanPhaseHandler) getOrCreateJobForClusterUpgrade(
	ctx context.Context,
	up *managementv1beta1.UpgradePlan,
) (*batchv1.Job, error) {
	nn := types.NamespacedName{
		Namespace: harvesterSystemNamespace,
		Name:      fmt.Sprintf("%s-%s", up.Name, ClusterComponent),
	}
	return getOrCreate(
		ctx, h.client, h.scheme, nn,
		func() *batchv1.Job { return &batchv1.Job{} },
		func() *batchv1.Job { return constructJobForClusterUpgrade(up) },
		up,
	)
}

func (h *UpgradePlanPhaseHandler) getOrCreatePlanForNodeUpgrade(
	ctx context.Context,
	up *managementv1beta1.UpgradePlan,
) (*upgradev1.Plan, error) {
	nn := types.NamespacedName{
		Namespace: cattleSystemNamespace,
		Name:      fmt.Sprintf("%s-%s", up.Name, NodeComponent),
	}
	single, err := h.isSingleNodeCluster(ctx)
	if err != nil {
		return nil, err
	}
	return getOrCreate(
		ctx, h.client, h.scheme, nn,
		func() *upgradev1.Plan { return &upgradev1.Plan{} },
		func() *upgradev1.Plan { return constructPlanForNodeUpgrade(up, !single) },
		up,
	)
}

func (h *UpgradePlanPhaseHandler) deleteResource(
	ctx context.Context,
	obj client.Object,
	namespacedName types.NamespacedName,
) error {
	if err := h.client.Get(ctx, namespacedName, obj); err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}

		// Resource already gone, don't bother delete it again.
		return nil
	}

	if err := h.client.Delete(ctx, obj, &client.DeleteOptions{
		PropagationPolicy: ptr.To(metav1.DeletePropagationBackground),
	}); err != nil && !apierrors.IsNotFound(err) {
		return err
	}

	return nil
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

func constructVirtualMachineImage(upgradePlan *managementv1beta1.UpgradePlan) *harvesterv1beta1.VirtualMachineImage {
	imageName := fmt.Sprintf("%s-%s", upgradePlan.Name, imageComponent)
	vmImage := &harvesterv1beta1.VirtualMachineImage{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				HarvesterUpgradePlanLabel:      upgradePlan.Name,
				HarvesterUpgradeComponentLabel: imageComponent,
			},
			Name:      imageName,
			Namespace: harvesterSystemNamespace,
		},
		Spec: harvesterv1beta1.VirtualMachineImageSpec{
			Backend:     harvesterv1beta1.VMIBackendBackingImage,
			DisplayName: fmt.Sprintf("%s-%s", upgradePlan.Name, upgradePlan.Spec.Version),
			SourceType:  harvesterv1beta1.VirtualMachineImageSourceTypeDownload,
			URL:         upgradePlan.Status.Version.ISODownloadURL,
			Checksum:    ptr.Deref(upgradePlan.Status.Version.ISOChecksum, ""),
			Retry:       3,
		},
	}
	return vmImage
}

func constructPersistentVolumeClaim(upgradePlan *managementv1beta1.UpgradePlan) *corev1.PersistentVolumeClaim {
	pvcName := fmt.Sprintf("%s-%s", upgradePlan.Name, repoComponent)
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				HarvesterUpgradePlanLabel:      upgradePlan.Name,
				HarvesterUpgradeComponentLabel: repoComponent,
			},
			Name:      pvcName,
			Namespace: harvesterSystemNamespace,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{
				corev1.ReadWriteMany,
			},
			VolumeMode: ptr.To(corev1.PersistentVolumeFilesystem),
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: *resource.NewQuantity(10*1024*1024*1024, resource.BinarySI),
				},
			},
			StorageClassName: ptr.To(longhornStaticStorageClassName),
		},
	}
	return pvc
}

func constructDaemonSet(
	upgradePlan *managementv1beta1.UpgradePlan,
	persistentVolumeClaim *corev1.PersistentVolumeClaim,
	cert string,
) *appsv1.DaemonSet {
	dsName := fmt.Sprintf("%s-%s", upgradePlan.Name, repoComponent)
	vmImageNamespace, vmImageName, _ := strings.Cut(
		ptr.Deref(upgradePlan.Status.ISOImageID, "nonexistent/nonexistent"),
		"/",
	)

	var (
		t   *template.Template
		buf bytes.Buffer
	)
	t = template.Must(template.New("script").Parse(isoDownloaderScriptTemplate))
	_ = t.Execute(&buf, nil)
	renderedISODownloadScript := buf.String()

	ds := &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				HarvesterUpgradePlanLabel:      upgradePlan.Name,
				HarvesterUpgradeComponentLabel: repoComponent,
			},
			Name:      dsName,
			Namespace: harvesterSystemNamespace,
		},
		Spec: appsv1.DaemonSetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					HarvesterUpgradePlanLabel:      upgradePlan.Name,
					HarvesterUpgradeComponentLabel: repoComponent,
				},
			},
			UpdateStrategy: appsv1.DaemonSetUpdateStrategy{
				Type: appsv1.RollingUpdateDaemonSetStrategyType,
				RollingUpdate: &appsv1.RollingUpdateDaemonSet{
					MaxUnavailable: &intstr.IntOrString{
						IntVal: 1,
					},
				},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						HarvesterUpgradePlanLabel:      upgradePlan.Name,
						HarvesterUpgradeComponentLabel: repoComponent,
					},
				},
				Spec: corev1.PodSpec{
					Tolerations: []corev1.Toleration{
						{
							Key:      "node-role.kubernetes.io/control-plane",
							Operator: corev1.TolerationOpExists,
							Effect:   corev1.TaintEffectNoSchedule,
						},
						{
							Key:      "node-role.kubernetes.io/master",
							Operator: corev1.TolerationOpExists,
							Effect:   corev1.TaintEffectNoSchedule,
						},
					},
					InitContainers: []corev1.Container{
						{
							Name:  "iso-downloader",
							Image: fmt.Sprintf("%s:%s", upgradeToolkitImage, getPreviousVersion(upgradePlan)),
							Command: []string{
								"sh",
								"-c",
								renderedISODownloadScript,
							},
							Env: []corev1.EnvVar{
								{
									Name: "POD_NAME",
									ValueFrom: &corev1.EnvVarSource{
										FieldRef: &corev1.ObjectFieldSelector{
											FieldPath: "metadata.name",
										},
									},
								},
								{
									Name:  "VM_IMAGE_NS",
									Value: vmImageNamespace,
								},
								{
									Name:  "VM_IMAGE_NAME",
									Value: vmImageName,
								},
								{
									Name:  "CA_CERT",
									Value: cert,
								},
							},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "iso",
									MountPath: "/iso",
								},
							},
						},
						{
							Name:  "preloader",
							Image: fmt.Sprintf("%s:%s", upgradeToolkitImage, getPreviousVersion(upgradePlan)),
							Command: []string{
								"sh",
								"-c",
								preloaderScript,
							},
							SecurityContext: &corev1.SecurityContext{
								Privileged: ptr.To(true),
							},
							Env: []corev1.EnvVar{
								{
									Name:  "HOST_DIR",
									Value: "/host",
								},
							},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "host-root",
									MountPath: "/host",
								},
								{
									Name:      "iso",
									MountPath: "/iso",
								},
							},
						},
					},
					Containers: []corev1.Container{
						{
							Name:  "iso-mounter",
							Image: fmt.Sprintf("%s:%s", upgradeToolkitImage, getUpgradeVersion(upgradePlan)),
							Command: []string{
								"sh",
								"-c",
								isoMounterScript,
							},
							SecurityContext: &corev1.SecurityContext{
								Privileged: ptr.To(true),
							},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "iso",
									MountPath: "/iso",
								},
								{
									Name:             "share-mount",
									MountPath:        "/share-mount",
									MountPropagation: ptr.To(corev1.MountPropagationBidirectional),
								},
							},
						},
						{
							Name:  "repo",
							Image: fmt.Sprintf("%s:%s", upgradeToolkitImage, getUpgradeVersion(upgradePlan)),
							Command: []string{
								"sh",
								"-c",
								repoScript,
							},
							Ports: []corev1.ContainerPort{
								{
									ContainerPort: 80,
								},
							},
							LivenessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									Exec: &corev1.ExecAction{
										Command: []string{
											"sh",
											"-c",
											"cat /srv/www/htdocs/harvester-release.yaml 2>&1 /dev/null",
										},
									},
								},
								PeriodSeconds:    10,
								TimeoutSeconds:   5,
								FailureThreshold: 3,
							},
							ReadinessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									HTTPGet: &corev1.HTTPGetAction{
										Path: "/harvester-release.yaml",
										Port: intstr.FromInt(80),
									},
								},
								InitialDelaySeconds: 5,
								PeriodSeconds:       10,
								TimeoutSeconds:      5,
								SuccessThreshold:    1,
								FailureThreshold:    1,
							},
							SecurityContext: &corev1.SecurityContext{
								Privileged: ptr.To(true),
							},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:             "share-mount",
									MountPath:        "/srv/www/htdocs",
									MountPropagation: ptr.To(corev1.MountPropagationBidirectional),
								},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "host-root",
							VolumeSource: corev1.VolumeSource{
								HostPath: &corev1.HostPathVolumeSource{
									Path: "/",
								},
							},
						},
						{
							Name: "iso",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: persistentVolumeClaim.Name,
								},
							},
						},
						{
							Name: "share-mount",
							VolumeSource: corev1.VolumeSource{
								EmptyDir: &corev1.EmptyDirVolumeSource{},
							},
						},
					},
				},
			},
		},
	}
	return ds
}

func constructService(upgradePlan *managementv1beta1.UpgradePlan) *corev1.Service {
	svcName := fmt.Sprintf("%s-%s", upgradePlan.Name, repoComponent)
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				HarvesterUpgradePlanLabel:      upgradePlan.Name,
				HarvesterUpgradeComponentLabel: repoComponent,
			},
			Name:      svcName,
			Namespace: harvesterSystemNamespace,
		},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{
				{
					Port:       80,
					Protocol:   corev1.ProtocolTCP,
					TargetPort: intstr.FromInt(80),
				},
			},
			Selector: map[string]string{
				HarvesterUpgradePlanLabel:      upgradePlan.Name,
				HarvesterUpgradeComponentLabel: repoComponent,
			},
		},
	}
	return svc
}

func constructJobForClusterUpgrade(upgradePlan *managementv1beta1.UpgradePlan) *batchv1.Job {
	jobName := fmt.Sprintf("%s-%s", upgradePlan.Name, ClusterComponent)
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				HarvesterUpgradePlanLabel:      upgradePlan.Name,
				HarvesterUpgradeComponentLabel: ClusterComponent,
			},
			Name:      jobName,
			Namespace: harvesterSystemNamespace,
		},
		Spec: batchv1.JobSpec{
			TTLSecondsAfterFinished: ptr.To[int32](defaultTTLSecondsAfterFinished),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						HarvesterUpgradePlanLabel:      upgradePlan.Name,
						HarvesterUpgradeComponentLabel: ClusterComponent,
					},
				},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{
						{
							Name:  "apply",
							Image: fmt.Sprintf("%s:%s", upgradeToolkitImage, getUpgradeVersion(upgradePlan)),
							Command: []string{
								"upgrade_manifests.sh",
							},
							Env: []corev1.EnvVar{
								{
									Name:  "HARVESTER_UPGRADEPLAN_NAME",
									Value: upgradePlan.Name,
								},
							},
						},
					},
					ServiceAccountName: harvesterName,
					Tolerations:        getDefaultTolerations(),
				},
			},
		},
	}
	return job
}

func constructPlan(
	upgradePlanName, componentName string,
	concurrency int,
	nodeSelector *metav1.LabelSelector,
	maintenance bool,
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

func constructPlanForImagePreload(upgradePlan *managementv1beta1.UpgradePlan) *upgradev1.Plan {
	selector := &metav1.LabelSelector{
		MatchLabels: map[string]string{
			harvesterManagedLabel: "true",
		},
	}
	container := &upgradev1.ContainerSpec{
		Image:   upgradeToolkitImage,
		Command: []string{"upgrade_node.sh"},
		Args:    []string{"prepare"},
		Env: []corev1.EnvVar{
			{
				Name:  "HARVESTER_UPGRADEPLAN_NAME",
				Value: upgradePlan.Name,
			},
		},
	}
	version := getUpgradeVersion(upgradePlan)

	return constructPlan(upgradePlan.Name, PrepareComponent, 1, selector, false, container, version)
}

func constructPlanForNodeUpgrade(upgradePlan *managementv1beta1.UpgradePlan, maintenance bool) *upgradev1.Plan {
	selector := &metav1.LabelSelector{
		MatchExpressions: []metav1.LabelSelectorRequirement{
			{
				Key:      "node-role.kubernetes.io/control-plane",
				Operator: metav1.LabelSelectorOpIn,
				Values: []string{
					"true",
				},
			},
		},
	}
	container := &upgradev1.ContainerSpec{
		Image: rke2UpgradeImage,
	}
	version := getKubernetesVersion(upgradePlan)

	return constructPlan(upgradePlan.Name, NodeComponent, 1, selector, maintenance, container, version)
}

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
