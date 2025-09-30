package upgradeplan

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	harvesterv1beta1 "github.com/harvester/harvester/pkg/apis/harvesterhci.io/v1beta1"
	"github.com/harvester/harvester/pkg/settings"
	upgradev1 "github.com/rancher/system-upgrade-controller/pkg/apis/upgrade.cattle.io/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	managementv1beta1 "github.com/harvester/upgrade-toolkit/api/v1beta1"
)

const (
	harvesterSystemNamespace = "harvester-system"
	cattleSystemNamespace    = "cattle-system"
	harvesterName            = "harvester"
	sucName                  = "system-upgrade-controller"

	harvesterManagedLabel = "harvesterhci.io/managed"
	imageComponent        = "iso"

	defaultTTLSecondsAfterFinished = 604800 // 7 days

	rke2UpgradeImage    = "rancher/rke2-upgrade"
	upgradeToolkitImage = "rancher/harvester-upgrade"
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

	upgradePlan.SetCondition(managementv1beta1.UpgradePlanAvailable, metav1.ConditionTrue, "Executable", "")
	updateProgressingPhase(upgradePlan, managementv1beta1.UpgradePlanPhaseInitializing, "")

	if err := h.loadVersion(ctx, upgradePlan); err != nil {
		return ctrl.Result{}, err
	}

	upgradePlan.Status.PreviousVersion = ptr.To(settings.ServerVersion.Get())
	updateProgressingPhase(upgradePlan, managementv1beta1.UpgradePlanPhaseInitialized, "")
	return ctrl.Result{}, nil
}

func (h *UpgradePlanPhaseHandler) isoDownload(
	ctx context.Context,
	upgradePlan *managementv1beta1.UpgradePlan,
) (ctrl.Result, error) {
	h.log.V(1).Info("handle iso download")

	vmimage, err := h.getOrCreateVirtualMachineImage(ctx, upgradePlan)
	if err != nil {
		h.log.Error(err, "unable to retrieve vmimage from upgradeplan")
		return ctrl.Result{}, err
	}
	if upgradePlan.Status.ISOImageID == nil {
		upgradePlan.Status.ISOImageID = ptr.To(fmt.Sprintf("%s/%s", vmimage.Namespace, vmimage.Name))
	}

	imported, success := isVirtualMachineImageImported(vmimage)

	// vmimage download still ongoing
	if !imported {
		h.log.V(1).Info("iso image downloading")
		upgradePlan.Status.Phase = managementv1beta1.UpgradePlanPhaseISODownloading
		return ctrl.Result{}, nil
	}

	// vmimage download finished but failed
	if !success {
		h.log.V(0).Info("iso image download failed")
		upgradePlan.Status.Phase = managementv1beta1.UpgradePlanPhaseFailed
		return ctrl.Result{}, nil
	}

	// vmimage download finished successfully
	upgradePlan.Status.Phase = managementv1beta1.UpgradePlanPhaseISODownloaded
	return ctrl.Result{}, nil
}

func (h *UpgradePlanPhaseHandler) repoCreate(upgradePlan *managementv1beta1.UpgradePlan) (ctrl.Result, error) {
	h.log.V(1).Info("handle repo create")

	// TODO: Repo creation
	if upgradePlan.Status.Phase == managementv1beta1.UpgradePlanPhaseRepoCreating {
		upgradePlan.Status.Phase = managementv1beta1.UpgradePlanPhaseRepoCreated
		return ctrl.Result{}, nil
	}
	upgradePlan.Status.Phase = managementv1beta1.UpgradePlanPhaseRepoCreating
	return ctrl.Result{}, nil
}

// metadataPopulate populates UpgradePlan with release metadata retrieved from the Upgrade Repo
func (h *UpgradePlanPhaseHandler) metadataPopulate(upgradePlan *managementv1beta1.UpgradePlan) (ctrl.Result, error) {
	h.log.V(1).Info("handle metadata populate")

	harvesterRelease := newHarvesterRelease(upgradePlan)
	if err := harvesterRelease.loadReleaseMetadata(); err != nil {
		upgradePlan.Status.Phase = managementv1beta1.UpgradePlanPhaseMetadataPopulating
		return ctrl.Result{}, err
	}
	upgradePlan.Status.ReleaseMetadata = harvesterRelease.ReleaseMetadata

	upgradePlan.Status.Phase = managementv1beta1.UpgradePlanPhaseMetadataPopulated
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

	finished := isPlanFinished(imagePreloadPlan)

	// Plan still running
	if !finished {
		h.log.V(1).Info("image-preload plan running")
		upgradePlan.Status.Phase = managementv1beta1.UpgradePlanPhaseImagePreloading
		return ctrl.Result{}, nil
	}

	// Plan finished successfully
	upgradePlan.Status.Phase = managementv1beta1.UpgradePlanPhaseImagePreloaded
	return ctrl.Result{}, nil
}

func (h *UpgradePlanPhaseHandler) clusterUpgrade(
	ctx context.Context,
	upgradePlan *managementv1beta1.UpgradePlan,
) (ctrl.Result, error) {
	h.log.V(1).Info("handle cluster upgrade")

	clusterUpgradeJob, err := h.getOrCreateJobForClusterUpgrade(ctx, upgradePlan)
	if err != nil {
		return ctrl.Result{}, err
	}

	finished, success := isJobFinished(clusterUpgradeJob)

	// Job still running
	if !finished {
		h.log.V(1).Info("cluster-upgrade job running")
		upgradePlan.Status.Phase = managementv1beta1.UpgradePlanPhaseClusterUpgrading
		return ctrl.Result{}, nil
	}

	// Job finished but failed
	if !success {
		h.log.V(0).Info("cluster-upgrade job failed")
		upgradePlan.Status.Phase = managementv1beta1.UpgradePlanPhaseFailed
		return ctrl.Result{}, nil
	}

	// Job finished successfully
	upgradePlan.Status.Phase = managementv1beta1.UpgradePlanPhaseClusterUpgraded
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

	finished := isPlanFinished(nodeUpgradePlan)

	// Plan still running
	if !finished {
		h.log.V(1).Info("node-upgrade plan running")
		upgradePlan.Status.Phase = managementv1beta1.UpgradePlanPhaseNodeUpgrading
		return ctrl.Result{}, nil
	}

	// Plan finished successfully
	upgradePlan.Status.Phase = managementv1beta1.UpgradePlanPhaseNodeUpgraded
	return ctrl.Result{}, nil
}

func (h *UpgradePlanPhaseHandler) resourceCleanup(upgradePlan *managementv1beta1.UpgradePlan) (ctrl.Result, error) {
	h.log.V(1).Info("handle resource cleanup")

	// Dummy resource cleanup
	if upgradePlan.Status.Phase == managementv1beta1.UpgradePlanPhaseCleaningUp {
		upgradePlan.Status.Phase = managementv1beta1.UpgradePlanPhaseCleanedUp
		return ctrl.Result{}, nil
	}
	upgradePlan.Status.Phase = managementv1beta1.UpgradePlanPhaseCleaningUp
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

func (h *UpgradePlanPhaseHandler) getOrCreateVirtualMachineImage(
	ctx context.Context,
	up *managementv1beta1.UpgradePlan,
) (*harvesterv1beta1.VirtualMachineImage, error) {
	nn := types.NamespacedName{
		Namespace: harvesterSystemNamespace,
		Name:      up.Name,
	}
	return getOrCreate(
		ctx, h.client, h.scheme, nn,
		func() *harvesterv1beta1.VirtualMachineImage { return &harvesterv1beta1.VirtualMachineImage{} },
		func() *harvesterv1beta1.VirtualMachineImage { return constructVirtualMachineImage(up) },
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

func constructVirtualMachineImage(upgradePlan *managementv1beta1.UpgradePlan) *harvesterv1beta1.VirtualMachineImage {
	vmimage := &harvesterv1beta1.VirtualMachineImage{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				HarvesterUpgradePlanLabel:      upgradePlan.Name,
				HarvesterUpgradeComponentLabel: imageComponent,
			},
			Name:      upgradePlan.Name,
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
	return vmimage
}

func constructJobForClusterUpgrade(upgradePlan *managementv1beta1.UpgradePlan) *batchv1.Job {
	jobName := fmt.Sprintf("%s-cluster-upgrade", upgradePlan.Name)
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
								// "upgrade_manifests.sh",
								"sleep",
								"30",
							},
							Env: []corev1.EnvVar{
								{
									Name:  "HARVESTER_UPGRADE_PLAN_NAME",
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
		Image: upgradeToolkitImage,
		// Command: []string{"do_upgrade_node.sh"},
		// Args:    []string{"prepare"},
		// Env: []corev1.EnvVar{
		// 	{
		// 		Name:  "HARVESTER_UPGRADEPLAN_NAME",
		// 		Value: upgradePlan.Name,
		// 	},
		// },
		Command: []string{
			"sleep",
			"30",
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
	upgradePlan.Status.Phase = phase

	if !upgradePlan.ConditionExists(managementv1beta1.UpgradePlanProgressing) {
		upgradePlan.SetCondition(managementv1beta1.UpgradePlanProgressing, metav1.ConditionTrue, string(phase), "")
	} else {
		cond := upgradePlan.LookupCondition(managementv1beta1.UpgradePlanProgressing)
		upgradePlan.SetCondition(managementv1beta1.UpgradePlanProgressing, cond.Status, string(phase), message)
	}
}

func isVirtualMachineImageImported(vmimage *harvesterv1beta1.VirtualMachineImage) (finished, success bool) {
	for _, condition := range vmimage.Status.Conditions {
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
