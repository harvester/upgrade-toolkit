/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"github.com/goccy/go-yaml"
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

	harvesterManagedLabel          = "harvesterhci.io/managed"
	harvesterUpgradePlanLabel      = "management.harvesterhci.io/upgrade-plan"
	harvesterUpgradeComponentLabel = "management.harvesterhci.io/upgrade-component"
	imageComponent                 = "iso"
	prepareComponent               = "image-preload"
	clusterComponent               = "cluster-upgrade"
	nodeComponent                  = "node-upgrade"

	defaultTTLSecondsAfterFinished = 604800 // 7 days

	rke2UpgradeImage    = "rancher/rke2-upgrade"
	upgradeToolkitImage = "rancher/harvester-upgrade"

	releaseURL = "http://localhost/harvester-release.yaml"
)

type harvesterRelease struct {
	ctx        context.Context
	httpClient *http.Client

	*managementv1beta1.UpgradePlan
	*managementv1beta1.ReleaseMetadata
}

func newHarvesterRelease(upgradePlan *managementv1beta1.UpgradePlan) *harvesterRelease {
	return &harvesterRelease{
		ctx: context.Background(),
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		UpgradePlan:     upgradePlan,
		ReleaseMetadata: &managementv1beta1.ReleaseMetadata{},
	}
}

func (h *harvesterRelease) loadReleaseMetadata() error {
	req, err := http.NewRequestWithContext(h.ctx, http.MethodGet, releaseURL, nil)
	if err != nil {
		return err
	}

	resp, err := h.httpClient.Do(req)
	if err != nil {
		return err
	}

	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status: %s", resp.Status)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	err = yaml.Unmarshal(body, h.ReleaseMetadata)
	if err != nil {
		return err
	}

	return nil
}

// UpgradePlanReconciler reconciles a UpgradePlan object
type UpgradePlanReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	Log    logr.Logger
}

// +kubebuilder:rbac:groups=management.harvesterhci.io,resources=upgradeplans,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=management.harvesterhci.io,resources=upgradeplans/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=management.harvesterhci.io,resources=upgradeplans/finalizers,verbs=update
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update
// +kubebuilder:rbac:groups=core,resources=nodes,verbs=get;list;watch
// +kubebuilder:rbac:groups=harvesterhci.io,resources=settings,verbs=get;list;watch
// +kubebuilder:rbac:groups=harvesterhci.io,resources=virtualmachineimages,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=upgrade.cattle.io,resources=plans,verbs=get;list;watch;create;update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the UpgradePlan object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.21.0/pkg/reconcile
func (r *UpgradePlanReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	r.Log.V(1).Info("reconciling upgradeplan")

	var upgradePlan managementv1beta1.UpgradePlan
	if err := r.Get(ctx, req.NamespacedName, &upgradePlan); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	upgradePlanCopy := upgradePlan.DeepCopy()

	// Handle deletion
	if !upgradePlan.DeletionTimestamp.IsZero() {
		return r.handleResourceCleanup(upgradePlanCopy)
	}

	// Phase-based reconciliation
	result, err := r.reconcilePhase(ctx, upgradePlanCopy)
	if err != nil {
		upgradePlanCopy.SetCondition(managementv1beta1.UpgradePlanDegraded, metav1.ConditionTrue, "ReconcileError", err.Error())
	} else {
		upgradePlanCopy.SetCondition(managementv1beta1.UpgradePlanDegraded, metav1.ConditionFalse, "ReconcileSuccess", "")
	}

	setUpgradePlanPhaseTransitionTimestamp(&upgradePlan, upgradePlanCopy)

	if !reflect.DeepEqual(upgradePlan.Status, upgradePlanCopy.Status) {
		if statusUpdateErr := r.Status().Update(ctx, upgradePlanCopy); statusUpdateErr != nil {
			if apierrors.IsConflict(statusUpdateErr) {
				return ctrl.Result{Requeue: true}, nil
			}
			return ctrl.Result{}, statusUpdateErr
		}
	}

	return result, err
}

// SetupWithManager sets up the controller with the Manager.
func (r *UpgradePlanReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&managementv1beta1.UpgradePlan{}).
		Owns(&batchv1.Job{}).
		Owns(&harvesterv1beta1.VirtualMachineImage{}).
		Owns(&upgradev1.Plan{}).
		Named("upgradeplan").
		Complete(r)
}

func (r *UpgradePlanReconciler) reconcilePhase(ctx context.Context, upgradePlan *managementv1beta1.UpgradePlan) (ctrl.Result, error) {
	switch upgradePlan.Status.Phase {
	case "":
		return r.initialize(ctx, upgradePlan)
	case managementv1beta1.UpgradePlanPhaseInit, managementv1beta1.UpgradePlanPhaseISODownloading:
		return r.handleISODownload(ctx, upgradePlan)
	case managementv1beta1.UpgradePlanPhaseISODownloaded, managementv1beta1.UpgradePlanPhaseRepoCreating:
		return r.handleRepoCreate(upgradePlan)
	case managementv1beta1.UpgradePlanPhaseRepoCreated, managementv1beta1.UpgradePlanPhaseMetadataPopulating:
		return r.handleMetadataPopulate(upgradePlan)
	case managementv1beta1.UpgradePlanPhaseMetadataPopulated, managementv1beta1.UpgradePlanPhaseImagePreloading:
		return r.handleImagePreload(ctx, upgradePlan)
	case managementv1beta1.UpgradePlanPhaseImagePreloaded, managementv1beta1.UpgradePlanPhaseClusterUpgrading:
		return r.handleClusterUpgrade(ctx, upgradePlan)
	case managementv1beta1.UpgradePlanPhaseClusterUpgraded, managementv1beta1.UpgradePlanPhaseNodeUpgrading:
		return r.handleNodeUpgrade(ctx, upgradePlan)
	case managementv1beta1.UpgradePlanPhaseNodeUpgraded, managementv1beta1.UpgradePlanPhaseCleaningUp:
		return r.handleResourceCleanup(upgradePlan)
	default:
		return r.finalize(upgradePlan)
	}
}

func (r *UpgradePlanReconciler) initialize(ctx context.Context, upgradePlan *managementv1beta1.UpgradePlan) (ctrl.Result, error) {
	r.Log.V(1).Info("handle initialize status")

	if err := r.loadVersion(ctx, upgradePlan); err != nil {
		return ctrl.Result{}, err
	}

	upgradePlan.Status.PreviousVersion = ptr.To(settings.ServerVersion.Get())
	upgradePlan.SetCondition(managementv1beta1.UpgradePlanAvailable, metav1.ConditionTrue, "Initialized", "")
	upgradePlan.SetCondition(managementv1beta1.UpgradePlanProgressing, metav1.ConditionTrue, string(managementv1beta1.UpgradePlanPhaseInit), "")
	upgradePlan.Status.Phase = managementv1beta1.UpgradePlanPhaseInit
	return ctrl.Result{}, nil
}

func (r *UpgradePlanReconciler) handleISODownload(ctx context.Context, upgradePlan *managementv1beta1.UpgradePlan) (ctrl.Result, error) {
	r.Log.V(1).Info("handle iso download")

	vmimage, err := r.getOrCreateVirtualMachineImage(ctx, upgradePlan)
	if err != nil {
		r.Log.Error(err, "unable to retrieve vmimage from upgradeplan")
		return ctrl.Result{}, err
	}
	if upgradePlan.Status.ISOImageID == nil {
		upgradePlan.Status.ISOImageID = ptr.To(fmt.Sprintf("%s/%s", vmimage.Namespace, vmimage.Name))
	}

	imported, success := isVirtualMachineImageImported(vmimage)

	// vmimage download still ongoing
	if !imported {
		r.Log.V(1).Info("iso image downloading")
		upgradePlan.Status.Phase = managementv1beta1.UpgradePlanPhaseISODownloading
		return ctrl.Result{}, nil
	}

	// vmimage download finished but failed
	if !success {
		r.Log.V(0).Info("iso image download failed")
		upgradePlan.Status.Phase = managementv1beta1.UpgradePlanPhaseFailed
		return ctrl.Result{}, nil
	}

	// vmimage download finished successfully
	upgradePlan.Status.Phase = managementv1beta1.UpgradePlanPhaseISODownloaded
	return ctrl.Result{}, nil
}

func (r *UpgradePlanReconciler) handleRepoCreate(upgradePlan *managementv1beta1.UpgradePlan) (ctrl.Result, error) {
	r.Log.V(1).Info("handle repo create")

	// TODO: Repo creation
	if upgradePlan.Status.Phase == managementv1beta1.UpgradePlanPhaseRepoCreating {
		upgradePlan.Status.Phase = managementv1beta1.UpgradePlanPhaseRepoCreated
		return ctrl.Result{}, nil
	}
	upgradePlan.Status.Phase = managementv1beta1.UpgradePlanPhaseRepoCreating
	return ctrl.Result{}, nil
}

// handleMetadataPopulate populates UpgradePlan with release metadata retrieved from the Upgrade Repo
func (r *UpgradePlanReconciler) handleMetadataPopulate(upgradePlan *managementv1beta1.UpgradePlan) (ctrl.Result, error) {
	r.Log.V(1).Info("handle metadata populate")

	harvesterRelease := newHarvesterRelease(upgradePlan)
	if err := harvesterRelease.loadReleaseMetadata(); err != nil {
		upgradePlan.Status.Phase = managementv1beta1.UpgradePlanPhaseMetadataPopulating
		return ctrl.Result{}, err
	}
	upgradePlan.Status.ReleaseMetadata = harvesterRelease.ReleaseMetadata

	upgradePlan.Status.Phase = managementv1beta1.UpgradePlanPhaseMetadataPopulated
	return ctrl.Result{}, nil
}

func (r *UpgradePlanReconciler) handleImagePreload(ctx context.Context, upgradePlan *managementv1beta1.UpgradePlan) (ctrl.Result, error) {
	r.Log.V(1).Info("handle image preload")

	imagePreloadPlan, err := r.getOrCreatePlanForImagePreload(ctx, upgradePlan)
	if err != nil {
		r.Log.Error(err, "unable to retrieve image-preload plan from upgradeplan")
		return ctrl.Result{}, err
	}

	finished := isPlanFinished(imagePreloadPlan)

	// Plan still running
	if !finished {
		r.Log.V(1).Info("image-preload plan running")
		upgradePlan.Status.Phase = managementv1beta1.UpgradePlanPhaseImagePreloading
		return ctrl.Result{}, nil
	}

	// Plan finished successfully
	upgradePlan.Status.Phase = managementv1beta1.UpgradePlanPhaseImagePreloaded
	return ctrl.Result{}, nil
}

func (r *UpgradePlanReconciler) handleClusterUpgrade(ctx context.Context, upgradePlan *managementv1beta1.UpgradePlan) (ctrl.Result, error) {
	r.Log.V(1).Info("handle cluster upgrade")

	clusterUpgradeJob, err := r.getOrCreateJobForClusterUpgrade(ctx, upgradePlan)
	if err != nil {
		return ctrl.Result{}, err
	}

	finished, success := isJobFinished(clusterUpgradeJob)

	// Job still running
	if !finished {
		r.Log.V(1).Info("cluster-upgrade job running")
		upgradePlan.Status.Phase = managementv1beta1.UpgradePlanPhaseClusterUpgrading
		return ctrl.Result{}, nil
	}

	// Job finished but failed
	if !success {
		r.Log.V(0).Info("cluster-upgrade job failed")
		upgradePlan.Status.Phase = managementv1beta1.UpgradePlanPhaseFailed
		return ctrl.Result{}, nil
	}

	// Job finished successfully
	upgradePlan.Status.Phase = managementv1beta1.UpgradePlanPhaseClusterUpgraded
	return ctrl.Result{}, nil
}

func (r *UpgradePlanReconciler) handleNodeUpgrade(ctx context.Context, upgradePlan *managementv1beta1.UpgradePlan) (ctrl.Result, error) {
	r.Log.V(1).Info("handle node upgrade")

	nodeUpgradePlan, err := r.getOrCreatePlanForNodeUpgrade(ctx, upgradePlan)
	if err != nil {
		r.Log.Error(err, "unable to retrieve node-upgrade plan from upgradeplan")
		return ctrl.Result{}, err
	}

	finished := isPlanFinished(nodeUpgradePlan)

	// Plan still running
	if !finished {
		r.Log.V(1).Info("node-upgrade plan running")
		upgradePlan.Status.Phase = managementv1beta1.UpgradePlanPhaseNodeUpgrading
		return ctrl.Result{}, nil
	}

	// Plan finished successfully
	upgradePlan.Status.Phase = managementv1beta1.UpgradePlanPhaseNodeUpgraded
	return ctrl.Result{}, nil
}

func (r *UpgradePlanReconciler) handleResourceCleanup(upgradePlan *managementv1beta1.UpgradePlan) (ctrl.Result, error) {
	r.Log.V(1).Info("handle resource cleanup")

	// Dummy resource cleanup
	if upgradePlan.Status.Phase == managementv1beta1.UpgradePlanPhaseCleaningUp {
		upgradePlan.Status.Phase = managementv1beta1.UpgradePlanPhaseCleanedUp
		return ctrl.Result{}, nil
	}
	upgradePlan.Status.Phase = managementv1beta1.UpgradePlanPhaseCleaningUp
	return ctrl.Result{}, nil
}

func (r *UpgradePlanReconciler) finalize(upgradePlan *managementv1beta1.UpgradePlan) (ctrl.Result, error) {
	r.Log.V(1).Info("handle finalize")

	markUpgradePlanComplete(upgradePlan)

	upgradePlan.Status.Phase = managementv1beta1.UpgradePlanPhaseSucceeded
	return ctrl.Result{}, nil
}

func (r *UpgradePlanReconciler) loadVersion(ctx context.Context, upgradePlan *managementv1beta1.UpgradePlan) error {
	var version managementv1beta1.Version
	if err := r.Get(ctx, types.NamespacedName{Name: upgradePlan.Spec.Version}, &version); err != nil {
		return err
	}
	upgradePlan.Status.Version = &version.Spec
	return nil
}

func (r *UpgradePlanReconciler) isSingleNodeCluster(ctx context.Context) (bool, error) {
	var nodeList corev1.NodeList
	if err := r.List(ctx, &nodeList, &client.ListOptions{
		LabelSelector: labels.SelectorFromSet(labels.Set{
			harvesterManagedLabel: "true",
		}),
	}); err != nil {
		return false, err
	}
	return len(nodeList.Items) == 1, nil
}

func (r *UpgradePlanReconciler) getOrCreateVirtualMachineImage(
	ctx context.Context,
	up *managementv1beta1.UpgradePlan,
) (*harvesterv1beta1.VirtualMachineImage, error) {
	nn := types.NamespacedName{
		Namespace: harvesterSystemNamespace,
		Name:      up.Name,
	}
	return getOrCreate(
		ctx, r.Client, r.Scheme, nn,
		func() *harvesterv1beta1.VirtualMachineImage { return &harvesterv1beta1.VirtualMachineImage{} },
		func() *harvesterv1beta1.VirtualMachineImage { return constructVirtualMachineImage(up) },
		up,
	)
}

func (r *UpgradePlanReconciler) getOrCreatePlanForImagePreload(
	ctx context.Context,
	up *managementv1beta1.UpgradePlan,
) (*upgradev1.Plan, error) {
	nn := types.NamespacedName{
		Namespace: cattleSystemNamespace,
		Name:      fmt.Sprintf("%s-%s", up.Name, prepareComponent),
	}
	return getOrCreate(
		ctx, r.Client, r.Scheme, nn,
		func() *upgradev1.Plan { return &upgradev1.Plan{} },
		func() *upgradev1.Plan { return constructPlanForImagePreload(up) },
		up,
	)
}

func (r *UpgradePlanReconciler) getOrCreateJobForClusterUpgrade(
	ctx context.Context,
	up *managementv1beta1.UpgradePlan,
) (*batchv1.Job, error) {
	nn := types.NamespacedName{
		Namespace: harvesterSystemNamespace,
		Name:      fmt.Sprintf("%s-%s", up.Name, clusterComponent),
	}
	return getOrCreate(
		ctx, r.Client, r.Scheme, nn,
		func() *batchv1.Job { return &batchv1.Job{} },
		func() *batchv1.Job { return constructJobForClusterUpgrade(up) },
		up,
	)
}

func (r *UpgradePlanReconciler) getOrCreatePlanForNodeUpgrade(
	ctx context.Context,
	up *managementv1beta1.UpgradePlan,
) (*upgradev1.Plan, error) {
	nn := types.NamespacedName{
		Namespace: cattleSystemNamespace,
		Name:      fmt.Sprintf("%s-%s", up.Name, nodeComponent),
	}
	single, err := r.isSingleNodeCluster(ctx)
	if err != nil {
		return nil, err
	}
	return getOrCreate(
		ctx, r.Client, r.Scheme, nn,
		func() *upgradev1.Plan { return &upgradev1.Plan{} },
		func() *upgradev1.Plan { return constructPlanForNodeUpgrade(up, !single) },
		up,
	)
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

func isTerminalPhase(phase managementv1beta1.UpgradePlanPhase) bool {
	return phase == managementv1beta1.UpgradePlanPhaseSucceeded ||
		phase == managementv1beta1.UpgradePlanPhaseFailed
}

// markUpgradePlanComplete marks UpgradePlan as no longer being processed due to reaching one of the terminating phases.
func markUpgradePlanComplete(upgradePlan *managementv1beta1.UpgradePlan) {
	if !isTerminalPhase(upgradePlan.Status.Phase) {
		upgradePlan.SetCondition(managementv1beta1.UpgradePlanAvailable, metav1.ConditionFalse, "Executed", "")
		upgradePlan.SetCondition(managementv1beta1.UpgradePlanProgressing, metav1.ConditionFalse, string(upgradePlan.Status.Phase), "UpgradePlan has completed")
	}
}

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

func getUpgradeVersion(upgradePlan *managementv1beta1.UpgradePlan) string {
	if upgradePlan != nil && upgradePlan.Spec.Upgrade != nil {
		return *upgradePlan.Spec.Upgrade
	}
	return upgradePlan.Spec.Version
}

func constructVirtualMachineImage(upgradePlan *managementv1beta1.UpgradePlan) *harvesterv1beta1.VirtualMachineImage {
	vmimage := &harvesterv1beta1.VirtualMachineImage{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				harvesterUpgradePlanLabel:      upgradePlan.Name,
				harvesterUpgradeComponentLabel: imageComponent,
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
				harvesterUpgradePlanLabel:      upgradePlan.Name,
				harvesterUpgradeComponentLabel: clusterComponent,
			},
			Name:      jobName,
			Namespace: harvesterSystemNamespace,
		},
		Spec: batchv1.JobSpec{
			TTLSecondsAfterFinished: ptr.To[int32](defaultTTLSecondsAfterFinished),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						harvesterUpgradePlanLabel:      upgradePlan.Name,
						harvesterUpgradeComponentLabel: clusterComponent,
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

func sanitizedVersion(version string) string {
	return strings.Replace(version, "+", "-", 1)
}

func getKubernetesVersion(upgradePlan *managementv1beta1.UpgradePlan) string {
	if upgradePlan != nil && upgradePlan.Status.ReleaseMetadata != nil {
		return sanitizedVersion(upgradePlan.Status.ReleaseMetadata.Kubernetes)
	}
	return ""
}

func constructPlan(upgradePlanName, componentName string, concurrency int, nodeSelector *metav1.LabelSelector, maintenance bool, container *upgradev1.ContainerSpec, version string) *upgradev1.Plan {
	planName := fmt.Sprintf("%s-%s", upgradePlanName, componentName)

	plan := &upgradev1.Plan{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				harvesterUpgradePlanLabel:      upgradePlanName,
				harvesterUpgradeComponentLabel: componentName,
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

	return constructPlan(upgradePlan.Name, prepareComponent, 1, selector, false, container, version)
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

	return constructPlan(upgradePlan.Name, nodeComponent, 1, selector, maintenance, container, version)
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

func setUpgradePlanPhaseTransitionTimestamp(oldUpgradePlan *managementv1beta1.UpgradePlan, newUpgradePlan *managementv1beta1.UpgradePlan) {
	if oldUpgradePlan.Status.Phase == newUpgradePlan.Status.Phase {
		for _, transitionTimestamp := range newUpgradePlan.Status.PhaseTransitionTimestamps {
			if transitionTimestamp.Phase == newUpgradePlan.Status.Phase {
				return
			}
		}
	}

	now := metav1.NewTime(time.Now())
	newUpgradePlan.Status.PhaseTransitionTimestamps = append(newUpgradePlan.Status.PhaseTransitionTimestamps, managementv1beta1.UpgradePlanPhaseTransitionTimestamp{
		Phase:                    newUpgradePlan.Status.Phase,
		PhaseTransitionTimestamp: now,
	})
}
