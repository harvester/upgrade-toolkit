package upgradeplan

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/go-logr/logr"
	harvesterv1beta1 "github.com/harvester/harvester/pkg/apis/harvesterhci.io/v1beta1"
	lhv1beta2 "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	provisioningv1 "github.com/rancher/rancher/pkg/apis/provisioning.cattle.io/v1"
	rkev1 "github.com/rancher/rancher/pkg/apis/rke.cattle.io/v1"
	"github.com/rancher/wrangler/v3/pkg/name"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	managementv1beta1 "github.com/harvester/upgrade-toolkit/api/v1beta1"
	"github.com/harvester/upgrade-toolkit/pkg/upgradehelper/vmlivemigratedetector"
)

// NodeUpgradePhase triggers Rancher V2 Provisioning to upgrade each node's
// Kubernetes runtime, using pre/post-drain hooks for custom workloads.
type NodeUpgradePhase struct {
	*PhaseDeps
}

func NewNodeUpgradePhase(deps *PhaseDeps) *NodeUpgradePhase {
	return &NodeUpgradePhase{PhaseDeps: deps}
}

func (p *NodeUpgradePhase) Name() string { return "NodeUpgrade" }

// PreRun initializes NodeUpgradeStatuses for all Harvester-managed nodes,
// extends Longhorn's replica replenishment wait interval, and disables the
// descheduler addon to prevent interference during node upgrades.
func (p *NodeUpgradePhase) PreRun(
	ctx context.Context,
	upgradePlan *managementv1beta1.UpgradePlan,
) error {
	if upgradePlan.Status.NodeUpgradeStatuses == nil {
		upgradePlan.Status.NodeUpgradeStatuses = make(map[string]managementv1beta1.NodeUpgradeStatus)
	}

	nodes, err := listManagedNodes(ctx, p.Client)
	if err != nil {
		return err
	}

	for _, node := range nodes {
		if _, exists := upgradePlan.Status.NodeUpgradeStatuses[node.Name]; !exists {
			upgradePlan.Status.NodeUpgradeStatuses[node.Name] = managementv1beta1.NodeUpgradeStatus{
				State: managementv1beta1.NodeStateImagePreloaded,
			}
		}
	}

	if err := p.extendLonghornReplicaReplenishmentInterval(ctx, upgradePlan); err != nil {
		return err
	}

	if err := p.disableDeschedulerAddon(ctx, upgradePlan); err != nil {
		return err
	}

	if err := p.ensureSkipManifestsApplied(ctx, upgradePlan); err != nil {
		return err
	}

	return nil
}

// ensureSkipManifestsApplied creates an SUC Plan to place .skip files on all
// managed nodes before multi-node upgrades begin. It returns an error when the
// Plan has not completed yet, which blocks the phase from being entered.
func (p *NodeUpgradePhase) ensureSkipManifestsApplied(
	ctx context.Context,
	upgradePlan *managementv1beta1.UpgradePlan,
) error {
	if upgradePlan.Status.SingleNode != nil {
		return nil
	}

	if _, ok := upgradePlan.Annotations[AnnotationSkipManifestsApplied]; ok {
		return nil
	}

	nodeCount, err := countManagedNodes(ctx, p.Client)
	if err != nil {
		return err
	}

	waiting, failed, err := ensureSkipManifestPlanCompleted(
		ctx, p.Client, p.Scheme, upgradePlan, true, p.PlanServiceAccount, nodeCount,
	)
	if err != nil {
		return err
	}
	if failed {
		updateProgressingPhase(upgradePlan, managementv1beta1.UpgradePlanPhaseFailed,
			"skip-manifest apply plan job(s) failed")
		return nil
	}
	if waiting {
		return fmt.Errorf("waiting for skip-manifest apply plan to complete")
	}

	if upgradePlan.Annotations == nil {
		upgradePlan.Annotations = make(map[string]string)
	}
	upgradePlan.Annotations[AnnotationSkipManifestsApplied] = valueTrue
	return nil
}

// extendLonghornReplicaReplenishmentInterval saves the current Longhorn
// replica-replenishment-wait-interval and extends it to prevent unnecessary
// replica rebuilding during node upgrades.
func (p *NodeUpgradePhase) extendLonghornReplicaReplenishmentInterval(
	ctx context.Context,
	upgradePlan *managementv1beta1.UpgradePlan,
) error {
	if _, ok := upgradePlan.Annotations[AnnotationReplicaReplenishmentOriginal]; ok {
		return nil
	}

	var setting lhv1beta2.Setting
	log := logr.FromContextOrDiscard(ctx)
	if err := p.Client.Get(ctx, types.NamespacedName{
		Namespace: LonghornSystemNamespace,
		Name:      LonghornSettingReplicaReplenishment,
	}, &setting); err != nil {
		if apierrors.IsNotFound(err) || apimeta.IsNoMatchError(err) {
			log.V(1).Info("Longhorn replica-replenishment-wait-interval setting not found, skipping")
			return nil
		}
		return fmt.Errorf("failed to get Longhorn setting %s: %w", LonghornSettingReplicaReplenishment, err)
	}

	// Save original value to annotation
	if upgradePlan.Annotations == nil {
		upgradePlan.Annotations = make(map[string]string)
	}
	upgradePlan.Annotations[AnnotationReplicaReplenishmentOriginal] = setting.Value

	// Patch the setting to the extended value
	patch := client.MergeFrom(setting.DeepCopy())
	setting.Value = strconv.Itoa(ExtendedReplicaReplenishmentWaitInterval)
	if err := p.Client.Patch(ctx, &setting, patch); err != nil {
		return fmt.Errorf("failed to patch Longhorn setting %s: %w", LonghornSettingReplicaReplenishment, err)
	}

	log.Info("extended Longhorn replica replenishment wait interval",
		"original", upgradePlan.Annotations[AnnotationReplicaReplenishmentOriginal],
		"new", ExtendedReplicaReplenishmentWaitInterval)
	return nil
}

// disableDeschedulerAddon disables the descheduler addon to prevent it from
// interfering with node drains during upgrade.
func (p *NodeUpgradePhase) disableDeschedulerAddon(
	ctx context.Context,
	upgradePlan *managementv1beta1.UpgradePlan,
) error {
	if _, ok := upgradePlan.Annotations[AnnotationDeschedulerWasEnabled]; ok {
		return nil
	}

	log := logr.FromContextOrDiscard(ctx)
	var addon harvesterv1beta1.Addon
	if err := p.Client.Get(ctx, types.NamespacedName{
		Namespace: DeschedulerAddonNamespace,
		Name:      DeschedulerAddonName,
	}, &addon); err != nil {
		if apierrors.IsNotFound(err) || apimeta.IsNoMatchError(err) {
			log.V(1).Info("descheduler addon not found, skipping")
			return nil
		}
		return fmt.Errorf("failed to get descheduler addon: %w", err)
	}

	if !addon.Spec.Enabled {
		return nil
	}

	// Save state and disable
	if upgradePlan.Annotations == nil {
		upgradePlan.Annotations = make(map[string]string)
	}
	upgradePlan.Annotations[AnnotationDeschedulerWasEnabled] = valueTrue

	patch := client.MergeFrom(addon.DeepCopy())
	addon.Spec.Enabled = false
	if err := p.Client.Patch(ctx, &addon, patch); err != nil {
		return fmt.Errorf("failed to disable descheduler addon: %w", err)
	}

	log.Info("disabled descheduler addon for node upgrade")
	return nil
}

func (p *NodeUpgradePhase) Run(
	ctx context.Context,
	upgradePlan *managementv1beta1.UpgradePlan,
) (ctrl.Result, error) {
	if upgradePlan.Status.SingleNode != nil {
		return p.runSingleNode(ctx, upgradePlan)
	}
	return p.runMultiNode(ctx, upgradePlan)
}

// runMultiNode handles the multi-node upgrade path via Rancher V2 Provisioning
// with pre/post-drain hooks.
func (p *NodeUpgradePhase) runMultiNode(
	ctx context.Context,
	upgradePlan *managementv1beta1.UpgradePlan,
) (ctrl.Result, error) {
	log := logr.FromContextOrDiscard(ctx)
	log.V(1).Info("handle node upgrade via Rancher V2 Provisioning")

	if err := p.ensureClusterPatched(ctx, upgradePlan); err != nil {
		log.Error(err, "unable to patch Cluster resource for Kubernetes upgrade")
		return ctrl.Result{}, err
	}

	return p.checkNodeStatuses(ctx, upgradePlan)
}

// runSingleNode handles the single-node upgrade path by creating a single Job
// that runs upgrade_node.sh with the single-node-upgrade argument.
func (p *NodeUpgradePhase) runSingleNode(
	ctx context.Context,
	upgradePlan *managementv1beta1.UpgradePlan,
) (ctrl.Result, error) {
	log := logr.FromContextOrDiscard(ctx)
	nodeName := *upgradePlan.Status.SingleNode
	log.V(1).Info("handle single-node upgrade", "node", nodeName)

	shouldPause := ShouldPauseNode(upgradePlan, nodeName)
	if err := p.ensureSingleNodeUpgradeJob(ctx, upgradePlan, nodeName, shouldPause); err != nil {
		log.Error(err, "unable to ensure single-node upgrade job")
		return ctrl.Result{}, err
	}

	return p.checkNodeStatuses(ctx, upgradePlan)
}

// checkNodeStatuses checks per-node statuses and transitions the phase accordingly.
// It scans all nodes before deciding what to report, using the following priority:
// 1. Failed
// 2. Paused
// 3. Still-progressing
// 4. All-terminal
//
// For nodes that have reached terminal state, it also dispatches restore-vm Jobs
// if enabled. This ensures the Jobs are created by the same reconciler that
// controls phase advancement, eliminating race conditions between controllers.
func (p *NodeUpgradePhase) checkNodeStatuses(
	ctx context.Context,
	upgradePlan *managementv1beta1.UpgradePlan,
) (ctrl.Result, error) {
	// Pre-fetch the restore-vm ConfigMap once for all nodes to avoid N+1 API calls.
	restoreVMCM := p.fetchRestoreVMConfigMap(ctx, upgradePlan)

	var failedNode string
	var failedMsg string
	var pausedNodes []string
	allTerminal := true

	for nodeName, status := range upgradePlan.Status.NodeUpgradeStatuses {
		if IsNodeUpgradeFailure(status) {
			failedNode = nodeName
			failedMsg = status.Message
			break
		}
		if IsNodeUpgradePaused(status) {
			pausedNodes = append(pausedNodes, nodeName)
			continue
		}
		if !isTerminalState(status) {
			allTerminal = false
			continue
		}
		p.Log.V(1).Info("node has reached the desired node upgrade state", "nodeName", nodeName)

		if err := p.dispatchRestoreVMJob(ctx, upgradePlan, nodeName, restoreVMCM); err != nil {
			return ctrl.Result{}, err
		}
	}

	if failedNode != "" {
		updateProgressingPhase(
			upgradePlan,
			managementv1beta1.UpgradePlanPhaseFailed,
			fmt.Sprintf("node %s upgrade failed: %s", failedNode, failedMsg),
		)
		return ctrl.Result{}, nil
	}

	if len(pausedNodes) > 0 {
		sort.Strings(pausedNodes)
		msg := fmt.Sprintf("node upgrade paused: %s", strings.Join(pausedNodes, ", "))
		updateProgressingPhase(
			upgradePlan,
			managementv1beta1.UpgradePlanPhaseNodeUpgrading,
			msg,
		)
		return ctrl.Result{}, nil
	}

	if !allTerminal {
		updateProgressingPhase(upgradePlan, managementv1beta1.UpgradePlanPhaseNodeUpgrading, "")
		return ctrl.Result{}, nil
	}

	updateProgressingPhase(upgradePlan, managementv1beta1.UpgradePlanPhaseNodeUpgraded, "")
	return ctrl.Result{}, nil
}

// fetchRestoreVMConfigMap returns the restore-vm ConfigMap if restore-vm is
// enabled and the ConfigMap exists. Returns nil otherwise.
func (p *NodeUpgradePhase) fetchRestoreVMConfigMap(
	ctx context.Context,
	upgradePlan *managementv1beta1.UpgradePlan,
) *corev1.ConfigMap {
	if !IsRestoreVMEnabled(upgradePlan) {
		return nil
	}

	log := logr.FromContextOrDiscard(ctx)
	cmName := vmlivemigratedetector.GetRestoreVMConfigMapName(upgradePlan.Name)
	var cm corev1.ConfigMap
	if err := p.Client.Get(ctx, types.NamespacedName{
		Namespace: HarvesterSystemNamespace,
		Name:      cmName,
	}, &cm); err != nil {
		if apierrors.IsNotFound(err) {
			log.V(1).Info("restore-vm ConfigMap not found, skipping", "configmap", cmName)
		} else {
			log.Error(err, "failed to get restore-vm ConfigMap", "configmap", cmName)
		}
		return nil
	}
	return &cm
}

// dispatchRestoreVMJob ensures the restore-vm Job exists for a node that has
// reached terminal state. It is idempotent via GetOrCreate. The caller provides
// the pre-fetched restore-vm ConfigMap; a nil ConfigMap means no dispatch is needed.
func (p *NodeUpgradePhase) dispatchRestoreVMJob(
	ctx context.Context,
	upgradePlan *managementv1beta1.UpgradePlan,
	nodeName string,
	restoreVMCM *corev1.ConfigMap,
) error {
	if restoreVMCM == nil || restoreVMCM.Data[nodeName] == "" {
		return nil
	}

	log := logr.FromContextOrDiscard(ctx)

	var node corev1.Node
	if err := p.Client.Get(ctx, types.NamespacedName{Name: nodeName}, &node); err != nil {
		if apierrors.IsNotFound(err) {
			log.V(1).Info("node no longer exists, skipping restore-vm", "node", nodeName)
			return nil
		}
		return fmt.Errorf("failed to get node %s: %w", nodeName, err)
	}
	if IsWitnessNode(&node) {
		log.V(1).Info("witness node, skipping restore-vm", "node", nodeName)
		return nil
	}

	jobName := name.SafeConcatName(upgradePlan.Name, NodeComponent, JobTypeRestoreVM, nodeName)
	_, created, err := GetOrCreate(
		ctx, p.Client, p.Scheme,
		types.NamespacedName{Namespace: HarvesterSystemNamespace, Name: jobName},
		func() *batchv1.Job { return &batchv1.Job{} },
		func() *batchv1.Job {
			return ConstructRestoreVMJob(upgradePlan, nodeName, jobName, p.JobServiceAccount)
		},
		upgradePlan,
	)
	if err != nil {
		return fmt.Errorf("failed to create restore-vm Job for node %s: %w", nodeName, err)
	}
	if created {
		log.Info("dispatched restore-vm Job", "node", nodeName, "job", jobName)
		p.RecordEvent(upgradePlan, corev1.EventTypeNormal, "RestoreVMJobDispatched",
			fmt.Sprintf("Dispatched restore-vm Job %s for node %s", jobName, nodeName))
	}
	return nil
}

// PostRun guards the transition from NodeUpgraded to Finalize for multi-node
// clusters. It verifies that all machine-plan secrets have had their
// rke2/post-drain annotations cleared by Rancher, meaning every node's upgrade
// has been fully acknowledged. If any rke2/post-drain annotation remains, it
// returns an error to block the pipeline from entering the next phase.
func (p *NodeUpgradePhase) PostRun(
	ctx context.Context,
	upgradePlan *managementv1beta1.UpgradePlan,
) error {
	if upgradePlan.Status.SingleNode != nil {
		return nil
	}

	log := logr.FromContextOrDiscard(ctx)
	var secretList corev1.SecretList
	if err := p.Client.List(ctx, &secretList, client.InNamespace(FleetLocalNamespace)); err != nil {
		return fmt.Errorf("failed to list secrets in %s: %w", FleetLocalNamespace, err)
	}

	for i := range secretList.Items {
		secret := &secretList.Items[i]
		if secret.Type != corev1.SecretType(MachinePlanSecretType) {
			continue
		}

		if secret.Annotations[RKE2PostDrainAnnotation] != "" {
			log.V(1).Info(
				"machine-plan secret still has post-drain annotation, waiting for Rancher to complete",
				"secret", secret.Name)
			return fmt.Errorf("waiting for Rancher to complete node upgrades: secret %s still has %s annotation",
				secret.Name, RKE2PostDrainAnnotation)
		}
	}

	return nil
}

// ensureSingleNodeUpgradeJob creates the single-node-upgrade Job if it doesn't already exist.
// If the Job already exists, it reconciles the suspend state.
func (p *NodeUpgradePhase) ensureSingleNodeUpgradeJob(
	ctx context.Context,
	upgradePlan *managementv1beta1.UpgradePlan,
	nodeName string,
	suspend bool,
) error {
	jobName := name.SafeConcatName(upgradePlan.Name, NodeComponent, JobTypeSingleNodeUpgrade, nodeName)
	nn := types.NamespacedName{
		Namespace: HarvesterSystemNamespace,
		Name:      jobName,
	}

	existing, _, err := GetOrCreate(
		ctx, p.Client, p.Scheme, nn,
		func() *batchv1.Job { return &batchv1.Job{} },
		func() *batchv1.Job {
			return ConstructNodeJob(upgradePlan, nodeName, jobName, JobTypeSingleNodeUpgrade, p.JobServiceAccount, suspend)
		},
		upgradePlan,
	)
	if err != nil {
		return err
	}
	return ReconcileJobSuspend(ctx, p.Client, existing, suspend)
}

// ensureClusterPatched patches the provisioning.cattle.io/v1 Cluster resource
// to trigger Rancher's Kubernetes upgrade with drain hooks.
func (p *NodeUpgradePhase) ensureClusterPatched(
	ctx context.Context,
	upgradePlan *managementv1beta1.UpgradePlan,
) error {
	var cluster provisioningv1.Cluster
	if err := p.Client.Get(ctx, types.NamespacedName{
		Namespace: FleetLocalNamespace,
		Name:      LocalClusterName,
	}, &cluster); err != nil {
		return fmt.Errorf("failed to get Cluster resource: %w", err)
	}

	targetK8sVersion := getKubernetesVersion(upgradePlan)
	if targetK8sVersion == "" {
		return fmt.Errorf("kubernetes version not found in release metadata")
	}

	// Check if already patched for this upgrade (idempotency).
	// We cannot compare kubernetesVersion because same-version upgrades are valid;
	// instead, we check for a status field that records the provisionGeneration we set.
	if upgradePlan.Status.ProvisionGeneration != nil {
		return nil
	}

	patch := client.MergeFrom(cluster.DeepCopy())

	// Set Kubernetes version
	cluster.Spec.KubernetesVersion = targetK8sVersion

	// Increment provisionGeneration
	newGeneration := cluster.Spec.RKEConfig.ProvisionGeneration + 1
	cluster.Spec.RKEConfig.ProvisionGeneration = newGeneration

	// Set upgrade strategy
	drainOpts := rkev1.DrainOptions{
		Enabled:            true,
		Force:              true,
		IgnoreDaemonSets:   ptr.To(true),
		DeleteEmptyDirData: true,
		PreDrainHooks:      []rkev1.DrainHook{{Annotation: PreHookAnnotation}},
		PostDrainHooks:     []rkev1.DrainHook{{Annotation: PostHookAnnotation}},
	}

	cluster.Spec.RKEConfig.UpgradeStrategy = rkev1.ClusterUpgradeStrategy{
		ControlPlaneConcurrency:  "1",
		ControlPlaneDrainOptions: drainOpts,
		WorkerConcurrency:        "1",
		WorkerDrainOptions:       drainOpts,
	}

	if err := p.Client.Patch(ctx, &cluster, patch); err != nil {
		return fmt.Errorf("failed to patch Cluster resource: %w", err)
	}

	// Record the provisionGeneration we set so subsequent reconciles skip this patch.
	// The reconciler's Status().Update() persists this atomically with other status changes.
	upgradePlan.Status.ProvisionGeneration = &newGeneration
	return nil
}
