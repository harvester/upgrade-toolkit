package upgradeplan

import (
	"context"
	"fmt"

	provisioningv1 "github.com/rancher/rancher/pkg/apis/provisioning.cattle.io/v1"
	rkev1 "github.com/rancher/rancher/pkg/apis/rke.cattle.io/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	managementv1beta1 "github.com/harvester/upgrade-toolkit/api/v1beta1"
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

// PreRun initializes NodeUpgradeStatuses for all Harvester-managed nodes.
func (p *NodeUpgradePhase) PreRun(
	ctx context.Context,
	upgradePlan *managementv1beta1.UpgradePlan,
) error {
	if upgradePlan.Status.NodeUpgradeStatuses == nil {
		upgradePlan.Status.NodeUpgradeStatuses = make(map[string]managementv1beta1.NodeUpgradeStatus)
	}

	var nodeList corev1.NodeList
	if err := p.Client.List(ctx, &nodeList, &client.ListOptions{
		LabelSelector: labels.SelectorFromSet(labels.Set{
			harvesterManagedLabel: "true",
		}),
	}); err != nil {
		return err
	}

	for _, node := range nodeList.Items {
		if _, exists := upgradePlan.Status.NodeUpgradeStatuses[node.Name]; !exists {
			upgradePlan.Status.NodeUpgradeStatuses[node.Name] = managementv1beta1.NodeUpgradeStatus{
				State: managementv1beta1.NodeStateImagePreloaded,
			}
		}
	}

	return nil
}

func (p *NodeUpgradePhase) Run(
	ctx context.Context,
	upgradePlan *managementv1beta1.UpgradePlan,
) (ctrl.Result, error) {
	p.Log.V(1).Info("handle node upgrade via Rancher V2 Provisioning")

	if err := p.ensureClusterPatched(ctx, upgradePlan); err != nil {
		p.Log.Error(err, "unable to patch Cluster resource for Kubernetes upgrade")
		return ctrl.Result{}, err
	}

	// Check per-node statuses
	for nodeName, status := range upgradePlan.Status.NodeUpgradeStatuses {
		if IsNodeUpgradeFailure(status) {
			updateProgressingPhase(
				upgradePlan,
				managementv1beta1.UpgradePlanPhaseFailed,
				fmt.Sprintf("node %s upgrade failed: %s", nodeName, status.Message),
			)
			return ctrl.Result{}, nil
		}
		if !isTerminalState(status) {
			updateProgressingPhase(upgradePlan, managementv1beta1.UpgradePlanPhaseNodeUpgrading, "")
			return ctrl.Result{}, nil
		}
		p.Log.V(1).Info("node has reached the desired node upgrade state", "nodeName", nodeName)
	}

	updateProgressingPhase(upgradePlan, managementv1beta1.UpgradePlanPhaseNodeUpgraded, "")
	return ctrl.Result{}, nil
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
