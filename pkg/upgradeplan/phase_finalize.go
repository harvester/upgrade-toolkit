package upgradeplan

import (
	"context"
	"fmt"

	harvesterv1beta1 "github.com/harvester/harvester/pkg/apis/harvesterhci.io/v1beta1"
	provisioningv1 "github.com/rancher/rancher/pkg/apis/provisioning.cattle.io/v1"
	rkev1 "github.com/rancher/rancher/pkg/apis/rke.cattle.io/v1"
	upgradev1 "github.com/rancher/system-upgrade-controller/pkg/apis/upgrade.cattle.io/v1"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	managementv1beta1 "github.com/harvester/upgrade-toolkit/api/v1beta1"
)

// FinalizePhase wraps up the UpgradePlan. PreRun performs resource cleanup
// (the former CleanUp phase). Run determines success/failure and marks terminal conditions.
type FinalizePhase struct {
	*PhaseDeps
}

func NewFinalizePhase(deps *PhaseDeps) *FinalizePhase {
	return &FinalizePhase{PhaseDeps: deps}
}

func (p *FinalizePhase) Name() string { return "Finalize" }

// PreRun performs resource cleanup: deletes all upgrade-related resources by label,
// cleans up node annotations, and reverts the Cluster's upgradeStrategy.
// Idempotent — operating on non-existent resources is a no-op.
func (p *FinalizePhase) PreRun(
	ctx context.Context,
	upgradePlan *managementv1beta1.UpgradePlan,
) error {
	p.Log.V(1).Info("handle resource cleanup")

	// Delete all upgrade-related resources by label. This covers both named
	// resources (VirtualMachineImage, Deployment, Service, Plan, cluster-upgrade Job) and
	// dynamically-named resources (pre/post-drain Jobs).
	resourcesToDelete := []struct {
		obj       client.Object
		namespace string
		component string
	}{
		{&harvesterv1beta1.VirtualMachineImage{}, harvesterSystemNamespace, imageComponent},
		{&appsv1.Deployment{}, harvesterSystemNamespace, repoComponent},
		{&corev1.Service{}, harvesterSystemNamespace, repoComponent},
		{&upgradev1.Plan{}, cattleSystemNamespace, PrepareComponent},
		{&batchv1.Job{}, harvesterSystemNamespace, ClusterComponent},
		{&batchv1.Job{}, harvesterSystemNamespace, NodeComponent},
	}

	for _, r := range resourcesToDelete {
		if err := p.Client.DeleteAllOf(ctx, r.obj,
			client.InNamespace(r.namespace),
			client.MatchingLabels{
				HarvesterUpgradePlanLabel:      upgradePlan.Name,
				HarvesterUpgradeComponentLabel: r.component,
			},
			&client.DeleteAllOfOptions{DeleteOptions: client.DeleteOptions{
				PropagationPolicy: ptr.To(metav1.DeletePropagationBackground),
			}},
		); err != nil {
			return err
		}
	}

	// Clean up lingering pendingOSImage annotations on Nodes (safety net for
	// aborted upgrades where nodes may still be in WaitingReboot state)
	if err := p.cleanupNodePendingOSImageAnnotations(ctx); err != nil {
		return err
	}

	// Revert the Cluster's upgradeStrategy that was set during NodeUpgrade
	if err := p.revertClusterUpgradeStrategy(ctx, upgradePlan); err != nil {
		return err
	}

	return nil
}

// Run determines success/failure and marks the UpgradePlan as complete.
func (p *FinalizePhase) Run(
	ctx context.Context,
	upgradePlan *managementv1beta1.UpgradePlan,
) (ctrl.Result, error) {
	p.Log.V(1).Info("handle finalize")

	if upgradePlan.Status.CurrentPhase != managementv1beta1.UpgradePlanPhaseFailed {
		upgradePlan.Status.CurrentPhase = managementv1beta1.UpgradePlanPhaseSucceeded
	}

	markUpgradePlanComplete(upgradePlan)
	return ctrl.Result{}, nil
}

func (p *FinalizePhase) cleanupNodePendingOSImageAnnotations(ctx context.Context) error {
	var nodeList corev1.NodeList
	if err := p.Client.List(ctx, &nodeList); err != nil {
		return err
	}

	for i := range nodeList.Items {
		node := &nodeList.Items[i]
		if _, ok := node.Annotations[PendingOSImageAnnotation]; !ok {
			continue
		}

		patch := client.MergeFrom(node.DeepCopy())
		delete(node.Annotations, PendingOSImageAnnotation)
		if err := p.Client.Patch(ctx, node, patch); err != nil {
			return err
		}
	}

	return nil
}

func (p *FinalizePhase) revertClusterUpgradeStrategy(
	ctx context.Context,
	upgradePlan *managementv1beta1.UpgradePlan,
) error {
	if upgradePlan.Status.ProvisionGeneration == nil {
		return nil
	}

	var cluster provisioningv1.Cluster
	if err := p.Client.Get(ctx, types.NamespacedName{
		Namespace: FleetLocalNamespace,
		Name:      LocalClusterName,
	}, &cluster); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("failed to get Cluster resource: %w", err)
	}

	if cluster.Spec.RKEConfig == nil {
		return nil
	}

	patch := client.MergeFrom(cluster.DeepCopy())
	cluster.Spec.RKEConfig.UpgradeStrategy = rkev1.ClusterUpgradeStrategy{}
	if err := p.Client.Patch(ctx, &cluster, patch); err != nil {
		return fmt.Errorf("failed to revert Cluster upgradeStrategy: %w", err)
	}

	return nil
}
