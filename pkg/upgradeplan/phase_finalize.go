package upgradeplan

import (
	"context"
	"fmt"

	harvesterv1beta1 "github.com/harvester/harvester/pkg/apis/harvesterhci.io/v1beta1"
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

// PreRun performs resource cleanup: deletes all upgrade-related resources.
// Idempotent — deleting non-existent resources is a no-op.
func (p *FinalizePhase) PreRun(
	ctx context.Context,
	upgradePlan *managementv1beta1.UpgradePlan,
) error {
	p.Log.V(1).Info("handle resource cleanup")

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
		{&upgradev1.Plan{}, cattleSystemNamespace, NodeComponent},
	}

	for _, r := range resourcesToDelete {
		namespacedName := types.NamespacedName{
			Namespace: r.namespace,
			Name:      fmt.Sprintf("%s-%s", upgradePlan.Name, r.component),
		}
		if err := p.deleteResource(ctx, r.obj, namespacedName); err != nil {
			return err
		}
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

func (p *FinalizePhase) deleteResource(
	ctx context.Context,
	obj client.Object,
	namespacedName types.NamespacedName,
) error {
	if err := p.Client.Get(ctx, namespacedName, obj); err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
		return nil
	}

	if err := p.Client.Delete(ctx, obj, &client.DeleteOptions{
		PropagationPolicy: ptr.To(metav1.DeletePropagationBackground),
	}); err != nil && !apierrors.IsNotFound(err) {
		return err
	}

	return nil
}
