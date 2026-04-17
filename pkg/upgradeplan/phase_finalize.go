package upgradeplan

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

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

// PreRun removes skip manifests (if applied) and then performs resource cleanup.
func (p *FinalizePhase) PreRun(
	ctx context.Context,
	upgradePlan *managementv1beta1.UpgradePlan,
) error {
	if err := p.ensureSkipManifestsRemoved(ctx, upgradePlan); err != nil {
		return err
	}
	return CleanupUpgradeResources(ctx, p.Client, p.Log, upgradePlan)
}

func (p *FinalizePhase) ensureSkipManifestsRemoved(
	ctx context.Context,
	upgradePlan *managementv1beta1.UpgradePlan,
) error {
	waiting, err := RemoveSkipManifests(ctx, p.Client, p.Scheme, upgradePlan, p.PlanServiceAccount)
	if err != nil {
		return err
	}
	if waiting {
		return fmt.Errorf("waiting for skip-manifest remove plan to complete")
	}
	return nil
}

// Run determines success/failure and marks the UpgradePlan as complete.
func (p *FinalizePhase) Run(
	ctx context.Context,
	upgradePlan *managementv1beta1.UpgradePlan,
) (ctrl.Result, error) {
	log := logr.FromContextOrDiscard(ctx)
	log.V(1).Info("handle finalize")

	// Determine terminal phase
	if upgradePlan.Status.CurrentPhase != managementv1beta1.UpgradePlanPhaseFailed {
		upgradePlan.Status.CurrentPhase = managementv1beta1.UpgradePlanPhaseSucceeded
	}

	phase := upgradePlan.Status.CurrentPhase

	// Available is always false once finalized
	upgradePlan.SetCondition(
		managementv1beta1.UpgradePlanAvailable,
		metav1.ConditionFalse,
		"Executed",
		"Entered one of the terminal phases",
	)

	// Preserve failure message if failed
	if phase == managementv1beta1.UpgradePlanPhaseFailed {
		cond := upgradePlan.LookupCondition(managementv1beta1.UpgradePlanProgressing)
		upgradePlan.SetCondition(
			managementv1beta1.UpgradePlanProgressing,
			metav1.ConditionFalse,
			string(phase),
			cond.Message,
		)
	} else {
		// Succeeded case
		upgradePlan.SetCondition(
			managementv1beta1.UpgradePlanProgressing,
			metav1.ConditionFalse,
			string(phase),
			"UpgradePlan has completed",
		)
	}

	controllerutil.RemoveFinalizer(upgradePlan, UpgradePlanFinalizer)

	return ctrl.Result{}, nil
}
