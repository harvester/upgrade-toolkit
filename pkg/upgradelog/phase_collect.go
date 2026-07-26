package upgradelog

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	managementv1beta1 "github.com/harvester/upgrade-toolkit/api/v1beta1"
)

// CollectPhase is the steady-state phase where the collector is running
// and sidecars are streaming logs. It completes when the associated
// UpgradePlan reaches a terminal state (Succeeded or Failed) or is deleted.
type CollectPhase struct {
	*PhaseDeps
}

func NewCollectPhase(deps *PhaseDeps) *CollectPhase {
	return &CollectPhase{PhaseDeps: deps}
}

func (p *CollectPhase) Name() string { return "Collect" }

func (p *CollectPhase) Run(ctx context.Context, upgradeLog *managementv1beta1.UpgradeLog) (ctrl.Result, error) {
	log := logr.FromContextOrDiscard(ctx)

	var upgradePlan managementv1beta1.UpgradePlan
	if err := p.Client.Get(ctx, types.NamespacedName{Name: upgradeLog.Spec.UpgradePlanName}, &upgradePlan); err != nil {
		if !apierrors.IsNotFound(err) {
			return ctrl.Result{}, fmt.Errorf("fetching UpgradePlan: %w", err)
		}
		log.V(1).Info("UpgradePlan not found, completing collection")
		upgradeLog.Status.CurrentPhase = managementv1beta1.UpgradeLogPhaseCollected
		return ctrl.Result{}, nil
	}

	if isUpgradePlanTerminal(upgradePlan.Status.CurrentPhase) {
		log.V(1).Info("UpgradePlan reached terminal phase, completing collection", "phase", upgradePlan.Status.CurrentPhase)
		upgradeLog.Status.CurrentPhase = managementv1beta1.UpgradeLogPhaseCollected
		return ctrl.Result{}, nil
	}

	log.V(2).Info("collecting logs, UpgradePlan still active", "phase", upgradePlan.Status.CurrentPhase)
	return ctrl.Result{}, nil
}

func isUpgradePlanTerminal(phase managementv1beta1.UpgradePlanPhase) bool {
	return phase == managementv1beta1.UpgradePlanPhaseSucceeded ||
		phase == managementv1beta1.UpgradePlanPhaseFailed
}
