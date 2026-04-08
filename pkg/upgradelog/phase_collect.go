package upgradelog

import (
	"context"

	"github.com/go-logr/logr"
	ctrl "sigs.k8s.io/controller-runtime"

	managementv1beta1 "github.com/harvester/upgrade-toolkit/api/v1beta1"
)

// CollectPhase is the steady-state phase where the collector is running
// and sidecars are streaming logs. It remains in this phase until the
// UpgradePlan reaches a terminal state, at which point the controller
// transitions the UpgradeLog to the Stopping phase externally.
type CollectPhase struct {
	*PhaseDeps
}

func NewCollectPhase(deps *PhaseDeps) *CollectPhase {
	return &CollectPhase{PhaseDeps: deps}
}

func (p *CollectPhase) Name() string { return "Collect" }

func (p *CollectPhase) Run(ctx context.Context, upgradeLog *managementv1beta1.UpgradeLog) (ctrl.Result, error) {
	log := logr.FromContextOrDiscard(ctx)
	log.V(2).Info("collecting logs, waiting for external stop signal")

	// This phase is a no-op on each reconcile. The transition to Stopping
	// is triggered by the UpgradePlan controller when the upgrade reaches
	// a terminal state (Succeeded or Failed).
	return ctrl.Result{}, nil
}
