package upgradeplan

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	managementv1beta1 "github.com/harvester/upgrade-toolkit/api/v1beta1"
)

// LogPreparePhase creates an UpgradeLog CR and waits for the log collector
// infrastructure to become ready before proceeding to the next phase.
type LogPreparePhase struct {
	*PhaseDeps
}

func NewLogPreparePhase(deps *PhaseDeps) *LogPreparePhase {
	return &LogPreparePhase{PhaseDeps: deps}
}

func (p *LogPreparePhase) Name() string { return "LogPrepare" }

func (p *LogPreparePhase) Run(ctx context.Context, upgradePlan *managementv1beta1.UpgradePlan) (ctrl.Result, error) {
	log := logr.FromContextOrDiscard(ctx)

	upgradeLogName := upgradePlan.Name

	// Get or create UpgradeLog CR
	var upgradeLog managementv1beta1.UpgradeLog
	err := p.Client.Get(ctx, types.NamespacedName{Name: upgradeLogName}, &upgradeLog)
	if apierrors.IsNotFound(err) {
		log.V(1).Info("creating UpgradeLog", "name", upgradeLogName)
		upgradeLog = managementv1beta1.UpgradeLog{
			ObjectMeta: metav1.ObjectMeta{
				Name: upgradeLogName,
			},
			Spec: managementv1beta1.UpgradeLogSpec{
				UpgradePlanName: upgradePlan.Name,
			},
		}

		// Set UpgradePlan as the owner so the UpgradeLog is cleaned up on deletion
		if setErr := controllerutil.SetOwnerReference(upgradePlan, &upgradeLog, p.Scheme); setErr != nil {
			return ctrl.Result{}, fmt.Errorf("setting owner reference: %w", setErr)
		}

		if createErr := p.Client.Create(ctx, &upgradeLog); createErr != nil {
			return ctrl.Result{}, fmt.Errorf("creating UpgradeLog: %w", createErr)
		}
		return ctrl.Result{RequeueAfter: RequeueAfterDuration}, nil
	}
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("getting UpgradeLog: %w", err)
	}

	// Check if collector is ready
	if upgradeLog.ConditionTrue(managementv1beta1.UpgradeLogCollectorReady) {
		log.V(1).Info("log collector is ready, advancing to next phase")
		updateProgressingPhase(upgradePlan, managementv1beta1.UpgradePlanPhaseLogPrepared, "")
		return ctrl.Result{}, nil
	}

	log.V(1).Info("waiting for log collector to become ready", "upgradeLogPhase", upgradeLog.Status.CurrentPhase)
	return ctrl.Result{RequeueAfter: RequeueAfterDuration}, nil
}
