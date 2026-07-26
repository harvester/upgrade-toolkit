package upgradelog

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"

	managementv1beta1 "github.com/harvester/upgrade-toolkit/api/v1beta1"
)

// StopPhase gracefully shuts down the log collector. It scales the collector
// Deployment to zero and waits for it to terminate. The PVC is intentionally
// preserved for post-mortem access.
type StopPhase struct {
	*PhaseDeps
}

func NewStopPhase(deps *PhaseDeps) *StopPhase {
	return &StopPhase{PhaseDeps: deps}
}

func (p *StopPhase) Name() string { return "Stop" }

func (p *StopPhase) Run(ctx context.Context, upgradeLog *managementv1beta1.UpgradeLog) (ctrl.Result, error) {
	log := logr.FromContextOrDiscard(ctx)

	deployName := collectorDeploymentName(upgradeLog.Name)
	var deploy appsv1.Deployment
	err := p.Client.Get(ctx, types.NamespacedName{
		Name:      deployName,
		Namespace: collectorNamespace,
	}, &deploy)
	if err != nil {
		// Deployment already deleted or doesn't exist; consider stopped.
		log.V(1).Info("collector deployment not found, marking as stopped")
		upgradeLog.Status.CurrentPhase = managementv1beta1.UpgradeLogPhaseStopped
		return ctrl.Result{}, nil
	}

	// Scale to zero if not already
	if deploy.Spec.Replicas == nil || *deploy.Spec.Replicas != 0 {
		log.V(1).Info("scaling collector deployment to zero")
		deploy.Spec.Replicas = ptr.To(int32(0))
		if updateErr := p.Client.Update(ctx, &deploy); updateErr != nil {
			return ctrl.Result{}, fmt.Errorf("scaling collector deployment to zero: %w", updateErr)
		}
		return ctrl.Result{RequeueAfter: RequeueAfterDuration}, nil
	}

	// Wait for all replicas to terminate
	if deploy.Status.Replicas > 0 {
		log.V(1).Info("waiting for collector pods to terminate", "replicas", deploy.Status.Replicas)
		return ctrl.Result{RequeueAfter: RequeueAfterDuration}, nil
	}

	log.V(1).Info("collector deployment stopped")
	upgradeLog.Status.CurrentPhase = managementv1beta1.UpgradeLogPhaseStopped
	return ctrl.Result{}, nil
}
