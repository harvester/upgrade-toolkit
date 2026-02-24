package upgradeplan

import (
	"context"

	harvesterv1beta1 "github.com/harvester/harvester/pkg/apis/harvesterhci.io/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"

	managementv1beta1 "github.com/harvester/upgrade-toolkit/api/v1beta1"
)

// InitPhase sets up the UpgradePlan resource's conditions and essential status fields.
// It implements Runnable and PostRunnable (for upgrade pre-checks).
type InitPhase struct {
	*PhaseDeps
}

func NewInitPhase(deps *PhaseDeps) *InitPhase {
	return &InitPhase{PhaseDeps: deps}
}

func (p *InitPhase) Name() string { return "Initialize" }

func (p *InitPhase) Run(
	ctx context.Context,
	upgradePlan *managementv1beta1.UpgradePlan,
) (ctrl.Result, error) {
	p.Log.V(1).Info("handle initialize status")

	if upgradePlan.Status.CurrentPhase == managementv1beta1.UpgradePlanPhaseInitializing {
		upgradePlan.SetCondition(managementv1beta1.UpgradePlanAvailable, metav1.ConditionTrue, "Executable", "")

		if err := p.loadVersion(ctx, upgradePlan); err != nil {
			return ctrl.Result{}, err
		}

		if err := p.loadPreviousVersion(ctx, upgradePlan); err != nil {
			return ctrl.Result{}, err
		}

		updateProgressingPhase(upgradePlan, managementv1beta1.UpgradePlanPhaseInitialized, "")
		return ctrl.Result{}, nil
	}

	updateProgressingPhase(upgradePlan, managementv1beta1.UpgradePlanPhaseInitializing, "")
	return ctrl.Result{}, nil
}

// PostRun performs upgrade pre-checks after initialization completes.
func (p *InitPhase) PostRun(
	ctx context.Context,
	upgradePlan *managementv1beta1.UpgradePlan,
) error {
	// TODO: Implement upgrade pre-checks here, e.g.:
	// - Minimum upgradable version validation
	// - Cluster health checks
	return nil
}

func (p *InitPhase) loadVersion(
	ctx context.Context,
	upgradePlan *managementv1beta1.UpgradePlan,
) error {
	var version managementv1beta1.Version
	if err := p.Client.Get(ctx, types.NamespacedName{Name: upgradePlan.Spec.Version}, &version); err != nil {
		return err
	}
	upgradePlan.Status.Version = &version.Spec
	return nil
}

func (p *InitPhase) loadPreviousVersion(
	ctx context.Context,
	upgradePlan *managementv1beta1.UpgradePlan,
) error {
	var setting harvesterv1beta1.Setting
	if err := p.Client.Get(ctx, types.NamespacedName{Name: serverVersionSettingName}, &setting); err != nil {
		return err
	}
	upgradePlan.Status.PreviousVersion = ptr.To(setting.Value)
	return nil
}
