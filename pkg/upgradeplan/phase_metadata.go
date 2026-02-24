package upgradeplan

import (
	"context"

	ctrl "sigs.k8s.io/controller-runtime"

	managementv1beta1 "github.com/harvester/upgrade-toolkit/api/v1beta1"
)

// MetadataPopulatePhase fetches release metadata from the upgrade repo.
type MetadataPopulatePhase struct {
	*PhaseDeps
}

func NewMetadataPopulatePhase(deps *PhaseDeps) *MetadataPopulatePhase {
	return &MetadataPopulatePhase{PhaseDeps: deps}
}

func (p *MetadataPopulatePhase) Name() string { return "MetadataPopulate" }

func (p *MetadataPopulatePhase) Run(
	ctx context.Context,
	upgradePlan *managementv1beta1.UpgradePlan,
) (ctrl.Result, error) {
	p.Log.V(1).Info("handle metadata populate")

	harvesterRelease := newHarvesterRelease(upgradePlan)
	if err := harvesterRelease.loadReleaseMetadata(); err != nil {
		updateProgressingPhase(upgradePlan, managementv1beta1.UpgradePlanPhaseMetadataPopulating, err.Error())
		return ctrl.Result{}, err
	}
	upgradePlan.Status.ReleaseMetadata = harvesterRelease.ReleaseMetadata
	updateProgressingPhase(upgradePlan, managementv1beta1.UpgradePlanPhaseMetadataPopulated, "")
	return ctrl.Result{}, nil
}
