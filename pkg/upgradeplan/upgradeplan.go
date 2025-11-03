package upgradeplan

import (
	"context"

	ctrl "sigs.k8s.io/controller-runtime"

	managementv1beta1 "github.com/harvester/upgrade-toolkit/api/v1beta1"
)

type UpgradePlan struct {
	phases map[managementv1beta1.UpgradePlanPhase]UpgradePlanPhase
}

func NewUpgradePlan(handler *UpgradePlanPhaseHandler) *UpgradePlan {
	return &UpgradePlan{
		phases: map[managementv1beta1.UpgradePlanPhase]UpgradePlanPhase{
			"": &InitializePhase{h: handler},
			managementv1beta1.UpgradePlanPhaseInitializing:       &InitializePhase{h: handler},
			managementv1beta1.UpgradePlanPhaseInitialized:        &ISODownloadPhase{h: handler},
			managementv1beta1.UpgradePlanPhaseISODownloading:     &ISODownloadPhase{h: handler},
			managementv1beta1.UpgradePlanPhaseISODownloaded:      &RepoCreatePhase{h: handler},
			managementv1beta1.UpgradePlanPhaseRepoCreating:       &RepoCreatePhase{h: handler},
			managementv1beta1.UpgradePlanPhaseRepoCreated:        &MetadataPopulatePhase{h: handler},
			managementv1beta1.UpgradePlanPhaseMetadataPopulating: &MetadataPopulatePhase{h: handler},
			managementv1beta1.UpgradePlanPhaseMetadataPopulated:  &ImagePreloadPhase{h: handler},
			managementv1beta1.UpgradePlanPhaseImagePreloading:    &ImagePreloadPhase{h: handler},
			managementv1beta1.UpgradePlanPhaseImagePreloaded:     &ClusterUpgradePhase{h: handler},
			managementv1beta1.UpgradePlanPhaseClusterUpgrading:   &ClusterUpgradePhase{h: handler},
			managementv1beta1.UpgradePlanPhaseClusterUpgraded:    &NodeUpgradePhase{h: handler},
			managementv1beta1.UpgradePlanPhaseNodeUpgrading:      &NodeUpgradePhase{h: handler},
			managementv1beta1.UpgradePlanPhaseNodeUpgraded:       &CleanupPhase{h: handler},
			managementv1beta1.UpgradePlanPhaseCleaningUp:         &CleanupPhase{h: handler},
			managementv1beta1.UpgradePlanPhaseCleanedUp:          &FinalizePhase{h: handler},
			managementv1beta1.UpgradePlanPhaseSucceeded:          &FinalizePhase{h: handler},
			managementv1beta1.UpgradePlanPhaseFailed:             &FinalizePhase{h: handler},
		},
	}
}

func (up *UpgradePlan) ExecutePhase(
	ctx context.Context,
	upgradePlan *managementv1beta1.UpgradePlan,
) (ctrl.Result, error) {
	phase, exists := up.phases[upgradePlan.Status.Phase]
	if !exists {
		// TODO: We cannot do anything because UpgradePlan is in limbo.
		return ctrl.Result{}, nil
	}

	if preHook, ok := phase.(PreHook); ok {
		if err := preHook.PreRun(ctx, upgradePlan); err != nil {
			return ctrl.Result{}, err
		}
	}

	result, err := phase.Run(ctx, upgradePlan)

	if postHook, ok := phase.(PostHook); ok {
		postHook.PostRun(ctx, upgradePlan, result, err)
	}

	return result, err
}
