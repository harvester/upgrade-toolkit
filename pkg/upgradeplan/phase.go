package upgradeplan

import (
	"context"

	ctrl "sigs.k8s.io/controller-runtime"

	managementv1beta1 "github.com/harvester/upgrade-toolkit/api/v1beta1"
)

type UpgradePlanPhase interface {
	Run(ctx context.Context, upgradePlan *managementv1beta1.UpgradePlan) (ctrl.Result, error)
	String() string
}

type PreHook interface {
	PreRun(ctx context.Context, upgradePlan *managementv1beta1.UpgradePlan) error
}

type PostHook interface {
	PostRun(ctx context.Context, upgradePlan *managementv1beta1.UpgradePlan, result ctrl.Result, err error)
}

type InitializePhase struct {
	h *UpgradePlanPhaseHandler
}

func (s *InitializePhase) Run(ctx context.Context, upgradePlan *managementv1beta1.UpgradePlan) (ctrl.Result, error) {
	return s.h.initialize(ctx, upgradePlan)
}
func (s *InitializePhase) String() string { return "Initialize" }

type ISODownloadPhase struct {
	h *UpgradePlanPhaseHandler
}

func (s *ISODownloadPhase) Run(ctx context.Context, upgradePlan *managementv1beta1.UpgradePlan) (ctrl.Result, error) {
	return s.h.isoDownload(ctx, upgradePlan)
}
func (s *ISODownloadPhase) String() string { return "ISODownload" }

type RepoCreatePhase struct {
	h *UpgradePlanPhaseHandler
}

func (s *RepoCreatePhase) Run(ctx context.Context, upgradePlan *managementv1beta1.UpgradePlan) (ctrl.Result, error) {
	return s.h.repoCreate(upgradePlan)
}
func (s *RepoCreatePhase) String() string { return "RepoCreate" }

type MetadataPopulatePhase struct {
	h *UpgradePlanPhaseHandler
}

func (s *MetadataPopulatePhase) Run(
	ctx context.Context,
	upgradePlan *managementv1beta1.UpgradePlan,
) (ctrl.Result, error) {
	return s.h.metadataPopulate(upgradePlan)
}
func (s *MetadataPopulatePhase) String() string { return "MetadataPopulate" }

type ImagePreloadPhase struct {
	h *UpgradePlanPhaseHandler
}

func (s *ImagePreloadPhase) Run(ctx context.Context, upgradePlan *managementv1beta1.UpgradePlan) (ctrl.Result, error) {
	return s.h.imagePreload(ctx, upgradePlan)
}
func (s *ImagePreloadPhase) String() string { return "ImagePreload" }

type ClusterUpgradePhase struct {
	h *UpgradePlanPhaseHandler
}

func (s *ClusterUpgradePhase) Run(
	ctx context.Context,
	upgradePlan *managementv1beta1.UpgradePlan,
) (ctrl.Result, error) {
	return s.h.clusterUpgrade(ctx, upgradePlan)
}
func (s *ClusterUpgradePhase) String() string { return "ClusterUpgrade" }

type NodeUpgradePhase struct {
	h *UpgradePlanPhaseHandler
}

func (s *NodeUpgradePhase) Run(ctx context.Context, upgradePlan *managementv1beta1.UpgradePlan) (ctrl.Result, error) {
	return s.h.nodeUpgrade(ctx, upgradePlan)
}
func (s *NodeUpgradePhase) String() string { return "NodeUpgrade" }

type CleanupPhase struct {
	h *UpgradePlanPhaseHandler
}

func (s *CleanupPhase) Run(ctx context.Context, upgradePlan *managementv1beta1.UpgradePlan) (ctrl.Result, error) {
	return s.h.resourceCleanup(upgradePlan)
}
func (s *CleanupPhase) String() string { return "Cleanup" }

type FinalizePhase struct {
	h *UpgradePlanPhaseHandler
}

func (s *FinalizePhase) Run(ctx context.Context, upgradePlan *managementv1beta1.UpgradePlan) (ctrl.Result, error) {
	finalize(upgradePlan)
	return ctrl.Result{}, nil
}
func (s *FinalizePhase) String() string { return "Finalize" }
