package upgradeplan

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"

	managementv1beta1 "github.com/harvester/upgrade-toolkit/api/v1beta1"
)

// PhaseEntry ties a Runnable to its API phase constants.
type PhaseEntry struct {
	Phase          Runnable
	ActivePhase    managementv1beta1.UpgradePlanPhase
	CompletedPhase managementv1beta1.UpgradePlanPhase
}

// Pipeline is an ordered sequence of phases that drives the upgrade lifecycle.
type Pipeline struct {
	deps     *PhaseDeps
	init     Runnable
	finalize Runnable
	phases   []PhaseEntry
	index    map[managementv1beta1.UpgradePlanPhase]int
}

// NewPipeline creates a new upgrade pipeline with all phases wired up.
func NewPipeline(deps *PhaseDeps) *Pipeline {
	p := &Pipeline{
		deps:     deps,
		init:     NewInitPhase(deps),
		finalize: NewFinalizePhase(deps),
		phases: []PhaseEntry{
			{
				NewISODownloadPhase(deps),
				managementv1beta1.UpgradePlanPhaseISODownloading,
				managementv1beta1.UpgradePlanPhaseISODownloaded,
			},
			{
				NewRepoCreatePhase(deps),
				managementv1beta1.UpgradePlanPhaseRepoCreating,
				managementv1beta1.UpgradePlanPhaseRepoCreated,
			},
			{
				NewMetadataPopulatePhase(deps),
				managementv1beta1.UpgradePlanPhaseMetadataPopulating,
				managementv1beta1.UpgradePlanPhaseMetadataPopulated,
			},
			{
				NewImagePreloadPhase(deps),
				managementv1beta1.UpgradePlanPhaseImagePreloading,
				managementv1beta1.UpgradePlanPhaseImagePreloaded,
			},
			{
				NewClusterUpgradePhase(deps),
				managementv1beta1.UpgradePlanPhaseClusterUpgrading,
				managementv1beta1.UpgradePlanPhaseClusterUpgraded,
			},
			{
				NewNodeUpgradePhase(deps),
				managementv1beta1.UpgradePlanPhaseNodeUpgrading,
				managementv1beta1.UpgradePlanPhaseNodeUpgraded,
			},
			{
				NewImageCleanupPhase(deps),
				managementv1beta1.UpgradePlanPhaseCleaningUp,
				managementv1beta1.UpgradePlanPhaseCleanedUp,
			},
		},
	}
	p.buildIndex()
	return p
}

func (p *Pipeline) buildIndex() {
	p.index = make(map[managementv1beta1.UpgradePlanPhase]int, len(p.phases)*2)
	for i, entry := range p.phases {
		p.index[entry.ActivePhase] = i
		p.index[entry.CompletedPhase] = i
	}
}

// Execute dispatches to the correct phase based on the UpgradePlan's current phase.
func (p *Pipeline) Execute(
	ctx context.Context,
	upgradePlan *managementv1beta1.UpgradePlan,
) (ctrl.Result, error) {
	currentPhase := upgradePlan.Status.CurrentPhase

	// Handle initial and terminal phases
	switch currentPhase {
	case managementv1beta1.UpgradePlanPhaseInitializing, managementv1beta1.UpgradePlanPhaseInitialized, "":
		return p.runInit(ctx, upgradePlan)
	case managementv1beta1.UpgradePlanPhaseFailed:
		return p.runFinalize(ctx, upgradePlan)
	case managementv1beta1.UpgradePlanPhaseSucceeded:
		return ctrl.Result{}, nil
	}

	// Look up current core phase
	idx, found := p.index[currentPhase]
	if !found {
		return ctrl.Result{}, fmt.Errorf("unknown phase: %s", currentPhase)
	}

	entry := p.phases[idx]

	// Completed phase: run PostRun if implemented, then advance
	if currentPhase == entry.CompletedPhase {
		if postRunnable, ok := entry.Phase.(PostRunnable); ok {
			if err := postRunnable.PostRun(ctx, upgradePlan); err != nil {
				return ctrl.Result{}, err
			}
		}

		if isTerminalPhase(upgradePlan.Status.CurrentPhase) {
			return ctrl.Result{}, nil
		}

		p.recordEvent(upgradePlan, corev1.EventTypeNormal, "PhaseCompleted",
			fmt.Sprintf("Completed phase %s", entry.ActivePhase))

		nextIdx := idx + 1
		if nextIdx >= len(p.phases) {
			// All core phases done, enter finalization
			return p.runFinalize(ctx, upgradePlan)
		}
		return p.enterPhase(ctx, upgradePlan, nextIdx)
	}

	// Active phase: run Run (PreRun was already called by enterPhase)
	return entry.Phase.Run(ctx, upgradePlan)
}

func (p *Pipeline) runInit(
	ctx context.Context,
	upgradePlan *managementv1beta1.UpgradePlan,
) (ctrl.Result, error) {
	if upgradePlan.Status.CurrentPhase != managementv1beta1.UpgradePlanPhaseInitialized {
		return p.init.Run(ctx, upgradePlan)
	}

	if postRunnable, ok := p.init.(PostRunnable); ok {
		if err := postRunnable.PostRun(ctx, upgradePlan); err != nil {
			return ctrl.Result{}, err
		}
	}

	if isTerminalPhase(upgradePlan.Status.CurrentPhase) {
		return ctrl.Result{}, nil
	}

	return p.enterPhase(ctx, upgradePlan, 0)
}

func (p *Pipeline) runFinalize(
	ctx context.Context,
	upgradePlan *managementv1beta1.UpgradePlan,
) (ctrl.Result, error) {
	if preRunnable, ok := p.finalize.(PreRunnable); ok {
		if err := preRunnable.PreRun(ctx, upgradePlan); err != nil {
			return ctrl.Result{}, err
		}
	}
	return p.finalize.Run(ctx, upgradePlan)
}

func (p *Pipeline) enterPhase(
	ctx context.Context,
	upgradePlan *managementv1beta1.UpgradePlan,
	idx int,
) (ctrl.Result, error) {
	entry := p.phases[idx]

	if preRunnable, ok := entry.Phase.(PreRunnable); ok {
		if err := preRunnable.PreRun(ctx, upgradePlan); err != nil {
			return ctrl.Result{}, err
		}
	}

	if isTerminalPhase(upgradePlan.Status.CurrentPhase) {
		return ctrl.Result{}, nil
	}

	updateProgressingPhase(upgradePlan, entry.ActivePhase, "")
	p.recordEvent(upgradePlan, corev1.EventTypeNormal, "PhaseTransition",
		fmt.Sprintf("Entering phase %s", entry.ActivePhase))
	return ctrl.Result{RequeueAfter: RequeueAfterDuration}, nil
}

// recordEvent emits a Kubernetes Event on the UpgradePlan if an EventRecorder
// is configured.
func (p *Pipeline) recordEvent(
	upgradePlan *managementv1beta1.UpgradePlan,
	eventType, reason, message string,
) {
	if p.deps != nil && p.deps.EventRecorder != nil {
		p.deps.EventRecorder.Event(upgradePlan, eventType, reason, message)
	}
}
