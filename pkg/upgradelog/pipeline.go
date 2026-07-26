package upgradelog

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"

	managementv1beta1 "github.com/harvester/upgrade-toolkit/api/v1beta1"
)

// PhaseEntry ties a Runnable to its API phase constants.
type PhaseEntry struct {
	Phase          Runnable
	ActivePhase    managementv1beta1.UpgradeLogPhase
	CompletedPhase managementv1beta1.UpgradeLogPhase
}

// Pipeline is an ordered sequence of phases that drives the UpgradeLog lifecycle.
type Pipeline struct {
	deps   *PhaseDeps
	phases []PhaseEntry
	index  map[managementv1beta1.UpgradeLogPhase]int
}

// NewPipeline creates a new UpgradeLog pipeline with all phases wired up.
func NewPipeline(deps *PhaseDeps) *Pipeline {
	p := &Pipeline{
		deps: deps,
		phases: []PhaseEntry{
			{
				NewCollectorDeployPhase(deps),
				managementv1beta1.UpgradeLogPhaseCollectorDeploying,
				managementv1beta1.UpgradeLogPhaseCollectorDeployed,
			},
			{
				NewCollectPhase(deps),
				managementv1beta1.UpgradeLogPhaseCollecting,
				managementv1beta1.UpgradeLogPhaseCollected,
			},
			{
				NewStopPhase(deps),
				managementv1beta1.UpgradeLogPhaseStopping,
				managementv1beta1.UpgradeLogPhaseStopped,
			},
		},
	}
	p.buildIndex()
	return p
}

func (p *Pipeline) buildIndex() {
	p.index = make(map[managementv1beta1.UpgradeLogPhase]int, len(p.phases)*2)
	for i, entry := range p.phases {
		p.index[entry.ActivePhase] = i
		if entry.CompletedPhase != "" {
			p.index[entry.CompletedPhase] = i
		}
	}
}

// contextWithLog returns a context enriched with a logger that includes
// upgradeLog and phase as structured fields.
func (p *Pipeline) contextWithLog(ctx context.Context, upgradeLog string, phase string) context.Context {
	var log logr.Logger
	if p.deps != nil {
		log = p.deps.Log
	} else {
		log = logr.Discard()
	}
	log = log.WithValues("upgradeLog", upgradeLog, "phase", phase)
	return logr.NewContext(ctx, log)
}

// Execute dispatches to the correct phase based on the UpgradeLog's current phase.
func (p *Pipeline) Execute(
	ctx context.Context,
	upgradeLog *managementv1beta1.UpgradeLog,
) (ctrl.Result, error) {
	currentPhase := upgradeLog.Status.CurrentPhase

	// First reconcile: enter first phase
	if currentPhase == "" {
		return p.enterPhase(ctx, upgradeLog, 0)
	}

	// Terminal phases
	if currentPhase == managementv1beta1.UpgradeLogPhaseStopped ||
		currentPhase == managementv1beta1.UpgradeLogPhaseFailed {
		return ctrl.Result{}, nil
	}

	// Look up current phase
	idx, found := p.index[currentPhase]
	if !found {
		return ctrl.Result{}, fmt.Errorf("unknown phase: %s", currentPhase)
	}

	entry := p.phases[idx]
	ctx = p.contextWithLog(ctx, upgradeLog.Name, entry.Phase.Name())

	// Completed phase: run PostRun if implemented, then advance
	if entry.CompletedPhase != "" && currentPhase == entry.CompletedPhase {
		if postRunnable, ok := entry.Phase.(PostRunnable); ok {
			if err := postRunnable.PostRun(ctx, upgradeLog); err != nil {
				return ctrl.Result{}, err
			}
		}

		p.recordEvent(upgradeLog, corev1.EventTypeNormal, "PhaseCompleted",
			fmt.Sprintf("Completed phase %s", entry.Phase.Name()))

		nextIdx := idx + 1
		if nextIdx >= len(p.phases) {
			return ctrl.Result{}, nil
		}
		return p.enterPhase(ctx, upgradeLog, nextIdx)
	}

	// Active phase: run Run
	return entry.Phase.Run(ctx, upgradeLog)
}

func (p *Pipeline) enterPhase(
	ctx context.Context,
	upgradeLog *managementv1beta1.UpgradeLog,
	idx int,
) (ctrl.Result, error) {
	entry := p.phases[idx]
	ctx = p.contextWithLog(ctx, upgradeLog.Name, entry.Phase.Name())

	if preRunnable, ok := entry.Phase.(PreRunnable); ok {
		if err := preRunnable.PreRun(ctx, upgradeLog); err != nil {
			return ctrl.Result{}, err
		}
	}

	upgradeLog.Status.CurrentPhase = entry.ActivePhase
	p.recordEvent(upgradeLog, corev1.EventTypeNormal, "PhaseTransition",
		fmt.Sprintf("Entering phase %s", entry.Phase.Name()))
	return ctrl.Result{RequeueAfter: RequeueAfterDuration}, nil
}

func (p *Pipeline) recordEvent(
	upgradeLog *managementv1beta1.UpgradeLog,
	eventType, reason, message string,
) {
	p.deps.RecordEvent(upgradeLog, eventType, reason, message)
}
