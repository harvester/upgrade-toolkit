package upgradelog

import (
	"context"

	ctrl "sigs.k8s.io/controller-runtime"

	managementv1beta1 "github.com/harvester/upgrade-toolkit/api/v1beta1"
)

// Runnable is the core phase interface. Run() is called on every reconcile
// loop while the phase is active. Implementations MUST be idempotent.
type Runnable interface {
	Run(ctx context.Context, upgradeLog *managementv1beta1.UpgradeLog) (ctrl.Result, error)
	Name() string
}

// PreRunnable is optionally implemented by phases that need setup before
// Run() on each reconcile. Implementations MUST be idempotent.
type PreRunnable interface {
	PreRun(ctx context.Context, upgradeLog *managementv1beta1.UpgradeLog) error
}

// PostRunnable is optionally implemented by phases that need teardown after
// Run() signals completion. Implementations MUST be idempotent.
type PostRunnable interface {
	PostRun(ctx context.Context, upgradeLog *managementv1beta1.UpgradeLog) error
}
