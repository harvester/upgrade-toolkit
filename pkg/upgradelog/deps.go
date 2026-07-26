package upgradelog

import (
	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	managementv1beta1 "github.com/harvester/upgrade-toolkit/api/v1beta1"
)

// PhaseDeps holds shared dependencies injected into all UpgradeLog phases.
type PhaseDeps struct {
	Client client.Client
	Scheme *runtime.Scheme
	Log    logr.Logger

	// EventRecorder emits Kubernetes Events on UpgradeLog resources.
	EventRecorder record.EventRecorder
}

// RecordEvent emits a Kubernetes Event on the given UpgradeLog.
// It is a no-op when the EventRecorder has not been set.
func (d *PhaseDeps) RecordEvent(
	upgradeLog *managementv1beta1.UpgradeLog,
	eventType, reason, message string,
) {
	if d != nil && d.EventRecorder != nil {
		d.EventRecorder.Event(upgradeLog.ObjectReference(), eventType, reason, message)
	}
}
