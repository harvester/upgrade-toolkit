package upgradeplan

import (
	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	managementv1beta1 "github.com/harvester/upgrade-toolkit/api/v1beta1"
)

// PhaseDeps holds shared dependencies injected into all phases.
type PhaseDeps struct {
	Client client.Client
	Scheme *runtime.Scheme
	Log    logr.Logger

	// EventRecorder emits Kubernetes Events on UpgradePlan resources.
	EventRecorder record.EventRecorder

	// JobServiceAccount is the ServiceAccount name used by Jobs created during
	// cluster-upgrade and node-upgrade phases.
	JobServiceAccount string
	// PlanServiceAccount is the ServiceAccount name used by SUC Plans created
	// during the image-preload phase.
	PlanServiceAccount string
}

// RecordEvent emits a Kubernetes Event on the given UpgradePlan in the default
// namespace. It is a no-op when the EventRecorder has not been set.
func (d *PhaseDeps) RecordEvent(
	upgradePlan *managementv1beta1.UpgradePlan,
	eventType, reason, message string,
) {
	if d != nil && d.EventRecorder != nil {
		d.EventRecorder.Event(upgradePlan.ObjectReference(eventNamespace), eventType, reason, message)
	}
}
