package upgradeplan

import (
	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// PhaseDeps holds shared dependencies injected into all phases.
type PhaseDeps struct {
	Client client.Client
	Scheme *runtime.Scheme
	Log    logr.Logger

	// JobServiceAccount is the ServiceAccount name used by Jobs created during
	// cluster-upgrade and node-upgrade phases.
	JobServiceAccount string
	// PlanServiceAccount is the ServiceAccount name used by SUC Plans created
	// during the image-preload phase.
	PlanServiceAccount string
}
