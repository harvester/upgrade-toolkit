package upgradeplan

import (
	"context"
	"testing"

	"github.com/go-logr/logr"
	upgradev1 "github.com/rancher/system-upgrade-controller/pkg/apis/upgrade.cattle.io/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	managementv1beta1 "github.com/harvester/upgrade-toolkit/api/v1beta1"
)

// fakePostRunFailPhase is a test double that sets the plan to Failed in PostRun.
type fakePostRunFailPhase struct{}

func (f *fakePostRunFailPhase) Name() string { return "FakePostRunFail" }
func (f *fakePostRunFailPhase) Run(_ context.Context, _ *managementv1beta1.UpgradePlan) (ctrl.Result, error) {
	return ctrl.Result{}, nil
}
func (f *fakePostRunFailPhase) PostRun(_ context.Context, up *managementv1beta1.UpgradePlan) error {
	updateProgressingPhase(up, managementv1beta1.UpgradePlanPhaseFailed, "post-run check failed")
	return nil
}

// fakeNoopPhase is a test double that does nothing.
type fakeNoopPhase struct{}

func (f *fakeNoopPhase) Name() string { return "FakeNoop" }
func (f *fakeNoopPhase) Run(_ context.Context, _ *managementv1beta1.UpgradePlan) (ctrl.Result, error) {
	return ctrl.Result{}, nil
}

func newTestPipeline() *Pipeline {
	scheme := runtime.NewScheme()
	_ = managementv1beta1.AddToScheme(scheme)
	_ = upgradev1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		Build()

	deps := &PhaseDeps{
		Client:             fakeClient,
		Scheme:             scheme,
		Log:                logr.Discard(),
		JobServiceAccount:  "harvester",
		PlanServiceAccount: "system-upgrade-controller",
	}

	return NewPipeline(deps)
}

func TestPipeline_Execute_PreRunFailedPhaseNotOverwritten(t *testing.T) {
	up := &managementv1beta1.UpgradePlan{
		ObjectMeta: metav1.ObjectMeta{
			Name: testUpgradePlanName,
		},
		Spec: managementv1beta1.UpgradePlanSpec{
			Version: "v1.6.1",
		},
		Status: managementv1beta1.UpgradePlanStatus{
			PreviousVersion: ptr.To("v1.7.1"),
			ReleaseMetadata: &managementv1beta1.ReleaseMetadata{
				Harvester:            "v1.6.1",
				MinUpgradableVersion: "v1.5.0",
			},
			CurrentPhase: managementv1beta1.UpgradePlanPhaseMetadataPopulated,
		},
	}
	up.SetCondition(managementv1beta1.UpgradePlanProgressing, metav1.ConditionTrue, "MetadataPopulated", "")

	pipeline := newTestPipeline()
	_, err := pipeline.Execute(context.Background(), up)
	require.NoError(t, err)

	// The phase must remain Failed, not be overwritten with ImagePreloading
	assert.Equal(t, managementv1beta1.UpgradePlanPhaseFailed, up.Status.CurrentPhase)
}

func TestPipeline_Execute_PostRunFailedPhaseNotOverwritten(t *testing.T) {
	// Build a two-phase pipeline where phase-0 PostRun sets Failed.
	p := &Pipeline{
		init:     &fakeNoopPhase{},
		finalize: &fakeNoopPhase{},
		phases: []PhaseEntry{
			{
				Phase:          &fakePostRunFailPhase{},
				ActivePhase:    "PhaseAActive",
				CompletedPhase: "PhaseACompleted",
			},
			{
				Phase:          &fakeNoopPhase{},
				ActivePhase:    "PhaseBActive",
				CompletedPhase: "PhaseBCompleted",
			},
		},
	}
	p.buildIndex()

	up := &managementv1beta1.UpgradePlan{
		ObjectMeta: metav1.ObjectMeta{
			Name: testUpgradePlanName,
		},
		Spec: managementv1beta1.UpgradePlanSpec{
			Version: "v1.2.0",
		},
		Status: managementv1beta1.UpgradePlanStatus{
			CurrentPhase: "PhaseACompleted",
		},
	}
	up.SetCondition(managementv1beta1.UpgradePlanProgressing, metav1.ConditionTrue, "PhaseACompleted", "")

	_, err := p.Execute(context.Background(), up)
	require.NoError(t, err)

	// PostRun set Failed; pipeline must not advance to PhaseBActive
	assert.Equal(t, managementv1beta1.UpgradePlanPhaseFailed, up.Status.CurrentPhase)
}

func TestPipeline_Execute_InitPostRunFailedPhaseNotOverwritten(t *testing.T) {
	// Build a pipeline where init PostRun sets Failed.
	p := &Pipeline{
		init:     &fakePostRunFailPhase{},
		finalize: &fakeNoopPhase{},
		phases: []PhaseEntry{
			{
				Phase:          &fakeNoopPhase{},
				ActivePhase:    "PhaseAActive",
				CompletedPhase: "PhaseACompleted",
			},
		},
	}
	p.buildIndex()

	up := &managementv1beta1.UpgradePlan{
		ObjectMeta: metav1.ObjectMeta{
			Name: testUpgradePlanName,
		},
		Spec: managementv1beta1.UpgradePlanSpec{
			Version: "v1.2.0",
		},
		Status: managementv1beta1.UpgradePlanStatus{
			CurrentPhase: managementv1beta1.UpgradePlanPhaseInitialized,
		},
	}
	up.SetCondition(managementv1beta1.UpgradePlanProgressing, metav1.ConditionTrue, "Initialized", "")

	_, err := p.Execute(context.Background(), up)
	require.NoError(t, err)

	// Init PostRun set Failed; pipeline must not enter phase-0
	assert.Equal(t, managementv1beta1.UpgradePlanPhaseFailed, up.Status.CurrentPhase)
}
