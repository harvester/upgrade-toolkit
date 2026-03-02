package upgradeplan

import (
	"context"
	"testing"

	"github.com/go-logr/logr"
	"github.com/harvester/go-common/version"
	upgradev1 "github.com/rancher/system-upgrade-controller/pkg/apis/upgrade.cattle.io/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	managementv1beta1 "github.com/harvester/upgrade-toolkit/api/v1beta1"
)

func newImagePreloadPhase() *ImagePreloadPhase {
	scheme := runtime.NewScheme()
	_ = managementv1beta1.AddToScheme(scheme)
	_ = upgradev1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		Build()

	return NewImagePreloadPhase(&PhaseDeps{
		Client:             fakeClient,
		Scheme:             scheme,
		Log:                logr.Discard(),
		JobServiceAccount:  "harvester",
		PlanServiceAccount: "system-upgrade-controller",
	})
}

func TestImagePreloadPhase_PreRun_EligibleUpgrade(t *testing.T) {
	up := &managementv1beta1.UpgradePlan{
		ObjectMeta: metav1.ObjectMeta{
			Name: testUpgradePlanName,
		},
		Spec: managementv1beta1.UpgradePlanSpec{
			Version: "v1.2.2",
		},
		Status: managementv1beta1.UpgradePlanStatus{
			PreviousVersion: ptr.To("v1.2.1"),
			ReleaseMetadata: &managementv1beta1.ReleaseMetadata{
				Harvester:            "v1.2.2",
				MinUpgradableVersion: "v1.2.1",
			},
		},
	}

	phase := newImagePreloadPhase()
	err := phase.PreRun(context.Background(), up)
	require.NoError(t, err)
	// Phase should not be changed to Failed
	assert.NotEqual(t, managementv1beta1.UpgradePlanPhaseFailed, up.Status.CurrentPhase)
}

func TestImagePreloadPhase_PreRun_IneligibleBelowMinVersion(t *testing.T) {
	up := &managementv1beta1.UpgradePlan{
		ObjectMeta: metav1.ObjectMeta{
			Name: testUpgradePlanName,
		},
		Spec: managementv1beta1.UpgradePlanSpec{
			Version: "v1.2.2",
		},
		Status: managementv1beta1.UpgradePlanStatus{
			PreviousVersion: ptr.To("v1.2.0"),
			ReleaseMetadata: &managementv1beta1.ReleaseMetadata{
				Harvester:            "v1.2.2",
				MinUpgradableVersion: "v1.2.1",
			},
			CurrentPhase: managementv1beta1.UpgradePlanPhaseMetadataPopulated,
		},
	}
	up.SetCondition(managementv1beta1.UpgradePlanProgressing, metav1.ConditionTrue, "MetadataPopulated", "")

	phase := newImagePreloadPhase()
	err := phase.PreRun(context.Background(), up)
	require.NoError(t, err)
	assert.Equal(t, managementv1beta1.UpgradePlanPhaseFailed, up.Status.CurrentPhase)
	cond := up.LookupCondition(managementv1beta1.UpgradePlanProgressing)
	assert.Equal(t, version.ErrMinUpgradeRequirement.Error(), cond.Message)
}

func TestImagePreloadPhase_PreRun_Downgrade(t *testing.T) {
	up := &managementv1beta1.UpgradePlan{
		ObjectMeta: metav1.ObjectMeta{
			Name: testUpgradePlanName,
		},
		Spec: managementv1beta1.UpgradePlanSpec{
			Version: "v1.2.0",
		},
		Status: managementv1beta1.UpgradePlanStatus{
			PreviousVersion: ptr.To("v1.2.1"),
			ReleaseMetadata: &managementv1beta1.ReleaseMetadata{
				Harvester:            "v1.2.0",
				MinUpgradableVersion: "v1.1.2",
			},
			CurrentPhase: managementv1beta1.UpgradePlanPhaseMetadataPopulated,
		},
	}
	up.SetCondition(managementv1beta1.UpgradePlanProgressing, metav1.ConditionTrue, "MetadataPopulated", "")

	phase := newImagePreloadPhase()
	err := phase.PreRun(context.Background(), up)
	require.NoError(t, err)
	assert.Equal(t, managementv1beta1.UpgradePlanPhaseFailed, up.Status.CurrentPhase)
	cond := up.LookupCondition(managementv1beta1.UpgradePlanProgressing)
	assert.Equal(t, version.ErrDowngrade.Error(), cond.Message)
}

func TestImagePreloadPhase_PreRun_ForceBypassesCheck(t *testing.T) {
	up := &managementv1beta1.UpgradePlan{
		ObjectMeta: metav1.ObjectMeta{
			Name: testUpgradePlanName,
		},
		Spec: managementv1beta1.UpgradePlanSpec{
			Version: "v1.2.2",
			Force:   ptr.To(true),
		},
		Status: managementv1beta1.UpgradePlanStatus{
			PreviousVersion: ptr.To("v1.2.0"),
			ReleaseMetadata: &managementv1beta1.ReleaseMetadata{
				Harvester:            "v1.2.2",
				MinUpgradableVersion: "v1.2.1",
			},
		},
	}

	phase := newImagePreloadPhase()
	err := phase.PreRun(context.Background(), up)
	require.NoError(t, err)
	// Phase should NOT be set to Failed even though current version is below minimum
	assert.NotEqual(t, managementv1beta1.UpgradePlanPhaseFailed, up.Status.CurrentPhase)
}

func TestImagePreloadPhase_PreRun_MissingReleaseMetadata(t *testing.T) {
	up := &managementv1beta1.UpgradePlan{
		ObjectMeta: metav1.ObjectMeta{
			Name: testUpgradePlanName,
		},
		Spec: managementv1beta1.UpgradePlanSpec{
			Version: "v1.2.2",
		},
		Status: managementv1beta1.UpgradePlanStatus{
			PreviousVersion: ptr.To("v1.2.1"),
			// ReleaseMetadata is nil — should not happen at this stage
		},
	}

	phase := newImagePreloadPhase()
	err := phase.PreRun(context.Background(), up)
	require.NoError(t, err)
	assert.Equal(t, managementv1beta1.UpgradePlanPhaseFailed, up.Status.CurrentPhase)
}

func TestImagePreloadPhase_PreRun_DevToStableStrictMode(t *testing.T) {
	up := &managementv1beta1.UpgradePlan{
		ObjectMeta: metav1.ObjectMeta{
			Name: testUpgradePlanName,
		},
		Spec: managementv1beta1.UpgradePlanSpec{
			Version: "v1.3.1",
		},
		Status: managementv1beta1.UpgradePlanStatus{
			PreviousVersion: ptr.To("v1.2-head"),
			ReleaseMetadata: &managementv1beta1.ReleaseMetadata{
				Harvester:            "v1.3.1",
				MinUpgradableVersion: "v1.2.2",
			},
			CurrentPhase: managementv1beta1.UpgradePlanPhaseMetadataPopulated,
		},
	}
	up.SetCondition(managementv1beta1.UpgradePlanProgressing, metav1.ConditionTrue, "MetadataPopulated", "")

	phase := newImagePreloadPhase()
	err := phase.PreRun(context.Background(), up)
	require.NoError(t, err)
	assert.Equal(t, managementv1beta1.UpgradePlanPhaseFailed, up.Status.CurrentPhase)
	cond := up.LookupCondition(managementv1beta1.UpgradePlanProgressing)
	assert.Equal(t, version.ErrDevUpgrade.Error(), cond.Message)
}
