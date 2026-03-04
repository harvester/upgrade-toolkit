package upgradeplan

import (
	"context"
	"testing"

	"github.com/go-logr/logr"
	"github.com/harvester/go-common/version"
	upgradev1 "github.com/rancher/system-upgrade-controller/pkg/apis/upgrade.cattle.io/v1"
	"github.com/rancher/wrangler/v3/pkg/name"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	managementv1beta1 "github.com/harvester/upgrade-toolkit/api/v1beta1"
)

func newImagePreloadPhase(objs ...client.Object) *ImagePreloadPhase {
	scheme := runtime.NewScheme()
	_ = managementv1beta1.AddToScheme(scheme)
	_ = upgradev1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objs...).
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

func TestConstructPlanForImagePreload(t *testing.T) {
	up := &managementv1beta1.UpgradePlan{
		ObjectMeta: metav1.ObjectMeta{
			Name: testUpgradePlanName,
		},
		Spec: managementv1beta1.UpgradePlanSpec{
			Version: "v1.2.0",
		},
	}

	plan := constructPlanForImagePreload(up, 3, "system-upgrade-controller")

	assert.Equal(t, name.SafeConcatName(testUpgradePlanName, PrepareComponent), plan.Name)
	assert.Equal(t, cattleSystemNamespace, plan.Namespace)
	assert.Equal(t, testUpgradePlanName, plan.Labels[HarvesterUpgradePlanLabel])
	assert.Equal(t, PrepareComponent, plan.Labels[HarvesterUpgradeComponentLabel])

	// Spec
	assert.Equal(t, int64(3), plan.Spec.Concurrency)
	assert.Equal(t, "system-upgrade-controller", plan.Spec.ServiceAccountName)
	assert.Equal(t, "v1.2.0", plan.Spec.Version)

	// NodeSelector
	require.NotNil(t, plan.Spec.NodeSelector)
	assert.Equal(t, "true", plan.Spec.NodeSelector.MatchLabels[harvesterManagedLabel])

	// Container
	require.NotNil(t, plan.Spec.Upgrade)
	assert.Equal(t, upgradeToolkitImage, plan.Spec.Upgrade.Image)
	assert.Equal(t, []string{"upgrade_node.sh"}, plan.Spec.Upgrade.Command)
	assert.Equal(t, []string{"prepare"}, plan.Spec.Upgrade.Args)

	// Env
	require.Len(t, plan.Spec.Upgrade.Env, 1)
	assert.Equal(t, "HARVESTER_UPGRADEPLAN_NAME", plan.Spec.Upgrade.Env[0].Name)
	assert.Equal(t, testUpgradePlanName, plan.Spec.Upgrade.Env[0].Value)

	// Tolerations
	assert.NotEmpty(t, plan.Spec.Tolerations)

	// No drain/cordon
	assert.False(t, plan.Spec.Cordon)
	assert.Nil(t, plan.Spec.Drain)
}

func newManagedNode(nodeName string) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: nodeName,
			Labels: map[string]string{
				harvesterManagedLabel: "true",
			},
		},
	}
}

func TestImagePreloadPhase_Run_NegativeConcurrency_SkipsPreloading(t *testing.T) {
	nodes := []client.Object{
		newManagedNode("node-1"),
		newManagedNode("node-2"),
		newManagedNode("node-3"),
	}

	up := &managementv1beta1.UpgradePlan{
		ObjectMeta: metav1.ObjectMeta{
			Name: testUpgradePlanName,
		},
		Spec: managementv1beta1.UpgradePlanSpec{
			Version: "v1.2.0",
			ImagePreloadOption: &managementv1beta1.ImagePreloadOption{
				Concurrency: ptr.To(-1),
			},
		},
		Status: managementv1beta1.UpgradePlanStatus{
			CurrentPhase: managementv1beta1.UpgradePlanPhaseImagePreloading,
		},
	}

	phase := newImagePreloadPhase(nodes...)
	_, err := phase.Run(context.Background(), up)
	require.NoError(t, err)
	assert.Equal(t, managementv1beta1.UpgradePlanPhaseImagePreloaded, up.Status.CurrentPhase)

	// Verify no SUC plan was created
	var plan upgradev1.Plan
	err = phase.Client.Get(context.Background(), types.NamespacedName{
		Namespace: cattleSystemNamespace,
		Name:      name.SafeConcatName(testUpgradePlanName, PrepareComponent),
	}, &plan)
	assert.Error(t, err, "SUC plan should not be created when concurrency is negative")
}

func TestImagePreloadPhase_Run_DefaultConcurrency_AllNodes(t *testing.T) {
	nodes := []client.Object{
		newManagedNode("node-1"),
		newManagedNode("node-2"),
		newManagedNode("node-3"),
	}

	up := &managementv1beta1.UpgradePlan{
		ObjectMeta: metav1.ObjectMeta{
			Name: testUpgradePlanName,
		},
		Spec: managementv1beta1.UpgradePlanSpec{
			Version: "v1.2.0",
		},
		Status: managementv1beta1.UpgradePlanStatus{
			CurrentPhase: managementv1beta1.UpgradePlanPhaseImagePreloading,
		},
	}

	phase := newImagePreloadPhase(nodes...)
	_, err := phase.Run(context.Background(), up)
	require.NoError(t, err)

	// Verify SUC plan was created with concurrency == node count
	var plan upgradev1.Plan
	err = phase.Client.Get(context.Background(), types.NamespacedName{
		Namespace: cattleSystemNamespace,
		Name:      name.SafeConcatName(testUpgradePlanName, PrepareComponent),
	}, &plan)
	require.NoError(t, err)
	assert.Equal(t, int64(3), plan.Spec.Concurrency)
}

func TestImagePreloadPhase_Run_PositiveConcurrency_Capped(t *testing.T) {
	nodes := []client.Object{
		newManagedNode("node-1"),
		newManagedNode("node-2"),
	}

	up := &managementv1beta1.UpgradePlan{
		ObjectMeta: metav1.ObjectMeta{
			Name: testUpgradePlanName,
		},
		Spec: managementv1beta1.UpgradePlanSpec{
			Version: "v1.2.0",
			ImagePreloadOption: &managementv1beta1.ImagePreloadOption{
				Concurrency: ptr.To(5),
			},
		},
		Status: managementv1beta1.UpgradePlanStatus{
			CurrentPhase: managementv1beta1.UpgradePlanPhaseImagePreloading,
		},
	}

	phase := newImagePreloadPhase(nodes...)
	_, err := phase.Run(context.Background(), up)
	require.NoError(t, err)

	var plan upgradev1.Plan
	err = phase.Client.Get(context.Background(), types.NamespacedName{
		Namespace: cattleSystemNamespace,
		Name:      name.SafeConcatName(testUpgradePlanName, PrepareComponent),
	}, &plan)
	require.NoError(t, err)
	assert.Equal(t, int64(2), plan.Spec.Concurrency)
}

func TestImagePreloadPhase_Run_PositiveConcurrency_Normal(t *testing.T) {
	nodes := []client.Object{
		newManagedNode("node-1"),
		newManagedNode("node-2"),
		newManagedNode("node-3"),
	}

	up := &managementv1beta1.UpgradePlan{
		ObjectMeta: metav1.ObjectMeta{
			Name: testUpgradePlanName,
		},
		Spec: managementv1beta1.UpgradePlanSpec{
			Version: "v1.2.0",
			ImagePreloadOption: &managementv1beta1.ImagePreloadOption{
				Concurrency: ptr.To(2),
			},
		},
		Status: managementv1beta1.UpgradePlanStatus{
			CurrentPhase: managementv1beta1.UpgradePlanPhaseImagePreloading,
		},
	}

	phase := newImagePreloadPhase(nodes...)
	_, err := phase.Run(context.Background(), up)
	require.NoError(t, err)

	var plan upgradev1.Plan
	err = phase.Client.Get(context.Background(), types.NamespacedName{
		Namespace: cattleSystemNamespace,
		Name:      name.SafeConcatName(testUpgradePlanName, PrepareComponent),
	}, &plan)
	require.NoError(t, err)
	assert.Equal(t, int64(2), plan.Spec.Concurrency)
}
