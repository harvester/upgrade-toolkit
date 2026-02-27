package upgradeplan

import (
	"context"
	"testing"

	"github.com/go-logr/logr"
	provisioningv1 "github.com/rancher/rancher/pkg/apis/provisioning.cattle.io/v1"
	rkev1 "github.com/rancher/rancher/pkg/apis/rke.cattle.io/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	managementv1beta1 "github.com/harvester/upgrade-toolkit/api/v1beta1"
)

func newTestCluster(k8sVersion string, provisionGeneration int) *provisioningv1.Cluster {
	return &provisioningv1.Cluster{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: FleetLocalNamespace,
			Name:      LocalClusterName,
		},
		Spec: provisioningv1.ClusterSpec{
			KubernetesVersion: k8sVersion,
			RKEConfig: &provisioningv1.RKEConfig{
				RKEClusterSpecCommon: rkev1.RKEClusterSpecCommon{
					ProvisionGeneration: provisionGeneration,
				},
			},
		},
	}
}

func newTestUpgradePlanWithMetadata(k8sVersion string) *managementv1beta1.UpgradePlan {
	up := &managementv1beta1.UpgradePlan{
		ObjectMeta: metav1.ObjectMeta{
			Name: testUpgradePlanName,
		},
		Spec: managementv1beta1.UpgradePlanSpec{
			Version: testVersion,
		},
		Status: managementv1beta1.UpgradePlanStatus{
			ReleaseMetadata: &managementv1beta1.ReleaseMetadata{
				Kubernetes: k8sVersion,
			},
		},
	}
	return up
}

func newNodeUpgradePhase(objs ...runtime.Object) *NodeUpgradePhase {
	scheme := runtime.NewScheme()
	_ = managementv1beta1.AddToScheme(scheme)
	_ = provisioningv1.AddToScheme(scheme)

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithRuntimeObjects(objs...).
		Build()

	return NewNodeUpgradePhase(&PhaseDeps{
		Client: fakeClient,
		Scheme: scheme,
		Log:    logr.Discard(),
	})
}

func TestEnsureClusterPatched_FirstCall(t *testing.T) {
	cluster := newTestCluster("v1.30.0+rke2r1", 0)
	up := newTestUpgradePlanWithMetadata("v1.31.0+rke2r1")

	phase := newNodeUpgradePhase(cluster)

	err := phase.ensureClusterPatched(context.Background(), up)
	require.NoError(t, err)

	// Verify Cluster was patched
	var patched provisioningv1.Cluster
	err = phase.Client.Get(context.Background(), types.NamespacedName{
		Namespace: FleetLocalNamespace,
		Name:      LocalClusterName,
	}, &patched)
	require.NoError(t, err)

	assert.Equal(t, "v1.31.0-rke2r1", patched.Spec.KubernetesVersion)
	assert.Equal(t, 1, patched.Spec.RKEConfig.ProvisionGeneration)

	// Verify upgrade strategy
	strategy := patched.Spec.RKEConfig.UpgradeStrategy
	assert.Equal(t, "1", strategy.ControlPlaneConcurrency)
	assert.Equal(t, "1", strategy.WorkerConcurrency)

	// Verify control plane drain options
	cpDrain := strategy.ControlPlaneDrainOptions
	assert.True(t, cpDrain.Enabled)
	assert.True(t, cpDrain.Force)
	require.NotNil(t, cpDrain.IgnoreDaemonSets)
	assert.True(t, *cpDrain.IgnoreDaemonSets)
	assert.True(t, cpDrain.DeleteEmptyDirData)

	// Verify control plane drain hooks
	require.Len(t, cpDrain.PreDrainHooks, 1)
	assert.Equal(t, PreHookAnnotation, cpDrain.PreDrainHooks[0].Annotation)
	require.Len(t, cpDrain.PostDrainHooks, 1)
	assert.Equal(t, PostHookAnnotation, cpDrain.PostDrainHooks[0].Annotation)

	// Verify worker drain options
	workerDrain := strategy.WorkerDrainOptions
	assert.True(t, workerDrain.Enabled)
	assert.True(t, workerDrain.Force)
	require.NotNil(t, workerDrain.IgnoreDaemonSets)
	assert.True(t, *workerDrain.IgnoreDaemonSets)
	assert.True(t, workerDrain.DeleteEmptyDirData)

	// Verify worker drain hooks
	require.Len(t, workerDrain.PreDrainHooks, 1)
	assert.Equal(t, PreHookAnnotation, workerDrain.PreDrainHooks[0].Annotation)
	require.Len(t, workerDrain.PostDrainHooks, 1)
	assert.Equal(t, PostHookAnnotation, workerDrain.PostDrainHooks[0].Annotation)

	// Verify UpgradePlan status field was set
	require.NotNil(t, up.Status.ProvisionGeneration)
	assert.Equal(t, 1, *up.Status.ProvisionGeneration)
}

func TestEnsureClusterPatched_SecondCallIsNoop(t *testing.T) {
	cluster := newTestCluster("v1.31.0-rke2r1", 1)
	up := newTestUpgradePlanWithMetadata("v1.31.0+rke2r1")
	up.Status.ProvisionGeneration = ptr.To(1)

	phase := newNodeUpgradePhase(cluster)

	err := phase.ensureClusterPatched(context.Background(), up)
	require.NoError(t, err)

	// Verify provisionGeneration was NOT incremented (still 1)
	var patched provisioningv1.Cluster
	err = phase.Client.Get(context.Background(), types.NamespacedName{
		Namespace: FleetLocalNamespace,
		Name:      LocalClusterName,
	}, &patched)
	require.NoError(t, err)

	assert.Equal(t, 1, patched.Spec.RKEConfig.ProvisionGeneration)
}

func TestEnsureClusterPatched_SameVersionUpgrade(t *testing.T) {
	// Core bug scenario: same K8s version but no provisionGeneration in status — should still patch
	cluster := newTestCluster("v1.31.0-rke2r1", 5)
	up := newTestUpgradePlanWithMetadata("v1.31.0+rke2r1")

	phase := newNodeUpgradePhase(cluster)

	err := phase.ensureClusterPatched(context.Background(), up)
	require.NoError(t, err)

	// Verify provisionGeneration was incremented
	var patched provisioningv1.Cluster
	err = phase.Client.Get(context.Background(), types.NamespacedName{
		Namespace: FleetLocalNamespace,
		Name:      LocalClusterName,
	}, &patched)
	require.NoError(t, err)

	assert.Equal(t, 6, patched.Spec.RKEConfig.ProvisionGeneration)

	// Verify status field was set
	require.NotNil(t, up.Status.ProvisionGeneration)
	assert.Equal(t, 6, *up.Status.ProvisionGeneration)
}
