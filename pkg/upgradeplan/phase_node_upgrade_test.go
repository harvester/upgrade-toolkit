package upgradeplan

import (
	"context"
	"testing"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	managementv1beta1 "github.com/harvester/upgrade-toolkit/api/v1beta1"
)

func newTestCluster(k8sVersion string, provisionGeneration int64) *unstructured.Unstructured {
	cluster := &unstructured.Unstructured{}
	cluster.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "provisioning.cattle.io",
		Version: "v1",
		Kind:    "Cluster",
	})
	cluster.SetNamespace(FleetLocalNamespace)
	cluster.SetName(LocalClusterName)

	_ = unstructured.SetNestedField(cluster.Object, k8sVersion, "spec", "kubernetesVersion")
	_ = unstructured.SetNestedField(cluster.Object, provisionGeneration, "spec", "rkeConfig", "provisionGeneration")

	return cluster
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

	clusterGVK := schema.GroupVersionKind{
		Group:   "provisioning.cattle.io",
		Version: "v1",
		Kind:    "Cluster",
	}
	scheme.AddKnownTypeWithName(clusterGVK, &unstructured.Unstructured{})
	clusterListGVK := schema.GroupVersionKind{
		Group:   "provisioning.cattle.io",
		Version: "v1",
		Kind:    "ClusterList",
	}
	scheme.AddKnownTypeWithName(clusterListGVK, &unstructured.UnstructuredList{})

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
	cluster := newTestCluster("v1.30.0+rke2r1", int64(0))
	up := newTestUpgradePlanWithMetadata("v1.31.0+rke2r1")

	phase := newNodeUpgradePhase(cluster)

	err := phase.ensureClusterPatched(context.Background(), up)
	require.NoError(t, err)

	// Verify Cluster was patched
	patched := &unstructured.Unstructured{}
	patched.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "provisioning.cattle.io",
		Version: "v1",
		Kind:    "Cluster",
	})
	err = phase.Client.Get(context.Background(), types.NamespacedName{
		Namespace: FleetLocalNamespace,
		Name:      LocalClusterName,
	}, patched)
	require.NoError(t, err)

	k8sVer, _, _ := unstructured.NestedString(patched.Object, "spec", "kubernetesVersion")
	assert.Equal(t, "v1.31.0-rke2r1", k8sVer)

	gen, _, _ := unstructured.NestedInt64(patched.Object, "spec", "rkeConfig", "provisionGeneration")
	assert.Equal(t, int64(1), gen)

	// Verify UpgradePlan status field was set
	require.NotNil(t, up.Status.ProvisionGeneration)
	assert.Equal(t, int64(1), *up.Status.ProvisionGeneration)
}

func TestEnsureClusterPatched_SecondCallIsNoop(t *testing.T) {
	cluster := newTestCluster("v1.31.0-rke2r1", int64(1))
	up := newTestUpgradePlanWithMetadata("v1.31.0+rke2r1")
	up.Status.ProvisionGeneration = ptr.To[int64](1)

	phase := newNodeUpgradePhase(cluster)

	err := phase.ensureClusterPatched(context.Background(), up)
	require.NoError(t, err)

	// Verify provisionGeneration was NOT incremented (still 1)
	patched := &unstructured.Unstructured{}
	patched.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "provisioning.cattle.io",
		Version: "v1",
		Kind:    "Cluster",
	})
	err = phase.Client.Get(context.Background(), types.NamespacedName{
		Namespace: FleetLocalNamespace,
		Name:      LocalClusterName,
	}, patched)
	require.NoError(t, err)

	gen, _, _ := unstructured.NestedInt64(patched.Object, "spec", "rkeConfig", "provisionGeneration")
	assert.Equal(t, int64(1), gen)
}

func TestEnsureClusterPatched_SameVersionUpgrade(t *testing.T) {
	// Core bug scenario: same K8s version but no provisionGeneration in status — should still patch
	cluster := newTestCluster("v1.31.0-rke2r1", int64(5))
	up := newTestUpgradePlanWithMetadata("v1.31.0+rke2r1")

	phase := newNodeUpgradePhase(cluster)

	err := phase.ensureClusterPatched(context.Background(), up)
	require.NoError(t, err)

	// Verify provisionGeneration was incremented
	patched := &unstructured.Unstructured{}
	patched.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "provisioning.cattle.io",
		Version: "v1",
		Kind:    "Cluster",
	})
	err = phase.Client.Get(context.Background(), types.NamespacedName{
		Namespace: FleetLocalNamespace,
		Name:      LocalClusterName,
	}, patched)
	require.NoError(t, err)

	gen, _, _ := unstructured.NestedInt64(patched.Object, "spec", "rkeConfig", "provisionGeneration")
	assert.Equal(t, int64(6), gen)

	// Verify status field was set
	require.NotNil(t, up.Status.ProvisionGeneration)
	assert.Equal(t, int64(6), *up.Status.ProvisionGeneration)
}
