package upgradeplan

import (
	"context"
	"testing"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	kubevirtv1 "kubevirt.io/api/core/v1"
)

func newTestKubeVirt(methods []kubevirtv1.WorkloadUpdateMethod) *kubevirtv1.KubeVirt {
	return &kubevirtv1.KubeVirt{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: harvesterSystemNamespace,
			Name:      KubeVirtObjectName,
		},
		Spec: kubevirtv1.KubeVirtSpec{
			WorkloadUpdateStrategy: kubevirtv1.KubeVirtWorkloadUpdateStrategy{
				WorkloadUpdateMethods: methods,
			},
		},
	}
}

func TestUpgradeToolkitManagedFieldsRemovalPatch(t *testing.T) {
	t.Run("no matching manager returns nil", func(t *testing.T) {
		entries := []metav1.ManagedFieldsEntry{{Manager: "fleet-agent"}, {Manager: "virt-operator"}}
		assert.Nil(t, upgradeToolkitManagedFieldsRemovalPatch(entries))
		assert.Nil(t, upgradeToolkitManagedFieldsRemovalPatch(nil))
	})

	t.Run("manager owning every entry returns nil", func(t *testing.T) {
		entries := []metav1.ManagedFieldsEntry{{Manager: upgradeToolkitFieldManager}}
		assert.Nil(t, upgradeToolkitManagedFieldsRemovalPatch(entries))
	})

	t.Run("matching entries are removed from the highest index down", func(t *testing.T) {
		entries := []metav1.ManagedFieldsEntry{
			{Manager: upgradeToolkitFieldManager},
			{Manager: "fleet-agent"},
			{Manager: upgradeToolkitFieldManager},
		}
		patch := upgradeToolkitManagedFieldsRemovalPatch(entries)
		assert.JSONEq(t, `[
			{"op":"test","path":"/metadata/managedFields/2/manager","value":"upgrade-toolkit"},
			{"op":"remove","path":"/metadata/managedFields/2"},
			{"op":"test","path":"/metadata/managedFields/0/manager","value":"upgrade-toolkit"},
			{"op":"remove","path":"/metadata/managedFields/0"}
		]`, string(patch))
	})
}

func TestRemoveKubeVirtFieldManagerEntries(t *testing.T) {
	kv := newTestKubeVirt([]kubevirtv1.WorkloadUpdateMethod{})
	kv.ManagedFields = []metav1.ManagedFieldsEntry{
		{Manager: "fleet-agent", Operation: metav1.ManagedFieldsOperationApply, APIVersion: "kubevirt.io/v1"},
		{Manager: upgradeToolkitFieldManager, Operation: metav1.ManagedFieldsOperationUpdate, APIVersion: "kubevirt.io/v1"},
	}
	c := newFakeClient(kv)

	require.NoError(t, removeKubeVirtFieldManagerEntries(context.Background(), c, logr.Discard()))

	var after kubevirtv1.KubeVirt
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{
		Namespace: harvesterSystemNamespace,
		Name:      KubeVirtObjectName,
	}, &after))
	require.Len(t, after.ManagedFields, 1)
	assert.Equal(t, "fleet-agent", after.ManagedFields[0].Manager)

	// Idempotent: nothing left to remove.
	require.NoError(t, removeKubeVirtFieldManagerEntries(context.Background(), c, logr.Discard()))
}

func TestRemoveKubeVirtFieldManagerEntries_MissingKubeVirt(t *testing.T) {
	c := newFakeClient()
	require.NoError(t, removeKubeVirtFieldManagerEntries(context.Background(), c, logr.Discard()))
}

func TestGetKubeVirt_NotFoundReturnsNil(t *testing.T) {
	c := newFakeClient()
	u, err := getKubeVirt(context.Background(), c)
	require.NoError(t, err)
	assert.Nil(t, u, "getKubeVirt must return (nil, nil) when the CR is absent")
}

func TestGetKubeVirt_ReturnsObject(t *testing.T) {
	kv := newTestKubeVirt([]kubevirtv1.WorkloadUpdateMethod{kubevirtv1.WorkloadUpdateMethodLiveMigrate})
	c := newFakeClient(kv)

	u, err := getKubeVirt(context.Background(), c)
	require.NoError(t, err)
	require.NotNil(t, u)
	assert.Equal(t, KubeVirtObjectName, u.GetName())
	assert.Equal(t, harvesterSystemNamespace, u.GetNamespace())

	methods, found, err := unstructured.NestedStringSlice(u.Object,
		"spec", "workloadUpdateStrategy", "workloadUpdateMethods")
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, []string{"LiveMigrate"}, methods)
}
