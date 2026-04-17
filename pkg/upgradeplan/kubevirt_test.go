package upgradeplan

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
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

func TestSetKubeVirtWorkloadUpdateMethods(t *testing.T) {
	cases := []struct {
		name         string
		initial      []kubevirtv1.WorkloadUpdateMethod
		desired      []string
		expectMethod *kubevirtv1.WorkloadUpdateMethod // nil means expect empty/nil
	}{
		{
			name:         "empty to empty is a no-op",
			initial:      []kubevirtv1.WorkloadUpdateMethod{},
			desired:      []string{},
			expectMethod: nil,
		},
		{
			name:         "empty to [LiveMigrate] restores",
			initial:      []kubevirtv1.WorkloadUpdateMethod{},
			desired:      []string{liveMigrateWorkloadUpdateMethod},
			expectMethod: ptr.To(kubevirtv1.WorkloadUpdateMethodLiveMigrate),
		},
		{
			name:         "[LiveMigrate] to empty clears",
			initial:      []kubevirtv1.WorkloadUpdateMethod{kubevirtv1.WorkloadUpdateMethodLiveMigrate},
			desired:      []string{},
			expectMethod: nil,
		},
		{
			name:         "[LiveMigrate] to [LiveMigrate] is idempotent",
			initial:      []kubevirtv1.WorkloadUpdateMethod{kubevirtv1.WorkloadUpdateMethodLiveMigrate},
			desired:      []string{liveMigrateWorkloadUpdateMethod},
			expectMethod: ptr.To(kubevirtv1.WorkloadUpdateMethodLiveMigrate),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			kv := newTestKubeVirt(tc.initial)
			c := newFakeClient(kv)

			u := &unstructured.Unstructured{}
			u.SetGroupVersionKind(kubevirtGVK)
			require.NoError(t, c.Get(context.Background(), types.NamespacedName{
				Namespace: harvesterSystemNamespace,
				Name:      KubeVirtObjectName,
			}, u))

			require.NoError(t, setKubeVirtWorkloadUpdateMethods(context.Background(), c, u, tc.desired))

			var after kubevirtv1.KubeVirt
			require.NoError(t, c.Get(context.Background(), types.NamespacedName{
				Namespace: harvesterSystemNamespace,
				Name:      KubeVirtObjectName,
			}, &after))
			actual := after.Spec.WorkloadUpdateStrategy.WorkloadUpdateMethods
			if tc.expectMethod == nil {
				// Empty and nil are equivalent here because kubevirt's JSON
				// tags strip an empty workloadUpdateMethods slice on round-trip.
				assert.Empty(t, actual)
			} else {
				require.Len(t, actual, 1)
				assert.Equal(t, *tc.expectMethod, actual[0])
			}
		})
	}
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
	assert.Equal(t, []string{liveMigrateWorkloadUpdateMethod}, methods)
}
