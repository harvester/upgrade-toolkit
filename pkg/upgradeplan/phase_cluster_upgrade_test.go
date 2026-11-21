package upgradeplan

import (
	"context"
	"fmt"
	"testing"

	"github.com/rancher/wrangler/v3/pkg/name"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	kubevirtv1 "kubevirt.io/api/core/v1"

	buildversion "github.com/harvester/upgrade-toolkit/pkg/version"
)

func TestConstructJobForClusterUpgrade(t *testing.T) {
	up := newTestUpgradePlan()
	serviceAccount := "harvester"

	job := constructJobForClusterUpgrade(up, serviceAccount)

	// Verify job metadata
	assert.Equal(t, name.SafeConcatName(testUpgradePlanName, ClusterComponent), job.Name)
	assert.Equal(t, harvesterSystemNamespace, job.Namespace)
	assert.Equal(t, testUpgradePlanName, job.Labels[HarvesterUpgradePlanLabel])
	assert.Equal(t, ClusterComponent, job.Labels[HarvesterUpgradeComponentLabel])

	// Verify pod template labels
	podLabels := job.Spec.Template.Labels
	assert.Equal(t, testUpgradePlanName, podLabels[HarvesterUpgradePlanLabel])
	assert.Equal(t, ClusterComponent, podLabels[HarvesterUpgradeComponentLabel])

	// Verify container
	require.Len(t, job.Spec.Template.Spec.Containers, 1)
	container := job.Spec.Template.Spec.Containers[0]
	assert.Equal(t, "apply", container.Name)
	assert.Equal(t, fmt.Sprintf("%s:%s", upgradeToolkitImage, buildversion.Version), container.Image)
	assert.Equal(t, []string{"upgrade_manifests.sh"}, container.Command)

	// Verify env vars
	require.Len(t, container.Env, 1)
	assert.Equal(t, "HARVESTER_UPGRADEPLAN_NAME", container.Env[0].Name)
	assert.Equal(t, testUpgradePlanName, container.Env[0].Value)

	// Verify SecurityContext runs as root
	require.NotNil(t, container.SecurityContext)
	require.NotNil(t, container.SecurityContext.RunAsUser)
	assert.Equal(t, int64(0), *container.SecurityContext.RunAsUser)

	// Verify TTL
	require.NotNil(t, job.Spec.TTLSecondsAfterFinished)
	assert.Equal(t, int32(defaultTTLSecondsAfterFinished), *job.Spec.TTLSecondsAfterFinished)

	// Verify pod spec
	podSpec := job.Spec.Template.Spec
	assert.Equal(t, serviceAccount, podSpec.ServiceAccountName)
	assert.NotEmpty(t, podSpec.Tolerations)
}

func TestConstructJobForClusterUpgrade_WithImageOverride(t *testing.T) {
	up := newTestUpgradePlan()
	up.Annotations = map[string]string{
		AnnotationUpgradeToolkitImage: "custom/upgrade-toolkit",
	}
	serviceAccount := "harvester"

	job := constructJobForClusterUpgrade(up, serviceAccount)

	container := job.Spec.Template.Spec.Containers[0]
	assert.Equal(t, fmt.Sprintf("custom/upgrade-toolkit:%s", buildversion.Version), container.Image)

	// SecurityContext must still be set
	require.NotNil(t, container.SecurityContext)
	assert.Equal(t, ptr.To(int64(0)), container.SecurityContext.RunAsUser)
}

func TestClusterUpgradePhase_PreRunDisablesKubeVirtLiveMigrate(t *testing.T) {
	up := newTestUpgradePlan()

	kv := &kubevirtv1.KubeVirt{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: harvesterSystemNamespace,
			Name:      KubeVirtObjectName,
			// A stale entry left by an earlier upgrade-toolkit release that
			// patched the KubeVirt CR directly.
			ManagedFields: []metav1.ManagedFieldsEntry{
				{Manager: "fleet-agent", Operation: metav1.ManagedFieldsOperationApply, APIVersion: "kubevirt.io/v1"},
				{Manager: upgradeToolkitFieldManager, Operation: metav1.ManagedFieldsOperationUpdate, APIVersion: "kubevirt.io/v1"},
			},
		},
		Spec: kubevirtv1.KubeVirtSpec{
			WorkloadUpdateStrategy: kubevirtv1.KubeVirtWorkloadUpdateStrategy{
				WorkloadUpdateMethods: []kubevirtv1.WorkloadUpdateMethod{
					kubevirtv1.WorkloadUpdateMethodLiveMigrate,
				},
			},
		},
	}
	mc := newTestManagedChart(nil)

	c := newFakeClient(kv, mc)
	phase := &ClusterUpgradePhase{PhaseDeps: &PhaseDeps{Client: c}}

	require.NoError(t, phase.PreRun(context.Background(), up))

	// The KubeVirt CR spec is left to Fleet; only the stale ownership is gone.
	var gotKV kubevirtv1.KubeVirt
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{
		Namespace: harvesterSystemNamespace,
		Name:      KubeVirtObjectName,
	}, &gotKV))
	assert.Equal(t,
		[]kubevirtv1.WorkloadUpdateMethod{kubevirtv1.WorkloadUpdateMethodLiveMigrate},
		gotKV.Spec.WorkloadUpdateStrategy.WorkloadUpdateMethods,
		"PreRun must not patch the KubeVirt CR spec")
	for _, e := range gotKV.ManagedFields {
		assert.NotEqual(t, upgradeToolkitFieldManager, e.Manager, "stale upgrade-toolkit managedFields entry must be removed")
	}

	gotMC := &unstructured.Unstructured{}
	gotMC.SetGroupVersionKind(managedChartGVK)
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{
		Namespace: FleetLocalNamespace,
		Name:      HarvesterManagedChartName,
	}, gotMC))
	methods, found, err := unstructured.NestedSlice(gotMC.Object, kubevirtWorkloadUpdateMethodsValuesPath...)
	require.NoError(t, err)
	require.True(t, found, "workloadUpdateMethods values override should have been set")
	assert.Empty(t, methods)
	assert.Equal(t, "null", gotMC.GetAnnotations()[AnnotationKubeVirtWorkloadUpdateMethodsOriginal])
	_, found, err = unstructured.NestedSlice(gotMC.Object, "spec", "diff", "comparePatches")
	require.NoError(t, err)
	assert.False(t, found, "no comparePatches entry is needed when Fleet owns the value")

	// Second invocation must be a no-op (idempotent).
	rv := gotMC.GetResourceVersion()
	require.NoError(t, phase.PreRun(context.Background(), up))
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{
		Namespace: FleetLocalNamespace,
		Name:      HarvesterManagedChartName,
	}, gotMC))
	assert.Equal(t, rv, gotMC.GetResourceVersion(), "PreRun must be idempotent")
	assert.Equal(t, "null", gotMC.GetAnnotations()[AnnotationKubeVirtWorkloadUpdateMethodsOriginal])
}
