package upgradeplan

import (
	"context"
	"testing"

	"github.com/go-logr/logr"
	fleetv1alpha1 "github.com/rancher/fleet/pkg/apis/fleet.cattle.io/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// TestFilterKubevirtComparePatches mirrors the upstream test matrix from
// harvester/harvester#10386 (pkg/controller/master/upgrade/filter_compare_patches_test.go).
func TestFilterKubevirtComparePatches(t *testing.T) {
	const targetJSONPointer = "/spec/workloadUpdateStrategy/workloadUpdateMethods"

	tests := []struct {
		name               string
		inputPatches       []fleetv1alpha1.ComparePatch
		expectedRemoved    bool
		expectedPatchCount int
		validateResult     func(t *testing.T, result []fleetv1alpha1.ComparePatch)
	}{
		{
			name:               "empty comparePatches",
			inputPatches:       []fleetv1alpha1.ComparePatch{},
			expectedRemoved:    false,
			expectedPatchCount: 0,
			validateResult: func(t *testing.T, result []fleetv1alpha1.ComparePatch) {
				assert.Empty(t, result)
			},
		},
		{
			name: "kubevirt patch with only target jsonPointer - should be removed entirely",
			inputPatches: []fleetv1alpha1.ComparePatch{
				{
					APIVersion: "kubevirt.io/v1",
					Kind:       "KubeVirt",
					Name:       "kubevirt",
					JsonPointers: []string{
						targetJSONPointer,
					},
				},
			},
			expectedRemoved:    true,
			expectedPatchCount: 0,
			validateResult: func(t *testing.T, result []fleetv1alpha1.ComparePatch) {
				assert.Empty(t, result, "patch should be completely removed when it only contains target jsonPointer")
			},
		},
		{
			name: "kubevirt patch with target and other jsonPointers - should keep patch with other pointers",
			inputPatches: []fleetv1alpha1.ComparePatch{
				{
					APIVersion: "kubevirt.io/v1",
					Kind:       "KubeVirt",
					Name:       "kubevirt",
					JsonPointers: []string{
						targetJSONPointer,
						"/spec/someOtherField",
						"/spec/configuration/developerConfiguration",
					},
				},
			},
			expectedRemoved:    true,
			expectedPatchCount: 1,
			validateResult: func(t *testing.T, result []fleetv1alpha1.ComparePatch) {
				require.Len(t, result, 1, "should keep one patch")
				patch := result[0]
				assert.Equal(t, "kubevirt.io/v1", patch.APIVersion)
				assert.Equal(t, "KubeVirt", patch.Kind)
				assert.Equal(t, "kubevirt", patch.Name)
				assert.Len(t, patch.JsonPointers, 2, "should have 2 remaining jsonPointers")
				assert.Equal(t, []string{"/spec/someOtherField", "/spec/configuration/developerConfiguration"}, patch.JsonPointers)
				assert.NotContains(t, patch.JsonPointers, targetJSONPointer, "target jsonPointer should be removed")
			},
		},
		{
			name: "kubevirt patch without target jsonPointer - should remain unchanged",
			inputPatches: []fleetv1alpha1.ComparePatch{
				{
					APIVersion: "kubevirt.io/v1",
					Kind:       "KubeVirt",
					Name:       "kubevirt",
					JsonPointers: []string{
						"/spec/someOtherField",
						"/spec/anotherField",
					},
				},
			},
			expectedRemoved:    false,
			expectedPatchCount: 1,
			validateResult: func(t *testing.T, result []fleetv1alpha1.ComparePatch) {
				require.Len(t, result, 1)
				assert.Equal(t, []string{"/spec/someOtherField", "/spec/anotherField"}, result[0].JsonPointers)
			},
		},
		{
			name: "non-kubevirt patches should remain unchanged",
			inputPatches: []fleetv1alpha1.ComparePatch{
				{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "test-deployment",
					JsonPointers: []string{
						"/spec/replicas",
					},
				},
				{
					APIVersion: "v1",
					Kind:       "Service",
					Name:       "test-service",
					JsonPointers: []string{
						"/spec/ports",
					},
				},
			},
			expectedRemoved:    false,
			expectedPatchCount: 2,
			validateResult: func(t *testing.T, result []fleetv1alpha1.ComparePatch) {
				require.Len(t, result, 2)
				assert.Equal(t, "apps/v1", result[0].APIVersion)
				assert.Equal(t, "v1", result[1].APIVersion)
			},
		},
		{
			name: "mixed patches - kubevirt with target and other patches",
			inputPatches: []fleetv1alpha1.ComparePatch{
				{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "test-deployment",
					JsonPointers: []string{
						"/spec/replicas",
					},
				},
				{
					APIVersion: "kubevirt.io/v1",
					Kind:       "KubeVirt",
					Name:       "kubevirt",
					JsonPointers: []string{
						targetJSONPointer,
					},
				},
				{
					APIVersion: "v1",
					Kind:       "Service",
					Name:       "test-service",
					JsonPointers: []string{
						"/spec/ports",
					},
				},
			},
			expectedRemoved:    true,
			expectedPatchCount: 2,
			validateResult: func(t *testing.T, result []fleetv1alpha1.ComparePatch) {
				require.Len(t, result, 2, "should have 2 patches after removing kubevirt patch")
				assert.Equal(t, "apps/v1", result[0].APIVersion)
				assert.Equal(t, "v1", result[1].APIVersion)
				for _, patch := range result {
					assert.NotEqual(t, "kubevirt.io/v1", patch.APIVersion)
				}
			},
		},
		{
			name: "kubevirt patch with wrong name - should remain unchanged",
			inputPatches: []fleetv1alpha1.ComparePatch{
				{
					APIVersion: "kubevirt.io/v1",
					Kind:       "KubeVirt",
					Name:       "different-kubevirt",
					JsonPointers: []string{
						targetJSONPointer,
					},
				},
			},
			expectedRemoved:    false,
			expectedPatchCount: 1,
			validateResult: func(t *testing.T, result []fleetv1alpha1.ComparePatch) {
				require.Len(t, result, 1)
				assert.Equal(t, "different-kubevirt", result[0].Name)
				assert.Contains(t, result[0].JsonPointers, targetJSONPointer, "should not filter patches with different name")
			},
		},
		{
			name: "kubevirt patch with wrong kind - should remain unchanged",
			inputPatches: []fleetv1alpha1.ComparePatch{
				{
					APIVersion: "kubevirt.io/v1",
					Kind:       "VirtualMachine",
					Name:       "kubevirt",
					JsonPointers: []string{
						targetJSONPointer,
					},
				},
			},
			expectedRemoved:    false,
			expectedPatchCount: 1,
			validateResult: func(t *testing.T, result []fleetv1alpha1.ComparePatch) {
				require.Len(t, result, 1)
				assert.Equal(t, "VirtualMachine", result[0].Kind)
				assert.Contains(t, result[0].JsonPointers, targetJSONPointer, "should not filter patches with different kind")
			},
		},
		{
			name: "multiple kubevirt patches - only matching one should be modified",
			inputPatches: []fleetv1alpha1.ComparePatch{
				{
					APIVersion: "kubevirt.io/v1",
					Kind:       "KubeVirt",
					Name:       "kubevirt",
					Namespace:  harvesterSystemNamespace,
					JsonPointers: []string{
						targetJSONPointer,
						"/spec/otherField",
					},
				},
				{
					APIVersion: "kubevirt.io/v1",
					Kind:       "KubeVirt",
					Name:       "other-kubevirt",
					JsonPointers: []string{
						"/spec/someField",
					},
				},
			},
			expectedRemoved:    true,
			expectedPatchCount: 2,
			validateResult: func(t *testing.T, result []fleetv1alpha1.ComparePatch) {
				require.Len(t, result, 2)
				var kubevirtPatch *fleetv1alpha1.ComparePatch
				var otherPatch *fleetv1alpha1.ComparePatch
				for i := range result {
					switch result[i].Name {
					case "kubevirt":
						kubevirtPatch = &result[i]
					case "other-kubevirt":
						otherPatch = &result[i]
					}
				}
				require.NotNil(t, kubevirtPatch, "kubevirt patch should still exist")
				require.NotNil(t, otherPatch, "other-kubevirt patch should exist")

				assert.Equal(t, []string{"/spec/otherField"}, kubevirtPatch.JsonPointers,
					"target jsonPointer should be removed from matching patch")
				assert.Equal(t, []string{"/spec/someField"}, otherPatch.JsonPointers,
					"non-matching patch should be unchanged")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actualPatches, actualRemoved := filterKubevirtComparePatches(tt.inputPatches, targetJSONPointer)

			assert.Equal(t, tt.expectedRemoved, actualRemoved, "removed flag should match expected")
			assert.Equal(t, tt.expectedPatchCount, len(actualPatches), "patch count should match expected")

			if tt.validateResult != nil {
				tt.validateResult(t, actualPatches)
			}
		})
	}
}

// newTestManagedChartWithValues returns a harvester ManagedChart with the given
// spec.values and annotations.
func newTestManagedChartWithValues(
	values map[string]interface{},
	annotations map[string]string,
) *unstructured.Unstructured {
	mc := newTestManagedChart(nil)
	if values != nil {
		_ = unstructured.SetNestedMap(mc.Object, values, "spec", "values")
	}
	if annotations != nil {
		mc.SetAnnotations(annotations)
	}
	return mc
}

func getTestHarvesterManagedChart(t *testing.T, c client.Client) *unstructured.Unstructured {
	t.Helper()
	mc, err := getHarvesterManagedChart(context.Background(), c)
	require.NoError(t, err)
	require.NotNil(t, mc)
	return mc
}

func workloadUpdateMethodsValue(t *testing.T, mc *unstructured.Unstructured) (interface{}, bool) {
	t.Helper()
	v, found, err := unstructured.NestedFieldCopy(mc.Object, kubevirtWorkloadUpdateMethodsValuesPath...)
	require.NoError(t, err)
	return v, found
}

func kubevirtValues(methods ...interface{}) map[string]interface{} {
	if methods == nil {
		methods = []interface{}{}
	}
	return map[string]interface{}{
		"kubevirt": map[string]interface{}{
			"spec": map[string]interface{}{
				"workloadUpdateStrategy": map[string]interface{}{
					"workloadUpdateMethods": methods,
				},
			},
		},
	}
}

func TestDisableKubevirtLiveMigrateInManagedChart(t *testing.T) {
	cases := []struct {
		name           string
		values         map[string]interface{}
		annotations    map[string]string
		expectOriginal string
	}{
		{
			name:           "no values override records null",
			values:         nil,
			expectOriginal: "null",
		},
		{
			name:           "unrelated values are kept and null is recorded",
			values:         map[string]interface{}{"storageClass": map[string]interface{}{"defaultStorageClass": false}},
			expectOriginal: "null",
		},
		{
			name:           "explicit [LiveMigrate] override is recorded",
			values:         kubevirtValues("LiveMigrate"),
			expectOriginal: `["LiveMigrate"]`,
		},
		{
			name:           "explicit [] override is recorded",
			values:         kubevirtValues(),
			expectOriginal: `[]`,
		},
		{
			name:           "existing record is not overwritten",
			values:         kubevirtValues(),
			annotations:    map[string]string{AnnotationKubeVirtWorkloadUpdateMethodsOriginal: "null"},
			expectOriginal: "null",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := newFakeClient(newTestManagedChartWithValues(tc.values, tc.annotations))

			require.NoError(t, disableKubevirtLiveMigrateInManagedChart(context.Background(), c, logr.Discard()))

			mc := getTestHarvesterManagedChart(t, c)
			v, found := workloadUpdateMethodsValue(t, mc)
			require.True(t, found, "workloadUpdateMethods override must be set")
			assert.Equal(t, []interface{}{}, v)
			assert.Equal(t, tc.expectOriginal, mc.GetAnnotations()[AnnotationKubeVirtWorkloadUpdateMethodsOriginal])

			if _, ok := tc.values["storageClass"]; ok {
				dsc, found, err := unstructured.NestedBool(mc.Object, "spec", "values", "storageClass", "defaultStorageClass")
				require.NoError(t, err)
				require.True(t, found, "unrelated values must be preserved")
				assert.False(t, dsc)
			}

			// A second call (for example, a retried PreRun) must keep the
			// original record instead of recording [].
			require.NoError(t, disableKubevirtLiveMigrateInManagedChart(context.Background(), c, logr.Discard()))
			mc = getTestHarvesterManagedChart(t, c)
			assert.Equal(t, tc.expectOriginal, mc.GetAnnotations()[AnnotationKubeVirtWorkloadUpdateMethodsOriginal])
		})
	}
}

func TestDisableKubevirtLiveMigrateInManagedChart_DropsStaleComparePatch(t *testing.T) {
	mc := newTestManagedChart([]map[string]interface{}{
		{
			"apiVersion":   "kubevirt.io/v1",
			"kind":         "KubeVirt",
			"name":         "kubevirt",
			"jsonPointers": []interface{}{KubeVirtWorkloadUpdateMethodsJSONPointer},
		},
		{
			"apiVersion":   "v1",
			"kind":         "ConfigMap",
			"name":         "other",
			"jsonPointers": []interface{}{"/data"},
		},
	})
	c := newFakeClient(mc)

	require.NoError(t, disableKubevirtLiveMigrateInManagedChart(context.Background(), c, logr.Discard()))

	got := getTestHarvesterManagedChart(t, c)
	patches, found, err := unstructured.NestedSlice(got.Object, "spec", "diff", "comparePatches")
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, patches, 1, "only the kubevirt entry must be removed")
	assert.Equal(t, "ConfigMap", patches[0].(map[string]interface{})["kind"])
}

func TestDisableKubevirtLiveMigrateInManagedChart_MissingChart(t *testing.T) {
	c := newFakeClient()
	require.NoError(t, disableKubevirtLiveMigrateInManagedChart(context.Background(), c, logr.Discard()))
}

func TestRestoreKubevirtLiveMigrateInManagedChart(t *testing.T) {
	cases := []struct {
		name        string
		values      map[string]interface{}
		annotations map[string]string
		expectFound bool
		expectValue interface{}
		expectEmpty bool // spec.values must be left without a kubevirt key
	}{
		{
			name:        "null record removes the override and empty parents",
			values:      kubevirtValues(),
			annotations: map[string]string{AnnotationKubeVirtWorkloadUpdateMethodsOriginal: "null"},
			expectFound: false,
			expectEmpty: true,
		},
		{
			name:        "recorded [LiveMigrate] is written back",
			values:      kubevirtValues(),
			annotations: map[string]string{AnnotationKubeVirtWorkloadUpdateMethodsOriginal: `["LiveMigrate"]`},
			expectFound: true,
			expectValue: []interface{}{"LiveMigrate"},
		},
		{
			name:        "recorded [] is written back",
			values:      kubevirtValues(),
			annotations: map[string]string{AnnotationKubeVirtWorkloadUpdateMethodsOriginal: `[]`},
			expectFound: true,
			expectValue: []interface{}{},
		},
		{
			name:        "no record leaves the values untouched",
			values:      kubevirtValues(),
			expectFound: true,
			expectValue: []interface{}{},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := newFakeClient(newTestManagedChartWithValues(tc.values, tc.annotations))

			require.NoError(t, restoreKubevirtLiveMigrateInManagedChart(context.Background(), c, logr.Discard()))

			mc := getTestHarvesterManagedChart(t, c)
			v, found := workloadUpdateMethodsValue(t, mc)
			assert.Equal(t, tc.expectFound, found)
			if tc.expectFound {
				assert.Equal(t, tc.expectValue, v)
			}
			if tc.expectEmpty {
				_, found, err := unstructured.NestedFieldNoCopy(mc.Object, "spec", "values", "kubevirt")
				require.NoError(t, err)
				assert.False(t, found, "empty parent maps must be pruned")
			}
			assert.NotContains(t, mc.GetAnnotations(), AnnotationKubeVirtWorkloadUpdateMethodsOriginal)

			// Idempotent.
			require.NoError(t, restoreKubevirtLiveMigrateInManagedChart(context.Background(), c, logr.Discard()))
		})
	}
}

func TestRestoreKubevirtLiveMigrateInManagedChart_KeepsSiblingValues(t *testing.T) {
	values := kubevirtValues()
	kvSpec := values["kubevirt"].(map[string]interface{})["spec"].(map[string]interface{})
	kvSpec["configuration"] = map[string]interface{}{
		"developerConfiguration": map[string]interface{}{"featureGates": []interface{}{"Foo"}},
	}
	c := newFakeClient(newTestManagedChartWithValues(values,
		map[string]string{AnnotationKubeVirtWorkloadUpdateMethodsOriginal: "null"}))

	require.NoError(t, restoreKubevirtLiveMigrateInManagedChart(context.Background(), c, logr.Discard()))

	mc := getTestHarvesterManagedChart(t, c)
	_, found := workloadUpdateMethodsValue(t, mc)
	assert.False(t, found)
	gates, found, err := unstructured.NestedStringSlice(mc.Object,
		"spec", "values", "kubevirt", "spec", "configuration", "developerConfiguration", "featureGates")
	require.NoError(t, err)
	require.True(t, found, "sibling kubevirt values must be preserved")
	assert.Equal(t, []string{"Foo"}, gates)
}

func TestRestoreKubevirtLiveMigrateInManagedChart_InvalidRecord(t *testing.T) {
	c := newFakeClient(newTestManagedChartWithValues(kubevirtValues(),
		map[string]string{AnnotationKubeVirtWorkloadUpdateMethodsOriginal: "not-json"}))
	require.Error(t, restoreKubevirtLiveMigrateInManagedChart(context.Background(), c, logr.Discard()))
}
