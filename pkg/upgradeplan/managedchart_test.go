package upgradeplan

import (
	"testing"

	fleetv1alpha1 "github.com/rancher/fleet/pkg/apis/fleet.cattle.io/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestAddKubevirtComparePatchEntry(t *testing.T) {
	const targetJSONPointer = KubeVirtWorkloadUpdateMethodsJSONPointer

	t.Run("nil diff creates new kubevirt entry", func(t *testing.T) {
		out, changed := addKubevirtComparePatchEntry(nil)
		require.True(t, changed)
		require.NotNil(t, out)
		require.Len(t, out.ComparePatches, 1)
		assert.Equal(t, "kubevirt.io/v1", out.ComparePatches[0].APIVersion)
		assert.Equal(t, "KubeVirt", out.ComparePatches[0].Kind)
		assert.Equal(t, "kubevirt", out.ComparePatches[0].Name)
		assert.Equal(t, []string{targetJSONPointer}, out.ComparePatches[0].JsonPointers)
	})

	t.Run("empty ComparePatches appends new kubevirt entry", func(t *testing.T) {
		in := &fleetv1alpha1.DiffOptions{ComparePatches: []fleetv1alpha1.ComparePatch{}}
		out, changed := addKubevirtComparePatchEntry(in)
		require.True(t, changed)
		require.Len(t, out.ComparePatches, 1)
		assert.Equal(t, []string{targetJSONPointer}, out.ComparePatches[0].JsonPointers)
	})

	t.Run("existing kubevirt entry without target pointer appends pointer", func(t *testing.T) {
		in := &fleetv1alpha1.DiffOptions{
			ComparePatches: []fleetv1alpha1.ComparePatch{
				{
					APIVersion:   "kubevirt.io/v1",
					Kind:         "KubeVirt",
					Name:         "kubevirt",
					JsonPointers: []string{"/spec/other"},
				},
			},
		}
		out, changed := addKubevirtComparePatchEntry(in)
		require.True(t, changed)
		require.Len(t, out.ComparePatches, 1)
		assert.Equal(t, []string{"/spec/other", targetJSONPointer}, out.ComparePatches[0].JsonPointers)
	})

	t.Run("existing kubevirt entry with target pointer is idempotent", func(t *testing.T) {
		in := &fleetv1alpha1.DiffOptions{
			ComparePatches: []fleetv1alpha1.ComparePatch{
				{
					APIVersion:   "kubevirt.io/v1",
					Kind:         "KubeVirt",
					Name:         "kubevirt",
					JsonPointers: []string{targetJSONPointer},
				},
			},
		}
		out, changed := addKubevirtComparePatchEntry(in)
		require.False(t, changed)
		require.Len(t, out.ComparePatches, 1)
		assert.Equal(t, []string{targetJSONPointer}, out.ComparePatches[0].JsonPointers)
	})

	t.Run("input slice is not mutated", func(t *testing.T) {
		in := &fleetv1alpha1.DiffOptions{
			ComparePatches: []fleetv1alpha1.ComparePatch{
				{
					APIVersion:   "kubevirt.io/v1",
					Kind:         "KubeVirt",
					Name:         "kubevirt",
					JsonPointers: []string{"/spec/other"},
				},
			},
		}
		_, _ = addKubevirtComparePatchEntry(in)
		assert.Equal(t, []string{"/spec/other"}, in.ComparePatches[0].JsonPointers,
			"input slice must not be mutated; addKubevirtComparePatchEntry should operate on a deep copy")
	})
}
