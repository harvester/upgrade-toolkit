package upgradeplan

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/go-logr/logr"
	fleetv1alpha1 "github.com/rancher/fleet/pkg/apis/fleet.cattle.io/v1alpha1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const kubevirtAPIVersion = "kubevirt.io/v1"

// managedChartGVK is used with the generic client to avoid a direct dependency
// on github.com/rancher/rancher/pkg/apis/management.cattle.io/v3, whose
// transitive deps (aks/eks/gke-operator, norman, kubernetes) conflict with the
// pinned harvester module.
var managedChartGVK = schema.GroupVersionKind{
	Group:   "management.cattle.io",
	Version: "v3",
	Kind:    "ManagedChart",
}

// kubevirtWorkloadUpdateMethodsValuesPath is the path of the KubeVirt
// workloadUpdateMethods override inside the harvester ManagedChart.
var kubevirtWorkloadUpdateMethodsValuesPath = []string{
	"spec", "values", "kubevirt", "spec", "workloadUpdateStrategy", "workloadUpdateMethods",
}

// getHarvesterManagedChart reads the harvester ManagedChart as an unstructured
// object. Returns (nil, nil) when it is absent or the CRD is not installed.
func getHarvesterManagedChart(ctx context.Context, c client.Client) (*unstructured.Unstructured, error) {
	mc := &unstructured.Unstructured{}
	mc.SetGroupVersionKind(managedChartGVK)
	if err := c.Get(ctx, types.NamespacedName{
		Namespace: FleetLocalNamespace,
		Name:      HarvesterManagedChartName,
	}, mc); err != nil {
		if apierrors.IsNotFound(err) || apimeta.IsNoMatchError(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get harvester managedchart: %w", err)
	}
	return mc, nil
}

// disableKubevirtLiveMigrateInManagedChart sets
// spec.values.kubevirt.spec.workloadUpdateStrategy.workloadUpdateMethods to []
// on the harvester ManagedChart, so Fleet itself renders and owns the value on
// the KubeVirt CR. The previous override is recorded in the
// AnnotationKubeVirtWorkloadUpdateMethodsOriginal annotation in the same
// patch, written only once, so a retried call cannot overwrite it with [].
// A kubevirt comparePatch entry left by earlier upgrade-toolkit releases is
// removed as well. Idempotent.
func disableKubevirtLiveMigrateInManagedChart(
	ctx context.Context,
	c client.Client,
	log logr.Logger,
) error {
	mc, err := getHarvesterManagedChart(ctx, c)
	if err != nil {
		return err
	}
	if mc == nil {
		log.V(1).Info("harvester managedchart not found, skipping LiveMigrate disable")
		return nil
	}
	before := mc.DeepCopy()

	annotations := mc.GetAnnotations()
	if _, recorded := annotations[AnnotationKubeVirtWorkloadUpdateMethodsOriginal]; !recorded {
		original := "null"
		current, found, err := unstructured.NestedFieldNoCopy(mc.Object, kubevirtWorkloadUpdateMethodsValuesPath...)
		if err != nil {
			return fmt.Errorf("failed to read kubevirt workloadUpdateMethods from harvester managedchart: %w", err)
		}
		if found {
			buf, err := json.Marshal(current)
			if err != nil {
				return fmt.Errorf("failed to encode kubevirt workloadUpdateMethods: %w", err)
			}
			original = string(buf)
		}
		if annotations == nil {
			annotations = map[string]string{}
		}
		annotations[AnnotationKubeVirtWorkloadUpdateMethodsOriginal] = original
		mc.SetAnnotations(annotations)
	}

	err = unstructured.SetNestedField(mc.Object, []interface{}{}, kubevirtWorkloadUpdateMethodsValuesPath...)
	if err != nil {
		return fmt.Errorf("failed to set kubevirt workloadUpdateMethods on harvester managedchart: %w", err)
	}

	if err := dropKubevirtComparePatch(mc); err != nil {
		return err
	}

	if equality.Semantic.DeepEqual(before.Object, mc.Object) {
		return nil
	}
	if err := c.Patch(ctx, mc, client.MergeFrom(before)); err != nil {
		return fmt.Errorf("failed to update harvester managedchart: %w", err)
	}
	log.Info("set kubevirt workloadUpdateMethods to [] in harvester managedchart values",
		"original", mc.GetAnnotations()[AnnotationKubeVirtWorkloadUpdateMethodsOriginal])
	return nil
}

// restoreKubevirtLiveMigrateInManagedChart restores the
// spec.values.kubevirt.spec.workloadUpdateStrategy.workloadUpdateMethods
// override recorded by disableKubevirtLiveMigrateInManagedChart and removes the
// annotation:
//   - annotation "null": the override was absent, so it is removed again and
//     the chart default ([LiveMigrate]) applies.
//   - any other annotation value: the recorded JSON value is written back.
//   - no annotation: the values are left untouched.
//
// A kubevirt comparePatch entry left by earlier upgrade-toolkit releases is
// removed as well. Idempotent.
func restoreKubevirtLiveMigrateInManagedChart(
	ctx context.Context,
	c client.Client,
	log logr.Logger,
) error {
	mc, err := getHarvesterManagedChart(ctx, c)
	if err != nil {
		return err
	}
	if mc == nil {
		log.V(1).Info("harvester managedchart not found, skipping LiveMigrate restore")
		return nil
	}
	before := mc.DeepCopy()

	annotations := mc.GetAnnotations()
	original, recorded := annotations[AnnotationKubeVirtWorkloadUpdateMethodsOriginal]
	if recorded {
		if original == "null" {
			removeNestedFieldAndEmptyParents(mc.Object, kubevirtWorkloadUpdateMethodsValuesPath...)
		} else {
			var value interface{}
			if err := json.Unmarshal([]byte(original), &value); err != nil {
				return fmt.Errorf("failed to decode annotation %s value %q: %w",
					AnnotationKubeVirtWorkloadUpdateMethodsOriginal, original, err)
			}
			if err := unstructured.SetNestedField(mc.Object, value, kubevirtWorkloadUpdateMethodsValuesPath...); err != nil {
				return fmt.Errorf("failed to restore kubevirt workloadUpdateMethods on harvester managedchart: %w", err)
			}
		}
		delete(annotations, AnnotationKubeVirtWorkloadUpdateMethodsOriginal)
		mc.SetAnnotations(annotations)
	}

	if err := dropKubevirtComparePatch(mc); err != nil {
		return err
	}

	if equality.Semantic.DeepEqual(before.Object, mc.Object) {
		return nil
	}
	if err := c.Patch(ctx, mc, client.MergeFrom(before)); err != nil {
		return fmt.Errorf("failed to update harvester managedchart: %w", err)
	}
	if recorded {
		log.Info("restored kubevirt workloadUpdateMethods in harvester managedchart values", "original", original)
	}
	return nil
}

// dropKubevirtComparePatch removes the kubevirt workloadUpdateMethods
// jsonPointer from spec.diff.comparePatches of the given ManagedChart in place.
// Earlier upgrade-toolkit releases added it; it is not needed when Fleet owns
// the value through the chart values.
func dropKubevirtComparePatch(mc *unstructured.Unstructured) error {
	diff, err := getDiffOptions(mc)
	if err != nil {
		return fmt.Errorf("failed to read spec.diff from harvester managedchart: %w", err)
	}
	if diff == nil || len(diff.ComparePatches) == 0 {
		return nil
	}
	filtered, removed := filterKubevirtComparePatches(diff.ComparePatches, KubeVirtWorkloadUpdateMethodsJSONPointer)
	if !removed {
		return nil
	}
	diff.ComparePatches = filtered
	if err := setDiffOptions(mc, diff); err != nil {
		return fmt.Errorf("failed to set spec.diff on harvester managedchart: %w", err)
	}
	return nil
}

// removeNestedFieldAndEmptyParents removes the field at path and then every
// parent map below spec.values that became empty because of it.
func removeNestedFieldAndEmptyParents(obj map[string]interface{}, path ...string) {
	unstructured.RemoveNestedField(obj, path...)
	// Stop at spec.values (index 2) so the values map itself is kept.
	for i := len(path) - 1; i > 2; i-- {
		parent, found, err := unstructured.NestedMap(obj, path[:i]...)
		if err != nil || !found || len(parent) > 0 {
			return
		}
		unstructured.RemoveNestedField(obj, path[:i]...)
	}
}

// getDiffOptions extracts spec.diff from an unstructured ManagedChart as a
// typed fleet.DiffOptions. Returns (nil, nil) when spec.diff is absent.
func getDiffOptions(mc *unstructured.Unstructured) (*fleetv1alpha1.DiffOptions, error) {
	raw, found, err := unstructured.NestedMap(mc.Object, "spec", "diff")
	if err != nil || !found || raw == nil {
		return nil, err
	}
	buf, err := json.Marshal(raw)
	if err != nil {
		return nil, err
	}
	var diff fleetv1alpha1.DiffOptions
	if err := json.Unmarshal(buf, &diff); err != nil {
		return nil, err
	}
	return &diff, nil
}

// setDiffOptions writes a typed fleet.DiffOptions back into spec.diff of an
// unstructured ManagedChart.
func setDiffOptions(mc *unstructured.Unstructured, diff *fleetv1alpha1.DiffOptions) error {
	buf, err := json.Marshal(diff)
	if err != nil {
		return err
	}
	var raw map[string]interface{}
	if err := json.Unmarshal(buf, &raw); err != nil {
		return err
	}
	return unstructured.SetNestedMap(mc.Object, raw, "spec", "diff")
}

// filterKubevirtComparePatches removes the given jsonPointer from the kubevirt
// comparePatch entry (apiVersion=kubevirt.io/v1, kind=KubeVirt, name=kubevirt).
// If the entry has no remaining jsonPointers afterwards, it is dropped from the
// slice entirely. The second return value is true if any jsonPointer was removed.
func filterKubevirtComparePatches(
	patches []fleetv1alpha1.ComparePatch,
	targetJSONPointer string,
) ([]fleetv1alpha1.ComparePatch, bool) {
	var out []fleetv1alpha1.ComparePatch
	removed := false

	for _, p := range patches {
		if p.APIVersion == kubevirtAPIVersion && p.Kind == "KubeVirt" && p.Name == KubeVirtObjectName {
			var kept []string
			for _, jp := range p.JsonPointers {
				if jp == targetJSONPointer {
					removed = true
				} else {
					kept = append(kept, jp)
				}
			}
			if len(kept) > 0 {
				p.JsonPointers = kept
				out = append(out, p)
			}
			continue
		}
		out = append(out, p)
	}

	return out, removed
}
