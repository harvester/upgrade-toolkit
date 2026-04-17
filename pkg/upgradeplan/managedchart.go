package upgradeplan

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/go-logr/logr"
	fleetv1alpha1 "github.com/rancher/fleet/pkg/apis/fleet.cattle.io/v1alpha1"
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

// addKubevirtComparePatches adds the kubevirt workloadUpdateMethods entry to
// spec.diff.comparePatches of the harvester ManagedChart if missing.
func addKubevirtComparePatches(
	ctx context.Context,
	c client.Client,
	log logr.Logger,
) error {
	mc := &unstructured.Unstructured{}
	mc.SetGroupVersionKind(managedChartGVK)
	if err := c.Get(ctx, types.NamespacedName{
		Namespace: FleetLocalNamespace,
		Name:      HarvesterManagedChartName,
	}, mc); err != nil {
		if apierrors.IsNotFound(err) || apimeta.IsNoMatchError(err) {
			log.V(1).Info("harvester managedchart not found, skipping comparePatches add")
			return nil
		}
		return fmt.Errorf("failed to get harvester managedchart: %w", err)
	}

	diff, err := getDiffOptions(mc)
	if err != nil {
		return fmt.Errorf("failed to read spec.diff from harvester managedchart: %w", err)
	}

	updated, changed := addKubevirtComparePatchEntry(diff)
	if !changed {
		return nil
	}

	before := mc.DeepCopy()
	if err := setDiffOptions(mc, updated); err != nil {
		return fmt.Errorf("failed to set spec.diff on harvester managedchart: %w", err)
	}
	if err := c.Patch(ctx, mc, client.MergeFrom(before)); err != nil {
		return fmt.Errorf("failed to update harvester managedchart: %w", err)
	}
	log.Info("added kubevirt comparePatches entry to harvester managedchart")
	return nil
}

// removeKubevirtComparePatches removes the kubevirt workloadUpdateMethods
// jsonPointer from spec.diff.comparePatches of the harvester ManagedChart.
func removeKubevirtComparePatches(
	ctx context.Context,
	c client.Client,
	log logr.Logger,
) error {
	mc := &unstructured.Unstructured{}
	mc.SetGroupVersionKind(managedChartGVK)
	if err := c.Get(ctx, types.NamespacedName{
		Namespace: FleetLocalNamespace,
		Name:      HarvesterManagedChartName,
	}, mc); err != nil {
		if apierrors.IsNotFound(err) || apimeta.IsNoMatchError(err) {
			log.V(1).Info("harvester managedchart not found, skipping comparePatches removal")
			return nil
		}
		return fmt.Errorf("failed to get harvester managedchart: %w", err)
	}

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

	before := mc.DeepCopy()
	if err := setDiffOptions(mc, diff); err != nil {
		return fmt.Errorf("failed to set spec.diff on harvester managedchart: %w", err)
	}
	if err := c.Patch(ctx, mc, client.MergeFrom(before)); err != nil {
		return fmt.Errorf("failed to update harvester managedchart: %w", err)
	}
	log.Info("removed kubevirt comparePatches entry from harvester managedchart")
	return nil
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

// addKubevirtComparePatchEntry ensures the kubevirt comparePatch entry
// references KubeVirtWorkloadUpdateMethodsJSONPointer. Returns a non-nil
// DiffOptions (deep-copied from input when non-nil) and a boolean indicating
// whether the entry was changed. Idempotent: returns changed=false when the
// jsonPointer is already present.
func addKubevirtComparePatchEntry(diff *fleetv1alpha1.DiffOptions) (*fleetv1alpha1.DiffOptions, bool) {
	if diff == nil {
		diff = &fleetv1alpha1.DiffOptions{}
	} else {
		diff = diff.DeepCopy()
	}

	for i, p := range diff.ComparePatches {
		if p.APIVersion == kubevirtAPIVersion && p.Kind == "KubeVirt" && p.Name == KubeVirtObjectName {
			for _, jp := range p.JsonPointers {
				if jp == KubeVirtWorkloadUpdateMethodsJSONPointer {
					return diff, false
				}
			}
			diff.ComparePatches[i].JsonPointers = append(
				diff.ComparePatches[i].JsonPointers,
				KubeVirtWorkloadUpdateMethodsJSONPointer,
			)
			return diff, true
		}
	}

	diff.ComparePatches = append(diff.ComparePatches, fleetv1alpha1.ComparePatch{
		APIVersion:   kubevirtAPIVersion,
		Kind:         "KubeVirt",
		Name:         KubeVirtObjectName,
		JsonPointers: []string{KubeVirtWorkloadUpdateMethodsJSONPointer},
	})
	return diff, true
}
