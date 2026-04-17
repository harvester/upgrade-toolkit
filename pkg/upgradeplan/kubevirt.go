package upgradeplan

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const liveMigrateWorkloadUpdateMethod = "LiveMigrate"

// kubevirtGVK identifies the KubeVirt CR without binding to the kubevirt.io
// Go types, which share one struct across v1 and v1alpha3 and thus cause the
// controller-runtime scheme to refuse typed Gets when both versions are
// registered (as happens under envtest).
var kubevirtGVK = schema.GroupVersionKind{
	Group:   "kubevirt.io",
	Version: "v1",
	Kind:    "KubeVirt",
}

// getKubeVirt reads the singleton kubevirt CR as an unstructured object.
// Returns (nil, nil) when the CR is absent or the CRD is not installed.
func getKubeVirt(ctx context.Context, c client.Client) (*unstructured.Unstructured, error) {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(kubevirtGVK)
	if err := c.Get(ctx, types.NamespacedName{
		Namespace: harvesterSystemNamespace,
		Name:      KubeVirtObjectName,
	}, u); err != nil {
		if apierrors.IsNotFound(err) || apimeta.IsNoMatchError(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get kubevirt object: %w", err)
	}
	return u, nil
}

// setKubeVirtWorkloadUpdateMethods patches spec.workloadUpdateStrategy.workloadUpdateMethods
// to the given slice when different from the current value. Idempotent.
func setKubeVirtWorkloadUpdateMethods(
	ctx context.Context,
	c client.Client,
	kv *unstructured.Unstructured,
	desired []string,
) error {
	current, _, _ := unstructured.NestedStringSlice(kv.Object, "spec", "workloadUpdateStrategy", "workloadUpdateMethods")
	if stringSlicesEqual(current, desired) {
		return nil
	}

	before := kv.DeepCopy()
	if err := unstructured.SetNestedStringSlice(
		kv.Object, desired,
		"spec", "workloadUpdateStrategy", "workloadUpdateMethods",
	); err != nil {
		return err
	}
	return c.Patch(ctx, kv, client.MergeFrom(before))
}

func stringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
