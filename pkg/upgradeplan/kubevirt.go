package upgradeplan

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// upgradeToolkitFieldManager is the field manager name the API server records
// for requests made by the upgrade-toolkit binary.
const upgradeToolkitFieldManager = "upgrade-toolkit"

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

// removeKubeVirtFieldManagerEntries drops the managedFields entries of the
// upgrade-toolkit field manager from the KubeVirt CR.
//
// Earlier upgrade-toolkit releases patched
// spec.workloadUpdateStrategy.workloadUpdateMethods on the KubeVirt CR
// directly, which recorded upgrade-toolkit as an owner of that field. Fleet
// applies the harvester chart with server-side apply and without forcing
// conflicts, so it fails as soon as the chart renders a different value for a
// field owned by another manager. Removing the stale entry hands the field
// back to Fleet; the field value itself is not changed. Idempotent.
func removeKubeVirtFieldManagerEntries(
	ctx context.Context,
	c client.Client,
	log logr.Logger,
) error {
	kv, err := getKubeVirt(ctx, c)
	if err != nil {
		return err
	}
	if kv == nil {
		log.V(1).Info("kubevirt object not found, skipping managedFields cleanup")
		return nil
	}

	patch := upgradeToolkitManagedFieldsRemovalPatch(kv.GetManagedFields())
	if patch == nil {
		return nil
	}
	if err := c.Patch(ctx, kv, client.RawPatch(types.JSONPatchType, patch)); err != nil {
		return fmt.Errorf("failed to remove %s managedFields entries from kubevirt object: %w",
			upgradeToolkitFieldManager, err)
	}
	log.Info("removed stale managedFields entries from kubevirt object", "manager", upgradeToolkitFieldManager)
	return nil
}

// upgradeToolkitManagedFieldsRemovalPatch builds a JSON patch that removes
// every managedFields entry owned by upgradeToolkitFieldManager. Each removal
// is guarded by a test operation so the patch fails instead of removing the
// wrong entry when the list changed in the meantime. Returns nil when there is nothing to remove or when
// upgradeToolkitFieldManager owns every entry.
func upgradeToolkitManagedFieldsRemovalPatch(entries []metav1.ManagedFieldsEntry) []byte {
	const manager = upgradeToolkitFieldManager
	var ops []map[string]interface{}
	// Remove from the highest index down so earlier indices stay valid.
	for i := len(entries) - 1; i >= 0; i-- {
		if entries[i].Manager != manager {
			continue
		}
		path := fmt.Sprintf("/metadata/managedFields/%d", i)
		ops = append(ops,
			map[string]interface{}{"op": "test", "path": path + "/manager", "value": manager},
			map[string]interface{}{"op": "remove", "path": path},
		)
	}
	// Removing every entry would make the API server treat the object as
	// never applied ("before-first-apply" owns all fields), which conflicts
	// just the same; leave such an object alone.
	if len(ops) == 0 || len(ops)/2 == len(entries) {
		return nil
	}
	buf, _ := json.Marshal(ops)
	return buf
}
