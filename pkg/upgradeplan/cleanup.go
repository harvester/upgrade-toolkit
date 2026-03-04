package upgradeplan

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	harvesterv1beta1 "github.com/harvester/harvester/pkg/apis/harvesterhci.io/v1beta1"
	provisioningv1 "github.com/rancher/rancher/pkg/apis/provisioning.cattle.io/v1"
	upgradev1 "github.com/rancher/system-upgrade-controller/pkg/apis/upgrade.cattle.io/v1"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	managementv1beta1 "github.com/harvester/upgrade-toolkit/api/v1beta1"
)

// CleanupUpgradeResources deletes all upgrade-related resources by label,
// cleans up node annotations, and reverts the Cluster's upgradeStrategy.
// It is idempotent, meaning operating on non-existent resources is a no-op.
func CleanupUpgradeResources(
	ctx context.Context,
	c client.Client,
	log logr.Logger,
	upgradePlan *managementv1beta1.UpgradePlan,
) error {
	log.V(1).Info("handle resource cleanup")

	resourcesToDelete := []struct {
		obj       client.Object
		namespace string
		component string
	}{
		{&harvesterv1beta1.VirtualMachineImage{}, harvesterSystemNamespace, imageComponent},
		{&appsv1.Deployment{}, harvesterSystemNamespace, repoComponent},
		{&corev1.Service{}, harvesterSystemNamespace, repoComponent},
		{&upgradev1.Plan{}, cattleSystemNamespace, PrepareComponent},
		{&upgradev1.Plan{}, cattleSystemNamespace, ImageCleanupComponent},
		{&batchv1.Job{}, harvesterSystemNamespace, ClusterComponent},
		{&batchv1.Job{}, harvesterSystemNamespace, NodeComponent},
	}

	for _, r := range resourcesToDelete {
		if err := c.DeleteAllOf(ctx, r.obj,
			client.InNamespace(r.namespace),
			client.MatchingLabels{
				HarvesterUpgradePlanLabel:      upgradePlan.Name,
				HarvesterUpgradeComponentLabel: r.component,
			},
			&client.DeleteAllOfOptions{DeleteOptions: client.DeleteOptions{
				PropagationPolicy: ptr.To(metav1.DeletePropagationBackground),
			}},
		); err != nil {
			// If the CRD is not installed, there is nothing to clean up
			if apimeta.IsNoMatchError(err) {
				continue
			}
			return err
		}
	}

	if err := cleanupNodePendingOSImageAnnotations(ctx, c); err != nil {
		return err
	}

	if err := revertClusterUpgradeStrategy(ctx, c, upgradePlan); err != nil {
		return err
	}

	return nil
}

func cleanupNodePendingOSImageAnnotations(ctx context.Context, c client.Client) error {
	var nodeList corev1.NodeList
	if err := c.List(ctx, &nodeList); err != nil {
		return err
	}

	for i := range nodeList.Items {
		node := &nodeList.Items[i]
		if _, ok := node.Annotations[PendingOSImageAnnotation]; !ok {
			continue
		}

		patch := client.MergeFrom(node.DeepCopy())
		delete(node.Annotations, PendingOSImageAnnotation)
		if err := c.Patch(ctx, node, patch); err != nil {
			return err
		}
	}

	return nil
}

func revertClusterUpgradeStrategy(
	ctx context.Context,
	c client.Client,
	upgradePlan *managementv1beta1.UpgradePlan,
) error {
	if upgradePlan.Status.ProvisionGeneration == nil {
		return nil
	}

	var cluster provisioningv1.Cluster
	if err := c.Get(ctx, types.NamespacedName{
		Namespace: FleetLocalNamespace,
		Name:      LocalClusterName,
	}, &cluster); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("failed to get Cluster resource: %w", err)
	}

	if cluster.Spec.RKEConfig == nil {
		return nil
	}

	raw := []byte(`{"spec":{"rkeConfig":{"upgradeStrategy":null}}}`)
	if err := c.Patch(ctx, &cluster, client.RawPatch(types.MergePatchType, raw)); err != nil {
		return fmt.Errorf("failed to revert Cluster upgradeStrategy: %w", err)
	}

	return nil
}
