package upgradeplan

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	harvesterv1beta1 "github.com/harvester/harvester/pkg/apis/harvesterhci.io/v1beta1"
	lhv1beta2 "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	provisioningv1 "github.com/rancher/rancher/pkg/apis/provisioning.cattle.io/v1"
	upgradev1 "github.com/rancher/system-upgrade-controller/pkg/apis/upgrade.cattle.io/v1"
	wranglername "github.com/rancher/wrangler/v3/pkg/name"
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
		jobType   string // optional: when set, also match HarvesterJobTypeLabel
	}{
		{&harvesterv1beta1.VirtualMachineImage{}, harvesterSystemNamespace, imageComponent, ""},
		{&appsv1.Deployment{}, harvesterSystemNamespace, repoComponent, ""},
		{&corev1.Service{}, harvesterSystemNamespace, repoComponent, ""},
		{&upgradev1.Plan{}, cattleSystemNamespace, PrepareComponent, ""},
		{&upgradev1.Plan{}, cattleSystemNamespace, ImageCleanupComponent, ""},
		{&batchv1.Job{}, harvesterSystemNamespace, ClusterComponent, ""},
		// Delete node-upgrade jobs by specific type so that restore-vm jobs
		// (which may still be running for the last upgraded node) are preserved.
		{&batchv1.Job{}, harvesterSystemNamespace, NodeComponent, JobTypePreDrain},
		{&batchv1.Job{}, harvesterSystemNamespace, NodeComponent, JobTypePostDrain},
		{&batchv1.Job{}, harvesterSystemNamespace, NodeComponent, JobTypeSingleNodeUpgrade},
	}

	for _, r := range resourcesToDelete {
		labels := client.MatchingLabels{
			HarvesterUpgradePlanLabel:      upgradePlan.Name,
			HarvesterUpgradeComponentLabel: r.component,
		}
		if r.jobType != "" {
			labels[HarvesterJobTypeLabel] = r.jobType
		}
		if err := c.DeleteAllOf(ctx, r.obj,
			client.InNamespace(r.namespace),
			labels,
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

	if err := cleanupRestoreVMConfigMap(ctx, c, upgradePlan); err != nil {
		return err
	}

	if err := cleanupNodePendingOSImageAnnotations(ctx, c); err != nil {
		return err
	}

	if err := revertClusterUpgradeStrategy(ctx, c, upgradePlan); err != nil {
		return err
	}

	if err := restoreLonghornReplicaReplenishmentInterval(ctx, c, log, upgradePlan); err != nil {
		return err
	}

	if err := reEnableDeschedulerAddon(ctx, c, log, upgradePlan); err != nil {
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

// restoreLonghornReplicaReplenishmentInterval restores the Longhorn
// replica-replenishment-wait-interval setting to its pre-upgrade value.
func restoreLonghornReplicaReplenishmentInterval(
	ctx context.Context,
	c client.Client,
	log logr.Logger,
	upgradePlan *managementv1beta1.UpgradePlan,
) error {
	originalValue, ok := upgradePlan.Annotations[AnnotationReplicaReplenishmentOriginal]
	if !ok {
		return nil
	}

	var setting lhv1beta2.Setting
	if err := c.Get(ctx, types.NamespacedName{
		Namespace: LonghornSystemNamespace,
		Name:      LonghornSettingReplicaReplenishment,
	}, &setting); err != nil {
		if apierrors.IsNotFound(err) || apimeta.IsNoMatchError(err) {
			log.V(1).Info("Longhorn replica-replenishment-wait-interval setting not found during cleanup, skipping")
			return nil
		}
		return fmt.Errorf("failed to get Longhorn setting %s during cleanup: %w", LonghornSettingReplicaReplenishment, err)
	}

	patch := client.MergeFrom(setting.DeepCopy())
	setting.Value = originalValue
	if err := c.Patch(ctx, &setting, patch); err != nil {
		return fmt.Errorf("failed to restore Longhorn setting %s: %w", LonghornSettingReplicaReplenishment, err)
	}

	log.Info("restored Longhorn replica replenishment wait interval", "value", originalValue)
	return nil
}

// reEnableDeschedulerAddon re-enables the descheduler addon if it was disabled
// during upgrade.
func reEnableDeschedulerAddon(
	ctx context.Context,
	c client.Client,
	log logr.Logger,
	upgradePlan *managementv1beta1.UpgradePlan,
) error {
	wasEnabled, ok := upgradePlan.Annotations[AnnotationDeschedulerWasEnabled]
	if !ok || wasEnabled != valueTrue {
		return nil
	}

	var addon harvesterv1beta1.Addon
	if err := c.Get(ctx, types.NamespacedName{
		Namespace: DeschedulerAddonNamespace,
		Name:      DeschedulerAddonName,
	}, &addon); err != nil {
		if apierrors.IsNotFound(err) || apimeta.IsNoMatchError(err) {
			log.V(1).Info("descheduler addon not found during cleanup, skipping")
			return nil
		}
		return fmt.Errorf("failed to get descheduler addon during cleanup: %w", err)
	}

	if !addon.Spec.Enabled {
		patch := client.MergeFrom(addon.DeepCopy())
		addon.Spec.Enabled = true
		if err := c.Patch(ctx, &addon, patch); err != nil {
			return fmt.Errorf("failed to re-enable descheduler addon: %w", err)
		}
		log.Info("re-enabled descheduler addon after upgrade")
	}

	return nil
}

// cleanupRestoreVMConfigMap deletes the restore-vm ConfigMap if it exists.
// This is best-effort; the ConfigMap may not exist if restoreVM was not enabled
// or the ConfigMap was never created.
func cleanupRestoreVMConfigMap(
	ctx context.Context,
	c client.Client,
	upgradePlan *managementv1beta1.UpgradePlan,
) error {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      restoreVMConfigMapName(upgradePlan.Name),
			Namespace: harvesterSystemNamespace,
		},
	}
	if err := c.Delete(ctx, cm); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}

// restoreVMConfigMapName returns the deterministic name for the restore-vm
// ConfigMap associated with the given UpgradePlan.
func restoreVMConfigMapName(upgradePlanName string) string {
	return wranglername.SafeConcatName(upgradePlanName, "restore-vm")
}
