/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1beta1

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	harvesterv1beta1 "github.com/harvester/harvester/pkg/apis/harvesterhci.io/v1beta1"
	lhv1beta2 "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	provisioningv1 "github.com/rancher/rancher/pkg/apis/provisioning.cattle.io/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	kubevirtv1 "kubevirt.io/api/core/v1"
	clusterv1 "sigs.k8s.io/cluster-api/api/v1beta1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	managementv1beta1 "github.com/harvester/upgrade-toolkit/api/v1beta1"
	vmihelper "github.com/harvester/upgrade-toolkit/pkg/upgradehelper/vmi"
	"github.com/harvester/upgrade-toolkit/pkg/upgradeplan"
)

const annotationValueTrue = "true"

// nolint:unused
// log is for logging in this package.
var upgradeplanlog = logf.Log.WithName("upgradeplan-resource")

// SetupUpgradePlanWebhookWithManager registers the webhook for UpgradePlan in the manager.
func SetupUpgradePlanWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr).
		For(&managementv1beta1.UpgradePlan{}).
		WithValidator(&UpgradePlanCustomValidator{Client: mgr.GetClient()}).
		WithDefaulter(&UpgradePlanCustomDefaulter{}).
		Complete()
}

// +kubebuilder:webhook:path=/mutate-management-harvesterhci-io-v1beta1-upgradeplan,mutating=true,failurePolicy=fail,sideEffects=None,groups=management.harvesterhci.io,resources=upgradeplans,verbs=create;update,versions=v1beta1,name=mupgradeplan-v1beta1.kb.io,admissionReviewVersions=v1

// UpgradePlanCustomDefaulter struct is responsible for setting default values on the custom resource of the
// Kind UpgradePlan when those are created or updated.
//
// NOTE: The +kubebuilder:object:generate=false marker prevents controller-gen from generating DeepCopy methods,
// as it is used only for temporary operations and does not need to be deeply copied.
type UpgradePlanCustomDefaulter struct{}

// Default implements webhook.CustomDefaulter so a webhook will be registered for the Kind UpgradePlan.
func (d *UpgradePlanCustomDefaulter) Default(_ context.Context, obj runtime.Object) error {
	upgradePlan, ok := obj.(*managementv1beta1.UpgradePlan)
	if !ok {
		return fmt.Errorf("expected an UpgradePlan object but got %T", obj)
	}
	upgradeplanlog.Info("Defaulting for UpgradePlan", "name", upgradePlan.GetName())

	return nil
}

// +kubebuilder:webhook:path=/validate-management-harvesterhci-io-v1beta1-upgradeplan,mutating=false,failurePolicy=fail,sideEffects=None,groups=management.harvesterhci.io,resources=upgradeplans,verbs=create;update;delete,versions=v1beta1,name=vupgradeplan-v1beta1.kb.io,admissionReviewVersions=v1

// UpgradePlanCustomValidator struct is responsible for validating the UpgradePlan resource
// when it is created, updated, or deleted.
//
// NOTE: The +kubebuilder:object:generate=false marker prevents controller-gen from generating DeepCopy methods,
// as this struct is used only for temporary operations and does not need to be deeply copied.
type UpgradePlanCustomValidator struct {
	Client client.Reader
}

// ValidateCreate implements webhook.CustomValidator so a webhook will be registered for the type UpgradePlan.
func (v *UpgradePlanCustomValidator) ValidateCreate(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	upgradePlan, ok := obj.(*managementv1beta1.UpgradePlan)
	if !ok {
		return nil, fmt.Errorf("expected an UpgradePlan object but got %T", obj)
	}
	upgradeplanlog.Info("Validation for UpgradePlan upon creation", "name", upgradePlan.GetName())

	// Skip all validation if the skipWebhook annotation is set
	if upgradePlan.Annotations[upgradeplan.AnnotationSkipWebhook] == annotationValueTrue {
		return nil, nil
	}

	var allErrs field.ErrorList

	// Validate spec.version references an existing Version CR
	var version managementv1beta1.Version
	if err := v.Client.Get(ctx, client.ObjectKey{Name: upgradePlan.Spec.Version}, &version); err != nil {
		if apierrors.IsNotFound(err) {
			allErrs = append(allErrs, field.NotFound(
				field.NewPath("spec", "version"), upgradePlan.Spec.Version))
		} else {
			allErrs = append(allErrs, field.InternalError(
				field.NewPath("spec", "version"), err))
		}
	}

	// No concurrent upgrade: block if any other UpgradePlan has Progressing=True
	conflicting, err := upgradeplan.FindConflictingUpgrade(ctx, v.Client, upgradePlan.Name)
	if err != nil {
		allErrs = append(allErrs, field.InternalError(field.NewPath(""), err))
	} else if conflicting != "" {
		allErrs = append(allErrs, field.Forbidden(
			field.NewPath("spec"),
			fmt.Sprintf("another upgrade %q is in progress", conflicting)))
	}

	allErrs = append(allErrs, validateNodeReadiness(ctx, v.Client)...)
	allErrs = append(allErrs, validateClusterReady(ctx, v.Client)...)
	allErrs = append(allErrs, validateMachinesRunning(ctx, v.Client)...)
	allErrs = append(allErrs, validateNodeMachineConsistency(ctx, v.Client)...)
	allErrs = append(allErrs, validateLonghornVolumes(ctx, v.Client, upgradePlan)...)
	allErrs = append(allErrs, validateVMBackups(ctx, v.Client)...)
	allErrs = append(allErrs, validateScheduleVMBackups(ctx, v.Client)...)
	allErrs = append(allErrs, validateNonLiveMigratableVMs(ctx, v.Client)...)
	allErrs = append(allErrs, validateNoCleanupInProgress(ctx, v.Client, upgradePlan.Name)...)
	allErrs = append(allErrs, validateNodeUpgradeOption(ctx, v.Client, upgradePlan)...)

	if len(allErrs) > 0 {
		return nil, apierrors.NewInvalid(
			managementv1beta1.GroupVersion.WithKind("UpgradePlan").GroupKind(),
			upgradePlan.Name, allErrs)
	}
	return nil, nil
}

// ValidateUpdate implements webhook.CustomValidator so a webhook will be registered for the type UpgradePlan.
func (v *UpgradePlanCustomValidator) ValidateUpdate(ctx context.Context, oldObj, newObj runtime.Object) (admission.Warnings, error) {
	oldUpgradePlan, ok := oldObj.(*managementv1beta1.UpgradePlan)
	if !ok {
		return nil, fmt.Errorf("expected an UpgradePlan object but got %T", oldObj)
	}
	newUpgradePlan, ok := newObj.(*managementv1beta1.UpgradePlan)
	if !ok {
		return nil, fmt.Errorf("expected an UpgradePlan object but got %T", newObj)
	}
	upgradeplanlog.Info("Validation for UpgradePlan upon update", "name", newUpgradePlan.GetName())

	var allErrs field.ErrorList

	if oldUpgradePlan.Spec.Version != newUpgradePlan.Spec.Version {
		allErrs = append(allErrs, field.Forbidden(
			field.NewPath("spec", "version"),
			"field is immutable after creation"))
	}

	if !reflect.DeepEqual(oldUpgradePlan.Spec.Upgrade, newUpgradePlan.Spec.Upgrade) {
		allErrs = append(allErrs, field.Forbidden(
			field.NewPath("spec", "upgrade"),
			"field is immutable after creation"))
	}

	allErrs = append(allErrs, validateNodeUpgradeOption(ctx, v.Client, newUpgradePlan)...)

	if len(allErrs) > 0 {
		return nil, apierrors.NewInvalid(
			managementv1beta1.GroupVersion.WithKind("UpgradePlan").GroupKind(),
			newUpgradePlan.Name, allErrs)
	}
	return nil, nil
}

// ValidateDelete implements webhook.CustomValidator so a webhook will be registered for the type UpgradePlan.
func (v *UpgradePlanCustomValidator) ValidateDelete(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	upgradePlan, ok := obj.(*managementv1beta1.UpgradePlan)
	if !ok {
		return nil, fmt.Errorf("expected an UpgradePlan object but got %T", obj)
	}
	upgradeplanlog.Info("Validation for UpgradePlan upon deletion", "name", upgradePlan.GetName())

	var allErrs field.ErrorList

	if upgradePlan.ConditionTrue(managementv1beta1.UpgradePlanProgressing) {
		allErrs = append(allErrs, field.Forbidden(
			field.NewPath("metadata", "name"),
			"cannot delete UpgradePlan while Progressing condition is True"))
	}

	// Block deletion while the cluster is being provisioned (not ready)
	var cluster provisioningv1.Cluster
	if err := v.Client.Get(ctx, client.ObjectKey{
		Namespace: upgradeplan.FleetLocalNamespace,
		Name:      upgradeplan.LocalClusterName,
	}, &cluster); err != nil {
		if !apierrors.IsNotFound(err) {
			allErrs = append(allErrs, field.InternalError(
				field.NewPath("metadata", "name"),
				fmt.Errorf("failed to get cluster: %w", err)))
		}
	} else if !cluster.Status.Ready {
		allErrs = append(allErrs, field.Forbidden(
			field.NewPath("metadata", "name"),
			"cannot delete UpgradePlan while the cluster is being provisioned"))
	}

	// Block deletion while nodes are being upgraded
	if upgradePlan.Status.CurrentPhase == managementv1beta1.UpgradePlanPhaseNodeUpgrading {
		allErrs = append(allErrs, field.Forbidden(
			field.NewPath("metadata", "name"),
			"cannot delete UpgradePlan while nodes are being upgraded"))
	}

	if len(allErrs) > 0 {
		return nil, apierrors.NewInvalid(
			managementv1beta1.GroupVersion.WithKind("UpgradePlan").GroupKind(),
			upgradePlan.Name, allErrs)
	}
	return nil, nil
}

// validateNodeReadiness checks that all nodes are Ready and schedulable.
func validateNodeReadiness(ctx context.Context, c client.Reader) field.ErrorList {
	var allErrs field.ErrorList

	var nodeList corev1.NodeList
	if err := c.List(ctx, &nodeList); err != nil {
		allErrs = append(allErrs, field.InternalError(
			field.NewPath(""), fmt.Errorf("failed to list nodes: %w", err)))
		return allErrs
	}

	for _, node := range nodeList.Items {
		// Check node Ready condition
		ready := false
		for _, cond := range node.Status.Conditions {
			if cond.Type == corev1.NodeReady {
				ready = cond.Status == corev1.ConditionTrue
				break
			}
		}
		if !ready {
			allErrs = append(allErrs, field.Forbidden(
				field.NewPath("spec"),
				fmt.Sprintf("node %q is not Ready", node.Name)))
		}

		// Check node unschedulable
		if node.Spec.Unschedulable {
			allErrs = append(allErrs, field.Forbidden(
				field.NewPath("spec"),
				fmt.Sprintf("node %q is unschedulable", node.Name)))
		}
	}

	return allErrs
}

// validateClusterReady checks that the provisioning cluster fleet-local/local is ready.
func validateClusterReady(ctx context.Context, c client.Reader) field.ErrorList {
	var allErrs field.ErrorList

	var cluster provisioningv1.Cluster
	if err := c.Get(ctx, client.ObjectKey{
		Namespace: upgradeplan.FleetLocalNamespace,
		Name:      upgradeplan.LocalClusterName,
	}, &cluster); err != nil {
		if apierrors.IsNotFound(err) {
			allErrs = append(allErrs, field.Forbidden(
				field.NewPath("spec"),
				"cluster not found"))
		} else {
			allErrs = append(allErrs, field.InternalError(
				field.NewPath("spec"), err))
		}
		return allErrs
	}

	if !cluster.Status.Ready {
		allErrs = append(allErrs, field.Forbidden(
			field.NewPath("spec"),
			"cluster is not ready"))
	}

	return allErrs
}

// validateMachinesRunning checks that all CAPI Machines in fleet-local are in Running phase.
func validateMachinesRunning(ctx context.Context, c client.Reader) field.ErrorList {
	var allErrs field.ErrorList

	var machineList clusterv1.MachineList
	if err := c.List(ctx, &machineList, client.InNamespace(upgradeplan.FleetLocalNamespace)); err != nil {
		allErrs = append(allErrs, field.InternalError(
			field.NewPath(""), fmt.Errorf("failed to list machines: %w", err)))
		return allErrs
	}

	for _, machine := range machineList.Items {
		if machine.Status.Phase != string(clusterv1.MachinePhaseRunning) {
			allErrs = append(allErrs, field.Forbidden(
				field.NewPath("spec"),
				fmt.Sprintf("machine %s/%s is not running", machine.Namespace, machine.Name)))
		}
	}

	return allErrs
}

// validateNodeMachineConsistency checks that nodes and CAPI Machines are correctly paired.
func validateNodeMachineConsistency(ctx context.Context, c client.Reader) field.ErrorList {
	var allErrs field.ErrorList

	var nodeList corev1.NodeList
	if err := c.List(ctx, &nodeList); err != nil {
		allErrs = append(allErrs, field.InternalError(
			field.NewPath(""), fmt.Errorf("failed to list nodes: %w", err)))
		return allErrs
	}

	var machineList clusterv1.MachineList
	if err := c.List(ctx, &machineList, client.InNamespace(upgradeplan.FleetLocalNamespace)); err != nil {
		allErrs = append(allErrs, field.InternalError(
			field.NewPath(""), fmt.Errorf("failed to list machines: %w", err)))
		return allErrs
	}

	if len(nodeList.Items) == 0 {
		allErrs = append(allErrs, field.Forbidden(
			field.NewPath("spec"),
			"no nodes found in the cluster"))
		return allErrs
	}

	if len(nodeList.Items) != len(machineList.Items) {
		allErrs = append(allErrs, field.Forbidden(
			field.NewPath("spec"),
			fmt.Sprintf("node count (%d) does not match machine count (%d)",
				len(nodeList.Items), len(machineList.Items))))
	}

	// Build machine lookup by name
	machineByName := make(map[string]*clusterv1.Machine, len(machineList.Items))
	for i := range machineList.Items {
		machineByName[machineList.Items[i].Name] = &machineList.Items[i]
	}

	// Check each machine has a valid NodeRef
	for _, machine := range machineList.Items {
		if machine.Status.NodeRef == nil {
			allErrs = append(allErrs, field.Forbidden(
				field.NewPath("spec"),
				fmt.Sprintf("machine %s/%s has no node reference", machine.Namespace, machine.Name)))
		}
	}

	// Check each node's labels and annotations
	const (
		managedLabel      = "harvesterhci.io/managed"
		machineAnnotation = "cluster.x-k8s.io/machine"
	)
	for _, node := range nodeList.Items {
		if node.Labels[managedLabel] != annotationValueTrue {
			allErrs = append(allErrs, field.Forbidden(
				field.NewPath("spec"),
				fmt.Sprintf("node %q is missing %s label", node.Name, managedLabel)))
			continue
		}

		machineName, ok := node.Annotations[machineAnnotation]
		if !ok {
			allErrs = append(allErrs, field.Forbidden(
				field.NewPath("spec"),
				fmt.Sprintf("node %q is missing %s annotation", node.Name, machineAnnotation)))
			continue
		}

		machine, exists := machineByName[machineName]
		if !exists {
			allErrs = append(allErrs, field.Forbidden(
				field.NewPath("spec"),
				fmt.Sprintf("node %q references machine %q which does not exist", node.Name, machineName)))
			continue
		}

		if machine.Status.NodeRef != nil && machine.Status.NodeRef.Name != node.Name {
			allErrs = append(allErrs, field.Forbidden(
				field.NewPath("spec"),
				fmt.Sprintf("machine %q NodeRef.Name %q does not match node %q",
					machineName, machine.Status.NodeRef.Name, node.Name)))
		}
	}

	return allErrs
}

// validateLonghornVolumes checks Longhorn volume health before allowing an upgrade.
func validateLonghornVolumes(ctx context.Context, c client.Reader, upgradePlan *managementv1beta1.UpgradePlan) field.ErrorList {
	var allErrs field.ErrorList

	var volumeList lhv1beta2.VolumeList
	if err := c.List(ctx, &volumeList, client.InNamespace(upgradeplan.LonghornSystemNamespace)); err != nil {
		allErrs = append(allErrs, field.InternalError(
			field.NewPath(""), fmt.Errorf("failed to list Longhorn volumes: %w", err)))
		return allErrs
	}

	// Count nodes for the degraded volume check (only enforce on 3+ node clusters)
	var nodeList corev1.NodeList
	if err := c.List(ctx, &nodeList); err != nil {
		allErrs = append(allErrs, field.InternalError(
			field.NewPath(""), fmt.Errorf("failed to list nodes: %w", err)))
		return allErrs
	}

	// Check for degraded volumes (only on 3+ node clusters)
	if len(nodeList.Items) >= 3 {
		for _, volume := range volumeList.Items {
			if volume.Status.Robustness == lhv1beta2.VolumeRobustnessDegraded {
				allErrs = append(allErrs, field.Forbidden(
					field.NewPath("spec"),
					"there are degraded volumes, please check all volumes are healthy"))
				break
			}
		}
	}

	// Collect single-replica volumes
	var activeSingleReplicaVols []string
	var detachedSingleReplicaVols []string
	for _, volume := range volumeList.Items {
		if volume.Spec.NumberOfReplicas != 1 {
			continue
		}
		pvcRef := volume.Status.KubernetesStatus.Namespace + "/" + volume.Status.KubernetesStatus.PVCName
		switch volume.Status.State {
		case lhv1beta2.VolumeStateCreating, lhv1beta2.VolumeStateAttached, lhv1beta2.VolumeStateAttaching:
			activeSingleReplicaVols = append(activeSingleReplicaVols, pvcRef)
		default:
			detachedSingleReplicaVols = append(detachedSingleReplicaVols, pvcRef)
		}
	}

	// Reject active single-replica volumes
	if len(activeSingleReplicaVols) > 0 {
		allErrs = append(allErrs, field.Forbidden(
			field.NewPath("spec"),
			fmt.Sprintf("active single-replica volumes found: %s", strings.Join(activeSingleReplicaVols, ", "))))
	}

	// Reject detached single-replica volumes unless skip annotation is set
	skipDetached := upgradePlan.Annotations[upgradeplan.AnnotationSkipSingleReplicaDetachedVol] == annotationValueTrue
	if !skipDetached && len(detachedSingleReplicaVols) > 0 {
		allErrs = append(allErrs, field.Forbidden(
			field.NewPath("spec"),
			fmt.Sprintf("single-replica volumes found: %s", strings.Join(detachedSingleReplicaVols, ", "))))
	}

	return allErrs
}

// validateNoCleanupInProgress checks that no other UpgradePlan is in the CleaningUp phase.
func validateNoCleanupInProgress(ctx context.Context, c client.Reader, currentName string) field.ErrorList {
	var allErrs field.ErrorList

	var upgradePlanList managementv1beta1.UpgradePlanList
	if err := c.List(ctx, &upgradePlanList); err != nil {
		allErrs = append(allErrs, field.InternalError(
			field.NewPath(""), fmt.Errorf("failed to list upgrade plans: %w", err)))
		return allErrs
	}

	for _, up := range upgradePlanList.Items {
		if up.Name == currentName {
			continue
		}
		if up.Status.CurrentPhase == managementv1beta1.UpgradePlanPhaseCleaningUp {
			allErrs = append(allErrs, field.Forbidden(
				field.NewPath("spec"),
				fmt.Sprintf("upgrade %q is still cleaning up", up.Name)))
		}
	}

	return allErrs
}

// validateVMBackups checks that no VirtualMachineBackup is currently in progress.
func validateVMBackups(ctx context.Context, c client.Reader) field.ErrorList {
	var allErrs field.ErrorList

	var backupList harvesterv1beta1.VirtualMachineBackupList
	if err := c.List(ctx, &backupList); err != nil {
		allErrs = append(allErrs, field.InternalError(
			field.NewPath(""), fmt.Errorf("failed to list VM backups: %w", err)))
		return allErrs
	}

	for _, backup := range backupList.Items {
		if backup.Status.ReadyToUse == nil || !*backup.Status.ReadyToUse {
			if backup.Status.Error != nil {
				continue
			}
			allErrs = append(allErrs, field.Forbidden(
				field.NewPath("spec"),
				fmt.Sprintf("please wait until all vmbackups are stopped, for example %s/%s is under processing",
					backup.Namespace, backup.Name)))
			break
		}
	}

	return allErrs
}

// validateScheduleVMBackups checks that all ScheduleVMBackup objects are suspended.
func validateScheduleVMBackups(ctx context.Context, c client.Reader) field.ErrorList {
	var allErrs field.ErrorList

	var scheduleList harvesterv1beta1.ScheduleVMBackupList
	if err := c.List(ctx, &scheduleList); err != nil {
		allErrs = append(allErrs, field.InternalError(
			field.NewPath(""), fmt.Errorf("failed to list schedule VM backups: %w", err)))
		return allErrs
	}

	for _, schedule := range scheduleList.Items {
		if !schedule.Status.Suspended {
			allErrs = append(allErrs, field.Forbidden(
				field.NewPath("spec"),
				fmt.Sprintf("please suspend all backup/snapshot schedule, for example %s/%s is running",
					schedule.Namespace, schedule.Name)))
			break
		}
	}

	return allErrs
}

// validateNonLiveMigratableVMs checks that there are no non-live-migratable VMIs on multi-node clusters.
func validateNonLiveMigratableVMs(ctx context.Context, c client.Reader) field.ErrorList {
	var allErrs field.ErrorList

	var nodeList corev1.NodeList
	if err := c.List(ctx, &nodeList); err != nil {
		allErrs = append(allErrs, field.InternalError(
			field.NewPath(""), fmt.Errorf("failed to list nodes: %w", err)))
		return allErrs
	}

	// Convert to pointer slice for the helper
	nodes := make([]*corev1.Node, len(nodeList.Items))
	for i := range nodeList.Items {
		nodes[i] = &nodeList.Items[i]
	}

	// On single non-witness node clusters, VMs will be shut down during upgrade
	nonWitnessCount := 0
	for _, node := range nodes {
		if _, isWitness := node.Labels["node-role.harvesterhci.io/witness"]; !isWitness {
			nonWitnessCount++
		}
	}
	if nonWitnessCount <= 1 {
		return nil
	}

	var vmiList kubevirtv1.VirtualMachineInstanceList
	if err := c.List(ctx, &vmiList); err != nil {
		allErrs = append(allErrs, field.InternalError(
			field.NewPath(""), fmt.Errorf("failed to list VMIs: %w", err)))
		return allErrs
	}

	vmis := make([]*kubevirtv1.VirtualMachineInstance, len(vmiList.Items))
	for i := range vmiList.Items {
		vmis[i] = &vmiList.Items[i]
	}

	nonMigratable, err := vmihelper.GetAllNonLiveMigratableVMINames(vmis, nodes)
	if err != nil {
		allErrs = append(allErrs, field.InternalError(
			field.NewPath(""), fmt.Errorf("failed to check non-live migratable VMs: %w", err)))
		return allErrs
	}

	if len(nonMigratable) > 0 {
		allErrs = append(allErrs, field.Forbidden(
			field.NewPath("spec"),
			fmt.Sprintf("there are non-live migratable VMs that need to be shut off before initiating the upgrade: %s",
				strings.Join(nonMigratable, ", "))))
	}

	return allErrs
}

// validateNodeUpgradeOption validates the nodeUpgradeOption field.
func validateNodeUpgradeOption(ctx context.Context, c client.Reader, upgradePlan *managementv1beta1.UpgradePlan) field.ErrorList {
	var allErrs field.ErrorList

	opt := upgradePlan.Spec.NodeUpgradeOption
	if opt == nil || len(opt.PauseNodes) == 0 {
		return nil
	}

	pauseNodesPath := field.NewPath("spec", "nodeUpgradeOption", "pauseNodes")

	// Build a set of existing node names for membership checks
	var nodeList corev1.NodeList
	if err := c.List(ctx, &nodeList); err != nil {
		allErrs = append(allErrs, field.InternalError(
			pauseNodesPath, fmt.Errorf("failed to list nodes: %w", err)))
		return allErrs
	}
	nodeSet := make(map[string]struct{}, len(nodeList.Items))
	for _, node := range nodeList.Items {
		nodeSet[node.Name] = struct{}{}
	}

	// Validate individual pauseNodes entries
	seen := make(map[string]bool, len(opt.PauseNodes))
	for i, n := range opt.PauseNodes {
		if n == "" {
			allErrs = append(allErrs, field.Required(
				pauseNodesPath.Index(i),
				"node name must not be empty"))
		} else {
			if _, exists := nodeSet[n]; !exists {
				allErrs = append(allErrs, field.NotFound(
					pauseNodesPath.Index(i), n))
			}
		}
		if seen[n] {
			allErrs = append(allErrs, field.Duplicate(
				pauseNodesPath.Index(i), n))
		}
		seen[n] = true
	}

	return allErrs
}
