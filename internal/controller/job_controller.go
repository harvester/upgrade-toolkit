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

package controller

import (
	"context"
	"fmt"
	"reflect"

	"github.com/go-logr/logr"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	managementv1beta1 "github.com/harvester/upgrade-toolkit/api/v1beta1"
	"github.com/harvester/upgrade-toolkit/pkg/upgradeplan"
)

// JobReconciler reconciles a Job object
type JobReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	Log    logr.Logger
}

type reconcileFuncs func(context.Context, *batchv1.Job) error

// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch
// +kubebuilder:rbac:groups=batch,resources=jobs/status,verbs=get
// +kubebuilder:rbac:groups=core,resources=nodes,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=management.harvesterhci.io,resources=upgradeplans/status,verbs=get;update;patch

func (r *JobReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	r.Log.V(2).Info("reconciling job")

	var job batchv1.Job
	if err := r.Get(ctx, req.NamespacedName, &job); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	jobCopy := job.DeepCopy()

	if !job.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	// Filter out jobs that are not of our interests
	if !isHarvesterUpgradePlanJobs(&job) {
		return ctrl.Result{}, nil
	}

	reconcilers := []reconcileFuncs{
		r.nodeUpgradeStatusUpdate,
	}

	for _, reconciler := range reconcilers {
		if err := reconciler(ctx, jobCopy); err != nil {
			if apierrors.IsConflict(err) {
				return ctrl.Result{RequeueAfter: upgradeplan.RequeueAfterDuration}, nil
			}
			return ctrl.Result{}, err
		}
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *JobReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&batchv1.Job{}).
		Named("job").
		Complete(r)
}

func (r *JobReconciler) nodeUpgradeStatusUpdate(ctx context.Context, job *batchv1.Job) error {
	r.Log.V(1).Info("node upgrade status update")

	upgradePlanName, ok := job.Labels[upgradeplan.HarvesterUpgradePlanLabel]
	if !ok {
		return fmt.Errorf("label %s not found", upgradeplan.HarvesterUpgradePlanLabel)
	}
	upgradeComponent, ok := job.Labels[upgradeplan.HarvesterUpgradeComponentLabel]
	if !ok {
		return fmt.Errorf("label %s not found", upgradeplan.HarvesterUpgradeComponentLabel)
	}

	var nodeName string
	switch upgradeComponent {
	case upgradeplan.PrepareComponent:
		nodeName, ok = job.Labels[upgradeplan.SUCNodeLabel]
		if !ok {
			return fmt.Errorf("label %s not found", upgradeplan.SUCNodeLabel)
		}
	case upgradeplan.ClusterComponent:
		return nil
	case upgradeplan.NodeComponent:
		nodeName, ok = job.Labels[upgradeplan.HarvesterUpgradeNodeLabel]
		if !ok {
			return fmt.Errorf("label %s not found", upgradeplan.HarvesterUpgradeNodeLabel)
		}
	case upgradeplan.ImageCleanupComponent:
		nodeName, ok = job.Labels[upgradeplan.SUCNodeLabel]
		if !ok {
			return fmt.Errorf("label %s not found", upgradeplan.SUCNodeLabel)
		}
	default:
		r.Log.V(0).Info("cannot identify upgrade component due to missing node label, skip it", "jobNamespace", job.Namespace, "jobName", job.Name)
		return nil
	}

	var upgradePlan managementv1beta1.UpgradePlan
	if err := r.Get(ctx, types.NamespacedName{Name: upgradePlanName}, &upgradePlan); err != nil {
		return err
	}

	upgradePlanCopy := upgradePlan.DeepCopy()

	nodeUpgradeStatus := buildNodeUpgradeStatus(job, upgradeComponent)

	// Forward-progress guard: skip update if the node has already progressed past this state.
	if currentStatus, exists := upgradePlan.Status.NodeUpgradeStatuses[nodeName]; exists {
		if managementv1beta1.IsNodeUpgradeStateAhead(currentStatus.State, nodeUpgradeStatus.State) {
			return nil
		}
	}

	if upgradePlan.Status.NodeUpgradeStatuses == nil {
		upgradePlanCopy.Status.NodeUpgradeStatuses = make(map[string]managementv1beta1.NodeUpgradeStatus)
	}
	upgradePlanCopy.Status.NodeUpgradeStatuses[nodeName] = nodeUpgradeStatus

	if !reflect.DeepEqual(upgradePlan.Status, upgradePlanCopy.Status) {
		if err := r.Status().Update(ctx, upgradePlanCopy); err != nil {
			return err
		}
	}

	// When entering WaitingReboot, annotate the Node with the expected OS version
	if nodeUpgradeStatus.State == managementv1beta1.NodeStateWaitingReboot {
		if err := r.setNodePendingOSImage(ctx, nodeName, &upgradePlan); err != nil {
			return err
		}
	}

	return nil
}

func (r *JobReconciler) setNodePendingOSImage(ctx context.Context, nodeName string, upgradePlan *managementv1beta1.UpgradePlan) error {
	expectedOS := ""
	if upgradePlan.Status.ReleaseMetadata != nil {
		expectedOS = upgradePlan.Status.ReleaseMetadata.OS
	}
	if expectedOS == "" {
		return nil
	}

	var node corev1.Node
	if err := r.Get(ctx, types.NamespacedName{Name: nodeName}, &node); err != nil {
		return err
	}

	patch := client.MergeFrom(node.DeepCopy())
	if node.Annotations == nil {
		node.Annotations = make(map[string]string)
	}
	node.Annotations[upgradeplan.PendingOSImageAnnotation] = expectedOS
	return r.Patch(ctx, &node, patch)
}

func isHarvesterUpgradePlanJobs(job *batchv1.Job) bool {
	if job.Labels == nil {
		return false
	}

	if _, upgradePlanLabelExists := job.Labels[upgradeplan.HarvesterUpgradePlanLabel]; !upgradePlanLabelExists {
		return false
	}

	if _, upgradeComponentLabelExists := job.Labels[upgradeplan.HarvesterUpgradeComponentLabel]; !upgradeComponentLabelExists {
		return false
	}

	return true
}

func defaultStateFor(component, hookType string) managementv1beta1.NodeUpgradeState {
	switch {
	case component == upgradeplan.PrepareComponent:
		return managementv1beta1.NodeStateImagePreloading
	case component == upgradeplan.NodeComponent && hookType == upgradeplan.JobTypePreDrain:
		return managementv1beta1.NodeStatePreDraining
	case component == upgradeplan.NodeComponent && hookType == upgradeplan.JobTypePostDrain:
		return managementv1beta1.NodeStatePostDraining
	case component == upgradeplan.NodeComponent && hookType == upgradeplan.JobTypeSingleNodeUpgrade:
		return managementv1beta1.NodeStateSingleNodeUpgrading
	case component == upgradeplan.ImageCleanupComponent:
		return managementv1beta1.NodeStateImageCleaning
	default:
		return ""
	}
}

func successStateFor(component, hookType string) managementv1beta1.NodeUpgradeState {
	switch {
	case component == upgradeplan.PrepareComponent:
		return managementv1beta1.NodeStateImagePreloaded
	case component == upgradeplan.NodeComponent && hookType == upgradeplan.JobTypePreDrain:
		return managementv1beta1.NodeStatePreDrained
	case component == upgradeplan.NodeComponent && hookType == upgradeplan.JobTypePostDrain:
		return managementv1beta1.NodeStateWaitingReboot
	case component == upgradeplan.NodeComponent && hookType == upgradeplan.JobTypeSingleNodeUpgrade:
		return managementv1beta1.NodeStateWaitingReboot
	case component == upgradeplan.ImageCleanupComponent:
		return managementv1beta1.NodeStateImageCleaned
	default:
		return ""
	}
}

func failureStateFor(component, hookType string) managementv1beta1.NodeUpgradeState {
	switch {
	case component == upgradeplan.PrepareComponent:
		return managementv1beta1.NodeStateImagePreloadFailed
	case component == upgradeplan.NodeComponent && hookType == upgradeplan.JobTypePreDrain:
		return managementv1beta1.NodeStatePreDrainFailed
	case component == upgradeplan.NodeComponent && hookType == upgradeplan.JobTypePostDrain:
		return managementv1beta1.NodeStatePostDrainFailed
	case component == upgradeplan.NodeComponent && hookType == upgradeplan.JobTypeSingleNodeUpgrade:
		return managementv1beta1.NodeStateSingleNodeUpgradeFailed
	case component == upgradeplan.ImageCleanupComponent:
		return managementv1beta1.NodeStateImageCleanFailed
	default:
		return ""
	}
}

func buildNodeUpgradeStatus(job *batchv1.Job, upgradeComponent string) managementv1beta1.NodeUpgradeStatus {
	hookType := job.Labels[upgradeplan.HarvesterJobTypeLabel]

	for _, condition := range job.Status.Conditions {
		if condition.Status != corev1.ConditionTrue {
			continue
		}
		switch condition.Type {
		case batchv1.JobComplete:
			return managementv1beta1.NodeUpgradeStatus{
				State: successStateFor(upgradeComponent, hookType),
			}
		case batchv1.JobFailed:
			return managementv1beta1.NodeUpgradeStatus{
				State:   failureStateFor(upgradeComponent, hookType),
				Reason:  condition.Reason,
				Message: condition.Message,
			}
		}
	}

	return managementv1beta1.NodeUpgradeStatus{
		State: defaultStateFor(upgradeComponent, hookType),
	}
}
