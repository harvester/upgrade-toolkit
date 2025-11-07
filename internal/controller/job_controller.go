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
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	managementv1beta1 "github.com/harvester/upgrade-toolkit/api/v1beta1"
	"github.com/harvester/upgrade-toolkit/pkg/upgradeplan"
)

const (
	nodeLabel = "upgrade.cattle.io/node"
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

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the Job object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.21.0/pkg/reconcile
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
		r.nodeLabelUpdate,
	}

	for _, reconciler := range reconcilers {
		if err := reconciler(ctx, jobCopy); err != nil {
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
	nodeName, ok := job.Labels[nodeLabel]
	if !ok {
		return fmt.Errorf("label %s not found", nodeLabel)
	}

	if upgradeComponent == upgradeplan.ClusterComponent {
		return nil
	}

	var upgradePlan managementv1beta1.UpgradePlan
	if err := r.Get(ctx, types.NamespacedName{Name: upgradePlanName}, &upgradePlan); err != nil {
		return err
	}

	upgradePlanCopy := upgradePlan.DeepCopy()

	nodeUpgradeStatus := buildNodeUpgradeStatus(job, upgradeComponent)

	if upgradePlan.Status.NodeUpgradeStatuses == nil {
		upgradePlanCopy.Status.NodeUpgradeStatuses = make(map[string]managementv1beta1.NodeUpgradeStatus)
	}
	upgradePlanCopy.Status.NodeUpgradeStatuses[nodeName] = nodeUpgradeStatus

	if !reflect.DeepEqual(upgradePlan.Status, upgradePlanCopy.Status) {
		return r.Status().Update(ctx, upgradePlanCopy)
	}

	return nil
}

func (r *JobReconciler) nodeLabelUpdate(ctx context.Context, job *batchv1.Job) error {
	r.Log.V(1).Info("node label update")

	upgradePlanName, ok := job.Labels[upgradeplan.HarvesterUpgradePlanLabel]
	if !ok {
		return fmt.Errorf("label %s not found", upgradeplan.HarvesterUpgradePlanLabel)
	}
	upgradeComponent, ok := job.Labels[upgradeplan.HarvesterUpgradeComponentLabel]
	if !ok {
		return fmt.Errorf("label %s not found", upgradeplan.HarvesterUpgradeComponentLabel)
	}
	nodeName, ok := job.Labels[nodeLabel]
	if !ok {
		return fmt.Errorf("label %s not found", nodeLabel)
	}

	if upgradeComponent != upgradeplan.NodeComponent {
		return nil
	}

	finished, success := isJobFinished(job)

	// Job still running or failed
	if !finished || !success {
		return nil
	}

	var node corev1.Node
	if err := r.Get(ctx, types.NamespacedName{Name: nodeName}, &node); err != nil {
		return err
	}

	var upgradePlan managementv1beta1.UpgradePlan
	if err := r.Get(ctx, types.NamespacedName{Name: upgradePlanName}, &upgradePlan); err != nil {
		return err
	}

	// Nudge the node to the next upgrade state once the Job has finished successfully
	if err := updateNodeLabel(&node, &upgradePlan); err != nil {
		return err
	}

	if err := r.Update(ctx, &node); err != nil {
		return err
	}

	return nil
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

	if _, nodeLabelExists := job.Labels[nodeLabel]; !nodeLabelExists {
		return false
	}

	return true
}

func defaultStateFor(component, t string) string {
	switch {
	case component == upgradeplan.PrepareComponent:
		return managementv1beta1.NodeStateImagePreloading
	case component == upgradeplan.NodeComponent && t == upgradeplan.NodeUpgradeTypeKubernetes:
		return managementv1beta1.NodeStateKubernetesUpgrading
	case component == upgradeplan.NodeComponent && t == upgradeplan.NodeUpgradeTypeOS:
		return managementv1beta1.NodeStateOSUpgrading
	default:
		return ""
	}
}

func successStateFor(component, t string) string {
	switch {
	case component == upgradeplan.PrepareComponent:
		return managementv1beta1.NodeStateImagePreloaded
	case component == upgradeplan.NodeComponent && t == upgradeplan.NodeUpgradeTypeKubernetes:
		return managementv1beta1.NodeStateKubernetesUpgraded
	case component == upgradeplan.NodeComponent && t == upgradeplan.NodeUpgradeTypeOS:
		return managementv1beta1.NodeStateOSUpgraded
	default:
		return ""
	}
}

func failureStateFor(component, t string) string {
	switch {
	case component == upgradeplan.PrepareComponent:
		return managementv1beta1.NodeStateImagePreloadFailed
	case component == upgradeplan.NodeComponent && t == upgradeplan.NodeUpgradeTypeKubernetes:
		return managementv1beta1.NodeStateKubernetesUpgradeFailed
	case component == upgradeplan.NodeComponent && t == upgradeplan.NodeUpgradeTypeOS:
		return managementv1beta1.NodeStateOSUpgradeFailed
	default:
		return ""
	}
}

func buildNodeUpgradeStatus(job *batchv1.Job, upgradeComponent string) managementv1beta1.NodeUpgradeStatus {
	jobType := job.Labels[upgradeplan.HarvesterNodeUpgradeTypeLabel]

	for _, condition := range job.Status.Conditions {
		if condition.Status != corev1.ConditionTrue {
			continue
		}
		switch condition.Type {
		case batchv1.JobComplete:
			return managementv1beta1.NodeUpgradeStatus{
				State: successStateFor(upgradeComponent, jobType),
			}
		case batchv1.JobFailed:
			return managementv1beta1.NodeUpgradeStatus{
				State:   failureStateFor(upgradeComponent, jobType),
				Reason:  condition.Reason,
				Message: condition.Message,
			}
		}
	}

	return managementv1beta1.NodeUpgradeStatus{
		State: defaultStateFor(upgradeComponent, jobType),
	}
}

func isJobFinished(job *batchv1.Job) (finished, success bool) {
	for _, condition := range job.Status.Conditions {
		if condition.Type == batchv1.JobComplete && condition.Status == corev1.ConditionTrue {
			finished, success = true, true
			return
		}
		if condition.Type == batchv1.JobFailed && condition.Status == corev1.ConditionTrue {
			finished, success = true, false
			return
		}
	}
	return
}

func updateNodeLabel(node *corev1.Node, upgradePlan *managementv1beta1.UpgradePlan) error {
	if node.Labels == nil {
		node.Labels = make(map[string]string)
	}

	nodeUpgradeDesiredStateLabelKey := fmt.Sprintf("%s/%s", upgradeplan.LabelPrefix, upgradePlan.Name)
	desiredState, ok := node.Labels[nodeUpgradeDesiredStateLabelKey]
	if !ok {
		return nil
	}

	switch desiredState {
	case upgradeplan.KubernetesUpgradeState:
		// If the desired upgrade state for the node is KubernetesUpgradeState and the node is already at
		// NodeStateKubernetesUpgraded, nudge the node to enter OSUpgradeState.
		currentStatus, ok := upgradePlan.Status.NodeUpgradeStatuses[node.Name]
		if !ok {
			return fmt.Errorf("node %s not found in the node upgrade statuses map", node.Name)
		}
		if currentStatus.State != managementv1beta1.NodeStateKubernetesUpgraded {
			return nil
		}

		if upgradePlan.Spec.SkipOSUpgrade != nil && *upgradePlan.Spec.SkipOSUpgrade {
			delete(node.Labels, nodeUpgradeDesiredStateLabelKey)
			return nil
		}

		node.Labels[nodeUpgradeDesiredStateLabelKey] = upgradeplan.OSUpgradeState
	case upgradeplan.OSUpgradeState:
		// If the desired upgrade state for the node is OSUpgradeState and the node is already at
		// NodeStateOSUpgraded, the node is considered fully upgraded.
		currentStatus, ok := upgradePlan.Status.NodeUpgradeStatuses[node.Name]
		if !ok {
			return fmt.Errorf("node %s not found in the node upgrade statuses map", node.Name)
		}
		if currentStatus.State != managementv1beta1.NodeStateOSUpgraded {
			return nil
		}

		delete(node.Labels, nodeUpgradeDesiredStateLabelKey)
	default:
		return fmt.Errorf("unrecognized %s label value: %s", nodeUpgradeDesiredStateLabelKey, desiredState)
	}

	return nil
}
