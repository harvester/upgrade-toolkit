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
	r.Log.V(1).Info("reconciling job")

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

	reconcilers := []reconcileFuncs{r.nodeUpgradeStatusUpdate}

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

func defaultStateFor(component string) string {
	switch component {
	case upgradeplan.PrepareComponent:
		return managementv1beta1.NodeStateImagePreloading
	case upgradeplan.NodeComponent:
		return managementv1beta1.NodeStateKubernetesUpgrading
	default:
		return ""
	}
}

func successStateFor(component string) string {
	switch component {
	case upgradeplan.PrepareComponent:
		return managementv1beta1.NodeStateImagePreloaded
	case upgradeplan.NodeComponent:
		return managementv1beta1.NodeStateKubernetesUpgraded
	default:
		return ""
	}
}

func failureStateFor(component string) string {
	switch component {
	case upgradeplan.PrepareComponent:
		return managementv1beta1.NodeStateImagePreloadFailed
	case upgradeplan.NodeComponent:
		return managementv1beta1.NodeStateKubernetesUpgradeFailed
	default:
		return ""
	}
}

func buildNodeUpgradeStatus(job *batchv1.Job, upgradeComponent string) managementv1beta1.NodeUpgradeStatus {
	status := managementv1beta1.NodeUpgradeStatus{
		State: defaultStateFor(upgradeComponent),
	}

	for _, condition := range job.Status.Conditions {
		if condition.Status != corev1.ConditionTrue {
			continue
		}
		switch condition.Type {
		case batchv1.JobComplete:
			return managementv1beta1.NodeUpgradeStatus{
				State: successStateFor(upgradeComponent),
			}
		case batchv1.JobFailed:
			return managementv1beta1.NodeUpgradeStatus{
				State:   failureStateFor(upgradeComponent),
				Reason:  condition.Reason,
				Message: condition.Message,
			}
		}
	}

	return status
}
