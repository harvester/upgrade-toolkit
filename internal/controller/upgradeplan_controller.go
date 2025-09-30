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
	"reflect"
	"time"

	"github.com/go-logr/logr"
	harvesterv1beta1 "github.com/harvester/harvester/pkg/apis/harvesterhci.io/v1beta1"
	upgradev1 "github.com/rancher/system-upgrade-controller/pkg/apis/upgrade.cattle.io/v1"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	managementv1beta1 "github.com/harvester/upgrade-toolkit/api/v1beta1"
	"github.com/harvester/upgrade-toolkit/pkg/upgradeplan"
)

// UpgradePlanReconciler reconciles a UpgradePlan object
type UpgradePlanReconciler struct {
	client.Client
	Scheme      *runtime.Scheme
	Log         logr.Logger
	upgradePlan *upgradeplan.UpgradePlan
}

// +kubebuilder:rbac:groups=management.harvesterhci.io,resources=upgradeplans,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=management.harvesterhci.io,resources=upgradeplans/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=management.harvesterhci.io,resources=upgradeplans/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=daemonsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update
// +kubebuilder:rbac:groups=core,resources=nodes,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=harvesterhci.io,resources=settings,verbs=get;list;watch
// +kubebuilder:rbac:groups=harvesterhci.io,resources=virtualmachineimages,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=upgrade.cattle.io,resources=plans,verbs=get;list;watch;create;update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the UpgradePlan object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.21.0/pkg/reconcile
func (r *UpgradePlanReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	r.Log.V(1).Info("reconciling upgradeplan")

	var upgradePlan managementv1beta1.UpgradePlan
	if err := r.Get(ctx, req.NamespacedName, &upgradePlan); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	upgradePlanCopy := upgradePlan.DeepCopy()

	// Handle deletion
	if !upgradePlan.DeletionTimestamp.IsZero() {
		r.Log.V(1).Info("upgradeplan under deletion")

		// TODO: Tear down all the stuff we've created that can't be handled by cascade deletion.
		return ctrl.Result{}, nil
	}

	// Unavailable UpgradePlans are ignored
	if upgradePlan.ConditionFalse(managementv1beta1.UpgradePlanAvailable) {
		return ctrl.Result{}, nil
	}

	result, err := r.upgradePlan.ExecutePhase(ctx, upgradePlanCopy)
	if err != nil {
		upgradePlanCopy.SetCondition(managementv1beta1.UpgradePlanDegraded, metav1.ConditionTrue, "ReconcileError", err.Error())
	} else {
		upgradePlanCopy.SetCondition(managementv1beta1.UpgradePlanDegraded, metav1.ConditionFalse, "ReconcileSuccess", "")
	}

	setUpgradePlanPhaseTransitionTimestamp(&upgradePlan, upgradePlanCopy)

	if !reflect.DeepEqual(upgradePlan.Status, upgradePlanCopy.Status) {
		if statusUpdateErr := r.Status().Update(ctx, upgradePlanCopy); statusUpdateErr != nil {
			if apierrors.IsConflict(statusUpdateErr) {
				return ctrl.Result{Requeue: true}, nil
			}
			return ctrl.Result{}, statusUpdateErr
		}
	}

	return result, err
}

// SetupWithManager sets up the controller with the Manager.
func (r *UpgradePlanReconciler) SetupWithManager(mgr ctrl.Manager) error {
	handler := upgradeplan.NewUpgradePlanPhaseHandler(r.Client, r.Scheme, r.Log)
	r.upgradePlan = upgradeplan.NewUpgradePlan(handler)
	return ctrl.NewControllerManagedBy(mgr).
		For(&managementv1beta1.UpgradePlan{}).
		Owns(&appsv1.DaemonSet{}).
		Owns(&batchv1.Job{}).
		Owns(&corev1.PersistentVolumeClaim{}).
		Owns(&corev1.Service{}).
		Owns(&harvesterv1beta1.VirtualMachineImage{}).
		Owns(&upgradev1.Plan{}).
		Named("upgradeplan").
		Complete(r)
}

func setUpgradePlanPhaseTransitionTimestamp(oldUpgradePlan *managementv1beta1.UpgradePlan, newUpgradePlan *managementv1beta1.UpgradePlan) {
	if oldUpgradePlan.Status.Phase == newUpgradePlan.Status.Phase {
		for _, transitionTimestamp := range newUpgradePlan.Status.PhaseTransitionTimestamps {
			if transitionTimestamp.Phase == newUpgradePlan.Status.Phase {
				return
			}
		}
	}

	now := metav1.NewTime(time.Now())
	newUpgradePlan.Status.PhaseTransitionTimestamps = append(newUpgradePlan.Status.PhaseTransitionTimestamps, managementv1beta1.UpgradePlanPhaseTransitionTimestamp{
		Phase:                    newUpgradePlan.Status.Phase,
		PhaseTransitionTimestamp: now,
	})
}
