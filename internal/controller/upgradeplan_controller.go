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
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	managementv1beta1 "github.com/harvester/upgrade-toolkit/api/v1beta1"
	"github.com/harvester/upgrade-toolkit/pkg/upgradeplan"
)

// UpgradePlanReconciler reconciles a UpgradePlan object
type UpgradePlanReconciler struct {
	client.Client
	Scheme             *runtime.Scheme
	Log                logr.Logger
	EventRecorder      record.EventRecorder
	JobServiceAccount  string
	PlanServiceAccount string
	pipeline           *upgradeplan.Pipeline
}

// +kubebuilder:rbac:groups=management.harvesterhci.io,resources=upgradeplans,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=management.harvesterhci.io,resources=upgradeplans/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=management.harvesterhci.io,resources=upgradeplans/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete;deletecollection
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;deletecollection
// +kubebuilder:rbac:groups=core,resources=nodes,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=nodes/proxy;nodes/stats,verbs=get
// +kubebuilder:rbac:groups=core,resources=services,verbs=get;list;watch;create;update;patch;delete;deletecollection
// +kubebuilder:rbac:groups=discovery.k8s.io,resources=endpointslices,verbs=get;list;watch
// +kubebuilder:rbac:groups=harvesterhci.io,resources=settings,verbs=get;list;watch
// +kubebuilder:rbac:groups=harvesterhci.io,resources=virtualmachineimages,verbs=get;list;watch;create;update;patch;delete;deletecollection
// +kubebuilder:rbac:groups=upgrade.cattle.io,resources=plans,verbs=get;list;watch;create;update;deletecollection
// +kubebuilder:rbac:groups=longhorn.io,resources=volumes,verbs=get;list;watch
// +kubebuilder:rbac:groups=longhorn.io,resources=settings,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=provisioning.cattle.io,resources=clusters,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=harvesterhci.io,resources=virtualmachinebackups,verbs=get;list;watch
// +kubebuilder:rbac:groups=harvesterhci.io,resources=schedulevmbackups,verbs=get;list;watch
// +kubebuilder:rbac:groups=harvesterhci.io,resources=addons,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=kubevirt.io,resources=virtualmachineinstances,verbs=get;list;watch
// +kubebuilder:rbac:groups=management.cattle.io,resources=managedcharts,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=events,verbs=create;patch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.21.0/pkg/reconcile
func (r *UpgradePlanReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	r.Log.V(2).Info("reconciling upgradeplan")

	var upgradePlan managementv1beta1.UpgradePlan
	if err := r.Get(ctx, req.NamespacedName, &upgradePlan); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Handle deletion
	if !upgradePlan.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&upgradePlan, upgradeplan.UpgradePlanFinalizer) {
			r.Log.V(1).Info("upgradeplan under deletion, running cleanup")
			if err := upgradeplan.CleanupUpgradeResources(ctx, r.Client, r.Log, &upgradePlan); err != nil {
				return ctrl.Result{}, err
			}
			controllerutil.RemoveFinalizer(&upgradePlan, upgradeplan.UpgradePlanFinalizer)
			if err := r.Update(ctx, &upgradePlan); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}

	// Ensure finalizer is present
	if !controllerutil.ContainsFinalizer(&upgradePlan, upgradeplan.UpgradePlanFinalizer) {
		controllerutil.AddFinalizer(&upgradePlan, upgradeplan.UpgradePlanFinalizer)
		if err := r.Update(ctx, &upgradePlan); err != nil {
			return ctrl.Result{}, err
		}
	}

	upgradePlanCopy := upgradePlan.DeepCopy()

	// Unavailable UpgradePlans are ignored
	if upgradePlan.ConditionFalse(managementv1beta1.UpgradePlanAvailable) {
		return ctrl.Result{}, nil
	}

	// Concurrent upgrade prevention
	conflicting, err := upgradeplan.FindConflictingUpgrade(ctx, r.Client, upgradePlan.Name)
	if err != nil {
		return ctrl.Result{}, err
	}
	if conflicting != "" {
		r.Log.Info("blocking concurrent upgrade",
			"upgradePlan", upgradePlan.Name,
			"conflicting", conflicting)
		r.EventRecorder.Eventf(&upgradePlan, corev1.EventTypeWarning, "ConcurrentUpgradeBlocked",
			"Blocked by in-progress upgrade %q", conflicting)
		upgradePlanCopy.SetCondition(
			managementv1beta1.UpgradePlanAvailable,
			metav1.ConditionFalse,
			"ConcurrentUpgradeBlocked",
			fmt.Sprintf("another upgrade %q is in progress", conflicting),
		)
		if statusUpdateErr := r.Status().Update(ctx, upgradePlanCopy); statusUpdateErr != nil {
			return ctrl.Result{}, statusUpdateErr
		}
		return ctrl.Result{}, nil
	}

	result, err := r.pipeline.Execute(ctx, upgradePlanCopy)
	if err != nil {
		upgradePlanCopy.SetCondition(managementv1beta1.UpgradePlanDegraded, metav1.ConditionTrue, "ReconcileError", err.Error())
		r.EventRecorder.Eventf(&upgradePlan, corev1.EventTypeWarning, "ReconcileError", "Pipeline error: %v", err)
	} else {
		upgradePlanCopy.SetCondition(managementv1beta1.UpgradePlanDegraded, metav1.ConditionFalse, "ReconcileSuccess", "")
	}

	setUpgradePlanPhaseTransitionTimestamp(&upgradePlan, upgradePlanCopy)

	if upgradePlanCopy.Status.CurrentPhase == managementv1beta1.UpgradePlanPhaseSucceeded &&
		upgradePlan.Status.CurrentPhase != managementv1beta1.UpgradePlanPhaseSucceeded {
		r.EventRecorder.Event(&upgradePlan, corev1.EventTypeNormal, "UpgradeSucceeded", "Upgrade completed successfully")
	} else if upgradePlanCopy.Status.CurrentPhase == managementv1beta1.UpgradePlanPhaseFailed &&
		upgradePlan.Status.CurrentPhase != managementv1beta1.UpgradePlanPhaseFailed {
		cond := upgradePlanCopy.LookupCondition(managementv1beta1.UpgradePlanProgressing)
		msg := "Upgrade failed"
		if cond.Message != "" {
			msg = fmt.Sprintf("Upgrade failed: %s", cond.Message)
		}
		r.EventRecorder.Event(&upgradePlan, corev1.EventTypeWarning, "UpgradeFailed", msg)
	}

	if !reflect.DeepEqual(upgradePlan.Status, upgradePlanCopy.Status) {
		if statusUpdateErr := r.Status().Update(ctx, upgradePlanCopy); statusUpdateErr != nil {
			if apierrors.IsConflict(statusUpdateErr) {
				return ctrl.Result{RequeueAfter: upgradeplan.RequeueAfterDuration}, nil
			}
			return ctrl.Result{}, statusUpdateErr
		}
	}

	return result, err
}

// SetupWithManager sets up the controller with the Manager.
func (r *UpgradePlanReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.EventRecorder == nil {
		r.EventRecorder = mgr.GetEventRecorderFor("upgradeplan-controller")
	}

	deps := &upgradeplan.PhaseDeps{
		Client:             r.Client,
		Scheme:             r.Scheme,
		Log:                r.Log,
		EventRecorder:      r.EventRecorder,
		JobServiceAccount:  r.JobServiceAccount,
		PlanServiceAccount: r.PlanServiceAccount,
	}
	r.pipeline = upgradeplan.NewPipeline(deps)
	return ctrl.NewControllerManagedBy(mgr).
		For(&managementv1beta1.UpgradePlan{}).
		Owns(&appsv1.Deployment{}).
		Owns(&batchv1.Job{}).
		Owns(&corev1.Service{}).
		Owns(&harvesterv1beta1.VirtualMachineImage{}).
		Owns(&upgradev1.Plan{}).
		Named("upgradeplan").
		Complete(r)
}

func setUpgradePlanPhaseTransitionTimestamp(oldUpgradePlan *managementv1beta1.UpgradePlan, newUpgradePlan *managementv1beta1.UpgradePlan) {
	if oldUpgradePlan.Status.CurrentPhase == newUpgradePlan.Status.CurrentPhase {
		for _, transitionTimestamp := range newUpgradePlan.Status.PhaseTransitionTimestamps {
			if transitionTimestamp.Phase == newUpgradePlan.Status.CurrentPhase {
				return
			}
		}
	}

	now := metav1.NewTime(time.Now())
	newUpgradePlan.Status.PhaseTransitionTimestamps = append(newUpgradePlan.Status.PhaseTransitionTimestamps, managementv1beta1.UpgradePlanPhaseTransitionTimestamp{
		Phase:                    newUpgradePlan.Status.CurrentPhase,
		PhaseTransitionTimestamp: now,
	})
}
