/*
Copyright 2026.

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

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	managementv1beta1 "github.com/harvester/upgrade-toolkit/api/v1beta1"
	"github.com/harvester/upgrade-toolkit/pkg/upgradelog"
)

// upgradeLogPipelineExecutor abstracts the upgrade log pipeline so that tests
// can inject lightweight fakes.
type upgradeLogPipelineExecutor interface {
	Execute(ctx context.Context, upgradeLog *managementv1beta1.UpgradeLog) (ctrl.Result, error)
}

// UpgradeLogReconciler reconciles a UpgradeLog object
type UpgradeLogReconciler struct {
	client.Client
	Scheme        *runtime.Scheme
	Log           logr.Logger
	EventRecorder record.EventRecorder

	pipeline upgradeLogPipelineExecutor
}

// +kubebuilder:rbac:groups=management.harvesterhci.io,resources=upgradelogs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=management.harvesterhci.io,resources=upgradelogs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=management.harvesterhci.io,resources=upgradelogs/finalizers,verbs=update
// +kubebuilder:rbac:groups=management.harvesterhci.io,resources=upgradeplans,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=persistentvolumeclaims,verbs=get;list;watch;create;delete
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=services,verbs=get;list;watch;create;update;patch;delete

func (r *UpgradeLogReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := r.Log.WithValues("upgradeLog", req.Name)
	ctx = logr.NewContext(ctx, log)

	var upgradeLog managementv1beta1.UpgradeLog
	if err := r.Get(ctx, req.NamespacedName, &upgradeLog); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("fetching UpgradeLog: %w", err)
	}

	upgradeLogCopy := upgradeLog.DeepCopy()

	if r.pipeline == nil {
		return ctrl.Result{}, fmt.Errorf("pipeline not initialized; call SetupWithManager first")
	}

	result, err := r.pipeline.Execute(ctx, upgradeLogCopy)

	// Update status if changed
	if statusErr := r.Status().Update(ctx, upgradeLogCopy); statusErr != nil {
		if apierrors.IsConflict(statusErr) {
			return ctrl.Result{RequeueAfter: upgradelog.RequeueAfterDuration}, nil
		}
		return ctrl.Result{}, fmt.Errorf("updating UpgradeLog status: %w", statusErr)
	}

	if err != nil {
		log.Error(err, "pipeline execution error")
	}

	return result, err
}

// SetupWithManager sets up the controller with the Manager.
func (r *UpgradeLogReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.EventRecorder == nil {
		r.EventRecorder = mgr.GetEventRecorderFor("upgradelog-controller")
	}

	deps := &upgradelog.PhaseDeps{
		Client:        r.Client,
		Scheme:        r.Scheme,
		Log:           r.Log,
		EventRecorder: r.EventRecorder,
	}
	r.pipeline = upgradelog.NewPipeline(deps)

	return ctrl.NewControllerManagedBy(mgr).
		For(&managementv1beta1.UpgradeLog{}).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.PersistentVolumeClaim{}).
		Watches(
			&managementv1beta1.UpgradePlan{},
			handler.EnqueueRequestsFromMapFunc(mapUpgradePlanToUpgradeLog),
			builder.WithPredicates(upgradePlanTerminalPredicate{}),
		).
		Named("upgradelog").
		Complete(r)
}

func mapUpgradePlanToUpgradeLog(_ context.Context, obj client.Object) []reconcile.Request {
	return []reconcile.Request{
		{NamespacedName: types.NamespacedName{Name: obj.GetName()}},
	}
}

type upgradePlanTerminalPredicate struct {
	predicate.Funcs
}

func (upgradePlanTerminalPredicate) Create(_ event.CreateEvent) bool   { return false }
func (upgradePlanTerminalPredicate) Delete(_ event.DeleteEvent) bool   { return true }
func (upgradePlanTerminalPredicate) Generic(_ event.GenericEvent) bool { return false }

func (upgradePlanTerminalPredicate) Update(e event.UpdateEvent) bool {
	newPlan, ok := e.ObjectNew.(*managementv1beta1.UpgradePlan)
	if !ok {
		return false
	}
	return isUpgradePlanTerminal(newPlan.Status.CurrentPhase)
}

func isUpgradePlanTerminal(phase managementv1beta1.UpgradePlanPhase) bool {
	return phase == managementv1beta1.UpgradePlanPhaseSucceeded ||
		phase == managementv1beta1.UpgradePlanPhaseFailed
}
