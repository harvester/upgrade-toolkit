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
	"github.com/rancher/wrangler/v3/pkg/name"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	managementv1beta1 "github.com/harvester/upgrade-toolkit/api/v1beta1"
	"github.com/harvester/upgrade-toolkit/pkg/upgradeplan"
)

// NodeNameResolver resolves a node name from a machine-plan Secret.
type NodeNameResolver interface {
	ResolveNodeName(ctx context.Context, c client.Client, secret *corev1.Secret) (string, error)
}

// MachineNodeNameResolver resolves node names by looking up CAPI Machine resources.
type MachineNodeNameResolver struct{}

func (r *MachineNodeNameResolver) ResolveNodeName(ctx context.Context, c client.Client, secret *corev1.Secret) (string, error) {
	machineName, ok := secret.Labels[upgradeplan.MachinePlanMachineLabel]
	if !ok || machineName == "" {
		return "", nil
	}

	machine := &unstructured.Unstructured{}
	machine.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "cluster.x-k8s.io",
		Version: "v1beta1",
		Kind:    "Machine",
	})

	if err := c.Get(ctx, types.NamespacedName{
		Namespace: secret.Namespace,
		Name:      machineName,
	}, machine); err != nil {
		return "", fmt.Errorf("failed to get machine %s: %w", machineName, err)
	}

	nodeName, found, err := unstructured.NestedString(machine.Object, "status", "nodeRef", "name")
	if err != nil || !found {
		return "", fmt.Errorf("machine %s has no status.nodeRef.name", machineName)
	}

	return nodeName, nil
}

// SecretReconciler reconciles machine-plan Secrets to orchestrate
// pre-drain and post-drain Jobs during node upgrades.
type SecretReconciler struct {
	client.Client
	Scheme            *runtime.Scheme
	Log               logr.Logger
	NodeNameResolver  NodeNameResolver
	JobServiceAccount string
}

// +kubebuilder:rbac:groups=core,resources=secrets,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=management.harvesterhci.io,resources=upgradeplans,verbs=get;list;watch
// +kubebuilder:rbac:groups=management.harvesterhci.io,resources=upgradeplans/status,verbs=get
// +kubebuilder:rbac:groups=cluster.x-k8s.io,resources=machines,verbs=get;list;watch

func (r *SecretReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	log.V(2).Info("reconciling secret", "name", req.Name, "namespace", req.Namespace)

	var secret corev1.Secret
	if err := r.Get(ctx, req.NamespacedName, &secret); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if secret.Type != corev1.SecretType(upgradeplan.MachinePlanSecretType) {
		return ctrl.Result{}, nil
	}

	if secret.Namespace != upgradeplan.FleetLocalNamespace {
		return ctrl.Result{}, nil
	}

	if !secret.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	// Find the active UpgradePlan in NodeUpgrading phase
	up, err := r.findActiveUpgradePlan(ctx)
	if err != nil {
		return ctrl.Result{}, err
	}
	if up == nil {
		return ctrl.Result{}, nil
	}

	// Single-node clusters don't use the drain hook mechanism
	if up.Status.SingleNode != nil {
		return ctrl.Result{}, nil
	}

	// Resolve node name from machine-plan Secret
	nodeName, err := r.NodeNameResolver.ResolveNodeName(ctx, r.Client, &secret)
	if err != nil {
		log.Error(err, "unable to resolve node name from machine-plan secret")
		return ctrl.Result{}, err
	}
	if nodeName == "" {
		return ctrl.Result{}, nil
	}

	// Handle pre-drain hook
	if err := r.handlePreDrain(ctx, &secret, up, nodeName); err != nil {
		return ctrl.Result{}, err
	}

	// Handle post-drain hook
	if err := r.handlePostDrain(ctx, &secret, up, nodeName); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *SecretReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.Secret{}).
		Owns(&batchv1.Job{}).
		Watches(
			&managementv1beta1.UpgradePlan{},
			handler.EnqueueRequestsFromMapFunc(r.mapUpgradePlanToSecrets),
			builder.WithPredicates(upgradePlanNodeStatusChangedPredicate{}),
		).
		Named("secret").
		Complete(r)
}

// mapUpgradePlanToSecrets returns reconcile requests for all machine-plan
// Secrets in fleet-local when an UpgradePlan in NodeUpgrading phase changes.
func (r *SecretReconciler) mapUpgradePlanToSecrets(ctx context.Context, obj client.Object) []reconcile.Request {
	up, ok := obj.(*managementv1beta1.UpgradePlan)
	if !ok {
		return nil
	}

	if up.Status.CurrentPhase != managementv1beta1.UpgradePlanPhaseNodeUpgrading &&
		up.Status.CurrentPhase != managementv1beta1.UpgradePlanPhaseNodeUpgraded {
		return nil
	}

	var secretList corev1.SecretList
	if err := r.List(ctx, &secretList, client.InNamespace(upgradeplan.FleetLocalNamespace)); err != nil {
		r.Log.Error(err, "failed to list secrets for UpgradePlan watch")
		return nil
	}

	var requests []reconcile.Request
	for i := range secretList.Items {
		if secretList.Items[i].Type != corev1.SecretType(upgradeplan.MachinePlanSecretType) {
			continue
		}
		requests = append(requests, reconcile.Request{
			NamespacedName: types.NamespacedName{
				Namespace: secretList.Items[i].Namespace,
				Name:      secretList.Items[i].Name,
			},
		})
	}

	return requests
}

// upgradePlanNodeStatusChangedPredicate filters UpgradePlan events to only
// those where NodeUpgradeStatuses changed while in NodeUpgrading phase.
type upgradePlanNodeStatusChangedPredicate struct {
	predicate.Funcs
}

func (upgradePlanNodeStatusChangedPredicate) Create(_ event.CreateEvent) bool {
	return false
}

func (upgradePlanNodeStatusChangedPredicate) Update(e event.UpdateEvent) bool {
	newUP, ok := e.ObjectNew.(*managementv1beta1.UpgradePlan)
	if !ok {
		return false
	}

	oldUP, ok := e.ObjectOld.(*managementv1beta1.UpgradePlan)
	if !ok {
		return false
	}

	// Trigger when the phase transitions from NodeUpgrading to NodeUpgraded,
	// so the secret controller can annotate any remaining machine-plan secrets.
	if oldUP.Status.CurrentPhase == managementv1beta1.UpgradePlanPhaseNodeUpgrading &&
		newUP.Status.CurrentPhase == managementv1beta1.UpgradePlanPhaseNodeUpgraded {
		return true
	}

	// Filter out not of interest phases
	if newUP.Status.CurrentPhase != managementv1beta1.UpgradePlanPhaseNodeUpgrading {
		return false
	}

	// Trigger when spec.nodeUpgradeOption changes (user pauses/unpauses nodes)
	if !reflect.DeepEqual(oldUP.Spec.NodeUpgradeOption, newUP.Spec.NodeUpgradeOption) {
		return true
	}

	return !reflect.DeepEqual(oldUP.Status.NodeUpgradeStatuses, newUP.Status.NodeUpgradeStatuses)
}

func (upgradePlanNodeStatusChangedPredicate) Delete(_ event.DeleteEvent) bool {
	return false
}

func (upgradePlanNodeStatusChangedPredicate) Generic(_ event.GenericEvent) bool {
	return false
}

func (r *SecretReconciler) findActiveUpgradePlan(ctx context.Context) (*managementv1beta1.UpgradePlan, error) {
	var upList managementv1beta1.UpgradePlanList
	if err := r.List(ctx, &upList); err != nil {
		return nil, err
	}

	for i := range upList.Items {
		phase := upList.Items[i].Status.CurrentPhase
		if phase == managementv1beta1.UpgradePlanPhaseNodeUpgrading ||
			phase == managementv1beta1.UpgradePlanPhaseNodeUpgraded {
			return &upList.Items[i], nil
		}
	}

	return nil, nil
}

func (r *SecretReconciler) handlePreDrain(
	ctx context.Context,
	secret *corev1.Secret,
	up *managementv1beta1.UpgradePlan,
	nodeName string,
) error {
	rke2Value := secret.Annotations[upgradeplan.RKE2PreDrainAnnotation]
	ourValue := secret.Annotations[upgradeplan.PreHookAnnotation]

	if rke2Value == "" || rke2Value == ourValue {
		return nil
	}

	// Check if the node has already completed pre-drain
	nodeStatus, exists := up.Status.NodeUpgradeStatuses[nodeName]
	if exists && nodeStatus.State == managementv1beta1.NodeStatePreDrained {
		// Pre-drain Job completed; signal Rancher by copying the annotation
		return r.annotateSecret(ctx, secret, upgradeplan.PreHookAnnotation, rke2Value)
	}

	// Guardrail: Do not create the pre-drain Job until image-preload has completed.
	// Also accept UpgradePaused state since the Job may already exist (suspended).
	if !exists || (nodeStatus.State != managementv1beta1.NodeStateImagePreloaded &&
		nodeStatus.State != managementv1beta1.NodeStateUpgradePaused) {
		return nil
	}

	shouldPause := upgradeplan.ShouldPauseNode(up, nodeName)
	jobName := name.SafeConcatName(up.Name, upgradeplan.NodeComponent, upgradeplan.JobTypePreDrain, nodeName)

	// Create the pre-drain Job (or reconcile its suspend state if it already exists)
	return r.ensureNodeJob(ctx, up, nodeName, jobName, upgradeplan.JobTypePreDrain, shouldPause)
}

func (r *SecretReconciler) handlePostDrain(
	ctx context.Context,
	secret *corev1.Secret,
	up *managementv1beta1.UpgradePlan,
	nodeName string,
) error {
	rke2Value := secret.Annotations[upgradeplan.RKE2PostDrainAnnotation]
	ourValue := secret.Annotations[upgradeplan.PostHookAnnotation]

	if rke2Value == "" || rke2Value == ourValue {
		return nil
	}

	nodeStatus, exists := up.Status.NodeUpgradeStatuses[nodeName]

	// Check if the node has already completed post-drain
	if exists && nodeStatus.State == managementv1beta1.NodeStatePostDrained {
		// Post-drain Job completed; signal Rancher
		return r.annotateSecret(ctx, secret, upgradeplan.PostHookAnnotation, rke2Value)
	}

	// Guardrail: Do not create the post-drain Job until pre-drain has completed
	if !exists || nodeStatus.State != managementv1beta1.NodeStatePreDrained {
		return nil
	}

	// Create or retrieve the post-drain Job
	jobName := name.SafeConcatName(up.Name, upgradeplan.NodeComponent, upgradeplan.JobTypePostDrain, nodeName)
	return r.ensureNodeJob(ctx, up, nodeName, jobName, upgradeplan.JobTypePostDrain, false)
}

func (r *SecretReconciler) ensureNodeJob(
	ctx context.Context,
	up *managementv1beta1.UpgradePlan,
	nodeName, jobName, jobType string,
	suspend bool,
) error {
	nn := types.NamespacedName{
		Namespace: upgradeplan.HarvesterSystemNamespace,
		Name:      jobName,
	}
	existing, err := upgradeplan.GetOrCreate(
		ctx, r.Client, r.Scheme, nn,
		func() *batchv1.Job { return &batchv1.Job{} },
		func() *batchv1.Job {
			return upgradeplan.ConstructNodeJob(up, nodeName, jobName, jobType, r.JobServiceAccount, suspend)
		},
		up,
	)
	if err != nil {
		return err
	}
	return upgradeplan.ReconcileJobSuspend(ctx, r.Client, existing, suspend)
}

func (r *SecretReconciler) annotateSecret(
	ctx context.Context,
	secret *corev1.Secret,
	key, value string,
) error {
	patch := client.MergeFrom(secret.DeepCopy())
	if secret.Annotations == nil {
		secret.Annotations = make(map[string]string)
	}
	secret.Annotations[key] = value
	return r.Patch(ctx, secret, patch)
}
