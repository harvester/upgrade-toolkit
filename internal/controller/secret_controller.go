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

	"github.com/go-logr/logr"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

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
	Scheme           *runtime.Scheme
	Log              logr.Logger
	NodeNameResolver NodeNameResolver
}

// +kubebuilder:rbac:groups=core,resources=secrets,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update
// +kubebuilder:rbac:groups=management.harvesterhci.io,resources=upgradeplans,verbs=get;list;watch
// +kubebuilder:rbac:groups=management.harvesterhci.io,resources=upgradeplans/status,verbs=get
// +kubebuilder:rbac:groups=cluster.x-k8s.io,resources=machines,verbs=get;list;watch

func (r *SecretReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	r.Log.V(2).Info("reconciling secret", "name", req.Name, "namespace", req.Namespace)

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

	// Resolve node name from machine-plan Secret
	nodeName, err := r.NodeNameResolver.ResolveNodeName(ctx, r.Client, &secret)
	if err != nil {
		r.Log.Error(err, "unable to resolve node name from machine-plan secret")
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
		Named("secret").
		Complete(r)
}

func (r *SecretReconciler) findActiveUpgradePlan(ctx context.Context) (*managementv1beta1.UpgradePlan, error) {
	var upList managementv1beta1.UpgradePlanList
	if err := r.List(ctx, &upList); err != nil {
		return nil, err
	}

	for i := range upList.Items {
		if upList.Items[i].Status.CurrentPhase == managementv1beta1.UpgradePlanPhaseNodeUpgrading {
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

	// Guardrail: Do not create the pre-drain Job until image-preload has completed
	if !exists || nodeStatus.State != managementv1beta1.NodeStateImagePreloaded {
		return nil
	}

	// Create or retrieve the pre-drain Job
	jobName := fmt.Sprintf(
		"%s-%s-%s-%s",
		up.Name, upgradeplan.NodeComponent, upgradeplan.DrainHookTypePreDrain, nodeName,
	)
	return r.ensureDrainJob(ctx, up, nodeName, jobName, upgradeplan.DrainHookTypePreDrain)
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
	jobName := fmt.Sprintf(
		"%s-%s-%s-%s",
		up.Name, upgradeplan.NodeComponent, upgradeplan.DrainHookTypePostDrain, nodeName,
	)
	return r.ensureDrainJob(ctx, up, nodeName, jobName, upgradeplan.DrainHookTypePostDrain)
}

func (r *SecretReconciler) ensureDrainJob(
	ctx context.Context,
	up *managementv1beta1.UpgradePlan,
	nodeName, jobName, hookType string,
) error {
	nn := types.NamespacedName{
		Namespace: upgradeplan.HarvesterSystemNamespace,
		Name:      jobName,
	}
	var existing batchv1.Job
	if err := r.Get(ctx, nn, &existing); err != nil {
		if apierrors.IsNotFound(err) {
			job := upgradeplan.ConstructDrainJob(up, nodeName, jobName, hookType)
			return r.Create(ctx, job)
		}
		return err
	}
	return nil
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
