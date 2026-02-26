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

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	managementv1beta1 "github.com/harvester/upgrade-toolkit/api/v1beta1"
	"github.com/harvester/upgrade-toolkit/pkg/upgradeplan"
)

// NodeReconciler reconciles a Node object to detect when a node has
// rebooted after an OS upgrade and its OSImage matches the expected version.
type NodeReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=core,resources=nodes,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=management.harvesterhci.io,resources=upgradeplans,verbs=get;list;watch
// +kubebuilder:rbac:groups=management.harvesterhci.io,resources=upgradeplans/status,verbs=get;update;patch

func (r *NodeReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	log.V(2).Info("reconciling node")

	var node corev1.Node
	if err := r.Get(ctx, req.NamespacedName, &node); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if !node.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	// Only process nodes that have the pending OS image annotation
	expectedOS, ok := node.Annotations[upgradeplan.PendingOSImageAnnotation]
	if !ok {
		return ctrl.Result{}, nil
	}

	// Check if the node's OS image matches the expected version
	if node.Status.NodeInfo.OSImage != expectedOS {
		return ctrl.Result{}, nil
	}

	// OS matches — the node has rebooted successfully.
	// Find the active UpgradePlan in NodeUpgrading phase.
	up, err := r.findActiveUpgradePlan(ctx)
	if err != nil {
		return ctrl.Result{}, err
	}
	if up == nil {
		return ctrl.Result{}, nil
	}

	// Verify the node is in WaitingReboot state
	nodeStatus, exists := up.Status.NodeUpgradeStatuses[node.Name]
	if !exists || nodeStatus.State != managementv1beta1.NodeStateWaitingReboot {
		return ctrl.Result{}, nil
	}

	// Transition to PostDrained
	upCopy := up.DeepCopy()
	upCopy.Status.NodeUpgradeStatuses[node.Name] = managementv1beta1.NodeUpgradeStatus{
		State: managementv1beta1.NodeStatePostDrained,
	}

	if !reflect.DeepEqual(up.Status, upCopy.Status) {
		if err := r.Status().Update(ctx, upCopy); err != nil {
			if apierrors.IsConflict(err) {
				return ctrl.Result{Requeue: true}, nil
			}
			return ctrl.Result{}, err
		}
	}

	// Remove the PendingOSImage annotation from the Node
	patch := client.MergeFrom(node.DeepCopy())
	delete(node.Annotations, upgradeplan.PendingOSImageAnnotation)
	if err := r.Patch(ctx, &node, patch); err != nil {
		return ctrl.Result{}, err
	}

	log.Info("node reboot verified, transitioned to PostDrained", "node", node.Name, "osImage", expectedOS)

	return ctrl.Result{}, nil
}

func (r *NodeReconciler) findActiveUpgradePlan(ctx context.Context) (*managementv1beta1.UpgradePlan, error) {
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

// SetupWithManager sets up the controller with the Manager.
func (r *NodeReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.Node{}).
		Named("node").
		Complete(r)
}
