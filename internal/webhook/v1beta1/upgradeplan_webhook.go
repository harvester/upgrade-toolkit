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

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	managementv1beta1 "github.com/harvester/upgrade-toolkit/api/v1beta1"
)

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
	var upgradePlanList managementv1beta1.UpgradePlanList
	if err := v.Client.List(ctx, &upgradePlanList); err != nil {
		allErrs = append(allErrs, field.InternalError(
			field.NewPath(""), fmt.Errorf("failed to list UpgradePlans: %w", err)))
	} else {
		for _, existing := range upgradePlanList.Items {
			if existing.Name == upgradePlan.Name {
				continue
			}
			if isProgressing(&existing) {
				allErrs = append(allErrs, field.Forbidden(
					field.NewPath("spec"),
					fmt.Sprintf("another upgrade %q is in progress", existing.Name)))
				break
			}
		}
	}

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
func (v *UpgradePlanCustomValidator) ValidateDelete(_ context.Context, obj runtime.Object) (admission.Warnings, error) {
	upgradePlan, ok := obj.(*managementv1beta1.UpgradePlan)
	if !ok {
		return nil, fmt.Errorf("expected an UpgradePlan object but got %T", obj)
	}
	upgradeplanlog.Info("Validation for UpgradePlan upon deletion", "name", upgradePlan.GetName())

	if isProgressing(upgradePlan) {
		return nil, apierrors.NewInvalid(
			managementv1beta1.GroupVersion.WithKind("UpgradePlan").GroupKind(),
			upgradePlan.Name,
			field.ErrorList{
				field.Forbidden(
					field.NewPath("metadata", "name"),
					"cannot delete UpgradePlan while Progressing condition is True"),
			})
	}
	return nil, nil
}

// isProgressing returns true if the UpgradePlan's Progressing condition is True.
func isProgressing(upgradePlan *managementv1beta1.UpgradePlan) bool {
	cond := upgradePlan.LookupCondition(managementv1beta1.UpgradePlanProgressing)
	return cond.Status == metav1.ConditionTrue
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
