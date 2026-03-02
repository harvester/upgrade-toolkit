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

	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	managementv1beta1 "github.com/harvester/upgrade-toolkit/api/v1beta1"
)

// nolint:unused
// log is for logging in this package.
var upgradeplanlog = logf.Log.WithName("upgradeplan-resource")

// SetupUpgradePlanWebhookWithManager registers the webhook for UpgradePlan in the manager.
func SetupUpgradePlanWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr, &managementv1beta1.UpgradePlan{}).
		WithValidator(&UpgradePlanCustomValidator{}).
		WithDefaulter(&UpgradePlanCustomDefaulter{}).
		Complete()
}

// TODO(user): EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!

// +kubebuilder:webhook:path=/mutate-management-harvesterhci-io-v1beta1-upgradeplan,mutating=true,failurePolicy=fail,sideEffects=None,groups=management.harvesterhci.io,resources=upgradeplans,verbs=create;update,versions=v1beta1,name=mupgradeplan-v1beta1.kb.io,admissionReviewVersions=v1

// UpgradePlanCustomDefaulter struct is responsible for setting default values on the custom resource of the
// Kind UpgradePlan when those are created or updated.
//
// NOTE: The +kubebuilder:object:generate=false marker prevents controller-gen from generating DeepCopy methods,
// as it is used only for temporary operations and does not need to be deeply copied.
type UpgradePlanCustomDefaulter struct {
	// TODO(user): Add more fields as needed for defaulting
}

// Default implements webhook.CustomDefaulter so a webhook will be registered for the Kind UpgradePlan.
func (d *UpgradePlanCustomDefaulter) Default(_ context.Context, obj *managementv1beta1.UpgradePlan) error {
	upgradeplanlog.Info("Defaulting for UpgradePlan", "name", obj.GetName())

	// TODO(user): fill in your defaulting logic.

	return nil
}

// TODO(user): change verbs to "verbs=create;update;delete" if you want to enable deletion validation.
// NOTE: If you want to customise the 'path', use the flags '--defaulting-path' or '--validation-path'.
// +kubebuilder:webhook:path=/validate-management-harvesterhci-io-v1beta1-upgradeplan,mutating=false,failurePolicy=fail,sideEffects=None,groups=management.harvesterhci.io,resources=upgradeplans,verbs=create;update,versions=v1beta1,name=vupgradeplan-v1beta1.kb.io,admissionReviewVersions=v1

// UpgradePlanCustomValidator struct is responsible for validating the UpgradePlan resource
// when it is created, updated, or deleted.
//
// NOTE: The +kubebuilder:object:generate=false marker prevents controller-gen from generating DeepCopy methods,
// as this struct is used only for temporary operations and does not need to be deeply copied.
type UpgradePlanCustomValidator struct {
	// TODO(user): Add more fields as needed for validation
}

// ValidateCreate implements webhook.CustomValidator so a webhook will be registered for the type UpgradePlan.
func (v *UpgradePlanCustomValidator) ValidateCreate(_ context.Context, obj *managementv1beta1.UpgradePlan) (admission.Warnings, error) {
	upgradeplanlog.Info("Validation for UpgradePlan upon creation", "name", obj.GetName())

	// TODO(user): fill in your validation logic upon object creation.

	return nil, nil
}

// ValidateUpdate implements webhook.CustomValidator so a webhook will be registered for the type UpgradePlan.
func (v *UpgradePlanCustomValidator) ValidateUpdate(_ context.Context, oldObj, newObj *managementv1beta1.UpgradePlan) (admission.Warnings, error) {
	upgradeplanlog.Info("Validation for UpgradePlan upon update", "name", newObj.GetName())

	// TODO(user): fill in your validation logic upon object update.

	return nil, nil
}

// ValidateDelete implements webhook.CustomValidator so a webhook will be registered for the type UpgradePlan.
func (v *UpgradePlanCustomValidator) ValidateDelete(_ context.Context, obj *managementv1beta1.UpgradePlan) (admission.Warnings, error) {
	upgradeplanlog.Info("Validation for UpgradePlan upon deletion", "name", obj.GetName())

	// TODO(user): fill in your validation logic upon object deletion.

	return nil, nil
}
