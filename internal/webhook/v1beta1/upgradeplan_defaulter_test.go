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

package v1beta1

import (
	"context"

	harvesterv1beta1 "github.com/harvester/harvester/pkg/apis/harvesterhci.io/v1beta1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	managementv1beta1 "github.com/harvester/upgrade-toolkit/api/v1beta1"
)

var _ = Describe("UpgradePlan Defaulter", func() {
	var (
		scheme *runtime.Scheme
		node1  *corev1.Node
		node2  *corev1.Node
	)

	const harvesterDefault = `{"imagePreloadOption":{"strategy":{"type":"sequential"}},"nodeUpgradeOption":{"strategy":{"mode":"auto"}},"restoreVM": false, "logReadyTimeout": "5"}`

	BeforeEach(func() {
		scheme = runtime.NewScheme()
		Expect(managementv1beta1.AddToScheme(scheme)).To(Succeed())
		Expect(corev1.AddToScheme(scheme)).To(Succeed())
		Expect(harvesterv1beta1.AddToScheme(scheme)).To(Succeed())

		node1 = &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-1"}}
		node2 = &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-2"}}
	})

	upgradeConfig := func(value string) *harvesterv1beta1.Setting {
		return &harvesterv1beta1.Setting{
			ObjectMeta: metav1.ObjectMeta{Name: "upgrade-config"},
			Default:    harvesterDefault,
			Value:      value,
		}
	}

	requestContext := func(op admissionv1.Operation) context.Context {
		return admission.NewContextWithRequest(ctx, admission.Request{
			AdmissionRequest: admissionv1.AdmissionRequest{Operation: op},
		})
	}

	newDefaulter := func(objs ...client.Object) *UpgradePlanCustomDefaulter {
		return &UpgradePlanCustomDefaulter{
			Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build(),
		}
	}

	newUpgradePlan := func() *managementv1beta1.UpgradePlan {
		return &managementv1beta1.UpgradePlan{
			ObjectMeta: metav1.ObjectMeta{Name: "upgrade-1"},
			Spec:       managementv1beta1.UpgradePlanSpec{Version: ptr.To("v1.4.0")},
		}
	}

	It("should fill unset spec fields from the upgrade-config value on create", func() {
		defaulter := newDefaulter(node1, node2, upgradeConfig(
			`{"imagePreloadOption":{"strategy":{"type":"skip"}},"nodeUpgradeOption":{"strategy":{"mode":"manual"}},"restoreVM":true}`))
		obj := newUpgradePlan()

		Expect(defaulter.Default(requestContext(admissionv1.Create), obj)).To(Succeed())

		Expect(obj.Spec.ImagePreloadOption).NotTo(BeNil())
		Expect(obj.Spec.ImagePreloadOption.Concurrency).To(Equal(ptr.To(-1)))
		Expect(obj.Spec.NodeUpgradeOption).NotTo(BeNil())
		Expect(obj.Spec.NodeUpgradeOption.PauseNodes).To(Equal([]string{"node-1", "node-2"}))
		Expect(obj.Spec.RestoreVM).To(Equal(ptr.To(true)))
	})

	It("should fall back to the upgrade-config default when value is empty", func() {
		defaulter := newDefaulter(node1, upgradeConfig(""))
		obj := newUpgradePlan()

		Expect(defaulter.Default(requestContext(admissionv1.Create), obj)).To(Succeed())

		Expect(obj.Spec.ImagePreloadOption).NotTo(BeNil())
		Expect(obj.Spec.ImagePreloadOption.Concurrency).To(Equal(ptr.To(1)))
		Expect(obj.Spec.NodeUpgradeOption).To(BeNil())
		Expect(obj.Spec.RestoreVM).To(Equal(ptr.To(false)))
	})

	It("should keep explicitly specified spec fields", func() {
		defaulter := newDefaulter(node1, node2, upgradeConfig(
			`{"imagePreloadOption":{"strategy":{"type":"skip"}},"nodeUpgradeOption":{"strategy":{"mode":"manual"}},"restoreVM":true}`))
		obj := newUpgradePlan()
		obj.Spec.ImagePreloadOption = &managementv1beta1.ImagePreloadOption{Concurrency: ptr.To(2)}
		obj.Spec.NodeUpgradeOption = &managementv1beta1.NodeUpgradeOption{PauseNodes: []string{"node-2"}}
		obj.Spec.RestoreVM = ptr.To(false)
		expected := obj.DeepCopy()

		Expect(defaulter.Default(requestContext(admissionv1.Create), obj)).To(Succeed())
		Expect(obj).To(Equal(expected))
	})

	It("should still apply defaults when the skip-check annotation is set", func() {
		defaulter := newDefaulter(node1, upgradeConfig(`{"imagePreloadOption":{"strategy":{"type":"skip"}}}`))
		obj := newUpgradePlan()
		obj.Annotations = map[string]string{"management.harvesterhci.io/skip-check": "true"}

		Expect(defaulter.Default(requestContext(admissionv1.Create), obj)).To(Succeed())

		Expect(obj.Spec.ImagePreloadOption).NotTo(BeNil())
		Expect(obj.Spec.ImagePreloadOption.Concurrency).To(Equal(ptr.To(-1)))
	})

	It("should not modify the UpgradePlan on update", func() {
		defaulter := newDefaulter(node1, upgradeConfig(`{"imagePreloadOption":{"strategy":{"type":"skip"}},"restoreVM":true}`))
		obj := newUpgradePlan()
		expected := obj.DeepCopy()

		Expect(defaulter.Default(requestContext(admissionv1.Update), obj)).To(Succeed())
		Expect(obj).To(Equal(expected))
	})

	It("should not modify the UpgradePlan when the upgrade-config setting is missing", func() {
		defaulter := newDefaulter(node1)
		obj := newUpgradePlan()
		expected := obj.DeepCopy()

		Expect(defaulter.Default(requestContext(admissionv1.Create), obj)).To(Succeed())
		Expect(obj).To(Equal(expected))
	})

	It("should ignore a malformed upgrade-config setting", func() {
		defaulter := newDefaulter(node1, upgradeConfig(`{not-json`))
		obj := newUpgradePlan()
		expected := obj.DeepCopy()

		Expect(defaulter.Default(requestContext(admissionv1.Create), obj)).To(Succeed())
		Expect(obj).To(Equal(expected))
	})

	It("should ignore an upgrade-config setting with invalid values", func() {
		defaulter := newDefaulter(node1, upgradeConfig(`{"imagePreloadOption":{"strategy":{"type":"bogus"}},"restoreVM":true}`))
		obj := newUpgradePlan()
		expected := obj.DeepCopy()

		Expect(defaulter.Default(requestContext(admissionv1.Create), obj)).To(Succeed())
		Expect(obj).To(Equal(expected))
	})
})
