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
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	managementv1beta1 "github.com/harvester/upgrade-toolkit/api/v1beta1"
	"github.com/harvester/upgrade-toolkit/pkg/upgradeplan"
)

// These tests exercise the CEL validation rules defined on the UpgradePlan CRD
// schema. They go through the real API server (envtest) to verify that the rules
// are correctly generated and enforced.
var _ = Describe("UpgradePlan CEL Validation", Ordered, func() {
	var version *managementv1beta1.Version

	BeforeAll(func() {
		version = &managementv1beta1.Version{
			ObjectMeta: metav1.ObjectMeta{Name: "v1.4.0"},
			Spec: managementv1beta1.VersionSpec{
				ISODownloadURL: "https://example.com/harvester-v1.4.0.iso",
			},
		}
		Expect(k8sClient.Create(ctx, version)).To(Succeed())
	})

	AfterAll(func() {
		Expect(k8sClient.Delete(ctx, version)).To(Succeed())
	})

	Context("spec.version immutability", func() {
		It("should reject updates that change spec.version", func() {
			plan := &managementv1beta1.UpgradePlan{
				ObjectMeta: metav1.ObjectMeta{
					Name: "cel-test-version-immutable",
					Annotations: map[string]string{
						upgradeplan.AnnotationSkipWebhook: "true",
					},
				},
				Spec: managementv1beta1.UpgradePlanSpec{
					Version: ptr.To("v1.4.0"),
				},
			}
			Expect(k8sClient.Create(ctx, plan)).To(Succeed())
			defer func() {
				Expect(k8sClient.Delete(ctx, plan)).To(Succeed())
			}()

			plan.Spec.Version = ptr.To("v1.5.0")
			err := k8sClient.Update(ctx, plan)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("spec.version is immutable after creation"))
		})
	})

	Context("spec.upgrade immutability", func() {
		It("should reject updates that change spec.upgrade from a value to a different value", func() {
			plan := &managementv1beta1.UpgradePlan{
				ObjectMeta: metav1.ObjectMeta{
					Name: "cel-test-upgrade-immutable",
					Annotations: map[string]string{
						upgradeplan.AnnotationSkipWebhook: "true",
					},
				},
				Spec: managementv1beta1.UpgradePlanSpec{
					Version: ptr.To("v1.4.0"),
					Upgrade: ptr.To("custom-image"),
				},
			}
			Expect(k8sClient.Create(ctx, plan)).To(Succeed())
			defer func() {
				Expect(k8sClient.Delete(ctx, plan)).To(Succeed())
			}()

			plan.Spec.Upgrade = ptr.To("different-image")
			err := k8sClient.Update(ctx, plan)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("spec.upgrade is immutable after creation"))
		})

		It("should reject updates that set spec.upgrade from nil to a value", func() {
			plan := &managementv1beta1.UpgradePlan{
				ObjectMeta: metav1.ObjectMeta{
					Name: "cel-test-upgrade-nil-to-value",
					Annotations: map[string]string{
						upgradeplan.AnnotationSkipWebhook: "true",
					},
				},
				Spec: managementv1beta1.UpgradePlanSpec{
					Version: ptr.To("v1.4.0"),
				},
			}
			Expect(k8sClient.Create(ctx, plan)).To(Succeed())
			defer func() {
				Expect(k8sClient.Delete(ctx, plan)).To(Succeed())
			}()

			plan.Spec.Upgrade = ptr.To("new-image")
			err := k8sClient.Update(ctx, plan)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("spec.upgrade is immutable after creation"))
		})

		It("should reject updates that clear spec.upgrade from a value to nil", func() {
			plan := &managementv1beta1.UpgradePlan{
				ObjectMeta: metav1.ObjectMeta{
					Name: "cel-test-upgrade-value-to-nil",
					Annotations: map[string]string{
						upgradeplan.AnnotationSkipWebhook: "true",
					},
				},
				Spec: managementv1beta1.UpgradePlanSpec{
					Version: ptr.To("v1.4.0"),
					Upgrade: ptr.To("custom-image"),
				},
			}
			Expect(k8sClient.Create(ctx, plan)).To(Succeed())
			defer func() {
				Expect(k8sClient.Delete(ctx, plan)).To(Succeed())
			}()

			plan.Spec.Upgrade = nil
			err := k8sClient.Update(ctx, plan)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("spec.upgrade is immutable after creation"))
		})
	})

	Context("spec.version MinLength", func() {
		It("should reject creation with empty spec.version", func() {
			plan := &managementv1beta1.UpgradePlan{
				ObjectMeta: metav1.ObjectMeta{
					Name: "cel-test-empty-version",
					Annotations: map[string]string{
						upgradeplan.AnnotationSkipWebhook: "true",
					},
				},
				Spec: managementv1beta1.UpgradePlanSpec{
					Version: ptr.To(""),
				},
			}
			err := k8sClient.Create(ctx, plan)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("spec.version"))
		})
	})

	Context("pauseNodes items MinLength", func() {
		It("should reject creation with empty string in pauseNodes", func() {
			plan := &managementv1beta1.UpgradePlan{
				ObjectMeta: metav1.ObjectMeta{
					Name: "cel-test-empty-pausenode",
					Annotations: map[string]string{
						upgradeplan.AnnotationSkipWebhook: "true",
					},
				},
				Spec: managementv1beta1.UpgradePlanSpec{
					Version: ptr.To("v1.4.0"),
					NodeUpgradeOption: &managementv1beta1.NodeUpgradeOption{
						PauseNodes: []string{""},
					},
				},
			}
			err := k8sClient.Create(ctx, plan)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("pauseNodes"))
		})
	})

	Context("mutable fields", func() {
		It("should allow updates to spec.force without affecting immutable fields", func() {
			plan := &managementv1beta1.UpgradePlan{
				ObjectMeta: metav1.ObjectMeta{
					Name: "cel-test-mutable-force",
					Annotations: map[string]string{
						upgradeplan.AnnotationSkipWebhook: "true",
					},
				},
				Spec: managementv1beta1.UpgradePlanSpec{
					Version: ptr.To("v1.4.0"),
				},
			}
			Expect(k8sClient.Create(ctx, plan)).To(Succeed())
			defer func() {
				Expect(k8sClient.Delete(ctx, plan)).To(Succeed())
			}()

			// Re-fetch to get the latest resource version
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(plan), plan)).To(Succeed())
			plan.Spec.Force = ptr.To(true)
			Expect(k8sClient.Update(ctx, plan)).To(Succeed())
		})
	})
})
