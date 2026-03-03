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
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	managementv1beta1 "github.com/harvester/upgrade-toolkit/api/v1beta1"
)

var _ = Describe("UpgradePlan Webhook", func() {
	var (
		scheme *runtime.Scheme
	)

	BeforeEach(func() {
		scheme = runtime.NewScheme()
		Expect(managementv1beta1.AddToScheme(scheme)).To(Succeed())
	})

	Context("ValidateCreate", func() {
		It("should reject when spec.version references a non-existent Version", func() {
			validator := UpgradePlanCustomValidator{
				Client: fake.NewClientBuilder().WithScheme(scheme).Build(),
			}
			obj := &managementv1beta1.UpgradePlan{
				ObjectMeta: metav1.ObjectMeta{Name: "upgrade-1"},
				Spec:       managementv1beta1.UpgradePlanSpec{Version: "v1.4.0"},
			}

			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("spec.version"))
		})

		It("should allow when spec.version references an existing Version", func() {
			version := &managementv1beta1.Version{
				ObjectMeta: metav1.ObjectMeta{Name: "v1.4.0"},
				Spec:       managementv1beta1.VersionSpec{ISODownloadURL: "https://example.com/iso"},
			}
			validator := UpgradePlanCustomValidator{
				Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(version).Build(),
			}
			obj := &managementv1beta1.UpgradePlan{
				ObjectMeta: metav1.ObjectMeta{Name: "upgrade-1"},
				Spec:       managementv1beta1.UpgradePlanSpec{Version: "v1.4.0"},
			}

			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should reject when another UpgradePlan has Progressing=True", func() {
			existing := &managementv1beta1.UpgradePlan{
				ObjectMeta: metav1.ObjectMeta{Name: "upgrade-old"},
				Spec:       managementv1beta1.UpgradePlanSpec{Version: "v1.3.0"},
				Status: managementv1beta1.UpgradePlanStatus{
					Conditions: []metav1.Condition{
						{
							Type:   managementv1beta1.UpgradePlanProgressing,
							Status: metav1.ConditionTrue,
						},
					},
				},
			}
			version := &managementv1beta1.Version{
				ObjectMeta: metav1.ObjectMeta{Name: "v1.4.0"},
				Spec:       managementv1beta1.VersionSpec{ISODownloadURL: "https://example.com/iso"},
			}
			validator := UpgradePlanCustomValidator{
				Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(existing, version).Build(),
			}
			obj := &managementv1beta1.UpgradePlan{
				ObjectMeta: metav1.ObjectMeta{Name: "upgrade-new"},
				Spec:       managementv1beta1.UpgradePlanSpec{Version: "v1.4.0"},
			}

			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("upgrade-old"))
		})

		It("should reject concurrent upgrade even with force=true", func() {
			existing := &managementv1beta1.UpgradePlan{
				ObjectMeta: metav1.ObjectMeta{Name: "upgrade-old"},
				Spec:       managementv1beta1.UpgradePlanSpec{Version: "v1.3.0"},
				Status: managementv1beta1.UpgradePlanStatus{
					Conditions: []metav1.Condition{
						{
							Type:   managementv1beta1.UpgradePlanProgressing,
							Status: metav1.ConditionTrue,
						},
					},
				},
			}
			version := &managementv1beta1.Version{
				ObjectMeta: metav1.ObjectMeta{Name: "v1.4.0"},
				Spec:       managementv1beta1.VersionSpec{ISODownloadURL: "https://example.com/iso"},
			}
			validator := UpgradePlanCustomValidator{
				Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(existing, version).Build(),
			}
			obj := &managementv1beta1.UpgradePlan{
				ObjectMeta: metav1.ObjectMeta{Name: "upgrade-new"},
				Spec: managementv1beta1.UpgradePlanSpec{
					Version: "v1.4.0",
					Force:   ptr.To(true),
				},
			}

			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("upgrade-old"))
		})

		It("should allow when existing UpgradePlan has Progressing=False", func() {
			existing := &managementv1beta1.UpgradePlan{
				ObjectMeta: metav1.ObjectMeta{Name: "upgrade-old"},
				Spec:       managementv1beta1.UpgradePlanSpec{Version: "v1.3.0"},
				Status: managementv1beta1.UpgradePlanStatus{
					Conditions: []metav1.Condition{
						{
							Type:   managementv1beta1.UpgradePlanProgressing,
							Status: metav1.ConditionFalse,
						},
					},
				},
			}
			version := &managementv1beta1.Version{
				ObjectMeta: metav1.ObjectMeta{Name: "v1.4.0"},
				Spec:       managementv1beta1.VersionSpec{ISODownloadURL: "https://example.com/iso"},
			}
			validator := UpgradePlanCustomValidator{
				Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(existing, version).Build(),
			}
			obj := &managementv1beta1.UpgradePlan{
				ObjectMeta: metav1.ObjectMeta{Name: "upgrade-new"},
				Spec:       managementv1beta1.UpgradePlanSpec{Version: "v1.4.0"},
			}

			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should allow when existing UpgradePlan has no Progressing condition", func() {
			existing := &managementv1beta1.UpgradePlan{
				ObjectMeta: metav1.ObjectMeta{Name: "upgrade-old"},
				Spec:       managementv1beta1.UpgradePlanSpec{Version: "v1.3.0"},
			}
			version := &managementv1beta1.Version{
				ObjectMeta: metav1.ObjectMeta{Name: "v1.4.0"},
				Spec:       managementv1beta1.VersionSpec{ISODownloadURL: "https://example.com/iso"},
			}
			validator := UpgradePlanCustomValidator{
				Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(existing, version).Build(),
			}
			obj := &managementv1beta1.UpgradePlan{
				ObjectMeta: metav1.ObjectMeta{Name: "upgrade-new"},
				Spec:       managementv1beta1.UpgradePlanSpec{Version: "v1.4.0"},
			}

			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Context("ValidateUpdate", func() {
		It("should reject when spec.version is changed", func() {
			validator := UpgradePlanCustomValidator{
				Client: fake.NewClientBuilder().WithScheme(scheme).Build(),
			}
			oldObj := &managementv1beta1.UpgradePlan{
				ObjectMeta: metav1.ObjectMeta{Name: "upgrade-1"},
				Spec:       managementv1beta1.UpgradePlanSpec{Version: "v1.3.0"},
			}
			newObj := &managementv1beta1.UpgradePlan{
				ObjectMeta: metav1.ObjectMeta{Name: "upgrade-1"},
				Spec:       managementv1beta1.UpgradePlanSpec{Version: "v1.4.0"},
			}

			_, err := validator.ValidateUpdate(ctx, oldObj, newObj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("spec.version"))
		})

		It("should reject when spec.upgrade is changed", func() {
			validator := UpgradePlanCustomValidator{
				Client: fake.NewClientBuilder().WithScheme(scheme).Build(),
			}
			oldObj := &managementv1beta1.UpgradePlan{
				ObjectMeta: metav1.ObjectMeta{Name: "upgrade-1"},
				Spec: managementv1beta1.UpgradePlanSpec{
					Version: "v1.4.0",
					Upgrade: ptr.To("custom-image"),
				},
			}
			newObj := &managementv1beta1.UpgradePlan{
				ObjectMeta: metav1.ObjectMeta{Name: "upgrade-1"},
				Spec: managementv1beta1.UpgradePlanSpec{
					Version: "v1.4.0",
					Upgrade: ptr.To("different-image"),
				},
			}

			_, err := validator.ValidateUpdate(ctx, oldObj, newObj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("spec.upgrade"))
		})

		It("should allow when only spec.mode is changed", func() {
			validator := UpgradePlanCustomValidator{
				Client: fake.NewClientBuilder().WithScheme(scheme).Build(),
			}
			oldObj := &managementv1beta1.UpgradePlan{
				ObjectMeta: metav1.ObjectMeta{Name: "upgrade-1"},
				Spec: managementv1beta1.UpgradePlanSpec{
					Version: "v1.4.0",
					Mode:    ptr.To("automatic"),
				},
			}
			newObj := &managementv1beta1.UpgradePlan{
				ObjectMeta: metav1.ObjectMeta{Name: "upgrade-1"},
				Spec: managementv1beta1.UpgradePlanSpec{
					Version: "v1.4.0",
					Mode:    ptr.To("interactive"),
				},
			}

			_, err := validator.ValidateUpdate(ctx, oldObj, newObj)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should allow when only spec.force is changed", func() {
			validator := UpgradePlanCustomValidator{
				Client: fake.NewClientBuilder().WithScheme(scheme).Build(),
			}
			oldObj := &managementv1beta1.UpgradePlan{
				ObjectMeta: metav1.ObjectMeta{Name: "upgrade-1"},
				Spec:       managementv1beta1.UpgradePlanSpec{Version: "v1.4.0"},
			}
			newObj := &managementv1beta1.UpgradePlan{
				ObjectMeta: metav1.ObjectMeta{Name: "upgrade-1"},
				Spec: managementv1beta1.UpgradePlanSpec{
					Version: "v1.4.0",
					Force:   ptr.To(true),
				},
			}

			_, err := validator.ValidateUpdate(ctx, oldObj, newObj)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should allow no-op update with identical spec", func() {
			validator := UpgradePlanCustomValidator{
				Client: fake.NewClientBuilder().WithScheme(scheme).Build(),
			}
			oldObj := &managementv1beta1.UpgradePlan{
				ObjectMeta: metav1.ObjectMeta{Name: "upgrade-1"},
				Spec:       managementv1beta1.UpgradePlanSpec{Version: "v1.4.0"},
			}
			newObj := &managementv1beta1.UpgradePlan{
				ObjectMeta: metav1.ObjectMeta{Name: "upgrade-1"},
				Spec:       managementv1beta1.UpgradePlanSpec{Version: "v1.4.0"},
			}

			_, err := validator.ValidateUpdate(ctx, oldObj, newObj)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Context("ValidateDelete", func() {
		It("should allow deletion when Progressing=False", func() {
			validator := UpgradePlanCustomValidator{
				Client: fake.NewClientBuilder().WithScheme(scheme).Build(),
			}
			obj := &managementv1beta1.UpgradePlan{
				ObjectMeta: metav1.ObjectMeta{Name: "upgrade-1"},
				Spec:       managementv1beta1.UpgradePlanSpec{Version: "v1.4.0"},
				Status: managementv1beta1.UpgradePlanStatus{
					Conditions: []metav1.Condition{
						{
							Type:   managementv1beta1.UpgradePlanProgressing,
							Status: metav1.ConditionFalse,
						},
					},
				},
			}

			_, err := validator.ValidateDelete(ctx, obj)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should allow deletion when no Progressing condition exists", func() {
			validator := UpgradePlanCustomValidator{
				Client: fake.NewClientBuilder().WithScheme(scheme).Build(),
			}
			obj := &managementv1beta1.UpgradePlan{
				ObjectMeta: metav1.ObjectMeta{Name: "upgrade-1"},
				Spec:       managementv1beta1.UpgradePlanSpec{Version: "v1.4.0"},
			}

			_, err := validator.ValidateDelete(ctx, obj)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should block deletion when Progressing=True", func() {
			validator := UpgradePlanCustomValidator{
				Client: fake.NewClientBuilder().WithScheme(scheme).Build(),
			}
			obj := &managementv1beta1.UpgradePlan{
				ObjectMeta: metav1.ObjectMeta{Name: "upgrade-1"},
				Spec:       managementv1beta1.UpgradePlanSpec{Version: "v1.4.0"},
				Status: managementv1beta1.UpgradePlanStatus{
					Conditions: []metav1.Condition{
						{
							Type:   managementv1beta1.UpgradePlanProgressing,
							Status: metav1.ConditionTrue,
						},
					},
				},
			}

			_, err := validator.ValidateDelete(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("Progressing"))
		})
	})
})
