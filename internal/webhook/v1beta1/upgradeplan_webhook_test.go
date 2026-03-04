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
	provisioningv1 "github.com/rancher/rancher/pkg/apis/provisioning.cattle.io/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	managementv1beta1 "github.com/harvester/upgrade-toolkit/api/v1beta1"
	"github.com/harvester/upgrade-toolkit/pkg/upgradeplan"
)

var _ = Describe("UpgradePlan Webhook", func() {
	var (
		scheme *runtime.Scheme
	)

	BeforeEach(func() {
		scheme = runtime.NewScheme()
		Expect(managementv1beta1.AddToScheme(scheme)).To(Succeed())
		Expect(corev1.AddToScheme(scheme)).To(Succeed())
		Expect(provisioningv1.AddToScheme(scheme)).To(Succeed())
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
			cluster := &provisioningv1.Cluster{
				ObjectMeta: metav1.ObjectMeta{Namespace: "fleet-local", Name: "local"},
				Status:     provisioningv1.ClusterStatus{Ready: true},
			}
			validator := UpgradePlanCustomValidator{
				Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(version, cluster).Build(),
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
			cluster := &provisioningv1.Cluster{
				ObjectMeta: metav1.ObjectMeta{Namespace: "fleet-local", Name: "local"},
				Status:     provisioningv1.ClusterStatus{Ready: true},
			}
			validator := UpgradePlanCustomValidator{
				Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(existing, version, cluster).Build(),
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
			cluster := &provisioningv1.Cluster{
				ObjectMeta: metav1.ObjectMeta{Namespace: "fleet-local", Name: "local"},
				Status:     provisioningv1.ClusterStatus{Ready: true},
			}
			validator := UpgradePlanCustomValidator{
				Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(existing, version, cluster).Build(),
			}
			obj := &managementv1beta1.UpgradePlan{
				ObjectMeta: metav1.ObjectMeta{Name: "upgrade-new"},
				Spec:       managementv1beta1.UpgradePlanSpec{Version: "v1.4.0"},
			}

			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Context("skipWebhook annotation", func() {
		It("should bypass all validation when skipWebhook annotation is set", func() {
			// No Version CR, no cluster - would normally fail, but skipWebhook bypasses all checks
			validator := UpgradePlanCustomValidator{
				Client: fake.NewClientBuilder().WithScheme(scheme).Build(),
			}
			obj := &managementv1beta1.UpgradePlan{
				ObjectMeta: metav1.ObjectMeta{
					Name: "upgrade-1",
					Annotations: map[string]string{
						upgradeplan.AnnotationSkipWebhook: "true",
					},
				},
				Spec: managementv1beta1.UpgradePlanSpec{Version: "v1.4.0"},
			}

			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Context("validateNodeReadiness", func() {
		It("should reject when a node is not Ready", func() {
			version := &managementv1beta1.Version{
				ObjectMeta: metav1.ObjectMeta{Name: "v1.4.0"},
				Spec:       managementv1beta1.VersionSpec{ISODownloadURL: "https://example.com/iso"},
			}
			cluster := &provisioningv1.Cluster{
				ObjectMeta: metav1.ObjectMeta{Namespace: "fleet-local", Name: "local"},
				Status:     provisioningv1.ClusterStatus{Ready: true},
			}
			node := &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "node-1"},
				Status: corev1.NodeStatus{
					Conditions: []corev1.NodeCondition{
						{Type: corev1.NodeReady, Status: corev1.ConditionFalse},
					},
				},
			}
			validator := UpgradePlanCustomValidator{
				Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(version, cluster, node).Build(),
			}
			obj := &managementv1beta1.UpgradePlan{
				ObjectMeta: metav1.ObjectMeta{Name: "upgrade-1"},
				Spec:       managementv1beta1.UpgradePlanSpec{Version: "v1.4.0"},
			}

			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(`node "node-1" is not Ready`))
		})

		It("should reject when a node is unschedulable", func() {
			version := &managementv1beta1.Version{
				ObjectMeta: metav1.ObjectMeta{Name: "v1.4.0"},
				Spec:       managementv1beta1.VersionSpec{ISODownloadURL: "https://example.com/iso"},
			}
			cluster := &provisioningv1.Cluster{
				ObjectMeta: metav1.ObjectMeta{Namespace: "fleet-local", Name: "local"},
				Status:     provisioningv1.ClusterStatus{Ready: true},
			}
			node := &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "node-1"},
				Spec:       corev1.NodeSpec{Unschedulable: true},
				Status: corev1.NodeStatus{
					Conditions: []corev1.NodeCondition{
						{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
					},
				},
			}
			validator := UpgradePlanCustomValidator{
				Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(version, cluster, node).Build(),
			}
			obj := &managementv1beta1.UpgradePlan{
				ObjectMeta: metav1.ObjectMeta{Name: "upgrade-1"},
				Spec:       managementv1beta1.UpgradePlanSpec{Version: "v1.4.0"},
			}

			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(`node "node-1" is unschedulable`))
		})

		It("should allow when all nodes are healthy", func() {
			version := &managementv1beta1.Version{
				ObjectMeta: metav1.ObjectMeta{Name: "v1.4.0"},
				Spec:       managementv1beta1.VersionSpec{ISODownloadURL: "https://example.com/iso"},
			}
			cluster := &provisioningv1.Cluster{
				ObjectMeta: metav1.ObjectMeta{Namespace: "fleet-local", Name: "local"},
				Status:     provisioningv1.ClusterStatus{Ready: true},
			}
			node1 := &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "node-1"},
				Status: corev1.NodeStatus{
					Conditions: []corev1.NodeCondition{
						{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
					},
				},
			}
			node2 := &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "node-2"},
				Status: corev1.NodeStatus{
					Conditions: []corev1.NodeCondition{
						{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
					},
				},
			}
			validator := UpgradePlanCustomValidator{
				Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(version, cluster, node1, node2).Build(),
			}
			obj := &managementv1beta1.UpgradePlan{
				ObjectMeta: metav1.ObjectMeta{Name: "upgrade-1"},
				Spec:       managementv1beta1.UpgradePlanSpec{Version: "v1.4.0"},
			}

			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Context("validateClusterReady", func() {
		It("should reject when cluster is not found", func() {
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
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("cluster not found"))
		})

		It("should reject when cluster is not ready", func() {
			version := &managementv1beta1.Version{
				ObjectMeta: metav1.ObjectMeta{Name: "v1.4.0"},
				Spec:       managementv1beta1.VersionSpec{ISODownloadURL: "https://example.com/iso"},
			}
			cluster := &provisioningv1.Cluster{
				ObjectMeta: metav1.ObjectMeta{Namespace: "fleet-local", Name: "local"},
				Status:     provisioningv1.ClusterStatus{Ready: false},
			}
			validator := UpgradePlanCustomValidator{
				Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(version, cluster).Build(),
			}
			obj := &managementv1beta1.UpgradePlan{
				ObjectMeta: metav1.ObjectMeta{Name: "upgrade-1"},
				Spec:       managementv1beta1.UpgradePlanSpec{Version: "v1.4.0"},
			}

			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("cluster is not ready"))
		})

		It("should allow when cluster is ready", func() {
			version := &managementv1beta1.Version{
				ObjectMeta: metav1.ObjectMeta{Name: "v1.4.0"},
				Spec:       managementv1beta1.VersionSpec{ISODownloadURL: "https://example.com/iso"},
			}
			cluster := &provisioningv1.Cluster{
				ObjectMeta: metav1.ObjectMeta{Namespace: "fleet-local", Name: "local"},
				Status:     provisioningv1.ClusterStatus{Ready: true},
			}
			validator := UpgradePlanCustomValidator{
				Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(version, cluster).Build(),
			}
			obj := &managementv1beta1.UpgradePlan{
				ObjectMeta: metav1.ObjectMeta{Name: "upgrade-1"},
				Spec:       managementv1beta1.UpgradePlanSpec{Version: "v1.4.0"},
			}

			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Context("validateNoCleanupInProgress", func() {
		It("should reject when another upgrade is cleaning up", func() {
			version := &managementv1beta1.Version{
				ObjectMeta: metav1.ObjectMeta{Name: "v1.4.0"},
				Spec:       managementv1beta1.VersionSpec{ISODownloadURL: "https://example.com/iso"},
			}
			cluster := &provisioningv1.Cluster{
				ObjectMeta: metav1.ObjectMeta{Namespace: "fleet-local", Name: "local"},
				Status:     provisioningv1.ClusterStatus{Ready: true},
			}
			cleaningUp := &managementv1beta1.UpgradePlan{
				ObjectMeta: metav1.ObjectMeta{Name: "upgrade-old"},
				Spec:       managementv1beta1.UpgradePlanSpec{Version: "v1.3.0"},
				Status: managementv1beta1.UpgradePlanStatus{
					CurrentPhase: managementv1beta1.UpgradePlanPhaseCleaningUp,
				},
			}
			validator := UpgradePlanCustomValidator{
				Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(version, cluster, cleaningUp).Build(),
			}
			obj := &managementv1beta1.UpgradePlan{
				ObjectMeta: metav1.ObjectMeta{Name: "upgrade-new"},
				Spec:       managementv1beta1.UpgradePlanSpec{Version: "v1.4.0"},
			}

			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(`upgrade "upgrade-old" is still cleaning up`))
		})

		It("should allow when no other upgrade is cleaning up", func() {
			version := &managementv1beta1.Version{
				ObjectMeta: metav1.ObjectMeta{Name: "v1.4.0"},
				Spec:       managementv1beta1.VersionSpec{ISODownloadURL: "https://example.com/iso"},
			}
			cluster := &provisioningv1.Cluster{
				ObjectMeta: metav1.ObjectMeta{Namespace: "fleet-local", Name: "local"},
				Status:     provisioningv1.ClusterStatus{Ready: true},
			}
			succeeded := &managementv1beta1.UpgradePlan{
				ObjectMeta: metav1.ObjectMeta{Name: "upgrade-old"},
				Spec:       managementv1beta1.UpgradePlanSpec{Version: "v1.3.0"},
				Status: managementv1beta1.UpgradePlanStatus{
					CurrentPhase: managementv1beta1.UpgradePlanPhaseSucceeded,
				},
			}
			validator := UpgradePlanCustomValidator{
				Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(version, cluster, succeeded).Build(),
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

		It("should allow when only spec.nodeUpgradeOption is changed", func() {
			node1 := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-1"}}
			validator := UpgradePlanCustomValidator{
				Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(node1).Build(),
			}
			oldObj := &managementv1beta1.UpgradePlan{
				ObjectMeta: metav1.ObjectMeta{Name: "upgrade-1"},
				Spec: managementv1beta1.UpgradePlanSpec{
					Version: "v1.4.0",
				},
			}
			newObj := &managementv1beta1.UpgradePlan{
				ObjectMeta: metav1.ObjectMeta{Name: "upgrade-1"},
				Spec: managementv1beta1.UpgradePlanSpec{
					Version: "v1.4.0",
					NodeUpgradeOption: &managementv1beta1.NodeUpgradeOption{
						PauseNodes: []string{"node-1"},
					},
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

		It("should block deletion while cluster is being provisioned", func() {
			cluster := &provisioningv1.Cluster{
				ObjectMeta: metav1.ObjectMeta{Namespace: "fleet-local", Name: "local"},
				Status:     provisioningv1.ClusterStatus{Ready: false},
			}
			validator := UpgradePlanCustomValidator{
				Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build(),
			}
			obj := &managementv1beta1.UpgradePlan{
				ObjectMeta: metav1.ObjectMeta{Name: "upgrade-1"},
				Spec:       managementv1beta1.UpgradePlanSpec{Version: "v1.4.0"},
			}

			_, err := validator.ValidateDelete(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("cluster is being provisioned"))
		})

		It("should block deletion while nodes are being upgraded", func() {
			validator := UpgradePlanCustomValidator{
				Client: fake.NewClientBuilder().WithScheme(scheme).Build(),
			}
			obj := &managementv1beta1.UpgradePlan{
				ObjectMeta: metav1.ObjectMeta{Name: "upgrade-1"},
				Spec:       managementv1beta1.UpgradePlanSpec{Version: "v1.4.0"},
				Status: managementv1beta1.UpgradePlanStatus{
					CurrentPhase: managementv1beta1.UpgradePlanPhaseNodeUpgrading,
				},
			}

			_, err := validator.ValidateDelete(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("nodes are being upgraded"))
		})

		It("should allow deletion when cluster is ready and not node upgrading", func() {
			cluster := &provisioningv1.Cluster{
				ObjectMeta: metav1.ObjectMeta{Namespace: "fleet-local", Name: "local"},
				Status:     provisioningv1.ClusterStatus{Ready: true},
			}
			validator := UpgradePlanCustomValidator{
				Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build(),
			}
			obj := &managementv1beta1.UpgradePlan{
				ObjectMeta: metav1.ObjectMeta{Name: "upgrade-1"},
				Spec:       managementv1beta1.UpgradePlanSpec{Version: "v1.4.0"},
				Status: managementv1beta1.UpgradePlanStatus{
					CurrentPhase: managementv1beta1.UpgradePlanPhaseSucceeded,
				},
			}

			_, err := validator.ValidateDelete(ctx, obj)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Context("validateNodeUpgradeOption", func() {
		It("should reject pauseNodes with non-existent node names on create", func() {
			version := &managementv1beta1.Version{
				ObjectMeta: metav1.ObjectMeta{Name: "v1.4.0"},
			}
			validator := UpgradePlanCustomValidator{
				Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(version).Build(),
			}
			obj := &managementv1beta1.UpgradePlan{
				ObjectMeta: metav1.ObjectMeta{Name: "upgrade-1"},
				Spec: managementv1beta1.UpgradePlanSpec{
					Version: "v1.4.0",
					NodeUpgradeOption: &managementv1beta1.NodeUpgradeOption{
						PauseNodes: []string{"nonexistent-node"},
					},
				},
			}

			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("nonexistent-node"))
		})

		It("should reject duplicate pauseNodes on update", func() {
			node1 := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-1"}}
			validator := UpgradePlanCustomValidator{
				Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(node1).Build(),
			}
			oldObj := &managementv1beta1.UpgradePlan{
				ObjectMeta: metav1.ObjectMeta{Name: "upgrade-1"},
				Spec:       managementv1beta1.UpgradePlanSpec{Version: "v1.4.0"},
			}
			newObj := &managementv1beta1.UpgradePlan{
				ObjectMeta: metav1.ObjectMeta{Name: "upgrade-1"},
				Spec: managementv1beta1.UpgradePlanSpec{
					Version: "v1.4.0",
					NodeUpgradeOption: &managementv1beta1.NodeUpgradeOption{
						PauseNodes: []string{"node-1", "node-1"},
					},
				},
			}

			_, err := validator.ValidateUpdate(ctx, oldObj, newObj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("Duplicate"))
		})

		It("should reject empty string in pauseNodes", func() {
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
					NodeUpgradeOption: &managementv1beta1.NodeUpgradeOption{
						PauseNodes: []string{""},
					},
				},
			}

			_, err := validator.ValidateUpdate(ctx, oldObj, newObj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("node name must not be empty"))
		})

		It("should accept valid pauseNodes", func() {
			node1 := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-1"}}
			node2 := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-2"}}
			validator := UpgradePlanCustomValidator{
				Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(node1, node2).Build(),
			}
			oldObj := &managementv1beta1.UpgradePlan{
				ObjectMeta: metav1.ObjectMeta{Name: "upgrade-1"},
				Spec:       managementv1beta1.UpgradePlanSpec{Version: "v1.4.0"},
			}
			newObj := &managementv1beta1.UpgradePlan{
				ObjectMeta: metav1.ObjectMeta{Name: "upgrade-1"},
				Spec: managementv1beta1.UpgradePlanSpec{
					Version: "v1.4.0",
					NodeUpgradeOption: &managementv1beta1.NodeUpgradeOption{
						PauseNodes: []string{"node-1", "node-2"},
					},
				},
			}

			_, err := validator.ValidateUpdate(ctx, oldObj, newObj)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should accept empty pauseNodes (no nodes paused)", func() {
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
					Version:           "v1.4.0",
					NodeUpgradeOption: &managementv1beta1.NodeUpgradeOption{},
				},
			}

			_, err := validator.ValidateUpdate(ctx, oldObj, newObj)
			Expect(err).NotTo(HaveOccurred())
		})
	})
})
