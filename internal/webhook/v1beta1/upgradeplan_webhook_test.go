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
	harvesterv1beta1 "github.com/harvester/harvester/pkg/apis/harvesterhci.io/v1beta1"
	lhv1beta2 "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	provisioningv1 "github.com/rancher/rancher/pkg/apis/provisioning.cattle.io/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	kubevirtv1 "kubevirt.io/api/core/v1"
	clusterv1 "sigs.k8s.io/cluster-api/api/v1beta1"
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
		Expect(clusterv1.AddToScheme(scheme)).To(Succeed())
		Expect(lhv1beta2.AddToScheme(scheme)).To(Succeed())
		Expect(harvesterv1beta1.AddToScheme(scheme)).To(Succeed())
		Expect(kubevirtv1.AddToScheme(scheme)).To(Succeed())
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
			node := &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "node-1",
					Labels:      map[string]string{"harvesterhci.io/managed": "true"},
					Annotations: map[string]string{"cluster.x-k8s.io/machine": "machine-1"},
				},
				Status: corev1.NodeStatus{
					Conditions: []corev1.NodeCondition{
						{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
					},
				},
			}
			machine := &clusterv1.Machine{
				ObjectMeta: metav1.ObjectMeta{Namespace: "fleet-local", Name: "machine-1"},
				Status: clusterv1.MachineStatus{
					Phase:   string(clusterv1.MachinePhaseRunning),
					NodeRef: &corev1.ObjectReference{Name: "node-1"},
				},
			}
			validator := UpgradePlanCustomValidator{
				Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(version, cluster, node, machine).Build(),
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
			node := &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "node-1",
					Labels:      map[string]string{"harvesterhci.io/managed": "true"},
					Annotations: map[string]string{"cluster.x-k8s.io/machine": "machine-1"},
				},
				Status: corev1.NodeStatus{
					Conditions: []corev1.NodeCondition{
						{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
					},
				},
			}
			machine := &clusterv1.Machine{
				ObjectMeta: metav1.ObjectMeta{Namespace: "fleet-local", Name: "machine-1"},
				Status: clusterv1.MachineStatus{
					Phase:   string(clusterv1.MachinePhaseRunning),
					NodeRef: &corev1.ObjectReference{Name: "node-1"},
				},
			}
			validator := UpgradePlanCustomValidator{
				Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(existing, version, cluster, node, machine).Build(),
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
			node := &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "node-1",
					Labels:      map[string]string{"harvesterhci.io/managed": "true"},
					Annotations: map[string]string{"cluster.x-k8s.io/machine": "machine-1"},
				},
				Status: corev1.NodeStatus{
					Conditions: []corev1.NodeCondition{
						{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
					},
				},
			}
			machine := &clusterv1.Machine{
				ObjectMeta: metav1.ObjectMeta{Namespace: "fleet-local", Name: "machine-1"},
				Status: clusterv1.MachineStatus{
					Phase:   string(clusterv1.MachinePhaseRunning),
					NodeRef: &corev1.ObjectReference{Name: "node-1"},
				},
			}
			validator := UpgradePlanCustomValidator{
				Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(existing, version, cluster, node, machine).Build(),
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
				ObjectMeta: metav1.ObjectMeta{
					Name:        "node-1",
					Labels:      map[string]string{"harvesterhci.io/managed": "true"},
					Annotations: map[string]string{"cluster.x-k8s.io/machine": "machine-1"},
				},
				Status: corev1.NodeStatus{
					Conditions: []corev1.NodeCondition{
						{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
					},
				},
			}
			node2 := &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "node-2",
					Labels:      map[string]string{"harvesterhci.io/managed": "true"},
					Annotations: map[string]string{"cluster.x-k8s.io/machine": "machine-2"},
				},
				Status: corev1.NodeStatus{
					Conditions: []corev1.NodeCondition{
						{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
					},
				},
			}
			machine1 := &clusterv1.Machine{
				ObjectMeta: metav1.ObjectMeta{Namespace: "fleet-local", Name: "machine-1"},
				Status: clusterv1.MachineStatus{
					Phase:   string(clusterv1.MachinePhaseRunning),
					NodeRef: &corev1.ObjectReference{Name: "node-1"},
				},
			}
			machine2 := &clusterv1.Machine{
				ObjectMeta: metav1.ObjectMeta{Namespace: "fleet-local", Name: "machine-2"},
				Status: clusterv1.MachineStatus{
					Phase:   string(clusterv1.MachinePhaseRunning),
					NodeRef: &corev1.ObjectReference{Name: "node-2"},
				},
			}
			validator := UpgradePlanCustomValidator{
				Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(version, cluster, node1, node2, machine1, machine2).Build(),
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
			node := &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "node-1",
					Labels:      map[string]string{"harvesterhci.io/managed": "true"},
					Annotations: map[string]string{"cluster.x-k8s.io/machine": "machine-1"},
				},
				Status: corev1.NodeStatus{
					Conditions: []corev1.NodeCondition{
						{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
					},
				},
			}
			machine := &clusterv1.Machine{
				ObjectMeta: metav1.ObjectMeta{Namespace: "fleet-local", Name: "machine-1"},
				Status: clusterv1.MachineStatus{
					Phase:   string(clusterv1.MachinePhaseRunning),
					NodeRef: &corev1.ObjectReference{Name: "node-1"},
				},
			}
			validator := UpgradePlanCustomValidator{
				Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(version, cluster, node, machine).Build(),
			}
			obj := &managementv1beta1.UpgradePlan{
				ObjectMeta: metav1.ObjectMeta{Name: "upgrade-1"},
				Spec:       managementv1beta1.UpgradePlanSpec{Version: "v1.4.0"},
			}

			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Context("validateMachinesRunning", func() {
		It("should reject when a machine is not running", func() {
			version := &managementv1beta1.Version{
				ObjectMeta: metav1.ObjectMeta{Name: "v1.4.0"},
				Spec:       managementv1beta1.VersionSpec{ISODownloadURL: "https://example.com/iso"},
			}
			cluster := &provisioningv1.Cluster{
				ObjectMeta: metav1.ObjectMeta{Namespace: "fleet-local", Name: "local"},
				Status:     provisioningv1.ClusterStatus{Ready: true},
			}
			node := &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "node-1",
					Labels:      map[string]string{"harvesterhci.io/managed": "true"},
					Annotations: map[string]string{"cluster.x-k8s.io/machine": "machine-1"},
				},
				Status: corev1.NodeStatus{
					Conditions: []corev1.NodeCondition{
						{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
					},
				},
			}
			machine := &clusterv1.Machine{
				ObjectMeta: metav1.ObjectMeta{Namespace: "fleet-local", Name: "machine-1"},
				Status: clusterv1.MachineStatus{
					Phase:   string(clusterv1.MachinePhaseProvisioning),
					NodeRef: &corev1.ObjectReference{Name: "node-1"},
				},
			}
			validator := UpgradePlanCustomValidator{
				Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(version, cluster, node, machine).Build(),
			}
			obj := &managementv1beta1.UpgradePlan{
				ObjectMeta: metav1.ObjectMeta{Name: "upgrade-1"},
				Spec:       managementv1beta1.UpgradePlanSpec{Version: "v1.4.0"},
			}

			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("machine fleet-local/machine-1 is not running"))
		})

		It("should allow when all machines are running", func() {
			version := &managementv1beta1.Version{
				ObjectMeta: metav1.ObjectMeta{Name: "v1.4.0"},
				Spec:       managementv1beta1.VersionSpec{ISODownloadURL: "https://example.com/iso"},
			}
			cluster := &provisioningv1.Cluster{
				ObjectMeta: metav1.ObjectMeta{Namespace: "fleet-local", Name: "local"},
				Status:     provisioningv1.ClusterStatus{Ready: true},
			}
			node := &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "node-1",
					Labels:      map[string]string{"harvesterhci.io/managed": "true"},
					Annotations: map[string]string{"cluster.x-k8s.io/machine": "machine-1"},
				},
				Status: corev1.NodeStatus{
					Conditions: []corev1.NodeCondition{
						{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
					},
				},
			}
			machine := &clusterv1.Machine{
				ObjectMeta: metav1.ObjectMeta{Namespace: "fleet-local", Name: "machine-1"},
				Status: clusterv1.MachineStatus{
					Phase:   string(clusterv1.MachinePhaseRunning),
					NodeRef: &corev1.ObjectReference{Name: "node-1"},
				},
			}
			validator := UpgradePlanCustomValidator{
				Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(version, cluster, node, machine).Build(),
			}
			obj := &managementv1beta1.UpgradePlan{
				ObjectMeta: metav1.ObjectMeta{Name: "upgrade-1"},
				Spec:       managementv1beta1.UpgradePlanSpec{Version: "v1.4.0"},
			}

			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Context("validateNodeMachineConsistency", func() {
		It("should reject when node count does not match machine count", func() {
			version := &managementv1beta1.Version{
				ObjectMeta: metav1.ObjectMeta{Name: "v1.4.0"},
				Spec:       managementv1beta1.VersionSpec{ISODownloadURL: "https://example.com/iso"},
			}
			cluster := &provisioningv1.Cluster{
				ObjectMeta: metav1.ObjectMeta{Namespace: "fleet-local", Name: "local"},
				Status:     provisioningv1.ClusterStatus{Ready: true},
			}
			node := &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "node-1",
					Labels:      map[string]string{"harvesterhci.io/managed": "true"},
					Annotations: map[string]string{"cluster.x-k8s.io/machine": "machine-1"},
				},
				Status: corev1.NodeStatus{
					Conditions: []corev1.NodeCondition{
						{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
					},
				},
			}
			// No machines - count mismatch
			validator := UpgradePlanCustomValidator{
				Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(version, cluster, node).Build(),
			}
			obj := &managementv1beta1.UpgradePlan{
				ObjectMeta: metav1.ObjectMeta{Name: "upgrade-1"},
				Spec:       managementv1beta1.UpgradePlanSpec{Version: "v1.4.0"},
			}

			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("node count"))
			Expect(err.Error()).To(ContainSubstring("does not match machine count"))
		})

		It("should reject when machine has no node reference", func() {
			version := &managementv1beta1.Version{
				ObjectMeta: metav1.ObjectMeta{Name: "v1.4.0"},
				Spec:       managementv1beta1.VersionSpec{ISODownloadURL: "https://example.com/iso"},
			}
			cluster := &provisioningv1.Cluster{
				ObjectMeta: metav1.ObjectMeta{Namespace: "fleet-local", Name: "local"},
				Status:     provisioningv1.ClusterStatus{Ready: true},
			}
			node := &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "node-1",
					Labels:      map[string]string{"harvesterhci.io/managed": "true"},
					Annotations: map[string]string{"cluster.x-k8s.io/machine": "machine-1"},
				},
				Status: corev1.NodeStatus{
					Conditions: []corev1.NodeCondition{
						{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
					},
				},
			}
			machine := &clusterv1.Machine{
				ObjectMeta: metav1.ObjectMeta{Namespace: "fleet-local", Name: "machine-1"},
				Status: clusterv1.MachineStatus{
					Phase: string(clusterv1.MachinePhaseRunning),
					// NodeRef intentionally nil
				},
			}
			validator := UpgradePlanCustomValidator{
				Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(version, cluster, node, machine).Build(),
			}
			obj := &managementv1beta1.UpgradePlan{
				ObjectMeta: metav1.ObjectMeta{Name: "upgrade-1"},
				Spec:       managementv1beta1.UpgradePlanSpec{Version: "v1.4.0"},
			}

			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("has no node reference"))
		})

		It("should reject when node is missing managed label", func() {
			version := &managementv1beta1.Version{
				ObjectMeta: metav1.ObjectMeta{Name: "v1.4.0"},
				Spec:       managementv1beta1.VersionSpec{ISODownloadURL: "https://example.com/iso"},
			}
			cluster := &provisioningv1.Cluster{
				ObjectMeta: metav1.ObjectMeta{Namespace: "fleet-local", Name: "local"},
				Status:     provisioningv1.ClusterStatus{Ready: true},
			}
			node := &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "node-1",
					// No managed label
				},
				Status: corev1.NodeStatus{
					Conditions: []corev1.NodeCondition{
						{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
					},
				},
			}
			machine := &clusterv1.Machine{
				ObjectMeta: metav1.ObjectMeta{Namespace: "fleet-local", Name: "machine-1"},
				Status: clusterv1.MachineStatus{
					Phase:   string(clusterv1.MachinePhaseRunning),
					NodeRef: &corev1.ObjectReference{Name: "node-1"},
				},
			}
			validator := UpgradePlanCustomValidator{
				Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(version, cluster, node, machine).Build(),
			}
			obj := &managementv1beta1.UpgradePlan{
				ObjectMeta: metav1.ObjectMeta{Name: "upgrade-1"},
				Spec:       managementv1beta1.UpgradePlanSpec{Version: "v1.4.0"},
			}

			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("missing harvesterhci.io/managed label"))
		})

		It("should reject when node is missing machine annotation", func() {
			version := &managementv1beta1.Version{
				ObjectMeta: metav1.ObjectMeta{Name: "v1.4.0"},
				Spec:       managementv1beta1.VersionSpec{ISODownloadURL: "https://example.com/iso"},
			}
			cluster := &provisioningv1.Cluster{
				ObjectMeta: metav1.ObjectMeta{Namespace: "fleet-local", Name: "local"},
				Status:     provisioningv1.ClusterStatus{Ready: true},
			}
			node := &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "node-1",
					Labels: map[string]string{"harvesterhci.io/managed": "true"},
					// No machine annotation
				},
				Status: corev1.NodeStatus{
					Conditions: []corev1.NodeCondition{
						{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
					},
				},
			}
			machine := &clusterv1.Machine{
				ObjectMeta: metav1.ObjectMeta{Namespace: "fleet-local", Name: "machine-1"},
				Status: clusterv1.MachineStatus{
					Phase:   string(clusterv1.MachinePhaseRunning),
					NodeRef: &corev1.ObjectReference{Name: "node-1"},
				},
			}
			validator := UpgradePlanCustomValidator{
				Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(version, cluster, node, machine).Build(),
			}
			obj := &managementv1beta1.UpgradePlan{
				ObjectMeta: metav1.ObjectMeta{Name: "upgrade-1"},
				Spec:       managementv1beta1.UpgradePlanSpec{Version: "v1.4.0"},
			}

			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("missing cluster.x-k8s.io/machine annotation"))
		})

		It("should reject when referenced machine does not exist", func() {
			version := &managementv1beta1.Version{
				ObjectMeta: metav1.ObjectMeta{Name: "v1.4.0"},
				Spec:       managementv1beta1.VersionSpec{ISODownloadURL: "https://example.com/iso"},
			}
			cluster := &provisioningv1.Cluster{
				ObjectMeta: metav1.ObjectMeta{Namespace: "fleet-local", Name: "local"},
				Status:     provisioningv1.ClusterStatus{Ready: true},
			}
			node := &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "node-1",
					Labels:      map[string]string{"harvesterhci.io/managed": "true"},
					Annotations: map[string]string{"cluster.x-k8s.io/machine": "nonexistent-machine"},
				},
				Status: corev1.NodeStatus{
					Conditions: []corev1.NodeCondition{
						{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
					},
				},
			}
			machine := &clusterv1.Machine{
				ObjectMeta: metav1.ObjectMeta{Namespace: "fleet-local", Name: "machine-1"},
				Status: clusterv1.MachineStatus{
					Phase:   string(clusterv1.MachinePhaseRunning),
					NodeRef: &corev1.ObjectReference{Name: "node-1"},
				},
			}
			validator := UpgradePlanCustomValidator{
				Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(version, cluster, node, machine).Build(),
			}
			obj := &managementv1beta1.UpgradePlan{
				ObjectMeta: metav1.ObjectMeta{Name: "upgrade-1"},
				Spec:       managementv1beta1.UpgradePlanSpec{Version: "v1.4.0"},
			}

			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(`references machine "nonexistent-machine" which does not exist`))
		})

		It("should reject when machine NodeRef does not match node", func() {
			version := &managementv1beta1.Version{
				ObjectMeta: metav1.ObjectMeta{Name: "v1.4.0"},
				Spec:       managementv1beta1.VersionSpec{ISODownloadURL: "https://example.com/iso"},
			}
			cluster := &provisioningv1.Cluster{
				ObjectMeta: metav1.ObjectMeta{Namespace: "fleet-local", Name: "local"},
				Status:     provisioningv1.ClusterStatus{Ready: true},
			}
			node := &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "node-1",
					Labels:      map[string]string{"harvesterhci.io/managed": "true"},
					Annotations: map[string]string{"cluster.x-k8s.io/machine": "machine-1"},
				},
				Status: corev1.NodeStatus{
					Conditions: []corev1.NodeCondition{
						{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
					},
				},
			}
			machine := &clusterv1.Machine{
				ObjectMeta: metav1.ObjectMeta{Namespace: "fleet-local", Name: "machine-1"},
				Status: clusterv1.MachineStatus{
					Phase:   string(clusterv1.MachinePhaseRunning),
					NodeRef: &corev1.ObjectReference{Name: "different-node"},
				},
			}
			validator := UpgradePlanCustomValidator{
				Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(version, cluster, node, machine).Build(),
			}
			obj := &managementv1beta1.UpgradePlan{
				ObjectMeta: metav1.ObjectMeta{Name: "upgrade-1"},
				Spec:       managementv1beta1.UpgradePlanSpec{Version: "v1.4.0"},
			}

			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("does not match node"))
		})

		It("should allow when nodes and machines are consistent", func() {
			version := &managementv1beta1.Version{
				ObjectMeta: metav1.ObjectMeta{Name: "v1.4.0"},
				Spec:       managementv1beta1.VersionSpec{ISODownloadURL: "https://example.com/iso"},
			}
			cluster := &provisioningv1.Cluster{
				ObjectMeta: metav1.ObjectMeta{Namespace: "fleet-local", Name: "local"},
				Status:     provisioningv1.ClusterStatus{Ready: true},
			}
			node := &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "node-1",
					Labels:      map[string]string{"harvesterhci.io/managed": "true"},
					Annotations: map[string]string{"cluster.x-k8s.io/machine": "machine-1"},
				},
				Status: corev1.NodeStatus{
					Conditions: []corev1.NodeCondition{
						{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
					},
				},
			}
			machine := &clusterv1.Machine{
				ObjectMeta: metav1.ObjectMeta{Namespace: "fleet-local", Name: "machine-1"},
				Status: clusterv1.MachineStatus{
					Phase:   string(clusterv1.MachinePhaseRunning),
					NodeRef: &corev1.ObjectReference{Name: "node-1"},
				},
			}
			validator := UpgradePlanCustomValidator{
				Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(version, cluster, node, machine).Build(),
			}
			obj := &managementv1beta1.UpgradePlan{
				ObjectMeta: metav1.ObjectMeta{Name: "upgrade-1"},
				Spec:       managementv1beta1.UpgradePlanSpec{Version: "v1.4.0"},
			}

			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Context("validateLonghornVolumes", func() {
		It("should reject when a volume is degraded on 3+ node clusters", func() {
			version := &managementv1beta1.Version{
				ObjectMeta: metav1.ObjectMeta{Name: "v1.4.0"},
				Spec:       managementv1beta1.VersionSpec{ISODownloadURL: "https://example.com/iso"},
			}
			cluster := &provisioningv1.Cluster{
				ObjectMeta: metav1.ObjectMeta{Namespace: "fleet-local", Name: "local"},
				Status:     provisioningv1.ClusterStatus{Ready: true},
			}
			nodes := []*corev1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "n1", Labels: map[string]string{"harvesterhci.io/managed": "true"}, Annotations: map[string]string{"cluster.x-k8s.io/machine": "m1"}}, Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}}},
				{ObjectMeta: metav1.ObjectMeta{Name: "n2", Labels: map[string]string{"harvesterhci.io/managed": "true"}, Annotations: map[string]string{"cluster.x-k8s.io/machine": "m2"}}, Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}}},
				{ObjectMeta: metav1.ObjectMeta{Name: "n3", Labels: map[string]string{"harvesterhci.io/managed": "true"}, Annotations: map[string]string{"cluster.x-k8s.io/machine": "m3"}}, Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}}},
			}
			machines := []*clusterv1.Machine{
				{ObjectMeta: metav1.ObjectMeta{Namespace: "fleet-local", Name: "m1"}, Status: clusterv1.MachineStatus{Phase: string(clusterv1.MachinePhaseRunning), NodeRef: &corev1.ObjectReference{Name: "n1"}}},
				{ObjectMeta: metav1.ObjectMeta{Namespace: "fleet-local", Name: "m2"}, Status: clusterv1.MachineStatus{Phase: string(clusterv1.MachinePhaseRunning), NodeRef: &corev1.ObjectReference{Name: "n2"}}},
				{ObjectMeta: metav1.ObjectMeta{Namespace: "fleet-local", Name: "m3"}, Status: clusterv1.MachineStatus{Phase: string(clusterv1.MachinePhaseRunning), NodeRef: &corev1.ObjectReference{Name: "n3"}}},
			}
			degradedVol := &lhv1beta2.Volume{
				ObjectMeta: metav1.ObjectMeta{Namespace: "longhorn-system", Name: "vol-1"},
				Spec:       lhv1beta2.VolumeSpec{NumberOfReplicas: 3},
				Status:     lhv1beta2.VolumeStatus{Robustness: lhv1beta2.VolumeRobustnessDegraded},
			}
			builder := fake.NewClientBuilder().WithScheme(scheme).WithObjects(version, cluster, degradedVol)
			for _, n := range nodes {
				builder = builder.WithObjects(n)
			}
			for _, m := range machines {
				builder = builder.WithObjects(m)
			}
			validator := UpgradePlanCustomValidator{Client: builder.Build()}
			obj := &managementv1beta1.UpgradePlan{
				ObjectMeta: metav1.ObjectMeta{Name: "upgrade-1"},
				Spec:       managementv1beta1.UpgradePlanSpec{Version: "v1.4.0"},
			}

			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("degraded volumes"))
		})

		It("should allow degraded volumes on 2-node clusters", func() {
			version := &managementv1beta1.Version{
				ObjectMeta: metav1.ObjectMeta{Name: "v1.4.0"},
				Spec:       managementv1beta1.VersionSpec{ISODownloadURL: "https://example.com/iso"},
			}
			cluster := &provisioningv1.Cluster{
				ObjectMeta: metav1.ObjectMeta{Namespace: "fleet-local", Name: "local"},
				Status:     provisioningv1.ClusterStatus{Ready: true},
			}
			node1 := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n1", Labels: map[string]string{"harvesterhci.io/managed": "true"}, Annotations: map[string]string{"cluster.x-k8s.io/machine": "m1"}}, Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}}}
			node2 := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n2", Labels: map[string]string{"harvesterhci.io/managed": "true"}, Annotations: map[string]string{"cluster.x-k8s.io/machine": "m2"}}, Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}}}
			machine1 := &clusterv1.Machine{ObjectMeta: metav1.ObjectMeta{Namespace: "fleet-local", Name: "m1"}, Status: clusterv1.MachineStatus{Phase: string(clusterv1.MachinePhaseRunning), NodeRef: &corev1.ObjectReference{Name: "n1"}}}
			machine2 := &clusterv1.Machine{ObjectMeta: metav1.ObjectMeta{Namespace: "fleet-local", Name: "m2"}, Status: clusterv1.MachineStatus{Phase: string(clusterv1.MachinePhaseRunning), NodeRef: &corev1.ObjectReference{Name: "n2"}}}
			degradedVol := &lhv1beta2.Volume{
				ObjectMeta: metav1.ObjectMeta{Namespace: "longhorn-system", Name: "vol-1"},
				Spec:       lhv1beta2.VolumeSpec{NumberOfReplicas: 2},
				Status:     lhv1beta2.VolumeStatus{Robustness: lhv1beta2.VolumeRobustnessDegraded},
			}
			validator := UpgradePlanCustomValidator{
				Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(version, cluster, node1, node2, machine1, machine2, degradedVol).Build(),
			}
			obj := &managementv1beta1.UpgradePlan{
				ObjectMeta: metav1.ObjectMeta{Name: "upgrade-1"},
				Spec:       managementv1beta1.UpgradePlanSpec{Version: "v1.4.0"},
			}

			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should reject active single-replica volumes", func() {
			version := &managementv1beta1.Version{
				ObjectMeta: metav1.ObjectMeta{Name: "v1.4.0"},
				Spec:       managementv1beta1.VersionSpec{ISODownloadURL: "https://example.com/iso"},
			}
			cluster := &provisioningv1.Cluster{
				ObjectMeta: metav1.ObjectMeta{Namespace: "fleet-local", Name: "local"},
				Status:     provisioningv1.ClusterStatus{Ready: true},
			}
			node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n1", Labels: map[string]string{"harvesterhci.io/managed": "true"}, Annotations: map[string]string{"cluster.x-k8s.io/machine": "m1"}}, Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}}}
			machine := &clusterv1.Machine{ObjectMeta: metav1.ObjectMeta{Namespace: "fleet-local", Name: "m1"}, Status: clusterv1.MachineStatus{Phase: string(clusterv1.MachinePhaseRunning), NodeRef: &corev1.ObjectReference{Name: "n1"}}}
			vol := &lhv1beta2.Volume{
				ObjectMeta: metav1.ObjectMeta{Namespace: "longhorn-system", Name: "vol-1"},
				Spec:       lhv1beta2.VolumeSpec{NumberOfReplicas: 1},
				Status: lhv1beta2.VolumeStatus{
					State:      lhv1beta2.VolumeStateAttached,
					Robustness: lhv1beta2.VolumeRobustnessHealthy,
					KubernetesStatus: lhv1beta2.KubernetesStatus{
						Namespace: "default",
						PVCName:   "my-pvc",
					},
				},
			}
			validator := UpgradePlanCustomValidator{
				Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(version, cluster, node, machine, vol).Build(),
			}
			obj := &managementv1beta1.UpgradePlan{
				ObjectMeta: metav1.ObjectMeta{Name: "upgrade-1"},
				Spec:       managementv1beta1.UpgradePlanSpec{Version: "v1.4.0"},
			}

			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("active single-replica volumes"))
			Expect(err.Error()).To(ContainSubstring("default/my-pvc"))
		})

		It("should reject detached single-replica volumes by default", func() {
			version := &managementv1beta1.Version{
				ObjectMeta: metav1.ObjectMeta{Name: "v1.4.0"},
				Spec:       managementv1beta1.VersionSpec{ISODownloadURL: "https://example.com/iso"},
			}
			cluster := &provisioningv1.Cluster{
				ObjectMeta: metav1.ObjectMeta{Namespace: "fleet-local", Name: "local"},
				Status:     provisioningv1.ClusterStatus{Ready: true},
			}
			node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n1", Labels: map[string]string{"harvesterhci.io/managed": "true"}, Annotations: map[string]string{"cluster.x-k8s.io/machine": "m1"}}, Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}}}
			machine := &clusterv1.Machine{ObjectMeta: metav1.ObjectMeta{Namespace: "fleet-local", Name: "m1"}, Status: clusterv1.MachineStatus{Phase: string(clusterv1.MachinePhaseRunning), NodeRef: &corev1.ObjectReference{Name: "n1"}}}
			vol := &lhv1beta2.Volume{
				ObjectMeta: metav1.ObjectMeta{Namespace: "longhorn-system", Name: "vol-1"},
				Spec:       lhv1beta2.VolumeSpec{NumberOfReplicas: 1},
				Status: lhv1beta2.VolumeStatus{
					State:      lhv1beta2.VolumeStateDetached,
					Robustness: lhv1beta2.VolumeRobustnessUnknown,
					KubernetesStatus: lhv1beta2.KubernetesStatus{
						Namespace: "default",
						PVCName:   "detached-pvc",
					},
				},
			}
			validator := UpgradePlanCustomValidator{
				Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(version, cluster, node, machine, vol).Build(),
			}
			obj := &managementv1beta1.UpgradePlan{
				ObjectMeta: metav1.ObjectMeta{Name: "upgrade-1"},
				Spec:       managementv1beta1.UpgradePlanSpec{Version: "v1.4.0"},
			}

			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("single-replica volumes found"))
			Expect(err.Error()).To(ContainSubstring("default/detached-pvc"))
		})

		It("should allow detached single-replica volumes when skip annotation is set", func() {
			version := &managementv1beta1.Version{
				ObjectMeta: metav1.ObjectMeta{Name: "v1.4.0"},
				Spec:       managementv1beta1.VersionSpec{ISODownloadURL: "https://example.com/iso"},
			}
			cluster := &provisioningv1.Cluster{
				ObjectMeta: metav1.ObjectMeta{Namespace: "fleet-local", Name: "local"},
				Status:     provisioningv1.ClusterStatus{Ready: true},
			}
			node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n1", Labels: map[string]string{"harvesterhci.io/managed": "true"}, Annotations: map[string]string{"cluster.x-k8s.io/machine": "m1"}}, Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}}}
			machine := &clusterv1.Machine{ObjectMeta: metav1.ObjectMeta{Namespace: "fleet-local", Name: "m1"}, Status: clusterv1.MachineStatus{Phase: string(clusterv1.MachinePhaseRunning), NodeRef: &corev1.ObjectReference{Name: "n1"}}}
			vol := &lhv1beta2.Volume{
				ObjectMeta: metav1.ObjectMeta{Namespace: "longhorn-system", Name: "vol-1"},
				Spec:       lhv1beta2.VolumeSpec{NumberOfReplicas: 1},
				Status: lhv1beta2.VolumeStatus{
					State:      lhv1beta2.VolumeStateDetached,
					Robustness: lhv1beta2.VolumeRobustnessUnknown,
					KubernetesStatus: lhv1beta2.KubernetesStatus{
						Namespace: "default",
						PVCName:   "detached-pvc",
					},
				},
			}
			validator := UpgradePlanCustomValidator{
				Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(version, cluster, node, machine, vol).Build(),
			}
			obj := &managementv1beta1.UpgradePlan{
				ObjectMeta: metav1.ObjectMeta{
					Name: "upgrade-1",
					Annotations: map[string]string{
						upgradeplan.AnnotationSkipSingleReplicaDetachedVol: "true",
					},
				},
				Spec: managementv1beta1.UpgradePlanSpec{Version: "v1.4.0"},
			}

			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should allow when all volumes are healthy multi-replica", func() {
			version := &managementv1beta1.Version{
				ObjectMeta: metav1.ObjectMeta{Name: "v1.4.0"},
				Spec:       managementv1beta1.VersionSpec{ISODownloadURL: "https://example.com/iso"},
			}
			cluster := &provisioningv1.Cluster{
				ObjectMeta: metav1.ObjectMeta{Namespace: "fleet-local", Name: "local"},
				Status:     provisioningv1.ClusterStatus{Ready: true},
			}
			node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n1", Labels: map[string]string{"harvesterhci.io/managed": "true"}, Annotations: map[string]string{"cluster.x-k8s.io/machine": "m1"}}, Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}}}
			machine := &clusterv1.Machine{ObjectMeta: metav1.ObjectMeta{Namespace: "fleet-local", Name: "m1"}, Status: clusterv1.MachineStatus{Phase: string(clusterv1.MachinePhaseRunning), NodeRef: &corev1.ObjectReference{Name: "n1"}}}
			vol := &lhv1beta2.Volume{
				ObjectMeta: metav1.ObjectMeta{Namespace: "longhorn-system", Name: "vol-1"},
				Spec:       lhv1beta2.VolumeSpec{NumberOfReplicas: 3},
				Status: lhv1beta2.VolumeStatus{
					State:      lhv1beta2.VolumeStateAttached,
					Robustness: lhv1beta2.VolumeRobustnessHealthy,
				},
			}
			validator := UpgradePlanCustomValidator{
				Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(version, cluster, node, machine, vol).Build(),
			}
			obj := &managementv1beta1.UpgradePlan{
				ObjectMeta: metav1.ObjectMeta{Name: "upgrade-1"},
				Spec:       managementv1beta1.UpgradePlanSpec{Version: "v1.4.0"},
			}

			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Context("validateVMBackups", func() {
		It("should reject when a VM backup is in progress", func() {
			version := &managementv1beta1.Version{
				ObjectMeta: metav1.ObjectMeta{Name: "v1.4.0"},
				Spec:       managementv1beta1.VersionSpec{ISODownloadURL: "https://example.com/iso"},
			}
			cluster := &provisioningv1.Cluster{
				ObjectMeta: metav1.ObjectMeta{Namespace: "fleet-local", Name: "local"},
				Status:     provisioningv1.ClusterStatus{Ready: true},
			}
			node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n1", Labels: map[string]string{"harvesterhci.io/managed": "true"}, Annotations: map[string]string{"cluster.x-k8s.io/machine": "m1"}}, Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}}}
			machine := &clusterv1.Machine{ObjectMeta: metav1.ObjectMeta{Namespace: "fleet-local", Name: "m1"}, Status: clusterv1.MachineStatus{Phase: string(clusterv1.MachinePhaseRunning), NodeRef: &corev1.ObjectReference{Name: "n1"}}}
			backup := &harvesterv1beta1.VirtualMachineBackup{
				ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "backup-1"},
				Spec: harvesterv1beta1.VirtualMachineBackupSpec{
					Source: corev1.TypedLocalObjectReference{
						APIGroup: ptr.To("kubevirt.io"),
						Kind:     "VirtualMachine",
						Name:     "vm-1",
					},
				},
				Status: harvesterv1beta1.VirtualMachineBackupStatus{
					ReadyToUse: ptr.To(false),
				},
			}
			validator := UpgradePlanCustomValidator{
				Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(version, cluster, node, machine, backup).Build(),
			}
			obj := &managementv1beta1.UpgradePlan{
				ObjectMeta: metav1.ObjectMeta{Name: "upgrade-1"},
				Spec:       managementv1beta1.UpgradePlanSpec{Version: "v1.4.0"},
			}

			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("vmbackups are stopped"))
			Expect(err.Error()).To(ContainSubstring("default/backup-1"))
		})

		It("should allow when VM backup has error even if not ready", func() {
			version := &managementv1beta1.Version{
				ObjectMeta: metav1.ObjectMeta{Name: "v1.4.0"},
				Spec:       managementv1beta1.VersionSpec{ISODownloadURL: "https://example.com/iso"},
			}
			cluster := &provisioningv1.Cluster{
				ObjectMeta: metav1.ObjectMeta{Namespace: "fleet-local", Name: "local"},
				Status:     provisioningv1.ClusterStatus{Ready: true},
			}
			node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n1", Labels: map[string]string{"harvesterhci.io/managed": "true"}, Annotations: map[string]string{"cluster.x-k8s.io/machine": "m1"}}, Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}}}
			machine := &clusterv1.Machine{ObjectMeta: metav1.ObjectMeta{Namespace: "fleet-local", Name: "m1"}, Status: clusterv1.MachineStatus{Phase: string(clusterv1.MachinePhaseRunning), NodeRef: &corev1.ObjectReference{Name: "n1"}}}
			errMsg := "backup failed"
			backup := &harvesterv1beta1.VirtualMachineBackup{
				ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "backup-1"},
				Spec: harvesterv1beta1.VirtualMachineBackupSpec{
					Source: corev1.TypedLocalObjectReference{
						APIGroup: ptr.To("kubevirt.io"),
						Kind:     "VirtualMachine",
						Name:     "vm-1",
					},
				},
				Status: harvesterv1beta1.VirtualMachineBackupStatus{
					ReadyToUse: ptr.To(false),
					Error: &harvesterv1beta1.Error{
						Message: &errMsg,
					},
				},
			}
			validator := UpgradePlanCustomValidator{
				Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(version, cluster, node, machine, backup).Build(),
			}
			obj := &managementv1beta1.UpgradePlan{
				ObjectMeta: metav1.ObjectMeta{Name: "upgrade-1"},
				Spec:       managementv1beta1.UpgradePlanSpec{Version: "v1.4.0"},
			}

			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should allow when all VM backups are ready", func() {
			version := &managementv1beta1.Version{
				ObjectMeta: metav1.ObjectMeta{Name: "v1.4.0"},
				Spec:       managementv1beta1.VersionSpec{ISODownloadURL: "https://example.com/iso"},
			}
			cluster := &provisioningv1.Cluster{
				ObjectMeta: metav1.ObjectMeta{Namespace: "fleet-local", Name: "local"},
				Status:     provisioningv1.ClusterStatus{Ready: true},
			}
			node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n1", Labels: map[string]string{"harvesterhci.io/managed": "true"}, Annotations: map[string]string{"cluster.x-k8s.io/machine": "m1"}}, Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}}}
			machine := &clusterv1.Machine{ObjectMeta: metav1.ObjectMeta{Namespace: "fleet-local", Name: "m1"}, Status: clusterv1.MachineStatus{Phase: string(clusterv1.MachinePhaseRunning), NodeRef: &corev1.ObjectReference{Name: "n1"}}}
			backup := &harvesterv1beta1.VirtualMachineBackup{
				ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "backup-1"},
				Spec: harvesterv1beta1.VirtualMachineBackupSpec{
					Source: corev1.TypedLocalObjectReference{
						APIGroup: ptr.To("kubevirt.io"),
						Kind:     "VirtualMachine",
						Name:     "vm-1",
					},
				},
				Status: harvesterv1beta1.VirtualMachineBackupStatus{
					ReadyToUse: ptr.To(true),
				},
			}
			validator := UpgradePlanCustomValidator{
				Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(version, cluster, node, machine, backup).Build(),
			}
			obj := &managementv1beta1.UpgradePlan{
				ObjectMeta: metav1.ObjectMeta{Name: "upgrade-1"},
				Spec:       managementv1beta1.UpgradePlanSpec{Version: "v1.4.0"},
			}

			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Context("validateScheduleVMBackups", func() {
		It("should reject when a schedule is not suspended", func() {
			version := &managementv1beta1.Version{
				ObjectMeta: metav1.ObjectMeta{Name: "v1.4.0"},
				Spec:       managementv1beta1.VersionSpec{ISODownloadURL: "https://example.com/iso"},
			}
			cluster := &provisioningv1.Cluster{
				ObjectMeta: metav1.ObjectMeta{Namespace: "fleet-local", Name: "local"},
				Status:     provisioningv1.ClusterStatus{Ready: true},
			}
			node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n1", Labels: map[string]string{"harvesterhci.io/managed": "true"}, Annotations: map[string]string{"cluster.x-k8s.io/machine": "m1"}}, Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}}}
			machine := &clusterv1.Machine{ObjectMeta: metav1.ObjectMeta{Namespace: "fleet-local", Name: "m1"}, Status: clusterv1.MachineStatus{Phase: string(clusterv1.MachinePhaseRunning), NodeRef: &corev1.ObjectReference{Name: "n1"}}}
			schedule := &harvesterv1beta1.ScheduleVMBackup{
				ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "schedule-1"},
				Spec: harvesterv1beta1.ScheduleVMBackupSpec{
					Cron:       "0 * * * *",
					Retain:     3,
					MaxFailure: 1,
					VMBackupSpec: harvesterv1beta1.VirtualMachineBackupSpec{
						Source: corev1.TypedLocalObjectReference{
							APIGroup: ptr.To("kubevirt.io"),
							Kind:     "VirtualMachine",
							Name:     "vm-1",
						},
					},
				},
				Status: harvesterv1beta1.ScheduleVMBackupStatus{
					Suspended: false,
				},
			}
			validator := UpgradePlanCustomValidator{
				Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(version, cluster, node, machine, schedule).Build(),
			}
			obj := &managementv1beta1.UpgradePlan{
				ObjectMeta: metav1.ObjectMeta{Name: "upgrade-1"},
				Spec:       managementv1beta1.UpgradePlanSpec{Version: "v1.4.0"},
			}

			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("suspend all backup/snapshot schedule"))
			Expect(err.Error()).To(ContainSubstring("default/schedule-1"))
		})

		It("should allow when all schedules are suspended", func() {
			version := &managementv1beta1.Version{
				ObjectMeta: metav1.ObjectMeta{Name: "v1.4.0"},
				Spec:       managementv1beta1.VersionSpec{ISODownloadURL: "https://example.com/iso"},
			}
			cluster := &provisioningv1.Cluster{
				ObjectMeta: metav1.ObjectMeta{Namespace: "fleet-local", Name: "local"},
				Status:     provisioningv1.ClusterStatus{Ready: true},
			}
			node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n1", Labels: map[string]string{"harvesterhci.io/managed": "true"}, Annotations: map[string]string{"cluster.x-k8s.io/machine": "m1"}}, Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}}}
			machine := &clusterv1.Machine{ObjectMeta: metav1.ObjectMeta{Namespace: "fleet-local", Name: "m1"}, Status: clusterv1.MachineStatus{Phase: string(clusterv1.MachinePhaseRunning), NodeRef: &corev1.ObjectReference{Name: "n1"}}}
			schedule := &harvesterv1beta1.ScheduleVMBackup{
				ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "schedule-1"},
				Spec: harvesterv1beta1.ScheduleVMBackupSpec{
					Cron:       "0 * * * *",
					Retain:     3,
					MaxFailure: 1,
					Suspend:    true,
					VMBackupSpec: harvesterv1beta1.VirtualMachineBackupSpec{
						Source: corev1.TypedLocalObjectReference{
							APIGroup: ptr.To("kubevirt.io"),
							Kind:     "VirtualMachine",
							Name:     "vm-1",
						},
					},
				},
				Status: harvesterv1beta1.ScheduleVMBackupStatus{
					Suspended: true,
				},
			}
			validator := UpgradePlanCustomValidator{
				Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(version, cluster, node, machine, schedule).Build(),
			}
			obj := &managementv1beta1.UpgradePlan{
				ObjectMeta: metav1.ObjectMeta{Name: "upgrade-1"},
				Spec:       managementv1beta1.UpgradePlanSpec{Version: "v1.4.0"},
			}

			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Context("validateNonLiveMigratableVMs", func() {
		It("should reject when non-migratable VMI exists on multi-node cluster", func() {
			version := &managementv1beta1.Version{
				ObjectMeta: metav1.ObjectMeta{Name: "v1.4.0"},
				Spec:       managementv1beta1.VersionSpec{ISODownloadURL: "https://example.com/iso"},
			}
			cluster := &provisioningv1.Cluster{
				ObjectMeta: metav1.ObjectMeta{Namespace: "fleet-local", Name: "local"},
				Status:     provisioningv1.ClusterStatus{Ready: true},
			}
			node1 := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n1", Labels: map[string]string{"harvesterhci.io/managed": "true"}, Annotations: map[string]string{"cluster.x-k8s.io/machine": "m1"}}, Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}}}
			node2 := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n2", Labels: map[string]string{"harvesterhci.io/managed": "true"}, Annotations: map[string]string{"cluster.x-k8s.io/machine": "m2"}}, Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}}}
			machine1 := &clusterv1.Machine{ObjectMeta: metav1.ObjectMeta{Namespace: "fleet-local", Name: "m1"}, Status: clusterv1.MachineStatus{Phase: string(clusterv1.MachinePhaseRunning), NodeRef: &corev1.ObjectReference{Name: "n1"}}}
			machine2 := &clusterv1.Machine{ObjectMeta: metav1.ObjectMeta{Namespace: "fleet-local", Name: "m2"}, Status: clusterv1.MachineStatus{Phase: string(clusterv1.MachinePhaseRunning), NodeRef: &corev1.ObjectReference{Name: "n2"}}}
			// VMI with node selector is non-migratable
			vmi := &kubevirtv1.VirtualMachineInstance{
				ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "vm-1"},
				Spec: kubevirtv1.VirtualMachineInstanceSpec{
					NodeSelector: map[string]string{"kubernetes.io/hostname": "n1"},
				},
			}
			validator := UpgradePlanCustomValidator{
				Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(version, cluster, node1, node2, machine1, machine2, vmi).Build(),
			}
			obj := &managementv1beta1.UpgradePlan{
				ObjectMeta: metav1.ObjectMeta{Name: "upgrade-1"},
				Spec:       managementv1beta1.UpgradePlanSpec{Version: "v1.4.0"},
			}

			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("non-live migratable VMs"))
			Expect(err.Error()).To(ContainSubstring("default/vm-1"))
		})

		It("should skip check on single-node cluster", func() {
			version := &managementv1beta1.Version{
				ObjectMeta: metav1.ObjectMeta{Name: "v1.4.0"},
				Spec:       managementv1beta1.VersionSpec{ISODownloadURL: "https://example.com/iso"},
			}
			cluster := &provisioningv1.Cluster{
				ObjectMeta: metav1.ObjectMeta{Namespace: "fleet-local", Name: "local"},
				Status:     provisioningv1.ClusterStatus{Ready: true},
			}
			node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n1", Labels: map[string]string{"harvesterhci.io/managed": "true"}, Annotations: map[string]string{"cluster.x-k8s.io/machine": "m1"}}, Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}}}
			machine := &clusterv1.Machine{ObjectMeta: metav1.ObjectMeta{Namespace: "fleet-local", Name: "m1"}, Status: clusterv1.MachineStatus{Phase: string(clusterv1.MachinePhaseRunning), NodeRef: &corev1.ObjectReference{Name: "n1"}}}
			// Non-migratable VMI - but single node, so check is skipped
			vmi := &kubevirtv1.VirtualMachineInstance{
				ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "vm-1"},
				Spec: kubevirtv1.VirtualMachineInstanceSpec{
					NodeSelector: map[string]string{"kubernetes.io/hostname": "n1"},
				},
			}
			validator := UpgradePlanCustomValidator{
				Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(version, cluster, node, machine, vmi).Build(),
			}
			obj := &managementv1beta1.UpgradePlan{
				ObjectMeta: metav1.ObjectMeta{Name: "upgrade-1"},
				Spec:       managementv1beta1.UpgradePlanSpec{Version: "v1.4.0"},
			}

			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should allow when all VMIs are migratable on multi-node cluster", func() {
			version := &managementv1beta1.Version{
				ObjectMeta: metav1.ObjectMeta{Name: "v1.4.0"},
				Spec:       managementv1beta1.VersionSpec{ISODownloadURL: "https://example.com/iso"},
			}
			cluster := &provisioningv1.Cluster{
				ObjectMeta: metav1.ObjectMeta{Namespace: "fleet-local", Name: "local"},
				Status:     provisioningv1.ClusterStatus{Ready: true},
			}
			node1 := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n1", Labels: map[string]string{"harvesterhci.io/managed": "true"}, Annotations: map[string]string{"cluster.x-k8s.io/machine": "m1"}}, Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}}}
			node2 := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n2", Labels: map[string]string{"harvesterhci.io/managed": "true"}, Annotations: map[string]string{"cluster.x-k8s.io/machine": "m2"}}, Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}}}
			machine1 := &clusterv1.Machine{ObjectMeta: metav1.ObjectMeta{Namespace: "fleet-local", Name: "m1"}, Status: clusterv1.MachineStatus{Phase: string(clusterv1.MachinePhaseRunning), NodeRef: &corev1.ObjectReference{Name: "n1"}}}
			machine2 := &clusterv1.Machine{ObjectMeta: metav1.ObjectMeta{Namespace: "fleet-local", Name: "m2"}, Status: clusterv1.MachineStatus{Phase: string(clusterv1.MachinePhaseRunning), NodeRef: &corev1.ObjectReference{Name: "n2"}}}
			// VMI with no node selector, no host devices, no strict affinity -> migratable
			vmi := &kubevirtv1.VirtualMachineInstance{
				ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "vm-1"},
				Spec:       kubevirtv1.VirtualMachineInstanceSpec{},
			}
			validator := UpgradePlanCustomValidator{
				Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(version, cluster, node1, node2, machine1, machine2, vmi).Build(),
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
			node := &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "node-1",
					Labels:      map[string]string{"harvesterhci.io/managed": "true"},
					Annotations: map[string]string{"cluster.x-k8s.io/machine": "machine-1"},
				},
				Status: corev1.NodeStatus{
					Conditions: []corev1.NodeCondition{
						{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
					},
				},
			}
			machine := &clusterv1.Machine{
				ObjectMeta: metav1.ObjectMeta{Namespace: "fleet-local", Name: "machine-1"},
				Status: clusterv1.MachineStatus{
					Phase:   string(clusterv1.MachinePhaseRunning),
					NodeRef: &corev1.ObjectReference{Name: "node-1"},
				},
			}
			validator := UpgradePlanCustomValidator{
				Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(version, cluster, succeeded, node, machine).Build(),
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
