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
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	managementv1beta1 "github.com/harvester/upgrade-toolkit/api/v1beta1"
	"github.com/harvester/upgrade-toolkit/pkg/upgradehelper/vmlivemigratedetector"
	"github.com/harvester/upgrade-toolkit/pkg/upgradeplan"
)

var _ = Describe("Node Controller", func() {
	const (
		upgradePlanName  = "test-upgrade-node"
		testNodeName     = "test-node-reboot"
		testVersion      = "test-version"
		testOSVersion    = "test-os-version"
		testOldOSVersion = "test-old-os-version"
	)

	var (
		reconciler *NodeReconciler
	)

	BeforeEach(func() {
		reconciler = &NodeReconciler{
			Client:            k8sClient,
			Scheme:            k8sClient.Scheme(),
			JobServiceAccount: "harvester",
		}

		// Ensure required namespace exists
		ns := &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: "harvester-system"},
		}
		_ = k8sClient.Create(ctx, ns)
	})

	AfterEach(func() {
		// Clean up UpgradePlans
		upList := &managementv1beta1.UpgradePlanList{}
		_ = k8sClient.List(ctx, upList)
		for i := range upList.Items {
			_ = k8sClient.Delete(ctx, &upList.Items[i])
		}

		// Clean up test nodes
		var node corev1.Node
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: testNodeName}, &node); err == nil {
			_ = k8sClient.Delete(ctx, &node)
		}

		// Clean up Jobs in harvester-system
		jobList := &batchv1.JobList{}
		_ = k8sClient.List(ctx, jobList, client.InNamespace("harvester-system"))
		for i := range jobList.Items {
			_ = k8sClient.Delete(ctx, &jobList.Items[i], client.PropagationPolicy(metav1.DeletePropagationBackground))
		}

		// Clean up ConfigMaps in harvester-system
		cmList := &corev1.ConfigMapList{}
		_ = k8sClient.List(ctx, cmList, client.InNamespace("harvester-system"))
		for i := range cmList.Items {
			_ = k8sClient.Delete(ctx, &cmList.Items[i])
		}
	})

	createUpgradePlan := func(nodeState managementv1beta1.NodeUpgradeState) *managementv1beta1.UpgradePlan {
		up := &managementv1beta1.UpgradePlan{
			ObjectMeta: metav1.ObjectMeta{
				Name: upgradePlanName,
			},
			Spec: managementv1beta1.UpgradePlanSpec{
				Version: testVersion,
			},
		}
		Expect(k8sClient.Create(ctx, up)).To(Succeed())

		up.Status.CurrentPhase = managementv1beta1.UpgradePlanPhaseNodeUpgrading
		up.Status.NodeUpgradeStatuses = map[string]managementv1beta1.NodeUpgradeStatus{
			testNodeName: {State: nodeState},
		}
		up.Status.ReleaseMetadata = &managementv1beta1.ReleaseMetadata{
			OS: testOSVersion,
		}
		Expect(k8sClient.Status().Update(ctx, up)).To(Succeed())

		return up
	}

	createUpgradePlanWithRestoreVM := func(nodeState managementv1beta1.NodeUpgradeState, restoreVM *bool, singleNode bool) *managementv1beta1.UpgradePlan {
		up := &managementv1beta1.UpgradePlan{
			ObjectMeta: metav1.ObjectMeta{
				Name: upgradePlanName,
			},
			Spec: managementv1beta1.UpgradePlanSpec{
				Version:   testVersion,
				RestoreVM: restoreVM,
			},
		}
		Expect(k8sClient.Create(ctx, up)).To(Succeed())

		up.Status.CurrentPhase = managementv1beta1.UpgradePlanPhaseNodeUpgrading
		up.Status.NodeUpgradeStatuses = map[string]managementv1beta1.NodeUpgradeStatus{
			testNodeName: {State: nodeState},
		}
		up.Status.ReleaseMetadata = &managementv1beta1.ReleaseMetadata{
			OS: testOSVersion,
		}
		if singleNode {
			up.Status.SingleNode = ptr.To(testNodeName)
		}
		Expect(k8sClient.Status().Update(ctx, up)).To(Succeed())

		return up
	}

	createRestoreVMConfigMap := func(nodeName, vmNames string) {
		cmName := vmlivemigratedetector.GetRestoreVMConfigMapName(upgradePlanName)
		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      cmName,
				Namespace: "harvester-system",
			},
			Data: map[string]string{
				nodeName: vmNames,
			},
		}
		Expect(k8sClient.Create(ctx, cm)).To(Succeed())
	}

	listRestoreVMJobs := func() []batchv1.Job {
		jobList := &batchv1.JobList{}
		Expect(k8sClient.List(ctx, jobList, client.InNamespace("harvester-system"),
			client.MatchingLabels{upgradeplan.HarvesterUpgradeComponentLabel: upgradeplan.NodeComponent},
		)).To(Succeed())
		return jobList.Items
	}

	createNode := func(osImage string, annotations map[string]string) *corev1.Node {
		node := &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name:        testNodeName,
				Annotations: annotations,
			},
		}
		Expect(k8sClient.Create(ctx, node)).To(Succeed())

		// NodeSystemInfo is in status, which must be updated via status subresource
		node.Status.NodeInfo = corev1.NodeSystemInfo{
			OSImage: osImage,
		}
		Expect(k8sClient.Status().Update(ctx, node)).To(Succeed())

		return node
	}

	reconcileNode := func() (ctrl.Result, error) {
		return reconciler.Reconcile(ctx, ctrl.Request{
			NamespacedName: types.NamespacedName{Name: testNodeName},
		})
	}

	Context("When the node has no PendingOSImage annotation", func() {
		It("should return early without changes", func() {
			createUpgradePlan(managementv1beta1.NodeStateWaitingReboot)
			createNode(testOldOSVersion, nil)

			result, err := reconcileNode()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(ctrl.Result{}))

			// UpgradePlan should be unchanged
			up := &managementv1beta1.UpgradePlan{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: upgradePlanName}, up)).To(Succeed())
			Expect(up.Status.NodeUpgradeStatuses[testNodeName].State).To(Equal(managementv1beta1.NodeStateWaitingReboot))
		})
	})

	Context("When the node has PendingOSImage annotation but OS does not match", func() {
		It("should return early without changes", func() {
			createUpgradePlan(managementv1beta1.NodeStateWaitingReboot)
			createNode(testOldOSVersion, map[string]string{
				upgradeplan.PendingOSImageAnnotation: testOSVersion,
			})

			result, err := reconcileNode()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(ctrl.Result{}))

			// UpgradePlan should be unchanged
			up := &managementv1beta1.UpgradePlan{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: upgradePlanName}, up)).To(Succeed())
			Expect(up.Status.NodeUpgradeStatuses[testNodeName].State).To(Equal(managementv1beta1.NodeStateWaitingReboot))

			// Annotation should still be present
			node := &corev1.Node{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: testNodeName}, node)).To(Succeed())
			Expect(node.Annotations).To(HaveKey(upgradeplan.PendingOSImageAnnotation))
		})
	})

	Context("When the node has PendingOSImage annotation and OS matches", func() {
		It("should transition node to PostDrained and remove the annotation", func() {
			createUpgradePlan(managementv1beta1.NodeStateWaitingReboot)
			createNode(testOSVersion, map[string]string{
				upgradeplan.PendingOSImageAnnotation: testOSVersion,
			})

			result, err := reconcileNode()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(ctrl.Result{}))

			// UpgradePlan node state should be PostDrained
			up := &managementv1beta1.UpgradePlan{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: upgradePlanName}, up)).To(Succeed())
			Expect(up.Status.NodeUpgradeStatuses[testNodeName].State).To(Equal(managementv1beta1.NodeStatePostDrained))

			// PendingOSImage annotation should be removed
			node := &corev1.Node{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: testNodeName}, node)).To(Succeed())
			Expect(node.Annotations).NotTo(HaveKey(upgradeplan.PendingOSImageAnnotation))
		})
	})

	Context("When there is no active UpgradePlan in NodeUpgrading phase", func() {
		It("should return early without changes", func() {
			// Create an UpgradePlan in a different phase
			up := &managementv1beta1.UpgradePlan{
				ObjectMeta: metav1.ObjectMeta{
					Name: upgradePlanName,
				},
				Spec: managementv1beta1.UpgradePlanSpec{
					Version: testVersion,
				},
			}
			Expect(k8sClient.Create(ctx, up)).To(Succeed())
			up.Status.CurrentPhase = managementv1beta1.UpgradePlanPhaseClusterUpgrading
			Expect(k8sClient.Status().Update(ctx, up)).To(Succeed())

			createNode(testOSVersion, map[string]string{
				upgradeplan.PendingOSImageAnnotation: testOSVersion,
			})

			result, err := reconcileNode()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(ctrl.Result{}))

			// Annotation should still be present
			node := &corev1.Node{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: testNodeName}, node)).To(Succeed())
			Expect(node.Annotations).To(HaveKey(upgradeplan.PendingOSImageAnnotation))
		})
	})

	Context("When the node is not in WaitingReboot state", func() {
		It("should return early without changes", func() {
			createUpgradePlan(managementv1beta1.NodeStatePostDraining)
			createNode(testOSVersion, map[string]string{
				upgradeplan.PendingOSImageAnnotation: testOSVersion,
			})

			result, err := reconcileNode()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(ctrl.Result{}))

			// State should remain PostDraining
			up := &managementv1beta1.UpgradePlan{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: upgradePlanName}, up)).To(Succeed())
			Expect(up.Status.NodeUpgradeStatuses[testNodeName].State).To(Equal(managementv1beta1.NodeStatePostDraining))
		})
	})

	Context("When the UpgradePlan is for a single-node cluster and OS matches", func() {
		It("should transition node to SingleNodeUpgraded instead of PostDrained", func() {
			up := &managementv1beta1.UpgradePlan{
				ObjectMeta: metav1.ObjectMeta{
					Name: upgradePlanName,
				},
				Spec: managementv1beta1.UpgradePlanSpec{
					Version: testVersion,
				},
			}
			Expect(k8sClient.Create(ctx, up)).To(Succeed())

			up.Status.CurrentPhase = managementv1beta1.UpgradePlanPhaseNodeUpgrading
			up.Status.SingleNode = ptr.To(testNodeName)
			up.Status.NodeUpgradeStatuses = map[string]managementv1beta1.NodeUpgradeStatus{
				testNodeName: {State: managementv1beta1.NodeStateWaitingReboot},
			}
			up.Status.ReleaseMetadata = &managementv1beta1.ReleaseMetadata{
				OS: testOSVersion,
			}
			Expect(k8sClient.Status().Update(ctx, up)).To(Succeed())

			createNode(testOSVersion, map[string]string{
				upgradeplan.PendingOSImageAnnotation: testOSVersion,
			})

			result, err := reconcileNode()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(ctrl.Result{}))

			// UpgradePlan node state should be SingleNodeUpgraded
			fresh := &managementv1beta1.UpgradePlan{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: upgradePlanName}, fresh)).To(Succeed())
			Expect(fresh.Status.NodeUpgradeStatuses[testNodeName].State).To(Equal(managementv1beta1.NodeStateSingleNodeUpgraded))

			// PendingOSImage annotation should be removed
			node := &corev1.Node{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: testNodeName}, node)).To(Succeed())
			Expect(node.Annotations).NotTo(HaveKey(upgradeplan.PendingOSImageAnnotation))
		})
	})

	Context("When the node does not exist in NodeUpgradeStatuses", func() {
		It("should return early without changes", func() {
			// Create UpgradePlan but without this node in statuses
			up := &managementv1beta1.UpgradePlan{
				ObjectMeta: metav1.ObjectMeta{
					Name: upgradePlanName,
				},
				Spec: managementv1beta1.UpgradePlanSpec{
					Version: testVersion,
				},
			}
			Expect(k8sClient.Create(ctx, up)).To(Succeed())
			up.Status.CurrentPhase = managementv1beta1.UpgradePlanPhaseNodeUpgrading
			up.Status.NodeUpgradeStatuses = map[string]managementv1beta1.NodeUpgradeStatus{}
			Expect(k8sClient.Status().Update(ctx, up)).To(Succeed())

			createNode(testOSVersion, map[string]string{
				upgradeplan.PendingOSImageAnnotation: testOSVersion,
			})

			result, err := reconcileNode()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(ctrl.Result{}))

			// Annotation should still be present
			node := &corev1.Node{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: testNodeName}, node)).To(Succeed())
			Expect(node.Annotations).To(HaveKey(upgradeplan.PendingOSImageAnnotation))
		})
	})

	Context("Restore-VM dispatch", func() {
		It("should create restore-vm Job when restoreVM enabled and ConfigMap has VMs", func() {
			createUpgradePlanWithRestoreVM(managementv1beta1.NodeStateWaitingReboot, ptr.To(true), false)
			createRestoreVMConfigMap(testNodeName, "default/vm1,default/vm2")
			createNode(testOSVersion, map[string]string{
				upgradeplan.PendingOSImageAnnotation: testOSVersion,
			})

			result, err := reconcileNode()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(ctrl.Result{}))

			// Node should transition to PostDrained
			up := &managementv1beta1.UpgradePlan{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: upgradePlanName}, up)).To(Succeed())
			Expect(up.Status.NodeUpgradeStatuses[testNodeName].State).To(Equal(managementv1beta1.NodeStatePostDrained))

			// Restore-VM Job should be created
			jobs := listRestoreVMJobs()
			Expect(jobs).To(HaveLen(1))
			Expect(jobs[0].Labels[upgradeplan.HarvesterUpgradeComponentLabel]).To(Equal(upgradeplan.NodeComponent))
			Expect(jobs[0].Labels[upgradeplan.HarvesterJobTypeLabel]).To(Equal(upgradeplan.JobTypeRestoreVM))
		})

		It("should not create restore-vm Job when restoreVM is nil", func() {
			createUpgradePlanWithRestoreVM(managementv1beta1.NodeStateWaitingReboot, nil, false)
			createRestoreVMConfigMap(testNodeName, "default/vm1")
			createNode(testOSVersion, map[string]string{
				upgradeplan.PendingOSImageAnnotation: testOSVersion,
			})

			result, err := reconcileNode()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(ctrl.Result{}))

			jobs := listRestoreVMJobs()
			Expect(jobs).To(BeEmpty())
		})

		It("should not create restore-vm Job when restoreVM is false", func() {
			createUpgradePlanWithRestoreVM(managementv1beta1.NodeStateWaitingReboot, ptr.To(false), false)
			createRestoreVMConfigMap(testNodeName, "default/vm1")
			createNode(testOSVersion, map[string]string{
				upgradeplan.PendingOSImageAnnotation: testOSVersion,
			})

			result, err := reconcileNode()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(ctrl.Result{}))

			jobs := listRestoreVMJobs()
			Expect(jobs).To(BeEmpty())
		})

		It("should not create restore-vm Job when no ConfigMap exists", func() {
			createUpgradePlanWithRestoreVM(managementv1beta1.NodeStateWaitingReboot, ptr.To(true), false)
			// No ConfigMap created
			createNode(testOSVersion, map[string]string{
				upgradeplan.PendingOSImageAnnotation: testOSVersion,
			})

			result, err := reconcileNode()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(ctrl.Result{}))

			jobs := listRestoreVMJobs()
			Expect(jobs).To(BeEmpty())
		})

		It("should not create restore-vm Job when ConfigMap entry is empty for node", func() {
			createUpgradePlanWithRestoreVM(managementv1beta1.NodeStateWaitingReboot, ptr.To(true), false)
			createRestoreVMConfigMap(testNodeName, "")
			createNode(testOSVersion, map[string]string{
				upgradeplan.PendingOSImageAnnotation: testOSVersion,
			})

			result, err := reconcileNode()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(ctrl.Result{}))

			jobs := listRestoreVMJobs()
			Expect(jobs).To(BeEmpty())
		})

		It("should not create restore-vm Job for witness node", func() {
			createUpgradePlanWithRestoreVM(managementv1beta1.NodeStateWaitingReboot, ptr.To(true), false)
			createRestoreVMConfigMap(testNodeName, "default/vm1")

			// Create node with witness label
			node := &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: testNodeName,
					Annotations: map[string]string{
						upgradeplan.PendingOSImageAnnotation: testOSVersion,
					},
					Labels: map[string]string{
						"node-role.harvesterhci.io/witness": "true",
					},
				},
			}
			Expect(k8sClient.Create(ctx, node)).To(Succeed())
			node.Status.NodeInfo = corev1.NodeSystemInfo{OSImage: testOSVersion}
			Expect(k8sClient.Status().Update(ctx, node)).To(Succeed())

			result, err := reconcileNode()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(ctrl.Result{}))

			jobs := listRestoreVMJobs()
			Expect(jobs).To(BeEmpty())
		})

		It("should create restore-vm Job for single-node cluster", func() {
			createUpgradePlanWithRestoreVM(managementv1beta1.NodeStateWaitingReboot, ptr.To(true), true)
			createRestoreVMConfigMap(testNodeName, "default/vm1")
			createNode(testOSVersion, map[string]string{
				upgradeplan.PendingOSImageAnnotation: testOSVersion,
			})

			result, err := reconcileNode()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(ctrl.Result{}))

			// Node should transition to SingleNodeUpgraded
			up := &managementv1beta1.UpgradePlan{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: upgradePlanName}, up)).To(Succeed())
			Expect(up.Status.NodeUpgradeStatuses[testNodeName].State).To(Equal(managementv1beta1.NodeStateSingleNodeUpgraded))

			// Restore-VM Job should be created
			jobs := listRestoreVMJobs()
			Expect(jobs).To(HaveLen(1))
		})
	})
})
