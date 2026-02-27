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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	managementv1beta1 "github.com/harvester/upgrade-toolkit/api/v1beta1"
	"github.com/harvester/upgrade-toolkit/pkg/upgradeplan"
)

var _ = Describe("Job Controller", func() {
	const (
		upgradePlanName  = "test-upgrade-job"
		testNodeName     = "test-node-job"
		testVersion      = "test-version"
		testOSVersion    = "test-os-version"
		testOldOSVersion = "test-old-os-version"
	)

	var (
		reconciler *JobReconciler
	)

	BeforeEach(func() {
		reconciler = &JobReconciler{
			Client: k8sClient,
			Scheme: k8sClient.Scheme(),
			Log:    logf.Log.WithName("test-job-controller"),
		}

		// Ensure required namespace exists
		ns := &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: "harvester-system"},
		}
		_ = k8sClient.Create(ctx, ns)
	})

	AfterEach(func() {
		// Clean up Jobs in cattle-system
		jobList := &batchv1.JobList{}
		_ = k8sClient.List(ctx, jobList, client.InNamespace("harvester-system"))
		for i := range jobList.Items {
			_ = k8sClient.Delete(ctx, &jobList.Items[i], client.PropagationPolicy(metav1.DeletePropagationBackground))
		}

		// Clean up UpgradePlans
		upList := &managementv1beta1.UpgradePlanList{}
		_ = k8sClient.List(ctx, upList)
		for i := range upList.Items {
			_ = k8sClient.Delete(ctx, &upList.Items[i])
		}

		// Clean up test nodes (not system nodes)
		var node corev1.Node
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: testNodeName}, &node); err == nil {
			_ = k8sClient.Delete(ctx, &node)
		}
	})

	createUpgradePlan := func() *managementv1beta1.UpgradePlan {
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
			testNodeName: {State: managementv1beta1.NodeStateImagePreloaded},
		}
		up.Status.ReleaseMetadata = &managementv1beta1.ReleaseMetadata{
			OS: testOSVersion,
		}
		Expect(k8sClient.Status().Update(ctx, up)).To(Succeed())

		return up
	}

	createNode := func(osImage string) *corev1.Node {
		node := &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name: testNodeName,
			},
			Status: corev1.NodeStatus{
				NodeInfo: corev1.NodeSystemInfo{
					OSImage: osImage,
				},
			},
		}
		Expect(k8sClient.Create(ctx, node)).To(Succeed())
		return node
	}

	createJob := func(component, hookType string) *batchv1.Job {
		jobName := fmt.Sprintf("%s-%s-%s-%s", upgradePlanName, component, hookType, testNodeName)
		labels := map[string]string{
			upgradeplan.HarvesterUpgradePlanLabel:      upgradePlanName,
			upgradeplan.HarvesterUpgradeComponentLabel: component,
		}
		if hookType == "" {
			labels[upgradeplan.SUCNodeLabel] = testNodeName
		} else {
			labels[upgradeplan.HarvesterDrainHookTypeLabel] = hookType
			labels[upgradeplan.HarvesterUpgradeNodeLabel] = testNodeName
		}

		job := &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Name:      jobName,
				Namespace: "harvester-system",
				Labels:    labels,
			},
			Spec: batchv1.JobSpec{
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						RestartPolicy: corev1.RestartPolicyNever,
						Containers: []corev1.Container{
							{Name: "test", Image: "test:latest"},
						},
					},
				},
			},
		}
		Expect(k8sClient.Create(ctx, job)).To(Succeed())
		return job
	}

	markJobComplete := func(job *batchv1.Job) {
		now := metav1.Now()
		job.Status.StartTime = &now
		job.Status.CompletionTime = &now
		job.Status.Conditions = []batchv1.JobCondition{
			{
				Type:   "SuccessCriteriaMet",
				Status: corev1.ConditionTrue,
			},
			{
				Type:   batchv1.JobComplete,
				Status: corev1.ConditionTrue,
			},
		}
		Expect(k8sClient.Status().Update(ctx, job)).To(Succeed())
	}

	markJobFailed := func(job *batchv1.Job) {
		now := metav1.Now()
		job.Status.StartTime = &now
		job.Status.Conditions = []batchv1.JobCondition{
			{
				Type:   batchv1.JobFailureTarget,
				Status: corev1.ConditionTrue,
				Reason: "BackoffLimitExceeded",
			},
			{
				Type:    batchv1.JobFailed,
				Status:  corev1.ConditionTrue,
				Reason:  "BackoffLimitExceeded",
				Message: "Job has reached the specified backoff limit",
			},
		}
		Expect(k8sClient.Status().Update(ctx, job)).To(Succeed())
	}

	reconcileJob := func(job *batchv1.Job) (ctrl.Result, error) {
		return reconciler.Reconcile(ctx, ctrl.Request{
			NamespacedName: types.NamespacedName{
				Namespace: job.Namespace,
				Name:      job.Name,
			},
		})
	}

	Context("When a post-drain job completes successfully", func() {
		It("should set node state to WaitingReboot and annotate the Node", func() {
			createUpgradePlan()
			createNode(testOldOSVersion) // OS does not match yet
			job := createJob(upgradeplan.NodeComponent, upgradeplan.DrainHookTypePostDrain)
			markJobComplete(job)

			result, err := reconcileJob(job)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(ctrl.Result{}))

			// Verify node state is WaitingReboot
			up := &managementv1beta1.UpgradePlan{}
			Expect(k8sClient.Get(context.Background(), types.NamespacedName{Name: upgradePlanName}, up)).To(Succeed())
			Expect(up.Status.NodeUpgradeStatuses[testNodeName].State).To(Equal(managementv1beta1.NodeStateWaitingReboot))

			// Verify PendingOSImageAnnotation is set on the Node
			node := &corev1.Node{}
			Expect(k8sClient.Get(context.Background(), types.NamespacedName{Name: testNodeName}, node)).To(Succeed())
			Expect(node.Annotations).To(HaveKeyWithValue(upgradeplan.PendingOSImageAnnotation, testOSVersion))
		})
	})

	Context("When a pre-drain job completes successfully", func() {
		It("should set node state to PreDrained", func() {
			createUpgradePlan()
			job := createJob(upgradeplan.NodeComponent, upgradeplan.DrainHookTypePreDrain)
			markJobComplete(job)

			result, err := reconcileJob(job)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(ctrl.Result{}))

			up := &managementv1beta1.UpgradePlan{}
			Expect(k8sClient.Get(context.Background(), types.NamespacedName{Name: upgradePlanName}, up)).To(Succeed())
			Expect(up.Status.NodeUpgradeStatuses[testNodeName].State).To(Equal(managementv1beta1.NodeStatePreDrained))
		})
	})

	Context("When an image-preload job completes successfully", func() {
		It("should set node state to ImagePreloaded", func() {
			createUpgradePlan()
			job := createJob(upgradeplan.PrepareComponent, "")
			markJobComplete(job)

			result, err := reconcileJob(job)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(ctrl.Result{}))

			up := &managementv1beta1.UpgradePlan{}
			Expect(k8sClient.Get(context.Background(), types.NamespacedName{Name: upgradePlanName}, up)).To(Succeed())
			Expect(up.Status.NodeUpgradeStatuses[testNodeName].State).To(Equal(managementv1beta1.NodeStateImagePreloaded))
		})
	})

	Context("When a post-drain job fails", func() {
		It("should set node state to PostDrainFailed", func() {
			createUpgradePlan()
			job := createJob(upgradeplan.NodeComponent, upgradeplan.DrainHookTypePostDrain)
			markJobFailed(job)

			result, err := reconcileJob(job)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(ctrl.Result{}))

			up := &managementv1beta1.UpgradePlan{}
			Expect(k8sClient.Get(context.Background(), types.NamespacedName{Name: upgradePlanName}, up)).To(Succeed())
			Expect(up.Status.NodeUpgradeStatuses[testNodeName].State).To(Equal(managementv1beta1.NodeStatePostDrainFailed))
		})
	})

	Context("When a completed pre-drain job is reconciled after node has progressed to PostDraining", func() {
		It("should not regress the node state", func() {
			up := createUpgradePlan()
			// Simulate: node has already progressed to PostDraining
			up.Status.NodeUpgradeStatuses[testNodeName] = managementv1beta1.NodeUpgradeStatus{
				State: managementv1beta1.NodeStatePostDraining,
			}
			Expect(k8sClient.Status().Update(ctx, up)).To(Succeed())

			// The stale pre-drain job that already completed
			job := createJob(upgradeplan.NodeComponent, upgradeplan.DrainHookTypePreDrain)
			markJobComplete(job)

			result, err := reconcileJob(job)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(ctrl.Result{}))

			// Verify node state is still PostDraining, not regressed to PreDrained
			fresh := &managementv1beta1.UpgradePlan{}
			Expect(k8sClient.Get(context.Background(), types.NamespacedName{Name: upgradePlanName}, fresh)).To(Succeed())
			Expect(fresh.Status.NodeUpgradeStatuses[testNodeName].State).To(Equal(managementv1beta1.NodeStatePostDraining))
		})
	})

	Context("When a completed pre-drain job is reconciled after node has progressed to WaitingReboot", func() {
		It("should not regress the node state", func() {
			up := createUpgradePlan()
			up.Status.NodeUpgradeStatuses[testNodeName] = managementv1beta1.NodeUpgradeStatus{
				State: managementv1beta1.NodeStateWaitingReboot,
			}
			Expect(k8sClient.Status().Update(ctx, up)).To(Succeed())

			job := createJob(upgradeplan.NodeComponent, upgradeplan.DrainHookTypePreDrain)
			markJobComplete(job)

			result, err := reconcileJob(job)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(ctrl.Result{}))

			fresh := &managementv1beta1.UpgradePlan{}
			Expect(k8sClient.Get(context.Background(), types.NamespacedName{Name: upgradePlanName}, fresh)).To(Succeed())
			Expect(fresh.Status.NodeUpgradeStatuses[testNodeName].State).To(Equal(managementv1beta1.NodeStateWaitingReboot))
		})
	})

	Context("When a completed image-preload job is reconciled after node has progressed to PreDrained", func() {
		It("should not regress the node state", func() {
			up := createUpgradePlan()
			up.Status.NodeUpgradeStatuses[testNodeName] = managementv1beta1.NodeUpgradeStatus{
				State: managementv1beta1.NodeStatePreDrained,
			}
			Expect(k8sClient.Status().Update(ctx, up)).To(Succeed())

			job := createJob(upgradeplan.PrepareComponent, "")
			markJobComplete(job)

			result, err := reconcileJob(job)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(ctrl.Result{}))

			fresh := &managementv1beta1.UpgradePlan{}
			Expect(k8sClient.Get(context.Background(), types.NamespacedName{Name: upgradePlanName}, fresh)).To(Succeed())
			Expect(fresh.Status.NodeUpgradeStatuses[testNodeName].State).To(Equal(managementv1beta1.NodeStatePreDrained))
		})
	})
})
