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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/rancher/wrangler/v3/pkg/name"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	managementv1beta1 "github.com/harvester/upgrade-toolkit/api/v1beta1"
	"github.com/harvester/upgrade-toolkit/pkg/upgradeplan"
)

// staticNodeNameResolver always returns a fixed node name for testing.
type staticNodeNameResolver struct {
	nodeName string
}

func (r *staticNodeNameResolver) ResolveNodeName(_ context.Context, _ client.Client, _ *corev1.Secret) (string, error) {
	return r.nodeName, nil
}

var _ = Describe("Secret Controller", func() {
	const (
		fleetLocalNamespace = "fleet-local"
		upgradePlanName     = "test-upgrade"
		testNodeName        = "test-node"
		testMachineName     = "test-machine"
		testSecretName      = "test-secret"
		testVersion         = "test-version"
		testK8sVersion      = "test-k8s-version"
	)

	var (
		reconciler *SecretReconciler
	)

	BeforeEach(func() {
		reconciler = &SecretReconciler{
			Client:           k8sClient,
			Scheme:           k8sClient.Scheme(),
			Log:              logf.Log.WithName("test-secret-controller"),
			NodeNameResolver: &staticNodeNameResolver{nodeName: testNodeName},
		}

		// Ensure required namespaces exist
		for _, nsName := range []string{fleetLocalNamespace, "harvester-system"} {
			ns := &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{Name: nsName},
			}
			_ = k8sClient.Create(ctx, ns)
		}
	})

	AfterEach(func() {
		// Clean up Secrets in fleet-local
		secretList := &corev1.SecretList{}
		_ = k8sClient.List(ctx, secretList, client.InNamespace(fleetLocalNamespace))
		for i := range secretList.Items {
			_ = k8sClient.Delete(ctx, &secretList.Items[i])
		}

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
	})

	createUpgradePlan := func(phase managementv1beta1.UpgradePlanPhase) *managementv1beta1.UpgradePlan {
		up := &managementv1beta1.UpgradePlan{
			ObjectMeta: metav1.ObjectMeta{
				Name: upgradePlanName,
			},
			Spec: managementv1beta1.UpgradePlanSpec{
				Version: testVersion,
			},
		}
		Expect(k8sClient.Create(ctx, up)).To(Succeed())

		// Update status
		up.Status.CurrentPhase = phase
		up.Status.NodeUpgradeStatuses = map[string]managementv1beta1.NodeUpgradeStatus{
			testNodeName: {State: managementv1beta1.NodeStateImagePreloaded},
		}
		up.Status.ReleaseMetadata = &managementv1beta1.ReleaseMetadata{
			Kubernetes: testK8sVersion,
		}
		Expect(k8sClient.Status().Update(ctx, up)).To(Succeed())

		return up
	}

	createMachinePlanSecret := func(annotations map[string]string) *corev1.Secret {
		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      testSecretName,
				Namespace: fleetLocalNamespace,
				Labels: map[string]string{
					upgradeplan.MachinePlanMachineLabel: testMachineName,
				},
				Annotations: annotations,
			},
			Type: corev1.SecretType(upgradeplan.MachinePlanSecretType),
		}
		Expect(k8sClient.Create(ctx, secret)).To(Succeed())
		return secret
	}

	Context("When the Secret is not a machine-plan Secret", func() {
		It("should ignore the Secret and return early", func() {
			secret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "not-a-machine-plan",
					Namespace: fleetLocalNamespace,
				},
				Type: corev1.SecretTypeOpaque,
			}
			Expect(k8sClient.Create(ctx, secret)).To(Succeed())

			result, err := reconciler.Reconcile(ctx, ctrl.Request{
				NamespacedName: types.NamespacedName{
					Namespace: fleetLocalNamespace,
					Name:      "not-a-machine-plan",
				},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(ctrl.Result{}))
		})
	})

	Context("When there is no active UpgradePlan in NodeUpgrading phase", func() {
		It("should return without creating any Jobs", func() {
			createMachinePlanSecret(map[string]string{
				upgradeplan.RKE2PreDrainAnnotation: "drain-1",
			})

			result, err := reconciler.Reconcile(ctx, ctrl.Request{
				NamespacedName: types.NamespacedName{
					Namespace: fleetLocalNamespace,
					Name:      testSecretName,
				},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(ctrl.Result{}))

			// Verify no Jobs were created
			jobList := &batchv1.JobList{}
			Expect(k8sClient.List(ctx, jobList, client.InNamespace("harvester-system"))).To(Succeed())
			Expect(jobList.Items).To(BeEmpty())
		})
	})

	Context("When pre-drain annotation is set by Rancher", func() {
		It("should create a pre-drain Job for the node", func() {
			createUpgradePlan(managementv1beta1.UpgradePlanPhaseNodeUpgrading)
			createMachinePlanSecret(map[string]string{
				upgradeplan.RKE2PreDrainAnnotation: "drain-1",
			})

			result, err := reconciler.Reconcile(ctx, ctrl.Request{
				NamespacedName: types.NamespacedName{
					Namespace: fleetLocalNamespace,
					Name:      testSecretName,
				},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(ctrl.Result{}))

			// Verify a pre-drain Job was created
			expectedJobName := name.SafeConcatName(
				upgradePlanName, upgradeplan.NodeComponent, upgradeplan.DrainHookTypePreDrain, testNodeName)
			job := &batchv1.Job{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: "harvester-system",
				Name:      expectedJobName,
			}, job)).To(Succeed())

			Expect(job.Labels[upgradeplan.HarvesterUpgradePlanLabel]).To(Equal(upgradePlanName))
			Expect(job.Labels[upgradeplan.HarvesterUpgradeComponentLabel]).To(Equal(upgradeplan.NodeComponent))
			Expect(job.Labels[upgradeplan.HarvesterDrainHookTypeLabel]).To(Equal(upgradeplan.DrainHookTypePreDrain))
		})
	})

	Context("When pre-drain Job has completed successfully", func() {
		It("should annotate the Secret to signal Rancher to proceed", func() {
			createUpgradePlan(managementv1beta1.UpgradePlanPhaseNodeUpgrading)
			createMachinePlanSecret(map[string]string{
				upgradeplan.RKE2PreDrainAnnotation: "drain-1",
			})

			// Create a completed pre-drain Job
			jobName := name.SafeConcatName(
				upgradePlanName, upgradeplan.NodeComponent, upgradeplan.DrainHookTypePreDrain, testNodeName)
			job := &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:      jobName,
					Namespace: "harvester-system",
					Labels: map[string]string{
						upgradeplan.HarvesterUpgradePlanLabel:      upgradePlanName,
						upgradeplan.HarvesterUpgradeComponentLabel: upgradeplan.NodeComponent,
						upgradeplan.HarvesterDrainHookTypeLabel:    upgradeplan.DrainHookTypePreDrain,
						upgradeplan.HarvesterUpgradeNodeLabel:      testNodeName,
					},
				},
				Spec: batchv1.JobSpec{
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							RestartPolicy: corev1.RestartPolicyNever,
							Containers: []corev1.Container{
								{Name: "pre-drain", Image: "test:latest"},
							},
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, job)).To(Succeed())

			// Simulate Job completion (K8s requires startTime, completionTime, and SuccessCriteriaMet)
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

			// Update UpgradePlan node status to PreDrained (as JobReconciler would)
			up := &managementv1beta1.UpgradePlan{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: upgradePlanName}, up)).To(Succeed())
			up.Status.NodeUpgradeStatuses[testNodeName] = managementv1beta1.NodeUpgradeStatus{
				State: managementv1beta1.NodeStatePreDrained,
			}
			Expect(k8sClient.Status().Update(ctx, up)).To(Succeed())

			// Reconcile the Secret
			result, err := reconciler.Reconcile(ctx, ctrl.Request{
				NamespacedName: types.NamespacedName{
					Namespace: fleetLocalNamespace,
					Name:      testSecretName,
				},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(ctrl.Result{}))

			// Verify the Secret was annotated with the pre-drain-done signal
			secret := &corev1.Secret{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: fleetLocalNamespace,
				Name:      testSecretName,
			}, secret)).To(Succeed())
			Expect(secret.Annotations[upgradeplan.PreHookAnnotation]).To(Equal("drain-1"))
		})
	})

	Context("When post-drain annotation is set by Rancher", func() {
		It("should create a post-drain Job for the node", func() {
			createUpgradePlan(managementv1beta1.UpgradePlanPhaseNodeUpgrading)

			// Mark the node as PreDrained so the post-drain guard allows Job creation
			up := &managementv1beta1.UpgradePlan{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: upgradePlanName}, up)).To(Succeed())
			up.Status.NodeUpgradeStatuses[testNodeName] = managementv1beta1.NodeUpgradeStatus{
				State: managementv1beta1.NodeStatePreDrained,
			}
			Expect(k8sClient.Status().Update(ctx, up)).To(Succeed())

			createMachinePlanSecret(map[string]string{
				upgradeplan.RKE2PreDrainAnnotation:  "drain-1",
				upgradeplan.PreHookAnnotation:       "drain-1", // pre-drain already completed
				upgradeplan.RKE2PostDrainAnnotation: "drain-1",
			})

			result, err := reconciler.Reconcile(ctx, ctrl.Request{
				NamespacedName: types.NamespacedName{
					Namespace: fleetLocalNamespace,
					Name:      testSecretName,
				},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(ctrl.Result{}))

			// Verify a post-drain Job was created
			expectedJobName := name.SafeConcatName(
				upgradePlanName, upgradeplan.NodeComponent, upgradeplan.DrainHookTypePostDrain, testNodeName)
			job := &batchv1.Job{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: "harvester-system",
				Name:      expectedJobName,
			}, job)).To(Succeed())

			Expect(job.Labels[upgradeplan.HarvesterDrainHookTypeLabel]).To(Equal(upgradeplan.DrainHookTypePostDrain))
		})
	})

	Context("When the node name is long enough to exceed 63-character limit", func() {
		const longNodeName = "charlie-1-tink-system-with-extra-long-suffix"

		It("should create a post-drain Job with a truncated name", func() {
			// Use a reconciler with the long node name
			longNameReconciler := &SecretReconciler{
				Client:           k8sClient,
				Scheme:           k8sClient.Scheme(),
				Log:              logf.Log.WithName("test-secret-controller-long"),
				NodeNameResolver: &staticNodeNameResolver{nodeName: longNodeName},
			}

			createUpgradePlan(managementv1beta1.UpgradePlanPhaseNodeUpgrading)

			// Mark the node as PreDrained so the post-drain guard allows Job creation
			up := &managementv1beta1.UpgradePlan{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: upgradePlanName}, up)).To(Succeed())
			up.Status.NodeUpgradeStatuses[longNodeName] = managementv1beta1.NodeUpgradeStatus{
				State: managementv1beta1.NodeStatePreDrained,
			}
			Expect(k8sClient.Status().Update(ctx, up)).To(Succeed())

			createMachinePlanSecret(map[string]string{
				upgradeplan.RKE2PreDrainAnnotation:  "drain-1",
				upgradeplan.PreHookAnnotation:       "drain-1",
				upgradeplan.RKE2PostDrainAnnotation: "drain-1",
			})

			result, err := longNameReconciler.Reconcile(ctx, ctrl.Request{
				NamespacedName: types.NamespacedName{
					Namespace: fleetLocalNamespace,
					Name:      testSecretName,
				},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(ctrl.Result{}))

			// Verify a post-drain Job was created with a safe name
			expectedJobName := name.SafeConcatName(
				upgradePlanName, upgradeplan.NodeComponent, upgradeplan.DrainHookTypePostDrain, longNodeName)
			Expect(len(expectedJobName)).To(BeNumerically("<=", 63))

			job := &batchv1.Job{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: "harvester-system",
				Name:      expectedJobName,
			}, job)).To(Succeed())

			Expect(job.Labels[upgradeplan.HarvesterDrainHookTypeLabel]).To(Equal(upgradeplan.DrainHookTypePostDrain))
		})
	})

	Context("When node is in WaitingReboot state with post-drain annotation", func() {
		It("should not acknowledge the post-drain hook", func() {
			createUpgradePlan(managementv1beta1.UpgradePlanPhaseNodeUpgrading)

			// Set node to WaitingReboot
			up := &managementv1beta1.UpgradePlan{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: upgradePlanName}, up)).To(Succeed())
			up.Status.NodeUpgradeStatuses[testNodeName] = managementv1beta1.NodeUpgradeStatus{
				State: managementv1beta1.NodeStateWaitingReboot,
			}
			Expect(k8sClient.Status().Update(ctx, up)).To(Succeed())

			createMachinePlanSecret(map[string]string{
				upgradeplan.RKE2PreDrainAnnotation:  "drain-1",
				upgradeplan.PreHookAnnotation:       "drain-1",
				upgradeplan.RKE2PostDrainAnnotation: "drain-1",
			})

			result, err := reconciler.Reconcile(ctx, ctrl.Request{
				NamespacedName: types.NamespacedName{
					Namespace: fleetLocalNamespace,
					Name:      testSecretName,
				},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(ctrl.Result{}))

			// Verify the post-drain hook was NOT acknowledged
			secret := &corev1.Secret{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: fleetLocalNamespace,
				Name:      testSecretName,
			}, secret)).To(Succeed())
			Expect(secret.Annotations).NotTo(HaveKey(upgradeplan.PostHookAnnotation))
		})
	})

	Context("When reconciling idempotently", func() {
		It("should not create duplicate Jobs on repeated reconciles", func() {
			createUpgradePlan(managementv1beta1.UpgradePlanPhaseNodeUpgrading)
			createMachinePlanSecret(map[string]string{
				upgradeplan.RKE2PreDrainAnnotation: "drain-1",
			})

			// First reconcile
			_, err := reconciler.Reconcile(ctx, ctrl.Request{
				NamespacedName: types.NamespacedName{
					Namespace: fleetLocalNamespace,
					Name:      testSecretName,
				},
			})
			Expect(err).NotTo(HaveOccurred())

			// Second reconcile
			_, err = reconciler.Reconcile(ctx, ctrl.Request{
				NamespacedName: types.NamespacedName{
					Namespace: fleetLocalNamespace,
					Name:      testSecretName,
				},
			})
			Expect(err).NotTo(HaveOccurred())

			// Verify only one Job exists
			jobList := &batchv1.JobList{}
			Expect(k8sClient.List(ctx, jobList, client.InNamespace("harvester-system"),
				client.MatchingLabels{
					upgradeplan.HarvesterDrainHookTypeLabel: upgradeplan.DrainHookTypePreDrain,
				})).To(Succeed())
			Expect(jobList.Items).To(HaveLen(1))
		})
	})

	Context("mapUpgradePlanToSecrets", func() {
		It("should enqueue machine-plan Secrets when UpgradePlan is in NodeUpgrading phase", func() {
			createUpgradePlan(managementv1beta1.UpgradePlanPhaseNodeUpgrading)
			createMachinePlanSecret(nil)

			// Also create a non-machine-plan Secret that should be excluded
			nonMachinePlanSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "not-machine-plan",
					Namespace: fleetLocalNamespace,
				},
				Type: corev1.SecretTypeOpaque,
			}
			Expect(k8sClient.Create(ctx, nonMachinePlanSecret)).To(Succeed())

			up := &managementv1beta1.UpgradePlan{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: upgradePlanName}, up)).To(Succeed())

			requests := reconciler.mapUpgradePlanToSecrets(ctx, up)
			Expect(requests).To(HaveLen(1))
			Expect(requests[0].NamespacedName).To(Equal(types.NamespacedName{
				Namespace: fleetLocalNamespace,
				Name:      testSecretName,
			}))
		})

		It("should return nil when UpgradePlan is not in NodeUpgrading phase", func() {
			up := &managementv1beta1.UpgradePlan{
				ObjectMeta: metav1.ObjectMeta{Name: upgradePlanName},
				Status: managementv1beta1.UpgradePlanStatus{
					CurrentPhase: managementv1beta1.UpgradePlanPhaseImagePreloading,
				},
			}

			requests := reconciler.mapUpgradePlanToSecrets(ctx, up)
			Expect(requests).To(BeNil())
		})
	})

	Context("upgradePlanNodeStatusChangedPredicate", func() {
		var pred upgradePlanNodeStatusChangedPredicate

		BeforeEach(func() {
			pred = upgradePlanNodeStatusChangedPredicate{}
		})

		It("should reject Create events", func() {
			Expect(pred.Create(event.CreateEvent{
				Object: &managementv1beta1.UpgradePlan{},
			})).To(BeFalse())
		})

		It("should reject Delete events", func() {
			Expect(pred.Delete(event.DeleteEvent{
				Object: &managementv1beta1.UpgradePlan{},
			})).To(BeFalse())
		})

		It("should reject Generic events", func() {
			Expect(pred.Generic(event.GenericEvent{
				Object: &managementv1beta1.UpgradePlan{},
			})).To(BeFalse())
		})

		It("should accept Update when NodeUpgradeStatuses changed in NodeUpgrading phase", func() {
			oldUP := &managementv1beta1.UpgradePlan{
				Status: managementv1beta1.UpgradePlanStatus{
					CurrentPhase: managementv1beta1.UpgradePlanPhaseNodeUpgrading,
					NodeUpgradeStatuses: map[string]managementv1beta1.NodeUpgradeStatus{
						testNodeName: {State: managementv1beta1.NodeStateImagePreloaded},
					},
				},
			}
			newUP := &managementv1beta1.UpgradePlan{
				Status: managementv1beta1.UpgradePlanStatus{
					CurrentPhase: managementv1beta1.UpgradePlanPhaseNodeUpgrading,
					NodeUpgradeStatuses: map[string]managementv1beta1.NodeUpgradeStatus{
						testNodeName: {State: managementv1beta1.NodeStatePreDrained},
					},
				},
			}
			Expect(pred.Update(event.UpdateEvent{
				ObjectOld: oldUP,
				ObjectNew: newUP,
			})).To(BeTrue())
		})

		It("should reject Update when not in NodeUpgrading phase", func() {
			oldUP := &managementv1beta1.UpgradePlan{
				Status: managementv1beta1.UpgradePlanStatus{
					CurrentPhase: managementv1beta1.UpgradePlanPhaseImagePreloading,
				},
			}
			newUP := &managementv1beta1.UpgradePlan{
				Status: managementv1beta1.UpgradePlanStatus{
					CurrentPhase: managementv1beta1.UpgradePlanPhaseImagePreloading,
				},
			}
			Expect(pred.Update(event.UpdateEvent{
				ObjectOld: oldUP,
				ObjectNew: newUP,
			})).To(BeFalse())
		})

		It("should reject Update when NodeUpgradeStatuses unchanged", func() {
			statuses := map[string]managementv1beta1.NodeUpgradeStatus{
				testNodeName: {State: managementv1beta1.NodeStatePreDrained},
			}
			oldUP := &managementv1beta1.UpgradePlan{
				Status: managementv1beta1.UpgradePlanStatus{
					CurrentPhase:        managementv1beta1.UpgradePlanPhaseNodeUpgrading,
					NodeUpgradeStatuses: statuses,
				},
			}
			newUP := &managementv1beta1.UpgradePlan{
				Status: managementv1beta1.UpgradePlanStatus{
					CurrentPhase:        managementv1beta1.UpgradePlanPhaseNodeUpgrading,
					NodeUpgradeStatuses: statuses,
				},
			}
			Expect(pred.Update(event.UpdateEvent{
				ObjectOld: oldUP,
				ObjectNew: newUP,
			})).To(BeFalse())
		})
	})
})
