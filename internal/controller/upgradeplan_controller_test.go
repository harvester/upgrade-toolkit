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

	"github.com/go-logr/logr"
	harvesterv1beta1 "github.com/harvester/harvester/pkg/apis/harvesterhci.io/v1beta1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"

	managementv1beta1 "github.com/harvester/upgrade-toolkit/api/v1beta1"
	upgradeplanpkg "github.com/harvester/upgrade-toolkit/pkg/upgradeplan"
)

func newReconciler() *UpgradePlanReconciler {
	deps := &upgradeplanpkg.PhaseDeps{
		Client:             k8sClient,
		Scheme:             k8sClient.Scheme(),
		Log:                logr.Discard(),
		JobServiceAccount:  "harvester",
		PlanServiceAccount: "system-upgrade-controller",
	}
	return &UpgradePlanReconciler{
		Client:             k8sClient,
		Scheme:             k8sClient.Scheme(),
		Log:                logr.Discard(),
		EventRecorder:      record.NewFakeRecorder(100),
		JobServiceAccount:  "harvester",
		PlanServiceAccount: "system-upgrade-controller",
		pipeline:           upgradeplanpkg.NewPipeline(deps),
	}
}

func createUpgradePlan(ctx context.Context, name string) {
	resource := &managementv1beta1.UpgradePlan{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: managementv1beta1.UpgradePlanSpec{
			Version: ptr.To("test-version"),
		},
	}
	Expect(k8sClient.Create(ctx, resource)).To(Succeed())
}

func setProgressingCondition(ctx context.Context, name string, status metav1.ConditionStatus) {
	var up managementv1beta1.UpgradePlan
	Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name}, &up)).To(Succeed())
	up.SetCondition(managementv1beta1.UpgradePlanProgressing, status, "test", "")
	Expect(k8sClient.Status().Update(ctx, &up)).To(Succeed())
}

func deleteUpgradePlan(ctx context.Context, name string) {
	resource := &managementv1beta1.UpgradePlan{}
	err := k8sClient.Get(ctx, types.NamespacedName{Name: name}, resource)
	if err != nil {
		return
	}
	// Remove finalizer if present so the object can be deleted
	if controllerutil.ContainsFinalizer(resource, upgradeplanpkg.UpgradePlanFinalizer) {
		controllerutil.RemoveFinalizer(resource, upgradeplanpkg.UpgradePlanFinalizer)
		Expect(k8sClient.Update(ctx, resource)).To(Succeed())
	}
	Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
}

var _ = Describe("UpgradePlan Controller", func() {
	Context("When reconciling a resource", func() {
		const resourceName = "test-resource"

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: "default", // TODO(user):Modify as needed
		}
		upgradeplan := &managementv1beta1.UpgradePlan{}

		BeforeEach(func() {
			By("creating the custom resource for the Kind UpgradePlan")
			err := k8sClient.Get(ctx, typeNamespacedName, upgradeplan)
			if err != nil && errors.IsNotFound(err) {
				resource := &managementv1beta1.UpgradePlan{
					ObjectMeta: metav1.ObjectMeta{
						Name:      resourceName,
						Namespace: "default",
					},
					Spec: managementv1beta1.UpgradePlanSpec{
						Version: ptr.To("test-version"),
					},
				}
				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			}
		})

		AfterEach(func() {
			By("Cleanup the specific resource instance UpgradePlan")
			deleteUpgradePlan(ctx, resourceName)
		})
		It("should successfully reconcile the resource", func() {
			By("Reconciling the created resource")
			controllerReconciler := newReconciler()

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())
			// TODO(user): Add more specific assertions depending on your controller's reconciliation logic.
			// Example: If you expect a certain status condition after reconciliation, verify it here.
		})
	})

	Context("delete reconciliation", func() {
		const deletePlanName = "delete-test"

		ctx := context.Background()

		AfterEach(func() {
			deleteUpgradePlan(ctx, deletePlanName)
		})

		It("should add finalizer on first reconcile via InitPhase", func() {
			By("creating an UpgradePlan without a finalizer")
			createUpgradePlan(ctx, deletePlanName)

			By("reconciling to trigger InitPhase which adds the finalizer")
			controllerReconciler := newReconciler()
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: deletePlanName},
			})
			Expect(err).NotTo(HaveOccurred())

			By("verifying the finalizer is present")
			var updated managementv1beta1.UpgradePlan
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: deletePlanName}, &updated)).To(Succeed())
			Expect(controllerutil.ContainsFinalizer(&updated, upgradeplanpkg.UpgradePlanFinalizer)).To(BeTrue())
		})

		It("should run cleanup and remove finalizer on deletion", func() {
			By("creating an UpgradePlan and reconciling to add the finalizer")
			createUpgradePlan(ctx, deletePlanName)
			controllerReconciler := newReconciler()
			_, _ = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: deletePlanName},
			})

			By("verifying the finalizer is present before deletion")
			var beforeDelete managementv1beta1.UpgradePlan
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: deletePlanName}, &beforeDelete)).To(Succeed())
			Expect(controllerutil.ContainsFinalizer(&beforeDelete, upgradeplanpkg.UpgradePlanFinalizer)).To(BeTrue())

			By("deleting the UpgradePlan (DeletionTimestamp set, held by finalizer)")
			Expect(k8sClient.Delete(ctx, &beforeDelete)).To(Succeed())

			By("verifying the object still exists with DeletionTimestamp set")
			var deleting managementv1beta1.UpgradePlan
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: deletePlanName}, &deleting)).To(Succeed())
			Expect(deleting.DeletionTimestamp.IsZero()).To(BeFalse())

			By("reconciling to run cleanup and remove the finalizer")
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: deletePlanName},
			})
			Expect(err).NotTo(HaveOccurred())

			By("verifying the object is deleted")
			Eventually(func() bool {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: deletePlanName}, &managementv1beta1.UpgradePlan{})
				return errors.IsNotFound(err)
			}).Should(BeTrue())
		})

		It("should be a no-op when finalizer is already removed", func() {
			By("reconciling a non-existent UpgradePlan (simulating already-deleted)")
			controllerReconciler := newReconciler()
			result, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: "nonexistent-plan"},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(reconcile.Result{}))
		})
	})

	Context("concurrent upgrade prevention", func() {
		ctx := context.Background()

		AfterEach(func() {
			deleteUpgradePlan(ctx, "upgrade-old")
			deleteUpgradePlan(ctx, "upgrade-new")
		})

		It("should set Available=False when another UpgradePlan has Progressing=True", func() {
			By("creating an existing UpgradePlan with Progressing=True")
			createUpgradePlan(ctx, "upgrade-old")
			setProgressingCondition(ctx, "upgrade-old", metav1.ConditionTrue)

			By("creating the target UpgradePlan")
			createUpgradePlan(ctx, "upgrade-new")

			By("reconciling the target UpgradePlan")
			controllerReconciler := newReconciler()
			result, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: "upgrade-new"},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(reconcile.Result{}))

			By("verifying Available=False with correct reason")
			var updated managementv1beta1.UpgradePlan
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "upgrade-new"}, &updated)).To(Succeed())
			Expect(updated.ConditionFalse(managementv1beta1.UpgradePlanAvailable)).To(BeTrue())
			cond := updated.LookupCondition(managementv1beta1.UpgradePlanAvailable)
			Expect(cond.Reason).To(Equal("ConcurrentUpgradeBlocked"))
			Expect(cond.Message).To(ContainSubstring("upgrade-old"))
		})

		It("should proceed normally when no other UpgradePlan has Progressing=True", func() {
			By("creating an existing UpgradePlan with Progressing=False")
			createUpgradePlan(ctx, "upgrade-old")
			setProgressingCondition(ctx, "upgrade-old", metav1.ConditionFalse)

			By("creating the target UpgradePlan")
			createUpgradePlan(ctx, "upgrade-new")

			By("reconciling the target UpgradePlan")
			controllerReconciler := newReconciler()
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: "upgrade-new"},
			})
			// The pipeline will run and may error (e.g., Version CR missing),
			// but the key assertion is that Available was NOT set to False
			// with ConcurrentUpgradeBlocked reason.
			_ = err

			By("verifying the UpgradePlan was not blocked")
			var updated managementv1beta1.UpgradePlan
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "upgrade-new"}, &updated)).To(Succeed())
			cond := updated.LookupCondition(managementv1beta1.UpgradePlanAvailable)
			Expect(cond.Reason).NotTo(Equal("ConcurrentUpgradeBlocked"))
		})

		It("should block even if this UpgradePlan also has Progressing=True", func() {
			By("creating UpgradePlan A with Progressing=True")
			createUpgradePlan(ctx, "upgrade-old")
			setProgressingCondition(ctx, "upgrade-old", metav1.ConditionTrue)

			By("creating UpgradePlan B also with Progressing=True")
			createUpgradePlan(ctx, "upgrade-new")
			setProgressingCondition(ctx, "upgrade-new", metav1.ConditionTrue)

			By("reconciling UpgradePlan B")
			controllerReconciler := newReconciler()
			result, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: "upgrade-new"},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(reconcile.Result{}))

			By("verifying B gets Available=False")
			var updated managementv1beta1.UpgradePlan
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "upgrade-new"}, &updated)).To(Succeed())
			Expect(updated.ConditionFalse(managementv1beta1.UpgradePlanAvailable)).To(BeTrue())
			cond := updated.LookupCondition(managementv1beta1.UpgradePlanAvailable)
			Expect(cond.Reason).To(Equal("ConcurrentUpgradeBlocked"))
		})
	})

	Context("mapVMImageToUpgradePlan", func() {
		const (
			vmImageName     = "external-iso"
			upgradePlanName = "map-test-upgrade"
		)
		ctx := context.Background()

		BeforeEach(func() {
			ns := &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{Name: "harvester-system"},
			}
			_ = k8sClient.Create(ctx, ns)
		})

		AfterEach(func() {
			deleteUpgradePlan(ctx, upgradePlanName)
		})

		It("should enqueue UpgradePlan when status.isoImageID matches VMImage name", func() {
			By("creating an UpgradePlan with isoImageID set")
			createUpgradePlan(ctx, upgradePlanName)
			var up managementv1beta1.UpgradePlan
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: upgradePlanName}, &up)).To(Succeed())
			up.Status.ISOImageID = ptr.To(vmImageName)
			Expect(k8sClient.Status().Update(ctx, &up)).To(Succeed())

			By("calling mapVMImageToUpgradePlan with a matching VMImage")
			reconciler := newReconciler()
			vmImage := &harvesterv1beta1.VirtualMachineImage{
				ObjectMeta: metav1.ObjectMeta{
					Name:      vmImageName,
					Namespace: "harvester-system",
				},
			}
			requests := reconciler.mapVMImageToUpgradePlan(ctx, vmImage)
			Expect(requests).To(HaveLen(1))
			Expect(requests[0].NamespacedName).To(Equal(types.NamespacedName{Name: upgradePlanName}))
		})

		It("should return nil for VMImages with an UpgradePlan owner reference", func() {
			reconciler := newReconciler()
			vmImage := &harvesterv1beta1.VirtualMachineImage{
				ObjectMeta: metav1.ObjectMeta{
					Name:      vmImageName,
					Namespace: "harvester-system",
					OwnerReferences: []metav1.OwnerReference{
						{
							Kind: "UpgradePlan",
							Name: "some-upgrade",
						},
					},
				},
			}
			requests := reconciler.mapVMImageToUpgradePlan(ctx, vmImage)
			Expect(requests).To(BeNil())
		})

		It("should return nil for VMImages in a different namespace", func() {
			reconciler := newReconciler()
			vmImage := &harvesterv1beta1.VirtualMachineImage{
				ObjectMeta: metav1.ObjectMeta{
					Name:      vmImageName,
					Namespace: "other-namespace",
				},
			}
			requests := reconciler.mapVMImageToUpgradePlan(ctx, vmImage)
			Expect(requests).To(BeNil())
		})

		It("should return nil when no UpgradePlan references the VMImage", func() {
			By("creating an UpgradePlan with a different isoImageID")
			createUpgradePlan(ctx, upgradePlanName)
			var up managementv1beta1.UpgradePlan
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: upgradePlanName}, &up)).To(Succeed())
			up.Status.ISOImageID = ptr.To("different-image")
			Expect(k8sClient.Status().Update(ctx, &up)).To(Succeed())

			reconciler := newReconciler()
			vmImage := &harvesterv1beta1.VirtualMachineImage{
				ObjectMeta: metav1.ObjectMeta{
					Name:      vmImageName,
					Namespace: "harvester-system",
				},
			}
			requests := reconciler.mapVMImageToUpgradePlan(ctx, vmImage)
			Expect(requests).To(BeEmpty())
		})
	})

	Context("vmImageStatusChangedPredicate", func() {
		var pred vmImageStatusChangedPredicate

		BeforeEach(func() {
			pred = vmImageStatusChangedPredicate{}
		})

		It("should reject Create events", func() {
			Expect(pred.Create(event.CreateEvent{
				Object: &harvesterv1beta1.VirtualMachineImage{},
			})).To(BeFalse())
		})

		It("should reject Delete events", func() {
			Expect(pred.Delete(event.DeleteEvent{
				Object: &harvesterv1beta1.VirtualMachineImage{},
			})).To(BeFalse())
		})

		It("should reject Generic events", func() {
			Expect(pred.Generic(event.GenericEvent{
				Object: &harvesterv1beta1.VirtualMachineImage{},
			})).To(BeFalse())
		})

		It("should accept Update when ImageImported changes from absent to True", func() {
			oldVMI := &harvesterv1beta1.VirtualMachineImage{}
			newVMI := &harvesterv1beta1.VirtualMachineImage{
				Status: harvesterv1beta1.VirtualMachineImageStatus{
					Conditions: []harvesterv1beta1.Condition{
						{
							Type:   harvesterv1beta1.ImageImported,
							Status: corev1.ConditionTrue,
						},
					},
				},
			}
			Expect(pred.Update(event.UpdateEvent{
				ObjectOld: oldVMI,
				ObjectNew: newVMI,
			})).To(BeTrue())
		})

		It("should accept Update when ImageImported changes from absent to False", func() {
			oldVMI := &harvesterv1beta1.VirtualMachineImage{}
			newVMI := &harvesterv1beta1.VirtualMachineImage{
				Status: harvesterv1beta1.VirtualMachineImageStatus{
					Conditions: []harvesterv1beta1.Condition{
						{
							Type:   harvesterv1beta1.ImageImported,
							Status: corev1.ConditionFalse,
						},
					},
				},
			}
			Expect(pred.Update(event.UpdateEvent{
				ObjectOld: oldVMI,
				ObjectNew: newVMI,
			})).To(BeTrue())
		})

		It("should reject Update when ImageImported condition is unchanged", func() {
			conditions := []harvesterv1beta1.Condition{
				{
					Type:   harvesterv1beta1.ImageImported,
					Status: corev1.ConditionTrue,
				},
			}
			oldVMI := &harvesterv1beta1.VirtualMachineImage{
				Status: harvesterv1beta1.VirtualMachineImageStatus{
					Conditions: conditions,
				},
			}
			newVMI := &harvesterv1beta1.VirtualMachineImage{
				Status: harvesterv1beta1.VirtualMachineImageStatus{
					Conditions: conditions,
				},
			}
			Expect(pred.Update(event.UpdateEvent{
				ObjectOld: oldVMI,
				ObjectNew: newVMI,
			})).To(BeFalse())
		})

		It("should reject Update when neither old nor new has ImageImported condition", func() {
			oldVMI := &harvesterv1beta1.VirtualMachineImage{}
			newVMI := &harvesterv1beta1.VirtualMachineImage{}
			Expect(pred.Update(event.UpdateEvent{
				ObjectOld: oldVMI,
				ObjectNew: newVMI,
			})).To(BeFalse())
		})
	})
})
