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

package controller

import (
	"context"

	"github.com/go-logr/logr"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	managementv1beta1 "github.com/harvester/upgrade-toolkit/api/v1beta1"
)

// fakeUpgradeLogPipeline is a no-op pipeline for testing.
type fakeUpgradeLogPipeline struct{}

func (f *fakeUpgradeLogPipeline) Execute(_ context.Context, _ *managementv1beta1.UpgradeLog) (ctrl.Result, error) {
	return ctrl.Result{}, nil
}

var _ = Describe("UpgradeLog Controller", func() {
	Context("When reconciling a resource", func() {
		const resourceName = "test-upgrade-log"

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name: resourceName,
		}
		upgradelogObj := &managementv1beta1.UpgradeLog{}

		BeforeEach(func() {
			By("creating the custom resource for the Kind UpgradeLog")
			err := k8sClient.Get(ctx, typeNamespacedName, upgradelogObj)
			if err != nil && errors.IsNotFound(err) {
				resource := &managementv1beta1.UpgradeLog{
					ObjectMeta: metav1.ObjectMeta{
						Name: resourceName,
					},
					Spec: managementv1beta1.UpgradeLogSpec{
						UpgradePlanName: "test-upgrade-plan",
					},
				}
				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			}
		})

		AfterEach(func() {
			resource := &managementv1beta1.UpgradeLog{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())

			By("Cleanup the specific resource instance UpgradeLog")
			Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
		})

		It("should successfully reconcile the resource", func() {
			By("Reconciling the created resource")
			controllerReconciler := &UpgradeLogReconciler{
				Client:   k8sClient,
				Scheme:   k8sClient.Scheme(),
				Log:      logr.Discard(),
				pipeline: &fakeUpgradeLogPipeline{},
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())
		})
	})
})
