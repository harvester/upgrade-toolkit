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

package upgradelog

import (
	"context"
	"testing"

	managementv1beta1 "github.com/harvester/upgrade-toolkit/api/v1beta1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func newTestUpgradePlan(name string, phase managementv1beta1.UpgradePlanPhase) *managementv1beta1.UpgradePlan {
	up := &managementv1beta1.UpgradePlan{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: managementv1beta1.UpgradePlanSpec{
			Version: ptr.To("v1.0.0"),
		},
	}
	up.Status.CurrentPhase = phase
	return up
}

func TestCollectPhase_Run(t *testing.T) {
	t.Run("stays in Collecting when UpgradePlan is active", func(t *testing.T) {
		scheme := newTestScheme(t)
		upgradePlan := newTestUpgradePlan("my-plan", managementv1beta1.UpgradePlanPhaseNodeUpgrading)

		fakeClient := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(upgradePlan).
			WithStatusSubresource(upgradePlan).
			Build()

		require.NoError(t, fakeClient.Status().Update(context.Background(), upgradePlan))

		phase := NewCollectPhase(&PhaseDeps{Client: fakeClient, Scheme: scheme})
		ul := newTestUpgradeLog()
		ul.Status.CurrentPhase = managementv1beta1.UpgradeLogPhaseCollecting

		result, err := phase.Run(context.Background(), ul)
		require.NoError(t, err)
		assert.Zero(t, result.RequeueAfter)
		assert.Equal(t, managementv1beta1.UpgradeLogPhaseCollecting, ul.Status.CurrentPhase)
	})

	t.Run("transitions to Collected when UpgradePlan is Succeeded", func(t *testing.T) {
		scheme := newTestScheme(t)
		upgradePlan := newTestUpgradePlan("my-plan", managementv1beta1.UpgradePlanPhaseSucceeded)

		fakeClient := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(upgradePlan).
			WithStatusSubresource(upgradePlan).
			Build()

		require.NoError(t, fakeClient.Status().Update(context.Background(), upgradePlan))

		phase := NewCollectPhase(&PhaseDeps{Client: fakeClient, Scheme: scheme})
		ul := newTestUpgradeLog()
		ul.Status.CurrentPhase = managementv1beta1.UpgradeLogPhaseCollecting

		result, err := phase.Run(context.Background(), ul)
		require.NoError(t, err)
		assert.Zero(t, result.RequeueAfter)
		assert.Equal(t, managementv1beta1.UpgradeLogPhaseCollected, ul.Status.CurrentPhase)
	})

	t.Run("transitions to Collected when UpgradePlan is Failed", func(t *testing.T) {
		scheme := newTestScheme(t)
		upgradePlan := newTestUpgradePlan("my-plan", managementv1beta1.UpgradePlanPhaseFailed)

		fakeClient := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(upgradePlan).
			WithStatusSubresource(upgradePlan).
			Build()

		require.NoError(t, fakeClient.Status().Update(context.Background(), upgradePlan))

		phase := NewCollectPhase(&PhaseDeps{Client: fakeClient, Scheme: scheme})
		ul := newTestUpgradeLog()
		ul.Status.CurrentPhase = managementv1beta1.UpgradeLogPhaseCollecting

		result, err := phase.Run(context.Background(), ul)
		require.NoError(t, err)
		assert.Zero(t, result.RequeueAfter)
		assert.Equal(t, managementv1beta1.UpgradeLogPhaseCollected, ul.Status.CurrentPhase)
	})

	t.Run("transitions to Collected when UpgradePlan does not exist", func(t *testing.T) {
		scheme := newTestScheme(t)

		fakeClient := fake.NewClientBuilder().
			WithScheme(scheme).
			Build()

		phase := NewCollectPhase(&PhaseDeps{Client: fakeClient, Scheme: scheme})
		ul := newTestUpgradeLog()
		ul.Status.CurrentPhase = managementv1beta1.UpgradeLogPhaseCollecting

		result, err := phase.Run(context.Background(), ul)
		require.NoError(t, err)
		assert.Zero(t, result.RequeueAfter)
		assert.Equal(t, managementv1beta1.UpgradeLogPhaseCollected, ul.Status.CurrentPhase)
	})
}
