package upgradeplan

import (
	"testing"

	"github.com/stretchr/testify/assert"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/utils/ptr"

	managementv1beta1 "github.com/harvester/upgrade-toolkit/api/v1beta1"
)

func TestIsDeploymentReady(t *testing.T) {
	testCases := []struct {
		name     string
		deploy   *appsv1.Deployment
		expected bool
	}{
		{
			name: "nil replicas",
			deploy: &appsv1.Deployment{
				Spec: appsv1.DeploymentSpec{},
			},
			expected: false,
		},
		{
			name: "not enough available replicas",
			deploy: &appsv1.Deployment{
				Spec: appsv1.DeploymentSpec{
					Replicas: ptr.To[int32](2),
				},
				Status: appsv1.DeploymentStatus{
					AvailableReplicas: 1,
				},
			},
			expected: false,
		},
		{
			name: "exact replicas available",
			deploy: &appsv1.Deployment{
				Spec: appsv1.DeploymentSpec{
					Replicas: ptr.To[int32](2),
				},
				Status: appsv1.DeploymentStatus{
					AvailableReplicas: 2,
				},
			},
			expected: true,
		},
		{
			name: "more than requested available",
			deploy: &appsv1.Deployment{
				Spec: appsv1.DeploymentSpec{
					Replicas: ptr.To[int32](1),
				},
				Status: appsv1.DeploymentStatus{
					AvailableReplicas: 2,
				},
			},
			expected: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, isDeploymentReady(tc.deploy))
		})
	}
}

func TestIsTerminalState(t *testing.T) {
	testCases := []struct {
		name     string
		state    string
		expected bool
	}{
		{
			name:     "PostDrained is terminal",
			state:    managementv1beta1.NodeStatePostDrained,
			expected: true,
		},
		{
			name:     "WaitingReboot is not terminal",
			state:    managementv1beta1.NodeStateWaitingReboot,
			expected: false,
		},
		{
			name:     "PostDraining is not terminal",
			state:    managementv1beta1.NodeStatePostDraining,
			expected: false,
		},
		{
			name:     "ImagePreloaded is not terminal",
			state:    managementv1beta1.NodeStateImagePreloaded,
			expected: false,
		},
		{
			name:     "PreDrained is not terminal",
			state:    managementv1beta1.NodeStatePreDrained,
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			status := managementv1beta1.NodeUpgradeStatus{State: tc.state}
			assert.Equal(t, tc.expected, isTerminalState(status))
		})
	}
}
