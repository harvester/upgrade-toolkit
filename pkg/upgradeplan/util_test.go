package upgradeplan

import (
	"testing"

	"github.com/stretchr/testify/assert"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/utils/ptr"
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
