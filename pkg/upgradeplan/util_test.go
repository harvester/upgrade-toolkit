package upgradeplan

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
		state    managementv1beta1.NodeUpgradeState
		expected bool
	}{
		{
			name:     "PostDrained is terminal",
			state:    managementv1beta1.NodeStatePostDrained,
			expected: true,
		},
		{
			name:     "SingleNodeUpgraded is terminal",
			state:    managementv1beta1.NodeStateSingleNodeUpgraded,
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
		{
			name:     "SingleNodeUpgrading is not terminal",
			state:    managementv1beta1.NodeStateSingleNodeUpgrading,
			expected: false,
		},
		{
			name:     "ImageCleaning is not terminal",
			state:    managementv1beta1.NodeStateImageCleaning,
			expected: false,
		},
		{
			name:     "ImageCleaned is not terminal",
			state:    managementv1beta1.NodeStateImageCleaned,
			expected: false,
		},
		{
			name:     "ImageCleanFailed is not terminal",
			state:    managementv1beta1.NodeStateImageCleanFailed,
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

func TestIsNodeUpgradeFailure(t *testing.T) {
	testCases := []struct {
		name     string
		state    managementv1beta1.NodeUpgradeState
		expected bool
	}{
		{
			name:     "PreDrainFailed is failure",
			state:    managementv1beta1.NodeStatePreDrainFailed,
			expected: true,
		},
		{
			name:     "PostDrainFailed is failure",
			state:    managementv1beta1.NodeStatePostDrainFailed,
			expected: true,
		},
		{
			name:     "SingleNodeUpgradeFailed is failure",
			state:    managementv1beta1.NodeStateSingleNodeUpgradeFailed,
			expected: true,
		},
		{
			name:     "PostDrained is not failure",
			state:    managementv1beta1.NodeStatePostDrained,
			expected: false,
		},
		{
			name:     "SingleNodeUpgraded is not failure",
			state:    managementv1beta1.NodeStateSingleNodeUpgraded,
			expected: false,
		},
		{
			name:     "ImagePreloaded is not failure",
			state:    managementv1beta1.NodeStateImagePreloaded,
			expected: false,
		},
		{
			name:     "ImageCleaning is not failure",
			state:    managementv1beta1.NodeStateImageCleaning,
			expected: false,
		},
		{
			name:     "ImageCleaned is not failure",
			state:    managementv1beta1.NodeStateImageCleaned,
			expected: false,
		},
		{
			name:     "ImageCleanFailed is not failure",
			state:    managementv1beta1.NodeStateImageCleanFailed,
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			status := managementv1beta1.NodeUpgradeStatus{State: tc.state}
			assert.Equal(t, tc.expected, IsNodeUpgradeFailure(status))
		})
	}
}

func TestIsNodeUpgradeStateAhead_SingleNode(t *testing.T) {
	testCases := []struct {
		name     string
		current  managementv1beta1.NodeUpgradeState
		proposed managementv1beta1.NodeUpgradeState
		expected bool
	}{
		{
			name:     "WaitingReboot is ahead of SingleNodeUpgrading",
			current:  managementv1beta1.NodeStateWaitingReboot,
			proposed: managementv1beta1.NodeStateSingleNodeUpgrading,
			expected: true,
		},
		{
			name:     "SingleNodeUpgraded is ahead of WaitingReboot",
			current:  managementv1beta1.NodeStateSingleNodeUpgraded,
			proposed: managementv1beta1.NodeStateWaitingReboot,
			expected: true,
		},
		{
			name:     "SingleNodeUpgradeFailed is ahead of SingleNodeUpgrading",
			current:  managementv1beta1.NodeStateSingleNodeUpgradeFailed,
			proposed: managementv1beta1.NodeStateSingleNodeUpgrading,
			expected: true,
		},
		{
			name:     "SingleNodeUpgrading is not ahead of WaitingReboot",
			current:  managementv1beta1.NodeStateSingleNodeUpgrading,
			proposed: managementv1beta1.NodeStateWaitingReboot,
			expected: false,
		},
		{
			name:     "SingleNodeUpgraded and PostDrained are at same ordinal",
			current:  managementv1beta1.NodeStateSingleNodeUpgraded,
			proposed: managementv1beta1.NodeStatePostDrained,
			expected: false,
		},
		{
			name:     "SingleNodeUpgrading and PreDraining are at same ordinal",
			current:  managementv1beta1.NodeStateSingleNodeUpgrading,
			proposed: managementv1beta1.NodeStatePreDraining,
			expected: false,
		},
		{
			name:     "ImageCleaning is ahead of PostDrained",
			current:  managementv1beta1.NodeStateImageCleaning,
			proposed: managementv1beta1.NodeStatePostDrained,
			expected: true,
		},
		{
			name:     "ImageCleaned is ahead of ImageCleaning",
			current:  managementv1beta1.NodeStateImageCleaned,
			proposed: managementv1beta1.NodeStateImageCleaning,
			expected: true,
		},
		{
			name:     "ImageCleanFailed is ahead of ImageCleaning",
			current:  managementv1beta1.NodeStateImageCleanFailed,
			proposed: managementv1beta1.NodeStateImageCleaning,
			expected: true,
		},
		{
			name:     "ImageCleaned and ImageCleanFailed are at same ordinal",
			current:  managementv1beta1.NodeStateImageCleaned,
			proposed: managementv1beta1.NodeStateImageCleanFailed,
			expected: false,
		},
		{
			name:     "PostDrained is not ahead of ImageCleaning",
			current:  managementv1beta1.NodeStatePostDrained,
			proposed: managementv1beta1.NodeStateImageCleaning,
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, managementv1beta1.IsNodeUpgradeStateAhead(tc.current, tc.proposed))
		})
	}
}

func TestConstructNodeJob_SingleNodeUpgrade(t *testing.T) {
	up := &managementv1beta1.UpgradePlan{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-upgrade",
		},
		Spec: managementv1beta1.UpgradePlanSpec{
			Version: "v1.4.0",
		},
	}

	job := ConstructNodeJob(
		up, "node-1", "test-upgrade-node-upgrade-single-node-upgrade-node-1",
		JobTypeSingleNodeUpgrade, "harvester", false,
	)

	// Verify labels
	assert.Equal(t, "test-upgrade", job.Labels[HarvesterUpgradePlanLabel])
	assert.Equal(t, NodeComponent, job.Labels[HarvesterUpgradeComponentLabel])
	assert.Equal(t, JobTypeSingleNodeUpgrade, job.Labels[HarvesterJobTypeLabel])
	assert.Equal(t, "node-1", job.Labels[HarvesterUpgradeNodeLabel])

	// Verify namespace
	assert.Equal(t, "harvester-system", job.Namespace)

	// Verify container
	require.Len(t, job.Spec.Template.Spec.Containers, 1)
	container := job.Spec.Template.Spec.Containers[0]
	assert.Equal(t, "apply", container.Name)
	assert.Equal(t, []string{"upgrade_node.sh"}, container.Command)
	assert.Equal(t, []string{"single-node-upgrade"}, container.Args)

	// Verify env vars
	envMap := make(map[string]string)
	for _, env := range container.Env {
		if env.Value != "" {
			envMap[env.Name] = env.Value
		}
	}
	assert.Equal(t, "test-upgrade", envMap["HARVESTER_UPGRADEPLAN_NAME"])
	assert.Equal(t, "node-1", envMap["HARVESTER_UPGRADE_NODE_NAME"])

	// Verify HARVESTER_UPGRADE_POD_NAME uses fieldRef
	var podNameEnv *corev1.EnvVar
	for i := range container.Env {
		if container.Env[i].Name == "HARVESTER_UPGRADE_POD_NAME" {
			podNameEnv = &container.Env[i]
			break
		}
	}
	require.NotNil(t, podNameEnv)
	require.NotNil(t, podNameEnv.ValueFrom)
	assert.Equal(t, "metadata.name", podNameEnv.ValueFrom.FieldRef.FieldPath)

	// Verify pod spec
	podSpec := job.Spec.Template.Spec
	assert.Equal(t, "node-1", podSpec.NodeName)
	assert.Equal(t, corev1.RestartPolicyNever, podSpec.RestartPolicy)
	assert.True(t, podSpec.HostPID)
	assert.Equal(t, "harvester", podSpec.ServiceAccountName)

	// Verify privileged
	require.NotNil(t, container.SecurityContext)
	require.NotNil(t, container.SecurityContext.Privileged)
	assert.True(t, *container.SecurityContext.Privileged)

	// Verify host-root volume mount
	require.Len(t, container.VolumeMounts, 1)
	assert.Equal(t, "host-root", container.VolumeMounts[0].Name)
	assert.Equal(t, "/host", container.VolumeMounts[0].MountPath)
}

func TestConstructNodeJob_Suspend(t *testing.T) {
	up := &managementv1beta1.UpgradePlan{
		ObjectMeta: metav1.ObjectMeta{Name: "test-upgrade"},
		Spec:       managementv1beta1.UpgradePlanSpec{Version: "v1.4.0"},
	}

	t.Run("suspend=true sets spec.suspend", func(t *testing.T) {
		job := ConstructNodeJob(up, "node-1", "job-1", JobTypePreDrain, "sa", true)
		require.NotNil(t, job.Spec.Suspend)
		assert.True(t, *job.Spec.Suspend)
	})

	t.Run("suspend=false leaves spec.suspend nil", func(t *testing.T) {
		job := ConstructNodeJob(up, "node-1", "job-1", JobTypePreDrain, "sa", false)
		assert.Nil(t, job.Spec.Suspend)
	})
}

func TestShouldPauseNode(t *testing.T) {
	testCases := []struct {
		name     string
		option   *managementv1beta1.NodeUpgradeOption
		nodeName string
		expected bool
	}{
		{
			name:     "nil option",
			option:   nil,
			nodeName: "node-1",
			expected: false,
		},
		{
			name:     "empty pauseNodes",
			option:   &managementv1beta1.NodeUpgradeOption{},
			nodeName: "node-1",
			expected: false,
		},
		{
			name: "node in pauseNodes",
			option: &managementv1beta1.NodeUpgradeOption{
				PauseNodes: []string{"node-1", "node-2"},
			},
			nodeName: "node-1",
			expected: true,
		},
		{
			name: "node not in pauseNodes",
			option: &managementv1beta1.NodeUpgradeOption{
				PauseNodes: []string{"node-2", "node-3"},
			},
			nodeName: "node-1",
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			up := &managementv1beta1.UpgradePlan{
				Spec: managementv1beta1.UpgradePlanSpec{
					NodeUpgradeOption: tc.option,
				},
			}
			assert.Equal(t, tc.expected, ShouldPauseNode(up, tc.nodeName))
		})
	}
}

func TestIsNodeUpgradeStateAhead_UpgradePaused(t *testing.T) {
	// UpgradePaused is ahead of ImagePreloaded
	assert.True(t, managementv1beta1.IsNodeUpgradeStateAhead(
		managementv1beta1.NodeStateUpgradePaused,
		managementv1beta1.NodeStateImagePreloaded,
	))

	// ImagePreloaded is NOT ahead of UpgradePaused
	assert.False(t, managementv1beta1.IsNodeUpgradeStateAhead(
		managementv1beta1.NodeStateImagePreloaded,
		managementv1beta1.NodeStateUpgradePaused,
	))

	// PreDraining is ahead of UpgradePaused
	assert.True(t, managementv1beta1.IsNodeUpgradeStateAhead(
		managementv1beta1.NodeStatePreDraining,
		managementv1beta1.NodeStateUpgradePaused,
	))

	// UpgradePaused is NOT ahead of PreDraining
	assert.False(t, managementv1beta1.IsNodeUpgradeStateAhead(
		managementv1beta1.NodeStateUpgradePaused,
		managementv1beta1.NodeStatePreDraining,
	))
}
