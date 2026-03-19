package upgradeplan

import (
	"fmt"
	"testing"

	"github.com/rancher/wrangler/v3/pkg/name"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/utils/ptr"

	buildversion "github.com/harvester/upgrade-toolkit/pkg/version"
)

func TestConstructJobForClusterUpgrade(t *testing.T) {
	up := newTestUpgradePlan()
	serviceAccount := "harvester"

	job := constructJobForClusterUpgrade(up, serviceAccount)

	// Verify job metadata
	assert.Equal(t, name.SafeConcatName(testUpgradePlanName, ClusterComponent), job.Name)
	assert.Equal(t, harvesterSystemNamespace, job.Namespace)
	assert.Equal(t, testUpgradePlanName, job.Labels[HarvesterUpgradePlanLabel])
	assert.Equal(t, ClusterComponent, job.Labels[HarvesterUpgradeComponentLabel])

	// Verify pod template labels
	podLabels := job.Spec.Template.Labels
	assert.Equal(t, testUpgradePlanName, podLabels[HarvesterUpgradePlanLabel])
	assert.Equal(t, ClusterComponent, podLabels[HarvesterUpgradeComponentLabel])

	// Verify container
	require.Len(t, job.Spec.Template.Spec.Containers, 1)
	container := job.Spec.Template.Spec.Containers[0]
	assert.Equal(t, "apply", container.Name)
	assert.Equal(t, fmt.Sprintf("%s:%s", upgradeToolkitImage, buildversion.Version), container.Image)
	assert.Equal(t, []string{"upgrade_manifests.sh"}, container.Command)

	// Verify env vars
	require.Len(t, container.Env, 1)
	assert.Equal(t, "HARVESTER_UPGRADEPLAN_NAME", container.Env[0].Name)
	assert.Equal(t, testUpgradePlanName, container.Env[0].Value)

	// Verify SecurityContext runs as root
	require.NotNil(t, container.SecurityContext)
	require.NotNil(t, container.SecurityContext.RunAsUser)
	assert.Equal(t, int64(0), *container.SecurityContext.RunAsUser)

	// Verify TTL
	require.NotNil(t, job.Spec.TTLSecondsAfterFinished)
	assert.Equal(t, int32(defaultTTLSecondsAfterFinished), *job.Spec.TTLSecondsAfterFinished)

	// Verify pod spec
	podSpec := job.Spec.Template.Spec
	assert.Equal(t, serviceAccount, podSpec.ServiceAccountName)
	assert.NotEmpty(t, podSpec.Tolerations)
}

func TestConstructJobForClusterUpgrade_WithImageOverride(t *testing.T) {
	up := newTestUpgradePlan()
	up.Annotations = map[string]string{
		AnnotationUpgradeToolkitImage: "custom/upgrade-toolkit",
	}
	serviceAccount := "harvester"

	job := constructJobForClusterUpgrade(up, serviceAccount)

	container := job.Spec.Template.Spec.Containers[0]
	assert.Equal(t, fmt.Sprintf("custom/upgrade-toolkit:%s", buildversion.Version), container.Image)

	// SecurityContext must still be set
	require.NotNil(t, container.SecurityContext)
	assert.Equal(t, ptr.To(int64(0)), container.SecurityContext.RunAsUser)
}
