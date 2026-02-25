package upgradeplan

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"

	managementv1beta1 "github.com/harvester/upgrade-toolkit/api/v1beta1"
)

func newTestUpgradePlan() *managementv1beta1.UpgradePlan {
	up := &managementv1beta1.UpgradePlan{}
	up.Name = testUpgradePlanName
	up.Spec.Version = testVersion
	return up
}

func TestConstructDeployment(t *testing.T) {
	up := newTestUpgradePlan()
	replicas := ptr.To[int32](2)

	deploy := constructDeployment(up, replicas)

	assert.Equal(t, fmt.Sprintf("%s-%s", testUpgradePlanName, repoComponent), deploy.Name)
	assert.Equal(t, harvesterSystemNamespace, deploy.Namespace)
	assert.Equal(t, int32(2), *deploy.Spec.Replicas)

	// Labels
	assert.Equal(t, testUpgradePlanName, deploy.Labels[HarvesterUpgradePlanLabel])
	assert.Equal(t, repoComponent, deploy.Labels[HarvesterUpgradeComponentLabel])

	// Pod template labels
	podLabels := deploy.Spec.Template.Labels
	assert.Equal(t, testUpgradePlanName, podLabels[HarvesterUpgradePlanLabel])
	assert.Equal(t, repoComponent, podLabels[HarvesterUpgradeComponentLabel])

	// Anti-affinity
	require.NotNil(t, deploy.Spec.Template.Spec.Affinity)
	require.NotNil(t, deploy.Spec.Template.Spec.Affinity.PodAntiAffinity)
	preferred := deploy.Spec.Template.Spec.Affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution
	require.Len(t, preferred, 1)
	assert.Equal(t, int32(100), preferred[0].Weight)
	assert.Equal(t, corev1.LabelHostname, preferred[0].PodAffinityTerm.TopologyKey)

	// Container
	require.Len(t, deploy.Spec.Template.Spec.Containers, 1)
	c := deploy.Spec.Template.Spec.Containers[0]
	assert.Equal(t, "nginx-iso-server", c.Name)
	assert.Equal(t, fmt.Sprintf("%s:%s", upgradeToolkitImage, testVersion), c.Image)
	assert.Equal(t, corev1.PullIfNotPresent, c.ImagePullPolicy)
	assert.True(t, *c.SecurityContext.Privileged)

	// Probes
	require.NotNil(t, c.LivenessProbe)
	require.NotNil(t, c.LivenessProbe.Exec)
	assert.Contains(t, c.LivenessProbe.Exec.Command[2], "/srv/www/htdocs/harvester-iso/harvester-release.yaml")

	require.NotNil(t, c.ReadinessProbe)
	require.NotNil(t, c.ReadinessProbe.HTTPGet)
	assert.Equal(t, "/harvester-iso/harvester-release.yaml", c.ReadinessProbe.HTTPGet.Path)
	assert.Equal(t, intstr.FromInt(80), c.ReadinessProbe.HTTPGet.Port)

	// Volume mount
	require.Len(t, c.VolumeMounts, 1)
	assert.Equal(t, "iso", c.VolumeMounts[0].Name)
	assert.Equal(t, "/iso", c.VolumeMounts[0].MountPath)

	// Volume
	require.Len(t, deploy.Spec.Template.Spec.Volumes, 1)
	vol := deploy.Spec.Template.Spec.Volumes[0]
	assert.Equal(t, "iso", vol.Name)
	assert.Equal(t, testUpgradePlanName+"-"+imageComponent, vol.PersistentVolumeClaim.ClaimName)
}

func TestConstructDeployment_SingleReplica(t *testing.T) {
	up := newTestUpgradePlan()
	replicas := ptr.To[int32](1)

	deploy := constructDeployment(up, replicas)

	assert.Equal(t, int32(1), *deploy.Spec.Replicas)
}

func TestConstructDeployment_UsesUpgradeOverride(t *testing.T) {
	up := newTestUpgradePlan()
	up.Spec.Upgrade = ptr.To("dev")
	replicas := ptr.To[int32](1)

	deploy := constructDeployment(up, replicas)

	c := deploy.Spec.Template.Spec.Containers[0]
	assert.Equal(t, upgradeToolkitImage+":dev", c.Image)
}

func TestConstructService(t *testing.T) {
	up := newTestUpgradePlan()

	svc := constructService(up)

	assert.Equal(t, fmt.Sprintf("%s-%s", testUpgradePlanName, repoComponent), svc.Name)
	assert.Equal(t, harvesterSystemNamespace, svc.Namespace)
	assert.Equal(t, testUpgradePlanName, svc.Labels[HarvesterUpgradePlanLabel])
	assert.Equal(t, repoComponent, svc.Labels[HarvesterUpgradeComponentLabel])

	require.Len(t, svc.Spec.Ports, 1)
	assert.Equal(t, int32(80), svc.Spec.Ports[0].Port)
	assert.Equal(t, corev1.ProtocolTCP, svc.Spec.Ports[0].Protocol)
	assert.Equal(t, intstr.FromInt(80), svc.Spec.Ports[0].TargetPort)

	assert.Equal(t, testUpgradePlanName, svc.Spec.Selector[HarvesterUpgradePlanLabel])
	assert.Equal(t, repoComponent, svc.Spec.Selector[HarvesterUpgradeComponentLabel])
}
