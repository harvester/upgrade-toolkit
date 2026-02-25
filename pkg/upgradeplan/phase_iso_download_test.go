package upgradeplan

import (
	"testing"

	harvesterv1beta1 "github.com/harvester/harvester/pkg/apis/harvesterhci.io/v1beta1"
	"github.com/stretchr/testify/assert"
	"k8s.io/utils/ptr"

	managementv1beta1 "github.com/harvester/upgrade-toolkit/api/v1beta1"
)

const (
	testUpgradePlanName = "test-upgradeplan"
	testVersion         = "test-version"
	testISOURL          = "test-iso-url"
	testISOChecksum     = "test-iso-checksum"
)

func TestConstructVirtualMachineImage(t *testing.T) {
	up := &managementv1beta1.UpgradePlan{}
	up.Name = testUpgradePlanName
	up.Spec.Version = testVersion
	up.Status.Version = &managementv1beta1.VersionSpec{
		ISODownloadURL: testISOURL,
		ISOChecksum:    ptr.To(testISOChecksum),
	}

	vmImage := constructVirtualMachineImage(up)

	assert.Equal(t, testUpgradePlanName+"-iso", vmImage.Name)
	assert.Equal(t, harvesterSystemNamespace, vmImage.Namespace)
	assert.Equal(t, harvesterv1beta1.VMIBackendCDI, vmImage.Spec.Backend)
	assert.Equal(t, longhornStaticStorageClassName, vmImage.Spec.TargetStorageClassName)
	assert.Equal(t, harvesterv1beta1.VirtualMachineImageSourceTypeDownload, vmImage.Spec.SourceType)
	assert.Equal(t, testISOURL, vmImage.Spec.URL)
	assert.Equal(t, testISOChecksum, vmImage.Spec.Checksum)
	assert.Equal(t, 3, vmImage.Spec.Retry)
	assert.Equal(t, testUpgradePlanName+"-"+testVersion, vmImage.Spec.DisplayName)
	assert.Equal(t, "True", vmImage.Annotations[HarvesterUpgradeImageAnnotation])
	assert.Equal(t, testUpgradePlanName, vmImage.Labels[HarvesterUpgradePlanLabel])
	assert.Equal(t, imageComponent, vmImage.Labels[HarvesterUpgradeComponentLabel])
}

func TestConstructVirtualMachineImage_NoChecksum(t *testing.T) {
	up := &managementv1beta1.UpgradePlan{}
	up.Name = testUpgradePlanName
	up.Spec.Version = testVersion
	up.Status.Version = &managementv1beta1.VersionSpec{
		ISODownloadURL: testISOURL,
		ISOChecksum:    ptr.To(""),
	}

	vmImage := constructVirtualMachineImage(up)

	assert.Equal(t, "", vmImage.Spec.Checksum)
}
