package upgradeplan

import (
	"context"
	"testing"

	"github.com/go-logr/logr"
	harvesterv1beta1 "github.com/harvester/harvester/pkg/apis/harvesterhci.io/v1beta1"
	"github.com/rancher/wrangler/v3/pkg/name"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

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
	up.Spec.Version = ptr.To("test-version")
	up.Status.Version = &managementv1beta1.VersionSpec{
		ISODownloadURL: testISOURL,
		ISOChecksum:    ptr.To(testISOChecksum),
	}

	vmImage := constructVirtualMachineImage(up)

	assert.Equal(t, name.SafeConcatName(testUpgradePlanName, imageComponent), vmImage.Name)
	assert.Equal(t, harvesterSystemNamespace, vmImage.Namespace)
	assert.Equal(t, harvesterv1beta1.VMIBackendCDI, vmImage.Spec.Backend)
	assert.Equal(t, longhornStaticStorageClassName, vmImage.Spec.TargetStorageClassName)
	assert.Equal(t, harvesterv1beta1.VirtualMachineImageSourceTypeDownload, vmImage.Spec.SourceType)
	assert.Equal(t, testISOURL, vmImage.Spec.URL)
	assert.Equal(t, testISOChecksum, vmImage.Spec.Checksum)
	assert.Equal(t, 3, vmImage.Spec.Retry)
	assert.Equal(t, testUpgradePlanName+"-"+*up.Spec.Version, vmImage.Spec.DisplayName)
	assert.Equal(t, "True", vmImage.Annotations[HarvesterUpgradeImageAnnotation])
	assert.Equal(t, testUpgradePlanName, vmImage.Labels[HarvesterUpgradePlanLabel])
	assert.Equal(t, imageComponent, vmImage.Labels[HarvesterUpgradeComponentLabel])
}

func TestConstructVirtualMachineImage_NoChecksum(t *testing.T) {
	up := &managementv1beta1.UpgradePlan{}
	up.Name = testUpgradePlanName
	up.Spec.Version = ptr.To("test-version")
	up.Status.Version = &managementv1beta1.VersionSpec{
		ISODownloadURL: testISOURL,
		ISOChecksum:    ptr.To(""),
	}

	vmImage := constructVirtualMachineImage(up)

	assert.Equal(t, "", vmImage.Spec.Checksum)
}

func newISODownloadTestScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	_ = managementv1beta1.AddToScheme(scheme)
	_ = harvesterv1beta1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	return scheme
}

func newTestVMImage(namespace, vmName string, imported *corev1.ConditionStatus) *harvesterv1beta1.VirtualMachineImage {
	vmImage := &harvesterv1beta1.VirtualMachineImage{
		ObjectMeta: metav1.ObjectMeta{
			Name:      vmName,
			Namespace: namespace,
		},
	}
	if imported != nil {
		vmImage.Status.Conditions = []harvesterv1beta1.Condition{
			{
				Type:   harvesterv1beta1.ImageImported,
				Status: *imported,
			},
		}
	}
	return vmImage
}

func TestISODownloadPhase_Run_ExistingVMI_Imported(t *testing.T) {
	scheme := newISODownloadTestScheme()
	vmImage := newTestVMImage("default", "my-iso", ptr.To(corev1.ConditionTrue))

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(vmImage).
		Build()

	phase := NewISODownloadPhase(&PhaseDeps{
		Client: fakeClient,
		Scheme: scheme,
		Log:    logr.Discard(),
	})

	up := &managementv1beta1.UpgradePlan{}
	up.Name = testUpgradePlanName
	up.Spec.Version = ptr.To("test-version")
	up.Spec.Image = ptr.To("default/my-iso")

	_, err := phase.Run(context.Background(), up)
	require.NoError(t, err)

	assert.Equal(t, managementv1beta1.UpgradePlanPhaseISODownloaded, up.Status.CurrentPhase)
	assert.Equal(t, ptr.To("default/my-iso"), up.Status.ISOImageID)
}

func TestISODownloadPhase_Run_ExistingVMI_NotImported(t *testing.T) {
	scheme := newISODownloadTestScheme()
	vmImage := newTestVMImage("default", "my-iso", nil)

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(vmImage).
		Build()

	phase := NewISODownloadPhase(&PhaseDeps{
		Client: fakeClient,
		Scheme: scheme,
		Log:    logr.Discard(),
	})

	up := &managementv1beta1.UpgradePlan{}
	up.Name = testUpgradePlanName
	up.Spec.Version = ptr.To("test-version")
	up.Spec.Image = ptr.To("default/my-iso")

	_, err := phase.Run(context.Background(), up)
	require.NoError(t, err)

	assert.Equal(t, managementv1beta1.UpgradePlanPhaseISODownloading, up.Status.CurrentPhase)
	assert.Equal(t, ptr.To("default/my-iso"), up.Status.ISOImageID)
}

func TestISODownloadPhase_Run_ExistingVMI_ImportFailed(t *testing.T) {
	scheme := newISODownloadTestScheme()
	vmImage := newTestVMImage("default", "my-iso", ptr.To(corev1.ConditionFalse))

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(vmImage).
		Build()

	phase := NewISODownloadPhase(&PhaseDeps{
		Client: fakeClient,
		Scheme: scheme,
		Log:    logr.Discard(),
	})

	up := &managementv1beta1.UpgradePlan{}
	up.Name = testUpgradePlanName
	up.Spec.Version = ptr.To("test-version")
	up.Spec.Image = ptr.To("default/my-iso")

	_, err := phase.Run(context.Background(), up)
	require.NoError(t, err)

	assert.Equal(t, managementv1beta1.UpgradePlanPhaseFailed, up.Status.CurrentPhase)
}

func TestISODownloadPhase_Run_ExistingVMI_NotFound(t *testing.T) {
	scheme := newISODownloadTestScheme()

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		Build()

	phase := NewISODownloadPhase(&PhaseDeps{
		Client: fakeClient,
		Scheme: scheme,
		Log:    logr.Discard(),
	})

	up := &managementv1beta1.UpgradePlan{}
	up.Name = testUpgradePlanName
	up.Spec.Version = ptr.To("test-version")
	up.Spec.Image = ptr.To("default/nonexistent")

	_, err := phase.Run(context.Background(), up)
	require.Error(t, err)
}

func TestISODownloadPhase_Run_ExistingVMI_DifferentNamespace(t *testing.T) {
	scheme := newISODownloadTestScheme()
	vmImage := newTestVMImage("my-namespace", "custom-iso", ptr.To(corev1.ConditionTrue))

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(vmImage).
		Build()

	phase := NewISODownloadPhase(&PhaseDeps{
		Client: fakeClient,
		Scheme: scheme,
		Log:    logr.Discard(),
	})

	up := &managementv1beta1.UpgradePlan{}
	up.Name = testUpgradePlanName
	up.Spec.Version = ptr.To("test-version")
	up.Spec.Image = ptr.To("my-namespace/custom-iso")

	_, err := phase.Run(context.Background(), up)
	require.NoError(t, err)

	assert.Equal(t, managementv1beta1.UpgradePlanPhaseISODownloaded, up.Status.CurrentPhase)
	assert.Equal(t, ptr.To("my-namespace/custom-iso"), up.Status.ISOImageID)
}

func TestISODownloadPhase_Run_ExistingVMI_NoOwnershipOrLabels(t *testing.T) {
	scheme := newISODownloadTestScheme()
	vmImage := newTestVMImage("default", "my-iso", ptr.To(corev1.ConditionTrue))

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(vmImage).
		Build()

	phase := NewISODownloadPhase(&PhaseDeps{
		Client: fakeClient,
		Scheme: scheme,
		Log:    logr.Discard(),
	})

	up := &managementv1beta1.UpgradePlan{}
	up.Name = testUpgradePlanName
	up.Spec.Version = ptr.To("test-version")
	up.Spec.Image = ptr.To("default/my-iso")

	_, err := phase.Run(context.Background(), up)
	require.NoError(t, err)

	// Verify the VMI was NOT modified with controller references or labels
	var fetched harvesterv1beta1.VirtualMachineImage
	err = fakeClient.Get(context.Background(), types.NamespacedName{
		Namespace: "default",
		Name:      "my-iso",
	}, &fetched)
	require.NoError(t, err)

	assert.Empty(t, fetched.OwnerReferences)
	assert.Empty(t, fetched.Labels)
}
