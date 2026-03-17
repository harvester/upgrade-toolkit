package vmlivemigratedetector

import (
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubevirtv1 "kubevirt.io/api/core/v1"
)

func TestGetRestoreVMNames_ExcludesPausedVMs(t *testing.T) {
	vmis := []*kubevirtv1.VirtualMachineInstance{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "paused-vm",
				Namespace: "default",
			},
			Status: kubevirtv1.VirtualMachineInstanceStatus{
				Conditions: []kubevirtv1.VirtualMachineInstanceCondition{
					{
						Type:   kubevirtv1.VirtualMachineInstanceConditionType("Paused"),
						Status: corev1.ConditionTrue,
					},
				},
			},
		},
	}
	candidates := []string{"default/paused-vm"}

	result := getRestoreVMNames(vmis, candidates)
	assert.Empty(t, result, "paused VMs should be excluded from restore candidates")
}

func TestGetRestoreVMNames_ExcludesUpgradeLabeledVMs(t *testing.T) {
	vmis := []*kubevirtv1.VirtualMachineInstance{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "upgrade-repo-vm",
				Namespace: "harvester-system",
				Labels: map[string]string{
					upgradeLabel: "hvst-upgrade-abc123",
				},
			},
		},
	}
	candidates := []string{"harvester-system/upgrade-repo-vm"}

	result := getRestoreVMNames(vmis, candidates)
	assert.Empty(t, result, "upgrade-labeled VMs should be excluded")
}

func TestGetRestoreVMNames_MixedVMs(t *testing.T) {
	vmis := []*kubevirtv1.VirtualMachineInstance{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "normal-vm",
				Namespace: "default",
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "paused-vm",
				Namespace: "default",
			},
			Status: kubevirtv1.VirtualMachineInstanceStatus{
				Conditions: []kubevirtv1.VirtualMachineInstanceCondition{
					{
						Type:   kubevirtv1.VirtualMachineInstanceConditionType("Paused"),
						Status: corev1.ConditionTrue,
					},
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "upgrade-vm",
				Namespace: "harvester-system",
				Labels: map[string]string{
					upgradeLabel: "hvst-upgrade-abc123",
				},
			},
		},
	}
	candidates := []string{
		"default/normal-vm",
		"default/paused-vm",
		"harvester-system/upgrade-vm",
	}

	result := getRestoreVMNames(vmis, candidates)
	assert.Equal(t, []string{"default/normal-vm"}, result,
		"only normal VMs should be included; paused and upgrade-labeled VMs should be excluded")
}

func TestGetRestoreVMNames_NoCandidates(t *testing.T) {
	vmis := []*kubevirtv1.VirtualMachineInstance{}
	candidates := []string{}

	result := getRestoreVMNames(vmis, candidates)
	assert.Empty(t, result)
}
