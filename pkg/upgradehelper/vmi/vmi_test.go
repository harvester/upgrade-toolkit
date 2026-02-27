package vmi

import (
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubevirtv1 "kubevirt.io/api/core/v1"
)

func TestGetAllNonLiveMigratableVMINames(t *testing.T) {
	tests := []struct {
		name     string
		vmis     []*kubevirtv1.VirtualMachineInstance
		nodes    []*corev1.Node
		expected []string
	}{
		{
			name: "single non-witness node, all VMs non-migratable",
			vmis: []*kubevirtv1.VirtualMachineInstance{
				{ObjectMeta: metav1.ObjectMeta{Name: "vm1", Namespace: "default"}},
				{ObjectMeta: metav1.ObjectMeta{Name: "vm2", Namespace: "default"}},
			},
			nodes: []*corev1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "node1"}},
			},
			expected: []string{"default/vm1", "default/vm2"},
		},
		{
			name: "single non-witness node with witness nodes, all VMs non-migratable",
			vmis: []*kubevirtv1.VirtualMachineInstance{
				{ObjectMeta: metav1.ObjectMeta{Name: "vm1", Namespace: "default"}},
			},
			nodes: []*corev1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "node1"}},
				{ObjectMeta: metav1.ObjectMeta{Name: "witness1", Labels: map[string]string{witnessNodeLabelKey: "true"}}},
			},
			expected: []string{"default/vm1"},
		},
		{
			name: "multi-node, VM with node selector is non-migratable",
			vmis: []*kubevirtv1.VirtualMachineInstance{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "vm-ns", Namespace: "default"},
					Spec: kubevirtv1.VirtualMachineInstanceSpec{
						NodeSelector: map[string]string{"kubernetes.io/hostname": "node1"},
					},
				},
			},
			nodes: []*corev1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "node1"}},
				{ObjectMeta: metav1.ObjectMeta{Name: "node2"}},
			},
			expected: []string{"default/vm-ns"},
		},
		{
			name: "multi-node, VM with host devices is non-migratable",
			vmis: []*kubevirtv1.VirtualMachineInstance{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "vm-pci", Namespace: "default"},
					Spec: kubevirtv1.VirtualMachineInstanceSpec{
						Domain: kubevirtv1.DomainSpec{
							Devices: kubevirtv1.Devices{
								HostDevices: []kubevirtv1.HostDevice{{Name: "gpu", DeviceName: "nvidia.com/A100"}},
							},
						},
					},
				},
			},
			nodes: []*corev1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "node1"}},
				{ObjectMeta: metav1.ObjectMeta{Name: "node2"}},
			},
			expected: []string{"default/vm-pci"},
		},
		{
			name: "multi-node, VM with no restrictions is migratable",
			vmis: []*kubevirtv1.VirtualMachineInstance{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "vm-ok", Namespace: "default"},
					Status: kubevirtv1.VirtualMachineInstanceStatus{
						NodeName: "node1",
					},
				},
			},
			nodes: []*corev1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "node1"}},
				{ObjectMeta: metav1.ObjectMeta{Name: "node2"}},
			},
			expected: nil,
		},
		{
			name: "multi-node, VM with node affinity restricting to current node only",
			vmis: []*kubevirtv1.VirtualMachineInstance{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "vm-affinity", Namespace: "default"},
					Spec: kubevirtv1.VirtualMachineInstanceSpec{
						Affinity: &corev1.Affinity{
							NodeAffinity: &corev1.NodeAffinity{
								RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
									NodeSelectorTerms: []corev1.NodeSelectorTerm{
										{
											MatchExpressions: []corev1.NodeSelectorRequirement{
												{
													Key:      "kubernetes.io/hostname",
													Operator: corev1.NodeSelectorOpIn,
													Values:   []string{"node1"},
												},
											},
										},
									},
								},
							},
						},
					},
					Status: kubevirtv1.VirtualMachineInstanceStatus{
						NodeName: "node1",
					},
				},
			},
			nodes: []*corev1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "node1", Labels: map[string]string{"kubernetes.io/hostname": "node1"}}},
				{ObjectMeta: metav1.ObjectMeta{Name: "node2", Labels: map[string]string{"kubernetes.io/hostname": "node2"}}},
			},
			expected: []string{"default/vm-affinity"},
		},
		{
			name: "multi-node, all other nodes unschedulable",
			vmis: []*kubevirtv1.VirtualMachineInstance{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "vm-unsched", Namespace: "default"},
					Status: kubevirtv1.VirtualMachineInstanceStatus{
						NodeName: "node1",
					},
				},
			},
			nodes: []*corev1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "node1"}},
				{ObjectMeta: metav1.ObjectMeta{Name: "node2"}, Spec: corev1.NodeSpec{Unschedulable: true}},
			},
			expected: []string{"default/vm-unsched"},
		},
		{
			name: "no VMIs",
			vmis: []*kubevirtv1.VirtualMachineInstance{},
			nodes: []*corev1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "node1"}},
				{ObjectMeta: metav1.ObjectMeta{Name: "node2"}},
			},
			expected: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := GetAllNonLiveMigratableVMINames(tc.vmis, tc.nodes)
			assert.NoError(t, err)
			assert.Equal(t, tc.expected, result)
		})
	}
}
