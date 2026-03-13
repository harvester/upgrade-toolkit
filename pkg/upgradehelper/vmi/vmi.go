package vmi

import (
	"fmt"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	schedulingcorev1 "k8s.io/component-helpers/scheduling/corev1"
	kubevirtv1 "kubevirt.io/api/core/v1"
)

const (
	witnessNodeLabelKey = "node-role.harvesterhci.io/witness"
)

// GetAllNonLiveMigratableVMINames returns the namespaced names (namespace/name) of VMIs
// that cannot be live-migrated to another node. This is a reimplementation of the upstream
// harvester/harvester/pkg/util/virtualmachineinstance.GetAllNonLiveMigratableVMINames.
func GetAllNonLiveMigratableVMINames(
	log logr.Logger,
	vmis []*kubevirtv1.VirtualMachineInstance, nodes []*corev1.Node,
) ([]string, error) {
	var nonLiveMigratableVMINames []string

	nonWitnessNodes := excludeWitnessNodes(nodes)

	// If there is only one node, all VMs are non-migratable
	if len(nonWitnessNodes) == 1 {
		for _, vmi := range vmis {
			vmiNamespacedName := fmt.Sprintf("%s/%s", vmi.Namespace, vmi.Name)
			nonLiveMigratableVMINames = append(nonLiveMigratableVMINames, vmiNamespacedName)
		}
		return nonLiveMigratableVMINames, nil
	}

	for _, vmi := range vmis {
		vmiNamespacedName := fmt.Sprintf("%s/%s", vmi.Namespace, vmi.Name)

		// Node selectors
		if vmi.Spec.NodeSelector != nil {
			log.Info("VMI considered non-live migratable due to node selectors", "vmi", vmiNamespacedName)
			nonLiveMigratableVMINames = append(nonLiveMigratableVMINames, vmiNamespacedName)
			continue
		}

		// PCIe devices
		if vmi.Spec.Domain.Devices.HostDevices != nil {
			log.Info("VMI considered non-live migratable due to pcie or usb devices", "vmi", vmiNamespacedName)
			nonLiveMigratableVMINames = append(nonLiveMigratableVMINames, vmiNamespacedName)
			continue
		}

		// Node affinities
		migratable, err := migratableByNodeAffinity(vmi, nonWitnessNodes)
		if err != nil {
			return nonLiveMigratableVMINames, err
		}
		if !migratable {
			log.Info("VMI considered non-live migratable due to node affinities", "vmi", vmiNamespacedName)
			nonLiveMigratableVMINames = append(nonLiveMigratableVMINames, vmiNamespacedName)
			continue
		}
	}

	return nonLiveMigratableVMINames, nil
}

func excludeWitnessNodes(nodes []*corev1.Node) []*corev1.Node {
	nonWitnessNodes := make([]*corev1.Node, 0, len(nodes))
	for _, node := range nodes {
		if _, ok := node.Labels[witnessNodeLabelKey]; !ok {
			nonWitnessNodes = append(nonWitnessNodes, node)
		}
	}
	return nonWitnessNodes
}

func migratableByNodeAffinity(vmi *kubevirtv1.VirtualMachineInstance, nodes []*corev1.Node) (bool, error) {
	migratabilityMap := make(map[string]bool, len(nodes)-1)
	for _, node := range nodes {
		// Skip the node the VM currently run on
		if vmi.Status.NodeName == node.Name {
			continue
		}

		migratabilityMap[node.Name] = true

		if node.Spec.Unschedulable {
			migratabilityMap[node.Name] = false
			continue
		}

		affinity := vmi.Spec.Affinity
		if affinity != nil && affinity.NodeAffinity != nil &&
			affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution != nil {
			nodeSelectorTerms := vmi.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution

			var err error
			migratabilityMap[node.Name], err = schedulingcorev1.MatchNodeSelectorTerms(node, nodeSelectorTerms)
			if err != nil {
				return false, err
			}
		}
	}

	var migratable bool
	for _, isMigratable := range migratabilityMap {
		if isMigratable {
			migratable = true
			break
		}
	}

	return migratable, nil
}
