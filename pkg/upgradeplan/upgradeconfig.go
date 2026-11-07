package upgradeplan

import (
	"fmt"
	"slices"

	"github.com/harvester/harvester/pkg/settings"
	"k8s.io/utils/ptr"

	managementv1beta1 "github.com/harvester/upgrade-toolkit/api/v1beta1"
)

// UpgradeConfigSettingName is the Harvester Setting holding the cluster-wide upgrade customization.
const UpgradeConfigSettingName = settings.UpgradeConfigSettingName

// ApplyUpgradeConfig fills the unset fields of spec with the cluster-wide
// customization from the upgrade-config Setting. Explicitly specified fields
// always take precedence. nodeNames lists the existing nodes and is used to
// resolve which nodes to pause in manual node upgrade mode. The config is
// validated before any change is made, so spec is untouched on error.
func ApplyUpgradeConfig(
	spec *managementv1beta1.UpgradePlanSpec, cfg *settings.UpgradeConfig, nodeNames []string,
) error {
	concurrency, err := imagePreloadConcurrencyFromConfig(cfg.PreloadOption)
	if err != nil {
		return err
	}
	pauseNodes, err := pauseNodesFromConfig(cfg.NodeUpgradeOption, nodeNames)
	if err != nil {
		return err
	}

	if spec.ImagePreloadOption == nil {
		spec.ImagePreloadOption = &managementv1beta1.ImagePreloadOption{}
	}
	if spec.ImagePreloadOption.Concurrency == nil {
		spec.ImagePreloadOption.Concurrency = ptr.To(concurrency)
	}

	if spec.NodeUpgradeOption == nil && len(pauseNodes) > 0 {
		spec.NodeUpgradeOption = &managementv1beta1.NodeUpgradeOption{PauseNodes: pauseNodes}
	}

	if spec.RestoreVM == nil {
		spec.RestoreVM = ptr.To(cfg.RestoreVM)
	}

	return nil
}

// imagePreloadConcurrencyFromConfig translates the V1 image preload strategy
// into the V2 concurrency semantics (-1: skip, 0: all nodes, N: N nodes).
func imagePreloadConcurrencyFromConfig(opt settings.ImagePreloadOption) (int, error) {
	switch opt.Strategy.Type {
	case settings.SkipType:
		return -1, nil
	case settings.SequentialType:
		return 1, nil
	case settings.ParallelType:
		if opt.Strategy.Concurrency < 0 {
			return 0, fmt.Errorf("invalid image preload strategy concurrency: %d", opt.Strategy.Concurrency)
		}
		return opt.Strategy.Concurrency, nil
	default:
		return 0, fmt.Errorf("invalid image preload strategy type: %q", opt.Strategy.Type)
	}
}

// pauseNodesFromConfig returns the sorted names of the nodes to pause. In
// manual mode, the configured pause nodes that exist are returned, or all
// nodes if none are configured. Otherwise, no nodes are paused.
func pauseNodesFromConfig(opt *settings.NodeUpgradeOption, nodeNames []string) ([]string, error) {
	if opt == nil || opt.Strategy == nil || opt.Strategy.Mode == nil {
		return nil, nil
	}

	switch *opt.Strategy.Mode {
	case settings.AutoType:
		return nil, nil
	case settings.ManualType:
	default:
		return nil, fmt.Errorf("invalid node upgrade strategy mode: %q", *opt.Strategy.Mode)
	}

	var pauseNodes []string
	if len(opt.Strategy.PauseNodes) == 0 {
		pauseNodes = slices.Clone(nodeNames)
	} else {
		for _, n := range nodeNames {
			if slices.Contains(opt.Strategy.PauseNodes, n) {
				pauseNodes = append(pauseNodes, n)
			}
		}
	}
	slices.Sort(pauseNodes)

	return pauseNodes, nil
}
