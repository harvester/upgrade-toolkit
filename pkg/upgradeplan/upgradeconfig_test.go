package upgradeplan

import (
	"testing"

	"github.com/harvester/harvester/pkg/settings"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/utils/ptr"

	managementv1beta1 "github.com/harvester/upgrade-toolkit/api/v1beta1"
)

func TestApplyUpgradeConfig(t *testing.T) {
	nodeNames := []string{"node-c", "node-a", "node-b"}
	manual := settings.ManualType
	auto := settings.AutoType
	unknownMode := settings.ModeType("bogus")

	preload := func(strategyType settings.StrategyType, concurrency int) settings.ImagePreloadOption {
		return settings.ImagePreloadOption{Strategy: settings.PreloadStrategy{Type: strategyType, Concurrency: concurrency}}
	}
	nodeUpgrade := func(mode *settings.ModeType, pauseNodes ...string) *settings.NodeUpgradeOption {
		return &settings.NodeUpgradeOption{Strategy: &settings.NodeUpgradeStrategy{Mode: mode, PauseNodes: pauseNodes}}
	}

	testCases := []struct {
		name      string
		spec      managementv1beta1.UpgradePlanSpec
		config    settings.UpgradeConfig
		expected  managementv1beta1.UpgradePlanSpec
		expectErr bool
	}{
		{
			name:   "skip strategy disables image preloading",
			config: settings.UpgradeConfig{PreloadOption: preload(settings.SkipType, 0)},
			expected: managementv1beta1.UpgradePlanSpec{
				ImagePreloadOption: &managementv1beta1.ImagePreloadOption{Concurrency: ptr.To(-1)},
				RestoreVM:          ptr.To(false),
			},
		},
		{
			name:   "sequential strategy preloads one node at a time",
			config: settings.UpgradeConfig{PreloadOption: preload(settings.SequentialType, 5)},
			expected: managementv1beta1.UpgradePlanSpec{
				ImagePreloadOption: &managementv1beta1.ImagePreloadOption{Concurrency: ptr.To(1)},
				RestoreVM:          ptr.To(false),
			},
		},
		{
			name:   "parallel strategy uses the configured concurrency",
			config: settings.UpgradeConfig{PreloadOption: preload(settings.ParallelType, 2)},
			expected: managementv1beta1.UpgradePlanSpec{
				ImagePreloadOption: &managementv1beta1.ImagePreloadOption{Concurrency: ptr.To(2)},
				RestoreVM:          ptr.To(false),
			},
		},
		{
			name:   "parallel strategy with zero concurrency means all nodes",
			config: settings.UpgradeConfig{PreloadOption: preload(settings.ParallelType, 0)},
			expected: managementv1beta1.UpgradePlanSpec{
				ImagePreloadOption: &managementv1beta1.ImagePreloadOption{Concurrency: ptr.To(0)},
				RestoreVM:          ptr.To(false),
			},
		},
		{
			name: "explicit concurrency in spec is preserved",
			spec: managementv1beta1.UpgradePlanSpec{
				ImagePreloadOption: &managementv1beta1.ImagePreloadOption{Concurrency: ptr.To(3)},
			},
			config: settings.UpgradeConfig{PreloadOption: preload(settings.SkipType, 0)},
			expected: managementv1beta1.UpgradePlanSpec{
				ImagePreloadOption: &managementv1beta1.ImagePreloadOption{Concurrency: ptr.To(3)},
				RestoreVM:          ptr.To(false),
			},
		},
		{
			name: "empty imagePreloadOption in spec is filled",
			spec: managementv1beta1.UpgradePlanSpec{
				ImagePreloadOption: &managementv1beta1.ImagePreloadOption{},
			},
			config: settings.UpgradeConfig{PreloadOption: preload(settings.SequentialType, 0)},
			expected: managementv1beta1.UpgradePlanSpec{
				ImagePreloadOption: &managementv1beta1.ImagePreloadOption{Concurrency: ptr.To(1)},
				RestoreVM:          ptr.To(false),
			},
		},
		{
			name:   "restoreVM is taken from the setting",
			config: settings.UpgradeConfig{PreloadOption: preload(settings.SequentialType, 0), RestoreVM: true},
			expected: managementv1beta1.UpgradePlanSpec{
				ImagePreloadOption: &managementv1beta1.ImagePreloadOption{Concurrency: ptr.To(1)},
				RestoreVM:          ptr.To(true),
			},
		},
		{
			name:   "explicit restoreVM false in spec is preserved",
			spec:   managementv1beta1.UpgradePlanSpec{RestoreVM: ptr.To(false)},
			config: settings.UpgradeConfig{PreloadOption: preload(settings.SequentialType, 0), RestoreVM: true},
			expected: managementv1beta1.UpgradePlanSpec{
				ImagePreloadOption: &managementv1beta1.ImagePreloadOption{Concurrency: ptr.To(1)},
				RestoreVM:          ptr.To(false),
			},
		},
		{
			name: "manual mode without pauseNodes pauses all nodes",
			config: settings.UpgradeConfig{
				PreloadOption:     preload(settings.SequentialType, 0),
				NodeUpgradeOption: nodeUpgrade(&manual),
			},
			expected: managementv1beta1.UpgradePlanSpec{
				ImagePreloadOption: &managementv1beta1.ImagePreloadOption{Concurrency: ptr.To(1)},
				NodeUpgradeOption:  &managementv1beta1.NodeUpgradeOption{PauseNodes: []string{"node-a", "node-b", "node-c"}},
				RestoreVM:          ptr.To(false),
			},
		},
		{
			name: "manual mode with pauseNodes filters out nonexistent nodes",
			config: settings.UpgradeConfig{
				PreloadOption:     preload(settings.SequentialType, 0),
				NodeUpgradeOption: nodeUpgrade(&manual, "node-c", "ghost", "node-a"),
			},
			expected: managementv1beta1.UpgradePlanSpec{
				ImagePreloadOption: &managementv1beta1.ImagePreloadOption{Concurrency: ptr.To(1)},
				NodeUpgradeOption:  &managementv1beta1.NodeUpgradeOption{PauseNodes: []string{"node-a", "node-c"}},
				RestoreVM:          ptr.To(false),
			},
		},
		{
			name: "manual mode with only nonexistent pauseNodes pauses nothing",
			config: settings.UpgradeConfig{
				PreloadOption:     preload(settings.SequentialType, 0),
				NodeUpgradeOption: nodeUpgrade(&manual, "ghost"),
			},
			expected: managementv1beta1.UpgradePlanSpec{
				ImagePreloadOption: &managementv1beta1.ImagePreloadOption{Concurrency: ptr.To(1)},
				RestoreVM:          ptr.To(false),
			},
		},
		{
			name: "auto mode pauses nothing",
			config: settings.UpgradeConfig{
				PreloadOption:     preload(settings.SequentialType, 0),
				NodeUpgradeOption: nodeUpgrade(&auto, "node-a"),
			},
			expected: managementv1beta1.UpgradePlanSpec{
				ImagePreloadOption: &managementv1beta1.ImagePreloadOption{Concurrency: ptr.To(1)},
				RestoreVM:          ptr.To(false),
			},
		},
		{
			name: "explicit empty nodeUpgradeOption in spec is preserved",
			spec: managementv1beta1.UpgradePlanSpec{
				NodeUpgradeOption: &managementv1beta1.NodeUpgradeOption{},
			},
			config: settings.UpgradeConfig{
				PreloadOption:     preload(settings.SequentialType, 0),
				NodeUpgradeOption: nodeUpgrade(&manual),
			},
			expected: managementv1beta1.UpgradePlanSpec{
				ImagePreloadOption: &managementv1beta1.ImagePreloadOption{Concurrency: ptr.To(1)},
				NodeUpgradeOption:  &managementv1beta1.NodeUpgradeOption{},
				RestoreVM:          ptr.To(false),
			},
		},
		{
			name:      "unknown strategy type is rejected",
			config:    settings.UpgradeConfig{PreloadOption: preload("bogus", 0), RestoreVM: true},
			expectErr: true,
		},
		{
			name:      "missing strategy type is rejected",
			config:    settings.UpgradeConfig{RestoreVM: true},
			expectErr: true,
		},
		{
			name:      "negative parallel concurrency is rejected",
			config:    settings.UpgradeConfig{PreloadOption: preload(settings.ParallelType, -1), RestoreVM: true},
			expectErr: true,
		},
		{
			name: "unknown node upgrade mode is rejected",
			config: settings.UpgradeConfig{
				PreloadOption:     preload(settings.SequentialType, 0),
				NodeUpgradeOption: nodeUpgrade(&unknownMode),
				RestoreVM:         true,
			},
			expectErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			spec := tc.spec.DeepCopy()
			err := ApplyUpgradeConfig(spec, &tc.config, nodeNames)
			if tc.expectErr {
				require.Error(t, err)
				assert.Equal(t, &tc.spec, spec, "spec must be untouched on error")
				return
			}
			require.NoError(t, err)
			assert.Equal(t, &tc.expected, spec)
		})
	}
}
