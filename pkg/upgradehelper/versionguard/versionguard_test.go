package versionguard

import (
	"testing"

	"github.com/harvester/go-common/version"
	"github.com/stretchr/testify/assert"
	"k8s.io/utils/ptr"

	managementv1beta1 "github.com/harvester/upgrade-toolkit/api/v1beta1"
)

func TestCheck(t *testing.T) {
	testCases := []struct {
		name                 string
		upgradePlan          *managementv1beta1.UpgradePlan
		strictMode           bool
		minUpgradableVersion string
		expectedErr          error
	}{
		{
			name: "upgrading from v1.2.1 to v1.2.2 with minimum upgradable version v1.2.1",
			upgradePlan: &managementv1beta1.UpgradePlan{
				Status: managementv1beta1.UpgradePlanStatus{
					PreviousVersion: ptr.To("v1.2.1"),
					ReleaseMetadata: &managementv1beta1.ReleaseMetadata{
						Harvester:            "v1.2.2",
						MinUpgradableVersion: "v1.2.1",
					},
				},
			},
			strictMode: true,
		},
		{
			name: "upgrading from v1.2.0 to v1.2.2 with minimum upgradable version v1.2.1",
			upgradePlan: &managementv1beta1.UpgradePlan{
				Status: managementv1beta1.UpgradePlanStatus{
					PreviousVersion: ptr.To("v1.2.0"),
					ReleaseMetadata: &managementv1beta1.ReleaseMetadata{
						Harvester:            "v1.2.2",
						MinUpgradableVersion: "v1.2.1",
					},
				},
			},
			strictMode:  true,
			expectedErr: version.ErrMinUpgradeRequirement,
		},
		{
			name: "upgrading from v1.2.1 to v1.2.0 with minimum upgradable version v1.1.2 (effectively downgrade)",
			upgradePlan: &managementv1beta1.UpgradePlan{
				Status: managementv1beta1.UpgradePlanStatus{
					PreviousVersion: ptr.To("v1.2.1"),
					ReleaseMetadata: &managementv1beta1.ReleaseMetadata{
						Harvester:            "v1.2.0",
						MinUpgradableVersion: "v1.1.2",
					},
				},
			},
			strictMode:  true,
			expectedErr: version.ErrDowngrade,
		},
		{
			name: "upgrading from v1.2.1 to v1.2.2-rc1 with minimum upgradable version v1.2.1 (upgrade to rc)",
			upgradePlan: &managementv1beta1.UpgradePlan{
				Status: managementv1beta1.UpgradePlanStatus{
					PreviousVersion: ptr.To("v1.2.1"),
					ReleaseMetadata: &managementv1beta1.ReleaseMetadata{
						Harvester:            "v1.2.2-rc1",
						MinUpgradableVersion: "v1.2.1",
					},
				},
			},
			strictMode: true,
		},
		{
			name: "upgrading from v1.2.2-rc1 to v1.2.2-rc2 with minimum upgradable version v1.2.1 (upgrade from rc to rc)",
			upgradePlan: &managementv1beta1.UpgradePlan{
				Status: managementv1beta1.UpgradePlanStatus{
					PreviousVersion: ptr.To("v1.2.2-rc1"),
					ReleaseMetadata: &managementv1beta1.ReleaseMetadata{
						Harvester:            "v1.2.2-rc2",
						MinUpgradableVersion: "v1.2.1",
					},
				},
			},
			strictMode: true,
		},
		{
			name: "upgrading from v1.2.2-rc2 to v1.2.2-rc1 with minimum upgradable version v1.2.1" +
				" (effectively downgrade from rc to rc)",
			upgradePlan: &managementv1beta1.UpgradePlan{
				Status: managementv1beta1.UpgradePlanStatus{
					PreviousVersion: ptr.To("v1.2.2-rc2"),
					ReleaseMetadata: &managementv1beta1.ReleaseMetadata{
						Harvester:            "v1.2.2-rc1",
						MinUpgradableVersion: "v1.2.1",
					},
				},
			},
			strictMode:  true,
			expectedErr: version.ErrDowngrade,
		},
		{
			name: "upgrading from v1.2.1 to v1.2-head without minimum upgradable version",
			upgradePlan: &managementv1beta1.UpgradePlan{
				Status: managementv1beta1.UpgradePlanStatus{
					PreviousVersion: ptr.To("v1.2.1"),
					ReleaseMetadata: &managementv1beta1.ReleaseMetadata{
						Harvester:            "v1.2-head",
						MinUpgradableVersion: "",
					},
				},
			},
			strictMode: true,
		},
		{
			name: "upgrading from v1.2-head to v1.3-head without minimum upgradable version (dev version upgrades)",
			upgradePlan: &managementv1beta1.UpgradePlan{
				Status: managementv1beta1.UpgradePlanStatus{
					PreviousVersion: ptr.To("v1.2-head"),
					ReleaseMetadata: &managementv1beta1.ReleaseMetadata{
						Harvester:            "v1.3-head",
						MinUpgradableVersion: "",
					},
				},
			},
			strictMode: true,
		},
		{
			name: "upgrading from v1.2-head to v1.3.1 with minimum upgradable version v1.2.2",
			upgradePlan: &managementv1beta1.UpgradePlan{
				Status: managementv1beta1.UpgradePlanStatus{
					PreviousVersion: ptr.To("v1.2-head"),
					ReleaseMetadata: &managementv1beta1.ReleaseMetadata{
						Harvester:            "v1.3.1",
						MinUpgradableVersion: "v1.2.2",
					},
				},
			},
			strictMode:  true,
			expectedErr: version.ErrDevUpgrade,
		},
		{
			name: "upgrading from v1.2-head to v1.3.1 with minimum upgradable version v1.2.2 (disable strict mode)",
			upgradePlan: &managementv1beta1.UpgradePlan{
				Status: managementv1beta1.UpgradePlanStatus{
					PreviousVersion: ptr.To("v1.2-head"),
					ReleaseMetadata: &managementv1beta1.ReleaseMetadata{
						Harvester:            "v1.3.1",
						MinUpgradableVersion: "v1.2.2",
					},
				},
			},
			strictMode: false,
		},
		{
			name: "upgrading from v1.2.2-rc1 to v1.2.2 with minimum upgradable version v1.2.1",
			upgradePlan: &managementv1beta1.UpgradePlan{
				Status: managementv1beta1.UpgradePlanStatus{
					PreviousVersion: ptr.To("v1.2.2-rc1"),
					ReleaseMetadata: &managementv1beta1.ReleaseMetadata{
						Harvester:            "v1.2.2",
						MinUpgradableVersion: "v1.2.1",
					},
				},
			},
			strictMode: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actualErr := Check(tc.upgradePlan, tc.strictMode, tc.minUpgradableVersion)
			if tc.expectedErr != nil {
				assert.Equal(t, tc.expectedErr, actualErr, tc.name)
			} else {
				assert.Nil(t, actualErr, tc.name)
			}
		})
	}
}
