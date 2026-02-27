package versionguard

import (
	"errors"

	"github.com/harvester/go-common/version"
	"github.com/sirupsen/logrus"

	managementv1beta1 "github.com/harvester/upgrade-toolkit/api/v1beta1"
)

// Check validates that the upgrade described by the UpgradePlan is eligible.
// It mirrors the upstream harvester/harvester/pkg/upgradehelper/versionguard.Check,
// adapted to read version information from the UpgradePlan status directly instead
// of parsing RepoInfo YAML.
func Check(upgradePlan *managementv1beta1.UpgradePlan, strictMode bool, minUpgradableVersionStr string) error {
	if upgradePlan.Status.ReleaseMetadata == nil {
		return errors.New("release metadata is not available in UpgradePlan status")
	}

	upgradeVersion, err := version.NewHarvesterVersion(upgradePlan.Status.ReleaseMetadata.Harvester)
	if err != nil {
		return err
	}

	if upgradePlan.Status.PreviousVersion == nil {
		return errors.New("previous version is not available in UpgradePlan status")
	}

	currentVersion, err := version.NewHarvesterVersion(*upgradePlan.Status.PreviousVersion)
	if err != nil {
		return err
	}

	var minUpgradableVersion *version.HarvesterVersion
	if minUpgradableVersionStr != "" {
		minUpgradableVersion, err = version.NewHarvesterVersion(minUpgradableVersionStr)
		if err != nil {
			return err
		}
	} else {
		minUpgradableVersion, err = version.NewHarvesterVersion(upgradePlan.Status.ReleaseMetadata.MinUpgradableVersion)
		// When the error is ErrInvalidVersion, let the nil minUpgradableVersion slip through the check since it's a
		// valid scenario. It implies "upgrade with no restrictions."
		if err != nil && !errors.Is(err, version.ErrInvalidVersion) {
			return err
		}
	}

	var minUpgradableVersionLog string
	if minUpgradableVersion != nil {
		minUpgradableVersionLog = minUpgradableVersion.String()
	}

	logrus.WithFields(logrus.Fields{
		"name":                 upgradePlan.Name,
		"currentVersion":       currentVersion.String(),
		"upgradeVersion":       upgradeVersion.String(),
		"minUpgradableVersion": minUpgradableVersionLog,
	}).Info("upgrade eligibility check")

	harvesterUpgradeVersion := version.NewHarvesterUpgradeVersion(currentVersion, upgradeVersion, minUpgradableVersion)

	return harvesterUpgradeVersion.CheckUpgradeEligibility(strictMode)
}
