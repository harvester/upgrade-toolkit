/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package preflight

import (
	"context"
	"errors"
	"fmt"

	"github.com/go-logr/logr"
	"github.com/harvester/go-common/version"
	harvesterv1beta1 "github.com/harvester/harvester/pkg/apis/harvesterhci.io/v1beta1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	minSupportedVersion      = "v1.7.1"
	serverVersionSettingName = "server-version"
)

// CheckMinimumVersion reads the server-version Setting CR and rejects versions
// older than the minimum supported version. Dev versions, missing settings, and
// empty values are allowed through with a warning.
func CheckMinimumVersion(ctx context.Context, reader client.Reader, logger logr.Logger) error {
	var setting harvesterv1beta1.Setting
	if err := reader.Get(ctx, types.NamespacedName{Name: serverVersionSettingName}, &setting); err != nil {
		if apierrors.IsNotFound(err) {
			logger.Info("server-version Setting not found, skipping minimum version check")
			return nil
		}
		return fmt.Errorf("failed to get server-version Setting: %w", err)
	}

	if setting.Value == "" {
		logger.Info("server-version Setting has empty value, skipping minimum version check")
		return nil
	}

	currentVersion, err := version.NewHarvesterVersion(setting.Value)
	if err != nil {
		logger.Info("unable to parse server-version, skipping minimum version check",
			"serverVersion", setting.Value, "error", err)
		return nil
	}

	minVersion, err := version.NewHarvesterVersion(minSupportedVersion)
	if err != nil {
		return fmt.Errorf("failed to parse minimum supported version %q: %w", minSupportedVersion, err)
	}

	older, err := currentVersion.IsOlder(minVersion)
	if err != nil {
		if errors.Is(err, version.ErrIncomparableVersion) {
			logger.Info("server-version is a dev build, skipping minimum version check",
				"serverVersion", setting.Value)
			return nil
		}
		return fmt.Errorf("failed to compare versions: %w", err)
	}

	if older {
		return fmt.Errorf("running Harvester version %s is older than the minimum supported version %s",
			setting.Value, minSupportedVersion)
	}

	logger.Info("minimum version check for Upgrade Manager passed",
		"serverVersion", setting.Value, "minSupportedVersion", minSupportedVersion)
	return nil
}
