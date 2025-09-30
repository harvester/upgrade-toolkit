package upgradeplan

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/goccy/go-yaml"

	managementv1beta1 "github.com/harvester/upgrade-toolkit/api/v1beta1"
)

const (
	releaseURL = "http://localhost/harvester-release.yaml"
)

type harvesterRelease struct {
	ctx        context.Context
	httpClient *http.Client

	*managementv1beta1.UpgradePlan
	*managementv1beta1.ReleaseMetadata
}

func newHarvesterRelease(upgradePlan *managementv1beta1.UpgradePlan) *harvesterRelease {
	return &harvesterRelease{
		ctx: context.Background(),
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		UpgradePlan:     upgradePlan,
		ReleaseMetadata: &managementv1beta1.ReleaseMetadata{},
	}
}

func (h *harvesterRelease) loadReleaseMetadata() error {
	req, err := http.NewRequestWithContext(h.ctx, http.MethodGet, releaseURL, nil)
	if err != nil {
		return err
	}

	resp, err := h.httpClient.Do(req)
	if err != nil {
		return err
	}

	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status: %s", resp.Status)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	err = yaml.Unmarshal(body, h.ReleaseMetadata)
	if err != nil {
		return err
	}

	return nil
}
