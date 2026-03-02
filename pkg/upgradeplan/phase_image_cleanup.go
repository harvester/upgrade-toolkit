package upgradeplan

import (
	"context"
	"net/http"
	"strings"
	"time"

	upgradev1 "github.com/rancher/system-upgrade-controller/pkg/apis/upgrade.cattle.io/v1"
	"github.com/rancher/wrangler/v3/pkg/name"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	managementv1beta1 "github.com/harvester/upgrade-toolkit/api/v1beta1"
)

const imageCleanupScript = `#!/usr/bin/env sh
HOST_DIR="${HOST_DIR:-/host}"
export CONTAINER_RUNTIME_ENDPOINT=unix:///$HOST_DIR/run/k3s/containerd/containerd.sock
export CONTAINERD_ADDRESS=$HOST_DIR/run/k3s/containerd/containerd.sock
CRICTL="$HOST_DIR/$(readlink $HOST_DIR/var/lib/rancher/rke2/bin)/crictl"
if [ -z "$CRICTL" ]; then
    echo "Failed to locate host crictl binary."
    exit 0
fi
for img in $IMAGES; do
    echo "Removing image: $img"
    "$CRICTL" rmi "$img" || echo "  Warning: failed to remove $img (non-fatal)"
done
`

// ImageCleanupPhase purges stale container images from all nodes via a
// system-upgrade-controller Plan. It compares the image lists of the previous
// and current versions to determine which images are no longer needed.
type ImageCleanupPhase struct {
	*PhaseDeps
}

func NewImageCleanupPhase(deps *PhaseDeps) *ImageCleanupPhase {
	return &ImageCleanupPhase{PhaseDeps: deps}
}

func (p *ImageCleanupPhase) Name() string { return "ImageCleanup" }

func (p *ImageCleanupPhase) Run(
	ctx context.Context,
	upgradePlan *managementv1beta1.UpgradePlan,
) (ctrl.Result, error) {
	p.Log.V(1).Info("handle image cleanup")

	plan, err := p.getOrCreatePlanForImageCleanup(ctx, upgradePlan)
	if err != nil {
		p.Log.Error(err, "unable to get or create image-cleanup plan")
		return ctrl.Result{}, err
	}

	// Plan may be nil when there are no images to clean up.
	if plan == nil {
		p.Log.V(1).Info("no stale images to clean up, skipping")
		updateProgressingPhase(upgradePlan, managementv1beta1.UpgradePlanPhaseCleanedUp, "")
		return ctrl.Result{}, nil
	}

	if !isPlanFinished(plan) {
		if isAnyPlanJobFailed(plan) {
			p.Log.V(0).Info("image-cleanup plan job failed")
			updateProgressingPhase(upgradePlan, managementv1beta1.UpgradePlanPhaseFailed, "image-cleanup plan job(s) failed")
			return ctrl.Result{}, nil
		}

		p.Log.V(1).Info("image-cleanup plan running")
		updateProgressingPhase(upgradePlan, managementv1beta1.UpgradePlanPhaseCleaningUp, "")
		return ctrl.Result{}, nil
	}

	updateProgressingPhase(upgradePlan, managementv1beta1.UpgradePlanPhaseCleanedUp, "")
	return ctrl.Result{}, nil
}

func (p *ImageCleanupPhase) getOrCreatePlanForImageCleanup(
	ctx context.Context,
	up *managementv1beta1.UpgradePlan,
) (*upgradev1.Plan, error) {
	nn := types.NamespacedName{
		Namespace: cattleSystemNamespace,
		Name:      name.SafeConcatName(up.Name, ImageCleanupComponent),
	}

	// Check if the Plan already exists before computing the diff (avoid
	// re-fetching image lists on every reconcile).
	existing := &upgradev1.Plan{}
	if err := p.Client.Get(ctx, nn, existing); err == nil {
		return existing, nil
	}

	// Plan does not exist yet — compute the image diff.
	imagesToPurge, err := p.computeImageDiff(ctx, up)
	if err != nil {
		return nil, err
	}
	if len(imagesToPurge) == 0 {
		return nil, nil
	}

	p.Log.V(1).Info("creating image-cleanup plan", "imageCount", len(imagesToPurge))

	return GetOrCreate(
		ctx, p.Client, p.Scheme, nn,
		func() *upgradev1.Plan { return &upgradev1.Plan{} },
		func() *upgradev1.Plan {
			return constructPlanForImageCleanup(up, imagesToPurge, p.PlanServiceAccount)
		},
		up,
	)
}

func (p *ImageCleanupPhase) computeImageDiff(
	ctx context.Context,
	up *managementv1beta1.UpgradePlan,
) ([]string, error) {
	if up.Status.PreviousVersion == nil {
		return nil, nil
	}

	baseURL := repoBaseURL(up.Name)
	httpClient := &http.Client{Timeout: 30 * time.Second}

	previousVersion := *up.Status.PreviousVersion
	currentVersion := getUpgradeVersion(up)

	previousImages, err := fetchImageList(ctx, httpClient, baseURL, previousVersion)
	if err != nil {
		return nil, err
	}

	currentImages, err := fetchImageList(ctx, httpClient, baseURL, currentVersion)
	if err != nil {
		return nil, err
	}

	diff := imagesDiff(previousImages, currentImages)
	return filterRetainedImages(diff), nil
}

func constructPlanForImageCleanup(
	upgradePlan *managementv1beta1.UpgradePlan,
	imagesToPurge []string,
	serviceAccountName string,
) *upgradev1.Plan {
	selector := &metav1.LabelSelector{
		MatchLabels: map[string]string{
			harvesterManagedLabel: "true",
		},
	}
	container := &upgradev1.ContainerSpec{
		Image:   upgradeToolkitImage,
		Command: []string{"sh", "-c", imageCleanupScript},
		Env: []corev1.EnvVar{
			{
				Name:  "IMAGES",
				Value: strings.Join(imagesToPurge, " "),
			},
		},
	}
	version := getUpgradeVersion(upgradePlan)

	return constructPlan(
		upgradePlan.Name, ImageCleanupComponent, 1, selector,
		false, nil, container, version, serviceAccountName,
	)
}
