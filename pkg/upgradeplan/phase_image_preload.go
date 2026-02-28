package upgradeplan

import (
	"context"

	upgradev1 "github.com/rancher/system-upgrade-controller/pkg/apis/upgrade.cattle.io/v1"
	"github.com/rancher/wrangler/v3/pkg/name"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	managementv1beta1 "github.com/harvester/upgrade-toolkit/api/v1beta1"
)

// ImagePreloadPhase preloads container images via system-upgrade-controller Plan.
type ImagePreloadPhase struct {
	*PhaseDeps
}

func NewImagePreloadPhase(deps *PhaseDeps) *ImagePreloadPhase {
	return &ImagePreloadPhase{PhaseDeps: deps}
}

func (p *ImagePreloadPhase) Name() string { return "ImagePreload" }

func (p *ImagePreloadPhase) Run(
	ctx context.Context,
	upgradePlan *managementv1beta1.UpgradePlan,
) (ctrl.Result, error) {
	p.Log.V(1).Info("handle image preload")

	plan, err := p.getOrCreatePlanForImagePreload(ctx, upgradePlan)
	if err != nil {
		p.Log.Error(err, "unable to retrieve image-preload plan from upgradeplan")
		return ctrl.Result{}, err
	}

	if !isPlanFinished(plan) {
		if isAnyPlanJobFailed(plan) {
			p.Log.V(0).Info("image-preload job failed")
			updateProgressingPhase(upgradePlan, managementv1beta1.UpgradePlanPhaseFailed, "image-preload plan job(s) failed")
			return ctrl.Result{}, nil
		}

		p.Log.V(1).Info("image-preload plan running")
		updateProgressingPhase(upgradePlan, managementv1beta1.UpgradePlanPhaseImagePreloading, "")
		return ctrl.Result{}, nil
	}

	updateProgressingPhase(upgradePlan, managementv1beta1.UpgradePlanPhaseImagePreloaded, "")
	return ctrl.Result{}, nil
}

func (p *ImagePreloadPhase) getOrCreatePlanForImagePreload(
	ctx context.Context,
	up *managementv1beta1.UpgradePlan,
) (*upgradev1.Plan, error) {
	nn := types.NamespacedName{
		Namespace: cattleSystemNamespace,
		Name:      name.SafeConcatName(up.Name, PrepareComponent),
	}
	return GetOrCreate(
		ctx, p.Client, p.Scheme, nn,
		func() *upgradev1.Plan { return &upgradev1.Plan{} },
		func() *upgradev1.Plan { return constructPlanForImagePreload(up) },
		up,
	)
}

func constructPlanForImagePreload(upgradePlan *managementv1beta1.UpgradePlan) *upgradev1.Plan {
	selector := &metav1.LabelSelector{
		MatchLabels: map[string]string{
			harvesterManagedLabel: "true",
		},
	}
	container := &upgradev1.ContainerSpec{
		Image:   upgradeToolkitImage,
		Command: []string{"upgrade_node.sh"},
		Args:    []string{"prepare"},
		Env: []corev1.EnvVar{
			{
				Name:  "HARVESTER_UPGRADEPLAN_NAME",
				Value: upgradePlan.Name,
			},
		},
	}
	version := getUpgradeVersion(upgradePlan)

	return constructPlan(upgradePlan.Name, PrepareComponent, 1, selector, false, nil, container, version)
}
