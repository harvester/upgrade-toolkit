package upgradeplan

import (
	"context"

	"github.com/go-logr/logr"

	upgradev1 "github.com/rancher/system-upgrade-controller/pkg/apis/upgrade.cattle.io/v1"
	"github.com/rancher/wrangler/v3/pkg/name"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"

	managementv1beta1 "github.com/harvester/upgrade-toolkit/api/v1beta1"
	"github.com/harvester/upgrade-toolkit/pkg/upgradehelper/versionguard"
)

// ImagePreloadPhase preloads container images via system-upgrade-controller Plan.
type ImagePreloadPhase struct {
	*PhaseDeps
}

func NewImagePreloadPhase(deps *PhaseDeps) *ImagePreloadPhase {
	return &ImagePreloadPhase{PhaseDeps: deps}
}

func (p *ImagePreloadPhase) Name() string { return "ImagePreload" }

// PreRun performs the upgrade eligibility check before image preloading begins.
// When the check fails the UpgradePlan is moved to the Failed phase. Returning
// nil (instead of an error) lets the reconciler persist the Failed status rather
// than retrying indefinitely.
func (p *ImagePreloadPhase) PreRun(
	ctx context.Context,
	upgradePlan *managementv1beta1.UpgradePlan,
) error {
	log := logr.FromContextOrDiscard(ctx)
	log.V(1).Info("running upgrade eligibility check")

	if upgradePlan.Spec.Force != nil && *upgradePlan.Spec.Force {
		log.V(0).Info("force mode enabled, skipping upgrade eligibility check")
		return nil
	}

	if err := versionguard.Check(log, upgradePlan, true, ""); err != nil {
		log.Error(err, "upgrade eligibility check failed")
		updateProgressingPhase(upgradePlan, managementv1beta1.UpgradePlanPhaseFailed, err.Error())
		return nil
	}

	return nil
}

func (p *ImagePreloadPhase) Run(
	ctx context.Context,
	upgradePlan *managementv1beta1.UpgradePlan,
) (ctrl.Result, error) {
	log := logr.FromContextOrDiscard(ctx)
	log.V(1).Info("handle image preload")

	nodeCount, err := countManagedNodes(ctx, p.Client)
	if err != nil {
		log.Error(err, "unable to count managed nodes")
		return ctrl.Result{}, err
	}

	concurrency, skip := resolveImagePreloadConcurrency(
		upgradePlan.Spec.ImagePreloadOption, nodeCount,
	)
	if skip {
		log.V(0).Info("image preloading skipped (negative concurrency)")
		updateProgressingPhase(upgradePlan, managementv1beta1.UpgradePlanPhaseImagePreloaded, "")
		return ctrl.Result{}, nil
	}

	plan, err := p.getOrCreatePlanForImagePreload(ctx, upgradePlan, concurrency)
	if err != nil {
		log.Error(err, "unable to retrieve image-preload plan from upgradeplan")
		return ctrl.Result{}, err
	}

	if !isPlanFinished(plan) {
		if isAnyPlanJobFailed(plan) {
			log.V(0).Info("image-preload job failed")
			updateProgressingPhase(upgradePlan, managementv1beta1.UpgradePlanPhaseFailed, "image-preload plan job(s) failed")
			return ctrl.Result{}, nil
		}

		log.V(1).Info("image-preload plan running")
		updateProgressingPhase(upgradePlan, managementv1beta1.UpgradePlanPhaseImagePreloading, "")
		return ctrl.Result{}, nil
	}

	updateProgressingPhase(upgradePlan, managementv1beta1.UpgradePlanPhaseImagePreloaded, "")
	return ctrl.Result{}, nil
}

func (p *ImagePreloadPhase) getOrCreatePlanForImagePreload(
	ctx context.Context,
	up *managementv1beta1.UpgradePlan,
	concurrency int,
) (*upgradev1.Plan, error) {
	nn := types.NamespacedName{
		Namespace: cattleSystemNamespace,
		Name:      name.SafeConcatName(up.Name, PrepareComponent),
	}
	obj, _, err := GetOrCreate(
		ctx, p.Client, p.Scheme, nn,
		func() *upgradev1.Plan { return &upgradev1.Plan{} },
		func() *upgradev1.Plan { return constructPlanForImagePreload(up, concurrency, p.PlanServiceAccount) },
		up,
	)
	return obj, err
}

func constructPlanForImagePreload(
	upgradePlan *managementv1beta1.UpgradePlan, concurrency int, serviceAccountName string,
) *upgradev1.Plan {
	selector := &metav1.LabelSelector{
		MatchLabels: map[string]string{
			harvesterManagedLabel: valueTrue,
		},
	}
	container := &upgradev1.ContainerSpec{
		Image:   getUpgradeToolkitImage(upgradePlan),
		Command: []string{"upgrade_node.sh"},
		Args:    []string{"prepare"},
		Env: []corev1.EnvVar{
			{
				Name:  "HARVESTER_UPGRADEPLAN_NAME",
				Value: upgradePlan.Name,
			},
		},
		SecurityContext: &corev1.SecurityContext{
			Privileged: ptr.To(true),
			RunAsUser:  ptr.To(int64(0)),
		},
	}
	version := getUpgradeVersion(upgradePlan)

	return constructPlan(
		upgradePlan.Name, PrepareComponent, concurrency, selector,
		false, nil, container, version, serviceAccountName,
	)
}
