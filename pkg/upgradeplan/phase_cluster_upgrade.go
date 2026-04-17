package upgradeplan

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	"github.com/rancher/wrangler/v3/pkg/name"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	managementv1beta1 "github.com/harvester/upgrade-toolkit/api/v1beta1"
)

// ClusterUpgradePhase applies upgrade manifests via a Kubernetes Job.
type ClusterUpgradePhase struct {
	*PhaseDeps
}

func NewClusterUpgradePhase(deps *PhaseDeps) *ClusterUpgradePhase {
	return &ClusterUpgradePhase{PhaseDeps: deps}
}

func (p *ClusterUpgradePhase) Name() string { return "ClusterUpgrade" }

// PreRun disables KubeVirt's LiveMigrate workload update method before the
// cluster-upgrade job runs, so the KubeVirt operator upgrade does not trigger
// a migration storm across every running VM. VMs migrate naturally during node
// upgrades. CPU/memory hot-plugging is unavailable for the duration of the
// upgrade and is restored during cleanup.
func (p *ClusterUpgradePhase) PreRun(
	ctx context.Context,
	upgradePlan *managementv1beta1.UpgradePlan,
) error {
	log := logr.FromContextOrDiscard(ctx)
	return disableKubevirtWorkloadLiveMigrate(ctx, p.Client, log)
}

// disableKubevirtWorkloadLiveMigrate clears KubeVirt workloadUpdateMethods and
// adds the matching comparePatch entry to the harvester ManagedChart so Fleet
// does not revert the change as drift. Idempotent.
func disableKubevirtWorkloadLiveMigrate(
	ctx context.Context,
	c client.Client,
	log logr.Logger,
) error {
	kv, err := getKubeVirt(ctx, c)
	if err != nil {
		return err
	}
	if kv == nil {
		log.V(1).Info("kubevirt object not found, skipping LiveMigrate disable")
		return addKubevirtComparePatches(ctx, c, log)
	}

	if err := setKubeVirtWorkloadUpdateMethods(ctx, c, kv, []string{}); err != nil {
		return fmt.Errorf("failed to clear kubevirt workloadUpdateMethods: %w", err)
	}
	log.Info("ensured KubeVirt workloadUpdateMethods is empty to disable LiveMigrate during upgrade")

	return addKubevirtComparePatches(ctx, c, log)
}

func (p *ClusterUpgradePhase) Run(
	ctx context.Context,
	upgradePlan *managementv1beta1.UpgradePlan,
) (ctrl.Result, error) {
	log := logr.FromContextOrDiscard(ctx)
	log.V(1).Info("handle cluster upgrade")

	job, err := p.getOrCreateJobForClusterUpgrade(ctx, upgradePlan)
	if err != nil {
		log.Error(err, "unable to retrieve cluster-upgrade job from upgradeplan")
		return ctrl.Result{}, err
	}

	finished, success := isJobFinished(job)

	if !finished {
		log.V(1).Info("cluster-upgrade job running")
		updateProgressingPhase(upgradePlan, managementv1beta1.UpgradePlanPhaseClusterUpgrading, "")
		return ctrl.Result{}, nil
	}

	if !success {
		log.V(0).Info("cluster-upgrade job failed")
		updateProgressingPhase(upgradePlan, managementv1beta1.UpgradePlanPhaseFailed, "cluster-upgrade job failed")
		return ctrl.Result{}, nil
	}

	updateProgressingPhase(upgradePlan, managementv1beta1.UpgradePlanPhaseClusterUpgraded, "")
	return ctrl.Result{}, nil
}

func (p *ClusterUpgradePhase) getOrCreateJobForClusterUpgrade(
	ctx context.Context,
	up *managementv1beta1.UpgradePlan,
) (*batchv1.Job, error) {
	nn := types.NamespacedName{
		Namespace: harvesterSystemNamespace,
		Name:      name.SafeConcatName(up.Name, ClusterComponent),
	}
	obj, _, err := GetOrCreate(
		ctx, p.Client, p.Scheme, nn,
		func() *batchv1.Job { return &batchv1.Job{} },
		func() *batchv1.Job { return constructJobForClusterUpgrade(up, p.JobServiceAccount) },
		up,
	)
	return obj, err
}

func constructJobForClusterUpgrade(upgradePlan *managementv1beta1.UpgradePlan, serviceAccountName string) *batchv1.Job {
	jobName := name.SafeConcatName(upgradePlan.Name, ClusterComponent)
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				HarvesterUpgradePlanLabel:      upgradePlan.Name,
				HarvesterUpgradeComponentLabel: ClusterComponent,
			},
			Name:      jobName,
			Namespace: harvesterSystemNamespace,
		},
		Spec: batchv1.JobSpec{
			TTLSecondsAfterFinished: ptr.To[int32](defaultTTLSecondsAfterFinished),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						HarvesterUpgradePlanLabel:      upgradePlan.Name,
						HarvesterUpgradeComponentLabel: ClusterComponent,
					},
				},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{
						{
							Name:  "apply",
							Image: fmt.Sprintf("%s:%s", getUpgradeToolkitImage(upgradePlan), getUpgradeVersion(upgradePlan)),
							Command: []string{
								"upgrade_manifests.sh",
							},
							Env: []corev1.EnvVar{
								{
									Name:  "HARVESTER_UPGRADEPLAN_NAME",
									Value: upgradePlan.Name,
								},
							},
							SecurityContext: &corev1.SecurityContext{
								RunAsUser: ptr.To(int64(0)),
							},
						},
					},
					ServiceAccountName: serviceAccountName,
					Tolerations:        getDefaultTolerations(),
				},
			},
		},
	}
	return job
}
