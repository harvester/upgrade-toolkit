package upgradeplan

import (
	"context"
	"fmt"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"

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

func (p *ClusterUpgradePhase) Run(
	ctx context.Context,
	upgradePlan *managementv1beta1.UpgradePlan,
) (ctrl.Result, error) {
	p.Log.V(1).Info("handle cluster upgrade")

	job, err := p.getOrCreateJobForClusterUpgrade(ctx, upgradePlan)
	if err != nil {
		p.Log.Error(err, "unable to retrieve cluster-upgrade job from upgradeplan")
		return ctrl.Result{}, err
	}

	finished, success := isJobFinished(job)

	if !finished {
		p.Log.V(1).Info("cluster-upgrade job running")
		updateProgressingPhase(upgradePlan, managementv1beta1.UpgradePlanPhaseClusterUpgrading, "")
		return ctrl.Result{}, nil
	}

	if !success {
		p.Log.V(0).Info("cluster-upgrade job failed")
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
		Name:      fmt.Sprintf("%s-%s", up.Name, ClusterComponent),
	}
	return getOrCreate(
		ctx, p.Client, p.Scheme, nn,
		func() *batchv1.Job { return &batchv1.Job{} },
		func() *batchv1.Job { return constructJobForClusterUpgrade(up) },
		up,
	)
}

func constructJobForClusterUpgrade(upgradePlan *managementv1beta1.UpgradePlan) *batchv1.Job {
	jobName := fmt.Sprintf("%s-%s", upgradePlan.Name, ClusterComponent)
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
							Image: fmt.Sprintf("%s:%s", upgradeToolkitImage, getUpgradeVersion(upgradePlan)),
							Command: []string{
								"upgrade_manifests.sh",
							},
							Env: []corev1.EnvVar{
								{
									Name:  "HARVESTER_UPGRADEPLAN_NAME",
									Value: upgradePlan.Name,
								},
							},
						},
					},
					ServiceAccountName: harvesterServiceAccountName,
					Tolerations:        getDefaultTolerations(),
				},
			},
		},
	}
	return job
}
