package upgradeplan

import (
	"context"
	"fmt"

	"github.com/rancher/wrangler/v3/pkg/name"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"

	managementv1beta1 "github.com/harvester/upgrade-toolkit/api/v1beta1"
)

const (
	repoScript = `
#!/usr/bin/env sh
set -e

echo "Mounting ISO and starting Nginx..."
mkdir -p /srv/www/htdocs/harvester-iso
mount -o loop,ro /iso/disk.img /srv/www/htdocs/harvester-iso
echo "iso mounted successfully to /srv/www/htdocs/harvester-iso"
nginx -g "daemon off;"
`
)

// RepoCreatePhase creates a Deployment and Service for the upgrade repo.
type RepoCreatePhase struct {
	*PhaseDeps
}

func NewRepoCreatePhase(deps *PhaseDeps) *RepoCreatePhase {
	return &RepoCreatePhase{PhaseDeps: deps}
}

func (p *RepoCreatePhase) Name() string { return "RepoCreate" }

func (p *RepoCreatePhase) Run(
	ctx context.Context,
	upgradePlan *managementv1beta1.UpgradePlan,
) (ctrl.Result, error) {
	p.Log.V(1).Info("handle repo create")

	replicas, err := p.getDeploymentReplicaCount(ctx)
	if err != nil {
		p.Log.Error(err, "unable to determine deployment replica count")
		return ctrl.Result{}, err
	}

	deploy, err := p.getOrCreateDeploymentForRepo(ctx, upgradePlan, replicas)
	if err != nil {
		p.Log.Error(err, "unable to retrieve repo deployment from upgradeplan")
		return ctrl.Result{}, err
	}

	ready := isDeploymentReady(deploy)

	if !ready {
		p.Log.V(1).Info("upgrade-repo deployment not ready")
		updateProgressingPhase(
			upgradePlan,
			managementv1beta1.UpgradePlanPhaseRepoCreating,
			"upgrade-repo deployment not ready",
		)
		return ctrl.Result{}, nil
	}

	svc, err := p.getOrCreateServiceForRepo(ctx, upgradePlan)
	if err != nil {
		p.Log.Error(err, "unable to retrieve repo service from upgradeplan")
		return ctrl.Result{}, err
	}

	ready = isServiceReady(ctx, p.Client, svc)

	if !ready {
		p.Log.V(1).Info("upgrade-repo service/endpoints not ready")
		updateProgressingPhase(upgradePlan, managementv1beta1.UpgradePlanPhaseRepoCreating, "upgrade-repo svc/ep not ready")
		return ctrl.Result{}, nil
	}

	updateProgressingPhase(upgradePlan, managementv1beta1.UpgradePlanPhaseRepoCreated, "")
	return ctrl.Result{}, nil
}

func (p *RepoCreatePhase) getOrCreateDeploymentForRepo(
	ctx context.Context,
	up *managementv1beta1.UpgradePlan,
	replicas *int32,
) (*appsv1.Deployment, error) {
	nn := types.NamespacedName{
		Namespace: harvesterSystemNamespace,
		Name:      name.SafeConcatName(up.Name, repoComponent),
	}
	return GetOrCreate(
		ctx, p.Client, p.Scheme, nn,
		func() *appsv1.Deployment { return &appsv1.Deployment{} },
		func() *appsv1.Deployment { return constructDeployment(up, replicas) },
		up,
	)
}

func (p *RepoCreatePhase) getOrCreateServiceForRepo(
	ctx context.Context,
	up *managementv1beta1.UpgradePlan,
) (*corev1.Service, error) {
	nn := types.NamespacedName{
		Namespace: harvesterSystemNamespace,
		Name:      name.SafeConcatName(up.Name, repoComponent),
	}
	return GetOrCreate(
		ctx, p.Client, p.Scheme, nn,
		func() *corev1.Service { return &corev1.Service{} },
		func() *corev1.Service { return constructService(up) },
		up,
	)
}

func (p *RepoCreatePhase) getDeploymentReplicaCount(ctx context.Context) (*int32, error) {
	nodes, err := listManagedNodes(ctx, p.Client)
	if err != nil {
		return nil, fmt.Errorf("failed to list nodes: %w", err)
	}

	nonWitnessCount := 0
	for i := range nodes {
		if nodes[i].Labels[witnessNodeRoleLabel] != "true" {
			nonWitnessCount++
		}
	}

	if nonWitnessCount == 0 {
		return nil, fmt.Errorf("no non-witness managed nodes found in the cluster")
	}

	var replicas int32
	if nonWitnessCount == 1 {
		replicas = 1
	} else {
		replicas = 2
	}

	return &replicas, nil
}

func constructDeployment(
	upgradePlan *managementv1beta1.UpgradePlan,
	replicas *int32,
) *appsv1.Deployment {
	deployName := name.SafeConcatName(upgradePlan.Name, repoComponent)
	pvcName := name.SafeConcatName(upgradePlan.Name, imageComponent)

	deploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				HarvesterUpgradePlanLabel:      upgradePlan.Name,
				HarvesterUpgradeComponentLabel: repoComponent,
			},
			Name:      deployName,
			Namespace: harvesterSystemNamespace,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					HarvesterUpgradePlanLabel:      upgradePlan.Name,
					HarvesterUpgradeComponentLabel: repoComponent,
				},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						HarvesterUpgradePlanLabel:      upgradePlan.Name,
						HarvesterUpgradeComponentLabel: repoComponent,
					},
				},
				Spec: corev1.PodSpec{
					Affinity: &corev1.Affinity{
						PodAntiAffinity: &corev1.PodAntiAffinity{
							PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{
								{
									Weight: 100,
									PodAffinityTerm: corev1.PodAffinityTerm{
										LabelSelector: &metav1.LabelSelector{
											MatchLabels: map[string]string{
												HarvesterUpgradePlanLabel:      upgradePlan.Name,
												HarvesterUpgradeComponentLabel: repoComponent,
											},
										},
										TopologyKey: corev1.LabelHostname,
									},
								},
							},
						},
					},
					Containers: []corev1.Container{
						{
							Name:            "nginx-iso-server",
							Image:           fmt.Sprintf("%s:%s", upgradeToolkitImage, getUpgradeVersion(upgradePlan)),
							ImagePullPolicy: corev1.PullIfNotPresent,
							Command:         []string{"sh", "-c", repoScript},
							Ports: []corev1.ContainerPort{
								{
									ContainerPort: 80,
									Protocol:      corev1.ProtocolTCP,
								},
							},
							SecurityContext: &corev1.SecurityContext{
								Privileged: ptr.To(true),
							},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "iso",
									MountPath: "/iso",
								},
							},
							LivenessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									Exec: &corev1.ExecAction{
										Command: []string{
											"sh", "-c",
											"test -f /srv/www/htdocs/harvester-iso/harvester-release.yaml",
										},
									},
								},
							},
							ReadinessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									HTTPGet: &corev1.HTTPGetAction{
										Path:   "/harvester-iso/harvester-release.yaml",
										Port:   intstr.FromInt(80),
										Scheme: corev1.URISchemeHTTP,
									},
								},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "iso",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: pvcName,
								},
							},
						},
					},
				},
			},
		},
	}
	return deploy
}

func constructService(upgradePlan *managementv1beta1.UpgradePlan) *corev1.Service {
	svcName := name.SafeConcatName(upgradePlan.Name, repoComponent)
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				HarvesterUpgradePlanLabel:      upgradePlan.Name,
				HarvesterUpgradeComponentLabel: repoComponent,
			},
			Name:      svcName,
			Namespace: harvesterSystemNamespace,
		},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{
				{
					Port:       80,
					Protocol:   corev1.ProtocolTCP,
					TargetPort: intstr.FromInt(80),
				},
			},
			Selector: map[string]string{
				HarvesterUpgradePlanLabel:      upgradePlan.Name,
				HarvesterUpgradeComponentLabel: repoComponent,
			},
		},
	}
	return svc
}
