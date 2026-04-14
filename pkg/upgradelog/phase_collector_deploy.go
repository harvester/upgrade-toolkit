package upgradelog

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	buildversion "github.com/harvester/upgrade-toolkit/pkg/version"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"

	managementv1beta1 "github.com/harvester/upgrade-toolkit/api/v1beta1"
)

const (
	collectorNamespace = "harvester-system"
)

// CollectorDeployPhase creates the PVC, Deployment, and Service for the log collector.
type CollectorDeployPhase struct {
	*PhaseDeps
}

func NewCollectorDeployPhase(deps *PhaseDeps) *CollectorDeployPhase {
	return &CollectorDeployPhase{PhaseDeps: deps}
}

func (p *CollectorDeployPhase) Name() string { return "CollectorDeploy" }

func (p *CollectorDeployPhase) Run(ctx context.Context, upgradeLog *managementv1beta1.UpgradeLog) (ctrl.Result, error) {
	log := logr.FromContextOrDiscard(ctx)

	collectorName := collectorDeploymentName(upgradeLog.Name)

	// Ensure PVC exists
	if err := p.ensurePVC(ctx, upgradeLog); err != nil {
		return ctrl.Result{}, fmt.Errorf("ensuring PVC: %w", err)
	}

	// Ensure Deployment exists
	if err := p.ensureDeployment(ctx, upgradeLog); err != nil {
		return ctrl.Result{}, fmt.Errorf("ensuring Deployment: %w", err)
	}

	// Ensure Service exists
	if err := p.ensureService(ctx, upgradeLog); err != nil {
		return ctrl.Result{}, fmt.Errorf("ensuring Service: %w", err)
	}

	// Check if deployment is ready
	var deploy appsv1.Deployment
	if err := p.Client.Get(ctx, types.NamespacedName{
		Name:      collectorName,
		Namespace: collectorNamespace,
	}, &deploy); err != nil {
		return ctrl.Result{}, fmt.Errorf("getting collector deployment: %w", err)
	}

	if deploy.Status.ReadyReplicas > 0 {
		log.V(1).Info("collector deployment is ready")
		upgradeLog.SetCondition(
			managementv1beta1.UpgradeLogCollectorReady,
			metav1.ConditionTrue,
			"CollectorReady",
			"Log collector deployment is ready",
		)
		upgradeLog.Status.CurrentPhase = managementv1beta1.UpgradeLogPhaseCollectorDeployed
		return ctrl.Result{}, nil
	}

	log.V(1).Info("waiting for collector deployment to become ready")
	return ctrl.Result{RequeueAfter: RequeueAfterDuration}, nil
}

func (p *CollectorDeployPhase) ensurePVC(ctx context.Context, upgradeLog *managementv1beta1.UpgradeLog) error {
	pvcName := collectorPVCName(upgradeLog.Name)
	var pvc corev1.PersistentVolumeClaim
	err := p.Client.Get(ctx, types.NamespacedName{
		Name:      pvcName,
		Namespace: collectorNamespace,
	}, &pvc)
	if err == nil {
		return nil // already exists
	}

	pvc = corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pvcName,
			Namespace: collectorNamespace,
			Labels: map[string]string{
				UpgradeLogLabel: upgradeLog.Name,
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse(CollectorPVCSize),
				},
			},
		},
	}

	return p.Client.Create(ctx, &pvc)
}

func (p *CollectorDeployPhase) ensureDeployment(ctx context.Context, upgradeLog *managementv1beta1.UpgradeLog) error {
	deployName := collectorDeploymentName(upgradeLog.Name)
	var deploy appsv1.Deployment
	err := p.Client.Get(ctx, types.NamespacedName{
		Name:      deployName,
		Namespace: collectorNamespace,
	}, &deploy)
	if err == nil {
		return nil // already exists
	}

	image := getCollectorImage(upgradeLog)
	pvcName := collectorPVCName(upgradeLog.Name)

	deploy = appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      deployName,
			Namespace: collectorNamespace,
			Labels: map[string]string{
				UpgradeLogLabel: upgradeLog.Name,
			},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: ptr.To(int32(CollectorReplicas)),
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					UpgradeLogLabel: upgradeLog.Name,
					"app":           CollectorComponent,
				},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						UpgradeLogLabel: upgradeLog.Name,
						"app":           CollectorComponent,
					},
				},
				Spec: corev1.PodSpec{
					SecurityContext: &corev1.PodSecurityContext{
						FSGroup: ptr.To(int64(1000)),
					},
					Containers: []corev1.Container{
						{
							Name:    CollectorComponent,
							Image:   image,
							Command: []string{"upgrade-toolkit"},
							Args: []string{
								"log-collector",
								fmt.Sprintf("--listen=:%d", CollectorPort),
								fmt.Sprintf("--log-dir=%s", CollectorLogDir),
								fmt.Sprintf("--upgrade-plan=%s", upgradeLog.Spec.UpgradePlanName),
							},
							Ports: []corev1.ContainerPort{
								{
									Name:          CollectorPortName,
									ContainerPort: int32(CollectorPort),
									Protocol:      corev1.ProtocolTCP,
								},
							},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "logs",
									MountPath: CollectorLogDir,
								},
							},
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("10m"),
									corev1.ResourceMemory: resource.MustParse("32Mi"),
								},
								Limits: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("200m"),
									corev1.ResourceMemory: resource.MustParse("128Mi"),
								},
							},
						},
						{
							Name:    LogViewerContainer,
							Image:   image,
							Command: []string{"upgrade-toolkit"},
							Args:    []string{"log-viewer", CollectorLogDir},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "logs",
									MountPath: CollectorLogDir,
									ReadOnly:  true,
								},
							},
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("5m"),
									corev1.ResourceMemory: resource.MustParse("8Mi"),
								},
								Limits: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("50m"),
									corev1.ResourceMemory: resource.MustParse("32Mi"),
								},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "logs",
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

	return p.Client.Create(ctx, &deploy)
}

func (p *CollectorDeployPhase) ensureService(ctx context.Context, upgradeLog *managementv1beta1.UpgradeLog) error {
	svcName := collectorServiceName(upgradeLog.Name)
	var svc corev1.Service
	err := p.Client.Get(ctx, types.NamespacedName{
		Name:      svcName,
		Namespace: collectorNamespace,
	}, &svc)
	if err == nil {
		return nil // already exists
	}

	svc = corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      svcName,
			Namespace: collectorNamespace,
			Labels: map[string]string{
				UpgradeLogLabel: upgradeLog.Name,
			},
		},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{
				UpgradeLogLabel: upgradeLog.Name,
				"app":           CollectorComponent,
			},
			Ports: []corev1.ServicePort{
				{
					Name:     CollectorPortName,
					Port:     int32(CollectorPort),
					Protocol: corev1.ProtocolTCP,
				},
			},
		},
	}

	return p.Client.Create(ctx, &svc)
}

// Naming helpers

func collectorDeploymentName(upgradeLogName string) string {
	return upgradeLogName + "-" + CollectorComponent
}

func collectorServiceName(upgradeLogName string) string {
	return upgradeLogName + "-" + CollectorComponent
}

func collectorPVCName(upgradeLogName string) string {
	return upgradeLogName + "-logs"
}

// CollectorServiceEndpoint returns the in-cluster DNS endpoint for the collector service.
func CollectorServiceEndpoint(upgradeLogName string) string {
	return fmt.Sprintf("%s.%s.svc:%d",
		collectorServiceName(upgradeLogName),
		collectorNamespace,
		CollectorPort,
	)
}

func getCollectorImage(upgradeLog *managementv1beta1.UpgradeLog) string {
	repo := CollectorImage
	if upgradeLog != nil {
		if image, ok := upgradeLog.Annotations[AnnotationUpgradeToolkitImage]; ok && image != "" {
			repo = image
		}
	}

	return fmt.Sprintf("%s:%s", repo, buildversion.Version)
}
