package upgradeplan

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"text/template"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"

	managementv1beta1 "github.com/harvester/upgrade-toolkit/api/v1beta1"
)

// RepoCreatePhase creates PVC, DaemonSet, and Service for the upgrade repo.
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

	pvc, err := p.getOrCreatePersistentVolumeClaimForRepo(ctx, upgradePlan)
	if err != nil {
		p.Log.Error(err, "unable to retrieve repo persistentvolumeclaim from upgradeplan")
		return ctrl.Result{}, err
	}

	bound := isPersistentVolumeClaimBound(pvc)

	if !bound {
		p.Log.V(1).Info("upgrade-repo persistentvolumeclaim not bound")
		updateProgressingPhase(upgradePlan, managementv1beta1.UpgradePlanPhaseRepoCreating, "upgrade-repo pvc not bound")
		return ctrl.Result{}, nil
	}

	updateProgressingPhase(upgradePlan, managementv1beta1.UpgradePlanPhaseRepoCreating, "upgrade-repo pvc bound")

	repo, err := p.getOrCreateDaemonSetForRepo(ctx, upgradePlan, pvc)
	if err != nil {
		p.Log.Error(err, "unable to retrieve repo daemonset from upgradeplan")
		return ctrl.Result{}, err
	}

	ready := isDaemonSetReady(repo)

	if !ready {
		p.Log.V(1).Info("upgrade-repo daemonset not ready")
		updateProgressingPhase(
			upgradePlan,
			managementv1beta1.UpgradePlanPhaseRepoCreating,
			"upgrade-repo daemonset not ready",
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

func (p *RepoCreatePhase) getOrCreatePersistentVolumeClaimForRepo(
	ctx context.Context,
	up *managementv1beta1.UpgradePlan,
) (*corev1.PersistentVolumeClaim, error) {
	nn := types.NamespacedName{
		Namespace: harvesterSystemNamespace,
		Name:      fmt.Sprintf("%s-%s", up.Name, repoComponent),
	}
	return getOrCreate(
		ctx, p.Client, p.Scheme, nn,
		func() *corev1.PersistentVolumeClaim { return &corev1.PersistentVolumeClaim{} },
		func() *corev1.PersistentVolumeClaim { return constructPersistentVolumeClaim(up) },
		up,
	)
}

func (p *RepoCreatePhase) getOrCreateDaemonSetForRepo(
	ctx context.Context,
	up *managementv1beta1.UpgradePlan,
	pvc *corev1.PersistentVolumeClaim,
) (*appsv1.DaemonSet, error) {
	nn := types.NamespacedName{
		Namespace: harvesterSystemNamespace,
		Name:      fmt.Sprintf("%s-%s", up.Name, repoComponent),
	}
	ca, err := p.getCA(ctx)
	if err != nil {
		return nil, err
	}
	return getOrCreate(
		ctx, p.Client, p.Scheme, nn,
		func() *appsv1.DaemonSet { return &appsv1.DaemonSet{} },
		func() *appsv1.DaemonSet { return constructDaemonSet(up, pvc, ca) },
		up,
	)
}

func (p *RepoCreatePhase) getOrCreateServiceForRepo(
	ctx context.Context,
	up *managementv1beta1.UpgradePlan,
) (*corev1.Service, error) {
	nn := types.NamespacedName{
		Namespace: harvesterSystemNamespace,
		Name:      fmt.Sprintf("%s-%s", up.Name, repoComponent),
	}
	return getOrCreate(
		ctx, p.Client, p.Scheme, nn,
		func() *corev1.Service { return &corev1.Service{} },
		func() *corev1.Service { return constructService(up) },
		up,
	)
}

func (p *RepoCreatePhase) getCA(ctx context.Context) (string, error) {
	var caSecret corev1.Secret
	if err := p.Client.Get(
		ctx,
		types.NamespacedName{Namespace: kubeSystemNamespace, Name: caName},
		&caSecret,
	); err != nil {
		return "", err
	}

	caPem, ok := caSecret.Data[corev1.TLSCertKey]
	if !ok {
		return "nil", fmt.Errorf("tls.crt not found")
	}

	return string(caPem), nil
}

func constructPersistentVolumeClaim(upgradePlan *managementv1beta1.UpgradePlan) *corev1.PersistentVolumeClaim {
	pvcName := fmt.Sprintf("%s-%s", upgradePlan.Name, repoComponent)
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				HarvesterUpgradePlanLabel:      upgradePlan.Name,
				HarvesterUpgradeComponentLabel: repoComponent,
			},
			Name:      pvcName,
			Namespace: harvesterSystemNamespace,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{
				corev1.ReadWriteMany,
			},
			VolumeMode: ptr.To(corev1.PersistentVolumeFilesystem),
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: *resource.NewQuantity(10*1024*1024*1024, resource.BinarySI),
				},
			},
			StorageClassName: ptr.To(longhornStaticStorageClassName),
		},
	}
	return pvc
}

func constructDaemonSet(
	upgradePlan *managementv1beta1.UpgradePlan,
	persistentVolumeClaim *corev1.PersistentVolumeClaim,
	cert string,
) *appsv1.DaemonSet {
	dsName := fmt.Sprintf("%s-%s", upgradePlan.Name, repoComponent)
	vmImageNamespace, vmImageName, _ := strings.Cut(
		ptr.Deref(upgradePlan.Status.ISOImageID, "nonexistent/nonexistent"),
		"/",
	)

	var (
		t   *template.Template
		buf bytes.Buffer
	)
	t = template.Must(template.New("script").Parse(isoDownloaderScriptTemplate))
	_ = t.Execute(&buf, nil)
	renderedISODownloadScript := buf.String()

	ds := &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				HarvesterUpgradePlanLabel:      upgradePlan.Name,
				HarvesterUpgradeComponentLabel: repoComponent,
			},
			Name:      dsName,
			Namespace: harvesterSystemNamespace,
		},
		Spec: appsv1.DaemonSetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					HarvesterUpgradePlanLabel:      upgradePlan.Name,
					HarvesterUpgradeComponentLabel: repoComponent,
				},
			},
			UpdateStrategy: appsv1.DaemonSetUpdateStrategy{
				Type: appsv1.RollingUpdateDaemonSetStrategyType,
				RollingUpdate: &appsv1.RollingUpdateDaemonSet{
					MaxUnavailable: &intstr.IntOrString{
						IntVal: 1,
					},
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
					Tolerations: []corev1.Toleration{
						{
							Key:      "node-role.kubernetes.io/control-plane",
							Operator: corev1.TolerationOpExists,
							Effect:   corev1.TaintEffectNoSchedule,
						},
						{
							Key:      "node-role.kubernetes.io/master",
							Operator: corev1.TolerationOpExists,
							Effect:   corev1.TaintEffectNoSchedule,
						},
					},
					InitContainers: []corev1.Container{
						{
							Name:  "iso-downloader",
							Image: fmt.Sprintf("%s:%s", upgradeToolkitImage, getPreviousVersion(upgradePlan)),
							Command: []string{
								"sh",
								"-c",
								renderedISODownloadScript,
							},
							Env: []corev1.EnvVar{
								{
									Name: "POD_NAME",
									ValueFrom: &corev1.EnvVarSource{
										FieldRef: &corev1.ObjectFieldSelector{
											FieldPath: "metadata.name",
										},
									},
								},
								{
									Name:  "VM_IMAGE_NS",
									Value: vmImageNamespace,
								},
								{
									Name:  "VM_IMAGE_NAME",
									Value: vmImageName,
								},
								{
									Name:  "CA_CERT",
									Value: cert,
								},
							},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "iso",
									MountPath: "/iso",
								},
							},
						},
						{
							Name:  "preloader",
							Image: fmt.Sprintf("%s:%s", upgradeToolkitImage, getPreviousVersion(upgradePlan)),
							Command: []string{
								"sh",
								"-c",
								preloaderScript,
							},
							SecurityContext: &corev1.SecurityContext{
								Privileged: ptr.To(true),
							},
							Env: []corev1.EnvVar{
								{
									Name:  "HOST_DIR",
									Value: "/host",
								},
							},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "host-root",
									MountPath: "/host",
								},
								{
									Name:      "iso",
									MountPath: "/iso",
								},
							},
						},
					},
					Containers: []corev1.Container{
						{
							Name:  "iso-mounter",
							Image: fmt.Sprintf("%s:%s", upgradeToolkitImage, getUpgradeVersion(upgradePlan)),
							Command: []string{
								"sh",
								"-c",
								isoMounterScript,
							},
							SecurityContext: &corev1.SecurityContext{
								Privileged: ptr.To(true),
							},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "iso",
									MountPath: "/iso",
								},
								{
									Name:             "share-mount",
									MountPath:        "/share-mount",
									MountPropagation: ptr.To(corev1.MountPropagationBidirectional),
								},
							},
						},
						{
							Name:  "repo",
							Image: fmt.Sprintf("%s:%s", upgradeToolkitImage, getUpgradeVersion(upgradePlan)),
							Command: []string{
								"sh",
								"-c",
								repoScript,
							},
							Ports: []corev1.ContainerPort{
								{
									ContainerPort: 80,
								},
							},
							LivenessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									Exec: &corev1.ExecAction{
										Command: []string{
											"sh",
											"-c",
											"cat /srv/www/htdocs/harvester-release.yaml 2>&1 /dev/null",
										},
									},
								},
								PeriodSeconds:    10,
								TimeoutSeconds:   5,
								FailureThreshold: 3,
							},
							ReadinessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									HTTPGet: &corev1.HTTPGetAction{
										Path: "/harvester-release.yaml",
										Port: intstr.FromInt(80),
									},
								},
								InitialDelaySeconds: 5,
								PeriodSeconds:       10,
								TimeoutSeconds:      5,
								SuccessThreshold:    1,
								FailureThreshold:    1,
							},
							SecurityContext: &corev1.SecurityContext{
								Privileged: ptr.To(true),
							},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:             "share-mount",
									MountPath:        "/srv/www/htdocs",
									MountPropagation: ptr.To(corev1.MountPropagationBidirectional),
								},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "host-root",
							VolumeSource: corev1.VolumeSource{
								HostPath: &corev1.HostPathVolumeSource{
									Path: "/",
								},
							},
						},
						{
							Name: "iso",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: persistentVolumeClaim.Name,
								},
							},
						},
						{
							Name: "share-mount",
							VolumeSource: corev1.VolumeSource{
								EmptyDir: &corev1.EmptyDirVolumeSource{},
							},
						},
					},
				},
			},
		},
	}
	return ds
}

func constructService(upgradePlan *managementv1beta1.UpgradePlan) *corev1.Service {
	svcName := fmt.Sprintf("%s-%s", upgradePlan.Name, repoComponent)
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
