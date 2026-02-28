package upgradeplan

import (
	"context"
	"fmt"
	"strings"

	harvesterv1beta1 "github.com/harvester/harvester/pkg/apis/harvesterhci.io/v1beta1"
	upgradev1 "github.com/rancher/system-upgrade-controller/pkg/apis/upgrade.cattle.io/v1"
	"github.com/rancher/wrangler/v3/pkg/name"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	managementv1beta1 "github.com/harvester/upgrade-toolkit/api/v1beta1"
)

const (
	harvesterSystemNamespace       = "harvester-system"
	cattleSystemNamespace          = "cattle-system"
	kubeSystemNamespace            = "kube-system"
	harvesterServiceAccountName    = "harvester"
	serverVersionSettingName       = "server-version"
	sucName                        = "system-upgrade-controller"
	longhornStaticStorageClassName = "longhorn-static"

	harvesterManagedLabel = "harvesterhci.io/managed"
	witnessNodeRoleLabel  = "node-role.harvesterhci.io/witness"
	serviceNameLabel      = "kubernetes.io/service-name"
	imageComponent        = "iso"
	repoComponent         = "repo"

	defaultTTLSecondsAfterFinished = 604800 // 7 days

	upgradeToolkitImage = "starbops/harvester-upgrade-toolkit"
)

// Version helpers

func getUpgradeVersion(upgradePlan *managementv1beta1.UpgradePlan) string {
	if upgradePlan == nil {
		return "nonexistent"
	}

	if upgradePlan.Spec.Upgrade != nil {
		return *upgradePlan.Spec.Upgrade
	}

	return upgradePlan.Spec.Version
}

func sanitizedVersion(version string) string {
	return strings.Replace(version, "+", "-", 1)
}

func getKubernetesVersion(upgradePlan *managementv1beta1.UpgradePlan) string {
	if upgradePlan != nil && upgradePlan.Status.ReleaseMetadata != nil {
		return sanitizedVersion(upgradePlan.Status.ReleaseMetadata.Kubernetes)
	}
	return ""
}

// Toleration helpers

func getDefaultTolerations() []corev1.Toleration {
	return []corev1.Toleration{
		{
			Key:      corev1.TaintNodeUnschedulable,
			Operator: corev1.TolerationOpExists,
			Effect:   corev1.TaintEffectNoSchedule,
		},
		{
			Key:      corev1.TaintNodeUnreachable,
			Operator: corev1.TolerationOpExists,
			Effect:   corev1.TaintEffectNoExecute,
		},
		{
			Key:      "node-role.kubernetes.io/control-plane",
			Operator: corev1.TolerationOpExists,
			Effect:   corev1.TaintEffectNoExecute,
		},
		{
			Key:      "node-role.kubernetes.io/etcd",
			Operator: corev1.TolerationOpExists,
			Effect:   corev1.TaintEffectNoExecute,
		},
		{
			Key:      "kubernetes.io/arch",
			Operator: corev1.TolerationOpEqual,
			Effect:   corev1.TaintEffectNoSchedule,
			Value:    "amd64",
		},
		{
			Key:      "kubernetes.io/arch",
			Operator: corev1.TolerationOpEqual,
			Effect:   corev1.TaintEffectNoSchedule,
			Value:    "arm64",
		},
		{
			Key:      "kubernetes.io/arch",
			Operator: corev1.TolerationOpEqual,
			Effect:   corev1.TaintEffectNoSchedule,
			Value:    "arm",
		},
		{
			Key:      "kubevirt.io/drain",
			Operator: corev1.TolerationOpExists,
			Effect:   corev1.TaintEffectNoSchedule,
		},
		{
			Key:      "CriticalAddonsOnly",
			Operator: corev1.TolerationOpExists,
		},
	}
}

// Phase and status helpers

func updateProgressingPhase(
	upgradePlan *managementv1beta1.UpgradePlan,
	phase managementv1beta1.UpgradePlanPhase,
	message string,
) {
	upgradePlan.Status.CurrentPhase = phase

	if !upgradePlan.ConditionExists(managementv1beta1.UpgradePlanProgressing) {
		upgradePlan.SetCondition(managementv1beta1.UpgradePlanProgressing, metav1.ConditionTrue, string(phase), "")
	} else {
		cond := upgradePlan.LookupCondition(managementv1beta1.UpgradePlanProgressing)
		upgradePlan.SetCondition(managementv1beta1.UpgradePlanProgressing, cond.Status, string(phase), message)
	}
}

func isTerminalPhase(phase managementv1beta1.UpgradePlanPhase) bool {
	return phase == managementv1beta1.UpgradePlanPhaseSucceeded ||
		phase == managementv1beta1.UpgradePlanPhaseFailed
}

func markUpgradePlanComplete(upgradePlan *managementv1beta1.UpgradePlan) {
	if isTerminalPhase(upgradePlan.Status.CurrentPhase) {
		upgradePlan.SetCondition(
			managementv1beta1.UpgradePlanAvailable,
			metav1.ConditionFalse,
			"Executed",
			"Entered one of the terminal phases",
		)
		upgradePlan.SetCondition(
			managementv1beta1.UpgradePlanProgressing,
			metav1.ConditionFalse,
			string(upgradePlan.Status.CurrentPhase),
			"UpgradePlan has completed",
		)
	}
}

// Resource readiness checks

func isVirtualMachineImageImported(vmImage *harvesterv1beta1.VirtualMachineImage) (finished, success bool) {
	for _, condition := range vmImage.Status.Conditions {
		if condition.Type == harvesterv1beta1.ImageImported && condition.Status == corev1.ConditionTrue {
			finished, success = true, true
			return
		}
		if condition.Type == harvesterv1beta1.ImageImported && condition.Status == corev1.ConditionFalse {
			finished, success = true, false
			return
		}
	}
	return
}

func isDeploymentReady(deploy *appsv1.Deployment) bool {
	if deploy.Spec.Replicas == nil {
		return false
	}
	return deploy.Status.AvailableReplicas >= *deploy.Spec.Replicas
}

func isServiceReady(ctx context.Context, c client.Client, svc *corev1.Service) bool {
	if svc.Spec.ClusterIP == "" {
		return false
	}
	return isAnyEndpointReady(ctx, c, svc)
}

func isAnyEndpointReady(ctx context.Context, c client.Client, svc *corev1.Service) bool {
	var epsList discoveryv1.EndpointSliceList
	if err := c.List(ctx, &epsList, &client.ListOptions{
		LabelSelector: labels.SelectorFromSet(labels.Set{
			serviceNameLabel: svc.Name,
		}),
	}); err != nil {
		return false
	}

	if len(epsList.Items) == 0 {
		return false
	}

	for _, ep := range epsList.Items[0].Endpoints {
		if ep.Conditions.Ready != nil && *ep.Conditions.Ready {
			return true
		}
	}

	return false
}

func isJobFinished(job *batchv1.Job) (finished, success bool) {
	for _, condition := range job.Status.Conditions {
		if condition.Type == batchv1.JobComplete && condition.Status == corev1.ConditionTrue {
			finished, success = true, true
			return
		}
		if condition.Type == batchv1.JobFailed && condition.Status == corev1.ConditionTrue {
			finished, success = true, false
			return
		}
	}
	return
}

func isPlanFinished(plan *upgradev1.Plan) bool {
	for _, condition := range plan.Status.Conditions {
		if condition.Type == string(upgradev1.PlanComplete) && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func isAnyPlanJobFailed(plan *upgradev1.Plan) bool {
	for _, condition := range plan.Status.Conditions {
		if condition.Type == string(upgradev1.PlanComplete) &&
			condition.Status == corev1.ConditionFalse &&
			condition.Reason == "JobFailed" {
			return true
		}
	}
	return false
}

func isTerminalState(status managementv1beta1.NodeUpgradeStatus) bool {
	return status.State == managementv1beta1.NodeStatePostDrained
}

func IsNodeUpgradeFailure(status managementv1beta1.NodeUpgradeStatus) bool {
	switch status.State {
	case managementv1beta1.NodeStatePreDrainFailed,
		managementv1beta1.NodeStatePostDrainFailed:
		return true
	}
	return false
}

// ConstructDrainJob builds a Job for pre-drain or post-drain hooks.
func ConstructDrainJob(
	upgradePlan *managementv1beta1.UpgradePlan,
	nodeName, jobName, hookType string,
) *batchv1.Job {
	args := "pre-drain"
	containerName := "pre-drain"
	if hookType == DrainHookTypePostDrain {
		args = "post-drain"
		containerName = "post-drain"
	}

	envVars := []corev1.EnvVar{
		{
			Name:  "HARVESTER_UPGRADEPLAN_NAME",
			Value: upgradePlan.Name,
		},
		{
			Name:  "HARVESTER_UPGRADE_NODE_NAME",
			Value: nodeName,
		},
	}

	if hookType == DrainHookTypePostDrain {
		envVars = append(envVars, corev1.EnvVar{
			Name: "HARVESTER_UPGRADE_POD_NAME",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{
					FieldPath: "metadata.name",
				},
			},
		})
	}

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: harvesterSystemNamespace,
			Labels: map[string]string{
				HarvesterUpgradePlanLabel:      upgradePlan.Name,
				HarvesterUpgradeComponentLabel: NodeComponent,
				HarvesterDrainHookTypeLabel:    hookType,
				HarvesterUpgradeNodeLabel:      nodeName,
			},
		},
		Spec: batchv1.JobSpec{
			TTLSecondsAfterFinished: ptr.To[int32](defaultTTLSecondsAfterFinished),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						HarvesterUpgradePlanLabel:      upgradePlan.Name,
						HarvesterUpgradeComponentLabel: NodeComponent,
						HarvesterDrainHookTypeLabel:    hookType,
						HarvesterUpgradeNodeLabel:      nodeName,
					},
				},
				Spec: corev1.PodSpec{
					RestartPolicy:      corev1.RestartPolicyNever,
					NodeName:           nodeName,
					ServiceAccountName: harvesterServiceAccountName,
					HostPID:            true,
					Tolerations:        getDefaultTolerations(),
					Containers: []corev1.Container{
						{
							Name:    containerName,
							Image:   fmt.Sprintf("%s:%s", upgradeToolkitImage, getUpgradeVersion(upgradePlan)),
							Command: []string{"upgrade_node.sh"},
							Args:    []string{args},
							Env:     envVars,
							SecurityContext: &corev1.SecurityContext{
								Privileged: ptr.To(true),
							},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "host-root",
									MountPath: "/host",
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
					},
				},
			},
		},
	}
}

// Generic resource helpers

func createOwnedAndFetch[T client.Object](
	ctx context.Context,
	c client.Client,
	scheme *runtime.Scheme,
	owner client.Object,
	obj T,
) (T, error) {
	if err := controllerutil.SetControllerReference(owner, obj, scheme); err != nil {
		var zero T
		return zero, err
	}
	if err := c.Create(ctx, obj, &client.CreateOptions{}); err != nil {
		var zero T
		return zero, err
	}
	nn := types.NamespacedName{Namespace: obj.GetNamespace(), Name: obj.GetName()}
	if err := c.Get(ctx, nn, obj); err != nil {
		var zero T
		return zero, err
	}
	return obj, nil
}

func GetOrCreate[T client.Object](
	ctx context.Context,
	c client.Client,
	scheme *runtime.Scheme,
	nn types.NamespacedName,
	newObj func() T,
	build func() T,
	owner client.Object,
) (T, error) {
	obj := newObj()
	if err := c.Get(ctx, nn, obj); err != nil {
		if apierrors.IsNotFound(err) {
			return createOwnedAndFetch(ctx, c, scheme, owner, build())
		}
		var zero T
		return zero, err
	}
	return obj, nil
}

// constructPlan builds a generic system-upgrade-controller Plan resource.
func constructPlan(
	upgradePlanName, componentName string,
	concurrency int,
	nodeSelector *metav1.LabelSelector,
	maintenance bool,
	prepare *upgradev1.ContainerSpec,
	container *upgradev1.ContainerSpec,
	version string,
) *upgradev1.Plan {
	planName := name.SafeConcatName(upgradePlanName, componentName)

	plan := &upgradev1.Plan{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				HarvesterUpgradePlanLabel:      upgradePlanName,
				HarvesterUpgradeComponentLabel: componentName,
			},
			Name:      planName,
			Namespace: cattleSystemNamespace,
		},
		Spec: upgradev1.PlanSpec{
			Concurrency:           int64(concurrency),
			JobActiveDeadlineSecs: ptr.To[int64](0),
			NodeSelector:          nodeSelector,
			ServiceAccountName:    sucName,
			Tolerations:           getDefaultTolerations(),
			Prepare:               prepare,
			Upgrade:               container,
			Version:               version,
		},
	}

	if maintenance {
		plan.Spec.Cordon = true
		plan.Spec.Drain = &upgradev1.DrainSpec{
			Force: true,
		}
	}

	return plan
}
