package upgradeplan

import (
	"context"
	"fmt"
	"strings"

	harvesterv1beta1 "github.com/harvester/harvester/pkg/apis/harvesterhci.io/v1beta1"
	buildversion "github.com/harvester/upgrade-toolkit/pkg/version"
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
	eventNamespace                 = "default"
	harvesterSystemNamespace       = "harvester-system"
	cattleSystemNamespace          = "cattle-system"
	kubeSystemNamespace            = "kube-system"
	serverVersionSettingName       = "server-version"
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

	if upgradePlan.Status.ReleaseMetadata != nil && upgradePlan.Status.ReleaseMetadata.Harvester != "" {
		return upgradePlan.Status.ReleaseMetadata.Harvester
	}

	return buildversion.Version
}

// pvcNameFromISOImageID returns the PVC name for a given ISOImageID.
// The PVC backing a VirtualMachineImage shares the VMImage's name
// and is always created in the harvester-system namespace.
func pvcNameFromISOImageID(isoImageID string) string {
	return isoImageID
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
		upgradePlan.SetCondition(managementv1beta1.UpgradePlanProgressing, metav1.ConditionTrue, string(phase), message)
	} else {
		cond := upgradePlan.LookupCondition(managementv1beta1.UpgradePlanProgressing)
		upgradePlan.SetCondition(managementv1beta1.UpgradePlanProgressing, cond.Status, string(phase), message)
	}
}

func isTerminalPhase(phase managementv1beta1.UpgradePlanPhase) bool {
	return phase == managementv1beta1.UpgradePlanPhaseSucceeded ||
		phase == managementv1beta1.UpgradePlanPhaseFailed
}

// FindConflictingUpgrade returns the name of the first UpgradePlan (other than
// currentName) that has Progressing=True, or "" if none is found.
func FindConflictingUpgrade(ctx context.Context, c client.Reader, currentName string) (string, error) {
	var upgradePlanList managementv1beta1.UpgradePlanList
	if err := c.List(ctx, &upgradePlanList); err != nil {
		return "", fmt.Errorf("failed to list UpgradePlans: %w", err)
	}
	for _, existing := range upgradePlanList.Items {
		if existing.Name == currentName {
			continue
		}
		if existing.ConditionTrue(managementv1beta1.UpgradePlanProgressing) {
			return existing.Name, nil
		}
	}
	return "", nil
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
	if err := c.List(ctx, &epsList,
		client.InNamespace(svc.Namespace),
		client.MatchingLabels{serviceNameLabel: svc.Name},
	); err != nil {
		return false
	}

	for _, eps := range epsList.Items {
		for _, ep := range eps.Endpoints {
			if ep.Conditions.Ready != nil && *ep.Conditions.Ready {
				return true
			}
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
	return status.State == managementv1beta1.NodeStatePostDrained ||
		status.State == managementv1beta1.NodeStateSingleNodeUpgraded
}

// ShouldPauseNode returns true if the given node should be paused based on
// the UpgradePlan's NodeUpgradeOption.PauseNodes list.
func ShouldPauseNode(up *managementv1beta1.UpgradePlan, nodeName string) bool {
	opt := up.Spec.NodeUpgradeOption
	if opt == nil {
		return false
	}
	for _, n := range opt.PauseNodes {
		if n == nodeName {
			return true
		}
	}
	return false
}

// ReconcileJobSuspend patches the Job's spec.suspend field if it differs from the desired state.
func ReconcileJobSuspend(ctx context.Context, c client.Client, job *batchv1.Job, suspend bool) error {
	currentlySuspended := job.Spec.Suspend != nil && *job.Spec.Suspend
	if currentlySuspended == suspend {
		return nil
	}
	patch := client.MergeFrom(job.DeepCopy())
	job.Spec.Suspend = ptr.To(suspend)
	return c.Patch(ctx, job, patch)
}

func IsNodeUpgradeFailure(status managementv1beta1.NodeUpgradeStatus) bool {
	switch status.State {
	case managementv1beta1.NodeStatePreDrainFailed,
		managementv1beta1.NodeStatePostDrainFailed,
		managementv1beta1.NodeStateSingleNodeUpgradeFailed:
		return true
	}
	return false
}

func IsNodeUpgradePaused(status managementv1beta1.NodeUpgradeStatus) bool {
	return status.State == managementv1beta1.NodeStateUpgradePaused
}

// ConstructNodeJob builds a Job for node-upgrade operations (pre-drain, post-drain,
// or single-node-upgrade). It runs upgrade_node.sh with jobType as the argument.
func ConstructNodeJob(
	upgradePlan *managementv1beta1.UpgradePlan,
	nodeName, jobName, jobType, serviceAccountName string,
	suspend bool,
) *batchv1.Job {
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

	if jobType == JobTypePostDrain || jobType == JobTypeSingleNodeUpgrade {
		envVars = append(envVars,
			corev1.EnvVar{
				Name: "HARVESTER_UPGRADE_POD_NAME",
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{
						FieldPath: "metadata.name",
					},
				},
			},
		)
	}

	var suspendPtr *bool
	if suspend {
		suspendPtr = ptr.To(true)
	}

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: harvesterSystemNamespace,
			Labels: map[string]string{
				HarvesterUpgradePlanLabel:      upgradePlan.Name,
				HarvesterUpgradeComponentLabel: NodeComponent,
				HarvesterJobTypeLabel:          jobType,
				HarvesterUpgradeNodeLabel:      nodeName,
			},
		},
		Spec: batchv1.JobSpec{
			Suspend:                 suspendPtr,
			TTLSecondsAfterFinished: ptr.To[int32](defaultTTLSecondsAfterFinished),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						HarvesterUpgradePlanLabel:      upgradePlan.Name,
						HarvesterUpgradeComponentLabel: NodeComponent,
						HarvesterJobTypeLabel:          jobType,
						HarvesterUpgradeNodeLabel:      nodeName,
					},
				},
				Spec: corev1.PodSpec{
					RestartPolicy:      corev1.RestartPolicyNever,
					NodeName:           nodeName,
					ServiceAccountName: serviceAccountName,
					HostPID:            true,
					Tolerations:        getDefaultTolerations(),
					Containers: []corev1.Container{
						{
							Name:    "apply",
							Image:   fmt.Sprintf("%s:%s", upgradeToolkitImage, getUpgradeVersion(upgradePlan)),
							Command: []string{"upgrade_node.sh"},
							Args:    []string{jobType},
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
	version, serviceAccountName string,
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
			ServiceAccountName:    serviceAccountName,
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

// listManagedNodes returns all nodes labeled with harvesterManagedLabel=true.
func listManagedNodes(ctx context.Context, c client.Reader) ([]corev1.Node, error) {
	var nodeList corev1.NodeList
	if err := c.List(ctx, &nodeList, &client.ListOptions{
		LabelSelector: labels.SelectorFromSet(labels.Set{
			harvesterManagedLabel: valueTrue,
		}),
	}); err != nil {
		return nil, err
	}
	return nodeList.Items, nil
}

// countManagedNodes returns the number of nodes labeled with harvesterManagedLabel=true.
func countManagedNodes(ctx context.Context, c client.Reader) (int, error) {
	nodes, err := listManagedNodes(ctx, c)
	if err != nil {
		return 0, err
	}
	return len(nodes), nil
}

// IsWitnessNode returns true if the node has the witness role label.
func IsWitnessNode(node *corev1.Node) bool {
	_, ok := node.Labels[witnessNodeRoleLabel]
	return ok
}

// IsRestoreVMEnabled returns true if the UpgradePlan has restoreVM enabled.
func IsRestoreVMEnabled(up *managementv1beta1.UpgradePlan) bool {
	return up.Spec.RestoreVM != nil && *up.Spec.RestoreVM
}

// ConstructRestoreVMJob builds a Job that runs the restore-vm CLI command on a specific node.
func ConstructRestoreVMJob(
	upgradePlan *managementv1beta1.UpgradePlan,
	nodeName, jobName, serviceAccountName string,
) *batchv1.Job {
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: harvesterSystemNamespace,
			Labels: map[string]string{
				HarvesterUpgradePlanLabel:      upgradePlan.Name,
				HarvesterUpgradeComponentLabel: NodeComponent,
				HarvesterJobTypeLabel:          JobTypeRestoreVM,
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
						HarvesterJobTypeLabel:          JobTypeRestoreVM,
						HarvesterUpgradeNodeLabel:      nodeName,
					},
				},
				Spec: corev1.PodSpec{
					RestartPolicy:      corev1.RestartPolicyNever,
					NodeName:           nodeName,
					ServiceAccountName: serviceAccountName,
					Tolerations:        getDefaultTolerations(),
					Containers: []corev1.Container{
						{
							Name:    "restore-vm",
							Image:   fmt.Sprintf("%s:%s", upgradeToolkitImage, getUpgradeVersion(upgradePlan)),
							Command: []string{"upgrade-toolkit"},
							Args:    []string{"restore-vm", "--node", nodeName, "--upgrade", upgradePlan.Name},
						},
					},
				},
			},
		},
	}
}

// resolveImagePreloadConcurrency computes the effective SUC plan concurrency.
// It returns:
// - (concurrency, false) for normal operation
// - (0, true) when image preloading should be skipped entirely
func resolveImagePreloadConcurrency(opt *managementv1beta1.ImagePreloadOption, nodeCount int) (int, bool) {
	if opt == nil || opt.Concurrency == nil || *opt.Concurrency == 0 {
		return nodeCount, false
	}
	c := *opt.Concurrency
	if c < 0 {
		return 0, true
	}
	if c > nodeCount {
		return nodeCount, false
	}
	return c, false
}
