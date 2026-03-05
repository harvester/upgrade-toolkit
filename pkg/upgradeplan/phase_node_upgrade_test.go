package upgradeplan

import (
	"context"
	"testing"

	"github.com/go-logr/logr"
	harvesterv1beta1 "github.com/harvester/harvester/pkg/apis/harvesterhci.io/v1beta1"
	lhv1beta2 "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	provisioningv1 "github.com/rancher/rancher/pkg/apis/provisioning.cattle.io/v1"
	rkev1 "github.com/rancher/rancher/pkg/apis/rke.cattle.io/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	managementv1beta1 "github.com/harvester/upgrade-toolkit/api/v1beta1"
)

func newNodeUpgradePhaseWithAll(objs ...runtime.Object) *NodeUpgradePhase {
	scheme := runtime.NewScheme()
	_ = managementv1beta1.AddToScheme(scheme)
	_ = provisioningv1.AddToScheme(scheme)
	_ = batchv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = lhv1beta2.AddToScheme(scheme)
	_ = harvesterv1beta1.AddToScheme(scheme)

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithRuntimeObjects(objs...).
		Build()

	return NewNodeUpgradePhase(&PhaseDeps{
		Client:             fakeClient,
		Scheme:             scheme,
		Log:                logr.Discard(),
		JobServiceAccount:  "harvester",
		PlanServiceAccount: "system-upgrade-controller",
	})
}

func newTestCluster(k8sVersion string, provisionGeneration int) *provisioningv1.Cluster {
	return &provisioningv1.Cluster{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: FleetLocalNamespace,
			Name:      LocalClusterName,
		},
		Spec: provisioningv1.ClusterSpec{
			KubernetesVersion: k8sVersion,
			RKEConfig: &provisioningv1.RKEConfig{
				RKEClusterSpecCommon: rkev1.RKEClusterSpecCommon{
					ProvisionGeneration: provisionGeneration,
				},
			},
		},
	}
}

func newTestUpgradePlanWithMetadata(k8sVersion string) *managementv1beta1.UpgradePlan {
	up := &managementv1beta1.UpgradePlan{
		ObjectMeta: metav1.ObjectMeta{
			Name: testUpgradePlanName,
		},
		Spec: managementv1beta1.UpgradePlanSpec{
			Version: testVersion,
		},
		Status: managementv1beta1.UpgradePlanStatus{
			ReleaseMetadata: &managementv1beta1.ReleaseMetadata{
				Kubernetes: k8sVersion,
			},
		},
	}
	return up
}

func newNodeUpgradePhase(objs ...runtime.Object) *NodeUpgradePhase {
	scheme := runtime.NewScheme()
	_ = managementv1beta1.AddToScheme(scheme)
	_ = provisioningv1.AddToScheme(scheme)

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithRuntimeObjects(objs...).
		Build()

	return NewNodeUpgradePhase(&PhaseDeps{
		Client:             fakeClient,
		Scheme:             scheme,
		Log:                logr.Discard(),
		JobServiceAccount:  "harvester",
		PlanServiceAccount: "system-upgrade-controller",
	})
}

func TestEnsureClusterPatched_FirstCall(t *testing.T) {
	cluster := newTestCluster("v1.30.0+rke2r1", 0)
	up := newTestUpgradePlanWithMetadata("v1.31.0+rke2r1")

	phase := newNodeUpgradePhase(cluster)

	err := phase.ensureClusterPatched(context.Background(), up)
	require.NoError(t, err)

	// Verify Cluster was patched
	var patched provisioningv1.Cluster
	err = phase.Client.Get(context.Background(), types.NamespacedName{
		Namespace: FleetLocalNamespace,
		Name:      LocalClusterName,
	}, &patched)
	require.NoError(t, err)

	assert.Equal(t, "v1.31.0-rke2r1", patched.Spec.KubernetesVersion)
	assert.Equal(t, 1, patched.Spec.RKEConfig.ProvisionGeneration)

	// Verify upgrade strategy
	strategy := patched.Spec.RKEConfig.UpgradeStrategy
	assert.Equal(t, "1", strategy.ControlPlaneConcurrency)
	assert.Equal(t, "1", strategy.WorkerConcurrency)

	// Verify control plane drain options
	cpDrain := strategy.ControlPlaneDrainOptions
	assert.True(t, cpDrain.Enabled)
	assert.True(t, cpDrain.Force)
	require.NotNil(t, cpDrain.IgnoreDaemonSets)
	assert.True(t, *cpDrain.IgnoreDaemonSets)
	assert.True(t, cpDrain.DeleteEmptyDirData)

	// Verify control plane drain hooks
	require.Len(t, cpDrain.PreDrainHooks, 1)
	assert.Equal(t, PreHookAnnotation, cpDrain.PreDrainHooks[0].Annotation)
	require.Len(t, cpDrain.PostDrainHooks, 1)
	assert.Equal(t, PostHookAnnotation, cpDrain.PostDrainHooks[0].Annotation)

	// Verify worker drain options
	workerDrain := strategy.WorkerDrainOptions
	assert.True(t, workerDrain.Enabled)
	assert.True(t, workerDrain.Force)
	require.NotNil(t, workerDrain.IgnoreDaemonSets)
	assert.True(t, *workerDrain.IgnoreDaemonSets)
	assert.True(t, workerDrain.DeleteEmptyDirData)

	// Verify worker drain hooks
	require.Len(t, workerDrain.PreDrainHooks, 1)
	assert.Equal(t, PreHookAnnotation, workerDrain.PreDrainHooks[0].Annotation)
	require.Len(t, workerDrain.PostDrainHooks, 1)
	assert.Equal(t, PostHookAnnotation, workerDrain.PostDrainHooks[0].Annotation)

	// Verify UpgradePlan status field was set
	require.NotNil(t, up.Status.ProvisionGeneration)
	assert.Equal(t, 1, *up.Status.ProvisionGeneration)
}

func TestEnsureClusterPatched_SecondCallIsNoop(t *testing.T) {
	cluster := newTestCluster("v1.31.0-rke2r1", 1)
	up := newTestUpgradePlanWithMetadata("v1.31.0+rke2r1")
	up.Status.ProvisionGeneration = ptr.To(1)

	phase := newNodeUpgradePhase(cluster)

	err := phase.ensureClusterPatched(context.Background(), up)
	require.NoError(t, err)

	// Verify provisionGeneration was NOT incremented (still 1)
	var patched provisioningv1.Cluster
	err = phase.Client.Get(context.Background(), types.NamespacedName{
		Namespace: FleetLocalNamespace,
		Name:      LocalClusterName,
	}, &patched)
	require.NoError(t, err)

	assert.Equal(t, 1, patched.Spec.RKEConfig.ProvisionGeneration)
}

func TestEnsureClusterPatched_SameVersionUpgrade(t *testing.T) {
	// Core bug scenario: same K8s version but no provisionGeneration in status — should still patch
	cluster := newTestCluster("v1.31.0-rke2r1", 5)
	up := newTestUpgradePlanWithMetadata("v1.31.0+rke2r1")

	phase := newNodeUpgradePhase(cluster)

	err := phase.ensureClusterPatched(context.Background(), up)
	require.NoError(t, err)

	// Verify provisionGeneration was incremented
	var patched provisioningv1.Cluster
	err = phase.Client.Get(context.Background(), types.NamespacedName{
		Namespace: FleetLocalNamespace,
		Name:      LocalClusterName,
	}, &patched)
	require.NoError(t, err)

	assert.Equal(t, 6, patched.Spec.RKEConfig.ProvisionGeneration)

	// Verify status field was set
	require.NotNil(t, up.Status.ProvisionGeneration)
	assert.Equal(t, 6, *up.Status.ProvisionGeneration)
}

func newNodeUpgradePhaseWithBatch(objs ...runtime.Object) *NodeUpgradePhase {
	scheme := runtime.NewScheme()
	_ = managementv1beta1.AddToScheme(scheme)
	_ = provisioningv1.AddToScheme(scheme)
	_ = batchv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithRuntimeObjects(objs...).
		Build()

	return NewNodeUpgradePhase(&PhaseDeps{
		Client:             fakeClient,
		Scheme:             scheme,
		Log:                logr.Discard(),
		JobServiceAccount:  "harvester",
		PlanServiceAccount: "system-upgrade-controller",
	})
}

func TestRunSingleNode_CreatesJob(t *testing.T) {
	up := newTestUpgradePlanWithMetadata("v1.31.0+rke2r1")
	up.Status.SingleNode = ptr.To("single-node-1")
	up.Status.NodeUpgradeStatuses = map[string]managementv1beta1.NodeUpgradeStatus{
		"single-node-1": {State: managementv1beta1.NodeStateImagePreloaded},
	}

	phase := newNodeUpgradePhaseWithBatch(up)

	_, err := phase.Run(context.Background(), up)
	require.NoError(t, err)

	// Verify the single-node-upgrade Job was created
	var jobList batchv1.JobList
	err = phase.Client.List(context.Background(), &jobList, client.InNamespace(HarvesterSystemNamespace))
	require.NoError(t, err)
	require.Len(t, jobList.Items, 1)

	job := jobList.Items[0]
	assert.Equal(t, up.Name, job.Labels[HarvesterUpgradePlanLabel])
	assert.Equal(t, NodeComponent, job.Labels[HarvesterUpgradeComponentLabel])
	assert.Equal(t, JobTypeSingleNodeUpgrade, job.Labels[HarvesterJobTypeLabel])
	assert.Equal(t, "single-node-1", job.Labels[HarvesterUpgradeNodeLabel])
	assert.Equal(t, []string{"single-node-upgrade"}, job.Spec.Template.Spec.Containers[0].Args)

	// Phase should still be NodeUpgrading (node not yet terminal)
	assert.Equal(t, managementv1beta1.UpgradePlanPhaseNodeUpgrading, up.Status.CurrentPhase)
}

func TestRunSingleNode_IdempotentJobCreation(t *testing.T) {
	up := newTestUpgradePlanWithMetadata("v1.31.0+rke2r1")
	up.Status.SingleNode = ptr.To("single-node-1")
	up.Status.NodeUpgradeStatuses = map[string]managementv1beta1.NodeUpgradeStatus{
		"single-node-1": {State: managementv1beta1.NodeStateImagePreloaded},
	}

	phase := newNodeUpgradePhaseWithBatch(up)

	// First call
	_, err := phase.Run(context.Background(), up)
	require.NoError(t, err)

	// Second call
	_, err = phase.Run(context.Background(), up)
	require.NoError(t, err)

	// Verify only one Job exists
	var jobList batchv1.JobList
	err = phase.Client.List(context.Background(), &jobList, client.InNamespace(HarvesterSystemNamespace))
	require.NoError(t, err)
	assert.Len(t, jobList.Items, 1)
}

func TestRunSingleNode_FailedNode(t *testing.T) {
	up := newTestUpgradePlanWithMetadata("v1.31.0+rke2r1")
	up.Status.SingleNode = ptr.To("single-node-1")
	up.Status.NodeUpgradeStatuses = map[string]managementv1beta1.NodeUpgradeStatus{
		"single-node-1": {
			State:   managementv1beta1.NodeStateSingleNodeUpgradeFailed,
			Message: "job failed",
		},
	}

	phase := newNodeUpgradePhaseWithBatch(up)

	_, err := phase.Run(context.Background(), up)
	require.NoError(t, err)

	// Phase should transition to Failed
	assert.Equal(t, managementv1beta1.UpgradePlanPhaseFailed, up.Status.CurrentPhase)
}

func TestRunSingleNode_TerminalNode(t *testing.T) {
	up := newTestUpgradePlanWithMetadata("v1.31.0+rke2r1")
	up.Status.SingleNode = ptr.To("single-node-1")
	up.Status.NodeUpgradeStatuses = map[string]managementv1beta1.NodeUpgradeStatus{
		"single-node-1": {State: managementv1beta1.NodeStateSingleNodeUpgraded},
	}

	phase := newNodeUpgradePhaseWithBatch(up)

	_, err := phase.Run(context.Background(), up)
	require.NoError(t, err)

	// Phase should transition to NodeUpgraded
	assert.Equal(t, managementv1beta1.UpgradePlanPhaseNodeUpgraded, up.Status.CurrentPhase)
}

func TestCheckNodeStatuses_MultiNode_AllTerminal(t *testing.T) {
	up := newTestUpgradePlanWithMetadata("v1.32.0+rke2r1")
	up.Status.NodeUpgradeStatuses = map[string]managementv1beta1.NodeUpgradeStatus{
		"node-1": {State: managementv1beta1.NodeStatePostDrained},
		"node-2": {State: managementv1beta1.NodeStatePostDrained},
		"node-3": {State: managementv1beta1.NodeStatePostDrained},
	}

	phase := newNodeUpgradePhaseWithBatch(up)

	_, err := phase.checkNodeStatuses(up)
	require.NoError(t, err)
	assert.Equal(t, managementv1beta1.UpgradePlanPhaseNodeUpgraded, up.Status.CurrentPhase)
}

func TestCheckNodeStatuses_MultiNode_OneNotTerminal(t *testing.T) {
	up := newTestUpgradePlanWithMetadata("v1.31.0+rke2r1")
	up.Status.NodeUpgradeStatuses = map[string]managementv1beta1.NodeUpgradeStatus{
		"node-1": {State: managementv1beta1.NodeStatePostDrained},
		"node-2": {State: managementv1beta1.NodeStatePreDraining},
		"node-3": {State: managementv1beta1.NodeStatePostDrained},
	}

	phase := newNodeUpgradePhaseWithBatch(up)

	_, err := phase.checkNodeStatuses(up)
	require.NoError(t, err)
	assert.Equal(t, managementv1beta1.UpgradePlanPhaseNodeUpgrading, up.Status.CurrentPhase)
}

func TestCheckNodeStatuses_MultiNode_PausedAndNonTerminal(t *testing.T) {
	up := newTestUpgradePlanWithMetadata("v1.31.0+rke2r1")
	up.Status.NodeUpgradeStatuses = map[string]managementv1beta1.NodeUpgradeStatus{
		"node-1": {State: managementv1beta1.NodeStatePostDrained},
		"node-2": {State: managementv1beta1.NodeStateImagePreloaded},
		"node-3": {
			State:   managementv1beta1.NodeStateUpgradePaused,
			Reason:  "AdministrativelyPaused",
			Message: "Node upgrade paused as requested by the user",
		},
	}

	phase := newNodeUpgradePhaseWithBatch(up)

	_, err := phase.checkNodeStatuses(up)
	require.NoError(t, err)
	assert.Equal(t, managementv1beta1.UpgradePlanPhaseNodeUpgrading, up.Status.CurrentPhase)

	cond := up.LookupCondition(managementv1beta1.UpgradePlanProgressing)
	assert.Contains(t, cond.Message, "node-3")
	assert.Contains(t, cond.Message, "node upgrade paused")
}

func TestCheckNodeStatuses_MultiNode_OnlyPaused(t *testing.T) {
	up := newTestUpgradePlanWithMetadata("v1.31.0+rke2r1")
	up.Status.NodeUpgradeStatuses = map[string]managementv1beta1.NodeUpgradeStatus{
		"node-1": {State: managementv1beta1.NodeStatePostDrained},
		"node-2": {
			State:   managementv1beta1.NodeStateUpgradePaused,
			Reason:  "AdministrativelyPaused",
			Message: "Node upgrade paused as requested by the user",
		},
		"node-3": {State: managementv1beta1.NodeStatePostDrained},
	}

	phase := newNodeUpgradePhaseWithBatch(up)

	_, err := phase.checkNodeStatuses(up)
	require.NoError(t, err)
	assert.Equal(t, managementv1beta1.UpgradePlanPhaseNodeUpgrading, up.Status.CurrentPhase)

	cond := up.LookupCondition(managementv1beta1.UpgradePlanProgressing)
	assert.Contains(t, cond.Message, "node-2")
	assert.Contains(t, cond.Message, "node upgrade paused")
}

func TestCheckNodeStatuses_MultiNode_MultiplePaused(t *testing.T) {
	up := newTestUpgradePlanWithMetadata("v1.31.0+rke2r1")
	up.Status.NodeUpgradeStatuses = map[string]managementv1beta1.NodeUpgradeStatus{
		"node-1": {State: managementv1beta1.NodeStatePostDrained},
		"node-2": {
			State:   managementv1beta1.NodeStateUpgradePaused,
			Reason:  "AdministrativelyPaused",
			Message: "Node upgrade paused as requested by the user",
		},
		"node-3": {
			State:   managementv1beta1.NodeStateUpgradePaused,
			Reason:  "AdministrativelyPaused",
			Message: "Node upgrade paused as requested by the user",
		},
	}

	phase := newNodeUpgradePhaseWithBatch(up)

	_, err := phase.checkNodeStatuses(up)
	require.NoError(t, err)
	assert.Equal(t, managementv1beta1.UpgradePlanPhaseNodeUpgrading, up.Status.CurrentPhase)

	cond := up.LookupCondition(managementv1beta1.UpgradePlanProgressing)
	assert.Equal(t, "node upgrade paused: node-2, node-3", cond.Message)
}

func TestCheckNodeStatuses_MultiNode_FailedTakesPriority(t *testing.T) {
	up := newTestUpgradePlanWithMetadata("v1.31.0+rke2r1")
	up.Status.NodeUpgradeStatuses = map[string]managementv1beta1.NodeUpgradeStatus{
		"node-1": {
			State:   managementv1beta1.NodeStatePreDrainFailed,
			Message: "pre-drain hook failed",
		},
		"node-2": {
			State:   managementv1beta1.NodeStateUpgradePaused,
			Reason:  "AdministrativelyPaused",
			Message: "Node upgrade paused as requested by the user",
		},
	}

	phase := newNodeUpgradePhaseWithBatch(up)

	_, err := phase.checkNodeStatuses(up)
	require.NoError(t, err)
	assert.Equal(t, managementv1beta1.UpgradePlanPhaseFailed, up.Status.CurrentPhase)
}

func newMachinePlanSecret(name string, annotations map[string]string) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   FleetLocalNamespace,
			Annotations: annotations,
		},
		Type: corev1.SecretType(MachinePlanSecretType),
	}
}

func TestPostRun_MultiNode_PostDrainAnnotationPresent(t *testing.T) {
	up := newTestUpgradePlanWithMetadata("v1.31.0+rke2r1")
	up.Status.NodeUpgradeStatuses = map[string]managementv1beta1.NodeUpgradeStatus{
		"node-1": {State: managementv1beta1.NodeStatePostDrained},
	}

	secret := newMachinePlanSecret("machine-plan-1", map[string]string{
		RKE2PostDrainAnnotation: "drain-1",
	})

	phase := newNodeUpgradePhaseWithBatch(up, secret)

	err := phase.PostRun(context.Background(), up)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "waiting for Rancher to complete node upgrades")
}

func TestPostRun_MultiNode_AllAnnotationsCleared(t *testing.T) {
	up := newTestUpgradePlanWithMetadata("v1.31.0+rke2r1")
	up.Status.NodeUpgradeStatuses = map[string]managementv1beta1.NodeUpgradeStatus{
		"node-1": {State: managementv1beta1.NodeStatePostDrained},
		"node-2": {State: managementv1beta1.NodeStatePostDrained},
	}

	// Secrets with no rke2/post-drain annotation (Rancher has cleared them)
	secret1 := newMachinePlanSecret("machine-plan-1", nil)
	secret2 := newMachinePlanSecret("machine-plan-2", nil)

	phase := newNodeUpgradePhaseWithBatch(up, secret1, secret2)

	err := phase.PostRun(context.Background(), up)
	assert.NoError(t, err)
}

func TestPostRun_SingleNode_Skipped(t *testing.T) {
	up := newTestUpgradePlanWithMetadata("v1.31.0+rke2r1")
	up.Status.SingleNode = ptr.To("single-node-1")
	up.Status.NodeUpgradeStatuses = map[string]managementv1beta1.NodeUpgradeStatus{
		"single-node-1": {State: managementv1beta1.NodeStateSingleNodeUpgraded},
	}

	// Even with a secret that has rke2/post-drain, PostRun should skip for single-node
	secret := newMachinePlanSecret("machine-plan-1", map[string]string{
		RKE2PostDrainAnnotation: "drain-1",
	})

	phase := newNodeUpgradePhaseWithBatch(up, secret)

	err := phase.PostRun(context.Background(), up)
	assert.NoError(t, err)
}

// --- Longhorn Replica Replenishment Tests ---

func newTestLonghornSetting(value string) *lhv1beta2.Setting {
	return &lhv1beta2.Setting{
		ObjectMeta: metav1.ObjectMeta{
			Name:      LonghornSettingReplicaReplenishment,
			Namespace: LonghornSystemNamespace,
		},
		Value: value,
	}
}

func TestPreRun_ExtendsLonghornReplicaReplenishment(t *testing.T) {
	setting := newTestLonghornSetting("300")
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "node-1",
			Labels: map[string]string{harvesterManagedLabel: "true"},
		},
	}
	up := newTestUpgradePlanWithMetadata("v1.31.0+rke2r1")

	phase := newNodeUpgradePhaseWithAll(setting, node, up)

	err := phase.PreRun(context.Background(), up)
	require.NoError(t, err)

	// Verify annotation saved original value
	assert.Equal(t, "300", up.Annotations[AnnotationReplicaReplenishmentOriginal])

	// Verify setting was patched
	var patched lhv1beta2.Setting
	err = phase.Client.Get(context.Background(), types.NamespacedName{
		Namespace: LonghornSystemNamespace,
		Name:      LonghornSettingReplicaReplenishment,
	}, &patched)
	require.NoError(t, err)
	assert.Equal(t, "1800", patched.Value)
}

func TestPreRun_LonghornReplenishment_Idempotent(t *testing.T) {
	setting := newTestLonghornSetting("1800")
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "node-1",
			Labels: map[string]string{harvesterManagedLabel: "true"},
		},
	}
	up := newTestUpgradePlanWithMetadata("v1.31.0+rke2r1")
	up.Annotations = map[string]string{
		AnnotationReplicaReplenishmentOriginal: "300",
	}

	phase := newNodeUpgradePhaseWithAll(setting, node, up)

	err := phase.PreRun(context.Background(), up)
	require.NoError(t, err)

	// Annotation should still have original value, not "1800"
	assert.Equal(t, "300", up.Annotations[AnnotationReplicaReplenishmentOriginal])
}

func TestPreRun_LonghornSettingNotFound_Skips(t *testing.T) {
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "node-1",
			Labels: map[string]string{harvesterManagedLabel: "true"},
		},
	}
	up := newTestUpgradePlanWithMetadata("v1.31.0+rke2r1")

	phase := newNodeUpgradePhaseWithAll(node, up)

	err := phase.PreRun(context.Background(), up)
	require.NoError(t, err)

	// No annotation should be set
	_, ok := up.Annotations[AnnotationReplicaReplenishmentOriginal]
	assert.False(t, ok)
}

// --- Descheduler Addon Tests ---

func newTestDeschedulerAddon(enabled bool) *harvesterv1beta1.Addon {
	return &harvesterv1beta1.Addon{
		ObjectMeta: metav1.ObjectMeta{
			Name:      DeschedulerAddonName,
			Namespace: DeschedulerAddonNamespace,
		},
		Spec: harvesterv1beta1.AddonSpec{
			Enabled: enabled,
		},
	}
}

func TestPreRun_DisablesDeschedulerAddon(t *testing.T) {
	addon := newTestDeschedulerAddon(true)
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "node-1",
			Labels: map[string]string{harvesterManagedLabel: "true"},
		},
	}
	up := newTestUpgradePlanWithMetadata("v1.31.0+rke2r1")

	phase := newNodeUpgradePhaseWithAll(addon, node, up)

	err := phase.PreRun(context.Background(), up)
	require.NoError(t, err)

	// Verify annotation
	assert.Equal(t, "true", up.Annotations[AnnotationDeschedulerWasEnabled])

	// Verify addon was disabled
	var patched harvesterv1beta1.Addon
	err = phase.Client.Get(context.Background(), types.NamespacedName{
		Namespace: DeschedulerAddonNamespace,
		Name:      DeschedulerAddonName,
	}, &patched)
	require.NoError(t, err)
	assert.False(t, patched.Spec.Enabled)
}

func TestPreRun_DeschedulerAddon_AlreadyDisabled(t *testing.T) {
	addon := newTestDeschedulerAddon(false)
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "node-1",
			Labels: map[string]string{harvesterManagedLabel: "true"},
		},
	}
	up := newTestUpgradePlanWithMetadata("v1.31.0+rke2r1")

	phase := newNodeUpgradePhaseWithAll(addon, node, up)

	err := phase.PreRun(context.Background(), up)
	require.NoError(t, err)

	// No annotation should be set since addon was already disabled
	_, ok := up.Annotations[AnnotationDeschedulerWasEnabled]
	assert.False(t, ok)
}

func TestPreRun_DeschedulerAddon_NotFound(t *testing.T) {
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "node-1",
			Labels: map[string]string{harvesterManagedLabel: "true"},
		},
	}
	up := newTestUpgradePlanWithMetadata("v1.31.0+rke2r1")

	phase := newNodeUpgradePhaseWithAll(node, up)

	err := phase.PreRun(context.Background(), up)
	require.NoError(t, err)

	_, ok := up.Annotations[AnnotationDeschedulerWasEnabled]
	assert.False(t, ok)
}

func TestPreRun_DeschedulerAddon_Idempotent(t *testing.T) {
	addon := newTestDeschedulerAddon(false) // already disabled by previous PreRun
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "node-1",
			Labels: map[string]string{harvesterManagedLabel: "true"},
		},
	}
	up := newTestUpgradePlanWithMetadata("v1.31.0+rke2r1")
	up.Annotations = map[string]string{
		AnnotationDeschedulerWasEnabled: "true",
	}

	phase := newNodeUpgradePhaseWithAll(addon, node, up)

	err := phase.PreRun(context.Background(), up)
	require.NoError(t, err)

	// Annotation should remain
	assert.Equal(t, "true", up.Annotations[AnnotationDeschedulerWasEnabled])
}
