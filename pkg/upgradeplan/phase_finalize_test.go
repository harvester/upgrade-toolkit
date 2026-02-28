package upgradeplan

import (
	"context"
	"fmt"
	"testing"

	"github.com/go-logr/logr"
	harvesterv1beta1 "github.com/harvester/harvester/pkg/apis/harvesterhci.io/v1beta1"
	provisioningv1 "github.com/rancher/rancher/pkg/apis/provisioning.cattle.io/v1"
	rkev1 "github.com/rancher/rancher/pkg/apis/rke.cattle.io/v1"
	upgradev1 "github.com/rancher/system-upgrade-controller/pkg/apis/upgrade.cattle.io/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
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

func newFinalizePhase(objs ...client.Object) *FinalizePhase {
	scheme := runtime.NewScheme()
	_ = managementv1beta1.AddToScheme(scheme)
	_ = provisioningv1.AddToScheme(scheme)
	_ = harvesterv1beta1.AddToScheme(scheme)
	_ = upgradev1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = batchv1.AddToScheme(scheme)

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objs...).
		Build()

	return NewFinalizePhase(&PhaseDeps{
		Client: fakeClient,
		Scheme: scheme,
		Log:    logr.Discard(),
	})
}

func upgradeLabels(component string) map[string]string {
	return map[string]string{
		HarvesterUpgradePlanLabel:      testUpgradePlanName,
		HarvesterUpgradeComponentLabel: component,
	}
}

func TestPreRun_DeletesExistingResources(t *testing.T) {
	up := newTestUpgradePlan()

	vmImage := &harvesterv1beta1.VirtualMachineImage{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-%s", testUpgradePlanName, imageComponent),
			Namespace: harvesterSystemNamespace,
			Labels:    upgradeLabels(imageComponent),
		},
	}
	deploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-%s", testUpgradePlanName, repoComponent),
			Namespace: harvesterSystemNamespace,
			Labels:    upgradeLabels(repoComponent),
		},
	}
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-%s", testUpgradePlanName, repoComponent),
			Namespace: harvesterSystemNamespace,
			Labels:    upgradeLabels(repoComponent),
		},
	}
	preparePlan := &upgradev1.Plan{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-%s", testUpgradePlanName, PrepareComponent),
			Namespace: cattleSystemNamespace,
			Labels:    upgradeLabels(PrepareComponent),
		},
	}
	clusterJob := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-%s", testUpgradePlanName, ClusterComponent),
			Namespace: harvesterSystemNamespace,
			Labels:    upgradeLabels(ClusterComponent),
		},
	}
	phase := newFinalizePhase(vmImage, deploy, svc, preparePlan, clusterJob)

	err := phase.PreRun(context.Background(), up)
	require.NoError(t, err)

	ctx := context.Background()

	err = phase.Client.Get(ctx, types.NamespacedName{
		Namespace: harvesterSystemNamespace,
		Name:      vmImage.Name,
	}, &harvesterv1beta1.VirtualMachineImage{})
	assert.Error(t, err, "VirtualMachineImage should be deleted")

	err = phase.Client.Get(ctx, types.NamespacedName{
		Namespace: harvesterSystemNamespace,
		Name:      deploy.Name,
	}, &appsv1.Deployment{})
	assert.Error(t, err, "Deployment should be deleted")

	err = phase.Client.Get(ctx, types.NamespacedName{
		Namespace: harvesterSystemNamespace,
		Name:      svc.Name,
	}, &corev1.Service{})
	assert.Error(t, err, "Service should be deleted")

	err = phase.Client.Get(ctx, types.NamespacedName{
		Namespace: cattleSystemNamespace,
		Name:      preparePlan.Name,
	}, &upgradev1.Plan{})
	assert.Error(t, err, "Prepare Plan should be deleted")

	err = phase.Client.Get(ctx, types.NamespacedName{
		Namespace: harvesterSystemNamespace,
		Name:      clusterJob.Name,
	}, &batchv1.Job{})
	assert.Error(t, err, "Cluster upgrade Job should be deleted")

}

func TestPreRun_IdempotentOnMissingResources(t *testing.T) {
	up := newTestUpgradePlan()
	phase := newFinalizePhase()

	err := phase.PreRun(context.Background(), up)
	require.NoError(t, err)
}

func TestPreRun_DeletesDrainHookJobs(t *testing.T) {
	up := newTestUpgradePlan()

	preDrainJob := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-%s-%s-node1", testUpgradePlanName, NodeComponent, DrainHookTypePreDrain),
			Namespace: harvesterSystemNamespace,
			Labels: map[string]string{
				HarvesterUpgradePlanLabel:      testUpgradePlanName,
				HarvesterUpgradeComponentLabel: NodeComponent,
				HarvesterDrainHookTypeLabel:    DrainHookTypePreDrain,
				HarvesterUpgradeNodeLabel:      "node1",
			},
		},
	}
	postDrainJob := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-%s-%s-node1", testUpgradePlanName, NodeComponent, DrainHookTypePostDrain),
			Namespace: harvesterSystemNamespace,
			Labels: map[string]string{
				HarvesterUpgradePlanLabel:      testUpgradePlanName,
				HarvesterUpgradeComponentLabel: NodeComponent,
				HarvesterDrainHookTypeLabel:    DrainHookTypePostDrain,
				HarvesterUpgradeNodeLabel:      "node1",
			},
		},
	}
	// A Job from a different upgrade plan — should NOT be deleted
	otherJob := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "other-upgradeplan-node-upgrade-pre-drain-node1",
			Namespace: harvesterSystemNamespace,
			Labels: map[string]string{
				HarvesterUpgradePlanLabel:      "other-upgradeplan",
				HarvesterUpgradeComponentLabel: NodeComponent,
				HarvesterDrainHookTypeLabel:    DrainHookTypePreDrain,
				HarvesterUpgradeNodeLabel:      "node1",
			},
		},
	}

	phase := newFinalizePhase(preDrainJob, postDrainJob, otherJob)

	err := phase.PreRun(context.Background(), up)
	require.NoError(t, err)

	ctx := context.Background()

	err = phase.Client.Get(ctx, types.NamespacedName{
		Namespace: harvesterSystemNamespace,
		Name:      preDrainJob.Name,
	}, &batchv1.Job{})
	assert.Error(t, err, "pre-drain Job should be deleted")

	err = phase.Client.Get(ctx, types.NamespacedName{
		Namespace: harvesterSystemNamespace,
		Name:      postDrainJob.Name,
	}, &batchv1.Job{})
	assert.Error(t, err, "post-drain Job should be deleted")

	err = phase.Client.Get(ctx, types.NamespacedName{
		Namespace: harvesterSystemNamespace,
		Name:      otherJob.Name,
	}, &batchv1.Job{})
	assert.NoError(t, err, "other upgrade plan's Job should NOT be deleted")
}

func TestPreRun_RevertsClusterUpgradeStrategy(t *testing.T) {
	up := newTestUpgradePlan()
	up.Status.ProvisionGeneration = ptr.To(5)

	cluster := &provisioningv1.Cluster{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: FleetLocalNamespace,
			Name:      LocalClusterName,
		},
		Spec: provisioningv1.ClusterSpec{
			KubernetesVersion: "v1.31.0-rke2r1",
			RKEConfig: &provisioningv1.RKEConfig{
				RKEClusterSpecCommon: rkev1.RKEClusterSpecCommon{
					ProvisionGeneration: 5,
					Registries: &rkev1.Registry{
						Mirrors: map[string]rkev1.Mirror{
							"docker.io": {Endpoints: []string{"https://mirror.example.com"}},
						},
					},
					UpgradeStrategy: rkev1.ClusterUpgradeStrategy{
						ControlPlaneConcurrency: "1",
						WorkerConcurrency:       "1",
						ControlPlaneDrainOptions: rkev1.DrainOptions{
							Enabled:       true,
							Force:         true,
							PreDrainHooks: []rkev1.DrainHook{{Annotation: PreHookAnnotation}},
						},
					},
				},
			},
		},
	}

	phase := newFinalizePhase(cluster)

	err := phase.PreRun(context.Background(), up)
	require.NoError(t, err)

	var patched provisioningv1.Cluster
	err = phase.Client.Get(context.Background(), types.NamespacedName{
		Namespace: FleetLocalNamespace,
		Name:      LocalClusterName,
	}, &patched)
	require.NoError(t, err)

	assert.Equal(t, rkev1.ClusterUpgradeStrategy{}, patched.Spec.RKEConfig.UpgradeStrategy)
	assert.Equal(t, 5, patched.Spec.RKEConfig.ProvisionGeneration)
	require.NotNil(t, patched.Spec.RKEConfig.Registries)
	assert.Contains(t, patched.Spec.RKEConfig.Registries.Mirrors, "docker.io")
	assert.Equal(t, "v1.31.0-rke2r1", patched.Spec.KubernetesVersion,
		"KubernetesVersion should be preserved")
}

func TestPreRun_SkipsClusterRevertWhenNoProvisionGeneration(t *testing.T) {
	up := newTestUpgradePlan()

	cluster := &provisioningv1.Cluster{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: FleetLocalNamespace,
			Name:      LocalClusterName,
		},
		Spec: provisioningv1.ClusterSpec{
			KubernetesVersion: "v1.30.0-rke2r1",
			RKEConfig: &provisioningv1.RKEConfig{
				RKEClusterSpecCommon: rkev1.RKEClusterSpecCommon{
					ProvisionGeneration: 3,
					UpgradeStrategy: rkev1.ClusterUpgradeStrategy{
						ControlPlaneConcurrency: "1",
						WorkerConcurrency:       "1",
					},
				},
			},
		},
	}

	phase := newFinalizePhase(cluster)

	err := phase.PreRun(context.Background(), up)
	require.NoError(t, err)

	var patched provisioningv1.Cluster
	err = phase.Client.Get(context.Background(), types.NamespacedName{
		Namespace: FleetLocalNamespace,
		Name:      LocalClusterName,
	}, &patched)
	require.NoError(t, err)

	assert.Equal(t, "1", patched.Spec.RKEConfig.UpgradeStrategy.ControlPlaneConcurrency)
	assert.Equal(t, "1", patched.Spec.RKEConfig.UpgradeStrategy.WorkerConcurrency)
}

func TestPreRun_CleansUpNodePendingOSImageAnnotation(t *testing.T) {
	up := newTestUpgradePlan()

	nodeWithAnnotation := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node1",
			Annotations: map[string]string{
				PendingOSImageAnnotation: "Harvester v1.4.0",
				"other-annotation":       "keep-me",
			},
		},
	}
	nodeClean := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node2",
		},
	}

	phase := newFinalizePhase(nodeWithAnnotation, nodeClean)

	err := phase.PreRun(context.Background(), up)
	require.NoError(t, err)

	ctx := context.Background()

	var patchedNode1 corev1.Node
	err = phase.Client.Get(ctx, types.NamespacedName{Name: "node1"}, &patchedNode1)
	require.NoError(t, err)
	assert.NotContains(t, patchedNode1.Annotations, PendingOSImageAnnotation,
		"pendingOSImage annotation should be removed")
	assert.Equal(t, "keep-me", patchedNode1.Annotations["other-annotation"],
		"other annotations should be preserved")

	var patchedNode2 corev1.Node
	err = phase.Client.Get(ctx, types.NamespacedName{Name: "node2"}, &patchedNode2)
	require.NoError(t, err)
}

func TestPreRun_NoopForCleanNodes(t *testing.T) {
	up := newTestUpgradePlan()

	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node1",
			Annotations: map[string]string{
				"some-other-annotation": "value",
			},
		},
	}

	phase := newFinalizePhase(node)

	err := phase.PreRun(context.Background(), up)
	require.NoError(t, err)

	var patched corev1.Node
	err = phase.Client.Get(context.Background(), types.NamespacedName{Name: "node1"}, &patched)
	require.NoError(t, err)
	assert.Equal(t, "value", patched.Annotations["some-other-annotation"])
}

func TestRun_SetsSucceeded(t *testing.T) {
	up := newTestUpgradePlan()
	up.Status.CurrentPhase = managementv1beta1.UpgradePlanPhaseNodeUpgraded

	phase := newFinalizePhase()

	_, err := phase.Run(context.Background(), up)
	require.NoError(t, err)

	assert.Equal(t, managementv1beta1.UpgradePlanPhaseSucceeded, up.Status.CurrentPhase)
}

func TestRun_PreservesFailed(t *testing.T) {
	up := newTestUpgradePlan()
	up.Status.CurrentPhase = managementv1beta1.UpgradePlanPhaseFailed

	phase := newFinalizePhase()

	_, err := phase.Run(context.Background(), up)
	require.NoError(t, err)

	assert.Equal(t, managementv1beta1.UpgradePlanPhaseFailed, up.Status.CurrentPhase)
}
