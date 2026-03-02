package upgradeplan

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-logr/logr"
	upgradev1 "github.com/rancher/system-upgrade-controller/pkg/apis/upgrade.cattle.io/v1"
	"github.com/rancher/wrangler/v3/pkg/genericcondition"
	"github.com/rancher/wrangler/v3/pkg/name"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	managementv1beta1 "github.com/harvester/upgrade-toolkit/api/v1beta1"
)

func newImageCleanupPhase(objs ...client.Object) *ImageCleanupPhase {
	scheme := runtime.NewScheme()
	_ = managementv1beta1.AddToScheme(scheme)
	_ = upgradev1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objs...).
		Build()

	return NewImageCleanupPhase(&PhaseDeps{
		Client:             fakeClient,
		Scheme:             scheme,
		Log:                logr.Discard(),
		JobServiceAccount:  "harvester",
		PlanServiceAccount: "system-upgrade-controller",
	})
}

func newTestUpgradePlanWithVersions(previousVersion, currentVersion string) *managementv1beta1.UpgradePlan {
	up := &managementv1beta1.UpgradePlan{
		ObjectMeta: metav1.ObjectMeta{
			Name: testUpgradePlanName,
		},
		Spec: managementv1beta1.UpgradePlanSpec{
			Version: currentVersion,
		},
		Status: managementv1beta1.UpgradePlanStatus{
			PreviousVersion: ptr.To(previousVersion),
		},
	}
	return up
}

func TestConstructPlanForImageCleanup(t *testing.T) {
	up := newTestUpgradePlanWithVersions("v1.1.0", "v1.2.0")
	imagesToPurge := []string{
		"docker.io/rancher/fleet:v0.5.0",
		"docker.io/rancher/rancher:v2.7.0",
	}

	plan := constructPlanForImageCleanup(up, imagesToPurge, "system-upgrade-controller")

	assert.Equal(t, name.SafeConcatName(testUpgradePlanName, ImageCleanupComponent), plan.Name)
	assert.Equal(t, cattleSystemNamespace, plan.Namespace)
	assert.Equal(t, testUpgradePlanName, plan.Labels[HarvesterUpgradePlanLabel])
	assert.Equal(t, ImageCleanupComponent, plan.Labels[HarvesterUpgradeComponentLabel])

	// Spec
	assert.Equal(t, int64(1), plan.Spec.Concurrency)
	assert.Equal(t, "system-upgrade-controller", plan.Spec.ServiceAccountName)
	assert.Equal(t, "v1.2.0", plan.Spec.Version)

	// NodeSelector
	require.NotNil(t, plan.Spec.NodeSelector)
	assert.Equal(t, "true", plan.Spec.NodeSelector.MatchLabels[harvesterManagedLabel])

	// Container
	require.NotNil(t, plan.Spec.Upgrade)
	assert.Equal(t, upgradeToolkitImage, plan.Spec.Upgrade.Image)
	assert.Equal(t, []string{"sh", "-c", imageCleanupScript}, plan.Spec.Upgrade.Command)

	// Env
	require.Len(t, plan.Spec.Upgrade.Env, 1)
	assert.Equal(t, "IMAGES", plan.Spec.Upgrade.Env[0].Name)
	assert.Equal(t, "docker.io/rancher/fleet:v0.5.0 docker.io/rancher/rancher:v2.7.0", plan.Spec.Upgrade.Env[0].Value)

	// Tolerations
	assert.NotEmpty(t, plan.Spec.Tolerations)

	// No drain/cordon
	assert.False(t, plan.Spec.Cordon)
	assert.Nil(t, plan.Spec.Drain)
}

func TestImageCleanupPhase_PlanAlreadyCompleted(t *testing.T) {
	up := newTestUpgradePlanWithVersions("v1.1.0", "v1.2.0")
	up.Status.CurrentPhase = managementv1beta1.UpgradePlanPhaseCleaningUp

	completedPlan := &upgradev1.Plan{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name.SafeConcatName(testUpgradePlanName, ImageCleanupComponent),
			Namespace: cattleSystemNamespace,
			Labels: map[string]string{
				HarvesterUpgradePlanLabel:      testUpgradePlanName,
				HarvesterUpgradeComponentLabel: ImageCleanupComponent,
			},
		},
		Status: upgradev1.PlanStatus{
			Conditions: []genericcondition.GenericCondition{
				{
					Type:   string(upgradev1.PlanComplete),
					Status: corev1.ConditionTrue,
				},
			},
		},
	}

	phase := newImageCleanupPhase(completedPlan)
	_, err := phase.Run(context.Background(), up)
	require.NoError(t, err)
	assert.Equal(t, managementv1beta1.UpgradePlanPhaseCleanedUp, up.Status.CurrentPhase)
}

func TestImageCleanupPhase_PlanRunning(t *testing.T) {
	up := newTestUpgradePlanWithVersions("v1.1.0", "v1.3.0")
	up.Status.CurrentPhase = managementv1beta1.UpgradePlanPhaseCleaningUp

	runningPlan := &upgradev1.Plan{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name.SafeConcatName(testUpgradePlanName, ImageCleanupComponent),
			Namespace: cattleSystemNamespace,
			Labels: map[string]string{
				HarvesterUpgradePlanLabel:      testUpgradePlanName,
				HarvesterUpgradeComponentLabel: ImageCleanupComponent,
			},
		},
		// No conditions set — plan is still running
	}

	phase := newImageCleanupPhase(runningPlan)
	_, err := phase.Run(context.Background(), up)
	require.NoError(t, err)
	assert.Equal(t, managementv1beta1.UpgradePlanPhaseCleaningUp, up.Status.CurrentPhase)
}

func TestImageCleanupPhase_PlanJobFailed(t *testing.T) {
	up := newTestUpgradePlanWithVersions("v1.0.0", "v1.2.0")
	up.Status.CurrentPhase = managementv1beta1.UpgradePlanPhaseCleaningUp

	failedPlan := &upgradev1.Plan{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name.SafeConcatName(testUpgradePlanName, ImageCleanupComponent),
			Namespace: cattleSystemNamespace,
			Labels: map[string]string{
				HarvesterUpgradePlanLabel:      testUpgradePlanName,
				HarvesterUpgradeComponentLabel: ImageCleanupComponent,
			},
		},
		Status: upgradev1.PlanStatus{
			Conditions: []genericcondition.GenericCondition{
				{
					Type:   string(upgradev1.PlanComplete),
					Status: corev1.ConditionFalse,
					Reason: "JobFailed",
				},
			},
		},
	}

	phase := newImageCleanupPhase(failedPlan)
	_, err := phase.Run(context.Background(), up)
	require.NoError(t, err)
	assert.Equal(t, managementv1beta1.UpgradePlanPhaseFailed, up.Status.CurrentPhase)
}

func TestImageCleanupPhase_NoPreviousVersion(t *testing.T) {
	up := &managementv1beta1.UpgradePlan{
		ObjectMeta: metav1.ObjectMeta{
			Name: testUpgradePlanName,
		},
		Spec: managementv1beta1.UpgradePlanSpec{
			Version: "v1.2.0",
		},
		Status: managementv1beta1.UpgradePlanStatus{
			CurrentPhase: managementv1beta1.UpgradePlanPhaseCleaningUp,
		},
	}

	phase := newImageCleanupPhase()
	_, err := phase.Run(context.Background(), up)
	require.NoError(t, err)
	assert.Equal(t, managementv1beta1.UpgradePlanPhaseCleanedUp, up.Status.CurrentPhase)
}

func TestComputeImageDiff(t *testing.T) {
	previousImages := []string{
		"docker.io/rancher/fleet:v0.5.0",
		"docker.io/rancher/harvester:v1.1.0",
		"docker.io/rancher/harvester-upgrade:v1.1.0",
		"docker.io/longhornio/longhorn-engine:v1.4.0",
	}
	currentImages := []string{
		"docker.io/rancher/fleet:v0.6.0",
		"docker.io/rancher/harvester:v1.2.0",
		"docker.io/rancher/harvester-upgrade:v1.2.0",
		"docker.io/longhornio/longhorn-engine:v1.5.0",
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.Contains(r.URL.Path, "v1.1.0"):
			w.WriteHeader(http.StatusOK)
			_, _ = fmt.Fprint(w, strings.Join(previousImages, "\n"))
		case strings.Contains(r.URL.Path, "v1.2.0"):
			w.WriteHeader(http.StatusOK)
			_, _ = fmt.Fprint(w, strings.Join(currentImages, "\n"))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	// Override repoBaseURL for testing by calling computeImageDiff indirectly
	// through fetchImageList + imagesDiff + filterRetainedImages
	httpClient := server.Client()
	baseURL := server.URL + "/harvester-iso"
	ctx := context.Background()

	prev, err := fetchImageList(ctx, httpClient, baseURL, "v1.1.0")
	require.NoError(t, err)
	curr, err := fetchImageList(ctx, httpClient, baseURL, "v1.2.0")
	require.NoError(t, err)

	diff := imagesDiff(prev, curr)
	filtered := filterRetainedImages(diff)

	// harvester-upgrade and longhorn-engine should be filtered out
	assert.Equal(t, []string{
		"docker.io/rancher/fleet:v0.5.0",
		"docker.io/rancher/harvester:v1.1.0",
	}, filtered)
}
