/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package upgradelog

import (
	"context"
	"fmt"
	"testing"

	managementv1beta1 "github.com/harvester/upgrade-toolkit/api/v1beta1"
	buildversion "github.com/harvester/upgrade-toolkit/pkg/version"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestGetCollectorImage(t *testing.T) {
	defaultImage := fmt.Sprintf("%s:%s", CollectorImage, buildversion.Version)

	t.Run("nil upgradeLog returns default", func(t *testing.T) {
		assert.Equal(t, defaultImage, getCollectorImage(nil))
	})

	t.Run("no annotations returns default", func(t *testing.T) {
		ul := &managementv1beta1.UpgradeLog{
			ObjectMeta: metav1.ObjectMeta{Name: "test"},
		}
		assert.Equal(t, defaultImage, getCollectorImage(ul))
	})

	t.Run("empty annotation returns default", func(t *testing.T) {
		ul := &managementv1beta1.UpgradeLog{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test",
				Annotations: map[string]string{
					AnnotationUpgradeToolkitImage: "",
				},
			},
		}
		assert.Equal(t, defaultImage, getCollectorImage(ul))
	})

	t.Run("annotation overrides repo", func(t *testing.T) {
		ul := &managementv1beta1.UpgradeLog{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test",
				Annotations: map[string]string{
					AnnotationUpgradeToolkitImage: "my-registry/harvester-upgrade-toolkit",
				},
			},
		}
		expected := fmt.Sprintf("my-registry/harvester-upgrade-toolkit:%s", buildversion.Version)
		assert.Equal(t, expected, getCollectorImage(ul))
	})
}

func TestEnsureDeployment_LogViewerContainer(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, appsv1.AddToScheme(scheme))

	ul := &managementv1beta1.UpgradeLog{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-upgrade",
		},
		Spec: managementv1beta1.UpgradeLogSpec{
			UpgradePlanName: "my-plan",
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	phase := &CollectorDeployPhase{
		PhaseDeps: &PhaseDeps{
			Client: fakeClient,
			Scheme: scheme,
		},
	}

	err := phase.ensureDeployment(context.Background(), ul)
	require.NoError(t, err)

	var deploy appsv1.Deployment
	err = fakeClient.Get(context.Background(), types.NamespacedName{
		Name:      collectorDeploymentName(ul.Name),
		Namespace: collectorNamespace,
	}, &deploy)
	require.NoError(t, err)

	containers := deploy.Spec.Template.Spec.Containers

	t.Run("has two containers", func(t *testing.T) {
		assert.Len(t, containers, 2)
	})

	// Find the log-viewer container
	var viewerIdx int
	for i, c := range containers {
		if c.Name == LogViewerContainer {
			viewerIdx = i
			break
		}
	}
	vc := containers[viewerIdx]

	t.Run("name is log-viewer", func(t *testing.T) {
		assert.Equal(t, LogViewerContainer, vc.Name)
	})

	t.Run("uses same image as collector", func(t *testing.T) {
		assert.Equal(t, containers[0].Image, vc.Image)
	})

	t.Run("command is upgrade-toolkit", func(t *testing.T) {
		assert.Equal(t, []string{"upgrade-toolkit"}, vc.Command)
	})

	t.Run("args invoke log-viewer subcommand", func(t *testing.T) {
		assert.Equal(t, []string{"log-viewer", CollectorLogDir}, vc.Args)
	})

	t.Run("volume mount is read-only", func(t *testing.T) {
		require.Len(t, vc.VolumeMounts, 1)
		assert.Equal(t, "logs", vc.VolumeMounts[0].Name)
		assert.Equal(t, CollectorLogDir, vc.VolumeMounts[0].MountPath)
		assert.True(t, vc.VolumeMounts[0].ReadOnly)
	})

	t.Run("resources are minimal", func(t *testing.T) {
		assert.Equal(t, resource.MustParse("5m"), vc.Resources.Requests["cpu"])
		assert.Equal(t, resource.MustParse("8Mi"), vc.Resources.Requests["memory"])
		assert.Equal(t, resource.MustParse("50m"), vc.Resources.Limits["cpu"])
		assert.Equal(t, resource.MustParse("32Mi"), vc.Resources.Limits["memory"])
	})
}
