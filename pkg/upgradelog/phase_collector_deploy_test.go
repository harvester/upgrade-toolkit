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
	"fmt"
	"testing"

	managementv1beta1 "github.com/harvester/upgrade-toolkit/api/v1beta1"
	buildversion "github.com/harvester/upgrade-toolkit/pkg/version"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
