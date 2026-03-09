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

package preflight

import (
	"context"
	"testing"

	harvesterv1beta1 "github.com/harvester/harvester/pkg/apis/harvesterhci.io/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

func init() {
	logf.SetLogger(zap.New(zap.UseDevMode(true)))
}

func TestCheckMinimumVersion(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := harvesterv1beta1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add harvester scheme: %v", err)
	}

	logger := logf.Log.WithName("test")

	tests := []struct {
		name      string
		setting   *harvesterv1beta1.Setting
		wantError bool
	}{
		{
			name:      "setting not found",
			setting:   nil,
			wantError: false,
		},
		{
			name: "empty value",
			setting: &harvesterv1beta1.Setting{
				ObjectMeta: metav1.ObjectMeta{Name: serverVersionSettingName},
				Value:      "",
			},
			wantError: false,
		},
		{
			name: "version equal to minimum",
			setting: &harvesterv1beta1.Setting{
				ObjectMeta: metav1.ObjectMeta{Name: serverVersionSettingName},
				Value:      "v1.7.1",
			},
			wantError: false,
		},
		{
			name: "version newer than minimum",
			setting: &harvesterv1beta1.Setting{
				ObjectMeta: metav1.ObjectMeta{Name: serverVersionSettingName},
				Value:      "v1.8.0",
			},
			wantError: false,
		},
		{
			name: "patch version newer than minimum",
			setting: &harvesterv1beta1.Setting{
				ObjectMeta: metav1.ObjectMeta{Name: serverVersionSettingName},
				Value:      "v1.7.2",
			},
			wantError: false,
		},
		{
			name: "version older than minimum",
			setting: &harvesterv1beta1.Setting{
				ObjectMeta: metav1.ObjectMeta{Name: serverVersionSettingName},
				Value:      "v1.7.0",
			},
			wantError: true,
		},
		{
			name: "version much older than minimum",
			setting: &harvesterv1beta1.Setting{
				ObjectMeta: metav1.ObjectMeta{Name: serverVersionSettingName},
				Value:      "v1.6.0",
			},
			wantError: true,
		},
		{
			name: "pre-release of minimum version is older",
			setting: &harvesterv1beta1.Setting{
				ObjectMeta: metav1.ObjectMeta{Name: serverVersionSettingName},
				Value:      "v1.7.1-rc1",
			},
			wantError: true,
		},
		{
			name: "dev version allowed through",
			setting: &harvesterv1beta1.Setting{
				ObjectMeta: metav1.ObjectMeta{Name: serverVersionSettingName},
				Value:      "v1.7-head",
			},
			wantError: false,
		},
		{
			name: "commit hash dev version allowed through",
			setting: &harvesterv1beta1.Setting{
				ObjectMeta: metav1.ObjectMeta{Name: serverVersionSettingName},
				Value:      "f024f49a",
			},
			wantError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := fake.NewClientBuilder().WithScheme(scheme)
			if tt.setting != nil {
				builder = builder.WithObjects(tt.setting)
			}
			reader := builder.Build()

			err := CheckMinimumVersion(context.Background(), reader, logger)
			if tt.wantError && err == nil {
				t.Errorf("expected error but got nil")
			}
			if !tt.wantError && err != nil {
				t.Errorf("expected no error but got: %v", err)
			}
		})
	}
}
