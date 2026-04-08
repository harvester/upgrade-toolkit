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

package v1

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"github.com/harvester/upgrade-toolkit/pkg/upgradelog"
)

var podlog = logf.Log.WithName("pod-resource")

// SetupPodWebhookWithManager registers the webhook for Pod in the manager.
func SetupPodWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr).
		For(&corev1.Pod{}).
		WithDefaulter(&PodCustomDefaulter{}).
		Complete()
}

// +kubebuilder:webhook:path=/mutate--v1-pod,mutating=true,failurePolicy=ignore,sideEffects=None,groups="",resources=pods,verbs=create,versions=v1,name=mpod-v1.kb.io,admissionReviewVersions=v1

// PodCustomDefaulter injects a log-shipper native sidecar into upgrade-labeled pods.
//
// NOTE: The +kubebuilder:object:generate=false marker prevents controller-gen from generating DeepCopy methods,
// as it is used only for temporary operations and does not need to be deeply copied.
type PodCustomDefaulter struct{}

// Default implements webhook.CustomDefaulter so a webhook will be registered for the Kind Pod.
func (d *PodCustomDefaulter) Default(_ context.Context, obj runtime.Object) error {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return fmt.Errorf("expected a Pod object but got %T", obj)
	}

	// Only mutate pods with the upgrade-plan label
	upgradePlanName, hasLabel := pod.Labels[upgradePlanLabel]
	if !hasLabel {
		return nil
	}

	// Skip if already injected
	for _, c := range pod.Spec.InitContainers {
		if c.Name == upgradelog.LogShipperContainer {
			return nil
		}
	}

	podlog.Info("injecting log-shipper sidecar", "pod", pod.Name, "upgradePlan", upgradePlanName)

	component := pod.Labels[upgradeComponentLabel]
	upgradeLogName := upgradePlanName // UpgradeLog is named after the UpgradePlan
	collectorEndpoint := upgradelog.CollectorServiceEndpoint(upgradeLogName)

	// Determine the image from the first container (same upgrade-toolkit image)
	image := ""
	if len(pod.Spec.Containers) > 0 {
		image = pod.Spec.Containers[0].Image
	}

	// 1. Add shared emptyDir volume
	pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{
		Name: upgradelog.SharedLogVolumeName,
		VolumeSource: corev1.VolumeSource{
			EmptyDir: &corev1.EmptyDirVolumeSource{},
		},
	})

	// 2. Wrap main container command with tee
	for i := range pod.Spec.Containers {
		c := &pod.Spec.Containers[i]
		if len(c.Command) > 0 {
			originalCmd := buildOriginalCommand(c.Command, c.Args)
			c.Command = []string{"/bin/sh", "-c"}
			c.Args = []string{
				fmt.Sprintf("%s 2>&1 | tee %s/%s; exit ${PIPESTATUS[0]}",
					originalCmd,
					upgradelog.SharedLogMountPath,
					upgradelog.SharedLogFileName,
				),
			}
		}
		c.VolumeMounts = append(c.VolumeMounts, corev1.VolumeMount{
			Name:      upgradelog.SharedLogVolumeName,
			MountPath: upgradelog.SharedLogMountPath,
		})
	}

	// 3. Inject native sidecar (init container with restartPolicy: Always)
	sidecar := corev1.Container{
		Name:          upgradelog.LogShipperContainer,
		RestartPolicy: ptr.To(corev1.ContainerRestartPolicyAlways),
		Image:         image,
		Command:       []string{"upgrade-toolkit", "log-shipper"},
		Args: []string{
			fmt.Sprintf("--log-dir=%s", upgradelog.SharedLogMountPath),
			fmt.Sprintf("--collector-endpoint=%s", collectorEndpoint),
		},
		Env: []corev1.EnvVar{
			{
				Name: "POD_NAME",
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"},
				},
			},
			{
				Name: "POD_NAMESPACE",
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"},
				},
			},
			{
				Name: "NODE_NAME",
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{FieldPath: "spec.nodeName"},
				},
			},
			{
				Name:  "COMPONENT",
				Value: component,
			},
		},
		VolumeMounts: []corev1.VolumeMount{
			{
				Name:      upgradelog.SharedLogVolumeName,
				MountPath: upgradelog.SharedLogMountPath,
			},
		},
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("10m"),
				corev1.ResourceMemory: resource.MustParse("16Mi"),
			},
			Limits: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("100m"),
				corev1.ResourceMemory: resource.MustParse("32Mi"),
			},
		},
	}

	// Prepend the sidecar to initContainers (native sidecars must start before main)
	pod.Spec.InitContainers = append([]corev1.Container{sidecar}, pod.Spec.InitContainers...)

	return nil
}

// PodCustomValidator is a no-op validator for Pod resources.
type PodCustomValidator struct{}

func (v *PodCustomValidator) ValidateCreate(_ context.Context, _ runtime.Object) (admission.Warnings, error) {
	return nil, nil
}

func (v *PodCustomValidator) ValidateUpdate(_ context.Context, _, _ runtime.Object) (admission.Warnings, error) {
	return nil, nil
}

func (v *PodCustomValidator) ValidateDelete(_ context.Context, _ runtime.Object) (admission.Warnings, error) {
	return nil, nil
}

const (
	upgradePlanLabel      = "management.harvesterhci.io/upgrade-plan"
	upgradeComponentLabel = "management.harvesterhci.io/upgrade-component"
)

// buildOriginalCommand reconstructs the original command string from command and args slices.
func buildOriginalCommand(command, args []string) string {
	parts := make([]string, 0, len(command)+len(args))
	parts = append(parts, command...)
	parts = append(parts, args...)
	return strings.Join(parts, " ")
}
