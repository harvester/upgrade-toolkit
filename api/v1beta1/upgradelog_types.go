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

package v1beta1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// UpgradeLog condition types
	UpgradeLogCollectorReady string = "CollectorReady"
)

// UpgradeLogPhase defines what overall phase the UpgradeLog is in.
type UpgradeLogPhase string

const (
	UpgradeLogPhaseCollectorDeploying UpgradeLogPhase = "CollectorDeploying"
	UpgradeLogPhaseCollectorDeployed  UpgradeLogPhase = "CollectorDeployed"
	UpgradeLogPhaseCollecting         UpgradeLogPhase = "Collecting"
	UpgradeLogPhaseStopping           UpgradeLogPhase = "Stopping"
	UpgradeLogPhaseStopped            UpgradeLogPhase = "Stopped"
	UpgradeLogPhaseFailed             UpgradeLogPhase = "Failed"
)

// PodLogState represents the state of log collection for a single pod.
type PodLogState string

const (
	PodLogStateStreaming PodLogState = "Streaming"
	PodLogStateCompleted PodLogState = "Completed"
	PodLogStateFailed    PodLogState = "Failed"
)

// UpgradeLogSpec defines the desired state of UpgradeLog.
type UpgradeLogSpec struct {
	// upgradePlanName references the UpgradePlan this log collection belongs to.
	// +required
	// +kubebuilder:validation:MinLength=1
	UpgradePlanName string `json:"upgradePlanName"`
}

// UpgradeLogStatus defines the observed state of UpgradeLog.
type UpgradeLogStatus struct {
	// currentPhase tracks the UpgradeLog phase state machine.
	CurrentPhase UpgradeLogPhase `json:"currentPhase,omitempty"`

	// conditions represent the current state of the UpgradeLog resource.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// trackedPods maps pod names to their log collection state.
	// +mapType=atomic
	// +optional
	TrackedPods map[string]PodLogStatus `json:"trackedPods,omitempty"`

	// storageUsage is the current PVC usage in bytes.
	// +optional
	StorageUsage int64 `json:"storageUsage,omitempty"`
}

// PodLogStatus tracks log collection state for a single pod.
type PodLogStatus struct {
	// component identifies the upgrade component (e.g., cluster-upgrade, node-upgrade, image-preload).
	Component string `json:"component"`
	// node is the node this pod ran on.
	// +optional
	Node string `json:"node,omitempty"`
	// state is the current log collection state for this pod.
	State PodLogState `json:"state"`
	// bytesCollected is the total bytes written for this pod.
	// +optional
	BytesCollected int64 `json:"bytesCollected,omitempty"`
	// logPath is the path within the PVC where logs are stored.
	// +optional
	LogPath string `json:"logPath,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster,shortName=ul;uls
// +kubebuilder:printcolumn:name="UPGRADE_PLAN",type="string",JSONPath=`.spec.upgradePlanName`
// +kubebuilder:printcolumn:name="PHASE",type="string",JSONPath=`.status.currentPhase`
// +kubebuilder:printcolumn:name="AGE",type="date",JSONPath=`.metadata.creationTimestamp`

// UpgradeLog is the Schema for the upgradelogs API.
type UpgradeLog struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of UpgradeLog
	// +required
	Spec UpgradeLogSpec `json:"spec"`

	// status defines the observed state of UpgradeLog
	// +optional
	Status UpgradeLogStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// UpgradeLogList contains a list of UpgradeLog.
type UpgradeLogList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []UpgradeLog `json:"items"`
}

func init() {
	SchemeBuilder.Register(&UpgradeLog{}, &UpgradeLogList{})
}

func (u *UpgradeLog) SetCondition(conditionType string, conditionStatus metav1.ConditionStatus, reason string, message string) {
	for i := range u.Status.Conditions {
		if u.Status.Conditions[i].Type == conditionType {
			u.Status.Conditions[i].Status = conditionStatus
			u.Status.Conditions[i].Reason = reason
			u.Status.Conditions[i].Message = message
			u.Status.Conditions[i].LastTransitionTime = metav1.Now()
			u.Status.Conditions[i].ObservedGeneration = u.Generation
			return
		}
	}
	u.Status.Conditions = append(u.Status.Conditions, metav1.Condition{
		Type:               conditionType,
		Status:             conditionStatus,
		LastTransitionTime: metav1.Now(),
		Reason:             reason,
		Message:            message,
		ObservedGeneration: u.Generation,
	})
}

func (u *UpgradeLog) ConditionTrue(conditionType string) bool {
	for _, v := range u.Status.Conditions {
		if v.Type == conditionType {
			return v.Status == metav1.ConditionTrue
		}
	}
	return false
}

// ObjectReference returns a corev1.ObjectReference for this UpgradeLog
// suitable for use with an EventRecorder.
func (u *UpgradeLog) ObjectReference() *corev1.ObjectReference {
	return &corev1.ObjectReference{
		Kind:            "UpgradeLog",
		APIVersion:      GroupVersion.String(),
		Name:            u.Name,
		UID:             u.UID,
		ResourceVersion: u.ResourceVersion,
	}
}
