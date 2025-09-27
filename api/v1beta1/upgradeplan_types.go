/*
Copyright 2025.

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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

const (
	// Available means the UpgradePlan is ready to be reconciled.
	UpgradePlanAvailable string = "Available"
	// Progressing means the cluster is currently applying the UpgradePlan.
	UpgradePlanProgressing string = "Progressing"
	// Degraded means the progress of the upgrade is stalled due to issues.
	UpgradePlanDegraded string = "Degraded"
)

const (
	NodeStateImagePreloading     string = "ImagePreloading"
	NodeStateImagePreloaded      string = "ImagePreloaded"
	NodeStateKubernetesUpgrading string = "KubernetesUpgrading"
	NodeStateKubernetesUpgraded  string = "KubernetesUpgraded"
	NodeStateOSUpgrading         string = "OSUpgrading"
	NodeStateOSUpgraded          string = "OSUpgraded"
)

const (
	UpgradePlanPhaseInit               UpgradePlanPhase = "Init"
	UpgradePlanPhaseISODownloading     UpgradePlanPhase = "ISODownloading"
	UpgradePlanPhaseISODownloaded      UpgradePlanPhase = "ISODownloaded"
	UpgradePlanPhaseRepoCreating       UpgradePlanPhase = "RepoCreating"
	UpgradePlanPhaseRepoCreated        UpgradePlanPhase = "RepoCreated"
	UpgradePlanPhaseMetadataPopulating UpgradePlanPhase = "MetadataPopulating"
	UpgradePlanPhaseMetadataPopulated  UpgradePlanPhase = "MetadataPopulated"
	UpgradePlanPhaseImagePreloading    UpgradePlanPhase = "ImagePreloading"
	UpgradePlanPhaseImagePreloaded     UpgradePlanPhase = "ImagePreloaded"
	UpgradePlanPhaseClusterUpgrading   UpgradePlanPhase = "ClusterUpgrading"
	UpgradePlanPhaseClusterUpgraded    UpgradePlanPhase = "ClusterUpgraded"
	UpgradePlanPhaseNodeUpgrading      UpgradePlanPhase = "NodeUpgrading"
	UpgradePlanPhaseNodeUpgraded       UpgradePlanPhase = "NodeUpgraded"
	UpgradePlanPhaseCleaningUp         UpgradePlanPhase = "CleaningUp"
	UpgradePlanPhaseCleanedUp          UpgradePlanPhase = "CleanedUp"
	UpgradePlanPhaseSucceeded          UpgradePlanPhase = "Succeeded"
	UpgradePlanPhaseFailed             UpgradePlanPhase = "Failed"
)

type NodeUpgradeStatus struct {
	State   string `json:"state,omitempty"`
	Reason  string `json:"reason,omitempty"`
	Message string `json:"message,omitempty"`
}

// UpgradePlanPhase defines what overall phase UpgradePlan is in
type UpgradePlanPhase string

type UpgradePlanPhaseTransitionTimestamp struct {
	Phase                    UpgradePlanPhase `json:"phase"`
	PhaseTransitionTimestamp metav1.Time      `json:"phaseTransitionTimestamp"`
}

type ReleaseMetadata struct {
	Harvester            string `json:"harvester,omitempty"`
	HarvesterChart       string `json:"harvesterChart,omitempty"`
	OS                   string `json:"os,omitempty"`
	Kubernetes           string `json:"kubernetes,omitempty"`
	Rancher              string `json:"rancher,omitempty"`
	MonitoringChart      string `json:"monitoringChart,omitempty"`
	MinUpgradableVersion string `json:"minUpgradableVersion,omitempty"`
}

// UpgradePlanSpec defines the desired state of UpgradePlan
type UpgradePlanSpec struct {
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// Important: Run "make" to regenerate code after modifying this file
	// The following markers will use OpenAPI v3 schema to validate the value
	// More info: https://book.kubebuilder.io/reference/markers/crd-validation.html

	// version refers to the corresponding version resource in the same namespace.
	// +required
	Version string `json:"version"`

	// upgrade can be specified to opt for any other specific upgrade image. If not provided, the version resource name is used.
	// For instance, specifying "dev" for the field can go for the "rancher/harvester-upgrade:dev" image.
	// +optional
	Upgrade *string `json:"upgrade,omitempty"`

	// force indicates the UpgradePlan will be forcibly applied, ignoring any pre-upgrade check failures. Default to "false".
	// +optional
	Force *bool `json:"force,omitempty"`

	// mode represents the manipulative style of the UpgradePlan. Can be either of "automatic" or "interactive". Default to "automatic".
	// +optional
	Mode *string `json:"mode,omitempty"`
}

// UpgradePlanStatus defines the observed state of UpgradePlan.
type UpgradePlanStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// For Kubernetes API conventions, see:
	// https://github.com/kubernetes/community/blob/master/contributors/devel/sig-architecture/api-conventions.md#typical-status-properties

	// conditions represent the current state of the UpgradePlan resource.
	// Each condition has a unique type and reflects the status of a specific aspect of the resource.
	//
	// Standard condition types include:
	// - "Available": the resource is fully functional
	// - "Progressing": the resource is being created or updated
	// - "Degraded": the resource failed to reach or maintain its desired state
	//
	// The status of each condition is one of True, False, or Unknown.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// isoImageID refers to the namespaced name of the VM image that will be used for the upgrade.
	// +optional
	ISOImageID *string `json:"isoImageID,omitempty"`

	// nodeStatuses reflect each node's upgrade status for node specific tasks.
	// +mapType=atomic
	// +optional
	NodeUpgradeStatuses map[string]NodeUpgradeStatus `json:"nodeUpgradeStatuses,omitempty"`

	// phase shows what overall phase the UpgradePlan resource is in.
	Phase UpgradePlanPhase `json:"phase,omitempty"`

	// phaseTransitionTimestamp is the timestamp of when the last phase change occurred.
	// +listType=atomic
	// +optional
	PhaseTransitionTimestamps []UpgradePlanPhaseTransitionTimestamp `json:"phaseTransitionTimestamps,omitempty"`

	// previousVersion is the Harvester version before upgrade.
	// +optional
	PreviousVersion *string `json:"previousVersion,omitempty"`

	// releaseMetadata reflects the essential metadata extracted from the artifact.
	// +optional
	ReleaseMetadata *ReleaseMetadata `json:"releaseMetadata,omitempty"`

	// version is the snapshot of the associated Version resource
	// +optional
	Version *VersionSpec `json:"version,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster,shortName=up;ups
// +kubebuilder:printcolumn:name="VERSION",type="string",JSONPath=`.spec.version`
// +kubebuilder:printcolumn:name="PHASE",type="string",JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="AVAILABLE",type=string,JSONPath=`.status.conditions[?(@.type=='Available')].status`
// +kubebuilder:printcolumn:name="PROGRESSING",type=string,JSONPath=`.status.conditions[?(@.type=='Progressing')].status`
// +kubebuilder:printcolumn:name="DEGRADED",type=string,JSONPath=`.status.conditions[?(@.type=='Degraded')].status`

// UpgradePlan is the Schema for the upgradeplans API
type UpgradePlan struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty,omitzero"`

	// spec defines the desired state of UpgradePlan
	// +required
	Spec UpgradePlanSpec `json:"spec"`

	// status defines the observed state of UpgradePlan
	// +optional
	Status UpgradePlanStatus `json:"status,omitempty,omitzero"`
}

// +kubebuilder:object:root=true

// UpgradePlanList contains a list of UpgradePlan
type UpgradePlanList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []UpgradePlan `json:"items"`
}

func init() {
	SchemeBuilder.Register(&UpgradePlan{}, &UpgradePlanList{})
}

func (u *UpgradePlan) SetCondition(conditionType string, conditionStatus metav1.ConditionStatus, reason string, message string) {
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

func (u *UpgradePlan) ConditionExists(conditionType string) bool {
	for _, v := range u.Status.Conditions {
		if v.Type == conditionType {
			return true
		}
	}
	return false
}

func (u *UpgradePlan) LookupCondition(conditionType string) metav1.Condition {
	for _, v := range u.Status.Conditions {
		if v.Type == conditionType {
			return v
		}
	}
	return metav1.Condition{}
}

func (u *UpgradePlan) ConditionTrue(conditionType string) bool {
	condition := u.LookupCondition(conditionType)
	return condition.Status == metav1.ConditionTrue
}

func (u *UpgradePlan) ConditionFalse(conditionType string) bool {
	condition := u.LookupCondition(conditionType)
	return condition.Status == metav1.ConditionFalse
}

func (u *UpgradePlan) ConditionUnknown(conditionType string) bool {
	condition := u.LookupCondition(conditionType)
	return condition.Status == metav1.ConditionUnknown
}
