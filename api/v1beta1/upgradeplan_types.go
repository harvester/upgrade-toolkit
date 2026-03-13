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
	corev1 "k8s.io/api/core/v1"
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

// NodeUpgradeState represents a node's position in the upgrade lifecycle.
type NodeUpgradeState string

const (
	// Image-preload lifecycle states (SUC)
	NodeStateImagePreloading    NodeUpgradeState = "ImagePreloading"
	NodeStateImagePreloaded     NodeUpgradeState = "ImagePreloaded"
	NodeStateImagePreloadFailed NodeUpgradeState = "ImagePreloadFailed"

	// Drain-hook lifecycle states (Rancher V2 Provisioning)
	NodeStatePreDraining     NodeUpgradeState = "PreDraining"
	NodeStatePreDrained      NodeUpgradeState = "PreDrained"
	NodeStatePreDrainFailed  NodeUpgradeState = "PreDrainFailed"
	NodeStatePostDraining    NodeUpgradeState = "PostDraining"
	NodeStateWaitingReboot   NodeUpgradeState = "WaitingReboot"
	NodeStatePostDrained     NodeUpgradeState = "PostDrained"
	NodeStatePostDrainFailed NodeUpgradeState = "PostDrainFailed"

	// Pause state: node is administratively paused before pre-drain
	NodeStateUpgradePaused NodeUpgradeState = "UpgradePaused"

	// Single-node upgrade lifecycle states
	NodeStateSingleNodeUpgrading     NodeUpgradeState = "SingleNodeUpgrading"
	NodeStateSingleNodeUpgradeFailed NodeUpgradeState = "SingleNodeUpgradeFailed"
	NodeStateSingleNodeUpgraded      NodeUpgradeState = "SingleNodeUpgraded"

	// Image-cleanup lifecycle states (SUC)
	NodeStateImageCleaning    NodeUpgradeState = "ImageCleaning"
	NodeStateImageCleaned     NodeUpgradeState = "ImageCleaned"
	NodeStateImageCleanFailed NodeUpgradeState = "ImageCleanFailed"
)

// nodeUpgradeStateGroups defines the forward-progress ordering of node upgrade states.
// States within the same group share the same ordinal (e.g., success and failure at a stage).
var nodeUpgradeStateGroups = [][]NodeUpgradeState{
	{NodeStateImagePreloading},                             // 0
	{NodeStateImagePreloaded, NodeStateImagePreloadFailed}, // 1
	{NodeStateUpgradePaused},                               // 2
	{NodeStatePreDraining, NodeStateSingleNodeUpgrading},   // 3
	{NodeStatePreDrained, NodeStatePreDrainFailed},         // 4
	{NodeStatePostDraining},                                // 5
	{NodeStateWaitingReboot},                               // 6
	{NodeStatePostDrained, NodeStatePostDrainFailed, NodeStateSingleNodeUpgraded, NodeStateSingleNodeUpgradeFailed}, // 7
	{NodeStateImageCleaning},                           // 8
	{NodeStateImageCleaned, NodeStateImageCleanFailed}, // 9
}

var nodeUpgradeStateIndex map[NodeUpgradeState]int

func init() {
	nodeUpgradeStateIndex = make(map[NodeUpgradeState]int, len(nodeUpgradeStateGroups)*2)
	for i, group := range nodeUpgradeStateGroups {
		for _, state := range group {
			nodeUpgradeStateIndex[state] = i
		}
	}
}

// IsNodeUpgradeStateAhead reports whether current is strictly ahead of proposed
// in the node upgrade lifecycle.
func IsNodeUpgradeStateAhead(current, proposed NodeUpgradeState) bool {
	return nodeUpgradeStateIndex[current] > nodeUpgradeStateIndex[proposed]
}

const (
	// Overall UpgradePlan phases
	UpgradePlanPhaseInitializing       UpgradePlanPhase = "Initializing"
	UpgradePlanPhaseInitialized        UpgradePlanPhase = "Initialized"
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

	UpgradePlanPhaseSucceeded UpgradePlanPhase = "Succeeded"
	UpgradePlanPhaseFailed    UpgradePlanPhase = "Failed"
)

type NodeUpgradeStatus struct {
	State   NodeUpgradeState `json:"state,omitempty"`
	Reason  string           `json:"reason,omitempty"`
	Message string           `json:"message,omitempty"`
}

// ImagePreloadOption configures image preload behavior during the ImagePreloading phase.
type ImagePreloadOption struct {
	// concurrency controls how many nodes preload images simultaneously.
	// When nil or zero, all Harvester-managed nodes preload concurrently.
	// When positive, the value is used as the SUC plan concurrency (capped at node count).
	// When negative, image preloading is skipped entirely.
	// +optional
	Concurrency *int `json:"concurrency,omitempty"`
}

// NodeUpgradeOption configures node upgrade behavior during the NodeUpgrading phase.
type NodeUpgradeOption struct {
	// pauseNodes lists specific node names to pause before PreDraining.
	// When non-empty, only the listed nodes are paused; all other nodes proceed
	// automatically. When empty (or when nodeUpgradeOption is nil), no nodes are
	// paused. Removing a node from this list unpauses it. To pause all nodes,
	// list every node name explicitly.
	// +listType=set
	// +optional
	// +kubebuilder:validation:items:MinLength=1
	PauseNodes []string `json:"pauseNodes,omitempty"`
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
// +kubebuilder:validation:XValidation:rule="has(self.upgrade) == has(oldSelf.upgrade) && (!has(self.upgrade) || self.upgrade == oldSelf.upgrade)",message="spec.upgrade is immutable after creation"
// +kubebuilder:validation:XValidation:rule="has(self.image) == has(oldSelf.image) && (!has(self.image) || self.image == oldSelf.image)",message="spec.image is immutable after creation"
// +kubebuilder:validation:XValidation:rule="has(self.version) == has(oldSelf.version) && (!has(self.version) || self.version == oldSelf.version)",message="spec.version is immutable after creation"
// +kubebuilder:validation:XValidation:rule="has(self.version) || has(self.image)",message="at least one of spec.version or spec.image must be set"
type UpgradePlanSpec struct {
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// Important: Run "make" to regenerate code after modifying this file
	// The following markers will use OpenAPI v3 schema to validate the value
	// More info: https://book.kubebuilder.io/reference/markers/crd-validation.html

	// version refers to the corresponding version resource.
	// When spec.image is set, this field is optional and no Version CR lookup is performed.
	// +optional
	// +kubebuilder:validation:MinLength=1
	Version *string `json:"version,omitempty"`

	// upgrade can be specified to opt for any other specific upgrade image. If not provided, the version resource name is used.
	// For instance, specifying "dev" for the field can go for the "rancher/harvester-upgrade:dev" image.
	// +optional
	Upgrade *string `json:"upgrade,omitempty"`

	// image references the name of a pre-uploaded VirtualMachineImage in the
	// harvester-system namespace to use as the upgrade ISO. When set, the
	// ISODownloading phase uses this existing VMImage instead of downloading a new one.
	// +optional
	Image *string `json:"image,omitempty"`

	// force indicates the UpgradePlan will be forcibly applied, ignoring any pre-upgrade check failures. Default to "false".
	// +optional
	Force *bool `json:"force,omitempty"`

	// imagePreloadOption configures image preload behavior including concurrency control.
	// +optional
	ImagePreloadOption *ImagePreloadOption `json:"imagePreloadOption,omitempty"`

	// nodeUpgradeOption configures node upgrade behavior including pause/unpause control.
	// +optional
	NodeUpgradeOption *NodeUpgradeOption `json:"nodeUpgradeOption,omitempty"`

	// restoreVM enables automatic restoration of non-live-migratable VMs
	// that were shut down during node upgrade. Default to false.
	// +optional
	RestoreVM *bool `json:"restoreVM,omitempty"`
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

	// isoImageID refers to the name of the VM image in the harvester-system
	// namespace that will be used for the upgrade.
	// +optional
	ISOImageID *string `json:"isoImageID,omitempty"`

	// nodeStatuses reflect each node's upgrade status for node specific tasks.
	// +mapType=atomic
	// +optional
	NodeUpgradeStatuses map[string]NodeUpgradeStatus `json:"nodeUpgradeStatuses,omitempty"`

	// currentPhase shows what overall phase the UpgradePlan resource is in.
	CurrentPhase UpgradePlanPhase `json:"currentPhase,omitempty"`

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

	// provisionGeneration records the provisionGeneration value set on the
	// Cluster resource during the NodeUpgrade phase. Used as an idempotency
	// guard so the Cluster is patched exactly once per upgrade.
	// +optional
	ProvisionGeneration *int `json:"provisionGeneration,omitempty"`

	// singleNode records the name of the single node in a single-node cluster.
	// Empty for multi-node clusters. Set during the Initialize phase.
	// +optional
	SingleNode *string `json:"singleNode,omitempty"`

	// version is the snapshot of the associated Version resource.
	// +optional
	Version *VersionSnapshot `json:"version,omitempty"`
}

// VersionSnapshot captures the fields from an upstream harvesterhci.io Version
// resource that are needed during the upgrade lifecycle.
type VersionSnapshot struct {
	// isoURL is the URL to download the ISO from.
	ISOURL string `json:"isoURL"`
	// isoChecksum is the checksum of the ISO.
	// +optional
	ISOChecksum string `json:"isoChecksum,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster,shortName=up;ups
// +kubebuilder:printcolumn:name="VERSION",type="string",JSONPath=`.spec.version`
// +kubebuilder:printcolumn:name="CURRENTPHASE",type="string",JSONPath=`.status.currentPhase`
// +kubebuilder:printcolumn:name="AVAILABLE",type="string",JSONPath=`.status.conditions[?(@.type=='Available')].status`
// +kubebuilder:printcolumn:name="PROGRESSING",type="string",JSONPath=`.status.conditions[?(@.type=='Progressing')].status`
// +kubebuilder:printcolumn:name="DEGRADED",type="string",JSONPath=`.status.conditions[?(@.type=='Degraded')].status`
// +kubebuilder:printcolumn:name="AGE",type="date",JSONPath=`.metadata.creationTimestamp`

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

// ObjectReference returns a corev1.ObjectReference for this UpgradePlan
// suitable for use with an EventRecorder. Because UpgradePlan is
// cluster-scoped, the returned reference has an empty Namespace. The
// client-go event recorder automatically places such events in the
// "default" namespace while preserving the empty involvedObject.namespace,
// which allows kubectl describe to associate them with the CR.
func (u *UpgradePlan) ObjectReference() *corev1.ObjectReference {
	return &corev1.ObjectReference{
		Kind:            "UpgradePlan",
		APIVersion:      GroupVersion.String(),
		Name:            u.Name,
		UID:             u.UID,
		ResourceVersion: u.ResourceVersion,
	}
}
