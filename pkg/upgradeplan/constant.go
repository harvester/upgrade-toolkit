package upgradeplan

const (
	AnnotationPrefix = "management.harvesterhci.io"
	LabelPrefix      = AnnotationPrefix
	// TODO: This is to cooperate with the upstream implementation. It should be removed eventually.
	UpstreamAnnotationPrefix = "harvesterhci.io"

	HarvesterUpgradeImageAnnotation = UpstreamAnnotationPrefix + "/" + "os-upgrade-image"

	HarvesterUpgradePlanLabel = LabelPrefix + "/" + "upgrade-plan"

	HarvesterUpgradeComponentLabel = LabelPrefix + "/" + "upgrade-component"
	PrepareComponent               = "image-preload"
	ClusterComponent               = "cluster-upgrade"
	NodeComponent                  = "node-upgrade"

	KubernetesUpgradeState = "k8s"
	OSUpgradeState         = "os"

	HarvesterNodeUpgradeTypeLabel = LabelPrefix + "/" + "node-upgrade-type"
	NodeUpgradeTypeKubernetes     = KubernetesUpgradeState
	NodeUpgradeTypeOS             = OSUpgradeState
)
