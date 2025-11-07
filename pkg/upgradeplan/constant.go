package upgradeplan

const (
	LabelPrefix               = "management.harvesterhci.io"
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
