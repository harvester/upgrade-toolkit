package upgradeplan

const (
	AnnotationPrefix = "management.harvesterhci.io"
	LabelPrefix      = AnnotationPrefix
	// TODO: This is to cooperate with the upstream implementation. It should be removed eventually.
	UpstreamAnnotationPrefix = "harvesterhci.io"
	SUCLabelPrefix           = "upgrade.cattle.io"

	HarvesterUpgradeImageAnnotation = UpstreamAnnotationPrefix + "/" + "os-upgrade-image"

	HarvesterUpgradePlanLabel = LabelPrefix + "/" + "upgrade-plan"

	HarvesterUpgradeComponentLabel = LabelPrefix + "/" + "upgrade-component"
	PrepareComponent               = "image-preload"
	ClusterComponent               = "cluster-upgrade"
	NodeComponent                  = "node-upgrade"

	// Label on image-preload Jobs set by SUC
	SUCNodeLabel = SUCLabelPrefix + "/" + "node"

	// Rancher V2 Provisioning
	FleetLocalNamespace = "fleet-local"
	LocalClusterName    = "local"

	// Annotations on machine-plan Secrets set by Rancher
	RKE2PreDrainAnnotation  = "rke.cattle.io/pre-drain"
	RKE2PostDrainAnnotation = "rke.cattle.io/post-drain"

	// Annotations set by us to signal hook completion to Rancher
	PreHookAnnotation  = AnnotationPrefix + "/" + "pre-hook"
	PostHookAnnotation = AnnotationPrefix + "/" + "post-hook"

	// Annotation on Node objects to track expected OS version during reboot
	PendingOSImageAnnotation = AnnotationPrefix + "/" + "pendingOSImage"

	// Labels for drain-hook Jobs
	HarvesterUpgradeNodeLabel   = LabelPrefix + "/" + "node"
	HarvesterDrainHookTypeLabel = LabelPrefix + "/" + "drain-hook-type"
	DrainHookTypePreDrain       = "pre-drain"
	DrainHookTypePostDrain      = "post-drain"

	// Machine-plan Secret
	MachinePlanSecretType   = "rke.cattle.io/machine-plan"
	MachinePlanMachineLabel = "rke.cattle.io/machine-name"
)
