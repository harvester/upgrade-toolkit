package upgradeplan

import "time"

const (
	RequeueAfterDuration = 1 * time.Second
	HttpGetRetryInterval = 10 * time.Second

	HarvesterSystemNamespace = harvesterSystemNamespace
	LonghornSystemNamespace  = "longhorn-system"

	AnnotationPrefix         = "management.harvesterhci.io"
	LabelPrefix              = AnnotationPrefix
	UpstreamAnnotationPrefix = "harvesterhci.io"
	SUCLabelPrefix           = "upgrade.cattle.io"

	// Longhorn replica replenishment wait interval management
	LonghornSettingReplicaReplenishment          = "replica-replenishment-wait-interval"
	AnnotationReplicaReplenishmentOriginal       = AnnotationPrefix + "/replica-replenishment-wait-interval"
	ExtendedReplicaReplenishmentWaitInterval int = 1800

	// Descheduler addon management during node upgrade
	DeschedulerAddonName            = "descheduler"
	DeschedulerAddonNamespace       = "kube-system"
	AnnotationDeschedulerWasEnabled = AnnotationPrefix + "/descheduler-was-enabled"

	HarvesterUpgradeImageAnnotation = UpstreamAnnotationPrefix + "/" + "os-upgrade-image"

	HarvesterUpgradePlanLabel = LabelPrefix + "/" + "upgrade-plan"

	HarvesterUpgradeComponentLabel = LabelPrefix + "/" + "upgrade-component"
	PrepareComponent               = "image-preload"
	ClusterComponent               = "cluster-upgrade"
	NodeComponent                  = "node-upgrade"
	ImageCleanupComponent          = "image-cleanup"

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

	// Labels for node-upgrade Jobs
	HarvesterUpgradeNodeLabel = LabelPrefix + "/" + "node"
	HarvesterJobTypeLabel     = LabelPrefix + "/" + "job-type"
	JobTypePreDrain           = "pre-drain"
	JobTypePostDrain          = "post-drain"
	JobTypeSingleNodeUpgrade  = "single-node-upgrade"

	// Finalizer for UpgradePlan cleanup on deletion
	UpgradePlanFinalizer = AnnotationPrefix + "/" + "upgradeplan-cleanup"

	// Annotation to skip webhook validation entirely
	AnnotationSkipWebhook = UpstreamAnnotationPrefix + "/" + "skipWebhook"

	// Annotation to skip single-replica detached volume check
	AnnotationSkipSingleReplicaDetachedVol = UpstreamAnnotationPrefix + "/" + "skipSingleReplicaDetachedVol"

	// Annotation to skip garbage collection threshold check during upgrade
	AnnotationSkipGCThresholdCheck = UpstreamAnnotationPrefix + "/" + "skipGarbageCollectionThresholdCheck"

	// Annotation to set minimum certificate expiration days before upgrade
	AnnotationMinCertsExpirationInDay = UpstreamAnnotationPrefix + "/" + "minCertsExpirationInDay"

	// RKE2 internal IP annotation on nodes
	RKE2InternalIPAnnotation = "rke2.io/internal-ip"

	// Machine-plan Secret
	MachinePlanSecretType   = "rke.cattle.io/machine-plan"
	MachinePlanMachineLabel = "rke.cattle.io/machine-name"
)
