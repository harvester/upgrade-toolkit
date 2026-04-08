package upgradelog

import "time"

const (
	RequeueAfterDuration = 1 * time.Second

	// Labels and annotations
	AnnotationPrefix = "management.harvesterhci.io"
	LabelPrefix      = AnnotationPrefix

	// Label applied to collector resources owned by an UpgradeLog
	UpgradeLogLabel = LabelPrefix + "/" + "upgrade-log"

	// Collector component names
	CollectorComponent  = "log-collector"
	CollectorPortName   = "grpc"
	CollectorPort       = 9500
	CollectorLogDir     = "/logs"
	CollectorPVCSize    = "5Gi"
	CollectorReplicas   = 1
	CollectorImage      = "rancher/harvester-upgrade-toolkit"
	LogShipperContainer = "log-shipper"

	// Shared volume for log tee between main container and sidecar
	SharedLogVolumeName = "upgrade-log-shared"
	SharedLogMountPath  = "/upgrade-log-shared"
	SharedLogFileName   = "output.log"

	// Finalizer for UpgradeLog cleanup on deletion
	UpgradeLogFinalizer = AnnotationPrefix + "/" + "upgradelog-cleanup"
)
