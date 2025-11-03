# Harvester Upgrade Toolkit

Upgrade Toolkit is the central part of the Harvester upgrade solution. The entire Upgrade V2 enhancement includes

- Upgrade Shim: embedded in the Harvester controller manager
- Upgrade Toolkit
  - Upgrade Repo
  - Upgrade Manager
  - Helper scripts

## How to Initiate an Upgrade

1. When the time comes, a yellow "Upgrade" button will show up on the top right corner of the Harvester UI dashboard
1. Click the "Upgrade" button
1. Tweak upgrade specific options on the pop-up upgrade dialog and then click the "Upgrade" button to start upgrading the cluster
1. Check the upgrade progress on the upgrade modal by clicking the green circle on the top right corner of the Harvester UI dashboard
1. Upgrade finishes

## Overall Workflow

1. The user creates a Version CR
1. The user creates the corresponding UpgradePlan CR or click the "Upgrade" button on the Harvester UI dashboard
1. Upgrade Shim downloads the ISO image from remote
1. Upgrade Shim deploys Upgrade Toolkit which includes Upgrade Repo and Manager
1. Upgrade Repo downloads the ISO image from internal
1. Upgrade Repo preloads all the container images
1. Upgrade Repo transitions to ready
1. Upgrade Manager transitions to ready
1. Upgrade Manager upgrades cluster components
1. Upgrade Manager upgrades node components
1. Upgrade Manager cleans up resources
1. Upgrade Manager marks the Upgrade CR as complete
1. Upgrade Shim tears down Upgrade Toolkit

## Customized Upgrades

Upgrade Toolkit supports upgrading a Harvester cluster using other container images for Upgrade Repo and Manager that are not packaged in the ISO image. To do so, please see below.

Create a Version CR. This is almost the same as before.

```bash
cat <<EOF | kubectl apply -f -
apiVersion: management.harvesterhci.io/v1beta1
kind: Version
metadata:
  name: v1.6.1
spec:
  isoURL: https://releases.rancher.com/harvester/v1.6.1/harvester-v1.6.1-amd64.iso
EOF
```

When creating the UpgradePlan CR, specifying a different container image tag to use for Upgrade Repo and Manager:

```bash
cat <<EOF | kubectl create -f -
apiVersion: management.harvesterhci.io/v1beta1
kind: UpgradePlan
metadata:
  generateName: hvst-upgrade-
spec:
  version: v1.6.1
  upgrade: dev
EOF
```

A successfully executed UpgradePlan looks like the following:

```yaml
apiVersion: management.harvesterhci.io/v1beta1
kind: UpgradePlan
metadata:
  creationTimestamp: "2025-11-03T15:59:51Z"
  generateName: hvst-upgrade-
  generation: 1
  name: hvst-upgrade-5qx48
  resourceVersion: "201154"
  uid: 45e83fc9-1f58-4bb9-b63d-a122101bf80e
spec:
  upgrade: dev
  version: v1.6.1
status:
  conditions:
  - lastTransitionTime: "2025-11-03T16:41:12Z"
    message: UpgradePlan has completed
    observedGeneration: 1
    reason: Succeeded
    status: "False"
    type: Progressing
  - lastTransitionTime: "2025-11-03T16:41:12Z"
    message: ""
    observedGeneration: 1
    reason: ReconcileSuccess
    status: "False"
    type: Degraded
  - lastTransitionTime: "2025-11-03T16:41:12Z"
    message: Entered one of the terminal phases
    observedGeneration: 1
    reason: Executed
    status: "False"
    type: Available
  isoImageID: harvester-system/hvst-upgrade-5qx48-iso
  nodeUpgradeStatuses:
    charlie-1-tink-system:
      state: KubernetesUpgraded
    charlie-2-tink-system:
      state: KubernetesUpgraded
    charlie-3-tink-system:
      state: KubernetesUpgraded
  phase: Succeeded
  phaseTransitionTimestamps:
  - phase: Initializing
    phaseTransitionTimestamp: "2025-11-03T15:59:51Z"
  - phase: Initialized
    phaseTransitionTimestamp: "2025-11-03T15:59:52Z"
  - phase: ISODownloading
    phaseTransitionTimestamp: "2025-11-03T15:59:54Z"
  - phase: ISODownloaded
    phaseTransitionTimestamp: "2025-11-03T16:01:03Z"
  - phase: RepoCreating
    phaseTransitionTimestamp: "2025-11-03T16:01:04Z"
  - phase: RepoCreated
    phaseTransitionTimestamp: "2025-11-03T16:10:51Z"
  - phase: MetadataPopulating
    phaseTransitionTimestamp: "2025-11-03T16:10:52Z"
  - phase: MetadataPopulated
    phaseTransitionTimestamp: "2025-11-03T16:10:53Z"
  - phase: ImagePreloading
    phaseTransitionTimestamp: "2025-11-03T16:10:54Z"
  - phase: ImagePreloaded
    phaseTransitionTimestamp: "2025-11-03T16:15:26Z"
  - phase: ClusterUpgrading
    phaseTransitionTimestamp: "2025-11-03T16:15:26Z"
  - phase: ClusterUpgraded
    phaseTransitionTimestamp: "2025-11-03T16:24:45Z"
  - phase: NodeUpgrading
    phaseTransitionTimestamp: "2025-11-03T16:24:46Z"
  - phase: NodeUpgraded
    phaseTransitionTimestamp: "2025-11-03T16:41:09Z"
  - phase: CleaningUp
    phaseTransitionTimestamp: "2025-11-03T16:41:09Z"
  - phase: CleanedUp
    phaseTransitionTimestamp: "2025-11-03T16:41:12Z"
  - phase: Succeeded
    phaseTransitionTimestamp: "2025-11-03T16:41:12Z"
  previousVersion: v1.6.0
  releaseMetadata:
    harvester: v1.6.1
    harvesterChart: 1.6.1
    kubernetes: v1.33.5+rke2r1
    minUpgradableVersion: v1.5.0
    monitoringChart: 105.1.2+up61.3.2
    os: Harvester v1.6.1
    rancher: v2.12.2
  version:
    isoURL: http://10.115.49.5/iso/harvester/v1.6.1/harvester-v1.6.1-amd64.iso
```
