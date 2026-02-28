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
  name: v1.7.1
spec:
  isoURL: https://releases.rancher.com/harvester/v1.7.1/harvester-v1.7.1-amd64.iso
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
  version: v1.7.1
  upgrade: dev
EOF
```

A successfully executed UpgradePlan looks like the following:

```yaml
apiVersion: management.harvesterhci.io/v1beta1
kind: UpgradePlan
metadata:
  creationTimestamp: "2026-02-27T17:27:06Z"
  generateName: hvst-upgrade-
  generation: 1
  name: hvst-upgrade-b5m8g
  resourceVersion: "967007"
  uid: 8125388a-77c4-4e5f-8c21-1d342e700b81
spec:
  mode: automatic
  upgrade: dev
  version: v1.7.1
status:
  conditions:
  - lastTransitionTime: "2026-02-28T05:15:20Z"
    message: UpgradePlan has completed
    observedGeneration: 1
    reason: Succeeded
    status: "False"
    type: Progressing
  - lastTransitionTime: "2026-02-28T05:15:20Z"
    message: ""
    observedGeneration: 1
    reason: ReconcileSuccess
    status: "False"
    type: Degraded
  - lastTransitionTime: "2026-02-28T05:15:20Z"
    message: Entered one of the terminal phases
    observedGeneration: 1
    reason: Executed
    status: "False"
    type: Available
  currentPhase: Succeeded
  isoImageID: harvester-system/hvst-upgrade-b5m8g-iso
  nodeUpgradeStatuses:
    charlie-1-tink-system:
      state: PostDrained
    charlie-2-tink-system:
      state: PostDrained
    charlie-3-tink-system:
      state: PostDrained
  phaseTransitionTimestamps:
  - phase: Initializing
    phaseTransitionTimestamp: "2026-02-27T17:27:06Z"
  - phase: Initialized
    phaseTransitionTimestamp: "2026-02-27T17:27:07Z"
  - phase: ISODownloading
    phaseTransitionTimestamp: "2026-02-27T17:27:08Z"
  - phase: ISODownloaded
    phaseTransitionTimestamp: "2026-02-27T17:31:11Z"
  - phase: RepoCreating
    phaseTransitionTimestamp: "2026-02-27T17:31:12Z"
  - phase: RepoCreated
    phaseTransitionTimestamp: "2026-02-27T17:31:36Z"
  - phase: MetadataPopulating
    phaseTransitionTimestamp: "2026-02-27T17:31:36Z"
  - phase: MetadataPopulated
    phaseTransitionTimestamp: "2026-02-27T17:32:08Z"
  - phase: ImagePreloading
    phaseTransitionTimestamp: "2026-02-27T17:32:08Z"
  - phase: ImagePreloaded
    phaseTransitionTimestamp: "2026-02-27T17:33:37Z"
  - phase: ClusterUpgrading
    phaseTransitionTimestamp: "2026-02-27T17:33:37Z"
  - phase: ClusterUpgraded
    phaseTransitionTimestamp: "2026-02-27T17:35:16Z"
  - phase: NodeUpgrading
    phaseTransitionTimestamp: "2026-02-27T17:35:16Z"
  - phase: NodeUpgraded
    phaseTransitionTimestamp: "2026-02-28T05:15:18Z"
  - phase: Succeeded
    phaseTransitionTimestamp: "2026-02-28T05:15:20Z"
  previousVersion: v1.7.1
  provisionGeneration: 1
  releaseMetadata:
    harvester: v1.7.1
    harvesterChart: 1.7.1
    kubernetes: v1.34.3+rke2r3
    minUpgradableVersion: v1.6.0
    monitoringChart: 107.1.0+up69.8.2-rancher.15
    os: Harvester v1.7.1
    rancher: v2.13.1
  version:
    isoURL: https://releases.rancher.com/harvester/v1.7.1/harvester-v1.7.1-amd64.iso
```
