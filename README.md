# Harvester Upgrade Toolkit

[![Build Status](https://github.com/harvester/upgrade-toolkit/actions/workflows/dev.yml/badge.svg)](https://github.com/harvester/upgrade-toolkit/actions/workflows/dev.yml)
[![Releases](https://img.shields.io/github/release/harvester/upgrade-toolkit.svg)](https://github.com/harvester/upgrade-toolkit/releases)

Upgrade Toolkit is the primary component of Harvester Upgrade V2.

## User Guide

### Installation

The official way to install Upgrade Toolkit is via Helm ([source](https://github.com/harvester/charts/tree/master/charts/harvester-upgrade)):

```bash
helm upgrade --install harvester-upgrade harvester-upgrade \
    --repo=https://charts.harvesterhci.io \
    --namespace=harvester-system \
    --create-namespace \
    --values=values.yaml
```

### Kickstart an upgrade

Create a Version CR in the `harvester-system` namespace. This is almost the same as [before](https://docs.harvesterhci.io/v1.7/upgrade/index#customize-the-version).

```bash
cat <<EOF | kubectl apply -f -
apiVersion: harvesterhci.io/v1beta1
kind: Version
metadata:
  name: master-head
  namespace: harvester-system
spec:
  isoURL: https://releases.rancher.com/harvester/master/harvester-master-amd64.iso
EOF
```

Create an UpgradePlan CR with the desired version.

```bash
cat <<EOF | kubectl create -f -
apiVersion: management.harvesterhci.io/v1beta1
kind: UpgradePlan
metadata:
  generateName: hvst-upgrade-
spec:
  version: master-head
EOF
```

Additionally, upgrade can be triggered by creating an UpgradePlan CR with an existing ISO image on the cluster. The ISO image can be downloaded from a URL or uploaded to the cluster using the Harvester UI or CLI, and then referenced in the UpgradePlan CR.

For instance, to download the latest Harvester ISO from the releases page and use it for an upgrade, you can create a VirtualMachineImage CR as shown below:

```bash
cat <<EOF | kubectl create -f -
apiVersion: harvesterhci.io/v1beta1
kind: VirtualMachineImage
metadata:
  annotations:
    harvesterhci.io/os-upgrade-image: "True"
  name: harvester-master-amd64
  namespace: harvester-system
spec:
  backend: cdi
  displayName: harvester-master-amd64.iso
  sourceType: download
  url: https://releases.rancher.com/harvester/master/harvester-master-amd64.iso
  checksum: ""
  retry: 3
  targetStorageClassName: longhorn-static
EOF
```

Later, when the image is ready (actually, you don’t need to wait; the controller will automatically pick it up as soon as it becomes ready), you can create an UpgradePlan CR that references it (no need for referencing a Version CR):

```bash
cat <<EOF | kubectl create -f -
apiVersion: management.harvesterhci.io/v1beta1
kind: UpgradePlan
metadata:
  generateName: hvst-upgrade-
spec:
  image: harvester-master-amd64
EOF
```

### Customized upgrades

Upgrade Toolkit supports upgrading a Harvester cluster using other container images that are not packaged in the ISO image for Upgrade Repo and also node-specific upgrade jobs. To do so, please see below.

When creating the UpgradePlan CR, specifying a different container image tag:

```bash
cat <<EOF | kubectl create -f -
apiVersion: management.harvesterhci.io/v1beta1
kind: UpgradePlan
metadata:
  generateName: hvst-upgrade-
spec:
  version: master-head
  upgrade: main-head
EOF
```

Or optionally, specify a few options to customize the upgrade process:

```bash
cat <<EOF | kubectl create -f -
apiVersion: management.harvesterhci.io/v1beta1
kind: UpgradePlan
metadata:
  generateName: hvst-upgrade-
spec:
  version: master-head
  upgrade: main-head
  imagePreloadOption:
    concurrency: -1
  nodeUpgradeOption:
    pauseNodes:
    - charlie-1-tink-system
    - charlie-3-tink-system
  restoreVM: true
EOF
```

For all the available options, see the output of `kubectl explain upgradeplans.spec`.

A successfully executed UpgradePlan looks like the following:

```yaml
apiVersion: management.harvesterhci.io/v1beta1
kind: UpgradePlan
metadata:
  creationTimestamp: "2026-03-13T03:57:09Z"
  generateName: hvst-upgrade-
  generation: 3
  name: hvst-upgrade-864fh
  resourceVersion: "219468"
  uid: 24fde87d-491b-4af8-bafd-001fceb20a62
spec:
  imagePreloadOption:
    concurrency: 100
  nodeUpgradeOption: {}
  restoreVM: true
  upgrade: main-head
  version: v1.8.0-rc1
status:
  conditions:
  - lastTransitionTime: "2026-03-13T05:32:57Z"
    message: UpgradePlan has completed
    observedGeneration: 3
    reason: Succeeded
    status: "False"
    type: Progressing
  - lastTransitionTime: "2026-03-13T05:32:57Z"
    message: ""
    observedGeneration: 3
    reason: ReconcileSuccess
    status: "False"
    type: Degraded
  - lastTransitionTime: "2026-03-13T05:32:57Z"
    message: Entered one of the terminal phases
    observedGeneration: 3
    reason: Executed
    status: "False"
    type: Available
  currentPhase: Succeeded
  isoImageID: hvst-upgrade-864fh-iso
  nodeUpgradeStatuses:
    charlie-1-tink-system:
      state: ImageCleaned
    charlie-2-tink-system:
      state: ImageCleaned
    charlie-3-tink-system:
      state: ImageCleaned
  phaseTransitionTimestamps:
  - phase: Initializing
    phaseTransitionTimestamp: "2026-03-13T03:57:09Z"
  - phase: Initialized
    phaseTransitionTimestamp: "2026-03-13T03:57:09Z"
  - phase: ISODownloading
    phaseTransitionTimestamp: "2026-03-13T03:57:09Z"
  - phase: ISODownloaded
    phaseTransitionTimestamp: "2026-03-13T04:01:53Z"
  - phase: RepoCreating
    phaseTransitionTimestamp: "2026-03-13T04:01:53Z"
  - phase: RepoCreated
    phaseTransitionTimestamp: "2026-03-13T04:01:58Z"
  - phase: MetadataPopulating
    phaseTransitionTimestamp: "2026-03-13T04:01:58Z"
  - phase: MetadataPopulated
    phaseTransitionTimestamp: "2026-03-13T04:01:59Z"
  - phase: ImagePreloading
    phaseTransitionTimestamp: "2026-03-13T04:01:59Z"
  - phase: ImagePreloaded
    phaseTransitionTimestamp: "2026-03-13T04:12:00Z"
  - phase: ClusterUpgrading
    phaseTransitionTimestamp: "2026-03-13T04:12:00Z"
  - phase: ClusterUpgraded
    phaseTransitionTimestamp: "2026-03-13T04:27:15Z"
  - phase: NodeUpgrading
    phaseTransitionTimestamp: "2026-03-13T04:27:15Z"
  - phase: NodeUpgraded
    phaseTransitionTimestamp: "2026-03-13T05:30:42Z"
  - phase: CleaningUp
    phaseTransitionTimestamp: "2026-03-13T05:30:43Z"
  - phase: CleanedUp
    phaseTransitionTimestamp: "2026-03-13T05:32:56Z"
  - phase: Succeeded
    phaseTransitionTimestamp: "2026-03-13T05:32:57Z"
  previousVersion: v1.7.1
  provisionGeneration: 1
  releaseMetadata:
    harvester: v1.8.0-rc1
    harvesterChart: 1.8.0-rc1
    kubernetes: v1.35.2+rke2r1
    minUpgradableVersion: v1.7.0
    monitoringChart: 108.0.2+up77.9.1-rancher.11
    os: Harvester v1.8.0-rc1
    rancher: v2.14.0-alpha9
  version:
    isoChecksum: 3c6f98efc02959da524828b0c44273c8375b7815ce5fcf1c11581479d979daa6e184f3d20535041137282c3d2b4a12d9ba3ce847b051e9aecb5310b73f19523c
    isoURL: https://releases.rancher.com/harvester/v1.8.0-rc1/harvester-v1.8.0-rc1-amd64.iso
```

### Upgrade-related events

During the upgrade, events are emitted at phase transitions and key points. View the events for an UpgradePlan with `kubectl describe upgradeplans <upgradeplan-name>`:

```
Events:
  Type     Reason                     Age                    From                      Message
  ----     ------                     ----                   ----                      -------
  Normal   PhaseTransition            105m                   upgradeplan-controller    Entering phase ISODownload
  Warning  ReconcileError             105m                   upgradeplan-controller    Pipeline error: VirtualMachineImage.harvesterhci.io "hvst-upgrade-864fh-iso" not found
  Normal   PhaseCompleted             100m                   upgradeplan-controller    Completed phase ISODownload
  Normal   PhaseTransition            100m                   upgradeplan-controller    Entering phase RepoCreate
  Warning  ReconcileError             100m                   upgradeplan-controller    Pipeline error: Deployment.apps "hvst-upgrade-864fh-repo" not found
  Warning  ReconcileError             100m                   upgradeplan-controller    Pipeline error: Service "hvst-upgrade-864fh-repo" not found
  Normal   PhaseCompleted             100m                   upgradeplan-controller    Completed phase RepoCreate
  Normal   PhaseTransition            100m                   upgradeplan-controller    Entering phase MetadataPopulate
  Normal   PhaseCompleted             100m                   upgradeplan-controller    Completed phase MetadataPopulate
  Normal   PhaseTransition            100m                   upgradeplan-controller    Entering phase ImagePreload
  Warning  ReconcileError             100m                   upgradeplan-controller    Pipeline error: Plan.upgrade.cattle.io "hvst-upgrade-864fh-image-preload" not found
  Normal   PhaseCompleted             90m                    upgradeplan-controller    Completed phase ImagePreload
  Normal   PhaseTransition            90m                    upgradeplan-controller    Entering phase ClusterUpgrade
  Normal   PhaseCompleted             75m                    upgradeplan-controller    Completed phase ClusterUpgrade
  Normal   PhaseTransition            75m                    upgradeplan-controller    Entering phase NodeUpgrade
  Normal   RestoreVMConfigMapCreated  63m                    vm-live-migrate-detector  ConfigMap harvester-system/hvst-upgrade-864fh-restore-vm created
  Normal   VMShutdownCompleted        63m                    vm-live-migrate-detector  Shutdown completed for 0 VM(s) on node charlie-1-tink-system, success: 0, failed: 0
  Normal   VMShutdownCompleted        39m                    vm-live-migrate-detector  Shutdown completed for 1 VM(s) on node charlie-2-tink-system, success: 1, failed: 0
  Normal   RestoreVMCompleted         26m                    restore-vm                Restored 1 VMs for node charlie-2-tink-system during upgrade hvst-upgrade-864fh, success: 1, failed: 0
  Normal   VMShutdownCompleted        25m                    vm-live-migrate-detector  Shutdown completed for 1 VM(s) on node charlie-3-tink-system, success: 1, failed: 0
  Warning  ReconcileError             12m                    upgradeplan-controller    Pipeline error: waiting for Rancher to complete node upgrades: secret custom-58afebea1719-machine-plan still has rke.cattle.io/post-drain annotation
  Normal   PhaseCompleted             12m                    upgradeplan-controller    Completed phase NodeUpgrade
  Normal   PhaseTransition            12m                    upgradeplan-controller    Entering phase ImageCleanup
  Warning  ReconcileError             12m                    upgradeplan-controller    Pipeline error: Plan.upgrade.cattle.io "hvst-upgrade-864fh-image-cleanup" not found
  Normal   RestoreVMCompleted         11m                    restore-vm                Restored 1 VMs for node charlie-3-tink-system during upgrade hvst-upgrade-864fh, success: 1, failed: 0
  Normal   PhaseCompleted             9m50s (x2 over 9m51s)  upgradeplan-controller    Completed phase ImageCleanup
  Normal   UpgradeSucceeded           9m50s (x2 over 9m50s)  upgradeplan-controller    Upgrade completed successfully
```

### User-facing annotations

The following annotations can be set on an UpgradePlan CR to skip or override specific pre-flight checks.

| Annotation | Value | Scope | Description |
|---|---|---|---|
| `management.harvesterhci.io/skip-webhook` | `"true"` | Webhook (create) | Bypasses all create-time validation checks |
| `management.harvesterhci.io/skip-single-replica-detached-vol` | `"true"` | Webhook (create) | Skips the detached single-replica Longhorn volume check (active single-replica volumes are still blocked) |
| `management.harvesterhci.io/allow-deletion` | `"true"` | Webhook (delete) | Allows deletion of a progressing UpgradePlan (hard-blocked during `ClusterUpgrading` and `NodeUpgrading` phases regardless) |
| `management.harvesterhci.io/skip-garbage-collection-threshold-check` | `"true"` | Controller (init phase) | Skips the kubelet disk-space / image GC threshold pre-flight check |
| `management.harvesterhci.io/min-certs-expiration-in-day` | Integer > 0 | Controller (init phase) | Overrides the minimum certificate expiration window in days (default: 7) |
| `management.harvesterhci.io/upgrade-toolkit-image` | Image repo+name | Controller (all phases) | Overrides the default upgrade-toolkit container image (`rancher/harvester-upgrade-toolkit`); tag is still controlled by `spec.upgrade` |

Example usage:

```bash
cat <<EOF | kubectl create -f -
apiVersion: management.harvesterhci.io/v1beta1
kind: UpgradePlan
metadata:
  generateName: hvst-upgrade-
  annotations:
    management.harvesterhci.io/skip-single-replica-detached-vol: "true"
    management.harvesterhci.io/min-certs-expiration-in-day: "3"
spec:
  version: master-head
EOF
```

## Developer Guide

After making changes, build and test the upgrade-toolkit binary and container image.

```bash
# Lint the code
make lint

# Run unit tests and interation tests
make test

# Build the upgrade-toolkit binary (under `bin/`)
make build

# Build the container image
# The built image will be tagged with `rancher/harvester-upgrade-toolkit:<branch>-head`
make docker-build
```

To build and push the container image, run:

```bash
# Adapt the `REPO` value below to your own Docker Hub repository
REPO=starbops make docker-buildx
```

### Kustomize manifests

Upgrade Toolkit comes with a set of Kustomize manifests that enable easy installation.

To build or update the Kustomize manifests, run:

```bash
make manifests
```

The generatedoutput is located in `config/`, and can be deployed with the following command:

```bash
# Specify the image name and tag in `IMG`
make deploy IMG=starbops/harvester-upgrade-toolkit:dev
```

### Installer manifests

Upgrade Toolkit comes with a single file of installer manifests that enable easy installation.

To build or update the installer manifests, run:

```bash
# Specify the image name and tag in `IMG`
make build-installer IMG=starbops/harvester-upgrade-toolkit:dev
```

The built installer manifests are located in `dist/installer.yaml`, and can be installed via `kubectl apply`:

```bash
kubectl apply -f dist/installer.yaml
```

### Helm chart

Upgrade Toolkit leverages [Kubebuilder's Helm plugin](https://book.kubebuilder.io/plugins/available/helm-v2-alpha) to manage the local Helm chart.

> [!NOTE]
> Kubebuilder’s Helm plugin generates Helm charts from the [installer manifests](./dist/install.yaml). Futhermore, `make build-installer` depends on the Kustomize manifests generated by `make manifests`, so it is recommended to run `make manifests` first, update the Kustomize manifests under `config/`, and then generate the Helm chart in order to ensure everything is in sync.

```bash
# Update the local Helm chart
kubebuilder edit --plugins=helm/v2-alpha
```

> [!NOTE]
> The `kubebuilder edit --plugins=helm/v2-alpha` command regenerates all template files under `dist/chart/templates/`. It does not preserve manual edits to templates. After running the plugin, the following manual fixups are required:
>
> 1. Delete `dist/chart/templates/cert-manager/` (the project does not use cert-manager)
> 2. Delete `dist/chart/templates/webhook/mutating-webhook-configuration.yaml` and `validating-webhook-configuration.yaml` (replaced by the consolidated webhook.yaml)
> 3. In `dist/chart/templates/manager/manager.yaml`, replace all occurrences of `.Values.certManager.enable` with `.Values.webhook.enable`
> 4. In `dist/chart/templates/monitoring/servicemonitor.yaml`, remove the cert-manager TLS configuration block and use `insecureSkipVerify: true` only
>
> The `dist/chart/templates/webhook/webhook.yaml` (which uses `genCA()`/`genSignedCert()` for self-signed cert generation) is not affected because the plugin does not delete unrecognized files.

### Run the controller manager locally

Every time you make changes to the code, especially in the control loop, you may want to see the changes in action locally from your IDE or terminal.

To do so, make sure you have a Harvester cluster running and can be accessed via `kubectl`.

Install the UpgradePlan CRD:

```bash
# Make sure you have a valid KUBECONFIG env var, pointing to your cluster
make install
```

Run the controller manager locally (without starting the webhook server):

```bash
ENABLE_WEBHOOKS=false make run
```

[Create the Version and UpgradePlan CRs](#kickstart-an-upgrade) to kickstart the upgrade process.

After the UpgradePlan CR passes the `RepoCreated` phase, set up a port-forward to allow the local controller manager to access the remote Upgrade Repo.

```bash
UP_NAME=$(kubectl get upgradeplans -o json | \
jq -r '.items[]
  | select(any(.status.conditions[]; .type=="Progressing" and .status=="True"))
  | .metadata.name')

# If privileges are not sufficient, run the following command as root with `sudo -E` prepended:
kubectl -n harvester-system port-forward svc/$UP_NAME-repo 80:80
```

The local controller manager should be able to access the remote Upgrade Repo, advance to the `MetadataPopulated` phase, and proceed further.

### Install the local Helm chart

Make sure you have the container image built and pushed to a registry.

```bash
# Specify the image name and tag in `IMG`
make helm-deploy IMG=starbops/harvester-upgrade-toolkit:dev
```

[Create the Version and UpgradePlan CRs](#kickstart-an-upgrade) to kickstart the upgrade process.

### Introduce new phases

The phase-based runner design facilitates well-organized phase ordering and allows for the easy integration of new phases.

Let's say we want to introduce a new phase called `PreCheck`. There will be three places in the codebase that require us to modify:

1. Update the `pkg/upgradeplan/pipeline.go` file
2. Create the new `pkg/upgradeplan/phase_precheck.go` file

## License

Copyright 2025-2026 [SUSE, LLC.](https://www.suse.com/)

This project is licensed under the Apache License 2.0 - see the [LICENSE](./LICENSE) file for details.
