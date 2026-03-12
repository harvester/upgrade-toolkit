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
  creationTimestamp: "2026-03-02T08:01:18Z"
  generateName: hvst-upgrade-
  generation: 1
  name: hvst-upgrade-gj8rf
  resourceVersion: "130247"
  uid: 59d59440-9bf9-4064-a664-4630a2940529
spec:
  mode: automatic
  upgrade: dev
  version: master-head
status:
  conditions:
  - lastTransitionTime: "2026-03-02T09:31:16Z"
    message: UpgradePlan has completed
    observedGeneration: 1
    reason: Succeeded
    status: "False"
    type: Progressing
  - lastTransitionTime: "2026-03-02T09:31:16Z"
    message: ""
    observedGeneration: 1
    reason: ReconcileSuccess
    status: "False"
    type: Degraded
  - lastTransitionTime: "2026-03-02T09:31:16Z"
    message: Entered one of the terminal phases
    observedGeneration: 1
    reason: Executed
    status: "False"
    type: Available
  currentPhase: Succeeded
  isoImageID: harvester-system/hvst-upgrade-gj8rf-iso
  nodeUpgradeStatuses:
    charlie-1-tink-system:
      state: ImageCleaned
    charlie-2-tink-system:
      state: ImageCleaned
    charlie-3-tink-system:
      state: ImageCleaned
  phaseTransitionTimestamps:
  - phase: Initializing
    phaseTransitionTimestamp: "2026-03-02T08:01:18Z"
  - phase: Initialized
    phaseTransitionTimestamp: "2026-03-02T08:01:18Z"
  - phase: ISODownloading
    phaseTransitionTimestamp: "2026-03-02T08:01:18Z"
  - phase: ISODownloaded
    phaseTransitionTimestamp: "2026-03-02T08:05:37Z"
  - phase: RepoCreating
    phaseTransitionTimestamp: "2026-03-02T08:05:37Z"
  - phase: RepoCreated
    phaseTransitionTimestamp: "2026-03-02T08:05:54Z"
  - phase: MetadataPopulating
    phaseTransitionTimestamp: "2026-03-02T08:05:54Z"
  - phase: MetadataPopulated
    phaseTransitionTimestamp: "2026-03-02T08:06:04Z"
  - phase: ImagePreloading
    phaseTransitionTimestamp: "2026-03-02T08:06:04Z"
  - phase: ImagePreloaded
    phaseTransitionTimestamp: "2026-03-02T08:32:20Z"
  - phase: ClusterUpgrading
    phaseTransitionTimestamp: "2026-03-02T08:32:20Z"
  - phase: ClusterUpgraded
    phaseTransitionTimestamp: "2026-03-02T08:46:14Z"
  - phase: NodeUpgrading
    phaseTransitionTimestamp: "2026-03-02T08:46:14Z"
  - phase: NodeUpgraded
    phaseTransitionTimestamp: "2026-03-02T09:29:34Z"
  - phase: CleaningUp
    phaseTransitionTimestamp: "2026-03-02T09:29:35Z"
  - phase: CleanedUp
    phaseTransitionTimestamp: "2026-03-02T09:31:16Z"
  - phase: Succeeded
    phaseTransitionTimestamp: "2026-03-02T09:31:16Z"
  previousVersion: v1.7.1
  provisionGeneration: 1
  releaseMetadata:
    harvester: 014fbeae
    harvesterChart: 0.0.0-master-014fbeae
    kubernetes: v1.35.1+rke2r1
    monitoringChart: 108.0.2+up77.9.1-rancher.11
    os: Harvester master
    rancher: v2.14.0-alpha5
  version:
    isoURL: https://releases.rancher.com/harvester/master/harvester-master-amd64.iso
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
