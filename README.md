# Harvester Upgrade Toolkit

Upgrade Toolkit is the primary component of Harvester Upgrade V2.

## Customized Upgrades

Upgrade Toolkit supports upgrading a Harvester cluster using other container images that are not packaged in the ISO image for Upgrade Repo and also node-specific upgrade jobs. To do so, please see below.

Create a Version CR. This is almost the same as before.

```bash
cat <<EOF | kubectl apply -f -
apiVersion: management.harvesterhci.io/v1beta1
kind: Version
metadata:
  name: master-head
spec:
  isoURL: https://releases.rancher.com/harvester/master/harvester-master-amd64.iso
EOF
```

When creating the UpgradePlan CR, specifying a different container image tag:

```bash
cat <<EOF | kubectl create -f -
apiVersion: management.harvesterhci.io/v1beta1
kind: UpgradePlan
metadata:
  generateName: hvst-upgrade-
spec:
  version: master-head
  upgrade: dev
EOF
```

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

## Development

### General Flow

```bash
# Lint the code
make lint

# Run unit tests and interation tests
make test

# Build the upgrade-toolkit binary (under `bin/`)
make build

# Build the container image
# Adapt the `IMG` value below to your own image name and tag
make docker-buildx IMG=starbops/harvester-upgrade-toolkit:dev
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
> Kubebuilder’s Helm plugin generates Helm charts from the installer manifests. Because `make build-installer` depends on the Kustomize manifests generated by `make manifests`, it is recommended to run `make manifests` first and then update the Kustomize manifests under `config/`, to ensure everything is in sync.

```bash
# Update the local Helm chart
kubebuilder edit --plugins=helm/v2-alpha
```

### Run the controller manager locally

Every time you make changes to the code, especially in the control loop, you may want to see the changes in action locally from your IDE or terminal.

To do so, make sure you have a Harvester cluster running and can be accessed via `kubectl`.

Install the UpgradePlan and Version CRDs:

```bash
make install
```

Run the controller manager locally:

```bash
export KUBECONFIG=/path/to/kubeconfig
make run
```

[Create the Version and UpgradePlan CRs](#customized-upgrades) to kickstart the upgrade process.

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

[Create the Version and UpgradePlan CRs](#customized-upgrades) to kickstart the upgrade process.
