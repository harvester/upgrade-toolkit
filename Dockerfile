# Build the upgrade-toolkit binary
FROM registry.suse.com/bci/golang:1.25.7 AS builder
ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev
ARG GIT_COMMIT=unknown
ARG GIT_TREE_STATE=clean

WORKDIR /workspace
# Copy the Go Modules manifests
COPY go.mod go.mod
COPY go.sum go.sum
# cache deps before building and copying source so that we don't need to re-download as much
# and so that source changes don't invalidate our downloaded layer
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

# Copy the go source
COPY cmd/ cmd/
COPY api/ api/
COPY internal/ internal/
COPY pkg/ pkg/

# Build
# the GOARCH has no default value to allow the binary to be built according to the host where the command
# was called. For example, if we call make docker-build in a local env which has the Apple Silicon M1 SO
# the docker BUILDPLATFORM arg will be linux/arm64 when for Apple x86 it will be linux/amd64. Therefore,
# by leaving it empty we can ensure that the container and binary shipped on it will have the same platform.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} go build \
    -ldflags "-X github.com/harvester/upgrade-toolkit/pkg/version.Version=${VERSION} -X github.com/harvester/upgrade-toolkit/pkg/version.GitCommit=${GIT_COMMIT} -X github.com/harvester/upgrade-toolkit/pkg/version.GitTreeState=${GIT_TREE_STATE}" \
    -o upgrade-toolkit ./cmd/

FROM registry.opensuse.org/isv/rancher/harvester/os/dev/main/baseos:latest AS baseos

# Generate addon manifests
FROM registry.suse.com/bci/golang:1.25.7 AS addons_generator

WORKDIR /workspace

ARG HARVESTER_ADDONS_VERSION=main
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    git clone --branch ${HARVESTER_ADDONS_VERSION} --single-branch --depth 1 https://github.com/harvester/addons.git && \
    mkdir generated_addons/ && \
    cd addons/ && \
    go run . -generateAddons -path ../generated_addons/

# Use the base BCI image to package the upgrade-toolkit binary
FROM registry.suse.com/bci/bci-base:16.0
ARG TARGETARCH

ENV KUBECTL_VERSION=v1.33.7
ENV KUBEVIRT_VERSION=v1.7.1
ENV YQ_VERSION=v4.52.4
ENV WHARFIE_VERSION=v0.7.0

# SHA256 checksums for verifying downloaded binaries (per architecture)
# To update: download the new binary and run `sha256sum <binary>`.
# kubectl: checksums available at https://dl.k8s.io/release/<ver>/bin/linux/<arch>/kubectl.sha256
# virtctl: checksums available athttps://github.com/kubevirt/kubevirt/releases/tag/<ver>
# yq: checksums available at https://github.com/mikefarah/yq/releases/download/<ver>/checksums
# wharfie: checksums at https://github.com/rancher/wharfie/releases/download/<ver>/sha256sum-<arch>.txt
ENV KUBECTL_SHA256_AMD64=471d94e208a89be62eb776700fc8206cbef11116a8de2dc06fc0086b0015375b
ENV KUBECTL_SHA256_ARM64=fa7ee98fdb6fba92ae05b5e0cde0abd5972b2d9a4a084f7052a1fd0dce6bc1de
ENV VIRTCTL_SHA256_AMD64=e0efcfc708067fa45232f3bab9cb2de3dbcd812d4c9aab88c727025fb213079f
ENV VIRTCTL_SHA256_ARM64=7737d967bc8512abedfdaa8a61a3512f93894c12162f9dde4fab73402a4f42d5
ENV YQ_SHA256_AMD64=0c4d965ea944b64b8fddaf7f27779ee3034e5693263786506ccd1c120f184e8c
ENV YQ_SHA256_ARM64=4c2cc022a129be5cc1187959bb4b09bebc7fb543c5837b93001c68f97ce39a5d
ENV WHARFIE_SHA256_AMD64=e5ff747b2f9f4155ce7b68917bac9dbe8a6a85727a94b0c8e6faca9252931e91
ENV WHARFIE_SHA256_ARM64=b8e02fe61d4f8cb1bd7927fd5e34b49b4dcf802c52670adfaa4527ed3d9afc41

RUN zypper rm -y container-suseconnect && \
    zypper --no-gpg-checks ref && \
    zypper in -y curl e2fsprogs rsync awk zstd jq helm zip unzip nginx util-linux && \
    zypper clean -a && \
    case "${TARGETARCH}" in \
      amd64) KUBECTL_SHA256=${KUBECTL_SHA256_AMD64}; VIRTCTL_SHA256=${VIRTCTL_SHA256_AMD64}; YQ_SHA256=${YQ_SHA256_AMD64}; WHARFIE_SHA256=${WHARFIE_SHA256_AMD64} ;; \
      arm64) KUBECTL_SHA256=${KUBECTL_SHA256_ARM64}; VIRTCTL_SHA256=${VIRTCTL_SHA256_ARM64}; YQ_SHA256=${YQ_SHA256_ARM64}; WHARFIE_SHA256=${WHARFIE_SHA256_ARM64} ;; \
      *) echo "Unsupported architecture: ${TARGETARCH}" && exit 1 ;; \
    esac && \
    curl -sfL https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/${TARGETARCH}/kubectl -o /usr/bin/kubectl && \
    echo "${KUBECTL_SHA256}  /usr/bin/kubectl" | sha256sum -c - && chmod +x /usr/bin/kubectl && \
    curl -sfL https://github.com/kubevirt/kubevirt/releases/download/${KUBEVIRT_VERSION}/virtctl-${KUBEVIRT_VERSION}-linux-${TARGETARCH} -o /usr/bin/virtctl && \
    echo "${VIRTCTL_SHA256}  /usr/bin/virtctl" | sha256sum -c - && chmod +x /usr/bin/virtctl && \
    curl -sfL https://github.com/mikefarah/yq/releases/download/${YQ_VERSION}/yq_linux_${TARGETARCH} -o /usr/bin/yq && \
    echo "${YQ_SHA256}  /usr/bin/yq" | sha256sum -c - && chmod +x /usr/bin/yq && \
    curl -sfL https://github.com/rancher/wharfie/releases/download/${WHARFIE_VERSION}/wharfie-${TARGETARCH} -o /usr/bin/wharfie && \
    echo "${WHARFIE_SHA256}  /usr/bin/wharfie" | sha256sum -c - && chmod +x /usr/bin/wharfie

RUN useradd -r -u 1000 -U -s /sbin/nologin -d /nonexistent upgrade-toolkit

WORKDIR /
COPY --from=builder /workspace/upgrade-toolkit /usr/local/bin/upgrade-toolkit

# Copy elemental binary to be used for upgrades
COPY --from=baseos /usr/bin/elemental /usr/local/bin/elemental

# Copy upgrade-relevant scripts and binaries
COPY package/lib.sh /usr/local/bin/lib.sh
COPY package/upgrade_manifests.sh /usr/local/bin/upgrade_manifests.sh
COPY package/upgrade_node.sh /usr/local/bin/upgrade_node.sh
COPY package/extra_manifests /usr/local/share/extra_manifests
COPY package/migrations /usr/local/share/migrations
COPY --from=addons_generator /workspace/generated_addons /usr/local/share/addons

USER 1000:1000

ENTRYPOINT ["/usr/local/bin/upgrade-toolkit"]
CMD ["manager"]
