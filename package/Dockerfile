# Build the upgrade-toolkit binary
FROM registry.suse.com/bci/golang:1.24 AS builder
ARG TARGETOS
ARG TARGETARCH

WORKDIR /workspace
# Copy the Go Modules manifests
COPY go.mod go.mod
COPY go.sum go.sum
# cache deps before building and copying source so that we don't need to re-download as much
# and so that source changes don't invalidate our downloaded layer
RUN go mod download

# Copy the go source
COPY cmd/main.go cmd/main.go
COPY api/ api/
COPY internal/ internal/
COPY pkg/ pkg/

# Build
# the GOARCH has no default value to allow the binary to be built according to the host where the command
# was called. For example, if we call make docker-build in a local env which has the Apple Silicon M1 SO
# the docker BUILDPLATFORM arg will be linux/arm64 when for Apple x86 it will be linux/amd64. Therefore,
# by leaving it empty we can ensure that the container and binary shipped on it will have the same platform.
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} go build -a -o upgrade-toolkit cmd/main.go

FROM registry.opensuse.org/isv/rancher/harvester/os/v1.6/main/baseos:v1.6 AS baseos

# Generate addon manifests
FROM registry.suse.com/bci/golang:1.24 AS addons_generator

WORKDIR /workspace

ENV HARVESTER_ADDONS_VERSION=main
RUN git clone --branch ${HARVESTER_ADDONS_VERSION} --single-branch --depth 1 https://github.com/harvester/addons.git && \
    mkdir generated_addons/ && \
    cd addons/ && \
    go run . -generateAddons -path ../generated_addons/

# Use the base BCI image to package the upgrade-toolkit binary
FROM registry.suse.com/bci/bci-base:15.7
ARG TARGETARCH

RUN zypper rm -y container-suseconnect && \
    zypper --no-gpg-checks ref && \
    zypper in -y curl e2fsprogs rsync awk zstd jq helm zip unzip nginx && zypper clean -a

ENV KUBECTL_VERSION=v1.32.3
ENV KUBEVIRT_VERSION=v1.4.0
ENV WHARFIE_VERSION=v0.6.8
RUN curl -sfL https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/${TARGETARCH}/kubectl -o /usr/bin/kubectl && chmod +x /usr/bin/kubectl && \
    curl -sfL https://github.com/kubevirt/kubevirt/releases/download/${KUBEVIRT_VERSION}/virtctl-${KUBEVIRT_VERSION}-linux-${TARGETARCH} -o /usr/bin/virtctl && chmod +x /usr/bin/virtctl && \
    curl -sfL https://github.com/mikefarah/yq/releases/latest/download/yq_linux_${TARGETARCH} -o /usr/bin/yq && chmod +x /usr/bin/yq && \
    curl -sfL https://github.com/rancher/wharfie/releases/download/${WHARFIE_VERSION}/wharfie-${TARGETARCH}  -o /usr/bin/wharfie && chmod +x /usr/bin/wharfie

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
# COPY package/upgrade-helper /usr/local/bin/upgrade-helper

ENTRYPOINT ["/usr/local/bin/upgrade-toolkit"]
CMD ["manager"]
