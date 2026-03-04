#!/usr/bin/env bash

# generate-webhook-certs.sh
#
# Generates self-signed TLS certificates for the webhook server and patches
# webhook configurations with the CA bundle. This replaces the cert-manager
# dependency for the Kustomize deploy path (make deploy).
#
# Usage:
#   hack/generate-webhook-certs.sh [NAMESPACE] [SERVICE_NAME] [SECRET_NAME]
#
# Environment variables (override defaults):
#   NAMESPACE    - namespace where the controller is deployed
#   SERVICE_NAME - name of the webhook service
#   SECRET_NAME  - name of the TLS secret to create

set -euo pipefail

NAMESPACE="${NAMESPACE:-${1:-upgrade-toolkit-system}}"
SERVICE_NAME="${SERVICE_NAME:-${2:-upgrade-toolkit-webhook-service}}"
SECRET_NAME="${SECRET_NAME:-${3:-webhook-server-cert}}"

TMPDIR=$(mktemp -d)
trap 'rm -rf "${TMPDIR}"' EXIT

CA_KEY="${TMPDIR}/ca.key"
CA_CERT="${TMPDIR}/ca.crt"
SERVER_KEY="${TMPDIR}/tls.key"
SERVER_CSR="${TMPDIR}/server.csr"
SERVER_CERT="${TMPDIR}/tls.crt"

DNS1="${SERVICE_NAME}.${NAMESPACE}.svc"
DNS2="${SERVICE_NAME}.${NAMESPACE}.svc.cluster.local"

echo "Generating webhook certificates for ${DNS1} ..."

# Generate CA key and certificate (10-year validity)
openssl genrsa -out "${CA_KEY}" 4096 2>/dev/null
openssl req -x509 -new -nodes \
  -key "${CA_KEY}" \
  -sha256 \
  -days 3650 \
  -out "${CA_CERT}" \
  -subj "/CN=upgrade-toolkit-ca" 2>/dev/null

# Generate server key and CSR
openssl genrsa -out "${SERVER_KEY}" 4096 2>/dev/null
openssl req -new \
  -key "${SERVER_KEY}" \
  -out "${SERVER_CSR}" \
  -subj "/CN=${DNS1}" 2>/dev/null

# Generate server certificate signed by CA (1-year validity)
openssl x509 -req \
  -in "${SERVER_CSR}" \
  -CA "${CA_CERT}" \
  -CAkey "${CA_KEY}" \
  -CAcreateserial \
  -out "${SERVER_CERT}" \
  -days 365 \
  -sha256 \
  -extfile <(printf "subjectAltName=DNS:%s,DNS:%s" "${DNS1}" "${DNS2}") 2>/dev/null

# Create or update the webhook-server-cert Secret
kubectl create secret generic "${SECRET_NAME}" \
  --namespace="${NAMESPACE}" \
  --from-file=ca.crt="${CA_CERT}" \
  --from-file=tls.crt="${SERVER_CERT}" \
  --from-file=tls.key="${SERVER_KEY}" \
  --dry-run=client -o yaml | kubectl apply -f -

# Patch webhook configurations with caBundle
CA_BUNDLE=$(base64 < "${CA_CERT}" | tr -d '\n')

echo "Patching MutatingWebhookConfiguration ..."
kubectl patch mutatingwebhookconfiguration upgrade-toolkit-mutating-webhook-configuration \
  --type='json' \
  -p="[{\"op\": \"add\", \"path\": \"/webhooks/0/clientConfig/caBundle\", \"value\": \"${CA_BUNDLE}\"}]"

echo "Patching ValidatingWebhookConfiguration ..."
kubectl patch validatingwebhookconfiguration upgrade-toolkit-validating-webhook-configuration \
  --type='json' \
  -p="[{\"op\": \"add\", \"path\": \"/webhooks/0/clientConfig/caBundle\", \"value\": \"${CA_BUNDLE}\"}]"

echo "Webhook certificates generated and applied successfully."
