#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
K8S_DIR="${K8S_DIR:-${SCRIPT_DIR}/../k8s}"
OUT_DIR="${1:-/tmp/k8s-rendered}"

# Defaults for non-secret values.
: "${AZURE_OPENAI_DEPLOYMENT_NAME:=gpt-5-nano}"
: "${AZURE_TEXT_EMBEDDING_DEPLOYMENT_NAME:=text-embedding-3-small}"
: "${K8S_NAMESPACE:=aviation-rag}"
: "${PGPORT:=5432}"
: "${PGDATABASE:=aviationrag}"
: "${PGUSER:=aviationrag_readonly}"
: "${FLASK_ENV:=production}"
: "${LOG_LEVEL:=INFO}"
: "${USE_POSTGRES:=true}"
: "${OTEL_SERVICE_NAME:=aviation-rag-backend}"
: "${ENVIRONMENT:=production}"
: "${AF_SESSION_TTL_SECONDS:=3600}"
: "${AF_MAX_SESSIONS:=500}"
: "${FABRIC_KQL_ENDPOINT:=}"
: "${FABRIC_GRAPH_ENDPOINT:=}"
: "${FABRIC_NOSQL_ENDPOINT:=}"
: "${BACKEND_INGRESS_HOST:=aviation-rag-api.westeurope.cloudapp.azure.com}"

export AZURE_CONTAINER_REGISTRY IMAGE_NAME
export AZURE_OPENAI_ENDPOINT AZURE_OPENAI_DEPLOYMENT_NAME AZURE_TEXT_EMBEDDING_DEPLOYMENT_NAME
export AZURE_SEARCH_ENDPOINT
export PII_ENDPOINT PII_CONTAINER_ENDPOINT
export K8S_NAMESPACE
export PGHOST PGPORT PGDATABASE PGUSER
export FLASK_ENV LOG_LEVEL USE_POSTGRES OTEL_SERVICE_NAME ENVIRONMENT AF_SESSION_TTL_SECONDS AF_MAX_SESSIONS
export FABRIC_KQL_ENDPOINT FABRIC_GRAPH_ENDPOINT FABRIC_NOSQL_ENDPOINT
export BACKEND_INGRESS_HOST

required=(
  AZURE_CONTAINER_REGISTRY
  IMAGE_NAME
  AZURE_OPENAI_ENDPOINT
  AZURE_SEARCH_ENDPOINT
  PII_ENDPOINT
  PII_CONTAINER_ENDPOINT
  PGHOST
)

for name in "${required[@]}"; do
  if [ -z "${!name:-}" ]; then
    echo "Missing required environment variable: ${name}" >&2
    exit 1
  fi
done

mkdir -p "${OUT_DIR}"

for manifest in namespace.yaml backend-service.yaml backend-configmap.yaml backend-deployment.yaml backend-ingress.yaml; do
  src="${K8S_DIR}/${manifest}"
  [ -f "${src}" ] || continue
  envsubst < "${src}" > "${OUT_DIR}/${manifest}"
done

echo "Rendered manifests written to ${OUT_DIR}"
