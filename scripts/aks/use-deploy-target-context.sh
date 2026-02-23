#!/usr/bin/env bash
set -euo pipefail

AZURE_SUBSCRIPTION_ID="${AZURE_SUBSCRIPTION_ID:-6a539906-6ce2-4e3b-84ee-89f701de18d8}"
AZURE_TENANT_ID="${AZURE_TENANT_ID:-52095a81-130f-4b06-83f1-9859b2c73de6}"
AZURE_ACCOUNT_UPN="${AZURE_ACCOUNT_UPN:-admin@MngEnvMCAP705508.onmicrosoft.com}"
AKS_RESOURCE_GROUP="${AKS_RESOURCE_GROUP:-rg-aviation-rag}"
AKS_CLUSTER="${AKS_CLUSTER:-aks-aviation-rag}"
AKS_NAMESPACE="${AKS_NAMESPACE:-aviation-rag}"
BACKEND_DEPLOYMENT="${BACKEND_DEPLOYMENT:-aviation-rag-backend}"
EXPECTED_AKS_RESOURCE_ID="${EXPECTED_AKS_RESOURCE_ID:-}"

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "ERROR: Missing required command: $1" >&2
    exit 1
  fi
}

require_cmd az
require_cmd kubectl

print_account_remediation() {
  cat >&2 <<EOF
Remediation:
  az login --tenant ${AZURE_TENANT_ID}
  az account set --subscription ${AZURE_SUBSCRIPTION_ID}
EOF
}

print_kube_remediation() {
  cat >&2 <<EOF
Remediation:
  az aks get-credentials --resource-group ${AKS_RESOURCE_GROUP} --name ${AKS_CLUSTER} --overwrite-existing
  kubectl config use-context ${AKS_CLUSTER}
  kubectl config set-context --current --namespace=${AKS_NAMESPACE}
EOF
}

actual_subscription="$(az account show --query id -o tsv 2>/dev/null || true)"
actual_tenant="$(az account show --query tenantId -o tsv 2>/dev/null || true)"
actual_account="$(az account show --query user.name -o tsv 2>/dev/null || true)"

if [ -z "${actual_subscription}" ] || [ -z "${actual_tenant}" ]; then
  echo "ERROR: Azure CLI is not authenticated." >&2
  print_account_remediation
  exit 1
fi

if [ "${actual_subscription}" != "${AZURE_SUBSCRIPTION_ID}" ]; then
  echo "ERROR: Active subscription '${actual_subscription}' does not match expected '${AZURE_SUBSCRIPTION_ID}'." >&2
  print_account_remediation
  exit 1
fi

if [ "${actual_tenant}" != "${AZURE_TENANT_ID}" ]; then
  echo "ERROR: Active tenant '${actual_tenant}' does not match expected '${AZURE_TENANT_ID}'." >&2
  print_account_remediation
  exit 1
fi

if [ -n "${AZURE_ACCOUNT_UPN}" ] && [ "${actual_account}" != "${AZURE_ACCOUNT_UPN}" ]; then
  echo "ERROR: Active account '${actual_account}' does not match expected '${AZURE_ACCOUNT_UPN}'." >&2
  print_account_remediation
  exit 1
fi

aks_id="$(az aks show --resource-group "${AKS_RESOURCE_GROUP}" --name "${AKS_CLUSTER}" --query id -o tsv 2>/dev/null || true)"
if [ -z "${aks_id}" ]; then
  echo "ERROR: Could not resolve AKS cluster '${AKS_CLUSTER}' in resource group '${AKS_RESOURCE_GROUP}'." >&2
  exit 1
fi

if [ -n "${EXPECTED_AKS_RESOURCE_ID}" ] && [ "${aks_id}" != "${EXPECTED_AKS_RESOURCE_ID}" ]; then
  echo "ERROR: AKS resource ID mismatch. Expected '${EXPECTED_AKS_RESOURCE_ID}' got '${aks_id}'." >&2
  exit 1
fi

current_context="$(kubectl config current-context 2>/dev/null || true)"
current_cluster="$(kubectl config view --minify -o jsonpath='{.contexts[0].context.cluster}' 2>/dev/null || true)"
current_server="$(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}' 2>/dev/null || true)"
current_namespace="$(kubectl config view --minify -o jsonpath='{.contexts[0].context.namespace}' 2>/dev/null || true)"

if [ -z "${current_context}" ] || [ -z "${current_cluster}" ]; then
  echo "ERROR: kubectl context is not configured for AKS." >&2
  print_kube_remediation
  exit 1
fi

if [ -z "${current_namespace}" ]; then
  current_namespace="default"
fi

if [[ "${current_cluster}" != *"${AKS_CLUSTER}"* ]]; then
  echo "ERROR: kubectl cluster '${current_cluster}' does not target '${AKS_CLUSTER}'." >&2
  print_kube_remediation
  exit 1
fi

if [ "${current_namespace}" != "${AKS_NAMESPACE}" ]; then
  echo "ERROR: kubectl namespace '${current_namespace}' does not match expected '${AKS_NAMESPACE}'." >&2
  print_kube_remediation
  exit 1
fi

echo ""
echo "Active kubectl target"
echo "  context: ${current_context}"
echo "  cluster: ${current_cluster}"
echo "  server: ${current_server}"
echo "  namespace: ${current_namespace}"
echo "  account: ${actual_account}"
echo "  tenant: ${actual_tenant}"
echo "  subscription: ${actual_subscription}"
echo "  target_aks_id: ${aks_id}"

echo ""
echo "Backend deployment status"
kubectl get deployment "${BACKEND_DEPLOYMENT}" -n "${AKS_NAMESPACE}" \
  -o jsonpath='name={.metadata.name} image={.spec.template.spec.containers[0].image} ready={.status.readyReplicas}/{.spec.replicas}{"\n"}'
