#!/usr/bin/env bash
set -euo pipefail

TARGET_ACCOUNT_UPN="${TARGET_ACCOUNT_UPN:-admin@MngEnvMCAP705508.onmicrosoft.com}"
TARGET_TENANT_ID="${TARGET_TENANT_ID:-52095a81-130f-4b06-83f1-9859b2c73de6}"
TARGET_SUBSCRIPTION_ID="${TARGET_SUBSCRIPTION_ID:-6a539906-6ce2-4e3b-84ee-89f701de18d8}"

AKS_RESOURCE_GROUP="${AKS_RESOURCE_GROUP:-rg-aviation-rag}"
AKS_CLUSTER="${AKS_CLUSTER:-aks-aviation-rag}"
AKS_NAMESPACE="${AKS_NAMESPACE:-aviation-rag}"
BACKEND_CONFIGMAP="${BACKEND_CONFIGMAP:-backend-config}"
BACKEND_DEPLOYMENT="${BACKEND_DEPLOYMENT:-aviation-rag-backend}"

RUNTIME_RESOURCE_GROUP="${RUNTIME_RESOURCE_GROUP:-rg-aviation-rag}"
OPENAI_RESOURCE_GROUP="${OPENAI_RESOURCE_GROUP:-rg-openai}"
ACR_NAME="${ACR_NAME:-avrag705508acr}"
WEBAPP_NAME="${WEBAPP_NAME:-aviation-rag-frontend-705508}"
OPENAI_ACCOUNT_NAME="${OPENAI_ACCOUNT_NAME:-aoaiaviation705508}"
SEARCH_SERVICE_NAME="${SEARCH_SERVICE_NAME:-aisearchozguler}"

failures=0

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "[FAIL] Missing required command: $1" >&2
    exit 1
  fi
}

ok() {
  echo "[PASS] $1"
}

fail() {
  echo "[FAIL] $1"
  failures=$((failures + 1))
}

check_subscription_prefix() {
  local label="$1"
  local resource_id="$2"
  if [[ "${resource_id}" == /subscriptions/${TARGET_SUBSCRIPTION_ID}/* ]]; then
    ok "${label} is in subscription ${TARGET_SUBSCRIPTION_ID}"
  else
    fail "${label} is out-of-policy. id=${resource_id}"
  fi
}

check_value() {
  local label="$1"
  local actual="$2"
  local expected="$3"
  if [ "${actual}" = "${expected}" ]; then
    ok "${label} matches ${expected}"
  else
    fail "${label} mismatch. expected=${expected} actual=${actual}"
  fi
}

require_cmd az
require_cmd kubectl

echo "Validating Azure account context..."
actual_subscription="$(az account show --query id -o tsv 2>/dev/null || true)"
actual_tenant="$(az account show --query tenantId -o tsv 2>/dev/null || true)"
actual_account="$(az account show --query user.name -o tsv 2>/dev/null || true)"

if [ -z "${actual_subscription}" ] || [ -z "${actual_tenant}" ]; then
  echo "[FAIL] Azure CLI is not authenticated." >&2
  echo "Run: az login --tenant ${TARGET_TENANT_ID} && az account set --subscription ${TARGET_SUBSCRIPTION_ID}" >&2
  exit 1
fi

check_value "azure.subscription" "${actual_subscription}" "${TARGET_SUBSCRIPTION_ID}"
check_value "azure.tenant" "${actual_tenant}" "${TARGET_TENANT_ID}"
check_value "azure.account" "${actual_account}" "${TARGET_ACCOUNT_UPN}"

echo "Validating resource groups..."
runtime_rg_id="$(az group show --name "${RUNTIME_RESOURCE_GROUP}" --query id -o tsv 2>/dev/null || true)"
openai_rg_id="$(az group show --name "${OPENAI_RESOURCE_GROUP}" --query id -o tsv 2>/dev/null || true)"
if [ -z "${runtime_rg_id}" ]; then
  fail "Resource group '${RUNTIME_RESOURCE_GROUP}' not found"
else
  check_subscription_prefix "resource_group.${RUNTIME_RESOURCE_GROUP}" "${runtime_rg_id}"
fi
if [ -z "${openai_rg_id}" ]; then
  fail "Resource group '${OPENAI_RESOURCE_GROUP}' not found"
else
  check_subscription_prefix "resource_group.${OPENAI_RESOURCE_GROUP}" "${openai_rg_id}"
fi

echo "Validating core Azure resources..."
aks_id="$(az aks show --resource-group "${AKS_RESOURCE_GROUP}" --name "${AKS_CLUSTER}" --query id -o tsv 2>/dev/null || true)"
aks_tenant="$(az aks show --resource-group "${AKS_RESOURCE_GROUP}" --name "${AKS_CLUSTER}" --query identity.tenantId -o tsv 2>/dev/null || true)"
acr_id="$(az acr show --resource-group "${RUNTIME_RESOURCE_GROUP}" --name "${ACR_NAME}" --query id -o tsv 2>/dev/null || true)"
webapp_id="$(az webapp show --resource-group "${RUNTIME_RESOURCE_GROUP}" --name "${WEBAPP_NAME}" --query id -o tsv 2>/dev/null || true)"
openai_id="$(az cognitiveservices account show --resource-group "${OPENAI_RESOURCE_GROUP}" --name "${OPENAI_ACCOUNT_NAME}" --query id -o tsv 2>/dev/null || true)"
search_id="$(az search service show --resource-group "${OPENAI_RESOURCE_GROUP}" --name "${SEARCH_SERVICE_NAME}" --query id -o tsv 2>/dev/null || true)"

if [ -z "${aks_id}" ]; then
  fail "AKS '${AKS_CLUSTER}' not found in '${AKS_RESOURCE_GROUP}'"
else
  check_subscription_prefix "aks.${AKS_CLUSTER}" "${aks_id}"
fi

if [ -n "${aks_tenant}" ]; then
  check_value "aks.identity.tenantId" "${aks_tenant}" "${TARGET_TENANT_ID}"
fi

if [ -z "${acr_id}" ]; then
  fail "ACR '${ACR_NAME}' not found in '${RUNTIME_RESOURCE_GROUP}'"
else
  check_subscription_prefix "acr.${ACR_NAME}" "${acr_id}"
fi

if [ -z "${webapp_id}" ]; then
  fail "Web App '${WEBAPP_NAME}' not found in '${RUNTIME_RESOURCE_GROUP}'"
else
  check_subscription_prefix "webapp.${WEBAPP_NAME}" "${webapp_id}"
fi

if [ -z "${openai_id}" ]; then
  fail "Azure OpenAI account '${OPENAI_ACCOUNT_NAME}' not found in '${OPENAI_RESOURCE_GROUP}'"
else
  check_subscription_prefix "openai.${OPENAI_ACCOUNT_NAME}" "${openai_id}"
fi

if [ -z "${search_id}" ]; then
  fail "Azure AI Search service '${SEARCH_SERVICE_NAME}' not found in '${OPENAI_RESOURCE_GROUP}'"
else
  check_subscription_prefix "search.${SEARCH_SERVICE_NAME}" "${search_id}"
fi

echo "Validating live AKS runtime guardrail config..."
if ! kubectl get namespace "${AKS_NAMESPACE}" >/dev/null 2>&1; then
  fail "Kubernetes namespace '${AKS_NAMESPACE}' not reachable from current kubectl context"
  echo "Run: ./scripts/aks/use-deploy-target-context.sh"
else
  ok "k8s namespace '${AKS_NAMESPACE}' is reachable"
fi

cm_expected_account="$(kubectl get configmap "${BACKEND_CONFIGMAP}" -n "${AKS_NAMESPACE}" -o jsonpath='{.data.EXPECTED_RUNTIME_ACCOUNT_UPN}' 2>/dev/null || true)"
cm_expected_tenant="$(kubectl get configmap "${BACKEND_CONFIGMAP}" -n "${AKS_NAMESPACE}" -o jsonpath='{.data.EXPECTED_RUNTIME_TENANT_ID}' 2>/dev/null || true)"
cm_expected_subscription="$(kubectl get configmap "${BACKEND_CONFIGMAP}" -n "${AKS_NAMESPACE}" -o jsonpath='{.data.EXPECTED_RUNTIME_SUBSCRIPTION_ID}' 2>/dev/null || true)"
cm_azure_tenant="$(kubectl get configmap "${BACKEND_CONFIGMAP}" -n "${AKS_NAMESPACE}" -o jsonpath='{.data.AZURE_TENANT_ID}' 2>/dev/null || true)"
cm_azure_subscription="$(kubectl get configmap "${BACKEND_CONFIGMAP}" -n "${AKS_NAMESPACE}" -o jsonpath='{.data.AZURE_SUBSCRIPTION_ID}' 2>/dev/null || true)"

if [ -z "${cm_expected_tenant}" ] && [ -z "${cm_expected_subscription}" ]; then
  fail "ConfigMap '${BACKEND_CONFIGMAP}' is missing runtime guardrail keys"
else
  check_value "configmap.EXPECTED_RUNTIME_ACCOUNT_UPN" "${cm_expected_account}" "${TARGET_ACCOUNT_UPN}"
  check_value "configmap.EXPECTED_RUNTIME_TENANT_ID" "${cm_expected_tenant}" "${TARGET_TENANT_ID}"
  check_value "configmap.EXPECTED_RUNTIME_SUBSCRIPTION_ID" "${cm_expected_subscription}" "${TARGET_SUBSCRIPTION_ID}"
  check_value "configmap.AZURE_TENANT_ID" "${cm_azure_tenant}" "${TARGET_TENANT_ID}"
  check_value "configmap.AZURE_SUBSCRIPTION_ID" "${cm_azure_subscription}" "${TARGET_SUBSCRIPTION_ID}"
fi

ready_status="$(kubectl get deployment "${BACKEND_DEPLOYMENT}" -n "${AKS_NAMESPACE}" -o jsonpath='{.status.readyReplicas}/{.spec.replicas}' 2>/dev/null || true)"
if [ -z "${ready_status}" ]; then
  fail "Unable to read deployment status for '${BACKEND_DEPLOYMENT}'"
else
  ok "backend deployment readiness ${ready_status}"
fi

if [ "${failures}" -gt 0 ]; then
  echo ""
  echo "Tenant lock validation FAILED with ${failures} issue(s)."
  exit 1
fi

echo ""
echo "Tenant lock validation passed."
