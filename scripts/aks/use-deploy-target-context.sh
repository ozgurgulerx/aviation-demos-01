#!/usr/bin/env bash
set -euo pipefail

AZURE_SUBSCRIPTION_ID="${AZURE_SUBSCRIPTION_ID:-6a539906-6ce2-4e3b-84ee-89f701de18d8}"
AZURE_TENANT_ID="${AZURE_TENANT_ID:-52095a81-130f-4b06-83f1-9859b2c73de6}"
AKS_RESOURCE_GROUP="${AKS_RESOURCE_GROUP:-rg-aviation-rag}"
AKS_CLUSTER="${AKS_CLUSTER:-aks-aviation-rag}"
AKS_NAMESPACE="${AKS_NAMESPACE:-aviation-rag}"
BACKEND_DEPLOYMENT="${BACKEND_DEPLOYMENT:-aviation-rag-backend}"

echo "Switching Azure subscription to target: ${AZURE_SUBSCRIPTION_ID}"
az account set --subscription "${AZURE_SUBSCRIPTION_ID}"

actual_subscription="$(az account show --query id -o tsv)"
actual_tenant="$(az account show --query tenantId -o tsv)"

if [ "${actual_subscription}" != "${AZURE_SUBSCRIPTION_ID}" ]; then
  echo "ERROR: Active subscription '${actual_subscription}' does not match expected '${AZURE_SUBSCRIPTION_ID}'." >&2
  exit 1
fi

if [ "${actual_tenant}" != "${AZURE_TENANT_ID}" ]; then
  echo "ERROR: Active tenant '${actual_tenant}' does not match expected '${AZURE_TENANT_ID}'." >&2
  exit 1
fi

aks_id="$(az aks show --resource-group "${AKS_RESOURCE_GROUP}" --name "${AKS_CLUSTER}" --query id -o tsv)"
if [ -z "${aks_id}" ]; then
  echo "ERROR: Could not resolve AKS cluster '${AKS_CLUSTER}' in resource group '${AKS_RESOURCE_GROUP}'." >&2
  exit 1
fi

echo "Target AKS: ${aks_id}"
az aks get-credentials --resource-group "${AKS_RESOURCE_GROUP}" --name "${AKS_CLUSTER}" --overwrite-existing

kubectl config set-context --current --namespace="${AKS_NAMESPACE}" >/dev/null

current_context="$(kubectl config current-context)"
current_cluster="$(kubectl config view --minify -o jsonpath='{.contexts[0].context.cluster}')"
current_server="$(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}')"
current_namespace="$(kubectl config view --minify -o jsonpath='{.contexts[0].context.namespace}')"

if [ -z "${current_namespace}" ]; then
  current_namespace="default"
fi

echo ""
echo "Active kubectl target"
echo "  context: ${current_context}"
echo "  cluster: ${current_cluster}"
echo "  server: ${current_server}"
echo "  namespace: ${current_namespace}"

echo ""
echo "Backend deployment status"
kubectl get deployment "${BACKEND_DEPLOYMENT}" -n "${AKS_NAMESPACE}" \
  -o jsonpath='name={.metadata.name} image={.spec.template.spec.containers[0].image} ready={.status.readyReplicas}/{.spec.replicas}{"\n"}'
