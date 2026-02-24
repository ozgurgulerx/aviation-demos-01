#!/usr/bin/env bash
# setup-workload-identity.sh
#
# Provisions Azure workload identity for the AKS backend so that
# DefaultAzureCredential can acquire Fabric/Kusto tokens without
# client secrets.
#
# Prerequisites:
#   - az CLI authenticated to the target tenant
#   - kubectl configured for the target AKS cluster
#
# What this script does:
#   1. Enables OIDC issuer + workload identity on the AKS cluster
#   2. Creates a user-assigned managed identity
#   3. Creates a federated credential linking the K8s ServiceAccount
#      to the managed identity
#   4. Prints the client ID to set as FABRIC_WORKLOAD_IDENTITY_CLIENT_ID
#
# After running this script you must still:
#   - Grant the managed identity access to the Fabric workspace / Kusto DB
#     (via Fabric admin portal or az role assignment)
#   - Set FABRIC_WORKLOAD_IDENTITY_CLIENT_ID as a GitHub Actions variable
#   - Redeploy the backend

set -euo pipefail

RESOURCE_GROUP="${RESOURCE_GROUP:-rg-aviation-rag}"
AKS_CLUSTER="${AKS_CLUSTER:-aks-aviation-rag}"
LOCATION="${LOCATION:-westeurope}"
K8S_NAMESPACE="${K8S_NAMESPACE:-aviation-rag}"
K8S_SERVICE_ACCOUNT="${K8S_SERVICE_ACCOUNT:-aviation-rag-backend}"
IDENTITY_NAME="${IDENTITY_NAME:-id-aviation-rag-fabric}"
FEDERATED_CRED_NAME="${FEDERATED_CRED_NAME:-fc-aviation-rag-backend}"

echo "=== Step 1: Enable OIDC issuer and workload identity on AKS ==="
az aks update \
  --resource-group "$RESOURCE_GROUP" \
  --name "$AKS_CLUSTER" \
  --enable-oidc-issuer \
  --enable-workload-identity \
  --only-show-errors

OIDC_ISSUER="$(az aks show \
  --resource-group "$RESOURCE_GROUP" \
  --name "$AKS_CLUSTER" \
  --query "oidcIssuerProfile.issuerUrl" -o tsv)"

if [ -z "$OIDC_ISSUER" ]; then
  echo "ERROR: OIDC issuer URL is empty after enabling. Check AKS cluster status." >&2
  exit 1
fi
echo "OIDC issuer: $OIDC_ISSUER"

echo ""
echo "=== Step 2: Create user-assigned managed identity ==="
az identity create \
  --resource-group "$RESOURCE_GROUP" \
  --name "$IDENTITY_NAME" \
  --location "$LOCATION" \
  --only-show-errors 2>/dev/null || true

IDENTITY_CLIENT_ID="$(az identity show \
  --resource-group "$RESOURCE_GROUP" \
  --name "$IDENTITY_NAME" \
  --query clientId -o tsv)"

IDENTITY_RESOURCE_ID="$(az identity show \
  --resource-group "$RESOURCE_GROUP" \
  --name "$IDENTITY_NAME" \
  --query id -o tsv)"

echo "Identity client ID: $IDENTITY_CLIENT_ID"
echo "Identity resource ID: $IDENTITY_RESOURCE_ID"

echo ""
echo "=== Step 3: Create federated credential ==="
az identity federated-credential create \
  --name "$FEDERATED_CRED_NAME" \
  --identity-name "$IDENTITY_NAME" \
  --resource-group "$RESOURCE_GROUP" \
  --issuer "$OIDC_ISSUER" \
  --subject "system:serviceaccount:${K8S_NAMESPACE}:${K8S_SERVICE_ACCOUNT}" \
  --audience "api://AzureADTokenExchange" \
  --only-show-errors 2>/dev/null || true

echo ""
echo "=== Done ==="
echo ""
echo "Managed identity client ID (set as FABRIC_WORKLOAD_IDENTITY_CLIENT_ID):"
echo "  $IDENTITY_CLIENT_ID"
echo ""
echo "Next steps:"
echo "  1. Grant this identity access to your Fabric workspace / Kusto database"
echo "  2. Set the GitHub Actions variable:"
echo "     gh variable set FABRIC_WORKLOAD_IDENTITY_CLIENT_ID --body '$IDENTITY_CLIENT_ID'"
echo "  3. Redeploy the backend"
