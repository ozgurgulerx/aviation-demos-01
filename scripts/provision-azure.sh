#!/usr/bin/env bash
#
# provision-azure.sh — Provision runtime infra for Aviation RAG migration
#
# Usage:
#   ./scripts/provision-azure.sh
#
# All settings can be overridden via environment variables.
#
set -euo pipefail

bool_true() {
  case "${1:-}" in
    1|true|TRUE|yes|YES|y|Y) return 0 ;;
    *) return 1 ;;
  esac
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Required command not found: $1" >&2
    exit 1
  fi
}

require_cmd az
require_cmd kubectl
require_cmd envsubst

CURRENT_SUBSCRIPTION="$(az account show --query id -o tsv)"
CURRENT_TENANT="$(az account show --query tenantId -o tsv)"

APP_NAME="${APP_NAME:-aviation-rag}"
LOCATION="${LOCATION:-westeurope}"
SUBSCRIPTION_ID="${SUBSCRIPTION_ID:-$CURRENT_SUBSCRIPTION}"
RESOURCE_GROUP="${RESOURCE_GROUP:-rg-aviation-rag}"

# Runtime infra names
VNET_NAME="${VNET_NAME:-vnet-aviation-rag}"
AKS_NAME="${AKS_NAME:-aks-aviation-rag}"
ACR_NAME="${ACR_NAME:-aviationragacr}"
ACR_RESOURCE_GROUP="${ACR_RESOURCE_GROUP:-$RESOURCE_GROUP}"
APP_SERVICE_PLAN="${APP_SERVICE_PLAN:-plan-aviation-rag-frontend}"
APP_SERVICE_PLAN_SKU="${APP_SERVICE_PLAN_SKU:-P1V3}"
WEBAPP_NAME="${WEBAPP_NAME:-aviation-rag-frontend}"
K8S_NAMESPACE="${K8S_NAMESPACE:-aviation-rag}"
IMAGE_NAME="${IMAGE_NAME:-aviation-rag-backend}"

# Network settings
VNET_CIDR="${VNET_CIDR:-10.0.0.0/16}"
AKS_SUBNET_CIDR="${AKS_SUBNET_CIDR:-10.0.0.0/22}"
APP_SERVICE_SUBNET_CIDR="${APP_SERVICE_SUBNET_CIDR:-10.0.4.0/24}"
PRIVATE_ENDPOINT_SUBNET_CIDR="${PRIVATE_ENDPOINT_SUBNET_CIDR:-10.0.5.0/24}"
AKS_SERVICE_CIDR="${AKS_SERVICE_CIDR:-10.1.0.0/16}"
AKS_DNS_IP="${AKS_DNS_IP:-10.1.0.10}"

# PostgreSQL settings
PG_SERVER_RG="${PG_SERVER_RG:-$RESOURCE_GROUP}"
PG_SERVER="${PG_SERVER:-aviationragpg}"
PG_DATABASE="${PG_DATABASE:-aviationrag}"
PG_READONLY_USER="${PG_READONLY_USER:-aviationrag_readonly}"
PG_PORT="${PG_PORT:-5432}"
CREATE_PG_SERVER="${CREATE_PG_SERVER:-false}"
CREATE_PG_DATABASE="${CREATE_PG_DATABASE:-true}"
ENABLE_POSTGRES_PRIVATE_ENDPOINT="${ENABLE_POSTGRES_PRIVATE_ENDPOINT:-true}"
PG_ADMIN_USER="${PG_ADMIN_USER:-}"
PG_ADMIN_PASSWORD="${PG_ADMIN_PASSWORD:-}"
PG_SKU="${PG_SKU:-Standard_B2s}"
PG_VERSION="${PG_VERSION:-17}"

# Application runtime endpoints (required for rendered manifests)
AZURE_OPENAI_ENDPOINT="${AZURE_OPENAI_ENDPOINT:-}"
AZURE_OPENAI_DEPLOYMENT_NAME="${AZURE_OPENAI_DEPLOYMENT_NAME:-aviation-chat-gpt5-mini}"
AZURE_TEXT_EMBEDDING_DEPLOYMENT_NAME="${AZURE_TEXT_EMBEDDING_DEPLOYMENT_NAME:-text-embedding-3-small}"
AZURE_OPENAI_VOICE_DEPLOYMENT_NAME="${AZURE_OPENAI_VOICE_DEPLOYMENT_NAME:-aviation-voice-tts}"
AZURE_OPENAI_VOICE_API_VERSION="${AZURE_OPENAI_VOICE_API_VERSION:-2025-03-01-preview}"
AZURE_OPENAI_VOICE_TURKISH="${AZURE_OPENAI_VOICE_TURKISH:-alloy}"
AZURE_OPENAI_VOICE_ENGLISH="${AZURE_OPENAI_VOICE_ENGLISH:-alloy}"
AZURE_OPENAI_AUTH_MODE="${AZURE_OPENAI_AUTH_MODE:-token}"
AZURE_OPENAI_MANAGED_IDENTITY_CLIENT_ID="${AZURE_OPENAI_MANAGED_IDENTITY_CLIENT_ID:-}"
AZURE_OPENAI_TENANT_ID="${AZURE_OPENAI_TENANT_ID:-}"
AZURE_OPENAI_CLIENT_ID="${AZURE_OPENAI_CLIENT_ID:-}"
AZURE_OPENAI_CLIENT_SECRET="${AZURE_OPENAI_CLIENT_SECRET:-}"
AZURE_SEARCH_ENDPOINT="${AZURE_SEARCH_ENDPOINT:-}"
PII_ENDPOINT="${PII_ENDPOINT:-}"
PII_CONTAINER_ENDPOINT="${PII_CONTAINER_ENDPOINT:-$PII_ENDPOINT}"
BACKEND_URL="${BACKEND_URL:-http://10.0.0.10}"

# Backend runtime config
FLASK_ENV="${FLASK_ENV:-production}"
LOG_LEVEL="${LOG_LEVEL:-INFO}"
USE_POSTGRES="${USE_POSTGRES:-true}"
OTEL_SERVICE_NAME="${OTEL_SERVICE_NAME:-aviation-rag-backend}"
ENVIRONMENT="${ENVIRONMENT:-production}"
AF_SESSION_TTL_SECONDS="${AF_SESSION_TTL_SECONDS:-3600}"
AF_MAX_SESSIONS="${AF_MAX_SESSIONS:-500}"
FABRIC_KQL_ENDPOINT="${FABRIC_KQL_ENDPOINT:-}"
FABRIC_GRAPH_ENDPOINT="${FABRIC_GRAPH_ENDPOINT:-}"
FABRIC_NOSQL_ENDPOINT="${FABRIC_NOSQL_ENDPOINT:-}"

# Optional behavior
CREATE_ACR="${CREATE_ACR:-true}"
DEPLOY_K8S="${DEPLOY_K8S:-true}"
SETUP_GITHUB_OIDC="${SETUP_GITHUB_OIDC:-true}"

# GitHub OIDC
GITHUB_ORG="${GITHUB_ORG:-ozgurgulerx}"
GITHUB_REPO="${GITHUB_REPO:-aviation-demos-01}"
OIDC_APP_NAME="${OIDC_APP_NAME:-github-aviation-rag-deploy}"
OIDC_FED_CRED_NAME="${OIDC_FED_CRED_NAME:-github-aviation-main}"

echo "============================================="
echo " Aviation RAG — Runtime Provisioning"
echo "============================================="
echo "Target subscription : ${SUBSCRIPTION_ID}"
echo "Target tenant       : ${CURRENT_TENANT}"
echo "Resource group      : ${RESOURCE_GROUP}"
echo "Location            : ${LOCATION}"
echo

az account set --subscription "$SUBSCRIPTION_ID"

AKS_SUBNET_ID="/subscriptions/${SUBSCRIPTION_ID}/resourceGroups/${RESOURCE_GROUP}/providers/Microsoft.Network/virtualNetworks/${VNET_NAME}/subnets/subnet-aks"
PG_RESOURCE_ID="/subscriptions/${SUBSCRIPTION_ID}/resourceGroups/${PG_SERVER_RG}/providers/Microsoft.DBforPostgreSQL/flexibleServers/${PG_SERVER}"

# Section 1: Resource Group
if az group show -n "$RESOURCE_GROUP" >/dev/null 2>&1; then
  echo "[1/10] Resource group exists: ${RESOURCE_GROUP}"
else
  az group create --name "$RESOURCE_GROUP" --location "$LOCATION" -o none
  echo "[1/10] Created resource group: ${RESOURCE_GROUP}"
fi

# Section 2: VNet + subnets
if az network vnet show -g "$RESOURCE_GROUP" -n "$VNET_NAME" >/dev/null 2>&1; then
  echo "[2/10] VNet exists: ${VNET_NAME}"
else
  az network vnet create --resource-group "$RESOURCE_GROUP" --name "$VNET_NAME" --address-prefix "$VNET_CIDR" -o none
  echo "[2/10] Created VNet: ${VNET_NAME}"
fi

if ! az network vnet subnet show -g "$RESOURCE_GROUP" --vnet-name "$VNET_NAME" -n subnet-aks >/dev/null 2>&1; then
  az network vnet subnet create --resource-group "$RESOURCE_GROUP" --vnet-name "$VNET_NAME" --name subnet-aks --address-prefix "$AKS_SUBNET_CIDR" -o none
fi
if ! az network vnet subnet show -g "$RESOURCE_GROUP" --vnet-name "$VNET_NAME" -n subnet-appservice >/dev/null 2>&1; then
  az network vnet subnet create --resource-group "$RESOURCE_GROUP" --vnet-name "$VNET_NAME" --name subnet-appservice --address-prefix "$APP_SERVICE_SUBNET_CIDR" --delegations Microsoft.Web/serverFarms -o none
fi
if ! az network vnet subnet show -g "$RESOURCE_GROUP" --vnet-name "$VNET_NAME" -n subnet-privateendpoint >/dev/null 2>&1; then
  az network vnet subnet create --resource-group "$RESOURCE_GROUP" --vnet-name "$VNET_NAME" --name subnet-privateendpoint --address-prefix "$PRIVATE_ENDPOINT_SUBNET_CIDR" --disable-private-endpoint-network-policies true -o none
fi

echo "[2/10] Subnets ready"

# Section 3: ACR
if bool_true "$CREATE_ACR"; then
  if az acr show -g "$ACR_RESOURCE_GROUP" -n "$ACR_NAME" >/dev/null 2>&1; then
    echo "[3/10] ACR exists: ${ACR_NAME}"
  else
    az acr create --resource-group "$ACR_RESOURCE_GROUP" --name "$ACR_NAME" --sku Basic --location "$LOCATION" -o none
    echo "[3/10] Created ACR: ${ACR_NAME}"
  fi
else
  echo "[3/10] Skipping ACR creation (CREATE_ACR=${CREATE_ACR})"
fi
ACR_LOGIN_SERVER="$(az acr show -g "$ACR_RESOURCE_GROUP" -n "$ACR_NAME" --query loginServer -o tsv)"

# Section 4: AKS
if az aks show -g "$RESOURCE_GROUP" -n "$AKS_NAME" >/dev/null 2>&1; then
  echo "[4/10] AKS exists: ${AKS_NAME}"
else
  az aks create \
    --resource-group "$RESOURCE_GROUP" \
    --name "$AKS_NAME" \
    --node-count 1 \
    --enable-cluster-autoscaler \
    --min-count 1 \
    --max-count 2 \
    --node-vm-size Standard_D2als_v6 \
    --network-plugin azure \
    --vnet-subnet-id "$AKS_SUBNET_ID" \
    --service-cidr "$AKS_SERVICE_CIDR" \
    --dns-service-ip "$AKS_DNS_IP" \
    --generate-ssh-keys \
    --attach-acr "$ACR_NAME" \
    -o none
  echo "[4/10] Created AKS: ${AKS_NAME}"
fi
az aks get-credentials --resource-group "$RESOURCE_GROUP" --name "$AKS_NAME" --overwrite-existing

# Section 5: PostgreSQL server/database
if ! az postgres flexible-server show -g "$PG_SERVER_RG" -n "$PG_SERVER" >/dev/null 2>&1; then
  if bool_true "$CREATE_PG_SERVER"; then
    if [ -z "$PG_ADMIN_USER" ] || [ -z "$PG_ADMIN_PASSWORD" ]; then
      echo "PG_ADMIN_USER and PG_ADMIN_PASSWORD are required when CREATE_PG_SERVER=true" >&2
      exit 1
    fi
    az postgres flexible-server create \
      --resource-group "$PG_SERVER_RG" \
      --name "$PG_SERVER" \
      --location "$LOCATION" \
      --admin-user "$PG_ADMIN_USER" \
      --admin-password "$PG_ADMIN_PASSWORD" \
      --sku-name "$PG_SKU" \
      --version "$PG_VERSION" \
      -o none
    echo "[5/10] Created PostgreSQL server: ${PG_SERVER}"
  else
    echo "PostgreSQL server not found: ${PG_SERVER_RG}/${PG_SERVER}" >&2
    echo "Set CREATE_PG_SERVER=true (and admin creds) or point PG_SERVER/PG_SERVER_RG to an existing server." >&2
    exit 1
  fi
else
  echo "[5/10] PostgreSQL server exists: ${PG_SERVER}"
fi

if bool_true "$CREATE_PG_DATABASE"; then
  if az postgres flexible-server db show --resource-group "$PG_SERVER_RG" --server-name "$PG_SERVER" --database-name "$PG_DATABASE" >/dev/null 2>&1; then
    echo "[5/10] PostgreSQL DB exists: ${PG_DATABASE}"
  else
    az postgres flexible-server db create --resource-group "$PG_SERVER_RG" --server-name "$PG_SERVER" --database-name "$PG_DATABASE" -o none
    echo "[5/10] Created PostgreSQL DB: ${PG_DATABASE}"
  fi
else
  echo "[5/10] Skipping PostgreSQL DB create (CREATE_PG_DATABASE=${CREATE_PG_DATABASE})"
fi

# Section 6: PostgreSQL private endpoint + DNS
if bool_true "$ENABLE_POSTGRES_PRIVATE_ENDPOINT"; then
  PE_NAME="pe-postgres-${APP_NAME}"
  DNS_ZONE="privatelink.postgres.database.azure.com"

  if ! az network private-endpoint show -g "$RESOURCE_GROUP" -n "$PE_NAME" >/dev/null 2>&1; then
    az network private-endpoint create \
      --resource-group "$RESOURCE_GROUP" \
      --name "$PE_NAME" \
      --vnet-name "$VNET_NAME" \
      --subnet subnet-privateendpoint \
      --private-connection-resource-id "$PG_RESOURCE_ID" \
      --group-id postgresqlServer \
      --connection-name "postgres-connection-${APP_NAME}" \
      -o none
  fi

  if ! az network private-dns zone show -g "$RESOURCE_GROUP" -n "$DNS_ZONE" >/dev/null 2>&1; then
    az network private-dns zone create --resource-group "$RESOURCE_GROUP" --name "$DNS_ZONE" -o none
  fi

  if ! az network private-dns link vnet show -g "$RESOURCE_GROUP" -z "$DNS_ZONE" -n postgres-dns-link >/dev/null 2>&1; then
    az network private-dns link vnet create \
      --resource-group "$RESOURCE_GROUP" \
      --zone-name "$DNS_ZONE" \
      --name postgres-dns-link \
      --virtual-network "$VNET_NAME" \
      --registration-enabled false \
      -o none
  fi

  PRIVATE_IP="$(az network private-endpoint show --resource-group "$RESOURCE_GROUP" --name "$PE_NAME" --query 'customDnsConfigs[0].ipAddresses[0]' -o tsv)"
  if ! az network private-dns record-set a show -g "$RESOURCE_GROUP" -z "$DNS_ZONE" -n "$PG_SERVER" >/dev/null 2>&1; then
    az network private-dns record-set a create --resource-group "$RESOURCE_GROUP" --zone-name "$DNS_ZONE" --name "$PG_SERVER" -o none
  fi
  EXISTING_A="$(az network private-dns record-set a show -g "$RESOURCE_GROUP" -z "$DNS_ZONE" -n "$PG_SERVER" --query 'aRecords[0].ipv4Address' -o tsv 2>/dev/null || true)"
  if [ -z "$EXISTING_A" ] && [ -n "$PRIVATE_IP" ]; then
    az network private-dns record-set a add-record --resource-group "$RESOURCE_GROUP" --zone-name "$DNS_ZONE" --record-set-name "$PG_SERVER" --ipv4-address "$PRIVATE_IP" -o none
  fi

  echo "[6/10] PostgreSQL private endpoint and DNS ready"
else
  echo "[6/10] Skipping PostgreSQL private endpoint setup"
fi

# Section 7: App Service
if az appservice plan show -g "$RESOURCE_GROUP" -n "$APP_SERVICE_PLAN" >/dev/null 2>&1; then
  echo "[7/10] App Service plan exists: ${APP_SERVICE_PLAN}"
else
  az appservice plan create --resource-group "$RESOURCE_GROUP" --name "$APP_SERVICE_PLAN" --sku "$APP_SERVICE_PLAN_SKU" --is-linux -o none
  echo "[7/10] Created App Service plan: ${APP_SERVICE_PLAN}"
fi

if az webapp show -g "$RESOURCE_GROUP" -n "$WEBAPP_NAME" >/dev/null 2>&1; then
  echo "[7/10] Web App exists: ${WEBAPP_NAME}"
else
  az webapp create --resource-group "$RESOURCE_GROUP" --plan "$APP_SERVICE_PLAN" --name "$WEBAPP_NAME" --runtime "NODE:20-lts" -o none
  echo "[7/10] Created Web App: ${WEBAPP_NAME}"
fi

az webapp identity assign --resource-group "$RESOURCE_GROUP" --name "$WEBAPP_NAME" -o none

az webapp vnet-integration add --resource-group "$RESOURCE_GROUP" --name "$WEBAPP_NAME" --vnet "$VNET_NAME" --subnet subnet-appservice -o none 2>/dev/null || true
az webapp config appsettings set \
  --resource-group "$RESOURCE_GROUP" \
  --name "$WEBAPP_NAME" \
  --settings \
    BACKEND_URL="$BACKEND_URL" \
    PII_ENDPOINT="$PII_ENDPOINT" \
    PII_CONTAINER_ENDPOINT="$PII_CONTAINER_ENDPOINT" \
    AZURE_OPENAI_ENDPOINT="$AZURE_OPENAI_ENDPOINT" \
    AZURE_OPENAI_VOICE_DEPLOYMENT_NAME="$AZURE_OPENAI_VOICE_DEPLOYMENT_NAME" \
    AZURE_OPENAI_VOICE_API_VERSION="$AZURE_OPENAI_VOICE_API_VERSION" \
    AZURE_OPENAI_VOICE_TURKISH="$AZURE_OPENAI_VOICE_TURKISH" \
    AZURE_OPENAI_VOICE_ENGLISH="$AZURE_OPENAI_VOICE_ENGLISH" \
    AZURE_OPENAI_AUTH_MODE="$AZURE_OPENAI_AUTH_MODE" \
    AZURE_OPENAI_MANAGED_IDENTITY_CLIENT_ID="$AZURE_OPENAI_MANAGED_IDENTITY_CLIENT_ID" \
    AZURE_OPENAI_TENANT_ID="$AZURE_OPENAI_TENANT_ID" \
    AZURE_OPENAI_CLIENT_ID="$AZURE_OPENAI_CLIENT_ID" \
    AZURE_OPENAI_CLIENT_SECRET="$AZURE_OPENAI_CLIENT_SECRET" \
    WEBSITE_VNET_ROUTE_ALL=1 \
    PORT=3000 \
    WEBSITES_PORT=3000 \
  -o none
az webapp config set --resource-group "$RESOURCE_GROUP" --name "$WEBAPP_NAME" --startup-file "node server.js" -o none

echo "[7/10] App Service configured"

# Section 8: Render/apply k8s runtime manifests
if bool_true "$DEPLOY_K8S"; then
  for req in AZURE_OPENAI_ENDPOINT AZURE_SEARCH_ENDPOINT PII_ENDPOINT PII_CONTAINER_ENDPOINT; do
    if [ -z "${!req:-}" ]; then
      echo "${req} is required when DEPLOY_K8S=true" >&2
      exit 1
    fi
  done

  export AZURE_CONTAINER_REGISTRY="$ACR_LOGIN_SERVER"
  export IMAGE_NAME
  export K8S_NAMESPACE
  export AZURE_OPENAI_ENDPOINT
  export AZURE_OPENAI_DEPLOYMENT_NAME
  export AZURE_TEXT_EMBEDDING_DEPLOYMENT_NAME
  export AZURE_SEARCH_ENDPOINT
  export PII_ENDPOINT
  export PII_CONTAINER_ENDPOINT
  export PGHOST="${PG_SERVER}.postgres.database.azure.com"
  export PGPORT="$PG_PORT"
  export PGDATABASE="$PG_DATABASE"
  export PGUSER="$PG_READONLY_USER"
  export FLASK_ENV
  export LOG_LEVEL
  export USE_POSTGRES
  export OTEL_SERVICE_NAME
  export ENVIRONMENT
  export AF_SESSION_TTL_SECONDS
  export AF_MAX_SESSIONS
  export FABRIC_KQL_ENDPOINT
  export FABRIC_GRAPH_ENDPOINT
  export FABRIC_NOSQL_ENDPOINT

  ./scripts/render-k8s-manifests.sh /tmp/k8s-rendered

  kubectl apply -f /tmp/k8s-rendered/namespace.yaml
  kubectl apply -f /tmp/k8s-rendered/backend-service.yaml
  kubectl apply -f /tmp/k8s-rendered/backend-configmap.yaml
  kubectl apply -f /tmp/k8s-rendered/backend-deployment.yaml

  ACR_USERNAME="$(az acr credential show --resource-group "$ACR_RESOURCE_GROUP" --name "$ACR_NAME" --query username -o tsv)"
  ACR_PASSWORD="$(az acr credential show --resource-group "$ACR_RESOURCE_GROUP" --name "$ACR_NAME" --query 'passwords[0].value' -o tsv)"
  kubectl create secret docker-registry acr-secret \
    --namespace "$K8S_NAMESPACE" \
    --docker-server "$ACR_LOGIN_SERVER" \
    --docker-username "$ACR_USERNAME" \
    --docker-password "$ACR_PASSWORD" \
    --dry-run=client -o yaml | kubectl apply -f -

  echo "[8/10] Kubernetes manifests applied"
else
  echo "[8/10] Skipping Kubernetes deployment"
fi

# Section 9: GitHub OIDC app + federated credential
if bool_true "$SETUP_GITHUB_OIDC"; then
  APP_ID="$(az ad app list --display-name "$OIDC_APP_NAME" --query '[0].appId' -o tsv 2>/dev/null || true)"

  if [ -z "$APP_ID" ]; then
    az ad app create --display-name "$OIDC_APP_NAME" -o none
    APP_ID="$(az ad app list --display-name "$OIDC_APP_NAME" --query '[0].appId' -o tsv)"
    az ad sp create --id "$APP_ID" -o none
  fi

  OBJECT_ID="$(az ad app list --display-name "$OIDC_APP_NAME" --query '[0].id' -o tsv)"

  az role assignment create \
    --assignee "$APP_ID" \
    --role Contributor \
    --scope "/subscriptions/${SUBSCRIPTION_ID}/resourceGroups/${RESOURCE_GROUP}" \
    -o none 2>/dev/null || true

  EXISTING_CRED="$(az ad app federated-credential list --id "$OBJECT_ID" --query "[?name=='${OIDC_FED_CRED_NAME}'].name" -o tsv 2>/dev/null || true)"
  if [ "$EXISTING_CRED" != "$OIDC_FED_CRED_NAME" ]; then
    az ad app federated-credential create --id "$OBJECT_ID" --parameters "{
      \"name\": \"${OIDC_FED_CRED_NAME}\",
      \"issuer\": \"https://token.actions.githubusercontent.com\",
      \"subject\": \"repo:${GITHUB_ORG}/${GITHUB_REPO}:ref:refs/heads/main\",
      \"audiences\": [\"api://AzureADTokenExchange\"]
    }" -o none
  fi

  echo "[9/10] GitHub OIDC configured"
else
  APP_ID=""
  echo "[9/10] Skipping GitHub OIDC setup"
fi

# Section 10: Summary
TENANT_ID="$(az account show --query tenantId -o tsv)"

echo
echo "============================================="
echo " Runtime Provisioning Complete"
echo "============================================="
echo "Resource Group : ${RESOURCE_GROUP}"
echo "AKS Cluster    : ${AKS_NAME}"
echo "ACR            : ${ACR_LOGIN_SERVER}"
echo "Web App        : https://${WEBAPP_NAME}.azurewebsites.net"
echo "PostgreSQL     : ${PG_SERVER}.postgres.database.azure.com / ${PG_DATABASE}"
echo "PII Endpoint   : ${PII_ENDPOINT}"
echo

echo "GitHub secrets to set/update:"
if [ -n "$APP_ID" ]; then
  echo "  AZURE_CLIENT_ID       = ${APP_ID}"
fi
echo "  AZURE_TENANT_ID       = ${TENANT_ID}"
echo "  AZURE_SUBSCRIPTION_ID = ${SUBSCRIPTION_ID}"
echo "  AZURE_OPENAI_API_KEY  = <value>"
echo "  AZURE_OPENAI_CLIENT_SECRET = <optional: Entra client secret for voice token auth>"
echo "  AZURE_SEARCH_ADMIN_KEY= <value>"
echo "  PGPASSWORD            = <${PG_READONLY_USER} password>"
echo
echo "GitHub repository variables to set/update:"
echo "  AZURE_RESOURCE_GROUP=${RESOURCE_GROUP}"
echo "  AKS_RESOURCE_GROUP=${RESOURCE_GROUP}"
echo "  AKS_CLUSTER=${AKS_NAME}"
echo "  AKS_NAMESPACE=${K8S_NAMESPACE}"
echo "  AZURE_WEBAPP_NAME=${WEBAPP_NAME}"
echo "  AZURE_CONTAINER_REGISTRY_NAME=${ACR_NAME}"
echo "  AZURE_CONTAINER_REGISTRY=${ACR_LOGIN_SERVER}"
echo "  BACKEND_URL=${BACKEND_URL}"
echo "  PII_ENDPOINT=${PII_ENDPOINT}"
echo "  PII_CONTAINER_ENDPOINT=${PII_CONTAINER_ENDPOINT}"
echo "  PG_SERVER_NAME=${PG_SERVER}"
echo "  PG_RESOURCE_GROUP=${PG_SERVER_RG}"
echo "  PGHOST=${PG_SERVER}.postgres.database.azure.com"
echo "  PGPORT=${PG_PORT}"
echo "  PGDATABASE=${PG_DATABASE}"
echo "  PGUSER=${PG_READONLY_USER}"
echo "  AZURE_OPENAI_ENDPOINT=${AZURE_OPENAI_ENDPOINT}"
echo "  AZURE_OPENAI_DEPLOYMENT_NAME=${AZURE_OPENAI_DEPLOYMENT_NAME}"
echo "  AZURE_OPENAI_VOICE_DEPLOYMENT_NAME=${AZURE_OPENAI_VOICE_DEPLOYMENT_NAME}"
echo "  AZURE_OPENAI_VOICE_API_VERSION=${AZURE_OPENAI_VOICE_API_VERSION}"
echo "  AZURE_OPENAI_VOICE_TURKISH=${AZURE_OPENAI_VOICE_TURKISH}"
echo "  AZURE_OPENAI_VOICE_ENGLISH=${AZURE_OPENAI_VOICE_ENGLISH}"
echo "  AZURE_OPENAI_AUTH_MODE=${AZURE_OPENAI_AUTH_MODE}"
echo "  AZURE_OPENAI_MANAGED_IDENTITY_CLIENT_ID=${AZURE_OPENAI_MANAGED_IDENTITY_CLIENT_ID}"
echo "  AZURE_OPENAI_TENANT_ID=${AZURE_OPENAI_TENANT_ID}"
echo "  AZURE_OPENAI_CLIENT_ID=${AZURE_OPENAI_CLIENT_ID}"
echo "  AZURE_SEARCH_ENDPOINT=${AZURE_SEARCH_ENDPOINT}"
echo

echo "Next steps:"
echo "  1) Create/update backend Kubernetes secret 'backend-secrets' with PGPASSWORD and API keys."
echo "  2) Run backend workflow, then frontend workflow."
echo "  3) Verify: /health, /api/health, /api/pii, and chat streaming."
