#!/usr/bin/env bash
#
# Bootstrap an Entra app + service principal for Fabric API usage.
# Uses Microsoft Graph with a tenant-scoped token and does not run `az login`.
#
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  scripts/fabric/bootstrap-sp.sh --tenant-id <tenant-guid> [options]

Options:
  --tenant-id <guid>           Required. Entra tenant ID.
  --display-name <name>        App display name (default: aviation-rag-fabric-sp).
  --secret-name <name>         Secret display name (default: fabric-api-secret).
  --secret-days <days>         Secret validity in days (default: 365).
  --env-out <path>             Optional file to write exported env vars.
  -h, --help                   Show help.

Requirements:
  - Azure CLI authenticated with directory rights in target tenant.
  - jq, curl, python3
EOF
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1" >&2
    exit 1
  fi
}

require_cmd "az"
require_cmd "jq"
require_cmd "curl"
require_cmd "python3"

TENANT_ID=""
DISPLAY_NAME="aviation-rag-fabric-sp"
SECRET_NAME="fabric-api-secret"
SECRET_DAYS="365"
ENV_OUT=""

while [ "$#" -gt 0 ]; do
  case "$1" in
    --tenant-id)
      TENANT_ID="${2:-}"
      shift 2
      ;;
    --display-name)
      DISPLAY_NAME="${2:-}"
      shift 2
      ;;
    --secret-name)
      SECRET_NAME="${2:-}"
      shift 2
      ;;
    --secret-days)
      SECRET_DAYS="${2:-}"
      shift 2
      ;;
    --env-out)
      ENV_OUT="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [ -z "${TENANT_ID}" ]; then
  echo "--tenant-id is required." >&2
  usage >&2
  exit 1
fi

if ! [[ "${SECRET_DAYS}" =~ ^[0-9]+$ ]]; then
  echo "--secret-days must be a positive integer." >&2
  exit 1
fi

APP_FILTER_ENCODED="$(
  python3 - "${DISPLAY_NAME}" <<'PY'
import sys
from urllib.parse import quote
name = sys.argv[1]
flt = f"displayName eq '{name}'"
print(quote(flt, safe=""))
PY
)"

GRAPH_TOKEN="$(az account get-access-token \
  --tenant "${TENANT_ID}" \
  --resource-type ms-graph \
  --query accessToken \
  -o tsv)"

if [ -z "${GRAPH_TOKEN}" ]; then
  echo "Unable to acquire Microsoft Graph token for tenant ${TENANT_ID}." >&2
  exit 1
fi

graph_get() {
  local url="$1"
  curl -sS --request GET "${url}" \
    --header "Authorization: Bearer ${GRAPH_TOKEN}" \
    --header "Content-Type: application/json"
}

graph_post() {
  local url="$1"
  local body="$2"
  curl -sS --request POST "${url}" \
    --header "Authorization: Bearer ${GRAPH_TOKEN}" \
    --header "Content-Type: application/json" \
    --data "${body}"
}

echo "Checking for existing app registration: ${DISPLAY_NAME}"
APP_RESULT="$(graph_get "https://graph.microsoft.com/v1.0/applications?\$filter=${APP_FILTER_ENCODED}&\$select=id,appId,displayName")"
APP_COUNT="$(echo "${APP_RESULT}" | jq '.value | length')"

if [ "${APP_COUNT}" -gt 0 ]; then
  APP_OBJECT_ID="$(echo "${APP_RESULT}" | jq -r '.value[0].id')"
  APP_ID="$(echo "${APP_RESULT}" | jq -r '.value[0].appId')"
  echo "Found existing app: appId=${APP_ID}"
else
  CREATE_APP_BODY="$(jq -n --arg name "${DISPLAY_NAME}" '{
    displayName: $name,
    signInAudience: "AzureADMyOrg"
  }')"
  CREATE_APP_RESULT="$(graph_post "https://graph.microsoft.com/v1.0/applications" "${CREATE_APP_BODY}")"

  if echo "${CREATE_APP_RESULT}" | jq -e '.error' >/dev/null 2>&1; then
    echo "Failed to create app registration:" >&2
    echo "${CREATE_APP_RESULT}" | jq >&2
    exit 1
  fi

  APP_OBJECT_ID="$(echo "${CREATE_APP_RESULT}" | jq -r '.id')"
  APP_ID="$(echo "${CREATE_APP_RESULT}" | jq -r '.appId')"
  echo "Created app registration: appId=${APP_ID}"
fi

echo "Checking for existing service principal..."
SP_RESULT="$(graph_get "https://graph.microsoft.com/v1.0/servicePrincipals?\$filter=appId%20eq%20'${APP_ID}'&\$select=id,appId,displayName")"
SP_COUNT="$(echo "${SP_RESULT}" | jq '.value | length')"

if [ "${SP_COUNT}" -gt 0 ]; then
  SP_OBJECT_ID="$(echo "${SP_RESULT}" | jq -r '.value[0].id')"
  echo "Found existing service principal: objectId=${SP_OBJECT_ID}"
else
  CREATE_SP_BODY="$(jq -n --arg app_id "${APP_ID}" '{ appId: $app_id }')"
  CREATE_SP_RESULT="$(graph_post "https://graph.microsoft.com/v1.0/servicePrincipals" "${CREATE_SP_BODY}")"

  if echo "${CREATE_SP_RESULT}" | jq -e '.error' >/dev/null 2>&1; then
    echo "Failed to create service principal:" >&2
    echo "${CREATE_SP_RESULT}" | jq >&2
    exit 1
  fi

  SP_OBJECT_ID="$(echo "${CREATE_SP_RESULT}" | jq -r '.id')"
  echo "Created service principal: objectId=${SP_OBJECT_ID}"
fi

SECRET_END_DATE="$(
  python3 - "${SECRET_DAYS}" <<'PY'
from datetime import datetime, timedelta, timezone
import sys
days = int(sys.argv[1])
end = datetime.now(timezone.utc) + timedelta(days=days)
print(end.replace(microsecond=0).isoformat().replace("+00:00", "Z"))
PY
)"

echo "Creating client secret (${SECRET_NAME}, ${SECRET_DAYS} days)..."
ADD_PASSWORD_BODY="$(jq -n \
  --arg name "${SECRET_NAME}" \
  --arg end "${SECRET_END_DATE}" \
  '{ passwordCredential: { displayName: $name, endDateTime: $end } }')"

ADD_PASSWORD_RESULT="$(graph_post "https://graph.microsoft.com/v1.0/applications/${APP_OBJECT_ID}/addPassword" "${ADD_PASSWORD_BODY}")"

if echo "${ADD_PASSWORD_RESULT}" | jq -e '.error' >/dev/null 2>&1; then
  echo "Failed to create client secret:" >&2
  echo "${ADD_PASSWORD_RESULT}" | jq >&2
  exit 1
fi

CLIENT_SECRET_VALUE="$(echo "${ADD_PASSWORD_RESULT}" | jq -r '.secretText // empty')"
if [ -z "${CLIENT_SECRET_VALUE}" ]; then
  echo "Client secret creation did not return secretText." >&2
  exit 1
fi

OUTPUT=$(
  cat <<EOF
export FABRIC_TENANT_ID="${TENANT_ID}"
export FABRIC_CLIENT_ID="${APP_ID}"
export FABRIC_CLIENT_SECRET="${CLIENT_SECRET_VALUE}"
EOF
)

echo ""
echo "Bootstrap complete."
echo "App ID: ${APP_ID}"
echo "Service Principal Object ID: ${SP_OBJECT_ID}"
echo ""
echo "Set these environment variables:"
echo "${OUTPUT}"
echo ""
echo "Next required manual steps:"
echo "1) In Fabric Admin Portal, allow service principals to use Fabric APIs."
echo "2) Add this service principal to the target workspace (Viewer/Contributor)."
echo "3) Validate with scripts/fabric/validate-sp-access.sh."

if [ -n "${ENV_OUT}" ]; then
  umask 077
  printf '%s\n' "${OUTPUT}" > "${ENV_OUT}"
  echo ""
  echo "Saved env exports to ${ENV_OUT} (permissions restricted)."
fi
