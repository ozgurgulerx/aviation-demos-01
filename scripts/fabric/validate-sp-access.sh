#!/usr/bin/env bash
#
# Validate Microsoft Fabric access using a service principal.
# This script does not use or change the current Azure CLI user session.
#
# Required env vars:
#   FABRIC_TENANT_ID
#   FABRIC_CLIENT_ID
#   FABRIC_CLIENT_SECRET
#
# Optional env vars:
#   FABRIC_BASE_URL (default: https://api.fabric.microsoft.com)
#   FABRIC_WORKSPACE_ID
#   FABRIC_WORKSPACE_NAME
#   FABRIC_TIMEOUT_SECONDS (default: 30)
#
set -euo pipefail

FABRIC_BASE_URL="${FABRIC_BASE_URL:-https://api.fabric.microsoft.com}"
FABRIC_TIMEOUT_SECONDS="${FABRIC_TIMEOUT_SECONDS:-30}"

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1" >&2
    exit 1
  fi
}

require_env() {
  local key="$1"
  if [ -z "${!key:-}" ]; then
    echo "Missing required env var: $key" >&2
    exit 1
  fi
}

require_cmd "curl"
require_cmd "jq"
require_env "FABRIC_TENANT_ID"
require_env "FABRIC_CLIENT_ID"
require_env "FABRIC_CLIENT_SECRET"

TOKEN_ENDPOINT="https://login.microsoftonline.com/${FABRIC_TENANT_ID}/oauth2/v2.0/token"
FABRIC_SCOPE="https://api.fabric.microsoft.com/.default"
TDS_SCOPE="https://database.windows.net/.default"

acquire_token() {
  local scope="$1"
  local label="$2"
  local response

  echo "Requesting ${label} token (scope=${scope})..."
  response="$(curl -sS --max-time "${FABRIC_TIMEOUT_SECONDS}" \
    --request POST "${TOKEN_ENDPOINT}" \
    --header "Content-Type: application/x-www-form-urlencoded" \
    --data-urlencode "client_id=${FABRIC_CLIENT_ID}" \
    --data-urlencode "client_secret=${FABRIC_CLIENT_SECRET}" \
    --data-urlencode "scope=${scope}" \
    --data-urlencode "grant_type=client_credentials")"

  if echo "${response}" | jq -e '.error' >/dev/null 2>&1; then
    echo "${label} token request failed:" >&2
    echo "${response}" | jq -r '.error + ": " + (.error_description // "unknown")' >&2
    return 1
  fi

  local token
  token="$(echo "${response}" | jq -r '.access_token // empty')"
  if [ -z "${token}" ]; then
    echo "${label} token request did not return an access_token." >&2
    return 1
  fi

  local expires_in
  expires_in="$(echo "${response}" | jq -r '.expires_in // "unknown"')"
  echo "${label} token acquired. Expires in ${expires_in} seconds."
  echo "${token}"
}

# --- Fabric API scope (primary) ---
FABRIC_TOKEN_OUTPUT="$(acquire_token "${FABRIC_SCOPE}" "Fabric API")" || {
  echo "Fabric API token acquisition failed. Aborting." >&2
  exit 1
}
# Last line of output is the token; preceding lines are status messages.
ACCESS_TOKEN="$(echo "${FABRIC_TOKEN_OUTPUT}" | tail -n 1)"
echo "${FABRIC_TOKEN_OUTPUT}" | sed '$d'

# --- TDS / Azure SQL Database scope ---
TDS_TOKEN_OUTPUT="$(acquire_token "${TDS_SCOPE}" "TDS (database.windows.net)")" || {
  echo ""
  echo "WARNING: TDS scope token acquisition failed."
  echo "  FABRIC_SQL queries using pyodbc (TDS mode) will fail with 'fabric_sql_auth_unavailable'."
  echo "  Grant the SP the 'Azure SQL Database' API permission (https://database.windows.net) and admin-consent."
  echo ""
}

fetch_json() {
  local url="$1"
  local body
  local code

  body="$(mktemp)"
  code="$(curl -sS --max-time "${FABRIC_TIMEOUT_SECONDS}" \
    --output "${body}" \
    --write-out "%{http_code}" \
    --request GET "${url}" \
    --header "Authorization: Bearer ${ACCESS_TOKEN}" \
    --header "Content-Type: application/json")"

  if [ "${code}" -lt 200 ] || [ "${code}" -ge 300 ]; then
    echo "Request failed (${code}) for ${url}" >&2
    cat "${body}" >&2
    rm -f "${body}"
    exit 1
  fi

  cat "${body}"
  rm -f "${body}"
}

echo "Checking Fabric API reachability..."
WORKSPACES_JSON="$(fetch_json "${FABRIC_BASE_URL}/v1/workspaces")"
COUNT="$(echo "${WORKSPACES_JSON}" | jq '.value | length')"
echo "Workspaces visible to service principal: ${COUNT}"

if [ "${COUNT}" -gt 0 ]; then
  echo "${WORKSPACES_JSON}" | jq -r '.value[] | [.id, .displayName, .type] | @tsv' \
    | awk 'BEGIN{print "id\tdisplayName\ttype"} {print}'
fi

if [ -n "${FABRIC_WORKSPACE_ID:-}" ]; then
  echo "Validating workspace by ID: ${FABRIC_WORKSPACE_ID}"
  WS_BY_ID="$(fetch_json "${FABRIC_BASE_URL}/v1/workspaces/${FABRIC_WORKSPACE_ID}")"
  echo "${WS_BY_ID}" | jq -r '"Workspace ID check passed: " + .id + " (" + .displayName + ")"'
fi

if [ -n "${FABRIC_WORKSPACE_NAME:-}" ]; then
  MATCH_COUNT="$(echo "${WORKSPACES_JSON}" \
    | jq --arg ws "${FABRIC_WORKSPACE_NAME}" '[.value[] | select(.displayName == $ws)] | length')"
  if [ "${MATCH_COUNT}" -eq 0 ]; then
    echo "No workspace found with name: ${FABRIC_WORKSPACE_NAME}" >&2
    exit 1
  fi
  MATCH_ID="$(echo "${WORKSPACES_JSON}" \
    | jq -r --arg ws "${FABRIC_WORKSPACE_NAME}" '.value[] | select(.displayName == $ws) | .id' \
    | head -n 1)"
  echo "Workspace name check passed: ${FABRIC_WORKSPACE_NAME} (${MATCH_ID})"
fi

echo "Fabric service principal validation completed (scopes tested: Fabric API, TDS/database.windows.net)."
