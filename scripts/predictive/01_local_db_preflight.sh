#!/usr/bin/env bash
set -euo pipefail

ENV_FILE="${1:-.env.predictive.local}"

if [ ! -f "${ENV_FILE}" ]; then
  echo "Missing env file: ${ENV_FILE}" >&2
  echo "Copy .env.predictive.local.example and fill local DB values." >&2
  exit 1
fi

set -a
# shellcheck disable=SC1090
source "${ENV_FILE}"
set +a

: "${PGHOST:?PGHOST is required}"
: "${PGPORT:=5432}"
: "${PGDATABASE:?PGDATABASE is required}"
: "${PGUSER:?PGUSER is required}"

host_lc="$(printf '%s' "${PGHOST}" | tr '[:upper:]' '[:lower:]')"

if [[ "${host_lc}" == *".postgres.database.azure.com"* ]]; then
  echo "Refusing to run: PGHOST points to Azure cloud host (${PGHOST})." >&2
  echo "This script is local-safe only and will not touch prod/runtime DB." >&2
  exit 2
fi

case "${host_lc}" in
  localhost|127.0.0.1|::1|host.docker.internal)
    ;;
  *)
    echo "Refusing to run: non-local PGHOST (${PGHOST})." >&2
    echo "Allowed local hosts: localhost, 127.0.0.1, ::1, host.docker.internal." >&2
    exit 2
    ;;
esac

echo "Checking local PostgreSQL connectivity:"
echo "  host=${PGHOST} port=${PGPORT} db=${PGDATABASE} user=${PGUSER}"

PGPASSWORD="${PGPASSWORD:-}" \
PGGSSENCMODE="${PGGSSENCMODE:-disable}" \
psql \
  --no-password \
  --set ON_ERROR_STOP=1 \
  --host "${PGHOST}" \
  --port "${PGPORT}" \
  --username "${PGUSER}" \
  --dbname "${PGDATABASE}" \
  --command "select 1 as local_db_ready;" >/dev/null

echo "Local DB preflight passed."
