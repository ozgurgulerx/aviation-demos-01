#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
ENV_FILE="${1:-${ROOT_DIR}/.env.predictive.local}"
MIGRATION_SQL="${ROOT_DIR}/scripts/predictive/00_create_predictive_tables.sql"

if [ ! -f "${MIGRATION_SQL}" ]; then
  echo "Missing migration file: ${MIGRATION_SQL}" >&2
  exit 1
fi

"${SCRIPT_DIR}/01_local_db_preflight.sh" "${ENV_FILE}"

set -a
# shellcheck disable=SC1090
source "${ENV_FILE}"
set +a

echo "Applying predictive migration to local DB only..."
PGPASSWORD="${PGPASSWORD:-}" \
PGGSSENCMODE="${PGGSSENCMODE:-disable}" \
psql \
  --no-password \
  --set ON_ERROR_STOP=1 \
  --host "${PGHOST}" \
  --port "${PGPORT:-5432}" \
  --username "${PGUSER}" \
  --dbname "${PGDATABASE}" \
  --file "${MIGRATION_SQL}"

echo "Migration applied successfully."
