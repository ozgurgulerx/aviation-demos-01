#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python3}"
DB_MODE="${DB_MODE:-sqlite}"
SQLITE_DB="${SQLITE_DB:-${ROOT_DIR}/aviation.db}"
RAW_DIR="${RAW_DIR:-${ROOT_DIR}/data/asrs/raw}"
MANIFEST_DIR="${MANIFEST_DIR:-${ROOT_DIR}/data/asrs/manifests}"
PROCESSED_DIR="${PROCESSED_DIR:-${ROOT_DIR}/data/processed}"
CHUNK_DAYS="${CHUNK_DAYS:-31}"
MAX_ROWS_PER_FILE="${MAX_ROWS_PER_FILE:-10000}"
BATCH_SIZE="${BATCH_SIZE:-100}"
RUN_ID="${RUN_ID:-asrs-pipeline-$(date -u +%Y%m%dT%H%M%SZ)}"

FROM_DATE="${FROM_DATE:-}"
TO_DATE="${TO_DATE:-}"

if [[ -z "$FROM_DATE" || -z "$TO_DATE" ]]; then
  read -r FROM_DATE TO_DATE < <("$PYTHON_BIN" - <<'PY'
from datetime import datetime, timedelta, timezone

today = datetime.now(timezone.utc).date()
first_this_month = today.replace(day=1)
last_prev_month = first_this_month - timedelta(days=1)
first_prev_month = last_prev_month.replace(day=1)
print(first_prev_month.isoformat(), last_prev_month.isoformat())
PY
)
fi

START_TS="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

echo "ASRS monthly pipeline"
echo "  Run ID: $RUN_ID"
echo "  Date range: $FROM_DATE -> $TO_DATE"
echo "  DB mode: $DB_MODE"

echo
echo "[1/4] Fetch ASRS exports"
"$PYTHON_BIN" "$ROOT_DIR/scripts/00_fetch_asrs_exports.py" \
  --from-date "$FROM_DATE" \
  --to-date "$TO_DATE" \
  --chunk-days "$CHUNK_DAYS" \
  --max-rows-per-file "$MAX_ROWS_PER_FILE" \
  --out-dir "$RAW_DIR" \
  --manifest-dir "$MANIFEST_DIR"

LATEST_MANIFEST="$(ls -1t "$MANIFEST_DIR"/asrs_manifest_*.json | head -n1)"

echo
echo "[2/4] Extract + normalize"
"$PYTHON_BIN" "$ROOT_DIR/scripts/01_extract_data.py" \
  --input "$RAW_DIR" \
  --output "$PROCESSED_DIR"

echo
echo "[3/4] Load SQL store"
if [[ "$DB_MODE" == "sqlite" ]]; then
  "$PYTHON_BIN" "$ROOT_DIR/scripts/02_load_database.py" \
    --mode sqlite \
    --db "$SQLITE_DB" \
    --data "$PROCESSED_DIR" \
    --run-id "$RUN_ID" \
    --source-manifest "$LATEST_MANIFEST"
else
  "$PYTHON_BIN" "$ROOT_DIR/scripts/02_load_database.py" \
    --mode postgres \
    --data "$PROCESSED_DIR" \
    --run-id "$RUN_ID" \
    --source-manifest "$LATEST_MANIFEST"
fi

echo
echo "[4/5] Create/update semantic index"
"$PYTHON_BIN" "$ROOT_DIR/scripts/03_create_search_index.py"

echo
echo "[5/5] Upload semantic documents"
"$PYTHON_BIN" "$ROOT_DIR/scripts/04_upload_documents.py" \
  --data "$PROCESSED_DIR" \
  --batch-size "$BATCH_SIZE"

END_TS="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

echo
echo "Pipeline complete"
echo "  Started: $START_TS"
echo "  Ended:   $END_TS"
echo "  Manifest: $LATEST_MANIFEST"
echo "  Processed records: $PROCESSED_DIR/asrs_records.jsonl"
echo "  Processed docs: $PROCESSED_DIR/asrs_documents.jsonl"
