#!/usr/bin/env python3
"""
ASRS database loader.

Loads normalized ASRS records into PostgreSQL.
"""

from __future__ import annotations

import argparse
import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, Tuple

from dotenv import load_dotenv

load_dotenv()


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS asrs_reports (
    asrs_report_id TEXT PRIMARY KEY,
    event_date DATE,
    location TEXT,
    aircraft_type TEXT,
    flight_phase TEXT,
    narrative_type TEXT,
    title TEXT,
    report_text TEXT NOT NULL,
    raw_json TEXT NOT NULL,
    ingested_at TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_asrs_reports_event_date ON asrs_reports(event_date);
CREATE INDEX IF NOT EXISTS idx_asrs_reports_aircraft_type ON asrs_reports(aircraft_type);
CREATE INDEX IF NOT EXISTS idx_asrs_reports_flight_phase ON asrs_reports(flight_phase);
CREATE INDEX IF NOT EXISTS idx_asrs_reports_location ON asrs_reports(location);

CREATE TABLE IF NOT EXISTS asrs_ingestion_runs (
    run_id TEXT PRIMARY KEY,
    started_at TIMESTAMP NOT NULL,
    completed_at TIMESTAMP,
    status TEXT NOT NULL,
    source_manifest_path TEXT,
    records_seen INTEGER NOT NULL,
    records_loaded INTEGER NOT NULL,
    records_failed INTEGER NOT NULL
);
"""


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def iter_jsonl(path: Path) -> Iterable[Dict[str, str]]:
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            stripped = line.strip()
            if not stripped:
                continue
            try:
                yield json.loads(stripped)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid JSON at {path}:{line_number}: {exc}") from exc


def validate_record(record: Dict[str, str]) -> Tuple[bool, str]:
    report_id = str(record.get("asrs_report_id", "")).strip()
    report_text = str(record.get("report_text", "")).strip()
    raw_json = str(record.get("raw_json", "")).strip()

    if not report_id:
        return False, "missing asrs_report_id"
    if not report_text:
        return False, f"missing report_text for {report_id}"
    if not raw_json:
        return False, f"missing raw_json for {report_id}"

    return True, ""


def upsert_postgres(cursor, record: Dict[str, str], ingested_at: str) -> None:
    cursor.execute(
        """
        INSERT INTO asrs_reports (
            asrs_report_id,
            event_date,
            location,
            aircraft_type,
            flight_phase,
            narrative_type,
            title,
            report_text,
            raw_json,
            ingested_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT(asrs_report_id) DO UPDATE SET
            event_date=excluded.event_date,
            location=excluded.location,
            aircraft_type=excluded.aircraft_type,
            flight_phase=excluded.flight_phase,
            narrative_type=excluded.narrative_type,
            title=excluded.title,
            report_text=excluded.report_text,
            raw_json=excluded.raw_json,
            ingested_at=excluded.ingested_at
        """,
        (
            record.get("asrs_report_id"),
            record.get("event_date"),
            record.get("location"),
            record.get("aircraft_type"),
            record.get("flight_phase"),
            record.get("narrative_type"),
            record.get("title"),
            record.get("report_text"),
            record.get("raw_json"),
            ingested_at,
        ),
    )


def upsert_run_postgres(
    cursor,
    run_id: str,
    started_at: str,
    completed_at: str,
    status: str,
    source_manifest_path: str,
    records_seen: int,
    records_loaded: int,
    records_failed: int,
) -> None:
    cursor.execute(
        """
        INSERT INTO asrs_ingestion_runs (
            run_id,
            started_at,
            completed_at,
            status,
            source_manifest_path,
            records_seen,
            records_loaded,
            records_failed
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT(run_id) DO UPDATE SET
            completed_at=excluded.completed_at,
            status=excluded.status,
            source_manifest_path=excluded.source_manifest_path,
            records_seen=excluded.records_seen,
            records_loaded=excluded.records_loaded,
            records_failed=excluded.records_failed
        """,
        (
            run_id,
            started_at,
            completed_at,
            status,
            source_manifest_path,
            records_seen,
            records_loaded,
            records_failed,
        ),
    )


def load_postgres(records_file: Path, run_id: str, source_manifest_path: str, batch_size: int) -> None:
    import psycopg2

    started_at = utc_now_iso()
    conn = psycopg2.connect(
        host=os.environ.get("PGHOST"),
        port=os.environ.get("PGPORT", "5432"),
        database=os.environ.get("PGDATABASE"),
        user=os.environ.get("PGUSER"),
        password=os.environ.get("PGPASSWORD"),
        sslmode="require",
    )
    cursor = conn.cursor()

    cursor.execute(SCHEMA_SQL)
    conn.commit()

    seen = 0
    loaded = 0
    failed = 0
    ingested_at = utc_now_iso()

    for record in iter_jsonl(records_file):
        seen += 1
        valid, reason = validate_record(record)
        if not valid:
            failed += 1
            print(f"Skip invalid record: {reason}")
            continue

        try:
            upsert_postgres(cursor, record, ingested_at)
            loaded += 1
            if loaded % batch_size == 0:
                conn.commit()
        except Exception as exc:
            failed += 1
            print(f"Failed record {record.get('asrs_report_id', 'unknown')}: {exc}")

    conn.commit()

    status = "success" if failed == 0 else "partial_success"
    completed_at = utc_now_iso()
    upsert_run_postgres(
        cursor,
        run_id,
        started_at,
        completed_at,
        status,
        source_manifest_path,
        seen,
        loaded,
        failed,
    )
    conn.commit()

    cursor.close()
    conn.close()

    print("PostgreSQL load complete")
    print(f"  Run ID: {run_id}")
    print(f"  Records seen: {seen}")
    print(f"  Records loaded: {loaded}")
    print(f"  Records failed: {failed}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Load ASRS records into PostgreSQL")
    parser.add_argument("--mode", choices=["postgres"], default="postgres", help="Database mode")
    parser.add_argument("--data", default="data/processed", help="Directory with processed files")
    parser.add_argument("--records-file", default="asrs_records.jsonl", help="Records JSONL filename")
    parser.add_argument("--run-id", default="", help="Explicit ingestion run ID")
    parser.add_argument("--source-manifest", default="", help="Manifest path for traceability")
    parser.add_argument("--batch-size", type=int, default=500, help="Commit batch size")
    args = parser.parse_args()

    if args.batch_size < 1:
        raise ValueError("--batch-size must be >= 1")

    run_id = args.run_id.strip() or f"asrs-load-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:8]}"
    records_file = Path(args.data) / args.records_file

    if not records_file.exists():
        raise FileNotFoundError(f"Records file not found: {records_file}")

    source_manifest = args.source_manifest.strip()
    load_postgres(records_file, run_id, source_manifest, args.batch_size)


if __name__ == "__main__":
    main()
