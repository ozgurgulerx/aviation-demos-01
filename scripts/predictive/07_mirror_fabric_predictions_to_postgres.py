#!/usr/bin/env python3
"""
Mirror predictive outputs into PostgreSQL demo tables.

Expected JSON files in input directory (all optional):
  - delay_predictions_current.json
  - delay_model_metrics_latest.json
  - delay_action_recommendations_current.json
  - delay_decision_trace.json
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any, Dict, Iterable, List

import psycopg2
from psycopg2.extras import Json


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_INPUT_DIR = ROOT / "artifacts" / "predictive_delay"


def _host_is_cloud(host: str) -> bool:
    h = str(host or "").strip().lower()
    if not h:
        return False
    cloud_markers = (
        ".postgres.database.azure.com",
        ".rds.amazonaws.com",
        ".supabase.co",
        ".neon.tech",
        ".railway.internal",
    )
    return any(marker in h for marker in cloud_markers)


def _allow_cloud_write() -> bool:
    return str(os.getenv("ALLOW_CLOUD_PREDICTIVE_DB_WRITE", "false")).strip().lower() in {
        "1",
        "true",
        "yes",
        "y",
        "on",
    }


def _connect():
    host = os.getenv("PGHOST")
    if not host:
        raise RuntimeError("PGHOST is required")
    if _host_is_cloud(host) and not _allow_cloud_write():
        raise RuntimeError(
            "refusing_cloud_db_write: PGHOST resolves to cloud host and ALLOW_CLOUD_PREDICTIVE_DB_WRITE is not true"
        )
    sslmode = str(os.getenv("PGSSLMODE", "require")).strip() or "require"
    gssencmode = str(os.getenv("PGGSSENCMODE", "")).strip()
    connect_kwargs = {
        "host": host,
        "port": int(os.getenv("PGPORT", "5432")),
        "database": os.getenv("PGDATABASE", "aviationrag"),
        "user": os.getenv("PGUSER"),
        "password": os.getenv("PGPASSWORD"),
        "sslmode": sslmode,
        "connect_timeout": 10,
    }
    if gssencmode:
        connect_kwargs["gssencmode"] = gssencmode
    return psycopg2.connect(
        **connect_kwargs,
    )


def _load_json(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    raw = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(raw, list):
        return [item for item in raw if isinstance(item, dict)]
    if isinstance(raw, dict):
        rows = raw.get("rows")
        if isinstance(rows, list):
            return [item for item in rows if isinstance(item, dict)]
        return [raw]
    return []


def _truncate(cur, schema: str, table: str) -> None:
    cur.execute(f'TRUNCATE TABLE "{schema}"."{table}"')


def _insert_predictions(cur, schema: str, rows: Iterable[Dict[str, Any]]) -> int:
    sql = f"""
        INSERT INTO "{schema}"."delay_predictions_current" (
          as_of_utc, model_variant, model_version, flight_leg_id, flight_number, origin, dest, std_utc,
          risk_a15, expected_delay_minutes, prediction_interval_low, prediction_interval_high, top_drivers,
          data_freshness, degraded_sources
        ) VALUES (
          %(as_of_utc)s, %(model_variant)s, %(model_version)s, %(flight_leg_id)s, %(flight_number)s,
          %(origin)s, %(dest)s, %(std_utc)s, %(risk_a15)s, %(expected_delay_minutes)s,
          %(prediction_interval_low)s, %(prediction_interval_high)s, %(top_drivers)s,
          %(data_freshness)s, %(degraded_sources)s
        )
    """
    count = 0
    for row in rows:
        cur.execute(
            sql,
            {
                "as_of_utc": row.get("as_of_utc"),
                "model_variant": row.get("model_variant") or row.get("model") or "optimized",
                "model_version": row.get("model_version"),
                "flight_leg_id": row.get("flight_leg_id") or row.get("leg_id"),
                "flight_number": row.get("flight_number") or row.get("flight_no"),
                "origin": row.get("origin") or row.get("origin_iata"),
                "dest": row.get("dest") or row.get("destination") or row.get("dest_iata"),
                "std_utc": row.get("std_utc") or row.get("scheduled_dep_utc"),
                "risk_a15": row.get("risk_a15") or row.get("a15_risk"),
                "expected_delay_minutes": row.get("expected_delay_minutes") or row.get("expected_delay"),
                "prediction_interval_low": (row.get("prediction_interval") or {}).get("low")
                if isinstance(row.get("prediction_interval"), dict)
                else row.get("prediction_interval_low"),
                "prediction_interval_high": (row.get("prediction_interval") or {}).get("high")
                if isinstance(row.get("prediction_interval"), dict)
                else row.get("prediction_interval_high"),
                "top_drivers": Json(row.get("top_drivers") or []),
                "data_freshness": row.get("data_freshness"),
                "degraded_sources": Json(row.get("degraded_sources") or []),
            },
        )
        count += 1
    return count


def _insert_metrics(cur, schema: str, rows: Iterable[Dict[str, Any]]) -> int:
    sql = f"""
        INSERT INTO "{schema}"."delay_model_metrics_latest" (
          as_of_utc, sample_window, baseline_auroc, baseline_brier, baseline_mae,
          optimized_auroc, optimized_brier, optimized_mae, auroc_delta, brier_delta, mae_delta
        ) VALUES (
          %(as_of_utc)s, %(sample_window)s, %(baseline_auroc)s, %(baseline_brier)s, %(baseline_mae)s,
          %(optimized_auroc)s, %(optimized_brier)s, %(optimized_mae)s, %(auroc_delta)s, %(brier_delta)s, %(mae_delta)s
        )
    """
    count = 0
    for row in rows:
        baseline = row.get("baseline") if isinstance(row.get("baseline"), dict) else {}
        optimized = row.get("optimized") if isinstance(row.get("optimized"), dict) else {}
        uplift = row.get("uplift") if isinstance(row.get("uplift"), dict) else {}
        cur.execute(
            sql,
            {
                "as_of_utc": row.get("as_of_utc"),
                "sample_window": row.get("sample_window"),
                "baseline_auroc": row.get("baseline_auroc", baseline.get("auroc")),
                "baseline_brier": row.get("baseline_brier", baseline.get("brier")),
                "baseline_mae": row.get("baseline_mae", baseline.get("mae")),
                "optimized_auroc": row.get("optimized_auroc", optimized.get("auroc")),
                "optimized_brier": row.get("optimized_brier", optimized.get("brier")),
                "optimized_mae": row.get("optimized_mae", optimized.get("mae")),
                "auroc_delta": row.get("auroc_delta", uplift.get("auroc_delta")),
                "brier_delta": row.get("brier_delta", uplift.get("brier_delta")),
                "mae_delta": row.get("mae_delta", uplift.get("mae_delta")),
            },
        )
        count += 1
    return count


def _insert_actions(cur, schema: str, rows: Iterable[Dict[str, Any]]) -> int:
    sql = f"""
        INSERT INTO "{schema}"."delay_action_recommendations_current" (
          as_of_utc, model_variant, flight_leg_id, flight_number, action_rank, action_code, action_label,
          expected_delta_minutes, feasibility_status, confidence_band, constraint_notes
        ) VALUES (
          %(as_of_utc)s, %(model_variant)s, %(flight_leg_id)s, %(flight_number)s, %(action_rank)s, %(action_code)s,
          %(action_label)s, %(expected_delta_minutes)s, %(feasibility_status)s, %(confidence_band)s, %(constraint_notes)s
        )
    """
    count = 0
    for row in rows:
        cur.execute(
            sql,
            {
                "as_of_utc": row.get("as_of_utc"),
                "model_variant": row.get("model_variant") or row.get("model") or "optimized",
                "flight_leg_id": row.get("flight_leg_id") or row.get("leg_id"),
                "flight_number": row.get("flight_number") or row.get("flight_no"),
                "action_rank": row.get("action_rank") or row.get("rank"),
                "action_code": row.get("action_code"),
                "action_label": row.get("action_label") or row.get("recommendation"),
                "expected_delta_minutes": row.get("expected_delta_minutes"),
                "feasibility_status": row.get("feasibility_status") or row.get("feasible"),
                "confidence_band": row.get("confidence_band") or row.get("confidence"),
                "constraint_notes": row.get("constraint_notes") or row.get("constraint_reason"),
            },
        )
        count += 1
    return count


def _insert_trace(cur, schema: str, rows: Iterable[Dict[str, Any]]) -> int:
    sql = f"""
        INSERT INTO "{schema}"."delay_decision_trace" (
          as_of_utc, model_variant, model_version, decision_policy_version, constraint_version, objective_version,
          flight_leg_id, selected_action_code, feasibility_status, override_reason, approved_by
        ) VALUES (
          %(as_of_utc)s, %(model_variant)s, %(model_version)s, %(decision_policy_version)s, %(constraint_version)s,
          %(objective_version)s, %(flight_leg_id)s, %(selected_action_code)s, %(feasibility_status)s, %(override_reason)s, %(approved_by)s
        )
    """
    count = 0
    for row in rows:
        cur.execute(
            sql,
            {
                "as_of_utc": row.get("as_of_utc"),
                "model_variant": row.get("model_variant") or row.get("model"),
                "model_version": row.get("model_version"),
                "decision_policy_version": row.get("decision_policy_version"),
                "constraint_version": row.get("constraint_version"),
                "objective_version": row.get("objective_version"),
                "flight_leg_id": row.get("flight_leg_id"),
                "selected_action_code": row.get("selected_action_code"),
                "feasibility_status": row.get("feasibility_status"),
                "override_reason": row.get("override_reason"),
                "approved_by": row.get("approved_by"),
            },
        )
        count += 1
    return count


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", type=Path, default=DEFAULT_INPUT_DIR)
    parser.add_argument("--schema", type=str, default=os.getenv("PREDICTIVE_MIRROR_SCHEMA", "demo"))
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    input_dir = args.input_dir
    schema = args.schema.strip() or "demo"

    predictions = _load_json(input_dir / "delay_predictions_current.json")
    metrics = _load_json(input_dir / "delay_model_metrics_latest.json")
    actions = _load_json(input_dir / "delay_action_recommendations_current.json")
    trace = _load_json(input_dir / "delay_decision_trace.json")

    print(f"Input dir: {input_dir}")
    print(
        "Rows found:",
        f"predictions={len(predictions)}",
        f"metrics={len(metrics)}",
        f"actions={len(actions)}",
        f"trace={len(trace)}",
    )

    if args.dry_run:
        print("Dry-run mode: no database writes.")
        return

    with _connect() as conn:
        with conn.cursor() as cur:
            for table in (
                "delay_predictions_current",
                "delay_model_metrics_latest",
                "delay_action_recommendations_current",
                "delay_decision_trace",
            ):
                _truncate(cur, schema, table)

            inserted_predictions = _insert_predictions(cur, schema, predictions)
            inserted_metrics = _insert_metrics(cur, schema, metrics)
            inserted_actions = _insert_actions(cur, schema, actions)
            inserted_trace = _insert_trace(cur, schema, trace)
        conn.commit()

    print(
        "Inserted:",
        f"predictions={inserted_predictions}",
        f"metrics={inserted_metrics}",
        f"actions={inserted_actions}",
        f"trace={inserted_trace}",
    )


if __name__ == "__main__":
    main()
