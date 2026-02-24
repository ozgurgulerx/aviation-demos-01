#!/usr/bin/env python3
"""Predictive delay service backed by PostgreSQL mirror tables."""

from __future__ import annotations

import json
import logging
import os
import re
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
except Exception:  # pragma: no cover - exercised via degraded path tests
    psycopg2 = None
    RealDictCursor = None

logger = logging.getLogger(__name__)


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_int(name: str, default: int, minimum: int, maximum: int) -> int:
    raw = os.getenv(name)
    try:
        value = int(str(raw).strip()) if raw is not None else default
    except Exception:
        value = default
    return max(minimum, min(maximum, value))


def _qident(name: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", str(name or "")):
        raise ValueError(f"invalid_identifier:{name}")
    return f'"{name}"'


def _pick(row: Dict[str, Any], keys: Sequence[str], default: Any = None) -> Any:
    for key in keys:
        if key in row and row[key] is not None:
            return row[key]
    return default


def _to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None


def _normalize_dt(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat()
    text = str(value).strip()
    if not text:
        return None
    return text


def _parse_listish(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(v) for v in value if str(v).strip()]
    if isinstance(value, tuple):
        return [str(v) for v in value if str(v).strip()]
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return []
        if raw.startswith("[") and raw.endswith("]"):
            try:
                parsed = json.loads(raw)
                if isinstance(parsed, list):
                    return [str(v) for v in parsed if str(v).strip()]
            except Exception:
                pass
        return [s.strip() for s in raw.split(",") if s.strip()]
    return [str(value)]


class PredictiveDelayService:
    """Read predictive optimization outputs from mirrored PostgreSQL tables."""

    def __init__(self, schema: Optional[str] = None, enabled: Optional[bool] = None):
        self.schema = (schema or os.getenv("PREDICTIVE_MIRROR_SCHEMA", "demo")).strip() or "demo"
        self.enabled = _env_bool("ENABLE_PREDICTIVE_API", False) if enabled is None else bool(enabled)
        self.default_window_hours = _env_int("PREDICTIVE_DEFAULT_WINDOW_HOURS", 6, minimum=1, maximum=72)
        self.default_limit = _env_int("PREDICTIVE_DEFAULT_LIMIT", 100, minimum=1, maximum=500)

    def _table_ref(self, table: str) -> str:
        return f"{_qident(self.schema)}.{_qident(table)}"

    def _disabled_payload(self, message: str) -> Dict[str, Any]:
        return {
            "status": "disabled",
            "enabled": False,
            "message": message,
        }

    def _degraded_payload(self, message: str, *, extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "status": "degraded",
            "enabled": self.enabled,
            "message": message,
        }
        if extra:
            payload.update(extra)
        return payload

    def _connect(self):
        if psycopg2 is None:
            raise RuntimeError("psycopg2_unavailable")
        host = os.getenv("PGHOST", "").strip()
        if not host:
            raise RuntimeError("pg_host_not_configured")
        sslmode = (os.getenv("PGSSLMODE", "prefer") or "prefer").strip()
        gssencmode = (os.getenv("PGGSSENCMODE", "") or "").strip()
        connect_kwargs = dict(
            host=host,
            port=int(os.getenv("PGPORT", "5432")),
            database=os.getenv("PGDATABASE", "aviationrag"),
            user=os.getenv("PGUSER"),
            password=os.getenv("PGPASSWORD"),
            connect_timeout=5,
        )
        if sslmode:
            connect_kwargs["sslmode"] = sslmode
        if gssencmode:
            connect_kwargs["gssencmode"] = gssencmode
        conn = psycopg2.connect(**connect_kwargs)
        conn.autocommit = True
        return conn

    @staticmethod
    def _table_exists(cur, schema: str, table: str) -> bool:
        cur.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = %s AND table_name = %s
            LIMIT 1
            """,
            (schema, table),
        )
        return bool(cur.fetchone())

    @staticmethod
    def _table_columns(cur, schema: str, table: str) -> List[str]:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
            """,
            (schema, table),
        )
        return [str(row["column_name"]) for row in cur.fetchall() or []]

    def get_delays(self, model: str = "optimized", window_hours: Optional[int] = None, limit: Optional[int] = None) -> Dict[str, Any]:
        if not self.enabled:
            return self._disabled_payload("Predictive API is disabled (ENABLE_PREDICTIVE_API=false).")

        safe_model = "baseline" if str(model).strip().lower() == "baseline" else "optimized"
        safe_limit = max(1, min(500, int(limit or self.default_limit)))
        safe_window = max(1, min(72, int(window_hours or self.default_window_hours)))
        table = "delay_predictions_current"
        try:
            with self._connect() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    if not self._table_exists(cur, self.schema, table):
                        return self._degraded_payload(
                            f"Missing mirror table: {self.schema}.{table}",
                            extra={"rows": [], "model": safe_model, "window_hours": safe_window},
                        )
                    columns = set(self._table_columns(cur, self.schema, table))
                    if not columns:
                        return self._degraded_payload(
                            f"Table has no visible columns: {self.schema}.{table}",
                            extra={"rows": [], "model": safe_model, "window_hours": safe_window},
                        )

                    selected = sorted(columns)
                    where: List[str] = []
                    params: List[Any] = []
                    if "model_variant" in columns:
                        where.append('"model_variant" = %s')
                        params.append(safe_model)
                    elif "model" in columns:
                        where.append('"model" = %s')
                        params.append(safe_model)

                    if "std_utc" in columns:
                        where.append('"std_utc" >= NOW()')
                        where.append('"std_utc" <= NOW() + (%s * INTERVAL \'1 hour\')')
                        params.append(safe_window)

                    where_sql = f" WHERE {' AND '.join(where)}" if where else ""
                    order_sql = (
                        ' ORDER BY "risk_a15" DESC NULLS LAST'
                        if "risk_a15" in columns
                        else (
                            ' ORDER BY "expected_delay_minutes" DESC NULLS LAST'
                            if "expected_delay_minutes" in columns
                            else (
                                ' ORDER BY "as_of_utc" DESC NULLS LAST'
                                if "as_of_utc" in columns
                                else ""
                            )
                        )
                    )

                    query = (
                        f"SELECT {', '.join(_qident(c) for c in selected)} "
                        f"FROM {self._table_ref(table)}{where_sql}{order_sql} LIMIT %s"
                    )
                    params.append(safe_limit)
                    cur.execute(query, params)
                    raw_rows = cur.fetchall() or []

                    mapped_rows: List[Dict[str, Any]] = []
                    as_of_candidates: List[str] = []
                    for row in raw_rows:
                        interval_low = _to_float(_pick(row, ("prediction_interval_low", "pi_low", "delay_min_low")))
                        interval_high = _to_float(_pick(row, ("prediction_interval_high", "pi_high", "delay_min_high")))
                        mapped = {
                            "flight_leg_id": _pick(row, ("flight_leg_id", "leg_id"), ""),
                            "flight_number": _pick(row, ("flight_number", "flight_no"), ""),
                            "origin": _pick(row, ("origin", "origin_iata"), ""),
                            "dest": _pick(row, ("dest", "destination", "dest_iata"), ""),
                            "std_utc": _normalize_dt(_pick(row, ("std_utc", "scheduled_dep_utc"))),
                            "risk_a15": _to_float(_pick(row, ("risk_a15", "a15_risk", "risk"))),
                            "expected_delay_minutes": _to_float(
                                _pick(row, ("expected_delay_minutes", "delay_minutes_expected", "expected_delay"))
                            ),
                            "prediction_interval": {"low": interval_low, "high": interval_high},
                            "top_drivers": _parse_listish(_pick(row, ("top_drivers", "drivers"))),
                            "model_variant": _pick(row, ("model_variant", "model"), safe_model),
                            "model_version": _pick(row, ("model_version", "version")),
                            "data_freshness": _pick(row, ("data_freshness", "freshness")),
                            "degraded_sources": _parse_listish(_pick(row, ("degraded_sources",))),
                        }
                        as_of = _normalize_dt(_pick(row, ("as_of_utc", "generated_at_utc", "updated_at")))
                        if as_of:
                            as_of_candidates.append(as_of)
                        mapped_rows.append(mapped)

                    status = "ok" if mapped_rows else "empty"
                    return {
                        "status": status,
                        "enabled": True,
                        "model": safe_model,
                        "window_hours": safe_window,
                        "as_of_utc": as_of_candidates[0] if as_of_candidates else datetime.now(timezone.utc).isoformat(),
                        "row_count": len(mapped_rows),
                        "rows": mapped_rows,
                    }
        except Exception as exc:
            logger.exception("Predictive delays query failed")
            return self._degraded_payload(
                "Unable to query predictive delays.",
                extra={"error": str(exc), "rows": [], "model": safe_model, "window_hours": safe_window},
            )

    def get_delay_metrics(self) -> Dict[str, Any]:
        if not self.enabled:
            return self._disabled_payload("Predictive API is disabled (ENABLE_PREDICTIVE_API=false).")

        table = "delay_model_metrics_latest"
        try:
            with self._connect() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    if not self._table_exists(cur, self.schema, table):
                        return self._degraded_payload(
                            f"Missing mirror table: {self.schema}.{table}",
                            extra={"baseline": {}, "optimized": {}, "uplift": {}},
                        )
                    columns = set(self._table_columns(cur, self.schema, table))
                    if not columns:
                        return self._degraded_payload(
                            f"Table has no visible columns: {self.schema}.{table}",
                            extra={"baseline": {}, "optimized": {}, "uplift": {}},
                        )

                    order_sql = (
                        ' ORDER BY "as_of_utc" DESC NULLS LAST'
                        if "as_of_utc" in columns
                        else (
                            ' ORDER BY "generated_at_utc" DESC NULLS LAST'
                            if "generated_at_utc" in columns
                            else ""
                        )
                    )
                    query = f"SELECT {', '.join(_qident(c) for c in sorted(columns))} FROM {self._table_ref(table)}{order_sql} LIMIT 1"
                    cur.execute(query)
                    row = cur.fetchone()
                    if not row:
                        return self._degraded_payload(
                            f"No rows in mirror table: {self.schema}.{table}",
                            extra={"baseline": {}, "optimized": {}, "uplift": {}},
                        )

                    baseline = {
                        "auroc": _to_float(_pick(row, ("baseline_auroc", "auroc_baseline"))),
                        "brier": _to_float(_pick(row, ("baseline_brier", "brier_baseline"))),
                        "mae": _to_float(_pick(row, ("baseline_mae", "mae_baseline"))),
                    }
                    optimized = {
                        "auroc": _to_float(_pick(row, ("optimized_auroc", "auroc_optimized"))),
                        "brier": _to_float(_pick(row, ("optimized_brier", "brier_optimized"))),
                        "mae": _to_float(_pick(row, ("optimized_mae", "mae_optimized"))),
                    }

                    auroc_delta = _to_float(_pick(row, ("auroc_delta",)))
                    if auroc_delta is None and optimized["auroc"] is not None and baseline["auroc"] is not None:
                        auroc_delta = optimized["auroc"] - baseline["auroc"]

                    brier_delta = _to_float(_pick(row, ("brier_delta",)))
                    if brier_delta is None and optimized["brier"] is not None and baseline["brier"] is not None:
                        brier_delta = optimized["brier"] - baseline["brier"]

                    mae_delta = _to_float(_pick(row, ("mae_delta",)))
                    if mae_delta is None and optimized["mae"] is not None and baseline["mae"] is not None:
                        mae_delta = optimized["mae"] - baseline["mae"]

                    uplift = {
                        "auroc_delta": auroc_delta,
                        "brier_delta": brier_delta,
                        "mae_delta": mae_delta,
                    }

                    return {
                        "status": "ok",
                        "enabled": True,
                        "as_of_utc": _normalize_dt(_pick(row, ("as_of_utc", "generated_at_utc"))) or datetime.now(timezone.utc).isoformat(),
                        "sample_window": _pick(row, ("sample_window", "evaluation_window", "window_label")),
                        "baseline": baseline,
                        "optimized": optimized,
                        "uplift": uplift,
                    }
        except Exception as exc:
            logger.exception("Predictive metrics query failed")
            return self._degraded_payload(
                "Unable to query predictive metrics.",
                extra={"error": str(exc), "baseline": {}, "optimized": {}, "uplift": {}},
            )

    def get_actions(self, model: str = "optimized", limit: Optional[int] = None) -> Dict[str, Any]:
        if not self.enabled:
            return self._disabled_payload("Predictive API is disabled (ENABLE_PREDICTIVE_API=false).")

        safe_model = "baseline" if str(model).strip().lower() == "baseline" else "optimized"
        safe_limit = max(1, min(500, int(limit or self.default_limit)))
        table = "delay_action_recommendations_current"
        try:
            with self._connect() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    if not self._table_exists(cur, self.schema, table):
                        return self._degraded_payload(
                            f"Missing mirror table: {self.schema}.{table}",
                            extra={"actions": [], "model": safe_model},
                        )
                    columns = set(self._table_columns(cur, self.schema, table))
                    if not columns:
                        return self._degraded_payload(
                            f"Table has no visible columns: {self.schema}.{table}",
                            extra={"actions": [], "model": safe_model},
                        )

                    where: List[str] = []
                    params: List[Any] = []
                    if "model_variant" in columns:
                        where.append('"model_variant" = %s')
                        params.append(safe_model)
                    elif "model" in columns:
                        where.append('"model" = %s')
                        params.append(safe_model)
                    where_sql = f" WHERE {' AND '.join(where)}" if where else ""
                    order_sql = (
                        ' ORDER BY "action_rank" ASC NULLS LAST'
                        if "action_rank" in columns
                        else (
                            ' ORDER BY "expected_delta_minutes" ASC NULLS LAST'
                            if "expected_delta_minutes" in columns
                            else ""
                        )
                    )
                    query = (
                        f"SELECT {', '.join(_qident(c) for c in sorted(columns))} "
                        f"FROM {self._table_ref(table)}{where_sql}{order_sql} LIMIT %s"
                    )
                    params.append(safe_limit)
                    cur.execute(query, params)
                    rows = cur.fetchall() or []
                    actions = []
                    as_of = None
                    for row in rows:
                        as_of = as_of or _normalize_dt(_pick(row, ("as_of_utc", "generated_at_utc", "updated_at")))
                        actions.append(
                            {
                                "flight_leg_id": _pick(row, ("flight_leg_id", "leg_id"), ""),
                                "flight_number": _pick(row, ("flight_number", "flight_no"), ""),
                                "action_rank": _pick(row, ("action_rank", "rank")),
                                "action_code": _pick(row, ("action_code", "recommendation_code"), ""),
                                "action_label": _pick(row, ("action_label", "recommendation"), ""),
                                "expected_delta_minutes": _to_float(
                                    _pick(row, ("expected_delta_minutes", "delta_minutes_expected"))
                                ),
                                "feasibility_status": _pick(row, ("feasibility_status", "feasible"), ""),
                                "confidence_band": _pick(row, ("confidence_band", "confidence"), ""),
                                "constraint_notes": _pick(row, ("constraint_notes", "constraint_reason"), ""),
                                "model_variant": _pick(row, ("model_variant", "model"), safe_model),
                            }
                        )

                    return {
                        "status": "ok" if actions else "empty",
                        "enabled": True,
                        "model": safe_model,
                        "as_of_utc": as_of or datetime.now(timezone.utc).isoformat(),
                        "row_count": len(actions),
                        "actions": actions,
                    }
        except Exception as exc:
            logger.exception("Predictive actions query failed")
            return self._degraded_payload(
                "Unable to query predictive actions.",
                extra={"error": str(exc), "actions": [], "model": safe_model},
            )

    def get_decision_metrics(self) -> Dict[str, Any]:
        if not self.enabled:
            return self._disabled_payload("Predictive API is disabled (ENABLE_PREDICTIVE_API=false).")

        table = "delay_decision_trace"
        try:
            with self._connect() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    if not self._table_exists(cur, self.schema, table):
                        return self._degraded_payload(
                            f"Missing mirror table: {self.schema}.{table}",
                            extra={"metrics": {}},
                        )
                    columns = set(self._table_columns(cur, self.schema, table))
                    if not columns:
                        return self._degraded_payload(
                            f"Table has no visible columns: {self.schema}.{table}",
                            extra={"metrics": {}},
                        )

                    select_exprs = ['COUNT(*)::INT AS total_decisions']
                    if "override_reason" in columns:
                        select_exprs.append("SUM(CASE WHEN override_reason IS NOT NULL THEN 1 ELSE 0 END)::INT AS override_count")
                    if "approved_by" in columns:
                        select_exprs.append("SUM(CASE WHEN approved_by IS NOT NULL THEN 1 ELSE 0 END)::INT AS approved_count")
                    if "feasibility_status" in columns:
                        select_exprs.append(
                            "SUM(CASE WHEN LOWER(feasibility_status) IN ('feasible','ok','pass') THEN 1 ELSE 0 END)::INT AS feasible_count"
                        )
                    if "model_variant" in columns:
                        select_exprs.append("COUNT(DISTINCT model_variant)::INT AS model_variant_count")

                    query = f"SELECT {', '.join(select_exprs)} FROM {self._table_ref(table)}"
                    cur.execute(query)
                    row = cur.fetchone() or {}

                    metrics = {
                        "total_decisions": int(row.get("total_decisions", 0) or 0),
                        "override_count": int(row.get("override_count", 0) or 0),
                        "approved_count": int(row.get("approved_count", 0) or 0),
                        "feasible_count": int(row.get("feasible_count", 0) or 0),
                        "model_variant_count": int(row.get("model_variant_count", 0) or 0),
                    }
                    return {
                        "status": "ok",
                        "enabled": True,
                        "as_of_utc": datetime.now(timezone.utc).isoformat(),
                        "metrics": metrics,
                    }
        except Exception as exc:
            logger.exception("Decision metrics query failed")
            return self._degraded_payload(
                "Unable to query predictive decision metrics.",
                extra={"error": str(exc), "metrics": {}},
            )
