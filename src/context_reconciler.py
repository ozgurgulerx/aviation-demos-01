#!/usr/bin/env python3
"""
Context reconciliation utilities for mixed-source retrieval.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

SOURCE_PRIORITY_DEFAULT = (
    "KQL",
    "GRAPH",
    "NOSQL",
    "SQL",
    "VECTOR_REG",
    "VECTOR_OPS",
    "VECTOR_AIRPORT",
)

AUTHORITY_DEFAULT = {
    "KQL": 1.0,
    "SQL": 0.95,
    "GRAPH": 0.9,
    "NOSQL": 0.8,
    "VECTOR_REG": 0.75,
    "VECTOR_OPS": 0.75,
    "VECTOR_AIRPORT": 0.75,
}

FRESHNESS_DEFAULT = {
    "KQL": 0.95,
    "GRAPH": 0.7,
    "NOSQL": 0.7,
    "SQL": 0.75,
    "VECTOR_REG": 0.65,
    "VECTOR_OPS": 0.65,
    "VECTOR_AIRPORT": 0.65,
}

PER_SOURCE_LIMIT_DEFAULT = {
    "SQL": 12,
    "KQL": 8,
    "GRAPH": 8,
    "NOSQL": 8,
    "VECTOR_REG": 6,
    "VECTOR_OPS": 6,
    "VECTOR_AIRPORT": 6,
}

DEFAULT_FUSION_WEIGHTS = {
    "relevance": 0.45,
    "authority": 0.30,
    "freshness": 0.20,
    "required_bonus": 0.05,
    "conflict_penalty": 0.10,
}


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _parse_ts(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        pass
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y-%m-%d %H:%M:%S"):
        try:
            dt = datetime.strptime(raw, fmt).replace(tzinfo=timezone.utc)
            return dt
        except Exception:
            continue
    return None


def _freshness_score(source: str, row: Dict[str, Any]) -> float:
    for key in ("timestamp", "event_date", "startDate", "endDate", "ingested_at", "start_utc", "end_utc"):
        dt = _parse_ts(row.get(key))
        if dt is None:
            continue
        delta_sec = abs((datetime.now(timezone.utc) - dt).total_seconds())
        days = delta_sec / 86400.0
        if days <= 1:
            return 1.0
        if days <= 7:
            return 0.85
        if days <= 30:
            return 0.65
        return 0.45
    return FRESHNESS_DEFAULT.get(source, 0.5)


def _identifier(row: Dict[str, Any], fallback: str) -> str:
    for key in ("asrs_report_id", "id", "notamNumber", "metric", "facilityDesignator", "title"):
        value = row.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return fallback


def _raw_relevance(row: Dict[str, Any]) -> float:
    for key in ("__vector_score_final", "@search.reranker_score", "@search.score", "score", "confidence"):
        if key in row:
            return _as_float(row.get(key))
    return 0.0


def _row_has_error(row: Dict[str, Any]) -> bool:
    if not isinstance(row, dict):
        return False
    return bool(row.get("error") or row.get("error_code"))


def normalize_scores(items: List[Dict[str, Any]]) -> None:
    by_source: Dict[str, List[float]] = {}
    for item in items:
        by_source.setdefault(item["source"], []).append(item.get("raw_relevance", 0.0))

    for item in items:
        source = item["source"]
        values = by_source.get(source, [])
        if not values:
            item["normalized_relevance"] = 0.0
            continue
        min_v = min(values)
        max_v = max(values)
        raw = _as_float(item.get("raw_relevance"))
        if max_v - min_v <= 1e-9:
            item["normalized_relevance"] = 1.0 if raw > 0 else 0.0
            continue
        item["normalized_relevance"] = (raw - min_v) / (max_v - min_v)


def compute_fusion_score(items: List[Dict[str, Any]], weights: Optional[Dict[str, float]] = None) -> None:
    w = dict(DEFAULT_FUSION_WEIGHTS)
    if weights:
        for key, value in weights.items():
            w[key] = _as_float(value, w.get(key, 0.0))

    for item in items:
        required_bonus = 1.0 if item.get("required_evidence") else 0.0
        conflict_penalty = _as_float(item.get("conflict_penalty", 0.0), 0.0)
        score = (
            w["relevance"] * _as_float(item.get("normalized_relevance", 0.0))
            + w["authority"] * _as_float(item.get("authority_score", 0.0))
            + w["freshness"] * _as_float(item.get("freshness_score", 0.0))
            + w["required_bonus"] * required_bonus
            - w["conflict_penalty"] * conflict_penalty
        )
        item["fusion_score"] = max(0.0, min(1.0, score))


def compute_rrf_scores(items: List[Dict[str, Any]], k: int = 60) -> None:
    """Reciprocal Rank Fusion: rank items within each source and compute RRF score."""
    by_source: Dict[str, List[Dict[str, Any]]] = {}
    for item in items:
        by_source.setdefault(str(item.get("source", "")), []).append(item)

    for source_items in by_source.values():
        source_items.sort(key=lambda it: -_as_float(it.get("raw_relevance", 0.0)))
        for rank, item in enumerate(source_items):
            item.setdefault("_rrf_contributions", [])
            item["_rrf_contributions"].append(1.0 / (k + rank))

    for item in items:
        contributions = item.pop("_rrf_contributions", [])
        rrf_raw = sum(contributions)
        item["rrf_score_raw"] = rrf_raw

    # Normalize to [0, 1].
    max_rrf = max((_as_float(it.get("rrf_score_raw", 0.0)) for it in items), default=0.0)
    for item in items:
        raw = _as_float(item.get("rrf_score_raw", 0.0))
        item["rrf_score"] = (raw / max_rrf) if max_rrf > 1e-9 else 0.0


def dedupe_items(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    seen: set[Tuple[str, str, str]] = set()
    for item in items:
        source = str(item.get("source", ""))
        row = item.get("row", {}) or {}
        ident = str(item.get("identifier", ""))
        content = str(row.get("content", "")).strip()[:160]
        key = (source, ident, content)
        if key in seen:
            continue
        seen.add(key)
        out.append(item)
    return out


def build_evidence_slots(
    required_evidence: List[Dict[str, Any]],
    items: List[Dict[str, Any]],
    authoritative_map: Dict[str, List[str]],
) -> Dict[str, Any]:
    slots: List[Dict[str, Any]] = []
    required_total = 0
    required_filled = 0
    optional_filled = 0
    missing_required: List[str] = []

    for req in required_evidence:
        name = str(req.get("name", "")).strip()
        if not name:
            continue
        optional = bool(req.get("optional", False))
        required_total += 0 if optional else 1
        candidates = [
            it
            for it in items
            if str(it.get("evidence_type", "")).strip() == name
            and not _row_has_error(it.get("row", {}) or {})
        ]
        authoritative_candidates: List[Dict[str, Any]] = []
        if not candidates:
            for source in authoritative_map.get(name, []):
                authoritative_candidates.extend([it for it in items if it.get("source") == source][:2])
            # Keep candidates deterministic and compact for UI/telemetry use.
            dedup_seen: set[tuple[str, str]] = set()
            deduped_authoritative: List[Dict[str, Any]] = []
            for candidate in authoritative_candidates:
                key = (str(candidate.get("source", "")), str(candidate.get("identifier", "")))
                if key in dedup_seen:
                    continue
                dedup_seen.add(key)
                deduped_authoritative.append(candidate)
            authoritative_candidates = deduped_authoritative[:3]

        # Only explicit evidence-type matches count as filled coverage.
        status = "filled" if candidates else "missing"
        if status == "filled" and optional:
            optional_filled += 1
        if status == "filled" and not optional:
            required_filled += 1
        if status == "missing" and not optional:
            missing_required.append(name)
        slots.append(
            {
                "evidence": name,
                "optional": optional,
                "status": status,
                "candidates": [
                    {
                        "source": c.get("source"),
                        "identifier": c.get("identifier"),
                        "fusion_score": c.get("fusion_score"),
                    }
                    for c in candidates[:3]
                ],
                "authoritative_candidates": [
                    {
                        "source": c.get("source"),
                        "identifier": c.get("identifier"),
                        "fusion_score": c.get("fusion_score"),
                    }
                    for c in authoritative_candidates
                ],
            }
        )

    return {
        "slots": slots,
        "required_total": required_total,
        "required_filled": required_filled,
        "optional_filled": optional_filled,
        "missing_required": missing_required,
    }


def detect_conflicts(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    conflicts: List[Dict[str, Any]] = []
    numeric_by_metric: Dict[str, List[float]] = {}
    status_by_entity: Dict[str, set[str]] = {}

    for item in items:
        row = item.get("row", {}) or {}
        metric = str(row.get("metric", "")).strip().lower()
        value = row.get("value")
        if metric and isinstance(value, (int, float)):
            numeric_by_metric.setdefault(metric, []).append(float(value))

        status = str(row.get("status", "")).strip().lower()
        entity = str(row.get("id") or row.get("facilityDesignator") or row.get("asrs_report_id") or "").strip()
        if status and entity:
            status_by_entity.setdefault(entity, set()).add(status)

    for metric, values in numeric_by_metric.items():
        if len(values) < 2:
            continue
        lo = min(values)
        hi = max(values)
        if lo == 0 and hi > 0:
            ratio = float("inf")
        elif lo == 0:
            ratio = 0.0
        else:
            ratio = hi / lo
        if ratio >= 1.25:
            conflicts.append(
                {
                    "type": "numeric",
                    "signal": metric,
                    "severity": "high" if ratio >= 2.0 else "medium",
                    "detail": f"metric spread detected ({lo}..{hi})",
                }
            )

    contradictory_pairs = [("open", "closed"), ("active", "inactive"), ("yes", "no")]
    for entity, states in status_by_entity.items():
        for a, b in contradictory_pairs:
            if a in states and b in states:
                conflicts.append(
                    {
                        "type": "status",
                        "signal": entity,
                        "severity": "medium",
                        "detail": f"conflicting statuses: {a}/{b}",
                    }
                )
                break

    return {
        "count": len(conflicts),
        "items": conflicts[:20],
        "severity": "high" if any(c.get("severity") == "high" for c in conflicts) else ("medium" if conflicts else "none"),
    }


def reconcile_context(
    source_results: Dict[str, List[Dict[str, Any]]],
    required_evidence: Optional[List[Dict[str, Any]]] = None,
    authoritative_map: Optional[Dict[str, List[str]]] = None,
    source_priority: Optional[List[str]] = None,
    per_source_limits: Optional[Dict[str, int]] = None,
    weights: Optional[Dict[str, float]] = None,
    enable_evidence_slotting: bool = True,
    enable_conflict_detection: bool = True,
    enable_rrf: bool = False,
) -> Dict[str, Any]:
    required = list(required_evidence or [])
    authoritative = dict(authoritative_map or {})
    priority_order = list(source_priority or SOURCE_PRIORITY_DEFAULT)
    priority_rank = {src: idx for idx, src in enumerate(priority_order)}
    limits = dict(PER_SOURCE_LIMIT_DEFAULT)
    if per_source_limits:
        for source, limit in per_source_limits.items():
            limits[source] = max(1, int(limit))

    items: List[Dict[str, Any]] = []
    for source in sorted(source_results.keys(), key=lambda s: priority_rank.get(s, 999)):
        rows = source_results.get(source, []) or []
        for idx, row in enumerate(rows):
            if not isinstance(row, dict):
                continue
            ident = _identifier(row, f"{source.lower()}_{idx}")
            evidence_type = str(row.get("__evidence_type", "")).strip()
            item = {
                "source": source,
                "row": dict(row),
                "row_index": idx,
                "identifier": ident,
                "evidence_type": evidence_type,
                "raw_relevance": _raw_relevance(row),
                "authority_score": AUTHORITY_DEFAULT.get(source, 0.5),
                "freshness_score": _freshness_score(source, row),
                "required_evidence": evidence_type in {str(x.get("name", "")).strip() for x in required},
                "conflict_penalty": 0.0,
            }
            items.append(item)

    normalize_scores(items)
    compute_fusion_score(items, weights=weights)

    if enable_rrf and len(items) > 1:
        compute_rrf_scores(items)
        for item in items:
            fusion = _as_float(item.get("fusion_score", 0.0))
            rrf = _as_float(item.get("rrf_score", 0.0))
            item["fusion_score"] = max(0.0, min(1.0, 0.5 * fusion + 0.5 * rrf))

    items.sort(
        key=lambda it: (
            priority_rank.get(it.get("source"), 999),
            -_as_float(it.get("fusion_score", 0.0)),
            it.get("identifier", ""),
            _as_float(it.get("row_index", 0.0)),
        )
    )
    deduped = dedupe_items(items)

    selected: List[Dict[str, Any]] = []
    per_source_count: Dict[str, int] = {}
    for item in deduped:
        source = str(item.get("source"))
        if per_source_count.get(source, 0) >= limits.get(source, 8):
            continue
        per_source_count[source] = per_source_count.get(source, 0) + 1
        selected.append(item)

    ordered_source_results: Dict[str, List[Dict[str, Any]]] = {}
    for source in sorted(source_results.keys(), key=lambda s: priority_rank.get(s, 999)):
        source_items = [it for it in selected if it.get("source") == source]
        source_items.sort(key=lambda it: -_as_float(it.get("fusion_score", 0.0)))
        ordered_rows: List[Dict[str, Any]] = []
        for it in source_items:
            row = dict(it.get("row", {}))
            row["__fusion_score"] = round(_as_float(it.get("fusion_score", 0.0)), 6)
            row["__normalized_relevance"] = round(_as_float(it.get("normalized_relevance", 0.0)), 6)
            row["__authority_score"] = round(_as_float(it.get("authority_score", 0.0)), 6)
            row["__freshness_score"] = round(_as_float(it.get("freshness_score", 0.0)), 6)
            ordered_rows.append(row)
        if ordered_rows:
            ordered_source_results[source] = ordered_rows

    coverage_summary = {
        "required_total": 0,
        "required_filled": 0,
        "optional_filled": 0,
        "missing_required": [],
        "slots": [],
    }
    if enable_evidence_slotting and required:
        coverage_summary = build_evidence_slots(required, selected, authoritative)

    conflict_summary = {"count": 0, "items": [], "severity": "none"}
    if enable_conflict_detection:
        conflict_summary = detect_conflicts(selected)
        # Apply conflict penalty to items involved in detected conflicts.
        if conflict_summary.get("count", 0) > 0:
            conflict_signals = set()
            for c in conflict_summary.get("items", []):
                signal = str(c.get("signal", "")).strip().lower()
                if signal:
                    conflict_signals.add(signal)
            if conflict_signals:
                for item in selected:
                    row = item.get("row", {}) or {}
                    metric = str(row.get("metric", "")).strip().lower()
                    entity = str(row.get("id") or row.get("facilityDesignator") or row.get("asrs_report_id") or "").strip().lower()
                    if metric in conflict_signals or entity in conflict_signals:
                        item["conflict_penalty"] = 0.5
                        # Recompute fusion score with penalty.
                        w = dict(DEFAULT_FUSION_WEIGHTS)
                        required_bonus = 1.0 if item.get("required_evidence") else 0.0
                        score = (
                            w["relevance"] * _as_float(item.get("normalized_relevance", 0.0))
                            + w["authority"] * _as_float(item.get("authority_score", 0.0))
                            + w["freshness"] * _as_float(item.get("freshness_score", 0.0))
                            + w["required_bonus"] * required_bonus
                            - w["conflict_penalty"] * 0.5
                        )
                        item["fusion_score"] = max(0.0, min(1.0, score))
                # Re-sort after penalty application.
                selected.sort(
                    key=lambda it: (
                        priority_rank.get(str(it.get("source", "")), 999),
                        -_as_float(it.get("fusion_score", 0.0)),
                        str(it.get("identifier", "")),
                        _as_float(it.get("row_index", 0.0)),
                    )
                )
                # Update ordered_source_results with adjusted scores.
                ordered_source_results.clear()
                for source in sorted(source_results.keys(), key=lambda s: priority_rank.get(s, 999)):
                    source_items = [it for it in selected if it.get("source") == source]
                    source_items.sort(key=lambda it: -_as_float(it.get("fusion_score", 0.0)))
                    ordered_rows_updated: List[Dict[str, Any]] = []
                    for it in source_items:
                        row_copy = dict(it.get("row", {}))
                        row_copy["__fusion_score"] = round(_as_float(it.get("fusion_score", 0.0)), 6)
                        row_copy["__normalized_relevance"] = round(_as_float(it.get("normalized_relevance", 0.0)), 6)
                        row_copy["__authority_score"] = round(_as_float(it.get("authority_score", 0.0)), 6)
                        row_copy["__freshness_score"] = round(_as_float(it.get("freshness_score", 0.0)), 6)
                        ordered_rows_updated.append(row_copy)
                    if ordered_rows_updated:
                        ordered_source_results[source] = ordered_rows_updated

    reconciled_items = [
        {
            "source": it.get("source"),
            "identifier": it.get("identifier"),
            "evidence_type": it.get("evidence_type"),
            "fusion_score": round(_as_float(it.get("fusion_score", 0.0)), 6),
            "authority_score": round(_as_float(it.get("authority_score", 0.0)), 6),
            "freshness_score": round(_as_float(it.get("freshness_score", 0.0)), 6),
            "normalized_relevance": round(_as_float(it.get("normalized_relevance", 0.0)), 6),
        }
        for it in selected[:80]
    ]

    return {
        "source_results": ordered_source_results,
        "reconciled_items": reconciled_items,
        "coverage_summary": coverage_summary,
        "conflict_summary": conflict_summary,
    }
