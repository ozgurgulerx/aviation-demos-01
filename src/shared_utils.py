#!/usr/bin/env python3
"""
Shared utilities for the aviation-rag backend.

Consolidates duplicated helpers, constants, and configuration that were
previously defined independently in multiple modules.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, Dict, List

from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Azure OpenAI API version (previously duplicated in 4+ files)
# ---------------------------------------------------------------------------
OPENAI_API_VERSION = os.getenv("AZURE_OPENAI_API_VERSION", "2024-06-01")

# ---------------------------------------------------------------------------
# Environment helpers
# ---------------------------------------------------------------------------

def env_bool(name: str, default: bool) -> bool:
    """Read a boolean from an environment variable."""
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    return raw.lower() in {"1", "true", "yes", "y", "on"}


def env_int(name: str, default: int) -> int:
    """Read an int from an environment variable."""
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except Exception:
        return default


def env_csv(name: str, default: str) -> List[str]:
    """Read a comma-separated list from an environment variable."""
    raw = (os.getenv(name, default) or default).strip()
    if not raw:
        return []
    seen: set[str] = set()
    out: List[str] = []
    for token in raw.split(","):
        value = token.strip()
        if not value:
            continue
        lowered = value.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        out.append(lowered)
    return out


# ---------------------------------------------------------------------------
# Time helpers
# ---------------------------------------------------------------------------

def utc_now() -> str:
    """ISO-formatted UTC timestamp."""
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# Model capability helpers
# ---------------------------------------------------------------------------

def supports_explicit_temperature(model_name: str) -> bool:
    """GPT-5/o-series deployments reject explicit temperature overrides."""
    model = (model_name or "").strip().lower()
    normalized = model.replace("-", "").replace("_", "")
    return not (
        model.startswith("gpt-5")
        or normalized.startswith("gpt5")
        or "gpt5" in normalized
        or model.startswith("o1")
        or model.startswith("o3")
        or model.startswith("o4")
        or normalized == "modelrouter"
    )


# ---------------------------------------------------------------------------
# Row preview helpers (used by af_context_provider and plan_executor)
# ---------------------------------------------------------------------------

def safe_preview_value(value: Any, max_chars: int = 180) -> Any:
    """Safely format a value for preview display."""
    import json as _json

    if value is None or isinstance(value, (int, float, bool)):
        return value

    if isinstance(value, str):
        return value if len(value) <= max_chars else value[: max_chars - 3] + "..."

    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            pass

    if isinstance(value, (dict, list, tuple)):
        try:
            serialized = _json.dumps(value, ensure_ascii=True)
        except Exception:
            serialized = str(value)
        return serialized if len(serialized) <= max_chars else serialized[: max_chars - 3] + "..."

    rendered = str(value)
    return rendered if len(rendered) <= max_chars else rendered[: max_chars - 3] + "..."


def build_rows_preview(
    rows: List[Dict[str, Any]],
    max_rows: int = 5,
    max_columns: int = 8,
    max_chars: int = 180,
) -> tuple[List[str], List[Dict[str, Any]], bool]:
    """Build a compact preview of result rows for SSE events."""
    if not rows:
        return [], [], False

    hidden_keys = {"content_vector"}
    columns: List[str] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        for key in row.keys():
            if not isinstance(key, str) or key.startswith("__") or key in hidden_keys:
                continue
            if key not in columns:
                columns.append(key)
                if len(columns) >= max_columns:
                    break
        if len(columns) >= max_columns:
            break

    preview: List[Dict[str, Any]] = []
    for row in rows[:max_rows]:
        if not isinstance(row, dict):
            continue
        item: Dict[str, Any] = {}
        for column in columns:
            if column in row:
                item[column] = safe_preview_value(row[column], max_chars=max_chars)
        if item:
            preview.append(item)

    return columns, preview, len(rows) > len(preview)


# ---------------------------------------------------------------------------
# Airport / ICAO constants
# ---------------------------------------------------------------------------

ENGLISH_4LETTER_BLOCKLIST: set[str] = {
    "WHAT", "WITH", "FROM", "YOUR", "SHOW", "THIS", "THAT", "WHEN",
    "WILL", "HAVE", "DOES", "BEEN", "WERE", "THEY", "THEM", "THEN",
    "THAN", "EACH", "MADE", "FIND", "HERE", "MANY", "SOME", "LIKE",
    "LONG", "MAKE", "JUST", "OVER", "SUCH", "TAKE", "YEAR", "ALSO",
    "INTO", "MOST", "ONLY", "COME", "VERY", "WELL", "BACK", "MUCH",
    "GIVE", "EVEN", "WANT", "GOOD", "LOOK", "LAST", "TELL", "NEED",
    "NEAR", "AREA", "BOTH", "KEEP", "HELP", "LINE", "TURN", "MOVE",
    "LIVE", "REAL", "LEFT", "SAME", "ABLE", "OPEN", "SEEM", "SURE",
    "HIGH", "RISK", "EVER", "NEXT", "TYPE", "LIST", "DATA", "USED",
    "BEST", "DONE", "FULL", "MUST", "KNOW", "TIME", "WENT", "GATE",
    "TAXI", "LAND", "HOLD", "TAKE", "CALL", "NOTE",
}

CITY_AIRPORT_MAP: Dict[str, List[str]] = {
    "new york": ["KJFK", "KLGA", "KEWR"],
    "nyc": ["KJFK", "KLGA", "KEWR"],
    "istanbul": ["LTFM", "LTBA", "LTFJ"],
    "london": ["EGLL", "EGKK", "EGSS"],
}

IATA_TO_ICAO_MAP: Dict[str, str] = {
    "JFK": "KJFK",
    "LGA": "KLGA",
    "EWR": "KEWR",
    "IST": "LTFM",
    "SAW": "LTFJ",
}


# ---------------------------------------------------------------------------
# Tool name canonicalization
# ---------------------------------------------------------------------------

KNOWN_TOOLS: set[str] = {"KQL", "SQL", "GRAPH", "VECTOR_REG", "VECTOR_OPS", "VECTOR_AIRPORT", "NOSQL"}

TOOL_ALIASES: Dict[str, str] = {
    "EVENTHOUSEKQL": "KQL",
    "KQL": "KQL",
    "WAREHOUSESQL": "SQL",
    "SQL": "SQL",
    "FABRICGRAPH": "GRAPH",
    "GRAPHTRAVERSAL": "GRAPH",
    "GRAPH": "GRAPH",
    "FOUNDRYIQ": "VECTOR_REG",
    "AZUREAISEARCH": "VECTOR_REG",
    "VECTOR_REG": "VECTOR_REG",
    "VECTOR_OPS": "VECTOR_OPS",
    "VECTOR_AIRPORT": "VECTOR_AIRPORT",
    "NOSQL": "NOSQL",
    "LAKEHOUSEDELTA": "KQL",
}


def canon_tool(raw: str) -> str:
    """Canonicalize a tool name to its standard form."""
    value = (raw or "").strip().upper()
    mapped = TOOL_ALIASES.get(value, value)
    return mapped if mapped in KNOWN_TOOLS else ""
