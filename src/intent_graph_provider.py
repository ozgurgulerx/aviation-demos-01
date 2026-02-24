#!/usr/bin/env python3
"""
Intent graph provider.

Primary source can be Fabric Graph endpoint.
Fallback source is static/default in-process JSON graph.
"""

from __future__ import annotations

import json
import logging
import os
import base64
import time
import urllib.parse
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List

logger = logging.getLogger(__name__)

import shared_utils  # noqa: F401  — ensures load_dotenv() runs

FABRIC_GRAPH_ENDPOINT = os.getenv("FABRIC_GRAPH_ENDPOINT", "").strip()
_FABRIC_DEFAULT_SCOPE = "https://api.fabric.microsoft.com/.default"
_fabric_token_cache: Dict[str, Dict[str, Any]] = {}


def _allow_static_fabric_bearer() -> bool:
    return os.getenv("ALLOW_STATIC_FABRIC_BEARER", "false").strip().lower() in {
        "1", "true", "yes", "y", "on"
    }


def _token_min_ttl_seconds() -> int:
    raw = os.getenv("FABRIC_TOKEN_MIN_TTL_SECONDS", "120").strip()
    try:
        return max(0, int(raw))
    except Exception:
        return 120


def _token_ttl_seconds(token: str) -> int | None:
    raw = str(token or "").strip()
    if not raw:
        return None
    parts = raw.split(".")
    if len(parts) < 2:
        return None
    try:
        payload = parts[1] + "=" * (-len(parts[1]) % 4)
        claims = json.loads(base64.urlsafe_b64decode(payload.encode("utf-8")).decode("utf-8", errors="ignore"))
        exp = int(claims.get("exp", 0))
        return exp - int(time.time())
    except Exception:
        return None


def _scope_for_graph_endpoint() -> str:
    endpoint = (FABRIC_GRAPH_ENDPOINT or "").strip()
    parsed = urllib.parse.urlparse(endpoint)
    host = str(parsed.hostname or "").lower()
    if parsed.scheme and parsed.hostname and "kusto.fabric.microsoft.com" in host:
        return f"{parsed.scheme}://{parsed.hostname}/.default"
    return _FABRIC_DEFAULT_SCOPE


def _get_fabric_bearer_token() -> str:
    """Acquire refreshable Fabric token; allow static bearer only when explicitly enabled."""
    min_ttl = _token_min_ttl_seconds()
    refresh_buffer = max(30, min_ttl)
    scope = _scope_for_graph_endpoint()
    client_id = os.getenv("FABRIC_CLIENT_ID", "").strip()
    client_secret = os.getenv("FABRIC_CLIENT_SECRET", "").strip()
    tenant_id = os.getenv("FABRIC_TENANT_ID", "").strip()
    if client_id and client_secret and tenant_id:
        cache_entry = _fabric_token_cache.get(scope, {})
        cached = str(cache_entry.get("token", "") or "")
        expires_at = float(cache_entry.get("expires_at", 0) or 0)

        def _cached_token_is_fresh() -> bool:
            if not cached:
                return False
            now = time.time()
            if now >= (expires_at - refresh_buffer):
                return False
            ttl = _token_ttl_seconds(cached)
            return ttl is None or ttl >= min_ttl

        if _cached_token_is_fresh():
            return cached
        token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
        body = urllib.parse.urlencode(
            {
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_secret": client_secret,
                "scope": scope,
            }
        ).encode("utf-8")
        req = urllib.request.Request(token_url, data=body, method="POST")
        req.add_header("Content-Type", "application/x-www-form-urlencoded")
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                payload = json.loads(resp.read())
            token = str(payload.get("access_token", "") or "")
            ttl = _token_ttl_seconds(token)
            if token and (ttl is None or ttl >= min_ttl):
                expires_in = int(payload.get("expires_in", 3600))
                _fabric_token_cache[scope] = {
                    "token": token,
                    "expires_at": time.time() + expires_in,
                }
                return token
        except Exception:
            logger.warning("Intent graph SP token acquisition failed", exc_info=True)
            if _cached_token_is_fresh():
                return cached
            if cached:
                logger.warning("Intent graph cached SP token is stale; ignoring cached fallback")
                _fabric_token_cache.pop(scope, None)

    static = os.getenv("FABRIC_BEARER_TOKEN", "").strip()
    if static and _allow_static_fabric_bearer():
        ttl = _token_ttl_seconds(static)
        if ttl is None or ttl >= min_ttl:
            return static
    return ""
INTENT_GRAPH_JSON_PATH = os.getenv("INTENT_GRAPH_JSON_PATH", "").strip()

ROOT = Path(__file__).resolve().parents[1]

DEFAULT_INTENT_GRAPH: Dict[str, Any] = {
    "intents": [
        {"name": "PilotBrief.Departure"},
        {"name": "PilotBrief.Arrival"},
        {"name": "Disruption.Explain"},
        {"name": "Policy.Check"},
        {"name": "Replay.History"},
        {"name": "Analytics.Compare"},
        {"name": "Fleet.Status"},
        {"name": "RouteNetwork.Query"},
        {"name": "Safety.Trend"},
        {"name": "Airport.Info"},
    ],
    "evidence": [
        {"name": "METAR", "requires_citations": False},
        {"name": "TAF", "requires_citations": False},
        {"name": "NOTAM", "requires_citations": False},
        {"name": "RunwayConstraints", "requires_citations": False},
        {"name": "Hazards", "requires_citations": False},
        {"name": "SOPClause", "requires_citations": True},
        {"name": "FleetData", "requires_citations": False},
        {"name": "RouteData", "requires_citations": False},
        {"name": "SafetyStats", "requires_citations": False},
        {"name": "AirportData", "requires_citations": False},
        {"name": "IncidentNarrative", "requires_citations": True},
        {"name": "RegulatoryDoc", "requires_citations": True},
        {"name": "DelayAnalytics", "requires_citations": False},
    ],
    "tools": [
        {"name": "GRAPH", "kind": "graph"},
        {"name": "KQL", "kind": "kql"},
        {"name": "SQL", "kind": "sql"},
        {"name": "NOSQL", "kind": "nosql"},
        {"name": "VECTOR_REG", "kind": "search"},
        {"name": "VECTOR_OPS", "kind": "search"},
        {"name": "VECTOR_AIRPORT", "kind": "search"},
        {"name": "FABRIC_SQL", "kind": "sql"},
    ],
    "requires": [
        {"intent": "PilotBrief.Departure", "evidence": "METAR", "optional": False},
        {"intent": "PilotBrief.Departure", "evidence": "TAF", "optional": False},
        {"intent": "PilotBrief.Departure", "evidence": "NOTAM", "optional": False},
        {"intent": "PilotBrief.Departure", "evidence": "RunwayConstraints", "optional": False},
        {"intent": "PilotBrief.Departure", "evidence": "Hazards", "optional": True},
        {"intent": "PilotBrief.Arrival", "evidence": "METAR", "optional": False},
        {"intent": "PilotBrief.Arrival", "evidence": "TAF", "optional": False},
        {"intent": "PilotBrief.Arrival", "evidence": "NOTAM", "optional": False},
        {"intent": "PilotBrief.Arrival", "evidence": "RunwayConstraints", "optional": False},
        {"intent": "Disruption.Explain", "evidence": "Hazards", "optional": False},
        {"intent": "Disruption.Explain", "evidence": "NOTAM", "optional": False},
        {"intent": "Disruption.Explain", "evidence": "METAR", "optional": True},
        {"intent": "Disruption.Explain", "evidence": "SOPClause", "optional": True},
        {"intent": "Policy.Check", "evidence": "SOPClause", "optional": False},
        {"intent": "Policy.Check", "evidence": "NOTAM", "optional": True},
        {"intent": "Replay.History", "evidence": "METAR", "optional": False},
        {"intent": "Replay.History", "evidence": "Hazards", "optional": False},
        {"intent": "Replay.History", "evidence": "NOTAM", "optional": True},
        {"intent": "Replay.History", "evidence": "IncidentNarrative", "optional": False},
        {"intent": "Replay.History", "evidence": "SafetyStats", "optional": True},
        # Analytics.Compare
        {"intent": "Analytics.Compare", "evidence": "SafetyStats", "optional": False},
        {"intent": "Analytics.Compare", "evidence": "AirportData", "optional": True},
        # Fleet.Status
        {"intent": "Fleet.Status", "evidence": "FleetData", "optional": False},
        {"intent": "Fleet.Status", "evidence": "SOPClause", "optional": True},
        {"intent": "Fleet.Status", "evidence": "DelayAnalytics", "optional": True},
        # RouteNetwork.Query
        {"intent": "RouteNetwork.Query", "evidence": "RouteData", "optional": False},
        {"intent": "RouteNetwork.Query", "evidence": "AirportData", "optional": True},
        # Safety.Trend
        {"intent": "Safety.Trend", "evidence": "SafetyStats", "optional": False},
        {"intent": "Safety.Trend", "evidence": "Hazards", "optional": True},
        # Airport.Info
        {"intent": "Airport.Info", "evidence": "AirportData", "optional": False},
        {"intent": "Airport.Info", "evidence": "RunwayConstraints", "optional": True},
        {"intent": "Airport.Info", "evidence": "NOTAM", "optional": True},
        # IncidentNarrative — for queries needing narrative/similarity/lessons
        {"intent": "Safety.Trend", "evidence": "IncidentNarrative", "optional": False},
        {"intent": "Disruption.Explain", "evidence": "IncidentNarrative", "optional": True},
        {"intent": "PilotBrief.Departure", "evidence": "IncidentNarrative", "optional": True},
        # RegulatoryDoc — for compliance/regulatory queries
        {"intent": "Policy.Check", "evidence": "RegulatoryDoc", "optional": False},
        {"intent": "PilotBrief.Departure", "evidence": "RegulatoryDoc", "optional": True},
        {"intent": "PilotBrief.Arrival", "evidence": "RegulatoryDoc", "optional": True},
        # DelayAnalytics — for delay/performance queries
        {"intent": "Analytics.Compare", "evidence": "DelayAnalytics", "optional": False},
        {"intent": "Disruption.Explain", "evidence": "DelayAnalytics", "optional": True},
    ],
    "authoritative_in": [
        {"evidence": "METAR", "tool": "KQL", "priority": 1},
        {"evidence": "TAF", "tool": "KQL", "priority": 1},
        {"evidence": "NOTAM", "tool": "NOSQL", "priority": 1},
        {"evidence": "NOTAM", "tool": "VECTOR_REG", "priority": 2},
        {"evidence": "RunwayConstraints", "tool": "SQL", "priority": 1,
         "hint_tables": ["demo.ourairports_runways", "demo.ourairports_airports"]},
        {"evidence": "Hazards", "tool": "KQL", "priority": 1},
        {"evidence": "SOPClause", "tool": "VECTOR_REG", "priority": 1},
        {"evidence": "FleetData", "tool": "SQL", "priority": 1,
         "hint_tables": ["demo.ops_flight_legs", "demo.ops_mel_techlog_events"]},
        {"evidence": "RouteData", "tool": "GRAPH", "priority": 1},
        {"evidence": "RouteData", "tool": "SQL", "priority": 2},
        {"evidence": "SafetyStats", "tool": "SQL", "priority": 1},
        {"evidence": "SafetyStats", "tool": "VECTOR_OPS", "priority": 2},
        {"evidence": "SafetyStats", "tool": "FABRIC_SQL", "priority": 3},
        {"evidence": "AirportData", "tool": "SQL", "priority": 1,
         "hint_tables": ["demo.ourairports_airports", "demo.openflights_airports"]},
        {"evidence": "AirportData", "tool": "VECTOR_AIRPORT", "priority": 2},
        {"evidence": "IncidentNarrative", "tool": "VECTOR_OPS", "priority": 1},
        {"evidence": "IncidentNarrative", "tool": "SQL", "priority": 2},
        {"evidence": "RegulatoryDoc", "tool": "VECTOR_REG", "priority": 1},
        {"evidence": "RegulatoryDoc", "tool": "NOSQL", "priority": 2},
        {"evidence": "DelayAnalytics", "tool": "FABRIC_SQL", "priority": 1},
        {"evidence": "DelayAnalytics", "tool": "SQL", "priority": 2,
         "hint_tables": ["demo.ops_flight_legs", "demo.ops_turnaround_milestones", "demo.schedule_delay_causes"]},
        # Historical fallbacks — structured data for analysis/trends when KQL is unavailable
        {"evidence": "METAR", "tool": "SQL", "priority": 2},
        {"evidence": "TAF", "tool": "SQL", "priority": 2},
        {"evidence": "Hazards", "tool": "SQL", "priority": 2},
        # Supplementary vector/document sources
        {"evidence": "RunwayConstraints", "tool": "VECTOR_AIRPORT", "priority": 2},
        {"evidence": "FleetData", "tool": "FABRIC_SQL", "priority": 2},
        {"evidence": "SOPClause", "tool": "NOSQL", "priority": 2},
    ],
    "expansion_rules": [
        {"intent": "PilotBrief.Departure", "tool": "GRAPH", "reason": "airport->runway/navaid/notam/alternate expansion"},
        {"intent": "PilotBrief.Arrival", "tool": "GRAPH", "reason": "airport->runway/navaid/notam/alternate expansion"},
        {"intent": "Disruption.Explain", "tool": "GRAPH", "reason": "airport->notam/runway/alternate expansion"},
        {"intent": "RouteNetwork.Query", "tool": "GRAPH", "reason": "airport->route->airline network traversal"},
        {"intent": "Airport.Info", "tool": "GRAPH", "reason": "airport->runway/navaid/frequency expansion"},
        {"intent": "Safety.Trend", "tool": "GRAPH", "reason": "airport->asrs_reports expansion"},
        {"intent": "Fleet.Status", "tool": "GRAPH", "reason": "tail->flight_leg->crew/mel expansion"},
    ],
}


@dataclass
class IntentGraphSnapshot:
    data: Dict[str, Any]
    source: str

    def required_evidence_for_intent(self, intent_name: str) -> List[Dict[str, Any]]:
        requires = self.data.get("requires") or []
        evidence_map = {
            str(e.get("name")): dict(e)
            for e in (self.data.get("evidence") or [])
            if isinstance(e, dict)
        }
        out: List[Dict[str, Any]] = []
        for row in requires:
            if not isinstance(row, dict):
                continue
            if str(row.get("intent")) != intent_name:
                continue
            name = str(row.get("evidence", "")).strip()
            meta = dict(evidence_map.get(name, {}))
            out.append(
                {
                    "name": name,
                    "optional": bool(row.get("optional", False)),
                    "requires_citations": bool(meta.get("requires_citations", False)),
                }
            )
        return out

    def expansion_rules_for_intent(self, intent_name: str) -> List[Dict[str, Any]]:
        rules = self.data.get("expansion_rules") or []
        return [dict(r) for r in rules
                if isinstance(r, dict) and str(r.get("intent")) == intent_name]

    def tools_for_evidence(self, evidence_name: str) -> List[str]:
        rows = self.data.get("authoritative_in") or []
        matches = [r for r in rows if isinstance(r, dict) and str(r.get("evidence")) == evidence_name]
        matches.sort(key=lambda r: int(r.get("priority", 999)))
        return [str(r.get("tool")) for r in matches if r.get("tool")]

    def hint_tables_for_evidence(self, evidence_name: str, tool_name: str) -> List[str]:
        """Return hint_tables for a given evidence/tool pair, if defined."""
        rows = self.data.get("authoritative_in") or []
        for row in rows:
            if not isinstance(row, dict):
                continue
            if str(row.get("evidence")) == evidence_name and str(row.get("tool")).upper() == tool_name.upper():
                hints = row.get("hint_tables")
                if isinstance(hints, list):
                    return [str(h) for h in hints]
        return []


class IntentGraphProvider:
    def __init__(self):
        self._cached: IntentGraphSnapshot | None = None

    def load(self, force_refresh: bool = False) -> IntentGraphSnapshot:
        if self._cached and not force_refresh:
            return self._cached

        graph = self._load_from_fabric()
        if graph:
            self._cached = IntentGraphSnapshot(data=graph, source="fabric-graph")
            return self._cached

        file_graph = self._load_from_file()
        if file_graph:
            self._cached = IntentGraphSnapshot(data=file_graph, source="json-file")
            return self._cached

        self._cached = IntentGraphSnapshot(data=DEFAULT_INTENT_GRAPH, source="builtin-default")
        return self._cached

    def _load_from_fabric(self) -> Dict[str, Any] | None:
        if not FABRIC_GRAPH_ENDPOINT:
            return None
        payload = {"operation": "intent_graph_snapshot"}
        body = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(FABRIC_GRAPH_ENDPOINT, data=body, method="POST")
        req.add_header("Content-Type", "application/json")
        token = _get_fabric_bearer_token()
        if token:
            req.add_header("Authorization", f"Bearer {token}")

        try:
            with urllib.request.urlopen(req, timeout=20) as resp:
                raw = resp.read().decode("utf-8", errors="ignore")
            data = json.loads(raw) if raw else {}
            # Expected either full object or wrapped snapshot key.
            if isinstance(data, dict) and "intents" in data and "requires" in data:
                return data
            if isinstance(data, dict) and isinstance(data.get("snapshot"), dict):
                snapshot = data.get("snapshot")
                if "intents" in snapshot and "requires" in snapshot:
                    return snapshot
        except urllib.error.HTTPError:
            logger.warning("Intent graph HTTP fetch failed", exc_info=True)
            return None
        except Exception:
            logger.warning("Intent graph fetch failed", exc_info=True)
            return None
        return None

    def _load_from_file(self) -> Dict[str, Any] | None:
        path = INTENT_GRAPH_JSON_PATH
        candidates: List[Path] = []
        if path:
            p = Path(path)
            if not p.is_absolute():
                p = ROOT / p
            candidates.append(p)
        candidates.append(ROOT / "data/intent_graph.json")

        for candidate in candidates:
            if not candidate.exists():
                continue
            try:
                payload = json.loads(candidate.read_text(encoding="utf-8"))
            except Exception:
                continue
            if isinstance(payload, dict) and "intents" in payload and "requires" in payload:
                return payload
        return None
