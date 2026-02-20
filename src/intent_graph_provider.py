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
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List

logger = logging.getLogger(__name__)

import shared_utils  # noqa: F401  — ensures load_dotenv() runs

FABRIC_GRAPH_ENDPOINT = os.getenv("FABRIC_GRAPH_ENDPOINT", "").strip()
def _get_fabric_bearer_token() -> str:
    """Re-read bearer token from env on each call so rotated tokens take effect."""
    return os.getenv("FABRIC_BEARER_TOKEN", "").strip()
INTENT_GRAPH_JSON_PATH = os.getenv("INTENT_GRAPH_JSON_PATH", "").strip()

ROOT = Path(__file__).resolve().parents[1]

DEFAULT_INTENT_GRAPH: Dict[str, Any] = {
    "intents": [
        {"name": "PilotBrief.Departure"},
        {"name": "PilotBrief.Arrival"},
        {"name": "Disruption.Explain"},
        {"name": "Policy.Check"},
        {"name": "Replay.History"},
    ],
    "evidence": [
        {"name": "METAR", "requires_citations": False},
        {"name": "TAF", "requires_citations": False},
        {"name": "NOTAM", "requires_citations": False},
        {"name": "RunwayConstraints", "requires_citations": False},
        {"name": "Hazards", "requires_citations": False},
        {"name": "SOPClause", "requires_citations": True},
    ],
    "tools": [
        {"name": "GRAPH", "kind": "graph"},
        {"name": "KQL", "kind": "kql"},
        {"name": "SQL", "kind": "sql"},
        {"name": "NOSQL", "kind": "nosql"},
        {"name": "VECTOR_REG", "kind": "search"},
        {"name": "VECTOR_OPS", "kind": "search"},
        {"name": "VECTOR_AIRPORT", "kind": "search"},
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
        {"intent": "Replay.History", "evidence": "METAR", "optional": False},
        {"intent": "Replay.History", "evidence": "Hazards", "optional": False},
        {"intent": "Replay.History", "evidence": "NOTAM", "optional": True},
    ],
    "authoritative_in": [
        {"evidence": "METAR", "tool": "KQL", "priority": 1},
        {"evidence": "TAF", "tool": "KQL", "priority": 1},
        {"evidence": "NOTAM", "tool": "NOSQL", "priority": 1},
        {"evidence": "NOTAM", "tool": "VECTOR_REG", "priority": 2},
        {"evidence": "RunwayConstraints", "tool": "SQL", "priority": 1},
        {"evidence": "Hazards", "tool": "KQL", "priority": 1},
        {"evidence": "SOPClause", "tool": "VECTOR_REG", "priority": 1},
    ],
    "expansion_rules": [
        {"intent": "PilotBrief.Departure", "tool": "GRAPH", "reason": "airport->stations/alternates expansion"},
        {"intent": "PilotBrief.Arrival", "tool": "GRAPH", "reason": "airport->stations/alternates expansion"},
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

    def tools_for_evidence(self, evidence_name: str) -> List[str]:
        rows = self.data.get("authoritative_in") or []
        matches = [r for r in rows if isinstance(r, dict) and str(r.get("evidence")) == evidence_name]
        matches.sort(key=lambda r: int(r.get("priority", 999)))
        return [str(r.get("tool")) for r in matches if r.get("tool")]


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
