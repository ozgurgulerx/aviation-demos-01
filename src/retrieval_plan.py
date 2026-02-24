#!/usr/bin/env python3
"""
Retrieval planning primitives for multi-source agentic RAG.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from shared_utils import (
    canon_tool,
    normalize_source_policy,
    validate_source_policy_request,
    matches_any,
    OPS_TABLE_SIGNALS,
    FABRIC_SQL_DELAY_TRIGGERS,
)


VALID_SOURCES = {
    "SQL",
    "KQL",
    "GRAPH",
    "VECTOR_OPS",
    "VECTOR_REG",
    "VECTOR_AIRPORT",
    "NOSQL",
    "FABRIC_SQL",
}

def _norm_source(value: str) -> Optional[str]:
    src = canon_tool(value)
    return src if src in VALID_SOURCES else None


def _norm_source_policy(value: str) -> str:
    return normalize_source_policy(value)


class ExactPolicyValidationError(ValueError):
    def __init__(
        self,
        message: str,
        *,
        required_sources_raw: Optional[List[str]] = None,
        required_sources_normalized: Optional[List[str]] = None,
        invalid_required_sources: Optional[List[str]] = None,
    ) -> None:
        super().__init__(message)
        self.required_sources_raw = list(required_sources_raw or [])
        self.required_sources_normalized = list(required_sources_normalized or [])
        self.invalid_required_sources = list(invalid_required_sources or [])

    def to_dict(self) -> Dict[str, Any]:
        return {
            "error_code": "exact_required_sources_invalid",
            "message": str(self),
            "required_sources_raw": self.required_sources_raw,
            "required_sources_normalized": self.required_sources_normalized,
            "invalid_required_sources": self.invalid_required_sources,
            "source_policy": "exact",
        }


@dataclass
class RetrievalRequest:
    query: str
    retrieval_mode: str = "code-rag"
    query_profile: str = "pilot-brief"
    required_sources: List[str] = field(default_factory=list)
    source_policy: str = "include"
    freshness_sla_minutes: Optional[int] = None
    explain_retrieval: bool = False
    forced_route: Optional[str] = None


@dataclass
class SourcePlan:
    source: str
    reason: str
    priority: int
    params: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "source": self.source,
            "reason": self.reason,
            "priority": self.priority,
            "params": self.params,
        }


@dataclass
class RetrievalPlan:
    route: str
    reasoning: str
    profile: str
    steps: List[SourcePlan] = field(default_factory=list)

    def to_event_payload(self) -> Dict[str, Any]:
        return {
            "route": self.route,
            "reasoning": self.reasoning,
            "profile": self.profile,
            "steps": [s.to_dict() for s in sorted(self.steps, key=lambda x: x.priority)],
        }


_REALTIME_MARKERS = frozenset({
    "last", "live", "real-time", "realtime", "minutes",
    "now", "current status", "recent",
})

_GRAPH_MARKERS = frozenset({
    "impact", "dependency", "depends on", "connected",
    "alternate", "route network", "relationship",
})

_REGULATORY_MARKERS = frozenset({
    "ad", "airworthiness", "notam", "easa", "compliance", "directive",
})

_NARRATIVE_MARKERS = frozenset({
    "summarize", "similar", "narrative", "what happened", "examples", "lessons",
})

_AIRPORT_OPS_MARKERS = frozenset({
    "runway", "gate", "turnaround", "airport", "station", "ltfm", "ltfj", "ltba",
})

_NOSQL_MARKERS = frozenset({
    "notam", "operational doc", "ops doc", "ground handling doc", "parking stand",
})


def _wants_realtime(query_l: str) -> bool:
    return matches_any(query_l, _REALTIME_MARKERS)


def _wants_graph(query_l: str) -> bool:
    return matches_any(query_l, _GRAPH_MARKERS)


def _wants_regulatory(query_l: str) -> bool:
    return matches_any(query_l, _REGULATORY_MARKERS)


def _wants_narrative(query_l: str) -> bool:
    return matches_any(query_l, _NARRATIVE_MARKERS)


def _wants_airport_ops(query_l: str) -> bool:
    return matches_any(query_l, _AIRPORT_OPS_MARKERS)


def _wants_nosql(query_l: str) -> bool:
    return matches_any(query_l, _NOSQL_MARKERS)


def _wants_analytics(query_l: str) -> bool:
    return (
        matches_any(query_l, FABRIC_SQL_DELAY_TRIGGERS)
        and not matches_any(query_l, OPS_TABLE_SIGNALS)
    )


def build_retrieval_plan(
    request: RetrievalRequest,
    route: str,
    route_reasoning: str,
    router_sources: Optional[List[str]] = None,
) -> RetrievalPlan:
    query_l = request.query.lower()
    profile = (request.query_profile or "pilot-brief").strip().lower()
    source_policy = _norm_source_policy(request.source_policy)

    steps: List[SourcePlan] = []

    def add(source: str, reason: str, priority: int, params: Optional[Dict[str, Any]] = None) -> None:
        if any(s.source == source for s in steps):
            return
        steps.append(
            SourcePlan(
                source=source,
                reason=reason,
                priority=priority,
                params=params or {},
            )
        )

    # Exact source policy runs only requested sources, in request order.
    if source_policy == "exact":
        validation = validate_source_policy_request(request.required_sources, source_policy)
        if not validation["is_valid"]:
            raise ExactPolicyValidationError(
                validation["error_message"] or "Invalid exact source policy request.",
                required_sources_raw=validation["required_sources_raw"],
                required_sources_normalized=validation["required_sources_normalized"],
                invalid_required_sources=validation["invalid_required_sources"],
            )
        for idx, src in enumerate(validation["required_sources_normalized"]):
            add(src, "Required by request (exact source policy)", 1 + idx)
        reasoning = route_reasoning
        if request.explain_retrieval:
            reasoning = (
                f"{route_reasoning}; profile={profile}; source_policy=exact;"
                f" sources={','.join(s.source for s in sorted(steps, key=lambda x: x.priority))}"
            )
        return RetrievalPlan(route=route, reasoning=reasoning, profile=profile, steps=steps)

    # When the router provides an explicit source list, use it as primary.
    if router_sources:
        for idx, src in enumerate(router_sources):
            normed = _norm_source(src)
            if normed:
                add(normed, "Router-selected source", 10 + idx)
    else:
        # Baseline by route.
        if route in ("SQL", "HYBRID"):
            add("SQL", "Structured metrics and deterministic filters", 10)
        if route in ("SEMANTIC", "HYBRID"):
            add("VECTOR_OPS", "Narrative and semantic context", 20)

        # Profile-driven enrichments.
        if profile in ("pilot-brief", "ops-live", "operations"):
            add("SQL", "Operational KPIs from relational tables", 10)
            add("VECTOR_OPS", "Relevant operational narratives", 20)

        if profile in ("compliance", "regulatory"):
            add("VECTOR_REG", "Regulatory corpus retrieval", 15)
            add("SQL", "Fleet applicability checks", 25)

        # Query-driven source activation.
        if _wants_realtime(query_l) or (request.freshness_sla_minutes is not None and request.freshness_sla_minutes <= 60):
            add("KQL", "Live operational and weather windows", 5, {"window_minutes": request.freshness_sla_minutes or 60})

        if _wants_graph(query_l):
            add("GRAPH", "Dependency and impact traversal", 8, {"hops": 2})

        if _wants_regulatory(query_l):
            add("VECTOR_REG", "NOTAM/AD and compliance lookup", 12)

        if _wants_narrative(query_l):
            add("VECTOR_OPS", "Narrative similarity retrieval", 18)

        if _wants_airport_ops(query_l):
            add("VECTOR_AIRPORT", "Airport/runway/station document lookup", 22)

        if _wants_nosql(query_l):
            add("NOSQL", "Operational document / NOTAM lookup", 24)

        if _wants_analytics(query_l):
            add("FABRIC_SQL", "BTS on-time analytics and delay causes", 15)

        if request.retrieval_mode == "foundry-iq":
            add("VECTOR_OPS", "Foundry IQ semantic-first context", 15)

    # Required sources from caller.
    for raw in request.required_sources:
        src = _norm_source(raw)
        if src:
            add(src, "Required by request", 1)

    # Fallback guarantees.
    if not steps:
        add("SQL", "Default fallback source", 10)
        add("VECTOR_OPS", "Default semantic fallback", 20)

    reasoning = route_reasoning
    if request.explain_retrieval:
        reasoning = (
            f"{route_reasoning}; profile={profile}; source_policy={source_policy};"
            f" sources={','.join(s.source for s in sorted(steps, key=lambda x: x.priority))}"
        )

    return RetrievalPlan(route=route, reasoning=reasoning, profile=profile, steps=steps)
