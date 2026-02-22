#!/usr/bin/env python3
"""
Retrieval planning primitives for multi-source agentic RAG.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


VALID_SOURCES = {
    "SQL",
    "KQL",
    "GRAPH",
    "VECTOR_OPS",
    "VECTOR_REG",
    "VECTOR_AIRPORT",
    "NOSQL",
}


def _norm_source(value: str) -> Optional[str]:
    src = value.strip().upper()
    if src in VALID_SOURCES:
        return src
    return None


@dataclass
class RetrievalRequest:
    query: str
    retrieval_mode: str = "code-rag"
    query_profile: str = "pilot-brief"
    required_sources: List[str] = field(default_factory=list)
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


def _wants_realtime(query_l: str) -> bool:
    markers = (
        "last ",
        "live",
        "real-time",
        "realtime",
        "minutes",
        "now",
        "current status",
        "recent",
    )
    return any(m in query_l for m in markers)


def _wants_graph(query_l: str) -> bool:
    markers = (
        "impact",
        "dependency",
        "depends on",
        "connected",
        "alternate",
        "route network",
        "relationship",
    )
    return any(m in query_l for m in markers)


def _wants_regulatory(query_l: str) -> bool:
    markers = ("ad ", "airworthiness", "notam", "easa", "compliance", "directive")
    return any(m in query_l for m in markers)


def _wants_narrative(query_l: str) -> bool:
    markers = ("summarize", "similar", "narrative", "what happened", "examples", "lessons")
    return any(m in query_l for m in markers)


def _wants_airport_ops(query_l: str) -> bool:
    markers = ("runway", "gate", "turnaround", "airport", "station", "ltfm", "ltfj", "ltba")
    return any(m in query_l for m in markers)


def _wants_nosql(query_l: str) -> bool:
    markers = ("notam", "operational doc", "ops doc", "ground handling doc", "parking stand")
    return any(m in query_l for m in markers)


def build_retrieval_plan(
    request: RetrievalRequest,
    route: str,
    route_reasoning: str,
    router_sources: Optional[List[str]] = None,
) -> RetrievalPlan:
    query_l = request.query.lower()
    profile = (request.query_profile or "pilot-brief").strip().lower()

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
        reasoning = f"{route_reasoning}; profile={profile}; sources={','.join(s.source for s in sorted(steps, key=lambda x: x.priority))}"

    return RetrievalPlan(route=route, reasoning=reasoning, profile=profile, steps=steps)
