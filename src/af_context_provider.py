#!/usr/bin/env python3
"""
Agent Framework RAG context provider for Aviation RAG.

This module builds retrieval context and citations from SQL, vector indexes,
KQL/event windows, graph traversal, and optional NoSQL sources.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from agentic_orchestrator import AgenticOrchestrator
from evidence_verifier import EvidenceVerifier
from intent_graph_provider import IntentGraphProvider
from plan_executor import PlanExecutor
from retrieval_plan import RetrievalRequest, RetrievalPlan, SourcePlan, build_retrieval_plan
from schema_provider import SchemaProvider
from unified_retriever import Citation, UnifiedRetriever


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class AviationRagContext:
    """Structured retrieval context produced for a single user query."""

    query: str
    route: str
    context_text: str
    citations: List[Citation] = field(default_factory=list)
    sql_query: Optional[str] = None
    sql_results: List[Dict[str, Any]] = field(default_factory=list)
    semantic_results: List[Dict[str, Any]] = field(default_factory=list)
    source_results: Dict[str, List[Dict[str, Any]]] = field(default_factory=dict)
    retrieval_plan: Dict[str, Any] = field(default_factory=dict)
    source_traces: List[Dict[str, Any]] = field(default_factory=list)
    reasoning: str = ""
    agentic_plan: Dict[str, Any] = field(default_factory=dict)

    def to_event_payload(self) -> Dict[str, Any]:
        return {
            "query": self.query,
            "route": self.route,
            "reasoning": self.reasoning,
            "sql_query": self.sql_query,
            "sql_result_count": len(self.sql_results),
            "semantic_result_count": len(self.semantic_results),
            "citation_count": len(self.citations),
            "source_result_counts": {k: len(v) for k, v in self.source_results.items()},
            "retrieval_plan": self.retrieval_plan,
        }


class AviationRagContextProvider:
    """
    RAG context provider built on top of the unified retriever.

    This provider is framework-agnostic and can be plugged into Agent Framework
    pipelines where a context-provider object is expected.
    """

    def __init__(self, retriever: UnifiedRetriever, semantic_top_k: int = 5):
        self.retriever = retriever
        self.semantic_top_k = semantic_top_k
        self.intent_graph_provider = IntentGraphProvider()
        self.schema_provider = SchemaProvider(retriever)
        self.evidence_verifier = EvidenceVerifier()
        self.plan_executor = PlanExecutor(retriever)
        self._agentic_enabled = True
        try:
            self.orchestrator = AgenticOrchestrator()
        except Exception:
            self.orchestrator = None
            self._agentic_enabled = False

    def build_context(
        self,
        query: str,
        retrieval_mode: str = "code-rag",
        forced_route: Optional[str] = None,
        query_profile: str = "pilot-brief",
        required_sources: Optional[List[str]] = None,
        freshness_sla_minutes: Optional[int] = None,
        explain_retrieval: bool = False,
        risk_mode: str = "standard",
        ask_recommendation: bool = False,
    ) -> AviationRagContext:
        if retrieval_mode == "code-rag" and self._agentic_enabled and self.orchestrator is not None:
            try:
                return self._build_agentic_context(
                    query=query,
                    query_profile=query_profile,
                    required_sources=required_sources or [],
                    freshness_sla_minutes=freshness_sla_minutes,
                    explain_retrieval=explain_retrieval,
                    risk_mode=risk_mode,
                    ask_recommendation=ask_recommendation,
                )
            except Exception as exc:
                fallback_note = f"Agentic orchestration failed, using legacy planner ({exc})"
        else:
            fallback_note = "Legacy retrieval planner used"

        return self._build_legacy_context(
            query=query,
            retrieval_mode=retrieval_mode,
            forced_route=forced_route,
            query_profile=query_profile,
            required_sources=required_sources,
            freshness_sla_minutes=freshness_sla_minutes,
            explain_retrieval=explain_retrieval,
            additional_reasoning=fallback_note,
        )

    def _build_legacy_context(
        self,
        query: str,
        retrieval_mode: str,
        forced_route: Optional[str],
        query_profile: str,
        required_sources: Optional[List[str]],
        freshness_sla_minutes: Optional[int],
        explain_retrieval: bool,
        additional_reasoning: str,
    ) -> AviationRagContext:
        route, reasoning, sql_hint = self._resolve_route(query, retrieval_mode, forced_route)

        plan_request = RetrievalRequest(
            query=query,
            retrieval_mode=retrieval_mode,
            query_profile=query_profile,
            required_sources=list(required_sources or []),
            freshness_sla_minutes=freshness_sla_minutes,
            explain_retrieval=explain_retrieval,
            forced_route=forced_route,
        )
        retrieval_plan = build_retrieval_plan(plan_request, route, reasoning)

        source_results, source_traces, all_citations, sql_query = self._execute_plan(
            query=query,
            plan=retrieval_plan,
            sql_hint=sql_hint,
        )

        sql_results = source_results.get("SQL", [])
        semantic_results: List[Dict[str, Any]] = []
        for source in ("VECTOR_OPS", "VECTOR_REG", "VECTOR_AIRPORT"):
            semantic_results.extend(source_results.get(source, []))

        context_text = self._compose_context_text(
            query=query,
            route=route,
            retrieval_plan=retrieval_plan,
            sql_query=sql_query,
            source_results=source_results,
        )

        return AviationRagContext(
            query=query,
            route=route,
            context_text=context_text,
            citations=all_citations[:20],
            sql_query=sql_query,
            sql_results=sql_results,
            semantic_results=semantic_results[:20],
            source_results=source_results,
            retrieval_plan=retrieval_plan.to_event_payload(),
            source_traces=source_traces,
            reasoning=f"{retrieval_plan.reasoning}; {additional_reasoning}",
        )

    def _build_agentic_context(
        self,
        query: str,
        query_profile: str,
        required_sources: List[str],
        freshness_sla_minutes: Optional[int],
        explain_retrieval: bool,
        risk_mode: str,
        ask_recommendation: bool,
    ) -> AviationRagContext:
        intent_graph = self.intent_graph_provider.load()
        schemas = self.schema_provider.snapshot()
        runtime_context = {
            "now_utc": _utc_now(),
            "default_time_horizon_min": int(freshness_sla_minutes or 120),
            "risk_mode": risk_mode,
            "ask_recommendation": ask_recommendation,
        }
        tool_catalog = {
            "allowed_tools": ["GRAPH", "KQL", "SQL", "VECTOR_OPS", "VECTOR_REG", "VECTOR_AIRPORT", "NOSQL"],
            "fallback_for": {
                "NOTAM": ["VECTOR_REG", "NOSQL"],
                "SOPClause": ["VECTOR_REG"],
                "Hazards": ["KQL"],
            },
        }
        entities = {"airports": [], "flight_ids": [], "routes": [], "stations": [], "alternates": []}
        plan = self.orchestrator.create_plan(
            user_query=query,
            runtime_context=runtime_context,
            entities=entities,
            intent_graph=intent_graph,
            tool_catalog=tool_catalog,
            schemas=schemas,
            required_sources=required_sources,
        )
        execution = self.plan_executor.execute(query, plan, schemas)
        verification = self.evidence_verifier.verify(
            plan=plan,
            source_results=execution.source_results,
            evidence_tool_map=execution.evidence_tool_map,
            ask_recommendation=ask_recommendation,
        )

        warnings = [*plan.warnings, *execution.warnings, *verification.warnings]
        route = "AGENTIC"
        reasoning = f"intent={plan.intent.name}; graph_source={intent_graph.source}; risk_mode={risk_mode}"
        if warnings and explain_retrieval:
            reasoning = f"{reasoning}; warnings={'; '.join(warnings[:3])}"

        semantic_results: List[Dict[str, Any]] = []
        for src in ("VECTOR_OPS", "VECTOR_REG", "VECTOR_AIRPORT"):
            semantic_results.extend(execution.source_results.get(src, []))

        sql_results = execution.source_results.get("SQL", [])
        sql_query = next(iter(execution.sql_queries.values()), None)
        retrieval_plan = {
            "route": route,
            "reasoning": reasoning,
            "profile": query_profile,
            "graph_source": intent_graph.source,
            "steps": [
                {
                    "source": call.tool,
                    "reason": call.operation,
                    "priority": idx + 1,
                    "params": call.params,
                }
                for idx, call in enumerate(plan.tool_calls)
            ],
            "coverage": verification.coverage,
            "warnings": warnings,
            "is_verified": verification.is_verified,
        }
        context_text = self._compose_agentic_context_text(
            query=query,
            plan=plan.to_dict(),
            source_results=execution.source_results,
            sql_queries=execution.sql_queries,
            warnings=warnings,
        )
        return AviationRagContext(
            query=query,
            route=route,
            context_text=context_text,
            citations=execution.citations[:20],
            sql_query=sql_query,
            sql_results=sql_results,
            semantic_results=semantic_results[:20],
            source_results=execution.source_results,
            retrieval_plan=retrieval_plan,
            source_traces=execution.source_traces,
            reasoning=reasoning,
            agentic_plan=plan.to_dict(),
        )

    def _execute_plan(
        self,
        query: str,
        plan: RetrievalPlan,
        sql_hint: Optional[str],
    ) -> tuple[Dict[str, List[Dict[str, Any]]], List[Dict[str, Any]], List[Citation], Optional[str]]:
        steps = sorted(plan.steps, key=lambda s: s.priority)
        source_results: Dict[str, List[Dict[str, Any]]] = {}
        source_traces: List[Dict[str, Any]] = []
        citations: List[Citation] = []
        sql_query: Optional[str] = None

        def _run(step: SourcePlan) -> tuple[str, List[Dict[str, Any]], List[Citation], Optional[str]]:
            params = dict(step.params)
            if step.source == "SQL" and sql_hint and "sql_hint" not in params:
                params["sql_hint"] = sql_hint
            if step.source.startswith("VECTOR_") and "top" not in params:
                params["top"] = self.semantic_top_k
            rows, row_citations, out_sql = self.retriever.retrieve_source(step.source, query, params)
            return step.source, rows, row_citations, out_sql

        with ThreadPoolExecutor(max_workers=max(1, min(6, len(steps)))) as executor:
            future_map = {}
            for step in steps:
                source_traces.append(
                    {
                        "type": "source_call_start",
                        "source": step.source,
                        "reason": step.reason,
                        "priority": step.priority,
                        "timestamp": _utc_now(),
                    }
                )
                future = executor.submit(_run, step)
                future_map[future] = step

            for future in as_completed(future_map):
                step = future_map[future]
                try:
                    source, rows, row_citations, out_sql = future.result()
                    source_results[source] = rows
                    citations.extend(row_citations)
                    if source == "SQL" and out_sql:
                        sql_query = out_sql
                    source_traces.append(
                        {
                            "type": "source_call_done",
                            "source": source,
                            "row_count": len(rows),
                            "citation_count": len(row_citations),
                            "timestamp": _utc_now(),
                        }
                    )
                except Exception as exc:
                    source_results[step.source] = [{"error": str(exc)}]
                    source_traces.append(
                        {
                            "type": "source_call_done",
                            "source": step.source,
                            "row_count": 1,
                            "citation_count": 0,
                            "error": str(exc),
                            "timestamp": _utc_now(),
                        }
                    )

        return source_results, source_traces, citations, sql_query

    def _resolve_route(
        self, query: str, retrieval_mode: str, forced_route: Optional[str]
    ) -> tuple[str, str, Optional[str]]:
        if forced_route in ("SQL", "SEMANTIC", "HYBRID"):
            return forced_route, "Route forced by caller", None

        # Preserve existing product behavior: foundry-iq favors semantic retrieval.
        if retrieval_mode == "foundry-iq":
            return "SEMANTIC", "Foundry IQ mode prefers semantic context", None

        route_result = self.retriever.router.route(query)
        route = route_result.get("route", "HYBRID")
        reasoning = route_result.get("reasoning", "Route inferred by QueryRouter")
        sql_hint = route_result.get("sql_hint")
        return route, reasoning, sql_hint

    def _compose_context_text(
        self,
        query: str,
        route: str,
        retrieval_plan: RetrievalPlan,
        sql_query: Optional[str],
        source_results: Dict[str, List[Dict[str, Any]]],
    ) -> str:
        sections: List[str] = [f"User query: {query}", f"Selected route: {route}"]
        sections.append(f"Retrieval profile: {retrieval_plan.profile}")
        sections.append(
            "Planned sources: "
            + ", ".join(f"{s.source}(p{s.priority})" for s in sorted(retrieval_plan.steps, key=lambda x: x.priority))
        )

        if sql_query:
            sections.append(f"SQL query:\n{sql_query}")

        for source in ("KQL", "GRAPH", "NOSQL", "SQL", "VECTOR_REG", "VECTOR_OPS", "VECTOR_AIRPORT"):
            rows = source_results.get(source)
            if not rows:
                continue
            sections.append(f"{source} results:\n{self._format_rows(rows, source)}")

        return "\n\n".join(sections)

    def _compose_agentic_context_text(
        self,
        query: str,
        plan: Dict[str, Any],
        source_results: Dict[str, List[Dict[str, Any]]],
        sql_queries: Dict[str, str],
        warnings: List[str],
    ) -> str:
        sections: List[str] = [
            f"User query: {query}",
            f"Selected route: AGENTIC ({plan.get('intent', {}).get('name', 'unknown-intent')})",
            f"Required evidence: {', '.join(e.get('name', '') for e in plan.get('required_evidence', []))}",
        ]
        if sql_queries:
            sections.append("Generated SQL:\n" + "\n\n".join(sql_queries.values()))
        for source in ("GRAPH", "KQL", "NOSQL", "SQL", "VECTOR_REG", "VECTOR_OPS", "VECTOR_AIRPORT"):
            rows = source_results.get(source)
            if not rows:
                continue
            sections.append(f"{source} results:\n{self._format_rows(rows, source)}")
        if warnings:
            sections.append("Warnings:\n" + "\n".join(f"- {w}" for w in warnings))
        return "\n\n".join(sections)

    def _format_rows(self, rows: List[Dict[str, Any]], source: str) -> str:
        lines: List[str] = []
        for idx, row in enumerate(rows[:8], start=1):
            if source.startswith("VECTOR_"):
                title = row.get("title") or row.get("id") or f"Document {idx}"
                doc_id = row.get("asrs_report_id") or row.get("id") or ""
                snippet = str(row.get("content", ""))[:220].replace("\n", " ")
                lines.append(f"{idx}. {title} ({doc_id})\n   {snippet}")
                continue
            compact = ", ".join(f"{k}={v}" for k, v in list(row.items())[:8])
            lines.append(f"{idx}. {compact}")
        return "\n".join(lines) if lines else "No rows returned."
