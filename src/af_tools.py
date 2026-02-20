#!/usr/bin/env python3
"""
Agent Framework tool wrappers for Aviation RAG retrieval primitives.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from af_context_provider import AviationRagContextProvider
from unified_retriever import Citation, UnifiedRetriever


def _citations_to_payload(citations: List[Citation]) -> List[Dict[str, Any]]:
    return [
        {
            "source_type": c.source_type,
            "identifier": c.identifier,
            "title": c.title,
            "content_preview": c.content_preview,
            "score": c.score,
        }
        for c in citations
    ]


class AviationRagTools:
    """Function-style tools that can be exposed to an Agent Framework agent."""

    def __init__(self, retriever: UnifiedRetriever, context_provider: AviationRagContextProvider):
        self.retriever = retriever
        self.context_provider = context_provider

    def run_sql_lookup(self, query: str, sql_hint: str = "") -> Dict[str, Any]:
        results, sql, citations = self.retriever.query_sql(query, sql_hint or None)
        return {
            "route": "SQL",
            "sql_query": sql,
            "results": results,
            "citations": _citations_to_payload(citations),
        }

    def run_semantic_lookup(
        self, query: str, top_k: int = 5, source: str = "VECTOR_OPS"
    ) -> Dict[str, Any]:
        results, citations = self.retriever.query_semantic(query, top=top_k, source=source)
        return {
            "route": "SEMANTIC",
            "source": source,
            "results": results,
            "citations": _citations_to_payload(citations),
        }

    def run_rag_lookup(
        self,
        query: str,
        retrieval_mode: str = "code-rag",
        query_profile: str = "pilot-brief",
        required_sources: Optional[List[str]] = None,
        freshness_sla_minutes: Optional[int] = None,
        explain_retrieval: bool = False,
        risk_mode: str = "standard",
        ask_recommendation: bool = False,
    ) -> Dict[str, Any]:
        ctx = self.context_provider.build_context(
            query,
            retrieval_mode=retrieval_mode,
            query_profile=query_profile,
            required_sources=required_sources or [],
            freshness_sla_minutes=freshness_sla_minutes,
            explain_retrieval=explain_retrieval,
            risk_mode=risk_mode,
            ask_recommendation=ask_recommendation,
        )
        return {
            "route": ctx.route,
            "reasoning": ctx.reasoning,
            "context_text": ctx.context_text,
            "sql_query": ctx.sql_query,
            "sql_results": ctx.sql_results,
            "semantic_results": ctx.semantic_results,
            "source_results": ctx.source_results,
            "reconciled_items": ctx.reconciled_items,
            "coverage_summary": ctx.coverage_summary,
            "conflict_summary": ctx.conflict_summary,
            "retrieval_plan": ctx.retrieval_plan,
            "source_traces": ctx.source_traces,
            "citations": _citations_to_payload(ctx.citations),
        }


def build_agent_framework_tools(toolset: AviationRagTools) -> List[Any]:
    """
    Build AF tool instances when agent-framework is installed.

    The API changed recently (AIFunction -> FunctionTool). This helper supports the
    latest naming and fails gracefully if AF packages are unavailable.
    """

    try:
        from agent_framework.core import FunctionTool  # type: ignore
    except Exception:
        return []

    return [
        FunctionTool(toolset.run_rag_lookup),
        FunctionTool(toolset.run_sql_lookup),
        FunctionTool(toolset.run_semantic_lookup),
    ]
