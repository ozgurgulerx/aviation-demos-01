#!/usr/bin/env python3
"""
Evidence verification utilities for safety-oriented agentic RAG.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from contracts.agentic_plan import AgenticPlan

if TYPE_CHECKING:
    from intent_graph_provider import IntentGraphSnapshot


@dataclass
class EvidenceVerificationResult:
    coverage: List[Dict[str, Any]] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    is_verified: bool = False
    requery_suggestions: List[Dict[str, Any]] = field(default_factory=list)


class EvidenceVerifier:
    def verify(
        self,
        plan: AgenticPlan,
        source_results: Dict[str, List[Dict[str, Any]]],
        evidence_tool_map: Dict[str, List[str]],
        ask_recommendation: bool = False,
        intent_graph: Optional["IntentGraphSnapshot"] = None,
    ) -> EvidenceVerificationResult:
        coverage: List[Dict[str, Any]] = []
        warnings: List[str] = []
        requery_suggestions: List[Dict[str, Any]] = []
        required_ok = True

        tried_tools: set = set()
        for tools in evidence_tool_map.values():
            tried_tools.update(tools)

        for req in plan.required_evidence:
            tools = evidence_tool_map.get(req.name, [])
            evidence_has_rows = False
            for tool in tools:
                rows = source_results.get(tool, [])
                if rows and not all("error" in row for row in rows if isinstance(row, dict)):
                    evidence_has_rows = True
                    break

            status = "planned" if evidence_has_rows else "missing"
            coverage.append({"evidence": req.name, "status": status, "via_tools": tools})

            if not evidence_has_rows and not req.optional:
                required_ok = False
                warnings.append(f"Missing required evidence: {req.name}")
                # Suggest fallback tools from intent graph that weren't already tried.
                if intent_graph is not None:
                    fallback_tools = intent_graph.tools_for_evidence(req.name)
                    for ft in fallback_tools:
                        if ft not in tried_tools:
                            requery_suggestions.append({
                                "evidence": req.name,
                                "tool": ft,
                                "reason": f"fallback for missing {req.name}",
                            })

        if ask_recommendation:
            sop_cov = next((c for c in coverage if c.get("evidence") == "SOPClause"), None)
            if not sop_cov or sop_cov.get("status") != "planned":
                required_ok = False
                warnings.append("Recommendation requested but SOPClause evidence is unavailable.")

        if not coverage:
            required_ok = False
            warnings.append("No evidence coverage available from plan.")

        return EvidenceVerificationResult(
            coverage=coverage,
            warnings=warnings,
            is_verified=required_ok,
            requery_suggestions=requery_suggestions,
        )
