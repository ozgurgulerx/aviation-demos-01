#!/usr/bin/env python3
"""
Evidence verification utilities for safety-oriented agentic RAG.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List

from contracts.agentic_plan import AgenticPlan


@dataclass
class EvidenceVerificationResult:
    coverage: List[Dict[str, Any]] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    is_verified: bool = False


class EvidenceVerifier:
    def verify(
        self,
        plan: AgenticPlan,
        source_results: Dict[str, List[Dict[str, Any]]],
        evidence_tool_map: Dict[str, List[str]],
        ask_recommendation: bool = False,
    ) -> EvidenceVerificationResult:
        coverage: List[Dict[str, Any]] = []
        warnings: List[str] = []
        required_ok = True

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
        )

