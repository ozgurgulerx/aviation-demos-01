#!/usr/bin/env python3
"""
Contracts for agentic routing/execution plans.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class TimeWindow:
    horizon_min: int = 120
    start_utc: Optional[str] = None
    end_utc: Optional[str] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TimeWindow":
        return cls(
            horizon_min=int(data.get("horizon_min", 120) or 120),
            start_utc=data.get("start_utc"),
            end_utc=data.get("end_utc"),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "horizon_min": self.horizon_min,
            "start_utc": self.start_utc,
            "end_utc": self.end_utc,
        }


@dataclass
class Intent:
    name: str = "PilotBrief.Departure"
    confidence: float = 0.5

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Intent":
        return cls(
            name=str(data.get("name", "PilotBrief.Departure")),
            confidence=float(data.get("confidence", 0.5) or 0.5),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {"name": self.name, "confidence": self.confidence}


@dataclass
class EvidenceRequirement:
    name: str
    optional: bool = False
    requires_citations: bool = False

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "EvidenceRequirement":
        return cls(
            name=str(data.get("name", "")).strip(),
            optional=bool(data.get("optional", False)),
            requires_citations=bool(data.get("requires_citations", False)),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "optional": self.optional,
            "requires_citations": self.requires_citations,
        }


@dataclass
class ToolCall:
    id: str
    tool: str
    operation: str
    depends_on: List[str] = field(default_factory=list)
    query: Optional[str] = None
    params: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: Dict[str, Any], idx: int) -> "ToolCall":
        return cls(
            id=str(data.get("id") or f"call_{idx}"),
            tool=str(data.get("tool", "")).strip(),
            operation=str(data.get("operation", "lookup")).strip(),
            depends_on=[str(v) for v in (data.get("depends_on") or [])],
            query=data.get("query"),
            params=dict(data.get("params") or {}),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "tool": self.tool,
            "operation": self.operation,
            "depends_on": self.depends_on,
            "query": self.query,
            "params": self.params,
        }


@dataclass
class CoverageItem:
    evidence: str
    status: str = "planned"  # planned | missing
    via_tools: List[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "CoverageItem":
        return cls(
            evidence=str(data.get("evidence", "")).strip(),
            status=str(data.get("status", "planned")),
            via_tools=[str(v) for v in (data.get("via_tools") or [])],
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "evidence": self.evidence,
            "status": self.status,
            "via_tools": self.via_tools,
        }


@dataclass
class AgenticPlan:
    intent: Intent = field(default_factory=Intent)
    time_window: TimeWindow = field(default_factory=TimeWindow)
    entities: Dict[str, List[str]] = field(
        default_factory=lambda: {
            "airports": [],
            "flight_ids": [],
            "routes": [],
            "stations": [],
            "alternates": [],
        }
    )
    required_evidence: List[EvidenceRequirement] = field(default_factory=list)
    tool_calls: List[ToolCall] = field(default_factory=list)
    coverage: List[CoverageItem] = field(default_factory=list)
    needs_schema: bool = False
    schema_requests: List[Dict[str, str]] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "AgenticPlan":
        return cls(
            intent=Intent.from_dict(dict(data.get("intent") or {})),
            time_window=TimeWindow.from_dict(dict(data.get("time_window") or {})),
            entities=dict(data.get("entities") or {}),
            required_evidence=[
                EvidenceRequirement.from_dict(dict(item))
                for item in (data.get("required_evidence") or [])
                if isinstance(item, dict)
            ],
            tool_calls=[
                ToolCall.from_dict(dict(item), idx)
                for idx, item in enumerate((data.get("tool_calls") or []), start=1)
                if isinstance(item, dict)
            ],
            coverage=[
                CoverageItem.from_dict(dict(item))
                for item in (data.get("coverage") or [])
                if isinstance(item, dict)
            ],
            needs_schema=bool(data.get("needs_schema", False)),
            schema_requests=[
                {"type": str(item.get("type", "")), "request": str(item.get("request", ""))}
                for item in (data.get("schema_requests") or [])
                if isinstance(item, dict)
            ],
            warnings=[str(item) for item in (data.get("warnings") or [])],
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "intent": self.intent.to_dict(),
            "time_window": self.time_window.to_dict(),
            "entities": self.entities,
            "required_evidence": [e.to_dict() for e in self.required_evidence],
            "tool_calls": [c.to_dict() for c in self.tool_calls],
            "coverage": [c.to_dict() for c in self.coverage],
            "needs_schema": self.needs_schema,
            "schema_requests": self.schema_requests,
            "warnings": self.warnings,
        }

