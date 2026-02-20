#!/usr/bin/env python3
"""
LLM orchestrator for code-rag agentic planning.
"""

from __future__ import annotations

import json
import os
from typing import Any, Dict, List

from azure.identity import DefaultAzureCredential, get_bearer_token_provider
from dotenv import load_dotenv
from openai import AzureOpenAI

from contracts.agentic_plan import AgenticPlan, CoverageItem, EvidenceRequirement, Intent, TimeWindow, ToolCall
from intent_graph_provider import IntentGraphSnapshot

load_dotenv()


def _client_tuning_kwargs() -> dict:
    try:
        timeout_seconds = float(os.getenv("AZURE_OPENAI_TIMEOUT_SECONDS", "45"))
    except Exception:
        timeout_seconds = 45.0
    try:
        max_retries = max(0, int(os.getenv("AZURE_OPENAI_MAX_RETRIES", "1")))
    except Exception:
        max_retries = 1
    return {"timeout": timeout_seconds, "max_retries": max_retries}


def _init_client() -> AzureOpenAI:
    endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
    api_key = os.getenv("AZURE_OPENAI_API_KEY")
    if api_key:
        return AzureOpenAI(
            azure_endpoint=endpoint,
            api_key=api_key,
            api_version="2024-06-01",
            **_client_tuning_kwargs(),
        )
    credential = DefaultAzureCredential()
    token_provider = get_bearer_token_provider(credential, "https://cognitiveservices.azure.com/.default")
    return AzureOpenAI(
        azure_endpoint=endpoint,
        azure_ad_token_provider=token_provider,
        api_version="2024-06-01",
        **_client_tuning_kwargs(),
    )


ROUTER_PROMPT = """You are ROUTER_LLM for an aviation Pilot Brief demo.
Output deterministic execution plan as JSON only.

RULES:
- Output valid JSON only, no prose.
- Use intent_graph as primary guide:
  Intent -> requires EvidenceType -> authoritative_in Tool.
- Choose tools only from tool_catalog.allowed_tools.
- Generate SQL/KQL using provided schemas only.
- If schema is missing, set needs_schema=true and include schema_requests.
- Always output coverage checklist for required evidence.
- If ask_recommendation=true, include SOPClause evidence.

Plan schema:
{
  "intent": {"name": string, "confidence": number},
  "time_window": {"horizon_min": number, "start_utc": string|null, "end_utc": string|null},
  "entities": {"airports": string[], "flight_ids": string[], "routes": string[], "stations": string[], "alternates": string[]},
  "required_evidence": [{"name": string, "optional": boolean, "requires_citations": boolean}],
  "tool_calls": [{"id": string, "tool": string, "operation": string, "depends_on": string[], "query": string|null, "params": object}],
  "coverage": [{"evidence": string, "status": "planned"|"missing", "via_tools": string[]}],
  "needs_schema": boolean,
  "schema_requests": [{"type":"sql"|"kql"|"graph","request":string}],
  "warnings": string[]
}
"""


class AgenticOrchestrator:
    def __init__(self):
        self.client = _init_client()
        self.model = os.getenv("AZURE_OPENAI_ORCHESTRATOR_DEPLOYMENT_NAME", "gpt-5-mini")

    def create_plan(
        self,
        user_query: str,
        runtime_context: Dict[str, Any],
        entities: Dict[str, Any],
        intent_graph: IntentGraphSnapshot,
        tool_catalog: Dict[str, Any],
        schemas: Dict[str, Any],
        required_sources: List[str] | None = None,
    ) -> AgenticPlan:
        payload = {
            "user_query": user_query,
            "runtime_context": runtime_context,
            "entities": entities,
            "intent_graph": intent_graph.data,
            "tool_catalog": tool_catalog,
            "schemas": {
                "sql_schema": schemas.get("sql_schema", {}),
                "kql_schema": schemas.get("kql_schema", {}),
            },
        }

        try:
            response = self.client.chat.completions.create(
                model=self.model,
                temperature=0,
                response_format={"type": "json_object"},
                messages=[
                    {"role": "system", "content": ROUTER_PROMPT},
                    {"role": "user", "content": json.dumps(payload, ensure_ascii=True)},
                ],
            )
            raw = response.choices[0].message.content or "{}"
            parsed = json.loads(raw)
            plan = AgenticPlan.from_dict(parsed)
            if not plan.tool_calls:
                return self._fallback_plan(user_query, runtime_context, intent_graph, required_sources or [])
            return self._enforce_required_sources(plan, required_sources or [])
        except Exception:
            return self._fallback_plan(user_query, runtime_context, intent_graph, required_sources or [])

    def _fallback_plan(
        self,
        query: str,
        runtime_context: Dict[str, Any],
        intent_graph: IntentGraphSnapshot,
        required_sources: List[str],
    ) -> AgenticPlan:
        intent_name = self._infer_intent(query)
        required_evidence = intent_graph.required_evidence_for_intent(intent_name)
        if runtime_context.get("ask_recommendation"):
            names = {r.get("name") for r in required_evidence}
            if "SOPClause" not in names:
                required_evidence.append({"name": "SOPClause", "optional": False, "requires_citations": True})

        tool_calls: List[ToolCall] = []
        call_idx = 1

        # Graph expansion first for pilot brief intents.
        if intent_name.startswith("PilotBrief"):
            tool_calls.append(
                ToolCall(
                    id=f"call_{call_idx}",
                    tool="GRAPH",
                    operation="entity_expansion",
                    depends_on=[],
                    params={"hops": 2},
                )
            )
            call_idx += 1

        evidence_tool_map: Dict[str, List[str]] = {}
        for req in required_evidence:
            ev_name = str(req.get("name", "")).strip()
            tools = intent_graph.tools_for_evidence(ev_name)
            canonical_tools = [self._canonical_tool_name(t) for t in tools if self._canonical_tool_name(t)]
            evidence_tool_map[ev_name] = canonical_tools
            for tool in canonical_tools[:1]:
                depends_on = ["call_1"] if tool_calls and tool_calls[0].operation == "entity_expansion" else []
                op = "lookup"
                if tool == "SQL":
                    op = "sql_lookup"
                elif tool == "KQL":
                    op = "kql_lookup"
                elif tool.startswith("VECTOR_"):
                    op = "semantic_lookup"
                tool_calls.append(
                    ToolCall(
                        id=f"call_{call_idx}",
                        tool=tool,
                        operation=op,
                        depends_on=depends_on,
                        query=query,
                        params={"evidence_type": ev_name},
                    )
                )
                call_idx += 1

        # Ensure required sources are always included.
        existing = {c.tool for c in tool_calls}
        for src in required_sources:
            canon = self._canonical_tool_name(src)
            if canon and canon not in existing:
                tool_calls.append(
                    ToolCall(
                        id=f"call_{call_idx}",
                        tool=canon,
                        operation="lookup",
                        depends_on=[],
                        query=query,
                        params={"forced": True},
                    )
                )
                call_idx += 1

        coverage = [
            CoverageItem(evidence=str(req.get("name", "")), status="planned", via_tools=evidence_tool_map.get(str(req.get("name", "")), []))
            for req in required_evidence
        ]
        return AgenticPlan(
            intent=Intent(name=intent_name, confidence=0.51),
            time_window=TimeWindow(horizon_min=int(runtime_context.get("default_time_horizon_min", 120) or 120)),
            entities={"airports": [], "flight_ids": [], "routes": [], "stations": [], "alternates": []},
            required_evidence=[EvidenceRequirement.from_dict(req) for req in required_evidence],
            tool_calls=tool_calls,
            coverage=coverage,
            needs_schema=False,
            schema_requests=[],
            warnings=["LLM routing unavailable; fallback orchestration used."],
        )

    def _infer_intent(self, query: str) -> str:
        q = query.lower()
        if any(t in q for t in ("policy", "sop", "compliance", "clause")):
            return "Policy.Check"
        if any(t in q for t in ("arrival", "approach", "landing")):
            return "PilotBrief.Arrival"
        if any(t in q for t in ("disruption", "delay", "irrops", "why")):
            return "Disruption.Explain"
        if any(t in q for t in ("replay", "history", "last week", "yesterday")):
            return "Replay.History"
        return "PilotBrief.Departure"

    def _enforce_required_sources(self, plan: AgenticPlan, required_sources: List[str]) -> AgenticPlan:
        existing = {self._canonical_tool_name(c.tool) for c in plan.tool_calls}
        next_id = len(plan.tool_calls) + 1
        for src in required_sources:
            canon = self._canonical_tool_name(src)
            if not canon or canon in existing:
                continue
            plan.tool_calls.append(
                ToolCall(
                    id=f"forced_{next_id}",
                    tool=canon,
                    operation="lookup",
                    depends_on=[],
                    query=None,
                    params={"forced": True},
                )
            )
            next_id += 1
        return plan

    def _canonical_tool_name(self, raw: str) -> str:
        value = (raw or "").strip().upper()
        mapping = {
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
        return mapping.get(value, value if value in {"KQL", "SQL", "GRAPH", "VECTOR_REG", "VECTOR_OPS", "VECTOR_AIRPORT", "NOSQL"} else "")
