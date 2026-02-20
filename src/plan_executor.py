#!/usr/bin/env python3
"""
Executes agentic plans against registered datastores/tools.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
import re
from typing import Any, Dict, List, Optional, Tuple

from contracts.agentic_plan import AgenticPlan, ToolCall
from query_writers import KQLWriter, SQLWriter
from unified_retriever import Citation, UnifiedRetriever


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class PlanExecutionResult:
    source_results: Dict[str, List[Dict[str, Any]]] = field(default_factory=dict)
    source_traces: List[Dict[str, Any]] = field(default_factory=list)
    citations: List[Citation] = field(default_factory=list)
    sql_queries: Dict[str, str] = field(default_factory=dict)
    evidence_tool_map: Dict[str, List[str]] = field(default_factory=dict)
    warnings: List[str] = field(default_factory=list)


class PlanExecutor:
    def __init__(self, retriever: UnifiedRetriever):
        self.retriever = retriever
        self.sql_writer = SQLWriter()
        self.kql_writer = KQLWriter()

    def execute(
        self,
        user_query: str,
        plan: AgenticPlan,
        schemas: Dict[str, Any],
    ) -> PlanExecutionResult:
        result = PlanExecutionResult()

        # Build evidence -> tool map for post-verification.
        for req in plan.required_evidence:
            via = []
            for cov in plan.coverage:
                if cov.evidence == req.name:
                    via = list(cov.via_tools)
                    break
            result.evidence_tool_map[req.name] = [self._canon_tool(t) for t in via if self._canon_tool(t)]

        pending: Dict[str, ToolCall] = {call.id: call for call in plan.tool_calls}
        done_ids: set[str] = set()

        while pending:
            ready = [call for call in pending.values() if all(dep in done_ids for dep in call.depends_on)]
            if not ready:
                # Break dependency deadlocks safely for demo execution.
                ready = [next(iter(pending.values()))]
                result.warnings.append("Dependency cycle detected; executing remaining calls without full dependency order.")

            with ThreadPoolExecutor(max_workers=max(1, min(4, len(ready)))) as pool:
                future_map = {}
                for call in ready:
                    source = self._canon_tool(call.tool)
                    result.source_traces.append(
                        {
                            "type": "source_call_start",
                            "source": source,
                            "reason": call.operation,
                            "priority": 0,
                            "source_meta": self.retriever.source_event_meta(source),
                            "event_id": call.id,
                            "timestamp": _utc_now(),
                        }
                    )
                    future_map[pool.submit(self._run_call, call, user_query, plan, schemas)] = call

                for future in as_completed(future_map):
                    call = future_map[future]
                    source = self._canon_tool(call.tool)
                    try:
                        rows, citations, sql_query = future.result()
                        result.source_results[source] = rows
                        result.citations.extend(citations)
                        if sql_query:
                            result.sql_queries[call.id] = sql_query
                        result.source_traces.append(
                            {
                                "type": "source_call_done",
                                "source": source,
                                "row_count": len(rows),
                                "citation_count": len(citations),
                                "source_meta": self.retriever.source_event_meta(source),
                                "event_id": call.id,
                                "timestamp": _utc_now(),
                            }
                        )
                    except Exception as exc:
                        result.source_results[source] = [{"error": str(exc)}]
                        result.source_traces.append(
                            {
                                "type": "source_call_done",
                                "source": source,
                                "row_count": 1,
                                "citation_count": 0,
                                "error": str(exc),
                                "source_meta": self.retriever.source_event_meta(source),
                                "event_id": call.id,
                                "timestamp": _utc_now(),
                            }
                        )
                    done_ids.add(call.id)
                    pending.pop(call.id, None)

        return result

    def _run_call(
        self,
        call: ToolCall,
        user_query: str,
        plan: AgenticPlan,
        schemas: Dict[str, Any],
    ) -> Tuple[List[Dict[str, Any]], List[Citation], Optional[str]]:
        source = self._canon_tool(call.tool)
        evidence_type = str(call.params.get("evidence_type", "")).strip()
        time_window = plan.time_window.to_dict()
        entities = plan.entities

        if source == "SQL":
            sql_query = call.query
            if not sql_query or not self._looks_like_sql(sql_query):
                sql_query = self.sql_writer.generate(
                    user_query=user_query,
                    evidence_type=evidence_type or "generic",
                    sql_schema=schemas.get("sql_schema", {}),
                    entities=entities,
                    time_window=time_window,
                    constraints=call.params,
                )
            if sql_query.strip().startswith("-- NEED_SCHEMA"):
                return [{"error": sql_query}], [], sql_query
            rows, citations = self._execute_sql_raw(sql_query)
            return rows, citations, sql_query

        if source == "KQL":
            kql_query = call.query
            if not kql_query or not self._looks_like_kql(kql_query):
                kql_query = self.kql_writer.generate(
                    user_query=user_query,
                    evidence_type=evidence_type or "generic",
                    kql_schema=schemas.get("kql_schema", {}),
                    entities=entities,
                    time_window=time_window,
                    constraints=call.params,
                )
            window = int(plan.time_window.horizon_min or call.params.get("window_minutes", 120))
            rows, citations = self.retriever.query_kql(kql_query, window_minutes=window)
            return rows, citations, kql_query

        if source == "GRAPH":
            hops = int(call.params.get("hops", 2))
            rows, citations = self.retriever.query_graph(call.query or user_query, hops=hops)
            return rows, citations, None

        if source == "NOSQL":
            rows, citations = self.retriever.query_nosql(call.query or user_query)
            return rows, citations, None

        if source in {"VECTOR_OPS", "VECTOR_REG", "VECTOR_AIRPORT"}:
            rows, citations = self.retriever.query_semantic(
                call.query or user_query,
                top=int(call.params.get("top", 5)),
                source=source,
                filter_expression=call.params.get("filter"),
            )
            return rows, citations, None

        return [{"error": f"unknown_tool:{source}"}], [], None

    def _execute_sql_raw(self, sql_query: str) -> Tuple[List[Dict[str, Any]], List[Citation]]:
        cur = self.retriever.db.cursor()
        cur.execute(sql_query)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description] if cur.description else []
        dict_rows = [dict(zip(columns, row)) for row in rows]

        citations: List[Citation] = []
        for idx, row in enumerate(dict_rows[:10], start=1):
            row_id = row.get("id") or row.get("asrs_report_id") or f"row_{idx}"
            title = row.get("title") or row.get("facilityDesignator") or f"SQL row {idx}"
            citations.append(
                Citation(
                    source_type="SQL",
                    identifier=str(row_id),
                    title=str(title),
                    content_preview=str(row)[:120],
                    score=0.9,
                    dataset="aviation_db",
                )
            )
        return dict_rows, citations

    def _canon_tool(self, raw: str) -> str:
        value = (raw or "").strip().upper()
        mapping = {
            "EVENTHOUSEKQL": "KQL",
            "WAREHOUSESQL": "SQL",
            "FABRICGRAPH": "GRAPH",
            "GRAPHTRAVERSAL": "GRAPH",
            "FOUNDRYIQ": "VECTOR_REG",
            "AZUREAISEARCH": "VECTOR_REG",
        }
        return mapping.get(value, value)

    def _looks_like_sql(self, text: str) -> bool:
        return bool(re.match(r"^\s*(SELECT|WITH)\b", text, re.IGNORECASE))

    def _looks_like_kql(self, text: str) -> bool:
        stripped = text.strip()
        return "|" in stripped or stripped.lower().startswith("let ")
