#!/usr/bin/env python3
"""
Executes agentic plans against registered datastores/tools.
"""

from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
import json
import re
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

from contracts.agentic_plan import AgenticPlan, ToolCall
from query_writers import KQLWriter, SQLWriter
from unified_retriever import Citation, UnifiedRetriever
from shared_utils import utc_now as _utc_now, safe_preview_value, build_rows_preview, KNOWN_TOOLS, TOOL_ALIASES, canon_tool


@dataclass
class PlanExecutionResult:
    source_results: Dict[str, List[Dict[str, Any]]] = field(default_factory=dict)
    source_results_by_call: Dict[str, "CallResult"] = field(default_factory=dict)
    source_traces: List[Dict[str, Any]] = field(default_factory=list)
    citations: List[Citation] = field(default_factory=list)
    sql_queries: Dict[str, str] = field(default_factory=dict)
    evidence_tool_map: Dict[str, List[str]] = field(default_factory=dict)
    warnings: List[str] = field(default_factory=list)


@dataclass
class CallResult:
    call_id: str
    source: str
    operation: str
    rows: List[Dict[str, Any]] = field(default_factory=list)
    citations: List[Citation] = field(default_factory=list)
    sql_query: Optional[str] = None
    error: Optional[str] = None
    started_at: str = ""
    completed_at: str = ""
    params: Dict[str, Any] = field(default_factory=dict)


class PlanExecutor:
    def __init__(self, retriever: UnifiedRetriever):
        self.retriever = retriever
        self.sql_writer: Optional[SQLWriter] = None
        self.kql_writer: Optional[KQLWriter] = None

    def execute(
        self,
        user_query: str,
        plan: AgenticPlan,
        schemas: Dict[str, Any],
    ) -> PlanExecutionResult:
        result = PlanExecutionResult()

        # Pre-compute shared embedding for VECTOR tool calls using the same query.
        has_vector_calls = any(
            self._canon_tool(call.tool).startswith("VECTOR_") for call in plan.tool_calls
        )
        self._shared_embedding = None
        self._shared_embedding_query = user_query
        if has_vector_calls:
            _t0_emb = time.perf_counter()
            self._shared_embedding = self.retriever.get_embedding(user_query)
            logger.info("perf stage=%s ms=%.1f", "shared_embedding_agentic", (time.perf_counter() - _t0_emb) * 1000)

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

            started_at_map: Dict[str, str] = {}
            with ThreadPoolExecutor(max_workers=max(1, min(4, len(ready)))) as pool:
                future_map = {}
                for call in ready:
                    source = self._canon_tool(call.tool)
                    started_at = _utc_now()
                    started_at_map[call.id] = started_at
                    result.source_traces.append(
                        {
                            "type": "source_call_start",
                            "source": source,
                            "planned_source": source,
                            "executed_source": source,
                            "reason": call.operation,
                            "priority": 0,
                            "source_meta": self.retriever.source_event_meta(source),
                            "execution_mode": self.retriever.source_mode(source),
                            "contract_status": "planned",
                            "event_id": call.id,
                            "timestamp": started_at,
                        }
                    )
                    future_map[pool.submit(self._run_call, call, user_query, plan, schemas)] = call

                for future in as_completed(future_map):
                    call = future_map[future]
                    source = self._canon_tool(call.tool)
                    completed_at = _utc_now()
                    try:
                        rows, citations, sql_query = future.result()
                        rows = self._annotate_rows(rows, source, call.id, call.operation, call.params, completed_at)
                        has_row_errors = self._rows_have_errors(rows)
                        columns, rows_preview, rows_truncated = self._build_rows_preview(rows)
                        result.source_results_by_call[call.id] = CallResult(
                            call_id=call.id,
                            source=source,
                            operation=call.operation,
                            rows=rows,
                            citations=citations,
                            sql_query=sql_query,
                            error=None,
                            started_at=started_at_map.get(call.id, ""),
                            completed_at=completed_at,
                            params=dict(call.params or {}),
                        )
                        if sql_query:
                            result.sql_queries[call.id] = sql_query
                        execution_mode = self.retriever.source_mode(source)
                        if has_row_errors:
                            contract_status = "failed"
                        else:
                            contract_status = "met"
                        result.source_traces.append(
                            {
                                "type": "source_call_done",
                                "source": source,
                                "planned_source": source,
                                "executed_source": source,
                                "row_count": len(rows),
                                "citation_count": len(citations),
                                "source_meta": self.retriever.source_event_meta(source),
                                "execution_mode": execution_mode,
                                "contract_status": contract_status,
                                "event_id": call.id,
                                "timestamp": completed_at,
                                "columns": columns,
                                "rows_preview": rows_preview,
                                "rows_truncated": rows_truncated,
                            }
                        )
                    except Exception as exc:
                        error_rows = self._annotate_rows(
                            [{"error": str(exc)}],
                            source,
                            call.id,
                            call.operation,
                            call.params,
                            completed_at,
                        )
                        result.source_results_by_call[call.id] = CallResult(
                            call_id=call.id,
                            source=source,
                            operation=call.operation,
                            rows=error_rows,
                            citations=[],
                            sql_query=None,
                            error=str(exc),
                            started_at=started_at_map.get(call.id, ""),
                            completed_at=completed_at,
                            params=dict(call.params or {}),
                        )
                        columns, rows_preview, rows_truncated = self._build_rows_preview(error_rows)
                        result.source_traces.append(
                            {
                                "type": "source_call_done",
                                "source": source,
                                "planned_source": source,
                                "executed_source": source,
                                "row_count": 1,
                                "citation_count": 0,
                                "error": str(exc),
                                "source_meta": self.retriever.source_event_meta(source),
                                "execution_mode": self.retriever.source_mode(source),
                                "contract_status": "failed",
                                "event_id": call.id,
                                "timestamp": completed_at,
                                "columns": columns,
                                "rows_preview": rows_preview,
                                "rows_truncated": rows_truncated,
                            }
                        )
                    done_ids.add(call.id)
                    pending.pop(call.id, None)

        result.source_results = self._flatten_source_results(result.source_results_by_call, plan.tool_calls)
        result.citations = self._flatten_citations(result.source_results_by_call, plan.tool_calls)
        return result

    def _run_call(
        self,
        call: ToolCall,
        user_query: str,
        plan: AgenticPlan,
        schemas: Dict[str, Any],
    ) -> Tuple[List[Dict[str, Any]], List[Citation], Optional[str]]:
        source = self._canon_tool(call.tool)
        if not source:
            return [{"error": f"unknown_tool:{call.tool}"}], [], None
        evidence_type = str(call.params.get("evidence_type", "")).strip()
        time_window = plan.time_window.to_dict()
        entities = plan.entities

        if source == "SQL":
            sql_query = call.query
            if sql_query and sql_query.strip().startswith("-- NEED_SCHEMA"):
                rows, citations, resolved_sql = self._handle_sql_need_schema(
                    sql_query=sql_query,
                    user_query=user_query,
                    evidence_type=evidence_type,
                    entities=entities,
                )
                return rows, citations, resolved_sql
            if not sql_query or not self._looks_like_sql(sql_query):
                try:
                    sql_query = self._get_sql_writer().generate(
                        user_query=user_query,
                        evidence_type=evidence_type or "generic",
                        sql_schema=schemas.get("sql_schema", {}),
                        entities=entities,
                        time_window=time_window,
                        constraints=call.params,
                    )
                except Exception as exc:
                    return [{"error": str(exc), "error_code": "sql_generation_failed"}], [], None
            if sql_query.strip().startswith("-- NEED_SCHEMA"):
                rows, citations, resolved_sql = self._handle_sql_need_schema(
                    sql_query=sql_query,
                    user_query=user_query,
                    evidence_type=evidence_type,
                    entities=entities,
                )
                return rows, citations, resolved_sql
            rows, citations = self._execute_sql_raw(sql_query)
            return rows, citations, sql_query

        if source == "KQL":
            kql_query = call.query
            if not kql_query or not self._looks_like_kql(kql_query):
                try:
                    kql_query = self._get_kql_writer().generate(
                        user_query=user_query,
                        evidence_type=evidence_type or "generic",
                        kql_schema=schemas.get("kql_schema", {}),
                        entities=entities,
                        time_window=time_window,
                        constraints=call.params,
                    )
                except Exception as exc:
                    window = int(plan.time_window.horizon_min or call.params.get("window_minutes", 120))
                    rows, citations = self.retriever.query_kql(user_query, window_minutes=window)
                    rows.append(
                        {
                            "warning": "kql_generation_failed_using_direct_query",
                            "detail": str(exc),
                        }
                    )
                    return rows, citations, None
            if kql_query.strip().startswith("// NEED_SCHEMA"):
                window = int(plan.time_window.horizon_min or call.params.get("window_minutes", 120))
                rows, citations = self.retriever.query_kql(user_query, window_minutes=window)
                return rows, citations, kql_query
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
            # Reuse shared embedding when the query text matches the original user query.
            effective_query = call.query or user_query
            shared_emb = None
            if (
                self._shared_embedding is not None
                and effective_query == self._shared_embedding_query
            ):
                shared_emb = self._shared_embedding
            rows, citations = self.retriever.query_semantic(
                effective_query,
                top=int(call.params.get("top", 5)),
                embedding=shared_emb,
                source=source,
                filter_expression=call.params.get("filter"),
            )
            return rows, citations, None

        return [{"error": f"unknown_tool:{source}"}], [], None

    def _execute_sql_raw(self, sql_query: str) -> Tuple[List[Dict[str, Any]], List[Citation]]:
        return self.retriever.execute_sql_query(sql_query)

    def _handle_sql_need_schema(
        self,
        sql_query: str,
        user_query: str,
        evidence_type: str,
        entities: Dict[str, Any],
    ) -> Tuple[List[Dict[str, Any]], List[Citation], str]:
        fallback_sql = self.retriever._heuristic_sql_fallback(user_query, sql_query)
        if fallback_sql:
            rows, citations = self._execute_sql_raw(fallback_sql)
            if rows and not rows[0].get("error_code"):
                for row in rows:
                    if isinstance(row, dict):
                        row["partial_schema"] = sql_query
                        row["fallback_sql"] = fallback_sql
            return rows, citations, fallback_sql

        return [{"error": sql_query, "error_code": "sql_schema_missing"}], [], sql_query

    def _canon_tool(self, raw: str) -> str:
        return canon_tool(raw)

    def _looks_like_sql(self, text: str) -> bool:
        return bool(re.match(r"^\s*(SELECT|WITH)\b", text, re.IGNORECASE))

    def _looks_like_kql(self, text: str) -> bool:
        stripped = text.strip()
        lowered = stripped.lower()
        return "|" in stripped or lowered.startswith("let ") or lowered.startswith(".show")

    def _get_sql_writer(self) -> SQLWriter:
        if self.sql_writer is None:
            self.sql_writer = SQLWriter()
        return self.sql_writer

    def _get_kql_writer(self) -> KQLWriter:
        if self.kql_writer is None:
            self.kql_writer = KQLWriter()
        return self.kql_writer

    def _annotate_rows(
        self,
        rows: List[Dict[str, Any]],
        source: str,
        call_id: str,
        operation: str,
        params: Dict[str, Any],
        completed_at: str,
    ) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        evidence_type = str((params or {}).get("evidence_type", "")).strip()
        if not evidence_type:
            evidence_type = self._infer_evidence_type(source, call_id, operation)
        for row in rows:
            if not isinstance(row, dict):
                continue
            enriched = dict(row)
            enriched["__source"] = source
            enriched["__call_id"] = call_id
            enriched["__operation"] = operation
            enriched["__fetched_at"] = completed_at
            if evidence_type:
                enriched["__evidence_type"] = evidence_type
            out.append(enriched)
        return out

    def _infer_evidence_type(self, source: str, call_id: str, operation: str) -> str:
        source_u = (source or "").upper()
        call_u = (call_id or "").upper()
        op_u = (operation or "").upper()
        token = f"{call_u}:{op_u}"

        if source_u == "KQL":
            if "METAR" in token:
                return "METAR"
            if "TAF" in token:
                return "TAF"
            if "HAZARD" in token:
                return "Hazards"
        if source_u == "NOSQL" and "NOTAM" in token:
            return "NOTAM"
        if source_u == "SQL" and ("RUNWAY" in token or "CONSTRAINT" in token):
            return "RunwayConstraints"
        if source_u == "VECTOR_REG" and ("SOP" in token or "POLICY" in token):
            return "SOPClause"
        return ""

    def _flatten_source_results(
        self,
        by_call: Dict[str, CallResult],
        plan_calls: List[ToolCall],
    ) -> Dict[str, List[Dict[str, Any]]]:
        flattened: Dict[str, List[Dict[str, Any]]] = {}
        for call in plan_calls:
            call_result = by_call.get(call.id)
            if call_result is None:
                continue
            flattened.setdefault(call_result.source, []).extend(call_result.rows)
        return flattened

    def _flatten_citations(
        self,
        by_call: Dict[str, CallResult],
        plan_calls: List[ToolCall],
    ) -> List[Citation]:
        out: List[Citation] = []
        for call in plan_calls:
            call_result = by_call.get(call.id)
            if call_result is None:
                continue
            out.extend(call_result.citations)
        return out

    def _build_rows_preview(
        self,
        rows: List[Dict[str, Any]],
        max_rows: int = 5,
        max_columns: int = 8,
        max_chars: int = 180,
    ) -> Tuple[List[str], List[Dict[str, Any]], bool]:
        return build_rows_preview(rows, max_rows, max_columns, max_chars)

    def _rows_have_errors(self, rows: List[Dict[str, Any]]) -> bool:
        for row in rows:
            if not isinstance(row, dict):
                continue
            if row.get("error") or row.get("error_code"):
                return True
        return False

    def _safe_preview_value(self, value: Any, max_chars: int = 180) -> Any:
        return safe_preview_value(value, max_chars)
