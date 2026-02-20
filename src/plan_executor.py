#!/usr/bin/env python3
"""
Executes agentic plans against registered datastores/tools.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
import json
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
                            "reason": call.operation,
                            "priority": 0,
                            "source_meta": self.retriever.source_event_meta(source),
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
                        result.source_traces.append(
                            {
                                "type": "source_call_done",
                                "source": source,
                                "row_count": len(rows),
                                "citation_count": len(citations),
                                "source_meta": self.retriever.source_event_meta(source),
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
                                "row_count": 1,
                                "citation_count": 0,
                                "error": str(exc),
                                "source_meta": self.retriever.source_event_meta(source),
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
        evidence_type = str(call.params.get("evidence_type", "")).strip()
        time_window = plan.time_window.to_dict()
        entities = plan.entities

        if source == "SQL":
            sql_query = call.query
            if not sql_query or not self._looks_like_sql(sql_query):
                sql_query = self._get_sql_writer().generate(
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
                kql_query = self._get_kql_writer().generate(
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
        if not rows:
            return [], [], False

        hidden_keys = {"content_vector"}
        columns: List[str] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            for key in row.keys():
                if not isinstance(key, str) or key.startswith("__") or key in hidden_keys:
                    continue
                if key not in columns:
                    columns.append(key)
                    if len(columns) >= max_columns:
                        break
            if len(columns) >= max_columns:
                break

        preview: List[Dict[str, Any]] = []
        for row in rows[:max_rows]:
            if not isinstance(row, dict):
                continue
            item: Dict[str, Any] = {}
            for column in columns:
                if column in row:
                    item[column] = self._safe_preview_value(row[column], max_chars=max_chars)
            if item:
                preview.append(item)

        return columns, preview, len(rows) > len(preview)

    def _safe_preview_value(self, value: Any, max_chars: int = 180) -> Any:
        if value is None or isinstance(value, (int, float, bool)):
            return value

        if isinstance(value, str):
            return value if len(value) <= max_chars else value[: max_chars - 3] + "..."

        if hasattr(value, "isoformat"):
            try:
                return value.isoformat()
            except Exception:
                pass

        if isinstance(value, (dict, list, tuple)):
            try:
                serialized = json.dumps(value, ensure_ascii=True)
            except Exception:
                serialized = str(value)
            return serialized if len(serialized) <= max_chars else serialized[: max_chars - 3] + "..."

        rendered = str(value)
        return rendered if len(rendered) <= max_chars else rendered[: max_chars - 3] + "..."
