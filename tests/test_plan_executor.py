import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from contracts.agentic_plan import AgenticPlan, ToolCall
from plan_executor import PlanExecutor


class _DummyRetriever:
    def source_event_meta(self, source: str):
        return {"source": source, "endpoint_label": "test", "freshness": "test"}

    def source_mode(self, source: str):
        return "live"

    def query_nosql(self, query: str):
        if "one" in query:
            return [{"id": "n1", "value": 1}], []
        return [{"id": "n2", "value": 2}], []

    def _heuristic_sql_fallback(self, _query: str, _detail: str):
        return "SELECT 'KJFK' AS facility, 2 AS report_count"

    def execute_sql_query(self, _sql_query: str):
        return [{"facility": "KJFK", "report_count": 2}], []

    def get_embedding(self, _query: str):
        raise RuntimeError("embedding backend unavailable")

    def query_semantic(self, _query: str, top: int = 5, embedding=None, source: str = "VECTOR_OPS", filter_expression=None):
        return [{"id": "vec1", "title": "sample"}], []


class _RepairingRetriever(_DummyRetriever):
    def execute_sql_query(self, sql_query: str):
        normalized = str(sql_query or "").strip().lower()
        if "bad_col" in normalized:
            return [
                {
                    "error": "missing columns in current schema: bad_col",
                    "error_code": "sql_schema_missing",
                }
            ], []
        if "coalesce(nullif(location" in normalized:
            return [{"facility": "KJFK", "report_count": 3}], []
        return [{"error": "unexpected_sql", "error_code": "sql_runtime_error"}], []


class _RepairingSQLWriter:
    def __init__(self):
        self.last_constraints = {}

    def generate(self, **kwargs):
        self.last_constraints = dict(kwargs.get("constraints") or {})
        return (
            "SELECT COALESCE(NULLIF(location, ''), 'UNKNOWN') AS facility, "
            "COUNT(*) AS report_count "
            "FROM asrs_reports GROUP BY facility ORDER BY report_count DESC LIMIT 5"
        )


class _StillBrokenRetriever(_DummyRetriever):
    def execute_sql_query(self, sql_query: str):
        normalized = str(sql_query or "").strip().lower()
        if "bad_col" in normalized:
            return [
                {
                    "error": "missing columns in current schema: bad_col",
                    "error_code": "sql_schema_missing",
                }
            ], []
        if "still_bad" in normalized:
            return [
                {
                    "error": "missing columns in current schema: still_bad",
                    "error_code": "sql_schema_missing",
                }
            ], []
        if "ltfj" in normalized:
            return [{"airport": "LTFJ", "runway_id": 1, "surface": "asphalt"}], []
        return [{"error": "unexpected_sql", "error_code": "sql_runtime_error"}], []

    def _heuristic_sql_fallback(self, _query: str, _detail: str):
        return "SELECT 'LTFJ' AS airport, 1 AS runway_id, 'asphalt' AS surface"


class _StillBrokenSQLWriter:
    def generate(self, **_kwargs):
        return "SELECT still_bad FROM demo.ourairports_runways LIMIT 5"


class PlanExecutorTests(unittest.TestCase):
    def test_execute_preserves_multiple_calls_same_source(self):
        retriever = _DummyRetriever()
        executor = PlanExecutor(retriever)  # type: ignore[arg-type]
        plan = AgenticPlan(
            tool_calls=[
                ToolCall(id="call_1", tool="NOSQL", operation="lookup", query="doc one"),
                ToolCall(id="call_2", tool="NOSQL", operation="lookup", query="doc two"),
            ]
        )

        result = executor.execute(user_query="test", plan=plan, schemas={})

        self.assertIn("NOSQL", result.source_results)
        rows = result.source_results["NOSQL"]
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0].get("id"), "n1")
        self.assertEqual(rows[1].get("id"), "n2")
        self.assertIn("call_1", result.source_results_by_call)
        self.assertIn("call_2", result.source_results_by_call)
        self.assertEqual(rows[0].get("__call_id"), "call_1")
        self.assertEqual(rows[1].get("__call_id"), "call_2")
        done_events = [e for e in result.source_traces if e.get("type") == "source_call_done"]
        self.assertTrue(done_events)
        self.assertEqual(done_events[0].get("contract_status"), "met")
        self.assertEqual(done_events[0].get("execution_mode"), "live")

    def test_sql_need_schema_uses_heuristic_fallback(self):
        retriever = _DummyRetriever()
        executor = PlanExecutor(retriever)  # type: ignore[arg-type]
        plan = AgenticPlan(
            tool_calls=[
                ToolCall(
                    id="call_sql",
                    tool="SQL",
                    operation="aggregate",
                    query="-- NEED_SCHEMA: damage_score missing",
                    params={"evidence_type": "generic"},
                )
            ]
        )

        result = executor.execute(user_query="top facilities by count", plan=plan, schemas={})

        self.assertIn("SQL", result.source_results)
        rows = result.source_results["SQL"]
        self.assertEqual(rows[0].get("facility"), "KJFK")
        self.assertEqual(rows[0].get("report_count"), 2)
        self.assertEqual(rows[0].get("partial_schema"), "-- NEED_SCHEMA: damage_score missing")

    def test_vector_execution_continues_when_shared_embedding_precompute_fails(self):
        retriever = _DummyRetriever()
        executor = PlanExecutor(retriever)  # type: ignore[arg-type]
        plan = AgenticPlan(
            tool_calls=[
                ToolCall(id="call_vec", tool="VECTOR_OPS", operation="lookup", query="runway risk"),
            ]
        )

        result = executor.execute(user_query="runway risk", plan=plan, schemas={})

        self.assertIn("VECTOR_OPS", result.source_results)
        self.assertEqual(result.source_results["VECTOR_OPS"][0].get("id"), "vec1")
        self.assertTrue(any("shared_embedding_failed:" in warning for warning in result.warnings))

    def test_sql_validation_failure_regenerates_and_executes_repaired_query(self):
        retriever = _RepairingRetriever()
        executor = PlanExecutor(retriever)  # type: ignore[arg-type]
        writer = _RepairingSQLWriter()
        executor.sql_writer = writer  # type: ignore[assignment]
        plan = AgenticPlan(
            tool_calls=[
                ToolCall(
                    id="call_sql_repair",
                    tool="SQL",
                    operation="aggregate",
                    query="SELECT bad_col FROM asrs_reports LIMIT 5",
                    params={"evidence_type": "generic"},
                )
            ]
        )

        result = executor.execute(
            user_query="Top facilities by ASRS report count",
            plan=plan,
            schemas={"sql_schema": {"tables": [{"table": "asrs_reports", "columns": [{"name": "location"}]}]}},
        )

        self.assertIn("SQL", result.source_results)
        rows = result.source_results["SQL"]
        self.assertEqual(rows[0].get("facility"), "KJFK")
        self.assertEqual(rows[0].get("report_count"), 3)
        self.assertEqual(writer.last_constraints.get("previous_error_code"), "sql_schema_missing")
        self.assertIn("missing columns", writer.last_constraints.get("previous_error_detail", ""))
        done_events = [e for e in result.source_traces if e.get("type") == "source_call_done" and e.get("source") == "SQL"]
        self.assertTrue(done_events)
        self.assertTrue(done_events[0].get("query_rewritten"))
        self.assertEqual(done_events[0].get("rewrite_reason"), "sql_regenerated_after_validation_failed")

    def test_sql_validation_failure_regeneration_can_fall_back_to_heuristic_sql(self):
        retriever = _StillBrokenRetriever()
        executor = PlanExecutor(retriever)  # type: ignore[arg-type]
        executor.sql_writer = _StillBrokenSQLWriter()  # type: ignore[assignment]
        plan = AgenticPlan(
            tool_calls=[
                ToolCall(
                    id="call_sql_fallback",
                    tool="SQL",
                    operation="aggregate",
                    query="SELECT bad_col FROM demo.ourairports_runways LIMIT 5",
                    params={"evidence_type": "generic"},
                )
            ]
        )

        result = executor.execute(
            user_query="compare next-90-minute flight risk across SAW, AYT, ADB",
            plan=plan,
            schemas={"sql_schema": {"tables": [{"table": "ourairports_runways", "columns": [{"name": "id"}]}]}},
        )

        self.assertIn("SQL", result.source_results)
        rows = result.source_results["SQL"]
        self.assertEqual(rows[0].get("airport"), "LTFJ")
        self.assertEqual(rows[0].get("runway_id"), 1)
        done_events = [e for e in result.source_traces if e.get("type") == "source_call_done" and e.get("source") == "SQL"]
        self.assertTrue(done_events)
        self.assertEqual(
            done_events[0].get("rewrite_reason"),
            "sql_regenerated_after_validation_failed+heuristic_fallback",
        )


if __name__ == "__main__":
    unittest.main()
