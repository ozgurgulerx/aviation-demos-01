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


if __name__ == "__main__":
    unittest.main()
