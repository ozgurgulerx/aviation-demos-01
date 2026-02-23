import sys
import types
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from af_runtime import AgentFrameworkRuntime  # noqa: E402


class AgentFrameworkRuntimeLocalPipelineTests(unittest.TestCase):
    def _build_runtime_with_failing_lookup(self) -> AgentFrameworkRuntime:
        runtime = object.__new__(AgentFrameworkRuntime)
        runtime._framework_label = "local-fallback"

        def _run_rag_lookup(*args, **kwargs):
            raise RuntimeError("lookup exploded")

        runtime.toolset = types.SimpleNamespace(run_rag_lookup=_run_rag_lookup)
        runtime._emit_source_trace_events = types.MethodType(lambda self, trace: [], runtime)
        runtime.retriever = types.SimpleNamespace()
        return runtime

    def _build_runtime_with_empty_synthesis(self) -> AgentFrameworkRuntime:
        runtime = object.__new__(AgentFrameworkRuntime)
        runtime._framework_label = "local-fallback"

        def _run_rag_lookup(*args, **kwargs):
            return {
                "route": "HYBRID",
                "reasoning": "test",
                "sql_results": [],
                "semantic_results": [],
                "source_results": {},
                "reconciled_items": [],
                "coverage_summary": {},
                "conflict_summary": {},
                "citations": [],
            }

        runtime.toolset = types.SimpleNamespace(run_rag_lookup=_run_rag_lookup)
        runtime._emit_source_trace_events = types.MethodType(lambda self, trace: [], runtime)
        runtime.retriever = types.SimpleNamespace(
            _filter_error_rows=lambda rows: rows,
            _synthesize_answer_stream=lambda *args, **kwargs: iter(()),
        )
        return runtime

    def _build_runtime_with_refusal_synthesis(self) -> AgentFrameworkRuntime:
        runtime = object.__new__(AgentFrameworkRuntime)
        runtime._framework_label = "local-fallback"

        def _run_rag_lookup(*args, **kwargs):
            return {
                "route": "HYBRID",
                "reasoning": "test",
                "sql_results": [],
                "semantic_results": [],
                "source_results": {},
                "reconciled_items": [],
                "coverage_summary": {},
                "conflict_summary": {},
                "citations": [],
            }

        def _synthesize_answer_stream(*args, **kwargs):
            yield {
                "type": "agent_error",
                "error_code": "llm_refusal",
                "terminal_reason": "llm_refusal",
                "message": "Model refused to answer this request.",
            }

        runtime.toolset = types.SimpleNamespace(run_rag_lookup=_run_rag_lookup)
        runtime._emit_source_trace_events = types.MethodType(lambda self, trace: [], runtime)
        runtime.retriever = types.SimpleNamespace(
            _filter_error_rows=lambda rows: rows,
            _synthesize_answer_stream=_synthesize_answer_stream,
        )
        return runtime

    def _build_runtime_with_internal_synthesis_error(self) -> AgentFrameworkRuntime:
        runtime = object.__new__(AgentFrameworkRuntime)
        runtime._framework_label = "local-fallback"

        def _run_rag_lookup(*args, **kwargs):
            return {
                "route": "HYBRID",
                "reasoning": "test",
                "sql_results": [],
                "semantic_results": [],
                "source_results": {},
                "reconciled_items": [],
                "coverage_summary": {},
                "conflict_summary": {},
                "citations": [],
            }

        def _synthesize_answer_stream(*args, **kwargs):
            yield {
                "type": "agent_error",
                "error_code": "synthesis_runtime_error",
                "terminal_reason": "synthesis_runtime_error",
                "message": "pyodbc failure: connection timeout",
            }

        runtime.toolset = types.SimpleNamespace(run_rag_lookup=_run_rag_lookup)
        runtime._emit_source_trace_events = types.MethodType(lambda self, trace: [], runtime)
        runtime.retriever = types.SimpleNamespace(
            _filter_error_rows=lambda rows: rows,
            _synthesize_answer_stream=_synthesize_answer_stream,
        )
        return runtime

    def test_local_pipeline_lookup_exception_emits_agent_error(self):
        runtime = self._build_runtime_with_failing_lookup()

        events = list(runtime._run_with_local_pipeline(
            query="What hazards exist?",
            session_id="session-1",
            retrieval_mode="code-rag",
            query_profile="pilot-brief",
            required_sources=["KQL"],
            source_policy="include",
            freshness_sla_minutes=None,
            explain_retrieval=False,
            risk_mode="standard",
            ask_recommendation=False,
            demo_scenario=None,
            precomputed_route={"route": "HYBRID"},
            conversation_history=None,
            failure_policy="graceful",
        ))

        error_event = next((event for event in events if event.get("type") == "agent_error"), None)
        self.assertIsNotNone(error_event)
        self.assertIn("RAG lookup failed before synthesis", str(error_event.get("message", "")))
        self.assertEqual(error_event.get("route"), "HYBRID")
        self.assertEqual(error_event.get("framework"), "local-fallback")

    def test_local_pipeline_emits_terminal_error_when_synthesis_is_empty(self):
        runtime = self._build_runtime_with_empty_synthesis()

        events = list(runtime._run_with_local_pipeline(
            query="Give me a quick operational brief.",
            session_id="session-2",
            retrieval_mode="code-rag",
            query_profile="pilot-brief",
            required_sources=[],
            source_policy="include",
            freshness_sla_minutes=None,
            explain_retrieval=False,
            risk_mode="standard",
            ask_recommendation=False,
            demo_scenario=None,
            precomputed_route={"route": "HYBRID"},
            conversation_history=None,
            failure_policy="graceful",
        ))

        synthesis_error = next(
            (
                event for event in events
                if event.get("type") == "agent_error"
                and event.get("error_code") == "empty_synthesis_output"
            ),
            None,
        )
        done_event = next((event for event in events if event.get("type") == "agent_done"), None)
        streamed_text = "".join(
            str(event.get("content", ""))
            for event in events
            if event.get("type") == "agent_update" and event.get("content")
        )

        self.assertIsNotNone(synthesis_error)
        self.assertIn("without answer text", str(synthesis_error.get("message", "")).lower())
        self.assertIn("could not produce a full synthesized brief", streamed_text.lower())
        self.assertIsNone(done_event)

    def test_local_pipeline_propagates_llm_refusal_as_terminal_error(self):
        runtime = self._build_runtime_with_refusal_synthesis()

        events = list(runtime._run_with_local_pipeline(
            query="Give me a quick operational brief.",
            session_id="session-3",
            retrieval_mode="code-rag",
            query_profile="pilot-brief",
            required_sources=[],
            source_policy="include",
            freshness_sla_minutes=None,
            explain_retrieval=False,
            risk_mode="standard",
            ask_recommendation=False,
            demo_scenario=None,
            precomputed_route={"route": "HYBRID"},
            conversation_history=None,
            failure_policy="graceful",
        ))

        synthesis_error = next(
            (
                event for event in events
                if event.get("type") == "agent_error"
                and event.get("error_code") == "llm_refusal"
            ),
            None,
        )
        done_event = next((event for event in events if event.get("type") == "agent_done"), None)

        self.assertIsNotNone(synthesis_error)
        self.assertEqual(synthesis_error.get("terminal_reason"), "llm_refusal")
        self.assertIsNone(done_event)

    def test_local_pipeline_sanitizes_internal_synthesis_errors(self):
        runtime = self._build_runtime_with_internal_synthesis_error()

        events = list(runtime._run_with_local_pipeline(
            query="Give me a quick operational brief.",
            session_id="session-4",
            retrieval_mode="code-rag",
            query_profile="pilot-brief",
            required_sources=[],
            source_policy="include",
            freshness_sla_minutes=None,
            explain_retrieval=False,
            risk_mode="standard",
            ask_recommendation=False,
            demo_scenario=None,
            precomputed_route={"route": "HYBRID"},
            conversation_history=None,
            failure_policy="graceful",
        ))

        synthesis_error = next(
            (
                event for event in events
                if event.get("type") == "agent_error"
                and event.get("error_code") == "synthesis_runtime_error"
            ),
            None,
        )
        self.assertIsNotNone(synthesis_error)
        self.assertNotIn("pyodbc", str(synthesis_error.get("message", "")).lower())

    def test_summarize_source_outcomes_treats_mixed_rows_as_satisfied(self):
        runtime = object.__new__(AgentFrameworkRuntime)
        degraded, failed = runtime._summarize_source_outcomes(
            {
                "KQL": [
                    {"error_code": "kql_runtime_error", "error": "http_400"},
                    {"flight_id": "TK1", "status": "ok"},
                ]
            },
            required_sources=["KQL"],
        )
        self.assertEqual(degraded, [])
        self.assertEqual(failed, [])

    def test_summarize_source_outcomes_marks_required_source_failed_when_all_rows_error(self):
        runtime = object.__new__(AgentFrameworkRuntime)
        degraded, failed = runtime._summarize_source_outcomes(
            {
                "KQL": [
                    {"error_code": "kql_runtime_error", "error": "http_400"},
                    {"error_code": "kql_runtime_error", "error": "kql_query_returned_no_rows"},
                ]
            },
            required_sources=["KQL"],
        )
        self.assertEqual(degraded, ["KQL"])
        self.assertEqual(failed, ["KQL"])


if __name__ == "__main__":
    unittest.main()
