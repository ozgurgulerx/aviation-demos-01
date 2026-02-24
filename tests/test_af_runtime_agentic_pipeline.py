import sys
import types
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from af_runtime import AgentFrameworkRuntime  # noqa: E402


class _FakeContext:
    retrieval_plan = {}
    source_traces = []
    route = "AGENTIC"
    reasoning = "test"
    context_text = "context"
    source_results = {}
    sql_results = []
    reconciled_items = []
    coverage_summary = {}
    conflict_summary = {}
    citations = []

    def to_event_payload(self):
        return {}


class AgentFrameworkRuntimeAgenticPipelineTests(unittest.TestCase):
    def _build_runtime_with_empty_agentic_synthesis(self) -> AgentFrameworkRuntime:
        runtime = object.__new__(AgentFrameworkRuntime)
        runtime._framework_label = "agentic-runtime"
        runtime.context_provider = types.SimpleNamespace(
            build_context=lambda *args, **kwargs: _FakeContext(),
        )
        runtime.retriever = types.SimpleNamespace(
            _filter_error_rows=lambda rows: rows,
            _synthesize_answer_stream=lambda *args, **kwargs: iter(()),
        )
        runtime._get_or_create_session = types.MethodType(lambda self, _session_id: {"id": "s"}, runtime)
        runtime._emit_source_trace_events = types.MethodType(lambda self, trace: [], runtime)
        runtime._invoke_agent = types.MethodType(
            lambda self, prompt, session, session_id: "",
            runtime,
        )
        return runtime

    def test_agentic_pipeline_emits_terminal_error_when_synthesis_is_empty(self):
        runtime = self._build_runtime_with_empty_agentic_synthesis()

        events = list(runtime._run_with_agent_framework(
            query="Build an operations summary.",
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
            precomputed_route={"route": "AGENTIC"},
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

        partial_done = next((e for e in events if e.get("type") == "agent_partial_done"), None)
        self.assertIsNotNone(partial_done)
        self.assertTrue(partial_done.get("partial"))
        self.assertFalse(partial_done.get("isVerified"))


if __name__ == "__main__":
    unittest.main()
