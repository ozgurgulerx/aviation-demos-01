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


if __name__ == "__main__":
    unittest.main()
