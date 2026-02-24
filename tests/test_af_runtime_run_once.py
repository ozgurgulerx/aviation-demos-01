import sys
import types
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from af_runtime import AgentFrameworkRuntime  # noqa: E402


class AgentFrameworkRuntimeRunOnceTests(unittest.TestCase):
    def _build_runtime_with_events(self, events):
        runtime = object.__new__(AgentFrameworkRuntime)
        runtime._framework_label = "test-framework"

        def _run_stream(self, *args, **kwargs):
            for event in events:
                yield event

        runtime.run_stream = types.MethodType(_run_stream, runtime)
        return runtime

    def test_run_once_returns_error_payload_when_stream_ends_with_agent_error(self):
        runtime = self._build_runtime_with_events(
            [
                {"type": "agent_update", "content": "partial text"},
                {"type": "agent_error", "message": "Strict failure policy triggered by source errors: KQL", "route": "AGENTIC"},
            ]
        )

        result = runtime.run_once("query", failure_policy="strict")

        self.assertEqual(result.get("status"), "error")
        self.assertEqual(result.get("error"), "Strict failure policy triggered by source errors: KQL")
        self.assertEqual(result.get("route"), "AGENTIC")
        self.assertFalse(result.get("is_verified"))
        self.assertEqual(result.get("answer"), "partial text")

    def test_run_once_returns_error_when_agent_error_and_partial_done_coexist(self):
        runtime = self._build_runtime_with_events(
            [
                {"type": "agent_update", "content": "fallback text"},
                {
                    "type": "agent_error",
                    "message": "Synthesis failed.",
                    "error_code": "synthesis_runtime_error",
                    "route": "HYBRID",
                    "degradedSources": ["KQL"],
                    "failedRequiredSources": [],
                    "requiredSourcesSatisfied": True,
                    "missingRequiredSources": [],
                    "sourcePolicy": "include",
                },
                {
                    "type": "agent_partial_done",
                    "isVerified": False,
                    "route": "HYBRID",
                    "reasoning": "test",
                    "partial": True,
                    "degradedSources": ["KQL"],
                    "failedRequiredSources": [],
                    "requiredSourcesSatisfied": True,
                },
            ]
        )

        result = runtime.run_once("query", failure_policy="graceful")

        self.assertEqual(result.get("status"), "error")
        self.assertEqual(result.get("route"), "HYBRID")
        self.assertFalse(result.get("is_verified"))
        self.assertEqual(result.get("answer"), "fallback text")

    def test_run_once_prefers_agent_done_metadata_when_present(self):
        runtime = self._build_runtime_with_events(
            [
                {"type": "agent_update", "content": "done text"},
                {"type": "agent_done", "route": "HYBRID", "reasoning": "ok", "isVerified": True},
            ]
        )

        result = runtime.run_once("query", failure_policy="graceful")

        self.assertNotIn("status", result)
        self.assertEqual(result.get("route"), "HYBRID")
        self.assertTrue(result.get("is_verified"))
        self.assertEqual(result.get("answer"), "done text")


if __name__ == "__main__":
    unittest.main()
