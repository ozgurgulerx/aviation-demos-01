import sys
import types
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

# Minimal Azure SDK stubs for importing af_context_provider in lightweight CI/local envs.
if "azure" not in sys.modules:
    azure_mod = types.ModuleType("azure")
    azure_mod.__path__ = []  # type: ignore[attr-defined]
    sys.modules["azure"] = azure_mod
else:
    setattr(sys.modules["azure"], "__path__", getattr(sys.modules["azure"], "__path__", []))
if "azure.search" not in sys.modules:
    sys.modules["azure.search"] = types.ModuleType("azure.search")
if "azure.search.documents" not in sys.modules:
    m = types.ModuleType("azure.search.documents")
    m.SearchClient = object
    sys.modules["azure.search.documents"] = m
if "azure.search.documents.models" not in sys.modules:
    m = types.ModuleType("azure.search.documents.models")
    m.VectorizedQuery = object
    sys.modules["azure.search.documents.models"] = m
if "azure.core" not in sys.modules:
    sys.modules["azure.core"] = types.ModuleType("azure.core")
if "azure.core.credentials" not in sys.modules:
    m = types.ModuleType("azure.core.credentials")
    m.AzureKeyCredential = object
    sys.modules["azure.core.credentials"] = m
if "azure.identity" not in sys.modules:
    m = types.ModuleType("azure.identity")
    m.AzureCliCredential = object
    m.DefaultAzureCredential = object

    def _fake_get_bearer_token_provider(*_args, **_kwargs):
        return lambda: ""

    m.get_bearer_token_provider = _fake_get_bearer_token_provider
    sys.modules["azure.identity"] = m

from af_context_provider import AviationRagContextProvider  # noqa: E402
from contracts.agentic_plan import AgenticPlan, ToolCall  # noqa: E402


class AgenticExactSourcePolicyTests(unittest.TestCase):
    def _provider(self) -> AviationRagContextProvider:
        # We only test _enforce_exact_source_policy, so __init__ dependencies are unnecessary.
        return object.__new__(AviationRagContextProvider)

    def test_exact_policy_preserves_dependencies_for_kept_calls(self):
        provider = self._provider()
        plan = AgenticPlan(
            tool_calls=[
                ToolCall(id="call_1", tool="GRAPH", operation="entity_expansion", depends_on=[]),
                ToolCall(id="call_2", tool="SQL", operation="sql_lookup", depends_on=["call_1"]),
                ToolCall(id="call_3", tool="KQL", operation="kql_lookup", depends_on=["call_1"]),
            ]
        )

        out = provider._enforce_exact_source_policy(plan, ["GRAPH", "SQL"], query="q")
        call_ids = [c.id for c in out.tool_calls]
        self.assertEqual(call_ids, ["call_1", "call_2"])
        self.assertEqual(out.tool_calls[1].depends_on, ["call_1"])

    def test_exact_policy_filters_dependencies_to_kept_calls(self):
        provider = self._provider()
        plan = AgenticPlan(
            tool_calls=[
                ToolCall(id="call_1", tool="GRAPH", operation="entity_expansion", depends_on=[]),
                ToolCall(id="call_2", tool="SQL", operation="sql_lookup", depends_on=["call_1"]),
            ]
        )

        out = provider._enforce_exact_source_policy(plan, ["SQL"], query="q")
        self.assertEqual(len(out.tool_calls), 1)
        self.assertEqual(out.tool_calls[0].id, "call_2")
        self.assertEqual(out.tool_calls[0].depends_on, [])
        self.assertTrue(any(str(w).startswith("source_policy_exact_dropped_dependencies:") for w in out.warnings))


if __name__ == "__main__":
    unittest.main()
