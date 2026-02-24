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


class _FakeRetriever:
    def __init__(self, capabilities):
        self._capabilities = capabilities

    def source_capabilities(self, refresh: bool = True):
        return list(self._capabilities)

    def _extract_airports_from_query(self, query: str):
        text = str(query or "").upper()
        airports = []
        if "SAW" in text:
            airports.append("LTFJ")
        if "AYT" in text:
            airports.append("LTAI")
        if "ADB" in text:
            airports.append("LTBJ")
        return airports

    def _extract_explicit_flight_identifiers(self, query: str):
        text = str(query or "").upper().replace(" ", "")
        return ["TK123"] if "TK123" in text else []


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

    def test_non_exact_policy_prunes_unavailable_tools(self):
        provider = self._provider()
        provider.retriever = _FakeRetriever(
            [
                {"source": "SQL", "status": "healthy", "reason_code": "ready"},
                {"source": "KQL", "status": "unavailable", "reason_code": "kql_endpoint_not_configured"},
            ]
        )
        plan = AgenticPlan(
            tool_calls=[
                ToolCall(id="call_sql", tool="SQL", operation="lookup", depends_on=[]),
                ToolCall(id="call_kql", tool="KQL", operation="lookup", depends_on=["call_sql"]),
            ]
        )

        out = provider._prune_non_viable_tool_calls(
            plan=plan,
            required_sources=[],
            source_policy="include",
            query="airport status",
        )

        self.assertEqual(len(out.tool_calls), 1)
        self.assertEqual(out.tool_calls[0].tool, "SQL")
        self.assertTrue(any(str(w).startswith("tool_pruned_unavailable:KQL") for w in out.warnings))

    def test_non_exact_policy_keeps_required_unavailable_source(self):
        provider = self._provider()
        provider.retriever = _FakeRetriever(
            [
                {"source": "KQL", "status": "unavailable", "reason_code": "kql_endpoint_not_configured"},
            ]
        )
        plan = AgenticPlan(
            tool_calls=[
                ToolCall(id="call_kql", tool="KQL", operation="lookup", depends_on=[]),
            ]
        )

        out = provider._prune_non_viable_tool_calls(
            plan=plan,
            required_sources=["KQL"],
            source_policy="include",
            query="airport status",
        )

        self.assertEqual(len(out.tool_calls), 1)
        self.assertEqual(out.tool_calls[0].tool, "KQL")

    def test_non_exact_policy_prunes_airport_only_kql_calls(self):
        provider = self._provider()
        provider.retriever = _FakeRetriever(
            [
                {"source": "KQL", "status": "healthy", "reason_code": "ready"},
            ]
        )
        plan = AgenticPlan(
            tool_calls=[
                ToolCall(id="call_kql", tool="KQL", operation="lookup", depends_on=[]),
            ]
        )

        out = provider._prune_non_viable_tool_calls(
            plan=plan,
            required_sources=[],
            source_policy="include",
            query="compare next-90-minute flight risk across SAW, AYT, ADB",
        )

        self.assertEqual(len(out.tool_calls), 0)
        self.assertIn("tool_pruned_unmappable_airport_kql:KQL", out.warnings)

    def test_non_exact_policy_prunes_fabric_sql_for_short_horizon_departure_risk_compare(self):
        provider = self._provider()
        provider.retriever = _FakeRetriever(
            [
                {"source": "FABRIC_SQL", "status": "healthy", "reason_code": "ready"},
            ]
        )
        plan = AgenticPlan(
            tool_calls=[
                ToolCall(id="call_fabric_sql", tool="FABRIC_SQL", operation="lookup", depends_on=[]),
            ]
        )

        out = provider._prune_non_viable_tool_calls(
            plan=plan,
            required_sources=[],
            source_policy="include",
            query="compare next-90-minute departure risk across SAW, AYT, ADB",
        )

        self.assertEqual(len(out.tool_calls), 0)
        self.assertIn(
            "tool_pruned_short_horizon_departure_risk_fabric_sql:FABRIC_SQL",
            out.warnings,
        )

    def test_non_exact_policy_keeps_required_fabric_sql_for_short_horizon_departure_risk_compare(self):
        provider = self._provider()
        provider.retriever = _FakeRetriever(
            [
                {"source": "FABRIC_SQL", "status": "healthy", "reason_code": "ready"},
            ]
        )
        plan = AgenticPlan(
            tool_calls=[
                ToolCall(id="call_fabric_sql", tool="FABRIC_SQL", operation="lookup", depends_on=[]),
            ]
        )

        out = provider._prune_non_viable_tool_calls(
            plan=plan,
            required_sources=["FABRIC_SQL"],
            source_policy="include",
            query="compare next-90-minute departure risk across SAW, AYT, ADB",
        )

        self.assertEqual(len(out.tool_calls), 1)
        self.assertEqual(out.tool_calls[0].tool, "FABRIC_SQL")


if __name__ == "__main__":
    unittest.main()
