import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from retrieval_plan import ExactPolicyValidationError, RetrievalRequest, build_retrieval_plan
from shared_utils import validate_source_policy_request


class RetrievalPlanTests(unittest.TestCase):
    def test_exact_policy_invalid_sources_raises_validation_error(self):
        req = RetrievalRequest(
            query="Give me a brief",
            required_sources=["SQLL"],  # typo
            source_policy="exact",
        )
        with self.assertRaises(ExactPolicyValidationError):
            build_retrieval_plan(req, route="HYBRID", route_reasoning="test")

    def test_exact_policy_empty_required_sources_raises_validation_error(self):
        req = RetrievalRequest(
            query="Give me a brief",
            required_sources=[],
            source_policy="exact",
        )
        with self.assertRaises(ExactPolicyValidationError):
            build_retrieval_plan(req, route="HYBRID", route_reasoning="test")

    def test_exact_policy_honors_aliases(self):
        req = RetrievalRequest(
            query="Give me a brief",
            required_sources=["FabricGraph", "fabricsql"],
            source_policy="exact",
        )
        plan = build_retrieval_plan(req, route="HYBRID", route_reasoning="test")
        self.assertEqual([s.source for s in plan.steps], ["GRAPH", "FABRIC_SQL"])

    def test_validate_source_policy_request_exact(self):
        valid = validate_source_policy_request(["sql", "graph"], "exact")
        self.assertTrue(valid["is_valid"])
        self.assertEqual(valid["required_sources_normalized"], ["SQL", "GRAPH"])

        invalid = validate_source_policy_request(["sql", "bogus"], "exact")
        self.assertFalse(invalid["is_valid"])
        self.assertEqual(invalid["error_code"], "exact_required_sources_invalid")
        self.assertIn("bogus", invalid["invalid_required_sources"])

    def test_plan_includes_realtime_and_graph_sources(self):
        req = RetrievalRequest(
            query="Live LTFM disruption impact dependencies in last 30 minutes",
            retrieval_mode="code-rag",
            query_profile="pilot-brief",
        )
        plan = build_retrieval_plan(req, route="HYBRID", route_reasoning="test")
        sources = [s.source for s in plan.steps]
        self.assertIn("KQL", sources)
        self.assertIn("GRAPH", sources)
        self.assertIn("SQL", sources)

    def test_plan_honors_required_sources(self):
        req = RetrievalRequest(
            query="Summarize relevant events",
            required_sources=["NOSQL", "VECTOR_REG"],
        )
        plan = build_retrieval_plan(req, route="SEMANTIC", route_reasoning="test")
        sources = [s.source for s in plan.steps]
        self.assertIn("NOSQL", sources)
        self.assertIn("VECTOR_REG", sources)

    def test_plan_for_foundry_iq_keeps_semantic_source(self):
        req = RetrievalRequest(
            query="Explain similar winter crosswind incidents",
            retrieval_mode="foundry-iq",
            query_profile="pilot-brief",
        )
        plan = build_retrieval_plan(req, route="SEMANTIC", route_reasoning="test")
        sources = [s.source for s in plan.steps]
        self.assertIn("VECTOR_OPS", sources)

    def test_ops_delay_query_prefers_sql_over_fabric_sql(self):
        req = RetrievalRequest(
            query="Show delay trend from ops_flight_legs for last 7 days",
            retrieval_mode="code-rag",
            query_profile="pilot-brief",
        )
        plan = build_retrieval_plan(req, route="HYBRID", route_reasoning="test")
        sources = [s.source for s in plan.steps]
        self.assertIn("SQL", sources)
        self.assertNotIn("FABRIC_SQL", sources)


if __name__ == "__main__":
    unittest.main()
