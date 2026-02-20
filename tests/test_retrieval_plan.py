import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from retrieval_plan import RetrievalRequest, build_retrieval_plan


class RetrievalPlanTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
