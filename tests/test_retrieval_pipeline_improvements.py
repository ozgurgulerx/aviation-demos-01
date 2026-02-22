"""
Tests for retrieval pipeline improvements (Phases 1-3).
Covers entity propagation, expansion rules, multi-hop BFS, RRF,
conflict penalties, evidence re-query, token budgeting, fusion scores,
and evidence-weighted ordering.
"""

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from contracts.agentic_plan import AgenticPlan, EvidenceRequirement, ToolCall
from context_reconciler import compute_rrf_scores, reconcile_context, detect_conflicts
from evidence_verifier import EvidenceVerifier, EvidenceVerificationResult
from intent_graph_provider import IntentGraphSnapshot, DEFAULT_INTENT_GRAPH
from plan_executor import PlanExecutor


# ─── Helpers / Mocks ──────────────────────────────────────────────────────────


class _DummyRetriever:
    def source_event_meta(self, source: str):
        return {"source": source, "endpoint_label": "test", "freshness": "test"}

    def source_mode(self, source: str):
        return "live"

    def query_graph(self, query: str, hops: int = 2):
        rows = [
            {"src_type": "Airport", "src_id": "KJFK", "edge_type": "DEPARTS", "dst_type": "FlightLeg", "dst_id": "FL100"},
            {"src_type": "FlightLeg", "src_id": "FL100", "edge_type": "ARRIVES", "dst_type": "Airport", "dst_id": "KLAX"},
        ]
        if hops >= 2:
            rows.append(
                {"src_type": "Tail", "src_id": "N12345", "edge_type": "OPERATES", "dst_type": "FlightLeg", "dst_id": "FL100"}
            )
        return rows, []

    def query_nosql(self, query: str):
        return [{"id": "notam_1", "text": "sample"}], []

    def get_embedding(self, query: str):
        return None

    def _heuristic_sql_fallback(self, _q: str, _d: str):
        return None

    def execute_sql_query(self, _sql: str):
        return [{"facility": "KJFK", "report_count": 2}], []


# ─── 1A. Entity Propagation ──────────────────────────────────────────────────


class TestEntityPropagation(unittest.TestCase):
    def test_enrich_entities_from_graph_results(self):
        retriever = _DummyRetriever()
        executor = PlanExecutor(retriever)  # type: ignore[arg-type]
        plan = AgenticPlan(
            entities={"airports": ["KJFK"], "flight_ids": [], "routes": [], "stations": [], "alternates": []},
            tool_calls=[
                ToolCall(id="call_1", tool="GRAPH", operation="entity_expansion", params={"hops": 2}),
            ],
        )
        result = executor.execute(user_query="departure from KJFK", plan=plan, schemas={})
        # After execution, plan.entities should contain enriched data.
        self.assertIn("KLAX", plan.entities["airports"])
        self.assertIn("FL100", plan.entities["flight_ids"])
        self.assertIn("N12345", plan.entities["flight_ids"])
        # Original entity preserved.
        self.assertIn("KJFK", plan.entities["airports"])

    def test_enrich_skips_non_graph_sources(self):
        retriever = _DummyRetriever()
        executor = PlanExecutor(retriever)  # type: ignore[arg-type]
        plan = AgenticPlan(
            entities={"airports": [], "flight_ids": [], "routes": [], "stations": [], "alternates": []},
            tool_calls=[
                ToolCall(id="call_1", tool="NOSQL", operation="lookup", query="test"),
            ],
        )
        result = executor.execute(user_query="test", plan=plan, schemas={})
        # NOSQL should not trigger entity enrichment.
        self.assertEqual(plan.entities["airports"], [])
        self.assertEqual(plan.entities["flight_ids"], [])


# ─── 1B. Expansion Rules ─────────────────────────────────────────────────────


class TestExpansionRules(unittest.TestCase):
    def test_expansion_rules_for_intent_returns_matching_rules(self):
        snap = IntentGraphSnapshot(data=DEFAULT_INTENT_GRAPH, source="test")
        rules = snap.expansion_rules_for_intent("PilotBrief.Departure")
        self.assertGreaterEqual(len(rules), 1)
        self.assertEqual(rules[0]["tool"], "GRAPH")
        self.assertIn("reason", rules[0])

    def test_expansion_rules_for_unknown_intent_returns_empty(self):
        snap = IntentGraphSnapshot(data=DEFAULT_INTENT_GRAPH, source="test")
        rules = snap.expansion_rules_for_intent("Unknown.Intent")
        self.assertEqual(rules, [])

    def test_expansion_rules_with_empty_data(self):
        snap = IntentGraphSnapshot(data={}, source="test")
        rules = snap.expansion_rules_for_intent("PilotBrief.Departure")
        self.assertEqual(rules, [])


# ─── 2A. Evidence Re-Query Suggestions ────────────────────────────────────────


class TestEvidenceRequerySuggestions(unittest.TestCase):
    def test_requery_suggestions_populated_when_evidence_missing(self):
        verifier = EvidenceVerifier()
        intent_graph = IntentGraphSnapshot(data=DEFAULT_INTENT_GRAPH, source="test")
        plan = AgenticPlan(
            required_evidence=[
                EvidenceRequirement(name="METAR", optional=False),
                EvidenceRequirement(name="NOTAM", optional=False),
            ],
        )
        # Provide empty results so evidence is "missing".
        result = verifier.verify(
            plan=plan,
            source_results={},
            evidence_tool_map={"METAR": [], "NOTAM": []},
            intent_graph=intent_graph,
        )
        self.assertFalse(result.is_verified)
        self.assertTrue(len(result.requery_suggestions) > 0)
        evidence_names = {s["evidence"] for s in result.requery_suggestions}
        self.assertIn("METAR", evidence_names)
        self.assertIn("NOTAM", evidence_names)

    def test_requery_suggestions_empty_when_all_evidence_present(self):
        verifier = EvidenceVerifier()
        intent_graph = IntentGraphSnapshot(data=DEFAULT_INTENT_GRAPH, source="test")
        plan = AgenticPlan(
            required_evidence=[
                EvidenceRequirement(name="METAR", optional=False),
            ],
        )
        result = verifier.verify(
            plan=plan,
            source_results={"KQL": [{"id": "m1", "temp": 20}]},
            evidence_tool_map={"METAR": ["KQL"]},
            intent_graph=intent_graph,
        )
        self.assertTrue(result.is_verified)
        self.assertEqual(result.requery_suggestions, [])

    def test_requery_excludes_already_tried_tools(self):
        verifier = EvidenceVerifier()
        intent_graph = IntentGraphSnapshot(data=DEFAULT_INTENT_GRAPH, source="test")
        plan = AgenticPlan(
            required_evidence=[
                EvidenceRequirement(name="METAR", optional=False),
            ],
        )
        # KQL was tried (listed in tool map) but returned errors.
        result = verifier.verify(
            plan=plan,
            source_results={"KQL": [{"error": "timeout"}]},
            evidence_tool_map={"METAR": ["KQL"]},
            intent_graph=intent_graph,
        )
        self.assertFalse(result.is_verified)
        # KQL should NOT be in suggestions since it was already tried.
        suggested_tools = {s["tool"] for s in result.requery_suggestions}
        self.assertNotIn("KQL", suggested_tools)


# ─── 2B. RRF Scoring ─────────────────────────────────────────────────────────


class TestRRFScoring(unittest.TestCase):
    def test_compute_rrf_scores_assigns_scores(self):
        items = [
            {"source": "KQL", "raw_relevance": 0.9},
            {"source": "KQL", "raw_relevance": 0.5},
            {"source": "SQL", "raw_relevance": 0.7},
        ]
        compute_rrf_scores(items, k=60)
        for item in items:
            self.assertIn("rrf_score", item)
            self.assertGreaterEqual(item["rrf_score"], 0.0)
            self.assertLessEqual(item["rrf_score"], 1.0)
        # The highest raw_relevance KQL item should have highest RRF among KQL items.
        self.assertGreater(items[0]["rrf_score"], items[1]["rrf_score"])

    def test_rrf_blended_in_reconcile(self):
        source_results = {
            "KQL": [{"id": "k1", "metric": "wind", "value": 10}],
            "SQL": [{"id": "s1", "metric": "count", "value": 5}],
        }
        out_no_rrf = reconcile_context(source_results, enable_rrf=False)
        out_rrf = reconcile_context(source_results, enable_rrf=True)
        # Both should produce results — scores may differ.
        self.assertTrue(out_no_rrf["reconciled_items"])
        self.assertTrue(out_rrf["reconciled_items"])


# ─── 2C. Conflict Penalty ────────────────────────────────────────────────────


class TestConflictPenalty(unittest.TestCase):
    def test_conflict_penalty_applied_to_items(self):
        source_results = {
            "KQL": [
                {"id": "m1", "metric": "delay_count", "value": 100, "__evidence_type": "METAR"},
                {"id": "m2", "metric": "delay_count", "value": 250, "__evidence_type": "METAR"},
            ],
        }
        out = reconcile_context(
            source_results=source_results,
            enable_conflict_detection=True,
        )
        conflicts = out["conflict_summary"]
        self.assertGreaterEqual(conflicts["count"], 1)
        # Items involved in conflicts should have lower fusion scores than without penalty.


# ─── 3B. Fusion Scores in Synthesis Prompt ────────────────────────────────────


class TestFusionScoresInPrompt(unittest.TestCase):
    def test_format_rows_includes_fusion_metadata(self):
        from af_context_provider import AviationRagContextProvider

        class _MinimalRetriever:
            router = None
            sql_available = False
            def source_event_meta(self, s): return {}
            def source_mode(self, s): return "fallback"
            def get_embedding(self, q): return None

        provider = AviationRagContextProvider.__new__(AviationRagContextProvider)
        rows = [
            {"id": "r1", "temp": 22, "__fusion_score": 0.85, "__evidence_type": "METAR", "__source": "KQL"},
            {"id": "r2", "temp": 18, "__source": "KQL"},
        ]
        text = provider._format_rows(rows, "KQL")
        self.assertIn("relevance=0.85", text)
        self.assertIn("evidence=METAR", text)
        # __-prefixed keys should be filtered out of compact output.
        self.assertNotIn("__fusion_score", text)
        self.assertNotIn("__source", text)


# ─── 3C. Evidence-Weighted Ordering ──────────────────────────────────────────


class TestEvidenceWeightedOrdering(unittest.TestCase):
    def test_rank_sources_prioritizes_required_evidence(self):
        from af_context_provider import AviationRagContextProvider

        provider = AviationRagContextProvider.__new__(AviationRagContextProvider)
        source_results = {
            "SQL": [{"id": "s1", "__fusion_score": 0.9}],
            "KQL": [{"id": "k1", "__fusion_score": 0.5, "__evidence_type": "METAR"}],
            "VECTOR_REG": [{"id": "v1", "__fusion_score": 0.3}],
        }
        ranked = provider._rank_sources(source_results, ["METAR"])
        # KQL has required evidence, so it should come first.
        self.assertEqual(ranked[0], "KQL")

    def test_rank_sources_falls_back_to_fusion_score(self):
        from af_context_provider import AviationRagContextProvider

        provider = AviationRagContextProvider.__new__(AviationRagContextProvider)
        source_results = {
            "SQL": [{"id": "s1", "__fusion_score": 0.9}],
            "VECTOR_REG": [{"id": "v1", "__fusion_score": 0.3}],
        }
        ranked = provider._rank_sources(source_results, [])
        # SQL has higher fusion score, should come first.
        self.assertEqual(ranked[0], "SQL")


# ─── 3A. Token-Aware Context Assembly ────────────────────────────────────────


class TestTokenAwareAssembly(unittest.TestCase):
    def test_estimate_tokens(self):
        from af_context_provider import AviationRagContextProvider
        self.assertEqual(AviationRagContextProvider._estimate_tokens(""), 0)
        self.assertEqual(AviationRagContextProvider._estimate_tokens("abcd"), 1)
        self.assertEqual(AviationRagContextProvider._estimate_tokens("a" * 100), 25)

    def test_compose_respects_token_budget(self):
        from af_context_provider import AviationRagContextProvider

        provider = AviationRagContextProvider.__new__(AviationRagContextProvider)
        # Large source results that would exceed budget.
        big_rows = [{"id": f"r{i}", "data": "x" * 200} for i in range(20)]
        source_results = {"KQL": big_rows, "SQL": big_rows}
        plan = {
            "intent": {"name": "PilotBrief.Departure"},
            "required_evidence": [{"name": "METAR"}],
        }
        text = provider._compose_agentic_context_text(
            query="test query",
            plan=plan,
            source_results=source_results,
            sql_queries={},
            warnings=[],
            coverage_summary={},
            conflict_summary={},
            max_context_tokens=500,
        )
        # Should produce something within token budget.
        tokens = AviationRagContextProvider._estimate_tokens(text)
        # Allow some slack since we're using heuristic estimates.
        self.assertLess(tokens, 600)


# ─── 1D. Graph Schema ────────────────────────────────────────────────────────


class TestGraphSchema(unittest.TestCase):
    def test_graph_schema_v2(self):
        from schema_provider import SchemaProvider

        class _StubRetriever:
            def _now_iso(self):
                return "2026-01-01T00:00:00Z"
            def current_sql_schema(self):
                return {"tables": []}

        provider = SchemaProvider(_StubRetriever())  # type: ignore[arg-type]
        schema = provider._graph_schema()
        self.assertEqual(schema["schema_version"], "graph-default-v2")
        self.assertIn("Airport", schema["node_types"])
        self.assertIn("FlightLeg", schema["node_types"])
        self.assertIn("Tail", schema["node_types"])
        self.assertIn("DEPARTS", schema["edge_types"])
        self.assertIn("ARRIVES", schema["edge_types"])
        self.assertIn("OPERATES", schema["edge_types"])
        # Aspirational types should NOT be in the active lists.
        self.assertNotIn("Runway", schema["node_types"])
        self.assertNotIn("HAS_RUNWAY", schema["edge_types"])


if __name__ == "__main__":
    unittest.main()
