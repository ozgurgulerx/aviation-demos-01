"""
Edge-case and boundary-condition tests for retrieval pipeline improvements.
Tests malformed inputs, empty data, None values, deduplication, token budget
extremes, conflict penalties, RRF edge cases, BFS termination, requery limits.
"""

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from contracts.agentic_plan import AgenticPlan, EvidenceRequirement, ToolCall
from context_reconciler import (
    compute_rrf_scores,
    reconcile_context,
    detect_conflicts,
    compute_fusion_score,
    normalize_scores,
)
from evidence_verifier import EvidenceVerifier
from intent_graph_provider import IntentGraphSnapshot, DEFAULT_INTENT_GRAPH
from plan_executor import PlanExecutor


# ─── Mocks ───────────────────────────────────────────────────────────────────


class _DummyRetriever:
    def source_event_meta(self, source):
        return {"source": source, "endpoint_label": "test", "freshness": "test"}

    def source_mode(self, source):
        return "live"

    def query_graph(self, query, hops=2):
        return [], []

    def query_nosql(self, query):
        return [{"id": "n1"}], []

    def get_embedding(self, query):
        return None

    def _heuristic_sql_fallback(self, _q, _d):
        return None

    def execute_sql_query(self, _sql):
        return [], []


class _GraphRetrieverWithMalformedRows(_DummyRetriever):
    """Returns rows with various malformed data for entity enrichment testing."""

    def query_graph(self, query, hops=2):
        rows = [
            # Normal row
            {"src_type": "Airport", "src_id": "KJFK", "edge_type": "DEPARTS", "dst_type": "FlightLeg", "dst_id": "FL100"},
            # Row with error key — should be skipped
            {"src_type": "Airport", "src_id": "SKIP1", "error": "some_error", "dst_type": "Airport", "dst_id": "SKIP2"},
            # Row with error_code — should be skipped
            {"src_type": "Airport", "src_id": "SKIP3", "error_code": "fail", "dst_type": "Airport", "dst_id": "SKIP4"},
            # Row with None src_id — should be skipped (empty after str())
            {"src_type": "Airport", "src_id": None, "edge_type": "X", "dst_type": "Airport", "dst_id": "KLAX"},
            # Row with empty src_id string — should be skipped
            {"src_type": "Airport", "src_id": "", "edge_type": "X", "dst_type": "FlightLeg", "dst_id": "FL200"},
            # Row with unknown node type — should be skipped
            {"src_type": "UnknownType", "src_id": "UNK1", "edge_type": "X", "dst_type": "AlsoUnknown", "dst_id": "UNK2"},
            # Not a dict — should be skipped
            "not_a_dict",
            # None — should be skipped (not isinstance dict)
            None,
            # Duplicate of first row — should not add duplicate entities
            {"src_type": "Airport", "src_id": "KJFK", "edge_type": "DEPARTS", "dst_type": "FlightLeg", "dst_id": "FL100"},
        ]
        return rows, []


class _GraphRetrieverWithMissingEntityKey(_DummyRetriever):
    """Returns GRAPH rows but plan.entities starts without the expected keys."""

    def query_graph(self, query, hops=2):
        return [
            {"src_type": "Airport", "src_id": "KATL", "edge_type": "DEPARTS", "dst_type": "FlightLeg", "dst_id": "FL300"},
        ], []


# ═══════════════════════════════════════════════════════════════════════════════
# 1A. Entity Propagation Edge Cases
# ═══════════════════════════════════════════════════════════════════════════════


class TestEntityPropagationEdgeCases(unittest.TestCase):
    def test_malformed_rows_skipped_gracefully(self):
        """Error rows, None rows, non-dict rows, and empty IDs should all be skipped."""
        retriever = _GraphRetrieverWithMalformedRows()
        executor = PlanExecutor(retriever)
        plan = AgenticPlan(
            entities={"airports": [], "flight_ids": [], "routes": [], "stations": [], "alternates": []},
            tool_calls=[ToolCall(id="call_1", tool="GRAPH", operation="entity_expansion", params={"hops": 2})],
        )
        executor.execute(user_query="test", plan=plan, schemas={})

        # Only valid entities should be added
        self.assertIn("KJFK", plan.entities["airports"])
        self.assertIn("KLAX", plan.entities["airports"])
        self.assertIn("FL100", plan.entities["flight_ids"])
        self.assertIn("FL200", plan.entities["flight_ids"])

        # Error rows should NOT contribute entities
        self.assertNotIn("SKIP1", plan.entities["airports"])
        self.assertNotIn("SKIP2", plan.entities["airports"])
        self.assertNotIn("SKIP3", plan.entities["airports"])
        self.assertNotIn("SKIP4", plan.entities["airports"])

        # Unknown types should NOT contribute
        self.assertNotIn("UNK1", plan.entities.get("airports", []))
        self.assertNotIn("UNK2", plan.entities.get("airports", []))

        # No duplicates
        self.assertEqual(plan.entities["airports"].count("KJFK"), 1)
        self.assertEqual(plan.entities["flight_ids"].count("FL100"), 1)

    def test_missing_entity_key_in_plan_creates_list(self):
        """If plan.entities doesn't have a key, _enrich should create it."""
        retriever = _GraphRetrieverWithMissingEntityKey()
        executor = PlanExecutor(retriever)
        # Start with entities missing 'airports' key entirely
        plan = AgenticPlan(
            entities={"flight_ids": [], "routes": []},
            tool_calls=[ToolCall(id="call_1", tool="GRAPH", operation="entity_expansion", params={"hops": 2})],
        )
        executor.execute(user_query="test", plan=plan, schemas={})
        self.assertIn("KATL", plan.entities.get("airports", []))
        self.assertIn("FL300", plan.entities.get("flight_ids", []))

    def test_entities_not_list_gets_replaced(self):
        """If plan.entities has a key with non-list value, it gets replaced."""
        retriever = _GraphRetrieverWithMissingEntityKey()
        executor = PlanExecutor(retriever)
        plan = AgenticPlan(
            entities={"airports": "bad_value", "flight_ids": None, "routes": []},
            tool_calls=[ToolCall(id="call_1", tool="GRAPH", operation="entity_expansion", params={"hops": 2})],
        )
        executor.execute(user_query="test", plan=plan, schemas={})
        self.assertIsInstance(plan.entities["airports"], list)
        self.assertIn("KATL", plan.entities["airports"])

    def test_non_entity_expansion_graph_call_skipped(self):
        """GRAPH calls with non-entity_expansion operation should not trigger enrichment."""
        retriever = _GraphRetrieverWithMissingEntityKey()
        executor = PlanExecutor(retriever)
        plan = AgenticPlan(
            entities={"airports": [], "flight_ids": [], "routes": [], "stations": [], "alternates": []},
            tool_calls=[ToolCall(id="call_1", tool="GRAPH", operation="lookup", params={"hops": 2})],
        )
        executor.execute(user_query="test", plan=plan, schemas={})
        # lookup operation should NOT trigger enrichment
        self.assertEqual(plan.entities["airports"], [])

    def test_empty_graph_results(self):
        """Empty GRAPH results should not crash or modify entities."""
        retriever = _DummyRetriever()  # query_graph returns ([], [])
        executor = PlanExecutor(retriever)
        plan = AgenticPlan(
            entities={"airports": ["KJFK"], "flight_ids": [], "routes": [], "stations": [], "alternates": []},
            tool_calls=[ToolCall(id="call_1", tool="GRAPH", operation="entity_expansion", params={"hops": 2})],
        )
        executor.execute(user_query="test", plan=plan, schemas={})
        self.assertEqual(plan.entities["airports"], ["KJFK"])


# ═══════════════════════════════════════════════════════════════════════════════
# 1B. Expansion Rules Edge Cases
# ═══════════════════════════════════════════════════════════════════════════════


class TestExpansionRulesEdgeCases(unittest.TestCase):
    def test_expansion_rules_with_none_values(self):
        """Rules list with None entries should be filtered out."""
        data = dict(DEFAULT_INTENT_GRAPH)
        data["expansion_rules"] = [
            None,
            {"intent": "PilotBrief.Departure", "tool": "GRAPH", "reason": "test"},
            "not_a_dict",
            42,
        ]
        snap = IntentGraphSnapshot(data=data, source="test")
        rules = snap.expansion_rules_for_intent("PilotBrief.Departure")
        self.assertEqual(len(rules), 1)
        self.assertEqual(rules[0]["tool"], "GRAPH")

    def test_expansion_rules_returns_copies_not_references(self):
        """Returned rules should be copies so mutations don't affect the original data."""
        snap = IntentGraphSnapshot(data=DEFAULT_INTENT_GRAPH, source="test")
        rules = snap.expansion_rules_for_intent("PilotBrief.Departure")
        if rules:
            rules[0]["mutated"] = True
        # Reload and check it's not mutated
        rules2 = snap.expansion_rules_for_intent("PilotBrief.Departure")
        if rules2:
            self.assertNotIn("mutated", rules2[0])

    def test_multiple_rules_for_same_intent(self):
        """Multiple rules for same intent should all be returned."""
        data = dict(DEFAULT_INTENT_GRAPH)
        data["expansion_rules"] = [
            {"intent": "PilotBrief.Departure", "tool": "GRAPH", "reason": "reason1"},
            {"intent": "PilotBrief.Departure", "tool": "KQL", "reason": "reason2"},
            {"intent": "PilotBrief.Arrival", "tool": "GRAPH", "reason": "reason3"},
        ]
        snap = IntentGraphSnapshot(data=data, source="test")
        rules = snap.expansion_rules_for_intent("PilotBrief.Departure")
        self.assertEqual(len(rules), 2)


# ═══════════════════════════════════════════════════════════════════════════════
# 2A. Evidence Re-Query Edge Cases
# ═══════════════════════════════════════════════════════════════════════════════


class TestEvidenceReQueryEdgeCases(unittest.TestCase):
    def test_no_intent_graph_produces_no_suggestions(self):
        """When intent_graph is None, no suggestions should be generated."""
        verifier = EvidenceVerifier()
        plan = AgenticPlan(
            required_evidence=[EvidenceRequirement(name="METAR", optional=False)],
        )
        result = verifier.verify(
            plan=plan,
            source_results={},
            evidence_tool_map={"METAR": []},
            intent_graph=None,
        )
        self.assertFalse(result.is_verified)
        self.assertEqual(result.requery_suggestions, [])

    def test_optional_missing_evidence_does_not_generate_suggestions(self):
        """Optional evidence that is missing should not trigger requery."""
        verifier = EvidenceVerifier()
        intent_graph = IntentGraphSnapshot(data=DEFAULT_INTENT_GRAPH, source="test")
        plan = AgenticPlan(
            required_evidence=[EvidenceRequirement(name="Hazards", optional=True)],
        )
        result = verifier.verify(
            plan=plan,
            source_results={},
            evidence_tool_map={"Hazards": []},
            intent_graph=intent_graph,
        )
        # Optional evidence missing should not create suggestions
        self.assertEqual(result.requery_suggestions, [])

    def test_all_tools_tried_produces_no_suggestions(self):
        """When all authoritative tools were already tried, no suggestions remain."""
        verifier = EvidenceVerifier()
        intent_graph = IntentGraphSnapshot(data=DEFAULT_INTENT_GRAPH, source="test")
        plan = AgenticPlan(
            required_evidence=[EvidenceRequirement(name="METAR", optional=False)],
        )
        # KQL is the authoritative tool for METAR, and it was already tried
        result = verifier.verify(
            plan=plan,
            source_results={"KQL": [{"error": "timeout"}]},
            evidence_tool_map={"METAR": ["KQL"]},
            intent_graph=intent_graph,
        )
        self.assertFalse(result.is_verified)
        suggested_tools = {s["tool"] for s in result.requery_suggestions}
        self.assertNotIn("KQL", suggested_tools)

    def test_mixed_valid_and_error_rows_counts_as_valid(self):
        """If even one row is not an error, evidence counts as present."""
        verifier = EvidenceVerifier()
        intent_graph = IntentGraphSnapshot(data=DEFAULT_INTENT_GRAPH, source="test")
        plan = AgenticPlan(
            required_evidence=[EvidenceRequirement(name="METAR", optional=False)],
        )
        result = verifier.verify(
            plan=plan,
            source_results={"KQL": [{"error": "timeout"}, {"id": "m1", "temp": 22}]},
            evidence_tool_map={"METAR": ["KQL"]},
            intent_graph=intent_graph,
        )
        self.assertTrue(result.is_verified)

    def test_empty_evidence_name(self):
        """Evidence with empty name should not crash."""
        verifier = EvidenceVerifier()
        plan = AgenticPlan(
            required_evidence=[EvidenceRequirement(name="", optional=False)],
        )
        result = verifier.verify(
            plan=plan,
            source_results={},
            evidence_tool_map={"": []},
            intent_graph=None,
        )
        self.assertFalse(result.is_verified)

    def test_no_required_evidence(self):
        """Plan with no required evidence should produce empty coverage."""
        verifier = EvidenceVerifier()
        plan = AgenticPlan(required_evidence=[])
        result = verifier.verify(
            plan=plan,
            source_results={},
            evidence_tool_map={},
            intent_graph=None,
        )
        self.assertFalse(result.is_verified)
        self.assertIn("No evidence coverage available from plan.", result.warnings)


# ═══════════════════════════════════════════════════════════════════════════════
# 2B. RRF Edge Cases
# ═══════════════════════════════════════════════════════════════════════════════


class TestRRFEdgeCases(unittest.TestCase):
    def test_rrf_single_item(self):
        """Single item should get rrf_score=1.0 after normalization."""
        items = [{"source": "KQL", "raw_relevance": 0.5}]
        compute_rrf_scores(items)
        self.assertAlmostEqual(items[0]["rrf_score"], 1.0)

    def test_rrf_empty_list(self):
        """Empty list should not crash."""
        items = []
        compute_rrf_scores(items)
        self.assertEqual(items, [])

    def test_rrf_all_zero_relevance(self):
        """All items with zero relevance should not cause division by zero."""
        items = [
            {"source": "KQL", "raw_relevance": 0.0},
            {"source": "SQL", "raw_relevance": 0.0},
        ]
        compute_rrf_scores(items)
        for item in items:
            self.assertGreaterEqual(item["rrf_score"], 0.0)
            self.assertLessEqual(item["rrf_score"], 1.0)

    def test_rrf_no_temporary_keys_leaked(self):
        """Internal _rrf_contributions key should be cleaned up."""
        items = [
            {"source": "KQL", "raw_relevance": 0.9},
            {"source": "SQL", "raw_relevance": 0.5},
        ]
        compute_rrf_scores(items)
        for item in items:
            self.assertNotIn("_rrf_contributions", item)

    def test_rrf_multiple_sources_ranking(self):
        """Items across multiple sources should be ranked independently within each."""
        items = [
            {"source": "KQL", "raw_relevance": 0.9},
            {"source": "KQL", "raw_relevance": 0.1},
            {"source": "SQL", "raw_relevance": 0.5},
            {"source": "SQL", "raw_relevance": 0.8},
        ]
        compute_rrf_scores(items)
        # Within KQL: item[0] (0.9) > item[1] (0.1)
        self.assertGreater(items[0]["rrf_score"], items[1]["rrf_score"])
        # Within SQL: item[3] (0.8) > item[2] (0.5)
        self.assertGreater(items[3]["rrf_score"], items[2]["rrf_score"])

    def test_rrf_blended_changes_fusion_score(self):
        """When RRF enabled, fusion scores should differ from non-RRF version."""
        source_results = {
            "KQL": [
                {"id": "k1", "metric": "wind", "value": 10, "@search.score": 0.9},
                {"id": "k2", "metric": "temp", "value": 20, "@search.score": 0.1},
            ],
            "SQL": [
                {"id": "s1", "metric": "count", "value": 5, "@search.score": 0.5},
            ],
        }
        out_no_rrf = reconcile_context(source_results, enable_rrf=False)
        out_rrf = reconcile_context(source_results, enable_rrf=True)
        # Scores should differ when RRF is blended in
        no_rrf_scores = {it["identifier"]: it["fusion_score"] for it in out_no_rrf["reconciled_items"]}
        rrf_scores = {it["identifier"]: it["fusion_score"] for it in out_rrf["reconciled_items"]}
        # At least one score should differ
        self.assertTrue(
            any(no_rrf_scores.get(k) != rrf_scores.get(k) for k in no_rrf_scores),
            "Expected at least one fusion score to differ with RRF enabled",
        )


# ═══════════════════════════════════════════════════════════════════════════════
# 2C. Conflict Penalty Edge Cases
# ═══════════════════════════════════════════════════════════════════════════════


class TestConflictPenaltyEdgeCases(unittest.TestCase):
    def test_no_conflicts_no_penalty(self):
        """Items with no conflicts should have conflict_penalty=0."""
        source_results = {
            "KQL": [{"id": "m1", "metric": "temp", "value": 22}],
        }
        out = reconcile_context(source_results, enable_conflict_detection=True)
        self.assertEqual(out["conflict_summary"]["count"], 0)

    def test_status_conflict_detected_and_penalized(self):
        """Contradictory statuses should create a conflict and penalize items."""
        # Both rows share the same id so detect_conflicts groups them together.
        # Different content fields prevent dedup from merging them.
        source_results = {
            "KQL": [
                {"id": "RWY09", "status": "open", "metric": "x", "value": 1, "content": "runway open"},
                {"id": "RWY09", "status": "closed", "metric": "y", "value": 2, "content": "runway closed"},
            ],
        }
        out = reconcile_context(source_results, enable_conflict_detection=True)
        conflicts = out["conflict_summary"]
        self.assertGreaterEqual(conflicts["count"], 1)
        # Check at least one conflict is a status conflict
        status_conflicts = [c for c in conflicts["items"] if c["type"] == "status"]
        self.assertTrue(status_conflicts)

    def test_numeric_conflict_below_threshold_no_penalty(self):
        """Numeric values within 25% ratio should NOT trigger a conflict."""
        source_results = {
            "KQL": [
                {"id": "m1", "metric": "delay_count", "value": 100},
                {"id": "m2", "metric": "delay_count", "value": 120},  # ratio=1.2 < 1.25
            ],
        }
        out = reconcile_context(source_results, enable_conflict_detection=True)
        delay_conflicts = [
            c for c in out["conflict_summary"]["items"]
            if c.get("signal") == "delay_count"
        ]
        self.assertEqual(len(delay_conflicts), 0)

    def test_high_severity_numeric_conflict(self):
        """Numeric values with ratio >= 2.0 should create high severity conflict."""
        source_results = {
            "KQL": [
                {"id": "m1", "metric": "delay_count", "value": 50},
                {"id": "m2", "metric": "delay_count", "value": 200},  # ratio=4.0
            ],
        }
        out = reconcile_context(source_results, enable_conflict_detection=True)
        self.assertEqual(out["conflict_summary"]["severity"], "high")

    def test_conflict_detection_disabled(self):
        """When disabled, no conflicts should be reported."""
        source_results = {
            "KQL": [
                {"id": "m1", "metric": "delay_count", "value": 50},
                {"id": "m2", "metric": "delay_count", "value": 500},
            ],
        }
        out = reconcile_context(source_results, enable_conflict_detection=False)
        self.assertEqual(out["conflict_summary"]["count"], 0)


# ═══════════════════════════════════════════════════════════════════════════════
# 3A. Token Budget Edge Cases
# ═══════════════════════════════════════════════════════════════════════════════


class TestTokenBudgetEdgeCases(unittest.TestCase):
    def _make_provider(self):
        from af_context_provider import AviationRagContextProvider
        return AviationRagContextProvider.__new__(AviationRagContextProvider)

    def test_zero_token_budget(self):
        """With zero budget, should still produce header+footer, just no source rows."""
        provider = self._make_provider()
        rows = [{"id": f"r{i}", "data": "x" * 100} for i in range(10)]
        text = provider._compose_agentic_context_text(
            query="test",
            plan={"intent": {"name": "Test"}, "required_evidence": []},
            source_results={"KQL": rows},
            sql_queries={},
            warnings=[],
            coverage_summary={},
            conflict_summary={},
            max_context_tokens=0,
        )
        # Should contain at least the header (query, route)
        self.assertIn("User query: test", text)

    def test_very_large_budget(self):
        """With huge budget, all rows should be included."""
        provider = self._make_provider()
        rows = [{"id": f"r{i}", "data": "abc"} for i in range(5)]
        text = provider._compose_agentic_context_text(
            query="test",
            plan={"intent": {"name": "Test"}, "required_evidence": []},
            source_results={"KQL": rows},
            sql_queries={},
            warnings=[],
            coverage_summary={},
            conflict_summary={},
            max_context_tokens=100000,
        )
        self.assertIn("KQL results:", text)

    def test_empty_source_results(self):
        """Empty source results should not crash."""
        provider = self._make_provider()
        text = provider._compose_agentic_context_text(
            query="test",
            plan={"intent": {"name": "Test"}, "required_evidence": []},
            source_results={},
            sql_queries={},
            warnings=[],
            coverage_summary={},
            conflict_summary={},
        )
        self.assertIn("User query: test", text)

    def test_conflict_summary_in_output(self):
        """Conflict summary should appear when conflicts present."""
        provider = self._make_provider()
        text = provider._compose_agentic_context_text(
            query="test",
            plan={"intent": {"name": "Test"}, "required_evidence": []},
            source_results={},
            sql_queries={},
            warnings=["test warning"],
            coverage_summary={"required_total": 3, "required_filled": 1, "missing_required": ["METAR"]},
            conflict_summary={
                "count": 1,
                "severity": "medium",
                "items": [{"type": "numeric", "signal": "delay_count", "severity": "medium", "detail": "spread"}],
            },
        )
        self.assertIn("INSTRUCTION: State the conflict explicitly", text)
        self.assertIn("test warning", text)
        self.assertIn("missing_required", text)


# ═══════════════════════════════════════════════════════════════════════════════
# 3B. Format Rows Edge Cases
# ═══════════════════════════════════════════════════════════════════════════════


class TestFormatRowsEdgeCases(unittest.TestCase):
    def _make_provider(self):
        from af_context_provider import AviationRagContextProvider
        return AviationRagContextProvider.__new__(AviationRagContextProvider)

    def test_empty_rows(self):
        provider = self._make_provider()
        text = provider._format_rows([], "KQL")
        self.assertEqual(text, "No rows returned.")

    def test_vector_source_missing_content(self):
        provider = self._make_provider()
        rows = [{"id": "v1", "title": "Doc1"}]
        text = provider._format_rows(rows, "VECTOR_REG")
        self.assertIn("Doc1", text)

    def test_vector_source_with_fusion_metadata(self):
        provider = self._make_provider()
        rows = [{"id": "v1", "title": "Doc1", "__fusion_score": 0.72, "__evidence_type": "SOPClause", "content": "text here"}]
        text = provider._format_rows(rows, "VECTOR_REG")
        self.assertIn("relevance=0.72", text)
        self.assertIn("evidence=SOPClause", text)

    def test_max_rows_parameter(self):
        provider = self._make_provider()
        rows = [{"id": f"r{i}"} for i in range(20)]
        text = provider._format_rows(rows, "KQL", max_rows=3)
        # Should only have 3 numbered rows
        self.assertIn("1.", text)
        self.assertIn("2.", text)
        self.assertIn("3.", text)
        self.assertNotIn("4.", text)

    def test_no_metadata_when_no_fusion_score(self):
        provider = self._make_provider()
        rows = [{"id": "r1", "temp": 22}]
        text = provider._format_rows(rows, "KQL")
        # No brackets prefix when no metadata
        self.assertTrue(text.startswith("1. id=r1"))


# ═══════════════════════════════════════════════════════════════════════════════
# 3C. Rank Sources Edge Cases
# ═══════════════════════════════════════════════════════════════════════════════


class TestRankSourcesEdgeCases(unittest.TestCase):
    def _make_provider(self):
        from af_context_provider import AviationRagContextProvider
        return AviationRagContextProvider.__new__(AviationRagContextProvider)

    def test_empty_source_results(self):
        provider = self._make_provider()
        ranked = provider._rank_sources({}, [])
        self.assertEqual(ranked, [])

    def test_source_with_no_rows(self):
        provider = self._make_provider()
        ranked = provider._rank_sources({"KQL": [], "SQL": [{"id": "s1", "__fusion_score": 0.5}]}, [])
        # KQL is empty, SQL has data — SQL should rank first
        self.assertEqual(ranked[0], "SQL")

    def test_all_sources_have_required_evidence(self):
        """When multiple sources have required evidence, highest fusion wins."""
        provider = self._make_provider()
        source_results = {
            "KQL": [{"id": "k1", "__fusion_score": 0.5, "__evidence_type": "METAR"}],
            "SQL": [{"id": "s1", "__fusion_score": 0.9, "__evidence_type": "METAR"}],
        }
        ranked = provider._rank_sources(source_results, ["METAR"])
        # Both have required evidence; SQL has higher fusion score
        self.assertEqual(ranked[0], "SQL")

    def test_no_fusion_scores_in_rows(self):
        """Sources without __fusion_score should default to 0.0."""
        provider = self._make_provider()
        source_results = {
            "KQL": [{"id": "k1"}],
            "SQL": [{"id": "s1"}],
        }
        ranked = provider._rank_sources(source_results, [])
        # Both score (False, 0.0), alphabetical order is implementation-dependent
        self.assertEqual(len(ranked), 2)


# ═══════════════════════════════════════════════════════════════════════════════
# Graph Schema Edge Cases
# ═══════════════════════════════════════════════════════════════════════════════


class TestGraphSchemaEdgeCases(unittest.TestCase):
    def test_schema_cache_ttl(self):
        """Snapshot should be cached and returned without recomputation."""
        from schema_provider import SchemaProvider

        call_count = 0

        class _CountingRetriever:
            def _now_iso(self):
                nonlocal call_count
                call_count += 1
                return "2026-01-01T00:00:00Z"
            def current_sql_schema(self):
                return {"tables": []}

        provider = SchemaProvider(_CountingRetriever())
        provider.cache_ttl_seconds = 3600
        snap1 = provider.snapshot()
        snap2 = provider.snapshot()
        # Second call should use cache (call_count should be same as after first call)
        self.assertIs(snap1, snap2)


# ═══════════════════════════════════════════════════════════════════════════════
# Reconciliation Integration Edge Cases
# ═══════════════════════════════════════════════════════════════════════════════


class TestReconciliationIntegration(unittest.TestCase):
    def test_reconcile_empty_source_results(self):
        """Empty inputs should produce empty outputs without crashing."""
        out = reconcile_context({})
        self.assertEqual(out["reconciled_items"], [])
        self.assertEqual(out["source_results"], {})

    def test_reconcile_with_all_error_rows(self):
        """All error rows should still be processed (they get low scores)."""
        out = reconcile_context({
            "KQL": [{"error": "timeout", "error_code": "kql_timeout"}],
        })
        self.assertTrue(out["reconciled_items"])

    def test_per_source_limits_enforced(self):
        """Source limits should cap number of items per source."""
        rows = [{"id": f"r{i}", "metric": f"m{i}", "value": i} for i in range(20)]
        out = reconcile_context(
            {"KQL": rows},
            per_source_limits={"KQL": 3},
        )
        kql_rows = out["source_results"].get("KQL", [])
        self.assertLessEqual(len(kql_rows), 3)

    def test_reconcile_with_rrf_and_conflicts(self):
        """RRF and conflict detection should work together without crashing."""
        source_results = {
            "KQL": [
                {"id": "m1", "metric": "delay_count", "value": 50},
                {"id": "m2", "metric": "delay_count", "value": 500},
            ],
            "SQL": [
                {"id": "s1", "metric": "ops_count", "value": 10},
            ],
        }
        out = reconcile_context(
            source_results,
            enable_rrf=True,
            enable_conflict_detection=True,
        )
        self.assertTrue(out["reconciled_items"])
        self.assertGreaterEqual(out["conflict_summary"]["count"], 1)


if __name__ == "__main__":
    unittest.main()
