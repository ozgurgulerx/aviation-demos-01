import sys
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tests"))

import unified_retriever as ur  # noqa: E402
from unified_retriever import UnifiedRetriever  # noqa: E402
from pg_mock import MockPool, patch_pg_pool  # noqa: E402


class SourceExecutionPolicyTests(unittest.TestCase):
    def _build_retriever(self) -> UnifiedRetriever:
        class _Writer:
            def __init__(self, sql: str = "SELECT id, title FROM asrs_reports LIMIT 1", should_raise: bool = False):
                self.sql = sql
                self.should_raise = should_raise

            def generate(self, *_args, **_kwargs):
                if self.should_raise:
                    raise RuntimeError("writer failure")
                return self.sql

        retriever = object.__new__(UnifiedRetriever)
        retriever.sql_backend = "postgres"
        retriever.sql_available = True
        retriever.sql_unavailable_reason = ""
        retriever.sql_dialect = "postgres"
        patch_pg_pool(retriever)
        retriever.search_clients = {}
        retriever._vector_k_param = "k_nearest_neighbors"
        retriever.vector_source_to_index = {
            "VECTOR_OPS": "idx_ops_narratives",
            "VECTOR_REG": "idx_regulatory",
            "VECTOR_AIRPORT": "idx_airport_ops_docs",
        }
        retriever.sql_writer = _Writer()
        retriever.sql_generator = _Writer()
        retriever.use_legacy_sql_generator = False
        return retriever

    def test_execute_sql_query_success(self):
        retriever = self._build_retriever()
        rows, citations = retriever.execute_sql_query("SELECT asrs_report_id, title FROM asrs_reports LIMIT 1")
        self.assertEqual(len(rows), 1)
        self.assertIsNotNone(rows[0].get("asrs_report_id"))
        self.assertEqual(len(citations), 1)

    def test_execute_sql_query_flags_missing_table(self):
        retriever = self._build_retriever()
        rows, _citations = retriever.execute_sql_query("SELECT * FROM missing_table")
        self.assertEqual(rows[0].get("error_code"), "sql_schema_missing")

    def test_query_sql_handles_writer_failures_without_crashing(self):
        retriever = self._build_retriever()
        retriever.sql_writer.should_raise = True
        retriever.sql_generator.should_raise = True

        rows, sql, _citations = retriever.query_sql("top facilities")

        self.assertEqual(sql, "")
        self.assertEqual(rows[0].get("error_code"), "sql_generation_failed")

    def test_query_sql_need_schema_uses_best_effort_fallback(self):
        retriever = self._build_retriever()
        retriever.sql_writer.sql = "-- NEED_SCHEMA: damage_score column in asrs_reports"
        rows, sql, _citations = retriever.query_sql(
            "Top 5 facilities by ASRS report count and average damage score."
        )
        self.assertIn("GROUP BY facility", sql)
        self.assertGreaterEqual(len(rows), 1)
        self.assertIsNone(rows[0].get("error_code"))
        self.assertIn("partial_schema", rows[0])

    def test_query_sql_need_schema_without_fallback_returns_schema_missing(self):
        retriever = self._build_retriever()
        retriever.sql_writer.sql = "-- NEED_SCHEMA: damage_score column in asrs_reports"
        # The heuristic fallback only fires for certain query patterns; use a query that won't match
        rows, sql, _citations = retriever.query_sql(
            "Describe the incidents with highest damage score."
        )
        self.assertEqual(sql, "-- NEED_SCHEMA: damage_score column in asrs_reports")
        self.assertEqual(rows[0].get("error_code"), "sql_schema_missing")

    def test_source_mode_blocks_sql_when_unavailable(self):
        retriever = self._build_retriever()
        retriever.sql_available = False
        retriever.sql_backend = "unavailable"
        retriever.sql_unavailable_reason = "db down"

        self.assertEqual(retriever.source_mode("SQL"), "blocked")
        rows, _citations, _sql = retriever.retrieve_source("SQL", "top facilities")
        self.assertEqual(rows[0].get("error_code"), "source_unavailable")

    def test_kql_blocked_without_endpoint(self):
        retriever = self._build_retriever()
        with patch.object(ur, "FABRIC_KQL_ENDPOINT", ""):
            rows, _citations = retriever.query_kql("opensky_states | take 1")
        self.assertEqual(rows[0].get("error_code"), "source_unavailable")

    def test_kql_natural_language_generates_csl_when_live(self):
        retriever = self._build_retriever()
        with patch.object(ur, "FABRIC_KQL_ENDPOINT", "https://demo.kusto.fabric.microsoft.com"):
            rows, _citations = retriever.query_kql("latest hazards for IST")
        # Natural language is auto-translated to KQL via airport extraction;
        # with a non-functional endpoint, the generated KQL fails at runtime.
        self.assertIn(rows[0].get("error_code"), {"kql_runtime_error", "kql_validation_failed"})

    def test_kql_blocks_unsafe_statement(self):
        retriever = self._build_retriever()
        with patch.object(ur, "FABRIC_KQL_ENDPOINT", "https://demo.kusto.fabric.microsoft.com"):
            rows, _citations = retriever.query_kql(".show database schema; drop table x")
        self.assertEqual(rows[0].get("error_code"), "kql_validation_failed")

    def test_kql_live_executes_provided_csl(self):
        retriever = self._build_retriever()
        seen = {}

        def fake_kusto_rows(_endpoint, csl):
            seen["csl"] = csl
            return [{"callsign": "THY123"}], None

        retriever._kusto_rows = fake_kusto_rows
        with patch.object(ur, "FABRIC_KQL_ENDPOINT", "https://demo.kusto.fabric.microsoft.com"):
            rows, citations = retriever.query_kql("opensky_states | where timestamp > ago(30m) | take 1")

        self.assertTrue(rows)
        self.assertEqual(rows[0]["callsign"], "THY123")
        self.assertIn("ago(30m)", seen["csl"])
        self.assertEqual(citations[0].source_type, "KQL")

    def test_graph_blocked_without_endpoint(self):
        retriever = self._build_retriever()
        with patch.object(ur, "FABRIC_GRAPH_ENDPOINT", ""):
            rows, _citations = retriever.query_graph("dependency paths")
        self.assertEqual(rows[0].get("error_code"), "source_unavailable")

    def test_nosql_blocked_without_endpoint(self):
        retriever = self._build_retriever()
        with patch.object(ur, "FABRIC_NOSQL_ENDPOINT", ""):
            rows, _citations = retriever.query_nosql("notam snapshot")
        self.assertEqual(rows[0].get("error_code"), "source_unavailable")

    def test_vector_source_blocked_without_search_client(self):
        retriever = self._build_retriever()
        self.assertEqual(retriever.source_mode("VECTOR_REG"), "blocked")

    def test_vector_query_unknown_source_uses_default_index_without_name_error(self):
        retriever = self._build_retriever()
        rows, _citations = retriever.query_semantic(
            "runway risk",
            top=1,
            embedding=[0.0] * 1536,
            source="UNKNOWN",
        )
        self.assertEqual(rows[0].get("error_code"), "source_unavailable")

    def test_vector_query_search_failure_returns_structured_error(self):
        retriever = self._build_retriever()

        class _BrokenClient:
            def search(self, **_kwargs):
                raise RuntimeError("search backend failure")

        retriever.search_clients = {"idx_ops_narratives": _BrokenClient()}
        rows, _citations = retriever.query_semantic(
            "runway risk",
            top=1,
            embedding=[0.0] * 1536,
            source="VECTOR_OPS",
        )
        self.assertEqual(rows[0].get("error_code"), "semantic_runtime_error")

    def test_unknown_source_returns_error(self):
        retriever = self._build_retriever()
        rows, _citations, _sql = retriever.retrieve_source("UNKNOWN", "x")
        self.assertIn("unknown_source", rows[0].get("error", ""))

    def test_extract_airports_from_city_query(self):
        retriever = self._build_retriever()
        airports = retriever._extract_airports_from_query("flight risk brief towards New York")
        self.assertIn("KJFK", airports)
        self.assertIn("KLGA", airports)
        self.assertIn("KEWR", airports)
        self.assertNotIn("RISK", airports)
        self.assertNotIn("YORK", airports)


if __name__ == "__main__":
    unittest.main()
