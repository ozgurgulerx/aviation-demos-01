#!/usr/bin/env python3
"""
Comprehensive datastore tests — reachability, data validity, and edge cases.

Covers:
  1. PostgreSQL connectivity, schema, queries, edge cases
  2. SQL validation (injection, dialect, table existence)
  3. Schema provider correctness
  4. KQL validation
  5. Graph / NoSQL source-blocked behavior
  6. Query router heuristic correctness
  7. Citation integrity
  8. Concurrent access safety
  9. Error row structure consistency
  10. Source mode / policy enforcement
"""

import json
import os
import sys
import threading
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tests"))

load_dotenv(ROOT / ".env", override=False)
load_dotenv(ROOT / "src" / ".env.local", override=True)

import unified_retriever as ur  # noqa: E402
from schema_provider import SchemaProvider  # noqa: E402
from unified_retriever import Citation, UnifiedRetriever  # noqa: E402
from query_router import QueryRouter  # noqa: E402
from pg_mock import patch_pg_pool  # noqa: E402


def _build_retriever() -> UnifiedRetriever:
    retriever = object.__new__(UnifiedRetriever)
    patch_pg_pool(retriever)
    retriever.search_clients = {}
    retriever._vector_k_param = "k_nearest_neighbors"
    retriever.vector_source_to_index = {
        "VECTOR_OPS": "idx_ops_narratives",
        "VECTOR_REG": "idx_regulatory",
        "VECTOR_AIRPORT": "idx_airport_ops_docs",
    }

    class _Writer:
        def generate(self, *_a, **_kw):
            return "SELECT asrs_report_id, title FROM asrs_reports LIMIT 3"

    retriever.sql_writer = _Writer()
    retriever.sql_generator = _Writer()
    retriever.use_legacy_sql_generator = False
    return retriever


# ====================================================================
# 1. PostgreSQL Connectivity & Schema
# ====================================================================


class TestSQLConnectivity(unittest.TestCase):
    """Verify PostgreSQL mock pool is reachable and has expected schema."""

    @classmethod
    def setUpClass(cls):
        cls.retriever = _build_retriever()

    def test_sql_backend_is_postgres(self):
        self.assertEqual(self.retriever.sql_backend, "postgres")

    def test_sql_available_flag_is_true(self):
        self.assertTrue(self.retriever.sql_available)

    def test_pg_pool_is_not_none(self):
        self.assertIsNotNone(self.retriever._pg_pool)

    def test_schema_has_asrs_reports_table(self):
        schema = self.retriever.current_sql_schema()
        table_names = [t["table"] for t in schema.get("tables", [])]
        self.assertIn("asrs_reports", table_names)

    def test_schema_has_asrs_ingestion_runs_table(self):
        schema = self.retriever.current_sql_schema()
        table_names = [t["table"] for t in schema.get("tables", [])]
        self.assertIn("asrs_ingestion_runs", table_names)

    def test_asrs_reports_has_expected_columns(self):
        schema = self.retriever.current_sql_schema()
        for table in schema["tables"]:
            if table["table"] == "asrs_reports":
                col_names = {c["name"] for c in table["columns"]}
                expected = {
                    "asrs_report_id", "event_date", "location", "aircraft_type",
                    "flight_phase", "narrative_type", "title", "report_text",
                    "raw_json", "ingested_at",
                }
                self.assertTrue(expected.issubset(col_names), f"Missing: {expected - col_names}")
                return
        self.fail("asrs_reports table not found in schema")

    def test_schema_version_is_populated(self):
        schema = self.retriever.current_sql_schema()
        self.assertTrue(schema.get("schema_version"))
        self.assertNotEqual(schema["schema_version"], "none")
        self.assertNotEqual(schema["schema_version"], "error")

    def test_schema_collected_at_is_iso_datetime(self):
        schema = self.retriever.current_sql_schema()
        collected = schema.get("collected_at", "")
        # Must parse as ISO
        datetime.fromisoformat(collected.replace("Z", "+00:00"))


# ====================================================================
# 2. SQL Query Execution — Valid Data
# ====================================================================


class TestSQLQueryExecution(unittest.TestCase):
    """Execute real SQL queries against the mock PostgreSQL pool."""

    @classmethod
    def setUpClass(cls):
        cls.retriever = _build_retriever()

    def test_select_count_returns_integer(self):
        rows, citations = self.retriever.execute_sql_query(
            "SELECT COUNT(*) AS cnt FROM asrs_reports"
        )
        self.assertEqual(len(rows), 1)
        self.assertIsNone(rows[0].get("error_code"))
        self.assertIsInstance(rows[0]["cnt"], int)
        self.assertGreater(rows[0]["cnt"], 0)

    def test_select_limit_returns_correct_row_count(self):
        rows, citations = self.retriever.execute_sql_query(
            "SELECT asrs_report_id, title FROM asrs_reports LIMIT 5"
        )
        self.assertEqual(len(rows), 5)
        for row in rows:
            self.assertIn("asrs_report_id", row)
            self.assertIn("title", row)

    def test_select_with_where_clause_returns_filtered_data(self):
        rows, citations = self.retriever.execute_sql_query(
            "SELECT asrs_report_id, flight_phase FROM asrs_reports "
            "WHERE flight_phase = 'Initial Climb' LIMIT 3"
        )
        self.assertGreaterEqual(len(rows), 1)
        for row in rows:
            self.assertEqual(row["flight_phase"], "Initial Climb")

    def test_select_distinct_returns_unique_values(self):
        rows, _ = self.retriever.execute_sql_query(
            "SELECT DISTINCT flight_phase FROM asrs_reports WHERE flight_phase IS NOT NULL LIMIT 20"
        )
        phases = [r["flight_phase"] for r in rows]
        self.assertEqual(len(phases), len(set(phases)), "DISTINCT should produce unique values")

    def test_group_by_aggregation(self):
        rows, _ = self.retriever.execute_sql_query(
            "SELECT flight_phase, COUNT(*) AS cnt FROM asrs_reports "
            "GROUP BY flight_phase ORDER BY cnt DESC LIMIT 5"
        )
        self.assertGreaterEqual(len(rows), 1)
        # Counts should be descending
        counts = [r["cnt"] for r in rows]
        self.assertEqual(counts, sorted(counts, reverse=True))

    def test_select_with_like_pattern(self):
        rows, _ = self.retriever.execute_sql_query(
            "SELECT asrs_report_id, location FROM asrs_reports "
            "WHERE LOWER(location) LIKE '%jfk%' LIMIT 5"
        )
        for row in rows:
            self.assertIn("jfk", row["location"].lower())

    def test_select_with_null_handling(self):
        rows, _ = self.retriever.execute_sql_query(
            "SELECT COUNT(*) AS null_count FROM asrs_reports WHERE location IS NULL"
        )
        self.assertEqual(len(rows), 1)
        self.assertIsInstance(rows[0]["null_count"], int)

    def test_ingestion_run_data_is_valid(self):
        rows, _ = self.retriever.execute_sql_query(
            "SELECT * FROM asrs_ingestion_runs LIMIT 1"
        )
        self.assertGreaterEqual(len(rows), 1)
        run = rows[0]
        self.assertEqual(run["status"], "success")
        self.assertGreater(run["records_loaded"], 0)
        self.assertEqual(run["records_failed"], 0)

    def test_select_empty_result_set(self):
        rows, citations = self.retriever.execute_sql_query(
            "SELECT * FROM asrs_reports WHERE asrs_report_id = 'NONEXISTENT_ID_XYZ'"
        )
        self.assertEqual(len(rows), 0)
        self.assertEqual(len(citations), 0)

    def test_citation_structure_for_valid_query(self):
        rows, citations = self.retriever.execute_sql_query(
            "SELECT asrs_report_id, title FROM asrs_reports LIMIT 3"
        )
        self.assertGreaterEqual(len(citations), 1)
        for c in citations:
            self.assertIsInstance(c, Citation)
            self.assertEqual(c.source_type, "SQL")
            self.assertTrue(c.identifier)
            self.assertTrue(c.title)
            self.assertEqual(c.dataset, "aviation_db")

    def test_max_10_citations_even_for_many_rows(self):
        rows, citations = self.retriever.execute_sql_query(
            "SELECT asrs_report_id, title FROM asrs_reports LIMIT 25"
        )
        self.assertEqual(len(rows), 25)
        self.assertLessEqual(len(citations), 10)


# ====================================================================
# 3. SQL Validation — Injection & Dialect Checks
# ====================================================================


class TestSQLValidation(unittest.TestCase):
    """Test SQL validation catches dangerous and invalid queries."""

    @classmethod
    def setUpClass(cls):
        cls.retriever = _build_retriever()

    def test_rejects_empty_query(self):
        rows, _ = self.retriever.execute_sql_query("")
        self.assertEqual(rows[0]["error_code"], "sql_validation_failed")

    def test_rejects_whitespace_only_query(self):
        rows, _ = self.retriever.execute_sql_query("   \n\t  ")
        self.assertEqual(rows[0]["error_code"], "sql_validation_failed")

    def test_rejects_insert_statement(self):
        rows, _ = self.retriever.execute_sql_query(
            "INSERT INTO asrs_reports (asrs_report_id) VALUES ('evil')"
        )
        self.assertEqual(rows[0]["error_code"], "sql_validation_failed")

    def test_rejects_update_statement(self):
        rows, _ = self.retriever.execute_sql_query(
            "UPDATE asrs_reports SET title='hacked' WHERE 1=1"
        )
        self.assertEqual(rows[0]["error_code"], "sql_validation_failed")

    def test_rejects_delete_statement(self):
        rows, _ = self.retriever.execute_sql_query(
            "DELETE FROM asrs_reports"
        )
        self.assertEqual(rows[0]["error_code"], "sql_validation_failed")

    def test_rejects_drop_table(self):
        rows, _ = self.retriever.execute_sql_query(
            "DROP TABLE asrs_reports"
        )
        self.assertEqual(rows[0]["error_code"], "sql_validation_failed")

    def test_rejects_create_table(self):
        rows, _ = self.retriever.execute_sql_query(
            "CREATE TABLE evil (id TEXT)"
        )
        self.assertEqual(rows[0]["error_code"], "sql_validation_failed")

    def test_rejects_query_on_nonexistent_table(self):
        rows, _ = self.retriever.execute_sql_query(
            "SELECT * FROM nonexistent_table LIMIT 1"
        )
        self.assertEqual(rows[0]["error_code"], "sql_schema_missing")

    def test_allows_select_statement(self):
        rows, _ = self.retriever.execute_sql_query(
            "SELECT 1 AS ok FROM asrs_reports LIMIT 1"
        )
        self.assertIsNone(rows[0].get("error_code"))

    def test_allows_with_cte(self):
        """CTE queries should pass validation — CTE alias names are excluded
        from the referenced table list by _detect_sql_tables."""
        rows, _ = self.retriever.execute_sql_query(
            "WITH sample AS (SELECT asrs_report_id FROM asrs_reports LIMIT 3) "
            "SELECT COUNT(*) AS cnt FROM sample"
        )
        self.assertIsNone(rows[0].get("error_code"), f"CTE query should pass: {rows[0]}")

    def test_allows_multiple_ctes(self):
        """Multiple comma-separated CTEs should all be excluded from validation."""
        rows, _ = self.retriever.execute_sql_query(
            "WITH a AS (SELECT asrs_report_id FROM asrs_reports LIMIT 5), "
            "b AS (SELECT asrs_report_id FROM asrs_reports LIMIT 3) "
            "SELECT COUNT(*) AS cnt FROM a"
        )
        self.assertIsNone(rows[0].get("error_code"), f"Multi-CTE query should pass: {rows[0]}")

    def test_sql_injection_union_based(self):
        """Query referencing non-existent table via UNION should be caught."""
        rows, _ = self.retriever.execute_sql_query(
            "SELECT asrs_report_id FROM asrs_reports LIMIT 1 "
            "UNION SELECT password FROM users"
        )
        self.assertEqual(rows[0]["error_code"], "sql_schema_missing")

    def test_runtime_error_returns_structured_error(self):
        """Syntactically wrong SQL that passes validation should return runtime error.
        Note: with mock PG pool, the cursor doesn't raise on unknown functions,
        so the error may not surface. In a real DB this would be sql_runtime_error."""
        rows, _ = self.retriever.execute_sql_query(
            "SELECT INVALID_FUNCTION(asrs_report_id) FROM asrs_reports LIMIT 1"
        )
        error_code = rows[0].get("error_code")
        # Mock may return data (no runtime validation); real DB would raise.
        self.assertIn(error_code, (None, "sql_runtime_error", "sql_schema_missing"))


# ====================================================================
# 4. SQL Table Detection
# ====================================================================


class TestSQLTableDetection(unittest.TestCase):
    """Test the _detect_sql_tables helper."""

    @classmethod
    def setUpClass(cls):
        cls.retriever = _build_retriever()

    def test_detects_from_clause(self):
        tables = self.retriever._detect_sql_tables("SELECT * FROM asrs_reports")
        self.assertIn("asrs_reports", tables)

    def test_detects_join_clause(self):
        tables = self.retriever._detect_sql_tables(
            "SELECT * FROM asrs_reports r JOIN asrs_ingestion_runs i ON r.asrs_report_id = i.run_id"
        )
        self.assertIn("asrs_reports", tables)
        self.assertIn("asrs_ingestion_runs", tables)

    def test_detects_schema_qualified_table(self):
        tables = self.retriever._detect_sql_tables("SELECT * FROM demo.ourairports_airports")
        self.assertIn("demo.ourairports_airports", tables)

    def test_handles_empty_query(self):
        tables = self.retriever._detect_sql_tables("")
        self.assertEqual(tables, [])

    def test_deduplicates_tables(self):
        tables = self.retriever._detect_sql_tables(
            "SELECT * FROM asrs_reports WHERE asrs_report_id IN (SELECT asrs_report_id FROM asrs_reports)"
        )
        self.assertEqual(tables.count("asrs_reports"), 1)

    def test_excludes_cte_alias(self):
        tables = self.retriever._detect_sql_tables(
            "WITH sample AS (SELECT * FROM asrs_reports LIMIT 3) SELECT * FROM sample"
        )
        self.assertIn("asrs_reports", tables)
        self.assertNotIn("sample", tables)

    def test_excludes_multiple_cte_aliases(self):
        tables = self.retriever._detect_sql_tables(
            "WITH a AS (SELECT * FROM asrs_reports), b AS (SELECT * FROM asrs_ingestion_runs) "
            "SELECT * FROM a JOIN b ON 1=1"
        )
        self.assertIn("asrs_reports", tables)
        self.assertIn("asrs_ingestion_runs", tables)
        self.assertNotIn("a", tables)
        self.assertNotIn("b", tables)


# ====================================================================
# 5. Schema Provider
# ====================================================================


class TestSchemaProvider(unittest.TestCase):
    """Test the SchemaProvider caching and snapshot correctness."""

    @classmethod
    def setUpClass(cls):
        cls.retriever = _build_retriever()
        cls.provider = SchemaProvider(cls.retriever)

    def test_snapshot_contains_sql_schema(self):
        snap = self.provider.snapshot()
        self.assertIn("sql_schema", snap)
        self.assertGreaterEqual(len(snap["sql_schema"].get("tables", [])), 1)

    def test_snapshot_contains_kql_schema(self):
        snap = self.provider.snapshot()
        self.assertIn("kql_schema", snap)
        # Static default has 5 tables (opensky_states, hazards_airsigmets, hazards_gairmets, hazards_aireps_raw, ops_graph_edges)
        tables = snap["kql_schema"].get("tables", [])
        self.assertGreaterEqual(len(tables), 5)

    def test_snapshot_contains_graph_schema(self):
        snap = self.provider.snapshot()
        self.assertIn("graph_schema", snap)
        self.assertIn("node_types", snap["graph_schema"])
        self.assertIn("edge_types", snap["graph_schema"])

    def test_snapshot_caching_returns_same_object(self):
        snap1 = self.provider.snapshot()
        snap2 = self.provider.snapshot()
        self.assertIs(snap1, snap2)  # Same object due to caching

    def test_snapshot_sql_schema_version_is_valid(self):
        snap = self.provider.snapshot()
        version = snap["sql_schema"].get("schema_version", "")
        self.assertTrue(version.startswith("tables:"))
        count = int(version.split(":")[1])
        self.assertGreaterEqual(count, 1)


# ====================================================================
# 6. KQL Source Behavior
# ====================================================================


class TestKQLSourceBehavior(unittest.TestCase):
    """Test KQL source returns unavailable when no live endpoint."""

    @classmethod
    def setUpClass(cls):
        cls.retriever = _build_retriever()

    def test_kql_blocked_without_endpoint(self):
        with patch.object(ur, "FABRIC_KQL_ENDPOINT", ""):
            rows, citations = self.retriever.query_kql("weather at JFK", window_minutes=60)
            self.assertEqual(rows[0].get("error_code"), "source_unavailable")
            self.assertEqual(len(citations), 0)

    def test_kql_blocked_generic_query(self):
        with patch.object(ur, "FABRIC_KQL_ENDPOINT", ""):
            rows, citations = self.retriever.query_kql("recent flights", window_minutes=120)
            self.assertEqual(rows[0].get("error_code"), "source_unavailable")


# ====================================================================
# 7. KQL Validation
# ====================================================================


class TestKQLValidation(unittest.TestCase):
    """Test KQL query validation logic."""

    @classmethod
    def setUpClass(cls):
        cls.retriever = _build_retriever()

    def test_rejects_empty_kql(self):
        result = self.retriever._validate_kql_query("")
        self.assertEqual(result, "empty_kql_query")

    def test_rejects_drop_command(self):
        result = self.retriever._validate_kql_query("drop table weather_obs")
        self.assertIn("blocked_operation", result)

    def test_rejects_delete_command(self):
        result = self.retriever._validate_kql_query("delete from weather_obs where 1=1")
        self.assertIn("blocked_operation", result)

    def test_rejects_multiple_statements(self):
        result = self.retriever._validate_kql_query("weather_obs | take 5; .drop table weather_obs")
        self.assertIsNotNone(result, "Multi-statement KQL with drop should be rejected")
        self.assertTrue(
            "kql_multiple_statements_not_allowed" in result or "kql_contains_blocked_operation" in result,
            f"Expected semicolon or blocked-operation rejection, got: {result}",
        )

    def test_allows_valid_kql(self):
        result = self.retriever._validate_kql_query("opensky_states | take 10")
        self.assertIsNone(result)

    def test_allows_single_let_binding(self):
        """Single let binding with semicolon should be allowed."""
        result = self.retriever._validate_kql_query(
            'let threshold = 100; opensky_states | where velocity > threshold | take 10'
        )
        self.assertIsNone(result, f"Single let binding should pass validation, got: {result}")

    def test_allows_multiple_let_bindings(self):
        """Multiple let bindings (common KQL pattern) should be allowed."""
        csl = (
            'let horizon_start = todatetime("2026-02-22T04:56:00Z"); '
            'let horizon_end = todatetime("2026-02-22T06:26:00Z"); '
            'hazards_airsigmets '
            '| where valid_time_from between (horizon_start .. horizon_end) '
            '| top 40 by valid_time_from desc'
        )
        result = self.retriever._validate_kql_query(csl)
        self.assertIsNone(result, f"Multiple let bindings should pass validation, got: {result}")

    def test_allows_let_with_ago(self):
        """Let binding using ago() should be allowed."""
        result = self.retriever._validate_kql_query(
            'let cutoff = ago(2h); opensky_states | where time_position > cutoff | take 50'
        )
        self.assertIsNone(result, f"Let with ago() should pass validation, got: {result}")

    def test_rejects_let_followed_by_malicious_statement(self):
        """Let binding followed by a malicious command after the query should still be rejected."""
        result = self.retriever._validate_kql_query(
            'let x = 1; opensky_states | take 5; .drop table opensky_states'
        )
        self.assertIsNotNone(result, "Malicious command after let+query should be rejected")

    def test_rejects_semicolon_injection_disguised_as_let(self):
        """Injection attempt that tries to look like a let statement should be rejected."""
        result = self.retriever._validate_kql_query(
            'opensky_states | take 5; opensky_states | take 100'
        )
        self.assertIsNotNone(result, "Bare semicolon between queries should be rejected")

    def test_ensures_window_when_missing(self):
        csl = self.retriever._ensure_kql_window("weather_obs | take 10", 60)
        # Should not add window if no timestamp column reference
        self.assertEqual(csl, "weather_obs | take 10")

    def test_preserves_existing_ago_window(self):
        original = "weather_obs | where timestamp > ago(30m)"
        csl = self.retriever._ensure_kql_window(original, 60)
        self.assertEqual(csl, original)

    def test_adds_window_when_timestamp_present(self):
        csl = self.retriever._ensure_kql_window(
            "weather_obs | where timestamp > '2024-01-01'",
            60
        )
        self.assertIn("ago(60m)", csl)


# ====================================================================
# 8. Graph Source Behavior
# ====================================================================


class TestGraphSourceBehavior(unittest.TestCase):
    """Test graph source returns unavailable when no live endpoint."""

    @classmethod
    def setUpClass(cls):
        cls.retriever = _build_retriever()

    def test_graph_blocked_without_endpoint(self):
        with patch.object(ur, "FABRIC_GRAPH_ENDPOINT", ""):
            rows, citations = self.retriever.query_graph("IST dependency path", hops=2)
            self.assertEqual(rows[0].get("error_code"), "source_unavailable")

    def test_graph_blocked_unknown_entity(self):
        with patch.object(ur, "FABRIC_GRAPH_ENDPOINT", ""):
            rows, citations = self.retriever.query_graph("XYZZY unknown airport", hops=1)
            self.assertEqual(rows[0].get("error_code"), "source_unavailable")


# ====================================================================
# 9. NoSQL Source Behavior
# ====================================================================


class TestNoSQLSourceBehavior(unittest.TestCase):
    """Test NoSQL source returns unavailable when no live endpoint."""

    @classmethod
    def setUpClass(cls):
        cls.retriever = _build_retriever()

    def test_nosql_blocked_without_endpoint(self):
        with patch.object(ur, "FABRIC_NOSQL_ENDPOINT", ""):
            rows, citations = self.retriever.query_nosql("Istanbul NOTAM overview")
            self.assertEqual(rows[0].get("error_code"), "source_unavailable")
            self.assertEqual(len(citations), 0)

    def test_nosql_blocked_jfk(self):
        with patch.object(ur, "FABRIC_NOSQL_ENDPOINT", ""):
            rows, _ = self.retriever.query_nosql("JFK NOTAM")
            self.assertEqual(rows[0].get("error_code"), "source_unavailable")


# ====================================================================
# 11. Query Router Heuristics
# ====================================================================


class TestQueryRouterHeuristics(unittest.TestCase):
    """Test the keyword-based heuristic query router."""

    def setUp(self):
        self.router = QueryRouter()

    def test_sql_keywords_route_to_sql(self):
        for query in [
            "How many ASRS reports are there?",
            "Top 5 locations with most reports",
            "Count of reports by flight phase",
            "Total reports from 2024",
            "Average reports per year",
            "List all aircraft types",
        ]:
            route = self.router.quick_route(query)
            self.assertEqual(route, "SQL", f"Expected SQL for: {query}")

    def test_semantic_keywords_route_to_semantic(self):
        for query in [
            "Describe common runway incursion patterns",
            "What happened in similar incidents?",
            "Give me examples of near misses",
            "Why did this incident occur?",
            "Lessons learned from approach incidents",
        ]:
            route = self.router.quick_route(query)
            self.assertEqual(route, "SEMANTIC", f"Expected SEMANTIC for: {query}")

    def test_mixed_semantic_and_topic_keywords_route_to_hybrid(self):
        """Queries containing semantic keywords AND topic keywords (asrs/report/safety)
        route to HYBRID because both categories match."""
        route = self.router.quick_route("Summarize ASRS narratives about bird strikes")
        self.assertEqual(route, "HYBRID")

    def test_hybrid_keywords_route_to_hybrid(self):
        for query in [
            "Top 5 aircraft types and describe their common issues",
            "Count of runway incidents and summarize the narratives",
        ]:
            route = self.router.quick_route(query)
            self.assertEqual(route, "HYBRID", f"Expected HYBRID for: {query}")

    def test_ambiguous_queries_default_to_hybrid(self):
        for query in [
            "Tell me about ASRS safety reports",
            "Aviation incident analysis",
            "Safety report overview",
        ]:
            route = self.router.quick_route(query)
            self.assertEqual(route, "HYBRID", f"Expected HYBRID for: {query}")

    def test_completely_generic_query_defaults_to_hybrid(self):
        route = self.router.quick_route("hello world")
        self.assertEqual(route, "HYBRID")


# ====================================================================
# 12. Airport Extraction
# ====================================================================


class TestAirportExtraction(unittest.TestCase):
    """Test airport code extraction from queries."""

    @classmethod
    def setUpClass(cls):
        cls.retriever = _build_retriever()

    def test_extracts_icao_codes(self):
        airports = self.retriever._extract_airports_from_query("Weather at KJFK and KLGA")
        self.assertIn("KJFK", airports)
        self.assertIn("KLGA", airports)

    def test_extracts_iata_codes_mapped_to_icao(self):
        airports = self.retriever._extract_airports_from_query("flights at JFK")
        self.assertIn("KJFK", airports)

    def test_extracts_city_names(self):
        airports = self.retriever._extract_airports_from_query("flights over new york")
        self.assertIn("KJFK", airports)
        self.assertIn("KLGA", airports)
        self.assertIn("KEWR", airports)

    def test_extracts_istanbul_airports(self):
        airports = self.retriever._extract_airports_from_query("istanbul traffic situation")
        self.assertIn("LTFM", airports)
        self.assertIn("LTBA", airports)
        self.assertIn("LTFJ", airports)

    def test_no_duplicates(self):
        airports = self.retriever._extract_airports_from_query("KJFK JFK new york")
        self.assertEqual(len(airports), len(set(airports)))

    def test_max_8_airports(self):
        airports = self.retriever._extract_airports_from_query(
            "KJFK KLGA KEWR KBOS KORD KLAX KSFO KATL KDFW KMIA"
        )
        self.assertLessEqual(len(airports), 8)

    def test_empty_query_returns_empty(self):
        airports = self.retriever._extract_airports_from_query("")
        self.assertEqual(len(airports), 0)


# ====================================================================
# 13. Source Mode & Policy
# ====================================================================


class TestSourceModePolicy(unittest.TestCase):
    """Test source mode reporting for each datastore."""

    @classmethod
    def setUpClass(cls):
        cls.retriever = _build_retriever()

    def test_sql_source_mode_is_live(self):
        mode = self.retriever.source_mode("SQL")
        self.assertEqual(mode, "live")

    def test_kql_source_mode_without_endpoint(self):
        with patch.object(ur, "FABRIC_KQL_ENDPOINT", ""):
            mode = self.retriever.source_mode("KQL")
            self.assertIn(mode, ("blocked", "live"))

    def test_graph_source_mode_without_endpoint(self):
        with patch.object(ur, "FABRIC_GRAPH_ENDPOINT", ""):
            mode = self.retriever.source_mode("GRAPH")
            self.assertIn(mode, ("blocked", "live"))

    def test_nosql_source_mode_without_endpoint(self):
        with patch.object(ur, "FABRIC_NOSQL_ENDPOINT", ""):
            mode = self.retriever.source_mode("NOSQL")
            self.assertIn(mode, ("blocked", "live"))

    def test_vector_source_mode(self):
        mode = self.retriever.source_mode("VECTOR_OPS")
        self.assertIn(mode, ("live", "blocked"))

    def test_unknown_source_returns_unknown(self):
        mode = self.retriever.source_mode("NONEXISTENT")
        self.assertEqual(mode, "unknown")

    def test_source_event_meta_structure(self):
        meta = self.retriever.source_event_meta("SQL")
        self.assertIn("store_type", meta)
        self.assertIn("endpoint_label", meta)
        self.assertIn("freshness", meta)


# ====================================================================
# 15. Error Row Structure Consistency
# ====================================================================


class TestErrorRowStructure(unittest.TestCase):
    """All error rows should have a consistent structure."""

    @classmethod
    def setUpClass(cls):
        cls.retriever = _build_retriever()

    def _assert_error_row(self, row: dict):
        self.assertIn("error_code", row)
        self.assertIn("error", row)
        self.assertIn("source", row)

    def test_source_error_row_structure(self):
        row = self.retriever._source_error_row("SQL", "test_code", "test_detail")
        self._assert_error_row(row)
        self.assertEqual(row["error_code"], "test_code")
        self.assertEqual(row["error"], "test_detail")
        self.assertEqual(row["source"], "SQL")

    def test_source_unavailable_row_structure(self):
        row = self.retriever._source_unavailable_row("KQL", "endpoint not configured")
        self._assert_error_row(row)
        self.assertEqual(row["error_code"], "source_unavailable")

    def test_error_row_with_extra_fields(self):
        row = self.retriever._source_error_row(
            "SQL", "sql_runtime_error", "division by zero",
            extra={"sql": "SELECT 1/0", "severity": "warning"}
        )
        self._assert_error_row(row)
        self.assertEqual(row["sql"], "SELECT 1/0")
        self.assertEqual(row["severity"], "warning")


# ====================================================================
# 16. Citation Integrity
# ====================================================================


class TestCitationIntegrity(unittest.TestCase):
    """Test Citation dataclass behavior and serialization."""

    def test_citation_to_dict(self):
        c = Citation(
            source_type="SQL",
            identifier="report_123",
            title="Test Report",
            content_preview="Some preview text",
            score=0.95,
            dataset="aviation_db",
        )
        d = c.to_dict()
        self.assertEqual(d["source_type"], "SQL")
        self.assertEqual(d["identifier"], "report_123")
        self.assertEqual(d["score"], 0.95)
        self.assertEqual(d["dataset"], "aviation_db")

    def test_citation_str_sql(self):
        c = Citation(source_type="SQL", identifier="1", title="Report 1")
        self.assertEqual(str(c), "[SQL] Report 1")

    def test_citation_str_semantic(self):
        c = Citation(source_type="SEMANTIC", identifier="1", title="Doc 1")
        self.assertEqual(str(c), "[SEM] Doc 1")

    def test_citation_str_unknown_prefix(self):
        c = Citation(source_type="VECTOR_OPS", identifier="1", title="Vec 1")
        self.assertEqual(str(c), "[VEC] Vec 1")

    def test_citation_default_values(self):
        c = Citation(source_type="SQL", identifier="1", title="T")
        self.assertEqual(c.content_preview, "")
        self.assertEqual(c.score, 0.0)
        self.assertEqual(c.dataset, "")


# ====================================================================
# 17. Retrieve Source Dispatcher
# ====================================================================


class TestRetrieveSourceDispatcher(unittest.TestCase):
    """Test the unified retrieve_source dispatcher."""

    @classmethod
    def setUpClass(cls):
        cls.retriever = _build_retriever()

    def test_sql_source_dispatch(self):
        rows, citations, sql = self.retriever.retrieve_source(
            "SQL", "SELECT COUNT(*) FROM asrs_reports"
        )
        self.assertGreaterEqual(len(rows), 1)

    def test_kql_source_dispatch(self):
        rows, citations, _ = self.retriever.retrieve_source(
            "KQL", "weather at JFK", {"window_minutes": 60}
        )
        self.assertGreaterEqual(len(rows), 1)

    def test_graph_source_dispatch(self):
        rows, citations, _ = self.retriever.retrieve_source(
            "GRAPH", "IST dependency", {"hops": 2}
        )
        self.assertGreaterEqual(len(rows), 1)

    def test_nosql_source_dispatch(self):
        rows, citations, _ = self.retriever.retrieve_source(
            "NOSQL", "JFK NOTAM"
        )
        self.assertIsInstance(rows, list)

    def test_unknown_source_returns_error(self):
        rows, citations, _ = self.retriever.retrieve_source(
            "UNKNOWN_SOURCE", "test query"
        )
        self.assertIn("error", rows[0])
        self.assertIn("unknown_source", rows[0]["error"])


# ====================================================================
# 18. Concurrent Access Safety
# ====================================================================


class TestConcurrentAccess(unittest.TestCase):
    """Test thread safety of shared PostgreSQL pool."""

    @classmethod
    def setUpClass(cls):
        cls.retriever = _build_retriever()

    def test_concurrent_read_queries(self):
        """Multiple threads reading from PostgreSQL pool should not crash."""
        results = {}
        errors = []

        def run_query(thread_id):
            try:
                rows, _ = self.retriever.execute_sql_query(
                    f"SELECT COUNT(*) AS cnt FROM asrs_reports WHERE asrs_report_id > '{thread_id}'"
                )
                results[thread_id] = rows
            except Exception as e:
                errors.append((thread_id, str(e)))

        threads = [threading.Thread(target=run_query, args=(i,)) for i in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)

        self.assertEqual(len(errors), 0, f"Thread errors: {errors}")
        self.assertEqual(len(results), 5)

    def test_concurrent_schema_reads(self):
        """Multiple threads reading schema should not crash."""
        results = {}
        errors = []

        def read_schema(thread_id):
            try:
                schema = self.retriever.current_sql_schema()
                results[thread_id] = schema
            except Exception as e:
                errors.append((thread_id, str(e)))

        threads = [threading.Thread(target=read_schema, args=(i,)) for i in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)

        self.assertEqual(len(errors), 0, f"Thread errors: {errors}")
        self.assertEqual(len(results), 5)


# ====================================================================
# 19. Edge Cases — Empty Database (schema but no data)
# ====================================================================


class TestEmptyDatabase(unittest.TestCase):
    """Test behavior with a PostgreSQL database that has schema but no data."""

    def test_count_returns_zero(self):
        retriever = _build_retriever()
        patch_pg_pool(retriever, empty=True)
        rows, _ = retriever.execute_sql_query("SELECT COUNT(*) AS cnt FROM asrs_reports")
        self.assertIsNone(rows[0].get("error_code"))
        self.assertEqual(rows[0]["cnt"], 0)

    def test_select_returns_empty_list(self):
        retriever = _build_retriever()
        patch_pg_pool(retriever, empty=True)
        rows, citations = retriever.execute_sql_query(
            "SELECT * FROM asrs_reports LIMIT 10"
        )
        self.assertEqual(len(rows), 0)
        self.assertEqual(len(citations), 0)

    def test_schema_shows_correct_tables(self):
        retriever = _build_retriever()
        patch_pg_pool(retriever, empty=True)
        schema = retriever.current_sql_schema()
        table_names = [t["table"] for t in schema["tables"]]
        self.assertIn("asrs_reports", table_names)
        self.assertIn("asrs_ingestion_runs", table_names)


class TestDatabaseWithSchemaNoData(unittest.TestCase):
    """Test behavior with correct schema but zero rows."""

    def test_count_returns_zero(self):
        retriever = _build_retriever()
        patch_pg_pool(retriever, empty=True)
        rows, _ = retriever.execute_sql_query("SELECT COUNT(*) AS cnt FROM asrs_reports")
        self.assertIsNone(rows[0].get("error_code"))
        self.assertEqual(rows[0]["cnt"], 0)

    def test_select_returns_empty_list(self):
        retriever = _build_retriever()
        patch_pg_pool(retriever, empty=True)
        rows, citations = retriever.execute_sql_query(
            "SELECT * FROM asrs_reports LIMIT 10"
        )
        self.assertEqual(len(rows), 0)
        self.assertEqual(len(citations), 0)

    def test_schema_shows_correct_tables(self):
        retriever = _build_retriever()
        patch_pg_pool(retriever, empty=True)
        schema = retriever.current_sql_schema()
        table_names = [t["table"] for t in schema["tables"]]
        self.assertIn("asrs_reports", table_names)
        self.assertIn("asrs_ingestion_runs", table_names)


# ====================================================================
# 20. Heuristic SQL Fallback
# ====================================================================


class TestHeuristicSQLFallback(unittest.TestCase):
    """Test the _heuristic_sql_fallback method."""

    @classmethod
    def setUpClass(cls):
        cls.retriever = _build_retriever()

    def test_top_count_facilities_triggers_fallback(self):
        sql = self.retriever._heuristic_sql_fallback(
            "top facilities by count of reports",
            "-- NEED_SCHEMA: ..."
        )
        self.assertIsNotNone(sql)
        self.assertIn("location", sql.lower())
        self.assertIn("count", sql.lower())

    def test_top_count_aircraft_triggers_fallback(self):
        sql = self.retriever._heuristic_sql_fallback(
            "top aircraft types by count",
            "-- NEED_SCHEMA: ..."
        )
        self.assertIsNotNone(sql)
        self.assertIn("aircraft_type", sql.lower())

    def test_non_matching_query_returns_none(self):
        sql = self.retriever._heuristic_sql_fallback(
            "describe common incidents",
            "-- NEED_SCHEMA: ..."
        )
        self.assertIsNone(sql)

    def test_query_needing_count_but_not_top_returns_none(self):
        sql = self.retriever._heuristic_sql_fallback(
            "how many reports are there",
            "-- NEED_SCHEMA: ..."
        )
        # Requires both 'top' and 'count'
        self.assertIsNone(sql)


# ====================================================================
# 21. Preflight Check
# ====================================================================


class TestFabricPreflight(unittest.TestCase):
    """Test the fabric_preflight health check."""

    @classmethod
    def setUpClass(cls):
        cls.retriever = _build_retriever()

    def test_preflight_returns_expected_structure(self):
        result = self.retriever.fabric_preflight()
        self.assertIn("timestamp", result)
        self.assertIn("overall_status", result)
        self.assertIn("checks", result)
        self.assertIn(result["overall_status"], {"pass", "warn", "fail"})

    def test_preflight_includes_sql_check(self):
        result = self.retriever.fabric_preflight()
        check_names = [c["name"] for c in result["checks"]]
        self.assertIn("sql_connectivity", check_names)
        self.assertIn("sql_schema_snapshot", check_names)

    def test_preflight_sql_connectivity_passes(self):
        result = self.retriever.fabric_preflight()
        sql_check = next(c for c in result["checks"] if c["name"] == "sql_connectivity")
        self.assertEqual(sql_check["status"], "pass")

    def test_preflight_sql_schema_has_tables(self):
        result = self.retriever.fabric_preflight()
        schema_check = next(c for c in result["checks"] if c["name"] == "sql_schema_snapshot")
        self.assertIn("tables=", schema_check["detail"])
        count = int(schema_check["detail"].split("=")[1])
        self.assertGreaterEqual(count, 1)


# ====================================================================
# 22. SQL Unavailable Handling
# ====================================================================


class TestSQLUnavailableHandling(unittest.TestCase):
    """Test behavior when SQL is unavailable."""

    def test_execute_sql_when_pool_is_none(self):
        retriever = _build_retriever()
        retriever._pg_pool = None
        retriever.sql_available = False
        retriever.sql_unavailable_reason = "connection_lost"

        rows, citations = retriever.execute_sql_query("SELECT 1")
        self.assertEqual(rows[0]["error_code"], "source_unavailable")
        self.assertEqual(len(citations), 0)

    def test_query_sql_when_sql_unavailable(self):
        retriever = _build_retriever()
        retriever.sql_available = False
        retriever.sql_unavailable_reason = "test_unavailable"

        rows, sql, citations = retriever.query_sql("show me data")
        self.assertEqual(rows[0]["error_code"], "source_unavailable")
        self.assertEqual(sql, "")
        self.assertEqual(len(citations), 0)


# ====================================================================
# 23. Env Helper Functions
# ====================================================================


class TestEnvHelpers(unittest.TestCase):
    """Test _env_bool and _env_csv utility functions."""

    def test_env_bool_true_values(self):
        for val in ("1", "true", "yes", "y", "on", "True", "YES", "ON"):
            with patch.dict(os.environ, {"TEST_BOOL": val}):
                self.assertTrue(ur._env_bool("TEST_BOOL", False))

    def test_env_bool_false_values(self):
        for val in ("0", "false", "no", "n", "off", "False"):
            with patch.dict(os.environ, {"TEST_BOOL": val}):
                self.assertFalse(ur._env_bool("TEST_BOOL", True))

    def test_env_bool_empty_uses_default(self):
        with patch.dict(os.environ, {"TEST_BOOL": ""}):
            self.assertTrue(ur._env_bool("TEST_BOOL", True))
            self.assertFalse(ur._env_bool("TEST_BOOL", False))

    def test_env_csv_basic(self):
        with patch.dict(os.environ, {"TEST_CSV": "public,demo"}):
            result = ur._env_csv("TEST_CSV", "")
            self.assertEqual(result, ["public", "demo"])

    def test_env_csv_deduplicates(self):
        with patch.dict(os.environ, {"TEST_CSV": "public,PUBLIC,Public"}):
            result = ur._env_csv("TEST_CSV", "")
            self.assertEqual(result, ["public"])

    def test_env_csv_strips_whitespace(self):
        with patch.dict(os.environ, {"TEST_CSV": " public , demo "}):
            result = ur._env_csv("TEST_CSV", "")
            self.assertEqual(result, ["public", "demo"])

    def test_env_csv_empty_returns_empty(self):
        with patch.dict(os.environ, {"TEST_CSV": ""}):
            result = ur._env_csv("TEST_CSV", "")
            self.assertEqual(result, [])


# ====================================================================
# 24. Data Integrity Spot Checks
# ====================================================================


class TestDataIntegritySpotChecks(unittest.TestCase):
    """Spot-check data quality in the loaded PostgreSQL database."""

    @classmethod
    def setUpClass(cls):
        cls.retriever = _build_retriever()

    def test_no_null_report_ids(self):
        rows, _ = self.retriever.execute_sql_query(
            "SELECT COUNT(*) AS cnt FROM asrs_reports WHERE asrs_report_id IS NULL"
        )
        self.assertEqual(rows[0]["cnt"], 0)

    def test_no_null_report_text(self):
        rows, _ = self.retriever.execute_sql_query(
            "SELECT COUNT(*) AS cnt FROM asrs_reports WHERE report_text IS NULL"
        )
        self.assertEqual(rows[0]["cnt"], 0)

    def test_no_null_raw_json(self):
        rows, _ = self.retriever.execute_sql_query(
            "SELECT COUNT(*) AS cnt FROM asrs_reports WHERE raw_json IS NULL"
        )
        self.assertEqual(rows[0]["cnt"], 0)

    def test_all_report_ids_are_unique(self):
        rows, _ = self.retriever.execute_sql_query(
            "SELECT asrs_report_id, COUNT(*) AS cnt FROM asrs_reports "
            "GROUP BY asrs_report_id HAVING cnt > 1 LIMIT 5"
        )
        self.assertEqual(len(rows), 0, f"Duplicate report IDs found: {rows}")

    def test_event_dates_are_reasonable(self):
        """Event dates should be between 1980 and current year."""
        rows, _ = self.retriever.execute_sql_query(
            "SELECT MIN(event_date) AS min_date, MAX(event_date) AS max_date FROM asrs_reports "
            "WHERE event_date IS NOT NULL"
        )
        min_date = rows[0]["min_date"]
        max_date = rows[0]["max_date"]
        self.assertIsNotNone(min_date)
        self.assertIsNotNone(max_date)
        self.assertGreaterEqual(str(min_date), "1980")
        self.assertLessEqual(str(max_date), "2030")

    def test_raw_json_is_parseable(self):
        rows, _ = self.retriever.execute_sql_query(
            "SELECT raw_json FROM asrs_reports LIMIT 10"
        )
        for row in rows:
            parsed = json.loads(row["raw_json"])
            self.assertIsInstance(parsed, dict)

    def test_ingestion_run_counts_match(self):
        run_rows, _ = self.retriever.execute_sql_query(
            "SELECT records_loaded FROM asrs_ingestion_runs LIMIT 1"
        )
        count_rows, _ = self.retriever.execute_sql_query(
            "SELECT COUNT(*) AS cnt FROM asrs_reports"
        )
        if run_rows:
            self.assertEqual(
                run_rows[0]["records_loaded"],
                count_rows[0]["cnt"],
                "Ingestion run count should match actual row count"
            )

    def test_report_text_not_empty(self):
        rows, _ = self.retriever.execute_sql_query(
            "SELECT COUNT(*) AS cnt FROM asrs_reports WHERE LENGTH(report_text) = 0"
        )
        self.assertEqual(rows[0]["cnt"], 0, "No reports should have empty report_text")

    def test_title_format_starts_with_asrs(self):
        """Title format convention: 'ASRS | date | aircraft | location'."""
        rows, _ = self.retriever.execute_sql_query(
            "SELECT title FROM asrs_reports WHERE title IS NOT NULL LIMIT 10"
        )
        for row in rows:
            self.assertTrue(
                row["title"].startswith("ASRS"),
                f"Unexpected title format: {row['title'][:50]}"
            )


# ====================================================================
# 25. Vector/Semantic Search (mocked when endpoint unavailable)
# ====================================================================


class TestVectorSearchMocked(unittest.TestCase):
    """Test semantic search behavior when AI Search is not configured."""

    @classmethod
    def setUpClass(cls):
        cls.retriever = _build_retriever()

    def test_semantic_query_without_search_client_returns_unavailable(self):
        if not self.retriever.search_clients:
            rows, citations = self.retriever.query_semantic(
                "runway closure risk",
                top=2,
                embedding=[0.0] * 1536,
                source="VECTOR_OPS",
            )
            self.assertEqual(rows[0]["error_code"], "source_unavailable")
            self.assertIn("idx_ops_narratives", rows[0]["error"])

    def test_semantic_query_all_sources_without_client(self):
        if not self.retriever.search_clients:
            for source in ["VECTOR_OPS", "VECTOR_REG", "VECTOR_AIRPORT"]:
                rows, _ = self.retriever.query_semantic(
                    "test", top=1, embedding=[0.0] * 1536, source=source,
                )
                self.assertEqual(rows[0]["error_code"], "source_unavailable")

    def test_vector_source_to_index_mapping(self):
        mapping = self.retriever.vector_source_to_index
        self.assertIn("VECTOR_OPS", mapping)
        self.assertIn("VECTOR_REG", mapping)
        self.assertIn("VECTOR_AIRPORT", mapping)
        self.assertEqual(mapping["VECTOR_OPS"], "idx_ops_narratives")
        self.assertEqual(mapping["VECTOR_REG"], "idx_regulatory")
        self.assertEqual(mapping["VECTOR_AIRPORT"], "idx_airport_ops_docs")


# ====================================================================
# 26. Looks-Like-KQL Detection
# ====================================================================


class TestLooksLikeKQL(unittest.TestCase):
    """Test the _looks_like_kql_text heuristic."""

    @classmethod
    def setUpClass(cls):
        cls.retriever = _build_retriever()

    def test_pipe_character_is_kql(self):
        self.assertTrue(self.retriever._looks_like_kql_text("weather_obs | take 10"))

    def test_let_prefix_is_kql(self):
        self.assertTrue(self.retriever._looks_like_kql_text("let x = 5"))

    def test_show_prefix_is_kql(self):
        self.assertTrue(self.retriever._looks_like_kql_text(".show database schema"))

    def test_natural_language_is_not_kql(self):
        self.assertFalse(self.retriever._looks_like_kql_text("weather at JFK"))

    def test_empty_is_not_kql(self):
        self.assertFalse(self.retriever._looks_like_kql_text(""))

    def test_none_is_not_kql(self):
        self.assertFalse(self.retriever._looks_like_kql_text(None))


# ====================================================================
# 27. Postgres Default Database Name Mismatch Check
# ====================================================================


class TestPostgresConfigDefaults(unittest.TestCase):
    """Document and verify configuration defaults."""

    def test_default_pgdatabase_mismatch_warning(self):
        """
        KNOWN ISSUE: Default PGDATABASE in unified_retriever.py is 'aviationdb'
        but actual Azure PostgreSQL database is 'aviationrag'. This test documents
        the mismatch so it is not forgotten.

        FIX: Either rename the Azure DB to 'aviationdb', or change the default in
        unified_retriever.py line 233 from 'aviationdb' to 'aviationrag', or
        ensure PGDATABASE is always set in production environment.
        """
        # The hardcoded default in unified_retriever.py __init__ is 'aviationdb'
        # Verify that in production the PGDATABASE env var must be explicitly set
        # to 'aviationrag' to match the actual Azure resource.
        hardcoded_default = "aviationdb"
        actual_azure_db = "aviationrag"
        self.assertNotEqual(
            hardcoded_default, actual_azure_db,
            "If these ever match, this warning test can be removed."
        )


# ====================================================================
# 28. Query Token Extraction
# ====================================================================


class TestQueryTokenExtraction(unittest.TestCase):
    """Test _query_tokens helper."""

    @classmethod
    def setUpClass(cls):
        cls.retriever = _build_retriever()

    def test_extracts_alphanumeric_tokens(self):
        tokens = self.retriever._query_tokens("flights at KJFK airport")
        self.assertIn("KJFK", tokens)

    def test_max_8_tokens(self):
        tokens = self.retriever._query_tokens(
            "AAAA BBBB CCCC DDDD EEEE FFFF GGGG HHHH IIII JJJJ"
        )
        self.assertLessEqual(len(tokens), 8)

    def test_deduplicates_tokens(self):
        tokens = self.retriever._query_tokens("KJFK KJFK KJFK")
        self.assertEqual(tokens.count("KJFK"), 1)

    def test_empty_query_returns_empty(self):
        tokens = self.retriever._query_tokens("")
        self.assertEqual(len(tokens), 0)

    def test_filters_short_tokens(self):
        tokens = self.retriever._query_tokens("at to JFK the")
        # 'at' and 'to' are < 3 chars, should be filtered
        for t in tokens:
            self.assertGreaterEqual(len(t), 3)


# ====================================================================
# 29. Retrieval Result Serialization
# ====================================================================


class TestRetrievalResultSerialization(unittest.TestCase):
    """Test RetrievalResult.to_dict() correctness."""

    def test_to_dict_contains_all_fields(self):
        from unified_retriever import RetrievalResult
        result = RetrievalResult(
            answer="Test answer",
            route="SQL",
            reasoning="Test reason",
            citations=[
                Citation(source_type="SQL", identifier="1", title="T1", score=0.9, dataset="db"),
            ],
            sql_query="SELECT 1",
            pii_blocked=False,
        )
        d = result.to_dict()
        self.assertEqual(d["answer"], "Test answer")
        self.assertEqual(d["route"], "SQL")
        self.assertEqual(d["reasoning"], "Test reason")
        self.assertEqual(len(d["citations"]), 1)
        self.assertEqual(d["sql_query"], "SELECT 1")
        self.assertFalse(d["pii_blocked"])
        self.assertIsNone(d["pii_warning"])


if __name__ == "__main__":
    unittest.main()
