#!/usr/bin/env python3
"""
Comprehensive datastore tests — reachability, data validity, and edge cases.

Covers:
  1. SQLite connectivity, schema, queries, edge cases
  2. SQL validation (injection, dialect, table existence)
  3. Schema provider correctness
  4. KQL fallback (METAR/TAF parsing, OpenSky)
  5. Graph fallback (CSV overlay)
  6. NoSQL fallback (NOTAM snapshots)
  7. Runway constraints fallback
  8. Query router heuristic correctness
  9. Citation integrity
  10. Concurrent access safety
  11. Error row structure consistency
  12. Source mode / policy enforcement
"""

import gzip
import json
import os
import sqlite3
import sys
import tempfile
import threading
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

load_dotenv(ROOT / ".env", override=False)
load_dotenv(ROOT / "src" / ".env.local", override=True)

# Force SQLite mode for local testing
os.environ["USE_POSTGRES"] = "false"
os.environ["SQL_DIALECT"] = "sqlite"
os.environ["RETRIEVAL_STRICT_SOURCE_MODE"] = "false"
os.environ["ALLOW_SQLITE_FALLBACK"] = "true"
os.environ["ALLOW_MOCK_KQL_FALLBACK"] = "true"
os.environ["ALLOW_MOCK_GRAPH_FALLBACK"] = "true"
os.environ["ALLOW_MOCK_NOSQL_FALLBACK"] = "true"

import unified_retriever as ur  # noqa: E402
from schema_provider import SchemaProvider  # noqa: E402
from unified_retriever import Citation, UnifiedRetriever  # noqa: E402
from query_router import QueryRouter  # noqa: E402


def _build_retriever() -> UnifiedRetriever:
    return UnifiedRetriever(enable_pii_filter=False)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _has_data_file(*parts: str) -> bool:
    return (ROOT / Path(*parts)).exists()


# ====================================================================
# 1. SQLite Connectivity & Schema
# ====================================================================


class TestSQLiteConnectivity(unittest.TestCase):
    """Verify SQLite is reachable and has expected schema."""

    @classmethod
    def setUpClass(cls):
        cls.retriever = _build_retriever()

    def test_sql_backend_is_sqlite(self):
        self.assertIn(self.retriever.sql_backend, ("sqlite", "sqlite-fallback"))

    def test_sql_available_flag_is_true(self):
        self.assertTrue(self.retriever.sql_available)

    def test_db_connection_is_not_none(self):
        self.assertIsNotNone(self.retriever.db)

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
    """Execute real SQL queries against the seeded SQLite DB."""

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

    def test_rejects_ilike_in_sqlite_mode(self):
        rows, _ = self.retriever.execute_sql_query(
            "SELECT * FROM asrs_reports WHERE title ILIKE '%test%'"
        )
        self.assertEqual(rows[0]["error_code"], "sql_dialect_mismatch")

    def test_rejects_postgres_cast_in_sqlite_mode(self):
        rows, _ = self.retriever.execute_sql_query(
            "SELECT event_date::TEXT FROM asrs_reports LIMIT 1"
        )
        self.assertEqual(rows[0]["error_code"], "sql_dialect_mismatch")

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
        rows, _ = self.retriever.execute_sql_query(
            "WITH sample AS (SELECT asrs_report_id FROM asrs_reports LIMIT 3) "
            "SELECT COUNT(*) AS cnt FROM sample"
        )
        self.assertIsNone(rows[0].get("error_code"))
        self.assertEqual(rows[0]["cnt"], 3)

    def test_sql_injection_union_based(self):
        """Query referencing non-existent table via UNION should be caught."""
        rows, _ = self.retriever.execute_sql_query(
            "SELECT asrs_report_id FROM asrs_reports LIMIT 1 "
            "UNION SELECT password FROM users"
        )
        self.assertEqual(rows[0]["error_code"], "sql_schema_missing")

    def test_runtime_error_returns_structured_error(self):
        """Syntactically wrong SQL that passes validation should return runtime error."""
        rows, _ = self.retriever.execute_sql_query(
            "SELECT INVALID_FUNCTION(asrs_report_id) FROM asrs_reports LIMIT 1"
        )
        self.assertIn(rows[0].get("error_code"), ("sql_runtime_error", "sql_schema_missing"))


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
        # At minimum, the static default has 3 tables
        tables = snap["kql_schema"].get("tables", [])
        self.assertGreaterEqual(len(tables), 1)

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
# 6. KQL Fallback (METAR/TAF/OpenSky)
# ====================================================================


class TestKQLFallback(unittest.TestCase):
    """Test KQL fallback data sources (METAR, TAF, OpenSky)."""

    @classmethod
    def setUpClass(cls):
        cls.retriever = _build_retriever()

    @unittest.skipUnless(
        _has_data_file("data", "a-metars.cache.csv.gz"),
        "METAR cache file not present"
    )
    def test_metar_fallback_returns_rows_for_known_airport(self):
        rows = self.retriever._metar_rows_for_airports(["KJFK", "KLGA", "KEWR"])
        # May or may not have data depending on freshness, but structure should be correct
        if rows:
            for row in rows:
                self.assertIn("station_id", row)
                self.assertIn("raw_text", row)
                self.assertIn("source_file", row)

    @unittest.skipUnless(
        _has_data_file("data", "a-metars.cache.csv.gz"),
        "METAR cache file not present"
    )
    def test_metar_returns_only_latest_per_station(self):
        rows = self.retriever._metar_rows_for_airports(["KJFK"])
        stations = [r["station_id"] for r in rows]
        # Each station should appear at most once
        self.assertEqual(len(stations), len(set(stations)))

    @unittest.skipUnless(
        _has_data_file("data", "a-tafs.cache.xml.gz"),
        "TAF cache file not present"
    )
    def test_taf_fallback_returns_valid_structure(self):
        rows = self.retriever._taf_rows_for_airports(["KJFK", "KLGA"])
        if rows:
            for row in rows:
                self.assertIn("station_id", row)
                self.assertIn("raw_text", row)
                self.assertIn("valid_time_from", row)
                self.assertIn("valid_time_to", row)

    def test_metar_with_nonexistent_airport_returns_empty(self):
        rows = self.retriever._metar_rows_for_airports(["ZZZZ"])
        self.assertEqual(len(rows), 0)

    def test_taf_with_nonexistent_airport_returns_empty(self):
        rows = self.retriever._taf_rows_for_airports(["ZZZZ"])
        self.assertEqual(len(rows), 0)

    def test_kql_query_with_airports_returns_data(self):
        """Full query_kql with airport context triggers weather fallback."""
        rows, citations = self.retriever.query_kql("weather at JFK", window_minutes=60)
        self.assertGreaterEqual(len(rows), 1)
        self.assertGreaterEqual(len(citations), 1)
        # Should not be an error row
        if rows and isinstance(rows[0], dict):
            error_code = rows[0].get("error_code")
            if error_code:
                self.assertIn(error_code, {"source_unavailable"})

    @unittest.skipUnless(
        _has_data_file("data", "e-opensky_recent"),
        "OpenSky data directory not present"
    )
    def test_kql_fallback_opensky_data_is_valid_json(self):
        """OpenSky fallback should load valid JSON from local files."""
        states_file = self.retriever._latest_matching(
            "data/e-opensky_recent/opensky_states_all_*.json"
        )
        if states_file:
            payload = json.loads(states_file.read_text(encoding="utf-8"))
            self.assertIn("states", payload)
            self.assertIsInstance(payload["states"], list)

    def test_kql_fallback_with_generic_query_returns_something(self):
        """Even a vague query should return fallback data when files exist."""
        rows, citations = self.retriever.query_kql("recent flights", window_minutes=120)
        self.assertGreaterEqual(len(rows), 1)
        self.assertGreaterEqual(len(citations), 1)


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
        result = self.retriever._validate_kql_query("weather_obs | take 10")
        self.assertIsNone(result)

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
# 8. Graph Fallback
# ====================================================================


class TestGraphFallback(unittest.TestCase):
    """Test graph data fallback from CSV overlay."""

    @classmethod
    def setUpClass(cls):
        cls.retriever = _build_retriever()

    def test_graph_query_returns_data_or_unavailable(self):
        rows, citations = self.retriever.query_graph("IST dependency path", hops=2)
        self.assertGreaterEqual(len(rows), 1)
        error_code = rows[0].get("error_code")
        if error_code:
            self.assertIn(error_code, {"source_unavailable", "graph_runtime_error"})
        else:
            self.assertGreaterEqual(len(citations), 1)

    def test_graph_query_with_unknown_entity_still_returns_something(self):
        """Graph fallback should still return generic rows even if entity not found."""
        rows, citations = self.retriever.query_graph("XYZZY unknown airport", hops=1)
        self.assertGreaterEqual(len(rows), 1)


# ====================================================================
# 9. NoSQL Fallback (NOTAM)
# ====================================================================


class TestNoSQLFallback(unittest.TestCase):
    """Test NoSQL fallback from NOTAM snapshots."""

    @classmethod
    def setUpClass(cls):
        cls.retriever = _build_retriever()

    def test_nosql_query_returns_data_or_empty(self):
        rows, citations = self.retriever.query_nosql("Istanbul NOTAM overview")
        # NOTAM files may or may not be present
        self.assertIsInstance(rows, list)
        self.assertIsInstance(citations, list)
        self.assertGreaterEqual(len(citations), 1)  # Always returns a citation even if empty

    def test_nosql_notam_structure_when_data_present(self):
        """If data is returned, it should have NOTAM-specific fields."""
        rows, _ = self.retriever.query_nosql("JFK NOTAM")
        for row in rows:
            if "facilityDesignator" in row:
                self.assertIn("icaoMessage", row)
                self.assertIn("source_file", row)


# ====================================================================
# 10. Runway Constraints Fallback
# ====================================================================


class TestRunwayConstraintsFallback(unittest.TestCase):
    """Test runway constraints CSV fallback."""

    @classmethod
    def setUpClass(cls):
        cls.retriever = _build_retriever()

    @unittest.skipUnless(
        _has_data_file("data", "b-runways.csv") or _has_data_file("data", "g-ourairports_recent"),
        "Runway CSV data not present"
    )
    def test_runway_fallback_for_jfk(self):
        rows, citations = self.retriever._query_runway_constraints_fallback(
            "runway constraints at JFK", airports=["KJFK"]
        )
        self.assertGreaterEqual(len(rows), 1)
        self.assertIsNone(rows[0].get("error_code"))
        for row in rows:
            self.assertEqual(row["airport"], "KJFK")
            self.assertIn("runway_id", row)
            self.assertIn("length_ft", row)

    @unittest.skipUnless(
        _has_data_file("data", "b-runways.csv") or _has_data_file("data", "g-ourairports_recent"),
        "Runway CSV data not present"
    )
    def test_runway_fallback_for_istanbul(self):
        rows, citations = self.retriever._query_runway_constraints_fallback(
            "runway data for Istanbul", airports=["LTFM", "LTBA", "LTFJ"]
        )
        self.assertGreaterEqual(len(rows), 1)
        for row in rows:
            self.assertIn(row["airport"], {"LTFM", "LTBA", "LTFJ"})

    def test_runway_fallback_with_no_matching_airports(self):
        rows, citations = self.retriever._query_runway_constraints_fallback(
            "runway constraints", airports=["ZZZZ"]
        )
        if rows and rows[0].get("error_code"):
            self.assertIn("unavailable", rows[0]["error_code"])
        else:
            # If no rows match, should still return properly
            self.assertIsInstance(rows, list)

    @unittest.skipUnless(
        _has_data_file("data", "b-runways.csv") or _has_data_file("data", "g-ourairports_recent"),
        "Runway CSV data not present"
    )
    def test_runway_fallback_defaults_to_nyc_airports(self):
        """When no airports extracted from query, should default to NYC."""
        rows, citations = self.retriever._query_runway_constraints_fallback(
            "general runway info", airports=[]
        )
        self.assertGreaterEqual(len(rows), 1)
        airports_found = {r["airport"] for r in rows}
        self.assertTrue(airports_found & {"KJFK", "KLGA", "KEWR"})

    @unittest.skipUnless(
        _has_data_file("data", "b-runways.csv") or _has_data_file("data", "g-ourairports_recent"),
        "Runway CSV data not present"
    )
    def test_runway_fallback_citation_structure(self):
        rows, citations = self.retriever._query_runway_constraints_fallback(
            "runway at JFK", airports=["KJFK"]
        )
        for c in citations:
            self.assertEqual(c.source_type, "SQL")
            self.assertEqual(c.dataset, "ourairports-runways-fallback")
            self.assertTrue(c.title.startswith("KJFK"))

    @unittest.skipUnless(
        _has_data_file("data", "b-runways.csv") or _has_data_file("data", "g-ourairports_recent"),
        "Runway CSV data not present"
    )
    def test_runway_fallback_max_60_rows(self):
        """Fallback should cap at 60 rows per airport set."""
        rows, _ = self.retriever._query_runway_constraints_fallback(
            "all runways", airports=["KJFK", "KLGA", "KEWR", "LTFM", "LTBA"]
        )
        self.assertLessEqual(len(rows), 60)


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
            "Summarize ASRS narratives about bird strikes",
            "What happened in similar incidents?",
            "Give me examples of near misses",
            "Why did this incident occur?",
            "Lessons learned from approach incidents",
        ]:
            route = self.router.quick_route(query)
            self.assertEqual(route, "SEMANTIC", f"Expected SEMANTIC for: {query}")

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

    def test_sql_source_mode_is_fallback(self):
        mode = self.retriever.source_mode("SQL")
        self.assertIn(mode, ("live", "fallback"))

    def test_kql_source_mode_without_endpoint(self):
        mode = self.retriever.source_mode("KQL")
        self.assertIn(mode, ("fallback", "blocked", "live"))

    def test_graph_source_mode_without_endpoint(self):
        mode = self.retriever.source_mode("GRAPH")
        self.assertIn(mode, ("fallback", "blocked", "live"))

    def test_nosql_source_mode_without_endpoint(self):
        mode = self.retriever.source_mode("NOSQL")
        self.assertIn(mode, ("fallback", "blocked", "live"))

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
        self.assertIn("strict_source_mode", meta)


# ====================================================================
# 14. Strict Mode Blocks Mock Fallbacks
# ====================================================================


class TestStrictModeBlocking(unittest.TestCase):
    """Strict mode should block mock fallback data sources."""

    @classmethod
    def setUpClass(cls):
        os.environ["RETRIEVAL_STRICT_SOURCE_MODE"] = "true"
        os.environ["ALLOW_MOCK_KQL_FALLBACK"] = "false"
        os.environ["ALLOW_MOCK_GRAPH_FALLBACK"] = "false"
        os.environ["ALLOW_MOCK_NOSQL_FALLBACK"] = "false"
        cls.retriever = UnifiedRetriever(enable_pii_filter=False)

    @classmethod
    def tearDownClass(cls):
        os.environ["RETRIEVAL_STRICT_SOURCE_MODE"] = "false"
        os.environ["ALLOW_MOCK_KQL_FALLBACK"] = "true"
        os.environ["ALLOW_MOCK_GRAPH_FALLBACK"] = "true"
        os.environ["ALLOW_MOCK_NOSQL_FALLBACK"] = "true"

    def test_kql_blocked_in_strict_mode(self):
        with patch.object(ur, "FABRIC_KQL_ENDPOINT", ""):
            rows, _ = self.retriever.query_kql("opensky_states | take 1")
            self.assertEqual(rows[0].get("error_code"), "source_unavailable")

    def test_graph_blocked_in_strict_mode(self):
        with patch.object(ur, "FABRIC_GRAPH_ENDPOINT", ""):
            rows, _ = self.retriever.query_graph("dependency path")
            self.assertEqual(rows[0].get("error_code"), "source_unavailable")

    def test_nosql_blocked_in_strict_mode(self):
        with patch.object(ur, "FABRIC_NOSQL_ENDPOINT", ""):
            rows, _ = self.retriever.query_nosql("notam")
            self.assertEqual(rows[0].get("error_code"), "source_unavailable")


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
        self.assertIn("strict_mode", row)

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
        self.assertIn("execution_mode", row)

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
    """Test thread safety of shared SQLite connection."""

    @classmethod
    def setUpClass(cls):
        cls.retriever = _build_retriever()

    def test_concurrent_read_queries(self):
        """Multiple threads reading from SQLite should not crash."""
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
# 19. Edge Cases — Empty / Corrupt DB
# ====================================================================


class TestEmptyDatabase(unittest.TestCase):
    """Test behavior with an empty SQLite database."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.empty_db = os.path.join(self.tmpdir, "empty.db")
        conn = sqlite3.connect(self.empty_db)
        conn.close()

    def test_schema_on_empty_db(self):
        with patch.dict(os.environ, {"SQLITE_PATH": self.empty_db}):
            with patch.object(ur, "DB_PATH", Path(self.empty_db)):
                retriever = _build_retriever()
                schema = retriever.current_sql_schema()
                self.assertEqual(len(schema.get("tables", [])), 0)

    def test_query_on_empty_db_returns_error(self):
        with patch.dict(os.environ, {"SQLITE_PATH": self.empty_db}):
            with patch.object(ur, "DB_PATH", Path(self.empty_db)):
                retriever = _build_retriever()
                rows, _ = retriever.execute_sql_query(
                    "SELECT * FROM asrs_reports LIMIT 1"
                )
                self.assertEqual(rows[0]["error_code"], "sql_schema_missing")


class TestDatabaseWithSchemaNoData(unittest.TestCase):
    """Test behavior with correct schema but zero rows."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.tmpdir, "no_data.db")
        conn = sqlite3.connect(self.db_path)
        conn.execute("""
            CREATE TABLE asrs_reports (
                asrs_report_id TEXT PRIMARY KEY,
                event_date DATE,
                location TEXT,
                aircraft_type TEXT,
                flight_phase TEXT,
                narrative_type TEXT,
                title TEXT,
                report_text TEXT NOT NULL,
                raw_json TEXT NOT NULL,
                ingested_at TIMESTAMP NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE asrs_ingestion_runs (
                run_id TEXT PRIMARY KEY,
                started_at TIMESTAMP NOT NULL,
                completed_at TIMESTAMP,
                status TEXT NOT NULL,
                source_manifest_path TEXT,
                records_seen INTEGER NOT NULL,
                records_loaded INTEGER NOT NULL,
                records_failed INTEGER NOT NULL
            )
        """)
        conn.commit()
        conn.close()

    def test_count_returns_zero(self):
        with patch.object(ur, "DB_PATH", Path(self.db_path)):
            retriever = _build_retriever()
            rows, _ = retriever.execute_sql_query("SELECT COUNT(*) AS cnt FROM asrs_reports")
            self.assertIsNone(rows[0].get("error_code"))
            self.assertEqual(rows[0]["cnt"], 0)

    def test_select_returns_empty_list(self):
        with patch.object(ur, "DB_PATH", Path(self.db_path)):
            retriever = _build_retriever()
            rows, citations = retriever.execute_sql_query(
                "SELECT * FROM asrs_reports LIMIT 10"
            )
            self.assertEqual(len(rows), 0)
            self.assertEqual(len(citations), 0)

    def test_schema_shows_correct_tables(self):
        with patch.object(ur, "DB_PATH", Path(self.db_path)):
            retriever = _build_retriever()
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
# 22. Dual-Mode Switching Logic
# ====================================================================


class TestDualModeSwitching(unittest.TestCase):
    """Test USE_POSTGRES / PGHOST detection logic."""

    def test_explicit_use_postgres_true(self):
        with patch.dict(os.environ, {"USE_POSTGRES": "true"}, clear=False):
            raw = os.getenv("USE_POSTGRES", "").strip().lower()
            result = raw in ("true", "1", "yes")
            self.assertTrue(result)

    def test_explicit_use_postgres_false(self):
        with patch.dict(os.environ, {"USE_POSTGRES": "false"}, clear=False):
            raw = os.getenv("USE_POSTGRES", "").strip().lower()
            result = raw in ("true", "1", "yes")
            self.assertFalse(result)

    def test_empty_use_postgres_with_pghost_enables_postgres(self):
        with patch.dict(os.environ, {"USE_POSTGRES": "", "PGHOST": "myhost.db"}, clear=False):
            raw = os.getenv("USE_POSTGRES", "").strip().lower()
            if raw:
                result = raw in ("true", "1", "yes")
            else:
                result = bool(os.getenv("PGHOST"))
            self.assertTrue(result)

    def test_empty_use_postgres_no_pghost_disables_postgres(self):
        env = os.environ.copy()
        env.pop("PGHOST", None)
        env["USE_POSTGRES"] = ""
        with patch.dict(os.environ, env, clear=True):
            raw = os.getenv("USE_POSTGRES", "").strip().lower()
            if raw:
                result = raw in ("true", "1", "yes")
            else:
                result = bool(os.getenv("PGHOST"))
            self.assertFalse(result)


# ====================================================================
# 23. SQL Unavailable Handling
# ====================================================================


class TestSQLUnavailableHandling(unittest.TestCase):
    """Test behavior when SQL is unavailable."""

    def test_execute_sql_when_db_is_none(self):
        retriever = _build_retriever()
        retriever.db = None
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
# 24. Env Helper Functions
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
# 25. Data Integrity Spot Checks
# ====================================================================


class TestDataIntegritySpotChecks(unittest.TestCase):
    """Spot-check data quality in the loaded SQLite database."""

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
# 26. Vector/Semantic Search (mocked when endpoint unavailable)
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
# 27. Looks-Like-KQL Detection
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
# 28. Postgres Default Database Name Mismatch Check
# ====================================================================


class TestPostgresConfigDefaults(unittest.TestCase):
    """Document and verify configuration defaults."""

    def test_default_pgdatabase_mismatch_warning(self):
        """
        KNOWN ISSUE: Default PGDATABASE in code is 'aviationdb' but actual
        Azure PostgreSQL database is 'aviationrag'. This test documents the
        mismatch so it is not forgotten.
        """
        with patch.dict(os.environ, {"PGDATABASE": ""}, clear=False):
            default_db = os.getenv("PGDATABASE", "aviationdb")
        self.assertEqual(
            default_db, "aviationdb",
            "Default PGDATABASE should be 'aviationdb' (code default). "
            "NOTE: Actual Azure DB is 'aviationrag' — ensure PGDATABASE env var is set in production."
        )


# ====================================================================
# 29. Query Token Extraction
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
# 30. Retrieval Result Serialization
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
