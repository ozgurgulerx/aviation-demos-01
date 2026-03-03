import base64
import json
import os
import sys
import time
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tests"))

import unified_retriever as ur  # noqa: E402
from unified_retriever import UnifiedRetriever  # noqa: E402
from pg_mock import MockPool, patch_pg_pool  # noqa: E402


class SourceExecutionPolicyTests(unittest.TestCase):
    @staticmethod
    def _set_runtime_identity_defaults() -> None:
        os.environ.setdefault(
            "AZURE_ACCOUNT_UPN",
            os.getenv("EXPECTED_RUNTIME_ACCOUNT_UPN", ur.GUARDRAIL_ACCOUNT_UPN),
        )
        os.environ.setdefault(
            "AZURE_TENANT_ID",
            os.getenv("EXPECTED_RUNTIME_TENANT_ID", ur.GUARDRAIL_TENANT_ID),
        )
        os.environ.setdefault(
            "AZURE_SUBSCRIPTION_ID",
            os.getenv("EXPECTED_RUNTIME_SUBSCRIPTION_ID", ur.GUARDRAIL_SUBSCRIPTION_ID),
        )

    def _fake_jwt(self, ttl_seconds: int) -> str:
        header = base64.urlsafe_b64encode(json.dumps({"alg": "none", "typ": "JWT"}).encode("utf-8")).decode("utf-8").rstrip("=")
        payload = base64.urlsafe_b64encode(
            json.dumps({"exp": int(time.time()) + max(0, int(ttl_seconds))}).encode("utf-8")
        ).decode("utf-8").rstrip("=")
        return f"{header}.{payload}.sig"

    def _build_retriever(self) -> UnifiedRetriever:
        self._set_runtime_identity_defaults()

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
        retriever._cosmos_container = None
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

    def test_query_sql_need_schema_without_fallback_returns_schema_missing(self):
        retriever = self._build_retriever()
        retriever.sql_writer.sql = "-- NEED_SCHEMA: damage_score column in asrs_reports"
        # The heuristic fallback only fires for certain query patterns; use a query that won't match
        rows, sql, _citations = retriever.query_sql(
            "Describe the incidents with highest damage score."
        )
        self.assertEqual(sql, "-- NEED_SCHEMA: damage_score column in asrs_reports")
        self.assertEqual(rows[0].get("error_code"), "sql_schema_missing")

    def test_heuristic_sql_fallback_builds_valid_runway_query_for_airport_compare(self):
        retriever = self._build_retriever()
        retriever.cached_sql_schema = MagicMock(
            return_value={
                "tables": [
                    {
                        "schema": "demo",
                        "table": "ourairports_airports",
                        "columns": [
                            {"name": "id"},
                            {"name": "ident"},
                            {"name": "iata_code"},
                        ],
                    },
                    {
                        "schema": "demo",
                        "table": "ourairports_runways",
                        "columns": [
                            {"name": "id"},
                            {"name": "airport_ref"},
                            {"name": "surface"},
                            {"name": "length_ft"},
                            {"name": "width_ft"},
                            {"name": "closed"},
                        ],
                    },
                ]
            }
        )

        sql = retriever._heuristic_sql_fallback(
            "compare next-90-minute flight risk across SAW, AYT, ADB",
            "missing columns in current schema: a.iata, active, airport, runway_id",
        )
        self.assertIsNotNone(sql)
        self.assertIn("ourairports_runways", sql)
        self.assertIn("ourairports_airports", sql)
        self.assertIn("a.iata_code", sql)
        self.assertIn("r.id AS runway_id", sql)
        self.assertIn("'SAW'", sql)
        self.assertIn("'AYT'", sql)
        self.assertIn("'ADB'", sql)

    def test_heuristic_sql_fallback_uses_detected_non_demo_schema(self):
        retriever = self._build_retriever()
        retriever.cached_sql_schema = MagicMock(
            return_value={
                "tables": [
                    {
                        "schema": "ops",
                        "table": "ourairports_airports",
                        "columns": [
                            {"name": "id"},
                            {"name": "ident"},
                            {"name": "iata_code"},
                        ],
                    },
                    {
                        "schema": "ops",
                        "table": "ourairports_runways",
                        "columns": [
                            {"name": "id"},
                            {"name": "airport_ref"},
                            {"name": "surface"},
                            {"name": "length_ft"},
                            {"name": "width_ft"},
                            {"name": "closed"},
                        ],
                    },
                ]
            }
        )

        sql = retriever._heuristic_sql_fallback(
            "compare next-90-minute flight risk across SAW, AYT, ADB",
            "missing columns in current schema: a.iata, active, airport, runway_id",
        )
        self.assertIsNotNone(sql)
        self.assertIn("FROM ops.ourairports_runways r", sql)
        self.assertIn("JOIN ops.ourairports_airports a", sql)

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
        self.assertIn(rows[0].get("error_code"), {"kql_runtime_error", "kql_validation_failed", "kql_unmappable_airport_filter"})

    def test_kql_airport_only_query_returns_unmappable_filter_error(self):
        retriever = self._build_retriever()
        schema = {
            "tables": [
                {
                    "table": "opensky_states",
                    "columns": [{"name": "callsign"}, {"name": "icao24"}],
                }
            ]
        }
        with patch.object(ur, "FABRIC_KQL_ENDPOINT", "https://demo.kusto.fabric.microsoft.com"):
            rows, _citations = retriever.query_kql(
                "compare next-90-minute flight risk across SAW, AYT, ADB",
                kql_schema=schema,
            )
        self.assertEqual(rows[0].get("error_code"), "kql_unmappable_airport_filter")

    def test_kql_airport_risk_query_prefers_hazards_table_when_schema_allows(self):
        retriever = self._build_retriever()
        seen: dict[str, object] = {}

        def fake_kusto_rows(_endpoint, csl, database=None, timeout_seconds=None):
            seen["csl"] = csl
            seen["database"] = database
            seen["timeout_seconds"] = timeout_seconds
            return [{"hazard": "TS", "severity": "MOD"}], None

        retriever._kusto_rows = fake_kusto_rows
        schema = {
            "tables": [
                {
                    "table": "hazards_airsigmets",
                    "columns": [
                        {"name": "raw_text"},
                        {"name": "valid_time_from"},
                        {"name": "valid_time_to"},
                        {"name": "hazard"},
                        {"name": "severity"},
                    ],
                }
            ]
        }
        with patch.object(ur, "FABRIC_KQL_ENDPOINT", "https://demo.kusto.fabric.microsoft.com"):
            rows, _citations = retriever.query_kql(
                "compare next-90-minute flight risk across SAW, AYT, ADB",
                kql_schema=schema,
            )
        self.assertEqual(rows[0].get("hazard"), "TS")
        self.assertIn("hazards_airsigmets", str(seen.get("csl", "")))
        self.assertIn("raw_text has_any", str(seen.get("csl", "")))
        self.assertGreater(float(seen.get("timeout_seconds", 0) or 0), 0)

    def test_kql_blocks_unsafe_statement(self):
        retriever = self._build_retriever()
        with patch.object(ur, "FABRIC_KQL_ENDPOINT", "https://demo.kusto.fabric.microsoft.com"):
            rows, _citations = retriever.query_kql(".show database schema; drop table x")
        self.assertEqual(rows[0].get("error_code"), "kql_validation_failed")

    def test_kql_live_executes_provided_csl(self):
        retriever = self._build_retriever()
        seen = {}

        def fake_kusto_rows(_endpoint, csl, database=None, timeout_seconds=None):
            seen["csl"] = csl
            seen["database"] = database
            seen["timeout_seconds"] = timeout_seconds
            return [{"callsign": "THY123"}], None

        retriever._kusto_rows = fake_kusto_rows
        with patch.object(ur, "FABRIC_KQL_ENDPOINT", "https://demo.kusto.fabric.microsoft.com"):
            rows, citations = retriever.query_kql("opensky_states | where timestamp > ago(30m) | take 1")

        self.assertTrue(rows)
        self.assertEqual(rows[0]["callsign"], "THY123")
        self.assertIn("ago(30m)", seen["csl"])
        self.assertEqual(citations[0].source_type, "KQL")

    def test_kusto_rows_uses_explicit_v2_query_endpoint_as_is(self):
        retriever = self._build_retriever()
        seen = {}

        def fake_post(endpoint, payload, token_scope=None, timeout_seconds=30.0):
            seen["endpoint"] = endpoint
            seen["payload"] = payload
            seen["token_scope"] = token_scope
            seen["timeout_seconds"] = timeout_seconds
            return {
                "Tables": [
                    {
                        "Columns": [{"ColumnName": "value"}],
                        "Rows": [[1]],
                    }
                ]
            }

        retriever._post_json = fake_post
        rows, error = retriever._kusto_rows(
            "https://demo.kusto.fabric.microsoft.com/v2/rest/query",
            "print 1",
            database="demo_db",
        )

        self.assertIsNone(error)
        self.assertEqual(rows[0]["value"], 1)
        self.assertEqual(seen["endpoint"], "https://demo.kusto.fabric.microsoft.com/v2/rest/query")
        self.assertEqual(seen["payload"]["db"], "demo_db")

    def test_kusto_rows_derives_v1_query_path_from_cluster_root(self):
        retriever = self._build_retriever()
        seen = {}

        def fake_post(endpoint, payload, token_scope=None, timeout_seconds=30.0):
            seen["endpoint"] = endpoint
            return {
                "Tables": [
                    {
                        "Columns": [{"ColumnName": "value"}],
                        "Rows": [[7]],
                    }
                ]
            }

        retriever._post_json = fake_post
        rows, error = retriever._kusto_rows(
            "https://demo.kusto.fabric.microsoft.com",
            "print 7",
            database="demo_db",
        )

        self.assertIsNone(error)
        self.assertEqual(rows[0]["value"], 7)
        self.assertEqual(seen["endpoint"], "https://demo.kusto.fabric.microsoft.com/v1/rest/query")

    def test_kusto_rows_rejects_duplicate_query_path_segments(self):
        retriever = self._build_retriever()
        rows, error = retriever._kusto_rows(
            "https://demo.kusto.fabric.microsoft.com/v2/rest/query/v1/rest/query",
            "print 1",
            database="demo_db",
        )
        self.assertEqual(rows, [])
        self.assertIn("invalid_kusto_endpoint_path", str(error))

    def test_graph_blocked_without_endpoint(self):
        retriever = self._build_retriever()
        with patch.object(ur, "FABRIC_GRAPH_ENDPOINT", ""):
            rows, _citations = retriever.query_graph("dependency paths")
        self.assertEqual(rows[0].get("error_code"), "source_unavailable")

    def test_graph_fallback_mode_skips_live_probe(self):
        retriever = self._build_retriever()
        retriever._query_graph_live = MagicMock(
            return_value=([{"src_id": "LIVE"}], None, 0, {"graph_path": "fabric_graph_live_kusto"})
        )
        retriever._query_graph_pg_fallback = MagicMock(
            return_value=([{"src_id": "FALLBACK"}], [])
        )
        with patch.object(ur, "FABRIC_GRAPH_ENDPOINT", "https://demo.kusto.fabric.microsoft.com"), patch.object(
            retriever,
            "source_capability",
            return_value={
                "source": "GRAPH",
                "status": "degraded",
                "reason_code": "missing_fabric_graph_database",
                "detail": "kusto graph endpoint requires FABRIC_GRAPH_DATABASE or FABRIC_KQL_DATABASE; using SQL fallback",
                "execution_mode": "fallback",
            },
        ):
            rows, _citations = retriever.query_graph("dependency paths", hops=1)
        retriever._query_graph_live.assert_not_called()
        retriever._query_graph_pg_fallback.assert_called_once()
        self.assertEqual(rows[0].get("src_id"), "FALLBACK")
        self.assertEqual(rows[0].get("graph_path"), "pg_fallback_capability_mode")

    def test_nosql_blocked_without_endpoint(self):
        retriever = self._build_retriever()
        retriever._cosmos_container = None
        with patch.object(ur, "FABRIC_NOSQL_ENDPOINT", ""):
            rows, _citations = retriever.query_nosql("notam snapshot")
        self.assertEqual(rows[0].get("error_code"), "source_unavailable")

    def test_nosql_cosmos_returns_docs(self):
        retriever = self._build_retriever()
        mock_container = MagicMock()
        mock_container.query_items.return_value = [
            {"id": "NOTAM-A0001-26-KJFK", "notam_number": "A0001/26", "icao": "KJFK", "content": "RWY 13R/31L CLSD", "status": "active"},
            {"id": "NOTAM-A0002-26-KJFK", "notam_number": "A0002/26", "icao": "KJFK", "content": "TWY B CLSD", "status": "active"},
        ]
        retriever._cosmos_container = mock_container

        rows, citations = retriever.query_nosql("active NOTAMs for JFK")
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["icao"], "KJFK")
        self.assertGreaterEqual(len(citations), 1)
        self.assertEqual(citations[0].source_type, "NOSQL")
        self.assertIn("KJFK", citations[0].title)

    def test_nosql_cosmos_extracts_airports(self):
        retriever = self._build_retriever()
        mock_container = MagicMock()
        mock_container.query_items.return_value = [
            {"id": "NOTAM-1", "notam_number": "A0001/26", "icao": "KJFK", "content": "test", "status": "active"},
        ]
        retriever._cosmos_container = mock_container

        retriever.query_nosql("NOTAMs for JFK and Istanbul")
        call_kwargs = mock_container.query_items.call_args
        query_str = call_kwargs.kwargs.get("query") or call_kwargs[1].get("query") or (call_kwargs[0][0] if call_kwargs[0] else "")
        params = call_kwargs.kwargs.get("parameters") or call_kwargs[1].get("parameters", [])
        icao_values = [p["value"] for p in params]
        self.assertIn("KJFK", icao_values)

    def test_nosql_cosmos_error_returns_structured_error(self):
        retriever = self._build_retriever()
        mock_container = MagicMock()
        mock_container.query_items.side_effect = RuntimeError("cosmos connection lost")
        retriever._cosmos_container = mock_container

        rows, citations = retriever.query_nosql("active NOTAMs")
        self.assertEqual(rows[0].get("error_code"), "cosmos_runtime_error")
        self.assertEqual(len(citations), 0)

    def test_source_mode_nosql_live_with_cosmos(self):
        retriever = self._build_retriever()
        retriever._cosmos_container = MagicMock()
        with patch.object(ur, "FABRIC_NOSQL_ENDPOINT", ""):
            mode = retriever.source_mode("NOSQL")
        self.assertEqual(mode, "live")

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

    def test_fabric_sql_falls_back_to_rest_when_tds_capability_missing(self):
        retriever = self._build_retriever()
        retriever._post_json = MagicMock(return_value=[{"airport": "JFK", "carrier": "AA"}])
        retriever._fabric_sql_tds_capability = MagicMock(
            return_value={"ok": False, "detail": "pyodbc_unavailable:No module named 'pyodbc'"}
        )
        retriever._fabric_auth_reason_for_source = MagicMock(
            return_value=(True, "ready", {"auth_mode": "sp_client_credentials", "auth_ready": True, "token": "x"})
        )
        with patch.object(ur, "FABRIC_SQL_ENDPOINT", "https://fabric.example/sql"), patch.dict(
            os.environ,
            {"FABRIC_SQL_SERVER": "warehouse.example.fabric.microsoft.com", "FABRIC_SQL_DATABASE": "wh"},
            clear=False,
        ):
            rows, citations = retriever.query_fabric_sql("top delay causes for JFK")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].get("airport"), "JFK")
        self.assertGreaterEqual(len(citations), 1)

    def test_fabric_sql_prefers_rest_when_rest_and_tds_are_both_configured(self):
        retriever = self._build_retriever()
        retriever._post_json = MagicMock(return_value=[{"airport": "IST", "carrier": "TK"}])
        retriever._fabric_sql_tds_capability = MagicMock(return_value={"ok": True, "detail": "ready"})
        retriever._fabric_auth_reason_for_source = MagicMock(
            return_value=(True, "ready", {"auth_mode": "sp_client_credentials", "auth_ready": True, "token": "x"})
        )
        retriever._query_fabric_sql_tds = MagicMock(return_value=([{"airport": "JFK"}], []))
        with patch.object(ur, "FABRIC_SQL_ENDPOINT", "https://fabric.example/sql"), patch.dict(
            os.environ,
            {"FABRIC_SQL_SERVER": "warehouse.example.fabric.microsoft.com", "FABRIC_SQL_DATABASE": "wh"},
            clear=False,
        ):
            rows, citations = retriever.query_fabric_sql("top delay causes for IST")
        self.assertEqual(rows[0].get("airport"), "IST")
        self.assertGreaterEqual(len(citations), 1)
        retriever._query_fabric_sql_tds.assert_not_called()

    def test_fabric_sql_effective_mode_prefers_rest(self):
        retriever = self._build_retriever()
        retriever._fabric_sql_tds_capability = MagicMock(return_value={"ok": True, "detail": "ready"})
        with patch.object(ur, "FABRIC_SQL_ENDPOINT", "https://fabric.example/sql"), patch.dict(
            os.environ,
            {"FABRIC_SQL_SERVER": "warehouse.example.fabric.microsoft.com", "FABRIC_SQL_DATABASE": "wh"},
            clear=False,
        ):
            self.assertEqual(retriever._fabric_sql_effective_mode(), "rest")

    def test_fabric_sql_effective_mode_auto_keeps_rest_when_auth_not_ready(self):
        retriever = self._build_retriever()
        retriever._fabric_sql_tds_capability = MagicMock(return_value={"ok": True, "detail": "ready"})
        retriever._fabric_auth_reason_for_source = MagicMock(
            return_value=(False, "static_bearer_disabled", {"auth_mode": "none"})
        )
        with patch.object(ur, "FABRIC_SQL_ENDPOINT", "https://fabric.example/sql"), patch.dict(
            os.environ,
            {"FABRIC_SQL_MODE": "auto", "FABRIC_SQL_SERVER": "warehouse.example.fabric.microsoft.com", "FABRIC_SQL_DATABASE": "wh"},
            clear=False,
        ):
            detail = retriever._fabric_sql_effective_mode_detail()
        self.assertEqual(detail.get("mode"), "rest")
        self.assertFalse(bool(detail.get("auth_ready")))
        self.assertIn("rest_auth_not_ready", str(detail.get("reason", "")))

    def test_fabric_token_bundle_blocks_static_bearer_when_not_allowed(self):
        token = self._fake_jwt(600)
        ur._fabric_token_cache.clear()
        with patch.dict(
            os.environ,
            {
                "FABRIC_CLIENT_ID": "",
                "FABRIC_CLIENT_SECRET": "",
                "FABRIC_TENANT_ID": "",
                "FABRIC_BEARER_TOKEN": token,
                "ALLOW_STATIC_FABRIC_BEARER": "false",
                "FABRIC_TOKEN_MIN_TTL_SECONDS": "120",
            },
            clear=False,
        ):
            bundle = ur._acquire_fabric_token_bundle()
        self.assertFalse(bool(bundle.get("auth_ready")))
        self.assertEqual(bundle.get("reason"), "static_bearer_disabled")
        self.assertEqual(bundle.get("auth_mode"), "none")

    def test_fabric_token_bundle_accepts_static_bearer_when_allowed_and_fresh(self):
        token = self._fake_jwt(600)
        ur._fabric_token_cache.clear()
        with patch.dict(
            os.environ,
            {
                "FABRIC_CLIENT_ID": "",
                "FABRIC_CLIENT_SECRET": "",
                "FABRIC_TENANT_ID": "",
                "FABRIC_BEARER_TOKEN": token,
                "ALLOW_STATIC_FABRIC_BEARER": "true",
                "FABRIC_TOKEN_MIN_TTL_SECONDS": "120",
            },
            clear=False,
        ):
            bundle = ur._acquire_fabric_token_bundle()
        self.assertTrue(bool(bundle.get("auth_ready")))
        self.assertEqual(bundle.get("reason"), "static_bearer_allowed")
        self.assertEqual(bundle.get("auth_mode"), "static_bearer")

    def test_fabric_sql_returns_source_unavailable_when_tds_missing_and_no_rest(self):
        retriever = self._build_retriever()
        retriever._fabric_sql_tds_capability = MagicMock(
            return_value={"ok": False, "detail": "pyodbc_unavailable:No module named 'pyodbc'"}
        )
        with patch.object(ur, "FABRIC_SQL_ENDPOINT", ""), patch.dict(
            os.environ,
            {"FABRIC_SQL_SERVER": "warehouse.example.fabric.microsoft.com", "FABRIC_SQL_DATABASE": "wh"},
            clear=False,
        ):
            rows, citations = retriever.query_fabric_sql("top delay causes for JFK")
        self.assertEqual(rows[0].get("error_code"), "source_unavailable")
        self.assertIn("TDS unavailable", rows[0].get("error", ""))
        self.assertEqual(len(citations), 0)

    def test_source_mode_blocks_fabric_sql_without_tds_or_rest(self):
        retriever = self._build_retriever()
        retriever._fabric_sql_tds_capability = MagicMock(
            return_value={"ok": False, "detail": "pyodbc_unavailable", "server_configured": True}
        )
        with patch.object(ur, "FABRIC_SQL_ENDPOINT", ""), patch.dict(
            os.environ,
            {"FABRIC_SQL_SERVER": "warehouse.example.fabric.microsoft.com", "FABRIC_SQL_DATABASE": "wh"},
            clear=False,
        ):
            self.assertEqual(retriever.source_mode("FABRIC_SQL"), "blocked")

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

    def test_graph_tokenization_accepts_structured_query_payload(self):
        retriever = self._build_retriever()
        tokens = retriever._query_tokens({"query": "graph neighbors for IST SAW", "hops": 2})
        self.assertIn("GRAPH", tokens)
        self.assertIn("IST", tokens)
        self.assertIn("SAW", tokens)

    def test_extract_airports_accepts_structured_query_payload(self):
        retriever = self._build_retriever()
        airports = retriever._extract_airports_from_query(
            {"query": "graph neighbors around IST and SAW", "hops": 2}
        )
        self.assertIn("LTFM", airports)
        self.assertIn("LTFJ", airports)

    def test_nosql_cosmos_empty_result(self):
        retriever = self._build_retriever()
        mock_container = MagicMock()
        mock_container.query_items.return_value = []
        retriever._cosmos_container = mock_container

        rows, citations = retriever.query_nosql("active NOTAMs for JFK")
        self.assertEqual(len(citations), 0)
        self.assertTrue(rows)
        self.assertIn(rows[0].get("error_code"), {"nosql_runtime_error"})

    def test_nosql_cosmos_missing_fields_in_doc(self):
        retriever = self._build_retriever()
        mock_container = MagicMock()
        mock_container.query_items.return_value = [
            {"id": "doc-1", "icao": "KJFK", "status": "active"},
        ]
        retriever._cosmos_container = mock_container

        rows, citations = retriever.query_nosql("NOTAMs for JFK")
        self.assertEqual(len(rows), 1)
        self.assertGreaterEqual(len(citations), 1)
        self.assertIn("KJFK", citations[0].title)

    def test_nosql_no_airports_extracted(self):
        retriever = self._build_retriever()
        mock_container = MagicMock()
        mock_container.query_items.return_value = [
            {"id": "generic-1", "notam_number": "G0001/26", "icao": "ZZZZ", "content": "test", "status": "active"},
        ]
        retriever._cosmos_container = mock_container

        rows, citations = retriever.query_nosql("show all active notices")
        call_kwargs = mock_container.query_items.call_args
        query_str = call_kwargs.kwargs.get("query") or call_kwargs[1].get("query") or (call_kwargs[0][0] if call_kwargs[0] else "")
        self.assertIn("LIMIT 30", query_str)
        self.assertNotIn("@icao", query_str)
        self.assertTrue(call_kwargs.kwargs.get("enable_cross_partition_query", False))

    def test_guardrail_mismatch_blocks_all_sources(self):
        retriever = self._build_retriever()
        with patch.dict(
            os.environ,
            {
                "EXPECTED_RUNTIME_TENANT_ID": ur.GUARDRAIL_TENANT_ID,
                "AZURE_TENANT_ID": "00000000-0000-0000-0000-000000000000",
            },
            clear=False,
        ):
            self.assertEqual(retriever.source_mode("SQL"), "blocked")
            cap = retriever.source_capability("SQL", refresh=False)
            self.assertEqual(cap.get("status"), "unavailable")
            self.assertEqual(cap.get("reason_code"), "tenant_guardrail_mismatch")

    def test_guardrail_missing_runtime_value_blocks_all_sources(self):
        retriever = self._build_retriever()
        with patch.dict(
            os.environ,
            {
                "EXPECTED_RUNTIME_TENANT_ID": ur.GUARDRAIL_TENANT_ID,
                "AZURE_TENANT_ID": "",
            },
            clear=False,
        ):
            self.assertEqual(retriever.source_mode("SQL"), "blocked")
            cap = retriever.source_capability("SQL", refresh=False)
            self.assertEqual(cap.get("status"), "unavailable")
            self.assertEqual(cap.get("reason_code"), "tenant_guardrail_mismatch")
            self.assertIn("missing_runtime_identity_value", str(retriever._identity_guardrail_report))

    def test_preflight_includes_guardrail_and_capability_sections(self):
        retriever = self._build_retriever()
        with patch.dict(
            os.environ,
            {
                "EXPECTED_RUNTIME_TENANT_ID": ur.GUARDRAIL_TENANT_ID,
                "AZURE_TENANT_ID": ur.GUARDRAIL_TENANT_ID,
            },
            clear=False,
        ):
            payload = retriever.fabric_preflight()

        self.assertIn("identity_guardrail", payload)
        self.assertIn("source_capabilities", payload)
        self.assertIn("baseline_sources", payload)
        self.assertIn("baseline_unavailable_sources", payload)
        self.assertIn("auth_mode_effective", payload)
        self.assertIn("auth_ready", payload)
        endpoint_checks = [c for c in payload["checks"] if c.get("name") in {"fabric_kql_endpoint", "fabric_graph_endpoint", "fabric_nosql_endpoint", "fabric_sql_endpoint"}]
        self.assertTrue(endpoint_checks)
        for check in endpoint_checks:
            self.assertIn("path_valid_for_runtime", check)
            self.assertIn("query_ready", check)
        self.assertTrue(any(c.get("source") == "SQL" for c in payload["source_capabilities"]))

    def test_preflight_probes_normalized_kusto_query_endpoint(self):
        retriever = self._build_retriever()

        def fake_probe(endpoint: str, timeout_seconds: int = 5):
            del timeout_seconds
            if str(endpoint).endswith("/v1/rest/query"):
                return {
                    "status": "warn",
                    "detail": "reachable_http_405",
                    "auth_mode": "sp_client_credentials",
                    "auth_ready": True,
                    "token_ttl_seconds": 600,
                }
            if endpoint:
                return {
                    "status": "warn",
                    "detail": "reachable_http_404",
                    "auth_mode": "sp_client_credentials",
                    "auth_ready": True,
                    "token_ttl_seconds": 600,
                }
            return {
                "status": "warn",
                "detail": "not_configured",
                "auth_mode": "none",
                "auth_ready": False,
                "token_ttl_seconds": None,
            }

        retriever._probe_endpoint = fake_probe
        with patch.object(ur, "FABRIC_KQL_ENDPOINT", "https://demo.kusto.fabric.microsoft.com"), patch.dict(
            os.environ,
            {
                "FABRIC_KQL_DATABASE": "demo_db",
                "FABRIC_BEARER_TOKEN": self._fake_jwt(600),
                "ALLOW_STATIC_FABRIC_BEARER": "true",
            },
            clear=False,
        ):
            payload = retriever.fabric_preflight()

        kql_check = next(c for c in payload["checks"] if c.get("name") == "fabric_kql_endpoint")
        self.assertEqual(
            kql_check.get("probe_endpoint"),
            "https://demo.kusto.fabric.microsoft.com/v1/rest/query",
        )
        self.assertTrue(bool(kql_check.get("query_ready")))


if __name__ == "__main__":
    unittest.main()
