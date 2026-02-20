import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import unified_retriever as ur  # noqa: E402
from schema_provider import SchemaProvider  # noqa: E402
from unified_retriever import UnifiedRetriever  # noqa: E402


class DatastoreSmokeMatrixTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        load_dotenv(ROOT / ".env", override=False)
        load_dotenv(ROOT / "src" / ".env.local", override=True)
        os.environ["USE_POSTGRES"] = "false"
        os.environ["SQL_DIALECT"] = "sqlite"
        os.environ["RETRIEVAL_STRICT_SOURCE_MODE"] = "false"
        os.environ["ALLOW_SQLITE_FALLBACK"] = "true"
        os.environ["ALLOW_MOCK_KQL_FALLBACK"] = "true"
        os.environ["ALLOW_MOCK_GRAPH_FALLBACK"] = "true"
        os.environ["ALLOW_MOCK_NOSQL_FALLBACK"] = "true"
        cls.retriever = UnifiedRetriever(enable_pii_filter=False)

    def test_schema_snapshot_contains_sql_tables(self):
        schema = SchemaProvider(self.retriever).snapshot()
        sql_schema = schema.get("sql_schema", {})
        self.assertGreaterEqual(len(sql_schema.get("tables", [])), 1)
        self.assertTrue(sql_schema.get("schema_version"))

    def test_sql_store_query_success(self):
        rows, citations = self.retriever.execute_sql_query(
            "SELECT asrs_report_id, title FROM asrs_reports LIMIT 3"
        )
        self.assertGreaterEqual(len(rows), 1)
        self.assertIsNone(rows[0].get("error_code"))
        self.assertGreaterEqual(len(citations), 1)

    def test_kql_store_query_success(self):
        rows, citations = self.retriever.query_kql(
            "opensky_states | where timestamp > ago(60m) | take 5",
            window_minutes=60,
        )
        self.assertGreaterEqual(len(rows), 1)
        self.assertIsNone(rows[0].get("error_code"))
        self.assertGreaterEqual(len(citations), 1)

    def test_graph_store_query_success(self):
        rows, citations = self.retriever.query_graph("IST dependency path", hops=2)
        self.assertGreaterEqual(len(rows), 1)
        self.assertIsNone(rows[0].get("error_code"))
        self.assertGreaterEqual(len(citations), 1)

    def test_nosql_store_query_success(self):
        rows, citations = self.retriever.query_nosql("Istanbul NOTAM overview")
        self.assertGreaterEqual(len(rows), 1)
        self.assertIsNone(rows[0].get("error_code"))
        self.assertGreaterEqual(len(citations), 1)

    def test_vector_store_query_success_or_structured_unavailable(self):
        rows, citations = self.retriever.query_semantic(
            "runway closure risk",
            top=2,
            embedding=[0.0] * 1536,
            source="VECTOR_REG",
        )
        self.assertGreaterEqual(len(rows), 1)
        error_code = rows[0].get("error_code")
        if error_code is None:
            self.assertGreaterEqual(len(citations), 1)
        else:
            self.assertIn(error_code, {"source_unavailable", "semantic_runtime_error"})

    def test_strict_mode_blocks_mock_fallbacks(self):
        os.environ["RETRIEVAL_STRICT_SOURCE_MODE"] = "true"
        os.environ["ALLOW_MOCK_KQL_FALLBACK"] = "false"
        os.environ["ALLOW_MOCK_GRAPH_FALLBACK"] = "false"
        os.environ["ALLOW_MOCK_NOSQL_FALLBACK"] = "false"
        strict = UnifiedRetriever(enable_pii_filter=False)
        with patch.object(ur, "FABRIC_KQL_ENDPOINT", ""):
            rows, _ = strict.query_kql("opensky_states | take 1")
            self.assertEqual(rows[0].get("error_code"), "source_unavailable")
        with patch.object(ur, "FABRIC_GRAPH_ENDPOINT", ""):
            rows, _ = strict.query_graph("dependency path")
            self.assertEqual(rows[0].get("error_code"), "source_unavailable")
        with patch.object(ur, "FABRIC_NOSQL_ENDPOINT", ""):
            rows, _ = strict.query_nosql("notam")
            self.assertEqual(rows[0].get("error_code"), "source_unavailable")


if __name__ == "__main__":
    unittest.main()
