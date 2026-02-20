import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tests"))

import unified_retriever as ur  # noqa: E402
from schema_provider import SchemaProvider  # noqa: E402
from unified_retriever import UnifiedRetriever  # noqa: E402
from pg_mock import patch_pg_pool  # noqa: E402


def _build_mock_retriever() -> UnifiedRetriever:
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


class DatastoreSmokeMatrixTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.retriever = _build_mock_retriever()

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

    def test_kql_store_blocked_without_endpoint(self):
        with patch.object(ur, "FABRIC_KQL_ENDPOINT", ""):
            rows, citations = self.retriever.query_kql(
                "opensky_states | where timestamp > ago(60m) | take 5",
                window_minutes=60,
            )
            self.assertEqual(rows[0].get("error_code"), "source_unavailable")

    def test_graph_store_blocked_without_endpoint(self):
        with patch.object(ur, "FABRIC_GRAPH_ENDPOINT", ""):
            rows, citations = self.retriever.query_graph("IST dependency path", hops=2)
            self.assertEqual(rows[0].get("error_code"), "source_unavailable")

    def test_nosql_store_blocked_without_endpoint(self):
        with patch.object(ur, "FABRIC_NOSQL_ENDPOINT", ""):
            rows, citations = self.retriever.query_nosql("Istanbul NOTAM overview")
            self.assertEqual(rows[0].get("error_code"), "source_unavailable")

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


if __name__ == "__main__":
    unittest.main()
