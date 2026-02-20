import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from context_reconciler import reconcile_context


class ContextReconcilerTests(unittest.TestCase):
    def test_reconcile_orders_sources_deterministically(self):
        source_results = {
            "SQL": [{"id": "sql_1", "value": 2}],
            "KQL": [{"id": "kql_1", "metric": "ops", "value": 5}],
            "VECTOR_OPS": [{"id": "vec_1", "@search.score": 0.4}],
        }

        out = reconcile_context(source_results)
        self.assertEqual(list(out["source_results"].keys())[:3], ["KQL", "SQL", "VECTOR_OPS"])
        self.assertTrue(out["reconciled_items"])
        self.assertIn("fusion_score", out["reconciled_items"][0])

    def test_reconcile_builds_coverage_and_conflicts(self):
        source_results = {
            "KQL": [
                {"id": "m1", "metric": "delay_count", "value": 100, "__evidence_type": "METAR"},
                {"id": "m2", "metric": "delay_count", "value": 170, "__evidence_type": "METAR"},
            ],
            "VECTOR_REG": [
                {"id": "sop_1", "title": "SOP clause", "__evidence_type": "SOPClause", "@search.score": 0.8}
            ],
        }
        required = [
            {"name": "METAR", "optional": False},
            {"name": "SOPClause", "optional": False},
            {"name": "NOTAM", "optional": True},
        ]
        authoritative = {
            "METAR": ["KQL"],
            "SOPClause": ["VECTOR_REG"],
            "NOTAM": ["VECTOR_REG"],
        }

        out = reconcile_context(
            source_results=source_results,
            required_evidence=required,
            authoritative_map=authoritative,
            enable_evidence_slotting=True,
            enable_conflict_detection=True,
        )

        coverage = out["coverage_summary"]
        self.assertEqual(coverage["required_total"], 2)
        self.assertEqual(coverage["required_filled"], 2)
        self.assertEqual(coverage["missing_required"], [])

        conflicts = out["conflict_summary"]
        self.assertGreaterEqual(conflicts["count"], 1)
        self.assertIn(conflicts["severity"], {"medium", "high"})


if __name__ == "__main__":
    unittest.main()
