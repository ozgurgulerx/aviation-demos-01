import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from shared_utils import OPS_TABLE_SIGNALS, matches_any
from unified_retriever import _contains_tsql_parameter_placeholders


class KeywordMatchingStabilityTests(unittest.TestCase):
    def test_matches_any_returns_false_for_empty_keywords(self):
        self.assertFalse(matches_any("delay analytics", frozenset()))

    def test_matches_any_enforces_word_boundaries(self):
        keywords = frozenset({"dispatch"})
        self.assertFalse(matches_any("dispatcher queue", keywords))
        self.assertTrue(matches_any("dispatch queue", keywords))

    def test_ops_table_signals_match_explicit_table_tokens(self):
        self.assertTrue(matches_any("query ops_flight_legs delays", OPS_TABLE_SIGNALS))
        self.assertTrue(matches_any("trace propagation chain", OPS_TABLE_SIGNALS))
        self.assertTrue(matches_any("dispatchable MEL item", OPS_TABLE_SIGNALS))

    def test_tsql_parameter_placeholder_detection(self):
        self.assertTrue(_contains_tsql_parameter_placeholders("SELECT * FROM t WHERE origin=@origin"))
        self.assertTrue(_contains_tsql_parameter_placeholders("SELECT * FROM t WHERE origin=@Origin"))
        self.assertFalse(_contains_tsql_parameter_placeholders("SELECT @@version AS version"))


if __name__ == "__main__":
    unittest.main()
