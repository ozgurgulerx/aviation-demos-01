import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from agentic_orchestrator import AgenticOrchestrator  # noqa: E402


class AgenticOrchestratorIntentTests(unittest.TestCase):
    def _orchestrator(self) -> AgenticOrchestrator:
        return object.__new__(AgenticOrchestrator)

    def test_infer_intent_prioritizes_short_horizon_departure_risk_compare(self):
        orchestrator = self._orchestrator()
        intent = orchestrator._infer_intent(
            "compare next-90-minute departure risk across SAW, AYT, ADB"
        )
        self.assertEqual(intent, "PilotBrief.Departure")

    def test_infer_intent_keeps_delay_analytics_for_non_horizon_queries(self):
        orchestrator = self._orchestrator()
        intent = orchestrator._infer_intent(
            "compare monthly delay performance across top carriers"
        )
        self.assertEqual(intent, "Analytics.Compare")


if __name__ == "__main__":
    unittest.main()
