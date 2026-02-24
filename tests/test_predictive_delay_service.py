#!/usr/bin/env python3
"""Tests for predictive delay service degraded/disabled behavior."""

import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from predictive_delay_service import PredictiveDelayService  # noqa: E402


class PredictiveDelayServiceTests(unittest.TestCase):
    def test_disabled_service_returns_disabled_payload(self):
        svc = PredictiveDelayService(enabled=False)
        payload = svc.get_delays()
        self.assertEqual(payload.get("status"), "disabled")
        self.assertFalse(payload.get("enabled"))

    def test_enabled_service_without_pg_host_returns_degraded(self):
        svc = PredictiveDelayService(enabled=True)
        with patch.dict(os.environ, {"PGHOST": ""}, clear=False):
            payload = svc.get_delays(model="optimized", window_hours=6, limit=10)
        self.assertEqual(payload.get("status"), "degraded")
        self.assertIn("rows", payload)

    def test_metrics_disabled_payload(self):
        svc = PredictiveDelayService(enabled=False)
        payload = svc.get_delay_metrics()
        self.assertEqual(payload.get("status"), "disabled")


if __name__ == "__main__":
    unittest.main()

