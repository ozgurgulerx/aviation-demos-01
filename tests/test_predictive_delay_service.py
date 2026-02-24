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
    class _FakeCursor:
        def __init__(self, fetchall_rows=None, fetchone_row=None):
            self.fetchall_rows = fetchall_rows if fetchall_rows is not None else []
            self.fetchone_row = fetchone_row
            self.queries = []

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, query, params=None):
            self.queries.append((query, list(params) if params is not None else []))

        def fetchall(self):
            return self.fetchall_rows

        def fetchone(self):
            return self.fetchone_row

    class _FakeConn:
        def __init__(self, cursor):
            self._cursor = cursor
            self.autocommit = True

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def cursor(self, cursor_factory=None):
            return self._cursor

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

    def test_delay_query_uses_forward_window_filter(self):
        svc = PredictiveDelayService(enabled=True)
        cursor = self._FakeCursor(fetchall_rows=[])
        conn = self._FakeConn(cursor)
        with patch.object(PredictiveDelayService, "_connect", return_value=conn), patch.object(
            PredictiveDelayService,
            "_table_exists",
            return_value=True,
        ), patch.object(
            PredictiveDelayService,
            "_table_columns",
            return_value=["model_variant", "std_utc", "risk_a15", "flight_leg_id", "flight_number"],
        ):
            payload = svc.get_delays(model="optimized", window_hours=6, limit=10)

        self.assertEqual(payload.get("status"), "empty")
        self.assertGreaterEqual(len(cursor.queries), 1)
        query, _ = cursor.queries[-1]
        self.assertIn('"std_utc" >= NOW()', query)
        self.assertIn('"std_utc" <= NOW() + (%s * INTERVAL \'1 hour\')', query)
        self.assertNotIn("- (%s * INTERVAL '1 hour')", query)

    def test_metrics_preserve_zero_delta(self):
        svc = PredictiveDelayService(enabled=True)
        cursor = self._FakeCursor(
            fetchone_row={
                "auroc_delta": 0.0,
                "brier_delta": 0.0,
                "mae_delta": 0.0,
                "as_of_utc": "2026-02-24T00:00:00Z",
            }
        )
        conn = self._FakeConn(cursor)
        with patch.object(PredictiveDelayService, "_connect", return_value=conn), patch.object(
            PredictiveDelayService,
            "_table_exists",
            return_value=True,
        ), patch.object(
            PredictiveDelayService,
            "_table_columns",
            return_value=["as_of_utc", "auroc_delta", "brier_delta", "mae_delta"],
        ):
            payload = svc.get_delay_metrics()

        self.assertEqual(payload.get("status"), "ok")
        self.assertEqual(payload.get("uplift", {}).get("auroc_delta"), 0.0)
        self.assertEqual(payload.get("uplift", {}).get("brier_delta"), 0.0)
        self.assertEqual(payload.get("uplift", {}).get("mae_delta"), 0.0)


if __name__ == "__main__":
    unittest.main()
