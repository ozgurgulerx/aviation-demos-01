#!/usr/bin/env python3
"""Tests for predictive Flask API routes."""

import sys
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import api_server  # noqa: E402


class _StubPredictiveService:
    def get_delays(self, model="optimized", window_hours=6, limit=100):
        return {
            "status": "ok",
            "enabled": True,
            "model": model,
            "window_hours": window_hours,
            "row_count": 1,
            "rows": [{"flight_leg_id": "LEG001", "flight_number": "TK123"}],
        }

    def get_delay_metrics(self):
        return {
            "status": "ok",
            "enabled": True,
            "baseline": {"auroc": 0.61},
            "optimized": {"auroc": 0.72},
            "uplift": {"auroc_delta": 0.11},
        }

    def get_actions(self, model="optimized", limit=100):
        return {
            "status": "ok",
            "enabled": True,
            "model": model,
            "actions": [{"flight_leg_id": "LEG001", "action_label": "Gate resequence"}],
        }

    def get_decision_metrics(self):
        return {"status": "ok", "enabled": True, "metrics": {"total_decisions": 4}}


class PredictiveApiRouteTests(unittest.TestCase):
    def setUp(self):
        self.client = api_server.app.test_client()

    def test_predictive_delays_rejects_invalid_model(self):
        response = self.client.get("/api/predictive/delays?model=bad")
        self.assertEqual(response.status_code, 400)
        payload = response.get_json()
        self.assertIn("error", payload)

    def test_predictive_delays_returns_payload(self):
        with patch.object(api_server, "get_predictive_service", return_value=_StubPredictiveService()):
            response = self.client.get("/api/predictive/delays?model=optimized&windowHours=12&limit=25")
        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload.get("status"), "ok")
        self.assertEqual(payload.get("model"), "optimized")
        self.assertEqual(payload.get("window_hours"), 12)
        self.assertEqual(len(payload.get("rows", [])), 1)

    def test_predictive_metrics_returns_payload(self):
        with patch.object(api_server, "get_predictive_service", return_value=_StubPredictiveService()):
            response = self.client.get("/api/predictive/delay-metrics")
        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload.get("status"), "ok")
        self.assertIn("baseline", payload)
        self.assertIn("optimized", payload)

    def test_predictive_actions_returns_payload(self):
        with patch.object(api_server, "get_predictive_service", return_value=_StubPredictiveService()):
            response = self.client.get("/api/predictive/actions?model=baseline&limit=10")
        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload.get("status"), "ok")
        self.assertEqual(payload.get("model"), "baseline")
        self.assertEqual(len(payload.get("actions", [])), 1)

    def test_predictive_decision_metrics_returns_payload(self):
        with patch.object(api_server, "get_predictive_service", return_value=_StubPredictiveService()):
            response = self.client.get("/api/predictive/decision-metrics")
        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload.get("status"), "ok")
        self.assertEqual(payload.get("metrics", {}).get("total_decisions"), 4)


if __name__ == "__main__":
    unittest.main()

