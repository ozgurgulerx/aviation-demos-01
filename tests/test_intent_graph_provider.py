import base64
import json
import os
import sys
import time
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import intent_graph_provider as igp  # noqa: E402


class IntentGraphProviderTokenFallbackTests(unittest.TestCase):
    def setUp(self):
        igp._fabric_token_cache.clear()

    def tearDown(self):
        igp._fabric_token_cache.clear()

    def _fake_jwt(self, ttl_seconds: int) -> str:
        header = base64.urlsafe_b64encode(
            json.dumps({"alg": "none", "typ": "JWT"}).encode("utf-8")
        ).decode("utf-8").rstrip("=")
        payload = base64.urlsafe_b64encode(
            json.dumps({"exp": int(time.time()) + int(ttl_seconds)}).encode("utf-8")
        ).decode("utf-8").rstrip("=")
        return f"{header}.{payload}.sig"

    def test_returns_cached_token_when_refresh_fails_and_cached_is_fresh(self):
        scope = igp._FABRIC_DEFAULT_SCOPE
        cached = self._fake_jwt(600)
        igp._fabric_token_cache[scope] = {
            "token": cached,
            "expires_at": time.time() + 600,
        }

        with patch.object(igp, "FABRIC_GRAPH_ENDPOINT", ""), patch.dict(
            os.environ,
            {
                "FABRIC_CLIENT_ID": "client-id",
                "FABRIC_CLIENT_SECRET": "client-secret",
                "FABRIC_TENANT_ID": "tenant-id",
                "FABRIC_TOKEN_MIN_TTL_SECONDS": "120",
                "ALLOW_STATIC_FABRIC_BEARER": "false",
                "FABRIC_BEARER_TOKEN": "",
            },
            clear=False,
        ), patch.object(igp.urllib.request, "urlopen", side_effect=RuntimeError("refresh_failed")):
            token = igp._get_fabric_bearer_token()

        self.assertEqual(token, cached)

    def test_does_not_return_expired_cached_token_after_refresh_failure(self):
        scope = igp._FABRIC_DEFAULT_SCOPE
        cached = self._fake_jwt(-10)
        igp._fabric_token_cache[scope] = {
            "token": cached,
            "expires_at": time.time() - 10,
        }

        with patch.object(igp, "FABRIC_GRAPH_ENDPOINT", ""), patch.dict(
            os.environ,
            {
                "FABRIC_CLIENT_ID": "client-id",
                "FABRIC_CLIENT_SECRET": "client-secret",
                "FABRIC_TENANT_ID": "tenant-id",
                "FABRIC_TOKEN_MIN_TTL_SECONDS": "120",
                "ALLOW_STATIC_FABRIC_BEARER": "false",
                "FABRIC_BEARER_TOKEN": "",
            },
            clear=False,
        ), patch.object(igp.urllib.request, "urlopen", side_effect=RuntimeError("refresh_failed")):
            token = igp._get_fabric_bearer_token()

        self.assertEqual(token, "")
        self.assertNotIn(scope, igp._fabric_token_cache)

    def test_falls_back_to_static_token_when_cached_is_expired_and_static_allowed(self):
        scope = igp._FABRIC_DEFAULT_SCOPE
        cached = self._fake_jwt(-10)
        static = self._fake_jwt(600)
        igp._fabric_token_cache[scope] = {
            "token": cached,
            "expires_at": time.time() - 10,
        }

        with patch.object(igp, "FABRIC_GRAPH_ENDPOINT", ""), patch.dict(
            os.environ,
            {
                "FABRIC_CLIENT_ID": "client-id",
                "FABRIC_CLIENT_SECRET": "client-secret",
                "FABRIC_TENANT_ID": "tenant-id",
                "FABRIC_TOKEN_MIN_TTL_SECONDS": "120",
                "ALLOW_STATIC_FABRIC_BEARER": "true",
                "FABRIC_BEARER_TOKEN": static,
            },
            clear=False,
        ), patch.object(igp.urllib.request, "urlopen", side_effect=RuntimeError("refresh_failed")):
            token = igp._get_fabric_bearer_token()

        self.assertEqual(token, static)
        self.assertNotIn(scope, igp._fabric_token_cache)

    def test_does_not_fallback_to_stale_static_token(self):
        scope = igp._FABRIC_DEFAULT_SCOPE
        cached = self._fake_jwt(-10)
        static = self._fake_jwt(10)
        igp._fabric_token_cache[scope] = {
            "token": cached,
            "expires_at": time.time() - 10,
        }

        with patch.object(igp, "FABRIC_GRAPH_ENDPOINT", ""), patch.dict(
            os.environ,
            {
                "FABRIC_CLIENT_ID": "client-id",
                "FABRIC_CLIENT_SECRET": "client-secret",
                "FABRIC_TENANT_ID": "tenant-id",
                "FABRIC_TOKEN_MIN_TTL_SECONDS": "120",
                "ALLOW_STATIC_FABRIC_BEARER": "true",
                "FABRIC_BEARER_TOKEN": static,
            },
            clear=False,
        ), patch.object(igp.urllib.request, "urlopen", side_effect=RuntimeError("refresh_failed")):
            token = igp._get_fabric_bearer_token()

        self.assertEqual(token, "")
        self.assertNotIn(scope, igp._fabric_token_cache)


if __name__ == "__main__":
    unittest.main()
