#!/usr/bin/env python3
"""Shared Azure OpenAI client initialization helpers."""

from __future__ import annotations

import logging
import os
import threading
from typing import Dict, Optional, Tuple

from azure.identity import DefaultAzureCredential, get_bearer_token_provider
from openai import AzureOpenAI

logger = logging.getLogger(__name__)

AZURE_OPENAI_SCOPE = "https://cognitiveservices.azure.com/.default"


def client_tuning_kwargs() -> Dict[str, float | int]:
    try:
        timeout_seconds = float(os.getenv("AZURE_OPENAI_TIMEOUT_SECONDS", "45"))
    except Exception:
        timeout_seconds = 45.0
    try:
        max_retries = max(0, int(os.getenv("AZURE_OPENAI_MAX_RETRIES", "1")))
    except Exception:
        max_retries = 1
    return {"timeout": timeout_seconds, "max_retries": max_retries}


def _auth_mode() -> str:
    return (os.getenv("AZURE_OPENAI_AUTH_MODE", "auto") or "auto").strip().lower()


def _build_credential() -> DefaultAzureCredential:
    managed_identity_client_id = os.getenv("AZURE_OPENAI_MANAGED_IDENTITY_CLIENT_ID", "").strip() or None

    kwargs: Dict[str, str] = {}
    if managed_identity_client_id:
        kwargs["managed_identity_client_id"] = managed_identity_client_id
    return DefaultAzureCredential(**kwargs)


def _token_client(endpoint: str, api_version: str, credential: Optional[DefaultAzureCredential] = None) -> AzureOpenAI:
    cred = credential or _build_credential()
    token_provider = get_bearer_token_provider(cred, AZURE_OPENAI_SCOPE)
    return AzureOpenAI(
        azure_endpoint=endpoint,
        azure_ad_token_provider=token_provider,
        api_version=api_version,
        **client_tuning_kwargs(),
    )


def _api_key_client(endpoint: str, api_version: str, api_key: str) -> AzureOpenAI:
    return AzureOpenAI(
        azure_endpoint=endpoint,
        api_key=api_key,
        api_version=api_version,
        **client_tuning_kwargs(),
    )


def init_azure_openai_client(
    *,
    endpoint: Optional[str] = None,
    api_version: Optional[str] = None,
) -> Tuple[AzureOpenAI, str]:
    """Build Azure OpenAI client with auth-mode aware behavior.

    Modes:
    - token: always Entra token auth
    - api-key: always API key auth
    - auto: prefer token when available, fallback to API key
    """

    resolved_endpoint = (endpoint or os.getenv("AZURE_OPENAI_ENDPOINT", "")).strip()
    if not resolved_endpoint:
        raise ValueError("Missing AZURE_OPENAI_ENDPOINT")
    resolved_api_version = (api_version or os.getenv("AZURE_OPENAI_API_VERSION", "2024-06-01")).strip() or "2024-06-01"
    api_key = os.getenv("AZURE_OPENAI_API_KEY", "").strip()
    mode = _auth_mode()

    if mode == "api-key":
        if not api_key:
            raise ValueError("AZURE_OPENAI_AUTH_MODE=api-key requires AZURE_OPENAI_API_KEY")
        return _api_key_client(resolved_endpoint, resolved_api_version, api_key), "api-key"

    credential = _build_credential()
    if mode == "token":
        return _token_client(resolved_endpoint, resolved_api_version, credential), "token"

    # auto mode: prefer token when a token can be acquired, else fallback to API key.
    try:
        credential.get_token(AZURE_OPENAI_SCOPE)
        return _token_client(resolved_endpoint, resolved_api_version, credential), "token"
    except Exception:
        if api_key:
            return _api_key_client(resolved_endpoint, resolved_api_version, api_key), "api-key"
        return _token_client(resolved_endpoint, resolved_api_version, credential), "token"


# ---------------------------------------------------------------------------
# Shared singleton client — reuses TCP/TLS connections across all callers
# ---------------------------------------------------------------------------

_shared_client: Optional[AzureOpenAI] = None
_shared_auth_mode: str = ""
_shared_lock = threading.Lock()


def get_shared_client(
    *,
    api_version: Optional[str] = None,
) -> Tuple[AzureOpenAI, str]:
    """Return a process-wide shared AzureOpenAI client (thread-safe lazy init).

    The shared client uses the default endpoint and auth mode from env vars,
    identical to what ``init_azure_openai_client()`` produces.  All callers
    still provide their own ``model`` / deployment name per-request.
    """
    global _shared_client, _shared_auth_mode

    if _shared_client is not None:
        return _shared_client, _shared_auth_mode

    with _shared_lock:
        # Double-checked locking.
        if _shared_client is not None:
            return _shared_client, _shared_auth_mode

        client, auth_mode = init_azure_openai_client(api_version=api_version)
        _shared_client = client
        _shared_auth_mode = auth_mode
        logger.info("Shared AzureOpenAI client initialized (auth=%s)", auth_mode)
        return _shared_client, _shared_auth_mode
