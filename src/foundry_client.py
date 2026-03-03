#!/usr/bin/env python3
"""Azure AI Foundry Responses API client for Foundry IQ retrieval mode.

Calls the Responses API on an AIServices endpoint, letting the portal-managed
agent (with AI Search + Fabric Data Agent tools) handle retrieval and synthesis.
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List

import requests

logger = logging.getLogger(__name__)

_AGENT_INFO_PATH = os.path.join(
    os.path.dirname(__file__), "..", "scripts", ".foundry-agent-info.json"
)

_DEFAULT_INSTRUCTIONS = (
    "You are an aviation intelligence analyst. Answer questions about aviation "
    "safety incidents, ASRS reports, NOTAMs, airport operations, and regulatory "
    "documents using the tools available to you. Cite your sources with numbered "
    "references like [1], [2]. Be concise and factual."
)


@dataclass
class FoundryCitation:
    """A citation reference parsed from Foundry agent response text."""

    id: int
    excerpt: str
    source: str = "Foundry IQ"


@dataclass
class FoundryResponse:
    """Parsed response from the Foundry Responses API."""

    text: str
    citations: List[FoundryCitation] = field(default_factory=list)
    model: str = ""
    latency_ms: float = 0.0


def _load_agent_info() -> Dict[str, Any]:
    """Load agent config from scripts/.foundry-agent-info.json if it exists."""
    try:
        with open(_AGENT_INFO_PATH, "r") as f:
            return json.load(f)
    except Exception:
        return {}


def _parse_citations_from_text(text: str) -> List[FoundryCitation]:
    """Extract [N] citation markers and their surrounding context from text."""
    citations: List[FoundryCitation] = []
    seen_ids: set[int] = set()

    for match in re.finditer(r"\[(\d+)\]", text):
        cit_id = int(match.group(1))
        if cit_id in seen_ids:
            continue
        seen_ids.add(cit_id)

        # Extract surrounding sentence for excerpt (~120 chars each side).
        start = max(0, match.start() - 120)
        end = min(len(text), match.end() + 120)
        excerpt = text[start:end].strip()

        citations.append(FoundryCitation(id=cit_id, excerpt=excerpt))

    return citations


class FoundryClient:
    """Client for the Azure AI Foundry Responses API."""

    def __init__(self) -> None:
        agent_info = _load_agent_info()

        self._endpoint = (
            os.getenv("FOUNDRY_AISERVICES_ENDPOINT", "").strip()
            or os.getenv("AZURE_FOUNDRY_PROJECT_ENDPOINT", "").strip().rsplit("/api/projects", 1)[0]
            or agent_info.get("endpoint", "").rsplit("/api/projects", 1)[0]
        )
        self._model = (
            os.getenv("FOUNDRY_MODEL", "").strip()
            or agent_info.get("model", "gpt-5-mini")
        )
        self._api_version = (
            os.getenv("FOUNDRY_API_VERSION", "").strip()
            or "2025-03-01-preview"
        )
        self._instructions = (
            os.getenv("FOUNDRY_AGENT_INSTRUCTIONS", "").strip()
            or _DEFAULT_INSTRUCTIONS
        )
        try:
            self._timeout = int(os.getenv("FOUNDRY_TIMEOUT_SECONDS", "60"))
        except ValueError:
            self._timeout = 60

        if not self._endpoint:
            logger.warning(
                "FoundryClient: no endpoint configured "
                "(set FOUNDRY_AISERVICES_ENDPOINT or provide scripts/.foundry-agent-info.json)"
            )

        logger.info(
            "FoundryClient initialized: endpoint=%s model=%s api_version=%s timeout=%ds",
            self._endpoint,
            self._model,
            self._api_version,
            self._timeout,
        )

    @property
    def model(self) -> str:
        return self._model

    @property
    def endpoint(self) -> str:
        return self._endpoint

    def _get_token(self) -> str:
        """Acquire an AAD bearer token for the Cognitive Services scope."""
        from azure_openai_client import AZURE_OPENAI_SCOPE, _build_credential

        credential = _build_credential()
        token = credential.get_token(AZURE_OPENAI_SCOPE)
        return token.token

    def query(self, user_query: str) -> FoundryResponse:
        """Send a query to the Foundry Responses API and return the parsed response."""
        if not self._endpoint:
            raise RuntimeError(
                "FoundryClient endpoint not configured. "
                "Set FOUNDRY_AISERVICES_ENDPOINT or provide scripts/.foundry-agent-info.json."
            )

        url = f"{self._endpoint.rstrip('/')}/openai/responses?api-version={self._api_version}"
        body = {
            "model": self._model,
            "input": user_query,
            "instructions": self._instructions,
        }
        headers = {
            "Authorization": f"Bearer {self._get_token()}",
            "Content-Type": "application/json",
        }

        logger.info("FoundryClient.query: POST %s model=%s", url, self._model)
        t0 = time.perf_counter()

        resp = requests.post(url, headers=headers, json=body, timeout=self._timeout)
        latency_ms = (time.perf_counter() - t0) * 1000

        if resp.status_code != 200:
            logger.error(
                "Foundry API error: status=%d body=%s",
                resp.status_code,
                resp.text[:500],
            )
            resp.raise_for_status()

        data = resp.json()
        text = self._extract_text(data)
        citations = _parse_citations_from_text(text)
        model = data.get("model", self._model)

        logger.info(
            "FoundryClient.query: done latency=%.0fms citations=%d model=%s",
            latency_ms,
            len(citations),
            model,
        )

        return FoundryResponse(
            text=text,
            citations=citations,
            model=model,
            latency_ms=latency_ms,
        )

    @staticmethod
    def _extract_text(data: Dict[str, Any]) -> str:
        """Extract answer text from the Responses API output structure."""
        # Standard structure: output[] -> content[] -> text (type=output_text)
        for output_item in data.get("output", []):
            if output_item.get("type") == "message":
                for content_block in output_item.get("content", []):
                    if content_block.get("type") == "output_text":
                        return content_block.get("text", "")

        # Fallback: try top-level output_text
        if isinstance(data.get("output"), list):
            for item in data["output"]:
                if item.get("type") == "output_text":
                    return item.get("text", "")

        # Last resort: stringify
        logger.warning("FoundryClient: unexpected response structure, keys=%s", list(data.keys()))
        return data.get("output_text", "") or str(data.get("output", ""))
