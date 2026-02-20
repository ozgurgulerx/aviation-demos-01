#!/usr/bin/env python3
"""
LLM-based query writers (Text2SQL / Text2KQL).
"""

from __future__ import annotations

import json
import os
import re
from typing import Any, Dict, Optional

from azure.identity import DefaultAzureCredential, get_bearer_token_provider
from dotenv import load_dotenv
from openai import AzureOpenAI

load_dotenv()


def _client_tuning_kwargs() -> dict:
    try:
        timeout_seconds = float(os.getenv("AZURE_OPENAI_TIMEOUT_SECONDS", "45"))
    except Exception:
        timeout_seconds = 45.0
    try:
        max_retries = max(0, int(os.getenv("AZURE_OPENAI_MAX_RETRIES", "1")))
    except Exception:
        max_retries = 1
    return {"timeout": timeout_seconds, "max_retries": max_retries}


def _init_client() -> AzureOpenAI:
    endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
    api_key = os.getenv("AZURE_OPENAI_API_KEY")
    if api_key:
        return AzureOpenAI(
            azure_endpoint=endpoint,
            api_key=api_key,
            api_version="2024-06-01",
            **_client_tuning_kwargs(),
        )
    credential = DefaultAzureCredential()
    token_provider = get_bearer_token_provider(credential, "https://cognitiveservices.azure.com/.default")
    return AzureOpenAI(
        azure_endpoint=endpoint,
        azure_ad_token_provider=token_provider,
        api_version="2024-06-01",
        **_client_tuning_kwargs(),
    )


def _strip_fences(text: str) -> str:
    out = re.sub(r"^```(?:sql|kql|json)?\s*", "", text.strip(), flags=re.IGNORECASE)
    out = re.sub(r"\s*```$", "", out)
    return out.strip()


def _supports_explicit_temperature(model_name: str) -> bool:
    """GPT-5/o-series deployments reject explicit temperature overrides."""
    model = (model_name or "").strip().lower()
    return not (
        model.startswith("gpt-5")
        or model.startswith("o1")
        or model.startswith("o3")
        or model.startswith("o4")
        or model == "model-router"
    )


class SQLWriter:
    def __init__(self, model: Optional[str] = None):
        self.client = _init_client()
        self.model = (
            model
            or os.getenv("AZURE_OPENAI_WORKER_DEPLOYMENT_NAME")
            or os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME", "aviation-chat-gpt5-mini")
        )

    def generate(
        self,
        user_query: str,
        evidence_type: str,
        sql_schema: Dict[str, Any],
        entities: Dict[str, Any],
        time_window: Dict[str, Any],
        constraints: Optional[Dict[str, Any]] = None,
    ) -> str:
        prompt = """You are SQL_WRITER. Output SQL ONLY.

Rules:
- Use only tables/columns provided in sql_schema.
- Prefer simple SELECTs with WHERE filters and LIMIT.
- If needed columns are missing, output exactly:
-- NEED_SCHEMA: <what is missing>
- Never generate INSERT/UPDATE/DELETE/DDL.
"""
        payload = {
            "user_query": user_query,
            "evidence_type": evidence_type,
            "sql_schema": sql_schema,
            "entities": entities,
            "time_window": time_window,
            "constraints": constraints or {},
        }
        request_kwargs = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": prompt},
                {"role": "user", "content": json.dumps(payload, ensure_ascii=True)},
            ],
        }
        if _supports_explicit_temperature(self.model):
            request_kwargs["temperature"] = 0

        response = self.client.chat.completions.create(
            **request_kwargs,
        )
        return _strip_fences(response.choices[0].message.content or "")


class KQLWriter:
    def __init__(self, model: Optional[str] = None):
        self.client = _init_client()
        self.model = (
            model
            or os.getenv("AZURE_OPENAI_WORKER_DEPLOYMENT_NAME")
            or os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME", "aviation-chat-gpt5-mini")
        )

    def generate(
        self,
        user_query: str,
        evidence_type: str,
        kql_schema: Dict[str, Any],
        entities: Dict[str, Any],
        time_window: Dict[str, Any],
        constraints: Optional[Dict[str, Any]] = None,
    ) -> str:
        prompt = """You are KQL_WRITER. Output KQL ONLY.

Rules:
- Use only tables/columns provided in kql_schema.
- Always include a time filter using the horizon.
- If needed columns are missing, output exactly:
// NEED_SCHEMA: <what is missing>
- Never invent table names.
"""
        payload = {
            "user_query": user_query,
            "evidence_type": evidence_type,
            "kql_schema": kql_schema,
            "entities": entities,
            "time_window": time_window,
            "constraints": constraints or {},
        }
        request_kwargs = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": prompt},
                {"role": "user", "content": json.dumps(payload, ensure_ascii=True)},
            ],
        }
        if _supports_explicit_temperature(self.model):
            request_kwargs["temperature"] = 0

        response = self.client.chat.completions.create(**request_kwargs)
        return _strip_fences(response.choices[0].message.content or "")
