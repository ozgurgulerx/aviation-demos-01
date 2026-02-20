#!/usr/bin/env python3
"""
LLM-based query writers (Text2SQL / Text2KQL).
"""

from __future__ import annotations

import json
import os
import re
from typing import Any, Dict, Optional

from azure_openai_client import get_shared_client
from shared_utils import OPENAI_API_VERSION, supports_explicit_temperature as _supports_explicit_temperature


def _init_client():
    client, _ = get_shared_client(api_version=OPENAI_API_VERSION)
    return client


def _strip_fences(text: str) -> str:
    out = re.sub(r"^```(?:sql|kql|json)?\s*", "", text.strip(), flags=re.IGNORECASE)
    out = re.sub(r"\s*```$", "", out)
    return out.strip()


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
