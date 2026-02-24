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
- If hint_tables is provided in constraints, PREFER those tables for the query.
- Prefer simple SELECTs with WHERE filters and LIMIT.
- If a requested column is not present in sql_schema, do not guess.
- If needed columns are missing, output exactly:
-- NEED_SCHEMA: <what is missing>
- Never generate INSERT/UPDATE/DELETE/DDL.
- IMPORTANT: Many tables (especially demo.* and ops_*) store ALL columns as TEXT. When sql_schema shows columns as type "text" that are semantically numeric or timestamps:
  * Cast timestamp columns (ending in _utc) via column::timestamptz before date/time comparisons or NOW().
  * Cast numeric columns (dep_delay_min, arr_delay_min, cumulative_duty_hours, legality_risk_flag, bag_count, distance_nm, passengers, deferred_flag) via column::numeric or column::integer before arithmetic, aggregation (SUM, AVG, MIN, MAX), or comparison.
  * Example: AVG(dep_delay_min::numeric), SUM(legality_risk_flag::integer), WHERE duty_end_utc::timestamptz >= NOW()
- If constraints include a casting_hint, follow it to add explicit CAST or :: operators.
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
- Start with a valid table reference (or let-binding followed by a table).
- Do not emit semicolons except required let-binding terminators.
- Do not use unsupported functions (for example: time_now()).
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
