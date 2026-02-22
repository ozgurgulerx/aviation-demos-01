#!/usr/bin/env python3
"""
Schema snapshots for SQL/KQL/Graph routing.
"""

from __future__ import annotations

import json
import os
import time
from typing import Any, Dict, List

from unified_retriever import UnifiedRetriever

KQL_SCHEMA_JSON = os.getenv("FABRIC_KQL_SCHEMA_JSON", "").strip()


class SchemaProvider:
    def __init__(self, retriever: UnifiedRetriever):
        self.retriever = retriever
        try:
            ttl = int(os.getenv("SCHEMA_CACHE_TTL_SECONDS", "300") or "300")
        except Exception:
            ttl = 300
        self.cache_ttl_seconds = max(0, ttl)
        self._cached_snapshot: Dict[str, Any] = {}
        self._cache_expires_at: float = 0.0

    def snapshot(self) -> Dict[str, Any]:
        now = time.time()
        if self._cached_snapshot and now < self._cache_expires_at:
            return self._cached_snapshot

        payload = {
            "sql_schema": self._sql_schema(),
            "kql_schema": self._kql_schema(),
            "graph_schema": self._graph_schema(),
        }
        self._cached_snapshot = payload
        self._cache_expires_at = now + self.cache_ttl_seconds
        return payload

    def _sql_schema(self) -> Dict[str, Any]:
        return self.retriever.current_sql_schema()

    def _parse_kql_show_schema(self, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        if not rows:
            return {"tables": []}

        table_map: Dict[str, List[Dict[str, str]]] = {}
        for row in rows:
            if not isinstance(row, dict):
                continue
            table = str(row.get("TableName") or row.get("table_name") or row.get("Name") or "").strip()
            column = str(row.get("ColumnName") or row.get("column_name") or "").strip()
            column_type = str(row.get("ColumnType") or row.get("column_type") or row.get("Type") or "string").strip()
            if not table or not column:
                continue
            table_map.setdefault(table, [])
            if not any(c["name"] == column for c in table_map[table]):
                table_map[table].append({"name": column, "type": column_type})

        tables = [{"table": t, "columns": cols} for t, cols in sorted(table_map.items(), key=lambda x: x[0])]
        return {"tables": tables}

    def _kql_schema(self) -> Dict[str, Any]:
        kql_schema_mode = os.getenv("KQL_SCHEMA_MODE", "static").strip().lower()
        endpoint = os.getenv("FABRIC_KQL_ENDPOINT", "").strip()
        database = os.getenv("FABRIC_KQL_DATABASE", "aviation_ops")

        if kql_schema_mode == "live" and endpoint:
            if self.retriever._is_kusto_endpoint(endpoint):
                rows, error = self.retriever._kusto_rows(endpoint, ".show database schema")
                if rows:
                    parsed = self._parse_kql_show_schema(rows)
                    parsed.update(
                        {
                            "database": database,
                            "source": "live",
                            "collected_at": self.retriever._now_iso(),
                            "schema_version": f"tables:{len(parsed.get('tables', []))}",
                        }
                    )
                    return parsed
                return {
                    "database": database,
                    "source": "live-error",
                    "collected_at": self.retriever._now_iso(),
                    "schema_version": "error",
                    "error": error or "unable_to_fetch_kql_schema",
                    "tables": [],
                }

        if KQL_SCHEMA_JSON:
            try:
                parsed = json.loads(KQL_SCHEMA_JSON)
                if isinstance(parsed, dict):
                    parsed.setdefault("database", database)
                    parsed.setdefault("source", "static-json")
                    parsed.setdefault("collected_at", self.retriever._now_iso())
                    parsed.setdefault("schema_version", "static-json")
                    parsed.setdefault("tables", [])
                    return parsed
            except Exception:
                pass
        # Compact default schema for prompt guidance.
        # Must match actual Kusto tables created by scripts/10_push_to_kusto.py.
        return {
            "database": database,
            "source": "static-default",
            "collected_at": self.retriever._now_iso(),
            "schema_version": "static-default-v2",
            "tables": [
                {
                    "table": "opensky_states",
                    "columns": [
                        "icao24", "callsign", "origin_country", "time_position",
                        "last_contact", "longitude", "latitude", "baro_altitude",
                        "on_ground", "velocity", "true_track", "vertical_rate",
                        "geo_altitude", "squawk", "position_source",
                    ],
                },
                {
                    "table": "hazards_airsigmets",
                    "columns": [
                        "raw_text", "valid_time_from", "valid_time_to", "points",
                        "min_ft_msl", "max_ft_msl", "movement_dir_degrees",
                        "movement_speed_kt", "hazard", "severity", "airsigmet_type",
                    ],
                },
                {
                    "table": "hazards_gairmets",
                    "columns": [
                        "receipt_time", "issue_time", "expire_time", "product",
                        "tag", "issue_to_valid_hours", "valid_time", "hazard",
                        "geometry_type", "due_to", "points",
                    ],
                },
                {
                    "table": "hazards_aireps_raw",
                    "columns": ["raw_line"],
                },
                {
                    "table": "ops_graph_edges",
                    "columns": ["src_type", "src_id", "edge_type", "dst_type", "dst_id"],
                },
            ],
        }

    def _graph_schema(self) -> Dict[str, Any]:
        return {
            "source": "builtin-default",
            "collected_at": self.retriever._now_iso(),
            "schema_version": "graph-default-v2",
            "node_types": ["Airport", "FlightLeg", "Tail", "Intent", "EvidenceType", "Tool"],
            "edge_types": ["DEPARTS", "ARRIVES", "OPERATES", "REQUIRES", "AUTHORITATIVE_IN", "EXPANDS_TO"],
            # Aspirational types not yet in data:
            # node: Runway, Station, Alternate
            # edge: HAS_RUNWAY, HAS_STATION, HAS_ALTERNATE
        }
