#!/usr/bin/env python3
"""
Schema snapshots for SQL/KQL/Graph routing.
"""

from __future__ import annotations

import json
import os
from typing import Any, Dict, List

from unified_retriever import UnifiedRetriever

KQL_SCHEMA_JSON = os.getenv("FABRIC_KQL_SCHEMA_JSON", "").strip()


class SchemaProvider:
    def __init__(self, retriever: UnifiedRetriever):
        self.retriever = retriever

    def snapshot(self) -> Dict[str, Any]:
        return {
            "sql_schema": self._sql_schema(),
            "kql_schema": self._kql_schema(),
            "graph_schema": self._graph_schema(),
        }

    def _sql_schema(self) -> Dict[str, Any]:
        tables: List[Dict[str, Any]] = []
        try:
            cur = self.retriever.db.cursor()
            if self.retriever.use_postgres:
                cur.execute(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema='public'
                    ORDER BY table_name
                    """
                )
                table_names = [str(row[0]) for row in cur.fetchall()]
                for table in table_names:
                    cur.execute(
                        """
                        SELECT column_name, data_type
                        FROM information_schema.columns
                        WHERE table_schema='public' AND table_name=%s
                        ORDER BY ordinal_position
                        """,
                        (table,),
                    )
                    cols = [{"name": str(r[0]), "type": str(r[1])} for r in cur.fetchall()]
                    tables.append({"table": table, "columns": cols})
            else:
                cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
                table_names = [str(row[0]) for row in cur.fetchall()]
                for table in table_names:
                    cur.execute(f"PRAGMA table_info('{table}')")
                    cols = [{"name": str(r[1]), "type": str(r[2])} for r in cur.fetchall()]
                    tables.append({"table": table, "columns": cols})
        except Exception as exc:
            return {"error": str(exc), "tables": []}
        return {"tables": tables}

    def _kql_schema(self) -> Dict[str, Any]:
        if KQL_SCHEMA_JSON:
            try:
                return json.loads(KQL_SCHEMA_JSON)
            except Exception:
                pass
        # Compact default schema for prompt guidance.
        return {
            "database": os.getenv("FABRIC_KQL_DATABASE", "aviation_ops"),
            "tables": [
                {
                    "table": "weather_obs",
                    "columns": ["timestamp", "station_id", "icao", "metar_raw", "wind_kt", "visibility_mi"],
                },
                {
                    "table": "hazards",
                    "columns": ["timestamp", "hazard_type", "location", "severity", "raw_payload"],
                },
                {
                    "table": "opensky_states",
                    "columns": ["timestamp", "icao24", "callsign", "lat", "lon", "velocity", "origin_country"],
                },
            ],
        }

    def _graph_schema(self) -> Dict[str, Any]:
        return {
            "node_types": ["Intent", "EvidenceType", "Tool", "Airport", "Runway", "Station", "Alternate"],
            "edge_types": ["REQUIRES", "AUTHORITATIVE_IN", "EXPANDS_TO", "HAS_RUNWAY", "HAS_STATION", "HAS_ALTERNATE"],
        }

