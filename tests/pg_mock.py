"""
Shared mock psycopg2 pool/connection/cursor for tests.

Simulates a PostgreSQL backend with pre-canned ASRS sample data so tests
can exercise SQL validation, schema introspection, query execution, and
citation generation without a live database.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple
from unittest.mock import MagicMock, patch

# ---------------------------------------------------------------------------
# Sample data
# ---------------------------------------------------------------------------

SAMPLE_REPORTS: List[Dict[str, Any]] = [
    {
        "asrs_report_id": f"ASRS-{100000 + i}",
        "event_date": f"2025-{((i % 12) + 1):02d}-15",
        "location": ["JFK, NY", "LAX, CA", "ORD, IL", "ATL, GA", "DFW, TX",
                      "SFO, CA", "MIA, FL", "BOS, MA", "SEA, WA", "DEN, CO"][i % 10],
        "aircraft_type": ["B737-800", "A320-200", "B777-300ER", "E175", "CRJ-900",
                          "B737-800", "A321neo", "B787-9", "A350-900", "B737 MAX 8"][i % 10],
        "flight_phase": ["Initial Climb", "Cruise", "Approach", "Landing", "Taxi",
                         "Takeoff", "Descent", "Go Around", "Initial Climb", "Cruise"][i % 10],
        "narrative_type": ["Narrative 1", "Narrative 2", "Callback"][i % 3],
        "title": f"ASRS | 2025-{((i % 12) + 1):02d}-15 | B737-800 | JFK",
        "report_text": f"Sample report text for report {100000 + i}. "
                        f"The aircraft experienced turbulence during the flight phase.",
        "raw_json": f'{{"asrs_report_id":"ASRS-{100000 + i}","source":"mock"}}',
        "ingested_at": "2025-12-01T00:00:00+00:00",
    }
    for i in range(50)
]

SAMPLE_RUNS: List[Dict[str, Any]] = [
    {
        "run_id": "asrs-load-20251201T000000Z-abcd1234",
        "started_at": "2025-12-01T00:00:00+00:00",
        "completed_at": "2025-12-01T00:01:00+00:00",
        "status": "success",
        "source_manifest_path": "/data/manifest.json",
        "records_seen": 50,
        "records_loaded": 50,
        "records_failed": 0,
    }
]

# Schema metadata (mirrors information_schema)
TABLE_SCHEMAS: Dict[str, List[Dict[str, str]]] = {
    "asrs_reports": [
        {"name": "asrs_report_id", "type": "text"},
        {"name": "event_date", "type": "date"},
        {"name": "location", "type": "text"},
        {"name": "aircraft_type", "type": "text"},
        {"name": "flight_phase", "type": "text"},
        {"name": "narrative_type", "type": "text"},
        {"name": "title", "type": "text"},
        {"name": "report_text", "type": "text"},
        {"name": "raw_json", "type": "text"},
        {"name": "ingested_at", "type": "timestamp with time zone"},
    ],
    "asrs_ingestion_runs": [
        {"name": "run_id", "type": "text"},
        {"name": "started_at", "type": "timestamp with time zone"},
        {"name": "completed_at", "type": "timestamp with time zone"},
        {"name": "status", "type": "text"},
        {"name": "source_manifest_path", "type": "text"},
        {"name": "records_seen", "type": "integer"},
        {"name": "records_loaded", "type": "integer"},
        {"name": "records_failed", "type": "integer"},
    ],
}


# ---------------------------------------------------------------------------
# Mock cursor — dispatches pre-canned results based on SQL pattern matching
# ---------------------------------------------------------------------------


class MockCursor:
    """Simulates a psycopg2 cursor with pre-canned responses."""

    def __init__(self, *, empty: bool = False):
        self._rows: List[Tuple] = []
        self._columns: List[str] = []
        self._empty = empty  # When True, tables exist but have zero rows

    @property
    def description(self) -> Optional[List[Tuple]]:
        if not self._columns:
            return None
        return [(col,) for col in self._columns]

    def execute(self, sql: str, params: Any = None) -> None:
        sql_lower = (sql or "").strip().lower()

        # SET search_path — no-op
        if sql_lower.startswith("set search_path"):
            self._rows = []
            self._columns = []
            return

        # information_schema.tables
        if "information_schema.tables" in sql_lower:
            self._columns = ["table_schema", "table_name"]
            self._rows = [("public", name) for name in TABLE_SCHEMAS]
            return

        # information_schema.columns
        if "information_schema.columns" in sql_lower:
            # Extract table name from params
            table_name = None
            if params and len(params) >= 2:
                table_name = params[1]
            self._columns = ["column_name", "data_type"]
            if table_name and table_name in TABLE_SCHEMAS:
                self._rows = [(c["name"], c["type"]) for c in TABLE_SCHEMAS[table_name]]
            else:
                self._rows = []
            return

        # For data queries, dispatch based on SQL content
        if self._empty:
            self._dispatch_empty(sql, sql_lower)
        else:
            self._dispatch_data(sql, sql_lower)

    def _dispatch_empty(self, sql: str, sql_lower: str) -> None:
        """Handle queries when database has schema but no data."""
        if "count" in sql_lower:
            self._columns = ["cnt"]
            self._rows = [(0,)]
        else:
            self._columns = list(TABLE_SCHEMAS.get("asrs_reports", [{}])[0].keys()) if TABLE_SCHEMAS.get("asrs_reports") else ["asrs_report_id"]
            # Return column names from schema
            cols = [c["name"] for c in TABLE_SCHEMAS.get("asrs_reports", [])]
            self._columns = cols if cols else ["asrs_report_id"]
            self._rows = []

    def _dispatch_data(self, sql: str, sql_lower: str) -> None:
        """Handle queries against sample data."""
        reports = SAMPLE_REPORTS
        runs = SAMPLE_RUNS

        # COUNT(*)
        if re.search(r"select\s+count\s*\(\s*\*\s*\)", sql_lower):
            target_table = "asrs_reports"
            if "asrs_ingestion_runs" in sql_lower:
                target_table = "asrs_ingestion_runs"
            data = reports if target_table == "asrs_reports" else runs
            # Apply WHERE filters approximately
            if "where" in sql_lower:
                data = self._filter_rows(data, sql_lower)
            self._columns = ["cnt"]
            self._rows = [(len(data),)]
            # Check for aliases
            alias_match = re.search(r"count\s*\(\s*\*\s*\)\s+as\s+(\w+)", sql_lower)
            if alias_match:
                self._columns = [alias_match.group(1)]
            return

        # GROUP BY queries
        if "group by" in sql_lower:
            self._handle_group_by(sql, sql_lower, reports)
            return

        # DISTINCT
        if "distinct" in sql_lower:
            col_match = re.search(r"select\s+distinct\s+(\w+)", sql_lower)
            if col_match:
                col = col_match.group(1)
                seen = set()
                self._columns = [col]
                self._rows = []
                for r in reports:
                    val = r.get(col)
                    if val and val not in seen:
                        seen.add(val)
                        self._rows.append((val,))
            return

        # MIN/MAX
        if "min(" in sql_lower or "max(" in sql_lower:
            self._handle_min_max(sql_lower, reports)
            return

        # SELECT * or SELECT specific columns FROM asrs_ingestion_runs
        if "asrs_ingestion_runs" in sql_lower:
            self._columns = list(runs[0].keys()) if runs else []
            self._rows = [tuple(r.values()) for r in runs]
            self._apply_limit(sql_lower)
            return

        # SELECT from asrs_reports
        # Extract column names from SELECT clause
        select_match = re.search(r"select\s+(.*?)\s+from", sql_lower, re.DOTALL)
        if select_match and "*" not in select_match.group(1):
            raw_cols = select_match.group(1)
            # Simple parsing: split by comma, strip aliases
            cols = []
            for part in raw_cols.split(","):
                part = part.strip()
                # Handle "expr AS alias"
                as_match = re.search(r"\bas\s+(\w+)\s*$", part, re.IGNORECASE)
                if as_match:
                    cols.append(as_match.group(1))
                else:
                    # Take last word (column name)
                    words = part.split(".")
                    cols.append(words[-1].strip())
        else:
            cols = list(reports[0].keys()) if reports else []

        filtered = reports
        if "where" in sql_lower:
            filtered = self._filter_rows(filtered, sql_lower)

        self._columns = cols
        self._rows = []
        for r in filtered:
            row_vals = tuple(r.get(c, None) for c in cols)
            self._rows.append(row_vals)

        self._apply_limit(sql_lower)

    def _filter_rows(self, data: List[Dict], sql_lower: str) -> List[Dict]:
        """Very basic WHERE clause filtering."""
        # WHERE asrs_report_id = 'NONEXISTENT_ID_XYZ'
        eq_match = re.search(r"where\s+(\w+)\s*=\s*'([^']*)'", sql_lower)
        if eq_match:
            col, val = eq_match.group(1), eq_match.group(2)
            data = [r for r in data if str(r.get(col, "")).lower() == val.lower()]
        # WHERE col IS NULL
        if "is null" in sql_lower:
            null_match = re.search(r"where\s+(\w+)\s+is\s+null", sql_lower)
            if null_match:
                col = null_match.group(1)
                data = [r for r in data if r.get(col) is None]
        # WHERE lower(col) LIKE '%xxx%'
        like_match = re.search(r"lower\((\w+)\)\s+like\s+'%([^%]*)%'", sql_lower)
        if like_match:
            col, pattern = like_match.group(1), like_match.group(2)
            data = [r for r in data if pattern in str(r.get(col, "")).lower()]
        # WHERE LENGTH(col) = N
        len_match = re.search(r"length\((\w+)\)\s*(=|>|<|>=|<=|!=)\s*(\d+)", sql_lower)
        if len_match:
            col, op, val = len_match.group(1), len_match.group(2), int(len_match.group(3))
            ops = {">": lambda a, b: a > b, ">=": lambda a, b: a >= b,
                   "<": lambda a, b: a < b, "<=": lambda a, b: a <= b,
                   "=": lambda a, b: a == b, "!=": lambda a, b: a != b}
            cmp_fn = ops.get(op, lambda a, b: True)
            data = [r for r in data if cmp_fn(len(str(r.get(col, ""))), val)]
        return data

    def _handle_group_by(self, sql: str, sql_lower: str, reports: List[Dict]) -> None:
        """Handle GROUP BY queries approximately."""
        # Find GROUP BY column
        gb_match = re.search(r"group\s+by\s+(\w+)", sql_lower)
        if not gb_match:
            self._columns = ["error"]
            self._rows = [("group_by_parse_error",)]
            return

        group_col = gb_match.group(1)
        groups: Dict[str, int] = {}
        for r in reports:
            key = str(r.get(group_col, "UNKNOWN"))
            groups[key] = groups.get(key, 0) + 1

        # Sort by count descending (most common default)
        sorted_groups = sorted(groups.items(), key=lambda x: x[1], reverse=True)

        # Apply HAVING clause if present (e.g., HAVING cnt > 1)
        having_match = re.search(r"having\s+\w+\s*(>|>=|<|<=|=|!=)\s*(\d+)", sql_lower)
        if having_match:
            op, threshold = having_match.group(1), int(having_match.group(2))
            ops = {">": lambda a, b: a > b, ">=": lambda a, b: a >= b,
                   "<": lambda a, b: a < b, "<=": lambda a, b: a <= b,
                   "=": lambda a, b: a == b, "!=": lambda a, b: a != b}
            cmp_fn = ops.get(op, lambda a, b: True)
            sorted_groups = [(k, v) for k, v in sorted_groups if cmp_fn(v, threshold)]

        self._columns = [group_col, "cnt"]
        self._rows = [(k, v) for k, v in sorted_groups]
        self._apply_limit(sql_lower)

    def _handle_min_max(self, sql_lower: str, reports: List[Dict]) -> None:
        """Handle MIN/MAX queries."""
        cols = []
        vals = []
        for func in ["min", "max"]:
            match = re.search(rf"{func}\((\w+)\)\s+as\s+(\w+)", sql_lower)
            if match:
                col, alias = match.group(1), match.group(2)
                values = [r.get(col) for r in reports if r.get(col) is not None]
                if values:
                    result = min(values) if func == "min" else max(values)
                else:
                    result = None
                cols.append(alias)
                vals.append(result)
        self._columns = cols
        self._rows = [tuple(vals)] if vals else []

    def _apply_limit(self, sql_lower: str) -> None:
        """Apply LIMIT clause if present."""
        limit_match = re.search(r"limit\s+(\d+)", sql_lower)
        if limit_match:
            limit = int(limit_match.group(1))
            self._rows = self._rows[:limit]

    def fetchall(self) -> List[Tuple]:
        return list(self._rows)

    def fetchone(self) -> Optional[Tuple]:
        return self._rows[0] if self._rows else None

    def close(self) -> None:
        pass

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


# ---------------------------------------------------------------------------
# Mock connection
# ---------------------------------------------------------------------------


class MockConnection:
    """Simulates a psycopg2 connection."""

    def __init__(self, *, empty: bool = False):
        self.autocommit = False
        self._empty = empty

    def cursor(self) -> MockCursor:
        return MockCursor(empty=self._empty)

    def commit(self) -> None:
        pass

    def close(self) -> None:
        pass


# ---------------------------------------------------------------------------
# Mock pool
# ---------------------------------------------------------------------------


class MockPool:
    """Simulates psycopg2.pool.ThreadedConnectionPool."""

    def __init__(self, *, empty: bool = False):
        self._empty = empty

    def getconn(self) -> MockConnection:
        return MockConnection(empty=self._empty)

    def putconn(self, conn: Any) -> None:
        pass

    def closeall(self) -> None:
        pass


# ---------------------------------------------------------------------------
# Patching helper
# ---------------------------------------------------------------------------


def patch_pg_pool(retriever, *, empty: bool = False) -> None:
    """Patch a UnifiedRetriever instance to use the mock PG pool.

    Args:
        retriever: UnifiedRetriever instance (built via object.__new__).
        empty: If True, tables exist but have zero rows.
    """
    retriever._pg_pool = MockPool(empty=empty)
    retriever.sql_backend = "postgres"
    retriever.sql_available = True
    retriever.sql_unavailable_reason = ""
    retriever.sql_dialect = "postgres"
    retriever.sql_visible_schemas = ["public", "demo"]
    retriever._schema_cache = None
    retriever._schema_cache_expires_at = 0.0
    retriever._schema_cache_ttl = 300.0
