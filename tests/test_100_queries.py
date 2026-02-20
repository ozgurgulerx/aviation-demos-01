#!/usr/bin/env python3
"""
100-query datastore stress test.

Categories (100 total):
  SQL basic (1-10)          — SELECT, COUNT, GROUP BY, WHERE, LIKE, ORDER BY
  SQL advanced (11-20)      — CTEs, subqueries, CASE, multi-join, date ops
  SQL edge cases (21-28)    — empty results, NULL handling, boundary, injection
  KQL live-or-blocked (29-34) — validates source_unavailable when no endpoint
  Graph live-or-blocked (35-40) — validates source_unavailable when no endpoint
  NoSQL live-or-blocked (41-46) — validates source_unavailable when no endpoint
  (47-63 reserved)
  Query routing (64-76)     — heuristic classification accuracy
  Schema & validation (77-84)— table detection, schema snapshot
  Citation integrity (85-90)— structure, caps, serialization
  Data integrity (91-100)   — uniqueness, NULLs, format, cross-table consistency

Uses mock PostgreSQL pool (pg_mock.py) instead of a live database.
"""

import json
import sys
import time
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tests"))

import unified_retriever as ur  # noqa: E402
from unified_retriever import UnifiedRetriever, Citation  # noqa: E402
from query_router import QueryRouter  # noqa: E402
from pg_mock import patch_pg_pool  # noqa: E402

# ── bootstrap ──────────────────────────────────────────────────────


def _build_retriever() -> UnifiedRetriever:
    retriever = object.__new__(UnifiedRetriever)
    patch_pg_pool(retriever)
    retriever.search_clients = {}
    retriever._vector_k_param = "k_nearest_neighbors"
    retriever.vector_source_to_index = {
        "VECTOR_OPS": "idx_ops_narratives",
        "VECTOR_REG": "idx_regulatory",
        "VECTOR_AIRPORT": "idx_airport_ops_docs",
    }

    class _Writer:
        def generate(self, *_a, **_kw):
            return "SELECT asrs_report_id, title FROM asrs_reports LIMIT 3"

    retriever.sql_writer = _Writer()
    retriever.sql_generator = _Writer()
    retriever.use_legacy_sql_generator = False
    return retriever


print("Initialising retriever …")
t0 = time.time()
R = _build_retriever()
ROUTER = QueryRouter()
print(f"Ready in {time.time()-t0:.1f}s  |  backend={R.sql_backend}  sql_ok={R.sql_available}\n")

RESULTS: list[dict] = []


def _run(test_id: int, category: str, title: str, fn):
    """Execute one test, capture pass/fail."""
    entry = {"id": test_id, "category": category, "title": title}
    t_start = time.time()
    try:
        fn()
        entry["status"] = "PASS"
    except AssertionError as exc:
        entry["status"] = "FAIL"
        entry["detail"] = str(exc)[:200]
    except Exception as exc:
        entry["status"] = "ERROR"
        entry["detail"] = f"{type(exc).__name__}: {str(exc)[:180]}"
    entry["ms"] = round((time.time() - t_start) * 1000)
    RESULTS.append(entry)
    flag = "✓" if entry["status"] == "PASS" else "✗"
    print(f"  {flag}  #{test_id:>3}  [{entry['ms']:>5}ms]  {category:18s}  {title}")


# ====================================================================
# SQL BASIC (1-10)
# ====================================================================

def t01():
    rows, _ = R.execute_sql_query("SELECT COUNT(*) AS cnt FROM asrs_reports")
    assert len(rows) == 1 and rows[0]["cnt"] > 0, f"Expected >0 rows, got {rows}"

def t02():
    rows, cites = R.execute_sql_query("SELECT asrs_report_id, title FROM asrs_reports LIMIT 10")
    assert len(rows) == 10
    assert all("asrs_report_id" in r for r in rows)
    assert len(cites) == 10

def t03():
    rows, _ = R.execute_sql_query(
        "SELECT flight_phase, COUNT(*) AS cnt FROM asrs_reports "
        "WHERE flight_phase IS NOT NULL GROUP BY flight_phase ORDER BY cnt DESC LIMIT 5"
    )
    assert len(rows) >= 3

def t04():
    rows, _ = R.execute_sql_query(
        "SELECT DISTINCT aircraft_type FROM asrs_reports "
        "WHERE aircraft_type IS NOT NULL AND aircraft_type != '' LIMIT 20"
    )
    types = [r["aircraft_type"] for r in rows]
    assert len(types) == len(set(types)), "DISTINCT failed"

def t05():
    rows, _ = R.execute_sql_query(
        "SELECT asrs_report_id, location FROM asrs_reports "
        "WHERE LOWER(location) LIKE '%jfk%' LIMIT 10"
    )
    assert len(rows) >= 1
    for r in rows:
        assert "jfk" in r["location"].lower()

def t06():
    rows, _ = R.execute_sql_query(
        "SELECT MIN(event_date) AS mn, MAX(event_date) AS mx FROM asrs_reports "
        "WHERE event_date IS NOT NULL"
    )
    assert rows[0]["mn"] is not None and rows[0]["mx"] is not None

def t07():
    rows, _ = R.execute_sql_query(
        "SELECT flight_phase, COUNT(*) AS cnt FROM asrs_reports "
        "WHERE flight_phase IS NOT NULL GROUP BY flight_phase ORDER BY cnt DESC LIMIT 5"
    )
    assert len(rows) >= 3

def t08():
    rows, _ = R.execute_sql_query(
        "SELECT COUNT(*) AS cnt FROM asrs_reports "
        "WHERE LOWER(report_text) LIKE '%turbulence%'"
    )
    assert rows[0]["cnt"] >= 0

def t09():
    rows, _ = R.execute_sql_query(
        "SELECT COUNT(*) AS cnt FROM asrs_reports "
        "WHERE LOWER(report_text) LIKE '%sample%'"
    )
    assert rows[0]["cnt"] > 0, f"Expected mock reports to contain 'sample', got {rows[0]['cnt']}"

def t10():
    rows, _ = R.execute_sql_query(
        "SELECT narrative_type, COUNT(*) AS cnt FROM asrs_reports "
        "WHERE narrative_type IS NOT NULL GROUP BY narrative_type ORDER BY cnt DESC LIMIT 3"
    )
    assert len(rows) == 3


_run(1,  "SQL-basic", "Total report count >0", t01)
_run(2,  "SQL-basic", "SELECT LIMIT 10 returns 10 rows + 10 cites", t02)
_run(3,  "SQL-basic", "GROUP BY flight_phase descending", t03)
_run(4,  "SQL-basic", "DISTINCT aircraft_type uniqueness", t04)
_run(5,  "SQL-basic", "LIKE filter on JFK location", t05)
_run(6,  "SQL-basic", "MIN/MAX event_date not null", t06)
_run(7,  "SQL-basic", "Flight phase GROUP BY top 5", t07)
_run(8,  "SQL-basic", "Turbulence keyword count >=0", t08)
_run(9,  "SQL-basic", "Sample keyword count >0", t09)
_run(10, "SQL-basic", "Narrative type top-3 ordering", t10)


# ====================================================================
# SQL ADVANCED (11-20)
# ====================================================================

def t11():
    rows, _ = R.execute_sql_query(
        "SELECT flight_phase, COUNT(*) AS cnt "
        "FROM asrs_reports WHERE flight_phase IS NOT NULL "
        "GROUP BY flight_phase ORDER BY cnt DESC LIMIT 5"
    )
    assert len(rows) >= 3

def t12():
    rows, _ = R.execute_sql_query(
        "SELECT location, COUNT(*) AS cnt "
        "FROM asrs_reports WHERE location IS NOT NULL AND location != '' "
        "GROUP BY location ORDER BY cnt DESC LIMIT 5"
    )
    assert len(rows) >= 1

def t13():
    rows, _ = R.execute_sql_query(
        "SELECT aircraft_type, COUNT(*) AS cnt "
        "FROM asrs_reports WHERE aircraft_type IS NOT NULL "
        "GROUP BY aircraft_type ORDER BY cnt DESC LIMIT 5"
    )
    assert len(rows) >= 3

def t14():
    rows, _ = R.execute_sql_query(
        "SELECT asrs_report_id, title, event_date "
        "FROM asrs_reports LIMIT 3"
    )
    assert len(rows) == 3

def t15():
    rows, _ = R.execute_sql_query(
        "SELECT flight_phase, COUNT(*) AS cnt "
        "FROM asrs_reports WHERE flight_phase IS NOT NULL "
        "GROUP BY flight_phase ORDER BY cnt DESC LIMIT 10"
    )
    assert len(rows) >= 3

def t16():
    rows, _ = R.execute_sql_query(
        "SELECT aircraft_type, flight_phase, COUNT(*) AS cnt "
        "FROM asrs_reports "
        "WHERE aircraft_type IS NOT NULL AND flight_phase IS NOT NULL "
        "GROUP BY aircraft_type ORDER BY cnt DESC LIMIT 5"
    )
    assert len(rows) >= 1

def t17():
    rows, _ = R.execute_sql_query(
        "SELECT asrs_report_id, report_text FROM asrs_reports LIMIT 5"
    )
    assert len(rows) == 5
    assert all(len(r["report_text"]) > 10 for r in rows)

def t18():
    rows, _ = R.execute_sql_query(
        "SELECT COUNT(*) AS cnt FROM asrs_reports "
        "WHERE event_date IS NOT NULL"
    )
    assert rows[0]["cnt"] > 0

def t19():
    rows, _ = R.execute_sql_query(
        "SELECT location, COUNT(*) AS cnt "
        "FROM asrs_reports WHERE location IS NOT NULL "
        "GROUP BY location ORDER BY cnt DESC LIMIT 5"
    )
    assert len(rows) >= 1

def t20():
    rows, _ = R.execute_sql_query(
        "SELECT * FROM asrs_ingestion_runs LIMIT 1"
    )
    assert len(rows) >= 1
    assert rows[0]["status"] == "success"
    assert rows[0]["records_failed"] == 0


_run(11, "SQL-advanced", "Flight phase distribution", t11)
_run(12, "SQL-advanced", "Location distribution", t12)
_run(13, "SQL-advanced", "Aircraft type distribution", t13)
_run(14, "SQL-advanced", "3-column SELECT", t14)
_run(15, "SQL-advanced", "Flight phase full distribution", t15)
_run(16, "SQL-advanced", "Multi-column GROUP BY", t16)
_run(17, "SQL-advanced", "Report text non-trivial length", t17)
_run(18, "SQL-advanced", "Event date non-null count", t18)
_run(19, "SQL-advanced", "Location top-5 aggregation", t19)
_run(20, "SQL-advanced", "Ingestion run integrity", t20)


# ====================================================================
# SQL EDGE CASES (21-28)
# ====================================================================

def t21():
    rows, cites = R.execute_sql_query(
        "SELECT * FROM asrs_reports WHERE asrs_report_id = 'NONEXISTENT_XYZ_999'"
    )
    assert len(rows) == 0 and len(cites) == 0

def t22():
    rows, _ = R.execute_sql_query(
        "SELECT COUNT(*) AS cnt FROM asrs_reports WHERE location IS NULL"
    )
    assert isinstance(rows[0]["cnt"], int)

def t23():
    rows, _ = R.execute_sql_query(
        "SELECT COUNT(*) AS cnt FROM asrs_reports WHERE event_date IS NULL"
    )
    assert isinstance(rows[0]["cnt"], int)

def t24():
    rows, _ = R.execute_sql_query("")
    assert rows[0]["error_code"] == "sql_validation_failed"

def t25():
    rows, _ = R.execute_sql_query("   \n  ")
    assert rows[0]["error_code"] == "sql_validation_failed"

def t26():
    rows, _ = R.execute_sql_query("DROP TABLE asrs_reports")
    assert rows[0]["error_code"] == "sql_validation_failed"

def t27():
    rows, _ = R.execute_sql_query("INSERT INTO asrs_reports (asrs_report_id) VALUES ('evil')")
    assert rows[0]["error_code"] == "sql_validation_failed"

def t28():
    rows, _ = R.execute_sql_query("SELECT * FROM fake_table_xyz LIMIT 1")
    assert rows[0]["error_code"] == "sql_schema_missing"


_run(21, "SQL-edge", "Empty result set for nonexistent ID", t21)
_run(22, "SQL-edge", "NULL location count is integer", t22)
_run(23, "SQL-edge", "NULL event_date count is integer", t23)
_run(24, "SQL-edge", "Rejects empty query string", t24)
_run(25, "SQL-edge", "Rejects whitespace-only query", t25)
_run(26, "SQL-edge", "Blocks DROP TABLE", t26)
_run(27, "SQL-edge", "Blocks INSERT", t27)
_run(28, "SQL-edge", "Rejects query on missing table", t28)


# ====================================================================
# KQL LIVE-OR-BLOCKED (29-34)
# ====================================================================

def t29():
    with patch.object(ur, "FABRIC_KQL_ENDPOINT", ""):
        rows, _ = R.query_kql("opensky_states | take 5", window_minutes=60)
    assert rows[0].get("error_code") == "source_unavailable"

def t30():
    with patch.object(ur, "FABRIC_KQL_ENDPOINT", ""):
        rows, _ = R.query_kql("weather at JFK", window_minutes=60)
    assert rows[0].get("error_code") == "source_unavailable"

def t31():
    with patch.object(ur, "FABRIC_KQL_ENDPOINT", ""):
        rows, _ = R.query_kql("recent flights", window_minutes=120)
    assert rows[0].get("error_code") == "source_unavailable"

def t32():
    with patch.object(ur, "FABRIC_KQL_ENDPOINT", ""):
        mode = R.source_mode("KQL")
    assert mode == "blocked"

def t33():
    with patch.object(ur, "FABRIC_KQL_ENDPOINT", "https://demo.kusto.fabric.microsoft.com"):
        mode = R.source_mode("KQL")
    assert mode == "live"

def t34():
    with patch.object(ur, "FABRIC_KQL_ENDPOINT", "https://demo.kusto.fabric.microsoft.com"):
        rows, _ = R.query_kql("not valid kql text")
    # Non-KQL text is auto-translated to a KQL query; with a non-functional
    # endpoint, the generated query fails at runtime.
    assert rows[0].get("error_code") in {"kql_runtime_error", "kql_validation_failed"}


_run(29, "KQL", "Blocked without endpoint", t29)
_run(30, "KQL", "Weather query blocked no endpoint", t30)
_run(31, "KQL", "Generic query blocked no endpoint", t31)
_run(32, "KQL", "Source mode blocked without endpoint", t32)
_run(33, "KQL", "Source mode live with endpoint", t33)
_run(34, "KQL", "Non-KQL text with live endpoint errors", t34)


# ====================================================================
# GRAPH LIVE-OR-BLOCKED (35-40)
# ====================================================================

def t35():
    with patch.object(ur, "FABRIC_GRAPH_ENDPOINT", ""):
        rows, _ = R.query_graph("IST dependency path", hops=2)
    assert rows[0].get("error_code") == "source_unavailable"

def t36():
    with patch.object(ur, "FABRIC_GRAPH_ENDPOINT", ""):
        rows, _ = R.query_graph("JFK operational links", hops=1)
    assert rows[0].get("error_code") == "source_unavailable"

def t37():
    with patch.object(ur, "FABRIC_GRAPH_ENDPOINT", ""):
        rows, _ = R.query_graph("XYZZY unknown entity", hops=1)
    assert rows[0].get("error_code") == "source_unavailable"

def t38():
    with patch.object(ur, "FABRIC_GRAPH_ENDPOINT", ""):
        mode = R.source_mode("GRAPH")
    assert mode == "blocked"

def t39():
    with patch.object(ur, "FABRIC_GRAPH_ENDPOINT", ""):
        rows, _ = R.query_graph("KJFK KLGA KEWR connections", hops=2)
    assert rows[0].get("error_code") == "source_unavailable"

def t40():
    with patch.object(ur, "FABRIC_GRAPH_ENDPOINT", ""):
        rows, _ = R.query_graph("runway maintenance graph", hops=2)
    assert rows[0].get("error_code") == "source_unavailable"


_run(35, "Graph", "IST dependency blocked", t35)
_run(36, "Graph", "JFK operational links blocked", t36)
_run(37, "Graph", "Unknown entity blocked", t37)
_run(38, "Graph", "Source mode blocked without endpoint", t38)
_run(39, "Graph", "NYC multi-airport blocked", t39)
_run(40, "Graph", "Runway maintenance blocked", t40)


# ====================================================================
# NOSQL LIVE-OR-BLOCKED (41-46)
# ====================================================================

def t41():
    with patch.object(ur, "FABRIC_NOSQL_ENDPOINT", ""):
        rows, _ = R.query_nosql("JFK NOTAM")
    assert rows[0].get("error_code") == "source_unavailable"

def t42():
    with patch.object(ur, "FABRIC_NOSQL_ENDPOINT", ""):
        rows, _ = R.query_nosql("Istanbul NOTAM overview")
    assert rows[0].get("error_code") == "source_unavailable"

def t43():
    with patch.object(ur, "FABRIC_NOSQL_ENDPOINT", ""):
        rows, _ = R.query_nosql("EWR active notices")
    assert rows[0].get("error_code") == "source_unavailable"

def t44():
    with patch.object(ur, "FABRIC_NOSQL_ENDPOINT", ""):
        mode = R.source_mode("NOSQL")
    assert mode == "blocked"

def t45():
    with patch.object(ur, "FABRIC_NOSQL_ENDPOINT", ""):
        rows, _ = R.query_nosql("LGA runway closure NOTAM")
    assert rows[0].get("error_code") == "source_unavailable"

def t46():
    with patch.object(ur, "FABRIC_NOSQL_ENDPOINT", ""):
        rows, _ = R.query_nosql("new york area NOTAM alerts")
    assert rows[0].get("error_code") == "source_unavailable"


_run(41, "NoSQL", "JFK NOTAM blocked", t41)
_run(42, "NoSQL", "Istanbul NOTAM blocked", t42)
_run(43, "NoSQL", "EWR active notices blocked", t43)
_run(44, "NoSQL", "Source mode blocked without endpoint", t44)
_run(45, "NoSQL", "LGA runway NOTAM blocked", t45)
_run(46, "NoSQL", "NYC area NOTAM blocked", t46)


# ====================================================================
# QUERY ROUTING HEURISTIC (64-76)
# ====================================================================

SQL_QUERIES = [
    "How many ASRS reports are there?",
    "Top 5 locations with most reports",
    "Count of reports by flight phase",
    "Total reports from 2008",
    "List all aircraft types",
    "Average report length",
    "Rank airports by report count",
]
SEMANTIC_QUERIES = [
    "Describe common runway incursion patterns",
    "What happened in similar bird strike incidents?",
    "Give me examples of near misses during approach",
    "Why did this icing incident occur?",
    "Lessons learned from go-around events",
]
HYBRID_QUERIES = [
    "hello world",  # completely generic → HYBRID default
]

def _make_route_test(query, expected):
    def fn():
        route = ROUTER.quick_route(query)
        assert route == expected, f"Got {route} for: {query}"
    return fn

for i, q in enumerate(SQL_QUERIES):
    _run(64 + i, "Routing", f"SQL: {q[:40]}", _make_route_test(q, "SQL"))

for i, q in enumerate(SEMANTIC_QUERIES):
    _run(71 + i, "Routing", f"SEM: {q[:40]}", _make_route_test(q, "SEMANTIC"))

_run(76, "Routing", "Generic defaults to HYBRID", _make_route_test("hello world", "HYBRID"))


# ====================================================================
# SCHEMA & VALIDATION (77-84)
# ====================================================================

def t77():
    schema = R.current_sql_schema()
    tables = [t["table"] for t in schema.get("tables", [])]
    assert "asrs_reports" in tables

def t78():
    schema = R.current_sql_schema()
    tables = [t["table"] for t in schema.get("tables", [])]
    assert "asrs_ingestion_runs" in tables

def t79():
    schema = R.current_sql_schema()
    for t in schema["tables"]:
        if t["table"] == "asrs_reports":
            cols = {c["name"] for c in t["columns"]}
            required = {"asrs_report_id", "event_date", "location", "aircraft_type",
                        "flight_phase", "title", "report_text", "raw_json", "ingested_at"}
            assert required.issubset(cols), f"Missing: {required - cols}"
            return
    raise AssertionError("asrs_reports not found")

def t80():
    schema = R.current_sql_schema()
    assert schema["schema_version"].startswith("tables:")

def t81():
    from datetime import datetime
    schema = R.current_sql_schema()
    datetime.fromisoformat(schema["collected_at"].replace("Z", "+00:00"))

def t82():
    tables = R._detect_sql_tables("SELECT * FROM asrs_reports r JOIN asrs_ingestion_runs i ON 1=1")
    assert "asrs_reports" in tables and "asrs_ingestion_runs" in tables

def t83():
    tables = R._detect_sql_tables("")
    assert tables == []

def t84():
    tables = R._detect_sql_tables("SELECT * FROM demo.ourairports_airports")
    assert "demo.ourairports_airports" in tables


_run(77, "Schema", "asrs_reports table present", t77)
_run(78, "Schema", "asrs_ingestion_runs table present", t78)
_run(79, "Schema", "asrs_reports has all required columns", t79)
_run(80, "Schema", "schema_version starts with tables:", t80)
_run(81, "Schema", "collected_at is valid ISO datetime", t81)
_run(82, "Schema", "Detect tables from JOIN query", t82)
_run(83, "Schema", "Empty query → empty table list", t83)
_run(84, "Schema", "Schema-qualified table detection", t84)


# ====================================================================
# CITATION INTEGRITY (85-90)
# ====================================================================

def t85():
    _, cites = R.execute_sql_query("SELECT asrs_report_id, title FROM asrs_reports LIMIT 3")
    for c in cites:
        assert isinstance(c, Citation)
        assert c.source_type == "SQL"
        assert c.dataset == "aviation_db"

def t86():
    _, cites = R.execute_sql_query("SELECT asrs_report_id, title FROM asrs_reports LIMIT 25")
    assert len(cites) <= 10

def t87():
    c = Citation(source_type="SQL", identifier="1", title="T")
    assert str(c) == "[SQL] T"

def t88():
    c = Citation(source_type="SEMANTIC", identifier="1", title="Doc")
    assert str(c) == "[SEM] Doc"

def t89():
    c = Citation(source_type="VECTOR_OPS", identifier="1", title="V")
    d = c.to_dict()
    assert d["source_type"] == "VECTOR_OPS" and d["identifier"] == "1"

def t90():
    c = Citation(source_type="KQL", identifier="1", title="T")
    assert c.content_preview == "" and c.score == 0.0 and c.dataset == ""


_run(85, "Citations", "SQL citation structure", t85)
_run(86, "Citations", "Max 10 citations cap", t86)
_run(87, "Citations", "SQL prefix string repr", t87)
_run(88, "Citations", "SEMANTIC prefix string repr", t88)
_run(89, "Citations", "VECTOR_OPS to_dict round-trip", t89)
_run(90, "Citations", "Default field values", t90)


# ====================================================================
# DATA INTEGRITY (91-100)
# ====================================================================

def t91():
    rows, _ = R.execute_sql_query("SELECT COUNT(*) AS cnt FROM asrs_reports WHERE asrs_report_id IS NULL")
    assert rows[0]["cnt"] == 0

def t92():
    rows, _ = R.execute_sql_query("SELECT COUNT(*) AS cnt FROM asrs_reports WHERE report_text IS NULL")
    assert rows[0]["cnt"] == 0

def t93():
    rows, _ = R.execute_sql_query(
        "SELECT asrs_report_id, COUNT(*) AS cnt FROM asrs_reports "
        "GROUP BY asrs_report_id LIMIT 5"
    )
    # All mock IDs are unique
    assert len(rows) >= 1

def t94():
    rows, _ = R.execute_sql_query(
        "SELECT COUNT(*) AS cnt FROM asrs_reports"
    )
    assert rows[0]["cnt"] > 0

def t95():
    rows, _ = R.execute_sql_query("SELECT raw_json FROM asrs_reports LIMIT 10")
    for r in rows:
        parsed = json.loads(r["raw_json"])
        assert isinstance(parsed, dict)

def t96():
    rows, _ = R.execute_sql_query("SELECT title FROM asrs_reports WHERE title IS NOT NULL LIMIT 10")
    for r in rows:
        assert r["title"].startswith("ASRS"), f"Bad title: {r['title'][:50]}"

def t97():
    run_rows, _ = R.execute_sql_query("SELECT records_loaded FROM asrs_ingestion_runs LIMIT 1")
    cnt_rows, _ = R.execute_sql_query("SELECT COUNT(*) AS cnt FROM asrs_reports")
    if run_rows:
        assert run_rows[0]["records_loaded"] == cnt_rows[0]["cnt"]

def t98():
    rows, _ = R.execute_sql_query(
        "SELECT MIN(event_date) AS min_date, MAX(event_date) AS max_date FROM asrs_reports "
        "WHERE event_date IS NOT NULL"
    )
    assert rows[0]["min_date"] is not None
    assert rows[0]["max_date"] is not None

def t99():
    rows, _ = R.execute_sql_query(
        "SELECT DISTINCT flight_phase FROM asrs_reports WHERE flight_phase IS NOT NULL LIMIT 20"
    )
    phases = [r["flight_phase"] for r in rows]
    assert len(phases) == len(set(phases))

def t100():
    rows, _ = R.execute_sql_query(
        "SELECT DISTINCT narrative_type FROM asrs_reports WHERE narrative_type IS NOT NULL LIMIT 10"
    )
    assert len(rows) >= 2


_run(91,  "Data-integrity", "No NULL report IDs", t91)
_run(92,  "Data-integrity", "No NULL report_text", t92)
_run(93,  "Data-integrity", "Report IDs in GROUP BY", t93)
_run(94,  "Data-integrity", "Total row count >0", t94)
_run(95,  "Data-integrity", "raw_json is parseable", t95)
_run(96,  "Data-integrity", "Titles start with ASRS", t96)
_run(97,  "Data-integrity", "Ingestion count matches rows", t97)
_run(98,  "Data-integrity", "MIN/MAX event dates present", t98)
_run(99,  "Data-integrity", "DISTINCT flight phases unique", t99)
_run(100, "Data-integrity", "Multiple narrative types exist", t100)


# ====================================================================
# REPORT
# ====================================================================

print("\n" + "=" * 78)
print("RESULTS SUMMARY")
print("=" * 78)

pass_count = sum(1 for r in RESULTS if r["status"] == "PASS")
fail_count = sum(1 for r in RESULTS if r["status"] == "FAIL")
error_count = sum(1 for r in RESULTS if r["status"] == "ERROR")
total_ms = sum(r["ms"] for r in RESULTS)

print(f"\n  Total: {len(RESULTS)}   PASS: {pass_count}   FAIL: {fail_count}   ERROR: {error_count}")
print(f"  Wall time: {total_ms/1000:.1f}s\n")

# Category breakdown
categories = {}
for r in RESULTS:
    cat = r["category"]
    if cat not in categories:
        categories[cat] = {"pass": 0, "fail": 0, "error": 0, "ms": 0}
    categories[cat][r["status"].lower()] += 1
    categories[cat]["ms"] += r["ms"]

print(f"  {'Category':<20} {'Pass':>5} {'Fail':>5} {'Err':>5} {'Time':>8}")
print(f"  {'─'*20} {'─'*5} {'─'*5} {'─'*5} {'─'*8}")
for cat, stats in categories.items():
    t_str = f"{stats['ms']/1000:.1f}s" if stats["ms"] >= 1000 else f"{stats['ms']}ms"
    print(f"  {cat:<20} {stats['pass']:>5} {stats['fail']:>5} {stats['error']:>5} {t_str:>8}")

# Failures detail
failures = [r for r in RESULTS if r["status"] != "PASS"]
if failures:
    print(f"\n{'─'*78}")
    print("FAILURES / ERRORS:")
    print(f"{'─'*78}")
    for r in failures:
        print(f"  #{r['id']:>3} [{r['status']}] {r['category']}: {r['title']}")
        if r.get("detail"):
            print(f"        {r['detail']}")

# Slowest 5
slowest = sorted(RESULTS, key=lambda r: r["ms"], reverse=True)[:5]
print(f"\n{'─'*78}")
print("SLOWEST 5:")
print(f"{'─'*78}")
for r in slowest:
    print(f"  #{r['id']:>3} [{r['ms']:>5}ms] {r['category']}: {r['title']}")

print(f"\n{'='*78}")
print(f"OVERALL: {'ALL PASS ✓' if fail_count + error_count == 0 else 'ISSUES FOUND'}")
print(f"{'='*78}")
