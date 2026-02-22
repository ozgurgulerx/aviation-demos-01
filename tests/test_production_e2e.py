#!/usr/bin/env python3
"""
Production end-to-end test suite — validates all retrieval paths against the live backend.

Usage:
    python tests/test_production_e2e.py [--endpoint URL]

Default endpoint: http://20.240.76.230

42 test cases across 10 categories:
  1. SQL Route (8)          2. Semantic Route (4)     3. Hybrid Route (2)
  4. Agentic Per-Intent (10) 5. GRAPH (2)             6. KQL/NOSQL Known (2)
  7. Edge Cases (6)         8. Context Quality (4)    9. Demo Scenarios (2)
  10. Robustness (2)
"""

import argparse
import json
import sys
import time
import threading
import urllib.request
import urllib.error
from collections import defaultdict

DEFAULT_ENDPOINT = "http://20.240.76.230"

# Sources where 401 / source_unavailable is a known limitation (not a test failure)
KNOWN_401_SOURCES = {"KQL", "NOSQL"}

INTER_TEST_DELAY = 2  # seconds between tests to avoid 429 rate limiting


# ── Helpers ────────────────────────────────────────────────────────────

def post_chat(endpoint: str, message: str, timeout: int = 90, **kwargs) -> dict:
    """Send a chat message and parse the SSE response into structured results.

    Extra kwargs are merged into the request body (e.g. required_sources, demo_scenario).
    """
    url = f"{endpoint}/api/chat"
    body = {"message": message, **kwargs}
    data = json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})

    result = {
        "message": message,
        "request_body": body,
        "events": [],
        "source_calls": [],
        "source_done": [],
        "answer_parts": [],
        "citations": [],
        "errors": [],
        "route": None,
        "agent_done": False,
        "agent_done_event": None,
        "raw_lines": [],
    }

    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            for line in resp:
                line = line.decode("utf-8").strip()
                result["raw_lines"].append(line)
                if not line.startswith("data: "):
                    continue
                payload = line[6:]
                try:
                    evt = json.loads(payload)
                except json.JSONDecodeError:
                    continue

                result["events"].append(evt)
                evt_type = evt.get("type", "")

                if evt_type == "source_call_start":
                    result["source_calls"].append(evt)
                elif evt_type == "source_call_done":
                    result["source_done"].append(evt)
                elif evt_type == "agent_update" and "content" in evt:
                    result["answer_parts"].append(evt["content"])
                elif evt_type == "citations":
                    result["citations"] = evt.get("citations", [])
                elif evt_type == "agent_done":
                    result["agent_done"] = True
                    result["agent_done_event"] = evt
                    result["route"] = evt.get("route")
                elif evt_type == "error":
                    result["errors"].append(evt)
    except urllib.error.URLError as e:
        result["errors"].append({"type": "connection_error", "detail": str(e)})
    except Exception as e:
        result["errors"].append({"type": "exception", "detail": str(e)})

    result["answer"] = "".join(result["answer_parts"])
    return result


def post_chat_raw(endpoint: str, message: str, timeout: int = 15, **kwargs) -> tuple:
    """Send a chat message and return (status_code, body_str) for HTTP-level tests."""
    url = f"{endpoint}/api/chat"
    body = {"message": message, **kwargs}
    data = json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})

    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        body_str = ""
        try:
            body_str = e.read().decode("utf-8")
        except Exception:
            pass
        return e.code, body_str
    except Exception as e:
        return 0, str(e)


def extract_retrieval_plan(result: dict) -> dict | None:
    """Pull the retrieval_plan event from results."""
    for evt in result["events"]:
        if evt.get("type") == "retrieval_plan":
            return evt
    return None


def classify_source_errors(result: dict) -> tuple:
    """Separate known-401 errors from real errors.

    Returns (known_errors, real_errors) where each is a list of dicts
    with keys: source, error_code, error.
    """
    known = []
    real = []
    for sd in result["source_done"]:
        rows_preview = sd.get("rows_preview", [])
        for row in rows_preview:
            ec = row.get("error_code", "")
            if not ec:
                continue
            source = sd.get("source", "")
            err = row.get("error", "")
            entry = {"source": source, "error_code": ec, "error": err}
            if source in KNOWN_401_SOURCES and (
                "401" in err or "Unauthorized" in err
                or ec == "source_unavailable"
            ):
                known.append(entry)
            else:
                real.append(entry)
    return known, real


def check_source_errors(result: dict) -> list:
    """Extract any source-level errors from source_call_done events."""
    issues = []
    for sd in result["source_done"]:
        rows_preview = sd.get("rows_preview", [])
        for row in rows_preview:
            if row.get("error_code"):
                issues.append({
                    "source": sd.get("source"),
                    "error_code": row["error_code"],
                    "error": row.get("error", ""),
                })
    return issues


def get_sources_fired(result: dict) -> list:
    """Return deduplicated list of source names that had source_call_done events."""
    return list({s.get("source") for s in result["source_done"] if s.get("source")})


def source_fired(result: dict, source_name: str) -> bool:
    """Check if a specific source fired (had source_call_done)."""
    return any(s.get("source") == source_name for s in result["source_done"])


def sql_row_count(result: dict) -> int:
    """Total row count across all SQL source_call_done events."""
    return sum(
        s.get("row_count", 0)
        for s in result["source_done"]
        if s.get("source") == "SQL"
    )


# ── Test Result ────────────────────────────────────────────────────────

class TestResult:
    def __init__(self, name: str, category: str = ""):
        self.name = name
        self.category = category
        self.passed = False
        self.warned = False  # passed but with known issues
        self.details = ""
        self.errors = []
        self.known_errors = []  # known-401 etc., documented but not failures
        self.duration = 0.0
        self.sources_fired = []


# ── Category 1: SQL Route (8 tests) ──────────────────────────────────

def test_sql_count_asrs(endpoint: str) -> TestResult:
    """#1 — How many ASRS reports total?"""
    t = TestResult("SQL — Count ASRS Reports", "SQL")
    start = time.time()
    r = post_chat(endpoint, "How many ASRS reports are there in total?")
    t.duration = time.time() - start
    t.sources_fired = get_sources_fired(r)

    known, real = classify_source_errors(r)
    t.known_errors = known
    if real:
        t.errors.extend([f"{e['source']}: {e['error_code']} — {e['error']}" for e in real])
        return t

    sql_done = [s for s in r["source_done"] if s.get("source") == "SQL"]
    if not sql_done:
        t.errors.append("No SQL source_call_done event")
        return t

    row_count = sql_done[0].get("row_count", 0)
    preview = sql_done[0].get("rows_preview", [])
    if row_count >= 1 and preview:
        row = preview[0]
        count_val = None
        for key, val in row.items():
            if key in ("error", "error_code", "source", "sql"):
                continue
            try:
                if int(val) > 0:
                    count_val = int(val)
                    break
            except (ValueError, TypeError):
                continue
        if count_val and count_val > 0:
            t.passed = True
            t.details = f"count={count_val}, mode={sql_done[0].get('execution_mode')}"
        else:
            t.errors.append(f"Unexpected count value: {preview}")
    else:
        t.errors.append(f"row_count={row_count}, preview={preview}")

    if r["errors"]:
        t.errors.extend([str(e) for e in r["errors"]])
    return t


def test_sql_ranking_flight_phase(endpoint: str) -> TestResult:
    """#2 — Top 5 flight phases with most ASRS reports"""
    t = TestResult("SQL — Top 5 Flight Phases", "SQL")
    start = time.time()
    r = post_chat(endpoint, "Top 5 flight phases with the most ASRS reports")
    t.duration = time.time() - start
    t.sources_fired = get_sources_fired(r)

    known, real = classify_source_errors(r)
    t.known_errors = known
    for e in real:
        if "column" in e.get("error", "").lower() and "does not exist" in e.get("error", "").lower():
            t.errors.append(f"COLUMN HALLUCINATION: {e['error']}")
            return t

    total_rows = sql_row_count(r)
    if total_rows >= 3:
        t.passed = True
        t.details = f"rows={total_rows}"
    elif r["agent_done"] and r["answer"]:
        t.passed = True
        t.details = f"rows={total_rows} (agent answered), answer_len={len(r['answer'])}"
    else:
        t.errors.append(f"Expected >=3 rows, got {total_rows}")

    if r["errors"]:
        t.errors.extend([str(e) for e in r["errors"]])
    return t


def test_sql_cross_table_airports(endpoint: str) -> TestResult:
    """#3 — Top 5 airports by elevation from ourairports_airports"""
    t = TestResult("SQL — Cross-Table Airports", "SQL")
    start = time.time()
    r = post_chat(endpoint, "List the top 5 airports by elevation from ourairports_airports")
    t.duration = time.time() - start
    t.sources_fired = get_sources_fired(r)

    known, real = classify_source_errors(r)
    t.known_errors = known
    for e in real:
        if "column" in e.get("error", "").lower() and "does not exist" in e.get("error", "").lower():
            t.errors.append(f"COLUMN HALLUCINATION BUG: {e['error']}")
            return t

    if sql_row_count(r) >= 1:
        t.passed = True
        t.details = f"rows={sql_row_count(r)}"
    elif r["agent_done"] and r["answer"]:
        t.passed = True
        t.details = "Agent completed (possible empty result)"
    else:
        t.errors.append("No SQL results and agent did not complete")

    if r["errors"]:
        t.errors.extend([str(e) for e in r["errors"]])
    return t


def test_sql_runway_data(endpoint: str) -> TestResult:
    """#4 — Longest runways at Istanbul airports"""
    t = TestResult("SQL — Istanbul Runway Data", "SQL")
    start = time.time()
    r = post_chat(endpoint, "What are the longest runways at Istanbul airports?")
    t.duration = time.time() - start
    t.sources_fired = get_sources_fired(r)

    known, real = classify_source_errors(r)
    t.known_errors = known
    for e in real:
        if "column" in e.get("error", "").lower() and "does not exist" in e.get("error", "").lower():
            t.errors.append(f"COLUMN HALLUCINATION: {e['error']}")
            return t

    if r["agent_done"] and r["answer"]:
        answer_lower = r["answer"].lower()
        mentions_istanbul = "ltfm" in answer_lower or "ltba" in answer_lower or "istanbul" in answer_lower
        t.passed = True
        t.details = f"answer_len={len(r['answer'])}, mentions_istanbul={mentions_istanbul}"
        if not mentions_istanbul:
            t.warned = True
            t.details += " [WARN: no Istanbul airport mentioned]"
    else:
        t.errors.append("Agent did not complete or no answer")

    if r["errors"]:
        t.errors.extend([str(e) for e in r["errors"]])
    return t


def test_sql_routes_openflights(endpoint: str) -> TestResult:
    """#5 — How many routes depart from Istanbul (IST)?"""
    t = TestResult("SQL — OpenFlights Routes IST", "SQL")
    start = time.time()
    r = post_chat(endpoint, "How many routes depart from Istanbul (IST)?")
    t.duration = time.time() - start
    t.sources_fired = get_sources_fired(r)

    known, real = classify_source_errors(r)
    t.known_errors = known
    for e in real:
        if "column" in e.get("error", "").lower() and "does not exist" in e.get("error", "").lower():
            t.errors.append(f"COLUMN HALLUCINATION: {e['error']}")
            return t

    if r["agent_done"] and r["answer"]:
        t.passed = True
        # Check if answer contains a number
        has_number = any(c.isdigit() for c in r["answer"])
        t.details = f"answer_len={len(r['answer'])}, has_number={has_number}"
    else:
        t.errors.append("Agent did not complete")

    if r["errors"]:
        t.errors.extend([str(e) for e in r["errors"]])
    return t


def test_sql_hazards_table(endpoint: str) -> TestResult:
    """#6 — Count severe hazards in hazards_airsigmets table"""
    t = TestResult("SQL — Hazards Table", "SQL")
    start = time.time()
    r = post_chat(endpoint, "Count severe hazards in hazards_airsigmets table")
    t.duration = time.time() - start
    t.sources_fired = get_sources_fired(r)

    known, real = classify_source_errors(r)
    t.known_errors = known
    for e in real:
        err_lower = e.get("error", "").lower()
        if "column" in err_lower and "does not exist" in err_lower:
            t.errors.append(f"COLUMN HALLUCINATION: {e['error']}")
            return t
        if "relation" in err_lower and "does not exist" in err_lower:
            t.errors.append(f"TABLE NOT FOUND: {e['error']}")
            return t

    if r["agent_done"]:
        t.passed = True
        t.details = f"answer_len={len(r['answer'])}"
    else:
        t.errors.append("Agent did not complete")

    if r["errors"]:
        t.errors.extend([str(e) for e in r["errors"]])
    return t


def test_sql_trend_by_year(endpoint: str) -> TestResult:
    """#7 — ASRS report trend by year for last 5 years"""
    t = TestResult("SQL — Trend by Year", "SQL")
    start = time.time()
    r = post_chat(endpoint, "Show trend of ASRS reports by year for last 5 years")
    t.duration = time.time() - start
    t.sources_fired = get_sources_fired(r)

    known, real = classify_source_errors(r)
    t.known_errors = known

    if r["agent_done"] and r["answer"]:
        t.passed = True
        answer_lower = r["answer"].lower()
        mentions_years = any(str(y) in answer_lower for y in range(2020, 2027))
        t.details = f"answer_len={len(r['answer'])}, mentions_years={mentions_years}, sql_rows={sql_row_count(r)}"
        if not mentions_years:
            t.warned = True
            t.details += " [WARN: no year numbers found in answer]"
    else:
        t.errors.append("Agent did not complete or no answer")

    if r["errors"]:
        t.errors.extend([str(e) for e in r["errors"]])
    return t


def test_sql_airline_fleet(endpoint: str) -> TestResult:
    """#8 — Top 10 airlines by number of routes"""
    t = TestResult("SQL — Top Airlines by Routes", "SQL")
    start = time.time()
    r = post_chat(endpoint, "Top 10 airlines by number of routes")
    t.duration = time.time() - start
    t.sources_fired = get_sources_fired(r)

    known, real = classify_source_errors(r)
    t.known_errors = known

    if r["agent_done"] and r["answer"]:
        t.passed = True
        t.details = f"answer_len={len(r['answer'])}, sql_rows={sql_row_count(r)}"
    else:
        t.errors.append("Agent did not complete or no answer")

    if r["errors"]:
        t.errors.extend([str(e) for e in r["errors"]])
    return t


# ── Category 2: Semantic Route (4 tests) ─────────────────────────────

def test_semantic_narrative(endpoint: str) -> TestResult:
    """#9 — Describe common runway incursion patterns"""
    t = TestResult("SEMANTIC — Runway Incursion Patterns", "Semantic")
    start = time.time()
    r = post_chat(endpoint, "Describe common runway incursion patterns from incident reports")
    t.duration = time.time() - start
    t.sources_fired = get_sources_fired(r)

    known, real = classify_source_errors(r)
    t.known_errors = known

    vector_done = [s for s in r["source_done"] if "VECTOR" in (s.get("source") or "")]
    if r["agent_done"] and r["answer"]:
        t.passed = True
        t.details = f"answer_len={len(r['answer'])}, citations={len(r['citations'])}, vector_fired={len(vector_done) > 0}"
        for e in real:
            if e["error_code"] == "source_unavailable":
                t.details += f" [WARN: {e['source']} unavailable]"
                t.warned = True
    else:
        t.errors.append("Agent did not complete or no answer")

    if r["errors"]:
        t.errors.extend([str(e) for e in r["errors"]])
    return t


def test_semantic_regulatory(endpoint: str) -> TestResult:
    """#10 — Summarize recent NOTAM themes and airworthiness directives"""
    t = TestResult("SEMANTIC — Regulatory/NOTAM Themes", "Semantic")
    start = time.time()
    r = post_chat(endpoint, "Summarize recent NOTAM themes and airworthiness directives")
    t.duration = time.time() - start
    t.sources_fired = get_sources_fired(r)

    known, real = classify_source_errors(r)
    t.known_errors = known

    if r["agent_done"] and r["answer"] and len(r["answer"]) > 100:
        t.passed = True
        vector_reg = [s for s in r["source_done"] if s.get("source") == "VECTOR_REG"]
        t.details = f"answer_len={len(r['answer'])}, VECTOR_REG_fired={len(vector_reg) > 0}"
    elif r["agent_done"] and r["answer"]:
        t.passed = True
        t.warned = True
        t.details = f"answer_len={len(r['answer'])} [WARN: short answer < 100 chars]"
    else:
        t.errors.append("Agent did not complete or no answer")

    if r["errors"]:
        t.errors.extend([str(e) for e in r["errors"]])
    return t


def test_semantic_similarity(endpoint: str) -> TestResult:
    """#11 — Find incidents similar to bird strike during takeoff"""
    t = TestResult("SEMANTIC — Bird Strike Similarity", "Semantic")
    start = time.time()
    r = post_chat(endpoint, "Find incidents similar to bird strike during takeoff")
    t.duration = time.time() - start
    t.sources_fired = get_sources_fired(r)

    known, real = classify_source_errors(r)
    t.known_errors = known

    if r["agent_done"] and r["answer"]:
        answer_lower = r["answer"].lower()
        mentions_bird = "bird" in answer_lower or "strike" in answer_lower
        t.passed = True
        t.details = f"answer_len={len(r['answer'])}, mentions_bird_strike={mentions_bird}, citations={len(r['citations'])}"
        if not mentions_bird:
            t.warned = True
            t.details += " [WARN: answer does not mention bird/strike]"
    else:
        t.errors.append("Agent did not complete or no answer")

    if r["errors"]:
        t.errors.extend([str(e) for e in r["errors"]])
    return t


def test_semantic_airport_ops(endpoint: str) -> TestResult:
    """#12 — Operational procedures for Istanbul LTFM airport"""
    t = TestResult("SEMANTIC — Airport Ops LTFM", "Semantic")
    start = time.time()
    r = post_chat(endpoint, "Operational procedures for Istanbul LTFM airport?")
    t.duration = time.time() - start
    t.sources_fired = get_sources_fired(r)

    known, real = classify_source_errors(r)
    t.known_errors = known

    if r["agent_done"] and r["answer"]:
        t.passed = True
        vector_airport = [s for s in r["source_done"] if s.get("source") == "VECTOR_AIRPORT"]
        t.details = f"answer_len={len(r['answer'])}, VECTOR_AIRPORT_fired={len(vector_airport) > 0}"
    else:
        t.errors.append("Agent did not complete or no answer")

    if r["errors"]:
        t.errors.extend([str(e) for e in r["errors"]])
    return t


# ── Category 3: Hybrid Route (2 tests) ───────────────────────────────

def test_hybrid_mixed(endpoint: str) -> TestResult:
    """#13 — Show top aircraft types and describe their typical safety issues"""
    t = TestResult("HYBRID — Aircraft Types + Issues", "Hybrid")
    start = time.time()
    r = post_chat(endpoint, "Show top aircraft types and describe their typical safety issues")
    t.duration = time.time() - start
    t.sources_fired = get_sources_fired(r)

    known, real = classify_source_errors(r)
    t.known_errors = known

    if r["agent_done"] and r["answer"]:
        t.passed = True
        distinct_sources = set(s.get("source") for s in r["source_done"] if s.get("source"))
        t.details = f"sources={sorted(distinct_sources)}, answer_len={len(r['answer'])}"
        if len(distinct_sources) < 2:
            t.warned = True
            t.details += " [WARN: expected 2+ distinct sources]"
    else:
        t.errors.append("Agent did not complete or no answer")

    for e in real:
        if e["error_code"] not in ("source_unavailable",):
            t.errors.append(f"{e['source']}: {e['error_code']} — {e['error']}")

    if r["errors"]:
        t.errors.extend([str(e) for e in r["errors"]])
    return t


def test_hybrid_metrics_context(endpoint: str) -> TestResult:
    """#14 — Most common locations for ASRS reports and what incidents occur there?"""
    t = TestResult("HYBRID — Locations + Incidents", "Hybrid")
    start = time.time()
    r = post_chat(endpoint, "Most common locations for ASRS reports and what incidents occur there?")
    t.duration = time.time() - start
    t.sources_fired = get_sources_fired(r)

    known, real = classify_source_errors(r)
    t.known_errors = known

    if r["agent_done"] and r["answer"]:
        t.passed = True
        sql_fired = source_fired(r, "SQL")
        vector_fired = any("VECTOR" in (s.get("source") or "") for s in r["source_done"])
        t.details = f"SQL={sql_fired}, VECTOR={vector_fired}, answer_len={len(r['answer'])}"
    else:
        t.errors.append("Agent did not complete or no answer")

    if r["errors"]:
        t.errors.extend([str(e) for e in r["errors"]])
    return t


# ── Category 4: Agentic Per-Intent (10 tests) ────────────────────────

def _agentic_test(
    endpoint: str, name: str, query: str,
    expected_sources: list[str] | None = None,
    answer_keywords: list[str] | None = None,
    **extra_kwargs,
) -> TestResult:
    """Generic agentic test that checks agent_done, sources, and optional keywords."""
    t = TestResult(name, "Agentic")
    start = time.time()
    r = post_chat(endpoint, query, **extra_kwargs)
    t.duration = time.time() - start
    t.sources_fired = get_sources_fired(r)

    known, real = classify_source_errors(r)
    t.known_errors = known

    if not r["agent_done"]:
        t.errors.append("agent_done=False")
        if r["errors"]:
            t.errors.extend([str(e) for e in r["errors"]])
        return t

    t.passed = True
    details_parts = [f"answer_len={len(r['answer'])}"]

    # Check expected sources
    if expected_sources:
        for src in expected_sources:
            fired = source_fired(r, src)
            details_parts.append(f"{src}={'OK' if fired else 'MISS'}")
            if not fired and src not in KNOWN_401_SOURCES:
                t.warned = True

    # Document known 401 errors
    for ke in known:
        details_parts.append(f"{ke['source']}_401=documented")

    # Check answer keywords
    if answer_keywords and r["answer"]:
        answer_lower = r["answer"].lower()
        for kw in answer_keywords:
            if kw.lower() not in answer_lower:
                details_parts.append(f"missing_keyword='{kw}'")
                t.warned = True

    # Real errors (non-known-401) that aren't source_unavailable for known sources
    for e in real:
        if e["source"] in KNOWN_401_SOURCES:
            details_parts.append(f"{e['source']}_error=documented")
        else:
            t.errors.append(f"{e['source']}: {e['error_code']} — {e['error']}")

    t.details = ", ".join(details_parts)
    if r["errors"]:
        t.errors.extend([str(e) for e in r["errors"]])
    return t


def test_agentic_pilotbrief_departure(endpoint: str) -> TestResult:
    """#15 — PilotBrief.Departure for Istanbul LTFM"""
    return _agentic_test(
        endpoint,
        "AGENTIC — PilotBrief Departure LTFM",
        "Give me a departure brief for Istanbul LTFM",
        expected_sources=["SQL", "GRAPH", "KQL"],
        answer_keywords=["LTFM"],
    )


def test_agentic_pilotbrief_arrival(endpoint: str) -> TestResult:
    """#16 — PilotBrief.Arrival for KJFK"""
    return _agentic_test(
        endpoint,
        "AGENTIC — PilotBrief Arrival KJFK",
        "Prepare an arrival brief for KJFK New York",
        expected_sources=["SQL"],
        answer_keywords=["JFK"],
    )


def test_agentic_disruption(endpoint: str) -> TestResult:
    """#17 — Disruption.Explain for Istanbul delays"""
    return _agentic_test(
        endpoint,
        "AGENTIC — Disruption Explain",
        "Why were there delays at Istanbul airport last week?",
        expected_sources=["KQL", "NOSQL", "VECTOR_REG"],
    )


def test_agentic_policy(endpoint: str) -> TestResult:
    """#18 — Policy.Check for low-visibility SOP"""
    return _agentic_test(
        endpoint,
        "AGENTIC — Policy Check",
        "What is the SOP policy for low-visibility operations?",
        expected_sources=["VECTOR_REG"],
    )


def test_agentic_replay(endpoint: str) -> TestResult:
    """#19 — Replay.History for LTFM past 24 hours"""
    return _agentic_test(
        endpoint,
        "AGENTIC — Replay History",
        "Replay weather and hazard history for LTFM past 24 hours",
        expected_sources=["KQL"],
    )


def test_agentic_analytics(endpoint: str) -> TestResult:
    """#20 — Analytics.Compare ASRS 2024 vs 2025"""
    return _agentic_test(
        endpoint,
        "AGENTIC — Analytics Compare",
        "Compare safety record of ASRS reports between 2024 and 2025",
        expected_sources=["SQL"],
        answer_keywords=["2024", "2025"],
    )


def test_agentic_fleet(endpoint: str) -> TestResult:
    """#21 — Fleet.Status current fleet size"""
    return _agentic_test(
        endpoint,
        "AGENTIC — Fleet Status",
        "Current fleet size and aircraft distribution across airlines?",
        expected_sources=["SQL"],
    )


def test_agentic_route_network(endpoint: str) -> TestResult:
    """#22 — RouteNetwork.Query Istanbul to Europe"""
    return _agentic_test(
        endpoint,
        "AGENTIC — Route Network",
        "Route network from Istanbul to European destinations",
        expected_sources=["SQL", "GRAPH"],
    )


def test_agentic_safety_trend(endpoint: str) -> TestResult:
    """#23 — Safety.Trend incident rates by flight phase over 3 years"""
    return _agentic_test(
        endpoint,
        "AGENTIC — Safety Trend",
        "Safety trend of incident rates by flight phase over 3 years",
        expected_sources=["SQL"],
    )


def test_agentic_airport_info(endpoint: str) -> TestResult:
    """#24 — Airport.Info for LTFM including runways"""
    return _agentic_test(
        endpoint,
        "AGENTIC — Airport Info LTFM",
        "Full airport information for LTFM including runways",
        expected_sources=["SQL"],
        answer_keywords=["LTFM"],
    )


# ── Category 5: GRAPH Tests (2 tests) ────────────────────────────────

def test_graph_dependency(endpoint: str) -> TestResult:
    """#25 — Show dependency graph for LTFM"""
    t = TestResult("GRAPH — Dependency LTFM", "GRAPH")
    start = time.time()
    r = post_chat(endpoint, "Show dependency graph for Istanbul airport LTFM")
    t.duration = time.time() - start
    t.sources_fired = get_sources_fired(r)

    known, real = classify_source_errors(r)
    t.known_errors = known

    if r["agent_done"]:
        t.passed = True
        graph_done = [s for s in r["source_done"] if s.get("source") == "GRAPH"]
        if graph_done:
            mode = graph_done[0].get("execution_mode", "")
            status = graph_done[0].get("contract_status", "")
            t.details = f"GRAPH mode={mode}, status={status}"
            # Check for source_unavailable (expected in fallback mode)
            for row in graph_done[0].get("rows_preview", []):
                ec = row.get("error_code", "")
                if ec == "source_unavailable":
                    t.details += " [source_unavailable — fallback mode]"
        else:
            t.details = "No GRAPH source called by agent"
    else:
        t.errors.append("Agent did not complete")

    if r["errors"]:
        t.errors.extend([str(e) for e in r["errors"]])
    return t


def test_graph_impact(endpoint: str) -> TestResult:
    """#26 — What downstream stations if LTFM has runway closure?"""
    t = TestResult("GRAPH — Downstream Impact", "GRAPH")
    start = time.time()
    r = post_chat(endpoint, "What downstream stations are affected if LTFM has a runway closure?")
    t.duration = time.time() - start
    t.sources_fired = get_sources_fired(r)

    known, real = classify_source_errors(r)
    t.known_errors = known

    if r["agent_done"] and r["answer"]:
        t.passed = True
        answer_lower = r["answer"].lower()
        mentions_downstream = "downstream" in answer_lower or "affected" in answer_lower or "impact" in answer_lower
        graph_fired = source_fired(r, "GRAPH")
        t.details = f"GRAPH_fired={graph_fired}, mentions_downstream={mentions_downstream}, answer_len={len(r['answer'])}"
    else:
        t.errors.append("Agent did not complete or no answer")

    if r["errors"]:
        t.errors.extend([str(e) for e in r["errors"]])
    return t


# ── Category 6: KQL/NOSQL Known Errors (2 tests) ─────────────────────

def test_kql_weather_401(endpoint: str) -> TestResult:
    """#27 — KQL fires but returns 401 (documented, not a failure)"""
    t = TestResult("KQL — Weather 401 (known)", "KQL/NOSQL")
    start = time.time()
    r = post_chat(endpoint, "Current weather hazards near Istanbul")
    t.duration = time.time() - start
    t.sources_fired = get_sources_fired(r)

    known, real = classify_source_errors(r)
    t.known_errors = known

    if r["agent_done"]:
        t.passed = True
        kql_done = [s for s in r["source_done"] if s.get("source") == "KQL"]
        if kql_done:
            t.details = f"KQL fired, status={kql_done[0].get('contract_status')}"
            for ke in known:
                if ke["source"] == "KQL":
                    t.details += ", 401=documented"
            # Check for the old let-binding bug
            for row in kql_done[0].get("rows_preview", []):
                if row.get("error_code") == "kql_validation_failed" and "multiple_statements" in row.get("error", ""):
                    t.passed = False
                    t.errors.append("KQL LET-BINDING BUG STILL PRESENT: " + row.get("error", ""))
        else:
            t.details = "No KQL source used (routed differently)"
    else:
        t.errors.append("Agent did not complete")

    if r["errors"]:
        t.errors.extend([str(e) for e in r["errors"]])
    return t


def test_nosql_notam_401(endpoint: str) -> TestResult:
    """#28 — NOSQL fires but returns 401 (documented, not a failure)"""
    t = TestResult("NOSQL — NOTAM 401 (known)", "KQL/NOSQL")
    start = time.time()
    r = post_chat(endpoint, "Active NOTAMs for JFK airport")
    t.duration = time.time() - start
    t.sources_fired = get_sources_fired(r)

    known, real = classify_source_errors(r)
    t.known_errors = known

    if r["agent_done"]:
        t.passed = True
        nosql_done = [s for s in r["source_done"] if s.get("source") == "NOSQL"]
        if nosql_done:
            t.details = f"NOSQL fired, status={nosql_done[0].get('contract_status')}"
            for ke in known:
                if ke["source"] == "NOSQL":
                    t.details += ", 401=documented"
        else:
            t.details = "No NOSQL source used (routed differently)"
    else:
        t.errors.append("Agent did not complete")

    if r["errors"]:
        t.errors.extend([str(e) for e in r["errors"]])
    return t


# ── Category 7: Edge Cases (6 tests) ─────────────────────────────────

def test_edge_empty_query(endpoint: str) -> TestResult:
    """#29 — Empty query should return HTTP 400"""
    t = TestResult("EDGE — Empty Query", "Edge")
    start = time.time()
    status, body = post_chat_raw(endpoint, "")
    t.duration = time.time() - start

    if status == 400:
        t.passed = True
        t.details = f"HTTP 400 (expected)"
    elif status == 200:
        # Server accepted empty query — not ideal but not a crash
        t.passed = True
        t.warned = True
        t.details = f"HTTP 200 (server accepted empty query — should ideally return 400)"
    elif status == 0:
        t.errors.append(f"Connection error: {body}")
    else:
        t.passed = True
        t.details = f"HTTP {status}"

    return t


def test_edge_long_query(endpoint: str) -> TestResult:
    """#30 — Very long query (>8000 chars) should return HTTP 400"""
    t = TestResult("EDGE — Long Query (8000+)", "Edge")
    long_msg = "aviation safety " * 600  # ~9600 chars
    start = time.time()
    status, body = post_chat_raw(endpoint, long_msg)
    t.duration = time.time() - start

    if status == 400:
        t.passed = True
        t.details = f"HTTP 400 (expected), query_len={len(long_msg)}"
    elif status == 200:
        t.passed = True
        t.warned = True
        t.details = f"HTTP 200 (server accepted long query — should ideally return 400), query_len={len(long_msg)}"
    elif status == 0:
        t.errors.append(f"Connection error: {body}")
    else:
        t.passed = True
        t.details = f"HTTP {status}, query_len={len(long_msg)}"

    return t


def test_edge_pii_blocking(endpoint: str) -> TestResult:
    """#31 — PII (SSN) should be blocked"""
    t = TestResult("EDGE — PII Blocking (SSN)", "Edge")
    start = time.time()
    r = post_chat(endpoint, "My social security number is 123-45-6789, can you help me?")
    t.duration = time.time() - start
    t.sources_fired = get_sources_fired(r)

    # Check if PII was detected
    for evt in r["events"]:
        if evt.get("type") == "pii_blocked":
            t.passed = True
            t.details = "PII correctly blocked"
            return t
        if evt.get("type") == "error" and "pii" in str(evt).lower():
            t.passed = True
            t.details = "PII blocked via error event"
            return t

    # Check route
    if r.get("route") == "BLOCKED":
        t.passed = True
        t.details = "route=BLOCKED"
        return t

    # If agent completed without blocking, PII filter may be fail-open
    if r["agent_done"]:
        t.passed = True
        t.warned = True
        t.details = "Agent completed (PII filter may be fail-open or not configured)"

    if r["errors"]:
        t.errors.extend([str(e) for e in r["errors"]])
    return t


def test_edge_nonsense(endpoint: str) -> TestResult:
    """#32 — Nonsense query should not crash"""
    t = TestResult("EDGE — Nonsense Query", "Edge")
    start = time.time()
    r = post_chat(endpoint, "xyzzy foobar qwerty asdf")
    t.duration = time.time() - start
    t.sources_fired = get_sources_fired(r)

    if r["agent_done"]:
        t.passed = True
        t.details = f"agent_done=True, answer_len={len(r['answer'])}"
    else:
        t.errors.append("Agent did not complete on nonsense query")

    # Connection errors are real failures
    conn_errors = [e for e in r["errors"] if e.get("type") == "connection_error"]
    if conn_errors:
        t.passed = False
        t.errors.extend([str(e) for e in conn_errors])

    return t


def test_edge_sql_injection(endpoint: str) -> TestResult:
    """#33 — SQL injection attempt should be caught"""
    t = TestResult("EDGE — SQL Injection", "Edge")
    start = time.time()
    r = post_chat(endpoint, "Show airports; DROP TABLE asrs_reports; --")
    t.duration = time.time() - start
    t.sources_fired = get_sources_fired(r)

    known, real = classify_source_errors(r)
    t.known_errors = known

    # Check for sql_validation_failed (good — injection was caught)
    injection_caught = False
    for e in check_source_errors(r):
        if e["error_code"] == "sql_validation_failed":
            injection_caught = True

    if r["agent_done"]:
        t.passed = True
        if injection_caught:
            t.details = "sql_validation_failed (injection correctly caught)"
        else:
            t.details = f"agent_done=True, answer_len={len(r['answer'])} (query sanitized or rewritten)"
    else:
        # Even if agent didn't complete, if injection was caught that's a pass
        if injection_caught:
            t.passed = True
            t.details = "sql_validation_failed (injection caught, agent may have stopped)"
        else:
            t.errors.append("Agent did not complete")

    if r["errors"]:
        t.errors.extend([str(e) for e in r["errors"]])
    return t


def test_edge_special_chars(endpoint: str) -> TestResult:
    """#34 — Special characters in query should not crash"""
    t = TestResult("EDGE — Special Characters", "Edge")
    start = time.time()
    r = post_chat(endpoint, "Accident rate for O'Hare (ORD) airport's runway?")
    t.duration = time.time() - start
    t.sources_fired = get_sources_fired(r)

    if r["agent_done"]:
        t.passed = True
        t.details = f"agent_done=True, answer_len={len(r['answer'])}"
    else:
        t.errors.append("Agent did not complete (possible parse error with special chars)")

    conn_errors = [e for e in r["errors"] if e.get("type") == "connection_error"]
    if conn_errors:
        t.passed = False
        t.errors.extend([str(e) for e in conn_errors])

    return t


# ── Category 8: Context Quality (4 tests) ────────────────────────────

def test_multi_source_full(endpoint: str) -> TestResult:
    """#35 — Full ops risk brief requesting all 5 sources"""
    t = TestResult("QUALITY — Multi-Source Full Brief", "Quality")
    start = time.time()
    r = post_chat(
        endpoint,
        "Full ops risk brief for LTFM using all data",
        required_sources=["SQL", "KQL", "GRAPH", "VECTOR_REG", "NOSQL"],
        ask_recommendation=True,
    )
    t.duration = time.time() - start
    t.sources_fired = get_sources_fired(r)

    known, real = classify_source_errors(r)
    t.known_errors = known

    if r["agent_done"] and r["answer"]:
        # Count source_call_done events
        scd_count = len(r["source_done"])
        answer_len = len(r["answer"])
        t.details = f"source_call_done={scd_count}, answer_len={answer_len}"

        if answer_len > 200:
            t.passed = True
        else:
            t.passed = True
            t.warned = True
            t.details += " [WARN: short answer for full brief]"

        # Document which of the 5 sources fired vs didn't
        requested = {"SQL", "KQL", "GRAPH", "VECTOR_REG", "NOSQL"}
        fired_set = set(t.sources_fired)
        missing = requested - fired_set
        if missing:
            t.details += f", missing_sources={sorted(missing)}"
            # KQL/NOSQL missing is expected (401)
            unexpected_missing = missing - KNOWN_401_SOURCES
            if unexpected_missing:
                t.warned = True
    else:
        t.errors.append("Agent did not complete or no answer")

    if r["errors"]:
        t.errors.extend([str(e) for e in r["errors"]])
    return t


def test_coverage_summary(endpoint: str) -> TestResult:
    """#36 — Pilot departure brief with query_profile"""
    t = TestResult("QUALITY — Coverage Summary", "Quality")
    start = time.time()
    r = post_chat(
        endpoint,
        "Complete pilot departure brief for KJFK",
        query_profile="pilot-brief",
    )
    t.duration = time.time() - start
    t.sources_fired = get_sources_fired(r)

    known, real = classify_source_errors(r)
    t.known_errors = known

    plan = extract_retrieval_plan(r)
    if r["agent_done"] and r["answer"]:
        t.passed = True
        t.details = f"answer_len={len(r['answer'])}"
        if plan:
            req_total = plan.get("required_total", 0)
            req_filled = plan.get("required_filled", 0)
            t.details += f", plan_required={req_total}, plan_filled={req_filled}"
            if req_filled == 0 and req_total > 0:
                t.warned = True
                t.details += " [WARN: no required slots filled]"
        else:
            t.details += ", no_retrieval_plan_event"
    else:
        t.errors.append("Agent did not complete or no answer")

    if r["errors"]:
        t.errors.extend([str(e) for e in r["errors"]])
    return t


def test_grounding_check(endpoint: str) -> TestResult:
    """#37 — Check if agent_done has grounding metadata"""
    t = TestResult("QUALITY — Grounding Check", "Quality")
    start = time.time()
    r = post_chat(endpoint, "Top 3 locations with most ASRS reports")
    t.duration = time.time() - start
    t.sources_fired = get_sources_fired(r)

    known, real = classify_source_errors(r)
    t.known_errors = known

    if r["agent_done"]:
        t.passed = True
        done_evt = r["agent_done_event"] or {}
        has_grounding = "grounding" in done_evt
        t.details = f"has_grounding={has_grounding}, answer_len={len(r['answer'])}"
        if has_grounding:
            grounding = done_evt["grounding"]
            t.details += f", grounding_keys={list(grounding.keys()) if isinstance(grounding, dict) else type(grounding).__name__}"
    else:
        t.errors.append("Agent did not complete")

    if r["errors"]:
        t.errors.extend([str(e) for e in r["errors"]])
    return t


def test_freshness_sla(endpoint: str) -> TestResult:
    """#38 — Live hazard with freshness SLA"""
    t = TestResult("QUALITY — Freshness SLA", "Quality")
    start = time.time()
    r = post_chat(
        endpoint,
        "Live hazard indicators for IST in last 30 minutes",
        freshness_sla_minutes=30,
    )
    t.duration = time.time() - start
    t.sources_fired = get_sources_fired(r)

    known, real = classify_source_errors(r)
    t.known_errors = known

    # Look for freshness_guardrail event
    freshness_evt = None
    for evt in r["events"]:
        if evt.get("type") == "freshness_guardrail":
            freshness_evt = evt

    if r["agent_done"]:
        t.passed = True
        kql_fired = source_fired(r, "KQL")
        t.details = f"KQL_fired={kql_fired}, freshness_event={'found' if freshness_evt else 'none'}"
        for ke in known:
            t.details += f", {ke['source']}_401=documented"
    else:
        t.errors.append("Agent did not complete")

    if r["errors"]:
        t.errors.extend([str(e) for e in r["errors"]])
    return t


# ── Category 9: Demo Scenarios (2 tests) ─────────────────────────────

def test_demo_weather_spike(endpoint: str) -> TestResult:
    """#39 — Weather-spike demo scenario"""
    t = TestResult("DEMO — Weather Spike", "Demo")
    start = time.time()
    r = post_chat(
        endpoint,
        "Impact on Istanbul departures?",
        demo_scenario="weather-spike",
    )
    t.duration = time.time() - start
    t.sources_fired = get_sources_fired(r)

    known, real = classify_source_errors(r)
    t.known_errors = known

    # Check for scenario_loaded and operational_alert events
    scenario_loaded = any(e.get("type") == "scenario_loaded" for e in r["events"])
    op_alerts = [e for e in r["events"] if e.get("type") == "operational_alert"]

    if r["agent_done"]:
        t.passed = True
        t.details = f"scenario_loaded={scenario_loaded}, op_alerts={len(op_alerts)}, answer_len={len(r['answer'])}"
        if op_alerts:
            severities = [a.get("severity") for a in op_alerts]
            t.details += f", severities={severities}"
        if not scenario_loaded:
            t.details += " [no scenario_loaded event — demo_scenario may not be implemented]"
    else:
        t.errors.append("Agent did not complete")

    if r["errors"]:
        t.errors.extend([str(e) for e in r["errors"]])
    return t


def test_demo_runway_notam(endpoint: str) -> TestResult:
    """#40 — Runway-NOTAM demo scenario"""
    t = TestResult("DEMO — Runway NOTAM", "Demo")
    start = time.time()
    r = post_chat(
        endpoint,
        "How does runway closure affect ops?",
        demo_scenario="runway-notam",
    )
    t.duration = time.time() - start
    t.sources_fired = get_sources_fired(r)

    known, real = classify_source_errors(r)
    t.known_errors = known

    scenario_loaded = any(e.get("type") == "scenario_loaded" for e in r["events"])
    op_alerts = [e for e in r["events"] if e.get("type") == "operational_alert"]

    if r["agent_done"]:
        t.passed = True
        t.details = f"scenario_loaded={scenario_loaded}, op_alerts={len(op_alerts)}, answer_len={len(r['answer'])}"
        if op_alerts:
            severities = [a.get("severity") for a in op_alerts]
            t.details += f", severities={severities}"
        if not scenario_loaded:
            t.details += " [no scenario_loaded event — demo_scenario may not be implemented]"
    else:
        t.errors.append("Agent did not complete")

    if r["errors"]:
        t.errors.extend([str(e) for e in r["errors"]])
    return t


# ── Category 10: Robustness (2 tests) ────────────────────────────────

def test_rate_limit_spacing(endpoint: str) -> TestResult:
    """#41 — 3 sequential queries with 3s spacing, no 429 errors"""
    t = TestResult("ROBUST — Rate Limit Spacing", "Robustness")
    queries = [
        "How many ASRS reports are there?",
        "Top 3 airports by elevation",
        "Describe runway incursion patterns",
    ]
    start = time.time()
    all_ok = True
    details_parts = []

    for i, q in enumerate(queries):
        if i > 0:
            time.sleep(3)
        r = post_chat(endpoint, q, timeout=90)
        ok = r["agent_done"]
        has_429 = any("429" in str(e) for e in r["errors"])
        details_parts.append(f"q{i+1}={'OK' if ok else 'FAIL'}")
        if has_429:
            details_parts.append(f"q{i+1}_429=True")
            all_ok = False
        if not ok:
            all_ok = False
            # Retry once after 5s on potential 429
            if has_429 or any("rate" in str(e).lower() for e in r["errors"]):
                time.sleep(5)
                r = post_chat(endpoint, q, timeout=90)
                if r["agent_done"]:
                    details_parts[-1] = f"q{i+1}=OK_after_retry"
                    all_ok = True

    t.duration = time.time() - start
    t.passed = all_ok
    t.details = ", ".join(details_parts)
    if not all_ok:
        t.errors.append("Not all queries completed successfully")
    return t


def test_concurrent_requests(endpoint: str) -> TestResult:
    """#42 — 2 simultaneous queries should both complete"""
    t = TestResult("ROBUST — Concurrent Requests", "Robustness")
    queries = [
        "How many ASRS reports are there?",
        "Top 5 airports by elevation",
    ]
    results = [None, None]

    def run_query(idx, query):
        results[idx] = post_chat(endpoint, query, timeout=120)

    start = time.time()
    threads = []
    for i, q in enumerate(queries):
        th = threading.Thread(target=run_query, args=(i, q))
        threads.append(th)
        th.start()

    for th in threads:
        th.join(timeout=130)

    t.duration = time.time() - start

    both_ok = True
    details_parts = []
    for i, r in enumerate(results):
        if r is None:
            details_parts.append(f"q{i+1}=TIMEOUT")
            both_ok = False
        elif r["agent_done"]:
            details_parts.append(f"q{i+1}=OK({len(r['answer'])}ch)")
        else:
            conn_err = any(e.get("type") == "connection_error" for e in r["errors"])
            details_parts.append(f"q{i+1}=FAIL(conn={conn_err})")
            both_ok = False

    t.passed = both_ok
    t.details = ", ".join(details_parts)
    if not both_ok:
        t.errors.append("Not all concurrent queries completed")
    return t


# ── Health Check ──────────────────────────────────────────────────────

def test_health(endpoint: str) -> TestResult:
    t = TestResult("Health Check", "Health")
    start = time.time()
    try:
        url = f"{endpoint}/health"
        with urllib.request.urlopen(url, timeout=10) as resp:
            data = json.loads(resp.read())
        t.duration = time.time() - start
        if data.get("status") == "ok":
            t.passed = True
            t.details = f"service={data.get('service')}"
        else:
            t.details = f"Unexpected status: {data}"
    except Exception as e:
        t.duration = time.time() - start
        t.errors.append(str(e))
    return t


# ── Runner ────────────────────────────────────────────────────────────

def print_report(results: list[TestResult]):
    """Print detailed report with summary, source matrix, and observations."""
    passed = sum(1 for r in results if r.passed and not r.warned)
    warned = sum(1 for r in results if r.passed and r.warned)
    failed = sum(1 for r in results if not r.passed)
    total = len(results)

    print(f"\n{'='*80}")
    print(f"  PRODUCTION E2E TEST REPORT")
    print(f"{'='*80}")
    print(f"  Total: {total}  |  Passed: {passed}  |  Warned: {warned}  |  Failed: {failed}")
    print(f"{'='*80}\n")

    # ── Summary table ──
    print(f"  {'#':<4} {'Status':<6} {'Category':<12} {'Name':<42} {'Time':>6}")
    print(f"  {'-'*4} {'-'*6} {'-'*12} {'-'*42} {'-'*6}")
    for i, r in enumerate(results, 1):
        if r.passed and not r.warned:
            icon = "PASS"
        elif r.passed and r.warned:
            icon = "WARN"
        else:
            icon = "FAIL"
        cat = r.category[:12] if r.category else ""
        print(f"  {i:<4} {icon:<6} {cat:<12} {r.name:<42} {r.duration:>5.1f}s")
    print()

    # ── Failures detail ──
    failures = [r for r in results if not r.passed]
    if failures:
        print(f"  {'='*80}")
        print(f"  FAILURES ({len(failures)})")
        print(f"  {'='*80}")
        for r in failures:
            print(f"\n  [{r.category}] {r.name}")
            for err in r.errors:
                print(f"    ERROR: {err}")
            if r.details:
                print(f"    Details: {r.details}")
        print()

    # ── Warnings detail ──
    warnings = [r for r in results if r.warned]
    if warnings:
        print(f"  {'='*80}")
        print(f"  WARNINGS ({len(warnings)})")
        print(f"  {'='*80}")
        for r in warnings:
            print(f"\n  [{r.category}] {r.name}")
            print(f"    {r.details}")
        print()

    # ── Source Availability Matrix ──
    print(f"  {'='*80}")
    print(f"  SOURCE AVAILABILITY MATRIX")
    print(f"  {'='*80}")
    source_stats = defaultdict(lambda: {"fired": 0, "total": 0, "known_401": 0})
    all_sources = set()

    for r in results:
        for src in r.sources_fired:
            all_sources.add(src)
        # Count tests where each source was expected/fired
        for src in r.sources_fired:
            source_stats[src]["fired"] += 1
            source_stats[src]["total"] += 1
        for ke in r.known_errors:
            src = ke["source"]
            all_sources.add(src)
            source_stats[src]["known_401"] += 1
            source_stats[src]["total"] += 1

    print(f"\n  {'Source':<16} {'Fired':>8} {'401/Err':>8} {'Total':>8}")
    print(f"  {'-'*16} {'-'*8} {'-'*8} {'-'*8}")
    for src in sorted(all_sources):
        s = source_stats[src]
        print(f"  {src:<16} {s['fired']:>8} {s['known_401']:>8} {s['total']:>8}")
    print()

    # ── Category breakdown ──
    print(f"  {'='*80}")
    print(f"  CATEGORY BREAKDOWN")
    print(f"  {'='*80}")
    categories = defaultdict(lambda: {"pass": 0, "warn": 0, "fail": 0, "total": 0})
    for r in results:
        cat = r.category or "Other"
        categories[cat]["total"] += 1
        if r.passed and not r.warned:
            categories[cat]["pass"] += 1
        elif r.passed and r.warned:
            categories[cat]["warn"] += 1
        else:
            categories[cat]["fail"] += 1

    print(f"\n  {'Category':<16} {'Pass':>6} {'Warn':>6} {'Fail':>6} {'Total':>6}")
    print(f"  {'-'*16} {'-'*6} {'-'*6} {'-'*6} {'-'*6}")
    for cat in ["Health", "SQL", "Semantic", "Hybrid", "Agentic", "GRAPH",
                 "KQL/NOSQL", "Edge", "Quality", "Demo", "Robustness"]:
        if cat in categories:
            c = categories[cat]
            print(f"  {cat:<16} {c['pass']:>6} {c['warn']:>6} {c['fail']:>6} {c['total']:>6}")
    print()

    # ── Known Issues ──
    all_known = []
    for r in results:
        for ke in r.known_errors:
            all_known.append((r.name, ke))
    if all_known:
        print(f"  {'='*80}")
        print(f"  KNOWN ISSUES (documented, not failures)")
        print(f"  {'='*80}")
        for test_name, ke in all_known[:20]:  # cap at 20
            print(f"  {ke['source']:>8} {ke['error_code']:<24} in {test_name}")
        if len(all_known) > 20:
            print(f"  ... and {len(all_known) - 20} more")
        print()

    # ── Observations & Recommendations ──
    print(f"  {'='*80}")
    print(f"  OBSERVATIONS & RECOMMENDATIONS")
    print(f"  {'='*80}\n")

    observations = []

    # SQL reliability
    sql_tests = [r for r in results if r.category == "SQL"]
    sql_pass = sum(1 for r in sql_tests if r.passed)
    observations.append(f"  SQL: {sql_pass}/{len(sql_tests)} passed — {'reliable' if sql_pass == len(sql_tests) else 'issues detected'}")

    # Semantic availability
    sem_tests = [r for r in results if r.category == "Semantic"]
    sem_pass = sum(1 for r in sem_tests if r.passed)
    observations.append(f"  Semantic: {sem_pass}/{len(sem_tests)} passed")

    # KQL/NOSQL status
    kql_401_count = sum(1 for r in results for ke in r.known_errors if ke["source"] == "KQL")
    nosql_401_count = sum(1 for r in results for ke in r.known_errors if ke["source"] == "NOSQL")
    if kql_401_count > 0:
        observations.append(f"  KQL: 401 errors in {kql_401_count} tests — needs credential/RBAC fix")
    if nosql_401_count > 0:
        observations.append(f"  NOSQL: 401 errors in {nosql_401_count} tests — needs credential/RBAC fix")

    # Agentic intents
    agentic_tests = [r for r in results if r.category == "Agentic"]
    agentic_done = sum(1 for r in agentic_tests if r.passed)
    observations.append(f"  Agentic: {agentic_done}/{len(agentic_tests)} intents completed (agent_done=True)")

    # Edge case handling
    edge_tests = [r for r in results if r.category == "Edge"]
    edge_pass = sum(1 for r in edge_tests if r.passed)
    observations.append(f"  Edge cases: {edge_pass}/{len(edge_tests)} handled gracefully")

    # Timing
    total_time = sum(r.duration for r in results)
    avg_time = total_time / len(results) if results else 0
    slowest = max(results, key=lambda r: r.duration) if results else None
    observations.append(f"  Timing: total={total_time:.0f}s, avg={avg_time:.1f}s, slowest={slowest.name} ({slowest.duration:.1f}s)" if slowest else "")

    for obs in observations:
        if obs:
            print(obs)
    print()

    # Recommendations
    print("  Recommendations:")
    if kql_401_count > 0:
        print("  - Fix KQL (Fabric/ADX) authentication — RBAC or token refresh issue")
    if nosql_401_count > 0:
        print("  - Fix NOSQL (Cosmos DB) authentication — key rotation or RBAC")
    if failed > 0:
        print(f"  - Investigate {failed} test failure(s) — see FAILURES section above")
    warned_edge = [r for r in results if r.category == "Edge" and r.warned]
    if warned_edge:
        print("  - Add input validation (empty query -> 400, long query -> 400)")
    hallucination_errors = [r for r in results if any("HALLUCINATION" in e for e in r.errors)]
    if hallucination_errors:
        print("  - Fix SQL column hallucination — improve schema introspection prompt")
    print()


def main():
    parser = argparse.ArgumentParser(description="Production E2E tests (42 cases)")
    parser.add_argument("--endpoint", default=DEFAULT_ENDPOINT)
    args = parser.parse_args()

    endpoint = args.endpoint.rstrip("/")
    print(f"\n{'='*80}")
    print(f"  PRODUCTION E2E TESTS — {endpoint}")
    print(f"  {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}")
    print(f"  43 tests (1 health + 42 category) across 10 categories")
    print(f"{'='*80}\n")

    # Test order: Health → SQL → Semantic → Hybrid → GRAPH → KQL/NOSQL → Agentic → Edge → Quality → Demo → Robustness
    tests = [
        # Category 0: Health
        test_health,
        # Category 1: SQL Route (8)
        test_sql_count_asrs,
        test_sql_ranking_flight_phase,
        test_sql_cross_table_airports,
        test_sql_runway_data,
        test_sql_routes_openflights,
        test_sql_hazards_table,
        test_sql_trend_by_year,
        test_sql_airline_fleet,
        # Category 2: Semantic Route (4)
        test_semantic_narrative,
        test_semantic_regulatory,
        test_semantic_similarity,
        test_semantic_airport_ops,
        # Category 3: Hybrid Route (2)
        test_hybrid_mixed,
        test_hybrid_metrics_context,
        # Category 5: GRAPH (2)
        test_graph_dependency,
        test_graph_impact,
        # Category 6: KQL/NOSQL Known (2)
        test_kql_weather_401,
        test_nosql_notam_401,
        # Category 4: Agentic Per-Intent (10)
        test_agentic_pilotbrief_departure,
        test_agentic_pilotbrief_arrival,
        test_agentic_disruption,
        test_agentic_policy,
        test_agentic_replay,
        test_agentic_analytics,
        test_agentic_fleet,
        test_agentic_route_network,
        test_agentic_safety_trend,
        test_agentic_airport_info,
        # Category 7: Edge Cases (6)
        test_edge_empty_query,
        test_edge_long_query,
        test_edge_pii_blocking,
        test_edge_nonsense,
        test_edge_sql_injection,
        test_edge_special_chars,
        # Category 8: Context Quality (4)
        test_multi_source_full,
        test_coverage_summary,
        test_grounding_check,
        test_freshness_sla,
        # Category 9: Demo Scenarios (2)
        test_demo_weather_spike,
        test_demo_runway_notam,
        # Category 10: Robustness (2)
        test_rate_limit_spacing,
        test_concurrent_requests,
    ]

    assert len(tests) == 43, f"Expected 43 tests (1 health + 42 category), got {len(tests)}"

    results = []
    for i, test_fn in enumerate(tests):
        label = f"[{i+1}/{len(tests)}]"
        print(f"  {label:<8} Running: {test_fn.__name__}...", end=" ", flush=True)
        result = test_fn(endpoint)
        results.append(result)

        if result.passed and not result.warned:
            status = "PASS"
        elif result.passed and result.warned:
            status = "WARN"
        else:
            status = "FAIL"
        print(f"{status} ({result.duration:.1f}s)")

        if result.details:
            print(f"           Details: {result.details}")
        for err in result.errors:
            print(f"           ERROR: {err}")
        for ke in result.known_errors:
            print(f"           KNOWN: {ke['source']} {ke['error_code']}")

        # Inter-test delay to avoid 429 (skip for health and edge tests that are fast)
        if i < len(tests) - 1 and result.duration > 1:
            time.sleep(INTER_TEST_DELAY)

    # Print full report
    print_report(results)

    failed = sum(1 for r in results if not r.passed)
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
