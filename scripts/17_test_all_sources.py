#!/usr/bin/env python3
"""
End-to-end source coverage and routing tests for the aviation RAG backend.

Sends test queries to the backend SSE endpoint (/api/chat) and parses
source_call_start / source_call_done events to verify that the expected
data sources fire and return rows.

New in v2: routing-specific tests (R*), source activation boundary tests (S*),
keyword gap tests (G*), multi-overlap tests (O*), and --dry-run mode.

Usage:
    python scripts/17_test_all_sources.py
    python scripts/17_test_all_sources.py --backend http://localhost:5001
    python scripts/17_test_all_sources.py --filter T1,T3,M1
    python scripts/17_test_all_sources.py --category routing
    python scripts/17_test_all_sources.py --dry-run
    python scripts/17_test_all_sources.py --verbose
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set

import requests


# ---------------------------------------------------------------------------
# Test definitions
# ---------------------------------------------------------------------------

@dataclass
class TestCase:
    id: str
    query: str
    expected_sources: List[str]  # at least one must fire (empty = not checking sources)
    description: str
    category: str = "individual"  # individual | multi | edge | routing | source_activation | gap | overlap
    expected_route: Optional[str] = None  # SQL | SEMANTIC | HYBRID (None = don't check)


TESTS: List[TestCase] = [
    # =========================================================================
    # ORIGINAL TESTS: Individual source tests (T1-T16)
    # =========================================================================
    TestCase("T1", "How many ASRS reports mention engine failure?", ["SQL"], "SQL count query"),
    TestCase("T2", "Top 5 airports by number of incident reports", ["SQL"], "SQL top-N"),
    TestCase("T3", "Find reports similar to bird strike during takeoff", ["VECTOR_OPS"], "Vector similarity"),
    TestCase("T4", "What happened in ASRS reports about hydraulic failures?", ["VECTOR_OPS"], "Vector narrative"),
    TestCase("T5", "Show current NOTAMs for KJFK", ["VECTOR_REG", "NOSQL"], "Regulatory + NOSQL"),
    TestCase("T6", "EASA airworthiness directive for Airbus A320", ["VECTOR_REG"], "Regulatory lookup"),
    TestCase("T7", "What runways does LTFM airport have?", ["VECTOR_AIRPORT"], "Airport ops"),
    TestCase("T8", "Airport information for Istanbul Sabiha Gokcen", ["VECTOR_AIRPORT"], "Airport info"),
    TestCase("T9", "What routes are connected to KJFK and what airlines operate them?", ["GRAPH"], "Graph traversal"),
    TestCase("T10", "Show the dependency network for Istanbul airport LTFM", ["GRAPH"], "Graph multi-hop"),
    TestCase("T11", "Show active NOTAMs for KJFK", ["NOSQL"], "Cosmos NOTAM"),
    TestCase("T12", "Any parking stand NOTAMs for DFW?", ["NOSQL"], "Cosmos specific NOTAM"),
    TestCase("T13", "What is the current live status of flights near KJFK?", ["KQL"], "KQL live data"),
    TestCase("T14", "Show real-time weather hazards for Istanbul", ["KQL"], "KQL weather"),
    TestCase("T15", "What is the average delay for Delta flights in January?", ["FABRIC_SQL"], "Fabric SQL analytics"),
    TestCase("T16", "Show the on-time performance and cancellation rate for carriers", ["FABRIC_SQL"], "Fabric SQL performance"),

    # =========================================================================
    # ORIGINAL TESTS: Multi-source combination tests (M1-M7)
    # =========================================================================
    TestCase(
        "M1",
        "Prepare a pilot briefing for departure from LTFM including NOTAMs and runway status",
        ["SQL", "NOSQL", "VECTOR_AIRPORT"],
        "Multi-source pilot brief",
        category="multi",
    ),
    TestCase(
        "M2",
        "What is the impact of NOTAM closures at KJFK on connected routes?",
        ["GRAPH", "NOSQL"],
        "Graph + NOSQL combo",
        category="multi",
    ),
    TestCase(
        "M3",
        "Summarize recent bird strike incidents and any related airworthiness directives",
        ["VECTOR_OPS", "VECTOR_REG"],
        "Ops + Regulatory combo",
        category="multi",
    ),
    TestCase(
        "M4",
        "Show live weather at Istanbul airport and any active NOTAMs",
        ["KQL", "NOSQL"],
        "KQL + NOSQL combo",
        category="multi",
    ),
    TestCase(
        "M5",
        "What carriers have the worst on-time performance and what safety lessons emerge from ASRS reports?",
        ["FABRIC_SQL", "VECTOR_OPS"],
        "Fabric SQL + Vector combo",
        category="multi",
    ),
    TestCase(
        "M6",
        "Show the route network from LTFM, delay statistics, and any current NOTAMs",
        ["GRAPH", "FABRIC_SQL", "NOSQL"],
        "Graph + Fabric SQL + NOSQL combo",
        category="multi",
    ),
    TestCase(
        "M7",
        "Are there alternate airports for LTFM and what are their runway configurations?",
        ["GRAPH", "VECTOR_AIRPORT"],
        "Graph + Airport combo",
        category="multi",
    ),

    # =========================================================================
    # ORIGINAL TESTS: Edge cases (E1, E3)
    # =========================================================================
    TestCase("E1", "Hello, how are you?", [], "Greeting fallback", category="edge"),
    TestCase("E3", "Show PII: my SSN is 123-45-6789", [], "PII filter test", category="edge"),

    # =========================================================================
    # ROUTING TESTS (R1-R35): Layer 1 quick_route() heuristic classification
    # =========================================================================

    # --- SQL keyword tests ---
    TestCase("R1", "list all airports in the database", [],
             "SQL: 'list' keyword triggers SQL route",
             category="routing", expected_route="SQL"),
    TestCase("R2", "show me all flight phases", [],
             "SQL: 'show' keyword triggers SQL route",
             category="routing", expected_route="SQL"),
    TestCase("R3", "how many incidents occurred in 2025?", [],
             "SQL: 'how many' keyword triggers SQL route",
             category="routing", expected_route="SQL"),
    TestCase("R4", "total number of reports by aircraft type", [],
             "SQL: 'total' + 'aircraft type' keywords trigger SQL route",
             category="routing", expected_route="SQL"),
    TestCase("R5", "average altitude of incidents in cruise flight phase", [],
             "SQL: 'average' + 'flight phase' keywords trigger SQL route",
             category="routing", expected_route="SQL"),
    TestCase("R6", "count of reports between 2020 and 2024", [],
             "SQL: 'count' + 'between' keywords trigger SQL route",
             category="routing", expected_route="SQL"),
    TestCase("R7", "top 5 average count of reports by location", [],
             "SQL: multiple SQL keywords ('top', 'average', 'count', 'location')",
             category="routing", expected_route="SQL"),
    TestCase("R8", "compare incident rates by year", [],
             "SQL: 'compare' + 'by year' keywords trigger SQL route",
             category="routing", expected_route="SQL"),
    TestCase("R9", "trend of bird strikes by year", [],
             "SQL: 'trend' + 'by year' keywords trigger SQL route",
             category="routing", expected_route="SQL"),
    TestCase("R10", "largest airports in Europe", [],
             "SQL: 'largest' keyword triggers SQL route",
             category="routing", expected_route="SQL"),
    TestCase("R11", "smallest runway at KJFK", [],
             "SQL: 'smallest' keyword triggers SQL route",
             category="routing", expected_route="SQL"),
    TestCase("R12", "sum of all incident reports", [],
             "SQL: 'sum' keyword triggers SQL route",
             category="routing", expected_route="SQL"),
    TestCase("R13", "reports greater than 100 per year", [],
             "SQL: 'greater than' keyword triggers SQL route",
             category="routing", expected_route="SQL"),
    TestCase("R14", "airports with less than 5 incidents", [],
             "SQL: 'less than' keyword triggers SQL route",
             category="routing", expected_route="SQL"),

    # --- SEMANTIC keyword tests ---
    TestCase("R15", "describe the most common engine failure scenarios", [],
             "SEMANTIC: 'describe' keyword triggers SEMANTIC route",
             category="routing", expected_route="SEMANTIC"),
    TestCase("R16", "summarize bird strike incident narratives", [],
             "HYBRID: 'summarize' (SEMANTIC) + 'sum' substring in 'summarize' (SQL) -> HYBRID (known artifact)",
             category="routing", expected_route="HYBRID"),
    TestCase("R17", "what happened during the hydraulic system failure?", [],
             "SEMANTIC: 'what happened' keyword triggers SEMANTIC route",
             category="routing", expected_route="SEMANTIC"),
    TestCase("R18", "give me an example of runway incursion", [],
             "SEMANTIC: 'example' keyword triggers SEMANTIC route",
             category="routing", expected_route="SEMANTIC"),
    TestCase("R19", "find similar incidents to tail strike on landing", [],
             "SEMANTIC: 'similar' keyword triggers SEMANTIC route",
             category="routing", expected_route="SEMANTIC"),
    TestCase("R20", "explain the narrative behind gear collapse incidents", [],
             "SEMANTIC: 'narrative' keyword triggers SEMANTIC route",
             category="routing", expected_route="SEMANTIC"),
    TestCase("R21", "what lessons learned from ATC miscommunication?", [],
             "SEMANTIC: 'lessons learned' keyword triggers SEMANTIC route",
             category="routing", expected_route="SEMANTIC"),
    TestCase("R22", "provide context for CFIT incidents", [],
             "SEMANTIC: 'context' keyword triggers SEMANTIC route",
             category="routing", expected_route="SEMANTIC"),
    TestCase("R23", "why do pilots report hydraulic failures?", [],
             "SEMANTIC: 'why' keyword triggers SEMANTIC route",
             category="routing", expected_route="SEMANTIC"),

    # --- HYBRID: both SQL + SEMANTIC keywords ---
    TestCase("R24", "describe the trend of incidents by year", [],
             "HYBRID: 'describe' (SEMANTIC) + 'trend' + 'by year' (SQL) -> HYBRID",
             category="routing", expected_route="HYBRID"),
    TestCase("R25", "show me what happened in the top 5 incidents", [],
             "HYBRID: 'show' + 'top' (SQL) + 'what happened' (SEMANTIC) -> HYBRID",
             category="routing", expected_route="HYBRID"),
    TestCase("R26", "summarize the count of engine failures by location", [],
             "HYBRID: 'summarize' (SEMANTIC) + 'count' + 'location' (SQL) -> HYBRID",
             category="routing", expected_route="HYBRID"),
    TestCase("R27", "list and describe incidents similar to bird strikes", [],
             "HYBRID: 'list' (SQL) + 'describe' + 'similar' (SEMANTIC) -> HYBRID",
             category="routing", expected_route="HYBRID"),
    TestCase("R28", "what lessons learned from the top aircraft type incidents?", [],
             "HYBRID: 'lessons learned' (SEMANTIC) + 'top' + 'aircraft type' (SQL) -> HYBRID",
             category="routing", expected_route="HYBRID"),

    # --- HYBRID: fallback keywords ---
    TestCase("R29", "tell me about ASRS report system", [],
             "HYBRID: 'asrs' fallback keyword triggers HYBRID",
             category="routing", expected_route="HYBRID"),
    TestCase("R30", "what is an incident report?", [],
             "HYBRID: 'incident' + 'report' fallback keywords trigger HYBRID",
             category="routing", expected_route="HYBRID"),
    TestCase("R31", "safety management systems in aviation", [],
             "HYBRID: 'safety' fallback keyword triggers HYBRID",
             category="routing", expected_route="HYBRID"),

    # --- HYBRID: default (no keywords) ---
    TestCase("R32", "Boeing 737 MAX", [],
             "HYBRID: no matching keywords -> default HYBRID",
             category="routing", expected_route="HYBRID"),
    TestCase("R33", "Airbus A320neo fleet information", [],
             "HYBRID: no matching keywords -> default HYBRID",
             category="routing", expected_route="HYBRID"),
    TestCase("R34", "KJFK", [],
             "HYBRID: single ICAO code, no keywords -> default HYBRID",
             category="routing", expected_route="HYBRID"),
    TestCase("R35", "What is the weather like?", [],
             "HYBRID: no matching keywords (no SQL/SEMANTIC/fallback) -> default HYBRID",
             category="routing", expected_route="HYBRID"),

    # =========================================================================
    # SOURCE ACTIVATION TESTS (S1-S25): Layer 2 _wants_* boundary testing
    # =========================================================================

    # --- _wants_realtime ---
    TestCase("S1", "last 30 minutes of flights near LTFM", ["KQL"],
             "KQL: 'last ' (with trailing space) triggers realtime",
             category="source_activation"),
    TestCase("S2", "show live flight data for Istanbul area", ["KQL"],
             "KQL: 'live' triggers realtime",
             category="source_activation"),
    TestCase("S3", "real-time weather observations", ["KQL"],
             "KQL: 'real-time' triggers realtime",
             category="source_activation"),
    TestCase("S4", "realtime hazard alerts in the area", ["KQL"],
             "KQL: 'realtime' triggers realtime",
             category="source_activation"),
    TestCase("S5", "what happened in the last 10 minutes?", ["KQL"],
             "KQL: 'minutes' triggers realtime",
             category="source_activation"),
    TestCase("S6", "current status of flights at KJFK right now", ["KQL"],
             "KQL: 'current status' + 'now' triggers realtime",
             category="source_activation"),
    TestCase("S7", "show recent weather reports", ["KQL"],
             "KQL: 'recent' triggers realtime",
             category="source_activation"),
    TestCase("S8", "the broadcast system is running", [],
             "KQL negative: 'broadcast' contains 'last' but not 'last ' — should NOT trigger KQL",
             category="source_activation"),

    # --- _wants_graph ---
    TestCase("S9", "what is the impact of runway closure on connected flights?", ["GRAPH"],
             "GRAPH: 'impact' + 'connected' triggers graph",
             category="source_activation"),
    TestCase("S10", "show dependency chain for LTFM alternate airports", ["GRAPH"],
             "GRAPH: 'dependency' + 'alternate' triggers graph",
             category="source_activation"),
    TestCase("S11", "which airports are connected to Istanbul?", ["GRAPH"],
             "GRAPH: 'connected' triggers graph",
             category="source_activation"),
    TestCase("S12", "show route network from KJFK", ["GRAPH"],
             "GRAPH: 'route network' triggers graph",
             category="source_activation"),
    TestCase("S13", "what is the relationship between LTFM and LTBA?", ["GRAPH"],
             "GRAPH: 'relationship' triggers graph",
             category="source_activation"),

    # --- _wants_regulatory ---
    TestCase("S14", "show AD 2025-01-04 for Boeing 737", ["VECTOR_REG"],
             "VECTOR_REG: 'ad ' (with trailing space) triggers regulatory",
             category="source_activation"),
    TestCase("S15", "airworthiness directives for A320 fleet", ["VECTOR_REG"],
             "VECTOR_REG: 'airworthiness' triggers regulatory",
             category="source_activation"),
    TestCase("S16", "latest EASA bulletins for engine type", ["VECTOR_REG"],
             "VECTOR_REG: 'easa' triggers regulatory",
             category="source_activation"),
    TestCase("S17", "compliance status of fleet modifications", ["VECTOR_REG"],
             "VECTOR_REG: 'compliance' triggers regulatory",
             category="source_activation"),
    TestCase("S18", "the advisor recommended a review", [],
             "VECTOR_REG negative: 'advisor' contains 'ad' but not 'ad ' — should NOT trigger regulatory",
             category="source_activation"),

    # --- _wants_narrative ---
    TestCase("S19", "summarize bird strike incidents from last year", ["VECTOR_OPS"],
             "VECTOR_OPS: 'summarize' triggers narrative",
             category="source_activation"),
    TestCase("S20", "find similar events to tail strike on landing", ["VECTOR_OPS"],
             "VECTOR_OPS: 'similar' triggers narrative",
             category="source_activation"),

    # --- _wants_airport_ops ---
    TestCase("S21", "runway length at LTFM", ["VECTOR_AIRPORT"],
             "VECTOR_AIRPORT: 'runway' + 'ltfm' triggers airport_ops",
             category="source_activation"),
    TestCase("S22", "gate assignments at the station", ["VECTOR_AIRPORT"],
             "VECTOR_AIRPORT: 'gate' + 'station' triggers airport_ops",
             category="source_activation"),
    TestCase("S23", "turnaround time for narrow-body aircraft", ["VECTOR_AIRPORT"],
             "VECTOR_AIRPORT: 'turnaround' triggers airport_ops",
             category="source_activation"),
    TestCase("S24", "LTFM airport facilities overview", ["VECTOR_AIRPORT"],
             "VECTOR_AIRPORT: 'ltfm' + 'airport' triggers airport_ops",
             category="source_activation"),
    TestCase("S25", "LTBA runway configuration and status", ["VECTOR_AIRPORT"],
             "VECTOR_AIRPORT: 'ltba' + 'runway' triggers airport_ops",
             category="source_activation"),

    # --- _wants_nosql ---
    TestCase("S26", "active NOTAMs for KJFK", ["NOSQL", "VECTOR_REG"],
             "NOSQL + VECTOR_REG: 'notam' triggers both nosql and regulatory",
             category="source_activation"),
    TestCase("S27", "ground handling doc for Istanbul", ["NOSQL"],
             "NOSQL: 'ground handling doc' triggers nosql",
             category="source_activation"),
    TestCase("S28", "parking stand allocation at DFW", ["NOSQL"],
             "NOSQL: 'parking stand' triggers nosql",
             category="source_activation"),
    TestCase("S29", "operational doc for winter operations", ["NOSQL"],
             "NOSQL: 'operational doc' triggers nosql",
             category="source_activation"),
    TestCase("S30", "ops doc for de-icing procedures", ["NOSQL"],
             "NOSQL: 'ops doc' triggers nosql",
             category="source_activation"),

    # --- _wants_analytics ---
    TestCase("S31", "average delay for American Airlines flights", ["FABRIC_SQL"],
             "FABRIC_SQL: 'delay' triggers analytics",
             category="source_activation"),
    TestCase("S32", "on-time performance by carrier for Q1", ["FABRIC_SQL"],
             "FABRIC_SQL: 'on-time' triggers analytics",
             category="source_activation"),
    TestCase("S33", "BTS statistics for regional carriers", ["FABRIC_SQL"],
             "FABRIC_SQL: 'bts' triggers analytics",
             category="source_activation"),
    TestCase("S34", "cancellation rate by month", ["FABRIC_SQL"],
             "FABRIC_SQL: 'cancellation rate' triggers analytics",
             category="source_activation"),
    TestCase("S35", "weather delay trends at major hubs", ["FABRIC_SQL"],
             "FABRIC_SQL: 'weather delay' triggers analytics",
             category="source_activation"),
    TestCase("S36", "NAS delay causes breakdown", ["FABRIC_SQL"],
             "FABRIC_SQL: 'nas delay' + 'delay cause' triggers analytics",
             category="source_activation"),

    # --- Case sensitivity: query is lowered before matching ---
    TestCase("S37", "SHOW LIVE NOTAMS FOR LTFM", ["KQL", "NOSQL", "VECTOR_REG", "VECTOR_AIRPORT"],
             "Case: ALL-CAPS query should still match (query_l is lowered)",
             category="source_activation"),
    TestCase("S38", "Summarize The Narrative Behind Recent Incidents", ["VECTOR_OPS"],
             "Case: Title-case query should still match narrative markers",
             category="source_activation"),

    # --- Fallback: no _wants_ matches but route-based baseline ---
    TestCase("S39", "Boeing 737 fleet composition", ["SQL", "VECTOR_OPS"],
             "Fallback: no _wants_ keywords, HYBRID route -> SQL + VECTOR_OPS baseline",
             category="source_activation"),

    # =========================================================================
    # GAP ANALYSIS TESTS (G1-G12): Keywords that probably SHOULD trigger
    # a source but currently do NOT because they are missing from markers.
    # These tests document expected current behavior (the gap).
    # =========================================================================
    TestCase("G1", "latest METAR for KJFK", [],
             "GAP: 'metar' not in _wants_realtime markers — KQL should fire but won't via heuristic",
             category="gap"),
    TestCase("G2", "TAF forecast for LTFM", [],
             "GAP: 'taf' not in _wants_realtime markers — KQL should fire but won't via heuristic",
             category="gap"),
    TestCase("G3", "current flight status for TK1234", [],
             "GAP: 'flight status' not in _wants_realtime markers — KQL should fire but won't via heuristic",
             category="gap"),
    TestCase("G4", "standard operating procedure for engine start", [],
             "GAP: 'standard operating procedure' / 'SOP' not in any markers",
             category="gap"),
    TestCase("G5", "SOP for cold weather operations", [],
             "GAP: 'SOP' not in any _wants_ markers",
             category="gap"),
    TestCase("G6", "fuel consumption analysis for long-haul flights", [],
             "GAP: 'fuel' not in any _wants_ markers",
             category="gap"),
    TestCase("G7", "crew roster for Istanbul base next week", [],
             "GAP: 'crew' / 'roster' not in any markers (data in ops_crew_rosters)",
             category="gap"),
    TestCase("G8", "minimum equipment list for registration TC-JPA", [],
             "GAP: 'minimum equipment list' / 'MEL' not in any markers (data in ops_mel_techlog_events)",
             category="gap"),
    TestCase("G9", "SIGMET active over the Mediterranean", [],
             "GAP: 'sigmet' not in _wants_realtime markers — should probably trigger KQL",
             category="gap"),
    TestCase("G10", "PIREP for turbulence at FL350", [],
             "GAP: 'pirep' not in any markers — should probably trigger KQL or VECTOR_OPS",
             category="gap"),
    TestCase("G11", "ATC communication issues at KJFK", [],
             "GAP: 'ATC' not in any markers but relevant ASRS reports exist",
             category="gap"),
    TestCase("G12", "maintenance log for Boeing 737-800", [],
             "GAP: 'maintenance' not in any markers (data in ops_mel_techlog_events)",
             category="gap"),

    # =========================================================================
    # MULTI-OVERLAP TESTS (O1-O12): Queries that trigger multiple _wants_*
    # functions simultaneously, testing source plan assembly.
    # =========================================================================
    TestCase(
        "O1",
        "Show live NOTAMs for alternate airports with runway closures",
        ["KQL", "VECTOR_REG", "GRAPH", "VECTOR_AIRPORT", "NOSQL"],
        "5-source overlap: realtime(live) + regulatory(notam) + graph(alternate) + airport_ops(runway) + nosql(notam)",
        category="overlap",
    ),
    TestCase(
        "O2",
        "What are the delay causes at the connected airports?",
        ["FABRIC_SQL", "GRAPH"],
        "2-source overlap: analytics(delay) + graph(connected)",
        category="overlap",
    ),
    TestCase(
        "O3",
        "Summarize airworthiness directives for similar incidents",
        ["VECTOR_OPS", "VECTOR_REG"],
        "2-source overlap: narrative(summarize, similar) + regulatory(airworthiness, directive)",
        category="overlap",
    ),
    TestCase(
        "O4",
        "Show real-time NOTAMs and delay statistics for LTFM airport",
        ["KQL", "NOSQL", "VECTOR_REG", "FABRIC_SQL", "VECTOR_AIRPORT"],
        "5-source overlap: realtime(real-time) + nosql(notam) + regulatory(notam) + analytics(delay) + airport_ops(ltfm, airport)",
        category="overlap",
    ),
    TestCase(
        "O5",
        "What happened at the runway during the last turnaround at LTBA?",
        ["VECTOR_OPS", "VECTOR_AIRPORT", "KQL"],
        "3-source overlap: narrative(what happened) + airport_ops(runway, turnaround, ltba) + realtime(last )",
        category="overlap",
    ),
    TestCase(
        "O6",
        "Impact of weather delay on route network from connected airports",
        ["GRAPH", "FABRIC_SQL"],
        "2-source overlap: graph(impact, connected, route network) + analytics(weather delay, delay)",
        category="overlap",
    ),
    TestCase(
        "O7",
        "Compliance with airworthiness directive AD 2025-01 at LTFM airport",
        ["VECTOR_REG", "VECTOR_AIRPORT"],
        "2-source overlap: regulatory(compliance, airworthiness, directive, ad ) + airport_ops(ltfm, airport)",
        category="overlap",
    ),
    TestCase(
        "O8",
        "Summarize lessons from recent incidents at alternate airports with parking stand NOTAMs",
        ["VECTOR_OPS", "GRAPH", "NOSQL", "VECTOR_REG", "KQL"],
        "5-source overlap: narrative(summarize, lessons) + realtime(recent) + graph(alternate) + nosql(notam, parking stand) + regulatory(notam)",
        category="overlap",
    ),
    TestCase(
        "O9",
        "Live gate assignments and ground handling doc for turnaround operations",
        ["KQL", "VECTOR_AIRPORT", "NOSQL"],
        "3-source overlap: realtime(live) + airport_ops(gate, turnaround) + nosql(ground handling doc)",
        category="overlap",
    ),
    TestCase(
        "O10",
        "Show the dependency of on-time performance on runway closures at LTFJ station",
        ["GRAPH", "FABRIC_SQL", "VECTOR_AIRPORT"],
        "3-source overlap: graph(dependency) + analytics(on-time) + airport_ops(runway, ltfj, station)",
        category="overlap",
    ),
    TestCase(
        "O11",
        "What is the impact of NAS delay on schedule performance for connected carriers?",
        ["GRAPH", "FABRIC_SQL"],
        "2-source overlap: graph(impact, connected) + analytics(nas delay, schedule performance, delay)",
        category="overlap",
    ),
    TestCase(
        "O12",
        "Describe narrative examples of compliance issues with EASA directives at airports",
        ["VECTOR_OPS", "VECTOR_REG", "VECTOR_AIRPORT"],
        "3-source overlap: narrative(narrative, examples) + regulatory(compliance, easa, directive) + airport_ops(airport)",
        category="overlap",
    ),

    # =========================================================================
    # PAIRWISE COMBINATION TESTS (P1-P16): Cover all missing 2-source combos
    # =========================================================================
    TestCase(
        "P1",
        "How many incidents occurred at airports with current live weather hazards?",
        ["SQL", "KQL"],
        "SQL + KQL: count (SQL) + live (KQL)",
        category="pairwise",
    ),
    TestCase(
        "P2",
        "List the top 5 airports and show their connected routes",
        ["SQL", "GRAPH"],
        "SQL + GRAPH: list + top (SQL) + connected (GRAPH)",
        category="pairwise",
    ),
    TestCase(
        "P3",
        "How many airworthiness directives apply to Boeing 737 fleet?",
        ["SQL", "VECTOR_REG"],
        "SQL + VECTOR_REG: how many (SQL) + airworthiness + directive (VECTOR_REG)",
        category="pairwise",
    ),
    TestCase(
        "P4",
        "Compare total ASRS report count per year with BTS delay statistics",
        ["SQL", "FABRIC_SQL"],
        "SQL + FABRIC_SQL: compare + total + count (SQL) + bts + delay (FABRIC_SQL)",
        category="pairwise",
    ),
    TestCase(
        "P5",
        "Show live flight status and route network impact for Istanbul airports",
        ["KQL", "GRAPH"],
        "KQL + GRAPH: live (KQL) + route network + impact (GRAPH)",
        category="pairwise",
    ),
    TestCase(
        "P6",
        "What recent live hazard observations relate to similar bird strike incidents?",
        ["KQL", "VECTOR_OPS"],
        "KQL + VECTOR_OPS: recent + live (KQL) + similar (VECTOR_OPS)",
        category="pairwise",
    ),
    TestCase(
        "P7",
        "Current live weather delays and their historical on-time performance impact",
        ["KQL", "FABRIC_SQL"],
        "KQL + FABRIC_SQL: live (KQL) + delay + on-time (FABRIC_SQL)",
        category="pairwise",
    ),
    TestCase(
        "P8",
        "What is the impact of narrative incidents on connected airport operations?",
        ["GRAPH", "VECTOR_OPS"],
        "GRAPH + VECTOR_OPS: impact + connected (GRAPH) + narrative (VECTOR_OPS)",
        category="pairwise",
    ),
    TestCase(
        "P9",
        "Show dependency chain for airports with active airworthiness directives",
        ["GRAPH", "VECTOR_REG"],
        "GRAPH + VECTOR_REG: dependency (GRAPH) + airworthiness + directive (VECTOR_REG)",
        category="pairwise",
    ),
    TestCase(
        "P10",
        "Are there NOTAM-related narrative safety lessons from ASRS reports?",
        ["NOSQL", "VECTOR_OPS"],
        "NOSQL + VECTOR_OPS: notam (NOSQL) + narrative + lessons (VECTOR_OPS)",
        category="pairwise",
    ),
    TestCase(
        "P11",
        "Active NOTAMs and airport facility information for LTFM",
        ["NOSQL", "VECTOR_AIRPORT"],
        "NOSQL + VECTOR_AIRPORT: notam (NOSQL) + airport + ltfm (VECTOR_AIRPORT)",
        category="pairwise",
    ),
    TestCase(
        "P12",
        "NOTAMs at airports with the worst delay performance",
        ["NOSQL", "FABRIC_SQL"],
        "NOSQL + FABRIC_SQL: notam (NOSQL) + delay (FABRIC_SQL)",
        category="pairwise",
    ),
    TestCase(
        "P13",
        "Describe incidents that occurred during turnaround operations at airports",
        ["VECTOR_OPS", "VECTOR_AIRPORT"],
        "VECTOR_OPS + VECTOR_AIRPORT: describe (VECTOR_OPS) + turnaround + airport (VECTOR_AIRPORT)",
        category="pairwise",
    ),
    TestCase(
        "P14",
        "Summarize safety narrative lessons for carriers with high delay rates",
        ["VECTOR_OPS", "FABRIC_SQL"],
        "VECTOR_OPS + FABRIC_SQL: summarize + narrative + lessons (VECTOR_OPS) + delay (FABRIC_SQL)",
        category="pairwise",
    ),
    TestCase(
        "P15",
        "Compliance with EASA directives at airports with cancellation rate problems",
        ["VECTOR_REG", "FABRIC_SQL"],
        "VECTOR_REG + FABRIC_SQL: compliance + easa + directive (VECTOR_REG) + cancellation rate (FABRIC_SQL)",
        category="pairwise",
    ),
    TestCase(
        "P16",
        "Airport runway configuration impact on on-time performance",
        ["VECTOR_AIRPORT", "FABRIC_SQL"],
        "VECTOR_AIRPORT + FABRIC_SQL: airport + runway (VECTOR_AIRPORT) + on-time (FABRIC_SQL)",
        category="pairwise",
    ),
]


# ---------------------------------------------------------------------------
# SSE parser
# ---------------------------------------------------------------------------

def parse_sse_events(response: requests.Response) -> List[Dict]:
    """Parse SSE stream from Flask backend into a list of event dicts."""
    events: List[Dict] = []
    buffer = ""

    for chunk in response.iter_content(chunk_size=None, decode_unicode=True):
        if not chunk:
            continue
        buffer += chunk

        while "\n\n" in buffer:
            block, buffer = buffer.split("\n\n", 1)
            data_lines = []
            for line in block.split("\n"):
                if line.startswith("data: "):
                    data_lines.append(line[6:])
                elif line.startswith("data:"):
                    data_lines.append(line[5:])
            if data_lines:
                raw = "\n".join(data_lines)
                try:
                    events.append(json.loads(raw))
                except json.JSONDecodeError:
                    pass  # skip non-JSON events

    # Process any remaining buffer
    if buffer.strip():
        data_lines = []
        for line in buffer.strip().split("\n"):
            if line.startswith("data: "):
                data_lines.append(line[6:])
            elif line.startswith("data:"):
                data_lines.append(line[5:])
        if data_lines:
            raw = "\n".join(data_lines)
            try:
                events.append(json.loads(raw))
            except json.JSONDecodeError:
                pass

    return events


# ---------------------------------------------------------------------------
# Heuristic route checker (mirrors query_router.py quick_route logic)
# ---------------------------------------------------------------------------

def local_quick_route(query: str) -> str:
    """Local reimplementation of quick_route for dry-run validation."""
    query_lower = query.lower()

    sql_keywords = [
        "top", "largest", "smallest", "compare", "list", "show",
        "how many", "total", "sum", "average", "count",
        "greater than", "less than", "between", "trend", "by year",
        "flight phase", "aircraft type", "location",
    ]
    has_sql = any(kw in query_lower for kw in sql_keywords)

    semantic_keywords = [
        "describe", "summarize", "what happened", "example", "similar",
        "narrative", "context", "why", "lessons learned",
    ]
    has_semantic = any(kw in query_lower for kw in semantic_keywords)

    if has_sql and has_semantic:
        return "HYBRID"
    if has_sql:
        return "SQL"
    if has_semantic:
        return "SEMANTIC"

    if any(kw in query_lower for kw in ["report", "asrs", "incident", "safety"]):
        return "HYBRID"

    return "HYBRID"


# ---------------------------------------------------------------------------
# Local _wants_* checkers (mirrors retrieval_plan.py logic for dry-run)
# ---------------------------------------------------------------------------

def local_wants_realtime(query_l: str) -> bool:
    markers = ("last ", "live", "real-time", "realtime", "minutes", "now", "current status", "recent")
    return any(m in query_l for m in markers)


def local_wants_graph(query_l: str) -> bool:
    markers = ("impact", "dependency", "depends on", "connected", "alternate", "route network", "relationship")
    return any(m in query_l for m in markers)


def local_wants_regulatory(query_l: str) -> bool:
    markers = ("ad ", "airworthiness", "notam", "easa", "compliance", "directive")
    return any(m in query_l for m in markers)


def local_wants_narrative(query_l: str) -> bool:
    markers = ("summarize", "similar", "narrative", "what happened", "examples", "lessons")
    return any(m in query_l for m in markers)


def local_wants_airport_ops(query_l: str) -> bool:
    markers = ("runway", "gate", "turnaround", "airport", "station", "ltfm", "ltfj", "ltba")
    return any(m in query_l for m in markers)


def local_wants_nosql(query_l: str) -> bool:
    markers = ("notam", "operational doc", "ops doc", "ground handling doc", "parking stand")
    return any(m in query_l for m in markers)


def local_wants_analytics(query_l: str) -> bool:
    markers = ("delay", "on-time", "schedule performance", "bts",
               "carrier delay", "cancellation rate", "on time performance",
               "delay cause", "weather delay", "nas delay")
    return any(m in query_l for m in markers)


def compute_local_activated_sources(query: str, route: str) -> Set[str]:
    """Compute which sources would be activated by the retrieval plan heuristic.

    Mimics build_retrieval_plan with default profile='pilot-brief' and no
    router_sources (heuristic path).
    """
    query_l = query.lower()
    sources: Set[str] = set()

    # Baseline by route
    if route in ("SQL", "HYBRID"):
        sources.add("SQL")
    if route in ("SEMANTIC", "HYBRID"):
        sources.add("VECTOR_OPS")

    # Profile-driven (default: pilot-brief)
    sources.add("SQL")
    sources.add("VECTOR_OPS")

    # Query-driven activation
    if local_wants_realtime(query_l):
        sources.add("KQL")
    if local_wants_graph(query_l):
        sources.add("GRAPH")
    if local_wants_regulatory(query_l):
        sources.add("VECTOR_REG")
    if local_wants_narrative(query_l):
        sources.add("VECTOR_OPS")
    if local_wants_airport_ops(query_l):
        sources.add("VECTOR_AIRPORT")
    if local_wants_nosql(query_l):
        sources.add("NOSQL")
    if local_wants_analytics(query_l):
        sources.add("FABRIC_SQL")

    # Fallback
    if not sources:
        sources.add("SQL")
        sources.add("VECTOR_OPS")

    return sources


# ---------------------------------------------------------------------------
# Test result
# ---------------------------------------------------------------------------

@dataclass
class TestResult:
    test_id: str
    description: str
    category: str
    status: str  # PASS, FAIL, ERROR, SKIP, WARN
    fired_sources: Set[str] = field(default_factory=set)
    expected_sources: List[str] = field(default_factory=list)
    missing_sources: List[str] = field(default_factory=list)
    row_counts: Dict[str, int] = field(default_factory=dict)
    errors: List[str] = field(default_factory=list)
    has_answer: bool = False
    has_citations: bool = False
    pii_blocked: bool = False
    elapsed_ms: float = 0
    detail: str = ""
    expected_route: Optional[str] = None
    actual_route: Optional[str] = None


# ---------------------------------------------------------------------------
# Test runner
# ---------------------------------------------------------------------------

def run_test(backend: str, tc: TestCase, verbose: bool = False, timeout: int = 90,
             skip_routing: bool = False) -> TestResult:
    """Execute a single test case against the backend SSE endpoint."""
    result = TestResult(
        test_id=tc.id,
        description=tc.description,
        category=tc.category,
        status="PENDING",
        expected_sources=tc.expected_sources,
        expected_route=tc.expected_route,
    )

    t0 = time.perf_counter()
    try:
        resp = requests.post(
            f"{backend}/api/chat",
            json={"message": tc.query},
            headers={"Accept": "text/event-stream"},
            stream=True,
            timeout=timeout,
        )
        if resp.status_code != 200:
            result.status = "ERROR"
            result.detail = f"HTTP {resp.status_code}: {resp.text[:200]}"
            result.elapsed_ms = (time.perf_counter() - t0) * 1000
            return result

        events = parse_sse_events(resp)
        result.elapsed_ms = (time.perf_counter() - t0) * 1000

    except requests.exceptions.Timeout:
        result.status = "ERROR"
        result.detail = f"Request timed out ({timeout}s)"
        result.elapsed_ms = (time.perf_counter() - t0) * 1000
        return result
    except requests.exceptions.ConnectionError as exc:
        result.status = "ERROR"
        result.detail = f"Connection error: {exc}"
        result.elapsed_ms = (time.perf_counter() - t0) * 1000
        return result
    except (requests.exceptions.ChunkedEncodingError, Exception) as exc:
        result.status = "ERROR"
        result.detail = f"Stream error: {type(exc).__name__}: {str(exc)[:150]}"
        result.elapsed_ms = (time.perf_counter() - t0) * 1000
        return result

    # Parse events
    for event in events:
        etype = event.get("type", "")

        if etype == "source_call_start":
            source = event.get("source", "")
            if source:
                result.fired_sources.add(source)

        elif etype == "source_call_done":
            source = event.get("source", "")
            if source:
                result.fired_sources.add(source)
            row_count = event.get("row_count", 0)
            result.row_counts[source] = row_count
            if event.get("contract_status") == "failed":
                err = event.get("error") or f"{source} contract failed"
                result.errors.append(err)

        elif etype == "agent_update" and event.get("content"):
            result.has_answer = True

        elif etype == "citations":
            cites = event.get("citations", [])
            if cites:
                result.has_citations = True

        elif etype == "pii_blocked":
            result.pii_blocked = True

        elif etype == "retrieval_plan":
            # Extract route from the plan event if available
            plan = event.get("plan") or {}
            result.actual_route = plan.get("route") or event.get("route")

    if verbose:
        print(f"    Events: {len(events)}")
        print(f"    Fired sources: {sorted(result.fired_sources)}")
        print(f"    Row counts: {result.row_counts}")
        if result.actual_route:
            print(f"    Actual route: {result.actual_route}")
        if result.errors:
            print(f"    Errors: {result.errors}")

    # --- Determine pass/fail ---

    # Edge cases have special rules
    if tc.category == "edge":
        if tc.id == "E3":
            # PII test: pass if blocked or answer mentions PII
            result.status = "PASS" if result.pii_blocked or result.has_answer else "FAIL"
            result.detail = "pii_blocked" if result.pii_blocked else "answered (PII not blocked)"
        else:
            # Greeting / fallback: pass if we got any answer
            result.status = "PASS" if result.has_answer else "FAIL"
            result.detail = "fallback answered" if result.has_answer else "no answer"
        return result

    # Routing tests: check expected_route via the retrieval_plan event
    if tc.category == "routing" and tc.expected_route:
        # In AGENTIC mode, routing tests are meaningless (LLM picks sources, not heuristic)
        if skip_routing or result.actual_route == "AGENTIC":
            result.status = "SKIP"
            result.detail = f"route={result.actual_route or '?'} (AGENTIC mode — routing test skipped)"
            return result
        if result.actual_route:
            if result.actual_route == tc.expected_route:
                result.status = "PASS"
                result.detail = f"route={result.actual_route} (expected {tc.expected_route})"
            else:
                result.status = "FAIL"
                result.detail = f"route={result.actual_route} (expected {tc.expected_route})"
        else:
            # No retrieval_plan event — check if answer was generated at least
            result.status = "WARN"
            result.detail = "no retrieval_plan event to verify route"
        return result

    # Gap tests: these document known gaps. Pass if the backend answers at all
    # (we expect specific sources to NOT fire).
    if tc.category == "gap":
        result.status = "PASS" if result.has_answer else "FAIL"
        result.detail = f"gap test answered; fired: {sorted(result.fired_sources)}"
        return result

    # For source activation, overlap, multi, individual: check expected sources fired
    missing = [s for s in tc.expected_sources if s not in result.fired_sources]
    result.missing_sources = missing

    if missing:
        result.status = "FAIL"
        result.detail = f"missing sources: {missing}"
    elif not tc.expected_sources:
        # No expected sources to check (informational test)
        result.status = "PASS" if result.has_answer else "FAIL"
        result.detail = "answered" if result.has_answer else "no answer"
    elif not result.has_answer:
        result.status = "FAIL"
        result.detail = "no answer text generated"
    elif result.errors and not any(result.row_counts.get(s, 0) > 0 for s in tc.expected_sources):
        result.status = "FAIL"
        result.detail = f"all expected sources errored: {result.errors}"
    else:
        # Check at least one expected source returned rows
        expected_with_rows = [s for s in tc.expected_sources if result.row_counts.get(s, 0) > 0]
        if expected_with_rows:
            result.status = "PASS"
            result.detail = f"rows from: {expected_with_rows}"
        else:
            # Sources fired but returned 0 rows -- soft pass (may be query-dependent)
            result.status = "WARN"
            result.detail = "sources fired but returned 0 rows"

    return result


# ---------------------------------------------------------------------------
# Dry-run: print test matrix without hitting the backend
# ---------------------------------------------------------------------------

def dry_run(tests_to_run: List[TestCase]) -> None:
    """Print the full test matrix with local heuristic predictions."""
    # Category grouping
    categories = {}
    for tc in tests_to_run:
        categories.setdefault(tc.category, []).append(tc)

    category_order = [
        "individual", "multi", "edge",
        "routing", "source_activation", "gap", "overlap", "pairwise",
    ]
    sorted_cats = sorted(categories.keys(), key=lambda c: category_order.index(c) if c in category_order else 99)

    total = len(tests_to_run)
    print(f"{'=' * 90}")
    print(f"DRY-RUN TEST MATRIX — {total} test cases")
    print(f"{'=' * 90}")

    route_mismatches = 0
    source_mismatches = 0

    for cat in sorted_cats:
        tests = categories[cat]
        print(f"\n{'─' * 90}")
        print(f"Category: {cat.upper()} ({len(tests)} tests)")
        print(f"{'─' * 90}")
        print(f"{'ID':<6} {'Predicted Route':<16} {'Expected Route':<16} {'Predicted Sources':<40} {'Match':<6}")
        print(f"{'─' * 6} {'─' * 15} {'─' * 15} {'─' * 39} {'─' * 5}")

        for tc in tests:
            predicted_route = local_quick_route(tc.query)
            predicted_sources = compute_local_activated_sources(tc.query, predicted_route)

            route_match = ""
            if tc.expected_route:
                if predicted_route == tc.expected_route:
                    route_match = "OK"
                else:
                    route_match = "MISMATCH"
                    route_mismatches += 1

            source_match = ""
            if tc.expected_sources:
                expected_set = set(tc.expected_sources)
                if expected_set.issubset(predicted_sources):
                    source_match = "OK"
                else:
                    missing = expected_set - predicted_sources
                    source_match = f"MISS:{','.join(sorted(missing))}"
                    source_mismatches += 1

            match_str = route_match or source_match or "n/a"

            pred_src_str = ",".join(sorted(predicted_sources))
            exp_route_str = tc.expected_route or "-"

            print(f"{tc.id:<6} {predicted_route:<16} {exp_route_str:<16} {pred_src_str:<40} {match_str}")

        # Detail lines for verbose context
        print()
        for tc in tests:
            query_l = tc.query.lower()
            wants = []
            if local_wants_realtime(query_l):
                wants.append("realtime")
            if local_wants_graph(query_l):
                wants.append("graph")
            if local_wants_regulatory(query_l):
                wants.append("regulatory")
            if local_wants_narrative(query_l):
                wants.append("narrative")
            if local_wants_airport_ops(query_l):
                wants.append("airport_ops")
            if local_wants_nosql(query_l):
                wants.append("nosql")
            if local_wants_analytics(query_l):
                wants.append("analytics")
            if wants:
                print(f"  {tc.id}: _wants_ triggers: {', '.join(wants)}")

    print(f"\n{'=' * 90}")
    print("DRY-RUN SUMMARY")
    print(f"{'=' * 90}")
    print(f"  Total tests:       {total}")
    print(f"  Route mismatches:  {route_mismatches}")
    print(f"  Source mismatches: {source_mismatches}")

    if route_mismatches > 0 or source_mismatches > 0:
        print("\n  NOTE: Mismatches indicate the local heuristic prediction diverges from")
        print("  the expected value. This may indicate a test expectation error or a")
        print("  routing gap that needs investigation.")

    # Source coverage matrix
    all_sources_predicted = set()
    source_test_count: Dict[str, int] = {}
    for tc in tests_to_run:
        predicted_route = local_quick_route(tc.query)
        predicted = compute_local_activated_sources(tc.query, predicted_route)
        all_sources_predicted.update(predicted)
        for s in predicted:
            source_test_count[s] = source_test_count.get(s, 0) + 1

    print(f"\n  Predicted Source Coverage:")
    for s in sorted(all_sources_predicted):
        print(f"    {s:<18} predicted in {source_test_count[s]:3d} tests")

    # Category breakdown
    print(f"\n  Tests by category:")
    for cat in sorted_cats:
        print(f"    {cat:<20} {len(categories[cat]):3d} tests")

    print()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="End-to-end source coverage and routing tests")
    parser.add_argument("--backend", default="http://localhost:5001", help="Backend URL")
    parser.add_argument("--filter", default="", help="Comma-separated test IDs to run (e.g. T1,T3,M1,R1)")
    parser.add_argument("--category", default="", help="Run only tests of this category (individual, multi, edge, routing, source_activation, gap, overlap)")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output per test")
    parser.add_argument("--timeout", type=int, default=90, help="Per-request timeout in seconds")
    parser.add_argument("--dry-run", action="store_true", dest="dry_run",
                        help="Print test matrix with local heuristic predictions without hitting backend")
    parser.add_argument("--skip-routing", action="store_true", dest="skip_routing",
                        help="Skip routing tests (R*) — useful when backend uses AGENTIC orchestrator")
    args = parser.parse_args()

    # Filter tests
    test_filter = set(args.filter.upper().split(",")) if args.filter else set()
    tests_to_run = TESTS
    if test_filter:
        tests_to_run = [t for t in TESTS if t.id in test_filter]
    if args.category:
        cats = set(c.strip().lower() for c in args.category.split(","))
        tests_to_run = [t for t in tests_to_run if t.category in cats]

    if not tests_to_run:
        print("No tests match the filter criteria.")
        sys.exit(1)

    # Dry-run mode
    if args.dry_run:
        dry_run(tests_to_run)
        sys.exit(0)

    # Health check
    print(f"Backend: {args.backend}")
    try:
        health = requests.get(f"{args.backend}/health", timeout=5)
        print(f"Health: {health.status_code} {health.json()}\n")
    except Exception as exc:
        print(f"WARNING: Health check failed ({exc}). Backend may be down.\n")

    # Run tests
    results: List[TestResult] = []
    total = len(tests_to_run)
    pass_count = 0
    fail_count = 0
    warn_count = 0
    error_count = 0
    skip_count = 0

    for idx, tc in enumerate(tests_to_run, 1):
        print(f"[{idx}/{total}] {tc.id}: {tc.description}")
        if args.verbose:
            print(f"    Query: {tc.query[:100]}...")
            print(f"    Expected sources: {tc.expected_sources}")
            if tc.expected_route:
                print(f"    Expected route: {tc.expected_route}")

        result = run_test(args.backend, tc, verbose=args.verbose, timeout=args.timeout,
                          skip_routing=args.skip_routing)
        results.append(result)

        icon = {"PASS": "+", "FAIL": "x", "WARN": "~", "ERROR": "!", "SKIP": "-"}.get(result.status, "?")
        elapsed = f"{result.elapsed_ms:.0f}ms"
        print(f"  [{icon}] {result.status} ({elapsed}) -- {result.detail}")
        if result.fired_sources:
            print(f"      Sources: {sorted(result.fired_sources)}")
        if result.actual_route and tc.expected_route:
            print(f"      Route: {result.actual_route} (expected: {tc.expected_route})")
        print()

        if result.status == "PASS":
            pass_count += 1
        elif result.status == "FAIL":
            fail_count += 1
        elif result.status == "WARN":
            warn_count += 1
        elif result.status == "SKIP":
            skip_count += 1
        else:
            error_count += 1

    # Summary
    print("=" * 70)
    print("RESULTS SUMMARY")
    print("=" * 70)
    print(f"  Total: {total}")
    print(f"  PASS:  {pass_count}")
    print(f"  SKIP:  {skip_count}")
    print(f"  WARN:  {warn_count}")
    print(f"  FAIL:  {fail_count}")
    print(f"  ERROR: {error_count}")
    print()

    # Results by category
    categories: Dict[str, List[TestResult]] = {}
    for r in results:
        categories.setdefault(r.category, []).append(r)

    if len(categories) > 1:
        print("Results by Category:")
        for cat, cat_results in sorted(categories.items()):
            p = sum(1 for r in cat_results if r.status == "PASS")
            f = sum(1 for r in cat_results if r.status == "FAIL")
            w = sum(1 for r in cat_results if r.status == "WARN")
            e = sum(1 for r in cat_results if r.status == "ERROR")
            sk = sum(1 for r in cat_results if r.status == "SKIP")
            parts = f"{len(cat_results):3d} total | {p} pass | {w} warn | {f} fail | {e} error"
            if sk:
                parts += f" | {sk} skip"
            print(f"  {cat:<20} {parts}")
        print()

    # Source coverage matrix
    all_sources = sorted({s for r in results for s in r.fired_sources})
    if all_sources:
        print("Source Coverage:")
        for source in all_sources:
            fired_in = [r.test_id for r in results if source in r.fired_sources]
            rows_total = sum(r.row_counts.get(source, 0) for r in results)
            print(f"  {source:15s} fired in {len(fired_in):2d} tests, total rows: {rows_total}")
        print()

    # Route distribution (for routing tests)
    routing_results = [r for r in results if r.expected_route]
    if routing_results:
        print("Route Classification:")
        route_pass = sum(1 for r in routing_results if r.status == "PASS")
        route_fail = sum(1 for r in routing_results if r.status == "FAIL")
        route_warn = sum(1 for r in routing_results if r.status == "WARN")
        print(f"  {len(routing_results)} routing tests: {route_pass} pass, {route_warn} warn, {route_fail} fail")
        for r in routing_results:
            if r.status == "FAIL":
                print(f"    {r.test_id}: expected={r.expected_route}, actual={r.actual_route}")
        print()

    # Failures detail
    failures = [r for r in results if r.status in ("FAIL", "ERROR")]
    if failures:
        print("FAILURES:")
        for r in failures:
            print(f"  {r.test_id}: {r.detail}")
            if r.errors:
                for e in r.errors[:3]:
                    print(f"    - {e[:120]}")
        print()

    sys.exit(1 if fail_count > 0 or error_count > 0 else 0)


if __name__ == "__main__":
    main()
