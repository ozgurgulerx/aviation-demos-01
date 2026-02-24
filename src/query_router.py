#!/usr/bin/env python3
"""
Query Router - Classifies queries into retrieval paths.
Routes: SQL, SEMANTIC, HYBRID
"""

import json
import logging
import os

from azure_openai_client import get_shared_client
from shared_utils import OPENAI_API_VERSION

logger = logging.getLogger(__name__)


ROUTING_PROMPT = """You are the retrieval planner for an aviation intelligence platform.
Your job is to (1) classify the user query route and (2) select the optimal combination
of data sources to retrieve the evidence needed for a comprehensive answer.

═══════════════════════════════════════════════════════════════════
 DATA SOURCES — STRUCTURED
═══════════════════════════════════════════════════════════════════

## 1. SQL — PostgreSQL Warehouse
Relational database with deterministic, structured aviation data.

Tables and key columns:
  asrs_reports — ASRS safety reports
    asrs_report_id, event_date, location, aircraft_type, flight_phase,
    narrative_type, title, report_text

  ops_flight_legs — Flight schedule, actuals, and delay metrics
    leg_id, source, carrier_code, flight_no, origin_iata, dest_iata,
    scheduled_dep_utc, scheduled_arr_utc, actual_dep_utc, actual_arr_utc,
    dep_delay_min, arr_delay_min, tailnum, distance_nm, passengers

  ops_turnaround_milestones — Ground handling milestones with delay cause
    milestone_id, leg_id, milestone, event_ts_utc, status, delay_cause_code

  ops_crew_rosters — Crew duty assignments and legality tracking
    duty_id, crew_id, role, leg_id, duty_start_utc, duty_end_utc,
    cumulative_duty_hours, legality_risk_flag

  ops_mel_techlog_events — MEL and technical log entries
    tech_event_id, leg_id, event_ts_utc, jasc_code,
    mel_category, deferred_flag, severity

  ops_baggage_events — Baggage handling events
    bag_event_id, leg_id, event_type, event_ts_utc,
    bag_count, status, root_cause

  ourairports_airports — Airport reference data
    ident, type, name, latitude_deg, longitude_deg, elevation_ft,
    continent, iso_country, municipality, iata_code, icao_code

  ourairports_runways — Runway specifications
    airport_ident, length_ft, width_ft, surface, lighted, closed,
    le_ident, he_ident

  ourairports_navaids — Navigation aids
    ident, name, type, frequency_khz, latitude_deg, longitude_deg,
    associated_airport

  ourairports_frequencies — ATC/comm frequencies
    airport_ident, type, description, frequency_mhz

  openflights_airlines — Airline reference
    name, iata, icao, callsign, country, active

  openflights_routes — Route network
    airline, source_airport, dest_airport, codeshare, stops, equipment

  hazards_airsigmets — SIGMET/AIRMET hazard alerts
    raw_text, valid_time_from, valid_time_to, hazard, severity,
    airsigmet_type, min_ft_msl, max_ft_msl

  hazards_gairmets — G-AIRMET graphical forecasts
    receipt_time, issue_time, expire_time, hazard, geometry_type,
    due_to, points

  notam_parsed — Parsed NOTAM records
    notam_id, icao, content, effective_from, effective_to, category

Best for:
  - Counts, rankings, aggregations ("how many incidents at JFK?")
  - Exact filtering ("flights departing IST in the next 2 hours")
  - Time-series analysis ("incident trends by year")
  - Fleet metrics (MEL items, crew duty times, turnaround delays)
  - Airport reference data (runway dimensions, frequencies, navaids)
  - Route network analysis (which airlines serve which routes)
  - Cross-table joins (flight legs + crew + MEL for a given tail)

───────────────────────────────────────────────────────────────────

## 2. KQL — Fabric Eventhouse (Kusto)
Near-real-time event streaming data via Kusto Query Language.

Tables and key columns:
  opensky_states — Live flight positions (ADS-B)
    icao24, callsign, origin_country, time_position, last_contact,
    longitude, latitude, baro_altitude, on_ground, velocity,
    true_track, vertical_rate, geo_altitude, squawk

  hazards_airsigmets — Active SIGMET alerts (live feed)
    raw_text, valid_time_from, valid_time_to, hazard, severity,
    airsigmet_type, min_ft_msl, max_ft_msl, points

  hazards_gairmets — Active G-AIRMET forecasts (live feed)
    receipt_time, issue_time, expire_time, hazard, geometry_type,
    due_to, points

Best for:
  - CURRENT weather conditions ("what's the weather at IST right now?")
  - METAR/TAF lookups and hazard windows
  - Live flight tracking ("where is TK123 right now?")
  - Active SIGMET/PIREP/AIRMET alerts in a time window
  - Any question with temporal urgency: "now", "current", "live",
    "real-time", "next N minutes", "active hazards"

When to use KQL vs SQL for hazards:
  KQL = live/current hazard windows (freshness matters)
  SQL  = historical hazard records (analysis, trends, counts)

───────────────────────────────────────────────────────────────────

## 3. FABRIC_SQL — Fabric SQL Warehouse (T-SQL)
Bureau of Transportation Statistics (BTS) airline performance data.

Tables and key columns:
  bts_ontime_reporting — Flight-level on-time performance
    Year, Month, FlightDate, IATA_Code_Marketing_Airline,
    IATA_Code_Operating_Airline, Tail_Number,
    Origin, OriginCityName, Dest, DestCityName,
    DepDelay, DepDelayMinutes, ArrDelay, ArrDelayMinutes,
    Cancelled, CancellationCode, Diverted,
    CarrierDelay, WeatherDelay, NASDelay,
    SecurityDelay, LateAircraftDelay, Distance

  airline_delay_causes — Aggregate delay statistics by carrier
    year, month, carrier, carrier_name, airport, airport_name,
    arr_flights, arr_del15, carrier_ct, weather_ct, nas_ct,
    late_aircraft_ct, arr_cancelled, arr_diverted,
    carrier_delay, weather_delay, nas_delay,
    late_aircraft_delay

Best for:
  - Delay analytics ("what are the main delay causes at JFK?")
  - On-time performance benchmarks ("compare on-time rate of TK vs LH")
  - Cancellation and diversion rates
  - Carrier performance trends over months/years
  - Weather vs carrier vs NAS delay breakdowns
  - Route-level punctuality statistics
  - Any query mentioning "BTS", "on-time", "delay cause",
    "carrier performance", "cancellation rate", "punctuality"

───────────────────────────────────────────────────────────────────

## 4. GRAPH — Graph Traversal (Fabric Kusto / PostgreSQL fallback)
Knowledge graph over aviation entity relationships.

Schema: ops_graph_edges (src_type, src_id, edge_type, dst_type, dst_id)

Node types:
  Airport, Runway, FlightLeg, Tail, Crew, NOTAM, Route, Airline,
  Navaid, Frequency, ASRSReport

Edge types:
  DEPARTS, ARRIVES, OPERATES, HAS_RUNWAY, SERVED_BY_ROUTE,
  OPERATED_BY, HAS_NAVAID, HAS_FREQUENCY, AFFECTS, AFFECTS_RUNWAY,
  CREWED_BY, MEL_ON, REPORTED_AT, CONNECTS, SAME_CITY

Best for:
  - Dependency and impact analysis ("what happens if runway 06 closes?")
  - Entity expansion (airport -> runways -> navaids -> alternates)
  - Cascade/ripple/knock-on effects of delays or closures
  - Aircraft tail dependency chains (tail -> flights -> crew -> MEL)
  - Route network connectivity (airport -> routes -> airlines)
  - Alternate airport selection based on connectivity
  - Upstream/downstream propagation of disruptions

═══════════════════════════════════════════════════════════════════
 DATA SOURCES — SEMANTIC (vector search with reranking)
═══════════════════════════════════════════════════════════════════

## 5. VECTOR_OPS — AI Search: Operational Narratives
Index: idx_ops_narratives (1536-dim embeddings, HNSW)

Fields: id, content, title, source, asrs_report_id, event_date,
  aircraft_type, flight_phase, location, narrative_type

Contains: ASRS incident reports, near-miss narratives, safety
  observations, pilot/controller experience descriptions, lessons
  learned, crew coordination issues, ATC communication events.

Best for:
  - "What happened" questions — narrative context and explanation
  - Similar incident discovery ("find reports similar to this event")
  - Safety pattern analysis ("common themes in bird strike reports")
  - Lessons learned and best practices
  - Contextual understanding when a query asks "why" or "describe"
  - Supplementing structured metrics with qualitative insight

───────────────────────────────────────────────────────────────────

## 6. VECTOR_REG — AI Search: Regulatory Documents
Index: idx_regulatory (1536-dim embeddings, HNSW)

Fields: id, content, title, source, document_number, effective_date,
  issuing_authority, aircraft_type, document_type

Contains: NOTAMs, Airworthiness Directives (ADs), EASA Safety
  Information Bulletins, FAA service bulletins, standard operating
  procedures (SOPs), regulatory compliance documents.

Best for:
  - Regulatory and compliance queries ("what ADs apply to B737?")
  - NOTAM content search (semantic, when exact ICAO lookup is not enough)
  - SOP clause lookup ("what does the SOP say about icing conditions?")
  - Airworthiness directive applicability by aircraft type
  - EASA/FAA bulletin retrieval
  - Compliance gap identification

───────────────────────────────────────────────────────────────────

## 7. VECTOR_AIRPORT — AI Search: Airport Operational Documents
Index: idx_airport_ops_docs (1536-dim embeddings, HNSW)

Fields: id, content, title, source, airport_icao, airport_iata,
  airport_name, facility_type, effective_date

Contains: Runway specification documents, station manuals, ground
  handling procedures, terminal facility descriptions, taxiway
  diagrams, gate/stand allocation rules, turnaround SOPs.

Best for:
  - Airport facility reference ("what are IST's runway specs?")
  - Ground handling procedure lookup
  - Gate/stand/apron documentation
  - Turnaround procedure references
  - Terminal operational constraints
  - Airport-specific operational docs not captured in structured data

═══════════════════════════════════════════════════════════════════
 DATA SOURCES — DOCUMENT STORE
═══════════════════════════════════════════════════════════════════

## 8. NOSQL — Cosmos DB: Operational Documents
Container: notams (partitioned by /icao)

Document structure:
  id, notam_number, icao, iata, airport_name, type, category,
  severity (HIGH/MEDIUM/LOW), content, status (active/expired),
  effective_from, effective_to, source (FAA/DGCA/CAA)

Categories: runway, taxiway, navaid, obstacle, procedure, airspace,
  apron, fuel, wildlife, security, aerodrome

Best for:
  - Live NOTAM lookup by airport ICAO code ("active NOTAMs for LTFM")
  - NOTAM filtering by severity, category, date range
  - Ground handling and parking stand documents
  - Operational document retrieval by airport
  - When you need the authoritative, structured NOTAM record
    (vs VECTOR_REG which does semantic search over NOTAM content)

When to use NOSQL vs VECTOR_REG for NOTAMs:
  NOSQL     = exact ICAO lookup, active status filter, structured fields
  VECTOR_REG = semantic/similarity search across NOTAM content text

═══════════════════════════════════════════════════════════════════
 ROUTE CLASSIFICATION
═══════════════════════════════════════════════════════════════════

Classify the query into one of these routes:

SQL — Query needs precise, structured results:
  "how many", "count", "top N", "most common", "average", "total",
  "list", "show", "compare" with specific metrics, date ranges,
  group-by conditions

SEMANTIC — Query needs narrative understanding or document retrieval:
  "describe", "summarize", "what happened", "examples", "similar to",
  "lessons learned", regulatory lookups, compliance questions

HYBRID — Query benefits from both structured data and semantic context:
  mixing metrics with explanation, operational questions needing both
  data and narrative context. When unsure, default to HYBRID.

═══════════════════════════════════════════════════════════════════
 SOURCE SELECTION RULES
═══════════════════════════════════════════════════════════════════

Select 1–4 sources. Follow these principles:

RULE 1 — Pair structured + semantic for richer answers
  When a query mixes metrics with context ("top incidents and why they
  happened"), always include at least one structured source (SQL, KQL,
  FABRIC_SQL) AND one semantic source (VECTOR_OPS, VECTOR_REG, or
  VECTOR_AIRPORT).

RULE 2 — Use KQL for freshness-critical data
  Include KQL when the query involves: "now", "current", "live",
  "real-time", "next N minutes/hours", "weather", "METAR", "TAF",
  "SIGMET", "active hazards", "flight position", "tracking".
  Do NOT use KQL for historical analysis — use SQL instead.

RULE 3 — Use GRAPH for relationship and impact queries
  Include GRAPH when the query involves: "impact", "dependency",
  "downstream", "upstream", "cascade", "propagate", "chain", "ripple",
  "knock-on", "what happens if", "connected", "alternate", "network".
  GRAPH is especially valuable for "if-then" disruption scenarios.

RULE 4 — Use FABRIC_SQL for airline performance and delay analytics
  Include FABRIC_SQL when the query involves: "delay", "on-time",
  "performance", "BTS", "carrier delay", "cancellation rate",
  "punctuality", "benchmark", "schedule performance", "historical trend",
  "weather delay vs carrier delay".

RULE 5 — Use VECTOR_REG + NOSQL for regulatory and compliance
  Include VECTOR_REG for semantic search over regulatory documents.
  Include NOSQL for exact NOTAM lookups by ICAO code.
  Use both together for comprehensive regulatory coverage.

RULE 6 — Use VECTOR_OPS for narrative and incident analysis
  Include VECTOR_OPS for: "incident", "report", "safety", "narrative",
  "near-miss", "similar", "lessons", "what happened", "observation".

RULE 7 — Use VECTOR_AIRPORT for airport facility reference
  Include VECTOR_AIRPORT for: "runway", "gate", "stand", "apron",
  "taxiway", "terminal", "ground handling", "turnaround", "facility".

RULE 8 — Omit sources that add no value
  A delay statistics query does not need VECTOR_AIRPORT.
  A runway specs query does not need FABRIC_SQL.
  Only include sources that contribute evidence to the answer.

═══════════════════════════════════════════════════════════════════
 INTENT GRAPH (when provided)
═══════════════════════════════════════════════════════════════════

When an intent_graph is supplied in the context, use it as PRIMARY
guidance for source selection:

1. Map the query to the closest intent
   (e.g., PilotBrief.Departure, Analytics.Compare, Disruption.Explain)
2. Look up required and optional evidence types for that intent
3. For each evidence type, select tools from the authoritative_in
   mappings, preferring priority-1 tools
4. Include optional evidence sources when they clearly add value
5. The source selection rules above serve as supplementary guidance

═══════════════════════════════════════════════════════════════════
 OUTPUT FORMAT
═══════════════════════════════════════════════════════════════════

Return JSON only — no prose, no markdown:
{
    "route": "SQL" | "SEMANTIC" | "HYBRID",
    "reasoning": "Brief explanation of route choice and why these sources were selected",
    "sql_hint": "Optional SQL generation hint if route involves SQL queries",
    "sources": ["SQL", "KQL", "VECTOR_OPS"]
}

Valid source names: SQL, KQL, GRAPH, VECTOR_OPS, VECTOR_REG, VECTOR_AIRPORT, NOSQL, FABRIC_SQL
"""

from retrieval_plan import VALID_SOURCES


class QueryRouter:
    """Routes queries to appropriate retrieval paths."""

    def __init__(self):
        self.client, _ = get_shared_client(api_version=OPENAI_API_VERSION)
        self.model = os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME", "aviation-chat-gpt5-mini")

    def route(self, query: str, intent_graph: dict | None = None) -> dict:
        """Classify a query into a retrieval route using LLM.

        When *intent_graph* is provided it is appended as additional user
        context so the LLM can use authoritative_in / requires mappings
        for source selection.
        """
        try:
            messages = [
                {"role": "system", "content": ROUTING_PROMPT},
            ]
            if intent_graph:
                messages.append({
                    "role": "user",
                    "content": (
                        "Intent graph context (use as PRIMARY guidance for source selection):\n"
                        + json.dumps(intent_graph, default=str)
                    ),
                })
            messages.append({"role": "user", "content": query})

            response = self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                response_format={"type": "json_object"}
            )

            result = json.loads(response.choices[0].message.content)

            if "route" not in result:
                result["route"] = "HYBRID"
            if "reasoning" not in result:
                result["reasoning"] = "Default routing"

            # Parse and validate sources list.
            raw_sources = result.get("sources", [])
            result["sources"] = (
                [s.upper() for s in raw_sources if isinstance(s, str) and s.upper() in VALID_SOURCES]
                if isinstance(raw_sources, list)
                else []
            )

            return result
        except Exception as exc:
            logger.warning("LLM routing failed, falling back to HYBRID: %s", exc)
            return {
                "route": "HYBRID",
                "reasoning": "Fallback to HYBRID due to routing error",
            }

    def quick_route(self, query: str) -> str:
        """Quick route classification using keyword heuristics."""
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

    def smart_route(self, query: str, intent_graph: dict | None = None) -> dict:
        """LLM-driven routing with keyword heuristic fallback.

        Always calls the LLM for optimal multi-source selection.  Falls back
        to ``quick_route()`` keyword heuristics only when the LLM call fails.

        Returns the same dict shape as ``route()`` (route, reasoning, sql_hint, sources).
        """
        result = self.route(query, intent_graph=intent_graph)
        # If the LLM route() succeeded with a valid route, return it directly.
        if result.get("route") in ("SQL", "SEMANTIC", "HYBRID") and result.get("reasoning", "") != "Fallback to HYBRID due to routing error":
            return result
        # LLM call failed — fall back to keyword heuristics.
        heuristic = self.quick_route(query)
        return {
            "route": heuristic,
            "reasoning": f"Heuristic fallback (LLM routing failed): {heuristic}",
            "sql_hint": None,
            "sources": [],
        }


def route_query(query: str, use_llm: bool = True, intent_graph: dict | None = None) -> dict:
    """Route a query to the appropriate retrieval path."""
    router = QueryRouter()
    if use_llm:
        return router.route(query, intent_graph=intent_graph)
    return {"route": router.quick_route(query), "reasoning": "Heuristic routing"}


if __name__ == "__main__":
    router = QueryRouter()

    test_queries = [
        ("Top 5 locations with most ASRS reports", "SQL"),
        ("Summarize common runway incursion narratives", "SEMANTIC"),
        ("Show top aircraft types and explain typical issues", "HYBRID"),
    ]

    print("=" * 70)
    print("QUERY ROUTER TEST")
    print("=" * 70)

    for query, expected in test_queries:
        print(f"\nQuery: {query}")
        print(f"   Expected: {expected}")

        quick = router.quick_route(query)
        print(f"   Heuristic: {quick}")
