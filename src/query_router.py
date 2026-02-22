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


ROUTING_PROMPT = """You are a query router for an aviation safety Q&A system with multiple data sources.

## Available Data Sources

1. **SQL Database (PostgreSQL)**
   - Table asrs_reports(asrs_report_id, event_date, location, aircraft_type, flight_phase, narrative_type, title, report_text, raw_json, ingested_at)
   - Table asrs_ingestion_runs(run_id, started_at, completed_at, status, source_manifest_path, records_seen, records_loaded, records_failed)
   - Additional tables in demo schema:
     - ourairports_airports(id, ident, type, name, latitude_deg, longitude_deg, elevation_ft, continent, iso_country, iso_region, municipality, scheduled_service, gps_code, iata_code, local_code, home_link, wikipedia_link, keywords)
     - ourairports_runways(id, airport_ref, airport_ident, length_ft, width_ft, surface, lighted, closed, le_ident, le_latitude_deg, le_longitude_deg, he_ident, he_latitude_deg, he_longitude_deg)
     - ourairports_navaids(id, ident, name, type, frequency_khz, latitude_deg, longitude_deg, iso_country, associated_airport)
     - ourairports_frequencies(id, airport_ref, airport_ident, type, description, frequency_mhz)
     - openflights_airports(airport_id, name, city, country, iata, icao, latitude, longitude, altitude, timezone, dst, tzdb, type, source)
     - openflights_airlines(airline_id, name, alias, iata, icao, callsign, country, active)
     - openflights_routes(airline, airline_id, source_airport, source_airport_id, dest_airport, dest_airport_id, codeshare, stops, equipment)
     - hazards_airsigmets(raw_text, valid_time_from, valid_time_to, points, min_ft_msl, max_ft_msl, movement_dir_degrees, movement_speed_kt, hazard, severity, airsigmet_type)
     - hazards_gairmets(receipt_time, issue_time, expire_time, product, tag, issue_to_valid_hours, valid_time, hazard, geometry_type, due_to, points)
     - ops_flight_legs(flight_id, airline, flight_number, dep_icao, arr_icao, scheduled_dep, scheduled_arr, actual_dep, actual_arr, aircraft_type, registration, status)
     - ops_turnaround_milestones(flight_id, milestone, scheduled_time, actual_time, station)
     - ops_crew_rosters(crew_id, name, role, flight_id, duty_start, duty_end, base)
     - ops_mel_techlog_events(event_id, registration, ata_chapter, description, opened_date, closed_date, mel_category, status)
     - ops_graph_edges(src_type, src_id, edge_type, dst_type, dst_id)
   - Best for: counts, rankings, exact filters, timelines, grouped metrics, runway data, route networks, operational stats
   - IMPORTANT: Use exact column names listed above. Do NOT guess column names.

2. **Semantic Indexes** (vector search with reranking)
   - idx_ops_narratives: ASRS narrative documents — incident reports, near-miss narratives, safety observations
   - idx_regulatory: Regulatory documents — NOTAMs, Airworthiness Directives, EASA/FAA bulletins
   - idx_airport_ops_docs: Airport operational documents — runway specs, station info, facility data
   - Best for: contextual explanations, similarity, narrative retrieval, regulatory lookups

3. **KQL Eventhouse** (when configured)
   - Near-real-time weather observations (METAR/TAF), OpenSky flight states, hazard alerts
   - Best for: current weather, live flight tracking, recent hazards

4. **Graph Store** (when configured)
   - Dependency paths between airports, runways, alternates, routes
   - Best for: impact analysis, dependency chains, alternate airport selection

## Route Definitions

**SQL** - Use when query needs precise, structured results:
- "how many", "count", "top", "most", "average"
- explicit group/filter conditions
- trend analysis by date/phase/location/aircraft type
- runway dimensions, airport details, route networks

**SEMANTIC** - Use when query needs narrative understanding:
- "describe", "summarize", "what happened", "examples"
- contextual or similarity-based retrieval
- regulatory lookups, compliance questions

**HYBRID** - Use when both are useful:
- requests mixing metrics with explanation
- quantitative answer plus narrative context
- operational questions needing both data and context

## Source Selection

In addition to route, select the most appropriate data sources from:
- SQL: PostgreSQL warehouse — for counts, rankings, exact filters, timelines, airport data, route networks
- KQL: Fabric Eventhouse — for current weather (METAR/TAF), live flight tracking, recent hazards
- GRAPH: Fabric Graph — for dependency paths, impact analysis, alternate airport selection
- VECTOR_OPS: AI Search narratives — for incident reports, near-miss narratives, safety observations
- VECTOR_REG: AI Search regulatory — for NOTAMs, Airworthiness Directives, EASA/FAA bulletins
- VECTOR_AIRPORT: AI Search airport ops — for runway specs, station info, facility data
- NOSQL: Fabric NoSQL — for operational documents, NOTAMs, ground handling docs, parking stands

Select 1-4 sources that best answer the query. Omit sources that add no value.

## Output Format

Return JSON only:
{
    "route": "SQL|SEMANTIC|HYBRID",
    "reasoning": "Brief explanation of why this route",
    "sql_hint": "Optional hint for SQL generation if route is SQL/HYBRID",
    "sources": ["SQL", "VECTOR_OPS"]
}
"""

from retrieval_plan import VALID_SOURCES


class QueryRouter:
    """Routes queries to appropriate retrieval paths."""

    def __init__(self):
        self.client, _ = get_shared_client(api_version=OPENAI_API_VERSION)
        self.model = os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME", "aviation-chat-gpt5-mini")

    def route(self, query: str) -> dict:
        """Classify a query into a retrieval route using LLM."""
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": ROUTING_PROMPT},
                    {"role": "user", "content": query}
                ],
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

    def smart_route(self, query: str) -> dict:
        """Heuristic-first routing: skip LLM when keywords give a confident answer.

        Returns the same dict shape as ``route()`` (route, reasoning, sql_hint, sources).
        When ``USE_SMART_ROUTING`` is false this delegates entirely to the LLM.
        """
        if not os.getenv("USE_SMART_ROUTING", "true").strip().lower() in {"1", "true", "yes", "y", "on"}:
            return self.route(query)

        heuristic = self.quick_route(query)
        if heuristic in ("SQL", "SEMANTIC"):
            return {
                "route": heuristic,
                "reasoning": f"Heuristic routing (confident: {heuristic})",
                "sql_hint": None,
                "sources": [],
            }
        # HYBRID is ambiguous — escalate to LLM for better source selection.
        return self.route(query)


def route_query(query: str, use_llm: bool = True) -> dict:
    """Route a query to the appropriate retrieval path."""
    router = QueryRouter()
    if use_llm:
        return router.route(query)
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
