#!/usr/bin/env python3
"""
Query Router - Classifies queries into retrieval paths.
Routes: SQL, SEMANTIC, HYBRID
"""

import os
import json
import logging
from dotenv import load_dotenv

from azure_openai_client import get_shared_client

logger = logging.getLogger(__name__)

load_dotenv()

OPENAI_API_VERSION = os.getenv("AZURE_OPENAI_API_VERSION", "2024-06-01")


ROUTING_PROMPT = """You are a query router for an aviation safety Q&A system with multiple data sources.

## Available Data Sources

1. **SQL Database (PostgreSQL)**
   - Table asrs_reports(asrs_report_id, event_date, location, aircraft_type, flight_phase, narrative_type, title, report_text, raw_json, ingested_at)
   - Table asrs_ingestion_runs(run_id, started_at, completed_at, status, source_manifest_path, records_seen, records_loaded, records_failed)
   - Additional tables in demo schema: ourairports_airports, ourairports_runways, ourairports_navaids, ourairports_frequencies, openflights_routes, openflights_airports, openflights_airlines, ops_flight_legs, ops_turnaround_milestones, ops_crew_rosters, ops_mel_techlog_events
   - Best for: counts, rankings, exact filters, timelines, grouped metrics, runway data, route networks, operational stats

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

## Output Format

Return JSON only:
{
    "route": "SQL|SEMANTIC|HYBRID",
    "reasoning": "Brief explanation of why this route",
    "sql_hint": "Optional hint for SQL generation if route is SQL/HYBRID"
}
"""


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
