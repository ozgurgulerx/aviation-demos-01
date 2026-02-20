#!/usr/bin/env python3
"""
Query Router - Classifies queries into retrieval paths.
Routes: SQL, SEMANTIC, HYBRID
"""

import os
import json
from dotenv import load_dotenv
from openai import AzureOpenAI
from azure.identity import DefaultAzureCredential, get_bearer_token_provider

load_dotenv()

ROUTING_PROMPT = """You are a query router for an aviation safety Q&A system with multiple data sources.

## Available Data Sources

1. **SQL Database**
   - Table asrs_reports(asrs_report_id, event_date, location, aircraft_type, flight_phase, narrative_type, title, report_text, raw_json, ingested_at)
   - Table asrs_ingestion_runs(run_id, started_at, completed_at, status, source_manifest_path, records_seen, records_loaded, records_failed)
   - Best for: counts, rankings, exact filters, timelines, grouped metrics

2. **Semantic Index** (aviation-index)
   - Chunked ASRS narrative documents with metadata
   - Best for: contextual explanations, similarity, narrative retrieval

## Route Definitions

**SQL** - Use when query needs precise, structured results:
- "how many", "count", "top", "most", "average"
- explicit group/filter conditions
- trend analysis by date/phase/location/aircraft type

**SEMANTIC** - Use when query needs narrative understanding:
- "describe", "summarize", "what happened", "examples"
- contextual or similarity-based retrieval

**HYBRID** - Use when both are useful:
- requests mixing metrics with explanation
- quantitative answer plus narrative context

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
        endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
        api_key = os.getenv("AZURE_OPENAI_API_KEY")

        if api_key:
            self.client = AzureOpenAI(
                azure_endpoint=endpoint,
                api_key=api_key,
                api_version="2024-06-01",
            )
        else:
            credential = DefaultAzureCredential()
            token_provider = get_bearer_token_provider(
                credential, "https://cognitiveservices.azure.com/.default"
            )
            self.client = AzureOpenAI(
                azure_endpoint=endpoint,
                azure_ad_token_provider=token_provider,
                api_version="2024-06-01",
            )
        self.model = os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME", "gpt-5-nano")

    def route(self, query: str) -> dict:
        """Classify a query into a retrieval route using LLM."""
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
