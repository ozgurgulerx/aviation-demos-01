#!/usr/bin/env python3
"""
SQL Generator - Generates SQL from natural language queries.
"""

import os
import re
from dotenv import load_dotenv
from openai import AzureOpenAI
from azure.identity import DefaultAzureCredential, get_bearer_token_provider

load_dotenv()


def _client_tuning_kwargs() -> dict:
    try:
        timeout_seconds = float(os.getenv("AZURE_OPENAI_TIMEOUT_SECONDS", "45"))
    except Exception:
        timeout_seconds = 45.0
    try:
        max_retries = max(0, int(os.getenv("AZURE_OPENAI_MAX_RETRIES", "1")))
    except Exception:
        max_retries = 1
    return {"timeout": timeout_seconds, "max_retries": max_retries}


FULL_SCHEMA = """
## Aviation ASRS Database Schema

### asrs_reports
Columns:
- asrs_report_id (TEXT, PRIMARY KEY)
- event_date (DATE)
- location (TEXT)
- aircraft_type (TEXT)
- flight_phase (TEXT)
- narrative_type (TEXT)
- title (TEXT)
- report_text (TEXT)
- raw_json (TEXT)
- ingested_at (TIMESTAMP)

### asrs_ingestion_runs
Columns:
- run_id (TEXT, PRIMARY KEY)
- started_at (TIMESTAMP)
- completed_at (TIMESTAMP)
- status (TEXT)
- source_manifest_path (TEXT)
- records_seen (INTEGER)
- records_loaded (INTEGER)
- records_failed (INTEGER)
"""

SYSTEM_PROMPT = f"""You are an expert SQL generator for an aviation safety database.
Generate SQL queries based on natural language questions.

{FULL_SCHEMA}

## Rules:
1. Return ONLY the SQL query, no explanations
2. Use only listed tables and columns
3. Always include identifying columns in SELECT (asrs_report_id, title, event_date when relevant)
4. Limit results to 20 unless user specifies otherwise
5. For aggregations, include clear aliases
6. Prefer case-insensitive matching using LOWER(column) LIKE LOWER('%value%')
7. Never generate INSERT/UPDATE/DELETE/DDL statements
"""


class SQLGenerator:
    """Generate SQL queries from natural language using LLM."""

    def __init__(self):
        endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
        api_key = os.getenv("AZURE_OPENAI_API_KEY")

        if api_key:
            self.client = AzureOpenAI(
                azure_endpoint=endpoint,
                api_key=api_key,
                api_version="2024-06-01",
                **_client_tuning_kwargs(),
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
                **_client_tuning_kwargs(),
            )
        self.model = os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME", "gpt-5-nano")

    def generate(self, query: str) -> str:
        """Generate SQL from natural language query."""
        response = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": query}
            ]
        )

        sql = response.choices[0].message.content.strip()

        sql = re.sub(r'```sql\n?', '', sql)
        sql = re.sub(r'```\n?', '', sql)
        sql = sql.strip()

        return sql

    def generate_with_context(self, query: str, context: str = None) -> str:
        """Generate SQL with additional context."""
        if context:
            enhanced_query = f"{query}\n\nAdditional context: {context}"
        else:
            enhanced_query = query

        return self.generate(enhanced_query)


def generate_sql(query: str) -> str:
    """Generate SQL from natural language query."""
    generator = SQLGenerator()
    return generator.generate(query)


if __name__ == "__main__":
    generator = SQLGenerator()

    test_queries = [
        "Top 10 flight phases with most ASRS reports",
        "How many ASRS reports mention runway incursions by year?",
        "Most common aircraft types in reports from 2024",
    ]

    print("=" * 70)
    print("SQL GENERATOR TEST")
    print("=" * 70)

    for query in test_queries:
        print(f"\nQuery: {query}")
        print("-" * 50)
        sql = generator.generate(query)
        print(f"SQL:\n{sql}")
        print()
