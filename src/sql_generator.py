#!/usr/bin/env python3
"""
SQL Generator - Generates SQL from natural language queries.
"""

import os
import re

from azure_openai_client import get_shared_client
from shared_utils import OPENAI_API_VERSION


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

## Demo Schema — Multi-Source Aviation Data

### ourairports_airports
Columns (all TEXT):
- id, ident, type, name, latitude_deg, longitude_deg, elevation_ft
- continent, iso_country, iso_region, municipality
- scheduled_service, gps_code, iata_code, local_code
- home_link, wikipedia_link, keywords

### ourairports_runways
Columns (all TEXT):
- id, airport_ref, airport_ident, length_ft, width_ft, surface
- lighted, closed
- le_ident, le_latitude_deg, le_longitude_deg, le_elevation_ft
- le_heading_degt, le_displaced_threshold_ft
- he_ident, he_latitude_deg, he_longitude_deg, he_elevation_ft
- he_heading_degt, he_displaced_threshold_ft

### ourairports_navaids
Columns (all TEXT):
- id, filename, ident, name, type, frequency_khz
- latitude_deg, longitude_deg, elevation_ft
- iso_country, dme_frequency_khz, dme_channel
- dme_latitude_deg, dme_longitude_deg, dme_elevation_ft
- slaved_variation_deg, magnetic_variation_deg
- usagetype, power, associated_airport

### ourairports_frequencies
Columns (all TEXT):
- id, airport_ref, airport_ident, type, description, frequency_mhz

### openflights_airports
Columns (all TEXT):
- airport_id, name, city, country, iata, icao
- latitude, longitude, altitude, timezone, dst, tzdb, type, source

### openflights_airlines
Columns (all TEXT):
- airline_id, name, alias, iata, icao, callsign, country, active

### openflights_routes
Columns (all TEXT):
- airline, airline_id, source_airport, source_airport_id
- dest_airport, dest_airport_id, codeshare, stops, equipment

### hazards_airsigmets
Columns (all TEXT):
- raw_text, valid_time_from, valid_time_to, points
- min_ft_msl, max_ft_msl, movement_dir_degrees, movement_speed_kt
- hazard, severity, airsigmet_type

### hazards_gairmets
Columns (all TEXT):
- receipt_time, issue_time, expire_time, product, tag
- issue_to_valid_hours, valid_time, hazard, geometry_type, due_to, points

### ops_flight_legs
Columns (all TEXT):
- flight_id, airline, flight_number, dep_icao, arr_icao
- scheduled_dep, scheduled_arr, actual_dep, actual_arr
- aircraft_type, registration, status

### ops_turnaround_milestones
Columns (all TEXT):
- flight_id, milestone, scheduled_time, actual_time, station

### ops_crew_rosters
Columns (all TEXT):
- crew_id, name, role, flight_id, duty_start, duty_end, base

### ops_mel_techlog_events
Columns (all TEXT):
- event_id, registration, ata_chapter, description
- opened_date, closed_date, mel_category, status

### ops_graph_edges
Columns (all TEXT):
- src_type, src_id, edge_type, dst_type, dst_id
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
        self.client, _ = get_shared_client(api_version=OPENAI_API_VERSION)
        self.model = os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME", "aviation-chat-gpt5-mini")

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
