#!/usr/bin/env python3
"""
Build enriched knowledge graph edges from existing PostgreSQL tables.

Generates ~500K+ edges across 16 edge types connecting airports, runways,
routes, airlines, navaids, NOTAMs, ASRS reports, flight legs, crew, etc.

Also creates the `demo.notam_parsed` table from PilotWeb JSONL files,
exports enriched edges to CSV for KQL sync, and creates PG indexes.

Usage:
    python scripts/14_build_graph_edges.py
    python scripts/14_build_graph_edges.py --pg-host $PGHOST --export-csv data/enriched_graph_edges.csv
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import psycopg2
from psycopg2.extras import execute_values

ROOT = Path(__file__).resolve().parents[1]


def qident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def iter_jsonl(path: Path) -> Iterable[dict]:
    with path.open("r", encoding="utf-8", errors="ignore") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue


# ---------------------------------------------------------------------------
# NOTAM parsing helpers
# ---------------------------------------------------------------------------

_RWY_RE = re.compile(r"RWY\s+(\d{2}[LRC]?(?:/\d{2}[LRC]?)?)", re.IGNORECASE)

# FAA facility designator -> ICAO mapping for common US airports
_FAA_TO_ICAO: Dict[str, str] = {
    "JFK": "KJFK", "LAX": "KLAX", "ORD": "KORD", "ATL": "KATL",
    "DFW": "KDFW", "DEN": "KDEN", "SFO": "KSFO", "SEA": "KSEA",
    "LAS": "KLAS", "MCO": "KMCO", "EWR": "KEWR", "CLT": "KCLT",
    "PHX": "KPHX", "IAH": "KIAH", "MIA": "KMIA", "BOS": "KBOS",
    "MSP": "KMSP", "FLL": "KFLL", "DTW": "KDTW", "PHL": "KPHL",
    "LGA": "KLGA", "BWI": "KBWI", "SLC": "KSLC", "SAN": "KSAN",
    "IAD": "KIAD", "DCA": "KDCA", "TPA": "KTPA", "AUS": "KAUS",
    "HNL": "PHNL", "PDX": "KPDX", "STL": "KSTL", "MCI": "KMCI",
    "RDU": "KRDU", "BNA": "KBNA", "SNA": "KSNA", "SAT": "KSAT",
    "DAL": "KDAL", "HOU": "KHOU", "MDW": "KMDW", "OAK": "KOAK",
    "SMF": "KSMF", "SJC": "KSJC", "CLE": "KCLE", "CVG": "KCVG",
    "CMH": "KCMH", "PIT": "KPIT", "IND": "KIND", "MKE": "KMKE",
    "MSY": "KMSY", "JAX": "KJAX", "RSW": "KRSW", "ABQ": "KABQ",
    "OMA": "KOMA", "BUF": "KBUF", "RNO": "KRNO", "ONT": "KONT",
    "BUR": "KBUR", "PBI": "KPBI", "TUS": "KTUS", "ELP": "KELP",
}


def _faa_to_icao(fac: str) -> str:
    """Convert FAA facility designator to ICAO code."""
    fac = fac.strip().upper()
    if fac in _FAA_TO_ICAO:
        return _FAA_TO_ICAO[fac]
    # If already 4 chars and starts with K/P/T, assume ICAO
    if len(fac) == 4 and fac[0] in ("K", "P", "T", "L", "E"):
        return fac
    # Default: prepend K for US airports
    if len(fac) == 3 and fac.isalpha():
        return f"K{fac}"
    return fac


def _classify_notam(content: str) -> str:
    """Classify NOTAM into a category from content text."""
    c = content.upper()
    if any(w in c for w in ("RWY", "RUNWAY")):
        return "runway"
    if any(w in c for w in ("TWY", "TAXIWAY")):
        return "taxiway"
    if any(w in c for w in ("ILS", "VOR", "DME", "NDB", "VASI", "PAPI", "LOC", "RNAV", "GPS", "NAVAID")):
        return "navaid"
    if any(w in c for w in ("CRANE", "TOWER", "OBSTACLE", "OBST")):
        return "obstacle"
    if any(w in c for w in ("SID", "STAR", "APPROACH", "PROCEDURE", "MDA", "DA ", "CIRCLING")):
        return "procedure"
    if any(w in c for w in ("AIRSPACE", "TFR", "RESTRICTED", "PROHIBITED", "MOA")):
        return "airspace"
    if any(w in c for w in ("APRON", "RAMP", "STAND", "GATE", "TERMINAL")):
        return "apron"
    if any(w in c for w in ("FUEL",)):
        return "fuel"
    if any(w in c for w in ("BIRD", "WILDLIFE")):
        return "wildlife"
    if any(w in c for w in ("SECURITY", "SCREENING")):
        return "security"
    if any(w in c for w in ("AD ", "AERODROME", "HOURS OF OPS", "CLSD TO ALL")):
        return "aerodrome"
    return "general"


def _severity_from_content(content: str) -> str:
    """Infer severity from NOTAM content."""
    c = content.upper()
    if any(w in c for w in ("CLSD", "CLOSED", "UNSERVICEABLE", "UNUSABLE", "OUT OF SERVICE")):
        return "HIGH"
    if any(w in c for w in ("RESTRICTED", "REDUCED", "CHANGED", "LIMITED", "WIP")):
        return "MEDIUM"
    return "LOW"


def _extract_runway(content: str) -> Optional[str]:
    """Extract runway designator from NOTAM content."""
    m = _RWY_RE.search(content)
    return m.group(1) if m else None


# ---------------------------------------------------------------------------
# Phase 2a: Load NOTAM data into demo.notam_parsed
# ---------------------------------------------------------------------------

def load_notam_parsed(cur, schema: str) -> int:
    """Parse real NOTAMs from PilotWeb JSONL files into demo.notam_parsed."""
    cur.execute(f"DROP TABLE IF EXISTS {qident(schema)}.notam_parsed;")
    cur.execute(f"""
        CREATE TABLE {qident(schema)}.notam_parsed (
            notam_number TEXT PRIMARY KEY,
            icao TEXT NOT NULL,
            iata TEXT,
            airport_name TEXT,
            category TEXT,
            affected_runway TEXT,
            content TEXT NOT NULL,
            effective_from TEXT,
            effective_to TEXT,
            severity TEXT,
            status TEXT DEFAULT 'active',
            raw_json JSONB,
            ingested_at TIMESTAMPTZ DEFAULT NOW()
        );
    """)

    # Glob all NOTAM JSONL files
    notam_dir = ROOT / "data" / "h-notam_recent"
    jsonl_files = sorted(notam_dir.glob("*/search_location_*.jsonl"))
    if not jsonl_files:
        print("  No NOTAM JSONL files found")
        return 0

    seen_numbers: set = set()
    rows: List[Tuple] = []
    for jf in jsonl_files:
        for obj in iter_jsonl(jf):
            notam_no = str(obj.get("notamNumber", "")).strip()
            if not notam_no or notam_no in seen_numbers:
                continue
            seen_numbers.add(notam_no)

            fac = str(obj.get("facilityDesignator", "")).strip()
            icao = _faa_to_icao(fac)
            content = str(obj.get("icaoMessage", ""))
            airport_name = str(obj.get("airportName", ""))

            rows.append((
                notam_no,
                icao,
                fac if len(fac) == 3 else "",
                airport_name,
                _classify_notam(content),
                _extract_runway(content),
                content,
                str(obj.get("startDate", "")),
                str(obj.get("endDate", "")),
                _severity_from_content(content),
                "active",
                json.dumps(obj),
            ))

    if rows:
        execute_values(
            cur,
            f"""INSERT INTO {qident(schema)}.notam_parsed
                (notam_number, icao, iata, airport_name, category,
                 affected_runway, content, effective_from, effective_to,
                 severity, status, raw_json)
                VALUES %s
                ON CONFLICT (notam_number) DO NOTHING""",
            rows,
            page_size=500,
        )
    cur.execute(f"SELECT COUNT(*) FROM {qident(schema)}.notam_parsed;")
    count = int(cur.fetchone()[0])
    print(f"  notam_parsed: {count} NOTAMs loaded from {len(jsonl_files)} files")
    return count


# ---------------------------------------------------------------------------
# Edge generation functions
# ---------------------------------------------------------------------------

EdgeRow = Tuple[str, str, str, str, str]  # (src_type, src_id, edge_type, dst_type, dst_id)


def _gen_has_runway(cur, schema: str) -> List[EdgeRow]:
    """Airport -> Runway edges from ourairports_runways."""
    cur.execute(f"""
        SELECT DISTINCT
            UPPER(COALESCE(a.ident, r.airport_ident)) AS airport_icao,
            UPPER(r.id) AS runway_id
        FROM {qident(schema)}.ourairports_runways r
        LEFT JOIN {qident(schema)}.ourairports_airports a
            ON LOWER(r.airport_ident) = LOWER(a.ident)
        WHERE r.airport_ident IS NOT NULL AND r.airport_ident != ''
    """)
    return [("Airport", row[0], "HAS_RUNWAY", "Runway", row[1]) for row in cur.fetchall()]


def _gen_served_by_route(cur, schema: str) -> List[EdgeRow]:
    """Airport -> Route edges (both source and dest airports)."""
    cur.execute(f"""
        SELECT DISTINCT
            UPPER(source_airport) AS src_apt,
            UPPER(dest_airport) AS dst_apt,
            UPPER(source_airport || '-' || dest_airport || '-' || airline) AS route_id
        FROM {qident(schema)}.openflights_routes
        WHERE source_airport IS NOT NULL AND source_airport != ''
          AND dest_airport IS NOT NULL AND dest_airport != ''
    """)
    edges: List[EdgeRow] = []
    for row in cur.fetchall():
        src_apt, dst_apt, route_id = row
        edges.append(("Airport", src_apt, "SERVED_BY_ROUTE", "Route", route_id))
        edges.append(("Airport", dst_apt, "SERVED_BY_ROUTE", "Route", route_id))
    return edges


def _gen_operated_by(cur, schema: str) -> List[EdgeRow]:
    """Route -> Airline edges from openflights_routes."""
    cur.execute(f"""
        SELECT DISTINCT
            UPPER(source_airport || '-' || dest_airport || '-' || airline) AS route_id,
            UPPER(airline) AS airline_code
        FROM {qident(schema)}.openflights_routes
        WHERE airline IS NOT NULL AND airline != '' AND airline != '\\N'
          AND source_airport IS NOT NULL AND source_airport != ''
          AND dest_airport IS NOT NULL AND dest_airport != ''
    """)
    return [("Route", row[0], "OPERATED_BY", "Airline", row[1]) for row in cur.fetchall()]


def _gen_has_navaid(cur, schema: str) -> List[EdgeRow]:
    """Airport -> Navaid edges from ourairports_navaids."""
    cur.execute(f"""
        SELECT DISTINCT
            UPPER(associated_airport) AS airport_icao,
            UPPER(id) AS navaid_id
        FROM {qident(schema)}.ourairports_navaids
        WHERE associated_airport IS NOT NULL AND associated_airport != ''
    """)
    return [("Airport", row[0], "HAS_NAVAID", "Navaid", row[1]) for row in cur.fetchall()]


def _gen_has_frequency(cur, schema: str) -> List[EdgeRow]:
    """Airport -> Frequency edges from ourairports_frequencies."""
    cur.execute(f"""
        SELECT DISTINCT
            UPPER(airport_ident) AS airport_icao,
            UPPER(id) AS freq_id
        FROM {qident(schema)}.ourairports_frequencies
        WHERE airport_ident IS NOT NULL AND airport_ident != ''
    """)
    return [("Airport", row[0], "HAS_FREQUENCY", "Frequency", row[1]) for row in cur.fetchall()]


def _gen_connects(cur, schema: str) -> List[EdgeRow]:
    """Airport -> Airport direct connections derived from routes."""
    cur.execute(f"""
        SELECT DISTINCT
            UPPER(source_airport) AS src,
            UPPER(dest_airport) AS dst
        FROM {qident(schema)}.openflights_routes
        WHERE source_airport IS NOT NULL AND source_airport != ''
          AND dest_airport IS NOT NULL AND dest_airport != ''
          AND UPPER(source_airport) < UPPER(dest_airport)
    """)
    return [("Airport", row[0], "CONNECTS", "Airport", row[1]) for row in cur.fetchall()]


def _gen_same_city(cur, schema: str) -> List[EdgeRow]:
    """Airport -> Airport edges for airports sharing the same municipality."""
    cur.execute(f"""
        SELECT DISTINCT
            UPPER(a1.ident) AS apt1,
            UPPER(a2.ident) AS apt2
        FROM {qident(schema)}.ourairports_airports a1
        JOIN {qident(schema)}.ourairports_airports a2
            ON LOWER(a1.municipality) = LOWER(a2.municipality)
            AND LOWER(a1.iso_country) = LOWER(a2.iso_country)
            AND a1.ident < a2.ident
        WHERE a1.municipality IS NOT NULL AND a1.municipality != ''
          AND a1.type IN ('large_airport', 'medium_airport')
          AND a2.type IN ('large_airport', 'medium_airport')
    """)
    return [("Airport", row[0], "SAME_CITY", "Airport", row[1]) for row in cur.fetchall()]


def _gen_ops_edges(cur, schema: str) -> List[EdgeRow]:
    """Operational edges: DEPARTS, ARRIVES, OPERATES from ops_flight_legs."""
    edges: List[EdgeRow] = []

    # Check if ops tables exist
    cur.execute(f"""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = '{schema}'
        AND table_name IN ('ops_flight_legs', 'ops_crew_rosters', 'ops_mel_techlog_events')
    """)
    available = {row[0] for row in cur.fetchall()}

    if "ops_flight_legs" in available:
        cur.execute(f"""
            SELECT DISTINCT
                UPPER(origin_iata) AS origin,
                UPPER(dest_iata) AS dest,
                UPPER(leg_id) AS leg,
                UPPER(tailnum) AS tail
            FROM {qident(schema)}.ops_flight_legs
            WHERE origin_iata IS NOT NULL AND dest_iata IS NOT NULL
        """)
        for row in cur.fetchall():
            origin, dest, leg, tail = row
            if origin and leg:
                edges.append(("Airport", origin, "DEPARTS", "FlightLeg", leg))
            if leg and dest:
                edges.append(("FlightLeg", leg, "ARRIVES", "Airport", dest))
            if tail and leg:
                edges.append(("Tail", tail, "OPERATES", "FlightLeg", leg))

    if "ops_crew_rosters" in available:
        cur.execute(f"""
            SELECT DISTINCT
                UPPER(leg_id) AS leg,
                UPPER(crew_id) AS crew
            FROM {qident(schema)}.ops_crew_rosters
            WHERE leg_id IS NOT NULL AND crew_id IS NOT NULL
        """)
        for row in cur.fetchall():
            edges.append(("FlightLeg", row[0], "CREWED_BY", "Crew", row[1]))

    if "ops_mel_techlog_events" in available:
        cur.execute(f"""
            SELECT DISTINCT
                UPPER(tailnum) AS tail,
                UPPER(leg_id) AS leg
            FROM {qident(schema)}.ops_mel_techlog_events
            WHERE tailnum IS NOT NULL AND leg_id IS NOT NULL
        """)
        for row in cur.fetchall():
            edges.append(("Tail", row[0], "MEL_ON", "FlightLeg", row[1]))

    return edges


def _gen_notam_edges(cur, schema: str) -> List[EdgeRow]:
    """NOTAM -> Airport and NOTAM -> Runway edges from notam_parsed."""
    edges: List[EdgeRow] = []
    cur.execute(f"""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = '{schema}' AND table_name = 'notam_parsed'
    """)
    if not cur.fetchone():
        return edges

    cur.execute(f"""
        SELECT notam_number, icao, affected_runway
        FROM {qident(schema)}.notam_parsed
        WHERE icao IS NOT NULL AND icao != ''
    """)
    for row in cur.fetchall():
        notam_id = f"NOTAM-{row[0].replace('/', '-')}"
        icao = row[1].upper()
        runway = row[2]
        edges.append(("NOTAM", notam_id, "AFFECTS", "Airport", icao))
        if runway:
            # Build a composite runway ID: ICAO-RWY
            rwy_id = f"{icao}-RWY{runway.upper()}"
            edges.append(("NOTAM", notam_id, "AFFECTS_RUNWAY", "Runway", rwy_id))
    return edges


def _gen_reported_at(cur) -> List[EdgeRow]:
    """ASRSReport -> Airport edges from public.asrs_reports location field."""
    edges: List[EdgeRow] = []
    cur.execute("""
        SELECT asrs_report_id, UPPER(location) AS loc
        FROM public.asrs_reports
        WHERE location IS NOT NULL AND location != ''
    """)
    # Parse ICAO or IATA codes from location field
    icao_re = re.compile(r"\b([A-Z]{4})\b")
    for row in cur.fetchall():
        report_id, loc = row
        matches = icao_re.findall(loc)
        for m in matches:
            # Filter out common English 4-letter words that aren't airport codes
            if m in ("NONE", "UNKN", "UNKNOWN", "FROM", "NEAR", "AREA",
                      "OVER", "WITH", "INTO", "UPON", "LAND", "WEST",
                      "EAST", "LEFT", "THIS", "THAT", "THEN", "WHEN",
                      "WERE", "BEEN", "HAVE", "SOME", "THEY", "THEM",
                      "EACH", "BOTH", "MUCH", "VERY", "ALSO", "JUST",
                      "MORE", "MOST", "ONLY", "BACK", "EVEN", "LONG",
                      "MADE", "MANY", "TAKE", "CAME", "COME", "MAKE",
                      "LIKE", "TIME", "PART"):
                continue
            edges.append(("ASRSReport", f"ASRS-{report_id}", "REPORTED_AT", "Airport", m))
            break  # Take first plausible match only
    return edges


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Build enriched knowledge graph edges")
    parser.add_argument("--pg-host", default=os.getenv("PGHOST", ""))
    parser.add_argument("--pg-port", default=os.getenv("PGPORT", "5432"))
    parser.add_argument("--pg-db", default=os.getenv("PGDATABASE", "aviationrag"))
    parser.add_argument("--pg-user", default=os.getenv("PGUSER", ""))
    parser.add_argument("--pg-password", default=os.getenv("PGPASSWORD", ""))
    parser.add_argument("--schema", default="demo")
    parser.add_argument("--export-csv", type=Path, default=None,
                        help="Export enriched edges to CSV for KQL sync")
    args = parser.parse_args()

    if not all([args.pg_host, args.pg_user, args.pg_password]):
        raise ValueError("PGHOST, PGUSER, and PGPASSWORD are required")

    conn = psycopg2.connect(
        host=args.pg_host,
        port=int(args.pg_port),
        database=args.pg_db,
        user=args.pg_user,
        password=args.pg_password,
        sslmode="require",
        connect_timeout=10,
    )
    conn.autocommit = False
    cur = conn.cursor()

    # Phase 2a: Create and load notam_parsed table
    print("Phase 2a: Loading notam_parsed table...")
    notam_count = load_notam_parsed(cur, args.schema)
    conn.commit()

    # Phase 1: Generate graph edges
    print("\nPhase 1: Generating graph edges...")
    all_edges: List[EdgeRow] = []
    edge_stats: Dict[str, int] = {}

    generators = [
        ("HAS_RUNWAY", lambda: _gen_has_runway(cur, args.schema)),
        ("SERVED_BY_ROUTE", lambda: _gen_served_by_route(cur, args.schema)),
        ("OPERATED_BY", lambda: _gen_operated_by(cur, args.schema)),
        ("HAS_NAVAID", lambda: _gen_has_navaid(cur, args.schema)),
        ("HAS_FREQUENCY", lambda: _gen_has_frequency(cur, args.schema)),
        ("CONNECTS", lambda: _gen_connects(cur, args.schema)),
        ("SAME_CITY", lambda: _gen_same_city(cur, args.schema)),
        ("OPS (DEPARTS/ARRIVES/OPERATES/CREWED_BY/MEL_ON)",
         lambda: _gen_ops_edges(cur, args.schema)),
        ("NOTAM (AFFECTS/AFFECTS_RUNWAY)",
         lambda: _gen_notam_edges(cur, args.schema)),
        ("REPORTED_AT", lambda: _gen_reported_at(cur)),
    ]

    for label, gen_fn in generators:
        try:
            edges = gen_fn()
            all_edges.extend(edges)
            # Count by actual edge_type
            for e in edges:
                edge_stats[e[2]] = edge_stats.get(e[2], 0) + 1
            print(f"  {label}: {len(edges)} edges")
        except Exception as exc:
            print(f"  {label}: FAILED — {exc}")

    # Deduplicate
    unique_edges = list(set(all_edges))
    print(f"\nTotal edges: {len(all_edges)} (unique: {len(unique_edges)})")

    # TRUNCATE and bulk INSERT into ops_graph_edges
    print("\nWriting to demo.ops_graph_edges...")
    cur.execute(f"TRUNCATE TABLE {qident(args.schema)}.ops_graph_edges;")
    if unique_edges:
        # Insert in batches of 5000
        batch_size = 5000
        for i in range(0, len(unique_edges), batch_size):
            batch = unique_edges[i:i + batch_size]
            execute_values(
                cur,
                f"INSERT INTO {qident(args.schema)}.ops_graph_edges "
                f"(src_type, src_id, edge_type, dst_type, dst_id) VALUES %s",
                batch,
                page_size=1000,
            )
            if (i + batch_size) % 50000 == 0 or i + batch_size >= len(unique_edges):
                print(f"  Inserted {min(i + batch_size, len(unique_edges))}/{len(unique_edges)}")

    # Create indexes for BFS performance
    print("\nCreating indexes...")
    cur.execute(f"DROP INDEX IF EXISTS {qident(args.schema)}.idx_graph_src_id;")
    cur.execute(f"DROP INDEX IF EXISTS {qident(args.schema)}.idx_graph_dst_id;")
    cur.execute(f"DROP INDEX IF EXISTS {qident(args.schema)}.idx_graph_edge_type;")
    cur.execute(f"""
        CREATE INDEX idx_graph_src_id ON {qident(args.schema)}.ops_graph_edges(UPPER(src_id));
    """)
    cur.execute(f"""
        CREATE INDEX idx_graph_dst_id ON {qident(args.schema)}.ops_graph_edges(UPPER(dst_id));
    """)
    cur.execute(f"""
        CREATE INDEX idx_graph_edge_type ON {qident(args.schema)}.ops_graph_edges(edge_type);
    """)
    print("  Indexes created: idx_graph_src_id, idx_graph_dst_id, idx_graph_edge_type")

    conn.commit()

    # Export to CSV if requested
    if args.export_csv:
        args.export_csv.parent.mkdir(parents=True, exist_ok=True)
        with args.export_csv.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["src_type", "src_id", "edge_type", "dst_type", "dst_id"])
            writer.writerows(unique_edges)
        print(f"\nExported {len(unique_edges)} edges to {args.export_csv}")

    cur.close()
    conn.close()

    # Summary
    print("\n=== Graph Build Summary ===")
    print(f"  notam_parsed: {notam_count} NOTAMs")
    print(f"  Total edges: {len(unique_edges)}")
    for etype, count in sorted(edge_stats.items(), key=lambda x: -x[1]):
        print(f"    {etype}: {count}")


if __name__ == "__main__":
    main()
