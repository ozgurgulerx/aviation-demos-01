#!/usr/bin/env python3
"""
Bulk load multi-source aviation datasets into Azure PostgreSQL.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import json
import os
import re
import zipfile
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Tuple

import psycopg2
from psycopg2.extras import execute_values, Json


ROOT = Path(__file__).resolve().parents[1]


def qident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def latest_path(glob_pattern: str) -> Path:
    candidates = []
    for p in ROOT.glob(glob_pattern):
        if p.is_file() and p.stat().st_size == 0:
            continue
        candidates.append(p)
    matches = sorted(candidates, key=lambda p: p.stat().st_mtime, reverse=True)
    if not matches:
        raise FileNotFoundError(f"No files found for pattern: {glob_pattern}")
    return matches[0]


def latest_dir_with_files(glob_pattern: str, required_files: Sequence[str]) -> Path:
    candidates = sorted(ROOT.glob(glob_pattern), key=lambda p: p.stat().st_mtime, reverse=True)
    for folder in candidates:
        if not folder.is_dir():
            continue
        ok = True
        for name in required_files:
            file_path = folder / name
            if not file_path.exists() or not file_path.is_file() or file_path.stat().st_size == 0:
                ok = False
                break
        if ok:
            return folder
    raise FileNotFoundError(
        f"No directory found for pattern {glob_pattern} with required files: {', '.join(required_files)}"
    )


def safe_col(name: str, idx: int) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_]", "_", (name or "").strip())
    cleaned = re.sub(r"_+", "_", cleaned).strip("_").lower()
    if not cleaned:
        cleaned = f"col_{idx}"
    if cleaned[0].isdigit():
        cleaned = f"c_{cleaned}"
    return cleaned[:60]


def iter_jsonl(path: Path) -> Iterable[dict]:
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def ensure_schema(cur, schema: str) -> None:
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {qident(schema)};")


def create_text_table(cur, schema: str, table: str, columns: Sequence[str], drop: bool = True) -> List[str]:
    safe = []
    seen = set()
    for idx, col in enumerate(columns, start=1):
        c = safe_col(col, idx)
        if c in seen:
            c = f"{c}_{idx}"
        seen.add(c)
        safe.append(c)
    if drop:
        cur.execute(f"DROP TABLE IF EXISTS {qident(schema)}.{qident(table)};")
    cols_sql = ", ".join(f"{qident(c)} TEXT" for c in safe)
    cur.execute(f"CREATE TABLE {qident(schema)}.{qident(table)} ({cols_sql});")
    return safe


def copy_csv(cur, schema: str, table: str, path: Path, drop: bool = True) -> int:
    with path.open("r", encoding="utf-8", errors="ignore", newline="") as f:
        reader = csv.reader(f)
        header = next(reader)
    safe = create_text_table(cur, schema, table, header, drop=drop)
    col_list = ", ".join(qident(c) for c in safe)
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        cur.copy_expert(
            f"COPY {qident(schema)}.{qident(table)} ({col_list}) FROM STDIN WITH (FORMAT csv, HEADER true)",
            f,
        )
    cur.execute(f"SELECT COUNT(*) FROM {qident(schema)}.{qident(table)};")
    return int(cur.fetchone()[0])


def copy_gz_csv(cur, schema: str, table: str, path: Path, drop: bool = True) -> int:
    with gzip.open(path, "rt", encoding="utf-8", errors="ignore", newline="") as f:
        reader = csv.reader(f)
        header = next(reader)
    safe = create_text_table(cur, schema, table, header, drop=drop)
    col_list = ", ".join(qident(c) for c in safe)
    with gzip.open(path, "rt", encoding="utf-8", errors="ignore") as f:
        cur.copy_expert(
            f"COPY {qident(schema)}.{qident(table)} ({col_list}) FROM STDIN WITH (FORMAT csv, HEADER true)",
            f,
        )
    cur.execute(f"SELECT COUNT(*) FROM {qident(schema)}.{qident(table)};")
    return int(cur.fetchone()[0])


def load_gz_lines_raw(cur, schema: str, table: str, path: Path) -> int:
    cur.execute(f"DROP TABLE IF EXISTS {qident(schema)}.{qident(table)};")
    cur.execute(
        f"""
        CREATE TABLE {qident(schema)}.{qident(table)} (
            line_no BIGINT NOT NULL,
            raw_text TEXT NOT NULL
        );
        """
    )
    rows: List[Tuple[int, str]] = []
    with gzip.open(path, "rt", encoding="utf-8", errors="ignore") as f:
        next(f, None)  # header
        for idx, line in enumerate(f, start=2):
            line = line.rstrip("\n")
            if not line:
                continue
            rows.append((idx, line))
            if len(rows) >= 1000:
                execute_values(
                    cur,
                    f"INSERT INTO {qident(schema)}.{qident(table)} (line_no, raw_text) VALUES %s",
                    rows,
                    page_size=1000,
                )
                rows = []
    if rows:
        execute_values(
            cur,
            f"INSERT INTO {qident(schema)}.{qident(table)} (line_no, raw_text) VALUES %s",
            rows,
            page_size=1000,
        )
    cur.execute(f"SELECT COUNT(*) FROM {qident(schema)}.{qident(table)};")
    return int(cur.fetchone()[0])


def load_openflights_dat(cur, schema: str, table: str, path: Path, columns: Sequence[str]) -> int:
    safe = create_text_table(cur, schema, table, columns, drop=True)
    rows: List[Tuple[str, ...]] = []
    with path.open("r", encoding="utf-8", errors="ignore", newline="") as f:
        reader = csv.reader(f)
        for r in reader:
            if not r:
                continue
            r = list(r)
            if len(r) < len(safe):
                r.extend([""] * (len(safe) - len(r)))
            rows.append(tuple(r[: len(safe)]))
    if rows:
        sql = f"INSERT INTO {qident(schema)}.{qident(table)} ({', '.join(qident(c) for c in safe)}) VALUES %s"
        execute_values(cur, sql, rows, page_size=1000)
    return len(rows)


def load_asrs(cur, records_file: Path) -> int:
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS public.asrs_reports (
            asrs_report_id TEXT PRIMARY KEY,
            event_date DATE,
            location TEXT,
            aircraft_type TEXT,
            flight_phase TEXT,
            narrative_type TEXT,
            title TEXT,
            report_text TEXT NOT NULL,
            raw_json TEXT NOT NULL,
            ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """
    )
    cur.execute("ALTER TABLE public.asrs_reports ALTER COLUMN ingested_at SET DEFAULT NOW();")
    batch: List[Tuple] = []
    total = 0
    for obj in iter_jsonl(records_file):
        batch.append(
            (
                str(obj.get("asrs_report_id", "")),
                obj.get("event_date"),
                obj.get("location"),
                obj.get("aircraft_type"),
                obj.get("flight_phase"),
                obj.get("narrative_type"),
                obj.get("title"),
                obj.get("report_text"),
                obj.get("raw_json"),
            )
        )
        if len(batch) >= 1000:
            execute_values(
                cur,
                """
                INSERT INTO public.asrs_reports (
                    asrs_report_id, event_date, location, aircraft_type, flight_phase,
                    narrative_type, title, report_text, raw_json
                ) VALUES %s
                ON CONFLICT (asrs_report_id) DO UPDATE SET
                    event_date = EXCLUDED.event_date,
                    location = EXCLUDED.location,
                    aircraft_type = EXCLUDED.aircraft_type,
                    flight_phase = EXCLUDED.flight_phase,
                    narrative_type = EXCLUDED.narrative_type,
                    title = EXCLUDED.title,
                    report_text = EXCLUDED.report_text,
                    raw_json = EXCLUDED.raw_json,
                    ingested_at = NOW()
                """,
                batch,
                page_size=200,
            )
            total += len(batch)
            if total % 10000 == 0:
                print(f"  ASRS upserted: {total}")
            batch = []

    if batch:
        execute_values(
            cur,
            """
            INSERT INTO public.asrs_reports (
                asrs_report_id, event_date, location, aircraft_type, flight_phase,
                narrative_type, title, report_text, raw_json
            ) VALUES %s
            ON CONFLICT (asrs_report_id) DO UPDATE SET
                event_date = EXCLUDED.event_date,
                location = EXCLUDED.location,
                aircraft_type = EXCLUDED.aircraft_type,
                flight_phase = EXCLUDED.flight_phase,
                narrative_type = EXCLUDED.narrative_type,
                title = EXCLUDED.title,
                report_text = EXCLUDED.report_text,
                raw_json = EXCLUDED.raw_json,
                ingested_at = NOW()
            """,
            batch,
            page_size=200,
        )
        total += len(batch)
    return total


def load_raw_json_table(cur, schema: str, table: str, rows: List[Tuple[str, dict]]) -> int:
    cur.execute(f"DROP TABLE IF EXISTS {qident(schema)}.{qident(table)};")
    cur.execute(
        f"""
        CREATE TABLE {qident(schema)}.{qident(table)} (
            source_file TEXT NOT NULL,
            payload JSONB NOT NULL
        );
        """
    )
    if rows:
        execute_values(
            cur,
            f"INSERT INTO {qident(schema)}.{qident(table)} (source_file, payload) VALUES %s",
            [(src, Json(obj)) for src, obj in rows],
            page_size=1000,
        )
    return len(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Bulk load multi-source datasets into PostgreSQL")
    parser.add_argument("--pg-host", default=os.getenv("PGHOST", ""), required=False)
    parser.add_argument("--pg-port", default=os.getenv("PGPORT", "5432"))
    parser.add_argument("--pg-db", default=os.getenv("PGDATABASE", "postgres"))
    parser.add_argument("--pg-user", default=os.getenv("PGUSER", ""))
    parser.add_argument("--pg-password", default=os.getenv("PGPASSWORD", ""))
    parser.add_argument("--schema", default="demo")
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
    ensure_schema(cur, args.schema)

    summary: Dict[str, int] = {}

    # ASRS core structured table
    summary["public.asrs_reports"] = load_asrs(cur, ROOT / "data/c1-asrs/processed/asrs_records.jsonl")
    conn.commit()

    # OurAirports dimensions
    ourairports_airports = latest_path("data/g-ourairports_recent/airports_*.csv")
    ourairports_runways = latest_path("data/g-ourairports_recent/runways_*.csv")
    ourairports_navaids = latest_path("data/g-ourairports_recent/navaids_*.csv")
    ourairports_freq = latest_path("data/g-ourairports_recent/airport-frequencies_*.csv")

    summary[f"{args.schema}.ourairports_airports"] = copy_csv(
        cur, args.schema, "ourairports_airports", ourairports_airports
    )
    summary[f"{args.schema}.ourairports_runways"] = copy_csv(
        cur, args.schema, "ourairports_runways", ourairports_runways
    )
    summary[f"{args.schema}.ourairports_navaids"] = copy_csv(
        cur, args.schema, "ourairports_navaids", ourairports_navaids
    )
    summary[f"{args.schema}.ourairports_frequencies"] = copy_csv(
        cur,
        args.schema,
        "ourairports_frequencies",
        ourairports_freq,
    )
    conn.commit()

    # OpenFlights network data
    openflights_routes = latest_path("data/f-openflights/raw/routes_*.dat")
    openflights_airports = latest_path("data/f-openflights/raw/airports_*.dat")
    openflights_airlines = latest_path("data/f-openflights/raw/airlines_*.dat")

    summary[f"{args.schema}.openflights_routes"] = load_openflights_dat(
        cur,
        args.schema,
        "openflights_routes",
        openflights_routes,
        ["airline", "airline_id", "source_airport", "source_airport_id", "dest_airport", "dest_airport_id", "codeshare", "stops", "equipment"],
    )
    summary[f"{args.schema}.openflights_airports"] = load_openflights_dat(
        cur,
        args.schema,
        "openflights_airports",
        openflights_airports,
        ["airport_id", "name", "city", "country", "iata", "icao", "latitude", "longitude", "altitude", "timezone", "dst", "tzdb", "type", "source"],
    )
    summary[f"{args.schema}.openflights_airlines"] = load_openflights_dat(
        cur,
        args.schema,
        "openflights_airlines",
        openflights_airlines,
        ["airline_id", "name", "alias", "iata", "icao", "callsign", "country", "active"],
    )
    conn.commit()

    # OpenSky + NOTAM raw JSON tables
    opensky_rows: List[Tuple[str, dict]] = []
    for f in sorted((ROOT / "data/e-opensky_recent").glob("*.json")):
        with f.open("r", encoding="utf-8") as h:
            obj = json.load(h)
        if isinstance(obj, list):
            for item in obj:
                opensky_rows.append((f.name, item))
        else:
            opensky_rows.append((f.name, obj))
    summary[f"{args.schema}.opensky_raw"] = load_raw_json_table(cur, args.schema, "opensky_raw", opensky_rows)

    notam_file = latest_path("data/h-notam_recent/*/search_location_istanbul.jsonl")
    notam_rows = [(notam_file.name, obj) for obj in iter_jsonl(notam_file)]
    summary[f"{args.schema}.notam_raw"] = load_raw_json_table(cur, args.schema, "notam_raw", notam_rows)
    conn.commit()

    # Aviation weather hazards
    hazards_airsigmets = latest_path("data/i-aviationweather_hazards_recent/airsigmets.cache.csv_*.gz")
    hazards_gairmets = latest_path("data/i-aviationweather_hazards_recent/gairmets.cache.csv_*.gz")
    hazards_aireps = latest_path("data/i-aviationweather_hazards_recent/aircraftreports.cache.csv_*.gz")

    summary[f"{args.schema}.hazards_airsigmets"] = copy_gz_csv(
        cur, args.schema, "hazards_airsigmets", hazards_airsigmets
    )
    summary[f"{args.schema}.hazards_gairmets"] = copy_gz_csv(
        cur, args.schema, "hazards_gairmets", hazards_gairmets
    )
    summary[f"{args.schema}.hazards_aireps_raw"] = load_gz_lines_raw(
        cur, args.schema, "hazards_aireps_raw", hazards_aireps
    )
    conn.commit()

    # Synthetic operational overlay tables
    syn_root = latest_dir_with_files(
        "data/j-synthetic_ops_overlay/*/synthetic",
        [
            "ops_flight_legs.csv",
            "ops_turnaround_milestones.csv",
            "ops_baggage_events.csv",
            "ops_crew_rosters.csv",
            "ops_mel_techlog_events.csv",
            "ops_graph_edges.csv",
        ],
    )
    summary[f"{args.schema}.ops_flight_legs"] = copy_csv(cur, args.schema, "ops_flight_legs", syn_root / "ops_flight_legs.csv")
    summary[f"{args.schema}.ops_turnaround_milestones"] = copy_csv(cur, args.schema, "ops_turnaround_milestones", syn_root / "ops_turnaround_milestones.csv")
    summary[f"{args.schema}.ops_baggage_events"] = copy_csv(cur, args.schema, "ops_baggage_events", syn_root / "ops_baggage_events.csv")
    summary[f"{args.schema}.ops_crew_rosters"] = copy_csv(cur, args.schema, "ops_crew_rosters", syn_root / "ops_crew_rosters.csv")
    summary[f"{args.schema}.ops_mel_techlog_events"] = copy_csv(cur, args.schema, "ops_mel_techlog_events", syn_root / "ops_mel_techlog_events.csv")
    summary[f"{args.schema}.ops_graph_edges"] = copy_csv(cur, args.schema, "ops_graph_edges", syn_root / "ops_graph_edges.csv")
    conn.commit()

    # Airline schedule feed (small delay-causes CSV from zip + metadata for larger on-time zips)
    delay_zip = latest_path("data/k-airline_schedule_feed/*/raw/*airline_delay_causes*.zip")
    schedule_raw_dir = delay_zip.parent
    with zipfile.ZipFile(delay_zip, "r") as zf:
        members = [m for m in zf.namelist() if m.lower().endswith(".csv")]
        if members:
            member = members[0]
            extracted = Path("/tmp") / member
            zf.extract(member, "/tmp")
            summary[f"{args.schema}.schedule_delay_causes"] = copy_csv(cur, args.schema, "schedule_delay_causes", extracted)

    cur.execute(f"DROP TABLE IF EXISTS {qident(args.schema)}.schedule_assets;")
    cur.execute(
        f"""
        CREATE TABLE {qident(args.schema)}.schedule_assets (
            file_name TEXT PRIMARY KEY,
            file_path TEXT NOT NULL,
            bytes BIGINT NOT NULL
        );
        """
    )
    assets = []
    for f in sorted(schedule_raw_dir.glob("*")):
        assets.append((f.name, str(f), f.stat().st_size))
    execute_values(
        cur,
        f"INSERT INTO {qident(args.schema)}.schedule_assets (file_name, file_path, bytes) VALUES %s",
        assets,
        page_size=200,
    )
    summary[f"{args.schema}.schedule_assets"] = len(assets)

    conn.commit()
    cur.close()
    conn.close()

    print("PostgreSQL bulk load complete")
    for k, v in summary.items():
        print(f"  {k}: {v}")


if __name__ == "__main__":
    main()
