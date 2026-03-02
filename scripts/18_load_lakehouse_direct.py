"""Load missing Lakehouse tables directly via OneLake + deltalake.

Reads from local CSV/SQLite files. Writes Delta tables to OneLake Lakehouse.
Bypasses the Fabric Warehouse entirely.
"""
import csv
import json
import os
import re
import sqlite3
import subprocess
import sys
import tempfile
import shutil

import pyarrow as pa
from deltalake import write_deltalake

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
WS_ID = "cfbb82a5-799e-421b-b2cc-8a164b17a849"
LH_ID = "de9e3323-0952-479e-a6ea-0de93c2d4f95"
ONELAKE_BASE = f"abfss://{WS_ID}@onelake.dfs.fabric.microsoft.com/{LH_ID}/Tables"

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data")
OPS_DIR = os.path.join(DATA_DIR, "j-synthetic_ops_overlay/20260219T180125Z/synthetic")
AIRPORTS_CSV = os.path.join(DATA_DIR, "b-airports.csv")
AIRLINES_DAT = os.path.join(DATA_DIR, "f-openflights/raw/airlines_20260219T165104Z.dat")
SQLITE_DB = os.path.join(DATA_DIR, "aviation.db")


def get_token():
    """Get Azure AD token for OneLake (storage.azure.com)."""
    result = subprocess.run(
        ["az", "account", "get-access-token", "--resource", "https://storage.azure.com",
         "--query", "accessToken", "-o", "tsv"],
        capture_output=True, text=True
    )
    return result.stdout.strip()


def storage_opts(token):
    """Storage options for deltalake OneLake access."""
    return {
        "bearer_token": token,
        "use_fabric_endpoint": "true",
    }


# ---------------------------------------------------------------------------
# Table loaders — each returns a PyArrow table
# ---------------------------------------------------------------------------

def load_dim_airports():
    """Load OurAirports data, filter to non-null IATA codes."""
    print("  Reading OurAirports CSV...")
    with open(AIRPORTS_CSV, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = []
        for r in reader:
            iata = (r.get("iata_code") or "").strip()
            if not iata:
                continue
            rows.append({
                "iata_code": iata,
                "icao_code": (r.get("icao_code") or "").strip() or None,
                "name": (r.get("name") or "").strip() or None,
                "airport_type": (r.get("type") or "").strip() or None,
                "latitude": float(r["latitude_deg"]) if r.get("latitude_deg") else None,
                "longitude": float(r["longitude_deg"]) if r.get("longitude_deg") else None,
                "elevation": float(r["elevation_ft"]) if r.get("elevation_ft") else None,
                "country": (r.get("iso_country") or "").strip() or None,
                "region": (r.get("iso_region") or "").strip() or None,
                "city": (r.get("municipality") or "").strip() or None,
            })

    schema = pa.schema([
        ("iata_code", pa.string()),
        ("icao_code", pa.string()),
        ("name", pa.string()),
        ("airport_type", pa.string()),
        ("latitude", pa.float64()),
        ("longitude", pa.float64()),
        ("elevation", pa.float64()),
        ("country", pa.string()),
        ("region", pa.string()),
        ("city", pa.string()),
    ])
    return pa.table({col.name: [r[col.name] for r in rows] for col in schema}, schema=schema)


def load_dim_airlines():
    """Load OpenFlights airlines, filter to active + non-null IATA."""
    print("  Reading OpenFlights airlines DAT...")
    rows = []
    with open(AIRLINES_DAT, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        for r in reader:
            if len(r) < 8:
                continue
            # Fields: airline_id, name, alias, iata, icao, callsign, country, active
            iata = r[3].strip().strip('"')
            active = r[7].strip().strip('"')
            if not iata or iata in ("\\N", "-", "N/A", ""):
                continue
            if active != "Y":
                continue
            rows.append({
                "iata": iata,
                "icao": r[4].strip().strip('"') if r[4].strip().strip('"') not in ("\\N", "") else None,
                "name": r[1].strip().strip('"') if r[1].strip().strip('"') != "\\N" else None,
                "callsign": r[5].strip().strip('"') if r[5].strip().strip('"') not in ("\\N", "") else None,
                "country": r[6].strip().strip('"') if r[6].strip().strip('"') not in ("\\N", "") else None,
                "active": active,
            })

    schema = pa.schema([
        ("iata", pa.string()),
        ("icao", pa.string()),
        ("name", pa.string()),
        ("callsign", pa.string()),
        ("country", pa.string()),
        ("active", pa.string()),
    ])
    return pa.table({col.name: [r[col.name] for r in rows] for col in schema}, schema=schema)


def load_ops_csv(filename, columns_spec):
    """Load an ops CSV file with type conversions."""
    filepath = os.path.join(OPS_DIR, filename)
    print(f"  Reading {filename}...")
    with open(filepath, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    arrays = {}
    schema_fields = []
    for col_name, pa_type, converter in columns_spec:
        values = []
        for r in rows:
            raw = r.get(col_name, "").strip()
            if not raw:
                values.append(None)
            else:
                try:
                    values.append(converter(raw))
                except (ValueError, TypeError):
                    values.append(None)
        arrays[col_name] = values
        schema_fields.append((col_name, pa_type))

    schema = pa.schema(schema_fields)
    return pa.table(arrays, schema=schema)


def load_ops_flight_legs():
    return load_ops_csv("ops_flight_legs.csv", [
        ("leg_id", pa.string(), str),
        ("carrier_code", pa.string(), str),
        ("flight_no", pa.string(), str),
        ("origin_iata", pa.string(), str),
        ("dest_iata", pa.string(), str),
        ("scheduled_dep_utc", pa.string(), str),
        ("scheduled_arr_utc", pa.string(), str),
        ("tailnum", pa.string(), str),
        ("distance_nm", pa.float64(), float),
        ("passengers", pa.int64(), int),
    ])


def load_ops_crew_rosters():
    return load_ops_csv("ops_crew_rosters.csv", [
        ("duty_id", pa.string(), str),
        ("crew_id", pa.string(), str),
        ("role", pa.string(), str),
        ("leg_id", pa.string(), str),
        ("duty_start_utc", pa.string(), str),
        ("duty_end_utc", pa.string(), str),
        ("cumulative_duty_hours", pa.float64(), float),
        ("legality_risk_flag", pa.int64(), int),
    ])


def load_ops_mel_techlog():
    return load_ops_csv("ops_mel_techlog_events.csv", [
        ("tech_event_id", pa.string(), str),
        ("leg_id", pa.string(), str),
        ("event_ts_utc", pa.string(), str),
        ("jasc_code", pa.string(), str),
        ("mel_category", pa.string(), str),
        ("deferred_flag", pa.int64(), int),
        ("severity", pa.string(), str),
        ("source_proxy", pa.string(), str),
        ("discrepancy_note", pa.string(), str),
    ])


_ICAO_RE = re.compile(r"\b([A-Z]{4})\b")
_IATA_RE = re.compile(r"\b([A-Z]{3})\b")
_ICAO_NOISE = frozenset([
    "NONE", "UNKN", "FROM", "NEAR", "AREA", "OVER", "WITH", "INTO", "UPON",
    "LAND", "WEST", "EAST", "LEFT", "THIS", "THAT", "THEN", "WHEN", "WERE",
    "BEEN", "HAVE", "SOME", "THEY", "THEM", "EACH", "BOTH", "MUCH", "VERY",
    "ALSO", "JUST", "MORE", "MOST", "ONLY", "BACK", "EVEN", "LONG", "MADE",
    "MANY", "TAKE", "CAME", "COME", "MAKE", "LIKE", "TIME", "PART", "TOLD",
    "SAID", "LOST", "USED", "CALL", "GAVE", "WENT", "DOES", "DONE", "TOOK",
    "KNEW", "FELT", "KEPT", "HELD", "WILL", "TURN",
])
_IATA_NOISE = frozenset([
    "THE", "AND", "FOR", "NOT", "WAS", "ARE", "BUT", "ALL", "CAN", "HAD",
    "HER", "ONE", "OUR", "OUT", "DAY", "GET", "HAS", "HIM", "HIS", "HOW",
    "ITS", "MAY", "NEW", "NOW", "OLD", "SEE", "WAY", "WHO", "DID", "GOT",
    "LET", "SAY", "TOO", "USE", "ATC", "VFR", "IFR", "ILS", "VOR", "DME",
    "FPM", "AGL", "MSL", "NDB", "GPS", "MEL", "APU", "TWR", "APP", "CTR",
    "DEP", "GND", "OAT", "RPM",
])


def _extract_location_iata(location: str) -> str | None:
    """Extract a plausible IATA airport code from ASRS free-text location."""
    if not location:
        return None
    loc_upper = location.upper().strip()

    # Try 4-letter ICAO codes first — strip leading K for US airports
    for m in _ICAO_RE.finditer(loc_upper):
        code = m.group(1)
        if code in _ICAO_NOISE:
            continue
        # US ICAO: K + 3-letter IATA
        if code.startswith("K") and len(code) == 4:
            return code[1:]
        # Pacific/Hawaii (P prefix) and other regions
        if code[0] in ("P", "T", "L", "E"):
            return code  # Return ICAO as-is, maps vary
        return None  # Unknown 4-letter code

    # Fall back to 3-letter IATA codes
    for m in _IATA_RE.finditer(loc_upper):
        code = m.group(1)
        if code in _IATA_NOISE:
            continue
        return code

    return None


def load_asrs_reports():
    """Load ASRS reports from SQLite (skip raw_json), extract location_iata."""
    print("  Reading ASRS reports from SQLite...")
    conn = sqlite3.connect(SQLITE_DB)
    cur = conn.execute(
        "SELECT asrs_report_id, event_date, location, aircraft_type, "
        "flight_phase, narrative_type, title, report_text, ingested_at "
        "FROM asrs_reports"
    )

    # Build column arrays
    cols = {
        "asrs_report_id": [], "event_date": [], "location": [],
        "location_iata": [],
        "aircraft_type": [], "flight_phase": [], "narrative_type": [],
        "title": [], "report_text": [], "ingested_at": [],
    }
    count = 0
    iata_count = 0
    for row in cur:
        cols["asrs_report_id"].append(row[0])
        cols["event_date"].append(row[1])
        cols["location"].append(row[2])
        iata = _extract_location_iata(row[2])
        cols["location_iata"].append(iata)
        if iata:
            iata_count += 1
        cols["aircraft_type"].append(row[3])
        cols["flight_phase"].append(row[4])
        cols["narrative_type"].append(row[5])
        cols["title"].append(row[6])
        cols["report_text"].append(row[7])
        cols["ingested_at"].append(row[8])
        count += 1
        if count % 50000 == 0:
            print(f"    ...{count:,} rows")

    conn.close()
    print(f"  location_iata extracted: {iata_count}/{count} rows ({100*iata_count/max(count,1):.1f}%)")

    schema = pa.schema([
        ("asrs_report_id", pa.string()),
        ("event_date", pa.string()),
        ("location", pa.string()),
        ("location_iata", pa.string()),
        ("aircraft_type", pa.string()),
        ("flight_phase", pa.string()),
        ("narrative_type", pa.string()),
        ("title", pa.string()),
        ("report_text", pa.string()),
        ("ingested_at", pa.string()),
    ])
    return pa.table(cols, schema=schema)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

TABLES = [
    ("dim_airports", load_dim_airports),
    ("dim_airlines", load_dim_airlines),
    ("ops_flight_legs", load_ops_flight_legs),
    ("ops_crew_rosters", load_ops_crew_rosters),
    ("ops_mel_techlog_events", load_ops_mel_techlog),
    ("asrs_reports", load_asrs_reports),
]


def main():
    # Fix data dir path
    global DATA_DIR, OPS_DIR, AIRPORTS_CSV, AIRLINES_DAT, SQLITE_DB
    base = os.path.dirname(os.path.abspath(__file__))
    if not os.path.exists(DATA_DIR):
        DATA_DIR = os.path.join(base, "data")
    if not os.path.exists(DATA_DIR):
        # Try from project root
        DATA_DIR = "/Users/ozgurguler/Developer/Projects/aviation-demos-01/data"
    OPS_DIR = os.path.join(DATA_DIR, "j-synthetic_ops_overlay/20260219T180125Z/synthetic")
    AIRPORTS_CSV = os.path.join(DATA_DIR, "b-airports.csv")
    AIRLINES_DAT = os.path.join(DATA_DIR, "f-openflights/raw/airlines_20260219T165104Z.dat")
    SQLITE_DB = os.path.join(DATA_DIR, "aviation.db")

    print("=== Direct Lakehouse Loader ===")
    print(f"OneLake base: {ONELAKE_BASE}")
    print(f"Data dir: {DATA_DIR}")
    print()

    token = get_token()
    if not token:
        print("ERROR: Could not get Azure token")
        sys.exit(1)
    print(f"Token acquired ({len(token)} chars)")

    opts = storage_opts(token)
    results = {}

    for table_name, loader_fn in TABLES:
        print(f"\n--- {table_name} ---")
        try:
            table = loader_fn()
            print(f"  Loaded: {table.num_rows:,} rows, {table.num_columns} columns")

            target = f"{ONELAKE_BASE}/{table_name}"
            print(f"  Writing to: {target}")
            write_deltalake(target, table, mode="overwrite", storage_options=opts)
            print(f"  OK: {table_name} written successfully")
            results[table_name] = ("OK", table.num_rows)
        except Exception as e:
            print(f"  FAILED: {e}")
            results[table_name] = ("FAIL", str(e))

    # Summary
    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")
    for name, (status, detail) in results.items():
        if status == "OK":
            print(f"  {name:30s}  {status}  {detail:,} rows")
        else:
            print(f"  {name:30s}  {status}  {detail}")


if __name__ == "__main__":
    main()
