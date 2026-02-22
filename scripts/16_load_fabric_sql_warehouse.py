#!/usr/bin/env python3
"""
Load BTS on-time performance data into a Fabric SQL Warehouse.

Reads BTS on-time CSVs (from zip) and airline delay causes CSV,
then bulk-inserts via pyodbc over TDS (ODBC Driver 18 for SQL Server)
with Azure AD token authentication.

Usage:
    python scripts/16_load_fabric_sql_warehouse.py
    python scripts/16_load_fabric_sql_warehouse.py --dry-run
    python scripts/16_load_fabric_sql_warehouse.py --warehouse-server <server> --database <db>
"""

from __future__ import annotations

import argparse
import csv
import io
import os
import struct
import zipfile
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

from azure.identity import DefaultAzureCredential
from dotenv import load_dotenv

load_dotenv()

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DATA_DIR = ROOT / "data" / "k-airline_schedule_feed"

# Fabric SQL Warehouse connection defaults (from workspace discovery)
DEFAULT_SERVER = os.getenv(
    "FABRIC_SQL_SERVER",
    "qfnasuqpcmdexa7rtbm3frz54y-uwblxt46penufmwmrilewf5ije.datawarehouse.fabric.microsoft.com",
)
DEFAULT_DATABASE = os.getenv("FABRIC_SQL_DATABASE", "PostAssignWarehouse1")

# BTS on-time columns we care about (subset of the full 110-column schema)
ONTIME_COLS = [
    ("Year", "INT"),
    ("Quarter", "INT"),
    ("Month", "INT"),
    ("DayofMonth", "INT"),
    ("DayOfWeek", "INT"),
    ("FlightDate", "VARCHAR(10)"),
    ("IATA_Code_Marketing_Airline", "VARCHAR(10)"),
    ("Flight_Number_Marketing_Airline", "VARCHAR(10)"),
    ("IATA_Code_Operating_Airline", "VARCHAR(10)"),
    ("Tail_Number", "VARCHAR(20)"),
    ("Flight_Number_Operating_Airline", "VARCHAR(10)"),
    ("Origin", "VARCHAR(10)"),
    ("OriginCityName", "VARCHAR(100)"),
    ("OriginState", "VARCHAR(5)"),
    ("Dest", "VARCHAR(10)"),
    ("DestCityName", "VARCHAR(100)"),
    ("DestState", "VARCHAR(5)"),
    ("CRSDepTime", "VARCHAR(10)"),
    ("DepTime", "VARCHAR(10)"),
    ("DepDelay", "FLOAT"),
    ("DepDelayMinutes", "FLOAT"),
    ("DepDel15", "FLOAT"),
    ("CRSArrTime", "VARCHAR(10)"),
    ("ArrTime", "VARCHAR(10)"),
    ("ArrDelay", "FLOAT"),
    ("ArrDelayMinutes", "FLOAT"),
    ("ArrDel15", "FLOAT"),
    ("Cancelled", "FLOAT"),
    ("CancellationCode", "VARCHAR(5)"),
    ("Diverted", "FLOAT"),
    ("CRSElapsedTime", "FLOAT"),
    ("ActualElapsedTime", "FLOAT"),
    ("AirTime", "FLOAT"),
    ("Distance", "FLOAT"),
    ("DistanceGroup", "INT"),
    ("CarrierDelay", "FLOAT"),
    ("WeatherDelay", "FLOAT"),
    ("NASDelay", "FLOAT"),
    ("SecurityDelay", "FLOAT"),
    ("LateAircraftDelay", "FLOAT"),
]

DELAY_CAUSES_COLS = [
    ("year", "INT"),
    ("month", "INT"),
    ("carrier", "VARCHAR(10)"),
    ("carrier_name", "VARCHAR(100)"),
    ("airport", "VARCHAR(10)"),
    ("airport_name", "VARCHAR(200)"),
    ("arr_flights", "FLOAT"),
    ("arr_del15", "FLOAT"),
    ("carrier_ct", "FLOAT"),
    ("weather_ct", "FLOAT"),
    ("nas_ct", "FLOAT"),
    ("security_ct", "FLOAT"),
    ("late_aircraft_ct", "FLOAT"),
    ("arr_cancelled", "FLOAT"),
    ("arr_diverted", "FLOAT"),
    ("arr_delay", "FLOAT"),
    ("carrier_delay", "FLOAT"),
    ("weather_delay", "FLOAT"),
    ("nas_delay", "FLOAT"),
    ("security_delay", "FLOAT"),
    ("late_aircraft_delay", "FLOAT"),
]


def _get_aad_token() -> str:
    """Get Azure AD token for Fabric SQL Warehouse (database.windows.net scope)."""
    credential = DefaultAzureCredential()
    token = credential.get_token("https://database.windows.net/.default")
    return token.token


def _token_to_pyodbc_attrs(token: str) -> bytes:
    """Convert AAD token string to pyodbc connection attribute bytes (SQL_COPT_SS_ACCESS_TOKEN)."""
    encoded = token.encode("UTF-16-LE")
    return struct.pack(f"<I{len(encoded)}s", len(encoded), encoded)


def connect(server: str, database: str):
    """Connect to Fabric SQL Warehouse using AAD token auth."""
    import pyodbc

    token = _get_aad_token()
    token_bytes = _token_to_pyodbc_attrs(token)

    conn_str = (
        f"Driver={{ODBC Driver 18 for SQL Server}};"
        f"Server={server},1433;"
        f"Database={database};"
        f"Encrypt=yes;"
        f"TrustServerCertificate=no;"
        f"Connection Timeout=120;"
    )
    conn = pyodbc.connect(conn_str, attrs_before={1256: token_bytes}, timeout=120)
    return conn


def _create_tables(conn, drop_first: bool = False) -> None:
    """Create BTS tables in Fabric SQL Warehouse."""
    cursor = conn.cursor()

    ontime_ddl = ", ".join(f"[{col}] {dtype} NULL" for col, dtype in ONTIME_COLS)
    delay_ddl = ", ".join(f"[{col}] {dtype} NULL" for col, dtype in DELAY_CAUSES_COLS)

    if drop_first:
        cursor.execute("DROP TABLE IF EXISTS bts_ontime_reporting")
        cursor.execute("DROP TABLE IF EXISTS airline_delay_causes")
        conn.commit()

    cursor.execute(f"""
        IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'bts_ontime_reporting')
        CREATE TABLE bts_ontime_reporting ({ontime_ddl})
    """)
    cursor.execute(f"""
        IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'airline_delay_causes')
        CREATE TABLE airline_delay_causes ({delay_ddl})
    """)
    conn.commit()
    print("Tables created/verified")


def _safe_float(val: str) -> Optional[float]:
    """Parse float, returning None for empty/invalid."""
    val = val.strip().strip('"')
    if not val:
        return None
    try:
        return float(val)
    except ValueError:
        return None


def _safe_int(val: str) -> Optional[int]:
    """Parse int, returning None for empty/invalid."""
    val = val.strip().strip('"')
    if not val:
        return None
    try:
        return int(float(val))
    except ValueError:
        return None


def _safe_str(val: str, max_len: int = 200) -> Optional[str]:
    """Clean string value."""
    val = val.strip().strip('"')
    if not val:
        return None
    return val[:max_len]


def _find_latest_data_dir() -> Path:
    """Find the latest BTS data snapshot directory."""
    candidates = sorted(DEFAULT_DATA_DIR.glob("*/raw"), key=lambda p: p.parent.name, reverse=True)
    for c in candidates:
        zips = list(c.glob("On_Time_*.zip"))
        if zips:
            return c
    raise FileNotFoundError(f"No BTS on-time zip files found in {DEFAULT_DATA_DIR}")


def _iter_ontime_from_zip(zip_path: Path) -> Iterable[Dict[str, str]]:
    """Yield rows from BTS on-time zip file."""
    with zipfile.ZipFile(zip_path) as zf:
        csv_names = [n for n in zf.namelist() if n.endswith(".csv")]
        if not csv_names:
            return
        with zf.open(csv_names[0]) as f:
            reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8", errors="ignore"))
            for row in reader:
                yield row


def _extract_ontime_row(row: Dict[str, str]) -> Tuple:
    """Extract the columns we need from a BTS on-time row."""
    return (
        _safe_int(row.get("Year", "")),
        _safe_int(row.get("Quarter", "")),
        _safe_int(row.get("Month", "")),
        _safe_int(row.get("DayofMonth", "")),
        _safe_int(row.get("DayOfWeek", "")),
        _safe_str(row.get("FlightDate", ""), 10),
        _safe_str(row.get("IATA_Code_Marketing_Airline", ""), 10),
        _safe_str(row.get("Flight_Number_Marketing_Airline", ""), 10),
        _safe_str(row.get("IATA_Code_Operating_Airline", ""), 10),
        _safe_str(row.get("Tail_Number", ""), 20),
        _safe_str(row.get("Flight_Number_Operating_Airline", ""), 10),
        _safe_str(row.get("Origin", ""), 10),
        _safe_str(row.get("OriginCityName", ""), 100),
        _safe_str(row.get("OriginState", ""), 5),
        _safe_str(row.get("Dest", ""), 10),
        _safe_str(row.get("DestCityName", ""), 100),
        _safe_str(row.get("DestState", ""), 5),
        _safe_str(row.get("CRSDepTime", ""), 10),
        _safe_str(row.get("DepTime", ""), 10),
        _safe_float(row.get("DepDelay", "")),
        _safe_float(row.get("DepDelayMinutes", "")),
        _safe_float(row.get("DepDel15", "")),
        _safe_str(row.get("CRSArrTime", ""), 10),
        _safe_str(row.get("ArrTime", ""), 10),
        _safe_float(row.get("ArrDelay", "")),
        _safe_float(row.get("ArrDelayMinutes", "")),
        _safe_float(row.get("ArrDel15", "")),
        _safe_float(row.get("Cancelled", "")),
        _safe_str(row.get("CancellationCode", ""), 5),
        _safe_float(row.get("Diverted", "")),
        _safe_float(row.get("CRSElapsedTime", "")),
        _safe_float(row.get("ActualElapsedTime", "")),
        _safe_float(row.get("AirTime", "")),
        _safe_float(row.get("Distance", "")),
        _safe_int(row.get("DistanceGroup", "")),
        _safe_float(row.get("CarrierDelay", "")),
        _safe_float(row.get("WeatherDelay", "")),
        _safe_float(row.get("NASDelay", "")),
        _safe_float(row.get("SecurityDelay", "")),
        _safe_float(row.get("LateAircraftDelay", "")),
    )


def _extract_delay_row(row: Dict[str, str]) -> Tuple:
    """Extract columns from a delay causes row."""
    return (
        _safe_int(row.get("year", "")),
        _safe_int(row.get(" month", row.get("month", ""))),
        _safe_str(row.get("carrier", ""), 10),
        _safe_str(row.get("carrier_name", ""), 100),
        _safe_str(row.get("airport", ""), 10),
        _safe_str(row.get("airport_name", ""), 200),
        _safe_float(row.get("arr_flights", "")),
        _safe_float(row.get("arr_del15", "")),
        _safe_float(row.get("carrier_ct", "")),
        _safe_float(row.get(" weather_ct", row.get("weather_ct", ""))),
        _safe_float(row.get("nas_ct", "")),
        _safe_float(row.get("security_ct", "")),
        _safe_float(row.get("late_aircraft_ct", "")),
        _safe_float(row.get("arr_cancelled", "")),
        _safe_float(row.get("arr_diverted", "")),
        _safe_float(row.get(" arr_delay", row.get("arr_delay", ""))),
        _safe_float(row.get(" carrier_delay", row.get("carrier_delay", ""))),
        _safe_float(row.get("weather_delay", "")),
        _safe_float(row.get("nas_delay", "")),
        _safe_float(row.get("security_delay", "")),
        _safe_float(row.get("late_aircraft_delay", "")),
    )


def load_delay_causes(conn, data_dir: Path, dry_run: bool) -> int:
    """Load airline delay causes CSV."""
    zips = sorted(data_dir.glob("*delay_causes*.zip"))
    if not zips:
        print("No delay causes zip found, skipping")
        return 0

    total = 0
    for zip_path in zips:
        with zipfile.ZipFile(zip_path) as zf:
            csv_names = [n for n in zf.namelist() if n.endswith(".csv")]
            if not csv_names:
                continue
            with zf.open(csv_names[0]) as f:
                reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8", errors="ignore"))
                batch: List[Tuple] = []
                placeholders = ", ".join(["?"] * len(DELAY_CAUSES_COLS))
                col_names = ", ".join(f"[{c}]" for c, _ in DELAY_CAUSES_COLS)
                insert_sql = f"INSERT INTO airline_delay_causes ({col_names}) VALUES ({placeholders})"

                for row in reader:
                    batch.append(_extract_delay_row(row))
                    if len(batch) >= 1000:
                        if not dry_run:
                            cursor = conn.cursor()
                            cursor.executemany(insert_sql, batch)
                            conn.commit()
                        total += len(batch)
                        batch.clear()

                if batch:
                    if not dry_run:
                        cursor = conn.cursor()
                        cursor.executemany(insert_sql, batch)
                        conn.commit()
                    total += len(batch)

    print(f"  airline_delay_causes: {total} rows {'(dry-run)' if dry_run else 'inserted'}")
    return total


def _upload_csv_to_onelake(csv_path: Path, workspace_id: str, lakehouse_id: str) -> str:
    """Upload a CSV file to OneLake (Fabric Lakehouse) for COPY INTO.

    Uses the OneLake DFS API (append/flush pattern) to upload the CSV
    to the lakehouse Files directory. Returns the abfss:// URL.
    """
    import requests

    credential = DefaultAzureCredential()
    token = credential.get_token("https://storage.azure.com/.default").token
    headers = {"Authorization": f"Bearer {token}"}
    dfs_base = "https://onelake.dfs.fabric.microsoft.com"
    dir_path = f"{workspace_id}/{lakehouse_id}/Files/bts_staging"
    blob_path = f"{dir_path}/{csv_path.name}"
    chunk_size = 4 * 1024 * 1024  # 4MB

    # Create directory
    requests.put(f"{dfs_base}/{dir_path}?resource=directory", headers=headers)

    # Create file
    resp = requests.put(f"{dfs_base}/{blob_path}?resource=file", headers=headers)
    if resp.status_code not in (200, 201):
        raise RuntimeError(f"Failed to create file: {resp.status_code} {resp.text[:200]}")

    # Append in chunks
    file_size = csv_path.stat().st_size
    print(f"    Uploading {csv_path.name} ({file_size / (1024*1024):.1f}MB) to OneLake...")
    offset = 0
    with csv_path.open("rb") as f:
        while True:
            data = f.read(chunk_size)
            if not data:
                break
            resp = requests.patch(
                f"{dfs_base}/{blob_path}?action=append&position={offset}",
                headers={**headers, "Content-Type": "application/octet-stream"},
                data=data,
            )
            if resp.status_code not in (200, 202):
                raise RuntimeError(f"Append failed at offset {offset}: {resp.status_code}")
            offset += len(data)

    # Flush
    resp = requests.patch(f"{dfs_base}/{blob_path}?action=flush&position={offset}", headers=headers)
    if resp.status_code != 200:
        raise RuntimeError(f"Flush failed: {resp.status_code} {resp.text[:200]}")

    url = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Files/bts_staging/{csv_path.name}"
    print(f"    Upload complete: {url}")
    return url


def _extract_ontime_csv(data_dir: Path, output_path: Path) -> int:
    """Extract BTS on-time data from zips into a single CSV for COPY INTO."""
    zips = sorted(data_dir.glob("On_Time_*.zip"))
    if not zips:
        return 0

    col_names = [c for c, _ in ONTIME_COLS]
    row_count = 0

    with output_path.open("w", newline="", encoding="utf-8") as out_f:
        writer = csv.writer(out_f)
        writer.writerow(col_names)

        for zip_path in zips:
            print(f"  Extracting {zip_path.name}...")
            file_count = 0
            for row in _iter_ontime_from_zip(zip_path):
                values = _extract_ontime_row(row)
                # Convert None to empty string for CSV
                writer.writerow(["" if v is None else v for v in values])
                file_count += 1
                if file_count % 100000 == 0:
                    print(f"    {file_count:,} rows...")
            row_count += file_count
            print(f"    {zip_path.name}: {file_count:,} rows")

    print(f"  Total extracted: {row_count:,} rows -> {output_path}")
    return row_count


def load_ontime(conn, data_dir: Path, dry_run: bool, batch_size: int = 5000) -> int:
    """Load BTS on-time performance data using COPY INTO via blob staging."""
    import tempfile

    zips = sorted(data_dir.glob("On_Time_*.zip"))
    if not zips:
        print("No on-time zip files found, skipping")
        return 0

    # Step 1: Extract to staging CSV
    staging_csv = Path(tempfile.mktemp(suffix="_bts_ontime.csv"))
    row_count = _extract_ontime_csv(data_dir, staging_csv)

    if dry_run:
        print(f"  bts_ontime_reporting: {row_count:,} rows (dry-run)")
        staging_csv.unlink(missing_ok=True)
        return row_count

    # Step 2: Upload to OneLake and COPY INTO
    workspace_id = os.getenv("FABRIC_WORKSPACE_ID", "cfbb82a5-799e-421b-b2cc-8a164b17a849")
    lakehouse_id = os.getenv("FABRIC_LAKEHOUSE_ID", "de9e3323-0952-479e-a6ea-0de93c2d4f95")
    try:
        onelake_url = _upload_csv_to_onelake(staging_csv, workspace_id, lakehouse_id)
        print("  Running COPY INTO from OneLake...")
        cursor = conn.cursor()
        copy_sql = f"""
            COPY INTO bts_ontime_reporting
            FROM '{onelake_url}'
            WITH (
                FILE_TYPE = 'CSV',
                FIRSTROW = 2,
                FIELDTERMINATOR = ',',
                ROWTERMINATOR = '0x0A'
            )
        """
        cursor.execute(copy_sql)
        conn.commit()
        print(f"  COPY INTO completed for {row_count:,} rows")
        staging_csv.unlink(missing_ok=True)
        return row_count
    except Exception as copy_err:
        print(f"  COPY INTO failed: {copy_err}")
        print("  Falling back to batch INSERT (this will be slow)...")

    # Step 3: Fallback - batch INSERT from the staging CSV
    placeholders = ", ".join(["?"] * len(ONTIME_COLS))
    col_names = ", ".join(f"[{c}]" for c, _ in ONTIME_COLS)
    insert_sql = f"INSERT INTO bts_ontime_reporting ({col_names}) VALUES ({placeholders})"

    total = 0
    for zip_path in zips:
        print(f"  Processing {zip_path.name}...")
        batch: List[Tuple] = []
        file_count = 0

        for row in _iter_ontime_from_zip(zip_path):
            batch.append(_extract_ontime_row(row))
            if len(batch) >= batch_size:
                cursor = conn.cursor()
                cursor.executemany(insert_sql, batch)
                conn.commit()
                file_count += len(batch)
                total += len(batch)
                batch.clear()
                if file_count % 50000 == 0:
                    print(f"    {file_count:,} rows processed...")

        if batch:
            cursor = conn.cursor()
            cursor.executemany(insert_sql, batch)
            conn.commit()
            file_count += len(batch)
            total += len(batch)

        print(f"    {zip_path.name}: {file_count:,} rows inserted")

    staging_csv.unlink(missing_ok=True)
    print(f"  bts_ontime_reporting total: {total:,} rows inserted")
    return total


def main() -> None:
    parser = argparse.ArgumentParser(description="Load BTS data into Fabric SQL Warehouse")
    parser.add_argument("--warehouse-server", default=DEFAULT_SERVER, help="Fabric SQL Warehouse server")
    parser.add_argument("--database", default=DEFAULT_DATABASE, help="Warehouse database name")
    parser.add_argument("--data-dir", type=Path, default=None, help="Directory containing BTS zip files")
    parser.add_argument("--batch-size", type=int, default=5000, help="INSERT batch size")
    parser.add_argument("--drop-first", action="store_true", help="Drop and recreate tables")
    parser.add_argument("--dry-run", action="store_true", help="Parse data without uploading")
    parser.add_argument("--delay-only", action="store_true", help="Only load delay causes (small dataset)")
    parser.add_argument("--ontime-only", action="store_true", help="Only load on-time data")
    args = parser.parse_args()

    data_dir = args.data_dir or _find_latest_data_dir()
    print(f"Data directory: {data_dir}")
    print(f"Warehouse: {args.warehouse_server}")
    print(f"Database: {args.database}")

    if args.dry_run:
        print("DRY RUN — will parse data but not connect to warehouse")
        conn = None
    else:
        print("Connecting to Fabric SQL Warehouse...")
        conn = connect(args.warehouse_server, args.database)
        print("Connected")
        _create_tables(conn, drop_first=args.drop_first)

    delay_count = 0
    ontime_count = 0

    if not args.ontime_only:
        print("Loading airline delay causes...")
        delay_count = load_delay_causes(conn, data_dir, args.dry_run)

    if not args.delay_only:
        print("Loading BTS on-time performance...")
        ontime_count = load_ontime(conn, data_dir, args.dry_run, args.batch_size)

    print(f"\nDone{'  (dry-run)' if args.dry_run else ''}.")
    print(f"  airline_delay_causes: {delay_count:,} rows")
    print(f"  bts_ontime_reporting: {ontime_count:,} rows")

    if conn:
        conn.close()


if __name__ == "__main__":
    main()
