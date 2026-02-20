#!/usr/bin/env python3
"""
Create Kusto tables and ingest demo datasets (OpenSky, hazards, graph edges).
"""

from __future__ import annotations

import argparse
import csv
import gzip
import json
import shutil
from pathlib import Path

from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.data_format import DataFormat
from azure.kusto.ingest import IngestionProperties, ManagedStreamingIngestClient


def create_opensky_csv(input_json: Path, output_csv: Path) -> int:
    with input_json.open("r", encoding="utf-8") as f:
        payload = json.load(f)
    states = payload.get("states") or []
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    with output_csv.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "icao24",
                "callsign",
                "origin_country",
                "time_position",
                "last_contact",
                "longitude",
                "latitude",
                "baro_altitude",
                "on_ground",
                "velocity",
                "true_track",
                "vertical_rate",
                "geo_altitude",
                "squawk",
                "position_source",
            ]
        )
        for s in states:
            writer.writerow(
                [
                    s[0] or "",
                    (s[1] or "").strip(),
                    s[2] or "",
                    s[3] or 0,
                    s[4] or 0,
                    s[5] or 0.0,
                    s[6] or 0.0,
                    s[7] or 0.0,
                    bool(s[8] or False),
                    s[9] or 0.0,
                    s[10] or 0.0,
                    s[11] or 0.0,
                    s[13] or 0.0,
                    s[14] or "",
                    s[16] or 0,
                ]
            )
    return len(states)


def gunzip_to_csv(input_gz: Path, output_csv: Path) -> None:
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(input_gz, "rb") as src, output_csv.open("wb") as dst:
        shutil.copyfileobj(src, dst)


def gunzip_to_txt(input_gz: Path, output_txt: Path) -> None:
    output_txt.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(input_gz, "rt", encoding="utf-8", errors="ignore") as src, output_txt.open("w", encoding="utf-8") as dst:
        _ = src.readline()  # drop header
        for line in src:
            line = line.rstrip("\n")
            if not line:
                continue
            dst.write(line + "\n")


def create_or_replace_tables(client: KustoClient, database: str) -> None:
    commands = [
        ".create-merge table opensky_states (icao24:string, callsign:string, origin_country:string, time_position:long, last_contact:long, longitude:real, latitude:real, baro_altitude:real, on_ground:bool, velocity:real, true_track:real, vertical_rate:real, geo_altitude:real, squawk:string, position_source:int)",
        ".create-merge table hazards_airsigmets (raw_text:string, valid_time_from:datetime, valid_time_to:datetime, points:string, min_ft_msl:string, max_ft_msl:string, movement_dir_degrees:string, movement_speed_kt:string, hazard:string, severity:string, airsigmet_type:string)",
        ".create-merge table hazards_gairmets (receipt_time:datetime, issue_time:datetime, expire_time:datetime, product:string, tag:string, issue_to_valid_hours:string, valid_time:datetime, hazard:string, geometry_type:string, due_to:string, points:string)",
        ".create-merge table hazards_aireps_raw (raw_line:string)",
        ".create-merge table ops_graph_edges (src_type:string, src_id:string, edge_type:string, dst_type:string, dst_id:string)",
    ]
    for cmd in commands:
        client.execute_mgmt(database, cmd)


def ingest_csv(client: ManagedStreamingIngestClient, database: str, table: str, csv_path: Path) -> None:
    props = IngestionProperties(
        database=database,
        table=table,
        data_format=DataFormat.CSV,
        ignore_first_record=True,
        flush_immediately=True,
    )
    client.ingest_from_file(str(csv_path), ingestion_properties=props)


def ingest_txt(client: ManagedStreamingIngestClient, database: str, table: str, txt_path: Path) -> None:
    props = IngestionProperties(
        database=database,
        table=table,
        data_format=DataFormat.TXT,
        flush_immediately=True,
    )
    client.ingest_from_file(str(txt_path), ingestion_properties=props)


def count_rows(client: KustoClient, database: str, table: str) -> int:
    resp = client.execute(database, f"{table} | count")
    rows = list(resp.primary_results[0])
    if not rows:
        return 0
    return int(rows[0]["Count"])


def main() -> None:
    parser = argparse.ArgumentParser(description="Push demo datasets to Kusto")
    parser.add_argument("--cluster-uri", required=True, help="Engine URI, e.g. https://<cluster>.<region>.kusto.windows.net")
    parser.add_argument("--ingest-uri", required=True, help="Ingest URI, e.g. https://ingest-<cluster>.<region>.kusto.windows.net")
    parser.add_argument("--database", default="aviationdemo")
    parser.add_argument("--opensky-json", required=True)
    parser.add_argument("--airsigmets-gz", required=True)
    parser.add_argument("--gairmets-gz", required=True)
    parser.add_argument("--aireps-gz", required=True)
    parser.add_argument("--graph-edges-csv", required=True)
    parser.add_argument("--tmp-dir", default="/tmp/kusto-load")
    args = parser.parse_args()

    tmp_dir = Path(args.tmp_dir)
    tmp_dir.mkdir(parents=True, exist_ok=True)

    opensky_csv = tmp_dir / "opensky_states.csv"
    airsig_csv = tmp_dir / "hazards_airsigmets.csv"
    gair_csv = tmp_dir / "hazards_gairmets.csv"
    airep_txt = tmp_dir / "hazards_aireps.txt"

    opensky_count = create_opensky_csv(Path(args.opensky_json), opensky_csv)
    gunzip_to_csv(Path(args.airsigmets_gz), airsig_csv)
    gunzip_to_csv(Path(args.gairmets_gz), gair_csv)
    gunzip_to_txt(Path(args.aireps_gz), airep_txt)

    engine_kcsb = KustoConnectionStringBuilder.with_az_cli_authentication(args.cluster_uri)
    ingest_kcsb = KustoConnectionStringBuilder.with_az_cli_authentication(args.ingest_uri)

    query_client = KustoClient(engine_kcsb)
    ingest_client = ManagedStreamingIngestClient(engine_kcsb, ingest_kcsb)

    create_or_replace_tables(query_client, args.database)
    if count_rows(query_client, args.database, "opensky_states") == 0:
        ingest_csv(ingest_client, args.database, "opensky_states", opensky_csv)
    if count_rows(query_client, args.database, "hazards_airsigmets") == 0:
        ingest_csv(ingest_client, args.database, "hazards_airsigmets", airsig_csv)
    if count_rows(query_client, args.database, "hazards_gairmets") == 0:
        ingest_csv(ingest_client, args.database, "hazards_gairmets", gair_csv)
    if count_rows(query_client, args.database, "hazards_aireps_raw") == 0:
        ingest_txt(ingest_client, args.database, "hazards_aireps_raw", airep_txt)
    if count_rows(query_client, args.database, "ops_graph_edges") == 0:
        ingest_csv(ingest_client, args.database, "ops_graph_edges", Path(args.graph_edges_csv))

    print("Kusto ingest requested (managed streaming).")
    print(f"  opensky_states source rows: {opensky_count}")
    for table in ["opensky_states", "hazards_airsigmets", "hazards_gairmets", "hazards_aireps_raw", "ops_graph_edges"]:
        try:
            c = count_rows(query_client, args.database, table)
            print(f"  {table}: {c}")
        except Exception as exc:
            print(f"  {table}: count_failed ({exc})")


if __name__ == "__main__":
    main()
