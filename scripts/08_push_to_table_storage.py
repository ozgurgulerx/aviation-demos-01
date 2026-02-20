#!/usr/bin/env python3
"""
Push demo NoSQL/graph-like datasets into Azure Table Storage.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List

from azure.core.credentials import AzureNamedKeyCredential
from azure.identity import DefaultAzureCredential
from azure.data.tables import TableServiceClient


def clean_key(value: str, fallback: str) -> str:
    raw = (value or "").strip()
    if not raw:
        raw = fallback
    cleaned = re.sub(r"[^A-Za-z0-9_\-=]", "_", raw)
    return cleaned[:1024]


def batched(items: List[Dict], size: int) -> Iterable[List[Dict]]:
    for idx in range(0, len(items), size):
        yield items[idx : idx + size]


def load_notam_entities(path: Path) -> List[Dict]:
    entities: List[Dict] = []
    with path.open("r", encoding="utf-8") as handle:
        for idx, line in enumerate(handle, start=1):
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            facility = clean_key(str(obj.get("facilityDesignator", "UNK")), "UNK")
            notam_no = clean_key(str(obj.get("notamNumber", f"row_{idx}")), f"row_{idx}")
            entities.append(
                {
                    "PartitionKey": facility,
                    "RowKey": notam_no,
                    "facilityDesignator": str(obj.get("facilityDesignator", "")),
                    "notamNumber": str(obj.get("notamNumber", "")),
                    "airportName": str(obj.get("airportName", "")),
                    "startDate": str(obj.get("startDate", "")),
                    "endDate": str(obj.get("endDate", "")),
                    "source": str(obj.get("source", "")),
                    "sourceType": str(obj.get("sourceType", "")),
                    "issueDate": str(obj.get("issueDate", "")),
                    "icaoMessage": str(obj.get("icaoMessage", ""))[:30000],
                }
            )
    return entities


def load_opensky_entities(path: Path) -> List[Dict]:
    entities: List[Dict] = []
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    states = payload.get("states") or []
    for idx, state in enumerate(states, start=1):
        icao24 = clean_key(str(state[0] or f"icao_{idx}"), f"icao_{idx}")
        last_contact = str(state[4] or "")
        row_key = clean_key(f"{icao24}_{last_contact}", f"{icao24}_{idx}")
        entities.append(
            {
                "PartitionKey": "opensky",
                "RowKey": row_key,
                "icao24": str(state[0] or ""),
                "callsign": str(state[1] or "").strip(),
                "origin_country": str(state[2] or ""),
                "time_position": int(state[3] or 0),
                "last_contact": int(state[4] or 0),
                "longitude": float(state[5] or 0.0),
                "latitude": float(state[6] or 0.0),
                "baro_altitude": float(state[7] or 0.0),
                "on_ground": bool(state[8] or False),
                "velocity": float(state[9] or 0.0),
                "true_track": float(state[10] or 0.0),
                "vertical_rate": float(state[11] or 0.0),
                "geo_altitude": float(state[13] or 0.0),
                "squawk": str(state[14] or ""),
                "position_source": int(state[16] or 0),
            }
        )
    return entities


def load_graph_edge_entities(path: Path) -> List[Dict]:
    entities: List[Dict] = []
    with path.open("r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for idx, row in enumerate(reader, start=1):
            edge_type = clean_key(str(row.get("edge_type", "EDGE")), "EDGE")
            src = clean_key(str(row.get("src_id", f"src_{idx}")), f"src_{idx}")
            dst = clean_key(str(row.get("dst_id", f"dst_{idx}")), f"dst_{idx}")
            row_key = clean_key(f"{src}_{edge_type}_{dst}_{idx}", f"edge_{idx}")
            entities.append(
                {
                    "PartitionKey": edge_type,
                    "RowKey": row_key,
                    "src_type": str(row.get("src_type", "")),
                    "src_id": str(row.get("src_id", "")),
                    "edge_type": str(row.get("edge_type", "")),
                    "dst_type": str(row.get("dst_type", "")),
                    "dst_id": str(row.get("dst_id", "")),
                }
            )
    return entities


def upsert_entities(table_client, entities: List[Dict], batch_size: int) -> None:
    grouped: Dict[str, List[Dict]] = defaultdict(list)
    for entity in entities:
        grouped[str(entity["PartitionKey"])].append(entity)

    for _, pk_entities in grouped.items():
        for chunk in batched(pk_entities, batch_size):
            ops = [("upsert", entity, {"mode": "replace"}) for entity in chunk]
            table_client.submit_transaction(ops)


def main() -> None:
    parser = argparse.ArgumentParser(description="Push datasets to Azure Table Storage")
    parser.add_argument("--account-name", required=True, help="Storage account name")
    parser.add_argument("--account-key", default="", help="Storage account key (optional when using AAD)")
    parser.add_argument("--notam-jsonl", required=True, help="Path to NOTAM JSONL file")
    parser.add_argument("--opensky-json", required=True, help="Path to OpenSky states JSON file")
    parser.add_argument("--graph-edges-csv", required=True, help="Path to graph edges CSV file")
    parser.add_argument("--batch-size", type=int, default=100, help="Transaction batch size (<=100)")
    args = parser.parse_args()

    if args.batch_size < 1 or args.batch_size > 100:
        raise ValueError("--batch-size must be between 1 and 100")

    endpoint = f"https://{args.account_name}.table.core.windows.net"
    if args.account_key.strip():
        credential = AzureNamedKeyCredential(args.account_name, args.account_key.strip())
    else:
        credential = DefaultAzureCredential()
    service = TableServiceClient(endpoint=endpoint, credential=credential)

    notam_table = service.create_table_if_not_exists("notamdocs")
    opensky_table = service.create_table_if_not_exists("openskystates")
    graph_table = service.create_table_if_not_exists("opsgraphedges")

    notam_entities = load_notam_entities(Path(args.notam_jsonl))
    opensky_entities = load_opensky_entities(Path(args.opensky_json))
    graph_entities = load_graph_edge_entities(Path(args.graph_edges_csv))

    upsert_entities(notam_table, notam_entities, args.batch_size)
    upsert_entities(opensky_table, opensky_entities, args.batch_size)
    upsert_entities(graph_table, graph_entities, args.batch_size)

    print("Table storage upload complete")
    print(f"  notamdocs: {len(notam_entities)}")
    print(f"  openskystates: {len(opensky_entities)}")
    print(f"  opsgraphedges: {len(graph_entities)}")


if __name__ == "__main__":
    main()
