#!/usr/bin/env python3
"""
Bulk load real NOTAMs from PilotWeb JSONL files into Cosmos DB (NoSQL).

Replaces the 25 synthetic NOTAMs from 13_seed_cosmos_notams.py with ~488
real NOTAMs parsed from FAA PilotWeb data in data/h-notam_recent/.

Usage:
    python scripts/15_bulk_load_cosmos_notams.py
    python scripts/15_bulk_load_cosmos_notams.py --endpoint https://cosmos-aviation-rag.documents.azure.com:443/ --key <key>
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Dict, Iterable, List, Optional

try:
    from azure.cosmos import CosmosClient
except ImportError:
    print("azure-cosmos SDK not installed. Run: pip install azure-cosmos>=4.7.0", file=sys.stderr)
    sys.exit(1)


ROOT = Path(__file__).resolve().parents[1]

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
    fac = fac.strip().upper()
    if fac in _FAA_TO_ICAO:
        return _FAA_TO_ICAO[fac]
    if len(fac) == 4 and fac[0] in ("K", "P", "T", "L", "E"):
        return fac
    if len(fac) == 3 and fac.isalpha():
        return f"K{fac}"
    return fac


def _classify_notam(content: str) -> str:
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
    if any(w in c for w in ("AD ", "AERODROME", "HOURS OF OPS")):
        return "aerodrome"
    return "general"


def _severity_from_content(content: str) -> str:
    c = content.upper()
    if any(w in c for w in ("CLSD", "CLOSED", "UNSERVICEABLE", "UNUSABLE", "OUT OF SERVICE")):
        return "HIGH"
    if any(w in c for w in ("RESTRICTED", "REDUCED", "CHANGED", "LIMITED", "WIP")):
        return "MEDIUM"
    return "LOW"


def _extract_runway(content: str) -> Optional[str]:
    m = _RWY_RE.search(content)
    return m.group(1) if m else None


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


def transform_notam(obj: dict) -> Optional[dict]:
    """Transform a FAA PilotWeb NOTAM into a Cosmos DB document."""
    notam_no = str(obj.get("notamNumber", "")).strip()
    fac = str(obj.get("facilityDesignator", "")).strip()
    if not notam_no or not fac:
        return None

    icao = _faa_to_icao(fac)
    content = str(obj.get("icaoMessage", ""))
    # Build a stable document ID
    safe_notam = notam_no.replace("/", "-").replace(" ", "_")
    doc_id = f"NOTAM-{safe_notam}-{icao}"

    return {
        "id": doc_id,
        "notam_number": notam_no,
        "type": "NOTAM",
        "icao": icao,
        "iata": fac if len(fac) == 3 else "",
        "airport_name": str(obj.get("airportName", "")),
        "content": content,
        "affected_runway": _extract_runway(content),
        "category": _classify_notam(content),
        "severity": _severity_from_content(content),
        "status": "active",
        "source": "FAA",
        "issue_date": str(obj.get("issueDate", "")),
        "start_date": str(obj.get("startDate", "")),
        "end_date": str(obj.get("endDate", "")),
    }


def main():
    parser = argparse.ArgumentParser(description="Bulk load real NOTAMs into Cosmos DB")
    parser.add_argument("--endpoint", default=os.getenv("AZURE_COSMOS_ENDPOINT", ""), help="Cosmos DB endpoint")
    parser.add_argument("--key", default=os.getenv("AZURE_COSMOS_KEY", ""), help="Cosmos DB primary key")
    parser.add_argument("--database", default=os.getenv("AZURE_COSMOS_DATABASE", "aviationrag"), help="Database name")
    parser.add_argument("--container", default=os.getenv("AZURE_COSMOS_CONTAINER", "notams"), help="Container name")
    parser.add_argument("--dry-run", action="store_true", help="Parse and validate but do not upload")
    args = parser.parse_args()

    if not args.endpoint and not args.dry_run:
        print("Error: --endpoint or AZURE_COSMOS_ENDPOINT is required", file=sys.stderr)
        sys.exit(1)

    # Parse all NOTAM JSONL files
    notam_dir = ROOT / "data" / "h-notam_recent"
    jsonl_files = sorted(notam_dir.glob("*/search_location_*.jsonl"))
    if not jsonl_files:
        print("No NOTAM JSONL files found in data/h-notam_recent/", file=sys.stderr)
        sys.exit(1)

    print(f"Found {len(jsonl_files)} NOTAM JSONL files")
    seen_ids: set = set()
    documents: List[dict] = []

    for jf in jsonl_files:
        file_count = 0
        for obj in iter_jsonl(jf):
            doc = transform_notam(obj)
            if doc and doc["id"] not in seen_ids:
                seen_ids.add(doc["id"])
                documents.append(doc)
                file_count += 1
        print(f"  {jf.name}: {file_count} unique NOTAMs")

    print(f"\nTotal unique NOTAMs: {len(documents)}")

    if args.dry_run:
        print("Dry run — no upload. Sample document:")
        if documents:
            print(json.dumps(documents[0], indent=2))
        return

    # Connect to Cosmos DB
    if args.key:
        client = CosmosClient(args.endpoint, credential=args.key)
    else:
        try:
            from azure.identity import DefaultAzureCredential
            print("No key provided — using DefaultAzureCredential (AAD auth)")
            client = CosmosClient(args.endpoint, credential=DefaultAzureCredential())
        except ImportError:
            print("Error: --key or AZURE_COSMOS_KEY required (azure-identity not installed)", file=sys.stderr)
            sys.exit(1)

    database = client.get_database_client(args.database)
    container = database.get_container_client(args.container)

    print(f"\nUpserting {len(documents)} NOTAMs into {args.endpoint} / {args.database} / {args.container}")

    upserted = 0
    failed = 0
    for doc in documents:
        try:
            container.upsert_item(doc)
            upserted += 1
            if upserted % 50 == 0:
                print(f"  Progress: {upserted}/{len(documents)}")
        except Exception as exc:
            failed += 1
            print(f"  FAILED {doc['id']}: {exc}", file=sys.stderr)

    print(f"\nDone: {upserted} upserted, {failed} failed out of {len(documents)} documents.")


if __name__ == "__main__":
    main()
