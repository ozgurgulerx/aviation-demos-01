#!/usr/bin/env python3
"""
Seed Azure Cosmos DB (NoSQL) with sample NOTAM documents.

Usage:
    python scripts/13_seed_cosmos_notams.py
    python scripts/13_seed_cosmos_notams.py --endpoint https://cosmos-aviation-rag.documents.azure.com:443/ --key <key>
"""

import argparse
import os
import sys

try:
    from azure.cosmos import CosmosClient, exceptions as cosmos_exceptions
except ImportError:
    print("azure-cosmos SDK not installed. Run: pip install azure-cosmos>=4.7.0", file=sys.stderr)
    sys.exit(1)


SAMPLE_NOTAMS = [
    # KJFK
    {"id": "NOTAM-A0001-26-KJFK", "notam_number": "A0001/26", "type": "NOTAM", "icao": "KJFK", "iata": "JFK", "airport_name": "John F Kennedy Intl", "effective_from": "2026-02-20T00:00:00Z", "effective_to": "2026-03-20T00:00:00Z", "severity": "HIGH", "content": "RWY 13R/31L CLSD FOR MAINT 0700-1500 DAILY", "status": "active", "source": "FAA", "category": "runway"},
    {"id": "NOTAM-A0002-26-KJFK", "notam_number": "A0002/26", "type": "NOTAM", "icao": "KJFK", "iata": "JFK", "airport_name": "John F Kennedy Intl", "effective_from": "2026-02-18T00:00:00Z", "effective_to": "2026-04-01T00:00:00Z", "severity": "MEDIUM", "content": "TWY B BTN TWY J AND TWY K CLSD", "status": "active", "source": "FAA", "category": "taxiway"},
    {"id": "NOTAM-A0003-26-KJFK", "notam_number": "A0003/26", "type": "NOTAM", "icao": "KJFK", "iata": "JFK", "airport_name": "John F Kennedy Intl", "effective_from": "2026-02-15T00:00:00Z", "effective_to": "2026-03-15T00:00:00Z", "severity": "LOW", "content": "ILS RWY 04L GP UNUSABLE", "status": "active", "source": "FAA", "category": "navaid"},
    # KLGA
    {"id": "NOTAM-A0004-26-KLGA", "notam_number": "A0004/26", "type": "NOTAM", "icao": "KLGA", "iata": "LGA", "airport_name": "LaGuardia", "effective_from": "2026-02-22T06:00:00Z", "effective_to": "2026-02-22T14:00:00Z", "severity": "HIGH", "content": "RWY 04/22 CLSD FOR SNOW REMOVAL", "status": "active", "source": "FAA", "category": "runway"},
    {"id": "NOTAM-A0005-26-KLGA", "notam_number": "A0005/26", "type": "NOTAM", "icao": "KLGA", "iata": "LGA", "airport_name": "LaGuardia", "effective_from": "2026-02-10T00:00:00Z", "effective_to": "2026-05-10T00:00:00Z", "severity": "MEDIUM", "content": "TERMINAL B CONSTRUCTION - APRON RESTRICTIONS IN EFFECT", "status": "active", "source": "FAA", "category": "apron"},
    # KEWR
    {"id": "NOTAM-A0006-26-KEWR", "notam_number": "A0006/26", "type": "NOTAM", "icao": "KEWR", "iata": "EWR", "airport_name": "Newark Liberty Intl", "effective_from": "2026-02-19T00:00:00Z", "effective_to": "2026-03-19T00:00:00Z", "severity": "MEDIUM", "content": "RWY 11/29 REDUCED LENGTH AVBL 9000FT", "status": "active", "source": "FAA", "category": "runway"},
    {"id": "NOTAM-A0007-26-KEWR", "notam_number": "A0007/26", "type": "NOTAM", "icao": "KEWR", "iata": "EWR", "airport_name": "Newark Liberty Intl", "effective_from": "2026-02-21T00:00:00Z", "effective_to": "2026-02-28T00:00:00Z", "severity": "LOW", "content": "VASI RWY 22L UNUSABLE", "status": "active", "source": "FAA", "category": "navaid"},
    # LTFM
    {"id": "NOTAM-A0008-26-LTFM", "notam_number": "A0008/26", "type": "NOTAM", "icao": "LTFM", "iata": "IST", "airport_name": "Istanbul Airport", "effective_from": "2026-02-20T00:00:00Z", "effective_to": "2026-03-20T00:00:00Z", "severity": "HIGH", "content": "RWY 16R/34L CLSD FOR RESURFACING 2200-0600 DAILY", "status": "active", "source": "DGCA", "category": "runway"},
    {"id": "NOTAM-A0009-26-LTFM", "notam_number": "A0009/26", "type": "NOTAM", "icao": "LTFM", "iata": "IST", "airport_name": "Istanbul Airport", "effective_from": "2026-02-15T00:00:00Z", "effective_to": "2026-04-15T00:00:00Z", "severity": "MEDIUM", "content": "TWY S5 BTN TWY S AND APRON 3 CLSD", "status": "active", "source": "DGCA", "category": "taxiway"},
    {"id": "NOTAM-A0010-26-LTFM", "notam_number": "A0010/26", "type": "NOTAM", "icao": "LTFM", "iata": "IST", "airport_name": "Istanbul Airport", "effective_from": "2026-02-22T00:00:00Z", "effective_to": "2026-03-01T00:00:00Z", "severity": "LOW", "content": "CRANE ERECTED 410230N 0285850E MAX HEIGHT 150FT AGL", "status": "active", "source": "DGCA", "category": "obstacle"},
    # LTBA
    {"id": "NOTAM-A0011-26-LTBA", "notam_number": "A0011/26", "type": "NOTAM", "icao": "LTBA", "iata": "SAW", "airport_name": "Sabiha Gokcen Intl", "effective_from": "2026-02-18T00:00:00Z", "effective_to": "2026-03-18T00:00:00Z", "severity": "MEDIUM", "content": "RWY 06/24 PCN REDUCED TO 60/F/B/W/T", "status": "active", "source": "DGCA", "category": "runway"},
    {"id": "NOTAM-A0012-26-LTBA", "notam_number": "A0012/26", "type": "NOTAM", "icao": "LTBA", "iata": "SAW", "airport_name": "Sabiha Gokcen Intl", "effective_from": "2026-02-21T00:00:00Z", "effective_to": "2026-02-25T00:00:00Z", "severity": "HIGH", "content": "AD HOURS OF OPS CHANGED TO 0500-2300 UTC", "status": "active", "source": "DGCA", "category": "aerodrome"},
    # LTFJ
    {"id": "NOTAM-A0013-26-LTFJ", "notam_number": "A0013/26", "type": "NOTAM", "icao": "LTFJ", "iata": "ESB", "airport_name": "Esenboga Intl", "effective_from": "2026-02-19T00:00:00Z", "effective_to": "2026-03-05T00:00:00Z", "severity": "MEDIUM", "content": "ILS RWY 03L LOC COVERAGE RESTRICTED BEYOND 25NM", "status": "active", "source": "DGCA", "category": "navaid"},
    {"id": "NOTAM-A0014-26-LTFJ", "notam_number": "A0014/26", "type": "NOTAM", "icao": "LTFJ", "iata": "ESB", "airport_name": "Esenboga Intl", "effective_from": "2026-02-20T00:00:00Z", "effective_to": "2026-04-20T00:00:00Z", "severity": "LOW", "content": "PAPI RWY 21R UNUSABLE", "status": "active", "source": "DGCA", "category": "navaid"},
    {"id": "NOTAM-A0015-26-LTFJ", "notam_number": "A0015/26", "type": "NOTAM", "icao": "LTFJ", "iata": "ESB", "airport_name": "Esenboga Intl", "effective_from": "2026-02-22T04:00:00Z", "effective_to": "2026-02-22T08:00:00Z", "severity": "HIGH", "content": "RWY 03L/21R CLSD FOR FICON TREATMENT", "status": "active", "source": "DGCA", "category": "runway"},
    # EGLL
    {"id": "NOTAM-A0016-26-EGLL", "notam_number": "A0016/26", "type": "NOTAM", "icao": "EGLL", "iata": "LHR", "airport_name": "Heathrow", "effective_from": "2026-02-17T00:00:00Z", "effective_to": "2026-03-17T00:00:00Z", "severity": "HIGH", "content": "RWY 09R/27L CLSD FOR RECONSTRUCTION 2300-0500 DAILY", "status": "active", "source": "CAA", "category": "runway"},
    {"id": "NOTAM-A0017-26-EGLL", "notam_number": "A0017/26", "type": "NOTAM", "icao": "EGLL", "iata": "LHR", "airport_name": "Heathrow", "effective_from": "2026-02-20T00:00:00Z", "effective_to": "2026-03-06T00:00:00Z", "severity": "MEDIUM", "content": "STAND 501-510 UNAVAILABLE DUE TO TERMINAL WORKS", "status": "active", "source": "CAA", "category": "apron"},
    {"id": "NOTAM-A0018-26-EGLL", "notam_number": "A0018/26", "type": "NOTAM", "icao": "EGLL", "iata": "LHR", "airport_name": "Heathrow", "effective_from": "2026-02-21T00:00:00Z", "effective_to": "2026-02-28T00:00:00Z", "severity": "LOW", "content": "DME/VOR LON UNDER TEST - DO NOT USE", "status": "active", "source": "CAA", "category": "navaid"},
    # EGKK
    {"id": "NOTAM-A0019-26-EGKK", "notam_number": "A0019/26", "type": "NOTAM", "icao": "EGKK", "iata": "LGW", "airport_name": "Gatwick", "effective_from": "2026-02-16T00:00:00Z", "effective_to": "2026-04-16T00:00:00Z", "severity": "HIGH", "content": "RWY 08R/26L CLSD UNTIL FURTHER NOTICE - SINGLE RWY OPS", "status": "active", "source": "CAA", "category": "runway"},
    {"id": "NOTAM-A0020-26-EGKK", "notam_number": "A0020/26", "type": "NOTAM", "icao": "EGKK", "iata": "LGW", "airport_name": "Gatwick", "effective_from": "2026-02-20T00:00:00Z", "effective_to": "2026-03-10T00:00:00Z", "severity": "MEDIUM", "content": "TWY J BTN TWY A AND RWY 08L/26R CLSD", "status": "active", "source": "CAA", "category": "taxiway"},
    {"id": "NOTAM-A0021-26-EGKK", "notam_number": "A0021/26", "type": "NOTAM", "icao": "EGKK", "iata": "LGW", "airport_name": "Gatwick", "effective_from": "2026-02-22T00:00:00Z", "effective_to": "2026-02-23T00:00:00Z", "severity": "LOW", "content": "FIRE PRACTICE IN PROGRESS NORTH SIDE OF AD", "status": "active", "source": "CAA", "category": "aerodrome"},
    # Additional mixed
    {"id": "NOTAM-A0022-26-KJFK", "notam_number": "A0022/26", "type": "NOTAM", "icao": "KJFK", "iata": "JFK", "airport_name": "John F Kennedy Intl", "effective_from": "2026-02-21T00:00:00Z", "effective_to": "2026-03-21T00:00:00Z", "severity": "MEDIUM", "content": "BIRD ACTIVITY REPORTED IN VICINITY OF AD - USE CAUTION", "status": "active", "source": "FAA", "category": "wildlife"},
    {"id": "NOTAM-A0023-26-KLGA", "notam_number": "A0023/26", "type": "NOTAM", "icao": "KLGA", "iata": "LGA", "airport_name": "LaGuardia", "effective_from": "2026-02-22T00:00:00Z", "effective_to": "2026-03-22T00:00:00Z", "severity": "LOW", "content": "RNAV (GPS) RWY 31 LNAV MDA INCREASED TO 780FT", "status": "active", "source": "FAA", "category": "procedure"},
    {"id": "NOTAM-A0024-26-LTFM", "notam_number": "A0024/26", "type": "NOTAM", "icao": "LTFM", "iata": "IST", "airport_name": "Istanbul Airport", "effective_from": "2026-02-22T00:00:00Z", "effective_to": "2026-02-28T00:00:00Z", "severity": "HIGH", "content": "FUEL SUPPLY RESTRICTIONS - MAX UPLIFT 50000KG PER AIRCRAFT", "status": "active", "source": "DGCA", "category": "fuel"},
    {"id": "NOTAM-A0025-26-EGLL", "notam_number": "A0025/26", "type": "NOTAM", "icao": "EGLL", "iata": "LHR", "airport_name": "Heathrow", "effective_from": "2026-02-22T08:00:00Z", "effective_to": "2026-02-22T18:00:00Z", "severity": "HIGH", "content": "SECURITY ALERT - ENHANCED SCREENING IN EFFECT ALL TERMINALS", "status": "active", "source": "CAA", "category": "security"},
]


def main():
    parser = argparse.ArgumentParser(description="Seed Cosmos DB with sample NOTAM documents")
    parser.add_argument("--endpoint", default=os.getenv("AZURE_COSMOS_ENDPOINT", ""), help="Cosmos DB endpoint")
    parser.add_argument("--key", default=os.getenv("AZURE_COSMOS_KEY", ""), help="Cosmos DB primary key")
    parser.add_argument("--database", default=os.getenv("AZURE_COSMOS_DATABASE", "aviationrag"), help="Database name")
    parser.add_argument("--container", default=os.getenv("AZURE_COSMOS_CONTAINER", "notams"), help="Container name")
    args = parser.parse_args()

    if not args.endpoint:
        print("Error: --endpoint or AZURE_COSMOS_ENDPOINT is required", file=sys.stderr)
        sys.exit(1)

    if args.key:
        client = CosmosClient(args.endpoint, credential=args.key)
    else:
        try:
            from azure.identity import DefaultAzureCredential
            print("No key provided — using DefaultAzureCredential (AAD auth)")
            client = CosmosClient(args.endpoint, credential=DefaultAzureCredential())
        except ImportError:
            print("Error: --key or AZURE_COSMOS_KEY is required (azure-identity not installed for AAD fallback)", file=sys.stderr)
            sys.exit(1)
    database = client.get_database_client(args.database)
    container = database.get_container_client(args.container)

    print(f"Seeding {len(SAMPLE_NOTAMS)} NOTAMs into {args.endpoint} / {args.database} / {args.container}")

    upserted = 0
    for notam in SAMPLE_NOTAMS:
        try:
            container.upsert_item(notam)
            upserted += 1
            print(f"  [{upserted}/{len(SAMPLE_NOTAMS)}] {notam['id']} ({notam['icao']})")
        except Exception as exc:
            print(f"  FAILED {notam['id']}: {exc}", file=sys.stderr)

    print(f"\nDone: {upserted}/{len(SAMPLE_NOTAMS)} documents upserted.")


if __name__ == "__main__":
    main()
