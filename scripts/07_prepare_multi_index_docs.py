#!/usr/bin/env python3
"""
Prepare multi-index vector documents for the pilot-brief demo.

Outputs:
  data/vector_docs/ops_narratives_docs.jsonl
  data/vector_docs/regulatory_docs.jsonl
  data/vector_docs/airport_ops_docs.jsonl
"""

from __future__ import annotations

import argparse
import csv
import json
import re
from pathlib import Path
from typing import Dict, Iterable, List, Optional


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUT_DIR = ROOT / "data" / "vector_docs"


def latest(glob_pattern: str) -> Optional[Path]:
    matches = sorted(ROOT.glob(glob_pattern), key=lambda p: p.stat().st_mtime, reverse=True)
    return matches[0] if matches else None


def iter_jsonl(path: Path) -> Iterable[Dict]:
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            try:
                yield json.loads(s)
            except json.JSONDecodeError:
                continue


def write_jsonl(path: Path, rows: List[Dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def doc_base(
    doc_id: str,
    content: str,
    title: str,
    source: str,
    *,
    asrs_report_id: str = "",
    event_date: str = "",
    aircraft_type: str = "",
    flight_phase: str = "",
    location: str = "",
    narrative_type: str = "",
    source_file: str = "",
) -> Dict:
    return {
        "id": doc_id,
        "content": content[:12000],
        "title": title[:500],
        "source": source,
        "asrs_report_id": asrs_report_id,
        "event_date": event_date,
        "aircraft_type": aircraft_type,
        "flight_phase": flight_phase,
        "location": location,
        "narrative_type": narrative_type,
        "source_file": source_file,
    }


def build_ops_docs(limit: int) -> List[Dict]:
    docs: List[Dict] = []
    asrs_file = latest("data/c1-asrs/processed/asrs_documents.jsonl")
    if asrs_file:
        for idx, row in enumerate(iter_jsonl(asrs_file), start=1):
            doc_id = f"ops_asrs_{row.get('id', idx)}"
            docs.append(
                doc_base(
                    doc_id=doc_id,
                    content=str(row.get("content", "")),
                    title=str(row.get("title", f"ASRS {idx}")),
                    source="ASRS",
                    asrs_report_id=str(row.get("asrs_report_id", "")),
                    event_date=str(row.get("event_date", "")),
                    aircraft_type=str(row.get("aircraft_type", "")),
                    flight_phase=str(row.get("flight_phase", "")),
                    location=str(row.get("location", "")),
                    narrative_type=str(row.get("narrative_type", "")),
                    source_file=str(row.get("source_file", str(asrs_file))),
                )
            )
            if len(docs) >= limit:
                break

    # Add synthetic operational overlays as short docs.
    legs_file = latest("data/j-synthetic_ops_overlay/*/synthetic/ops_flight_legs.csv")
    if legs_file:
        with legs_file.open("r", encoding="utf-8", errors="ignore", newline="") as f:
            for idx, row in enumerate(csv.DictReader(f), start=1):
                content = (
                    f"Flight leg {row.get('leg_id')} carrier {row.get('carrier_code')} "
                    f"from {row.get('origin_iata')} to {row.get('dest_iata')} "
                    f"scheduled dep {row.get('scheduled_dep_utc')} arr {row.get('scheduled_arr_utc')} "
                    f"tail {row.get('tailnum')} passengers {row.get('passengers')} distance_nm {row.get('distance_nm')}"
                )
                docs.append(
                    doc_base(
                        doc_id=f"ops_leg_{row.get('leg_id', idx)}",
                        content=content,
                        title=f"Operational flight leg {row.get('leg_id', idx)}",
                        source="SYNTHETIC_OPS",
                        event_date=str(row.get("scheduled_dep_utc", ""))[:10],
                        location=f"{row.get('origin_iata', '')}->{row.get('dest_iata', '')}",
                        source_file=str(legs_file),
                    )
                )
                if len(docs) >= limit:
                    break

    return docs[:limit]


def build_reg_docs(limit: int) -> List[Dict]:
    docs: List[Dict] = []

    notam_file = latest("data/h-notam_recent/*/search_location_istanbul.jsonl")
    if notam_file:
        for idx, row in enumerate(iter_jsonl(notam_file), start=1):
            fac = str(row.get("facilityDesignator", ""))
            notam_no = str(row.get("notamNumber", ""))
            content = str(row.get("icaoMessage", "")) or str(row)
            docs.append(
                doc_base(
                    doc_id=f"reg_notam_{notam_no or idx}",
                    content=content,
                    title=f"NOTAM {notam_no} {fac}".strip(),
                    source="NOTAM",
                    event_date=str(row.get("issueDate", ""))[:10],
                    location=fac,
                    narrative_type="REGULATORY",
                    source_file=str(notam_file),
                )
            )
            if len(docs) >= limit:
                break

    easa_csv = latest("data/d-easa_ads_recent/downloaded_ads_with_metadata.csv")
    if easa_csv and len(docs) < limit:
        with easa_csv.open("r", encoding="utf-8", errors="ignore", newline="") as f:
            for idx, row in enumerate(csv.DictReader(f), start=1):
                class_number = str(row.get("class_number", ""))
                content = (
                    f"AD {class_number} class {row.get('ad_class', '')}. "
                    f"Issue {row.get('issue_date', '')}, effective {row.get('effective_date', '')}. "
                    f"Subject: {row.get('subject', '')}. Applicability: {row.get('approval_holder_type', '')}. "
                    f"Reference URL: {row.get('ad_url', '')}"
                )
                docs.append(
                    doc_base(
                        doc_id=f"reg_easa_{class_number or idx}",
                        content=content,
                        title=f"EASA AD {class_number}".strip(),
                        source="EASA_AD",
                        event_date=str(row.get("effective_date", "")),
                        aircraft_type=str(row.get("approval_holder_type", "")),
                        narrative_type="REGULATORY",
                        source_file=str(easa_csv),
                    )
                )
                if len(docs) >= limit:
                    break

    return docs[:limit]


def _csv_or_dat(path: Path) -> Iterable[Dict[str, str]]:
    if path.suffix.lower() == ".csv":
        with path.open("r", encoding="utf-8", errors="ignore", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                yield row
        return

    # OpenFlights .dat format (CSV-like, no header).
    if "routes_" in path.name:
        with path.open("r", encoding="utf-8", errors="ignore", newline="") as f:
            reader = csv.reader(f)
            for row in reader:
                if len(row) < 6:
                    continue
                yield {
                    "airline": row[0],
                    "source_airport": row[2],
                    "destination_airport": row[4],
                    "stops": row[7] if len(row) > 7 else "",
                }


def build_airport_docs(limit: int) -> List[Dict]:
    docs: List[Dict] = []

    airport_csv = latest("data/g-ourairports_recent/airports_*.csv")
    runway_csv = latest("data/g-ourairports_recent/runways_*.csv")
    navaid_csv = latest("data/g-ourairports_recent/navaids_*.csv")
    routes_dat = latest("data/f-openflights/raw/routes_*.dat")

    for source_file, source_name in [
        (airport_csv, "OURAIRPORTS_AIRPORT"),
        (runway_csv, "OURAIRPORTS_RUNWAY"),
        (navaid_csv, "OURAIRPORTS_NAVAID"),
        (routes_dat, "OPENFLIGHTS_ROUTE"),
    ]:
        if not source_file:
            continue

        for idx, row in enumerate(_csv_or_dat(source_file), start=1):
            row_str = json.dumps(row, ensure_ascii=False)
            title_key = (
                row.get("name")
                or row.get("ident")
                or row.get("iata_code")
                or row.get("source_airport")
                or f"record-{idx}"
            )
            location = row.get("municipality") or row.get("iso_country") or row.get("source_airport") or ""
            docs.append(
                doc_base(
                    doc_id=f"apt_{source_name.lower()}_{idx}",
                    content=row_str,
                    title=f"{source_name} {title_key}",
                    source=source_name,
                    location=str(location),
                    narrative_type="AIRPORT_OPS",
                    source_file=str(source_file),
                )
            )
            if len(docs) >= limit:
                return docs

    return docs[:limit]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    ap.add_argument("--ops-limit", type=int, default=3000)
    ap.add_argument("--reg-limit", type=int, default=1200)
    ap.add_argument("--airport-limit", type=int, default=1500)
    args = ap.parse_args()

    ops_docs = build_ops_docs(max(1, args.ops_limit))
    reg_docs = build_reg_docs(max(1, args.reg_limit))
    airport_docs = build_airport_docs(max(1, args.airport_limit))

    out = args.out_dir
    write_jsonl(out / "ops_narratives_docs.jsonl", ops_docs)
    write_jsonl(out / "regulatory_docs.jsonl", reg_docs)
    write_jsonl(out / "airport_ops_docs.jsonl", airport_docs)

    summary = {
        "out_dir": str(out),
        "ops_docs": len(ops_docs),
        "reg_docs": len(reg_docs),
        "airport_docs": len(airport_docs),
    }
    (out / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
