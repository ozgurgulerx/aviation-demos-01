#!/usr/bin/env python3
"""
ASRS extraction and normalization.

Reads raw ASRS CSV exports and writes:
- data/processed/asrs_records.jsonl
- data/processed/asrs_documents.jsonl
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Tuple


REPORT_ID_CANDIDATES = [
    "asrs_report_id",
    "report_id",
    "report_number",
    "report_no",
    "record_id",
    "acn",
    "id",
]

EVENT_DATE_CANDIDATES = [
    "event_date",
    "occurrence_date",
    "date",
    "incident_date",
    "report_date",
]

AIRCRAFT_TYPE_CANDIDATES = [
    "aircraft_type",
    "aircraft_make_model",
    "aircraft_model",
    "aircraft",
    "acft_type",
]

FLIGHT_PHASE_CANDIDATES = [
    "flight_phase",
    "phase_of_flight",
    "phase",
]

LOCATION_CANDIDATES = [
    "location",
    "airport",
    "airport_id",
]

NARRATIVE_TYPE_CANDIDATES = [
    "narrative_type",
    "reporter_type",
    "person_role",
]

TITLE_CANDIDATES = [
    "title",
    "subject",
    "headline",
    "event_title",
]

NARRATIVE_CANDIDATES = [
    "narrative",
    "report_text",
    "report",
    "synopsis",
    "description",
    "callback",
    "callback_conversation",
]

ARCHIVE_TEXT_FILENAME = "TEXT_DATA_TABLE.csv"
ARCHIVE_ALL_ITEMS_FILENAME = "ALL_ITEMS_DATA_TABLE.csv"

ARCHIVE_TEXT_ATTRIBUTES = {
    "Narrative",
    "Narrative2",
    "Narrative3",
    "Narrative4",
    "Synopsis",
    "Callback",
    "Diagnosis",
    "Keywords",
}

ARCHIVE_METADATA_ATTRIBUTE_MAP = {
    "ASRS Report Number.Accession Number": "asrs_report_id",
    "Date": "event_date_raw",
    "Make Model Name": "aircraft_type",
    "Flight Phase": "flight_phase",
    "State Reference": "state",
    "Locale Reference.Airport": "airport",
    "Reporter Organization": "reporter_organization",
    "Function.Flight Crew": "flight_crew_function",
    "Function.Air Traffic Control": "atc_function",
}


def normalize_column(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", name.strip().lower()).strip("_")


def clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def parse_event_date(value: str) -> str | None:
    raw = value.strip()
    if not raw:
        return None

    fmts = [
        "%Y-%m-%d",
        "%m/%d/%Y",
        "%m/%d/%y",
        "%d-%b-%Y",
        "%Y/%m/%d",
    ]
    for fmt in fmts:
        try:
            return datetime.strptime(raw, fmt).date().isoformat()
        except ValueError:
            continue

    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).date().isoformat()
    except ValueError:
        return None


def pick_value(row: Dict[str, str], candidates: List[str]) -> str:
    for candidate in candidates:
        value = row.get(candidate, "")
        if value:
            return value
    return ""


def build_location(row: Dict[str, str]) -> str:
    direct = pick_value(row, LOCATION_CANDIDATES)
    if direct:
        return direct

    parts = [row.get("city", ""), row.get("state", ""), row.get("country", "")]
    parts = [part for part in parts if part]
    return ", ".join(parts)


def build_title(report_id: str, event_date: str | None, aircraft_type: str, location: str) -> str:
    bits = ["ASRS"]
    if event_date:
        bits.append(event_date)
    if aircraft_type:
        bits.append(aircraft_type)
    if location:
        bits.append(location)

    candidate = " | ".join(bits)
    return candidate if len(candidate) > 8 else f"ASRS Report {report_id}"


def collect_narrative(row: Dict[str, str]) -> str:
    parts: List[str] = []
    for key in NARRATIVE_CANDIDATES:
        value = row.get(key, "")
        if value:
            parts.append(clean_text(value))

    deduped: List[str] = []
    seen = set()
    for part in parts:
        if part not in seen:
            deduped.append(part)
            seen.add(part)

    return "\n\n".join(deduped).strip()


def fallback_report_id(row: Dict[str, str]) -> str:
    payload = json.dumps(row, sort_keys=True, ensure_ascii=False)
    digest = hashlib.sha1(payload.encode("utf-8")).hexdigest()[:16]
    return f"auto-{digest}"


def chunk_text(text: str, chunk_size_chars: int, overlap_chars: int) -> List[str]:
    normalized = text.strip()
    if not normalized:
        return []
    if len(normalized) <= chunk_size_chars:
        return [normalized]

    chunks: List[str] = []
    start = 0
    text_len = len(normalized)

    while start < text_len:
        end = min(start + chunk_size_chars, text_len)
        if end < text_len:
            split_at = normalized.rfind(" ", start + (chunk_size_chars // 2), end)
            if split_at > start:
                end = split_at

        chunk = normalized[start:end].strip()
        if chunk:
            chunks.append(chunk)

        if end >= text_len:
            break

        next_start = max(0, end - overlap_chars)
        if next_start <= start:
            next_start = end
        start = next_start

    return chunks


def merge_records(existing: Dict[str, str], incoming: Dict[str, str]) -> Dict[str, str]:
    result = dict(existing)

    if len(incoming.get("report_text", "")) > len(existing.get("report_text", "")):
        result["report_text"] = incoming["report_text"]

    for key in ["event_date", "location", "aircraft_type", "flight_phase", "narrative_type", "title"]:
        if not result.get(key) and incoming.get(key):
            result[key] = incoming[key]

    raw_sources = set(result.get("raw_sources", []))
    raw_sources.update(incoming.get("raw_sources", []))
    result["raw_sources"] = sorted(raw_sources)

    if len(incoming.get("raw_json", "")) > len(existing.get("raw_json", "")):
        result["raw_json"] = incoming["raw_json"]

    return result


def iter_csv_rows(path: Path) -> Iterable[Dict[str, str]]:
    with path.open("r", encoding="utf-8", errors="replace", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            normalized = {}
            for key, value in row.items():
                if key is None:
                    continue
                norm_key = normalize_column(key)
                normalized[norm_key] = clean_text(value or "")
            yield normalized


def iter_raw_csv_rows(path: Path) -> Iterable[Dict[str, str]]:
    with path.open("r", encoding="utf-8", errors="replace", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            normalized = {}
            for key, value in row.items():
                if key is None:
                    continue
                normalized[key.strip()] = clean_text(value or "")
            yield normalized


def parse_archive_date(value: str) -> str | None:
    raw = value.strip()
    if not raw:
        return None

    if re.fullmatch(r"\d{6}", raw):
        return f"{raw[0:4]}-{raw[4:6]}-01"
    if re.fullmatch(r"\d{8}", raw):
        return f"{raw[0:4]}-{raw[4:6]}-{raw[6:8]}"
    return parse_event_date(raw)


def join_values(values: List[str], sep: str = "; ") -> str:
    deduped: List[str] = []
    seen = set()
    for value in values:
        cleaned = clean_text(value)
        if cleaned and cleaned not in seen:
            deduped.append(cleaned)
            seen.add(cleaned)
    return sep.join(deduped)


def extract_archive_tables(input_path: Path) -> Tuple[Dict[str, Dict[str, str]], int, int]:
    text_tables = sorted(input_path.rglob(ARCHIVE_TEXT_FILENAME))
    all_items_tables = sorted(input_path.rglob(ARCHIVE_ALL_ITEMS_FILENAME))

    if not text_tables or not all_items_tables:
        raise FileNotFoundError(
            f"Archive mode requires both {ARCHIVE_TEXT_FILENAME} and {ARCHIVE_ALL_ITEMS_FILENAME}"
        )

    text_segments_by_item: Dict[str, List[Tuple[int, int, str]]] = {}
    rows_seen = 0

    attr_priority = {
        "Narrative": 1,
        "Narrative2": 2,
        "Narrative3": 3,
        "Narrative4": 4,
        "Synopsis": 5,
        "Callback": 6,
        "Diagnosis": 7,
        "Keywords": 8,
    }

    for text_table in text_tables:
        for row in iter_raw_csv_rows(text_table):
            rows_seen += 1
            item_id = row.get("ITEM_ID", "")
            attribute = row.get("ATTRIBUTE", "")
            text = row.get("TEXT", "")
            enum_raw = row.get("ENUMERATOR", "")

            if not item_id or not text or attribute not in ARCHIVE_TEXT_ATTRIBUTES:
                continue

            try:
                enum_val = int(enum_raw)
            except ValueError:
                enum_val = 0

            priority = attr_priority.get(attribute, 99)
            text_segments_by_item.setdefault(item_id, []).append((priority, enum_val, text))

    if not text_segments_by_item:
        return {}, rows_seen, len(text_tables) + len(all_items_tables)

    metadata_by_item: Dict[str, Dict[str, List[str] | str | None]] = {}

    def ensure_meta(item_id: str) -> Dict[str, List[str] | str | None]:
        if item_id not in metadata_by_item:
            metadata_by_item[item_id] = {
                "asrs_report_id": None,
                "event_date_raw": None,
                "aircraft_type": [],
                "flight_phase": [],
                "state": [],
                "airport": [],
                "reporter_organization": [],
                "flight_crew_function": [],
                "atc_function": [],
            }
        return metadata_by_item[item_id]

    for all_items_table in all_items_tables:
        for row in iter_raw_csv_rows(all_items_table):
            rows_seen += 1
            item_id = row.get("ITEM_ID", "")
            if not item_id or item_id not in text_segments_by_item:
                continue

            attribute = row.get("ATTRIBUTE", "")
            mapped_key = ARCHIVE_METADATA_ATTRIBUTE_MAP.get(attribute)
            if not mapped_key:
                continue

            value = row.get("DISPLAY_VALUE", "") or row.get("VALUE", "")
            value = clean_text(value)
            if not value:
                continue

            meta = ensure_meta(item_id)

            if mapped_key in {"asrs_report_id", "event_date_raw"}:
                if not meta[mapped_key]:
                    meta[mapped_key] = value
            else:
                values = meta[mapped_key]
                if isinstance(values, list):
                    values.append(value)

    records_by_id: Dict[str, Dict[str, str]] = {}

    for item_id, segments in text_segments_by_item.items():
        segments_sorted = sorted(segments, key=lambda x: (x[0], x[1]))
        report_text = "\n\n".join(part for _, __, part in segments_sorted if part).strip()
        if not report_text:
            continue

        meta = ensure_meta(item_id)
        report_id = str(meta.get("asrs_report_id") or item_id)
        event_date = parse_archive_date(str(meta.get("event_date_raw") or ""))
        aircraft_type = join_values(meta.get("aircraft_type", []))  # type: ignore[arg-type]
        flight_phase = join_values(meta.get("flight_phase", []))  # type: ignore[arg-type]
        airport = join_values(meta.get("airport", []), sep=", ")  # type: ignore[arg-type]
        state = join_values(meta.get("state", []), sep=", ")  # type: ignore[arg-type]
        location = ", ".join(part for part in [airport, state] if part)
        roles = join_values(meta.get("flight_crew_function", []))  # type: ignore[arg-type]
        atc_roles = join_values(meta.get("atc_function", []))  # type: ignore[arg-type]
        reporter_org = join_values(meta.get("reporter_organization", []))  # type: ignore[arg-type]
        narrative_type = " / ".join(part for part in [roles, atc_roles, reporter_org] if part)
        title = build_title(report_id, event_date, aircraft_type, location)

        compact_raw = {
            "item_id": item_id,
            "event_date_raw": meta.get("event_date_raw"),
            "airport": meta.get("airport"),
            "state": meta.get("state"),
            "aircraft_type": meta.get("aircraft_type"),
            "flight_phase": meta.get("flight_phase"),
            "reporter_organization": meta.get("reporter_organization"),
        }

        records_by_id[report_id] = {
            "asrs_report_id": report_id,
            "event_date": event_date,
            "location": location,
            "aircraft_type": aircraft_type,
            "flight_phase": flight_phase,
            "narrative_type": narrative_type,
            "title": title,
            "report_text": report_text,
            "source": "ASRS",
            "raw_json": json.dumps(compact_raw, ensure_ascii=False, sort_keys=True),
            "raw_sources": [str(text_tables[0]), str(all_items_tables[0])],
        }

    return records_by_id, rows_seen, len(text_tables) + len(all_items_tables)


def extract_data(input_dir: str, output_dir: str, chunk_size_chars: int, overlap_chars: int) -> None:
    input_path = Path(input_dir)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    archive_text_tables = list(input_path.rglob(ARCHIVE_TEXT_FILENAME))
    archive_all_items_tables = list(input_path.rglob(ARCHIVE_ALL_ITEMS_FILENAME))
    duplicate_rows = 0

    if archive_text_tables and archive_all_items_tables:
        records_by_id, rows_seen, files_scanned = extract_archive_tables(input_path)
        csv_files = sorted(set(archive_text_tables + archive_all_items_tables))
    else:
        csv_files = sorted(input_path.rglob("*.csv"))
        if not csv_files:
            raise FileNotFoundError(f"No CSV files found under {input_path}")

        files_scanned = len(csv_files)
        rows_seen = 0
        records_by_id: Dict[str, Dict[str, str]] = {}

        for csv_file in csv_files:
            for row in iter_csv_rows(csv_file):
                rows_seen += 1

                report_id = pick_value(row, REPORT_ID_CANDIDATES) or fallback_report_id(row)
                event_date = parse_event_date(pick_value(row, EVENT_DATE_CANDIDATES))
                aircraft_type = pick_value(row, AIRCRAFT_TYPE_CANDIDATES)
                flight_phase = pick_value(row, FLIGHT_PHASE_CANDIDATES)
                location = build_location(row)
                narrative_type = pick_value(row, NARRATIVE_TYPE_CANDIDATES)
                title = pick_value(row, TITLE_CANDIDATES)
                report_text = collect_narrative(row)

                if not report_text:
                    continue

                if not title:
                    title = build_title(report_id, event_date, aircraft_type, location)

                normalized_record = {
                    "asrs_report_id": report_id,
                    "event_date": event_date,
                    "location": location,
                    "aircraft_type": aircraft_type,
                    "flight_phase": flight_phase,
                    "narrative_type": narrative_type,
                    "title": title,
                    "report_text": report_text,
                    "source": "ASRS",
                    "raw_json": json.dumps(row, ensure_ascii=False, sort_keys=True),
                    "raw_sources": [str(csv_file)],
                }

                if report_id in records_by_id:
                    duplicate_rows += 1
                    records_by_id[report_id] = merge_records(records_by_id[report_id], normalized_record)
                else:
                    records_by_id[report_id] = normalized_record

    records = [records_by_id[key] for key in sorted(records_by_id.keys())]

    records_path = output_path / "asrs_records.jsonl"
    with records_path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")

    documents: List[Dict[str, str]] = []
    for record in records:
        chunks = chunk_text(record["report_text"], chunk_size_chars, overlap_chars)
        total_chunks = len(chunks)

        for idx, chunk in enumerate(chunks):
            title = record["title"]
            if total_chunks > 1:
                title = f"{title} (part {idx + 1}/{total_chunks})"

            documents.append(
                {
                    "id": f"{record['asrs_report_id']}:{idx + 1:03d}",
                    "asrs_report_id": record["asrs_report_id"],
                    "title": title,
                    "content": chunk,
                    "source": record["source"],
                    "event_date": record.get("event_date"),
                    "location": record.get("location"),
                    "aircraft_type": record.get("aircraft_type"),
                    "flight_phase": record.get("flight_phase"),
                    "narrative_type": record.get("narrative_type"),
                    "source_file": record.get("raw_sources", [""])[0],
                }
            )

    docs_path = output_path / "asrs_documents.jsonl"
    with docs_path.open("w", encoding="utf-8") as handle:
        for document in documents:
            handle.write(json.dumps(document, ensure_ascii=False) + "\n")

    summary = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "input_dir": str(input_path),
        "files_scanned": files_scanned,
        "rows_seen": rows_seen,
        "duplicates_collapsed": duplicate_rows,
        "unique_records": len(records),
        "documents_created": len(documents),
        "records_path": str(records_path),
        "documents_path": str(docs_path),
    }

    summary_path = output_path / "asrs_extract_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    print("ASRS extraction complete")
    print(f"  CSV files scanned: {summary['files_scanned']}")
    print(f"  Rows seen: {summary['rows_seen']}")
    print(f"  Unique records: {summary['unique_records']}")
    print(f"  Documents created: {summary['documents_created']}")
    print(f"  Records output: {records_path}")
    print(f"  Documents output: {docs_path}")
    print(f"  Summary: {summary_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Extract and normalize ASRS CSV exports")
    parser.add_argument("--input", default="data/asrs/raw", help="Input directory with raw ASRS CSV files")
    parser.add_argument("--output", default="data/processed", help="Output directory for processed files")
    parser.add_argument("--chunk-size-chars", type=int, default=1200, help="Chunk size for vector documents")
    parser.add_argument("--chunk-overlap-chars", type=int, default=200, help="Chunk overlap for vector documents")
    args = parser.parse_args()

    if args.chunk_size_chars < 100:
        raise ValueError("--chunk-size-chars must be >= 100")
    if args.chunk_overlap_chars < 0:
        raise ValueError("--chunk-overlap-chars must be >= 0")
    if args.chunk_overlap_chars >= args.chunk_size_chars:
        raise ValueError("--chunk-overlap-chars must be smaller than --chunk-size-chars")

    extract_data(args.input, args.output, args.chunk_size_chars, args.chunk_overlap_chars)


if __name__ == "__main__":
    main()
