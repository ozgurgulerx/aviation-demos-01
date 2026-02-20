#!/usr/bin/env python3
"""
ASRS export fetcher.

Downloads ASRS CSV export files from a configurable HTTP endpoint and writes
an ingestion manifest for traceability.

Usage:
    python scripts/00_fetch_asrs_exports.py --from-date 2026-01-01 --to-date 2026-01-31

Required environment variables:
    ASRS_EXPORT_URL

Optional environment variables:
    ASRS_HTTP_METHOD=GET|POST
    ASRS_QUERY_TEMPLATE_JSON='{"q":"runway incursion"}'
    ASRS_HEADERS_JSON='{"Authorization":"Bearer ..."}'
    ASRS_SESSION_COOKIE='name=value; name2=value2'
    ASRS_FROM_PARAM=fromDate
    ASRS_TO_PARAM=toDate
    ASRS_FORMAT_PARAM=format
    ASRS_FORMAT_VALUE=csv
    ASRS_LIMIT_PARAM=limit
    ASRS_TIMEOUT_SECONDS=90
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import os
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Tuple
from urllib.parse import urlencode
from urllib.request import Request, urlopen


DEFAULT_MAX_ROWS = 10_000


@dataclass(frozen=True)
class DateWindow:
    start: date
    end: date

    @property
    def days(self) -> int:
        return (self.end - self.start).days + 1


def parse_iso_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def previous_month_range(today_utc: date | None = None) -> Tuple[date, date]:
    today = today_utc or datetime.now(timezone.utc).date()
    first_this_month = today.replace(day=1)
    last_prev_month = first_this_month - timedelta(days=1)
    first_prev_month = last_prev_month.replace(day=1)
    return first_prev_month, last_prev_month


def iter_windows(start: date, end: date, chunk_days: int) -> List[DateWindow]:
    windows: List[DateWindow] = []
    cursor = start
    while cursor <= end:
        window_end = min(cursor + timedelta(days=chunk_days - 1), end)
        windows.append(DateWindow(cursor, window_end))
        cursor = window_end + timedelta(days=1)
    return windows


def split_window(window: DateWindow) -> Tuple[DateWindow, DateWindow]:
    midpoint = window.start + timedelta(days=(window.days // 2) - 1)
    if midpoint < window.start:
        midpoint = window.start
    left = DateWindow(window.start, midpoint)
    right = DateWindow(midpoint + timedelta(days=1), window.end)
    return left, right


def parse_json_env(name: str, default: Dict[str, str] | None = None) -> Dict[str, str]:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default or {}
    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        raise ValueError(f"{name} must be a JSON object")
    return {str(k): str(v) for k, v in parsed.items()}


def count_csv_rows(csv_text: str) -> int:
    reader = csv.reader(io.StringIO(csv_text))
    header = next(reader, None)
    if header is None:
        return 0
    return sum(1 for _ in reader)


def looks_like_csv(text: str) -> bool:
    sample = text[:2048]
    return "," in sample and "\n" in sample


def build_params(
    template: Dict[str, str],
    window: DateWindow,
    max_rows: int,
    include_format: bool = True,
) -> Dict[str, str]:
    params = dict(template)
    from_param = os.getenv("ASRS_FROM_PARAM", "fromDate")
    to_param = os.getenv("ASRS_TO_PARAM", "toDate")
    limit_param = os.getenv("ASRS_LIMIT_PARAM", "limit")
    format_param = os.getenv("ASRS_FORMAT_PARAM", "format")
    format_value = os.getenv("ASRS_FORMAT_VALUE", "csv")

    params[from_param] = window.start.isoformat()
    params[to_param] = window.end.isoformat()
    params[limit_param] = str(max_rows)

    if include_format:
        params[format_param] = format_value

    return params


def sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def unique_output_path(base_dir: Path, stem: str) -> Path:
    candidate = base_dir / f"{stem}.csv"
    if not candidate.exists():
        return candidate

    suffix = 2
    while True:
        alt = base_dir / f"{stem}_v{suffix}.csv"
        if not alt.exists():
            return alt
        suffix += 1


def fetch_csv(
    url: str,
    method: str,
    timeout_seconds: int,
    params: Dict[str, str],
    headers: Dict[str, str],
) -> Tuple[bytes, str]:
    if method == "GET":
        query = urlencode(params)
        sep = "&" if "?" in url else "?"
        request_url = f"{url}{sep}{query}" if query else url
        req = Request(request_url, headers=headers, method="GET")
    else:
        body = urlencode(params).encode("utf-8")
        post_headers = dict(headers)
        post_headers.setdefault("Content-Type", "application/x-www-form-urlencoded")
        req = Request(url, data=body, headers=post_headers, method="POST")

    with urlopen(req, timeout=timeout_seconds) as response:
        payload = response.read()
        content_type = response.headers.get("content-type", "")
        return payload, content_type


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch ASRS exports as CSV files")
    parser.add_argument("--from-date", help="Start date (YYYY-MM-DD). Defaults to previous month start.")
    parser.add_argument("--to-date", help="End date (YYYY-MM-DD). Defaults to previous month end.")
    parser.add_argument("--chunk-days", type=int, default=31, help="Initial date window size")
    parser.add_argument("--max-rows-per-file", type=int, default=DEFAULT_MAX_ROWS, help="Max rows per exported file")
    parser.add_argument("--out-dir", default="data/asrs/raw", help="Output directory for CSV files")
    parser.add_argument("--manifest-dir", default="data/asrs/manifests", help="Output directory for run manifests")
    parser.add_argument("--dry-run", action="store_true", help="Plan windows without downloading")
    parser.add_argument("--keep-empty", action="store_true", help="Keep files with zero rows")
    args = parser.parse_args()

    export_url = os.getenv("ASRS_EXPORT_URL", "").strip()
    if not export_url and not args.dry_run:
        raise ValueError("ASRS_EXPORT_URL is required unless --dry-run is used")

    if args.chunk_days < 1:
        raise ValueError("--chunk-days must be >= 1")

    if args.max_rows_per_file < 1:
        raise ValueError("--max-rows-per-file must be >= 1")

    if args.from_date and args.to_date:
        start_date = parse_iso_date(args.from_date)
        end_date = parse_iso_date(args.to_date)
    elif args.from_date or args.to_date:
        raise ValueError("Provide both --from-date and --to-date, or neither")
    else:
        start_date, end_date = previous_month_range()

    if end_date < start_date:
        raise ValueError("--to-date must be >= --from-date")

    run_id = f"asrs-fetch-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:8]}"
    out_dir = Path(args.out_dir)
    manifest_dir = Path(args.manifest_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    manifest_dir.mkdir(parents=True, exist_ok=True)

    query_template = parse_json_env("ASRS_QUERY_TEMPLATE_JSON")
    headers = parse_json_env("ASRS_HEADERS_JSON")
    method = os.getenv("ASRS_HTTP_METHOD", "GET").upper().strip()
    if method not in {"GET", "POST"}:
        raise ValueError("ASRS_HTTP_METHOD must be GET or POST")

    timeout_seconds = int(os.getenv("ASRS_TIMEOUT_SECONDS", "90"))
    request_headers = dict(headers)
    session_cookie = os.getenv("ASRS_SESSION_COOKIE", "").strip()
    if session_cookie:
        request_headers["Cookie"] = session_cookie

    seed_windows = iter_windows(start_date, end_date, args.chunk_days)
    queue: List[DateWindow] = list(seed_windows)

    manifest = {
        "run_id": run_id,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "source": {
            "url": export_url,
            "method": method,
            "template_params": query_template,
        },
        "request": {
            "from_date": start_date.isoformat(),
            "to_date": end_date.isoformat(),
            "chunk_days": args.chunk_days,
            "max_rows_per_file": args.max_rows_per_file,
            "dry_run": args.dry_run,
        },
        "files": [],
        "warnings": [],
    }

    print(f"Run ID: {run_id}")
    print(f"Requested range: {start_date} -> {end_date}")
    print(f"Initial windows: {len(seed_windows)}")

    while queue:
        window = queue.pop(0)
        params = build_params(query_template, window, args.max_rows_per_file)

        if args.dry_run:
            manifest["files"].append(
                {
                    "date_window": {
                        "start": window.start.isoformat(),
                        "end": window.end.isoformat(),
                    },
                    "planned": True,
                    "query_params": params,
                }
            )
            continue

        payload, content_type = fetch_csv(
            url=export_url,
            method=method,
            timeout_seconds=timeout_seconds,
            params=params,
            headers=request_headers,
        )

        text = payload.decode("utf-8", errors="replace")
        if not looks_like_csv(text):
            snippet = text[:200].replace("\n", " ")
            raise ValueError(
                "Export response did not look like CSV "
                f"for {window.start}..{window.end} (content-type={content_type!r}, sample={snippet!r})"
            )

        row_count = count_csv_rows(text)

        if row_count >= args.max_rows_per_file and window.days > 1:
            left, right = split_window(window)
            queue.insert(0, right)
            queue.insert(0, left)
            manifest["warnings"].append(
                {
                    "type": "window_split",
                    "reason": "row_count_reached_limit",
                    "window": {
                        "start": window.start.isoformat(),
                        "end": window.end.isoformat(),
                    },
                    "row_count": row_count,
                    "max_rows_per_file": args.max_rows_per_file,
                    "split_into": [
                        {"start": left.start.isoformat(), "end": left.end.isoformat()},
                        {"start": right.start.isoformat(), "end": right.end.isoformat()},
                    ],
                }
            )
            print(
                "Split window "
                f"{window.start}..{window.end} because row_count={row_count} reached limit={args.max_rows_per_file}"
            )
            continue

        if row_count == 0 and not args.keep_empty:
            manifest["warnings"].append(
                {
                    "type": "empty_window",
                    "window": {
                        "start": window.start.isoformat(),
                        "end": window.end.isoformat(),
                    },
                    "action": "skipped",
                }
            )
            print(f"Skip empty window {window.start}..{window.end}")
            continue

        stem = f"asrs_{window.start.strftime('%Y%m%d')}_{window.end.strftime('%Y%m%d')}"
        csv_path = unique_output_path(out_dir, stem)
        csv_path.write_bytes(payload)

        file_entry = {
            "path": str(csv_path),
            "filename": csv_path.name,
            "bytes": len(payload),
            "sha256": sha256_bytes(payload),
            "row_count": row_count,
            "content_type": content_type,
            "date_window": {
                "start": window.start.isoformat(),
                "end": window.end.isoformat(),
            },
            "query_params": params,
        }
        manifest["files"].append(file_entry)
        print(f"Saved {csv_path} ({row_count} rows)")

    total_rows = sum(f.get("row_count", 0) for f in manifest["files"] if not f.get("planned"))
    manifest["summary"] = {
        "file_count": len([f for f in manifest["files"] if not f.get("planned")]),
        "planned_windows": len([f for f in manifest["files"] if f.get("planned")]),
        "total_rows": total_rows,
        "warnings": len(manifest["warnings"]),
    }

    manifest_path = manifest_dir / f"asrs_manifest_{run_id}.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    print("\nRun summary")
    print(f"  Manifest: {manifest_path}")
    print(f"  Downloaded files: {manifest['summary']['file_count']}")
    print(f"  Planned windows: {manifest['summary']['planned_windows']}")
    print(f"  Total rows: {manifest['summary']['total_rows']}")
    print(f"  Warnings: {manifest['summary']['warnings']}")


if __name__ == "__main__":
    main()
