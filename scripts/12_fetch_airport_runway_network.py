#!/usr/bin/env python3
"""
Fetch one-shot airport/runway/network reference datasets.

Sources:
- OpenFlights (.dat snapshots)
- OurAirports (.csv snapshots)

Outputs:
- data/f-openflights/raw/*_{timestamp}.dat
- data/g-ourairports_recent/*_{timestamp}.csv
- per-source manifest_{timestamp}.txt
"""

from __future__ import annotations

import argparse
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List


ROOT = Path(__file__).resolve().parents[1]
OPENFLIGHTS_DIR = ROOT / "data" / "f-openflights" / "raw"
OURAIRPORTS_DIR = ROOT / "data" / "g-ourairports_recent"

OPENFLIGHTS_BASE = "https://raw.githubusercontent.com/jpatokal/openflights/master/data"
OURAIRPORTS_BASE = "https://davidmegginson.github.io/ourairports-data"

OPENFLIGHTS_FILES = {
    "airports": f"{OPENFLIGHTS_BASE}/airports.dat",
    "airlines": f"{OPENFLIGHTS_BASE}/airlines.dat",
    "routes": f"{OPENFLIGHTS_BASE}/routes.dat",
    "airports-extended": f"{OPENFLIGHTS_BASE}/airports-extended.dat",
    "countries": f"{OPENFLIGHTS_BASE}/countries.dat",
    "planes": f"{OPENFLIGHTS_BASE}/planes.dat",
}

OURAIRPORTS_FILES = {
    "airports": f"{OURAIRPORTS_BASE}/airports.csv",
    "runways": f"{OURAIRPORTS_BASE}/runways.csv",
    "navaids": f"{OURAIRPORTS_BASE}/navaids.csv",
    "airport-frequencies": f"{OURAIRPORTS_BASE}/airport-frequencies.csv",
    "countries": f"{OURAIRPORTS_BASE}/countries.csv",
    "regions": f"{OURAIRPORTS_BASE}/regions.csv",
}


def run(cmd: List[str], check: bool = False) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, check=check, text=True, capture_output=True)


def curl_download(url: str, dst: Path, retries: int, timeout_seconds: int) -> subprocess.CompletedProcess:
    dst.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        "curl",
        "-fL",
        "-sS",
        "--connect-timeout",
        "20",
        "--max-time",
        str(max(30, timeout_seconds)),
        "--retry",
        str(max(0, retries)),
        "--retry-delay",
        "2",
        "--retry-connrefused",
        url,
        "-o",
        str(dst),
    ]
    return run(cmd, check=False)


def count_rows(path: Path) -> int:
    # For CSV, row count excludes the first header line.
    row_count = 0
    with path.open("r", encoding="utf-8", errors="ignore") as handle:
        for _ in handle:
            row_count += 1
    if path.suffix.lower() == ".csv":
        return max(0, row_count - 1)
    return row_count


def write_manifest(path: Path, ts: str, source_name: str, records: List[Dict[str, object]]) -> None:
    lines = [
        f"generated_at_utc={ts}",
        f"source={source_name}",
        f"record_count={len(records)}",
    ]
    for idx, rec in enumerate(records, start=1):
        lines.append(
            "record_{idx}=name:{name}|status:{status}|bytes:{bytes}|rows:{rows}|file:{file}|url:{url}".format(
                idx=idx,
                name=rec.get("name", ""),
                status=rec.get("status", ""),
                bytes=rec.get("bytes", 0),
                rows=rec.get("rows", 0),
                file=rec.get("file", ""),
                url=rec.get("url", ""),
            )
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def fetch_group(
    source_name: str,
    out_dir: Path,
    file_map: Dict[str, str],
    extension: str,
    ts: str,
    retries: int,
    timeout_seconds: int,
) -> Dict[str, object]:
    out_dir.mkdir(parents=True, exist_ok=True)
    results: List[Dict[str, object]] = []
    failures = 0

    for name, url in file_map.items():
        dst = out_dir / f"{name}_{ts}{extension}"
        tmp_dst = dst.with_name(dst.name + ".tmp")
        cp = curl_download(url, tmp_dst, retries=retries, timeout_seconds=timeout_seconds)
        if cp.returncode == 0 and tmp_dst.exists() and tmp_dst.stat().st_size > 0:
            tmp_dst.replace(dst)
        else:
            if tmp_dst.exists():
                tmp_dst.unlink()
            if dst.exists() and dst.stat().st_size == 0:
                dst.unlink()
        exists = dst.exists()
        size = dst.stat().st_size if exists else 0
        rows = count_rows(dst) if exists and size > 0 else 0
        status = "ok" if cp.returncode == 0 and exists and size > 0 else "error"
        if status != "ok":
            failures += 1
        results.append(
            {
                "name": name,
                "url": url,
                "file": str(dst),
                "status": status,
                "return_code": cp.returncode,
                "bytes": size,
                "rows": rows,
                "stderr": (cp.stderr or "")[:400],
            }
        )

    manifest_path = out_dir / f"manifest_{ts}.txt"
    write_manifest(manifest_path, ts, source_name, results)

    return {
        "source": source_name,
        "manifest": str(manifest_path),
        "output_dir": str(out_dir),
        "files": results,
        "failures": failures,
        "status": "ok" if failures == 0 else "error",
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Fetch one-shot OpenFlights + OurAirports snapshots")
    ap.add_argument("--timestamp", default="", help="Optional fixed UTC timestamp, e.g. 20260220T120000Z")
    ap.add_argument("--timeout-seconds", type=int, default=120)
    ap.add_argument("--retries", type=int, default=3)
    ap.add_argument("--skip-openflights", action="store_true")
    ap.add_argument("--skip-ourairports", action="store_true")
    ap.add_argument("--strict", dest="strict", action="store_true")
    ap.add_argument("--no-strict", dest="strict", action="store_false")
    ap.set_defaults(strict=True)
    args = ap.parse_args()

    ts = args.timestamp.strip() or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    summary: Dict[str, object] = {
        "generated_at_utc": ts,
        "strict": bool(args.strict),
        "groups": {},
    }

    if not args.skip_openflights:
        summary["groups"] = dict(summary["groups"])
        summary["groups"]["openflights"] = fetch_group(
            source_name="openflights-github",
            out_dir=OPENFLIGHTS_DIR,
            file_map=OPENFLIGHTS_FILES,
            extension=".dat",
            ts=ts,
            retries=args.retries,
            timeout_seconds=args.timeout_seconds,
        )

    if not args.skip_ourairports:
        summary["groups"] = dict(summary["groups"])
        summary["groups"]["ourairports"] = fetch_group(
            source_name="ourairports-data",
            out_dir=OURAIRPORTS_DIR,
            file_map=OURAIRPORTS_FILES,
            extension=".csv",
            ts=ts,
            retries=args.retries,
            timeout_seconds=args.timeout_seconds,
        )

    total_failures = 0
    groups = summary.get("groups", {})
    if isinstance(groups, dict):
        for group in groups.values():
            if isinstance(group, dict):
                total_failures += int(group.get("failures", 0))
    summary["total_failures"] = total_failures
    summary["status"] = "ok" if total_failures == 0 else "error"

    print(json.dumps(summary, indent=2))
    if args.strict and total_failures > 0:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
