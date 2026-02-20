#!/usr/bin/env python3
"""
Fetch source-6 airline schedule/feed references and sample data artifacts.

Outputs a timestamped folder under data/k-airline_schedule_feed by default.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin
from zipfile import ZipFile


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUT_ROOT = ROOT / "data" / "k-airline_schedule_feed"

PREZIP_URL = "https://transtats.bts.gov/PREZIP/"
OPENSKY_API_HOME = "https://opensky-network.org/api"
OPENSKY_STATES_ALL = "https://opensky-network.org/api/states/all"
AERODATABOX_HOME = "https://aerodatabox.com/"
AERODATABOX_RAPIDAPI = "https://rapidapi.com/aedbx-aedbx/api/aerodatabox/"


def run(cmd: List[str], *, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, check=check, text=True, capture_output=True)


def curl_download(
    url: str,
    dst: Path,
    extra: Optional[List[str]] = None,
    *,
    check: bool = True,
    max_time_sec: int = 180,
) -> subprocess.CompletedProcess:
    dst.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        "curl",
        "-sSL",
        "--connect-timeout",
        "20",
        "--max-time",
        str(max(30, max_time_sec)),
        url,
        "-o",
        str(dst),
    ]
    if extra:
        cmd[1:1] = extra
    return run(cmd, check=check)


def curl_headers(url: str, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    cp = run(
        [
            "curl",
            "-sSI",
            "--connect-timeout",
            "20",
            "--max-time",
            "90",
            url,
        ],
        check=False,
    )
    dst.write_text(cp.stdout + cp.stderr, encoding="utf-8")


def best_effort_download(url: str, dst: Path, max_time_sec: int = 90) -> Dict[str, object]:
    try:
        cp = curl_download(url, dst, check=False, max_time_sec=max_time_sec)
        return {
            "url": url,
            "saved_file": str(dst),
            "return_code": cp.returncode,
            "status": "ok" if cp.returncode == 0 else "error",
        }
    except Exception as e:  # noqa: BLE001 - keep fetcher resilient for optional refs
        dst.parent.mkdir(parents=True, exist_ok=True)
        dst.write_text(f"download failed: {e}\n", encoding="utf-8")
        return {"url": url, "saved_file": str(dst), "status": "exception", "error": str(e)}


def parse_prezip_links(index_html: str) -> List[str]:
    hrefs = re.findall(r'HREF="([^"]+)"', index_html, flags=re.IGNORECASE)
    files: List[str] = []
    for href in hrefs:
        if "/PREZIP/" in href:
            name = href.rsplit("/", 1)[-1]
        else:
            name = href
        if name.lower().endswith(".zip"):
            files.append(name)
    return sorted(set(files))


def parse_year_month(filename: str) -> Optional[Tuple[int, int]]:
    m = re.search(r"_(\d{4})_(\d{1,2})\.zip$", filename)
    if not m:
        return None
    return int(m.group(1)), int(m.group(2))


def select_recent_files(
    files: List[str],
    *,
    regex: str,
    max_items: int,
) -> List[str]:
    pat = re.compile(regex, flags=re.IGNORECASE)
    candidates = [f for f in files if pat.search(f)]
    dated = []
    undated = []
    for f in candidates:
        ym = parse_year_month(f)
        if ym:
            dated.append((ym[0], ym[1], f))
        else:
            undated.append(f)
    dated.sort(reverse=True)
    picked = [item[2] for item in dated[:max_items]]
    if len(picked) < max_items:
        for f in sorted(undated, reverse=True):
            if len(picked) >= max_items:
                break
            picked.append(f)
    return picked


def write_zip_preview(zip_path: Path, out_preview: Path, max_lines: int) -> Dict[str, str]:
    meta: Dict[str, str] = {}
    with ZipFile(zip_path, "r") as zf:
        members = zf.namelist()
        meta["member_count"] = str(len(members))
        csv_members = [m for m in members if m.lower().endswith(".csv")]
        target = csv_members[0] if csv_members else (members[0] if members else "")
        meta["preview_member"] = target
        if not target:
            out_preview.write_text("", encoding="utf-8")
            return meta
        lines: List[str] = []
        with zf.open(target, "r") as src:
            # Read only a small preview; this avoids full extraction.
            for idx, bline in enumerate(src):
                if idx >= max_lines:
                    break
                lines.append(bline.decode("utf-8", errors="replace"))
        out_preview.write_text("".join(lines), encoding="utf-8")
    return meta


def sanitize_name(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", name).strip("_")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    ap.add_argument("--ontime-months", type=int, default=2)
    ap.add_argument("--delay-files", type=int, default=1)
    ap.add_argument("--preview-lines", type=int, default=20)
    args = ap.parse_args()

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_dir = args.out_root / ts
    raw_dir = out_dir / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    prezip_index = raw_dir / "bts_prezip_index.html"
    curl_download(PREZIP_URL, prezip_index)
    index_html = prezip_index.read_text(encoding="utf-8", errors="ignore")

    all_zip_files = parse_prezip_links(index_html)
    ontime_files = select_recent_files(
        all_zip_files,
        regex=r"On_Time_Marketing_Carrier_On_Time_Performance.*\.zip$",
        max_items=max(1, args.ontime_months),
    )
    delay_files = select_recent_files(
        all_zip_files,
        regex=r"airline_delay_causes.*\.zip$",
        max_items=max(1, args.delay_files),
    )

    download_log: List[Dict[str, object]] = []

    for name in ontime_files:
        dst = raw_dir / sanitize_name(name)
        curl_download(urljoin(PREZIP_URL, name), dst)
        preview_path = raw_dir / f"{dst.stem}.preview.txt"
        preview_meta = write_zip_preview(dst, preview_path, max_lines=max(1, args.preview_lines))
        download_log.append(
            {
                "type": "bts_ontime",
                "source_file": name,
                "saved_file": str(dst),
                "bytes": dst.stat().st_size,
                "preview_file": str(preview_path),
                "preview_member": preview_meta.get("preview_member", ""),
                "member_count": int(preview_meta.get("member_count", "0")),
            }
        )

    for name in delay_files:
        dst = raw_dir / sanitize_name(name)
        curl_download(urljoin(PREZIP_URL, name), dst)
        preview_path = raw_dir / f"{dst.stem}.preview.txt"
        preview_meta = write_zip_preview(dst, preview_path, max_lines=max(1, args.preview_lines))
        download_log.append(
            {
                "type": "bts_delay_causes",
                "source_file": name,
                "saved_file": str(dst),
                "bytes": dst.stat().st_size,
                "preview_file": str(preview_path),
                "preview_member": preview_meta.get("preview_member", ""),
                "member_count": int(preview_meta.get("member_count", "0")),
            }
        )

    # Source-reference captures for OpenSky (API-first) and AeroDataBox (future schedule API).
    curl_headers(OPENSKY_STATES_ALL, raw_dir / "opensky_states_all.headers.txt")
    ref_fetches: List[Dict[str, object]] = []
    ref_fetches.append(
        best_effort_download(OPENSKY_API_HOME, raw_dir / "opensky_api_home.html", max_time_sec=60)
    )
    curl_headers(AERODATABOX_HOME, raw_dir / "aerodatabox_home.headers.txt")
    curl_headers(AERODATABOX_RAPIDAPI, raw_dir / "aerodatabox_rapidapi.headers.txt")
    ref_fetches.append(
        best_effort_download(
            AERODATABOX_RAPIDAPI,
            raw_dir / "aerodatabox_rapidapi.html",
            max_time_sec=90,
        )
    )

    files_info = {
        "bts_prezip_index": str(prezip_index),
        "opensky_states_all_headers": str(raw_dir / "opensky_states_all.headers.txt"),
        "opensky_api_home": str(raw_dir / "opensky_api_home.html"),
        "aerodatabox_home_headers": str(raw_dir / "aerodatabox_home.headers.txt"),
        "aerodatabox_rapidapi_headers": str(raw_dir / "aerodatabox_rapidapi.headers.txt"),
        "aerodatabox_rapidapi_html": str(raw_dir / "aerodatabox_rapidapi.html"),
    }

    manifest = {
        "generated_at_utc": ts,
        "output_dir": str(out_dir),
        "config": {
            "ontime_months": args.ontime_months,
            "delay_files": args.delay_files,
            "preview_lines": args.preview_lines,
        },
        "sources": {
            "bts_prezip_url": PREZIP_URL,
            "opensky_api_home": OPENSKY_API_HOME,
            "opensky_states_all": OPENSKY_STATES_ALL,
            "aerodatabox_home": AERODATABOX_HOME,
            "aerodatabox_rapidapi": AERODATABOX_RAPIDAPI,
        },
        "downloaded_files": download_log,
        "reference_fetches": ref_fetches,
        "artifacts": files_info,
        "notes": [
            "BTS On-Time PREZIP files include scheduled and actual operational timestamps for U.S. flights.",
            "OpenSky endpoint artifacts capture API-first flight-state availability (states endpoint headers + API page fetch).",
            "AeroDataBox artifacts are stored as future-schedule API reference links for optional paid/freemium integration.",
        ],
    }
    manifest_path = out_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
