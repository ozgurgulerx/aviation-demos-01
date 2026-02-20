#!/usr/bin/env python3
"""
Upload curated demo datasets from ./data into Fabric OneLake Lakehouse Files.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Tuple
from urllib import error, parse, request


ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = ROOT / "data"
DEFAULT_CHUNK_BYTES = 8 * 1024 * 1024


def run_az_token(resource: str) -> str:
    out = subprocess.check_output(
        ["az", "account", "get-access-token", "--resource", resource, "--query", "accessToken", "-o", "tsv"],
        text=True,
    )
    token = out.strip()
    if not token:
        raise RuntimeError("Failed to acquire access token via az CLI")
    return token


class OneLakeClient:
    def __init__(self, workspace_id: str, lakehouse_id: str, token: str):
        self.workspace_id = workspace_id
        self.lakehouse_id = lakehouse_id
        self.token = token
        self.base = f"https://onelake.dfs.fabric.microsoft.com/{workspace_id}/{lakehouse_id}/Files"
        self._created_dirs = set()

    def _url(self, relative_path: str, query: str = "") -> str:
        rel = parse.quote(relative_path.strip("/"), safe="/-_.")
        if query:
            return f"{self.base}/{rel}?{query}"
        return f"{self.base}/{rel}"

    def _request(self, method: str, url: str, data: bytes | None = None) -> tuple[int, bytes, Dict[str, str]]:
        headers = {
            "Authorization": f"Bearer {self.token}",
            "x-ms-version": "2023-11-03",
        }
        if data is not None:
            headers["Content-Type"] = "application/octet-stream"
            headers["Content-Length"] = str(len(data))

        req = request.Request(url=url, data=data, headers=headers, method=method)
        try:
            with request.urlopen(req, timeout=120) as resp:
                return resp.getcode(), resp.read(), dict(resp.headers.items())
        except error.HTTPError as exc:
            body = exc.read()
            return exc.code, body, dict(exc.headers.items())

    def ensure_dir(self, directory: str) -> None:
        normalized = directory.strip("/")
        if not normalized:
            return
        parts = normalized.split("/")
        accum = []
        for part in parts:
            accum.append(part)
            full = "/".join(accum)
            if full in self._created_dirs:
                continue
            code, body, _ = self._request("PUT", self._url(full, "resource=directory"))
            if code not in (201, 409):
                raise RuntimeError(f"Failed creating directory {full}: HTTP {code}, body={body[:400]!r}")
            self._created_dirs.add(full)

    def upload_file(self, local_file: Path, remote_relpath: str, chunk_size: int = DEFAULT_CHUNK_BYTES) -> None:
        remote_relpath = remote_relpath.strip("/")
        remote_dir = str(Path(remote_relpath).parent).replace("\\", "/")
        if remote_dir and remote_dir != ".":
            self.ensure_dir(remote_dir)

        remote_size = self.remote_file_size(remote_relpath)
        local_size = local_file.stat().st_size
        if remote_size is not None and remote_size == local_size:
            return

        # Best-effort delete so reruns can overwrite cleanly.
        self._request("DELETE", self._url(remote_relpath))
        code, body, _ = self._request("PUT", self._url(remote_relpath, "resource=file"))
        if code not in (201, 202):
            raise RuntimeError(f"Failed creating file {remote_relpath}: HTTP {code}, body={body[:400]!r}")

        pos = 0
        with local_file.open("rb") as handle:
            while True:
                chunk = handle.read(chunk_size)
                if not chunk:
                    break
                append_qs = f"action=append&position={pos}"
                code, body, _ = self._request("PATCH", self._url(remote_relpath, append_qs), data=chunk)
                if code not in (202,):
                    raise RuntimeError(
                        f"Failed appending {remote_relpath} at pos {pos}: HTTP {code}, body={body[:400]!r}"
                    )
                pos += len(chunk)

        flush_qs = f"action=flush&position={pos}&close=true"
        code, body, _ = self._request("PATCH", self._url(remote_relpath, flush_qs))
        if code not in (200,):
            raise RuntimeError(f"Failed flushing {remote_relpath}: HTTP {code}, body={body[:400]!r}")

    def remote_file_size(self, remote_relpath: str) -> int | None:
        code, _, headers = self._request("HEAD", self._url(remote_relpath))
        if code == 404:
            return None
        if code not in (200,):
            return None
        # ADLS/OneLake returns Content-Length for paths.
        raw = headers.get("Content-Length") or headers.get("content-length")
        if not raw:
            return None
        try:
            return int(raw)
        except ValueError:
            return None


def latest_timestamp_subdir(parent: Path) -> Path:
    subs = [p for p in parent.iterdir() if p.is_dir()]
    if not subs:
        raise FileNotFoundError(f"No subdirectories found under {parent}")
    return sorted(subs)[-1]


def gather_sources(data_root: Path) -> List[Tuple[Path, str]]:
    pairs: List[Tuple[Path, str]] = []

    def add_glob(base: Path, pattern: str, remote_prefix: str) -> None:
        for p in sorted(base.glob(pattern)):
            if p.is_file():
                pairs.append((p, f"{remote_prefix}/{p.name}"))

    add_glob(data_root / "e-opensky_recent", "*.json", "ingest_full/opensky")
    add_glob(data_root / "e-opensky_recent", "manifest_*.txt", "ingest_full/opensky")

    add_glob(data_root / "f-openflights", "*.dat", "ingest_full/openflights")
    add_glob(data_root / "f-openflights", "manifest_*.txt", "ingest_full/openflights")

    add_glob(data_root / "g-ourairports_recent", "*.csv", "ingest_full/ourairports")
    add_glob(data_root / "g-ourairports_recent", "manifest_*.txt", "ingest_full/ourairports")

    notam_latest = latest_timestamp_subdir(data_root / "h-notam_recent")
    add_glob(notam_latest, "*.json", "ingest_full/notam")
    add_glob(notam_latest, "*.jsonl", "ingest_full/notam")
    add_glob(notam_latest, "*.txt", "ingest_full/notam")

    add_glob(data_root / "i-aviationweather_hazards_recent", "*.gz", "ingest_full/hazards")
    add_glob(data_root / "i-aviationweather_hazards_recent", "manifest_*.txt", "ingest_full/hazards")

    overlay_latest = latest_timestamp_subdir(data_root / "j-synthetic_ops_overlay")
    add_glob(overlay_latest / "synthetic", "*.csv", "ingest_full/synthetic")
    add_glob(overlay_latest, "manifest.json", "ingest_full/synthetic")
    add_glob(overlay_latest / "raw", "*extract.csv", "ingest_full/synthetic")
    add_glob(overlay_latest / "raw", "bts_mishandled_baggage_*.csv", "ingest_full/synthetic")

    schedule_latest = latest_timestamp_subdir(data_root / "k-airline_schedule_feed")
    add_glob(schedule_latest / "raw", "*.zip", "ingest_full/schedule")
    add_glob(schedule_latest / "raw", "*.preview.txt", "ingest_full/schedule")
    add_glob(schedule_latest / "raw", "*.headers.txt", "ingest_full/schedule")

    add_glob(data_root / "c1-asrs/processed", "*.jsonl", "ingest_full/asrs")
    add_glob(data_root / "c1-asrs/processed", "*.json", "ingest_full/asrs")

    add_glob(data_root / "d-easa_ads_recent", "*.tsv", "ingest_full/easa")
    add_glob(data_root / "d-easa_ads_recent/pdfs", "*.pdf", "ingest_full/easa/pdfs")

    for fixed in [data_root / "c2-avall.zip", data_root / "c2-PRE1982.zip"]:
        if fixed.exists():
            pairs.append((fixed, f"ingest_full/ntsb/{fixed.name}"))

    add_glob(data_root / "vector_docs", "*.jsonl", "ingest_full/vector_docs")
    add_glob(data_root / "vector_docs", "*.json", "ingest_full/vector_docs")

    return pairs


def human_bytes(value: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    size = float(value)
    idx = 0
    while size >= 1024 and idx < len(units) - 1:
        size /= 1024
        idx += 1
    return f"{size:.2f} {units[idx]}"


def main() -> None:
    parser = argparse.ArgumentParser(description="Upload curated datasets to Fabric OneLake")
    parser.add_argument("--workspace-id", required=True)
    parser.add_argument("--lakehouse-id", required=True)
    parser.add_argument("--data-root", default=str(DATA_ROOT))
    parser.add_argument("--chunk-bytes", type=int, default=DEFAULT_CHUNK_BYTES)
    parser.add_argument(
        "--bearer-token",
        default=os.getenv("ONELAKE_BEARER_TOKEN", ""),
        help="AAD bearer token for https://storage.azure.com (optional)",
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    data_root = Path(args.data_root).resolve()
    pairs = gather_sources(data_root)
    if not pairs:
        print("No source files found")
        return

    total_bytes = sum(p.stat().st_size for p, _ in pairs)
    print(f"Selected files: {len(pairs)}")
    print(f"Total size: {human_bytes(total_bytes)}")

    if args.dry_run:
        for local, remote in pairs:
            print(f"DRY-RUN {local} -> {remote}")
        return

    token = args.bearer_token.strip() or run_az_token("https://storage.azure.com")
    client = OneLakeClient(args.workspace_id, args.lakehouse_id, token)

    uploaded = 0
    skipped = 0
    uploaded_bytes = 0
    for idx, (local, remote) in enumerate(pairs, start=1):
        size = local.stat().st_size
        existing = client.remote_file_size(remote)
        if existing is not None and existing == size:
            print(f"[{idx}/{len(pairs)}] Skipping {remote} (already uploaded)")
            skipped += 1
            continue
        print(f"[{idx}/{len(pairs)}] Uploading {local} -> {remote} ({human_bytes(size)})")
        client.upload_file(local, remote, chunk_size=args.chunk_bytes)
        uploaded += 1
        uploaded_bytes += size

    print("Upload complete")
    print(f"  files_uploaded: {uploaded}")
    print(f"  files_skipped: {skipped}")
    print(f"  bytes_uploaded: {uploaded_bytes} ({human_bytes(uploaded_bytes)})")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Interrupted")
        sys.exit(130)
