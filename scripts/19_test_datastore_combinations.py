#!/usr/bin/env python3
"""
Exhaustive datastore-combination tests against backend /api/chat SSE endpoint.

Designed for long-running cloud validation with resumable JSON output.
"""

from __future__ import annotations

import argparse
import itertools
import json
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple

import requests

SOURCES: List[str] = [
    "SQL",
    "KQL",
    "GRAPH",
    "VECTOR_OPS",
    "VECTOR_REG",
    "VECTOR_AIRPORT",
    "NOSQL",
    "FABRIC_SQL",
]


@dataclass
class RunResult:
    combo_index: int
    combo: List[str]
    policy: str
    status: str
    elapsed_ms: float
    fired_sources: List[str] = field(default_factory=list)
    missing_required: List[str] = field(default_factory=list)
    unexpected_sources: List[str] = field(default_factory=list)
    failed_sources: List[str] = field(default_factory=list)
    row_counts: Dict[str, int] = field(default_factory=dict)
    terminal_type: Optional[str] = None
    terminal_route: Optional[str] = None
    required_sources_satisfied: Optional[bool] = None
    missing_required_sources_meta: List[str] = field(default_factory=list)
    source_policy_meta: Optional[str] = None
    tool_result_seen: bool = False
    retrieval_plan_seen: bool = False
    error: Optional[str] = None


def generate_combos(sources: Iterable[str]) -> List[Tuple[str, ...]]:
    src = list(sources)
    out: List[Tuple[str, ...]] = []
    for r in range(1, len(src) + 1):
        out.extend(itertools.combinations(src, r))
    return out


def parse_data_line(line: str) -> Optional[Dict]:
    line = line.strip()
    if not line.startswith("data:"):
        return None
    payload = line[5:].strip()
    if not payload:
        return None
    try:
        return json.loads(payload)
    except json.JSONDecodeError:
        return None


def run_one(
    backend: str,
    combo: Tuple[str, ...],
    combo_index: int,
    policy: str,
    source_policy: str,
    retrieval_mode: str,
    query_profile: str,
    timeout_seconds: int,
    stop_after: str,
) -> RunResult:
    required = list(combo)
    result = RunResult(
        combo_index=combo_index,
        combo=required,
        policy=policy,
        status="ERROR",
        elapsed_ms=0.0,
    )

    fired: Set[str] = set()
    failed: Set[str] = set()
    row_counts: Dict[str, int] = {}

    payload = {
        "message": "connectivity probe",
        "required_sources": required,
        "source_policy": source_policy,
        "failure_policy": policy,
        "retrieval_mode": retrieval_mode,
        "query_profile": query_profile,
        "explain_retrieval": False,
    }

    t0 = time.perf_counter()
    resp = None
    try:
        resp = requests.post(
            f"{backend.rstrip('/')}/api/chat",
            json=payload,
            headers={"Accept": "text/event-stream"},
            stream=True,
            timeout=timeout_seconds,
        )
        if resp.status_code != 200:
            result.error = f"HTTP {resp.status_code}: {resp.text[:200]}"
            result.elapsed_ms = (time.perf_counter() - t0) * 1000
            return result

        terminal_types = {"agent_done", "agent_partial_done", "agent_error", "done", "error"}
        stop_after = stop_after.lower().strip()

        for raw in resp.iter_lines(decode_unicode=True):
            if not raw:
                continue
            event = parse_data_line(raw)
            if not event:
                continue

            etype = str(event.get("type", ""))
            if etype == "source_call_start":
                source = str(event.get("source", "")).upper().strip()
                if source:
                    fired.add(source)
            elif etype == "source_call_done":
                source = str(event.get("source", "")).upper().strip()
                if source:
                    fired.add(source)
                count = int(event.get("row_count") or 0)
                if source:
                    row_counts[source] = max(count, row_counts.get(source, 0))
                contract_failed = str(event.get("contract_status", "")).lower() == "failed"
                err = event.get("error") or event.get("error_code")
                if source and (contract_failed or err):
                    failed.add(source)
            elif etype == "retrieval_plan":
                result.retrieval_plan_seen = True
            elif etype == "tool_result":
                result.tool_result_seen = True
                if stop_after == "tool_result":
                    break
            elif etype in terminal_types:
                result.terminal_type = etype
                result.terminal_route = str(event.get("route", "") or "") or None
                if "requiredSourcesSatisfied" in event:
                    result.required_sources_satisfied = bool(event.get("requiredSourcesSatisfied"))
                if "missingRequiredSources" in event:
                    result.missing_required_sources_meta = [str(x).upper() for x in (event.get("missingRequiredSources") or [])]
                if "sourcePolicy" in event:
                    result.source_policy_meta = str(event.get("sourcePolicy") or "")
                if stop_after == "terminal":
                    break

    except requests.exceptions.Timeout:
        result.error = f"timeout>{timeout_seconds}s"
    except requests.exceptions.RequestException as exc:
        result.error = f"request_error:{exc}"
    finally:
        if resp is not None:
            resp.close()

    result.elapsed_ms = (time.perf_counter() - t0) * 1000
    result.fired_sources = sorted(fired)
    result.failed_sources = sorted(failed)
    result.row_counts = row_counts

    req = set(required)
    result.missing_required = sorted(req - fired)
    result.unexpected_sources = sorted(fired - req)

    # Connectivity + policy assertions for this run.
    if result.error:
        result.status = "ERROR"
        return result

    if result.missing_required:
        result.status = "FAIL"
        return result

    if source_policy == "exact" and result.unexpected_sources:
        result.status = "FAIL"
        return result

    if stop_after == "terminal" and result.terminal_type is None:
        result.status = "FAIL"
        result.error = "missing_terminal_event"
        return result

    result.status = "PASS"
    return result


def summarize(results: List[RunResult]) -> Dict:
    status_counts: Dict[str, int] = {}
    total_ms = 0.0
    source_failures: Dict[str, int] = {}
    exactness_failures = 0
    missing_required_failures = 0

    for r in results:
        status_counts[r.status] = status_counts.get(r.status, 0) + 1
        total_ms += r.elapsed_ms
        if r.unexpected_sources:
            exactness_failures += 1
        if r.missing_required:
            missing_required_failures += 1
        for src in r.failed_sources:
            source_failures[src] = source_failures.get(src, 0) + 1

    return {
        "total_runs": len(results),
        "status_counts": status_counts,
        "avg_elapsed_ms": (total_ms / len(results)) if results else 0.0,
        "exactness_failures": exactness_failures,
        "missing_required_failures": missing_required_failures,
        "source_failures": dict(sorted(source_failures.items(), key=lambda kv: (-kv[1], kv[0]))),
    }


def load_existing(path: Path) -> List[RunResult]:
    if not path.exists():
        return []
    data = json.loads(path.read_text())
    items = data.get("results", []) if isinstance(data, dict) else []
    out: List[RunResult] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        out.append(RunResult(**item))
    return out


def save_output(path: Path, args: argparse.Namespace, results: List[RunResult]) -> None:
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "config": {
            "backend": args.backend,
            "policies": args.policies,
            "source_policy": args.source_policy,
            "retrieval_mode": args.retrieval_mode,
            "query_profile": args.query_profile,
            "timeout": args.timeout,
            "stop_after": args.stop_after,
            "start_index": args.start_index,
            "max_combos": args.max_combos,
        },
        "sources": SOURCES,
        "summary": summarize(results),
        "results": [asdict(r) for r in results],
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2))


def main() -> int:
    parser = argparse.ArgumentParser(description="Exhaustive datastore-combination tests")
    parser.add_argument("--backend", required=True, help="Backend base URL, e.g. http://127.0.0.1:5001")
    parser.add_argument("--policies", default="strict,graceful", help="Comma-separated failure policies")
    parser.add_argument("--source-policy", default="exact", choices=["include", "exact"])
    parser.add_argument("--retrieval-mode", default="code-rag", choices=["code-rag", "foundry-iq"])
    parser.add_argument("--query-profile", default="pilot-brief")
    parser.add_argument("--timeout", type=int, default=120, help="Per-request timeout seconds")
    parser.add_argument("--stop-after", default="tool_result", choices=["tool_result", "terminal"])
    parser.add_argument("--start-index", type=int, default=1, help="1-based combo index start")
    parser.add_argument("--max-combos", type=int, default=255, help="Number of combos to run from start-index")
    parser.add_argument("--sleep-ms", type=int, default=0)
    parser.add_argument("--out", default="artifacts/datastore_combo_results.json")
    parser.add_argument("--resume", action="store_true")
    args = parser.parse_args()

    policies = [p.strip().lower() for p in args.policies.split(",") if p.strip()]
    policies = [p for p in policies if p in {"strict", "graceful"}]
    if not policies:
        raise SystemExit("No valid policies provided")

    combos = generate_combos(SOURCES)
    start_idx = max(1, args.start_index)
    end_idx = min(len(combos), start_idx + max(0, args.max_combos) - 1)

    out_path = Path(args.out)
    results: List[RunResult] = load_existing(out_path) if args.resume else []
    seen = {(r.combo_index, tuple(r.combo), r.policy) for r in results}

    print("=" * 90)
    print("DATASTORE COMBINATION TEST")
    print("=" * 90)
    print(f"backend={args.backend}")
    print(
        f"source_policy={args.source_policy} retrieval_mode={args.retrieval_mode} "
        f"query_profile={args.query_profile} stop_after={args.stop_after} policies={policies}"
    )
    print(f"combo_range={start_idx}..{end_idx} total_combos={len(combos)}")
    print(f"existing_results={len(results)} resume={args.resume}")

    run_count = 0
    total_target = (end_idx - start_idx + 1) * len(policies)

    for combo_index in range(start_idx, end_idx + 1):
        combo = combos[combo_index - 1]
        for policy in policies:
            key = (combo_index, tuple(combo), policy)
            if key in seen:
                continue

            run_count += 1
            print(
                f"[{run_count}/{total_target}] combo#{combo_index} policy={policy} "
                f"sources={'+'.join(combo)}"
            )
            r = run_one(
                backend=args.backend,
                combo=combo,
                combo_index=combo_index,
                policy=policy,
                source_policy=args.source_policy,
                retrieval_mode=args.retrieval_mode,
                query_profile=args.query_profile,
                timeout_seconds=args.timeout,
                stop_after=args.stop_after,
            )
            print(
                f"  -> {r.status} elapsed={r.elapsed_ms:.0f}ms "
                f"fired={len(r.fired_sources)} missing={len(r.missing_required)} "
                f"unexpected={len(r.unexpected_sources)} failed={len(r.failed_sources)}"
            )
            if r.error:
                print(f"     error={r.error}")
            if r.unexpected_sources:
                print(f"     unexpected={','.join(r.unexpected_sources)}")
            if r.missing_required:
                print(f"     missing_required={','.join(r.missing_required)}")

            results.append(r)
            save_output(out_path, args, results)

            if args.sleep_ms > 0:
                time.sleep(args.sleep_ms / 1000.0)

    summary = summarize(results)
    print("-" * 90)
    print("SUMMARY")
    print(json.dumps(summary, indent=2))
    print(f"saved={out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
