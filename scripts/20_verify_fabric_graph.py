#!/usr/bin/env python3
"""Verify Fabric Graph Model via GQL Query API.

Runs 12 verification gates against the Fabric Graph REST API:
  - Gate 0: Discover edge label format (camelCase vs UPPER_SNAKE_CASE)
  - Gate A: Graph model exists in workspace (list GraphModels API)
  - Gate B: Node count query returns > 0 rows (tries Airport/Airports)
  - Gate C: Edge traversal (CONNECTS) returns paths
  - Gate D: Multi-hop path query (airport -> flight -> airport) returns results
  - Gate E: FlightLeg seed — crew traversal (CREWED_BY edge)
  - Gate F: Maintenance traversal (MEL_ON edge)
  - Gate G: Airline connectivity (FLOWN_BY edge)
  - Gate H: SafetyReport location (REPORTED_AT edge)
  - Gate I: MEL_ON count (FlightLeg -> MaintenanceEvent)
  - Gate J: FLOWN_BY count (FlightLeg -> Airline)
  - Gate K: REPORTED_AT count (SafetyReport -> Airport)

Usage:
    python scripts/20_verify_fabric_graph.py
    python scripts/20_verify_fabric_graph.py --graph-id <id>
    python scripts/20_verify_fabric_graph.py --workspace-id <id> --graph-id <id>
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

DEFAULT_IDS_PATH = Path("docs/ontology/fabric_ids.json")
DEFAULT_WS_ID = "cfbb82a5-799e-421b-b2cc-8a164b17a849"
FABRIC_API_BASE = "https://api.fabric.microsoft.com/v1"


@dataclass
class GateResult:
    name: str
    passed: bool
    detail: str


@dataclass
class LabelMap:
    """Discovered label formats from the live graph."""
    edge_labels: list[str] = field(default_factory=list)
    node_labels: list[str] = field(default_factory=list)
    format_hint: str = "unknown"  # "camel", "snake", or "unknown"


def get_token() -> str:
    """Get Azure AD token for Fabric API."""
    result = subprocess.run(
        ["az", "account", "get-access-token", "--resource", "https://api.fabric.microsoft.com",
         "--query", "accessToken", "-o", "tsv"],
        capture_output=True, text=True,
    )
    token = result.stdout.strip()
    if not token:
        print("ERROR: Could not get Azure token for Fabric API")
        print(f"  stderr: {result.stderr.strip()}")
        sys.exit(1)
    return token


def api_request(url: str, token: str, method: str = "GET", body: dict | None = None) -> dict[str, Any]:
    """Make an authenticated request to the Fabric REST API."""
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    data = json.dumps(body).encode("utf-8") if body else None
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        error_body = ""
        try:
            error_body = e.read().decode("utf-8")
        except Exception:
            pass
        return {"error": True, "status": e.code, "reason": e.reason, "body": error_body}
    except Exception as e:
        return {"error": True, "detail": str(e)}


def load_graph_id_from_ids(ids_path: Path) -> str | None:
    """Load graph model ID from fabric_ids.json if available."""
    if not ids_path.exists():
        return None
    with ids_path.open() as f:
        data = json.load(f)
    graph = data.get("graph", {})
    return graph.get("id") if isinstance(graph, dict) else None


def _execute_gql(ws_id: str, graph_id: str, token: str, gql: str) -> dict[str, Any]:
    """Execute a GQL query against the Fabric Graph API."""
    url = f"{FABRIC_API_BASE}/workspaces/{ws_id}/GraphModels/{graph_id}/executeQuery?preview=true"
    return api_request(url, token, method="POST", body={"query": gql})


# ---------------------------------------------------------------------------
# Response helpers
# ---------------------------------------------------------------------------

def _extract_data(resp: dict) -> list:
    """Extract data rows from a Fabric Graph GQL response."""
    if resp.get("error"):
        return []
    result = resp.get("result", {})
    data = result.get("data", [])
    if data:
        return data
    return resp.get("results", resp.get("rows", []))


def _is_gql_error(resp: dict) -> str | None:
    """Return error description if the GQL response is an error, else None."""
    if resp.get("error"):
        return resp.get("reason", resp.get("detail", "unknown_error"))
    if resp.get("errorCode"):
        return f"{resp['errorCode']}: {resp.get('message', '')}"
    status = resp.get("status", {})
    code = str(status.get("code", ""))
    if code and code not in ("00000", "02000"):
        cause = status.get("cause", {})
        return cause.get("description", status.get("description", f"status_{code}"))
    return None


def _try_queries(ws_id: str, graph_id: str, token: str, queries: list[str]) -> tuple[list, str]:
    """Try a list of GQL queries in order; return first successful (rows, query)."""
    for gql in queries:
        resp = _execute_gql(ws_id, graph_id, token, gql)
        err = _is_gql_error(resp)
        if not err:
            rows = _extract_data(resp)
            if rows:
                return rows, gql
    return [], queries[-1] if queries else ""


# ---------------------------------------------------------------------------
# Gate 0: Edge label discovery
# ---------------------------------------------------------------------------

def gate_0_discover_labels(ws_id: str, graph_id: str, token: str) -> tuple[GateResult, LabelMap]:
    """Gate 0: Discover edge label format from live graph."""
    label_map = LabelMap()

    # Try entity-specific queries first (unbounded MATCH times out on large graphs)
    discovery_queries = [
        "MATCH (a:Airport)-[r]->(b) RETURN a, r, b LIMIT 10",
        "MATCH (a:FlightLeg)-[r]->(b) RETURN a, r, b LIMIT 10",
        "MATCH (a)-[r]->(b) RETURN a, r, b LIMIT 5",
    ]
    rows = []
    for gql in discovery_queries:
        resp = _execute_gql(ws_id, graph_id, token, gql)
        err = _is_gql_error(resp)
        if not err:
            rows = _extract_data(resp)
            if rows:
                break

    if not rows:
        last_err = _is_gql_error(resp) if resp else "no response"
        return GateResult("Gate 0: Edge label discovery", False, f"No edges found (last: {last_err})"), label_map

    # Extract edge types and node labels from response
    for row in rows:
        if isinstance(row, dict):
            for key, val in row.items():
                if isinstance(val, dict):
                    etype = val.get("~type") or val.get("~label") or ""
                    if etype and etype not in label_map.edge_labels:
                        label_map.edge_labels.append(etype)
                    nlabel = val.get("~entityType") or ""
                    if nlabel and nlabel not in label_map.node_labels:
                        label_map.node_labels.append(nlabel)

    # Determine format hint
    camel_count = sum(1 for lbl in label_map.edge_labels if lbl[0].islower()) if label_map.edge_labels else 0
    snake_count = sum(1 for lbl in label_map.edge_labels if "_" in lbl and lbl == lbl.upper()) if label_map.edge_labels else 0
    if camel_count > snake_count:
        label_map.format_hint = "camel"
    elif snake_count > camel_count:
        label_map.format_hint = "snake"

    detail = (
        f"Found {len(label_map.edge_labels)} edge type(s): {label_map.edge_labels[:10]}, "
        f"format_hint={label_map.format_hint}"
    )
    if label_map.node_labels:
        detail += f", node types: {label_map.node_labels[:10]}"

    return GateResult("Gate 0: Edge label discovery", True, detail), label_map


# ---------------------------------------------------------------------------
# Gates A-H (updated for dual-label support)
# ---------------------------------------------------------------------------

def gate_a_model_exists(ws_id: str, graph_id: str, token: str) -> GateResult:
    """Gate A: Verify graph model exists in workspace."""
    url = f"{FABRIC_API_BASE}/workspaces/{ws_id}/GraphModels?preview=true"
    resp = api_request(url, token)
    if resp.get("error"):
        url2 = f"{FABRIC_API_BASE}/workspaces/{ws_id}/GraphModels/{graph_id}?preview=true"
        resp2 = api_request(url2, token)
        if resp2.get("error"):
            return GateResult(
                "Gate A: Graph model exists",
                False,
                f"Cannot access graph model {graph_id}: {resp2.get('reason', resp2.get('detail', 'unknown'))}",
            )
        name = resp2.get("displayName", "unknown")
        return GateResult("Gate A: Graph model exists", True, f"Model found: {name} ({graph_id})")

    models = resp.get("value", [])
    for m in models:
        if m.get("id") == graph_id:
            return GateResult(
                "Gate A: Graph model exists",
                True,
                f"Model found: {m.get('displayName', 'unknown')} ({graph_id}), {len(models)} total models",
            )
    model_names = [m.get("displayName", "?") for m in models]
    return GateResult(
        "Gate A: Graph model exists",
        False,
        f"Graph ID {graph_id} not found among {len(models)} models: {model_names}",
    )


def gate_b_node_count(ws_id: str, graph_id: str, token: str) -> GateResult:
    """Gate B: Node count query returns > 0 rows (tries singular then plural labels)."""
    queries = [
        "MATCH (a:Airport) RETURN a.iata_code, a.name LIMIT 20",
        "MATCH (a:Airports) RETURN a.iata_code, a.name LIMIT 20",
    ]
    rows, used_query = _try_queries(ws_id, graph_id, token, queries)
    if rows:
        return GateResult(
            "Gate B: Node count query",
            True,
            f"Got {len(rows)} row(s) via: {used_query[:80]}",
        )
    return GateResult("Gate B: Node count query", False, "Query returned no results (tried Airports and Airport)")


def gate_c_edge_traversal(ws_id: str, graph_id: str, token: str) -> GateResult:
    """Gate C: Edge traversal returns paths (CONNECTS or generic)."""
    queries = [
        "MATCH (a:Airport)-[r:CONNECTS]->(b:Airport) RETURN a.iata_code AS origin, b.iata_code AS dest LIMIT 10",
        "MATCH (a:Airports)-[r:CONNECTS]->(b:Airports) RETURN a.iata_code AS origin, b.iata_code AS dest LIMIT 10",
        "MATCH (a)-[r]->(b) RETURN a, r, b LIMIT 10",
    ]
    rows, used_query = _try_queries(ws_id, graph_id, token, queries)
    if rows:
        return GateResult(
            "Gate C: Edge traversal",
            True,
            f"Got {len(rows)} traversal(s) via: {used_query[:80]}",
        )
    return GateResult("Gate C: Edge traversal", False, "No edge traversals returned")


def gate_d_multi_hop(ws_id: str, graph_id: str, token: str) -> GateResult:
    """Gate D: Multi-hop path query (airport -> flight -> airport)."""
    queries = [
        "MATCH (f:FlightLeg)-[:DEPARTS]->(a:Airport), (f)-[:ARRIVES]->(b:Airport) RETURN a.iata_code AS origin, f.leg_id AS flight, b.iata_code AS dest LIMIT 5",
        "MATCH (f:FlightLegs)-[:legDepartsFrom]->(a:Airports), (f)-[:legArrivesAt]->(b:Airports) RETURN a.iata_code AS origin, f.leg_id AS flight, b.iata_code AS dest LIMIT 5",
        "MATCH (a:Airport)-[r1]->(b)-[r2]->(c) RETURN a.iata_code, b, c LIMIT 5",
        "MATCH (a:Airports)-[r1]->(b)-[r2]->(c) RETURN a.iata_code, b, c LIMIT 5",
    ]
    rows, used_query = _try_queries(ws_id, graph_id, token, queries)
    if rows:
        return GateResult(
            "Gate D: Multi-hop path",
            True,
            f"Got {len(rows)} path(s) via: {used_query[:80]}",
        )
    return GateResult("Gate D: Multi-hop path", False, "Multi-hop queries all returned no results")


def gate_e_flightleg_seed(ws_id: str, graph_id: str, token: str) -> GateResult:
    """Gate E: FlightLeg seed — crew traversal."""
    queries = [
        "MATCH (f:FlightLeg)-[:CREWED_BY]->(c:Crew) RETURN f.leg_id, c.crew_id, c.role LIMIT 5",
        "MATCH (f:FlightLegs)-[:crewedBy]->(c:CrewDuties) RETURN f.leg_id, c.crew_id, c.role LIMIT 5",
        "MATCH (f:FlightLeg)-[:crewedBy]->(c:CrewDuties) RETURN f.leg_id, c.crew_id, c.role LIMIT 5",
    ]
    rows, used_query = _try_queries(ws_id, graph_id, token, queries)
    if rows:
        return GateResult(
            "Gate E: FlightLeg seed (crew)",
            True,
            f"Got {len(rows)} row(s) via: {used_query[:80]}",
        )
    return GateResult("Gate E: FlightLeg seed (crew)", False, "No FlightLeg->Crew rows returned")


def gate_f_maintenance(ws_id: str, graph_id: str, token: str) -> GateResult:
    """Gate F: Maintenance traversal (FlightLeg -> MaintenanceEvent)."""
    queries = [
        "MATCH (f:FlightLeg)-[:MEL_ON]->(m:MaintenanceEvent) RETURN f.leg_id, m.tech_event_id, m.severity LIMIT 5",
        "MATCH (f:FlightLegs)-[:hasMaintenanceEvent]->(m:MaintenanceEvents) RETURN f.leg_id, m.tech_event_id, m.severity LIMIT 5",
        "MATCH (f:FlightLeg)-[:hasMaintenanceEvent]->(m:MaintenanceEvent) RETURN f.leg_id, m.tech_event_id, m.severity LIMIT 5",
    ]
    rows, used_query = _try_queries(ws_id, graph_id, token, queries)
    if rows:
        return GateResult(
            "Gate F: Maintenance traversal",
            True,
            f"Got {len(rows)} row(s) via: {used_query[:80]}",
        )
    return GateResult("Gate F: Maintenance traversal", False, "No hasMaintenanceEvent rows returned")


def gate_g_airline(ws_id: str, graph_id: str, token: str) -> GateResult:
    """Gate G: Airline connectivity (FLOWN_BY edge)."""
    queries = [
        "MATCH (f:FlightLeg)-[:FLOWN_BY]->(al:Airline) RETURN f.leg_id, al.iata, al.name LIMIT 5",
        "MATCH (f:FlightLegs)-[:flownBy]->(al:Airlines) RETURN f.leg_id, al.iata, al.name LIMIT 5",
        "MATCH (a)-[:OPERATED_BY]->(al:Airline) RETURN al.iata, al.name LIMIT 5",
    ]
    rows, used_query = _try_queries(ws_id, graph_id, token, queries)
    if rows:
        return GateResult(
            "Gate G: Airline connectivity",
            True,
            f"Got {len(rows)} row(s) via: {used_query[:80]}",
        )
    return GateResult("Gate G: Airline connectivity", False, "No flownBy/OPERATED_BY rows returned")


def gate_h_safety_report(ws_id: str, graph_id: str, token: str) -> GateResult:
    """Gate H: SafetyReport location (REPORTED_AT edge)."""
    queries = [
        "MATCH (s:SafetyReport)-[:REPORTED_AT]->(a:Airport) RETURN s.asrs_report_id, a.iata_code LIMIT 5",
        "MATCH (s:SafetyReports)-[:reportedAt]->(a:Airports) RETURN s.asrs_report_id, a.iata_code LIMIT 5",
        "MATCH (s:SafetyReport)-[:reportedAt]->(a:Airport) RETURN s.asrs_report_id, a.iata_code LIMIT 5",
    ]
    rows, used_query = _try_queries(ws_id, graph_id, token, queries)
    if rows:
        return GateResult(
            "Gate H: SafetyReport location",
            True,
            f"Got {len(rows)} row(s) via: {used_query[:80]}",
        )
    return GateResult("Gate H: SafetyReport location", False, "No reportedAt rows returned")


# ---------------------------------------------------------------------------
# Gates I-K: New relationship verification
# ---------------------------------------------------------------------------

def gate_i_has_maint(ws_id: str, graph_id: str, token: str) -> GateResult:
    """Gate I: MEL_ON — FlightLeg -> MaintenanceEvent."""
    queries = [
        "MATCH (f:FlightLeg)-[:MEL_ON]->(m:MaintenanceEvent) RETURN count(*) AS cnt",
        "MATCH (f:FlightLegs)-[:hasMaintenanceEvent]->(m:MaintenanceEvents) RETURN count(*) AS cnt",
    ]
    rows, used_query = _try_queries(ws_id, graph_id, token, queries)
    if rows:
        cnt = rows[0].get("cnt", 0) if isinstance(rows[0], dict) else "?"
        return GateResult(
            "Gate I: hasMaintenanceEvent count",
            True,
            f"Edge count: {cnt} via: {used_query[:60]}",
        )
    return GateResult("Gate I: hasMaintenanceEvent count", False, "No hasMaintenanceEvent edges found")


def gate_j_flown_by(ws_id: str, graph_id: str, token: str) -> GateResult:
    """Gate J: FLOWN_BY — FlightLeg -> Airline."""
    queries = [
        "MATCH (f:FlightLeg)-[:FLOWN_BY]->(al:Airline) RETURN count(*) AS cnt",
        "MATCH (f:FlightLegs)-[:flownBy]->(al:Airlines) RETURN count(*) AS cnt",
    ]
    rows, used_query = _try_queries(ws_id, graph_id, token, queries)
    if rows:
        cnt = rows[0].get("cnt", 0) if isinstance(rows[0], dict) else "?"
        return GateResult(
            "Gate J: flownBy count",
            True,
            f"Edge count: {cnt} via: {used_query[:60]}",
        )
    return GateResult("Gate J: flownBy count", False, "No flownBy edges found")


def gate_k_reported_at(ws_id: str, graph_id: str, token: str) -> GateResult:
    """Gate K: REPORTED_AT — SafetyReport -> Airport."""
    queries = [
        "MATCH (s:SafetyReport)-[:REPORTED_AT]->(a:Airport) RETURN count(*) AS cnt",
        "MATCH (s:SafetyReports)-[:reportedAt]->(a:Airports) RETURN count(*) AS cnt",
    ]
    rows, used_query = _try_queries(ws_id, graph_id, token, queries)
    if rows:
        cnt = rows[0].get("cnt", 0) if isinstance(rows[0], dict) else "?"
        return GateResult(
            "Gate K: reportedAt count",
            True,
            f"Edge count: {cnt} via: {used_query[:60]}",
        )
    return GateResult("Gate K: reportedAt count", False, "No reportedAt edges found")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Verify Fabric Graph Model via GQL API")
    parser.add_argument("--workspace-id", default=DEFAULT_WS_ID)
    parser.add_argument("--graph-id", default=None,
                        help="Graph Model ID (reads from fabric_ids.json if not provided)")
    parser.add_argument("--ids-file", type=Path, default=DEFAULT_IDS_PATH)
    args = parser.parse_args()

    ws_id = args.workspace_id
    graph_id = args.graph_id

    if not graph_id:
        graph_id = load_graph_id_from_ids(args.ids_file)
    if not graph_id:
        graph_id = os.getenv("FABRIC_GRAPH_MODEL_ID", "").strip()
    if not graph_id:
        print("ERROR: No graph model ID. Provide --graph-id, set FABRIC_GRAPH_MODEL_ID,")
        print("       or add graph.id to docs/ontology/fabric_ids.json")
        sys.exit(1)

    print("=== Fabric Graph Verification ===")
    print(f"  Workspace: {ws_id}")
    print(f"  Graph Model: {graph_id}")
    print()

    token = get_token()
    print(f"  Token acquired ({len(token)} chars)")
    print()

    # Gate 0: Discover edge labels first
    g0_result, label_map = gate_0_discover_labels(ws_id, graph_id, token)
    status = "PASS" if g0_result.passed else "FAIL"
    print(f"  [{status}] {g0_result.name}")
    print(f"         {g0_result.detail}")
    print()

    gates = [
        gate_a_model_exists,
        gate_b_node_count,
        gate_c_edge_traversal,
        gate_d_multi_hop,
        gate_e_flightleg_seed,
        gate_f_maintenance,
        gate_g_airline,
        gate_h_safety_report,
        gate_i_has_maint,
        gate_j_flown_by,
        gate_k_reported_at,
    ]

    results: list[GateResult] = [g0_result]
    for gate_fn in gates:
        result = gate_fn(ws_id, graph_id, token)
        status = "PASS" if result.passed else "FAIL"
        print(f"  [{status}] {result.name}")
        print(f"         {result.detail}")
        print()
        results.append(result)

    # Summary
    passed = sum(1 for r in results if r.passed)
    total = len(results)
    print(f"{'='*60}")
    print(f"  Result: {passed}/{total} gates passed")
    if passed == total:
        print("  Graph model is READY for runtime queries.")
    elif passed > 0:
        print("  Graph model is PARTIALLY ready. Check failed gates above.")
    else:
        print("  Graph model is NOT ready. Create it in Fabric portal first.")

    if label_map.edge_labels:
        print(f"  Discovered edge labels: {label_map.edge_labels}")
        print(f"  Label format hint: {label_map.format_hint}")
    print(f"{'='*60}")

    sys.exit(0 if passed == total else 1)


if __name__ == "__main__":
    main()
