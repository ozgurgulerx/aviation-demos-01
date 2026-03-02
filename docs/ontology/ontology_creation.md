# Ontology Creation Log — AviationOntology V2

> Operational note (2026-02-27): This file is a historical log.  
> The current deterministic workflow is documented in `docs/ontology/ONTOLOGY_RUNTIME_RUNBOOK.md`.

## Goal

Create a Fabric IQ Ontology with 8 entity types, 10 relationships, and data bindings to 8 Lakehouse Delta tables. Then configure the Data Agent to use it.

---

## Timeline

### Phase 1: Lakehouse Tables Loaded (Prior Session)

All 8 Delta tables were loaded into `PostAssignLakehouse1` via `scripts/18_load_lakehouse_direct.py`:

| Table | Entity Type | Row Count |
|---|---|---|
| `bts_ontime_reporting` | BTSFlights | ~500K |
| `airline_delay_causes` | DelayAggregates | ~13K |
| `dim_airports` | Airports | ~8K |
| `dim_airlines` | Airlines | ~6K |
| `ops_flight_legs` | FlightLegs | 500 |
| `ops_crew_rosters` | CrewDuties | 750 |
| `ops_mel_techlog_events` | MaintenanceEvents | 300 |
| `asrs_reports` | SafetyReports | 3K |

### Phase 2: Ontology Provisioned via REST API (Prior Session)

Script: `scripts/provision_fabric_ontology.py`

- Created ontology `AviationOntology` via `POST /v1/workspaces/{ws_id}/ontologies`
- Definition: 38 parts (8 entity type definitions + 8 data bindings + 10 relationship definitions + 10 contextualizations + definition.json + .platform)
- Original ontology ID: `9b5c43e2-c93e-4240-9cc9-f1e48393a2b7`
- Verified via `getDefinition` REST API — all 38 parts confirmed present

### Phase 3: Data Agent Configuration Script Updated (Current Session — 2026-02-27)

Script: `scripts/configure-data-agent.py`

Already had V2 content:
- **Cell 3** (Agent instructions): References all 8 entity types and 10 relationships
- **Cell 8** (Lakehouse table selection): Selects all 8 tables
- **Cell 10** (Ontology instructions): Full V2 description with entities, properties, relationships, synonyms
- **Cell 12** (Test questions): Includes V2-specific questions (crew, maintenance, safety reports)

### Phase 4: Upload & Run configure-data-agent Notebook (Current Session)

Script: `scripts/upload-configure-data-agent.py`

1. **Upload succeeded** — Notebook `configure-data-agent` (ID: `64517a43-d9fc-431b-b838-e989e45d9e38`) updated in workspace via Fabric REST API
2. **API-triggered run failed** — `JobInstanceStatusFailed` with "Job instance failed without detail error"
   - **Root cause**: Data Agent SDK requires interactive Fabric session context (notebook kernel auth). Cannot run headlessly via REST API.
   - **Workaround**: Must run the notebook manually in the Fabric portal (Run all)

### Phase 5: Graph Model Empty — Investigation (Current Session)

**Problem**: Opening `AviationOntology_graph_9b5c43e2...` in Fabric portal showed:
- Nodes (0), Edges (0) — empty canvas
- All 8 tables visible in the Data panel on the right
- Modes: Model, Query. Components: Nodes (0), Edges (0)

**Verification**: REST API `getDefinition` confirmed all 38 parts still present and correct.

**Attempted fix 1 — Graph refresh via portal**:
1. Workspace item list → right-click `AviationOntology_graph_9b5c43e2...` → `...` → Schedule
2. Clicked **Refresh** button
3. Got green toast: "Refresh Started — Beginning refresh for AviationOntology_graph_9b5c43e..."
4. **Result**: Refresh completed but graph still showed Nodes (0), Edges (0)

**Analysis**: The graph model is a child item auto-created when the ontology is provisioned. The "refresh" populates/syncs graph data from Lakehouse tables, but it does NOT build the visual nodes/edges from the ontology definition. The graph model UI and the ontology definition appear to be disconnected — the REST API successfully stores entity types and relationships as definition parts, but the graph model canvas doesn't automatically reflect them.

### Phase 6: Delete & Recreate Ontology (Current Session)

Ran `python3 scripts/provision_fabric_ontology.py --delete`:

1. **Delete succeeded** — `DELETE /v1/workspaces/{ws_id}/ontologies/9b5c43e2-...` returned OK
2. **Immediate re-list still showed the old ontology** (eventual consistency) — script printed "already exists"
3. **Waited 15 seconds**, ran script again without `--delete`
4. **Create succeeded** — New ontology created via LRO (polled 2 cycles, ~40s)
5. **New ontology ID**: `0924977d-f00a-4ce6-941b-2f981f0a2c28`
6. Updated `docs/ontology/fabric_ids.json` with new ID

**Status**: Ontology recreated. Waiting for user to open new graph model in portal and check if nodes/edges are populated.

### Phase 7: Runtime Mismatch Verification (2026-02-27)

Additional API checks confirmed the root problem:

- Ontology item definition exists and contains 32 parts.
- Child graph model item exists, but graph definition has only `.platform` (1 part).
- `queryableGraphType` returns `EntityNotFound`.
- Latest graph refresh for the graph model ended in `Cancelled`.

Conclusion:
- Definition upload is not sufficient to guarantee runtime graph schema readiness.
- Treat API provisioning as definition storage; use portal Publish -> Preview Refresh as the canonical runtime path.

### Phase 8: Runtime Gate Script Added (2026-02-27)

Added `scripts/verify_ontology_runtime.py` and executed it against current IDs.

Result snapshot:
- Preflight checks: PASS (all 8 managed Delta tables present)
- Gate A/B/C/D: FAIL (graph definition is still `.platform` only, queryable graph type missing, latest refresh cancelled, traversal query not executable)

This gives deterministic failure visibility while UI-first recovery is executed.

### Phase 9: Hard Reset + API Iterations (2026-02-28)

Executed hard-reset recovery phases that can be automated from local/API:

1. Baseline snapshot captured:
   - `/tmp/ontology_runtime_before.json`
   - Preflight PASS, Gates A/B/C/D FAIL.

2. Hard reset completed:
   - `python3 scripts/provision_fabric_ontology.py --delete-only`
   - Verified ontology and `AviationOntology_*` child items removed from workspace.

3. Verifier updated for reset-safe preflight:
   - Added `--preflight-only` mode to `scripts/verify_ontology_runtime.py`.
   - Preflight-only check passes after deletion (source tables healthy).

4. Iteration 1 (API recreate):
   - Recreated ontology via `scripts/provision_fabric_ontology.py`.
   - New ontology ID: `9841c850-c575-4216-9cda-93fa080ffc10`.
   - Updated `docs/ontology/fabric_ids.json` with new ontology ID.
   - Runtime gates still FAIL (`Graph definition parts=1`, `queryableGraphType=404`).

5. Iteration 2 (API refresh on graph item):
   - Triggered graph refresh job for new graph model.
   - Refresh status ended `Cancelled`.
   - Runtime gates still FAIL (same pattern).

Current conclusion after iterative automation:
- API-only flow is insufficient to materialize runtime ontology graph.
- Remaining recovery steps are portal-interactive:
  - Open ontology item -> model check -> Publish -> Preview Refresh.
  - Re-run `scripts/verify_ontology_runtime.py` after each portal iteration.

---

## Current State (2026-02-27 ~22:55 UTC)

| Item | Status |
|---|---|
| Lakehouse tables (8) | Loaded |
| Ontology definition (32 parts) | Created (new ID: `0924977d-...`) |
| Graph model visual nodes/edges | **Not ready** — runtime schema mismatch validated (definition-only state) |
| configure-data-agent notebook | Uploaded, needs manual Run all |
| Data Agent configuration | **Not yet done** — blocked on notebook manual run |
| Graph refresh | **Not yet done** — need to trigger on new graph model |

---

## Ontology Definition Summary

### Entity Types (8)

| Entity Type | ID | Source Table | Primary Key | Display Name Property |
|---|---|---|---|---|
| BTSFlights | 1000000000001 | bts_ontime_reporting | Year+Month+DayofMonth+IATA_Code+FlightNum | Flight_Number_Marketing_Airline |
| Airports | 1000000000002 | dim_airports | iata_code | name |
| DelayAggregates | 1000000000003 | airline_delay_causes | carrier+year+month+airport | carrier_name |
| Airlines | 1000000000004 | dim_airlines | iata | name |
| FlightLegs | 1000000000005 | ops_flight_legs | leg_id | flight_no |
| CrewDuties | 1000000000006 | ops_crew_rosters | duty_id | crew_id |
| MaintenanceEvents | 1000000000007 | ops_mel_techlog_events | tech_event_id | jasc_code |
| SafetyReports | 1000000000008 | asrs_reports | asrs_report_id | title |

### Relationships (10)

| Relationship | ID | Source Entity | Target Entity | Join Logic |
|---|---|---|---|---|
| departsFrom | 5000000000001 | BTSFlights | Airports | Origin → iata_code |
| arrivesAt | 5000000000002 | BTSFlights | Airports | Dest → iata_code |
| operatedBy | 5000000000003 | BTSFlights | DelayAggregates | IATA_Code+Year+Month+Origin → carrier+year+month+airport |
| marketedBy | 5000000000004 | BTSFlights | Airlines | IATA_Code_Marketing_Airline → iata |
| legDepartsFrom | 5000000000005 | FlightLegs | Airports | origin_iata → iata_code |
| legArrivesAt | 5000000000006 | FlightLegs | Airports | dest_iata → iata_code |
| crewedBy | 5000000000007 | FlightLegs | CrewDuties | leg_id → duty_id (via ops_crew_rosters) |
| hasMaintenanceEvent | 5000000000008 | FlightLegs | MaintenanceEvents | leg_id → tech_event_id (via ops_mel_techlog_events) |
| flownBy | 5000000000009 | FlightLegs | Airlines | carrier_code → iata (via ops_flight_legs) |
| reportedAt | 5000000000010 | SafetyReports | Airports | location_iata → iata_code (via asrs_reports) |

---

## Key Scripts

| Script | Purpose |
|---|---|
| `scripts/provision_fabric_ontology.py` | Create/delete ontology via Fabric REST API |
| `scripts/verify_ontology_runtime.py` | Runtime gates A-D (graph schema, queryable graph type, refresh status, traversal query) |
| `scripts/configure-data-agent.py` | Configure Data Agent (SDK, runs in Fabric notebook) |
| `scripts/upload-configure-data-agent.py` | Upload configure script as notebook + trigger run |
| `scripts/18_load_lakehouse_direct.py` | Load Delta tables into Lakehouse |

---

## Known Issues & Gotchas

1. **Definition vs runtime mismatch** — API-created ontology definitions can exist while runtime graph schema remains non-queryable (`queryableGraphType` missing, graph definition only `.platform`).

2. **Data Agent SDK requires interactive auth** — Cannot run `configure-data-agent` notebook via Fabric REST API (`RunNotebook` job type). Must open in portal and click "Run all".

3. **Ontology delete has eventual consistency** — After `DELETE` returns, the ontology may still appear in `GET /ontologies` list for 10-20 seconds. Wait before re-creating.

4. **Fabric REST API `getDefinition` requires Content-Length** — POST with empty body returns HTTP 411 unless `Content-Length: 0` header is explicitly set. Returns 202 (LRO) — must poll Location header, then fetch `/result`.

5. **Ontology ID changes on recreate** — After delete+create, the ontology gets a new ID. Must update `docs/ontology/fabric_ids.json` and any scripts referencing the old ID.

---

## Current Recommended Workflow

1. Rebuild ontology source tables in `PostAssignLakehouse1` using `notebooks/prepare_ontology_lakehouse.py` (interactive Fabric notebook).
2. Provision ontology definition (`scripts/provision_fabric_ontology.py`).
3. In Fabric portal, open ontology -> Publish -> Preview Refresh.
4. Run `scripts/verify_ontology_runtime.py` and require all gates to pass.
5. Only then run Data Agent onboarding (`scripts/configure-data-agent.py` / `scripts/upload-configure-data-agent.py`).
