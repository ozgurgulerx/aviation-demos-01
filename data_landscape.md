# Data Landscape

Last verified: 2026-02-23  
Scope: repository code paths + local workspace artifacts in `data/` + local SQLite snapshots.

## 1) Runtime Datastores (used by the app at query time)

| Runtime Source ID | Backing Store | What It Stores | Read Path in Code | Primary Write Path |
| --- | --- | --- | --- | --- |
| `SQL` | Azure PostgreSQL | Structured aviation tables. Core ASRS in `public.asrs_reports` (+ `public.asrs_ingestion_runs`), plus broader demo tables in `demo.*` (airports/runways/routes/NOTAM raw/hazards/synthetic ops/schedule assets). | `src/unified_retriever.py` (`query_sql`, schema introspection over visible schemas `public,demo`) | `scripts/02_load_database.py`, `scripts/09_bulk_load_multisource_postgres.py`, `scripts/14_build_graph_edges.py` |
| `VECTOR_OPS` | Azure AI Search index `idx_ops_narratives` | ASRS narratives + synthetic ops narrative docs (`content`, metadata, `content_vector`) | `src/unified_retriever.py` (`query_semantic`) | `scripts/03_create_search_index.py` (index), then upload docs from `data/vector_docs/ops_narratives_docs.jsonl` |
| `VECTOR_REG` | Azure AI Search index `idx_regulatory` | Regulatory corpus (NOTAM + EASA AD style docs, metadata, vectors) | `src/unified_retriever.py` (`query_semantic`) | `scripts/03_create_search_index.py` + upload `data/vector_docs/regulatory_docs.jsonl` |
| `VECTOR_AIRPORT` | Azure AI Search index `idx_airport_ops_docs` | Airport/runway/navaid/network documents, vectors | `src/unified_retriever.py` (`query_semantic`) | `scripts/03_create_search_index.py` + upload `data/vector_docs/airport_ops_docs.jsonl` |
| `KQL` | Fabric Eventhouse / Kusto | Live/event-window tables: `opensky_states`, `hazards_airsigmets`, `hazards_gairmets`, `hazards_aireps_raw`, optionally `ops_graph_edges` | `src/unified_retriever.py` (`query_kql`) | `scripts/10_push_to_kusto.py` |
| `GRAPH` | Fabric Graph endpoint (live) with PostgreSQL fallback | Relationship graph across Airport/Runway/Route/Airline/NOTAM/ASRS/ops entities | `src/unified_retriever.py` (`query_graph`) | Live endpoint external; fallback graph edges built into `demo.ops_graph_edges` via `scripts/14_build_graph_edges.py` |
| `NOSQL` | Azure Cosmos DB container (preferred) or Fabric NoSQL endpoint fallback | Active NOTAM documents by airport ICAO | `src/unified_retriever.py` (`query_nosql`) | `scripts/15_bulk_load_cosmos_notams.py` (real NOTAMs) or `scripts/13_seed_cosmos_notams.py` (sample seed) |
| `FABRIC_SQL` | Fabric SQL Warehouse | Analytics tables `bts_ontime_reporting`, `airline_delay_causes` | `src/unified_retriever.py` (`query_fabric_sql`) | `scripts/16_load_fabric_sql_warehouse.py` |

## 2) Local/Batch Data Stores (pipeline inputs, staging, snapshots)

| Store | What It Contains | Current Snapshot Notes |
| --- | --- | --- |
| `data/c1-asrs/processed/*.jsonl` | Normalized ASRS structured records + chunked narrative docs | `asrs_records.jsonl`: 150,257 rows; `asrs_documents.jsonl`: 240,605 rows |
| `aviation.db` and `data/c1-asrs/aviation.db` (SQLite) | Local ASRS snapshot (`asrs_reports`, `asrs_ingestion_runs`) | Root `aviation.db` currently has `asrs_reports=150257`, `asrs_ingestion_runs=1` |
| `data/vector_docs/*.jsonl` | Prepared multi-index vector payloads | `ops_narratives_docs=240,778`, `regulatory_docs=475`, `airport_ops_docs=210,853` |
| `data/h-notam_recent/*` | FAA NOTAM JSON/JSONL snapshots | Latest folder includes paged aggregates (e.g. `search_location_us_hubs_all.jsonl` with 390 rows) |
| `data/i-aviationweather_hazards_recent/*` | Aviation weather hazard cache snapshots (AIRMET/SIGMET/PIREP files) | Latest manifest shows `airsigmets`, `gairmets`, `aircraftreports` captures |
| `data/e-opensky_recent/*` | OpenSky state vectors + arrivals/departures snapshots | Includes `opensky_states_all_*.json` + route-specific arrivals/departures |
| `data/f-openflights/raw/*` | OpenFlights network reference datasets (`routes`, `airports`, `airlines`, etc.) | Most recent successful non-empty snapshot is `20260219T165104Z` |
| `data/g-ourairports_recent/*` | OurAirports reference datasets (`airports`, `runways`, `navaids`, `frequencies`) | Most recent successful non-empty snapshot is `20260219T165154Z` |
| `data/j-synthetic_ops_overlay/*` | Synthetic operational overlay tables (`ops_flight_legs`, `ops_graph_edges`, etc.) | Latest manifest (`20260219T180125Z`) reports 173 legs, 519 graph edges |
| `data/k-airline_schedule_feed/*` | BTS on-time zip payloads + delay-causes zip + source metadata | Latest folder `20260219T181602Z` used for Fabric SQL loading |
| `data/d-easa_ads_recent/*` | EASA AD metadata and downloaded PDF references | `downloaded_ads_with_metadata.csv` currently 26 rows |

## 3) Main PostgreSQL Table Map (what is in SQL)

### Core (`public`)
- `public.asrs_reports`: ASRS incident/event rows (id, date, location, aircraft, phase, narrative text, raw JSON, ingestion timestamp).
- `public.asrs_ingestion_runs`: ingestion execution tracking (run status/counts/manifest).

### Demo schema (`demo`, loaded by multi-source bulk loader)
- Reference/network: `ourairports_airports`, `ourairports_runways`, `ourairports_navaids`, `ourairports_frequencies`, `openflights_routes`, `openflights_airports`, `openflights_airlines`.
- Raw operational/regulatory snapshots: `opensky_raw` (`JSONB` payload), `notam_raw` (`JSONB` payload), `hazards_airsigmets`, `hazards_gairmets`, `hazards_aireps_raw`.
- Synthetic ops overlays: `ops_flight_legs`, `ops_turnaround_milestones`, `ops_baggage_events`, `ops_crew_rosters`, `ops_mel_techlog_events`, `ops_graph_edges`.
- Schedule metadata: `schedule_delay_causes`, `schedule_assets`.
- Parsed NOTAM enrichment (from graph build): `notam_parsed`.

## 4) Data Flow (high level)

1. External sources -> `data/*` raw snapshots (ASRS, OpenSky, FAA NOTAM, weather hazards, OpenFlights, OurAirports, BTS, EASA).
2. Normalization/prep -> processed JSONL and synthetic CSV artifacts.
3. Store loading:
   - Structured -> PostgreSQL (`public` + `demo`).
   - Vector -> Azure AI Search indexes (ops/reg/airport).
   - Live/event -> Fabric Eventhouse (KQL tables).
   - Graph -> Fabric Graph endpoint and/or PostgreSQL `ops_graph_edges`.
   - NoSQL -> Cosmos `notams`.
   - Analytics -> Fabric SQL Warehouse tables.
4. Runtime retrieval (`src/unified_retriever.py`) orchestrates across these stores per query.

## 5) Notable Mismatches / Caveats

- `docs/ARCHITECTURE.md` mentions local fallbacks for KQL/Graph/NoSQL; current runtime code uses:
  - Graph fallback to PostgreSQL `ops_graph_edges` (implemented).
  - NoSQL prefers Cosmos and can fallback to Fabric endpoint (implemented).
  - KQL requires configured endpoint (no local-file query fallback path in runtime).
- `README.md` states no local-file fallback mechanisms; this is consistent with runtime behavior for KQL, but Graph does have a PostgreSQL fallback.
- `scripts/03_create_search_index.py` is multi-index (`idx_ops_narratives`, `idx_regulatory`, `idx_airport_ops_docs`), while `scripts/04_upload_documents.py` defaults to single index `aviation-index` unless overridden via `AZURE_SEARCH_INDEX_NAME`.

