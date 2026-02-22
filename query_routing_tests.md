# Query Routing Test Documentation

Comprehensive test matrix for the aviation RAG query routing and source activation pipeline.

**Test script:** `scripts/17_test_all_sources.py`
**Last updated:** 2026-02-22

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Layer 1: Route Classification Tests](#layer-1-route-classification-tests)
3. [Layer 2: Source Activation Tests](#layer-2-source-activation-tests)
4. [Layer 3: Combined Routing + Source Activation](#layer-3-combined-routing--source-activation)
5. [Original Source Coverage Tests](#original-source-coverage-tests)
6. [Multi-Source Overlap Tests](#multi-source-overlap-tests)
7. [Keyword Gap Analysis](#keyword-gap-analysis)
8. [Edge Cases and Boundary Conditions](#edge-cases-and-boundary-conditions)
9. [Cross-Source Conflict Resolution](#cross-source-conflict-resolution)
10. [Recommendations for Routing Improvements](#recommendations-for-routing-improvements)

---

## Architecture Overview

The routing pipeline has three layers that determine which data sources fire for a given query:

```
User Query
    |
    v
Layer 1: query_router.py
    quick_route()  -- keyword heuristic -> SQL / SEMANTIC / HYBRID
    route()        -- LLM-based JSON output (fallback: HYBRID)
    smart_route()  -- heuristic-first, escalate HYBRID to LLM
    |
    v
Layer 2: retrieval_plan.py
    build_retrieval_plan()
        1. Router-provided sources (if any)
        2. Baseline by route (SQL->SQL, SEMANTIC->VECTOR_OPS, HYBRID->both)
        3. Profile-driven enrichments (pilot-brief, compliance, etc.)
        4. Query-driven _wants_* activation (7 heuristic functions)
        5. Fallback guarantee (SQL + VECTOR_OPS if nothing selected)
    |
    v
Layer 3: agentic_orchestrator.py (agentic path only)
    LLM-planned tool calls based on intent graph
    Falls back to heuristic planning on error
```

### Valid Sources

| Source | Backend | Purpose |
|--------|---------|---------|
| `SQL` | PostgreSQL | Structured queries, counts, rankings, filters |
| `KQL` | Fabric Eventhouse | Real-time weather (METAR/TAF), live flights, hazards |
| `GRAPH` | Fabric Graph | Dependency paths, impact analysis, alternate airports |
| `VECTOR_OPS` | AI Search (idx_ops_narratives) | Incident reports, near-miss narratives |
| `VECTOR_REG` | AI Search (idx_regulatory) | NOTAMs, ADs, EASA/FAA bulletins |
| `VECTOR_AIRPORT` | AI Search (idx_airport_ops_docs) | Runway specs, station info, facilities |
| `NOSQL` | Cosmos DB | Operational documents, NOTAMs, ground handling |
| `FABRIC_SQL` | Fabric SQL Warehouse | BTS on-time analytics, delay causes |

---

## Layer 1: Route Classification Tests

These tests verify `quick_route()` in `query_router.py`, which classifies queries into SQL, SEMANTIC, or HYBRID routes based on keyword matching.

### SQL Keyword Triggers

The following keywords cause a query to be classified as SQL (unless a SEMANTIC keyword is also present):

`top`, `largest`, `smallest`, `compare`, `list`, `show`, `how many`, `total`, `sum`, `average`, `count`, `greater than`, `less than`, `between`, `trend`, `by year`, `flight phase`, `aircraft type`, `location`

| Test ID | Query | Expected Route | Triggering Keywords | Rationale |
|---------|-------|---------------|---------------------|-----------|
| R1 | "list all airports in the database" | SQL | `list` | Single SQL keyword |
| R2 | "show me all flight phases" | SQL | `show`, `flight phase` | Two SQL keywords |
| R3 | "how many incidents occurred in 2025?" | SQL | `how many` | Multi-word keyword |
| R4 | "total number of reports by aircraft type" | SQL | `total`, `aircraft type` | Two SQL keywords |
| R5 | "average altitude of incidents in cruise flight phase" | SQL | `average`, `flight phase` | Two SQL keywords |
| R6 | "count of reports between 2020 and 2024" | SQL | `count`, `between` | Two SQL keywords |
| R7 | "top 5 average count of reports by location" | SQL | `top`, `average`, `count`, `location` | Four SQL keywords (saturation test) |
| R8 | "compare incident rates by year" | SQL | `compare`, `by year` | Two SQL keywords |
| R9 | "trend of bird strikes by year" | SQL | `trend`, `by year` | Two SQL keywords |
| R10 | "largest airports in Europe" | SQL | `largest` | Single SQL keyword |
| R11 | "smallest runway at KJFK" | SQL | `smallest` | Single SQL keyword |
| R12 | "sum of all incident reports" | SQL | `sum` | Single SQL keyword |
| R13 | "reports greater than 100 per year" | SQL | `greater than` | Multi-word keyword |
| R14 | "airports with less than 5 incidents" | SQL | `less than` | Multi-word keyword |

### SEMANTIC Keyword Triggers

The following keywords cause a query to be classified as SEMANTIC:

`describe`, `summarize`, `what happened`, `example`, `similar`, `narrative`, `context`, `why`, `lessons learned`

| Test ID | Query | Expected Route | Triggering Keywords | Rationale |
|---------|-------|---------------|---------------------|-----------|
| R15 | "describe the most common engine failure scenarios" | SEMANTIC | `describe` | Single SEMANTIC keyword |
| R16 | "summarize bird strike incident narratives" | HYBRID | `summarize`, `narrative` (SEMANTIC) + `sum` substring match (SQL) | **Known artifact**: "summarize" contains "sum", causing false SQL match -> HYBRID |
| R17 | "what happened during the hydraulic system failure?" | SEMANTIC | `what happened` | Multi-word keyword |
| R18 | "give me an example of runway incursion" | SEMANTIC | `example` | Single SEMANTIC keyword |
| R19 | "find similar incidents to tail strike on landing" | SEMANTIC | `similar` | Single SEMANTIC keyword |
| R20 | "explain the narrative behind gear collapse incidents" | SEMANTIC | `narrative` | Single SEMANTIC keyword |
| R21 | "what lessons learned from ATC miscommunication?" | SEMANTIC | `lessons learned` | Multi-word keyword |
| R22 | "provide context for CFIT incidents" | SEMANTIC | `context` | Single SEMANTIC keyword |
| R23 | "why do pilots report hydraulic failures?" | SEMANTIC | `why` | Single SEMANTIC keyword |

### HYBRID: Both SQL + SEMANTIC Keywords

When both SQL and SEMANTIC keywords are present, the route is always HYBRID.

| Test ID | Query | Expected Route | SQL Keywords | SEMANTIC Keywords | Rationale |
|---------|-------|---------------|-------------|-------------------|-----------|
| R24 | "describe the trend of incidents by year" | HYBRID | `trend`, `by year` | `describe` | Conflict resolution: both present -> HYBRID |
| R25 | "show me what happened in the top 5 incidents" | HYBRID | `show`, `top` | `what happened` | Conflict resolution |
| R26 | "summarize the count of engine failures by location" | HYBRID | `count`, `location` | `summarize` | Conflict resolution |
| R27 | "list and describe incidents similar to bird strikes" | HYBRID | `list` | `describe`, `similar` | Conflict resolution |
| R28 | "what lessons learned from the top aircraft type incidents?" | HYBRID | `top`, `aircraft type` | `lessons learned` | Conflict resolution |

### HYBRID: Fallback Keywords

When no SQL or SEMANTIC keywords match, these fallback keywords still trigger HYBRID:

`report`, `asrs`, `incident`, `safety`

| Test ID | Query | Expected Route | Triggering Keywords | Rationale |
|---------|-------|---------------|---------------------|-----------|
| R29 | "tell me about ASRS report system" | HYBRID | `asrs`, `report` | Fallback keywords |
| R30 | "what is an incident report?" | HYBRID | `incident`, `report` | Fallback keywords |
| R31 | "safety management systems in aviation" | HYBRID | `safety` | Single fallback keyword |

### HYBRID: Default (No Keywords Match)

When no keywords match at all, the default route is HYBRID.

| Test ID | Query | Expected Route | Rationale |
|---------|-------|---------------|-----------|
| R32 | "Boeing 737 MAX" | HYBRID | No matching keywords whatsoever |
| R33 | "Airbus A320neo fleet information" | HYBRID | No matching keywords |
| R34 | "KJFK" | HYBRID | Just an ICAO code, no keywords |
| R35 | "What is the weather like?" | HYBRID | No matching keywords (note: no SQL/SEMANTIC/fallback triggers) |

---

## Layer 2: Source Activation Tests

These tests verify the `_wants_*` functions in `retrieval_plan.py`, which determine specific source activation based on query content. The query is lowercased before matching (`query_l = request.query.lower()`).

### _wants_realtime -> KQL

Markers: `"last "` (trailing space), `"live"`, `"real-time"`, `"realtime"`, `"minutes"`, `"now"`, `"current status"`, `"recent"`

| Test ID | Query | Expected Sources | Triggering Marker | Rationale |
|---------|-------|-----------------|-------------------|-----------|
| S1 | "last 30 minutes of flights near LTFM" | KQL | `last `, `minutes` | Two markers, trailing space on "last " important |
| S2 | "show live flight data for Istanbul area" | KQL | `live` | Standard marker |
| S3 | "real-time weather observations" | KQL | `real-time` | Hyphenated marker |
| S4 | "realtime hazard alerts in the area" | KQL | `realtime` | Non-hyphenated variant |
| S5 | "what happened in the last 10 minutes?" | KQL | `minutes`, `last ` | Dual trigger |
| S6 | "current status of flights at KJFK right now" | KQL | `current status`, `now` | Multi-word + single markers |
| S7 | "show recent weather reports" | KQL | `recent` | Standard marker |
| S8 | "the broadcast system is running" | (none from realtime) | n/a | **Negative test**: "broadcast" contains "last" but not "last " (trailing space prevents false positive) |

### _wants_graph -> GRAPH

Markers: `"impact"`, `"dependency"`, `"depends on"`, `"connected"`, `"alternate"`, `"route network"`, `"relationship"`

| Test ID | Query | Expected Sources | Triggering Marker | Rationale |
|---------|-------|-----------------|-------------------|-----------|
| S9 | "what is the impact of runway closure on connected flights?" | GRAPH | `impact`, `connected` | Two markers |
| S10 | "show dependency chain for LTFM alternate airports" | GRAPH | `dependency`, `alternate` | Two markers |
| S11 | "which airports are connected to Istanbul?" | GRAPH | `connected` | Single marker |
| S12 | "show route network from KJFK" | GRAPH | `route network` | Multi-word marker |
| S13 | "what is the relationship between LTFM and LTBA?" | GRAPH | `relationship` | Single marker |

### _wants_regulatory -> VECTOR_REG

Markers: `"ad "` (trailing space), `"airworthiness"`, `"notam"`, `"easa"`, `"compliance"`, `"directive"`

| Test ID | Query | Expected Sources | Triggering Marker | Rationale |
|---------|-------|-----------------|-------------------|-----------|
| S14 | "show AD 2025-01-04 for Boeing 737" | VECTOR_REG | `ad ` | Trailing space prevents matching "add", "advisor", etc. |
| S15 | "airworthiness directives for A320 fleet" | VECTOR_REG | `airworthiness`, `directive` | Two markers |
| S16 | "latest EASA bulletins for engine type" | VECTOR_REG | `easa` | Single marker |
| S17 | "compliance status of fleet modifications" | VECTOR_REG | `compliance` | Single marker |
| S18 | "the advisor recommended a review" | (none from regulatory) | n/a | **Negative test**: "advisor" contains "ad" but not "ad " (trailing space prevents false positive) |

### _wants_narrative -> VECTOR_OPS

Markers: `"summarize"`, `"similar"`, `"narrative"`, `"what happened"`, `"examples"`, `"lessons"`

| Test ID | Query | Expected Sources | Triggering Marker | Rationale |
|---------|-------|-----------------|-------------------|-----------|
| S19 | "summarize bird strike incidents from last year" | VECTOR_OPS | `summarize` | Standard marker |
| S20 | "find similar events to tail strike on landing" | VECTOR_OPS | `similar` | Standard marker |

### _wants_airport_ops -> VECTOR_AIRPORT

Markers: `"runway"`, `"gate"`, `"turnaround"`, `"airport"`, `"station"`, `"ltfm"`, `"ltfj"`, `"ltba"`

| Test ID | Query | Expected Sources | Triggering Marker | Rationale |
|---------|-------|-----------------|-------------------|-----------|
| S21 | "runway length at LTFM" | VECTOR_AIRPORT | `runway`, `ltfm` | Two markers |
| S22 | "gate assignments at the station" | VECTOR_AIRPORT | `gate`, `station` | Two markers |
| S23 | "turnaround time for narrow-body aircraft" | VECTOR_AIRPORT | `turnaround` | Single marker |
| S24 | "LTFM airport facilities overview" | VECTOR_AIRPORT | `ltfm`, `airport` | ICAO code + generic marker (case-insensitive) |
| S25 | "LTBA runway configuration and status" | VECTOR_AIRPORT | `ltba`, `runway` | ICAO code + marker |

### _wants_nosql -> NOSQL

Markers: `"notam"`, `"operational doc"`, `"ops doc"`, `"ground handling doc"`, `"parking stand"`

| Test ID | Query | Expected Sources | Triggering Marker | Rationale |
|---------|-------|-----------------|-------------------|-----------|
| S26 | "active NOTAMs for KJFK" | NOSQL, VECTOR_REG | `notam` | "notam" triggers BOTH _wants_nosql AND _wants_regulatory |
| S27 | "ground handling doc for Istanbul" | NOSQL | `ground handling doc` | Multi-word marker |
| S28 | "parking stand allocation at DFW" | NOSQL | `parking stand` | Multi-word marker |
| S29 | "operational doc for winter operations" | NOSQL | `operational doc` | Multi-word marker |
| S30 | "ops doc for de-icing procedures" | NOSQL | `ops doc` | Multi-word marker |

### _wants_analytics -> FABRIC_SQL

Markers: `"delay"`, `"on-time"`, `"schedule performance"`, `"bts"`, `"carrier delay"`, `"cancellation rate"`, `"on time performance"`, `"delay cause"`, `"weather delay"`, `"nas delay"`

| Test ID | Query | Expected Sources | Triggering Marker | Rationale |
|---------|-------|-----------------|-------------------|-----------|
| S31 | "average delay for American Airlines flights" | FABRIC_SQL | `delay` | Single marker |
| S32 | "on-time performance by carrier for Q1" | FABRIC_SQL | `on-time` | Single marker |
| S33 | "BTS statistics for regional carriers" | FABRIC_SQL | `bts` | Single marker (case-insensitive) |
| S34 | "cancellation rate by month" | FABRIC_SQL | `cancellation rate` | Multi-word marker |
| S35 | "weather delay trends at major hubs" | FABRIC_SQL | `weather delay`, `delay` | Overlapping markers |
| S36 | "NAS delay causes breakdown" | FABRIC_SQL | `nas delay`, `delay cause`, `delay` | Three overlapping markers |

### Case Sensitivity Tests

| Test ID | Query | Expected Sources | Rationale |
|---------|-------|-----------------|-----------|
| S37 | "SHOW LIVE NOTAMS FOR LTFM" | KQL, NOSQL, VECTOR_REG, VECTOR_AIRPORT | ALL-CAPS: query_l = query.lower() ensures all matching works |
| S38 | "Summarize The Narrative Behind Recent Incidents" | VECTOR_OPS | Title-case: lowered before matching |

### Fallback Test

| Test ID | Query | Expected Sources | Rationale |
|---------|-------|-----------------|-----------|
| S39 | "Boeing 737 fleet composition" | SQL, VECTOR_OPS | No _wants_ triggers; HYBRID route -> baseline SQL + VECTOR_OPS + profile pilot-brief enrichments |

---

## Layer 3: Combined Routing + Source Activation

The full pipeline combines Layer 1 (route classification) with Layer 2 (source activation). The `smart_route()` function in `query_router.py` uses heuristics first, then escalates to the LLM for ambiguous (HYBRID) cases.

### smart_route() Decision Matrix

| Heuristic Result | Action | Source Selection |
|-----------------|--------|-----------------|
| SQL | Use heuristic directly | build_retrieval_plan with route=SQL, no router_sources |
| SEMANTIC | Use heuristic directly | build_retrieval_plan with route=SEMANTIC, no router_sources |
| HYBRID | Escalate to LLM | LLM may return explicit sources list |

### Source Selection Priority When LLM Provides Sources

When the LLM `route()` returns an explicit `sources` list, `build_retrieval_plan` uses those sources directly (priority 10+idx) instead of applying heuristic-based _wants_* activation. This means:

1. LLM-routed queries get LLM-selected sources
2. Heuristic-routed queries (SQL/SEMANTIC from smart_route) get _wants_*-based sources
3. The _wants_* functions add ADDITIONAL sources on top of the baseline

### Profile-Driven Source Addition

The default profile is `pilot-brief`, which always adds SQL + VECTOR_OPS regardless of route:

| Profile | Sources Added | Priority |
|---------|-------------|----------|
| `pilot-brief` / `ops-live` / `operations` | SQL (10), VECTOR_OPS (20) | Baseline |
| `compliance` / `regulatory` | VECTOR_REG (15), SQL (25) | Regulatory-first |

---

## Original Source Coverage Tests

These are the original T1-T16, M1-M7, and E1/E3 tests that verify end-to-end source firing via the SSE endpoint.

### Individual Source Tests (T1-T16)

| Test ID | Query | Expected Source(s) | Description |
|---------|-------|--------------------|-------------|
| T1 | "How many ASRS reports mention engine failure?" | SQL | SQL count query |
| T2 | "Top 5 airports by number of incident reports" | SQL | SQL top-N |
| T3 | "Find reports similar to bird strike during takeoff" | VECTOR_OPS | Vector similarity |
| T4 | "What happened in ASRS reports about hydraulic failures?" | VECTOR_OPS | Vector narrative |
| T5 | "Show current NOTAMs for KJFK" | VECTOR_REG, NOSQL | Regulatory + NOSQL |
| T6 | "EASA airworthiness directive for Airbus A320" | VECTOR_REG | Regulatory lookup |
| T7 | "What runways does LTFM airport have?" | VECTOR_AIRPORT | Airport ops |
| T8 | "Airport information for Istanbul Sabiha Gokcen" | VECTOR_AIRPORT | Airport info |
| T9 | "What routes are connected to KJFK and what airlines operate them?" | GRAPH | Graph traversal |
| T10 | "Show the dependency network for Istanbul airport LTFM" | GRAPH | Graph multi-hop |
| T11 | "Show active NOTAMs for KJFK" | NOSQL | Cosmos NOTAM |
| T12 | "Any parking stand NOTAMs for DFW?" | NOSQL | Cosmos specific NOTAM |
| T13 | "What is the current live status of flights near KJFK?" | KQL | KQL live data |
| T14 | "Show real-time weather hazards for Istanbul" | KQL | KQL weather |
| T15 | "What is the average delay for Delta flights in January?" | FABRIC_SQL | Fabric SQL analytics |
| T16 | "Show the on-time performance and cancellation rate for carriers" | FABRIC_SQL | Fabric SQL performance |

### Multi-Source Combination Tests (M1-M7)

| Test ID | Query | Expected Sources | Description |
|---------|-------|-----------------|-------------|
| M1 | "Prepare a pilot briefing for departure from LTFM including NOTAMs and runway status" | SQL, NOSQL, VECTOR_AIRPORT | Multi-source pilot brief |
| M2 | "What is the impact of NOTAM closures at KJFK on connected routes?" | GRAPH, NOSQL | Graph + NOSQL combo |
| M3 | "Summarize recent bird strike incidents and any related airworthiness directives" | VECTOR_OPS, VECTOR_REG | Ops + Regulatory combo |
| M4 | "Show live weather at Istanbul airport and any active NOTAMs" | KQL, NOSQL | KQL + NOSQL combo |
| M5 | "What carriers have the worst on-time performance and what safety lessons emerge from ASRS reports?" | FABRIC_SQL, VECTOR_OPS | Fabric SQL + Vector combo |
| M6 | "Show the route network from LTFM, delay statistics, and any current NOTAMs" | GRAPH, FABRIC_SQL, NOSQL | Graph + Fabric SQL + NOSQL combo |
| M7 | "Are there alternate airports for LTFM and what are their runway configurations?" | GRAPH, VECTOR_AIRPORT | Graph + Airport combo |

### Edge Cases (E1, E3)

| Test ID | Query | Expected | Pass Condition |
|---------|-------|----------|----------------|
| E1 | "Hello, how are you?" | No sources | Answer generated (greeting fallback) |
| E3 | "Show PII: my SSN is 123-45-6789" | PII blocked | pii_blocked event OR answer generated |

---

## Multi-Source Overlap Tests

These tests verify that queries triggering multiple `_wants_*` functions correctly activate all expected sources simultaneously.

### Overlap Matrix

| Test ID | Query | Sources Activated | _wants_ Functions Triggered |
|---------|-------|-------------------|----------------------------|
| O1 | "Show live NOTAMs for alternate airports with runway closures" | KQL, VECTOR_REG, GRAPH, VECTOR_AIRPORT, NOSQL (5) | realtime, regulatory, graph, airport_ops, nosql |
| O2 | "What are the delay causes at the connected airports?" | FABRIC_SQL, GRAPH (2) | analytics, graph |
| O3 | "Summarize airworthiness directives for similar incidents" | VECTOR_OPS, VECTOR_REG (2) | narrative, regulatory |
| O4 | "Show real-time NOTAMs and delay statistics for LTFM airport" | KQL, NOSQL, VECTOR_REG, FABRIC_SQL, VECTOR_AIRPORT (5) | realtime, nosql, regulatory, analytics, airport_ops |
| O5 | "What happened at the runway during the last turnaround at LTBA?" | VECTOR_OPS, VECTOR_AIRPORT, KQL (3) | narrative, airport_ops, realtime |
| O6 | "Impact of weather delay on route network from connected airports" | GRAPH, FABRIC_SQL (2) | graph, analytics |
| O7 | "Compliance with airworthiness directive AD 2025-01 at LTFM airport" | VECTOR_REG, VECTOR_AIRPORT (2) | regulatory, airport_ops |
| O8 | "Summarize lessons from recent incidents at alternate airports with parking stand NOTAMs" | VECTOR_OPS, GRAPH, NOSQL, VECTOR_REG, KQL (5) | narrative, realtime, graph, nosql, regulatory |
| O9 | "Live gate assignments and ground handling doc for turnaround operations" | KQL, VECTOR_AIRPORT, NOSQL (3) | realtime, airport_ops, nosql |
| O10 | "Show the dependency of on-time performance on runway closures at LTFJ station" | GRAPH, FABRIC_SQL, VECTOR_AIRPORT (3) | graph, analytics, airport_ops |
| O11 | "What is the impact of NAS delay on schedule performance for connected carriers?" | GRAPH, FABRIC_SQL (2) | graph, analytics |
| O12 | "Describe narrative examples of compliance issues with EASA directives at airports" | VECTOR_OPS, VECTOR_REG, VECTOR_AIRPORT (3) | narrative, regulatory, airport_ops |

### Maximum Overlap Scenarios

The theoretical maximum overlap is 7 sources (all _wants_* functions fire plus baseline). O1, O4, and O8 test near-maximum overlap with 5 sources each.

Note that the baseline profile (`pilot-brief`) always adds SQL + VECTOR_OPS, so actual activated sources may be higher than what the _wants_* functions alone suggest.

---

## Keyword Gap Analysis

These tests document keywords or query patterns that SHOULD probably trigger a specific source but currently do NOT because the markers are missing.

### Gap Test Matrix

| Test ID | Query | Missing Source | Missing Keyword | Recommendation |
|---------|-------|---------------|----------------|----------------|
| G1 | "latest METAR for KJFK" | KQL | `metar` | Add `"metar"` to `_wants_realtime` markers |
| G2 | "TAF forecast for LTFM" | KQL | `taf` | Add `"taf"` to `_wants_realtime` markers |
| G3 | "current flight status for TK1234" | KQL | `flight status` | Add `"flight status"` to `_wants_realtime` markers |
| G4 | "standard operating procedure for engine start" | NOSQL or VECTOR_OPS | `standard operating procedure` | Add to `_wants_nosql` or create `_wants_procedures` |
| G5 | "SOP for cold weather operations" | NOSQL or VECTOR_OPS | `sop` | Add `"sop"` to `_wants_nosql` markers |
| G6 | "fuel consumption analysis for long-haul flights" | SQL | `fuel` | Add `"fuel"` to SQL baseline triggers or create `_wants_fuel_ops` |
| G7 | "crew roster for Istanbul base next week" | SQL | `crew`, `roster` | Add to SQL triggers or create `_wants_crew_ops`; data in `ops_crew_rosters` |
| G8 | "minimum equipment list for registration TC-JPA" | SQL or NOSQL | `minimum equipment list`, `mel` | Add to appropriate markers; data in `ops_mel_techlog_events` |
| G9 | "SIGMET active over the Mediterranean" | KQL | `sigmet` | Add `"sigmet"` to `_wants_realtime` markers |
| G10 | "PIREP for turbulence at FL350" | KQL or VECTOR_OPS | `pirep` | Add to `_wants_realtime` or `_wants_narrative` |
| G11 | "ATC communication issues at KJFK" | VECTOR_OPS | `atc` | Currently matches via LLM routing but not heuristic |
| G12 | "maintenance log for Boeing 737-800" | SQL | `maintenance` | Add to markers; data in `ops_mel_techlog_events` |

### Gap Categories

**Weather/Real-time gaps** (G1, G2, G9, G10): Aviation-specific weather product names (METAR, TAF, SIGMET, PIREP) are not in `_wants_realtime` markers. These are common aviation queries and should be added.

**Operational data gaps** (G6, G7, G8, G12): Domain-specific operational terms (fuel, crew, MEL, maintenance) have corresponding database tables but no keyword triggers. The LLM router may handle these, but heuristic routing will miss them.

**Procedure/SOP gaps** (G4, G5): Standard operating procedures are a common aviation query category but have no keyword triggers.

**Communication gaps** (G3, G11): "flight status" and "ATC" are common aviation terms that fall through heuristic routing.

---

## Edge Cases and Boundary Conditions

### Trailing Space Sensitivity

Two markers use a trailing space to avoid false substring matches:

| Marker | Avoids Matching | Test |
|--------|----------------|------|
| `"last "` (with space) | "broadcast", "lasting", "blast" | S8: "the broadcast system is running" should NOT trigger KQL |
| `"ad "` (with space) | "advisor", "add", "adventure" | S18: "the advisor recommended a review" should NOT trigger VECTOR_REG |

**Risk**: If the marker appears at the end of the query string (e.g., "show me the last"), the trailing space will NOT match. This is a known limitation.

### Substring Matching Hazards

The `_wants_*` functions use simple `in` substring matching, which can cause false positives:

| Marker | False Positive Risk | Example |
|--------|-------------------|---------|
| `"sum"` (SQL) | Matches "summarize", "summary", "assume" | **Active bug**: "summarize" triggers SQL via "sum" substring, causing HYBRID instead of SEMANTIC (R16) |
| `"live"` | Matches "deliver", "olive" | "olive garden delivery" would trigger KQL |
| `"now"` | Matches "know", "known", "snow" | "what do we know about snow?" triggers KQL |
| `"gate"` | Matches "investigate", "mitigate" | "investigate the incident" triggers VECTOR_AIRPORT |
| `"similar"` | Low risk | Generally safe |
| `"impact"` | Matches "impacted" | Acceptable (impacted is relevant) |
| `"station"` | Matches "destination" | "destination airport" triggers VECTOR_AIRPORT (may be acceptable) |
| `"alternate"` | Matches "alternated" | Acceptable |
| `"examples"` | Low risk | Generally safe |
| `"delay"` | Matches "delayed" | Acceptable |
| `"recent"` | Low risk | Generally safe |
| `"minutes"` | Low risk | Generally safe in aviation context |

### The "notam" Dual Activation

The keyword `"notam"` triggers BOTH `_wants_regulatory` (-> VECTOR_REG) and `_wants_nosql` (-> NOSQL) simultaneously. This is intentional: NOTAMs exist in both the regulatory vector index and the Cosmos DB document store. Test S26 verifies this behavior.

### Case Sensitivity Behavior

All `_wants_*` functions operate on `query_l = request.query.lower()`, so case is not a boundary concern. Tests S37 and S38 verify this with ALL-CAPS and Title-Case queries respectively.

### Profile Override Behavior

The `compliance` / `regulatory` profile adds VECTOR_REG and SQL even when no keywords match. This means a query like "Boeing 737 fleet" under the compliance profile will include VECTOR_REG, which would not happen under `pilot-brief`.

---

## Cross-Source Conflict Resolution

### Priority System

Each source in the retrieval plan has a priority number. Lower numbers execute first:

| Priority | Source | Context |
|----------|--------|---------|
| 1 | Required sources | Explicitly requested by caller |
| 5 | KQL | Real-time data (highest heuristic priority) |
| 8 | GRAPH | Dependency traversal |
| 10 | SQL | Baseline structured queries |
| 12 | VECTOR_REG | Regulatory lookup |
| 15 | FABRIC_SQL | Analytics |
| 18 | VECTOR_OPS (narrative) | Narrative similarity |
| 20 | VECTOR_OPS (baseline) | Default semantic |
| 22 | VECTOR_AIRPORT | Airport ops |
| 24 | NOSQL | Document lookup |

### Deduplication

The `add()` function in `build_retrieval_plan` prevents duplicate sources:
```python
def add(source, reason, priority, params=None):
    if any(s.source == source for s in steps):
        return  # skip if source already added
```

This means the FIRST addition wins. For example, if SQL is added by baseline (priority 10) and then by profile (priority 10), only the first one is kept. The reason and priority of the first addition are preserved.

### Router Sources vs. Heuristic Sources

When the LLM router returns an explicit `sources` list, the entire heuristic _wants_* pipeline is bypassed. The `router_sources` path and the heuristic path are mutually exclusive in `build_retrieval_plan`.

This has implications for `smart_route()`:
- SQL/SEMANTIC confident -> heuristic path (no router_sources, _wants_* applies)
- HYBRID -> LLM path (may provide router_sources, bypassing _wants_*)

This means a query like "show live NOTAMs for LTFM" routed as SQL by heuristic will get _wants_* enrichment (KQL, NOSQL, VECTOR_REG, VECTOR_AIRPORT), but the same query routed as HYBRID by LLM may get different sources depending on what the LLM returns.

---

## Recommendations for Routing Improvements

### High Priority

1. **Fix "sum" substring false positive in quick_route SQL keywords**
   - Problem: SQL keyword `"sum"` matches inside `"summarize"` and `"summary"`, causing SEMANTIC queries to be misrouted as HYBRID
   - Fix: Use word-boundary matching (`r'\bsum\b'`) or check `" sum "` / `" sum"` with surrounding spaces
   - Impact: Test R16 documents this active bug
   - Related: `"top"` also matches inside "stop", "topology"; `"show"` matches inside "shower", "showdown"

2. **Add aviation weather product keywords to `_wants_realtime`**
   - Add: `"metar"`, `"taf"`, `"sigmet"`, `"pirep"`, `"atis"`
   - Rationale: These are the most common aviation real-time data queries (gaps G1, G2, G9, G10)

3. **Add flight status keyword to `_wants_realtime`**
   - Add: `"flight status"`, `"tracking"`
   - Rationale: Very common user query pattern (gap G3)

4. **Add operational data keywords**
   - Add `"crew"`, `"roster"` to a new `_wants_crew_ops` or to SQL enrichment
   - Add `"mel"`, `"minimum equipment"`, `"maintenance"`, `"techlog"` to a new `_wants_maintenance`
   - Rationale: Data exists in `ops_crew_rosters` and `ops_mel_techlog_events` but is unreachable via heuristic routing (gaps G7, G8, G12)

5. **Add SOP/procedure keywords**
   - Add `"sop"`, `"standard operating procedure"`, `"procedure"` to `_wants_nosql`
   - Rationale: Common aviation query pattern (gaps G4, G5)

### Medium Priority

6. **Use word-boundary matching instead of substring matching**
   - Replace `m in query_l` with regex word boundary checks for short markers
   - Specifically for: `"live"` (avoids "deliver", "olive"), `"now"` (avoids "know", "snow"), `"gate"` (avoids "investigate"), `"station"` (avoids "destination")
   - This would require changing `_wants_*` functions from `any(m in query_l ...)` to regex-based matching

7. **Add fuel operations keyword**
   - Add `"fuel"` to SQL enrichment or a new `_wants_fuel_ops`
   - Rationale: Fuel data may exist in operational tables (gap G6)

8. **Handle end-of-string trailing space markers**
   - `"last "` and `"ad "` will fail to match at query end (e.g., "show me the last")
   - Fix: Also check `query_l.endswith("last")` and `query_l.endswith(" ad")`

### Low Priority

9. **Add ATC keyword**
   - Add `"atc"` to `_wants_narrative` (ASRS reports frequently mention ATC)
   - Rationale: Gap G11

10. **Consider profile-aware _wants_ functions**
    - Different profiles may need different keyword sensitivity
    - Example: `compliance` profile should be more sensitive to regulatory keywords

11. **Add telemetry/logging to routing decisions**
    - Log which _wants_* functions fired and why
    - This would help debug routing in production

### Test Coverage Statistics

| Category | Test Count | Coverage |
|----------|-----------|----------|
| Individual source (T*) | 16 | All 8 sources covered |
| Multi-source (M*) | 7 | Common 2-3 source combos |
| Edge cases (E*) | 2 | Greeting + PII |
| Route classification (R*) | 35 | All SQL keywords, all SEMANTIC keywords, HYBRID conflicts, fallbacks, defaults |
| Source activation (S*) | 39 | All _wants_* functions, negative tests, case sensitivity, fallback |
| Gap analysis (G*) | 12 | Weather, ops, procedures, comms |
| Overlap (O*) | 12 | 2-source to 5-source overlaps |
| **Total** | **123** | |

### Source Coverage Summary

| Source | Direct Tests | Multi Tests | Overlap Tests | Total Coverage |
|--------|-------------|------------|---------------|----------------|
| SQL | T1, T2 | M1 | - | 3+ |
| KQL | T13, T14, S1-S7 | M4 | O1, O4, O5, O8, O9 | 14+ |
| GRAPH | T9, T10, S9-S13 | M2, M6, M7 | O1, O2, O6, O8, O10, O11 | 14+ |
| VECTOR_OPS | T3, T4, S19, S20, S38 | M3, M5 | O3, O5, O8, O12 | 11+ |
| VECTOR_REG | T5, T6, S14-S17, S26 | M3 | O1, O3, O4, O7, O8, O12 | 13+ |
| VECTOR_AIRPORT | T7, T8, S21-S25, S37 | M1, M7 | O1, O4, O5, O7, O9, O10, O12 | 16+ |
| NOSQL | T11, T12, S26-S30 | M1, M2, M4, M6 | O1, O4, O8, O9 | 13+ |
| FABRIC_SQL | T15, T16, S31-S36 | M5, M6 | O2, O4, O6, O10, O11 | 13+ |

---

## Running the Tests

### Full test suite against live backend

```bash
python scripts/17_test_all_sources.py --backend http://localhost:5001
```

### Dry-run mode (no backend required)

```bash
python scripts/17_test_all_sources.py --dry-run
```

### Filter by test IDs

```bash
python scripts/17_test_all_sources.py --filter R1,R2,R3,S1,S2
```

### Filter by category

```bash
python scripts/17_test_all_sources.py --category routing
python scripts/17_test_all_sources.py --category source_activation
python scripts/17_test_all_sources.py --category gap
python scripts/17_test_all_sources.py --category overlap
python scripts/17_test_all_sources.py --category routing,source_activation
```

### Verbose mode

```bash
python scripts/17_test_all_sources.py --verbose --filter O1
```
