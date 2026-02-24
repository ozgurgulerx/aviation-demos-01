# Query Planning & Routing — Deep Dive

**Date:** 2026-02-24
**Branch:** `codex/subscription-guardrail-and-source-details`
**Commit:** `b6e8720` (LLM-driven multi-source retrieval with intent graph)

---

## 1. High-Level Flow

```
User message
    │
    ▼
┌─────────────────────────────────────────────────────┐
│  af_runtime.py  run_stream()                        │
│                                                     │
│  ┌──────────────┐   ┌────────────────────────────┐  │
│  │ PII check    │   │ Query routing              │  │
│  │ (parallel)   │   │ (parallel)                 │  │
│  │              │   │                            │  │
│  │ check_pii()  │   │ intent_graph_provider      │  │
│  │              │   │   .load()                  │  │
│  │              │   │       │                    │  │
│  │              │   │       ▼                    │  │
│  │              │   │ router.smart_route(        │  │
│  │              │   │   query,                   │  │
│  │              │   │   intent_graph=graph.data  │  │
│  │              │   │ )                          │  │
│  └──────┬───────┘   └─────────────┬──────────────┘  │
│         │                         │                  │
│         ▼                         ▼                  │
│  pii_result              precomputed_route           │
│  (block/pass/redact)     {route, sources, sql_hint}  │
│                                                     │
│         │                         │                  │
│         └─────────┬───────────────┘                  │
│                   ▼                                  │
│    af_context_provider.build_context()               │
│         │                                           │
│         ├── agentic_enabled? ──yes──▶ _build_agentic_context()
│         │                                           │
│         └── no / failed ──────────▶ _build_legacy_context()
│                                                     │
└─────────────────────────────────────────────────────┘
```

PII scanning and query routing run **in parallel** inside a `ThreadPoolExecutor(max_workers=2)`. Both complete before the pipeline proceeds. The routing result (`precomputed_route`) is passed downstream so it doesn't need to be recomputed.

---

## 2. The Two Routing Paths

There are two execution paths once routing completes. The path selection happens in `_build_context_inner()`:

```python
if retrieval_mode == "code-rag" and self._agentic_enabled and self.orchestrator is not None:
    return self._build_agentic_context(...)   # PRIMARY path
except Exception:
    fallback to _build_legacy_context(...)    # FALLBACK path
```

### Path A: Agentic (primary)

The `AgenticOrchestrator.create_plan()` is an LLM call (gpt-5-mini) that produces a full `AgenticPlan` with typed `ToolCall` objects. It receives:

- The user query
- The full intent graph (intents, requires, authoritative_in, expansion_rules)
- A tool catalog listing all 8 sources
- Database schemas
- Runtime context (time, risk mode, etc.)

The agentic path does **not** use `QueryRouter` at all — the orchestrator LLM decides both the intent and the tool calls directly. The `precomputed_route` from the parallel step is ignored.

After plan creation:
1. `_prune_non_viable_tool_calls()` removes sources that are unavailable at runtime
2. `plan_executor.execute()` runs the tool calls (parallel where possible)
3. `evidence_verifier.verify()` checks coverage against required evidence
4. If coverage gaps exist, a **re-query loop** tries fallback tools (up to 3)
5. `reconcile_context()` deduplicates and ranks results

### Path B: Legacy (fallback)

Used when the agentic orchestrator is unavailable or fails. This is the path that now benefits from the new LLM-driven routing.

```
_build_legacy_context()
    │
    ▼
_resolve_route()  ◀── uses precomputed_route if available
    │                  otherwise loads intent graph and calls router.route()
    │
    ├── Returns: (route, reasoning, sql_hint, router_sources)
    │
    ▼
build_retrieval_plan()  ◀── retrieval_plan.py
    │
    ├── If router_sources provided: uses them directly as plan steps
    │
    ├── If no router_sources: falls back to heuristic source selection
    │   based on route type, query profile, and keyword detection
    │
    ▼
_execute_plan()  ◀── parallel ThreadPoolExecutor
    │
    ▼
_apply_reconciliation()
    │
    ▼
AviationRagContext (returned to runtime for synthesis)
```

---

## 3. QueryRouter — The LLM Routing Call

**File:** `src/query_router.py`
**Model:** gpt-5-nano (env: `AZURE_OPENAI_DEPLOYMENT_NAME`)

### 3.1 Entry Points

| Method | When Used | Behavior |
|--------|-----------|----------|
| `smart_route(query, intent_graph)` | Runtime parallel routing (af_runtime.py) | Always calls LLM; falls back to heuristics on failure |
| `route(query, intent_graph)` | Direct LLM routing (af_context_provider.py fallback) | Single LLM call; returns HYBRID on error |
| `quick_route(query)` | Keyword heuristic only | No LLM; pattern matching only |

### 3.2 smart_route() Logic

```python
def smart_route(query, intent_graph=None):
    result = route(query, intent_graph=intent_graph)   # always try LLM

    if result is valid and not a fallback error:
        return result                                   # LLM succeeded

    heuristic = quick_route(query)                     # LLM failed
    return {route: heuristic, sources: []}             # no source selection
```

Key behavioral change: the old `smart_route()` used to skip the LLM entirely when keyword heuristics gave a confident SQL or SEMANTIC answer. Now it **always calls the LLM** because the LLM also performs multi-source selection — something heuristics cannot do.

### 3.3 route() — The LLM Call

The LLM receives up to 3 messages:

```
[system]  ROUTING_PROMPT         (~350 lines: 8 source descriptions,
                                  route definitions, 8 selection rules,
                                  intent graph instructions, output format)

[user]    Intent graph JSON       (only when intent_graph is provided —
                                  ~250 lines of JSON with intents, requires,
                                  authoritative_in, expansion_rules)

[user]    The actual user query
```

The LLM returns a JSON object:

```json
{
    "route": "SQL | SEMANTIC | HYBRID",
    "reasoning": "why this route and these sources",
    "sql_hint": "optional hint for SQL generation",
    "sources": ["SQL", "KQL", "VECTOR_OPS"]
}
```

The `sources` array is validated against `VALID_SOURCES` — any unknown source names are silently dropped.

### 3.4 quick_route() — Keyword Heuristic Fallback

Only used when the LLM call fails. Returns a route string only (no sources):

| Keywords Detected | Route |
|-------------------|-------|
| "top", "count", "average", "how many", "trend", "list" | SQL |
| "describe", "summarize", "what happened", "similar", "lessons" | SEMANTIC |
| Both SQL + semantic keywords | HYBRID |
| "report", "asrs", "incident", "safety" | HYBRID |
| None of the above | HYBRID (default) |

The heuristic returns an empty `sources: []` — this means `build_retrieval_plan()` will fall back to its own keyword-based source detection (see section 5).

---

## 4. Intent Graph — Source Selection Guidance

**File:** `src/intent_graph_provider.py`
**Sources:** Fabric Graph endpoint → JSON file → builtin default (fallback chain)

The intent graph is a knowledge structure that maps user intents to required evidence types, and evidence types to authoritative data sources. It is fed to both:

1. The agentic orchestrator (as the primary planning input)
2. The legacy LLM router (as supplementary context for source selection)

### 4.1 Structure

```
intents          — 10 named intents (PilotBrief.Departure, Analytics.Compare, etc.)
    │
    ▼
requires         — maps intent → evidence types (required or optional)
    │
    ▼
authoritative_in — maps evidence type → tools, ranked by priority
    │
    ▼
expansion_rules  — maps intent → GRAPH traversal reasons
```

### 4.2 Intent → Evidence Mapping (requires)

| Intent | Required Evidence | Optional Evidence |
|--------|-------------------|-------------------|
| PilotBrief.Departure | METAR, TAF, NOTAM, RunwayConstraints | Hazards, IncidentNarrative, RegulatoryDoc |
| PilotBrief.Arrival | METAR, TAF, NOTAM, RunwayConstraints | RegulatoryDoc |
| Disruption.Explain | Hazards, NOTAM | METAR, SOPClause, IncidentNarrative, DelayAnalytics |
| Policy.Check | SOPClause, RegulatoryDoc | NOTAM |
| Replay.History | METAR, Hazards, IncidentNarrative | NOTAM, SafetyStats |
| Analytics.Compare | SafetyStats, DelayAnalytics | AirportData |
| Fleet.Status | FleetData | SOPClause, DelayAnalytics |
| RouteNetwork.Query | RouteData | AirportData |
| Safety.Trend | SafetyStats, IncidentNarrative | Hazards |
| Airport.Info | AirportData | RunwayConstraints, NOTAM |

### 4.3 Evidence → Tool Mapping (authoritative_in)

Each evidence type maps to one or more tools, ranked by priority (1 = preferred):

| Evidence | Priority 1 (preferred) | Priority 2 (fallback) | Priority 3 |
|----------|------------------------|-----------------------|------------|
| METAR | KQL | SQL | — |
| TAF | KQL | SQL | — |
| NOTAM | NOSQL | VECTOR_REG | — |
| RunwayConstraints | SQL | VECTOR_AIRPORT | — |
| Hazards | KQL | SQL | — |
| SOPClause | VECTOR_REG | NOSQL | — |
| FleetData | SQL | FABRIC_SQL | — |
| RouteData | GRAPH | SQL | — |
| SafetyStats | SQL | VECTOR_OPS | FABRIC_SQL |
| AirportData | SQL | VECTOR_AIRPORT | — |
| IncidentNarrative | VECTOR_OPS | SQL | — |
| RegulatoryDoc | VECTOR_REG | NOSQL | — |
| DelayAnalytics | FABRIC_SQL | SQL | — |

### 4.4 How the LLM Uses the Graph

When the intent graph JSON is injected into the routing prompt, the LLM is instructed to:

1. **Map** the query to the closest intent (e.g., "departure readiness at SAW" → `PilotBrief.Departure`)
2. **Look up** required evidence for that intent (METAR, TAF, NOTAM, RunwayConstraints)
3. **For each evidence type**, select the tool from `authoritative_in` — prefer priority 1
4. **Include optional** evidence sources when they clearly add value
5. **Apply the 8 source selection rules** as supplementary guidance

This means for "departure readiness at SAW", the LLM would reason:
- PilotBrief.Departure → needs METAR(KQL), TAF(KQL), NOTAM(NOSQL), RunwayConstraints(SQL)
- Optional: Hazards(KQL) — adds value for departure readiness
- Result: `sources: ["KQL", "NOSQL", "SQL"]`, `route: "HYBRID"`

---

## 5. Retrieval Plan Builder

**File:** `src/retrieval_plan.py`
**Function:** `build_retrieval_plan(request, route, reasoning, router_sources)`

This module converts the routing decision into an executable `RetrievalPlan` with ordered `SourcePlan` steps.

### 5.1 Source Policy Modes

| Policy | Behavior |
|--------|----------|
| `include` (default) | Router sources are primary; caller's `required_sources` are added |
| `exact` | Only the caller's `required_sources` are used; all others are excluded |

### 5.2 Plan Construction Logic

```
1. If source_policy == "exact":
      └── Use only required_sources from the request. Validate. Done.

2. If router_sources is non-empty (LLM provided sources):
      └── Use router_sources as the plan steps (priority 10+)

3. Else (no router sources — heuristic fallback):
      ├── Baseline by route type:
      │   ├── SQL/HYBRID → add SQL(p10)
      │   └── SEMANTIC/HYBRID → add VECTOR_OPS(p20)
      │
      ├── Profile enrichments:
      │   ├── pilot-brief/ops-live → SQL + VECTOR_OPS
      │   └── compliance/regulatory → VECTOR_REG + SQL
      │
      └── Keyword activation:
          ├── realtime markers → KQL(p5)
          ├── impact/dependency → GRAPH(p8)
          ├── regulatory terms → VECTOR_REG(p12)
          ├── narrative terms → VECTOR_OPS(p18)
          ├── airport/runway → VECTOR_AIRPORT(p22)
          ├── notam/ops doc → NOSQL(p24)
          └── delay/bts terms → FABRIC_SQL(p15)

4. Add caller's required_sources (priority 1 — highest)

5. If still empty → fallback: SQL(p10) + VECTOR_OPS(p20)
```

The priority numbers determine execution order in the `ThreadPoolExecutor` — lower priority = higher importance = executed first (though in practice all steps run in parallel).

### 5.3 Key Difference: With vs Without LLM Sources

**With LLM sources** (normal path): The LLM has already done the intelligent source selection considering all 8 datastores, the intent graph, and the selection rules. The plan builder just converts the source list into `SourcePlan` objects.

**Without LLM sources** (heuristic fallback): The plan builder falls back to keyword pattern matching to decide which sources to activate. This is significantly less accurate — for example, it cannot reason about intent graph evidence requirements, and it may miss FABRIC_SQL or GRAPH for queries that don't contain obvious trigger keywords.

---

## 6. Execution & Reconciliation

### 6.1 Parallel Execution

`_execute_plan()` in `af_context_provider.py` runs all plan steps concurrently:

```python
with ThreadPoolExecutor(max_workers=min(6, len(steps))) as executor:
    for step in steps:
        future = executor.submit(_run, step)
```

Each step calls `retriever.retrieve_source(step.source, query, params)` which dispatches to the appropriate backend (PostgreSQL, AI Search, Cosmos DB, Fabric KQL, etc.).

For VECTOR_* sources, a **shared embedding** is precomputed once and reused across all vector steps to avoid redundant embedding API calls.

### 6.2 Source Traces (SSE Events)

Each source execution emits two SSE events:

1. `source_call_start` — when the source query begins
2. `source_call_done` — when it completes (with row count, duration, error status)

These are streamed to the frontend in real-time via the `on_trace` callback, so the UI can show live source status.

### 6.3 Context Reconciliation

After all sources return, `_apply_reconciliation()` performs:

1. **Evidence slotting** — maps rows to required evidence types
2. **Conflict detection** — identifies contradictory data across sources
3. **Per-source limits** — truncates oversized result sets (SQL: 12, KQL: 8, vectors: 6)
4. **Fusion scoring** — ranks results by relevance (optional RRF)

### 6.4 Context Text Composition

The final context text sent to the synthesis LLM includes:

```
User query: ...
Selected route: HYBRID
Retrieval profile: pilot-brief
Planned sources: SQL(p10), KQL(p5), VECTOR_OPS(p20)

SQL query:
SELECT ...

KQL results:
1. ...

SQL results:
1. ...

FABRIC_SQL results:           ◀── newly added in this commit
1. ...

VECTOR_OPS results:
1. ...

Coverage summary:
required_filled=3/4, missing_required=[TAF]
```

---

## 7. ROUTING_PROMPT — The 8 Source Selection Rules

The new ROUTING_PROMPT gives the LLM explicit rules for choosing sources:

| Rule | Trigger | Sources |
|------|---------|---------|
| **Rule 1** — Pair structured + semantic | Mixing metrics with context | At least 1 structured (SQL/KQL/FABRIC_SQL) AND 1 semantic (VECTOR_*) |
| **Rule 2** — Freshness-critical | "now", "current", "live", "METAR", "SIGMET" | KQL |
| **Rule 3** — Relationships | "impact", "dependency", "cascade", "what happens if" | GRAPH |
| **Rule 4** — Delay analytics | "delay", "on-time", "BTS", "cancellation rate" | FABRIC_SQL |
| **Rule 5** — Regulatory | Compliance, ADs, NOTAMs | VECTOR_REG + NOSQL |
| **Rule 6** — Narratives | "incident", "safety", "similar", "lessons" | VECTOR_OPS |
| **Rule 7** — Airport facilities | "runway", "gate", "stand", "turnaround" | VECTOR_AIRPORT |
| **Rule 8** — Omit noise | Always | Drop sources that add no value |

---

## 8. End-to-End Trace Example

Query: _"What is the current departure readiness for SAW in the next 90 minutes?"_

```
1. af_runtime.run_stream()
   ├── [parallel] PII check → pass
   └── [parallel] _routing_task()
       ├── intent_graph_provider.load() → builtin-default graph
       └── router.smart_route(query, intent_graph=graph.data)
           └── router.route(query, intent_graph=graph.data)
               ├── LLM call (gpt-5-nano):
               │   system: ROUTING_PROMPT (350 lines)
               │   user: intent graph JSON (~250 lines)
               │   user: "What is the current departure readiness..."
               │
               └── LLM returns:
                   {
                     "route": "HYBRID",
                     "reasoning": "Departure readiness needs live weather (KQL),
                                   flight schedule data (SQL), and operational
                                   context (VECTOR_OPS)",
                     "sql_hint": "Query ops_flight_legs for SAW departures
                                  in next 90 minutes",
                     "sources": ["KQL", "SQL", "VECTOR_OPS"]
                   }

2. precomputed_route passed to build_context()

3. _build_context_inner()
   ├── agentic_enabled? → yes → _build_agentic_context()
   │   (agentic orchestrator creates its own plan using intent graph)
   │
   └── [if agentic fails] → _build_legacy_context()
       ├── _resolve_route() → uses precomputed_route directly
       │   returns (HYBRID, reasoning, sql_hint, [KQL, SQL, VECTOR_OPS])
       │
       ├── build_retrieval_plan()
       │   └── router_sources=[KQL, SQL, VECTOR_OPS]
       │       → SourcePlan(KQL, p10), SourcePlan(SQL, p11), SourcePlan(VECTOR_OPS, p12)
       │
       ├── _execute_plan()  [parallel]
       │   ├── KQL → query Fabric Eventhouse for METAR/hazards
       │   ├── SQL → query PostgreSQL for flight legs at SAW
       │   └── VECTOR_OPS → search idx_ops_narratives
       │
       ├── _apply_reconciliation()
       │
       └── _compose_context_text()
           → "User query: ... Selected route: HYBRID
              KQL results: [weather data]
              SQL results: [flight schedule]
              VECTOR_OPS results: [narratives]"

4. Synthesis LLM (gpt-5-nano) generates answer from context

5. SSE stream to frontend:
   reasoning_stage(pii_scan) → reasoning_stage(understanding_request)
   → reasoning_stage(intent_mapped) → source_call_start(KQL)
   → source_call_start(SQL) → source_call_start(VECTOR_OPS)
   → source_call_done(KQL) → source_call_done(SQL)
   → source_call_done(VECTOR_OPS) → retrieval_plan
   → reasoning_stage(drafting_brief) → agent_update(text chunks)
   → citations → reasoning_stage(evidence_check_complete) → agent_done
```

---

## 9. Source Availability (Current Deployment)

| Source | AKS Status | Local Dev | Fallback |
|--------|------------|-----------|----------|
| SQL | **Live** (PostgreSQL) | Unavailable | None |
| VECTOR_OPS | **Live** (AI Search) | Unavailable | None |
| VECTOR_REG | **Live** (AI Search) | Unavailable | None |
| VECTOR_AIRPORT | **Live** (AI Search) | Unavailable | None |
| NOSQL | **Live** (Cosmos DB) | Unavailable | Fabric REST |
| GRAPH | **Degraded** (SQL fallback) | Unavailable | SQL via pg |
| KQL | Unavailable | Unavailable | None |
| FABRIC_SQL | Unavailable | Unavailable | None |

The routing LLM will still select KQL and FABRIC_SQL when appropriate — the `_prune_non_viable_tool_calls()` step in the agentic path removes unavailable sources before execution. In the legacy path, `retrieve_source()` returns error rows for unavailable sources, which are flagged as `degradedSources` in the SSE output.

---

## 10. Files Involved

| File | Role |
|------|------|
| `src/af_runtime.py` | Entry point; parallel PII + routing; SSE streaming |
| `src/query_router.py` | `QueryRouter` class; ROUTING_PROMPT; LLM + heuristic routing |
| `src/intent_graph_provider.py` | Intent graph loading (Fabric → file → builtin); `DEFAULT_INTENT_GRAPH` |
| `src/af_context_provider.py` | Context provider; agentic vs legacy path selection; plan execution |
| `src/retrieval_plan.py` | `build_retrieval_plan()`; source plan construction; policy enforcement |
| `src/unified_retriever.py` | `retrieve_source()` dispatch; per-source query execution; synthesis |
| `src/agentic_orchestrator.py` | LLM-based plan creation for agentic path |
| `src/plan_executor.py` | Parallel tool call execution for agentic plans |
| `src/evidence_verifier.py` | Post-retrieval coverage check; re-query suggestions |
| `src/context_reconciler.py` | Deduplication, conflict detection, evidence slotting |
