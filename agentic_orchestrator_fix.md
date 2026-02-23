# Agentic Orchestrator Fix Plan

## Problem Statement

Test results show that **4 of 8 data sources never fire**: VECTOR_OPS, VECTOR_REG, VECTOR_AIRPORT, and FABRIC_SQL. All queries go through the agentic path (`_agentic_enabled = True`), which uses `AgenticOrchestrator.create_plan()` to select tools. The agentic planner consistently selects only SQL, GRAPH, KQL, and NOSQL.

## Observed Test Results

| Source | Fired In | Expected In | Status |
|--------|----------|-------------|--------|
| SQL | 106 tests | widely | LIVE |
| GRAPH | 105 tests | 16+ tests | LIVE |
| KQL | 89 tests | 19+ tests | LIVE (some contract failures) |
| NOSQL | 99 tests | 17+ tests | LIVE |
| VECTOR_OPS | 0 tests | 11+ tests | NEVER FIRES |
| VECTOR_REG | 0 tests | 13+ tests | NEVER FIRES |
| VECTOR_AIRPORT | 0 tests | 16+ tests | NEVER FIRES |
| FABRIC_SQL | 3 tests | 15+ tests | RARELY FIRES |

---

## Root Causes (5 bugs)

### Bug 1: `_canonical_tool_name` drops FABRIC_SQL

**File:** `src/agentic_orchestrator.py:419-437`

The `_canonical_tool_name` method has a mapping dict and a fallback identity check. FABRIC_SQL is missing from both:

```python
mapping = {
    "EVENTHOUSEKQL": "KQL",
    ...
    "NOSQL": "NOSQL",
    "LAKEHOUSEDELTA": "KQL",
    # FABRIC_SQL NOT HERE
}
return mapping.get(value, value if value in {
    "KQL", "SQL", "GRAPH", "VECTOR_REG", "VECTOR_OPS", "VECTOR_AIRPORT", "NOSQL"
    # FABRIC_SQL NOT HERE EITHER
} else "")
```

When the intent graph returns `"FABRIC_SQL"` as a tool name, `_canonical_tool_name` returns `""`, and line 178 filters it out:
```python
canonical_tools = [self._canonical_tool_name(t) for t in tools if self._canonical_tool_name(t)]
# FABRIC_SQL -> "" -> filtered out
```

**Impact:** FABRIC_SQL can never appear in any tool_call, whether LLM-planned or fallback.

**Fix:** Add `"FABRIC_SQL": "FABRIC_SQL"` and `"FABRICSQL": "FABRIC_SQL"` to the mapping dict, and add `"FABRIC_SQL"` to the fallback identity set.

---

### Bug 2: `_fallback_plan` only selects top-priority tool per evidence (`[:1]` slice)

**File:** `src/agentic_orchestrator.py:182`

```python
for tool in canonical_tools[:1]:  # ONLY takes the first (highest-priority) tool
```

The intent graph `authoritative_in` mapping has these priority structures:

| Evidence | Priority 1 | Priority 2 | Priority 3 |
|----------|------------|------------|------------|
| METAR | **KQL** | — | — |
| TAF | **KQL** | — | — |
| NOTAM | **NOSQL** | VECTOR_REG | — |
| RunwayConstraints | **SQL** | — | — |
| Hazards | **KQL** | — | — |
| SOPClause | **VECTOR_REG** | — | — |
| FleetData | **SQL** | — | — |
| RouteData | **GRAPH** | SQL | — |
| SafetyStats | **SQL** | VECTOR_OPS | FABRIC_SQL |
| AirportData | **SQL** | VECTOR_AIRPORT | — |

Since `[:1]` only takes the first tool, VECTOR_OPS (priority 2 on SafetyStats) and VECTOR_AIRPORT (priority 2 on AirportData) are never selected. FABRIC_SQL (priority 3) is doubly blocked (by Bug 1 + this).

VECTOR_REG is the exception — it IS priority 1 for SOPClause. But SOPClause evidence is only required for `Policy.Check` and `Disruption.Explain` intents, which rarely trigger.

**Impact:** In fallback mode, VECTOR_OPS, VECTOR_AIRPORT, and FABRIC_SQL never get tool_calls.

**Fix:** Change `canonical_tools[:1]` to `canonical_tools[:2]` (or iterate all). This ensures the secondary tool also gets a call. Use `depends_on` to avoid redundancy — the secondary call can depend on the primary to enable reranking.

---

### Bug 3: ROUTER_PROMPT lacks tool descriptions

**File:** `src/agentic_orchestrator.py:44-69`

The LLM prompt says "Choose tools only from tool_catalog.allowed_tools" but provides no description of what each tool does. The LLM receives `allowed_tools: ["GRAPH", "KQL", "SQL", "VECTOR_OPS", "VECTOR_REG", "VECTOR_AIRPORT", "NOSQL", "FABRIC_SQL"]` as a raw list with no semantic guidance.

Without descriptions, the LLM defaults to SQL/KQL/GRAPH because:
- SQL is a universally understood concept
- KQL is mentioned with schemas in the payload
- GRAPH has clear semantics from the expansion_rules
- VECTOR_* names are opaque — the LLM doesn't know they're semantic search indexes
- FABRIC_SQL looks like a duplicate of SQL

**Impact:** The LLM almost never selects VECTOR_* or FABRIC_SQL tools.

**Fix:** Add a tool description section to ROUTER_PROMPT:

```
## Tool Descriptions
- SQL: PostgreSQL warehouse. Use for counts, rankings, filters, timelines, airport metadata, route statistics.
- KQL: Fabric Eventhouse (Kusto). Use for real-time/near-real-time data: weather (METAR/TAF), live flight tracking, hazard alerts.
- GRAPH: Graph traversal. Use for dependency analysis, impact chains, alternate airports, route networks.
- VECTOR_OPS: Semantic search over operational narratives. Use for incident reports, near-miss narratives, safety observations, lessons learned. Best when query needs contextual understanding or similarity.
- VECTOR_REG: Semantic search over regulatory documents. Use for NOTAMs, Airworthiness Directives, EASA/FAA bulletins, compliance checks, SOPs.
- VECTOR_AIRPORT: Semantic search over airport operational documents. Use for runway specifications, station information, facility data, airport reference docs.
- NOSQL: Cosmos DB document store. Use for operational NOTAMs, ground handling documents, parking stand assignments.
- FABRIC_SQL: Fabric SQL Warehouse (T-SQL). Use for BTS on-time performance analytics, airline delay causes, schedule statistics. Contains 1.3M+ flight records.

## Tool Selection Rules
- For narrative/similarity queries ("summarize", "similar", "what happened", "lessons"), ALWAYS include a VECTOR_* source.
- For regulatory/compliance queries ("NOTAM", "airworthiness", "directive"), include VECTOR_REG and/or NOSQL.
- For airport operational details ("runway", "gate", "turnaround"), include VECTOR_AIRPORT.
- For delay/performance analytics ("delay", "on-time", "cancellation"), include FABRIC_SQL.
- Select 2-4 complementary sources per query. Don't rely on a single source.
```

---

### Bug 4: `fallback_for` in tool_catalog is incomplete

**File:** `src/af_context_provider.py:281-285`

```python
"fallback_for": {
    "NOTAM": ["VECTOR_REG", "NOSQL"],
    "SOPClause": ["VECTOR_REG"],
    "Hazards": ["KQL"],
    # Missing: SafetyStats, AirportData
}
```

The evidence verifier uses `fallback_for` entries to suggest requery tools when evidence is missing. Since SafetyStats and AirportData have no fallback entries, the verifier never suggests VECTOR_OPS, VECTOR_AIRPORT, or FABRIC_SQL as fallback tools.

**Impact:** Even when the primary SQL tool fails to provide SafetyStats/AirportData evidence, VECTOR_OPS/VECTOR_AIRPORT/FABRIC_SQL are never suggested for requery.

**Fix:** Add missing fallback entries:

```python
"fallback_for": {
    "NOTAM": ["VECTOR_REG", "NOSQL"],
    "SOPClause": ["VECTOR_REG"],
    "Hazards": ["KQL"],
    "SafetyStats": ["VECTOR_OPS", "FABRIC_SQL"],
    "AirportData": ["VECTOR_AIRPORT"],
    "FleetData": ["FABRIC_SQL"],
}
```

---

### Bug 5: Intent graph evidence types don't cover all query patterns

**File:** `src/intent_graph_provider.py:45-56` (evidence list) and `src/agentic_orchestrator.py:377-397` (`_infer_intent`)

The intent graph defines 10 evidence types: METAR, TAF, NOTAM, RunwayConstraints, Hazards, SOPClause, FleetData, RouteData, SafetyStats, AirportData.

Missing evidence types that would trigger VECTOR_* sources:
- **IncidentNarrative** → VECTOR_OPS (for "summarize incidents", "bird strike reports", "what happened")
- **RegulatoryDoc** → VECTOR_REG (for "airworthiness directive", "EASA bulletin", "compliance")
- **DelayAnalytics** → FABRIC_SQL (for "delay causes", "on-time performance", "cancellation rate")

The `_infer_intent` heuristic also has gaps — queries about summaries, narratives, or delay analytics don't map to intents that require VECTOR_OPS or FABRIC_SQL evidence.

**Impact:** The evidence-driven pipeline never creates demand for VECTOR_OPS/FABRIC_SQL because no evidence type exclusively requires them.

**Fix:** Add new evidence types and intent-evidence-tool mappings to the default intent graph.

---

## Fix Implementation Plan

### Step 1: Fix `_canonical_tool_name` (Bug 1) — `src/agentic_orchestrator.py`

Add FABRIC_SQL to the mapping dict and fallback set:

```python
mapping = {
    ...existing entries...
    "FABRIC_SQL": "FABRIC_SQL",
    "FABRICSQL": "FABRIC_SQL",
}
return mapping.get(value, value if value in {
    "KQL", "SQL", "GRAPH", "VECTOR_REG", "VECTOR_OPS", "VECTOR_AIRPORT", "NOSQL", "FABRIC_SQL"
} else "")
```

### Step 2: Expand `_fallback_plan` to select top-2 tools (Bug 2) — `src/agentic_orchestrator.py`

Change `canonical_tools[:1]` to `canonical_tools[:2]`:

```python
for tool in canonical_tools[:2]:  # Select top-2 priority tools per evidence
```

This ensures VECTOR_OPS gets a tool_call alongside SQL for SafetyStats, and VECTOR_AIRPORT alongside SQL for AirportData.

### Step 3: Enhance ROUTER_PROMPT with tool descriptions (Bug 3) — `src/agentic_orchestrator.py`

Add a tool description + selection rules section to ROUTER_PROMPT (see Bug 3 section above for exact text).

### Step 4: Expand `fallback_for` (Bug 4) — `src/af_context_provider.py`

Add SafetyStats, AirportData, and FleetData entries to `fallback_for` dict.

### Step 5: Add new evidence types to intent graph (Bug 5) — `src/intent_graph_provider.py`

Add to `DEFAULT_INTENT_GRAPH`:

**New evidence types:**
```python
{"name": "IncidentNarrative", "requires_citations": True},
{"name": "RegulatoryDoc", "requires_citations": True},
{"name": "DelayAnalytics", "requires_citations": False},
```

**New authoritative_in entries:**
```python
{"evidence": "IncidentNarrative", "tool": "VECTOR_OPS", "priority": 1},
{"evidence": "IncidentNarrative", "tool": "SQL", "priority": 2},
{"evidence": "RegulatoryDoc", "tool": "VECTOR_REG", "priority": 1},
{"evidence": "DelayAnalytics", "tool": "FABRIC_SQL", "priority": 1},
{"evidence": "DelayAnalytics", "tool": "SQL", "priority": 2},
```

**New requires entries** (connect evidence to intents):
```python
{"intent": "Safety.Trend", "evidence": "IncidentNarrative", "optional": False},
{"intent": "PilotBrief.Departure", "evidence": "RegulatoryDoc", "optional": True},
{"intent": "PilotBrief.Arrival", "evidence": "RegulatoryDoc", "optional": True},
{"intent": "Disruption.Explain", "evidence": "IncidentNarrative", "optional": True},
{"intent": "Disruption.Explain", "evidence": "DelayAnalytics", "optional": True},
{"intent": "Analytics.Compare", "evidence": "DelayAnalytics", "optional": False},
{"intent": "Analytics.Compare", "evidence": "IncidentNarrative", "optional": True},
```

**New `_infer_intent` keywords** in `agentic_orchestrator.py`:
```python
if any(t in q for t in ("summarize", "similar", "narrative", "lessons", "bird strike")):
    return "Safety.Trend"  # This already exists but needs these keywords added
if any(t in q for t in ("delay", "on-time", "cancellation", "schedule performance")):
    return "Analytics.Compare"  # needs delay keywords
if any(t in q for t in ("airworthiness", "directive", "easa", "bulletin", "regulatory")):
    return "Policy.Check"  # needs regulatory keywords
```

### Step 6: Add query-driven heuristic enrichment as post-LLM safety net — `src/agentic_orchestrator.py`

Even after the LLM plan is generated, apply `_wants_*`-style checks to ensure critical sources aren't missed. Add a new method `_enrich_plan_with_heuristics`:

```python
def _enrich_plan_with_heuristics(self, plan: AgenticPlan, user_query: str, allowed_tools: List[str]) -> AgenticPlan:
    """Post-LLM heuristic: ensure query-relevant sources are included."""
    query_l = user_query.lower()
    existing_tools = {self._canonical_tool_name(c.tool) for c in plan.tool_calls}
    next_id = len(plan.tool_calls) + 1

    enrichments = []
    # Narrative/similarity -> VECTOR_OPS
    if any(m in query_l for m in ("summarize", "similar", "narrative", "what happened", "examples", "lessons")):
        enrichments.append(("VECTOR_OPS", "semantic_lookup", "Query mentions narrative/similarity"))
    # Regulatory -> VECTOR_REG
    if any(m in query_l for m in ("airworthiness", "notam", "easa", "compliance", "directive", "ad ")):
        enrichments.append(("VECTOR_REG", "semantic_lookup", "Query mentions regulatory content"))
    # Airport ops -> VECTOR_AIRPORT
    if any(m in query_l for m in ("runway", "gate", "turnaround", "airport", "station")):
        enrichments.append(("VECTOR_AIRPORT", "semantic_lookup", "Query mentions airport operations"))
    # Delay analytics -> FABRIC_SQL
    if any(m in query_l for m in ("delay", "on-time", "cancellation", "schedule performance", "bts")):
        enrichments.append(("FABRIC_SQL", "sql_lookup", "Query mentions delay/performance analytics"))

    for tool, op, reason in enrichments:
        if tool in existing_tools:
            continue
        if allowed_tools and tool not in allowed_tools:
            continue
        plan.tool_calls.append(ToolCall(
            id=f"enrich_{next_id}",
            tool=tool,
            operation=op,
            depends_on=[],
            query=user_query,
            params={"evidence_type": "heuristic_enrichment", "reason": reason},
        ))
        next_id += 1
        existing_tools.add(tool)

    return plan
```

Call this after `_ensure_required_evidence_calls` in `create_plan()`:

```python
plan = self._ensure_required_evidence_calls(plan, ...)
plan = self._enrich_plan_with_heuristics(plan, user_query, allowed_tools)
return self._enforce_required_sources(plan, required_sources or [])
```

---

## Implementation Order

| Step | Bug | File | Change Size | Risk |
|------|-----|------|------------|------|
| 1 | Bug 1 | `agentic_orchestrator.py:419-437` | 3 lines | None |
| 2 | Bug 2 | `agentic_orchestrator.py:182` | 1 line | Low — may add 1 extra call per evidence |
| 3 | Bug 3 | `agentic_orchestrator.py:44-69` | ~25 lines | Low — prompt change, LLM behavior may vary |
| 4 | Bug 4 | `af_context_provider.py:281-285` | 3 lines | None |
| 5 | Bug 5 | `intent_graph_provider.py:32-131` + `agentic_orchestrator.py:377-397` | ~25 lines | Low — adds evidence types |
| 6 | New | `agentic_orchestrator.py` (new method) | ~30 lines | Medium — heuristic may add unwanted calls |

## Expected Impact

After all fixes:
- **VECTOR_OPS**: Will fire for narrative/similarity/incident queries via Bug 2 fix (secondary tool for SafetyStats) + Bug 5 (IncidentNarrative evidence) + Bug 6 (heuristic enrichment)
- **VECTOR_REG**: Will fire for regulatory/NOTAM/compliance queries via Bug 5 (RegulatoryDoc evidence) + Bug 6 (heuristic enrichment). Already fires for SOPClause under Policy.Check intent.
- **VECTOR_AIRPORT**: Will fire for airport/runway queries via Bug 2 fix (secondary tool for AirportData) + Bug 6 (heuristic enrichment)
- **FABRIC_SQL**: Will fire for delay/performance queries via Bug 1 fix (canonical name) + Bug 4 (fallback_for) + Bug 5 (DelayAnalytics evidence) + Bug 6 (heuristic enrichment)

## Verification

After implementing all fixes:

```bash
# Dry-run validation (no backend needed)
python scripts/17_test_all_sources.py --dry-run

# Full E2E test
python scripts/17_test_all_sources.py --backend http://localhost:5001

# Quick smoke test for previously-broken sources
python scripts/17_test_all_sources.py --filter T3,T5,T7,T15,S14,S19,S21,S31 -v
```

Target: All 8 sources should appear in Source Coverage table. VECTOR_OPS, VECTOR_REG, VECTOR_AIRPORT, FABRIC_SQL should each fire in 5+ tests.
