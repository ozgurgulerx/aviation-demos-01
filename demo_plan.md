# SunExpress Quick Demo Plan: Prediction Optimization, Semantic Layer, Decision Intelligence

## Summary
Build **3 flagship live demos** using your existing stack and datasets, with no core platform rewrite.
The demos should prove:
1. You can **predict and optimize** operational outcomes.
2. You can create a **semantic layer** over fragmented aviation data.
3. You can drive **decision intelligence** with explainable, source-backed recommendations.

This plan is optimized for fast preparation (1-3 days) and high stakeholder impact.

## Current Data You Can Reliably Use (already in repo)
- ASRS structured records: `150,257` (`aviation.db`, date range `1987-09-01` to `2014-08-01`).
- ASRS semantic docs: `240,605` (`data/c1-asrs/processed/asrs_documents.jsonl`).
- Vector corpora summary: `ops_docs=240,778`, `reg_docs=475`, `airport_docs=210,853` (`data/vector_docs/summary.json`).
- Synthetic ops overlay (`2026-02-19` complete snapshot):
  - flight legs `173`
  - turnaround milestones `1,384`
  - baggage events `682`
  - crew roster rows `519` (risk-flagged `192`)
  - tech events `41`
- Enriched graph edges CSV: `311,987` rows (`data/enriched_graph_edges.csv`).
- NOTAM JSONL corpus: `548` rows across 5 files.
- Airport/network base:
  - OurAirports runways `47,603`
  - navaids `11,011`
  - airport frequencies `30,212`
  - OpenFlights routes `67,663`
- Schedule feed snapshot (`2026-02-19`): BTS on-time zips (`2025-10`, `2025-11`) + delay-causes zip.

Reason this matters: this is enough to show real cross-source AI behavior without waiting for new ingestion work.

## Demo 1: Prediction Optimization
### Scenario
**"Turnaround and disruption risk optimizer for next-wave departures."**

### Flow
1. Ask for flights with highest delay/disruption risk in the next window.
2. Model combines proxy features from existing tables:
- turnaround delay cause patterns (`ATC`, `MX`, `WX`, `BAG`)
- crew legality risk flag
- MEL/tech severity
- route/airport complexity via graph connectivity
- historical delay-cause priors
3. Return ranked flights + recommended interventions:
- swap crew
- gate resequence
- maintenance priority
- baggage staffing shift
4. Show "before vs after" KPI estimate:
- predicted delay minutes
- expected on-time improvement
- risk reduction score

### Why this demonstrates Prediction Optimization
- Prediction: risk scoring from multi-source operational signals.
- Optimization: recommendation set that minimizes expected delay impact.
- Business relevance: directly maps to OTP, cost, and passenger experience.

### Suggested live prompt examples
- "Which 10 legs in the next 6 hours have the highest delay risk and what intervention gives the biggest OTP gain?"
- "If MX and WX events both increase at SAW, what is the best mitigation order?"

## Demo 2: Semantic Layer
### Scenario
**"Unified airline semantic Q&A across incidents, regulations, airport ops, and NOTAM context."**

### Flow
1. Ask a mixed business/operational question using natural language.
2. Planner retrieves from:
- `VECTOR_OPS` (narratives)
- `VECTOR_REG` (regulatory)
- `VECTOR_AIRPORT` (airport/runway docs)
- `SQL` for structured grounding
3. Response provides:
- normalized interpretation of terms (e.g., "runway closure risk," "dispatch impact," "MEL relevance")
- citation-backed answer across multiple corpora
- confidence/limitations statement

### Why this demonstrates Semantic Layer
- Shows that fragmented schemas can be queried as a single business language.
- Reduces dependency on knowing raw table names or source location.
- Makes cross-domain data usable by non-technical operations users.

### Suggested live prompt examples
- "For wet-runway operations at Istanbul airports, summarize key operational risks, relevant NOTAM/AD context, and similar historical narratives."
- "Explain how MEL-related issues and runway constraints interact operationally for short-haul rotations."

## Demo 3: Decision Intelligence
### Scenario
**"Network disruption command center: explainable option ranking under constraints."**

### Flow
1. Trigger disruption scenario:
- runway closure / severe weather / tech issue at a hub.
2. Agent builds decision options:
- keep schedule and absorb delay
- reroute selected legs
- swap aircraft/crew
- cancel low-priority rotations
3. For each option, output a decision card:
- operational impact
- compliance risk
- customer impact
- confidence and evidence
- recommended action with rationale
4. Show provenance via retrieval-plan and source-call events.

### Why this demonstrates Decision Intelligence
- Not just analytics; it compares alternatives and recommends a choice.
- Transparent reasoning with evidence increases trust for ops leadership.
- Demonstrates "human-in-the-loop AI advisor," not black-box automation.

### Suggested live prompt examples
- "A runway closure starts at SAW for 90 minutes. Rank 3 mitigation strategies and justify each."
- "Given crew legality and MEL constraints, what is the least-cost disruption strategy with best OTP retention?"

## Cross-Demo Delivery Pattern (fastest way)
1. Use existing `/api/chat` pipeline with:
- `query_profile=ops-live` or `pilot-brief`
- `explain_retrieval=true`
- `required_sources` per demo
2. Pre-create a "demo script" with 5-7 prompts per theme.
3. Keep one fallback prompt per theme that uses only SQL + vector if Fabric source is unavailable.
4. Keep a one-slide "what AI is doing now" per demo:
- inputs
- reasoning
- recommendation
- KPI impact

## Important API / Interface Changes
No mandatory backend API change is required to run these demos.

Optional (recommended) thin additions for clarity:
1. Add `demo_scenario` presets in frontend to auto-fill source hints and prompt templates.
2. Add response block `decision_options[]` for Demo 3 to standardize option comparison.
3. Add response block `prediction_features[]` + `predicted_delta_kpi` for Demo 1.

These are additive, backward-compatible enhancements to existing `/api/chat` usage.

## Test Cases and Demo Acceptance
1. Prediction Optimization acceptance:
- For a fixed scenario prompt, system returns ranked risk list and at least 2 interventions with estimated KPI deltas.
- Output includes source citations and no uncited KPI claim.
2. Semantic Layer acceptance:
- Same question answered using at least 2 heterogeneous source types (vector + structured or vector + regulatory).
- Response includes explicit limitation when data recency is insufficient.
3. Decision Intelligence acceptance:
- At least 3 options are generated and ranked with explicit tradeoffs.
- Each option has evidence references and a confidence note.
4. Reliability acceptance:
- If Fabric endpoint fails, response degrades gracefully and states which source is unavailable.
- Demo still completes with available sources.
5. Explainability acceptance:
- Retrieval plan/source-call telemetry visible during live run.

## Assumptions and Defaults
- Primary audience is mixed business + operations stakeholders.
- Goal is quick-win demos, not production-grade optimization engine.
- Existing snapshots (`2026-02-19`/`2026-02-20`) are acceptable for demonstration.
- Live Fabric connectivity may be partial; plan includes graceful fallback paths.
- Subscription/tenant references in docs are inconsistent across files; before live exec, lock runtime context to one approved tenant/subscription and validate via preflight.
