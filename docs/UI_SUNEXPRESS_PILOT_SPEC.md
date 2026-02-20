# SunExpress Pilot Brief Bot UI Specification

## Purpose
This UI presents the pilot brief assistant as an explainable retrieval system, not just a chat box. It emphasizes:
- live orchestration visibility
- audited evidence usage
- confidence and limitation signaling
- user control over retrieval constraints

## Audience
Primary audience: SunExpress operations and executive stakeholders evaluating a pilot deployment.

## Experience Structure
1. Mission header
- SunExpress pilot branding
- data freshness strip
- evidence mode status

2. Workbench body
- left rail: brief history, scenario presets, watchlist
- center canvas: telemetry timeline + chat synthesis
- right rail: evidence ledger + source status + confidence and trust notes

3. Control surface
- retrieval mode switch
- query profile switch
- explainability toggle
- source filter chips
- architecture modal trigger

## Visual System
- Brand blue for structure and trust surfaces
- Accent orange for action and urgency
- clean light background with subtle aviation-grid texture
- card-based hierarchy with medium corner radius and restrained shadows
- display typography for section headlines, mono for telemetry and IDs

## SSE Telemetry Mapping
- `agent_update`: stage updates and run progress
- `retrieval_plan`: planner milestone
- `source_call_start`: source marked as querying
- `source_call_done`: source marked as ready with row count
- `tool_call`/`tool_result`: execution trace rows
- `agent_done`: route + completion status
- `agent_error`: error state + recovery messaging

## HAX + PAIR Alignment
- real-time status and source visibility during execution
- confidence state displayed at answer closeout
- explicit note when answers can be incomplete
- retrieval constraint controls available pre-run
- provenance visible by default in evidence ledger

## Component Inventory
- Header: mission/brand context and global status
- Sidebar: sessions, presets, watchlist
- ChatThread: timeline + response stream + quick-start categories
- SourcesPanel: source health, evidence manifest, trust guarantees
- ArchitectureMap: context-to-datastore mapping with live status highlights

## Acceptance Checklist
- telemetry updates appear in center timeline while run is active
- source state badges transition idle -> querying -> ready
- citations populate right ledger with dataset and row references
- confidence label updates at run completion
- architecture modal reflects current source status
- mobile and desktop layouts remain usable and readable
