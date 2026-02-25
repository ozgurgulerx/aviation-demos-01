import type { Conversation, Message, Citation, WatchlistItem, PipelineConfig } from "@/types";

export const SAMPLE_CITATIONS: Citation[] = [
  {
    id: 1,
    provider: "Microsoft Fabric Eventhouse",
    dataset: "ops_turnaround_milestones",
    rowId: "LEG-PGT161K-20260220T0714Z",
    timestamp: "2026-02-20T07:14:22Z",
    confidence: 0.97,
    excerpt:
      "Gate-open to off-block elapsed 31 minutes; no delay_cause_code assigned.",
  },
  {
    id: 2,
    provider: "Microsoft Fabric Warehouse",
    dataset: "ops_crew_rosters",
    rowId: "DUTY-CAP002-20260220-AM",
    timestamp: "2026-02-20T06:52:00Z",
    confidence: 0.94,
    excerpt:
      "Captain CAP002 remains legal for next sector with 1h43m remaining before duty threshold.",
  },
  {
    id: 3,
    provider: "Azure AI Search",
    dataset: "regulatory_docs",
    rowId: "EASA-CF-2026-08",
    timestamp: "2026-02-19T18:11:00Z",
    confidence: 0.9,
    excerpt:
      "Applicable bulletin references overhead compartment latch inspection cycle and escalation criteria.",
  },
];

const SAMPLE_MESSAGES: Message[] = [
  {
    id: "msg-u-1",
    role: "user",
    content:
      "Create a 90-minute departure risk brief for SAW to IST morning bank and highlight crew or turnaround constraints.",
    createdAt: new Date("2026-02-20T07:20:00Z"),
  },
  {
    id: "msg-a-1",
    role: "assistant",
    content: `## Pilot Brief - SAW -> IST (next 90 minutes)

### Operational posture
- **Readiness**: Stable, low disruption risk in current window.
- **Turnaround performance**: Last two legs completed inside target envelope [1].
- **Crew legality**: Assigned flight deck remains within legal duty boundaries [2].

### Watch items
1. Maintain gate servicing sequence to preserve current buffer.
2. Re-check weather hazard feed at T-30 for micro-cell updates.
3. Keep maintenance standby aligned with compartment bulletin requirements [3].

### Confidence and limits
- Confidence is **high** for current window, but this may shift if weather or stand assignment changes.
- This brief uses available telemetry and document evidence only; no manual dispatch override data was provided.`,
    createdAt: new Date("2026-02-20T07:20:16Z"),
    citations: SAMPLE_CITATIONS,
    isVerified: true,
  },
];

export const SAMPLE_CONVERSATIONS: Conversation[] = [
  {
    id: "conv-pilot-brief",
    title: "Morning Bank Risk Brief",
    createdAt: new Date("2026-02-20T07:20:00Z"),
    updatedAt: new Date("2026-02-20T07:20:16Z"),
    isSaved: true,
    messages: SAMPLE_MESSAGES,
  },
  {
    id: "conv-maint",
    title: "MEL Escalation Snapshot",
    createdAt: new Date("2026-02-19T14:05:00Z"),
    updatedAt: new Date("2026-02-19T14:09:00Z"),
    isSaved: false,
    messages: [],
  },
  {
    id: "conv-reg",
    title: "Regulatory Exposure Check",
    createdAt: new Date("2026-02-18T09:00:00Z"),
    updatedAt: new Date("2026-02-18T09:12:00Z"),
    isSaved: true,
    messages: [],
  },
];

export const SAMPLE_WATCHLIST: WatchlistItem[] = [
  {
    id: "w1",
    type: "airport",
    name: "SAW turnaround wave",
    addedAt: new Date("2026-02-20T07:00:00Z"),
  },
  {
    id: "w2",
    type: "aircraft",
    name: "Crew legality thresholds",
    addedAt: new Date("2026-02-20T06:40:00Z"),
  },
  {
    id: "w3",
    type: "route",
    name: "NOTAM-driven route impact",
    addedAt: new Date("2026-02-19T22:10:00Z"),
  },
];

export type QueryType =
  | "ops-live"
  | "safety"
  | "network"
  | "maintenance"
  | "compliance";

export interface FollowUpSuggestion {
  text: string;
  type: QueryType;
  sources?: number;
}

export const ENHANCED_FOLLOW_UP_SUGGESTIONS: FollowUpSuggestion[] = [
  // --- Tier 1 (1-2 sources) ---
  {
    text: "Compare next-90-minute departure risk across JFK, LGA, and EWR.",
    type: "ops-live",
    sources: 1,
  },
  {
    text: "Trace dependency chain for a delayed inbound tail and downstream legs.",
    type: "network",
    sources: 2,
  },
  {
    text: "List applicable AD/NOTAM documents tied to current disruptions.",
    type: "compliance",
    sources: 2,
  },
  // --- Tier 2 (3-4 sources) ---
  {
    text: "Compare BTS cancellation rates at JFK over past winters with current SIGMET coverage and turnaround bottlenecks.",
    type: "ops-live",
    sources: 3,
  },
  {
    text: "For carriers with the worst on-time record to EWR, surface matching ASRS safety narratives and delay root causes.",
    type: "safety",
    sources: 3,
  },
  {
    text: "Identify legs where baggage mishandling, crew duty overruns, and turnaround delays coincide.",
    type: "maintenance",
    sources: 3,
  },
  {
    text: "Which airworthiness directives apply to tails currently flying with deferred MEL items?",
    type: "compliance",
    sources: 3,
  },
  // --- Tier 3 (5+ sources) ---
  {
    text: "Assess full operational risk for the IST evening bank: live weather hazards, historical delay trends, active NOTAMs, crew legality status, and related ASRS incident history.",
    type: "ops-live",
    sources: 5,
  },
  {
    text: "Map the full disruption cascade if TC-JHZ is grounded at SAW: downstream flight network, crew re-assignments, baggage impacts, regulatory constraints, and airport facility alternatives.",
    type: "network",
    sources: 5,
  },
  {
    text: "Produce a go/no-go departure briefing for SAW: live SIGMETs, active NOTAMs, MEL and crew status, on-time baseline, runway procedures, and related safety narratives.",
    type: "ops-live",
    sources: 6,
  },
];

export interface QueryCategory {
  id: QueryType;
  title: string;
  description: string;
  icon: string;
  tone: "blue" | "orange" | "teal";
  examples: string[];
}

export const QUERY_CATEGORIES: QueryCategory[] = [
  {
    id: "ops-live",
    title: "Live Operations",
    description: "Sub-minute telemetry for gates, turnaround and departure posture",
    icon: "Radar",
    tone: "blue",
    examples: [
      "What is the current departure readiness for SAW in the next 90 minutes?",
      "Which flights are at risk due to stand congestion right now?",
    ],
  },
  {
    id: "safety",
    title: "Safety Signals",
    description: "Crew legality, hazards and safety constraints",
    icon: "ShieldAlert",
    tone: "teal",
    examples: [
      "Highlight legality risk flags for crew on the next wave.",
      "What weather hazards may affect climb and approach in this window?",
    ],
  },
  {
    id: "network",
    title: "Dependency Graph",
    description: "Multi-hop route and asset dependencies for disruption tracing",
    icon: "Workflow",
    tone: "blue",
    examples: [
      "Show downstream impact if PGT161K departs 20 minutes late.",
      "Map aircraft-tail dependency chain for IST evening bank.",
    ],
  },
  {
    id: "maintenance",
    title: "Maintenance Context",
    description: "MEL, techlog and dispatchability overlays",
    icon: "Wrench",
    tone: "orange",
    examples: [
      "Which active MEL items could block dispatch in the next 4 hours?",
      "Summarize recurring defects by tail across current schedule.",
    ],
  },
  {
    id: "compliance",
    title: "Regulatory Evidence",
    description: "AD, NOTAM and policy material with citation-level grounding",
    icon: "BookCheck",
    tone: "teal",
    examples: [
      "What regulatory docs apply to this delay pattern?",
      "Retrieve NOTAM and AD references for LTBA/LTFM constraints.",
    ],
  },
];

export const ALL_QUERY_EXAMPLES = QUERY_CATEGORIES.flatMap((category) =>
  category.examples.map((text) => ({
    text,
    type: category.id,
    tone: category.tone,
  }))
);

export const PIPELINE_CONFIGS: PipelineConfig[] = [
  {
    id: "code-rag",
    name: "Custom Agentic Retrieval",
    tagline: "Multi-Source Evidence Orchestration",
    description:
      "Purpose-built RAG pipeline with deterministic SQL, vector search, and live telemetry — orchestrated by an agentic query router.",
    dataSources: [
      "PostgreSQL (SQL)",
      "AI Search (Vector)",
      "Eventhouse (KQL)",
      "Cosmos DB (NOTAMs)",
    ],
    accentColor: "blue",
  },
  {
    id: "foundry-iq",
    name: "Foundry IQ",
    tagline: "Semantic Intelligence",
    description:
      "Microsoft Fabric Data Agent with built-in grounding, semantic ranking, and managed retrieval across Lakehouse and OneLake assets.",
    dataSources: [
      "Fabric Data Agent",
      "OneLake Lakehouse",
      "Fabric Semantic Model",
    ],
    accentColor: "teal",
  },
];

export const DATA_PROVIDERS = [
  { name: "Fabric Eventhouse (KQL)", type: "Telemetry" },
  { name: "Fabric Warehouse (SQL)", type: "Relational" },
  { name: "Fabric Graph (Preview)", type: "Dependency" },
  { name: "Azure AI Search", type: "Vector" },
  { name: "Azure Cosmos DB", type: "NOTAM" },
];

export interface DataSourceBlueprint {
  id: string;
  label: string;
  datastore: string;
  retrievalMode: string;
  rationale: string;
}

export const DATA_SOURCE_BLUEPRINT: DataSourceBlueprint[] = [
  {
    id: "KQL",
    label: "Live telemetry + hazards",
    datastore: "Fabric Eventhouse (KQL)",
    retrievalMode: "Event-window scans",
    rationale: "Used for sub-minute operational windows and anomaly checks.",
  },
  {
    id: "SQL",
    label: "ASRS reports & airport ops",
    datastore: "Azure PostgreSQL (SQL)",
    retrievalMode: "Deterministic SQL joins",
    rationale: "Used where auditable KPI logic and constraint joins are required.",
  },
  {
    id: "FABRIC_SQL",
    label: "BTS delay analytics",
    datastore: "Fabric SQL Warehouse",
    retrievalMode: "Deterministic T-SQL joins",
    rationale: "Used for BTS on-time performance, delay causes, and schedule statistics.",
  },
  {
    id: "GRAPH",
    label: "Dependency traversal",
    datastore: "Fabric Graph (Preview)",
    retrievalMode: "Multi-hop graph walk",
    rationale: "Used to expose disruption propagation across route and asset edges.",
  },
  {
    id: "VECTOR_REG",
    label: "Narratives + regulations",
    datastore: "Azure AI Search (vector + hybrid)",
    retrievalMode: "Semantic + lexical retrieval",
    rationale: "Used for document-heavy safety and regulatory grounding.",
  },
  {
    id: "NOSQL",
    label: "NOTAM store",
    datastore: "Azure Cosmos DB (NOTAMs)",
    retrievalMode: "Document point reads",
    rationale: "Used for active NOTAM retrieval by airport ICAO code.",
  },
];

export const PAIR_TRUST_NOTES = [
  "System status and source activity are always visible during execution.",
  "Final answers expose confidence and known limitations.",
  "Users can constrain or re-run retrieval with explicit source controls.",
];

export function getDataAsOfTimestamp(): string {
  return new Date().toISOString().replace("T", " ").slice(0, 16) + " UTC";
}
