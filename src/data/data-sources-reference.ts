// ---------------------------------------------------------------------------
// Data Sources Reference — 8 canonical sources for the Aviation RAG platform
// ---------------------------------------------------------------------------

/* ── Badge colour key (Tailwind class prefixes) ── */
export type BadgeVariant =
  | "sql"
  | "kql"
  | "vector"
  | "nosql"
  | "graph"
  | "fabric";

export const BADGE_COLORS: Record<BadgeVariant, { bg: string; text: string; border: string }> = {
  sql:    { bg: "bg-blue-500/10",   text: "text-blue-400",   border: "border-blue-500/30" },
  kql:    { bg: "bg-teal-500/10",   text: "text-teal-400",   border: "border-teal-500/30" },
  vector: { bg: "bg-purple-500/10", text: "text-purple-400", border: "border-purple-500/30" },
  nosql:  { bg: "bg-orange-500/10", text: "text-orange-400", border: "border-orange-500/30" },
  graph:  { bg: "bg-green-500/10",  text: "text-green-400",  border: "border-green-500/30" },
  fabric: { bg: "bg-amber-500/10",  text: "text-amber-400",  border: "border-amber-500/30" },
};

// ── Schema ──────────────────────────────────────────────────────────────────

export interface SchemaColumn {
  name: string;
  description: string;
}

export interface SchemaTable {
  name: string;
  columns: SchemaColumn[];
  description: string;
}

export interface SampleQuery {
  query: string;
  route: string;
}

export interface MetaItem {
  label: string;
  value: string;
}

export interface DataSourceReference {
  id: string;
  title: string;
  badgeVariant: BadgeVariant;
  storeTag: string;
  whatIsThis: string;
  content: string;
  schemaTables: SchemaTable[];
  triggerKeywords: string[];
  triggerNote?: string;
  sampleQueries: SampleQuery[];
  exampleRecord: Record<string, unknown>;
  meta: MetaItem[];
}

// ── 8 Data Sources ──────────────────────────────────────────────────────────

export const DATA_SOURCES: DataSourceReference[] = [
  // 1. SQL
  {
    id: "SQL",
    title: "PostgreSQL Warehouse",
    badgeVariant: "sql",
    storeTag: "Azure PostgreSQL · aistartupstr.postgres.database.azure.com",
    whatIsThis:
      "ASRS (Aviation Safety Reporting System) reports are confidential safety incident narratives voluntarily submitted by pilots, controllers, and crew to NASA. The SQL source also holds 20 operational tables — flight legs with delay metrics, crew duty rosters with legality tracking, MEL/techlog entries, baggage events, turnaround milestones, plus airport/runway/navaid reference data from OurAirports and route network data from OpenFlights.",
    content:
      "Relational database holding ASRS safety reports plus 20 operational and reference tables: flight legs, turnaround milestones, crew rosters, MEL/techlog events, baggage events, airport reference (OurAirports), airline/route network (OpenFlights), weather hazards (AIRSIGMETs, G-AIRMETs), and parsed NOTAMs.",
    schemaTables: [
      {
        name: "asrs_reports",
        description: "ASRS safety/incident reports",
        columns: [
          { name: "asrs_report_id", description: "Unique report identifier" },
          { name: "event_date", description: "Date of the event" },
          { name: "location", description: "Location of the event" },
          { name: "aircraft_type", description: "Aircraft type involved" },
          { name: "flight_phase", description: "Phase of flight" },
          { name: "title", description: "Report title" },
          { name: "report_text", description: "Full narrative text" },
        ],
      },
      {
        name: "ops_flight_legs",
        description: "Flight schedule + actuals + delay metrics",
        columns: [
          { name: "leg_id", description: "Unique leg identifier" },
          { name: "carrier_code", description: "Airline carrier code" },
          { name: "flight_no", description: "Flight number" },
          { name: "origin_iata", description: "Departure airport IATA" },
          { name: "dest_iata", description: "Arrival airport IATA" },
          { name: "scheduled_dep_utc", description: "Scheduled departure (UTC)" },
          { name: "actual_dep_utc", description: "Actual departure (UTC)" },
          { name: "dep_delay_min", description: "Departure delay in minutes" },
          { name: "tailnum", description: "Aircraft tail number" },
        ],
      },
      {
        name: "ops_turnaround_milestones",
        description: "Ground handling milestone tracking",
        columns: [
          { name: "milestone_id", description: "Unique milestone ID" },
          { name: "leg_id", description: "Associated flight leg" },
          { name: "milestone", description: "Milestone type" },
          { name: "event_ts_utc", description: "Event timestamp (UTC)" },
          { name: "status", description: "Completion status" },
          { name: "delay_cause_code", description: "Root cause of delay" },
        ],
      },
      {
        name: "ops_crew_rosters",
        description: "Crew duty assignments + legality",
        columns: [
          { name: "duty_id", description: "Duty period identifier" },
          { name: "crew_id", description: "Crew member identifier" },
          { name: "role", description: "Captain / FO / Cabin" },
          { name: "leg_id", description: "Associated flight leg" },
          { name: "cumulative_duty_hours", description: "Hours on duty" },
          { name: "legality_risk_flag", description: "Duty-time legality flag" },
        ],
      },
      {
        name: "ops_mel_techlog_events",
        description: "MEL and tech log entries",
        columns: [
          { name: "tech_event_id", description: "Unique tech event ID" },
          { name: "leg_id", description: "Associated flight leg" },
          { name: "jasc_code", description: "JASC system code" },
          { name: "mel_category", description: "A/B/C/D classification" },
          { name: "deferred_flag", description: "Whether deferred" },
          { name: "severity", description: "Impact severity" },
        ],
      },
      {
        name: "ourairports_airports",
        description: "Airport reference data (70K airports)",
        columns: [
          { name: "ident", description: "Airport identifier" },
          { name: "name", description: "Airport name" },
          { name: "iata_code", description: "IATA code" },
          { name: "latitude_deg", description: "Latitude" },
          { name: "elevation_ft", description: "Elevation in feet" },
        ],
      },
      {
        name: "openflights_routes",
        description: "Route network (67K routes)",
        columns: [
          { name: "airline", description: "Airline code" },
          { name: "source_airport", description: "Origin airport" },
          { name: "dest_airport", description: "Destination airport" },
          { name: "stops", description: "Number of stops" },
          { name: "equipment", description: "Aircraft type" },
        ],
      },
    ],
    triggerKeywords: [
      "how many", "count", "top N", "average", "total", "compare", "list",
      "trend", "by year", "rankings", "fleet metrics", "MEL", "crew duty",
      "turnaround", "cross-table joins",
    ],
    sampleQueries: [
      { query: "How many ASRS reports involve engine failure during initial climb?", route: "SQL" },
      { query: "Which flights departing IST have crew duty hours exceeding 10 hours?", route: "SQL" },
      { query: "Top 5 airports by average turnaround delay this month", route: "SQL" },
    ],
    exampleRecord: {
      leg_id: "LEG-TK1780-20260220",
      carrier_code: "TK",
      flight_no: "TK1780",
      origin_iata: "SAW",
      dest_iata: "ESB",
      scheduled_dep_utc: "2026-02-20T08:30:00Z",
      actual_dep_utc: "2026-02-20T08:42:00Z",
      dep_delay_min: 12,
      tailnum: "TC-JHZ",
      passengers: 187,
    },
    meta: [
      { label: "Freshness", value: "Batch ETL · Scripts 00-02, 09, 14" },
      { label: "Query Method", value: "Deterministic SQL (via SQLGenerator LLM)" },
      { label: "Retrieval Routes", value: "SQL, HYBRID" },
      { label: "Record Counts", value: "ASRS ~3K reports · OurAirports ~70K · Routes ~67K" },
    ],
  },

  // 2. KQL
  {
    id: "KQL",
    title: "Fabric Eventhouse (Kusto)",
    badgeVariant: "kql",
    storeTag: "Microsoft Fabric · Near-real-time streaming",
    whatIsThis:
      "Near-real-time event streaming data via Kusto Query Language. Contains live ADS-B flight positions from OpenSky Network, active SIGMET/AIRMET alerts, and G-AIRMET graphical weather forecasts. Optimized for temporal window queries where freshness is critical.",
    content:
      "Near-real-time event streaming data via Kusto Query Language. Contains live ADS-B flight positions from OpenSky Network, active SIGMET/AIRMET alerts, and G-AIRMET graphical weather forecasts. Optimized for temporal window queries.",
    schemaTables: [
      {
        name: "opensky_states",
        description: "Live ADS-B flight positions",
        columns: [
          { name: "icao24", description: "ICAO 24-bit transponder address" },
          { name: "callsign", description: "Flight callsign" },
          { name: "origin_country", description: "Country of registration" },
          { name: "longitude", description: "Position longitude" },
          { name: "latitude", description: "Position latitude" },
          { name: "baro_altitude", description: "Barometric altitude" },
          { name: "velocity", description: "Ground speed (m/s)" },
          { name: "on_ground", description: "Whether on ground" },
        ],
      },
      {
        name: "hazards_airsigmets",
        description: "Active SIGMET alerts (live feed)",
        columns: [
          { name: "raw_text", description: "Raw SIGMET text" },
          { name: "valid_time_from", description: "Start of validity" },
          { name: "valid_time_to", description: "End of validity" },
          { name: "hazard", description: "Hazard type" },
          { name: "severity", description: "Severity level" },
          { name: "airsigmet_type", description: "SIGMET or AIRMET" },
        ],
      },
      {
        name: "hazards_gairmets",
        description: "Active G-AIRMET forecasts (live feed)",
        columns: [
          { name: "receipt_time", description: "Time received" },
          { name: "issue_time", description: "Issuance time" },
          { name: "expire_time", description: "Expiration time" },
          { name: "hazard", description: "Hazard type" },
          { name: "geometry_type", description: "Geometry (area/point)" },
          { name: "due_to", description: "Cause of hazard" },
        ],
      },
    ],
    triggerKeywords: [
      "live", "real-time", "now", "current", "minutes", "weather", "METAR",
      "TAF", "SIGMET", "AIRMET", "flight tracking", "active hazards", "current status",
    ],
    triggerNote: "KQL vs SQL for hazards: KQL = live/current windows (freshness matters). SQL = historical records (analysis, trends, counts).",
    sampleQueries: [
      { query: "Are there any active SIGMETs along the IST-LHR route right now?", route: "KQL" },
      { query: "Show current ADS-B positions for Turkish Airlines flights near LTFM", route: "KQL" },
      { query: "What weather hazards are active in the last 60 minutes?", route: "HYBRID" },
    ],
    exampleRecord: {
      icao24: "4b1803",
      callsign: "THY6047",
      origin_country: "Turkey",
      longitude: 28.8144,
      latitude: 41.2753,
      baro_altitude: 10668,
      velocity: 245.3,
      on_ground: false,
      squawk: "4521",
    },
    meta: [
      { label: "Freshness", value: "Near-real-time · Script 10 (push to Kusto)" },
      { label: "Query Method", value: "KQL (Kusto Query Language) via Fabric endpoint" },
      { label: "Retrieval Routes", value: "SQL, HYBRID (freshness-triggered)" },
      { label: "SLA Trigger", value: "Activated when freshness_sla_minutes <= 60" },
    ],
  },

  // 3. VECTOR_OPS
  {
    id: "VECTOR_OPS",
    title: "Operational Narratives (AI Search)",
    badgeVariant: "vector",
    storeTag: "Azure AI Search · idx_ops_narratives · aisearchozguler",
    whatIsThis:
      "ASRS incident reports chunked into narrative segments with 1536-dim embeddings (text-embedding-3-small). Contains near-miss narratives, safety observations, pilot/controller experience descriptions, lessons learned, crew coordination issues, and ATC communication events. The primary source for 'find similar incidents' type queries.",
    content:
      "ASRS incident reports chunked into narrative segments with 1536-dim embeddings (text-embedding-3-small). Contains near-miss narratives, safety observations, pilot/controller experience descriptions, lessons learned, crew coordination issues, and ATC communication events.",
    schemaTables: [
      {
        name: "idx_ops_narratives",
        description: "AI Search index — operational narratives",
        columns: [
          { name: "id", description: "Key, Filterable" },
          { name: "content", description: "Searchable narrative text" },
          { name: "title", description: "Searchable, Filterable" },
          { name: "asrs_report_id", description: "Cross-ref to SQL (Filterable)" },
          { name: "event_date", description: "Filterable, Sortable" },
          { name: "aircraft_type", description: "Searchable, Filterable" },
          { name: "flight_phase", description: "Searchable, Filterable" },
          { name: "location", description: "Searchable, Filterable" },
          { name: "content_vector", description: "1536-dim HNSW embedding" },
        ],
      },
    ],
    triggerKeywords: [
      "summarize", "similar", "narrative", "what happened", "examples",
      "lessons", "safety", "incident", "near-miss",
    ],
    sampleQueries: [
      { query: "Find incidents similar to a bird strike during takeoff roll at JFK", route: "SEMANTIC" },
      { query: "Summarize lessons learned from crew coordination failures in ASRS reports", route: "SEMANTIC" },
      { query: "What are the most common ATC communication issues reported?", route: "HYBRID" },
    ],
    exampleRecord: {
      id: "asrs-100007-chunk-001",
      content: "During initial climb out of runway 25L, the crew observed a flock of birds at approximately 800 feet AGL. The captain initiated an immediate...",
      title: "Bird Strike During Initial Climb — B737-800",
      asrs_report_id: "100007",
      event_date: "2025-11-15",
      aircraft_type: "B737-800",
      flight_phase: "Initial Climb",
      location: "KJFK",
    },
    meta: [
      { label: "Freshness", value: "Batch · Scripts 07, 04 (prepare + upload)" },
      { label: "Query Method", value: "Vector + semantic hybrid search with reranking" },
      { label: "Retrieval Routes", value: "SEMANTIC, HYBRID" },
      { label: "Document Count", value: "~240K narrative chunks (3K source reports)" },
    ],
  },

  // 4. VECTOR_REG
  {
    id: "VECTOR_REG",
    title: "Regulatory Documents (AI Search)",
    badgeVariant: "vector",
    storeTag: "Azure AI Search · idx_regulatory · aisearchozguler",
    whatIsThis:
      "NOTAMs, Airworthiness Directives (ADs), EASA Safety Information Bulletins, FAA service bulletins, standard operating procedures (SOPs), and regulatory compliance documents. Embedded with 1536-dim vectors for semantic search. Use this for 'is there an AD affecting...' or 'what are the compliance requirements for...' questions.",
    content:
      "NOTAMs, Airworthiness Directives (ADs), EASA Safety Information Bulletins, FAA service bulletins, standard operating procedures (SOPs), and regulatory compliance documents. Embedded with 1536-dim vectors for semantic search.",
    schemaTables: [
      {
        name: "idx_regulatory",
        description: "AI Search index — regulatory documents",
        columns: [
          { name: "id", description: "Key, Filterable" },
          { name: "content", description: "Searchable document text" },
          { name: "title", description: "Searchable, Filterable" },
          { name: "document_number", description: "Filterable" },
          { name: "effective_date", description: "Filterable, Sortable" },
          { name: "issuing_authority", description: "Searchable, Filterable" },
          { name: "aircraft_type", description: "Searchable, Filterable" },
          { name: "document_type", description: "Searchable, Filterable" },
          { name: "content_vector", description: "1536-dim HNSW embedding" },
        ],
      },
    ],
    triggerKeywords: [
      "AD", "airworthiness", "NOTAM", "EASA", "compliance", "directive",
      "SOP", "regulatory", "bulletin",
    ],
    triggerNote: "VECTOR_REG vs NOSQL for NOTAMs: VECTOR_REG = semantic/similarity search across NOTAM content text. NOSQL = exact ICAO lookup with structured filters.",
    sampleQueries: [
      { query: "Are there any active ADs affecting B737-800 aircraft?", route: "SEMANTIC" },
      { query: "What EASA compliance requirements apply to A320 engine inspections?", route: "SEMANTIC" },
      { query: "Find regulatory documents related to lithium battery transport", route: "HYBRID" },
    ],
    exampleRecord: {
      id: "reg-easa-ad-2026-0042",
      content: "Emergency AD: Inspection of the horizontal stabilizer trim actuator on A320 family aircraft with MSN 6500-7200...",
      title: "EASA AD 2026-0042 — A320 Stabilizer Trim Actuator Inspection",
      document_number: "2026-0042",
      effective_date: "2026-02-15",
      issuing_authority: "EASA",
      aircraft_type: "A320",
      document_type: "Airworthiness Directive",
    },
    meta: [
      { label: "Freshness", value: "Batch · Scripts 07, 04" },
      { label: "Query Method", value: "Vector + semantic hybrid search with reranking" },
      { label: "Retrieval Routes", value: "SEMANTIC, HYBRID" },
      { label: "Document Count", value: "55+ regulatory documents" },
    ],
  },

  // 5. VECTOR_AIRPORT
  {
    id: "VECTOR_AIRPORT",
    title: "Airport Operational Documents (AI Search)",
    badgeVariant: "vector",
    storeTag: "Azure AI Search · idx_airport_ops_docs · aisearchozguler",
    whatIsThis:
      "Runway specification documents, station manuals, ground handling procedures, terminal facility descriptions, taxiway diagrams, gate/stand allocation rules, and turnaround SOPs. Each document is tagged with airport ICAO/IATA codes. Use for 'what are the runway specs at...' or 'ground handling procedures for...' questions.",
    content:
      "Runway specification documents, station manuals, ground handling procedures, terminal facility descriptions, taxiway diagrams, gate/stand allocation rules, and turnaround SOPs. Each document is tagged with airport ICAO/IATA codes.",
    schemaTables: [
      {
        name: "idx_airport_ops_docs",
        description: "AI Search index — airport operations docs",
        columns: [
          { name: "id", description: "Key, Filterable" },
          { name: "content", description: "Searchable document text" },
          { name: "title", description: "Searchable, Filterable" },
          { name: "airport_icao", description: "Filterable (e.g. LTFM)" },
          { name: "airport_iata", description: "Filterable (e.g. IST)" },
          { name: "airport_name", description: "Searchable, Filterable" },
          { name: "facility_type", description: "Searchable, Filterable" },
          { name: "content_vector", description: "1536-dim HNSW embedding" },
        ],
      },
    ],
    triggerKeywords: [
      "runway", "gate", "turnaround", "airport", "station",
      "LTFM", "LTFJ", "LTBA", "ground handling", "apron",
    ],
    sampleQueries: [
      { query: "What are the runway specifications at Istanbul Airport (LTFM)?", route: "SEMANTIC" },
      { query: "Ground handling procedures for wide-body aircraft at Sabiha Gokcen", route: "SEMANTIC" },
      { query: "Gate allocation rules for Terminal 1 at LTFM", route: "SEMANTIC" },
    ],
    exampleRecord: {
      id: "airport-ltfm-rwy-35l",
      content: "Runway 35L/17R at Istanbul Airport (LTFM): Length 3,750m, Width 60m, Surface Asphalt, PCN 82/R/B/W/T. CAT IIIB ILS available...",
      title: "LTFM Runway 35L/17R Specifications",
      airport_icao: "LTFM",
      airport_iata: "IST",
      airport_name: "Istanbul Airport",
      facility_type: "runway",
    },
    meta: [
      { label: "Freshness", value: "Batch · Scripts 07, 04" },
      { label: "Query Method", value: "Vector + semantic hybrid search with reranking" },
      { label: "Retrieval Routes", value: "SEMANTIC, HYBRID" },
      { label: "Document Count", value: "~2,000 airport operations documents" },
    ],
  },

  // 6. NOSQL
  {
    id: "NOSQL",
    title: "Cosmos DB — Operational NOTAMs",
    badgeVariant: "nosql",
    storeTag: "Azure Cosmos DB (NoSQL) · cosmos-aviation-rag · aviationrag / notams",
    whatIsThis:
      "Structured NOTAM documents partitioned by ICAO code for fast point reads. Each document includes severity classification (HIGH/MEDIUM/LOW), category (runway, taxiway, navaid, obstacle, procedure, airspace, apron, fuel, wildlife, security, aerodrome), and effective date range with active/expired status. Best for 'show me active NOTAMs at JFK' style queries.",
    content:
      "Structured NOTAM documents partitioned by ICAO code for fast point reads. Each document includes severity classification (HIGH/MEDIUM/LOW), category, and effective date range with active/expired status.",
    schemaTables: [
      {
        name: "notams (container)",
        description: "Cosmos DB container — NOTAM documents (partition key: /icao)",
        columns: [
          { name: "id", description: "Unique NOTAM identifier" },
          { name: "notam_number", description: "Official NOTAM number (e.g. A0001/26)" },
          { name: "icao", description: "Airport ICAO code (partition key)" },
          { name: "iata", description: "Airport IATA code" },
          { name: "airport_name", description: "Airport name" },
          { name: "category", description: "runway | taxiway | navaid | obstacle | procedure | airspace | ..." },
          { name: "severity", description: "HIGH | MEDIUM | LOW" },
          { name: "content", description: "NOTAM text content" },
          { name: "status", description: "active | expired" },
          { name: "effective_from", description: "Start of validity (ISO 8601)" },
          { name: "effective_to", description: "End of validity (ISO 8601)" },
          { name: "source", description: "FAA | DGCA | CAA" },
        ],
      },
    ],
    triggerKeywords: [
      "NOTAM", "operational doc", "ops doc", "ground handling doc", "parking stand",
    ],
    sampleQueries: [
      { query: "Show active NOTAMs at JFK airport", route: "HYBRID" },
      { query: "Are there any high-severity NOTAMs affecting runway operations at LTFM?", route: "HYBRID" },
      { query: "List all NOTAMs for EGLL with runway category", route: "HYBRID" },
    ],
    exampleRecord: {
      id: "NOTAM-A0001-26-KJFK",
      notam_number: "A0001/26",
      icao: "KJFK",
      iata: "JFK",
      airport_name: "John F Kennedy Intl",
      severity: "HIGH",
      content: "RWY 13R/31L CLSD FOR MAINT 0700-1500 DAILY",
      status: "active",
      category: "runway",
      source: "FAA",
    },
    meta: [
      { label: "Freshness", value: "Batch seed · Script 13 (seed), 15 (bulk load)" },
      { label: "Query Method", value: "Point reads by ICAO partition key + cross-partition queries" },
      { label: "Retrieval Routes", value: "HYBRID (triggered by NOTAM/ops doc keywords)" },
      { label: "Auth", value: "AAD-only (disableLocalAuth=true) · AKS kubelet identity RBAC" },
    ],
  },

  // 7. GRAPH
  {
    id: "GRAPH",
    title: "Knowledge Graph (Fabric Kusto / PG Fallback)",
    badgeVariant: "graph",
    storeTag: "Fabric Eventhouse or PostgreSQL · demo.ops_graph_edges",
    whatIsThis:
      "Knowledge graph with 500K+ edges across 16 edge types connecting 11 node types. Models relationships between airports, runways, flight legs, aircraft tails, crew, NOTAMs, routes, airlines, navaids, frequencies, and ASRS reports. Supports multi-hop BFS traversal for impact and dependency analysis — perfect for 'what happens if runway X closes' scenarios.",
    content:
      "Knowledge graph with 500K+ edges across 16 edge types connecting 11 node types. Models relationships between airports, runways, flight legs, aircraft tails, crew, NOTAMs, routes, airlines, navaids, frequencies, and ASRS reports. Supports multi-hop BFS traversal for impact and dependency analysis.",
    schemaTables: [
      {
        name: "ops_graph_edges",
        description: "Graph edge table (500K+ unique edges)",
        columns: [
          { name: "src_type", description: "Source node type (e.g. Airport, Tail, FlightLeg)" },
          { name: "src_id", description: "Source node identifier" },
          { name: "edge_type", description: "Relationship type (16 types)" },
          { name: "dst_type", description: "Destination node type" },
          { name: "dst_id", description: "Destination node identifier" },
        ],
      },
    ],
    triggerKeywords: [
      "impact", "dependency", "depends on", "connected", "alternate",
      "route network", "relationship", "cascade", "propagate", "knock-on", "downstream",
    ],
    sampleQueries: [
      { query: "What flights are impacted if runway 35L at LTFM closes?", route: "HYBRID" },
      { query: "Show the dependency chain for flight TK1780 — crew, aircraft, gate", route: "HYBRID" },
      { query: "Which alternate airports are connected to IST in the route network?", route: "HYBRID" },
    ],
    exampleRecord: {
      src_type: "FlightLeg",
      src_id: "LEG-TK1780-20260220",
      edge_type: "DEPARTS",
      dst_type: "Airport",
      dst_id: "LTFM",
    },
    meta: [
      { label: "Freshness", value: "Batch · Script 14 (build from PG tables, CSV for KQL sync)" },
      { label: "Query Method", value: "Multi-hop BFS traversal (configurable hops, default 2)" },
      { label: "Retrieval Routes", value: "HYBRID (triggered by relationship/impact keywords)" },
      { label: "Scale", value: "500K+ unique edges · 11 node types · 16 edge types" },
    ],
  },

  // 8. FABRIC_SQL
  {
    id: "FABRIC_SQL",
    title: "Fabric SQL Warehouse — BTS On-Time Performance",
    badgeVariant: "fabric",
    storeTag: "Microsoft Fabric SQL Warehouse · PostAssignWarehouse1",
    whatIsThis:
      "Bureau of Transportation Statistics (BTS) airline performance data. Contains flight-level on-time reporting with 38 columns and aggregate delay statistics by carrier. Supports delay root-cause analysis across 5 categories: carrier, weather, NAS, security, and late aircraft. Use for 'which airline has the worst on-time performance' or 'what causes the most delays at LAX'.",
    content:
      "Bureau of Transportation Statistics (BTS) airline performance data. Contains flight-level on-time reporting with 38 columns and aggregate delay statistics by carrier. Supports delay root-cause analysis across 5 categories: carrier, weather, NAS, security, and late aircraft.",
    schemaTables: [
      {
        name: "bts_ontime_reporting",
        description: "Flight-level on-time performance (38 columns)",
        columns: [
          { name: "FlightDate", description: "Date of the flight" },
          { name: "IATA_Code_Marketing_Airline", description: "Marketing carrier IATA code" },
          { name: "Origin", description: "Origin airport" },
          { name: "Dest", description: "Destination airport" },
          { name: "DepDelay", description: "Departure delay (minutes)" },
          { name: "ArrDelay", description: "Arrival delay (minutes)" },
          { name: "Cancelled", description: "Whether cancelled (0/1)" },
          { name: "CancellationCode", description: "A=Carrier, B=Weather, C=NAS, D=Security" },
          { name: "CarrierDelay", description: "Carrier-caused delay (minutes)" },
          { name: "WeatherDelay", description: "Weather-caused delay (minutes)" },
          { name: "Distance", description: "Flight distance (miles)" },
        ],
      },
      {
        name: "airline_delay_causes",
        description: "Aggregate delay statistics by carrier (21 columns)",
        columns: [
          { name: "carrier_name", description: "Airline name" },
          { name: "airport", description: "Airport code" },
          { name: "arr_flights", description: "Total arriving flights" },
          { name: "arr_del15", description: "Flights delayed 15+ min" },
          { name: "carrier_ct", description: "Carrier delay count" },
          { name: "weather_ct", description: "Weather delay count" },
          { name: "nas_ct", description: "NAS delay count" },
          { name: "late_aircraft_ct", description: "Late aircraft delay count" },
        ],
      },
    ],
    triggerKeywords: [
      "delay", "on-time", "cancellation", "BTS", "carrier performance",
      "weather delay", "NAS delay", "delay cause", "average delay",
      "cancellation rate", "on time performance", "schedule performance",
    ],
    sampleQueries: [
      { query: "What is the average departure delay for Delta flights at ATL?", route: "SQL" },
      { query: "Compare cancellation rates: weather vs carrier causes in January 2026", route: "SQL" },
      { query: "Top 10 airports by NAS delay minutes this quarter", route: "HYBRID" },
    ],
    exampleRecord: {
      FlightDate: "2026-01-15",
      IATA_Code_Marketing_Airline: "DL",
      Origin: "ATL",
      Dest: "LAX",
      DepDelay: 23,
      ArrDelay: 18,
      Cancelled: 0,
      CarrierDelay: 15,
      WeatherDelay: 0,
      NASDelay: 8,
      Distance: 1946,
    },
    meta: [
      { label: "Freshness", value: "Batch · Script 16 (load via COPY INTO / batch INSERT)" },
      { label: "Query Method", value: "T-SQL via ODBC (AAD token auth)" },
      { label: "Retrieval Routes", value: "SQL, HYBRID (triggered by delay/BTS keywords)" },
      { label: "Auth", value: "DefaultAzureCredential · database.windows.net scope" },
    ],
  },
];

// ── Routing Rules ───────────────────────────────────────────────────────────

export interface RoutingRule {
  number: number;
  title: string;
  description: string;
}

export const ROUTING_RULES: RoutingRule[] = [
  {
    number: 1,
    title: "Pair Structured + Semantic",
    description:
      "When a query mixes metrics with context (\"top incidents and why they happened\"), always include at least one structured source (SQL, KQL, FABRIC_SQL) AND one semantic source (VECTOR_OPS, VECTOR_REG, VECTOR_AIRPORT).",
  },
  {
    number: 2,
    title: "KQL for Freshness-Critical Data",
    description:
      "Include KQL when the query involves: \"now\", \"current\", \"live\", \"real-time\", \"next N minutes/hours\", \"weather\", \"METAR\", \"SIGMET\", \"active hazards\", \"flight position\". Do NOT use KQL for historical analysis.",
  },
  {
    number: 3,
    title: "GRAPH for Relationships & Impact",
    description:
      "Include GRAPH for: \"impact\", \"dependency\", \"downstream\", \"cascade\", \"propagate\", \"knock-on\", \"what happens if\", \"connected\", \"alternate\", \"network\". Especially valuable for \"if-then\" disruption scenarios.",
  },
  {
    number: 4,
    title: "FABRIC_SQL for Delay Analytics",
    description:
      "Include FABRIC_SQL for: \"delay\", \"on-time\", \"BTS\", \"carrier delay\", \"cancellation rate\", \"punctuality\", \"schedule performance\", \"weather delay vs carrier delay\". Not activated when ops table signals (MEL, turnaround, crew) are present.",
  },
  {
    number: 5,
    title: "VECTOR_REG + NOSQL for Regulatory",
    description:
      "VECTOR_REG for semantic search over regulatory documents. NOSQL for exact NOTAM lookups by ICAO code. Use both together for comprehensive regulatory coverage.",
  },
  {
    number: 6,
    title: "Profile-Driven Enrichments",
    description:
      "pilot-brief / ops-live / operations: always includes SQL + VECTOR_OPS. compliance / regulatory: always includes VECTOR_REG + SQL. Profiles supplement but don't override keyword-triggered source activation.",
  },
  {
    number: 7,
    title: "VECTOR_AIRPORT for Facility Reference",
    description:
      "Include VECTOR_AIRPORT for: \"runway\", \"gate\", \"stand\", \"apron\", \"taxiway\", \"terminal\", \"ground handling\", \"turnaround\", \"facility\". Airport-specific operational docs not captured in structured data.",
  },
  {
    number: 8,
    title: "Omit Sources That Add No Value",
    description:
      "A delay statistics query does not need VECTOR_AIRPORT. A runway specs query does not need FABRIC_SQL. Only include sources that contribute evidence to the answer.",
  },
];

// ── Pipeline Steps ──────────────────────────────────────────────────────────

export interface PipelineStep {
  scriptNum: string;
  description: string;
  targetStore: string;
}

export const PIPELINE_STEPS: PipelineStep[] = [
  { scriptNum: "00", description: "Fetch ASRS exports from FAA (date-range parameterized)", targetStore: "data/asrs/raw/" },
  { scriptNum: "01", description: "Extract and normalize raw ASRS data to JSONL", targetStore: "data/processed/" },
  { scriptNum: "02", description: "Load structured records into PostgreSQL", targetStore: "SQL (PostgreSQL)" },
  { scriptNum: "03", description: "Create/update AI Search indexes (ops, regulatory, airport)", targetStore: "VECTOR_OPS, VECTOR_REG, VECTOR_AIRPORT" },
  { scriptNum: "04", description: "Upload embedded documents to AI Search indexes", targetStore: "VECTOR_OPS, VECTOR_REG, VECTOR_AIRPORT" },
  { scriptNum: "05", description: "Fetch synthetic operational overlay data (flight legs, crew, MEL, baggage)", targetStore: "data/j-synthetic_ops_overlay/" },
  { scriptNum: "06", description: "Fetch BTS airline schedule feed (on-time + delay causes)", targetStore: "data/k-airline_schedule_feed/" },
  { scriptNum: "07", description: "Prepare multi-index documents (chunk + embed for 3 AI Search indexes)", targetStore: "data/search_docs/" },
  { scriptNum: "08", description: "Push to Azure Table Storage (legacy/alternate)", targetStore: "Azure Table Storage" },
  { scriptNum: "09", description: "Bulk load multi-source datasets into PostgreSQL (20+ tables)", targetStore: "SQL (PostgreSQL)" },
  { scriptNum: "10", description: "Push event data to Fabric Eventhouse (Kusto)", targetStore: "KQL (Fabric Eventhouse)" },
  { scriptNum: "12", description: "Fetch airport, runway, and network reference data", targetStore: "data/g-ourairports/ data/f-openflights/" },
  { scriptNum: "13", description: "Seed Cosmos DB with sample NOTAM documents (25 NOTAMs)", targetStore: "NOSQL (Cosmos DB)" },
  { scriptNum: "14", description: "Build enriched graph edges (500K+) from PG tables + NOTAM parsing", targetStore: "GRAPH (PG + CSV for KQL sync)" },
  { scriptNum: "15", description: "Bulk load real NOTAMs from PilotWeb to Cosmos DB", targetStore: "NOSQL (Cosmos DB)" },
  { scriptNum: "16", description: "Load BTS on-time performance into Fabric SQL Warehouse", targetStore: "FABRIC_SQL (Fabric SQL Warehouse)" },
];

// ── Source Relationships ────────────────────────────────────────────────────

export interface SourceRelationship {
  srcId: string;
  srcColor: string;
  dstId: string;
  dstColor: string;
  sharedKey: string;
  description: string;
}

export const SOURCE_RELATIONSHIPS: SourceRelationship[] = [
  { srcId: "SQL", srcColor: "text-blue-400", dstId: "VECTOR_OPS", dstColor: "text-purple-400", sharedKey: "asrs_report_id", description: "ASRS report <-> narrative chunks" },
  { srcId: "NOSQL", srcColor: "text-orange-400", dstId: "VECTOR_AIRPORT", dstColor: "text-purple-400", sharedKey: "icao / iata", description: "NOTAMs <-> airport ops docs" },
  { srcId: "SQL", srcColor: "text-blue-400", dstId: "NOSQL", dstColor: "text-orange-400", sharedKey: "icao / iata", description: "Airport tables <-> NOTAM store" },
  { srcId: "SQL", srcColor: "text-blue-400", dstId: "SQL", dstColor: "text-blue-400", sharedKey: "leg_id", description: "ops_flight_legs <-> crew/MEL/baggage/turnaround" },
  { srcId: "SQL", srcColor: "text-blue-400", dstId: "FABRIC_SQL", dstColor: "text-amber-400", sharedKey: "origin_iata / dest_iata", description: "Ops legs <-> BTS on-time (same airport codes)" },
  { srcId: "GRAPH", srcColor: "text-green-400", dstId: "ALL SOURCES", dstColor: "text-muted-foreground", sharedKey: "all entity IDs", description: "Graph materializes cross-source relationships" },
  { srcId: "KQL", srcColor: "text-teal-400", dstId: "SQL", dstColor: "text-blue-400", sharedKey: "icao24 / callsign", description: "Live ADS-B <-> ops_flight_legs (tail correlation)" },
  { srcId: "VECTOR_REG", srcColor: "text-purple-400", dstId: "SQL", dstColor: "text-blue-400", sharedKey: "aircraft_type", description: "ADs/SBs <-> fleet data (type applicability)" },
];

// ── Retrieval Mode Comparison ───────────────────────────────────────────────

export interface RetrievalModeRow {
  dimension: string;
  codeRag: string;
  foundryIq: string;
}

export const RETRIEVAL_MODE_COMPARISON: RetrievalModeRow[] = [
  { dimension: "Philosophy", codeRag: "You control what runs. Full source-level transparency.", foundryIq: "The platform decides. Fabric handles grounding and ranking." },
  { dimension: "Query Routing", codeRag: "Agentic LLM router (SQL / SEMANTIC / HYBRID)", foundryIq: "Fabric Data Agent (automatic grounding)" },
  { dimension: "Data Sources", codeRag: "8 independent stores: SQL, KQL, VECTOR_OPS, VECTOR_REG, VECTOR_AIRPORT, NOSQL, GRAPH, FABRIC_SQL", foundryIq: "4 managed stores: VECTOR_OPS, VECTOR_AIRPORT, VECTOR_REG, Fabric Data Agent (Lakehouse + OneLake)" },
  { dimension: "Retrieval Style", codeRag: "Deterministic SQL + vector search + KQL, queried in parallel", foundryIq: "Semantic ranking with managed retrieval" },
  { dimension: "Transparency", codeRag: "Full tool-call trace with per-source citations", foundryIq: "Platform-level grounding signals" },
  { dimension: "Best For", codeRag: "Precise SQL queries, KPI audits, cross-source correlation, NOTAM lookups", foundryIq: "Exploratory analysis, semantic search, Lakehouse-native datasets" },
  { dimension: "Trade-off", codeRag: "Requires pipeline tuning; each data source is independently managed", foundryIq: "Less control over retrieval path; dependent on Fabric Data Agent availability" },
  { dimension: "LLM", codeRag: "gpt-5-nano (routing + synthesis)", foundryIq: "gpt-5-mini (Responses API)" },
];

// ── Summary Stats ───────────────────────────────────────────────────────────

export interface SummaryStat {
  value: string;
  label: string;
}

export const SUMMARY_STATS: SummaryStat[] = [
  { value: "8", label: "Data Sources" },
  { value: "5", label: "Storage Technologies" },
  { value: "3", label: "Retrieval Routes" },
  { value: "20+", label: "SQL Tables" },
  { value: "500K+", label: "Graph Edges" },
];
