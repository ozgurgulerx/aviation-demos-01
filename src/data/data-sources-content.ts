export interface DataInventoryItem {
  id: string;
  source: string;
  dataType: string;
  airlinePurpose: string;
  primaryDatastores: string[];
  repoArtifacts: string[];
}

export interface DataShapeDetail {
  id: string;
  title: string;
  format: string;
  keyFields: string[];
  exampleRecord: string;
  retrievalUse: string[];
  repoArtifacts: string[];
}

export interface RetrievalMappingItem {
  contextFamily: string;
  datastore: string;
  retrievalMode: string;
  rationale: string;
}

export interface RetrievalFlowStep {
  step: string;
  description: string;
}

export interface GlossaryItem {
  acronym: string;
  longForm: string;
  whyItMatters: string;
}

export const DATA_INVENTORY: DataInventoryItem[] = [
  {
    id: "aviation-weather",
    source: "AviationWeather",
    dataType: "METAR / TAF / SIGMET / AIRMET / PIREP",
    airlinePurpose: "Pre-departure risk awareness and weather-driven safety context.",
    primaryDatastores: ["Fabric Eventhouse (KQL)", "Fabric Lakehouse"],
    repoArtifacts: [
      "data/a-metars.cache.csv.gz",
      "data/a-metars.cache.xml.gz",
      "data/a-tafs.cache.xml.gz",
      "data/a-stations.cache.json.gz",
    ],
  },
  {
    id: "opensky",
    source: "OpenSky",
    dataType: "State vectors, tracks, flight-level telemetry",
    airlinePurpose: "Live traffic pressure and near-term operational context.",
    primaryDatastores: ["Fabric Eventhouse (KQL)", "Fabric Lakehouse"],
    repoArtifacts: [
      "Runtime API feed (live retrieval in ingestion/runtime layer)",
    ],
  },
  {
    id: "airport-network",
    source: "OurAirports + OpenFlights",
    dataType: "Airports, runways, navaids, frequencies, network routes",
    airlinePurpose: "Runway/topology suitability, alternates, network adjacency.",
    primaryDatastores: ["Fabric Warehouse", "Fabric Graph (Preview)"],
    repoArtifacts: [
      "data/b-airports.csv",
      "data/b-runways.csv",
      "data/b-navaids.csv",
      "data/b-airport-frequencies.csv",
    ],
  },
  {
    id: "asrs",
    source: "NASA ASRS",
    dataType: "Safety narratives and normalized metadata",
    airlinePurpose: "Case-based reasoning and analogous incident retrieval.",
    primaryDatastores: ["Azure AI Search (idx_ops_narratives)", "PostgreSQL/SQL stores"],
    repoArtifacts: [
      "data/c1-asrs/processed/asrs_records.jsonl",
      "data/c1-asrs/processed/asrs_documents.jsonl",
      "data/c1-asrs/aviation.db",
    ],
  },
  {
    id: "ntsb",
    source: "NTSB Accident Archive",
    dataType: "Incident/accident archives (MDB and compressed archives)",
    airlinePurpose: "Historical analogs and risk lessons for narrative grounding.",
    primaryDatastores: ["Fabric Lakehouse", "Azure AI Search (post-extraction)"],
    repoArtifacts: [
      "data/c2-avall.mdb",
      "data/c2-avall.zip",
      "data/c2-PRE1982.zip",
      "data/c2-ntsb_pre1982/PRE1982.MDB",
    ],
  },
  {
    id: "easa-notam",
    source: "EASA AD + NOTAM corpus",
    dataType: "Regulatory metadata, PDF corpus, NOTAM records",
    airlinePurpose: "Dispatch restrictions and compliance-aware decision support.",
    primaryDatastores: ["Azure AI Search (idx_regulatory)", "Fabric Lakehouse"],
    repoArtifacts: [
      "data/d-easa_ads_recent/downloaded_ads_with_metadata.csv",
      "data/d-easa_ads_recent/recent_ads_with_pdf.csv",
      "data/d-easa_ads_recent/pdfs/*.pdf",
    ],
  },
  {
    id: "synthetic-ops",
    source: "Synthetic Ops Overlay",
    dataType: "Crew, gate, turnaround, MEL, dependency edges",
    airlinePurpose: "Operational realism for multi-agent dispatchability decisions.",
    primaryDatastores: ["Fabric Warehouse", "Fabric Eventhouse (KQL)", "Fabric Graph (Preview)"],
    repoArtifacts: [
      "Generated in demo pipelines and loaded to relational/graph stores",
    ],
  },
];

export const DATA_SHAPE_DETAILS: DataShapeDetail[] = [
  {
    id: "shape-aviation-weather",
    title: "AviationWeather (METAR/TAF/SIGMET/AIRMET/PIREP)",
    format: "CSV.gz and XML.gz near-real-time snapshots",
    keyFields: [
      "raw_text",
      "station_id",
      "observation_time",
      "latitude / longitude",
      "wind_dir_degrees",
      "wind_speed_kt",
      "visibility_statute_mi",
      "hazard / severity",
      "valid_time_from / valid_time_to",
    ],
    exampleRecord:
      "station_id=UNBB, observation_time=2026-02-18T15:37:00Z, wind_dir_degrees=230, wind_speed_kt=19, hazard=CONVECTIVE, type=SIGMET",
    retrievalUse: [
      "Sub-minute weather hazard windows in KQL/Eventhouse.",
      "Freshness checks before final brief composition.",
    ],
    repoArtifacts: [
      "data/a-metars.cache.csv.gz",
      "data/a-tafs.cache.xml.gz",
    ],
  },
  {
    id: "shape-opensky",
    title: "OpenSky Live States and Flights",
    format: "REST JSON objects/arrays",
    keyFields: [
      "time",
      "states[]",
      "icao24",
      "callsign",
      "last_contact",
      "lon / lat",
      "baro_altitude",
      "velocity",
      "estDepartureAirport / estArrivalAirport",
    ],
    exampleRecord:
      "callsign=THY6047, estDepartureAirport=LTFM, path[0]=[1771517590,41.9796,22.2923,5486,90,false]",
    retrievalUse: [
      "Live traffic pressure checks near operational windows.",
      "Context enrichment for departure risk and congestion.",
    ],
    repoArtifacts: [
      "Runtime ingestion (live API pull)",
    ],
  },
  {
    id: "shape-airport-network",
    title: "OurAirports + OpenFlights Network",
    format: "CSV and CSV-like DAT rows",
    keyFields: [
      "ident",
      "type",
      "name",
      "gps_code",
      "runway length_ft",
      "surface",
      "airline route source/destination",
      "stops",
      "equipment",
    ],
    exampleRecord:
      "airport_ident=00A, length_ft=80, surface=ASPH-G; route: airline=2B, src=AER, dst=KZN, stops=0",
    retrievalUse: [
      "Deterministic airport/runway compatibility checks.",
      "Graph expansion for route alternatives and dependency paths.",
    ],
    repoArtifacts: [
      "data/b-airports.csv",
      "data/b-runways.csv",
      "data/b-navaids.csv",
      "data/b-airport-frequencies.csv",
    ],
  },
  {
    id: "shape-asrs",
    title: "ASRS Narratives",
    format: "Processed JSONL + relational records",
    keyFields: [
      "asrs_report_id",
      "event_date",
      "location",
      "aircraft_type",
      "flight_phase",
      "title",
      "report_text",
      "raw_json",
    ],
    exampleRecord:
      "asrs_report_id=100007, event_date=1988-12-01, location=LIZ, CA, flight_phase=Initial Climb",
    retrievalUse: [
      "Vector chunking and semantic retrieval for similar-case narratives.",
      "Citation-backed evidence in brief outputs.",
    ],
    repoArtifacts: [
      "data/c1-asrs/processed/asrs_records.jsonl",
      "data/c1-asrs/processed/asrs_documents.jsonl",
    ],
  },
  {
    id: "shape-ntsb",
    title: "NTSB Archive (Raw MDB Staging)",
    format: "ZIP + Microsoft Access MDB archives",
    keyFields: [
      "event_id (parse target)",
      "event_date (parse target)",
      "location (parse target)",
      "injury/fatality indicators (parse target)",
      "probable cause narrative (parse target)",
    ],
    exampleRecord:
      "raw_file=c2-avall.mdb, parse_target.event_date=YYYY-MM-DD, parse_target.narrative=free text probable cause",
    retrievalUse: [
      "Historical analog retrieval after normalization and chunking.",
      "Long-horizon safety evidence for high-stakes responses.",
    ],
    repoArtifacts: [
      "data/c2-avall.mdb",
      "data/c2-avall.zip",
      "data/c2-PRE1982.zip",
    ],
  },
  {
    id: "shape-easa-notam",
    title: "EASA AD + NOTAM Corpus",
    format: "CSV metadata, PDF corpus, JSONL NOTAM feed",
    keyFields: [
      "class_number",
      "issue_date",
      "effective_date",
      "subject",
      "pdf_url",
      "facilityDesignator",
      "notamNumber",
      "icaoMessage",
      "start/end validity",
    ],
    exampleRecord:
      "class_number=CF-2026-08, effective_date=2026-02-19, facilityDesignator=LTBA, notamNumber=G1555/12",
    retrievalUse: [
      "Compliance and dispatch constraints from unstructured docs.",
      "Hybrid semantic + lexical retrieval with metadata filters.",
    ],
    repoArtifacts: [
      "data/d-easa_ads_recent/downloaded_ads_with_metadata.csv",
      "data/d-easa_ads_recent/pdfs/*.pdf",
    ],
  },
  {
    id: "shape-synthetic-ops",
    title: "Synthetic Operational Overlay",
    format: "Operational CSV tables and schedule extracts",
    keyFields: [
      "ops_flight_legs",
      "ops_turnaround_milestones",
      "ops_crew_rosters",
      "ops_graph_edges",
      "schedule_delay_causes",
      "legality_risk_flag",
      "delay_cause_code",
    ],
    exampleRecord:
      "flight_no=PGT161K, milestone=GATE_OPEN, status=done, delay_cause_code=NONE, duty_id=DUTY-CAP002-0001",
    retrievalUse: [
      "Deterministic operational scoring and legality checks.",
      "Graph + SQL joins for disruption propagation analysis.",
    ],
    repoArtifacts: [
      "Generated demo operational datasets (warehouse/event/graph layers)",
    ],
  },
];

export const CONTEXT_TO_DATASTORE_MAPPING: RetrievalMappingItem[] = [
  {
    contextFamily: "Live weather + hazards",
    datastore: "Fabric Eventhouse (KQL)",
    retrievalMode: "Sub-minute event windows and anomaly scans",
    rationale: "Rolling brief updates require event-native, freshness-aware query patterns.",
  },
  {
    contextFamily: "Ops relational state (crew, gate, MEL)",
    datastore: "Fabric Warehouse",
    retrievalMode: "Deterministic SQL joins with constraints",
    rationale: "Dispatch decisions rely on auditable KPI math and explicit thresholds.",
  },
  {
    contextFamily: "Raw evidence and replay",
    datastore: "Fabric Lakehouse",
    retrievalMode: "Immutable landing zone + batch curation",
    rationale: "Traceability, reprocessing, and replay are required for governance and drift control.",
  },
  {
    contextFamily: "Narratives and regulations",
    datastore: "Azure AI Search (vector + hybrid)",
    retrievalMode: "Semantic + lexical ranking with metadata filtering",
    rationale: "Safety and compliance questions depend on unstructured document grounding.",
  },
  {
    contextFamily: "Dependency and propagation analysis",
    datastore: "Fabric Graph (Preview)",
    retrievalMode: "Multi-hop traversal",
    rationale: "Complex disruptions require relationship reasoning beyond flat joins.",
  },
];

export const RETRIEVAL_FLOW_STEPS: RetrievalFlowStep[] = [
  {
    step: "Planner agent decomposition",
    description: "The query is split into KQL, SQL, Graph, and vector sub-tasks based on intent.",
  },
  {
    step: "Parallel retriever execution",
    description: "Retriever agents execute against Eventhouse, Warehouse, Graph, and Azure AI Search.",
  },
  {
    step: "Verifier pass",
    description: "Freshness, conflict detection, and policy/regulatory consistency are validated.",
  },
  {
    step: "Citation-aware composition",
    description: "Answer synthesis includes source trace, confidence tier, and unresolved-risk notes.",
  },
];

export const GLOSSARY_ITEMS: GlossaryItem[] = [
  {
    acronym: "RAG",
    longForm: "Retrieval-Augmented Generation",
    whyItMatters: "Grounds generated answers with explicit evidence and traceability.",
  },
  {
    acronym: "KQL",
    longForm: "Kusto Query Language",
    whyItMatters: "Supports low-latency event-window analytics in Eventhouse.",
  },
  {
    acronym: "SQL",
    longForm: "Structured Query Language",
    whyItMatters: "Provides deterministic relational retrieval and KPI calculations.",
  },
  {
    acronym: "NOTAM",
    longForm: "Notice to Air Missions",
    whyItMatters: "Publishes operational notices that impact dispatch and briefing decisions.",
  },
  {
    acronym: "METAR",
    longForm: "Meteorological Aerodrome Report",
    whyItMatters: "Captures current airport weather observations used in risk checks.",
  },
  {
    acronym: "TAF",
    longForm: "Terminal Aerodrome Forecast",
    whyItMatters: "Provides short-term forecast horizon for departure planning.",
  },
  {
    acronym: "SIGMET",
    longForm: "Significant Meteorological Information",
    whyItMatters: "Flags severe weather hazards relevant to route safety.",
  },
  {
    acronym: "AIRMET",
    longForm: "Airmen's Meteorological Information",
    whyItMatters: "Captures lower-severity but operationally relevant weather advisories.",
  },
  {
    acronym: "PIREP",
    longForm: "Pilot Report",
    whyItMatters: "Adds pilot-observed hazard signals to complement meteorological feeds.",
  },
  {
    acronym: "ASRS",
    longForm: "Aviation Safety Reporting System",
    whyItMatters: "Supplies narrative safety cases for analogy-based reasoning.",
  },
  {
    acronym: "NTSB",
    longForm: "National Transportation Safety Board",
    whyItMatters: "Provides historical incident patterns and probable-cause evidence.",
  },
  {
    acronym: "AD",
    longForm: "Airworthiness Directive",
    whyItMatters: "Defines mandatory ongoing airworthiness compliance requirements.",
  },
  {
    acronym: "MEL",
    longForm: "Minimum Equipment List",
    whyItMatters: "Constrains dispatchability when aircraft systems are inoperative.",
  },
];
