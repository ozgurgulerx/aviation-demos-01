export interface DatastoreVisual {
  id: string;
  shortLabel: string;
  longLabel: string;
  description: string;
  iconSrc: string;
  isFabric: boolean;
}

const VISUALS: Record<string, DatastoreVisual> = {
  KQL: {
    id: "KQL",
    shortLabel: "Fabric Eventhouse",
    longLabel: "Fabric Eventhouse (KQL)",
    description: "Weather hazards, AIRMETs & telemetry",
    iconSrc: "/service-icons/fabric.png",
    isFabric: true,
  },
  SQL: {
    id: "SQL",
    shortLabel: "Azure PostgreSQL",
    longLabel: "Azure PostgreSQL (SQL)",
    description: "ASRS reports, airports, runways & flight ops",
    iconSrc: "/service-icons/postgresql.jpeg",
    isFabric: false,
  },
  GRAPH: {
    id: "GRAPH",
    shortLabel: "Fabric Graph",
    longLabel: "Fabric Graph (Preview)",
    description: "Disruption propagation across flights",
    iconSrc: "/service-icons/fabric.png",
    isFabric: true,
  },
  VECTOR_REG: {
    id: "VECTOR_REG",
    shortLabel: "Azure AI Search",
    longLabel: "Azure AI Search (Vector + Hybrid)",
    description: "Safety narratives, ADs & regulatory bulletins",
    iconSrc: "/service-icons/azure-ai-search.png",
    isFabric: false,
  },
  FABRIC_SQL: {
    id: "FABRIC_SQL",
    shortLabel: "Fabric SQL Warehouse",
    longLabel: "Fabric SQL Warehouse",
    description: "BTS on-time performance & delay analytics",
    iconSrc: "/service-icons/fabric.png",
    isFabric: true,
  },
  NOSQL: {
    id: "NOSQL",
    shortLabel: "Cosmos DB",
    longLabel: "Azure Cosmos DB (NOTAMs)",
    description: "Active NOTAMs by airport ICAO code",
    iconSrc: "/service-icons/cosmosdb.png",
    isFabric: false,
  },
  UNKNOWN: {
    id: "UNKNOWN",
    shortLabel: "Unknown Source",
    longLabel: "Unknown Source",
    description: "",
    iconSrc: "/service-icons/fabric.png",
    isFabric: false,
  },
};

const SOURCE_ALIASES: Record<string, string> = {
  SQL: "SQL",
  KQL: "KQL",
  GRAPH: "GRAPH",
  VECTOR: "VECTOR_REG",
  VECTOR_OPS: "VECTOR_REG",
  VECTOR_REG: "VECTOR_REG",
  NOSQL: "NOSQL",
  FABRIC_SQL: "FABRIC_SQL",
  FABRICSQL: "FABRIC_SQL",
};

export function normalizeSourceId(source?: string): string {
  if (!source) return "UNKNOWN";
  const upper = source.toUpperCase();
  return SOURCE_ALIASES[upper] || upper;
}

export function getDatastoreVisual(source?: string): DatastoreVisual {
  const normalized = normalizeSourceId(source);
  return VISUALS[normalized] || {
    ...VISUALS.UNKNOWN,
    id: normalized,
    shortLabel: normalized,
    longLabel: normalized,
    description: "",
  };
}
