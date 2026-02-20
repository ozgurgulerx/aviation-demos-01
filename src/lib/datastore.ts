export interface DatastoreVisual {
  id: string;
  shortLabel: string;
  longLabel: string;
  iconSrc: string;
  isFabric: boolean;
}

const VISUALS: Record<string, DatastoreVisual> = {
  KQL: {
    id: "KQL",
    shortLabel: "Fabric Eventhouse",
    longLabel: "Fabric Eventhouse (KQL)",
    iconSrc: "/service-icons/fabric.png",
    isFabric: true,
  },
  SQL: {
    id: "SQL",
    shortLabel: "Fabric Warehouse",
    longLabel: "Fabric Warehouse (SQL)",
    iconSrc: "/service-icons/fabric.png",
    isFabric: true,
  },
  GRAPH: {
    id: "GRAPH",
    shortLabel: "Fabric Graph",
    longLabel: "Fabric Graph (Preview)",
    iconSrc: "/service-icons/fabric.png",
    isFabric: true,
  },
  VECTOR_REG: {
    id: "VECTOR_REG",
    shortLabel: "Azure AI Search",
    longLabel: "Azure AI Search (Vector + Hybrid)",
    iconSrc: "/service-icons/azure-ai-search.png",
    isFabric: false,
  },
  NOSQL: {
    id: "NOSQL",
    shortLabel: "Azure PostgreSQL",
    longLabel: "Azure PostgreSQL",
    iconSrc: "/service-icons/postgresql.jpeg",
    isFabric: false,
  },
  POSTGRES: {
    id: "POSTGRES",
    shortLabel: "Azure PostgreSQL",
    longLabel: "Azure PostgreSQL",
    iconSrc: "/service-icons/postgresql.jpeg",
    isFabric: false,
  },
  UNKNOWN: {
    id: "UNKNOWN",
    shortLabel: "Unknown Source",
    longLabel: "Unknown Source",
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
  POSTGRES: "POSTGRES",
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
  };
}
