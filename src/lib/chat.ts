import type {
  Message,
  Conversation,
  TelemetryEvent,
  SourceHealthStatus,
  OperationalAlert,
  OperationalAlertSeverity,
} from "@/types";
import { generateId } from "./utils";
import { normalizeSourceId } from "./datastore";

/**
 * Create a new empty conversation
 */
export function createConversation(): Conversation {
  return {
    id: generateId(),
    title: "New Chat",
    messages: [],
    createdAt: new Date(),
    updatedAt: new Date(),
    isSaved: false,
  };
}

/**
 * Generate a title from the first user message
 */
export function generateConversationTitle(messages: Message[]): string {
  const firstUserMessage = messages.find((m) => m.role === "user");
  if (!firstUserMessage) return "New Chat";

  const content = firstUserMessage.content;
  if (content.length <= 52) return content;

  return `${content.substring(0, 52).trim()}...`;
}

/**
 * Parse streaming SSE response
 */
export interface StreamEvent {
  type:
    | "agent_update"
    | "tool_call"
    | "tool_result"
    | "retrieval_plan"
    | "source_call_start"
    | "source_call_done"
    | "citations"
    | "agent_done"
    | "agent_error"
    | "text"
    | "progress"
    | "scenario_loaded"
    | "freshness_guardrail"
    | "fallback_mode_changed"
    | "fabric_preflight"
    | "operational_alert"
    | "done"
    | "error";
  id?: string;
  stage?: string;
  sessionId?: string;
  framework?: string;
  content?: string;
  message?: string;
  name?: string;
  arguments?: Record<string, unknown>;
  result?: Record<string, unknown>;
  plan?: Record<string, unknown>;
  source?: string;
  reason?: string;
  priority?: number;
  row_count?: number;
  citation_count?: number;
  error?: string;
  citations?: Array<{
    id: number;
    provider: string;
    dataset: string;
    rowId: string;
    timestamp: string;
    confidence: number;
    excerpt?: string;
  }>;
  isVerified?: boolean;
  route?: string;
  reasoning?: string;
  event_id?: string;
  parent_event_id?: string;
  timestamp?: string;
  started_at?: string;
  finished_at?: string;
  duration_ms?: number;
  source_meta?: {
    store_type?: string;
    endpoint_label?: string;
    freshness?: string;
  };
  retrieval_reason?: string;
  evidence_refs?: number[];
  mode?: "live" | "fallback" | "unknown";
  scenario?: string;
  severity?: OperationalAlertSeverity;
  title?: string;
}

export function parseSSELine(line: string): StreamEvent | null {
  const sanitized = line.replace(/\r$/, "").trimStart();
  if (!sanitized.startsWith("data:")) return null;

  try {
    return JSON.parse(sanitized.slice(5).trimStart()) as StreamEvent;
  } catch {
    return null;
  }
}

export function parseSSEFrames(buffer: string): {
  events: StreamEvent[];
  remainder: string;
} {
  const frames = buffer.split(/\r?\n\r?\n/);
  const maybeRemainder = frames.pop();
  const remainder = maybeRemainder ?? "";
  const events: StreamEvent[] = [];

  for (const frame of frames) {
    const lines = frame.split(/\r?\n/);
    for (const line of lines) {
      const parsed = parseSSELine(line);
      if (parsed) {
        events.push(parsed);
      }
    }
  }

  return { events, remainder };
}

export function toTelemetryEvent(event: StreamEvent): TelemetryEvent | null {
  const timestamp = resolveEventTimestamp(event);
  const fallbackId = generateId();

  switch (event.type) {
    case "agent_update":
      return {
        id: fallbackId,
        type: event.type,
        stage: event.stage || "agent",
        message: event.message || "Agent update received",
        status: "running",
        timestamp,
        framework: event.framework,
        eventId: event.event_id,
        parentEventId: event.parent_event_id,
        durationMs: event.duration_ms,
        retrievalReason: event.retrieval_reason,
        sourceMeta: event.source_meta
          ? {
              storeType: event.source_meta.store_type,
              endpointLabel: event.source_meta.endpoint_label,
              freshness: event.source_meta.freshness,
            }
          : undefined,
        evidenceRefs: event.evidence_refs,
      };
    case "tool_call":
      return {
        id: fallbackId,
        type: event.type,
        stage: "tool_call",
        message: `Calling ${event.name || "tool"}`,
        status: "running",
        timestamp,
      };
    case "tool_result":
      return {
        id: fallbackId,
        type: event.type,
        stage: "tool_result",
        message: `${event.name || "Tool"} completed`,
        status: "completed",
        timestamp,
      };
    case "retrieval_plan":
      return {
        id: fallbackId,
        type: event.type,
        stage: "retrieval_plan",
        message: "Retrieval plan assembled",
        status: "completed",
        timestamp,
      };
    case "source_call_start":
      return {
        id: fallbackId,
        type: event.type,
        stage: "source_call",
        message: `Querying ${event.source || "source"}`,
        status: "running",
        timestamp,
        source: normalizeSourceName(event.source),
        eventId: event.event_id,
        parentEventId: event.parent_event_id,
        retrievalReason: event.retrieval_reason,
        sourceMeta: event.source_meta
          ? {
              storeType: event.source_meta.store_type,
              endpointLabel: event.source_meta.endpoint_label,
              freshness: event.source_meta.freshness,
            }
          : undefined,
      };
    case "source_call_done":
      return {
        id: fallbackId,
        type: event.type,
        stage: "source_call",
        message: `${event.source || "Source"} returned ${event.row_count || 0} rows`,
        status: "completed",
        timestamp,
        source: normalizeSourceName(event.source),
        rowCount: event.row_count || 0,
        citationCount: event.citation_count || 0,
        durationMs: event.duration_ms,
        eventId: event.event_id,
        parentEventId: event.parent_event_id,
        sourceMeta: event.source_meta
          ? {
              storeType: event.source_meta.store_type,
              endpointLabel: event.source_meta.endpoint_label,
              freshness: event.source_meta.freshness,
            }
          : undefined,
      };
    case "scenario_loaded":
      return {
        id: fallbackId,
        type: event.type,
        stage: event.stage || "scenario",
        message: event.message || `Scenario loaded: ${event.scenario || "custom"}`,
        status: "completed",
        timestamp,
      };
    case "freshness_guardrail":
      return {
        id: fallbackId,
        type: event.type,
        stage: event.stage || "freshness",
        message: event.message || "Freshness guardrail applied",
        status: "info",
        timestamp,
      };
    case "fallback_mode_changed":
      return {
        id: fallbackId,
        type: event.type,
        stage: event.stage || "source_mode",
        message: event.message || "Source mode changed",
        status: "info",
        timestamp,
        source: normalizeSourceName(event.source),
        mode: event.mode || "unknown",
      };
    case "fabric_preflight":
      return {
        id: fallbackId,
        type: event.type,
        stage: event.stage || "preflight",
        message: event.message || "Fabric preflight completed",
        status: event.mode === "fallback" ? "running" : "completed",
        timestamp,
      };
    case "operational_alert":
      return {
        id: fallbackId,
        type: event.type,
        stage: event.stage || "ops_alert",
        message: event.message || event.title || "Operational advisory",
        status: event.severity === "critical" ? "error" : "info",
        timestamp,
        source: normalizeSourceName(event.source),
        alertSeverity: event.severity,
      };
    case "agent_done":
      return {
        id: fallbackId,
        type: event.type,
        stage: "agent_done",
        message: `Run complete via ${event.route || "orchestrated route"}`,
        status: "completed",
        timestamp,
      };
    case "agent_error":
    case "error":
      return {
        id: fallbackId,
        type: "agent_error",
        stage: "error",
        message: event.message || event.error || "Unexpected error",
        status: "error",
        timestamp,
      };
    case "progress":
      return {
        id: fallbackId,
        type: event.type,
        stage: event.stage || "progress",
        message: event.message || "Progress update",
        status: "running",
        timestamp,
      };
    default:
      return null;
  }
}

export function updateSourceHealth(
  current: SourceHealthStatus[],
  event: StreamEvent
): SourceHealthStatus[] {
  if (event.type === "retrieval_plan") {
    const plannedSources = extractPlannedSources(event.plan);
    if (plannedSources.length === 0) {
      return current;
    }
    const now = resolveEventTimestamp(event);
    const normalizedPlanned = new Set(plannedSources.map((source) => normalizeSourceName(source)));
    const existingBySource = new Set(current.map((item) => item.source));
    const updated: SourceHealthStatus[] = current.map((item): SourceHealthStatus => {
      if (!normalizedPlanned.has(item.source)) {
        return item;
      }
      return {
        ...item,
        status: item.status === "ready" ? "ready" : "querying",
        updatedAt: now,
      };
    });

    for (const source of normalizedPlanned) {
      if (existingBySource.has(source)) {
        continue;
      }
      updated.push({
        source,
        status: "querying",
        rowCount: 0,
        updatedAt: now,
        mode: "unknown",
      });
    }
    return updated;
  }

  if (event.type === "tool_result") {
    const sourceResultCounts = extractSourceResultCounts(event.result);
    if (Object.keys(sourceResultCounts).length === 0) {
      return current;
    }
    const now = resolveEventTimestamp(event);
    const normalizedToCount = Object.entries(sourceResultCounts).reduce(
      (acc, [source, count]) => {
        const normalized = normalizeSourceName(source);
        acc[normalized] = (acc[normalized] || 0) + count;
        return acc;
      },
      {} as Record<string, number>
    );

    const existingBySource = new Set(current.map((item) => item.source));
    const updated: SourceHealthStatus[] = current.map((item): SourceHealthStatus => {
      const count = normalizedToCount[item.source];
      if (typeof count !== "number") {
        return item;
      }
      return {
        ...item,
        status: "ready" as const,
        rowCount: count,
        updatedAt: now,
      };
    });

    for (const [source, count] of Object.entries(normalizedToCount)) {
      if (existingBySource.has(source)) {
        continue;
      }
      updated.push({
        source,
        status: "ready",
        rowCount: count,
        updatedAt: now,
        mode: "unknown",
      });
    }

    return updated;
  }

  if (
    event.type !== "source_call_start" &&
    event.type !== "source_call_done" &&
    event.type !== "fallback_mode_changed"
  ) {
    return current;
  }

  const source = normalizeSourceName(event.source);
  const existing = current.find((item) => item.source === source);
  const next = [...current];
  const mode =
    event.mode ||
    (event.source_meta?.endpoint_label === "live" || event.source_meta?.endpoint_label === "fallback"
      ? event.source_meta?.endpoint_label
      : undefined);
  const freshness = event.source_meta?.freshness;
  const eventTimestamp = resolveEventTimestamp(event);

  if (!existing) {
    next.push({
      source,
      status:
        event.type === "source_call_start"
          ? "querying"
          : event.type === "source_call_done"
            ? "ready"
            : "idle",
      rowCount: event.row_count || 0,
      updatedAt: eventTimestamp,
      mode: mode || "unknown",
      freshness,
    });
    return next;
  }

  return next.map((item) =>
    item.source === source
      ? {
          ...item,
          status:
            event.type === "source_call_start"
              ? "querying"
              : event.type === "source_call_done"
                ? "ready"
                : item.status,
          rowCount:
            event.type === "source_call_done"
              ? event.row_count || item.rowCount
              : item.rowCount,
          updatedAt: eventTimestamp,
          mode: (mode as SourceHealthStatus["mode"]) || item.mode,
          freshness: freshness || item.freshness,
        }
      : item
  );
}

function resolveEventTimestamp(event: StreamEvent): string {
  const candidate = event.finished_at || event.started_at || event.timestamp;
  if (candidate) {
    const parsed = new Date(candidate);
    if (!Number.isNaN(parsed.getTime())) {
      return parsed.toISOString();
    }
  }
  return new Date().toISOString();
}

export function toOperationalAlert(event: StreamEvent): OperationalAlert | null {
  const timestamp = resolveEventTimestamp(event);
  if (event.type === "operational_alert") {
    return {
      id: event.id || event.event_id || generateId(),
      severity: event.severity || "warning",
      title: event.title || "Operational Advisory",
      message: event.message || "A new advisory was received from runtime telemetry.",
      source: normalizeSourceName(event.source),
      timestamp,
    };
  }

  if (event.type === "agent_error" || event.type === "error") {
    return {
      id: event.id || event.event_id || generateId(),
      severity: "critical",
      title: "Runtime Alert",
      message: event.message || event.error || "An unexpected runtime error occurred.",
      source: normalizeSourceName(event.source),
      timestamp,
    };
  }

  if (event.type === "fallback_mode_changed" && event.mode === "fallback" && event.source) {
    return {
      id: event.id || event.event_id || generateId(),
      severity: "warning",
      title: "Source Fallback Active",
      message: `${normalizeSourceName(event.source)} switched to fallback mode. Validate freshness before dispatch decisions.`,
      source: normalizeSourceName(event.source),
      timestamp,
    };
  }

  return null;
}

function extractSourceResultCounts(result?: Record<string, unknown>): Record<string, number> {
  if (!result) {
    return {};
  }
  const raw = result["source_result_counts"];
  if (!raw || typeof raw !== "object") {
    return {};
  }

  const out: Record<string, number> = {};
  for (const [source, value] of Object.entries(raw as Record<string, unknown>)) {
    const count = Number(value);
    if (!Number.isFinite(count) || count < 0) {
      continue;
    }
    out[source] = count;
  }
  return out;
}

function extractPlannedSources(plan?: Record<string, unknown>): string[] {
  if (!plan) {
    return [];
  }
  const steps = plan["steps"];
  if (!Array.isArray(steps)) {
    return [];
  }
  const sources: string[] = [];
  for (const step of steps) {
    if (!step || typeof step !== "object") {
      continue;
    }
    const source = (step as Record<string, unknown>)["source"];
    if (typeof source !== "string" || !source.trim()) {
      continue;
    }
    sources.push(source.trim());
  }
  return sources;
}

function normalizeSourceName(source?: string): string {
  return normalizeSourceId(source);
}

/**
 * Format message for API request
 */
export function formatMessagesForApi(
  messages: Message[]
): Array<{ role: string; content: string }> {
  return messages.map((m) => ({
    role: m.role,
    content: m.content,
  }));
}
