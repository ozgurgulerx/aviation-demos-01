import type { Message, Conversation, TelemetryEvent, SourceHealthStatus } from "@/types";
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
}

export function parseSSELine(line: string): StreamEvent | null {
  if (!line.startsWith("data: ")) return null;

  try {
    return JSON.parse(line.slice(6)) as StreamEvent;
  } catch {
    return null;
  }
}

export function parseSSEFrames(buffer: string): {
  events: StreamEvent[];
  remainder: string;
} {
  const frames = buffer.split("\n\n");
  const remainder = frames.pop() || "";
  const events: StreamEvent[] = [];

  for (const frame of frames) {
    const lines = frame.split("\n");
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
