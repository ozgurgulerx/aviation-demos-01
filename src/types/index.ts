import { z } from "zod";

// PII Detection Types
export interface PiiEntity {
  text: string;
  category: string;
  offset: number;
  length: number;
  confidenceScore: number;
}

export interface PiiCheckResult {
  hasPii: boolean;
  entities: PiiEntity[];
  redactedText?: string;
}

// Chat Types
export interface Message {
  id: string;
  role: "user" | "assistant" | "system";
  content: string;
  createdAt: Date;
  citations?: Citation[];
  isVerified?: boolean;
  toolCalls?: ToolCall[];
}

export interface Citation {
  id: number;
  provider: string;
  dataset: string;
  rowId: string;
  timestamp: string;
  confidence: number;
  excerpt?: string;
}

/** @deprecated Retained for Message type compatibility; not used at runtime. */
export interface ToolCall {
  id: string;
  name: string;
  arguments: Record<string, unknown>;
  result?: unknown;
}

export interface Conversation {
  id: string;
  title: string;
  messages: Message[];
  createdAt: Date;
  updatedAt: Date;
  isSaved?: boolean;
}

// Watchlist Types
export interface WatchlistItem {
  id: string;
  type: "route" | "airport" | "aircraft";
  name: string;
  addedAt: Date;
}

// Source Panel Types
export interface SourceReference {
  citationId: number;
  provider: string;
  dataset: string;
  rowId: string;
  timestamp: string;
  confidence: number;
  excerpt?: string;
  isActive?: boolean;
}

export type TelemetryEventStatus = "running" | "completed" | "error" | "info";
export type OperationalAlertSeverity = "advisory" | "warning" | "critical";
export type ReasoningStage =
  | "understanding_request"
  | "intent_mapped"
  | "evidence_retrieval"
  | "drafting_brief"
  | "evidence_check_complete";
export type ReasoningConfidence = "High" | "Medium" | "Low";
export type ReasoningVerification = "Verified" | "Partial";

export interface TelemetryEvent {
  id: string;
  type:
    | "agent_update"
    | "tool_call"
    | "tool_result"
    | "retrieval_plan"
    | "source_call_start"
    | "source_call_done"
    | "agent_done"
    | "agent_error"
    | "text"
    | "progress"
    | "scenario_loaded"
    | "freshness_guardrail"
    | "fallback_mode_changed"
    | "fabric_preflight"
    | "operational_alert";
  stage: string;
  message: string;
  status: TelemetryEventStatus;
  timestamp: string;
  source?: string;
  rowCount?: number;
  citationCount?: number;
  framework?: string;
  durationMs?: number;
  eventId?: string;
  parentEventId?: string;
  retrievalReason?: string;
  sourceMeta?: {
    storeType?: string;
    endpointLabel?: string;
    freshness?: string;
  };
  evidenceRefs?: number[];
  mode?: "live" | "fallback" | "unknown";
  alertSeverity?: OperationalAlertSeverity;
}

export interface OperationalAlert {
  id: string;
  severity: OperationalAlertSeverity;
  title: string;
  message: string;
  source?: string;
  timestamp: string;
}

export interface ReasoningEventPayload {
  intentLabel?: string;
  confidence?: ReasoningConfidence;
  route?: string;
  sources?: string[];
  callCount?: number;
  verification?: ReasoningVerification;
  failOpen?: boolean;
}

export interface ReasoningSseEvent {
  type: "reasoning_stage";
  stage: ReasoningStage;
  ts: string;
  payload?: ReasoningEventPayload;
}

export interface EvidenceManifestItem {
  id: string;
  source: string;
  dataset: string;
  rowId: string;
  confidence?: number;
  usedInAnswer: boolean;
  timestamp?: string;
}

export interface SourceHealthStatus {
  source: string;
  status: "idle" | "querying" | "ready" | "error";
  rowCount: number;
  updatedAt?: string;
  mode?: "live" | "fallback" | "unknown";
  freshness?: string;
}

export interface SourceResultSnapshot {
  source: string;
  eventId?: string;
  rowCount: number;
  columns: string[];
  rowsPreview: Array<Record<string, unknown>>;
  rowsTruncated?: boolean;
  timestamp: string;
  mode?: "live" | "fallback" | "unknown";
  freshness?: string;
}

export interface FabricPreflightCheck {
  name: string;
  status: "pass" | "warn" | "fail";
  detail: string;
  mode?: string;
  endpoint?: string;
}

export interface FabricPreflightStatus {
  timestamp?: string;
  overall_status: "pass" | "warn" | "fail";
  live_path_available?: boolean;
  checks?: FabricPreflightCheck[];
  error?: string;
}

// API Response Types
export interface ApiResponse<T> {
  success: boolean;
  data?: T;
  error?: string;
}

export const SourceHealthSchema = z.object({
  source: z.string(),
  status: z.enum(["idle", "querying", "ready", "error"]),
  rowCount: z.number(),
  updatedAt: z.string().optional(),
  mode: z.enum(["live", "fallback", "unknown"]).optional(),
  freshness: z.string().optional(),
});
