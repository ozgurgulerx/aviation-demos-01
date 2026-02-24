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
export type MessageStatus = "loading" | "streaming" | "complete" | "error";

export interface Message {
  id: string;
  role: "user" | "assistant" | "system";
  content: string;
  createdAt: Date;
  citations?: Citation[];
  isVerified?: boolean;
  toolCalls?: ToolCall[];
  status?: MessageStatus;      // undefined = "complete" (backward compat)
  errorMessage?: string;       // populated when status is "error"
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
  | "pii_scan"
  | "understanding_request"
  | "intent_mapped"
  | "evidence_retrieval"
  | "drafting_brief"
  | "evidence_check_complete";

export interface GroundingInfo {
  hasCitations: boolean;
  citationMarkers: number[];
  invalidMarkers: number[];
  groundingStatus: "grounded" | "partially_grounded" | "ungrounded";
}
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
    | "agent_partial_done"
    | "agent_done"
    | "agent_error"
    | "text"
    | "progress"
    | "scenario_loaded"
    | "freshness_guardrail"
    | "fallback_mode_changed"
    | "fabric_preflight"
    | "operational_alert"
    | "pii_redacted";
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
  contractStatus?: "planned" | "met" | "degraded" | "failed";
  errorCode?: string;
  terminalReason?: string;
  degradedSources?: string[];
  failedRequiredSources?: string[];
  requiredSourcesSatisfied?: boolean;
  missingRequiredSources?: string[];
  sourcePolicy?: "include" | "exact";
  fatalSourceCount?: number;
  failurePolicy?: "graceful" | "strict";
  partial?: boolean;
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
  detail?: string;
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

// Predictive Optimization Types
export interface PredictiveDelayRow {
  flight_leg_id: string;
  flight_number: string;
  origin: string;
  dest: string;
  std_utc?: string | null;
  risk_a15?: number | null;
  expected_delay_minutes?: number | null;
  prediction_interval?: {
    low?: number | null;
    high?: number | null;
  };
  top_drivers?: string[];
  model_variant?: "baseline" | "optimized" | string;
  model_version?: string | null;
  data_freshness?: string | null;
  degraded_sources?: string[];
}

export interface PredictiveDelaysResponse {
  status: "ok" | "empty" | "degraded" | "disabled" | "error";
  enabled?: boolean;
  message?: string;
  error?: string;
  model?: "baseline" | "optimized" | string;
  window_hours?: number;
  as_of_utc?: string;
  row_count?: number;
  rows: PredictiveDelayRow[];
}

export interface PredictiveMetricsResponse {
  status: "ok" | "degraded" | "disabled" | "error";
  enabled?: boolean;
  message?: string;
  error?: string;
  as_of_utc?: string;
  sample_window?: string | null;
  baseline?: {
    auroc?: number | null;
    brier?: number | null;
    mae?: number | null;
  };
  optimized?: {
    auroc?: number | null;
    brier?: number | null;
    mae?: number | null;
  };
  uplift?: {
    auroc_delta?: number | null;
    brier_delta?: number | null;
    mae_delta?: number | null;
  };
}

export interface PredictiveActionRow {
  flight_leg_id: string;
  flight_number: string;
  action_rank?: number | null;
  action_code?: string | null;
  action_label?: string | null;
  expected_delta_minutes?: number | null;
  feasibility_status?: string | null;
  confidence_band?: string | null;
  constraint_notes?: string | null;
  model_variant?: "baseline" | "optimized" | string;
}

export interface PredictiveActionsResponse {
  status: "ok" | "empty" | "degraded" | "disabled" | "error";
  enabled?: boolean;
  message?: string;
  error?: string;
  model?: "baseline" | "optimized" | string;
  as_of_utc?: string;
  row_count?: number;
  actions: PredictiveActionRow[];
}

export interface PredictiveDecisionMetricsResponse {
  status: "ok" | "degraded" | "disabled" | "error";
  enabled?: boolean;
  message?: string;
  error?: string;
  as_of_utc?: string;
  metrics?: {
    total_decisions?: number;
    override_count?: number;
    approved_count?: number;
    feasible_count?: number;
    model_variant_count?: number;
  };
}
