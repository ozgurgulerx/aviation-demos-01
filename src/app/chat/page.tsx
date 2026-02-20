"use client";

import { useState, useCallback, useEffect, useRef } from "react";
import { AnimatePresence, motion, useReducedMotion } from "framer-motion";
import { AlertTriangle, X, Settings2 } from "lucide-react";
import { Sidebar } from "@/components/layout/sidebar";
import { SourcesPanel } from "@/components/layout/sources-panel";
import { ChatThread } from "@/components/chat/chat-thread";
import { TimelinePanel } from "@/components/chat/timeline-panel";
import { MessageComposer } from "@/components/chat/message-composer";
import { FollowUpChips } from "@/components/chat/follow-up-chips";
import { ToggleGroup } from "@/components/ui/switch";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";
import { Badge } from "@/components/ui/badge";
import { ArchitectureMap } from "@/components/architecture/architecture-map";
import { cn, deduplicateCitations, generateId } from "@/lib/utils";
import { parseSSEFrames, toOperationalAlert, toTelemetryEvent, updateSourceHealth } from "@/lib/chat";
import { normalizeSourceId } from "@/lib/datastore";
import {
  SAMPLE_CONVERSATIONS,
  ENHANCED_FOLLOW_UP_SUGGESTIONS,
  DATA_SOURCE_BLUEPRINT,
} from "@/data/seed";
import type {
  Message,
  Citation,
  TelemetryEvent,
  SourceHealthStatus,
  FabricPreflightStatus,
  OperationalAlert,
  ReasoningConfidence,
  ReasoningEventPayload,
  ReasoningSseEvent,
  ReasoningStage,
  SourceResultSnapshot,
} from "@/types";
import type { StreamEvent } from "@/lib/chat";

type QueryProfile = "pilot-brief" | "ops-live" | "compliance";
type DemoScenario = "none" | "weather-spike" | "runway-notam" | "ground-bottleneck";
type VoiceMode = "off" | "tr-TR" | "en-US";
type VoiceClipStatus = "idle" | "preparing" | "ready" | "error";

function createInitialSourceHealth(): SourceHealthStatus[] {
  return DATA_SOURCE_BLUEPRINT.map((source) => ({
    source: source.id,
    status: "idle",
    rowCount: 0,
    mode: "unknown",
  }));
}

function toSpeechText(raw: string): string {
  return raw
    .replace(/```[\s\S]*?```/g, " ")
    .replace(/`([^`]+)`/g, "$1")
    .replace(/\[(\d+)\]/g, " ")
    .replace(/[#>*_~|]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

const REASONING_STAGE_ORDER: ReasoningStage[] = [
  "understanding_request",
  "intent_mapped",
  "evidence_retrieval",
  "drafting_brief",
  "evidence_check_complete",
];

function getReasoningStageRank(stage: ReasoningStage): number {
  const index = REASONING_STAGE_ORDER.indexOf(stage);
  return index === -1 ? REASONING_STAGE_ORDER.length : index;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function normalizeIntentLabel(value: string): string {
  const compact = value.replace(/[._-]+/g, " ").replace(/\s+/g, " ").trim().toLowerCase();
  if (!compact) {
    return "Intent mapped";
  }
  return compact.replace(/\b\w/g, (char) => char.toUpperCase());
}

function parseReasoningConfidence(value: unknown): ReasoningConfidence | undefined {
  if (typeof value === "number") {
    if (value >= 0.75) return "High";
    if (value >= 0.4) return "Medium";
    return "Low";
  }

  if (typeof value !== "string") {
    return undefined;
  }

  const normalized = value.trim().toLowerCase();
  if (!normalized) {
    return undefined;
  }
  if (normalized === "high") return "High";
  if (normalized === "medium") return "Medium";
  if (normalized === "low") return "Low";

  const numeric = Number(normalized);
  if (Number.isFinite(numeric)) {
    return parseReasoningConfidence(numeric);
  }
  return undefined;
}

function resolveStreamEventTimestamp(event: StreamEvent): string {
  const candidate = event.finished_at || event.started_at || event.timestamp;
  if (!candidate) {
    return new Date().toISOString();
  }
  const parsed = new Date(candidate);
  if (Number.isNaN(parsed.getTime())) {
    return new Date().toISOString();
  }
  return parsed.toISOString();
}

function extractIntentPayloadFromPlan(plan?: Record<string, unknown>): ReasoningEventPayload {
  if (!plan) {
    return {};
  }

  const payload: ReasoningEventPayload = {};

  if (typeof plan.route === "string" && plan.route.trim()) {
    payload.route = plan.route.trim();
  }

  const intentObject = isRecord(plan.intent) ? plan.intent : undefined;
  if (intentObject && typeof intentObject.name === "string" && intentObject.name.trim()) {
    payload.intentLabel = normalizeIntentLabel(intentObject.name);
  }
  const intentConfidence = intentObject ? parseReasoningConfidence(intentObject.confidence) : undefined;
  if (intentConfidence) {
    payload.confidence = intentConfidence;
  }

  const reasoning = typeof plan.reasoning === "string" ? plan.reasoning : "";
  if (!payload.intentLabel && reasoning) {
    const match = reasoning.match(/(?:^|;)\s*intent=([^;]+)/i);
    if (match?.[1]) {
      payload.intentLabel = normalizeIntentLabel(match[1]);
    }
  }
  if (!payload.confidence && reasoning) {
    const match = reasoning.match(/(?:^|;)\s*confidence=([^;]+)/i);
    if (match?.[1]) {
      const confidence = parseReasoningConfidence(match[1]);
      if (confidence) {
        payload.confidence = confidence;
      }
    }
  }

  if (!payload.confidence && payload.intentLabel) {
    const warnings = Array.isArray(plan.warnings) ? plan.warnings.length : 0;
    const verified = plan.is_verified === true;
    payload.confidence = verified ? "High" : warnings > 0 ? "Medium" : "Low";
  }

  return payload;
}

function extractPlannedSourcesFromPlan(plan?: Record<string, unknown>): string[] {
  if (!plan) {
    return [];
  }

  const steps = plan.steps;
  if (!Array.isArray(steps)) {
    return [];
  }

  const seen = new Set<string>();
  const sources: string[] = [];
  for (const step of steps) {
    if (!isRecord(step)) continue;
    const raw = step.source;
    if (typeof raw !== "string" || !raw.trim()) continue;
    const normalized = normalizeSourceId(raw);
    if (seen.has(normalized)) continue;
    seen.add(normalized);
    sources.push(normalized);
  }
  return sources;
}

function extractSourceCountsFromToolResult(result?: Record<string, unknown>): Record<string, number> {
  if (!result) {
    return {};
  }

  const rawCounts = result.source_result_counts;
  if (!isRecord(rawCounts)) {
    return {};
  }

  const counts: Record<string, number> = {};
  for (const [source, countCandidate] of Object.entries(rawCounts)) {
    const count = Number(countCandidate);
    if (!Number.isFinite(count) || count < 0) {
      continue;
    }
    counts[normalizeSourceId(source)] = count;
  }
  return counts;
}

function extractPreviewColumns(columns: unknown): string[] {
  if (!Array.isArray(columns)) {
    return [];
  }
  const seen = new Set<string>();
  const out: string[] = [];
  for (const candidate of columns) {
    if (typeof candidate !== "string" || !candidate.trim()) {
      continue;
    }
    const column = candidate.trim();
    if (seen.has(column)) {
      continue;
    }
    seen.add(column);
    out.push(column);
    if (out.length >= 8) {
      break;
    }
  }
  return out;
}

function extractPreviewRows(rows: unknown): Array<Record<string, unknown>> {
  if (!Array.isArray(rows)) {
    return [];
  }
  const out: Array<Record<string, unknown>> = [];
  for (const row of rows.slice(0, 5)) {
    if (!isRecord(row)) {
      continue;
    }
    out.push({ ...row });
  }
  return out;
}

function buildSourceSnapshot(event: StreamEvent, source: string): SourceResultSnapshot {
  const rowsPreview = extractPreviewRows(event.rows_preview);
  const fallbackColumns =
    rowsPreview.length > 0
      ? Object.keys(rowsPreview[0] || {}).filter((column) => column && !column.startsWith("__"))
      : [];
  const columns = extractPreviewColumns(event.columns);
  return {
    source,
    eventId: event.event_id,
    rowCount: Number(event.row_count || 0),
    columns: columns.length > 0 ? columns : fallbackColumns,
    rowsPreview,
    rowsTruncated: event.rows_truncated,
    timestamp: resolveStreamEventTimestamp(event),
    mode:
      (event.mode as SourceResultSnapshot["mode"]) ||
      (event.source_meta?.endpoint_label === "live" || event.source_meta?.endpoint_label === "fallback"
        ? (event.source_meta.endpoint_label as SourceResultSnapshot["mode"])
        : undefined),
    freshness: event.source_meta?.freshness,
  };
}

function mergeReasoningPayload(
  previous?: ReasoningEventPayload,
  incoming?: ReasoningEventPayload
): ReasoningEventPayload | undefined {
  if (!previous && !incoming) {
    return undefined;
  }

  const merged: ReasoningEventPayload = { ...(previous || {}) };
  if (incoming?.intentLabel !== undefined) {
    merged.intentLabel = incoming.intentLabel;
  }
  if (incoming?.confidence !== undefined) {
    merged.confidence = incoming.confidence;
  }
  if (incoming?.route !== undefined) {
    merged.route = incoming.route;
  }
  if (incoming?.sources !== undefined) {
    merged.sources = incoming.sources;
  }
  if (incoming?.callCount !== undefined) {
    merged.callCount = incoming.callCount;
  }
  if (incoming?.verification !== undefined) {
    merged.verification = incoming.verification;
  }
  if (incoming?.failOpen !== undefined) {
    merged.failOpen = incoming.failOpen;
  }

  return Object.keys(merged).length > 0 ? merged : undefined;
}

function upsertReasoningEventTimeline(
  previous: ReasoningSseEvent[],
  incoming: ReasoningSseEvent
): ReasoningSseEvent[] {
  const existingIndex = previous.findIndex((event) => event.stage === incoming.stage);
  if (existingIndex >= 0) {
    const existing = previous[existingIndex];
    const mergedPayload = mergeReasoningPayload(existing.payload, incoming.payload);
    const next = [...previous];
    next[existingIndex] = {
      ...existing,
      ...incoming,
      payload: mergedPayload,
    };
    return next;
  }

  const next = [...previous, incoming];
  next.sort((a, b) => getReasoningStageRank(a.stage) - getReasoningStageRank(b.stage));
  return next;
}

export default function ChatPage() {
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false);
  const [sourcesPanelCollapsed, setSourcesPanelCollapsed] = useState(false);

  const [queryProfile, setQueryProfile] = useState<QueryProfile>("pilot-brief");
  const [explainRetrieval, setExplainRetrieval] = useState(true);
  const [freshnessSlaMinutes, setFreshnessSlaMinutes] = useState<number>(60);
  const [demoScenario, setDemoScenario] = useState<DemoScenario>("none");


  const [activeConversationId, setActiveConversationId] = useState<string | null>(
    SAMPLE_CONVERSATIONS[0]?.id || null
  );
  const [messages, setMessages] = useState<Message[]>(
    SAMPLE_CONVERSATIONS[0]?.messages || []
  );
  const [citations, setCitations] = useState<Citation[]>(
    deduplicateCitations(
      SAMPLE_CONVERSATIONS[0]?.messages.flatMap((message) => message.citations || []) || []
    )
  );

  const [activeCitationId, setActiveCitationId] = useState<number | null>(null);

  const [isLoading, setIsLoading] = useState(false);
  const [streamingContent, setStreamingContent] = useState("");
  const [timelineEvents, setTimelineEvents] = useState<TelemetryEvent[]>([]);
  const [reasoningEvents, setReasoningEvents] = useState<ReasoningSseEvent[]>([]);
  const [sourceSnapshots, setSourceSnapshots] = useState<Record<string, SourceResultSnapshot>>({});
  const [sourceHealth, setSourceHealth] = useState<SourceHealthStatus[]>(
    createInitialSourceHealth()
  );

  const [showFollowUps, setShowFollowUps] = useState(true);
  const [routeLabel, setRouteLabel] = useState<string>("Pending");
  const [confidenceLabel, setConfidenceLabel] = useState<string>("Awaiting run");
  const [fabricPreflight, setFabricPreflight] = useState<FabricPreflightStatus | null>(null);
  const [preflightLoading, setPreflightLoading] = useState(false);
  const [operationalAlert, setOperationalAlert] = useState<OperationalAlert | null>(null);
  const [showAdvanced, setShowAdvanced] = useState(false);
  const [voiceMode, setVoiceMode] = useState<VoiceMode>("off");
  const [speakingMessageId, setSpeakingMessageId] = useState<string | null>(null);
  const [voiceStatuses, setVoiceStatuses] = useState<Record<string, VoiceClipStatus>>({});

  const speechRequestRef = useRef(0);
  const voicePreparationSeqRef = useRef(0);
  const voicePreparationByMessageRef = useRef<Record<string, number>>({});
  const voiceClipByMessageRef = useRef<Record<string, { blob: Blob; text: string; language: string }>>({});
  const retrievalProgressRef = useRef<{ sources: Set<string>; callCount: number }>({
    sources: new Set<string>(),
    callCount: 0,
  });
  const audioRef = useRef<HTMLAudioElement | null>(null);
  const audioUrlRef = useRef<string | null>(null);

  const emitReasoningEvent = useCallback(
    (stage: ReasoningStage, payload?: ReasoningEventPayload, ts?: string) => {
      setReasoningEvents((previous) =>
        upsertReasoningEventTimeline(previous, {
          type: "reasoning_stage",
          stage,
          ts: ts || new Date().toISOString(),
          payload,
        })
      );
    },
    []
  );

  const markEvidenceRetrieval = useCallback(
    (sources: string[], callIncrement: number, ts?: string) => {
      if (!sources.length && callIncrement <= 0) {
        return;
      }

      if (callIncrement > 0) {
        retrievalProgressRef.current.callCount += callIncrement;
      }

      for (const source of sources) {
        retrievalProgressRef.current.sources.add(normalizeSourceId(source));
      }

      emitReasoningEvent(
        "evidence_retrieval",
        {
          sources: Array.from(retrievalProgressRef.current.sources),
          callCount: retrievalProgressRef.current.callCount,
        },
        ts
      );
    },
    [emitReasoningEvent]
  );

  const handleNewChat = useCallback(() => {
    setActiveConversationId(null);
    setMessages([]);
    setCitations([]);
    setActiveCitationId(null);
    setShowFollowUps(false);
    setStreamingContent("");
    setTimelineEvents([]);
    setReasoningEvents([]);
    setSourceSnapshots({});
    setSourceHealth(createInitialSourceHealth());
    setRouteLabel("Pending");
    setConfidenceLabel("Awaiting run");
    setOperationalAlert(null);
    setVoiceStatuses({});
    voiceClipByMessageRef.current = {};
    voicePreparationByMessageRef.current = {};
    retrievalProgressRef.current = { sources: new Set<string>(), callCount: 0 };
  }, []);

  const handleSelectConversation = useCallback((id: string) => {
    const conversation = SAMPLE_CONVERSATIONS.find((item) => item.id === id);
    if (!conversation) return;

    setActiveConversationId(id);
    setMessages(conversation.messages);
    setCitations(
      deduplicateCitations(
        conversation.messages.flatMap((message) => message.citations || [])
      )
    );
    setActiveCitationId(null);
    setShowFollowUps(true);
    setReasoningEvents([]);
    setSourceSnapshots({});
    setVoiceStatuses({});
    voiceClipByMessageRef.current = {};
    voicePreparationByMessageRef.current = {};
    retrievalProgressRef.current = { sources: new Set<string>(), callCount: 0 };
  }, []);

  const handleCitationClick = useCallback((id: number) => {
    setActiveCitationId((previous) => (previous === id ? null : id));
  }, []);


  const fetchFabricPreflight = useCallback(async () => {
    setPreflightLoading(true);
    try {
      const response = await fetch("/api/fabric/preflight", { method: "GET" });
      const payload = (await response.json()) as FabricPreflightStatus;
      setFabricPreflight(payload);
    } catch (error) {
      setFabricPreflight({
        overall_status: "fail",
        error: error instanceof Error ? error.message : "Unable to fetch preflight",
      });
    } finally {
      setPreflightLoading(false);
    }
  }, []);

  useEffect(() => {
    void fetchFabricPreflight();
  }, [fetchFabricPreflight]);

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }

    const onAuditOpen = (event: Event) => {
      const detail = (event as CustomEvent<{ tab?: string }>).detail;
      if (detail?.tab === "queries") {
        setSourcesPanelCollapsed(false);
      }
    };

    window.addEventListener("pilotbrief:audit-open", onAuditOpen as EventListener);
    return () => {
      window.removeEventListener("pilotbrief:audit-open", onAuditOpen as EventListener);
    };
  }, []);

  const stopVoicePlayback = useCallback(() => {
    speechRequestRef.current += 1;

    if (audioRef.current) {
      audioRef.current.pause();
      audioRef.current = null;
    }
    if (audioUrlRef.current) {
      URL.revokeObjectURL(audioUrlRef.current);
      audioUrlRef.current = null;
    }
    setSpeakingMessageId(null);
  }, []);

  const setVoiceStatus = useCallback((messageId: string, status: VoiceClipStatus) => {
    setVoiceStatuses((previous) => {
      if (previous[messageId] === status) {
        return previous;
      }
      return {
        ...previous,
        [messageId]: status,
      };
    });
  }, []);

  const clearVoiceClips = useCallback(() => {
    setVoiceStatuses({});
    voiceClipByMessageRef.current = {};
    voicePreparationByMessageRef.current = {};
  }, []);

  const prepareVoiceClip = useCallback(
    async (messageId: string, rawContent: string): Promise<boolean> => {
      if (voiceMode === "off") return false;

      const text = toSpeechText(rawContent);
      if (!text) {
        setVoiceStatus(messageId, "error");
        return false;
      }

      const language = voiceMode === "tr-TR" ? "tr-TR" : "en-US";
      const existingClip = voiceClipByMessageRef.current[messageId];
      if (existingClip && existingClip.text === text && existingClip.language === language) {
        setVoiceStatus(messageId, "ready");
        return true;
      }

      const preparationId = ++voicePreparationSeqRef.current;
      voicePreparationByMessageRef.current[messageId] = preparationId;
      setVoiceStatus(messageId, "preparing");

      try {
        const response = await fetch("/api/voice/speak", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ text, language }),
        });
        if (!response.ok) {
          throw new Error("Voice model request failed");
        }

        const blob = await response.blob();
        if (voicePreparationByMessageRef.current[messageId] !== preparationId) {
          return false;
        }

        voiceClipByMessageRef.current[messageId] = {
          blob,
          text,
          language,
        };
        setVoiceStatus(messageId, "ready");
        return true;
      } catch {
        if (voicePreparationByMessageRef.current[messageId] !== preparationId) {
          return false;
        }
        delete voiceClipByMessageRef.current[messageId];
        setVoiceStatus(messageId, "error");
        return false;
      }
    },
    [setVoiceStatus, voiceMode]
  );

  const speakMessage = useCallback(
    async (messageId: string, rawContent: string) => {
      if (voiceMode === "off") return;

      const text = toSpeechText(rawContent);
      if (!text) {
        setVoiceStatus(messageId, "error");
        return;
      }

      if (speakingMessageId === messageId) {
        stopVoicePlayback();
        return;
      }

      const language = voiceMode === "tr-TR" ? "tr-TR" : "en-US";
      const existingClip = voiceClipByMessageRef.current[messageId];
      let clip = existingClip;

      if (!clip || clip.text !== text || clip.language !== language) {
        const prepared = await prepareVoiceClip(messageId, rawContent);
        if (!prepared) return;
        clip = voiceClipByMessageRef.current[messageId];
      }
      if (!clip) return;

      stopVoicePlayback();
      const requestId = speechRequestRef.current;
      const audioUrl = URL.createObjectURL(clip.blob);
      audioUrlRef.current = audioUrl;
      const audio = new Audio(audioUrl);
      audioRef.current = audio;
      setSpeakingMessageId(messageId);

      const finishIfActive = () => {
        if (audioUrlRef.current) {
          URL.revokeObjectURL(audioUrlRef.current);
          audioUrlRef.current = null;
        }
        audioRef.current = null;
        if (speechRequestRef.current === requestId) {
          setSpeakingMessageId(null);
        }
      };

      audio.onended = finishIfActive;
      audio.onerror = () => {
        setVoiceStatus(messageId, "error");
        finishIfActive();
      };

      try {
        await audio.play();
      } catch {
        setVoiceStatus(messageId, "error");
        finishIfActive();
      }
    },
    [prepareVoiceClip, setVoiceStatus, speakingMessageId, stopVoicePlayback, voiceMode]
  );

  useEffect(() => {
    if (voiceMode === "off") {
      stopVoicePlayback();
      return;
    }

    for (const message of messages) {
      if (message.role !== "assistant") continue;
      const text = toSpeechText(message.content);
      const language = voiceMode === "tr-TR" ? "tr-TR" : "en-US";
      const existingClip = voiceClipByMessageRef.current[message.id];
      const status = voiceStatuses[message.id] || "idle";
      const isReusable =
        !!existingClip && existingClip.text === text && existingClip.language === language;
      if (isReusable || status === "preparing" || status === "error") {
        continue;
      }
      void prepareVoiceClip(message.id, message.content);
    }
  }, [messages, prepareVoiceClip, stopVoicePlayback, voiceMode, voiceStatuses]);

  useEffect(() => {
    if (!speakingMessageId) {
      return;
    }
    const messageStillVisible = messages.some((message) => message.id === speakingMessageId);
    if (!messageStillVisible) {
      stopVoicePlayback();
    }
  }, [messages, speakingMessageId, stopVoicePlayback]);

  useEffect(() => {
    if (voiceMode === "off") {
      clearVoiceClips();
    }
  }, [clearVoiceClips, voiceMode]);

  useEffect(() => {
    return () => {
      stopVoicePlayback();
      clearVoiceClips();
    };
  }, [clearVoiceClips, stopVoicePlayback]);

  const handleSendMessage = useCallback(
    async (content: string) => {
      const conversationId = activeConversationId ?? generateId();
      if (!activeConversationId) {
        setActiveConversationId(conversationId);
      }

      const userMessage: Message = {
        id: generateId(),
        role: "user",
        content,
        createdAt: new Date(),
      };

      setMessages((previous) => [...previous, userMessage]);
      setIsLoading(true);
      setStreamingContent("");
      setTimelineEvents([]);
      setReasoningEvents([]);
      setShowFollowUps(false);
      setSourceSnapshots({});
      setSourceHealth(createInitialSourceHealth());
      setRouteLabel("Running");
      setConfidenceLabel("Calculating");
      setOperationalAlert(null);
      retrievalProgressRef.current = { sources: new Set<string>(), callCount: 0 };
      emitReasoningEvent("understanding_request");
      stopVoicePlayback();

      try {
        const response = await fetch("/api/chat", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            messages: [...messages, userMessage].map((message) => ({
              role: message.role,
              content: message.content,
            })),
            retrievalMode: "code-rag",
            conversationId,
            queryProfile,
            freshnessSlaMinutes,
            explainRetrieval,
            demoScenario: demoScenario === "none" ? undefined : demoScenario,
          }),
        });

        if (!response.ok) {
          throw new Error("Chat request failed");
        }

        const reader = response.body?.getReader();
        if (!reader) throw new Error("No response body");

        const decoder = new TextDecoder();
        let fullContent = "";
        let newCitations: Citation[] = [];
        let isVerified = false;
        let buffer = "";

        const processEvent = (event: ReturnType<typeof parseSSEFrames>["events"][number]) => {
          const eventTs = resolveStreamEventTimestamp(event);

          if (event.type === "tool_call" || event.type === "progress") {
            emitReasoningEvent("understanding_request", undefined, eventTs);
          }

          if (event.type === "retrieval_plan" && event.plan) {
            const intentPayload = extractIntentPayloadFromPlan(event.plan);
            if (!intentPayload.intentLabel) {
              intentPayload.intentLabel = "Operational brief intent";
            }
            if (!intentPayload.confidence) {
              intentPayload.confidence = "Medium";
            }
            emitReasoningEvent("intent_mapped", intentPayload, eventTs);

            const plannedSources = extractPlannedSourcesFromPlan(event.plan);
            markEvidenceRetrieval(plannedSources, 0, eventTs);
          }

          if (event.type === "source_call_start") {
            const source = typeof event.source === "string" ? normalizeSourceId(event.source) : undefined;
            markEvidenceRetrieval(source ? [source] : [], 1, eventTs);
          }

          if (event.type === "source_call_done") {
            const source = typeof event.source === "string" ? normalizeSourceId(event.source) : undefined;
            markEvidenceRetrieval(source ? [source] : [], 0, eventTs);
            if (source) {
              const snapshot = buildSourceSnapshot(event, source);
              setSourceSnapshots((previous) => ({
                ...previous,
                [source]: snapshot,
              }));
            }
          }

          if (event.type === "tool_result") {
            const sourceCounts = extractSourceCountsFromToolResult(event.result);
            const retrievedSources = Object.keys(sourceCounts);
            const callIncrement =
              retrievalProgressRef.current.callCount === 0 ? retrievedSources.length : 0;
            markEvidenceRetrieval(retrievedSources, callIncrement, eventTs);

            const route =
              typeof event.result?.route === "string" && event.result.route.trim()
                ? event.result.route.trim()
                : undefined;
            emitReasoningEvent("drafting_brief", route ? { route } : undefined, eventTs);
          }

          if ((event.type === "agent_update" || event.type === "text") && event.content) {
            fullContent += event.content;
            setStreamingContent(fullContent);
            emitReasoningEvent("drafting_brief", undefined, eventTs);
          }

          if (event.type === "citations" && event.citations) {
            newCitations = event.citations;
          }

          if (event.type === "agent_done" || event.type === "done") {
            isVerified = !!event.isVerified;
            setRouteLabel(event.route || "ORCHESTRATED");
            emitReasoningEvent(
              "evidence_check_complete",
              {
                verification: isVerified ? "Verified" : "Partial",
                failOpen: !isVerified,
                route: event.route || undefined,
              },
              eventTs
            );
          }

          if (event.type === "agent_error" || event.type === "error") {
            throw new Error(event.message || event.error || "Agent runtime error");
          }

          const alert = toOperationalAlert(event);
          if (alert) {
            setOperationalAlert(alert);
          }

          setSourceHealth((previous) => updateSourceHealth(previous, event));

          const telemetry = toTelemetryEvent(event);
          if (telemetry) {
            setTimelineEvents((previous) => [...previous, telemetry]);
          }
        };

        while (true) {
          const { done, value } = await reader.read();
          if (done) break;

          buffer += decoder.decode(value, { stream: true });
          const parsed = parseSSEFrames(buffer);
          buffer = parsed.remainder;

          for (const event of parsed.events) {
            processEvent(event);
          }
        }

        if (buffer.trim()) {
          const parsed = parseSSEFrames(`${buffer}\n\n`);
          for (const event of parsed.events) {
            processEvent(event);
          }
        }

        const assistantMessage: Message = {
          id: generateId(),
          role: "assistant",
          content: fullContent,
          createdAt: new Date(),
          citations: newCitations,
          isVerified,
        };

        setMessages((previous) => [...previous, assistantMessage]);

        if (newCitations.length > 0) {
          setCitations((previous) => {
            const existingCitationKeys = new Set(
              previous.map((citation) => `${citation.provider}::${citation.dataset}::${citation.rowId}`)
            );
            const uniqueNew = newCitations
              .filter(
                (citation) =>
                  !existingCitationKeys.has(`${citation.provider}::${citation.dataset}::${citation.rowId}`)
              )
              .map((citation, index) => ({
                ...citation,
                id: previous.length + index + 1,
              }));
            return [...previous, ...uniqueNew];
          });
        }

        setConfidenceLabel(
          isVerified
            ? newCitations.length > 0
              ? "High (verified with evidence)"
              : "Medium (verified without citations)"
            : newCitations.length > 0
              ? "Medium (evidence present, not fully verified)"
              : "Low (limited evidence)"
        );

        setShowFollowUps(true);
      } catch (error) {
        console.error("Chat error:", error);
        emitReasoningEvent("evidence_check_complete", {
          verification: "Partial",
          failOpen: true,
        });

        const errorMessage: Message = {
          id: generateId(),
          role: "assistant",
          content:
            "I encountered an error while preparing the flight brief. Please retry or narrow the required data sources.",
          createdAt: new Date(),
          isVerified: false,
        };

        setMessages((previous) => [...previous, errorMessage]);
        setTimelineEvents((previous) => [
          ...previous,
          {
            id: generateId(),
            type: "agent_error",
            stage: "error",
            message: error instanceof Error ? error.message : "Unknown error",
            status: "error",
            timestamp: new Date().toISOString(),
          },
        ]);
        setRouteLabel("Error");
        setConfidenceLabel("Unavailable");
        setOperationalAlert({
          id: generateId(),
          severity: "critical",
          title: "Briefing Service Interrupted",
          message:
            "Brief generation was interrupted. Re-check weather hazards, NOTAM status, and crew legality before release.",
          timestamp: new Date().toISOString(),
        });
      } finally {
        setIsLoading(false);
        setStreamingContent("");
      }
    },
    [
      activeConversationId,
      messages,
      queryProfile,
      freshnessSlaMinutes,
      explainRetrieval,
      demoScenario,
      stopVoicePlayback,
      emitReasoningEvent,
      markEvidenceRetrieval,
    ]
  );

  const handleRunPreset = useCallback(
    (prompt: string) => {
      if (!prompt) return;
      void handleSendMessage(prompt);
    },
    [handleSendMessage]
  );

  const handleFollowUpSelect = useCallback(
    (suggestion: string) => {
      void handleSendMessage(suggestion);
    },
    [handleSendMessage]
  );

  const handleRetryLast = useCallback(() => {
    if (isLoading) return;
    const lastUserMessage = [...messages].reverse().find((message) => message.role === "user");
    if (!lastUserMessage?.content) return;
    void handleSendMessage(lastUserMessage.content);
  }, [isLoading, messages, handleSendMessage]);

  const preflightStatus = fabricPreflight?.overall_status || "warn";
  const preflightVariant =
    preflightStatus === "pass"
      ? "success"
      : preflightStatus === "warn"
        ? "warning"
        : "destructive";
  const preflightText = preflightLoading
    ? "Data path health: checking"
    : `Data path health: ${preflightStatus.toUpperCase()}`;

  return (
    <div className="flex h-full flex-col bg-transparent">
      <div className="flex min-h-0 flex-1">
      <Sidebar
        isCollapsed={sidebarCollapsed}
        onToggle={() => setSidebarCollapsed((previous) => !previous)}
        onSelectConversation={handleSelectConversation}
        activeConversationId={activeConversationId ?? undefined}
        onNewChat={handleNewChat}
        onRunPreset={handleRunPreset}
      />

      <div className="relative flex min-h-0 min-w-0 flex-1 flex-col">
        <div className="pointer-events-none absolute inset-0 flight-grid opacity-30" />

        <div className="relative z-10 border-b border-border bg-card/80 px-4 py-3">
          <div className="mx-auto flex max-w-5xl flex-wrap items-center justify-between gap-3">
            <div className="flex flex-wrap items-center gap-2">
              <Badge variant="outline">{routeLabel}</Badge>
            </div>

            <div className="flex flex-wrap items-center gap-2">
              <ToggleGroup
                value={queryProfile}
                onValueChange={(value) => setQueryProfile(value as QueryProfile)}
                options={[
                  { value: "pilot-brief", label: "Pilot Brief" },
                  { value: "ops-live", label: "Ops Live" },
                  { value: "compliance", label: "Compliance" },
                ]}
              />

              <Button
                size="sm"
                variant="ghost"
                onClick={() => setShowAdvanced((previous) => !previous)}
                className="h-7 gap-1.5 px-2 text-[11px]"
              >
                <Settings2 className="h-3.5 w-3.5" />
                {showAdvanced ? "Less" : "More"}
              </Button>
            </div>
          </div>

          <AnimatePresence initial={false}>
            {showAdvanced && (
              <motion.div
                initial={{ opacity: 0, height: 0 }}
                animate={{ opacity: 1, height: "auto" }}
                exit={{ opacity: 0, height: 0 }}
                transition={{ duration: 0.2 }}
                className="overflow-hidden"
              >
                <div className="mx-auto mt-3 max-w-5xl border-t border-border/50 pt-3">
                  <div className="flex flex-wrap items-center gap-2">
                    <ToggleGroup
                      value={voiceMode}
                      onValueChange={(value) => {
                        const next = value as VoiceMode;
                        setVoiceMode(next);
                        if (next === "off") {
                          stopVoicePlayback();
                        }
                      }}
                      options={[
                        { value: "off", label: "Voice Off" },
                        { value: "tr-TR", label: "Voice TR" },
                        { value: "en-US", label: "Voice EN" },
                      ]}
                    />

                    <ToggleGroup
                      value={String(freshnessSlaMinutes)}
                      onValueChange={(value) => setFreshnessSlaMinutes(Number(value))}
                      options={[
                        { value: "15", label: "SLA 15m" },
                        { value: "60", label: "SLA 60m" },
                        { value: "180", label: "SLA 180m" },
                      ]}
                    />

                    <ToggleGroup
                      value={demoScenario}
                      onValueChange={(value) => setDemoScenario(value as DemoScenario)}
                      options={[
                        { value: "none", label: "Scenario Off" },
                        { value: "weather-spike", label: "Weather Spike" },
                        { value: "runway-notam", label: "Runway NOTAM" },
                        { value: "ground-bottleneck", label: "Ground Bottleneck" },
                      ]}
                    />

                    <Button
                      size="sm"
                      variant={explainRetrieval ? "secondary" : "outline"}
                      onClick={() => setExplainRetrieval((previous) => !previous)}
                    >
                      Explainability {explainRetrieval ? "On" : "Off"}
                    </Button>

                    <Dialog>
                      <DialogTrigger asChild>
                        <Button size="sm" variant="outline">
                          Architecture View
                        </Button>
                      </DialogTrigger>
                      <DialogContent className="max-h-[85vh] max-w-4xl overflow-y-auto">
                        <DialogHeader>
                          <DialogTitle>Context-to-Evidence Architecture</DialogTitle>
                          <DialogDescription>
                            Live source status and retrieval rationale for each datastore used by the flight brief assistant.
                          </DialogDescription>
                        </DialogHeader>
                        <ArchitectureMap sourceHealth={sourceHealth} />
                      </DialogContent>
                    </Dialog>

                    <Badge variant={preflightVariant}>{preflightText}</Badge>
                    <Button
                      size="sm"
                      variant="ghost"
                      onClick={() => void fetchFabricPreflight()}
                      disabled={preflightLoading}
                      className="h-7 px-2 text-[11px]"
                    >
                      {preflightLoading ? "Checking..." : "Refresh data path"}
                    </Button>
                  </div>
                </div>
              </motion.div>
            )}
          </AnimatePresence>
        </div>

        <OperationalAlertBanner
          alert={operationalAlert}
          onDismiss={() => setOperationalAlert(null)}
        />

        <ChatThread
          messages={messages}
          isLoading={isLoading}
          streamingContent={streamingContent}
          onCitationClick={handleCitationClick}
          activeCitationId={activeCitationId}
          onSpeakMessage={(messageId, content) => {
            void speakMessage(messageId, content);
          }}
          speakingMessageId={speakingMessageId}
          voiceStatuses={voiceStatuses}
          voiceEnabled={voiceMode !== "off"}
          onSendMessage={(message) => {
            void handleSendMessage(message);
          }}
        />

        <FollowUpChips
          suggestions={ENHANCED_FOLLOW_UP_SUGGESTIONS}
          onSelect={handleFollowUpSelect}
          isVisible={showFollowUps && !isLoading && messages.length > 0}
        />

        <MessageComposer
          onSubmit={(message) => {
            void handleSendMessage(message);
          }}
          isLoading={isLoading}
          reasoningEvents={reasoningEvents}
        />

        {(isLoading || timelineEvents.length > 0) && (
          <TimelinePanel
            events={timelineEvents}
            sourceHealth={sourceHealth}
            sourceSnapshots={sourceSnapshots}
            isLoading={isLoading}
            onRetryLast={handleRetryLast}
            docked
          />
        )}
      </div>

      <SourcesPanel
        isCollapsed={sourcesPanelCollapsed}
        onToggle={() => setSourcesPanelCollapsed((previous) => !previous)}
        citations={citations}
        activeCitationId={activeCitationId}
        onCitationClick={handleCitationClick}
        sourceHealth={sourceHealth}
        route={routeLabel}
        confidenceLabel={confidenceLabel}
      />
      </div>
    </div>
  );
}

function OperationalAlertBanner({
  alert,
  onDismiss,
}: {
  alert: OperationalAlert | null;
  onDismiss: () => void;
}) {
  const reducedMotion = useReducedMotion();
  const severity = alert?.severity || "advisory";
  const tickerMessage = alert ? toPilotRiskTickerMessage(alert) : "";
  const marquee = [tickerMessage, tickerMessage];

  return (
    <AnimatePresence>
      {alert && (
        <motion.section
          initial={reducedMotion ? false : { opacity: 0, y: -8 }}
          animate={{ opacity: 1, y: 0 }}
          exit={reducedMotion ? { opacity: 1 } : { opacity: 0, y: -6 }}
          transition={{ duration: reducedMotion ? 0 : 0.24 }}
          role="alert"
          aria-live={severity === "critical" ? "assertive" : "polite"}
          className={cn(
            "relative z-20 border-b px-4 py-2",
            severity === "critical" && "border-orange-500/55 bg-orange-500/16",
            severity === "warning" && "border-amber-500/50 bg-amber-500/14",
            severity === "advisory" && "border-primary/40 bg-primary/10"
          )}
        >
          <div className="mx-auto flex max-w-5xl items-center gap-3">
            <div className="flex items-center gap-2 whitespace-nowrap text-xs font-semibold uppercase tracking-[0.1em]">
              <AlertTriangle className="h-4 w-4" />
              {severity === "critical" ? "Critical Flight Risk Advisory" : "Flight Risk Advisory"}
            </div>

            <div className="min-w-0 flex-1 overflow-hidden">
              {reducedMotion ? (
                <p className="truncate text-sm">{tickerMessage}</p>
              ) : (
                <div className="alert-marquee-track">
                  {marquee.map((text, index) => (
                    <span key={`${alert.id}-${index}`} className="alert-marquee-segment">
                      {text}
                    </span>
                  ))}
                </div>
              )}
            </div>

            <div className="hidden text-[11px] text-muted-foreground md:block">
              {new Date(alert.timestamp).toLocaleTimeString("en-US", {
                hour12: false,
                hour: "2-digit",
                minute: "2-digit",
                second: "2-digit",
                timeZone: "UTC",
              })}{" "}
              UTC
            </div>

            <Button
              size="icon-sm"
              variant="ghost"
              onClick={onDismiss}
              className="h-7 w-7 shrink-0"
              aria-label="Dismiss advisory"
            >
              <X className="h-4 w-4" />
            </Button>
          </div>
        </motion.section>
      )}
    </AnimatePresence>
  );
}

function toPilotRiskTickerMessage(alert: OperationalAlert): string {
  const message = (alert.message || "").trim();
  const infraSignalPattern =
    /(runtime|backend|proxy|stream|deployment|kudu|telemetry|fabric|fallback|source mode|service)/i;

  if (!message) {
    return alert.severity === "critical"
      ? "Critical risk update: verify weather cells, runway status, alternates, and crew legality before dispatch."
      : "Risk update: confirm weather, NOTAM, and stand constraints before release.";
  }

  if (!infraSignalPattern.test(message)) {
    return message;
  }

  return alert.severity === "critical"
    ? "Critical risk update: briefing feed degraded. Verify weather cells, runway status, alternates, and crew legality before dispatch."
    : "Risk update: one or more briefing feeds are degraded. Cross-check NOTAM, weather, and slot constraints before release.";
}
