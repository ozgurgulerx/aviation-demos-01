"use client";

import { useState, useCallback, useEffect } from "react";
import { Sidebar } from "@/components/layout/sidebar";
import { SourcesPanel } from "@/components/layout/sources-panel";
import { ChatThread } from "@/components/chat/chat-thread";
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
import { generateId } from "@/lib/utils";
import { parseSSEFrames, toTelemetryEvent, updateSourceHealth } from "@/lib/chat";
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
} from "@/types";

type RetrievalMode = "code-rag" | "foundry-iq";
type QueryProfile = "pilot-brief" | "ops-live" | "compliance";
type DemoScenario = "none" | "weather-spike" | "runway-notam" | "ground-bottleneck";

function createInitialSourceHealth(): SourceHealthStatus[] {
  return DATA_SOURCE_BLUEPRINT.map((source) => ({
    source: source.id,
    status: "idle",
    rowCount: 0,
    mode: "unknown",
  }));
}

export default function ChatPage() {
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false);
  const [sourcesPanelCollapsed, setSourcesPanelCollapsed] = useState(false);

  const [retrievalMode, setRetrievalMode] = useState<RetrievalMode>("code-rag");
  const [queryProfile, setQueryProfile] = useState<QueryProfile>("pilot-brief");
  const [explainRetrieval, setExplainRetrieval] = useState(true);
  const [freshnessSlaMinutes, setFreshnessSlaMinutes] = useState<number>(60);
  const [demoScenario, setDemoScenario] = useState<DemoScenario>("none");

  const [requiredSources, setRequiredSources] = useState<string[]>([]);

  const [activeConversationId, setActiveConversationId] = useState<string | null>(
    SAMPLE_CONVERSATIONS[0]?.id || null
  );
  const [messages, setMessages] = useState<Message[]>(
    SAMPLE_CONVERSATIONS[0]?.messages || []
  );
  const [citations, setCitations] = useState<Citation[]>(
    SAMPLE_CONVERSATIONS[0]?.messages
      .flatMap((message) => message.citations || [])
      .filter((citation, index, array) => array.findIndex((item) => item.id === citation.id) === index) || []
  );

  const [activeCitationId, setActiveCitationId] = useState<number | null>(null);

  const [isLoading, setIsLoading] = useState(false);
  const [streamingContent, setStreamingContent] = useState("");
  const [timelineEvents, setTimelineEvents] = useState<TelemetryEvent[]>([]);
  const [sourceHealth, setSourceHealth] = useState<SourceHealthStatus[]>(
    createInitialSourceHealth()
  );

  const [showFollowUps, setShowFollowUps] = useState(true);
  const [routeLabel, setRouteLabel] = useState<string>("Pending");
  const [confidenceLabel, setConfidenceLabel] = useState<string>("Awaiting run");
  const [fabricPreflight, setFabricPreflight] = useState<FabricPreflightStatus | null>(null);
  const [preflightLoading, setPreflightLoading] = useState(false);

  const handleNewChat = useCallback(() => {
    setActiveConversationId(null);
    setMessages([]);
    setCitations([]);
    setActiveCitationId(null);
    setShowFollowUps(false);
    setStreamingContent("");
    setTimelineEvents([]);
    setSourceHealth(createInitialSourceHealth());
    setRouteLabel("Pending");
    setConfidenceLabel("Awaiting run");
  }, []);

  const handleSelectConversation = useCallback((id: string) => {
    const conversation = SAMPLE_CONVERSATIONS.find((item) => item.id === id);
    if (!conversation) return;

    setActiveConversationId(id);
    setMessages(conversation.messages);
    setCitations(
      conversation.messages
        .flatMap((message) => message.citations || [])
        .filter((citation, index, array) => array.findIndex((item) => item.id === citation.id) === index)
    );
    setActiveCitationId(null);
    setShowFollowUps(true);
  }, []);

  const handleCitationClick = useCallback((id: number) => {
    setActiveCitationId((previous) => (previous === id ? null : id));
  }, []);

  const toggleRequiredSource = useCallback((sourceId: string) => {
    setRequiredSources((previous) =>
      previous.includes(sourceId)
        ? previous.filter((item) => item !== sourceId)
        : [...previous, sourceId]
    );
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
      setShowFollowUps(false);
      setSourceHealth(createInitialSourceHealth());
      setRouteLabel("Running");
      setConfidenceLabel("Calculating");

      try {
        const response = await fetch("/api/chat", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            messages: [...messages, userMessage].map((message) => ({
              role: message.role,
              content: message.content,
            })),
            retrievalMode,
            conversationId,
            queryProfile,
            requiredSources,
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
          if ((event.type === "agent_update" || event.type === "text") && event.content) {
            fullContent += event.content;
            setStreamingContent(fullContent);
          }

          if (event.type === "citations" && event.citations) {
            newCitations = event.citations;
          }

          if (event.type === "agent_done" || event.type === "done") {
            isVerified = !!event.isVerified;
            setRouteLabel(event.route || "ORCHESTRATED");
          }

          if (event.type === "agent_error" || event.type === "error") {
            throw new Error(event.message || event.error || "Agent runtime error");
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

        const errorMessage: Message = {
          id: generateId(),
          role: "assistant",
          content:
            "I encountered an error while preparing the pilot brief. Please retry or narrow the required data sources.",
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
      } finally {
        setIsLoading(false);
        setStreamingContent("");
      }
    },
    [
      activeConversationId,
      messages,
      retrievalMode,
      queryProfile,
      requiredSources,
      freshnessSlaMinutes,
      explainRetrieval,
      demoScenario,
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
    ? "Fabric preflight: checking"
    : `Fabric preflight: ${preflightStatus.toUpperCase()}`;

  return (
    <div className="flex h-full bg-transparent">
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
              <span className="mission-chip">Pilot Brief Mode</span>
              <Badge variant="outline">{routeLabel}</Badge>
              <Badge variant={isLoading ? "warning" : "success"}>
                {isLoading ? "Telemetry streaming" : "Idle"}
              </Badge>
              <Badge variant={preflightVariant}>{preflightText}</Badge>
              <Badge variant={fabricPreflight?.live_path_available ? "success" : "outline"}>
                {fabricPreflight?.live_path_available ? "Live Fabric path ready" : "Fallback path available"}
              </Badge>
              <Button
                size="sm"
                variant="ghost"
                onClick={() => void fetchFabricPreflight()}
                disabled={preflightLoading}
                className="h-7 px-2 text-[11px]"
              >
                {preflightLoading ? "Checking..." : "Refresh preflight"}
              </Button>
            </div>

            <div className="flex flex-wrap items-center gap-2">
              <ToggleGroup
                value={retrievalMode}
                onValueChange={(value) => setRetrievalMode(value as RetrievalMode)}
                options={[
                  { value: "code-rag", label: "Code RAG" },
                  { value: "foundry-iq", label: "Foundry IQ" },
                ]}
              />

              <ToggleGroup
                value={queryProfile}
                onValueChange={(value) => setQueryProfile(value as QueryProfile)}
                options={[
                  { value: "pilot-brief", label: "Pilot Brief" },
                  { value: "ops-live", label: "Ops Live" },
                  { value: "compliance", label: "Compliance" },
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
                    <DialogTitle>Context-to-Datastore Architecture</DialogTitle>
                    <DialogDescription>
                      Live source status and retrieval rationale for each datastore used by the pilot brief agent.
                    </DialogDescription>
                  </DialogHeader>
                  <ArchitectureMap sourceHealth={sourceHealth} />
                </DialogContent>
              </Dialog>
            </div>
          </div>

          <div className="mx-auto mt-3 flex max-w-5xl flex-wrap gap-2">
            {DATA_SOURCE_BLUEPRINT.map((source) => {
              const selected = requiredSources.includes(source.id);
              return (
                <Button
                  key={source.id}
                  size="sm"
                  variant={selected ? "secondary" : "outline"}
                  onClick={() => toggleRequiredSource(source.id)}
                  className="h-7 rounded-full px-3 text-[11px]"
                >
                  {source.id}
                </Button>
              );
            })}
          </div>
          {fabricPreflight && (
            <div className="mx-auto mt-2 max-w-5xl text-xs text-muted-foreground">
              Preflight timestamp: {fabricPreflight.timestamp || "n/a"} · checks:{" "}
              {fabricPreflight.checks?.length || 0}
              {fabricPreflight.error ? ` · error: ${fabricPreflight.error}` : ""}
            </div>
          )}
        </div>

        <ChatThread
          messages={messages}
          isLoading={isLoading}
          streamingContent={streamingContent}
          timelineEvents={timelineEvents}
          sourceHealth={sourceHealth}
          onCitationClick={handleCitationClick}
          activeCitationId={activeCitationId}
          onRetryLast={handleRetryLast}
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
        />
      </div>

      <SourcesPanel
        isCollapsed={sourcesPanelCollapsed}
        onToggle={() => setSourcesPanelCollapsed((previous) => !previous)}
        citations={citations}
        activeCitationId={activeCitationId}
        onCitationClick={handleCitationClick}
        sourceHealth={sourceHealth}
        route={routeLabel}
        isLoading={isLoading}
        confidenceLabel={confidenceLabel}
      />
    </div>
  );
}
