"use client";

import { useRef, useEffect, useState } from "react";
import { motion, AnimatePresence } from "framer-motion";
import Image from "next/image";
import {
  Loader2,
  Sparkles,
  CheckCircle2,
  AlertCircle,
  Radar,
  ShieldAlert,
  Workflow,
  Wrench,
  BookCheck,
  Bot,
} from "lucide-react";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Message } from "./message";
import { QUERY_CATEGORIES, type QueryType } from "@/data/seed";
import type { Message as MessageType, TelemetryEvent, SourceHealthStatus } from "@/types";
import { getDatastoreVisual } from "@/lib/datastore";

interface ChatThreadProps {
  messages: MessageType[];
  isLoading: boolean;
  streamingContent?: string;
  timelineEvents?: TelemetryEvent[];
  sourceHealth?: SourceHealthStatus[];
  onCitationClick?: (id: number) => void;
  activeCitationId?: number | null;
  onSendMessage?: (message: string) => void;
}

const iconMap: Record<QueryType, React.ElementType> = {
  "ops-live": Radar,
  safety: ShieldAlert,
  network: Workflow,
  maintenance: Wrench,
  compliance: BookCheck,
};

const toneMap: Record<string, string> = {
  blue: "border-primary/25 bg-primary/5 text-primary",
  orange: "border-orange-500/30 bg-orange-500/10 text-orange-700 dark:text-orange-300",
  teal: "border-teal-500/30 bg-teal-500/10 text-teal-700 dark:text-teal-300",
};

export function ChatThread({
  messages,
  isLoading,
  streamingContent,
  timelineEvents = [],
  sourceHealth = [],
  onCitationClick,
  activeCitationId,
  onSendMessage,
}: ChatThreadProps) {
  const scrollRef = useRef<HTMLDivElement>(null);
  const bottomRef = useRef<HTMLDivElement>(null);
  const [expandedCategory, setExpandedCategory] = useState<QueryType | null>(null);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages, streamingContent, timelineEvents.length]);

  return (
    <ScrollArea className="flex-1 min-h-0" ref={scrollRef}>
      <div className="mx-auto max-w-5xl space-y-4 px-4 py-5">
        <TimelinePanel events={timelineEvents} sourceHealth={sourceHealth} isLoading={isLoading} />

        {messages.length === 0 ? (
          <div className="space-y-3 rounded-2xl border border-border bg-card p-5">
            <div className="mb-2">
              <h2 className="font-display text-2xl font-semibold text-brand-gradient">
                Operational Brief Workbench
              </h2>
              <p className="mt-1 text-sm text-muted-foreground">
                Ask for live operations, safety, dependency, maintenance, or compliance intelligence.
              </p>
            </div>

            {QUERY_CATEGORIES.map((category) => {
              const Icon = iconMap[category.id] || Radar;
              const tone = toneMap[category.tone] || toneMap.blue;
              const isExpanded = expandedCategory === category.id;

              return (
                <div key={category.id} className="rounded-xl border border-border bg-background/80">
                  <button
                    onClick={() => setExpandedCategory(isExpanded ? null : category.id)}
                    className="flex w-full items-center justify-between gap-3 px-4 py-3 text-left"
                  >
                    <div className="flex items-center gap-3">
                      <div className={`rounded-lg border p-2 ${tone}`}>
                        <Icon className="h-4 w-4" />
                      </div>
                      <div>
                        <p className="text-sm font-semibold">{category.title}</p>
                        <p className="text-xs text-muted-foreground">{category.description}</p>
                      </div>
                    </div>
                    <Badge variant="outline" className="text-[10px] uppercase tracking-[0.1em]">
                      {category.id}
                    </Badge>
                  </button>

                  <AnimatePresence>
                    {isExpanded && (
                      <motion.div
                        initial={{ opacity: 0, height: 0 }}
                        animate={{ opacity: 1, height: "auto" }}
                        exit={{ opacity: 0, height: 0 }}
                        className="space-y-2 overflow-hidden px-4 pb-4"
                      >
                        {category.examples.map((example) => (
                          <Button
                            key={example}
                            variant="outline"
                            size="sm"
                            className="h-auto w-full justify-start py-2 text-left text-xs"
                            onClick={() => onSendMessage?.(example)}
                          >
                            {example}
                          </Button>
                        ))}
                      </motion.div>
                    )}
                  </AnimatePresence>
                </div>
              );
            })}
          </div>
        ) : (
          <AnimatePresence mode="popLayout">
            {messages.map((message) => (
              <Message
                key={message.id}
                message={message}
                onCitationClick={onCitationClick}
                activeCitationId={activeCitationId}
              />
            ))}

            {isLoading && streamingContent && (
              <motion.div
                initial={{ opacity: 0, y: 10 }}
                animate={{ opacity: 1, y: 0 }}
                className="flex gap-3 py-4"
              >
                <div className="flex h-8 w-8 items-center justify-center rounded-full bg-primary/10">
                  <Sparkles className="h-4 w-4 text-primary animate-pulse" />
                </div>
                <div className="max-w-[88%] flex-1 rounded-xl border border-border bg-card px-4 py-3">
                  <div className="markdown-content text-sm">
                    {streamingContent}
                    <span className="ml-0.5 inline-block h-4 w-2 animate-pulse bg-primary/40" />
                  </div>
                </div>
              </motion.div>
            )}

            {isLoading && !streamingContent && (
              <motion.div
                initial={{ opacity: 0, y: 10 }}
                animate={{ opacity: 1, y: 0 }}
                className="flex gap-3 py-4"
              >
                <div className="flex h-8 w-8 items-center justify-center rounded-full bg-primary/10">
                  <Loader2 className="h-4 w-4 animate-spin text-primary" />
                </div>
                <div className="max-w-[88%] flex-1 rounded-xl border border-border bg-card px-4 py-3 text-sm text-muted-foreground">
                  Waiting for telemetry and synthesis updates...
                </div>
              </motion.div>
            )}
          </AnimatePresence>
        )}

        <div ref={bottomRef} className="h-1" />
      </div>
    </ScrollArea>
  );
}

function TimelinePanel({
  events,
  sourceHealth,
  isLoading,
}: {
  events: TelemetryEvent[];
  sourceHealth: SourceHealthStatus[];
  isLoading: boolean;
}) {
  const latestEvents = events.slice(-10);

  return (
    <section className="surface-panel rounded-2xl p-4">
      <div className="mb-3 flex flex-wrap items-center justify-between gap-2">
        <div>
          <p className="font-display text-sm font-semibold">Retrieval Orchestration Timeline</p>
          <p className="text-xs text-muted-foreground">
            Live SSE telemetry from agent runtime, tools, and source calls.
          </p>
        </div>
        <div className="flex items-center gap-2">
          <Badge variant={isLoading ? "warning" : "success"}>
            {isLoading ? "Running" : "Ready"}
          </Badge>
          <Badge variant="outline" className="font-mono">
            {events.length} events
          </Badge>
        </div>
      </div>

      <div className="mb-3 flex flex-wrap gap-2">
        {sourceHealth.map((source) => {
          const visual = getDatastoreVisual(source.source);
          return (
            <Badge
              key={source.source}
              variant={
                source.status === "ready"
                  ? "success"
                  : source.status === "querying"
                    ? "warning"
                    : source.status === "error"
                      ? "destructive"
                      : "outline"
              }
              className="gap-1.5"
            >
              <span className="h-3.5 w-3.5 overflow-hidden rounded-sm bg-white/80">
                <Image
                  src={visual.iconSrc}
                  alt={visual.shortLabel}
                  width={14}
                  height={14}
                  className="h-full w-full object-contain"
                />
              </span>
              <span>{visual.shortLabel}</span>
              {source.rowCount > 0 && (
                <span className="font-mono text-[10px]">{source.rowCount} rows</span>
              )}
            </Badge>
          );
        })}
      </div>

      <div className="max-h-44 space-y-2 overflow-y-auto pr-1">
        {latestEvents.length === 0 ? (
          <p className="text-xs text-muted-foreground">
            No events yet. Submit a prompt to watch retrieval planning and source execution.
          </p>
        ) : (
          latestEvents.map((event) => (
            <div
              key={event.id}
              className="flex items-start gap-2 rounded-lg border border-border bg-background px-3 py-2 text-xs"
            >
              <StatusIcon status={event.status} />
              <div className="min-w-0 flex-1">
                <p className="font-medium text-foreground">{event.message}</p>
                <p className="font-mono text-[11px] text-muted-foreground">
                  {event.stage} · {new Date(event.timestamp).toLocaleTimeString("en-US", {
                    hour12: false,
                    hour: "2-digit",
                    minute: "2-digit",
                    second: "2-digit",
                    timeZone: "UTC",
                  })} UTC
                </p>
              </div>
              {event.durationMs && (
                <span className="font-mono text-[11px] text-muted-foreground">
                  {event.durationMs}ms
                </span>
              )}
            </div>
          ))
        )}
      </div>
    </section>
  );
}

function StatusIcon({ status }: { status: TelemetryEvent["status"] }) {
  if (status === "completed") {
    return <CheckCircle2 className="mt-0.5 h-3.5 w-3.5 shrink-0 text-emerald-600 dark:text-emerald-400" />;
  }

  if (status === "error") {
    return <AlertCircle className="mt-0.5 h-3.5 w-3.5 shrink-0 text-destructive" />;
  }

  if (status === "running") {
    return <Loader2 className="mt-0.5 h-3.5 w-3.5 shrink-0 animate-spin text-primary" />;
  }

  return <Bot className="mt-0.5 h-3.5 w-3.5 shrink-0 text-muted-foreground" />;
}
