"use client";

import { useEffect, useState } from "react";
import { motion, AnimatePresence, useReducedMotion } from "framer-motion";
import Image from "next/image";
import {
  Loader2,
  CheckCircle2,
  AlertCircle,
  Lock,
  RotateCcw,
  ChevronDown,
  ChevronUp,
  Bot,
} from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import type {
  TelemetryEvent,
  SourceHealthStatus,
  SourceResultSnapshot,
} from "@/types";
import { getDatastoreVisual } from "@/lib/datastore";
import { motionTokens, subtlePulse } from "@/lib/motion";

interface TimelinePanelProps {
  events: TelemetryEvent[];
  sourceHealth: SourceHealthStatus[];
  isLoading: boolean;
  onRetryLast?: () => void;
  sourceSnapshots?: Record<string, SourceResultSnapshot>;
  docked?: boolean;
}

export function TimelinePanel({
  events,
  sourceHealth,
  isLoading,
  onRetryLast,
  sourceSnapshots = {},
  docked = false,
}: TimelinePanelProps) {
  const reducedMotion = useReducedMotion();
  const latestEvents = events.slice(-12);
  const hasError = latestEvents.some((event) => event.status === "error");
  const [expanded, setExpanded] = useState(false);
  const [selectedSource, setSelectedSource] = useState<string | null>(null);

  useEffect(() => {
    if (isLoading) {
      setExpanded(true);
      return;
    }
    if (events.length > 0) {
      setExpanded(false);
    }
  }, [isLoading, events.length]);

  const selectedSnapshot = selectedSource ? sourceSnapshots[selectedSource] : null;
  const latestEventMessage = latestEvents.length > 0 ? latestEvents[latestEvents.length - 1]?.message : null;

  return (
    <section className={docked ? "border-t border-border bg-card/85 px-4 py-3" : "surface-panel rounded-2xl p-4"}>
      <div className={`mx-auto ${docked ? "max-w-5xl" : ""}`}>
        <div className="mb-3 flex flex-wrap items-center justify-between gap-2">
          <div>
            <p className="font-display text-sm font-semibold">Source Activity</p>
            <p className="text-xs text-muted-foreground">
              {isLoading
                ? "Live stage updates while the orchestrator retrieves evidence."
                : latestEventMessage || "Submit a request to start retrieval telemetry."}
            </p>
          </div>
          <div className="flex items-center gap-2">
            <Badge variant={isLoading ? "warning" : "success"}>{isLoading ? "Running" : "Ready"}</Badge>
            <Badge variant="outline" className="font-mono">
              {events.length} events
            </Badge>
            <Button
              size="sm"
              variant="outline"
              className="h-7 gap-1.5 px-2 text-[11px]"
              onClick={() => setExpanded((previous) => !previous)}
            >
              {expanded ? <ChevronUp className="h-3.5 w-3.5" /> : <ChevronDown className="h-3.5 w-3.5" />}
              {expanded ? "Collapse" : "Expand"}
            </Button>
          </div>
        </div>

        <div className="mb-3 flex flex-wrap gap-2">
          {sourceHealth.map((source) => {
            const visual = getDatastoreVisual(source.source);
            const pulse = subtlePulse(source.status === "querying", !!reducedMotion);
            const sourceSnapshot = sourceSnapshots[source.source];
            const hasPreview = !!sourceSnapshot && sourceSnapshot.rowsPreview.length > 0;
            return (
              <motion.div key={source.source} animate={pulse} className="rounded-full">
                <Badge
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
                  {source.mode === "live" && (
                    <span className="rounded border border-emerald-500/35 bg-emerald-500/15 px-1 text-[9px] font-semibold uppercase text-emerald-700 dark:text-emerald-300">
                      Live
                    </span>
                  )}
                  {source.mode === "fallback" && (
                    <span className="rounded border border-orange-500/35 bg-orange-500/15 px-1 text-[9px] font-semibold uppercase text-orange-700 dark:text-orange-300">
                      Fallback
                    </span>
                  )}
                  <AnimatePresence mode="popLayout" initial={false}>
                    {source.rowCount > 0 && (
                      <motion.button
                        key={`${source.source}-${source.rowCount}`}
                        type="button"
                        initial={reducedMotion ? false : { opacity: 0, y: 6 }}
                        animate={{ opacity: 1, y: 0 }}
                        exit={reducedMotion ? { opacity: 1, y: 0 } : { opacity: 0, y: -4 }}
                        transition={{ duration: reducedMotion ? 0 : motionTokens.micro }}
                        className="rounded border border-border/70 bg-background/70 px-1.5 font-mono text-[10px] hover:bg-background disabled:cursor-not-allowed disabled:opacity-55"
                        onClick={() => {
                          if (hasPreview) {
                            setSelectedSource(source.source);
                          }
                        }}
                        disabled={!hasPreview}
                        title={hasPreview ? "View retrieved rows" : "No row preview available for this source call"}
                      >
                        {source.rowCount} rows
                      </motion.button>
                    )}
                  </AnimatePresence>
                </Badge>
              </motion.div>
            );
          })}
        </div>
        <p className="mb-3 text-[11px] text-muted-foreground">
          Click a row-count chip to inspect retrieved records for that datastore.
        </p>

        <AnimatePresence initial={false}>
          {expanded && (
            <motion.div
              initial={reducedMotion ? false : { opacity: 0, y: -4 }}
              animate={{ opacity: 1, y: 0 }}
              exit={reducedMotion ? { opacity: 1, y: 0 } : { opacity: 0, y: -4 }}
              transition={{ duration: reducedMotion ? 0 : motionTokens.state, ease: motionTokens.easeOut }}
            >
              <LiveOpsPulse sourceHealth={sourceHealth} />

              <div className="max-h-52 space-y-2 overflow-y-auto pr-1">
                {latestEvents.length === 0 ? (
                  <p className="text-xs text-muted-foreground">
                    No stages yet. Submit a request to watch intent mapping and evidence retrieval.
                  </p>
                ) : (
                  <AnimatePresence initial={false}>
                    {latestEvents.map((event) => {
                      const isToolEvent = event.type === "tool_call" || event.type === "tool_result";
                      const isErrorEvent = event.status === "error";
                      return (
                        <motion.div
                          key={event.id}
                          layout
                          initial={
                            reducedMotion
                              ? false
                              : {
                                  opacity: 0,
                                  x: isToolEvent ? -12 : 0,
                                  y: isToolEvent ? 0 : 8,
                                }
                          }
                          animate={{ opacity: 1, x: 0, y: 0 }}
                          exit={reducedMotion ? { opacity: 1 } : { opacity: 0, y: -4 }}
                          transition={{ duration: reducedMotion ? 0 : motionTokens.state, ease: motionTokens.easeOut }}
                          className={`relative flex items-start gap-2 rounded-lg border px-3 py-2 text-xs ${
                            isErrorEvent
                              ? "border-destructive/40 bg-destructive/[0.05]"
                              : isToolEvent
                                ? "border-primary/25 bg-primary/[0.04]"
                                : "border-border bg-background"
                          }`}
                        >
                          {isErrorEvent && !reducedMotion && (
                            <motion.span
                              className="pointer-events-none absolute inset-0 rounded-lg border border-destructive/60"
                              initial={{ opacity: 0.9 }}
                              animate={{ opacity: [0.9, 0.2, 0] }}
                              transition={{ duration: 0.9, ease: motionTokens.easeOut }}
                            />
                          )}

                          <StatusIcon status={event.status} />
                          <div className="min-w-0 flex-1">
                            <div className="mb-0.5 flex items-center gap-1.5">
                              {isToolEvent && <EventChip type={event.type} />}
                              <p className="font-medium text-foreground">{event.message}</p>
                            </div>
                            <p className="font-mono text-[11px] text-muted-foreground">
                              {event.stage} ·{" "}
                              {new Date(event.timestamp).toLocaleTimeString("en-US", {
                                hour12: false,
                                hour: "2-digit",
                                minute: "2-digit",
                                second: "2-digit",
                                timeZone: "UTC",
                              })}{" "}
                              UTC
                            </p>
                          </div>
                          {event.durationMs && (
                            <span className="font-mono text-[11px] text-muted-foreground">{event.durationMs}ms</span>
                          )}
                        </motion.div>
                      );
                    })}
                  </AnimatePresence>
                )}
              </div>
            </motion.div>
          )}
        </AnimatePresence>

        <AnimatePresence>
          {hasError && onRetryLast && (
            <motion.div
              initial={reducedMotion ? false : { opacity: 0, y: 8 }}
              animate={{ opacity: 1, y: 0 }}
              exit={reducedMotion ? { opacity: 1, y: 0 } : { opacity: 0, y: -4 }}
              transition={{ duration: reducedMotion ? 0 : motionTokens.state, ease: motionTokens.easeOut }}
              className="mt-3 flex items-center justify-between rounded-lg border border-destructive/30 bg-destructive/[0.06] px-3 py-2"
            >
              <div className="flex items-center gap-2 text-xs text-muted-foreground">
                <Lock className="h-3.5 w-3.5 text-destructive" />
                Retrieval failed for one or more stages. Retry with current constraints.
              </div>
              <Button size="sm" variant="outline" onClick={onRetryLast} className="h-7 gap-1.5 text-xs">
                <RotateCcw className="h-3.5 w-3.5" />
                Retry
              </Button>
            </motion.div>
          )}
        </AnimatePresence>
      </div>

      <Dialog
        open={!!selectedSnapshot}
        onOpenChange={(open) => {
          if (!open) {
            setSelectedSource(null);
          }
        }}
      >
        <DialogContent className="max-h-[82vh] max-w-5xl overflow-y-auto">
          <DialogHeader>
            <DialogTitle>{selectedSnapshot?.source || "Source"} retrieved rows</DialogTitle>
            <DialogDescription>
              {selectedSnapshot?.rowCount || 0} rows returned at{" "}
              {selectedSnapshot?.timestamp
                ? new Date(selectedSnapshot.timestamp).toLocaleTimeString("en-US", {
                    hour12: false,
                    hour: "2-digit",
                    minute: "2-digit",
                    second: "2-digit",
                    timeZone: "UTC",
                  }) + " UTC"
                : "unknown time"}
            </DialogDescription>
          </DialogHeader>

          {selectedSnapshot && selectedSnapshot.rowsPreview.length > 0 ? (
            <div className="space-y-4">
              <div className="overflow-x-auto rounded-lg border border-border">
                <table className="min-w-full border-collapse text-left text-xs">
                  <thead className="bg-muted/70">
                    <tr>
                      {selectedSnapshot.columns.map((column) => (
                        <th key={column} className="border-b border-border px-3 py-2 font-semibold text-foreground">
                          {column}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {selectedSnapshot.rowsPreview.map((row, rowIndex) => (
                      <tr key={`${selectedSnapshot.eventId || selectedSnapshot.source}-${rowIndex}`}>
                        {selectedSnapshot.columns.map((column) => (
                          <td key={`${rowIndex}-${column}`} className="border-b border-border/60 px-3 py-2 align-top">
                            {String(row[column] ?? "")}
                          </td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>

              {selectedSnapshot.rowsTruncated && (
                <p className="text-xs text-muted-foreground">
                  Preview truncated to the first {selectedSnapshot.rowsPreview.length} rows.
                </p>
              )}

              <div className="rounded-lg border border-border bg-background p-3">
                <p className="mb-2 text-xs font-semibold">Raw preview payload</p>
                <pre className="max-h-64 overflow-auto text-[11px] leading-relaxed">
                  {JSON.stringify(selectedSnapshot.rowsPreview, null, 2)}
                </pre>
              </div>
            </div>
          ) : (
            <p className="text-sm text-muted-foreground">
              This source completed successfully but no preview rows were captured in the stream payload.
            </p>
          )}
        </DialogContent>
      </Dialog>
    </section>
  );
}

function EventChip({ type }: { type: TelemetryEvent["type"] }) {
  if (type === "tool_call") {
    return (
      <span className="inline-flex rounded-full border border-primary/30 bg-primary/10 px-2 py-0.5 text-[10px] font-semibold uppercase tracking-[0.08em] text-primary">
        Tool call
      </span>
    );
  }

  if (type === "tool_result") {
    return (
      <span className="inline-flex rounded-full border border-emerald-500/30 bg-emerald-500/10 px-2 py-0.5 text-[10px] font-semibold uppercase tracking-[0.08em] text-emerald-700 dark:text-emerald-300">
        Tool result
      </span>
    );
  }

  return null;
}

function LiveOpsPulse({ sourceHealth }: { sourceHealth: SourceHealthStatus[] }) {
  const kql = sourceHealth.find((source) => source.source === "KQL");
  const graph = sourceHealth.find((source) => source.source === "GRAPH");
  const live = sourceHealth.filter((source) => source.mode === "live").length;
  const fallback = sourceHealth.filter((source) => source.mode === "fallback").length;
  const latestUpdate = sourceHealth
    .map((source) => source.updatedAt)
    .filter(Boolean)
    .sort()
    .pop();

  return (
    <div className="mb-3 grid gap-2 rounded-xl border border-border bg-card p-3 text-xs md:grid-cols-4">
      <div>
        <p className="text-muted-foreground">Live operations pulse</p>
        <p className="font-semibold text-foreground">{kql?.rowCount ?? 0} KQL rows</p>
      </div>
      <div>
        <p className="text-muted-foreground">Impact graph pulse</p>
        <p className="font-semibold text-foreground">{graph?.rowCount ?? 0} graph rows</p>
      </div>
      <div>
        <p className="text-muted-foreground">Data path</p>
        <p className="font-semibold text-foreground">
          {live} live / {fallback} fallback
        </p>
      </div>
      <div>
        <p className="text-muted-foreground">Last source update</p>
        <p className="font-semibold text-foreground">
          {latestUpdate
            ? new Date(latestUpdate).toLocaleTimeString("en-US", {
                hour12: false,
                hour: "2-digit",
                minute: "2-digit",
                second: "2-digit",
                timeZone: "UTC",
              }) + " UTC"
            : "N/A"}
        </p>
      </div>
    </div>
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
