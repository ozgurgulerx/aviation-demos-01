"use client";

import { useMemo, useState } from "react";
import { AnimatePresence, motion, useReducedMotion } from "framer-motion";
import {
  CheckCircle2,
  ChevronDown,
  ChevronUp,
  Circle,
  Loader2,
  ShieldAlert,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";
import type { ReasoningSseEvent } from "@/types";
import { getLatestReasoningEvent, getReasoningStageIndex } from "@/lib/reasoning-stream";

interface ReasoningStripProps {
  events: ReasoningSseEvent[];
  isLoading: boolean;
  onOpenQueries?: () => void;
}

interface StageDefinition {
  key: ReasoningSseEvent["stage"];
  label: string;
}

const STAGES: StageDefinition[] = [
  { key: "pii_scan", label: "PII scan" },
  { key: "understanding_request", label: "Understanding request" },
  { key: "intent_mapped", label: "Intent mapped" },
  { key: "evidence_retrieval", label: "Evidence retrieval" },
  { key: "drafting_brief", label: "Drafting brief" },
  { key: "evidence_check_complete", label: "Evidence check complete" },
];

export function ReasoningStrip({
  events,
  isLoading,
  onOpenQueries,
}: ReasoningStripProps) {
  const reducedMotion = useReducedMotion();
  const [expanded, setExpanded] = useState(false);
  const latest = getLatestReasoningEvent(events);

  const currentStageIndex = latest ? getReasoningStageIndex(latest.stage) : isLoading ? 0 : -1;
  const progressPercent = currentStageIndex < 0 ? 0 : ((currentStageIndex + 1) / STAGES.length) * 100;
  const compactLabel = latest
    ? STAGES[getReasoningStageIndex(latest.stage)]?.label
    : isLoading
      ? "Understanding request"
      : "Waiting for analysis";

  const compactArtifact = useMemo(() => {
    if (!latest?.payload) return "";
    if (latest.stage === "intent_mapped") {
      const intent = latest.payload.intentLabel || "Intent identified";
      const confidence = latest.payload.confidence || "Medium";
      return `${intent} • ${confidence}`;
    }
    if (latest.stage === "evidence_retrieval") {
      const sources = (latest.payload.sources || []).join(", ");
      const calls = latest.payload.callCount || 0;
      if (!sources) return "";
      return `${sources} • ${calls} calls`;
    }
    if (latest.stage === "evidence_check_complete") {
      const verification = latest.payload.verification || "Partial";
      return latest.payload.failOpen ? `${verification} • fail-open` : verification;
    }
    return "";
  }, [latest]);

  const compactDetail = latest?.payload?.detail || "";

  if (!isLoading && events.length === 0) {
    return null;
  }

  return (
    <section className="mt-3 rounded-lg border border-border/70 bg-background/80 px-3 py-2">
      <div className="mb-2 flex items-center gap-2">
        <p className="text-xs font-semibold uppercase tracking-[0.12em] text-muted-foreground">
          Agent Pipeline
        </p>
        <div className="relative h-1 flex-1 overflow-hidden rounded-full bg-muted">
          <motion.div
            className="absolute left-0 top-0 h-full rounded-full bg-primary"
            initial={false}
            animate={{ width: `${Math.max(4, progressPercent)}%` }}
            transition={{ duration: reducedMotion ? 0 : 0.24, ease: "easeOut" }}
          />
        </div>
      </div>

      <div className="flex items-center justify-between gap-2">
        <div className="min-w-0">
          <div className="flex items-center gap-1.5 text-sm">
            {isLoading ? (
              <Loader2 className="h-3.5 w-3.5 shrink-0 animate-spin text-primary" />
            ) : (
              <CheckCircle2 className="h-3.5 w-3.5 shrink-0 text-emerald-600 dark:text-emerald-400" />
            )}
            <span className="truncate font-medium text-foreground">{compactLabel}</span>
          </div>
          {compactArtifact && (
            <p className="mt-0.5 truncate text-xs text-muted-foreground">
              {compactArtifact}
            </p>
          )}
          {compactDetail && (
            <motion.p
              key={compactDetail}
              initial={reducedMotion ? false : { opacity: 0, y: 2 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ duration: reducedMotion ? 0 : 0.15 }}
              className="mt-0.5 truncate text-xs text-muted-foreground"
            >
              {compactDetail}
            </motion.p>
          )}
        </div>

        <Button
          type="button"
          variant="ghost"
          size="sm"
          className="h-7 px-2 text-xs"
          onClick={() => setExpanded((v) => !v)}
          aria-expanded={expanded}
          aria-controls="reasoning-strip-details"
        >
          Details
          {expanded ? (
            <ChevronUp className="ml-1 h-3.5 w-3.5" />
          ) : (
            <ChevronDown className="ml-1 h-3.5 w-3.5" />
          )}
        </Button>
      </div>

      <AnimatePresence initial={false}>
        {expanded && (
          <motion.div
            id="reasoning-strip-details"
            initial={reducedMotion ? false : { opacity: 0, y: 3 }}
            animate={{ opacity: 1, y: 0 }}
            exit={reducedMotion ? { opacity: 1 } : { opacity: 0, y: -2 }}
            transition={{ duration: reducedMotion ? 0 : 0.18 }}
            className="mt-3 space-y-2"
          >
            {STAGES.map((stage, index) => {
              const stageEvent = findStageEvent(events, stage.key);
              const baselineMs = events[0] ? new Date(events[0].ts).getTime() : null;
              const stageStatus =
                index < currentStageIndex
                  ? "done"
                  : index === currentStageIndex && isLoading
                    ? "active"
                    : !isLoading && index <= currentStageIndex
                      ? "done"
                      : "pending";
              return (
                <div
                  key={stage.key}
                  className={cn(
                    "rounded-md border px-2.5 py-2",
                    stageStatus === "active" && "border-primary/35 bg-primary/[0.05]",
                    stageStatus === "done" && "border-emerald-500/30 bg-emerald-500/[0.05]",
                    stageStatus === "pending" && "border-border bg-card"
                  )}
                >
                  <div className="flex items-center justify-between gap-2">
                    <div className="flex min-w-0 items-center gap-2">
                      <StatusGlyph status={stageStatus} />
                      <p className="truncate text-xs font-medium">{stage.label}</p>
                    </div>
                    <p className="font-mono text-[10px] text-muted-foreground">
                      {stageEvent
                        ? `${toUtcClock(stageEvent.ts)} ${formatElapsedMs(baselineMs, stageEvent.ts)}`
                        : "--:--:--"}
                    </p>
                  </div>

                  {stageStatus === "active" && stageEvent?.payload?.detail && (
                    <motion.p
                      key={stageEvent.payload.detail}
                      initial={reducedMotion ? false : { opacity: 0 }}
                      animate={{ opacity: 0.8 }}
                      transition={{ duration: reducedMotion ? 0 : 0.15 }}
                      className="mt-1 text-xs text-muted-foreground italic"
                    >
                      {stageEvent.payload.detail}
                    </motion.p>
                  )}

                  {stage.key === "intent_mapped" && stageEvent?.payload?.intentLabel && (
                    <p className="mt-1 text-xs text-muted-foreground">
                      <span className="inline-flex items-center rounded-full border border-primary/30 bg-primary/10 px-2 py-0.5 text-[10px] font-semibold uppercase tracking-[0.08em] text-primary">
                        {stageEvent.payload.intentLabel} • {stageEvent.payload.confidence || "Medium"}
                      </span>
                    </p>
                  )}

                  {stage.key === "evidence_retrieval" && stageEvent?.payload?.sources?.length ? (
                    <div className="mt-1 flex items-center justify-between gap-2 text-xs text-muted-foreground">
                      <span className="truncate">
                        {(stageEvent.payload.sources || []).join(", ")} • {stageEvent.payload.callCount || 0} calls
                      </span>
                      {stageStatus === "done" && (
                        <button
                          type="button"
                          onClick={onOpenQueries}
                          className="shrink-0 text-primary underline underline-offset-2 hover:text-primary/80"
                        >
                          View queries
                        </button>
                      )}
                    </div>
                  ) : null}

                  {stage.key === "evidence_check_complete" && stageEvent?.payload?.verification && (
                    <p className="mt-1 flex items-center gap-1.5 text-xs text-muted-foreground">
                      {stageEvent.payload.failOpen && (
                        <ShieldAlert className="h-3.5 w-3.5 text-orange-600 dark:text-orange-400" />
                      )}
                      {stageEvent.payload.verification}
                      {stageEvent.payload.failOpen ? " • fail-open" : ""}
                    </p>
                  )}
                </div>
              );
            })}
          </motion.div>
        )}
      </AnimatePresence>
    </section>
  );
}

function StatusGlyph({ status }: { status: "pending" | "active" | "done" }) {
  if (status === "done") {
    return <CheckCircle2 className="h-3.5 w-3.5 text-emerald-600 dark:text-emerald-400" />;
  }
  if (status === "active") {
    return <Loader2 className="h-3.5 w-3.5 animate-spin text-primary" />;
  }
  return <Circle className="h-3.5 w-3.5 text-muted-foreground/70" />;
}

function findStageEvent(
  events: ReasoningSseEvent[],
  stage: ReasoningSseEvent["stage"]
): ReasoningSseEvent | undefined {
  for (let i = events.length - 1; i >= 0; i -= 1) {
    const event = events[i];
    if (event?.stage === stage) {
      return event;
    }
  }
  return undefined;
}

function toUtcClock(value: string): string {
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return "--:--:--";
  }
  return parsed.toLocaleTimeString("en-US", {
    hour12: false,
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    timeZone: "UTC",
  });
}

function formatElapsedMs(baselineMs: number | null, eventTs: string): string {
  if (baselineMs === null) return "";
  const eventMs = new Date(eventTs).getTime();
  if (Number.isNaN(eventMs)) return "";
  const delta = eventMs - baselineMs;
  if (delta < 0) return "";
  if (delta < 1000) return `+${delta}ms`;
  return `+${(delta / 1000).toFixed(1)}s`;
}
