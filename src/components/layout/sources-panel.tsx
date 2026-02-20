"use client";

import { motion, AnimatePresence, useReducedMotion } from "framer-motion";
import Image from "next/image";
import {
  ChevronLeft,
  ChevronRight,
  Database,
  FileText,
  Clock,
  CheckCircle2,
  ShieldCheck,
  RefreshCw,
  AlertTriangle,
  Gauge,
  Lock,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Badge } from "@/components/ui/badge";
import { Separator } from "@/components/ui/separator";
import { cn, formatDateTime } from "@/lib/utils";
import { PAIR_TRUST_NOTES } from "@/data/seed";
import { getDatastoreVisual } from "@/lib/datastore";
import { motionTokens } from "@/lib/motion";
import type { Citation, SourceHealthStatus } from "@/types";

interface SourcesPanelProps {
  isCollapsed: boolean;
  onToggle: () => void;
  citations: Citation[];
  activeCitationId: number | null;
  onCitationClick: (id: number) => void;
  sourceHealth: SourceHealthStatus[];
  route?: string;
  isLoading: boolean;
  confidenceLabel: string;
}

export function SourcesPanel({
  isCollapsed,
  onToggle,
  citations,
  activeCitationId,
  onCitationClick,
  sourceHealth,
  route,
  isLoading,
  confidenceLabel,
}: SourcesPanelProps) {
  const reducedMotion = useReducedMotion();

  return (
    <motion.aside
      initial={false}
      animate={{ width: isCollapsed ? 64 : 360 }}
      transition={{ duration: reducedMotion ? 0 : motionTokens.panel, ease: motionTokens.easeInOut }}
      className="relative flex h-full flex-col border-l border-border bg-surface-1/80"
    >
      <Button
        variant="ghost"
        size="icon-sm"
        onClick={onToggle}
        className="absolute -left-3 top-16 z-10 h-6 w-6 rounded-full border border-border bg-background shadow-subtle"
        aria-label={isCollapsed ? "Expand panel" : "Collapse panel"}
      >
        {isCollapsed ? <ChevronLeft className="h-3.5 w-3.5" /> : <ChevronRight className="h-3.5 w-3.5" />}
      </Button>

      <div className="border-b border-border p-4">
        <AnimatePresence mode="wait">
          {!isCollapsed ? (
            <motion.div
              key="expanded"
              initial={reducedMotion ? false : { opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={reducedMotion ? { opacity: 1 } : { opacity: 0 }}
              transition={{ duration: reducedMotion ? 0 : motionTokens.state }}
            >
              <div className="mb-3 flex items-center justify-between gap-2">
                <h2 className="font-display text-sm font-semibold">Evidence Ledger</h2>
                <Badge variant="gold">{citations.length} refs</Badge>
              </div>

              <div className="grid gap-2 text-xs">
                <div className="rounded-lg border border-border bg-card p-2.5">
                  <p className="mb-1 text-muted-foreground">Route</p>
                  <p className="font-mono font-semibold text-foreground">{route || "Pending"}</p>
                </div>
                <div className="rounded-lg border border-border bg-card p-2.5">
                  <p className="mb-1 text-muted-foreground">Confidence</p>
                  <p className="font-semibold text-foreground">{confidenceLabel}</p>
                </div>
              </div>

              <AnimatePresence>
                {!isLoading && citations.length > 0 && (
                  <motion.div
                    initial={reducedMotion ? false : { opacity: 0, y: 8 }}
                    animate={{ opacity: 1, y: 0 }}
                    exit={reducedMotion ? { opacity: 1 } : { opacity: 0, y: -4 }}
                    transition={{ duration: reducedMotion ? 0 : motionTokens.state, ease: motionTokens.easeOut }}
                    className="mt-3 flex items-center gap-2 rounded-lg border border-emerald-500/30 bg-emerald-500/10 px-2.5 py-2 text-[11px] text-emerald-700 dark:text-emerald-300"
                  >
                    <Lock className="h-3.5 w-3.5" />
                    Evidence manifest locked for this answer.
                  </motion.div>
                )}
              </AnimatePresence>
            </motion.div>
          ) : (
            <motion.div
              key="collapsed"
              initial={reducedMotion ? false : { opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={reducedMotion ? { opacity: 1 } : { opacity: 0 }}
              className="flex justify-center"
            >
              <Database className="h-4 w-4 text-muted-foreground" />
            </motion.div>
          )}
        </AnimatePresence>
      </div>

      <ScrollArea className="flex-1">
        <AnimatePresence>
          {!isCollapsed && (
            <motion.div
              initial={reducedMotion ? false : { opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={reducedMotion ? { opacity: 1 } : { opacity: 0 }}
              transition={{ duration: reducedMotion ? 0 : motionTokens.state }}
              className="space-y-4 p-4"
            >
              <section className="space-y-2">
                <p className="text-xs font-semibold uppercase tracking-[0.12em] text-muted-foreground">
                  Data Stores Queried
                </p>
                {sourceHealth.length === 0 ? (
                  <div className="rounded-lg border border-dashed border-border p-3 text-xs text-muted-foreground">
                    Source status appears after query execution starts.
                  </div>
                ) : (
                  <AnimatePresence initial={false}>
                    {sourceHealth.map((source) => {
                      const visual = getDatastoreVisual(source.source);
                      const tone = getSourceTone(source.source);

                      return (
                        <motion.div
                          key={source.source}
                          layout
                          animate={{ opacity: 1, y: 0 }}
                          initial={
                            reducedMotion
                              ? false
                              : {
                                  opacity: 0,
                                  y: 6,
                                }
                          }
                          transition={{ duration: reducedMotion ? 0 : motionTokens.state, ease: motionTokens.easeOut }}
                          className={cn(
                            "relative overflow-hidden rounded-lg border bg-card px-3 py-2 text-xs",
                            source.status === "querying" && tone.queryCard,
                            source.status === "ready" && tone.readyCard,
                            source.status === "error" && "border-destructive/45 bg-destructive/[0.05]"
                          )}
                        >
                          <span
                            className={cn(
                              "pointer-events-none absolute inset-y-0 left-0 w-1 rounded-l-lg opacity-0 transition-opacity duration-300",
                              tone.accentBar,
                              source.status !== "idle" && "opacity-100"
                            )}
                          />
                          {source.status === "querying" && !reducedMotion && (
                            <>
                              <motion.span
                                className={cn("pointer-events-none absolute inset-0 rounded-lg border", tone.queryBorder)}
                                animate={{ opacity: [0.65, 0.25, 0.65] }}
                                transition={{ duration: 1.6, repeat: Infinity, ease: "easeInOut" }}
                              />
                              <motion.span className="pointer-events-none absolute inset-0 overflow-hidden rounded-lg">
                                <motion.span
                                  className="absolute inset-y-0 -left-1/3 w-1/2 blur-xl"
                                  style={{ background: tone.querySheen }}
                                  animate={{ x: ["-140%", "240%"] }}
                                  transition={{ duration: 1.8, repeat: Infinity, ease: "easeInOut" }}
                                />
                              </motion.span>
                            </>
                          )}
                          {source.status === "ready" && !reducedMotion && (
                            <motion.span
                              className="pointer-events-none absolute inset-0 rounded-lg"
                              style={{ boxShadow: `inset 0 0 0 1px ${tone.readyGlow}` }}
                              animate={{ opacity: [0.2, 0.55, 0.2] }}
                              transition={{ duration: 2.8, repeat: Infinity, ease: "easeInOut" }}
                            />
                          )}
                          <div className="relative z-[1] mb-1 flex items-center justify-between gap-2">
                            <div className="flex min-w-0 items-center gap-2">
                              <motion.div
                                animate={
                                  source.status === "querying" && !reducedMotion
                                    ? { scale: [1, 1.04, 1] }
                                    : { scale: 1 }
                                }
                                transition={{ duration: 1.2, repeat: source.status === "querying" ? Infinity : 0 }}
                                className="h-7 w-7 shrink-0 overflow-hidden rounded-md border border-border bg-white/80 p-0.5"
                              >
                                <Image
                                  src={visual.iconSrc}
                                  alt={visual.shortLabel}
                                  width={28}
                                  height={28}
                                  className="h-full w-full object-contain"
                                />
                              </motion.div>
                              <div className="min-w-0">
                                <span className="block truncate font-medium text-foreground">{visual.longLabel}</span>
                                {visual.isFabric && (
                                  <span className="text-[10px] font-semibold uppercase tracking-[0.09em] text-primary">
                                    Fabric datastore
                                  </span>
                                )}
                              </div>
                            </div>
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
                            >
                              {source.status}
                            </Badge>
                          </div>
                          <div className="relative z-[1] flex items-center justify-between text-muted-foreground">
                            <AnimatePresence mode="popLayout" initial={false}>
                              <motion.span
                                key={`${source.source}-${source.rowCount}`}
                                initial={reducedMotion ? false : { opacity: 0, y: 6 }}
                                animate={{ opacity: 1, y: 0 }}
                                exit={reducedMotion ? { opacity: 1 } : { opacity: 0, y: -4 }}
                                transition={{ duration: reducedMotion ? 0 : motionTokens.micro }}
                              >
                                {source.rowCount} rows
                              </motion.span>
                            </AnimatePresence>
                            <span className="font-mono text-[10px]">
                              {source.updatedAt ? formatDateTime(source.updatedAt) : "-"}
                            </span>
                          </div>
                          {source.mode && (
                            <div className="relative z-[1] mt-1 text-[10px] text-muted-foreground">
                              Retrieval mode: <span className="font-semibold text-foreground">{source.mode}</span>
                            </div>
                          )}
                        </motion.div>
                      );
                    })}
                  </AnimatePresence>
                )}
              </section>

              <Separator />

              <section className="space-y-2">
                <p className="text-xs font-semibold uppercase tracking-[0.12em] text-muted-foreground">
                  Data Used In Answer
                </p>
                {citations.length === 0 ? (
                  <div className="rounded-lg border border-dashed border-border p-3 text-xs text-muted-foreground">
                    Evidence manifest will be generated once citations are emitted.
                  </div>
                ) : (
                  citations.map((citation) => (
                    <CitationCard
                      key={citation.id}
                      citation={citation}
                      isActive={citation.id === activeCitationId}
                      onClick={() => onCitationClick(citation.id)}
                    />
                  ))
                )}
              </section>

              <Separator />

              <section className="space-y-2">
                <p className="text-xs font-semibold uppercase tracking-[0.12em] text-muted-foreground">
                  Interaction Guarantees
                </p>
                <div className="space-y-2 rounded-lg border border-border bg-card p-3 text-xs text-muted-foreground">
                  {PAIR_TRUST_NOTES.map((note) => (
                    <div key={note} className="flex items-start gap-2">
                      <ShieldCheck className="mt-0.5 h-3.5 w-3.5 shrink-0 text-primary" />
                      <span>{note}</span>
                    </div>
                  ))}
                </div>
              </section>

              <Separator />

              <section className="rounded-lg border border-border bg-card p-3 text-xs">
                <div className="mb-2 flex items-center gap-1.5 font-semibold text-foreground">
                  <Gauge className="h-3.5 w-3.5 text-primary" />
                  Runtime state
                </div>
                <div className="flex items-center gap-2 text-muted-foreground">
                  {isLoading ? (
                    <>
                      <RefreshCw className="h-3.5 w-3.5 animate-spin" />
                      <span>Agent run in progress</span>
                    </>
                  ) : (
                    <>
                      <CheckCircle2 className="h-3.5 w-3.5 text-emerald-600 dark:text-emerald-400" />
                      <span>Run complete</span>
                    </>
                  )}
                </div>
                <p className="mt-2 flex items-start gap-1.5 text-muted-foreground">
                  <AlertTriangle className="mt-0.5 h-3.5 w-3.5 shrink-0" />
                  The final answer can be incomplete if a source returns no rows or times out.
                </p>
              </section>
            </motion.div>
          )}
        </AnimatePresence>

        {isCollapsed && citations.length > 0 && (
          <div className="flex flex-col items-center gap-2 p-2">
            {citations.map((citation) => (
              <Button
                key={citation.id}
                variant={citation.id === activeCitationId ? "secondary" : "ghost"}
                size="icon-sm"
                onClick={() => onCitationClick(citation.id)}
                className="h-8 w-8"
              >
                <span className="text-xs font-semibold">{citation.id}</span>
              </Button>
            ))}
          </div>
        )}
      </ScrollArea>
    </motion.aside>
  );
}

interface SourceTone {
  accentBar: string;
  queryCard: string;
  queryBorder: string;
  readyCard: string;
  querySheen: string;
  readyGlow: string;
}

function getSourceTone(sourceId: string): SourceTone {
  const source = sourceId.toUpperCase();
  if (source.startsWith("VECTOR")) {
    return {
      accentBar: "bg-sky-500/80",
      queryCard: "border-sky-500/45 bg-sky-500/[0.08]",
      queryBorder: "border-sky-500/55",
      readyCard: "border-sky-500/35 bg-sky-500/[0.05]",
      querySheen: "linear-gradient(115deg, transparent 10%, rgba(14, 165, 233, 0.24) 52%, transparent 88%)",
      readyGlow: "rgba(14, 165, 233, 0.24)",
    };
  }

  if (source === "NOSQL" || source === "POSTGRES") {
    return {
      accentBar: "bg-teal-500/80",
      queryCard: "border-teal-500/45 bg-teal-500/[0.08]",
      queryBorder: "border-teal-500/55",
      readyCard: "border-teal-500/35 bg-teal-500/[0.05]",
      querySheen: "linear-gradient(115deg, transparent 10%, rgba(20, 184, 166, 0.24) 52%, transparent 88%)",
      readyGlow: "rgba(20, 184, 166, 0.24)",
    };
  }

  return {
    accentBar: "bg-primary/80",
    queryCard: "border-primary/45 bg-primary/[0.07]",
    queryBorder: "border-primary/55",
    readyCard: "border-primary/35 bg-primary/[0.05]",
    querySheen: "linear-gradient(115deg, transparent 10%, rgba(25, 81, 171, 0.24) 52%, transparent 88%)",
    readyGlow: "rgba(25, 81, 171, 0.22)",
  };
}

function CitationCard({
  citation,
  isActive,
  onClick,
}: {
  citation: Citation;
  isActive: boolean;
  onClick: () => void;
}) {
  const reducedMotion = useReducedMotion();

  return (
    <motion.button
      onClick={onClick}
      initial={reducedMotion ? false : { opacity: 0, y: 6 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: reducedMotion ? 0 : motionTokens.state, ease: motionTokens.easeOut }}
      className={cn(
        "w-full rounded-lg border p-3 text-left transition-all",
        isActive
          ? "border-primary/40 bg-primary/5 ring-1 ring-primary/25"
          : "border-border bg-card hover:border-primary/20 hover:bg-primary/[0.03]"
      )}
    >
      <div className="space-y-2">
        <div className="flex items-start justify-between gap-2">
          <div className="flex items-center gap-2">
            <span className="citation-chip">{citation.id}</span>
            <span className="text-sm font-medium text-foreground">{citation.provider}</span>
          </div>
          <div className="flex items-center gap-1 text-xs text-emerald-600 dark:text-emerald-400">
            <CheckCircle2 className="h-3 w-3" />
            <span>{Math.round(citation.confidence * 100)}%</span>
          </div>
        </div>

        <div className="space-y-1 text-xs text-muted-foreground">
          <div className="flex items-center gap-1.5">
            <Database className="h-3 w-3" />
            <span>{citation.dataset}</span>
          </div>
          <div className="flex items-center gap-1.5">
            <FileText className="h-3 w-3" />
            <span className="font-mono">{citation.rowId}</span>
          </div>
          <div className="flex items-center gap-1.5">
            <Clock className="h-3 w-3" />
            <span>{formatDateTime(citation.timestamp)}</span>
          </div>
        </div>

        {citation.excerpt && (
          <>
            <Separator className="my-2" />
            <p className="line-clamp-3 text-xs italic text-muted-foreground">&ldquo;{citation.excerpt}&rdquo;</p>
          </>
        )}
      </div>
    </motion.button>
  );
}
