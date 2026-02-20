"use client";

import { motion, AnimatePresence } from "framer-motion";
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
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Badge } from "@/components/ui/badge";
import { Separator } from "@/components/ui/separator";
import { cn, formatDateTime } from "@/lib/utils";
import { PAIR_TRUST_NOTES } from "@/data/seed";
import { getDatastoreVisual } from "@/lib/datastore";
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
  return (
    <motion.aside
      initial={false}
      animate={{ width: isCollapsed ? 64 : 360 }}
      transition={{ duration: 0.2, ease: "easeInOut" }}
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
            <motion.div key="expanded" initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }}>
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
            </motion.div>
          ) : (
            <motion.div
              key="collapsed"
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
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
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
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
                  sourceHealth.map((source) => {
                    const visual = getDatastoreVisual(source.source);
                    return (
                      <div key={source.source} className="rounded-lg border border-border bg-card px-3 py-2 text-xs">
                        <div className="mb-1 flex items-center justify-between gap-2">
                          <div className="flex min-w-0 items-center gap-2">
                            <div className="h-7 w-7 shrink-0 overflow-hidden rounded-md border border-border bg-white/80 p-0.5">
                              <Image
                                src={visual.iconSrc}
                                alt={visual.shortLabel}
                                width={28}
                                height={28}
                                className="h-full w-full object-contain"
                              />
                            </div>
                            <div className="min-w-0">
                              <span className="block truncate font-medium text-foreground">
                                {visual.longLabel}
                              </span>
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
                        <div className="flex items-center justify-between text-muted-foreground">
                          <span>{source.rowCount} rows</span>
                          <span className="font-mono text-[10px]">
                            {source.updatedAt ? formatDateTime(source.updatedAt) : "-"}
                          </span>
                        </div>
                      </div>
                    );
                  })
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

function CitationCard({
  citation,
  isActive,
  onClick,
}: {
  citation: Citation;
  isActive: boolean;
  onClick: () => void;
}) {
  return (
    <button
      onClick={onClick}
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
            <p className="line-clamp-3 text-xs italic text-muted-foreground">
              &ldquo;{citation.excerpt}&rdquo;
            </p>
          </>
        )}
      </div>
    </button>
  );
}
