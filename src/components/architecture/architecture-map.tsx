"use client";

import { motion } from "framer-motion";
import Image from "next/image";
import { DATA_SOURCE_BLUEPRINT } from "@/data/seed";
import type { SourceHealthStatus } from "@/types";
import { cn } from "@/lib/utils";
import { getDatastoreVisual } from "@/lib/datastore";

interface ArchitectureMapProps {
  sourceHealth: SourceHealthStatus[];
}

export function ArchitectureMap({ sourceHealth }: ArchitectureMapProps) {
  const getSourceState = (id: string) => {
    return sourceHealth.find((entry) => entry.source === id)?.status || "idle";
  };

  return (
    <div className="space-y-4">
      <p className="text-sm text-muted-foreground">
        This map reveals which data stores are activated by the retrieval planner and how
        context is assembled before answer synthesis.
      </p>

      <div className="grid gap-3 lg:grid-cols-2">
        {DATA_SOURCE_BLUEPRINT.map((node, index) => {
          const status = getSourceState(node.id);
          const visual = getDatastoreVisual(node.id);

          return (
            <motion.div
              key={node.id}
              initial={{ opacity: 0, y: 10 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: index * 0.05, duration: 0.25 }}
              className={cn(
                "relative rounded-xl border bg-card p-4",
                status === "querying" && "border-primary/60 bg-primary/5",
                status === "ready" && "border-emerald-500/40 bg-emerald-500/5",
                status === "error" && "border-destructive/40 bg-destructive/5",
                status === "idle" && "border-border"
              )}
            >
              <div className="mb-2 flex items-center justify-between gap-2">
                <div className="flex min-w-0 items-center gap-2">
                  <div className="h-9 w-9 shrink-0 overflow-hidden rounded-md border border-border bg-white/80 p-1">
                    <Image
                      src={visual.iconSrc}
                      alt={visual.shortLabel}
                      width={36}
                      height={36}
                      className="h-full w-full object-contain"
                    />
                  </div>
                  <div className="min-w-0">
                    <p className="truncate text-sm font-semibold">{node.label}</p>
                    <p className="truncate text-xs text-muted-foreground">{visual.longLabel}</p>
                  </div>
                </div>
                <StatusPill status={status} isFabric={visual.isFabric} />
              </div>

              <div className="space-y-1.5 text-xs">
                <p>
                  <span className="font-semibold text-foreground">Store: </span>
                  <span className="text-muted-foreground">{node.datastore}</span>
                </p>
                <p>
                  <span className="font-semibold text-foreground">Mode: </span>
                  <span className="text-muted-foreground">{node.retrievalMode}</span>
                </p>
                <p className="text-muted-foreground">{node.rationale}</p>
              </div>
            </motion.div>
          );
        })}
      </div>

      <div className="rounded-lg border border-border bg-secondary/40 p-3 text-xs text-muted-foreground">
        Context path: Planner -&gt; Source arbitration -&gt; Retrieval execution -&gt;
        Evidence assembly -&gt; Response synthesis.
      </div>
    </div>
  );
}

function StatusPill({
  status,
  isFabric,
}: {
  status: SourceHealthStatus["status"] | "idle";
  isFabric: boolean;
}) {
  if (status === "querying") {
    return (
      <span className="inline-flex items-center gap-1 rounded-full border border-primary/35 bg-primary/10 px-2 py-0.5 text-[11px] font-semibold text-primary">
        <span className="status-pulse" />
        Querying
      </span>
    );
  }

  if (status === "ready") {
    return (
      <span className="inline-flex items-center rounded-full border border-emerald-500/35 bg-emerald-500/10 px-2 py-0.5 text-[11px] font-semibold text-emerald-600 dark:text-emerald-400">
        Complete
      </span>
    );
  }

  if (status === "error") {
    return (
      <span className="inline-flex items-center rounded-full border border-destructive/35 bg-destructive/10 px-2 py-0.5 text-[11px] font-semibold text-destructive">
        Error
      </span>
    );
  }

  if (isFabric) {
    return (
      <span className="inline-flex items-center rounded-full border border-primary/35 bg-primary/10 px-2 py-0.5 text-[11px] font-semibold text-primary">
        Fabric
      </span>
    );
  }

  return (
    <span className="inline-flex items-center rounded-full border border-border bg-background px-2 py-0.5 text-[11px] font-semibold text-muted-foreground">
      Idle
    </span>
  );
}
