"use client";

import { useState, useEffect } from "react";
import { AnimatePresence, motion, useReducedMotion } from "framer-motion";
import { Workflow, Sparkles, Check, ArrowRightLeft } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";
import { motionTokens, fadeUp } from "@/lib/motion";
import { PIPELINE_CONFIGS } from "@/data/seed";
import type { RetrievalMode } from "@/types";

interface PipelineSelectorProps {
  value: RetrievalMode;
  onChange: (mode: RetrievalMode) => void;
  expanded: boolean;
}

const ICON_MAP = {
  "code-rag": Workflow,
  "foundry-iq": Sparkles,
} as const;

export function PipelineSelector({
  value,
  onChange,
  expanded,
}: PipelineSelectorProps) {
  const reducedMotion = useReducedMotion() ?? false;

  const [isHydrated, setIsHydrated] = useState(false);
  useEffect(() => {
    setIsHydrated(true);
  }, []);

  const active = PIPELINE_CONFIGS.find((p) => p.id === value)!;
  const other = PIPELINE_CONFIGS.find((p) => p.id !== value)!;
  const OtherIcon = ICON_MAP[other.id];

  if (!isHydrated) {
    // SSR / pre-hydration: render banner as plain div (no motion)
    return expanded ? null : (
      <div className="border-b border-border bg-card/80">
        <div className="mx-auto flex max-w-5xl items-center justify-between px-4 py-2">
          <div className="flex items-center gap-2 text-sm">
            <Workflow className="h-4 w-4 text-primary" />
            <span className="font-medium">{active.name}</span>
            <Badge variant="outline" className="text-[10px] uppercase tracking-[0.08em]">
              {active.tagline}
            </Badge>
          </div>
        </div>
      </div>
    );
  }

  return (
    <AnimatePresence mode="wait">
      {expanded ? (
        <motion.div
          key="hero"
          {...fadeUp(reducedMotion, 12)}
          className="mx-auto w-full max-w-3xl px-4 pt-8 pb-2"
        >
          <h2 className="mb-5 text-center text-sm font-medium uppercase tracking-widest text-muted-foreground">
            Choose Your Intelligence Pipeline
          </h2>

          <div className="grid grid-cols-1 gap-4 md:grid-cols-2">
            {PIPELINE_CONFIGS.map((pipeline) => {
              const isSelected = pipeline.id === value;
              const Icon = ICON_MAP[pipeline.id];
              const isTeal = pipeline.accentColor === "teal";

              return (
                <button
                  key={pipeline.id}
                  type="button"
                  onClick={() => onChange(pipeline.id)}
                  className={cn(
                    "group relative flex flex-col gap-3 rounded-xl border border-border bg-card p-5 text-left transition-all",
                    "border-l-4",
                    isTeal ? "border-l-teal-500" : "border-l-primary",
                    isSelected && !isTeal && "bg-primary/5 border-primary/40",
                    isSelected && isTeal && "bg-teal-500/5 border-teal-500/40",
                    !isSelected && "hover:border-border/80",
                    isTeal
                      ? "hover:shadow-[0_0_16px_hsl(187,72%,54%,0.12)]"
                      : "hover:shadow-[0_0_16px_hsl(214,88%,66%,0.12)]",
                  )}
                >
                  {/* Selected checkmark */}
                  {isSelected && (
                    <div
                      className={cn(
                        "absolute right-3 top-3 flex h-5 w-5 items-center justify-center rounded-full",
                        isTeal ? "bg-teal-500" : "bg-primary",
                      )}
                    >
                      <Check className="h-3 w-3 text-white" />
                    </div>
                  )}

                  {/* Icon + Name */}
                  <div className="flex items-center gap-2">
                    <Icon
                      className={cn(
                        "h-5 w-5",
                        isTeal ? "text-teal-400" : "text-primary",
                      )}
                    />
                    <span className="text-sm font-semibold">{pipeline.name}</span>
                  </div>

                  {/* Tagline badge */}
                  <Badge
                    variant="outline"
                    className={cn(
                      "w-fit text-[10px] uppercase tracking-[0.08em]",
                      isTeal
                        ? "border-teal-500/30 text-teal-400"
                        : "border-primary/30 text-primary",
                    )}
                  >
                    {pipeline.tagline}
                  </Badge>

                  {/* Description */}
                  <p className="text-xs leading-relaxed text-muted-foreground">
                    {pipeline.description}
                  </p>

                  {/* Data source chips */}
                  <div className="flex flex-wrap gap-1.5">
                    {pipeline.dataSources.map((ds) => (
                      <span
                        key={ds}
                        className="rounded-md bg-surface-2 px-2 py-0.5 text-[10px] text-muted-foreground"
                      >
                        {ds}
                      </span>
                    ))}
                  </div>
                </button>
              );
            })}
          </div>
        </motion.div>
      ) : (
        <motion.div
          key="banner"
          initial={reducedMotion ? false : { opacity: 0, height: 0 }}
          animate={{ opacity: 1, height: "auto" }}
          exit={reducedMotion ? { opacity: 1 } : { opacity: 0, height: 0 }}
          transition={{
            duration: motionTokens.state,
            ease: motionTokens.easeOut,
          }}
          className={cn(
            "border-b border-border bg-card/80",
            active.accentColor === "teal"
              ? "border-t-2 border-t-teal-500"
              : "border-t-2 border-t-primary",
          )}
        >
          <div className="mx-auto flex max-w-5xl items-center justify-between px-4 py-2">
            {/* Active pipeline info */}
            <div className="flex items-center gap-2 text-sm">
              {active.accentColor === "teal" ? (
                <Sparkles className="h-4 w-4 text-teal-400" />
              ) : (
                <Workflow className="h-4 w-4 text-primary" />
              )}
              <span className="font-medium">{active.name}</span>
              <Badge
                variant="outline"
                className={cn(
                  "text-[10px] uppercase tracking-[0.08em]",
                  active.accentColor === "teal"
                    ? "border-teal-500/30 text-teal-400"
                    : "border-primary/30 text-primary",
                )}
              >
                {active.tagline}
              </Badge>
            </div>

            {/* Switch button */}
            <Button
              variant="ghost"
              size="sm"
              className="h-7 gap-1.5 text-xs text-muted-foreground"
              onClick={() => onChange(other.id)}
            >
              <ArrowRightLeft className="h-3 w-3" />
              <OtherIcon className="h-3 w-3" />
              Switch to {other.name}
            </Button>
          </div>
        </motion.div>
      )}
    </AnimatePresence>
  );
}
