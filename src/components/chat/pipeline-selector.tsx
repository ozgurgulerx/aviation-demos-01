"use client";

import { useState, useEffect } from "react";
import { AnimatePresence, motion, useReducedMotion } from "framer-motion";
import {
  Workflow,
  Sparkles,
  Check,
  ArrowRightLeft,
  Route,
  Database,
  Layers,
  Eye,
} from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";
import { motionTokens, fadeUp } from "@/lib/motion";
import { PIPELINE_CONFIGS } from "@/data/seed";
import type { RetrievalMode, PipelineConfig, PipelineFeature } from "@/types";

interface PipelineSelectorProps {
  value: RetrievalMode;
  onChange: (mode: RetrievalMode) => void;
  expanded: boolean;
}

const ICON_MAP = {
  "code-rag": Workflow,
  "foundry-iq": Sparkles,
} as const;

const FEATURE_ICON_MAP: Record<
  PipelineFeature["icon"],
  React.ComponentType<{ className?: string }>
> = {
  Route,
  Database,
  Layers,
  Eye,
};

function badgeClasses(badge: PipelineConfig["badge"]): string {
  return badge === "Default"
    ? "border-primary/40 bg-primary/10 text-primary"
    : "border-border text-muted-foreground";
}

function FeatureRow({ feature, isTeal }: { feature: PipelineFeature; isTeal: boolean }) {
  const IconComp = FEATURE_ICON_MAP[feature.icon];
  return (
    <div className="flex items-start gap-2 text-[11px]">
      <IconComp
        className={cn(
          "mt-0.5 h-3 w-3 shrink-0",
          isTeal ? "text-teal-400/70" : "text-primary/70",
        )}
      />
      <div className="min-w-0">
        <span className="font-medium uppercase tracking-wide text-muted-foreground">
          {feature.label}
        </span>
        <span className="mx-1.5 text-border">|</span>
        <span className="text-foreground/80">{feature.value}</span>
      </div>
    </div>
  );
}

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
            <Badge
              variant="outline"
              className={cn("text-[10px] uppercase tracking-[0.08em]", badgeClasses(active.badge))}
            >
              {active.badge}
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
                    "group relative flex flex-col gap-2.5 rounded-xl border border-border bg-card p-5 text-left transition-all",
                    "border-l-4",
                    isTeal ? "border-l-teal-500" : "border-l-primary",
                    isSelected && !isTeal && "bg-primary/5 border-primary/40 ring-1 ring-primary/20",
                    isSelected && isTeal && "bg-teal-500/5 border-teal-500/40 ring-1 ring-teal-500/20",
                    !isSelected && "hover:border-border/80",
                    isTeal
                      ? "hover:shadow-[0_0_16px_hsl(187,72%,54%,0.12)]"
                      : "hover:shadow-[0_0_16px_hsl(214,88%,66%,0.12)]",
                  )}
                >
                  {/* Top row: Checkmark + Icon + Name + Badge */}
                  <div className="flex items-center gap-2">
                    {/* Reserve space for checkmark to prevent layout shift */}
                    <div
                      className={cn(
                        "flex h-5 w-5 shrink-0 items-center justify-center rounded-full",
                        isSelected
                          ? isTeal ? "bg-teal-500" : "bg-primary"
                          : "invisible",
                      )}
                    >
                      <Check className="h-3 w-3 text-white" />
                    </div>
                    <Icon
                      className={cn(
                        "h-5 w-5 shrink-0",
                        isTeal ? "text-teal-400" : "text-primary",
                      )}
                    />
                    <span className="text-base font-semibold">{pipeline.name}</span>

                    {/* Badge pill — pushed to right */}
                    <Badge
                      variant="outline"
                      className={cn(
                        "ml-auto text-[10px] uppercase tracking-[0.08em]",
                        badgeClasses(pipeline.badge),
                      )}
                    >
                      {pipeline.badge}
                    </Badge>
                  </div>

                  {/* Philosophy — italic conceptual "why" */}
                  <p className="text-xs italic leading-relaxed text-muted-foreground">
                    {pipeline.philosophy}
                  </p>

                  {/* Dashed separator */}
                  <div className="border-t border-dashed border-border/60" />

                  {/* Feature comparison rows */}
                  <div className="flex flex-col gap-1.5">
                    {pipeline.features.map((feature) => (
                      <FeatureRow
                        key={feature.label}
                        feature={feature}
                        isTeal={isTeal}
                      />
                    ))}
                  </div>

                  {/* Dashed separator */}
                  <div className="border-t border-dashed border-border/60" />

                  {/* Best for footer */}
                  <div className="text-[10px] leading-relaxed">
                    <span className="font-medium uppercase tracking-wide text-muted-foreground">
                      Best for
                    </span>
                    <span className="mx-1.5 text-border">|</span>
                    <span className="text-foreground/70">{pipeline.bestFor}</span>
                  </div>

                  {/* Tradeoff note */}
                  <p className="text-[10px] leading-relaxed text-muted-foreground/60">
                    ⚠ {pipeline.tradeoff}
                  </p>

                  {/* Data source chips (de-emphasized) */}
                  <div className="flex flex-wrap gap-1.5">
                    {pipeline.dataSources.map((ds) => (
                      <span
                        key={ds}
                        className="rounded-md bg-surface-2 px-2 py-0.5 text-[10px] text-muted-foreground/70"
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
            <div className="flex min-w-0 items-center gap-2 text-sm">
              {active.accentColor === "teal" ? (
                <Sparkles className="h-4 w-4 shrink-0 text-teal-400" />
              ) : (
                <Workflow className="h-4 w-4 shrink-0 text-primary" />
              )}
              <span className="font-medium">{active.name}</span>
              <Badge
                variant="outline"
                className={cn("text-[10px] uppercase tracking-[0.08em]", badgeClasses(active.badge))}
              >
                {active.badge}
              </Badge>
              <span className="hidden truncate text-xs text-muted-foreground md:inline">
                · {active.sourceCount} sources · {active.features[0].value}
              </span>
            </div>

            {/* Switch button */}
            <Button
              variant="ghost"
              size="sm"
              className="h-7 shrink-0 gap-1.5 text-xs text-muted-foreground"
              onClick={() => onChange(other.id)}
            >
              <ArrowRightLeft className="h-3 w-3" />
              <OtherIcon className="h-3 w-3" />
              <span>
                Switch to {other.name}{" "}
                <span className="hidden sm:inline">({other.badge})</span>
              </span>
            </Button>
          </div>
        </motion.div>
      )}
    </AnimatePresence>
  );
}
