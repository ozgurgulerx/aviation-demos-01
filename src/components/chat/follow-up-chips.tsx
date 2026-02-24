"use client";

import { useState, useEffect } from "react";
import { motion, AnimatePresence } from "framer-motion";
import {
  Radar,
  ShieldAlert,
  Workflow,
  Wrench,
  BookCheck,
  ChevronDown,
  ChevronUp,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import type { QueryType } from "@/data/seed";

interface FollowUpSuggestion {
  text: string;
  type: QueryType;
  sources?: number;
}

interface FollowUpChipsProps {
  suggestions: string[] | FollowUpSuggestion[];
  onSelect: (suggestion: string) => void;
  isVisible: boolean;
}

const iconMap: Record<QueryType, React.ElementType> = {
  "ops-live": Radar,
  safety: ShieldAlert,
  network: Workflow,
  maintenance: Wrench,
  compliance: BookCheck,
};

type ComplexityTier = 1 | 2 | 3;

function getComplexityTier(sources: number): ComplexityTier {
  if (sources <= 2) return 1;
  if (sources <= 4) return 2;
  return 3;
}

const tierColorMap: Record<ComplexityTier, { bg: string; text: string; border: string; glow?: string }> = {
  1: {
    bg: "bg-slate-500/10",
    text: "text-slate-600 dark:text-slate-300",
    border: "border-slate-500/25",
  },
  2: {
    bg: "bg-amber-500/10",
    text: "text-amber-700 dark:text-amber-300",
    border: "border-amber-500/30",
  },
  3: {
    bg: "bg-purple-500/15",
    text: "text-purple-700 dark:text-purple-300",
    border: "border-purple-500/40",
    glow: "shadow-[0_0_8px_hsl(270,60%,60%,0.25)]",
  },
};

function inferQueryType(text: string): QueryType {
  const normalized = text.toLowerCase();

  if (normalized.includes("crew") || normalized.includes("hazard") || normalized.includes("risk")) {
    return "safety";
  }

  if (normalized.includes("dependency") || normalized.includes("downstream") || normalized.includes("graph")) {
    return "network";
  }

  if (normalized.includes("mel") || normalized.includes("techlog") || normalized.includes("dispatch")) {
    return "maintenance";
  }

  if (normalized.includes("notam") || normalized.includes("regulatory") || normalized.includes("ad ")) {
    return "compliance";
  }

  return "ops-live";
}

const DEFAULT_VISIBLE_COUNT = 3;

export function FollowUpChips({ suggestions, onSelect, isVisible }: FollowUpChipsProps) {
  const [isHydrated, setIsHydrated] = useState(false);
  const [expanded, setExpanded] = useState(false);
  useEffect(() => setIsHydrated(true), []);

  if (!isVisible || suggestions.length === 0) return null;

  const normalizedSuggestions: FollowUpSuggestion[] = suggestions.map((suggestion) => {
    if (typeof suggestion === "string") {
      return {
        text: suggestion,
        type: inferQueryType(suggestion),
        sources: 1,
      };
    }

    return suggestion;
  });

  const visibleSuggestions = normalizedSuggestions.slice(0, DEFAULT_VISIBLE_COUNT);
  const overflowSuggestions = normalizedSuggestions.slice(DEFAULT_VISIBLE_COUNT);
  const hasOverflow = overflowSuggestions.length > 0;

  const Container = isHydrated ? motion.div : "div";
  const containerProps = isHydrated
    ? { initial: { opacity: 0, y: 10 }, animate: { opacity: 1, y: 0 }, exit: { opacity: 0, y: 10 } }
    : {};

  function renderChip(suggestion: FollowUpSuggestion, index: number) {
    const Icon = iconMap[suggestion.type];
    const tier = getComplexityTier(suggestion.sources ?? 1);
    const palette = tierColorMap[tier];
    const Chip = isHydrated ? motion.div : "div";
    const chipProps = isHydrated
      ? { initial: { opacity: 0, scale: 0.94 }, animate: { opacity: 1, scale: 1 }, transition: { delay: index * 0.04 } }
      : {};
    return (
      <Chip key={suggestion.text} {...chipProps}>
        <Button
          variant="outline"
          size="sm"
          onClick={() => {
            console.log("[FollowUpChip] clicked:", suggestion.text);
            onSelect(suggestion.text);
          }}
          className={`h-auto border ${palette.border} ${palette.bg} ${palette.glow ?? ""} px-3 py-2 text-xs transition-all hover:-translate-y-0.5`}
        >
          <Icon className={`h-3.5 w-3.5 ${palette.text}`} />
          <span className="text-left">{suggestion.text}</span>
        </Button>
      </Chip>
    );
  }

  return (
    <Container {...containerProps} className="px-4 pb-3">
      <div className="mx-auto max-w-5xl">
        <div className="flex flex-wrap gap-2">
          {visibleSuggestions.map((s, i) => renderChip(s, i))}
          {hasOverflow && (
            <Button
              variant="ghost"
              size="sm"
              onClick={() => setExpanded((prev) => !prev)}
              className="h-auto px-2.5 py-2 text-xs text-muted-foreground hover:text-foreground transition-colors"
            >
              {expanded ? (
                <>Show less <ChevronUp className="ml-1 h-3 w-3" /></>
              ) : (
                <>+{overflowSuggestions.length} more <ChevronDown className="ml-1 h-3 w-3" /></>
              )}
            </Button>
          )}
        </div>
        <AnimatePresence initial={false}>
          {expanded && hasOverflow && (
            <motion.div
              initial={{ height: 0, opacity: 0 }}
              animate={{ height: "auto", opacity: 1 }}
              exit={{ height: 0, opacity: 0 }}
              transition={{ duration: 0.22, ease: "easeOut" }}
              className="overflow-hidden"
            >
              <div className="flex flex-wrap gap-2 pt-2">
                {overflowSuggestions.map((s, i) => renderChip(s, i + DEFAULT_VISIBLE_COUNT))}
              </div>
            </motion.div>
          )}
        </AnimatePresence>
      </div>
    </Container>
  );
}
