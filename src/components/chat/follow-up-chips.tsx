"use client";

import { motion } from "framer-motion";
import {
  Sparkles,
  Radar,
  ShieldAlert,
  Workflow,
  Wrench,
  BookCheck,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import type { QueryType } from "@/data/seed";

interface FollowUpSuggestion {
  text: string;
  type: QueryType;
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

const colorMap: Record<QueryType, { bg: string; text: string; border: string }> = {
  "ops-live": {
    bg: "bg-primary/10",
    text: "text-primary",
    border: "border-primary/30",
  },
  safety: {
    bg: "bg-teal-500/10",
    text: "text-teal-700 dark:text-teal-300",
    border: "border-teal-500/30",
  },
  network: {
    bg: "bg-blue-500/10",
    text: "text-blue-700 dark:text-blue-300",
    border: "border-blue-500/30",
  },
  maintenance: {
    bg: "bg-orange-500/10",
    text: "text-orange-700 dark:text-orange-300",
    border: "border-orange-500/30",
  },
  compliance: {
    bg: "bg-cyan-500/10",
    text: "text-cyan-700 dark:text-cyan-300",
    border: "border-cyan-500/30",
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

export function FollowUpChips({ suggestions, onSelect, isVisible }: FollowUpChipsProps) {
  if (!isVisible || suggestions.length === 0) return null;

  const normalizedSuggestions: FollowUpSuggestion[] = suggestions.map((suggestion) => {
    if (typeof suggestion === "string") {
      return {
        text: suggestion,
        type: inferQueryType(suggestion),
      };
    }

    return suggestion;
  });

  return (
    <motion.div
      initial={{ opacity: 0, y: 10 }}
      animate={{ opacity: 1, y: 0 }}
      exit={{ opacity: 0, y: 10 }}
      className="px-4 pb-4"
    >
      <div className="mx-auto max-w-5xl rounded-xl border border-border bg-card p-3">
        <div className="mb-3 flex items-center gap-2">
          <Sparkles className="h-3.5 w-3.5 text-gold" />
          <span className="text-xs font-medium text-muted-foreground">
            Suggested follow-up briefs
          </span>
        </div>

        <div className="flex flex-wrap gap-2">
          {normalizedSuggestions.map((suggestion, index) => {
            const Icon = iconMap[suggestion.type];
            const palette = colorMap[suggestion.type];
            return (
              <motion.div
                key={suggestion.text}
                initial={{ opacity: 0, scale: 0.94 }}
                animate={{ opacity: 1, scale: 1 }}
                transition={{ delay: index * 0.04 }}
              >
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => onSelect(suggestion.text)}
                  className={`h-auto border ${palette.border} ${palette.bg} px-3 py-2 text-xs transition-all hover:-translate-y-0.5`}
                >
                  <Icon className={`h-3.5 w-3.5 ${palette.text}`} />
                  <span className="text-left">{suggestion.text}</span>
                </Button>
              </motion.div>
            );
          })}
        </div>
      </div>
    </motion.div>
  );
}
