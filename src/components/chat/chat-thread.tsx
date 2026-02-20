"use client";

import { useRef, useEffect, useState } from "react";
import { motion, AnimatePresence, useReducedMotion } from "framer-motion";
import {
  Loader2,
  Sparkles,
  Radar,
  ShieldAlert,
  Workflow,
  Wrench,
  BookCheck,
} from "lucide-react";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Message } from "./message";
import { QUERY_CATEGORIES, type QueryType } from "@/data/seed";
import type { Message as MessageType } from "@/types";
import { motionTokens } from "@/lib/motion";

interface ChatThreadProps {
  messages: MessageType[];
  isLoading: boolean;
  streamingContent?: string;
  onCitationClick?: (id: number) => void;
  activeCitationId?: number | null;
  onSpeakMessage?: (messageId: string, content: string) => void;
  speakingMessageId?: string | null;
  voiceEnabled?: boolean;
  voiceStatuses?: Record<string, "idle" | "preparing" | "ready" | "error">;
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
  onCitationClick,
  activeCitationId,
  onSpeakMessage,
  speakingMessageId,
  voiceEnabled = true,
  voiceStatuses = {},
  onSendMessage,
}: ChatThreadProps) {
  const scrollRef = useRef<HTMLDivElement>(null);
  const bottomRef = useRef<HTMLDivElement>(null);
  const [expandedCategory, setExpandedCategory] = useState<QueryType | null>(null);
  const reducedMotion = useReducedMotion();

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: reducedMotion ? "auto" : "smooth" });
  }, [messages, streamingContent, reducedMotion]);

  return (
    <ScrollArea className="flex-1 min-h-0" ref={scrollRef}>
      <div className="mx-auto max-w-5xl space-y-4 px-4 py-5">
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
                        initial={reducedMotion ? false : { opacity: 0, height: 0 }}
                        animate={{ opacity: 1, height: "auto" }}
                        exit={reducedMotion ? { opacity: 1, height: "auto" } : { opacity: 0, height: 0 }}
                        transition={{ duration: reducedMotion ? 0 : motionTokens.state, ease: motionTokens.easeOut }}
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
                onSpeakMessage={onSpeakMessage}
                isSpeaking={speakingMessageId === message.id}
                voiceEnabled={voiceEnabled}
                voiceStatus={voiceStatuses[message.id] || "idle"}
              />
            ))}

            {isLoading && streamingContent && (
              <motion.div
                initial={reducedMotion ? false : { opacity: 0, y: 10 }}
                animate={{ opacity: 1, y: 0 }}
                transition={{ duration: reducedMotion ? 0 : motionTokens.state, ease: motionTokens.easeOut }}
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
                initial={reducedMotion ? false : { opacity: 0, y: 10 }}
                animate={{ opacity: 1, y: 0 }}
                transition={{ duration: reducedMotion ? 0 : motionTokens.state, ease: motionTokens.easeOut }}
                className="flex gap-3 py-4"
              >
                <div className="flex h-8 w-8 items-center justify-center rounded-full bg-primary/10">
                  <Loader2 className="h-4 w-4 animate-spin text-primary" />
                </div>
                <div className="max-w-[88%] flex-1 rounded-xl border border-border bg-card px-4 py-3 text-sm text-muted-foreground">
                  Analyzing intent, retrieval path, and evidence...
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
