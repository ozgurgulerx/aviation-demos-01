"use client";

import { useState, useRef, useEffect, KeyboardEvent, useCallback } from "react";
import { motion, AnimatePresence } from "framer-motion";
import {
  Send,
  Loader2,
  Shield,
  ShieldCheck,
  ShieldX,
  XCircle,
  Scan,
  Fingerprint,
  RotateCcw,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { Textarea } from "@/components/ui/textarea";
import { cn } from "@/lib/utils";
import { useToast } from "@/hooks/use-toast";
import { PiiGuidanceDialog } from "./pii-guidance-dialog";
import { ReasoningStrip } from "./reasoning-strip";
import { startMockReasoningStream } from "@/lib/reasoning-stream";
import type { ReasoningSseEvent } from "@/types";

type PiiStatus = "idle" | "checking" | "passed" | "blocked";

// Format category names for display (defined outside component to avoid recreation)
const formatCategory = (category: string): string => {
  return category
    .replace(/([A-Z])/g, " $1")
    .replace(/^US\s/, "US ")
    .trim();
};

interface MessageComposerProps {
  onSubmit: (message: string) => void;
  isLoading: boolean;
  reasoningEvents?: ReasoningSseEvent[];
  disabled?: boolean;
}

export function MessageComposer({
  onSubmit,
  isLoading,
  reasoningEvents,
  disabled,
}: MessageComposerProps) {
  const [input, setInput] = useState("");
  const [piiStatus, setPiiStatus] = useState<PiiStatus>("idle");
  const [piiError, setPiiError] = useState<string | null>(null);
  const [detectedCategories, setDetectedCategories] = useState<string[]>([]);
  const [blockedMessage, setBlockedMessage] = useState<string | null>(null);
  const [mockReasoningEvents, setMockReasoningEvents] = useState<ReasoningSseEvent[]>([]);
  const [lastSubmittedPrompt, setLastSubmittedPrompt] = useState("");
  // Track hydration to prevent SSR/client mismatch with Framer Motion animations
  const [isHydrated, setIsHydrated] = useState(false);
  const textareaRef = useRef<HTMLTextAreaElement>(null);
  const passedTimeoutRef = useRef<NodeJS.Timeout | null>(null);
  const liveRegionRef = useRef<HTMLDivElement>(null);
  const { toast } = useToast();

  // Mark component as hydrated after mount to enable animations
  useEffect(() => {
    setIsHydrated(true);
  }, []);

  // Auto-resize textarea
  useEffect(() => {
    const textarea = textareaRef.current;
    if (textarea) {
      textarea.style.height = "auto";
      textarea.style.height = `${Math.min(textarea.scrollHeight, 200)}px`;
    }
  }, [input]);

  useEffect(() => {
    return () => {
      if (passedTimeoutRef.current) {
        clearTimeout(passedTimeoutRef.current);
      }
    };
  }, []);

  useEffect(() => {
    const hasLiveReasoning = (reasoningEvents?.length || 0) > 0;

    if (!isLoading) {
      setMockReasoningEvents((previous) => {
        if (!previous.length) {
          return previous;
        }
        const completed = previous.some((event) => event.stage === "evidence_check_complete");
        if (completed) {
          return previous;
        }
        return [
          ...previous,
          {
            type: "reasoning_stage",
            stage: "evidence_check_complete",
            ts: new Date().toISOString(),
            payload: {
              verification: "Partial",
              failOpen: true,
            },
          },
        ];
      });
      return;
    }

    if (hasLiveReasoning) {
      return;
    }

    setMockReasoningEvents([]);
    const stopMock = startMockReasoningStream({
      prompt: lastSubmittedPrompt || "Pilot brief request",
      onEvent: (event) => {
        setMockReasoningEvents((previous) => [...previous, event]);
      },
    });

    // TODO(unit): replace mock stream with backend-driven SSE reasoning events.
    return stopMock;
  }, [isLoading, lastSubmittedPrompt, reasoningEvents]);

  // Focus trap and accessibility announcement when blocked
  useEffect(() => {
    if (piiStatus === "blocked") {
      // Announce to screen readers
      if (liveRegionRef.current) {
        liveRegionRef.current.textContent = `Message blocked. Personal information detected: ${detectedCategories.map(c => formatCategory(c)).join(", ")}. Please remove personal information and try again.`;
      }
      // Keep focus on textarea for immediate editing
      textareaRef.current?.focus();
    } else if (piiStatus === "passed") {
      // Announce success to screen readers
      if (liveRegionRef.current) {
        liveRegionRef.current.textContent = "Security check passed. No personal information detected.";
      }
    }
  }, [piiStatus, detectedCategories]);

  // Restore blocked message handler
  const handleRestoreMessage = () => {
    if (blockedMessage) {
      setInput(blockedMessage);
      setPiiStatus("idle");
      setPiiError(null);
      setDetectedCategories([]);
      setBlockedMessage(null);
      textareaRef.current?.focus();
      toast({
        title: "Message restored",
        description: "Edit to remove personal information before sending",
      });
    }
  };

  const checkForPii = async (text: string): Promise<boolean> => {
    setPiiStatus("checking");
    setPiiError(null);
    setDetectedCategories([]);

    if (passedTimeoutRef.current) {
      clearTimeout(passedTimeoutRef.current);
    }

    try {
      const response = await fetch("/api/pii", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ text }),
      });

      const result = await response.json();

      if (result.blocked) {
        setPiiStatus("blocked");
        setPiiError(result.message);
        setDetectedCategories(result.detectedCategories || []);
        setBlockedMessage(text); // Store for potential restore
        return false;
      }

      setPiiStatus("passed");

      // Fade status back to idle after 1.5 seconds
      passedTimeoutRef.current = setTimeout(() => {
        setPiiStatus("idle");
      }, 1500);

      return true;
    } catch (error) {
      console.error("PII check failed:", error);
      setPiiStatus("idle");
      toast({
        variant: "warning",
        title: "Security check unavailable",
        description: "Message sent without PII verification",
      });
      return true;
    }
  };

  const handleSubmit = async () => {
    const trimmedInput = input.trim();
    if (!trimmedInput || isLoading || piiStatus === "checking" || disabled) return;

    // Check for PII before submitting
    const isClean = await checkForPii(trimmedInput);
    if (!isClean) return;

    setLastSubmittedPrompt(trimmedInput);
    setMockReasoningEvents([]);
    onSubmit(trimmedInput);
    setInput("");
    setPiiError(null);
    setDetectedCategories([]);

    // Reset textarea height
    if (textareaRef.current) {
      textareaRef.current.style.height = "auto";
    }
  };

  const handleKeyDown = (e: KeyboardEvent<HTMLTextAreaElement>) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      handleSubmit();
    }
  };

  const handleInputChange = (e: React.ChangeEvent<HTMLTextAreaElement>) => {
    setInput(e.target.value);
    // Reset to idle when user starts typing again after a block
    if (piiStatus === "blocked") {
      setPiiStatus("idle");
      setPiiError(null);
      setDetectedCategories([]);
      setBlockedMessage(null); // Clear stored message since user is editing
    }
  };

  const isSubmitDisabled =
    !input.trim() || isLoading || piiStatus === "checking" || disabled;

  const handleOpenAuditQueries = useCallback(() => {
    if (typeof window === "undefined") {
      return;
    }
    window.dispatchEvent(
      new CustomEvent("pilotbrief:audit-open", {
        detail: { tab: "queries" },
      })
    );
    // TODO(unit): verify AuditDrawer opens the Queries tab on this event.
  }, []);

  const displayedReasoningEvents =
    (reasoningEvents?.length || 0) > 0 ? reasoningEvents || [] : mockReasoningEvents;

  return (
    <div className="border-t border-border bg-surface-1 p-4 relative">
      {/* ========== PII BLOCKED - Inline Error Message ========== */}
      <AnimatePresence>
        {isHydrated && piiError && (
          <motion.div
            initial={{ opacity: 0, height: 0 }}
            animate={{ opacity: 1, height: "auto" }}
            exit={{ opacity: 0, height: 0 }}
            transition={{ duration: 0.2 }}
            className="mb-3 overflow-hidden"
          >
            <div className="p-3 rounded-lg bg-red-500/10 border border-red-500/30">
              <div className="flex items-start gap-3">
                <ShieldX className="h-4 w-4 text-red-500 mt-0.5 shrink-0" />
                <div className="flex-1 min-w-0">
                  <p className="text-red-600 dark:text-red-400 font-semibold text-sm">
                    Message Blocked - PII Detected
                  </p>
                  <p className="text-red-600/80 dark:text-red-400/80 text-xs mt-1">
                    {piiError}
                  </p>
                  {detectedCategories.length > 0 && (
                    <div className="flex flex-wrap gap-1.5 mt-2">
                      {detectedCategories.map((category) => (
                        <span
                          key={category}
                          className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full bg-red-500/15 border border-red-500/30 text-red-600 dark:text-red-400 text-[11px] font-medium"
                        >
                          <Fingerprint className="h-2.5 w-2.5" />
                          {formatCategory(category)}
                        </span>
                      ))}
                    </div>
                  )}
                  {blockedMessage && (
                    <button
                      onClick={handleRestoreMessage}
                      className="mt-2 inline-flex items-center gap-1.5 px-2.5 py-1 rounded-md bg-slate-100 dark:bg-slate-800 hover:bg-slate-200 dark:hover:bg-slate-700 text-slate-700 dark:text-slate-300 text-xs font-medium transition-colors border border-slate-300 dark:border-slate-600"
                    >
                      <RotateCcw className="h-3 w-3" />
                      Restore message to edit
                    </button>
                  )}
                </div>
              </div>
            </div>
          </motion.div>
        )}
      </AnimatePresence>

      {/* ========== INPUT AREA ========== */}
      <div className="flex gap-3 items-end relative z-20">
        <div className="flex-1 relative">
          <Textarea
            ref={textareaRef}
            value={input}
            onChange={handleInputChange}
            onKeyDown={handleKeyDown}
            placeholder="Ask for a flight risk brief, crew legality scan, or disruption impact analysis..."
            disabled={disabled || isLoading || piiStatus === "checking"}
            className={cn(
              "min-h-[52px] max-h-[200px] py-3.5 pr-36 resize-none transition-all duration-300 text-base",
              piiStatus === "blocked" && "border-red-500/60 focus-visible:ring-red-500/50 bg-red-500/5",
              piiStatus === "passed" && "border-emerald-500/60 focus-visible:ring-emerald-500/50 bg-emerald-500/5",
              piiStatus === "checking" && "border-amber-500/60 focus-visible:ring-amber-500/50 bg-amber-500/5"
            )}
            rows={1}
          />

          {/* Security Status Badge - Inside textarea */}
          <div className="absolute right-3 bottom-3">
            <div
              className={cn(
                "flex items-center gap-1.5 px-2.5 py-1 rounded-full text-[11px] font-semibold transition-colors duration-300",
                piiStatus === "idle" && "bg-slate-100 dark:bg-slate-800 text-slate-500 dark:text-slate-400",
                piiStatus === "checking" && "bg-amber-100 dark:bg-amber-900/50 text-amber-600 dark:text-amber-400",
                piiStatus === "passed" && "bg-emerald-100 dark:bg-emerald-900/50 text-emerald-600 dark:text-emerald-400",
                piiStatus === "blocked" && "bg-red-100 dark:bg-red-900/50 text-red-600 dark:text-red-400"
              )}
            >
              {piiStatus === "idle" && <Shield className="h-3 w-3" />}
              {piiStatus === "checking" && <Scan className="h-3 w-3 animate-spin" />}
              {piiStatus === "passed" && <ShieldCheck className="h-3 w-3" />}
              {piiStatus === "blocked" && <ShieldX className="h-3 w-3" />}
              <span>
                {piiStatus === "idle" && "PII Protected"}
                {piiStatus === "checking" && "Scanning..."}
                {piiStatus === "passed" && "Secure"}
                {piiStatus === "blocked" && "Blocked"}
              </span>
            </div>
          </div>
        </div>

        {/* Submit Button */}
        <Button
          onClick={handleSubmit}
          disabled={isSubmitDisabled}
          size="icon"
          className={cn(
            "h-[52px] w-[52px] shrink-0 transition-all duration-300 rounded-xl",
            piiStatus === "idle" && "bg-primary hover:bg-primary/90",
            piiStatus === "passed" && "bg-emerald-500 hover:bg-emerald-600",
            piiStatus === "blocked" && "bg-red-500 hover:bg-red-600",
            piiStatus === "checking" && "bg-amber-500 hover:bg-amber-600"
          )}
        >
          {isLoading ? (
            <Loader2 className="h-5 w-5 animate-spin text-white" />
          ) : piiStatus === "checking" ? (
            <Scan className="h-5 w-5 animate-spin text-white" />
          ) : piiStatus === "blocked" ? (
            <XCircle className="h-5 w-5 text-white" />
          ) : (
            <Send className="h-5 w-5 text-white" />
          )}
        </Button>
      </div>

      <ReasoningStrip
        events={displayedReasoningEvents}
        isLoading={isLoading}
        onOpenQueries={handleOpenAuditQueries}
      />

      {/* Helper text with PII guidance link */}
      <div className="flex items-center justify-between mt-3">
        <p className="text-xs text-muted-foreground">
          {piiStatus === "blocked"
            ? "Remove personal information and try again"
            : "Enter to send \u00b7 Shift+Enter for new line"}
        </p>
        <PiiGuidanceDialog />
      </div>

      {/* Accessibility Live Region - announces PII status to screen readers */}
      <div
        ref={liveRegionRef}
        role="status"
        aria-live="polite"
        aria-atomic="true"
        className="sr-only"
      />
    </div>
  );
}
