"use client";

import { useState, useRef, useEffect, KeyboardEvent, useCallback } from "react";
import { motion, AnimatePresence } from "framer-motion";
import {
  Send,
  Loader2,
  Shield,
  ShieldCheck,
  ShieldX,
  AlertTriangle,
  CheckCircle2,
  XCircle,
  Scan,
  Lock,
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
  const [showPassedBanner, setShowPassedBanner] = useState(false);
  const [blockedMessage, setBlockedMessage] = useState<string | null>(null); // Store blocked message for restore
  const [mockReasoningEvents, setMockReasoningEvents] = useState<ReasoningSseEvent[]>([]);
  const [lastSubmittedPrompt, setLastSubmittedPrompt] = useState("");
  // Track hydration to prevent SSR/client mismatch with Framer Motion animations
  const [isHydrated, setIsHydrated] = useState(false);
  const textareaRef = useRef<HTMLTextAreaElement>(null);
  const passedTimeoutRef = useRef<NodeJS.Timeout | null>(null);
  const bannerTimeoutRef = useRef<NodeJS.Timeout | null>(null);
  const liveRegionRef = useRef<HTMLDivElement>(null); // For accessibility announcements
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

  // Cleanup timeouts on unmount
  useEffect(() => {
    return () => {
      if (passedTimeoutRef.current) {
        clearTimeout(passedTimeoutRef.current);
      }
      if (bannerTimeoutRef.current) {
        clearTimeout(bannerTimeoutRef.current);
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
    setShowPassedBanner(false);

    // Clear any existing timeouts
    if (passedTimeoutRef.current) {
      clearTimeout(passedTimeoutRef.current);
    }
    if (bannerTimeoutRef.current) {
      clearTimeout(bannerTimeoutRef.current);
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

      // PII check passed - show green status and banner
      setPiiStatus("passed");
      setShowPassedBanner(true);

      // Hide banner after 2 seconds
      bannerTimeoutRef.current = setTimeout(() => {
        setShowPassedBanner(false);
      }, 2000);

      // Fade status back to idle after 3 seconds
      passedTimeoutRef.current = setTimeout(() => {
        setPiiStatus("idle");
      }, 3000);

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
    <div className="border-t border-border bg-surface-1 p-4 relative overflow-hidden">
      {/* ========== SCANNING STATE - Full Width Overlay ========== */}
      <AnimatePresence>
        {isHydrated && piiStatus === "checking" && (
          <motion.div
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            className="absolute inset-0 z-30 pointer-events-none"
          >
            {/* Scanning gradient background */}
            <div className="absolute inset-0 bg-gradient-to-r from-amber-500/5 via-amber-500/10 to-amber-500/5" />

            {/* Scanning line animation */}
            <motion.div
              className="absolute left-0 right-0 h-1 bg-gradient-to-r from-transparent via-amber-500 to-transparent"
              initial={{ top: 0 }}
              animate={{ top: ["0%", "100%", "0%"] }}
              transition={{ duration: 1.5, repeat: Infinity, ease: "easeInOut" }}
            />

            {/* Scanning border pulse */}
            <motion.div
              className="absolute inset-0 border-2 border-amber-500/50 rounded-lg"
              animate={{
                borderColor: ["rgba(245, 158, 11, 0.3)", "rgba(245, 158, 11, 0.7)", "rgba(245, 158, 11, 0.3)"],
              }}
              transition={{ duration: 1, repeat: Infinity }}
            />
          </motion.div>
        )}
      </AnimatePresence>

      {/* ========== SUCCESS STATE - Green Flash & Banner ========== */}
      <AnimatePresence>
        {isHydrated && piiStatus === "passed" && (
          <>
            {/* Full overlay green flash */}
            <motion.div
              initial={{ opacity: 0 }}
              animate={{ opacity: [0, 1, 0.5] }}
              exit={{ opacity: 0 }}
              transition={{ duration: 0.5 }}
              className="absolute inset-0 z-30 pointer-events-none"
            >
              <div className="absolute inset-0 bg-gradient-to-r from-emerald-500/20 via-emerald-500/30 to-emerald-500/20" />
              <motion.div
                className="absolute inset-0 border-2 border-emerald-500 rounded-lg"
                initial={{ opacity: 0 }}
                animate={{ opacity: [0, 1, 0.6] }}
                transition={{ duration: 0.4 }}
              />
            </motion.div>

            {/* Corner checkmarks */}
            <motion.div
              initial={{ scale: 0, opacity: 0 }}
              animate={{ scale: 1, opacity: 1 }}
              exit={{ scale: 0, opacity: 0 }}
              transition={{ type: "spring", stiffness: 500, damping: 20 }}
              className="absolute top-2 right-2 z-40"
            >
              <div className="bg-emerald-500 rounded-full p-1.5 shadow-lg shadow-emerald-500/50">
                <CheckCircle2 className="h-4 w-4 text-white" />
              </div>
            </motion.div>
          </>
        )}
      </AnimatePresence>

      {/* ========== SUCCESS BANNER ========== */}
      <AnimatePresence>
        {isHydrated && showPassedBanner && (
          <motion.div
            initial={{ opacity: 0, y: -30, scale: 0.95 }}
            animate={{ opacity: 1, y: 0, scale: 1 }}
            exit={{ opacity: 0, y: -20, scale: 0.95 }}
            transition={{ type: "spring", stiffness: 400, damping: 25 }}
            className="mb-4 relative z-40"
          >
            <div className="flex items-center justify-center gap-3 p-3 rounded-xl bg-gradient-to-r from-emerald-500/20 via-emerald-500/25 to-emerald-500/20 border-2 border-emerald-500/50 shadow-lg shadow-emerald-500/20">
              <motion.div
                initial={{ scale: 0, rotate: -180 }}
                animate={{ scale: 1, rotate: 0 }}
                transition={{ type: "spring", stiffness: 400, damping: 15, delay: 0.1 }}
              >
                <div className="bg-emerald-500 rounded-full p-2">
                  <ShieldCheck className="h-5 w-5 text-white" />
                </div>
              </motion.div>
              <div className="flex flex-col">
                <motion.span
                  initial={{ opacity: 0, x: -10 }}
                  animate={{ opacity: 1, x: 0 }}
                  transition={{ delay: 0.15 }}
                  className="text-emerald-600 dark:text-emerald-400 font-semibold text-sm"
                >
                  Security Check Passed
                </motion.span>
                <motion.span
                  initial={{ opacity: 0, x: -10 }}
                  animate={{ opacity: 1, x: 0 }}
                  transition={{ delay: 0.2 }}
                  className="text-emerald-600/80 dark:text-emerald-400/80 text-xs"
                >
                  No personal information detected
                </motion.span>
              </div>
              <motion.div
                initial={{ scale: 0 }}
                animate={{ scale: [0, 1.2, 1] }}
                transition={{ delay: 0.25, duration: 0.3 }}
              >
                <Lock className="h-4 w-4 text-emerald-500" />
              </motion.div>
            </div>
          </motion.div>
        )}
      </AnimatePresence>

      {/* ========== BLOCKED STATE - Red Flash & Error Banner ========== */}
      <AnimatePresence>
        {isHydrated && piiStatus === "blocked" && (
          <>
            {/* Full overlay red flash */}
            <motion.div
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              className="absolute inset-0 z-30 pointer-events-none"
            >
              <motion.div
                className="absolute inset-0 bg-gradient-to-r from-red-500/15 via-red-500/25 to-red-500/15"
                animate={{ opacity: [0.5, 1, 0.7] }}
                transition={{ duration: 0.3 }}
              />
              <motion.div
                className="absolute inset-0 border-2 border-red-500 rounded-lg"
                animate={{
                  boxShadow: [
                    "0 0 0 0 rgba(239, 68, 68, 0)",
                    "0 0 30px 5px rgba(239, 68, 68, 0.4)",
                    "0 0 15px 2px rgba(239, 68, 68, 0.3)"
                  ]
                }}
                transition={{ duration: 0.5 }}
              />
            </motion.div>

            {/* Corner X mark */}
            <motion.div
              initial={{ scale: 0, opacity: 0 }}
              animate={{ scale: 1, opacity: 1 }}
              exit={{ scale: 0, opacity: 0 }}
              transition={{ type: "spring", stiffness: 500, damping: 20 }}
              className="absolute top-2 right-2 z-40"
            >
              <motion.div
                className="bg-red-500 rounded-full p-1.5 shadow-lg shadow-red-500/50"
                animate={{ rotate: [0, -10, 10, -10, 10, 0] }}
                transition={{ duration: 0.5, delay: 0.2 }}
              >
                <XCircle className="h-4 w-4 text-white" />
              </motion.div>
            </motion.div>
          </>
        )}
      </AnimatePresence>

      {/* ========== ERROR BANNER ========== */}
      <AnimatePresence>
        {isHydrated && piiError && (
          <motion.div
            initial={{ opacity: 0, y: -20, scale: 0.95 }}
            animate={{ opacity: 1, y: 0, scale: 1 }}
            exit={{ opacity: 0, y: -10, scale: 0.95 }}
            className="mb-4 relative z-40"
          >
            <motion.div
              className="p-4 rounded-xl bg-gradient-to-r from-red-500/15 via-red-500/20 to-red-500/15 border-2 border-red-500/50 shadow-lg shadow-red-500/20"
              animate={{ x: [0, -8, 8, -8, 8, 0] }}
              transition={{ duration: 0.5, delay: 0.1 }}
            >
              <div className="flex items-start gap-3">
                <motion.div
                  initial={{ scale: 0 }}
                  animate={{ scale: 1 }}
                  transition={{ type: "spring", stiffness: 500, damping: 15 }}
                >
                  <div className="bg-red-500 rounded-full p-2 mt-0.5">
                    <ShieldX className="h-5 w-5 text-white" />
                  </div>
                </motion.div>
                <div className="flex-1 min-w-0">
                  <motion.div
                    initial={{ opacity: 0, x: -10 }}
                    animate={{ opacity: 1, x: 0 }}
                    transition={{ delay: 0.15 }}
                  >
                    <h4 className="text-red-600 dark:text-red-400 font-bold text-base flex items-center gap-2">
                      <AlertTriangle className="h-4 w-4" />
                      Message Blocked - PII Detected
                    </h4>
                  </motion.div>
                  <motion.p
                    initial={{ opacity: 0, x: -10 }}
                    animate={{ opacity: 1, x: 0 }}
                    transition={{ delay: 0.2 }}
                    className="text-red-600/90 dark:text-red-400/90 text-sm mt-1"
                  >
                    {piiError}
                  </motion.p>
                  {detectedCategories.length > 0 && (
                    <motion.div
                      initial={{ opacity: 0, y: 10 }}
                      animate={{ opacity: 1, y: 0 }}
                      transition={{ delay: 0.3 }}
                      className="flex flex-wrap gap-2 mt-3"
                    >
                      {detectedCategories.map((category, idx) => (
                        <motion.span
                          key={category}
                          initial={{ opacity: 0, scale: 0.8 }}
                          animate={{ opacity: 1, scale: 1 }}
                          transition={{ delay: 0.35 + idx * 0.1 }}
                          className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full bg-red-500/20 border border-red-500/40 text-red-600 dark:text-red-400 text-xs font-medium"
                        >
                          <Fingerprint className="h-3 w-3" />
                          {formatCategory(category)}
                        </motion.span>
                      ))}
                    </motion.div>
                  )}

                  {/* Restore Message Button */}
                  {blockedMessage && (
                    <motion.button
                      initial={{ opacity: 0, y: 10 }}
                      animate={{ opacity: 1, y: 0 }}
                      transition={{ delay: 0.5 }}
                      onClick={handleRestoreMessage}
                      className="mt-3 inline-flex items-center gap-2 px-3 py-1.5 rounded-lg bg-slate-100 dark:bg-slate-800 hover:bg-slate-200 dark:hover:bg-slate-700 text-slate-700 dark:text-slate-300 text-xs font-medium transition-colors border border-slate-300 dark:border-slate-600"
                    >
                      <RotateCcw className="h-3.5 w-3.5" />
                      Restore message to edit
                    </motion.button>
                  )}
                </div>

                {/* Persistent Pulsing Reminder */}
                <motion.div
                  initial={{ opacity: 0, scale: 0.8 }}
                  animate={{ opacity: 1, scale: 1 }}
                  transition={{ delay: 0.4 }}
                  className="flex-shrink-0 self-start"
                >
                  <motion.div
                    className="relative"
                    animate={{ scale: [1, 1.05, 1] }}
                    transition={{ duration: 2, repeat: Infinity, ease: "easeInOut" }}
                  >
                    <div className="absolute inset-0 bg-red-500 rounded-full blur-md opacity-40" />
                    <motion.div
                      className="relative bg-red-500 rounded-full p-2"
                      animate={{
                        boxShadow: [
                          "0 0 0 0 rgba(239, 68, 68, 0)",
                          "0 0 0 8px rgba(239, 68, 68, 0.3)",
                          "0 0 0 0 rgba(239, 68, 68, 0)"
                        ]
                      }}
                      transition={{ duration: 2, repeat: Infinity }}
                    >
                      <AlertTriangle className="h-5 w-5 text-white" />
                    </motion.div>
                  </motion.div>
                </motion.div>
              </div>
            </motion.div>
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
            placeholder="Ask about fund data..."
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
            {/* Static badge before hydration to prevent mismatch */}
            {!isHydrated && (
              <div className="flex items-center gap-2 px-3 py-1.5 rounded-full text-xs font-semibold bg-slate-100 dark:bg-slate-800 text-slate-500 dark:text-slate-400">
                <Shield className="h-3.5 w-3.5" />
                <span>PII Protected</span>
              </div>
            )}
            {/* Animated badge after hydration */}
            {isHydrated && (
              <AnimatePresence mode="wait">
                <motion.div
                  key={piiStatus}
                  initial={{ opacity: 0, scale: 0.8, y: 5 }}
                  animate={{ opacity: 1, scale: 1, y: 0 }}
                  exit={{ opacity: 0, scale: 0.8, y: -5 }}
                  transition={{ duration: 0.2 }}
                  className={cn(
                    "flex items-center gap-2 px-3 py-1.5 rounded-full text-xs font-semibold transition-all",
                    piiStatus === "idle" && "bg-slate-100 dark:bg-slate-800 text-slate-500 dark:text-slate-400",
                    piiStatus === "checking" && "bg-amber-100 dark:bg-amber-900/50 text-amber-600 dark:text-amber-400",
                    piiStatus === "passed" && "bg-emerald-100 dark:bg-emerald-900/50 text-emerald-600 dark:text-emerald-400",
                    piiStatus === "blocked" && "bg-red-100 dark:bg-red-900/50 text-red-600 dark:text-red-400"
                  )}
                >
                  {piiStatus === "idle" && (
                    <>
                      <Shield className="h-3.5 w-3.5" />
                      <span>PII Protected</span>
                    </>
                  )}
                  {piiStatus === "checking" && (
                    <>
                      <motion.div
                        animate={{ rotate: 360 }}
                        transition={{ duration: 1, repeat: Infinity, ease: "linear" }}
                      >
                        <Scan className="h-3.5 w-3.5" />
                      </motion.div>
                      <span>Scanning...</span>
                    </>
                  )}
                  {piiStatus === "passed" && (
                    <>
                      <motion.div
                        initial={{ scale: 0 }}
                        animate={{ scale: [0, 1.3, 1] }}
                        transition={{ duration: 0.3 }}
                      >
                        <ShieldCheck className="h-3.5 w-3.5" />
                      </motion.div>
                      <span>Secure</span>
                      <motion.div
                        initial={{ scale: 0 }}
                        animate={{ scale: 1 }}
                        transition={{ delay: 0.1, type: "spring" }}
                      >
                        <CheckCircle2 className="h-3.5 w-3.5" />
                      </motion.div>
                    </>
                  )}
                  {piiStatus === "blocked" && (
                    <>
                      <motion.div
                        animate={{ scale: [1, 1.2, 1] }}
                        transition={{ duration: 0.5, repeat: Infinity }}
                      >
                        <ShieldX className="h-3.5 w-3.5" />
                      </motion.div>
                      <span>Blocked</span>
                      <XCircle className="h-3.5 w-3.5" />
                    </>
                  )}
                </motion.div>
              </AnimatePresence>
            )}
          </div>
        </div>

        {/* Submit Button */}
        {!isHydrated ? (
          <div>
            <Button
              onClick={handleSubmit}
              disabled={isSubmitDisabled}
              size="icon"
              className="h-[52px] w-[52px] shrink-0 transition-all duration-300 rounded-xl bg-primary hover:bg-primary/90"
            >
              <Send className="h-5 w-5 text-white" />
            </Button>
          </div>
        ) : (
          <motion.div
            animate={piiStatus === "blocked" ? { x: [0, -5, 5, -5, 5, 0] } : {}}
            transition={{ duration: 0.4 }}
          >
            <Button
              onClick={handleSubmit}
              disabled={isSubmitDisabled}
              size="icon"
              className={cn(
                "h-[52px] w-[52px] shrink-0 transition-all duration-300 rounded-xl",
                piiStatus === "idle" && "bg-primary hover:bg-primary/90",
                piiStatus === "passed" && "bg-emerald-500 hover:bg-emerald-600 shadow-lg shadow-emerald-500/30",
                piiStatus === "blocked" && "bg-red-500 hover:bg-red-600 shadow-lg shadow-red-500/30",
                piiStatus === "checking" && "bg-amber-500 hover:bg-amber-600 shadow-lg shadow-amber-500/30"
              )}
            >
              {isLoading ? (
                <Loader2 className="h-5 w-5 animate-spin text-white" />
              ) : piiStatus === "checking" ? (
                <motion.div
                  animate={{ rotate: 360 }}
                  transition={{ duration: 1, repeat: Infinity, ease: "linear" }}
                >
                  <Scan className="h-5 w-5 text-white" />
                </motion.div>
              ) : piiStatus === "blocked" ? (
                <motion.div
                  animate={{ rotate: [0, -15, 15, -15, 15, 0] }}
                  transition={{ duration: 0.5 }}
                >
                  <XCircle className="h-5 w-5 text-white" />
                </motion.div>
              ) : piiStatus === "passed" ? (
                <motion.div
                  initial={{ scale: 0, rotate: -180 }}
                  animate={{ scale: 1, rotate: 0 }}
                  transition={{ type: "spring", stiffness: 400, damping: 15 }}
                >
                  <CheckCircle2 className="h-5 w-5 text-white" />
                </motion.div>
              ) : (
                <Send className="h-5 w-5 text-white" />
              )}
            </Button>
          </motion.div>
        )}
      </div>

      <ReasoningStrip
        events={displayedReasoningEvents}
        isLoading={isLoading}
        onOpenQueries={handleOpenAuditQueries}
      />

      {/* Helper text with PII guidance link */}
      <div className="flex items-center justify-between mt-3">
        {!isHydrated ? (
          <p className="text-xs flex items-center gap-1.5 text-muted-foreground">
            <Shield className="h-3.5 w-3.5" />
            <span>Press Enter to send. Personal information (SSN, credit cards, etc.) is automatically blocked.</span>
          </p>
        ) : (
          <motion.p
            className={cn(
              "text-xs flex items-center gap-1.5 transition-colors duration-300",
              piiStatus === "blocked" ? "text-red-500" : "text-muted-foreground"
            )}
            animate={piiStatus === "blocked" ? { x: [0, -2, 2, -2, 2, 0] } : {}}
            transition={{ duration: 0.3 }}
          >
            <Shield className="h-3.5 w-3.5" />
            <span>
              {piiStatus === "blocked"
                ? "Please remove personal information and try again"
                : "Press Enter to send. Personal information (SSN, credit cards, etc.) is automatically blocked."
              }
            </span>
          </motion.p>
        )}
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
