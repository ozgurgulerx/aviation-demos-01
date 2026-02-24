"use client";

import { useState, useEffect } from "react";
import { motion, AnimatePresence, useReducedMotion } from "framer-motion";
import {
  Loader2,
  Sparkles,
  Bot,
  AlertTriangle,
  CheckCircle2,
  Volume2,
  Square,
  RotateCcw,
} from "lucide-react";
import { Avatar, AvatarFallback } from "@/components/ui/avatar";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { MarkdownContent } from "./markdown-content";
import { cn } from "@/lib/utils";
import type { Message as MessageType, MessageStatus } from "@/types";

interface AnswerFrameProps {
  message: MessageType;
  streamingContent?: string;
  currentReasoningDetail?: string;
  loadingElapsedMs?: number;
  onCitationClick?: (id: number) => void;
  activeCitationId?: number | null;
  onSpeakMessage?: (messageId: string, content: string) => void;
  isSpeaking?: boolean;
  voiceEnabled?: boolean;
  voiceStatus?: "idle" | "preparing" | "ready" | "error";
}

export function AnswerFrame({
  message,
  streamingContent,
  currentReasoningDetail,
  loadingElapsedMs = 0,
  onCitationClick,
  activeCitationId,
  onSpeakMessage,
  isSpeaking = false,
  voiceEnabled = true,
  voiceStatus = "idle",
}: AnswerFrameProps) {
  const status: MessageStatus = message.status || "complete";
  const reducedMotion = useReducedMotion();

  // Prevent hydration mismatch with Framer Motion
  const [isHydrated, setIsHydrated] = useState(false);
  useEffect(() => {
    setIsHydrated(true);
  }, []);

  const Container = isHydrated ? motion.div : "div";
  const containerProps = isHydrated
    ? {
        initial: reducedMotion ? false : { opacity: 0, y: 10 },
        animate: { opacity: 1, y: 0 },
        transition: { duration: reducedMotion ? 0 : 0.2 },
      }
    : {};

  // Avatar icon based on status
  const AvatarIcon =
    status === "loading"
      ? Loader2
      : status === "streaming"
        ? Sparkles
        : status === "error"
          ? AlertTriangle
          : Bot;

  const avatarIconClass = cn(
    "h-4 w-4",
    status === "loading" && "animate-spin text-primary",
    status === "streaming" && "animate-pulse text-primary",
    status === "error" && "text-destructive",
    status === "complete" && "text-muted-foreground",
  );

  const canPressVoiceButton =
    !!onSpeakMessage && voiceEnabled && (isSpeaking || voiceStatus === "ready" || voiceStatus === "error");
  const voiceLabel = isSpeaking
    ? "Stop audio"
    : voiceStatus === "ready"
      ? "Play voice"
      : voiceStatus === "error"
        ? "Retry voice"
        : "Preparing voice...";
  const VoiceIcon = isSpeaking
    ? Square
    : voiceStatus === "error"
      ? RotateCcw
      : voiceStatus === "ready"
        ? Volume2
        : Loader2;

  return (
    <Container
      {...containerProps}
      className="flex gap-3 py-4 flex-row"
    >
      {/* Avatar */}
      <Avatar className="h-8 w-8 shrink-0">
        <AvatarFallback className="bg-surface-3 text-muted-foreground">
          <AvatarIcon className={avatarIconClass} />
        </AvatarFallback>
      </Avatar>

      {/* Content */}
      <div className="flex-1 space-y-2 max-w-[85%]">
        {/* Message bubble */}
        <div
          className={cn(
            "rounded-xl px-4 py-3",
            status === "error"
              ? "bg-destructive/10 border border-destructive/30"
              : "bg-surface-2 border border-border",
          )}
        >
          {status === "loading" && (
            <div className="text-sm text-muted-foreground">
              <AnimatePresence mode="wait">
                <motion.span
                  key={currentReasoningDetail || "default"}
                  initial={reducedMotion ? false : { opacity: 0, y: 4 }}
                  animate={{ opacity: 1, y: 0 }}
                  exit={reducedMotion ? { opacity: 1 } : { opacity: 0, y: -4 }}
                  transition={{ duration: reducedMotion ? 0 : 0.15 }}
                >
                  {loadingElapsedMs >= 15000
                    ? "Retrieval is taking longer than usual. Generating a provisional brief from available evidence..."
                    : (currentReasoningDetail || "Analyzing intent, retrieval path, and evidence...")}
                </motion.span>
              </AnimatePresence>
            </div>
          )}

          {status === "streaming" && (
            <div className="markdown-content text-sm">
              {streamingContent || ""}
              <span className="ml-0.5 inline-block h-4 w-2 animate-pulse bg-primary/40" />
            </div>
          )}

          {status === "complete" && (
            <div className="markdown-content text-sm">
              <MarkdownContent
                content={message.content}
                onCitationClick={onCitationClick}
                activeCitationId={activeCitationId}
              />
            </div>
          )}

          {status === "error" && (
            <div className="text-sm text-destructive">
              {message.content || message.errorMessage || "An error occurred."}
            </div>
          )}
        </div>

        {/* Voice controls — complete state only */}
        {status === "complete" && voiceEnabled && (
          <div className="flex items-center gap-2">
            <Button
              size="sm"
              variant="ghost"
              className="h-6 px-2 text-[11px]"
              onClick={() => onSpeakMessage?.(message.id, message.content)}
              disabled={!canPressVoiceButton}
            >
              <VoiceIcon
                className={cn(
                  "h-3.5 w-3.5",
                  voiceStatus === "preparing" && "animate-spin"
                )}
              />
              {voiceLabel}
            </Button>
          </div>
        )}

        {/* Citation badges — complete state only */}
        {status === "complete" && message.citations && message.citations.length > 0 && (
          <div className="flex items-center gap-2">
            <Badge variant="success" className="text-xs gap-1">
              <CheckCircle2 className="h-3 w-3" />
              Verified
            </Badge>
            <span className="text-xs text-muted-foreground">
              {message.citations.length} source
              {message.citations.length > 1 ? "s" : ""}
            </span>
          </div>
        )}

        {/* Verified badge for Foundry IQ (inline citations) */}
        {status === "complete" && message.isVerified && (!message.citations || message.citations.length === 0) && (
          <Badge variant="success" className="text-xs gap-1">
            <CheckCircle2 className="h-3 w-3" />
            Verified
          </Badge>
        )}
      </div>
    </Container>
  );
}
