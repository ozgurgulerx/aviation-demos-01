"use client";

import { useState, useEffect } from "react";
import { motion } from "framer-motion";
import { User, Bot, CheckCircle2, Loader2, Volume2, Square, RotateCcw } from "lucide-react";
import { Avatar, AvatarFallback } from "@/components/ui/avatar";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { MarkdownContent } from "./markdown-content";
import { cn } from "@/lib/utils";
import type { Message as MessageType } from "@/types";

interface MessageProps {
  message: MessageType;
  onCitationClick?: (id: number) => void;
  activeCitationId?: number | null;
  onSpeakMessage?: (messageId: string, content: string) => void;
  isSpeaking?: boolean;
  voiceEnabled?: boolean;
  voiceStatus?: "idle" | "preparing" | "ready" | "error";
}

export function Message({
  message,
  onCitationClick,
  activeCitationId,
  onSpeakMessage,
  isSpeaking = false,
  voiceEnabled = true,
  voiceStatus = "idle",
}: MessageProps) {
  const isUser = message.role === "user";
  // Prevent hydration mismatch with Framer Motion
  const [isHydrated, setIsHydrated] = useState(false);

  useEffect(() => {
    setIsHydrated(true);
  }, []);

  // Use regular div during SSR, motion.div after hydration
  const Container = isHydrated ? motion.div : "div";
  const containerProps = isHydrated
    ? {
        initial: { opacity: 0, y: 10 },
        animate: { opacity: 1, y: 0 },
        transition: { duration: 0.2 },
      }
    : {};

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
      className={cn("flex gap-3 py-4", isUser ? "flex-row-reverse" : "flex-row")}
    >
      {/* Avatar */}
      <Avatar className={cn("h-8 w-8 shrink-0", isUser && "bg-gold/20")}>
        <AvatarFallback
          className={cn(
            "text-xs",
            isUser
              ? "bg-gold/20 text-gold"
              : "bg-surface-3 text-muted-foreground"
          )}
        >
          {isUser ? <User className="h-4 w-4" /> : <Bot className="h-4 w-4" />}
        </AvatarFallback>
      </Avatar>

      {/* Content */}
      <div
        className={cn(
          "flex-1 space-y-2 max-w-[85%]",
          isUser && "flex flex-col items-end"
        )}
      >
        {/* Message bubble */}
        <div
          className={cn(
            "rounded-xl px-4 py-3",
            isUser
              ? "bg-gold/10 text-foreground border border-gold/20"
              : "bg-surface-2 border border-border"
          )}
        >
          {isUser ? (
            <p className="text-sm">{message.content}</p>
          ) : (
            <div className="markdown-content text-sm">
              <MarkdownContent
                content={message.content}
                onCitationClick={onCitationClick}
                activeCitationId={activeCitationId}
              />
            </div>
          )}
        </div>

        {/* Voice controls for assistant messages (only when voice is on) */}
        {!isUser && voiceEnabled && (
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

        {!isUser && message.citations && message.citations.length > 0 && (
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

        {/* Show Verified badge for Foundry IQ (inline citations) when isVerified is true */}
        {!isUser && message.isVerified && (!message.citations || message.citations.length === 0) && (
          <Badge variant="success" className="text-xs gap-1">
            <CheckCircle2 className="h-3 w-3" />
            Verified
          </Badge>
        )}

      </div>
    </Container>
  );
}

