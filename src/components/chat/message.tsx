"use client";

import { useState, useEffect } from "react";
import { motion } from "framer-motion";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";
import { User, Bot, CheckCircle2, AlertCircle } from "lucide-react";
import { Avatar, AvatarFallback } from "@/components/ui/avatar";
import { Badge } from "@/components/ui/badge";
import { cn } from "@/lib/utils";
import type { Message as MessageType } from "@/types";

interface MessageProps {
  message: MessageType;
  onCitationClick?: (id: number) => void;
  activeCitationId?: number | null;
}

export function Message({
  message,
  onCitationClick,
  activeCitationId,
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

        {/* Verification badge for assistant messages */}
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

        {!isUser && !message.isVerified && (!message.citations || message.citations.length === 0) && (
          <Badge variant="warning" className="text-xs gap-1">
            <AlertCircle className="h-3 w-3" />
            Unverified
          </Badge>
        )}
      </div>
    </Container>
  );
}

function MarkdownContent({
  content,
  onCitationClick,
  activeCitationId,
}: {
  content: string;
  onCitationClick?: (id: number) => void;
  activeCitationId?: number | null;
}) {
  // Parse citation references like [1] and make them clickable
  const processContent = (text: string) => {
    const parts = text.split(/(\[\d+\])/g);
    return parts.map((part, index) => {
      const match = part.match(/\[(\d+)\]/);
      if (match) {
        const citationId = parseInt(match[1]);
        return (
          <button
            key={index}
            onClick={() => onCitationClick?.(citationId)}
            className={cn(
              "citation-chip mx-0.5",
              activeCitationId === citationId && "active"
            )}
          >
            {citationId}
          </button>
        );
      }
      return part;
    });
  };

  return (
    <ReactMarkdown
      remarkPlugins={[remarkGfm]}
      components={{
        p: ({ children }) => (
          <p>
            {typeof children === "string"
              ? processContent(children)
              : children}
          </p>
        ),
        td: ({ children }) => (
          <td>
            {typeof children === "string"
              ? processContent(children)
              : children}
          </td>
        ),
        th: ({ children }) => (
          <th>
            {typeof children === "string"
              ? processContent(children)
              : children}
          </th>
        ),
        strong: ({ children }) => (
          <strong className="font-semibold text-foreground">{children}</strong>
        ),
        h2: ({ children }) => (
          <h2 className="text-base font-semibold mt-4 mb-2 first:mt-0">
            {typeof children === "string"
              ? processContent(children)
              : children}
          </h2>
        ),
        h3: ({ children }) => (
          <h3 className="text-sm font-semibold mt-3 mb-1.5">
            {children}
          </h3>
        ),
        table: ({ children }) => (
          <div className="overflow-x-auto my-3">
            <table className="w-full">{children}</table>
          </div>
        ),
      }}
    >
      {content}
    </ReactMarkdown>
  );
}
