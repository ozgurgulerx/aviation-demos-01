"use client";

import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";
import { cn } from "@/lib/utils";

interface MarkdownContentProps {
  content: string;
  onCitationClick?: (id: number) => void;
  activeCitationId?: number | null;
}

export function MarkdownContent({
  content,
  onCitationClick,
  activeCitationId,
}: MarkdownContentProps) {
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
