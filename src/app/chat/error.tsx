"use client";

import { useEffect } from "react";
import Link from "next/link";

export default function ChatError({
  error,
  reset,
}: {
  error: Error & { digest?: string };
  reset: () => void;
}) {
  useEffect(() => {
    console.error("Chat error:", error);
  }, [error]);

  return (
    <div className="flex min-h-screen items-center justify-center bg-background">
      <div className="text-center space-y-4">
        <h2 className="text-xl font-semibold text-foreground">
          Chat encountered an error
        </h2>
        <p className="text-muted-foreground">
          There was an error loading the chat. Please try again.
        </p>
        <div className="flex gap-3 justify-center">
          <button
            onClick={reset}
            className="px-4 py-2 rounded-md bg-primary text-primary-foreground hover:bg-primary/90"
          >
            Try again
          </button>
          <Link
            href="/chat"
            className="px-4 py-2 rounded-md border border-border text-foreground hover:bg-accent"
          >
            Reload chat
          </Link>
        </div>
      </div>
    </div>
  );
}
