"use client";

import { useState, useCallback } from "react";
import type { PiiCheckResult } from "@/types";

interface UsePiiFilterReturn {
  checkMessage: (text: string) => Promise<PiiCheckResult>;
  isChecking: boolean;
  lastResult: PiiCheckResult | null;
  clearResult: () => void;
}

export function usePiiFilter(): UsePiiFilterReturn {
  const [isChecking, setIsChecking] = useState(false);
  const [lastResult, setLastResult] = useState<PiiCheckResult | null>(null);

  const checkMessage = useCallback(async (text: string): Promise<PiiCheckResult> => {
    setIsChecking(true);

    try {
      const response = await fetch("/api/pii", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ text }),
      });

      if (!response.ok) {
        throw new Error("PII check failed");
      }

      const data = await response.json();

      const result: PiiCheckResult = {
        hasPii: data.blocked ?? false,
        entities: data.detectedCategories?.map((category: string) => ({
          text: "",
          category,
          offset: 0,
          length: 0,
          confidenceScore: 1,
        })) ?? [],
        redactedText: data.message,
      };

      setLastResult(result);
      return result;
    } catch (error) {
      console.error("PII filter error:", error);
      // On error, return safe result (no PII detected)
      const result: PiiCheckResult = {
        hasPii: false,
        entities: [],
      };
      setLastResult(result);
      return result;
    } finally {
      setIsChecking(false);
    }
  }, []);

  const clearResult = useCallback(() => {
    setLastResult(null);
  }, []);

  return {
    checkMessage,
    isChecking,
    lastResult,
    clearResult,
  };
}
