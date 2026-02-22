import type { ReasoningSseEvent } from "@/types";

interface MockReasoningStreamOptions {
  prompt: string;
  onEvent: (event: ReasoningSseEvent) => void;
}

const STAGE_ORDER: ReasoningSseEvent["stage"][] = [
  "pii_scan",
  "understanding_request",
  "intent_mapped",
  "evidence_retrieval",
  "drafting_brief",
  "evidence_check_complete",
];

function isoNow(): string {
  return new Date().toISOString();
}

function inferIntentArtifact(prompt: string): {
  intentLabel: string;
  confidence: "High" | "Medium" | "Low";
  route: string;
} {
  const q = prompt.toLowerCase();
  if (q.includes("policy") || q.includes("compliance") || q.includes("sop")) {
    return {
      intentLabel: "Policy compliance check",
      confidence: "High",
      route: "AGENTIC",
    };
  }
  if (q.includes("delay") || q.includes("disruption") || q.includes("irrops")) {
    return {
      intentLabel: "Disruption explainability scan",
      confidence: "Medium",
      route: "AGENTIC",
    };
  }
  if (q.includes("arrival") || q.includes("approach") || q.includes("landing")) {
    return {
      intentLabel: "Arrival risk brief",
      confidence: "High",
      route: "AGENTIC",
    };
  }
  if (q.includes("crew") || q.includes("legality")) {
    return {
      intentLabel: "Crew legality risk scan",
      confidence: "High",
      route: "AGENTIC",
    };
  }
  return {
    intentLabel: "Departure risk brief",
    confidence: "Medium",
    route: "AGENTIC",
  };
}

function inferRetrievalArtifact(prompt: string): { sources: string[]; callCount: number } {
  const q = prompt.toLowerCase();
  const baseSources = ["KQL", "SQL", "VECTOR_REG"];
  if (q.includes("network") || q.includes("dependency")) {
    baseSources.unshift("GRAPH");
  }
  if (q.includes("history") || q.includes("replay")) {
    return { sources: ["KQL", "VECTOR_REG"], callCount: 3 };
  }
  if (q.includes("policy") || q.includes("compliance")) {
    return { sources: ["VECTOR_REG"], callCount: 1 };
  }
  return { sources: baseSources, callCount: baseSources.length + 2 };
}

function buildMockEvents(prompt: string): ReasoningSseEvent[] {
  const intent = inferIntentArtifact(prompt);
  const retrieval = inferRetrievalArtifact(prompt);
  return [
    {
      type: "reasoning_stage",
      stage: "understanding_request",
      ts: isoNow(),
    },
    {
      type: "reasoning_stage",
      stage: "intent_mapped",
      ts: isoNow(),
      payload: intent,
    },
    {
      type: "reasoning_stage",
      stage: "evidence_retrieval",
      ts: isoNow(),
      payload: {
        sources: retrieval.sources,
        callCount: retrieval.callCount,
      },
    },
    {
      type: "reasoning_stage",
      stage: "drafting_brief",
      ts: isoNow(),
      payload: {
        route: intent.route,
      },
    },
    {
      type: "reasoning_stage",
      stage: "evidence_check_complete",
      ts: isoNow(),
      payload: {
        verification: "Verified",
        failOpen: false,
      },
    },
  ];
}

function randomStageDelayMs(): number {
  // Keep full run in ~2-6s for UI wait-state simulation.
  return 380 + Math.floor(Math.random() * 980);
}

export function startMockReasoningStream({
  prompt,
  onEvent,
}: MockReasoningStreamOptions): () => void {
  const events = buildMockEvents(prompt);
  const timers: ReturnType<typeof setTimeout>[] = [];
  let elapsed = 0;

  for (const event of events) {
    elapsed += randomStageDelayMs();
    const timer = setTimeout(() => {
      onEvent({
        ...event,
        ts: isoNow(),
      });
    }, elapsed);
    timers.push(timer);
  }

  return () => {
    for (const timer of timers) {
      clearTimeout(timer);
    }
  };
}

export function getReasoningStageIndex(stage: ReasoningSseEvent["stage"]): number {
  const idx = STAGE_ORDER.indexOf(stage);
  return idx === -1 ? 0 : idx;
}

export function getLatestReasoningEvent(events: ReasoningSseEvent[]): ReasoningSseEvent | null {
  if (!events.length) {
    return null;
  }
  return events[events.length - 1] ?? null;
}

