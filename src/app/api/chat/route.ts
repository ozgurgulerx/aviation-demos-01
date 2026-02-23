import { NextRequest } from "next/server";
import { z } from "zod";

const MessageSchema = z.object({
  role: z.enum(["user", "assistant", "system"]),
  content: z.string(),
});

const RequestSchema = z.object({
  messages: z.array(MessageSchema),
  retrievalMode: z.enum(["code-rag", "foundry-iq"]).optional().default("code-rag"),
  conversationId: z.string().optional(),
  queryProfile: z.string().optional().default("pilot-brief"),
  requiredSources: z.array(z.string()).optional().default([]),
  sourcePolicy: z.enum(["include", "exact"]).optional().default("include"),
  freshnessSlaMinutes: z.number().int().positive().optional(),
  explainRetrieval: z.boolean().optional().default(false),
  riskMode: z.enum(["standard", "strict"]).optional().default("standard"),
  failurePolicy: z.enum(["graceful", "strict"]).optional().default("graceful"),
  askRecommendation: z.boolean().optional().default(false),
  demoScenario: z.string().optional(),
});

const DEV_BACKEND_FALLBACK = process.env.NODE_ENV === "development" ? "http://localhost:5001" : "";
const PYTHON_API_URL = process.env.BACKEND_URL || process.env.PYTHON_API_URL || DEV_BACKEND_FALLBACK;
const BACKEND_REQUEST_TIMEOUT_MS = Number(process.env.BACKEND_REQUEST_TIMEOUT_MS || "180000");
const CHAT_STREAM_TIMEOUT_MS = Number(process.env.CHAT_STREAM_TIMEOUT_MS || "240000");

const encoder = new TextEncoder();
const TERMINAL_EVENT_TYPES = new Set(["agent_done", "agent_partial_done", "done", "agent_error", "error"]);

function createSSEMessage(data: unknown): string {
  return `data: ${JSON.stringify(data)}\n\n`;
}

function createProxyErrorSseResponse(message: string, route: string): Response {
  const stream = new ReadableStream({
    start(controller) {
      controller.enqueue(
        encoder.encode(
          createSSEMessage({
            type: "agent_error",
            message,
          })
        )
      );
      controller.enqueue(
        encoder.encode(
          createSSEMessage({
            type: "done",
            route,
            isVerified: false,
          })
        )
      );
      controller.close();
    },
  });

  return new Response(stream, {
    status: 200,
    headers: {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache",
      Connection: "keep-alive",
    },
  });
}

function parseTerminalEvents(buffer: string): { sawTerminal: boolean; remainder: string } {
  const frames = buffer.split(/\r?\n\r?\n/);
  const remainder = frames.pop() ?? "";
  let sawTerminal = false;

  for (const frame of frames) {
    const lines = frame.split(/\r?\n/);
    for (const line of lines) {
      const trimmed = line.trimStart();
      if (!trimmed.startsWith("data:")) continue;
      try {
        const parsed = JSON.parse(trimmed.slice(5).trimStart()) as { type?: string };
        if (typeof parsed.type === "string" && TERMINAL_EVENT_TYPES.has(parsed.type)) {
          sawTerminal = true;
        }
      } catch {
        continue;
      }
    }
  }

  return { sawTerminal, remainder };
}

async function fetchWithRetry(
  url: string,
  options: RequestInit,
  totalTimeoutMs: number,
  maxRetries = 3
): Promise<Response> {
  let lastError: Error | null = null;
  const deadline = Date.now() + Math.max(1, totalTimeoutMs);

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    const remainingMs = deadline - Date.now();
    if (remainingMs <= 0) {
      throw new Error(`Backend request timed out after ${totalTimeoutMs}ms`);
    }

    const controller = new AbortController();
    let didTimeout = false;
    const timeoutId = setTimeout(() => {
      didTimeout = true;
      controller.abort();
    }, remainingMs);

    try {
      const response = await fetch(url, {
        ...options,
        signal: controller.signal,
      });
      if (response.ok) return response;

      if (response.status >= 400 && response.status < 500) {
        return response;
      }

      if (attempt === maxRetries) return response;
    } catch (error) {
      if (didTimeout) {
        throw new Error(`Backend request timed out after ${totalTimeoutMs}ms`);
      }
      lastError = error instanceof Error ? error : new Error(String(error));
      if (attempt === maxRetries) throw lastError;
      await new Promise((r) => setTimeout(r, 500 * attempt));
    } finally {
      clearTimeout(timeoutId);
    }
  }

  throw lastError || new Error("Max retries exceeded");
}

export async function POST(request: NextRequest) {
  try {
    const body = await request.json();
    const parsed = RequestSchema.safeParse(body);

    if (!parsed.success) {
      return new Response(
        JSON.stringify({ error: "Invalid request", details: parsed.error.issues }),
        { status: 400, headers: { "Content-Type": "application/json" } }
      );
    }

    const {
      messages,
      retrievalMode,
      conversationId,
      queryProfile,
      requiredSources,
      sourcePolicy,
      freshnessSlaMinutes,
      explainRetrieval,
      riskMode,
      failurePolicy,
      askRecommendation,
      demoScenario,
    } = parsed.data;
    if (!PYTHON_API_URL) {
      return new Response(
        JSON.stringify({ error: "Backend URL is not configured. Set BACKEND_URL or PYTHON_API_URL." }),
        { status: 500, headers: { "Content-Type": "application/json" } }
      );
    }
    const lastUserMessage = messages.filter((m) => m.role === "user").pop();
    if (!lastUserMessage) {
      return new Response(
        JSON.stringify({ error: "No user message found" }),
        { status: 400, headers: { "Content-Type": "application/json" } }
      );
    }

    let backendResponse: Response;
    try {
      const backendRequestBody = JSON.stringify({
        message: lastUserMessage.content,
        messages: messages.map((m) => ({ role: m.role, content: m.content })),
        retrieval_mode: retrievalMode,
        conversation_id: conversationId,
        query_profile: queryProfile,
        required_sources: requiredSources,
        source_policy: sourcePolicy,
        freshness_sla_minutes: freshnessSlaMinutes,
        explain_retrieval: explainRetrieval,
        risk_mode: riskMode,
        failure_policy: failurePolicy,
        ask_recommendation: askRecommendation,
        demo_scenario: demoScenario,
      });

      backendResponse = await fetchWithRetry(`${PYTHON_API_URL}/api/chat`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: backendRequestBody,
      }, BACKEND_REQUEST_TIMEOUT_MS);
    } catch (error) {
      console.error("Backend connect error:", error);
      const message = error instanceof Error ? error.message : "Unable to reach backend service";
      const isTimeout = /timed out/i.test(message);
      return createProxyErrorSseResponse(
        isTimeout ? message : "Unable to reach backend service.",
        isTimeout ? "PROXY_CONNECT_TIMEOUT" : "PROXY_CONNECT_ERROR"
      );
    }

    if (!backendResponse.ok || !backendResponse.body) {
      const errText = await backendResponse.text().catch(() => "");
      console.error(`Backend error (${backendResponse.status}): ${errText}`);
      return createProxyErrorSseResponse(`Backend service error (${backendResponse.status})`, "PROXY_ERROR");
    }

    // Pass-through AF-native SSE stream from backend.
    const stream = new ReadableStream({
      async start(controller) {
        let closed = false;
        let timedOut = false;
        let sawTerminal = false;
        let sseBuffer = "";
        const chunkDecoder = new TextDecoder();
        let streamTimeout: ReturnType<typeof setTimeout> | null = null;

        const safeClose = () => {
          if (closed) return;
          closed = true;
          controller.close();
        };

        try {
          const reader = backendResponse.body!.getReader();
          streamTimeout = setTimeout(() => {
            timedOut = true;
            if (closed) return;
            controller.enqueue(
              encoder.encode(
                createSSEMessage({
                  type: "agent_error",
                  message: `Chat stream timed out after ${CHAT_STREAM_TIMEOUT_MS}ms`,
                })
              )
            );
            controller.enqueue(
              encoder.encode(
                createSSEMessage({
                  type: "done",
                  route: "PROXY_TIMEOUT",
                  isVerified: false,
                })
              )
            );
            void reader.cancel("stream timeout");
            safeClose();
          }, CHAT_STREAM_TIMEOUT_MS);

          while (true) {
            const { done, value } = await reader.read();
            if (done) break;
            const decoded = chunkDecoder.decode(value, { stream: true });
            const parsed = parseTerminalEvents(sseBuffer + decoded);
            sawTerminal = sawTerminal || parsed.sawTerminal;
            sseBuffer = parsed.remainder;
            controller.enqueue(value);
          }
          if (streamTimeout) {
            clearTimeout(streamTimeout);
            streamTimeout = null;
          }

          const finalParsed = parseTerminalEvents(`${sseBuffer}\n\n`);
          sawTerminal = sawTerminal || finalParsed.sawTerminal;
          if (!sawTerminal && !timedOut) {
            controller.enqueue(
              encoder.encode(
                createSSEMessage({
                  type: "done",
                  route: "PROXY_EOF",
                  isVerified: false,
                })
              )
            );
          }
          safeClose();
        } catch (error) {
          if (streamTimeout) {
            clearTimeout(streamTimeout);
            streamTimeout = null;
          }
          if (timedOut) return;
          console.error("SSE streaming error:", error);
          controller.enqueue(
            encoder.encode(
              createSSEMessage({
                type: "agent_error",
                message: "A streaming error occurred.",
              })
            )
          );
          controller.enqueue(
            encoder.encode(
              createSSEMessage({
                type: "done",
                route: "PROXY_STREAM_ERROR",
                isVerified: false,
              })
            )
          );
          safeClose();
        }
      },
    });

    return new Response(stream, {
      headers: {
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-cache",
        Connection: "keep-alive",
      },
    });
  } catch (error) {
    console.error("Chat route error:", error);
    return new Response(
      JSON.stringify({ error: "Internal server error" }),
      { status: 500, headers: { "Content-Type": "application/json" } }
    );
  }
}
