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
  freshnessSlaMinutes: z.number().int().positive().optional(),
  explainRetrieval: z.boolean().optional().default(false),
  riskMode: z.enum(["standard", "strict"]).optional().default("standard"),
  askRecommendation: z.boolean().optional().default(false),
  demoScenario: z.string().optional(),
});

const PYTHON_API_URL =
  process.env.BACKEND_URL || process.env.PYTHON_API_URL || "http://localhost:5001";
const BACKEND_REQUEST_TIMEOUT_MS = Number(process.env.BACKEND_REQUEST_TIMEOUT_MS || "45000");
const CHAT_STREAM_TIMEOUT_MS = Number(process.env.CHAT_STREAM_TIMEOUT_MS || "180000");

const encoder = new TextEncoder();
const TERMINAL_EVENT_TYPES = new Set(["agent_done", "done", "agent_error", "error"]);

function createSSEMessage(data: unknown): string {
  return `data: ${JSON.stringify(data)}\n\n`;
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
  maxRetries = 3
): Promise<Response> {
  let lastError: Error | null = null;

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      const response = await fetch(url, options);
      if (response.ok) return response;

      if (response.status >= 400 && response.status < 500) {
        return response;
      }

      if (attempt === maxRetries) return response;
    } catch (error) {
      lastError = error instanceof Error ? error : new Error(String(error));
      if (attempt === maxRetries) throw lastError;
      await new Promise((r) => setTimeout(r, 500 * attempt));
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
      freshnessSlaMinutes,
      explainRetrieval,
      riskMode,
      askRecommendation,
      demoScenario,
    } = parsed.data;
    const lastUserMessage = messages.filter((m) => m.role === "user").pop();
    if (!lastUserMessage) {
      return new Response(
        JSON.stringify({ error: "No user message found" }),
        { status: 400, headers: { "Content-Type": "application/json" } }
      );
    }

    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), BACKEND_REQUEST_TIMEOUT_MS);

    let backendResponse: Response;
    try {
      backendResponse = await fetchWithRetry(`${PYTHON_API_URL}/api/chat`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        signal: controller.signal,
        body: JSON.stringify({
          message: lastUserMessage.content,
          messages: messages.map((m) => ({ role: m.role, content: m.content })),
          retrieval_mode: retrievalMode,
          conversation_id: conversationId,
          query_profile: queryProfile,
          required_sources: requiredSources,
          freshness_sla_minutes: freshnessSlaMinutes,
          explain_retrieval: explainRetrieval,
          risk_mode: riskMode,
          ask_recommendation: askRecommendation,
          demo_scenario: demoScenario,
        }),
      });
    } finally {
      clearTimeout(timeoutId);
    }

    if (!backendResponse.ok || !backendResponse.body) {
      const errText = await backendResponse.text().catch(() => "");
      console.error(`Backend error (${backendResponse.status}): ${errText}`);
      const stream = new ReadableStream({
        start(controller) {
          controller.enqueue(
            encoder.encode(
              createSSEMessage({
                type: "agent_error",
                message: `Backend service error (${backendResponse.status})`,
              })
            )
          );
          controller.enqueue(
            encoder.encode(
              createSSEMessage({
                type: "done",
                route: "PROXY_ERROR",
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
