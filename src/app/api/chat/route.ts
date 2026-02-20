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

const encoder = new TextEncoder();

function createSSEMessage(data: unknown): string {
  return `data: ${JSON.stringify(data)}\n\n`;
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

    const backendResponse = await fetchWithRetry(`${PYTHON_API_URL}/api/chat`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        message: lastUserMessage.content,
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

    if (!backendResponse.ok || !backendResponse.body) {
      const errText = await backendResponse.text().catch(() => "");
      const stream = new ReadableStream({
        start(controller) {
          controller.enqueue(
            encoder.encode(
              createSSEMessage({
                type: "agent_error",
                message: errText || `Backend error (${backendResponse.status})`,
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
        try {
          const reader = backendResponse.body!.getReader();
          while (true) {
            const { done, value } = await reader.read();
            if (done) break;
            controller.enqueue(value);
          }
          controller.close();
        } catch (error) {
          controller.enqueue(
            encoder.encode(
              createSSEMessage({
                type: "agent_error",
                message:
                  error instanceof Error
                    ? error.message
                    : "Unknown streaming error",
              })
            )
          );
          controller.close();
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
    return new Response(
      JSON.stringify({
        error: "Internal server error",
        message: error instanceof Error ? error.message : "Unknown error",
      }),
      { status: 500, headers: { "Content-Type": "application/json" } }
    );
  }
}
