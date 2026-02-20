import { NextRequest } from "next/server";
import { z } from "zod";

const RequestSchema = z.object({
  text: z.string().trim().min(1).max(4000),
  language: z.enum(["tr-TR", "en-US"]).optional().default("tr-TR"),
});

const ENDPOINT = process.env.AZURE_OPENAI_ENDPOINT?.replace(/\/$/, "");
const DEPLOYMENT = process.env.AZURE_OPENAI_VOICE_DEPLOYMENT_NAME || "aviation-voice-tts";
const API_VERSION = process.env.AZURE_OPENAI_VOICE_API_VERSION || "2025-03-01-preview";
const API_KEY = process.env.AZURE_OPENAI_API_KEY;
const BEARER_TOKEN = process.env.AZURE_OPENAI_BEARER_TOKEN;

function defaultVoice(language: "tr-TR" | "en-US") {
  if (language === "tr-TR") {
    return process.env.AZURE_OPENAI_VOICE_TURKISH || "alloy";
  }
  return process.env.AZURE_OPENAI_VOICE_ENGLISH || "alloy";
}

export async function POST(request: NextRequest) {
  try {
    if (!ENDPOINT) {
      return new Response(
        JSON.stringify({ error: "AZURE_OPENAI_ENDPOINT is not configured" }),
        { status: 500, headers: { "Content-Type": "application/json" } }
      );
    }

    if (!API_KEY && !BEARER_TOKEN) {
      return new Response(
        JSON.stringify({
          error:
            "Missing Azure OpenAI credentials. Set AZURE_OPENAI_API_KEY or AZURE_OPENAI_BEARER_TOKEN.",
        }),
        { status: 500, headers: { "Content-Type": "application/json" } }
      );
    }

    const body = await request.json();
    const parsed = RequestSchema.safeParse(body);
    if (!parsed.success) {
      return new Response(
        JSON.stringify({ error: "Invalid request", details: parsed.error.issues }),
        { status: 400, headers: { "Content-Type": "application/json" } }
      );
    }

    const { text, language } = parsed.data;
    const response = await fetch(
      `${ENDPOINT}/openai/deployments/${DEPLOYMENT}/audio/speech?api-version=${API_VERSION}`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          ...(API_KEY ? { "api-key": API_KEY } : {}),
          ...(BEARER_TOKEN ? { Authorization: `Bearer ${BEARER_TOKEN}` } : {}),
        },
        body: JSON.stringify({
          model: "gpt-4o-mini-tts",
          voice: defaultVoice(language),
          input: text,
        }),
      }
    );

    if (!response.ok) {
      const details = await response.text().catch(() => "");
      return new Response(
        JSON.stringify({
          error: "Voice synthesis failed",
          status: response.status,
          details,
        }),
        { status: 502, headers: { "Content-Type": "application/json" } }
      );
    }

    const audioBuffer = await response.arrayBuffer();
    return new Response(audioBuffer, {
      status: 200,
      headers: {
        "Content-Type": response.headers.get("content-type") || "audio/mpeg",
        "Cache-Control": "no-store",
      },
    });
  } catch (error) {
    return new Response(
      JSON.stringify({
        error: "Voice synthesis route failed",
        details: error instanceof Error ? error.message : "Unknown error",
      }),
      { status: 500, headers: { "Content-Type": "application/json" } }
    );
  }
}
