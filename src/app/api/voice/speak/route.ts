import { NextRequest } from "next/server";
import { z } from "zod";

const RequestSchema = z.object({
  text: z.string().trim().min(1).max(4000),
  language: z.enum(["tr-TR", "en-US"]).optional().default("tr-TR"),
});

const ENDPOINT = process.env.AZURE_OPENAI_ENDPOINT?.replace(/\/$/, "");
const DEPLOYMENT = process.env.AZURE_OPENAI_VOICE_DEPLOYMENT_NAME || "aviation-voice-tts";
const VOICE_MODEL = process.env.AZURE_OPENAI_VOICE_MODEL || "gpt-4o-mini-tts";
const API_VERSION = process.env.AZURE_OPENAI_VOICE_API_VERSION || "2025-03-01-preview";
const API_KEY = process.env.AZURE_OPENAI_API_KEY;
const BEARER_TOKEN = process.env.AZURE_OPENAI_BEARER_TOKEN;
const AUTH_MODE = (process.env.AZURE_OPENAI_AUTH_MODE || "token").toLowerCase();
const OPENAI_SCOPE = "https://cognitiveservices.azure.com/.default";
const OPENAI_RESOURCE = "https://cognitiveservices.azure.com/";
const APP_SERVICE_MI_API_VERSION = "2019-08-01";
const IMDS_API_VERSION = "2018-02-01";
const TOKEN_TIMEOUT_MS = 5000;

function defaultVoice(language: "tr-TR" | "en-US") {
  if (language === "tr-TR") {
    return process.env.AZURE_OPENAI_VOICE_TURKISH || "alloy";
  }
  return process.env.AZURE_OPENAI_VOICE_ENGLISH || "alloy";
}

async function getTokenViaClientCredentials(): Promise<string | null> {
  const tenantId = process.env.AZURE_OPENAI_TENANT_ID || process.env.AZURE_TENANT_ID;
  const clientId = process.env.AZURE_OPENAI_CLIENT_ID;
  const clientSecret = process.env.AZURE_OPENAI_CLIENT_SECRET;
  if (!tenantId || !clientId || !clientSecret) {
    return null;
  }

  const body = new URLSearchParams({
    client_id: clientId,
    client_secret: clientSecret,
    grant_type: "client_credentials",
    scope: OPENAI_SCOPE,
  });

  const response = await fetch(
    `https://login.microsoftonline.com/${tenantId}/oauth2/v2.0/token`,
    {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: body.toString(),
      cache: "no-store",
      signal: AbortSignal.timeout(TOKEN_TIMEOUT_MS),
    }
  );

  if (!response.ok) {
    const details = await response.text().catch(() => "");
    throw new Error(`Client credential token request failed (${response.status}): ${details}`);
  }

  const payload = (await response.json()) as { access_token?: string };
  if (!payload.access_token) {
    throw new Error("Client credential token response did not include access_token");
  }
  return payload.access_token;
}

async function getTokenViaAppServiceManagedIdentity(): Promise<string | null> {
  const identityEndpoint = process.env.IDENTITY_ENDPOINT;
  const identityHeader = process.env.IDENTITY_HEADER;
  if (!identityEndpoint || !identityHeader) {
    return null;
  }

  const managedIdentityClientId = process.env.AZURE_OPENAI_MANAGED_IDENTITY_CLIENT_ID;
  const params = new URLSearchParams({
    "api-version": APP_SERVICE_MI_API_VERSION,
    resource: OPENAI_RESOURCE,
  });
  if (managedIdentityClientId) {
    params.set("client_id", managedIdentityClientId);
  }

  const response = await fetch(`${identityEndpoint}?${params.toString()}`, {
    method: "GET",
    headers: {
      "X-IDENTITY-HEADER": identityHeader,
      Metadata: "true",
    },
    cache: "no-store",
    signal: AbortSignal.timeout(TOKEN_TIMEOUT_MS),
  });

  if (!response.ok) {
    const details = await response.text().catch(() => "");
    throw new Error(`App Service MI token request failed (${response.status}): ${details}`);
  }

  const payload = (await response.json()) as { access_token?: string };
  if (!payload.access_token) {
    throw new Error("App Service MI token response did not include access_token");
  }
  return payload.access_token;
}

async function getTokenViaImdsManagedIdentity(): Promise<string | null> {
  const managedIdentityClientId = process.env.AZURE_OPENAI_MANAGED_IDENTITY_CLIENT_ID;
  const params = new URLSearchParams({
    "api-version": IMDS_API_VERSION,
    resource: OPENAI_RESOURCE,
  });
  if (managedIdentityClientId) {
    params.set("client_id", managedIdentityClientId);
  }

  const response = await fetch(`http://169.254.169.254/metadata/identity/oauth2/token?${params.toString()}`, {
    method: "GET",
    headers: { Metadata: "true" },
    cache: "no-store",
    signal: AbortSignal.timeout(TOKEN_TIMEOUT_MS),
  });

  if (!response.ok) {
    const details = await response.text().catch(() => "");
    throw new Error(`IMDS token request failed (${response.status}): ${details}`);
  }

  const payload = (await response.json()) as { access_token?: string };
  if (!payload.access_token) {
    throw new Error("IMDS token response did not include access_token");
  }
  return payload.access_token;
}

async function getEntraToken(): Promise<string> {
  if (BEARER_TOKEN) {
    return BEARER_TOKEN;
  }

  const tokenErrors: string[] = [];

  try {
    const token = await getTokenViaClientCredentials();
    if (token) return token;
  } catch (error) {
    tokenErrors.push(error instanceof Error ? error.message : "Client credential token failed");
  }

  try {
    const token = await getTokenViaAppServiceManagedIdentity();
    if (token) return token;
  } catch (error) {
    tokenErrors.push(error instanceof Error ? error.message : "App Service MI token failed");
  }

  try {
    const token = await getTokenViaImdsManagedIdentity();
    if (token) return token;
  } catch (error) {
    tokenErrors.push(error instanceof Error ? error.message : "IMDS token failed");
  }

  if (tokenErrors.length === 0) {
    throw new Error(
      "No token auth path configured. Provide AZURE_OPENAI_CLIENT_ID/SECRET/TENANT_ID, managed identity, or AZURE_OPENAI_BEARER_TOKEN."
    );
  }

  throw new Error(tokenErrors.join(" | "));
}

async function buildAuthHeaders(): Promise<Record<string, string>> {
  if (AUTH_MODE === "api-key") {
    if (!API_KEY) {
      throw new Error("AZURE_OPENAI_AUTH_MODE=api-key requires AZURE_OPENAI_API_KEY");
    }
    return { "api-key": API_KEY };
  }

  if (AUTH_MODE === "auto") {
    try {
      const token = await getEntraToken();
      return { Authorization: `Bearer ${token}` };
    } catch {
      if (!API_KEY) throw new Error("AZURE_OPENAI_AUTH_MODE=auto failed to acquire token and no API key is set");
      return { "api-key": API_KEY };
    }
  }

  const token = await getEntraToken();
  return { Authorization: `Bearer ${token}` };
}

export async function POST(request: NextRequest) {
  try {
    if (!ENDPOINT) {
      return new Response(
        JSON.stringify({ error: "AZURE_OPENAI_ENDPOINT is not configured" }),
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
    const authHeaders = await buildAuthHeaders();
    const response = await fetch(
      `${ENDPOINT}/openai/deployments/${DEPLOYMENT}/audio/speech?api-version=${API_VERSION}`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          ...authHeaders,
        },
        body: JSON.stringify({
          model: VOICE_MODEL,
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
