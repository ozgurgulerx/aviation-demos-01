import { NextRequest, NextResponse } from "next/server";
import { z } from "zod";
import { checkPii, formatPiiWarning } from "@/lib/pii";

const RequestSchema = z.object({
  text: z.string().min(1).max(5000),
});

// Helper to get service info for logging
function getServiceInfo(): { type: string; endpoint: string } {
  const containerEndpoint = process.env.PII_CONTAINER_ENDPOINT;
  const endpoint = process.env.PII_ENDPOINT || containerEndpoint || "http://localhost:5000";
  const isContainer = containerEndpoint && endpoint === containerEndpoint;

  return {
    type: isContainer ? "Azure PII Container (On-Prem Simulation)" : "Azure Language Service (Cloud)",
    endpoint: endpoint,
  };
}

export async function POST(request: NextRequest) {
  const serviceInfo = getServiceInfo();

  try {
    const body = await request.json();
    const parsed = RequestSchema.safeParse(body);

    if (!parsed.success) {
      return NextResponse.json(
        { error: "Invalid request body", details: parsed.error.issues },
        { status: 400 }
      );
    }

    const userMessage = parsed.data.text;

    // Log the PII check request
    console.log("\n" + "═".repeat(70));
    console.log("🔒 PII DETECTION CHECK");
    console.log("═".repeat(70));
    console.log(`📡 Service: ${serviceInfo.type}`);
    console.log(`🌐 Endpoint: ${serviceInfo.endpoint}`);
    console.log(`💬 Message: "${userMessage.substring(0, 100)}${userMessage.length > 100 ? '...' : ''}"`);
    console.log("─".repeat(70));

    const result = await checkPii({ text: userMessage });

    if (result.hasPii) {
      const warningMessage = formatPiiWarning(result.entities);
      const categories = result.entities.map((e) => e.category);

      // Log blocked result
      console.log("⛔ RESULT: BLOCKED");
      console.log(`🚨 Detected PII Categories: ${categories.join(", ")}`);
      console.log(`📋 Entities Found:`);
      result.entities.forEach((entity) => {
        console.log(`   • "${entity.text}" → ${entity.category} (${(entity.confidenceScore * 100).toFixed(0)}% confidence)`);
      });
      console.log(`💬 User Warning: "${warningMessage}"`);
      console.log("═".repeat(70) + "\n");

      return NextResponse.json({
        blocked: true,
        message: warningMessage,
        detectedCategories: categories,
      });
    }

    // Log allowed result
    console.log("✅ RESULT: ALLOWED");
    console.log("No configured PII categories detected in message");
    console.log("➡️  Message will be forwarded to AI agent");
    console.log("═".repeat(70) + "\n");

    return NextResponse.json({
      blocked: false,
      message: null,
    });
  } catch (error) {
    console.log("❌ RESULT: ERROR");
    console.log(`🔧 Service: ${serviceInfo.endpoint}`);
    console.log(`⚠️  Error: ${error instanceof Error ? error.message : "Unknown error"}`);
    console.log("📝 Failing open - message allowed through");
    console.log("═".repeat(70) + "\n");

    // On error, allow the message through (fail open for availability)
    return NextResponse.json({
      blocked: false,
      message: null,
      warning: "PII check unavailable",
    });
  }
}
