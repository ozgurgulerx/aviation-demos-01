import { NextResponse } from "next/server";

export async function GET() {
  return NextResponse.json({
    status: "ok",
    service: "aviation-rag-frontend",
    timestamp: new Date().toISOString(),
  });
}
