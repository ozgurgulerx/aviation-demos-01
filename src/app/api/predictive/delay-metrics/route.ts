import { NextResponse } from "next/server";

const PYTHON_API_URL =
  process.env.BACKEND_URL ||
  process.env.PYTHON_API_URL ||
  (process.env.NODE_ENV === "development" ? "http://localhost:5001" : "");

export async function GET() {
  if (!PYTHON_API_URL) {
    return NextResponse.json(
      {
        status: "degraded",
        error: "Backend URL is not configured. Set BACKEND_URL or PYTHON_API_URL.",
        baseline: {},
        optimized: {},
        uplift: {},
      },
      { status: 200 }
    );
  }

  try {
    const response = await fetch(`${PYTHON_API_URL}/api/predictive/delay-metrics`, {
      method: "GET",
      headers: { "Content-Type": "application/json" },
      cache: "no-store",
    });
    const text = await response.text();
    const payload = text ? JSON.parse(text) : {};
    if (!response.ok) {
      return NextResponse.json(
        {
          status: "degraded",
          error: payload.error || `Backend error (${response.status})`,
          baseline: {},
          optimized: {},
          uplift: {},
        },
        { status: 200 }
      );
    }
    return NextResponse.json(payload);
  } catch (error) {
    return NextResponse.json(
      {
        status: "degraded",
        error: error instanceof Error ? error.message : "Unable to reach predictive metrics endpoint",
        baseline: {},
        optimized: {},
        uplift: {},
      },
      { status: 200 }
    );
  }
}

