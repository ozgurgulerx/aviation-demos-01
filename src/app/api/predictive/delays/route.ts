import { NextRequest, NextResponse } from "next/server";

const PYTHON_API_URL =
  process.env.BACKEND_URL ||
  process.env.PYTHON_API_URL ||
  (process.env.NODE_ENV === "development" ? "http://localhost:5001" : "");

export async function GET(request: NextRequest) {
  if (!PYTHON_API_URL) {
    return NextResponse.json(
      {
        status: "degraded",
        error: "Backend URL is not configured. Set BACKEND_URL or PYTHON_API_URL.",
        rows: [],
      },
      { status: 200 }
    );
  }

  try {
    const query = request.nextUrl.searchParams.toString();
    const target = `${PYTHON_API_URL}/api/predictive/delays${query ? `?${query}` : ""}`;
    const response = await fetch(target, {
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
          rows: [],
        },
        { status: 200 }
      );
    }
    return NextResponse.json(payload);
  } catch (error) {
    return NextResponse.json(
      {
        status: "degraded",
        error: error instanceof Error ? error.message : "Unable to reach predictive delays endpoint",
        rows: [],
      },
      { status: 200 }
    );
  }
}

