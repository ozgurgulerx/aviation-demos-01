import { NextResponse } from "next/server";

const PYTHON_API_URL =
  process.env.BACKEND_URL || process.env.PYTHON_API_URL || "http://localhost:5001";

export async function GET() {
  try {
    const response = await fetch(`${PYTHON_API_URL}/api/fabric/preflight`, {
      method: "GET",
      headers: { "Content-Type": "application/json" },
      cache: "no-store",
    });

    const text = await response.text();
    const payload = text ? JSON.parse(text) : {};
    if (!response.ok) {
      return NextResponse.json(
        { overall_status: "fail", error: payload.error || `Backend error (${response.status})` },
        { status: 200 }
      );
    }
    return NextResponse.json(payload);
  } catch (error) {
    return NextResponse.json(
      {
        overall_status: "fail",
        error: error instanceof Error ? error.message : "Unable to reach backend preflight endpoint",
      },
      { status: 200 }
    );
  }
}
