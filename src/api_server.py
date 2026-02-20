#!/usr/bin/env python3
"""Flask API server for Aviation RAG backend."""

import logging
import os

from flask import Flask, Response, jsonify, request, stream_with_context
from flask_cors import CORS
from af_runtime import AgentFrameworkRuntime
from af_streaming import to_sse

logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app, origins=[
    os.getenv("ALLOWED_ORIGIN", "https://aviation-rag-frontend-705508.azurewebsites.net"),
])

runtime = None


def get_runtime() -> AgentFrameworkRuntime:
    """Lazily initialize runtime so health checks are not blocked by cold-start."""
    global runtime
    if runtime is None:
        logger.info("Initializing Agent Framework runtime...")
        runtime = AgentFrameworkRuntime()
        logger.info("Runtime ready (af_enabled=%s)", runtime.af_enabled)
    return runtime


@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint."""
    return jsonify({"status": "ok", "service": "aviation-rag-api"})


@app.route('/api/chat', methods=['POST'])
def chat():
    """Main chat endpoint. Streams AF-native events using SSE."""
    data = request.get_json(silent=True) or {}
    # Single-turn: only the latest user message is used per request.
    # The backend does not maintain conversation history.
    # TODO: implement multi-turn by accepting and forwarding the full messages array.
    message = data.get("message")
    retrieval_mode = data.get("retrieval_mode", "code-rag")
    conversation_id = data.get("conversation_id")
    query_profile = data.get("query_profile", "pilot-brief")
    required_sources = data.get("required_sources") or []
    freshness_sla_minutes = data.get("freshness_sla_minutes")
    explain_retrieval = bool(data.get("explain_retrieval", False))
    risk_mode = data.get("risk_mode", "standard")
    ask_recommendation = bool(data.get("ask_recommendation", False))
    demo_scenario = data.get("demo_scenario")

    if not message:
        return jsonify({"error": "Missing 'message' field"}), 400

    MAX_MESSAGE_LENGTH = int(os.getenv("MAX_MESSAGE_LENGTH", "8000"))
    if len(message) > MAX_MESSAGE_LENGTH:
        return jsonify({"error": f"Message exceeds maximum length of {MAX_MESSAGE_LENGTH} characters"}), 400

    af_runtime = get_runtime()

    def event_stream():
        try:
            for event in af_runtime.run_stream(
                query=message,
                session_id=conversation_id,
                retrieval_mode=retrieval_mode,
                query_profile=query_profile,
                required_sources=required_sources,
                freshness_sla_minutes=freshness_sla_minutes,
                explain_retrieval=explain_retrieval,
                risk_mode=risk_mode,
                ask_recommendation=ask_recommendation,
                demo_scenario=demo_scenario,
            ):
                yield to_sse(event)
        except Exception as exc:
            logger.exception("SSE stream error")
            yield to_sse({"type": "agent_error", "message": "An internal error occurred while processing your request."})

    return Response(
        stream_with_context(event_stream()),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.route('/api/query', methods=['POST'])
def query():
    """
    Direct query endpoint with route specification.

    Request body:
        {
            "message": "Top 5 airlines by fleet size",
            "route": "SQL"
        }
    """
    try:
        data = request.get_json(silent=True) or {}
        message = data.get("message")
        if not message:
            return jsonify({"error": "Missing 'message' field"}), 400

        MAX_MESSAGE_LENGTH = int(os.getenv("MAX_MESSAGE_LENGTH", "8000"))
        if len(message) > MAX_MESSAGE_LENGTH:
            return jsonify({"error": f"Message exceeds maximum length of {MAX_MESSAGE_LENGTH} characters"}), 400

        retrieval_mode = data.get("retrieval_mode", "code-rag")
        conversation_id = data.get("conversation_id")
        query_profile = data.get("query_profile", "pilot-brief")
        required_sources = data.get("required_sources") or []
        freshness_sla_minutes = data.get("freshness_sla_minutes")
        explain_retrieval = bool(data.get("explain_retrieval", False))
        risk_mode = data.get("risk_mode", "standard")
        ask_recommendation = bool(data.get("ask_recommendation", False))
        demo_scenario = data.get("demo_scenario")
        af_runtime = get_runtime()
        result = af_runtime.run_once(
            query=message,
            session_id=conversation_id,
            retrieval_mode=retrieval_mode,
            query_profile=query_profile,
            required_sources=required_sources,
            freshness_sla_minutes=freshness_sla_minutes,
            explain_retrieval=explain_retrieval,
            risk_mode=risk_mode,
            ask_recommendation=ask_recommendation,
            demo_scenario=demo_scenario,
        )
        return jsonify(result)
    except Exception as exc:
        logger.exception("Error in query endpoint")
        return jsonify({"error": "An internal error occurred."}), 500


@app.route('/api/fabric/preflight', methods=['GET'])
def fabric_preflight():
    """Fabric integration preflight checks for live demo readiness."""
    try:
        af_runtime = get_runtime()
        payload = af_runtime.retriever.fabric_preflight()
        return jsonify(payload)
    except Exception as exc:
        logger.exception("Fabric preflight error")
        return jsonify({"overall_status": "fail", "error": "Preflight check failed."}), 500


if __name__ == '__main__':
    print("=" * 60)
    print("AVIATION RAG API SERVER")
    print("=" * 60)
    print("Endpoints:")
    print("  GET  /health     - Health check")
    print("  POST /api/chat   - Chat with aviation RAG")
    print("  POST /api/query  - Direct query")
    print("  GET  /api/fabric/preflight - Fabric live-path readiness")
    print("=" * 60)

    app.run(
        host='0.0.0.0',
        port=int(os.getenv("PORT", "5001")),
        debug=os.getenv("FLASK_DEBUG", "false").lower() in ("true", "1", "yes"),
    )
