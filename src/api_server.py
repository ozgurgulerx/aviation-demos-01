#!/usr/bin/env python3
"""Flask API server for Aviation RAG backend."""

from flask import Flask, Response, jsonify, request, stream_with_context
from flask_cors import CORS
from af_runtime import AgentFrameworkRuntime
from af_streaming import to_sse

app = Flask(__name__)
CORS(app)  # Enable CORS for Next.js frontend

runtime = None


def get_runtime() -> AgentFrameworkRuntime:
    """Lazily initialize runtime so health checks are not blocked by cold-start."""
    global runtime
    if runtime is None:
        print("Initializing Agent Framework runtime...")
        runtime = AgentFrameworkRuntime()
        print(f"Runtime ready (af_enabled={runtime.af_enabled})")
    return runtime


@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint."""
    return jsonify({"status": "ok", "service": "aviation-rag-api"})


@app.route('/api/chat', methods=['POST'])
def chat():
    """Main chat endpoint. Streams AF-native events using SSE."""
    data = request.get_json(silent=True) or {}
    message = data.get("message")
    retrieval_mode = data.get("retrieval_mode", "code-rag")
    conversation_id = data.get("conversation_id")
    query_profile = data.get("query_profile", "pilot-brief")
    required_sources = data.get("required_sources") or []
    freshness_sla_minutes = data.get("freshness_sla_minutes")
    explain_retrieval = bool(data.get("explain_retrieval", False))
    risk_mode = data.get("risk_mode", "standard")
    ask_recommendation = bool(data.get("ask_recommendation", False))

    if not message:
        return jsonify({"error": "Missing 'message' field"}), 400

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
            ):
                yield to_sse(event)
        except Exception as exc:
            yield to_sse({"type": "agent_error", "message": str(exc)})

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

        retrieval_mode = data.get("retrieval_mode", "code-rag")
        conversation_id = data.get("conversation_id")
        query_profile = data.get("query_profile", "pilot-brief")
        required_sources = data.get("required_sources") or []
        freshness_sla_minutes = data.get("freshness_sla_minutes")
        explain_retrieval = bool(data.get("explain_retrieval", False))
        risk_mode = data.get("risk_mode", "standard")
        ask_recommendation = bool(data.get("ask_recommendation", False))
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
        )
        return jsonify(result)
    except Exception as exc:
        print(f"Error in query endpoint: {exc}")
        return jsonify({"error": str(exc)}), 500


if __name__ == '__main__':
    print("=" * 60)
    print("AVIATION RAG API SERVER")
    print("=" * 60)
    print("Endpoints:")
    print("  GET  /health     - Health check")
    print("  POST /api/chat   - Chat with aviation RAG")
    print("  POST /api/query  - Direct query")
    print("=" * 60)

    app.run(host='0.0.0.0', port=5001, debug=True)
