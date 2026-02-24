#!/usr/bin/env python3
"""Flask API server for Aviation RAG backend."""

import logging
import os
import sys
import threading

from flask import Flask, Response, jsonify, request, stream_with_context

# Structured JSON logging when python-json-logger is available.
try:
    from pythonjsonlogger.json import JsonFormatter  # type: ignore[import-untyped]
    _json_handler = logging.StreamHandler(sys.stdout)
    _json_handler.setFormatter(JsonFormatter(
        fmt="%(asctime)s %(name)s %(levelname)s %(message)s",
        rename_fields={"asctime": "timestamp", "levelname": "level"},
    ))
    logging.root.handlers.clear()
    logging.root.addHandler(_json_handler)
    logging.root.setLevel(logging.INFO)
except ImportError:
    logging.basicConfig(level=logging.INFO)
from flask_cors import CORS
from af_runtime import AgentFrameworkRuntime
from af_streaming import to_sse
from shared_utils import validate_source_policy_request

logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app, origins=[
    os.getenv("ALLOWED_ORIGIN", "https://aviation-rag-frontend-705508.azurewebsites.net"),
])

# Auto-instrument Flask with OpenTelemetry when providers are available.
try:
    from opentelemetry.instrumentation.flask import FlaskInstrumentor
    FlaskInstrumentor().instrument_app(app)
except ImportError:
    pass

runtime = None
runtime_lock = threading.Lock()


def get_runtime() -> AgentFrameworkRuntime:
    """Lazily initialize runtime so health checks are not blocked by cold-start."""
    global runtime
    if runtime is not None:
        return runtime

    with runtime_lock:
        if runtime is None:
            logger.info("Initializing Agent Framework runtime...")
            runtime = AgentFrameworkRuntime()
            try:
                capabilities = runtime.retriever.source_capabilities(refresh=True)
                unavailable = [
                    c.get("source")
                    for c in capabilities
                    if str(c.get("status", "")).lower() == "unavailable"
                ]
                degraded = [
                    c.get("source")
                    for c in capabilities
                    if str(c.get("status", "")).lower() == "degraded"
                ]
                guardrail_status = (
                    (runtime.retriever._identity_guardrail_report or {}).get("status", "unknown")
                )
                logger.info(
                    "Runtime source capabilities initialized (guardrail=%s unavailable=%s degraded=%s)",
                    guardrail_status,
                    ",".join(str(s) for s in unavailable if s) or "none",
                    ",".join(str(s) for s in degraded if s) or "none",
                )
            except Exception:
                logger.exception("Failed to initialize source capability snapshot")
            logger.info("Runtime ready (af_enabled=%s)", runtime.af_enabled)
    return runtime


@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint.

    Query parameters:
        detail=auth — include Fabric token status for both API and TDS scopes.
    """
    payload: dict = {"status": "ok", "service": "aviation-rag-api"}
    detail = request.args.get("detail", "").lower()

    if detail == "auth" and runtime is not None:
        try:
            from unified_retriever import _acquire_fabric_token_bundle

            fabric_bundle = _acquire_fabric_token_bundle()
            tds_bundle = _acquire_fabric_token_bundle(
                scope="https://database.windows.net/.default",
            )
            payload["fabric_auth"] = {
                "auth_mode": fabric_bundle.get("auth_mode"),
                "auth_ready": fabric_bundle.get("auth_ready"),
                "reason": fabric_bundle.get("reason"),
                "token_ttl_seconds": fabric_bundle.get("token_ttl_seconds"),
            }
            payload["fabric_sql_tds_auth"] = {
                "auth_mode": tds_bundle.get("auth_mode"),
                "auth_ready": tds_bundle.get("auth_ready"),
                "reason": tds_bundle.get("reason"),
                "token_ttl_seconds": tds_bundle.get("token_ttl_seconds"),
            }
        except Exception:
            logger.exception("Failed to gather auth diagnostics")
            payload["fabric_auth"] = {"error": "diagnostics_unavailable"}
            payload["fabric_sql_tds_auth"] = {"error": "diagnostics_unavailable"}

    return jsonify(payload)


@app.route('/api/chat', methods=['POST'])
def chat():
    """Main chat endpoint. Streams AF-native events using SSE."""
    data = request.get_json(silent=True) or {}
    message = data.get("message")
    retrieval_mode = data.get("retrieval_mode", "code-rag")
    conversation_id = data.get("conversation_id")
    query_profile = data.get("query_profile", "pilot-brief")
    required_sources = data.get("required_sources") or []
    source_policy = data.get("source_policy", "include")
    freshness_sla_minutes = data.get("freshness_sla_minutes")
    explain_retrieval = bool(data.get("explain_retrieval", False))
    risk_mode = data.get("risk_mode", "standard")
    ask_recommendation = bool(data.get("ask_recommendation", False))
    demo_scenario = data.get("demo_scenario")
    failure_policy = data.get("failure_policy", "graceful")

    # Multi-turn: extract conversation history from messages array.
    max_history_turns = int(os.getenv("MAX_CONVERSATION_HISTORY_TURNS", "3"))
    conversation_history = None
    raw_messages = data.get("messages")
    if isinstance(raw_messages, list) and raw_messages:
        pairs = [
            {"role": str(m.get("role", "")), "content": str(m.get("content", ""))}
            for m in raw_messages
            if isinstance(m, dict) and m.get("role") in ("user", "assistant") and m.get("content")
        ]
        # Keep last N turn-pairs (each pair = 2 messages), excluding the final user message.
        if len(pairs) > 1:
            conversation_history = pairs[-(max_history_turns * 2 + 1):-1]
            if not conversation_history:
                conversation_history = None

    if not message:
        return jsonify({"error": "Missing 'message' field"}), 400

    MAX_MESSAGE_LENGTH = int(os.getenv("MAX_MESSAGE_LENGTH", "8000"))
    if len(message) > MAX_MESSAGE_LENGTH:
        return jsonify({"error": f"Message exceeds maximum length of {MAX_MESSAGE_LENGTH} characters"}), 400

    def event_stream():
        # Emit an immediate event so proxy callers do not time out waiting for
        # the first byte when runtime cold-start is slow.
        yield to_sse({
            "type": "agent_update",
            "stage": "runtime_init",
            "message": "Initializing runtime",
        })

        try:
            af_runtime = get_runtime()
        except Exception:
            logger.exception("Runtime initialization error")
            yield to_sse({
                "type": "agent_error",
                "message": "Runtime initialization failed while preparing your request.",
                "route": "RUNTIME_INIT_ERROR",
            })
            yield to_sse({
                "type": "done",
                "route": "RUNTIME_INIT_ERROR",
                "isVerified": False,
            })
            return

        try:
            for event in af_runtime.run_stream(
                query=message,
                session_id=conversation_id,
                retrieval_mode=retrieval_mode,
                query_profile=query_profile,
                required_sources=required_sources,
                source_policy=source_policy,
                freshness_sla_minutes=freshness_sla_minutes,
                explain_retrieval=explain_retrieval,
                risk_mode=risk_mode,
                ask_recommendation=ask_recommendation,
                demo_scenario=demo_scenario,
                conversation_history=conversation_history,
                failure_policy=failure_policy,
            ):
                yield to_sse(event)
        except Exception:
            logger.exception("SSE stream error")
            yield to_sse({"type": "agent_error", "message": "An internal error occurred while processing your request."})
            yield to_sse({"type": "done", "route": "STREAM_ERROR", "isVerified": False})

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
        source_policy = data.get("source_policy", "include")
        source_policy_validation = validate_source_policy_request(required_sources, source_policy)
        if source_policy_validation.get("is_exact") and not source_policy_validation.get("is_valid"):
            return jsonify(
                {
                    "error": source_policy_validation.get("error_message") or "Invalid exact source policy request.",
                    "error_code": source_policy_validation.get("error_code", "exact_required_sources_invalid"),
                    "required_sources_raw": list(source_policy_validation.get("required_sources_raw", [])),
                    "required_sources_normalized": list(source_policy_validation.get("required_sources_normalized", [])),
                    "invalid_required_sources": list(source_policy_validation.get("invalid_required_sources", [])),
                    "source_policy": source_policy_validation.get("source_policy", "exact"),
                }
            ), 400
        freshness_sla_minutes = data.get("freshness_sla_minutes")
        explain_retrieval = bool(data.get("explain_retrieval", False))
        risk_mode = data.get("risk_mode", "standard")
        ask_recommendation = bool(data.get("ask_recommendation", False))
        demo_scenario = data.get("demo_scenario")
        failure_policy = data.get("failure_policy", "graceful")
        af_runtime = get_runtime()
        result = af_runtime.run_once(
            query=message,
            session_id=conversation_id,
            retrieval_mode=retrieval_mode,
            query_profile=query_profile,
            required_sources=required_sources,
            source_policy=source_policy,
            freshness_sla_minutes=freshness_sla_minutes,
            explain_retrieval=explain_retrieval,
            risk_mode=risk_mode,
            ask_recommendation=ask_recommendation,
            demo_scenario=demo_scenario,
            failure_policy=failure_policy,
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
