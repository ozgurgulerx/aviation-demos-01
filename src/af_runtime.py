#!/usr/bin/env python3
"""
Agent Framework runtime orchestration for Aviation RAG.

This module implements:
- Agent/session lifecycle
- RAG context-provider execution
- Tool-call/tool-result events
- AF-native SSE event payloads
- OpenTelemetry export bootstrap (Azure Monitor / App Insights)
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import os
import queue
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Generator, Iterable, List, Optional

from concurrent.futures import ThreadPoolExecutor

import re

from opentelemetry import trace, metrics as otel_metrics

from af_context_provider import AviationRagContextProvider
from af_tools import AviationRagTools, build_agent_framework_tools
from pii_filter import PII_HIGH_SEVERITY, PII_LOW_SEVERITY
from shared_utils import normalize_source_policy, validate_source_policy_request
from retrieval_plan import ExactPolicyValidationError
from unified_retriever import Citation, UnifiedRetriever, _truncate_context_to_budget, _check_answer_grounding

logger = logging.getLogger(__name__)

# OTel instruments — no-op when APPLICATIONINSIGHTS_CONNECTION_STRING is unset.
_tracer = trace.get_tracer("aviation-rag-backend", "0.1.0")
_meter = otel_metrics.get_meter("aviation-rag-backend", "0.1.0")

_query_counter = _meter.create_counter("rag.query.count", description="Total RAG queries")
_route_counter = _meter.create_counter("rag.route.count", description="Queries per route")
_error_counter = _meter.create_counter("rag.error.count", description="Pipeline errors")
_pii_block_counter = _meter.create_counter("rag.pii.blocked", description="PII-blocked queries")

_query_latency = _meter.create_histogram("rag.query.latency_ms", unit="ms", description="End-to-end query latency")
_source_latency = _meter.create_histogram("rag.source.latency_ms", unit="ms", description="Per-source retrieval latency")
_synthesis_latency = _meter.create_histogram("rag.synthesis.latency_ms", unit="ms", description="LLM synthesis latency")

EMPTY_SYNTHESIS_ERROR = "Synthesis completed without answer text."


@dataclass
class _SessionState:
    session: Any
    last_seen: float


class AgentFrameworkRuntime:
    """Runtime facade for AF-based RAG execution and streaming."""

    _otel_initialized = False

    def __init__(self):
        self.retriever = UnifiedRetriever(enable_pii_filter=True)
        self.context_provider = AviationRagContextProvider(self.retriever)
        self.toolset = AviationRagTools(self.retriever, self.context_provider)

        self.session_ttl_seconds = int(os.getenv("AF_SESSION_TTL_SECONDS", "3600"))
        self.max_sessions = int(os.getenv("AF_MAX_SESSIONS", "500"))
        self._sessions: Dict[str, _SessionState] = {}
        self._session_lock = threading.Lock()

        self._agent: Any = None
        self._af_enabled = False
        self._framework_label = "local-fallback"
        self._foundry_client: Any = None  # lazy-init in _run_with_foundry_iq

        self._init_observability()
        self._init_agent_framework()

    @property
    def af_enabled(self) -> bool:
        return self._af_enabled

    def _init_observability(self) -> None:
        if AgentFrameworkRuntime._otel_initialized:
            return

        conn_str = os.getenv("APPLICATIONINSIGHTS_CONNECTION_STRING", "").strip()
        if not conn_str:
            return

        try:
            from azure.monitor.opentelemetry.exporter import (  # type: ignore
                AzureMonitorMetricExporter,
                AzureMonitorTraceExporter,
            )
            from opentelemetry import metrics, trace  # type: ignore
            from opentelemetry.sdk.metrics import MeterProvider  # type: ignore
            from opentelemetry.sdk.metrics.export import (  # type: ignore
                PeriodicExportingMetricReader,
            )
            from opentelemetry.sdk.resources import Resource  # type: ignore
            from opentelemetry.sdk.trace import TracerProvider  # type: ignore
            from opentelemetry.sdk.trace.export import BatchSpanProcessor  # type: ignore

            resource = Resource.create(
                {
                    "service.name": os.getenv("OTEL_SERVICE_NAME", "aviation-rag-backend"),
                    "service.version": os.getenv("APP_VERSION", "0.1.0"),
                    "deployment.environment": os.getenv("ENVIRONMENT", "development"),
                }
            )

            tracer_provider = TracerProvider(resource=resource)
            tracer_provider.add_span_processor(
                BatchSpanProcessor(
                    AzureMonitorTraceExporter(connection_string=conn_str),
                )
            )
            trace.set_tracer_provider(tracer_provider)

            metric_reader = PeriodicExportingMetricReader(
                AzureMonitorMetricExporter(connection_string=conn_str)
            )
            meter_provider = MeterProvider(resource=resource, metric_readers=[metric_reader])
            metrics.set_meter_provider(meter_provider)

            AgentFrameworkRuntime._otel_initialized = True
            logger.info("OpenTelemetry initialized with Azure Monitor exporters")
        except Exception as exc:
            logger.warning("OpenTelemetry setup skipped: %s", exc)

        # Optional AF-level setup when package is available.
        try:
            from agent_framework.observability import setup_observability  # type: ignore

            otlp_endpoint = os.getenv("OTLP_ENDPOINT", "").strip()
            if otlp_endpoint:
                setup_observability(otlp_endpoint=otlp_endpoint)
            else:
                setup_observability()
        except Exception:
            # Not fatal; SDK bootstrap above is enough for standard OTel export.
            pass

    def _init_agent_framework(self) -> None:
        try:
            from agent_framework import Agent  # type: ignore
        except Exception as exc:
            logger.warning("Agent Framework package unavailable: %s", exc)
            self._agent = None
            self._af_enabled = False
            return

        client = self._build_af_client()
        if client is None:
            logger.warning("Agent Framework client initialization failed; using fallback")
            self._agent = None
            self._af_enabled = False
            return

        try:
            self._agent = self._build_agent_instance(Agent, client)
            self._af_enabled = self._agent is not None
            self._framework_label = "agent-framework" if self._af_enabled else "local-fallback"
            if self._af_enabled:
                logger.info("Agent Framework runtime initialized successfully")
        except Exception as exc:
            logger.warning("Agent Framework initialization failed: %s", exc)
            self._agent = None
            self._af_enabled = False

    def _build_af_client(self) -> Optional[Any]:
        deployment_name = os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME", "aviation-chat-gpt5-mini")
        endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
        project_endpoint = os.getenv("AZURE_FOUNDRY_PROJECT_ENDPOINT")

        client_ctors: List[Any] = []
        try:
            from agent_framework.azure import AzureOpenAIResponsesClient  # type: ignore

            client_ctors.append(AzureOpenAIResponsesClient)
        except Exception:
            pass

        try:
            from agent_framework.openai import OpenAIResponsesClient  # type: ignore

            client_ctors.append(OpenAIResponsesClient)
        except Exception:
            pass

        for ctor in client_ctors:
            sig = inspect.signature(ctor)
            kwargs: Dict[str, Any] = {}
            params = sig.parameters

            if "deployment_name" in params:
                kwargs["deployment_name"] = deployment_name
            elif "model_id" in params:
                kwargs["model_id"] = deployment_name

            if "endpoint" in params and endpoint:
                kwargs["endpoint"] = endpoint
            if "project_endpoint" in params and project_endpoint:
                kwargs["project_endpoint"] = project_endpoint

            if "credential" in params:
                try:
                    from azure.identity import DefaultAzureCredential

                    kwargs["credential"] = DefaultAzureCredential()
                except Exception:
                    pass

            try:
                return ctor(**kwargs)
            except Exception as exc:
                logger.debug("Client ctor %s failed with kwargs %s: %s", ctor, kwargs, exc)

        return None

    def _build_agent_instance(self, agent_cls: Any, chat_client: Any) -> Optional[Any]:
        sig = inspect.signature(agent_cls)
        params = sig.parameters
        af_tools = build_agent_framework_tools(self.toolset)

        instructions = (
            "You are an aviation safety analyst. Use provided context and tools to answer "
            "questions accurately, and include citations when available."
        )

        kwargs: Dict[str, Any] = {}
        if "name" in params:
            kwargs["name"] = "aviation-rag-agent"
        if "chat_client" in params:
            kwargs["chat_client"] = chat_client
        if "instructions" in params:
            kwargs["instructions"] = instructions
        if "tools" in params and af_tools:
            kwargs["tools"] = af_tools
        if "context_providers" in params:
            kwargs["context_providers"] = [self.context_provider]
        elif "context_provider" in params:
            kwargs["context_provider"] = self.context_provider

        try:
            return agent_cls(**kwargs)
        except Exception:
            # Fallback to minimal constructor if signature assumptions were too strict.
            return agent_cls(chat_client=chat_client, name="aviation-rag-agent")

    def _prune_sessions(self, now: float) -> None:
        expired = [
            sid for sid, state in self._sessions.items() if now - state.last_seen > self.session_ttl_seconds
        ]
        for sid in expired:
            self._sessions.pop(sid, None)

        if len(self._sessions) <= self.max_sessions:
            return

        for sid, _state in sorted(self._sessions.items(), key=lambda kv: kv[1].last_seen)[
            : len(self._sessions) - self.max_sessions
        ]:
            self._sessions.pop(sid, None)

    def _get_or_create_session(self, session_id: str) -> Any:
        now = time.time()
        with self._session_lock:
            self._prune_sessions(now)
            existing = self._sessions.get(session_id)
            if existing:
                existing.last_seen = now
                return existing.session

            session = {"id": session_id}
            if self._af_enabled and self._agent is not None:
                session = self._create_af_session(session_id) or session

            self._sessions[session_id] = _SessionState(session=session, last_seen=now)
            return session

    def _create_af_session(self, session_id: str) -> Any:
        if self._agent is None:
            return None

        if hasattr(self._agent, "get_session"):
            try:
                return self._agent.get_session(service_session_id=session_id)
            except Exception:
                pass

        if hasattr(self._agent, "create_session"):
            create = self._agent.create_session
            for kwargs in ({"service_session_id": session_id}, {}):
                try:
                    return create(**kwargs)
                except TypeError:
                    continue
                except Exception:
                    break

        return None

    @staticmethod
    def _reasoning_stage_event(
        stage: str,
        detail: str,
        **extra: Any,
    ) -> Dict[str, Any]:
        """Build a reasoning_stage SSE event."""
        payload: Dict[str, Any] = {"detail": detail}
        payload.update(extra)
        return {
            "type": "reasoning_stage",
            "stage": stage,
            "ts": datetime.now(timezone.utc).isoformat(),
            "payload": payload,
        }

    def run_stream(
        self,
        query: str,
        session_id: Optional[str] = None,
        retrieval_mode: str = "code-rag",
        query_profile: str = "pilot-brief",
        required_sources: Optional[List[str]] = None,
        source_policy: str = "include",
        freshness_sla_minutes: Optional[int] = None,
        explain_retrieval: bool = False,
        risk_mode: str = "standard",
        ask_recommendation: bool = False,
        demo_scenario: Optional[str] = None,
        conversation_history: Optional[List[Dict[str, str]]] = None,
        failure_policy: str = "graceful",
    ) -> Generator[Dict[str, Any], None, None]:
        _t0_total = time.perf_counter()
        _request_budget = float(os.getenv("REQUEST_BUDGET_SECONDS", "220"))
        _deadline = _t0_total + _request_budget
        sid = session_id or str(uuid.uuid4())
        _resolved_route = "UNKNOWN"
        failure_policy = self._normalize_failure_policy(failure_policy)
        source_policy = self._normalize_source_policy(source_policy)
        required_sources = list(required_sources or [])
        source_policy_validation = validate_source_policy_request(required_sources, source_policy)
        required_sources = list(source_policy_validation.get("required_sources_normalized", []))

        yield {
            "type": "agent_update",
            "stage": "start",
            "message": "Agent session initialized",
            "sessionId": sid,
            "framework": self._framework_label,
        }

        if source_policy_validation.get("is_exact") and not source_policy_validation.get("is_valid"):
            error_payload = {
                "type": "agent_error",
                "error_code": source_policy_validation.get("error_code", "exact_required_sources_invalid"),
                "message": source_policy_validation.get("error_message") or "Invalid exact source policy request.",
                "route": "VALIDATION_ERROR",
                "sessionId": sid,
                "framework": self._framework_label,
                "required_sources_raw": list(source_policy_validation.get("required_sources_raw", [])),
                "required_sources_normalized": list(source_policy_validation.get("required_sources_normalized", [])),
                "invalid_required_sources": list(source_policy_validation.get("invalid_required_sources", [])),
                "sourcePolicy": source_policy,
                "requiredSourcesSatisfied": False,
                "failedRequiredSources": list(source_policy_validation.get("required_sources_raw", [])),
                "missingRequiredSources": list(source_policy_validation.get("required_sources_raw", [])),
                "partial": False,
            }
            yield error_payload
            yield {
                "type": "done",
                "route": "VALIDATION_ERROR",
                "isVerified": False,
                "sourcePolicy": source_policy,
                "error_code": error_payload["error_code"],
            }
            return

        if demo_scenario:
            query = self._apply_demo_scenario(query, demo_scenario)
            yield {
                "type": "scenario_loaded",
                "stage": "scenario",
                "scenario": demo_scenario,
                "message": f"Demo scenario loaded: {demo_scenario}",
                "status": "completed",
                "sessionId": sid,
            }
            scenario_alert = self._scenario_operational_alert(demo_scenario)
            if scenario_alert:
                yield scenario_alert

        if freshness_sla_minutes:
            yield {
                "type": "freshness_guardrail",
                "stage": "freshness",
                "message": f"Freshness SLA set to {freshness_sla_minutes} minutes",
                "status": "info",
                "sessionId": sid,
            }

        # --- Reasoning: PII scan starting ---
        yield self._reasoning_stage_event("pii_scan", "Scanning for PII entities...")

        # Run PII check and query routing in parallel — they are independent.
        # For foundry-iq mode, skip routing (the Foundry agent handles its own).
        _t0_parallel = time.perf_counter()
        precomputed_route = None
        _skip_routing = retrieval_mode == "foundry-iq"

        def _pii_task():
            return self.retriever.check_pii(query)

        def _routing_task():
            remaining = _deadline - time.perf_counter()
            if remaining < 60:
                logger.info("Budget tight (%.1fs remaining), using heuristic routing", remaining)
                heuristic = self.retriever.router.quick_route(query)
                return {"route": heuristic, "reasoning": "Heuristic routing (budget tight)", "sources": []}
            intent_graph = self.context_provider.intent_graph_provider.load()
            intent_graph_data = intent_graph.data if intent_graph else None
            return self.retriever.router.smart_route(query, intent_graph=intent_graph_data)

        with _tracer.start_as_current_span("pipeline.pii_routing", attributes={"query.length": len(query), "session.id": sid}) as _pii_span:
            with ThreadPoolExecutor(max_workers=2) as pool:
                pii_future = pool.submit(_pii_task)
                route_future = None if _skip_routing else pool.submit(_routing_task)
                route_done = _skip_routing

                # Poll both futures so we can emit reasoning events as each completes.
                pii_done = False
                pii_result = None
                while not (pii_done and route_done):
                    if not pii_done and pii_future.done():
                        pii_done = True
                        pii_result = pii_future.result()
                        yield self._reasoning_stage_event(
                            "pii_scan",
                            "PII scan complete \u2014 no PII detected" if not pii_result.has_pii else "PII scan complete \u2014 PII detected",
                        )
                    if not route_done and route_future is not None and route_future.done():
                        route_done = True
                        try:
                            precomputed_route = route_future.result()
                        except Exception:
                            precomputed_route = None
                        route_label = (precomputed_route or {}).get("route", "HYBRID")
                        route_reasoning = (precomputed_route or {}).get("reasoning", "")
                        yield self._reasoning_stage_event(
                            "understanding_request",
                            f"Query classified as {route_label}",
                            route=route_label,
                            reasoning=route_reasoning,
                        )
                    if not (pii_done and route_done):
                        time.sleep(0.02)

                # Ensure pii_result is set (should always be by now).
                if pii_result is None:
                    pii_result = pii_future.result()
            _pii_span.set_attribute("pii.detected", pii_result.has_pii)

        logger.info(
            "perf stage=%s ms=%.1f",
            "parallel_pii_routing",
            (time.perf_counter() - _t0_parallel) * 1000,
        )

        if pii_result.has_pii:
            pii_tiered = os.getenv("PII_TIERED_MODE", "false").strip().lower() in {"1", "true", "yes", "y", "on"}
            if pii_tiered:
                high = [e for e in pii_result.entities if e.category in PII_HIGH_SEVERITY]
                low = [e for e in pii_result.entities if e.category in PII_LOW_SEVERITY]
                if high:
                    # High-severity PII → block as before.
                    _pii_block_counter.add(1, {"severity": "high"})
                    warning = self.retriever.pii_filter.format_warning(pii_result.entities)  # type: ignore[attr-defined]
                    for event in self._emit_text_chunks(warning):
                        yield event
                    yield {
                        "type": "agent_done",
                        "isVerified": False,
                        "route": "BLOCKED",
                        "reasoning": "PII policy blocked request (high-severity)",
                        "sessionId": sid,
                        "framework": self._framework_label,
                    }
                    return
                elif low and pii_result.redacted_text:
                    # Low-severity PII → redact and continue.
                    query = pii_result.redacted_text
                    yield {
                        "type": "pii_redacted",
                        "stage": "pii",
                        "message": f"Low-severity PII redacted: {', '.join(set(e.category for e in low))}",
                        "status": "info",
                        "sessionId": sid,
                    }
            else:
                # Non-tiered mode: block all PII.
                _pii_block_counter.add(1, {"severity": "high"})
                warning = self.retriever.pii_filter.format_warning(pii_result.entities)  # type: ignore[attr-defined]
                for event in self._emit_text_chunks(warning):
                    yield event
                yield {
                    "type": "agent_done",
                    "isVerified": False,
                    "route": "BLOCKED",
                    "reasoning": "PII policy blocked request",
                    "sessionId": sid,
                    "framework": self._framework_label,
                }
                return

        # --- Foundry IQ branch: skip AF/local entirely ---
        if retrieval_mode == "foundry-iq":
            try:
                with _tracer.start_as_current_span("pipeline.foundry_iq", attributes={"session.id": sid}):
                    for event in self._run_with_foundry_iq(query, sid, failure_policy=failure_policy):
                        if event.get("type") in {"agent_done", "agent_partial_done"}:
                            _resolved_route = event.get("route", "FOUNDRY_IQ")
                        yield event
            except Exception as exc:
                _error_counter.add(1, {"stage": "foundry_iq"})
                logger.exception("Foundry IQ run failed: %s", exc)
                yield {
                    "type": "agent_error",
                    "message": f"Foundry IQ error: {exc}",
                    "sessionId": sid,
                }
                yield {
                    "type": "agent_done",
                    "isVerified": False,
                    "route": "FOUNDRY_IQ",
                    "reasoning": "Foundry IQ call failed",
                    "sessionId": sid,
                    "framework": "foundry-iq",
                }
            _total_ms = (time.perf_counter() - _t0_total) * 1000
            _query_counter.add(1)
            _route_counter.add(1, {"route": _resolved_route})
            _query_latency.record(_total_ms, {"route": _resolved_route})
            logger.info("perf stage=%s ms=%.1f", "run_stream_total", _total_ms)
            return

        if self._af_enabled:
            try:
                with _tracer.start_as_current_span("pipeline.agent_framework", attributes={"session.id": sid, "framework": self._framework_label}):
                    for event in self._run_with_agent_framework(
                        query,
                        sid,
                        retrieval_mode,
                        query_profile=query_profile,
                        required_sources=required_sources,
                        source_policy=source_policy,
                        freshness_sla_minutes=freshness_sla_minutes,
                        explain_retrieval=explain_retrieval,
                        risk_mode=risk_mode,
                        ask_recommendation=ask_recommendation,
                        demo_scenario=demo_scenario,
                        precomputed_route=precomputed_route,
                        conversation_history=conversation_history,
                        failure_policy=failure_policy,
                        deadline=_deadline,
                    ):
                        if event.get("type") in {"agent_done", "agent_partial_done"}:
                            _resolved_route = event.get("route", "AGENTIC")
                        yield event
                _total_ms = (time.perf_counter() - _t0_total) * 1000
                _query_counter.add(1)
                _route_counter.add(1, {"route": _resolved_route})
                _query_latency.record(_total_ms, {"route": _resolved_route})
                logger.info("perf stage=%s ms=%.1f", "run_stream_total", _total_ms)
                return
            except Exception as exc:
                _error_counter.add(1, {"stage": "agent_framework"})
                logger.exception("Agent Framework run failed")
                yield {
                    "type": "agent_update",
                    "stage": "fallback",
                    "message": "AF runtime failed; switching to local fallback",
                    "sessionId": sid,
                }

        with _tracer.start_as_current_span("pipeline.local", attributes={"session.id": sid}):
            for event in self._run_with_local_pipeline(
                query,
                sid,
                retrieval_mode,
                query_profile=query_profile,
                required_sources=required_sources,
                source_policy=source_policy,
                freshness_sla_minutes=freshness_sla_minutes,
                explain_retrieval=explain_retrieval,
                risk_mode=risk_mode,
                ask_recommendation=ask_recommendation,
                demo_scenario=demo_scenario,
                precomputed_route=precomputed_route,
                conversation_history=conversation_history,
                failure_policy=failure_policy,
                deadline=_deadline,
            ):
                if event.get("type") in {"agent_done", "agent_partial_done"}:
                    _resolved_route = event.get("route", "LOCAL")
                yield event
        _total_ms = (time.perf_counter() - _t0_total) * 1000
        _query_counter.add(1)
        _route_counter.add(1, {"route": _resolved_route})
        _query_latency.record(_total_ms, {"route": _resolved_route})
        logger.info("perf stage=%s ms=%.1f", "run_stream_total", _total_ms)

    def _run_with_foundry_iq(
        self,
        query: str,
        session_id: str,
        failure_policy: str = "graceful",
    ) -> Generator[Dict[str, Any], None, None]:
        """Run query through the Azure AI Foundry Responses API (Foundry IQ mode).

        Emits SSE events compatible with the existing frontend protocol.
        """
        _ = failure_policy  # reserved for future error-handling policy
        now_iso = datetime.now(timezone.utc).isoformat()

        # Lazy-init the client to avoid import/startup cost for code-rag users.
        if self._foundry_client is None:
            from foundry_client import FoundryClient
            self._foundry_client = FoundryClient()

        client = self._foundry_client

        # 1. Reasoning stage
        yield self._reasoning_stage_event(
            "intent_mapped",
            "Routing to Foundry IQ agent...",
            route="FOUNDRY_IQ",
        )

        # 2. Tool call event
        call_id = str(uuid.uuid4())
        yield {
            "type": "tool_call",
            "id": call_id,
            "name": "foundry_iq.query",
            "arguments": {
                "query": query,
                "model": client.model,
                "endpoint": client.endpoint,
            },
        }

        # 3. Call the Foundry Responses API
        _t0 = time.perf_counter()
        try:
            result = client.query(query)
        except Exception as exc:
            logger.error("Foundry IQ query failed: %s", exc)
            yield {
                "type": "tool_result",
                "id": call_id,
                "name": "foundry_iq.query",
                "result": {"error": str(exc)},
            }
            yield {
                "type": "agent_error",
                "message": f"Foundry IQ agent returned an error: {exc}",
                "sessionId": session_id,
            }
            yield {
                "type": "agent_done",
                "isVerified": False,
                "route": "FOUNDRY_IQ",
                "reasoning": f"Foundry IQ call failed: {exc}",
                "sessionId": session_id,
                "framework": "foundry-iq",
            }
            return

        _latency_ms = (time.perf_counter() - _t0) * 1000

        # 4. Tool result
        yield {
            "type": "tool_result",
            "id": call_id,
            "name": "foundry_iq.query",
            "result": {
                "status": "ok",
                "model": result.model,
                "latency_ms": round(result.latency_ms, 1),
                "citation_count": len(result.citations),
            },
        }

        # 5. Stream answer text
        if result.text:
            for event in self._emit_text_chunks(result.text):
                yield event

        # 6. Citations
        if result.citations:
            formatted_citations = []
            for cit in result.citations:
                formatted_citations.append({
                    "id": cit.id,
                    "provider": "FOUNDRY_IQ",
                    "dataset": "foundry-iq-agent",
                    "rowId": f"foundry-ref-{cit.id}",
                    "timestamp": now_iso,
                    "confidence": 0.85,
                    "excerpt": cit.excerpt,
                    "authority": "foundry-agent",
                })
            yield {"type": "citations", "citations": formatted_citations}

        # 7. Evidence check complete
        yield self._reasoning_stage_event(
            "evidence_check_complete",
            f"Foundry IQ response received ({round(_latency_ms)}ms, {len(result.citations)} citations)",
        )

        # 8. Done
        yield {
            "type": "agent_done",
            "isVerified": True,
            "route": "FOUNDRY_IQ",
            "reasoning": "Answered via Foundry IQ agent (Responses API)",
            "sessionId": session_id,
            "framework": "foundry-iq",
            "model": result.model,
        }

    def _run_with_agent_framework(
        self,
        query: str,
        session_id: str,
        retrieval_mode: str,
        query_profile: str,
        required_sources: List[str],
        source_policy: str,
        freshness_sla_minutes: Optional[int],
        explain_retrieval: bool,
        risk_mode: str,
        ask_recommendation: bool,
        demo_scenario: Optional[str],
        precomputed_route: Optional[Dict[str, Any]] = None,
        conversation_history: Optional[List[Dict[str, str]]] = None,
        failure_policy: str = "graceful",
        deadline: float = 0.0,
    ) -> Generator[Dict[str, Any], None, None]:
        session = self._get_or_create_session(session_id)
        call_id = str(uuid.uuid4())

        # --- Reasoning: intent mapped / building retrieval plan ---
        route_label = (precomputed_route or {}).get("route", "HYBRID")
        yield self._reasoning_stage_event(
            "intent_mapped",
            "Building retrieval plan...",
            route=route_label,
            sources=list((precomputed_route or {}).get("sources", [])),
        )

        yield {
            "type": "tool_call",
            "id": call_id,
            "name": "context_provider.build_context",
            "arguments": {
                "query": query,
                "retrieval_mode": retrieval_mode,
                "query_profile": query_profile,
                "required_sources": required_sources,
                "source_policy": source_policy,
                "freshness_sla_minutes": freshness_sla_minutes,
                "explain_retrieval": explain_retrieval,
                "risk_mode": risk_mode,
                "ask_recommendation": ask_recommendation,
                "demo_scenario": demo_scenario,
            },
        }

        # Run build_context in a thread so we can drain source traces in real-time.
        trace_queue: queue.Queue[Optional[Dict[str, Any]]] = queue.Queue()
        ctx_holder: List[Any] = []  # mutable container for thread result
        exc_holder: List[Exception] = []
        _t0_ctx = time.perf_counter()

        def _build_ctx() -> None:
            try:
                ctx = self.context_provider.build_context(
                    query,
                    retrieval_mode=retrieval_mode,
                    query_profile=query_profile,
                    required_sources=required_sources,
                    source_policy=source_policy,
                    freshness_sla_minutes=freshness_sla_minutes,
                    explain_retrieval=explain_retrieval,
                    risk_mode=risk_mode,
                    ask_recommendation=ask_recommendation,
                    precomputed_route=precomputed_route,
                    on_trace=trace_queue.put,
                )
                ctx_holder.append(ctx)
            except ExactPolicyValidationError as exc:
                exc_holder.append(exc)
            except Exception as exc:
                exc_holder.append(exc)
            finally:
                trace_queue.put(None)  # sentinel

        ctx_thread = threading.Thread(target=_build_ctx, daemon=True)
        ctx_thread.start()

        # Drain trace queue in real-time — emit source_call_* events as they arrive.
        traces_streamed = False
        while True:
            try:
                trace_event = trace_queue.get(timeout=0.05)
            except queue.Empty:
                continue
            if trace_event is None:
                break
            traces_streamed = True
            for item in self._emit_source_trace_events(trace_event):
                yield item

        ctx_thread.join()
        if exc_holder:
            if isinstance(exc_holder[0], ExactPolicyValidationError):
                exact_error = exc_holder[0]
                yield {
                    "type": "agent_error",
                    "error_code": "exact_required_sources_invalid",
                    "message": str(exact_error),
                    "route": "VALIDATION_ERROR",
                    "sessionId": session_id,
                    "framework": self._framework_label,
                    "required_sources_raw": list(getattr(exact_error, "required_sources_raw", [])),
                    "required_sources_normalized": list(getattr(exact_error, "required_sources_normalized", [])),
                    "invalid_required_sources": list(getattr(exact_error, "invalid_required_sources", [])),
                    "sourcePolicy": source_policy,
                    "requiredSourcesSatisfied": False,
                    "failedRequiredSources": list(getattr(exact_error, "required_sources_raw", [])),
                    "missingRequiredSources": list(getattr(exact_error, "required_sources_raw", [])),
                    "partial": False,
                }
                return
            raise exc_holder[0]
        ctx = ctx_holder[0]

        logger.info("perf stage=%s ms=%.1f", "build_context_af", (time.perf_counter() - _t0_ctx) * 1000)
        if ctx.retrieval_plan:
            yield {"type": "retrieval_plan", "plan": ctx.retrieval_plan}
        # Only emit batch traces if on_trace callback was not used (backward compat).
        if not traces_streamed:
            for trace in ctx.source_traces:
                for item in self._emit_source_trace_events(trace):
                    yield item

        # --- Reasoning: drafting brief ---
        yield self._reasoning_stage_event(
            "drafting_brief",
            "Synthesizing answer from evidence...",
            route=ctx.route,
        )

        yield {
            "type": "tool_result",
            "id": call_id,
            "name": "context_provider.build_context",
            "result": ctx.to_event_payload(),
        }

        prompt = (
            "Use the retrieval context below to answer the user. "
            "If context is insufficient, say that clearly.\n\n"
            f"{ctx.context_text}\n\n"
            f"User question: {query}"
        )

        # Deadline guard: skip synthesis if budget is exhausted.
        _remaining = deadline - time.perf_counter() if deadline > 0 else float("inf")
        if _remaining < 10:
            logger.warning("Budget exhausted (%.1fs remaining), skipping AF synthesis", _remaining)
            degraded_sources, failed_required_sources = self._summarize_source_outcomes(
                ctx.source_results, required_sources=required_sources,
            )
            for event in self._emit_no_answer_fallback_text(
                route=ctx.route, degraded_sources=degraded_sources,
                failed_required_sources=failed_required_sources,
                required_sources_satisfied=len(failed_required_sources) == 0,
                failure_policy=failure_policy, source_policy=source_policy,
            ):
                yield event
            yield self._build_partial_done_event(
                route=ctx.route, reasoning="Budget exhausted before synthesis",
                session_id=session_id, framework=self._framework_label,
                degraded_sources=degraded_sources,
                failed_required_sources=failed_required_sources,
                required_sources_satisfied=len(failed_required_sources) == 0,
                failure_policy=failure_policy, source_policy=source_policy,
            )
            return

        answer = self._invoke_agent(prompt=prompt, session=session, session_id=session_id)
        degraded_sources, failed_required_sources = self._summarize_source_outcomes(
            ctx.source_results,
            required_sources=required_sources,
        )
        required_sources_satisfied = len(failed_required_sources) == 0
        if not answer.strip():
            # AF agent returned nothing — use true streaming synthesis.
            _filter = self.retriever._filter_error_rows
            vector_rows = (
                _filter(list(ctx.source_results.get("VECTOR_REG", [])))
                + _filter(list(ctx.source_results.get("VECTOR_OPS", [])))
                + _filter(list(ctx.source_results.get("VECTOR_AIRPORT", [])))
            )
            synthesis_context: Dict[str, Any] = {
                "sql_results": _filter(ctx.sql_results[:12]),
                "kql_results": _filter(ctx.source_results.get("KQL", [])[:8]),
                "graph_results": _filter(ctx.source_results.get("GRAPH", [])[:8]),
                "nosql_results": _filter(ctx.source_results.get("NOSQL", [])[:8]),
                "vector_results": [
                    {k: str(v)[:200] for k, v in row.items() if k != "content_vector"}
                    for row in vector_rows[:12]
                ],
                "reconciled_items": ctx.reconciled_items[:40],
                "coverage_summary": ctx.coverage_summary,
                "conflict_summary": ctx.conflict_summary,
            }
            budget = int(os.getenv("SYNTHESIS_TOKEN_BUDGET", "6000"))
            if budget > 0:
                synthesis_context = _truncate_context_to_budget(synthesis_context, budget)
            af_answer_parts: List[str] = []
            synthesis_terminal_error: Optional[Dict[str, Any]] = None
            for event in self.retriever._synthesize_answer_stream(query, synthesis_context, ctx.route, conversation_history=conversation_history, deadline=deadline):
                if event.get("type") == "agent_update" and event.get("content"):
                    af_answer_parts.append(str(event["content"]))
                    yield event
                    continue
                if event.get("type") in {"agent_error", "error"}:
                    synthesis_terminal_error = self._normalize_synthesis_error_event(
                        dict(event),
                        route=ctx.route,
                        session_id=session_id,
                        framework=self._framework_label,
                        degraded_sources=degraded_sources,
                        failed_required_sources=failed_required_sources,
                        required_sources_satisfied=required_sources_satisfied,
                        failure_policy=failure_policy,
                        source_policy=source_policy,
                    )
                    yield synthesis_terminal_error
                    break
                yield event
            af_answer_text = "".join(af_answer_parts)
            if synthesis_terminal_error:
                if not af_answer_text.strip():
                    for event in self._emit_no_answer_fallback_text(
                        route=ctx.route,
                        degraded_sources=degraded_sources,
                        failed_required_sources=failed_required_sources,
                        required_sources_satisfied=required_sources_satisfied,
                        failure_policy=failure_policy,
                        source_policy=source_policy,
                    ):
                        yield event
                yield self._reasoning_stage_event(
                    "evidence_check_complete",
                    "Evidence verification complete",
                    verification="Partial",
                    failOpen=True,
                    route=ctx.route,
                )
                yield self._build_partial_done_event(
                    route=ctx.route,
                    reasoning=ctx.reasoning,
                    session_id=session_id,
                    framework=self._framework_label,
                    degraded_sources=degraded_sources,
                    failed_required_sources=failed_required_sources,
                    required_sources_satisfied=required_sources_satisfied,
                    failure_policy=failure_policy,
                    source_policy=source_policy,
                )
                return
        else:
            af_answer_text = answer
            for event in self._emit_text_chunks(answer):
                yield event

        if ctx.citations:
            yield {"type": "citations", "citations": self._format_citations(ctx.citations)}

        # Use actual evidence coverage to determine verification status.
        required_total = ctx.coverage_summary.get("required_total", 0) if ctx.coverage_summary else 0
        required_filled = ctx.coverage_summary.get("required_filled", 0) if ctx.coverage_summary else 0
        if required_total > 0:
            is_verified = required_filled == required_total
        else:
            is_verified = len(ctx.citations) > 0

        if not af_answer_text.strip():
            for event in self._emit_no_answer_fallback_text(
                route=ctx.route,
                degraded_sources=degraded_sources,
                failed_required_sources=failed_required_sources,
                required_sources_satisfied=required_sources_satisfied,
                failure_policy=failure_policy,
                source_policy=source_policy,
            ):
                yield event
            yield self._reasoning_stage_event(
                "evidence_check_complete",
                "Evidence verification complete",
                verification="Partial",
                failOpen=True,
                route=ctx.route,
            )
            yield self._build_terminal_agent_error(
                route=ctx.route,
                session_id=session_id,
                framework=self._framework_label,
                message=EMPTY_SYNTHESIS_ERROR,
                error_code="empty_synthesis_output",
                terminal_reason="empty_synthesis_output",
                degraded_sources=degraded_sources,
                failed_required_sources=failed_required_sources,
                required_sources_satisfied=required_sources_satisfied,
                failure_policy=failure_policy,
                source_policy=source_policy,
            )
            yield self._build_partial_done_event(
                route=ctx.route,
                reasoning=ctx.reasoning,
                session_id=session_id,
                framework=self._framework_label,
                degraded_sources=degraded_sources,
                failed_required_sources=failed_required_sources,
                required_sources_satisfied=required_sources_satisfied,
                failure_policy=failure_policy,
                source_policy=source_policy,
            )
            return

        grounding = _check_answer_grounding(af_answer_text, len(ctx.citations))

        # --- Reasoning: evidence check complete ---
        yield self._reasoning_stage_event(
            "evidence_check_complete",
            "Evidence verification complete",
            verification="Verified" if is_verified else "Partial",
            failOpen=not is_verified,
            route=ctx.route,
        )

        if failure_policy == "strict" and degraded_sources:
            detail = ", ".join(degraded_sources)
            yield {
                "type": "agent_error",
                "message": f"Strict failure policy triggered by source errors: {detail}",
                "error_code": "strict_failure_policy_triggered",
                "terminal_reason": "strict_failure_policy_triggered",
                "route": ctx.route,
                "sessionId": session_id,
                "framework": self._framework_label,
                "degradedSources": degraded_sources,
                "failedRequiredSources": failed_required_sources,
                "requiredSourcesSatisfied": required_sources_satisfied,
                "missingRequiredSources": failed_required_sources,
                "failurePolicy": failure_policy,
                "sourcePolicy": source_policy,
            }
            yield self._build_partial_done_event(
                route=ctx.route,
                reasoning=ctx.reasoning,
                session_id=session_id,
                framework=self._framework_label,
                degraded_sources=degraded_sources,
                failed_required_sources=failed_required_sources,
                required_sources_satisfied=required_sources_satisfied,
                failure_policy=failure_policy,
                source_policy=source_policy,
            )
            return

        if degraded_sources:
            yield {
                "type": "agent_partial_done",
                "isVerified": is_verified,
                "route": ctx.route,
                "reasoning": ctx.reasoning,
                "sessionId": session_id,
                "framework": self._framework_label,
                "grounding": grounding,
                "degradedSources": degraded_sources,
                "failedRequiredSources": failed_required_sources,
                "requiredSourcesSatisfied": required_sources_satisfied,
                "missingRequiredSources": failed_required_sources,
                "fatalSourceCount": len(degraded_sources),
                "failurePolicy": failure_policy,
                "sourcePolicy": source_policy,
                "partial": True,
            }

        yield {
            "type": "agent_done",
            "isVerified": is_verified,
            "route": ctx.route,
            "reasoning": ctx.reasoning,
            "sessionId": session_id,
            "framework": self._framework_label,
            "grounding": grounding,
            "degradedSources": degraded_sources,
            "failedRequiredSources": failed_required_sources,
            "requiredSourcesSatisfied": required_sources_satisfied,
            "missingRequiredSources": failed_required_sources,
            "fatalSourceCount": len(degraded_sources),
            "failurePolicy": failure_policy,
            "sourcePolicy": source_policy,
            "partial": bool(degraded_sources),
        }

    def _run_with_local_pipeline(
        self,
        query: str,
        session_id: str,
        retrieval_mode: str,
        query_profile: str,
        required_sources: List[str],
        source_policy: str,
        freshness_sla_minutes: Optional[int],
        explain_retrieval: bool,
        risk_mode: str,
        ask_recommendation: bool,
        demo_scenario: Optional[str],
        precomputed_route: Optional[Dict[str, Any]] = None,
        conversation_history: Optional[List[Dict[str, str]]] = None,
        failure_policy: str = "graceful",
        deadline: float = 0.0,
    ) -> Generator[Dict[str, Any], None, None]:
        call_id = str(uuid.uuid4())

        # --- Reasoning: intent mapped / building retrieval plan ---
        route_label = (precomputed_route or {}).get("route", "HYBRID")
        yield self._reasoning_stage_event(
            "intent_mapped",
            "Building retrieval plan...",
            route=route_label,
            sources=list((precomputed_route or {}).get("sources", [])),
        )

        yield {
            "type": "tool_call",
            "id": call_id,
            "name": "run_rag_lookup",
            "arguments": {
                "query": query,
                "retrieval_mode": retrieval_mode,
                "query_profile": query_profile,
                "required_sources": required_sources,
                "source_policy": source_policy,
                "freshness_sla_minutes": freshness_sla_minutes,
                "explain_retrieval": explain_retrieval,
                "risk_mode": risk_mode,
                "ask_recommendation": ask_recommendation,
                "demo_scenario": demo_scenario,
            },
        }

        # Run run_rag_lookup in a thread so we can drain source traces in real-time.
        trace_queue: queue.Queue[Optional[Dict[str, Any]]] = queue.Queue()
        result_holder: List[Dict[str, Any]] = []
        exc_holder: List[Exception] = []
        _t0_rag = time.perf_counter()

        def _run_lookup() -> None:
            try:
                tool_result = self.toolset.run_rag_lookup(
                    query,
                    retrieval_mode=retrieval_mode,
                    query_profile=query_profile,
                    required_sources=required_sources,
                    source_policy=source_policy,
                    freshness_sla_minutes=freshness_sla_minutes,
                    explain_retrieval=explain_retrieval,
                    risk_mode=risk_mode,
                    ask_recommendation=ask_recommendation,
                    precomputed_route=precomputed_route,
                    on_trace=trace_queue.put,
                )
                result_holder.append(tool_result)
            except Exception as exc:
                exc_holder.append(exc)
            finally:
                trace_queue.put(None)  # sentinel

        lookup_thread = threading.Thread(target=_run_lookup, daemon=True)
        lookup_thread.start()

        # Drain trace queue in real-time.
        traces_streamed = False
        while True:
            try:
                trace_event = trace_queue.get(timeout=0.05)
            except queue.Empty:
                continue
            if trace_event is None:
                break
            traces_streamed = True
            for item in self._emit_source_trace_events(trace_event):
                yield item

        lookup_thread.join()
        if exc_holder:
            exc = exc_holder[0]
            route = route_label if isinstance(route_label, str) and route_label else "AGENTIC"
            logger.error(
                "Local pipeline rag lookup failed",
                exc_info=(type(exc), exc, exc.__traceback__),
            )
            yield {
                "type": "agent_error",
                "message": "RAG lookup failed before synthesis. Please retry.",
                "error_code": "rag_lookup_failed",
                "terminal_reason": "rag_lookup_failed",
                "route": route,
                "sessionId": session_id,
                "framework": "local-fallback",
                "degradedSources": [],
                "failedRequiredSources": [],
                "requiredSourcesSatisfied": len(required_sources) == 0,
                "missingRequiredSources": list(required_sources),
                "failurePolicy": failure_policy,
                "sourcePolicy": source_policy,
                "partial": False,
            }
            yield self._build_partial_done_event(
                route=route,
                reasoning="rag_lookup_failed",
                session_id=session_id,
                framework="local-fallback",
                degraded_sources=[],
                failed_required_sources=[],
                required_sources_satisfied=len(required_sources) == 0,
                failure_policy=failure_policy,
                source_policy=source_policy,
            )
            return
        tool_result = result_holder[0]

        logger.info("perf stage=%s ms=%.1f", "rag_lookup", (time.perf_counter() - _t0_rag) * 1000)
        route = tool_result.get("route", "HYBRID")
        reasoning = tool_result.get("reasoning", "Fallback retrieval path")
        sql_results = tool_result.get("sql_results", [])
        semantic_results = tool_result.get("semantic_results", [])
        source_results = tool_result.get("source_results", {}) or {}
        reconciled_items = tool_result.get("reconciled_items", []) or []
        coverage_summary = tool_result.get("coverage_summary", {}) or {}
        conflict_summary = tool_result.get("conflict_summary", {}) or {}
        citations_payload = tool_result.get("citations", [])
        if tool_result.get("retrieval_plan"):
            yield {"type": "retrieval_plan", "plan": tool_result.get("retrieval_plan")}
        # Only emit batch traces if on_trace was not used.
        if not traces_streamed:
            for trace in tool_result.get("source_traces", []):
                for item in self._emit_source_trace_events(trace):
                    yield item

        # --- Reasoning: drafting brief ---
        yield self._reasoning_stage_event(
            "drafting_brief",
            "Synthesizing answer from evidence...",
            route=route,
        )

        yield {
            "type": "tool_result",
            "id": call_id,
            "name": "run_rag_lookup",
            "result": {
                "route": route,
                "reasoning": reasoning,
                "sql_result_count": len(sql_results),
                "semantic_result_count": len(semantic_results),
                "citation_count": len(citations_payload),
                "source_result_counts": {
                    src: len(rows)
                    for src, rows in source_results.items()
                },
                "reconciled_item_count": len(reconciled_items),
                "coverage_summary": coverage_summary,
                "conflict_summary": conflict_summary,
            },
        }

        _filter = self.retriever._filter_error_rows
        vector_rows = (
            _filter(list(source_results.get("VECTOR_REG", [])))
            + _filter(list(source_results.get("VECTOR_OPS", [])))
            + _filter(list(source_results.get("VECTOR_AIRPORT", [])))
        )
        synthesis_context: Dict[str, Any] = {
            "sql_results": _filter(sql_results[:12]),
            "kql_results": _filter(source_results.get("KQL", [])[:8]),
            "graph_results": _filter(source_results.get("GRAPH", [])[:8]),
            "nosql_results": _filter(source_results.get("NOSQL", [])[:8]),
            "vector_results": [
                {k: str(v)[:200] for k, v in row.items() if k != "content_vector"}
                for row in vector_rows[:12]
            ],
            "reconciled_items": reconciled_items[:40],
            "coverage_summary": coverage_summary,
            "conflict_summary": conflict_summary,
        }
        budget = int(os.getenv("SYNTHESIS_TOKEN_BUDGET", "6000"))
        if budget > 0:
            synthesis_context = _truncate_context_to_budget(synthesis_context, budget)

        degraded_sources, failed_required_sources = self._summarize_source_outcomes(
            source_results,
            required_sources=required_sources,
        )
        required_sources_satisfied = len(failed_required_sources) == 0

        # Deadline guard: skip synthesis if budget is exhausted.
        _remaining = deadline - time.perf_counter() if deadline > 0 else float("inf")
        if _remaining < 10:
            logger.warning("Budget exhausted (%.1fs remaining), skipping local synthesis", _remaining)
            for event in self._emit_no_answer_fallback_text(
                route=route, degraded_sources=degraded_sources,
                failed_required_sources=failed_required_sources,
                required_sources_satisfied=required_sources_satisfied,
                failure_policy=failure_policy, source_policy=source_policy,
            ):
                yield event
            yield self._build_partial_done_event(
                route=route, reasoning="Budget exhausted before synthesis",
                session_id=session_id, framework="local-fallback",
                degraded_sources=degraded_sources,
                failed_required_sources=failed_required_sources,
                required_sources_satisfied=required_sources_satisfied,
                failure_policy=failure_policy, source_policy=source_policy,
            )
            return

        # True streaming: yield tokens as they arrive from the LLM.
        local_answer_parts: List[str] = []
        synthesis_terminal_error: Optional[Dict[str, Any]] = None
        for event in self.retriever._synthesize_answer_stream(query, synthesis_context, route, conversation_history=conversation_history, deadline=deadline):
            if event.get("type") == "agent_update" and event.get("content"):
                local_answer_parts.append(str(event["content"]))
                yield event
                continue
            if event.get("type") in {"agent_error", "error"}:
                synthesis_terminal_error = self._normalize_synthesis_error_event(
                    dict(event),
                    route=route,
                    session_id=session_id,
                    framework="local-fallback",
                    degraded_sources=degraded_sources,
                    failed_required_sources=failed_required_sources,
                    required_sources_satisfied=required_sources_satisfied,
                    failure_policy=failure_policy,
                    source_policy=source_policy,
                )
                yield synthesis_terminal_error
                break
            yield event
        local_answer_text = "".join(local_answer_parts)
        if synthesis_terminal_error:
            if not local_answer_text.strip():
                for event in self._emit_no_answer_fallback_text(
                    route=route,
                    degraded_sources=degraded_sources,
                    failed_required_sources=failed_required_sources,
                    required_sources_satisfied=required_sources_satisfied,
                    failure_policy=failure_policy,
                    source_policy=source_policy,
                ):
                    yield event
            yield self._reasoning_stage_event(
                "evidence_check_complete",
                "Evidence verification complete",
                verification="Partial",
                failOpen=True,
                route=route,
            )
            yield self._build_partial_done_event(
                route=route,
                reasoning=reasoning,
                session_id=session_id,
                framework="local-fallback",
                degraded_sources=degraded_sources,
                failed_required_sources=failed_required_sources,
                required_sources_satisfied=required_sources_satisfied,
                failure_policy=failure_policy,
                source_policy=source_policy,
            )
            return

        if citations_payload:
            citations = [
                Citation(
                    source_type=c.get("source_type", "SEMANTIC"),
                    identifier=c.get("identifier", ""),
                    title=c.get("title", ""),
                    content_preview=c.get("content_preview", ""),
                    score=float(c.get("score", 0.0)),
                )
                for c in citations_payload
            ]
            yield {"type": "citations", "citations": self._format_citations(citations)}

        # Use actual evidence coverage to determine verification status.
        local_required_total = coverage_summary.get("required_total", 0) if coverage_summary else 0
        local_required_filled = coverage_summary.get("required_filled", 0) if coverage_summary else 0
        if local_required_total > 0:
            local_is_verified = local_required_filled == local_required_total
        else:
            local_is_verified = len(citations_payload) > 0

        if not local_answer_text.strip():
            for event in self._emit_no_answer_fallback_text(
                route=route,
                degraded_sources=degraded_sources,
                failed_required_sources=failed_required_sources,
                required_sources_satisfied=required_sources_satisfied,
                failure_policy=failure_policy,
                source_policy=source_policy,
            ):
                yield event
            yield self._reasoning_stage_event(
                "evidence_check_complete",
                "Evidence verification complete",
                verification="Partial",
                failOpen=True,
                route=route,
            )
            yield self._build_terminal_agent_error(
                route=route,
                session_id=session_id,
                framework="local-fallback",
                message=EMPTY_SYNTHESIS_ERROR,
                error_code="empty_synthesis_output",
                terminal_reason="empty_synthesis_output",
                degraded_sources=degraded_sources,
                failed_required_sources=failed_required_sources,
                required_sources_satisfied=required_sources_satisfied,
                failure_policy=failure_policy,
                source_policy=source_policy,
            )
            yield self._build_partial_done_event(
                route=route,
                reasoning=reasoning,
                session_id=session_id,
                framework="local-fallback",
                degraded_sources=degraded_sources,
                failed_required_sources=failed_required_sources,
                required_sources_satisfied=required_sources_satisfied,
                failure_policy=failure_policy,
                source_policy=source_policy,
            )
            return

        local_grounding = _check_answer_grounding(local_answer_text, len(citations_payload))

        # --- Reasoning: evidence check complete ---
        yield self._reasoning_stage_event(
            "evidence_check_complete",
            "Evidence verification complete",
            verification="Verified" if local_is_verified else "Partial",
            failOpen=not local_is_verified,
            route=route,
        )

        if failure_policy == "strict" and degraded_sources:
            detail = ", ".join(degraded_sources)
            yield {
                "type": "agent_error",
                "message": f"Strict failure policy triggered by source errors: {detail}",
                "error_code": "strict_failure_policy_triggered",
                "terminal_reason": "strict_failure_policy_triggered",
                "route": route,
                "sessionId": session_id,
                "framework": "local-fallback",
                "degradedSources": degraded_sources,
                "failedRequiredSources": failed_required_sources,
                "requiredSourcesSatisfied": required_sources_satisfied,
                "missingRequiredSources": failed_required_sources,
                "failurePolicy": failure_policy,
                "sourcePolicy": source_policy,
            }
            yield self._build_partial_done_event(
                route=route,
                reasoning=reasoning,
                session_id=session_id,
                framework="local-fallback",
                degraded_sources=degraded_sources,
                failed_required_sources=failed_required_sources,
                required_sources_satisfied=required_sources_satisfied,
                failure_policy=failure_policy,
                source_policy=source_policy,
            )
            return

        if degraded_sources:
            yield {
                "type": "agent_partial_done",
                "isVerified": local_is_verified,
                "route": route,
                "reasoning": reasoning,
                "sessionId": session_id,
                "framework": "local-fallback",
                "grounding": local_grounding,
                "degradedSources": degraded_sources,
                "failedRequiredSources": failed_required_sources,
                "requiredSourcesSatisfied": required_sources_satisfied,
                "missingRequiredSources": failed_required_sources,
                "fatalSourceCount": len(degraded_sources),
                "failurePolicy": failure_policy,
                "sourcePolicy": source_policy,
                "partial": True,
            }

        yield {
            "type": "agent_done",
            "isVerified": local_is_verified,
            "route": route,
            "reasoning": reasoning,
            "sessionId": session_id,
            "framework": "local-fallback",
            "grounding": local_grounding,
            "degradedSources": degraded_sources,
            "failedRequiredSources": failed_required_sources,
            "requiredSourcesSatisfied": required_sources_satisfied,
            "missingRequiredSources": failed_required_sources,
            "fatalSourceCount": len(degraded_sources),
            "failurePolicy": failure_policy,
            "sourcePolicy": source_policy,
            "partial": bool(degraded_sources),
        }

    @staticmethod
    def _normalize_failure_policy(value: str) -> str:
        normalized = (value or "").strip().lower()
        if normalized in {"strict", "graceful"}:
            return normalized
        return "graceful"

    @staticmethod
    def _normalize_source_policy(value: str) -> str:
        return normalize_source_policy(value)

    def _summarize_source_outcomes(
        self,
        source_results: Dict[str, List[Dict[str, Any]]],
        required_sources: List[str],
    ) -> tuple[List[str], List[str]]:
        degraded_sources: List[str] = []
        for source, rows in source_results.items():
            if not isinstance(rows, list):
                continue
            has_errors = False
            has_success = False
            for row in rows:
                if not isinstance(row, dict):
                    continue
                if row.get("error") or row.get("error_code"):
                    has_errors = True
                else:
                    has_success = True

            # Strict-mode degradation should reflect source-level failure, not
            # mixed outcomes where at least one successful row exists.
            if has_errors and not has_success:
                degraded_sources.append(source)

        required_upper = {str(src).upper() for src in required_sources if str(src).strip()}
        failed_required_sources = [
            source
            for source in degraded_sources
            if str(source).upper() in required_upper
        ]
        return sorted(set(degraded_sources)), sorted(set(failed_required_sources))

    def _invoke_agent(self, prompt: str, session: Any, session_id: str) -> str:
        if self._agent is None:
            return ""

        # Prefer streaming APIs when available, then fallback to a single run call.
        if hasattr(self._agent, "run_stream"):
            stream = self._call_with_common_kwargs(self._agent.run_stream, prompt, session, session_id)
            streamed = self._consume_stream(stream)
            if streamed:
                return streamed

        if hasattr(self._agent, "run"):
            result = self._call_with_common_kwargs(self._agent.run, prompt, session, session_id)
            return self._extract_text(result)

        if hasattr(self._agent, "invoke"):
            result = self._call_with_common_kwargs(self._agent.invoke, prompt, session, session_id)
            return self._extract_text(result)

        return ""

    @staticmethod
    def _build_terminal_agent_error(
        *,
        route: str,
        session_id: str,
        framework: str,
        message: str,
        error_code: str,
        terminal_reason: str,
        degraded_sources: List[str],
        failed_required_sources: List[str],
        required_sources_satisfied: bool,
        failure_policy: str,
        source_policy: str,
    ) -> Dict[str, Any]:
        return {
            "type": "agent_error",
            "message": message,
            "error_code": error_code,
            "terminal_reason": terminal_reason,
            "route": route,
            "sessionId": session_id,
            "framework": framework,
            "degradedSources": degraded_sources,
            "failedRequiredSources": failed_required_sources,
            "requiredSourcesSatisfied": required_sources_satisfied,
            "missingRequiredSources": failed_required_sources,
            "failurePolicy": failure_policy,
            "sourcePolicy": source_policy,
            "partial": False,
        }

    @staticmethod
    def _build_partial_done_event(
        *,
        route: str,
        reasoning: str,
        session_id: str,
        framework: str,
        degraded_sources: List[str],
        failed_required_sources: List[str],
        required_sources_satisfied: bool,
        failure_policy: str,
        source_policy: str,
    ) -> Dict[str, Any]:
        return {
            "type": "agent_partial_done",
            "isVerified": False,
            "route": route,
            "reasoning": reasoning,
            "sessionId": session_id,
            "framework": framework,
            "degradedSources": degraded_sources,
            "failedRequiredSources": failed_required_sources,
            "requiredSourcesSatisfied": required_sources_satisfied,
            "missingRequiredSources": failed_required_sources,
            "fatalSourceCount": len(degraded_sources),
            "failurePolicy": failure_policy,
            "sourcePolicy": source_policy,
            "partial": True,
        }

    def _normalize_synthesis_error_event(
        self,
        event: Dict[str, Any],
        *,
        route: str,
        session_id: str,
        framework: str,
        degraded_sources: List[str],
        failed_required_sources: List[str],
        required_sources_satisfied: bool,
        failure_policy: str,
        source_policy: str,
    ) -> Dict[str, Any]:
        error_code = str(event.get("error_code") or event.get("errorCode") or "synthesis_runtime_error")
        raw_message = str(event.get("message") or event.get("error") or "Synthesis failed.")
        message = self._public_error_message(raw_message, error_code)
        terminal_reason = str(event.get("terminal_reason") or error_code)
        return self._build_terminal_agent_error(
            route=route,
            session_id=session_id,
            framework=framework,
            message=message,
            error_code=error_code,
            terminal_reason=terminal_reason,
            degraded_sources=degraded_sources,
            failed_required_sources=failed_required_sources,
            required_sources_satisfied=required_sources_satisfied,
            failure_policy=failure_policy,
            source_policy=source_policy,
        )

    @staticmethod
    def _public_error_message(message: str, error_code: str) -> str:
        text = (message or "").strip()
        code = (error_code or "").strip().lower()
        if not text:
            return "An internal error occurred while preparing the response."

        # Keep user-intentful refusals explicit; sanitize backend/runtime failures.
        if code in {"llm_refusal", "policy_refusal"}:
            return text

        if code in {
            "synthesis_runtime_error",
            "rag_lookup_failed",
            "tool_runtime_error",
            "execution_exception",
        }:
            return "An internal error occurred while preparing the response."

        lowered = text.lower()
        sensitive_tokens = (
            "traceback",
            "exception",
            "stack",
            "sqlstate",
            "pyodbc",
            "connection",
            "token",
            "secret",
            "http_",
            "azure.",
            "fabric_",
            "cosmos",
        )
        if any(token in lowered for token in sensitive_tokens):
            return "An internal error occurred while preparing the response."
        return text

    def _emit_no_answer_fallback_text(
        self,
        *,
        route: str,
        degraded_sources: List[str],
        failed_required_sources: List[str],
        required_sources_satisfied: bool,
        failure_policy: str,
        source_policy: str,
    ) -> Generator[Dict[str, Any], None, None]:
        message = self._build_no_answer_fallback_text(
            route=route,
            degraded_sources=degraded_sources,
            failed_required_sources=failed_required_sources,
            required_sources_satisfied=required_sources_satisfied,
            failure_policy=failure_policy,
            source_policy=source_policy,
        )
        for event in self._emit_text_chunks(message):
            yield event

    @staticmethod
    def _build_no_answer_fallback_text(
        *,
        route: str,
        degraded_sources: List[str],
        failed_required_sources: List[str],
        required_sources_satisfied: bool,
        failure_policy: str,
        source_policy: str,
    ) -> str:
        route_label = str(route or "UNKNOWN")
        failure_label = str(failure_policy or "graceful")
        policy_label = str(source_policy or "include")

        lines = [
            "I could not produce a full synthesized brief from the retrieved evidence.",
            f"Status: partial fail-open (route={route_label}, failurePolicy={failure_label}, sourcePolicy={policy_label}).",
        ]
        if degraded_sources:
            lines.append("Degraded sources: " + ", ".join(sorted(set(str(s) for s in degraded_sources if s))) + ".")
        if failed_required_sources:
            lines.append(
                "Required sources still failing: "
                + ", ".join(sorted(set(str(s) for s in failed_required_sources if s)))
                + "."
            )
        elif required_sources_satisfied:
            lines.append("At least one call succeeded for each required source.")
        lines.append("Retry with a narrower source scope or rerun shortly to refresh live evidence.")
        return " ".join(line for line in lines if line)

    def _call_with_common_kwargs(self, fn: Any, prompt: str, session: Any, session_id: str) -> Any:
        attempts = (
            {"session": session},
            {"session_id": session_id},
            {"conversation_id": session_id},
            {},
        )
        for kwargs in attempts:
            try:
                value = fn(prompt, **kwargs)
                return self._resolve_awaitable(value)
            except TypeError:
                continue
        value = fn(prompt)
        return self._resolve_awaitable(value)

    def _resolve_awaitable(self, value: Any) -> Any:
        if inspect.isawaitable(value):
            return asyncio.run(value)
        return value

    def _consume_stream(self, stream_obj: Any) -> str:
        if stream_obj is None:
            return ""

        chunks: List[str] = []

        if hasattr(stream_obj, "__aiter__"):
            async def consume_async() -> List[str]:
                parts: List[str] = []
                async for item in stream_obj:
                    text = self._extract_text(item)
                    if text:
                        parts.append(text)
                return parts

            chunks.extend(asyncio.run(consume_async()))
            return "".join(chunks).strip()

        if isinstance(stream_obj, Iterable) and not isinstance(stream_obj, (str, bytes, dict)):
            for item in stream_obj:
                text = self._extract_text(item)
                if text:
                    chunks.append(text)
            return "".join(chunks).strip()

        return self._extract_text(stream_obj)

    def _extract_text(self, value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, str):
            return value
        if isinstance(value, dict):
            for key in ("content", "text", "message", "answer", "data"):
                field = value.get(key)
                if isinstance(field, str):
                    return field
            return str(value)

        for attr in ("content", "text", "message", "answer"):
            if hasattr(value, attr):
                field = getattr(value, attr)
                if isinstance(field, str):
                    return field

        if hasattr(value, "data"):
            data = getattr(value, "data")
            if isinstance(data, str):
                return data
            if isinstance(data, dict):
                return self._extract_text(data)

        return str(value)

    def _emit_text_chunks(self, text: str, chunk_size: int = 80) -> Generator[Dict[str, Any], None, None]:
        if not text:
            return

        # Split into chunks at word boundaries while preserving all whitespace
        # (newlines, tabs, multiple spaces) so markdown formatting survives.
        pos = 0
        length = len(text)
        while pos < length:
            end = min(pos + chunk_size, length)
            if end < length:
                # Try to break at a space boundary to avoid splitting words.
                space_idx = text.rfind(" ", pos, end)
                newline_idx = text.rfind("\n", pos, end)
                break_at = max(space_idx, newline_idx)
                if break_at > pos:
                    end = break_at + 1
            yield {"type": "agent_update", "content": text[pos:end]}
            pos = end

    def _emit_source_trace_events(self, trace: Dict[str, Any]) -> Generator[Dict[str, Any], None, None]:
        yield trace
        source_meta = trace.get("source_meta") or {}
        mode = (source_meta.get("endpoint_label") or "").strip().lower()
        source = trace.get("source")
        if trace.get("type") == "source_call_start" and mode in {"live", "fallback", "blocked"} and source:
            yield {
                "type": "fallback_mode_changed",
                "stage": "source_mode",
                "source": source,
                "mode": mode,
                "message": f"{source} is running in {mode} mode",
                "status": "info" if mode == "live" else ("error" if mode == "blocked" else "running"),
            }

    def _apply_demo_scenario(self, query: str, demo_scenario: str) -> str:
        scenario_prompts = {
            "weather-spike": "Scenario context: severe weather spike around departure bank. Prioritize KQL hazard window and downstream impacts.",
            "runway-notam": "Scenario context: runway NOTAM closure impacts airport throughput. Prioritize NOTAM evidence and dependency graph.",
            "ground-bottleneck": "Scenario context: ground handling bottleneck at major hub. Prioritize turnaround telemetry, gate utilization, and mitigation recommendation.",
        }
        prefix = scenario_prompts.get((demo_scenario or "").strip().lower())
        if not prefix:
            return query
        return f"{prefix}\n\nUser query: {query}"

    def _scenario_operational_alert(self, demo_scenario: str) -> Optional[Dict[str, Any]]:
        now = datetime.now(timezone.utc).isoformat()
        scenario = (demo_scenario or "").strip().lower()
        alerts = {
            "weather-spike": {
                "severity": "warning",
                "title": "Weather Advisory",
                "message": "Convective weather spike detected near departure corridor. Re-check hazard windows at T-30 and prepare alternate flow sequencing.",
                "source": "KQL",
            },
            "runway-notam": {
                "severity": "critical",
                "title": "Runway NOTAM Critical",
                "message": "Runway constraint NOTAM is active and may reduce throughput. Validate dispatch sequence and slot exposure before release.",
                "source": "VECTOR_REG",
            },
            "ground-bottleneck": {
                "severity": "warning",
                "title": "Ground Handling Bottleneck",
                "message": "Ground handling saturation detected at hub stands. Prioritize turnaround recovery and downstream tail protection.",
                "source": "KQL",
            },
        }
        payload = alerts.get(scenario)
        if not payload:
            return None
        return {
            "type": "operational_alert",
            "stage": "ops_alert",
            "severity": payload["severity"],
            "title": payload["title"],
            "message": payload["message"],
            "source": payload["source"],
            "timestamp": now,
        }

    def _format_citations(self, citations: List[Citation]) -> List[Dict[str, Any]]:
        now = datetime.now(timezone.utc).isoformat()
        dataset_by_provider = {
            "SQL": "aviation_db",
            "KQL": "fabric-eventhouse",
            "GRAPH": "fabric-graph",
            "NOSQL": "nosql",
            "VECTOR_OPS": "idx_ops_narratives",
            "VECTOR_REG": "idx_regulatory",
            "VECTOR_AIRPORT": "idx_airport_ops_docs",
            "SEMANTIC": "aviation-index",
        }
        formatted: List[Dict[str, Any]] = []
        for idx, citation in enumerate(citations, start=1):
            provider = citation.source_type or "SEMANTIC"
            dataset = citation.dataset or dataset_by_provider.get(provider, "aviation-index")
            formatted.append(
                {
                    "id": idx,
                    "provider": provider,
                    "dataset": dataset,
                    "rowId": citation.identifier,
                    "timestamp": now,
                    "confidence": citation.score or 0.9,
                    "excerpt": citation.content_preview,
                    "authority": self.retriever.source_event_meta(provider).get("store_type"),
                    "freshness": self.retriever.source_event_meta(provider).get("freshness"),
                }
            )
        return formatted

    def run_once(
        self,
        query: str,
        session_id: Optional[str] = None,
        retrieval_mode: str = "code-rag",
        query_profile: str = "pilot-brief",
        required_sources: Optional[List[str]] = None,
        source_policy: str = "include",
        freshness_sla_minutes: Optional[int] = None,
        explain_retrieval: bool = False,
        risk_mode: str = "standard",
        ask_recommendation: bool = False,
        demo_scenario: Optional[str] = None,
        failure_policy: str = "graceful",
    ) -> Dict[str, Any]:
        answer_parts: List[str] = []
        citations: List[Dict[str, Any]] = []
        metadata: Dict[str, Any] = {}
        terminal_error: Dict[str, Any] = {}

        retrieval_plan: Dict[str, Any] = {}
        for event in self.run_stream(
            query,
            session_id=session_id,
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
        ):
            if event.get("type") == "agent_update" and event.get("content"):
                answer_parts.append(str(event["content"]))
            elif event.get("type") == "citations":
                citations = list(event.get("citations", []))
            elif event.get("type") == "retrieval_plan":
                retrieval_plan = dict(event.get("plan", {}))
            elif event.get("type") in {"agent_error", "error"}:
                terminal_error = {
                    "message": event.get("message") or event.get("error") or "Agent runtime error",
                    "errorCode": event.get("error_code") or event.get("errorCode") or "",
                    "terminalReason": event.get("terminal_reason") or "",
                    "route": event.get("route", "ERROR"),
                    "degradedSources": list(event.get("degradedSources", [])),
                    "failedRequiredSources": list(event.get("failedRequiredSources", [])),
                    "requiredSourcesSatisfied": bool(
                        event.get("requiredSourcesSatisfied", len(list(event.get("failedRequiredSources", []))) == 0)
                    ),
                    "missingRequiredSources": list(event.get("missingRequiredSources", [])),
                    "sourcePolicy": event.get("sourcePolicy", self._normalize_source_policy(source_policy)),
                    "partial": bool(event.get("partial", False)),
                    "required_sources_raw": list(event.get("required_sources_raw", [])),
                    "required_sources_normalized": list(event.get("required_sources_normalized", [])),
                    "invalid_required_sources": list(event.get("invalid_required_sources", [])),
                }
            elif event.get("type") in {"agent_done", "agent_partial_done"}:
                metadata = event

        if terminal_error:
            return {
                "answer": "".join(answer_parts).strip(),
                "citations": citations,
                "retrieval_plan": retrieval_plan,
                "route": terminal_error.get("route", "ERROR"),
                "reasoning": "terminal_agent_error",
                "framework": self._framework_label,
                "is_verified": False,
                "degraded_sources": list(terminal_error.get("degradedSources", [])),
                "failed_required_sources": list(terminal_error.get("failedRequiredSources", [])),
                "required_sources_satisfied": bool(terminal_error.get("requiredSourcesSatisfied", False)),
                "missing_required_sources": list(terminal_error.get("missingRequiredSources", [])),
                "source_policy": str(terminal_error.get("sourcePolicy", self._normalize_source_policy(source_policy))),
                "partial": bool(terminal_error.get("partial", False)),
                "status": "error",
                "error": terminal_error.get("message", "Agent runtime error"),
                "error_code": terminal_error.get("errorCode", ""),
                "terminal_reason": terminal_error.get("terminalReason", ""),
                "required_sources_raw": list(terminal_error.get("required_sources_raw", [])),
                "required_sources_normalized": list(terminal_error.get("required_sources_normalized", [])),
                "invalid_required_sources": list(terminal_error.get("invalid_required_sources", [])),
            }

        return {
            "answer": "".join(answer_parts).strip(),
            "citations": citations,
            "retrieval_plan": retrieval_plan,
            "route": metadata.get("route", "HYBRID"),
            "reasoning": metadata.get("reasoning", ""),
            "framework": metadata.get("framework", self._framework_label),
            "is_verified": bool(metadata.get("isVerified")),
            "degraded_sources": list(metadata.get("degradedSources", [])),
            "failed_required_sources": list(metadata.get("failedRequiredSources", [])),
            "required_sources_satisfied": bool(
                metadata.get("requiredSourcesSatisfied", len(list(metadata.get("failedRequiredSources", []))) == 0)
            ),
            "missing_required_sources": list(
                metadata.get("missingRequiredSources", list(metadata.get("failedRequiredSources", [])))
            ),
            "source_policy": str(metadata.get("sourcePolicy", self._normalize_source_policy(source_policy))),
            "partial": bool(metadata.get("partial", False)),
        }
