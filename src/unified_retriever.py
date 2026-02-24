#!/usr/bin/env python3
"""
Unified Retriever - Multi-source retrieval combining SQL and Semantic search.
Routes queries to appropriate sources and returns answers with citations.
"""

import json
import logging
import os
import re
import base64
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Generator, List, Dict, Optional, Tuple
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor
from azure.core.credentials import AzureKeyCredential
from azure.search.documents import SearchClient
from azure.search.documents.models import VectorizedQuery
from opentelemetry import trace as _otel_trace

_ur_tracer = _otel_trace.get_tracer("aviation-rag-backend", "0.1.0")

try:
    from azure.cosmos import CosmosClient, exceptions as cosmos_exceptions
    _COSMOS_SDK_AVAILABLE = True
except ImportError:
    _COSMOS_SDK_AVAILABLE = False

from azure_openai_client import get_shared_client
from query_router import QueryRouter
from query_writers import SQLWriter
from sql_generator import SQLGenerator
from pii_filter import PiiFilter, PiiCheckResult
from shared_utils import (
    OPENAI_API_VERSION,
    ENGLISH_4LETTER_BLOCKLIST as _ENGLISH_4LETTER_BLOCKLIST,
    CITY_AIRPORT_MAP,
    IATA_TO_ICAO_MAP,
    env_bool as _env_bool,
    env_csv as _env_csv,
)

logger = logging.getLogger(__name__)

# Database configuration — PostgreSQL only.
# When PG is unavailable, SQL is simply marked as unavailable (no SQLite fallback).
SEARCH_SEMANTIC_CONFIG = os.getenv("AZURE_SEARCH_SEMANTIC_CONFIG_NAME", "aviation-semantic-config")
try:
    _RERANK_RAW_CANDIDATES = max(1, int(os.getenv("CONTEXT_VECTOR_RAW_CANDIDATES", "20") or "20"))
except Exception:
    _RERANK_RAW_CANDIDATES = 20
_RERANK_ENABLED = os.getenv("CONTEXT_RERANK_ENABLED", "true").strip().lower() in {"1", "true", "yes", "y", "on"}

_SEMANTIC_MIN_SCORE = float(os.getenv("SEMANTIC_MIN_SCORE_THRESHOLD", "0.0"))

FABRIC_KQL_ENDPOINT = os.getenv("FABRIC_KQL_ENDPOINT")
FABRIC_GRAPH_ENDPOINT = os.getenv("FABRIC_GRAPH_ENDPOINT")
FABRIC_NOSQL_ENDPOINT = os.getenv("FABRIC_NOSQL_ENDPOINT")
FABRIC_SQL_ENDPOINT = os.getenv("FABRIC_SQL_ENDPOINT")
FABRIC_KUSTO_CLUSTER_URL = os.getenv("FABRIC_KUSTO_CLUSTER_URL", "").strip()
FABRIC_KQL_DATABASE = os.getenv("FABRIC_KQL_DATABASE", "").strip()
FABRIC_GRAPH_DATABASE = os.getenv("FABRIC_GRAPH_DATABASE", "").strip()
FABRIC_SQL_DATABASE = os.getenv("FABRIC_SQL_DATABASE", "").strip()

GUARDRAIL_ACCOUNT_UPN = "admin@MngEnvMCAP705508.onmicrosoft.com"
GUARDRAIL_TENANT_ID = "52095a81-130f-4b06-83f1-9859b2c73de6"
GUARDRAIL_SUBSCRIPTION_ID = "6a539906-6ce2-4e3b-84ee-89f701de18d8"

_RUNTIME_ACCOUNT_ENV_KEYS = ("AZURE_ACCOUNT_UPN", "AZURE_CLIENT_UPN")
_RUNTIME_TENANT_ENV_KEYS = ("AZURE_TENANT_ID", "FABRIC_TENANT_ID", "AZURE_OPENAI_TENANT_ID")
_RUNTIME_SUBSCRIPTION_ENV_KEYS = ("AZURE_SUBSCRIPTION_ID", "SUBSCRIPTION_ID")

AZURE_COSMOS_ENDPOINT = os.getenv("AZURE_COSMOS_ENDPOINT", "").strip()
AZURE_COSMOS_KEY = os.getenv("AZURE_COSMOS_KEY", "").strip()
AZURE_COSMOS_DATABASE = os.getenv("AZURE_COSMOS_DATABASE", "aviationrag").strip()
AZURE_COSMOS_CONTAINER = os.getenv("AZURE_COSMOS_CONTAINER", "notams").strip()

_SQL_RESERVED_WORDS = {
    "as", "and", "or", "on", "where", "group", "order", "by", "limit", "offset",
    "join", "left", "right", "inner", "outer", "full", "cross", "having",
    "union", "all", "distinct", "asc", "desc", "case", "when", "then", "else",
    "end", "from", "select", "with", "into", "over", "partition",
}

_KQL_RESERVED_WORDS = {
    "where", "project", "extend", "summarize", "by", "sort", "order", "top",
    "take", "distinct", "count", "as", "and", "or", "not", "in", "in~",
    "has", "has_any", "contains", "between", "asc", "desc", "true", "false",
    "let", "mv-expand", "join", "on", "kind", "limit",
}

_KQL_ALLOWED_FUNCTIONS = {
    "ago", "now", "todatetime", "tolower", "toupper", "coalesce", "isempty",
    "isnotempty", "strlen", "trim", "extract", "format_datetime", "bin", "iff",
    "datetime", "datetime_utc_to_local",
}


def _env_int(name: str, default: int, minimum: int = 0) -> int:
    raw = os.getenv(name)
    try:
        value = int(str(raw).strip()) if raw is not None else default
    except Exception:
        value = default
    return max(minimum, value)


def _env_float(name: str, default: float, minimum: float = 0.0) -> float:
    raw = os.getenv(name)
    try:
        value = float(str(raw).strip()) if raw is not None else default
    except Exception:
        value = default
    return max(minimum, value)


def _get_fabric_bearer_token() -> str:
    """Re-read bearer token from env on each call so rotated tokens take effect."""
    return os.getenv("FABRIC_BEARER_TOKEN", "")


_fabric_token_lock = threading.Lock()
_fabric_token_cache: Dict[str, Dict[str, Any]] = {}  # scope -> {token, expires_at}

_FABRIC_DEFAULT_SCOPE = "https://api.fabric.microsoft.com/.default"


def _fabric_token_min_ttl_seconds() -> int:
    return _env_int("FABRIC_TOKEN_MIN_TTL_SECONDS", 120, minimum=0)


def _source_call_timeout_seconds() -> float:
    return _env_float("SOURCE_CALL_TIMEOUT_SECONDS", 20.0, minimum=1.0)


def _allow_static_fabric_bearer() -> bool:
    return os.getenv("ALLOW_STATIC_FABRIC_BEARER", "false").strip().lower() in {
        "1", "true", "yes", "y", "on"
    }


def _decode_jwt_payload(token: str) -> Dict[str, Any]:
    raw = str(token or "").strip()
    if not raw:
        return {}
    parts = raw.split(".")
    if len(parts) < 2:
        return {}
    try:
        payload = parts[1]
        payload += "=" * (-len(payload) % 4)
        decoded = base64.urlsafe_b64decode(payload.encode("utf-8")).decode("utf-8", errors="ignore")
        data = json.loads(decoded)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _token_ttl_seconds(token: str) -> Optional[int]:
    payload = _decode_jwt_payload(token)
    exp = payload.get("exp")
    try:
        exp_ts = int(exp)
    except Exception:
        return None
    return exp_ts - int(time.time())


def _cluster_scope_for_endpoint(endpoint: str) -> str:
    raw = (endpoint or "").strip()
    if not raw:
        return _FABRIC_DEFAULT_SCOPE
    parsed = urllib.parse.urlparse(raw)
    if not parsed.scheme or not parsed.hostname:
        return _FABRIC_DEFAULT_SCOPE
    host = str(parsed.hostname or "").lower()
    if "kusto.fabric.microsoft.com" not in host:
        return _FABRIC_DEFAULT_SCOPE
    return f"{parsed.scheme}://{parsed.hostname}/.default"


def _acquire_fabric_token_bundle(scope: str = _FABRIC_DEFAULT_SCOPE) -> Dict[str, Any]:
    """Acquire Fabric token with auth metadata and freshness validation."""
    min_ttl = _fabric_token_min_ttl_seconds()
    allow_static = _allow_static_fabric_bearer()

    # 1) SP client credentials (refreshable) — preferred.
    client_id = os.getenv("FABRIC_CLIENT_ID", "").strip()
    client_secret = os.getenv("FABRIC_CLIENT_SECRET", "").strip()
    tenant_id = os.getenv("FABRIC_TENANT_ID", "").strip()
    if client_id and client_secret and tenant_id:
        with _fabric_token_lock:
            entry = _fabric_token_cache.get(scope, {})
            cached = entry.get("token")
            expires = entry.get("expires_at", 0)
            if cached and time.time() < expires - max(30, min_ttl):
                ttl = int(expires - time.time())
                return {
                    "token": cached,
                    "auth_mode": "sp_client_credentials",
                    "reason": "cached",
                    "token_ttl_seconds": ttl,
                    "auth_ready": True,
                }

            token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
            body = urllib.parse.urlencode({
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_secret": client_secret,
                "scope": scope,
            }).encode()
            req = urllib.request.Request(token_url, data=body, method="POST")
            req.add_header("Content-Type", "application/x-www-form-urlencoded")
            try:
                with urllib.request.urlopen(req, timeout=10) as resp:
                    data = json.loads(resp.read())
                access_token = data["access_token"]
                expires_in = int(data.get("expires_in", 3600))
                ttl = _token_ttl_seconds(access_token)
                if ttl is not None and ttl < min_ttl:
                    return {
                        "token": "",
                        "auth_mode": "sp_client_credentials",
                        "reason": "sp_token_near_expiry",
                        "token_ttl_seconds": ttl,
                        "auth_ready": False,
                    }
                _fabric_token_cache[scope] = {
                    "token": access_token,
                    "expires_at": time.time() + expires_in,
                }
                logger.info("Acquired Fabric bearer token scope=%s (expires_in=%ds)", scope, expires_in)
                return {
                    "token": access_token,
                    "auth_mode": "sp_client_credentials",
                    "reason": "refreshed",
                    "token_ttl_seconds": int(ttl if ttl is not None else expires_in),
                    "auth_ready": True,
                }
            except Exception as exc:
                logger.warning("MSAL token acquisition failed (scope=%s): %s", scope, exc)
                if cached:
                    ttl = _token_ttl_seconds(cached)
                    return {
                        "token": cached,
                        "auth_mode": "sp_client_credentials",
                        "reason": "cached_after_refresh_failure",
                        "token_ttl_seconds": int(ttl if ttl is not None else max(0, int(expires - time.time()))),
                        "auth_ready": (ttl is None) or (ttl >= min_ttl),
                    }
                return {
                    "token": "",
                    "auth_mode": "sp_client_credentials",
                    "reason": f"sp_token_acquisition_failed:{exc}",
                    "token_ttl_seconds": None,
                    "auth_ready": False,
                }

    # 2) Optional static bearer fallback (explicit opt-in only).
    static = os.getenv("FABRIC_BEARER_TOKEN", "").strip()
    if static and not allow_static:
        return {
            "token": "",
            "auth_mode": "none",
            "reason": "static_bearer_disabled",
            "token_ttl_seconds": _token_ttl_seconds(static),
            "auth_ready": False,
        }
    if static and allow_static:
        ttl = _token_ttl_seconds(static)
        if ttl is not None and ttl < min_ttl:
            reason = "static_bearer_expired" if ttl <= 0 else "static_bearer_near_expiry"
            return {
                "token": "",
                "auth_mode": "static_bearer",
                "reason": reason,
                "token_ttl_seconds": ttl,
                "auth_ready": False,
            }
        return {
            "token": static,
            "auth_mode": "static_bearer",
            "reason": "static_bearer_allowed",
            "token_ttl_seconds": ttl,
            "auth_ready": True,
        }

    return {
        "token": "",
        "auth_mode": "none",
        "reason": "fabric_auth_not_configured",
        "token_ttl_seconds": None,
        "auth_ready": False,
    }


def _acquire_fabric_token(scope: str = _FABRIC_DEFAULT_SCOPE) -> str:
    bundle = _acquire_fabric_token_bundle(scope)
    return str(bundle.get("token") or "")


@dataclass
class Citation:
    """Citation for a source used in the answer."""
    source_type: str  # SQL, SEMANTIC
    identifier: str   # e.g., record_id, doc_id
    title: str        # Human-readable title
    content_preview: str = ""  # First ~100 chars of content
    score: float = 0.0  # Relevance score if applicable
    dataset: str = ""

    def __str__(self):
        prefix_map = {"SQL": "SQL", "SEMANTIC": "SEM"}
        prefix = prefix_map.get(self.source_type, self.source_type[:3])
        return f"[{prefix}] {self.title}"

    def to_dict(self):
        return {
            "source_type": self.source_type,
            "identifier": self.identifier,
            "title": self.title,
            "content_preview": self.content_preview,
            "score": self.score,
            "dataset": self.dataset,
        }


@dataclass
class RetrievalResult:
    """Result from unified retrieval."""
    answer: str
    route: str
    reasoning: str
    citations: List[Citation] = field(default_factory=list)
    sql_results: Optional[List[Dict]] = None
    semantic_results: Optional[List[Dict]] = None
    sql_query: Optional[str] = None
    pii_blocked: bool = False
    pii_warning: Optional[str] = None

    def to_dict(self):
        return {
            "answer": self.answer,
            "route": self.route,
            "reasoning": self.reasoning,
            "citations": [c.to_dict() for c in self.citations],
            "sql_query": self.sql_query,
            "pii_blocked": self.pii_blocked,
            "pii_warning": self.pii_warning
        }


class UnifiedRetriever:
    """
    Unified retrieval interface combining:
    - SQL database (aviation data)
    - Semantic search (aviation-index)

    All queries are filtered through PII detection before processing.
    """

    def __init__(self, enable_pii_filter: bool = True):
        # LLM client
        self.llm, self.llm_auth_mode = get_shared_client(api_version=OPENAI_API_VERSION)
        self.llm_deployment = os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME", "aviation-chat-gpt5-mini")
        self.embedding_deployment = os.getenv(
            "AZURE_TEXT_EMBEDDING_DEPLOYMENT_NAME", "text-embedding-3-small"
        )

        # Search clients (multi-index)
        search_endpoint = os.getenv("AZURE_SEARCH_ENDPOINT")
        search_key = os.getenv("AZURE_SEARCH_ADMIN_KEY")
        search_index_ops = os.getenv("AZURE_SEARCH_INDEX_OPS_NAME", "idx_ops_narratives")
        search_index_regulatory = os.getenv("AZURE_SEARCH_INDEX_REGULATORY_NAME", "idx_regulatory")
        search_index_airport = os.getenv("AZURE_SEARCH_INDEX_AIRPORT_NAME", "idx_airport_ops_docs")
        self.search_clients: Dict[str, SearchClient] = {}
        self.vector_source_to_index = {
            "VECTOR_OPS": search_index_ops,
            "VECTOR_REG": search_index_regulatory,
            "VECTOR_AIRPORT": search_index_airport,
        }
        if search_endpoint and search_key:
            search_credential = AzureKeyCredential(search_key)
            for index_name in sorted(set(self.vector_source_to_index.values())):
                self.search_clients[index_name] = SearchClient(
                    endpoint=search_endpoint,
                    index_name=index_name,
                    credential=search_credential,
                )
        else:
            logger.warning("Azure AI Search is not configured; semantic retrieval will be unavailable.")

        # Database connection — PostgreSQL only.
        self._pg_pool = None  # PostgreSQL connection pool (thread-safe)
        self._pg_pool_lock = threading.Lock()
        self.sql_backend = "unavailable"
        self.sql_available = False
        self.sql_unavailable_reason = ""
        self.sql_visible_schemas = _env_csv("SQL_VISIBLE_SCHEMAS", "public,demo")
        try:
            import psycopg2
            import psycopg2.pool
            connect_kwargs: Dict[str, Any] = {
                "host": os.getenv("PGHOST"),
                "port": int(os.getenv("PGPORT", 5432)),
                "database": os.getenv("PGDATABASE", "aviationrag"),
                "user": os.getenv("PGUSER"),
                "password": os.getenv("PGPASSWORD"),
                "sslmode": "require",
                "connect_timeout": 5,
                "keepalives": 1,
                "keepalives_idle": 30,
                "keepalives_interval": 10,
                "keepalives_count": 5,
            }
            if self.sql_visible_schemas:
                schemas_param = ",".join(self.sql_visible_schemas)
                connect_kwargs["options"] = f"-c search_path={schemas_param}"

            self._pg_pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=1,
                maxconn=5,
                **connect_kwargs,
            )
            # Verify pool is usable with a test connection.
            test_conn = self._pg_pool.getconn()
            try:
                test_conn.autocommit = True
                if self.sql_visible_schemas:
                    with test_conn.cursor() as cur:
                        cur.execute(
                            "SET search_path TO " + ", ".join(
                                "%s" for _ in self.sql_visible_schemas
                            ),
                            self.sql_visible_schemas,
                        )
            finally:
                self._pg_pool.putconn(test_conn)
            self.sql_backend = "postgres"
            self.sql_available = True
            logger.info(
                "Connected to PostgreSQL pool: %s/%s (search_path=%s)",
                os.getenv("PGHOST"), os.getenv("PGDATABASE", "aviationrag"),
                ",".join(self.sql_visible_schemas) or "default",
            )
        except Exception as exc:
            self.sql_unavailable_reason = str(exc)
            self._pg_pool = None
            self.sql_backend = "unavailable"
            self.sql_available = False
            logger.warning("PostgreSQL unavailable (%s)", exc)

        self.sql_dialect = "postgres"

        # Specialized components
        self.router = QueryRouter()
        self.sql_generator = SQLGenerator()
        self.sql_writer = SQLWriter(
            model=os.getenv("AZURE_OPENAI_WORKER_DEPLOYMENT_NAME") or self.llm_deployment
        )
        self.use_legacy_sql_generator = _env_bool("USE_LEGACY_SQL_GENERATOR", False)
        self._vector_k_param = self._detect_vector_k_param()

        # Schema cache (avoids repeated DB introspection within TTL)
        self._schema_cache: Optional[Dict[str, Any]] = None
        self._schema_cache_expires_at: float = 0.0
        self._schema_cache_ttl: float = float(os.getenv("SQL_SCHEMA_CACHE_TTL_SECONDS", "300"))

        # Embedding cache (LRU)
        self._embedding_cache_size = int(os.getenv("EMBEDDING_CACHE_SIZE", "256"))
        self._embedding_cache: Dict[str, List[float]] = {}
        self._embedding_cache_order: List[str] = []
        self._embedding_cache_lock = threading.Lock()

        # PII filter
        self.enable_pii_filter = enable_pii_filter
        if enable_pii_filter:
            self.pii_filter = PiiFilter()
            if self.pii_filter.is_available():
                logger.info("PII filter enabled and available")
            else:
                logger.warning("PII filter enabled but service unavailable")
        else:
            self.pii_filter = None
            logger.warning("PII filter disabled")

        # Cosmos DB client for NOSQL (NOTAM) retrieval
        # Prefer DefaultAzureCredential (AAD/managed identity) because the
        # Cosmos DB account may have local key auth disabled.  Fall back to
        # key-based auth only if AAD fails.
        self._cosmos_container = None
        if _COSMOS_SDK_AVAILABLE and AZURE_COSMOS_ENDPOINT:
            cosmos_client = None
            # 1. Try AAD / managed-identity first
            try:
                from azure.identity import DefaultAzureCredential
                _cred = DefaultAzureCredential()
                cosmos_client = CosmosClient(AZURE_COSMOS_ENDPOINT, credential=_cred)
                # Validate the credential by listing databases (lightweight)
                cosmos_client.get_database_client(AZURE_COSMOS_DATABASE).read()
                logger.info("Cosmos DB connected via DefaultAzureCredential")
            except Exception as aad_exc:
                cosmos_client = None
                logger.info("Cosmos DB AAD auth failed (%s), trying key-based auth", aad_exc)
            # 2. Fall back to key-based auth
            if cosmos_client is None and AZURE_COSMOS_KEY:
                try:
                    cosmos_client = CosmosClient(AZURE_COSMOS_ENDPOINT, credential=AZURE_COSMOS_KEY)
                    logger.info("Cosmos DB connected via account key")
                except Exception as key_exc:
                    cosmos_client = None
                    logger.warning("Cosmos DB key-based auth also failed (%s)", key_exc)
            if cosmos_client is not None:
                try:
                    cosmos_db = cosmos_client.get_database_client(AZURE_COSMOS_DATABASE)
                    self._cosmos_container = cosmos_db.get_container_client(AZURE_COSMOS_CONTAINER)
                    logger.info(
                        "Connected to Cosmos DB: %s/%s/%s",
                        AZURE_COSMOS_ENDPOINT, AZURE_COSMOS_DATABASE, AZURE_COSMOS_CONTAINER,
                    )
                except Exception as exc:
                    self._cosmos_container = None
                    logger.warning("Cosmos DB unavailable (%s)", exc)
            else:
                logger.warning("Cosmos DB: no valid credential available")
        elif not _COSMOS_SDK_AVAILABLE and AZURE_COSMOS_ENDPOINT:
            logger.warning("azure-cosmos SDK not installed; Cosmos NOSQL retrieval unavailable")

        # Graph retrieval resilience controls.
        self._graph_timeout_seconds = _env_float("GRAPH_TIMEOUT_SECONDS", 12.0, minimum=1.0)
        self._graph_max_retries = _env_int("GRAPH_MAX_RETRIES", 2, minimum=0)
        self._graph_retry_backoff_seconds = _env_float("GRAPH_RETRY_BACKOFF_SECONDS", 0.75, minimum=0.0)
        self._graph_cb_fail_threshold = _env_int("GRAPH_CIRCUIT_BREAKER_FAIL_THRESHOLD", 4, minimum=1)
        self._graph_cb_open_seconds = _env_float("GRAPH_CIRCUIT_BREAKER_OPEN_SECONDS", 45.0, minimum=1.0)
        self._graph_circuit_lock = threading.Lock()
        self._graph_circuit_failures = 0
        self._graph_circuit_open_until = 0.0

        # Fabric SQL capability cache (pyodbc + driver readiness for TDS path).
        self._fabric_sql_tds_capability_cache: Optional[Dict[str, Any]] = None
        self._source_capabilities: Dict[str, Dict[str, Any]] = {}
        self._identity_guardrail_report: Dict[str, Any] = {}
        self._refresh_source_capabilities(refresh_tds=True)

    @staticmethod
    def _filter_error_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Remove error/error_code rows from result lists before synthesis."""
        return [row for row in rows if isinstance(row, dict) and not row.get("error") and not row.get("error_code")]

    def _format_schema_for_legacy_generator(self, schema: Dict[str, Any]) -> str:
        """Format live SQL schema for injection into legacy SQLGenerator context."""
        lines: List[str] = []
        for table_entry in schema.get("tables", []):
            if not isinstance(table_entry, dict):
                continue
            table_name = table_entry.get("table", "")
            schema_name = table_entry.get("schema", "")
            columns = [str(col.get("name", "")) for col in (table_entry.get("columns") or []) if isinstance(col, dict)]
            qualified = f"{schema_name}.{table_name}" if schema_name else table_name
            lines.append(f"{qualified}: {', '.join(columns)}")
        return "\n".join(lines)

    def _detect_vector_k_param(self) -> str:
        """Handle azure-search-documents SDK drift (k vs k_nearest_neighbors)."""
        try:
            import inspect

            params = inspect.signature(VectorizedQuery.__init__).parameters
            if "k" in params:
                return "k"
            if "k_nearest_neighbors" in params:
                return "k_nearest_neighbors"
        except Exception:
            pass
        return "k_nearest_neighbors"

    def get_embedding(self, text: str) -> List[float]:
        """Get embedding from Azure OpenAI with LRU cache."""
        if not isinstance(text, str):
            raise TypeError(f"embedding_input_must_be_string:{type(text).__name__}")
        normalized = text.strip()[:8000]
        with self._embedding_cache_lock:
            if normalized in self._embedding_cache:
                self._embedding_cache_order.remove(normalized)
                self._embedding_cache_order.append(normalized)
                logger.info("perf stage=%s cache=hit", "get_embedding")
                return self._embedding_cache[normalized]

        _t0 = time.perf_counter()
        response = self.llm.embeddings.create(
            model=self.embedding_deployment,
            input=normalized,
        )
        result = response.data[0].embedding
        elapsed = (time.perf_counter() - _t0) * 1000

        with self._embedding_cache_lock:
            self._embedding_cache[normalized] = result
            self._embedding_cache_order.append(normalized)
            while len(self._embedding_cache_order) > self._embedding_cache_size:
                evicted = self._embedding_cache_order.pop(0)
                self._embedding_cache.pop(evicted, None)

        logger.info("perf stage=%s cache=miss ms=%.1f", "get_embedding", elapsed)
        return result

    def get_embedding_safe(self, text: str) -> Tuple[Optional[List[float]], Optional[str]]:
        """Best-effort embedding lookup that never raises."""
        try:
            return self.get_embedding(text), None
        except Exception as exc:
            logger.warning("Embedding lookup failed; continuing without shared embedding: %s", exc)
            return None, str(exc)

    def _now_iso(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def _post_json(
        self,
        endpoint: str,
        payload: Any,
        token_scope: str = None,
        timeout_seconds: float = 30.0,
    ) -> Any:
        body = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(endpoint, data=body, method="POST")
        req.add_header("Content-Type", "application/json")
        token = _acquire_fabric_token(token_scope) if token_scope else _acquire_fabric_token()
        if token:
            req.add_header("Authorization", f"Bearer {token}")
        try:
            with urllib.request.urlopen(req, timeout=max(1.0, float(timeout_seconds))) as resp:
                raw = resp.read().decode("utf-8", errors="ignore")
                if not raw:
                    return {}
                return json.loads(raw)
        except urllib.error.HTTPError as exc:
            return {"error": f"http_{exc.code}", "detail": exc.read().decode("utf-8", errors="ignore")}
        except Exception as exc:
            return {"error": str(exc)}

    def _is_kusto_endpoint(self, endpoint: str) -> bool:
        endpoint_l = (endpoint or "").lower()
        return "kusto.fabric.microsoft.com" in endpoint_l

    def _effective_fabric_endpoint(self, source: str) -> str:
        source_norm = (source or "").upper()
        if source_norm == "KQL":
            return (FABRIC_KQL_ENDPOINT or FABRIC_KUSTO_CLUSTER_URL or "").strip()
        if source_norm == "GRAPH":
            return (FABRIC_GRAPH_ENDPOINT or FABRIC_KUSTO_CLUSTER_URL or "").strip()
        if source_norm == "NOSQL":
            return (FABRIC_NOSQL_ENDPOINT or FABRIC_KUSTO_CLUSTER_URL or "").strip()
        if source_norm == "FABRIC_SQL":
            return (FABRIC_SQL_ENDPOINT or "").strip()
        return ""

    def _fabric_auth_bundle_for_endpoint(self, endpoint: str, force_scope: Optional[str] = None) -> Dict[str, Any]:
        scope = force_scope or _cluster_scope_for_endpoint(endpoint)
        bundle = _acquire_fabric_token_bundle(scope=scope)
        return {
            **bundle,
            "scope": scope,
        }

    def _fabric_auth_reason_for_source(self, source: str) -> Tuple[bool, str, Dict[str, Any]]:
        endpoint = self._effective_fabric_endpoint(source)
        auth_bundle = self._fabric_auth_bundle_for_endpoint(endpoint)
        auth_ready = bool(auth_bundle.get("auth_ready") and auth_bundle.get("token"))
        reason = str(auth_bundle.get("reason") or "fabric_auth_not_ready")
        if auth_ready:
            return True, "ready", auth_bundle
        return False, reason, auth_bundle

    def _query_tokens(self, query: str) -> List[str]:
        if isinstance(query, str):
            text = query
        elif isinstance(query, (dict, list)):
            try:
                text = json.dumps(query, ensure_ascii=False)
            except Exception:
                text = str(query)
        else:
            text = str(query or "")

        tokens = [t.upper() for t in re.findall(r"[A-Za-z0-9]{3,8}", text)]
        deduped: List[str] = []
        for token in tokens:
            if token not in deduped:
                deduped.append(token)
            if len(deduped) >= 8:
                break
        return deduped

    def _extract_airports_from_query(self, query: str) -> List[str]:
        if isinstance(query, str):
            text = query
        elif isinstance(query, (dict, list)):
            try:
                text = json.dumps(query, ensure_ascii=False)
            except Exception:
                text = str(query)
        else:
            text = str(query or "")
        upper = text.upper()
        lower = text.lower()
        out: List[str] = []

        # ICAO codes in free text (case-sensitive to avoid matching regular words).
        for match in re.findall(r"\b[A-Z]{4}\b", text):
            if match not in out and match not in _ENGLISH_4LETTER_BLOCKLIST:
                out.append(match)

        # Common IATA references used by users in natural language.
        for match in re.findall(r"\b[A-Z]{3}\b", upper):
            icao = IATA_TO_ICAO_MAP.get(match)
            if icao and icao not in out:
                out.append(icao)

        # City-level shortcuts for common demo routes.
        for city, airports in CITY_AIRPORT_MAP.items():
            if city in lower:
                for airport in airports:
                    if airport not in out:
                        out.append(airport)

        return out[:8]

    def _extract_airports_from_sql(self, sql_query: str) -> List[str]:
        if not sql_query:
            return []
        return self._extract_airports_from_query(sql_query)

    def _extract_explicit_flight_identifiers(self, query: str) -> List[str]:
        if isinstance(query, str):
            text = query
        elif isinstance(query, (dict, list)):
            try:
                text = json.dumps(query, ensure_ascii=False)
            except Exception:
                text = str(query)
        else:
            text = str(query or "")

        out: List[str] = []
        upper = text.upper()

        # Airline flight number style identifiers (e.g., TK123, THY6047).
        for match in re.findall(r"\b[A-Z]{2,3}\s?\d{1,4}[A-Z]?\b", upper):
            normalized = re.sub(r"\s+", "", match)
            if normalized not in out:
                out.append(normalized)

        # Raw 24-bit ICAO transponder hex (icao24) identifiers.
        for match in re.findall(r"\b[0-9A-F]{6}\b", upper):
            if match not in out:
                out.append(match)

        return out[:12]

    def _resolve_kusto_query_endpoint(self, endpoint: str) -> Tuple[str, bool, str]:
        """Normalize Kusto endpoint to a valid query URI.

        Supported inputs:
        - Cluster root: https://<cluster>.kusto.fabric.microsoft.com
        - Explicit query path: .../v1/rest/query or .../v2/rest/query
        """
        raw = (endpoint or "").strip()
        if not raw:
            return "", False, "kusto_endpoint_missing"

        parsed = urllib.parse.urlparse(raw)
        if not parsed.scheme or not parsed.netloc:
            return raw, False, "kusto_endpoint_invalid_url"

        clean = parsed._replace(params="", query="", fragment="")
        path = (clean.path or "").rstrip("/")
        lower_path = path.lower()
        rest_query_count = lower_path.count("/rest/query")

        if rest_query_count > 1:
            return urllib.parse.urlunparse(clean._replace(path=path)), False, "kusto_endpoint_path_duplicate_query_segments"

        if lower_path.endswith("/v1/rest/query") or lower_path.endswith("/v2/rest/query"):
            return urllib.parse.urlunparse(clean._replace(path=path)), True, "kusto_query_path_explicit"

        if "/rest/query" in lower_path:
            return urllib.parse.urlunparse(clean._replace(path=path)), False, "kusto_endpoint_path_invalid"

        base = urllib.parse.urlunparse(clean._replace(path=path)).rstrip("/")
        return f"{base}/v1/rest/query", True, "kusto_query_path_derived_v1"

    def _kusto_rows(
        self,
        endpoint: str,
        csl: str,
        database: Optional[str] = None,
        timeout_seconds: Optional[float] = None,
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        db_name = (database or FABRIC_KQL_DATABASE or os.getenv("FABRIC_KQL_DATABASE_NAME", "")).strip()
        if not db_name:
            return [], "missing_fabric_kql_database"

        kql_endpoint, path_valid, path_reason = self._resolve_kusto_query_endpoint(endpoint)
        if not path_valid:
            return [], f"invalid_kusto_endpoint_path:{path_reason}"

        # Derive cluster-specific token scope from endpoint URL.
        parsed = urllib.parse.urlparse(kql_endpoint)
        kusto_scope = f"{parsed.scheme}://{parsed.hostname}/.default"

        response = self._post_json(
            kql_endpoint,
            {"db": db_name, "csl": csl},
            token_scope=kusto_scope,
            timeout_seconds=timeout_seconds if timeout_seconds is not None else 30.0,
        )
        if isinstance(response, dict) and response.get("error"):
            return [], str(response.get("error"))

        # v1 response: {"Tables": [{"TableName": ..., "Columns": [...], "Rows": [[...]]}]}
        if isinstance(response, dict) and "Tables" in response:
            for table in response["Tables"]:
                if not isinstance(table, dict):
                    continue
                columns = [str(c.get("ColumnName", "")) for c in (table.get("Columns") or [])]
                rows: List[Dict[str, Any]] = []
                for row in table.get("Rows") or []:
                    if isinstance(row, list):
                        rows.append(dict(zip(columns, row)))
                if rows:
                    return rows, None
            return [], "kusto_tables_empty"

        # v2 streaming format (legacy fallback)
        if isinstance(response, list):
            for frame in response:
                if not isinstance(frame, dict):
                    continue
                if frame.get("FrameType") != "DataTable":
                    continue
                if frame.get("TableKind") != "PrimaryResult":
                    continue
                columns = [str(c.get("ColumnName", "")) for c in (frame.get("Columns") or [])]
                rows = []
                for row in frame.get("Rows") or []:
                    if isinstance(row, list):
                        rows.append(dict(zip(columns, row)))
                return rows, None
            return [], "kusto_primary_result_not_found"

        return [], "unexpected_kusto_response_type"

    def _graph_database(self) -> str:
        return (FABRIC_GRAPH_DATABASE or FABRIC_KQL_DATABASE or os.getenv("FABRIC_KQL_DATABASE_NAME", "")).strip()

    def _ensure_graph_runtime_state(self) -> None:
        """Lazily initialize graph runtime controls for partially constructed test objects."""
        if not hasattr(self, "_graph_timeout_seconds"):
            self._graph_timeout_seconds = _env_float("GRAPH_TIMEOUT_SECONDS", 12.0, minimum=1.0)
        if not hasattr(self, "_graph_max_retries"):
            self._graph_max_retries = _env_int("GRAPH_MAX_RETRIES", 2, minimum=0)
        if not hasattr(self, "_graph_retry_backoff_seconds"):
            self._graph_retry_backoff_seconds = _env_float("GRAPH_RETRY_BACKOFF_SECONDS", 0.75, minimum=0.0)
        if not hasattr(self, "_graph_cb_fail_threshold"):
            self._graph_cb_fail_threshold = _env_int("GRAPH_CIRCUIT_BREAKER_FAIL_THRESHOLD", 4, minimum=1)
        if not hasattr(self, "_graph_cb_open_seconds"):
            self._graph_cb_open_seconds = _env_float("GRAPH_CIRCUIT_BREAKER_OPEN_SECONDS", 45.0, minimum=1.0)
        if not hasattr(self, "_graph_circuit_lock") or self._graph_circuit_lock is None:
            self._graph_circuit_lock = threading.Lock()
        if not hasattr(self, "_graph_circuit_failures"):
            self._graph_circuit_failures = 0
        if not hasattr(self, "_graph_circuit_open_until"):
            self._graph_circuit_open_until = 0.0

    def _graph_retryable_error(self, detail: Optional[str]) -> bool:
        text = str(detail or "").strip().lower()
        if not text:
            return False
        if text in {"missing_fabric_kql_database", "missing_fabric_graph_database"}:
            return False
        if text.startswith("http_"):
            try:
                code = int(text.split("_", 1)[1])
            except Exception:
                return False
            return code == 429 or code >= 500
        if any(token in text for token in ("timeout", "timed out", "temporar", "refused", "reset", "unreach", "connection", "service unavailable", "gateway")):
            return True
        return text in {"unexpected_kusto_response_type", "kusto_primary_result_not_found"}

    def _graph_retry_sleep(self, attempt_number: int) -> None:
        if self._graph_retry_backoff_seconds <= 0:
            return
        sleep_seconds = min(8.0, self._graph_retry_backoff_seconds * (2 ** max(0, attempt_number - 1)))
        time.sleep(sleep_seconds)

    def _graph_circuit_snapshot(self) -> Dict[str, Any]:
        self._ensure_graph_runtime_state()
        now = time.time()
        with self._graph_circuit_lock:
            is_open = self._graph_circuit_open_until > now
            remaining = max(0.0, self._graph_circuit_open_until - now)
            failures = self._graph_circuit_failures
        return {
            "graph_circuit_open": is_open,
            "graph_circuit_remaining_seconds": round(remaining, 2),
            "graph_circuit_failures": failures,
            "graph_circuit_threshold": self._graph_cb_fail_threshold,
        }

    def _graph_circuit_is_open(self) -> bool:
        self._ensure_graph_runtime_state()
        now = time.time()
        with self._graph_circuit_lock:
            if self._graph_circuit_open_until <= now:
                if self._graph_circuit_open_until > 0:
                    self._graph_circuit_open_until = 0.0
                    self._graph_circuit_failures = 0
                return False
            return True

    def _graph_circuit_record_success(self) -> None:
        self._ensure_graph_runtime_state()
        with self._graph_circuit_lock:
            self._graph_circuit_failures = 0
            self._graph_circuit_open_until = 0.0

    def _graph_circuit_record_failure(self) -> None:
        self._ensure_graph_runtime_state()
        now = time.time()
        with self._graph_circuit_lock:
            self._graph_circuit_failures += 1
            if self._graph_circuit_failures >= self._graph_cb_fail_threshold:
                self._graph_circuit_open_until = max(self._graph_circuit_open_until, now + self._graph_cb_open_seconds)

    def _annotate_graph_rows(
        self,
        rows: List[Dict[str, Any]],
        graph_path: str,
        fallback_used: bool,
        retry_attempts: int,
        extra: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            item = dict(row)
            item["graph_path"] = graph_path
            item["fallback_used"] = bool(fallback_used)
            item["retry_attempts"] = int(retry_attempts)
            if extra:
                item.update(extra)
            out.append(item)
        return out

    def _query_graph_live(self, query: str, hops: int = 2, probe: bool = False) -> Tuple[List[Dict[str, Any]], Optional[str], int, Dict[str, Any]]:
        self._ensure_graph_runtime_state()
        max_attempts = 1 + max(0, self._graph_max_retries)
        attempts = 0
        graph_endpoint = self._effective_fabric_endpoint("GRAPH")

        if self._is_kusto_endpoint(graph_endpoint):
            tokens = [] if probe else self._query_tokens(query)
            if probe:
                csl = "ops_graph_edges | take 1"
            elif tokens:
                values = ",".join(f"'{t}'" for t in tokens)
                csl = f"ops_graph_edges | where src_id in~ ({values}) or dst_id in~ ({values}) | take 50"
            else:
                csl = "ops_graph_edges | take 30"

            db_name = self._graph_database()
            if not db_name:
                return [], "missing_fabric_graph_database", 0, {"graph_path": "fabric_graph_live_kusto", "csl": csl}

            last_error: Optional[str] = None
            for attempt in range(1, max_attempts + 1):
                attempts = attempt
                rows, error = self._kusto_rows(
                    graph_endpoint,
                    csl,
                    database=db_name,
                    timeout_seconds=self._graph_timeout_seconds,
                )
                if rows:
                    return rows, None, attempts, {"graph_path": "fabric_graph_live_kusto", "csl": csl}
                if not error:
                    return [], "graph_query_returned_no_rows", attempts, {"graph_path": "fabric_graph_live_kusto", "csl": csl}
                if error == "kusto_tables_empty":
                    return [], "graph_query_returned_no_rows", attempts, {"graph_path": "fabric_graph_live_kusto", "csl": csl}
                last_error = error
                if attempt < max_attempts and self._graph_retryable_error(error):
                    self._graph_retry_sleep(attempt)
                    continue
                break
            return [], last_error or "graph_query_returned_no_rows", attempts, {"graph_path": "fabric_graph_live_kusto", "csl": csl}

        payload = {"query": "graph preflight probe", "hops": 1} if probe else {"query": query, "hops": hops}
        last_error = None
        for attempt in range(1, max_attempts + 1):
            attempts = attempt
            response = self._post_json(
                graph_endpoint,
                payload,
                timeout_seconds=self._graph_timeout_seconds,
            )
            if isinstance(response, dict) and "error" not in response:
                paths = response.get("paths", [])
                if paths:
                    return paths, None, attempts, {"graph_path": "fabric_graph_live_http"}
                return [], "graph_query_returned_no_rows", attempts, {"graph_path": "fabric_graph_live_http"}
            detail = str(response.get("error")) if isinstance(response, dict) else "graph_endpoint_query_failed"
            last_error = detail
            if attempt < max_attempts and self._graph_retryable_error(detail):
                self._graph_retry_sleep(attempt)
                continue
            break
        return [], last_error or "graph_endpoint_query_failed", attempts, {"graph_path": "fabric_graph_live_http"}

    def _probe_graph_query(self) -> Dict[str, Any]:
        self._ensure_graph_runtime_state()
        graph_endpoint = self._effective_fabric_endpoint("GRAPH")
        if not graph_endpoint:
            return {
                "status": "warn",
                "detail": "graph_endpoint_not_configured",
                "mode": self.source_mode("GRAPH"),
                "graph_path": "pg_fallback_no_endpoint",
                "retry_attempts": 0,
            }

        if self._graph_circuit_is_open():
            snapshot = self._graph_circuit_snapshot()
            return {
                "status": "warn",
                "detail": "graph_circuit_open_probe_skipped",
                "mode": self.source_mode("GRAPH"),
                "graph_path": "pg_fallback_circuit_open",
                "retry_attempts": 0,
                **snapshot,
            }

        rows, error, attempts, meta = self._query_graph_live("graph preflight probe", hops=1, probe=True)
        if rows:
            return {
                "status": "pass",
                "detail": f"rows={len(rows)}",
                "mode": self.source_mode("GRAPH"),
                "retry_attempts": attempts,
                **meta,
            }
        if error == "graph_query_returned_no_rows":
            return {
                "status": "pass",
                "detail": error,
                "mode": self.source_mode("GRAPH"),
                "retry_attempts": attempts,
                **meta,
            }
        return {
            "status": "fail",
            "detail": error or "graph_probe_failed",
            "mode": self.source_mode("GRAPH"),
            "retry_attempts": attempts,
            **meta,
        }

    def _source_error_row(
        self, source: str, code: str, detail: str, extra: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "error": detail,
            "error_code": code,
            "source": source,
        }
        if extra:
            payload.update(extra)
        return payload

    def _source_unavailable_row(self, source: str, detail: str) -> Dict[str, Any]:
        return self._source_error_row(
            source=source,
            code="source_unavailable",
            detail=detail,
        )

    @staticmethod
    def _first_present_env(keys: Tuple[str, ...]) -> Tuple[str, str]:
        for key in keys:
            value = os.getenv(key, "").strip()
            if value:
                return key, value
        return "", ""

    def _identity_guardrail_checks(self) -> Dict[str, Any]:
        strict_guardrail = os.getenv("ENFORCE_RUNTIME_GUARDRAILS", "false").strip().lower() in {
            "1", "true", "yes", "y", "on"
        }
        expected_account = os.getenv("EXPECTED_RUNTIME_ACCOUNT_UPN", "").strip()
        expected_tenant = os.getenv("EXPECTED_RUNTIME_TENANT_ID", "").strip()
        expected_subscription = os.getenv("EXPECTED_RUNTIME_SUBSCRIPTION_ID", "").strip()
        account_key, account_value = self._first_present_env(_RUNTIME_ACCOUNT_ENV_KEYS)
        tenant_key, tenant_value = self._first_present_env(_RUNTIME_TENANT_ENV_KEYS)
        subscription_key, subscription_value = self._first_present_env(_RUNTIME_SUBSCRIPTION_ENV_KEYS)

        checks: List[Dict[str, Any]] = []

        def _append_check(name: str, expected: str, fallback_expected: str, env_key: str, actual: str) -> None:
            effective_expected = expected or (fallback_expected if strict_guardrail else "")
            if not effective_expected:
                status = "warn"
                detail = "expected_runtime_value_not_set"
            elif actual:
                status = "pass" if actual.lower() == effective_expected.lower() else "fail"
                detail = "matches_guardrail" if status == "pass" else f"mismatch:{actual}"
            else:
                status = "fail"
                detail = "missing_runtime_identity_value"
            checks.append(
                {
                    "name": name,
                    "status": status,
                    "detail": detail,
                    "expected": effective_expected,
                    "env_key": env_key,
                    "actual": actual,
                }
            )

        _append_check("guardrail_account_upn", expected_account, GUARDRAIL_ACCOUNT_UPN, account_key, account_value)
        _append_check("guardrail_tenant_id", expected_tenant, GUARDRAIL_TENANT_ID, tenant_key, tenant_value)
        _append_check("guardrail_subscription_id", expected_subscription, GUARDRAIL_SUBSCRIPTION_ID, subscription_key, subscription_value)

        if any(check["status"] == "fail" for check in checks):
            overall_status = "fail"
        elif any(check["status"] == "warn" for check in checks):
            overall_status = "warn"
        else:
            overall_status = "pass"

        return {
            "timestamp": self._now_iso(),
            "status": overall_status,
            "checks": checks,
        }

    def _source_capability_payload(
        self,
        source: str,
        status: str,
        reason_code: str,
        detail: str,
        execution_mode: str,
    ) -> Dict[str, Any]:
        return {
            "source": source,
            "status": status,
            "reason_code": reason_code,
            "detail": detail,
            "execution_mode": execution_mode,
            "last_checked_at": self._now_iso(),
        }

    def _shared_fabric_endpoint_policy_allows(self) -> bool:
        return os.getenv("ALLOW_SHARED_FABRIC_ENDPOINTS", "false").strip().lower() in {
            "1", "true", "yes", "y", "on"
        }

    def _refresh_source_capabilities(self, refresh_tds: bool = False) -> Dict[str, Dict[str, Any]]:
        self._identity_guardrail_report = self._identity_guardrail_checks()
        identity_mismatch = self._identity_guardrail_report.get("status") == "fail"
        sql_available = bool(getattr(self, "sql_available", False))
        sql_backend = str(getattr(self, "sql_backend", "unavailable") or "unavailable")
        sql_unavailable_reason = str(getattr(self, "sql_unavailable_reason", "") or "")
        cosmos_container = getattr(self, "_cosmos_container", None)
        search_clients = getattr(self, "search_clients", {}) or {}
        vector_source_to_index = getattr(self, "vector_source_to_index", {}) or {}
        source_caps: Dict[str, Dict[str, Any]] = {}

        if identity_mismatch:
            for source in ("SQL", "KQL", "GRAPH", "NOSQL", "FABRIC_SQL", "VECTOR_OPS", "VECTOR_REG", "VECTOR_AIRPORT"):
                source_caps[source] = self._source_capability_payload(
                    source=source,
                    status="unavailable",
                    reason_code="tenant_guardrail_mismatch",
                    detail="runtime identity does not match hardcoded guardrail",
                    execution_mode="blocked",
                )
            self._source_capabilities = source_caps
            return source_caps

        # SQL
        if sql_available:
            source_caps["SQL"] = self._source_capability_payload(
                source="SQL",
                status="healthy",
                reason_code="ready",
                detail=f"backend={sql_backend}",
                execution_mode="live",
            )
        else:
            source_caps["SQL"] = self._source_capability_payload(
                source="SQL",
                status="unavailable",
                reason_code="sql_backend_not_available",
                detail=sql_unavailable_reason or "sql_backend_not_available",
                execution_mode="blocked",
            )

        # KQL
        kql_db = (FABRIC_KQL_DATABASE or os.getenv("FABRIC_KQL_DATABASE_NAME", "")).strip()
        kql_endpoint = self._effective_fabric_endpoint("KQL")
        if not kql_endpoint:
            source_caps["KQL"] = self._source_capability_payload(
                source="KQL",
                status="unavailable",
                reason_code="kql_endpoint_not_configured",
                detail="FABRIC_KQL_ENDPOINT/FABRIC_KUSTO_CLUSTER_URL is empty",
                execution_mode="blocked",
            )
        elif self._is_kusto_endpoint(kql_endpoint):
            _normalized, path_valid, path_reason = self._resolve_kusto_query_endpoint(kql_endpoint)
            if not path_valid:
                source_caps["KQL"] = self._source_capability_payload(
                    source="KQL",
                    status="unavailable",
                    reason_code="invalid_kusto_endpoint_path",
                    detail=path_reason,
                    execution_mode="blocked",
                )
            elif not kql_db:
                source_caps["KQL"] = self._source_capability_payload(
                    source="KQL",
                    status="degraded",
                    reason_code="missing_fabric_kql_database",
                    detail="kusto endpoint requires FABRIC_KQL_DATABASE",
                    execution_mode="live",
                )
            else:
                auth_ready, auth_reason, auth_bundle = self._fabric_auth_reason_for_source("KQL")
                if not auth_ready:
                    source_caps["KQL"] = self._source_capability_payload(
                        source="KQL",
                        status="degraded",
                        reason_code="fabric_auth_unavailable",
                        detail=f"{auth_reason};auth_mode={auth_bundle.get('auth_mode', 'none')}",
                        execution_mode="live",
                    )
                else:
                    source_caps["KQL"] = self._source_capability_payload(
                        source="KQL",
                        status="healthy",
                        reason_code="ready",
                        detail=f"endpoint configured;auth_mode={auth_bundle.get('auth_mode', 'unknown')}",
                        execution_mode="live",
                    )
        else:
            auth_ready, auth_reason, auth_bundle = self._fabric_auth_reason_for_source("KQL")
            if not auth_ready:
                source_caps["KQL"] = self._source_capability_payload(
                    source="KQL",
                    status="degraded",
                    reason_code="fabric_auth_unavailable",
                    detail=f"{auth_reason};auth_mode={auth_bundle.get('auth_mode', 'none')}",
                    execution_mode="live",
                )
            else:
                source_caps["KQL"] = self._source_capability_payload(
                    source="KQL",
                    status="healthy",
                    reason_code="ready",
                    detail=f"endpoint configured;auth_mode={auth_bundle.get('auth_mode', 'unknown')}",
                    execution_mode="live",
                )

        # GRAPH
        graph_db = self._graph_database()
        graph_endpoint = self._effective_fabric_endpoint("GRAPH")
        if not graph_endpoint:
            if sql_available:
                source_caps["GRAPH"] = self._source_capability_payload(
                    source="GRAPH",
                    status="degraded",
                    reason_code="graph_endpoint_not_configured",
                    detail="FABRIC_GRAPH_ENDPOINT/FABRIC_KUSTO_CLUSTER_URL is empty; using SQL fallback",
                    execution_mode="fallback",
                )
            else:
                source_caps["GRAPH"] = self._source_capability_payload(
                    source="GRAPH",
                    status="unavailable",
                    reason_code="graph_endpoint_not_configured",
                    detail=f"FABRIC_GRAPH_ENDPOINT/FABRIC_KUSTO_CLUSTER_URL is empty and SQL fallback is unavailable ({sql_unavailable_reason or 'sql_backend_not_available'})",
                    execution_mode="blocked",
                )
        elif self._is_kusto_endpoint(graph_endpoint):
            _normalized, path_valid, path_reason = self._resolve_kusto_query_endpoint(graph_endpoint)
            if not path_valid:
                source_caps["GRAPH"] = self._source_capability_payload(
                    source="GRAPH",
                    status="unavailable",
                    reason_code="invalid_kusto_endpoint_path",
                    detail=path_reason,
                    execution_mode="blocked",
                )
            elif not graph_db:
                if sql_available:
                    source_caps["GRAPH"] = self._source_capability_payload(
                        source="GRAPH",
                        status="degraded",
                        reason_code="missing_fabric_graph_database",
                        detail="kusto graph endpoint requires FABRIC_GRAPH_DATABASE or FABRIC_KQL_DATABASE; using SQL fallback",
                        execution_mode="fallback",
                    )
                else:
                    source_caps["GRAPH"] = self._source_capability_payload(
                        source="GRAPH",
                        status="unavailable",
                        reason_code="missing_fabric_graph_database",
                        detail="kusto graph endpoint requires FABRIC_GRAPH_DATABASE or FABRIC_KQL_DATABASE",
                        execution_mode="blocked",
                    )
            else:
                auth_ready, auth_reason, auth_bundle = self._fabric_auth_reason_for_source("GRAPH")
                if not auth_ready:
                    source_caps["GRAPH"] = self._source_capability_payload(
                        source="GRAPH",
                        status="degraded",
                        reason_code="fabric_auth_unavailable",
                        detail=f"{auth_reason};auth_mode={auth_bundle.get('auth_mode', 'none')}",
                        execution_mode="fallback" if sql_available else "live",
                    )
                elif (
                    graph_endpoint
                    and kql_endpoint
                    and graph_endpoint == kql_endpoint
                    and not self._shared_fabric_endpoint_policy_allows()
                ):
                    source_caps["GRAPH"] = self._source_capability_payload(
                        source="GRAPH",
                        status="degraded",
                        reason_code="graph_endpoint_shared_with_kql",
                        detail="GRAPH and KQL share endpoint; set ALLOW_SHARED_FABRIC_ENDPOINTS=true to suppress",
                        execution_mode="live",
                    )
                else:
                    source_caps["GRAPH"] = self._source_capability_payload(
                        source="GRAPH",
                        status="healthy",
                        reason_code="ready",
                        detail=f"endpoint configured;auth_mode={auth_bundle.get('auth_mode', 'unknown')}",
                        execution_mode="live",
                    )
        else:
            auth_ready, auth_reason, auth_bundle = self._fabric_auth_reason_for_source("GRAPH")
            if not auth_ready:
                source_caps["GRAPH"] = self._source_capability_payload(
                    source="GRAPH",
                    status="degraded",
                    reason_code="fabric_auth_unavailable",
                    detail=f"{auth_reason};auth_mode={auth_bundle.get('auth_mode', 'none')}",
                    execution_mode="fallback" if sql_available else "live",
                )
            else:
                source_caps["GRAPH"] = self._source_capability_payload(
                    source="GRAPH",
                    status="healthy",
                    reason_code="ready",
                    detail=f"endpoint configured;auth_mode={auth_bundle.get('auth_mode', 'unknown')}",
                    execution_mode="live",
                )

        # NOSQL
        if cosmos_container is not None:
            source_caps["NOSQL"] = self._source_capability_payload(
                source="NOSQL",
                status="healthy",
                reason_code="cosmos_ready",
                detail="cosmos container connected",
                execution_mode="live",
            )
        else:
            nosql_endpoint = self._effective_fabric_endpoint("NOSQL")
            if not nosql_endpoint:
                source_caps["NOSQL"] = self._source_capability_payload(
                    source="NOSQL",
                    status="unavailable",
                    reason_code="nosql_endpoint_not_configured",
                    detail="no Cosmos client and FABRIC_NOSQL_ENDPOINT/FABRIC_KUSTO_CLUSTER_URL is empty",
                    execution_mode="blocked",
                )
            elif self._is_kusto_endpoint(nosql_endpoint):
                _normalized, path_valid, path_reason = self._resolve_kusto_query_endpoint(nosql_endpoint)
                if not path_valid:
                    source_caps["NOSQL"] = self._source_capability_payload(
                        source="NOSQL",
                        status="unavailable",
                        reason_code="invalid_kusto_endpoint_path",
                        detail=path_reason,
                        execution_mode="blocked",
                    )
                elif not kql_db:
                    source_caps["NOSQL"] = self._source_capability_payload(
                        source="NOSQL",
                        status="unavailable",
                        reason_code="missing_fabric_kql_database",
                        detail="kusto nosql endpoint requires FABRIC_KQL_DATABASE",
                        execution_mode="blocked",
                    )
                else:
                    auth_ready, auth_reason, auth_bundle = self._fabric_auth_reason_for_source("NOSQL")
                    if not auth_ready:
                        source_caps["NOSQL"] = self._source_capability_payload(
                            source="NOSQL",
                            status="degraded",
                            reason_code="fabric_auth_unavailable",
                            detail=f"{auth_reason};auth_mode={auth_bundle.get('auth_mode', 'none')}",
                            execution_mode="live",
                        )
                    else:
                        source_caps["NOSQL"] = self._source_capability_payload(
                            source="NOSQL",
                            status="healthy",
                            reason_code="ready",
                            detail=f"endpoint configured;auth_mode={auth_bundle.get('auth_mode', 'unknown')}",
                            execution_mode="live",
                        )
            else:
                auth_ready, auth_reason, auth_bundle = self._fabric_auth_reason_for_source("NOSQL")
                if not auth_ready:
                    source_caps["NOSQL"] = self._source_capability_payload(
                        source="NOSQL",
                        status="degraded",
                        reason_code="fabric_auth_unavailable",
                        detail=f"{auth_reason};auth_mode={auth_bundle.get('auth_mode', 'none')}",
                        execution_mode="live",
                    )
                else:
                    source_caps["NOSQL"] = self._source_capability_payload(
                        source="NOSQL",
                        status="healthy",
                        reason_code="ready",
                        detail=f"endpoint configured;auth_mode={auth_bundle.get('auth_mode', 'unknown')}",
                        execution_mode="live",
                    )

        # FABRIC_SQL
        if refresh_tds:
            self._fabric_sql_tds_capability(refresh=True)
        fabric_sql_mode_detail = self._fabric_sql_effective_mode_detail()
        fabric_sql_mode = str(fabric_sql_mode_detail.get("mode") or "blocked")
        if fabric_sql_mode == "tds":
            source_caps["FABRIC_SQL"] = self._source_capability_payload(
                source="FABRIC_SQL",
                status="healthy",
                reason_code="tds_ready",
                detail="pyodbc and ODBC driver are available",
                execution_mode="live",
            )
        elif fabric_sql_mode == "rest":
            auth_ready = bool(fabric_sql_mode_detail.get("auth_ready", True))
            source_caps["FABRIC_SQL"] = self._source_capability_payload(
                source="FABRIC_SQL",
                status="healthy" if auth_ready else "degraded",
                reason_code="rest_ready" if auth_ready else "fabric_auth_unavailable",
                detail=str(fabric_sql_mode_detail.get("reason") or "rest_ready"),
                execution_mode="live",
            )
        else:
            source_caps["FABRIC_SQL"] = self._source_capability_payload(
                source="FABRIC_SQL",
                status="unavailable",
                reason_code="fabric_sql_not_usable",
                detail=str(fabric_sql_mode_detail.get("reason") or "fabric_sql_not_configured"),
                execution_mode="blocked",
            )

        # Vector indexes
        for source in ("VECTOR_OPS", "VECTOR_REG", "VECTOR_AIRPORT"):
            index_name = vector_source_to_index.get(source) or vector_source_to_index.get("VECTOR_OPS", "")
            if index_name and search_clients.get(index_name):
                source_caps[source] = self._source_capability_payload(
                    source=source,
                    status="healthy",
                    reason_code="ready",
                    detail=f"index={index_name}",
                    execution_mode="live",
                )
            else:
                source_caps[source] = self._source_capability_payload(
                    source=source,
                    status="unavailable",
                    reason_code="search_index_unavailable",
                    detail=f"index={index_name or 'unset'}",
                    execution_mode="blocked",
                )

        self._source_capabilities = source_caps
        return source_caps

    def source_capability(self, source: str, refresh: bool = True) -> Dict[str, Any]:
        source_norm = (source or "").upper()
        if refresh or not getattr(self, "_source_capabilities", None):
            self._refresh_source_capabilities()
        capability = (self._source_capabilities or {}).get(source_norm)
        if capability:
            return capability
        return {
            "source": source_norm,
            "status": "unknown",
            "reason_code": "unknown_source",
            "detail": "source is not recognized by capability registry",
            "execution_mode": "unknown",
            "last_checked_at": self._now_iso(),
        }

    def source_capabilities(self, refresh: bool = True) -> List[Dict[str, Any]]:
        if refresh or not getattr(self, "_source_capabilities", None):
            self._refresh_source_capabilities()
        return [
            dict(capability)
            for _, capability in sorted((self._source_capabilities or {}).items(), key=lambda item: item[0])
        ]

    def _fabric_sql_tds_capability(self, refresh: bool = False) -> Dict[str, Any]:
        """Return TDS readiness for Fabric SQL (`pyodbc` + driver + env)."""
        server = os.getenv("FABRIC_SQL_SERVER", "").strip()
        database = os.getenv("FABRIC_SQL_DATABASE", "").strip() or FABRIC_SQL_DATABASE
        cache_key = f"{server}|{database}"
        if not refresh:
            cached = getattr(self, "_fabric_sql_tds_capability_cache", None)
            if isinstance(cached, dict) and cached.get("cache_key") == cache_key:
                return cached

        if not server or not database:
            result = {
                "ok": False,
                "detail": "fabric_sql_tds_missing_server_or_database",
                "server_configured": bool(server),
                "database_configured": bool(database),
                "driver_present": False,
                "cache_key": cache_key,
            }
            self._fabric_sql_tds_capability_cache = result
            return result

        try:
            import pyodbc  # type: ignore[import-untyped]
        except Exception as exc:
            result = {
                "ok": False,
                "detail": f"pyodbc_unavailable:{exc}",
                "server_configured": True,
                "database_configured": True,
                "driver_present": False,
                "cache_key": cache_key,
            }
            self._fabric_sql_tds_capability_cache = result
            return result

        driver_present = False
        try:
            drivers = {str(d).strip() for d in pyodbc.drivers()}
            driver_present = "ODBC Driver 18 for SQL Server" in drivers
        except Exception:
            # Some environments do not expose driver listing; leave as unknown/present.
            driver_present = True

        result = {
            "ok": bool(driver_present),
            "detail": "ready" if driver_present else "odbc_driver_18_missing",
            "server_configured": True,
            "database_configured": True,
            "driver_present": bool(driver_present),
            "cache_key": cache_key,
        }
        self._fabric_sql_tds_capability_cache = result
        return result

    def _fabric_sql_effective_mode(self) -> str:
        detail = self._fabric_sql_effective_mode_detail()
        return str(detail.get("mode") or "blocked")

    def _fabric_sql_effective_mode_detail(self) -> Dict[str, Any]:
        """Compute effective FABRIC_SQL mode and block reason."""
        configured_mode = (os.getenv("FABRIC_SQL_MODE", "auto").strip().lower() or "auto")
        if configured_mode not in {"auto", "tds", "rest"}:
            configured_mode = "auto"

        rest_endpoint = self._effective_fabric_endpoint("FABRIC_SQL")
        has_rest = bool(rest_endpoint)
        has_tds_cfg = bool(os.getenv("FABRIC_SQL_SERVER", "").strip()) and bool(
            os.getenv("FABRIC_SQL_DATABASE", "").strip() or FABRIC_SQL_DATABASE
        )
        tds_cap = self._fabric_sql_tds_capability() if has_tds_cfg else {
            "ok": False,
            "detail": "fabric_sql_tds_missing_server_or_database",
        }
        rest_auth_ready, rest_auth_reason, rest_auth_bundle = self._fabric_auth_reason_for_source("FABRIC_SQL")

        def _rest_detail(reason: str) -> Dict[str, Any]:
            return {
                "mode": "rest",
                "configured_mode": configured_mode,
                "reason": reason,
                "auth_mode": str(rest_auth_bundle.get("auth_mode") or "none"),
                "auth_ready": bool(rest_auth_ready),
            }

        if configured_mode == "tds":
            if has_tds_cfg and bool(tds_cap.get("ok")):
                return {"mode": "tds", "configured_mode": configured_mode, "reason": "tds_ready"}
            return {
                "mode": "blocked",
                "configured_mode": configured_mode,
                "reason": f"TDS unavailable: {tds_cap.get('detail', 'fabric_sql_tds_unavailable')}",
            }

        if configured_mode == "rest":
            if has_rest and rest_auth_ready:
                return _rest_detail("rest_ready")
            if has_rest:
                return _rest_detail(f"rest_auth_not_ready:{rest_auth_reason}")
            reason = "fabric_sql_rest_endpoint_missing"
            return {
                "mode": "blocked",
                "configured_mode": configured_mode,
                "reason": reason,
                "auth_mode": str(rest_auth_bundle.get("auth_mode") or "none"),
            }

        # auto: REST-first policy when endpoint is configured.
        if has_rest and rest_auth_ready:
            reason = "rest_ready"
            if has_tds_cfg and bool(tds_cap.get("ok")):
                reason = "rest_preferred_over_tds"
            elif has_tds_cfg and not bool(tds_cap.get("ok")):
                reason = f"tds_unavailable_using_rest:{tds_cap.get('detail', 'fabric_sql_tds_unavailable')}"
            return _rest_detail(reason)
        if has_rest and not rest_auth_ready:
            reason = f"rest_auth_not_ready:{rest_auth_reason}"
            if has_tds_cfg and bool(tds_cap.get("ok")):
                reason = f"{reason};tds_available_but_rest_preferred"
            elif has_tds_cfg and not bool(tds_cap.get("ok")):
                reason = f"{reason};tds_unavailable:{tds_cap.get('detail', 'fabric_sql_tds_unavailable')}"
            return _rest_detail(reason)
        if has_tds_cfg and bool(tds_cap.get("ok")):
            return {"mode": "tds", "configured_mode": configured_mode, "reason": "tds_ready"}
        if has_tds_cfg and not bool(tds_cap.get("ok")):
            return {
                "mode": "blocked",
                "configured_mode": configured_mode,
                "reason": f"TDS unavailable: {tds_cap.get('detail', 'fabric_sql_tds_unavailable')}",
            }
        return {
            "mode": "blocked",
            "configured_mode": configured_mode,
            "reason": "fabric_sql_not_configured",
        }

    def source_mode(self, source: str) -> str:
        source_norm = (source or "").upper()
        capability = self.source_capability(source_norm, refresh=True)
        execution_mode = str(capability.get("execution_mode") or "").strip().lower()
        if execution_mode in {"live", "fallback", "blocked"}:
            return execution_mode
        return "unknown"

    def source_event_meta(self, source: str) -> Dict[str, Any]:
        source_norm = (source or "").upper()
        cosmos_container = getattr(self, "_cosmos_container", None)
        sql_backend = str(getattr(self, "sql_backend", "unavailable") or "unavailable")
        store_type_map = {
            "KQL": "fabric-eventhouse",
            "GRAPH": "fabric-graph",
            "NOSQL": "cosmos-nosql" if cosmos_container is not None else "fabric-nosql",
            "SQL": "warehouse-sql",
            "FABRIC_SQL": "fabric-sql-warehouse",
            "VECTOR_OPS": "vector-ops",
            "VECTOR_REG": "vector-regulatory",
            "VECTOR_AIRPORT": "vector-airport",
        }
        freshness_map = {
            "KQL": "near-real-time",
            "GRAPH": "dependency-snapshot",
            "NOSQL": "ops-doc-snapshot",
            "SQL": "warehouse-snapshot",
            "FABRIC_SQL": "warehouse-snapshot",
            "VECTOR_OPS": "indexed-context",
            "VECTOR_REG": "indexed-context",
            "VECTOR_AIRPORT": "indexed-context",
        }
        return {
            "store_type": store_type_map.get(source_norm, "unknown"),
            "endpoint_label": self.source_mode(source_norm),
            "freshness": freshness_map.get(source_norm, "unknown"),
            "sql_backend": sql_backend if source_norm == "SQL" else "",
            "capability_status": self.source_capability(source_norm, refresh=False).get("status", "unknown"),
            "capability_reason": self.source_capability(source_norm, refresh=False).get("reason_code", "unknown"),
        }

    def _probe_endpoint(self, endpoint: str, timeout_seconds: int = 5) -> Dict[str, Any]:
        if not endpoint:
            return {
                "status": "warn",
                "detail": "not_configured",
                "auth_mode": "none",
                "auth_ready": False,
                "token_ttl_seconds": None,
            }

        req = urllib.request.Request(endpoint, method="GET")
        token_bundle = self._fabric_auth_bundle_for_endpoint(endpoint)
        token = str(token_bundle.get("token") or "")
        if token:
            req.add_header("Authorization", f"Bearer {token}")
        try:
            with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:
                return {
                    "status": "pass",
                    "detail": f"reachable_http_{resp.status}",
                    "auth_mode": str(token_bundle.get("auth_mode") or "none"),
                    "auth_ready": bool(token_bundle.get("auth_ready")),
                    "token_ttl_seconds": token_bundle.get("token_ttl_seconds"),
                }
        except urllib.error.HTTPError as exc:
            # Treat auth and method errors as reachable endpoint.
            if exc.code in (400, 401, 403, 404, 405):
                return {
                    "status": "warn",
                    "detail": f"reachable_http_{exc.code}",
                    "auth_mode": str(token_bundle.get("auth_mode") or "none"),
                    "auth_ready": bool(token_bundle.get("auth_ready")),
                    "token_ttl_seconds": token_bundle.get("token_ttl_seconds"),
                }
            return {
                "status": "fail",
                "detail": f"http_{exc.code}",
                "auth_mode": str(token_bundle.get("auth_mode") or "none"),
                "auth_ready": bool(token_bundle.get("auth_ready")),
                "token_ttl_seconds": token_bundle.get("token_ttl_seconds"),
            }
        except Exception as exc:
            return {
                "status": "fail",
                "detail": str(exc),
                "auth_mode": str(token_bundle.get("auth_mode") or "none"),
                "auth_ready": bool(token_bundle.get("auth_ready")),
                "token_ttl_seconds": token_bundle.get("token_ttl_seconds"),
            }

    def fabric_preflight(self) -> Dict[str, Any]:
        self._refresh_source_capabilities(refresh_tds=True)
        identity_report = dict(getattr(self, "_identity_guardrail_report", {}) or {})
        source_capabilities = self.source_capabilities(refresh=False)
        checks: List[Dict[str, Any]] = []

        for guardrail_check in identity_report.get("checks", []):
            checks.append(
                {
                    "name": f"identity_{guardrail_check.get('name', 'guardrail')}",
                    "status": guardrail_check.get("status", "warn"),
                    "detail": guardrail_check.get("detail", ""),
                    "mode": "policy",
                    "expected": guardrail_check.get("expected", ""),
                    "actual": guardrail_check.get("actual", ""),
                    "env_key": guardrail_check.get("env_key", ""),
                }
            )

        fabric_bundle = _acquire_fabric_token_bundle()
        fabric_token = str(fabric_bundle.get("token") or "")
        token_status = "pass" if fabric_bundle.get("auth_ready") else "fail"
        checks.append(
            {
                "name": "fabric_bearer_token",
                "status": token_status,
                "detail": str(fabric_bundle.get("reason") or ("present" if fabric_token else "missing_optional_or_not_configured")),
                "mode": "n/a",
                "token_ttl_seconds": fabric_bundle.get("token_ttl_seconds"),
            }
        )
        auth_mode_effective = str(fabric_bundle.get("auth_mode") or "none")
        auth_ready = bool(fabric_bundle.get("auth_ready") and fabric_token)
        checks.append(
            {
                "name": "fabric_auth_mode",
                "status": "pass" if auth_mode_effective == "sp_client_credentials" and auth_ready else ("warn" if auth_mode_effective == "static_bearer" else "fail"),
                "detail": auth_mode_effective,
                "mode": "policy",
                "auth_mode_effective": auth_mode_effective,
                "auth_ready": auth_ready,
                "allow_static_bearer": _allow_static_fabric_bearer(),
                "token_ttl_seconds": fabric_bundle.get("token_ttl_seconds"),
            }
        )
        checks.append(
            {
                "name": "kql_schema_mode",
                "status": "pass",
                "detail": os.getenv("KQL_SCHEMA_MODE", "static"),
                "mode": "policy",
            }
        )

        endpoint_checks = [
            ("fabric_kql_endpoint", self._effective_fabric_endpoint("KQL"), "KQL"),
            ("fabric_graph_endpoint", self._effective_fabric_endpoint("GRAPH"), "GRAPH"),
            ("fabric_nosql_endpoint", self._effective_fabric_endpoint("NOSQL"), "NOSQL"),
            ("fabric_sql_endpoint", self._effective_fabric_endpoint("FABRIC_SQL"), "FABRIC_SQL"),
        ]

        live_configured = False
        for check_name, endpoint, source in endpoint_checks:
            mode = self.source_mode(source)
            if endpoint:
                live_configured = True
            path_valid = True
            path_detail = "n/a"
            normalized_endpoint = endpoint or ""
            probe_endpoint = endpoint or ""
            if endpoint and self._is_kusto_endpoint(endpoint):
                normalized_endpoint, path_valid, path_detail = self._resolve_kusto_query_endpoint(endpoint)
                if path_valid and normalized_endpoint:
                    # Probe the same normalized query URI used by runtime execution.
                    probe_endpoint = normalized_endpoint
            probe = self._probe_endpoint(probe_endpoint)
            auth_required = source in {"KQL", "GRAPH", "FABRIC_SQL"} or (
                source == "NOSQL" and endpoint and self._is_kusto_endpoint(endpoint)
            )
            auth_ready_for_source = bool(probe.get("auth_ready")) if auth_required else True
            probe_detail = str(probe.get("detail", ""))
            query_ready = bool(endpoint) and path_valid and auth_ready_for_source and probe.get("status") != "fail"
            if probe_detail in {"reachable_http_401", "reachable_http_403", "reachable_http_404"}:
                query_ready = False
            check_status = probe["status"]
            if endpoint and not path_valid:
                check_status = "fail"
            elif endpoint and auth_required and not auth_ready_for_source:
                check_status = "fail"
            elif endpoint and not query_ready:
                check_status = "fail"
            checks.append(
                {
                    "name": check_name,
                    "status": check_status,
                    "detail": probe["detail"],
                    "mode": mode,
                    "endpoint": endpoint if endpoint else "",
                    "probe_endpoint": probe_endpoint,
                    "normalized_endpoint": normalized_endpoint,
                    "path_valid_for_runtime": path_valid,
                    "path_detail": path_detail,
                    "auth_required": auth_required,
                    "auth_ready": auth_ready_for_source,
                    "auth_mode": probe.get("auth_mode", "none"),
                    "token_ttl_seconds": probe.get("token_ttl_seconds"),
                    "query_ready": query_ready,
                }
            )

        fabric_sql_tds = self._fabric_sql_tds_capability()
        fabric_sql_rest_capable = bool(FABRIC_SQL_ENDPOINT)
        fabric_sql_mode_detail = self._fabric_sql_effective_mode_detail()
        fabric_sql_effective_mode = str(fabric_sql_mode_detail.get("mode") or "blocked")
        if fabric_sql_effective_mode in {"tds", "rest"}:
            live_configured = True
        checks.append(
            {
                "name": "fabric_sql_tds_capability",
                "status": "pass" if fabric_sql_tds.get("ok") else "warn",
                "detail": str(fabric_sql_tds.get("detail", "unknown")),
                "mode": "tds",
                "server_configured": bool(fabric_sql_tds.get("server_configured")),
                "database_configured": bool(fabric_sql_tds.get("database_configured")),
                "driver_present": bool(fabric_sql_tds.get("driver_present")),
            }
        )
        checks.append(
            {
                "name": "fabric_sql_rest_capability",
                "status": "pass" if fabric_sql_rest_capable else "warn",
                "detail": "configured" if fabric_sql_rest_capable else "not_configured",
                "mode": "rest",
                "endpoint": FABRIC_SQL_ENDPOINT or "",
            }
        )
        checks.append(
            {
                "name": "fabric_sql_effective_mode",
                "status": "pass" if fabric_sql_effective_mode in {"tds", "rest"} else "fail",
                "detail": str(fabric_sql_mode_detail.get("reason") or fabric_sql_effective_mode),
                "mode": self.source_mode("FABRIC_SQL"),
                "configured_mode": str(fabric_sql_mode_detail.get("configured_mode") or os.getenv("FABRIC_SQL_MODE", "auto")),
                "effective_mode": fabric_sql_effective_mode,
            }
        )

        graph_query_probe = self._probe_graph_query()
        checks.append(
            {
                "name": "fabric_graph_query_probe",
                "status": graph_query_probe.get("status", "fail"),
                "detail": graph_query_probe.get("detail", "graph_probe_failed"),
                "mode": graph_query_probe.get("mode", self.source_mode("GRAPH")),
                "graph_path": graph_query_probe.get("graph_path", ""),
                "retry_attempts": graph_query_probe.get("retry_attempts", 0),
                "graph_circuit_open": graph_query_probe.get("graph_circuit_open", False),
                "graph_circuit_remaining_seconds": graph_query_probe.get("graph_circuit_remaining_seconds", 0),
            }
        )

        graph_circuit = self._graph_circuit_snapshot()
        checks.append(
            {
                "name": "graph_circuit_breaker_state",
                "status": "warn" if graph_circuit.get("graph_circuit_open") else "pass",
                "detail": (
                    f"open_for={graph_circuit.get('graph_circuit_remaining_seconds', 0)}s"
                    if graph_circuit.get("graph_circuit_open")
                    else "closed"
                ),
                "mode": self.source_mode("GRAPH"),
                **graph_circuit,
            }
        )

        # Cosmos DB health check
        if self._cosmos_container is not None:
            live_configured = True
            try:
                self._cosmos_container.read()
                cosmos_status = "pass"
                cosmos_detail = "cosmos_container_reachable"
            except Exception as exc:
                cosmos_status = "fail"
                cosmos_detail = f"cosmos_probe_failed: {exc}"
            checks.append({
                "name": "cosmos_nosql",
                "status": cosmos_status,
                "detail": cosmos_detail,
                "mode": "live",
                "endpoint": AZURE_COSMOS_ENDPOINT,
            })

        sql_mode = self.source_mode("SQL")
        checks.append(
            {
                "name": "sql_connectivity",
                "status": "pass" if self.sql_available else "fail",
                "detail": self.sql_backend if self.sql_available else (self.sql_unavailable_reason or "sql_not_available"),
                "mode": sql_mode,
            }
        )
        sql_schema = self.current_sql_schema()
        checks.append(
            {
                "name": "sql_schema_snapshot",
                "status": "pass" if sql_schema.get("tables") else "fail",
                "detail": f"tables={len(sql_schema.get('tables', []))}",
                "mode": sql_mode,
            }
        )

        for capability in source_capabilities:
            status_map = {"healthy": "pass", "degraded": "warn", "unavailable": "fail"}
            checks.append(
                {
                    "name": f"source_capability_{capability.get('source', '').lower()}",
                    "status": status_map.get(str(capability.get("status", "unknown")).lower(), "warn"),
                    "detail": str(capability.get("reason_code", "unknown")),
                    "mode": str(capability.get("execution_mode", "unknown")),
                }
            )

        baseline_sources = ("SQL", "NOSQL")
        baseline_unavailable = [
            source
            for source in baseline_sources
            if str(self.source_capability(source, refresh=False).get("status", "")) == "unavailable"
        ]
        checks.append(
            {
                "name": "baseline_sources",
                "status": "pass" if not baseline_unavailable else "fail",
                "detail": "ready" if not baseline_unavailable else f"unavailable:{','.join(baseline_unavailable)}",
                "mode": "policy",
            }
        )

        if any(c["status"] == "fail" for c in checks):
            overall = "fail"
        elif any(c["status"] == "warn" for c in checks):
            overall = "warn"
        else:
            overall = "pass"

        return {
            "timestamp": self._now_iso(),
            "overall_status": overall,
            "live_path_available": live_configured,
            "fabric_sql_tds_capable": bool(fabric_sql_tds.get("ok")),
            "fabric_sql_rest_capable": fabric_sql_rest_capable,
            "fabric_sql_effective_mode": fabric_sql_effective_mode,
            "auth_mode_effective": auth_mode_effective,
            "auth_ready": auth_ready,
            "fabric_token_ttl_seconds": fabric_bundle.get("token_ttl_seconds"),
            "identity_guardrail": identity_report,
            "source_capabilities": source_capabilities,
            "baseline_sources": list(baseline_sources),
            "baseline_unavailable_sources": baseline_unavailable,
            "checks": checks,
        }

    def _get_pg_connection(self, read_only: bool = False) -> Any:
        """Acquire a connection from the PostgreSQL pool.

        When *read_only* is ``True`` the connection is returned with
        ``autocommit = False`` so callers can wrap SQL in a
        ``SET TRANSACTION READ ONLY`` block.
        """
        if self._pg_pool is None:
            return None
        try:
            conn = self._pg_pool.getconn()
            if read_only:
                conn.autocommit = False
            else:
                conn.autocommit = True
            return conn
        except Exception as exc:
            logger.warning("Failed to acquire PG connection: %s", exc)
            return None

    def _put_pg_connection(self, conn: Any) -> None:
        """Return a connection to the PostgreSQL pool."""
        if self._pg_pool is not None and conn is not None:
            try:
                self._pg_pool.putconn(conn)
            except Exception as exc:
                logger.warning("Failed to return PG connection to pool: %s", exc)

    def current_sql_schema(self) -> Dict[str, Any]:
        _t0_schema = time.perf_counter()
        if not self.sql_available:
            return {
                "source": "unavailable",
                "collected_at": self._now_iso(),
                "schema_version": "none",
                "error": self.sql_unavailable_reason or "sql_connection_unavailable",
                "tables": [],
            }

        tables: List[Dict[str, Any]] = []
        conn = self._get_pg_connection()
        if conn is None:
            return {
                "source": "unavailable",
                "collected_at": self._now_iso(),
                "schema_version": "none",
                "error": "pg_pool_connection_unavailable",
                "tables": [],
            }
        try:
            cur = conn.cursor()
            visible_schemas = self.sql_visible_schemas or ["public"]
            cur.execute(
                """
                SELECT table_schema, table_name
                FROM information_schema.tables
                WHERE table_type='BASE TABLE'
                  AND table_schema = ANY(%s)
                ORDER BY table_schema, table_name
                """,
                (visible_schemas,),
            )
            table_rows = [(str(row[0]), str(row[1])) for row in cur.fetchall()]
            for schema_name, table in table_rows:
                cur.execute(
                    """
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_schema=%s AND table_name=%s
                    ORDER BY ordinal_position
                    """,
                    (schema_name, table),
                )
                cols = [{"name": str(r[0]), "type": str(r[1])} for r in cur.fetchall()]
                tables.append({"schema": schema_name, "table": table, "columns": cols})
            cur.close()
        except Exception as exc:
            return {
                "source": "live",
                "collected_at": self._now_iso(),
                "schema_version": "error",
                "error": str(exc),
                "tables": [],
            }
        finally:
            self._put_pg_connection(conn)
        result = {
            "source": "live",
            "collected_at": self._now_iso(),
            "schema_version": f"tables:{len(tables)}",
            "tables": tables,
        }
        logger.info("perf stage=%s ms=%.1f", "current_sql_schema", (time.perf_counter() - _t0_schema) * 1000)
        return result

    def cached_sql_schema(self) -> Dict[str, Any]:
        """Return SQL schema from cache if fresh, otherwise refresh from DB."""
        now = time.perf_counter()
        if self._schema_cache is not None and now < self._schema_cache_expires_at:
            return self._schema_cache
        schema = self.current_sql_schema()
        self._schema_cache = schema
        self._schema_cache_expires_at = now + self._schema_cache_ttl
        return schema

    def _detect_sql_tables(self, sql_query: str) -> List[str]:
        # Collect CTE names defined by WITH ... AS so they can be excluded.
        cte_names = {
            m.lower()
            for m in re.findall(
                r"\bWITH\s+([A-Za-z_][A-Za-z0-9_]*)\s+AS\b", sql_query, flags=re.IGNORECASE
            )
        }
        # Also handle comma-separated CTEs: WITH a AS (...), b AS (...)
        cte_names.update(
            m.lower()
            for m in re.findall(
                r",\s*([A-Za-z_][A-Za-z0-9_]*)\s+AS\s*\(", sql_query, flags=re.IGNORECASE
            )
        )

        table_tokens = re.findall(r"\b(?:FROM|JOIN)\s+([A-Za-z_][A-Za-z0-9_\.]*)", sql_query, flags=re.IGNORECASE)
        cleaned: List[str] = []
        for token in table_tokens:
            table = token.strip().strip('"').strip("`")
            if not table:
                continue
            parts = [p for p in table.split(".") if p]
            if not parts:
                continue
            if len(parts) >= 2:
                table_ref = f"{parts[-2].lower()}.{parts[-1].lower()}"
            else:
                table_ref = parts[-1].lower()
            # Skip CTE-defined names — they are not real tables.
            if table_ref in cte_names:
                continue
            if table_ref not in cleaned:
                cleaned.append(table_ref)
        return cleaned

    def _sql_table_columns(self, schema: Dict[str, Any]) -> Dict[str, set[str]]:
        table_columns: Dict[str, set[str]] = {}
        for table_entry in schema.get("tables", []):
            if not isinstance(table_entry, dict):
                continue
            table_name = str(table_entry.get("table", "")).lower().strip()
            schema_name = str(table_entry.get("schema", "")).lower().strip()
            cols = {
                str(col.get("name", "")).lower().strip()
                for col in (table_entry.get("columns") or [])
                if isinstance(col, dict) and str(col.get("name", "")).strip()
            }
            if not table_name:
                continue
            table_columns[table_name] = cols
            if schema_name:
                table_columns[f"{schema_name}.{table_name}"] = cols
        return table_columns

    def _sql_alias_map(self, sql_query: str, referenced_tables: List[str]) -> Dict[str, str]:
        alias_map: Dict[str, str] = {}
        known_refs = {t.lower() for t in referenced_tables}
        alias_pattern = re.compile(
            r"\b(?:FROM|JOIN)\s+([A-Za-z_][A-Za-z0-9_\.\"`]*)"
            r"(?:\s+(?:AS\s+)?([A-Za-z_][A-Za-z0-9_]*))?",
            flags=re.IGNORECASE,
        )
        for table_token, alias in alias_pattern.findall(sql_query):
            table_clean = table_token.strip().strip('"').strip("`")
            if not table_clean:
                continue
            parts = [p for p in table_clean.split(".") if p]
            if not parts:
                continue
            if len(parts) >= 2:
                table_ref = f"{parts[-2].lower()}.{parts[-1].lower()}"
            else:
                table_ref = parts[-1].lower()
            if table_ref not in known_refs:
                continue
            alias_norm = (alias or "").strip().lower()
            if alias_norm and alias_norm not in _SQL_RESERVED_WORDS:
                alias_map[alias_norm] = table_ref
            alias_map[parts[-1].lower()] = table_ref
        return alias_map

    def _split_sql_select_items(self, select_expr: str) -> List[str]:
        items: List[str] = []
        buf: List[str] = []
        depth = 0
        for ch in select_expr:
            if ch == "(":
                depth += 1
            elif ch == ")" and depth > 0:
                depth -= 1
            if ch == "," and depth == 0:
                item = "".join(buf).strip()
                if item:
                    items.append(item)
                buf = []
                continue
            buf.append(ch)
        tail = "".join(buf).strip()
        if tail:
            items.append(tail)
        return items

    def _validate_sql_columns(
        self,
        sql_query: str,
        schema: Dict[str, Any],
        referenced_tables: List[str],
    ) -> Optional[str]:
        if not referenced_tables:
            return None

        table_columns = self._sql_table_columns(schema)
        alias_map = self._sql_alias_map(sql_query, referenced_tables)
        missing: set[str] = set()

        def _columns_for_table(table_ref: str) -> set[str]:
            table_ref = table_ref.lower()
            if table_ref in table_columns:
                return table_columns[table_ref]
            parts = [p for p in table_ref.split(".") if p]
            bare = parts[-1] if parts else table_ref
            return table_columns.get(bare, set())

        # Validate qualified references: alias.column
        for alias, column in re.findall(
            r"\b([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)\b", sql_query
        ):
            alias_norm = alias.lower().strip()
            column_norm = column.lower().strip()
            table_ref = alias_map.get(alias_norm)
            if not table_ref:
                continue
            valid_cols = _columns_for_table(table_ref)
            if valid_cols and column_norm not in valid_cols:
                missing.add(f"{alias_norm}.{column_norm}")

        # Validate simple unqualified SELECT columns.
        select_match = re.search(r"\bSELECT\b(.*?)\bFROM\b", sql_query, flags=re.IGNORECASE | re.DOTALL)
        if select_match:
            select_items = self._split_sql_select_items(select_match.group(1))
            all_cols: set[str] = set()
            for table_ref in referenced_tables:
                all_cols.update(_columns_for_table(table_ref))
            for item in select_items:
                expr = item.strip()
                if not expr:
                    continue
                expr = re.sub(r"^\s*DISTINCT\s+", "", expr, flags=re.IGNORECASE)
                expr = re.sub(r"\s+AS\s+[A-Za-z_][A-Za-z0-9_]*\s*$", "", expr, flags=re.IGNORECASE)
                expr = re.sub(r"\s+[A-Za-z_][A-Za-z0-9_]*\s*$", "", expr)
                if expr == "*" or expr.endswith(".*"):
                    continue
                if "." in expr:
                    continue
                if "(" in expr or ")" in expr:
                    continue
                if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", expr):
                    continue
                col = expr.lower()
                if all_cols and col not in all_cols:
                    missing.add(col)

        if missing:
            return f"missing columns in current schema: {', '.join(sorted(missing))}"
        return None

    def _validate_sql_query(self, sql_query: str) -> Optional[Dict[str, Any]]:
        sql = (sql_query or "").strip()
        if not sql:
            return {"code": "sql_validation_failed", "detail": "empty_sql_query"}
        if not re.match(r"^\s*(SELECT|WITH)\b", sql, flags=re.IGNORECASE):
            return {"code": "sql_validation_failed", "detail": "only_select_or_with_queries_are_allowed"}

        # Block mutating / DDL keywords anywhere in the query.
        _sql_blocked_patterns = (
            r"\bDELETE\b", r"\bDROP\b", r"\bALTER\b", r"\bCREATE\b",
            r"\bINSERT\b", r"\bUPDATE\b", r"\bEXEC\b", r"\bEXECUTE\b",
            r"\bTRUNCATE\b", r"\bGRANT\b", r"\bREVOKE\b", r"\bMERGE\b",
        )
        for pattern in _sql_blocked_patterns:
            if re.search(pattern, sql, flags=re.IGNORECASE):
                return {"code": "sql_validation_failed", "detail": f"sql_contains_blocked_operation: {pattern}"}

        # Check for semicolons outside of string literals to prevent multi-statement injection.
        # A single trailing semicolon is a valid SQL terminator, so strip it first.
        stripped = re.sub(r'\$([a-zA-Z_]\w*)?\$.*?\$\1\$', '', sql, flags=re.DOTALL)
        stripped = re.sub(r'"[^"]*"', '', stripped)
        stripped = re.sub(r"'[^']*'", '', stripped)
        stripped = stripped.rstrip().rstrip(";")
        if ";" in stripped:
            return {"code": "sql_validation_failed", "detail": "sql_multiple_statements_not_allowed"}

        schema = self.cached_sql_schema()
        available_tables = set()
        for table_entry in schema.get("tables", []):
            if not isinstance(table_entry, dict):
                continue
            table_name = str(table_entry.get("table", "")).lower().strip()
            schema_name = str(table_entry.get("schema", "")).lower().strip()
            if table_name:
                available_tables.add(table_name)
            if schema_name and table_name:
                available_tables.add(f"{schema_name}.{table_name}")
        referenced_tables = self._detect_sql_tables(sql)
        missing_tables = [t for t in referenced_tables if t.lower() not in available_tables]
        if missing_tables:
            return {
                "code": "sql_schema_missing",
                "detail": f"missing tables in current schema: {', '.join(missing_tables)}",
            }
        missing_columns = self._validate_sql_columns(sql, schema, referenced_tables)
        if missing_columns:
            return {
                "code": "sql_schema_missing",
                "detail": missing_columns,
            }
        return None

    def execute_sql_query(self, sql_query: str) -> Tuple[List[Dict[str, Any]], List[Citation]]:
        if not self.sql_available:
            return [self._source_unavailable_row("SQL", self.sql_unavailable_reason or "sql_backend_not_available")], []

        validation_error = self._validate_sql_query(sql_query)
        if validation_error:
            return [
                self._source_error_row(
                    source="SQL",
                    code=str(validation_error.get("code")),
                    detail=str(validation_error.get("detail")),
                    extra={"sql": sql_query},
                )
            ], []

        conn = self._get_pg_connection(read_only=True)
        if conn is None:
            return [self._source_unavailable_row("SQL", "pg_pool_connection_unavailable")], []
        try:
            cur = conn.cursor()
            cur.execute("SET TRANSACTION READ ONLY")
            cur.execute(sql_query)
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description] if cur.description else []
            dict_rows = [dict(zip(columns, row)) for row in rows]
            cur.close()
            conn.commit()
        except Exception as exc:
            try:
                conn.rollback()
            except Exception:
                pass
            return [
                self._source_error_row(
                    source="SQL",
                    code="sql_runtime_error",
                    detail=str(exc),
                    extra={"sql": sql_query},
                )
            ], []
        finally:
            self._put_pg_connection(conn)

        citations: List[Citation] = []
        for idx, row in enumerate(dict_rows[:10], start=1):
            row_id = row.get("id") or row.get("asrs_report_id") or f"row_{idx}"
            title = row.get("title") or row.get("facilityDesignator") or f"SQL row {idx}"
            citations.append(
                Citation(
                    source_type="SQL",
                    identifier=str(row_id),
                    title=str(title),
                    content_preview=str(row)[:120],
                    score=0.9,
                    dataset="aviation_db",
                )
            )
        return dict_rows, citations

    def _heuristic_sql_fallback(self, query: str, need_schema_detail: str) -> Optional[str]:
        """Best-effort SQL fallback when writer returns NEED_SCHEMA."""
        schema = self.cached_sql_schema()
        tables: Dict[str, set[str]] = {}
        for table_entry in schema.get("tables", []):
            if not isinstance(table_entry, dict):
                continue
            table = str(table_entry.get("table", "")).lower().strip()
            schema_name = str(table_entry.get("schema", "")).lower().strip()
            if not table:
                continue
            cols = {
                str(col.get("name", "")).lower().strip()
                for col in (table_entry.get("columns") or [])
                if isinstance(col, dict)
            }
            tables[table] = cols
            if schema_name:
                tables[f"{schema_name}.{table}"] = cols
        asrs_cols = tables.get("asrs_reports", set())
        q = (query or "").lower()
        need_detail = str(need_schema_detail or "").lower()

        # Runway/airport fallback for queries that fail with generated columns that are
        # not present in OurAirports schema (e.g., a.iata, runway_id, airport, active).
        runway_terms = {"runway", "airport", "airports", "flight risk", "disruption", "compare"}
        runway_missing_markers = {"runway_id", "a.iata", " airport", " active", "ourairports"}
        if (
            any(term in q for term in runway_terms)
            and any(marker in need_detail for marker in runway_missing_markers)
        ):
            required_airport_cols = {"id", "ident", "iata_code"}
            required_runway_cols = {"id", "airport_ref", "surface", "length_ft", "width_ft", "closed"}

            def _safe_ident(ident: str) -> Optional[str]:
                value = str(ident or "").strip()
                if re.fullmatch(r"[a-z_][a-z0-9_]*", value):
                    return value
                return None

            airport_table_ref: Optional[str] = None
            runway_table_ref: Optional[str] = None

            for table_entry in schema.get("tables", []):
                if not isinstance(table_entry, dict):
                    continue
                table_name = str(table_entry.get("table", "")).lower().strip()
                schema_name = str(table_entry.get("schema", "")).lower().strip()
                table_ident = _safe_ident(table_name)
                schema_ident = _safe_ident(schema_name) if schema_name else None
                if not table_ident:
                    continue
                cols = {
                    str(col.get("name", "")).lower().strip()
                    for col in (table_entry.get("columns") or [])
                    if isinstance(col, dict)
                }
                qualified = f"{schema_ident}.{table_ident}" if schema_ident else table_ident
                if table_name == "ourairports_airports" and cols.issuperset(required_airport_cols):
                    airport_table_ref = qualified
                if table_name == "ourairports_runways" and cols.issuperset(required_runway_cols):
                    runway_table_ref = qualified

            if airport_table_ref and runway_table_ref:
                iata_tokens = [
                    token
                    for token in re.findall(r"\b[A-Z]{3}\b", (query or "").upper())
                    if token
                    and token not in {
                        "THE", "AND", "FOR", "WITH", "FROM", "NEXT", "MIN",
                        "RISK", "ACROSS", "FLIGHT", "COMPARE", "OVER", "UNDER",
                    }
                ]
                icao_tokens = self._extract_airports_from_query(query)
                where_clause = ""
                if iata_tokens or icao_tokens:
                    filters: List[str] = []
                    if iata_tokens:
                        filters.append(
                            "a.iata_code IN (" + ", ".join(f"'{token}'" for token in sorted(set(iata_tokens))) + ")"
                        )
                    if icao_tokens:
                        filters.append(
                            "a.ident IN (" + ", ".join(f"'{token}'" for token in sorted(set(icao_tokens))) + ")"
                        )
                    where_clause = "WHERE (" + " OR ".join(filters) + ") "
                return (
                    "SELECT "
                    "COALESCE(NULLIF(a.iata_code, ''), a.ident) AS airport, "
                    "r.id AS runway_id, "
                    "r.surface, "
                    "r.length_ft, "
                    "r.width_ft, "
                    "r.closed "
                    f"FROM {runway_table_ref} r "
                    f"JOIN {airport_table_ref} a ON r.airport_ref = a.id "
                    f"{where_clause}"
                    "ORDER BY airport ASC, r.length_ft DESC NULLS LAST "
                    "LIMIT 200"
                )

        if not asrs_cols:
            return None

        ask_top = any(token in q for token in ("top", "highest", "rank"))
        ask_count = "count" in q or "how many" in q or "number of" in q
        ask_facility = any(token in q for token in ("facility", "facilities", "airport", "airports", "location", "station"))
        if not (ask_top and ask_count):
            return None

        if ask_facility and "location" in asrs_cols:
            return (
                "SELECT COALESCE(NULLIF(location, ''), 'UNKNOWN') AS facility, "
                "COUNT(*) AS report_count "
                "FROM asrs_reports "
                "GROUP BY facility "
                "ORDER BY report_count DESC "
                "LIMIT 5"
            )

        if "aircraft_type" in asrs_cols:
            return (
                "SELECT COALESCE(NULLIF(aircraft_type, ''), 'UNKNOWN') AS category, "
                "COUNT(*) AS report_count "
                "FROM asrs_reports "
                "GROUP BY category "
                "ORDER BY report_count DESC "
                "LIMIT 5"
            )

        if "flight_phase" in asrs_cols:
            return (
                "SELECT COALESCE(NULLIF(flight_phase, ''), 'UNKNOWN') AS category, "
                "COUNT(*) AS report_count "
                "FROM asrs_reports "
                "GROUP BY category "
                "ORDER BY report_count DESC "
                "LIMIT 5"
            )

        return None

    # =========================================================================
    # Core Retrieval Methods
    # =========================================================================

    def query_sql(self, query: str, sql_hint: str = None) -> Tuple[List[Dict], str, List[Citation]]:
        """Execute SQL query against the aviation database."""
        _t0_sql = time.perf_counter()
        sql_capability = self.source_capability("SQL", refresh=True)
        if sql_capability.get("status") == "unavailable":
            detail = str(sql_capability.get("detail") or sql_capability.get("reason_code") or "sql_backend_not_available")
            row = self._source_unavailable_row("SQL", detail)
            return [row], "", []

        if sql_hint:
            enhanced_query = f"{query}\nHint: {sql_hint}"
        else:
            enhanced_query = query

        try:
            if self.use_legacy_sql_generator:
                sql = self.sql_generator.generate(enhanced_query)
            else:
                schema = self.cached_sql_schema()
                sql = self.sql_writer.generate(
                    user_query=enhanced_query,
                    evidence_type="generic",
                    sql_schema=schema,
                    entities={"airports": [], "flight_ids": [], "routes": [], "stations": [], "alternates": []},
                    time_window={"horizon_min": 120, "start_utc": None, "end_utc": None},
                    constraints={"sql_hint": sql_hint or "", "dialect": self.sql_dialect},
                )
        except Exception as exc:
            try:
                schema = self.cached_sql_schema()
                if schema.get("tables"):
                    schema_text = self._format_schema_for_legacy_generator(schema)
                    sql = self.sql_generator.generate_with_context(enhanced_query, context=schema_text)
                else:
                    sql = self.sql_generator.generate(enhanced_query)
            except Exception as fallback_exc:
                row = self._source_error_row(
                    "SQL",
                    "sql_generation_failed",
                    f"primary_writer_error={exc}; legacy_writer_error={fallback_exc}",
                )
                return [row], "", []

        if sql.strip().startswith("-- NEED_SCHEMA"):
            fallback_sql = self._heuristic_sql_fallback(query, sql)
            if fallback_sql:
                results, citations = self.execute_sql_query(fallback_sql)
                if results and not results[0].get("error_code"):
                    logger.info("NEED_SCHEMA fallback succeeded: original=%s fallback=%s rows=%d",
                                sql[:120], fallback_sql[:120], len(results))
                return results, fallback_sql, citations
            return [self._source_error_row("SQL", "sql_schema_missing", sql, {"sql": sql})], sql, []

        results, citations = self.execute_sql_query(sql)
        logger.info("perf stage=%s ms=%.1f", "query_sql", (time.perf_counter() - _t0_sql) * 1000)
        return results, sql, citations

    def query_semantic(
        self,
        query: str,
        top: int = 5,
        embedding: Optional[List[float]] = None,
        source: str = "VECTOR_OPS",
        filter_expression: Optional[str] = None,
    ) -> Tuple[List[Dict], List[Citation]]:
        """Search a specific semantic index using hybrid/vector retrieval."""
        _t0_sem = time.perf_counter()
        source_capability = self.source_capability(source, refresh=True)
        if source_capability.get("status") == "unavailable":
            return [self._source_unavailable_row(source, str(source_capability.get("reason_code") or "search_index_unavailable"))], []

        index_name = self.vector_source_to_index.get(source) or self.vector_source_to_index.get("VECTOR_OPS", "idx_ops_narratives")
        client = self.search_clients.get(index_name)
        if client is None:
            return [self._source_unavailable_row(source, f"search_index_unavailable:{index_name}")], []

        top = max(1, int(top))
        top_raw = max(top, _RERANK_RAW_CANDIDATES if _RERANK_ENABLED else top)

        if embedding is None:
            embedding, embedding_error = self.get_embedding_safe(query)
            if embedding is None:
                return [
                    self._source_error_row(
                        source=source,
                        code="embedding_runtime_error",
                        detail=embedding_error or "embedding_lookup_failed",
                        extra={"index": index_name},
                    )
                ], []

        vector_kwargs: Dict[str, Any] = {
            "vector": embedding,
            "fields": "content_vector",
        }
        vector_kwargs[self._vector_k_param] = top_raw
        vector_query = VectorizedQuery(**vector_kwargs)

        select_fields = [
            "id",
            "asrs_report_id",
            "title",
            "content",
            "event_date",
            "aircraft_type",
            "flight_phase",
            "location",
            "narrative_type",
            "source",
            "source_file",
        ]

        search_kwargs: Dict[str, Any] = {
            "search_text": query,
            "vector_queries": [vector_query],
            "top": top_raw,
            "filter": filter_expression,
            "select": select_fields,
        }

        if _RERANK_ENABLED:
            search_kwargs["query_type"] = "semantic"
            search_kwargs["semantic_configuration_name"] = SEARCH_SEMANTIC_CONFIG

        try:
            results = client.search(**search_kwargs)
        except Exception as exc:
            if _RERANK_ENABLED:
                search_kwargs.pop("query_type", None)
                search_kwargs.pop("semantic_configuration_name", None)
                try:
                    results = client.search(**search_kwargs)
                except Exception as fallback_exc:
                    return [
                        self._source_error_row(
                            source=source,
                            code="semantic_runtime_error",
                            detail=str(fallback_exc),
                            extra={"index": index_name},
                        )
                    ], []
            else:
                return [
                    self._source_error_row(
                        source=source,
                        code="semantic_runtime_error",
                        detail=str(exc),
                        extra={"index": index_name},
                    )
                ], []

        results_list: List[Dict[str, Any]] = []
        citations: List[Citation] = []
        try:
            for r in results:
                result_dict = dict(r)
                result_dict["vector_source"] = source
                result_dict["vector_index"] = index_name
                score_raw = float(r.get("@search.score", 0.0) or 0.0)
                score_rerank = float(r.get("@search.reranker_score", 0.0) or 0.0)
                score_final = score_rerank if score_rerank > 0 else score_raw
                result_dict["__vector_score_raw"] = score_raw
                result_dict["__vector_score_rerank"] = score_rerank
                result_dict["__vector_score_final"] = score_final
                results_list.append(result_dict)

                report_id = r.get("asrs_report_id") or r.get("id", "")
                citation_title = r.get("title") or f"{source} {report_id}"
                citations.append(
                    Citation(
                        source_type=source,
                        identifier=str(report_id),
                        title=str(citation_title),
                        content_preview=str(r.get("content", ""))[:120],
                        score=score_final,
                        dataset=index_name,
                    )
                )
        except Exception as exc:
            return [
                self._source_error_row(
                    source=source,
                    code="semantic_runtime_error",
                    detail=str(exc),
                    extra={"index": index_name},
                )
            ], []

        results_list.sort(key=lambda row: float(row.get("__vector_score_final", 0.0) or 0.0), reverse=True)
        citations.sort(key=lambda c: c.score, reverse=True)

        # Relevance filtering (1.2): drop low-score results when threshold is configured.
        if _SEMANTIC_MIN_SCORE > 0:
            results_list = [r for r in results_list if float(r.get("__vector_score_final", 0)) >= _SEMANTIC_MIN_SCORE]
            citations = [c for c in citations if c.score >= _SEMANTIC_MIN_SCORE]

        logger.info("perf stage=%s source=%s ms=%.1f", "query_semantic", source, (time.perf_counter() - _t0_sem) * 1000)
        return results_list[:top], citations[:top]

    def query_semantic_multi(
        self,
        query: str,
        sources: Optional[List[str]] = None,
        top_per_source: int = 3,
        filters: Optional[Dict[str, str]] = None,
    ) -> Tuple[List[Dict], List[Citation]]:
        """Query multiple vector indexes and merge by score."""
        source_list = sources or ["VECTOR_OPS"]
        embedding = self.get_embedding(query)

        merged_rows: List[Dict[str, Any]] = []
        merged_citations: List[Citation] = []
        for source in source_list:
            source_filter = (filters or {}).get(source)
            rows, cites = self.query_semantic(
                query,
                top=top_per_source,
                embedding=embedding,
                source=source,
                filter_expression=source_filter,
            )
            merged_rows.extend(rows)
            merged_citations.extend(cites)

        merged_rows.sort(key=lambda r: float(r.get("__vector_score_final", r.get("@search.score", 0.0)) or 0.0), reverse=True)
        merged_citations.sort(key=lambda c: c.score, reverse=True)
        return merged_rows, merged_citations

    def _looks_like_kql_text(self, query: str) -> bool:
        candidate = (query or "").strip()
        if not candidate:
            return False
        if "|" in candidate:
            return True
        lowered = candidate.lower()
        return lowered.startswith("let ") or lowered.startswith(".show")

    def _kql_table_columns(self, kql_schema: Optional[Dict[str, Any]]) -> Dict[str, set[str]]:
        if not isinstance(kql_schema, dict):
            return {}
        out: Dict[str, set[str]] = {}
        for table_entry in kql_schema.get("tables", []):
            if not isinstance(table_entry, dict):
                continue
            table = str(table_entry.get("table", "")).strip().lower()
            if not table:
                continue
            cols: set[str] = set()
            for col in (table_entry.get("columns") or []):
                if isinstance(col, dict):
                    name = str(col.get("name", "")).strip()
                else:
                    name = str(col).strip()
                if name:
                    cols.add(name.lower())
            out[table] = cols
        return out

    def _infer_kql_table(self, csl: str) -> str:
        stripped = re.sub(r'"[^"]*"', "", csl or "")
        stripped = re.sub(r"'[^']*'", "", stripped)
        stripped, _bindings = self._split_kql_let_bindings(stripped)
        match = re.search(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\b", stripped)
        if not match:
            return ""
        table = match.group(1).strip().lower()
        if table in _KQL_RESERVED_WORDS:
            return ""
        return table

    def _split_kql_let_bindings(self, csl: str) -> Tuple[str, Dict[str, str]]:
        """Return (main_query, let_bindings) for leading let declarations."""
        remaining = (csl or "").strip()
        bindings: Dict[str, str] = {}
        while True:
            match = re.match(
                r"^\s*let\s+([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*?);\s*",
                remaining,
                flags=re.IGNORECASE | re.DOTALL,
            )
            if not match:
                break
            alias = match.group(1).strip().lower()
            expr = match.group(2).strip()
            if alias and expr:
                bindings[alias] = expr
            remaining = remaining[match.end():]
        return remaining.strip(), bindings

    def _resolve_kql_table_alias(self, table: str, bindings: Dict[str, str]) -> str:
        """Resolve let-bound alias to a concrete table when possible."""
        current = (table or "").strip().lower()
        if not current:
            return ""
        visited: set[str] = set()
        for _ in range(8):
            if current in visited:
                break
            visited.add(current)
            expr = bindings.get(current)
            if not expr:
                return current
            resolved = self._infer_kql_table(expr)
            if not resolved:
                return current
            current = resolved
        return current

    def _extract_kql_column_refs(self, csl: str) -> set[str]:
        refs: set[str] = set()
        stripped = re.sub(r'"[^"]*"', "", csl or "")
        stripped = re.sub(r"'[^']*'", "", stripped)
        for segment in [seg.strip() for seg in stripped.split("|") if seg.strip()]:
            seg_low = segment.lower()
            if seg_low.startswith("where "):
                for col in re.findall(
                    r"\b([A-Za-z_][A-Za-z0-9_]*)\s*(==|=~|!=|!~|has_any|has|contains|in~|in|between|>|<|>=|<=)",
                    segment,
                    flags=re.IGNORECASE,
                ):
                    refs.add(str(col[0]).lower())
                continue
            if seg_low.startswith("project "):
                project_expr = segment[len("project "):]
                for item in [x.strip() for x in project_expr.split(",") if x.strip()]:
                    expr = item.split("=", 1)[1].strip() if "=" in item else item
                    if re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", expr):
                        refs.add(expr.lower())
                        continue
                    function_tokens = {
                        fn.strip().lower()
                        for fn in re.findall(r"\b([A-Za-z_][A-Za-z0-9_]*)\s*\(", expr)
                    }
                    for token in re.findall(r"\b([A-Za-z_][A-Za-z0-9_]*)\b", expr):
                        token_l = token.lower()
                        if token_l in function_tokens:
                            continue
                        refs.add(token_l)
                continue
            if seg_low.startswith("sort by ") or seg_low.startswith("order by "):
                by_expr = re.split(r"\bby\b", segment, maxsplit=1, flags=re.IGNORECASE)[-1]
                for item in [x.strip() for x in by_expr.split(",") if x.strip()]:
                    token = item.split()[0].strip()
                    if re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", token):
                        refs.add(token.lower())
                continue
            if seg_low.startswith("top ") and re.search(r"\bby\b", seg_low):
                by_expr = re.split(r"\bby\b", segment, maxsplit=1, flags=re.IGNORECASE)[-1]
                for item in [x.strip() for x in by_expr.split(",") if x.strip()]:
                    token = item.split()[0].strip()
                    if re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", token):
                        refs.add(token.lower())
                continue
            if seg_low.startswith("summarize ") and re.search(r"\bby\b", seg_low):
                by_expr = re.split(r"\bby\b", segment, maxsplit=1, flags=re.IGNORECASE)[-1]
                for item in [x.strip() for x in by_expr.split(",") if x.strip()]:
                    token = item.split("=")[-1].split()[0].strip()
                    if re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", token):
                        refs.add(token.lower())
        return refs

    def _sanitize_kql_query(self, csl: str) -> Tuple[str, bool, str]:
        text = (csl or "").strip()
        if not text:
            return text, False, ""
        reasons: List[str] = []
        updated = re.sub(r";\s*\|", " |", text)
        if updated != text:
            text = updated
            reasons.append("semicolon_before_pipe_removed")
        if text.rstrip().endswith(";"):
            text = text.rstrip().rstrip(";").rstrip()
            reasons.append("trailing_semicolon_removed")
        return text, bool(reasons), ",".join(reasons)

    def prepare_kql_query(self, csl: str, window_minutes: int = 60) -> Tuple[str, Dict[str, Any]]:
        prepared = self._ensure_kql_window(csl, window_minutes)
        sanitized, changed, reason = self._sanitize_kql_query(prepared)
        return sanitized, {
            "query_rewritten": bool(changed),
            "rewrite_reason": reason if changed else "",
        }

    def _validate_kql_query(self, csl: str, kql_schema: Optional[Dict[str, Any]] = None) -> Optional[str]:
        text = (csl or "").strip()
        if not text:
            return "empty_kql_query"
        blocked_patterns = (
            r"\bdrop\b",
            r"\bdelete\b",
            r"\bcreate\b",
            r"\balter\b",
            r"\bingest\b",
        )
        for pattern in blocked_patterns:
            if re.search(pattern, text, flags=re.IGNORECASE):
                return f"kql_contains_blocked_operation:{pattern}"
        # Check for semicolons outside of string literals to prevent multi-statement injection.
        # First strip string literals, then strip legitimate `let <name> = <expr>;` bindings
        # which require semicolons as delimiters in valid KQL.
        stripped = re.sub(r'"[^"]*"', '', text)
        stripped = re.sub(r"'[^']*'", '', stripped)
        stripped = re.sub(r'\blet\s+\w+\s*=\s*[^;]*;', '', stripped)
        if ";" in stripped:
            return "kql_multiple_statements_not_allowed"
        if re.search(r"\btime_now\s*\(", stripped, flags=re.IGNORECASE):
            return "kql_unsupported_function:time_now"
        # After stripping let bindings, block Kusto management commands (dot-commands)
        # that could leak info or mutate state (e.g. `.show commands`, `.set-or-replace`).
        if re.search(r'\.\s*(show|set|append|move|rename|replace|enable|disable)\b', stripped, flags=re.IGNORECASE):
            return "kql_contains_blocked_management_command"
        table_columns = self._kql_table_columns(kql_schema)
        if table_columns:
            main_query, let_bindings = self._split_kql_let_bindings(text)
            table = self._infer_kql_table(main_query)
            table_is_alias = bool(table and table in let_bindings)
            resolved_table = self._resolve_kql_table_alias(table, let_bindings) if table else ""
            if table and resolved_table not in table_columns:
                return f"kql_unknown_table:{table}"
            if resolved_table and resolved_table in table_columns and not table_is_alias:
                allowed = table_columns.get(resolved_table, set())
                refs = self._extract_kql_column_refs(text)
                unknown = sorted(
                    ref for ref in refs
                    if ref not in allowed
                    and ref not in _KQL_RESERVED_WORDS
                    and ref not in _KQL_ALLOWED_FUNCTIONS
                )
                if unknown:
                    return f"kql_unknown_columns:{','.join(unknown[:8])}"
        return None

    # Time columns used by known Kusto tables.
    _KQL_TIME_COLUMNS = ("time_position", "valid_time_from", "valid_time_to", "timestamp")

    def _ensure_kql_window(self, csl: str, window_minutes: int) -> str:
        text = (csl or "").strip()
        if not text:
            return text
        # Already has an explicit time window — don't double-filter.
        if re.search(r"\bago\s*\(", text, flags=re.IGNORECASE):
            return text
        if re.search(r"\bbetween\b.*\bdatetime\b", text, flags=re.IGNORECASE):
            return text
        # Only append a time filter when a known time column appears as a column
        # reference in a pipe expression (not inside a string literal).
        stripped = re.sub(r'"[^"]*"', '', text)
        stripped = re.sub(r"'[^']*'", '', stripped)
        for col in self._KQL_TIME_COLUMNS:
            if re.search(rf"\b{col}\b", stripped, flags=re.IGNORECASE):
                return f"{text}\n| where {col} > ago({max(1, int(window_minutes))}m)"
        return text

    def query_kql(
        self,
        query: str,
        window_minutes: int = 60,
        kql_schema: Optional[Dict[str, Any]] = None,
    ) -> Tuple[List[Dict], List[Citation]]:
        """Retrieve event-window signals from Eventhouse (live only)."""
        effective_kql_schema = kql_schema
        if not isinstance(effective_kql_schema, dict) or "tables" not in effective_kql_schema:
            effective_kql_schema = {
                "tables": [
                    {
                        "table": "opensky_states",
                        "columns": ["callsign", "icao24", "time_position", "timestamp"],
                    },
                    {
                        "table": "hazards_airsigmets",
                        "columns": ["raw_text", "valid_time_from", "valid_time_to", "points"],
                    },
                    {
                        "table": "hazards_gairmets",
                        "columns": ["valid_time", "points", "hazard"],
                    },
                ]
            }
        table_columns = self._kql_table_columns(effective_kql_schema)
        kql_endpoint = self._effective_fabric_endpoint("KQL")
        kql_capability = self.source_capability("KQL", refresh=True)
        if kql_capability.get("status") == "unavailable":
            reason_code = str(kql_capability.get("reason_code") or "")
            soft_block_reasons = {"fabric_auth_unavailable", "missing_fabric_kql_database"}
            if kql_endpoint and reason_code in soft_block_reasons:
                pass
            else:
                detail = str(kql_capability.get("detail") or reason_code or "kql_endpoint_not_configured")
                return [self._source_unavailable_row("KQL", detail)], []

        if self._is_kusto_endpoint(kql_endpoint):
            if self._looks_like_kql_text(query):
                csl, _meta = self.prepare_kql_query(query, window_minutes)
            else:
                airports = self._extract_airports_from_query(query)
                query_lower = (query or "").lower()
                is_weather_query = any(w in query_lower for w in ("weather", "hazard", "sigmet", "airmet", "turbulence", "icing", "storm"))
                is_airport_risk_query = any(
                    token in query_lower
                    for token in (
                        "risk",
                        "departure",
                        "disruption",
                        "compare",
                        "next-90",
                        "next 90",
                    )
                )
                if airports and (is_weather_query or is_airport_risk_query):
                    iata_tokens = sorted(set(re.findall(r"\b[A-Z]{3}\b", (query or "").upper())))
                    terms = sorted(set([*airports, *iata_tokens]))
                    values = ",".join(f"'{term}'" for term in terms)

                    has_airsigmets = "hazards_airsigmets" in table_columns
                    has_gairmets = "hazards_gairmets" in table_columns
                    airsig_cols = table_columns.get("hazards_airsigmets", set())
                    gair_cols = table_columns.get("hazards_gairmets", set())
                    window = max(1, int(window_minutes))

                    if has_airsigmets and {"raw_text", "valid_time_from", "valid_time_to"}.issubset(airsig_cols):
                        csl = (
                            "let horizon_start = now(); "
                            f"let horizon_end = now() + {window}m; "
                            "hazards_airsigmets "
                            "| where valid_time_to >= horizon_start and valid_time_from <= horizon_end "
                            f"| where raw_text has_any ({values}) "
                            "| top 40 by valid_time_from desc"
                        )
                    elif has_gairmets and {"valid_time", "points"}.issubset(gair_cols):
                        csl = (
                            "let horizon_start = now(); "
                            f"let horizon_end = now() + {window}m; "
                            "hazards_gairmets "
                            "| where valid_time between (horizon_start .. horizon_end) "
                            f"| where tostring(points) has_any ({values}) or tostring(hazard) has_any ({values}) "
                            "| top 40 by valid_time desc"
                        )
                    else:
                        explicit_ids = self._extract_explicit_flight_identifiers(query)
                        if not explicit_ids:
                            return [
                                self._source_error_row(
                                    source="KQL",
                                    code="kql_unmappable_airport_filter",
                                    detail="airport identifiers cannot be mapped to available KQL tables",
                                    extra={"airports": airports},
                                )
                            ], []
                        values = ",".join(f"'{value}'" for value in explicit_ids)
                        csl = (
                            "opensky_states "
                            f"| where callsign has_any ({values}) or icao24 in~ ({values}) "
                            "| take 50"
                        )
                elif airports:
                    explicit_ids = self._extract_explicit_flight_identifiers(query)
                    if not explicit_ids:
                        return [
                            self._source_error_row(
                                source="KQL",
                                code="kql_unmappable_airport_filter",
                                detail="airport identifiers cannot be mapped to opensky_states without explicit callsign or icao24 values",
                                extra={"airports": airports},
                            )
                        ], []
                    values = ",".join(f"'{value}'" for value in explicit_ids)
                    csl = (
                        "opensky_states "
                        f"| where callsign has_any ({values}) or icao24 in~ ({values}) "
                        "| take 50"
                    )
                else:
                    tokens = self._query_tokens(query)
                    if tokens:
                        values = ",".join(f"'{t}'" for t in tokens)
                        csl = (
                            "opensky_states "
                            f"| where callsign has_any ({values}) or icao24 in~ ({values}) "
                            "| take 50"
                        )
                    else:
                        csl = "opensky_states | take 50"
                csl, _changed, _reason = self._sanitize_kql_query(csl)

            validation_error = self._validate_kql_query(csl, kql_schema=effective_kql_schema)
            if validation_error:
                return [
                    self._source_error_row(
                        source="KQL",
                        code="kql_validation_failed",
                        detail=validation_error,
                        extra={"csl": csl},
                    )
                ], []

            rows, error = self._kusto_rows(
                kql_endpoint,
                csl,
                timeout_seconds=_source_call_timeout_seconds(),
            )
            if rows:
                citation = Citation(
                    source_type="KQL",
                    identifier="eventhouse_live",
                    title="Fabric Eventhouse query",
                    content_preview=str(rows)[:120],
                    score=1.0,
                    dataset="fabric-eventhouse",
                )
                return rows, [citation]
            return [
                self._source_error_row(
                    source="KQL",
                    code="kql_runtime_error",
                    detail=error or "kql_query_returned_no_rows",
                    extra={"csl": csl},
                )
            ], []
        else:
            payload = {"query": query, "window_minutes": window_minutes}
            response = self._post_json(
                kql_endpoint,
                payload,
                timeout_seconds=_source_call_timeout_seconds(),
            )
            if isinstance(response, dict) and "error" not in response:
                rows = response.get("rows", [])
                if rows:
                    citation = Citation(
                        source_type="KQL",
                        identifier="eventhouse_live",
                        title="Fabric Eventhouse query",
                        content_preview=str(rows)[:120],
                        score=1.0,
                        dataset="fabric-eventhouse",
                    )
                    return rows, [citation]
            detail = str(response.get("error")) if isinstance(response, dict) else "kql_endpoint_query_failed"
            return [self._source_error_row("KQL", "kql_runtime_error", detail)], []

    def _query_graph_pg_fallback(
        self,
        query: str,
        hops: int = 2,
        edge_types: Optional[List[str]] = None,
        unavailable_detail: Optional[str] = None,
    ) -> Tuple[List[Dict], List[Citation]]:
        """Fallback: query ops_graph_edges from PostgreSQL with iterative BFS multi-hop traversal.

        Args:
            query: Natural language query to extract seed tokens from.
            hops: Maximum BFS hops (capped at 4).
            edge_types: Optional list of edge types to filter on (e.g. ["HAS_RUNWAY", "CONNECTS"]).
                        When provided, only edges of these types are traversed.
        """
        if not self.sql_available:
            detail = "FABRIC_GRAPH_ENDPOINT not configured and SQL unavailable"
            if unavailable_detail:
                detail = f"{detail}; context={unavailable_detail}"
            return [self._source_unavailable_row("GRAPH", detail)], []

        # Verify ops_graph_edges table exists in the current schema.
        schema = self.cached_sql_schema()
        table_names = {str(t.get("table", "")).lower() for t in schema.get("tables", []) if isinstance(t, dict)}
        if "ops_graph_edges" not in table_names:
            detail = "FABRIC_GRAPH_ENDPOINT not configured and ops_graph_edges table not found in PostgreSQL"
            if unavailable_detail:
                detail = f"{detail}; context={unavailable_detail}"
            return [self._source_unavailable_row("GRAPH", detail)], []

        # Build optional edge-type filter clause.
        edge_filter_clause = ""
        if edge_types:
            safe_types = [t.replace("'", "''").upper() for t in edge_types]
            type_list = ", ".join(f"'{t}'" for t in safe_types)
            edge_filter_clause = f" AND edge_type IN ({type_list})"

        tokens = self._query_tokens(query)
        if not tokens:
            sql = f"SELECT src_type, src_id, edge_type, dst_type, dst_id FROM ops_graph_edges WHERE 1=1{edge_filter_clause} LIMIT 30"
            rows, citations = self.execute_sql_query(sql)
            if rows and not rows[0].get("error_code"):
                return rows, [Citation(source_type="GRAPH", identifier="graph_pg_fallback", title="Graph edges (PostgreSQL fallback)", content_preview=str(rows)[:120], score=0.8, dataset="ops_graph_edges")]
            return rows, citations

        # Iterative BFS: start from seed tokens, expand up to `hops` iterations.
        max_hops = min(hops, 4)
        frontier: set = set(tokens)
        visited_ids: set = set()
        seen_edges: set = set()
        all_rows: List[Dict] = []
        max_total_rows = 200

        for _hop in range(max_hops):
            if not frontier or len(all_rows) >= max_total_rows:
                break
            search_ids = sorted(frontier - visited_ids)
            if not search_ids:
                break
            visited_ids.update(search_ids)
            # Sanitize tokens (defense-in-depth: _query_tokens already limits to alnum).
            safe_ids = [t.replace("'", "''") for t in search_ids]
            placeholders = ", ".join(f"'{t}'" for t in safe_ids)
            sql = (
                f"SELECT src_type, src_id, edge_type, dst_type, dst_id "
                f"FROM ops_graph_edges "
                f"WHERE (UPPER(src_id) IN ({placeholders}) OR UPPER(dst_id) IN ({placeholders})){edge_filter_clause} "
                f"LIMIT 50"
            )
            rows, _ = self.execute_sql_query(sql)
            next_frontier: set = set()
            for row in rows:
                if not isinstance(row, dict) or row.get("error_code"):
                    continue
                edge_key = (str(row.get("src_type", "")), str(row.get("src_id", "")), str(row.get("edge_type", "")), str(row.get("dst_type", "")), str(row.get("dst_id", "")))
                if edge_key in seen_edges:
                    continue
                seen_edges.add(edge_key)
                all_rows.append(row)
                if len(all_rows) >= max_total_rows:
                    break
                src_id = str(row.get("src_id", "")).upper()
                dst_id = str(row.get("dst_id", "")).upper()
                if src_id and src_id not in visited_ids:
                    next_frontier.add(src_id)
                if dst_id and dst_id not in visited_ids:
                    next_frontier.add(dst_id)
            frontier = next_frontier

        if all_rows:
            return all_rows, [Citation(source_type="GRAPH", identifier="graph_pg_fallback", title="Graph edges (PostgreSQL fallback)", content_preview=str(all_rows)[:120], score=0.8, dataset="ops_graph_edges")]
        return [self._source_error_row("GRAPH", "graph_runtime_error", "bfs_returned_no_rows")], []

    def query_graph(self, query: str, hops: int = 2) -> Tuple[List[Dict], List[Citation]]:
        """Retrieve graph relationships from Fabric graph endpoint, with PostgreSQL fallback."""
        graph_capability = self.source_capability("GRAPH", refresh=True)
        if graph_capability.get("status") == "unavailable":
            detail = str(graph_capability.get("detail") or graph_capability.get("reason_code") or "graph_endpoint_not_configured")
            return [self._source_unavailable_row("GRAPH", detail)], []
        graph_execution_mode = str(graph_capability.get("execution_mode") or "").strip().lower()
        if graph_execution_mode == "fallback":
            reason = str(graph_capability.get("detail") or graph_capability.get("reason_code") or "graph_fallback_mode")
            rows, citations = self._query_graph_pg_fallback(
                query,
                hops=hops,
                unavailable_detail=f"Graph live path skipped by capability mode fallback ({reason})",
            )
            snapshot = self._graph_circuit_snapshot()
            return self._annotate_graph_rows(
                rows,
                "pg_fallback_capability_mode",
                True,
                0,
                extra={"graph_live_error": "capability_forced_fallback", **snapshot},
            ), citations

        graph_endpoint = self._effective_fabric_endpoint("GRAPH")
        if not graph_endpoint:
            rows, citations = self._query_graph_pg_fallback(
                query,
                hops=hops,
                unavailable_detail="FABRIC_GRAPH_ENDPOINT not configured; attempting SQL fallback",
            )
            snapshot = self._graph_circuit_snapshot()
            return self._annotate_graph_rows(
                rows,
                "pg_fallback_no_endpoint",
                True,
                0,
                extra=snapshot,
            ), citations

        if self._graph_circuit_is_open():
            snapshot = self._graph_circuit_snapshot()
            rows, citations = self._query_graph_pg_fallback(
                query,
                hops=hops,
                unavailable_detail="Graph live path skipped because circuit breaker is open",
            )
            return self._annotate_graph_rows(
                rows,
                "pg_fallback_circuit_open",
                True,
                0,
                extra=snapshot,
            ), citations

        paths, live_error, retry_attempts, live_meta = self._query_graph_live(query, hops=hops)
        if paths:
            self._graph_circuit_record_success()
            graph_path = str(live_meta.get("graph_path", "fabric_graph_live"))
            enriched = self._annotate_graph_rows(
                paths,
                graph_path,
                False,
                retry_attempts,
                extra={"graph_live_error": ""},
            )
            citation = Citation(
                source_type="GRAPH",
                identifier="fabric_graph_live",
                title="Fabric graph traversal",
                content_preview=str(paths)[:120],
                score=1.0,
                dataset="fabric-graph",
            )
            return enriched, [citation]

        if live_error and live_error != "graph_query_returned_no_rows":
            self._graph_circuit_record_failure()
        else:
            # Empty result sets should not open the circuit.
            self._graph_circuit_record_success()

        fallback_path = "pg_fallback_live_error" if live_error and live_error != "graph_query_returned_no_rows" else "pg_fallback_live_empty"
        fallback_rows, fallback_citations = self._query_graph_pg_fallback(
            query,
            hops=hops,
            unavailable_detail=f"Fabric graph live query failed ({live_error or 'unknown_error'}) and SQL fallback unavailable",
        )
        snapshot = self._graph_circuit_snapshot()
        fallback_enriched = self._annotate_graph_rows(
            fallback_rows,
            fallback_path,
            True,
            retry_attempts,
            extra={
                "graph_live_error": live_error or "graph_query_returned_no_rows",
                **snapshot,
            },
        )
        if fallback_enriched:
            return fallback_enriched, fallback_citations

        return [
            self._source_error_row(
                "GRAPH",
                "graph_runtime_error",
                live_error or "graph_query_returned_no_rows",
                {
                    "graph_path": "live_graph_error_no_fallback",
                    "fallback_used": False,
                    "retry_attempts": retry_attempts,
                    "graph_live_error": live_error or "graph_query_returned_no_rows",
                    **live_meta,
                    **snapshot,
                },
            )
        ], []

    def _query_cosmos_notams(self, query: str) -> Tuple[List[Dict], List[Citation]]:
        """Query NOTAM documents from Cosmos DB for NoSQL."""
        airports = self._extract_airports_from_query(query)
        try:
            if airports:
                placeholders = ", ".join(f"@icao{i}" for i in range(len(airports)))
                cosmos_sql = (
                    f"SELECT * FROM c WHERE c.icao IN ({placeholders})"
                    " AND c.status = 'active'"
                    " ORDER BY c.effective_from DESC"
                )
                parameters = [{"name": f"@icao{i}", "value": code} for i, code in enumerate(airports)]
                items = list(self._cosmos_container.query_items(
                    query=cosmos_sql,
                    parameters=parameters,
                    enable_cross_partition_query=True,
                    max_item_count=30,
                ))
            else:
                cosmos_sql = (
                    "SELECT * FROM c WHERE c.status = 'active'"
                    " ORDER BY c.effective_from DESC"
                    " OFFSET 0 LIMIT 30"
                )
                items = list(self._cosmos_container.query_items(
                    query=cosmos_sql,
                    enable_cross_partition_query=True,
                    max_item_count=30,
                ))
        except Exception as exc:
            return [self._source_error_row("NOSQL", "cosmos_runtime_error", str(exc))], []

        if not items:
            return [self._source_error_row("NOSQL", "nosql_runtime_error", "cosmos_query_returned_no_docs")], []

        citations: List[Citation] = []
        for idx, doc in enumerate(items[:10], start=1):
            notam_num = doc.get("notam_number") or doc.get("id") or f"notam_{idx}"
            icao = doc.get("icao", "")
            title = f"NOTAM {notam_num}" + (f" ({icao})" if icao else "")
            citations.append(Citation(
                source_type="NOSQL",
                identifier=str(doc.get("id", notam_num)),
                title=title,
                content_preview=str(doc.get("content", ""))[:120],
                score=1.0,
                dataset="cosmos-notams",
            ))
        return items, citations

    def query_nosql(self, query: str) -> Tuple[List[Dict], List[Citation]]:
        """Retrieve NoSQL-style records — Cosmos DB first, then Fabric REST fallback."""
        nosql_capability = self.source_capability("NOSQL", refresh=True)
        if nosql_capability.get("status") == "unavailable":
            detail = str(nosql_capability.get("detail") or nosql_capability.get("reason_code") or "nosql_endpoint_not_configured")
            return [self._source_unavailable_row("NOSQL", detail)], []

        # Path 1: Cosmos DB native SDK
        if self._cosmos_container is not None:
            return self._query_cosmos_notams(query)

        # Path 2: Fabric REST / Kusto endpoint (backward compat)
        nosql_endpoint = self._effective_fabric_endpoint("NOSQL")
        if not nosql_endpoint:
            return [self._source_unavailable_row("NOSQL", "NOSQL source not configured (no Cosmos DB or FABRIC_NOSQL_ENDPOINT)")], []

        if self._is_kusto_endpoint(nosql_endpoint):
            docs, _error = self._kusto_rows(
                nosql_endpoint,
                "hazards_airsigmets | take 30",
                timeout_seconds=_source_call_timeout_seconds(),
            )
            if docs:
                citation = Citation(
                    source_type="NOSQL",
                    identifier="nosql_live",
                    title="NoSQL lookup",
                    content_preview=str(docs)[:120],
                    score=1.0,
                    dataset="nosql-live",
                )
                return docs, [citation]
            return [self._source_error_row("NOSQL", "nosql_runtime_error", _error or "nosql_query_returned_no_rows")], []

        payload = {"query": query}
        response = self._post_json(
            nosql_endpoint,
            payload,
            timeout_seconds=_source_call_timeout_seconds(),
        )
        if isinstance(response, dict) and "error" not in response:
            docs = response.get("docs", [])
            if docs:
                citation = Citation(
                    source_type="NOSQL",
                    identifier="nosql_live",
                    title="NoSQL lookup",
                    content_preview=str(docs)[:120],
                    score=1.0,
                    dataset="nosql-live",
                )
                return docs, [citation]
        detail = str(response.get("error")) if isinstance(response, dict) else "nosql_endpoint_query_failed"
        return [self._source_error_row("NOSQL", "nosql_runtime_error", detail)], []

    def _get_fabric_sql_schema(self) -> str:
        """Return warehouse-specific schema hint for the Fabric SQL writer."""
        return """
Tables in Fabric SQL Warehouse (T-SQL dialect):

- bts_ontime_reporting: Year INT, Quarter INT, Month INT, DayofMonth INT,
    DayOfWeek INT, FlightDate VARCHAR, IATA_Code_Marketing_Airline VARCHAR,
    Flight_Number_Marketing_Airline VARCHAR, IATA_Code_Operating_Airline VARCHAR,
    Tail_Number VARCHAR, Flight_Number_Operating_Airline VARCHAR,
    Origin VARCHAR, OriginCityName VARCHAR, OriginState VARCHAR,
    Dest VARCHAR, DestCityName VARCHAR, DestState VARCHAR,
    CRSDepTime VARCHAR, DepTime VARCHAR, DepDelay FLOAT, DepDelayMinutes FLOAT,
    DepDel15 FLOAT, CRSArrTime VARCHAR, ArrTime VARCHAR, ArrDelay FLOAT,
    ArrDelayMinutes FLOAT, ArrDel15 FLOAT, Cancelled FLOAT,
    CancellationCode VARCHAR, Diverted FLOAT, CRSElapsedTime FLOAT,
    ActualElapsedTime FLOAT, AirTime FLOAT, Distance FLOAT, DistanceGroup INT,
    CarrierDelay FLOAT, WeatherDelay FLOAT, NASDelay FLOAT,
    SecurityDelay FLOAT, LateAircraftDelay FLOAT

- airline_delay_causes: year INT, month INT, carrier VARCHAR,
    carrier_name VARCHAR, airport VARCHAR, airport_name VARCHAR,
    arr_flights FLOAT, arr_del15 FLOAT, carrier_ct FLOAT,
    weather_ct FLOAT, nas_ct FLOAT, security_ct FLOAT,
    late_aircraft_ct FLOAT, arr_cancelled FLOAT, arr_diverted FLOAT,
    arr_delay FLOAT, carrier_delay FLOAT, weather_delay FLOAT,
    nas_delay FLOAT, security_delay FLOAT, late_aircraft_delay FLOAT

Notes:
- IATA_Code_Marketing_Airline is the 2-letter carrier code (e.g. 'DL', 'AA', 'UA')
- Origin/Dest are IATA airport codes (e.g. 'ATL', 'JFK', 'LAX')
- Delay columns are in minutes; NULL means no delay of that type
- Cancelled: 1.0 = cancelled, 0.0 = not cancelled
- Use TOP N instead of LIMIT N (T-SQL dialect)
"""

    def _query_fabric_sql_tds(self, query: str, server: str, database: str) -> Tuple[List[Dict], List[Citation]]:
        """Query Fabric SQL Warehouse via TDS (pyodbc) with AAD token auth."""
        # Generate SQL using warehouse-specific schema.
        try:
            schema = self._get_fabric_sql_schema()
            sql = self.sql_writer.generate(
                user_query=query,
                evidence_type="generic",
                sql_schema=schema,
                entities={"airports": [], "flight_ids": [], "routes": [], "stations": [], "alternates": []},
                time_window={"horizon_min": 120, "start_utc": None, "end_utc": None},
                constraints={"dialect": "tsql"},
            )
        except Exception as exc:
            return [self._source_error_row("FABRIC_SQL", "sql_generation_failed", str(exc))], []

        if sql.strip().startswith("-- NEED_SCHEMA"):
            return [self._source_error_row("FABRIC_SQL", "sql_schema_missing", sql, {"sql": sql})], []

        logger.info("FABRIC_SQL TDS query: %s", sql[:200])

        sql_token_scope = "https://database.windows.net/.default"
        token_bundle = _acquire_fabric_token_bundle(scope=sql_token_scope)
        token = str(token_bundle.get("token") or "")
        if not token or not bool(token_bundle.get("auth_ready")):
            detail = str(token_bundle.get("reason") or "fabric_sql_tds_auth_not_ready")
            return [self._source_error_row("FABRIC_SQL", "fabric_sql_auth_unavailable", detail, {"sql": sql})], []
        token_aud = str(_decode_jwt_payload(token).get("aud") or "")
        if token_aud and "database.windows.net" not in token_aud:
            detail = f"fabric_sql_tds_invalid_token_audience:{token_aud}"
            return [self._source_error_row("FABRIC_SQL", "fabric_sql_auth_unavailable", detail, {"sql": sql})], []

        connect_timeout = _env_int("FABRIC_SQL_TDS_CONNECT_TIMEOUT_SECONDS", 10, minimum=1)
        query_timeout = _env_int("FABRIC_SQL_TDS_QUERY_TIMEOUT_SECONDS", 15, minimum=1)

        try:
            import pyodbc
            import struct
            encoded = token.encode("UTF-16-LE")
            token_bytes = struct.pack(f"<I{len(encoded)}s", len(encoded), encoded)

            conn_str = (
                f"Driver={{ODBC Driver 18 for SQL Server}};"
                f"Server={server},1433;"
                f"Database={database};"
                f"Encrypt=yes;"
                f"TrustServerCertificate=no;"
                f"Connection Timeout={connect_timeout};"
            )
            conn = pyodbc.connect(conn_str, attrs_before={1256: token_bytes}, timeout=connect_timeout)
            cursor = conn.cursor()
            cursor.timeout = query_timeout
            cursor.execute(sql)
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
            conn.close()
        except Exception as exc:
            detail = str(exc)
            if "18456" in detail:
                return [self._source_error_row("FABRIC_SQL", "fabric_sql_auth_unavailable", detail, {"sql": sql})], []
            return [self._source_error_row("FABRIC_SQL", "fabric_sql_tds_error", detail, {"sql": sql})], []

        if not rows:
            return [self._source_error_row("FABRIC_SQL", "fabric_sql_no_rows", "query returned no rows", {"sql": sql})], []

        citations: List[Citation] = []
        for idx, row in enumerate(rows[:10], start=1):
            row_id = row.get("id") or f"fabric_sql_row_{idx}"
            title = row.get("title") or f"Fabric SQL row {idx}"
            citations.append(Citation(
                source_type="FABRIC_SQL",
                identifier=str(row_id),
                title=str(title),
                content_preview=str(row)[:120],
                score=0.9,
                dataset="fabric-sql-warehouse",
            ))
        return rows, citations

    def _query_fabric_sql_rest(self, query: str) -> Tuple[List[Dict], List[Citation]]:
        """Query Fabric SQL via REST endpoint."""
        fabric_sql_endpoint = self._effective_fabric_endpoint("FABRIC_SQL")
        if not fabric_sql_endpoint:
            return [self._source_unavailable_row("FABRIC_SQL", "FABRIC_SQL_ENDPOINT not configured")], []

        try:
            schema = self._get_fabric_sql_schema()
            sql = self.sql_writer.generate(
                user_query=query,
                evidence_type="generic",
                sql_schema=schema,
                entities={"airports": [], "flight_ids": [], "routes": [], "stations": [], "alternates": []},
                time_window={"horizon_min": 120, "start_utc": None, "end_utc": None},
                constraints={"dialect": "tsql"},
            )
        except Exception as exc:
            return [self._source_error_row("FABRIC_SQL", "sql_generation_failed", str(exc))], []

        if sql.strip().startswith("-- NEED_SCHEMA"):
            return [self._source_error_row("FABRIC_SQL", "sql_schema_missing", sql, {"sql": sql})], []

        payload: Dict[str, Any] = {"query": sql}
        if FABRIC_SQL_DATABASE:
            payload["database"] = FABRIC_SQL_DATABASE

        response = self._post_json(
            fabric_sql_endpoint,
            payload,
            timeout_seconds=_source_call_timeout_seconds(),
        )
        if isinstance(response, dict) and response.get("error"):
            return [self._source_error_row("FABRIC_SQL", "fabric_sql_runtime_error", str(response.get("error")), {"sql": sql})], []

        rows: List[Dict[str, Any]] = []
        if isinstance(response, list):
            rows = response
        elif isinstance(response, dict):
            rows = response.get("rows", response.get("results", []))

        if not rows:
            return [self._source_error_row("FABRIC_SQL", "fabric_sql_runtime_error", "fabric_sql_query_returned_no_rows", {"sql": sql})], []

        citations: List[Citation] = []
        for idx, row in enumerate(rows[:10], start=1):
            row_id = row.get("id") or f"fabric_sql_row_{idx}"
            title = row.get("title") or f"Fabric SQL row {idx}"
            citations.append(Citation(
                source_type="FABRIC_SQL",
                identifier=str(row_id),
                title=str(title),
                content_preview=str(row)[:120],
                score=0.9,
                dataset="fabric-sql-warehouse",
            ))
        return rows, citations

    def query_fabric_sql(self, query: str) -> Tuple[List[Dict], List[Citation]]:
        """Retrieve data from Fabric SQL warehouse."""
        fabric_sql_capability = self.source_capability("FABRIC_SQL", refresh=True)
        if fabric_sql_capability.get("status") == "unavailable":
            detail = str(
                fabric_sql_capability.get("detail")
                or fabric_sql_capability.get("reason_code")
                or "fabric_sql_not_configured"
            )
            return [self._source_unavailable_row("FABRIC_SQL", detail)], []

        mode_detail = self._fabric_sql_effective_mode_detail()
        mode = str(mode_detail.get("mode") or "blocked")
        if mode == "rest":
            if not bool(mode_detail.get("auth_ready")):
                detail = str(mode_detail.get("reason") or "fabric_sql_rest_auth_not_ready")
                return [self._source_unavailable_row("FABRIC_SQL", detail)], []
            return self._query_fabric_sql_rest(query)
        if mode == "tds":
            tds_capability = self._fabric_sql_tds_capability(refresh=False)
            if not bool(tds_capability.get("ok")):
                detail = str(tds_capability.get("detail") or "fabric_sql_tds_unavailable")
                return [self._source_unavailable_row("FABRIC_SQL", detail)], []
            fabric_sql_server = os.getenv("FABRIC_SQL_SERVER", "").strip()
            fabric_sql_database = os.getenv("FABRIC_SQL_DATABASE", "").strip() or FABRIC_SQL_DATABASE
            return self._query_fabric_sql_tds(query, fabric_sql_server, fabric_sql_database)
        return [self._source_unavailable_row("FABRIC_SQL", str(mode_detail.get("reason") or "fabric_sql_not_usable"))], []

    # =========================================================================
    # Route Execution Methods
    # =========================================================================

    def execute_sql_route(self, query: str, sql_hint: str = None) -> RetrievalResult:
        """Execute SQL-only retrieval."""
        results, sql, citations = self.query_sql(query, sql_hint)

        context = {"sql_results": results}
        answer = self._synthesize_answer(query, context, "SQL")

        return RetrievalResult(
            answer=answer,
            route="SQL",
            reasoning="Query requires precise structured data",
            citations=citations,
            sql_results=results,
            sql_query=sql
        )

    def execute_semantic_route(self, query: str) -> RetrievalResult:
        """Execute semantic-only retrieval (multi-index)."""
        multi_sources = _env_csv("SEMANTIC_ROUTE_INDEXES", "VECTOR_OPS,VECTOR_REG")
        valid = [s.upper() for s in multi_sources if s.upper() in self.vector_source_to_index]
        if not valid:
            valid = ["VECTOR_OPS"]
        results, citations = self.query_semantic_multi(
            query, sources=valid, top_per_source=max(1, 5 // len(valid))
        )

        context = {"semantic_results": [
            {k: v for k, v in r.items() if k != "content_vector"}
            for r in results
        ]}
        answer = self._synthesize_answer(query, context, "SEMANTIC")

        return RetrievalResult(
            answer=answer,
            route="SEMANTIC",
            reasoning="Query requires semantic understanding/similarity",
            citations=citations,
            semantic_results=results
        )

    def execute_hybrid_route(self, query: str, sql_hint: str = None) -> RetrievalResult:
        """Execute hybrid retrieval (SQL + Semantic in parallel)."""
        query_embedding, _embedding_error = self.get_embedding_safe(query)

        sql_results, sql_query, sql_citations = [], None, []
        semantic_results, semantic_citations = [], []

        with ThreadPoolExecutor(max_workers=2) as executor:
            sql_future = executor.submit(self.query_sql, query, sql_hint)
            semantic_future = executor.submit(self.query_semantic, query, 3, query_embedding)

            try:
                sql_results, sql_query, sql_citations = sql_future.result(timeout=30)
            except Exception as e:
                logger.error("SQL query error in parallel execution: %s", e)

            try:
                semantic_results, semantic_citations = semantic_future.result(timeout=30)
            except Exception as e:
                logger.error("Semantic query error in parallel execution: %s", e)

        context = {
            "sql_results": sql_results[:10] if sql_results else [],
            "semantic_context": [
                {k: str(v)[:200] for k, v in r.items() if k != "content_vector"}
                for r in semantic_results
            ] if semantic_results else [],
        }

        answer = self._synthesize_answer(query, context, "HYBRID")

        all_citations = sql_citations[:5] + semantic_citations[:3]

        return RetrievalResult(
            answer=answer,
            route="HYBRID",
            reasoning="Query requires both structured data and semantic context",
            citations=all_citations,
            sql_results=sql_results[:10] if sql_results else [],
            semantic_results=semantic_results[:5] if semantic_results else [],
            sql_query=sql_query
        )

    def retrieve_source(
        self,
        source: str,
        query: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Tuple[List[Dict[str, Any]], List[Citation], Optional[str]]:
        """Execute retrieval against one logical source."""
        cfg = params or {}
        capability = self.source_capability(source, refresh=True)
        source_mode = self.source_mode(source)
        if source_mode == "blocked" or capability.get("status") == "unavailable":
            row = self._source_unavailable_row(
                source,
                str(capability.get("reason_code") or f"{source} is blocked by current source policy/configuration"),
            )
            return [row], [], None
        with _ur_tracer.start_as_current_span(f"source.{source.lower()}", attributes={"query.length": len(query)}) as span:
            if source == "SQL":
                rows, sql, citations = self.query_sql(query, cfg.get("sql_hint"))
                span.set_attribute("row_count", len(rows))
                return rows, citations, sql
            if source == "KQL":
                wm = int(cfg.get("window_minutes", 60))
                rows, citations = self.query_kql(query, window_minutes=wm)
                span.set_attribute("row_count", len(rows))
                span.set_attribute("window_minutes", wm)
                return rows, citations, None
            if source == "GRAPH":
                hops = int(cfg.get("hops", 2))
                rows, citations = self.query_graph(query, hops=hops)
                span.set_attribute("row_count", len(rows))
                span.set_attribute("hops", hops)
                return rows, citations, None
            if source == "NOSQL":
                rows, citations = self.query_nosql(query)
                span.set_attribute("row_count", len(rows))
                return rows, citations, None
            if source == "FABRIC_SQL":
                rows, citations = self.query_fabric_sql(query)
                span.set_attribute("row_count", len(rows))
                return rows, citations, None
            if source in ("VECTOR_OPS", "VECTOR_REG", "VECTOR_AIRPORT"):
                rows, citations = self.query_semantic(
                    query,
                    top=int(cfg.get("top", 5)),
                    embedding=cfg.get("embedding"),
                    source=source,
                    filter_expression=cfg.get("filter"),
                )
                span.set_attribute("row_count", len(rows))
                return rows, citations, None
        return [{"error": f"unknown_source:{source}"}], [], None

    # =========================================================================
    # Main Interface
    # =========================================================================

    def check_pii(self, text: str) -> PiiCheckResult:
        """Check text for PII."""
        if not self.pii_filter:
            return PiiCheckResult(has_pii=False, entities=[])
        return self.pii_filter.check(text)

    def answer(self, query: str, use_llm_routing: bool = True) -> RetrievalResult:
        """
        Main entry point - route query and return answer with citations.
        All queries are checked for PII before processing.
        """
        # Step 0: Check for PII before processing
        if self.pii_filter:
            pii_result = self.pii_filter.check(query)
            if pii_result.error:
                logger.warning("PII check completed with error (fail-open): %s", pii_result.error)
            if pii_result.has_pii:
                warning = self.pii_filter.format_warning(pii_result.entities)
                logger.warning("PII DETECTED - Query blocked. Categories: %s",
                               [e.category for e in pii_result.entities])
                return RetrievalResult(
                    answer=warning,
                    route="BLOCKED",
                    reasoning="Query contains personally identifiable information",
                    pii_blocked=True,
                    pii_warning=warning
                )

        # Route the query
        if use_llm_routing:
            route_result = self.router.route(query)
        else:
            route_result = {
                "route": self.router.quick_route(query),
                "reasoning": "Heuristic routing"
            }

        route = route_result.get("route", "HYBRID")
        sql_hint = route_result.get("sql_hint")

        logger.info("Query: %s | Route: %s - %s", query, route, route_result.get("reasoning", ""))

        # Execute appropriate route
        if route == "SQL":
            result = self.execute_sql_route(query, sql_hint)
        elif route == "SEMANTIC":
            result = self.execute_semantic_route(query)
        else:  # HYBRID
            result = self.execute_hybrid_route(query, sql_hint)

        result.reasoning = route_result.get("reasoning", result.reasoning)
        return result

    _ROUTE_INSTRUCTIONS: Dict[str, str] = {
        "SQL": "Focus on the precise data from SQL results.",
        "SEMANTIC": "Focus on document content and similarity matches.",
        "HYBRID": "Combine structured data with semantic context for a comprehensive answer.",
        "AGENTIC": "Prioritize evidence reconciliation across multiple source types.",
    }

    def _synthesis_system_prompt(self, route: str) -> str:
        return f"""You are a helpful aviation data analyst assistant.
Answer the user's question based on the provided context.
{self._ROUTE_INSTRUCTIONS.get(route, '')}

Guidelines:
- Be concise but informative
- Format numbers nicely
- If showing multiple items, use a clear list or table format
- Reference data sources using [N] citation markers for key claims
- Do not make claims unsupported by the retrieved context
- If the context is insufficient, say so clearly"""

    def _synthesize_answer(
        self,
        query: str,
        context: dict,
        route: str,
        conversation_history: Optional[List[Dict[str, str]]] = None,
    ) -> str:
        """Generate natural language answer from retrieved context."""
        _t0_synth = time.perf_counter()

        context_str = f"""
Query: {query}

Retrieved Data:
{context}
"""

        messages: List[Dict[str, str]] = [
            {"role": "system", "content": self._synthesis_system_prompt(route)},
        ]
        if conversation_history:
            messages.extend(conversation_history)
        messages.append({"role": "user", "content": context_str})

        try:
            response = self.llm.chat.completions.create(
                model=self.llm_deployment,
                messages=messages,
            )
            logger.info("perf stage=%s ms=%.1f", "synthesize_answer", (time.perf_counter() - _t0_synth) * 1000)
            return response.choices[0].message.content
        except Exception as exc:
            logger.error("LLM synthesis failed: %s", exc)
            return "I'm unable to generate a response right now due to a temporary service issue. Please try again shortly."

    def _synthesize_answer_stream(
        self,
        query: str,
        context: dict,
        route: str,
        conversation_history: Optional[List[Dict[str, str]]] = None,
    ) -> Generator[Dict[str, Any], None, None]:
        """Stream synthesis tokens as agent_update events (true streaming)."""
        _synth_span = _ur_tracer.start_span("synthesis", attributes={"route": route})
        _t0_synth = time.perf_counter()

        context_str = f"""
Query: {query}

Retrieved Data:
{context}
"""

        messages: List[Dict[str, str]] = [
            {"role": "system", "content": self._synthesis_system_prompt(route)},
        ]
        if conversation_history:
            messages.extend(conversation_history)
        messages.append({"role": "user", "content": context_str})

        try:
            stream = self.llm.chat.completions.create(
                model=self.llm_deployment,
                messages=messages,
                stream=True,
            )
            emitted_chars = 0
            refusal_message = ""
            for chunk in stream:
                text_chunk, refusal_chunk = self._extract_chat_chunk_text(chunk)
                if text_chunk:
                    emitted_chars += len(text_chunk)
                    yield {"type": "agent_update", "content": text_chunk}
                if refusal_chunk and not refusal_message:
                    refusal_message = refusal_chunk
            if emitted_chars == 0 and refusal_message:
                yield {
                    "type": "agent_error",
                    "error_code": "llm_refusal",
                    "terminal_reason": "llm_refusal",
                    "message": refusal_message,
                }
            _synth_ms = (time.perf_counter() - _t0_synth) * 1000
            _synth_span.set_attribute("latency_ms", _synth_ms)
            _synth_span.end()
            logger.info("perf stage=%s ms=%.1f", "synthesize_answer_stream", _synth_ms)
        except Exception as exc:
            _synth_span.set_attribute("error", True)
            _synth_span.end()
            logger.error("LLM streaming synthesis failed: %s — falling back to non-streaming", exc)
            answer = self._synthesize_answer(query, context, route, conversation_history=conversation_history)
            yield {"type": "agent_update", "content": answer}

    @classmethod
    def _normalize_text_delta(cls, value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, str):
            return value
        if isinstance(value, list):
            return "".join(cls._normalize_text_delta(item) for item in value)
        if isinstance(value, dict):
            return "".join(
                cls._normalize_text_delta(value.get(key))
                for key in ("text", "content", "value", "output_text")
            )
        return "".join(
            cls._normalize_text_delta(getattr(value, attr, None))
            for attr in ("text", "content", "value", "output_text")
        )

    @classmethod
    def _extract_chat_chunk_text(cls, chunk: Any) -> Tuple[str, str]:
        choices = getattr(chunk, "choices", None) or []
        choice = choices[0] if choices else None
        delta = getattr(choice, "delta", None) if choice is not None else None

        content = ""
        if delta is not None:
            content = cls._normalize_text_delta(getattr(delta, "content", None))
            if not content:
                content = cls._normalize_text_delta(getattr(delta, "text", None))
        if not content and choice is not None:
            content = cls._normalize_text_delta(getattr(choice, "text", None))
        if not content and choice is not None:
            content = cls._normalize_text_delta(getattr(choice, "content", None))
        if not content:
            content = cls._normalize_text_delta(getattr(chunk, "output_text", None))
        if not content:
            content = cls._normalize_text_delta(getattr(chunk, "text", None))

        refusal = ""
        if delta is not None:
            refusal = cls._normalize_text_delta(getattr(delta, "refusal", None))
        if not refusal and choice is not None:
            refusal = cls._normalize_text_delta(getattr(choice, "refusal", None))
        if not refusal:
            refusal = cls._normalize_text_delta(getattr(chunk, "refusal", None))
        return content, refusal


# =============================================================================
# Context Window Management (3.1)
# =============================================================================

def _estimate_tokens(text: str) -> int:
    """Rough token count: ~4 chars per token."""
    return len(text) // 4


def _truncate_context_to_budget(context_dict: Dict[str, Any], budget: int) -> Dict[str, Any]:
    """Truncate synthesis context to stay within a token budget.

    Always includes small summary keys.  Iterates source keys in priority order
    and includes items until the budget is exhausted.
    """
    if budget <= 0:
        return context_dict

    # Always-include keys (small metadata).
    always_include = {"coverage_summary", "conflict_summary"}
    result: Dict[str, Any] = {}
    running = 0
    for key in always_include:
        if key in context_dict:
            val = str(context_dict[key])
            running += _estimate_tokens(val)
            result[key] = context_dict[key]

    # Priority order for source data.
    priority_keys = [
        "reconciled_items", "sql_results", "kql_results",
        "graph_results", "nosql_results", "vector_results",
    ]
    for key in priority_keys:
        if key not in context_dict:
            continue
        items = context_dict[key]
        if not isinstance(items, list):
            result[key] = items
            running += _estimate_tokens(str(items))
            continue

        # Sort by fusion score if available.
        sorted_items = sorted(
            items,
            key=lambda r: float(r.get("__fusion_score", 0)) if isinstance(r, dict) else 0,
            reverse=True,
        )
        kept: List[Any] = []
        for item in sorted_items:
            cost = _estimate_tokens(str(item))
            if running + cost > budget and kept:
                break
            kept.append(item)
            running += cost
        result[key] = kept

    # Carry over any remaining keys not in the priority list.
    for key in context_dict:
        if key not in result:
            result[key] = context_dict[key]

    return result


# =============================================================================
# Answer Grounding Check (3.3)
# =============================================================================

def _check_answer_grounding(answer: str, citation_count: int) -> Dict[str, Any]:
    """Check whether the synthesized answer references valid citations."""
    markers = set(int(m) for m in re.findall(r'\[(\d+)\]', answer))
    valid = {m for m in markers if 1 <= m <= citation_count}
    invalid = markers - valid
    if valid and not invalid:
        status = "grounded"
    elif valid:
        status = "partially_grounded"
    else:
        status = "ungrounded"
    return {
        "has_citations": len(valid) > 0,
        "citation_markers": sorted(valid),
        "invalid_markers": sorted(invalid),
        "grounding_status": status,
    }


# =============================================================================
# Convenience Functions
# =============================================================================

_singleton_retriever: Optional[UnifiedRetriever] = None
_singleton_lock = threading.Lock()


def answer_question(query: str, use_llm_routing: bool = True) -> dict:
    """Simple function to get an answer with citations."""
    global _singleton_retriever
    if _singleton_retriever is None:
        with _singleton_lock:
            if _singleton_retriever is None:
                _singleton_retriever = UnifiedRetriever()
    result = _singleton_retriever.answer(query, use_llm_routing)
    return result.to_dict()


if __name__ == "__main__":
    print("=" * 70)
    print("UNIFIED RETRIEVER TEST")
    print("=" * 70)

    retriever = UnifiedRetriever()

    test_queries = [
        ("Top 5 largest airlines by fleet size", "SQL"),
        ("Airlines known for premium service", "SEMANTIC"),
        ("Best airlines for current travel demand", "HYBRID"),
    ]

    for query, expected_route in test_queries:
        print(f"\n{'='*70}")
        print(f"TEST: {query}")
        print(f"Expected Route: {expected_route}")
        print("=" * 70)

        result = retriever.answer(query)

        print(f"\nRoute Used: {result.route}")
        print(f"Reasoning: {result.reasoning}")
        print(f"\nAnswer:\n{result.answer[:500]}...")
        print(f"\nCitations ({len(result.citations)}):")
        for c in result.citations[:5]:
            print(f"  - {c}")

        if result.sql_query:
            print(f"\nSQL Query: {result.sql_query[:100]}...")
