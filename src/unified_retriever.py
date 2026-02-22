#!/usr/bin/env python3
"""
Unified Retriever - Multi-source retrieval combining SQL and Semantic search.
Routes queries to appropriate sources and returns answers with citations.
"""

import json
import logging
import os
import re
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Generator, List, Dict, Optional, Tuple
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor
from azure.core.credentials import AzureKeyCredential
from azure.search.documents import SearchClient
from azure.search.documents.models import VectorizedQuery

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
FABRIC_KQL_DATABASE = os.getenv("FABRIC_KQL_DATABASE", "").strip()

AZURE_COSMOS_ENDPOINT = os.getenv("AZURE_COSMOS_ENDPOINT", "").strip()
AZURE_COSMOS_KEY = os.getenv("AZURE_COSMOS_KEY", "").strip()
AZURE_COSMOS_DATABASE = os.getenv("AZURE_COSMOS_DATABASE", "aviationrag").strip()
AZURE_COSMOS_CONTAINER = os.getenv("AZURE_COSMOS_CONTAINER", "notams").strip()


def _get_fabric_bearer_token() -> str:
    """Re-read bearer token from env on each call so rotated tokens take effect."""
    return os.getenv("FABRIC_BEARER_TOKEN", "")


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
        self._cosmos_container = None
        if _COSMOS_SDK_AVAILABLE and AZURE_COSMOS_ENDPOINT:
            try:
                if AZURE_COSMOS_KEY:
                    cosmos_client = CosmosClient(AZURE_COSMOS_ENDPOINT, credential=AZURE_COSMOS_KEY)
                else:
                    from azure.identity import DefaultAzureCredential
                    cosmos_client = CosmosClient(AZURE_COSMOS_ENDPOINT, credential=DefaultAzureCredential())
                cosmos_db = cosmos_client.get_database_client(AZURE_COSMOS_DATABASE)
                self._cosmos_container = cosmos_db.get_container_client(AZURE_COSMOS_CONTAINER)
                logger.info(
                    "Connected to Cosmos DB: %s/%s/%s",
                    AZURE_COSMOS_ENDPOINT, AZURE_COSMOS_DATABASE, AZURE_COSMOS_CONTAINER,
                )
            except Exception as exc:
                self._cosmos_container = None
                logger.warning("Cosmos DB unavailable (%s)", exc)
        elif not _COSMOS_SDK_AVAILABLE and AZURE_COSMOS_ENDPOINT:
            logger.warning("azure-cosmos SDK not installed; Cosmos NOSQL retrieval unavailable")

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

    def _now_iso(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def _post_json(self, endpoint: str, payload: Any) -> Any:
        body = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(endpoint, data=body, method="POST")
        req.add_header("Content-Type", "application/json")
        token = _get_fabric_bearer_token()
        if token:
            req.add_header("Authorization", f"Bearer {token}")
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
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

    def _query_tokens(self, query: str) -> List[str]:
        tokens = [t.upper() for t in re.findall(r"[A-Za-z0-9]{3,8}", query or "")]
        deduped: List[str] = []
        for token in tokens:
            if token not in deduped:
                deduped.append(token)
            if len(deduped) >= 8:
                break
        return deduped

    def _extract_airports_from_query(self, query: str) -> List[str]:
        text = query or ""
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

    def _kusto_rows(self, endpoint: str, csl: str) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        db_name = FABRIC_KQL_DATABASE or os.getenv("FABRIC_KQL_DATABASE_NAME", "").strip()
        if not db_name:
            return [], "missing_fabric_kql_database"

        response = self._post_json(endpoint, {"db": db_name, "csl": csl})
        if isinstance(response, dict) and response.get("error"):
            return [], str(response.get("error"))
        if not isinstance(response, list):
            return [], "unexpected_kusto_response_type"

        for frame in response:
            if not isinstance(frame, dict):
                continue
            if frame.get("FrameType") != "DataTable":
                continue
            if frame.get("TableKind") != "PrimaryResult":
                continue

            columns = [str(c.get("ColumnName", "")) for c in (frame.get("Columns") or []) if isinstance(c, dict)]
            rows: List[Dict[str, Any]] = []
            for row in frame.get("Rows") or []:
                if not isinstance(row, list):
                    continue
                rows.append(dict(zip(columns, row)))
            return rows, None
        return [], "kusto_primary_result_not_found"

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

    def source_mode(self, source: str) -> str:
        source_norm = (source or "").upper()
        if source_norm == "SQL":
            return "live" if self.sql_available else "blocked"
        if source_norm == "KQL":
            return "live" if FABRIC_KQL_ENDPOINT else "blocked"
        if source_norm == "GRAPH":
            if FABRIC_GRAPH_ENDPOINT:
                return "live"
            return "fallback" if self.sql_available else "blocked"
        if source_norm == "NOSQL":
            if self._cosmos_container is not None:
                return "live"
            return "live" if FABRIC_NOSQL_ENDPOINT else "blocked"
        if source_norm in {"VECTOR_OPS", "VECTOR_REG", "VECTOR_AIRPORT"}:
            return "live" if self.search_clients else "blocked"
        return "unknown"

    def source_event_meta(self, source: str) -> Dict[str, Any]:
        source_norm = (source or "").upper()
        store_type_map = {
            "KQL": "fabric-eventhouse",
            "GRAPH": "fabric-graph",
            "NOSQL": "cosmos-nosql" if self._cosmos_container is not None else "fabric-nosql",
            "SQL": "warehouse-sql",
            "VECTOR_OPS": "vector-ops",
            "VECTOR_REG": "vector-regulatory",
            "VECTOR_AIRPORT": "vector-airport",
        }
        freshness_map = {
            "KQL": "near-real-time",
            "GRAPH": "dependency-snapshot",
            "NOSQL": "ops-doc-snapshot",
            "SQL": "warehouse-snapshot",
            "VECTOR_OPS": "indexed-context",
            "VECTOR_REG": "indexed-context",
            "VECTOR_AIRPORT": "indexed-context",
        }
        return {
            "store_type": store_type_map.get(source_norm, "unknown"),
            "endpoint_label": self.source_mode(source_norm),
            "freshness": freshness_map.get(source_norm, "unknown"),
            "sql_backend": self.sql_backend if source_norm == "SQL" else "",
        }

    def _probe_endpoint(self, endpoint: str, timeout_seconds: int = 5) -> Dict[str, Any]:
        if not endpoint:
            return {"status": "warn", "detail": "not_configured"}

        req = urllib.request.Request(endpoint, method="GET")
        token = _get_fabric_bearer_token()
        if token:
            req.add_header("Authorization", f"Bearer {token}")
        try:
            with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:
                return {
                    "status": "pass",
                    "detail": f"reachable_http_{resp.status}",
                }
        except urllib.error.HTTPError as exc:
            # Treat auth and method errors as reachable endpoint.
            if exc.code in (400, 401, 403, 404, 405):
                return {"status": "warn", "detail": f"reachable_http_{exc.code}"}
            return {"status": "fail", "detail": f"http_{exc.code}"}
        except Exception as exc:
            return {"status": "fail", "detail": str(exc)}

    def fabric_preflight(self) -> Dict[str, Any]:
        checks: List[Dict[str, Any]] = []

        fabric_token = _get_fabric_bearer_token()
        token_status = "pass" if fabric_token else "warn"
        checks.append(
            {
                "name": "fabric_bearer_token",
                "status": token_status,
                "detail": "present" if fabric_token else "missing_optional_or_not_configured",
                "mode": "n/a",
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
            ("fabric_kql_endpoint", FABRIC_KQL_ENDPOINT or "", "KQL"),
            ("fabric_graph_endpoint", FABRIC_GRAPH_ENDPOINT or "", "GRAPH"),
            ("fabric_nosql_endpoint", FABRIC_NOSQL_ENDPOINT or "", "NOSQL"),
        ]

        live_configured = False
        for check_name, endpoint, source in endpoint_checks:
            mode = self.source_mode(source)
            if endpoint:
                live_configured = True
            probe = self._probe_endpoint(endpoint)
            checks.append(
                {
                    "name": check_name,
                    "status": probe["status"],
                    "detail": probe["detail"],
                    "mode": mode,
                    "endpoint": endpoint if endpoint else "",
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
        tables = {
            str(t.get("table", "")).lower(): {
                str(col.get("name", "")).lower()
                for col in (t.get("columns") or [])
                if isinstance(col, dict)
            }
            for t in schema.get("tables", [])
            if isinstance(t, dict)
        }
        asrs_cols = tables.get("asrs_reports", set())
        if not asrs_cols:
            return None

        q = (query or "").lower()
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
        if not self.sql_available:
            row = self._source_unavailable_row("SQL", self.sql_unavailable_reason or "sql_backend_not_available")
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
                    for row in results:
                        if isinstance(row, dict):
                            row["partial_schema"] = sql
                            row["fallback_sql"] = fallback_sql
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
        index_name = self.vector_source_to_index.get(source) or self.vector_source_to_index.get("VECTOR_OPS", "idx_ops_narratives")
        client = self.search_clients.get(index_name)
        if client is None:
            return [self._source_unavailable_row(source, f"search_index_unavailable:{index_name}")], []

        top = max(1, int(top))
        top_raw = max(top, _RERANK_RAW_CANDIDATES if _RERANK_ENABLED else top)

        if embedding is None:
            embedding = self.get_embedding(query)

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

    def _validate_kql_query(self, csl: str) -> Optional[str]:
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
        # After stripping let bindings, block Kusto management commands (dot-commands)
        # that could leak info or mutate state (e.g. `.show commands`, `.set-or-replace`).
        if re.search(r'\.\s*(show|set|append|move|rename|replace|enable|disable)\b', stripped, flags=re.IGNORECASE):
            return "kql_contains_blocked_management_command"
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

    def query_kql(self, query: str, window_minutes: int = 60) -> Tuple[List[Dict], List[Citation]]:
        """Retrieve event-window signals from Eventhouse (live only)."""
        if not FABRIC_KQL_ENDPOINT:
            return [self._source_unavailable_row("KQL", "FABRIC_KQL_ENDPOINT not configured")], []

        if self._is_kusto_endpoint(FABRIC_KQL_ENDPOINT):
            if self._looks_like_kql_text(query):
                csl = self._ensure_kql_window(query, window_minutes)
            else:
                airports = self._extract_airports_from_query(query)
                query_lower = (query or "").lower()
                is_weather_query = any(w in query_lower for w in ("weather", "hazard", "sigmet", "airmet", "turbulence", "icing", "storm"))
                if airports and is_weather_query:
                    # Query hazards_airsigmets for weather-related airport queries.
                    # Note: hazards_airsigmets has points (lat/lon polygon) not station IDs,
                    # so we search raw_text for the airport identifiers.
                    values = ",".join(f"'{a}'" for a in airports)
                    csl = (
                        "hazards_airsigmets "
                        f"| where raw_text has_any ({values}) "
                        f"| where valid_time_to > ago({max(1, int(window_minutes))}m) "
                        "| top 40 by valid_time_from desc"
                    )
                elif airports:
                    # Query opensky_states for flight tracking near airports.
                    values = ",".join(f"'{a}'" for a in airports)
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

            validation_error = self._validate_kql_query(csl)
            if validation_error:
                return [
                    self._source_error_row(
                        source="KQL",
                        code="kql_validation_failed",
                        detail=validation_error,
                        extra={"csl": csl},
                    )
                ], []

            rows, error = self._kusto_rows(FABRIC_KQL_ENDPOINT, csl)
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
            response = self._post_json(FABRIC_KQL_ENDPOINT, payload)
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

    def _query_graph_pg_fallback(self, query: str, hops: int = 2) -> Tuple[List[Dict], List[Citation]]:
        """Fallback: query ops_graph_edges from PostgreSQL with iterative BFS multi-hop traversal."""
        if not self.sql_available:
            return [self._source_unavailable_row("GRAPH", "FABRIC_GRAPH_ENDPOINT not configured and SQL unavailable")], []

        # Verify ops_graph_edges table exists in the current schema.
        schema = self.cached_sql_schema()
        table_names = {str(t.get("table", "")).lower() for t in schema.get("tables", []) if isinstance(t, dict)}
        if "ops_graph_edges" not in table_names:
            return [self._source_unavailable_row("GRAPH", "FABRIC_GRAPH_ENDPOINT not configured and ops_graph_edges table not found in PostgreSQL")], []

        tokens = self._query_tokens(query)
        if not tokens:
            sql = "SELECT src_type, src_id, edge_type, dst_type, dst_id FROM ops_graph_edges LIMIT 30"
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
                f"WHERE UPPER(src_id) IN ({placeholders}) OR UPPER(dst_id) IN ({placeholders}) "
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
        if not FABRIC_GRAPH_ENDPOINT:
            return self._query_graph_pg_fallback(query, hops=hops)

        if self._is_kusto_endpoint(FABRIC_GRAPH_ENDPOINT):
            tokens = self._query_tokens(query)
            if tokens:
                values = ",".join(f"'{t}'" for t in tokens)
                csl = f"ops_graph_edges | where src_id in~ ({values}) or dst_id in~ ({values}) | take 50"
            else:
                csl = "ops_graph_edges | take 30"

            paths, _error = self._kusto_rows(FABRIC_GRAPH_ENDPOINT, csl)
            if paths:
                citation = Citation(
                    source_type="GRAPH",
                    identifier="fabric_graph_live",
                    title="Fabric graph traversal",
                    content_preview=str(paths)[:120],
                    score=1.0,
                    dataset="fabric-graph",
                )
                return paths, [citation]
            return [self._source_error_row("GRAPH", "graph_runtime_error", _error or "graph_query_returned_no_rows", {"csl": csl})], []
        else:
            payload = {"query": query, "hops": hops}
            response = self._post_json(FABRIC_GRAPH_ENDPOINT, payload)
            if isinstance(response, dict) and "error" not in response:
                paths = response.get("paths", [])
                if paths:
                    citation = Citation(
                        source_type="GRAPH",
                        identifier="fabric_graph_live",
                        title="Fabric graph traversal",
                        content_preview=str(paths)[:120],
                        score=1.0,
                        dataset="fabric-graph",
                    )
                    return paths, [citation]
            detail = str(response.get("error")) if isinstance(response, dict) else "graph_endpoint_query_failed"
            return [self._source_error_row("GRAPH", "graph_runtime_error", detail)], []

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
        # Path 1: Cosmos DB native SDK
        if self._cosmos_container is not None:
            return self._query_cosmos_notams(query)

        # Path 2: Fabric REST / Kusto endpoint (backward compat)
        if not FABRIC_NOSQL_ENDPOINT:
            return [self._source_unavailable_row("NOSQL", "NOSQL source not configured (no Cosmos DB or FABRIC_NOSQL_ENDPOINT)")], []

        if self._is_kusto_endpoint(FABRIC_NOSQL_ENDPOINT):
            docs, _error = self._kusto_rows(FABRIC_NOSQL_ENDPOINT, "hazards_airsigmets | take 30")
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
        response = self._post_json(FABRIC_NOSQL_ENDPOINT, payload)
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
        query_embedding = self.get_embedding(query)

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
        source_mode = self.source_mode(source)
        if source_mode == "blocked":
            row = self._source_unavailable_row(source, f"{source} is blocked by current source policy/configuration")
            return [row], [], None
        if source == "SQL":
            rows, sql, citations = self.query_sql(query, cfg.get("sql_hint"))
            return rows, citations, sql
        if source == "KQL":
            rows, citations = self.query_kql(query, window_minutes=int(cfg.get("window_minutes", 60)))
            return rows, citations, None
        if source == "GRAPH":
            rows, citations = self.query_graph(query, hops=int(cfg.get("hops", 2)))
            return rows, citations, None
        if source == "NOSQL":
            rows, citations = self.query_nosql(query)
            return rows, citations, None
        if source in ("VECTOR_OPS", "VECTOR_REG", "VECTOR_AIRPORT"):
            rows, citations = self.query_semantic(
                query,
                top=int(cfg.get("top", 5)),
                embedding=cfg.get("embedding"),
                source=source,
                filter_expression=cfg.get("filter"),
            )
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
            for chunk in stream:
                if chunk.choices and chunk.choices[0].delta and chunk.choices[0].delta.content:
                    yield {"type": "agent_update", "content": chunk.choices[0].delta.content}
            logger.info("perf stage=%s ms=%.1f", "synthesize_answer_stream", (time.perf_counter() - _t0_synth) * 1000)
        except Exception as exc:
            logger.error("LLM streaming synthesis failed: %s — falling back to non-streaming", exc)
            answer = self._synthesize_answer(query, context, route, conversation_history=conversation_history)
            yield {"type": "agent_update", "content": answer}


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
