#!/usr/bin/env python3
"""
Unified Retriever - Multi-source retrieval combining SQL and Semantic search.
Routes queries to appropriate sources and returns answers with citations.
"""

import csv
import gzip
import json
import os
import re
import sqlite3
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, List, Dict, Optional, Tuple
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv
from openai import AzureOpenAI
from azure.identity import DefaultAzureCredential, get_bearer_token_provider
from azure.core.credentials import AzureKeyCredential
from azure.search.documents import SearchClient
from azure.search.documents.models import VectorizedQuery

from query_router import QueryRouter
from sql_generator import SQLGenerator
from pii_filter import PiiFilter, PiiCheckResult

# Load .env file
load_dotenv()

# Database configuration - support both SQLite (local) and PostgreSQL (production)
_USE_POSTGRES_RAW = os.getenv("USE_POSTGRES", "").strip().lower()
if _USE_POSTGRES_RAW:
    USE_POSTGRES = _USE_POSTGRES_RAW in ("true", "1", "yes")
else:
    USE_POSTGRES = bool(os.getenv("PGHOST"))
DB_PATH = Path(os.getenv("SQLITE_PATH", "aviation.db"))

# Azure OpenAI configuration
OPENAI_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT")
OPENAI_KEY = os.getenv("AZURE_OPENAI_API_KEY")
LLM_DEPLOYMENT = os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME", "aviation-chat-gpt5-mini")
EMBEDDING_DEPLOYMENT = os.getenv("AZURE_TEXT_EMBEDDING_DEPLOYMENT_NAME", "text-embedding-3-small")

# Azure AI Search configuration
SEARCH_ENDPOINT = os.getenv("AZURE_SEARCH_ENDPOINT")
SEARCH_KEY = os.getenv("AZURE_SEARCH_ADMIN_KEY")
SEARCH_INDEX_OPS = os.getenv("AZURE_SEARCH_INDEX_OPS_NAME", "idx_ops_narratives")
SEARCH_INDEX_REGULATORY = os.getenv("AZURE_SEARCH_INDEX_REGULATORY_NAME", "idx_regulatory")
SEARCH_INDEX_AIRPORT = os.getenv("AZURE_SEARCH_INDEX_AIRPORT_NAME", "idx_airport_ops_docs")
SEARCH_SEMANTIC_CONFIG = os.getenv("AZURE_SEARCH_SEMANTIC_CONFIG_NAME", "aviation-semantic-config")
try:
    _RERANK_RAW_CANDIDATES = max(1, int(os.getenv("CONTEXT_VECTOR_RAW_CANDIDATES", "20") or "20"))
except Exception:
    _RERANK_RAW_CANDIDATES = 20
_RERANK_ENABLED = os.getenv("CONTEXT_RERANK_ENABLED", "true").strip().lower() in {"1", "true", "yes", "y", "on"}

FABRIC_KQL_ENDPOINT = os.getenv("FABRIC_KQL_ENDPOINT")
FABRIC_GRAPH_ENDPOINT = os.getenv("FABRIC_GRAPH_ENDPOINT")
FABRIC_NOSQL_ENDPOINT = os.getenv("FABRIC_NOSQL_ENDPOINT")
FABRIC_BEARER_TOKEN = os.getenv("FABRIC_BEARER_TOKEN", "")
FABRIC_KQL_DATABASE = os.getenv("FABRIC_KQL_DATABASE", "").strip()
ROOT = Path(__file__).resolve().parents[1]


def _client_tuning_kwargs() -> dict:
    try:
        timeout_seconds = float(os.getenv("AZURE_OPENAI_TIMEOUT_SECONDS", "45"))
    except Exception:
        timeout_seconds = 45.0
    try:
        max_retries = max(0, int(os.getenv("AZURE_OPENAI_MAX_RETRIES", "1")))
    except Exception:
        max_retries = 1
    return {"timeout": timeout_seconds, "max_retries": max_retries}


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
        if OPENAI_KEY:
            self.llm = AzureOpenAI(
                azure_endpoint=OPENAI_ENDPOINT,
                api_key=OPENAI_KEY,
                api_version="2024-06-01",
                **_client_tuning_kwargs(),
            )
        else:
            credential = DefaultAzureCredential()
            token_provider = get_bearer_token_provider(
                credential, "https://cognitiveservices.azure.com/.default"
            )
            self.llm = AzureOpenAI(
                azure_endpoint=OPENAI_ENDPOINT,
                azure_ad_token_provider=token_provider,
                api_version="2024-06-01",
                **_client_tuning_kwargs(),
            )

        # Search clients (multi-index)
        self.search_clients: Dict[str, SearchClient] = {}
        self.vector_source_to_index = {
            "VECTOR_OPS": SEARCH_INDEX_OPS,
            "VECTOR_REG": SEARCH_INDEX_REGULATORY,
            "VECTOR_AIRPORT": SEARCH_INDEX_AIRPORT,
        }
        if SEARCH_ENDPOINT and SEARCH_KEY:
            search_credential = AzureKeyCredential(SEARCH_KEY)
            for index_name in sorted(set(self.vector_source_to_index.values())):
                self.search_clients[index_name] = SearchClient(
                    endpoint=SEARCH_ENDPOINT,
                    index_name=index_name,
                    credential=search_credential,
                )
        else:
            print("Warning: Azure AI Search is not configured; semantic retrieval will be unavailable.")

        # Database connection - SQLite or PostgreSQL
        self.use_postgres = USE_POSTGRES
        if self.use_postgres:
            try:
                import psycopg2
                self.db = psycopg2.connect(
                    host=os.getenv("PGHOST"),
                    port=int(os.getenv("PGPORT", 5432)),
                    database=os.getenv("PGDATABASE", "aviationdb"),
                    user=os.getenv("PGUSER"),
                    password=os.getenv("PGPASSWORD"),
                    sslmode="require",
                    connect_timeout=5,
                )
                self.db.autocommit = True
                print(f"Connected to PostgreSQL: {os.getenv('PGHOST')}/{os.getenv('PGDATABASE', 'aviationdb')}")
            except Exception as exc:
                self.use_postgres = False
                self.db = sqlite3.connect(str(DB_PATH), check_same_thread=False)
                self.db.row_factory = sqlite3.Row
                print(f"Warning: PostgreSQL unavailable ({exc}); falling back to SQLite: {DB_PATH}")
        else:
            self.db = sqlite3.connect(str(DB_PATH), check_same_thread=False)
            self.db.row_factory = sqlite3.Row
            print(f"Connected to SQLite: {DB_PATH}")

        # Specialized components
        self.router = QueryRouter()
        self.sql_generator = SQLGenerator()

        # PII filter
        self.enable_pii_filter = enable_pii_filter
        if enable_pii_filter:
            self.pii_filter = PiiFilter()
            if self.pii_filter.is_available():
                print("PII filter enabled and available")
            else:
                print("Warning: PII filter enabled but service unavailable")
        else:
            self.pii_filter = None
            print("Warning: PII filter disabled")

    def get_embedding(self, text: str) -> List[float]:
        """Get embedding from Azure OpenAI."""
        response = self.llm.embeddings.create(
            model=EMBEDDING_DEPLOYMENT,
            input=text[:8000]
        )
        return response.data[0].embedding

    def _now_iso(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def _latest_matching(self, pattern: str) -> Optional[Path]:
        matches = sorted(ROOT.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)
        return matches[0] if matches else None

    def _post_json(self, endpoint: str, payload: Any) -> Any:
        body = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(endpoint, data=body, method="POST")
        req.add_header("Content-Type", "application/json")
        if FABRIC_BEARER_TOKEN:
            req.add_header("Authorization", f"Bearer {FABRIC_BEARER_TOKEN}")
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

    def source_mode(self, source: str) -> str:
        source_norm = (source or "").upper()
        if source_norm == "KQL":
            return "live" if FABRIC_KQL_ENDPOINT else "fallback"
        if source_norm == "GRAPH":
            return "live" if FABRIC_GRAPH_ENDPOINT else "fallback"
        if source_norm == "NOSQL":
            return "live" if FABRIC_NOSQL_ENDPOINT else "fallback"
        if source_norm == "SQL":
            return "live" if self.use_postgres else "fallback"
        if source_norm in {"VECTOR_OPS", "VECTOR_REG", "VECTOR_AIRPORT"}:
            return "live" if self.search_clients else "fallback"
        return "unknown"

    def source_event_meta(self, source: str) -> Dict[str, Any]:
        source_norm = (source or "").upper()
        store_type_map = {
            "KQL": "fabric-eventhouse",
            "GRAPH": "fabric-graph",
            "NOSQL": "fabric-nosql",
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
        }

    def _probe_endpoint(self, endpoint: str, timeout_seconds: int = 5) -> Dict[str, Any]:
        if not endpoint:
            return {"status": "warn", "detail": "not_configured"}

        req = urllib.request.Request(endpoint, method="GET")
        if FABRIC_BEARER_TOKEN:
            req.add_header("Authorization", f"Bearer {FABRIC_BEARER_TOKEN}")
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

        token_status = "pass" if FABRIC_BEARER_TOKEN else "warn"
        checks.append(
            {
                "name": "fabric_bearer_token",
                "status": token_status,
                "detail": "present" if FABRIC_BEARER_TOKEN else "missing_optional_or_not_configured",
                "mode": "n/a",
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

        fallback_ready = bool(
            self._latest_matching("data/e-opensky_recent/opensky_states_all_*.json")
            or self._latest_matching("data/a-metars.cache.csv.gz")
            or self._latest_matching("data/j-synthetic_ops_overlay/*/synthetic/ops_graph_edges.csv")
        )
        checks.append(
            {
                "name": "local_fallback_datasets",
                "status": "pass" if fallback_ready else "warn",
                "detail": "ready" if fallback_ready else "not_found",
                "mode": "fallback",
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

    # =========================================================================
    # Core Retrieval Methods
    # =========================================================================

    def query_sql(self, query: str, sql_hint: str = None) -> Tuple[List[Dict], str, List[Citation]]:
        """Execute SQL query against the aviation database."""
        if sql_hint:
            enhanced_query = f"{query}\nHint: {sql_hint}"
        else:
            enhanced_query = query

        sql = self.sql_generator.generate(enhanced_query)

        # [TBD: Add PostgreSQL schema prefix if needed]

        citations = []
        try:
            cur = self.db.cursor()
            cur.execute(sql)
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            results = [dict(zip(columns, row)) for row in rows]

            for i, row in enumerate(results[:10]):
                record_id = row.get("asrs_report_id", row.get("id", row.get("record_id", f"row_{i}")))
                title = row.get("title", f"ASRS {record_id}")
                citations.append(Citation(
                    source_type="SQL",
                    identifier=str(record_id),
                    title=str(title),
                    content_preview=str(row)[:100],
                    dataset="aviation_db",
                ))

            return results, sql, citations

        except Exception as e:
            return [{"error": str(e), "sql": sql}], sql, []

    def query_semantic(
        self,
        query: str,
        top: int = 5,
        embedding: Optional[List[float]] = None,
        source: str = "VECTOR_OPS",
        filter_expression: Optional[str] = None,
    ) -> Tuple[List[Dict], List[Citation]]:
        """Search a specific semantic index using hybrid/vector retrieval."""
        index_name = self.vector_source_to_index.get(source, SEARCH_INDEX_OPS)
        client = self.search_clients.get(index_name)
        if client is None:
            return [{"error": f"search_index_unavailable:{index_name}"}], []

        top = max(1, int(top))
        top_raw = max(top, _RERANK_RAW_CANDIDATES if _RERANK_ENABLED else top)

        if embedding is None:
            embedding = self.get_embedding(query)

        vector_query = VectorizedQuery(
            vector=embedding,
            k_nearest_neighbors=top_raw,
            fields="content_vector",
        )

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
                    return [{"error": str(fallback_exc), "index": index_name}], []
            else:
                return [{"error": str(exc), "index": index_name}], []

        results_list: List[Dict[str, Any]] = []
        citations: List[Citation] = []

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

        results_list.sort(key=lambda row: float(row.get("__vector_score_final", 0.0) or 0.0), reverse=True)
        citations.sort(key=lambda c: c.score, reverse=True)
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

    def query_kql(self, query: str, window_minutes: int = 60) -> Tuple[List[Dict], List[Citation]]:
        """Retrieve event-window signals from Eventhouse or local fallback."""
        if FABRIC_KQL_ENDPOINT:
            if self._is_kusto_endpoint(FABRIC_KQL_ENDPOINT):
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

                rows, _error = self._kusto_rows(FABRIC_KQL_ENDPOINT, csl)
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

        # Local deterministic fallback for demo readiness.
        rows: List[Dict[str, Any]] = []
        states_file = self._latest_matching("data/e-opensky_recent/opensky_states_all_*.json")
        if states_file:
            try:
                payload = json.loads(states_file.read_text(encoding="utf-8"))
                states_count = len(payload.get("states", []) or [])
                rows.append(
                    {
                        "metric": "opensky_states_count",
                        "value": states_count,
                        "window_minutes": window_minutes,
                        "source_file": str(states_file),
                    }
                )
            except Exception as exc:
                rows.append({"metric": "opensky_states_count", "error": str(exc)})

        metars_file = self._latest_matching("data/a-metars.cache.csv.gz")
        if metars_file:
            obs_count = 0
            try:
                with gzip.open(metars_file, "rt", encoding="utf-8", errors="ignore") as f:
                    # Ignore header, count sample quickly.
                    for idx, _line in enumerate(f):
                        if idx == 0:
                            continue
                        obs_count += 1
                        if obs_count >= 5000:
                            break
                rows.append(
                    {
                        "metric": "metar_observation_count_sample",
                        "value": obs_count,
                        "source_file": str(metars_file),
                    }
                )
            except Exception as exc:
                rows.append({"metric": "metar_observation_count_sample", "error": str(exc)})

        citation = Citation(
            source_type="KQL",
            identifier="eventhouse_mock",
            title="Eventhouse fallback snapshot",
            content_preview=str(rows)[:120],
            score=0.8,
            dataset="eventhouse-mock",
        )
        return rows, [citation]

    def query_graph(self, query: str, hops: int = 2) -> Tuple[List[Dict], List[Citation]]:
        """Retrieve graph relationships from Fabric graph endpoint or local overlay graph."""
        if FABRIC_GRAPH_ENDPOINT:
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

        graph_file = self._latest_matching("data/j-synthetic_ops_overlay/*/synthetic/ops_graph_edges.csv")
        if not graph_file:
            return [{"error": "graph_edges_unavailable"}], []

        paths: List[Dict[str, Any]] = []
        tokens = {t.upper() for t in re.findall(r"[A-Za-z0-9]{3,6}", query)}
        with graph_file.open("r", encoding="utf-8", errors="ignore", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                src_id = str(row.get("src_id", "")).upper()
                dst_id = str(row.get("dst_id", "")).upper()
                if tokens and src_id not in tokens and dst_id not in tokens:
                    continue
                paths.append(row)
                if len(paths) >= 30:
                    break

        if not paths:
            with graph_file.open("r", encoding="utf-8", errors="ignore", newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    paths.append(row)
                    if len(paths) >= 10:
                        break

        citation = Citation(
            source_type="GRAPH",
            identifier="overlay_graph",
            title="Graph dependency paths",
            content_preview=str(paths[:3])[:120],
            score=0.85,
            dataset="fabric-graph-preview-mock",
        )
        return paths, [citation]

    def query_nosql(self, query: str) -> Tuple[List[Dict], List[Citation]]:
        """Retrieve NoSQL-style records from endpoint or local NOTAM snapshots."""
        if FABRIC_NOSQL_ENDPOINT:
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
            else:
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

        notam_file = self._latest_matching("data/h-notam_recent/*/search_location_istanbul.jsonl")
        docs: List[Dict[str, Any]] = []
        if notam_file:
            with notam_file.open("r", encoding="utf-8", errors="ignore") as f:
                for idx, line in enumerate(f):
                    if idx >= 30:
                        break
                    try:
                        obj = json.loads(line)
                    except Exception:
                        continue
                    docs.append(
                        {
                            "facilityDesignator": obj.get("facilityDesignator"),
                            "notamNumber": obj.get("notamNumber"),
                            "startDate": obj.get("startDate"),
                            "endDate": obj.get("endDate"),
                        }
                    )
        citation = Citation(
            source_type="NOSQL",
            identifier="notam_snapshot",
            title="NoSQL fallback documents",
            content_preview=str(docs[:3])[:120],
            score=0.75,
            dataset="nosql-mock",
        )
        return docs, [citation]

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
        """Execute semantic-only retrieval."""
        results, citations = self.query_semantic(query)

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
                print(f"SQL query error in parallel execution: {e}")

            try:
                semantic_results, semantic_citations = semantic_future.result(timeout=30)
            except Exception as e:
                print(f"Semantic query error in parallel execution: {e}")

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
            if pii_result.has_pii:
                warning = self.pii_filter.format_warning(pii_result.entities)
                print(f"\n{'='*60}")
                print(f"PII DETECTED - Query blocked")
                print(f"Categories: {[e.category for e in pii_result.entities]}")
                print(f"{'='*60}")
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

        print(f"\n{'='*60}")
        print(f"Query: {query}")
        print(f"Route: {route} - {route_result.get('reasoning', '')}")
        print(f"{'='*60}")

        # Execute appropriate route
        if route == "SQL":
            result = self.execute_sql_route(query, sql_hint)
        elif route == "SEMANTIC":
            result = self.execute_semantic_route(query)
        else:  # HYBRID
            result = self.execute_hybrid_route(query, sql_hint)

        result.reasoning = route_result.get("reasoning", result.reasoning)
        return result

    def _synthesize_answer(self, query: str, context: dict, route: str) -> str:
        """Generate natural language answer from retrieved context."""

        route_instructions = {
            "SQL": "Focus on the precise data from SQL results.",
            "SEMANTIC": "Focus on document content and similarity matches.",
            "HYBRID": "Combine structured data with semantic context for a comprehensive answer.",
            "AGENTIC": "Prioritize evidence reconciliation across multiple source types.",
        }

        system_prompt = f"""You are a helpful aviation data analyst assistant.
Answer the user's question based on the provided context.
{route_instructions.get(route, '')}

Guidelines:
- Be concise but informative
- Format numbers nicely
- If showing multiple items, use a clear list or table format
- Reference the data sources when relevant
- If the context is insufficient, say so clearly"""

        context_str = f"""
Query: {query}

Retrieved Data:
{context}
"""

        try:
            response = self.llm.chat.completions.create(
                model=LLM_DEPLOYMENT,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": context_str}
                ]
            )
            return response.choices[0].message.content
        except Exception as exc:
            return f"Unable to synthesize with model right now ({exc}). Retrieved context: {str(context)[:1200]}"


# =============================================================================
# Convenience Functions
# =============================================================================

def answer_question(query: str, use_llm_routing: bool = True) -> dict:
    """Simple function to get an answer with citations."""
    retriever = UnifiedRetriever()
    result = retriever.answer(query, use_llm_routing)
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
