#!/usr/bin/env python3
"""
Unified Retriever - Multi-source retrieval combining SQL and Semantic search.
Routes queries to appropriate sources and returns answers with citations.
"""

import csv
import gzip
import io
import json
import os
import re
import sqlite3
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, List, Dict, Optional, Tuple
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv
from azure.core.credentials import AzureKeyCredential
from azure.search.documents import SearchClient
from azure.search.documents.models import VectorizedQuery

from azure_openai_client import init_azure_openai_client
from query_router import QueryRouter
from query_writers import SQLWriter
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
OPENAI_API_VERSION = os.getenv("AZURE_OPENAI_API_VERSION", "2024-06-01")
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


def _resolve_project_root() -> Path:
    module_dir = Path(__file__).resolve().parent
    candidates = [module_dir, module_dir.parent]
    marker_paths = [
        Path("data/b-runways.csv"),
        Path("data/a-metars.cache.csv.gz"),
        Path("data/a-tafs.cache.xml.gz"),
        Path("data/h-notam_recent"),
    ]
    for candidate in candidates:
        if any((candidate / marker).exists() for marker in marker_paths):
            return candidate
    for candidate in candidates:
        if (candidate / "contracts").exists():
            return candidate
    return module_dir


ROOT = _resolve_project_root()

CITY_AIRPORT_MAP: Dict[str, List[str]] = {
    "new york": ["KJFK", "KLGA", "KEWR"],
    "nyc": ["KJFK", "KLGA", "KEWR"],
    "istanbul": ["LTFM", "LTBA", "LTFJ"],
    "london": ["EGLL", "EGKK", "EGSS"],
}

IATA_TO_ICAO_MAP: Dict[str, str] = {
    "JFK": "KJFK",
    "LGA": "KLGA",
    "EWR": "KEWR",
    "IST": "LTFM",
    "SAW": "LTFJ",
}


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    return raw.lower() in {"1", "true", "yes", "y", "on"}


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
        self.llm, self.llm_auth_mode = init_azure_openai_client(api_version=OPENAI_API_VERSION)
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
            print("Warning: Azure AI Search is not configured; semantic retrieval will be unavailable.")

        # Source execution policy and dialect controls.
        self.strict_source_mode = _env_bool("RETRIEVAL_STRICT_SOURCE_MODE", False)
        self.allow_sqlite_fallback = _env_bool("ALLOW_SQLITE_FALLBACK", not self.strict_source_mode)
        self.allow_mock_kql_fallback = _env_bool("ALLOW_MOCK_KQL_FALLBACK", not self.strict_source_mode)
        self.allow_mock_graph_fallback = _env_bool("ALLOW_MOCK_GRAPH_FALLBACK", not self.strict_source_mode)
        self.allow_mock_nosql_fallback = _env_bool("ALLOW_MOCK_NOSQL_FALLBACK", not self.strict_source_mode)
        self.allow_legacy_sql_fallback = _env_bool("ALLOW_LEGACY_SQL_FALLBACK", not self.strict_source_mode)

        # Database connection - SQLite or PostgreSQL.
        self.use_postgres = USE_POSTGRES
        self.db = None
        self.sql_backend = "unavailable"
        self.sql_available = False
        self.sql_unavailable_reason = ""
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
                self.sql_backend = "postgres"
                self.sql_available = True
                print(f"Connected to PostgreSQL: {os.getenv('PGHOST')}/{os.getenv('PGDATABASE', 'aviationdb')}")
            except Exception as exc:
                self.sql_unavailable_reason = str(exc)
                if self.allow_sqlite_fallback:
                    self.use_postgres = False
                    self.db = sqlite3.connect(str(DB_PATH), check_same_thread=False)
                    self.db.row_factory = sqlite3.Row
                    self.sql_backend = "sqlite-fallback"
                    self.sql_available = True
                    print(f"Warning: PostgreSQL unavailable ({exc}); falling back to SQLite: {DB_PATH}")
                else:
                    self.db = None
                    self.sql_backend = "unavailable"
                    self.sql_available = False
                    print(f"Warning: PostgreSQL unavailable and fallback disabled ({exc})")
        else:
            self.db = sqlite3.connect(str(DB_PATH), check_same_thread=False)
            self.db.row_factory = sqlite3.Row
            self.sql_backend = "sqlite"
            self.sql_available = True
            print(f"Connected to SQLite: {DB_PATH}")

        dialect_default = "postgres" if self.use_postgres else "sqlite"
        self.sql_dialect = (os.getenv("SQL_DIALECT", dialect_default) or dialect_default).strip().lower()

        # Specialized components
        self.router = QueryRouter()
        self.sql_generator = SQLGenerator()
        self.sql_writer = SQLWriter(
            model=os.getenv("AZURE_OPENAI_WORKER_DEPLOYMENT_NAME") or self.llm_deployment
        )
        self.use_legacy_sql_generator = _env_bool("USE_LEGACY_SQL_GENERATOR", False)
        self._vector_k_param = self._detect_vector_k_param()

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
        """Get embedding from Azure OpenAI."""
        response = self.llm.embeddings.create(
            model=self.embedding_deployment,
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

    def _extract_airports_from_query(self, query: str) -> List[str]:
        text = query or ""
        upper = text.upper()
        lower = text.lower()
        out: List[str] = []

        # ICAO codes in free text (case-sensitive to avoid matching regular words).
        for match in re.findall(r"\b[A-Z]{4}\b", text):
            if match not in out:
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

    def _latest_runways_file(self) -> Optional[Path]:
        return (
            self._latest_matching("data/g-ourairports_recent/runways_*.csv")
            or self._latest_matching("data/b-runways.csv")
        )

    def _query_runway_constraints_fallback(self, query: str, airports: Optional[List[str]] = None) -> Tuple[List[Dict[str, Any]], List[Citation]]:
        runways_file = self._latest_runways_file()
        if not runways_file:
            return (
                [self._source_unavailable_row("SQL", "runway_constraints_fallback_dataset_missing")],
                [],
            )

        airport_list = [a.upper() for a in (airports or self._extract_airports_from_query(query)) if a]
        if not airport_list:
            airport_list = ["KJFK", "KLGA", "KEWR"]
        airport_set = set(airport_list)

        rows: List[Dict[str, Any]] = []
        with runways_file.open("r", encoding="utf-8", errors="ignore", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                airport = str(row.get("airport_ident") or "").upper().strip()
                if airport not in airport_set:
                    continue
                le_ident = str(row.get("le_ident") or "").strip()
                he_ident = str(row.get("he_ident") or "").strip()
                runway_id = "/".join([x for x in [le_ident, he_ident] if x]) or str(row.get("id") or "unknown")
                rows.append(
                    {
                        "airport": airport,
                        "runway_id": runway_id,
                        "constraint_type": "runway_profile",
                        "effective_from": None,
                        "effective_to": None,
                        "length_ft": int(float(row.get("length_ft") or 0)) if row.get("length_ft") else None,
                        "width_ft": int(float(row.get("width_ft") or 0)) if row.get("width_ft") else None,
                        "surface": row.get("surface"),
                        "lighted": row.get("lighted"),
                        "source_file": str(runways_file),
                    }
                )
                if len(rows) >= 60:
                    break

        if not rows:
            return (
                [self._source_error_row("SQL", "runway_constraints_unavailable", f"no runway rows matched {airport_list}")],
                [],
            )

        citations = [
            Citation(
                source_type="SQL",
                identifier=f"runway_{idx}",
                title=f"{row.get('airport')} {row.get('runway_id')}",
                content_preview=str(row)[:120],
                score=0.85,
                dataset="ourairports-runways-fallback",
            )
            for idx, row in enumerate(rows[:10], start=1)
        ]
        return rows, citations

    def _load_gzip_text(self, local_file: Optional[Path], remote_url: str) -> Optional[str]:
        try:
            if local_file and local_file.exists():
                with gzip.open(local_file, "rt", encoding="utf-8", errors="ignore") as f:
                    return f.read()
            with urllib.request.urlopen(remote_url, timeout=20) as resp:
                payload = resp.read()
            return gzip.decompress(payload).decode("utf-8", errors="ignore")
        except Exception:
            return None

    def _metar_rows_for_airports(self, airports: List[str], max_rows: int = 12) -> List[Dict[str, Any]]:
        metars_file = self._latest_matching("data/a-metars.cache.csv.gz")
        text = self._load_gzip_text(
            metars_file,
            "https://aviationweather.gov/data/cache/metars.cache.csv.gz",
        )
        if not text:
            return []

        latest_by_station: Dict[str, Dict[str, Any]] = {}
        reader = csv.DictReader(io.StringIO(text))
        airport_set = {a.upper() for a in airports}
        for row in reader:
            station = str(row.get("station_id") or "").upper().strip()
            if station not in airport_set:
                continue
            existing = latest_by_station.get(station)
            current_ts = str(row.get("observation_time") or "")
            existing_ts = str(existing.get("observation_time") or "") if existing else ""
            if not existing or current_ts > existing_ts:
                latest_by_station[station] = row

        out: List[Dict[str, Any]] = []
        for station in airports:
            row = latest_by_station.get(station.upper())
            if not row:
                continue
            out.append(
                {
                    "station_id": row.get("station_id"),
                    "observation_time": row.get("observation_time"),
                    "raw_text": row.get("raw_text"),
                    "flight_category": row.get("flight_category"),
                    "wind_speed_kt": row.get("wind_speed_kt"),
                    "wind_gust_kt": row.get("wind_gust_kt"),
                    "visibility_statute_mi": row.get("visibility_statute_mi"),
                    "source_file": str(metars_file) if metars_file else "aviationweather_cache_feed",
                }
            )
            if len(out) >= max_rows:
                break
        return out

    def _taf_rows_for_airports(self, airports: List[str], max_rows: int = 12) -> List[Dict[str, Any]]:
        tafs_file = self._latest_matching("data/a-tafs.cache.xml.gz")
        text = self._load_gzip_text(
            tafs_file,
            "https://aviationweather.gov/data/cache/tafs.cache.xml.gz",
        )
        if not text:
            return []

        try:
            root = ET.fromstring(text)
        except Exception:
            return []

        airport_set = {a.upper() for a in airports}
        out: List[Dict[str, Any]] = []
        for taf in root.findall(".//TAF"):
            station = (taf.findtext("station_id") or "").upper().strip()
            if station not in airport_set:
                continue
            out.append(
                {
                    "station_id": station,
                    "issue_time": taf.findtext("issue_time"),
                    "valid_time_from": taf.findtext("valid_time_from"),
                    "valid_time_to": taf.findtext("valid_time_to"),
                    "raw_text": (taf.findtext("raw_text") or "").strip(),
                    "source_file": str(tafs_file) if tafs_file else "aviationweather_cache_feed",
                }
            )
            if len(out) >= max_rows:
                break
        return out

    def _notam_docs_for_airports(self, query: str, max_rows: int = 30) -> List[Dict[str, Any]]:
        airports = self._extract_airports_from_query(query)
        facility_targets = {a[1:] if a.startswith("K") and len(a) == 4 else a for a in airports}
        local_candidates = [
            self._latest_matching("data/h-notam_recent/*/search_location_us_hubs_all.jsonl"),
            self._latest_matching("data/h-notam_recent/*/search_location_us_hubs.jsonl"),
            self._latest_matching("data/h-notam_recent/*/search_location_istanbul_all.jsonl"),
            self._latest_matching("data/h-notam_recent/*/search_location_istanbul.jsonl"),
        ]

        docs: List[Dict[str, Any]] = []
        for file in [f for f in local_candidates if f]:
            try:
                with file.open("r", encoding="utf-8", errors="ignore") as f:
                    for line in f:
                        try:
                            obj = json.loads(line)
                        except Exception:
                            continue
                        facility = str(obj.get("facilityDesignator") or "").upper().strip()
                        if facility_targets and facility not in facility_targets:
                            icao_msg = str(obj.get("icaoMessage") or "").upper()
                            if not any(airport in icao_msg for airport in airports):
                                continue
                        docs.append(
                            {
                                "facilityDesignator": obj.get("facilityDesignator"),
                                "notamNumber": obj.get("notamNumber"),
                                "issueDate": obj.get("issueDate"),
                                "startDate": obj.get("startDate"),
                                "endDate": obj.get("endDate"),
                                "sourceType": obj.get("sourceType"),
                                "airportName": obj.get("airportName"),
                                "icaoMessage": str(obj.get("icaoMessage") or "")[:400],
                                "source_file": str(file),
                            }
                        )
                        if len(docs) >= max_rows:
                            return docs
            except Exception:
                continue
        return docs

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
            "strict_mode": self.strict_source_mode,
        }
        if extra:
            payload.update(extra)
        return payload

    def _source_unavailable_row(self, source: str, detail: str) -> Dict[str, Any]:
        return self._source_error_row(
            source=source,
            code="source_unavailable",
            detail=detail,
            extra={"execution_mode": self.source_mode(source)},
        )

    def source_mode(self, source: str) -> str:
        source_norm = (source or "").upper()
        if source_norm == "SQL":
            if not self.sql_available:
                return "blocked"
            if self.sql_backend == "postgres":
                return "live"
            return "fallback"
        if source_norm == "KQL":
            if FABRIC_KQL_ENDPOINT:
                return "live"
            return "fallback" if self.allow_mock_kql_fallback else "blocked"
        if source_norm == "GRAPH":
            if FABRIC_GRAPH_ENDPOINT:
                return "live"
            return "fallback" if self.allow_mock_graph_fallback else "blocked"
        if source_norm == "NOSQL":
            if FABRIC_NOSQL_ENDPOINT:
                return "live"
            return "fallback" if self.allow_mock_nosql_fallback else "blocked"
        if source_norm in {"VECTOR_OPS", "VECTOR_REG", "VECTOR_AIRPORT"}:
            return "live" if self.search_clients else "blocked"
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
            "strict_source_mode": self.strict_source_mode,
            "sql_backend": self.sql_backend if source_norm == "SQL" else "",
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
        checks.append(
            {
                "name": "strict_source_mode",
                "status": "pass",
                "detail": "enabled" if self.strict_source_mode else "disabled",
                "mode": "policy",
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
                "status": "pass" if sql_schema.get("tables") else ("fail" if self.strict_source_mode else "warn"),
                "detail": f"tables={len(sql_schema.get('tables', []))}",
                "mode": sql_mode,
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

        strict_blockers = [c["name"] for c in checks if c.get("mode") == "blocked"]
        if self.strict_source_mode and strict_blockers:
            checks.append(
                {
                    "name": "strict_mode_blockers",
                    "status": "fail",
                    "detail": ",".join(strict_blockers),
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
            "strict_source_mode": self.strict_source_mode,
            "checks": checks,
        }

    def current_sql_schema(self) -> Dict[str, Any]:
        if self.db is None:
            return {
                "source": "unavailable",
                "collected_at": self._now_iso(),
                "schema_version": "none",
                "error": self.sql_unavailable_reason or "sql_connection_unavailable",
                "tables": [],
            }

        tables: List[Dict[str, Any]] = []
        try:
            cur = self.db.cursor()
            if self.use_postgres:
                cur.execute(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema='public'
                    ORDER BY table_name
                    """
                )
                table_names = [str(row[0]) for row in cur.fetchall()]
                for table in table_names:
                    cur.execute(
                        """
                        SELECT column_name, data_type
                        FROM information_schema.columns
                        WHERE table_schema='public' AND table_name=%s
                        ORDER BY ordinal_position
                        """,
                        (table,),
                    )
                    cols = [{"name": str(r[0]), "type": str(r[1])} for r in cur.fetchall()]
                    tables.append({"table": table, "columns": cols})
            else:
                cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
                table_names = [str(row[0]) for row in cur.fetchall()]
                for table in table_names:
                    cur.execute(f"PRAGMA table_info('{table}')")
                    cols = [{"name": str(r[1]), "type": str(r[2])} for r in cur.fetchall()]
                    tables.append({"table": table, "columns": cols})
        except Exception as exc:
            return {
                "source": "live" if self.sql_backend == "postgres" else "fallback",
                "collected_at": self._now_iso(),
                "schema_version": "error",
                "error": str(exc),
                "tables": [],
            }
        return {
            "source": "live" if self.sql_backend == "postgres" else "fallback",
            "collected_at": self._now_iso(),
            "schema_version": f"tables:{len(tables)}",
            "tables": tables,
        }

    def _detect_sql_tables(self, sql_query: str) -> List[str]:
        table_tokens = re.findall(r"\b(?:FROM|JOIN)\s+([A-Za-z_][A-Za-z0-9_\.]*)", sql_query, flags=re.IGNORECASE)
        cleaned: List[str] = []
        for token in table_tokens:
            table = token.strip().strip('"').strip("`")
            table = table.split(".")[-1]
            if table and table not in cleaned:
                cleaned.append(table)
        return cleaned

    def _validate_sql_query(self, sql_query: str) -> Optional[Dict[str, Any]]:
        sql = (sql_query or "").strip()
        if not sql:
            return {"code": "sql_validation_failed", "detail": "empty_sql_query"}
        if not re.match(r"^\s*(SELECT|WITH)\b", sql, flags=re.IGNORECASE):
            return {"code": "sql_validation_failed", "detail": "only_select_or_with_queries_are_allowed"}

        dialect = (self.sql_dialect or "").lower()
        if dialect == "sqlite":
            if re.search(r"\bILIKE\b", sql, flags=re.IGNORECASE):
                return {"code": "sql_dialect_mismatch", "detail": "ILIKE is not supported by sqlite"}
            if re.search(r"::\s*[A-Za-z_][A-Za-z0-9_]*", sql):
                return {"code": "sql_dialect_mismatch", "detail": "PostgreSQL-style :: casts are not supported by sqlite"}

        schema = self.current_sql_schema()
        available_tables = {str(t.get("table", "")).lower() for t in schema.get("tables", []) if isinstance(t, dict)}
        referenced_tables = self._detect_sql_tables(sql)
        missing_tables = [t for t in referenced_tables if t.lower() not in available_tables]
        if missing_tables:
            return {
                "code": "sql_schema_missing",
                "detail": f"missing tables in current schema: {', '.join(missing_tables)}",
            }
        return None

    def execute_sql_query(self, sql_query: str) -> Tuple[List[Dict[str, Any]], List[Citation]]:
        if not self.sql_available or self.db is None:
            return [self._source_unavailable_row("SQL", self.sql_unavailable_reason or "sql_backend_not_available")], []

        validation_error = self._validate_sql_query(sql_query)
        if validation_error:
            if (
                str(validation_error.get("code")) == "sql_schema_missing"
                and "runway_constraints" in str(validation_error.get("detail", "")).lower()
                and not self.strict_source_mode
            ):
                airports = self._extract_airports_from_sql(sql_query)
                return self._query_runway_constraints_fallback(sql_query, airports=airports)
            return [
                self._source_error_row(
                    source="SQL",
                    code=str(validation_error.get("code")),
                    detail=str(validation_error.get("detail")),
                    extra={"sql": sql_query},
                )
            ], []

        try:
            cur = self.db.cursor()
            cur.execute(sql_query)
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description] if cur.description else []
            dict_rows = [dict(zip(columns, row)) for row in rows]
        except Exception as exc:
            return [
                self._source_error_row(
                    source="SQL",
                    code="sql_runtime_error",
                    detail=str(exc),
                    extra={"sql": sql_query},
                )
            ], []

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
        schema = self.current_sql_schema()
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
                schema = self.current_sql_schema()
                sql = self.sql_writer.generate(
                    user_query=enhanced_query,
                    evidence_type="generic",
                    sql_schema=schema,
                    entities={"airports": [], "flight_ids": [], "routes": [], "stations": [], "alternates": []},
                    time_window={"horizon_min": 120, "start_utc": None, "end_utc": None},
                    constraints={"sql_hint": sql_hint or "", "dialect": self.sql_dialect},
                )
        except Exception as exc:
            if not self.allow_legacy_sql_fallback:
                row = self._source_error_row("SQL", "sql_generation_failed", str(exc))
                return [row], "", []
            try:
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
        if ";" in text:
            return "kql_multiple_statements_not_allowed"
        return None

    def _ensure_kql_window(self, csl: str, window_minutes: int) -> str:
        text = (csl or "").strip()
        if not text:
            return text
        if re.search(r"\bago\s*\(", text, flags=re.IGNORECASE):
            return text
        if re.search(r"\btimestamp\b", text, flags=re.IGNORECASE):
            return f"{text}\n| where timestamp > ago({max(1, int(window_minutes))}m)"
        return text

    def query_kql(self, query: str, window_minutes: int = 60) -> Tuple[List[Dict], List[Citation]]:
        """Retrieve event-window signals from Eventhouse or local fallback."""
        if FABRIC_KQL_ENDPOINT:
            if self._is_kusto_endpoint(FABRIC_KQL_ENDPOINT):
                if self._looks_like_kql_text(query):
                    csl = self._ensure_kql_window(query, window_minutes)
                elif self.strict_source_mode:
                    return [
                        self._source_error_row(
                            source="KQL",
                            code="kql_validation_failed",
                            detail="selected KQL source requires executable KQL/CSL query text",
                        )
                    ], []
                else:
                    airports = self._extract_airports_from_query(query)
                    if airports:
                        values = ",".join(f"'{a}'" for a in airports)
                        csl = (
                            "weather_obs "
                            f"| where toupper(station_id) in ({values}) or toupper(icao) in ({values}) "
                            f"| where timestamp > ago({max(1, int(window_minutes))}m) "
                            "| top 40 by timestamp desc"
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
                if self.strict_source_mode:
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
                if self.strict_source_mode:
                    detail = str(response.get("error")) if isinstance(response, dict) else "kql_endpoint_query_failed"
                    return [self._source_error_row("KQL", "kql_runtime_error", detail)], []

        if not FABRIC_KQL_ENDPOINT and not self.allow_mock_kql_fallback:
            return [self._source_unavailable_row("KQL", "FABRIC_KQL_ENDPOINT not configured and fallback disabled")], []

        # Local deterministic fallback for demo readiness.
        rows: List[Dict[str, Any]] = []
        airports = self._extract_airports_from_query(query)
        if airports:
            metar_rows = self._metar_rows_for_airports(airports, max_rows=12)
            rows.extend(metar_rows)
            taf_rows = self._taf_rows_for_airports(airports, max_rows=12)
            rows.extend(taf_rows)

        if rows:
            citation = Citation(
                source_type="KQL",
                identifier="eventhouse_weather_fallback",
                title=f"Eventhouse weather fallback ({', '.join(airports[:3])})",
                content_preview=str(rows[:3])[:120],
                score=0.9,
                dataset="eventhouse-weather-fallback",
            )
            return rows, [citation]

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
                if self.strict_source_mode:
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
                if self.strict_source_mode:
                    detail = str(response.get("error")) if isinstance(response, dict) else "graph_endpoint_query_failed"
                    return [self._source_error_row("GRAPH", "graph_runtime_error", detail)], []

        if not FABRIC_GRAPH_ENDPOINT and not self.allow_mock_graph_fallback:
            return [self._source_unavailable_row("GRAPH", "FABRIC_GRAPH_ENDPOINT not configured and fallback disabled")], []

        graph_file = self._latest_matching("data/j-synthetic_ops_overlay/*/synthetic/ops_graph_edges.csv")
        if not graph_file:
            return [self._source_unavailable_row("GRAPH", "graph_edges_unavailable")], []

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
                if self.strict_source_mode:
                    return [self._source_error_row("NOSQL", "nosql_runtime_error", _error or "nosql_query_returned_no_rows")], []
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
                if self.strict_source_mode:
                    detail = str(response.get("error")) if isinstance(response, dict) else "nosql_endpoint_query_failed"
                    return [self._source_error_row("NOSQL", "nosql_runtime_error", detail)], []

        if not FABRIC_NOSQL_ENDPOINT and not self.allow_mock_nosql_fallback:
            return [self._source_unavailable_row("NOSQL", "FABRIC_NOSQL_ENDPOINT not configured and fallback disabled")], []

        docs = self._notam_docs_for_airports(query, max_rows=30)
        citation = Citation(
            source_type="NOSQL",
            identifier="notam_snapshot",
            title="NoSQL fallback documents",
            content_preview=str(docs[:3])[:120],
            score=0.75,
            dataset="nosql-mock",
        )
        return docs, [citation]

    def query_runway_constraints_fallback(self, query: str, airports: Optional[List[str]] = None) -> Tuple[List[Dict[str, Any]], List[Citation]]:
        """Expose runway constraint fallback rows for planner/executor fallback flows."""
        return self._query_runway_constraints_fallback(query, airports=airports)

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
                model=self.llm_deployment,
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
