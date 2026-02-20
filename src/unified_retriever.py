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
LLM_DEPLOYMENT = os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME", "gpt-5-nano")
EMBEDDING_DEPLOYMENT = os.getenv("AZURE_TEXT_EMBEDDING_DEPLOYMENT_NAME", "text-embedding-3-small")

# Azure AI Search configuration
SEARCH_ENDPOINT = os.getenv("AZURE_SEARCH_ENDPOINT")
SEARCH_KEY = os.getenv("AZURE_SEARCH_ADMIN_KEY")
SEARCH_INDEX_OPS = os.getenv("AZURE_SEARCH_INDEX_OPS_NAME", "idx_ops_narratives")
SEARCH_INDEX_REGULATORY = os.getenv("AZURE_SEARCH_INDEX_REGULATORY_NAME", "idx_regulatory")
SEARCH_INDEX_AIRPORT = os.getenv("AZURE_SEARCH_INDEX_AIRPORT_NAME", "idx_airport_ops_docs")

FABRIC_KQL_ENDPOINT = os.getenv("FABRIC_KQL_ENDPOINT")
FABRIC_GRAPH_ENDPOINT = os.getenv("FABRIC_GRAPH_ENDPOINT")
FABRIC_NOSQL_ENDPOINT = os.getenv("FABRIC_NOSQL_ENDPOINT")
FABRIC_BEARER_TOKEN = os.getenv("FABRIC_BEARER_TOKEN", "")
ROOT = Path(__file__).resolve().parents[1]


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

    def _post_json(self, endpoint: str, payload: Dict[str, Any]) -> Dict[str, Any]:
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

        if embedding is None:
            embedding = self.get_embedding(query)

        vector_query = VectorizedQuery(
            vector=embedding,
            k_nearest_neighbors=top,
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

        try:
            results = client.search(
                search_text=query,
                vector_queries=[vector_query],
                top=top,
                filter=filter_expression,
                select=select_fields,
            )
        except Exception as exc:
            return [{"error": str(exc), "index": index_name}], []

        results_list: List[Dict[str, Any]] = []
        citations: List[Citation] = []

        for r in results:
            result_dict = dict(r)
            result_dict["vector_source"] = source
            result_dict["vector_index"] = index_name
            results_list.append(result_dict)

            report_id = r.get("asrs_report_id") or r.get("id", "")
            citation_title = r.get("title") or f"{source} {report_id}"
            citations.append(
                Citation(
                    source_type=source,
                    identifier=str(report_id),
                    title=str(citation_title),
                    content_preview=str(r.get("content", ""))[:120],
                    score=float(r.get("@search.score", 0.0) or 0.0),
                    dataset=index_name,
                )
            )

        return results_list, citations

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

        merged_rows.sort(key=lambda r: float(r.get("@search.score", 0.0) or 0.0), reverse=True)
        merged_citations.sort(key=lambda c: c.score, reverse=True)
        return merged_rows, merged_citations

    def query_kql(self, query: str, window_minutes: int = 60) -> Tuple[List[Dict], List[Citation]]:
        """Retrieve event-window signals from Eventhouse or local fallback."""
        if FABRIC_KQL_ENDPOINT:
            payload = {"query": query, "window_minutes": window_minutes}
            response = self._post_json(FABRIC_KQL_ENDPOINT, payload)
            if isinstance(response, dict) and "error" in response:
                return [response], []
            rows = response.get("rows", []) if isinstance(response, dict) else []
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
            payload = {"query": query, "hops": hops}
            response = self._post_json(FABRIC_GRAPH_ENDPOINT, payload)
            if isinstance(response, dict) and "error" in response:
                return [response], []
            paths = response.get("paths", []) if isinstance(response, dict) else []
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
            payload = {"query": query}
            response = self._post_json(FABRIC_NOSQL_ENDPOINT, payload)
            if isinstance(response, dict) and "error" in response:
                return [response], []
            docs = response.get("docs", []) if isinstance(response, dict) else []
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
