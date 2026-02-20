#!/usr/bin/env python3
"""
Create/update Azure AI Search indexes for the multi-index architecture.

Creates three indexes matching what unified_retriever.py expects:
  - idx_ops_narratives     (VECTOR_OPS)   — ASRS narrative documents
  - idx_regulatory         (VECTOR_REG)   — Regulatory / NOTAM / AD documents
  - idx_airport_ops_docs   (VECTOR_AIRPORT) — Airport / runway / station docs

Each index shares the same semantic config name ("aviation-semantic-config")
and vector profile so the retriever code works uniformly across all indexes.

Env vars:
  AZURE_SEARCH_ENDPOINT           (required)
  AZURE_SEARCH_ADMIN_KEY          (required unless using managed identity)
  AZURE_SEARCH_INDEX_OPS_NAME     (default: idx_ops_narratives)
  AZURE_SEARCH_INDEX_REGULATORY_NAME (default: idx_regulatory)
  AZURE_SEARCH_INDEX_AIRPORT_NAME (default: idx_airport_ops_docs)
  AZURE_SEARCH_VECTOR_DIMENSIONS  (default: 1536)
"""

import os
import sys
from dotenv import load_dotenv
from azure.identity import DefaultAzureCredential
from azure.core.credentials import AzureKeyCredential
from azure.search.documents.indexes import SearchIndexClient
from azure.search.documents.indexes.models import (
    HnswAlgorithmConfiguration,
    SearchField,
    SearchFieldDataType,
    SearchIndex,
    SearchableField,
    SemanticConfiguration,
    SemanticField,
    SemanticPrioritizedFields,
    SemanticSearch,
    SimpleField,
    VectorSearch,
    VectorSearchProfile,
)

load_dotenv()

VECTOR_DIMENSIONS = int(os.getenv("AZURE_SEARCH_VECTOR_DIMENSIONS", "1536"))

# Index names — must match the defaults in unified_retriever.py
INDEX_OPS = os.getenv("AZURE_SEARCH_INDEX_OPS_NAME", "idx_ops_narratives")
INDEX_REGULATORY = os.getenv("AZURE_SEARCH_INDEX_REGULATORY_NAME", "idx_regulatory")
INDEX_AIRPORT = os.getenv("AZURE_SEARCH_INDEX_AIRPORT_NAME", "idx_airport_ops_docs")

# Shared vector and semantic config used by all indexes.
VECTOR_PROFILE_NAME = "aviation-vector-profile"
HNSW_ALGO_NAME = "aviation-hnsw"
SEMANTIC_CONFIG_NAME = "aviation-semantic-config"


def _build_vector_search() -> VectorSearch:
    return VectorSearch(
        algorithms=[HnswAlgorithmConfiguration(name=HNSW_ALGO_NAME)],
        profiles=[
            VectorSearchProfile(
                name=VECTOR_PROFILE_NAME,
                algorithm_configuration_name=HNSW_ALGO_NAME,
            )
        ],
    )


def _build_semantic_search(
    content_field: str = "content",
    title_field: str = "title",
    keyword_fields: list[str] | None = None,
) -> SemanticSearch:
    kw_fields = keyword_fields or []
    return SemanticSearch(
        configurations=[
            SemanticConfiguration(
                name=SEMANTIC_CONFIG_NAME,
                prioritized_fields=SemanticPrioritizedFields(
                    content_fields=[SemanticField(field_name=content_field)],
                    title_field=SemanticField(field_name=title_field),
                    keywords_fields=[SemanticField(field_name=f) for f in kw_fields],
                ),
            )
        ]
    )


def _ops_index() -> SearchIndex:
    """ASRS narrative documents (VECTOR_OPS)."""
    fields = [
        SimpleField(name="id", type=SearchFieldDataType.String, key=True, filterable=True),
        SearchableField(name="content", type=SearchFieldDataType.String),
        SearchableField(name="title", type=SearchFieldDataType.String, filterable=True),
        SimpleField(name="source", type=SearchFieldDataType.String, filterable=True),
        SimpleField(name="asrs_report_id", type=SearchFieldDataType.String, filterable=True),
        SimpleField(name="event_date", type=SearchFieldDataType.String, filterable=True, sortable=True),
        SearchableField(name="aircraft_type", type=SearchFieldDataType.String, filterable=True),
        SearchableField(name="flight_phase", type=SearchFieldDataType.String, filterable=True),
        SearchableField(name="location", type=SearchFieldDataType.String, filterable=True),
        SearchableField(name="narrative_type", type=SearchFieldDataType.String, filterable=True),
        SimpleField(name="source_file", type=SearchFieldDataType.String, filterable=True),
        SearchField(
            name="content_vector",
            type=SearchFieldDataType.Collection(SearchFieldDataType.Single),
            searchable=True,
            vector_search_dimensions=VECTOR_DIMENSIONS,
            vector_search_profile_name=VECTOR_PROFILE_NAME,
        ),
    ]
    return SearchIndex(
        name=INDEX_OPS,
        fields=fields,
        vector_search=_build_vector_search(),
        semantic_search=_build_semantic_search(
            keyword_fields=["flight_phase", "aircraft_type", "location", "narrative_type"],
        ),
    )


def _regulatory_index() -> SearchIndex:
    """Regulatory / NOTAM / Airworthiness Directive documents (VECTOR_REG)."""
    fields = [
        SimpleField(name="id", type=SearchFieldDataType.String, key=True, filterable=True),
        SearchableField(name="content", type=SearchFieldDataType.String),
        SearchableField(name="title", type=SearchFieldDataType.String, filterable=True),
        SimpleField(name="source", type=SearchFieldDataType.String, filterable=True),
        SimpleField(name="document_number", type=SearchFieldDataType.String, filterable=True),
        SimpleField(name="effective_date", type=SearchFieldDataType.String, filterable=True, sortable=True),
        SearchableField(name="issuing_authority", type=SearchFieldDataType.String, filterable=True),
        SearchableField(name="aircraft_type", type=SearchFieldDataType.String, filterable=True),
        SearchableField(name="document_type", type=SearchFieldDataType.String, filterable=True),
        SimpleField(name="source_file", type=SearchFieldDataType.String, filterable=True),
        SearchField(
            name="content_vector",
            type=SearchFieldDataType.Collection(SearchFieldDataType.Single),
            searchable=True,
            vector_search_dimensions=VECTOR_DIMENSIONS,
            vector_search_profile_name=VECTOR_PROFILE_NAME,
        ),
    ]
    return SearchIndex(
        name=INDEX_REGULATORY,
        fields=fields,
        vector_search=_build_vector_search(),
        semantic_search=_build_semantic_search(
            keyword_fields=["issuing_authority", "aircraft_type", "document_type"],
        ),
    )


def _airport_index() -> SearchIndex:
    """Airport / runway / station operational documents (VECTOR_AIRPORT)."""
    fields = [
        SimpleField(name="id", type=SearchFieldDataType.String, key=True, filterable=True),
        SearchableField(name="content", type=SearchFieldDataType.String),
        SearchableField(name="title", type=SearchFieldDataType.String, filterable=True),
        SimpleField(name="source", type=SearchFieldDataType.String, filterable=True),
        SimpleField(name="airport_icao", type=SearchFieldDataType.String, filterable=True),
        SimpleField(name="airport_iata", type=SearchFieldDataType.String, filterable=True),
        SearchableField(name="airport_name", type=SearchFieldDataType.String, filterable=True),
        SearchableField(name="facility_type", type=SearchFieldDataType.String, filterable=True),
        SimpleField(name="effective_date", type=SearchFieldDataType.String, filterable=True, sortable=True),
        SimpleField(name="source_file", type=SearchFieldDataType.String, filterable=True),
        SearchField(
            name="content_vector",
            type=SearchFieldDataType.Collection(SearchFieldDataType.Single),
            searchable=True,
            vector_search_dimensions=VECTOR_DIMENSIONS,
            vector_search_profile_name=VECTOR_PROFILE_NAME,
        ),
    ]
    return SearchIndex(
        name=INDEX_AIRPORT,
        fields=fields,
        vector_search=_build_vector_search(),
        semantic_search=_build_semantic_search(
            keyword_fields=["airport_icao", "airport_name", "facility_type"],
        ),
    )


def create_indexes() -> None:
    endpoint = os.environ["AZURE_SEARCH_ENDPOINT"]
    admin_key = os.getenv("AZURE_SEARCH_ADMIN_KEY") or os.getenv("AZURE_SEARCH_KEY")
    if admin_key:
        credential = AzureKeyCredential(admin_key)
    else:
        credential = DefaultAzureCredential()

    client = SearchIndexClient(endpoint=endpoint, credential=credential)

    indexes = [
        ("ops_narratives (VECTOR_OPS)", _ops_index()),
        ("regulatory (VECTOR_REG)", _regulatory_index()),
        ("airport_ops (VECTOR_AIRPORT)", _airport_index()),
    ]

    for label, index_def in indexes:
        result = client.create_or_update_index(index_def)
        print(f"[{label}] Index '{result.name}' created/updated.")
        print(f"  Fields: {len(result.fields)}")
        print(f"  Vector dimensions: {VECTOR_DIMENSIONS}")
        print(f"  Semantic config: {SEMANTIC_CONFIG_NAME}")
        print()


if __name__ == "__main__":
    create_indexes()
