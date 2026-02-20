#!/usr/bin/env python3
"""
Create/update Azure AI Search index for ASRS documents.
"""

import os
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

INDEX_NAME = os.getenv("AZURE_SEARCH_INDEX_NAME", "aviation-index")
VECTOR_DIMENSIONS = int(os.getenv("AZURE_SEARCH_VECTOR_DIMENSIONS", "1536"))


def create_index() -> None:
    endpoint = os.environ["AZURE_SEARCH_ENDPOINT"]
    admin_key = os.getenv("AZURE_SEARCH_ADMIN_KEY") or os.getenv("AZURE_SEARCH_KEY")
    if admin_key:
        credential = AzureKeyCredential(admin_key)
    else:
        credential = DefaultAzureCredential()

    client = SearchIndexClient(endpoint=endpoint, credential=credential)

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
            vector_search_profile_name="aviation-vector-profile",
        ),
    ]

    vector_search = VectorSearch(
        algorithms=[HnswAlgorithmConfiguration(name="aviation-hnsw")],
        profiles=[
            VectorSearchProfile(
                name="aviation-vector-profile",
                algorithm_configuration_name="aviation-hnsw",
            )
        ],
    )

    semantic_config = SemanticConfiguration(
        name="aviation-semantic-config",
        prioritized_fields=SemanticPrioritizedFields(
            content_fields=[SemanticField(field_name="content")],
            title_field=SemanticField(field_name="title"),
            keywords_fields=[
                SemanticField(field_name="flight_phase"),
                SemanticField(field_name="aircraft_type"),
                SemanticField(field_name="location"),
                SemanticField(field_name="narrative_type"),
            ],
        ),
    )
    semantic_search = SemanticSearch(configurations=[semantic_config])

    index = SearchIndex(
        name=INDEX_NAME,
        fields=fields,
        vector_search=vector_search,
        semantic_search=semantic_search,
    )

    result = client.create_or_update_index(index)
    print(f"Index '{result.name}' created/updated successfully.")
    print(f"  Fields: {len(result.fields)}")
    print(f"  Vector dimensions: {VECTOR_DIMENSIONS}")


if __name__ == "__main__":
    create_index()
