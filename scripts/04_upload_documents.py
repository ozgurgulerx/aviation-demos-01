#!/usr/bin/env python3
"""
Upload ASRS vector documents to Azure AI Search.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
from pathlib import Path
from typing import Dict, Iterable, List

from dotenv import load_dotenv
from azure.core.credentials import AzureKeyCredential
from azure.identity import DefaultAzureCredential, get_bearer_token_provider
from azure.search.documents import SearchClient
from openai import AzureOpenAI

load_dotenv()

INDEX_NAME = os.getenv("AZURE_SEARCH_INDEX_NAME", "aviation-index")
BATCH_SIZE = 100
VECTOR_DIMENSIONS = int(os.getenv("AZURE_SEARCH_VECTOR_DIMENSIONS", "1536"))
UNSAFE_KEY_CHARS = re.compile(r"[^A-Za-z0-9_\-=]")


def normalize_search_key(value: str) -> str:
    normalized = UNSAFE_KEY_CHARS.sub("_", value.strip())
    if not normalized:
        raise ValueError("Document id is empty after normalization")
    return normalized[:1024]


def get_embedding_azure(client: AzureOpenAI, text: str, model: str) -> List[float]:
    response = client.embeddings.create(input=[text], model=model)
    return response.data[0].embedding


def get_embedding_hash(text: str, dims: int) -> List[float]:
    vec = [0.0] * dims
    tokens = text.lower().split()
    if not tokens:
        return vec
    for token in tokens:
        digest = hashlib.sha256(token.encode("utf-8")).digest()
        idx = int.from_bytes(digest[:4], "big") % dims
        sign = -1.0 if digest[4] % 2 else 1.0
        mag = 0.5 + (digest[5] / 255.0)
        vec[idx] += sign * mag
    norm = math.sqrt(sum(v * v for v in vec)) or 1.0
    return [v / norm for v in vec]


def iter_jsonl(path: Path) -> Iterable[Dict[str, str]]:
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            stripped = line.strip()
            if not stripped:
                continue
            try:
                yield json.loads(stripped)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid JSON at {path}:{line_number}: {exc}") from exc


def sanitize_document(doc: Dict[str, str]) -> Dict[str, str]:
    def clean_optional(value) -> str:
        if value is None:
            return ""
        return str(value).strip()

    required = ["id", "content", "title", "source"]
    for key in required:
        if not str(doc.get(key, "")).strip():
            raise ValueError(f"Document missing required field '{key}'")

    clean = {
        "id": normalize_search_key(str(doc["id"])),
        "content": str(doc["content"]).strip(),
        "title": str(doc.get("title", "")).strip(),
        "source": str(doc.get("source", "ASRS")).strip(),
        "asrs_report_id": clean_optional(doc.get("asrs_report_id")),
        "event_date": clean_optional(doc.get("event_date")) or None,
        "aircraft_type": clean_optional(doc.get("aircraft_type")),
        "flight_phase": clean_optional(doc.get("flight_phase")),
        "location": clean_optional(doc.get("location")),
        "narrative_type": clean_optional(doc.get("narrative_type")),
        "source_file": clean_optional(doc.get("source_file")),
    }

    return clean


def batched(items: List[Dict[str, str]], size: int) -> Iterable[List[Dict[str, str]]]:
    for idx in range(0, len(items), size):
        yield items[idx: idx + size]


def ensure_unique_ids(docs: List[Dict[str, str]]) -> None:
    seen: Dict[str, int] = {}
    for doc in docs:
        base = doc["id"]
        count = seen.get(base, 0)
        if count == 0:
            seen[base] = 1
            continue

        # Preserve deterministic uniqueness when normalization collapses IDs.
        while True:
            suffix = f"_{count + 1}"
            if len(base) + len(suffix) > 1024:
                candidate = f"{base[: 1024 - len(suffix)]}{suffix}"
            else:
                candidate = f"{base}{suffix}"
            if candidate not in seen:
                doc["id"] = candidate
                seen[base] = count + 1
                seen[candidate] = 1
                break
            count += 1


def upload_documents(
    data_dir: str,
    documents_file: str,
    batch_size: int,
    dry_run: bool,
    embedding_mode: str,
) -> None:
    credential = DefaultAzureCredential()
    openai_client = None
    if embedding_mode == "azure":
        openai_key = os.getenv("AZURE_OPENAI_API_KEY", "").strip()
        openai_use_key = os.getenv("AZURE_OPENAI_USE_KEY", "").strip().lower() in ("1", "true", "yes")
        if openai_key and openai_use_key:
            openai_client = AzureOpenAI(
                azure_endpoint=os.environ["AZURE_OPENAI_ENDPOINT"],
                api_key=openai_key,
                api_version="2024-06-01",
            )
        else:
            token_provider = get_bearer_token_provider(
                credential, "https://cognitiveservices.azure.com/.default"
            )
            openai_client = AzureOpenAI(
                azure_endpoint=os.environ["AZURE_OPENAI_ENDPOINT"],
                azure_ad_token_provider=token_provider,
                api_version="2024-06-01",
            )
    embedding_model = os.environ.get(
        "AZURE_TEXT_EMBEDDING_DEPLOYMENT_NAME", "text-embedding-3-small"
    )

    search_key = os.getenv("AZURE_SEARCH_ADMIN_KEY") or os.getenv("AZURE_SEARCH_KEY")
    if search_key:
        search_credential = AzureKeyCredential(search_key)
    else:
        search_credential = credential

    search_client = SearchClient(
        endpoint=os.environ["AZURE_SEARCH_ENDPOINT"],
        index_name=INDEX_NAME,
        credential=search_credential,
    )

    docs_path = Path(data_dir) / documents_file
    if not docs_path.exists():
        raise FileNotFoundError(f"Documents file not found: {docs_path}")

    docs: List[Dict[str, str]] = []
    for raw_doc in iter_jsonl(docs_path):
        doc = sanitize_document(raw_doc)
        docs.append(doc)

    ensure_unique_ids(docs)

    if not docs:
        print("No documents to upload.")
        return

    print(f"Preparing {len(docs)} documents from {docs_path}")

    for idx, doc in enumerate(docs, start=1):
        if embedding_mode == "hash":
            doc["content_vector"] = get_embedding_hash(doc["content"], VECTOR_DIMENSIONS)
        else:
            doc["content_vector"] = get_embedding_azure(openai_client, doc["content"], embedding_model)
        if idx % 100 == 0:
            print(f"  Embedded {idx}/{len(docs)}")

    if dry_run:
        print("Dry run enabled: embeddings generated, upload skipped.")
        return

    uploaded = 0
    failed = 0

    for batch_no, batch in enumerate(batched(docs, batch_size), start=1):
        result = search_client.upload_documents(documents=batch)
        batch_uploaded = 0
        batch_failed = 0
        for item in result:
            if item.succeeded:
                batch_uploaded += 1
            else:
                batch_failed += 1
                print(f"  Upload failed for key={item.key}: {item.error_message}")

        uploaded += batch_uploaded
        failed += batch_failed
        print(f"  Batch {batch_no}: uploaded={batch_uploaded}, failed={batch_failed}")

    print("Upload complete")
    print(f"  Index: {INDEX_NAME}")
    print(f"  Total documents: {len(docs)}")
    print(f"  Uploaded: {uploaded}")
    print(f"  Failed: {failed}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Upload ASRS documents to Azure AI Search")
    parser.add_argument("--data", default="data/processed", help="Directory with processed documents")
    parser.add_argument("--documents-file", default="asrs_documents.jsonl", help="JSONL file containing docs")
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE, help="Upload batch size")
    parser.add_argument("--dry-run", action="store_true", help="Compute embeddings but do not upload")
    parser.add_argument(
        "--embedding-mode",
        choices=["azure", "hash"],
        default="azure",
        help="Embedding backend: azure (default) or deterministic hash fallback",
    )
    args = parser.parse_args()

    if args.batch_size < 1:
        raise ValueError("--batch-size must be >= 1")

    upload_documents(args.data, args.documents_file, args.batch_size, args.dry_run, args.embedding_mode)


if __name__ == "__main__":
    main()
