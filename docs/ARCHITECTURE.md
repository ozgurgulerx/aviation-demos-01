# Aviation RAG - Architecture

## Overview

Aviation RAG uses planner-driven multi-source retrieval:
- Structured retrieval from SQL (`asrs_reports`, `asrs_ingestion_runs`)
- Multi-index semantic/vector retrieval from Azure AI Search (ops, regulatory, airport-doc indexes)
- Event-window retrieval from KQL/Eventhouse (or local fallback when Fabric endpoint is not configured)
- Graph-assisted retrieval from Fabric graph endpoint (or local synthetic graph fallback)
- Optional NoSQL retrieval path (endpoint-backed or local NOTAM snapshot fallback)

## Components

### Frontend (Next.js 15)
- Azure App Service deployment
- shadcn/ui component library
- SSE streaming for real-time responses
- PII detection integration

### Backend (Flask/Python)
- AKS (Azure Kubernetes Service) deployment
- Agent Framework runtime (`Agent`, `AgentSession`, tool/context-provider primitives)
- Custom RAG context provider over SQL + KQL + Graph + multi-index vector
- AF-native SSE event streaming (`agent_update`, `tool_call`, `tool_result`, `agent_done`)
- Retrieval planner and source-level events (`retrieval_plan`, `source_call_start`, `source_call_done`)
- Azure OpenAI for model inference
- Azure AI Search for semantic retrieval

### Data Pipeline
- ASRS export fetcher with manifest tracking (`scripts/00_fetch_asrs_exports.py`)
- CSV normalization and chunking (`scripts/01_extract_data.py`)
- SQL load with idempotent upsert (`scripts/02_load_database.py`)
- Azure AI Search index management
- Vector upload with embeddings (`scripts/04_upload_documents.py`)
- PostgreSQL / SQLite dual-mode support

### Security
- Azure PII Detection container (on-prem simulation)
- Pre-query PII filtering on both frontend and backend
- SOC 2 Type II compliant architecture

### Observability
- OpenTelemetry traces/metrics emitted from backend runtime
- Azure Monitor / Application Insights exporter path
- Correlated tool-call and retrieval-stage events in chat stream

## Deployment

### Infrastructure
- Azure App Service (frontend)
- AKS cluster (backend)
- Azure PostgreSQL Flexible Server
- Azure AI Search
- Azure OpenAI
- Azure Container Instances (PII)

### CI/CD
- GitHub Actions workflows for frontend and backend deployment
- Infrastructure health check automation
