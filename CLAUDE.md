# Aviation RAG — Development Guide

## Project Overview

Enterprise aviation intelligence platform using Retrieval-Augmented Generation. Next.js 15 frontend on Azure App Service proxies to a Flask/Python backend on AKS, which orchestrates queries across PostgreSQL (structured data), Azure AI Search (semantic/vector), and Azure OpenAI (gpt-5-nano) with dual-layer PII detection.

## Project Structure

```
src/                          # Monorepo: Next.js frontend + Python backend
  api_server.py               # Flask API (routes: /health, /api/chat, /api/query)
  unified_retriever.py        # Core RAG orchestrator (SQL + Semantic + Hybrid)
  query_router.py             # LLM-based and heuristic query classification
  sql_generator.py            # Natural language -> SQL over ASRS schema
  pii_filter.py               # PII detection client (14 categories, threshold 0.8)
  app/                        # Next.js App Router
    api/chat/route.ts         # SSE streaming proxy to Python backend
    api/pii/route.ts          # PII check proxy to Azure PII container
    api/health/route.ts       # Frontend health endpoint
    chat/page.tsx             # Main chat page (3-column layout)
    login/page.tsx            # SSO login (mock)
  components/chat/            # ChatThread, Message, MessageComposer, FollowUpChips
  components/layout/          # Header, Sidebar, Footer, SourcesPanel
  components/ui/              # shadcn/ui primitives
  lib/                        # chat.ts (SSE parser), pii.ts, utils.ts
  types/index.ts              # All TypeScript interfaces
  data/seed.ts                # Sample data, query categories, DATA_PROVIDERS
k8s/                          # Kubernetes manifests (namespace, deployment, services, configmap, secret, ingress)
scripts/                      # ASRS pipeline + provision-azure.sh + start-pii-container.sh
data/                         # Raw data files (gitignored, not committed)
docs/                         # Architecture documentation
Dockerfile.backend            # Python 3.11-slim, gunicorn, port 5001
Dockerfile.frontend           # Node 20-alpine, standalone, port 3001
.github/workflows/            # deploy-backend, deploy-frontend, infra-health-check, migrate-database
```

## Development Commands

```bash
# Frontend (Next.js dev server)
cd src && npm install && npm run dev                  # http://localhost:3001

# Backend (Flask dev server)
cd src && python api_server.py                        # http://localhost:5001

# Lint
cd src && npx next lint

# PII container (local Docker)
./scripts/start-pii-container.sh                      # http://localhost:5000

# Provision Azure infrastructure (idempotent)
./scripts/provision-azure.sh

# Data pipeline (ASRS)
python scripts/00_fetch_asrs_exports.py --from-date 2026-01-01 --to-date 2026-01-31
python scripts/01_extract_data.py --input data/asrs/raw --output data/processed
python scripts/02_load_database.py --mode postgres --data data/processed
python scripts/03_create_search_index.py
python scripts/04_upload_documents.py --data data/processed

# Seed Cosmos DB with sample NOTAMs
python scripts/13_seed_cosmos_notams.py
```

## Azure Tenant Context (Required)

- Account: `admin@MngEnvMCAP705508.onmicrosoft.com`
- Tenant ID: `52095a81-130f-4b06-83f1-9859b2c73de6`
- Subscription: `ME-MngEnvMCAP705508-ozgurguler-1` (`6a539906-6ce2-4e3b-84ee-89f701de18d8`)

**Always use this tenant.** All Azure operations (provisioning, deployment, CLI commands) must target this tenant and subscription. Do not switch or prompt for alternatives.

Always verify Azure CLI context before provisioning/deploying:

```bash
az account show --query "{user:user.name,tenantId:tenantId,name:name,id:id}" -o table
```

## Architecture

```
User -> MessageComposer (PII pre-check) -> POST /api/pii -> Azure PII Container
     -> POST /api/chat (Next.js) -> POST /api/chat (Flask)
        -> PiiFilter.check() (backend PII layer)
        -> QueryRouter.route() (LLM) or .quick_route() (heuristic)
           -> SQL route:      SQLGenerator -> PostgreSQL -> citations
           -> SEMANTIC route: AI Search (vector + semantic) -> citations
           -> HYBRID route:   SQL + Semantic in parallel (ThreadPoolExecutor)
        -> Azure OpenAI (gpt-5-nano) synthesizes answer from context
     <- SSE stream: tool_call -> text (word-by-word) -> metadata -> citations -> done
```

**Query routes:** SQL (rankings, counts, comparisons), SEMANTIC (similarity, descriptions), HYBRID (combined).
**PII detection:** Dual-layer — frontend pre-send + backend pre-LLM. 14 categories. Fail-open on service unavailability.
**SSE streaming:** Cosmetic word-by-word (5ms delay). Backend returns full JSON; Next.js splits it.

## Key Conventions

### Frontend
- Next.js 15 App Router with `output: "standalone"` for container deployment
- shadcn/ui (new-york style) + Tailwind + Framer Motion for animations
- Dark theme default ("Obsidian Ledger"), gold accent color
- `isHydrated` pattern in Message component for Framer Motion SSR safety
- Citation chips `[N]` parsed from markdown with regex, rendered as clickable buttons
- PII scan UI: idle -> checking (amber) -> passed (emerald) -> blocked (red + shake)

### Backend
- Flask with flask-cors (all origins allowed)
- `DefaultAzureCredential` + `get_bearer_token_provider` for Azure OpenAI auth
- DB: PostgreSQL only (requires `PGHOST` set)
- Single gunicorn worker in production (`--workers 1 --timeout 180`)
- PII filter: 14 categories, confidence >= 0.8, fail-open on timeout (5s)

## Environment Variables

### Frontend (server-side)
- `BACKEND_URL` / `PYTHON_API_URL` — Python backend URL (default: `http://localhost:5001`)
- `PII_ENDPOINT` / `PII_CONTAINER_ENDPOINT` — PII service URL
- `PII_API_KEY` — PII API key (not needed for container mode)

### Backend
- `AZURE_OPENAI_ENDPOINT` — Azure OpenAI endpoint
- `AZURE_OPENAI_API_KEY` — API key (fallback when no managed identity)
- `AZURE_OPENAI_DEPLOYMENT_NAME` — LLM deployment (default: `gpt-5-nano`)
- `AZURE_TEXT_EMBEDDING_DEPLOYMENT_NAME` — Embedding model (default: `text-embedding-3-small`)
- `AZURE_SEARCH_ENDPOINT` — AI Search endpoint
- `AZURE_SEARCH_ADMIN_KEY` — AI Search admin key
- `AZURE_SEARCH_INDEX_NAME` — Search index (default: `aviation-index`)
- `ASRS_EXPORT_URL` — ASRS export endpoint URL for `scripts/00_fetch_asrs_exports.py`
- `ASRS_QUERY_TEMPLATE_JSON` — optional JSON object of fixed query params for ASRS export requests
- `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD` — PostgreSQL connection
- `PII_ENDPOINT` / `PII_CONTAINER_ENDPOINT` — PII service URL
- `AZURE_COSMOS_ENDPOINT` — Cosmos DB for NoSQL endpoint (NOTAM storage)
- `AZURE_COSMOS_KEY` — Cosmos DB primary key
- `AZURE_COSMOS_DATABASE` — Cosmos DB database name (default: `aviationrag`)
- `AZURE_COSMOS_CONTAINER` — Cosmos DB container name (default: `notams`)

## What Is TBD

1. AI Search index may be empty until `scripts/04_upload_documents.py` is executed with real data
2. Read-only PostgreSQL user `aviationrag_readonly` — not yet created
3. GitHub Actions secrets (6 secrets) — not configured
4. Initial Docker image push to ACR — backend never built/pushed
5. Login page (`app/login/page.tsx`) — still shows "Fund Intelligence" from predecessor project
6. `chain` and `raptor` query types in frontend — no backend support
7. Export buttons (PDF/CSV) in SourcesPanel — mock implementations
8. Foundry IQ retrieval mode — wired in UI but backend ignores it

## Do NOT

- **Commit `.env.local` or `.env`** — contains real Azure secrets (keys, passwords, connection strings)
- **Increase gunicorn workers** beyond 1 without reviewing in-memory state in unified_retriever.py
- **Skip PostgreSQL connection pool** changes without reviewing thread safety in unified_retriever.py
- **Forget CSP `connect-src`** in `next.config.mjs` when changing backend domain
- **Assume SSE reduces latency** — streaming is cosmetic, backend returns full response synchronously
- **Use `AZURE_OPENAI_API_KEY` in production** — prefer `DefaultAzureCredential` (managed identity)
