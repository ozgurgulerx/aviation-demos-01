# Aviation RAG

Enterprise-grade aviation data intelligence platform powered by RAG (Retrieval-Augmented Generation).

## Tech Stack

- **Frontend**: Next.js 15 / React 19 / shadcn/ui on Azure App Service
- **Backend**: Python/Flask + Agent Framework runtime on AKS (Azure Kubernetes Service)
- **AI**: Azure OpenAI + Azure AI Search
- **Security**: Azure PII Detection (on-prem container simulation)
- **Database**: PostgreSQL (Azure Flexible Server) / SQLite (local dev)
- **Observability**: OpenTelemetry + Azure Monitor/Application Insights

## Getting Started

### ASRS Pipeline (Implemented)

The project includes an end-to-end ASRS ingestion flow:

1. Fetch CSV exports from ASRS endpoint (`scripts/00_fetch_asrs_exports.py`)
2. Normalize and chunk data (`scripts/01_extract_data.py`)
3. Load relational store (`scripts/02_load_database.py`)
4. Create/update vector index (`scripts/03_create_search_index.py`)
5. Upload embedded chunks (`scripts/04_upload_documents.py`)

#### Required environment variables

- `ASRS_EXPORT_URL` (for fetch script)
- `AZURE_OPENAI_ENDPOINT`
- `AZURE_OPENAI_DEPLOYMENT_NAME` (recommended: `aviation-chat-gpt5-mini` or `gpt-5-mini`)
- `AZURE_SEARCH_ENDPOINT`
- `AZURE_TEXT_EMBEDDING_DEPLOYMENT_NAME` (optional, defaults to `text-embedding-3-small`)
- `AZURE_OPENAI_VOICE_DEPLOYMENT_NAME` (optional, defaults to `aviation-voice-tts`)
- `AZURE_OPENAI_VOICE_API_VERSION` (optional, defaults to `2025-03-01-preview`)
- `AZURE_OPENAI_AUTH_MODE` (optional: `token`/`auto`/`api-key`, defaults to `token`)
- Token-mode options: `AZURE_OPENAI_TENANT_ID`, `AZURE_OPENAI_CLIENT_ID`, `AZURE_OPENAI_CLIENT_SECRET`, or managed identity (`AZURE_OPENAI_MANAGED_IDENTITY_CLIENT_ID`)
- `APPLICATIONINSIGHTS_CONNECTION_STRING` (recommended for runtime telemetry export)
- DB settings when using postgres mode: `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD`

#### Monthly run (recommended)

```bash
./scripts/run_asrs_monthly_pipeline.sh
```

#### Manual run

```bash
python scripts/00_fetch_asrs_exports.py --from-date 2026-01-01 --to-date 2026-01-31
python scripts/01_extract_data.py --input data/asrs/raw --output data/processed
python scripts/02_load_database.py --mode sqlite --db aviation.db --data data/processed
python scripts/03_create_search_index.py
python scripts/04_upload_documents.py --data data/processed
```

## One-Time Airport/Runway/Network Collection

Use this to collect and snapshot reference data from **OpenFlights** and **OurAirports** (no cron/scheduler).

```bash
./.venv/bin/python scripts/12_fetch_airport_runway_network.py
```

Outputs:

- `data/f-openflights/raw/*_{timestamp}.dat`
- `data/g-ourairports_recent/*_{timestamp}.csv`
- `manifest_{timestamp}.txt` in each folder

Then run downstream prep/load using the latest snapshot files:

```bash
./.venv/bin/python scripts/07_prepare_multi_index_docs.py
./.venv/bin/python scripts/09_bulk_load_multisource_postgres.py --schema demo
./.venv/bin/python scripts/05_fetch_synthetic_overlay.py
```

## Architecture

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for detailed architecture documentation.

## Runtime Migration and Provisioning

Use `scripts/provision-azure.sh` to provision runtime infrastructure (frontend, backend, networking, AKS, App Service, ACR, PostgreSQL wiring) in a target Azure subscription.

```bash
SUBSCRIPTION_ID="<target-subscription-id>" \
RESOURCE_GROUP="rg-aviation-rag" \
LOCATION="westeurope" \
ACR_NAME="aviationragacr" \
ACR_RESOURCE_GROUP="rg-aviation-rag" \
AZURE_OPENAI_ENDPOINT="https://<openai-account>.openai.azure.com/" \
AZURE_SEARCH_ENDPOINT="https://<search-account>.search.windows.net" \
PII_ENDPOINT="http://<pii-endpoint>:5000" \
PG_SERVER="<postgres-server-name>" \
PG_SERVER_RG="<postgres-resource-group>" \
./scripts/provision-azure.sh
```

### GitHub Variables Used by CI/CD

- `AZURE_RESOURCE_GROUP`
- `AZURE_WEBAPP_NAME`
- `AKS_RESOURCE_GROUP`
- `AKS_CLUSTER`
- `AKS_NAMESPACE`
- `AZURE_CONTAINER_REGISTRY_NAME`
- `AZURE_CONTAINER_REGISTRY`
- `BACKEND_URL`
- `PII_ENDPOINT`
- `PII_CONTAINER_ENDPOINT`
- `PG_SERVER_NAME`
- `PG_RESOURCE_GROUP`
- `PGHOST`
- `PGPORT`
- `PGDATABASE`
- `PGUSER`
- `AZURE_OPENAI_ENDPOINT`
- `AZURE_OPENAI_DEPLOYMENT_NAME` (optional, default `aviation-chat-gpt5-mini`)
- `AZURE_TEXT_EMBEDDING_DEPLOYMENT_NAME` (optional, default `text-embedding-3-small`)
- `AZURE_SEARCH_ENDPOINT`

### GitHub Secrets Used by CI/CD

- `AZURE_CLIENT_ID`
- `AZURE_TENANT_ID`
- `AZURE_SUBSCRIPTION_ID`
- `AZURE_OPENAI_API_KEY`
- `AZURE_SEARCH_ADMIN_KEY`
- `PGPASSWORD`
- `APPLICATIONINSIGHTS_CONNECTION_STRING` (optional)
- `FABRIC_BEARER_TOKEN` (optional)

Detailed cutover steps are documented in [docs/RUNTIME_CUTOVER_RUNBOOK.md](docs/RUNTIME_CUTOVER_RUNBOOK.md).

### Retrieval Profiles and Source Hints

The chat API supports retrieval planning hints (all optional):

- `queryProfile` (example: `pilot-brief`, `ops-live`, `compliance`)
- `requiredSources` (example: `["SQL","KQL","GRAPH","VECTOR_REG"]`)
- `freshnessSlaMinutes` (for live-window queries)
- `explainRetrieval` (`true` to include detailed planner reasoning)

## Fabric Service Principal Access

Use [docs/FABRIC_SERVICE_PRINCIPAL_SETUP.md](docs/FABRIC_SERVICE_PRINCIPAL_SETUP.md) to configure and validate Microsoft Fabric API access with a service principal (without changing local Azure CLI user login).

Current FDPO/F2 assignment status is tracked in [docs/FDPO_FABRIC_CURRENT_STATUS.md](docs/FDPO_FABRIC_CURRENT_STATUS.md).
