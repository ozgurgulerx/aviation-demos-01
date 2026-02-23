# Codex Memory: Tenant, Infra, and Fabric Context

Last updated: 2026-02-23

## Freshness metadata (non-secret)

- Runtime facts last verified: 2026-02-23
- Runtime facts stale-after window: 7 days
- Infrastructure topology last verified: 2026-02-23
- Infrastructure topology stale-after window: 30 days
- On stale/contradictory data: verify against repo truth and command output before risky execution.
- Structure reference for future updates: `docs/CODEX_MEMORY_TEMPLATE.md`.

## Identity and tenant context

- Primary admin identity: `admin@MngEnvMCAP705508.onmicrosoft.com` (user-provided)
- Tenant IDs/domains used:
  - `16b3c013-d300-468d-ac64-7eda0820b6d3` (`fdpo.onmicrosoft.com`) from `docs/FDPO_FABRIC_CURRENT_STATUS.md`
  - One MTC tenant is referenced in `docs/FABRIC_SERVICE_PRINCIPAL_SETUP.md` for Fabric service principal bootstrap
- Subscription(s) in scope:
  - Foundry subscription: `a20bc194-9787-44ee-9c7f-7c3130e651b6` (`MCAPS-Hybrid-REQ-102171-2024-ozgurguler`)
- Notes:
  - This file stores non-secret memory only.

## Runtime infrastructure this solution is built on

- Frontend runtime: Next.js 15 on Azure App Service
- Backend runtime: Flask/Python runtime on AKS
- Data/AI services: PostgreSQL Flexible Server, Azure OpenAI, Azure AI Search
- Security/observability: PII endpoint/container path, OpenTelemetry -> Azure Monitor/Application Insights
- Provisioning source of truth: `scripts/provision-azure.sh`

Default runtime naming and layout:

- Resource group: `rg-aviation-rag`
- Region: `westeurope`
- VNet: `vnet-aviation-rag`
- AKS/App runtime: `aks-aviation-rag`
- Registry: `aviationragacr`
- Namespace: `aviation-rag`
- Databases:
  - PostgreSQL server: `aviationragpg`
  - PostgreSQL database: `aviationrag`
  - Read-only DB user target: `aviationrag_readonly`

Network defaults:

- VNet CIDR: `10.0.0.0/16`
- Subnet(s):
  - AKS subnet: `10.0.0.0/22`
  - App Service subnet: `10.0.4.0/24`
  - Private endpoint subnet: `10.0.5.0/24`
- Service CIDR: `10.1.0.0/16`

## Azure AI Foundry context (active project wiring)

- Foundry subscription: `a20bc194-9787-44ee-9c7f-7c3130e651b6` (`MCAPS-Hybrid-REQ-102171-2024-ozgurguler`)
- Foundry account: `ai-eastus2hubozguler527669401205` (`rg-openai`, `eastus2`)
- Foundry project resource: `ai-eastus2hubozguler527-project`
- Project endpoint: `https://ai-eastus2hubozguler527669401205.services.ai.azure.com/api/projects/ai-eastus2hubozguler527-project`

Deployments currently used:

- Chat: `aviation-chat-gpt5-mini` -> `gpt-5-mini` (`2025-08-07`), `GlobalStandard`, capacity `50`
- Voice/TTS: `aviation-voice-tts` -> `gpt-4o-mini-tts` (`2025-12-15`), `GlobalStandard`, capacity `20`
- Realtime/audio: `aviation-voice-gpt4o-audio` -> `gpt-4o-audio-preview` (`2024-12-17`), `GlobalStandard`, capacity `20`
- Embeddings (if applicable): not separately tracked in this file

Runtime defaults expected:

- Backend env defaults:
  - `AZURE_OPENAI_DEPLOYMENT_NAME=aviation-chat-gpt5-mini`
- Frontend env defaults:
  - `AZURE_OPENAI_ENDPOINT=https://ai-eastus2hubozguler527669401205.cognitiveservices.azure.com/`
  - `AZURE_OPENAI_VOICE_DEPLOYMENT_NAME=aviation-voice-tts`
  - `AZURE_OPENAI_VOICE_MODEL=gpt-4o-mini-tts`
  - `AZURE_OPENAI_VOICE_API_VERSION=2025-03-01-preview`
  - `AZURE_OPENAI_AUTH_MODE=token`
  - `AZURE_OPENAI_VOICE_TURKISH=alloy`
  - `AZURE_OPENAI_VOICE_ENGLISH=alloy`
  - Token auth supports Entra service principal (`AZURE_OPENAI_TENANT_ID`, `AZURE_OPENAI_CLIENT_ID`, `AZURE_OPENAI_CLIENT_SECRET`) and managed identity (`AZURE_OPENAI_MANAGED_IDENTITY_CLIENT_ID`)

## Fabric integration context

Primary docs and scripts:

- `docs/FABRIC_SERVICE_PRINCIPAL_SETUP.md`
- `docs/FDPO_FABRIC_CURRENT_STATUS.md`
- `scripts/fabric/bootstrap-sp.sh`
- `scripts/fabric/validate-sp-access.sh`

Runtime integration in code:

- Env vars:
  - `FABRIC_KQL_ENDPOINT`
  - `FABRIC_GRAPH_ENDPOINT`
  - `FABRIC_NOSQL_ENDPOINT`
  - `FABRIC_BEARER_TOKEN`
- API endpoints:
  - Backend: `GET /api/fabric/preflight` in `src/api_server.py`
  - Frontend proxy: `src/app/api/fabric/preflight/route.ts`
- Core modules:
  - `src/unified_retriever.py`

Current tracked status:

- Workspace/capacity status:
  - Workspace `fdpo-access-check-ws-20260218-171328` exists (`6bf216a8-e74a-44cc-9c0d-6f141c82d487`)
  - Fabric F2 capacity `fdpofabricf20218202320` is active in `rg-fabric` (UK South)
- Blockers:
  - F2 capacity is not visible in Fabric capacities for current identity; admin-side role/tenant settings are still required

## CI/CD and deployment memory pointers

- Backend deploy workflow: `.github/workflows/deploy-backend.yaml`
- Frontend deploy workflow: `.github/workflows/deploy-frontend.yaml`
- Infra health workflow: `.github/workflows/infra-health-check.yaml`
- DB migration workflow: `.github/workflows/migrate-database.yaml`

## High-signal files to load first for codex memory

- `AGENTS.md`
- `docs/CODEX_MEMORY.md`
- `README.md`
- `CLAUDE.md`
- `docs/ARCHITECTURE.md`
- `docs/RUNTIME_CUTOVER_RUNBOOK.md`
- `scripts/provision-azure.sh`
- `k8s/backend-configmap.yaml`
- `k8s/backend-deployment.yaml`
- `k8s/backend-service.yaml`
- `docs/FABRIC_SERVICE_PRINCIPAL_SETUP.md`
- `docs/FDPO_FABRIC_CURRENT_STATUS.md`
- `scripts/fabric/bootstrap-sp.sh`
- `scripts/fabric/validate-sp-access.sh`
- `src/unified_retriever.py`
- `src/api_server.py`
- `src/app/api/fabric/preflight/route.ts`

## Optional supporting artifacts

- `artifacts/Azure_Infra_Deployment_Overview_HQ_v3.pptx`
- `artifacts/THY_Fabric_Data_Service_Mapping_Readable.pptx`
- `artifacts/THY_Fabric_PilotBrief_Impact_Deck.pptx`
- `artifacts/Aviation_Demo_Context_Engineering_Retrieval_Detailed.pptx`

## Decision log (non-secret)

- 2026-02-23: Standardized memory contract.
  - Decision: Keep `AGENTS.md` policy-only and treat `docs/CODEX_MEMORY.md` as volatile factual memory.
  - Why: Reduce regressions from stale runtime values in policy files.
  - Sources: `AGENTS.md`, `docs/CODEX_MEMORY.md`.
  - Changed-from: `AGENTS.md` previously contained volatile Foundry runtime facts and rotation date.
- 2026-02-23: Normalized memory layout to canonical template.
  - Decision: Reordered and relabeled `docs/CODEX_MEMORY.md` sections to match `docs/CODEX_MEMORY_TEMPLATE.md`.
  - Why: Keep future updates deterministic and reduce structure drift across edits.
  - Sources: `docs/CODEX_MEMORY_TEMPLATE.md`, `docs/CODEX_MEMORY.md`.
  - Changed-from: Earlier file used equivalent content with partially different labels/order.
- 2026-02-23: Hardened chat transport against cold-start and proxy timeout failures.
  - Decision: Increased frontend proxy defaults to `BACKEND_REQUEST_TIMEOUT_MS=180000` and `CHAT_STREAM_TIMEOUT_MS=240000`, ensured deploy workflow syncs these values to App Service settings, and reduced worker recycle churn via `GUNICORN_MAX_REQUESTS=1000` / `GUNICORN_MAX_REQUESTS_JITTER=200` defaults.
  - Why: Prevent premature proxy aborts during backend cold-start and long-running retrieval/synthesis calls.
  - Sources: `src/app/api/chat/route.ts`, `.github/workflows/deploy-frontend.yaml`, `.github/workflows/deploy-backend.yaml`, `scripts/render-k8s-manifests.sh`, `Dockerfile.backend`.
  - Changed-from: Previous defaults were `BACKEND_REQUEST_TIMEOUT_MS=45000`, `CHAT_STREAM_TIMEOUT_MS=180000`, `GUNICORN_MAX_REQUESTS=200`, `GUNICORN_MAX_REQUESTS_JITTER=50`; workflow did not sync frontend timeout settings.
- 2026-02-23: Restored ACR configurability in backend deploy workflow.
  - Decision: Reinstated repository variable overrides for `AZURE_CONTAINER_REGISTRY_NAME` and `AZURE_CONTAINER_REGISTRY` in `.github/workflows/deploy-backend.yaml`, keeping safe defaults.
  - Why: Avoid deployment failures in environments that use a non-default ACR while preserving strict ACR target validation.
  - Sources: `.github/workflows/deploy-backend.yaml`, `tests/test_deployment_pipelines.py`.
  - Changed-from: Hard-coded `aviationragacr` / `aviationragacr.azurecr.io`.
