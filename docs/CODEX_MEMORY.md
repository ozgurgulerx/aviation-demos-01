# Codex Memory: Tenant, Infra, and Fabric Context

Last updated: 2026-02-20

## Identity and tenant context

- Primary admin identity to keep in context: `admin@MngEnvMCAP705508.onmicrosoft.com` (user-provided).
- Additional tenant context in repo:
  - FDPO tenant: `16b3c013-d300-468d-ac64-7eda0820b6d3` (`fdpo.onmicrosoft.com`) from `docs/FDPO_FABRIC_CURRENT_STATUS.md`.
  - One MTC tenant is referenced in `docs/FABRIC_SERVICE_PRINCIPAL_SETUP.md` for Fabric service principal bootstrap.

## Runtime infrastructure this solution is built on

- Frontend: Next.js 15 on Azure App Service.
- Backend: Flask/Python runtime on AKS.
- Data and AI: PostgreSQL Flexible Server, Azure OpenAI, Azure AI Search.
- Security and ops: PII endpoint/container path, OpenTelemetry -> Azure Monitor/Application Insights.
- Infra provisioning source of truth: `scripts/provision-azure.sh`.

Default runtime naming and layout (from `scripts/provision-azure.sh`):

- Resource group: `rg-aviation-rag`
- Region: `westeurope`
- VNet: `vnet-aviation-rag`
- AKS: `aks-aviation-rag`
- ACR: `aviationragacr`
- Frontend app service plan: `plan-aviation-rag-frontend`
- Frontend web app: `aviation-rag-frontend`
- Kubernetes namespace: `aviation-rag`
- PostgreSQL server: `aviationragpg`
- PostgreSQL database: `aviationrag`
- Read-only DB user target: `aviationrag_readonly`

Network defaults:

- VNet CIDR: `10.0.0.0/16`
- AKS subnet: `10.0.0.0/22`
- App Service subnet: `10.0.4.0/24`
- Private endpoint subnet: `10.0.5.0/24`
- AKS service CIDR: `10.1.0.0/16`

## Azure AI Foundry context (active project wiring)

- Foundry subscription: `a20bc194-9787-44ee-9c7f-7c3130e651b6` (`MCAPS-Hybrid-REQ-102171-2024-ozgurguler`).
- Foundry account (AIServices): `ai-eastus2hubozguler527669401205` in `rg-openai` (`eastus2`).
- Foundry project resource: `ai-eastus2hubozguler527-project`.
- Project endpoint used in app env: `https://ai-eastus2hubozguler527669401205.services.ai.azure.com/api/projects/ai-eastus2hubozguler527-project`.

Deployments created on 2026-02-20 for this repo's chat/voice UX:

- `aviation-chat-gpt5-mini` -> `gpt-5-mini` (`2025-08-07`), `GlobalStandard`, capacity `50`.
- `aviation-voice-gpt4o-audio` -> `gpt-4o-audio-preview` (`2024-12-17`), `GlobalStandard`, capacity `20`.
- `aviation-voice-tts` -> `gpt-4o-mini-tts` (`2025-12-15`), `GlobalStandard`, capacity `20`.
- Frontend voice route defaults to token auth (`AZURE_OPENAI_AUTH_MODE=token`) and supports:
  - Entra service principal (`AZURE_OPENAI_TENANT_ID`, `AZURE_OPENAI_CLIENT_ID`, `AZURE_OPENAI_CLIENT_SECRET`)
  - Managed identity (`AZURE_OPENAI_MANAGED_IDENTITY_CLIENT_ID`, optional for user-assigned MI)

## Fabric integration context

Primary Fabric docs and scripts:

- `docs/FABRIC_SERVICE_PRINCIPAL_SETUP.md`
- `docs/FDPO_FABRIC_CURRENT_STATUS.md`
- `scripts/fabric/bootstrap-sp.sh`
- `scripts/fabric/validate-sp-access.sh`

Fabric runtime integration in code:

- Env vars used by backend:
  - `FABRIC_KQL_ENDPOINT`
  - `FABRIC_GRAPH_ENDPOINT`
  - `FABRIC_NOSQL_ENDPOINT`
  - `FABRIC_BEARER_TOKEN`
- Preflight endpoints:
  - Backend: `GET /api/fabric/preflight` in `src/api_server.py`
  - Frontend proxy: `src/app/api/fabric/preflight/route.ts`
- Core retrieval logic: `src/unified_retriever.py`

Current tracked Fabric status (from `docs/FDPO_FABRIC_CURRENT_STATUS.md`, last updated 2026-02-18):

- Workspace `fdpo-access-check-ws-20260218-171328` exists (`6bf216a8-e74a-44cc-9c0d-6f141c82d487`).
- Fabric F2 capacity `fdpofabricf20218202320` is active in `rg-fabric` (UK South).
- Blocking issue: F2 capacity is not visible in Fabric capacities for current identity; admin-side role/tenant settings are still required.

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
