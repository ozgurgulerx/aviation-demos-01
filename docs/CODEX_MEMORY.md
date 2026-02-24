# Codex Memory: Tenant, Infra, and Fabric Context

Last updated: 2026-02-24

## Freshness metadata (non-secret)

- Runtime facts last verified: 2026-02-24
- Runtime facts stale-after window: 7 days
- Infrastructure topology last verified: 2026-02-24
- Infrastructure topology stale-after window: 30 days
- On stale/contradictory data: verify against repo truth and command output before risky execution.
- Structure reference for future updates: `docs/CODEX_MEMORY_TEMPLATE.md`.

## Identity and tenant context

- Primary admin identity: `admin@MngEnvMCAP705508.onmicrosoft.com` (user-provided)
- Hard guardrail tenant/domain:
  - Tenant ID: `52095a81-130f-4b06-83f1-9859b2c73de6`
  - Domain: `MngEnvMCAP705508.onmicrosoft.com`
- Subscription(s) in scope:
  - Runtime/deploy subscription: `6a539906-6ce2-4e3b-84ee-89f701de18d8` (`ME-MngEnvMCAP705508-ozgurguler-1`)
- Out-of-policy context:
  - Any tenant/subscription outside the hard guardrail above must be treated as drift and blocked.
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
- AKS node architecture (verified 2026-02-23): `amd64`
- Registry: `avrag705508acr`
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

## Azure OpenAI and search runtime context

- Subscription: `6a539906-6ce2-4e3b-84ee-89f701de18d8` (`ME-MngEnvMCAP705508-ozgurguler-1`)
- Azure OpenAI account: `aoaiaviation705508` (`rg-openai`, `swedencentral`)
- Azure OpenAI endpoint: `https://swedencentral.api.cognitive.microsoft.com/`
- Azure AI Search service: `aisearchozguler` (`rg-openai`, `swedencentral`)
- Azure AI Search endpoint: `https://aisearchozguler.search.windows.net`
- Out-of-policy (historical only): `ai-eastus2hubozguler527669401205` and the corresponding Foundry project endpoint are not in the guardrail tenant/subscription and must not be used for runtime/deploy.

Runtime defaults expected:

- Backend:
  - `AZURE_OPENAI_DEPLOYMENT_NAME=gpt-5-nano`
  - `AZURE_SEARCH_ENDPOINT=https://aisearchozguler.search.windows.net`
- Frontend:
  - `AZURE_OPENAI_ENDPOINT=https://swedencentral.api.cognitive.microsoft.com/`
  - `AZURE_OPENAI_AUTH_MODE=api-key`
  - `AZURE_OPENAI_VOICE_DEPLOYMENT_NAME=aviation-voice-tts`
  - `AZURE_OPENAI_VOICE_MODEL=gpt-4o-mini-tts`
  - `AZURE_OPENAI_VOICE_API_VERSION=2025-03-01-preview`
  - `AZURE_OPENAI_VOICE_TURKISH=alloy`
  - `AZURE_OPENAI_VOICE_ENGLISH=alloy`

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

Fabric runtime guardrails (verified policy):

- Kusto endpoint values must be either cluster root or explicit `/v1/rest/query` or `/v2/rest/query` paths.
- Fabric auth should use SP credentials first (`FABRIC_CLIENT_ID`/`FABRIC_CLIENT_SECRET`/`FABRIC_TENANT_ID`), with static `FABRIC_BEARER_TOKEN` as fallback only.
- Fabric SQL execution policy is REST-first when `FABRIC_SQL_ENDPOINT` is configured.
- Fabric preflight health claims require query-readiness checks (`path_valid_for_runtime`, `query_ready`), not only endpoint reachability.

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

- 2026-02-24: Rolled backend to latest local `main` commits after additional auth-hardening changes.
  - Decision: Built and pushed backend image `avrag705508acr.azurecr.io/aviation-rag-backend:backend-3177058-manual-20260224100916` from `HEAD` (`3177058`) using `docker buildx --platform linux/amd64`, verified ACR manifest platform (`os=linux`, `architecture=amd64`), and rolled AKS deployment `aviation-rag-backend` to the new tag. Kept frontend on the already-deployed `bd74d4f` build because no newer frontend file changes existed after that commit.
  - Why: Deploy the latest backend fixes (`cc278eb`, `3177058`) to production while preserving platform and tenant/subscription guardrails.
  - Sources: `git log --oneline -n 3`, `./scripts/aks/use-deploy-target-context.sh`, `./scripts/validate-tenant-lock.sh`, `docker buildx build --platform linux/amd64 -f Dockerfile.backend ... --push`, `az acr repository show-manifests --name avrag705508acr --repository aviation-rag-backend --detail --orderby time_desc -o json`, `kubectl -n aviation-rag set image deployment/aviation-rag-backend ...`, `kubectl -n aviation-rag rollout status deployment/aviation-rag-backend --timeout=900s`, `kubectl -n aviation-rag get deployment aviation-rag-backend -o jsonpath=...`, `curl http://127.0.0.1:18081/health`, `curl http://127.0.0.1:18081/api/fabric/preflight`.
  - Changed-from: Backend image `avrag705508acr.azurecr.io/aviation-rag-backend:backend-013c513-manual-20260224090444`.
- 2026-02-24: Rolled local `HEAD` to backend AKS and frontend App Service with baseline preflight gate.
  - Decision: Built and pushed backend image `avrag705508acr.azurecr.io/aviation-rag-backend:backend-583946c-manual-20260224074650` via `docker buildx --platform linux/amd64`, updated AKS deployment to that tag, synced frontend App Service runtime settings (including `CHAT_STREAM_TIMEOUT_MS=240000`), and deployed `/tmp/app.zip` through OneDeploy.
  - Why: Deploy current local branch commits while preserving subscription guardrails and AKS architecture compatibility.
  - Sources: `az account show --query "{user:user.name,tenantId:tenantId,id:id}" -o json`, `./scripts/aks/use-deploy-target-context.sh`, `./scripts/validate-tenant-lock.sh`, `docker buildx build --platform linux/amd64 -f Dockerfile.backend ... --push`, `az acr manifest list-metadata --registry avrag705508acr --name aviation-rag-backend --orderby time_desc --top 5 -o json`, `kubectl -n aviation-rag rollout status deployment/aviation-rag-backend --timeout=900s`, `az webapp deploy --resource-group rg-aviation-rag --name aviation-rag-frontend-705508 --src-path /tmp/app.zip --type zip --async true --restart true`, `az webapp log deployment list --resource-group rg-aviation-rag --name aviation-rag-frontend-705508 --query '[0].{status:status,...}' -o json`, `curl https://aviation-rag-frontend-705508.azurewebsites.net/api/health`, `curl https://aviation-rag-frontend-705508.azurewebsites.net/api/fabric/preflight`.
  - Changed-from: Backend image `avrag705508acr.azurecr.io/aviation-rag-backend:backend-3f1f4de-manual-20260224082048`; prior frontend OneDeploy ended `2026-02-24T07:09:23Z`.
- 2026-02-23: Disabled Gunicorn control socket for read-only AKS pods and completed live frontend/backend rollout.
  - Decision: Added `--no-control-socket` to backend Gunicorn startup command in both `Dockerfile.backend` and `k8s/backend-deployment.yaml`, then rolled AKS backend to image tag `fix-risk-stream-20260223200655` and deployed frontend via App Service zip deploy.
  - Why: Gunicorn `25.1.0` control socket defaults to `gunicorn.ctl` under `/app`; with `readOnlyRootFilesystem=true` this caused startup probe timeouts (`Control server error: [Errno 30] Read-only file system`) and stalled backend rollout.
  - Sources: `kubectl -n aviation-rag describe pod ...`, `kubectl -n aviation-rag logs deployment/aviation-rag-backend --tail=60`, `kubectl -n aviation-rag rollout status deployment/aviation-rag-backend --timeout=600s`, `kubectl -n aviation-rag get deployment aviation-rag-backend -o jsonpath=...`, `az webapp deploy --resource-group rg-aviation-rag --name aviation-rag-frontend-705508 --src-path /tmp/app.zip --type zip --async true --restart true`, live SSE probes through `https://aviation-rag-frontend-705508.azurewebsites.net/api/chat`.
  - Changed-from: Backend Gunicorn command did not disable control socket; rollout to `fix-risk-stream-20260223200655` stalled with unready pods.
- 2026-02-23: Hard-locked backend image architecture for AKS compatibility.
  - Decision: Added policy guardrail that `aviation-rag-backend` release images must be `linux/amd64`, with explicit build-platform pinning and manifest verification before rollout.
  - Why: Prevent wrong-architecture image pulls during AKS deployment/boot and avoid rollout failures on `amd64` node pools.
  - Sources: `kubectl get nodes -o jsonpath='{range .items[*]}{.metadata.name}{" arch="}{.status.nodeInfo.architecture}{"\\n"}{end}'`, `kubectl get deploy aviation-rag-backend -n aviation-rag -o jsonpath='{.spec.template.spec.containers[0].image}{"\\n"}'`, `az acr manifest list-metadata --registry avrag705508acr --name aviation-rag-backend --orderby time_desc --top 3 -o json`.
  - Changed-from: No explicit image-platform lock in `AGENTS.md`.
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
- 2026-02-23: Hardened KQL/SQL/Fabric SQL execution validation and fallback behavior.
  - Decision: Added KQL sanitize+validate flow (including schema-aware table/column checks and unsupported function guard), added SQL column-level schema validation pre-execution, and changed FABRIC_SQL capability handling to use TDS only when `pyodbc`/driver prerequisites are available with automatic REST fallback when configured.
  - Why: Prevent runtime failures caused by malformed planner-generated KQL/SQL and missing TDS dependencies (`pyodbc`) during live demos.
  - Sources: `src/unified_retriever.py`, `src/plan_executor.py`, `src/query_writers.py`, `tests/test_datastore_comprehensive.py`, `tests/test_source_execution_policy.py`.
  - Changed-from: KQL accepted malformed semicolon-before-pipe patterns until validation failure, SQL validated table existence but not column existence, FABRIC_SQL attempted TDS solely based on env and failed with `fabric_sql_tds_error` when `pyodbc` was unavailable.
- 2026-02-23: Hard-locked tenant/subscription guardrail to MngEnvMCAP705508.
  - Decision: Enforced account `admin@MngEnvMCAP705508.onmicrosoft.com`, tenant `52095a81-130f-4b06-83f1-9859b2c73de6`, and subscription `6a539906-6ce2-4e3b-84ee-89f701de18d8` as the only in-policy runtime/deploy target; updated deployment workflows to validate hardcoded tenant/subscription constants.
  - Why: Eliminate target drift and repeated back-and-forth shifts across tenants/subscriptions.
  - Sources: `AGENTS.md`, `.github/workflows/deploy-backend.yaml`, `.github/workflows/deploy-frontend.yaml`, `az account show --query "{name:name,id:id,tenantId:tenantId,user:user.name}" -o json`.
  - Changed-from: Prior memory/workflow context allowed fallback/variable-driven targeting and documented `a20bc194-9787-44ee-9c7f-7c3130e651b6`.
- 2026-02-23: Removed cross-tenant runtime endpoint references.
  - Decision: Standardized runtime endpoint memory to in-tenant Azure OpenAI (`aoaiaviation705508`) and in-tenant Search (`aisearchozguler`), and marked eastus2 Foundry project wiring as out-of-policy historical context.
  - Why: Prevent runtime routing back to tenant `16b3c013-d300-468d-ac64-7eda0820b6d3` and subscription `a20bc194-9787-44ee-9c7f-7c3130e651b6`.
  - Sources: `az cognitiveservices account show -g rg-openai -n aoaiaviation705508`, `az search service show -g rg-openai -n aisearchozguler`, `az resource list --subscription a20bc194-9787-44ee-9c7f-7c3130e651b6`.
  - Changed-from: Memory listed `ai-eastus2hubozguler527669401205` as active project wiring.
- 2026-02-23: Verified live datastore queryability matrix from deployed frontend `/api/chat`.
  - Decision: Mark SQL and NOSQL as currently queryable in live mode; mark KQL, GRAPH, and FABRIC_SQL as not healthy for strict live queries.
  - Why: Live probes with `requiredSources=[SOURCE]`, `sourcePolicy=exact`, and `failurePolicy=strict` returned source-level runtime failures for KQL/GRAPH/FABRIC_SQL while SQL and NOSQL returned successful live rows.
  - Sources: `curl https://aviation-rag-frontend-705508.azurewebsites.net/api/fabric/preflight`, `curl https://aviation-rag-frontend-705508.azurewebsites.net/api/chat` (SSE `source_call_*` events), `az account show --query "{user:user.name,tenantId:tenantId,id:id}" -o json`.
  - Changed-from: Assumed post-fix behavior that all datastore paths were healthy.
- 2026-02-23: Added runtime source-capability and tenant-guardrail enforcement in backend.
  - Decision: Implemented backend capability registry (`healthy/degraded/unavailable`) per source, identity guardrail checks tied to expected runtime account/tenant/subscription envs, and preflight payload expansion with `identity_guardrail`, `source_capabilities`, and baseline source status.
  - Why: Prevent repeated regressions from endpoint/dependency drift by making source readiness explicit and blockable before query execution.
  - Sources: `src/unified_retriever.py`, `src/api_server.py`, `k8s/backend-configmap.yaml`, `scripts/render-k8s-manifests.sh`.
  - Changed-from: Source-mode checks were distributed and lacked centralized capability state + identity guardrail visibility.
- 2026-02-23: Added post-deploy baseline datastore gate in backend workflow.
  - Decision: Added a deploy workflow step that calls backend `/api/fabric/preflight` and fails deployment when baseline sources (`SQL`, `NOSQL`) are unavailable.
  - Why: Ensure regressions are caught in deployment flow instead of rediscovered during runtime demos.
  - Sources: `.github/workflows/deploy-backend.yaml`, `src/unified_retriever.py`.
  - Changed-from: Deploy workflow validated rollout/image, but did not gate on datastore baseline readiness.
- 2026-02-23: Fixed KQL/Graph query-path normalization for Fabric Kusto endpoints.
  - Decision: Normalized Kusto endpoint handling so explicit `/v1/rest/query` or `/v2/rest/query` paths are used as-is, cluster roots derive `/v1/rest/query`, and duplicated query segments are rejected.
  - Why: Prevent runtime 404 failures caused by malformed endpoint concatenation (for example `.../v2/rest/query/v1/rest/query`).
  - Sources: `src/unified_retriever.py`, `tests/test_source_execution_policy.py`, live endpoint probe logs.
  - Changed-from: Runtime always appended `/v1/rest/query` regardless of configured endpoint path.
- 2026-02-23: Standardized Fabric SQL to REST-first execution policy.
  - Decision: Fabric SQL now prioritizes REST execution when `FABRIC_SQL_ENDPOINT` is configured; TDS is fallback only when REST is unavailable.
  - Why: Avoid recurring production failures from missing `pyodbc`/ODBC runtime dependencies.
  - Sources: `src/unified_retriever.py`, `tests/test_source_execution_policy.py`, `Dockerfile.backend`.
  - Changed-from: TDS path was preferred whenever `FABRIC_SQL_SERVER`/`FABRIC_SQL_DATABASE` were set.
- 2026-02-23: Expanded Fabric preflight to include execution-readiness signals.
  - Decision: Added explicit auth mode/readiness and endpoint query-readiness/path-validity fields in preflight output.
  - Why: Endpoint reachability-only checks were insufficient and masked real runtime failures.
  - Sources: `src/unified_retriever.py`, `tests/test_datastore_comprehensive.py`, `tests/test_source_execution_policy.py`.
  - Changed-from: Preflight primarily reported reachability (`reachable_http_*`) without explicit query execution readiness flags.
- 2026-02-23: Implemented token-freshness auth handling and re-aligned runtime fallback behavior.
  - Decision: Added token TTL/static-bearer policy handling for Fabric and voice token acquisition, enforced REST-first `FABRIC_SQL_MODE=auto`, and restored GRAPH fallback behavior when live endpoint is missing while keeping source capability and preflight auth diagnostics.
  - Why: Prevent stale-token regressions without reintroducing prior route-execution breakages (`kql_multiple_statements_not_allowed`, missing TDS deps, graph fallback regressions).
  - Sources: `src/unified_retriever.py`, `src/intent_graph_provider.py`, `src/app/api/voice/speak/route.ts`, `tests/test_source_execution_policy.py`, `tests/test_datastore_comprehensive.py`.
  - Changed-from: `FABRIC_SQL_MODE=auto` selected TDS first and GRAPH no-endpoint path was hard-blocked.
- 2026-02-23: Restored Fabric SQL TDS runtime dependencies in production image.
  - Decision: Added `pyodbc` to `requirements.txt`, installed `msodbcsql18` + unixODBC libs in backend image, and pinned backend base image to `python:3.11-slim-bookworm` for Microsoft ODBC feed compatibility.
  - Why: `python:3.11-slim` resolved to Debian 13 (`trixie`) where the current Microsoft package-feed path/signing failed; runtime lacked `pyodbc` and blocked Fabric SQL mode.
  - Sources: `Dockerfile.backend`, `requirements.txt`, `tests/test_deployment_pipelines.py`, `docker buildx build --platform linux/amd64 -f Dockerfile.backend ...`.
  - Changed-from: Runtime image had no SQL ODBC driver and failed preflight with `pyodbc_unavailable`.
- 2026-02-23: Fixed static bearer token audience for Fabric Kusto-backed sources.
  - Decision: Rotated `FABRIC_BEARER_TOKEN` to a Kusto-cluster-scoped token (`https://<cluster>.kusto.fabric.microsoft.com`) instead of only `https://api.fabric.microsoft.com`, then rolled backend deployment.
  - Why: `api.fabric` token produced `401` on Eventhouse query endpoint while cluster-scoped token produced query-level `400` (auth accepted).
  - Sources: `az account get-access-token --resource https://api.fabric.microsoft.com`, `az account get-access-token --resource https://trd-rjvjgwebssdwhxbdy0.z2.kusto.fabric.microsoft.com`, `curl https://trd-rjvjgwebssdwhxbdy0.z2.kusto.fabric.microsoft.com/v2/rest/query ...`, `kubectl -n aviation-rag patch secret backend-secrets ...`, `curl https://aviation-rag-frontend-705508.azurewebsites.net/api/fabric/preflight`.
  - Changed-from: Static bearer rotation used `api.fabric` audience and regressed KQL/Graph/NoSQL to `reachable_http_401`.
- 2026-02-23: Updated datastore combo runbook payload contract for frontend `/api/chat`.
  - Decision: Updated `scripts/19_test_datastore_combinations.py` to include `messages` array in request body while keeping `message` for backward compatibility.
  - Why: Frontend validation now requires `messages`; legacy payloads caused false-negative matrix runs with HTTP 400 before source execution.
  - Sources: `scripts/19_test_datastore_combinations.py`, run output in `artifacts/datastore_combo_results_latest.json`.
  - Changed-from: Script sent only `message` and produced request-shape failures.
- 2026-02-23: Live datastore status after ODBC + token-audience fixes.
  - Decision: Mark `SQL`, `NOSQL`, `KQL`, and `GRAPH` as queryable in strict exact-source probes; keep `FABRIC_SQL` as failing due warehouse login/authz (`SQL Server 18456`) despite TDS capability pass.
  - Why: Source probes returned successful `source_call_done` rows for SQL/NOSQL/KQL/GRAPH; FABRIC_SQL returned contract failure with login error from ODBC driver.
  - Sources: `curl https://aviation-rag-frontend-705508.azurewebsites.net/api/fabric/preflight`, live SSE source probes via `POST https://aviation-rag-frontend-705508.azurewebsites.net/api/chat`, `kubectl -n aviation-rag logs ...`.
  - Changed-from: Earlier status marked KQL/GRAPH as unhealthy and FABRIC_SQL blocked by missing `pyodbc`.
