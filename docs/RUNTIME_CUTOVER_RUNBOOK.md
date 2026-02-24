# Runtime Cutover Runbook (Big-Bang)

## Scope

This runbook covers runtime migration only:
- Frontend (App Service)
- Backend (AKS)
- Networking (VNet/subnets/private endpoint wiring)
- CI/CD identity and workflows
- Essential runtime dependencies (ACR, PostgreSQL connectivity, PII endpoint, OpenAI/Search runtime endpoints)

Data platform migration is handled separately.

## 1) Pre-cutover checklist

- Confirm target login and subscription:
  - `az account show --query "{user:user.name,tenantId:tenantId,id:id}" -o table`
  - Expected hard guardrail:
    - user: `admin@MngEnvMCAP705508.onmicrosoft.com`
    - tenantId: `52095a81-130f-4b06-83f1-9859b2c73de6`
    - subscriptionId: `6a539906-6ce2-4e3b-84ee-89f701de18d8`
- Validate local Azure and kubectl targeting before any verification:
  - `./scripts/aks/use-deploy-target-context.sh`
- Validate tenant-lock end-to-end (account + resources + live AKS guardrail config):
  - `./scripts/validate-tenant-lock.sh`
- Freeze deploys to `main` during migration window.
- Prepare GitHub secrets (target values):
  - `AZURE_CLIENT_ID`
  - `AZURE_OPENAI_API_KEY`, `AZURE_SEARCH_ADMIN_KEY`, `PGPASSWORD`
  - Optional: `APPLICATIONINSIGHTS_CONNECTION_STRING`, `FABRIC_BEARER_TOKEN`
- Prepare GitHub variables (target values):
  - `AZURE_RESOURCE_GROUP`, `AZURE_WEBAPP_NAME` (only if intentionally changing hardcoded workflow target)
  - `AKS_RESOURCE_GROUP`, `AKS_CLUSTER`, `AKS_NAMESPACE` (only if intentionally changing hardcoded workflow target)
  - `AZURE_CONTAINER_REGISTRY_NAME`, `AZURE_CONTAINER_REGISTRY` (only if intentionally changing hardcoded workflow target)
  - `BACKEND_URL`, `PII_ENDPOINT`, `PII_CONTAINER_ENDPOINT`
  - `PG_SERVER_NAME`, `PG_RESOURCE_GROUP`, `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`
  - `AZURE_OPENAI_ENDPOINT`, `AZURE_SEARCH_ENDPOINT`

## 2) Provision target runtime infra

Example:

```bash
SUBSCRIPTION_ID="<target-subscription-id>" \
RESOURCE_GROUP="rg-aviation-rag" \
LOCATION="westeurope" \
ACR_NAME="avrag705508acr" \
ACR_RESOURCE_GROUP="rg-aviation-rag" \
PG_SERVER_RG="<postgres-rg>" \
PG_SERVER="<postgres-server>" \
PG_DATABASE="aviationrag" \
PG_READONLY_USER="aviationrag_readonly" \
AZURE_OPENAI_ENDPOINT="https://<openai>.openai.azure.com/" \
AZURE_SEARCH_ENDPOINT="https://<search>.search.windows.net" \
PII_ENDPOINT="http://<pii-host>:5000" \
PII_CONTAINER_ENDPOINT="http://<pii-host>:5000" \
GITHUB_ORG="ozgurgulerx" \
GITHUB_REPO="aviation-demos-01" \
./scripts/provision-azure.sh
```

## 3) Deploy backend and frontend in target

- Run workflow: `Deploy Backend to AKS`
- Run workflow: `Deploy Frontend to App Service`
- Run workflow: `Infrastructure Health Check`

## 4) Big-bang cutover

- Disable source runtime traffic.
- Confirm target endpoints are reachable.
- Execute smoke tests:
  - `GET /health` on backend LB
  - `GET /api/health` on frontend
  - `POST /api/pii` on frontend
  - Chat request/streaming through `/api/chat`

## 5) Rollback

- Re-enable source frontend/backend.
- Re-point CI/CD secrets and variables to source values.
- Re-run source deploy workflows.

## 6) Post-cutover hardening

- Rotate all runtime secrets.
- Set App Service `httpsOnly=true`.
- Remove stale source runtime resources after rollback window closes.
