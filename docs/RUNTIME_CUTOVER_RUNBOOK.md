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
- Freeze deploys to `main` during migration window.
- Prepare GitHub secrets (target values):
  - `AZURE_CLIENT_ID`, `AZURE_TENANT_ID`, `AZURE_SUBSCRIPTION_ID`
  - `AZURE_OPENAI_API_KEY`, `AZURE_SEARCH_ADMIN_KEY`, `PGPASSWORD`
  - Optional: `APPLICATIONINSIGHTS_CONNECTION_STRING`, `FABRIC_BEARER_TOKEN`
- Prepare GitHub variables (target values):
  - `AZURE_RESOURCE_GROUP`, `AZURE_WEBAPP_NAME`
  - `AKS_RESOURCE_GROUP`, `AKS_CLUSTER`, `AKS_NAMESPACE`
  - `AZURE_CONTAINER_REGISTRY_NAME`, `AZURE_CONTAINER_REGISTRY`
  - `BACKEND_URL`, `PII_ENDPOINT`, `PII_CONTAINER_ENDPOINT`
  - `PG_SERVER_NAME`, `PG_RESOURCE_GROUP`, `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`
  - `AZURE_OPENAI_ENDPOINT`, `AZURE_SEARCH_ENDPOINT`

## 2) Provision target runtime infra

Example:

```bash
SUBSCRIPTION_ID="<target-subscription-id>" \
RESOURCE_GROUP="rg-aviation-rag" \
LOCATION="westeurope" \
ACR_NAME="aviationragacr" \
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
