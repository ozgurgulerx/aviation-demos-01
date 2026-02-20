# Fabric Service Principal Setup (One MTC Tenant)

This runbook configures Microsoft Fabric access through a service principal so local automation can work without changing the current Azure CLI user session.

## Goal

Enable this project to call Fabric REST APIs with:

- `FABRIC_TENANT_ID`
- `FABRIC_CLIENT_ID`
- `FABRIC_CLIENT_SECRET`
- `FABRIC_WORKSPACE_ID` (recommended)

## 1) Create app registration in One MTC tenant

You can do this either with the included bootstrap script or manually.

### Option A (recommended): bootstrap script

```bash
scripts/fabric/bootstrap-sp.sh \
  --tenant-id "<one-mtc-tenant-id>" \
  --display-name "aviation-rag-fabric-sp" \
  --env-out ".fabric-sp.env"
```

The script creates/reuses:

- App registration
- Service principal
- New client secret

Then it prints exported env vars and writes them to `--env-out` if provided.

### Option B: manual portal steps

Use Microsoft Entra admin center in **One MTC - Prod** tenant:

1. Go to `App registrations` -> `New registration`.
2. Name: `aviation-rag-fabric-sp` (or your preferred name).
3. Account type: `Single tenant`.
4. Create.

Capture:

- Application (client) ID -> `FABRIC_CLIENT_ID`
- Directory (tenant) ID -> `FABRIC_TENANT_ID`

## 2) Create client secret

1. Open the app registration.
2. Go to `Certificates & secrets`.
3. Create a `Client secret`.
4. Copy the **secret value** immediately.

Store it as `FABRIC_CLIENT_SECRET`.

## 3) Create service principal object

From Azure CLI (logged-in identity must have directory permissions in One MTC tenant):

```bash
az ad sp create --id "<FABRIC_CLIENT_ID>"
```

If this returns `already exists`, continue.

## 4) Fabric tenant admin settings

In Fabric Admin Portal (One MTC tenant), enable service principal usage:

1. `Tenant settings` -> enable service principals for Fabric APIs.
2. Prefer scope by security group (recommended), not tenant-wide.
3. Add the app/service principal (or group containing it) to allowed principals.

Without this step, Fabric API calls typically fail with `403 Forbidden`.

## 5) Grant workspace access

In the target Fabric workspace:

1. Open `Manage access`.
2. Add the service principal (or its security group).
3. Assign role:
   - `Viewer` for read-only checks.
   - `Contributor` for create/update operations.

Capture workspace ID from workspace URL and store as `FABRIC_WORKSPACE_ID`.

## 6) Configure local environment

Add these to your local environment (or secret manager):

```bash
export FABRIC_TENANT_ID="<tenant-guid>"
export FABRIC_CLIENT_ID="<app-guid>"
export FABRIC_CLIENT_SECRET="<secret-value>"
export FABRIC_WORKSPACE_ID="<workspace-guid>"
export FABRIC_BASE_URL="https://api.fabric.microsoft.com"
```

## 7) Validate access from this repo

Use the built-in script:

```bash
scripts/fabric/validate-sp-access.sh
```

Optional checks:

```bash
FABRIC_WORKSPACE_NAME="My workspace" scripts/fabric/validate-sp-access.sh
FABRIC_WORKSPACE_ID="<workspace-guid>" scripts/fabric/validate-sp-access.sh
```

Expected result:

- Token acquisition succeeds.
- Workspace list is returned.
- Optional workspace ID/name check passes.

## Troubleshooting

- `invalid_client`: wrong client ID/secret or app not in tenant.
- `unauthorized_client`: tenant policy blocks app auth.
- `403 Forbidden`: Fabric admin setting or workspace access missing.
- `404` for workspace: incorrect workspace ID or wrong tenant context.
