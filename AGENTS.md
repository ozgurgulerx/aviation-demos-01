# Codex Workspace Operating Contract

Last reviewed: 2026-02-23

## Purpose and Scope

`AGENTS.md` is the stable behavior contract for agents working in this repo.

- Keep this file policy-focused and low-churn.
- Keep volatile runtime facts (deployment names, endpoint values, current status, rotation dates) in `docs/CODEX_MEMORY.md`.
- Do not store secrets in either file.

## Primary Memory File

Use `docs/CODEX_MEMORY.md` as the primary memory file for non-secret tenant, infrastructure, runtime, and Fabric context.
Use `docs/CODEX_MEMORY_TEMPLATE.md` as the canonical structure for future memory refactors/new memory files.

## Instruction Precedence

Within repository context, resolve instruction conflicts in this order:

1. Explicit user instruction in the active conversation.
2. `AGENTS.md` policy rules.
3. Task-specific docs (`README.md`, `CLAUDE.md`, runbooks, architecture docs).
4. `docs/CODEX_MEMORY.md` factual memory.
5. Current code/config/manifests as final implementation truth.

When sources conflict, call out the mismatch and proceed using the highest-precedence source.

## Required Context Load Order

For tenant, infrastructure, voice runtime, or Fabric tasks, load these files early and in this order:

1. `AGENTS.md`
2. `docs/CODEX_MEMORY.md`
3. `README.md`
4. `CLAUDE.md`
5. `docs/ARCHITECTURE.md`
6. `docs/RUNTIME_CUTOVER_RUNBOOK.md`
7. `scripts/provision-azure.sh`
8. `k8s/backend-configmap.yaml`
9. `k8s/backend-deployment.yaml`
10. `k8s/backend-service.yaml`
11. `docs/FABRIC_SERVICE_PRINCIPAL_SETUP.md`
12. `docs/FDPO_FABRIC_CURRENT_STATUS.md`
13. `scripts/fabric/bootstrap-sp.sh`
14. `scripts/fabric/validate-sp-access.sh`
15. `src/unified_retriever.py`
16. `src/api_server.py`
17. `src/app/api/fabric/preflight/route.ts`

## Freshness and Drift Rules

Use `docs/CODEX_MEMORY.md` freshness metadata before acting on memory:

- Runtime facts are stale after 7 days.
- Infrastructure topology facts are stale after 30 days.

If stale data or contradictions are detected:

1. Block risky execution until verification is completed.
2. Verify against repo truth (manifests, scripts, code, env defaults) and/or command output.
3. State the resolved value and source in the response.
4. Update or propose updates to `docs/CODEX_MEMORY.md`.

## Conflict Resolution Protocol

When values differ across docs or code:

1. List conflicting values with file paths.
2. Pick value by precedence rules.
3. Mark whether the decision is temporary or final.
4. Add a memory update note in `docs/CODEX_MEMORY.md` decision log.

## Decision Log and Memory Update Rules

For non-trivial tenant/infra/fabric/runtime decisions, add an entry to the decision log in `docs/CODEX_MEMORY.md` with:

- Date (`YYYY-MM-DD`)
- Decision
- Why
- Source files or command used for verification
- Changed-from value (when applicable)

Whenever runtime context changes are verified:

- Update `Last updated` in `docs/CODEX_MEMORY.md`.
- Update relevant section freshness markers.
- Keep only non-secret values.

## Regression-Prevention Preflight

Before finalizing tenant/infra/fabric/runtime changes, confirm:

1. Tenant/subscription/account alignment across docs and code.
2. Deployment and env var name consistency across manifests/routes/backend defaults.
3. Endpoint and API-version consistency between frontend and backend paths.
4. Role scope assumptions are explicit and non-secret.

If any preflight check fails, stop and surface the blocker.

## Fabric Regression Guardrails

For any Fabric datastore change (KQL, Graph, NoSQL, Fabric SQL), enforce:

1. Kusto endpoint path normalization:
   - Accept either cluster root (`https://<cluster>.kusto.fabric.microsoft.com`) or explicit query paths ending with `/v1/rest/query` or `/v2/rest/query`.
   - Reject duplicate query path suffixes (for example, `.../v2/rest/query/v1/rest/query`).
2. Auth mode precedence:
   - Prefer service principal credentials (`FABRIC_CLIENT_ID`, `FABRIC_CLIENT_SECRET`, `FABRIC_TENANT_ID`).
   - Treat `FABRIC_BEARER_TOKEN` as fallback only.
3. Fabric SQL mode policy:
   - Default to REST-first execution when `FABRIC_SQL_ENDPOINT` is configured.
   - Use TDS only as fallback when REST is unavailable and prerequisites are present.
4. Preflight quality bar:
   - Do not claim Fabric source health from endpoint reachability alone.
   - Require preflight query-readiness fields (`path_valid_for_runtime`, `query_ready`) to be healthy for configured sources.
5. Regression tests:
   - Any Fabric runtime path/mode/auth change must include or update tests covering endpoint normalization, auth behavior, and execution-mode selection.
6. Static bearer fallback discipline:
   - `FABRIC_BEARER_TOKEN` is ignored unless `ALLOW_STATIC_FABRIC_BEARER=true`.
   - If static bearer fallback is enabled, rotate token proactively and validate TTL/readiness in preflight.
   - For KQL/Graph/NoSQL, static bearer token audience must match the Kusto cluster endpoint (`https://<cluster>.kusto.fabric.microsoft.com`), not only `https://api.fabric.microsoft.com`.
7. Strict required-source semantics:
   - In strict mode, treat a required source as satisfied when at least one call for that source succeeds.
   - Do not fail strict mode solely because an additional call for the same source returned an error.
8. Fabric SQL container dependency baseline:
   - Backend image must include `pyodbc` and `msodbcsql18` (plus unixODBC libs) for TDS mode.
   - Pin a Microsoft-supported Debian base (currently `python:3.11-slim-bookworm`) to avoid package-feed drift on newer distros.
9. Frontend probe payload contract:
   - Frontend `/api/chat` validation requires `messages` array; runbooks/scripts should send both `message` and `messages` for compatibility.

## AKS Backend Image Platform Guardrail

For backend image build/deploy actions in this repository, enforce:

1. Platform lock:
   - `aviation-rag-backend` release images must be built and pushed as `linux/amd64`.
2. Build command discipline:
   - Use explicit platform pinning for backend builds (for example, `docker buildx build --platform linux/amd64 ...`).
   - Treat unqualified `docker build` on non-`amd64` hosts as out-of-policy for release images.
3. Manifest verification before rollout:
   - Verify ACR manifest metadata for the target tag/digest reports `os=linux` and `architecture=amd64`.
4. Block on mismatch:
   - If target image is not `linux/amd64`, stop deployment and rebuild with the correct platform.
5. Runtime compatibility note:
   - Current AKS nodepool runtime architecture is `amd64`; keep backend image architecture aligned.

## Subscription Guardrail

For any Azure action in this repository, use only this hardcoded tenant/account/subscription target:

- Account: `admin@MngEnvMCAP705508.onmicrosoft.com`
- Tenant ID: `52095a81-130f-4b06-83f1-9859b2c73de6`
- Subscription ID: `6a539906-6ce2-4e3b-84ee-89f701de18d8`

Treat any other tenant/subscription/account as out of policy. If docs, memory, scripts, CLI context, workflow values, or environment values conflict with this guardrail, stop and surface the mismatch before proceeding.

## Local Change Policy

Treat unexpected local tracked/untracked source changes as intentional by default and include them in the working set unless the user explicitly says to exclude them.

## Memory Hygiene Rules

Do:

- Keep stable identifiers and validated non-secret topology.
- Keep concise context plus source pointers.
- Prefer dated entries over undated notes.

Do not:

- Store secrets, tokens, passwords, keys, or connection strings.
- Mix one-off transient observations into stable policy sections.
- Keep stale values without freshness dates.
