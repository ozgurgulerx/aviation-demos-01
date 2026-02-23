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
