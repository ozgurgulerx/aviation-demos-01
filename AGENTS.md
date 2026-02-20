# Codex Workspace Memory Entry

Use `docs/CODEX_MEMORY.md` as the primary memory file for this repository.

When working on tenant, infrastructure, or Fabric tasks, load these files early:

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

Do not store secrets in memory files. Only keep non-secret identity, tenant, and infra context.

## Local Change Policy

Treat unexpected local tracked/untracked source changes as intentional by default and include them in the working set unless the user explicitly says to exclude them.
