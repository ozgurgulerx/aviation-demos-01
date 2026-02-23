# Codex Memory Template: Tenant, Infra, and Fabric Context

Last updated: YYYY-MM-DD

## Freshness metadata (non-secret)

- Runtime facts last verified: YYYY-MM-DD
- Runtime facts stale-after window: 7 days
- Infrastructure topology last verified: YYYY-MM-DD
- Infrastructure topology stale-after window: 30 days
- On stale/contradictory data: verify against repo truth and command output before risky execution.

## Identity and tenant context

- Primary admin identity:
- Tenant IDs/domains used:
- Subscription(s) in scope:
- Notes:

## Runtime infrastructure this solution is built on

- Frontend runtime:
- Backend runtime:
- Data/AI services:
- Security/observability:
- Provisioning source of truth:

Default runtime naming and layout:

- Resource group:
- Region:
- VNet:
- AKS/App runtime:
- Registry:
- Namespace:
- Databases:

Network defaults:

- VNet CIDR:
- Subnet(s):
- Service CIDR:

## Azure AI Foundry context (active project wiring)

- Foundry subscription:
- Foundry account:
- Foundry project resource:
- Project endpoint:

Deployments currently used:

- Chat:
- Voice/TTS:
- Realtime/audio:
- Embeddings (if applicable):

Runtime defaults expected:

- Backend env defaults:
- Frontend env defaults:

## Fabric integration context

Primary docs and scripts:

- `docs/FABRIC_SERVICE_PRINCIPAL_SETUP.md`
- `docs/FDPO_FABRIC_CURRENT_STATUS.md`
- `scripts/fabric/bootstrap-sp.sh`
- `scripts/fabric/validate-sp-access.sh`

Runtime integration in code:

- Env vars:
- API endpoints:
- Core modules:

Current tracked status:

- Workspace/capacity status:
- Blockers:

## CI/CD and deployment memory pointers

- Backend deploy workflow:
- Frontend deploy workflow:
- Infra health workflow:
- DB migration workflow:

## High-signal files to load first for codex memory

- `AGENTS.md`
- `docs/CODEX_MEMORY.md`
- `README.md`
- `CLAUDE.md`

## Optional supporting artifacts

- Add optional non-secret references here.

## Decision log (non-secret)

- YYYY-MM-DD: <short decision title>
  - Decision:
  - Why:
  - Sources:
  - Changed-from:
