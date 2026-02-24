# Predictive Optimization Deployment Runbook (Dark Launch)

Last updated: 2026-02-24

## Scope

Deploy predictive optimization as an additive capability with no impact on existing chat operations.

This runbook covers:
1. Postgres mirror table setup
2. One-time Fabric training/scoring (manual notebook flow)
3. Backend/Frontend dark launch with feature flags
4. Validation and rollback

## Feature flags

### Backend
- `ENABLE_PREDICTIVE_API` (default: `false`)
- `PREDICTIVE_MIRROR_SCHEMA` (default: `demo`)
- `PREDICTIVE_DATA_MAX_AGE_HOURS` (default: `48`)
- `PREDICTIVE_DEFAULT_WINDOW_HOURS` (default: `6`)
- `PREDICTIVE_DEFAULT_LIMIT` (default: `100`)

### Frontend
- `NEXT_PUBLIC_ENABLE_PREDICTIVE_PANEL` (default: `false`)
- `NEXT_PUBLIC_ENABLE_PREDICTIVE_ACTIONS_TAB` (default: `false`)

## 1) Pre-deploy checks

1. Validate tenant/subscription lock:
   - `./scripts/validate-tenant-lock.sh`
2. Ensure AKS context is the deploy target:
   - `./scripts/aks/use-deploy-target-context.sh`
3. Confirm existing endpoints are healthy:
   - backend: `GET /health`
   - frontend: `GET /api/health`
   - chat: `POST /api/chat`

## 2) Create predictive mirror tables

Use local-safe wrapper (recommended):

```bash
cp .env.predictive.local.example .env.predictive.local
# edit .env.predictive.local to your LOCAL postgres values only
./scripts/predictive/02_apply_local_migration.sh .env.predictive.local
```

Safety guardrails:
1. `scripts/predictive/01_local_db_preflight.sh` rejects cloud hosts (for example `*.postgres.database.azure.com`).
2. Only local hosts are allowed by default (`localhost`, `127.0.0.1`, `::1`, `host.docker.internal`).

## 3) Run Fabric modeling/scoring manually (demo path)

Execute notebooks in Fabric:
1. `fabric_delay_train_demo.ipynb`
2. `fabric_delay_score_demo.ipynb`

Export outputs to local JSON files under:
- `artifacts/predictive_delay/`

Expected files:
- `delay_predictions_current.json`
- `delay_model_metrics_latest.json`
- `delay_action_recommendations_current.json`
- `delay_decision_trace.json`

## 4) Mirror Fabric outputs to Postgres

Dry-run first:

```bash
set -a; source .env.predictive.local; set +a
python3 scripts/predictive/07_mirror_fabric_predictions_to_postgres.py --dry-run
```

Then write:

```bash
set -a; source .env.predictive.local; set +a
python3 scripts/predictive/07_mirror_fabric_predictions_to_postgres.py
```

Note: mirror script refuses cloud DB writes unless `ALLOW_CLOUD_PREDICTIVE_DB_WRITE=true`.

## 5) Deploy backend and frontend (dark launch)

1. Deploy backend workflow with `ENABLE_PREDICTIVE_API=false`.
2. Deploy frontend workflow with `NEXT_PUBLIC_ENABLE_PREDICTIVE_PANEL=false`.
3. Verify existing functionality remains unchanged.

## 6) Enable predictive feature

1. Enable backend flag:
   - `ENABLE_PREDICTIVE_API=true`
2. Validate API endpoints:
   - `GET /api/predictive/delays`
   - `GET /api/predictive/delay-metrics`
   - `GET /api/predictive/actions`
   - `GET /api/predictive/decision-metrics`
3. Enable frontend panel:
   - `NEXT_PUBLIC_ENABLE_PREDICTIVE_PANEL=true`
4. Optional:
   - `NEXT_PUBLIC_ENABLE_PREDICTIVE_ACTIONS_TAB=true`

## 7) Demo validation checklist

1. Left sidebar bottom shows `Predictive Ops`.
2. Panel opens and loads data.
3. Baseline/Optimized toggle works.
4. Action recommendations tab works (if enabled).
5. Existing chat flow is unaffected.

## Rollback

Primary rollback (recommended):
1. Set `NEXT_PUBLIC_ENABLE_PREDICTIVE_PANEL=false`
2. Set `NEXT_PUBLIC_ENABLE_PREDICTIVE_ACTIONS_TAB=false`
3. Optionally set `ENABLE_PREDICTIVE_API=false`

Secondary rollback (only if needed):
1. Roll back backend image tag to previous stable release.
2. Roll back frontend package to previous deployment.
