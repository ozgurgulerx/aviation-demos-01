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

## Foundry Voice Runtime Context (2026-02-20)

- Foundry subscription: `a20bc194-9787-44ee-9c7f-7c3130e651b6`.
- Foundry account: `ai-eastus2hubozguler527669401205` (`rg-openai`, `eastus2`).
- Foundry project workspace: `ai-eastus2hubozguler527-project`.

Deployed model targets for this repo:

- Chat: `aviation-chat-gpt5-mini` -> `gpt-5-mini`.
- Voice/TTS: `aviation-voice-tts` -> `gpt-4o-mini-tts`.
- Voice realtime/audio support: `aviation-voice-gpt4o-audio` -> `gpt-4o-audio-preview`.

Runtime defaults expected:

- Backend model env default: `AZURE_OPENAI_DEPLOYMENT_NAME=aviation-chat-gpt5-mini`.
- Frontend voice route env defaults:
  - `AZURE_OPENAI_ENDPOINT=https://ai-eastus2hubozguler527669401205.cognitiveservices.azure.com/`
  - `AZURE_OPENAI_VOICE_DEPLOYMENT_NAME=aviation-voice-tts`
  - `AZURE_OPENAI_VOICE_MODEL=gpt-4o-mini-tts`
  - `AZURE_OPENAI_VOICE_API_VERSION=2025-03-01-preview`
  - `AZURE_OPENAI_AUTH_MODE=token`
  - `AZURE_OPENAI_VOICE_TURKISH=alloy`
  - `AZURE_OPENAI_VOICE_ENGLISH=alloy`

Token-auth identity context (non-secret):

- Entra app display name: `aviation-rag-openai-voice-sp`
- Client ID: `6e36ed48-f9eb-4e4b-afac-e17f13141df5`
- Minimum required role assignment scope: Foundry account resource (`Microsoft.CognitiveServices/accounts/ai-eastus2hubozguler527669401205`)
- Assigned roles currently include:
  - `Cognitive Services OpenAI User`
  - `Cognitive Services User`
  - `Cognitive Services OpenAI Contributor`

Operational note:

- Tenant policy enforces short client-secret lifetime. Treat voice SP secret as 30-day rotation cadence; next rotation target is on/before `2026-03-22`.
