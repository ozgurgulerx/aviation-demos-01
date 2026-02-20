#!/usr/bin/env python3
"""Inspect Azure OpenAI env config and optionally run live model checks."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

from azure.identity import DefaultAzureCredential, get_bearer_token_provider
from dotenv import dotenv_values, load_dotenv
from openai import AzureOpenAI

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ENV_FILES = [ROOT / ".env", ROOT / "src/.env.local"]

SENSITIVE_KEYS = {
    "AZURE_OPENAI_API_KEY",
    "AZURE_OPENAI_CLIENT_SECRET",
    "AZURE_OPENAI_BEARER_TOKEN",
    "PGPASSWORD",
    "PASSWORD",
    "SECRET",
    "KEY",
}


def _mask(value: str) -> str:
    if not value:
        return "(empty)"
    if len(value) <= 8:
        return "*" * len(value)
    return f"{value[:4]}...{value[-4:]} (len={len(value)})"


def _is_sensitive(name: str) -> bool:
    upper = name.upper()
    return any(token in upper for token in SENSITIVE_KEYS)


def _print_file_env_summary(path: Path, keys: Iterable[str]) -> None:
    if not path.exists():
        print(f"[MISSING] {path}")
        return

    values = dotenv_values(path)
    print(f"[FILE] {path}")
    for key in keys:
        raw = values.get(key)
        if raw is None:
            print(f"  - {key}: (not set)")
            continue
        value = str(raw).strip().strip('"').strip("'")
        display = _mask(value) if _is_sensitive(key) else value
        print(f"  - {key}: {display}")


def _load_env_files(paths: List[Path]) -> None:
    for path in paths:
        if path.exists():
            load_dotenv(path, override=True)


def _build_client(auth_mode: str) -> Tuple[AzureOpenAI, str]:
    endpoint = os.getenv("AZURE_OPENAI_ENDPOINT", "").strip()
    api_version = os.getenv("AZURE_OPENAI_API_VERSION", "2024-06-01").strip() or "2024-06-01"
    api_key = os.getenv("AZURE_OPENAI_API_KEY", "").strip()

    if not endpoint:
        raise ValueError("Missing AZURE_OPENAI_ENDPOINT")

    use_api_key = auth_mode == "api-key" or (auth_mode == "auto" and bool(api_key))

    if use_api_key:
        if not api_key:
            raise ValueError("AZURE_OPENAI_API_KEY is required when --auth-mode=api-key")
        return (
            AzureOpenAI(
                azure_endpoint=endpoint,
                api_key=api_key,
                api_version=api_version,
                timeout=30,
                max_retries=1,
            ),
            "api-key",
        )

    credential = DefaultAzureCredential()
    token_provider = get_bearer_token_provider(
        credential, "https://cognitiveservices.azure.com/.default"
    )
    return (
        AzureOpenAI(
            azure_endpoint=endpoint,
            azure_ad_token_provider=token_provider,
            api_version=api_version,
            timeout=30,
            max_retries=1,
        ),
        "entra-token",
    )


def _chat_check(client: AzureOpenAI, deployment: str) -> Tuple[bool, str]:
    try:
        response = client.chat.completions.create(
            model=deployment,
            messages=[{"role": "user", "content": "Reply with: OK"}],
        )
        content = (response.choices[0].message.content or "").strip()
        return True, content[:120] if content else "(empty response)"
    except Exception as exc:  # noqa: BLE001
        return False, str(exc)


def _embedding_check(client: AzureOpenAI, deployment: str) -> Tuple[bool, str]:
    try:
        response = client.embeddings.create(
            model=deployment,
            input=["Aviation operations sample text."],
        )
        dims = len(response.data[0].embedding)
        return True, f"embedding dimensions={dims}"
    except Exception as exc:  # noqa: BLE001
        return False, str(exc)


def _confirm_models(model_values: Iterable[str]) -> Dict[str, bool]:
    normalized = {m.strip().lower() for m in model_values if m and m.strip()}
    return {
        "gpt-5-nano": "gpt-5-nano" in normalized,
        "gpt-5-mini": "gpt-5-mini" in normalized,
        "gpt-5-mini-low": "gpt-5-mini-low" in normalized,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Inspect .env/.env.local LLM config and optionally run live Azure OpenAI checks.",
    )
    parser.add_argument(
        "--env-file",
        action="append",
        default=[],
        help="Env file path (repeatable). Defaults to .env and src/.env.local.",
    )
    parser.add_argument(
        "--live",
        action="store_true",
        help="Run live chat + embedding checks against configured deployments.",
    )
    parser.add_argument(
        "--chat-deployment",
        action="append",
        default=[],
        help="Additional chat deployment names to test in --live mode.",
    )
    parser.add_argument(
        "--embedding-deployment",
        default="",
        help="Override embedding deployment for --live mode.",
    )
    parser.add_argument(
        "--auth-mode",
        choices=["auto", "api-key", "token"],
        default="auto",
        help="Auth mode for --live checks. auto prefers API key when present.",
    )
    args = parser.parse_args()

    env_files = [Path(p).resolve() for p in args.env_file] if args.env_file else DEFAULT_ENV_FILES

    report_keys = [
        "AZURE_OPENAI_ENDPOINT",
        "AZURE_OPENAI_API_VERSION",
        "AZURE_OPENAI_DEPLOYMENT_NAME",
        "AZURE_OPENAI_REASONING_DEPLOYMENT_NAME",
        "AZURE_OPENAI_WORKER_DEPLOYMENT_NAME",
        "AZURE_OPENAI_ORCHESTRATOR_DEPLOYMENT_NAME",
        "AZURE_TEXT_EMBEDDING_DEPLOYMENT_NAME",
        "AZURE_OPENAI_API_KEY",
        "AZURE_OPENAI_CLIENT_SECRET",
        "PGPASSWORD",
    ]

    print("=== Env File Summary (masked for sensitive values) ===")
    for path in env_files:
        _print_file_env_summary(path, report_keys)

    _load_env_files(env_files)

    chat_models = [
        os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME", "").strip(),
        os.getenv("AZURE_OPENAI_REASONING_DEPLOYMENT_NAME", "").strip(),
        os.getenv("AZURE_OPENAI_WORKER_DEPLOYMENT_NAME", "").strip(),
        os.getenv("AZURE_OPENAI_ORCHESTRATOR_DEPLOYMENT_NAME", "").strip(),
    ]
    confirmations = _confirm_models(chat_models)
    embedding_model = (
        args.embedding_deployment.strip()
        or os.getenv("AZURE_TEXT_EMBEDDING_DEPLOYMENT_NAME", "").strip()
    )

    print("\n=== Deployment Confirmation (from effective env) ===")
    print(f"- gpt-5-nano present: {confirmations['gpt-5-nano']}")
    print(f"- gpt-5-mini present: {confirmations['gpt-5-mini']}")
    print(f"- gpt-5-mini-low present: {confirmations['gpt-5-mini-low']}")
    print(f"- embedding deployment: {embedding_model or '(not set)'}")

    if not args.live:
        print("\nLive checks skipped. Re-run with --live to validate actual endpoint access.")
        return 0

    client, active_auth_mode = _build_client(args.auth_mode)
    print(f"\n=== Live Checks (auth={active_auth_mode}) ===")

    chat_deployments = []
    for deployment in chat_models + args.chat_deployment:
        dep = deployment.strip()
        if dep and dep not in chat_deployments:
            chat_deployments.append(dep)

    if not chat_deployments:
        print("No chat deployments found to test.")
        return 1

    failures = 0
    for deployment in chat_deployments:
        ok, detail = _chat_check(client, deployment)
        status = "PASS" if ok else "FAIL"
        print(f"[{status}] chat deployment '{deployment}': {detail}")
        if not ok:
            failures += 1

    if embedding_model:
        ok, detail = _embedding_check(client, embedding_model)
        status = "PASS" if ok else "FAIL"
        print(f"[{status}] embedding deployment '{embedding_model}': {detail}")
        if not ok:
            failures += 1
    else:
        print("[WARN] embedding deployment is not set; skipping embedding live check.")

    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
