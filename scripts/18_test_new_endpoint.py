#!/usr/bin/env python3
"""
Verify Azure OpenAI endpoint compatibility after migration.

Tests:
1. response_format={"type":"json_object"} with gpt-5-nano
2. response_format={"type":"json_object"} with gpt-5-mini
3. stream=True with gpt-5-nano
"""

import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from shared_utils import OPENAI_API_VERSION
from azure_openai_client import get_shared_client

client, auth_mode = get_shared_client(api_version=OPENAI_API_VERSION)
print(f"Auth mode: {auth_mode}")
print(f"API version: {OPENAI_API_VERSION}")
print()

NANO = os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME", "gpt-5-nano")
MINI = os.getenv("AZURE_OPENAI_REASONING_DEPLOYMENT_NAME", "gpt-5-mini")

passed = 0
failed = 0


def test(name: str):
    print(f"{'='*60}")
    print(f"TEST: {name}")
    print(f"{'='*60}")


# ── Test 1: JSON mode with gpt-5-nano ──────────────────────
test(f"response_format={{type:json_object}} with {NANO}")
try:
    resp = client.chat.completions.create(
        model=NANO,
        messages=[
            {"role": "system", "content": "You are a query router. Return JSON only."},
            {"role": "user", "content": 'Classify: "Top 5 locations with most ASRS reports". Return {"route":"SQL","reasoning":"..."}'},
        ],
        response_format={"type": "json_object"},
    )
    raw = resp.choices[0].message.content
    print(f"Raw output:\n{raw}\n")
    parsed = json.loads(raw)
    print(f"Parsed JSON: {json.dumps(parsed, indent=2)}")
    print(f"PASS\n")
    passed += 1
except Exception as e:
    print(f"FAIL: {e}\n")
    failed += 1

# ── Test 2: JSON mode with gpt-5-mini ──────────────────────
test(f"response_format={{type:json_object}} with {MINI}")
try:
    resp = client.chat.completions.create(
        model=MINI,
        messages=[
            {"role": "system", "content": "You are a flight planner. Return JSON only."},
            {"role": "user", "content": 'Plan for: "What NOTAMs affect KJFK?". Return {"intent":{"name":"PilotBrief.Departure","confidence":0.9},"tool_calls":[]}'},
        ],
        response_format={"type": "json_object"},
        reasoning_effort="low",
    )
    raw = resp.choices[0].message.content
    print(f"Raw output:\n{raw}\n")
    parsed = json.loads(raw)
    print(f"Parsed JSON: {json.dumps(parsed, indent=2)}")
    print(f"PASS\n")
    passed += 1
except Exception as e:
    print(f"FAIL: {e}\n")
    failed += 1

# ── Test 3: Streaming with gpt-5-nano ──────────────────────
test(f"stream=True with {NANO}")
try:
    stream = client.chat.completions.create(
        model=NANO,
        messages=[
            {"role": "system", "content": "You are an aviation safety assistant."},
            {"role": "user", "content": "In one sentence, what is ASRS?"},
        ],
        stream=True,
    )
    chunks = []
    empty_deltas = 0
    for chunk in stream:
        if chunk.choices and chunk.choices[0].delta:
            delta = chunk.choices[0].delta
            if delta.content:
                chunks.append(delta.content)
            else:
                empty_deltas += 1
    full_text = "".join(chunks)
    print(f"Streamed text ({len(chunks)} content chunks, {empty_deltas} empty deltas):")
    print(f"  {full_text}")
    if full_text.strip():
        print(f"PASS\n")
        passed += 1
    else:
        print(f"FAIL: No content received in stream\n")
        failed += 1
except Exception as e:
    print(f"FAIL: {e}\n")
    failed += 1

# ── Summary ─────────────────────────────────────────────────
print(f"{'='*60}")
print(f"RESULTS: {passed} passed, {failed} failed")
print(f"{'='*60}")
sys.exit(1 if failed else 0)
