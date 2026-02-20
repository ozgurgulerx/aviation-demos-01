#!/usr/bin/env python3
"""
Helpers for SSE framing with Agent Framework-style events.
"""

from __future__ import annotations

import json
from typing import Any, Dict


def to_sse(event: Dict[str, Any]) -> str:
    """Encode one event as Server-Sent Events frame."""
    return f"data: {json.dumps(event, ensure_ascii=True)}\n\n"
