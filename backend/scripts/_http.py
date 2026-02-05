#!/usr/bin/env python3
from __future__ import annotations

import json
import urllib.request
import urllib.error
from typing import Any, Dict, Optional


def _merge_headers(extra: Optional[Dict[str, str]] = None) -> Dict[str, str]:
    h = {
        "Accept": "application/json",
        "User-Agent": "pm-liquidity-lab/0.1",
    }
    if extra:
        h.update(extra)
    return h


def http_get_json(url: str, timeout: float = 30.0) -> Any:
    req = urllib.request.Request(url, method="GET", headers=_merge_headers())
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            return json.loads(raw) if raw else None
    except urllib.error.HTTPError as e:
        body = ""
        try:
            body = e.read().decode("utf-8", errors="replace")
        except Exception:
            pass
        raise RuntimeError(f"GET {url} -> HTTP {e.code} {e.reason}: {body}") from e


def http_post_json(url: str, payload: Any, timeout: float = 30.0) -> Any:
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        method="POST",
        headers=_merge_headers({"Content-Type": "application/json"}),
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            return json.loads(raw) if raw else None
    except urllib.error.HTTPError as e:
        body = ""
        try:
            body = e.read().decode("utf-8", errors="replace")
        except Exception:
            pass
        raise RuntimeError(f"POST {url} -> HTTP {e.code} {e.reason}: {body}") from e
