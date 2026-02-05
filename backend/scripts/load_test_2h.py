#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import random
import time
from typing import Any, Dict, List, Optional, Tuple

from _http import http_get_json, http_post_json


def parse_clob_token_ids(m: Dict[str, Any]) -> List[str]:
    raw = m.get("clobTokenIds") or m.get("clob_token_ids") or m.get("_clobTokenIds_norm") or []
    if raw is None:
        return []
    if isinstance(raw, list):
        return [str(x) for x in raw if str(x)]
    if isinstance(raw, str):
        s = raw.strip()
        if not s:
            return []
        if s.startswith("["):
            try:
                arr = json.loads(s)
                if isinstance(arr, list):
                    return [str(x) for x in arr if str(x)]
            except Exception:
                return []
        return [s]
    return []


def flatten_markets(resp: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    ms = resp.get("markets")
    if isinstance(ms, list):
        out.extend([m for m in ms if isinstance(m, dict)])
    evs = resp.get("events")
    if isinstance(evs, list):
        for e in evs:
            if not isinstance(e, dict):
                continue
            ems = e.get("markets")
            if isinstance(ems, list):
                out.extend([m for m in ems if isinstance(m, dict)])
    # de-dup
    seen = set()
    dedup: List[Dict[str, Any]] = []
    for m in out:
        key = m.get("slug") or m.get("marketSlug") or m.get("id")
        if not key:
            continue
        if key in seen:
            continue
        seen.add(key)
        dedup.append(m)
    return dedup


def is_good_market(m: Dict[str, Any]) -> bool:
    """
    Keep it permissive for data collection.
    Key points:
      - must have orderbook enabled
      - should be not archived
      - should be not closed AND acceptingOrders==true (best effort)
      - DO NOT filter restricted (most markets are restricted=true in Gamma)
    """
    if m.get("archived") is True:
        return False
    if m.get("enableOrderBook") is False:
        return False
    if m.get("closed") is True:
        return False
    if m.get("acceptingOrders") is False:
        return False
    toks = parse_clob_token_ids(m)
    return len(toks) >= 2


def pick_tokens(api: str, markets_n: int, max_tokens: int) -> Tuple[List[str], List[Dict[str, Any]]]:
    """
    Pull a chunk of active markets and select up to max_tokens tokens.
    """
    api = api.rstrip("/")
    limit = max(200, markets_n * 50)
    url = f"{api}/gamma/active_markets?limit={limit}&offset=0"
    resp = http_get_json(url)
    if not isinstance(resp, dict):
        return [], []
    markets = flatten_markets(resp)

    good = [m for m in markets if is_good_market(m)]
    random.shuffle(good)

    picked_markets: List[Dict[str, Any]] = []
    tokens: List[str] = []

    for m in good:
        toks = parse_clob_token_ids(m)
        if len(toks) < 2:
            continue
        for t in toks[:2]:
            if t not in tokens:
                tokens.append(t)
        picked_markets.append(m)
        if len(tokens) >= max_tokens:
            tokens = tokens[:max_tokens]
            break

    return tokens, picked_markets


def sizeof_file(path: str) -> Optional[int]:
    try:
        return os.stat(path).st_size
    except Exception:
        return None


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--api", required=True, help="e.g. http://127.0.0.1:8000")
    ap.add_argument("--markets", type=int, default=20, help="target markets to sample")
    ap.add_argument("--duration", type=int, default=7200, help="seconds to run")
    ap.add_argument("--max_tokens", type=int, default=60, help="max token_ids to monitor")
    ap.add_argument("--poll", type=int, default=10, help="seconds between status polls")
    args = ap.parse_args()

    api = args.api.rstrip("/")
    tokens, picked_markets = pick_tokens(api, args.markets, args.max_tokens)
    print(f"[pick] markets={len(picked_markets)} tokens={len(tokens)} max_tokens={args.max_tokens}")
    if not tokens:
        print("[err] no tokens selected")
        print("      likely原因：筛选太严/ clobTokenIds 未 parse / active_markets 返回结构变化")
        return

    out = http_post_json(f"{api}/monitor/set_tokens", {"token_ids": tokens, "restart": True})
    print("[ok] set_tokens:", {"token_n": len(tokens), "ok": bool(out and out.get("ok")) if isinstance(out, dict) else True})

    t0 = time.time()
    last_status: Optional[Dict[str, Any]] = None
    last_raw_path: Optional[str] = None
    samples = 0

    while True:
        now = time.time()
        if now - t0 >= args.duration:
            break
        try:
            st = http_get_json(f"{api}/monitor/status")
            if isinstance(st, dict):
                last_status = st
                last_raw_path = st.get("raw_log_path") or last_raw_path
            _ = http_get_json(f"{api}/metrics/current")
            samples += 1
            if samples % max(1, int(60 / args.poll)) == 0:
                ws = last_status.get("ws_connected") if last_status else None
                rb = last_status.get("rebase_count") if last_status else None
                raw_n = last_status.get("raw_events_written") if last_status else None
                print(f"[stat] t+{int(now-t0)}s ws_connected={ws} rebase={rb} raw_events={raw_n}")
        except Exception as e:
            print("[warn] poll failed:", e)

        time.sleep(args.poll)

    print("[done] duration_s=", int(time.time() - t0), "samples=", samples)
    if last_status:
        print("[final status]")
        for k in [
            "started",
            "ws_connected",
            "token_ids",
            "ws_book_msgs",
            "ws_other_msgs",
            "rest_polls",
            "rebase_count",
            "raw_log_path",
            "raw_events_written",
        ]:
            print("  -", k, "=", last_status.get(k))

    if last_raw_path:
        sz = sizeof_file(last_raw_path)
        if sz is not None:
            print(f"[raw log size] {last_raw_path} bytes={sz} (~{sz/1024/1024:.2f} MB)")
        else:
            print(f"[raw log size] {last_raw_path} (not found)")


if __name__ == "__main__":
    main()
