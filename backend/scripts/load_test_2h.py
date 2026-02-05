#!/usr/bin/env python3
from __future__ import annotations

import glob
import argparse
import json
import os
import random
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from _http import http_get_json, http_post_json


def percentile(vals, pct: float):
    if not vals:
        return None
    xs = sorted(vals)
    k = (len(xs) - 1) * (pct / 100.0)
    f = int(k)
    c = min(f + 1, len(xs) - 1)
    if c == f:
        return float(xs[f])
    return float(xs[f] + (xs[c] - xs[f]) * (k - f))


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


def _first_thresholds(raw_path: str, event: str) -> Optional[Dict[str, Any]]:
    """
    Return the first thresholds snapshot from raw log for a given event.
    Expected schema (compat):
      - {"event": "...", "payload": {"thresholds": {...}}}
      - {"event": "...", "payload": {"payload": {"thresholds": {...}}}}
      - {"event": "...", "thresholds": {...}}
    """
    if not raw_path:
        return None
    # raw_log_path often comes back as "./data/..." from backend, but the actual file
    # is at "backend/data/..." when running this script from repo root.
    p = Path(raw_path)
    if not p.is_absolute():
        cand = None
        if isinstance(raw_path, str) and raw_path.startswith("./data/"):
            cand = Path("backend") / raw_path[2:]
        else:
            cand = p
        if not cand.exists():
            cand2 = Path("backend") / p
            if cand2.exists():
                cand = cand2
        p = cand
    if not p or not p.exists():
        return None
    with open(p, "r", encoding="utf-8") as f:
        for line in f:
            try:
                o = json.loads(line)
            except Exception:
                continue
            if o.get("event") != event:
                continue
            # Prefer payload.thresholds (new format), but be defensive:
            # sometimes payload can be nested or thresholds might be at top-level.
            payload = o.get("payload") or {}
            candidates = []
            # 1) payload.thresholds
            candidates.append(payload.get("thresholds"))
            # 2) payload.payload.thresholds (rare nested payload)
            if isinstance(payload.get("payload"), dict):
                candidates.append(payload.get("payload", {}).get("thresholds"))
            # 3) top-level thresholds
            candidates.append(o.get("thresholds"))

            th = None
            for c in candidates:
                if isinstance(c, dict) and c:
                    th = c
                    break
            if th is not None:
                return th
    return None


def _assert_rebase_thresholds_consistent(raw_path: str) -> None:
    """
    Acceptance check:
      - calibration_compare must have payload.thresholds
      - if rebase exists, its payload.thresholds must match core fields
    """
    cal = _first_thresholds(raw_path, "calibration_compare")
    if not cal:
        print("[accept] thresholds: FAIL (no calibration_compare.payload.thresholds found)")
        return
    reb = _first_thresholds(raw_path, "rebase")
    if not reb:
        print("[accept] thresholds: OK (no rebase event; calibration thresholds present)")
        return
    keys = ["thresh_top_n", "thresh_mismatch_levels", "thresh_mismatch_notional", "thresh_tob_must_match"]
    mismatch = {k: (cal.get(k), reb.get(k)) for k in keys if cal.get(k) != reb.get(k)}
    if mismatch:
        print("[accept] thresholds: FAIL (rebase thresholds != calibration thresholds)")
        print("         diff:", mismatch)
    else:
        print("[accept] thresholds: OK (rebase thresholds consistent)")


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
        size_path = last_raw_path
        if isinstance(size_path, str) and size_path.startswith("./data/"):
            size_path = "backend/" + size_path[2:]
        sz = sizeof_file(size_path)
        if sz is not None:
            print(f"[raw log size] {size_path} bytes={sz} (~{sz/1024/1024:.2f} MB)")
        else:
            print(f"[raw log size] {size_path} (not found)")

    if last_raw_path:
        _assert_rebase_thresholds_consistent(last_raw_path)

    # ---- compute compare stats from raw log (best-effort) ----
    raw_path = last_status.get("raw_log_path") if last_status else None
    p: Optional[Path] = Path(raw_path) if raw_path else None
    if p and not p.is_absolute():
        cand1 = (Path.cwd() / p).resolve()
        cand2 = (Path.cwd() / "backend" / p).resolve()
        parts = p.parts
        cand3 = None
        if len(parts) >= 2 and parts[0] == "." and parts[1] == "data":
            cand3 = (Path.cwd() / "backend" / Path(*parts[1:])).resolve()
        for cand in (cand1, cand2, cand3):
            if cand and cand.exists():
                p = cand
                break

    if p and p.exists():
        tob = []
        mismatch_notional = []
        with open(p, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    obj = json.loads(line)
                except Exception:
                    continue
                if obj.get("event") != "calibration_compare":
                    continue
                payload = obj.get("payload") or {}
                # schema compatibility:
                # - old: payload has tob_match / mismatch_notional at top-level
                # - new: payload has {"compare": {...}, "thresholds": {...}}
                cmp = payload.get("compare")
                if not isinstance(cmp, dict):
                    cmp = payload

                if "tob_match" in cmp:
                    tob.append(bool(cmp["tob_match"]))
                if "mismatch_notional" in cmp:
                    try:
                        mismatch_notional.append(float(cmp["mismatch_notional"]))
                    except Exception:
                        pass
        if tob:
            tob_match_rate = sum(1 for x in tob if x) / len(tob)
        else:
            tob_match_rate = None
        p90 = percentile(mismatch_notional, 90.0)
        print("[calibration stats]")
        print("  - compares =", len(tob))
        print("  - tob_match_rate =", tob_match_rate)
        print("  - mismatch_notional_p90 =", p90)
    else:
        print(f"[calibration stats] raw_log_path missing or not found; skipping (raw_log_path={raw_path})")


if __name__ == "__main__":
    main()
