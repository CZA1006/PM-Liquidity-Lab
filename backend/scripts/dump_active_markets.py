#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import time
from socket import timeout as SocketTimeout
from typing import Any, Dict, Iterable, List, Optional, Tuple

from _http import http_get_json


def ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)


def parse_clob_token_ids(m: Dict[str, Any]) -> List[str]:
    """
    Gamma often returns clobTokenIds as a STRING containing a JSON array:
      "clobTokenIds": "[\"id1\",\"id2\"]"
    sometimes it's already a list.
    """
    raw = m.get("clobTokenIds") or m.get("clob_token_ids") or []
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
        # single token id string fallback
        return [s]
    return []


def normalize_event_tags(v: Any) -> List[str]:
    if v is None:
        return []
    if isinstance(v, list):
        out: List[str] = []
        for x in v:
            if isinstance(x, dict):
                name = x.get("name") or x.get("slug") or x.get("title") or x.get("id")
                if name is not None and str(name):
                    out.append(str(name))
            elif str(x):
                out.append(str(x))
        return out
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return []
        if s.startswith("["):
            try:
                arr = json.loads(s)
                if isinstance(arr, list):
                    return normalize_event_tags(arr)
            except Exception:
                return [s]
        return [s]
    return [str(v)]


def flatten_markets(resp: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Accepts either:
      { "markets": [...] }
    or:
      { "events": [ { "markets": [...] }, ... ], "markets": [...] }
    """
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
    # de-dup by slug/id
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


def _parse_iso_date(s: Any) -> Optional[dt.date]:
    if not s:
        return None
    t = str(s).strip()
    if not t:
        return None
    try:
        if len(t) >= 10:
            return dt.date.fromisoformat(t[:10])
    except Exception:
        return None
    return None


def _market_in_date_window(m: Dict[str, Any], d: dt.date) -> bool:
    # Prefer event-level boundaries; fall back to market-level if event-level missing.
    start = _parse_iso_date(m.get("event_start")) or _parse_iso_date(m.get("startDate")) or _parse_iso_date(m.get("start_date"))
    end = (
        _parse_iso_date(m.get("event_end"))
        or _parse_iso_date(m.get("endDate"))
        or _parse_iso_date(m.get("end_date"))
        or _parse_iso_date(m.get("closeTime"))
        or _parse_iso_date(m.get("close_time"))
    )
    if start and d < start:
        return False
    if end and d > end:
        return False
    return True


def _as_bool(v: Any) -> Optional[bool]:
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        s = v.strip().lower()
        if s in ("true", "1", "yes", "y"):
            return True
        if s in ("false", "0", "no", "n", ""):
            return False
    return None


def _is_strict_active_market(m: Dict[str, Any]) -> bool:
    # Enforce a conservative "monitorable active market" policy in the dump itself.
    active = _as_bool(m.get("active"))
    closed = _as_bool(m.get("closed"))
    archived = _as_bool(m.get("archived"))
    enable_ob = _as_bool(m.get("enableOrderBook"))
    accepting = _as_bool(m.get("acceptingOrders"))

    if active is False:
        return False
    if closed is True:
        return False
    if archived is True:
        return False
    if enable_ob is False:
        return False
    if accepting is False:
        return False
    return True


def _get_with_retry(url: str, timeout_s: float, retries: int, retry_sleep_s: float) -> Any:
    last_err: Optional[Exception] = None
    attempts = max(1, int(retries))
    for i in range(attempts):
        try:
            return http_get_json(url, timeout=timeout_s)
        except (TimeoutError, SocketTimeout) as e:
            last_err = e
        except Exception as e:
            # Keep retrying for transient backend/Gamma failures.
            last_err = e
        if i + 1 < attempts:
            time.sleep(max(0.0, float(retry_sleep_s)))
    raise RuntimeError(f"GET failed after {attempts} attempts: {url} err={last_err}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--api", required=True, help="e.g. http://127.0.0.1:8000")
    ap.add_argument("--out", required=True, help="output dir, e.g. backend/data/dumps")
    ap.add_argument("--max", type=int, default=2000, help="max markets to dump")
    ap.add_argument("--page", type=int, default=200, help="page size")
    ap.add_argument("--all_events", action="store_true", help="include non-active events (active_only=false)")
    ap.add_argument("--on_date", default="", help="optional YYYY-MM-DD filter by market/event date window")
    ap.add_argument("--timeout", type=float, default=90.0, help="http timeout seconds per request")
    ap.add_argument("--retries", type=int, default=3, help="retries per request")
    ap.add_argument("--retry_sleep", type=float, default=2.0, help="sleep seconds between retries")
    args = ap.parse_args()

    ensure_dir(args.out)
    ts = int(time.time())
    out_jsonl = os.path.join(args.out, f"active_markets-{ts}.jsonl")
    out_parquet = os.path.join(args.out, f"active_markets-{ts}.parquet")

    rows: List[Dict[str, Any]] = []
    seen_global: set[str] = set()
    offset = 0
    target_date: Optional[dt.date] = None
    if args.on_date:
        try:
            target_date = dt.date.fromisoformat(args.on_date)
        except Exception as e:
            raise SystemExit(f"[err] invalid --on_date '{args.on_date}': {e}")

    active_only = "false" if args.all_events else "true"
    while len(rows) < args.max:
        url = f"{args.api.rstrip('/')}/gamma/active_markets?limit={args.page}&offset={offset}&active_only={active_only}"
        resp = _get_with_retry(
            url,
            timeout_s=float(args.timeout),
            retries=int(args.retries),
            retry_sleep_s=float(args.retry_sleep),
        )
        if not isinstance(resp, dict):
            break
        ms = flatten_markets(resp)
        if not ms:
            break
        for m in ms:
            key = str(m.get("id") or m.get("slug") or m.get("marketSlug") or "").strip()
            if key:
                if key in seen_global:
                    continue
                seen_global.add(key)
            # normalize a few fields useful for later pipelines
            m = dict(m)
            m["_clobTokenIds_norm"] = parse_clob_token_ids(m)
            m["_event_tags_norm"] = normalize_event_tags(m.get("event_tags"))
            m["_event_category_norm"] = (
                str(m.get("event_category")).strip() if m.get("event_category") is not None else ""
            )
            if (not args.all_events) and (not _is_strict_active_market(m)):
                continue
            if target_date and (not _market_in_date_window(m, target_date)):
                continue
            rows.append(m)
            if len(rows) >= args.max:
                break
        offset += args.page

    # write jsonl
    with open(out_jsonl, "w", encoding="utf-8") as f:
        for m in rows:
            f.write(json.dumps(m, ensure_ascii=False) + "\n")
    print(f"[ok] wrote jsonl: {out_jsonl} markets={len(rows)}")

    # optional parquet
    try:
        import pyarrow as pa  # type: ignore
        import pyarrow.parquet as pq  # type: ignore

        table = pa.Table.from_pylist(rows)
        pq.write_table(table, out_parquet)
        print(f"[ok] wrote parquet: {out_parquet}")
    except Exception:
        print("[info] pyarrow not found -> parquet skipped")


if __name__ == "__main__":
    main()
