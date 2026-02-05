#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import time
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


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--api", required=True, help="e.g. http://127.0.0.1:8000")
    ap.add_argument("--out", required=True, help="output dir, e.g. backend/data/dumps")
    ap.add_argument("--max", type=int, default=2000, help="max markets to dump")
    ap.add_argument("--page", type=int, default=200, help="page size")
    args = ap.parse_args()

    ensure_dir(args.out)
    ts = int(time.time())
    out_jsonl = os.path.join(args.out, f"active_markets-{ts}.jsonl")
    out_parquet = os.path.join(args.out, f"active_markets-{ts}.parquet")

    rows: List[Dict[str, Any]] = []
    offset = 0
    while len(rows) < args.max:
        url = f"{args.api.rstrip('/')}/gamma/active_markets?limit={args.page}&offset={offset}"
        resp = http_get_json(url)
        if not isinstance(resp, dict):
            break
        ms = flatten_markets(resp)
        if not ms:
            break
        for m in ms:
            # normalize a few fields useful for later pipelines
            m = dict(m)
            m["_clobTokenIds_norm"] = parse_clob_token_ids(m)
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
