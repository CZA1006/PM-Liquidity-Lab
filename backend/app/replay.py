from __future__ import annotations

import argparse
import json
import os
from typing import Any, Dict

from .book_engine import BookState
from .normalize import normalize_book


def _is_levels(obj: Any) -> bool:
    return isinstance(obj, list)


def run(events_path: str, out_path: str) -> int:
    books: Dict[str, BookState] = {}
    per_token_updates: Dict[str, int] = {}
    errors = 0
    applied = 0
    run_id = None

    with open(events_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                ev = json.loads(line)
            except Exception:
                errors += 1
                continue

            if run_id is None:
                run_id = ev.get("run_id")

            et = ev.get("event")
            nb = None

            if et in ("ws_book", "rest_book"):
                msg = ev.get("msg")
                if isinstance(msg, dict):
                    nb = normalize_book(msg, source="ws" if et == "ws_book" else "rest")
            elif et == "liq_snapshot":
                bids = ev.get("bids")
                asks = ev.get("asks")
                if _is_levels(bids) or _is_levels(asks):
                    source = ev.get("source") or "rest"
                    if isinstance(source, str):
                        nb = normalize_book(ev, source=source)

            if not nb:
                continue

            tid = nb.get("token_id") or ""
            if not tid:
                continue

            if tid not in books:
                books[tid] = BookState(token_id=tid)
                per_token_updates[tid] = 0

            books[tid].apply_snapshot(nb.get("bids") or [], nb.get("asks") or [], exchange_ts_ms=nb.get("exchange_ts_ms"))
            per_token_updates[tid] += 1
            applied += 1

    invariants_ok = True
    if applied <= 0:
        invariants_ok = False
    if errors != 0:
        invariants_ok = False
    for tid, n in per_token_updates.items():
        if n <= 0:
            invariants_ok = False

    report: Dict[str, Any] = {
        "run_id": run_id or "",
        "events_path": events_path,
        "applied_events": applied,
        "errors": errors,
        "tokens": sorted(list(books.keys())),
        "per_token": {
            tid: {
                "updates": per_token_updates[tid],
                "last_exchange_ts_ms": books[tid].exchange_ts_ms,
                "top": {"bid": books[tid].top()[0], "ask": books[tid].top()[1]},
            }
            for tid in books.keys()
        },
        "invariants_ok": invariants_ok,
    }

    out_dir = os.path.dirname(out_path)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    return 0 if invariants_ok else 2


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--events", required=True, help="path to events.jsonl")
    p.add_argument("--out", default=None, help="output replay_report.json")
    args = p.parse_args()

    out_path = args.out or os.path.join(os.path.dirname(args.events), "replay_report.json")
    raise SystemExit(run(args.events, out_path))


if __name__ == "__main__":
    main()
