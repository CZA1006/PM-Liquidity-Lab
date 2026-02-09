#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import glob
import json
import time
from collections import Counter
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from _http import http_get_json


def _resolve_path(p: Optional[str]) -> Optional[Path]:
    if not p:
        return None
    rp = Path(p)
    if rp.is_absolute():
        return rp if rp.exists() else None
    cand1 = (Path.cwd() / rp).resolve()
    cand2 = (Path.cwd() / "backend" / rp).resolve()
    cand3 = None
    parts = rp.parts
    if len(parts) >= 2 and parts[0] == "." and parts[1] == "data":
        cand3 = (Path.cwd() / "backend" / Path(*parts[1:])).resolve()
    for cand in (cand1, cand2, cand3):
        if cand and cand.exists():
            return cand
    return None


def _latest(pattern: str) -> Optional[Path]:
    fs = sorted(glob.glob(pattern))
    if not fs:
        return None
    return Path(fs[-1])


def _pick_report_path(report: Optional[str]) -> Optional[Path]:
    if report:
        return _resolve_path(report)
    p = _latest("backend/data/reports/scale_report-*.csv")
    return _resolve_path(str(p)) if p else None


def _pick_metrics_events_path(run_id: Optional[str]) -> Optional[Path]:
    if run_id:
        p = _resolve_path(f"./data/runs/{run_id}/events.jsonl")
        if p:
            return p
    return _resolve_path("backend/data/runs/local/events.jsonl")


def _print_header(title: str) -> None:
    print(f"\n=== {title} ===")


def inspect_scale_report(path: Optional[Path]) -> None:
    _print_header("Scale Report")
    if not path or not path.exists():
        print("file: <missing>")
        return
    print(f"file: {path}")
    with open(path, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        rows = list(r)
        print("columns:", r.fieldnames or [])
        print("rows:", len(rows))
        if rows:
            print("latest_row:", rows[-1])


def inspect_raw_log(path: Optional[Path], sample_lines: int = 500, recent_sec: int = 0) -> None:
    _print_header("Raw Log")
    if not path or not path.exists():
        print("file: <missing>")
        return
    print(f"file: {path}")
    total = 0
    matched = 0
    cutoff_ms = int(time.time() * 1000) - recent_sec * 1000 if recent_sec and recent_sec > 0 else None
    key_counter: Counter[str] = Counter()
    event_counter: Counter[str] = Counter()
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            total += 1
            try:
                o = json.loads(line)
            except Exception:
                continue
            if cutoff_ms is not None:
                ts_ms = o.get("ts_ms")
                if not isinstance(ts_ms, int) or ts_ms < cutoff_ms:
                    continue
            matched += 1
            if cutoff_ms is None and matched > sample_lines:
                break
            for k in o.keys():
                key_counter[k] += 1
            if "event" in o:
                event_counter[f"event:{o.get('event')}"] += 1
            if "kind" in o:
                event_counter[f"kind:{o.get('kind')}"] += 1
    if cutoff_ms is None:
        print("sampled_lines:", min(matched, sample_lines))
        print("mode:", "head_sample")
    else:
        print("sampled_lines:", matched)
        print("mode:", f"recent_window_{recent_sec}s")
        print("cutoff_ts_ms:", cutoff_ms)
    print("top_keys:", key_counter.most_common(20))
    print("event_kind_counts:", dict(event_counter))


def inspect_metrics_events(path: Optional[Path], sample_lines: int = 5000, recent_sec: int = 0) -> None:
    _print_header("Metrics Events")
    if not path or not path.exists():
        print("file: <missing>")
        return
    print(f"file: {path}")
    total = 0
    matched = 0
    cutoff_ms = int(time.time() * 1000) - recent_sec * 1000 if recent_sec and recent_sec > 0 else None
    mtick = 0
    tokens: set[str] = set()
    mid = spread = depth = slippage = 0
    metrics_keys: Counter[str] = Counter()
    top_level: Counter[str] = Counter()
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            total += 1
            try:
                o = json.loads(line)
            except Exception:
                continue
            if cutoff_ms is not None:
                ts_ms = o.get("ts_ms")
                if not isinstance(ts_ms, int) or ts_ms < cutoff_ms:
                    continue
            matched += 1
            if cutoff_ms is None and matched > sample_lines:
                break
            if o.get("event") != "metrics_tick":
                continue
            mtick += 1
            for k in o.keys():
                top_level[k] += 1
            tid = o.get("token_id")
            if isinstance(tid, str) and tid:
                tokens.add(tid)
            m = o.get("metrics")
            if isinstance(m, dict):
                for k in m.keys():
                    metrics_keys[k] += 1
                if m.get("mid") is not None:
                    mid += 1
                if m.get("spread_bps") is not None:
                    spread += 1
                if m.get("depth") is not None:
                    depth += 1
                if m.get("slippage") is not None:
                    slippage += 1
    if cutoff_ms is None:
        print("sampled_lines:", min(matched, sample_lines))
        print("mode:", "head_sample")
    else:
        print("sampled_lines:", matched)
        print("mode:", f"recent_window_{recent_sec}s")
        print("cutoff_ts_ms:", cutoff_ms)
    print("metrics_tick_count:", mtick)
    print("metrics_tick_tokens:", len(tokens))
    if mtick > 0:
        print(
            "coverage:",
            {
                "mid": mid / mtick,
                "spread_bps": spread / mtick,
                "depth": depth / mtick,
                "slippage": slippage / mtick,
            },
        )
    print("metrics_keys_top:", metrics_keys.most_common(20))
    print("event_top_level_keys:", top_level.most_common(20))


def inspect_duckdb(path: Optional[Path]) -> None:
    _print_header("DuckDB")
    if not path or not path.exists():
        print("file: <missing>")
        return
    print(f"file: {path}")
    try:
        import duckdb  # type: ignore
    except Exception:
        print("duckdb: not installed")
        return
    con = duckdb.connect(str(path))
    try:
        tables = con.execute("show tables").fetchall()
        print("tables:", tables)
        if any(t[0] == "market_dim" for t in tables):
            print("market_dim_count:", con.execute("select count(*) from market_dim").fetchone()[0])
            print(
                "market_dim_sample:",
                con.execute(
                    """
                    select market_id, market_slug, event_title, event_category, event_tags_json, clob_tokens_n, start_date, end_date
                    from market_dim
                    limit 3
                    """
                ).fetchall(),
            )
        if any(t[0] == "token_dim" for t in tables):
            print("token_dim_count:", con.execute("select count(*) from token_dim").fetchone()[0])
            print(
                "token_dim_sample:",
                con.execute(
                    """
                    select token_id, market_id, market_slug, outcome_index, outcome_name, event_title
                    from token_dim
                    limit 3
                    """
                ).fetchall(),
            )
    finally:
        con.close()


def inspect_books_zst(sample_bytes: int = 200000) -> None:
    _print_header("Books ZST")
    try:
        import zstandard as zstd  # type: ignore
    except Exception:
        print("zstandard: not installed")
        return

    for kind in ("books_delta", "books_snapshot"):
        p = _latest(f"backend/data/books/local/{kind}-*.jsonl.zst")
        rp = _resolve_path(str(p)) if p else None
        if not rp or not rp.exists():
            print(f"{kind}: <missing>")
            continue
        with open(rp, "rb") as f:
            reader = zstd.ZstdDecompressor().stream_reader(f)
            data = reader.read(sample_bytes).decode("utf-8", "replace").splitlines()
        print(f"{kind}: file={rp} sampled_lines={len(data)}")
        if data:
            try:
                o = json.loads(data[0])
                print(f"{kind}: top_keys={sorted(o.keys())}")
                payload = o.get("payload")
                if isinstance(payload, dict):
                    print(f"{kind}: payload_keys={sorted(payload.keys())}")
            except Exception:
                print(f"{kind}: first line parse failed")


def load_status(api: Optional[str]) -> Dict[str, Any]:
    if not api:
        return {}
    try:
        out = http_get_json(f"{api.rstrip('/')}/monitor/status")
        return out if isinstance(out, dict) else {}
    except Exception:
        return {}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--api", default="http://127.0.0.1:8000", help="backend api base")
    ap.add_argument("--report", default="", help="scale_report csv path")
    ap.add_argument("--db", default="backend/data/warehouse/pmliq.duckdb", help="duckdb path")
    ap.add_argument("--sample_raw_lines", type=int, default=500, help="raw log sampled lines")
    ap.add_argument("--sample_events_lines", type=int, default=5000, help="events log sampled lines")
    ap.add_argument("--sample_zst_bytes", type=int, default=200000, help="zst decompressed bytes")
    ap.add_argument("--recent_sec", type=int, default=0, help="if >0, inspect records with ts_ms in last N seconds")
    args = ap.parse_args()

    status = load_status(args.api)
    raw_path = _resolve_path(status.get("raw_log_path")) if status else _latest("backend/data/raw/local/events-*.jsonl")
    report_path = _pick_report_path(args.report or None)
    events_path = _pick_metrics_events_path(status.get("run_id") if status else None)
    db_path = _resolve_path(args.db)

    _print_header("Status")
    if status:
        print(
            {
                "run_id": status.get("run_id"),
                "started": status.get("started"),
                "ws_connected": status.get("ws_connected"),
                "token_n": len(status.get("token_ids") or []),
                "raw_log_path": status.get("raw_log_path"),
                "books_delta_log_path": status.get("books_delta_log_path"),
                "books_snapshot_log_path": status.get("books_snapshot_log_path"),
            }
        )
    else:
        print("monitor status unavailable")

    inspect_scale_report(report_path)
    inspect_raw_log(raw_path, sample_lines=args.sample_raw_lines, recent_sec=args.recent_sec)
    inspect_metrics_events(events_path, sample_lines=args.sample_events_lines, recent_sec=args.recent_sec)
    inspect_duckdb(db_path)
    inspect_books_zst(sample_bytes=args.sample_zst_bytes)


if __name__ == "__main__":
    main()
