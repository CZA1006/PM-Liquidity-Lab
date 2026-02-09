#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from _http import http_get_json, http_post_json
from load_test_2h import pick_tokens, percentile, sizeof_file

try:
    from load_test_2h import _first_thresholds  # type: ignore
except Exception:
    _first_thresholds = None  # type: ignore


def _now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


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


def _resolve_metrics_events_path(status: Dict[str, Any]) -> Optional[Path]:
    run_id = status.get("run_id")
    if isinstance(run_id, str) and run_id:
        c1 = _resolve_path(f"./data/runs/{run_id}/events.jsonl")
        if c1:
            return c1
    raw_path = status.get("raw_log_path")
    if isinstance(raw_path, str) and raw_path:
        rp = _resolve_path(raw_path)
        if rp:
            parts = rp.parts
            try:
                i = parts.index("raw")
                if i + 1 < len(parts):
                    run = parts[i + 1]
                    c2 = _resolve_path(f"./data/runs/{run}/events.jsonl")
                    if c2:
                        return c2
            except ValueError:
                pass
    return None


def _metrics_tick_stats(events_path: Optional[Path], ts_from: int, ts_to: int) -> Dict[str, Any]:
    if not events_path or not events_path.exists():
        return {
            "metrics_tick_count": 0,
            "metrics_tick_tokens": 0,
            "metrics_mid_cov": None,
            "metrics_spread_cov": None,
            "metrics_depth_cov": None,
            "metrics_slippage_cov": None,
        }

    n = 0
    tokens: set[str] = set()
    mid = 0
    spread = 0
    depth = 0
    slippage = 0

    with open(events_path, "r", encoding="utf-8") as f:
        for line in f:
            try:
                obj = json.loads(line)
            except Exception:
                continue
            if obj.get("event") != "metrics_tick":
                continue
            ts_ms = obj.get("ts_ms")
            if not isinstance(ts_ms, int) or ts_ms < ts_from or ts_ms > ts_to:
                continue
            n += 1
            tid = obj.get("token_id")
            if isinstance(tid, str) and tid:
                tokens.add(tid)
            m = obj.get("metrics")
            if isinstance(m, dict):
                if m.get("mid") is not None:
                    mid += 1
                if m.get("spread_bps") is not None:
                    spread += 1
                if m.get("depth") is not None:
                    depth += 1
                if m.get("slippage") is not None:
                    slippage += 1

    def _cov(x: int) -> Optional[float]:
        if n <= 0:
            return None
        return float(x) / float(n)

    return {
        "metrics_tick_count": n,
        "metrics_tick_tokens": len(tokens),
        "metrics_mid_cov": _cov(mid),
        "metrics_spread_cov": _cov(spread),
        "metrics_depth_cov": _cov(depth),
        "metrics_slippage_cov": _cov(slippage),
    }


def _first_thresholds_local(raw_path: Path, event: str) -> Optional[Dict[str, Any]]:
    if not raw_path.exists():
        return None
    with open(raw_path, "r", encoding="utf-8") as f:
        for line in f:
            try:
                obj = json.loads(line)
            except Exception:
                continue
            if obj.get("event") != event:
                continue
            payload = obj.get("payload") or {}
            candidates = []
            if isinstance(payload, dict):
                candidates.append(payload.get("thresholds"))
                nested = payload.get("payload")
                if isinstance(nested, dict):
                    candidates.append(nested.get("thresholds"))
            candidates.append(obj.get("thresholds"))
            for c in candidates:
                if isinstance(c, dict) and c:
                    return c
    return None


def _thresholds_ok(raw_path: Optional[Path]) -> bool:
    if not raw_path:
        return False
    if callable(_first_thresholds):
        cal = _first_thresholds(str(raw_path), "calibration_compare")  # type: ignore
        reb = _first_thresholds(str(raw_path), "rebase")  # type: ignore
    else:
        cal = _first_thresholds_local(raw_path, "calibration_compare")
        reb = _first_thresholds_local(raw_path, "rebase")
    if not cal:
        return False
    if not reb:
        return True
    keys = ["thresh_top_n", "thresh_mismatch_levels", "thresh_mismatch_notional", "thresh_tob_must_match"]
    return all(cal.get(k) == reb.get(k) for k in keys)


def _calibration_stats(raw_path: Optional[Path], ts_from: int, ts_to: int) -> Dict[str, Any]:
    if not raw_path or not raw_path.exists():
        return {"compares": 0, "tob_match_rate": None, "mismatch_notional_p90": None}

    compares = 0
    tob = 0
    mismatch_notional: List[float] = []
    latest_summary: Optional[Dict[str, Any]] = None

    with open(raw_path, "r", encoding="utf-8") as f:
        for line in f:
            try:
                obj = json.loads(line)
            except Exception:
                continue
            ev_ts = obj.get("ts_ms")
            if not isinstance(ev_ts, int):
                continue
            if ev_ts < ts_from or ev_ts > ts_to:
                continue
            ev = obj.get("event")
            payload = obj.get("payload") or {}
            if ev == "calibration_summary" and isinstance(payload, dict):
                latest_summary = payload
                continue
            if ev != "calibration_compare":
                continue
            cmp = payload.get("compare") if isinstance(payload, dict) else None
            if not isinstance(cmp, dict):
                cmp = payload if isinstance(payload, dict) else {}
            if "tob_match" in cmp:
                compares += 1
                if bool(cmp.get("tob_match")):
                    tob += 1
            if "mismatch_notional" in cmp:
                try:
                    mismatch_notional.append(float(cmp["mismatch_notional"]))
                except Exception:
                    pass

    if latest_summary:
        return {
            "compares": int(latest_summary.get("compares") or 0),
            "tob_match_rate": latest_summary.get("tob_match_rate"),
            "mismatch_notional_p90": latest_summary.get("mismatch_notional_p90"),
        }

    tob_match_rate = (float(tob) / float(compares)) if compares > 0 else None
    return {
        "compares": compares,
        "tob_match_rate": tob_match_rate,
        "mismatch_notional_p90": percentile(mismatch_notional, 90.0),
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--api", required=True, help="e.g. http://127.0.0.1:8000")
    ap.add_argument("--markets", type=int, default=50, help="target markets to sample")
    ap.add_argument("--tiers", type=str, default="200,500,1000,2000,5000", help="comma-separated token tiers")
    ap.add_argument("--tier_duration", type=int, default=600, help="seconds per tier")
    ap.add_argument("--poll", type=int, default=10, help="seconds between status polls")
    ap.add_argument("--out", type=str, default="", help="output csv path")
    args = ap.parse_args()

    tiers: List[int] = []
    for x in (args.tiers or "").split(","):
        x = x.strip()
        if not x:
            continue
        try:
            tiers.append(int(x))
        except Exception:
            continue
    if not tiers:
        tiers = [200, 500, 1000, 2000, 5000]

    out_path = Path(args.out) if args.out else Path("backend/data/reports") / f"scale_report-{int(time.time())}.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    fields = [
        "ts_iso",
        "tier_tokens",
        "picked_markets",
        "duration_s",
        "ws_book_msgs",
        "ws_other_msgs",
        "rest_polls",
        "rebase_count",
        "raw_events_written",
        "books_delta_written",
        "ws_book_msgs_per_s",
        "ws_other_msgs_per_s",
        "raw_events_per_s",
        "raw_bytes",
        "raw_mb_per_hour",
        "compares",
        "tob_match_rate",
        "mismatch_notional_p90",
        "thresholds_ok",
        "metrics_tick_count",
        "metrics_tick_tokens",
        "metrics_mid_cov",
        "metrics_spread_cov",
        "metrics_depth_cov",
        "metrics_slippage_cov",
    ]

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()

        for tier_n in tiers:
            tokens, picked = pick_tokens(args.api, args.markets, max_tokens=tier_n)
            if not tokens:
                print(f"[tier {tier_n}] no tokens picked")
                continue

            http_post_json(f"{args.api.rstrip('/')}/monitor/set_tokens", {"token_ids": tokens, "restart": True})

            st0 = http_get_json(f"{args.api.rstrip('/')}/monitor/status")
            if not isinstance(st0, dict):
                print(f"[tier {tier_n}] status unavailable")
                continue

            delta_path = _resolve_path(st0.get("books_delta_log_path"))
            size0 = sizeof_file(str(delta_path)) if delta_path else 0
            t0 = time.time()
            ts_from = int(t0 * 1000)
            last_status: Dict[str, Any] = st0

            while True:
                now = time.time()
                if now - t0 >= args.tier_duration:
                    break
                try:
                    st = http_get_json(f"{args.api.rstrip('/')}/monitor/status")
                    if isinstance(st, dict):
                        last_status = st
                    _ = http_get_json(f"{args.api.rstrip('/')}/metrics/current")
                except Exception:
                    pass
                time.sleep(args.poll)

            ts_to = int(time.time() * 1000)
            dur = max(1, int(ts_to / 1000 - t0))
            delta_path = _resolve_path(last_status.get("books_delta_log_path"))
            raw_path = _resolve_path(last_status.get("raw_log_path"))
            metrics_events_path = _resolve_metrics_events_path(last_status)
            size1 = sizeof_file(str(delta_path)) if delta_path else None

            raw_bytes = None
            raw_mb_per_hour = None
            if size1 is not None:
                base = size0 or 0
                raw_bytes = max(0, size1 - base)
                raw_mb_per_hour = (float(raw_bytes) / float(dur)) * 3600.0 / 1024.0 / 1024.0

            cal = _calibration_stats(raw_path, ts_from=ts_from, ts_to=ts_to)
            th_ok = _thresholds_ok(raw_path)
            mt = _metrics_tick_stats(metrics_events_path, ts_from=ts_from, ts_to=ts_to)

            ws_book_msgs = float(last_status.get("ws_book_msgs") or 0) - float(st0.get("ws_book_msgs") or 0)
            ws_other_msgs = float(last_status.get("ws_other_msgs") or 0) - float(st0.get("ws_other_msgs") or 0)
            rest_polls = float(last_status.get("rest_polls") or 0) - float(st0.get("rest_polls") or 0)
            rebase_count = float(last_status.get("rebase_count") or 0) - float(st0.get("rebase_count") or 0)
            raw_events_written = float(last_status.get("raw_events_written") or 0) - float(st0.get("raw_events_written") or 0)
            books_delta_written = float(last_status.get("books_delta_written") or 0) - float(
                st0.get("books_delta_written") or 0
            )

            row = {
                "ts_iso": _now_iso(),
                "tier_tokens": tier_n,
                "picked_markets": len(picked),
                "duration_s": dur,
                "ws_book_msgs": int(ws_book_msgs),
                "ws_other_msgs": int(ws_other_msgs),
                "rest_polls": int(rest_polls),
                "rebase_count": int(rebase_count),
                "raw_events_written": int(raw_events_written),
                "books_delta_written": int(books_delta_written),
                "ws_book_msgs_per_s": ws_book_msgs / float(dur),
                "ws_other_msgs_per_s": ws_other_msgs / float(dur),
                "raw_events_per_s": raw_events_written / float(dur),
                "raw_bytes": raw_bytes,
                "raw_mb_per_hour": raw_mb_per_hour,
                "compares": cal.get("compares"),
                "tob_match_rate": cal.get("tob_match_rate"),
                "mismatch_notional_p90": cal.get("mismatch_notional_p90"),
                "thresholds_ok": th_ok,
                "metrics_tick_count": mt.get("metrics_tick_count"),
                "metrics_tick_tokens": mt.get("metrics_tick_tokens"),
                "metrics_mid_cov": mt.get("metrics_mid_cov"),
                "metrics_spread_cov": mt.get("metrics_spread_cov"),
                "metrics_depth_cov": mt.get("metrics_depth_cov"),
                "metrics_slippage_cov": mt.get("metrics_slippage_cov"),
            }
            w.writerow(row)
            f.flush()

            print(
                f"[tier {tier_n}] dur={dur}s mkts={len(picked)} tok={len(tokens)} "
                f"delta={raw_bytes}B (~{(raw_mb_per_hour or 0):.2f}MB/h) "
                f"delta_w={row['books_delta_written']} "
                f"ws_book/s={row['ws_book_msgs_per_s']:.2f} rebase={row['rebase_count']} "
                f"compares={row['compares']} tob={row['tob_match_rate']} "
                f"p90={row['mismatch_notional_p90']} thresholds_ok={row['thresholds_ok']} "
                f"mtick={row['metrics_tick_count']} mtok={row['metrics_tick_tokens']} "
                f"m_cov(mid/sp/depth/slip)="
                f"{row['metrics_mid_cov']}/{row['metrics_spread_cov']}/"
                f"{row['metrics_depth_cov']}/{row['metrics_slippage_cov']}"
            )

    print(f"[ok] wrote CSV: {out_path}")


if __name__ == "__main__":
    main()
