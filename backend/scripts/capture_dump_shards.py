#!/usr/bin/env python3
from __future__ import annotations

import argparse
import concurrent.futures
import csv
import json
import random
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from _http import http_get_json, http_post_json


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


def _sizeof(path: Optional[Path]) -> Optional[int]:
    if not path:
        return None
    try:
        return path.stat().st_size
    except Exception:
        return None


def _mbh(delta_bytes: Optional[int], duration_s: int) -> Optional[float]:
    if delta_bytes is None or duration_s <= 0:
        return None
    return (float(delta_bytes) / float(duration_s)) * 3600.0 / 1024.0 / 1024.0


def _parse_json_list(v: Any) -> List[Any]:
    if v is None:
        return []
    if isinstance(v, list):
        return v
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return []
        if s.startswith("["):
            try:
                arr = json.loads(s)
                if isinstance(arr, list):
                    return arr
            except Exception:
                return []
        return [s]
    return [v]


def _parse_token_ids(m: Dict[str, Any]) -> List[str]:
    raw = m.get("_clobTokenIds_norm")
    if raw is None:
        raw = m.get("clobTokenIds") or m.get("clob_token_ids") or []
    out: List[str] = []
    for x in _parse_json_list(raw):
        s = str(x).strip()
        if s:
            out.append(s)
    return out


def _market_volume_score(m: Dict[str, Any]) -> float:
    # Gamma payloads vary; try common volume-like fields.
    for k in (
        "volumeNum",
        "volume",
        "volume24hr",
        "volume24h",
        "liquidityNum",
        "liquidity",
    ):
        v = m.get(k)
        try:
            if v is not None:
                return float(v)
        except Exception:
            continue
    return 0.0


def _collect_tokens_from_markets(
    markets: Iterable[Dict[str, Any]],
    monitorable_only: bool = True,
    sort_by: str = "none",
    shuffle_seed: Optional[int] = None,
) -> List[str]:
    seen: set[str] = set()
    out: List[str] = []
    first_idx: Dict[str, int] = {}
    token_score: Dict[str, float] = {}

    for m in markets:
        if not isinstance(m, dict):
            continue
        if monitorable_only:
            if m.get("archived") is True:
                continue
            if m.get("enableOrderBook") is False:
                continue
            # Prefer markets that are likely tradable now.
            if m.get("closed") is True:
                continue
            if m.get("acceptingOrders") is False:
                continue
            if m.get("active") is False:
                continue
        tids = _parse_token_ids(m)
        score = _market_volume_score(m) if sort_by == "volume" else 0.0
        for t in tids:
            if t not in seen:
                seen.add(t)
                first_idx[t] = len(out)
                out.append(t)
            if sort_by == "volume":
                prev = token_score.get(t, float("-inf"))
                if score > prev:
                    token_score[t] = score

    if sort_by == "volume":
        out.sort(key=lambda t: (-token_score.get(t, 0.0), first_idx.get(t, 0)))

    if shuffle_seed is not None:
        rnd = random.Random(int(shuffle_seed))
        rnd.shuffle(out)

    return out


def _collect_tokens_from_dump(
    dump_path: Path,
    monitorable_only: bool = True,
    sort_by: str = "none",
    shuffle_seed: Optional[int] = None,
) -> List[str]:
    markets: List[Dict[str, Any]] = []
    with open(dump_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                m = json.loads(line)
            except Exception:
                continue
            if isinstance(m, dict):
                markets.append(m)
    return _collect_tokens_from_markets(
        markets,
        monitorable_only=monitorable_only,
        sort_by=sort_by,
        shuffle_seed=shuffle_seed,
    )


def _collect_tokens_from_active_api(
    api: str,
    monitorable_only: bool = True,
    sort_by: str = "none",
    shuffle_seed: Optional[int] = None,
    page_size: int = 200,
    max_pages: int = 500,
) -> List[str]:
    markets: List[Dict[str, Any]] = []
    offset = 0
    for _ in range(max_pages):
        url = (
            f"{api.rstrip('/')}/gamma/active_markets"
            f"?limit={int(page_size)}&offset={int(offset)}&active_only=true"
        )
        payload = http_get_json(url)
        if not isinstance(payload, dict):
            break
        batch = payload.get("markets") or []
        if not isinstance(batch, list) or not batch:
            break
        for m in batch:
            if isinstance(m, dict):
                markets.append(m)
        offset += int(page_size)
    return _collect_tokens_from_markets(
        markets,
        monitorable_only=monitorable_only,
        sort_by=sort_by,
        shuffle_seed=shuffle_seed,
    )


def _chunks(xs: List[str], n: int) -> Iterable[List[str]]:
    n = max(1, int(n))
    for i in range(0, len(xs), n):
        yield xs[i : i + n]


def _resolve_metrics_events_path(status: Dict[str, Any]) -> Optional[Path]:
    run_id = status.get("run_id")
    if isinstance(run_id, str) and run_id:
        # 1) Absolute DATA_DIR style path based on raw_log_path: .../data/raw/<run>/events-*.jsonl
        raw_path = status.get("raw_log_path")
        if isinstance(raw_path, str) and raw_path:
            rp = _resolve_path(raw_path)
            if rp and rp.is_absolute():
                parts = rp.parts
                try:
                    i_data = parts.index("data")
                    i_raw = parts.index("raw")
                    if i_raw == i_data + 1 and i_raw + 1 < len(parts):
                        run = parts[i_raw + 1]
                        base = Path(*parts[: i_data + 1])
                        c0 = (base / "runs" / run / "events.jsonl").resolve()
                        if c0.exists():
                            return c0
                except ValueError:
                    pass
        # 2) Relative project paths.
        c1 = _resolve_path(f"./data/runs/{run_id}/events.jsonl")
        if c1:
            return c1
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
    mid = spread = depth = slippage = 0
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


def _status(api: str) -> Dict[str, Any]:
    out = http_get_json(f"{api.rstrip('/')}/monitor/status")
    return out if isinstance(out, dict) else {}


def _health_ok(api: str) -> bool:
    try:
        out = http_get_json(f"{api.rstrip('/')}/health")
        return isinstance(out, dict) and bool(out.get("ok"))
    except Exception:
        return False


def _to_int(v: Any) -> int:
    try:
        return int(v or 0)
    except Exception:
        return 0


def _run_one_shard(
    api: str,
    shard_idx: int,
    total_shards: int,
    shard: List[str],
    dump_path: Path,
    shard_duration: int,
    poll: int,
) -> Dict[str, Any]:
    http_post_json(f"{api.rstrip('/')}/monitor/set_tokens", {"token_ids": shard, "restart": True})
    st0 = _status(api)

    raw_p0 = _resolve_path(st0.get("raw_log_path"))
    delta_p0 = _resolve_path(st0.get("books_delta_log_path"))
    snap_p0 = _resolve_path(st0.get("books_snapshot_log_path"))
    raw_sz0 = _sizeof(raw_p0) or 0
    delta_sz0 = _sizeof(delta_p0) or 0
    snap_sz0 = _sizeof(snap_p0) or 0

    t0 = time.time()
    ts_from = int(t0 * 1000)
    last = st0
    while True:
        now = time.time()
        if now - t0 >= shard_duration:
            break
        try:
            cur = _status(api)
            if cur:
                last = cur
            _ = http_get_json(f"{api.rstrip('/')}/metrics/current")
        except Exception:
            pass
        time.sleep(poll)

    ts_to = int(time.time() * 1000)
    dur = max(1, int(time.time() - t0))

    raw_p1 = _resolve_path(last.get("raw_log_path"))
    delta_p1 = _resolve_path(last.get("books_delta_log_path"))
    snap_p1 = _resolve_path(last.get("books_snapshot_log_path"))
    raw_sz1 = _sizeof(raw_p1)
    delta_sz1 = _sizeof(delta_p1)
    snap_sz1 = _sizeof(snap_p1)

    raw_delta = (raw_sz1 - raw_sz0) if raw_sz1 is not None else None
    delta_delta = (delta_sz1 - delta_sz0) if delta_sz1 is not None else None
    snap_delta = (snap_sz1 - snap_sz0) if snap_sz1 is not None else None

    mt = _metrics_tick_stats(_resolve_metrics_events_path(last), ts_from, ts_to)

    row = {
        "ts_iso": _now_iso(),
        "worker_api": api,
        "dump_path": str(dump_path),
        "shard_idx": shard_idx,
        "shard_tokens": len(shard),
        "duration_s": dur,
        "ws_book_msgs": _to_int(last.get("ws_book_msgs")) - _to_int(st0.get("ws_book_msgs")),
        "ws_other_msgs": _to_int(last.get("ws_other_msgs")) - _to_int(st0.get("ws_other_msgs")),
        "rest_polls": _to_int(last.get("rest_polls")) - _to_int(st0.get("rest_polls")),
        "rebase_count": _to_int(last.get("rebase_count")) - _to_int(st0.get("rebase_count")),
        "raw_events_written": _to_int(last.get("raw_events_written")) - _to_int(st0.get("raw_events_written")),
        "books_delta_written": _to_int(last.get("books_delta_written")) - _to_int(st0.get("books_delta_written")),
        "books_snapshot_written": _to_int(last.get("books_snapshot_written"))
        - _to_int(st0.get("books_snapshot_written")),
        "books_snapshot_rle_written": _to_int(last.get("books_snapshot_rle_written"))
        - _to_int(st0.get("books_snapshot_rle_written")),
        "book_store_top_k": _to_int(last.get("book_store_top_k") or st0.get("book_store_top_k")),
        "book_rle_enabled": bool(
            last.get("book_rle_enabled")
            if "book_rle_enabled" in last
            else st0.get("book_rle_enabled")
        ),
        "raw_bytes_delta": raw_delta,
        "books_delta_bytes_delta": delta_delta,
        "books_snapshot_bytes_delta": snap_delta,
        "raw_mb_per_hour": _mbh(raw_delta, dur),
        "books_delta_mb_per_hour": _mbh(delta_delta, dur),
        "books_snapshot_mb_per_hour": _mbh(snap_delta, dur),
        "metrics_tick_count": mt.get("metrics_tick_count"),
        "metrics_tick_tokens": mt.get("metrics_tick_tokens"),
        "metrics_mid_cov": mt.get("metrics_mid_cov"),
        "metrics_spread_cov": mt.get("metrics_spread_cov"),
        "metrics_depth_cov": mt.get("metrics_depth_cov"),
        "metrics_slippage_cov": mt.get("metrics_slippage_cov"),
        "raw_log_path": last.get("raw_log_path"),
        "books_delta_log_path": last.get("books_delta_log_path"),
        "books_snapshot_log_path": last.get("books_snapshot_log_path"),
    }
    print(
        f"[shard {shard_idx}/{total_shards}] api={api} tok={len(shard)} dur={dur}s "
        f"delta={delta_delta}B (~{(_mbh(delta_delta, dur) or 0):.2f}MB/h) "
        f"raw={raw_delta}B (~{(_mbh(raw_delta, dur) or 0):.2f}MB/h) "
        f"mtick={row['metrics_tick_count']} mtok={row['metrics_tick_tokens']}"
    )
    return row


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--api", required=True, help="e.g. http://127.0.0.1:8000")
    ap.add_argument("--worker_apis", default="", help="comma-separated apis for parallel workers, e.g. http://127.0.0.1:8001,http://127.0.0.1:8002")
    ap.add_argument("--dump", required=True, help="active_markets jsonl dump path")
    ap.add_argument("--shard_tokens", type=int, default=200, help="token count per shard")
    ap.add_argument("--shard_duration", type=int, default=300, help="seconds per shard")
    ap.add_argument("--poll", type=int, default=10, help="seconds between status polls")
    ap.add_argument("--max_shards", type=int, default=0, help="0 means all shards")
    ap.add_argument(
        "--refresh_active_every_sec",
        type=int,
        default=0,
        help=">0 enables live active refresh + auto-repartition before each worker wave",
    )
    ap.add_argument("--monitorable_only", action="store_true", help="skip archived/disableOrderBook markets")
    ap.add_argument("--sort_by", choices=["none", "volume"], default="none", help="token ordering before sharding")
    ap.add_argument("--shuffle_seed", type=int, default=None, help="optional deterministic shuffle seed")
    ap.add_argument("--out", default="", help="output csv path")
    args = ap.parse_args()

    dump_path = _resolve_path(args.dump) or Path(args.dump)
    if not dump_path.exists():
        raise SystemExit(f"[err] dump not found: {dump_path}")

    tokens = _collect_tokens_from_dump(
        dump_path,
        monitorable_only=bool(args.monitorable_only),
        sort_by=str(args.sort_by),
        shuffle_seed=args.shuffle_seed,
    )
    if not tokens:
        raise SystemExit("[err] no tokens parsed from dump")

    shard_size = max(1, int(args.shard_tokens))
    shards = list(_chunks(tokens, shard_size))
    if args.max_shards and args.max_shards > 0:
        shards = shards[: args.max_shards]

    worker_apis = [x.strip() for x in str(args.worker_apis or "").split(",") if x.strip()]
    if not worker_apis:
        worker_apis = [args.api.rstrip("/")]
    else:
        worker_apis = [x.rstrip("/") for x in worker_apis]

    alive_workers = [api for api in worker_apis if _health_ok(api)]
    dead_workers = [api for api in worker_apis if api not in alive_workers]
    for api in dead_workers:
        print(f"[warn] worker unreachable (skip): {api}")
    if not alive_workers:
        raise SystemExit(
            "[err] no reachable worker api. start uvicorn first and verify /health."
        )
    worker_apis = alive_workers

    out_path = Path(args.out) if args.out else Path("backend/data/reports") / f"dump_shard_capture-{int(time.time())}.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    fields = [
        "ts_iso",
        "worker_api",
        "dump_path",
        "shard_idx",
        "shard_tokens",
        "duration_s",
        "ws_book_msgs",
        "ws_other_msgs",
        "rest_polls",
        "rebase_count",
        "raw_events_written",
        "books_delta_written",
        "books_snapshot_written",
        "books_snapshot_rle_written",
        "book_store_top_k",
        "book_rle_enabled",
        "raw_bytes_delta",
        "books_delta_bytes_delta",
        "books_snapshot_bytes_delta",
        "raw_mb_per_hour",
        "books_delta_mb_per_hour",
        "books_snapshot_mb_per_hour",
        "metrics_tick_count",
        "metrics_tick_tokens",
        "metrics_mid_cov",
        "metrics_spread_cov",
        "metrics_depth_cov",
        "metrics_slippage_cov",
        "raw_log_path",
        "books_delta_log_path",
        "books_snapshot_log_path",
    ]

    rows: List[Dict[str, Any]] = []
    refresh_sec = max(0, int(args.refresh_active_every_sec or 0))

    if refresh_sec <= 0:
        jobs: List[Tuple[int, List[str], str]] = []
        for idx, shard in enumerate(shards, start=1):
            api = worker_apis[(idx - 1) % len(worker_apis)]
            jobs.append((idx, shard, api))

        if len(worker_apis) <= 1:
            for idx, shard, api in jobs:
                rows.append(
                    _run_one_shard(
                        api=api,
                        shard_idx=idx,
                        total_shards=len(shards),
                        shard=shard,
                        dump_path=dump_path,
                        shard_duration=int(args.shard_duration),
                        poll=int(args.poll),
                    )
                )
        else:
            with concurrent.futures.ThreadPoolExecutor(max_workers=len(worker_apis)) as ex:
                futs = [
                    ex.submit(
                        _run_one_shard,
                        api,
                        idx,
                        len(shards),
                        shard,
                        dump_path,
                        int(args.shard_duration),
                        int(args.poll),
                    )
                    for idx, shard, api in jobs
                ]
                for fut in concurrent.futures.as_completed(futs):
                    rows.append(fut.result())
    else:
        # Live mode: refresh active universe and auto-repartition before worker waves.
        target_total_shards = int(args.max_shards) if int(args.max_shards) > 0 else len(shards)
        processed = 0
        next_refresh_ts = 0.0
        cur_shards: List[List[str]] = []
        cur_cursor = 0

        while processed < target_total_shards:
            now = time.time()
            need_refresh = (not cur_shards) or (now >= next_refresh_ts)
            if need_refresh:
                live_tokens = _collect_tokens_from_active_api(
                    args.api,
                    monitorable_only=bool(args.monitorable_only),
                    sort_by=str(args.sort_by),
                    shuffle_seed=args.shuffle_seed,
                )
                if live_tokens:
                    cur_shards = list(_chunks(live_tokens, shard_size))
                    cur_cursor = 0
                    next_refresh_ts = now + float(refresh_sec)
                    print(
                        f"[refresh] active tokens={len(live_tokens)} "
                        f"shards={len(cur_shards)} shard_tokens={shard_size}"
                    )
                else:
                    # Keep existing shards if refresh fails; fallback once from dump if needed.
                    if not cur_shards:
                        cur_shards = shards
                        cur_cursor = 0
                    next_refresh_ts = now + float(refresh_sec)
                    print("[warn] live active refresh returned no tokens; keep previous shard plan")

            if not cur_shards:
                break

            remaining = target_total_shards - processed
            wave_n = min(len(worker_apis), remaining, len(cur_shards) - cur_cursor)
            if wave_n <= 0:
                # Exhausted current shard plan; force immediate refresh.
                next_refresh_ts = 0.0
                continue

            wave_jobs: List[Tuple[int, List[str], str]] = []
            for i in range(wave_n):
                shard_idx = processed + i + 1
                shard = cur_shards[cur_cursor + i]
                api = worker_apis[i % len(worker_apis)]
                wave_jobs.append((shard_idx, shard, api))

            if len(wave_jobs) <= 1:
                idx, shard, api = wave_jobs[0]
                rows.append(
                    _run_one_shard(
                        api=api,
                        shard_idx=idx,
                        total_shards=target_total_shards,
                        shard=shard,
                        dump_path=dump_path,
                        shard_duration=int(args.shard_duration),
                        poll=int(args.poll),
                    )
                )
            else:
                with concurrent.futures.ThreadPoolExecutor(max_workers=len(wave_jobs)) as ex:
                    futs = [
                        ex.submit(
                            _run_one_shard,
                            api,
                            idx,
                            target_total_shards,
                            shard,
                            dump_path,
                            int(args.shard_duration),
                            int(args.poll),
                        )
                        for idx, shard, api in wave_jobs
                    ]
                    for fut in concurrent.futures.as_completed(futs):
                        rows.append(fut.result())

            processed += wave_n
            cur_cursor += wave_n

    rows.sort(key=lambda r: int(r.get("shard_idx") or 0))
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)

    print(f"[ok] wrote CSV: {out_path}")


if __name__ == "__main__":
    main()
