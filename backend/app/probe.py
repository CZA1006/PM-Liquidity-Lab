from __future__ import annotations

import argparse
import asyncio
import ast
import json
import logging
import os
import statistics
import time
from typing import Dict, List, Optional

from .config import settings
from .logging_setup import setup_logging
from .storage import JsonlEventWriter, default_eventlog_path, default_report_path, ensure_dir, safe_write
from .gamma import GammaClient, pick_tokens_from_markets
from .clob_rest import ClobRestClient
from .ws_client import MarketWsClient
from .book_engine import BookState, compute_liquidity_metrics
from .calibrator import Calibrator
from . import schemas
from .normalize import extract_exchange_ts_ms, normalize_book
from .report import ProbeStats

log = logging.getLogger("probe")

async def run_probe(duration_sec: int, keyword: str, pick_tokens: int, markets_limit: int) -> int:
    setup_logging("INFO")
    ensure_dir(settings.DATA_DIR)

    run_dir = os.path.join(settings.DATA_DIR, "runs", settings.RUN_ID)
    ensure_dir(run_dir)
    eventlog_path = default_eventlog_path(settings.DATA_DIR, settings.RUN_ID)
    report_path = default_report_path(settings.DATA_DIR, settings.RUN_ID)

    writer = JsonlEventWriter(eventlog_path, run_id=settings.RUN_ID)
    stats = ProbeStats()

    # --- Phase 1.2 configs (minimal) ---
    NOTIONAL_LIST = [100.0, 500.0, 1000.0]
    DEPTH_BPS_LIST = [10, 25, 50, 100]
    TOPK_IMB = 10

    # 1) Discover tokens via Gamma (prefer public-search)
    gamma = GammaClient(settings.GAMMA_HTTP_BASE)
    token_ids: List[str] = []

    def _normalize_token_ids(ids_raw):
        """
        Robustly normalize Gamma clob token ids into list[str].
        Handles:
          - list[str]
          - str that is JSON array: '["id1","id2"]'
          - str that is JSON-encoded string of JSON array: '"[\\"id1\\",\\"id2\\"]"'
          - list containing a JSON array string: ['["id1","id2"]']
          - odd quoting via ast.literal_eval fallback
        """
        def _parse_array_string(s: str):
            s0 = s.strip()
            if not s0:
                return []

            if (s0.startswith('"') and s0.endswith('"')) or (s0.startswith("'") and s0.endswith("'")):
                s0 = s0[1:-1].strip()

            if s0.startswith("[") and s0.endswith("]"):
                try:
                    v = json.loads(s0)
                    if isinstance(v, list):
                        return [str(x) for x in v if x]
                except Exception:
                    pass

                try:
                    s1 = s0.replace('\\"', '"')
                    v = json.loads(s1)
                    if isinstance(v, list):
                        return [str(x) for x in v if x]
                except Exception:
                    pass

                try:
                    v = ast.literal_eval(s0)
                    if isinstance(v, list):
                        return [str(x) for x in v if x]
                except Exception:
                    pass

            return [s0]

        if ids_raw is None:
            return []

        if isinstance(ids_raw, list):
            out: List[str] = []
            for x in ids_raw:
                if x is None:
                    continue
                if isinstance(x, str):
                    out.extend(_parse_array_string(x))
                else:
                    out.append(str(x))
            seen = set()
            dedup: List[str] = []
            for t in out:
                if t and t not in seen:
                    seen.add(t)
                    dedup.append(t)
            return dedup

        if isinstance(ids_raw, str):
            return _parse_array_string(ids_raw)

        return [str(ids_raw)]

    # --- public-search path ---
    candidate_token_ids: List[str] = []
    if keyword:
        try:
            s = await gamma.public_search(
                q=keyword, limit_per_type=25, page=0, events_status="active", cache=True
            )
            events = s.get("events") or []
            for ev in events:
                for m in (ev.get("markets") or []):
                    ids_raw = (
                        m.get("clob_token_ids")
                        or m.get("clobTokenIds")
                        or m.get("clobTokenIDs")
                        or []
                    )
                    for tid in _normalize_token_ids(ids_raw):
                        if tid and tid not in candidate_token_ids:
                            candidate_token_ids.append(tid)
                if len(candidate_token_ids) >= 30:
                    break
        except Exception as e:
            log.warning(f"public_search failed, fallback to list_markets: {e!r}")

    # --- fallback list_markets path ---
    if not candidate_token_ids:
        markets = await gamma.list_markets(
            limit=markets_limit,
            offset=0,
            active=True,
            closed=False,
            archived=False,
        )
        candidate_token_ids = pick_tokens_from_markets(markets, keyword=keyword, pick_n=30)

    # --- Phase 1.1: choose active tokens by one /books snapshot ---
    rest = ClobRestClient(settings.CLOB_HTTP_BASE)
    cand = candidate_token_ids[:30]
    snaps = await rest.post_books(cand) if cand else []

    def _score_book(snap: dict) -> float:
        bids = snap.get("bids") or []
        asks = snap.get("asks") or []
        nb = len(bids)
        na = len(asks)
        two = 1 if (nb > 0 and na > 0) else 0
        return two * 1_000_000 + min(nb, na) * 10_000 + (nb + na) * 10

    scored = []
    for s in snaps or []:
        tid = str(s.get("asset_id") or s.get("token_id") or "")
        if not tid:
            continue
        scored.append((tid, _score_book(s)))

    scored.sort(key=lambda x: x[1], reverse=True)
    picked_ranked = [tid for tid, _ in scored]

    if not picked_ranked:
        picked_ranked = cand

    token_ids = picked_ranked[:pick_tokens]

    if not token_ids:
        log.error(f"No token_ids found for keyword={keyword!r}. Try another keyword.")
        return 2

    # IMPORTANT: sanity check - token ids must NOT contain '[' or ']'
    if any(("[" in t or "]" in t) for t in token_ids):
        log.error(f"Token parsing still wrong, got bracket token_ids: {token_ids}")
        return 2

    log.info(f"Picked tokens ({len(token_ids)}): {token_ids}")
    safe_write(
        writer,
        schemas.make_envelope(
            event="probe_start",
            schema=schemas.PROBE_START_V1,
            run_id=settings.RUN_ID,
            token_ids=token_ids,
            keyword=keyword,
            duration_sec=duration_sec,
        ),
        on_error_event=schemas.make_envelope(
            event="generic_error",
            schema=schemas.GENERIC_ERROR_V1,
            run_id=settings.RUN_ID,
            stage="probe_start",
        ),
    )

    def _now_ms() -> int:
        return int(time.time() * 1000)

    def _get_dict_attr(obj, name: str) -> dict:
        d = getattr(obj, name, None)
        if d is None:
            d = {}
            setattr(obj, name, d)
        return d

    def _inc_counter(d: dict, key: str, n: int = 1) -> None:
        d[key] = d.get(key, 0) + n

    def _add_spread_sample(token_id: str, value: float) -> None:
        samples = _get_dict_attr(stats, "liq_spread_bps_samples_by_token")
        arr = samples.get(token_id)
        if arr is None:
            arr = []
            samples[token_id] = arr
        arr.append(value)

    def _add_filled(token_id: str, notional_k: str, buy_ok: bool, sell_ok: bool) -> None:
        filled = _get_dict_attr(stats, "liq_filled_by_token")
        by_token = filled.get(token_id)
        if by_token is None:
            by_token = {}
            filled[token_id] = by_token
        st = by_token.get(notional_k)
        if st is None:
            st = {"n": 0, "buy": 0, "sell": 0}
            by_token[notional_k] = st
        st["n"] += 1
        if buy_ok:
            st["buy"] += 1
        if sell_ok:
            st["sell"] += 1

    def _write_liq_snapshot(book: dict, source: str, token_id: str, run_id: str) -> None:
        try:
            metrics = compute_liquidity_metrics(
                book,
                notional_list=NOTIONAL_LIST,
                depth_bps_list=DEPTH_BPS_LIST,
                topk_imbalance=TOPK_IMB,
            )
            if metrics is None:
                return
            exchange_ts_ms = extract_exchange_ts_ms(book) if isinstance(book, dict) else None
            safe_write(
                writer,
                schemas.make_envelope(
                    event="liq_snapshot",
                    schema=schemas.LIQ_SNAPSHOT_V1,
                    run_id=run_id,
                    source=source,
                    token_id=token_id,
                    exchange_ts_ms=exchange_ts_ms,
                    metrics=metrics,
                ),
                on_error_event=schemas.make_envelope(
                    event="liq_snapshot_error",
                    schema=schemas.LIQ_SNAPSHOT_ERROR_V1,
                    run_id=run_id,
                    source=source,
                    token_id=token_id,
                ),
            )
            src = _get_dict_attr(stats, "liq_snapshot_sources")
            _inc_counter(src, source, 1)

            sbps = (metrics or {}).get("spread_bps")
            if isinstance(sbps, (int, float)):
                _add_spread_sample(token_id, float(sbps))

            filled = (metrics or {}).get("slippage_filled") or {}
            buy_map = filled.get("buy") or {}
            sell_map = filled.get("sell") or {}
            for n in NOTIONAL_LIST:
                k = str(n)
                _add_filled(
                    token_id,
                    k,
                    bool(buy_map.get(k, False)),
                    bool(sell_map.get(k, False)),
                )
            stats.liq_snapshots = getattr(stats, "liq_snapshots", 0) + 1
            by = getattr(stats, "liq_updates_by_token", None)
            if by is None:
                stats.liq_updates_by_token = {}
                by = stats.liq_updates_by_token
            by[token_id] = by.get(token_id, 0) + 1
        except Exception as e:
            safe_write(
                writer,
                schemas.make_envelope(
                    event="liq_snapshot_error",
                    schema=schemas.LIQ_SNAPSHOT_ERROR_V1,
                    run_id=run_id,
                    source=source,
                    token_id=token_id,
                    error=repr(e),
                ),
                on_error_event=schemas.make_envelope(
                    event="generic_error",
                    schema=schemas.GENERIC_ERROR_V1,
                    run_id=run_id,
                    stage="liq_snapshot_error_write",
                ),
            )
            stats.liq_snapshot_errors = getattr(stats, "liq_snapshot_errors", 0) + 1

    def _extract_msg_type(msg: dict) -> str:
        if not isinstance(msg, dict):
            return "unknown"
        mt = msg.get("type") or msg.get("event_type") or msg.get("eventType")
        return str(mt) if mt is not None else "unknown"

    def _extract_token_id(msg: dict) -> Optional[str]:
        if not isinstance(msg, dict):
            return None
        for k in ("asset_id", "assetId", "token_id", "tokenId"):
            v = msg.get(k)
            if v:
                return str(v)
        for k in ("price_changes", "priceChanges", "changes"):
            arr = msg.get(k)
            if isinstance(arr, list):
                ids: List[str] = []
                for item in arr:
                    if not isinstance(item, dict):
                        continue
                    v = item.get("asset_id") or item.get("assetId") or item.get("token_id") or item.get("tokenId")
                    if v:
                        ids.append(str(v))
                if not ids:
                    continue
                uniq = list(dict.fromkeys(ids))
                if len(uniq) == 1:
                    return uniq[0]
                matches = [i for i in uniq if i in token_ids]
                if len(matches) == 1:
                    return matches[0]
        return None

    def _write_ws_msg(msg: dict) -> None:
        safe_write(
            writer,
            schemas.make_envelope(
                event="ws_other",
                schema=schemas.WS_OTHER_V1,
                run_id=settings.RUN_ID,
                msg_type=_extract_msg_type(msg),
                token_id=_extract_token_id(msg),
                msg=msg,
            ),
            on_error_event=schemas.make_envelope(
                event="generic_error",
                schema=schemas.GENERIC_ERROR_V1,
                run_id=settings.RUN_ID,
                stage="ws_other_write",
            ),
        )

    # 2) In-memory books
    books: Dict[str, BookState] = {tid: BookState(token_id=tid) for tid in token_ids}

    # 3) WS stream task
    stop_event = asyncio.Event()
    ws = MarketWsClient(settings.CLOB_WS_BASE, channel=settings.CLOB_WS_CHANNEL)

    async def ws_task():
        async for msg in ws.connect_and_stream(token_ids, stop_event):
            stats.ws_msgs += 1
            # ---- WS batch book handling (list of books wrapped as __non_dict__) ----
            if (
                isinstance(msg, dict)
                and msg.get("event_type") == "__non_dict__"
                and isinstance(msg.get("data"), list)
            ):
                batch = msg.get("data") or []
                if batch and all(isinstance(x, dict) for x in batch):
                    for item in batch:
                        et = item.get("event_type")
                        if et == "book":
                            stats.ws_book_msgs += 1
                            nb = normalize_book(item, "ws")
                            tid = nb.get("token_id") or str(item.get("asset_id") or "")
                            if tid:
                                stats.add_book_update(tid)

                            try:
                                ts_ms = int(item.get("timestamp"))
                                lag = int(time.time() * 1000) - ts_ms
                                if 0 <= lag <= 60_000:
                                    stats.add_ws_lag(lag)
                            except Exception:
                                pass

                            if tid and tid in books:
                                books[tid].apply_snapshot(
                                    bids=nb.get("bids") or [],
                                    asks=nb.get("asks") or [],
                                    ts_ms=_now_ms(),
                                    book_hash=item.get("hash"),
                                    source="ws",
                                    exchange_ts_ms=nb.get("exchange_ts_ms"),
                                )

                            safe_write(
                                writer,
                                schemas.make_envelope(
                                    event="ws_book",
                                    schema=schemas.WS_BOOK_V1,
                                    run_id=settings.RUN_ID,
                                    exchange_ts_ms=nb.get("exchange_ts_ms"),
                                    msg=item,
                                ),
                                on_error_event=schemas.make_envelope(
                                    event="generic_error",
                                    schema=schemas.GENERIC_ERROR_V1,
                                    run_id=settings.RUN_ID,
                                    stage="ws_book_write",
                                ),
                            )
                            if tid:
                                _write_liq_snapshot(item, "ws", tid, settings.RUN_ID)
                        else:
                            stats.ws_other_msgs += 1
                            _write_ws_msg(item)
                    continue
            # ---- end batch handling ----
            if not isinstance(msg, dict):
                stats.ws_other_msgs += 1
                safe_write(
                    writer,
                    schemas.make_envelope(
                        event="ws_non_dict",
                        schema=schemas.WS_NON_DICT_V1,
                        run_id=settings.RUN_ID,
                        msg=msg,
                    ),
                    on_error_event=schemas.make_envelope(
                        event="generic_error",
                        schema=schemas.GENERIC_ERROR_V1,
                        run_id=settings.RUN_ID,
                        stage="ws_non_dict_write",
                    ),
                )
                continue
            et = msg.get("event_type")
            if et == "book":
                stats.ws_book_msgs += 1
                nb = normalize_book(msg, "ws")
                tid = nb.get("token_id") or str(msg.get("asset_id") or "")
                if tid:
                    stats.add_book_update(tid)

                # estimate lag if timestamp present
                try:
                    ts_ms = int(msg.get("timestamp"))
                    lag = int(time.time() * 1000) - ts_ms
                    if 0 <= lag <= 60_000:
                        stats.add_ws_lag(lag)
                except Exception:
                    pass

                # apply snapshot
                if tid and tid in books:
                    books[tid].apply_snapshot(
                        bids=nb.get("bids") or [],
                        asks=nb.get("asks") or [],
                        ts_ms=_now_ms(),
                        book_hash=msg.get("hash"),
                        source="ws",
                        exchange_ts_ms=nb.get("exchange_ts_ms"),
                    )

                safe_write(
                    writer,
                    schemas.make_envelope(
                        event="ws_book",
                        schema=schemas.WS_BOOK_V1,
                        run_id=settings.RUN_ID,
                        exchange_ts_ms=nb.get("exchange_ts_ms"),
                        msg=msg,
                    ),
                    on_error_event=schemas.make_envelope(
                        event="generic_error",
                        schema=schemas.GENERIC_ERROR_V1,
                        run_id=settings.RUN_ID,
                        stage="ws_book_write",
                    ),
                )
                if tid:
                    _write_liq_snapshot(msg, "ws", tid, settings.RUN_ID)

            else:
                stats.ws_other_msgs += 1
                _write_ws_msg(msg)

    # 4) Calibration task
    class _RestWithLiq:
        def __init__(self, inner: ClobRestClient):
            self._inner = inner

        async def post_books(self, token_ids: List[str]):
            snaps = await self._inner.post_books(token_ids)
            for snap in snaps or []:
                if not isinstance(snap, dict):
                    continue
                nb = normalize_book(snap, "rest")
                tid = nb.get("token_id") or str(snap.get("asset_id") or snap.get("token_id") or "")
                ex_ts = nb.get("exchange_ts_ms")
                safe_write(
                    writer,
                    schemas.make_envelope(
                        event="rest_book",
                        schema=schemas.REST_BOOK_V1,
                        run_id=settings.RUN_ID,
                        exchange_ts_ms=ex_ts,
                        msg=snap,
                    ),
                    on_error_event=schemas.make_envelope(
                        event="generic_error",
                        schema=schemas.GENERIC_ERROR_V1,
                        run_id=settings.RUN_ID,
                        stage="rest_book_write",
                    ),
                )
                if tid:
                    _write_liq_snapshot(snap, "rest", tid, settings.RUN_ID)
            return snaps

    rest_for_cal = _RestWithLiq(rest)

    async def on_rebase(e):
        stats.rebase_count += 1
        if isinstance(e, dict):
            payload = {k: v for k, v in e.items() if k not in ("event", "schema", "run_id", "ts_ms")}
            ts_ms = e.get("ts_ms")
        else:
            payload = {"data": e}
            ts_ms = None
        safe_write(
            writer,
            schemas.make_envelope(
                event="rebase",
                schema=schemas.REBASE_V1,
                run_id=settings.RUN_ID,
                ts_ms=ts_ms,
                **payload,
            ),
            on_error_event=schemas.make_envelope(
                event="generic_error",
                schema=schemas.GENERIC_ERROR_V1,
                run_id=settings.RUN_ID,
                stage="rebase_write",
            ),
        )

    async def on_compare(e):
        stats.books_polls += 1  # count per token compare, not per poll; fine for MVP
        cmp = e.get("compare", {}) if isinstance(e, dict) else {}
        mismatch_levels = int(cmp.get("mismatch_levels", 0))
        stats.mismatch_level_samples.append(mismatch_levels)

        tob_mismatch = (cmp.get("a_best_bid") != cmp.get("b_best_bid")) or (cmp.get("a_best_ask") != cmp.get("b_best_ask"))
        if tob_mismatch:
            stats.tob_mismatch_samples += 1

        if isinstance(e, dict):
            payload = {k: v for k, v in e.items() if k not in ("event", "schema", "run_id", "ts_ms")}
            ts_ms = e.get("ts_ms")
        else:
            payload = {"data": e}
            ts_ms = None
        safe_write(
            writer,
            schemas.make_envelope(
                event="calibration_compare",
                schema=schemas.CALIBRATION_COMPARE_V1,
                run_id=settings.RUN_ID,
                ts_ms=ts_ms,
                **payload,
            ),
            on_error_event=schemas.make_envelope(
                event="generic_error",
                schema=schemas.GENERIC_ERROR_V1,
                run_id=settings.RUN_ID,
                stage="calibration_compare_write",
            ),
        )

    calibrator = Calibrator(rest_for_cal, interval_sec=settings.CALIBRATION_INTERVAL_SEC, top_n=settings.CALIBRATION_TOP_N)

    async def calibrator_task():
        await calibrator.run(token_ids, books, stop_event, on_rebase, on_compare)

    # 5) Run for duration
    t_end = time.time() + duration_sec
    tasks = [
        asyncio.create_task(ws_task()),
        asyncio.create_task(calibrator_task()),
    ]

    try:
        while time.time() < t_end:
            await asyncio.sleep(1.0)
    finally:
        stop_event.set()
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    # 6) Write report
    report = stats.to_report()

    spread_summary = {}
    spread_samples = getattr(stats, "liq_spread_bps_samples_by_token", {}) or {}
    for tid, arr in (spread_samples or {}).items():
        if not arr:
            continue
        xs = sorted(arr)
        p50 = statistics.median(xs)
        p90 = xs[int(0.9 * (len(xs) - 1))] if len(xs) >= 2 else xs[0]
        spread_summary[tid] = {"n": len(xs), "p50": p50, "p90": p90}

    filled_rates = {}
    filled = getattr(stats, "liq_filled_by_token", {}) or {}
    for tid, mp in (filled or {}).items():
        if not isinstance(mp, dict):
            continue
        out = {}
        for notional_k, st in mp.items():
            n = int((st or {}).get("n", 0))
            if n <= 0:
                continue
            buy = int((st or {}).get("buy", 0))
            sell = int((st or {}).get("sell", 0))
            out[notional_k] = {
                "n": n,
                "buy_rate": buy / n,
                "sell_rate": sell / n,
            }
        if out:
            filled_rates[tid] = out

    report.update({
        "run_id": settings.RUN_ID,
        "token_ids": token_ids,
        "keyword": keyword,
        "duration_sec": duration_sec,
        "eventlog_path": eventlog_path,
        "report_path": report_path,
        "liq_snapshots": getattr(stats, "liq_snapshots", 0),
        "liq_updates_by_token": getattr(stats, "liq_updates_by_token", {}),
        "liq_snapshot_sources": getattr(stats, "liq_snapshot_sources", {}),
        "liq_snapshot_errors": getattr(stats, "liq_snapshot_errors", 0),
        "liq_spread_bps_by_token": spread_summary,
        "liq_filled_by_token": filled_rates,
    })

    with open(report_path, "w", encoding="utf-8") as f:
        import json
        json.dump(report, f, indent=2, ensure_ascii=False)

    log.info(f"✅ Probe report written: {report_path}")
    log.info(f"✅ Raw events written:  {eventlog_path}")
    log.info(report)
    return 0

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--duration-sec", type=int, default=600, help="Run duration in seconds (default 600=10min)")
    p.add_argument("--keyword", type=str, default=settings.PROBE_KEYWORD)
    p.add_argument("--pick-tokens", type=int, default=settings.PROBE_PICK_TOKENS)
    p.add_argument("--markets-limit", type=int, default=settings.PROBE_MARKETS_LIMIT)
    args = p.parse_args()

    raise SystemExit(asyncio.run(run_probe(args.duration_sec, args.keyword, args.pick_tokens, args.markets_limit)))

if __name__ == "__main__":
    main()
