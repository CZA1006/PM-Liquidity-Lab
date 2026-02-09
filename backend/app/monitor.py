from __future__ import annotations

import asyncio
import os
import random
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple

from . import schemas
from .book_engine import BookState, compare_top_n, compute_liquidity_metrics
from .clob_rest import ClobRestClient
from .config import Settings
from .gamma import GammaClient, pick_tokens_from_markets
from .normalize import extract_exchange_ts_ms, normalize_book
from .storage import JsonlEventWriter, default_eventlog_path, default_run_dir, ensure_dir, safe_write
from .raw_log import RawLogWriter
from .calibrator import CompareResult, should_rebase
from .ws_client import MarketWsClient


def _now_ms() -> int:
    import time

    return int(time.time() * 1000)


def _split_csv(s: str) -> List[str]:
    s = (s or "").strip()
    if not s:
        return []
    return [x.strip() for x in s.split(",") if x.strip()]


def _normalize_token_ids(ids_raw: Any) -> List[str]:
    if ids_raw is None:
        return []
    if isinstance(ids_raw, list):
        out: List[str] = []
        for x in ids_raw:
            if x is None:
                continue
            out.extend(_normalize_token_ids(x))
        return [t for t in out if t]
    if isinstance(ids_raw, str):
        s0 = ids_raw.strip()
        if s0.startswith("[") and s0.endswith("]"):
            try:
                import json

                v = json.loads(s0)
                if isinstance(v, list):
                    return [str(x) for x in v if x]
            except Exception:
                return [s0]
        return [s0]
    return [str(ids_raw)]


# ---- calibration env ----
CALIB_TOPN = int(os.getenv("CALIB_TOPN", "50"))
CALIB_MISMATCH_LEVELS = int(os.getenv("CALIB_MISMATCH_LEVELS", "4"))
CALIB_MISMATCH_NOTIONAL = float(os.getenv("CALIB_MISMATCH_NOTIONAL", "50.0"))
CALIB_TOB_MUST_MATCH = os.getenv("CALIB_TOB_MUST_MATCH", "1") not in ("0", "false", "False")
CALIB_LOG_SAMPLE_RATE = float(os.getenv("CALIB_LOG_SAMPLE_RATE", "0.02"))


@dataclass
class MonitorStatus:
    run_id: str
    started: bool
    token_ids: List[str]
    ws_connected: bool
    last_publish_ts_ms: int
    last_rest_poll_ts_ms: int
    ws_book_msgs: int
    ws_other_msgs: int
    rest_polls: int
    rebase_count: int
    raw_log_path: str = ""
    raw_events_written: int = 0
    raw_log_dir: str = ""
    books_delta_log_path: str = ""
    books_snapshot_log_path: str = ""
    books_delta_written: int = 0
    books_snapshot_written: int = 0
    compare_total: int = 0
    compare_tob_match: int = 0
    compare_mismatch_notional_samples: List[float] = field(default_factory=list)
    last_compare: Optional[Dict[str, Any]] = None
    last_rest_snapshot_ts_ms: Optional[int] = None
    # ---- effective config (debug / acceptance) ----
    effective_rest_poll_sec: float = 0.0
    effective_calibration_interval_sec: int = 0
    effective_calibration_top_n: int = 0
    effective_monitor_mismatch_levels: int = 0


class MetricsHub:
    """
    Fan-out publish-subscribe for SSE streams.
    Each subscriber gets its own asyncio.Queue (bounded).
    """

    def __init__(self, *, queue_max: int = 2000) -> None:
        self._queue_max = int(queue_max)
        self._subs: Set[asyncio.Queue] = set()
        self._lock = asyncio.Lock()

    async def subscribe(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=self._queue_max)
        async with self._lock:
            self._subs.add(q)
        return q

    async def unsubscribe(self, q: asyncio.Queue) -> None:
        async with self._lock:
            self._subs.discard(q)

    async def publish(self, item: Dict[str, Any]) -> None:
        async with self._lock:
            subs = list(self._subs)
        for q in subs:
            try:
                q.put_nowait(item)
            except Exception:
                pass


class Monitor:
    """
    Long-running monitor:
      - token selection (explicit token_ids OR keyword search)
      - WS book updates
      - periodic REST /books snapshots for drift correction
      - compute metrics on each applied book update
      - publish metrics via MetricsHub
    """

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.run_id = settings.RUN_ID

        self.gamma = GammaClient(settings.GAMMA_HTTP_BASE)
        self.rest = ClobRestClient(settings.CLOB_HTTP_BASE)

        run_dir = default_run_dir(settings.DATA_DIR, settings.RUN_ID)
        ensure_dir(run_dir)
        eventlog_path = default_eventlog_path(settings.DATA_DIR, settings.RUN_ID)
        self.writer = JsonlEventWriter(eventlog_path, run_id=self.run_id)
        self.raw = RawLogWriter(self.run_id)

        self.hub = MetricsHub(queue_max=settings.MONITOR_SSE_QUEUE_MAX)

        self._started = False
        self._tasks: List[asyncio.Task] = []
        self._stop_evt = asyncio.Event()

        self.token_ids: List[str] = []
        self._override_token_ids: Optional[List[str]] = None
        self.book_states: Dict[str, BookState] = {}
        self.latest_metrics: Dict[str, Dict[str, Any]] = {}

        self.ws_connected: bool = False
        self.last_publish_ts_ms: int = 0
        self.last_rest_poll_ts_ms: int = 0
        self.ws_book_msgs: int = 0
        self.ws_other_msgs: int = 0
        self.rest_polls: int = 0
        self.rebase_count: int = 0
        self.last_trade_price: Dict[str, float] = {}
        self.compare_total: int = 0
        self.compare_tob_match: int = 0
        self.compare_mismatch_notional_samples: List[float] = []
        self.last_compare: Optional[Dict[str, Any]] = None
        self.last_rest_snapshot_ts_ms: Optional[int] = None
        self._seen_ws_tokens: Set[str] = set()
        # WS-only baseline for delta logging; keep independent from REST rebases.
        self._ws_prev_books: Dict[str, Tuple[Dict[float, float], Dict[float, float]]] = {}
        # effective config snapshots (for /monitor/status observability)
        self._effective_rest_poll_sec: float = float(self.settings.CALIBRATION_INTERVAL_SEC)
        self._effective_calibration_interval_sec: int = int(self.settings.CALIBRATION_INTERVAL_SEC)
        self._effective_calibration_top_n: int = int(self.settings.CALIBRATION_TOP_N)
        self._effective_monitor_mismatch_levels: int = int(self.settings.MONITOR_MISMATCH_LEVELS)

    def _write_event(self, *, event: str, schema: str, **fields: Any) -> None:
        safe_write(
            self.writer,
            schemas.make_envelope(event=event, schema=schema, run_id=self.run_id, **fields),
            on_error_event=schemas.make_envelope(
                event="generic_error",
                schema=schemas.GENERIC_ERROR_V1,
                run_id=self.run_id,
                stage=event,
            ),
        )

    def _raw_write(self, kind: str, payload: Dict[str, Any], token_id: Optional[str] = None) -> None:
        try:
            self.raw.write(kind, payload, token_id=token_id)
        except Exception:
            pass

    async def start(self) -> None:
        if self._started:
            return
        self._started = True
        self._stop_evt.clear()
        # stop() may close zstd writers; recreate raw writer on restart.
        if not self.raw.books_writers_enabled:
            self.raw = RawLogWriter(self.run_id)

        self.token_ids = await self._select_tokens()
        self.book_states = {tid: BookState(token_id=tid) for tid in self.token_ids}
        self._seen_ws_tokens = set()
        self._ws_prev_books = {}

        await self._apply_rest_snapshot_and_publish(reason="bootstrap")

        self._tasks = [
            asyncio.create_task(self._ws_loop(), name="monitor_ws_loop"),
            asyncio.create_task(self._rest_poll_loop(), name="monitor_rest_poll_loop"),
        ]

        self._write_event(
            event="monitor_start",
            schema="monitor_start_v1",
            token_ids=self.token_ids,
        )

    async def stop(self) -> None:
        if not self._started:
            return
        self._stop_evt.set()
        for t in self._tasks:
            try:
                t.cancel()
            except Exception:
                pass
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks = []
        self._started = False
        self.ws_connected = False
        try:
            self.raw.close()
        except Exception:
            pass
        self._write_event(
            event="monitor_stop",
            schema="monitor_stop_v1",
        )

    def status(self) -> MonitorStatus:
        return MonitorStatus(
            run_id=self.run_id,
            started=self._started,
            token_ids=list(self.token_ids),
            ws_connected=self.ws_connected,
            last_publish_ts_ms=self.last_publish_ts_ms,
            last_rest_poll_ts_ms=self.last_rest_poll_ts_ms,
            ws_book_msgs=self.ws_book_msgs,
            ws_other_msgs=self.ws_other_msgs,
            rest_polls=self.rest_polls,
            rebase_count=self.rebase_count,
            raw_log_path=self.raw.status.file_path,
            raw_events_written=self.raw.status.written,
            raw_log_dir=self.raw.status.dir_path,
            books_delta_log_path=self.raw.status.books_delta_path or "",
            books_snapshot_log_path=self.raw.status.books_snapshot_path or "",
            books_delta_written=self.raw.status.books_delta_written,
            books_snapshot_written=self.raw.status.books_snapshot_written,
            compare_total=self.compare_total,
            compare_tob_match=self.compare_tob_match,
            compare_mismatch_notional_samples=self.compare_mismatch_notional_samples,
            last_compare=self.last_compare,
            last_rest_snapshot_ts_ms=self.last_rest_snapshot_ts_ms,
            effective_rest_poll_sec=self._effective_rest_poll_sec,
            effective_calibration_interval_sec=self._effective_calibration_interval_sec,
            effective_calibration_top_n=self._effective_calibration_top_n,
            effective_monitor_mismatch_levels=self._effective_monitor_mismatch_levels,
        )

    def _empty_metrics_tick(self, token_id: str) -> Dict[str, Any]:
        ts_ms = _now_ms()
        return {
            "event": "metrics_tick",
            "schema": "metrics_tick_v1",
            "run_id": self.run_id,
            "source": "init",
            "reason": "no_data",
            "token_id": token_id,
            "ts_ms": ts_ms,
            "exchange_ts_ms": None,
            "metrics": None,
        }

    def current_metrics(self, token_id: Optional[str] = None) -> Dict[str, Any]:
        if token_id:
            payload = self.latest_metrics.get(token_id)
            return {token_id: payload if payload is not None else self._empty_metrics_tick(token_id)}
        out: Dict[str, Any] = {}
        for tid in self.token_ids:
            payload = self.latest_metrics.get(tid)
            out[tid] = payload if payload is not None else self._empty_metrics_tick(tid)
        return out

    async def _select_tokens(self) -> List[str]:
        if self._override_token_ids:
            return list(self._override_token_ids)
        explicit = _split_csv(self.settings.MONITOR_TOKEN_IDS)
        if explicit:
            pick_n = int(self.settings.MONITOR_PICK_TOKENS or len(explicit))
            return explicit[: max(1, pick_n)]

        keyword = (self.settings.MONITOR_KEYWORD or "").strip() or "ethereum"

        candidate_token_ids: List[str] = []
        try:
            s = await self.gamma.public_search(
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
        except Exception:
            candidate_token_ids = []

        if not candidate_token_ids:
            markets = await self.gamma.list_markets(limit=50, offset=0)
            candidate_token_ids = pick_tokens_from_markets(markets, keyword=keyword, pick_n=30)

        pick_n = int(self.settings.MONITOR_PICK_TOKENS or 3)
        return candidate_token_ids[: max(1, pick_n)]

    async def set_tokens(self, token_ids: List[str], restart: bool = True) -> None:
        """Update monitored token_ids.

        If restart=True and monitor is running, stop + restart so WS subscriptions align.
        """
        seen = set()
        norm: List[str] = []
        for t in token_ids:
            t = str(t).strip()
            if not t or t in seen:
                continue
            seen.add(t)
            norm.append(t)

        if hasattr(self.settings, "MONITOR_TOKEN_IDS"):
            try:
                setattr(self.settings, "MONITOR_TOKEN_IDS", ",".join(norm))
            except Exception:
                pass
        if hasattr(self.settings, "MONITOR_KEYWORD"):
            try:
                setattr(self.settings, "MONITOR_KEYWORD", "")
            except Exception:
                pass

        self._override_token_ids = norm

        if restart and self._started:
            await self.stop()

        self.token_ids = list(norm)
        self.book_states = {tid: BookState(token_id=tid) for tid in self.token_ids}
        self.latest_metrics = {}
        self._seen_ws_tokens = set()
        self._ws_prev_books = {}

        if restart and not self._started:
            await self.start()

    async def _apply_rest_snapshot_and_publish(self, *, reason: str) -> None:
        snaps = await self.rest.post_books(self.token_ids)
        self.rest_polls += 1
        self.last_rest_poll_ts_ms = _now_ms()

        self._write_event(
            event="rest_poll",
            schema="rest_poll_v1",
            reason=reason,
            count=len(snaps) if isinstance(snaps, list) else None,
        )

        if not isinstance(snaps, list):
            return

        for idx, raw in enumerate(snaps):
            if not isinstance(raw, dict):
                continue
            nb = normalize_book(raw, "rest")
            tid = nb.get("token_id") or ""
            if (not tid) and idx < len(self.token_ids):
                tid = self.token_ids[idx]
                nb["token_id"] = tid
            if not tid or tid not in self.book_states:
                continue
            self.book_states[tid].apply_snapshot(
                nb.get("bids") or [],
                nb.get("asks") or [],
                ts_ms=_now_ms(),
                source="rest",
                exchange_ts_ms=nb.get("exchange_ts_ms"),
            )
            await self._publish_metrics_from_snapshot(nb, source="rest", reason=reason)

    async def _publish_metrics_from_snapshot(self, snap: Dict[str, Any], *, source: str, reason: str) -> None:
        tid = snap.get("token_id")
        if not isinstance(tid, str) or not tid:
            return

        ltp = self.last_trade_price.get(tid)
        if ltp is not None:
            snap = dict(snap)
            snap["last_trade_price"] = ltp

        metrics = compute_liquidity_metrics(
            snap,
            notional_list=list(self.settings.LIQ_NOTIONALS),
            depth_bps_list=list(self.settings.LIQ_DEPTH_BPS_LIST),
        )
        metrics["health"] = {
            "ws_connected": bool(self.ws_connected),
            "ws_lag_ms": None,
            "last_rest_snapshot_ts_ms": self.last_rest_snapshot_ts_ms,
            "last_compare": self.last_compare,
            "rebase_count": self.rebase_count,
        }
        ts_ms = _now_ms()
        exch_ts = extract_exchange_ts_ms(snap)

        out = {
            "event": "metrics_tick",
            "schema": "metrics_tick_v1",
            "run_id": self.run_id,
            "source": source,
            "reason": reason,
            "token_id": tid,
            "ts_ms": ts_ms,
            "exchange_ts_ms": exch_ts,
            "metrics": metrics,
        }

        self.latest_metrics[tid] = out
        self.last_publish_ts_ms = ts_ms

        self._write_event(
            event="metrics_tick",
            schema="metrics_tick_v1",
            ts_ms=ts_ms,
            source=source,
            reason=reason,
            token_id=tid,
            exchange_ts_ms=exch_ts,
            metrics=metrics,
        )
        await self.hub.publish(out)

    async def _handle_book_msg(self, msg: Dict[str, Any]) -> None:
        nb = normalize_book(msg, "ws")
        tid = nb.get("token_id") or ""
        if not tid or tid not in self.book_states:
            return
        first_ws = tid not in self._seen_ws_tokens
        st = self.book_states[tid]
        old_bids, old_asks = self._ws_prev_books.get(tid, ({}, {}))
        new_bids = {
            float(p): float(s)
            for p, s in (nb.get("bids") or [])
            if p is not None and s is not None
        }
        new_asks = {
            float(p): float(s)
            for p, s in (nb.get("asks") or [])
            if p is not None and s is not None
        }
        self._raw_write("ws_book", {"msg": msg}, token_id=tid)
        st.apply_snapshot(
            nb.get("bids") or [],
            nb.get("asks") or [],
            ts_ms=_now_ms(),
            source="ws",
            exchange_ts_ms=nb.get("exchange_ts_ms"),
        )
        self._seen_ws_tokens.add(tid)

        # delta-only compressed log
        try:
            delta = self._delta_from_maps(old_bids, old_asks, new_bids, new_asks)
            if delta.get("bids") or delta.get("asks"):
                self.raw.write_books_delta(
                    token_id=tid,
                    payload={"ts_ms": _now_ms(), "source": "ws", "delta": delta},
                )
            self._ws_prev_books[tid] = (new_bids, new_asks)
        except Exception:
            pass

        # first snapshot bootstrap log
        if first_ws:
            try:
                snap = self._snapshot_from_state(tid)
                if snap:
                    self.raw.write_books_snapshot(
                        token_id=tid,
                        payload={"ts_ms": _now_ms(), "source": "ws", "reason": "first_ws", "book": snap},
                    )
            except Exception:
                pass

        await self._publish_metrics_from_snapshot(nb, source="ws", reason="ws_book")

    @staticmethod
    def _extract_token_id(msg: Dict[str, Any]) -> Optional[str]:
        for k in ("asset_id", "token_id", "assetId", "tokenId", "id"):
            v = msg.get(k)
            if isinstance(v, str) and v:
                return v
        d = msg.get("data")
        if isinstance(d, dict):
            for k in ("asset_id", "token_id", "assetId", "tokenId", "id"):
                v = d.get(k)
                if isinstance(v, str) and v:
                    return v
        return None

    def _snapshot_from_state(self, token_id: str) -> Optional[Dict[str, Any]]:
        st = self.book_states.get(token_id)
        if not st:
            return None
        bids = [[p, s] for p, s in st.bids.items()]
        asks = [[p, s] for p, s in st.asks.items()]
        return {
            "token_id": token_id,
            "bids": bids,
            "asks": asks,
            "exchange_ts_ms": st.exchange_ts_ms,
        }

    def _delta_from_maps(
        self,
        old_bids: Dict[float, float],
        old_asks: Dict[float, float],
        new_bids: Dict[float, float],
        new_asks: Dict[float, float],
    ) -> Dict[str, Any]:
        def _diff(a: Dict[float, float], b: Dict[float, float]) -> List[List[float]]:
            out: List[List[float]] = []
            prices = set(a.keys()) | set(b.keys())
            for p in prices:
                av = float(a.get(p, 0.0) or 0.0)
                bv = float(b.get(p, 0.0) or 0.0)
                if av != bv:
                    out.append([float(p), av, bv])
            out.sort(key=lambda x: x[0])
            return out

        return {"bids": _diff(old_bids, new_bids), "asks": _diff(old_asks, new_asks)}

    @staticmethod
    def _percentile(vals: List[float], pct: float) -> Optional[float]:
        if not vals:
            return None
        xs = sorted(vals)
        k = (len(xs) - 1) * (pct / 100.0)
        f = int(k)
        c = min(f + 1, len(xs) - 1)
        if c == f:
            return float(xs[f])
        return float(xs[f] + (xs[c] - xs[f]) * (k - f))

    def _compare_topn(self, token_id: str, book: BookState, snap: Dict[str, Any], topn: int) -> CompareResult:
        def _sorted_levels(levels: List[List[float]], side: str) -> List[List[float]]:
            if side == "bids":
                return sorted(levels, key=lambda x: x[0], reverse=True)
            return sorted(levels, key=lambda x: x[0])

        best_bid = book.best_bid()
        best_ask = book.best_ask()
        snap_bids = _sorted_levels(snap.get("bids") or [], "bids")
        snap_asks = _sorted_levels(snap.get("asks") or [], "asks")
        snap_best_bid = snap_bids[0][0] if snap_bids else None
        snap_best_ask = snap_asks[0][0] if snap_asks else None
        tob_match = (best_bid[0] if best_bid else None) == snap_best_bid and (best_ask[0] if best_ask else None) == snap_best_ask

        def to_map(levels: List[List[float]], n: int) -> Dict[float, float]:
            m: Dict[float, float] = {}
            for lv in levels[:n]:
                try:
                    p = float(lv[0])
                    s = float(lv[1])
                except Exception:
                    continue
                m[p] = s
            return m

        b_map = dict(book.levels("bid", topn))
        a_map = dict(book.levels("ask", topn))
        sb = to_map(snap_bids, topn)
        sa = to_map(snap_asks, topn)

        prices = set(b_map.keys()) | set(a_map.keys()) | set(sb.keys()) | set(sa.keys())
        mismatch_levels = 0
        mismatch_notional = 0.0
        for p in prices:
            bsz = b_map.get(p, 0.0)
            sbsz = sb.get(p, 0.0)
            asz = a_map.get(p, 0.0)
            sasz = sa.get(p, 0.0)
            if bsz != sbsz:
                mismatch_levels += 1
                mismatch_notional += abs(bsz - sbsz) * p
            if asz != sasz:
                mismatch_levels += 1
                mismatch_notional += abs(asz - sasz) * p

        return CompareResult(
            token_id=token_id,
            compared_topn=topn,
            tob_match=tob_match,
            mismatch_levels=mismatch_levels,
            mismatch_notional=float(mismatch_notional),
            best_bid=best_bid[0] if best_bid else None,
            best_ask=best_ask[0] if best_ask else None,
            snap_best_bid=snap_best_bid,
            snap_best_ask=snap_best_ask,
        )

    async def _ws_loop(self) -> None:
        ws = MarketWsClient(self.settings.CLOB_WS_BASE, channel=self.settings.CLOB_WS_CHANNEL)
        try:
            self.ws_connected = True
            self._write_event(
                event="ws_status",
                schema="ws_status_v1",
                state="connected",
                token_ids=self.token_ids,
            )
            async for msg in ws.connect_and_stream(self.token_ids, self._stop_evt):
                if self._stop_evt.is_set():
                    break
                if not isinstance(msg, dict):
                    self.ws_other_msgs += 1
                    continue
                # batch wrapper from ws_client non-dict handling
                if msg.get("event_type") == "__non_dict__" and isinstance(msg.get("data"), list):
                    batch = msg.get("data") or []
                    for item in batch:
                        if not isinstance(item, dict):
                            continue
                        if item.get("event_type") == "book" or ("bids" in item and "asks" in item):
                            self.ws_book_msgs += 1
                            await self._handle_book_msg(item)
                        else:
                            self.ws_other_msgs += 1
                            et = str(item.get("event_type") or item.get("eventType") or item.get("type") or "").lower()
                            self._raw_write("ws_other", {"event_type": et, "msg": item})
                    continue

                et = str(msg.get("event_type") or msg.get("eventType") or msg.get("type") or "").lower()
                if msg.get("event_type") == "book" or ("bids" in msg and "asks" in msg):
                    self.ws_book_msgs += 1
                    await self._handle_book_msg(msg)
                else:
                    if et in ("price_change", "last_trade_price", "trade", "last_trade"):
                        token_id = self._extract_token_id(msg)
                        if token_id:
                            try:
                                price = msg.get("price") or msg.get("last_trade_price") or msg.get("lastTradePrice")
                                if price is None and isinstance(msg.get("data"), dict):
                                    d = msg["data"]
                                    price = d.get("price") or d.get("last_trade_price") or d.get("lastTradePrice")
                                p = float(price) if price is not None else None
                            except Exception:
                                p = None
                            if p is not None:
                                self.last_trade_price[token_id] = p
                            self._raw_write(
                                "ws_price_change",
                                {"event_type": et, "price": p, "msg": msg},
                                token_id=token_id,
                            )
                            snap = self._snapshot_from_state(token_id)
                            if snap:
                                await self._publish_metrics_from_snapshot(snap, source="ws", reason=et)
                            self.ws_other_msgs += 1
                            continue
                    self.ws_other_msgs += 1
                    self._raw_write("ws_other", {"event_type": et, "msg": msg})
        except asyncio.CancelledError:
            pass
        except Exception as e:
            self._write_event(
                event="ws_error",
                schema="ws_error_v1",
                error=str(e),
            )
        finally:
            self.ws_connected = False
            self._write_event(
                event="ws_status",
                schema="ws_status_v1",
                state="disconnected",
            )

    async def _rest_poll_loop(self) -> None:
        # ---- threshold snapshot (for raw events / acceptance) ----
        # NOTE: do not change decision logic; only record the effective thresholds used.
        def _thresh_snapshot(top_n: int, mismatch_levels: int) -> Dict[str, Any]:
            try:
                mismatch_notional = float(os.getenv("CALIB_MISMATCH_NOTIONAL", "50.0"))
            except Exception:
                mismatch_notional = 50.0
            tob_must_match = os.getenv("CALIB_TOB_MUST_MATCH", "1") not in ("0", "false", "False")
            return {
                "thresh_top_n": int(top_n),
                "thresh_mismatch_levels": int(mismatch_levels),
                "thresh_mismatch_notional": float(mismatch_notional),
                "thresh_tob_must_match": bool(tob_must_match),
                "thresh_source": "mixed",
            }

        # Single source of truth for REST snapshot polling interval:
        # use CALIBRATION_INTERVAL_SEC (avoid MONITOR_POLL_INTERVAL_SEC affecting /books drift correction).
        poll_sec = float(self.settings.CALIBRATION_INTERVAL_SEC)
        mismatch_threshold = int(self.settings.MONITOR_MISMATCH_LEVELS)
        top_n = int(self.settings.CALIBRATION_TOP_N)
        self._effective_rest_poll_sec = poll_sec
        self._effective_calibration_interval_sec = int(self.settings.CALIBRATION_INTERVAL_SEC)
        self._effective_calibration_top_n = top_n
        self._effective_monitor_mismatch_levels = mismatch_threshold

        while not self._stop_evt.is_set():
            try:
                cycle_compares = 0
                cycle_tob_match = 0
                cycle_mismatch_notional: List[float] = []
                snaps = await self.rest.post_books(self.token_ids)
                self.rest_polls += 1
                self.last_rest_poll_ts_ms = _now_ms()

                self._write_event(
                    event="rest_poll",
                    schema="rest_poll_v1",
                    reason="poll",
                    count=len(snaps) if isinstance(snaps, list) else None,
                )

                if isinstance(snaps, list):
                    for raw in snaps:
                        if not isinstance(raw, dict):
                            continue
                        nb = normalize_book(raw, "rest")
                        tid = nb.get("token_id") or ""
                        if not tid or tid not in self.book_states:
                            continue
                        self.last_rest_snapshot_ts_ms = _now_ms()
                        cur = self.book_states[tid]
                        # Compare using the same Top-N as the calibration settings
                        # (avoid env CALIB_TOPN drifting from Settings/CALIBRATION_TOP_N)
                        cmp = self._compare_topn(tid, cur, nb, top_n)
                        self.compare_total += 1
                        if cmp.tob_match:
                            self.compare_tob_match += 1
                        self.compare_mismatch_notional_samples.append(cmp.mismatch_notional)
                        cycle_compares += 1
                        if cmp.tob_match:
                            cycle_tob_match += 1
                        try:
                            cycle_mismatch_notional.append(float(cmp.mismatch_notional))
                        except Exception:
                            pass
                        self.last_compare = cmp.as_dict()
                        do_rebase = should_rebase(
                            cmp,
                            tob_must_match=CALIB_TOB_MUST_MATCH,
                            mismatch_levels_th=mismatch_threshold,
                            mismatch_notional_th=CALIB_MISMATCH_NOTIONAL,
                        )

                        if self.raw and (do_rebase or random.random() < CALIB_LOG_SAMPLE_RATE):
                            self.raw.write_event(
                                run_id=self.run_id,
                                token_id=tid,
                                event="calibration_compare",
                                payload={
                                    "compare": cmp.as_dict(),
                                    "thresholds": _thresh_snapshot(top_n, mismatch_threshold),
                                },
                                ts_ms=self.last_rest_snapshot_ts_ms,
                            )

                        if do_rebase:
                            before = {
                                "best_bid": cur.best_bid(),
                                "best_ask": cur.best_ask(),
                            }
                            self.book_states[tid].apply_snapshot(
                                nb.get("bids") or [],
                                nb.get("asks") or [],
                                ts_ms=_now_ms(),
                                source="rest_rebase",
                                exchange_ts_ms=nb.get("exchange_ts_ms"),
                            )
                            after = {
                                "best_bid": cur.best_bid(),
                                "best_ask": cur.best_ask(),
                            }
                            self.rebase_count += 1
                            if self.raw:
                                self.raw.write_event(
                                    run_id=self.run_id,
                                    token_id=tid,
                                    event="rebase",
                                    payload={
                                        "reason": "threshold_exceeded",
                                        # Keep the threshold snapshot consistent with calibration_compare.
                                        "thresholds": _thresh_snapshot(top_n, mismatch_threshold),
                                        "compare": cmp.as_dict(),
                                        "before": before,
                                        "after": after,
                                    },
                                    ts_ms=self.last_rest_snapshot_ts_ms,
                                )
                                try:
                                    snap = self._snapshot_from_state(tid)
                                    if snap:
                                        self.raw.write_books_snapshot(
                                            token_id=tid,
                                            payload={
                                                "ts_ms": _now_ms(),
                                                "source": "rest",
                                                "reason": "rebase",
                                                "book": snap,
                                            },
                                        )
                                except Exception:
                                    pass
                            self._write_event(
                                event="rebase",
                                schema="rebase_v1",
                                token_id=tid,
                                reason={"mismatch_levels": cmp.mismatch_levels},
                            )
                            await self._publish_metrics_from_snapshot(nb, source="rest", reason="rebase")
                        else:
                            self.book_states[tid].apply_snapshot(
                                nb.get("bids") or [],
                                nb.get("asks") or [],
                                ts_ms=_now_ms(),
                                source="rest",
                                exchange_ts_ms=nb.get("exchange_ts_ms"),
                            )
                            if self.raw:
                                try:
                                    snap = self._snapshot_from_state(tid)
                                    if snap:
                                        self.raw.write_books_snapshot(
                                            token_id=tid,
                                            payload={
                                                "ts_ms": _now_ms(),
                                                "source": "rest",
                                                "reason": "calibration",
                                                "book": snap,
                                            },
                                        )
                                except Exception:
                                    pass
                            await self._publish_metrics_from_snapshot(nb, source="rest", reason="rest_book")

                if self.raw:
                    try:
                        tob_rate = (
                            float(cycle_tob_match) / float(cycle_compares)
                            if cycle_compares > 0
                            else None
                        )
                        p90 = self._percentile(cycle_mismatch_notional, 90.0)
                        self.raw.write_event(
                            run_id=self.run_id,
                            token_id=None,
                            event="calibration_summary",
                            payload={
                                "compares": cycle_compares,
                                "tob_match_rate": tob_rate,
                                "mismatch_notional_p90": p90,
                                "thresholds": _thresh_snapshot(top_n, mismatch_threshold),
                                "sample_rate": CALIB_LOG_SAMPLE_RATE,
                            },
                            ts_ms=self.last_rest_poll_ts_ms,
                        )
                    except Exception:
                        pass
            except asyncio.CancelledError:
                break
            except Exception as e:
                self._write_event(
                    event="rest_poll_error",
                    schema="rest_poll_error_v1",
                    error=str(e),
                )
            await asyncio.sleep(poll_sec)
