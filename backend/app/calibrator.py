from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Any

from .book_engine import BookState, compare_top_n
from .normalize import normalize_book
from .clob_rest import ClobRestClient

log = logging.getLogger("calibrator")

@dataclass
class CompareResult:
    token_id: str
    compared_topn: int
    tob_match: bool
    mismatch_levels: int
    mismatch_notional: float
    best_bid: Optional[float] = None
    best_ask: Optional[float] = None
    snap_best_bid: Optional[float] = None
    snap_best_ask: Optional[float] = None

    def as_dict(self) -> Dict[str, Any]:
        return {
            "token_id": self.token_id,
            "compared_topn": self.compared_topn,
            "tob_match": self.tob_match,
            "mismatch_levels": self.mismatch_levels,
            "mismatch_notional": self.mismatch_notional,
            "best_bid": self.best_bid,
            "best_ask": self.best_ask,
            "snap_best_bid": self.snap_best_bid,
            "snap_best_ask": self.snap_best_ask,
        }


def should_rebase(
    cmp: CompareResult,
    *,
    tob_must_match: bool = True,
    mismatch_levels_th: int = 4,
    mismatch_notional_th: float = 50.0,
) -> bool:
    if tob_must_match and (not cmp.tob_match):
        return True
    if cmp.mismatch_levels >= mismatch_levels_th:
        return True
    if cmp.mismatch_notional >= mismatch_notional_th:
        return True
    return False

class Calibrator:
    def __init__(self, rest: ClobRestClient, interval_sec: int, top_n: int):
        self.rest = rest
        self.interval_sec = interval_sec
        self.top_n = top_n

    async def run(
        self,
        token_ids: List[str],
        books: Dict[str, BookState],
        stop_event: asyncio.Event,
        on_rebase_event,
        on_compare_event,
    ) -> None:
        while not stop_event.is_set():
            t0 = time.time()
            try:
                snapshots = await self.rest.post_books(token_ids)
                ts_ms = int(time.time() * 1000)

                # snapshots is list of book summaries (each includes token_id + bids/asks)
                for snap in snapshots:
                    if not isinstance(snap, dict):
                        continue
                    tid = str(snap.get("token_id") or snap.get("asset_id") or "")
                    if not tid:
                        continue

                    if tid not in books:
                        books[tid] = BookState(token_id=tid)

                    nb = normalize_book(snap, "rest")

                    # Create a BookState from snapshot for comparison
                    snap_book = BookState(token_id=tid)
                    snap_book.apply_snapshot(
                        bids=nb.get("bids") or [],
                        asks=nb.get("asks") or [],
                        ts_ms=int(snap.get("timestamp") or ts_ms),
                        book_hash=snap.get("hash"),
                        source="rest",
                        exchange_ts_ms=nb.get("exchange_ts_ms"),
                    )

                    cur = books[tid]
                    mismatch_levels = compare_top_n(cur, snap_book, self.top_n)
                    a_best_bid, a_best_ask = cur.top()
                    b_best_bid, b_best_ask = snap_book.top()
                    await on_compare_event({
                        "event": "calibration_compare",
                        "token_id": tid,
                        "ts_ms": ts_ms,
                        "compare": {
                            "topn": self.top_n,
                            "mismatch_levels": mismatch_levels,
                            "a_best_bid": a_best_bid,
                            "a_best_ask": a_best_ask,
                            "b_best_bid": b_best_bid,
                            "b_best_ask": b_best_ask,
                        },
                    })

                    # Rebase heuristic: if mismatch levels are non-trivial OR TOB mismatch
                    tob_mismatch = (a_best_bid != b_best_bid) or (a_best_ask != b_best_ask)
                    if tob_mismatch or mismatch_levels >= 4:
                        # hard reset to snapshot
                        cur.apply_snapshot(
                            bids=nb.get("bids") or [],
                            asks=nb.get("asks") or [],
                            ts_ms=int(snap.get("timestamp") or ts_ms),
                            book_hash=snap.get("hash"),
                            source="rest_rebase",
                            exchange_ts_ms=nb.get("exchange_ts_ms"),
                        )
                        await on_rebase_event({
                            "event": "rebase",
                            "token_id": tid,
                            "ts_ms": ts_ms,
                            "reason": {
                                "tob_mismatch": tob_mismatch,
                                "mismatch_levels": mismatch_levels,
                            },
                        })

            except Exception as e:
                log.warning(f"calibration error: {e!r}")

            elapsed = time.time() - t0
            sleep_for = max(0.0, self.interval_sec - elapsed)
            await asyncio.sleep(sleep_for)
