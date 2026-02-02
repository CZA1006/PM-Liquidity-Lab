from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Tuple, Any, Optional

def _to_float(x: Any) -> float:
    return float(x)

Level = Tuple[float, float]  # (price, size)

@dataclass
class BookState:
    token_id: str = ""
    # price -> size
    bids: Dict[float, float] = field(default_factory=dict)
    asks: Dict[float, float] = field(default_factory=dict)

    last_ts_ms: int = 0
    exchange_ts_ms: Optional[int] = None
    last_hash: Optional[str] = None
    source: str = "unknown"  # "ws" or "rest"
    version: int = 0

    def best_bid(self) -> Optional[Level]:
        if not self.bids:
            return None
        p = max(self.bids.keys())
        return (p, self.bids[p])

    def best_ask(self) -> Optional[Level]:
        if not self.asks:
            return None
        p = min(self.asks.keys())
        return (p, self.asks[p])

    def top(self) -> Tuple[Optional[float], Optional[float]]:
        bb = self.best_bid()
        ba = self.best_ask()
        return (bb[0] if bb else None, ba[0] if ba else None)

    def top_n(self, side: str, n: int) -> List[Level]:
        if side == "bids":
            items = sorted(self.bids.items(), key=lambda kv: kv[0], reverse=True)
        else:
            items = sorted(self.asks.items(), key=lambda kv: kv[0])
        return [(p, s) for p, s in items[:n]]

    def levels(self, side: str, n: int) -> List[Level]:
        side_key = "bids" if side in ("bid", "bids") else "asks"
        return self.top_n(side_key, n)

    def apply_snapshot(
        self,
        bids: List[Any],
        asks: List[Any],
        ts_ms: Optional[int] = None,
        book_hash: Optional[str] = None,
        source: Optional[str] = None,
        exchange_ts_ms: Optional[int] = None,
    ) -> None:
        bids_parsed = _parse_levels(bids, "bids")
        asks_parsed = _parse_levels(asks, "asks")
        self.bids = {p: s for p, s in bids_parsed}
        self.asks = {p: s for p, s in asks_parsed}
        if ts_ms is not None:
            self.last_ts_ms = ts_ms
        if exchange_ts_ms is not None:
            self.exchange_ts_ms = exchange_ts_ms
        if book_hash is not None:
            self.last_hash = book_hash
        if source is not None:
            self.source = source
        self.version += 1

def compare_top_n(a: BookState, b: BookState, n: int = 50) -> int:
    """
    Compare top-n price levels (price->size) between two books.
    Return number of mismatched levels (union over both sides).
    """
    mism = 0
    for side in ("bid", "ask"):
        la = dict(a.levels(side, n))
        lb = dict(b.levels(side, n))
        prices = set(la.keys()) | set(lb.keys())
        for p in prices:
            sa = la.get(p, 0.0)
            sb = lb.get(p, 0.0)
            if abs(sa - sb) > 1e-9:
                mism += 1
    return mism

def _parse_levels(levels: Any, side: str) -> List[Level]:
    out: List[Level] = []
    if not levels:
        return out
    for lv in levels:
        try:
            if isinstance(lv, dict):
                p = lv.get("price") or lv.get("p") or lv.get("rate")
                s = lv.get("size") or lv.get("s") or lv.get("quantity")
            elif isinstance(lv, (list, tuple)) and len(lv) >= 2:
                p, s = lv[0], lv[1]
            else:
                continue
            p_f = float(p)
            s_f = float(s)
            if p_f <= 0 or s_f <= 0:
                continue
            out.append((p_f, s_f))
        except Exception:
            continue
    if side == "bids":
        out.sort(key=lambda x: x[0], reverse=True)
    else:
        out.sort(key=lambda x: x[0])
    return out

def _best_bid_ask(bids: List[Level], asks: List[Level]) -> Tuple[Optional[float], Optional[float]]:
    best_bid = bids[0][0] if bids else None
    best_ask = asks[0][0] if asks else None
    return best_bid, best_ask

def _depth_within_bps(bids: List[Level], asks: List[Level], mid: float, bps: int) -> Dict[str, float]:
    if mid <= 0:
        return {"bid_notional": 0.0, "ask_notional": 0.0, "bid_qty": 0.0, "ask_qty": 0.0}
    bid_px = mid * (1.0 - bps / 10000.0)
    ask_px = mid * (1.0 + bps / 10000.0)

    bid_notional = 0.0
    bid_qty = 0.0
    for p, s in bids:
        if p < bid_px:
            break
        bid_qty += s
        bid_notional += p * s

    ask_notional = 0.0
    ask_qty = 0.0
    for p, s in asks:
        if p > ask_px:
            break
        ask_qty += s
        ask_notional += p * s

    return {
        "bid_notional": bid_notional,
        "ask_notional": ask_notional,
        "bid_qty": bid_qty,
        "ask_qty": ask_qty,
    }

def _slippage_for_notional_buy(asks: List[Level], mid: float, notional: float) -> Optional[Dict[str, float]]:
    if not asks or notional <= 0 or mid <= 0:
        return None

    cost = 0.0
    qty = 0.0
    for p, s in asks:
        lv_notional = p * s
        if cost + lv_notional >= notional:
            remain = notional - cost
            take = remain / p
            qty += take
            cost += remain
            break
        else:
            qty += s
            cost += lv_notional

    if cost + 1e-12 < notional or qty <= 0:
        return None

    vwap = cost / qty
    impact_bps = (vwap - mid) / mid * 10000.0
    return {"vwap": vwap, "qty": qty, "notional": cost, "impact_bps": impact_bps}

def _slippage_for_notional_sell(bids: List[Level], mid: float, notional: float) -> Optional[Dict[str, float]]:
    if not bids or notional <= 0 or mid <= 0:
        return None

    proceeds = 0.0
    qty = 0.0
    for p, s in bids:
        lv_notional = p * s
        if proceeds + lv_notional >= notional:
            remain = notional - proceeds
            take = remain / p
            qty += take
            proceeds += remain
            break
        else:
            qty += s
            proceeds += lv_notional

    if proceeds + 1e-12 < notional or qty <= 0:
        return None

    vwap = proceeds / qty
    impact_bps = (mid - vwap) / mid * 10000.0
    return {"vwap": vwap, "qty": qty, "notional": proceeds, "impact_bps": impact_bps}

def compute_liquidity_metrics(
    book: Dict[str, Any],
    *,
    notional_list: List[float],
    depth_bps_list: List[int],
    topk_imbalance: int = 10,
) -> Dict[str, Any]:
    """
    Compute liquidity metrics from a single book snapshot (WS or REST /books).
    Expected fields:
      - bids: list[{price,size}], asks: list[{price,size}]
      - asset_id (token id), timestamp (ms) optional
    """
    bids = _parse_levels(book.get("bids"), "bids")
    asks = _parse_levels(book.get("asks"), "asks")
    # defensive: ensure canonical sort (bids desc, asks asc)
    bids.sort(key=lambda x: x[0], reverse=True)
    asks.sort(key=lambda x: x[0])
    best_bid, best_ask = _best_bid_ask(bids, asks)

    levels_bid = len(bids)
    levels_ask = len(asks)

    spread = None
    mid = None
    spread_bps = None
    if best_bid is not None and best_ask is not None and best_ask >= best_bid:
        spread = best_ask - best_bid
        mid = (best_ask + best_bid) / 2.0

    if mid is None:
        if best_bid is not None:
            mid = best_bid
        elif best_ask is not None:
            mid = best_ask

    depth: Dict[str, Any] = {}
    if mid is not None and mid > 0:
        for bps in depth_bps_list:
            depth[str(bps)] = _depth_within_bps(bids, asks, mid, bps)

    slippage: Dict[str, Any] = {"buy": {}, "sell": {}}
    if mid is not None and mid > 0:
        for n in notional_list:
            k = str(n)
            slippage["buy"][k] = _slippage_for_notional_buy(asks, mid, float(n))
            slippage["sell"][k] = _slippage_for_notional_sell(bids, mid, float(n))

    if spread is not None and mid is not None and mid > 0:
        spread_bps = (spread / mid) * 10000.0

    slippage_filled: Dict[str, Any] = {"buy": {}, "sell": {}}
    for n in notional_list:
        k = str(n)
        slippage_filled["buy"][k] = slippage["buy"].get(k) is not None
        slippage_filled["sell"][k] = slippage["sell"].get(k) is not None

    imb = None
    if topk_imbalance > 0:
        bid_sum = sum(s for _, s in bids[:topk_imbalance])
        ask_sum = sum(s for _, s in asks[:topk_imbalance])
        denom = bid_sum + ask_sum
        if denom > 0:
            imb = bid_sum / denom

    last_trade_price = None
    try:
        ltp = book.get("last_trade_price") or book.get("lastTradePrice")
        if ltp is not None:
            last_trade_price = _to_float(ltp)
    except Exception:
        last_trade_price = None

    out = {
        "levels": {"bids": levels_bid, "asks": levels_ask},
        "best": {"bid": best_bid, "ask": best_ask},
        "spread": spread,
        "mid": mid,
        "spread_bps": spread_bps,
        "two_sided": bool(bids) and bool(asks),
        "depth": depth,
        "slippage": slippage,
        "slippage_filled": slippage_filled,
        "imbalance_topk": imb,
        "last_trade_price": last_trade_price,
    }
    return out
