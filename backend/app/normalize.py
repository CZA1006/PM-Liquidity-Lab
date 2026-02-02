from __future__ import annotations

from typing import Any, Dict, List, Optional


def parse_ms(x: Any) -> Optional[int]:
    if x is None:
        return None
    try:
        return int(float(x))
    except Exception:
        return None


def _to_num(x: Any) -> Optional[float]:
    try:
        v = float(x)
        return v if v == v else None
    except Exception:
        return None


def extract_exchange_ts_ms(msg: Dict[str, Any]) -> Optional[int]:
    if not isinstance(msg, dict):
        return None
    for k in ("exchange_ts_ms", "timestamp", "ts_ms"):
        if k in msg and msg.get(k) is not None:
            return parse_ms(msg.get(k))
    return None


def _norm_levels(levels: Any) -> List[List[float]]:
    out: List[List[float]] = []
    if not levels:
        return out
    if isinstance(levels, list):
        for lv in levels:
            p = s = None
            if isinstance(lv, (list, tuple)) and len(lv) >= 2:
                p = _to_num(lv[0])
                s = _to_num(lv[1])
            elif isinstance(lv, dict):
                p = _to_num(lv.get("price"))
                s = _to_num(lv.get("size", lv.get("amount")))
            if p is None or s is None:
                continue
            out.append([p, s])
    return out


def normalize_book(msg: Dict[str, Any], source: str) -> Dict[str, Any]:
    """
    output:
      {
        token_id: str,
        source: "ws"|"rest",
        exchange_ts_ms: Optional[int],
        bids: [[p,s]...],
        asks: [[p,s]...],
      }
    """
    token_id = (
        msg.get("token_id")
        or msg.get("asset_id")
        or msg.get("tokenId")
        or msg.get("market_token_id")
    )
    bids = msg.get("bids") or msg.get("buys") or msg.get("bid_levels")
    asks = msg.get("asks") or msg.get("sells") or msg.get("ask_levels")
    return {
        "token_id": str(token_id) if token_id is not None else "",
        "source": source,
        "exchange_ts_ms": extract_exchange_ts_ms(msg),
        "bids": _norm_levels(bids),
        "asks": _norm_levels(asks),
    }
