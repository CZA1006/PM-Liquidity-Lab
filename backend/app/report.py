from __future__ import annotations

import statistics
from dataclasses import dataclass, field
from typing import Dict, List, Any

@dataclass
class ProbeStats:
    ws_msgs: int = 0
    ws_book_msgs: int = 0
    ws_other_msgs: int = 0
    books_polls: int = 0
    rebase_count: int = 0

    # per token
    book_updates_by_token: Dict[str, int] = field(default_factory=dict)
    ws_lag_ms_samples: List[int] = field(default_factory=list)
    tob_mismatch_samples: int = 0
    mismatch_level_samples: List[int] = field(default_factory=list)

    def add_ws_lag(self, lag_ms: int) -> None:
        self.ws_lag_ms_samples.append(lag_ms)

    def add_book_update(self, token_id: str) -> None:
        self.book_updates_by_token[token_id] = self.book_updates_by_token.get(token_id, 0) + 1

    def to_report(self) -> Dict[str, Any]:
        lags = self.ws_lag_ms_samples
        mismatch_levels = self.mismatch_level_samples

        def _pct(xs: List[int], p: float) -> float:
            if not xs:
                return 0.0
            xs2 = sorted(xs)
            k = int(round((len(xs2) - 1) * p))
            return float(xs2[k])

        return {
            "ws_msgs": self.ws_msgs,
            "ws_book_msgs": self.ws_book_msgs,
            "ws_other_msgs": self.ws_other_msgs,
            "books_polls": self.books_polls,
            "rebase_count": self.rebase_count,
            "book_updates_by_token": self.book_updates_by_token,
            "ws_lag_ms": {
                "count": len(lags),
                "p50": _pct(lags, 0.50),
                "p90": _pct(lags, 0.90),
                "p99": _pct(lags, 0.99),
                "avg": float(statistics.mean(lags)) if lags else 0.0,
                "max": float(max(lags)) if lags else 0.0,
            },
            "calibration_mismatch_levels": {
                "count": len(mismatch_levels),
                "avg": float(statistics.mean(mismatch_levels)) if mismatch_levels else 0.0,
                "max": float(max(mismatch_levels)) if mismatch_levels else 0.0,
            },
            "tob_mismatch_samples": self.tob_mismatch_samples,
        }