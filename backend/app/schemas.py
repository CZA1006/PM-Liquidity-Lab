from __future__ import annotations

import time
from typing import Any, Dict, Optional


def now_ms() -> int:
    return int(time.time() * 1000)


# ---- schema names (v1) ----
PROBE_START_V1 = "probe_start_v1"
WS_BOOK_V1 = "ws_book_v1"
WS_OTHER_V1 = "ws_other_v1"
WS_NON_DICT_V1 = "ws_non_dict_v1"
REST_BOOK_V1 = "rest_book_v1"

LIQ_SNAPSHOT_V1 = "liq_snapshot_v1"
LIQ_SNAPSHOT_ERROR_V1 = "liq_snapshot_error_v1"

CALIBRATION_COMPARE_V1 = "calibration_compare_v1"
REBASE_V1 = "rebase_v1"

GENERIC_ERROR_V1 = "generic_error_v1"


def make_envelope(
    *,
    event: str,
    schema: str,
    run_id: str,
    ts_ms: Optional[int] = None,
    **fields: Any,
) -> Dict[str, Any]:
    obj: Dict[str, Any] = {
        "event": event,
        "schema": schema,
        "run_id": run_id,
        "ts_ms": now_ms() if ts_ms is None else ts_ms,
    }
    obj.update(fields)
    return obj
