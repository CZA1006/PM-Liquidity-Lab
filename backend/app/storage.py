import os
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Iterable, List

import orjson

class JsonlWriter:
    """
    Lightweight JSONL writer for arbitrary dict records.
    (Phase4 raw log / mapping dumps use this instead of requiring event+schema.)
    """

    def __init__(self, path: str) -> None:
        self.path = path
        os.makedirs(os.path.dirname(self.path), exist_ok=True)

    def write(self, obj: Dict[str, Any]) -> None:
        obj.setdefault("write_ts_ms", int(time.time() * 1000))
        with open(self.path, "ab") as f:
            f.write(orjson.dumps(obj))
            f.write(b"\n")

    def close(self) -> None:
        return


class OptionalParquetWriter:
    """
    Best-effort Parquet writer.
    If pyarrow is not installed, this becomes a no-op (but JSONL still works).
    """

    def __init__(self, path: str) -> None:
        self.path = path
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        self._enabled = False
        self._rows: List[Dict[str, Any]] = []
        try:
            import pyarrow  # noqa: F401
            import pyarrow.parquet  # noqa: F401
            self._enabled = True
        except Exception:
            self._enabled = False

    @property
    def enabled(self) -> bool:
        return self._enabled

    def append(self, row: Dict[str, Any]) -> None:
        if not self._enabled:
            return
        self._rows.append(row)

    def flush(self) -> None:
        if not self._enabled:
            return
        if not self._rows:
            return
        import pyarrow as pa
        import pyarrow.parquet as pq

        table = pa.Table.from_pylist(self._rows)
        pq.write_table(table, self.path)
        self._rows = []

@dataclass
class JsonlEventWriter:
    path: str
    run_id: str

    def __post_init__(self) -> None:
        os.makedirs(os.path.dirname(self.path), exist_ok=True)

    def write(self, obj: Dict[str, Any]) -> None:
        if "run_id" not in obj:
            obj["run_id"] = self.run_id
        if "ts_ms" not in obj:
            obj["ts_ms"] = int(time.time() * 1000)
        if "event" not in obj or "schema" not in obj:
            raise ValueError("jsonl event must include 'event' and 'schema'")
        # Always add local write ts for debugging
        obj.setdefault("write_ts_ms", int(time.time() * 1000))
        with open(self.path, "ab") as f:
            f.write(orjson.dumps(obj))
            f.write(b"\n")

def safe_write(writer: JsonlEventWriter, obj: Dict[str, Any], *, on_error_event: Dict[str, Any]) -> None:
    try:
        writer.write(obj)
    except Exception as e:
        err = dict(on_error_event)
        err["error"] = repr(e)
        try:
            writer.write(err)
        except Exception:
            pass

def ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)

def default_run_dir(data_dir: str, run_id: str) -> str:
    return os.path.join(data_dir, "runs", run_id)

def default_eventlog_path(data_dir: str, run_id: str) -> str:
    return os.path.join(default_run_dir(data_dir, run_id), "events.jsonl")

def default_report_path(data_dir: str, run_id: str) -> str:
    return os.path.join(default_run_dir(data_dir, run_id), "probe_report.json")
