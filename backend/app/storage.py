import os
import time
from dataclasses import dataclass
from typing import Any, Dict

import orjson

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
