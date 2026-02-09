from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

from .config import settings
from .storage import JsonlWriter, JsonlZstWriter


@dataclass
class RawLogStatus:
    run_id: str
    dir_path: str
    file_path: str
    written: int = 0
    books_delta_path: Optional[str] = None
    books_snapshot_path: Optional[str] = None
    books_delta_written: int = 0
    books_snapshot_written: int = 0


class RawLogWriter:
    """
    Append-only raw event log for later replay/audit.
    Phase4 (historical DB) will build from this.
    """

    def __init__(self, run_id: str):
        ts = int(time.time())
        safe_run = run_id.replace("/", "_").replace("..", "_")
        dir_path = os.path.join(settings.DATA_DIR, "raw", safe_run)
        os.makedirs(dir_path, exist_ok=True)
        file_path = os.path.join(dir_path, f"events-{ts}.jsonl")
        self._writer = JsonlWriter(file_path)
        books_dir = os.path.join(settings.DATA_DIR, "books", safe_run)
        os.makedirs(books_dir, exist_ok=True)
        books_delta_path = os.path.join(books_dir, f"books_delta-{ts}.jsonl.zst")
        books_snapshot_path = os.path.join(books_dir, f"books_snapshot-{ts}.jsonl.zst")
        self._books_delta_writer = JsonlZstWriter(books_delta_path)
        self._books_snapshot_writer = JsonlZstWriter(books_snapshot_path)
        self.status = RawLogStatus(
            run_id=safe_run,
            dir_path=dir_path,
            file_path=file_path,
            written=0,
            books_delta_path=books_delta_path,
            books_snapshot_path=books_snapshot_path,
        )

    def write(self, kind: str, payload: Dict[str, Any], token_id: Optional[str] = None) -> None:
        rec = {
            "kind": kind,
            "ts_ms": int(time.time() * 1000),
            "token_id": token_id,
            "payload": payload,
        }
        self._writer.write(rec)
        self.status.written += 1

    def write_event(
        self,
        *,
        run_id: str,
        token_id: Optional[str],
        event: str,
        payload: Dict[str, Any],
        ts_ms: Optional[int] = None,
    ) -> None:
        if ts_ms is None:
            ts_ms = int(time.time() * 1000)
        self._writer.write(
            {
                "ts_ms": ts_ms,
                "run_id": run_id,
                "token_id": token_id,
                "event": event,
                "payload": payload,
            }
        )
        self.status.written += 1

    def write_books_delta(self, token_id: str, payload: Dict[str, Any]) -> None:
        ts_ms = int(time.time() * 1000)
        rec = {
            "ts_ms": ts_ms,
            "run_id": self.status.run_id,
            "token_id": token_id,
            "event": "books_delta",
            "payload": payload,
        }
        self._books_delta_writer.write(rec)
        # Keep file size observable during runtime for scale_probe.
        self._books_delta_writer.flush()
        if self._books_delta_writer.enabled:
            self.status.books_delta_written += 1

    def write_books_snapshot(self, token_id: str, payload: Dict[str, Any]) -> None:
        ts_ms = int(time.time() * 1000)
        rec = {
            "ts_ms": ts_ms,
            "run_id": self.status.run_id,
            "token_id": token_id,
            "event": "books_snapshot",
            "payload": payload,
        }
        self._books_snapshot_writer.write(rec)
        # Keep file size observable during runtime for scale_probe.
        self._books_snapshot_writer.flush()
        if self._books_snapshot_writer.enabled:
            self.status.books_snapshot_written += 1

    @property
    def books_writers_enabled(self) -> bool:
        return bool(self._books_delta_writer.enabled and self._books_snapshot_writer.enabled)

    def close(self) -> None:
        try:
            self._books_delta_writer.close()
        except Exception:
            pass
        try:
            self._books_snapshot_writer.close()
        except Exception:
            pass
        try:
            self._writer.close()
        except Exception:
            pass
