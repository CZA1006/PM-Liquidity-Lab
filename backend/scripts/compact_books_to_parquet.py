#!/usr/bin/env python3
from __future__ import annotations

import argparse
import glob
import io
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

try:
    import duckdb  # type: ignore
except Exception:  # pragma: no cover
    duckdb = None  # type: ignore


def _iter_jsonl(path: Path) -> Iterable[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception:
                continue
            if isinstance(obj, dict):
                yield obj


def _iter_jsonl_zst(path: Path) -> Iterable[Dict[str, Any]]:
    try:
        import zstandard as zstd  # type: ignore
    except Exception as e:
        raise RuntimeError("zstandard not installed; cannot read .jsonl.zst") from e

    with open(path, "rb") as fh:
        reader = zstd.ZstdDecompressor().stream_reader(fh)
        text = io.TextIOWrapper(reader, encoding="utf-8", errors="replace")
        for line in text:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception:
                continue
            if isinstance(obj, dict):
                yield obj


def _find_run_ids(data_root: Path, run_id: Optional[str]) -> List[str]:
    if run_id:
        return [run_id]
    raw_root = data_root / "raw"
    books_root = data_root / "books"
    raw_runs = {p.name for p in raw_root.glob("*") if p.is_dir()} if raw_root.exists() else set()
    book_runs = {p.name for p in books_root.glob("*") if p.is_dir()} if books_root.exists() else set()
    out = sorted(raw_runs | book_runs)
    return out


def _target_file(
    out_root: Path,
    fact_name: str,
    run_id: str,
    min_ts_ms: Optional[int],
    dt_partition: bool,
) -> Path:
    base = out_root / fact_name
    if dt_partition and min_ts_ms and min_ts_ms > 0:
        dt = datetime.fromtimestamp(min_ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
        base = base / f"dt={dt}"
    base.mkdir(parents=True, exist_ok=True)
    return base / f"{run_id}.parquet"


def _create_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        create table t_snapshot (
          run_id varchar,
          token_id varchar,
          ts_ms bigint,
          exchange_ts_ms bigint,
          source varchar,
          reason varchar,
          state_hash varchar,
          top_k integer,
          bids_json varchar,
          asks_json varchar
        )
        """
    )
    con.execute(
        """
        create table t_snapshot_rle (
          run_id varchar,
          token_id varchar,
          source varchar,
          reason varchar,
          state_hash varchar,
          span_start_ts_ms bigint,
          span_end_ts_ms bigint,
          repeat_count bigint,
          top_k integer
        )
        """
    )
    con.execute(
        """
        create table t_delta (
          run_id varchar,
          token_id varchar,
          ts_ms bigint,
          source varchar,
          top_k integer,
          delta_bids_json varchar,
          delta_asks_json varchar
        )
        """
    )


def _insert_rows(con: duckdb.DuckDBPyConnection, run_id: str, data_root: Path) -> Dict[str, int]:
    books_dir = data_root / "books" / run_id
    raw_dir = data_root / "raw" / run_id
    n_snapshot = n_rle = n_delta = 0

    for p in sorted(glob.glob(str(books_dir / "books_snapshot-*.jsonl.zst"))):
        for obj in _iter_jsonl_zst(Path(p)):
            ev = str(obj.get("event") or "")
            token_id = str(obj.get("token_id") or "")
            payload = obj.get("payload") if isinstance(obj.get("payload"), dict) else {}
            if ev == "books_snapshot":
                book = payload.get("book") if isinstance(payload.get("book"), dict) else {}
                con.execute(
                    """
                    insert into t_snapshot values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        str(obj.get("run_id") or run_id),
                        token_id,
                        int(obj.get("ts_ms") or payload.get("ts_ms") or 0),
                        int(book.get("exchange_ts_ms") or 0) or None,
                        payload.get("source"),
                        payload.get("reason"),
                        payload.get("state_hash"),
                        int(payload.get("top_k_applied") or 0),
                        json.dumps(book.get("bids") or [], ensure_ascii=False),
                        json.dumps(book.get("asks") or [], ensure_ascii=False),
                    ],
                )
                n_snapshot += 1
            elif ev == "books_snapshot_rle":
                con.execute(
                    """
                    insert into t_snapshot_rle values (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        str(obj.get("run_id") or run_id),
                        token_id,
                        payload.get("source"),
                        payload.get("reason"),
                        payload.get("state_hash"),
                        int(payload.get("span_start_ts_ms") or 0),
                        int(payload.get("span_end_ts_ms") or 0),
                        int(payload.get("repeat_count") or 0),
                        int(payload.get("top_k_applied") or 0),
                    ],
                )
                n_rle += 1

    for p in sorted(glob.glob(str(books_dir / "books_delta-*.jsonl.zst"))):
        for obj in _iter_jsonl_zst(Path(p)):
            if str(obj.get("event") or "") != "books_delta":
                continue
            payload = obj.get("payload") if isinstance(obj.get("payload"), dict) else {}
            delta = payload.get("delta") if isinstance(payload.get("delta"), dict) else {}
            con.execute(
                """
                insert into t_delta values (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    str(obj.get("run_id") or run_id),
                    str(obj.get("token_id") or ""),
                    int(obj.get("ts_ms") or payload.get("ts_ms") or 0),
                    payload.get("source"),
                    int(payload.get("top_k_applied") or 0),
                    json.dumps(delta.get("bids") or [], ensure_ascii=False),
                    json.dumps(delta.get("asks") or [], ensure_ascii=False),
                ],
            )
            n_delta += 1

    # Optional: if there is already parsed delta in raw jsonl kind stream, keep compatibility by adding none.
    for p in sorted(glob.glob(str(raw_dir / "events-*.jsonl"))):
        for obj in _iter_jsonl(Path(p)):
            if str(obj.get("kind") or "") != "books_delta":
                continue
            payload = obj.get("payload") if isinstance(obj.get("payload"), dict) else {}
            delta = payload.get("delta") if isinstance(payload.get("delta"), dict) else {}
            con.execute(
                """
                insert into t_delta values (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    run_id,
                    str(obj.get("token_id") or ""),
                    int(obj.get("ts_ms") or payload.get("ts_ms") or 0),
                    payload.get("source"),
                    int(payload.get("top_k_applied") or 0),
                    json.dumps(delta.get("bids") or [], ensure_ascii=False),
                    json.dumps(delta.get("asks") or [], ensure_ascii=False),
                ],
            )
            n_delta += 1

    return {
        "snapshot_rows": n_snapshot,
        "snapshot_rle_rows": n_rle,
        "delta_rows": n_delta,
    }


def _copy_table(
    con: duckdb.DuckDBPyConnection,
    table: str,
    out_root: Path,
    run_id: str,
    dt_partition: bool,
) -> Optional[Path]:
    cnt = int(con.execute(f"select count(*) from {table}").fetchone()[0])
    if cnt <= 0:
        return None
    min_ts = con.execute(f"select min(ts_ms) from {table}").fetchone()[0] if table != "t_snapshot_rle" else con.execute(f"select min(span_start_ts_ms) from {table}").fetchone()[0]
    fact_name = {
        "t_snapshot": "book_snapshot_fact",
        "t_snapshot_rle": "book_snapshot_rle_fact",
        "t_delta": "book_delta_fact",
    }[table]
    dst = _target_file(out_root, fact_name, run_id, int(min_ts) if min_ts else None, dt_partition)
    con.execute(f"copy (select * from {table}) to ? (format parquet, compression zstd)", [str(dst)])
    return dst


def main() -> None:
    if duckdb is None:
        raise SystemExit("[err] duckdb is required. install with: pip install duckdb")

    ap = argparse.ArgumentParser()
    ap.add_argument("--run_root", default="", help="data root that contains raw/ and books/ (default uses --data_dir)")
    ap.add_argument("--run_id", default="", help="single run_id (default: compact all runs found)")
    ap.add_argument("--data_dir", default="backend/data", help="fallback data dir when --run_root not set")
    ap.add_argument("--out", required=True, help="output parquet root dir")
    ap.add_argument("--dt_partition", action="store_true", help="partition output under dt=YYYY-MM-DD")
    args = ap.parse_args()

    data_root = Path(args.run_root or args.data_dir).expanduser().resolve()
    out_root = Path(args.out).expanduser().resolve()
    out_root.mkdir(parents=True, exist_ok=True)

    run_ids = _find_run_ids(data_root, args.run_id or None)
    if not run_ids:
        raise SystemExit(f"[err] no run found under {data_root}")

    for run_id in run_ids:
        con = duckdb.connect()
        _create_tables(con)
        counts = _insert_rows(con, run_id, data_root)
        p1 = _copy_table(con, "t_snapshot", out_root, run_id, args.dt_partition)
        p2 = _copy_table(con, "t_snapshot_rle", out_root, run_id, args.dt_partition)
        p3 = _copy_table(con, "t_delta", out_root, run_id, args.dt_partition)
        con.close()
        print(f"[ok] run_id={run_id} snapshot={counts['snapshot_rows']} rle={counts['snapshot_rle_rows']} delta={counts['delta_rows']}")
        if p1:
            print(f"[ok] book_snapshot_fact -> {p1}")
        if p2:
            print(f"[ok] book_snapshot_rle_fact -> {p2}")
        if p3:
            print(f"[ok] book_delta_fact -> {p3}")


if __name__ == "__main__":
    main()
