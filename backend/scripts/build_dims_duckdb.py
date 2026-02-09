#!/usr/bin/env python3
from __future__ import annotations

import argparse
import glob
import json
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


def _to_bool(v: Any) -> Optional[bool]:
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)
    s = str(v).strip().lower()
    if s in ("1", "true", "yes", "y", "on"):
        return True
    if s in ("0", "false", "no", "n", "off"):
        return False
    return None


def _parse_json_list(v: Any) -> List[Any]:
    if v is None:
        return []
    if isinstance(v, list):
        return v
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return []
        if s.startswith("["):
            try:
                out = json.loads(s)
                if isinstance(out, list):
                    return out
            except Exception:
                return []
        return [s]
    return [v]


def _parse_clob_token_ids(m: Dict[str, Any]) -> List[str]:
    raw = m.get("_clobTokenIds_norm")
    if raw is None:
        raw = m.get("clobTokenIds") or m.get("clob_token_ids") or []
    arr = _parse_json_list(raw)
    out: List[str] = []
    for x in arr:
        s = str(x).strip()
        if s:
            out.append(s)
    return out


def _normalize_event_tags(v: Any) -> List[str]:
    arr = _parse_json_list(v)
    out: List[str] = []
    for x in arr:
        if isinstance(x, dict):
            name = x.get("name") or x.get("slug") or x.get("title") or x.get("id")
            if name is not None and str(name):
                out.append(str(name))
        else:
            s = str(x).strip()
            if s:
                out.append(s)
    return out


def _latest_dump(path: Optional[str], dumps_dir: str) -> Optional[str]:
    if path:
        return path
    fs = sorted(glob.glob(os.path.join(dumps_dir, "active_markets-*.jsonl")))
    if not fs:
        return None
    return fs[-1]


def _build_rows(dump_path: str) -> Tuple[List[Tuple[Any, ...]], List[Tuple[Any, ...]]]:
    market_rows: List[Tuple[Any, ...]] = []
    token_rows: List[Tuple[Any, ...]] = []
    ts_ms = int(time.time() * 1000)

    with open(dump_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                m = json.loads(line)
            except Exception:
                continue
            if not isinstance(m, dict):
                continue

            market_id = str(m.get("id") or m.get("market_id") or m.get("slug") or "").strip()
            market_slug = str(m.get("slug") or "").strip()
            if not market_id and not market_slug:
                continue
            if not market_id:
                market_id = market_slug

            question = m.get("question") or m.get("title") or ""
            event_id = str(m.get("event_id") or "").strip()
            event_slug = str(m.get("event_slug") or "").strip()
            event_title = str(m.get("event_title") or "").strip()
            event_category = str(m.get("_event_category_norm") or m.get("event_category") or "").strip()
            event_tags = _normalize_event_tags(m.get("_event_tags_norm") or m.get("event_tags"))
            outcomes = [str(x) for x in _parse_json_list(m.get("outcomes"))]
            token_ids = _parse_clob_token_ids(m)

            market_rows.append(
                (
                    market_id,
                    market_slug,
                    str(question),
                    event_id,
                    event_slug,
                    event_title,
                    event_category,
                    json.dumps(event_tags, ensure_ascii=False),
                    json.dumps(outcomes, ensure_ascii=False),
                    json.dumps(token_ids, ensure_ascii=False),
                    len(token_ids),
                    _to_bool(m.get("active")),
                    _to_bool(m.get("closed")),
                    _to_bool(m.get("archived")),
                    _to_bool(m.get("acceptingOrders")),
                    _to_bool(m.get("enableOrderBook")),
                    str(m.get("startDate") or m.get("start_date") or ""),
                    str(m.get("endDate") or m.get("end_date") or ""),
                    dump_path,
                    ts_ms,
                )
            )

            for i, tid in enumerate(token_ids):
                outcome_name = outcomes[i] if i < len(outcomes) else None
                token_rows.append(
                    (
                        str(tid),
                        market_id,
                        market_slug,
                        i,
                        outcome_name,
                        event_id,
                        event_slug,
                        event_title,
                        event_category,
                        json.dumps(event_tags, ensure_ascii=False),
                        _to_bool(m.get("active")),
                        _to_bool(m.get("closed")),
                        _to_bool(m.get("archived")),
                        dump_path,
                        ts_ms,
                    )
                )

    return market_rows, token_rows


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dump", default="", help="active_markets jsonl path; default latest in --dumps_dir")
    ap.add_argument("--dumps_dir", default="backend/data/dumps", help="directory containing active_markets-*.jsonl")
    ap.add_argument("--db", default="backend/data/warehouse/pmliq.duckdb", help="DuckDB file path")
    ap.add_argument("--replace", action="store_true", help="replace all rows in market_dim/token_dim")
    args = ap.parse_args()

    dump_path = _latest_dump(args.dump or None, args.dumps_dir)
    if not dump_path:
        raise SystemExit("[err] no active_markets dump found; run dump_active_markets.py first")

    market_rows, token_rows = _build_rows(dump_path)
    if not market_rows:
        raise SystemExit(f"[err] no rows parsed from dump: {dump_path}")

    try:
        import duckdb  # type: ignore
    except Exception:
        raise SystemExit("[err] duckdb is not installed. Install with: pip install duckdb")

    db_path = Path(args.db)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(db_path))
    try:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS market_dim (
                market_id VARCHAR,
                market_slug VARCHAR,
                question VARCHAR,
                event_id VARCHAR,
                event_slug VARCHAR,
                event_title VARCHAR,
                event_category VARCHAR,
                event_tags_json VARCHAR,
                outcomes_json VARCHAR,
                clob_token_ids_json VARCHAR,
                clob_tokens_n INTEGER,
                active BOOLEAN,
                closed BOOLEAN,
                archived BOOLEAN,
                accepting_orders BOOLEAN,
                enable_orderbook BOOLEAN,
                start_date VARCHAR,
                end_date VARCHAR,
                source_dump_path VARCHAR,
                ingested_ts_ms BIGINT
            )
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS token_dim (
                token_id VARCHAR,
                market_id VARCHAR,
                market_slug VARCHAR,
                outcome_index INTEGER,
                outcome_name VARCHAR,
                event_id VARCHAR,
                event_slug VARCHAR,
                event_title VARCHAR,
                event_category VARCHAR,
                event_tags_json VARCHAR,
                active BOOLEAN,
                closed BOOLEAN,
                archived BOOLEAN,
                source_dump_path VARCHAR,
                ingested_ts_ms BIGINT
            )
            """
        )
        if args.replace:
            con.execute("DELETE FROM market_dim")
            con.execute("DELETE FROM token_dim")
        con.executemany(
            """
            INSERT INTO market_dim VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            market_rows,
        )
        con.executemany(
            """
            INSERT INTO token_dim VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            token_rows,
        )
        m_n = con.execute("SELECT COUNT(*) FROM market_dim").fetchone()[0]
        t_n = con.execute("SELECT COUNT(*) FROM token_dim").fetchone()[0]
    finally:
        con.close()

    print(f"[ok] dump={dump_path}")
    print(f"[ok] db={db_path}")
    print(f"[ok] inserted market_rows={len(market_rows)} token_rows={len(token_rows)}")
    print(f"[ok] table_counts market_dim={m_n} token_dim={t_n}")


if __name__ == "__main__":
    main()

