#!/usr/bin/env python3
from __future__ import annotations

import argparse
import glob
from pathlib import Path

try:
    import duckdb  # type: ignore
except Exception:  # pragma: no cover
    duckdb = None  # type: ignore


def _table_exists(con: duckdb.DuckDBPyConnection, name: str) -> bool:
    q = "select count(*) from information_schema.tables where lower(table_name)=lower(?)"
    return int(con.execute(q, [name]).fetchone()[0]) > 0


def _create_fact_view(
    con: duckdb.DuckDBPyConnection,
    name: str,
    parquet_glob: str,
    empty_sql: str,
    replace: bool,
) -> None:
    files = glob.glob(parquet_glob, recursive=True)
    ckw = "create or replace view" if replace else "create view"
    if files:
        con.execute(f"{ckw} {name} as select * from read_parquet(?)", [parquet_glob])
    else:
        con.execute(f"{ckw} {name} as {empty_sql}")


def main() -> None:
    if duckdb is None:
        raise SystemExit("[err] duckdb is required. install with: pip install duckdb")

    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True, help="duckdb path")
    ap.add_argument("--parquet_root", required=True, help="root containing *_fact parquet dirs")
    ap.add_argument("--replace_views", action="store_true", help="create or replace views")
    args = ap.parse_args()

    db = Path(args.db).expanduser().resolve()
    root = Path(args.parquet_root).expanduser().resolve()
    db.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(db))

    _create_fact_view(
        con,
        "v_book_snapshot",
        str(root / "book_snapshot_fact" / "**" / "*.parquet"),
        """
        select
          cast(null as varchar) run_id,
          cast(null as varchar) token_id,
          cast(null as bigint) ts_ms,
          cast(null as bigint) exchange_ts_ms,
          cast(null as varchar) source,
          cast(null as varchar) reason,
          cast(null as varchar) state_hash,
          cast(null as integer) top_k,
          cast(null as varchar) bids_json,
          cast(null as varchar) asks_json
        where 1=0
        """,
        args.replace_views,
    )
    _create_fact_view(
        con,
        "v_book_snapshot_rle",
        str(root / "book_snapshot_rle_fact" / "**" / "*.parquet"),
        """
        select
          cast(null as varchar) run_id,
          cast(null as varchar) token_id,
          cast(null as varchar) source,
          cast(null as varchar) reason,
          cast(null as varchar) state_hash,
          cast(null as bigint) span_start_ts_ms,
          cast(null as bigint) span_end_ts_ms,
          cast(null as bigint) repeat_count,
          cast(null as integer) top_k
        where 1=0
        """,
        args.replace_views,
    )
    _create_fact_view(
        con,
        "v_book_delta",
        str(root / "book_delta_fact" / "**" / "*.parquet"),
        """
        select
          cast(null as varchar) run_id,
          cast(null as varchar) token_id,
          cast(null as bigint) ts_ms,
          cast(null as varchar) source,
          cast(null as integer) top_k,
          cast(null as varchar) delta_bids_json,
          cast(null as varchar) delta_asks_json
        where 1=0
        """,
        args.replace_views,
    )

    ckw = "create or replace view" if args.replace_views else "create view"
    if _table_exists(con, "token_dim") and _table_exists(con, "market_dim"):
        con.execute(
            f"""
            {ckw} v_book_snapshot_enriched as
            select
              s.*,
              t.market_id,
              t.market_slug,
              t.outcome_idx,
              t.outcome_name,
              m.question,
              m.event_id,
              m.event_slug,
              m.event_title,
              m.event_category,
              m.event_tags_json
            from v_book_snapshot s
            left join token_dim t on s.token_id = t.token_id
            left join market_dim m on t.market_id = m.market_id
            """
        )
        con.execute(
            f"""
            {ckw} v_book_delta_enriched as
            select
              d.*,
              t.market_id,
              t.market_slug,
              t.outcome_idx,
              t.outcome_name,
              m.question,
              m.event_id,
              m.event_slug,
              m.event_title,
              m.event_category,
              m.event_tags_json
            from v_book_delta d
            left join token_dim t on d.token_id = t.token_id
            left join market_dim m on t.market_id = m.market_id
            """
        )
    else:
        con.execute(
            f"""
            {ckw} v_book_snapshot_enriched as
            select
              s.*,
              cast(null as varchar) market_id,
              cast(null as varchar) market_slug,
              cast(null as integer) outcome_idx,
              cast(null as varchar) outcome_name,
              cast(null as varchar) question,
              cast(null as varchar) event_id,
              cast(null as varchar) event_slug,
              cast(null as varchar) event_title,
              cast(null as varchar) event_category,
              cast(null as varchar) event_tags_json
            from v_book_snapshot s
            """
        )
        con.execute(
            f"""
            {ckw} v_book_delta_enriched as
            select
              d.*,
              cast(null as varchar) market_id,
              cast(null as varchar) market_slug,
              cast(null as integer) outcome_idx,
              cast(null as varchar) outcome_name,
              cast(null as varchar) question,
              cast(null as varchar) event_id,
              cast(null as varchar) event_slug,
              cast(null as varchar) event_title,
              cast(null as varchar) event_category,
              cast(null as varchar) event_tags_json
            from v_book_delta d
            """
        )

    counts = {
        "v_book_snapshot": int(con.execute("select count(*) from v_book_snapshot").fetchone()[0]),
        "v_book_snapshot_rle": int(con.execute("select count(*) from v_book_snapshot_rle").fetchone()[0]),
        "v_book_delta": int(con.execute("select count(*) from v_book_delta").fetchone()[0]),
    }
    con.close()
    print(f"[ok] db={db}")
    print(f"[ok] parquet_root={root}")
    print(f"[ok] counts={counts}")


if __name__ == "__main__":
    main()
