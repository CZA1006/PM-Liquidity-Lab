#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.request import Request, urlopen


def _is_port_open(host: str, port: int, timeout: float = 0.25) -> bool:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(timeout)
    try:
        s.connect((host, port))
        return True
    except Exception:
        return False
    finally:
        s.close()


def _health_ok(api: str, timeout: float = 1.5) -> bool:
    try:
        req = Request(f"{api.rstrip('/')}/health", headers={"Accept": "application/json"})
        with urlopen(req, timeout=timeout) as resp:
            data = resp.read().decode("utf-8", errors="replace")
        return '"ok"' in data and "true" in data.lower()
    except Exception:
        return False


def _kill_port(port: int) -> None:
    try:
        out = subprocess.check_output(["lsof", "-ti", f"tcp:{port}"], text=True).strip()
    except Exception:
        return
    for line in out.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            os.kill(int(line), signal.SIGTERM)
        except Exception:
            pass


def _wait_healthy(apis: List[str], timeout_s: int = 45) -> List[str]:
    pending = set(apis)
    deadline = time.time() + float(timeout_s)
    while pending and time.time() < deadline:
        for api in list(pending):
            if _health_ok(api):
                pending.remove(api)
        if pending:
            time.sleep(1.0)
    return sorted(pending)


def _latest_dump(dumps_dir: Path) -> Optional[Path]:
    xs = sorted(dumps_dir.glob("active_markets-*.jsonl"), key=lambda p: p.stat().st_mtime, reverse=True)
    return xs[0] if xs else None


def _run(cmd: List[str], env: Optional[Dict[str, str]] = None) -> None:
    print("[run]", " ".join(cmd))
    subprocess.run(cmd, check=True, env=env)


def _to_int(v: str) -> int:
    try:
        return int(float(v or "0"))
    except Exception:
        return 0


def _summarize_csv(csv_path: Path) -> None:
    rows = list(csv.DictReader(open(csv_path, "r", encoding="utf-8")))
    if not rows:
        print("[warn] no rows in report:", csv_path)
        return

    total_shards = len(rows)
    total_tokens = sum(_to_int(r.get("shard_tokens", "0")) for r in rows)
    total_raw = sum(_to_int(r.get("raw_bytes_delta", "0")) for r in rows)
    total_delta = sum(_to_int(r.get("books_delta_bytes_delta", "0")) for r in rows)
    total_snap = sum(_to_int(r.get("books_snapshot_bytes_delta", "0")) for r in rows)
    max_dur = max(1, max(_to_int(r.get("duration_s", "0")) for r in rows))

    mb = 1024.0 * 1024.0
    raw_mbh = (float(total_raw) / float(max_dur)) * 3600.0 / mb
    delta_mbh = (float(total_delta) / float(max_dur)) * 3600.0 / mb
    snap_mbh = (float(total_snap) / float(max_dur)) * 3600.0 / mb

    print("\n=== Capture Summary ===")
    print("report_csv:", csv_path)
    print("rows(shards):", total_shards)
    print("total_shard_tokens:", total_tokens)
    print("wall_duration_s:", max_dur)
    print("raw_bytes_total:", total_raw, "raw_mb_per_hour_wall:", round(raw_mbh, 3))
    print("books_delta_bytes_total:", total_delta, "delta_mb_per_hour_wall:", round(delta_mbh, 3))
    print("books_snapshot_bytes_total:", total_snap, "snapshot_mb_per_hour_wall:", round(snap_mbh, 3))


def main() -> None:
    ap = argparse.ArgumentParser(
        description="One-shot active-market capture wave: dump + workers + shard capture + summary."
    )
    ap.add_argument("--pmdb", required=True, help="external disk root, e.g. /Volumes/T7/PolyMarket DB")
    ap.add_argument("--api_base", default="http://127.0.0.1")
    ap.add_argument("--base_port", type=int, default=8101, help="first worker port")
    ap.add_argument("--workers", type=int, default=24, help="worker count")
    ap.add_argument("--clean_ports", action="store_true", help="kill existing processes on worker ports first")
    ap.add_argument("--keep_workers", action="store_true", help="keep workers alive after capture")
    ap.add_argument("--run_prefix", default="active15m")

    ap.add_argument("--refresh_dump", action="store_true", help="refresh active markets dump before capture")
    ap.add_argument("--dump", default="", help="use existing dump path (skip refresh when set)")
    ap.add_argument("--dump_max", type=int, default=40000)
    ap.add_argument("--dump_page", type=int, default=200)
    ap.add_argument("--dump_timeout", type=int, default=120)
    ap.add_argument("--dump_retries", type=int, default=5)
    ap.add_argument("--dump_retry_sleep", type=int, default=3)

    ap.add_argument("--shard_tokens", type=int, default=100)
    ap.add_argument("--shard_duration", type=int, default=900, help="seconds per shard")
    ap.add_argument("--poll", type=int, default=10)
    ap.add_argument("--max_shards", type=int, default=24)
    ap.add_argument("--refresh_active_every_sec", type=int, default=120)
    ap.add_argument("--sort_by", choices=["none", "volume"], default="volume")
    ap.add_argument("--shuffle_seed", type=int, default=None)
    ap.add_argument("--out", default="", help="output report csv path")
    args = ap.parse_args()

    repo_root = Path(__file__).resolve().parents[2]
    pmdb = Path(args.pmdb).expanduser()
    data_dir = pmdb / "data"
    dumps_dir = pmdb / "dumps"
    reports_dir = pmdb / "reports"
    logs_dir = reports_dir / "worker_logs"
    for p in (data_dir, dumps_dir, reports_dir, logs_dir):
        p.mkdir(parents=True, exist_ok=True)

    ports = [int(args.base_port) + i for i in range(int(args.workers))]
    apis = [f"{args.api_base.rstrip('/')}:{p}" for p in ports]

    if args.clean_ports:
        for p in ports:
            _kill_port(p)
        time.sleep(1.0)
    else:
        occupied = [p for p in ports if _is_port_open("127.0.0.1", p)]
        if occupied:
            raise SystemExit(
                "[err] some worker ports already in use: "
                + ",".join(str(x) for x in occupied)
                + " (use --clean_ports or change --base_port)"
            )

    procs: List[Tuple[int, subprocess.Popen, object]] = []
    try:
        # Start workers.
        for p in ports:
            log_fp = open(logs_dir / f"uvicorn_{p}.log", "a", encoding="utf-8")
            env = os.environ.copy()
            env["DATA_DIR"] = str(data_dir)
            env["RUN_ID"] = f"{args.run_prefix}_w{p}"
            proc = subprocess.Popen(
                [
                    "uvicorn",
                    "backend.app.main:app",
                    "--host",
                    "127.0.0.1",
                    "--port",
                    str(p),
                ],
                cwd=str(repo_root),
                env=env,
                stdout=log_fp,
                stderr=log_fp,
            )
            procs.append((p, proc, log_fp))

        down = _wait_healthy(apis, timeout_s=60)
        if down:
            raise SystemExit("[err] workers not healthy: " + ",".join(down))

        # Determine dump file.
        dump_path: Optional[Path] = None
        if args.dump:
            dump_path = Path(args.dump).expanduser()
            if not dump_path.exists():
                raise SystemExit(f"[err] --dump not found: {dump_path}")
        else:
            if args.refresh_dump or _latest_dump(dumps_dir) is None:
                _run(
                    [
                        sys.executable,
                        str(repo_root / "backend/scripts/dump_active_markets.py"),
                        "--api",
                        apis[0],
                        "--out",
                        str(dumps_dir),
                        "--max",
                        str(int(args.dump_max)),
                        "--page",
                        str(int(args.dump_page)),
                        "--timeout",
                        str(int(args.dump_timeout)),
                        "--retries",
                        str(int(args.dump_retries)),
                        "--retry_sleep",
                        str(int(args.dump_retry_sleep)),
                    ]
                )
            dump_path = _latest_dump(dumps_dir)
            if dump_path is None:
                raise SystemExit(f"[err] no dump found in {dumps_dir}")

        out_path = Path(args.out).expanduser() if args.out else (
            reports_dir / f"active_capture_{time.strftime('%Y%m%d_%H%M%S')}.csv"
        )
        out_path.parent.mkdir(parents=True, exist_ok=True)

        worker_apis = ",".join(apis)
        cmd = [
            sys.executable,
            str(repo_root / "backend/scripts/capture_dump_shards.py"),
            "--api",
            apis[0],
            "--worker_apis",
            worker_apis,
            "--dump",
            str(dump_path),
            "--monitorable_only",
            "--sort_by",
            str(args.sort_by),
            "--shard_tokens",
            str(int(args.shard_tokens)),
            "--shard_duration",
            str(int(args.shard_duration)),
            "--poll",
            str(int(args.poll)),
            "--max_shards",
            str(int(args.max_shards)),
            "--refresh_active_every_sec",
            str(int(args.refresh_active_every_sec)),
            "--out",
            str(out_path),
        ]
        if args.shuffle_seed is not None:
            cmd.extend(["--shuffle_seed", str(int(args.shuffle_seed))])
        _run(cmd)
        _summarize_csv(out_path)
        print("\n[ok] worker logs:", logs_dir)

    finally:
        if not args.keep_workers:
            for _, proc, _ in procs:
                try:
                    proc.terminate()
                except Exception:
                    pass
            deadline = time.time() + 10.0
            for _, proc, _ in procs:
                try:
                    left = max(0.1, deadline - time.time())
                    proc.wait(timeout=left)
                except Exception:
                    try:
                        proc.kill()
                    except Exception:
                        pass
            for _, _, fp in procs:
                try:
                    fp.close()
                except Exception:
                    pass


if __name__ == "__main__":
    main()
