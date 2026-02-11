#!/usr/bin/env python3
from __future__ import annotations

import argparse
import math
import os
import subprocess
from pathlib import Path
from typing import Any, Dict, Iterable, List

from capture_dump_shards import _collect_tokens_from_dump  # reuse existing dump parsing logic


def _to_int(v: Any, default: int) -> int:
    try:
        return int(v)
    except Exception:
        return default


def _to_float(v: Any, default: float) -> float:
    try:
        return float(v)
    except Exception:
        return default


def _physical_cpu_count() -> int:
    # macOS first
    try:
        out = subprocess.check_output(["sysctl", "-n", "hw.physicalcpu"], text=True).strip()
        n = int(out)
        if n > 0:
            return n
    except Exception:
        pass
    # Linux fallback
    try:
        out = subprocess.check_output(["nproc", "--all"], text=True).strip()
        n = int(out)
        if n > 0:
            return n
    except Exception:
        pass
    return max(1, (os.cpu_count() or 1))


def _logical_cpu_count() -> int:
    return max(1, (os.cpu_count() or 1))


def _memory_total_gb() -> float:
    # POSIX path
    try:
        page_size = os.sysconf("SC_PAGE_SIZE")
        pages = os.sysconf("SC_PHYS_PAGES")
        if isinstance(page_size, int) and isinstance(pages, int) and page_size > 0 and pages > 0:
            return float(page_size * pages) / (1024.0**3)
    except Exception:
        pass
    # macOS fallback
    try:
        out = subprocess.check_output(["sysctl", "-n", "hw.memsize"], text=True).strip()
        mem = int(out)
        if mem > 0:
            return float(mem) / (1024.0**3)
    except Exception:
        pass
    return 0.0


def _parse_candidates(s: str) -> List[int]:
    out: List[int] = []
    for x in (s or "").split(","):
        x = x.strip()
        if not x:
            continue
        try:
            v = int(x)
        except Exception:
            continue
        if v > 0:
            out.append(v)
    return sorted(set(out))


def _fmt_hhmmss(sec: int) -> str:
    sec = max(0, int(sec))
    h = sec // 3600
    m = (sec % 3600) // 60
    s = sec % 60
    return f"{h:02d}:{m:02d}:{s:02d}"


def _plan_rows(token_total: int, shard_duration_s: int, worker_cap: int, shard_tokens: Iterable[int]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for st in shard_tokens:
        shards = int(math.ceil(float(token_total) / float(st)))
        waves = int(math.ceil(float(shards) / float(max(1, worker_cap))))
        total_s = int(waves * shard_duration_s)
        rows.append(
            {
                "shard_tokens": st,
                "required_workers_for_15m": shards,
                "fits_single_window": shards <= worker_cap,
                "waves_at_worker_cap": waves,
                "estimated_total_s": total_s,
                "estimated_total_hhmmss": _fmt_hhmmss(total_s),
            }
        )
    return rows


def main() -> None:
    ap = argparse.ArgumentParser(
        description=(
            "Estimate worker/shard strategy for active-market capture. "
            "Shows whether you can cover all active tokens inside one 15-min window."
        )
    )
    ap.add_argument("--dump", required=True, help="active_markets dump jsonl path")
    ap.add_argument("--monitorable_only", action="store_true", help="same filter as capture script")
    ap.add_argument("--sort_by", choices=["none", "volume"], default="none")
    ap.add_argument("--shuffle_seed", type=int, default=None)
    ap.add_argument("--shard_duration_s", type=int, default=900, help="seconds per shard (900=15min)")
    ap.add_argument(
        "--shard_tokens_candidates",
        default="100,150,200,250,300,400,500,800,1000,1500,2000",
        help="comma-separated shard token sizes to evaluate",
    )
    ap.add_argument("--cpu_oversubscribe", type=float, default=2.0, help="logical CPU multiplier")
    ap.add_argument("--worker_mem_gb", type=float, default=0.40, help="estimated RAM per worker")
    ap.add_argument("--reserve_mem_gb", type=float, default=4.0, help="RAM reserved for system/other processes")
    ap.add_argument("--max_workers_cap", type=int, default=256, help="hard cap to avoid absurd suggestions")
    args = ap.parse_args()

    dump_path = Path(args.dump)
    if not dump_path.exists():
        raise SystemExit(f"[err] dump not found: {dump_path}")

    tokens = _collect_tokens_from_dump(
        dump_path,
        monitorable_only=bool(args.monitorable_only),
        sort_by=str(args.sort_by),
        shuffle_seed=args.shuffle_seed,
    )
    token_total = len(tokens)
    if token_total <= 0:
        raise SystemExit("[err] no tokens parsed from dump")

    phy = _physical_cpu_count()
    logi = _logical_cpu_count()
    mem_gb = _memory_total_gb()

    cpu_cap = max(1, int(math.floor(float(logi) * _to_float(args.cpu_oversubscribe, 2.0))))
    mem_cap = max(
        1,
        int(
            math.floor(
                max(0.0, mem_gb - _to_float(args.reserve_mem_gb, 4.0))
                / max(0.05, _to_float(args.worker_mem_gb, 0.40))
            )
        ),
    )
    worker_cap = max(1, min(cpu_cap, mem_cap, _to_int(args.max_workers_cap, 256)))

    candidates = _parse_candidates(str(args.shard_tokens_candidates))
    if not candidates:
        candidates = [100, 200, 400, 800, 1000]

    rows = _plan_rows(
        token_total=token_total,
        shard_duration_s=max(1, int(args.shard_duration_s)),
        worker_cap=worker_cap,
        shard_tokens=candidates,
    )

    # minimum shard_tokens needed to finish in one wave at current worker_cap
    min_tokens_for_one_wave = int(math.ceil(float(token_total) / float(worker_cap)))

    print("=== Machine Estimate ===")
    print("dump:", dump_path)
    print("tokens_total:", token_total)
    print("cpu_physical:", phy)
    print("cpu_logical:", logi)
    print("memory_total_gb:", round(mem_gb, 2))
    print("worker_cap_cpu:", cpu_cap)
    print("worker_cap_mem:", mem_cap)
    print("worker_cap_final:", worker_cap)
    print("shard_duration_s:", int(args.shard_duration_s), f"({_fmt_hhmmss(int(args.shard_duration_s))})")
    print("min_shard_tokens_for_one_wave:", min_tokens_for_one_wave)

    print("\n=== Candidate Plans ===")
    print(
        "shard_tokens | req_workers(15m) | fits_15m | waves@cap | est_total\n"
        "------------+-------------------+----------+-----------+-----------"
    )
    for r in rows:
        print(
            f"{r['shard_tokens']:>11} | "
            f"{r['required_workers_for_15m']:>17} | "
            f"{str(r['fits_single_window']):>8} | "
            f"{r['waves_at_worker_cap']:>9} | "
            f"{r['estimated_total_hhmmss']}"
        )

    good = [r for r in rows if r["fits_single_window"]]
    if good:
        # choose smallest shard_tokens that still fits one-wave for better per-worker load quality
        rec = sorted(good, key=lambda x: x["shard_tokens"])[0]
        print("\n=== Recommended (single 15m window possible) ===")
        print(
            f"Use shard_tokens={rec['shard_tokens']} with workers~{rec['required_workers_for_15m']} "
            f"(<= cap {worker_cap})"
        )
    else:
        # choose fastest total-time candidate under current cap
        rec = sorted(rows, key=lambda x: x["estimated_total_s"])[0]
        print("\n=== Recommended (single 15m window NOT possible at current cap) ===")
        print(
            f"At worker_cap={worker_cap}, best candidate shard_tokens={rec['shard_tokens']} "
            f"still needs {rec['waves_at_worker_cap']} waves (est {rec['estimated_total_hhmmss']})"
        )
        print(
            f"To force one-wave 15m, you need either workers >= {rec['required_workers_for_15m']} "
            f"or shard_tokens >= {min_tokens_for_one_wave}"
        )


if __name__ == "__main__":
    main()
