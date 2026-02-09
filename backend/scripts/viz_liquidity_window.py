#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
import shutil
import subprocess
import tempfile
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from _http import http_get_json


def _resolve_path(p: Optional[str]) -> Optional[Path]:
    if not p:
        return None
    rp = Path(p)
    if rp.is_absolute():
        return rp if rp.exists() else None
    cand1 = (Path.cwd() / rp).resolve()
    cand2 = (Path.cwd() / "backend" / rp).resolve()
    cand3 = None
    parts = rp.parts
    if len(parts) >= 2 and parts[0] == "." and parts[1] == "data":
        cand3 = (Path.cwd() / "backend" / Path(*parts[1:])).resolve()
    for cand in (cand1, cand2, cand3):
        if cand and cand.exists():
            return cand
    return None


def _to_iso(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).astimezone().isoformat(timespec="seconds")


def _parse_ts_arg(v: str) -> int:
    s = (v or "").strip()
    if not s:
        raise ValueError("empty timestamp")
    if s.isdigit():
        n = int(s)
        if n > 10_000_000_000:
            return n
        return n * 1000
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def _infer_events_path(api: str) -> Optional[Path]:
    try:
        st = http_get_json(f"{api.rstrip('/')}/monitor/status")
    except Exception:
        return None
    if not isinstance(st, dict):
        return None

    run_id = st.get("run_id")
    if isinstance(run_id, str) and run_id:
        p = _resolve_path(f"./data/runs/{run_id}/events.jsonl")
        if p:
            return p

    raw_path = st.get("raw_log_path")
    rp = _resolve_path(raw_path) if isinstance(raw_path, str) else None
    if rp:
        parts = rp.parts
        try:
            i_data = parts.index("data")
            i_raw = parts.index("raw")
            if i_raw == i_data + 1 and i_raw + 1 < len(parts):
                run = parts[i_raw + 1]
                base = Path(*parts[: i_data + 1])
                c = (base / "runs" / run / "events.jsonl").resolve()
                if c.exists():
                    return c
        except ValueError:
            pass
    return _resolve_path("backend/data/runs/local/events.jsonl")


def _parse_impact_bps(slippage: Any, side: str, notional: float) -> Optional[float]:
    if not isinstance(slippage, dict):
        return None
    sd = slippage.get(side)
    if not isinstance(sd, dict):
        return None
    k1 = str(notional)
    k2 = f"{float(notional):.1f}"
    row = sd.get(k1) or sd.get(k2)
    if row is None:
        for k, v in sd.items():
            try:
                if float(k) == float(notional):
                    row = v
                    break
            except Exception:
                continue
    if not isinstance(row, dict):
        return None
    x = row.get("impact_bps")
    try:
        return float(x) if x is not None else None
    except Exception:
        return None


def _token_meta(db_path: Optional[Path]) -> Dict[str, Dict[str, str]]:
    if not db_path or not db_path.exists():
        return {}
    try:
        import duckdb  # type: ignore
    except Exception:
        return {}

    out: Dict[str, Dict[str, str]] = {}
    con = duckdb.connect(str(db_path))
    try:
        rows = con.execute(
            """
            select token_id, coalesce(market_slug,''), coalesce(outcome_name,'')
            from token_dim
            """
        ).fetchall()
        for tid, slug, outcome in rows:
            if tid is None:
                continue
            out[str(tid)] = {"market_slug": str(slug or ""), "outcome_name": str(outcome or "")}
    finally:
        con.close()
    return out


def _sample_points(xs: List[Tuple[int, float]], max_points: int) -> List[Tuple[int, float]]:
    if len(xs) <= max_points:
        return xs
    step = float(len(xs)) / float(max_points)
    out: List[Tuple[int, float]] = []
    i = 0.0
    while int(i) < len(xs):
        out.append(xs[int(i)])
        i += step
    return out


def _percentile(vals: List[float], pct: float) -> Optional[float]:
    if not vals:
        return None
    xs = sorted(vals)
    k = (len(xs) - 1) * (pct / 100.0)
    f = int(k)
    c = min(f + 1, len(xs) - 1)
    if c == f:
        return float(xs[f])
    return float(xs[f] + (xs[c] - xs[f]) * (k - f))


def _clip_bounds(vals: List[float], y_clip_p: float) -> Tuple[float, float]:
    v_min = min(vals)
    v_max = max(vals)
    if y_clip_p >= 100.0:
        return v_min, v_max
    # Keep central y_clip_p percent and clamp tails.
    p = min(max(float(y_clip_p), 50.0), 100.0)
    tail = (100.0 - p) / 2.0
    lo = _percentile(vals, tail)
    hi = _percentile(vals, 100.0 - tail)
    if lo is None or hi is None or hi <= lo:
        return v_min, v_max
    return float(lo), float(hi)


def _svg_polyline(
    points: List[Tuple[int, float]],
    ts_min: int,
    ts_max: int,
    v_min: float,
    v_max: float,
    x0: int,
    y0: int,
    w: int,
    h: int,
) -> str:
    if not points or ts_max <= ts_min or v_max <= v_min:
        return ""
    coords: List[str] = []
    for ts, v in points:
        vv = min(max(v, v_min), v_max)
        x = x0 + (float(ts - ts_min) / float(ts_max - ts_min)) * w
        y = y0 + h - (float(vv - v_min) / float(v_max - v_min)) * h
        coords.append(f"{x:.1f},{y:.1f}")
    return " ".join(coords)


def _color(i: int) -> str:
    palette = [
        "#0ea5e9",
        "#22c55e",
        "#f59e0b",
        "#ef4444",
        "#6366f1",
        "#14b8a6",
        "#eab308",
        "#a855f7",
        "#f97316",
        "#10b981",
    ]
    return palette[i % len(palette)]


def _normalize_outcome_key(v: Any) -> str:
    s = str(v or "").strip().lower()
    if s in ("yes", "no"):
        return s
    if s:
        return s
    return "unknown"


def _write_svg(
    out_svg: Path,
    rows: List[Dict[str, Any]],
    max_points_per_series: int,
    title: str,
    split_outcomes: bool,
    y_clip_p: float,
) -> None:
    if not rows:
        out_svg.write_text("<svg xmlns='http://www.w3.org/2000/svg' width='1200' height='200'></svg>", encoding="utf-8")
        return

    series: Dict[str, Dict[str, List[Tuple[int, float]]]] = defaultdict(lambda: defaultdict(list))
    label_outcome: Dict[str, str] = {}
    ts_min = min(int(r["ts_ms"]) for r in rows)
    ts_max = max(int(r["ts_ms"]) for r in rows)
    for r in rows:
        ts = int(r["ts_ms"])
        label = str(r["series_label"])
        label_outcome[label] = _normalize_outcome_key(r.get("outcome_name"))
        for k in ("mid", "spread_bps", "impact_buy_100_bps", "impact_sell_100_bps"):
            v = r.get(k)
            if isinstance(v, (int, float)) and math.isfinite(float(v)):
                series[k][label].append((ts, float(v)))

    width = 1400
    panel_h = 240
    margin_l = 70
    margin_r = 20
    margin_t = 36
    margin_b = 30
    panel_gap = 30
    chart_w = width - margin_l - margin_r
    metric_defs: List[Tuple[List[str], str]] = [
        (["mid"], "mid"),
        (["spread_bps"], "spread_bps"),
        (["impact_buy_100_bps", "impact_sell_100_bps"], "impact_buy_100 / impact_sell_100 (bps)"),
    ]

    panel_specs: List[Tuple[List[str], str, Optional[str]]] = []
    if split_outcomes:
        known = {"yes", "no"}
        outcomes = sorted(set(label_outcome.values()))
        ordered: List[str] = [x for x in ("yes", "no") if x in outcomes]
        ordered.extend([x for x in outcomes if x not in known])
        for metric_keys, y_title in metric_defs:
            for oc in ordered:
                panel_specs.append((metric_keys, f"{y_title} [{oc}]", oc))
    else:
        for metric_keys, y_title in metric_defs:
            panel_specs.append((metric_keys, y_title, None))

    total_h = margin_t + (panel_h + panel_gap) * len(panel_specs) - panel_gap + margin_b + 80

    labels = sorted(set(str(r["series_label"]) for r in rows))
    label_color: Dict[str, str] = {lb: _color(i) for i, lb in enumerate(labels)}

    def _panel(metric_keys: List[str], y_title: str, i_panel: int, outcome_filter: Optional[str]) -> str:
        y0 = margin_t + i_panel * (panel_h + panel_gap)
        all_vals: List[float] = []
        for metric in metric_keys:
            for label, pts in series[metric].items():
                if outcome_filter and label_outcome.get(label) != outcome_filter:
                    continue
                all_vals.extend([v for _, v in pts])
        if not all_vals:
            return (
                f"<text x='{margin_l}' y='{y0+16}' fill='#94a3b8' font-size='13'>{y_title}: no data</text>"
            )

        v_min, v_max = _clip_bounds(all_vals, y_clip_p=y_clip_p)
        if abs(v_max - v_min) < 1e-12:
            v_min -= 1.0
            v_max += 1.0

        parts: List[str] = []
        parts.append(f"<rect x='{margin_l}' y='{y0}' width='{chart_w}' height='{panel_h}' fill='none' stroke='#334155'/>")
        parts.append(f"<text x='{margin_l}' y='{y0-8}' fill='#e2e8f0' font-size='13'>{y_title}</text>")
        parts.append(f"<text x='{margin_l+4}' y='{y0+14}' fill='#64748b' font-size='11'>max={v_max:.4f}</text>")
        parts.append(
            f"<text x='{margin_l+4}' y='{y0+panel_h-6}' fill='#64748b' font-size='11'>min={v_min:.4f}</text>"
        )
        if y_clip_p < 100.0:
            parts.append(
                f"<text x='{margin_l+150}' y='{y0+14}' fill='#64748b' font-size='11'>y_clip_p={y_clip_p:.1f}</text>"
            )

        for label in labels:
            if outcome_filter and label_outcome.get(label) != outcome_filter:
                continue
            for metric in metric_keys:
                pts_raw = series[metric].get(label) or []
                if not pts_raw:
                    continue
                pts = _sample_points(sorted(pts_raw, key=lambda x: x[0]), max_points_per_series)
                poly = _svg_polyline(pts, ts_min, ts_max, v_min, v_max, margin_l, y0, chart_w, panel_h)
                if not poly:
                    continue
                c = label_color.get(label, "#e2e8f0")
                dash = "6,4" if metric == "impact_sell_100_bps" else ""
                extra = f" stroke-dasharray='{dash}'" if dash else ""
                parts.append(f"<polyline points='{poly}' fill='none' stroke='{c}' stroke-width='1.6'{extra}/>")
        return "\n".join(parts)

    legend_items: List[str] = []
    for i, lb in enumerate(labels):
        x = margin_l + (i % 4) * 320
        y = total_h - 56 + (i // 4) * 18
        legend_items.append(f"<line x1='{x}' y1='{y}' x2='{x+14}' y2='{y}' stroke='{label_color[lb]}' stroke-width='2'/>")
        legend_items.append(f"<text x='{x+18}' y='{y+4}' fill='#cbd5e1' font-size='12'>{lb}</text>")
    legend_items.append(
        f"<text x='{margin_l}' y='{total_h-10}' fill='#94a3b8' font-size='11'>impact_sell_100_bps is dashed</text>"
    )

    svg = f"""<svg xmlns='http://www.w3.org/2000/svg' width='{width}' height='{total_h}' viewBox='0 0 {width} {total_h}'>
<rect x='0' y='0' width='{width}' height='{total_h}' fill='#0b1220'/>
<text x='{margin_l}' y='22' fill='#f8fafc' font-size='16'>{title}</text>
<text x='{margin_l}' y='38' fill='#94a3b8' font-size='11'>window: {_to_iso(ts_min)} .. {_to_iso(ts_max)} | rows={len(rows)}</text>
{chr(10).join(_panel(keys, panel_title, i, oc) for i, (keys, panel_title, oc) in enumerate(panel_specs))}
{"".join(legend_items)}
</svg>
"""
    out_svg.write_text(svg, encoding="utf-8")


def _write_png_from_svg(svg_path: Path, png_path: Path) -> bool:
    # 1) Preferred: pure Python conversion if cairosvg is installed.
    try:
        import cairosvg  # type: ignore

        cairosvg.svg2png(url=str(svg_path), write_to=str(png_path))
        return png_path.exists()
    except Exception:
        pass

    # 2) Common CLI converters.
    cli_candidates: List[List[str]] = []
    if shutil.which("rsvg-convert"):
        cli_candidates.append(["rsvg-convert", str(svg_path), "-o", str(png_path)])
    if shutil.which("inkscape"):
        cli_candidates.append(["inkscape", str(svg_path), "--export-type=png", f"--export-filename={png_path}"])
    if shutil.which("magick"):
        cli_candidates.append(["magick", "convert", str(svg_path), str(png_path)])

    for cmd in cli_candidates:
        try:
            subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            if png_path.exists():
                return True
        except Exception:
            continue

    # 3) macOS fallback: qlmanage thumbnail as PNG.
    if shutil.which("qlmanage"):
        try:
            with tempfile.TemporaryDirectory() as td:
                subprocess.run(
                    ["qlmanage", "-t", "-s", "4096", "-o", td, str(svg_path)],
                    check=True,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
                src = Path(td) / f"{svg_path.name}.png"
                if src.exists():
                    png_path.parent.mkdir(parents=True, exist_ok=True)
                    shutil.move(str(src), str(png_path))
                    return True
        except Exception:
            pass

    return False


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--api", default="http://127.0.0.1:8000", help="backend api (for events path inference)")
    ap.add_argument("--events", default="", help="events.jsonl path; default infer from /monitor/status")
    ap.add_argument("--db", default="backend/data/warehouse/pmliq.duckdb", help="duckdb path for token->market mapping")
    ap.add_argument("--market_slug", default="", help="filter a single market slug")
    ap.add_argument("--token_ids", default="", help="comma-separated token ids filter")
    ap.add_argument("--start", default="", help="start time (epoch sec/ms or ISO8601)")
    ap.add_argument("--end", default="", help="end time (epoch sec/ms or ISO8601)")
    ap.add_argument("--recent_sec", type=int, default=0, help="use now-recent_sec..now when start/end not set")
    ap.add_argument("--max_points_per_series", type=int, default=1500, help="downsample cap per line in SVG")
    ap.add_argument("--split_outcomes", action="store_true", help="split YES/NO (or outcome_name) into separate panels")
    ap.add_argument(
        "--y_clip_p",
        type=float,
        default=100.0,
        help="keep central percent on y-axis per panel (100=no clipping, 99=clip 0.5%% tails)",
    )
    ap.add_argument("--out_csv", default="", help="output csv path")
    ap.add_argument("--out_svg", default="", help="output svg path")
    ap.add_argument("--out_png", default="", help="optional output png path (auto-convert from svg)")
    args = ap.parse_args()

    ev_path = _resolve_path(args.events) if args.events else _infer_events_path(args.api)
    if not ev_path or not ev_path.exists():
        raise SystemExit("[err] cannot locate events.jsonl; pass --events explicitly")

    db_path = _resolve_path(args.db)
    meta = _token_meta(db_path)

    tok_filter = {x.strip() for x in (args.token_ids or "").split(",") if x.strip()}
    market_filter = (args.market_slug or "").strip()
    if market_filter and not meta:
        raise SystemExit("[err] --market_slug requires token_dim mapping; provide --db with token_dim table")

    now_ms = int(time.time() * 1000)
    if args.start:
        ts_from = _parse_ts_arg(args.start)
    elif args.recent_sec > 0:
        ts_from = now_ms - int(args.recent_sec * 1000)
    else:
        ts_from = 0
    if args.end:
        ts_to = _parse_ts_arg(args.end)
    else:
        ts_to = now_ms
    if ts_to < ts_from:
        raise SystemExit("[err] end < start")

    rows: List[Dict[str, Any]] = []
    with open(ev_path, "r", encoding="utf-8") as f:
        for line in f:
            try:
                o = json.loads(line)
            except Exception:
                continue
            if o.get("event") != "metrics_tick":
                continue
            ts_ms = o.get("ts_ms")
            if not isinstance(ts_ms, int) or ts_ms < ts_from or ts_ms > ts_to:
                continue
            tid = str(o.get("token_id") or "")
            if not tid:
                continue
            if tok_filter and tid not in tok_filter:
                continue

            md = meta.get(tid, {})
            market_slug = md.get("market_slug", "")
            outcome = md.get("outcome_name", "")
            if market_filter and market_slug != market_filter:
                continue

            m = o.get("metrics")
            if not isinstance(m, dict):
                continue
            depth = m.get("depth") if isinstance(m.get("depth"), dict) else {}
            d25 = depth.get("25") if isinstance(depth, dict) else None
            d25_bid = d25.get("bid_notional") if isinstance(d25, dict) else None
            d25_ask = d25.get("ask_notional") if isinstance(d25, dict) else None
            d25_total = None
            if isinstance(d25_bid, (int, float)) and isinstance(d25_ask, (int, float)):
                d25_total = float(d25_bid) + float(d25_ask)

            label = market_slug if market_slug else tid
            if outcome:
                label = f"{label}:{outcome}"

            row = {
                "ts_ms": ts_ms,
                "ts_iso": _to_iso(ts_ms),
                "run_id": o.get("run_id"),
                "source": o.get("source"),
                "reason": o.get("reason"),
                "token_id": tid,
                "market_slug": market_slug,
                "outcome_name": outcome,
                "series_label": label,
                "mid": m.get("mid"),
                "spread_bps": m.get("spread_bps"),
                "depth_25_bid_notional": d25_bid,
                "depth_25_ask_notional": d25_ask,
                "depth_25_total_notional": d25_total,
                "impact_buy_100_bps": _parse_impact_bps(m.get("slippage"), "buy", 100.0),
                "impact_sell_100_bps": _parse_impact_bps(m.get("slippage"), "sell", 100.0),
            }
            rows.append(row)

    if not rows:
        raise SystemExit("[err] no rows matched this filter window")

    rows.sort(key=lambda r: (int(r["ts_ms"]), str(r["token_id"])))

    ts = int(time.time())
    out_csv = Path(args.out_csv) if args.out_csv else Path("backend/data/reports") / f"liquidity_slice-{ts}.csv"
    out_svg = Path(args.out_svg) if args.out_svg else Path("backend/data/reports") / f"liquidity_slice-{ts}.svg"
    out_png = Path(args.out_png) if args.out_png else None
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out_svg.parent.mkdir(parents=True, exist_ok=True)
    if out_png:
        out_png.parent.mkdir(parents=True, exist_ok=True)

    fields = [
        "ts_ms",
        "ts_iso",
        "run_id",
        "source",
        "reason",
        "token_id",
        "market_slug",
        "outcome_name",
        "series_label",
        "mid",
        "spread_bps",
        "depth_25_bid_notional",
        "depth_25_ask_notional",
        "depth_25_total_notional",
        "impact_buy_100_bps",
        "impact_sell_100_bps",
    ]
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)

    title = "Liquidity Window"
    if market_filter:
        title = f"{title} | market={market_filter}"
    _write_svg(
        out_svg,
        rows,
        max_points_per_series=max(100, int(args.max_points_per_series)),
        title=title,
        split_outcomes=bool(args.split_outcomes),
        y_clip_p=float(args.y_clip_p),
    )

    tokens = len(set(str(r["token_id"]) for r in rows))
    markets = len(set(str(r["market_slug"]) for r in rows if r.get("market_slug")))
    print(f"[ok] events={ev_path}")
    print(f"[ok] rows={len(rows)} tokens={tokens} markets={markets} window={_to_iso(ts_from)}..{_to_iso(ts_to)}")
    print(f"[ok] csv={out_csv}")
    print(f"[ok] svg={out_svg}")
    if out_png:
        if _write_png_from_svg(out_svg, out_png):
            print(f"[ok] png={out_png}")
        else:
            print("[warn] png conversion failed (install cairosvg or rsvg-convert/inkscape/magick)")


if __name__ == "__main__":
    main()
