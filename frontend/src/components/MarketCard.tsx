"use client";

import Link from "next/link";
import { useMemo } from "react";
import type { MetricsTick } from "@/lib/api";

type Props = {
  slug: string;
  title: string;
  outcomes: [string, string];
  tokenIds: [string | null, string | null];
  ticksByToken: Record<string, MetricsTick | undefined>;
  onRemove?: () => void;
};

function fmtNum(x: unknown, dp: number) {
  return typeof x === "number" && Number.isFinite(x) ? x.toFixed(dp) : "—";
}

function pickMid(t?: MetricsTick) {
  const m = t?.metrics;
  return typeof m?.mid === "number" ? m.mid : null;
}

function pickSpreadBps(t?: MetricsTick) {
  const m = t?.metrics;
  return typeof m?.spread_bps === "number" ? m.spread_bps : null;
}

function pickImpactBps(t?: MetricsTick, side: "buy" | "sell") {
  const m = t?.metrics;
  const sl = m?.slippage?.[side];
  if (!sl || typeof sl !== "object") return null;
  const row = (sl["100"] ?? sl["100.0"]) as any;
  return typeof row?.impact_bps === "number" ? row.impact_bps : null;
}

export default function MarketCard(props: Props) {
  const { slug, title, outcomes, tokenIds, ticksByToken, onRemove } = props;
  const [yesId, noId] = tokenIds;

  const ty = yesId ? ticksByToken[yesId] : undefined;
  const tn = noId ? ticksByToken[noId] : undefined;

  const yes = useMemo(() => {
    return {
      mid: pickMid(ty),
      spread_bps: pickSpreadBps(ty),
      impact_buy: pickImpactBps(ty, "buy"),
      impact_sell: pickImpactBps(ty, "sell")
    };
  }, [ty]);

  const no = useMemo(() => {
    return {
      mid: pickMid(tn),
      spread_bps: pickSpreadBps(tn),
      impact_buy: pickImpactBps(tn, "buy"),
      impact_sell: pickImpactBps(tn, "sell")
    };
  }, [tn]);

  return (
    <div className="rounded-lg border border-zinc-800 bg-zinc-950/40 p-4">
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0">
          <Link className="text-sm font-semibold underline" href={`/market/${encodeURIComponent(slug)}`}>
            {title}
          </Link>
          <div className="mt-1 font-mono text-xs text-zinc-500 break-all">{slug}</div>
        </div>
        {onRemove ? (
          <button className="shrink-0 text-xs text-zinc-400 underline" onClick={onRemove}>
            remove
          </button>
        ) : null}
      </div>

      <div className="mt-3 grid grid-cols-2 gap-3 text-sm">
        <div className="rounded border border-zinc-800 p-3">
          <div className="text-xs text-zinc-400">{outcomes[0]}</div>
          <div className="mt-1">
            mid: <span className="font-mono">{fmtNum(yes.mid, 6)}</span>
          </div>
          <div className="mt-1">
            spread_bps: <span className="font-mono">{fmtNum(yes.spread_bps, 2)}</span>
          </div>
          <div className="mt-1 text-xs text-zinc-500">
            impact@100: buy <span className="font-mono">{fmtNum(yes.impact_buy, 2)}</span> / sell{" "}
            <span className="font-mono">{fmtNum(yes.impact_sell, 2)}</span>
          </div>
          {yesId ? (
            <div className="mt-2 text-[10px] text-zinc-500" title={yesId}>
              token: {yesId.slice(0, 8)}…{yesId.slice(-6)}
            </div>
          ) : null}
        </div>

        <div className="rounded border border-zinc-800 p-3">
          <div className="text-xs text-zinc-400">{outcomes[1]}</div>
          <div className="mt-1">
            mid: <span className="font-mono">{fmtNum(no.mid, 6)}</span>
          </div>
          <div className="mt-1">
            spread_bps: <span className="font-mono">{fmtNum(no.spread_bps, 2)}</span>
          </div>
          <div className="mt-1 text-xs text-zinc-500">
            impact@100: buy <span className="font-mono">{fmtNum(no.impact_buy, 2)}</span> / sell{" "}
            <span className="font-mono">{fmtNum(no.impact_sell, 2)}</span>
          </div>
          {noId ? (
            <div className="mt-2 text-[10px] text-zinc-500" title={noId}>
              token: {noId.slice(0, 8)}…{noId.slice(-6)}
            </div>
          ) : null}
        </div>
      </div>

      <div className="mt-3 text-xs text-zinc-500">
        ticks: {yesId && ty?.ts_ms ? "YES✅" : "YES—"} / {noId && tn?.ts_ms ? "NO✅" : "NO—"}
      </div>
    </div>
  );
}
