"use client";

import Link from "next/link";
import { useEffect, useMemo, useRef, useState } from "react";
import {
  gammaMarket,
  monitorSetTokens,
  monitorStatus,
  normalizeClobTokenIds,
  openMetricsStream,
  fmtTs,
  pickImpact,
  type MetricsTick
} from "@/lib/api";
import LineChart, { type LinePoint, type LineSeriesSpec } from "@/components/charts/LineCharts";
import { addToWatchlist, loadWatchlist, removeFromWatchlist } from "@/lib/watchlist";

type Pt = { t: number; v: number | null };

const MAX_POINTS = 600;
const HEARTBEAT_OK_MS = 20_000;

const HELP: Record<string, string> = {
  bid: "Best bid price (highest buy price) on the order book.",
  ask: "Best ask price (lowest sell price) on the order book.",
  mid: "Mid price = (best_bid + best_ask) / 2 when two-sided; otherwise falls back to best side if only one exists.",
  spread: "Spread = best_ask - best_bid when two-sided; null if one-sided.",
  spread_bps: "Spread in basis points relative to mid: spread / mid * 10,000.",
  two_sided: "Whether both bid and ask exist (true means the book is two-sided).",
  impact:
    "Impact (bps) for trading a given notional at current book: compares VWAP vs reference (usually mid or best). Higher = worse liquidity."
};

function Info({ title }: { title: string }) {
  return (
    <span
      title={title}
      className="ml-1 inline-flex h-4 w-4 cursor-help items-center justify-center rounded-full border border-zinc-700 text-[10px] text-zinc-400"
    >
      i
    </span>
  );
}

function pushHist(map: Record<string, Pt[]>, tokenId: string, t: number, v: number | null) {
  const arr = map[tokenId] ?? [];
  const next = arr.concat([{ t, v }]).slice(-MAX_POINTS);
  map[tokenId] = next;
}

function toLinePoints(xs: Pt[]): LinePoint[] {
  /**
   * lightweight-charts v5 requires:
   * - strictly increasing `time` (no duplicates)
   * - ascending order
   *
   * Our ticks are ms, but we render seconds -> many points collapse into the same second.
   * So we coalesce by second (keep the last value within that second) and then sort.
   */
  const bySec = new Map<number, number>();
  for (const p of xs) {
    if (typeof p.v !== "number") continue;
    const sec = Math.floor(p.t / 1000);
    bySec.set(sec, p.v);
  }
  const secs = Array.from(bySec.keys()).sort((a, b) => a - b);
  return secs.map((sec) => ({ time: sec as LinePoint["time"], value: bySec.get(sec)! }));
}

export default function MarketView({ slug }: { slug: string }) {
  const [loading, setLoading] = useState(true);
  const [err, setErr] = useState<string | null>(null);

  const [market, setMarket] = useState<any | null>(null);
  const [status, setStatus] = useState<any | null>(null);
  const [busy, setBusy] = useState(false);
  const [watched, setWatched] = useState<boolean>(false);

  useEffect(() => {
    const wl = loadWatchlist();
    setWatched(wl.includes(slug));
  }, [slug]);

  const [tickByToken, setTickByToken] = useState<Record<string, MetricsTick>>({});
  const [lastHeartbeat, setLastHeartbeat] = useState<number | null>(null);
  const [lastTickTs, setLastTickTs] = useState<number | null>(null);

  const histMid = useRef<Record<string, Pt[]>>({});
  const histSpreadBps = useRef<Record<string, Pt[]>>({});
  const histImpactBuy100 = useRef<Record<string, Pt[]>>({});
  const histImpactSell100 = useRef<Record<string, Pt[]>>({});

  const tokens = useMemo(() => (market ? normalizeClobTokenIds(market) : []), [market]);

  async function refreshStatus() {
    const s = await monitorStatus();
    setStatus(s);
  }

  useEffect(() => {
    let cancelled = false;
    (async () => {
      setLoading(true);
      setErr(null);
      setMarket(null);
      setStatus(null);
      try {
        const m = await gammaMarket(slug);
        if (!cancelled) setMarket(m);
        const s = await monitorStatus();
        if (!cancelled) setStatus(s);
      } catch (e: any) {
        if (!cancelled) setErr(e?.message ?? String(e));
      } finally {
        if (!cancelled) setLoading(false);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [slug]);

  // SSE stream: single connection, filter by current market tokens
  useEffect(() => {
    if (!tokens.length) return;

    let alive = true;
    const es = openMetricsStream({
      onHeartbeat: () => {
        if (!alive) return;
        setLastHeartbeat(Date.now());
      },
      onTick: (t) => {
        if (!alive) return;
        if (!tokens.includes(t.token_id)) return;

        setTickByToken((prev) => ({ ...prev, [t.token_id]: t }));
        setLastTickTs(t.ts_ms ?? Date.now());

        const now = t.ts_ms ?? Date.now();
        const m = t.metrics ?? {};

        const mid = typeof m.mid === "number" ? m.mid : null;
        const spreadBps = typeof m.spread_bps === "number" ? m.spread_bps : null;
        const ib = pickImpact(m, "buy", 100).impact_bps;
        const is = pickImpact(m, "sell", 100).impact_bps;

        pushHist(histMid.current, t.token_id, now, mid);
        pushHist(histSpreadBps.current, t.token_id, now, spreadBps);
        pushHist(histImpactBuy100.current, t.token_id, now, typeof ib === "number" ? ib : null);
        pushHist(histImpactSell100.current, t.token_id, now, typeof is === "number" ? is : null);
      },
      onError: () => {
        // EventSource will auto-reconnect; we just mark heartbeat stale.
      }
    });

    return () => {
      alive = false;
      try {
        es.close();
      } catch {}
    };
  }, [tokens.join("|")]);

  async function applyTokens(restart: boolean) {
    setBusy(true);
    setErr(null);
    try {
      if (!tokens.length) throw new Error("clobTokenIds is empty for this market.");
      await monitorSetTokens(tokens, restart);
      await refreshStatus();
    } catch (e: any) {
      setErr(e?.message ?? String(e));
    } finally {
      setBusy(false);
    }
  }

  function toggleWatchlist() {
    if (watched) {
      removeFromWatchlist(slug);
      setWatched(false);
    } else {
      addToWatchlist(slug);
      setWatched(true);
    }
  }

  if (loading) return <div className="text-sm text-zinc-300">Loading market...</div>;
  if (err) {
    return (
      <div className="space-y-3">
        <div className="text-sm text-red-300">Error: {err}</div>
        <Link href="/" className="text-sm underline">
          ← Back
        </Link>
      </div>
    );
  }

  const outcomes: string[] =
    Array.isArray(market?.outcomes) && market.outcomes.length >= 2
      ? market.outcomes.slice(0, 2).map(String)
      : ["YES", "NO"];

  const liveConnected = lastHeartbeat ? Date.now() - lastHeartbeat < HEARTBEAT_OK_MS : false;

  const yesToken = tokens[0] ?? null;
  const noToken = tokens[1] ?? null;

  function sideCard(label: string, tokenId: string | null) {
    if (!tokenId) {
      return (
        <div className="rounded-lg border border-zinc-800 bg-zinc-950/40 p-4">
          <div className="text-sm font-semibold">{label}</div>
          <div className="mt-2 text-sm text-zinc-400">No token</div>
        </div>
      );
    }

    const tick = tickByToken[tokenId];
    const m = tick?.metrics ?? {};
    const best = m.best ?? {};

    const bid = typeof best.bid === "number" ? best.bid : null;
    const ask = typeof best.ask === "number" ? best.ask : null;
    const mid = typeof m.mid === "number" ? m.mid : null;
    const spread = typeof m.spread === "number" ? m.spread : null;
    const spreadBps = typeof m.spread_bps === "number" ? m.spread_bps : null;
    const twoSided = !!m.two_sided;

    const ib100 = pickImpact(m, "buy", 100);
    const is100 = pickImpact(m, "sell", 100);

    return (
      <div className="rounded-lg border border-zinc-800 bg-zinc-950/40 p-4">
        <div className="flex items-center justify-between">
          <div className="text-sm font-semibold">
            {label}
            <span className="ml-2 text-xs text-zinc-500" title={`token_id: ${tokenId}`}>
              (hover for token)
            </span>
          </div>
          <div className="text-xs text-zinc-500" title={`token_id: ${tokenId}`}>
            {tokenId.slice(0, 8)}…{tokenId.slice(-6)}
          </div>
        </div>

        <div className="mt-3 grid grid-cols-2 gap-3 text-sm">
          <div>
            bid: <span className="font-mono">{bid ?? "null"}</span>
            <Info title={HELP.bid} />
          </div>
          <div>
            ask: <span className="font-mono">{ask ?? "null"}</span>
            <Info title={HELP.ask} />
          </div>
          <div>
            mid: <span className="font-mono">{mid ?? "null"}</span>
            <Info title={HELP.mid} />
          </div>
          <div>
            spread: <span className="font-mono">{spread ?? "null"}</span>
            <Info title={HELP.spread} />
          </div>
          <div>
            spread_bps: <span className="font-mono">{spreadBps ?? "null"}</span>
            <Info title={HELP.spread_bps} />
          </div>
          <div>
            two_sided: <span className="font-mono">{String(twoSided)}</span>
            <Info title={HELP.two_sided} />
          </div>
        </div>

        <div className="mt-3 text-xs text-zinc-400">
          impact_bps@100 buy: <span className="font-mono">{ib100.impact_bps ?? "null"}</span>
          <Info title={HELP.impact} />
          {" | "}sell: <span className="font-mono">{is100.impact_bps ?? "null"}</span>
          <Info title={HELP.impact} />
        </div>

        <div className="mt-3 text-xs text-zinc-500">
          last tick: {fmtTs(tick?.ts_ms ?? null)} (exchange: {fmtTs(tick?.exchange_ts_ms ?? null)})
        </div>
      </div>
    );
  }

  // Build 3 main charts across YES/NO (two lines per chart where possible)
  const midSeries: LineSeriesSpec[] = [];
  const spreadSeries: LineSeriesSpec[] = [];
  const impactSeries: LineSeriesSpec[] = [];

  if (yesToken) {
    midSeries.push({ id: "yes_mid", name: `${outcomes[0]} mid`, data: toLinePoints(histMid.current[yesToken] ?? []) });
    spreadSeries.push({
      id: "yes_spread",
      name: `${outcomes[0]} spread_bps`,
      data: toLinePoints(histSpreadBps.current[yesToken] ?? [])
    });
    impactSeries.push({
      id: "yes_impact_buy",
      name: `${outcomes[0]} buy@100`,
      data: toLinePoints(histImpactBuy100.current[yesToken] ?? [])
    });
    impactSeries.push({
      id: "yes_impact_sell",
      name: `${outcomes[0]} sell@100`,
      data: toLinePoints(histImpactSell100.current[yesToken] ?? [])
    });
  }
  if (noToken) {
    midSeries.push({ id: "no_mid", name: `${outcomes[1]} mid`, data: toLinePoints(histMid.current[noToken] ?? []) });
    spreadSeries.push({
      id: "no_spread",
      name: `${outcomes[1]} spread_bps`,
      data: toLinePoints(histSpreadBps.current[noToken] ?? [])
    });
    impactSeries.push({
      id: "no_impact_buy",
      name: `${outcomes[1]} buy@100`,
      data: toLinePoints(histImpactBuy100.current[noToken] ?? [])
    });
    impactSeries.push({
      id: "no_impact_sell",
      name: `${outcomes[1]} sell@100`,
      data: toLinePoints(histImpactSell100.current[noToken] ?? [])
    });
  }

  return (
    <section className="space-y-4">
      <div className="flex items-start justify-between gap-4">
        <div className="space-y-1">
          <Link href="/" className="text-sm underline">
            ← Back
          </Link>
          <h1 className="text-xl font-semibold leading-snug">{market?.question ?? slug}</h1>
          <div className="font-mono text-xs text-zinc-400">slug: {slug}</div>
          <div className="text-xs text-zinc-400">
            live stream:{" "}
            <span className={liveConnected ? "text-emerald-400" : "text-red-300"}>
              {liveConnected ? "connected" : "disconnected"}
            </span>
            {"  "}last tick: {fmtTs(lastTickTs)}
          </div>
        </div>

        <div className="flex flex-col items-end gap-2">
          <div className="flex gap-2">
            <button
              onClick={toggleWatchlist}
              className="rounded-md border border-zinc-600 bg-zinc-950 px-3 py-2 text-sm text-zinc-100 hover:border-zinc-500"
              title={watched ? "Remove from watchlist" : "Add to watchlist"}
            >
              {watched ? "★ Watched" : "☆ Watch"}
            </button>
            <Link
              href="/watchlist"
              className="rounded-md border border-zinc-600 bg-zinc-950 px-3 py-2 text-sm text-zinc-100 hover:border-zinc-500"
            >
              Watchlist
            </Link>
          </div>
          <button
            onClick={() => applyTokens(true)}
            disabled={busy || tokens.length === 0}
            className="rounded-md bg-zinc-100 px-3 py-2 text-sm text-zinc-900 disabled:opacity-50"
          >
            Monitor tokens (restart)
          </button>
          <button
            onClick={() => applyTokens(false)}
            disabled={busy || tokens.length === 0}
            className="rounded-md border border-zinc-600 px-3 py-2 text-sm text-zinc-100 disabled:opacity-50"
          >
            Monitor tokens (no restart)
          </button>
          <div className="text-xs text-zinc-500">
            monitor: started={String(status?.started)} ws={String(status?.ws_connected)} token_n=
            {status?.token_ids?.length ?? tokens.length}
          </div>
        </div>
      </div>

      <div className="grid grid-cols-1 gap-4 md:grid-cols-2">
        {sideCard(outcomes[0] ?? "YES", yesToken)}
        {sideCard(outcomes[1] ?? "NO", noToken)}
      </div>

      <div className="grid grid-cols-1 gap-4">
        <LineChart title="mid (YES/NO)" series={midSeries} valueFormatter={(v) => v.toFixed(6)} />
        <LineChart title="spread_bps (YES/NO)" series={spreadSeries} valueFormatter={(v) => v.toFixed(2)} />
        <LineChart title="impact_bps @ notional=100 (buy/sell)" series={impactSeries} valueFormatter={(v) => v.toFixed(2)} />
      </div>

      <details className="rounded-lg border border-zinc-800 bg-zinc-950/40 p-4">
        <summary className="cursor-pointer text-sm font-semibold text-zinc-200">
          指标解释 / Metric glossary
        </summary>
        <div className="mt-3 grid grid-cols-1 gap-2 text-sm text-zinc-300 md:grid-cols-2">
          <div>
            <div className="font-semibold">mid</div>
            <div className="text-xs text-zinc-400">{HELP.mid}</div>
          </div>
          <div>
            <div className="font-semibold">spread_bps</div>
            <div className="text-xs text-zinc-400">{HELP.spread_bps}</div>
          </div>
          <div>
            <div className="font-semibold">impact_bps@100</div>
            <div className="text-xs text-zinc-400">{HELP.impact}</div>
          </div>
          <div>
            <div className="font-semibold">two_sided</div>
            <div className="text-xs text-zinc-400">{HELP.two_sided}</div>
          </div>
        </div>
      </details>

      <details className="rounded-lg border border-zinc-800 bg-zinc-950/40 p-4">
        <summary className="cursor-pointer text-sm font-semibold text-zinc-200">
          Debug (tokens / raw monitor status)
        </summary>
        <div className="mt-3 text-xs text-zinc-400">
          tokens: {tokens.length ? tokens.join(", ") : "—"}
        </div>
        <pre className="mt-2 overflow-auto rounded bg-zinc-950 p-3 text-xs text-zinc-300">
{JSON.stringify(
  {
    started: status?.started,
    ws_connected: status?.ws_connected,
    token_n: status?.token_ids?.length,
    token_ids: status?.token_ids,
    ws_book_msgs: status?.ws_book_msgs,
    ws_other_msgs: status?.ws_other_msgs,
    rest_polls: status?.rest_polls,
    rebase_count: status?.rebase_count
  },
  null,
  2
)}
        </pre>
      </details>
    </section>
  );
}
