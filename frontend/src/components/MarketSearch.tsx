"use client";

import { useMemo, useState } from "react";
import Link from "next/link";
import { gammaSearch } from "@/lib/api";
import { addToWatchlist, loadWatchlist, removeFromWatchlist } from "@/lib/watchlist";

type MarketItem = {
  slug: string;
  question?: string;
};

function extractMarkets(resp: any): MarketItem[] {
  if (Array.isArray(resp?.markets)) {
    return resp.markets
      .map((m: any) => ({ slug: m?.slug, question: m?.question }))
      .filter((x: any) => typeof x.slug === "string" && x.slug.length > 0);
  }

  const out: MarketItem[] = [];
  for (const e of resp?.events ?? []) {
    for (const m of e?.markets ?? []) {
      if (m?.slug) out.push({ slug: String(m.slug), question: m?.question });
    }
  }
  return out;
}

export default function MarketSearch() {
  const [q, setQ] = useState("ethereum");
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [resp, setResp] = useState<any | null>(null);
  const [wl, setWl] = useState<string[]>(() => loadWatchlist());

  const markets = useMemo(() => (resp ? extractMarkets(resp) : []), [resp]);

  async function run() {
    setLoading(true);
    setErr(null);
    try {
      const r = await gammaSearch(q, 10, 0);
      setResp(r);
    } catch (e: any) {
      setErr(e?.message ?? String(e));
    } finally {
      setLoading(false);
    }
  }

  return (
    <section className="rounded-lg border border-zinc-800 bg-zinc-900/40 p-4">
      <div className="flex items-baseline justify-between gap-3">
        <h1 className="text-xl font-semibold">PM Liquidity Lab</h1>
        <div className="flex items-center gap-3 text-sm">
          <Link href="/watchlist" className="underline">
            Watchlist
          </Link>
          <div className="text-xs text-zinc-400">watched: {wl.length}</div>
        </div>
      </div>

      <div className="mt-4 flex flex-col gap-3 sm:flex-row sm:items-center">
        <input
          value={q}
          onChange={(e) => setQ(e.target.value)}
          className="w-full rounded-md border border-zinc-700 bg-zinc-950 px-3 py-2 text-sm outline-none focus:border-zinc-500"
          placeholder="keyword e.g. ethereum"
        />
        <button
          onClick={run}
          className="rounded-md bg-blue-600 px-4 py-2 text-sm font-semibold hover:bg-blue-500 disabled:opacity-50"
          disabled={loading || !q.trim()}
        >
          {loading ? "Searching..." : "Search"}
        </button>
      </div>

      {err && <div className="mt-3 text-sm text-red-300">Error: {err}</div>}

      <div className="mt-4 text-sm text-zinc-300">
        Results: <span className="font-mono">{markets.length}</span>
      </div>

      <div className="mt-3 grid grid-cols-1 gap-3">
        {markets.slice(0, 20).map((m) => (
          <div
            key={m.slug}
            className="flex items-start justify-between gap-3 rounded-md border border-zinc-800 bg-zinc-950/40 p-3 hover:border-zinc-700"
          >
            <Link className="min-w-0 flex-1" href={`/market/${encodeURIComponent(m.slug)}`}>
              <div className="text-sm font-semibold">{m.question ?? m.slug}</div>
              <div className="mt-1 font-mono text-xs text-zinc-400 break-all">{m.slug}</div>
            </Link>

            {wl.includes(m.slug) ? (
              <button
                className="shrink-0 rounded border border-zinc-700 bg-zinc-950 px-2 py-1 text-xs text-zinc-200 hover:border-zinc-600"
                onClick={() => setWl(removeFromWatchlist(m.slug))}
                title="Remove from watchlist"
              >
                ★ Watched
              </button>
            ) : (
              <button
                className="shrink-0 rounded border border-zinc-700 bg-zinc-950 px-2 py-1 text-xs text-zinc-200 hover:border-zinc-600"
                onClick={() => setWl(addToWatchlist(m.slug))}
                title="Add to watchlist"
              >
                ☆ Watch
              </button>
            )}
          </div>
        ))}
      </div>
    </section>
  );
}
