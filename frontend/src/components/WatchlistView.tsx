"use client";

import Link from "next/link";
import { useEffect, useMemo, useRef, useState } from "react";
import {
  gammaMarket,
  monitorSetTokens,
  monitorStatus,
  normalizeClobTokenIds,
  openMetricsStream,
  type MetricsTick
} from "@/lib/api";
import MarketCard from "@/components/MarketCard";
import { loadWatchlist, removeFromWatchlist } from "@/lib/watchlist";

const MAX_MARKETS = 12;

type Item = {
  slug: string;
  question: string;
  tokens: string[];
};

export default function WatchlistView() {
  const [slugs, setSlugs] = useState<string[]>([]);
  const [items, setItems] = useState<Item[]>([]);
  const [tickByToken, setTickByToken] = useState<Record<string, MetricsTick>>({});
  const [status, setStatus] = useState<any | null>(null);
  const didApplyTokens = useRef(false);

  // load slugs from localStorage
  useEffect(() => {
    const xs = loadWatchlist();
    setSlugs(xs.slice(0, MAX_MARKETS));
  }, []);

  // fetch market details
  useEffect(() => {
    let cancelled = false;
    (async () => {
      const xs = slugs.slice(0, MAX_MARKETS);
      const out: Item[] = [];
      for (const slug of xs) {
        try {
          const m = await gammaMarket(slug);
          const tokens = normalizeClobTokenIds(m).slice(0, 2);
          out.push({
            slug,
            question: String(m?.question ?? slug),
            tokens
          });
        } catch {
          out.push({
            slug,
            question: slug,
            tokens: []
          });
        }
      }
      if (!cancelled) setItems(out);
    })();
    return () => {
      cancelled = true;
    };
  }, [slugs.join("|")]);

  const tokenSet = useMemo(() => {
    const xs: string[] = [];
    for (const it of items) for (const t of it.tokens) xs.push(t);
    return Array.from(new Set(xs));
  }, [items]);

  // Apply union tokens to backend monitor once (and whenever watchlist changes materially).
  useEffect(() => {
    let cancelled = false;
    (async () => {
      if (!tokenSet.length) return;
      try {
        const restart = !didApplyTokens.current;
        didApplyTokens.current = true;
        await monitorSetTokens(tokenSet, restart);
        const s = await monitorStatus();
        if (!cancelled) setStatus(s);
      } catch {
        // If backend monitor is not running, watchlist still renders using SSE ticks.
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [tokenSet.join("|")]);

  // single SSE stream, update ticks for any watched tokens
  useEffect(() => {
    if (!tokenSet.length) return;
    let alive = true;

    const es = openMetricsStream({
      onTick: (t) => {
        if (!alive) return;
        if (!tokenSet.includes(t.token_id)) return;
        setTickByToken((prev) => ({ ...prev, [t.token_id]: t }));
      }
    });

    return () => {
      alive = false;
      try {
        es.close();
      } catch {}
    };
  }, [tokenSet.join("|")]);

  function removeSlug(s: string) {
    const next = removeFromWatchlist(s);
    setSlugs(next.slice(0, MAX_MARKETS));
  }

  return (
    <section className="space-y-4">
      <div className="flex items-start justify-between">
        <div className="space-y-1">
          <Link href="/" className="text-sm underline">
            ← Back
          </Link>
          <h1 className="text-xl font-semibold">Watchlist</h1>
          <div className="text-xs text-zinc-400">saved in localStorage, max {MAX_MARKETS} markets</div>
          <div className="text-xs text-zinc-500">
            monitor: started={String(status?.started ?? "—")} ws={String(status?.ws_connected ?? "—")} token_n=
            {status?.token_ids?.length ?? tokenSet.length}
          </div>
        </div>
      </div>

      {items.length === 0 ? (
        <div className="text-sm text-zinc-400">
          No markets in watchlist yet. Use Search on the home page and click “☆ Watch” to save markets.
        </div>
      ) : (
        <div className="grid grid-cols-1 gap-3 md:grid-cols-2 xl:grid-cols-3">
          {items.map((it) => (
            <MarketCard
              key={it.slug}
              slug={it.slug}
              title={it.question}
              outcomes={["YES", "NO"]}
              tokenIds={[it.tokens[0] ?? null, it.tokens[1] ?? null]}
              ticksByToken={tickByToken}
              onRemove={() => removeSlug(it.slug)}
            />
          ))}
        </div>
      )}
    </section>
  );
}
