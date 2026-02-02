"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";
import {
  gammaMarket,
  normalizeClobTokenIds,
  openMetricsStream,
  type MetricsTick
} from "@/lib/api";

const LS_KEY = "pmliq_watchlist_slugs";
const MAX_MARKETS = 12;

type CardMarket = {
  slug: string;
  question: string;
  tokens: string[];
};

export default function WatchlistView() {
  const [slugs, setSlugs] = useState<string[]>([]);
  const [items, setItems] = useState<CardMarket[]>([]);
  const [tickByToken, setTickByToken] = useState<Record<string, MetricsTick>>({});

  // load slugs from localStorage
  useEffect(() => {
    try {
      const raw = localStorage.getItem(LS_KEY);
      const arr = raw ? (JSON.parse(raw) as any[]) : [];
      const xs = Array.isArray(arr) ? arr.map(String).filter(Boolean) : [];
      setSlugs(xs.slice(0, MAX_MARKETS));
    } catch {
      setSlugs([]);
    }
  }, []);

  // fetch market details
  useEffect(() => {
    let cancelled = false;
    (async () => {
      const xs = slugs.slice(0, MAX_MARKETS);
      const out: CardMarket[] = [];
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
    const next = slugs.filter((x) => x !== s);
    setSlugs(next);
    localStorage.setItem(LS_KEY, JSON.stringify(next));
  }

  return (
    <section className="space-y-4">
      <div className="flex items-start justify-between">
        <div className="space-y-1">
          <Link href="/" className="text-sm underline">
            ← Back
          </Link>
          <h1 className="text-xl font-semibold">Watchlist</h1>
          <div className="text-xs text-zinc-400">
            saved in localStorage (“{LS_KEY}”), max {MAX_MARKETS} markets
          </div>
        </div>
      </div>

      {items.length === 0 ? (
        <div className="text-sm text-zinc-400">
          No markets in watchlist yet. Open a market page and add its slug into localStorage key "{LS_KEY}".
        </div>
      ) : (
        <div className="grid grid-cols-1 gap-3 md:grid-cols-2 xl:grid-cols-3">
          {items.map((it) => {
            const yes = it.tokens[0];
            const no = it.tokens[1];
            const ty = yes ? tickByToken[yes]?.metrics ?? {} : {};
            const tn = no ? tickByToken[no]?.metrics ?? {} : {};

            const ymid = typeof ty?.mid === "number" ? ty.mid : null;
            const nmid = typeof tn?.mid === "number" ? tn.mid : null;
            const ysp = typeof ty?.spread_bps === "number" ? ty.spread_bps : null;
            const nsp = typeof tn?.spread_bps === "number" ? tn.spread_bps : null;

            return (
              <div key={it.slug} className="rounded-lg border border-zinc-800 bg-zinc-950/40 p-4">
                <div className="flex items-start justify-between gap-3">
                  <div className="min-w-0">
                    <Link className="text-sm font-semibold underline" href={`/market/${encodeURIComponent(it.slug)}`}>
                      {it.question}
                    </Link>
                    <div className="mt-1 font-mono text-xs text-zinc-500 break-all">{it.slug}</div>
                  </div>
                  <button
                    className="text-xs text-zinc-400 underline"
                    onClick={() => removeSlug(it.slug)}
                  >
                    remove
                  </button>
                </div>

                <div className="mt-3 grid grid-cols-2 gap-3 text-sm">
                  <div className="rounded border border-zinc-800 p-3">
                    <div className="text-xs text-zinc-400">YES</div>
                    <div className="mt-1">mid: <span className="font-mono">{ymid ?? "—"}</span></div>
                    <div className="mt-1">spread_bps: <span className="font-mono">{ysp ?? "—"}</span></div>
                    {yes ? (
                      <div className="mt-2 text-[10px] text-zinc-500" title={yes}>
                        token: {yes.slice(0, 8)}…{yes.slice(-6)}
                      </div>
                    ) : null}
                  </div>

                  <div className="rounded border border-zinc-800 p-3">
                    <div className="text-xs text-zinc-400">NO</div>
                    <div className="mt-1">mid: <span className="font-mono">{nmid ?? "—"}</span></div>
                    <div className="mt-1">spread_bps: <span className="font-mono">{nsp ?? "—"}</span></div>
                    {no ? (
                      <div className="mt-2 text-[10px] text-zinc-500" title={no}>
                        token: {no.slice(0, 8)}…{no.slice(-6)}
                      </div>
                    ) : null}
                  </div>
                </div>

                <div className="mt-3 text-xs text-zinc-500">
                  ticks: {yes && tickByToken[yes]?.ts_ms ? "YES✅" : "YES—"} / {no && tickByToken[no]?.ts_ms ? "NO✅" : "NO—"}
                </div>
              </div>
            );
          })}
        </div>
      )}
    </section>
  );
}