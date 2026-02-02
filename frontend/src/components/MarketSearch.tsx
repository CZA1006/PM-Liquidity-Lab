"use client";

import { useMemo, useState } from "react";
import Link from "next/link";
import { gammaSearch } from "@/lib/api";

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
      <div className="flex flex-col gap-3 sm:flex-row sm:items-center">
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
          <Link
            key={m.slug}
            href={`/market/${encodeURIComponent(m.slug)}`}
            className="rounded-md border border-zinc-800 bg-zinc-950/40 p-3 hover:border-zinc-700"
          >
            <div className="text-sm font-semibold">{m.question ?? m.slug}</div>
            <div className="mt-1 font-mono text-xs text-zinc-400">{m.slug}</div>
          </Link>
        ))}
      </div>
    </section>
  );
}
