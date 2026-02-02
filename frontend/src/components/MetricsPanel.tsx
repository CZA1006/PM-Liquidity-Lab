"use client";

import { useEffect, useMemo, useState } from "react";
import { metricsCurrent, type MetricsTick } from "@/lib/api";
import { openMetricsStream } from "@/lib/sse";

export default function MetricsPanel({ tokenIds }: { tokenIds: string[] }) {
  const [current, setCurrent] = useState<Record<string, MetricsTick>>({});
  const [selected, setSelected] = useState<string>("");
  const [lastMsgTs, setLastMsgTs] = useState<number | null>(null);
  const [err, setErr] = useState<string | null>(null);

  const effectiveTokens = useMemo(
    () => (Array.isArray(tokenIds) ? tokenIds.filter(Boolean) : []),
    [tokenIds]
  );

  useEffect(() => {
    if (!selected && effectiveTokens.length) setSelected(effectiveTokens[0]);
  }, [effectiveTokens, selected]);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const m = await metricsCurrent();
        if (!cancelled) setCurrent(m);
      } catch (e: any) {
        if (!cancelled) setErr(e?.message ?? String(e));
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [effectiveTokens.join("|")]);

  useEffect(() => {
    if (!selected) return;
    setErr(null);
    const close = openMetricsStream(selected, {
      onTick: (tick) => {
        setLastMsgTs(Date.now());
        setCurrent((prev) => ({ ...prev, [tick.token_id]: tick }));
      },
      onError: () => {
        setErr("SSE connection error (auto-retrying)...");
      }
    });
    return () => close();
  }, [selected]);

  const tick = selected ? current[selected] : null;
  const metrics = tick?.metrics ?? null;
  const stale = lastMsgTs ? Date.now() - lastMsgTs > 15000 : false;

  return (
    <section className="rounded-lg border border-zinc-800 bg-zinc-900/40 p-4">
      <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <div className="text-sm font-semibold">Metrics stream</div>
          <div className="text-xs text-zinc-400">
            token_id filter -> <span className="font-mono">{selected || "(none)"}</span>{" "}
            {stale ? <span className="ml-2 text-amber-300">(stale)</span> : null}
          </div>
        </div>
        <select
          value={selected}
          onChange={(e) => setSelected(e.target.value)}
          className="rounded-md border border-zinc-700 bg-zinc-950 px-3 py-2 text-xs outline-none"
        >
          {effectiveTokens.map((t) => (
            <option key={t} value={t}>
              {t}
            </option>
          ))}
        </select>
      </div>

      {err && <div className="mt-3 text-xs text-amber-300">{err}</div>}

      <div className="mt-4 grid grid-cols-1 gap-3 sm:grid-cols-2">
        <div className="rounded border border-zinc-800 bg-zinc-950/40 p-3">
          <div className="text-xs text-zinc-400">Last tick</div>
          <pre className="mt-2 overflow-auto text-xs text-zinc-200">
{tick
  ? JSON.stringify(
      {
        ts_ms: tick.ts_ms,
        exchange_ts_ms: tick.exchange_ts_ms,
        source: tick.source,
        reason: tick.reason,
        best: metrics?.best,
        mid: metrics?.mid,
        spread_bps: metrics?.spread_bps,
        two_sided: metrics?.two_sided
      },
      null,
      2
    )
  : "(no tick yet)"}
          </pre>
        </div>

        <div className="rounded border border-zinc-800 bg-zinc-950/40 p-3">
          <div className="text-xs text-zinc-400">Raw metrics</div>
          <pre className="mt-2 max-h-64 overflow-auto text-xs text-zinc-200">
{metrics ? JSON.stringify(metrics, null, 2) : "(no metrics)"} 
          </pre>
        </div>
      </div>
    </section>
  );
}
