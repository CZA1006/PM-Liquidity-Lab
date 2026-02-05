"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import {
  createChart,
  LineSeries,
  LineStyle,
  type IChartApi,
  type ISeriesApi,
  type UTCTimestamp,
  type LineData,
  type WhitespaceData,
  type MouseEventParams
} from "lightweight-charts";

export type LinePoint = { time: UTCTimestamp; value: number };

export type LineSeriesStyle = {
  color?: string;
  lineWidth?: number;
  lineStyle?: LineStyle;
};

export type LineSeriesSpec = {
  id: string;
  name: string;
  data: LinePoint[];
  style?: LineSeriesStyle;
};

function normalizeLineData(data: LinePoint[]): LinePoint[] {
  /**
   * lightweight-charts v5 asserts:
   * - data must be strictly asc ordered by time
   * - duplicate timestamps are NOT allowed
   *
   * We coalesce duplicates (keep last) and sort ascending.
   */
  const byTime = new Map<number, number>();
  for (const p of data) {
    byTime.set(p.time as number, p.value);
  }
  const times = Array.from(byTime.keys()).sort((a, b) => a - b);
  return times.map((t) => ({ time: t as UTCTimestamp, value: byTime.get(t)! }));
}

function inferStyle(name: string): Required<LineSeriesStyle> {
  // Heuristic:
  // - YES / NO use different colors
  // - buy = solid, sell = dashed
  const upper = name.toUpperCase();

  const isYes = upper.includes("YES");
  const isNo = upper.includes("NO");
  const isSell = upper.includes("SELL");

  const color = isYes
    ? "#22c55e"
    : isNo
      ? "#60a5fa"
      : "#a78bfa";

  const lineStyle = isSell ? LineStyle.Dashed : LineStyle.Solid;
  const lineWidth = 2;
  return { color, lineStyle, lineWidth };
}

function inferColor(spec: LineSeriesSpec): string {
  if (spec.style?.color) return spec.style.color;
  return inferStyle(spec.name).color;
}

export default function LineChart(props: {
  title: string;
  height?: number;
  series: LineSeriesSpec[];
  valueFormatter?: (v: number) => string;
}) {
  const { title, series, height = 240, valueFormatter } = props;

  const boxRef = useRef<HTMLDivElement | null>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const seriesRef = useRef<Record<string, ISeriesApi<"Line">>>({});
  const latestSeriesRef = useRef<LineSeriesSpec[]>(series);
  const [hoverText, setHoverText] = useState<string>("—");

  const fmt = useMemo(() => {
    return valueFormatter ?? ((v: number) => String(v));
  }, [valueFormatter]);

  useEffect(() => {
    latestSeriesRef.current = series;
  }, [series]);

  useEffect(() => {
    if (!boxRef.current) return;

    const chart = createChart(boxRef.current, {
      width: boxRef.current.clientWidth,
      height,
      layout: {
        background: { color: "transparent" },
        textColor: "#cbd5e1"
      },
      grid: {
        vertLines: { color: "rgba(148,163,184,0.08)" },
        horzLines: { color: "rgba(148,163,184,0.08)" }
      },
      rightPriceScale: { borderVisible: false },
      timeScale: {
        borderVisible: false,
        timeVisible: true,
        secondsVisible: true
      },
      crosshair: {
        mode: 1 // normal
      }
    });

    chartRef.current = chart;

    chart.timeScale().fitContent();

    // hover handler: show values for all series at crosshair
    chart.subscribeCrosshairMove((param: MouseEventParams) => {
      if (!param.time) {
        setHoverText("—");
        return;
      }
      const parts: string[] = [];
      for (const s of latestSeriesRef.current) {
        const ls = seriesRef.current[s.id];
        if (!ls) continue;
        const row = param.seriesData.get(ls) as LineData | WhitespaceData | undefined;
        const v = row && "value" in row ? row.value : undefined;
        if (typeof v === "number") parts.push(`${s.name}: ${fmt(v)}`);
      }
      setHoverText(parts.length ? parts.join(" | ") : "—");
    });

    // resize
    const ro = new ResizeObserver(() => {
      if (!boxRef.current || !chartRef.current) return;
      const w = boxRef.current.clientWidth;
      chartRef.current.applyOptions({ width: w });
    });
    ro.observe(boxRef.current);

    return () => {
      ro.disconnect();
      chart.remove();
      chartRef.current = null;
      seriesRef.current = {};
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // update data when series change
  useEffect(() => {
    if (!chartRef.current) return;

    const chart = chartRef.current;

    // remove series that no longer exist
    const nextIds = new Set(series.map((s) => s.id));
    for (const id of Object.keys(seriesRef.current)) {
      if (!nextIds.has(id)) {
        const sApi = seriesRef.current[id];
        chart.removeSeries(sApi);
        delete seriesRef.current[id];
      }
    }

    // create missing / update existing (v5 style)
    for (const s of series) {
      if (!seriesRef.current[s.id]) {
        const base = inferStyle(s.name);
        const opt: LineSeriesStyle = { ...base, ...(s.style ?? {}) };
        const ls: ISeriesApi<"Line"> = chart.addSeries(LineSeries, {
          lineWidth: opt.lineWidth ?? 2,
          lineStyle: opt.lineStyle ?? LineStyle.Solid,
          color: opt.color
        });
        seriesRef.current[s.id] = ls;
      } else {
        const base = inferStyle(s.name);
        const opt: LineSeriesStyle = { ...base, ...(s.style ?? {}) };
        seriesRef.current[s.id].applyOptions({
          lineWidth: opt.lineWidth ?? 2,
          lineStyle: opt.lineStyle ?? LineStyle.Solid,
          color: opt.color
        });
      }
      seriesRef.current[s.id].setData(normalizeLineData(s.data));
    }

    chart.timeScale().fitContent();
  }, [series]);

  return (
    <div className="rounded-lg border border-zinc-800 bg-zinc-950/40 p-3">
      <div className="flex items-baseline justify-between gap-3">
        <div className="text-sm font-semibold text-zinc-200">{title}</div>
        <div className="text-xs text-zinc-400">{hoverText}</div>
      </div>
      <div className="mt-2 flex flex-wrap gap-2">
        {series.map((s) => (
          <div
            key={s.id}
            className="inline-flex items-center gap-2 rounded border border-zinc-800 bg-zinc-950/30 px-2 py-1 text-xs text-zinc-300"
            title={s.name}
          >
            <span
              className="h-2 w-2 rounded-full"
              style={{ background: inferColor(s) }}
            />
            <span className="max-w-[18rem] truncate">{s.name}</span>
          </div>
        ))}
      </div>
      <div ref={boxRef} className="mt-2 w-full" />
    </div>
  );
}
