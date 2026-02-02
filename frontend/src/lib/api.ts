export const API_BASE =
  process.env.NEXT_PUBLIC_API_BASE?.replace(/\/+$/, "") ?? "http://127.0.0.1:8000";

export function apiBase() {
  return API_BASE;
}

async function http<T>(path: string, init?: RequestInit): Promise<T> {
  const url = `${API_BASE}${path}`;
  const res = await fetch(url, {
    ...init,
    headers: {
      "Content-Type": "application/json",
      ...(init?.headers ?? {})
    },
    cache: "no-store"
  });
  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`HTTP ${res.status} ${res.statusText}: ${text}`);
  }
  return (await res.json()) as T;
}

export type GammaSearchResponse = {
  events?: any[];
  markets?: any[];
  pagination?: any;
};

export type GammaMarket = {
  id?: string | number | null;
  slug?: string | null;
  question?: string | null;
  clobTokenIds?: any;
  clob_token_ids?: any;
};

export type MonitorStatus = {
  run_id: string;
  started: boolean;
  token_ids: string[];
  ws_connected: boolean;
  ws_book_msgs: number;
  ws_other_msgs: number;
  rest_polls: number;
  rebase_count: number;
  last_publish_ts_ms?: number | null;
  last_rest_poll_ts_ms?: number | null;
};

export type MetricsTick = {
  event: string;
  schema: string;
  run_id: string;
  token_id: string;
  source: string;
  reason: string;
  ts_ms: number;
  exchange_ts_ms: number;
  metrics: any;
};

// -------------------------
// Metrics helpers
// -------------------------
// Robustly pick slippage impact for side+notional across possible key formats.
// Keep this in lib to avoid any Safari/HMR oddities.
export function pickImpact(
  metrics: any,
  side: "buy" | "sell",
  notional: number
): { filled: boolean; vwap: number | null; impact_bps: number | null } {
  const sl = metrics?.slippage?.[side] ?? null;
  const filledMap = metrics?.slippage_filled?.[side] ?? null;

  if (!sl || typeof sl !== "object") {
    return { filled: false, vwap: null, impact_bps: null };
  }

  const keyA = String(notional);
  const keyB = String(Number(notional).toFixed(1));
  let row = sl[keyA] ?? sl[keyB];

  if (!row) {
    const k = Object.keys(sl).find((x) => Number(x) === notional);
    if (k) row = sl[k];
  }

  let filled: any = filledMap?.[keyA] ?? filledMap?.[keyB];
  if (filled === undefined && filledMap && typeof filledMap === "object") {
    const fk = Object.keys(filledMap).find((x) => Number(x) === notional);
    if (fk) filled = (filledMap as any)[fk];
  }

  const vwap = typeof row?.vwap === "number" ? row.vwap : null;
  const impact_bps = typeof row?.impact_bps === "number" ? row.impact_bps : null;
  return { filled: !!(filled ?? row), vwap, impact_bps };
}

// SSE endpoint for real-time ticks
export function metricsStreamUrl(token_id?: string) {
  if (token_id && token_id.length) {
    const qs = new URLSearchParams({ token_id });
    return `${API_BASE}/metrics/stream?${qs.toString()}`;
  }
  return `${API_BASE}/metrics/stream`;
}

export function fmtTs(ms?: number | null) {
  if (!ms) return "—";
  try {
    const d = new Date(ms);
    return d.toLocaleTimeString();
  } catch {
    return String(ms);
  }
}

export async function health() {
  return http<{ ok: boolean }>("/health");
}

export async function gammaSearch(q: string, limit_per_type = 10, page = 0) {
  const qs = new URLSearchParams({
    q,
    limit_per_type: String(limit_per_type),
    page: String(page)
  });
  return http<GammaSearchResponse>(`/gamma/search?${qs.toString()}`);
}

export async function gammaMarket(slug: string) {
  return http<GammaMarket>(`/gamma/market/${encodeURIComponent(slug)}`);
}

export function normalizeClobTokenIds(m: GammaMarket): string[] {
  const raw = (m.clobTokenIds ?? m.clob_token_ids ?? []) as any;
  if (!raw) return [];

  if (Array.isArray(raw) && raw.every((x) => typeof x === "string" && !x.trim().startsWith("["))) {
    return raw as string[];
  }

  if (
    Array.isArray(raw) &&
    raw.length === 1 &&
    typeof raw[0] === "string" &&
    raw[0].trim().startsWith("[")
  ) {
    try {
      const parsed = JSON.parse(raw[0]);
      if (Array.isArray(parsed)) return parsed.map(String);
    } catch {
      return [];
    }
  }

  if (Array.isArray(raw)) return raw.map(String);
  return [];
}

export async function monitorStatus() {
  return http<MonitorStatus>("/monitor/status");
}

export async function metricsTokens() {
  return http<{ token_ids: string[] }>("/metrics/tokens");
}

export async function metricsCurrent() {
  return http<Record<string, MetricsTick>>("/metrics/current");
}

export async function monitorSetTokens(token_ids: string[], restart: boolean) {
  return http<{ ok: boolean; status: MonitorStatus }>(`/monitor/set_tokens`, {
    method: "POST",
    body: JSON.stringify({ token_ids, restart })
  });
}

// -------------------------
// SSE helpers (browser only)
// -------------------------
export function openMetricsStream(opts: {
  token_id?: string;
  onTick: (tick: MetricsTick) => void;
  onHeartbeat?: () => void;
  onError?: (err: any) => void;
}): EventSource {
  const qs = new URLSearchParams();
  if (opts.token_id) qs.set("token_id", opts.token_id);
  const url = `${API_BASE}/metrics/stream${qs.toString() ? `?${qs.toString()}` : ""}`;

  const es = new EventSource(url);

  // backend sends: "event: heartbeat\ndata: {}"
  es.addEventListener("heartbeat", () => {
    opts.onHeartbeat?.();
  });

  // backend sends metrics ticks as default SSE message:
  // data: {"event":"metrics_tick", ...}
  es.onmessage = (ev) => {
    try {
      const obj = JSON.parse(ev.data);
      if (obj && obj.event === "metrics_tick") {
        opts.onTick(obj as MetricsTick);
      }
    } catch {
      // ignore parse errors
    }
  };

  es.onerror = (e) => {
    opts.onError?.(e);
  };

  return es;
}
