import { API_BASE, type MetricsTick } from "@/lib/api";

export type MetricsStreamHandlers = {
  onTick: (tick: MetricsTick) => void;
  onHeartbeat?: () => void;
  onError?: (err: any) => void;
};

export function openMetricsStream(token_id?: string, handlers?: MetricsStreamHandlers) {
  const qs = token_id ? `?token_id=${encodeURIComponent(token_id)}` : "";
  const url = `${API_BASE}/metrics/stream${qs}`;

  const es = new EventSource(url);

  es.onmessage = (evt) => {
    try {
      const obj = JSON.parse(evt.data);
      if (obj?.event === "metrics_tick") handlers?.onTick(obj as MetricsTick);
    } catch {
      // ignore
    }
  };

  es.addEventListener("heartbeat", () => handlers?.onHeartbeat?.());
  es.onerror = (err) => handlers?.onError?.(err);

  return () => es.close();
}
