import MarketSearch from "@/components/MarketSearch";

export default function HomePage() {
  return (
    <main className="space-y-4">
      <h1 className="text-2xl font-bold">Phase 3 - Gamma Search -> Market -> Set Tokens -> Metrics</h1>
      <p className="text-sm text-zinc-300">
        Use <span className="font-mono">/gamma/search</span> to find markets, open a market page, call{" "}
        <span className="font-mono">/monitor/set_tokens</span> to switch tokens, and watch{" "}
        <span className="font-mono">/metrics/stream</span> for realtime metrics.
      </p>
      <MarketSearch />
    </main>
  );
}
