# PM Liquidity Lab

Realtime liquidity monitoring + visualization for **Polymarket CLOB** markets.

This repo is a full-stack local app:
- **Backend (FastAPI)**: market metadata proxy, token monitoring orchestration, metrics computation, realtime streaming via **SSE**.
- **Frontend (Next.js App Router)**: market search → market detail, **YES/NO** cards, realtime charts (lightweight-charts **v5**).

> Current status: local prototype is functional: market detail + realtime charts (mid / spread_bps / impact_bps).  
> Current focus: **monitor calibration + raw event logging** for acceptance and debugging.  
> Next milestone: watchlist dashboard + improved UX + definitions panel + persistence (ClickHouse/Postgres).

---

## Contents
- [Features](#features)
- [Calibration + Raw Event Logs](#calibration--raw-event-logs)
- [Data Dictionary](#data-dictionary)
- [Research Playbook](#research-playbook)
- [Screenshots / UI](#screenshots--ui)
- [Repo Structure](#repo-structure)
- [Quickstart](#quickstart)
- [Architecture](#architecture)
- [Backend API](#backend-api)
- [Realtime SSE Protocol](#realtime-sse-protocol)
- [Liquidity Metrics](#liquidity-metrics)
- [Frontend Charts](#frontend-charts)
- [Development Workflow](#development-workflow)
- [Acceptance / Load Test](#acceptance--load-test)
- [External Disk 1h Test](#external-disk-1h-test)
- [Troubleshooting](#troubleshooting)
- [Roadmap](#roadmap)
- [License](#license)

---

## Features

### Market discovery
- Search Polymarket markets by keyword (via backend `/gamma/search`)
- Open market detail page by slug

### Token monitoring control
From a market detail page, you can configure backend to monitor that market’s CLOB token IDs:
- **Monitor tokens (restart)**: restart the monitor loop and reconnect WS/SSE pipeline
- **Monitor tokens (no restart)**: update token list without forced restart (useful when already running)

### Realtime metrics + charts
- Backend streams `metrics_tick` over **SSE** (`/metrics/stream`)
- Frontend consumes SSE, filters by selected tokens, maintains in-memory rolling history
- 3 charts rendered in market detail:
  1) **mid (YES/NO)**
  2) **spread_bps (YES/NO)**
  3) **impact_bps @ notional=100 (buy/sell)**

> Note: backend also writes **raw events JSONL** (monitor status provides `raw_log_path`) for auditing/acceptance.

---

## Screenshots / UI

Market detail page typically shows:
- Header: question / slug / connection status / last tick timestamp
- Two cards: YES / NO (bid/ask/mid/spread/spread_bps/two_sided + impact snapshot)
- Three charts (mid/spread_bps/impact_bps)
- Debug section (tokens + monitor status)

---

## Calibration + Raw Event Logs

Backend monitor includes a periodic **calibration** step that compares:
- **WS snapshot-derived best bid/ask**
vs
- **REST best bid/ask (top-N)** (best-effort)

It writes structured raw events to disk (JSONL) for offline inspection and acceptance checks.

### Raw log location

By default the backend writes under:
- `backend/data/raw/local/events-<ts>.jsonl`

`GET /monitor/status` returns a `raw_log_path` (often `./data/raw/local/events-<ts>.jsonl`), and the load test script maps it to `backend/...` for local file checks.

### Raw event schemas (high level)

Two key events:

1) `calibration_compare`
```json
{
  "event": "calibration_compare",
  "payload": {
    "compare": { "tob_match": true, "mismatch_levels": 0, "mismatch_notional": 0.0, "...": "..." },
    "thresholds": {
      "thresh_top_n": 50,
      "thresh_mismatch_levels": 50,
      "thresh_mismatch_notional": 200.0,
      "thresh_tob_must_match": true,
      "thresh_source": "mixed"
    }
  }
}
```

2) `rebase`
```json
{
  "event": "rebase",
  "payload": { "thresholds": { "...same fields as above..." }, "...": "..." }
}
```

### Threshold sources (two layers)

There are **two configuration layers**:

1) `CALIBRATION_*` (Settings / `.env`) — calibration **interval + top-N defaults**
   - `CALIBRATION_INTERVAL_SEC` (default `30`)
   - `CALIBRATION_TOP_N` (default `50`)

2) `CALIB_*` (environment variables read inside `monitor.py`) — acceptance **thresholds**
   - `CALIB_TOPN` (default `50`)
   - `CALIB_MISMATCH_LEVELS` (default `4` unless overridden)
   - `CALIB_MISMATCH_NOTIONAL` (default `50.0` unless overridden)
   - `CALIB_TOB_MUST_MATCH` (default `1` / true)

Recommended: keep `CALIBRATION_*` for cadence and sampling, and tune `CALIB_*` for acceptance thresholds.

---
## Data Dictionary

This section defines what is stored, where each field comes from, and why each dataset exists for liquidity research.

### Scope and truth model
- The system reconstructs near-real-time **L2 orderbook state** from CLOB WS + REST calibration.
- It does not reconstruct every individual user order action; it reconstructs their effect on visible book levels.
- Book depth is not fixed to top-1/top-5. In-memory `BookState` stores all levels returned by source snapshots. `top_n` is used in calibration comparison only.

### End-to-end data flow
1. Universe enumeration:
`GET /gamma/active_markets` (backend) flattens Gamma event + market metadata.
2. Dump:
`backend/scripts/dump_active_markets.py` writes `active_markets-*.jsonl`.
3. Dimension build:
`backend/scripts/build_dims_duckdb.py` writes `market_dim` + `token_dim`.
4. Capture:
`backend/scripts/capture_dump_shards.py` shards token ids and repeatedly calls `POST /monitor/set_tokens`.
5. Runtime monitor:
WS (`/ws/market`) + REST (`POST /books`) maintain state and publish `metrics_tick`.
6. Persistence:
metrics facts, raw audit events, compressed book delta/snapshot streams.
7. Visualization:
`backend/scripts/viz_liquidity_window.py` slices a window and exports CSV + SVG/PNG.

### API sources and subscriptions
- Gamma API:
  - `GET /events` (via backend `/gamma/active_markets`)
  - `GET /public-search` (via backend `/gamma/search`)
  - `GET /markets` / slug lookup (via backend `/gamma/market/{slug}`)
- CLOB WS:
  - `wss://ws-subscriptions-clob.polymarket.com/ws/market`
  - subscribe payload: `{"type":"market","assets_ids":[token_ids...]}`
- CLOB REST:
  - `GET /book` (single token snapshot)
  - `POST /books` (batch snapshot for calibration)
- Backend runtime API:
  - `/monitor/set_tokens`, `/monitor/status`, `/metrics/current`, `/metrics/stream`

### Dataset 1: Universe dump (`active_markets-*.jsonl`)
Producer: `backend/scripts/dump_active_markets.py`

Core market fields (Gamma):
- `id`, `slug`, `question`, `outcomes`
- `clobTokenIds` / `clob_token_ids`
- `startDate`, `endDate`, `closeTime`
- `active`, `closed`, `archived`, `acceptingOrders`, `enableOrderBook`

Flattened event fields (backend `/gamma/active_markets`):
- `event_id`, `event_slug`, `event_title`
- `event_category`, `event_tags`
- `event_start`, `event_end`, `event_active`, `event_closed`, `event_archived`

Normalization fields (dump script):
- `_clobTokenIds_norm`: normalized token id array
- `_event_tags_norm`: normalized tag array
- `_event_category_norm`: normalized category string

Strict active filtering (default, when `--all_events` is NOT set):
- Request parameter: `active_only=true` against backend `/gamma/active_markets`
- Additional conservative filter in dump script:
  - `active == true`
  - `closed == false`
  - `archived == false`
  - `enableOrderBook == true`
  - `acceptingOrders == true`
- This avoids mixed-status rows (for example `active=true` but `closed=true`) in capture universes.

Research use:
- Defines capture universe and temporal cohort (`--on_date`).
- Provides category/tag dimensions for cross-market studies.

### Dataset 2: Dimension DB (`pmliq.duckdb`)
Producer: `backend/scripts/build_dims_duckdb.py`

Table `market_dim`:
- Identity: `market_id`, `market_slug`, `question`
- Event dimensions: `event_id`, `event_slug`, `event_title`, `event_category`, `event_tags_json`
- Outcomes/tokens: `outcomes_json`, `clob_token_ids_json`, `clob_tokens_n`
- State flags: `active`, `closed`, `archived`, `accepting_orders`, `enable_orderbook`
- Lifecycle: `start_date`, `end_date`
- Lineage: `source_dump_path`, `ingested_ts_ms`

Table `token_dim`:
- Identity: `token_id`, `market_id`, `market_slug`
- Outcome mapping: `outcome_index`, `outcome_name`
- Event dimensions: `event_id`, `event_slug`, `event_title`, `event_category`, `event_tags_json`
- State flags: `active`, `closed`, `archived`
- Lineage: `source_dump_path`, `ingested_ts_ms`

Research use:
- Stable join keys from token-level facts to market/event metadata.
- Enables grouped comparisons by category/tag/event/outcome.

### Dataset 3: Metrics facts (`data/runs/<run_id>/events.jsonl`)
Producer: `Monitor._write_event(event="metrics_tick", ...)`

Top-level fields:
- `event`, `schema`, `run_id`
- `ts_ms`, `exchange_ts_ms`
- `token_id`, `source` (`ws|rest`), `reason`
- `metrics`
- `write_ts_ms`

`metrics` fields:
- `levels`: `{bids, asks}` number of visible levels
- `best`: `{bid, ask}`
- `spread`, `mid`, `spread_bps`, `two_sided`
- `depth`: depth bands by bps (`10/25/50/100`) with bid/ask qty+notional
- `slippage`: buy/sell impact by notional (default `100/500/1000`)
- `slippage_filled`: fillability boolean per notional/side
- `imbalance_topk`
- `last_trade_price`
- `health`:
  - `ws_connected`
  - `last_rest_snapshot_ts_ms`
  - `last_compare`
  - `rebase_count`

Research use:
- Primary fact table for spread/depth/impact timeseries.
- Input for all window slicing, ranking, and strategy research.

### Dataset 4: Raw audit log (`data/raw/<run_id>/events-*.jsonl`)
Producer: `RawLogWriter`

Record families:
- Transport family (`kind:*`): `ws_book`, `ws_other`, `ws_price_change`, `rest_snapshot`
- Calibration family (`event:*`): `calibration_compare`, `rebase`, `calibration_summary`

Common fields:
- `ts_ms`, `run_id`, `token_id`, `payload`, `write_ts_ms`
- discriminator: `kind` or `event`

Calibration payload highlights:
- `calibration_compare.payload.compare`:
  - `tob_match`, `mismatch_levels`, `mismatch_notional`, `compared_topn`, TOB prices
- `calibration_summary.payload`:
  - `compares`, `tob_match_rate`, `mismatch_notional_p90`, `sample_rate`, `thresholds`
- `thresholds` schema is unified as:
  - `thresh_top_n`
  - `thresh_mismatch_levels`
  - `thresh_mismatch_notional`
  - `thresh_tob_must_match`
  - `thresh_source`

Research use:
- Data quality diagnostics and trust scoring.
- Distinguishes market moves from calibration/rebase artifacts.

### Dataset 5: Compressed book logs (`data/books/<run_id>/*.jsonl.zst`)
Producer: `RawLogWriter.write_books_delta`, `RawLogWriter.write_books_snapshot`

Shared envelope:
- `{ts_ms, run_id, token_id, event, payload}`

`books_delta-*.jsonl.zst`:
- `event="books_delta"`
- `payload`:
  - `source`
  - `ts_ms`
  - `delta`
- `delta` format:
  - `bids`: `[[price, old_size, new_size], ...]`
  - `asks`: `[[price, old_size, new_size], ...]`

`books_snapshot-*.jsonl.zst`:
- `event="books_snapshot"`
- `payload`:
  - `source` (`ws|rest`)
  - `reason` (`first_ws|calibration|rebase`)
  - `ts_ms`
  - `book` (full book state at that moment)

Research use:
- High-compression historical storage.
- Better long-run capacity planning (`books_delta_mb_per_hour`) before cloud migration.

### Dataset 6: Capacity and quality reports (`reports/*.csv`)
Producers: `scale_probe.py`, `capture_dump_shards.py`

Core fields:
- Throughput:
  - `ws_book_msgs`, `ws_other_msgs`, `*_per_s`
- Calibration quality:
  - `rebase_count`, `compares`, `tob_match_rate`, `mismatch_notional_p90`, `thresholds_ok`
- Storage growth:
  - `raw_bytes/raw_mb_per_hour`
  - `books_delta_bytes_delta/books_delta_mb_per_hour`
  - `books_snapshot_bytes_delta/books_snapshot_mb_per_hour`
- Metrics coverage:
  - `metrics_tick_count`, `metrics_tick_tokens`
  - `metrics_mid_cov`, `metrics_spread_cov`, `metrics_depth_cov`, `metrics_slippage_cov`

Research use:
- Converts technical run logs into cost-quality dashboards.
- Direct input for cloud sizing (S3 + warehouse + compute).

### Runtime status fields (`/monitor/status`) and meaning
- `run_id`: monitor run identity.
- `started`: monitor loop is running.
- `ws_connected`: WS transport connected.
- `token_ids`: currently monitored tokens.
- `ws_book_msgs/ws_other_msgs`: WS event counters.
- `rest_polls`: number of REST calibration cycles.
- `rebase_count`: number of hard resets.
- `raw_log_path/raw_events_written`: raw audit stream location + count.
- `books_delta_log_path/books_delta_written`: compressed delta stream location + count.
- `books_snapshot_log_path/books_snapshot_written`: compressed snapshot stream location + count.
- `effective_rest_poll_sec`: effective rest poll interval.
- `effective_calibration_interval_sec`: configured calibration interval.
- `effective_calibration_top_n`: top-N used for compare.
- `effective_monitor_mismatch_levels`: mismatch-level threshold used for rebase gate.

### Terms and definitions
- **L2 book**: price-level aggregated orderbook (`price -> size`) by side.
- **TOB**: top-of-book (`best_bid`, `best_ask`).
- **mid**: `(best_bid + best_ask)/2` when two-sided.
- **spread**: `best_ask - best_bid`.
- **spread_bps**: `spread / mid * 10,000`.
- **depth band (N bps)**: cumulative bid/ask quantity+notional within `mid +/- Nbps`.
- **impact/slippage**: simulated VWAP impact for target notional buy/sell.
- **drift**: divergence between in-memory WS-derived book and REST snapshot.
- **rebase**: hard overwrite from REST snapshot after threshold breach.
- **calibration_summary**: per REST cycle quality summary row.

---
## Research Playbook

This section is a practical runbook for liquidity research using your current pipeline.

### A) Build a research-ready dataset
1. Build universe dump:
`dump_active_markets.py` with desired `--on_date` (or without date for "now active").
2. Build dimensions:
`build_dims_duckdb.py` into your target DuckDB.
3. Capture facts:
`capture_dump_shards.py` with desired shard/worker strategy.
4. Confirm data quality:
`inspect_dataset.py --recent_sec ...` and check coverage + calibration summary.

### B) Recommended capture modes
- Single-worker debug:
  - quick correctness and schema validation.
- Multi-worker throughput:
  - `--worker_apis` across multiple ports/processes.
  - `--sort_by volume` for active-first capture.
- Comparable randomization:
  - add `--shuffle_seed` for reproducible token order.

### C) Understand window semantics
- `capture_dump_shards.py` runs each shard for `--shard_duration`.
- With `N` workers and `S` shards:
  - wall time is roughly `ceil(S/N) * shard_duration`.
- For a true "same 15-minute global window" over all shards:
  - need enough workers to finish all shards in one round, or increase `shard_tokens` to reduce shard count.
- Important timing split:
  - **Capture wall clock** = only the shard capture stage (`capture_dump_shards.py`).
  - **End-to-end wall clock** = dump refresh + worker start/health checks + capture + shutdown.
  - If you want a clean "15-minute" measurement, prepare dump/workers first, then time only capture.

### D) Produce visualization slices
Use `viz_liquidity_window.py`:
- Full window aggregate:
  - `--start`, `--end`, `--split_outcomes`, `--y_clip_p`
- Single market:
  - `--market_slug`
- Outputs:
  - `--out_csv`
  - `--out_svg`
  - `--out_png` (recommended for readability)

### E) Market selection strategy for readable plots
- Avoid plotting all markets in one figure when token count is large.
- Rank first by activity from sliced CSV:
  - sample count per market
  - `avg_depth_25_total_notional`
  - `avg(spread_bps)` with minimum sample threshold
- Then produce per-market plots for top-N.

### F) Quality gates before using a slice for research
- Monitor health:
  - `ws_connected=true`
  - non-zero `ws_book_msgs`, non-zero `rest_polls`
- Calibration quality:
  - `thresholds_ok=true`
  - reasonable `tob_match_rate`
  - `rebase_count` not exploding
- Metrics completeness:
  - `metrics_mid_cov` near 1.0
  - `metrics_depth_cov` near 1.0
  - `metrics_slippage_cov` near 1.0
  - `metrics_spread_cov` can be lower on one-sided books

### G) Suggested first analyses
1. Cross-sectional liquidity ranking:
   compare `avg_spread_bps`, `avg_depth_25_total_notional`, `avg_impact_buy_100_bps`.
2. Stability analysis:
   per-market variance of spread/impact over window.
3. Stress detection:
   detect jumps in `spread_bps` and simultaneous drops in depth.
4. Health-adjusted filtering:
   exclude windows with high rebase pressure.

### H) Cost and cloud planning from local tests
- Use per-hour metrics from reports:
  - `raw_mb_per_hour`
  - `books_delta_mb_per_hour`
  - `books_snapshot_mb_per_hour`
- Extrapolate by:
  - target token coverage
  - runtime hours/day
  - expected retention days
- Use `books_delta_mb_per_hour` as the primary long-horizon storage baseline.

### I) Current known limitation (important)
- This pipeline is optimized for capture from "now onward".
- `--on_date` controls market universe selection, not true time travel replay of historical L2 states.
- Exact historical L2 at arbitrary past timestamps requires historical archival feed retention from that period.

### J) Stable single-wave 15-minute full-active run (external disk)
Goal: capture one global 15-minute window without multi-wave spillover.

Preconditions:
- External disk mounted (for example `/Volumes/T7/PolyMarket DB`)
- Worker count chosen from capacity estimation (`estimate_capture_capacity.py`)
- For current MacBook-class setup, common profile is:
  - `workers=27`
  - `shard_tokens=2000`
  - `max_shards=27`

Recommended two-stage flow:
1. Prepare once (not timed as capture):
   - start workers
   - health-check all workers
   - refresh one dump file
2. Timed stage:
   - run only `capture_dump_shards.py` with fixed `--dump`, fixed workers, fixed shard plan

Pass criteria for single-wave 15-minute run:
- report rows == worker count (for example `27`)
- `duration_s` per shard near 900s (allowing poll overshoot, often up to ~930s)
- no short shards (`duration_s < 765`)
- `shard_tokens_sum` matches monitorable token universe for the dump
- non-zero bytes for raw/books outputs

### K) Why a "15-minute run" can exceed 15 minutes
Most common causes:
1. You measured end-to-end runtime, not capture-only runtime.
2. `S > N` (shards exceed alive workers), so shards run in multiple waves.
3. `shard_tokens` too small (creates too many shards).
4. Poll granularity and request overhead add per-shard overshoot.

Quick sanity formula:
- `required_workers_for_one_wave = ceil(total_tokens / shard_tokens)`
- one-wave 15-minute capture requires:
  - `alive_workers >= required_workers_for_one_wave`
  - and `max_shards <= alive_workers`

### L) Rebase pressure diagnosis (quick)
`rebase` is usually driven by calibration compare thresholds, not by a single cause.

Check per run:
- `tob_match_rate`
- `mismatch_notional_p90`
- `rebase_count / compares`
- REST observability:
  - `last_rest_poll_rtt_ms`
  - `rest_poll_rtt_ms_p50/p90/p99`
  - `snapshot_age_ms_p50/p90/p99`

Interpretation guideline:
- High `snapshot_age` with moderate RTT:
  - upstream snapshot staleness likely dominates.
- High RTT and high snapshot age:
  - local/network load plus upstream effects.
- High rebase with low TOB mismatch but high mismatch_notional:
  - threshold sensitivity is likely too strict for current concurrency scale.

---
## Repo structure

```
pm-liquidity-lab/
  backend/                     # FastAPI backend
  frontend/                    # Next.js frontend (App Router)
  README.md
```

### Frontend structure (actual)

```
frontend/
  src/
    app/
      layout.tsx
      globals.css
      page.tsx                 # Home: search + entry
      market/
        [slug]/
          page.tsx             # Market detail route
      watchlist/
        page.tsx               # Watchlist route (WIP / planned)
    components/
      MarketSearch.tsx
      MarketView.tsx
      MetricsPanel.tsx
      WatchlistView.tsx
      charts/
        LineCharts.tsx         # lightweight-charts v5 wrapper
    lib/
      api.ts                   # REST + SSE helper
      sse.ts                   # (if present) SSE helpers / shared types
  package.json
  next.config.mjs
  tsconfig.json
  tailwind.config.ts
  postcss.config.mjs
  .env.local
```

> Note: `.next/` and `node_modules/` are build/runtime artifacts and should not be committed.

---

## Quickstart

### Requirements
- Node.js 18+ (recommended)
- Python 3.10+ (recommended)

### 1) Backend
```bash
cd backend

# one-time: create venv
python -m venv .venv
source .venv/bin/activate  # macOS/Linux
# .venv\Scripts\activate   # Windows PowerShell

pip install -r requirements.txt

uvicorn app.main:app --host 0.0.0.0 --port 8000
```

#### Backend environment

backend/.env example:
```
# ---- CLOB ----
CLOB_HTTP_BASE=https://clob.polymarket.com
CLOB_WS_BASE=wss://ws-subscriptions-clob.polymarket.com/ws
CLOB_WS_CHANNEL=market

# ---- Gamma ----
GAMMA_HTTP_BASE=https://gamma-api.polymarket.com

# ---- Calibration cadence (Settings/.env) ----
CALIBRATION_INTERVAL_SEC=30
CALIBRATION_TOP_N=50

# ---- Calibration thresholds (monitor.py CALIB_*) ----
CALIB_TOPN=50
CALIB_MISMATCH_LEVELS=50
CALIB_MISMATCH_NOTIONAL=200
CALIB_TOB_MUST_MATCH=1

# ---- Logging / storage ----
DATA_DIR=./data
RUN_ID=local
```

Health check:
```bash
curl http://127.0.0.1:8000/health
```

### 2) Frontend
```bash
cd frontend
npm install
npm run dev
```

Open:
- Frontend: `http://localhost:3000`
- Backend: `http://127.0.0.1:8000`

---

## Architecture

### Data flow (high level)

1. **Search**: Frontend → `GET /gamma/search?q=...`  
2. **Market metadata**: Frontend → `GET /gamma/market/{slug}` → returns market question + outcomes + `clobTokenIds`  
3. **Set tokens**: Frontend → `POST /monitor/set_tokens` with `{ token_ids, restart }`  
4. **Realtime stream**: Frontend opens **SSE** `GET /metrics/stream`  
5. **Metrics ticks**: Backend emits `metrics_tick` for monitored tokens; frontend filters only tokens for current market view  
6. **Charts**: Frontend appends points into in-memory history buffers and renders charts

### Key design choices
- **SSE** is used for realtime streaming (simple + browser-friendly + auto reconnect).
- Frontend keeps a **rolling buffer** per token per metric (e.g., last 600 points) to control memory and performance.
- Chart wrapper normalizes timestamps to satisfy `lightweight-charts v5` ordering requirements (strict asc + no duplicates).

---

## Backend API

> Endpoints used by frontend (naming is backend-owned; these are typical):

### Health
`GET /health` → `{ ok: boolean }`

### Market search / market info proxy
- `GET /gamma/search?q={query}&limit_per_type={n}&page={p}`  
  Returns `events[]`, `markets[]`, `pagination`

- `GET /gamma/market/{slug}`  
  Returns `question`, `outcomes`, and `clobTokenIds` (or `clob_token_ids`)

### Monitor control
- `GET /monitor/status`  
  Returns:
  - `started`
  - `ws_connected`
  - `token_ids[]`
  - counters (book msgs, rest polls, etc.)

- `POST /monitor/set_tokens`  
  Body: `{ token_ids: string[], restart: boolean }`  
  Returns: `{ ok: boolean, status: MonitorStatus }`

### Metrics snapshot (optional)
- `GET /metrics/current`  
  Returns current tick per token (useful for initial state).

---

## Realtime SSE Protocol

Frontend listens on:

`GET /metrics/stream`

Backend sends events:

### Heartbeat
```
event: heartbeat
data: {}
```

### Metrics tick
Default message:
```json
{
  "event": "metrics_tick",
  "schema": "v1",
  "run_id": "...",
  "token_id": "...",
  "ts_ms": 1770...,
  "exchange_ts_ms": 1770...,
  "metrics": { ... }
}
```

---

## Liquidity metrics

### Best prices
- **bid**: best bid price (highest buy price).
- **ask**: best ask price (lowest sell price).

### Mid price
- **mid**: typically `(bid + ask) / 2` when two-sided.
  - If one-sided, may fall back to whichever side exists (backend-defined).

### Spread
- **spread**: `ask - bid` when two-sided.
- **spread_bps**: `spread / mid * 10,000` (basis points).
  - Smaller spread → generally better liquidity.

### Two-sided
- **two_sided**: whether both bid and ask exist.
  - If false, the market is “thin” or temporarily one-sided.

### Impact / slippage
**impact_bps @ notional=N** estimates how costly it is to trade size `N` at current book.
- A backend may compute VWAP for filling `N` notional and compare to a reference (mid/best).
- **Higher impact_bps** = worse liquidity (more slippage).

In the UI we commonly plot:
- `impact_bps` for **buy@100** and **sell@100** (for each token side YES/NO).

---

## Frontend charts

We render three charts in MarketView:

1) **mid (YES/NO)**  
   - Lines: `YES mid`, `NO mid`

2) **spread_bps (YES/NO)**  
   - Lines: `YES spread_bps`, `NO spread_bps`

3) **impact_bps @ notional=100 (buy/sell)**  
   - Lines (up to four):
     - `YES buy@100`
     - `YES sell@100`
     - `NO buy@100`
     - `NO sell@100`

Hover shows exact values at the crosshair time.

---

## Development workflow

### Common commands
Backend:
```bash
cd backend
source .venv/bin/activate
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

Frontend:
```bash
cd frontend
npm run dev
```

### Minimal acceptance checklist
- [ ] Home page search returns results
- [ ] Market detail loads question/outcomes
- [ ] Clicking “Monitor tokens (restart)” returns `{ ok: true }`
- [ ] SSE `/metrics/stream` stays connected and ticks arrive
- [ ] Charts update in realtime without runtime errors

---

## Acceptance / Load Test

We provide a local load test script that:
- picks a set of active markets → extracts token IDs
- calls POST /monitor/set_tokens
- runs for N seconds while polling /monitor/status and /metrics/current
- validates raw-log acceptance criteria (threshold snapshots + consistency)
- prints calibration quality stats (TOB match rate, mismatch notional p90)

### Run
```bash
python backend/scripts/load_test_2h.py --api http://127.0.0.1:8000 --markets 20 --duration 120 --max_tokens 60
```

### Pass/Fail signals
- ws_connected=True in [final status]
- [raw log size] backend/data/raw/local/events-*.jsonl ... (file exists)
- [accept] thresholds: OK (rebase thresholds consistent)
- [calibration stats] compares > 0 (unless run too short / no calibration tick)

---
## External Disk 1h Test

Use this runbook to validate a 1-hour capture on an external disk before AWS deployment.

Assumptions:
- Repo remains on local SSD.
- Only `DATA_DIR`/artifacts are redirected to external disk.
- Example external disk root: `/Volumes/T7/PolyMarket DB`

### 0) Prepare paths
```bash
cd /Users/caizhuoang/Desktop/Dabanc/pm-liquidity-lab
export PMDB="/Volumes/T7/PolyMarket DB"
mkdir -p "$PMDB/data" "$PMDB/dumps" "$PMDB/warehouse" "$PMDB/reports"
```

### 1) Start backend with disk-backed DATA_DIR
Use a dedicated port to avoid conflicts:
```bash
cd backend
DATA_DIR="$PMDB/data" RUN_ID="t7_jan01_probe" uvicorn app.main:app --host 0.0.0.0 --port 8001
```

In another terminal:
```bash
curl -s http://127.0.0.1:8001/health | jq
```

### 2) Build market universe for `2026-01-01`
```bash
cd /Users/caizhuoang/Desktop/Dabanc/pm-liquidity-lab
python backend/scripts/dump_active_markets.py \
  --api http://127.0.0.1:8001 \
  --out "$PMDB/dumps" \
  --max 5000 \
  --page 200 \
  --all_events \
  --on_date 2026-01-01
```

Optional: build/update dimension DB
```bash
LATEST=$(ls -1t "$PMDB"/dumps/active_markets-*.jsonl | head -n 1)
python backend/scripts/build_dims_duckdb.py \
  --dump "$LATEST" \
  --db "$PMDB/warehouse/pmliq.duckdb" \
  --replace
```

### 3) Run 1-hour shard capture
```bash
LATEST=$(ls -1t "$PMDB"/dumps/active_markets-*.jsonl | head -n 1)
python backend/scripts/capture_dump_shards.py \
  --api http://127.0.0.1:8001 \
  --dump "$LATEST" \
  --monitorable_only \
  --shard_tokens 100 \
  --shard_duration 3600 \
  --poll 10 \
  --max_shards 1 \
  --out "$PMDB/reports/jan01_shard_capture_1h.csv"
```

### 4) Acceptance checks
```bash
cat "$PMDB/reports/jan01_shard_capture_1h.csv"
curl -s "http://127.0.0.1:8001/monitor/status" | jq '{
  ws_connected, raw_log_path, books_delta_log_path, books_snapshot_log_path,
  books_delta_written, books_snapshot_written, raw_events_written
}'
du -h "$PMDB/data/raw" "$PMDB/data/books" "$PMDB/reports"
```

Minimum Go criteria:
- `ws_connected=true` for most of the run.
- `metrics_tick_count > 0` and `metrics_tick_tokens > 0`.
- `books_delta_written > 0` and `books_snapshot_written > 0`.
- `books_delta_bytes_delta > 0` in capture report.
- Paths in `/monitor/status` point to external disk (`/Volumes/T7/...`).

### 5) Interpreting storage for cloud sizing
- Prefer `books_delta_mb_per_hour` as replay-oriented core estimate.
- Track `raw_mb_per_hour` separately (audit/debug overhead).
- For planning, multiply by target token coverage and duty cycle, then apply safety factor (e.g., 1.5x).

### 6) Post-run window visualization (time + market filters)
Generate a research slice from `events.jsonl` and output both CSV and SVG:
```bash
python backend/scripts/viz_liquidity_window.py \
  --api http://127.0.0.1:8001 \
  --db "$PMDB/warehouse/pmliq.duckdb" \
  --market_slug "microstrategy-sell-any-bitcoin-in-2025" \
  --start "2026-02-06T09:00:00+08:00" \
  --end   "2026-02-06T10:00:00+08:00" \
  --out_csv "$PMDB/reports/liquidity_slice_1h.csv" \
  --out_svg "$PMDB/reports/liquidity_slice_1h.svg"
```

Quick recent-window example (no explicit time range):
```bash
python backend/scripts/viz_liquidity_window.py \
  --api http://127.0.0.1:8001 \
  --db "$PMDB/warehouse/pmliq.duckdb" \
  --recent_sec 1800 \
  --out_csv "$PMDB/reports/liquidity_slice_recent.csv" \
  --out_svg "$PMDB/reports/liquidity_slice_recent.svg"
```

---
## Troubleshooting

### 1) `Module not found: Can't resolve '@/components/...'`
- Confirm file names match imports exactly (case-sensitive on some systems).
- Confirm `tsconfig.json` path alias:
  - `"paths": { "@/*": ["./src/*"] }`
- Restart `npm run dev` after changes.

### 2) `lightweight-charts`: `addLineSeries is not a function`
You are on lightweight-charts **v5**.  
Use v5 API:
- `chart.addSeries(LineSeries, options)`
instead of v4 style `chart.addLineSeries(...)`.

### 3) `Assertion failed: data must be asc ordered by time`
`lightweight-charts v5` requires:
- strictly increasing `time`
- no duplicates

Fix:
- normalize/sort and coalesce duplicates (keep last value)
- or append using `series.update()` with unique timestamps

### 4) SSE shows disconnected quickly
- Check backend logs: monitor WS ping/reconnect loops
- Ensure backend sends heartbeat periodically
- Confirm browser Network tab shows the SSE request as “pending/streaming”

### 5) Load test says FAIL (no calibration_compare.payload.thresholds found) but you can see thresholds in the JSONL
This usually means the script is reading the wrong file path (relative raw_log_path vs actual repo path).
Expected behavior:
- /monitor/status returns something like ./data/raw/local/events-....jsonl
- actual file is at backend/data/raw/local/events-....jsonl

Fix:
- Ensure DATA_DIR=./data and backend working directory is backend/ when starting uvicorn
- Or rely on the load test path mapping (./data/... → backend/data/...) and keep repo layout intact

### 6) CALIB_* env vars not taking effect
If you edited backend/.env but thresholds stay unchanged:
- confirm your backend actually loads .env (Settings loader) and that monitor.py reads from process env
- restart backend process after changes
- print /monitor/status fields like effective_* to confirm what backend thinks is active

---

## Roadmap

### UI/UX
- Watchlist page: card-based multi-market dashboard (grid) with throttled updates
- Hide token IDs by default; show on hover only
- Metric glossary panel: explain each metric with tooltips
- Per-series styling: differentiate YES/NO & buy/sell visually

### Data + storage
- Spool-to-disk fallback (local)
- Plug-in persistence: ClickHouse/Postgres for historical analysis
- Aggregations: rolling median spread, depth bands, impact curves, anomaly detection

### Deployment
- Frontend: Vercel
- Backend: cloud VM/container + SSE support

---

## License

MIT (or TBD)
