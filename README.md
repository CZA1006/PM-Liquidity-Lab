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
