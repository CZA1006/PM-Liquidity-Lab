from __future__ import annotations

from pathlib import Path

import asyncio
import json
import os
import time
from typing import Any, Dict, List, Optional

# ---------------------------------------------------------------------------
# Load backend/.env into process env so monitor.py os.getenv("CALIB_*") works.
# - Keep override=False so shell exports win.
# - Must happen before any os.getenv(...) usage in this module.
# ---------------------------------------------------------------------------
try:
    from dotenv import load_dotenv  # type: ignore

    _env_path = Path(__file__).resolve().parents[1] / ".env"
    if _env_path.exists():
        load_dotenv(dotenv_path=_env_path, override=False)
except Exception:
    pass

from fastapi import Body, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field

from .config import settings
from .gamma import GammaClient, list_events as gamma_list_events
from .monitor import Monitor

# NOTE:
# - This file owns the real FastAPI app instance.
# - main.py only re-exports: from .api import app
app = FastAPI(title="pm-liquidity-lab-backend", version="0.1.0")

# ---------------------------------------------------------------------------
# CORS (dev)
#
# Next.js dev server runs on http://localhost:3000 (or LAN IP).
# Browser will send a CORS preflight OPTIONS request for /gamma/search etc.
# Without CORSMiddleware, FastAPI returns 405 for OPTIONS -> frontend "Load failed".
#
# Env:
#   CORS_ALLOW_ORIGINS="http://localhost:3000,http://127.0.0.1:3000,http://192.168.31.203:3000"
#   CORS_ALLOW_ALL="true"  (default true for local dev)
# ---------------------------------------------------------------------------
_cors_allow_all = os.getenv("CORS_ALLOW_ALL", "true").strip().lower() in ("1", "true", "yes", "y", "on")
_cors_origins_env = os.getenv(
    "CORS_ALLOW_ORIGINS",
    "http://localhost:3000,http://127.0.0.1:3000,http://127.0.0.1:3001,http://localhost:3001",
)
_cors_origins = [o.strip() for o in _cors_origins_env.split(",") if o.strip()]

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"] if _cors_allow_all else _cors_origins,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
    max_age=86400,
)

gamma_client = GammaClient(settings.GAMMA_HTTP_BASE)
_monitor: Optional[Monitor] = None
_monitor_lock = asyncio.Lock()


async def _get_or_start_monitor() -> Monitor:
    global _monitor
    async with _monitor_lock:
        if _monitor is None:
            _monitor = Monitor(settings)
        if settings.MONITOR_AUTOSTART and not _monitor.status().started:
            await _monitor.start()
        return _monitor


@app.on_event("startup")
async def _startup() -> None:
    if settings.MONITOR_AUTOSTART:
        m = await _get_or_start_monitor()
        if not m.status().started:
            await m.start()


@app.on_event("shutdown")
async def _shutdown() -> None:
    global _monitor
    if _monitor is not None:
        await _monitor.stop()
        _monitor = None


@app.get("/health")
def health() -> Dict[str, Any]:
    return {"ok": True}


# ---------------------------------------------------------------------------
# Helpers: Gamma payload normalization (stable API surface for Phase3)
# ---------------------------------------------------------------------------

def _flatten_markets_from_search(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Gamma public-search returns {events:[{markets:[...]}], pagination:{...}}.
    Also expose top-level `markets` for client convenience.
    """
    out: List[Dict[str, Any]] = []
    events = payload.get("events") or []
    if isinstance(events, list):
        for ev in events:
            if not isinstance(ev, dict):
                continue
            ms = ev.get("markets") or []
            if isinstance(ms, list):
                for m in ms:
                    if isinstance(m, dict):
                        out.append(m)
    return out


def _normalize_token_ids(ids_raw: Any) -> List[str]:
    if not ids_raw:
        return []
    if isinstance(ids_raw, list):
        return [str(x) for x in ids_raw if x is not None and str(x).strip()]
    s = str(ids_raw).strip()
    return [s] if s else []


def _normalize_clob_token_ids(raw: Any) -> List[str]:
    """
    Gamma sometimes returns clobTokenIds like: ["[\"tid1\",\"tid2\"]"].
    Normalize it into a flat list of token ids.
    """
    if raw is None:
        return []
    if isinstance(raw, list):
        if len(raw) == 1 and isinstance(raw[0], str):
            s = raw[0].strip()
            if s.startswith("[") and s.endswith("]"):
                try:
                    arr = json.loads(s)
                    if isinstance(arr, list):
                        return [str(x) for x in arr if x is not None]
                except Exception:
                    pass
        return [str(x) for x in raw if x is not None and str(x).strip()]
    if isinstance(raw, str):
        s = raw.strip()
        if s.startswith("[") and s.endswith("]"):
            try:
                arr = json.loads(s)
                if isinstance(arr, list):
                    return [str(x) for x in arr if x is not None]
            except Exception:
                return []
        return [s] if s else []
    return []


def _extract_clob_token_ids_from_market(m: Dict[str, Any]) -> List[str]:
    """
    Prefer market.clobTokenIds/clob_token_ids; fallback to outcomes[*].clobTokenId/tokenId.
    """
    direct = (
        m.get("clobTokenIds")
        or m.get("clob_token_ids")
        or m.get("clobTokenIDs")
        or m.get("clobtokenids")
        or []
    )
    ids = _normalize_clob_token_ids(direct)
    if ids:
        return ids

    outs = m.get("outcomes") or m.get("outcome") or []
    if isinstance(outs, list):
        acc: List[str] = []
        for o in outs:
            if not isinstance(o, dict):
                continue
            cand = (
                o.get("clobTokenId")
                or o.get("clob_token_id")
                or o.get("tokenId")
                or o.get("token_id")
            )
            for tid in _normalize_token_ids(cand):
                if tid and tid not in acc:
                    acc.append(tid)
        return acc

    return []


async def _find_market_by_slug(gamma: GammaClient, slug: str) -> Optional[Dict[str, Any]]:
    """
    Resolve a market dict that contains clob token ids if possible.
    Strategy:
      1) scan list_markets pages (more likely to include clobTokenIds)
      2) fallback to public_search(q=slug) and find in events[].markets[]
    """
    slug = (slug or "").strip()
    if not slug:
        return None

    offset = 0
    limit = 100
    for _ in range(20):
        try:
            page = await gamma.list_markets(limit=limit, offset=offset)
        except Exception:
            page = None

        if not page:
            break

        markets = page
        if isinstance(page, dict):
            markets = page.get("markets") or page.get("data") or []

        if not isinstance(markets, list) or not markets:
            break

        for m in markets:
            if isinstance(m, dict) and (m.get("slug") == slug):
                return m

        offset += limit

    try:
        payload = await gamma.public_search(
            q=slug, limit_per_type=25, page=0, events_status="active", cache=True
        )
        if isinstance(payload, dict):
            for m in _flatten_markets_from_search(payload):
                if m.get("slug") == slug:
                    return m
    except Exception:
        pass

    return None

@app.post("/monitor/start")
async def monitor_start() -> JSONResponse:
    m = await _get_or_start_monitor()
    await m.start()
    return JSONResponse(content={"ok": True, "status": m.status().__dict__})


@app.post("/monitor/stop")
async def monitor_stop() -> JSONResponse:
    global _monitor
    if _monitor is None:
        return JSONResponse(content={"ok": True, "status": "not_running"})
    await _monitor.stop()
    return JSONResponse(content={"ok": True, "status": "stopped"})


@app.get("/monitor/status")
async def monitor_status() -> JSONResponse:
    m = await _get_or_start_monitor()
    return JSONResponse(content=m.status().__dict__)


@app.get("/metrics/tokens")
async def metrics_tokens() -> Dict[str, Any]:
    m = await _get_or_start_monitor()
    return {"token_ids": m.status().token_ids}


@app.get("/metrics/current")
async def metrics_current(token_id: Optional[str] = Query(default=None)) -> Dict[str, Any]:
    m = await _get_or_start_monitor()
    return m.current_metrics(token_id=token_id)


@app.get("/metrics/stream")
async def metrics_stream(token_id: Optional[str] = Query(default=None)) -> StreamingResponse:
    m = await _get_or_start_monitor()
    if token_id and token_id not in m.token_ids:
        raise HTTPException(status_code=404, detail=f"unknown token_id: {token_id}")
    q = await m.hub.subscribe()
    heartbeat = int(settings.MONITOR_SSE_HEARTBEAT_SEC)

    async def gen():
        last_hb = time.time()
        try:
            for payload in m.current_metrics(token_id=token_id).values():
                data = json.dumps(payload, ensure_ascii=False)
                yield f"data: {data}\n\n"
            while True:
                now = time.time()
                if now - last_hb >= heartbeat:
                    last_hb = now
                    yield "event: heartbeat\ndata: {}\n\n"

                try:
                    item = await asyncio.wait_for(q.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue

                if token_id is not None:
                    if not isinstance(item, dict) or item.get("token_id") != token_id:
                        continue

                data = json.dumps(item, ensure_ascii=False)
                yield f"data: {data}\n\n"
        finally:
            await m.hub.unsubscribe(q)

    return StreamingResponse(gen(), media_type="text/event-stream")


class SetTokensRequest(BaseModel):
    token_ids: List[str] = Field(default_factory=list, description="CLOB outcome token IDs to monitor")
    restart: bool = Field(True, description="Restart monitor if already running")


@app.post("/monitor/set_tokens")
async def monitor_set_tokens(payload: SetTokensRequest = Body(...)) -> JSONResponse:
    m = await _get_or_start_monitor()
    token_ids = _normalize_clob_token_ids(payload.token_ids)
    if not token_ids:
        return JSONResponse(
            content={
                "ok": True,
                "note": "token_ids empty after normalization; keeping current tokens",
                "status": m.status().__dict__,
            }
        )
    await m.set_tokens(token_ids, restart=payload.restart)
    return JSONResponse(content={"ok": True, "status": m.status().__dict__})


@app.get("/gamma/search")
async def gamma_search(
    q: str = Query(..., description="Search keyword"),
    limit_per_type: int = Query(25, ge=1, le=100),
    page: int = Query(0, ge=0),
    events_status: str = Query("active"),
    cache: bool = Query(True),
) -> JSONResponse:
    data = await gamma_client.public_search(
        q=q,
        limit_per_type=limit_per_type,
        page=page,
        events_status=events_status,
        cache=cache,
    )
    if not isinstance(data, dict):
        raise HTTPException(status_code=502, detail="gamma public_search returned non-dict")
    markets = _flatten_markets_from_search(data)
    for m in markets:
        raw = m.get("clobTokenIds") or m.get("clob_token_ids")
        if raw is not None:
            m["clobTokenIds"] = _normalize_clob_token_ids(raw)
    data["markets"] = markets
    return JSONResponse(content=data)


@app.get("/gamma/market/{slug}")
async def gamma_market(slug: str) -> JSONResponse:
    market = await _find_market_by_slug(gamma_client, slug)
    if not market:
        raise HTTPException(status_code=404, detail="market_not_found")
    tids = _extract_clob_token_ids_from_market(market)
    market["clobTokenIds"] = tids
    market["clob_token_ids"] = tids
    return JSONResponse(content=market)


@app.get("/gamma/active_markets")
async def gamma_active_markets(
    limit: int = 100,
    offset: int = 0,
    active_only: bool = True,
) -> JSONResponse:
    """
    Enumerate ACTIVE markets (event-based; best-effort).
    Response shape: { markets: [...], events_n: n, markets_n: m, limit, offset }
    """
    if active_only:
        data = await gamma_list_events(limit=limit, offset=offset, active=True, closed=False)
    else:
        data = await gamma_list_events(limit=limit, offset=offset, active=None, closed=None, archived=None)

    events = data if isinstance(data, list) else data.get("events") or data.get("data") or []
    out_markets: List[Dict[str, Any]] = []
    for ev in events:
        if not isinstance(ev, dict):
            continue
        ev_id = ev.get("id")
        ev_title = ev.get("title") or ev.get("name")
        ev_slug = ev.get("slug")
        ev_category = ev.get("category")
        ev_tags = ev.get("tags")
        ev_start = ev.get("startDate") or ev.get("start_date")
        ev_end = ev.get("endDate") or ev.get("end_date")
        ev_closed = ev.get("closed")
        ev_active = ev.get("active")
        ev_archived = ev.get("archived")

        markets = ev.get("markets") or []
        if not isinstance(markets, list):
            continue

        for m in markets:
            if not isinstance(m, dict):
                continue
            mm = dict(m)
            mm["event_id"] = ev_id
            mm["event_title"] = ev_title
            mm["event_slug"] = ev_slug
            mm["event_category"] = ev_category
            mm["event_tags"] = ev_tags
            mm["event_start"] = ev_start
            mm["event_end"] = ev_end
            mm["event_active"] = ev_active
            mm["event_closed"] = ev_closed
            mm["event_archived"] = ev_archived
            out_markets.append(mm)

    return JSONResponse(
        content={
            "limit": limit,
            "offset": offset,
            "active_only": active_only,
            "events_n": len(events),
            "markets": out_markets,
            "markets_n": len(out_markets),
        }
    )
