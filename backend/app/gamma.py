from __future__ import annotations

from typing import Any, Dict, List, Optional
import httpx
from .config import settings

class GammaClient:
    def __init__(self, base: str, timeout: float = 10.0):
        self.base = base.rstrip("/")
        self.timeout = timeout

    async def list_markets(
        self,
        limit: int = 50,
        offset: int = 0,
        order: Optional[str] = "volume_num",
        ascending: bool = False,
        active: Optional[bool] = None,
        closed: Optional[bool] = None,
        archived: Optional[bool] = None,
    ) -> List[Dict[str, Any]]:
        """
        Gamma /markets supports query params: limit, offset, order, ascending, etc.
        But 'order' allowed field names can vary; if we get 422, we retry with fallbacks.
        Docs: order is a comma-separated list of fields to order by; ascending is boolean.
        """
        url = f"{self.base}/markets"

        def _with_filters(params: Dict[str, Any]) -> Dict[str, Any]:
            out = dict(params)
            if active is not None:
                out["active"] = "true" if active else "false"
            if closed is not None:
                out["closed"] = "true" if closed else "false"
            if archived is not None:
                out["archived"] = "true" if archived else "false"
            return out

        # candidate param sets (most -> least opinionated)
        candidates: List[Dict[str, Any]] = []

        if order is not None:
            candidates.append(
                _with_filters({"limit": limit, "offset": offset, "order": order, "ascending": ascending})
            )
            # common fallback: camelCase field names
            candidates.append(
                _with_filters({"limit": limit, "offset": offset, "order": "volumeNum", "ascending": ascending})
            )
            candidates.append(
                _with_filters({"limit": limit, "offset": offset, "order": "liquidityNum", "ascending": ascending})
            )

        # safest fallback: no ordering at all
        candidates.append(_with_filters({"limit": limit, "offset": offset}))

        last_err: Optional[Exception] = None

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            for params in candidates:
                try:
                    r = await client.get(url, params=params)
                    r.raise_for_status()
                    return r.json()
                except httpx.HTTPStatusError as e:
                    last_err = e
                    # If it's 422, try next candidate; otherwise raise immediately
                    if e.response is not None and e.response.status_code == 422:
                        continue
                    raise

        # If all candidates failed, raise last error
        if last_err:
            raise last_err
        raise RuntimeError("Gamma list_markets failed unexpectedly.")

    async def public_search(
        self,
        q: str,
        limit_per_type: int = 15,
        page: int = 0,
        events_status: str = "active",
        cache: bool = True,
    ) -> Dict[str, Any]:
        """
        GET /public-search
        Docs: q required; returns {events, tags, profiles, pagination}.
        """
        url = f"{self.base}/public-search"
        params = {
            "q": q,
            "limit_per_type": limit_per_type,
            "page": page,
            "events_status": events_status,
            "cache": cache,
        }
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            r = await client.get(url, params=params)
            r.raise_for_status()
            return r.json()

    async def list_events(
        self,
        *,
        limit: int = 50,
        offset: int = 0,
        active: Optional[bool] = None,
        closed: Optional[bool] = None,
        archived: Optional[bool] = None,
    ) -> Dict[str, Any]:
        """
        GET /events (Gamma).
        Used for active market enumeration.
        """
        url = f"{self.base}/events"
        params: Dict[str, Any] = {"limit": int(limit), "offset": int(offset)}
        if active is not None:
            params["active"] = "true" if active else "false"
        if closed is not None:
            params["closed"] = "true" if closed else "false"
        if archived is not None:
            params["archived"] = "true" if archived else "false"

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            r = await client.get(url, params=params)
            r.raise_for_status()
            return r.json()

    async def get_market_by_slug(
        self, slug: str, max_pages: int = 20, page_size: int = 200
    ) -> Optional[Dict[str, Any]]:
        """Fetch a single market dict by its slug via paginated /markets."""
        slug = str(slug).strip()
        if not slug:
            return None

        offset = 0
        for _ in range(max_pages):
            batch = await self.list_markets(limit=page_size, offset=offset)
            markets = batch
            if isinstance(batch, dict):
                markets = batch.get("markets") or batch.get("data") or []
            if not markets:
                return None
            for m in markets:
                if isinstance(m, dict) and m.get("slug") == slug:
                    return m
            offset += page_size
        return None

def pick_tokens_from_markets(markets: List[Dict[str, Any]], keyword: str, pick_n: int) -> List[str]:
    kw = keyword.lower().strip()
    token_ids: List[str] = []
    for m in markets:
        text = f"{m.get('question','')} {m.get('slug','')}".lower()
        if kw and kw not in text:
            continue

        # Gamma field name seen in docs is 'clob_token_ids' (snake_case),
        # but some responses may include camelCase variants.
        ids = (
            m.get("clob_token_ids")
            or m.get("clobTokenIds")
            or m.get("clobTokenIDs")
            or []
        )
        if isinstance(ids, str):
            ids = [ids]
        for tid in ids:
            if tid and tid not in token_ids:
                token_ids.append(str(tid))
                if len(token_ids) >= pick_n:
                    return token_ids
    return token_ids


async def list_markets(
    *,
    limit: int = 100,
    offset: int = 0,
    active_only: bool = True,
) -> Dict[str, Any]:
    """
    Enumerate markets from Gamma.
    Best-effort active filtering (Gamma fields vary over time).
    """
    gamma = GammaClient(settings.GAMMA_HTTP_BASE)
    data = await gamma.list_markets(limit=limit, offset=offset)

    markets = data if isinstance(data, list) else data.get("markets") or data.get("data") or []

    if not active_only:
        return {"markets": markets, "raw": data}

    def is_active(m: Dict[str, Any]) -> bool:
        if isinstance(m.get("active"), bool) and m.get("active") is False:
            return False
        if isinstance(m.get("closed"), bool) and m.get("closed") is True:
            return False
        if isinstance(m.get("archived"), bool) and m.get("archived") is True:
            return False
        st = str(m.get("status") or "").lower()
        if st in ("resolved", "closed", "ended", "archived", "settled"):
            return False
        if st in ("active", "trading", "open"):
            return True
        return True

    filtered = [m for m in markets if isinstance(m, dict) and is_active(m)]
    return {"markets": filtered, "raw": data}


async def list_events(
    *,
    limit: int = 50,
    offset: int = 0,
    active: Optional[bool] = None,
    closed: Optional[bool] = None,
    archived: Optional[bool] = None,
) -> Dict[str, Any]:
    gamma = GammaClient(settings.GAMMA_HTTP_BASE)
    return await gamma.list_events(
        limit=limit,
        offset=offset,
        active=active,
        closed=closed,
        archived=archived,
    )
