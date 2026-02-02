from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from typing import Any, AsyncIterator, Dict, List, Optional

import websockets

log = logging.getLogger("ws")

class MarketWsClient:
    """
    Connects to CLOB market websocket and yields parsed JSON messages.

    Base per docs: wss://ws-subscriptions-clob.polymarket.com/ws/
    Channel: /market
    Subscription payload example:
      { "type": "market", "assets_ids": [tokenId] }
    """
    def __init__(self, ws_base: str, channel: str = "market"):
        self.ws_base = ws_base.rstrip("/")
        self.channel = channel
        self.url = f"{self.ws_base}/{self.channel}"

    async def connect_and_stream(self, token_ids: List[str], stop_event: asyncio.Event) -> AsyncIterator[Dict[str, Any]]:
        backoff = 1.0
        while not stop_event.is_set():
            try:
                async with websockets.connect(self.url, ping_interval=None, close_timeout=5) as ws:
                    # initial subscribe
                    sub = {"type": "market", "assets_ids": token_ids}
                    await ws.send(json.dumps(sub))
                    log.info(f"WS connected {self.url}, subscribed tokens={len(token_ids)}")

                    backoff = 1.0
                    last_ping = time.time()

                    while not stop_event.is_set():
                        # manual ping every 15s
                        now = time.time()
                        if now - last_ping > 15:
                            try:
                                pong_waiter = await ws.ping()
                                await asyncio.wait_for(pong_waiter, timeout=10)
                            except Exception:
                                log.warning("ping failed; forcing reconnect")
                                break
                            last_ping = now

                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=2.0)
                        except asyncio.TimeoutError:
                            continue

                        try:
                            data = json.loads(msg)
                        except Exception:
                            continue

                        if not isinstance(data, dict):
                            yield {"event_type": "__non_dict__", "data": data}
                        else:
                            yield data

            except Exception as e:
                if stop_event.is_set():
                    break
                log.warning(f"WS error: {e!r}")
                await asyncio.sleep(backoff + random.random() * 0.2)
                backoff = min(backoff * 2, 30.0)
