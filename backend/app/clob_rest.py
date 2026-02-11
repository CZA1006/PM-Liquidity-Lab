from __future__ import annotations

from typing import Any, Dict, List
import httpx


class ClobRestClient:
    def __init__(self, base: str, timeout: float = 10.0):
        self.base = base.rstrip("/")
        self.timeout = timeout

    async def get_book(self, token_id: str) -> Dict[str, Any]:
        url = f"{self.base}/book"
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            r = await client.get(url, params={"token_id": token_id})
            r.raise_for_status()
            return r.json()

    async def post_books(self, token_ids: List[str]) -> List[Dict[str, Any]]:
        """
        POST /books body example:
        [
          {"token_id":"123"},
          {"token_id":"456"}
        ]
        """
        url = f"{self.base}/books"
        # Avoid overloading /books with very large one-shot payloads.
        # Keep behavior backward-compatible for normal sizes.
        batch_size = 500
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            out: List[Dict[str, Any]] = []
            for i in range(0, len(token_ids), batch_size):
                batch = token_ids[i : i + batch_size]
                payload = [{"token_id": tid} for tid in batch]
                r = await client.post(url, json=payload)
                r.raise_for_status()
                data = r.json()
                if isinstance(data, list):
                    out.extend(data)
            return out
