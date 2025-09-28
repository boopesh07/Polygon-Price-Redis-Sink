from __future__ import annotations

from typing import List
import httpx

from .logging_setup import get_logger


async def fetch_all_active_stock_tickers(api_key: str) -> List[str]:
    logger = get_logger()
    base = "https://api.polygon.io/v3/reference/tickers"
    params = {
        "market": "stocks",
        "active": "true",
        "order": "asc",
        "limit": 1000,
        "sort": "ticker",
        "apiKey": api_key,
    }
    symbols: List[str] = []
    async with httpx.AsyncClient(timeout=httpx.Timeout(20.0, read=60.0)) as client:
        url: str | None = base
        query = params
        while url:
            # Ensure apiKey is always included; some next_url values omit it
            if url == base:
                resp = await client.get(url, params=query)
            else:
                if "apiKey=" in url:
                    resp = await client.get(url)
                else:
                    resp = await client.get(url, params={"apiKey": api_key})
            resp.raise_for_status()
            data = resp.json()
            for item in data.get("results", []) or []:
                t = item.get("ticker")
                if isinstance(t, str) and t:
                    symbols.append(t.upper())
            next_url = data.get("next_url")
            logger.info("ticker_page", page_count=len(data.get("results", []) or []), total=len(symbols), has_next=bool(next_url))
            if next_url:
                # Ensure apiKey is included when following next_url
                url = next_url if "apiKey=" in next_url else f"{next_url}&apiKey={api_key}"
            else:
                url = None
            query = None
    logger.info("ticker_discovery_complete", count=len(symbols))
    return symbols


