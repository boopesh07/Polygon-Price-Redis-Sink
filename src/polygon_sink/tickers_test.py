from __future__ import annotations

import asyncio
import os
import time
from dotenv import load_dotenv

from .tickers import fetch_all_active_stock_tickers


async def main() -> None:
    load_dotenv()
    api_key = os.getenv("POLYGON_API_KEY")
    if not api_key:
        print("POLYGON_API_KEY is required in environment")
        raise SystemExit(1)

    started = time.time()
    symbols = await fetch_all_active_stock_tickers(api_key)
    elapsed = time.time() - started

    print(f"tickers_test: PASS count={len(symbols)} elapsed_sec={elapsed:.2f}")
    if "MGN" in symbols:
        print("Ticker 'MGN' was found.")
    else:
        print("Ticker 'MGN' was not found.")


if __name__ == "__main__":
    asyncio.run(main())


