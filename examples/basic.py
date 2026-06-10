"""Example: stream real-time OHLC from Hyperliquid using the built-in candle feed."""

from __future__ import annotations

import asyncio
import json
import signal

from hyperliquid_ohlc import HyperliquidWsClient, CandleStream
from hyperliquid_ohlc.types import Candle


async def main() -> None:
    client = HyperliquidWsClient(network="mainnet")
    stream = CandleStream(client)

    bars: list[Candle] = []

    async def on_candle(candle: Candle) -> None:
        bars.append(candle)
        print(json.dumps({
            "coin": candle["s"],
            "interval": candle["i"],
            "open": candle["o"],
            "high": candle["h"],
            "low": candle["l"],
            "close": candle["c"],
            "volume": candle["v"],
        }))

    stream.on_candle("BTC", "1m", on_candle)

    await client.connect()
    await stream.subscribe("BTC", "1m")

    # Run until interrupt
    stop = asyncio.get_running_loop().create_future()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, lambda *_: stop.done() or stop.set_result(None))  # type: ignore[misc]
        except Exception:
            pass

    try:
        await stop
    finally:
        await client.stop()
        print(f"\nCollected {len(bars)} candles.")


if __name__ == "__main__":
    asyncio.run(main())
