"""Built-in candle subscription handler.

Hyperliquid provides a `candle` WebSocket subscription that streams
pre-computed OHLC bars. This module wraps that stream with callback support.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from typing import Any

from hyperliquid_ohlc.types import Candle

logger = logging.getLogger(__name__)

CANDLE_INTERVALS: frozenset[str] = frozenset(
    {
        "1m",
        "3m",
        "5m",
        "15m",
        "30m",
        "1h",
        "2h",
        "4h",
        "8h",
        "12h",
        "1d",
        "3d",
        "1w",
        "1M",
    }
)

type CandleCallback = Callable[[Candle], Awaitable[None]]


class CandleStream:
    """Subscribe to Hyperliquid's built-in candle feed.

    Usage:
        stream = CandleStream(client)
        stream.on_candle("BTC", "1m", my_handler)
        await stream.subscribe("BTC", "1m")
    """

    def __init__(
        self, client: Any
    ) -> None:  # HyperliquidWsClient (avoid circular import)
        self._client = client
        self._callbacks: dict[tuple[str, str], list[CandleCallback]] = {}
        client.on_message("candle", self._handle_candle)

    def on_candle(self, coin: str, interval: str, callback: CandleCallback) -> None:
        """Register a callback for a specific coin + interval pair."""
        if interval not in CANDLE_INTERVALS:
            raise ValueError(
                f"Invalid interval: {interval}. Valid: {sorted(CANDLE_INTERVALS)}"
            )
        key = (coin.upper(), interval)
        self._callbacks.setdefault(key, []).append(callback)

    async def subscribe(self, coin: str, interval: str) -> None:
        """Subscribe to candle updates for a coin and interval."""
        if interval not in CANDLE_INTERVALS:
            raise ValueError(f"Invalid interval: {interval}")
        coin_upper = coin.upper()
        await self._client.subscribe(
            {
                "type": "candle",
                "coin": coin_upper,
                "interval": interval,
            }
        )

    async def _handle_candle(self, data: Any) -> None:
        """Dispatch incoming candle array to registered callbacks."""
        if isinstance(data, dict):
            data = [data]  # single candle update
        elif not isinstance(data, list):
            logger.warning("Unexpected candle data type: %s", type(data))
            return
        for raw in data:
            candle = Candle(
                t=raw["t"],
                T=raw["T"],
                s=raw["s"],
                i=raw["i"],
                o=float(raw["o"]),
                c=float(raw["c"]),
                h=float(raw["h"]),
                l=float(raw["l"]),
                v=float(raw["v"]),
                n=int(raw["n"]),
            )
            key = (candle["s"], candle["i"])
            for cb in self._callbacks.get(key, []):
                try:
                    await cb(candle)
                except Exception:
                    logger.exception("Candle callback raised")
