"""Build OHLC bars from raw trade data.

When you need OHLC intervals not supported by the built-in candle feed
(e.g. 10s, 2m, 4s), subscribe to the `trades` feed and aggregate locally.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Coroutine
from typing import Any

from hyperliquid_ohlc.types import OhlcBar, WsTrade

logger = logging.getLogger(__name__)

INTERVAL_MAP: dict[str, int] = {
    "s": 1000,
    "m": 60_000,
    "h": 3_600_000,
    "d": 86_400_000,
    "w": 604_800_000,
}

type OhlcBarCallback = Callable[[OhlcBar], Coroutine[Any, Any, None]]


def _parse_interval(interval: str) -> int:
    """Parse an interval string like '1m', '15m', '10s' into milliseconds."""
    if len(interval) < 2:
        raise ValueError(f"Invalid interval: {interval!r}. Examples: '1m', '5m', '15m', '1h', '10s'")
    unit = interval[-1]
    try:
        value = int(interval[:-1])
        multiplier = INTERVAL_MAP[unit]
    except (ValueError, KeyError):
        raise ValueError(f"Invalid interval: {interval}. Examples: '1m', '5m', '15m', '1h', '10s'")
    if value <= 0:
        raise ValueError(f"Interval value must be positive: {interval}")
    return value * multiplier


class TradeOhlcBuilder:
    """Aggregate incoming trades into OHLC bars of a custom interval.

    Usage:
        builder = TradeOhlcBuilder(client, interval="5m")
        builder.on_bar(my_handler)
        await builder.subscribe("BTC")
    """

    def __init__(self, client: Any, interval: str) -> None:  # HyperliquidWsClient
        self._client = client
        self._interval_ms = _parse_interval(interval)
        self._interval_str = interval
        self._bars: dict[str, OhlcBar] = {}  # coin -> current bar
        self._callbacks: list[OhlcBarCallback] = []
        client.on_message("trades", self._handle_trades)

    def on_bar(self, callback: OhlcBarCallback) -> None:
        """Register a callback invoked when an OHLC bar closes."""
        self._callbacks.append(callback)

    async def subscribe(self, coin: str) -> None:
        """Subscribe to trades for a coin."""
        coin_upper = coin.upper()
        await self._client.subscribe({"type": "trades", "coin": coin_upper})

    async def _handle_trades(self, data: Any) -> None:
        """Process incoming trade array and update OHLC bars."""
        if not isinstance(data, list):
            logger.warning("Unexpected trades data type: %s", type(data))
            return

        for raw in data:
            trade = WsTrade(
                coin=raw["coin"],
                side=raw["side"],
                px=raw["px"],
                sz=raw["sz"],
                hash=raw["hash"],
                time=raw["time"],
                tid=raw["tid"],
                users=(raw["users"][0], raw["users"][1]),
            )
            self._process_trade(trade)

    def _process_trade(self, trade: WsTrade) -> None:
        """Aggregate a single trade into the current bar for its coin."""
        coin = trade["coin"]
        price = float(trade["px"])
        size = float(trade["sz"])
        timestamp_ms = trade["time"]

        bucket_start = (timestamp_ms // self._interval_ms) * self._interval_ms
        bucket_end = bucket_start + self._interval_ms
        current = self._bars.get(coin)

        if current is None:
            bar = OhlcBar(bucket_start, bucket_end, price)
            bar.update(price, size)
            self._bars[coin] = bar
            return

        if bucket_start > current.open_time:
            # Bar closed — emit and start new
            closed = current
            asyncio_task = self._emit_bar(closed)
            # Best-effort: if there's an event loop, schedule it
            import asyncio
            try:
                loop = asyncio.get_running_loop()
                loop.create_task(asyncio_task)
            except RuntimeError:
                pass
            new_bar = OhlcBar(bucket_start, bucket_end, price)
            new_bar.update(price, size)
            self._bars[coin] = new_bar
        elif bucket_start == current.open_time:
            current.update(price, size)
        else:
            # Late trade (before current bar) — skip or log
            logger.debug("Late trade for %s: ts=%s < bar_start=%s", coin, timestamp_ms, current.open_time)

    async def _emit_bar(self, bar: OhlcBar) -> None:
        """Invoke all registered callbacks with the closed bar."""
        if bar.trades == 0:
            return
        for cb in self._callbacks:
            try:
                await cb(bar)
            except Exception:
                logger.exception("Bar callback raised")
