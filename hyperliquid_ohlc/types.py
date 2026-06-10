"""Type definitions matching Hyperliquid WebSocket data schemas."""

from __future__ import annotations

from typing import TypedDict


class Candle(TypedDict):
    """OHLC candle from the `candle` subscription."""

    t: int  # open time millis
    T: int  # close time millis
    s: str  # coin symbol
    i: str  # interval (1m, 5m, 15m, 1h, etc.)
    o: float  # open
    c: float  # close
    h: float  # high
    l: float  # low
    v: float  # volume (base units)
    n: int  # number of trades


class WsTrade(TypedDict):
    """Individual trade from the `trades` subscription."""

    coin: str
    side: str  # "A" (ask/sell) or "B" (bid/buy)
    px: str  # price as string
    sz: str  # size as string
    hash: str  # L1 transaction hash
    time: int  # unix millis
    tid: int  # 50-bit hash of (buyer_oid, seller_oid)
    users: tuple[str, str]  # [buyer, seller]


class WsLevel(TypedDict):
    """Single order book level."""

    px: str  # price
    sz: str  # size
    n: int  # number of orders


class WsBook(TypedDict):
    """Order book snapshot from the `l2Book` subscription."""

    coin: str
    levels: tuple[list[WsLevel], list[WsLevel]]  # [bids, asks]
    time: int


class WsBbo(TypedDict):
    """Best bid/offer from the `bbo` subscription."""

    coin: str
    time: int
    bbo: tuple[WsLevel | None, WsLevel | None]  # [bid, ask]


class OhlcBar:
    """Computed OHLC bar with helper methods."""

    __slots__ = ("open", "high", "low", "close", "volume", "trades", "open_time", "close_time")

    def __init__(
        self,
        open_time: int,
        close_time: int,
        open_price: float,
    ) -> None:
        self.open_time = open_time
        self.close_time = close_time
        self.open = open_price
        self.high = open_price
        self.low = open_price
        self.close = open_price
        self.volume = 0.0
        self.trades = 0

    def update(self, price: float, size: float) -> None:
        """Incorporate a trade into the bar."""
        self.close = price
        if price > self.high:
            self.high = price
        if price < self.low:
            self.low = price
        self.volume += size
        self.trades += 1

    def to_dict(self, coin: str, interval: str) -> dict:
        """Serialize to a dict matching the Candle format."""
        return {
            "s": coin,
            "i": interval,
            "t": self.open_time,
            "T": self.close_time,
            "o": self.open,
            "h": self.high,
            "l": self.low,
            "c": self.close,
            "v": self.volume,
            "n": self.trades,
        }
