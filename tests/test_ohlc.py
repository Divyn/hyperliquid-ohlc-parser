"""Tests for hyperliquid-ohlc types and aggregation logic."""

from __future__ import annotations

import pytest

from hyperliquid_ohlc.types import OhlcBar
from hyperliquid_ohlc.trade_parser import _parse_interval, TradeOhlcBuilder


class TestOhlcBar:
    def test_initial_state(self) -> None:
        bar = OhlcBar(60000, 120000, 50000.0)
        assert bar.open == bar.high == bar.low == bar.close == 50000.0
        assert bar.volume == 0.0
        assert bar.trades == 0

    def test_single_trade(self) -> None:
        bar = OhlcBar(60000, 120000, 50000.0)
        bar.update(50100.0, 2.0)
        assert bar.open == 50000.0
        assert bar.high == 50100.0
        assert bar.low == 50000.0
        assert bar.close == 50100.0
        assert bar.volume == 2.0
        assert bar.trades == 1

    def test_multiple_trades(self) -> None:
        bar = OhlcBar(60000, 120000, 50000.0)
        bar.update(50100.0, 1.0)
        bar.update(50200.0, 1.5)
        bar.update(49900.0, 0.5)
        assert bar.high == 50200.0
        assert bar.low == 49900.0
        assert bar.close == 49900.0
        assert bar.volume == 3.0
        assert bar.trades == 3

    def test_to_dict(self) -> None:
        bar = OhlcBar(60000, 120000, 50000.0)
        bar.update(50100.0, 1.0)
        d = bar.to_dict("BTC", "1m")
        assert d["s"] == "BTC"
        assert d["i"] == "1m"
        assert d["t"] == 60000
        assert d["T"] == 120000
        assert d["o"] == 50000.0
        assert d["h"] == 50100.0
        assert d["l"] == 50000.0
        assert d["c"] == 50100.0
        assert d["v"] == 1.0
        assert d["n"] == 1


class TestParseInterval:
    def test_minutes(self) -> None:
        assert _parse_interval("1m") == 60_000
        assert _parse_interval("5m") == 300_000
        assert _parse_interval("15m") == 900_000

    def test_hours(self) -> None:
        assert _parse_interval("1h") == 3_600_000
        assert _parse_interval("4h") == 14_400_000

    def test_days(self) -> None:
        assert _parse_interval("1d") == 86_400_000

    def test_seconds(self) -> None:
        assert _parse_interval("10s") == 10_000
        assert _parse_interval("30s") == 30_000

    def test_invalid(self) -> None:
        with pytest.raises(ValueError):
            _parse_interval("1x")
        with pytest.raises(ValueError):
            _parse_interval("0m")
        with pytest.raises(ValueError):
            _parse_interval("")
