"""CLI entry point for the Hyperliquid OHLC parser."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import signal
import sys
from datetime import datetime, timezone

from hyperliquid_ohlc.ws_client import HyperliquidWsClient
from hyperliquid_ohlc.candle_parser import CandleStream
from hyperliquid_ohlc.trade_parser import TradeOhlcBuilder
from hyperliquid_ohlc.types import Candle, OhlcBar

LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s %(message)s"


def _format_candle_json(candle: Candle) -> str:
    """Format a candle for JSON line output."""
    return json.dumps({
        "coin": candle["s"],
        "interval": candle["i"],
        "open_time": datetime.fromtimestamp(candle["t"] / 1000, tz=timezone.utc).isoformat(),
        "close_time": datetime.fromtimestamp(candle["T"] / 1000, tz=timezone.utc).isoformat(),
        "open": candle["o"],
        "high": candle["h"],
        "low": candle["l"],
        "close": candle["c"],
        "volume": candle["v"],
        "trades": candle["n"],
    })


def _format_bar_json(bar: OhlcBar) -> str:
    """Format a bar for JSON line output."""
    return json.dumps({
        "coin": getattr(bar, "coin", "?"),
        "interval": getattr(bar, "interval", "?"),
        "open_time": datetime.fromtimestamp(bar.open_time / 1000, tz=timezone.utc).isoformat(),
        "close_time": datetime.fromtimestamp(bar.close_time / 1000, tz=timezone.utc).isoformat(),
        "open": bar.open,
        "high": bar.high,
        "low": bar.low,
        "close": bar.close,
        "volume": bar.volume,
        "trades": bar.trades,
    })


async def _run_candle_mode(args: argparse.Namespace) -> None:
    """Run in built-in candle subscription mode."""
    client = HyperliquidWsClient(network=args.network)
    stream = CandleStream(client)

    async def print_candle(candle: Candle) -> None:
        if candle["n"] > 0:  # only print candles that have trades
            print(_format_candle_json(candle), flush=True)

    coins = [c.strip().upper() for c in args.coins.split(",")]
    for coin in coins:
        stream.on_candle(coin, args.interval, print_candle)

    await client.connect()
    for coin in coins:
        await stream.subscribe(coin, args.interval)

    logging.info("Streaming candles for %s @ %s on %s", coins, args.interval, args.network)
    await client.run_forever()


async def _run_trades_mode(args: argparse.Namespace) -> None:
    """Run in trade-aggregation mode (custom OHLC from raw trades)."""
    client = HyperliquidWsClient(network=args.network)
    builder = TradeOhlcBuilder(client, interval=args.interval)

    async def print_bar(bar: OhlcBar) -> None:
        bar.coin = getattr(bar, "coin", "?")  # type: ignore[attr-defined]
        bar.interval = builder._interval_str  # type: ignore[attr-defined]
        print(_format_bar_json(bar), flush=True)

    builder.on_bar(print_bar)

    coins = [c.strip().upper() for c in args.coins.split(",")]
    await client.connect()
    for coin in coins:
        await builder.subscribe(coin)

    logging.info(
        "Building %s OHLC from trades for %s on %s",
        args.interval, coins, args.network,
    )
    await client.run_forever()


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Hyperliquid real-time OHLC parser",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  hl-ohlc --coins BTC,ETH --interval 1m
  hl-ohlc --coins SOL --interval 15m --network testnet
  hl-ohlc --coins BTC --interval 10s --mode trades
  hl-ohlc --coins PURR/USDC --interval 5m
        """,
    )
    parser.add_argument(
        "--coins", required=True,
        help="Comma-separated coin symbols (e.g. BTC,ETH,SOL)",
    )
    parser.add_argument(
        "--interval", required=True,
        help="OHLC interval. For candle mode: 1m,3m,5m,15m,30m,1h,2h,4h,8h,12h,1d,3d,1w,1M. For trades mode: any custom (e.g. 10s, 2m)",
    )
    parser.add_argument(
        "--mode", choices=("candle", "trades"), default="candle",
        help="candle = built-in feed (default). trades = aggregate raw trades.",
    )
    parser.add_argument(
        "--network", choices=("mainnet", "testnet"), default="mainnet",
        help="Hyperliquid network (default: mainnet)",
    )
    parser.add_argument(
        "--log-level", default="INFO",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
        help="Logging level (default: INFO)",
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format=LOG_FORMAT,
        stream=sys.stderr,
    )

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    if args.mode == "candle":
        runner = _run_candle_mode(args)
    else:
        runner = _run_trades_mode(args)

    # Graceful shutdown on SIGINT/SIGTERM
    async def _run_with_signals() -> None:
        try:
            await runner
        except asyncio.CancelledError:
            pass

    main_task = loop.create_task(_run_with_signals())

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, main_task.cancel)

    try:
        loop.run_until_complete(main_task)
    except KeyboardInterrupt:
        pass
    finally:
        loop.close()


if __name__ == "__main__":
    main()
