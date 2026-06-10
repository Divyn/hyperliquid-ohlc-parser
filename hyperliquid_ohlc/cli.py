"""CLI entry point for the Hyperliquid OHLC parser."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import signal
import sys
from datetime import datetime, timezone

from hyperliquid_ohlc.candle_parser import CandleStream
from hyperliquid_ohlc.types import Candle
from hyperliquid_ohlc.ws_client import HyperliquidWsClient

LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s %(message)s"


def _format_candle(candle: Candle) -> str:
    """Format a candle for JSON line output."""
    return json.dumps(
        {
            "coin": candle["s"],
            "interval": candle["i"],
            "open_time": datetime.fromtimestamp(
                candle["t"] / 1000, tz=timezone.utc
            ).isoformat(),
            "close_time": datetime.fromtimestamp(
                candle["T"] / 1000, tz=timezone.utc
            ).isoformat(),
            "open": candle["o"],
            "high": candle["h"],
            "low": candle["l"],
            "close": candle["c"],
            "volume": candle["v"],
            "trades": candle["n"],
        }
    )


async def _run(args: argparse.Namespace) -> None:
    """Stream candles, emitting one finalized bar per interval."""
    client = HyperliquidWsClient(network=args.network)
    stream = CandleStream(client)

    _current: dict[str, Candle] = {}  # coin -> in-progress candle

    async def on_candle(candle: Candle) -> None:
        coin = candle["s"]
        t = candle["t"]
        prev = _current.get(coin)

        if prev is not None and t != prev["t"]:
            # Bar rolled over — emit the finalized previous bar
            if prev["n"] > 0:
                print(_format_candle(prev), flush=True)
            _current[coin] = candle
        elif prev is None:
            _current[coin] = candle
        else:
            # Same bar — keep the latest snapshot
            _current[coin] = candle

    coins = [c.strip().upper() for c in args.coins.split(",")]
    for coin in coins:
        stream.on_candle(coin, args.interval, on_candle)

    await client.connect()
    for coin in coins:
        await stream.subscribe(coin, args.interval)

    logging.info(
        "Streaming candles for %s @ %s on %s", coins, args.interval, args.network
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
  hl-ohlc --coins PURR/USDC --interval 5m
        """,
    )
    parser.add_argument(
        "--coins",
        required=True,
        help="Comma-separated coin symbols (e.g. BTC,ETH,SOL)",
    )
    parser.add_argument(
        "--interval",
        required=True,
        help="OHLC interval: 1m,3m,5m,15m,30m,1h,2h,4h,8h,12h,1d,3d,1w,1M",
    )
    parser.add_argument(
        "--network",
        choices=("mainnet", "testnet"),
        default="mainnet",
        help="Hyperliquid network (default: mainnet)",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
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

    runner = _run(args)

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
