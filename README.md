# HYPERLIQUID OHLC in REAL_TIME

Real-time OHLC parser for [Hyperliquid](https://hyperliquid.xyz) using WebSocket streams.

Two modes of operation:

- **Candle mode** — streams pre-computed OHLC from Hyperliquid's built-in `candle` feed (supports standard intervals: `1m` through `1M`)
- **Trades mode** — aggregates raw trades locally into custom OHLC bars (supports any interval, e.g. `10s`, `2m`)

## Installation

```bash
git clone https://github.com/divyn/hyperliquid-ohlc-parser.git
cd hyperliquid-ohlc-parser
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

## Quick start

```bash
# Stream 1-minute candles for BTC and ETH
hl-ohlc --coins BTC,ETH --interval 1m

# Stream 5-minute candles on testnet
hl-ohlc --coins SOL --interval 5m --network testnet

# Build 10-second OHLC bars from raw trades
hl-ohlc --coins BTC --interval 10s --mode trades
```

Output is newline-delimited JSON, one bar per line:

```json
{"coin": "BTC", "interval": "1m", "open_time": "...", "close_time": "...", "open": 98750.0, "high": 98800.0, "low": 98730.0, "close": 98780.0, "volume": 12.45, "trades": 8}
```

## Programmatic usage

```python
import asyncio
from hyperliquid_ohlc import HyperliquidWsClient, CandleStream

async def main():
    client = HyperliquidWsClient(network="mainnet")
    stream = CandleStream(client)

    async def on_candle(candle):
        print(f"{candle['s']} | O: {candle['o']} H: {candle['h']} "
              f"L: {candle['l']} C: {candle['c']} V: {candle['v']}")

    stream.on_candle("BTC", "1m", on_candle)

    await client.connect()
    await stream.subscribe("BTC", "1m")
    await client.run_forever()

asyncio.run(main())
```

### Building custom-interval OHLC from trades

```python
from hyperliquid_ohlc import HyperliquidWsClient, TradeOhlcBuilder

client = HyperliquidWsClient()
builder = TradeOhlcBuilder(client, interval="10s")

async def on_bar(bar):
    print(f"OHLC {bar.open_time}-{bar.close_time}: "
          f"O={bar.open} C={bar.close} V={bar.volume}")

builder.on_bar(on_bar)

await client.connect()
await builder.subscribe("BTC")
await client.run_forever()
```

## CLI reference

```
hl-ohlc --coins COINS --interval INTERVAL [OPTIONS]
```

| Option | Description | Default |
|---|---|---|
| `--coins` | Comma-separated coin symbols (e.g. `BTC,ETH,SOL`) | *required* |
| `--interval` | OHLC interval — standard intervals for candle mode, any custom for trades mode | *required* |
| `--mode` | `candle` (built-in feed) or `trades` (local aggregation) | `candle` |
| `--network` | `mainnet` or `testnet` | `mainnet` |
| `--log-level` | `DEBUG`, `INFO`, `WARNING`, or `ERROR` | `INFO` |

### Supported intervals (candle mode)

`1m` `3m` `5m` `15m` `30m` `1h` `2h` `4h` `8h` `12h` `1d` `3d` `1w` `1M`

### Trades mode intervals

Any custom interval string ending in `s`, `m`, `h`, `d`, or `w` (e.g. `10s`, `2m`, `4h`, `1d`).

## API overview

| Class | Purpose |
|---|---|
| `HyperliquidWsClient` | WebSocket connection manager with auto-reconnect |
| `CandleStream` | Subscribe to built-in candle feed with per-coin callbacks |
| `TradeOhlcBuilder` | Aggregate raw trades into custom-interval OHLC bars |
| `Candle` | TypedDict for a candle from the `candle` subscription |
| `WsTrade` | TypedDict for a trade from the `trades` subscription |
| `WsBook` | TypedDict for order book snapshot (`l2Book`) |
| `WsBbo` | TypedDict for best bid/offer (`bbo`) |
| `OhlcBar` | Computed OHLC bar with `.update(price, size)` aggregation |

## Running tests

```bash
pip install -e ".[dev]"
pytest
```

## Dependencies

- `websockets >= 12.0`
- Python 3.10+
- No API key required — streams public market data over WebSocket
