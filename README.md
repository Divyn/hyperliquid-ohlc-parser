# hyperliquid-ohlc

Real-time OHLC streamer for [Hyperliquid](https://hyperliquid.xyz) using the WebSocket candle feed.

Subscribes to Hyperliquid's built-in `candle` stream, accumulates progressive snapshots, and emits one finalized bar per interval — no duplicate updates.

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

# 15-minute candles on testnet
hl-ohlc --coins SOL --interval 15m --network testnet
```

Output is newline-delimited JSON, one bar per interval:

```json
{"coin": "BTC", "interval": "1m", "open_time": "2026-06-10T08:38:00+00:00", "close_time": "2026-06-10T08:38:59.999000+00:00", "open": 61392.0, "high": 61427.0, "low": 61386.0, "close": 61387.0, "volume": 19.84521, "trades": 205}
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

## CLI reference

```
hl-ohlc --coins COINS --interval INTERVAL [OPTIONS]
```

| Option | Description | Default |
|---|---|---|
| `--coins` | Comma-separated coin symbols (e.g. `BTC,ETH,SOL`) | *required* |
| `--interval` | OHLC interval | *required* |
| `--network` | `mainnet` or `testnet` | `mainnet` |
| `--log-level` | `DEBUG`, `INFO`, `WARNING`, or `ERROR` | `INFO` |

### Supported intervals

`1m` `3m` `5m` `15m` `30m` `1h` `2h` `4h` `8h` `12h` `1d` `3d` `1w` `1M`

## Running tests

```bash
pip install -e ".[dev]"
pytest
```

## Dependencies

- `websockets >= 12.0`
- Python 3.10+
- No API key required
