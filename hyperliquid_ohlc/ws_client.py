"""WebSocket connection manager for Hyperliquid."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Callable, Awaitable
from typing import Any

import websockets
import websockets.exceptions
import websockets.asyncio.client

logger = logging.getLogger(__name__)

# Network URLs
MAINNET_WS_URL = "wss://api.hyperliquid.xyz/ws"
TESTNET_WS_URL = "wss://api.hyperliquid-testnet.xyz/ws"

# Reconnect parameters
RECONNECT_DELAY_BASE = 1.0  # seconds
RECONNECT_DELAY_MAX = 60.0


type MessageHandler = Callable[[dict[str, Any]], Awaitable[None]]


class HyperliquidWsClient:
    """Manages a WebSocket connection to Hyperliquid with auto-reconnect.

    Usage:
        client = HyperliquidWsClient(network="mainnet")
        client.on_message("trades", handle_trade)
        await client.connect()
        await client.subscribe({"type": "trades", "coin": "BTC"})
        await client.run_forever()
    """

    def __init__(self, network: str = "mainnet") -> None:
        if network == "mainnet":
            self._url = MAINNET_WS_URL
        elif network == "testnet":
            self._url = TESTNET_WS_URL
        else:
            raise ValueError(f"Unknown network: {network}. Use 'mainnet' or 'testnet'.")

        self._ws: websockets.asyncio.client.ClientConnection | None = None
        self._handlers: dict[str, list[MessageHandler]] = {}
        self._subscriptions: list[dict[str, Any]] = []
        self._running = False
        self._reconnect_delay = RECONNECT_DELAY_BASE

    def on_message(self, channel: str, handler: MessageHandler) -> None:
        """Register an async handler for a channel (e.g. 'trades', 'candle', 'l2Book')."""
        self._handlers.setdefault(channel, []).append(handler)

    async def connect(self) -> None:
        """Establish the WebSocket connection."""
        logger.info("Connecting to %s", self._url)
        self._ws = await websockets.asyncio.client.connect(
            self._url,
            ping_interval=30,
            ping_timeout=10,
            close_timeout=5,
        )
        self._reconnect_delay = RECONNECT_DELAY_BASE
        logger.info("Connected.")

    async def subscribe(self, subscription: dict[str, Any]) -> None:
        """Send a subscription request over the WebSocket."""
        msg = {"method": "subscribe", "subscription": subscription}
        await self._send(msg)
        self._subscriptions.append(subscription)
        logger.info("Subscribed: %s", subscription)

    async def unsubscribe(self, subscription: dict[str, Any]) -> None:
        """Send an unsubscribe request."""
        msg = {"method": "unsubscribe", "subscription": subscription}
        await self._send(msg)
        if subscription in self._subscriptions:
            self._subscriptions.remove(subscription)
        logger.info("Unsubscribed: %s", subscription)

    async def run_forever(self) -> None:
        """Run the client with automatic reconnection. Blocks until stop() is called."""
        self._running = True
        while self._running:
            try:
                await self._drain_messages()
            except (
                websockets.exceptions.ConnectionClosed,
                OSError,
                asyncio.TimeoutError,
            ) as exc:
                if not self._running:
                    break
                logger.warning("Disconnected: %s. Reconnecting in %.1fs...", exc, self._reconnect_delay)
                await asyncio.sleep(self._reconnect_delay)
                self._reconnect_delay = min(self._reconnect_delay * 1.5, RECONNECT_DELAY_MAX)
                try:
                    await self.connect()
                    for sub in self._subscriptions:
                        await self.subscribe(sub)
                except OSError as retry_exc:
                    logger.warning("Reconnect failed: %s", retry_exc)

    async def stop(self) -> None:
        """Stop the client gracefully."""
        self._running = False
        if self._ws is not None:
            await self._ws.close()
            self._ws = None

    async def _send(self, msg: dict[str, Any]) -> None:
        """Send a JSON message. Raises if not connected."""
        if self._ws is None:
            raise RuntimeError("Not connected. Call connect() first.")
        await self._ws.send(json.dumps(msg))

    async def _drain_messages(self) -> None:
        """Read messages from the WebSocket and dispatch to handlers."""
        if self._ws is None:
            raise RuntimeError("Not connected.")
        async for raw in self._ws:
            try:
                msg: dict[str, Any] = json.loads(raw)
            except json.JSONDecodeError:
                logger.warning("Unparseable message: %s", raw[:200])
                continue

            channel = msg.get("channel", "")
            if channel == "subscriptionResponse":
                logger.debug("Subscription ack: %s", msg.get("data"))
                continue

            data = msg.get("data")
            if data is None:
                continue

            for handler in self._handlers.get(channel, []):
                try:
                    await handler(data)
                except Exception:
                    logger.exception("Handler for channel '%s' raised", channel)
