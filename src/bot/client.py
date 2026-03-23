"""Network gateway for Derive/Lyra Protocol.

Handles WebSocket connectivity, instrument fetching, and message parsing.
Implements reconnection with exponential backoff.

API Reference: https://docs.derive.xyz/reference/
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Final

import aiohttp
import websockets
from websockets.exceptions import ConnectionClosed

from bot.types import Quote

if TYPE_CHECKING:
    from websockets.asyncio.client import ClientConnection

    from bot.config import Config

logger = logging.getLogger(__name__)

# =============================================================================
# Constants - Lyra API
# =============================================================================

WS_URL: Final[str] = "wss://api.lyra.finance/ws"
REST_URL: Final[str] = "https://api.lyra.finance"

RECONNECT_BASE_DELAY: Final[float] = 1.0
RECONNECT_MAX_DELAY: Final[float] = 60.0
RECONNECT_MULTIPLIER: Final[float] = 2.0
HEARTBEAT_INTERVAL: Final[float] = 20.0
PING_TIMEOUT: Final[float] = 10.0
THROUGHPUT_LOG_INTERVAL: Final[float] = 10.0


class DeriveClient:
    """Async client for Derive/Lyra Protocol WebSocket API."""

    __slots__ = (
        "_config",
        "_instruments",
        "_last_message_time",
        "_quote_queue",
        "_running",
        "_subscriptions",
        "_ws",
    )

    def __init__(
        self,
        config: Config,
        quote_queue: asyncio.Queue[Quote],
    ) -> None:
        """Initialize the client."""
        self._config = config
        self._quote_queue = quote_queue
        self._ws: ClientConnection | None = None
        self._subscriptions: set[str] = set()
        self._instruments: list[str] = []
        self._running = False
        self._last_message_time: float = 0.0

    async def connect(self) -> None:
        """Connect and maintain WebSocket connection with exponential backoff."""
        self._running = True
        delay = RECONNECT_BASE_DELAY

        await self._fetch_instruments()

        logger.info("[CLIENT] Starting | url=%s | instruments=%d", WS_URL, len(self._instruments))

        while self._running:
            try:
                logger.info("[CLIENT] Connecting | delay=%.1fs", delay)

                async with websockets.connect(
                    WS_URL,
                    ping_interval=HEARTBEAT_INTERVAL,
                    ping_timeout=PING_TIMEOUT,
                    close_timeout=5.0,
                    max_size=2**20,
                ) as ws:
                    self._ws = ws
                    delay = RECONNECT_BASE_DELAY

                    logger.info("[CLIENT] Connected | channels=%d", len(self._instruments))

                    await self._subscribe_all()
                    await self._listen()

            except ConnectionClosed as e:
                logger.warning("[CLIENT] Connection closed | code=%s", e.code)
            except OSError as e:
                logger.warning("[CLIENT] Network error | error=%s", e)
            except asyncio.CancelledError:
                logger.info("[CLIENT] Cancelled")
                break
            finally:
                self._ws = None

            if self._running:
                logger.info("[CLIENT] Reconnecting in %.1fs", delay)
                await asyncio.sleep(delay)
                delay = min(delay * RECONNECT_MULTIPLIER, RECONNECT_MAX_DELAY)

        logger.info("[CLIENT] Stopped")

    async def stop(self) -> None:
        """Stop the client."""
        logger.info("[CLIENT] Stop requested")
        self._running = False
        if self._ws:
            await self._ws.close()

    async def subscribe_instruments(self, _patterns: list[str]) -> None:
        """Subscribe to ticker updates (instruments already fetched)."""
        if self._ws and self._instruments:
            await self._subscribe_all()

    async def _fetch_instruments(self) -> None:
        """Fetch available instruments from REST API."""
        self._instruments = []

        async with aiohttp.ClientSession() as session:
            for currency in self._config.underlyings:
                try:
                    instruments = await self._fetch_currency_instruments(session, currency)
                    self._instruments.extend(instruments)
                    logger.info("[CLIENT] Fetched %d %s options", len(instruments), currency)
                except Exception:
                    logger.exception("[CLIENT] Failed to fetch %s instruments", currency)

        logger.info("[CLIENT] Total instruments: %d", len(self._instruments))

    async def _fetch_currency_instruments(
        self,
        session: aiohttp.ClientSession,
        currency: str,
    ) -> list[str]:
        """Fetch option instruments for a currency."""
        request_data = {
            "jsonrpc": "2.0",
            "id": int(time.time() * 1000),
            "method": "public/get_instruments",
            "params": {
                "currency": currency,
                "expired": False,
                "instrument_type": "option",
            },
        }

        async with session.post(
            f"{REST_URL}/public/get_instruments",
            json=request_data,
        ) as response:
            data = await response.json()

            if "error" in data:
                logger.warning("[CLIENT] API error: %s", data["error"])
                return []

            result = data.get("result", [])
            return [inst["instrument_name"] for inst in result if inst.get("is_active")]

    async def _subscribe_all(self) -> None:
        """Subscribe to all instrument tickers."""
        if not self._ws or not self._instruments:
            return

        channels = [f"ticker.{inst}.100ms" for inst in self._instruments]

        for i in range(0, len(channels), 50):
            batch = channels[i : i + 50]
            await self._subscribe(batch)
            await asyncio.sleep(0.1)

        logger.info("[CLIENT] Subscribed to %d channels", len(channels))

    async def _subscribe(self, channels: list[str]) -> None:
        """Send subscription request."""
        if not self._ws:
            return

        request = {
            "jsonrpc": "2.0",
            "id": int(time.time() * 1000),
            "method": "subscribe",
            "params": {"channels": channels},
        }

        await self._ws.send(json.dumps(request))
        self._subscriptions.update(channels)

    async def _listen(self) -> None:
        """Listen for incoming messages."""
        if not self._ws:
            return

        message_count = 0
        last_log_time = time.monotonic()

        async for raw_msg in self._ws:
            self._last_message_time = time.monotonic()
            message_count += 1

            text = raw_msg.decode("utf-8") if isinstance(raw_msg, bytes) else raw_msg

            if self._last_message_time - last_log_time > THROUGHPUT_LOG_INTERVAL:
                logger.debug("[CLIENT] Messages: %d in 10s", message_count)
                message_count = 0
                last_log_time = self._last_message_time

            try:
                self._handle_message(text)
            except Exception:
                logger.exception("[CLIENT] Error handling message")

    def _handle_message(self, raw: str) -> None:
        """Parse and dispatch incoming message."""
        data: dict[str, Any] = json.loads(raw)

        method = data.get("method")
        if method != "subscription":
            return

        params = data.get("params", {})
        channel = params.get("channel", "")

        if channel.startswith("ticker."):
            self._handle_ticker(channel, params.get("data", {}))

    def _handle_ticker(self, channel: str, data: dict[str, Any]) -> None:
        """Parse ticker data into Quote and push to queue.

        TickerSlim format:
        - a: best ask price
        - b: best bid price
        - A: ask amount
        - B: bid amount
        - M: mark price
        - t: timestamp
        - option_pricing.i: IV
        - option_pricing.d: delta
        """
        parts = channel.split(".")
        if len(parts) < 2:
            return

        instrument = parts[1]
        option_pricing: dict[str, Any] = data.get("option_pricing") or {}

        try:
            quote = Quote(
                instrument=instrument,
                bid=Decimal(str(data.get("b", "0"))),
                bid_size=Decimal(str(data.get("B", "0"))),
                ask=Decimal(str(data.get("a", "0"))),
                ask_size=Decimal(str(data.get("A", "0"))),
                mark=Decimal(str(data.get("M", "0"))),
                iv=Decimal(str(option_pricing.get("i", "0"))),
                delta=Decimal(str(option_pricing.get("d", "0"))),
                timestamp_ms=int(data.get("t", 0)),
            )

            logger.debug("[CLIENT] Quote | %s | bid=%s ask=%s", instrument, quote.bid, quote.ask)

            try:
                self._quote_queue.put_nowait(quote)
            except asyncio.QueueFull:
                logger.warning("[CLIENT] Queue full | dropping oldest")
                try:
                    self._quote_queue.get_nowait()
                    self._quote_queue.put_nowait(quote)
                except asyncio.QueueEmpty:
                    pass

        except (ValueError, TypeError) as e:
            logger.debug("[CLIENT] Invalid ticker | error=%s", e)
