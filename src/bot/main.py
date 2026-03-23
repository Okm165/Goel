"""Application orchestrator.

Initializes and coordinates all bot components.
Manages the async event loop and graceful shutdown.
"""

from __future__ import annotations

import asyncio
import collections
import contextlib
import csv
import json
import logging
import signal
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

from bot.client import DeriveClient
from bot.config import ConfigError, ExecutorConfig, load_config
from bot.evaluator import Evaluator
from bot.executor import Executor
from bot.orderbook import OrderBook
from bot.risk import RiskManager
from bot.types import Opportunity, Quote, RejectedOpportunity, parse_instrument

if TYPE_CHECKING:
    from bot.config import Config

logger = logging.getLogger(__name__)


def setup_logging(level: str) -> None:
    """Configure application logging with structured format."""
    numeric_level = getattr(logging, level.upper(), logging.INFO)

    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s.%(msecs)03d | %(levelname)-8s | %(name)-20s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stderr,
    )

    logging.getLogger("websockets").setLevel(logging.WARNING)
    logging.getLogger("aiohttp").setLevel(logging.WARNING)


class Bot:
    """Main bot orchestrator."""

    __slots__ = (
        "_client",
        "_config",
        "_csv_file",
        "_csv_writer",
        "_evaluator",
        "_executor",
        "_found_count",
        "_opportunity_queue",
        "_orderbook",
        "_quote_queue",
        "_recent_opportunities",
        "_rejection_counts",
        "_risk_manager",
        "_running",
        "_start_time",
        "_tasks",
        "_valid_count",
    )

    def __init__(self, config: Config) -> None:
        """Initialize the bot with all components."""
        self._config = config

        self._quote_queue: asyncio.Queue[Quote] = asyncio.Queue(maxsize=10000)
        self._opportunity_queue: asyncio.Queue[Opportunity] = asyncio.Queue(maxsize=100)

        self._orderbook = OrderBook()
        self._client = DeriveClient(config, self._quote_queue)
        self._evaluator = Evaluator(config, self._orderbook, self._opportunity_queue)
        self._risk_manager = RiskManager(config, self._orderbook)

        self._executor: Executor | None = None
        if isinstance(config, ExecutorConfig):
            self._executor = Executor(config, self._risk_manager, self._opportunity_queue)

        self._csv_writer: csv.DictWriter[str] | None = None
        self._csv_file = None
        self._running = False
        self._tasks: list[asyncio.Task[None]] = []
        self._found_count = 0
        self._valid_count = 0
        self._start_time: datetime | None = None
        self._recent_opportunities: collections.deque[dict[str, object]] = collections.deque(
            maxlen=50
        )
        self._rejection_counts: dict[str, int] = {}

        logger.info(
            "[BOT] Initialized | mode=%s | underlyings=%s | min_profit=$%.2f",
            "EXECUTION" if self._executor else "OBSERVATION",
            ",".join(config.underlyings),
            float(config.min_profit_usd),
        )

    async def run(self) -> None:
        """Run the bot until stopped."""
        self._running = True
        self._start_time = datetime.now(UTC)
        self._setup_csv_output()

        mode = "EXECUTION" if self._executor else "OBSERVATION"
        logger.info("[BOT] Starting in %s mode", mode)

        self._tasks = [
            asyncio.create_task(self._client.connect(), name="client"),
            asyncio.create_task(self._quote_processor(), name="quote_processor"),
            asyncio.create_task(self._evaluator.run(), name="evaluator"),
            asyncio.create_task(self._stats_reporter(), name="stats"),
        ]

        if self._executor:
            self._tasks.append(asyncio.create_task(self._executor.run(), name="executor"))
        else:
            self._tasks.append(asyncio.create_task(self._opportunity_logger(), name="logger"))

        logger.info("[BOT] All tasks started | task_count=%d", len(self._tasks))

        try:
            await asyncio.gather(*self._tasks)
        except asyncio.CancelledError:
            logger.info("[BOT] Tasks cancelled")
        finally:
            await self._cleanup()

    async def stop(self) -> None:
        """Stop the bot gracefully."""
        if not self._running:
            return

        logger.info("[BOT] Graceful shutdown initiated")
        self._running = False

        await self._client.stop()

        if self._executor:
            await self._executor.stop()

        for task in self._tasks:
            if not task.done():
                task.cancel()
                logger.debug("[BOT] Cancelled task: %s", task.get_name())

        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)

        logger.info("[BOT] All tasks stopped")

    async def _quote_processor(self) -> None:
        """Process quotes from client and update orderbook."""
        logger.info("[QUOTE_PROCESSOR] Started")
        processed = 0

        while self._running:
            try:
                quote = await asyncio.wait_for(self._quote_queue.get(), timeout=1.0)
                self._orderbook.update(quote)
                processed += 1

                if processed % 1000 == 0:
                    logger.debug(
                        "[QUOTE_PROCESSOR] processed=%d | orderbook=%d | queue=%d",
                        processed,
                        self._orderbook.quote_count,
                        self._quote_queue.qsize(),
                    )

            except TimeoutError:
                continue
            except asyncio.CancelledError:
                break

        logger.info("[QUOTE_PROCESSOR] Stopped | total_processed=%d", processed)

    async def _opportunity_logger(self) -> None:
        """Log opportunities to CSV (observation mode)."""
        logger.info("[OPP_LOGGER] Started in observation mode")

        while self._running:
            try:
                opportunity = await asyncio.wait_for(self._opportunity_queue.get(), timeout=1.0)
                self._found_count += 1

                result = self._risk_manager.validate(opportunity)

                if isinstance(result, RejectedOpportunity):
                    name = result.reason.name
                    self._rejection_counts[name] = self._rejection_counts.get(name, 0) + 1
                else:
                    self._valid_count += 1
                    legs_str = "; ".join(
                        f"{leg.side.value} {leg.size} {leg.instrument} @ {leg.price}"
                        for leg in opportunity.legs
                    )
                    self._recent_opportunities.append(
                        {
                            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                            "arb_type": opportunity.arb_type.name,
                            "underlying": opportunity.underlying,
                            "expiry": opportunity.expiry,
                            "net_profit": float(opportunity.net_profit),
                            "gross_credit": float(opportunity.gross_credit),
                            "total_fees": float(opportunity.total_fees),
                            "guaranteed_floor": float(opportunity.guaranteed_floor),
                            "legs": legs_str,
                        }
                    )
                    self._log_opportunity(opportunity)
                    logger.info(
                        "[OPP_LOGGER] VALID | %s | %s | profit=$%.2f",
                        opportunity.arb_type.name,
                        opportunity.underlying,
                        float(opportunity.net_profit),
                    )

            except TimeoutError:
                continue
            except asyncio.CancelledError:
                break

        logger.info(
            "[OPP_LOGGER] Stopped | found=%d | validated=%d",
            self._found_count,
            self._valid_count,
        )

    async def _stats_reporter(self) -> None:
        """Periodically report system statistics and write live state file."""
        logger.info("[STATS] Reporter started")
        cycle = 0

        while self._running:
            await asyncio.sleep(5.0)

            if not self._running:
                break

            cycle += 1
            self._write_state()

            if cycle % 6 == 0:
                expiries = {
                    u: len(self._orderbook.get_expiries(u)) for u in self._config.underlyings
                }
                index_prices = {
                    u: float(self._orderbook.get_index_price(u) or 0)
                    for u in self._config.underlyings
                }
                spot_prices = {
                    u: float(self._orderbook.get_spot_price(u) or 0)
                    for u in self._config.underlyings
                }
                logger.info(
                    "[STATS] quotes=%d | queue=%d | expiries=%s",
                    self._orderbook.quote_count,
                    self._quote_queue.qsize(),
                    expiries,
                )
                logger.info(
                    "[STATS] index_prices=%s | spot_prices=%s",
                    index_prices,
                    spot_prices,
                )

        logger.info("[STATS] Reporter stopped")

    def _write_state(self) -> None:
        """Write live bot state to JSON file for dashboard consumption."""
        try:
            now = datetime.now(UTC)
            uptime_s = (now - self._start_time).total_seconds() if self._start_time else 0.0
            underlyings = list(self._config.underlyings)
            now_ms = int(time.time() * 1000)

            # Build live quotes snapshot: top 30 options by bid-ask spread % (market anomalies)
            quotes_snapshot: list[dict[str, object]] = []
            for q in self._orderbook.quotes:
                if q.bid > 0 and q.ask > 0 and q.mark > 0:
                    spread_usd = float(q.ask - q.bid)
                    spread_pct = spread_usd / float(q.mark) * 100
                    info = parse_instrument(q.instrument)
                    quotes_snapshot.append(
                        {
                            "instrument": q.instrument,
                            "underlying": info.underlying if info else "",
                            "expiry": info.expiry if info else "",
                            "strike": float(info.strike) if info else 0.0,
                            "type": info.option_type.value if info else "",
                            "bid": float(q.bid),
                            "ask": float(q.ask),
                            "spread_usd": round(spread_usd, 4),
                            "spread_pct": round(spread_pct, 2),
                            "mark": float(q.mark),
                            "iv_pct": round(float(q.iv) * 100, 1),
                            "delta": round(float(q.delta), 3),
                            "age_ms": now_ms - q.timestamp_ms,
                        }
                    )
            quotes_snapshot.sort(
                key=lambda x: x["spread_pct"] if isinstance(x["spread_pct"], float) else 0.0,
                reverse=True,
            )

            state: dict[str, object] = {
                "updated_at": now.isoformat(),
                "start_time": self._start_time.isoformat() if self._start_time else None,
                "uptime_s": uptime_s,
                "mode": "EXECUTION" if self._executor else "OBSERVATION",
                "underlyings": underlyings,
                "min_profit_usd": float(self._config.min_profit_usd),
                "quote_count": self._orderbook.quote_count,
                "quote_queue_size": self._quote_queue.qsize(),
                "opportunity_queue_size": self._opportunity_queue.qsize(),
                "scan_count": self._evaluator.scan_count,
                "found_count": self._found_count,
                "valid_count": self._valid_count,
                "index_prices": {
                    u: float(self._orderbook.get_index_price(u) or 0) for u in underlyings
                },
                "spot_prices": {
                    u: float(self._orderbook.get_spot_price(u) or 0) for u in underlyings
                },
                "perp_marks": {
                    u: float(self._orderbook.get_perp_mark(u) or 0) for u in underlyings
                },
                "perp_funding_rates": {
                    u: float(self._orderbook.get_perp_funding_rate(u) or 0) for u in underlyings
                },
                "expiry_counts": {u: len(self._orderbook.get_expiries(u)) for u in underlyings},
                "rejection_counts": dict(self._rejection_counts),
                "scan_history": self._evaluator.scan_history,
                "quotes_snapshot": quotes_snapshot[:30],
                "recent_opportunities": list(self._recent_opportunities),
            }

            state_path = Path(self._config.output_csv).with_name("bot_state.json")
            tmp_path = state_path.with_suffix(".tmp")
            tmp_path.write_text(json.dumps(state, indent=2))
            tmp_path.replace(state_path)

        except Exception:
            logger.debug("[STATS] Failed to write state file", exc_info=True)

    def _setup_csv_output(self) -> None:
        """Initialize CSV output file."""
        csv_path = Path(self._config.output_csv)
        file_exists = csv_path.exists()

        self._csv_file = csv_path.open("a", newline="")

        fieldnames = [
            "timestamp",
            "arb_type",
            "underlying",
            "expiry",
            "legs",
            "gross_credit",
            "total_fees",
            "net_profit",
            "guaranteed_floor",
        ]

        self._csv_writer = csv.DictWriter(self._csv_file, fieldnames=fieldnames)

        if not file_exists:
            self._csv_writer.writeheader()
            logger.info("[BOT] Created CSV: %s", csv_path)
        else:
            logger.info("[BOT] Appending to CSV: %s", csv_path)

    def _log_opportunity(self, opp: Opportunity) -> None:
        """Write opportunity to CSV."""
        if not self._csv_writer:
            return

        legs_str = "; ".join(
            f"{leg.side.value} {leg.size} {leg.instrument} @ {leg.price}" for leg in opp.legs
        )

        self._csv_writer.writerow(
            {
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "arb_type": opp.arb_type.name,
                "underlying": opp.underlying,
                "expiry": opp.expiry,
                "legs": legs_str,
                "gross_credit": f"{opp.gross_credit:.4f}",
                "total_fees": f"{opp.total_fees:.4f}",
                "net_profit": f"{opp.net_profit:.4f}",
                "guaranteed_floor": f"{opp.guaranteed_floor:.2f}",
            }
        )

        if self._csv_file:
            self._csv_file.flush()

    async def _cleanup(self) -> None:
        """Clean up resources."""
        if self._csv_file:
            self._csv_file.close()

        logger.info("[BOT] Cleanup complete")


async def async_main() -> int:
    """Async entry point."""
    try:
        config = load_config()
    except ConfigError:
        logger.exception("[MAIN] Configuration error")
        return 1

    setup_logging(config.log_level)

    mode = "EXECUTION" if isinstance(config, ExecutorConfig) else "OBSERVATION"

    logger.info("=" * 60)
    logger.info("[MAIN] Derive HFT Bot v0.1.0")
    logger.info("[MAIN] Mode: %s", mode)
    logger.info("[MAIN] Underlyings: %s", ", ".join(config.underlyings))
    logger.info("[MAIN] Min profit: $%.2f", float(config.min_profit_usd))
    logger.info("=" * 60)

    bot = Bot(config)

    loop = asyncio.get_running_loop()
    shutdown_event = asyncio.Event()

    def signal_handler() -> None:
        logger.info("[MAIN] Shutdown signal received (SIGINT/SIGTERM)")
        shutdown_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, signal_handler)

    bot_task = asyncio.create_task(bot.run())

    shutdown_waiter = asyncio.create_task(shutdown_event.wait())

    done, pending = await asyncio.wait(
        [bot_task, shutdown_waiter],
        return_when=asyncio.FIRST_COMPLETED,
    )

    if shutdown_waiter in done:
        logger.info("[MAIN] Initiating graceful shutdown")
        await bot.stop()
        bot_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await bot_task

    for task in pending:
        task.cancel()

    logger.info("[MAIN] Bot terminated")
    return 0


def main() -> int:
    """Synchronous entry point."""
    return asyncio.run(async_main())
