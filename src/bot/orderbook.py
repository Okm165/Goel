"""In-memory order book state management.

Maintains real-time market state with O(1) lookups.
Thread-safe for concurrent access from multiple coroutines.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

from bot.types import OptionType, Quote, parse_instrument

if TYPE_CHECKING:
    from decimal import Decimal

logger = logging.getLogger(__name__)


class OrderBook:
    """In-memory cache of market quotes.

    Provides fast O(1) lookups by instrument name and efficient
    iteration over all quotes for a given underlying/expiry.

    Thread-safe for use with asyncio coroutines.
    """

    __slots__ = (
        "_expiries",
        "_quotes",
        "_strikes",
        "_update_event",
    )

    def __init__(self) -> None:
        """Initialize empty order book."""
        self._quotes: dict[str, Quote] = {}
        self._expiries: dict[str, set[str]] = {}
        self._strikes: dict[str, set[Decimal]] = {}
        self._update_event = asyncio.Event()

    def update(self, quote: Quote) -> None:
        """Update quote in the cache.

        Args:
            quote: New quote data
        """
        self._quotes[quote.instrument] = quote

        info = parse_instrument(quote.instrument)
        if info:
            underlying = info.underlying

            if underlying not in self._expiries:
                self._expiries[underlying] = set()
            self._expiries[underlying].add(info.expiry)

            expiry_key = f"{underlying}:{info.expiry}"
            if expiry_key not in self._strikes:
                self._strikes[expiry_key] = set()
            self._strikes[expiry_key].add(info.strike)

        self._update_event.set()

    def get(self, instrument: str) -> Quote | None:
        """Get quote by instrument name.

        Args:
            instrument: Instrument name (e.g., "ETH-20260401-3500-C")

        Returns:
            Quote if found, None otherwise
        """
        return self._quotes.get(instrument)

    def get_pair(
        self,
        underlying: str,
        expiry: str,
        strike: Decimal,
    ) -> tuple[Quote | None, Quote | None]:
        """Get call and put quotes for a strike.

        Args:
            underlying: Asset symbol
            expiry: Expiry date (YYYYMMDD)
            strike: Strike price

        Returns:
            Tuple of (call_quote, put_quote), either may be None
        """
        call_name = f"{underlying}-{expiry}-{strike}-C"
        put_name = f"{underlying}-{expiry}-{strike}-P"
        return self._quotes.get(call_name), self._quotes.get(put_name)

    def get_option(
        self,
        underlying: str,
        expiry: str,
        strike: Decimal,
        option_type: OptionType,
    ) -> Quote | None:
        """Get quote for specific option.

        Args:
            underlying: Asset symbol
            expiry: Expiry date
            strike: Strike price
            option_type: Call or Put

        Returns:
            Quote if found
        """
        name = f"{underlying}-{expiry}-{strike}-{option_type.value}"
        return self._quotes.get(name)

    def get_expiries(self, underlying: str) -> list[str]:
        """Get all available expiries for an underlying.

        Args:
            underlying: Asset symbol

        Returns:
            Sorted list of expiry dates
        """
        expiries = self._expiries.get(underlying, set())
        return sorted(expiries)

    def get_strikes(self, underlying: str, expiry: str) -> list[Decimal]:
        """Get all available strikes for an underlying/expiry.

        Args:
            underlying: Asset symbol
            expiry: Expiry date

        Returns:
            Sorted list of strikes
        """
        key = f"{underlying}:{expiry}"
        strikes = self._strikes.get(key, set())
        return sorted(strikes)

    def get_spot_price(self, underlying: str) -> Decimal | None:
        """Get current spot price for underlying.

        Derives from mark price of near-ATM option.

        Args:
            underlying: Asset symbol

        Returns:
            Spot price estimate or None
        """
        for instrument, quote in self._quotes.items():
            if instrument.startswith(f"{underlying}-") and quote.mark > 0:
                info = parse_instrument(instrument)
                if info:
                    if info.option_type == OptionType.CALL:
                        return info.strike + quote.mark
                    return info.strike - quote.mark
        return None

    async def wait_for_update(self) -> None:
        """Wait for the next quote update.

        Efficient mechanism for scanners to trigger on new data.
        """
        self._update_event.clear()
        await self._update_event.wait()

    def clear(self) -> None:
        """Clear all cached data."""
        self._quotes.clear()
        self._expiries.clear()
        self._strikes.clear()

    @property
    def quote_count(self) -> int:
        """Number of quotes in cache."""
        return len(self._quotes)

    @property
    def instruments(self) -> list[str]:
        """List of all cached instrument names."""
        return list(self._quotes.keys())
