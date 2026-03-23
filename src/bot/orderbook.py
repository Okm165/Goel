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
        "_forward_prices",
        "_index_prices",
        "_perp_funding_rates",
        "_perp_marks",
        "_quotes",
        "_strikes",
        "_update_event",
    )

    def __init__(self) -> None:
        """Initialize empty order book."""
        self._quotes: dict[str, Quote] = {}
        self._expiries: dict[str, set[str]] = {}
        self._strikes: dict[str, set[Decimal]] = {}
        self._index_prices: dict[str, Decimal] = {}
        # Oracle forward price keyed by "{underlying}:{expiry}" (e.g. "BTC:20260403").
        # Sourced from option_pricing.f in ticker_slim — the Block Scholes oracle value
        # that Derive's smart contract uses for Black76 pricing with r=0.
        self._forward_prices: dict[str, Decimal] = {}
        # Perp mark price and hourly funding rate, keyed by underlying (e.g. "BTC").
        self._perp_marks: dict[str, Decimal] = {}
        self._perp_funding_rates: dict[str, Decimal] = {}
        self._update_event = asyncio.Event()

    def update(self, quote: Quote) -> None:
        """Update quote in the cache.

        Args:
            quote: New quote data
        """
        instrument = quote.instrument

        # Handle perpetual instruments separately — they carry the funding rate
        # and their mark price serves as a near-term forward proxy.
        if instrument.endswith("-PERP"):
            underlying = instrument[: -len("-PERP")]
            if quote.mark > 0:
                self._perp_marks[underlying] = quote.mark
            if quote.funding_rate != 0:
                self._perp_funding_rates[underlying] = quote.funding_rate
            if quote.index_price > 0:
                self._index_prices[underlying] = quote.index_price
            self._update_event.set()
            return

        self._quotes[instrument] = quote

        info = parse_instrument(instrument)
        if info:
            underlying = info.underlying

            if underlying not in self._expiries:
                self._expiries[underlying] = set()
            self._expiries[underlying].add(info.expiry)

            expiry_key = f"{underlying}:{info.expiry}"
            if expiry_key not in self._strikes:
                self._strikes[expiry_key] = set()
            self._strikes[expiry_key].add(info.strike)

            if quote.index_price > 0:
                self._index_prices[underlying] = quote.index_price

            # Cache the oracle forward price for this expiry.
            # Every option in the same expiry carries the same forward price (f in
            # option_pricing). We keep updating it so it stays fresh.
            if quote.forward_price > 0:
                self._forward_prices[expiry_key] = quote.forward_price

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

        Uses index price from ticker data if available, otherwise
        derives from mark price of near-ATM option.

        Args:
            underlying: Asset symbol

        Returns:
            Spot price estimate or None
        """
        if underlying in self._index_prices and self._index_prices[underlying] > 0:
            return self._index_prices[underlying]

        for instrument, quote in self._quotes.items():
            if instrument.startswith(f"{underlying}-") and quote.mark > 0:
                info = parse_instrument(instrument)
                if info:
                    if info.option_type == OptionType.CALL:
                        return info.strike + quote.mark
                    return info.strike - quote.mark
        return None

    async def wait_for_update(self, timeout: float = 5.0) -> bool:
        """Wait for the next quote update with timeout.

        Efficient mechanism for scanners to trigger on new data.
        Includes timeout to prevent indefinite blocking.

        Args:
            timeout: Maximum seconds to wait for update

        Returns:
            True if update received, False if timeout
        """
        self._update_event.clear()
        try:
            await asyncio.wait_for(self._update_event.wait(), timeout=timeout)
            return True
        except asyncio.TimeoutError:
            logger.warning(
                "[ORDERBOOK] Wait timeout after %.1fs | quotes=%d",
                timeout,
                len(self._quotes),
            )
            return False

    def update_index_price(self, underlying: str, price: Decimal) -> None:
        """Update the index price for an underlying."""
        if price > 0:
            self._index_prices[underlying] = price

    def get_index_price(self, underlying: str) -> Decimal | None:
        """Get the index price for an underlying."""
        return self._index_prices.get(underlying)

    def get_forward_price(self, underlying: str, expiry: str) -> Decimal | None:
        """Return the oracle forward price for a given expiry.

        Sourced from option_pricing.f in ticker_slim — the Block Scholes oracle
        value that Derive's smart contract uses for Black76 pricing (r=0).

        Returns None if no oracle forward has been received yet for this expiry.
        Callers skip the expiry until the value is cached; this is safe because
        the WebSocket delivers option ticks within seconds of connection and
        using a wrong proxy (e.g. perp mark) for a long-dated expiry would
        introduce thousands of dollars of forward-price error.
        """
        key = f"{underlying}:{expiry}"
        fwd = self._forward_prices.get(key)
        if fwd and fwd > 0:
            return fwd
        return None

    def get_perp_mark(self, underlying: str) -> Decimal | None:
        """Return the current perpetual mark price for an underlying."""
        return self._perp_marks.get(underlying)

    def get_perp_funding_rate(self, underlying: str) -> Decimal | None:
        """Return the current hourly funding rate for the perpetual."""
        return self._perp_funding_rates.get(underlying)

    def clear(self) -> None:
        """Clear all cached data."""
        self._quotes.clear()
        self._expiries.clear()
        self._strikes.clear()
        self._index_prices.clear()
        self._forward_prices.clear()
        self._perp_marks.clear()
        self._perp_funding_rates.clear()

    @property
    def quote_count(self) -> int:
        """Number of quotes in cache."""
        return len(self._quotes)

    @property
    def instruments(self) -> list[str]:
        """List of all cached instrument names."""
        return list(self._quotes.keys())
