"""Tests for order book state management."""

from __future__ import annotations

from decimal import Decimal

from bot.orderbook import OrderBook
from bot.types import OptionType, Quote


def make_quote(
    instrument: str,
    bid: str = "100",
    ask: str = "101",
    timestamp_ms: int = 1711929600000,
) -> Quote:
    """Create a test quote."""
    return Quote(
        instrument=instrument,
        bid=Decimal(bid),
        bid_size=Decimal(10),
        ask=Decimal(ask),
        ask_size=Decimal(10),
        mark=Decimal("100.5"),
        iv=Decimal("0.65"),
        delta=Decimal("0.55"),
        timestamp_ms=timestamp_ms,
    )


class TestOrderBook:
    """Tests for OrderBook class."""

    def test_update_and_get(self) -> None:
        """Test updating and retrieving quotes."""
        ob = OrderBook()
        quote = make_quote("ETH-20260401-3500-C")

        ob.update(quote)

        result = ob.get("ETH-20260401-3500-C")
        assert result is not None
        assert result.instrument == "ETH-20260401-3500-C"
        assert result.bid == Decimal(100)

    def test_get_missing(self) -> None:
        """Test getting non-existent quote returns None."""
        ob = OrderBook()
        assert ob.get("MISSING") is None

    def test_get_pair(self) -> None:
        """Test getting call/put pair."""
        ob = OrderBook()
        call = make_quote("ETH-20260401-3500-C", bid="100", ask="101")
        put = make_quote("ETH-20260401-3500-P", bid="50", ask="51")

        ob.update(call)
        ob.update(put)

        call_result, put_result = ob.get_pair("ETH", "20260401", Decimal(3500))

        assert call_result is not None
        assert put_result is not None
        assert call_result.bid == Decimal(100)
        assert put_result.bid == Decimal(50)

    def test_get_pair_partial(self) -> None:
        """Test getting pair when only one exists."""
        ob = OrderBook()
        call = make_quote("ETH-20260401-3500-C")
        ob.update(call)

        call_result, put_result = ob.get_pair("ETH", "20260401", Decimal(3500))

        assert call_result is not None
        assert put_result is None

    def test_get_option(self) -> None:
        """Test getting specific option."""
        ob = OrderBook()
        call = make_quote("ETH-20260401-3500-C")
        ob.update(call)

        result = ob.get_option("ETH", "20260401", Decimal(3500), OptionType.CALL)
        assert result is not None
        assert result.instrument == "ETH-20260401-3500-C"

    def test_get_expiries(self) -> None:
        """Test getting available expiries."""
        ob = OrderBook()
        ob.update(make_quote("ETH-20260401-3500-C"))
        ob.update(make_quote("ETH-20260401-4000-C"))
        ob.update(make_quote("ETH-20260501-3500-C"))

        expiries = ob.get_expiries("ETH")

        assert expiries == ["20260401", "20260501"]

    def test_get_expiries_empty(self) -> None:
        """Test getting expiries for unknown underlying."""
        ob = OrderBook()
        assert ob.get_expiries("UNKNOWN") == []

    def test_get_strikes(self) -> None:
        """Test getting available strikes."""
        ob = OrderBook()
        ob.update(make_quote("ETH-20260401-3000-C"))
        ob.update(make_quote("ETH-20260401-3500-C"))
        ob.update(make_quote("ETH-20260401-4000-C"))

        strikes = ob.get_strikes("ETH", "20260401")

        assert strikes == [Decimal(3000), Decimal(3500), Decimal(4000)]

    def test_get_strikes_empty(self) -> None:
        """Test getting strikes for unknown expiry."""
        ob = OrderBook()
        assert ob.get_strikes("ETH", "99990101") == []

    def test_quote_count(self) -> None:
        """Test quote count property."""
        ob = OrderBook()
        assert ob.quote_count == 0

        ob.update(make_quote("ETH-20260401-3500-C"))
        assert ob.quote_count == 1

        ob.update(make_quote("ETH-20260401-3500-P"))
        assert ob.quote_count == 2

    def test_instruments(self) -> None:
        """Test instruments property."""
        ob = OrderBook()
        ob.update(make_quote("ETH-20260401-3500-C"))
        ob.update(make_quote("ETH-20260401-3500-P"))

        instruments = ob.instruments

        assert len(instruments) == 2
        assert "ETH-20260401-3500-C" in instruments
        assert "ETH-20260401-3500-P" in instruments

    def test_clear(self) -> None:
        """Test clearing the order book."""
        ob = OrderBook()
        ob.update(make_quote("ETH-20260401-3500-C"))
        ob.update(make_quote("ETH-20260401-3500-P"))

        ob.clear()

        assert ob.quote_count == 0
        assert ob.instruments == []
        assert ob.get_expiries("ETH") == []

    def test_update_replaces_existing(self) -> None:
        """Test that updating replaces existing quote."""
        ob = OrderBook()

        old_quote = make_quote("ETH-20260401-3500-C", bid="100")
        ob.update(old_quote)

        new_quote = make_quote("ETH-20260401-3500-C", bid="105")
        ob.update(new_quote)

        result = ob.get("ETH-20260401-3500-C")
        assert result is not None
        assert result.bid == Decimal(105)
        assert ob.quote_count == 1
