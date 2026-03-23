"""Tests for domain types - business logic only."""

from __future__ import annotations

from decimal import Decimal

from bot.types import OptionType, parse_instrument


class TestParseInstrument:
    """Tests for instrument parsing - actual business logic."""

    def test_parse_call_option(self) -> None:
        """Test parsing call option instrument name."""
        info = parse_instrument("ETH-20260401-3500-C")

        assert info is not None
        assert info.underlying == "ETH"
        assert info.expiry == "20260401"
        assert info.strike == Decimal(3500)
        assert info.option_type == OptionType.CALL

    def test_parse_put_option(self) -> None:
        """Test parsing put option instrument name."""
        info = parse_instrument("BTC-20261231-100000-P")

        assert info is not None
        assert info.underlying == "BTC"
        assert info.expiry == "20261231"
        assert info.strike == Decimal(100000)
        assert info.option_type == OptionType.PUT

    def test_parse_invalid_format(self) -> None:
        """Test parsing invalid instrument name."""
        assert parse_instrument("INVALID") is None
        assert parse_instrument("ETH-20260401-3500") is None
        assert parse_instrument("ETH-20260401-ABC-C") is None
        assert parse_instrument("ETH-20260401-3500-X") is None

    def test_parse_decimal_strike(self) -> None:
        """Test parsing decimal strike price."""
        info = parse_instrument("ETH-20260401-3500.5-C")

        assert info is not None
        assert info.strike == Decimal("3500.5")
