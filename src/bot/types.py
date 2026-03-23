"""Domain types for the HFT bot.

All types use __slots__ for memory efficiency and are immutable.
No nullable fields - use discriminated unions instead.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from enum import Enum, auto
from typing import TYPE_CHECKING, Final

if TYPE_CHECKING:
    from collections.abc import Sequence

# =============================================================================
# Constants
# =============================================================================

ZERO: Final[Decimal] = Decimal(0)
ONE: Final[Decimal] = Decimal(1)
TWO: Final[Decimal] = Decimal(2)
HUNDRED: Final[Decimal] = Decimal(100)


# =============================================================================
# Enums
# =============================================================================


class OptionType(Enum):
    """Option contract type."""

    CALL = "C"
    PUT = "P"


class Side(Enum):
    """Trade direction."""

    BUY = "buy"
    SELL = "sell"


class ArbType(Enum):
    """Arbitrage strategy classification."""

    CONVERSION = auto()
    REVERSAL = auto()
    ZERO_COLLAR = auto()
    BOX_SPREAD = auto()
    NEG_BUTTERFLY = auto()


class RejectionReason(Enum):
    """Reasons for opportunity rejection."""

    STALE_QUOTE = auto()
    INSUFFICIENT_LIQUIDITY = auto()
    BELOW_THRESHOLD = auto()
    NEGATIVE_FLOOR = auto()
    FEE_EXCEEDS_PROFIT = auto()
    MARGIN_BREACH = auto()
    MISSING_QUOTE = auto()


# =============================================================================
# Data Classes - No Nullable Fields
# =============================================================================


@dataclass(frozen=True, slots=True)
class Quote:
    """Market data snapshot for a single instrument."""

    instrument: str
    bid: Decimal
    bid_size: Decimal
    ask: Decimal
    ask_size: Decimal
    mark: Decimal
    iv: Decimal
    delta: Decimal
    timestamp_ms: int
    index_price: Decimal = ZERO
    # Oracle forward price for this expiry (option_pricing.f from ticker_slim).
    # Derive uses Black76 with r=0; this field is the market-implied forward price
    # supplied by the Block Scholes oracle — the only correct basis for put-call parity.
    forward_price: Decimal = ZERO
    # Current hourly funding rate (for perp instruments; null/zero for options).
    funding_rate: Decimal = ZERO

    @property
    def mid(self) -> Decimal:
        """Calculate mid price."""
        return (self.bid + self.ask) / TWO

    @property
    def spread(self) -> Decimal:
        """Calculate bid-ask spread."""
        return self.ask - self.bid


@dataclass(frozen=True, slots=True)
class Leg:
    """Single leg of a multi-leg trade."""

    instrument: str
    side: Side
    price: Decimal
    size: Decimal
    available: Decimal


@dataclass(frozen=True, slots=True)
class Opportunity:
    """Detected arbitrage opportunity."""

    arb_type: ArbType
    legs: tuple[Leg, ...]
    gross_credit: Decimal
    total_fees: Decimal
    net_profit: Decimal
    guaranteed_floor: Decimal
    expiry: str
    underlying: str
    timestamp_ms: int

    @property
    def leg_count(self) -> int:
        """Number of legs in the trade."""
        return len(self.legs)


@dataclass(frozen=True, slots=True)
class ValidOpportunity:
    """Validated opportunity ready for execution."""

    opportunity: Opportunity


@dataclass(frozen=True, slots=True)
class RejectedOpportunity:
    """Opportunity that failed validation."""

    reason: RejectionReason


# Discriminated union - no nullable fields needed
ValidationResult = ValidOpportunity | RejectedOpportunity


@dataclass(frozen=True, slots=True)
class InstrumentInfo:
    """Parsed instrument metadata."""

    name: str
    underlying: str
    expiry: str
    strike: Decimal
    option_type: OptionType


# =============================================================================
# Utility Functions
# =============================================================================


def parse_instrument(name: str) -> InstrumentInfo | None:
    """Parse instrument name into components.

    Args:
        name: Instrument name (e.g., "ETH-20260401-3500-C")

    Returns:
        InstrumentInfo if valid, None otherwise
    """
    parts = name.split("-")
    if len(parts) != 4:
        return None

    underlying, expiry, strike_str, opt_type_str = parts

    try:
        strike = Decimal(strike_str)
    except (ValueError, ArithmeticError):
        return None

    if opt_type_str == "C":
        option_type = OptionType.CALL
    elif opt_type_str == "P":
        option_type = OptionType.PUT
    else:
        return None

    return InstrumentInfo(
        name=name,
        underlying=underlying,
        expiry=expiry,
        strike=strike,
        option_type=option_type,
    )


def legs_to_instruments(legs: Sequence[Leg]) -> tuple[str, ...]:
    """Extract instrument names from legs."""
    return tuple(leg.instrument for leg in legs)


def is_valid(result: ValidationResult) -> bool:
    """Check if validation result is valid."""
    return isinstance(result, ValidOpportunity)
