"""Risk management and validation.

Final gatekeeper before execution. Validates that opportunities
meet all constraints including the zero-loss invariant.
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING

from bot.types import (
    ZERO,
    ArbType,
    Opportunity,
    RejectedOpportunity,
    RejectionReason,
    ValidationResult,
    ValidOpportunity,
)

if TYPE_CHECKING:
    from bot.config import Config
    from bot.orderbook import OrderBook

logger = logging.getLogger(__name__)


class RiskManager:
    """Validates opportunities before execution."""

    __slots__ = (
        "_config",
        "_orderbook",
    )

    def __init__(
        self,
        config: Config,
        orderbook: OrderBook,
    ) -> None:
        """Initialize the risk manager."""
        self._config = config
        self._orderbook = orderbook

    def validate(self, opportunity: Opportunity) -> ValidationResult:
        """Validate an opportunity against all risk criteria."""
        checks = [
            (self._check_freshness, RejectionReason.STALE_QUOTE),
            (self._check_liquidity, RejectionReason.INSUFFICIENT_LIQUIDITY),
            (self._check_profit_threshold, RejectionReason.BELOW_THRESHOLD),
            (self._check_zero_loss, RejectionReason.NEGATIVE_FLOOR),
            (self._check_fee_coverage, RejectionReason.FEE_EXCEEDS_PROFIT),
            (self._verify_quotes_exist, RejectionReason.MISSING_QUOTE),
        ]

        for check_fn, reason in checks:
            if not check_fn(opportunity):
                logger.debug(
                    "[RISK] Rejected | type=%s | reason=%s | profit=$%.2f",
                    opportunity.arb_type.name,
                    reason.name,
                    float(opportunity.net_profit),
                )
                return RejectedOpportunity(reason=reason)

        logger.info(
            "[RISK] Validated | type=%s | profit=$%.2f | floor=$%.2f",
            opportunity.arb_type.name,
            float(opportunity.net_profit),
            float(opportunity.guaranteed_floor),
        )
        return ValidOpportunity(opportunity=opportunity)

    def _check_freshness(self, opp: Opportunity) -> bool:
        """Check if opportunity quotes are still fresh."""
        now_ms = int(time.time() * 1000)
        age_ms = now_ms - opp.timestamp_ms
        is_fresh = age_ms < self._config.max_quote_age_ms

        if not is_fresh:
            logger.debug(
                "[RISK] Stale quotes | age=%dms | max=%dms",
                age_ms,
                self._config.max_quote_age_ms,
            )

        return is_fresh

    def _check_liquidity(self, opp: Opportunity) -> bool:
        """Check if sufficient liquidity exists for all legs."""
        for leg in opp.legs:
            if leg.available < leg.size:
                logger.debug(
                    "[RISK] Insufficient liquidity | instrument=%s | need=%s | have=%s",
                    leg.instrument,
                    leg.size,
                    leg.available,
                )
                return False
        return True

    def _check_profit_threshold(self, opp: Opportunity) -> bool:
        """Check if net profit meets minimum threshold."""
        meets_threshold = opp.net_profit >= self._config.min_profit_usd

        if not meets_threshold:
            logger.debug(
                "[RISK] Below threshold | profit=$%.2f | min=$%.2f",
                float(opp.net_profit),
                float(self._config.min_profit_usd),
            )

        return meets_threshold

    def _check_zero_loss(self, opp: Opportunity) -> bool:
        """Check zero-loss constraint based on strategy type."""
        match opp.arb_type:
            case ArbType.CONVERSION | ArbType.REVERSAL:
                valid = opp.guaranteed_floor > ZERO

            case ArbType.ZERO_COLLAR:
                valid = opp.guaranteed_floor > ZERO and opp.net_profit >= ZERO

            case ArbType.BOX_SPREAD:
                valid = opp.guaranteed_floor > ZERO and opp.net_profit > ZERO

            case ArbType.NEG_BUTTERFLY:
                valid = opp.guaranteed_floor >= ZERO

        if not valid:
            logger.debug(
                "[RISK] Zero-loss violated | type=%s | floor=%s | profit=$%.2f",
                opp.arb_type.name,
                opp.guaranteed_floor,
                float(opp.net_profit),
            )

        return valid

    def _check_fee_coverage(self, opp: Opportunity) -> bool:
        """Check if gross credit covers fees."""
        covered = opp.gross_credit > opp.total_fees

        if not covered:
            logger.debug(
                "[RISK] Fees exceed credit | gross=$%.2f | fees=$%.2f",
                float(opp.gross_credit),
                float(opp.total_fees),
            )

        return covered

    def _verify_quotes_exist(self, opp: Opportunity) -> bool:
        """Verify all leg instruments still have quotes."""
        for leg in opp.legs:
            if "-PERP" in leg.instrument:
                continue

            quote = self._orderbook.get(leg.instrument)
            if quote is None:
                logger.debug("[RISK] Missing quote | instrument=%s", leg.instrument)
                return False

        return True
