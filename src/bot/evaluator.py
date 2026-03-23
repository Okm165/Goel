"""Arbitrage opportunity scanner.

The mathematical brain of the system. Scans for t=0 arbitrage
opportunities using live order book data.
"""

from __future__ import annotations

import asyncio
import logging
import math
import time
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING

from bot.types import (
    ONE,
    TWO,
    ZERO,
    ArbType,
    Leg,
    Opportunity,
    OptionType,
    Quote,
    Side,
)

if TYPE_CHECKING:
    from bot.config import Config
    from bot.orderbook import OrderBook

logger = logging.getLogger(__name__)


class Evaluator:
    """Scans order book for arbitrage opportunities.

    Implements scanning algorithms for:
    - Put-Call Parity (Conversion/Reversal)
    - Zero-Cost Collar
    - Box Spread
    - Negative-Cost Butterfly

    All calculations are deterministic using t=0 data only.
    """

    __slots__ = (
        "_config",
        "_opportunity_queue",
        "_orderbook",
    )

    def __init__(
        self,
        config: Config,
        orderbook: OrderBook,
        opportunity_queue: asyncio.Queue[Opportunity],
    ) -> None:
        """Initialize the evaluator."""
        self._config = config
        self._orderbook = orderbook
        self._opportunity_queue = opportunity_queue

    async def run(self) -> None:
        """Main scanning loop - triggers on every order book update."""
        logger.info("[EVALUATOR] Started | underlyings=%s", ",".join(self._config.underlyings))

        scan_count = 0
        total_opportunities = 0

        while True:
            await self._orderbook.wait_for_update()

            try:
                scan_start = time.monotonic()
                opportunities = self._scan_all()
                scan_duration_ms = (time.monotonic() - scan_start) * 1000

                scan_count += 1

                if opportunities:
                    opportunities.sort(key=lambda o: o.net_profit, reverse=True)

                    logger.info(
                        "[EVALUATOR] Scan #%d complete | found=%d | duration=%.2fms",
                        scan_count,
                        len(opportunities),
                        scan_duration_ms,
                    )

                    for opp in opportunities[:10]:
                        total_opportunities += 1
                        logger.debug(
                            "[EVALUATOR] Opportunity | type=%s | profit=$%.2f | floor=$%.2f",
                            opp.arb_type.name,
                            float(opp.net_profit),
                            float(opp.guaranteed_floor),
                        )

                        try:
                            self._opportunity_queue.put_nowait(opp)
                        except asyncio.QueueFull:
                            logger.warning("[EVALUATOR] Opportunity queue full - dropping")
                            break
                elif scan_count % 100 == 0:
                    logger.debug(
                        "[EVALUATOR] Scan #%d | no opportunities | orderbook_size=%d",
                        scan_count,
                        self._orderbook.quote_count,
                    )

            except Exception:
                logger.exception("[EVALUATOR] Error in scan loop")

    def _scan_all(self) -> list[Opportunity]:
        """Run all scanning algorithms across all underlyings."""
        opportunities: list[Opportunity] = []

        for underlying in self._config.underlyings:
            opportunities.extend(self._scan_conversions(underlying))
            opportunities.extend(self._scan_collars(underlying))
            opportunities.extend(self._scan_box_spreads(underlying))
            opportunities.extend(self._scan_butterflies(underlying))

        return opportunities

    def _scan_conversions(self, underlying: str) -> list[Opportunity]:
        """Scan for Put-Call Parity violations (Conversion/Reversal)."""
        opportunities: list[Opportunity] = []
        spot = self._orderbook.get_spot_price(underlying)

        if spot is None:
            logger.debug("[EVALUATOR] No spot price for %s - skipping conversions", underlying)
            return opportunities

        now_ms = int(time.time() * 1000)

        for expiry in self._orderbook.get_expiries(underlying):
            tte = self._time_to_expiry_years(expiry)
            if tte <= ZERO:
                continue

            discount = self._discount_factor(tte)

            for strike in self._orderbook.get_strikes(underlying, expiry):
                call, put = self._orderbook.get_pair(underlying, expiry, strike)

                if not call or not put:
                    continue

                if not self._is_fresh(call) or not self._is_fresh(put):
                    continue

                pv_strike = strike * discount

                synthetic_short = call.bid - put.ask
                actual_forward = spot - pv_strike
                gross_conv = synthetic_short - actual_forward

                if gross_conv > ZERO:
                    legs = (
                        Leg(call.instrument, Side.SELL, call.bid, ONE, call.bid_size),
                        Leg(put.instrument, Side.BUY, put.ask, ONE, put.ask_size),
                    )

                    fees = self._estimate_fees(legs, spot)
                    net_profit = gross_conv - fees

                    if net_profit >= self._config.min_profit_usd:
                        min_size = min(call.bid_size, put.ask_size)
                        if min_size >= self._config.min_trade_size:
                            opportunities.append(
                                Opportunity(
                                    arb_type=ArbType.CONVERSION,
                                    legs=legs,
                                    gross_credit=gross_conv,
                                    total_fees=fees,
                                    net_profit=net_profit,
                                    guaranteed_floor=strike,
                                    expiry=expiry,
                                    underlying=underlying,
                                    timestamp_ms=now_ms,
                                )
                            )
                            logger.debug(
                                "[EVALUATOR] CONVERSION found | %s | strike=%s | profit=$%.2f",
                                underlying,
                                strike,
                                float(net_profit),
                            )

                synthetic_long = call.ask - put.bid
                gross_rev = actual_forward - synthetic_long

                if gross_rev > ZERO:
                    legs = (
                        Leg(call.instrument, Side.BUY, call.ask, ONE, call.ask_size),
                        Leg(put.instrument, Side.SELL, put.bid, ONE, put.bid_size),
                    )

                    fees = self._estimate_fees(legs, spot)
                    net_profit = gross_rev - fees

                    if net_profit >= self._config.min_profit_usd:
                        min_size = min(call.ask_size, put.bid_size)
                        if min_size >= self._config.min_trade_size:
                            opportunities.append(
                                Opportunity(
                                    arb_type=ArbType.REVERSAL,
                                    legs=legs,
                                    gross_credit=gross_rev,
                                    total_fees=fees,
                                    net_profit=net_profit,
                                    guaranteed_floor=strike,
                                    expiry=expiry,
                                    underlying=underlying,
                                    timestamp_ms=now_ms,
                                )
                            )
                            logger.debug(
                                "[EVALUATOR] REVERSAL found | %s | strike=%s | profit=$%.2f",
                                underlying,
                                strike,
                                float(net_profit),
                            )

        return opportunities

    def _scan_collars(self, underlying: str) -> list[Opportunity]:
        """Scan for Zero-Cost Collar opportunities."""
        opportunities: list[Opportunity] = []
        spot = self._orderbook.get_spot_price(underlying)

        if spot is None:
            return opportunities

        now_ms = int(time.time() * 1000)

        for expiry in self._orderbook.get_expiries(underlying):
            tte = self._time_to_expiry_years(expiry)
            if tte <= ZERO:
                continue

            strikes = self._orderbook.get_strikes(underlying, expiry)

            for i, k_put in enumerate(strikes):
                if k_put >= spot:
                    continue

                put = self._orderbook.get_option(underlying, expiry, k_put, OptionType.PUT)
                if not put or not self._is_fresh(put):
                    continue

                for k_call in strikes[i + 1 :]:
                    if k_call <= spot:
                        continue

                    call = self._orderbook.get_option(underlying, expiry, k_call, OptionType.CALL)
                    if not call or not self._is_fresh(call):
                        continue

                    net_premium = call.bid - put.ask

                    legs = (
                        Leg(put.instrument, Side.BUY, put.ask, ONE, put.ask_size),
                        Leg(call.instrument, Side.SELL, call.bid, ONE, call.bid_size),
                    )

                    fees = self._estimate_fees(legs, spot)
                    net_credit = net_premium - fees

                    if net_credit < ZERO:
                        continue

                    entry_cost = spot - net_credit
                    guaranteed_floor = k_put

                    if guaranteed_floor >= entry_cost:
                        guaranteed_profit = guaranteed_floor - entry_cost

                        min_size = min(put.ask_size, call.bid_size)
                        if min_size >= self._config.min_trade_size:
                            opportunities.append(
                                Opportunity(
                                    arb_type=ArbType.ZERO_COLLAR,
                                    legs=legs,
                                    gross_credit=net_premium,
                                    total_fees=fees,
                                    net_profit=guaranteed_profit,
                                    guaranteed_floor=guaranteed_floor,
                                    expiry=expiry,
                                    underlying=underlying,
                                    timestamp_ms=now_ms,
                                )
                            )
                            logger.debug(
                                "[EVALUATOR] ZERO_COLLAR | %s | put=%s call=%s | profit=$%.2f",
                                underlying,
                                k_put,
                                k_call,
                                float(guaranteed_profit),
                            )

        return opportunities

    def _scan_box_spreads(self, underlying: str) -> list[Opportunity]:
        """Scan for Box Spread arbitrage."""
        opportunities: list[Opportunity] = []
        spot = self._orderbook.get_spot_price(underlying)

        if spot is None:
            return opportunities

        now_ms = int(time.time() * 1000)

        for expiry in self._orderbook.get_expiries(underlying):
            tte = self._time_to_expiry_years(expiry)
            if tte <= ZERO:
                continue

            discount = self._discount_factor(tte)
            strikes = self._orderbook.get_strikes(underlying, expiry)

            for i, k1 in enumerate(strikes[:-1]):
                for k2 in strikes[i + 1 :]:
                    c1 = self._orderbook.get_option(underlying, expiry, k1, OptionType.CALL)
                    c2 = self._orderbook.get_option(underlying, expiry, k2, OptionType.CALL)
                    p1 = self._orderbook.get_option(underlying, expiry, k1, OptionType.PUT)
                    p2 = self._orderbook.get_option(underlying, expiry, k2, OptionType.PUT)

                    if not all([c1, c2, p1, p2]):
                        continue

                    assert c1 is not None
                    assert c2 is not None
                    assert p1 is not None
                    assert p2 is not None

                    if not all(self._is_fresh(q) for q in [c1, c2, p1, p2]):
                        continue

                    bull_call_cost = c1.ask - c2.bid
                    bear_put_cost = p2.ask - p1.bid
                    total_cost = bull_call_cost + bear_put_cost

                    payoff = k2 - k1
                    pv_payoff = payoff * discount

                    gross_profit = pv_payoff - total_cost

                    if gross_profit <= ZERO:
                        continue

                    legs = (
                        Leg(c1.instrument, Side.BUY, c1.ask, ONE, c1.ask_size),
                        Leg(c2.instrument, Side.SELL, c2.bid, ONE, c2.bid_size),
                        Leg(p1.instrument, Side.SELL, p1.bid, ONE, p1.bid_size),
                        Leg(p2.instrument, Side.BUY, p2.ask, ONE, p2.ask_size),
                    )

                    standard_fees = self._estimate_fees(legs, spot)
                    box_fee = spot * Decimal("0.01") * tte
                    total_fees = standard_fees + box_fee

                    net_profit = gross_profit - total_fees

                    if net_profit >= self._config.min_profit_usd:
                        min_size = min(c1.ask_size, c2.bid_size, p1.bid_size, p2.ask_size)
                        if min_size >= self._config.min_trade_size:
                            opportunities.append(
                                Opportunity(
                                    arb_type=ArbType.BOX_SPREAD,
                                    legs=legs,
                                    gross_credit=gross_profit,
                                    total_fees=total_fees,
                                    net_profit=net_profit,
                                    guaranteed_floor=payoff,
                                    expiry=expiry,
                                    underlying=underlying,
                                    timestamp_ms=now_ms,
                                )
                            )
                            logger.debug(
                                "[EVALUATOR] BOX_SPREAD found | %s | k1=%s k2=%s | profit=$%.2f",
                                underlying,
                                k1,
                                k2,
                                float(net_profit),
                            )

        return opportunities

    def _scan_butterflies(self, underlying: str) -> list[Opportunity]:
        """Scan for Negative-Cost Butterfly spreads."""
        opportunities: list[Opportunity] = []
        spot = self._orderbook.get_spot_price(underlying)

        if spot is None:
            return opportunities

        now_ms = int(time.time() * 1000)

        for expiry in self._orderbook.get_expiries(underlying):
            tte = self._time_to_expiry_years(expiry)
            if tte <= ZERO:
                continue

            strikes = self._orderbook.get_strikes(underlying, expiry)

            for i, k1 in enumerate(strikes[:-2]):
                for k2 in strikes[i + 1 : -1]:
                    k3_target = k2 + (k2 - k1)

                    if k3_target not in strikes:
                        continue

                    k3 = k3_target

                    c1 = self._orderbook.get_option(underlying, expiry, k1, OptionType.CALL)
                    c2 = self._orderbook.get_option(underlying, expiry, k2, OptionType.CALL)
                    c3 = self._orderbook.get_option(underlying, expiry, k3, OptionType.CALL)

                    if not all([c1, c2, c3]):
                        continue

                    assert c1 is not None
                    assert c2 is not None
                    assert c3 is not None

                    if not all(self._is_fresh(q) for q in [c1, c2, c3]):
                        continue

                    net_cost = c1.ask - (TWO * c2.bid) + c3.ask

                    if net_cost >= ZERO:
                        continue

                    net_credit = -net_cost

                    legs = (
                        Leg(c1.instrument, Side.BUY, c1.ask, ONE, c1.ask_size),
                        Leg(c2.instrument, Side.SELL, c2.bid, TWO, c2.bid_size / TWO),
                        Leg(c3.instrument, Side.BUY, c3.ask, ONE, c3.ask_size),
                    )

                    fees = self._estimate_fees(legs, spot)
                    net_profit_at_entry = net_credit - fees

                    if net_profit_at_entry > ZERO:
                        min_size = min(c1.ask_size, c2.bid_size / TWO, c3.ask_size)
                        if min_size >= self._config.min_trade_size:
                            opportunities.append(
                                Opportunity(
                                    arb_type=ArbType.NEG_BUTTERFLY,
                                    legs=legs,
                                    gross_credit=net_credit,
                                    total_fees=fees,
                                    net_profit=net_profit_at_entry,
                                    guaranteed_floor=net_profit_at_entry,
                                    expiry=expiry,
                                    underlying=underlying,
                                    timestamp_ms=now_ms,
                                )
                            )
                            logger.debug(
                                "[EVALUATOR] NEG_BUTTERFLY | %s | strikes=%s/%s/%s | profit=$%.2f",
                                underlying,
                                k1,
                                k2,
                                k3,
                                float(net_profit_at_entry),
                            )

        return opportunities

    def _is_fresh(self, quote: Quote) -> bool:
        """Check if quote is within acceptable staleness."""
        now_ms = int(time.time() * 1000)
        return (now_ms - quote.timestamp_ms) < self._config.max_quote_age_ms

    def _time_to_expiry_years(self, expiry: str) -> Decimal:
        """Calculate time to expiry in years."""
        try:
            expiry_date = datetime.strptime(expiry, "%Y%m%d").replace(tzinfo=UTC)
            now = datetime.now(UTC)
            delta = expiry_date - now
            seconds = delta.total_seconds()
            return Decimal(str(seconds / (365.25 * 24 * 3600)))
        except ValueError:
            return ZERO

    def _discount_factor(self, tte_years: Decimal) -> Decimal:
        """Calculate e^(-r*T) for present value calculations."""
        rate = float(self._config.risk_free_rate)
        tte = float(tte_years)
        return Decimal(str(math.exp(-rate * tte)))

    def _estimate_fees(
        self,
        legs: tuple[Leg, ...],
        spot: Decimal,
    ) -> Decimal:
        """Estimate total execution fees with RFQ discounts."""
        leg_fees: list[Decimal] = []

        for leg in legs:
            notional = leg.size * spot
            base_fee = max(Decimal("0.50"), notional * Decimal("0.0003"))
            leg_fees.append(base_fee)

        leg_fees.sort()

        if len(leg_fees) >= 2:
            leg_fees[0] = ZERO
            if len(leg_fees) >= 3:
                leg_fees[1] *= Decimal("0.5")
            if len(leg_fees) >= 4:
                leg_fees[2] *= Decimal("0.5")

        gas_estimate = Decimal("0.01") * Decimal(str(len(legs)))

        return sum(leg_fees) + gas_estimate
