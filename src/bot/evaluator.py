"""Arbitrage opportunity scanner.

The mathematical brain of the system. Scans for t=0 arbitrage
opportunities using live order book data.
"""

from __future__ import annotations

import asyncio
import collections
import logging
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
        "_scan_count",
        "_scan_history",
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
        self._scan_count = 0
        # Rolling window of the last 360 scans (~3 min at 0.5s interval)
        self._scan_history: collections.deque[dict[str, object]] = collections.deque(maxlen=360)

    @property
    def scan_count(self) -> int:
        """Total number of scans completed."""
        return self._scan_count

    @property
    def scan_history(self) -> list[dict[str, object]]:
        """Recent per-scan results for charting."""
        return list(self._scan_history)

    async def run(self) -> None:
        """Main scanning loop - runs at fixed intervals."""
        logger.info("[EVALUATOR] Started | underlyings=%s", ",".join(self._config.underlyings))

        total_opportunities = 0
        scan_interval = 0.5

        while self._orderbook.quote_count == 0:
            logger.info("[EVALUATOR] Waiting for initial quotes...")
            await asyncio.sleep(1.0)

        while True:
            await asyncio.sleep(scan_interval)

            try:
                scan_start = time.monotonic()
                opportunities = self._scan_all()
                scan_duration_ms = (time.monotonic() - scan_start) * 1000

                self._scan_count += 1

                if opportunities:
                    opportunities.sort(key=lambda o: o.net_profit, reverse=True)

                    logger.info(
                        "[EVALUATOR] Scan #%d complete | found=%d | duration=%.2fms",
                        self._scan_count,
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

                self._scan_history.append(
                    {
                        "ts": datetime.now(UTC).isoformat(),
                        "scan_num": self._scan_count,
                        "found": len(opportunities),
                        "duration_ms": round(scan_duration_ms, 1),
                    }
                )

                if not opportunities and self._scan_count % 100 == 0:
                    logger.debug(
                        "[EVALUATOR] Scan #%d | no opportunities | orderbook_size=%d",
                        self._scan_count,
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
        """Scan for Put-Call Parity violations (Conversion/Reversal).

        Correct formula (Derive uses Black76 with r=0 per protocol docs):
            C - P = F_oracle - K

        where F_oracle is the oracle-supplied forward price from option_pricing.f
        in ticker_slim -- NOT spot * e^(-r*T) with a static rate.

        The forward price captures the actual market carry rate implied by perpetual
        futures (BTC static funding alone is ~11%/year), which is far higher than any
        US Treasury rate.  Using a static rate produces phantom arbitrage at every
        high-strike option; using F_oracle eliminates these artefacts.
        """
        opportunities: list[Opportunity] = []
        spot = self._orderbook.get_spot_price(underlying)

        if spot is None:
            logger.debug("[EVALUATOR] No spot price for %s - skipping conversions", underlying)
            return opportunities

        now_ms = int(time.time() * 1000)
        log_sample = (now_ms % 30000) < 500

        for expiry in self._orderbook.get_expiries(underlying):
            tte = self._time_to_expiry_years(expiry)
            if tte <= ZERO:
                continue

            # Obtain the oracle forward price for this expiry.
            # This is sourced from option_pricing.f arriving in ticker_slim messages
            # for every option instrument — no extra API call needed.
            forward = self._orderbook.get_forward_price(underlying, expiry)
            if forward is None or forward <= 0:
                logger.debug(
                    "[EVALUATOR] No forward price for %s/%s - skipping", underlying, expiry
                )
                continue

            if log_sample:
                logger.info(
                    "[DEBUG] %s exp=%s | spot=%.2f forward=%.2f tte=%.4f",
                    underlying,
                    expiry,
                    float(spot),
                    float(forward),
                    float(tte),
                )

            for strike in self._orderbook.get_strikes(underlying, expiry):
                call, put = self._orderbook.get_pair(underlying, expiry, strike)

                if not call or not put:
                    continue

                if not self._is_fresh(call) or not self._is_fresh(put):
                    continue

                # Correct put-call parity: C - P = F - K  (Derive, r=0)
                actual_forward = forward - strike

                synthetic_short = call.bid - put.ask
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
                                    # The guaranteed_floor is the net premium locked in at
                                    # entry — the minimum P&L if the trade is held to expiry
                                    # and F_oracle matches settlement (delta risk aside).
                                    guaranteed_floor=net_profit,
                                    expiry=expiry,
                                    underlying=underlying,
                                    timestamp_ms=now_ms,
                                )
                            )
                            if log_sample:
                                logger.info(
                                    "[DEBUG] CONVERSION %s K=%s exp=%s | "
                                    "C.bid=%.2f P.ask=%.2f | synth_short=%.2f | "
                                    "F=%.2f K=%.2f fwd=%.2f | "
                                    "gross=%.2f fees=%.2f net=%.2f",
                                    underlying,
                                    strike,
                                    expiry,
                                    float(call.bid),
                                    float(put.ask),
                                    float(synthetic_short),
                                    float(forward),
                                    float(strike),
                                    float(actual_forward),
                                    float(gross_conv),
                                    float(fees),
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
                                    guaranteed_floor=net_profit,
                                    expiry=expiry,
                                    underlying=underlying,
                                    timestamp_ms=now_ms,
                                )
                            )
                            if log_sample:
                                logger.info(
                                    "[DEBUG] REVERSAL %s K=%s exp=%s | "
                                    "C.ask=%.2f P.bid=%.2f | synth_long=%.2f | "
                                    "F=%.2f K=%.2f fwd=%.2f | "
                                    "gross=%.2f fees=%.2f net=%.2f",
                                    underlying,
                                    strike,
                                    expiry,
                                    float(call.ask),
                                    float(put.bid),
                                    float(synthetic_long),
                                    float(forward),
                                    float(strike),
                                    float(actual_forward),
                                    float(gross_rev),
                                    float(fees),
                                    float(net_profit),
                                )

        return opportunities

    def _scan_collars(self, underlying: str) -> list[Opportunity]:
        """Scan for Zero-Cost Collar opportunities.

        DISABLED: A collar requires holding the underlying spot asset to be
        risk-free.  Derive does not support spot trading (marked 'coming soon'
        in the product docs).  Without the spot leg there is no hedge and the
        strategy is not a guaranteed-profit arbitrage.  Re-enable only when
        Derive adds spot markets.
        """
        return []

    def _scan_box_spreads(self, underlying: str) -> list[Opportunity]:
        """Scan for Box Spread arbitrage.

        Fee model (source: https://docs.derive.xyz/reference/fees-1):
          Box spreads use a SPECIAL fee schedule that replaces the standard
          per-leg RFQ fees entirely:
            box_fee = (K2 - K1) * 1% * years_to_expiry   [both sides]
            base_fee = $0.50                               [taker only]
          Total taker fee = box_fee + $0.50
        """
        opportunities: list[Opportunity] = []
        spot = self._orderbook.get_spot_price(underlying)

        if spot is None:
            return opportunities

        now_ms = int(time.time() * 1000)

        _BOX_YIELD_RATE = Decimal("0.01")  # 1% yield spread per year (Derive docs)
        _BOX_BASE_FEE = Decimal("0.50")  # taker-side base fee

        for expiry in self._orderbook.get_expiries(underlying):
            tte = self._time_to_expiry_years(expiry)
            if tte <= ZERO:
                continue

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
                    # Derive uses r=0: fair value of the box today = K2-K1 (no discount).
                    gross_profit = payoff - total_cost

                    if gross_profit <= ZERO:
                        continue

                    legs = (
                        Leg(c1.instrument, Side.BUY, c1.ask, ONE, c1.ask_size),
                        Leg(c2.instrument, Side.SELL, c2.bid, ONE, c2.bid_size),
                        Leg(p1.instrument, Side.SELL, p1.bid, ONE, p1.bid_size),
                        Leg(p2.instrument, Side.BUY, p2.ask, ONE, p2.ask_size),
                    )

                    # Box spread fee schedule replaces standard per-leg RFQ fees.
                    # notional = K2 - K1  (NOT spot — see Derive docs example)
                    total_fees = payoff * _BOX_YIELD_RATE * tte + _BOX_BASE_FEE

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
        """Scan for Negative-Cost Butterfly spreads (calls and puts).

        A negative-cost butterfly is a net credit: buy the wings (K1, K3) and
        sell 2x the body (K2) where K1 < K2 < K3 and K2 - K1 = K3 - K2.
        The initial credit is the guaranteed minimum profit (payoff ≥ 0).

        Both call and put variants are scanned.
        """
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
            strikes_set = set(strikes)  # O(1) membership test

            for i, k1 in enumerate(strikes[:-2]):
                for k2 in strikes[i + 1 : -1]:
                    k3_target = k2 + (k2 - k1)

                    if k3_target not in strikes_set:
                        continue

                    k3 = k3_target

                    # ── Call butterfly ─────────────────────────────────────
                    c1 = self._orderbook.get_option(underlying, expiry, k1, OptionType.CALL)
                    c2 = self._orderbook.get_option(underlying, expiry, k2, OptionType.CALL)
                    c3 = self._orderbook.get_option(underlying, expiry, k3, OptionType.CALL)

                    if all([c1, c2, c3]):
                        assert c1 is not None
                        assert c2 is not None
                        assert c3 is not None

                        if all(self._is_fresh(q) for q in [c1, c2, c3]):
                            net_cost = c1.ask - (TWO * c2.bid) + c3.ask

                            if net_cost < ZERO:
                                net_credit = -net_cost
                                legs: tuple[Leg, ...] = (
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
                                            "[EVALUATOR] NEG_BUTTERFLY(C) | %s"
                                            " | strikes=%s/%s/%s | profit=$%.2f",
                                            underlying,
                                            k1,
                                            k2,
                                            k3,
                                            float(net_profit_at_entry),
                                        )

                    # ── Put butterfly ──────────────────────────────────────
                    p1 = self._orderbook.get_option(underlying, expiry, k1, OptionType.PUT)
                    p2 = self._orderbook.get_option(underlying, expiry, k2, OptionType.PUT)
                    p3 = self._orderbook.get_option(underlying, expiry, k3, OptionType.PUT)

                    if all([p1, p2, p3]):
                        assert p1 is not None
                        assert p2 is not None
                        assert p3 is not None

                        if all(self._is_fresh(q) for q in [p1, p2, p3]):
                            net_cost_p = p1.ask - (TWO * p2.bid) + p3.ask

                            if net_cost_p < ZERO:
                                net_credit_p = -net_cost_p
                                legs_p: tuple[Leg, ...] = (
                                    Leg(p1.instrument, Side.BUY, p1.ask, ONE, p1.ask_size),
                                    Leg(p2.instrument, Side.SELL, p2.bid, TWO, p2.bid_size / TWO),
                                    Leg(p3.instrument, Side.BUY, p3.ask, ONE, p3.ask_size),
                                )
                                fees_p = self._estimate_fees(legs_p, spot)
                                net_profit_p = net_credit_p - fees_p

                                if net_profit_p > ZERO:
                                    min_size_p = min(p1.ask_size, p2.bid_size / TWO, p3.ask_size)
                                    if min_size_p >= self._config.min_trade_size:
                                        opportunities.append(
                                            Opportunity(
                                                arb_type=ArbType.NEG_BUTTERFLY,
                                                legs=legs_p,
                                                gross_credit=net_credit_p,
                                                total_fees=fees_p,
                                                net_profit=net_profit_p,
                                                guaranteed_floor=net_profit_p,
                                                expiry=expiry,
                                                underlying=underlying,
                                                timestamp_ms=now_ms,
                                            )
                                        )
                                        logger.debug(
                                            "[EVALUATOR] NEG_BUTTERFLY(P) | %s"
                                            " | strikes=%s/%s/%s | profit=$%.2f",
                                            underlying,
                                            k1,
                                            k2,
                                            k3,
                                            float(net_profit_p),
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

    # NOTE: _discount_factor() has been intentionally removed.
    # Derive's smart contract specifies INTEREST_RATE = 0.00 (see asset-parameters-1
    # in the protocol docs). All option pricing uses Black76 with r=0; the forward
    # price is supplied by the Block Scholes oracle (option_pricing.f in ticker_slim).
    # Using a static US-Treasury rate (e.g. 5%) artificially inflates the apparent
    # forward price for high strikes and produces phantom arbitrage at every strike.

    def _estimate_fees(
        self,
        legs: tuple[Leg, ...],
        spot: Decimal,
    ) -> Decimal:
        """Estimate total taker execution fees for a multi-leg RFQ.

        Fee formula per leg (source: https://docs.derive.xyz/reference/fees-1):
          taker_fee = $0.50 + 0.03% * notional_volume
          where notional_volume = contract_size * spot_price

        Capped at 12.5% of the option's value per leg:
          cap = 12.5% * (leg.price * leg.size)

        Multi-leg RFQ discount (grouping: long calls / short calls /
        long puts / short puts / perps):
          - Cheapest group  → 100% discount (zeroed)
          - 2nd/3rd cheapest groups → 50% discount
          - Most expensive  → full fee
        """
        _TAKER_BASE = Decimal("0.50")
        _TAKER_RATE = Decimal("0.0003")  # 0.03% per notional
        _OPTION_CAP = Decimal("0.125")  # 12.5% of option value

        leg_fees: list[Decimal] = []

        for leg in legs:
            notional = leg.size * spot
            raw_fee = _TAKER_BASE + notional * _TAKER_RATE

            # Apply the 12.5% option value cap.  Cap is 0 only if the leg is
            # a perp or has no price, in which case no cap is applied.
            option_value = leg.price * leg.size
            if option_value > ZERO:
                fee_cap = option_value * _OPTION_CAP
                leg_fee = min(raw_fee, fee_cap)
            else:
                leg_fee = raw_fee

            leg_fees.append(leg_fee)

        # Apply RFQ multi-leg discount (sort ascending = cheapest first).
        leg_fees.sort()

        if len(leg_fees) >= 2:
            leg_fees[0] = ZERO  # cheapest group: 100% discount
            if len(leg_fees) >= 3:
                leg_fees[1] *= Decimal("0.5")  # 2nd cheapest: 50% discount
            if len(leg_fees) >= 4:
                leg_fees[2] *= Decimal("0.5")  # 3rd cheapest: 50% discount

        return sum(leg_fees, ZERO)
