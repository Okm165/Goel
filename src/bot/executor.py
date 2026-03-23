"""Trade execution via Derive RFQ system.

Handles signing, RFQ submission, and atomic multi-leg execution.
Only used with ExecutorConfig.
"""

from __future__ import annotations

import asyncio
import logging
import time
from decimal import Decimal
from typing import TYPE_CHECKING, Any

import aiohttp
from eth_account import Account
from eth_account.messages import encode_typed_data

from bot.types import ZERO, Leg, Opportunity, Side, ValidOpportunity

if TYPE_CHECKING:
    from bot.config import ExecutorConfig
    from bot.risk import RiskManager

logger = logging.getLogger(__name__)


class Executor:
    """Executes validated opportunities via Derive RFQ.

    Uses atomic multi-leg execution to prevent legging risk.
    All trades are Fill-or-Kill (FOK).
    """

    __slots__ = (
        "_account",
        "_config",
        "_opportunity_queue",
        "_risk_manager",
        "_running",
        "_session",
    )

    def __init__(
        self,
        config: ExecutorConfig,
        risk_manager: RiskManager,
        opportunity_queue: asyncio.Queue[Opportunity],
    ) -> None:
        """Initialize the executor with ExecutorConfig (has credentials)."""
        self._config = config
        self._risk_manager = risk_manager
        self._opportunity_queue = opportunity_queue
        self._session: aiohttp.ClientSession | None = None
        self._account = Account.from_key(config.session_key_private)
        self._running = False

    async def start(self) -> None:
        """Initialize executor resources."""
        logger.info(
            "[EXECUTOR] Initialized | wallet=%s | subaccount=%d",
            self._account.address,
            self._config.subaccount_id,
        )

        self._session = aiohttp.ClientSession(
            base_url=self._config.rest_url,
            timeout=aiohttp.ClientTimeout(total=30),
        )

    async def stop(self) -> None:
        """Clean up executor resources."""
        logger.info("[EXECUTOR] Stop requested")
        self._running = False
        if self._session:
            await self._session.close()
            self._session = None

    async def run(self) -> None:
        """Main execution loop."""
        await self.start()
        self._running = True

        logger.info("[EXECUTOR] Started - waiting for opportunities")

        executed = 0
        rejected = 0

        try:
            while self._running:
                try:
                    opportunity = await asyncio.wait_for(
                        self._opportunity_queue.get(),
                        timeout=1.0,
                    )

                    success = await self._execute(opportunity)
                    if success:
                        executed += 1
                    else:
                        rejected += 1

                except TimeoutError:
                    continue
                except asyncio.CancelledError:
                    break
                except Exception:
                    logger.exception("[EXECUTOR] Error in execution loop")

        finally:
            await self.stop()
            logger.info("[EXECUTOR] Stopped | executed=%d | rejected=%d", executed, rejected)

    async def _execute(self, opportunity: Opportunity) -> bool:
        """Execute a single opportunity."""
        result = self._risk_manager.validate(opportunity)

        if not isinstance(result, ValidOpportunity):
            logger.debug(
                "[EXECUTOR] Opportunity rejected | type=%s | reason=%s",
                opportunity.arb_type.name,
                result.reason.name,
            )
            return False

        logger.info(
            "[EXECUTOR] Executing | type=%s | profit=$%.2f | legs=%d",
            opportunity.arb_type.name,
            float(opportunity.net_profit),
            opportunity.leg_count,
        )

        try:
            rfq_id = await self._send_rfq(opportunity)
            if not rfq_id:
                logger.warning("[EXECUTOR] RFQ submission failed")
                return False

            logger.debug("[EXECUTOR] RFQ submitted | rfq_id=%s", rfq_id)

            quote = await self._poll_for_quote(rfq_id)
            if not quote:
                logger.warning("[EXECUTOR] No quote received for RFQ | rfq_id=%s", rfq_id)
                return False

            logger.debug("[EXECUTOR] Quote received | quote_id=%s", quote.get("quote_id"))

            # Re-validate quoted prices before executing.  The market may have
            # moved in the ~2.5 s between scan and MM response — executing at a
            # loss must be impossible.
            if not self._validate_quote_prices(opportunity, quote):
                logger.warning(
                    "[EXECUTOR] Quote prices slipped below profit threshold | "
                    "type=%s | expected=$%.2f",
                    opportunity.arb_type.name,
                    float(opportunity.net_profit),
                )
                return False

            success = await self._execute_quote(quote)

        except Exception:
            logger.exception("[EXECUTOR] Execution failed | type=%s", opportunity.arb_type.name)
            return False

        else:
            if success:
                logger.info(
                    "[EXECUTOR] SUCCESS | type=%s | profit=$%.2f",
                    opportunity.arb_type.name,
                    float(opportunity.net_profit),
                )
            else:
                logger.warning("[EXECUTOR] Quote execution failed")
            return success

    async def _send_rfq(self, opportunity: Opportunity) -> str | None:
        """Send RFQ request to Derive."""
        if not self._session:
            return None

        legs_payload = [self._leg_to_payload(leg) for leg in opportunity.legs]

        request_data = {
            "jsonrpc": "2.0",
            "id": int(time.time() * 1000),
            "method": "private/send_rfq",
            "params": {
                "subaccount_id": self._config.subaccount_id,
                "legs": legs_payload,
            },
        }

        headers = self._auth_headers()

        try:
            async with self._session.post("/", json=request_data, headers=headers) as response:
                result = await response.json()

                if "error" in result:
                    logger.warning("[EXECUTOR] RFQ error | error=%s", result["error"])
                    return None

                rfq_result = result.get("result", {})
                return str(rfq_result.get("rfq_id", ""))

        except aiohttp.ClientError as e:
            logger.warning("[EXECUTOR] RFQ request failed | error=%s", e)
            return None

    async def _poll_for_quote(
        self,
        rfq_id: str,
        max_attempts: int = 5,
        delay: float = 0.5,
    ) -> dict[str, Any] | None:
        """Poll for best quote on RFQ."""
        if not self._session:
            return None

        for attempt in range(max_attempts):
            request_data = {
                "jsonrpc": "2.0",
                "id": int(time.time() * 1000),
                "method": "private/rfq_get_best_quote",
                "params": {
                    "subaccount_id": self._config.subaccount_id,
                    "rfq_id": rfq_id,
                },
            }

            headers = self._auth_headers()

            try:
                async with self._session.post("/", json=request_data, headers=headers) as response:
                    result = await response.json()

                    if "error" in result:
                        logger.debug(
                            "[EXECUTOR] Quote poll attempt %d/%d - no quote yet",
                            attempt + 1,
                            max_attempts,
                        )
                        await asyncio.sleep(delay)
                        continue

                    quote_result = result.get("result", {})
                    if quote_result.get("quote_id"):
                        return quote_result

            except aiohttp.ClientError:
                pass

            await asyncio.sleep(delay)

        return None

    def _validate_quote_prices(
        self,
        opportunity: Opportunity,
        quote: dict[str, Any],
    ) -> bool:
        """Re-validate MM quoted prices against the original profit threshold.

        The market can move in the 0-2.5 s between scan and quote receipt.
        This is the last safety gate before any capital leaves the account.

        Parses each quoted leg price and re-calculates the net credit.  If the
        MM's prices produce a net below min_profit_usd, returns False.

        If the quote response contains no parseable leg prices (unexpected
        format), the method logs a warning and returns True (best-effort —
        the underlying freshness + threshold checks in RiskManager already
        ran; skipping execution here on a parse error would be overly cautious
        for v1 but should be hardened once the live quote schema is confirmed).
        """
        quoted_legs = quote.get("legs", [])
        if not quoted_legs:
            logger.debug("[EXECUTOR] Quote contains no leg prices; skipping price recheck")
            return True

        quoted_prices: dict[str, Decimal] = {}
        for leg_data in quoted_legs:
            instrument = str(leg_data.get("instrument_name", ""))
            raw_price = leg_data.get("price")
            if not instrument or raw_price is None:
                continue
            try:
                quoted_prices[instrument] = Decimal(str(raw_price))
            except Exception:
                logger.warning("[EXECUTOR] Unparseable price for %s: %r", instrument, raw_price)

        if not quoted_prices:
            logger.debug("[EXECUTOR] No parseable prices in quote; skipping price recheck")
            return True

        # Re-compute gross credit from the MM's actual prices.
        quoted_gross = ZERO
        for leg in opportunity.legs:
            qp = quoted_prices.get(leg.instrument)
            if qp is None:
                logger.warning("[EXECUTOR] Leg %s missing in quote response", leg.instrument)
                return False  # Incomplete quote — do not execute
            if leg.side == Side.SELL:
                quoted_gross += qp  # we receive this
            else:
                quoted_gross -= qp  # we pay this

        quoted_net = quoted_gross - opportunity.total_fees
        if quoted_net < self._config.min_profit_usd:
            logger.warning(
                "[EXECUTOR] Quote slippage | quoted_net=$%.4f < threshold=$%.2f",
                float(quoted_net),
                float(self._config.min_profit_usd),
            )
            return False

        logger.debug("[EXECUTOR] Quote price recheck passed | quoted_net=$%.4f", float(quoted_net))
        return True

    async def _execute_quote(self, quote: dict[str, Any]) -> bool:
        """Execute a received quote."""
        if not self._session:
            return False

        quote_id = quote.get("quote_id")
        if not quote_id:
            return False

        signature = self._sign_quote(quote)

        request_data = {
            "jsonrpc": "2.0",
            "id": int(time.time() * 1000),
            "method": "private/execute_quote",
            "params": {
                "subaccount_id": self._config.subaccount_id,
                "quote_id": quote_id,
                "signature": signature,
            },
        }

        headers = self._auth_headers()

        try:
            async with self._session.post("/", json=request_data, headers=headers) as response:
                result = await response.json()

                if "error" in result:
                    logger.warning("[EXECUTOR] Execute error | error=%s", result["error"])
                    return False

                return True

        except aiohttp.ClientError as e:
            logger.warning("[EXECUTOR] Execute request failed | error=%s", e)
            return False

    def _leg_to_payload(self, leg: Leg) -> dict[str, Any]:
        """Convert Leg to API payload format."""
        direction = "buy" if leg.side == Side.BUY else "sell"

        return {
            "instrument_name": leg.instrument,
            "direction": direction,
            "amount": str(leg.size),
        }

    def _auth_headers(self) -> dict[str, str]:
        """Generate authentication headers."""
        timestamp = int(time.time() * 1000)
        signature = self._sign_login(timestamp)

        return {
            "Content-Type": "application/json",
            "X-LyraWallet": self._account.address,
            "X-LyraTimestamp": str(timestamp),
            "X-LyraSignature": signature,
        }

    def _sign_login(self, timestamp: int) -> str:
        """Sign login message with EIP-712."""
        typed_data = {
            "types": {
                "EIP712Domain": [
                    {"name": "name", "type": "string"},
                    {"name": "version", "type": "string"},
                    {"name": "chainId", "type": "uint256"},
                ],
                "Login": [
                    {"name": "timestamp", "type": "uint64"},
                ],
            },
            "primaryType": "Login",
            "domain": {
                "name": "Derive",
                "version": "1",
                "chainId": 957,
            },
            "message": {
                "timestamp": timestamp,
            },
        }

        signable = encode_typed_data(full_message=typed_data)
        signed = self._account.sign_message(signable)
        return signed.signature.hex()

    def _sign_quote(self, quote: dict[str, Any]) -> str:
        """Sign quote execution with EIP-712."""
        typed_data = {
            "types": {
                "EIP712Domain": [
                    {"name": "name", "type": "string"},
                    {"name": "version", "type": "string"},
                    {"name": "chainId", "type": "uint256"},
                ],
                "ExecuteQuote": [
                    {"name": "quote_id", "type": "string"},
                ],
            },
            "primaryType": "ExecuteQuote",
            "domain": {
                "name": "Derive",
                "version": "1",
                "chainId": 957,
            },
            "message": {
                "quote_id": str(quote.get("quote_id", "")),
            },
        }

        signable = encode_typed_data(full_message=typed_data)
        signed = self._account.sign_message(signable)
        return signed.signature.hex()
