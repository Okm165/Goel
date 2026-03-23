"""Configuration management.

Uses discriminated union pattern to avoid nullable fields.
ObserverConfig for observation mode, ExecutorConfig for execution mode.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Final

# =============================================================================
# Defaults
# =============================================================================

DEFAULT_WS_URL: Final[str] = "wss://api.lyra.finance/ws"
DEFAULT_REST_URL: Final[str] = "https://api.lyra.finance"
DEFAULT_MIN_PROFIT_USD: Final[Decimal] = Decimal("1.00")
DEFAULT_MAX_QUOTE_AGE_MS: Final[int] = 1000
DEFAULT_MIN_TRADE_SIZE: Final[Decimal] = Decimal("0.1")
DEFAULT_RISK_FREE_RATE: Final[Decimal] = Decimal("0.05")
DEFAULT_UNDERLYINGS: Final[tuple[str, ...]] = ("ETH", "BTC")
DEFAULT_LOG_LEVEL: Final[str] = "INFO"
DEFAULT_OUTPUT_CSV: Final[str] = "opportunities.csv"


# =============================================================================
# Exceptions
# =============================================================================


class ConfigError(Exception):
    """Configuration validation error."""


# =============================================================================
# Base Configuration (shared fields)
# =============================================================================


@dataclass(frozen=True, slots=True)
class BaseConfig:
    """Shared configuration fields."""

    ws_url: str
    rest_url: str
    min_profit_usd: Decimal
    max_quote_age_ms: int
    min_trade_size: Decimal
    risk_free_rate: Decimal
    underlyings: tuple[str, ...]
    log_level: str
    output_csv: str


@dataclass(frozen=True, slots=True)
class ObserverConfig(BaseConfig):
    """Configuration for observation mode - no credentials needed."""


@dataclass(frozen=True, slots=True)
class ExecutorConfig(BaseConfig):
    """Configuration for execution mode - credentials required."""

    session_key_private: str
    subaccount_id: int


# Discriminated union - no nullable fields
Config = ObserverConfig | ExecutorConfig


# =============================================================================
# Helper Functions
# =============================================================================


def _get_bool(key: str, default: bool) -> bool:
    """Parse boolean from environment variable."""
    value = os.getenv(key, str(default)).lower()
    return value in ("true", "1", "yes", "on")


def _get_decimal(key: str, default: Decimal) -> Decimal:
    """Parse Decimal from environment variable."""
    value = os.getenv(key)
    if value is None:
        return default
    try:
        return Decimal(value)
    except InvalidOperation as e:
        msg = f"Invalid decimal value for {key}: {value}"
        raise ConfigError(msg) from e


def _get_int(key: str, default: int) -> int:
    """Parse integer from environment variable."""
    value = os.getenv(key)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError as e:
        msg = f"Invalid integer value for {key}: {value}"
        raise ConfigError(msg) from e


def _get_list(key: str, default: tuple[str, ...]) -> tuple[str, ...]:
    """Parse comma-separated list from environment variable."""
    value = os.getenv(key)
    if value is None:
        return default
    items = [item.strip().upper() for item in value.split(",") if item.strip()]
    return tuple(items) if items else default


def _validate_base(
    min_profit_usd: Decimal,
    max_quote_age_ms: int,
    min_trade_size: Decimal,
    underlyings: tuple[str, ...],
) -> None:
    """Validate common configuration fields."""
    if min_profit_usd < ZERO:
        msg = "MIN_PROFIT_USD must be non-negative"
        raise ConfigError(msg)

    if max_quote_age_ms <= 0:
        msg = "MAX_QUOTE_AGE_MS must be positive"
        raise ConfigError(msg)

    if min_trade_size <= ZERO:
        msg = "MIN_TRADE_SIZE must be positive"
        raise ConfigError(msg)

    if not underlyings:
        msg = "At least one underlying asset must be specified"
        raise ConfigError(msg)


ZERO: Final[Decimal] = Decimal(0)


# =============================================================================
# Config Loader
# =============================================================================


def load_config() -> Config:
    """Load configuration from environment variables.

    Returns ObserverConfig if ENABLE_EXECUTOR=false (default),
    ExecutorConfig if ENABLE_EXECUTOR=true.

    Returns:
        Validated Config instance (ObserverConfig or ExecutorConfig)

    Raises:
        ConfigError: If configuration is invalid
    """
    ws_url = os.getenv("DERIVE_WS_URL", DEFAULT_WS_URL)
    rest_url = os.getenv("DERIVE_REST_URL", DEFAULT_REST_URL)
    min_profit_usd = _get_decimal("MIN_PROFIT_USD", DEFAULT_MIN_PROFIT_USD)
    max_quote_age_ms = _get_int("MAX_QUOTE_AGE_MS", DEFAULT_MAX_QUOTE_AGE_MS)
    min_trade_size = _get_decimal("MIN_TRADE_SIZE", DEFAULT_MIN_TRADE_SIZE)
    risk_free_rate = _get_decimal("RISK_FREE_RATE", DEFAULT_RISK_FREE_RATE)
    underlyings = _get_list("UNDERLYINGS", DEFAULT_UNDERLYINGS)
    log_level = os.getenv("LOG_LEVEL", DEFAULT_LOG_LEVEL).upper()
    output_csv = os.getenv("OUTPUT_CSV", DEFAULT_OUTPUT_CSV)

    _validate_base(min_profit_usd, max_quote_age_ms, min_trade_size, underlyings)

    enable_executor = _get_bool("ENABLE_EXECUTOR", default=False)

    if enable_executor:
        session_key = os.getenv("SESSION_KEY_PRIVATE")
        if not session_key:
            msg = "SESSION_KEY_PRIVATE is required when ENABLE_EXECUTOR=true"
            raise ConfigError(msg)

        subaccount_str = os.getenv("SUBACCOUNT_ID")
        if not subaccount_str:
            msg = "SUBACCOUNT_ID is required when ENABLE_EXECUTOR=true"
            raise ConfigError(msg)

        try:
            subaccount_id = int(subaccount_str)
        except ValueError as e:
            msg = f"Invalid SUBACCOUNT_ID: {subaccount_str}"
            raise ConfigError(msg) from e

        return ExecutorConfig(
            ws_url=ws_url,
            rest_url=rest_url,
            min_profit_usd=min_profit_usd,
            max_quote_age_ms=max_quote_age_ms,
            min_trade_size=min_trade_size,
            risk_free_rate=risk_free_rate,
            underlyings=underlyings,
            log_level=log_level,
            output_csv=output_csv,
            session_key_private=session_key,
            subaccount_id=subaccount_id,
        )

    return ObserverConfig(
        ws_url=ws_url,
        rest_url=rest_url,
        min_profit_usd=min_profit_usd,
        max_quote_age_ms=max_quote_age_ms,
        min_trade_size=min_trade_size,
        risk_free_rate=risk_free_rate,
        underlyings=underlyings,
        log_level=log_level,
        output_csv=output_csv,
    )


def is_executor_mode(config: Config) -> bool:
    """Check if config is for execution mode."""
    return isinstance(config, ExecutorConfig)
