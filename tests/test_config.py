"""Tests for configuration management."""

from __future__ import annotations

import os
from decimal import Decimal
from unittest import mock

import pytest

from bot.config import (
    ConfigError,
    ExecutorConfig,
    ObserverConfig,
    load_config,
)


class TestLoadConfig:
    """Tests for configuration loading."""

    def test_observer_mode_default(self) -> None:
        """Test loading observer config with defaults."""
        env = {
            "DERIVE_WS_URL": "wss://test.example.com/ws",
            "DERIVE_REST_URL": "https://test.example.com",
        }
        with mock.patch.dict(os.environ, env, clear=True):
            config = load_config()

            assert isinstance(config, ObserverConfig)
            assert config.ws_url == "wss://test.example.com/ws"
            assert config.rest_url == "https://test.example.com"
            assert config.min_profit_usd == Decimal("1.00")
            assert config.max_quote_age_ms == 1000
            assert config.underlyings == ("ETH", "BTC")

    def test_executor_mode_requires_credentials(self) -> None:
        """Test that executor mode requires credentials."""
        env = {"ENABLE_EXECUTOR": "true"}
        with (
            mock.patch.dict(os.environ, env, clear=True),
            pytest.raises(ConfigError, match="SESSION_KEY_PRIVATE is required"),
        ):
            load_config()

    def test_executor_mode_requires_subaccount(self) -> None:
        """Test that executor mode requires subaccount."""
        env = {
            "ENABLE_EXECUTOR": "true",
            "SESSION_KEY_PRIVATE": "0x1234567890abcdef",
        }
        with (
            mock.patch.dict(os.environ, env, clear=True),
            pytest.raises(ConfigError, match="SUBACCOUNT_ID is required"),
        ):
            load_config()

    def test_executor_mode_full(self) -> None:
        """Test executor mode with all credentials."""
        env = {
            "ENABLE_EXECUTOR": "true",
            "SESSION_KEY_PRIVATE": "0x1234567890abcdef",
            "SUBACCOUNT_ID": "12345",
        }
        with mock.patch.dict(os.environ, env, clear=True):
            config = load_config()

            assert isinstance(config, ExecutorConfig)
            assert config.session_key_private == "0x1234567890abcdef"
            assert config.subaccount_id == 12345

    def test_invalid_subaccount_id(self) -> None:
        """Test invalid subaccount ID raises error."""
        env = {
            "ENABLE_EXECUTOR": "true",
            "SESSION_KEY_PRIVATE": "0xabc",
            "SUBACCOUNT_ID": "not_a_number",
        }
        with (
            mock.patch.dict(os.environ, env, clear=True),
            pytest.raises(ConfigError, match="Invalid SUBACCOUNT_ID"),
        ):
            load_config()

    def test_custom_parameters(self) -> None:
        """Test loading with custom parameters."""
        env = {
            "MIN_PROFIT_USD": "5.00",
            "MAX_QUOTE_AGE_MS": "500",
            "MIN_TRADE_SIZE": "0.5",
            "RISK_FREE_RATE": "0.03",
            "UNDERLYINGS": "ETH,SOL",
            "LOG_LEVEL": "debug",
        }
        with mock.patch.dict(os.environ, env, clear=True):
            config = load_config()

            assert config.min_profit_usd == Decimal("5.00")
            assert config.max_quote_age_ms == 500
            assert config.min_trade_size == Decimal("0.5")
            assert config.risk_free_rate == Decimal("0.03")
            assert config.underlyings == ("ETH", "SOL")
            assert config.log_level == "DEBUG"

    def test_invalid_min_profit(self) -> None:
        """Test negative min profit raises error."""
        env = {"MIN_PROFIT_USD": "-1.00"}
        with (
            mock.patch.dict(os.environ, env, clear=True),
            pytest.raises(ConfigError, match="MIN_PROFIT_USD must be non-negative"),
        ):
            load_config()

    def test_invalid_max_quote_age(self) -> None:
        """Test non-positive quote age raises error."""
        env = {"MAX_QUOTE_AGE_MS": "0"}
        with (
            mock.patch.dict(os.environ, env, clear=True),
            pytest.raises(ConfigError, match="MAX_QUOTE_AGE_MS must be positive"),
        ):
            load_config()
