# Derive HFT Options Arbitrage Bot

A high-frequency trading bot designed to exploit order book inefficiencies on the [Derive Protocol](https://derive.xyz) through deterministic, zero-cost arbitrage strategies.

## Features

- **Deterministic Arbitrage**: Identifies mathematically provable t=0 opportunities
- **Zero-Loss Constraint**: Every position guarantees non-negative worst-case P&L
- **Multi-Strategy Support**: Conversion/Reversal, Zero-Cost Collar, Box Spread, Negative Butterfly
- **Observation Mode**: Run without credentials to validate strategy viability
- **Execution Mode**: Full atomic RFQ execution with Fill-or-Kill orders

## Quick Start

### Prerequisites

- Python 3.12+
- [uv](https://docs.astral.sh/uv/) (recommended) or pip

### Installation

```bash
# Clone the repository
git clone https://github.com/your-org/market-bot.git
cd market-bot

# Install with uv (recommended)
uv sync

# Or with pip
pip install -e .
```

### Configuration

```bash
# Copy example environment file
cp .env.example .env

# Edit configuration
vim .env
```

### Running

```bash
# Observation mode (default - no credentials needed)
uv run python -m bot

# Or with pip-installed package
python -m bot

# Execution mode (requires credentials)
ENABLE_EXECUTOR=true python -m bot
```

### Docker

```bash
# Build and run
docker-compose up bot

# Development mode with hot reload
docker-compose --profile dev up dev

# Run CI checks
docker-compose --profile ci up lint typecheck test
```

## Development

### Setup

```bash
# Install with dev dependencies
uv sync --all-extras

# Or with pip
pip install -e ".[dev,fast]"
```

### Quality Checks

```bash
# Format code
ruff format src tests

# Lint
ruff check src tests

# Type check
pyright src

# Run tests
pytest
```

## Architecture

```
src/bot/
├── __init__.py     # Package marker
├── __main__.py     # Entry point
├── config.py       # Configuration management
├── types.py        # Domain types (Quote, Opportunity, etc.)
├── client.py       # WebSocket/REST gateway
├── orderbook.py    # In-memory state management
├── evaluator.py    # Arbitrage scanner
├── risk.py         # Validation and proofs
├── executor.py     # RFQ execution (optional)
└── main.py         # Orchestrator
```

## Strategies

| Strategy | Description | Risk Profile |
|----------|-------------|--------------|
| **Conversion** | Put-Call Parity: Long S + Long P + Short C | Risk-free |
| **Reversal** | Put-Call Parity: Short S + Short P + Long C | Risk-free |
| **Zero-Cost Collar** | Long S + Long P + Short C (different strikes) | Floor guaranteed |
| **Box Spread** | Bull Call + Bear Put spread | Risk-free |
| **Negative Butterfly** | Butterfly acquired at net credit | Max loss = $0 |

## Configuration Options

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `DERIVE_WS_URL` | Yes | `wss://api.derive.xyz/ws` | WebSocket endpoint |
| `DERIVE_REST_URL` | Yes | `https://api.derive.xyz` | REST API endpoint |
| `ENABLE_EXECUTOR` | No | `false` | Enable live execution |
| `SESSION_KEY_PRIVATE` | When executing | - | Session key for signing |
| `SUBACCOUNT_ID` | When executing | - | Trading subaccount |
| `MIN_PROFIT_USD` | No | `1.00` | Minimum profit threshold |
| `MAX_QUOTE_AGE_MS` | No | `1000` | Quote staleness limit |
| `UNDERLYINGS` | No | `ETH,BTC` | Assets to monitor |

## License

MIT

## Disclaimer

This software is for educational purposes only. Trading involves risk. Use at your own discretion.
