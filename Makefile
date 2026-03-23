.PHONY: install lint format typecheck test check run clean

install:
	uv sync --all-extras

lint:
	uv run ruff check src tests

format:
	uv run ruff format src tests

typecheck:
	uv run pyright src

test:
	uv run pytest -v

check: lint typecheck test

run:
	uv run python -m bot

clean:
	rm -rf .venv __pycache__ .pytest_cache .ruff_cache
