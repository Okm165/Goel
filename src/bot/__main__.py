"""Package entry point.

Allows running the bot with: python -m bot
"""

from __future__ import annotations

import sys

from bot.main import main

if __name__ == "__main__":
    sys.exit(main())
