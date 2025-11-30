#!/usr/bin/env python3
"""
Kalshi/Polymarket Cross-Platform Arbitrage Bot

Usage:
    python main.py [--dry-run] [--verbose]

Options:
    --dry-run       Run in simulation mode without executing trades
    --verbose       Enable debug logging
"""

import argparse
import asyncio
import logging
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))

from src.bot import ArbitrageBot
from src.config import MarketCategory, load_config
from src.mappings import load_mappings_from_file


def setup_logging(verbose: bool = False) -> None:
    """Configure logging."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Reduce noise from third-party libraries
    logging.getLogger("websockets").setLevel(logging.WARNING)
    logging.getLogger("aiohttp").setLevel(logging.WARNING)


async def run_bot(args: argparse.Namespace) -> None:
    """Main entry point for the bot."""
    logger = logging.getLogger(__name__)

    # Load configuration
    config = load_config()

    if args.dry_run:
        logger.info("Running in DRY RUN mode - no trades will be executed")

    # Create bot
    bot = ArbitrageBot(config)

    try:
        # Initialize connections
        logger.info("Initializing bot...")
        await bot.initialize()

        # Load contract mappings filtered by enabled categories
        enabled_categories = config.trading.enabled_categories
        logger.info(f"Enabled market categories: {enabled_categories}")

        mappings = load_mappings_from_file("contracts.json", categories=enabled_categories)
        if not mappings:
            logger.warning(
                "No contract mappings found for enabled categories! "
                "Please add mappings to contracts.json"
            )
            logger.info(
                "Run 'python scripts/discover_nba.py' to find NBA markets"
            )
        else:
            for pair in mappings:
                if pair.active:
                    bot.add_contract_pair(pair)
            logger.info(f"Loaded {len(mappings)} contract pairs for categories: {enabled_categories}")

        # Show status
        status = bot.get_status()
        logger.info(f"Bot status: {status}")

        # Run the bot
        logger.info("Starting bot (mode: WebSocket)")
        await bot.run()

    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.exception(f"Fatal error: {e}")
        raise
    finally:
        await bot.shutdown()


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Kalshi/Polymarket Arbitrage Bot",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simulation mode - don't execute real trades",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable debug logging",
    )

    args = parser.parse_args()
    setup_logging(args.verbose)

    asyncio.run(run_bot(args))


if __name__ == "__main__":
    main()
