"""
Main Arbitrage Bot Orchestrator

Coordinates all modules for the arbitrage trading system.
"""

import asyncio
import logging
import signal
from datetime import datetime
from typing import Optional

from .clients import KalshiClient, PolymarketClient
from .config import Config, load_config
from .models import ArbitrageOpportunity, ContractPair, TradeResult
from .modules import ArbitrageFinder, DataCollector, ResultsRecorder, TradeExecutor

logger = logging.getLogger(__name__)


class ArbitrageBot:
    """
    Main orchestrator for the Kalshi/Polymarket arbitrage system.

    Coordinates:
    - Data collection (Module A)
    - Opportunity detection (Module B)
    - Trade execution (Module C)
    """

    def __init__(self, config: Optional[Config] = None):
        self.config = config or load_config()

        # Initialize clients
        self.kalshi_client = KalshiClient(self.config.kalshi)
        self.polymarket_client = PolymarketClient(self.config.polymarket)

        # Initialize modules
        self.data_collector = DataCollector(
            self.config,
            self.kalshi_client,
            self.polymarket_client,
        )
        self.arbitrage_finder = ArbitrageFinder(self.config.trading)
        self.trade_executor = TradeExecutor(
            self.config.trading,
            self.kalshi_client,
            self.polymarket_client,
        )
        self.results_recorder = ResultsRecorder()

        # State
        self._running = False
        self._trade_count = 0
        self._total_profit = 0.0
        self._last_opportunity: Optional[ArbitrageOpportunity] = None

    async def initialize(self) -> None:
        """Initialize all clients and connections."""
        logger.info("Initializing arbitrage bot...")

        await self.kalshi_client.initialize()
        logger.info("Kalshi client initialized")

        await self.polymarket_client.initialize()
        logger.info("Polymarket client initialized")

        logger.info("Bot initialization complete")

    async def shutdown(self) -> None:
        """Gracefully shutdown the bot."""
        logger.info("Shutting down arbitrage bot...")

        self._running = False

        # Cancel all pending orders
        await self.trade_executor.cancel_all_orders()

        # Stop data collection
        await self.data_collector.stop()

        # Close client connections
        await self.kalshi_client.close()
        await self.polymarket_client.close()

        # Print final report
        logger.info(self.results_recorder.generate_report())

        logger.info(
            f"Bot shutdown complete. "
            f"Trades: {self._trade_count}, Total profit: ${self._total_profit:.2f}"
        )

    def add_contract_pair(self, pair: ContractPair) -> None:
        """Add a contract pair to monitor for arbitrage."""
        self.data_collector.add_contract_pair(pair)

    async def run_once(self) -> Optional[TradeResult]:
        """
        Run a single arbitrage cycle.

        1. Fetch quotes for all pairs
        2. Find opportunities
        3. Execute best opportunity if found

        Returns TradeResult if a trade was executed, None otherwise.
        """
        # Step 1: Fetch quotes
        quotes = await self.data_collector.fetch_quotes_polling()

        if not quotes:
            return None

        # Step 2: Prepare data for analysis
        pairs_data = {}
        for pair in self.data_collector.get_contract_pairs():
            if pair.event_name in quotes:
                pairs_data[pair.event_name] = {
                    "pm": quotes[pair.event_name]["pm"],
                    "kl": quotes[pair.event_name]["kl"],
                    "pair": pair,
                }

        # Step 3: Find opportunities
        opportunities = self.arbitrage_finder.analyze_all_pairs(pairs_data)

        if not opportunities:
            return None

        # Step 4: Execute best opportunity (highest profit rate)
        best_opportunity = opportunities[0]
        self._last_opportunity = best_opportunity

        logger.info(
            f"Executing opportunity: {best_opportunity.contract_pair.event_name} "
            f"mode={best_opportunity.mode} "
            f"profit_rate={best_opportunity.net_profit_rate:.4f}"
        )

        result = await self.trade_executor.execute(best_opportunity)

        # Record the trade result
        category = self.config.trading.enabled_categories[0] if self.config.trading.enabled_categories else "unknown"
        self.results_recorder.record_trade(best_opportunity, result, category=category)

        if result.success:
            self._trade_count += 1
            self._total_profit += result.net_profit
            logger.info(
                f"Trade #{self._trade_count} completed. "
                f"Profit: ${result.net_profit:.2f}, "
                f"Total: ${self._total_profit:.2f}"
            )

        return result

    async def run(
        self,
        use_websocket: bool = True,
        polling_interval: float = 1.0,
    ) -> None:
        """
        Main event loop for the arbitrage bot.

        Args:
            use_websocket: Use WebSocket for real-time data (preferred)
            polling_interval: Polling interval in seconds (fallback)
        """
        self._running = True
        logger.info("Starting arbitrage bot main loop")

        # Setup signal handlers for graceful shutdown
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda: asyncio.create_task(self.shutdown()))

        try:
            if use_websocket:
                # Start WebSocket streams
                await self.data_collector.start_websocket_streams()

                # Register quote callback to trigger analysis
                self.data_collector.on_quote_update(
                    lambda q: asyncio.create_task(self._on_quote_update())
                )

                # Keep running until stopped
                while self._running:
                    await asyncio.sleep(1)
            else:
                # Polling mode
                while self._running:
                    try:
                        await self.run_once()
                    except Exception as e:
                        logger.error(f"Error in main loop: {e}")

                    await asyncio.sleep(polling_interval)

        except asyncio.CancelledError:
            logger.info("Main loop cancelled")
        finally:
            await self.shutdown()

    async def _on_quote_update(self) -> None:
        """Callback triggered on quote updates to check for opportunities."""
        if not self._running:
            return

        # Debounce - don't execute too frequently
        # This is a simple implementation; production would use proper rate limiting

        try:
            await self.run_once()
        except Exception as e:
            logger.error(f"Error processing quote update: {e}")

    def get_status(self) -> dict:
        """Get current bot status."""
        stats = self.results_recorder.get_total_stats()
        return {
            "running": self._running,
            "trade_count": self._trade_count,
            "total_profit": self._total_profit,
            "active_orders": len(self.trade_executor.get_active_orders()),
            "monitored_pairs": len(self.data_collector.get_contract_pairs()),
            "win_rate": stats.get("win_rate", 0),
            "last_opportunity": (
                {
                    "event": self._last_opportunity.contract_pair.event_name,
                    "mode": self._last_opportunity.mode,
                    "profit_rate": self._last_opportunity.net_profit_rate,
                }
                if self._last_opportunity
                else None
            ),
        }

    def get_report(self) -> str:
        """Get performance report."""
        return self.results_recorder.generate_report()


async def main():
    """Entry point for running the bot."""
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # Load config and create bot
    config = load_config()
    bot = ArbitrageBot(config)

    # Initialize
    await bot.initialize()

    # Add contract pairs to monitor
    # Example - you would load these from a config file or API
    # bot.add_contract_pair(ContractPair(
    #     event_name="Example Event",
    #     polymarket_token_id="0x...",
    #     kalshi_ticker="TICKER",
    #     outcome="YES",
    # ))

    # Run the bot
    await bot.run(use_websocket=False, polling_interval=1.0)


if __name__ == "__main__":
    asyncio.run(main())
