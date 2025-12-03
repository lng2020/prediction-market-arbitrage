"""
Main Arbitrage Bot Orchestrator

Coordinates all modules for the arbitrage trading system.
"""

import asyncio
import logging
import signal
import time
from datetime import datetime
from typing import Optional

from .clients import KalshiClient, PolymarketClient
from .config import Config, load_config
from .models import ArbitrageOpportunity, ContractPair, OrderType, Side, TradeResult
from .modules import ArbitrageFinder, DataCollector, PositionManager, ResultsRecorder, TradeExecutor

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
        self.position_manager = PositionManager(
            positions_file="data/positions.json",
            min_exit_profit_rate=self.config.trading.min_profit_target,
        )

        # State
        self._running = False
        self._exit_count = 0
        self._trade_count = 0
        self._total_profit = 0.0
        self._last_opportunity: Optional[ArbitrageOpportunity] = None

        # Debouncing for WebSocket mode
        self._last_analysis_time = 0.0
        self._analysis_interval = 0.2  # Reduced from 1.0s to catch more opportunities
        self._analysis_lock = asyncio.Lock()

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

        # Sell all open positions before shutdown
        await self._sell_all_positions()

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

    async def _sell_all_positions(self) -> None:
        """Sell all open positions on shutdown."""
        positions = self.position_manager.get_all_positions()
        if not positions:
            logger.info("No positions to sell on shutdown")
            return

        logger.info(f"Selling {len(positions)} position(s) on shutdown...")

        for position in positions:
            try:
                # Sell PM YES position
                logger.info(f"Selling PM {position.pm_quantity:.2f} shares of {position.contract_pair.event_name}")
                try:
                    pm_quote = await self.polymarket_client.get_quote(position.pm_token_id)
                    if pm_quote.bid > 0:
                        await self.polymarket_client.create_limit_order(
                            token_id=position.pm_token_id,
                            side=Side.SELL,
                            price=pm_quote.bid,
                            size=position.pm_quantity,
                        )
                        logger.info(f"  PM sell order placed at {pm_quote.bid:.4f}")
                except Exception as e:
                    logger.error(f"  PM sell failed: {e}")

                # Sell KL NO position
                logger.info(f"Selling KL {position.kl_quantity} NO contracts of {position.kl_ticker}")
                try:
                    kl_quote = await self.kalshi_client.get_quote(position.kl_ticker)
                    kl_no_bid = 1 - kl_quote.ask  # NO bid = 1 - YES ask
                    if kl_no_bid > 0:
                        await self.kalshi_client.create_order(
                            ticker=position.kl_ticker,
                            side=Side.SELL,
                            action="sell",
                            count=int(position.kl_quantity),
                            price_cents=int(kl_no_bid * 100),
                            order_type=OrderType.LIMIT,
                        )
                        logger.info(f"  KL sell order placed at {kl_no_bid:.4f}")
                except Exception as e:
                    logger.error(f"  KL sell failed: {e}")

                # Remove position from tracking
                self.position_manager.remove_position(position.position_id)

            except Exception as e:
                logger.error(f"Failed to sell position {position.position_id[:8]}: {e}")

    def add_contract_pair(self, pair: ContractPair) -> None:
        """Add a contract pair to monitor for arbitrage."""
        self.data_collector.add_contract_pair(pair)

    async def run_once(self) -> Optional[TradeResult]:
        """
        Run a single arbitrage cycle using cached WebSocket quotes.

        1. Get cached quotes for all pairs
        2. Check for exit opportunities on existing positions
        3. Find entry opportunities
        4. Execute best opportunity if found

        Returns TradeResult if a trade was executed, None otherwise.
        """
        # Step 1: Get cached quotes from WebSocket updates
        pairs_data = {}
        quotes_by_token = {}  # For exit opportunity lookup
        for pair in self.data_collector.get_contract_pairs():
            pm_quote, kl_quote = self.data_collector.get_pair_quotes(pair)
            if pm_quote and kl_quote:
                pairs_data[pair.event_name] = {
                    "pm": pm_quote,
                    "kl": kl_quote,
                    "pair": pair,
                }
                # Also index by token_id for position exit lookup
                quotes_by_token[pair.polymarket_token_id] = {
                    "pm": pm_quote,
                    "kl": kl_quote,
                }

        if not pairs_data:
            return None

        # Step 2: Check for exit opportunities on existing positions
        if self.position_manager.get_position_count() > 0:
            exit_opportunities = self.position_manager.find_all_exit_opportunities(quotes_by_token)
            if exit_opportunities:
                best_exit = exit_opportunities[0]
                logger.info(
                    f"Exit opportunity found: {best_exit.position.contract_pair.event_name} "
                    f"profit=${best_exit.profit:.2f} ({best_exit.profit_rate*100:.2f}%)"
                )

                exit_result = await self.trade_executor.execute_exit(best_exit)

                if exit_result.success:
                    # Remove the closed position
                    self.position_manager.remove_position(best_exit.position.position_id)
                    self._exit_count += 1
                    self._total_profit += exit_result.net_profit
                    logger.info(
                        f"Exit #{self._exit_count} completed. "
                        f"Profit: ${exit_result.net_profit:.2f}, "
                        f"Total: ${self._total_profit:.2f}"
                    )
                    return exit_result

        # Step 3: Find entry opportunities
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

            # Record the position for future exit tracking
            position = self.position_manager.record_position(best_opportunity, result)
            if position:
                logger.info(
                    f"Trade #{self._trade_count} completed, position recorded. "
                    f"Profit: ${result.net_profit:.2f}, "
                    f"Total: ${self._total_profit:.2f}, "
                    f"Open positions: {self.position_manager.get_position_count()}"
                )
            else:
                logger.info(
                    f"Trade #{self._trade_count} completed. "
                    f"Profit: ${result.net_profit:.2f}, "
                    f"Total: ${self._total_profit:.2f}"
                )

        return result

    async def run(self) -> None:
        """Main event loop for the arbitrage bot using WebSocket streams."""
        self._running = True
        self._analysis_count = 0
        logger.info("Starting arbitrage bot main loop")

        # Setup signal handlers for graceful shutdown
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda: asyncio.create_task(self.shutdown()))

        try:
            # Start WebSocket streams
            await self.data_collector.start_websocket_streams()

            # Register quote callback to trigger analysis
            self.data_collector.on_quote_update(
                lambda q: asyncio.create_task(self._on_quote_update())
            )

            # Keep running with periodic status updates
            heartbeat_interval = 30  # seconds
            seconds_elapsed = 0
            while self._running:
                await asyncio.sleep(1)
                seconds_elapsed += 1

                if seconds_elapsed >= heartbeat_interval:
                    seconds_elapsed = 0
                    self._log_heartbeat()

        except asyncio.CancelledError:
            logger.info("Main loop cancelled")
        finally:
            await self.shutdown()

    def _log_heartbeat(self) -> None:
        """Log periodic status update (verbose mode only)."""
        # Count how many pairs have cached quotes
        pairs_with_quotes = 0
        for pair in self.data_collector.get_contract_pairs():
            pm_quote, kl_quote = self.data_collector.get_pair_quotes(pair)
            if pm_quote and kl_quote:
                pairs_with_quotes += 1

        total_pairs = len(self.data_collector.get_contract_pairs())
        logger.debug(
            f"[Heartbeat] Pairs: {pairs_with_quotes}/{total_pairs} | "
            f"Analyses: {self._analysis_count} | "
            f"Trades: {self._trade_count}"
        )

    async def _on_quote_update(self) -> None:
        """Callback triggered on quote updates to check for opportunities."""
        if not self._running:
            return

        # Debounce - don't execute too frequently
        current_time = time.time()
        if current_time - self._last_analysis_time < self._analysis_interval:
            return  # Skip if too soon

        # Try to acquire lock (non-blocking)
        if self._analysis_lock.locked():
            return  # Skip if another analysis is in progress

        async with self._analysis_lock:
            self._last_analysis_time = time.time()
            self._analysis_count += 1
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
            "exit_count": self._exit_count,
            "total_profit": self._total_profit,
            "active_orders": len(self.trade_executor.get_active_orders()),
            "monitored_pairs": len(self.data_collector.get_contract_pairs()),
            "open_positions": self.position_manager.get_position_count(),
            "position_value": self.position_manager.get_total_value(),
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
    await bot.run()


if __name__ == "__main__":
    asyncio.run(main())
