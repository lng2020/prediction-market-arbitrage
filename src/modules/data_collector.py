"""
DataCollector Module (Module A)

Handles API connections and real-time quote collection from both platforms.
"""

import asyncio
import logging
from datetime import datetime
from typing import Callable, Optional

from ..clients import KalshiClient, PolymarketClient
from ..config import Config
from ..models import ContractPair, Platform, Quote

logger = logging.getLogger(__name__)


class DataCollector:
    """
    Collects real-time quotes from Polymarket and Kalshi via WebSocket.

    Responsibilities:
    - A.1: API connection management
    - A.2: Real-time quote fetching via WebSocket
    - A.3: Event/contract mapping maintenance
    """

    def __init__(
        self,
        config: Config,
        kalshi_client: KalshiClient,
        polymarket_client: PolymarketClient,
    ):
        self.config = config
        self.kalshi = kalshi_client
        self.polymarket = polymarket_client

        # Contract mappings
        self._contract_pairs: list[ContractPair] = []

        # Latest quotes cache
        self._quotes: dict[str, Quote] = {}  # key: "{platform}:{contract_id}"

        # Callbacks for quote updates
        self._quote_callbacks: list[Callable[[Quote], None]] = []

        # WebSocket tasks
        self._ws_tasks: list[asyncio.Task] = []
        self._running = False

    def add_contract_pair(self, pair: ContractPair) -> None:
        """Add a contract pair to monitor."""
        self._contract_pairs.append(pair)
        logger.info(f"Added contract pair: {pair.event_name} - {pair.outcome}")

    def remove_contract_pair(self, event_name: str) -> None:
        """Remove a contract pair from monitoring."""
        self._contract_pairs = [
            p for p in self._contract_pairs if p.event_name != event_name
        ]

    def get_contract_pairs(self) -> list[ContractPair]:
        """Get all active contract pairs."""
        return [p for p in self._contract_pairs if p.active]

    def on_quote_update(self, callback: Callable[[Quote], None]) -> None:
        """Register callback for quote updates."""
        self._quote_callbacks.append(callback)

    def _cache_key(self, platform: Platform, contract_id: str) -> str:
        """Generate cache key for a quote."""
        return f"{platform.value}:{contract_id}"

    def get_cached_quote(self, platform: Platform, contract_id: str) -> Optional[Quote]:
        """Get cached quote for a contract."""
        key = self._cache_key(platform, contract_id)
        return self._quotes.get(key)

    def get_pair_quotes(
        self, pair: ContractPair
    ) -> tuple[Optional[Quote], Optional[Quote]]:
        """Get cached quotes for a contract pair (PM, KL)."""
        pm_quote = self.get_cached_quote(Platform.POLYMARKET, pair.polymarket_token_id)
        kl_quote = self.get_cached_quote(Platform.KALSHI, pair.kalshi_ticker)
        return pm_quote, kl_quote

    async def _handle_quote_update(self, quote: Quote) -> None:
        """Handle incoming quote update."""
        key = self._cache_key(quote.platform, quote.contract_id)
        old_quote = self._quotes.get(key)

        # Preserve sizes from cached quote if new quote has size=0
        # (WebSocket price_changes messages don't include size)
        if old_quote and quote.bid_size == 0 and quote.ask_size == 0:
            quote = Quote(
                platform=quote.platform,
                contract_id=quote.contract_id,
                bid=quote.bid,
                ask=quote.ask,
                bid_size=old_quote.bid_size,
                ask_size=old_quote.ask_size,
            )

        self._quotes[key] = quote

        # Log quote update in verbose mode (only if price changed)
        if old_quote is None or old_quote.bid != quote.bid or old_quote.ask != quote.ask:
            # Find the pair name for this quote
            pair_name = quote.contract_id[:20]  # Default to truncated contract_id
            for pair in self._contract_pairs:
                if quote.platform == Platform.POLYMARKET and pair.polymarket_token_id == quote.contract_id:
                    pair_name = f"{pair.event_name} - {pair.outcome}"
                    break
                elif quote.platform == Platform.KALSHI and pair.kalshi_ticker == quote.contract_id:
                    pair_name = f"{pair.event_name} - {pair.outcome}"
                    break

            logger.debug(
                f"[Quote] {quote.platform.value} | {pair_name} | "
                f"bid={quote.bid:.3f} ask={quote.ask:.3f}"
            )

        # Notify callbacks
        for callback in self._quote_callbacks:
            try:
                callback(quote)
            except Exception as e:
                logger.error(f"Error in quote callback: {e}")

    async def start_websocket_streams(self) -> None:
        """Start WebSocket connections for real-time quotes."""
        self._running = True

        # Get all contract IDs to subscribe
        pm_tokens = [p.polymarket_token_id for p in self.get_contract_pairs()]
        kl_tickers = [p.kalshi_ticker for p in self.get_contract_pairs()]

        # Connect to Polymarket WebSocket
        if pm_tokens:
            try:
                await self.polymarket.connect_websocket("market")
                await self.polymarket.subscribe_market(pm_tokens)
                self.polymarket.on_quote_update(
                    lambda q: asyncio.create_task(self._handle_quote_update(q))
                )
                task = asyncio.create_task(self._run_pm_websocket())
                self._ws_tasks.append(task)
                logger.info(f"Started Polymarket WebSocket for {len(pm_tokens)} tokens")
            except Exception as e:
                logger.error(f"Failed to start Polymarket WebSocket: {e}")

        # Connect to Kalshi WebSocket
        if kl_tickers:
            try:
                await self.kalshi.connect_websocket()
                await self.kalshi.subscribe_orderbook(kl_tickers)
                self.kalshi.on_quote_update(
                    lambda q: asyncio.create_task(self._handle_quote_update(q))
                )
                task = asyncio.create_task(self._run_kl_websocket())
                self._ws_tasks.append(task)
                logger.info(f"Started Kalshi WebSocket for {len(kl_tickers)} tickers")
            except Exception as e:
                logger.error(f"Failed to start Kalshi WebSocket: {e}")

        # Fetch initial quotes for all pairs (WebSocket only updates on changes)
        await self._fetch_initial_quotes()

    async def _fetch_initial_quotes(self) -> None:
        """Fetch initial quotes for all pairs via REST API (parallelized)."""
        logger.info("Fetching initial quotes for all pairs...")
        pairs = self.get_contract_pairs()

        if not pairs:
            return

        async def fetch_pair_quotes(pair: ContractPair) -> tuple[ContractPair, Optional[Quote], Optional[Quote]]:
            """Fetch quotes for a single pair from both platforms."""
            pm_quote = None
            kl_quote = None
            try:
                pm_quote = await self.polymarket.get_quote(pair.polymarket_token_id)
            except Exception as e:
                logger.warning(f"Failed to fetch PM quote for {pair.event_name}: {e}")
            try:
                kl_quote = await self.kalshi.get_quote(pair.kalshi_ticker)
            except Exception as e:
                logger.warning(f"Failed to fetch KL quote for {pair.event_name}: {e}")
            return (pair, pm_quote, kl_quote)

        # Fetch all pairs in parallel
        results = await asyncio.gather(
            *[fetch_pair_quotes(pair) for pair in pairs],
            return_exceptions=True
        )

        success_count = 0
        for result in results:
            if isinstance(result, Exception):
                logger.warning(f"Failed to fetch initial quote: {result}")
                continue
            pair, pm_quote, kl_quote = result
            if pm_quote:
                await self._handle_quote_update(pm_quote)
            if kl_quote:
                await self._handle_quote_update(kl_quote)
            if pm_quote and kl_quote:
                success_count += 1

        logger.info(f"Fetched initial quotes for {success_count}/{len(pairs)} pairs")

    async def _run_pm_websocket(self) -> None:
        """Run Polymarket WebSocket listener."""
        while self._running:
            try:
                await self.polymarket.listen_websocket()
            except Exception as e:
                logger.error(f"Polymarket WebSocket error: {e}")
                if self._running:
                    await asyncio.sleep(5)
                    await self.polymarket.connect_websocket("market")
                    pm_tokens = [p.polymarket_token_id for p in self.get_contract_pairs()]
                    await self.polymarket.subscribe_market(pm_tokens)

    async def _run_kl_websocket(self) -> None:
        """Run Kalshi WebSocket listener."""
        while self._running:
            try:
                await self.kalshi.listen_websocket()
            except Exception as e:
                logger.error(f"Kalshi WebSocket error: {e}")
                if self._running:
                    await asyncio.sleep(5)
                    await self.kalshi.connect_websocket()
                    kl_tickers = [p.kalshi_ticker for p in self.get_contract_pairs()]
                    await self.kalshi.subscribe_orderbook(kl_tickers)

    async def stop(self) -> None:
        """Stop all data collection."""
        self._running = False

        # Cancel WebSocket tasks
        for task in self._ws_tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        self._ws_tasks.clear()
        logger.info("DataCollector stopped")
