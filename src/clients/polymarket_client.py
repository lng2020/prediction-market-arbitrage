"""Polymarket API client with REST and WebSocket support."""

import json
from datetime import datetime
from typing import Callable, Optional

import aiohttp
import websockets
from common.async_poly_client import AsyncPolyClient
from py_clob_client.clob_types import MarketOrderArgs, OpenOrderParams, OrderArgs, OrderType as PMOrderType

from ..config import PolymarketConfig
from ..models import Order, OrderStatus, OrderType, Platform, Quote, Side

# Gamma API for market discovery
GAMMA_API_URL = "https://gamma-api.polymarket.com"


class PolymarketClient:
    """Async client for Polymarket CLOB API."""

    def __init__(self, config: PolymarketConfig):
        self.config = config
        self._client: Optional[AsyncPolyClient] = None
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._api_creds: Optional[dict] = None
        self._quote_callbacks: list[Callable[[Quote], None]] = []

    async def initialize(self) -> None:
        """Initialize the client and authenticate."""
        self._client = AsyncPolyClient(
            host=self.config.base_url,
            key=self.config.private_key,
            chain_id=self.config.chain_id,
            signature_type=self.config.signature_type,
            funder=self.config.funder_address,
        )
        # Derive API credentials for authenticated endpoints
        self._api_creds = await self._client.create_or_derive_api_creds()
        self._client.set_api_creds(self._api_creds)

    async def close(self) -> None:
        """Close all connections."""
        if self._client:
            await self._client.close()
        if self._ws:
            await self._ws.close()

    # Market Data Methods (Gamma API)

    async def get_markets(
        self,
        tag: Optional[str] = None,
        tag_id: Optional[int] = None,
        closed: bool = False,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict]:
        """
        Get list of markets from Gamma API.

        Args:
            tag: Filter by tag slug (e.g., "nba", "sports")
            tag_id: Filter by tag ID
            closed: Include closed markets
            limit: Max results per page (max 100)
            offset: Pagination offset
        """
        params = {
            "closed": str(closed).lower(),
            "limit": limit,
            "offset": offset,
        }
        if tag:
            params["tag"] = tag
        if tag_id:
            params["tag_id"] = tag_id

        async with aiohttp.ClientSession() as session:
            async with session.get(f"{GAMMA_API_URL}/markets", params=params) as resp:
                if resp.status == 200:
                    return await resp.json()
                return []

    async def get_events(
        self,
        tag: Optional[str] = None,
        closed: bool = False,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict]:
        """
        Get list of events from Gamma API.

        Events contain multiple related markets.
        """
        params = {
            "closed": str(closed).lower(),
            "limit": limit,
            "offset": offset,
            "order": "id",
            "ascending": "false",
        }
        if tag:
            params["tag"] = tag

        async with aiohttp.ClientSession() as session:
            async with session.get(f"{GAMMA_API_URL}/events", params=params) as resp:
                if resp.status == 200:
                    return await resp.json()
                return []

    async def get_event_by_slug(self, slug: str) -> Optional[dict]:
        """Get event by slug (e.g., 'nba-orl-det-2025-11-28')."""
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{GAMMA_API_URL}/events", params={"slug": slug}) as resp:
                if resp.status == 200:
                    events = await resp.json()
                    return events[0] if events else None
                return None

    async def get_sports_tags(self) -> list[dict]:
        """Get all sports tags and metadata."""
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{GAMMA_API_URL}/sports") as resp:
                if resp.status == 200:
                    return await resp.json()
                return []

    async def search_nba_games(self, date: Optional[str] = None) -> list[dict]:
        """
        Search for NBA game markets using fast event_date filter.

        Args:
            date: Optional date filter (YYYY-MM-DD format)

        Returns:
            List of NBA game events with their markets
        """
        events = []

        if not date:
            # If no date specified, use current date
            date = datetime.now().strftime("%Y-%m-%d")

        # Use event_date filter for fast querying
        async with aiohttp.ClientSession() as session:
            # Fetch all events for this date - they're paginated
            for offset in range(0, 500, 100):  # Cap at 500 events
                params = {
                    "event_date": date,
                    "limit": 100,
                    "offset": offset,
                    "active": "true",
                    "closed": "false",
                }

                async with session.get(f"{GAMMA_API_URL}/events", params=params) as resp:
                    if resp.status != 200:
                        break
                    batch = await resp.json()
                    if not batch:
                        break

                    for event in batch:
                        slug = event.get("slug", "")
                        # NBA game slugs look like: nba-orl-det-2025-11-28
                        if slug.startswith("nba-") and date in slug:
                            events.append(event)

                    if len(batch) < 100:
                        break

        return events

    async def get_nba_game_by_teams(
        self,
        away_team: str,
        home_team: str,
        date: str
    ) -> Optional[dict]:
        """
        Get NBA game event by team abbreviations and date.

        This is the fastest method - constructs slug directly.

        Args:
            away_team: Away team abbreviation (e.g., 'orl', 'chi')
            home_team: Home team abbreviation (e.g., 'det', 'bos')
            date: Game date in YYYY-MM-DD format

        Returns:
            Event dict or None if not found
        """
        # Construct the slug directly (much faster than searching)
        slug = f"nba-{away_team.lower()}-{home_team.lower()}-{date}"
        return await self.get_event_by_slug(slug)

    async def get_market_by_condition(self, condition_id: str) -> Optional[dict]:
        """Get market by condition ID from Gamma API."""
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{GAMMA_API_URL}/markets",
                params={"condition_id": condition_id}
            ) as resp:
                if resp.status == 200:
                    markets = await resp.json()
                    return markets[0] if markets else None
                return None

    async def get_market(self, condition_id: str) -> dict:
        """Get single market details."""
        return await self._client.get_market(condition_id)

    async def get_orderbook(self, token_id: str) -> dict:
        """Get orderbook for a token."""
        return await self._client.get_order_book(token_id)

    async def get_midpoint(self, token_id: str) -> float:
        """Get midpoint price for a token."""
        result = await self._client.get_midpoint(token_id)
        return float(result.get("mid", 0)) if result else 0.0

    async def get_price(self, token_id: str, side: str = "BUY") -> float:
        """Get best price for a token and side."""
        result = await self._client.get_price(token_id, side)
        return float(result.get("price", 0)) if result else 0.0

    async def get_quote(self, token_id: str) -> Quote:
        """Get current quote for a token."""
        orderbook = await self._client.get_order_book(token_id)

        # py-clob-client returns OrderBookSummary object, handle both dict and object
        if hasattr(orderbook, "bids"):
            bids = orderbook.bids or []
            asks = orderbook.asks or []
        else:
            bids = orderbook.get("bids", [])
            asks = orderbook.get("asks", [])

        # Parse bids/asks - can be list of dicts or objects
        def get_price_size(item):
            if hasattr(item, "price"):
                return float(item.price), float(item.size)
            return float(item.get("price", 0)), float(item.get("size", 0))

        # Polymarket returns bids ascending, asks descending - best prices are at the end
        best_bid, best_bid_size = get_price_size(bids[-1]) if bids else (0.0, 0.0)
        best_ask, best_ask_size = get_price_size(asks[-1]) if asks else (1.0, 0.0)

        return Quote(
            platform=Platform.POLYMARKET,
            contract_id=token_id,
            bid=best_bid,
            ask=best_ask,
            bid_size=best_bid_size,
            ask_size=best_ask_size,
        )

    # Order Methods

    async def create_limit_order(
        self,
        token_id: str,
        side: Side,
        price: float,
        size: float,
    ) -> Order:
        """Create a limit order (Maker)."""
        order_args = OrderArgs(
            token_id=token_id,
            price=price,
            size=size,
            side="BUY" if side == Side.BUY else "SELL",
        )

        signed_order = await self._client.create_order(order_args)
        response = await self._client.post_order(signed_order, PMOrderType.GTC)

        return Order(
            platform=Platform.POLYMARKET,
            contract_id=token_id,
            side=side,
            order_type=OrderType.LIMIT,
            price=price,
            quantity=size,
            order_id=response.get("orderID"),
            status=self._map_order_status(response.get("status", "")),
        )

    async def create_market_order(
        self,
        token_id: str,
        side: Side,
        amount: float,  # Dollar amount
    ) -> Order:
        """Create a market order (Taker) with FOK."""
        order_args = MarketOrderArgs(
            token_id=token_id,
            amount=amount,
            side="BUY" if side == Side.BUY else "SELL",
        )

        signed_order = await self._client.create_market_order(order_args)
        response = await self._client.post_order(signed_order, PMOrderType.FOK)

        return Order(
            platform=Platform.POLYMARKET,
            contract_id=token_id,
            side=side,
            order_type=OrderType.MARKET,
            price=0.0,  # Market order
            quantity=amount,
            order_id=response.get("orderID"),
            status=self._map_order_status(response.get("status", "")),
        )

    async def cancel_order(self, order_id: str) -> bool:
        """Cancel an order."""
        try:
            await self._client.cancel(order_id)
            return True
        except Exception:
            return False

    async def cancel_all_orders(self) -> bool:
        """Cancel all open orders."""
        try:
            await self._client.cancel_all()
            return True
        except Exception:
            return False

    async def get_orders(self) -> list[dict]:
        """Get all open orders."""
        params = OpenOrderParams()
        return await self._client.get_orders(params)

    async def get_trades(self) -> list[dict]:
        """Get trade history."""
        return await self._client.get_trades()

    def _map_order_status(self, status: str) -> OrderStatus:
        """Map Polymarket order status to internal status."""
        mapping = {
            "live": OrderStatus.OPEN,
            "matched": OrderStatus.FILLED,
            "cancelled": OrderStatus.CANCELLED,
            "delayed": OrderStatus.PENDING,
        }
        return mapping.get(status.lower(), OrderStatus.PENDING)

    # WebSocket Methods

    async def connect_websocket(self, channel: str = "market") -> None:
        """
        Establish WebSocket connection.

        Args:
            channel: "market" for orderbook data, "user" for account updates
        """
        ws_url = f"{self.config.ws_url}/{channel}"
        self._ws = await websockets.connect(ws_url)

    async def subscribe_market(self, asset_ids: list[str]) -> None:
        """Subscribe to market data for given asset IDs."""
        message = {"assets_ids": asset_ids, "type": "market"}
        await self._ws.send(json.dumps(message))

    async def subscribe_user(self, market_ids: list[str]) -> None:
        """Subscribe to user data for given markets (requires auth)."""
        if not self._api_creds:
            raise ValueError("API credentials required for user channel")

        message = {
            "markets": market_ids,
            "type": "user",
            "auth": {
                "apiKey": self._api_creds.get("apiKey"),
                "secret": self._api_creds.get("secret"),
                "passphrase": self._api_creds.get("passphrase"),
            },
        }
        await self._ws.send(json.dumps(message))

    def on_quote_update(self, callback: Callable[[Quote], None]) -> None:
        """Register callback for quote updates."""
        self._quote_callbacks.append(callback)

    async def listen_websocket(self) -> None:
        """Listen for WebSocket messages."""
        async for message in self._ws:
            data = json.loads(message)

            # Handle price_changes messages (incremental updates with best_bid/best_ask)
            if "price_changes" in data:
                for change in data["price_changes"]:
                    if "asset_id" in change and "best_bid" in change and "best_ask" in change:
                        quote = Quote(
                            platform=Platform.POLYMARKET,
                            contract_id=change["asset_id"],
                            bid=float(change["best_bid"]),
                            ask=float(change["best_ask"]),
                            bid_size=0.0,  # Not provided in price_changes
                            ask_size=0.0,
                        )
                        for callback in self._quote_callbacks:
                            callback(quote)
            # Handle initial snapshot messages (full orderbook with bids/asks arrays)
            elif "asset_id" in data and "bids" in data and "asks" in data:
                bids = data.get("bids", [])
                asks = data.get("asks", [])
                # Polymarket returns bids ascending, asks descending - best prices at end
                best_bid = float(bids[-1]["price"]) if bids else 0.0
                best_ask = float(asks[-1]["price"]) if asks else 1.0
                best_bid_size = float(bids[-1]["size"]) if bids else 0.0
                best_ask_size = float(asks[-1]["size"]) if asks else 0.0
                quote = Quote(
                    platform=Platform.POLYMARKET,
                    contract_id=data["asset_id"],
                    bid=best_bid,
                    ask=best_ask,
                    bid_size=best_bid_size,
                    ask_size=best_ask_size,
                )
                for callback in self._quote_callbacks:
                    callback(quote)

    async def _send_ping(self) -> None:
        """Send ping to keep connection alive."""
        while self._ws and self._ws.open:
            await self._ws.ping()
            await asyncio.sleep(10)

    # Fee Calculation (currently 0 bps)

    @staticmethod
    def calculate_taker_fee(amount: float) -> float:
        """Calculate taker fee (currently 0 bps)."""
        return 0.0

    @staticmethod
    def calculate_maker_fee(amount: float) -> float:
        """Calculate maker fee (currently 0 bps)."""
        return 0.0
