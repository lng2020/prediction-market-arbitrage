"""Polymarket API client with REST and WebSocket support."""

import asyncio
import json
from datetime import datetime
from typing import Any, Callable, Optional

import websockets
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import MarketOrderArgs, OpenOrderParams, OrderArgs, OrderType as PMOrderType

from ..config import PolymarketConfig
from ..models import Order, OrderStatus, OrderType, Platform, Quote, Side


class PolymarketClient:
    """Async client for Polymarket CLOB API."""

    def __init__(self, config: PolymarketConfig):
        self.config = config
        self._client: Optional[ClobClient] = None
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._api_creds: Optional[dict] = None
        self._quote_callbacks: list[Callable[[Quote], None]] = []

    async def initialize(self) -> None:
        """Initialize the client and authenticate."""
        # py-clob-client is synchronous, wrap in executor for async compatibility
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._init_client)

    def _init_client(self) -> None:
        """Synchronous client initialization."""
        self._client = ClobClient(
            host=self.config.base_url,
            key=self.config.private_key,
            chain_id=self.config.chain_id,
            signature_type=self.config.signature_type,
            funder=self.config.funder_address,
        )
        # Derive API credentials for authenticated endpoints
        self._api_creds = self._client.create_or_derive_api_creds()
        self._client.set_api_creds(self._api_creds)

    async def close(self) -> None:
        """Close all connections."""
        if self._ws:
            await self._ws.close()

    # Market Data Methods

    async def get_markets(self) -> list[dict]:
        """Get list of available markets."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._client.get_simplified_markets)

    async def get_market(self, condition_id: str) -> dict:
        """Get single market details."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._client.get_market, condition_id)

    async def get_orderbook(self, token_id: str) -> dict:
        """Get orderbook for a token."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._client.get_order_book, token_id)

    async def get_midpoint(self, token_id: str) -> float:
        """Get midpoint price for a token."""
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(None, self._client.get_midpoint, token_id)
        return float(result) if result else 0.0

    async def get_price(self, token_id: str, side: str = "BUY") -> float:
        """Get best price for a token and side."""
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None, lambda: self._client.get_price(token_id, side)
        )
        return float(result) if result else 0.0

    async def get_quote(self, token_id: str) -> Quote:
        """Get current quote for a token."""
        loop = asyncio.get_event_loop()
        orderbook = await loop.run_in_executor(
            None, self._client.get_order_book, token_id
        )

        # Parse orderbook - bids and asks are lists of [price, size]
        bids = orderbook.get("bids", [])
        asks = orderbook.get("asks", [])

        best_bid = float(bids[0]["price"]) if bids else 0.0
        best_bid_size = float(bids[0]["size"]) if bids else 0.0
        best_ask = float(asks[0]["price"]) if asks else 1.0
        best_ask_size = float(asks[0]["size"]) if asks else 0.0

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
        loop = asyncio.get_event_loop()

        order_args = OrderArgs(
            token_id=token_id,
            price=price,
            size=size,
            side="BUY" if side == Side.BUY else "SELL",
        )

        signed_order = await loop.run_in_executor(
            None, self._client.create_order, order_args
        )

        response = await loop.run_in_executor(
            None, lambda: self._client.post_order(signed_order, PMOrderType.GTC)
        )

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
        loop = asyncio.get_event_loop()

        order_args = MarketOrderArgs(
            token_id=token_id,
            amount=amount,
            side="BUY" if side == Side.BUY else "SELL",
        )

        signed_order = await loop.run_in_executor(
            None, self._client.create_market_order, order_args
        )

        response = await loop.run_in_executor(
            None, lambda: self._client.post_order(signed_order, PMOrderType.FOK)
        )

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
        loop = asyncio.get_event_loop()
        try:
            await loop.run_in_executor(None, self._client.cancel, order_id)
            return True
        except Exception:
            return False

    async def cancel_all_orders(self) -> bool:
        """Cancel all open orders."""
        loop = asyncio.get_event_loop()
        try:
            await loop.run_in_executor(None, self._client.cancel_all)
            return True
        except Exception:
            return False

    async def get_orders(self) -> list[dict]:
        """Get all open orders."""
        loop = asyncio.get_event_loop()
        params = OpenOrderParams()
        return await loop.run_in_executor(None, self._client.get_orders, params)

    async def get_trades(self) -> list[dict]:
        """Get trade history."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._client.get_trades)

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

            # Handle different message types
            if "asset_id" in data:
                # Quote update
                quote = Quote(
                    platform=Platform.POLYMARKET,
                    contract_id=data["asset_id"],
                    bid=float(data.get("best_bid", 0)),
                    ask=float(data.get("best_ask", 1)),
                    bid_size=float(data.get("best_bid_size", 0)),
                    ask_size=float(data.get("best_ask_size", 0)),
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
