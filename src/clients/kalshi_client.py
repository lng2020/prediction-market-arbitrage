"""Kalshi API client with REST and WebSocket support."""

import asyncio
import base64
import json
import time
from collections import deque
from datetime import datetime
from typing import Any, Callable, Optional

import aiohttp
import websockets
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

from ..config import KalshiConfig
from ..models import Order, OrderStatus, OrderType, Platform, Quote, Side


class RateLimiter:
    """Centralized rate limiter for API requests."""

    def __init__(self, max_requests: int = 15, window_seconds: float = 1.0):
        """
        Initialize rate limiter.

        Args:
            max_requests: Maximum requests allowed per window (default 15, leaving buffer below 20)
            window_seconds: Time window in seconds
        """
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self._request_times: deque = deque()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        """Wait until a request slot is available."""
        async with self._lock:
            now = time.time()

            # Remove requests outside the window
            while self._request_times and now - self._request_times[0] >= self.window_seconds:
                self._request_times.popleft()

            # If at capacity, wait until oldest request expires
            if len(self._request_times) >= self.max_requests:
                wait_time = self.window_seconds - (now - self._request_times[0])
                if wait_time > 0:
                    await asyncio.sleep(wait_time)
                    # Re-check after sleeping
                    now = time.time()
                    while self._request_times and now - self._request_times[0] >= self.window_seconds:
                        self._request_times.popleft()

            # Record this request
            self._request_times.append(time.time())


class KalshiClient:
    """Async client for Kalshi API."""

    def __init__(self, config: KalshiConfig):
        self.config = config
        self._private_key = None
        self._session: Optional[aiohttp.ClientSession] = None
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._ws_message_id = 0
        self._quote_callbacks: list[Callable[[Quote], None]] = []
        # Centralized rate limiter: 15 req/sec (buffer below 20 read limit)
        self._rate_limiter = RateLimiter(max_requests=15, window_seconds=1.0)

    async def initialize(self) -> None:
        """Initialize the client and load credentials."""
        key_pem = self.config.load_private_key()
        self._private_key = serialization.load_pem_private_key(
            key_pem.encode(), password=None
        )
        self._session = aiohttp.ClientSession()

    async def close(self) -> None:
        """Close all connections."""
        if self._ws:
            await self._ws.close()
        if self._session:
            await self._session.close()

    def _generate_signature(self, timestamp_ms: int, method: str, path: str) -> str:
        """Generate RSA-PSS signature for request authentication."""
        # Strip query parameters from path for signing
        path_without_query = path.split("?")[0]
        message = f"{timestamp_ms}{method}{path_without_query}"
        signature = self._private_key.sign(
            message.encode(),
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.DIGEST_LENGTH,  # Must be DIGEST_LENGTH, not MAX_LENGTH
            ),
            hashes.SHA256(),
        )
        return base64.b64encode(signature).decode()

    def _get_auth_headers(self, method: str, path: str) -> dict[str, str]:
        """Generate authentication headers for a request."""
        timestamp_ms = int(time.time() * 1000)
        signature = self._generate_signature(timestamp_ms, method, path)
        return {
            "KALSHI-ACCESS-KEY": self.config.api_key_id,
            "KALSHI-ACCESS-TIMESTAMP": str(timestamp_ms),
            "KALSHI-ACCESS-SIGNATURE": signature,
            "Content-Type": "application/json",
        }

    async def _request(
        self,
        method: str,
        path: str,
        params: Optional[dict] = None,
        data: Optional[dict] = None,
        auth_required: bool = True,
    ) -> dict[str, Any]:
        """Make a REST API request."""
        # Apply rate limiting
        await self._rate_limiter.acquire()

        url = f"{self.config.base_url}{path}"

        if auth_required and self._private_key:
            headers = self._get_auth_headers(method, path)
        else:
            headers = {"Content-Type": "application/json"}

        async with self._session.request(
            method, url, headers=headers, params=params, json=data
        ) as response:
            response.raise_for_status()
            return await response.json()

    # Market Data Endpoints

    async def get_markets(
        self,
        series_ticker: Optional[str] = None,
        status: str = "open",
        limit: int = 100,
    ) -> list[dict]:
        """Get list of markets."""
        params = {"status": status, "limit": limit}
        if series_ticker:
            params["series_ticker"] = series_ticker
        result = await self._request("GET", "/markets", params=params)
        return result.get("markets", [])

    async def get_market(self, ticker: str) -> dict:
        """Get single market details."""
        result = await self._request("GET", f"/markets/{ticker}")
        return result.get("market", {})

    async def get_orderbook(self, ticker: str) -> dict:
        """Get orderbook for a market."""
        result = await self._request("GET", f"/markets/{ticker}/orderbook")
        return result.get("orderbook", {})

    async def get_quote(self, ticker: str) -> Quote:
        """Get current quote for a market."""
        orderbook = await self.get_orderbook(ticker)

        # Kalshi returns prices in cents, convert to 0-1
        yes_bids = orderbook.get("yes", [])
        no_bids = orderbook.get("no", [])

        # Best bid for YES = highest yes bid
        # Best ask for YES = 100 - highest no bid (reciprocal relationship)
        best_yes_bid = yes_bids[0][0] / 100 if yes_bids else 0.0
        best_yes_bid_size = yes_bids[0][1] if yes_bids else 0.0

        best_no_bid = no_bids[0][0] / 100 if no_bids else 0.0
        best_yes_ask = 1 - best_no_bid if no_bids else 1.0
        best_yes_ask_size = no_bids[0][1] if no_bids else 0.0

        return Quote(
            platform=Platform.KALSHI,
            contract_id=ticker,
            bid=best_yes_bid,
            ask=best_yes_ask,
            bid_size=best_yes_bid_size,
            ask_size=best_yes_ask_size,
        )

    # Portfolio Endpoints

    async def get_balance(self) -> dict:
        """Get account balance."""
        result = await self._request("GET", "/portfolio/balance")
        return result

    async def get_positions(self) -> list[dict]:
        """Get current positions."""
        result = await self._request("GET", "/portfolio/positions")
        return result.get("market_positions", [])

    # Order Endpoints

    async def create_order(
        self,
        ticker: str,
        side: Side,
        action: str,  # "buy" or "sell"
        count: int,
        price_cents: int,
        order_type: OrderType = OrderType.LIMIT,
        client_order_id: Optional[str] = None,
    ) -> Order:
        """Create a new order."""
        data = {
            "ticker": ticker,
            "side": "yes" if side == Side.BUY else "no",
            "action": action,
            "count": count,
            "type": order_type.value,
        }

        if order_type == OrderType.LIMIT:
            data["yes_price"] = price_cents

        if client_order_id:
            data["client_order_id"] = client_order_id

        result = await self._request("POST", "/portfolio/orders", data=data)
        order_data = result.get("order", {})

        return Order(
            platform=Platform.KALSHI,
            contract_id=ticker,
            side=side,
            order_type=order_type,
            price=price_cents / 100,
            quantity=count,
            order_id=order_data.get("order_id"),
            client_order_id=client_order_id,
            status=self._map_order_status(order_data.get("status", "")),
            filled_quantity=order_data.get("fill_count", 0),
        )

    async def cancel_order(self, order_id: str) -> bool:
        """Cancel an order."""
        try:
            await self._request("DELETE", f"/portfolio/orders/{order_id}")
            return True
        except Exception:
            return False

    async def get_order(self, order_id: str) -> dict:
        """Get order status."""
        result = await self._request("GET", f"/portfolio/orders/{order_id}")
        return result.get("order", {})

    def _map_order_status(self, status: str) -> OrderStatus:
        """Map Kalshi order status to internal status."""
        mapping = {
            "resting": OrderStatus.OPEN,
            "canceled": OrderStatus.CANCELLED,
            "executed": OrderStatus.FILLED,
            "pending": OrderStatus.PENDING,
        }
        return mapping.get(status, OrderStatus.PENDING)

    # WebSocket Methods

    async def connect_websocket(self) -> None:
        """Establish WebSocket connection."""
        timestamp_ms = int(time.time() * 1000)
        signature = self._generate_signature(timestamp_ms, "GET", "/trade-api/ws/v2")

        headers = {
            "KALSHI-ACCESS-KEY": self.config.api_key_id,
            "KALSHI-ACCESS-TIMESTAMP": str(timestamp_ms),
            "KALSHI-ACCESS-SIGNATURE": signature,
        }

        # websockets 11.0+ uses 'additional_headers', older versions use 'extra_headers'
        try:
            self._ws = await websockets.connect(
                self.config.ws_url, additional_headers=headers
            )
        except TypeError:
            # Fallback for older websockets versions
            self._ws = await websockets.connect(
                self.config.ws_url, extra_headers=headers
            )

    async def subscribe_orderbook(self, tickers: list[str]) -> None:
        """Subscribe to orderbook updates for given tickers."""
        self._ws_message_id += 1
        message = {
            "id": self._ws_message_id,
            "cmd": "subscribe",
            "params": {"channels": ["orderbook_delta"], "market_tickers": tickers},
        }
        await self._ws.send(json.dumps(message))

    async def subscribe_trades(self, tickers: list[str]) -> None:
        """Subscribe to trade updates."""
        self._ws_message_id += 1
        message = {
            "id": self._ws_message_id,
            "cmd": "subscribe",
            "params": {"channels": ["trades"], "market_tickers": tickers},
        }
        await self._ws.send(json.dumps(message))

    def on_quote_update(self, callback: Callable[[Quote], None]) -> None:
        """Register callback for quote updates."""
        self._quote_callbacks.append(callback)

    async def listen_websocket(self) -> None:
        """Listen for WebSocket messages."""
        last_fetch_time: dict[str, float] = {}
        min_interval_per_ticker = 1.0  # Min 1 second between fetches per ticker

        async for message in self._ws:
            data = json.loads(message)
            msg_type = data.get("type")

            if msg_type == "orderbook_delta":
                # Process orderbook update and notify callbacks
                ticker = data.get("msg", {}).get("market_ticker")
                if ticker:
                    current_time = time.time()

                    # Per-ticker rate limit to avoid flooding queue
                    if ticker in last_fetch_time:
                        if current_time - last_fetch_time[ticker] < min_interval_per_ticker:
                            continue  # Skip this update

                    last_fetch_time[ticker] = current_time

                    try:
                        # Centralized rate limiter handles global throttling
                        quote = await self.get_quote(ticker)
                        for callback in self._quote_callbacks:
                            callback(quote)
                    except Exception as e:
                        # Log but continue on error
                        pass

    # Fee Calculation

    @staticmethod
    def calculate_taker_fee(count: int, price: float) -> float:
        """Calculate taker fee using Kalshi formula: 0.07 × C × P × (1-P)."""
        return 0.07 * count * price * (1 - price)

    @staticmethod
    def calculate_maker_fee(count: int, price: float, has_maker_fee: bool = False) -> float:
        """Calculate maker fee (only applies to certain markets)."""
        if not has_maker_fee:
            return 0.0
        return 0.0175 * count * price * (1 - price)
