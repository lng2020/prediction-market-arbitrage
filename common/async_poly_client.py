# -*- coding = utf-8 -*-
"""Async Polymarket CLOB client using aiohttp."""

import asyncio
import logging
from typing import Optional

import aiohttp

from py_clob_client.clob_types import (
    ApiCreds,
    BookParams,
    CreateOrderOptions,
    MarketOrderArgs,
    OpenOrderParams,
    OrderArgs,
    OrderBookSummary,
    OrderType,
    PartialCreateOrderOptions,
    RequestArgs,
    TickSize,
    TradeParams,
)
from py_clob_client.config import get_contract_config
from py_clob_client.constants import END_CURSOR, L0, L1, L1_AUTH_UNAVAILABLE, L2, L2_AUTH_UNAVAILABLE
from py_clob_client.endpoints import (
    CANCEL,
    CANCEL_ALL,
    CANCEL_ORDERS,
    CREATE_API_KEY,
    DERIVE_API_KEY,
    GET_FEE_RATE,
    GET_MARKET,
    GET_NEG_RISK,
    GET_ORDER,
    GET_ORDER_BOOK,
    GET_ORDER_BOOKS,
    GET_TICK_SIZE,
    MID_POINT,
    MID_POINTS,
    ORDERS,
    POST_ORDER,
    PRICE,
    TRADES,
)
from py_clob_client.exceptions import PolyApiException, PolyException
from py_clob_client.headers.headers import create_level_1_headers, create_level_2_headers
from py_clob_client.http_helpers.helpers import add_query_open_orders_params, add_query_trade_params
from py_clob_client.order_builder.builder import OrderBuilder, ROUNDING_CONFIG
from py_clob_client.order_builder.helpers import to_token_decimals, round_down, round_normal, decimal_places, round_up
from py_clob_client.signer import Signer
from py_clob_client.utilities import is_tick_size_smaller, order_to_json, parse_raw_orderbook_summary, price_valid

# Fast signing imports
from py_order_utils.builders import OrderBuilder as UtilsOrderBuilder
from py_order_utils.signer import Signer as UtilsSigner
from py_order_utils.model import OrderData, SignedOrder, BUY as UtilsBuy, SELL as UtilsSell, EOA


class AsyncPolyClient:
    """Async Polymarket CLOB client."""

    def __init__(
        self,
        host: str,
        chain_id: int = None,
        key: str = None,
        creds: ApiCreds = None,
        signature_type: int = None,
        funder: str = None,
    ):
        self.host = host[:-1] if host.endswith("/") else host
        self.chain_id = chain_id
        self.signer = Signer(key, chain_id) if key else None
        self.creds = creds
        self.mode = self._get_client_mode()

        if self.signer:
            self.builder = OrderBuilder(self.signer, sig_type=signature_type, funder=funder)
            # Pre-initialize fast signing components (cached for reuse)
            self._utils_signer = UtilsSigner(key=key)
            self._funder = funder if funder is not None else self.signer.address()
            self._sig_type = signature_type if signature_type is not None else EOA
            # Cache UtilsOrderBuilder per (neg_risk) - created lazily
            self._utils_builders: dict[bool, UtilsOrderBuilder] = {}

        # Local cache
        self._tick_sizes: dict[str, TickSize] = {}
        self._neg_risk: dict[str, bool] = {}
        self._fee_rates: dict[str, int] = {}

        self._session: Optional[aiohttp.ClientSession] = None
        self.logger = logging.getLogger(self.__class__.__name__)

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create the aiohttp session."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                headers={
                    "User-Agent": "py_clob_client",
                    "Accept": "*/*",
                    "Content-Type": "application/json",
                }
            )
        return self._session

    async def close(self) -> None:
        """Close the HTTP session."""
        if self._session and not self._session.closed:
            await self._session.close()

    async def _request(
        self,
        method: str,
        endpoint: str,
        headers: Optional[dict] = None,
        data: Optional[dict | list] = None,
    ) -> dict | str | list:
        """Make an async HTTP request."""
        session = await self._get_session()
        request_headers = dict(session.headers) if session.headers else {}
        if headers:
            request_headers.update(headers)
        if method == "GET":
            request_headers["Accept-Encoding"] = "gzip"

        async with session.request(method, endpoint, headers=request_headers, json=data) as resp:
            if resp.status != 200:
                text = await resp.text()
                raise PolyApiException(error_msg=f"HTTP {resp.status}: {text}")
            try:
                return await resp.json()
            except aiohttp.ContentTypeError:
                return await resp.text()

    async def _get(self, endpoint: str, headers: Optional[dict] = None) -> dict | str | list:
        return await self._request("GET", endpoint, headers)

    async def _post(self, endpoint: str, headers: Optional[dict] = None, data: Optional[dict | list] = None) -> dict | str | list:
        return await self._request("POST", endpoint, headers, data)

    async def _delete(self, endpoint: str, headers: Optional[dict] = None, data: Optional[dict | list] = None) -> dict | str | list:
        return await self._request("DELETE", endpoint, headers, data)

    # Auth methods
    def _get_client_mode(self) -> int:
        if self.signer is not None and self.creds is not None:
            return L2
        if self.signer is not None:
            return L1
        return L0

    def assert_level_1_auth(self):
        if self.mode < L1:
            raise PolyException(L1_AUTH_UNAVAILABLE)

    def assert_level_2_auth(self):
        if self.mode < L2:
            raise PolyException(L2_AUTH_UNAVAILABLE)

    def get_address(self) -> Optional[str]:
        return self.signer.address() if self.signer else None

    def get_collateral_address(self) -> Optional[str]:
        config = get_contract_config(self.chain_id)
        return config.collateral if config else None

    def get_exchange_address(self, neg_risk: bool = False) -> Optional[str]:
        config = get_contract_config(self.chain_id, neg_risk)
        return config.exchange if config else None

    # API Key methods
    async def create_api_key(self, nonce: int = None) -> Optional[ApiCreds]:
        self.assert_level_1_auth()
        endpoint = f"{self.host}{CREATE_API_KEY}"
        headers = create_level_1_headers(self.signer, nonce)
        creds_raw = await self._post(endpoint, headers=headers)
        try:
            return ApiCreds(
                api_key=creds_raw["apiKey"],
                api_secret=creds_raw["secret"],
                api_passphrase=creds_raw["passphrase"],
            )
        except Exception:
            self.logger.error("Couldn't parse created CLOB creds")
            return None

    async def derive_api_key(self, nonce: int = None) -> Optional[ApiCreds]:
        self.assert_level_1_auth()
        endpoint = f"{self.host}{DERIVE_API_KEY}"
        headers = create_level_1_headers(self.signer, nonce)
        creds_raw = await self._get(endpoint, headers=headers)
        try:
            return ApiCreds(
                api_key=creds_raw["apiKey"],
                api_secret=creds_raw["secret"],
                api_passphrase=creds_raw["passphrase"],
            )
        except Exception:
            self.logger.error("Couldn't parse derived CLOB creds")
            return None

    async def create_or_derive_api_creds(self, nonce: int = None) -> Optional[ApiCreds]:
        try:
            return await self.create_api_key(nonce)
        except Exception:
            return await self.derive_api_key(nonce)

    def set_api_creds(self, creds: ApiCreds) -> None:
        self.creds = creds
        self.mode = self._get_client_mode()

    # Market data methods
    async def get_midpoint(self, token_id: str) -> dict:
        return await self._get(f"{self.host}{MID_POINT}?token_id={token_id}")

    async def get_midpoints(self, params: list[BookParams]) -> list:
        body = [{"token_id": p.token_id} for p in params]
        return await self._post(f"{self.host}{MID_POINTS}", data=body)

    async def get_price(self, token_id: str, side: str) -> dict:
        return await self._get(f"{self.host}{PRICE}?token_id={token_id}&side={side}")

    async def get_order_book(self, token_id: str) -> OrderBookSummary:
        raw = await self._get(f"{self.host}{GET_ORDER_BOOK}?token_id={token_id}")
        return parse_raw_orderbook_summary(raw)

    async def get_order_books(self, params: list[BookParams]) -> list[OrderBookSummary]:
        body = [{"token_id": p.token_id} for p in params]
        raw_list = await self._post(f"{self.host}{GET_ORDER_BOOKS}", data=body)
        return [parse_raw_orderbook_summary(r) for r in raw_list]

    async def get_market(self, condition_id: str) -> dict:
        return await self._get(f"{self.host}{GET_MARKET}{condition_id}")

    # Tick size, neg risk, fee rate (with caching)
    async def get_tick_size(self, token_id: str) -> TickSize:
        if token_id in self._tick_sizes:
            return self._tick_sizes[token_id]
        result = await self._get(f"{self.host}{GET_TICK_SIZE}?token_id={token_id}")
        self._tick_sizes[token_id] = str(result["minimum_tick_size"])
        return self._tick_sizes[token_id]

    async def get_neg_risk(self, token_id: str) -> bool:
        if token_id in self._neg_risk:
            return self._neg_risk[token_id]
        result = await self._get(f"{self.host}{GET_NEG_RISK}?token_id={token_id}")
        self._neg_risk[token_id] = result["neg_risk"]
        return result["neg_risk"]

    async def get_fee_rate_bps(self, token_id: str) -> int:
        if token_id in self._fee_rates:
            return self._fee_rates[token_id]
        result = await self._get(f"{self.host}{GET_FEE_RATE}?token_id={token_id}")
        fee_rate = result.get("base_fee") or 0
        self._fee_rates[token_id] = fee_rate
        return fee_rate

    async def _resolve_tick_size(self, token_id: str, tick_size: Optional[TickSize] = None) -> TickSize:
        min_tick_size = await self.get_tick_size(token_id)
        if tick_size is not None:
            if is_tick_size_smaller(tick_size, min_tick_size):
                raise Exception(f"invalid tick size ({tick_size}), minimum for the market is {min_tick_size}")
        else:
            tick_size = min_tick_size
        return tick_size

    async def _resolve_fee_rate(self, token_id: str, user_fee_rate: Optional[int] = None) -> int:
        market_fee_rate = await self.get_fee_rate_bps(token_id)
        if (
            market_fee_rate is not None
            and market_fee_rate > 0
            and user_fee_rate is not None
            and user_fee_rate > 0
            and user_fee_rate != market_fee_rate
        ):
            raise Exception(f"invalid user provided fee rate: ({user_fee_rate}), fee rate for the market must be {market_fee_rate}")
        return market_fee_rate

    # Prefetch order signature fields in parallel
    async def prefetch_order_fields(
        self,
        token_id: str,
        tick_size: Optional[TickSize] = None,
        fee_rate_bps: Optional[float] = None,
        neg_risk: Optional[bool] = None,
    ) -> None:
        """Prefetch tick_size, fee_rate, neg_risk in parallel if not cached."""
        tasks = []
        if tick_size is None and token_id not in self._tick_sizes:
            tasks.append(self.get_tick_size(token_id))
        if fee_rate_bps is None and token_id not in self._fee_rates:
            tasks.append(self.get_fee_rate_bps(token_id))
        if neg_risk is None and token_id not in self._neg_risk:
            tasks.append(self.get_neg_risk(token_id))
        if tasks:
            await asyncio.gather(*tasks)

    # Order creation (signing is sync/CPU-bound, HTTP calls are async)
    async def create_order(
        self,
        order_args: OrderArgs,
        options: Optional[PartialCreateOrderOptions] = None,
        tick_size: Optional[TickSize] = None,
        fee_rate_bps: Optional[float] = None,
        neg_risk: Optional[bool] = None,
    ):
        """Create and sign an order."""
        self.assert_level_1_auth()

        # Prefetch in parallel
        await self.prefetch_order_fields(order_args.token_id, tick_size, fee_rate_bps, neg_risk)

        # Resolve tick size
        if tick_size is None:
            tick_size = await self._resolve_tick_size(
                order_args.token_id,
                options.tick_size if options else None,
            )

        if not price_valid(order_args.price, tick_size):
            raise Exception(
                f"price ({order_args.price}), min: {tick_size} - max: {1 - float(tick_size)}"
            )

        # Resolve neg_risk
        if neg_risk is None:
            neg_risk = (
                options.neg_risk
                if options and options.neg_risk
                else await self.get_neg_risk(order_args.token_id)
            )

        # Resolve fee rate
        if fee_rate_bps is None:
            fee_rate_bps = await self._resolve_fee_rate(
                order_args.token_id, options.fee_rate_bps if options else None
            )
        order_args.fee_rate_bps = fee_rate_bps

        # Signing is CPU-bound, run in executor if needed for strict async
        return self.builder.create_order(
            order_args,
            CreateOrderOptions(tick_size=tick_size, neg_risk=neg_risk),
        )

    def _get_utils_builder(self, neg_risk: bool) -> UtilsOrderBuilder:
        """Get or create cached UtilsOrderBuilder for fast signing."""
        if neg_risk not in self._utils_builders:
            contract_config = get_contract_config(self.chain_id, neg_risk)
            self._utils_builders[neg_risk] = UtilsOrderBuilder(
                contract_config.exchange,
                self.chain_id,
                self._utils_signer,
            )
        return self._utils_builders[neg_risk]

    def create_order_fast(
        self,
        order_args: OrderArgs,
        tick_size: TickSize,
        neg_risk: bool,
    ) -> SignedOrder:
        """
        Fast order signing using cached builder components.

        ~10ms vs ~50ms+ for standard create_order.
        Requires tick_size and neg_risk to be known upfront (skip HTTP lookups).
        """
        self.assert_level_1_auth()

        round_config = ROUNDING_CONFIG[tick_size]
        side = order_args.side
        price = order_args.price
        size = order_args.size

        # Calculate amounts (same logic as OrderBuilder.get_order_amounts)
        raw_price = round_normal(price, round_config.price)

        if side == "BUY":
            raw_taker_amt = round_down(size, round_config.size)
            raw_maker_amt = raw_taker_amt * raw_price
            if decimal_places(raw_maker_amt) > round_config.amount:
                raw_maker_amt = round_up(raw_maker_amt, round_config.amount + 4)
                if decimal_places(raw_maker_amt) > round_config.amount:
                    raw_maker_amt = round_down(raw_maker_amt, round_config.amount)
            utils_side = UtilsBuy
        else:  # SELL
            raw_maker_amt = round_down(size, round_config.size)
            raw_taker_amt = raw_maker_amt * raw_price
            if decimal_places(raw_taker_amt) > round_config.amount:
                raw_taker_amt = round_up(raw_taker_amt, round_config.amount + 4)
                if decimal_places(raw_taker_amt) > round_config.amount:
                    raw_taker_amt = round_down(raw_taker_amt, round_config.amount)
            utils_side = UtilsSell

        maker_amount = to_token_decimals(raw_maker_amt)
        taker_amount = to_token_decimals(raw_taker_amt)

        data = OrderData(
            maker=self._funder,
            taker=order_args.taker,
            tokenId=order_args.token_id,
            makerAmount=str(maker_amount),
            takerAmount=str(taker_amount),
            side=utils_side,
            feeRateBps=str(order_args.fee_rate_bps),
            nonce=str(order_args.nonce),
            signer=self.signer.address(),
            expiration=str(order_args.expiration),
            signatureType=self._sig_type,
        )

        utils_builder = self._get_utils_builder(neg_risk)
        return utils_builder.build_signed_order(data)

    async def create_market_order(
        self,
        order_args: MarketOrderArgs,
        options: Optional[PartialCreateOrderOptions] = None,
    ):
        """Create and sign a market order."""
        self.assert_level_1_auth()

        # Prefetch in parallel
        await self.prefetch_order_fields(order_args.token_id)

        tick_size = await self._resolve_tick_size(
            order_args.token_id,
            options.tick_size if options else None,
        )

        if order_args.price is None or order_args.price <= 0:
            order_args.price = await self._calculate_market_price(
                order_args.token_id,
                order_args.side,
                order_args.amount,
                order_args.order_type,
            )

        if not price_valid(order_args.price, tick_size):
            raise Exception(
                f"price ({order_args.price}), min: {tick_size} - max: {1 - float(tick_size)}"
            )

        neg_risk = (
            options.neg_risk
            if options and options.neg_risk
            else await self.get_neg_risk(order_args.token_id)
        )

        fee_rate_bps = await self._resolve_fee_rate(
            order_args.token_id, order_args.fee_rate_bps
        )
        order_args.fee_rate_bps = fee_rate_bps

        return self.builder.create_market_order(
            order_args,
            CreateOrderOptions(tick_size=tick_size, neg_risk=neg_risk),
        )

    async def _calculate_market_price(
        self, token_id: str, side: str, amount: float, order_type: OrderType
    ) -> float:
        book = await self.get_order_book(token_id)
        if book is None:
            raise Exception("no orderbook")
        if side == "BUY":
            if book.asks is None:
                raise Exception("no match")
            return self.builder.calculate_buy_market_price(book.asks, amount, order_type)
        else:
            if book.bids is None:
                raise Exception("no match")
            return self.builder.calculate_sell_market_price(book.bids, amount, order_type)

    # Post order
    async def post_order(self, order, order_type: OrderType = OrderType.GTC) -> dict:
        """Post a signed order."""
        self.assert_level_2_auth()
        body = order_to_json(order, self.creds.api_key, order_type)
        request_args = RequestArgs(method="POST", request_path=POST_ORDER, body=body)
        headers = create_level_2_headers(self.signer, self.creds, request_args)
        return await self._post(f"{self.host}{POST_ORDER}", headers=headers, data=body)

    # Cancel orders
    async def cancel(self, order_id: str) -> dict:
        """Cancel an order."""
        self.assert_level_2_auth()
        body = {"orderID": order_id}
        request_args = RequestArgs(method="DELETE", request_path=CANCEL, body=body)
        headers = create_level_2_headers(self.signer, self.creds, request_args)
        return await self._delete(f"{self.host}{CANCEL}", headers=headers, data=body)

    async def cancel_orders(self, order_ids: list[str]) -> dict:
        """Cancel multiple orders."""
        self.assert_level_2_auth()
        request_args = RequestArgs(method="DELETE", request_path=CANCEL_ORDERS, body=order_ids)
        headers = create_level_2_headers(self.signer, self.creds, request_args)
        return await self._delete(f"{self.host}{CANCEL_ORDERS}", headers=headers, data=order_ids)

    async def cancel_all(self) -> dict:
        """Cancel all orders."""
        self.assert_level_2_auth()
        request_args = RequestArgs(method="DELETE", request_path=CANCEL_ALL)
        headers = create_level_2_headers(self.signer, self.creds, request_args)
        return await self._delete(f"{self.host}{CANCEL_ALL}", headers=headers)

    # Get orders/trades
    async def get_order(self, order_id: str) -> dict:
        """Get an order by ID."""
        self.assert_level_2_auth()
        endpoint = f"{GET_ORDER}{order_id}"
        request_args = RequestArgs(method="GET", request_path=endpoint)
        headers = create_level_2_headers(self.signer, self.creds, request_args)
        return await self._get(f"{self.host}{endpoint}", headers=headers)

    async def get_orders(self, params: Optional[OpenOrderParams] = None, next_cursor: str = "MA==") -> list:
        """Get open orders with pagination."""
        self.assert_level_2_auth()
        request_args = RequestArgs(method="GET", request_path=ORDERS)
        headers = create_level_2_headers(self.signer, self.creds, request_args)

        results = []
        cursor = next_cursor or "MA=="
        while cursor != END_CURSOR:
            url = add_query_open_orders_params(f"{self.host}{ORDERS}", params, cursor)
            response = await self._get(url, headers=headers)
            cursor = response["next_cursor"]
            results += response["data"]
        return results

    async def get_trades(self, params: Optional[TradeParams] = None, next_cursor: str = "MA==") -> list:
        """Get trade history with pagination."""
        self.assert_level_2_auth()
        request_args = RequestArgs(method="GET", request_path=TRADES)
        headers = create_level_2_headers(self.signer, self.creds, request_args)

        results = []
        cursor = next_cursor or "MA=="
        while cursor != END_CURSOR:
            url = add_query_trade_params(f"{self.host}{TRADES}", params, cursor)
            response = await self._get(url, headers=headers)
            cursor = response["next_cursor"]
            results += response["data"]
        return results
