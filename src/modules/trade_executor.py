"""
TradeExecutor Module (Module C)

Executes arbitrage trades with proper risk management.
"""

import asyncio
import logging
import uuid
from datetime import datetime
from typing import Optional

from ..clients import KalshiClient, PolymarketClient
from ..config import TradingConfig
from ..models import (
    ArbitrageOpportunity,
    Order,
    OrderStatus,
    OrderType,
    Platform,
    Side,
    TradeResult,
)

logger = logging.getLogger(__name__)


class TradeExecutor:
    """
    Executes arbitrage trades between Polymarket and Kalshi.

    Responsibilities:
    - C.1: M2T mode execution (Maker on PM, Taker on KL)
    - C.2: T2T mode execution (Taker on both)
    - C.3: Emergency panic sell
    - C.4: Order status tracking
    """

    def __init__(
        self,
        config: TradingConfig,
        kalshi_client: KalshiClient,
        polymarket_client: PolymarketClient,
    ):
        self.config = config
        self.kalshi = kalshi_client
        self.polymarket = polymarket_client

        # Track active orders and positions
        self._active_orders: dict[str, Order] = {}
        self._pending_hedges: dict[str, ArbitrageOpportunity] = {}

    async def execute_m2t(
        self,
        opportunity: ArbitrageOpportunity,
    ) -> TradeResult:
        """
        Execute Maker + Taker arbitrage.

        Flow:
        1. Place limit (maker) order on Polymarket
        2. Wait for maker order to fill
        3. Once filled, immediately place market (taker) order on Kalshi
        4. If taker fails, trigger panic sell

        This is the preferred mode for better pricing.
        """
        client_order_id = str(uuid.uuid4())
        logger.info(
            f"Executing M2T for {opportunity.contract_pair.event_name} "
            f"qty={opportunity.suggested_quantity:.2f}"
        )

        pm_order = None
        kl_order = None

        try:
            # Step 1: Place maker order on Polymarket
            pm_order = await self.polymarket.create_limit_order(
                token_id=opportunity.contract_pair.polymarket_token_id,
                side=Side.BUY,
                price=opportunity.pm_price,
                size=opportunity.suggested_quantity,
            )

            self._active_orders[pm_order.order_id] = pm_order
            self._pending_hedges[pm_order.order_id] = opportunity

            logger.info(f"PM maker order placed: {pm_order.order_id}")

            # Step 2: Wait for maker order to fill
            filled = await self._wait_for_fill(
                pm_order,
                timeout=self.config.maker_timeout_seconds,
            )

            if not filled:
                # Timeout - check for partial fills before cancelling
                logger.warning(f"PM maker order timeout, checking for partial fills")

                # Get final order state before cancelling
                orders = await self.polymarket.get_orders()
                for o in orders:
                    if o.get("id") == pm_order.order_id:
                        size_matched = float(o.get("size_matched", 0) or 0)
                        if size_matched > 0:
                            pm_order.filled_quantity = size_matched
                            pm_order.status = OrderStatus.PARTIALLY_FILLED
                            logger.info(f"PM order partially filled: {size_matched} shares")
                        break

                # Cancel unfilled portion
                try:
                    await self.polymarket.cancel_order(pm_order.order_id)
                except Exception as e:
                    logger.warning(f"Cancel order failed (may already be filled): {e}")

                # If we have partial fills, hedge them!
                if pm_order.filled_quantity and pm_order.filled_quantity > 0:
                    logger.info(f"Hedging partial fill of {pm_order.filled_quantity} shares on Kalshi")

                    try:
                        kl_order = await self.kalshi.create_order(
                            ticker=opportunity.contract_pair.kalshi_ticker,
                            side=Side.BUY,
                            action="buy",
                            count=int(pm_order.filled_quantity),
                            price_cents=int(opportunity.kl_price * 100),
                            order_type=OrderType.MARKET,
                            client_order_id=client_order_id,
                        )

                        del self._active_orders[pm_order.order_id]
                        del self._pending_hedges[pm_order.order_id]

                        return TradeResult(
                            success=True,
                            pm_order=pm_order,
                            kl_order=kl_order,
                            net_profit=(1.0 - pm_order.average_fill_price - opportunity.kl_price) * pm_order.filled_quantity if pm_order.average_fill_price else 0,
                        )
                    except Exception as e:
                        logger.error(f"Failed to hedge partial fill: {e}")
                        # Panic sell the PM position
                        await self._panic_sell(pm_order)
                        return TradeResult(
                            success=False,
                            pm_order=pm_order,
                            error_message=f"Partial fill hedge failed: {e}",
                            requires_panic_sell=True,
                        )

                del self._active_orders[pm_order.order_id]
                del self._pending_hedges[pm_order.order_id]

                return TradeResult(
                    success=False,
                    pm_order=pm_order,
                    error_message="Maker order timeout",
                )

            # Step 3: Maker filled - execute taker hedge on Kalshi
            logger.info(f"PM maker filled, executing KL taker hedge")

            # Calculate the dollar amount for Kalshi based on filled quantity
            kl_amount = pm_order.filled_quantity * opportunity.kl_price

            kl_order = await self.kalshi.create_order(
                ticker=opportunity.contract_pair.kalshi_ticker,
                side=Side.BUY,
                action="buy",
                count=int(pm_order.filled_quantity),
                price_cents=int(opportunity.kl_price * 100),
                order_type=OrderType.MARKET,
                client_order_id=client_order_id,
            )

            self._active_orders[kl_order.order_id] = kl_order

            # Check if Kalshi order filled
            if kl_order.status not in [OrderStatus.FILLED, OrderStatus.OPEN]:
                # Taker failed - PANIC SELL
                logger.error(f"KL taker failed! Triggering panic sell")
                await self._panic_sell(pm_order)

                return TradeResult(
                    success=False,
                    pm_order=pm_order,
                    kl_order=kl_order,
                    error_message="Taker hedge failed, panic sell executed",
                    requires_panic_sell=True,
                )

            # Success!
            net_profit = (
                1.0
                - (pm_order.average_fill_price or opportunity.pm_price)
                - (kl_order.average_fill_price or opportunity.kl_price)
            ) * pm_order.filled_quantity

            logger.info(
                f"M2T trade completed successfully! "
                f"Net profit: ${net_profit:.2f}"
            )

            # Cleanup
            del self._active_orders[pm_order.order_id]
            del self._pending_hedges[pm_order.order_id]
            if kl_order.order_id in self._active_orders:
                del self._active_orders[kl_order.order_id]

            return TradeResult(
                success=True,
                pm_order=pm_order,
                kl_order=kl_order,
                net_profit=net_profit,
            )

        except Exception as e:
            logger.error(f"M2T execution error: {e}")

            # If we have an unfilled PM order, cancel it
            if pm_order and pm_order.order_id:
                try:
                    await self.polymarket.cancel_order(pm_order.order_id)
                except Exception:
                    pass

            # If PM was filled but KL failed, panic sell
            if pm_order and pm_order.status == OrderStatus.FILLED and not kl_order:
                await self._panic_sell(pm_order)

            return TradeResult(
                success=False,
                pm_order=pm_order,
                kl_order=kl_order,
                error_message=str(e),
            )

    async def execute_t2t(
        self,
        opportunity: ArbitrageOpportunity,
    ) -> TradeResult:
        """
        Execute Taker + Taker arbitrage.

        Flow:
        1. Place market orders on both platforms nearly simultaneously
        2. Monitor both for fills
        3. If one fails, panic sell the other

        This mode is faster but typically has worse pricing.
        """
        client_order_id = str(uuid.uuid4())
        logger.info(
            f"Executing T2T for {opportunity.contract_pair.event_name} "
            f"qty={opportunity.suggested_quantity:.2f}"
        )

        pm_order = None
        kl_order = None

        try:
            # Execute both orders concurrently
            pm_task = self.polymarket.create_market_order(
                token_id=opportunity.contract_pair.polymarket_token_id,
                side=Side.BUY,
                amount=opportunity.suggested_quantity * opportunity.pm_price,
            )

            kl_task = self.kalshi.create_order(
                ticker=opportunity.contract_pair.kalshi_ticker,
                side=Side.BUY,
                action="buy",
                count=int(opportunity.suggested_quantity),
                price_cents=int(opportunity.kl_price * 100),
                order_type=OrderType.MARKET,
                client_order_id=client_order_id,
            )

            # Wait for both orders
            results = await asyncio.gather(pm_task, kl_task, return_exceptions=True)

            pm_result, kl_result = results

            # Handle PM result
            if isinstance(pm_result, Exception):
                logger.error(f"PM taker failed: {pm_result}")
                pm_order = None
            else:
                pm_order = pm_result
                self._active_orders[pm_order.order_id] = pm_order

            # Handle KL result
            if isinstance(kl_result, Exception):
                logger.error(f"KL taker failed: {kl_result}")
                kl_order = None
            else:
                kl_order = kl_result
                self._active_orders[kl_order.order_id] = kl_order

            # Check for partial execution - need panic sell
            pm_filled = pm_order and pm_order.status in [
                OrderStatus.FILLED,
                OrderStatus.PARTIALLY_FILLED,
            ]
            kl_filled = kl_order and kl_order.status in [
                OrderStatus.FILLED,
                OrderStatus.PARTIALLY_FILLED,
            ]

            if pm_filled and not kl_filled:
                logger.error("PM filled but KL failed - panic sell PM")
                await self._panic_sell(pm_order)
                return TradeResult(
                    success=False,
                    pm_order=pm_order,
                    kl_order=kl_order,
                    error_message="Partial execution - KL failed",
                    requires_panic_sell=True,
                )

            if kl_filled and not pm_filled:
                logger.error("KL filled but PM failed - panic sell KL")
                await self._panic_sell_kalshi(kl_order)
                return TradeResult(
                    success=False,
                    pm_order=pm_order,
                    kl_order=kl_order,
                    error_message="Partial execution - PM failed",
                    requires_panic_sell=True,
                )

            if not pm_filled and not kl_filled:
                return TradeResult(
                    success=False,
                    pm_order=pm_order,
                    kl_order=kl_order,
                    error_message="Both orders failed",
                )

            # Both filled - success!
            net_profit = (
                1.0
                - (pm_order.average_fill_price or opportunity.pm_price)
                - (kl_order.average_fill_price or opportunity.kl_price)
            ) * min(pm_order.filled_quantity, kl_order.filled_quantity)

            logger.info(f"T2T trade completed successfully! Net profit: ${net_profit:.2f}")

            # Cleanup
            if pm_order.order_id in self._active_orders:
                del self._active_orders[pm_order.order_id]
            if kl_order.order_id in self._active_orders:
                del self._active_orders[kl_order.order_id]

            return TradeResult(
                success=True,
                pm_order=pm_order,
                kl_order=kl_order,
                net_profit=net_profit,
            )

        except Exception as e:
            logger.error(f"T2T execution error: {e}")

            return TradeResult(
                success=False,
                pm_order=pm_order,
                kl_order=kl_order,
                error_message=str(e),
            )

    async def execute(self, opportunity: ArbitrageOpportunity) -> TradeResult:
        """
        Execute an arbitrage opportunity using the specified mode.

        Dispatches to M2T or T2T based on opportunity.mode.
        """
        if opportunity.mode == "M2T":
            return await self.execute_m2t(opportunity)
        elif opportunity.mode == "T2T":
            return await self.execute_t2t(opportunity)
        else:
            return TradeResult(
                success=False,
                error_message=f"Unknown execution mode: {opportunity.mode}",
            )

    async def _wait_for_fill(
        self,
        order: Order,
        timeout: float,
        poll_interval: float = 0.1,  # Reduced from 0.5s for faster fill detection
    ) -> bool:
        """
        Wait for an order to fill.

        Returns True if filled within timeout, False otherwise.
        """
        start_time = asyncio.get_event_loop().time()

        while asyncio.get_event_loop().time() - start_time < timeout:
            # Check order status
            if order.platform == Platform.POLYMARKET:
                orders = await self.polymarket.get_orders()
                for o in orders:
                    if o.get("id") == order.order_id:
                        status = o.get("status", "").lower()
                        size_matched = float(o.get("size_matched", 0) or 0)
                        original_size = float(o.get("original_size", order.quantity) or order.quantity)

                        if status == "matched" or size_matched >= original_size * 0.99:
                            # Fully filled
                            order.status = OrderStatus.FILLED
                            order.filled_quantity = size_matched if size_matched > 0 else order.quantity
                            return True
                        elif size_matched > 0:
                            # Partially filled - update filled quantity and continue waiting
                            order.filled_quantity = size_matched
                            order.status = OrderStatus.PARTIALLY_FILLED
                            logger.debug(f"PM order partially filled: {size_matched}/{original_size}")
                        elif status == "cancelled":
                            order.status = OrderStatus.CANCELLED
                            return False
            else:
                order_data = await self.kalshi.get_order(order.order_id)
                status = order_data.get("status", "")
                if status == "executed":
                    order.status = OrderStatus.FILLED
                    order.filled_quantity = order_data.get("fill_count", 0)
                    return True
                elif status == "canceled":
                    order.status = OrderStatus.CANCELLED
                    return False

            await asyncio.sleep(poll_interval)

        return False

    async def _panic_sell(self, order: Order) -> None:
        """
        Emergency panic sell on Polymarket.

        Executes market sell to close position immediately.
        """
        logger.warning(f"PANIC SELL on PM: {order.contract_id}")

        try:
            # Market sell the filled quantity
            sell_order = await self.polymarket.create_market_order(
                token_id=order.contract_id,
                side=Side.SELL,
                amount=order.filled_quantity,
            )
            logger.info(f"Panic sell executed: {sell_order.order_id}")
        except Exception as e:
            logger.error(f"PANIC SELL FAILED: {e}")

    async def _panic_sell_kalshi(self, order: Order) -> None:
        """
        Emergency panic sell on Kalshi.

        Executes market sell to close position immediately.
        Uses urgent priority to skip rate limiter queue.
        """
        logger.warning(f"PANIC SELL on KL: {order.contract_id}")

        try:
            sell_order = await self.kalshi.create_order(
                ticker=order.contract_id,
                side=Side.SELL,
                action="sell",
                count=int(order.filled_quantity),
                price_cents=1,  # Market sell - very low price
                order_type=OrderType.MARKET,
                urgent=True,  # Skip rate limiter queue for emergency sells
            )
            logger.info(f"Panic sell executed: {sell_order.order_id}")
        except Exception as e:
            logger.error(f"PANIC SELL FAILED: {e}")

    async def cancel_all_orders(self) -> None:
        """Cancel all active orders on both platforms."""
        logger.info("Cancelling all active orders")

        # Cancel PM orders
        try:
            await self.polymarket.cancel_all_orders()
        except Exception as e:
            logger.error(f"Failed to cancel PM orders: {e}")

        # Cancel KL orders individually
        for order_id, order in list(self._active_orders.items()):
            if order.platform == Platform.KALSHI:
                try:
                    await self.kalshi.cancel_order(order_id)
                except Exception as e:
                    logger.error(f"Failed to cancel KL order {order_id}: {e}")

        self._active_orders.clear()
        self._pending_hedges.clear()

    def get_active_orders(self) -> list[Order]:
        """Get list of active orders."""
        return list(self._active_orders.values())
