"""
PositionManager Module

Tracks open arbitrage positions and detects exit opportunities.
"""

import json
import logging
import uuid
from pathlib import Path
from typing import Optional

from ..models import (
    ArbitrageOpportunity,
    ArbitragePosition,
    ContractPair,
    ExitOpportunity,
    Quote,
    TradeResult,
)

logger = logging.getLogger(__name__)


class PositionManager:
    """
    Manages arbitrage positions and exit opportunities.

    Responsibilities:
    - Track open positions (PM YES + KL NO pairs)
    - Persist positions to JSON file
    - Detect profitable exit opportunities
    - Remove closed positions
    """

    def __init__(
        self,
        positions_file: str = "data/positions.json",
        min_exit_profit_rate: float = 0.005,  # 0.5% minimum profit to exit
    ):
        self.positions_file = Path(positions_file)
        self.min_exit_profit_rate = min_exit_profit_rate
        self._positions: dict[str, ArbitragePosition] = {}
        self._contract_pairs: dict[str, ContractPair] = {}  # Cache of contract pairs

        # Load existing positions
        self._load_positions()

    def _load_positions(self) -> None:
        """Load positions from JSON file."""
        if not self.positions_file.exists():
            logger.info("No existing positions file found")
            return

        try:
            with open(self.positions_file, "r") as f:
                data = json.load(f)

            for pos_data in data.get("positions", []):
                # Reconstruct ContractPair
                pair = ContractPair(
                    event_name=pos_data.get("event_name", "Unknown"),
                    polymarket_token_id=pos_data["pm_token_id"],
                    kalshi_ticker=pos_data["kl_ticker"],
                    outcome="YES",
                )
                self._contract_pairs[pos_data["position_id"]] = pair

                position = ArbitragePosition.from_dict(pos_data, pair)
                self._positions[position.position_id] = position

            logger.info(f"Loaded {len(self._positions)} existing positions")
        except Exception as e:
            logger.error(f"Failed to load positions: {e}")

    def _save_positions(self) -> None:
        """Save positions to JSON file."""
        try:
            self.positions_file.parent.mkdir(parents=True, exist_ok=True)

            data = {
                "positions": [pos.to_dict() for pos in self._positions.values()]
            }

            with open(self.positions_file, "w") as f:
                json.dump(data, f, indent=2)

            logger.debug(f"Saved {len(self._positions)} positions to {self.positions_file}")
        except Exception as e:
            logger.error(f"Failed to save positions: {e}")

    def record_position(
        self,
        opportunity: ArbitrageOpportunity,
        trade_result: TradeResult,
    ) -> Optional[ArbitragePosition]:
        """
        Record a new arbitrage position after successful trade.

        Args:
            opportunity: The arbitrage opportunity that was executed
            trade_result: The result of the trade execution

        Returns:
            The created ArbitragePosition, or None if trade wasn't successful
        """
        if not trade_result.success:
            return None

        pm_order = trade_result.pm_order
        kl_order = trade_result.kl_order

        if not pm_order or not kl_order:
            logger.warning("Cannot record position: missing order data")
            return None

        position_id = str(uuid.uuid4())

        # Calculate quantities and prices
        # Use filled_quantity if available, otherwise fall back to order quantity or opportunity quantity
        pm_qty = pm_order.filled_quantity or pm_order.quantity or opportunity.suggested_quantity
        kl_qty = kl_order.filled_quantity or kl_order.quantity or int(opportunity.suggested_quantity)

        # Ensure we have valid quantities
        if pm_qty <= 0 or kl_qty <= 0:
            logger.warning(f"Cannot record position: invalid quantities pm={pm_qty}, kl={kl_qty}")
            return None

        pm_price = pm_order.average_fill_price or opportunity.pm_price
        kl_price = kl_order.average_fill_price or opportunity.kl_price

        total_cost = (pm_price * pm_qty) + (kl_price * kl_qty)

        position = ArbitragePosition(
            position_id=position_id,
            contract_pair=opportunity.contract_pair,
            pm_token_id=opportunity.contract_pair.polymarket_token_id,
            pm_quantity=pm_qty,
            pm_entry_price=pm_price,
            kl_ticker=opportunity.contract_pair.kalshi_ticker,
            kl_quantity=kl_qty,
            kl_entry_price=kl_price,
            total_entry_cost=total_cost,
        )

        self._positions[position_id] = position
        self._contract_pairs[position_id] = opportunity.contract_pair
        self._save_positions()

        logger.info(
            f"Recorded position {position_id[:8]}: "
            f"{opportunity.contract_pair.event_name} "
            f"qty={position.quantity:.2f} cost=${total_cost:.2f}"
        )

        return position

    def get_position(self, position_id: str) -> Optional[ArbitragePosition]:
        """Get a position by ID."""
        return self._positions.get(position_id)

    def get_all_positions(self) -> list[ArbitragePosition]:
        """Get all open positions."""
        return list(self._positions.values())

    def get_positions_for_contract(self, pm_token_id: str) -> list[ArbitragePosition]:
        """Get all positions for a specific Polymarket token."""
        return [
            pos for pos in self._positions.values()
            if pos.pm_token_id == pm_token_id
        ]

    def remove_position(self, position_id: str) -> bool:
        """Remove a closed position."""
        if position_id in self._positions:
            del self._positions[position_id]
            if position_id in self._contract_pairs:
                del self._contract_pairs[position_id]
            self._save_positions()
            logger.info(f"Removed position {position_id[:8]}")
            return True
        return False

    def find_exit_opportunity(
        self,
        position: ArbitragePosition,
        pm_quote: Quote,
        kl_quote: Quote,
    ) -> Optional[ExitOpportunity]:
        """
        Check if a position can be exited profitably.

        For exit:
        - Sell PM YES at bid
        - Sell KL NO at bid (buy YES to close)

        Args:
            position: The position to check
            pm_quote: Current Polymarket quote
            kl_quote: Current Kalshi quote (for YES side)

        Returns:
            ExitOpportunity if profitable, None otherwise
        """
        # Skip positions with invalid quantities
        if position.quantity <= 0:
            return None

        pm_bid = pm_quote.bid
        # For KL NO position, to close we need the NO bid
        # kl_quote is for YES, so NO bid = 1 - YES ask
        kl_no_bid = 1 - kl_quote.ask

        # Calculate exit value and profit
        exit_value = position.calculate_exit_value(pm_bid, kl_no_bid)
        profit = position.calculate_exit_profit(pm_bid, kl_no_bid)
        profit_rate = profit / position.total_entry_cost if position.total_entry_cost > 0 else 0

        # Check if profitable enough
        if profit_rate >= self.min_exit_profit_rate:
            logger.info(
                f"Exit opportunity for {position.contract_pair.event_name}: "
                f"profit=${profit:.2f} ({profit_rate*100:.2f}%)"
            )
            return ExitOpportunity(
                position=position,
                pm_bid=pm_bid,
                kl_bid=kl_no_bid,
                exit_value=exit_value,
                profit=profit,
                profit_rate=profit_rate,
            )

        return None

    def find_all_exit_opportunities(
        self,
        quotes: dict[str, dict],
    ) -> list[ExitOpportunity]:
        """
        Find all exit opportunities across all positions.

        Args:
            quotes: Dict mapping pm_token_id to {"pm": Quote, "kl": Quote}

        Returns:
            List of exit opportunities, sorted by profit rate descending
        """
        opportunities = []

        for position in self._positions.values():
            quote_data = quotes.get(position.pm_token_id)
            if not quote_data:
                continue

            pm_quote = quote_data.get("pm")
            kl_quote = quote_data.get("kl")

            if not pm_quote or not kl_quote:
                continue

            exit_opp = self.find_exit_opportunity(position, pm_quote, kl_quote)
            if exit_opp:
                opportunities.append(exit_opp)

        # Sort by profit rate descending
        opportunities.sort(key=lambda o: o.profit_rate, reverse=True)

        return opportunities

    def get_total_value(self) -> float:
        """Get total entry cost of all positions."""
        return sum(pos.total_entry_cost for pos in self._positions.values())

    def get_position_count(self) -> int:
        """Get number of open positions."""
        return len(self._positions)

    def get_summary(self) -> dict:
        """Get summary of all positions."""
        return {
            "position_count": len(self._positions),
            "total_entry_cost": self.get_total_value(),
            "positions": [
                {
                    "id": pos.position_id[:8],
                    "event": pos.contract_pair.event_name,
                    "qty": pos.quantity,
                    "cost": pos.total_entry_cost,
                }
                for pos in self._positions.values()
            ],
        }
