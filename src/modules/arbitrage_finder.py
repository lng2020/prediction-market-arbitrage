"""
ArbitrageFinder Module (Module B)

Analyzes quotes to find arbitrage opportunities between Polymarket and Kalshi.
"""

import logging
from dataclasses import dataclass
from typing import Optional

from ..clients import KalshiClient, PolymarketClient
from ..config import TradingConfig
from ..models import ArbitrageOpportunity, ContractPair, Quote

logger = logging.getLogger(__name__)


@dataclass
class CostAnalysis:
    """Detailed cost breakdown for an arbitrage trade."""

    pm_price: float
    kl_price: float
    pm_fee: float
    kl_fee: float
    total_cost: float
    guaranteed_payout: float
    net_profit: float
    profit_rate: float


class ArbitrageFinder:
    """
    Finds arbitrage opportunities between Polymarket and Kalshi.

    The core arbitrage logic:
    - Buy YES on one platform, Buy YES on the other (for the opposite outcome)
    - OR Buy YES + NO on the same event across platforms
    - If sum of prices < 1 (100%), there's arbitrage profit

    Responsibilities:
    - B.1: Net cost calculation with fees
    - B.2: Taker + Taker (T2T) mode analysis
    - B.3: Maker + Taker (M2T) mode analysis
    - B.4: Minimum profit filtering
    """

    def __init__(self, config: TradingConfig):
        self.config = config

    def calculate_net_cost_t2t(
        self,
        pm_quote: Quote,
        kl_quote: Quote,
        quantity: float,
    ) -> CostAnalysis:
        """
        Calculate net cost for Taker + Taker mode.

        Both orders are market/taker orders executed at ask prices.

        Strategy: Buy YES on PM at ask, Buy NO on KL at ask
        (or equivalently, the reciprocal positions)
        """
        # Use ask prices for taker orders
        pm_price = pm_quote.ask
        kl_price = kl_quote.ask

        # Calculate fees
        pm_fee = PolymarketClient.calculate_taker_fee(quantity * pm_price)
        kl_fee = KalshiClient.calculate_taker_fee(int(quantity), kl_price)

        # Total cost = PM ask + KL ask + fees
        # For binary outcomes where PM_YES + KL_NO should = 1
        # We buy PM_YES and KL_NO (which costs 1 - KL_YES_bid)
        # Simplified: total cost = pm_ask + (1 - kl_bid)
        # But since we're comparing same event: total_cost = pm_ask + kl_ask

        # Actually for arbitrage:
        # If PM has YES token and KL has YES contract for same outcome:
        # Buy YES on platform with lower ask
        # Or buy complementary positions if sum < 1

        total_cost = pm_price + (1 - kl_quote.bid) + pm_fee + kl_fee

        # Guaranteed payout: One of the positions pays $1
        guaranteed_payout = 1.0

        net_profit = guaranteed_payout - total_cost
        profit_rate = net_profit / total_cost if total_cost > 0 else 0

        return CostAnalysis(
            pm_price=pm_price,
            kl_price=1 - kl_quote.bid,  # Cost to buy NO on Kalshi
            pm_fee=pm_fee,
            kl_fee=kl_fee,
            total_cost=total_cost,
            guaranteed_payout=guaranteed_payout,
            net_profit=net_profit,
            profit_rate=profit_rate,
        )

    def calculate_net_cost_m2t(
        self,
        pm_quote: Quote,
        kl_quote: Quote,
        target_maker_price: float,
        quantity: float,
    ) -> CostAnalysis:
        """
        Calculate potential net cost for Maker + Taker mode.

        PM order is a limit/maker order at target_maker_price.
        KL order is a market/taker order at ask price.
        """
        pm_price = target_maker_price
        kl_price = 1 - kl_quote.bid  # Cost to buy NO

        # Maker fee on PM (currently 0)
        pm_fee = PolymarketClient.calculate_maker_fee(quantity * pm_price)
        # Taker fee on KL
        kl_fee = KalshiClient.calculate_taker_fee(int(quantity), kl_price)

        total_cost = pm_price + kl_price + pm_fee + kl_fee
        guaranteed_payout = 1.0

        net_profit = guaranteed_payout - total_cost
        profit_rate = net_profit / total_cost if total_cost > 0 else 0

        return CostAnalysis(
            pm_price=pm_price,
            kl_price=kl_price,
            pm_fee=pm_fee,
            kl_fee=kl_fee,
            total_cost=total_cost,
            guaranteed_payout=guaranteed_payout,
            net_profit=net_profit,
            profit_rate=profit_rate,
        )

    def calculate_optimal_maker_price(
        self,
        pm_quote: Quote,
        kl_quote: Quote,
    ) -> float:
        """
        Calculate optimal maker price on PM to achieve minimum profit target.

        Given KL price is fixed, find PM price such that:
        profit_rate >= min_profit_target
        """
        kl_cost = 1 - kl_quote.bid
        kl_fee = KalshiClient.calculate_taker_fee(1, kl_cost)

        # Target: (1 - pm_price - kl_cost - kl_fee) / (pm_price + kl_cost + kl_fee) >= min_profit
        # Solve for pm_price:
        # 1 - pm_price - kl_cost - kl_fee >= min_profit * (pm_price + kl_cost + kl_fee)
        # 1 - kl_cost - kl_fee >= pm_price + min_profit * pm_price + min_profit * (kl_cost + kl_fee)
        # 1 - kl_cost - kl_fee - min_profit * (kl_cost + kl_fee) >= pm_price * (1 + min_profit)
        # pm_price <= (1 - (1 + min_profit) * (kl_cost + kl_fee)) / (1 + min_profit)

        min_profit = self.config.min_profit_target
        max_pm_price = (1 - (1 + min_profit) * (kl_cost + kl_fee)) / (1 + min_profit)

        # Price should be at least slightly better than current bid to get filled
        optimal_price = min(max_pm_price, pm_quote.bid + 0.001)

        # Ensure price is reasonable (between 0 and 1)
        return max(0.01, min(0.99, optimal_price))

    def find_t2t_opportunity(
        self,
        contract_pair: ContractPair,
        pm_quote: Quote,
        kl_quote: Quote,
    ) -> Optional[ArbitrageOpportunity]:
        """
        Find Taker + Taker arbitrage opportunity.

        Returns opportunity if profit rate >= min_profit_target.
        """
        # Calculate quantity based on capital and prices
        avg_price = (pm_quote.ask + (1 - kl_quote.bid)) / 2
        quantity = self.config.capital_per_trade / avg_price

        # Account for available liquidity
        max_quantity = min(pm_quote.ask_size, kl_quote.bid_size, quantity)

        if max_quantity <= 0:
            return None

        analysis = self.calculate_net_cost_t2t(pm_quote, kl_quote, max_quantity)

        if analysis.profit_rate >= self.config.min_profit_target:
            logger.info(
                f"T2T opportunity found: {contract_pair.event_name} "
                f"profit_rate={analysis.profit_rate:.4f}"
            )
            return ArbitrageOpportunity(
                contract_pair=contract_pair,
                pm_quote=pm_quote,
                kl_quote=kl_quote,
                mode="T2T",
                net_profit_rate=analysis.profit_rate,
                suggested_quantity=max_quantity,
                pm_price=analysis.pm_price,
                kl_price=analysis.kl_price,
            )

        return None

    def find_m2t_opportunity(
        self,
        contract_pair: ContractPair,
        pm_quote: Quote,
        kl_quote: Quote,
        target_maker_price: Optional[float] = None,
    ) -> Optional[ArbitrageOpportunity]:
        """
        Find Maker + Taker arbitrage opportunity.

        PM: Limit order (maker) at target_maker_price
        KL: Market order (taker)

        Returns opportunity if potential profit rate >= min_profit_target.
        """
        if target_maker_price is None:
            target_maker_price = self.calculate_optimal_maker_price(pm_quote, kl_quote)

        # Calculate quantity
        avg_price = (target_maker_price + (1 - kl_quote.bid)) / 2
        quantity = self.config.capital_per_trade / avg_price

        # Account for liquidity (for maker, we care about the opposite side)
        max_quantity = min(kl_quote.bid_size, quantity)

        if max_quantity <= 0:
            return None

        analysis = self.calculate_net_cost_m2t(
            pm_quote, kl_quote, target_maker_price, max_quantity
        )

        if analysis.profit_rate >= self.config.min_profit_target:
            logger.info(
                f"M2T opportunity found: {contract_pair.event_name} "
                f"profit_rate={analysis.profit_rate:.4f} "
                f"maker_price={target_maker_price:.4f}"
            )
            return ArbitrageOpportunity(
                contract_pair=contract_pair,
                pm_quote=pm_quote,
                kl_quote=kl_quote,
                mode="M2T",
                net_profit_rate=analysis.profit_rate,
                suggested_quantity=max_quantity,
                pm_price=target_maker_price,
                kl_price=analysis.kl_price,
            )

        return None

    def find_opportunities(
        self,
        contract_pair: ContractPair,
        pm_quote: Quote,
        kl_quote: Quote,
    ) -> list[ArbitrageOpportunity]:
        """
        Find all arbitrage opportunities for a contract pair.

        Returns list of opportunities, prioritized by mode (M2T > T2T).
        """
        opportunities = []

        # Check M2T first (preferred due to better pricing)
        m2t = self.find_m2t_opportunity(contract_pair, pm_quote, kl_quote)
        if m2t:
            opportunities.append(m2t)

        # Check T2T as fallback
        t2t = self.find_t2t_opportunity(contract_pair, pm_quote, kl_quote)
        if t2t:
            opportunities.append(t2t)

        return opportunities

    def analyze_all_pairs(
        self,
        pairs_quotes: dict[str, dict],
    ) -> list[ArbitrageOpportunity]:
        """
        Analyze all contract pairs for arbitrage opportunities.

        Args:
            pairs_quotes: Dict mapping event_name to {"pm": Quote, "kl": Quote, "pair": ContractPair}

        Returns:
            List of all opportunities found, sorted by profit rate descending.
        """
        all_opportunities = []

        for event_name, data in pairs_quotes.items():
            pm_quote = data.get("pm")
            kl_quote = data.get("kl")
            pair = data.get("pair")

            if not all([pm_quote, kl_quote, pair]):
                continue

            # Calculate spread for logging
            # Spread = 1 - (PM_ask + KL_no_cost) where KL_no_cost = 1 - KL_bid
            pm_ask = pm_quote.ask
            kl_no_cost = 1 - kl_quote.bid
            total_cost = pm_ask + kl_no_cost
            spread = 1 - total_cost  # Positive spread = potential profit

            opportunities = self.find_opportunities(pair, pm_quote, kl_quote)

            # Log analysis in verbose mode
            if opportunities:
                logger.debug(
                    f"[Analysis] {pair.event_name} - {pair.outcome} | "
                    f"PM_ask={pm_ask:.3f} KL_no={kl_no_cost:.3f} | "
                    f"Spread: {spread*100:+.1f}% | OPPORTUNITY"
                )
            else:
                logger.debug(
                    f"[Analysis] {pair.event_name} - {pair.outcome} | "
                    f"PM_ask={pm_ask:.3f} KL_no={kl_no_cost:.3f} | "
                    f"Spread: {spread*100:+.1f}%"
                )

            all_opportunities.extend(opportunities)

        # Sort by profit rate descending
        all_opportunities.sort(key=lambda o: o.net_profit_rate, reverse=True)

        return all_opportunities
