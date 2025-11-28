"""Data models for the arbitrage system."""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional


class Platform(Enum):
    """Trading platform identifier."""

    POLYMARKET = "polymarket"
    KALSHI = "kalshi"


class Side(Enum):
    """Order side."""

    BUY = "buy"
    SELL = "sell"


class OrderType(Enum):
    """Order type."""

    LIMIT = "limit"
    MARKET = "market"


class OrderStatus(Enum):
    """Order status."""

    PENDING = "pending"
    OPEN = "open"
    FILLED = "filled"
    PARTIALLY_FILLED = "partially_filled"
    CANCELLED = "cancelled"
    FAILED = "failed"


@dataclass
class Quote:
    """Real-time quote data."""

    platform: Platform
    contract_id: str
    bid: float  # Best bid price (0-1)
    ask: float  # Best ask price (0-1)
    bid_size: float  # Size at best bid
    ask_size: float  # Size at best ask
    timestamp: datetime = field(default_factory=datetime.utcnow)

    @property
    def midpoint(self) -> float:
        """Calculate midpoint price."""
        return (self.bid + self.ask) / 2

    @property
    def spread(self) -> float:
        """Calculate bid-ask spread."""
        return self.ask - self.bid


@dataclass
class ContractPair:
    """Mapping between Polymarket and Kalshi contracts for the same event."""

    event_name: str
    polymarket_token_id: str
    kalshi_ticker: str
    outcome: str  # e.g., "YES" or "NO"
    active: bool = True


@dataclass
class ArbitrageOpportunity:
    """Detected arbitrage opportunity."""

    contract_pair: ContractPair
    pm_quote: Quote
    kl_quote: Quote
    mode: str  # "M2T" or "T2T"
    net_profit_rate: float
    suggested_quantity: float
    pm_price: float  # Price to use on Polymarket
    kl_price: float  # Price to use on Kalshi
    timestamp: datetime = field(default_factory=datetime.utcnow)


@dataclass
class Order:
    """Order representation."""

    platform: Platform
    contract_id: str
    side: Side
    order_type: OrderType
    price: float
    quantity: float
    order_id: Optional[str] = None
    client_order_id: Optional[str] = None
    status: OrderStatus = OrderStatus.PENDING
    filled_quantity: float = 0.0
    average_fill_price: Optional[float] = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
class Position:
    """Current position in a contract."""

    platform: Platform
    contract_id: str
    quantity: float  # Positive for long, negative for short
    average_cost: float
    unrealized_pnl: float = 0.0


@dataclass
class TradeResult:
    """Result of an arbitrage trade execution."""

    success: bool
    pm_order: Optional[Order] = None
    kl_order: Optional[Order] = None
    net_profit: float = 0.0
    error_message: Optional[str] = None
    requires_panic_sell: bool = False
