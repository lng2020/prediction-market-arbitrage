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


@dataclass
class ArbitragePosition:
    """
    Paired arbitrage position tracking PM YES + KL NO.

    This represents a complete arbitrage position that needs to be
    exited together to realize profits.
    """

    position_id: str  # Unique identifier
    contract_pair: ContractPair

    # Polymarket side (YES position)
    pm_token_id: str
    pm_quantity: float
    pm_entry_price: float  # Price paid for YES

    # Kalshi side (NO position)
    kl_ticker: str
    kl_quantity: float
    kl_entry_price: float  # Price paid for NO

    # Totals
    total_entry_cost: float  # pm_entry_price * qty + kl_entry_price * qty

    created_at: datetime = field(default_factory=datetime.utcnow)

    @property
    def quantity(self) -> float:
        """Return the matched quantity (min of both sides)."""
        return min(self.pm_quantity, self.kl_quantity)

    def calculate_exit_value(self, pm_bid: float, kl_bid: float) -> float:
        """Calculate the value if we exit at current bids."""
        return (pm_bid * self.pm_quantity) + (kl_bid * self.kl_quantity)

    def calculate_exit_profit(self, pm_bid: float, kl_bid: float) -> float:
        """Calculate profit if we exit at current bids."""
        return self.calculate_exit_value(pm_bid, kl_bid) - self.total_entry_cost

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "position_id": self.position_id,
            "event_name": self.contract_pair.event_name,
            "pm_token_id": self.pm_token_id,
            "pm_quantity": self.pm_quantity,
            "pm_entry_price": self.pm_entry_price,
            "kl_ticker": self.kl_ticker,
            "kl_quantity": self.kl_quantity,
            "kl_entry_price": self.kl_entry_price,
            "total_entry_cost": self.total_entry_cost,
            "created_at": self.created_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: dict, contract_pair: ContractPair) -> "ArbitragePosition":
        """Create from dictionary."""
        return cls(
            position_id=data["position_id"],
            contract_pair=contract_pair,
            pm_token_id=data["pm_token_id"],
            pm_quantity=data["pm_quantity"],
            pm_entry_price=data["pm_entry_price"],
            kl_ticker=data["kl_ticker"],
            kl_quantity=data["kl_quantity"],
            kl_entry_price=data["kl_entry_price"],
            total_entry_cost=data["total_entry_cost"],
            created_at=datetime.fromisoformat(data["created_at"]),
        )


@dataclass
class ExitOpportunity:
    """Detected opportunity to exit an arbitrage position profitably."""

    position: ArbitragePosition
    pm_bid: float  # Current PM YES bid
    kl_bid: float  # Current KL NO bid
    exit_value: float  # Total value if exiting now
    profit: float  # Profit after subtracting entry cost
    profit_rate: float  # profit / entry_cost
    timestamp: datetime = field(default_factory=datetime.utcnow)
