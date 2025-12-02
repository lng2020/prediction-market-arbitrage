"""Configuration management for the arbitrage system."""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

from dotenv import load_dotenv

load_dotenv()


@dataclass
class KalshiConfig:
    """Kalshi API configuration."""

    api_key_id: str = field(default_factory=lambda: os.getenv("KALSHI_API_KEY_ID", ""))
    private_key_path: str = field(
        default_factory=lambda: os.getenv("KALSHI_PRIVATE_KEY_PATH", "")
    )
    env: Literal["demo", "prod"] = field(
        default_factory=lambda: os.getenv("KALSHI_ENV", "demo")  # type: ignore
    )

    @property
    def base_url(self) -> str:
        if self.env == "prod":
            return "https://api.elections.kalshi.com/trade-api/v2"
        return "https://demo-api.kalshi.co/trade-api/v2"

    @property
    def ws_url(self) -> str:
        if self.env == "prod":
            return "wss://api.elections.kalshi.com/trade-api/ws/v2"
        return "wss://demo-api.kalshi.co/trade-api/ws/v2"

    def load_private_key(self) -> str:
        """Load private key from file."""
        path = Path(self.private_key_path)
        if not path.exists():
            raise FileNotFoundError(f"Kalshi private key not found: {path}")
        return path.read_text()


@dataclass
class PolymarketConfig:
    """Polymarket API configuration."""

    private_key: str = field(default_factory=lambda: os.getenv("POLYMARKET_PRIVATE_KEY", ""))
    funder_address: str = field(
        default_factory=lambda: os.getenv("POLYMARKET_FUNDER_ADDRESS", "")
    )
    signature_type: int = field(
        default_factory=lambda: int(os.getenv("POLYMARKET_SIGNATURE_TYPE", "0"))
    )
    chain_id: int = 137  # Polygon mainnet

    @property
    def base_url(self) -> str:
        return "https://clob.polymarket.com"

    @property
    def ws_url(self) -> str:
        return "wss://ws-subscriptions-clob.polymarket.com/ws"


class MarketCategory:
    """Market categories for phased rollout."""

    NBA = "nba"
    NFL = "nfl"
    MLB = "mlb"
    NHL = "nhl"
    SOCCER = "soccer"
    CS2 = "cs2"
    ALL_SPORTS = "all_sports"
    ALL_MARKETS = "all_markets"

    # Kalshi series tickers by category (futures/championship)
    KALSHI_SERIES = {
        NBA: ["KXNBA", "NBA"],
        NFL: ["KXNFL", "NFL"],
        MLB: ["KXMLB", "MLB"],
        NHL: ["KXNHL", "NHL"],
        SOCCER: ["KXSOCCER", "SOCCER"],
        CS2: ["KXCSGOGAME"],
    }

    # Kalshi series tickers for daily game markets
    KALSHI_GAME_SERIES = {
        NBA: ["KXNBAGAME"],
        NFL: ["KXNFLGAME"],
        MLB: ["KXMLBGAME"],
        NHL: ["KXNHLGAME"],
        CS2: ["KXCSGOGAME"],
    }

    # Polymarket search terms by category
    POLYMARKET_TAGS = {
        NBA: ["nba", "basketball"],
        NFL: ["nfl", "football"],
        MLB: ["mlb", "baseball"],
        NHL: ["nhl", "hockey"],
        SOCCER: ["soccer", "football", "premier league", "mls"],
        CS2: ["cs2", "counter-strike", "csgo", "counter strike"],
    }


@dataclass
class TradingConfig:
    """Trading parameters configuration."""

    min_profit_target: float = field(
        default_factory=lambda: float(os.getenv("MIN_PROFIT_TARGET", "0.002"))
    )
    capital_per_trade: float = field(
        default_factory=lambda: float(os.getenv("CAPITAL_PER_TRADE", "5"))
    )
    slippage_buffer: float = field(
        default_factory=lambda: float(os.getenv("SLIPPAGE_BUFFER", "0.005"))
    )
    maker_timeout_seconds: float = 30.0  # Give maker orders time to fill
    max_retries: int = 3
    min_spread_threshold: float = 0.005  # Only trade when spread >= 0.5% (real edge)
    # Maker price aggressiveness: 0.0 = at bid (conservative), 1.0 = at ask (aggressive)
    # 0.5 = midpoint between bid and ask
    maker_aggressiveness: float = field(
        default_factory=lambda: float(os.getenv("MAKER_AGGRESSIVENESS", "0.7"))
    )

    # Market categories to trade (NBA only - CS2 has low liquidity)
    enabled_categories: list[str] = field(
        default_factory=lambda: os.getenv(
            "ENABLED_CATEGORIES", MarketCategory.NBA
        ).split(",")
    )


@dataclass
class Config:
    """Main configuration container."""

    kalshi: KalshiConfig = field(default_factory=KalshiConfig)
    polymarket: PolymarketConfig = field(default_factory=PolymarketConfig)
    trading: TradingConfig = field(default_factory=TradingConfig)


def load_config() -> Config:
    """Load configuration from environment."""
    return Config()
