"""
Results Recorder Module

Records and tracks all trading activity for analysis and auditing.
"""

import csv
import json
import logging
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

from ..models import ArbitrageOpportunity, Order, Platform, TradeResult

logger = logging.getLogger(__name__)


@dataclass
class TradeRecord:
    """Complete record of a trade execution."""

    # Identifiers
    trade_id: str
    timestamp: str

    # Event info
    event_name: str
    category: str
    outcome: str

    # Execution details
    mode: str  # M2T or T2T
    success: bool

    # Polymarket side
    pm_token_id: str
    pm_order_id: Optional[str]
    pm_side: str
    pm_price: float
    pm_quantity: float
    pm_filled_quantity: float
    pm_status: str

    # Kalshi side
    kl_ticker: str
    kl_order_id: Optional[str]
    kl_side: str
    kl_price: float
    kl_quantity: float
    kl_filled_quantity: float
    kl_status: str

    # Financials
    expected_profit_rate: float
    actual_profit: float
    pm_fee: float
    kl_fee: float
    total_fees: float

    # Risk events
    panic_sell_triggered: bool
    error_message: Optional[str]


@dataclass
class DailyStats:
    """Aggregated daily statistics."""

    date: str
    total_trades: int = 0
    successful_trades: int = 0
    failed_trades: int = 0
    total_profit: float = 0.0
    total_fees: float = 0.0
    net_profit: float = 0.0
    avg_profit_per_trade: float = 0.0
    win_rate: float = 0.0
    panic_sells: int = 0
    m2t_trades: int = 0
    t2t_trades: int = 0


class ResultsRecorder:
    """
    Records and analyzes trading results.

    Features:
    - Logs every trade to JSON and CSV
    - Tracks daily/weekly/monthly statistics
    - Provides performance analytics
    - Supports export for external analysis
    """

    def __init__(
        self,
        data_dir: str = "data",
        json_file: str = "trades.json",
        csv_file: str = "trades.csv",
    ):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(exist_ok=True)

        self.json_path = self.data_dir / json_file
        self.csv_path = self.data_dir / csv_file
        self.stats_path = self.data_dir / "daily_stats.json"

        # In-memory cache
        self._trades: list[TradeRecord] = []
        self._daily_stats: dict[str, DailyStats] = {}
        self._trade_counter = 0

        # Load existing data
        self._load_existing_data()

    def _load_existing_data(self) -> None:
        """Load existing trade records from disk."""
        if self.json_path.exists():
            try:
                with open(self.json_path) as f:
                    data = json.load(f)
                    self._trades = [TradeRecord(**t) for t in data.get("trades", [])]
                    self._trade_counter = len(self._trades)
                logger.info(f"Loaded {len(self._trades)} existing trade records")
            except Exception as e:
                logger.warning(f"Could not load existing trades: {e}")

        if self.stats_path.exists():
            try:
                with open(self.stats_path) as f:
                    data = json.load(f)
                    self._daily_stats = {
                        k: DailyStats(**v) for k, v in data.items()
                    }
            except Exception as e:
                logger.warning(f"Could not load daily stats: {e}")

    def _generate_trade_id(self) -> str:
        """Generate unique trade ID."""
        self._trade_counter += 1
        timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        return f"T{timestamp}-{self._trade_counter:05d}"

    def record_trade(
        self,
        opportunity: ArbitrageOpportunity,
        result: TradeResult,
        category: str = "nba",
    ) -> TradeRecord:
        """
        Record a completed trade.

        Args:
            opportunity: The arbitrage opportunity that was executed
            result: The result of the trade execution
            category: Market category (nba, nfl, etc.)

        Returns:
            The created TradeRecord
        """
        trade_id = self._generate_trade_id()
        timestamp = datetime.utcnow().isoformat()

        # Extract order details
        pm_order = result.pm_order
        kl_order = result.kl_order

        record = TradeRecord(
            trade_id=trade_id,
            timestamp=timestamp,
            event_name=opportunity.contract_pair.event_name,
            category=category,
            outcome=opportunity.contract_pair.outcome,
            mode=opportunity.mode,
            success=result.success,
            # Polymarket
            pm_token_id=opportunity.contract_pair.polymarket_token_id,
            pm_order_id=pm_order.order_id if pm_order else None,
            pm_side=pm_order.side.value if pm_order else "",
            pm_price=opportunity.pm_price,
            pm_quantity=opportunity.suggested_quantity,
            pm_filled_quantity=pm_order.filled_quantity if pm_order else 0,
            pm_status=pm_order.status.value if pm_order else "",
            # Kalshi
            kl_ticker=opportunity.contract_pair.kalshi_ticker,
            kl_order_id=kl_order.order_id if kl_order else None,
            kl_side=kl_order.side.value if kl_order else "",
            kl_price=opportunity.kl_price,
            kl_quantity=opportunity.suggested_quantity,
            kl_filled_quantity=kl_order.filled_quantity if kl_order else 0,
            kl_status=kl_order.status.value if kl_order else "",
            # Financials
            expected_profit_rate=opportunity.net_profit_rate,
            actual_profit=result.net_profit,
            pm_fee=0.0,  # Currently 0 bps
            kl_fee=0.07 * opportunity.suggested_quantity * opportunity.kl_price * (1 - opportunity.kl_price),
            total_fees=0.0,
            # Risk
            panic_sell_triggered=result.requires_panic_sell,
            error_message=result.error_message,
        )

        record.total_fees = record.pm_fee + record.kl_fee

        # Store record
        self._trades.append(record)

        # Update daily stats
        self._update_daily_stats(record)

        # Persist to disk
        self._save_trade(record)

        logger.info(
            f"Recorded trade {trade_id}: "
            f"{'SUCCESS' if result.success else 'FAILED'} "
            f"profit=${result.net_profit:.2f}"
        )

        return record

    def _update_daily_stats(self, record: TradeRecord) -> None:
        """Update daily statistics with new trade."""
        date = record.timestamp[:10]  # YYYY-MM-DD

        if date not in self._daily_stats:
            self._daily_stats[date] = DailyStats(date=date)

        stats = self._daily_stats[date]
        stats.total_trades += 1

        if record.success:
            stats.successful_trades += 1
            stats.total_profit += record.actual_profit
        else:
            stats.failed_trades += 1

        stats.total_fees += record.total_fees
        stats.net_profit = stats.total_profit - stats.total_fees

        if stats.total_trades > 0:
            stats.avg_profit_per_trade = stats.total_profit / stats.total_trades
            stats.win_rate = stats.successful_trades / stats.total_trades

        if record.panic_sell_triggered:
            stats.panic_sells += 1

        if record.mode == "M2T":
            stats.m2t_trades += 1
        else:
            stats.t2t_trades += 1

        # Save stats
        self._save_stats()

    def _save_trade(self, record: TradeRecord) -> None:
        """Save trade record to disk."""
        # Save to JSON
        try:
            data = {"trades": [asdict(t) for t in self._trades]}
            with open(self.json_path, "w") as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save trades JSON: {e}")

        # Append to CSV
        try:
            file_exists = self.csv_path.exists()
            with open(self.csv_path, "a", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=asdict(record).keys())
                if not file_exists:
                    writer.writeheader()
                writer.writerow(asdict(record))
        except Exception as e:
            logger.error(f"Failed to save trades CSV: {e}")

    def _save_stats(self) -> None:
        """Save daily stats to disk."""
        try:
            data = {k: asdict(v) for k, v in self._daily_stats.items()}
            with open(self.stats_path, "w") as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save stats: {e}")

    # Analytics Methods

    def get_total_stats(self) -> dict:
        """Get all-time statistics."""
        total_trades = len(self._trades)
        successful = sum(1 for t in self._trades if t.success)
        failed = total_trades - successful
        total_profit = sum(t.actual_profit for t in self._trades)
        total_fees = sum(t.total_fees for t in self._trades)

        return {
            "total_trades": total_trades,
            "successful_trades": successful,
            "failed_trades": failed,
            "win_rate": successful / total_trades if total_trades > 0 else 0,
            "total_profit": total_profit,
            "total_fees": total_fees,
            "net_profit": total_profit - total_fees,
            "avg_profit_per_trade": total_profit / total_trades if total_trades > 0 else 0,
            "panic_sells": sum(1 for t in self._trades if t.panic_sell_triggered),
            "m2t_trades": sum(1 for t in self._trades if t.mode == "M2T"),
            "t2t_trades": sum(1 for t in self._trades if t.mode == "T2T"),
        }

    def get_daily_stats(self, date: Optional[str] = None) -> Optional[DailyStats]:
        """Get stats for a specific date (default: today)."""
        if date is None:
            date = datetime.utcnow().strftime("%Y-%m-%d")
        return self._daily_stats.get(date)

    def get_recent_trades(self, limit: int = 10) -> list[TradeRecord]:
        """Get most recent trades."""
        return self._trades[-limit:]

    def get_trades_by_category(self, category: str) -> list[TradeRecord]:
        """Get all trades for a specific category."""
        return [t for t in self._trades if t.category == category]

    def get_trades_by_event(self, event_name: str) -> list[TradeRecord]:
        """Get all trades for a specific event."""
        return [t for t in self._trades if t.event_name == event_name]

    def get_failed_trades(self) -> list[TradeRecord]:
        """Get all failed trades for analysis."""
        return [t for t in self._trades if not t.success]

    def get_profit_by_mode(self) -> dict[str, float]:
        """Get total profit broken down by execution mode."""
        m2t_profit = sum(t.actual_profit for t in self._trades if t.mode == "M2T")
        t2t_profit = sum(t.actual_profit for t in self._trades if t.mode == "T2T")
        return {"M2T": m2t_profit, "T2T": t2t_profit}

    def generate_report(self) -> str:
        """Generate a text summary report."""
        stats = self.get_total_stats()
        today_stats = self.get_daily_stats()

        lines = [
            "=" * 60,
            "ARBITRAGE BOT PERFORMANCE REPORT",
            "=" * 60,
            "",
            "ALL-TIME STATISTICS:",
            f"  Total Trades:     {stats['total_trades']}",
            f"  Successful:       {stats['successful_trades']}",
            f"  Failed:           {stats['failed_trades']}",
            f"  Win Rate:         {stats['win_rate']:.1%}",
            f"  Total Profit:     ${stats['total_profit']:.2f}",
            f"  Total Fees:       ${stats['total_fees']:.2f}",
            f"  Net Profit:       ${stats['net_profit']:.2f}",
            f"  Avg per Trade:    ${stats['avg_profit_per_trade']:.2f}",
            f"  Panic Sells:      {stats['panic_sells']}",
            "",
            "BY EXECUTION MODE:",
            f"  M2T Trades:       {stats['m2t_trades']}",
            f"  T2T Trades:       {stats['t2t_trades']}",
        ]

        profit_by_mode = self.get_profit_by_mode()
        lines.extend([
            f"  M2T Profit:       ${profit_by_mode['M2T']:.2f}",
            f"  T2T Profit:       ${profit_by_mode['T2T']:.2f}",
        ])

        if today_stats:
            lines.extend([
                "",
                f"TODAY ({today_stats.date}):",
                f"  Trades:           {today_stats.total_trades}",
                f"  Successful:       {today_stats.successful_trades}",
                f"  Profit:           ${today_stats.total_profit:.2f}",
            ])

        lines.append("=" * 60)

        return "\n".join(lines)

    def export_to_csv(self, filepath: str) -> bool:
        """Export all trades to a custom CSV file."""
        try:
            with open(filepath, "w", newline="") as f:
                if self._trades:
                    writer = csv.DictWriter(f, fieldnames=asdict(self._trades[0]).keys())
                    writer.writeheader()
                    for trade in self._trades:
                        writer.writerow(asdict(trade))
            logger.info(f"Exported {len(self._trades)} trades to {filepath}")
            return True
        except Exception as e:
            logger.error(f"Export failed: {e}")
            return False
