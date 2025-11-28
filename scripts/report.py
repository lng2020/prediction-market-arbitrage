#!/usr/bin/env python3
"""
Performance Report Tool

View trading performance reports and analytics.

Usage:
    python scripts/report.py [--export FILE] [--daily DATE] [--failed]
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.modules.results_recorder import ResultsRecorder


def main() -> None:
    parser = argparse.ArgumentParser(description="View trading performance reports")
    parser.add_argument(
        "--export",
        type=str,
        help="Export all trades to CSV file",
    )
    parser.add_argument(
        "--daily",
        type=str,
        help="Show stats for specific date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--failed",
        action="store_true",
        help="Show failed trades only",
    )
    parser.add_argument(
        "--recent",
        type=int,
        default=0,
        help="Show N most recent trades",
    )

    args = parser.parse_args()

    recorder = ResultsRecorder()

    if args.export:
        if recorder.export_to_csv(args.export):
            print(f"✓ Exported to {args.export}")
        else:
            print("✗ Export failed")
        return

    if args.daily:
        stats = recorder.get_daily_stats(args.daily)
        if stats:
            print(f"\n📅 Stats for {args.daily}:")
            print(f"  Trades:      {stats.total_trades}")
            print(f"  Successful:  {stats.successful_trades}")
            print(f"  Failed:      {stats.failed_trades}")
            print(f"  Win Rate:    {stats.win_rate:.1%}")
            print(f"  Profit:      ${stats.total_profit:.2f}")
            print(f"  Fees:        ${stats.total_fees:.2f}")
            print(f"  Net:         ${stats.net_profit:.2f}")
            print(f"  M2T/T2T:     {stats.m2t_trades}/{stats.t2t_trades}")
            print(f"  Panic Sells: {stats.panic_sells}")
        else:
            print(f"No data for {args.daily}")
        return

    if args.failed:
        failed = recorder.get_failed_trades()
        print(f"\n❌ Failed Trades ({len(failed)} total):\n")
        for t in failed:
            print(f"  {t.trade_id} | {t.timestamp[:16]}")
            print(f"    Event: {t.event_name}")
            print(f"    Mode:  {t.mode}")
            print(f"    Error: {t.error_message or 'Unknown'}")
            print(f"    Panic: {'Yes' if t.panic_sell_triggered else 'No'}")
            print()
        return

    if args.recent > 0:
        recent = recorder.get_recent_trades(args.recent)
        print(f"\n📋 Recent Trades ({len(recent)}):\n")
        for t in recent:
            status = "✓" if t.success else "✗"
            print(f"  {status} {t.trade_id} | {t.timestamp[:16]}")
            print(f"    {t.event_name} ({t.mode})")
            print(f"    Profit: ${t.actual_profit:.2f}")
            print()
        return

    # Default: show full report
    print(recorder.generate_report())

    # Also show profit by mode
    profit_by_mode = recorder.get_profit_by_mode()
    print("\nPROFIT BY MODE:")
    print(f"  M2T: ${profit_by_mode['M2T']:.2f}")
    print(f"  T2T: ${profit_by_mode['T2T']:.2f}")


if __name__ == "__main__":
    main()
