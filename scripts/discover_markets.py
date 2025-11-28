#!/usr/bin/env python3
"""
Market Discovery Tool

Helps find matching events between Polymarket and Kalshi.

Usage:
    python scripts/discover_markets.py "search term"
"""

import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.clients import KalshiClient, PolymarketClient
from src.config import load_config
from src.mappings import ContractMapper


async def discover_markets(search_term: str) -> None:
    """Search for matching markets across platforms."""
    config = load_config()

    # Initialize clients
    kalshi = KalshiClient(config.kalshi)
    polymarket = PolymarketClient(config.polymarket)

    await kalshi.initialize()
    await polymarket.initialize()

    # Create mapper
    mapper = ContractMapper(kalshi, polymarket)

    print(f"\nSearching for: {search_term}\n")
    print("=" * 60)

    # Search Kalshi
    print("\n📊 KALSHI MARKETS:")
    print("-" * 40)
    kl_markets = await mapper.search_kalshi_markets(search_term)
    for m in kl_markets[:10]:
        print(f"  Ticker: {m.get('ticker')}")
        print(f"  Title:  {m.get('title')}")
        print(f"  Bid/Ask: {m.get('yes_bid', 'N/A')}/{m.get('yes_ask', 'N/A')}")
        print()

    # Search Polymarket
    print("\n🔮 POLYMARKET MARKETS:")
    print("-" * 40)
    pm_markets = await mapper.search_polymarket_markets(search_term)
    for m in pm_markets[:10]:
        print(f"  Condition: {m.get('condition_id', 'N/A')[:20]}...")
        print(f"  Question: {m.get('question', 'N/A')[:60]}...")
        tokens = m.get("tokens", [])
        if tokens:
            for t in tokens[:2]:
                print(f"    Token ({t.get('outcome')}): {t.get('token_id', 'N/A')[:20]}...")
        print()

    # Find potential matches
    print("\n🎯 POTENTIAL MATCHES:")
    print("-" * 40)
    matches = await mapper.find_matching_events(search_term)
    if matches:
        for i, match in enumerate(matches[:5], 1):
            print(f"\nMatch #{i}:")
            print(f"  Kalshi: {match['kalshi']['ticker']} - {match['kalshi']['title'][:50]}...")
            print(f"  Polymarket: {match['polymarket']['question'][:50]}...")
            print(f"  Common words: {match['common_words']}")
    else:
        print("  No automatic matches found. Please verify manually.")

    # Cleanup
    await kalshi.close()
    await polymarket.close()


def main():
    if len(sys.argv) < 2:
        print("Usage: python discover_markets.py 'search term'")
        print("Example: python discover_markets.py 'fed rate'")
        sys.exit(1)

    search_term = " ".join(sys.argv[1:])
    asyncio.run(discover_markets(search_term))


if __name__ == "__main__":
    main()
