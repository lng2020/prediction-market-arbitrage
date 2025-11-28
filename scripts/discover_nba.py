#!/usr/bin/env python3
"""
NBA Market Discovery Tool

Discovers and matches NBA markets between Polymarket and Kalshi.

Usage:
    python scripts/discover_nba.py [--save] [--date YYYY-MM-DD]

This tool:
1. Fetches all open NBA markets from both platforms
2. Attempts to match games/events automatically
3. Outputs potential arbitrage pairs for manual verification
4. Optionally saves matches to contracts.json
"""

import argparse
import asyncio
import json
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.clients import KalshiClient, PolymarketClient
from src.config import MarketCategory, load_config
from src.models import ContractPair


# NBA team name variations for matching
NBA_TEAMS = {
    "ATL": ["hawks", "atlanta"],
    "BOS": ["celtics", "boston"],
    "BKN": ["nets", "brooklyn"],
    "CHA": ["hornets", "charlotte"],
    "CHI": ["bulls", "chicago"],
    "CLE": ["cavaliers", "cavs", "cleveland"],
    "DAL": ["mavericks", "mavs", "dallas"],
    "DEN": ["nuggets", "denver"],
    "DET": ["pistons", "detroit"],
    "GSW": ["warriors", "golden state", "gs"],
    "HOU": ["rockets", "houston"],
    "IND": ["pacers", "indiana"],
    "LAC": ["clippers", "la clippers"],
    "LAL": ["lakers", "la lakers", "los angeles lakers"],
    "MEM": ["grizzlies", "memphis"],
    "MIA": ["heat", "miami"],
    "MIL": ["bucks", "milwaukee"],
    "MIN": ["timberwolves", "wolves", "minnesota"],
    "NOP": ["pelicans", "new orleans"],
    "NYK": ["knicks", "new york"],
    "OKC": ["thunder", "oklahoma city", "okc"],
    "ORL": ["magic", "orlando"],
    "PHI": ["76ers", "sixers", "philadelphia"],
    "PHX": ["suns", "phoenix"],
    "POR": ["blazers", "trail blazers", "portland"],
    "SAC": ["kings", "sacramento"],
    "SAS": ["spurs", "san antonio"],
    "TOR": ["raptors", "toronto"],
    "UTA": ["jazz", "utah"],
    "WAS": ["wizards", "washington"],
}


def extract_teams_from_text(text: str) -> list[str]:
    """Extract NBA team abbreviations from text."""
    text_lower = text.lower()
    found_teams = []

    for abbrev, names in NBA_TEAMS.items():
        for name in names:
            if name in text_lower:
                found_teams.append(abbrev)
                break

    return found_teams


def extract_date_from_text(text: str) -> Optional[datetime]:
    """Try to extract a date from market text."""
    # Common patterns: "Dec 15", "12/15", "December 15, 2024"
    patterns = [
        r"(\d{1,2})/(\d{1,2})/(\d{4})",
        r"(\d{1,2})/(\d{1,2})",
        r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\w*\s+(\d{1,2})",
    ]

    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            try:
                # Simplified date parsing - would need more robust handling
                return datetime.now()  # Placeholder
            except Exception:
                pass

    return None


class NBAMarketDiscovery:
    """Discovers and matches NBA markets across platforms."""

    def __init__(self, kalshi_client: KalshiClient, polymarket_client: PolymarketClient):
        self.kalshi = kalshi_client
        self.polymarket = polymarket_client

    async def fetch_kalshi_nba_markets(self) -> list[dict]:
        """Fetch all open NBA markets from Kalshi."""
        all_markets = []

        # Try different series tickers for NBA
        for series in MarketCategory.KALSHI_SERIES[MarketCategory.NBA]:
            try:
                markets = await self.kalshi.get_markets(series_ticker=series, status="open")
                all_markets.extend(markets)
            except Exception as e:
                print(f"  Warning: Could not fetch series {series}: {e}")

        # Also do a general search
        try:
            markets = await self.kalshi.get_markets(status="open", limit=200)
            for m in markets:
                title = m.get("title", "").lower()
                if "nba" in title or any(
                    team in title for teams in NBA_TEAMS.values() for team in teams
                ):
                    if m not in all_markets:
                        all_markets.append(m)
        except Exception as e:
            print(f"  Warning: General search failed: {e}")

        return all_markets

    async def fetch_polymarket_nba_markets(self) -> list[dict]:
        """Fetch all NBA markets from Polymarket."""
        all_markets = []

        try:
            markets = await self.polymarket.get_markets()
            for m in markets:
                question = m.get("question", "").lower()
                tags = [t.lower() for t in m.get("tags", [])]

                # Check if NBA-related
                is_nba = (
                    "nba" in question
                    or "nba" in tags
                    or "basketball" in tags
                    or any(
                        team in question
                        for teams in NBA_TEAMS.values()
                        for team in teams
                    )
                )

                if is_nba:
                    all_markets.append(m)

        except Exception as e:
            print(f"  Warning: Polymarket fetch failed: {e}")

        return all_markets

    def match_markets(
        self,
        kalshi_markets: list[dict],
        polymarket_markets: list[dict],
    ) -> list[dict]:
        """
        Match markets between platforms.

        Returns list of potential matches for verification.
        """
        matches = []

        for kl in kalshi_markets:
            kl_title = kl.get("title", "")
            kl_teams = extract_teams_from_text(kl_title)

            if len(kl_teams) < 2:
                continue  # Need at least 2 teams for a game

            for pm in polymarket_markets:
                pm_question = pm.get("question", "")
                pm_teams = extract_teams_from_text(pm_question)

                # Check if same teams
                if set(kl_teams) == set(pm_teams) and len(kl_teams) >= 2:
                    # Get token info
                    tokens = pm.get("tokens", [])
                    yes_token = None
                    no_token = None

                    for t in tokens:
                        if t.get("outcome", "").upper() == "YES":
                            yes_token = t.get("token_id")
                        elif t.get("outcome", "").upper() == "NO":
                            no_token = t.get("token_id")

                    matches.append({
                        "teams": kl_teams,
                        "kalshi": {
                            "ticker": kl.get("ticker"),
                            "title": kl_title,
                            "yes_bid": kl.get("yes_bid"),
                            "yes_ask": kl.get("yes_ask"),
                        },
                        "polymarket": {
                            "condition_id": pm.get("condition_id"),
                            "question": pm_question,
                            "yes_token": yes_token,
                            "no_token": no_token,
                        },
                        "confidence": "high" if len(kl_teams) == 2 else "medium",
                    })

        return matches

    def create_contract_pairs(self, matches: list[dict]) -> list[ContractPair]:
        """Create ContractPair objects from verified matches."""
        pairs = []

        for match in matches:
            if match.get("confidence") != "high":
                continue

            teams = match["teams"]
            kl = match["kalshi"]
            pm = match["polymarket"]

            if not pm.get("yes_token") or not kl.get("ticker"):
                continue

            # Create pair for YES outcome
            pair = ContractPair(
                event_name=f"NBA: {teams[0]} vs {teams[1]}",
                polymarket_token_id=pm["yes_token"],
                kalshi_ticker=kl["ticker"],
                outcome="YES",
                active=True,
            )
            pairs.append(pair)

        return pairs


async def main(args: argparse.Namespace) -> None:
    """Main discovery routine."""
    print("\n🏀 NBA Market Discovery Tool")
    print("=" * 60)

    config = load_config()

    # Initialize clients
    print("\n📡 Initializing API clients...")
    kalshi = KalshiClient(config.kalshi)
    polymarket = PolymarketClient(config.polymarket)

    try:
        await kalshi.initialize()
        await polymarket.initialize()
        print("  ✓ Clients initialized")
    except Exception as e:
        print(f"  ✗ Failed to initialize: {e}")
        print("\n  Make sure your API credentials are configured in .env")
        return

    discovery = NBAMarketDiscovery(kalshi, polymarket)

    # Fetch markets
    print("\n📊 Fetching NBA markets...")

    print("  Kalshi...")
    kl_markets = await discovery.fetch_kalshi_nba_markets()
    print(f"  ✓ Found {len(kl_markets)} Kalshi NBA markets")

    print("  Polymarket...")
    pm_markets = await discovery.fetch_polymarket_nba_markets()
    print(f"  ✓ Found {len(pm_markets)} Polymarket NBA markets")

    # Display Kalshi markets
    print("\n" + "=" * 60)
    print("KALSHI NBA MARKETS:")
    print("-" * 60)
    for m in kl_markets[:15]:
        ticker = m.get("ticker", "N/A")
        title = m.get("title", "N/A")[:50]
        yes_bid = m.get("yes_bid", "N/A")
        yes_ask = m.get("yes_ask", "N/A")
        print(f"  {ticker:<20} | Bid: {yes_bid} Ask: {yes_ask}")
        print(f"    {title}")

    # Display Polymarket markets
    print("\n" + "=" * 60)
    print("POLYMARKET NBA MARKETS:")
    print("-" * 60)
    for m in pm_markets[:15]:
        question = m.get("question", "N/A")[:60]
        cid = m.get("condition_id", "N/A")[:20]
        tokens = m.get("tokens", [])
        print(f"  {cid}...")
        print(f"    Q: {question}...")
        if tokens:
            for t in tokens[:2]:
                print(f"    Token ({t.get('outcome')}): {t.get('token_id', 'N/A')[:30]}...")

    # Match markets
    print("\n" + "=" * 60)
    print("🎯 MATCHING MARKETS...")
    print("-" * 60)

    matches = discovery.match_markets(kl_markets, pm_markets)

    if matches:
        print(f"\n  Found {len(matches)} potential matches:\n")
        for i, match in enumerate(matches, 1):
            teams = " vs ".join(match["teams"])
            conf = match["confidence"]
            print(f"  Match #{i} [{conf}]: {teams}")
            print(f"    KL: {match['kalshi']['ticker']} - {match['kalshi']['title'][:40]}...")
            print(f"    PM: {match['polymarket']['question'][:50]}...")
            print(f"    PM YES Token: {match['polymarket'].get('yes_token', 'N/A')[:30]}...")
            print()
    else:
        print("\n  No automatic matches found.")
        print("  This could mean:")
        print("    - No overlapping NBA games currently")
        print("    - Markets use different naming conventions")
        print("    - Manual matching required")

    # Save if requested
    if args.save and matches:
        pairs = discovery.create_contract_pairs(matches)
        if pairs:
            contracts_file = Path("contracts.json")

            # Load existing
            existing = []
            if contracts_file.exists():
                with open(contracts_file) as f:
                    existing = json.load(f)

            # Add new pairs
            for pair in pairs:
                entry = {
                    "event_name": pair.event_name,
                    "polymarket_token_id": pair.polymarket_token_id,
                    "kalshi_ticker": pair.kalshi_ticker,
                    "outcome": pair.outcome,
                    "active": pair.active,
                    "category": "nba",
                }
                # Avoid duplicates
                if not any(
                    e.get("kalshi_ticker") == pair.kalshi_ticker
                    and e.get("polymarket_token_id") == pair.polymarket_token_id
                    for e in existing
                ):
                    existing.append(entry)

            with open(contracts_file, "w") as f:
                json.dump(existing, f, indent=2)

            print(f"\n  ✓ Saved {len(pairs)} pairs to contracts.json")

    # Cleanup
    await kalshi.close()
    await polymarket.close()

    print("\n" + "=" * 60)
    print("Done! Review matches above and add to contracts.json manually if needed.")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Discover NBA markets for arbitrage")
    parser.add_argument(
        "--save",
        action="store_true",
        help="Save high-confidence matches to contracts.json",
    )
    parser.add_argument(
        "--date",
        type=str,
        help="Filter by date (YYYY-MM-DD)",
    )

    args = parser.parse_args()
    asyncio.run(main(args))
