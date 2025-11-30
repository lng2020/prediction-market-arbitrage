#!/usr/bin/env python3
"""
NBA Game Market Discovery Tool

Discovers and matches today's NBA game markets between Polymarket and Kalshi.

Usage:
    python scripts/discover_nba.py [--save] [--date YYYY-MM-DD]
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


# NBA team abbreviations and name variations
NBA_TEAMS = {
    "ATL": ["hawks", "atlanta", "atl"],
    "BOS": ["celtics", "boston", "bos"],
    "BKN": ["nets", "brooklyn", "bkn"],
    "CHA": ["hornets", "charlotte", "cha"],
    "CHI": ["bulls", "chicago", "chi"],
    "CLE": ["cavaliers", "cavs", "cleveland", "cle"],
    "DAL": ["mavericks", "mavs", "dallas", "dal"],
    "DEN": ["nuggets", "denver", "den"],
    "DET": ["pistons", "detroit", "det"],
    "GSW": ["warriors", "golden state", "gsw", "gs"],
    "HOU": ["rockets", "houston", "hou"],
    "IND": ["pacers", "indiana", "ind"],
    "LAC": ["clippers", "la clippers", "lac"],
    "LAL": ["lakers", "la lakers", "los angeles lakers", "lal"],
    "MEM": ["grizzlies", "memphis", "mem"],
    "MIA": ["heat", "miami", "mia"],
    "MIL": ["bucks", "milwaukee", "mil"],
    "MIN": ["timberwolves", "wolves", "minnesota", "min"],
    "NOP": ["pelicans", "new orleans", "nop", "no"],
    "NYK": ["knicks", "new york", "nyk", "ny"],
    "OKC": ["thunder", "oklahoma city", "okc"],
    "ORL": ["magic", "orlando", "orl"],
    "PHI": ["76ers", "sixers", "philadelphia", "phi"],
    "PHX": ["suns", "phoenix", "phx"],
    "POR": ["blazers", "trail blazers", "portland", "por"],
    "SAC": ["kings", "sacramento", "sac"],
    "SAS": ["spurs", "san antonio", "sas", "sa"],
    "TOR": ["raptors", "toronto", "tor"],
    "UTA": ["jazz", "utah", "uta"],
    "WAS": ["wizards", "washington", "was"],
}

# Reverse lookup: name -> abbreviation
NAME_TO_ABBREV = {}
for abbrev, names in NBA_TEAMS.items():
    for name in names:
        NAME_TO_ABBREV[name.lower()] = abbrev


def extract_teams_from_text(text: str) -> list[str]:
    """Extract NBA team abbreviations from text."""
    text_lower = text.lower()
    found_teams = set()

    for abbrev, names in NBA_TEAMS.items():
        for name in names:
            if name.lower() in text_lower:
                found_teams.add(abbrev)
                break

    return list(found_teams)


def extract_teams_from_slug(slug: str) -> list[str]:
    """Extract teams from a slug like 'nba-orl-det-2025-11-28'."""
    parts = slug.lower().split("-")
    teams = []

    for part in parts:
        if part in NAME_TO_ABBREV:
            teams.append(NAME_TO_ABBREV[part])
        elif part.upper() in NBA_TEAMS:
            teams.append(part.upper())

    return teams


def normalize_team_set(teams: list[str]) -> frozenset:
    """Normalize team list to a frozenset for comparison."""
    return frozenset(t.upper() for t in teams)


async def fetch_kalshi_nba_games(kalshi: KalshiClient, date: str) -> list[dict]:
    """
    Fetch today's NBA game markets from Kalshi.

    Kalshi game tickers look like: KXNBAGAME-25NOV28CHICHA
    """
    games = []
    all_nba_games = []

    # Format date for Kalshi ticker - try multiple formats
    # e.g., 2025-11-28 -> 25NOV28
    date_patterns = []
    try:
        dt = datetime.strptime(date, "%Y-%m-%d")
        date_patterns.append(dt.strftime("%y%b%d").upper())  # 25NOV28
        date_patterns.append(dt.strftime("%d%b").upper())     # 28NOV
        date_patterns.append(dt.strftime("%y%m%d"))           # 251128
    except ValueError:
        pass

    try:
        # Fetch NBA game markets specifically using series_ticker
        # KXNBAGAME is the series for NBA game winner markets
        markets = await kalshi.get_markets(series_ticker="KXNBAGAME", status="open", limit=500)

        # If series_ticker returns nothing, fallback to general search
        if not markets:
            print("    (series_ticker filter returned no results, trying general search...)")
            markets = await kalshi.get_markets(status="open", limit=1000)

        for m in markets:
            ticker = m.get("ticker", "").upper()
            title = m.get("title", "").lower()

            # Check if it's an NBA game market
            is_nba_game = (
                "KXNBAGAME" in ticker or
                "NBAGAME" in ticker or
                "professional basketball" in title or
                ("nba" in title and "game" in title)
            )

            if is_nba_game:
                all_nba_games.append(m)

                # Check if date matches any pattern
                date_match = False
                for pattern in date_patterns:
                    if pattern in ticker:
                        date_match = True
                        break

                if date_match:
                    games.append(m)

        # If no date-specific games found, show what we have
        if not games and all_nba_games:
            print(f"    (Found {len(all_nba_games)} total NBA game markets, but none for {date})")
            print(f"    Sample tickers: {[m.get('ticker') for m in all_nba_games[:3]]}")

    except Exception as e:
        print(f"  Warning: Kalshi fetch error: {e}")
        import traceback
        traceback.print_exc()

    return games


async def fetch_polymarket_nba_games(polymarket: PolymarketClient, date: str) -> list[dict]:
    """
    Fetch today's NBA game events from Polymarket.

    Polymarket slugs look like: nba-orl-det-2025-11-28
    """
    games = []

    try:
        # Try fast event_date filter first
        events = await polymarket.search_nba_games(date=date)
        games.extend(events)
    except Exception as e:
        print(f"  Warning: Polymarket fetch error: {e}")

    return games


async def fetch_polymarket_game_by_teams(
    polymarket: PolymarketClient,
    teams: list[str],
    date: str
) -> Optional[dict]:
    """
    Fetch a specific Polymarket game by trying different team order combinations.

    Args:
        polymarket: Polymarket client
        teams: List of two team abbreviations
        date: Date in YYYY-MM-DD format

    Returns:
        Event dict or None
    """
    if len(teams) != 2:
        return None

    # Try both team orderings (away-home and home-away)
    team_a, team_b = teams[0].lower(), teams[1].lower()

    # Map Kalshi team abbreviations to Polymarket abbreviations
    # Some teams have different abbreviations
    abbrev_map = {
        "gs": "gsw",
        "no": "nop",
        "ny": "nyk",
        "sa": "sas",
        "phx": "pho",  # Phoenix
    }

    team_a = abbrev_map.get(team_a, team_a)
    team_b = abbrev_map.get(team_b, team_b)

    for away, home in [(team_a, team_b), (team_b, team_a)]:
        event = await polymarket.get_nba_game_by_teams(away, home, date)
        if event:
            return event

    return None


def build_pm_lookup(polymarket_games: list[dict]) -> dict:
    """Build a lookup dict of Polymarket games by team set."""
    pm_by_teams = {}
    for pm in polymarket_games:
        slug = pm.get("slug", "")
        teams = extract_teams_from_slug(slug)
        if len(teams) >= 2:
            key = normalize_team_set(teams)
            pm_by_teams[key] = pm
    return pm_by_teams


def extract_pm_tokens(pm: dict, game_winner_only: bool = False) -> list[dict]:
    """
    Extract token IDs from a Polymarket event.

    Args:
        pm: Polymarket event dict
        game_winner_only: If True, only extract tokens from the first game winner market
                         (excludes over/under, spread, and prop markets)
    """
    markets = pm.get("markets", [])
    tokens = []
    game_winner_found = False

    for market in markets:
        clob_token_ids = market.get("clobTokenIds", [])
        outcomes = market.get("outcomes", [])

        # Parse JSON strings if needed (Gamma API sometimes returns these as strings)
        if isinstance(clob_token_ids, str):
            try:
                clob_token_ids = json.loads(clob_token_ids)
            except (json.JSONDecodeError, ValueError):
                clob_token_ids = []

        if isinstance(outcomes, str):
            try:
                outcomes = json.loads(outcomes)
            except (json.JSONDecodeError, ValueError):
                outcomes = []

        # Skip non-game-winner markets if requested
        if game_winner_only and outcomes:
            # Over/Under and spread markets have specific outcome patterns
            outcome_lower = [o.lower() for o in outcomes]
            is_over_under = any("over" in o or "under" in o for o in outcome_lower)
            is_spread = any("+" in o or "-" in o for o in outcomes)
            if is_over_under or is_spread:
                continue

            # Only take the first game winner market
            if game_winner_found:
                continue
            game_winner_found = True

        if clob_token_ids and outcomes:
            for i, outcome in enumerate(outcomes):
                if i < len(clob_token_ids):
                    tokens.append({
                        "outcome": outcome,
                        "token_id": clob_token_ids[i],
                    })
    return tokens


async def match_games_smart(
    kalshi_games: list[dict],
    polymarket_games: list[dict],
    polymarket: PolymarketClient,
    date: str
) -> list[dict]:
    """
    Match games between platforms.
    Uses direct slug lookups for games not found in initial search.
    """
    matches = []

    # Build Polymarket lookup from search results
    pm_by_teams = build_pm_lookup(polymarket_games)

    # Match Kalshi games
    for kl in kalshi_games:
        title = kl.get("title", "")
        ticker = kl.get("ticker", "")
        teams = extract_teams_from_text(title)

        if len(teams) < 2:
            continue

        key = normalize_team_set(teams)

        # Try to find in existing Polymarket games
        pm = pm_by_teams.get(key)

        # If not found, try direct slug lookup (fast)
        if not pm:
            print(f"    Direct lookup for {teams}...")
            pm = await fetch_polymarket_game_by_teams(polymarket, teams, date)
            if pm:
                pm_by_teams[key] = pm  # Cache for future

        if pm:
            markets = pm.get("markets", [])
            # Only extract game winner tokens (not over/under or spreads)
            tokens = extract_pm_tokens(pm, game_winner_only=True)

            matches.append({
                "teams": list(key),
                "kalshi": {
                    "ticker": ticker,
                    "title": title,
                    "yes_bid": kl.get("yes_bid"),
                    "yes_ask": kl.get("yes_ask"),
                },
                "polymarket": {
                    "slug": pm.get("slug"),
                    "title": pm.get("title"),
                    "condition_id": pm.get("condition_id"),
                    "markets": markets,
                    "tokens": tokens,
                },
                })

    return matches


async def main(args: argparse.Namespace) -> None:
    """Main discovery routine."""
    # Determine date
    if args.date:
        target_date = args.date
    else:
        target_date = datetime.now().strftime("%Y-%m-%d")

    print(f"\n🏀 NBA Game Discovery - {target_date}")
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
        print(f"  ⚠ Partial init (trading may require credentials): {e}")

    # Fetch games
    print(f"\n📊 Fetching NBA games for {target_date}...")

    print("  Kalshi...")
    kl_games = await fetch_kalshi_nba_games(kalshi, target_date)
    print(f"  ✓ Found {len(kl_games)} Kalshi NBA game markets")

    print("  Polymarket...")
    pm_games = await fetch_polymarket_nba_games(polymarket, target_date)
    print(f"  ✓ Found {len(pm_games)} Polymarket NBA game events")

    # Display Kalshi games
    print("\n" + "=" * 60)
    print(f"KALSHI NBA GAMES ({target_date}):")
    print("-" * 60)
    if kl_games:
        for m in kl_games[:20]:
            ticker = m.get("ticker", "N/A")
            title = m.get("title", "N/A")[:50]
            yes_bid = m.get("yes_bid", "N/A")
            yes_ask = m.get("yes_ask", "N/A")
            teams = extract_teams_from_text(title)
            print(f"  {ticker}")
            print(f"    {title}...")
            print(f"    Teams: {teams} | Bid: {yes_bid}¢ Ask: {yes_ask}¢")
            print()
    else:
        print("  No games found for this date")

    # Display Polymarket games
    print("\n" + "=" * 60)
    print(f"POLYMARKET NBA GAMES ({target_date}):")
    print("-" * 60)
    if pm_games:
        for event in pm_games[:20]:
            slug = event.get("slug", "N/A")
            title = event.get("title", "N/A")[:50]
            teams = extract_teams_from_slug(slug)
            markets = event.get("markets", [])

            print(f"  Slug: {slug}")
            print(f"    {title}...")
            print(f"    Teams: {teams}")

            for market in markets[:2]:
                outcomes = market.get("outcomes", [])
                prices = market.get("outcomePrices", [])
                token_ids = market.get("clobTokenIds", [])

                # Handle JSON strings - Gamma API sometimes returns these as strings
                if isinstance(outcomes, str):
                    try:
                        outcomes = json.loads(outcomes)
                    except (json.JSONDecodeError, ValueError):
                        outcomes = []

                if isinstance(prices, str):
                    try:
                        prices = json.loads(prices)
                    except (json.JSONDecodeError, ValueError):
                        prices = []

                if isinstance(token_ids, str):
                    try:
                        token_ids = json.loads(token_ids)
                    except (json.JSONDecodeError, ValueError):
                        token_ids = []

                if outcomes:
                    for i, outcome in enumerate(outcomes):
                        token = token_ids[i] if i < len(token_ids) else "N/A"
                        price = prices[i] if i < len(prices) else 0
                        try:
                            price_val = float(price) * 100
                            print(f"      {outcome}: {price_val:.1f}¢ | Token: {token[:30]}...")
                        except (ValueError, TypeError):
                            print(f"      {outcome}: N/A | Token: {token[:30]}...")
            print()
    else:
        print("  No games found for this date")

    # Match games
    print("\n" + "=" * 60)
    print("🎯 MATCHING GAMES...")
    print("-" * 60)

    # Use smart matching that does direct slug lookups for missing games
    matches = await match_games_smart(kl_games, pm_games, polymarket, target_date)

    if matches:
        print(f"\n  ✓ Found {len(matches)} matching games:\n")
        for i, match in enumerate(matches, 1):
            teams = " vs ".join(match["teams"])
            kl = match["kalshi"]
            pm = match["polymarket"]

            print(f"  Match #{i}: {teams}")
            print(f"    Kalshi:     {kl['ticker']}")
            print(f"                Bid: {kl['yes_bid']}¢ Ask: {kl['yes_ask']}¢")
            print(f"    Polymarket: {pm['slug']}")

            for token in pm.get("tokens", [])[:2]:
                print(f"                {token['outcome']}: {token['token_id'][:30]}...")
            print()
    else:
        print("\n  No matching games found.")
        print("  Possible reasons:")
        print("    - No overlapping games on this date")
        print("    - Different team naming conventions")
        print("    - Markets not yet created on one platform")

    # Save if requested
    if args.save and matches:
        contracts_file = Path("contracts.json")

        # Override by default (start fresh)
        existing = []

        # Add new matches
        added = 0
        for match in matches:
            teams = match["teams"]
            kl = match["kalshi"]
            pm = match["polymarket"]

            # Get Polymarket tokens
            tokens = pm.get("tokens", [])
            if not tokens:
                continue

            # Extract Kalshi ticker suffix (team abbreviation like -SAS, -DEN)
            kalshi_ticker = kl["ticker"]
            ticker_suffix = kalshi_ticker.split("-")[-1].upper() if "-" in kalshi_ticker else ""

            # Find the matching Polymarket token based on team
            matching_token = None
            for token in tokens:
                outcome = token["outcome"]
                # Match by checking if team abbreviation matches outcome
                outcome_teams = extract_teams_from_text(outcome)
                if ticker_suffix in [t.upper() for t in outcome_teams]:
                    matching_token = token
                    break

            # If no direct match, try matching by team name
            if not matching_token:
                for token in tokens:
                    outcome_lower = token["outcome"].lower()
                    # Check if ticker suffix team matches outcome
                    if ticker_suffix.lower() in NAME_TO_ABBREV:
                        # ticker_suffix is a name like "hawks"
                        if ticker_suffix.lower() in outcome_lower:
                            matching_token = token
                            break
                    else:
                        # ticker_suffix is an abbreviation like "ATL"
                        for name in NBA_TEAMS.get(ticker_suffix, []):
                            if name.lower() in outcome_lower:
                                matching_token = token
                                break
                    if matching_token:
                        break

            if not matching_token:
                continue

            entry = {
                "event_name": f"NBA: {' vs '.join(teams)} ({target_date})",
                "polymarket_token_id": matching_token["token_id"],
                "kalshi_ticker": kalshi_ticker,
                "outcome": matching_token["outcome"],
                "category": "nba",
                "active": True,
                "date": target_date,
            }

            # Avoid duplicates
            is_duplicate = any(
                e.get("kalshi_ticker") == kalshi_ticker and
                e.get("polymarket_token_id") == matching_token["token_id"]
                for e in existing
            )

            if not is_duplicate:
                existing.append(entry)
                added += 1

        # Remove example/placeholder entries
        existing = [e for e in existing if not e.get("polymarket_token_id", "").startswith("REPLACE")]

        with open(contracts_file, "w") as f:
            json.dump(existing, f, indent=2)

        print(f"\n  ✓ Added {added} contract pairs to contracts.json")

    # Cleanup
    await kalshi.close()
    await polymarket.close()

    print("\n" + "=" * 60)
    print("Done!")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Discover NBA game markets")
    parser.add_argument(
        "--save",
        action="store_true",
        help="Save matches to contracts.json",
    )
    parser.add_argument(
        "--date",
        type=str,
        help="Date to search (YYYY-MM-DD), defaults to today",
    )

    args = parser.parse_args()
    asyncio.run(main(args))
