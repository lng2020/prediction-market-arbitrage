#!/usr/bin/env python3
"""
CS2 Game Market Discovery Tool

Discovers and matches Counter-Strike 2 game markets between Polymarket and Kalshi.

Usage:
    python scripts/discover_cs2.py [--save] [--date YYYY-MM-DD]
"""

import argparse
import asyncio
import json
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

import aiohttp

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.clients import KalshiClient, PolymarketClient
from src.config import load_config

# Kalshi CS2 series ticker
KALSHI_CS2_SERIES = "KXCSGOGAME"
POLYMARKET_GAMMA_API = "https://gamma-api.polymarket.com"

# CS2 team name normalizations (include slug abbreviations)
CS2_TEAM_ALIASES = {
    "liquid": ["liquid", "team liquid", "tl", "tl1"],
    "tyloo": ["tyloo", "tyl"],
    "navi": ["navi", "natus vincere", "natus-vincere", "nav"],
    "g2": ["g2", "g2 esports"],
    "faze": ["faze", "faze clan", "faz"],
    "vitality": ["vitality", "team vitality", "vit"],
    "astralis": ["astralis", "ast", "ast10"],
    "fnatic": ["fnatic", "fnc"],
    "mouz": ["mouz", "mousesports", "mou"],
    "heroic": ["heroic", "her"],
    "ence": ["ence"],
    "cloud9": ["cloud9", "c9"],
    "complexity": ["complexity", "col"],
    "furia": ["furia", "fur"],
    "imperial": ["imperial", "imp"],
    "m80": ["m80"],
    "3dmax": ["3dmax", "3dm"],
    "b8": ["b8"],
    "passion ua": ["passion ua", "passionua", "pssnua", "pssn"],
    "aurora": ["aurora", "aurora gaming", "aur", "aur1"],
    "parivision": ["parivision", "prv"],
    "nip": ["nip", "ninjas in pyjamas"],
    "eternal fire": ["eternal fire", "ef", "ef1", "eternalfire"],
    "nuclear tigeres": ["nuclear tigeres", "ntr", "nucleartiger"],
}

# Reverse lookup
NAME_TO_CANONICAL = {}
for canonical, aliases in CS2_TEAM_ALIASES.items():
    for alias in aliases:
        NAME_TO_CANONICAL[alias.lower()] = canonical


def normalize_team_name(name: str) -> str:
    """Normalize a team name to canonical form."""
    name_lower = name.lower().strip()
    return NAME_TO_CANONICAL.get(name_lower, name_lower)


def extract_teams_from_kalshi(market: dict) -> tuple[str, str]:
    """Extract team names from Kalshi market title."""
    title = market.get("title", "")

    # Title format: "Will TYLOO win the TYLOO vs. Liquid match?"
    # Look for "the X vs. Y match" pattern first
    match = re.search(r"the\s+(.+?)\s+vs\.?\s+(.+?)\s+match", title, re.IGNORECASE)
    if match:
        team1 = normalize_team_name(match.group(1))
        team2 = normalize_team_name(match.group(2))
        return team1, team2

    # Fallback: just look for X vs Y
    match = re.search(r"([A-Za-z0-9]+(?:\s+[A-Za-z0-9]+)?)\s+vs\.?\s+([A-Za-z0-9]+(?:\s+[A-Za-z0-9]+)?)", title, re.IGNORECASE)
    if match:
        team1 = normalize_team_name(match.group(1))
        team2 = normalize_team_name(match.group(2))
        return team1, team2

    return "", ""


def extract_teams_from_polymarket(event: dict) -> tuple[str, str]:
    """Extract team names from Polymarket event title."""
    title = event.get("title", "")

    # Title format: "Counter-Strike: Team1 vs Team2 (BO3)"
    clean = title.replace("Counter-Strike: ", "").replace("Counter-Strike:", "")
    clean = re.sub(r"\s*\(BO\d+\)", "", clean)  # Remove (BO3) etc

    if " vs " in clean:
        parts = clean.split(" vs ")
        if len(parts) == 2:
            team1 = normalize_team_name(parts[0].strip())
            team2 = normalize_team_name(parts[1].strip())
            return team1, team2

    return "", ""


async def fetch_kalshi_cs2_games(kalshi: KalshiClient, date: str) -> list[dict]:
    """Fetch CS2 game markets from Kalshi for a specific date."""
    games = []
    all_cs2_games = []

    # Format date for Kalshi ticker - e.g., 2025-12-01 -> 25DEC01
    date_patterns = []
    try:
        dt = datetime.strptime(date, "%Y-%m-%d")
        date_patterns.append(dt.strftime("%y%b%d").upper())  # 25DEC01
        date_patterns.append(dt.strftime("%d%b").upper())     # 01DEC
    except ValueError:
        pass

    try:
        markets = await kalshi.get_markets(series_ticker=KALSHI_CS2_SERIES, status="open", limit=500)

        for m in markets:
            ticker = m.get("ticker", "").upper()
            all_cs2_games.append(m)

            # Check if date matches
            date_match = False
            for pattern in date_patterns:
                if pattern in ticker:
                    date_match = True
                    break

            if date_match:
                games.append(m)

        if not games and all_cs2_games:
            print(f"    (Found {len(all_cs2_games)} total CS2 markets, but none for {date})")
            print(f"    Sample tickers: {[m.get('ticker') for m in all_cs2_games[:3]]}")

    except Exception as e:
        print(f"  Warning: Kalshi fetch error: {e}")

    return games


async def fetch_polymarket_cs2_games(date: str, kalshi_games: list[dict] = None) -> list[dict]:
    """
    Fetch CS2 game events from Polymarket for a specific date.

    Uses the series endpoint to find event slugs, then fetches full details.
    """
    games = []
    seen_ids = set()
    slugs_to_fetch = []

    async with aiohttp.ClientSession() as session:
        # Step 1: Use series endpoint to get all CS2 event slugs for the date
        series_url = f"{POLYMARKET_GAMMA_API}/series?slug=counter-strike"
        try:
            async with session.get(series_url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data and len(data) > 0:
                        series = data[0]
                        events = series.get("events", [])
                        for event in events:
                            event_date = event.get("endDate", "")[:10]
                            slug = event.get("slug", "")
                            closed = event.get("closed", False)

                            # Filter by date and not closed
                            if not closed and (date in event_date or date in slug):
                                if slug not in slugs_to_fetch:
                                    slugs_to_fetch.append(slug)
        except Exception as e:
            print(f"    Warning: Series fetch error: {e}")

        # Step 2: Fetch full event details for each slug (includes markets/tokens)
        url = f"{POLYMARKET_GAMMA_API}/events"
        for slug in slugs_to_fetch:
            try:
                params = {"slug": slug}
                async with session.get(url, params=params) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        for event in data:
                            eid = event.get("id")
                            if eid and eid not in seen_ids:
                                seen_ids.add(eid)
                                games.append(event)
            except Exception:
                pass

        # Step 3: Fallback - also try slug patterns from Kalshi teams
        if kalshi_games:
            for kl in kalshi_games:
                team1, team2 = extract_teams_from_kalshi(kl)
                if team1 and team2:
                    t1_aliases = CS2_TEAM_ALIASES.get(team1, [team1])
                    t2_aliases = CS2_TEAM_ALIASES.get(team2, [team2])

                    for t1_alias in t1_aliases:
                        for t2_alias in t2_aliases:
                            t1_slug = t1_alias.replace(" ", "-").replace("_", "-").lower()
                            t2_slug = t2_alias.replace(" ", "-").replace("_", "-").lower()

                            for slug in [f"cs2-{t1_slug}-{t2_slug}-{date}", f"cs2-{t2_slug}-{t1_slug}-{date}"]:
                                if slug not in slugs_to_fetch:
                                    try:
                                        params = {"slug": slug}
                                        async with session.get(url, params=params) as resp:
                                            if resp.status == 200:
                                                data = await resp.json()
                                                for event in data:
                                                    eid = event.get("id")
                                                    if eid and eid not in seen_ids:
                                                        seen_ids.add(eid)
                                                        games.append(event)
                                    except Exception:
                                        pass

    return games


def extract_pm_tokens(pm: dict) -> list[dict]:
    """Extract token IDs from a Polymarket event (game winner market only)."""
    markets = pm.get("markets", [])
    tokens = []

    for market in markets:
        question = market.get("question", "").lower()

        # Skip over/under markets
        if "o/u" in question or "over" in question or "under" in question or "total" in question:
            continue

        clob_token_ids = market.get("clobTokenIds", [])
        outcomes = market.get("outcomes", [])

        # Parse JSON strings if needed
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

        if clob_token_ids and outcomes:
            for i, outcome in enumerate(outcomes):
                if i < len(clob_token_ids):
                    tokens.append({
                        "outcome": outcome,
                        "token_id": clob_token_ids[i],
                    })
            break  # Only take first game winner market

    return tokens


async def match_games(
    kalshi_games: list[dict],
    polymarket_games: list[dict],
) -> list[dict]:
    """Match games between platforms by team names."""
    matches = []

    # Build Polymarket lookup by team set
    pm_by_teams = {}
    for pm in polymarket_games:
        team1, team2 = extract_teams_from_polymarket(pm)
        if team1 and team2:
            key = frozenset([team1, team2])
            pm_by_teams[key] = pm

    # Group Kalshi markets by game (each game has 2 markets, one per team)
    kl_by_game = {}
    for kl in kalshi_games:
        ticker = kl.get("ticker", "")
        # Base ticker without team suffix: KXCSGOGAME-25DEC01TYLOOTL
        base = "-".join(ticker.split("-")[:-1])
        if base not in kl_by_game:
            kl_by_game[base] = []
        kl_by_game[base].append(kl)

    # Match games
    for base_ticker, kl_markets in kl_by_game.items():
        if not kl_markets:
            continue

        # Extract teams from first market
        team1, team2 = extract_teams_from_kalshi(kl_markets[0])
        if not team1 or not team2:
            continue

        key = frozenset([team1, team2])
        pm = pm_by_teams.get(key)

        if pm:
            tokens = extract_pm_tokens(pm)
            matches.append({
                "teams": [team1, team2],
                "kalshi_markets": kl_markets,
                "kalshi_base_ticker": base_ticker,
                "polymarket": pm,
                "pm_tokens": tokens,
            })

    return matches


async def main(args: argparse.Namespace) -> None:
    """Main discovery routine."""
    # Determine date
    if args.date:
        target_date = args.date
    else:
        target_date = datetime.now().strftime("%Y-%m-%d")

    print(f"\n🎮 CS2 Game Discovery - {target_date}")
    print("=" * 60)

    config = load_config()

    # Initialize Kalshi client
    print("\n📡 Initializing API clients...")
    kalshi = KalshiClient(config.kalshi)

    try:
        await kalshi.initialize()
        print("  ✓ Kalshi client initialized")
    except Exception as e:
        print(f"  ⚠ Kalshi init error: {e}")

    # Fetch games
    print(f"\n📊 Fetching CS2 games for {target_date}...")

    print("  Kalshi...")
    kl_games = await fetch_kalshi_cs2_games(kalshi, target_date)
    print(f"  ✓ Found {len(kl_games)} Kalshi CS2 markets")

    print("  Polymarket (using Kalshi teams to build slug queries)...")
    pm_games = await fetch_polymarket_cs2_games(target_date, kl_games)
    print(f"  ✓ Found {len(pm_games)} Polymarket CS2 events")

    # Display Kalshi games
    print("\n" + "=" * 60)
    print(f"KALSHI CS2 GAMES ({target_date}):")
    print("-" * 60)

    if kl_games:
        seen_bases = set()
        for m in kl_games:
            ticker = m.get("ticker", "")
            base = "-".join(ticker.split("-")[:-1])
            if base in seen_bases:
                continue
            seen_bases.add(base)

            title = m.get("title", "")[:60]
            team1, team2 = extract_teams_from_kalshi(m)
            yes_bid = m.get("yes_bid", "N/A")
            yes_ask = m.get("yes_ask", "N/A")

            print(f"  {base}")
            print(f"    {team1.upper()} vs {team2.upper()}")
            print(f"    Bid: {yes_bid}¢ Ask: {yes_ask}¢")
            print()
    else:
        print("  No games found for this date")

    # Display Polymarket games
    print("\n" + "=" * 60)
    print(f"POLYMARKET CS2 GAMES ({target_date}):")
    print("-" * 60)

    if pm_games:
        for event in pm_games:
            slug = event.get("slug", "N/A")
            title = event.get("title", "N/A")[:60]
            team1, team2 = extract_teams_from_polymarket(event)

            print(f"  {slug}")
            print(f"    {team1.upper()} vs {team2.upper()}")

            tokens = extract_pm_tokens(event)
            for token in tokens[:2]:
                print(f"    {token['outcome']}: {token['token_id'][:40]}...")
            print()
    else:
        print("  No games found for this date")

    # Match games
    print("\n" + "=" * 60)
    print("🎯 MATCHING GAMES...")
    print("-" * 60)

    matches = await match_games(kl_games, pm_games)

    if matches:
        print(f"\n  ✓ Found {len(matches)} matching games:\n")
        for i, match in enumerate(matches, 1):
            teams = " vs ".join([t.upper() for t in match["teams"]])
            base_ticker = match["kalshi_base_ticker"]
            pm = match["polymarket"]

            print(f"  Match #{i}: {teams}")
            print(f"    Kalshi Base: {base_ticker}")
            for kl in match["kalshi_markets"][:2]:
                print(f"      {kl.get('ticker')}")
            print(f"    Polymarket:  {pm.get('slug')}")
            for token in match["pm_tokens"][:2]:
                print(f"      {token['outcome']}: {token['token_id'][:40]}...")
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

        # Load existing contracts to preserve other categories
        existing_data = {}
        if contracts_file.exists():
            with open(contracts_file) as f:
                existing_data = json.load(f)

        # Build new CS2 entries
        cs2_entries = []
        added = 0
        for match in matches:
            teams = match["teams"]
            pm = match["polymarket"]
            pm_tokens = match["pm_tokens"]

            if not pm_tokens:
                continue

            for kl in match["kalshi_markets"]:
                kalshi_ticker = kl.get("ticker", "")
                # Get team from ticker suffix
                ticker_team = kalshi_ticker.split("-")[-1].lower()

                # Normalize ticker team to canonical name
                ticker_team_canonical = NAME_TO_CANONICAL.get(ticker_team, ticker_team)

                # Find matching PM token
                matching_token = None
                for token in pm_tokens:
                    outcome_normalized = normalize_team_name(token["outcome"])

                    # Check if canonical names match
                    if ticker_team_canonical == outcome_normalized:
                        matching_token = token
                        break

                    # Check if ticker team is in outcome or vice versa
                    if ticker_team in outcome_normalized or outcome_normalized in ticker_team:
                        matching_token = token
                        break

                # Try broader match using all aliases
                if not matching_token:
                    ticker_aliases = CS2_TEAM_ALIASES.get(ticker_team_canonical, [ticker_team])
                    for token in pm_tokens:
                        outcome_lower = token["outcome"].lower()
                        outcome_canonical = normalize_team_name(outcome_lower)

                        # Check if any alias matches
                        for alias in ticker_aliases:
                            if alias.lower() in outcome_lower or outcome_lower in alias.lower():
                                matching_token = token
                                break

                        # Also check canonical match
                        if ticker_team_canonical == outcome_canonical:
                            matching_token = token
                            break

                        if matching_token:
                            break

                if not matching_token:
                    continue

                entry = {
                    "event_name": f"CS2: {' vs '.join([t.upper() for t in teams])} ({target_date})",
                    "polymarket_token_id": matching_token["token_id"],
                    "kalshi_ticker": kalshi_ticker,
                    "outcome": matching_token["outcome"],
                    "active": True,
                    "date": target_date,
                }

                # Avoid duplicates
                is_duplicate = any(
                    e.get("kalshi_ticker") == kalshi_ticker and
                    e.get("polymarket_token_id") == matching_token["token_id"]
                    for e in cs2_entries
                )

                if not is_duplicate:
                    cs2_entries.append(entry)
                    added += 1

        # Update only the cs2 category, preserve others
        existing_data["cs2"] = cs2_entries

        with open(contracts_file, "w") as f:
            json.dump(existing_data, f, indent=2)

        print(f"\n  ✓ Added {added} contract pairs to contracts.json (cs2 category)")

    # Cleanup
    await kalshi.close()

    print("\n" + "=" * 60)
    print("Done!")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Discover CS2 game markets")
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
