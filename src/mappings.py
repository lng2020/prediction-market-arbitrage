"""
Contract Mapping Configuration

Defines mappings between Polymarket and Kalshi contracts for the same events.
"""

import json
import logging
from pathlib import Path
from typing import Optional

from .models import ContractPair

logger = logging.getLogger(__name__)


# Example contract mappings - these need to be updated with real contract IDs
# You can find these by:
# - Polymarket: Look at the token_id in the market URL or API response
# - Kalshi: Look at the ticker in the market URL or API response

EXAMPLE_MAPPINGS = [
    # Example format - replace with real contract IDs
    # ContractPair(
    #     event_name="Fed Rate Decision Dec 2024",
    #     polymarket_token_id="0x1234567890abcdef...",
    #     kalshi_ticker="FED-24DEC-T5.00",
    #     outcome="YES",
    # ),
]


def load_mappings_from_file(
    file_path: str,
    categories: Optional[list[str]] = None,
) -> list[ContractPair]:
    """
    Load contract mappings from a JSON file.

    Args:
        file_path: Path to the JSON mappings file
        categories: Optional list of categories to filter by (e.g., ["nba"])
                   If None, loads all mappings

    Expected format:
    [
        {
            "event_name": "Event Name",
            "polymarket_token_id": "0x...",
            "kalshi_ticker": "TICKER",
            "outcome": "YES",
            "category": "nba",
            "active": true
        }
    ]
    """
    path = Path(file_path)
    if not path.exists():
        logger.warning(f"Mappings file not found: {file_path}")
        return []

    try:
        with open(path) as f:
            data = json.load(f)

        mappings = []
        for item in data:
            # Skip comment/instruction entries
            if item.get("_comment") or item.get("_instructions"):
                if "event_name" not in item:
                    continue

            # Filter by category if specified
            item_category = item.get("category", "").lower()
            if categories:
                categories_lower = [c.lower() for c in categories]
                if item_category and item_category not in categories_lower:
                    continue

            pair = ContractPair(
                event_name=item["event_name"],
                polymarket_token_id=item["polymarket_token_id"],
                kalshi_ticker=item["kalshi_ticker"],
                outcome=item.get("outcome", "YES"),
                active=item.get("active", True),
            )
            mappings.append(pair)

        logger.info(
            f"Loaded {len(mappings)} contract mappings from {file_path}"
            + (f" (categories: {categories})" if categories else "")
        )
        return mappings

    except Exception as e:
        logger.error(f"Error loading mappings: {e}")
        return []


def save_mappings_to_file(mappings: list[ContractPair], file_path: str) -> bool:
    """Save contract mappings to a JSON file."""
    try:
        data = [
            {
                "event_name": m.event_name,
                "polymarket_token_id": m.polymarket_token_id,
                "kalshi_ticker": m.kalshi_ticker,
                "outcome": m.outcome,
                "active": m.active,
            }
            for m in mappings
        ]

        with open(file_path, "w") as f:
            json.dump(data, f, indent=2)

        logger.info(f"Saved {len(mappings)} contract mappings to {file_path}")
        return True

    except Exception as e:
        logger.error(f"Error saving mappings: {e}")
        return False


class ContractMapper:
    """
    Utility class to help discover and map contracts between platforms.

    This helps identify matching events across Polymarket and Kalshi.
    """

    def __init__(self, kalshi_client, polymarket_client):
        self.kalshi = kalshi_client
        self.polymarket = polymarket_client

    async def search_kalshi_markets(
        self,
        query: str,
        status: str = "open",
    ) -> list[dict]:
        """Search for Kalshi markets matching a query."""
        markets = await self.kalshi.get_markets(status=status)

        # Filter by query (simple string matching)
        query_lower = query.lower()
        matching = [
            m for m in markets
            if query_lower in m.get("title", "").lower()
            or query_lower in m.get("ticker", "").lower()
        ]

        return matching

    async def search_polymarket_markets(self, query: str) -> list[dict]:
        """Search for Polymarket markets matching a query."""
        markets = await self.polymarket.get_markets()

        # Filter by query
        query_lower = query.lower()
        matching = [
            m for m in markets
            if query_lower in m.get("question", "").lower()
            or query_lower in str(m.get("condition_id", "")).lower()
        ]

        return matching

    async def find_matching_events(
        self,
        search_term: str,
    ) -> list[dict]:
        """
        Find potentially matching events across both platforms.

        Returns list of potential matches for manual verification.
        """
        # Search both platforms
        kl_markets = await self.search_kalshi_markets(search_term)
        pm_markets = await self.search_polymarket_markets(search_term)

        potential_matches = []

        for kl in kl_markets:
            for pm in pm_markets:
                # Simple heuristic - check if titles are similar
                kl_title = kl.get("title", "").lower()
                pm_question = pm.get("question", "").lower()

                # You would want more sophisticated matching in production
                # This is just a starting point for manual verification
                common_words = set(kl_title.split()) & set(pm_question.split())

                if len(common_words) >= 3:
                    potential_matches.append({
                        "kalshi": {
                            "ticker": kl.get("ticker"),
                            "title": kl.get("title"),
                            "yes_bid": kl.get("yes_bid"),
                            "yes_ask": kl.get("yes_ask"),
                        },
                        "polymarket": {
                            "condition_id": pm.get("condition_id"),
                            "question": pm.get("question"),
                            "tokens": pm.get("tokens", []),
                        },
                        "common_words": list(common_words),
                    })

        return potential_matches

    def create_pair_from_match(
        self,
        kalshi_ticker: str,
        polymarket_token_id: str,
        event_name: str,
        outcome: str = "YES",
    ) -> ContractPair:
        """Create a ContractPair from a verified match."""
        return ContractPair(
            event_name=event_name,
            polymarket_token_id=polymarket_token_id,
            kalshi_ticker=kalshi_ticker,
            outcome=outcome,
            active=True,
        )
