"""
Leaderboard aggregation utilities.
"""

from typing import List, Dict


def rank_by_green_score(results: List[Dict]) -> List[Dict]:
    """
    Sorts results by green_score (descending).
    """
    return sorted(
        results,
        key=lambda r: r["green_score"],
        reverse=True,
    )
