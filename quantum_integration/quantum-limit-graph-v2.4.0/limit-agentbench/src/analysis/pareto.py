"""
Pareto frontier computation for multi-objective agent comparison.
"""

from typing import List, Dict, Iterable


def dominates(a: Dict, b: Dict, objectives: Iterable[str]) -> bool:
    """
    True if a dominates b (>= all, > at least one).
    """
    better_or_equal = True
    strictly_better = False

    for obj in objectives:
        if a[obj] > b[obj] and obj != "accuracy":
            better_or_equal = False
        if a[obj] < b[obj] and obj != "accuracy":
            strictly_better = True
        if obj == "accuracy":
            if a[obj] < b[obj]:
                better_or_equal = False
            if a[obj] > b[obj]:
                strictly_better = True

    return better_or_equal and strictly_better


def pareto_front(
    results: List[Dict],
    objectives=("accuracy", "energy", "latency"),
) -> List[Dict]:
    """
    Returns non-dominated results.
    """
    front = []

    for candidate in results:
        dominated = False
        for other in results:
            if other is candidate:
                continue
            if dominates(other, candidate, objectives):
                dominated = True
                break
        if not dominated:
            front.append(candidate)

    return front
