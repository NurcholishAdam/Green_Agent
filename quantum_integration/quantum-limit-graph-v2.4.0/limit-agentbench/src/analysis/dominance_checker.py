# src/analysis/dominance_checker.py

from typing import Set, Dict


def dominates(
    a: Dict,
    b: Dict,
    minimize: Set[str],
    maximize: Set[str],
) -> bool:
    """
    Returns True if solution a Pareto-dominates solution b.
    """
    better_or_equal = True
    strictly_better = False

    for k in minimize:
        if a[k] > b[k]:
            better_or_equal = False
        elif a[k] < b[k]:
            strictly_better = True

    for k in maximize:
        if a[k] < b[k]:
            better_or_equal = False
        elif a[k] > b[k]:
            strictly_better = True

    return better_or_equal and strictly_better
