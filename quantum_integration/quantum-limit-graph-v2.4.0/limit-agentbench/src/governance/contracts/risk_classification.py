"""
RiskClassification — the risk level attached to a decision.

An immutable, self-validating value object combining a closed
:class:`RiskLevel` with the list of contributing factors that produced
it.

Key guarantees
--------------
* **Immutable**: ``frozen=True`` and ``factors`` is a ``tuple``, so a
  classification cannot be rewritten after the fact.
* **Typed**: ``level`` is a :class:`RiskLevel`; plain strings are
  coerced and unknown values rejected.
* **Comparable**: ``at_least`` / ``at_most`` accept either a
  :class:`RiskLevel` or its string form. ``RiskLevel.rank`` gives a
  stable ordering for ``sorted`` calls.
* **Composable**: ``merge`` combines two classifications (highest
  level wins, factor lists are unioned, order preserved).
  ``with_factor`` returns an updated copy without mutating the
  original.
* **Round-trippable**: ``to_dict`` / ``from_dict`` / ``to_json`` /
  ``to_schema`` match the conventions of ``DecisionRequest``,
  ``PolicyDecision``, and ``GovernanceEvent``.

Note on ``RiskLevel``
---------------------
``PolicyDecision`` also defines a ``RiskLevel`` enum with the same
members. In a shared codebase these should be imported from one
canonical module (e.g. ``governance.enums``); the duplication is
preserved here so this module remains self-contained, but consumers
should treat the two as interchangeable. Because both mix in ``str``,
cross-module ``==`` comparisons work; ``isinstance`` checks do not.
"""

from __future__ import annotations

import json
import unittest
from dataclasses import FrozenInstanceError, dataclass
from enum import Enum
from typing import Any, Dict, Mapping, Optional, Tuple, Union

__all__ = ["RiskLevel", "RiskClassification"]


# ---------------------------------------------------------------------------
# RiskLevel
# ---------------------------------------------------------------------------
class RiskLevel(str, Enum):
    """Closed vocabulary for risk severity, ordered by ``rank``."""

    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"

    @property
    def rank(self) -> int:
        """Position in the severity order (LOW=0 … CRITICAL=3)."""
        return _RISK_RANK[self.value]


# Rank is derived from enum definition order — one source of truth.
# Adding a new level only requires inserting it in the class body.
_RISK_RANK: Dict[str, int] = {
    level.value: i for i, level in enumerate(RiskLevel)
}


def _as_level(level: Union[RiskLevel, str]) -> RiskLevel:
    """Coerce ``level`` to a :class:`RiskLevel` or raise ``ValueError``."""
    return level if isinstance(level, RiskLevel) else RiskLevel(level)


# ---------------------------------------------------------------------------
# RiskClassification
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class RiskClassification:
    """
    Risk level with the contributing factors that produced it.

    ``factors`` is a ``tuple`` (not a ``list``) so the object is truly
    immutable once constructed — even though a ``frozen`` dataclass
    with a mutable field would technically allow ``.factors.append``.
    """

    level: RiskLevel = RiskLevel.LOW
    factors: Tuple[str, ...] = ()

    # -- invariants --------------------------------------------------------
    def __post_init__(self) -> None:
        # Coerce level
        if not isinstance(self.level, RiskLevel):
            object.__setattr__(self, "level", RiskLevel(self.level))

        # Reject a bare string; ``tuple("abc")`` would silently split it.
        if isinstance(self.factors, str):
            raise TypeError(
                "factors must be an iterable of strings, not a single "
                "string; wrap in a list, e.g. factors=['my_factor']"
            )

        # Coerce any other iterable to a tuple.
        if not isinstance(self.factors, tuple):
            object.__setattr__(self, "factors", tuple(self.factors))

        # Validate each factor.
        for f in self.factors:
            if not isinstance(f, str) or not f.strip():
                raise ValueError(
                    f"factor entries must be non-empty strings, got {f!r}"
                )

    # -- comparison --------------------------------------------------------
    def at_least(self, level: Union[RiskLevel, str]) -> bool:
        """True when this classification is at least as severe as ``level``."""
        return self.level.rank >= _as_level(level).rank

    def at_most(self, level: Union[RiskLevel, str]) -> bool:
        """True when this classification is at most as severe as ``level``."""
        return self.level.rank <= _as_level(level).rank

    # -- level predicates --------------------------------------------------
    def is_low(self) -> bool:
        return self.level is RiskLevel.LOW

    def is_medium(self) -> bool:
        return self.level is RiskLevel.MEDIUM

    def is_high(self) -> bool:
        return self.level is RiskLevel.HIGH

    def is_critical(self) -> bool:
        return self.level is RiskLevel.CRITICAL

    # -- combinators -------------------------------------------------------
    @property
    def unique_factors(self) -> Tuple[str, ...]:
        """Factors deduplicated while preserving first-seen order."""
        return tuple(dict.fromkeys(self.factors))

    def with_factor(
        self,
        factor: str,
        *,
        raise_to: Optional[Union[RiskLevel, str]] = None,
    ) -> "RiskClassification":
        """Return a copy with ``factor`` appended and, optionally, the
        level raised to ``raise_to`` if that level is more severe.

        ``raise_to`` never downgrades the level — pass a lower value and
        the current level wins.
        """
        if not isinstance(factor, str) or not factor.strip():
            raise ValueError("factor must be a non-empty string")

        new_level = self.level
        if raise_to is not None:
            new_level = max(
                self.level,
                _as_level(raise_to),
                key=lambda r: r.rank,
            )

        return RiskClassification(
            level=new_level,
            factors=self.factors + (factor,),
        )

    def merge(self, other: "RiskClassification") -> "RiskClassification":
        """Combine with ``other``: highest level wins, factors are unioned.

        The operation is commutative for level (``max``) and for the
        factor set (dedup preserving order). Order of ``factors`` in the
        result depends on argument order — callers that need a canonical
        result should sort ``unique_factors`` downstream.
        """
        if not isinstance(other, RiskClassification):
            raise TypeError("other must be a RiskClassification")

        highest = max(
            self.level, other.level, key=lambda r: r.rank
        )
        combined = tuple(dict.fromkeys(self.factors + other.factors))
        return RiskClassification(level=highest, factors=combined)

    # -- serialization -----------------------------------------------------
    def to_dict(self) -> Dict[str, Any]:
        return {
            "level": self.level.value,
            "factors": list(self.factors),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "RiskClassification":
        if not isinstance(data, Mapping):
            raise TypeError(
                f"data must be a Mapping, got {type(data).__name__}"
            )
        raw_level = data.get("level", "low")
        return cls(
            level=_as_level(raw_level),
            factors=tuple(data.get("factors") or ()),
        )

    def to_json(self) -> str:
        return json.dumps(
            self.to_dict(), separators=(",", ":"), sort_keys=True
        )

    def to_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "required": ["level", "factors"],
            "properties": {
                "level": {
                    "type": "string",
                    "enum": [r.value for r in RiskLevel],
                },
                "factors": {
                    "type": "array",
                    "items": {"type": "string", "minLength": 1},
                },
            },
            "additionalProperties": False,
        }

    # -- misc --------------------------------------------------------------
    def __str__(self) -> str:
        return (
            f"RiskClassification({self.level.value}: "
            f"{list(self.factors)})"
        )


# ===========================================================================
# TESTS
# ===========================================================================
class TestRiskLevel(unittest.TestCase):
    def test_values(self):
        self.assertEqual(RiskLevel.LOW.value, "low")
        self.assertEqual(RiskLevel.MEDIUM.value, "medium")
        self.assertEqual(RiskLevel.HIGH.value, "high")
        self.assertEqual(RiskLevel.CRITICAL.value, "critical")

    def test_str_enum_equality(self):
        self.assertEqual(RiskLevel.LOW, "low")
        self.assertEqual(RiskLevel.HIGH, "high")

    def test_rank_ordering(self):
        self.assertEqual(RiskLevel.LOW.rank, 0)
        self.assertEqual(RiskLevel.MEDIUM.rank, 1)
        self.assertEqual(RiskLevel.HIGH.rank, 2)
        self.assertEqual(RiskLevel.CRITICAL.rank, 3)

    def test_rank_is_stable_across_enum_order(self):
        ordered = sorted(RiskLevel, key=lambda r: r.rank)
        self.assertEqual(
            ordered,
            [
                RiskLevel.LOW,
                RiskLevel.MEDIUM,
                RiskLevel.HIGH,
                RiskLevel.CRITICAL,
            ],
        )


class TestConstruction(unittest.TestCase):
    def test_default(self):
        rc = RiskClassification()
        self.assertIs(rc.level, RiskLevel.LOW)
        self.assertEqual(rc.factors, ())

    def test_level_from_string(self):
        rc = RiskClassification(level="high")
        self.assertIs(rc.level, RiskLevel.HIGH)

    def test_level_from_enum(self):
        rc = RiskClassification(level=RiskLevel.CRITICAL)
        self.assertIs(rc.level, RiskLevel.CRITICAL)

    def test_uppercase_level_rejected(self):
        with self.assertRaises(ValueError):
            RiskClassification(level="HIGH")

    def test_typo_level_rejected(self):
        with self.assertRaises(ValueError):
            RiskClassification(level="hig")

    def test_factors_coerced_to_tuple(self):
        rc = RiskClassification(factors=["a", "b"])
        self.assertEqual(rc.factors, ("a", "b"))
        self.assertIsInstance(rc.factors, tuple)

    def test_single_string_factors_rejected(self):
        with self.assertRaises(TypeError):
            RiskClassification(factors="abc")  # type: ignore[arg-type]

    def test_empty_factor_rejected(self):
        with self.assertRaises(ValueError):
            RiskClassification(factors=[""])

    def test_whitespace_factor_rejected(self):
        with self.assertRaises(ValueError):
            RiskClassification(factors=["   "])

    def test_non_string_factor_rejected(self):
        with self.assertRaises(ValueError):
            RiskClassification(factors=[123])  # type: ignore[list-item]

    def test_duplicate_factors_allowed(self):
        rc = RiskClassification(factors=["a", "a", "b"])
        self.assertEqual(rc.factors, ("a", "a", "b"))


class TestImmutability(unittest.TestCase):
    def test_frozen(self):
        rc = RiskClassification()
        with self.assertRaises(FrozenInstanceError):
            rc.level = RiskLevel.HIGH  # type: ignore[misc]

    def test_factors_tuple_not_mutable(self):
        rc = RiskClassification(factors=["a"])
        with self.assertRaises(AttributeError):
            rc.factors.append("b")  # type: ignore[attr-defined]

    def test_caller_list_not_aliased(self):
        src = ["a"]
        rc = RiskClassification(factors=src)
        src.append("b")
        self.assertEqual(rc.factors, ("a",))

    def test_hashable(self):
        rc = RiskClassification(level="high", factors=["a"])
        self.assertEqual({rc, rc}, {rc})

    def test_equality(self):
        self.assertEqual(
            RiskClassification(level="high", factors=["a"]),
            RiskClassification(level="high", factors=["a"]),
        )


class TestComparison(unittest.TestCase):
    def test_at_least_enum(self):
        rc = RiskClassification(level="high")
        self.assertTrue(rc.at_least(RiskLevel.MEDIUM))
        self.assertTrue(rc.at_least(RiskLevel.HIGH))
        self.assertFalse(rc.at_least(RiskLevel.CRITICAL))

    def test_at_least_string(self):
        rc = RiskClassification(level="high")
        self.assertTrue(rc.at_least("high"))
        self.assertFalse(rc.at_least("critical"))

    def test_at_least_unknown_string(self):
        with self.assertRaises(ValueError):
            RiskClassification().at_least("hig")

    def test_at_most_symmetric(self):
        rc = RiskClassification(level="medium")
        self.assertTrue(rc.at_most(RiskLevel.HIGH))
        self.assertTrue(rc.at_most(RiskLevel.MEDIUM))
        self.assertFalse(rc.at_most(RiskLevel.LOW))

    def test_sort_by_rank(self):
        rcs = [
            RiskClassification(level="high"),
            RiskClassification(level="low"),
            RiskClassification(level="critical"),
            RiskClassification(level="medium"),
        ]
        ordered = sorted(rcs, key=lambda r: r.level.rank)
        self.assertEqual(
            [r.level for r in ordered],
            [
                RiskLevel.LOW,
                RiskLevel.MEDIUM,
                RiskLevel.HIGH,
                RiskLevel.CRITICAL,
            ],
        )


class TestPredicates(unittest.TestCase):
    def test_exclusive(self):
        cases = [
            ("low", "is_low"),
            ("medium", "is_medium"),
            ("high", "is_high"),
            ("critical", "is_critical"),
        ]
        for level, method in cases:
            rc = RiskClassification(level=level)
            for _, other in cases:
                with self.subTest(level=level, method=other):
                    expected = method == other
                    self.assertEqual(getattr(rc, other)(), expected)


class TestUniqueFactors(unittest.TestCase):
    def test_preserves_first_seen_order(self):
        rc = RiskClassification(factors=["b", "a", "b", "c", "a"])
        self.assertEqual(rc.unique_factors, ("b", "a", "c"))

    def test_empty(self):
        self.assertEqual(RiskClassification().unique_factors, ())


class TestWithFactor(unittest.TestCase):
    def test_appends_factor(self):
        rc = RiskClassification(level="low", factors=["a"])
        rc2 = rc.with_factor("b")
        self.assertEqual(rc2.factors, ("a", "b"))
        self.assertEqual(rc.factors, ("a",))  # unchanged
        self.assertIs(rc2.level, RiskLevel.LOW)

    def test_raise_to_upgrades(self):
        rc = RiskClassification(level="low")
        rc2 = rc.with_factor("x", raise_to="high")
        self.assertIs(rc2.level, RiskLevel.HIGH)
        self.assertEqual(rc2.factors, ("x",))

    def test_raise_to_never_downgrades(self):
        rc = RiskClassification(level="critical")
        rc2 = rc.with_factor("x", raise_to="low")
        self.assertIs(rc2.level, RiskLevel.CRITICAL)

    def test_empty_factor_rejected(self):
        with self.assertRaises(ValueError):
            RiskClassification().with_factor("")


class TestMerge(unittest.TestCase):
    def test_takes_highest_level(self):
        a = RiskClassification(level="low", factors=["a"])
        b = RiskClassification(level="critical", factors=["b"])
        self.assertIs(a.merge(b).level, RiskLevel.CRITICAL)
        self.assertIs(b.merge(a).level, RiskLevel.CRITICAL)

    def test_dedups_factors(self):
        a = RiskClassification(level="high", factors=["a", "b"])
        b = RiskClassification(level="high", factors=["b", "c"])
        self.assertEqual(a.merge(b).factors, ("a", "b", "c"))

    def test_commutative_level_and_factor_set(self):
        a = RiskClassification(level="medium", factors=["x"])
        b = RiskClassification(level="high", factors=["y"])
        ab = a.merge(b)
        ba = b.merge(a)
        self.assertEqual(ab.level, ba.level)
        self.assertEqual(set(ab.factors), set(ba.factors))

    def test_identity_element(self):
        a = RiskClassification(level="high", factors=["x"])
        empty = RiskClassification()
        merged = a.merge(empty)
        self.assertIs(merged.level, RiskLevel.HIGH)
        self.assertEqual(merged.factors, ("x",))

    def test_type_checked(self):
        with self.assertRaises(TypeError):
            RiskClassification().merge("nope")  # type: ignore[arg-type]


class TestSerialization(unittest.TestCase):
    def test_to_dict_shape(self):
        rc = RiskClassification(level="high", factors=["a", "b"])
        self.assertEqual(
            rc.to_dict(), {"level": "high", "factors": ["a", "b"]}
        )

    def test_to_dict_is_json_safe(self):
        rc = RiskClassification(level="critical", factors=["a"])
        json.dumps(rc.to_dict())  # must not raise

    def test_round_trip(self):
        rc = RiskClassification(level="high", factors=["a", "b"])
        self.assertEqual(RiskClassification.from_dict(rc.to_dict()), rc)

    def test_from_dict_default_level(self):
        rc = RiskClassification.from_dict({"factors": ["x"]})
        self.assertIs(rc.level, RiskLevel.LOW)

    def test_from_dict_bad_level(self):
        with self.assertRaises(ValueError):
            RiskClassification.from_dict({"level": "hig"})

    def test_from_dict_non_mapping(self):
        with self.assertRaises(TypeError):
            RiskClassification.from_dict("nope")  # type: ignore[arg-type]

    def test_to_json_compact_and_stable(self):
        rc = RiskClassification(level="high", factors=["a", "b"])
        s = rc.to_json()
        self.assertNotIn(" ", s)
        self.assertEqual(s, rc.to_json())
        self.assertEqual(
            json.loads(s), {"level": "high", "factors": ["a", "b"]}
        )


class TestSchema(unittest.TestCase):
    def test_schema_shape(self):
        s = RiskClassification().to_schema()
        self.assertEqual(s["type"], "object")
        self.assertEqual(set(s["required"]), {"level", "factors"})
        self.assertEqual(
            set(s["properties"]["level"]["enum"]),
            {r.value for r in RiskLevel},
        )
        self.assertFalse(s["additionalProperties"])


class TestStr(unittest.TestCase):
    def test_str_contains_level_and_factors(self):
        rc = RiskClassification(level="high", factors=["a", "b"])
        s = str(rc)
        self.assertIn("high", s)
        self.assertIn("a", s)
        self.assertIn("b", s)


if __name__ == "__main__":
    unittest.main(verbosity=2)
