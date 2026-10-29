"""
HardwareCapabilityPolicy — decide whether a device may run a task.

A pure, stateless governance rule that accepts a
:class:`HardwareCapability` and returns a structured
:class:`CapabilityDecision` describing whether the device meets the
policy's floors and, if not, why.

Key guarantees
--------------
* **Fail-closed.** An invalid ``HardwareCapability`` (``None``, wrong
  type, non-finite numbers, out-of-range availability, non-``bool``
  tensor-core flag) is **denied** with ``INVALID_CAPABILITY``. In
  particular, ``NaN`` readings — for which every comparison returns
  ``False`` — no longer pass the memory floor.
* **Validated thresholds.** The policy refuses to construct with a
  negative memory floor, an out-of-range or non-finite Helium floor,
  or a non-``bool`` tensor-core requirement.
* **Immutable.** ``frozen=True`` on the policy; the floors cannot be
  rewritten after construction.
* **Structured result.** :class:`CapabilityDecision` carries a boolean,
  human-readable reasons, and :class:`ReasonCode` values for machine
  consumption. Reasons are sorted by severity so the most important
  one appears first.
* **Backward compatible.** :meth:`HardwareCapabilityPolicy.evaluate`
  still returns ``(bool, List[str])`` with the same semantics as
  before, and :meth:`HardwareCapabilityPolicy.check` is the fast path
  for callers that only want the boolean.
* **Round-trippable.** ``to_dict`` / ``from_dict`` / ``to_schema``
  match the conventions of the rest of the governance stack.
"""

from __future__ import annotations

import math
import unittest
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Tuple, Union

# ---------------------------------------------------------------------------
# Real imports (package mode) with standalone stubs
# ---------------------------------------------------------------------------
try:  # pragma: no cover
    from .helium_policy_adapter import HardwareCapability  # type: ignore
except Exception:  # pragma: no cover — standalone / test mode
    @dataclass(frozen=True)
    class HardwareCapability:  # type: ignore[no-redef]
        """Stand-in matching the fields the policy reads."""

        max_memory_mb: float
        helium_availability: float = 1.0
        has_tensor_cores: bool = False


__all__ = [
    "ReasonCode",
    "CapabilityDecision",
    "HardwareCapabilityPolicy",
    "DEFAULT_POLICY",
]


# ---------------------------------------------------------------------------
# Reason codes
# ---------------------------------------------------------------------------
class ReasonCode(str, Enum):
    """Machine-readable codes explaining a failed capability check."""

    MEMORY_BELOW_FLOOR = "memory_below_floor"
    HELIUM_BELOW_FLOOR = "helium_below_floor"
    TENSOR_CORES_REQUIRED = "tensor_cores_required"
    INVALID_CAPABILITY = "invalid_capability"


# Lower value = higher priority. Used to sort reasons by severity.
_REASON_SEVERITY: Dict[ReasonCode, int] = {
    ReasonCode.INVALID_CAPABILITY: 0,
    ReasonCode.MEMORY_BELOW_FLOOR: 1,
    ReasonCode.HELIUM_BELOW_FLOOR: 2,
    ReasonCode.TENSOR_CORES_REQUIRED: 3,
}


# ---------------------------------------------------------------------------
# Structured result
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class CapabilityDecision:
    """Structured outcome of :meth:`HardwareCapabilityPolicy.evaluate_decision`."""

    permitted: bool
    reasons: Tuple[str, ...] = ()
    codes: Tuple[ReasonCode, ...] = ()

    def __bool__(self) -> bool:
        """Ergonomic: ``if policy.evaluate_decision(cap): ...``."""
        return self.permitted

    def to_dict(self) -> Dict[str, Any]:
        return {
            "permitted": self.permitted,
            "reasons": list(self.reasons),
            "codes": [c.value for c in self.codes],
        }


# ---------------------------------------------------------------------------
# Helper: numeric + capability validation
# ---------------------------------------------------------------------------
def _is_number(value: Any) -> bool:
    """True for ``int``/``float``/``numbers.Real`` but NOT ``bool``."""
    if isinstance(value, bool):
        return False
    if not isinstance(value, (int, float)):
        # Accept numpy scalars etc. that behave like numbers.
        try:
            return math.isfinite(float(value)) or math.isinf(float(value))
        except (TypeError, ValueError):
            return False
    return True


def _is_finite_number(value: Any) -> bool:
    if not _is_number(value):
        return False
    try:
        return math.isfinite(float(value))
    except (TypeError, ValueError, OverflowError):
        return False


def _why_invalid_capability(capability: Any) -> str:
    """Return a human-readable reason the capability is invalid, or ``""``."""
    if not isinstance(capability, HardwareCapability):
        return (
            f"capability must be HardwareCapability, got "
            f"{type(capability).__name__}"
        )
    if not _is_finite_number(getattr(capability, "max_memory_mb", None)):
        return "capability.max_memory_mb must be a finite number"
    if not _is_finite_number(
        getattr(capability, "helium_availability", None)
    ):
        return "capability.helium_availability must be a finite number"
    helium = float(capability.helium_availability)
    if not 0.0 <= helium <= 1.0:
        return (
            f"capability.helium_availability {helium} out of "
            f"range [0, 1]"
        )
    if not isinstance(getattr(capability, "has_tensor_cores", None), bool):
        return "capability.has_tensor_cores must be a bool"
    return ""


# ---------------------------------------------------------------------------
# Policy
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class HardwareCapabilityPolicy:
    """
    Governance rule: deny devices that fall below capability floors.

    ``memory_epsilon_mb`` adds tolerance to the memory comparison, in
    the same units as ``min_memory_mb``. The default ``0.0`` means a
    strict ``<`` check. Set it to a small positive value (e.g. ``0.5``)
    to tolerate measurement noise at the floor.
    """

    min_memory_mb: float = 512.0
    min_helium_availability: float = 0.3
    require_tensor_cores: bool = False
    memory_epsilon_mb: float = 0.0

    # -- invariants --------------------------------------------------------
    def __post_init__(self) -> None:
        if not _is_finite_number(self.min_memory_mb):
            raise ValueError("min_memory_mb must be a finite number")
        if float(self.min_memory_mb) < 0:
            raise ValueError("min_memory_mb must be >= 0")

        if not _is_finite_number(self.min_helium_availability):
            raise ValueError(
                "min_helium_availability must be a finite number"
            )
        if not 0.0 <= float(self.min_helium_availability) <= 1.0:
            raise ValueError(
                "min_helium_availability must be in [0, 1]"
            )

        if not isinstance(self.require_tensor_cores, bool):
            raise TypeError("require_tensor_cores must be a bool")

        if not _is_finite_number(self.memory_epsilon_mb):
            raise ValueError("memory_epsilon_mb must be a finite number")
        if float(self.memory_epsilon_mb) < 0:
            raise ValueError("memory_epsilon_mb must be >= 0")

    # -- canonical API -----------------------------------------------------
    def evaluate_decision(self, capability: Any) -> CapabilityDecision:
        """Return a structured :class:`CapabilityDecision`."""
        # 1. Validate capability (fail closed).
        invalid = _why_invalid_capability(capability)
        if invalid:
            return CapabilityDecision(
                permitted=False,
                reasons=(invalid,),
                codes=(ReasonCode.INVALID_CAPABILITY,),
            )

        # 2. Evaluate floors.
        collected: List[Tuple[ReasonCode, str]] = []

        memory = float(capability.max_memory_mb)
        floor = float(self.min_memory_mb)
        epsilon = float(self.memory_epsilon_mb)
        if memory + epsilon < floor:
            collected.append((
                ReasonCode.MEMORY_BELOW_FLOOR,
                f"memory {capability.max_memory_mb}MB below floor "
                f"{self.min_memory_mb}MB",
            ))

        helium = float(capability.helium_availability)
        helium_floor = float(self.min_helium_availability)
        if helium < helium_floor:
            collected.append((
                ReasonCode.HELIUM_BELOW_FLOOR,
                f"helium availability {capability.helium_availability} "
                f"below floor {self.min_helium_availability}",
            ))

        if self.require_tensor_cores and capability.has_tensor_cores is not True:
            collected.append((
                ReasonCode.TENSOR_CORES_REQUIRED,
                "device lacks tensor cores",
            ))

        # 3. Sort by severity, then unpack.
        collected.sort(key=lambda pair: _REASON_SEVERITY[pair[0]])
        reasons = tuple(msg for _, msg in collected)
        codes = tuple(code for code, _ in collected)

        return CapabilityDecision(
            permitted=not collected,
            reasons=reasons,
            codes=codes,
        )

    # -- backward-compatible API ------------------------------------------
    def evaluate(self, capability: Any) -> Tuple[bool, List[str]]:
        """Backward-compatible wrapper returning ``(ok, reasons)``."""
        decision = self.evaluate_decision(capability)
        return (decision.permitted, list(decision.reasons))

    def check(self, capability: Any) -> bool:
        """Boolean fast path."""
        return self.evaluate_decision(capability).permitted

    # -- introspection -----------------------------------------------------
    def describe(self) -> str:
        """Human-readable summary of the policy's floors."""
        parts = [
            f"memory>={self.min_memory_mb}MB",
            f"helium>={self.min_helium_availability}",
        ]
        if self.require_tensor_cores:
            parts.append("tensor_cores_required")
        if self.memory_epsilon_mb:
            parts.append(f"epsilon={self.memory_epsilon_mb}MB")
        return "HardwareCapabilityPolicy(" + ", ".join(parts) + ")"

    def floors(self) -> Dict[str, Any]:
        """Structured floors for tooling."""
        return {
            "min_memory_mb": self.min_memory_mb,
            "min_helium_availability": self.min_helium_availability,
            "require_tensor_cores": self.require_tensor_cores,
            "memory_epsilon_mb": self.memory_epsilon_mb,
        }

    def __str__(self) -> str:
        return self.describe()

    def __repr__(self) -> str:
        return self.describe()

    # -- serialization -----------------------------------------------------
    def to_dict(self) -> Dict[str, Any]:
        return self.floors()

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "HardwareCapabilityPolicy":
        if not isinstance(data, dict):
            raise TypeError(
                f"data must be a dict, got {type(data).__name__}"
            )
        return cls(
            min_memory_mb=data.get("min_memory_mb", 512.0),
            min_helium_availability=data.get(
                "min_helium_availability", 0.3
            ),
            require_tensor_cores=data.get(
                "require_tensor_cores", False
            ),
            memory_epsilon_mb=data.get("memory_epsilon_mb", 0.0),
        )

    def to_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "required": [
                "min_memory_mb",
                "min_helium_availability",
                "require_tensor_cores",
            ],
            "properties": {
                "min_memory_mb": {"type": "number", "minimum": 0},
                "min_helium_availability": {
                    "type": "number",
                    "minimum": 0,
                    "maximum": 1,
                },
                "require_tensor_cores": {"type": "boolean"},
                "memory_epsilon_mb": {"type": "number", "minimum": 0},
            },
            "additionalProperties": False,
        }


# ---------------------------------------------------------------------------
# Convenience singleton
# ---------------------------------------------------------------------------
DEFAULT_POLICY: HardwareCapabilityPolicy = HardwareCapabilityPolicy()


# ===========================================================================
# TESTS
# ===========================================================================
def _cap(**kwargs: Any) -> HardwareCapability:
    """Build a capability that passes the default policy."""
    defaults: Dict[str, Any] = dict(
        max_memory_mb=2048.0,
        helium_availability=0.9,
        has_tensor_cores=True,
    )
    defaults.update(kwargs)
    return HardwareCapability(**defaults)


class TestPolicyConstruction(unittest.TestCase):
    def test_defaults(self):
        p = HardwareCapabilityPolicy()
        self.assertEqual(p.min_memory_mb, 512.0)
        self.assertEqual(p.min_helium_availability, 0.3)
        self.assertFalse(p.require_tensor_cores)
        self.assertEqual(p.memory_epsilon_mb, 0.0)

    def test_negative_memory_rejected(self):
        with self.assertRaises(ValueError):
            HardwareCapabilityPolicy(min_memory_mb=-1)

    def test_nan_memory_rejected(self):
        with self.assertRaises(ValueError):
            HardwareCapabilityPolicy(min_memory_mb=float("nan"))

    def test_inf_memory_rejected(self):
        with self.assertRaises(ValueError):
            HardwareCapabilityPolicy(min_memory_mb=float("inf"))

    def test_helium_above_one_rejected(self):
        with self.assertRaises(ValueError):
            HardwareCapabilityPolicy(min_helium_availability=1.5)

    def test_helium_below_zero_rejected(self):
        with self.assertRaises(ValueError):
            HardwareCapabilityPolicy(min_helium_availability=-0.1)

    def test_nan_helium_rejected(self):
        with self.assertRaises(ValueError):
            HardwareCapabilityPolicy(
                min_helium_availability=float("nan"),
            )

    def test_non_bool_tensor_cores_rejected(self):
        for bad in (0, 1, "true", None):
            with self.subTest(bad=bad):
                with self.assertRaises(TypeError):
                    HardwareCapabilityPolicy(require_tensor_cores=bad)  # type: ignore[arg-type]

    def test_negative_epsilon_rejected(self):
        with self.assertRaises(ValueError):
            HardwareCapabilityPolicy(memory_epsilon_mb=-0.1)

    def test_frozen(self):
        p = HardwareCapabilityPolicy()
        from dataclasses import FrozenInstanceError
        with self.assertRaises(FrozenInstanceError):
            p.min_memory_mb = 0  # type: ignore[misc]

    def test_default_policy_singleton(self):
        self.assertIsInstance(DEFAULT_POLICY, HardwareCapabilityPolicy)


class TestInvalidCapability(unittest.IsolatedAsyncioTestCase):
    def _policy(self) -> HardwareCapabilityPolicy:
        return HardwareCapabilityPolicy()

    def test_none(self):
        d = self._policy().evaluate_decision(None)
        self.assertFalse(d.permitted)
        self.assertEqual(d.codes, (ReasonCode.INVALID_CAPABILITY,))
        self.assertIn("HardwareCapability", d.reasons[0])

    def test_wrong_type(self):
        d = self._policy().evaluate_decision("hello")
        self.assertFalse(d.permitted)
        self.assertEqual(d.codes, (ReasonCode.INVALID_CAPABILITY,))

    def test_nan_memory_is_denied(self):
        # The original code would PASS this — NaN comparisons are False.
        cap = _cap(max_memory_mb=float("nan"))
        d = self._policy().evaluate_decision(cap)
        self.assertFalse(d.permitted)
        self.assertEqual(d.codes, (ReasonCode.INVALID_CAPABILITY,))
        self.assertIn("finite", d.reasons[0])

    def test_inf_memory_is_denied(self):
        cap = _cap(max_memory_mb=float("inf"))
        d = self._policy().evaluate_decision(cap)
        self.assertFalse(d.permitted)
        self.assertEqual(d.codes, (ReasonCode.INVALID_CAPABILITY,))

    def test_nan_helium_is_denied(self):
        cap = _cap(helium_availability=float("nan"))
        d = self._policy().evaluate_decision(cap)
        self.assertFalse(d.permitted)
        self.assertEqual(d.codes, (ReasonCode.INVALID_CAPABILITY,))

    def test_out_of_range_helium_is_denied(self):
        cap = _cap(helium_availability=2.0)
        d = self._policy().evaluate_decision(cap)
        self.assertFalse(d.permitted)
        self.assertEqual(d.codes, (ReasonCode.INVALID_CAPABILITY,))
        self.assertIn("range", d.reasons[0])

    def test_string_bool_is_denied(self):
        cap = HardwareCapability(
            max_memory_mb=2048.0,
            helium_availability=0.9,
            has_tensor_cores="false",  # type: ignore[arg-type]
        )
        d = self._policy().evaluate_decision(cap)
        self.assertFalse(d.permitted)
        self.assertEqual(d.codes, (ReasonCode.INVALID_CAPABILITY,))


class TestFloorComparisons(unittest.TestCase):
    def _policy(self, **kwargs: Any) -> HardwareCapabilityPolicy:
        return HardwareCapabilityPolicy(**kwargs)

    def test_memory_exactly_at_floor_allowed(self):
        p = self._policy(min_memory_mb=512.0)
        self.assertTrue(p.check(_cap(max_memory_mb=512.0)))

    def test_memory_below_floor_denied(self):
        p = self._policy(min_memory_mb=512.0)
        d = p.evaluate_decision(_cap(max_memory_mb=128.0))
        self.assertFalse(d.permitted)
        self.assertIn(ReasonCode.MEMORY_BELOW_FLOOR, d.codes)

    def test_memory_above_floor_allowed(self):
        p = self._policy(min_memory_mb=512.0)
        self.assertTrue(p.check(_cap(max_memory_mb=1024.0)))

    def test_helium_exactly_at_floor_allowed(self):
        p = self._policy(min_helium_availability=0.3)
        self.assertTrue(p.check(_cap(helium_availability=0.3)))

    def test_helium_below_floor_denied(self):
        p = self._policy(min_helium_availability=0.3)
        d = p.evaluate_decision(_cap(helium_availability=0.1))
        self.assertFalse(d.permitted)
        self.assertIn(ReasonCode.HELIUM_BELOW_FLOOR, d.codes)

    def test_tensor_cores_required_absent_denied(self):
        p = self._policy(require_tensor_cores=True)
        d = p.evaluate_decision(_cap(has_tensor_cores=False))
        self.assertFalse(d.permitted)
        self.assertIn(ReasonCode.TENSOR_CORES_REQUIRED, d.codes)

    def test_tensor_cores_required_present_allowed(self):
        p = self._policy(require_tensor_cores=True)
        self.assertTrue(p.check(_cap(has_tensor_cores=True)))

    def test_tensor_cores_not_required_absent_allowed(self):
        p = self._policy(require_tensor_cores=False)
        self.assertTrue(p.check(_cap(has_tensor_cores=False)))

    def test_reason_ordering_by_severity(self):
        # All three floors fail; memory should come first (severity 1),
        # then helium (2), then tensor cores (3).
        p = self._policy(require_tensor_cores=True)
        d = p.evaluate_decision(_cap(
            max_memory_mb=0.0,
            helium_availability=0.0,
            has_tensor_cores=False,
        ))
        self.assertEqual(
            d.codes,
            (
                ReasonCode.MEMORY_BELOW_FLOOR,
                ReasonCode.HELIUM_BELOW_FLOOR,
                ReasonCode.TENSOR_CORES_REQUIRED,
            ),
        )


class TestBackwardCompatibility(unittest.TestCase):
    def test_evaluate_returns_tuple(self):
        p = HardwareCapabilityPolicy()
        result = p.evaluate(_cap())
        self.assertIsInstance(result, tuple)
        self.assertEqual(len(result), 2)
        ok, reasons = result
        self.assertIsInstance(ok, bool)
        self.assertIsInstance(reasons, list)
        self.assertTrue(ok)
        self.assertEqual(reasons, [])

    def test_evaluate_deny(self):
        p = HardwareCapabilityPolicy(min_memory_mb=4096.0)
        ok, reasons = p.evaluate(_cap(max_memory_mb=512.0))
        self.assertFalse(ok)
        self.assertTrue(any("memory" in r for r in reasons))

    def test_check_matches_evaluate(self):
        p = HardwareCapabilityPolicy(min_memory_mb=1024.0)
        for mem in (500.0, 1024.0, 2048.0):
            cap = _cap(max_memory_mb=mem)
            ok, _ = p.evaluate(cap)
            self.assertEqual(ok, p.check(cap))


class TestEpsilon(unittest.TestCase):
    def test_strict_by_default(self):
        p = HardwareCapabilityPolicy(
            min_memory_mb=512.0, memory_epsilon_mb=0.0,
        )
        self.assertFalse(p.check(_cap(max_memory_mb=511.7)))

    def test_epsilon_tolerates_below(self):
        p = HardwareCapabilityPolicy(
            min_memory_mb=512.0, memory_epsilon_mb=0.5,
        )
        self.assertTrue(p.check(_cap(max_memory_mb=511.7)))

    def test_epsilon_does_not_bridge_large_gaps(self):
        p = HardwareCapabilityPolicy(
            min_memory_mb=512.0, memory_epsilon_mb=0.5,
        )
        self.assertFalse(p.check(_cap(max_memory_mb=510.0)))


class TestIntrospectionAndSerialization(unittest.TestCase):
    def test_describe_mentions_floors(self):
        p = HardwareCapabilityPolicy(require_tensor_cores=True)
        s = p.describe()
        self.assertIn("memory", s)
        self.assertIn("helium", s)
        self.assertIn("tensor_cores_required", s)

    def test_describe_omits_tensor_cores_when_not_required(self):
        p = HardwareCapabilityPolicy(require_tensor_cores=False)
        self.assertNotIn("tensor_cores_required", p.describe())

    def test_str_matches_describe(self):
        p = HardwareCapabilityPolicy()
        self.assertEqual(str(p), p.describe())
        self.assertEqual(repr(p), p.describe())

    def test_floors_dict(self):
        p = HardwareCapabilityPolicy(
            min_memory_mb=1024.0,
            min_helium_availability=0.5,
            require_tensor_cores=True,
            memory_epsilon_mb=0.5,
        )
        self.assertEqual(
            p.floors(),
            {
                "min_memory_mb": 1024.0,
                "min_helium_availability": 0.5,
                "require_tensor_cores": True,
                "memory_epsilon_mb": 0.5,
            },
        )

    def test_to_from_dict_round_trip(self):
        p = HardwareCapabilityPolicy(
            min_memory_mb=1024.0,
            min_helium_availability=0.5,
            require_tensor_cores=True,
            memory_epsilon_mb=0.5,
        )
        p2 = HardwareCapabilityPolicy.from_dict(p.to_dict())
        self.assertEqual(p.to_dict(), p2.to_dict())

    def test_from_dict_defaults(self):
        p = HardwareCapabilityPolicy.from_dict({})
        self.assertEqual(p.min_memory_mb, 512.0)
        self.assertEqual(p.min_helium_availability, 0.3)
        self.assertFalse(p.require_tensor_cores)
        self.assertEqual(p.memory_epsilon_mb, 0.0)

    def test_from_dict_non_dict_rejected(self):
        with self.assertRaises(TypeError):
            HardwareCapabilityPolicy.from_dict("nope")  # type: ignore[arg-type]

    def test_to_schema_shape(self):
        p = HardwareCapabilityPolicy()
        s = p.to_schema()
        self.assertEqual(s["type"], "object")
        self.assertEqual(
            set(s["required"]),
            {"min_memory_mb", "min_helium_availability",
             "require_tensor_cores"},
        )
        self.assertEqual(
            s["properties"]["min_helium_availability"]["maximum"], 1,
        )
        self.assertFalse(s["additionalProperties"])


class TestCapabilityDecision(unittest.TestCase):
    def test_bool_true(self):
        self.assertTrue(CapabilityDecision(permitted=True))

    def test_bool_false(self):
        self.assertFalse(CapabilityDecision(permitted=False))

    def test_to_dict(self):
        d = CapabilityDecision(
            permitted=False,
            reasons=("memory too low",),
            codes=(ReasonCode.MEMORY_BELOW_FLOOR,),
        )
        self.assertEqual(
            d.to_dict(),
            {
                "permitted": False,
                "reasons": ["memory too low"],
                "codes": ["memory_below_floor"],
            },
        )

    def test_frozen(self):
        from dataclasses import FrozenInstanceError
        d = CapabilityDecision(permitted=True)
        with self.assertRaises(FrozenInstanceError):
            d.permitted = False  # type: ignore[misc]


if __name__ == "__main__":
    unittest.main(verbosity=2)
