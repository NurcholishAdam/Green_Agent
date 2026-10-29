"""
HeliumPolicyAdapter — translate hardware capabilities into permitted
execution policies.

A pure, stateless translation layer between
:class:`HardwareCapability` (what a device reports) and
:class:`PermittedPolicy` (what the runtime is allowed to do).

Key guarantees
--------------
* **Fail-closed.** ``NaN`` / ``±inf`` numeric fields, out-of-range
  Helium availability, empty or non-string precision sets, and
  ``None`` capabilities are **denied** with a structured
  :class:`FallbackReason`. The adapter never emits a
  :class:`PermittedPolicy` whose limits are broken.
* **Immutable.** Both dataclasses are frozen; ``supported_precisions``
  and ``allowed_precisions`` / ``notes`` are tuples/frozensets, so
  governance output cannot be rewritten after the fact.
* **Per-instance configuration.** ``memory_headroom``,
  ``thermal_headroom_c``, and ``helium_fallback_threshold`` are
  instance fields (not class attributes), so two deployments running
  in the same process can apply different headroom without fighting
  over a shared global.
* **Semantic precision order.** ``fp32 > fp16 > int8 > int4``, so the
  first entry of ``allowed_precisions`` is the highest precision the
  device supports. Unknown precisions are preserved but placed at the
  end.
* **Structured fallback reason.** ``PermittedPolicy.fallback_reason``
  distinguishes "no fallback needed" from "Helium scarcity" from
  "invalid capability" from "no precision available", so downstream
  tooling does not have to parse ``notes`` strings.
* **Round-trippable.** ``to_dict`` / ``from_dict`` / ``to_schema`` on
  both dataclasses, consistent with the rest of the governance stack.
"""

from __future__ import annotations

import math
import unittest
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, FrozenSet, Iterable, List, Optional, Tuple


__all__ = [
    "FallbackReason",
    "HardwareCapability",
    "PermittedPolicy",
    "HeliumPolicyAdapter",
    "DEFAULT_PRECISION_ORDER",
    "DEFAULT_ADAPTER",
]


DEFAULT_PRECISION_ORDER: Tuple[str, ...] = ("fp32", "fp16", "int8", "int4")


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------
class FallbackReason(str, Enum):
    """Why a policy requires fallback (or not)."""

    NONE = "none"
    HELIUM_SCARCITY = "helium_scarcity"
    INVALID_CAPABILITY = "invalid_capability"
    NO_PRECISION = "no_precision"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _is_finite_number(value: Any) -> bool:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return False
    try:
        return math.isfinite(float(value))
    except (TypeError, ValueError, OverflowError):
        return False


def _validate_precisions(value: Any) -> FrozenSet[str]:
    if isinstance(value, str):
        raise TypeError(
            "supported_precisions must be an iterable of strings, "
            "not a single string; wrap in a list, e.g. "
            "supported_precisions=['fp16']"
        )
    if not isinstance(value, Iterable):
        raise TypeError(
            "supported_precisions must be iterable of strings"
        )
    result: set = set()
    for item in value:
        if not isinstance(item, str) or not item.strip():
            raise ValueError(
                f"supported_precisions entries must be non-empty "
                f"strings, got {item!r}"
            )
        result.add(item)
    return frozenset(result)


def _order_precisions(supported: FrozenSet[str]) -> Tuple[str, ...]:
    """Sort precisions by the canonical semantic order, then alphabetically."""
    order = {p: i for i, p in enumerate(DEFAULT_PRECISION_ORDER)}
    return tuple(sorted(
        supported,
        key=lambda p: (order.get(p, len(DEFAULT_PRECISION_ORDER)), p),
    ))


# ---------------------------------------------------------------------------
# HardwareCapability
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class HardwareCapability:
    """Detected capabilities of a target device."""

    device_id: str
    max_memory_mb: float
    supported_precisions: FrozenSet[str]
    max_thermal_c: Optional[float] = None
    has_tensor_cores: bool = False
    supports_int8: bool = False
    supports_int4: bool = False
    helium_availability: float = 1.0

    def __post_init__(self) -> None:
        if not isinstance(self.device_id, str) or not self.device_id.strip():
            raise ValueError("device_id must be a non-empty string")
        if not _is_finite_number(self.max_memory_mb):
            raise ValueError("max_memory_mb must be a finite number")
        if float(self.max_memory_mb) < 0:
            raise ValueError("max_memory_mb must be >= 0")
        object.__setattr__(
            self,
            "supported_precisions",
            _validate_precisions(self.supported_precisions),
        )
        if self.max_thermal_c is not None:
            if not _is_finite_number(self.max_thermal_c):
                raise ValueError(
                    "max_thermal_c must be a finite number or None"
                )
        if not _is_finite_number(self.helium_availability):
            raise ValueError(
                "helium_availability must be a finite number"
            )
        if not 0.0 <= float(self.helium_availability) <= 1.0:
            raise ValueError(
                "helium_availability must be in [0, 1]"
            )
        for flag_name in (
            "has_tensor_cores",
            "supports_int8",
            "supports_int4",
        ):
            if not isinstance(getattr(self, flag_name), bool):
                raise TypeError(f"{flag_name} must be a bool")

    # -- serialization -----------------------------------------------------
    def to_dict(self) -> Dict[str, Any]:
        return {
            "device_id": self.device_id,
            "max_memory_mb": self.max_memory_mb,
            "supported_precisions": sorted(self.supported_precisions),
            "max_thermal_c": self.max_thermal_c,
            "has_tensor_cores": self.has_tensor_cores,
            "supports_int8": self.supports_int8,
            "supports_int4": self.supports_int4,
            "helium_availability": self.helium_availability,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "HardwareCapability":
        if not isinstance(data, dict):
            raise TypeError(
                f"data must be a dict, got {type(data).__name__}"
            )
        return cls(
            device_id=data["device_id"],
            max_memory_mb=data["max_memory_mb"],
            supported_precisions=tuple(data.get(
                "supported_precisions", ()
            )),
            max_thermal_c=data.get("max_thermal_c"),
            has_tensor_cores=data.get("has_tensor_cores", False),
            supports_int8=data.get("supports_int8", False),
            supports_int4=data.get("supports_int4", False),
            helium_availability=data.get("helium_availability", 1.0),
        )


# ---------------------------------------------------------------------------
# PermittedPolicy
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class PermittedPolicy:
    """The execution policy permitted for a given capability."""

    device_id: str
    allowed_precisions: Tuple[str, ...]
    max_memory_mb: float
    max_thermal_c: Optional[float]
    fallback_reason: FallbackReason = FallbackReason.NONE
    notes: Tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.device_id, str) or not self.device_id.strip():
            raise ValueError("device_id must be a non-empty string")

        if not isinstance(self.allowed_precisions, tuple):
            object.__setattr__(
                self, "allowed_precisions",
                tuple(self.allowed_precisions),
            )
        for p in self.allowed_precisions:
            if not isinstance(p, str) or not p.strip():
                raise ValueError(
                    f"allowed_precisions entries must be non-empty "
                    f"strings, got {p!r}"
                )

        if not _is_finite_number(self.max_memory_mb):
            raise ValueError("max_memory_mb must be a finite number")
        if float(self.max_memory_mb) < 0:
            raise ValueError("max_memory_mb must be >= 0")

        if self.max_thermal_c is not None:
            if not _is_finite_number(self.max_thermal_c):
                raise ValueError(
                    "max_thermal_c must be a finite number or None"
                )
            if float(self.max_thermal_c) < 0:
                raise ValueError("max_thermal_c must be >= 0 or None")

        if not isinstance(self.fallback_reason, FallbackReason):
            object.__setattr__(
                self, "fallback_reason",
                FallbackReason(self.fallback_reason),
            )

        if not isinstance(self.notes, tuple):
            object.__setattr__(self, "notes", tuple(self.notes))
        for note in self.notes:
            if not isinstance(note, str):
                raise TypeError(
                    f"notes entries must be strings, got {note!r}"
                )

    # -- derived ----------------------------------------------------------
    @property
    def requires_fallback(self) -> bool:
        return self.fallback_reason is not FallbackReason.NONE

    def is_executable(self) -> bool:
        """True when at least one precision is available AND no fallback
        is required AND at least one memory byte is available."""
        return (
            self.fallback_reason is FallbackReason.NONE
            and bool(self.allowed_precisions)
            and self.max_memory_mb > 0
        )

    # -- factories --------------------------------------------------------
    @classmethod
    def denied(
        cls,
        device_id: str,
        reason: FallbackReason,
        notes: Iterable[str] = (),
    ) -> "PermittedPolicy":
        if reason is FallbackReason.NONE:
            raise ValueError(
                "PermittedPolicy.denied requires a non-NONE reason"
            )
        return cls(
            device_id=device_id or "unknown",
            allowed_precisions=(),
            max_memory_mb=0.0,
            max_thermal_c=None,
            fallback_reason=reason,
            notes=tuple(notes),
        )

    # -- serialization -----------------------------------------------------
    def to_dict(self) -> Dict[str, Any]:
        return {
            "device_id": self.device_id,
            "allowed_precisions": list(self.allowed_precisions),
            "max_memory_mb": self.max_memory_mb,
            "max_thermal_c": self.max_thermal_c,
            "fallback_reason": self.fallback_reason.value,
            "requires_fallback": self.requires_fallback,
            "notes": list(self.notes),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PermittedPolicy":
        if not isinstance(data, dict):
            raise TypeError(
                f"data must be a dict, got {type(data).__name__}"
            )
        return cls(
            device_id=data["device_id"],
            allowed_precisions=tuple(
                data.get("allowed_precisions", ())
            ),
            max_memory_mb=data["max_memory_mb"],
            max_thermal_c=data.get("max_thermal_c"),
            fallback_reason=FallbackReason(
                data.get("fallback_reason", "none")
            ),
            notes=tuple(data.get("notes", ())),
        )

    def to_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "required": [
                "device_id",
                "allowed_precisions",
                "max_memory_mb",
                "fallback_reason",
            ],
            "properties": {
                "device_id": {"type": "string", "minLength": 1},
                "allowed_precisions": {
                    "type": "array",
                    "items": {"type": "string", "minLength": 1},
                },
                "max_memory_mb": {"type": "number", "minimum": 0},
                "max_thermal_c": {
                    "type": ["number", "null"],
                    "minimum": 0,
                },
                "fallback_reason": {
                    "type": "string",
                    "enum": [r.value for r in FallbackReason],
                },
                "notes": {
                    "type": "array",
                    "items": {"type": "string"},
                },
            },
            "additionalProperties": False,
        }

    # -- misc -------------------------------------------------------------
    def __str__(self) -> str:
        return (
            f"PermittedPolicy({self.device_id}: "
            f"precisions={list(self.allowed_precisions)}, "
            f"mem={self.max_memory_mb}MB, "
            f"fallback={self.fallback_reason.value})"
        )


# ---------------------------------------------------------------------------
# HeliumPolicyAdapter
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class HeliumPolicyAdapter:
    """
    Translate hardware capability into an execution policy.

    The policy:
        * Constrains precision to what the device supports.
        * Preserves a memory headroom.
        * Preserves a thermal headroom.
        * Flags when helium scarcity requires fallback.

    Headroom values are instance fields so two deployments in the same
    process can apply different values without conflicting.
    """

    memory_headroom: float = 0.15            # 15% margin
    thermal_headroom_c: float = 5.0
    helium_fallback_threshold: float = 0.4   # below this, force fallback

    # -- invariants --------------------------------------------------------
    def __post_init__(self) -> None:
        if not _is_finite_number(self.memory_headroom):
            raise ValueError("memory_headroom must be a finite number")
        if not 0.0 <= float(self.memory_headroom) < 1.0:
            raise ValueError("memory_headroom must be in [0, 1)")

        if not _is_finite_number(self.thermal_headroom_c):
            raise ValueError(
                "thermal_headroom_c must be a finite number"
            )
        if float(self.thermal_headroom_c) < 0:
            raise ValueError("thermal_headroom_c must be >= 0")

        if not _is_finite_number(self.helium_fallback_threshold):
            raise ValueError(
                "helium_fallback_threshold must be a finite number"
            )
        if not 0.0 <= float(self.helium_fallback_threshold) <= 1.0:
            raise ValueError(
                "helium_fallback_threshold must be in [0, 1]"
            )

    # -- adaptation --------------------------------------------------------
    def adapt(self, capability: Any) -> PermittedPolicy:
        """Return the execution policy for ``capability``.

        A malformed capability or a device with no usable precision
        produces a **denied** :class:`PermittedPolicy`; it never
        produces a policy with broken numeric limits.
        """
        # -- 1. Type / value validation --------------------------------
        if not isinstance(capability, HardwareCapability):
            return PermittedPolicy.denied(
                getattr(capability, "device_id", "unknown") or "unknown",
                FallbackReason.INVALID_CAPABILITY,
                notes=[
                    f"capability must be HardwareCapability, got "
                    f"{type(capability).__name__}"
                ],
            )

        # Redundant with __post_init__ but defensive against subclasses.
        if not _is_finite_number(capability.max_memory_mb) or float(
            capability.max_memory_mb
        ) < 0:
            return PermittedPolicy.denied(
                capability.device_id,
                FallbackReason.INVALID_CAPABILITY,
                notes=["max_memory_mb is not a valid number"],
            )
        if not (
            0.0 <= float(capability.helium_availability) <= 1.0
        ):
            return PermittedPolicy.denied(
                capability.device_id,
                FallbackReason.INVALID_CAPABILITY,
                notes=["helium_availability is out of range"],
            )

        # -- 2. Precision set ------------------------------------------
        if not capability.supported_precisions:
            return PermittedPolicy.denied(
                capability.device_id,
                FallbackReason.NO_PRECISION,
                notes=["device supports no execution precision"],
            )
        allowed = _order_precisions(capability.supported_precisions)

        # -- 3. Memory ------------------------------------------------
        memory = float(capability.max_memory_mb) * (
            1.0 - float(self.memory_headroom)
        )

        # -- 4. Thermal (clamp, never negative) ------------------------
        notes: List[str] = []
        thermal: Optional[float]
        if capability.max_thermal_c is not None:
            thermal = float(capability.max_thermal_c) - float(
                self.thermal_headroom_c
            )
            if thermal < 0.0:
                notes.append(
                    "thermal limit below headroom; clamped to 0"
                )
                thermal = 0.0
        else:
            thermal = None

        # -- 5. Helium: positive predicate so NaN fails closed ---------
        # Use `not (helium >= threshold)` so a NaN helium (which would
        # make `nan < threshold` False) engages fallback.
        helium = float(capability.helium_availability)
        scarcity = not (helium >= float(self.helium_fallback_threshold))
        reason = (
            FallbackReason.HELIUM_SCARCITY
            if scarcity
            else FallbackReason.NONE
        )
        if scarcity:
            notes.append(
                "helium scarcity below threshold; force fallback"
            )
        if not capability.has_tensor_cores:
            notes.append("no tensor cores; FP16 may not accelerate")

        # -- 6. Build --------------------------------------------------
        return PermittedPolicy(
            device_id=capability.device_id,
            allowed_precisions=allowed,
            max_memory_mb=memory,
            max_thermal_c=thermal,
            fallback_reason=reason,
            notes=tuple(notes),
        )

    # -- introspection -----------------------------------------------------
    def describe(self) -> str:
        return (
            f"HeliumPolicyAdapter("
            f"memory_headroom={self.memory_headroom:.0%}, "
            f"thermal_headroom={self.thermal_headroom_c}°C, "
            f"helium_fallback={self.helium_fallback_threshold})"
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "memory_headroom": self.memory_headroom,
            "thermal_headroom_c": self.thermal_headroom_c,
            "helium_fallback_threshold": self.helium_fallback_threshold,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "HeliumPolicyAdapter":
        if not isinstance(data, dict):
            raise TypeError(
                f"data must be a dict, got {type(data).__name__}"
            )
        return cls(
            memory_headroom=data.get("memory_headroom", 0.15),
            thermal_headroom_c=data.get("thermal_headroom_c", 5.0),
            helium_fallback_threshold=data.get(
                "helium_fallback_threshold", 0.4
            ),
        )

    def to_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "required": [
                "memory_headroom",
                "thermal_headroom_c",
                "helium_fallback_threshold",
            ],
            "properties": {
                "memory_headroom": {
                    "type": "number", "minimum": 0, "maximum": 0.999,
                },
                "thermal_headroom_c": {
                    "type": "number", "minimum": 0,
                },
                "helium_fallback_threshold": {
                    "type": "number", "minimum": 0, "maximum": 1,
                },
            },
            "additionalProperties": False,
        }

    def __str__(self) -> str:
        return self.describe()

    def __repr__(self) -> str:
        return self.describe()


DEFAULT_ADAPTER: HeliumPolicyAdapter = HeliumPolicyAdapter()


# ===========================================================================
# TESTS
# ===========================================================================
def _cap(**kwargs: Any) -> HardwareCapability:
    defaults: Dict[str, Any] = dict(
        device_id="dev-1",
        max_memory_mb=2048.0,
        supported_precisions={"fp16", "int8"},
        max_thermal_c=45.0,
        has_tensor_cores=True,
        helium_availability=0.9,
    )
    defaults.update(kwargs)
    return HardwareCapability(**defaults)


class TestHardwareCapabilityValidation(unittest.TestCase):
    def test_ok(self):
        c = _cap()
        self.assertIsInstance(c.supported_precisions, frozenset)

    def test_empty_device_id(self):
        with self.assertRaises(ValueError):
            _cap(device_id="")

    def test_whitespace_device_id(self):
        with self.assertRaises(ValueError):
            _cap(device_id="   ")

    def test_negative_memory(self):
        with self.assertRaises(ValueError):
            _cap(max_memory_mb=-1)

    def test_nan_memory(self):
        with self.assertRaises(ValueError):
            _cap(max_memory_mb=float("nan"))

    def test_inf_memory(self):
        with self.assertRaises(ValueError):
            _cap(max_memory_mb=float("inf"))

    def test_nan_thermal(self):
        with self.assertRaises(ValueError):
            _cap(max_thermal_c=float("nan"))

    def test_helium_above_one(self):
        with self.assertRaises(ValueError):
            _cap(helium_availability=1.5)

    def test_helium_below_zero(self):
        with self.assertRaises(ValueError):
            _cap(helium_availability=-0.1)

    def test_nan_helium(self):
        with self.assertRaises(ValueError):
            _cap(helium_availability=float("nan"))

    def test_single_string_precisions_rejected(self):
        with self.assertRaises(TypeError):
            _cap(supported_precisions="fp16")  # type: ignore[arg-type]

    def test_empty_precision_entry_rejected(self):
        with self.assertRaises(ValueError):
            _cap(supported_precisions={"fp16", ""})

    def test_non_string_precision_rejected(self):
        with self.assertRaises(ValueError):
            _cap(supported_precisions={"fp16", 42})  # type: ignore[set-item]

    def test_non_bool_flag_rejected(self):
        with self.assertRaises(TypeError):
            _cap(has_tensor_cores=1)  # type: ignore[arg-type]

    def test_frozen(self):
        from dataclasses import FrozenInstanceError
        c = _cap()
        with self.assertRaises(FrozenInstanceError):
            c.device_id = "other"  # type: ignore[misc]

    def test_hashable(self):
        self.assertEqual({_cap(), _cap()}, {_cap()})


class TestAdapterConstruction(unittest.TestCase):
    def test_defaults(self):
        a = HeliumPolicyAdapter()
        self.assertEqual(a.memory_headroom, 0.15)
        self.assertEqual(a.thermal_headroom_c, 5.0)
        self.assertEqual(a.helium_fallback_threshold, 0.4)

    def test_memory_headroom_one_rejected(self):
        with self.assertRaises(ValueError):
            HeliumPolicyAdapter(memory_headroom=1.0)

    def test_memory_headroom_negative_rejected(self):
        with self.assertRaises(ValueError):
            HeliumPolicyAdapter(memory_headroom=-0.1)

    def test_memory_headroom_nan_rejected(self):
        with self.assertRaises(ValueError):
            HeliumPolicyAdapter(memory_headroom=float("nan"))

    def test_thermal_headroom_negative_rejected(self):
        with self.assertRaises(ValueError):
            HeliumPolicyAdapter(thermal_headroom_c=-1)

    def test_helium_threshold_above_one_rejected(self):
        with self.assertRaises(ValueError):
            HeliumPolicyAdapter(helium_fallback_threshold=1.5)

    def test_helium_threshold_below_zero_rejected(self):
        with self.assertRaises(ValueError):
            HeliumPolicyAdapter(helium_fallback_threshold=-0.1)

    def test_frozen(self):
        from dataclasses import FrozenInstanceError
        a = HeliumPolicyAdapter()
        with self.assertRaises(FrozenInstanceError):
            a.memory_headroom = 0.5  # type: ignore[misc]

    def test_two_instances_independent(self):
        # Regression for the old class-attribute bug.
        a1 = HeliumPolicyAdapter(memory_headroom=0.15)
        a2 = HeliumPolicyAdapter(memory_headroom=0.50)
        cap = _cap(max_memory_mb=1000.0)
        self.assertAlmostEqual(a1.adapt(cap).max_memory_mb, 850.0)
        self.assertAlmostEqual(a2.adapt(cap).max_memory_mb, 500.0)

    def test_default_adapter_singleton(self):
        self.assertIsInstance(DEFAULT_ADAPTER, HeliumPolicyAdapter)


class TestAdapterHappyPath(unittest.TestCase):
    def test_memory_headroom_applied(self):
        p = HeliumPolicyAdapter().adapt(_cap(max_memory_mb=1000.0))
        self.assertAlmostEqual(p.max_memory_mb, 850.0)

    def test_thermal_headroom_applied(self):
        p = HeliumPolicyAdapter().adapt(_cap(max_thermal_c=45.0))
        self.assertEqual(p.max_thermal_c, 40.0)

    def test_thermal_none_passthrough(self):
        p = HeliumPolicyAdapter().adapt(_cap(max_thermal_c=None))
        self.assertIsNone(p.max_thermal_c)

    def test_thermal_clamped_to_zero(self):
        # Regression: original could produce a negative thermal limit.
        p = HeliumPolicyAdapter().adapt(_cap(max_thermal_c=3.0))
        self.assertEqual(p.max_thermal_c, 0.0)
        self.assertTrue(any("clamped" in n for n in p.notes))

    def test_precision_order_semantic(self):
        # fp32 > fp16 > int8 > int4
        cap = _cap(supported_precisions={"int8", "fp16", "fp32"})
        p = HeliumPolicyAdapter().adapt(cap)
        self.assertEqual(p.allowed_precisions, ("fp32", "fp16", "int8"))

    def test_precision_unknown_preserved_at_end(self):
        cap = _cap(supported_precisions={"fp16", "bf16"})
        p = HeliumPolicyAdapter().adapt(cap)
        self.assertEqual(p.allowed_precisions[0], "fp16")
        self.assertIn("bf16", p.allowed_precisions)

    def test_no_tensor_cores_note(self):
        p = HeliumPolicyAdapter().adapt(_cap(has_tensor_cores=False))
        self.assertTrue(any("tensor cores" in n for n in p.notes))
        self.assertFalse(p.requires_fallback)


class TestAdapterFailClosed(unittest.TestCase):
    def test_none_capability(self):
        p = HeliumPolicyAdapter().adapt(None)
        self.assertTrue(p.requires_fallback)
        self.assertIs(
            p.fallback_reason, FallbackReason.INVALID_CAPABILITY
        )
        self.assertEqual(p.allowed_precisions, ())

    def test_wrong_type(self):
        p = HeliumPolicyAdapter().adapt("not a capability")
        self.assertIs(
            p.fallback_reason, FallbackReason.INVALID_CAPABILITY
        )

    def test_empty_precisions_denied(self):
        cap = _cap(supported_precisions=set())
        p = HeliumPolicyAdapter().adapt(cap)
        self.assertIs(p.fallback_reason, FallbackReason.NO_PRECISION)
        self.assertEqual(p.allowed_precisions, ())

    def test_nan_helium_would_be_denied_at_construction(self):
        # Because __post_init__ rejects NaN, the adapter never sees it.
        with self.assertRaises(ValueError):
            _cap(helium_availability=float("nan"))

    def test_helium_at_threshold_passes(self):
        p = HeliumPolicyAdapter().adapt(
            _cap(helium_availability=0.4)
        )
        self.assertIs(p.fallback_reason, FallbackReason.NONE)

    def test_helium_below_threshold_falls_back(self):
        p = HeliumPolicyAdapter().adapt(
            _cap(helium_availability=0.39)
        )
        self.assertIs(p.fallback_reason, FallbackReason.HELIUM_SCARCITY)
        self.assertTrue(p.requires_fallback)
        self.assertTrue(any("scarcity" in n for n in p.notes))


class TestPermittedPolicy(unittest.TestCase):
    def test_requires_fallback_derived(self):
        p = PermittedPolicy(
            device_id="d", allowed_precisions=("fp16",),
            max_memory_mb=100.0, max_thermal_c=None,
            fallback_reason=FallbackReason.NONE,
        )
        self.assertFalse(p.requires_fallback)
        p2 = PermittedPolicy(
            device_id="d", allowed_precisions=("fp16",),
            max_memory_mb=100.0, max_thermal_c=None,
            fallback_reason=FallbackReason.HELIUM_SCARCITY,
        )
        self.assertTrue(p2.requires_fallback)

    def test_denied_factory(self):
        p = PermittedPolicy.denied(
            "d", FallbackReason.INVALID_CAPABILITY,
        )
        self.assertTrue(p.requires_fallback)
        self.assertEqual(p.allowed_precisions, ())
        self.assertFalse(p.is_executable())

    def test_denied_requires_non_none_reason(self):
        with self.assertRaises(ValueError):
            PermittedPolicy.denied("d", FallbackReason.NONE)

    def test_is_executable(self):
        p = PermittedPolicy(
            device_id="d", allowed_precisions=("fp16",),
            max_memory_mb=100.0, max_thermal_c=None,
        )
        self.assertTrue(p.is_executable())

    def test_not_executable_when_empty(self):
        p = PermittedPolicy(
            device_id="d", allowed_precisions=(),
            max_memory_mb=100.0, max_thermal_c=None,
        )
        self.assertFalse(p.is_executable())

    def test_empty_device_id_rejected(self):
        with self.assertRaises(ValueError):
            PermittedPolicy(
                device_id="", allowed_precisions=("fp16",),
                max_memory_mb=100.0, max_thermal_c=None,
            )

    def test_negative_memory_rejected(self):
        with self.assertRaises(ValueError):
            PermittedPolicy(
                device_id="d", allowed_precisions=("fp16",),
                max_memory_mb=-1.0, max_thermal_c=None,
            )

    def test_negative_thermal_rejected(self):
        with self.assertRaises(ValueError):
            PermittedPolicy(
                device_id="d", allowed_precisions=("fp16",),
                max_memory_mb=100.0, max_thermal_c=-5.0,
            )

    def test_frozen(self):
        from dataclasses import FrozenInstanceError
        p = PermittedPolicy(
            device_id="d", allowed_precisions=("fp16",),
            max_memory_mb=100.0, max_thermal_c=None,
        )
        with self.assertRaises(FrozenInstanceError):
            p.device_id = "x"  # type: ignore[misc]

    def test_notes_copied_to_tuple(self):
        src = ["n1"]
        p = PermittedPolicy(
            device_id="d", allowed_precisions=("fp16",),
            max_memory_mb=100.0, max_thermal_c=None,
            notes=src,  # type: ignore[arg-type]
        )
        src.append("n2")
        self.assertEqual(p.notes, ("n1",))

    def test_str(self):
        p = PermittedPolicy(
            device_id="dev",
            allowed_precisions=("fp16",),
            max_memory_mb=100.0,
            max_thermal_c=None,
        )
        s = str(p)
        self.assertIn("dev", s)
        self.assertIn("fp16", s)


class TestSerialization(unittest.TestCase):
    def test_capability_round_trip(self):
        c = _cap()
        d = c.to_dict()
        c2 = HardwareCapability.from_dict(d)
        self.assertEqual(c, c2)

    def test_policy_round_trip(self):
        p = HeliumPolicyAdapter().adapt(_cap())
        p2 = PermittedPolicy.from_dict(p.to_dict())
        self.assertEqual(p.to_dict(), p2.to_dict())

    def test_policy_from_dict_defaults(self):
        p = PermittedPolicy.from_dict({
            "device_id": "d",
            "max_memory_mb": 100.0,
        })
        self.assertEqual(p.fallback_reason, FallbackReason.NONE)
        self.assertEqual(p.allowed_precisions, ())

    def test_policy_to_dict_json_safe(self):
        import json
        p = HeliumPolicyAdapter().adapt(_cap())
        json.dumps(p.to_dict())  # must not raise

    def test_adapter_round_trip(self):
        a = HeliumPolicyAdapter(memory_headroom=0.1, thermal_headroom_c=3.0)
        a2 = HeliumPolicyAdapter.from_dict(a.to_dict())
        self.assertEqual(a, a2)

    def test_schemas(self):
        self.assertIn("required", PermittedPolicy(
            device_id="d", allowed_precisions=(), max_memory_mb=0.0,
            max_thermal_c=None,
        ).to_schema())
        self.assertIn("required", HeliumPolicyAdapter().to_schema())


class TestDescribe(unittest.TestCase):
    def test_describe_mentions_headroom(self):
        s = HeliumPolicyAdapter().describe()
        self.assertIn("memory_headroom", s)
        self.assertIn("thermal_headroom", s)
        self.assertIn("helium_fallback", s)

    def test_str_matches_describe(self):
        a = HeliumPolicyAdapter()
        self.assertEqual(str(a), a.describe())
        self.assertEqual(repr(a), a.describe())


if __name__ == "__main__":
    unittest.main(verbosity=2)
