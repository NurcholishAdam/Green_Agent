"""
ReproducibilityRules — what a benchmark run must declare to be
considered reproducible.

A policy is a tuple of :class:`FieldRule` objects. Each rule describes
one submission field and how to validate it (presence, emptiness,
type, allowed values, regex pattern). The registry is immutable once
constructed, so it is safe to share across threads and across
modules.

Example
-------
>>> rules = ReproducibilityRules()
>>> rules.is_complete({
...     "system_version": "1.4.2",
...     "policy_version": "2024-06",
...     "dataset_id": "ds-abc",
...     "random_seed": 0,
...     "hardware_profile": "a100-80g",
...     "region": "us-east-1",
... })
True
>>> rules.missing({"dataset_id": "ds-abc"})
['system_version', 'policy_version', 'random_seed', 'hardware_profile', 'region']
"""

from __future__ import annotations

import re
import threading
from dataclasses import dataclass
from typing import (
    Any,
    Dict,
    Iterable,
    Iterator,
    List,
    Literal,
    Mapping,
    NamedTuple,
    Optional,
    Tuple,
)

__all__ = [
    "FieldRule",
    "FieldViolation",
    "ReproducibilityError",
    "ReproducibilityRules",
    "DEFAULT_RULES",
]


# ---------------------------------------------------------------------------
# Violations
# ---------------------------------------------------------------------------
ViolationCode = Literal[
    "missing",
    "empty",
    "wrong_type",
    "not_in_choices",
    "pattern_mismatch",
]


class FieldViolation(NamedTuple):
    """A single validation finding for one submission field."""

    field: str
    code: ViolationCode
    message: str


class ReproducibilityError(ValueError):
    """Aggregate error raised by :meth:`ReproducibilityRules.validate_or_raise`."""

    def __init__(self, violations: List[FieldViolation]) -> None:
        self.violations = list(violations)
        super().__init__("; ".join(v.message for v in self.violations))


# ---------------------------------------------------------------------------
# Blankness helper
# ---------------------------------------------------------------------------
def _is_blank(value: Any) -> bool:
    """True when a value counts as 'not declared'.

    IMPORTANT: ``0``, ``0.0``, and ``False`` are *not* blank. A
    ``random_seed=0`` submission is valid and must not be rejected.
    """
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() == ""
    if isinstance(value, (bytes, bytearray)):
        return len(value) == 0
    return False


# ---------------------------------------------------------------------------
# Field rules
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class FieldRule:
    """Immutable description of how to validate one submission field."""

    name: str
    required: bool = True
    allow_empty: bool = False
    expected_type: Optional[type] = None
    choices: Optional[Tuple[str, ...]] = None
    pattern: Optional[re.Pattern[str]] = None

    def __post_init__(self) -> None:
        if not self.name or not self.name.strip():
            raise ValueError("FieldRule.name must be non-empty")
        if self.choices is not None:
            object.__setattr__(self, "choices", tuple(self.choices))


# ---------------------------------------------------------------------------
# Policy
# ---------------------------------------------------------------------------
_DEFAULT_FIELDS: Tuple[FieldRule, ...] = (
    FieldRule("system_version"),
    FieldRule("policy_version"),
    FieldRule("dataset_id"),
    FieldRule("random_seed", expected_type=int),
    FieldRule("hardware_profile"),
    FieldRule("region"),
)


class ReproducibilityRules:
    """Immutable, thread-safe description of a reproducibility policy."""

    DEFAULT_FIELDS: Tuple[FieldRule, ...] = _DEFAULT_FIELDS

    def __init__(self, fields: Optional[Iterable[FieldRule]] = None) -> None:
        rules = tuple(self.DEFAULT_FIELDS if fields is None else fields)
        if not rules:
            raise ValueError("at least one FieldRule is required")

        seen: set[str] = set()
        for rule in rules:
            if not isinstance(rule, FieldRule):
                raise TypeError(
                    f"expected FieldRule, got {type(rule).__name__}"
                )
            if rule.name in seen:
                raise ValueError(f"duplicate FieldRule for '{rule.name}'")
            seen.add(rule.name)

        self._fields: Tuple[FieldRule, ...] = rules
        self._by_name: Dict[str, FieldRule] = {r.name: r for r in rules}
        self._lock = threading.RLock()

    # -- read-only views ---------------------------------------------------
    @property
    def fields(self) -> Tuple[FieldRule, ...]:
        return self._fields

    @property
    def required_fields(self) -> Tuple[str, ...]:
        return tuple(r.name for r in self._fields if r.required)

    def __len__(self) -> int:
        return len(self._fields)

    def __iter__(self) -> Iterator[FieldRule]:
        return iter(self._fields)

    def __contains__(self, name: object) -> bool:
        return name in self._by_name

    # -- validation --------------------------------------------------------
    def check(self, submission: Mapping[str, Any]) -> List[FieldViolation]:
        """Return structured violations (empty list means valid)."""
        if not isinstance(submission, Mapping):
            raise TypeError(
                f"submission must be a Mapping, got {type(submission).__name__}"
            )

        violations: List[FieldViolation] = []

        for rule in self._fields:
            if rule.name not in submission:
                if rule.required:
                    violations.append(
                        FieldViolation(
                            rule.name,
                            "missing",
                            f"field '{rule.name}' is required",
                        )
                    )
                continue

            value = submission[rule.name]

            if _is_blank(value):
                if rule.required and not rule.allow_empty:
                    violations.append(
                        FieldViolation(
                            rule.name,
                            "empty",
                            f"field '{rule.name}' must not be empty",
                        )
                    )
                continue

            if (
                rule.expected_type is not None
                and not isinstance(value, rule.expected_type)
            ):
                violations.append(
                    FieldViolation(
                        rule.name,
                        "wrong_type",
                        f"field '{rule.name}' must be "
                        f"{rule.expected_type.__name__}, "
                        f"got {type(value).__name__}",
                    )
                )
                continue

            if rule.choices is not None and value not in rule.choices:
                violations.append(
                    FieldViolation(
                        rule.name,
                        "not_in_choices",
                        f"field '{rule.name}' must be one of "
                        f"{rule.choices!r}, got {value!r}",
                    )
                )
                continue

            if (
                rule.pattern is not None
                and isinstance(value, str)
                and not rule.pattern.match(value)
            ):
                violations.append(
                    FieldViolation(
                        rule.name,
                        "pattern_mismatch",
                        f"field '{rule.name}' does not match "
                        f"{rule.pattern.pattern!r}",
                    )
                )

        return violations

    # -- backward-compatible wrapper --------------------------------------
    def validate(self, submission: Mapping[str, Any]) -> List[str]:
        """Return a list of human-readable error messages (legacy API)."""
        return [v.message for v in self.check(submission)]

    def is_complete(self, submission: Mapping[str, Any]) -> bool:
        return not self.check(submission)

    def validate_or_raise(self, submission: Mapping[str, Any]) -> None:
        violations = self.check(submission)
        if violations:
            raise ReproducibilityError(violations)

    # -- convenience -------------------------------------------------------
    def missing(self, submission: Mapping[str, Any]) -> List[str]:
        """Field names that are missing or blank, in policy order."""
        return [
            v.field
            for v in self.check(submission)
            if v.code in ("missing", "empty")
        ]

    def present(self, submission: Mapping[str, Any]) -> Dict[str, Any]:
        """Fields declared by the policy that are present and non-blank."""
        return {
            name: submission[name]
            for name in self._by_name
            if name in submission and not _is_blank(submission[name])
        }

    # -- schema export -----------------------------------------------------
    def to_schema(self) -> Dict[str, Any]:
        """Return a JSON-schema-shaped dict describing this policy."""
        properties: Dict[str, Dict[str, Any]] = {}
        for rule in self._fields:
            prop: Dict[str, Any] = {
                "type": (
                    rule.expected_type.__name__
                    if rule.expected_type is not None
                    else "string"
                )
            }
            if rule.choices is not None:
                prop["enum"] = list(rule.choices)
            if rule.pattern is not None:
                prop["pattern"] = rule.pattern.pattern
            properties[rule.name] = prop

        schema: Dict[str, Any] = {
            "type": "object",
            "required": [r.name for r in self._fields if r.required],
            "properties": properties,
        }
        return schema

    # -- repr --------------------------------------------------------------
    def __repr__(self) -> str:
        names = ", ".join(r.name for r in self._fields)
        return f"ReproducibilityRules([{names}])"


# ---------------------------------------------------------------------------
# Convenience singleton
# ---------------------------------------------------------------------------
DEFAULT_RULES: ReproducibilityRules = ReproducibilityRules()
