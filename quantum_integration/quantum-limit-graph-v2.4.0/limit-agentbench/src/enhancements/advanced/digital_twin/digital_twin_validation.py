# src/quantum_integration/digital_twin/digital_twin_validation.py

"""Scenario parameter validation with type and range checks.

Overview
--------
* :class:`ParamSpec` — a self-describing schema entry: type, inclusive
  bounds, and whether the parameter is required.
* :class:`ValidationIssue` — a structured record of a single problem,
  with a stable ``code``, the field name, a human-readable message, and
  a severity (``"error"`` or ``"warning"``).
* :class:`ValidationResult` — a frozen, hashable outcome. ``errors``
  and ``warnings`` are retained as plain string tuples for backward
  compatibility; ``issues`` is the structured view.
* :class:`ScenarioParameterValidator` — validates a parameter mapping
  against the schema for a scenario. Supports a ``strict`` mode that
  promotes unknown keys to errors, and a ``validate_many`` batch API.

Schema hygiene
--------------
The schema is validated at import time: every ``ParamSpec`` must have a
supported type, bounds must satisfy ``lo <= hi``, and string parameters
must not declare numeric bounds. A bad schema fails loudly on import
rather than at first validation.

Finiteness
----------
All numeric values must be finite. ``NaN`` and ``±inf`` are rejected
with :class:`DigitalTwinInputError` before any range check.
"""

from __future__ import annotations

import json
import logging
import math
from collections.abc import Mapping as ABCMapping
from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, Optional, Tuple

from .digital_twin_errors import (
    DigitalTwinInputError,
    DigitalTwinParseError,
)
from .digital_twin_helpers import (
    _deep_freeze,
    _hashable,
    _is_real_int,
    _to_plain,
)
from .digital_twin_schemas import SimulationScenario

logger = logging.getLogger(__name__)
SCHEMA_VERSION: int = 2

_SUPPORTED_TYPES: Tuple[type, ...] = (float, int, str)


# --------------------------------------------------------------------------- #
# ParamSpec
# --------------------------------------------------------------------------- #

@dataclass(frozen=True)
class ParamSpec:
    """A single parameter's schema entry.

    Parameters
    ----------
    type:
        One of ``float``, ``int``, or ``str``.
    lo, hi:
        Inclusive numeric bounds. ``None`` skips the bound. Must be
        ``None`` for ``str`` parameters.
    required:
        When ``True``, the parameter must be present.
    """

    type: type
    lo: Optional[float] = None
    hi: Optional[float] = None
    required: bool = False

    def __post_init__(self) -> None:
        if self.type not in _SUPPORTED_TYPES:
            raise DigitalTwinInputError(
                f"ParamSpec.type must be one of "
                f"{[t.__name__ for t in _SUPPORTED_TYPES]}, "
                f"got {self.type!r}."
            )
        if self.lo is not None:
            if isinstance(self.lo, bool) or not isinstance(
                self.lo, (int, float),
            ):
                raise DigitalTwinInputError(
                    "ParamSpec.lo must be numeric or None."
                )
            if not math.isfinite(self.lo):
                raise DigitalTwinInputError("ParamSpec.lo must be finite.")
        if self.hi is not None:
            if isinstance(self.hi, bool) or not isinstance(
                self.hi, (int, float),
            ):
                raise DigitalTwinInputError(
                    "ParamSpec.hi must be numeric or None."
                )
            if not math.isfinite(self.hi):
                raise DigitalTwinInputError("ParamSpec.hi must be finite.")
        if (
            self.lo is not None
            and self.hi is not None
            and self.lo > self.hi
        ):
            raise DigitalTwinInputError(
                f"ParamSpec requires lo <= hi, got lo={self.lo!r}, "
                f"hi={self.hi!r}."
            )
        if self.type is str and (
            self.lo is not None or self.hi is not None
        ):
            raise DigitalTwinInputError(
                "str parameters must not declare numeric bounds."
            )

    def as_tuple(self) -> Tuple[type, Optional[float], Optional[float], bool]:
        """Return the legacy tuple form ``(type, lo, hi, required)``."""
        return (self.type, self.lo, self.hi, self.required)


# --------------------------------------------------------------------------- #
# ValidationIssue
# --------------------------------------------------------------------------- #

@dataclass(frozen=True)
class ValidationIssue:
    """Structured description of a single validation problem."""

    code: str
    field: str
    message: str
    severity: str = "error"

    def __post_init__(self) -> None:
        for name in ("code", "field", "message", "severity"):
            v = getattr(self, name)
            if not isinstance(v, str) or not v:
                raise DigitalTwinInputError(
                    f"ValidationIssue.{name} must be a non-empty string."
                )
        if self.severity not in ("error", "warning"):
            raise DigitalTwinInputError(
                f"ValidationIssue.severity must be 'error' or 'warning', "
                f"got {self.severity!r}."
            )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "code": self.code,
            "field": self.field,
            "message": self.message,
            "severity": self.severity,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ValidationIssue":
        if not isinstance(data, ABCMapping):
            raise DigitalTwinParseError(
                "ValidationIssue.from_dict expects a Mapping."
            )
        try:
            return cls(
                code=str(data["code"]),
                field=str(data["field"]),
                message=str(data["message"]),
                severity=str(data.get("severity", "error")),
            )
        except KeyError as exc:
            raise DigitalTwinParseError(
                f"ValidationIssue missing key {exc.args[0]!r}."
            ) from exc


# --------------------------------------------------------------------------- #
# ValidationResult
# --------------------------------------------------------------------------- #

@dataclass(frozen=True)
class ValidationResult:
    """Frozen, hashable outcome of a validation pass.

    ``normalized`` is a read-only mapping. Callers who need a mutable
    view should take ``dict(result.normalized)``. ``errors`` and
    ``warnings`` are plain string tuples retained for backward
    compatibility; ``issues`` is the structured view with codes and
    fields.
    """

    ok: bool
    errors: Tuple[str, ...] = ()
    warnings: Tuple[str, ...] = ()
    normalized: Mapping[str, Any] = field(default_factory=dict)
    issues: Tuple[ValidationIssue, ...] = ()
    schema_version: int = SCHEMA_VERSION

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "errors",
            tuple(str(e) for e in self.errors),
        )
        object.__setattr__(
            self, "warnings",
            tuple(str(w) for w in self.warnings),
        )
        object.__setattr__(
            self, "normalized",
            _deep_freeze(dict(self.normalized)),
        )
        object.__setattr__(self, "issues", tuple(self.issues))

    def __hash__(self) -> int:
        # ``normalized`` is a MappingProxyType (unhashable); project
        # through ``_hashable``. Same for the issues tuple (dataclasses
        # are hashable because their fields are hashable).
        return hash((
            self.ok,
            self.errors,
            self.warnings,
            _hashable(self.normalized),
            self.issues,
            self.schema_version,
        ))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "ok": self.ok,
            "errors": list(self.errors),
            "warnings": list(self.warnings),
            "normalized": _to_plain(self.normalized),
            "issues": [issue.to_dict() for issue in self.issues],
            "schema_version": self.schema_version,
        }

    def to_json(self, *, indent: Optional[int] = None) -> str:
        return json.dumps(self.to_dict(), default=str, indent=indent)

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any], *, strict: bool = False,
    ) -> "ValidationResult":
        if not isinstance(data, ABCMapping):
            raise DigitalTwinParseError(
                "ValidationResult.from_dict expects a Mapping."
            )
        version = int(data.get("schema_version", SCHEMA_VERSION))
        if version > SCHEMA_VERSION:
            raise DigitalTwinParseError(
                f"schema_version {version} is newer than supported "
                f"({SCHEMA_VERSION})."
            )
        raw_issues = data.get("issues") or ()
        issues: List[ValidationIssue] = []
        for i, i_data in enumerate(raw_issues):
            try:
                issues.append(ValidationIssue.from_dict(i_data))
            except DigitalTwinParseError as exc:
                if strict:
                    raise DigitalTwinParseError(
                        f"issues[{i}]: {exc}"
                    ) from exc
                logger.warning("Skipping malformed issue %d: %s", i, exc)
        return cls(
            ok=bool(data.get("ok", False)),
            errors=tuple(data.get("errors") or ()),
            warnings=tuple(data.get("warnings") or ()),
            normalized=dict(data.get("normalized") or {}),
            issues=tuple(issues),
            schema_version=version,
        )

    @classmethod
    def from_json(
        cls, payload: str, *, strict: bool = False,
    ) -> "ValidationResult":
        if not isinstance(payload, str):
            raise DigitalTwinParseError(
                "ValidationResult.from_json expects a string."
            )
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise DigitalTwinParseError(
                f"ValidationResult.from_json invalid JSON: {exc}"
            ) from exc
        return cls.from_dict(data, strict=strict)

    def __repr__(self) -> str:
        return (
            f"ValidationResult(ok={self.ok}, "
            f"errors={len(self.errors)}, "
            f"warnings={len(self.warnings)}, "
            f"normalized={len(self.normalized)} fields)"
        )


# --------------------------------------------------------------------------- #
# Schema
# --------------------------------------------------------------------------- #

#: Canonical schema definition. Each scenario maps to a dict of
#: parameter name -> ParamSpec.
_PARAM_SCHEMA_SPECS: Dict[SimulationScenario, Dict[str, ParamSpec]] = {
    SimulationScenario.POLICY_CHANGE: {
        "carbon_reduction_rate": ParamSpec(float, 0.0, 1.0, True),
        "helium_conservation_rate": ParamSpec(float, 0.0, 1.0, False),
    },
    SimulationScenario.MARKET_SHOCK: {
        "shock_size": ParamSpec(float, -1.0, 1.0, True),
        "shock_duration_days": ParamSpec(int, 1, None, False),
    },
    SimulationScenario.RESOURCE_DEPLETION: {
        "resource_type": ParamSpec(str, None, None, True),
        "depletion_rate": ParamSpec(float, 0.0, 1.0, True),
    },
    SimulationScenario.TECHNOLOGY_ADOPTION: {
        "adoption_rate": ParamSpec(float, 0.0, 1.0, True),
        "technology": ParamSpec(str, None, None, False),
    },
    SimulationScenario.REGULATORY_CHANGE: {
        "regulatory_change_factor": ParamSpec(float, 0.0, 2.0, True),
    },
    SimulationScenario.CLIMATE_EVENT: {
        "severity": ParamSpec(float, 0.0, 1.0, True),
        "duration_days": ParamSpec(int, 1, None, False),
    },
    SimulationScenario.POLICY_AND_TECHNOLOGY: {
        "carbon_reduction_rate": ParamSpec(float, 0.0, 1.0, True),
        "adoption_rate": ParamSpec(float, 0.0, 1.0, True),
    },
    SimulationScenario.MARKET_AND_REGULATORY: {
        "shock_size": ParamSpec(float, -1.0, 1.0, True),
        "regulatory_change_factor": ParamSpec(float, 0.0, 2.0, True),
    },
    SimulationScenario.RESOURCE_AND_CLIMATE: {
        "resource_type": ParamSpec(str, None, None, True),
        "severity": ParamSpec(float, 0.0, 1.0, True),
    },
}

#: Backward-compatible view: same shape as the original module.
_PARAM_SCHEMAS: Dict[
    SimulationScenario,
    Dict[str, Tuple[type, Optional[float], Optional[float], bool]],
] = {
    scenario: {
        name: spec.as_tuple() for name, spec in fields.items()
    }
    for scenario, fields in _PARAM_SCHEMA_SPECS.items()
}


def _validate_schema_at_import() -> None:
    """Fail loudly on a malformed schema.

    ``ParamSpec.__post_init__`` already validates each entry. This
    function catches cross-entry mistakes: missing scenarios and empty
    schemas.
    """
    missing = [
        s for s in SimulationScenario if s not in _PARAM_SCHEMA_SPECS
    ]
    if missing:
        logger.warning(
            "Scenarios without a schema definition: %s",
            [s.value for s in missing],
        )
    for scenario, fields in _PARAM_SCHEMA_SPECS.items():
        if not fields:
            raise RuntimeError(
                f"Schema for {scenario.value} is empty."
            )


_validate_schema_at_import()


# --------------------------------------------------------------------------- #
# Coercion / range helpers
# --------------------------------------------------------------------------- #

def _coerce_value(name: str, spec: ParamSpec, raw: Any) -> Any:
    """Coerce ``raw`` to the type declared by ``spec``.

    Raises ``TypeError`` / ``ValueError`` with a human-readable message.
    """
    ptype = spec.type

    if ptype is float:
        if isinstance(raw, bool) or not isinstance(raw, (int, float)):
            raise TypeError(f"{name} must be numeric.")
        value = float(raw)
        if not math.isfinite(value):
            raise ValueError(f"{name} must be finite.")
        return value

    if ptype is int:
        if isinstance(raw, bool):
            raise TypeError(f"{name} must be an int.")
        if _is_real_int(raw):
            return int(raw)
        # Accept integral floats (e.g., JSON decodes 5 as 5.0).
        if isinstance(raw, float) and raw.is_integer() and math.isfinite(raw):
            return int(raw)
        raise TypeError(f"{name} must be an int.")

    if ptype is str:
        if not isinstance(raw, str) or not raw:
            raise TypeError(f"{name} must be a non-empty string.")
        return raw

    # Should be unreachable; ParamSpec.__post_init__ enforces the set.
    raise TypeError(
        f"{name} has unsupported schema type {ptype!r}."
    )


def _check_range(name: str, spec: ParamSpec, value: Any) -> Optional[str]:
    """Return an error message if ``value`` is out of ``spec`` bounds."""
    if isinstance(value, bool):
        return None  # bools never reach here for numeric specs.
    if not isinstance(value, (int, float)):
        return None  # str params have no numeric bounds.
    if spec.lo is not None and value < spec.lo:
        return f"{name}={value!r} is below {spec.lo}."
    if spec.hi is not None and value > spec.hi:
        return f"{name}={value!r} is above {spec.hi}."
    return None


# --------------------------------------------------------------------------- #
# Validator
# --------------------------------------------------------------------------- #

class ScenarioParameterValidator:
    """Validates and normalizes scenario parameters.

    Parameters are matched against the schema for ``scenario_type``.
    Unknown keys produce warnings (or errors when ``strict=True``).
    Coercion is deterministic: ``float`` accepts any finite real,
    ``int`` accepts ``int`` and integral floats, ``str`` must be
    non-empty.
    """

    # -- schema access -------------------------------------------------- #

    @classmethod
    def schema_for(
        cls, scenario_type: SimulationScenario,
    ) -> Dict[str, Tuple[type, Optional[float], Optional[float], bool]]:
        """Return the legacy tuple-form schema for ``scenario_type``.

        The returned mapping is a fresh dict; the inner tuples are
        immutable and shared with the module-level schema.
        """
        return dict(_PARAM_SCHEMAS.get(scenario_type, {}))

    @classmethod
    def schema_specs_for(
        cls, scenario_type: SimulationScenario,
    ) -> Dict[str, ParamSpec]:
        """Return the :class:`ParamSpec` form of the schema."""
        return dict(_PARAM_SCHEMA_SPECS.get(scenario_type, {}))

    # -- single validation ---------------------------------------------- #

    @classmethod
    def validate(
        cls,
        scenario_type: Any,
        parameters: Mapping[str, Any],
        *,
        strict: bool = False,
    ) -> ValidationResult:
        """Validate ``parameters`` against the schema for ``scenario_type``.

        Parameters
        ----------
        scenario_type:
            A :class:`SimulationScenario` or its string value.
        parameters:
            The parameter mapping to validate.
        strict:
            When ``True``, unknown keys become errors. When ``False``
            (default), they are reported as warnings.
        """
        if not isinstance(parameters, ABCMapping):
            issue = ValidationIssue(
                code="not_a_mapping",
                field="",
                message="parameters must be a Mapping.",
            )
            return ValidationResult(
                ok=False,
                errors=(issue.message,),
                issues=(issue,),
            )

        try:
            scenario = SimulationScenario.coerce(scenario_type)
        except DigitalTwinInputError as exc:
            issue = ValidationIssue(
                code="invalid_scenario",
                field="scenario_type",
                message=str(exc),
            )
            return ValidationResult(
                ok=False,
                errors=(issue.message,),
                issues=(issue,),
            )

        specs = cls.schema_specs_for(scenario)
        errors: List[str] = []
        warnings: List[str] = []
        issues: List[ValidationIssue] = []
        normalized: Dict[str, Any] = {}

        # ---- Per-field validation ------------------------------------ #
        for name, spec in specs.items():
            if name not in parameters:
                if spec.required:
                    msg = f"Missing required parameter: {name}"
                    errors.append(msg)
                    issues.append(ValidationIssue(
                        code="missing_required",
                        field=name,
                        message=msg,
                    ))
                continue

            raw = parameters[name]

            # Coerce.
            try:
                value = _coerce_value(name, spec, raw)
            except (TypeError, ValueError) as exc:
                msg = str(exc)
                code = "type_error" if isinstance(exc, TypeError) else "not_finite"
                errors.append(msg)
                issues.append(ValidationIssue(
                    code=code, field=name, message=msg,
                ))
                continue

            # Range check.
            range_msg = _check_range(name, spec, value)
            if range_msg is not None:
                errors.append(range_msg)
                issues.append(ValidationIssue(
                    code="out_of_range", field=name, message=range_msg,
                ))
                continue

            normalized[name] = value

        # ---- Unknown keys -------------------------------------------- #
        extra = set(parameters) - set(specs)
        for k in sorted(extra):
            if strict:
                msg = f"Unknown parameter {k!r}."
                errors.append(msg)
                issues.append(ValidationIssue(
                    code="unknown_parameter",
                    field=k,
                    message=msg,
                ))
            else:
                msg = f"Unknown parameter {k!r} ignored."
                warnings.append(msg)
                issues.append(ValidationIssue(
                    code="unknown_parameter",
                    field=k,
                    message=msg,
                    severity="warning",
                ))

        return ValidationResult(
            ok=not errors,
            errors=tuple(errors),
            warnings=tuple(warnings),
            normalized=normalized,
            issues=tuple(issues),
        )

    # -- batch validation ----------------------------------------------- #

    @classmethod
    def validate_many(
        cls,
        scenario_type: Any,
        parameters_list: Any,
        *,
        strict: bool = False,
    ) -> List[ValidationResult]:
        """Validate a sequence of parameter mappings.

        Returns one :class:`ValidationResult` per input mapping, in
        order. ``scenario_type`` is resolved once.
        """
        if isinstance(parameters_list, (str, bytes)) or not hasattr(
            parameters_list, "__iter__",
        ):
            raise DigitalTwinInputError(
                "parameters_list must be an iterable of Mappings."
            )
        return [
            cls.validate(scenario_type, p, strict=strict)
            for p in parameters_list
        ]

    # -- raising variant ------------------------------------------------ #

    @classmethod
    def validate_or_raise(
        cls,
        scenario_type: Any,
        parameters: Mapping[str, Any],
        *,
        strict: bool = False,
    ) -> Mapping[str, Any]:
        """Validate and return the frozen normalized mapping.

        Raises
        ------
        DigitalTwinInputError
            If validation fails. The exception carries ``errors`` and
            ``warnings`` as structured context.
        """
        res = cls.validate(scenario_type, parameters, strict=strict)
        if not res.ok:
            raise DigitalTwinInputError(
                "; ".join(res.errors) or "scenario parameter validation failed",
                errors=list(res.errors),
                warnings=list(res.warnings),
            )
        return res.normalized


__all__ = [
    "SCHEMA_VERSION",
    "ParamSpec",
    "ScenarioParameterValidator",
    "ValidationIssue",
    "ValidationResult",
]
