# src/quantum_integration/digital_twin/digital_twin_validation.py

"""Scenario parameter validation with type and range checks."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, Optional, Tuple

from .digital_twin_errors import DigitalTwinInputError
from .digital_twin_helpers import _is_finite_nonneg, _is_real_int
from .digital_twin_schemas import SimulationScenario

SCHEMA_VERSION: int = 1


@dataclass(frozen=True)
class ValidationResult:
    ok: bool
    errors: Tuple[str, ...] = ()
    warnings: Tuple[str, ...] = ()
    normalized: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "ok": self.ok,
            "errors": list(self.errors),
            "warnings": list(self.warnings),
            "normalized": dict(self.normalized),
        }


#: Per-scenario parameter schema: name -> (type, min, max, required).
#: ``min`` / ``max`` are inclusive bounds; use ``None`` to skip.
_PARAM_SCHEMAS: Dict[SimulationScenario, Dict[str, Tuple[type, Optional[float], Optional[float], bool]]] = {
    SimulationScenario.POLICY_CHANGE: {
        "carbon_reduction_rate": (float, 0.0, 1.0, True),
        "helium_conservation_rate": (float, 0.0, 1.0, False),
    },
    SimulationScenario.MARKET_SHOCK: {
        "shock_size": (float, -1.0, 1.0, True),
        "shock_duration_days": (int, 1, None, False),
    },
    SimulationScenario.RESOURCE_DEPLETION: {
        "resource_type": (str, None, None, True),
        "depletion_rate": (float, 0.0, 1.0, True),
    },
    SimulationScenario.TECHNOLOGY_ADOPTION: {
        "adoption_rate": (float, 0.0, 1.0, True),
        "technology": (str, None, None, False),
    },
    SimulationScenario.REGULATORY_CHANGE: {
        "regulatory_change_factor": (float, 0.0, 2.0, True),
    },
    SimulationScenario.CLIMATE_EVENT: {
        "severity": (float, 0.0, 1.0, True),
        "duration_days": (int, 1, None, False),
    },
    SimulationScenario.POLICY_AND_TECHNOLOGY: {
        "carbon_reduction_rate": (float, 0.0, 1.0, True),
        "adoption_rate": (float, 0.0, 1.0, True),
    },
    SimulationScenario.MARKET_AND_REGULATORY: {
        "shock_size": (float, -1.0, 1.0, True),
        "regulatory_change_factor": (float, 0.0, 2.0, True),
    },
    SimulationScenario.RESOURCE_AND_CLIMATE: {
        "resource_type": (str, None, None, True),
        "severity": (float, 0.0, 1.0, True),
    },
}


class ScenarioParameterValidator:
    """Validates and normalizes scenario parameters."""

    @classmethod
    def schema_for(
        cls, scenario_type: SimulationScenario,
    ) -> Dict[str, Tuple[type, Optional[float], Optional[float], bool]]:
        return dict(_PARAM_SCHEMAS.get(scenario_type, {}))

    @classmethod
    def validate(
        cls,
        scenario_type: Any,
        parameters: Mapping[str, Any],
    ) -> ValidationResult:
        if not isinstance(parameters, Mapping):
            return ValidationResult(
                ok=False,
                errors=("parameters must be a Mapping.",),
            )
        try:
            scenario = SimulationScenario.coerce(scenario_type)
        except DigitalTwinInputError as exc:
            return ValidationResult(ok=False, errors=(str(exc),))

        schema = cls.schema_for(scenario)
        errors: List[str] = []
        warnings: List[str] = []
        normalized: Dict[str, Any] = {}

        for name, (ptype, lo, hi, required) in schema.items():
            if name not in parameters:
                if required:
                    errors.append(f"Missing required parameter: {name}")
                continue
            raw = parameters[name]
            # Coerce.
            try:
                if ptype is float:
                    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
                        raise TypeError(
                            f"{name} must be numeric."
                        )
                    value: Any = float(raw)
                    if not _is_finite_nonneg(abs(value)) and abs(value) != 0:
                        raise ValueError(f"{name} must be finite.")
                elif ptype is int:
                    if not _is_real_int(raw):
                        raise TypeError(f"{name} must be an int.")
                    value = int(raw)
                elif ptype is str:
                    if not isinstance(raw, str) or not raw:
                        raise TypeError(
                            f"{name} must be a non-empty string."
                        )
                    value = raw
                else:
                    value = raw
            except (TypeError, ValueError) as exc:
                errors.append(str(exc))
                continue

            # Range check.
            if lo is not None and value < lo:
                errors.append(f"{name}={value!r} is below {lo}.")
                continue
            if hi is not None and value > hi:
                errors.append(f"{name}={value!r} is above {hi}.")
                continue
            normalized[name] = value

        # Unknown keys become warnings.
        extra = set(parameters) - set(schema)
        for k in sorted(extra):
            warnings.append(f"Unknown parameter {k!r} ignored.")

        return ValidationResult(
            ok=not errors,
            errors=tuple(errors),
            warnings=tuple(warnings),
            normalized=normalized,
        )

    @classmethod
    def validate_or_raise(
        cls,
        scenario_type: Any,
        parameters: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        res = cls.validate(scenario_type, parameters)
        if not res.ok:
            raise DigitalTwinInputError(
                "; ".join(res.errors)
            )
        return res.normalized


__all__ = ["SCHEMA_VERSION", "ScenarioParameterValidator", "ValidationResult"]
