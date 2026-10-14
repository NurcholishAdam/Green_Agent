# sustainability/config.py
"""
Sustainability-Aware Model Compression – Configuration v7.1.0
=============================================================

Pydantic v1/v2 compatible configuration for sustainability-aware compression.

Highlights
----------
* Pydantic v1, v2, and a dataclass fallback — same public API in all paths.
* Canonical MOPD objective keys that match the compressor's
  `MOPD_OBJECTIVE_NAMES` ("accuracy", "energy", "carbon_savings_kg",
  "material_index") to eliminate silent weight fallbacks.
* Cross-cutting flags at the top level (`enable_xai`, `enable_temporal_safety`,
  `require_human_approval`, `aggressive_threshold`, `enable_chaos_testing`,
  `chaos_test_interval_seconds`).
* Secret-manager-friendly: only the *env-var name* is stored
  (`carbon_api_key_env`); the raw key is resolved at use-time.
* JSON-safe serialization (`to_dict()` / `to_json()`).
* Cross-module alignment helper to detect drift with the compressor's
  schema and objective constants.
* `extra="forbid"` and `validate_assignment=True` (v2) to catch silent typos
  and post-construction mutation.

Environment
-----------
Every field can be set via `SUSTAINABILITY_<FIELD_NAME_UPPERCASE>`.
Example: `SUSTAINABILITY_ENERGY_THRESHOLD_JOULES=2.5`.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field, fields as dataclass_fields
from enum import Enum
from typing import Any, Dict, FrozenSet, List, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)


# ============================================================================
# Cross-module constants
# ----------------------------------------------------------------------------
# `STATE_SCHEMA_VERSION` MUST match the compressor's constant.
# `MOPD_OBJECTIVE_NAMES` and `MOPD_HIGHER_IS_BETTER` MUST match the compressor's
# `MOPDPoint.objective_vector()` contract.
# ============================================================================
CONFIG_SCHEMA_VERSION: str = "7.1.0"
STATE_SCHEMA_VERSION: int = 7

MOPD_OBJECTIVE_NAMES: Tuple[str, ...] = (
    'accuracy',
    'energy',
    'carbon_savings_kg',
    'material_index',
)
MOPD_HIGHER_IS_BETTER: FrozenSet[str] = frozenset({
    'accuracy',
    'carbon_savings_kg',
})


# ============================================================================
# Pydantic version detection
# ============================================================================
PYDANTIC_AVAILABLE: bool = False
PYDANTIC_V2: bool = False
PYDANTIC_V1: bool = False

BaseModel: Any = object  # default placeholder

try:
    import pydantic as _pydantic  # type: ignore

    _ver = getattr(_pydantic, 'VERSION', '0') or '0'
    _maj = int(str(_ver).split('.')[0])

    if _maj >= 2:
        from pydantic import (  # type: ignore
            BaseModel, ConfigDict, Field, field_validator, model_validator,
        )
        PYDANTIC_AVAILABLE = True
        PYDANTIC_V2 = True
    elif _maj == 1:
        from pydantic import (  # type: ignore
            BaseModel, Field, root_validator, validator,
        )
        PYDANTIC_AVAILABLE = True
        PYDANTIC_V1 = True
except Exception:
    PYDANTIC_AVAILABLE = False
    PYDANTIC_V1 = False
    PYDANTIC_V2 = False


# ============================================================================
# Enums
# ============================================================================
class CompressionMethod(str, Enum):
    STRUCTURED_PRUNING = "structured_pruning"
    UNSTRUCTURED_PRUNING = "unstructured_pruning"
    INT8_QUANT = "int8_quant"
    HYBRID = "hybrid"
    SVD = "svd"


class QuantizationMethod(str, Enum):
    INT8_DYNAMIC = "int8_dynamic"
    INT8_STATIC = "int8_static"
    FP16 = "fp16"


# ============================================================================
# Defaults
# ============================================================================
_DEFAULT_HARDWARE_PROFILES: Dict[str, float] = {
    'default': 0.5e-12,
    'gpu': 0.3e-12,
    'cpu': 0.5e-12,
    'tpu': 0.2e-12,
}

_DEFAULT_MOPD_WEIGHTS: Dict[str, float] = {
    'accuracy': 0.40,
    'energy': 0.30,
    'carbon_savings_kg': 0.20,
    'material_index': 0.10,
}

_DEFAULT_COMPRESSION_PRIORITY: Tuple[CompressionMethod, ...] = (
    CompressionMethod.STRUCTURED_PRUNING,
    CompressionMethod.UNSTRUCTURED_PRUNING,
    CompressionMethod.INT8_QUANT,
    CompressionMethod.HYBRID,
    CompressionMethod.SVD,
)

# A small allow-list of well-known Electricity Map zones. Kept conservative —
# `"global"` is treated as "fallback-only" by consumers.
_KNOWN_CARBON_REGIONS: FrozenSet[str] = frozenset({
    "global", "DE", "FR", "GB", "US-CAL-CISO", "US-NEISO", "US-MIDA-PJM",
    "US-NYISO", "US-ISONE", "US-MISO", "EU", "IN", "JP-TK", "KR", "SG", "AU",
    "BR", "CA", "ZA",
})


# ============================================================================
# Helpers
# ============================================================================
def _redact(value: Optional[str]) -> str:
    if not value:
        return "<unset>"
    if len(value) <= 4:
        return "***"
    return f"{value[:2]}***{value[-2:]}"


def _resolve_env_secret(env_var_name: Optional[str]) -> Optional[str]:
    if not env_var_name:
        return None
    return os.getenv(env_var_name)


def _normalise_weights_inplace(weights: Dict[str, float]) -> Dict[str, float]:
    total = sum(float(v) for v in weights.values() if isinstance(v, (int, float)))
    if total <= 0:
        return dict(weights)
    return {k: float(v) / total for k, v in weights.items()}


def _validate_objective_weights(weights: Dict[str, float]) -> Dict[str, float]:
    """Raise on missing/extra/negative keys; return normalised copy."""
    if not isinstance(weights, dict) or not weights:
        raise ValueError("objective_weights must be a non-empty dict")
    keys = set(weights.keys())
    allowed = set(MOPD_OBJECTIVE_NAMES)
    extra = keys - allowed
    missing = allowed - keys
    if extra:
        raise ValueError(
            f"objective_weights has unknown keys {sorted(extra)}; allowed={sorted(allowed)}"
        )
    if missing:
        raise ValueError(
            f"objective_weights missing keys {sorted(missing)}; required={sorted(allowed)}"
        )
    for k, v in weights.items():
        if not isinstance(v, (int, float)):
            raise ValueError(f"objective_weights[{k}] must be numeric")
        if v <= 0:
            raise ValueError(f"objective_weights[{k}] must be > 0 (got {v})")
    total = sum(weights.values())
    if abs(total - 1.0) > 1e-6:
        logger.warning(f"MOPD weights sum to {total:.6f}; normalising to 1.0.")
        return _normalise_weights_inplace(weights)
    return dict(weights)


def _normalise_fitness_weights(
    accuracy: float, energy: float, carbon: float, material: float,
) -> Tuple[float, float, float, float]:
    total = accuracy + energy + carbon + material
    if total <= 0:
        raise ValueError("Fitness weights must sum to > 0")
    if abs(total - 1.0) > 1e-6:
        logger.warning(f"Fitness weights sum to {total:.6f}; normalising to 1.0.")
        return accuracy / total, energy / total, carbon / total, material / total
    return accuracy, energy, carbon, material


# ============================================================================
# Utility mixin (methods only — no fields)
# ============================================================================
class _SustainabilityUtilityMixin:
    """Shared utility methods for all three model paths (v1/v2/dataclass)."""

    # NOTE: subclasses provide the field values via attribute access.

    def get_energy_per_mac(self) -> float:  # noqa: D401
        profiles = getattr(self, "hardware_profiles", {}) or {}
        profile = getattr(self, "hardware_profile", "default")
        return float(profiles.get(profile, profiles.get("default", 0.5e-12)))

    def get_energy_normalization_max(self) -> float:
        explicit = getattr(self, "energy_normalization_max_joules", None)
        if explicit is not None:
            return float(explicit)
        thr = float(getattr(self, "energy_threshold_joules", 5.0))
        return max(1e-9, thr * 2.0)

    def normalized_fitness_weights(self) -> Dict[str, float]:
        weights = {
            "accuracy": float(getattr(self, "fitness_accuracy_weight", 0.5)),
            "energy": float(getattr(self, "fitness_energy_weight", 0.3)),
            "carbon_savings_kg": float(getattr(self, "fitness_carbon_weight", 0.15)),
            "material_index": float(getattr(self, "fitness_material_weight", 0.05)),
        }
        total = sum(weights.values())
        return {k: v / total for k, v in weights.items()} if total > 0 else weights

    def get_mopd_weights(self) -> Dict[str, float]:
        mopd = getattr(self, "mopd", None)
        return dict(getattr(mopd, "objective_weights", {})) if mopd else {}

    def is_mopd_enabled(self) -> bool:
        mopd = getattr(self, "mopd", None)
        return bool(getattr(mopd, "enabled", False)) if mopd else False

    def is_xai_enabled(self) -> bool:
        return bool(getattr(self, "enable_xai", True))

    def is_temporal_safety_enabled(self) -> bool:
        return bool(getattr(self, "enable_temporal_safety", True))

    def is_human_approval_required(self) -> bool:
        return bool(getattr(self, "require_human_approval", False))

    def is_chaos_testing_enabled(self) -> bool:
        return bool(getattr(self, "enable_chaos_testing", False))

    def get_federated_config(self) -> Any:
        return getattr(self, "federated", None)

    def get_compression_method_priority(self) -> List[CompressionMethod]:
        priority = getattr(self, "compression_strategy_priority", None) or []
        return [CompressionMethod(m) if not isinstance(m, CompressionMethod) else m
                for m in priority]

    def resolve_carbon_api_key(self) -> Optional[str]:
        """Fetch the raw secret from the environment. Never stored on the model."""
        return _resolve_env_secret(getattr(self, "carbon_api_key_env", None))

    def get_prometheus_metrics_labels(self) -> Dict[str, str]:
        mopd = getattr(self, "mopd", None)
        federated = getattr(self, "federated", None)
        return {
            "config_version": str(getattr(self, "version", "unknown")),
            "mopd_enabled": str(bool(getattr(mopd, "enabled", False))).lower(),
            "xai_enabled": str(self.is_xai_enabled()).lower(),
            "temporal_safety": str(self.is_temporal_safety_enabled()).lower(),
            "human_approval": str(self.is_human_approval_required()).lower(),
            "chaos_testing": str(self.is_chaos_testing_enabled()).lower(),
            "federated_enabled": str(bool(getattr(federated, "enabled", False))).lower(),
            "hardware_profile": str(getattr(self, "hardware_profile", "default")),
        }

    def log_effective_config(self, logger_obj: Optional[logging.Logger] = None) -> None:
        """Log the effective config with secrets redacted."""
        log = logger_obj or logger
        log.info("Effective SustainabilityConfig: %s",
                 json.dumps(self.to_dict(), indent=2, default=str))


# ============================================================================
# Path 1: Pydantic v2
# ============================================================================
if PYDANTIC_V2:

    class MOPDConfig(BaseModel):  # type: ignore[valid-type, misc]
        model_config = ConfigDict(extra="forbid")

        enabled: bool = Field(True)
        objective_weights: Dict[str, float] = Field(
            default_factory=lambda: dict(_DEFAULT_MOPD_WEIGHTS),
        )
        grid_resolution: int = Field(5, ge=1)
        enable_cost_benefit: bool = Field(True)
        enable_predictive: bool = Field(True)

        @model_validator(mode="after")
        def _validate_objectives(self):
            self.objective_weights = _validate_objective_weights(self.objective_weights)
            return self

    class FederatedConfig(BaseModel):  # type: ignore[valid-type, misc]
        model_config = ConfigDict(extra="forbid")

        enabled: bool = Field(False)
        server_url: Optional[str] = Field(None)
        sparsity_ratio: float = Field(0.1, ge=0, le=1)
        privacy_epsilon: float = Field(1.0, gt=0)
        privacy_delta: float = Field(1e-5, gt=0, lt=1)
        privacy_clip_norm: float = Field(1.0, gt=0)
        sync_interval_seconds: int = Field(3600, ge=60)
        max_retries: int = Field(3, ge=0)
        request_timeout_seconds: float = Field(30.0, gt=0)

    class SustainabilityConfig(BaseModel, _SustainabilityUtilityMixin):  # type: ignore[valid-type, misc]
        model_config = ConfigDict(
            env_prefix="SUSTAINABILITY_",
            extra="forbid",
            validate_assignment=True,
        )

        # General
        energy_threshold_joules: float = Field(5.0, ge=0)
        accuracy_drop_tolerance: float = Field(0.02, ge=0, le=1)
        relative_accuracy_drop_tolerance: float = Field(0.03, ge=0, le=1)

        # Hardware
        hardware_profile: str = Field("default")
        hardware_profiles: Dict[str, float] = Field(
            default_factory=lambda: dict(_DEFAULT_HARDWARE_PROFILES),
        )
        energy_normalization_max_joules: Optional[float] = Field(None, ge=0)

        # Fitness weights
        fitness_accuracy_weight: float = Field(0.5, ge=0, le=1)
        fitness_energy_weight: float = Field(0.3, ge=0, le=1)
        fitness_carbon_weight: float = Field(0.15, ge=0, le=1)
        fitness_material_weight: float = Field(0.05, ge=0, le=1)
        compression_bonus: float = Field(0.05, ge=0, le=0.5)

        # Adaptive weights
        use_adaptive_weights: bool = Field(True)
        adaptive_learning_rate: float = Field(0.01, gt=0, le=1)

        # Compression strategies
        pruning_sparsity: float = Field(0.3, ge=0, le=1)
        structured_pruning_enabled: bool = Field(True)
        quantization_method: QuantizationMethod = Field(QuantizationMethod.INT8_DYNAMIC)
        hybrid_pruning_sparsity: float = Field(0.2, ge=0, le=1)
        svd_rank_factor: float = Field(0.5, gt=0, le=1)
        compression_strategy_priority: List[CompressionMethod] = Field(
            default_factory=lambda: list(_DEFAULT_COMPRESSION_PRIORITY),
        )

        # Carbon
        carbon_aware_enabled: bool = Field(True)
        carbon_offset_enabled: bool = Field(False)
        carbon_api_key_env: str = Field("ELECTRICITYMAP_API_KEY")
        carbon_region: str = Field("global")
        carbon_intensity_fallback_enabled: bool = Field(True)
        default_carbon_intensity_g_per_kwh: float = Field(400.0, ge=0)

        # Persistence
        compressed_model_dir: str = Field("./compressed_models")
        history_db_path: str = Field("./compression_history.db")
        history_retention_days: int = Field(365, ge=0)

        # Telemetry
        log_compression_events: bool = Field(True)
        export_metrics: bool = Field(True)
        prometheus_port: Optional[int] = Field(9090, ge=1024, le=65535)

        # Integration hooks
        enable_anomaly_trigger: bool = Field(True)
        enable_predictive_maintenance: bool = Field(True)
        enable_auto_recompress: bool = Field(True)
        recompress_interval_seconds: int = Field(3600, ge=60)

        # Cross-cutting flags (top-level)
        enable_xai: bool = Field(True)
        enable_temporal_safety: bool = Field(True)
        require_human_approval: bool = Field(False)
        aggressive_threshold: float = Field(0.2, ge=0, le=1)
        enable_chaos_testing: bool = Field(False)
        chaos_test_interval_seconds: int = Field(3600, ge=60)
        enable_idempotency: bool = Field(True)

        # Sub-configs
        mopd: MOPDConfig = Field(default_factory=MOPDConfig)
        federated: FederatedConfig = Field(default_factory=FederatedConfig)

        # Versioning
        version: str = Field(CONFIG_SCHEMA_VERSION)

        # ---------------- Validators ----------------
        @model_validator(mode="after")
        def _validate_all(self):
            # Hardware profile exists
            if not self.hardware_profiles:
                raise ValueError("hardware_profiles must contain at least one entry")
            if self.hardware_profile not in self.hardware_profiles:
                raise ValueError(
                    f"hardware_profile '{self.hardware_profile}' not in "
                    f"hardware_profiles {sorted(self.hardware_profiles.keys())}"
                )
            # Compression priority non-empty and unique
            if not self.compression_strategy_priority:
                raise ValueError("compression_strategy_priority must be non-empty")
            seen = set()
            for m in self.compression_strategy_priority:
                if m in seen:
                    raise ValueError(f"duplicate compression method: {m}")
                seen.add(m)
            # Prometheus
            if self.export_metrics and self.prometheus_port is None:
                raise ValueError("prometheus_port required when export_metrics is True")
            # Carbon
            if (self.carbon_aware_enabled and not self.carbon_intensity_fallback_enabled
                    and not self.carbon_api_key_env):
                raise ValueError(
                    "carbon_api_key_env must be set when carbon_aware_enabled=True "
                    "and carbon_intensity_fallback_enabled=False"
                )
            if self.carbon_aware_enabled and self.carbon_region not in _KNOWN_CARBON_REGIONS:
                logger.warning(
                    f"carbon_region '{self.carbon_region}' not in known zones; "
                    "consumer may fall back to default intensity."
                )
            # Fitness weights
            (self.fitness_accuracy_weight, self.fitness_energy_weight,
             self.fitness_carbon_weight, self.fitness_material_weight) = \
                _normalise_fitness_weights(
                    self.fitness_accuracy_weight, self.fitness_energy_weight,
                    self.fitness_carbon_weight, self.fitness_material_weight,
                )
            return self

        # ---------------- Serialization ----------------
        def to_dict(self) -> Dict[str, Any]:
            """JSON-safe dict (enums as strings)."""
            return self.model_dump(mode="json", exclude_none=False)

        def to_json(self) -> str:
            return json.dumps(self.to_dict(), indent=2)

        @classmethod
        def from_dict(cls, data: Dict[str, Any]) -> "SustainabilityConfig":
            return cls(**data)

        @classmethod
        def from_env(cls) -> "SustainabilityConfig":
            return cls()


# ============================================================================
# Path 2: Pydantic v1
# ============================================================================
elif PYDANTIC_V1:

    class MOPDConfig(BaseModel):  # type: ignore[valid-type, misc]
        enabled: bool = Field(True)
        objective_weights: Dict[str, float] = Field(
            default_factory=lambda: dict(_DEFAULT_MOPD_WEIGHTS),
        )
        grid_resolution: int = Field(5, ge=1)
        enable_cost_benefit: bool = Field(True)
        enable_predictive: bool = Field(True)

        class Config:
            extra = "forbid"

        @root_validator  # type: ignore[name-defined]
        def _validate_objectives(cls, values):  # noqa: N805
            weights = values.get("objective_weights")
            if weights is not None:
                values["objective_weights"] = _validate_objective_weights(weights)
            return values

    class FederatedConfig(BaseModel):  # type: ignore[valid-type, misc]
        enabled: bool = Field(False)
        server_url: Optional[str] = Field(None)
        sparsity_ratio: float = Field(0.1, ge=0, le=1)
        privacy_epsilon: float = Field(1.0, gt=0)
        privacy_delta: float = Field(1e-5, gt=0, lt=1)
        privacy_clip_norm: float = Field(1.0, gt=0)
        sync_interval_seconds: int = Field(3600, ge=60)
        max_retries: int = Field(3, ge=0)
        request_timeout_seconds: float = Field(30.0, gt=0)

        class Config:
            extra = "forbid"

    class SustainabilityConfig(BaseModel, _SustainabilityUtilityMixin):  # type: ignore[valid-type, misc]
        energy_threshold_joules: float = Field(5.0, ge=0)
        accuracy_drop_tolerance: float = Field(0.02, ge=0, le=1)
        relative_accuracy_drop_tolerance: float = Field(0.03, ge=0, le=1)

        hardware_profile: str = Field("default")
        hardware_profiles: Dict[str, float] = Field(
            default_factory=lambda: dict(_DEFAULT_HARDWARE_PROFILES),
        )
        energy_normalization_max_joules: Optional[float] = Field(None, ge=0)

        fitness_accuracy_weight: float = Field(0.5, ge=0, le=1)
        fitness_energy_weight: float = Field(0.3, ge=0, le=1)
        fitness_carbon_weight: float = Field(0.15, ge=0, le=1)
        fitness_material_weight: float = Field(0.05, ge=0, le=1)
        compression_bonus: float = Field(0.05, ge=0, le=0.5)

        use_adaptive_weights: bool = Field(True)
        adaptive_learning_rate: float = Field(0.01, gt=0, le=1)

        pruning_sparsity: float = Field(0.3, ge=0, le=1)
        structured_pruning_enabled: bool = Field(True)
        quantization_method: QuantizationMethod = Field(QuantizationMethod.INT8_DYNAMIC)
        hybrid_pruning_sparsity: float = Field(0.2, ge=0, le=1)
        svd_rank_factor: float = Field(0.5, gt=0, le=1)
        compression_strategy_priority: List[CompressionMethod] = Field(
            default_factory=lambda: list(_DEFAULT_COMPRESSION_PRIORITY),
        )

        carbon_aware_enabled: bool = Field(True)
        carbon_offset_enabled: bool = Field(False)
        carbon_api_key_env: str = Field("ELECTRICITYMAP_API_KEY")
        carbon_region: str = Field("global")
        carbon_intensity_fallback_enabled: bool = Field(True)
        default_carbon_intensity_g_per_kwh: float = Field(400.0, ge=0)

        compressed_model_dir: str = Field("./compressed_models")
        history_db_path: str = Field("./compression_history.db")
        history_retention_days: int = Field(365, ge=0)

        log_compression_events: bool = Field(True)
        export_metrics: bool = Field(True)
        prometheus_port: Optional[int] = Field(9090, ge=1024, le=65535)

        enable_anomaly_trigger: bool = Field(True)
        enable_predictive_maintenance: bool = Field(True)
        enable_auto_recompress: bool = Field(True)
        recompress_interval_seconds: int = Field(3600, ge=60)

        # Cross-cutting flags
        enable_xai: bool = Field(True)
        enable_temporal_safety: bool = Field(True)
        require_human_approval: bool = Field(False)
        aggressive_threshold: float = Field(0.2, ge=0, le=1)
        enable_chaos_testing: bool = Field(False)
        chaos_test_interval_seconds: int = Field(3600, ge=60)
        enable_idempotency: bool = Field(True)

        mopd: MOPDConfig = Field(default_factory=MOPDConfig)
        federated: FederatedConfig = Field(default_factory=FederatedConfig)

        version: str = Field(CONFIG_SCHEMA_VERSION)

        class Config:
            env_prefix = "SUSTAINABILITY_"
            extra = "forbid"
            validate_assignment = True

        @root_validator  # type: ignore[name-defined]
        def _validate_all(cls, values):  # noqa: N805
            # Hardware
            hp = values.get("hardware_profile", "default")
            hps = values.get("hardware_profiles") or {}
            if not hps:
                raise ValueError("hardware_profiles must contain at least one entry")
            if hp not in hps:
                raise ValueError(
                    f"hardware_profile '{hp}' not in hardware_profiles {sorted(hps.keys())}"
                )
            # Compression priority
            prio = values.get("compression_strategy_priority") or []
            if not prio:
                raise ValueError("compression_strategy_priority must be non-empty")
            if len(set(prio)) != len(prio):
                raise ValueError("duplicate entries in compression_strategy_priority")
            # Prometheus
            if values.get("export_metrics") and values.get("prometheus_port") is None:
                raise ValueError("prometheus_port required when export_metrics is True")
            # Carbon
            if (values.get("carbon_aware_enabled")
                    and not values.get("carbon_intensity_fallback_enabled")
                    and not values.get("carbon_api_key_env")):
                raise ValueError(
                    "carbon_api_key_env required when carbon_aware_enabled=True "
                    "and fallback disabled"
                )
            region = values.get("carbon_region", "global")
            if region not in _KNOWN_CARBON_REGIONS:
                logger.warning(
                    f"carbon_region '{region}' not in known zones; fallback may trigger."
                )
            # Fitness weights
            (values["fitness_accuracy_weight"], values["fitness_energy_weight"],
             values["fitness_carbon_weight"], values["fitness_material_weight"]) = \
                _normalise_fitness_weights(
                    values.get("fitness_accuracy_weight", 0.5),
                    values.get("fitness_energy_weight", 0.3),
                    values.get("fitness_carbon_weight", 0.15),
                    values.get("fitness_material_weight", 0.05),
                )
            return values

        # ---------------- Serialization ----------------
        def to_dict(self) -> Dict[str, Any]:
            """JSON-safe dict via Pydantic v1's `.json()` then re-parse."""
            return json.loads(self.json())

        def to_json(self) -> str:
            return json.dumps(self.to_dict(), indent=2)

        @classmethod
        def from_dict(cls, data: Dict[str, Any]) -> "SustainabilityConfig":
            return cls(**data)

        @classmethod
        def from_env(cls) -> "SustainabilityConfig":
            return cls()


# ============================================================================
# Path 3: Dataclass fallback (no Pydantic installed)
# ============================================================================
else:

    @dataclass
    class MOPDConfig:
        enabled: bool = True
        objective_weights: Dict[str, float] = field(
            default_factory=lambda: dict(_DEFAULT_MOPD_WEIGHTS),
        )
        grid_resolution: int = 5
        enable_cost_benefit: bool = True
        enable_predictive: bool = True

        def __post_init__(self) -> None:
            self.objective_weights = _validate_objective_weights(self.objective_weights)

    @dataclass
    class FederatedConfig:
        enabled: bool = False
        server_url: Optional[str] = None
        sparsity_ratio: float = 0.1
        privacy_epsilon: float = 1.0
        privacy_delta: float = 1e-5
        privacy_clip_norm: float = 1.0
        sync_interval_seconds: int = 3600
        max_retries: int = 3
        request_timeout_seconds: float = 30.0

    @dataclass
    class SustainabilityConfig(_SustainabilityUtilityMixin):
        energy_threshold_joules: float = 5.0
        accuracy_drop_tolerance: float = 0.02
        relative_accuracy_drop_tolerance: float = 0.03

        hardware_profile: str = "default"
        hardware_profiles: Dict[str, float] = field(
            default_factory=lambda: dict(_DEFAULT_HARDWARE_PROFILES),
        )
        energy_normalization_max_joules: Optional[float] = None

        fitness_accuracy_weight: float = 0.5
        fitness_energy_weight: float = 0.3
        fitness_carbon_weight: float = 0.15
        fitness_material_weight: float = 0.05
        compression_bonus: float = 0.05

        use_adaptive_weights: bool = True
        adaptive_learning_rate: float = 0.01

        pruning_sparsity: float = 0.3
        structured_pruning_enabled: bool = True
        quantization_method: QuantizationMethod = QuantizationMethod.INT8_DYNAMIC
        hybrid_pruning_sparsity: float = 0.2
        svd_rank_factor: float = 0.5
        compression_strategy_priority: List[CompressionMethod] = field(
            default_factory=lambda: list(_DEFAULT_COMPRESSION_PRIORITY),
        )

        carbon_aware_enabled: bool = True
        carbon_offset_enabled: bool = False
        carbon_api_key_env: str = "ELECTRICITYMAP_API_KEY"
        carbon_region: str = "global"
        carbon_intensity_fallback_enabled: bool = True
        default_carbon_intensity_g_per_kwh: float = 400.0

        compressed_model_dir: str = "./compressed_models"
        history_db_path: str = "./compression_history.db"
        history_retention_days: int = 365

        log_compression_events: bool = True
        export_metrics: bool = True
        prometheus_port: Optional[int] = 9090

        enable_anomaly_trigger: bool = True
        enable_predictive_maintenance: bool = True
        enable_auto_recompress: bool = True
        recompress_interval_seconds: int = 3600

        enable_xai: bool = True
        enable_temporal_safety: bool = True
        require_human_approval: bool = False
        aggressive_threshold: float = 0.2
        enable_chaos_testing: bool = False
        chaos_test_interval_seconds: int = 3600
        enable_idempotency: bool = True

        mopd: MOPDConfig = field(default_factory=MOPDConfig)
        federated: FederatedConfig = field(default_factory=FederatedConfig)

        version: str = CONFIG_SCHEMA_VERSION

        def __post_init__(self) -> None:
            self._apply_env_overrides()
            # Hardware
            if not self.hardware_profiles:
                raise ValueError("hardware_profiles must contain at least one entry")
            if self.hardware_profile not in self.hardware_profiles:
                raise ValueError(
                    f"hardware_profile '{self.hardware_profile}' not in "
                    f"hardware_profiles {sorted(self.hardware_profiles.keys())}"
                )
            # Compression priority
            if not self.compression_strategy_priority:
                raise ValueError("compression_strategy_priority must be non-empty")
            if len(set(self.compression_strategy_priority)) != len(
                    self.compression_strategy_priority):
                raise ValueError("duplicate entries in compression_strategy_priority")
            # Prometheus
            if self.export_metrics and self.prometheus_port is None:
                raise ValueError("prometheus_port required when export_metrics is True")
            # Carbon
            if (self.carbon_aware_enabled and not self.carbon_intensity_fallback_enabled
                    and not self.carbon_api_key_env):
                raise ValueError("carbon_api_key_env required when fallback disabled")
            if self.carbon_region not in _KNOWN_CARBON_REGIONS:
                logger.warning(f"carbon_region '{self.carbon_region}' not in known zones.")
            # Fitness weights
            (self.fitness_accuracy_weight, self.fitness_energy_weight,
             self.fitness_carbon_weight, self.fitness_material_weight) = \
                _normalise_fitness_weights(
                    self.fitness_accuracy_weight, self.fitness_energy_weight,
                    self.fitness_carbon_weight, self.fitness_material_weight,
                )

        def _apply_env_overrides(self) -> None:
            """Simple env-var override loader for the dataclass path."""
            prefix = "SUSTAINABILITY_"
            for f in dataclass_fields(self):
                if f.name in ("mopd", "federated"):
                    continue
                env_key = f"{prefix}{f.name.upper()}"
                if env_key not in os.environ:
                    continue
                raw = os.environ[env_key]
                try:
                    current = getattr(self, f.name)
                    if isinstance(current, bool):
                        setattr(self, f.name, raw.lower() in ("1", "true", "yes", "on"))
                    elif isinstance(current, int) and not isinstance(current, bool):
                        setattr(self, f.name, int(raw))
                    elif isinstance(current, float):
                        setattr(self, f.name, float(raw))
                    elif isinstance(current, list):
                        # Comma-separated list of enum values
                        items = [x.strip() for x in raw.split(",") if x.strip()]
                        try:
                            setattr(self, f.name,
                                    [CompressionMethod(i) for i in items])
                        except Exception:
                            setattr(self, f.name, items)
                    else:
                        setattr(self, f.name, raw)
                except Exception as e:
                    logger.warning(f"Failed to parse env var {env_key}={raw!r}: {e}")

        # ---------------- Serialization ----------------
        def to_dict(self) -> Dict[str, Any]:
            def _conv(v: Any) -> Any:
                if isinstance(v, Enum):
                    return v.value
                if isinstance(v, (list, tuple)):
                    return [_conv(x) for x in v]
                if isinstance(v, dict):
                    return {str(k): _conv(val) for k, val in v.items()}
                if hasattr(v, "to_dict"):
                    return v.to_dict()
                if hasattr(v, "__dataclass_fields__"):
                    return {f.name: _conv(getattr(v, f.name))
                            for f in dataclass_fields(v)}
                return v

            return _conv(self)

        def to_json(self) -> str:
            return json.dumps(self.to_dict(), indent=2)

        @classmethod
        def from_dict(cls, data: Dict[str, Any]) -> "SustainabilityConfig":
            data = dict(data)
            if isinstance(data.get("mopd"), dict):
                data["mopd"] = MOPDConfig(**data["mopd"])
            if isinstance(data.get("federated"), dict):
                data["federated"] = FederatedConfig(**data["federated"])
            if "compression_strategy_priority" in data:
                data["compression_strategy_priority"] = [
                    CompressionMethod(m) if not isinstance(m, CompressionMethod) else m
                    for m in data["compression_strategy_priority"]
                ]
            if isinstance(data.get("quantization_method"), str):
                data["quantization_method"] = QuantizationMethod(data["quantization_method"])
            return cls(**data)

        @classmethod
        def from_env(cls) -> "SustainabilityConfig":
            return cls()


# ============================================================================
# Module-level singleton (lazy — reads env at import time)
# ============================================================================
SUSTAINABILITY_CONFIG: "SustainabilityConfig" = SustainabilityConfig()


# ============================================================================
# Cross-module alignment helper
# ============================================================================
def validate_cross_module_alignment(
    state_schema_version: Optional[int] = None,
    objective_names: Optional[Sequence[str]] = None,
    higher_is_better: Optional[Sequence[str]] = None,
    mopd_weight_keys: Optional[Sequence[str]] = None,
) -> List[str]:
    """
    Compare this module's contracts against a compressor's contracts.

    Returns a list of human-readable discrepancies. Empty list means aligned.
    """
    discrepancies: List[str] = []

    if state_schema_version is not None and state_schema_version != STATE_SCHEMA_VERSION:
        discrepancies.append(
            f"STATE_SCHEMA_VERSION mismatch: config={STATE_SCHEMA_VERSION} "
            f"vs compressor={state_schema_version}"
        )

    if objective_names is not None and set(objective_names) != set(MOPD_OBJECTIVE_NAMES):
        discrepancies.append(
            f"MOPD_OBJECTIVE_NAMES mismatch: config={sorted(MOPD_OBJECTIVE_NAMES)} "
            f"vs compressor={sorted(objective_names)}"
        )

    if higher_is_better is not None and set(higher_is_better) != set(MOPD_HIGHER_IS_BETTER):
        discrepancies.append(
            f"MOPD_HIGHER_IS_BETTER mismatch: config={sorted(MOPD_HIGHER_IS_BETTER)} "
            f"vs compressor={sorted(higher_is_better)}"
        )

    if mopd_weight_keys is not None and set(mopd_weight_keys) != set(MOPD_OBJECTIVE_NAMES):
        discrepancies.append(
            f"MOPD weight key mismatch: config={sorted(MOPD_OBJECTIVE_NAMES)} "
            f"vs provided={sorted(mopd_weight_keys)}"
        )

    return discrepancies


# ============================================================================
# Optional self-check on import
# ============================================================================
if os.getenv("SUSTAINABILITY_SELF_CHECK", "0").lower() in ("1", "true", "yes"):
    _issues = validate_cross_module_alignment(
        state_schema_version=STATE_SCHEMA_VERSION,
        mopd_weight_keys=list(SUSTAINABILITY_CONFIG.get_mopd_weights().keys()),
    )
    if _issues:
        logger.warning("Sustainability config self-check issues: %s", _issues)
    else:
        logger.info("Sustainability config self-check passed.")


# ============================================================================
# Exports
# ============================================================================
__all__ = [
    # Config classes
    "SustainabilityConfig",
    "MOPDConfig",
    "FederatedConfig",
    "SUSTAINABILITY_CONFIG",
    # Enums
    "CompressionMethod",
    "QuantizationMethod",
    # Constants (cross-module)
    "CONFIG_SCHEMA_VERSION",
    "STATE_SCHEMA_VERSION",
    "MOPD_OBJECTIVE_NAMES",
    "MOPD_HIGHER_IS_BETTER",
    # Pydantic flags
    "PYDANTIC_AVAILABLE",
    "PYDANTIC_V1",
    "PYDANTIC_V2",
    # Utilities
    "validate_cross_module_alignment",
]
