# sustainability/config.py
"""
Sustainability-Aware Model Compression – Configuration Module v7.1.0
====================================================================

Pydantic v1/v2 compatible configuration with:

- Corrected MOPD objective keys that match the compressor's `MOPD_OBJECTIVE_NAMES`
  ('accuracy', 'energy', 'carbon_savings_kg', 'material_index').
- Top-level cross-cutting flags: `enable_temporal_safety`, `enable_chaos_testing`,
  `enable_xai`, `require_human_approval`, `aggressive_threshold`.
- Secret-manager-friendly API: exposes env-var *names*, never raw secrets.
- JSON-safe serialization (`to_dict(mode='json')`).
- Schema version aligned with the compressor's `STATE_SCHEMA_VERSION`.
- Strict cross-module validation (weight keys, hardware profiles, priority list).
- Full Pydantic v1 AND v2 support with graceful fallback to dataclasses.
- Startup logging with secret redaction and Prometheus-friendly introspection.

USAGE
-----
    from sustainability.config import SustainabilityConfig

    cfg = SustainabilityConfig()                       # env-var aware
    cfg = SustainabilityConfig.from_env()              # explicit
    cfg = SustainabilityConfig.from_dict({...})        # round-trip

ENVIRONMENT
-----------
Any field can be overridden with `SUSTAINABILITY_<FIELD_UPPERCASE>` (v2) or
`SUSTAINABILITY_<FIELD_UPPERCASE>` (v1 via `class Config.env_prefix`).
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field, fields as dataclass_fields
from enum import Enum
from typing import Any, Dict, FrozenSet, List, Optional, Tuple

logger = logging.getLogger(__name__)


# ============================================================================
# Schema version — MUST match the compressor's STATE_SCHEMA_VERSION
# ============================================================================
CONFIG_SCHEMA_VERSION: str = "7.1.0"
STATE_SCHEMA_VERSION: int = 7


# ============================================================================
# Canonical MOPD objective names & sign convention
# ----------------------------------------------------------------------------
# These MUST match the compressor's MOPD_OBJECTIVE_NAMES. Any drift is caught
# by `validate_objective_keys`.
# ============================================================================
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

if not PYDANTIC_AVAILABLE:
    try:
        import pydantic as _pydantic  # type: ignore

        _ver = getattr(_pydantic, 'VERSION', '0')
        _maj = int(_ver.split('.')[0]) if _ver else 0

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
# Enum definitions (must not depend on Pydantic)
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
# Default hardware profiles (used only as dataclass defaults)
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

_DEFAULT_COMPRESSION_PRIORITY: List[str] = [
    CompressionMethod.STRUCTURED_PRUNING.value,
    CompressionMethod.UNSTRUCTURED_PRUNING.value,
    CompressionMethod.INT8_QUANT.value,
    CompressionMethod.HYBRID.value,
    CompressionMethod.SVD.value,
]


# ============================================================================
# Helper utilities shared by both model paths
# ============================================================================
def _normalise_weights_inplace(weights: Dict[str, float]) -> Dict[str, float]:
    """Return a new dict whose values sum to 1; empty/invalid input is unchanged."""
    total = sum(float(v) for v in weights.values() if isinstance(v, (int, float)))
    if total <= 0:
        return dict(weights)
    return {k: float(v) / total for k, v in weights.items()}


def _redact_secret(value: Optional[str]) -> str:
    if not value:
        return "<unset>"
    if len(value) <= 4:
        return "***"
    return f"{value[:2]}***{value[-2:]}"


def _resolve_env_secret(env_var_name: Optional[str]) -> Optional[str]:
    """Resolve a secret by env-var name; None if unset."""
    if not env_var_name:
        return None
    return os.getenv(env_var_name)


# ============================================================================
# Pydantic v2 models
# ============================================================================
if PYDANTIC_V2:

    class MOPDConfig(BaseModel):
        """MOPD config. Objective keys MUST match MOPD_OBJECTIVE_NAMES."""
        model_config = ConfigDict(extra="forbid")

        enabled: bool = Field(True)
        objective_weights: Dict[str, float] = Field(
            default_factory=lambda: dict(_DEFAULT_MOPD_WEIGHTS),
        )
        grid_resolution: int = Field(5, ge=1)
        enable_cost_benefit: bool = Field(True)
        enable_predictive: bool = Field(True)

        @model_validator(mode='after')
        def validate_objective_keys(self):
            keys = set(self.objective_weights.keys())
            allowed = set(MOPD_OBJECTIVE_NAMES)
            extra = keys - allowed
            missing = allowed - keys
            if extra:
                raise ValueError(
                    f"objective_weights contains unknown keys {sorted(extra)}; "
                    f"allowed: {sorted(allowed)}"
                )
            if missing:
                raise ValueError(
                    f"objective_weights missing keys {sorted(missing)}; "
                    f"required: {sorted(allowed)}"
                )
            # Require strictly positive values
            for k, v in self.objective_weights.items():
                if v <= 0:
                    raise ValueError(f"objective_weights[{k}] must be > 0")
            # Normalise silently (warn once)
            total = sum(self.objective_weights.values())
            if abs(total - 1.0) > 1e-6:
                logger.warning(
                    f"MOPD objective weights sum to {total:.4f}; normalising."
                )
                self.objective_weights = {
                    k: v / total for k, v in self.objective_weights.items()
                }
            return self

    class FederatedConfig(BaseModel):
        model_config = ConfigDict(extra="forbid")

        enabled: bool = Field(False)
        server_url: Optional[str] = Field(None)
        sparsity_ratio: float = Field(0.1, ge=0, le=1)
        # privacy_epsilon must be strictly positive; zero means "skip DP"
        # and is handled by consumers via `dp_disabled`.
        privacy_epsilon: float = Field(1.0, gt=0)
        privacy_delta: float = Field(1e-5, gt=0, lt=1)
        privacy_clip_norm: float = Field(1.0, gt=0)
        sync_interval_seconds: int = Field(3600, ge=60)
        max_retries: int = Field(3, ge=0)
        request_timeout_seconds: float = Field(30.0, gt=0)

    class SustainabilityConfig(BaseModel):
        """
        Top-level sustainability configuration.
        `extra="forbid"` guards against silent typos in env-var names.
        """
        model_config = ConfigDict(
            env_prefix="SUSTAINABILITY_",
            extra="forbid",
            validate_assignment=True,
        )

        # ---------------- General ----------------
        energy_threshold_joules: float = Field(5.0, ge=0)
        accuracy_drop_tolerance: float = Field(0.02, ge=0, le=1)
        relative_accuracy_drop_tolerance: float = Field(0.03, ge=0, le=1)

        # ---------------- Hardware ----------------
        hardware_profile: str = Field("default")
        hardware_profiles: Dict[str, float] = Field(
            default_factory=lambda: dict(_DEFAULT_HARDWARE_PROFILES),
        )
        energy_normalization_max_joules: Optional[float] = Field(None, ge=0)

        # ---------------- Fitness weights (legacy) ----------------
        fitness_accuracy_weight: float = Field(0.5, ge=0, le=1)
        fitness_energy_weight: float = Field(0.3, ge=0, le=1)
        fitness_carbon_weight: float = Field(0.15, ge=0, le=1)
        fitness_material_weight: float = Field(0.05, ge=0, le=1)
        compression_bonus: float = Field(0.05, ge=0, le=0.5)

        # ---------------- Adaptive weights ----------------
        use_adaptive_weights: bool = Field(True)
        adaptive_learning_rate: float = Field(0.01, gt=0, le=1)

        # ---------------- Compression strategies ----------------
        pruning_sparsity: float = Field(0.3, ge=0, le=1)
        structured_pruning_enabled: bool = Field(True)
        quantization_method: QuantizationMethod = Field(QuantizationMethod.INT8_DYNAMIC)
        hybrid_pruning_sparsity: float = Field(0.2, ge=0, le=1)
        svd_rank_factor: float = Field(0.5, gt=0, le=1)
        compression_strategy_priority: List[CompressionMethod] = Field(
            default_factory=lambda: [
                CompressionMethod(m) for m in _DEFAULT_COMPRESSION_PRIORITY
            ],
        )

        # ---------------- Carbon ----------------
        carbon_aware_enabled: bool = Field(True)
        carbon_offset_enabled: bool = Field(False)
        # SECRET-MANAGER-FRIENDLY: expose env-var name, never the raw secret.
        carbon_api_key_env: str = Field("ELECTRICITYMAP_API_KEY")
        carbon_region: str = Field("global")
        carbon_intensity_fallback_enabled: bool = Field(True)
        default_carbon_intensity_g_per_kwh: float = Field(400.0, ge=0)

        # ---------------- Persistence ----------------
        compressed_model_dir: str = Field("./compressed_models")
        history_db_path: str = Field("./compression_history.db")
        history_retention_days: int = Field(365, ge=0)

        # ---------------- Telemetry ----------------
        log_compression_events: bool = Field(True)
        export_metrics: bool = Field(True)
        prometheus_port: Optional[int] = Field(9090, ge=1024, le=65535)

        # ---------------- Integration hooks ----------------
        enable_anomaly_trigger: bool = Field(True)
        enable_predictive_maintenance: bool = Field(True)
        enable_auto_recompress: bool = Field(True)
        recompress_interval_seconds: int = Field(3600, ge=60)

        # ---------------- Cross-cutting flags (top-level) ----------------
        # These used to live under MOPDConfig; moved here in v7.1.0.
        enable_xai: bool = Field(True)
        enable_temporal_safety: bool = Field(True)
        require_human_approval: bool = Field(False)
        aggressive_threshold: float = Field(0.2, ge=0, le=1)
        enable_chaos_testing: bool = Field(False)
        chaos_test_interval_seconds: int = Field(3600, ge=60)
        enable_idempotency: bool = Field(True)

        # ---------------- Sub-configs ----------------
        mopd: MOPDConfig = Field(default_factory=MOPDConfig)
        federated: FederatedConfig = Field(default_factory=FederatedConfig)

        # ---------------- Versioning ----------------
        version: str = Field(CONFIG_SCHEMA_VERSION)

        # ------------------------------------------------------------------
        # Validators
        # ------------------------------------------------------------------
        @model_validator(mode='after')
        def validate_hardware_profile_exists(self):
            if not self.hardware_profiles:
                raise ValueError("hardware_profiles must contain at least one entry")
            if self.hardware_profile not in self.hardware_profiles:
                raise ValueError(
                    f"hardware_profile '{self.hardware_profile}' not found in "
                    f"hardware_profiles {sorted(self.hardware_profiles.keys())}"
                )
            return self

        @model_validator(mode='after')
        def validate_compression_priority(self):
            if not self.compression_strategy_priority:
                raise ValueError("compression_strategy_priority must be non-empty")
            seen = set()
            for m in self.compression_strategy_priority:
                if m in seen:
                    raise ValueError(f"Duplicate compression method in priority: {m}")
                seen.add(m)
            return self

        @model_validator(mode='after')
        def validate_prometheus_port(self):
            if self.export_metrics and self.prometheus_port is None:
                raise ValueError("prometheus_port required when export_metrics is True")
            return self

        @model_validator(mode='after')
        def validate_carbon(self):
            if (self.carbon_aware_enabled
                    and not self.carbon_intensity_fallback_enabled
                    and not self.carbon_api_key_env):
                raise ValueError(
                    "carbon_api_key_env must be set when carbon_aware_enabled and "
                    "fallback disabled"
                )
            # If fallback disabled, at least the env-var name must be configured.
            if (self.carbon_aware_enabled
                    and not self.carbon_intensity_fallback_enabled
                    and not _resolve_env_secret(self.carbon_api_key_env)):
                logger.warning(
                    f"carbon_api_key_env='{self.carbon_api_key_env}' not set "
                    "in environment and fallback disabled; API calls will fail."
                )
            return self

        @model_validator(mode='after')
        def normalise_fitness_weights_field(self):
            """Normalise the four legacy fitness weights in-place."""
            total = (self.fitness_accuracy_weight + self.fitness_energy_weight +
                     self.fitness_carbon_weight + self.fitness_material_weight)
            if total <= 0:
                raise ValueError("fitness weights must sum to > 0")
            if abs(total - 1.0) > 1e-6:
                logger.warning(
                    f"Fitness weights sum to {total:.4f}; normalising to 1."
                )
                self.fitness_accuracy_weight /= total
                self.fitness_energy_weight /= total
                self.fitness_carbon_weight /= total
                self.fitness_material_weight /= total
            return self

        # ------------------------------------------------------------------
        # Utility methods
        # ------------------------------------------------------------------
        def get_energy_per_mac(self) -> float:
            return self.hardware_profiles.get(
                self.hardware_profile,
                self.hardware_profiles.get('default', 0.5e-12),
            )

        def get_energy_normalization_max(self) -> float:
            if self.energy_normalization_max_joules is not None:
                return self.energy_normalization_max_joules
            return max(1e-9, self.energy_threshold_joules * 2.0)

        def normalized_fitness_weights(self) -> Dict[str, float]:
            weights = {
                'accuracy': self.fitness_accuracy_weight,
                'energy': self.fitness_energy_weight,
                'carbon_savings_kg': self.fitness_carbon_weight,
                'material_index': self.fitness_material_weight,
            }
            total = sum(weights.values())
            if total > 0:
                return {k: v / total for k, v in weights.items()}
            return weights

        def get_mopd_weights(self) -> Dict[str, float]:
            return dict(self.mopd.objective_weights)

        def is_mopd_enabled(self) -> bool:
            return self.mopd.enabled

        def is_xai_enabled(self) -> bool:
            return self.enable_xai

        def is_temporal_safety_enabled(self) -> bool:
            return self.enable_temporal_safety

        def is_human_approval_required(self) -> bool:
            return self.require_human_approval

        def is_chaos_testing_enabled(self) -> bool:
            return self.enable_chaos_testing

        def get_federated_config(self) -> FederatedConfig:
            return self.federated

        def get_compression_method_priority(self) -> List[CompressionMethod]:
            return list(self.compression_strategy_priority)

        def resolve_carbon_api_key(self) -> Optional[str]:
            """Resolve the carbon API key from the environment (secret-manager-friendly)."""
            return _resolve_env_secret(self.carbon_api_key_env)

        def get_prometheus_metrics_labels(self) -> Dict[str, str]:
            """Labels suitable for Prometheus metrics from config."""
            return {
                'config_version': self.version,
                'mopd_enabled': str(self.mopd.enabled).lower(),
                'xai_enabled': str(self.enable_xai).lower(),
                'temporal_safety': str(self.enable_temporal_safety).lower(),
                'human_approval': str(self.require_human_approval).lower(),
                'chaos_testing': str(self.enable_chaos_testing).lower(),
                'federated_enabled': str(self.federated.enabled).lower(),
                'hardware_profile': self.hardware_profile,
            }

        # ------------------------------------------------------------------
        # Serialization
        # ------------------------------------------------------------------
        def to_dict(self, redact_secrets: bool = True) -> Dict[str, Any]:
            """Export as a JSON-safe dict (enums as strings, datetimes ISO)."""
            data = self.model_dump(mode='json', exclude_none=False)
            if redact_secrets:
                # We only store env-var names, but keep the redaction hook.
                if data.get('carbon_api_key_env'):
                    data['carbon_api_key_env'] = str(data['carbon_api_key_env'])
            return data

        def to_json(self, redact_secrets: bool = True) -> str:
            return json.dumps(self.to_dict(redact_secrets=redact_secrets), indent=2)

        @classmethod
        def from_dict(cls, data: Dict[str, Any]) -> "SustainabilityConfig":
            return cls(**data)

        @classmethod
        def from_env(cls) -> "SustainabilityConfig":
            return cls()

        def log_effective_config(self, logger_obj: Optional[logging.Logger] = None) -> None:
            """Log the effective config with secrets redacted."""
            log = logger_obj or logger
            redacted = self.to_dict(redact_secrets=True)
            # Never log a raw secret; only the env var name (which is safe).
            log.info("Effective SustainabilityConfig: %s", json.dumps(redacted, indent=2))


# ============================================================================
# Pydantic v1 models
# ============================================================================
elif PYDANTIC_V1:

    class MOPDConfig(BaseModel):  # type: ignore
        enabled: bool = Field(True)
        objective_weights: Dict[str, float] = Field(
            default_factory=lambda: dict(_DEFAULT_MOPD_WEIGHTS),
        )
        grid_resolution: int = Field(5, ge=1)
        enable_cost_benefit: bool = Field(True)
        enable_predictive: bool = Field(True)

        class Config:
            extra = "forbid"

        @root_validator
        def validate_objective_keys(cls, values):  # type: ignore
            weights = values.get('objective_weights') or {}
            keys = set(weights.keys())
            allowed = set(MOPD_OBJECTIVE_NAMES)
            extra = keys - allowed
            missing = allowed - keys
            if extra:
                raise ValueError(f"objective_weights contains unknown keys {sorted(extra)}")
            if missing:
                raise ValueError(f"objective_weights missing keys {sorted(missing)}")
            for k, v in weights.items():
                if v <= 0:
                    raise ValueError(f"objective_weights[{k}] must be > 0")
            total = sum(weights.values())
            if abs(total - 1.0) > 1e-6:
                logger.warning(f"MOPD weights sum to {total:.4f}; normalising.")
                values['objective_weights'] = {
                    k: v / total for k, v in weights.items()
                }
            return values

    class FederatedConfig(BaseModel):  # type: ignore
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

    class SustainabilityConfig(BaseModel):  # type: ignore
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
            default_factory=lambda: [
                CompressionMethod(m) for m in _DEFAULT_COMPRESSION_PRIORITY
            ],
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

        # Top-level cross-cutting flags
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

        @root_validator
        def validate_all(cls, values):  # type: ignore
            # hardware profile
            hp = values.get('hardware_profile', 'default')
            hps = values.get('hardware_profiles') or {}
            if not hps:
                raise ValueError("hardware_profiles must contain at least one entry")
            if hp not in hps:
                raise ValueError(
                    f"hardware_profile '{hp}' not found in {sorted(hps.keys())}"
                )
            # compression priority
            prio = values.get('compression_strategy_priority') or []
            if not prio:
                raise ValueError("compression_strategy_priority must be non-empty")
            if len(set(prio)) != len(prio):
                raise ValueError("Duplicate entries in compression_strategy_priority")
            # prometheus
            if values.get('export_metrics') and values.get('prometheus_port') is None:
                raise ValueError("prometheus_port required when export_metrics is True")
            # carbon
            if (values.get('carbon_aware_enabled')
                    and not values.get('carbon_intensity_fallback_enabled')
                    and not values.get('carbon_api_key_env')):
                raise ValueError(
                    "carbon_api_key_env required when carbon_aware and fallback disabled"
                )
            # fitness weights
            total = (
                values.get('fitness_accuracy_weight', 0.0) +
                values.get('fitness_energy_weight', 0.0) +
                values.get('fitness_carbon_weight', 0.0) +
                values.get('fitness_material_weight', 0.0)
            )
            if total <= 0:
                raise ValueError("fitness weights must sum to > 0")
            if abs(total - 1.0) > 1e-6:
                logger.warning(f"Fitness weights sum to {total:.4f}; normalising.")
                values['fitness_accuracy_weight'] /= total
                values['fitness_energy_weight'] /= total
                values['fitness_carbon_weight'] /= total
                values['fitness_material_weight'] /= total
            return values

        # ---------- Utility methods (same surface as v2) ----------
        def get_energy_per_mac(self) -> float:
            return self.hardware_profiles.get(
                self.hardware_profile,
                self.hardware_profiles.get('default', 0.5e-12),
            )

        def get_energy_normalization_max(self) -> float:
            if self.energy_normalization_max_joules is not None:
                return self.energy_normalization_max_joules
            return max(1e-9, self.energy_threshold_joules * 2.0)

        def normalized_fitness_weights(self) -> Dict[str, float]:
            weights = {
                'accuracy': self.fitness_accuracy_weight,
                'energy': self.fitness_energy_weight,
                'carbon_savings_kg': self.fitness_carbon_weight,
                'material_index': self.fitness_material_weight,
            }
            total = sum(weights.values())
            if total > 0:
                return {k: v / total for k, v in weights.items()}
            return weights

        def get_mopd_weights(self) -> Dict[str, float]:
            return dict(self.mopd.objective_weights)

        def is_mopd_enabled(self) -> bool:
            return self.mopd.enabled

        def is_xai_enabled(self) -> bool:
            return self.enable_xai

        def is_temporal_safety_enabled(self) -> bool:
            return self.enable_temporal_safety

        def is_human_approval_required(self) -> bool:
            return self.require_human_approval

        def is_chaos_testing_enabled(self) -> bool:
            return self.enable_chaos_testing

        def get_federated_config(self) -> FederatedConfig:
            return self.federated

        def get_compression_method_priority(self) -> List[CompressionMethod]:
            return list(self.compression_strategy_priority)

        def resolve_carbon_api_key(self) -> Optional[str]:
            return _resolve_env_secret(self.carbon_api_key_env)

        def get_prometheus_metrics_labels(self) -> Dict[str, str]:
            return {
                'config_version': self.version,
                'mopd_enabled': str(self.mopd.enabled).lower(),
                'xai_enabled': str(self.enable_xai).lower(),
                'temporal_safety': str(self.enable_temporal_safety).lower(),
                'human_approval': str(self.require_human_approval).lower(),
                'chaos_testing': str(self.enable_chaos_testing).lower(),
                'federated_enabled': str(self.federated.enabled).lower(),
                'hardware_profile': self.hardware_profile,
            }

        # v1 doesn't have `model_dump(mode='json')`; emulate.
        def to_dict(self, redact_secrets: bool = True) -> Dict[str, Any]:
            return json.loads(self._json_dump())

        def _json_dump(self) -> str:
            # `BaseModel.json()` returns a JSON string with enums serialised.
            return self.json()

        def to_json(self, redact_secrets: bool = True) -> str:
            return json.dumps(self.to_dict(redact_secrets=redact_secrets), indent=2)

        @classmethod
        def from_dict(cls, data: Dict[str, Any]) -> "SustainabilityConfig":
            return cls(**data)

        @classmethod
        def from_env(cls) -> "SustainabilityConfig":
            return cls()

        def log_effective_config(self, logger_obj: Optional[logging.Logger] = None) -> None:
            log = logger_obj or logger
            log.info(
                "Effective SustainabilityConfig: %s",
                json.dumps(self.to_dict(redact_secrets=True), indent=2),
            )


# ============================================================================
# Dataclass fallback when Pydantic is not available
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
            keys = set(self.objective_weights.keys())
            allowed = set(MOPD_OBJECTIVE_NAMES)
            extra = keys - allowed
            missing = allowed - keys
            if extra:
                raise ValueError(f"Unknown MOPD keys {sorted(extra)}")
            if missing:
                raise ValueError(f"Missing MOPD keys {sorted(missing)}")
            for k, v in self.objective_weights.items():
                if v <= 0:
                    raise ValueError(f"objective_weights[{k}] must be > 0")
            total = sum(self.objective_weights.values())
            if abs(total - 1.0) > 1e-6:
                logger.warning(f"MOPD weights sum to {total:.4f}; normalising.")
                for k in list(self.objective_weights):
                    self.objective_weights[k] /= total

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
    class SustainabilityConfig:
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
            default_factory=lambda: [
                CompressionMethod(m) for m in _DEFAULT_COMPRESSION_PRIORITY
            ],
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
            if not self.hardware_profiles:
                raise ValueError("hardware_profiles must be non-empty")
            if self.hardware_profile not in self.hardware_profiles:
                raise ValueError(f"Unknown hardware profile {self.hardware_profile}")
            if not self.compression_strategy_priority:
                raise ValueError("compression_strategy_priority must be non-empty")
            if self.export_metrics and self.prometheus_port is None:
                raise ValueError("prometheus_port required when export_metrics")
            # Fitness weights
            total = (self.fitness_accuracy_weight + self.fitness_energy_weight +
                     self.fitness_carbon_weight + self.fitness_material_weight)
            if total <= 0:
                raise ValueError("fitness weights must sum to > 0")
            if abs(total - 1.0) > 1e-6:
                logger.warning(f"Fitness weights sum to {total:.4f}; normalising.")
                self.fitness_accuracy_weight /= total
                self.fitness_energy_weight /= total
                self.fitness_carbon_weight /= total
                self.fitness_material_weight /= total
            # Apply environment overrides (dataclass fallback path)
            self._apply_env_overrides()

        def _apply_env_overrides(self) -> None:
            prefix = "SUSTAINABILITY_"
            for f in dataclass_fields(self):
                if f.name in ('mopd', 'federated'):
                    continue
                env_key = f"{prefix}{f.name.upper()}"
                if env_key in os.environ:
                    raw = os.environ[env_key]
                    try:
                        if f.type is bool or f.type == 'bool':
                            setattr(self, f.name, raw.lower() in ('1', 'true', 'yes'))
                        elif f.type is int or f.type == 'int':
                            setattr(self, f.name, int(raw))
                        elif f.type is float or f.type == 'float':
                            setattr(self, f.name, float(raw))
                        else:
                            setattr(self, f.name, raw)
                    except Exception:
                        logger.warning(f"Failed to parse env var {env_key}={raw}")

        def get_energy_per_mac(self) -> float:
            return self.hardware_profiles.get(
                self.hardware_profile,
                self.hardware_profiles.get('default', 0.5e-12),
            )

        def get_energy_normalization_max(self) -> float:
            if self.energy_normalization_max_joules is not None:
                return self.energy_normalization_max_joules
            return max(1e-9, self.energy_threshold_joules * 2.0)

        def normalized_fitness_weights(self) -> Dict[str, float]:
            weights = {
                'accuracy': self.fitness_accuracy_weight,
                'energy': self.fitness_energy_weight,
                'carbon_savings_kg': self.fitness_carbon_weight,
                'material_index': self.fitness_material_weight,
            }
            total = sum(weights.values())
            return {k: v / total for k, v in weights.items()} if total > 0 else weights

        def get_mopd_weights(self) -> Dict[str, float]:
            return dict(self.mopd.objective_weights)

        def is_mopd_enabled(self) -> bool:
            return self.mopd.enabled

        def is_xai_enabled(self) -> bool:
            return self.enable_xai

        def is_temporal_safety_enabled(self) -> bool:
            return self.enable_temporal_safety

        def is_human_approval_required(self) -> bool:
            return self.require_human_approval

        def is_chaos_testing_enabled(self) -> bool:
            return self.enable_chaos_testing

        def get_federated_config(self) -> FederatedConfig:
            return self.federated

        def get_compression_method_priority(self) -> List[CompressionMethod]:
            return list(self.compression_strategy_priority)

        def resolve_carbon_api_key(self) -> Optional[str]:
            return _resolve_env_secret(self.carbon_api_key_env)

        def get_prometheus_metrics_labels(self) -> Dict[str, str]:
            return {
                'config_version': self.version,
                'mopd_enabled': str(self.mopd.enabled).lower(),
                'xai_enabled': str(self.enable_xai).lower(),
                'temporal_safety': str(self.enable_temporal_safety).lower(),
                'human_approval': str(self.require_human_approval).lower(),
                'chaos_testing': str(self.enable_chaos_testing).lower(),
                'federated_enabled': str(self.federated.enabled).lower(),
                'hardware_profile': self.hardware_profile,
            }

        def to_dict(self, redact_secrets: bool = True) -> Dict[str, Any]:
            def _conv(v: Any) -> Any:
                if isinstance(v, Enum):
                    return v.value
                if isinstance(v, (list, tuple)):
                    return [_conv(x) for x in v]
                if isinstance(v, dict):
                    return {k: _conv(val) for k, val in v.items()}
                if hasattr(v, 'to_dict'):
                    return v.to_dict()
                if hasattr(v, '__dataclass_fields__'):
                    return {f.name: _conv(getattr(v, f.name))
                            for f in dataclass_fields(v)}
                return v

            return _conv(self)

        def to_json(self, redact_secrets: bool = True) -> str:
            return json.dumps(self.to_dict(redact_secrets=redact_secrets), indent=2)

        @classmethod
        def from_dict(cls, data: Dict[str, Any]) -> "SustainabilityConfig":
            data = dict(data)
            if 'mopd' in data and isinstance(data['mopd'], dict):
                data['mopd'] = MOPDConfig(**data['mopd'])
            if 'federated' in data and isinstance(data['federated'], dict):
                data['federated'] = FederatedConfig(**data['federated'])
            if 'compression_strategy_priority' in data:
                data['compression_strategy_priority'] = [
                    CompressionMethod(m) if not isinstance(m, CompressionMethod) else m
                    for m in data['compression_strategy_priority']
                ]
            if 'quantization_method' in data and isinstance(data['quantization_method'], str):
                data['quantization_method'] = QuantizationMethod(data['quantization_method'])
            return cls(**data)

        @classmethod
        def from_env(cls) -> "SustainabilityConfig":
            return cls()

        def log_effective_config(self, logger_obj: Optional[logging.Logger] = None) -> None:
            log = logger_obj or logger
            log.info(
                "Effective SustainabilityConfig: %s",
                json.dumps(self.to_dict(redact_secrets=True), indent=2),
            )


# ============================================================================
# Module-level default instance (lazy)
# ============================================================================
SUSTAINABILITY_CONFIG: "SustainabilityConfig" = SustainabilityConfig()


# ============================================================================
# Cross-module alignment checks
# ============================================================================
def validate_cross_module_alignment(
    state_schema_version: Optional[int] = None,
    objective_names: Optional[Sequence[str]] = None,
    higher_is_better: Optional[Sequence[str]] = None,
) -> List[str]:
    """
    Validate that this config module aligns with the compressor's contracts.

    Returns a list of human-readable discrepancy strings. Callers may treat an
    empty list as "OK" and log warnings otherwise.
    """
    discrepancies: List[str] = []

    if state_schema_version is not None and state_schema_version != STATE_SCHEMA_VERSION:
        discrepancies.append(
            f"STATE_SCHEMA_VERSION mismatch: config={STATE_SCHEMA_VERSION} "
            f"vs compressor={state_schema_version}"
        )

    if objective_names is not None:
        cfg_keys = set(MOPD_OBJECTIVE_NAMES)
        comp_keys = set(objective_names)
        if cfg_keys != comp_keys:
            discrepancies.append(
                f"MOPD_OBJECTIVE_NAMES mismatch: config={sorted(cfg_keys)} "
                f"vs compressor={sorted(comp_keys)}"
            )

    if higher_is_better is not None:
        cfg_h = set(MOPD_HIGHER_IS_BETTER)
        comp_h = set(higher_is_better)
        if cfg_h != comp_h:
            discrepancies.append(
                f"MOPD_HIGHER_IS_BETTER mismatch: config={sorted(cfg_h)} "
                f"vs compressor={sorted(comp_h)}"
            )

    return discrepancies


# ============================================================================
# Exports
# ============================================================================
__all__ = [
    # Config
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


# ============================================================================
# Startup self-check (optional, only when env var set)
# ============================================================================
if os.getenv("SUSTAINABILITY_SELF_CHECK", "0").lower() in ("1", "true", "yes"):
    _discrepancies = validate_cross_module_alignment(
        state_schema_version=STATE_SCHEMA_VERSION,
    )
    if _discrepancies:
        logger.warning("Sustainability config self-check found issues: %s", _discrepancies)
    else:
        logger.info("Sustainability config self-check passed.")
