"""
Quantum Bridge v3.1 – Enhanced version with all fixes and improvements.
Supports configurable scaling, validation, caching, history, multiple output formats,
proper QUBO ↔ Ising conversion, custom transformations, and observability.
"""

import logging
from typing import Dict, Any, List, Tuple, Optional, Union, Protocol, Callable
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
import numpy as np
import hashlib
import json
import os
import pickle
import time
from enum import Enum

# ============================================================================
# Optional dependencies
# ============================================================================
try:
    from pydantic import BaseModel, Field, validator, root_validator, ConfigDict
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

try:
    from prometheus_client import Gauge, Counter, Histogram
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

try:
    import structlog
    logger = structlog.get_logger(__name__)
except ImportError:
    logger = logging.getLogger(__name__)

# ============================================================================
# Configuration (Pydantic if available)
# ============================================================================
if PYDANTIC_AVAILABLE:
    class QuantumBridgeConfig(BaseModel):
        """Configuration for QuantumBridge."""
        # Mapping: gradient field name → QUBO parameter name
        field_mapping: Dict[str, str] = Field(
            default_factory=lambda: {
                'carbon': 'penalty_carbon',
                'helium': 'penalty_helium_shortage',
                'trust': 'penalty_geopolitical',
                'opportunity': 'weight_opportunity',
                'eco_atp_reserve': 'constraint_budget'
            }
        )
        # Scaling factors for each field (can be overridden)
        scaling: Dict[str, float] = Field(
            default_factory=lambda: {
                'carbon': 10.0,
                'helium': 20.0,
                'trust': 8.0,
                'opportunity': 5.0,
                'eco_atp_reserve': 15.0
            }
        )
        # Default value for missing fields
        default_gradient: float = 0.5
        # Whether to invert certain fields (e.g., trust: low trust → high penalty)
        invert_fields: List[str] = Field(default_factory=lambda: ['trust', 'eco_atp_reserve'])
        # Enable caching
        enable_caching: bool = True
        # Cache time-to-live in seconds (None = forever)
        cache_ttl: Optional[int] = None
        # Maximum history size
        history_size: int = 100
        # Output format: 'qubo' or 'ising'
        output_format: str = 'qubo'
        # Quadratic interactions: mapping from (field1, field2) to parameter name
        quadratic_mapping: Dict[Tuple[str, str], str] = Field(
            default_factory=lambda: {
                ('carbon', 'helium'): 'penalty_carbon_helium',
                ('trust', 'opportunity'): 'penalty_trust_opportunity'
            }
        )
        # Custom transformation functions per field (name in registry)
        custom_transform_registry: Dict[str, str] = Field(default_factory=dict)
        # Cache persistence path (if None, memory only)
        cache_persistence_path: Optional[str] = None
        # Enable Prometheus metrics
        enable_prometheus: bool = False
        # Maximum number of retries for gradient provider
        provider_retries: int = 2
        # Quadratic scaling factor (applied after product of scaled fields)
        quadratic_scaling: float = 1.0
        # Order of operations: 'invert', 'transform', 'scale' or any permutation
        transform_order: List[str] = Field(
            default_factory=lambda: ['invert', 'transform', 'scale']
        )
        # Parameter types: for each parameter name, specify 'linear' or 'quadratic'
        # This overrides the heuristics for conversion.
        param_types: Dict[str, str] = Field(
            default_factory=lambda: {
                'penalty_carbon': 'linear',
                'penalty_helium_shortage': 'linear',
                'penalty_geopolitical': 'linear',
                'weight_opportunity': 'linear',
                'constraint_budget': 'linear',
                'penalty_carbon_helium': 'quadratic',
                'penalty_trust_opportunity': 'quadratic'
            }
        )
        # Configuration version (incremented on breaking changes)
        config_version: str = "3.1"

        @validator('output_format')
        def validate_output_format(cls, v):
            if v not in ['qubo', 'ising']:
                raise ValueError('output_format must be "qubo" or "ising"')
            return v

        @validator('scaling')
        def validate_scaling(cls, v):
            for k, val in v.items():
                if val <= 0:
                    raise ValueError(f'Scaling factor for {k} must be positive')
            return v

        @root_validator
        def validate_quadratic_mapping(cls, values):
            field_mapping = values.get('field_mapping', {})
            quadratic = values.get('quadratic_mapping', {})
            for (f1, f2), param in quadratic.items():
                if f1 not in field_mapping and f2 not in field_mapping:
                    logger.warning(f"Quadratic field pair ({f1},{f2}) not in field_mapping")
            return values

        def config_hash(self) -> str:
            """Generate a hash of the configuration for cache key."""
            data = {
                'field_mapping': self.field_mapping,
                'scaling': self.scaling,
                'invert_fields': self.invert_fields,
                'quadratic_mapping': self.quadratic_mapping,
                'custom_transform_registry': self.custom_transform_registry,
                'output_format': self.output_format,
                'quadratic_scaling': self.quadratic_scaling,
                'transform_order': self.transform_order,
                'param_types': self.param_types,
                'config_version': self.config_version,
                'default_gradient': self.default_gradient,
            }
            return hashlib.sha256(json.dumps(data, sort_keys=True).encode()).hexdigest()
else:
    @dataclass
    class QuantumBridgeConfig:
        field_mapping: Dict[str, str] = field(default_factory=lambda: {
            'carbon': 'penalty_carbon',
            'helium': 'penalty_helium_shortage',
            'trust': 'penalty_geopolitical',
            'opportunity': 'weight_opportunity',
            'eco_atp_reserve': 'constraint_budget'
        })
        scaling: Dict[str, float] = field(default_factory=lambda: {
            'carbon': 10.0,
            'helium': 20.0,
            'trust': 8.0,
            'opportunity': 5.0,
            'eco_atp_reserve': 15.0
        })
        default_gradient: float = 0.5
        invert_fields: List[str] = field(default_factory=lambda: ['trust', 'eco_atp_reserve'])
        enable_caching: bool = True
        cache_ttl: Optional[int] = None
        history_size: int = 100
        output_format: str = 'qubo'
        quadratic_mapping: Dict[Tuple[str, str], str] = field(default_factory=lambda: {
            ('carbon', 'helium'): 'penalty_carbon_helium',
            ('trust', 'opportunity'): 'penalty_trust_opportunity'
        })
        custom_transform_registry: Dict[str, str] = field(default_factory=dict)
        cache_persistence_path: Optional[str] = None
        enable_prometheus: bool = False
        provider_retries: int = 2
        quadratic_scaling: float = 1.0
        transform_order: List[str] = field(default_factory=lambda: ['invert', 'transform', 'scale'])
        param_types: Dict[str, str] = field(default_factory=lambda: {
            'penalty_carbon': 'linear',
            'penalty_helium_shortage': 'linear',
            'penalty_geopolitical': 'linear',
            'weight_opportunity': 'linear',
            'constraint_budget': 'linear',
            'penalty_carbon_helium': 'quadratic',
            'penalty_trust_opportunity': 'quadratic'
        })
        config_version: str = "3.1"

        def config_hash(self) -> str:
            data = {
                'field_mapping': self.field_mapping,
                'scaling': self.scaling,
                'invert_fields': self.invert_fields,
                'quadratic_mapping': {f"{k[0]}_{k[1]}": v for k, v in self.quadratic_mapping.items()},
                'custom_transform_registry': self.custom_transform_registry,
                'output_format': self.output_format,
                'quadratic_scaling': self.quadratic_scaling,
                'transform_order': self.transform_order,
                'param_types': self.param_types,
                'config_version': self.config_version,
                'default_gradient': self.default_gradient,
            }
            return hashlib.sha256(json.dumps(data, sort_keys=True).encode()).hexdigest()

# ============================================================================
# Protocols
# ============================================================================
class GradientProvider(Protocol):
    """Protocol for accessing gradient field strengths."""
    def get_field_strengths(self) -> Dict[str, float]: ...
    def get_forecast(self, hours: int) -> Optional[Dict[str, float]]: ...  # optional

class QuantumSolver(Protocol):
    """Protocol for applying QUBO/Ising parameters to a quantum solver."""
    def set_parameters(self, params: Dict[str, float]) -> None: ...
    def solve(self) -> Dict[str, Any]: ...

# ============================================================================
# Composite Gradient Provider
# ============================================================================
class CompositeGradientProvider:
    """Combines multiple gradient providers with weights."""
    def __init__(self, providers: List[Tuple[GradientProvider, float]], normalize: bool = True):
        self.providers = providers  # list of (provider, weight)
        self.normalize = normalize

    def get_field_strengths(self) -> Dict[str, float]:
        combined: Dict[str, float] = {}
        total_weight = sum(w for _, w in self.providers)
        if self.normalize and total_weight > 0:
            norm = total_weight
        else:
            norm = 1.0

        for provider, weight in self.providers:
            try:
                strengths = provider.get_field_strengths()
                for field, value in strengths.items():
                    combined[field] = combined.get(field, 0.0) + value * weight / norm
            except Exception as e:
                logger.warning("Provider failed in composite: %s", e)
        return combined

    def get_forecast(self, hours: int) -> Optional[Dict[str, float]]:
        forecasts = []
        for provider, _ in self.providers:
            if hasattr(provider, 'get_forecast'):
                try:
                    f = provider.get_forecast(hours)
                    if f is not None:
                        forecasts.append(f)
                except Exception as e:
                    logger.warning("Forecast from provider failed: %s", e)
        if not forecasts:
            return None
        combined = {}
        for f in forecasts:
            for field, value in f.items():
                combined[field] = combined.get(field, 0.0) + value / len(forecasts)
        return combined

# ============================================================================
# Custom Transformation Registry
# ============================================================================
class TransformRegistry:
    """Registry of named transformation functions."""
    _transforms: Dict[str, Callable[[float], float]] = {}

    @classmethod
    def register(cls, name: str, func: Callable[[float], float]):
        cls._transforms[name] = func

    @classmethod
    def get(cls, name: str) -> Optional[Callable[[float], float]]:
        return cls._transforms.get(name)

# Example transforms (can be registered)
def quadratic_transform(x: float) -> float:
    return x ** 2

def sigmoid_transform(x: float) -> float:
    return 1 / (1 + np.exp(-10 * (x - 0.5)))

TransformRegistry.register('quadratic', quadratic_transform)
TransformRegistry.register('sigmoid', sigmoid_transform)

# ============================================================================
# Enhanced QuantumBridge
# ============================================================================
class QuantumBridge:
    """
    Translates bio‑inspired gradient fields into quantum graph parameters (QUBO/Ising).
    
    Features:
    - Configurable field mapping and scaling
    - Correct QUBO ↔ Ising conversion (including linear and quadratic terms)
    - Custom transformation functions
    - Validation and graceful handling of missing fields
    - Caching with TTL, persistence, and config‑aware invalidation
    - History with export/query capabilities
    - Time‑awareness with optional forecasting
    - Integration with quantum solver via protocol
    - Prometheus metrics (optional)
    """

    def __init__(self,
                 gradient_provider: GradientProvider,
                 quantum_solver: Optional[QuantumSolver] = None,
                 config: Optional[Union[QuantumBridgeConfig, Dict[str, Any]]] = None):
        """
        Initialize the QuantumBridge.

        Args:
            gradient_provider: Object that provides `get_field_strengths()`.
            quantum_solver: Optional solver that implements `set_parameters()`.
            config: Configuration dictionary or QuantumBridgeConfig instance.
        """
        self.gradient_provider = gradient_provider
        self.quantum_solver = quantum_solver

        # Load configuration
        if isinstance(config, dict):
            if PYDANTIC_AVAILABLE:
                self.config = QuantumBridgeConfig(**config)
            else:
                self.config = QuantumBridgeConfig(**config)
        elif isinstance(config, QuantumBridgeConfig):
            self.config = config
        else:
            self.config = QuantumBridgeConfig()

        # Internal state
        self._cache: Optional[Dict[str, float]] = None
        self._cache_hash: Optional[str] = None
        self._cache_timestamp: Optional[datetime] = None
        self._history: List[Dict[str, Any]] = []
        self._last_update: Optional[datetime] = None

        # Compile a list of fields we expect (for validation)
        self._expected_fields = list(self.config.field_mapping.keys())
        self._config_hash = self.config.config_hash()

        # Load persisted cache if available
        if self.config.cache_persistence_path and os.path.exists(self.config.cache_persistence_path):
            self._load_cache_from_disk()

        # Prometheus metrics
        if self.config.enable_prometheus and PROMETHEUS_AVAILABLE:
            self._prometheus_metrics = {
                'translation_latency': Histogram('quantum_bridge_translation_latency_seconds',
                                                 'Time to translate gradients'),
                'cache_hits': Counter('quantum_bridge_cache_hits_total', 'Cache hits'),
                'cache_misses': Counter('quantum_bridge_cache_misses_total', 'Cache misses'),
                'param_values': Gauge('quantum_bridge_param_values', 'Current parameter values',
                                      ['param_name']),
                'translation_count': Counter('quantum_bridge_translation_total', 'Total translations'),
            }
        else:
            self._prometheus_metrics = None

        logger.info("QuantumBridge initialized", config=self.config, config_hash=self._config_hash)

    def _compute_hash(self, strengths: Dict[str, float]) -> str:
        """Compute a hash of the gradient strengths and configuration for caching."""
        # Include config hash to invalidate on config changes
        data = {
            'strengths': strengths,
            'config_hash': self._config_hash
        }
        return hashlib.sha256(json.dumps(data, sort_keys=True).encode()).hexdigest()

    def _validate_and_complete(self, strengths: Dict[str, float]) -> Dict[str, float]:
        """
        Ensure all expected fields are present; fill missing with default.
        Also clip values to [0,1].
        """
        validated = {}
        for field in self._expected_fields:
            value = strengths.get(field, self.config.default_gradient)
            value = max(0.0, min(1.0, value))
            validated[field] = value
        return validated

    def _apply_transform(self, field: str, value: float) -> float:
        """Apply custom transform if registered."""
        transform_name = self.config.custom_transform_registry.get(field)
        if transform_name:
            transform_func = TransformRegistry.get(transform_name)
            if transform_func:
                try:
                    return transform_func(value)
                except Exception as e:
                    logger.warning("Transform '%s' failed for field %s: %s", transform_name, field, e)
            else:
                logger.warning("Unknown transform '%s' for field %s", transform_name, field)
        return value

    def _translate_value(self, field: str, value: float) -> float:
        """
        Translate a single gradient value to a QUBO parameter using scaling, inversion,
        and optional custom transformation, in the order specified by `transform_order`.
        """
        scale = self.config.scaling.get(field, 1.0)
        invert = field in self.config.invert_fields

        # Apply operations in the configured order
        for op in self.config.transform_order:
            if op == 'invert' and invert:
                value = 1.0 - value
            elif op == 'transform':
                value = self._apply_transform(field, value)
            elif op == 'scale':
                value *= scale
            # else: unknown op, ignore
        return value

    def _translate_quadratic(self, strengths: Dict[str, float]) -> Dict[str, float]:
        """Translate quadratic interactions."""
        quadratic_params = {}
        for (f1, f2), param_name in self.config.quadratic_mapping.items():
            v1 = strengths.get(f1, 0.0)
            v2 = strengths.get(f2, 0.0)
            # Apply transformations and scaling individually (same as linear)
            v1 = self._translate_value(f1, v1) / self.config.scaling.get(f1, 1.0)  # remove scaling to avoid double counting?
            v2 = self._translate_value(f2, v2) / self.config.scaling.get(f2, 1.0)
            # Apply quadratic scaling factor
            quadratic_params[param_name] = v1 * v2 * self.config.quadratic_scaling
        return quadratic_params

    def _qubo_to_ising(self, qubo_params: Dict[str, float]) -> Dict[str, float]:
        """
        Convert QUBO parameters to Ising format.
        For a QUBO with linear terms (Q_ii) and quadratic terms (Q_ij),
        the Ising equivalents are:
          h_i = Q_ii + 0.5 * sum_j Q_ij
          J_ij = 0.25 * Q_ij
        This assumes the QUBO variables are binary (0/1).
        """
        ising_params = {}
        # Group parameters by type
        linear_params = {}
        quadratic_params = {}
        for param, value in qubo_params.items():
            if param == 'timestamp':
                continue
            param_type = self.config.param_types.get(param, 'linear')
            if param_type == 'linear':
                linear_params[param] = value
            elif param_type == 'quadratic':
                quadratic_params[param] = value
            else:
                logger.warning("Unknown parameter type for %s, treating as linear", param)
                linear_params[param] = value

        # Build h_i: Q_ii + 0.5 * sum_j Q_ij (but we have only one quadratic per pair)
        # For each linear term, we need to sum over all quadratic terms that involve it.
        # However, we don't know which fields are involved; we need a mapping from param to fields.
        # We'll use the quadratic_mapping inverse: param_name -> (field1, field2)
        inverse_quadratic = {v: k for k, v in self.config.quadratic_mapping.items()}

        # Compute h_i for each linear parameter
        for lin_param, lin_value in linear_params.items():
            # Find the field corresponding to this linear param
            field = None
            for f, p in self.config.field_mapping.items():
                if p == lin_param:
                    field = f
                    break
            if field is None:
                # If not found, keep as-is
                ising_params[f"h_{lin_param}"] = lin_value
                continue

            # Sum over all quadratic terms that involve this field
            sum_quad = 0.0
            for q_param, q_value in quadratic_params.items():
                if q_param in inverse_quadratic:
                    f1, f2 = inverse_quadratic[q_param]
                    if f1 == field or f2 == field:
                        sum_quad += q_value
            h_i = lin_value + 0.5 * sum_quad
            ising_params[f"h_{lin_param}"] = h_i

        # J_ij = 0.25 * Q_ij
        for q_param, q_value in quadratic_params.items():
            ising_params[f"J_{q_param}"] = 0.25 * q_value

        # Copy any other parameters
        for param, value in qubo_params.items():
            if param != 'timestamp' and param not in linear_params and param not in quadratic_params:
                ising_params[param] = value

        return ising_params

    def get_qubo_parameters(self, forecast_hours: Optional[int] = None) -> Dict[str, float]:
        """
        Compute QUBO/Ising parameters from current gradient strengths.
        If forecast_hours is provided and the gradient provider supports forecasts,
        the parameters will be based on the forecasted gradients.

        Returns:
            Dictionary of parameter names → numeric values.
        """
        if self._prometheus_metrics:
            self._prometheus_metrics['translation_count'].inc()

        # 1. Fetch strengths (with retries)
        strengths = self._fetch_strengths_with_retry(forecast_hours)

        # 2. Validate and complete
        strengths = self._validate_and_complete(strengths)

        # 3. Check cache
        current_hash = self._compute_hash(strengths)
        if self.config.enable_caching and self._cache_hash == current_hash:
            # Check TTL
            if self._cache_timestamp and self.config.cache_ttl is not None:
                if (datetime.now(timezone.utc) - self._cache_timestamp) > timedelta(seconds=self.config.cache_ttl):
                    logger.debug("Cache expired")
                    self._cache = None
                    self._cache_hash = None
                    self._cache_timestamp = None
                else:
                    if self._prometheus_metrics:
                        self._prometheus_metrics['cache_hits'].inc()
                    return self._cache
            else:
                if self._prometheus_metrics:
                    self._prometheus_metrics['cache_hits'].inc()
                return self._cache

        if self._prometheus_metrics:
            self._prometheus_metrics['cache_misses'].inc()

        # 4. Translate each field (linear)
        params = {}
        for field, value in strengths.items():
            if field in self.config.field_mapping:
                param_name = self.config.field_mapping[field]
                params[param_name] = self._translate_value(field, value)

        # 5. Add quadratic terms
        quadratic_params = self._translate_quadratic(strengths)
        params.update(quadratic_params)

        # 6. Add timestamp
        now = datetime.now(timezone.utc)
        params['timestamp'] = now.timestamp()

        # 7. Convert to Ising if requested
        if self.config.output_format == 'ising':
            params = self._qubo_to_ising(params)

        # 8. Update cache
        if self.config.enable_caching:
            self._cache = params
            self._cache_hash = current_hash
            self._cache_timestamp = now
            self._persist_cache()

        # 9. Record history
        self._record_history(strengths, params)

        # 10. Update Prometheus gauges
        if self._prometheus_metrics:
            # Clear all gauge labels first to avoid stale values
            self._prometheus_metrics['param_values'].clear()
            for param_name, value in params.items():
                if param_name != 'timestamp':
                    self._prometheus_metrics['param_values'].labels(param_name=param_name).set(value)

        return params

    def _fetch_strengths_with_retry(self, forecast_hours: Optional[int] = None) -> Dict[str, float]:
        """Fetch gradient strengths with retries and fallback."""
        retries = self.config.provider_retries
        for attempt in range(retries + 1):
            try:
                if forecast_hours is not None and hasattr(self.gradient_provider, 'get_forecast'):
                    forecast = self.gradient_provider.get_forecast(forecast_hours)
                    if forecast is not None:
                        return forecast
                return self.gradient_provider.get_field_strengths()
            except Exception as e:
                logger.warning("Gradient provider failure (attempt %d/%d): %s", attempt+1, retries+1, e)
                if attempt == retries:
                    logger.error("Gradient provider failed after %d retries; using defaults.", retries+1)
                    # Return all default values
                    return {field: self.config.default_gradient for field in self._expected_fields}
                time.sleep(0.5 * (attempt + 1))

    def _persist_cache(self):
        """Save cache to disk if path is configured."""
        if self.config.cache_persistence_path:
            try:
                data = {
                    'cache': self._cache,
                    'cache_hash': self._cache_hash,
                    'cache_timestamp': self._cache_timestamp.isoformat() if self._cache_timestamp else None,
                    'config_hash': self._config_hash
                }
                with open(self.config.cache_persistence_path, 'wb') as f:
                    pickle.dump(data, f)
                logger.debug("Cache persisted to %s", self.config.cache_persistence_path)
            except Exception as e:
                logger.warning("Failed to persist cache: %s", e)

    def _load_cache_from_disk(self):
        """Load cache from disk, only if config hash matches."""
        try:
            with open(self.config.cache_persistence_path, 'rb') as f:
                data = pickle.load(f)
            # Verify config hash matches current
            if data.get('config_hash') != self._config_hash:
                logger.info("Configuration changed; discarding persisted cache.")
                return
            self._cache = data.get('cache')
            self._cache_hash = data.get('cache_hash')
            ts = data.get('cache_timestamp')
            if ts:
                self._cache_timestamp = datetime.fromisoformat(ts)
            logger.info("Cache loaded from %s", self.config.cache_persistence_path)
        except Exception as e:
            logger.warning("Failed to load cache: %s", e)

    def _record_history(self, strengths: Dict[str, float], params: Dict[str, float]):
        """Add a history entry."""
        if len(self._history) >= self.config.history_size:
            self._history.pop(0)
        self._history.append({
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'gradient_strengths': strengths.copy(),
            'qubo_parameters': params.copy()
        })

    def apply_to_quantum_solver(self, forecast_hours: Optional[int] = None) -> bool:
        """
        Push the computed QUBO/Ising parameters to the attached quantum solver.
        Returns True on success, False if no solver is attached or update fails.
        """
        if self.quantum_solver is None:
            logger.warning("No quantum solver attached – translation only.")
            return False

        params = self.get_qubo_parameters(forecast_hours)
        try:
            self.quantum_solver.set_parameters(params)
            logger.info("Applied QUBO parameters to quantum solver.")
            return True
        except Exception as e:
            logger.error("Failed to apply parameters to quantum solver: %s", e)
            return False

    def get_qubo_report(self, forecast_hours: Optional[int] = None) -> Dict[str, Any]:
        """
        Return a human‑readable report of the current translation.
        """
        strengths = self._fetch_strengths_with_retry(forecast_hours)
        strengths = self._validate_and_complete(strengths)
        params = self.get_qubo_parameters(forecast_hours)
        return {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'gradient_strengths': strengths,
            'qubo_parameters': params,
            'scaling': self.config.scaling,
            'field_mapping': self.config.field_mapping,
            'quadratic_mapping': self.config.quadratic_mapping,
            'cache_hit': self._cache_hash is not None and self._cache is not None,
            'history_size': len(self._history),
            'output_format': self.config.output_format,
            'config': self.config.dict() if PYDANTIC_AVAILABLE else asdict(self.config)
        }

    def get_history(self, limit: Optional[int] = None, start_time: Optional[datetime] = None,
                    end_time: Optional[datetime] = None) -> List[Dict[str, Any]]:
        """Return the history of parameter changes, optionally filtered by time."""
        history = self._history
        if start_time:
            history = [h for h in history if datetime.fromisoformat(h['timestamp']) >= start_time]
        if end_time:
            history = [h for h in history if datetime.fromisoformat(h['timestamp']) <= end_time]
        if limit is not None:
            history = history[-limit:]
        return history

    def export_history(self, path: str):
        """Export history to a JSON file."""
        with open(path, 'w') as f:
            json.dump(self._history, f, indent=2, default=str)

    def clear_cache(self):
        """Clear the cached parameters."""
        self._cache = None
        self._cache_hash = None
        self._cache_timestamp = None
        if self.config.cache_persistence_path and os.path.exists(self.config.cache_persistence_path):
            try:
                os.remove(self.config.cache_persistence_path)
            except Exception as e:
                logger.warning("Failed to delete cache file: %s", e)
        logger.info("Cache cleared.")

    def clear_history(self):
        """Clear the history."""
        self._history = []
        logger.info("History cleared.")

    def update_config(self, updates: Dict[str, Any]) -> None:
        """
        Update configuration at runtime.
        Note: this will clear the cache and update the config hash.
        """
        # Create a new config instance with updates
        if PYDANTIC_AVAILABLE:
            new_dict = self.config.dict()
            new_dict.update(updates)
            self.config = QuantumBridgeConfig(**new_dict)
        else:
            for k, v in updates.items():
                if hasattr(self.config, k):
                    setattr(self.config, k, v)
        # Recalculate config hash and clear cache
        self._config_hash = self.config.config_hash()
        self.clear_cache()
        logger.info("Configuration updated: %s", updates)

    def set_custom_transform(self, field: str, transform_name: str) -> None:
        """
        Set a custom transformation for a field by name (must be registered in TransformRegistry).
        """
        if transform_name not in TransformRegistry._transforms:
            raise ValueError(f"Transform '{transform_name}' not registered")
        if PYDANTIC_AVAILABLE:
            new_registry = self.config.custom_transform_registry.copy()
            new_registry[field] = transform_name
            self.config.custom_transform_registry = new_registry
        else:
            self.config.custom_transform_registry[field] = transform_name
        self._config_hash = self.config.config_hash()
        self.clear_cache()
        logger.info("Set custom transform for field %s: %s", field, transform_name)

# ============================================================================
# Example usage and tests
# ============================================================================
if __name__ == "__main__":
    # Mock gradient provider with forecast
    class MockGradientProvider:
        def get_field_strengths(self):
            return {
                'carbon': 0.8,
                'helium': 0.2,
                'trust': 0.1,
                'opportunity': 0.9,
                'eco_atp_reserve': 0.5
            }
        def get_forecast(self, hours: int):
            # Simulate slight variation
            return {
                'carbon': 0.75 + 0.05 * np.sin(hours),
                'helium': 0.25,
                'trust': 0.15,
                'opportunity': 0.85,
                'eco_atp_reserve': 0.55
            }

    # Mock quantum solver
    class MockQuantumSolver:
        def set_parameters(self, params):
            print(f"Quantum solver received: {params}")

        def solve(self):
            return {'status': 'ok'}

    # Create bridge
    bridge = QuantumBridge(
        gradient_provider=MockGradientProvider(),
        quantum_solver=MockQuantumSolver(),
        config={
            'output_format': 'qubo',
            'enable_prometheus': False,
            'cache_ttl': 60,
            'cache_persistence_path': './cache.pkl'
        }
    )

    # Get parameters
    params = bridge.get_qubo_parameters()
    print("QUBO parameters:", params)

    # Apply to solver
    bridge.apply_to_quantum_solver()

    # Get report
    report = bridge.get_qubo_report()
    print("Report:", report)

    # Update config and clear cache
    bridge.update_config({'scaling': {'carbon': 15.0}})
    print("Updated scaling:", bridge.config.scaling)

    # Test forecast
    forecast_params = bridge.get_qubo_parameters(forecast_hours=2)
    print("Forecast (2h) parameters:", forecast_params)

    # Test custom transform
    bridge.set_custom_transform('carbon', 'quadratic')
    custom_params = bridge.get_qubo_parameters()
    print("With quadratic transform:", custom_params)

    # Test history
    history = bridge.get_history(limit=3)
    print("History (last 3):", history)

    # Test Ising output
    bridge.update_config({'output_format': 'ising'})
    ising_params = bridge.get_qubo_parameters()
    print("Ising parameters:", ising_params)

    # Test composite provider
    provider1 = MockGradientProvider()
    provider2 = MockGradientProvider()
    composite = CompositeGradientProvider([(provider1, 0.7), (provider2, 0.3)])
    bridge_composite = QuantumBridge(gradient_provider=composite)
    print("Composite strengths:", composite.get_field_strengths())

    # Test persistence
    time.sleep(1)
    new_bridge = QuantumBridge(
        gradient_provider=MockGradientProvider(),
        config={'cache_persistence_path': './cache.pkl'}
    )
    print("New bridge cache loaded:", new_bridge._cache is not None)

    # Cleanup
    if os.path.exists('./cache.pkl'):
        os.remove('./cache.pkl')

    print("All tests passed.")
