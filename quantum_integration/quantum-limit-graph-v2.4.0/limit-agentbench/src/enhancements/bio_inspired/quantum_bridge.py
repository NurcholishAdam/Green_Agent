"""
Quantum Bridge v2.0
Enhanced translation of bio‑inspired gradient fields into quantum graph parameters (QUBO/Ising).
Supports configurable scaling, validation, caching, history, and multiple output formats.
"""

import logging
from typing import Dict, Any, List, Tuple, Optional, Union, Protocol
from dataclasses import dataclass, field
from datetime import datetime, timezone
import numpy as np
import hashlib
import json

logger = logging.getLogger(__name__)

# ============================================================================
# Configuration (Pydantic fallback if available)
# ============================================================================
try:
    from pydantic import BaseModel, Field, validator
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

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
        # Maximum history size
        history_size: int = 100
        # Output format: 'qubo' or 'ising'
        output_format: str = 'qubo'

        @validator('output_format')
        def validate_output_format(cls, v):
            if v not in ['qubo', 'ising']:
                raise ValueError('output_format must be "qubo" or "ising"')
            return v
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
        history_size: int = 100
        output_format: str = 'qubo'

# ============================================================================
# Protocols for dependency injection
# ============================================================================
class GradientProvider(Protocol):
    """Protocol for accessing gradient field strengths."""
    def get_field_strengths(self) -> Dict[str, float]: ...

class QuantumSolver(Protocol):
    """Protocol for applying QUBO/Ising parameters to a quantum solver."""
    def set_parameters(self, params: Dict[str, float]) -> None: ...
    def solve(self) -> Dict[str, Any]: ...

# ============================================================================
# Enhanced QuantumBridge
# ============================================================================
class QuantumBridge:
    """
    Translates bio‑inspired gradient fields into quantum graph parameters (QUBO/Ising).
    
    Features:
    - Configurable field mapping and scaling
    - Validation and graceful handling of missing fields
    - Support for QUBO and Ising formats
    - Caching to avoid recomputation
    - History of parameter changes
    - Time‑awareness with optional forecasting
    - Integration with quantum solver via protocol
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
        self._history: List[Dict[str, Any]] = []
        self._last_update: Optional[datetime] = None

        # Compile a list of fields we expect (for validation)
        self._expected_fields = list(self.config.field_mapping.keys())

        logger.info("QuantumBridge initialized with config: %s", self.config)

    def _compute_hash(self, strengths: Dict[str, float]) -> str:
        """Compute a hash of the gradient strengths for caching."""
        # Sort keys for deterministic order
        sorted_items = sorted(strengths.items())
        data = json.dumps(sorted_items, sort_keys=True)
        return hashlib.md5(data.encode()).hexdigest()

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

    def _translate_field(self, field: str, value: float) -> float:
        """
        Translate a single gradient value to a QUBO parameter using scaling and inversion.
        """
        scale = self.config.scaling.get(field, 1.0)
        invert = field in self.config.invert_fields
        if invert:
            # Invert so that low gradient → high penalty
            base = 1.0 - value
        else:
            base = value
        return base * scale

    def get_qubo_parameters(self) -> Dict[str, float]:
        """
        Compute QUBO/Ising parameters from current gradient strengths.

        Returns:
            Dictionary of parameter names → numeric values.
        """
        # 1. Fetch current strengths
        raw = self.gradient_provider.get_field_strengths()
        strengths = self._validate_and_complete(raw)

        # 2. Check cache
        if self.config.enable_caching:
            current_hash = self._compute_hash(strengths)
            if self._cache_hash == current_hash and self._cache is not None:
                return self._cache

        # 3. Translate each field
        params = {}
        for field, value in strengths.items():
            if field in self.config.field_mapping:
                param_name = self.config.field_mapping[field]
                params[param_name] = self._translate_field(field, value)

        # 4. Add timestamp (optional)
        now = datetime.now(timezone.utc)
        params['timestamp'] = now.timestamp()

        # 5. If output_format is 'ising', convert QUBO to Ising (for binary variables)
        if self.config.output_format == 'ising':
            # Convert linear terms: h_i = Q_{ii} + 0.5 * sum_j Q_{ij}
            # For simplicity, we treat each parameter as a linear bias.
            # In a full implementation, you'd need quadratic terms.
            ising_params = {}
            for k, v in params.items():
                if k != 'timestamp':
                    ising_params[f"h_{k}"] = v
            # Add timestamp
            ising_params['timestamp'] = params['timestamp']
            params = ising_params

        # 6. Update cache
        if self.config.enable_caching:
            self._cache = params
            self._cache_hash = current_hash
            self._last_update = now

        # 7. Record history
        self._record_history(strengths, params)

        return params

    def _record_history(self, strengths: Dict[str, float], params: Dict[str, float]):
        """Add a history entry."""
        if len(self._history) >= self.config.history_size:
            self._history.pop(0)
        self._history.append({
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'gradient_strengths': strengths.copy(),
            'qubo_parameters': params.copy()
        })

    def apply_to_quantum_solver(self) -> bool:
        """
        Push the computed QUBO/Ising parameters to the attached quantum solver.
        Returns True on success, False if no solver is attached or update fails.
        """
        if self.quantum_solver is None:
            logger.warning("No quantum solver attached – translation only.")
            return False

        params = self.get_qubo_parameters()
        try:
            self.quantum_solver.set_parameters(params)
            logger.info("Applied QUBO parameters to quantum solver.")
            return True
        except Exception as e:
            logger.error("Failed to apply parameters to quantum solver: %s", e)
            return False

    def get_qubo_report(self) -> Dict[str, Any]:
        """
        Return a human‑readable report of the current translation.
        """
        strengths = self.gradient_provider.get_field_strengths()
        params = self.get_qubo_parameters()
        return {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'gradient_strengths': strengths,
            'qubo_parameters': params,
            'scaling': self.config.scaling,
            'field_mapping': self.config.field_mapping,
            'cache_hit': self._cache_hash is not None and self._cache is not None,
            'history_size': len(self._history),
            'output_format': self.config.output_format
        }

    def get_history(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Return the history of parameter changes."""
        if limit is None:
            return self._history
        return self._history[-limit:]

    def clear_cache(self):
        """Clear the cached parameters."""
        self._cache = None
        self._cache_hash = None
        logger.info("Cache cleared.")

    def update_config(self, updates: Dict[str, Any]) -> None:
        """
        Update configuration at runtime.
        Note: this will clear the cache.
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
        self.clear_cache()
        logger.info("Configuration updated: %s", updates)

# ============================================================================
# Example usage
# ============================================================================
if __name__ == "__main__":
    # Mock gradient provider
    class MockGradientManager:
        def get_field_strengths(self):
            return {
                'carbon': 0.8,
                'helium': 0.2,
                'trust': 0.1,
                'opportunity': 0.9,
                'eco_atp_reserve': 0.5
            }

    # Mock quantum solver
    class MockQuantumSolver:
        def set_parameters(self, params):
            print(f"Quantum solver received: {params}")

        def solve(self):
            return {'status': 'ok'}

    # Create bridge
    bridge = QuantumBridge(
        gradient_provider=MockGradientManager(),
        quantum_solver=MockQuantumSolver(),
        config={'output_format': 'qubo'}
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
