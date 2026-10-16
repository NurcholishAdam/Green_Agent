"""
common_state.py — Canonical state-vector schema shared across consumers.

The causal SCM, the federated aggregator, and the XAI explainer all consume
a state vector. To avoid divergence between the profiler and the accelerator,
we define the schema exactly once here.
"""

from __future__ import annotations

from typing import Any, Dict, Tuple

import numpy as np

PROFILER_SCHEMA_VERSION = "gpu_profiler_v4"
ACCELERATOR_SCHEMA_VERSION = "gpu_accelerator_v14"
CAUSAL_SCHEMA_VERSION = "causal_scm_v3"
FEDERATED_SCHEMA_VERSION = "federated_v3"
STL_SCHEMA_VERSION = "stl_v3"

STATE_VECTOR_FIELDS: Tuple[str, ...] = (
    "gpu_utilization_pct",
    "gpu_power_watts_norm",
    "gpu_temp_c_norm",
    "cpu_utilization_pct",
    "carbon_intensity_norm",
    "carbon_price_norm",
    "rec_kwh_norm",
    "uncertainty_score",
)
STATE_VECTOR_DIM = len(STATE_VECTOR_FIELDS)

STATE_VECTOR_NORMALIZATION: Dict[str, float] = {
    "gpu_power_watts": 400.0,
    "gpu_temp_c": 100.0,
    "carbon_intensity_g_per_kwh": 1000.0,
    "carbon_price_per_kg": 0.5,
    "rec_kwh": 100.0,
}


def state_vector_from_metrics(m: Dict[str, Any]) -> np.ndarray:
    """
    Build a normalized state vector from a metrics dict.

    Missing keys default to zero. The output shape is always
    (STATE_VECTOR_DIM,).
    """
    return np.array([
        float(m.get("gpu_utilization_pct", 0.0)),
        float(m.get("gpu_power_watts", 0.0)) / STATE_VECTOR_NORMALIZATION["gpu_power_watts"],
        float(m.get("gpu_temp_c", 0.0)) / STATE_VECTOR_NORMALIZATION["gpu_temp_c"],
        float(m.get("cpu_utilization_pct", 0.0)),
        float(m.get("carbon_intensity_g_per_kwh", 400.0)) / STATE_VECTOR_NORMALIZATION["carbon_intensity_g_per_kwh"],
        float(m.get("carbon_price_per_kg", 0.0)) / STATE_VECTOR_NORMALIZATION["carbon_price_per_kg"],
        float(m.get("rec_kwh_available", 0.0)) / STATE_VECTOR_NORMALIZATION["rec_kwh"],
        float(m.get("uncertainty_score", 0.0)),
    ], dtype=np.float64)


def pad_or_truncate(vec: np.ndarray, dim: int = STATE_VECTOR_DIM) -> np.ndarray:
    """Ensure a vector has exactly `dim` elements."""
    v = np.asarray(vec, dtype=np.float64).ravel()
    if v.size == dim:
        return v
    if v.size < dim:
        return np.pad(v, (0, dim - v.size))
    return v[:dim]
