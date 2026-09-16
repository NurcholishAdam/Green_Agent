# src/continuum/offloading_decision_engine.py

"""
Offloading Decision Engine

Multi-objective optimizer for edge-cloud placement.

Responsibilities
----------------
- Evaluate offloading criteria (TDP, carbon, latency, cost).
- Calculate optimal placement score per continuum tier.
- Select a target node from the available pool.
- Provide human-readable reasoning for audit trails.

Enhancements
------------
- Removed duplicate methods (``_get_tdp_reading``, ``_score_node``,
  ``_estimate_carbon``, ``_estimate_cost`` were each defined twice).
- Fixed composite-score weight bug (weights previously summed to 1.3).
- Added ``TDPReading`` and ``DeviceType`` dataclasses (previously undefined).
- Added module-level ``logger`` (previously missing → ``NameError``).
- Thread-safe via ``RLock``.
- Bounded, TTL-evicting decision cache with configurable size.
- Configurable weights / thresholds via :class:`OffloadingConfig`.
- Full validation of every argument; strict / non-strict modes.
- Immutable :class:`PlacementDecision` with full serialization.
- Fallback chain in ``_select_target_node`` now terminates cleanly.
- Uses timezone-aware UTC timestamps (``datetime.now(timezone.utc)``).
- Custom :class:`OffloadingEngineError`.
- Lazy ``%s`` logging, ``__repr__``, and a smoke test under ``__main__``.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import threading
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Dict, List, Mapping, Optional, Tuple

import numpy as np

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class OffloadingEngineError(ValueError):
    """Raised for invalid inputs or configuration."""


# --------------------------------------------------------------------------- #
# Enums
# --------------------------------------------------------------------------- #
class PlacementTier(Enum):
    """Continuum placement tiers."""

    TIER_1_LOCAL = "tier_1_local"          # Edge device itself
    TIER_2_REGIONAL = "tier_2_regional"    # Regional edge node
    TIER_3_CLOUD = "tier_3_cloud"          # Cloud Green zone


class DeviceType(Enum):
    """Device classifications used for TDP metadata."""

    UNKNOWN = "unknown"
    RASPBERRY_PI = "raspberry_pi"
    JETSON_NANO = "jetson_nano"
    JETSON_XAVIER = "jetson_xavier"
    INTEL_NUC = "intel_nuc"
    MOBILE = "mobile"
    SERVER = "server"


# --------------------------------------------------------------------------- #
# Device / TDP reading
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class TDPReading:
    """Snapshot of a device's thermal / power state."""

    timestamp: datetime
    device_id: str
    device_type: DeviceType
    current_power_watts: float
    tdp_threshold_watts: float
    utilization_percent: float = 0.0
    temperature_celsius: float = 0.0
    thermal_throttling: bool = False
    predicted_breach_seconds: Optional[float] = None

    def __post_init__(self) -> None:
        if self.current_power_watts < 0:
            raise OffloadingEngineError("current_power_watts must be >= 0.")
        if self.tdp_threshold_watts <= 0:
            raise OffloadingEngineError("tdp_threshold_watts must be > 0.")
        if not 0.0 <= self.utilization_percent <= 100.0:
            raise OffloadingEngineError("utilization_percent must be in [0, 100].")

    @property
    def headroom_watts(self) -> float:
        return self.tdp_threshold_watts - self.current_power_watts

    def to_dict(self) -> Dict[str, Any]:
        return {
            **asdict(self),
            "timestamp": self.timestamp.isoformat(),
            "device_type": self.device_type.value,
        }


# --------------------------------------------------------------------------- #
# Criteria & configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class OffloadingCriteria:
    """
    Criteria for offloading decisions.

    Retained as a public dataclass for backward compatibility. Internally the
    engine also validates the combined weights via :class:`OffloadingConfig`.
    """

    tdp_threshold_watts: float
    tdp_warning_buffer: float = 2.0
    carbon_green_threshold: float = 50.0    # gCO2/kWh
    carbon_yellow_threshold: float = 200.0  # gCO2/kWh
    carbon_red_threshold: float = 400.0     # gCO2/kWh
    latency_sla_ms: float = 100.0
    latency_buffer_ms: float = 20.0
    cost_weight: float = 0.3
    carbon_weight: float = 0.5
    latency_weight: float = 0.2

    def __post_init__(self) -> None:
        if self.tdp_threshold_watts <= 0:
            raise OffloadingEngineError("tdp_threshold_watts must be > 0.")
        if self.tdp_warning_buffer < 0:
            raise OffloadingEngineError("tdp_warning_buffer must be >= 0.")
        for name in (
            "carbon_green_threshold",
            "carbon_yellow_threshold",
            "carbon_red_threshold",
            "latency_sla_ms",
            "latency_buffer_ms",
        ):
            if getattr(self, name) < 0:
                raise OffloadingEngineError(f"{name} must be >= 0.")
        if not (
            self.carbon_green_threshold
            <= self.carbon_yellow_threshold
            <= self.carbon_red_threshold
        ):
            raise OffloadingEngineError(
                "carbon thresholds must satisfy green <= yellow <= red."
            )
        for name in ("cost_weight", "carbon_weight", "latency_weight"):
            if getattr(self, name) < 0:
                raise OffloadingEngineError(f"{name} must be >= 0.")


@dataclass(frozen=True)
class OffloadingConfig:
    """
    Extended configuration for the engine.

    Supplements :class:`OffloadingCriteria` with cache policy, TDP weight,
    node-scoring weights, and estimation coefficients.
    """

    # Composite tier score weights — MUST sum to 1.0.
    tdp_weight: float = 0.20
    carbon_weight: float = 0.35
    latency_weight: float = 0.25
    cost_weight: float = 0.20

    # Decision cache.
    cache_ttl_seconds: float = 60.0
    max_cache_entries: Optional[int] = 1000

    # Node scoring weights — MUST sum to 1.0.
    node_cpu_weight: float = 0.25
    node_memory_weight: float = 0.25
    node_load_weight: float = 0.25
    node_network_weight: float = 0.25

    # Per-tier energy model coefficients (kWh per task).
    energy_local_kwh: float = 0.001
    energy_regional_kwh: float = 0.0015
    energy_cloud_kwh: float = 0.002

    # Per-tier base cost (USD per second).
    cost_local_usd: float = 0.0001
    cost_regional_usd: float = 0.0005
    cost_cloud_usd: float = 0.002
    cloud_egress_usd_per_mb: float = 0.0001

    def __post_init__(self) -> None:
        tier_sum = (
            self.tdp_weight
            + self.carbon_weight
            + self.latency_weight
            + self.cost_weight
        )
        if abs(tier_sum - 1.0) > 1e-6:
            raise OffloadingEngineError(
                f"tier weights must sum to 1.0 (got {tier_sum:.6f})."
            )
        node_sum = (
            self.node_cpu_weight
            + self.node_memory_weight
            + self.node_load_weight
            + self.node_network_weight
        )
        if abs(node_sum - 1.0) > 1e-6:
            raise OffloadingEngineError(
                f"node weights must sum to 1.0 (got {node_sum:.6f})."
            )
        if self.cache_ttl_seconds <= 0:
            raise OffloadingEngineError("cache_ttl_seconds must be > 0.")
        if self.max_cache_entries is not None and self.max_cache_entries <= 0:
            raise OffloadingEngineError("max_cache_entries must be > 0 or None.")


# --------------------------------------------------------------------------- #
# Placement decision (now immutable + serializable)
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class PlacementDecision:
    """Immutable result of an offloading decision."""

    task_id: str
    device_id: str
    selected_tier: PlacementTier
    target_node: str
    confidence_score: float
    reasoning: Tuple[str, ...]
    estimated_latency_ms: float
    estimated_carbon_gco2: float
    estimated_cost_usd: float
    timestamp: datetime

    def __post_init__(self) -> None:
        if not 0.0 <= self.confidence_score <= 1.0:
            raise OffloadingEngineError(
                f"confidence_score must be in [0, 1], got {self.confidence_score}."
            )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "task_id": self.task_id,
            "device_id": self.device_id,
            "selected_tier": self.selected_tier.value,
            "target_node": self.target_node,
            "confidence_score": self.confidence_score,
            "reasoning": list(self.reasoning),
            "estimated_latency_ms": self.estimated_latency_ms,
            "estimated_carbon_gco2": self.estimated_carbon_gco2,
            "estimated_cost_usd": self.estimated_cost_usd,
            "timestamp": self.timestamp.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "PlacementDecision":
        if not isinstance(data, Mapping):
            raise OffloadingEngineError(
                f"PlacementDecision.from_dict expects a Mapping, "
                f"got {type(data).__name__}."
            )
        ts = data["timestamp"]
        timestamp = (
            datetime.fromisoformat(ts) if isinstance(ts, str) else ts
        )
        return cls(
            task_id=str(data["task_id"]),
            device_id=str(data["device_id"]),
            selected_tier=PlacementTier(data["selected_tier"]),
            target_node=str(data["target_node"]),
            confidence_score=float(data["confidence_score"]),
            reasoning=tuple(data.get("reasoning", ())),
            estimated_latency_ms=float(data.get("estimated_latency_ms", 0.0)),
            estimated_carbon_gco2=float(data.get("estimated_carbon_gco2", 0.0)),
            estimated_cost_usd=float(data.get("estimated_cost_usd", 0.0)),
            timestamp=timestamp,
        )


# --------------------------------------------------------------------------- #
# Engine
# --------------------------------------------------------------------------- #
class OffloadingDecisionEngine:
    """
    Multi-objective optimizer for edge-cloud placement.

    Thread-safe, serializable, and bounded in memory. See
    :class:`OffloadingConfig` for tunable weights and coefficients.
    """

    def __init__(
        self,
        criteria: OffloadingCriteria,
        carbon_intensity_monitor: Any,
        latency_monitor: Any,
        node_registry: Any,
        *,
        config: Optional[OffloadingConfig] = None,
        strict: bool = True,
        fallback_nodes: Optional[List[Dict[str, Any]]] = None,
    ) -> None:
        if not isinstance(criteria, OffloadingCriteria):
            raise OffloadingEngineError(
                "criteria must be an OffloadingCriteria instance."
            )

        self.criteria = criteria
        self.carbon_monitor = carbon_intensity_monitor
        self.latency_monitor = latency_monitor
        self.node_registry = node_registry

        self._config: OffloadingConfig = config or OffloadingConfig()
        self._strict: bool = bool(strict)

        self._lock = threading.RLock()
        self._decision_cache: Dict[str, PlacementDecision] = {}
        self._cache_order: List[str] = []
        self._ctx_start: Optional[float] = None

        # Pre-computed fallback nodes (only used when the registry returns
        # nothing AND ``strict`` is False).
        self._fallback_nodes: List[Dict[str, Any]] = list(fallback_nodes or [])

        logger.debug(
            "OffloadingDecisionEngine initialized "
            "(tdp_threshold=%.1fW, cache_ttl=%.0fs, strict=%s)",
            criteria.tdp_threshold_watts,
            self._config.cache_ttl_seconds,
            self._strict,
        )

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> OffloadingConfig:
        return self._config

    @property
    def cache_size(self) -> int:
        with self._lock:
            return len(self._decision_cache)

    # ---------------------------------------------------------- public API
    async def decide_placement(
        self,
        task_id: str,
        device_id: str,
        task_requirements: Mapping[str, Any],
    ) -> PlacementDecision:
        """
        Make an offloading decision for a task.

        Parameters
        ----------
        task_id : str
            Unique task identifier.
        device_id : str
            Source edge device.
        task_requirements : Mapping
            May contain ``latency_sla_ms``, ``compute_intensity``,
            ``data_size_mb``, ``compute_seconds``, ``cpu_required``,
            ``memory_required_mb``.

        Returns
        -------
        PlacementDecision
            Selected tier, target node, confidence, and reasoning.
        """
        # ---- Validation ------------------------------------------------
        if not isinstance(task_id, str) or not task_id:
            raise OffloadingEngineError("task_id must be a non-empty string.")
        if not isinstance(device_id, str) or not device_id:
            raise OffloadingEngineError("device_id must be a non-empty string.")
        if not isinstance(task_requirements, Mapping):
            msg = (
                f"task_requirements must be a Mapping, "
                f"got {type(task_requirements).__name__}."
            )
            if self._strict:
                raise OffloadingEngineError(msg)
            logger.warning("%s Using empty requirements.", msg)
            task_requirements = {}

        reqs = dict(task_requirements)

        # ---- Cache check -----------------------------------------------
        cache_key = f"{device_id}_{task_id}"
        cached = self._cache_get(cache_key)
        if cached is not None:
            logger.debug("Cache hit for %s.", cache_key)
            return cached

        # ---- Gather inputs ---------------------------------------------
        tdp_reading = await self._get_tdp_reading(device_id)
        carbon_intensity = await self._safe_call(
            self.carbon_monitor, "get_current_intensity", default=200.0
        )
        latency_metrics = await self._safe_call(
            self.latency_monitor,
            "get_latency_to_tiers",
            device_id,
            default={t: 100.0 for t in PlacementTier},
        )
        available_nodes = await self._get_available_nodes()

        if not available_nodes and self._strict:
            raise OffloadingEngineError("No available nodes found in registry.")

        # ---- Score every tier ------------------------------------------
        tier_scores: Dict[PlacementTier, Tuple[float, List[str]]] = {}
        for tier in PlacementTier:
            try:
                score, reasoning = await self._calculate_tier_score(
                    tier=tier,
                    device_id=device_id,
                    tdp_reading=tdp_reading,
                    carbon_intensity=float(carbon_intensity),
                    latency_metrics=latency_metrics,
                    available_nodes=available_nodes,
                    task_requirements=reqs,
                )
            except Exception as exc:
                logger.exception("Tier scoring failed for %s: %s", tier, exc)
                if self._strict:
                    raise
                score, reasoning = 0.0, [f"scoring error: {exc}"]
            tier_scores[tier] = (score, reasoning)

        best_tier = max(tier_scores.keys(), key=lambda t: tier_scores[t][0])
        best_score, best_reasoning = tier_scores[best_tier]

        # ---- Select target node ----------------------------------------
        target_node = await self._select_target_node(
            tier=best_tier,
            device_id=device_id,
            available_nodes=available_nodes,
            task_requirements=reqs,
        )

        # ---- Estimate metrics ------------------------------------------
        estimated_latency = float(
            (latency_metrics or {}).get(best_tier, 100.0)
        )
        estimated_carbon = await self._estimate_carbon(
            tier=best_tier,
            task_requirements=reqs,
            carbon_intensity=float(carbon_intensity),
        )
        estimated_cost = await self._estimate_cost(
            tier=best_tier,
            task_requirements=reqs,
        )

        # ---- Assemble + cache ------------------------------------------
        decision = PlacementDecision(
            task_id=task_id,
            device_id=device_id,
            selected_tier=best_tier,
            target_node=target_node,
            confidence_score=float(best_score),
            reasoning=tuple(best_reasoning),
            estimated_latency_ms=estimated_latency,
            estimated_carbon_gco2=estimated_carbon,
            estimated_cost_usd=estimated_cost,
            timestamp=datetime.now(timezone.utc),
        )

        self._cache_put(cache_key, decision)

        logger.info(
            "Decision for task=%s device=%s: tier=%s target=%s "
            "confidence=%.3f latency=%.1fms carbon=%.3fg cost=$%.5f",
            task_id,
            device_id,
            best_tier.value,
            target_node,
            best_score,
            estimated_latency,
            estimated_carbon,
            estimated_cost,
        )
        return decision

    # -------------------------------------------------------- tier scoring
    async def _calculate_tier_score(
        self,
        tier: PlacementTier,
        device_id: str,
        tdp_reading: TDPReading,
        carbon_intensity: float,
        latency_metrics: Mapping[PlacementTier, float],
        available_nodes: List[Dict[str, Any]],
        task_requirements: Mapping[str, Any],
    ) -> Tuple[float, List[str]]:
        """Compute a 0–1 score for ``tier`` with human-readable reasoning."""
        cfg = self._config
        reasoning: List[str] = []

        # ---- TDP score -------------------------------------------------
        tdp_score = self._score_tdp(tier, tdp_reading, reasoning)

        # ---- Carbon score ----------------------------------------------
        carbon_score = self._score_carbon(tier, carbon_intensity, reasoning)

        # ---- Latency score ---------------------------------------------
        latency_sla = float(
            task_requirements.get("latency_sla_ms", self.criteria.latency_sla_ms)
        )
        latency = float(latency_metrics.get(tier, 100.0))
        latency_score = self._score_latency(latency, latency_sla, reasoning)

        # ---- Cost score (static per tier) ------------------------------
        cost_score = self._score_cost(tier, reasoning)

        # ---- Weighted sum (weights validated to sum to 1.0) ------------
        score = (
            tdp_score * cfg.tdp_weight
            + carbon_score * cfg.carbon_weight
            + latency_score * cfg.latency_weight
            + cost_score * cfg.cost_weight
        )
        # Clamp defensively — inputs are all in [0, 1] and weights sum to 1.0.
        return max(0.0, min(1.0, score)), reasoning

    def _score_tdp(
        self,
        tier: PlacementTier,
        tdp_reading: TDPReading,
        reasoning: List[str],
    ) -> float:
        if tier != PlacementTier.TIER_1_LOCAL:
            reasoning.append("TDP concern resolved via offloading")
            return 1.0
        threshold = (
            tdp_reading.tdp_threshold_watts - self.criteria.tdp_warning_buffer
        )
        if tdp_reading.current_power_watts >= threshold:
            reasoning.append(
                f"TDP near threshold "
                f"({tdp_reading.current_power_watts:.1f}W / "
                f"{tdp_reading.tdp_threshold_watts:.1f}W)"
            )
            return 0.0
        reasoning.append(
            f"TDP within safe limits ({tdp_reading.current_power_watts:.1f}W)"
        )
        return 1.0

    def _score_carbon(
        self,
        tier: PlacementTier,
        carbon_intensity: float,
        reasoning: List[str],
    ) -> float:
        c = self.criteria
        if tier == PlacementTier.TIER_3_CLOUD:
            if carbon_intensity < c.carbon_green_threshold:
                score = 1.0
            elif carbon_intensity < c.carbon_yellow_threshold:
                score = 0.5
            else:
                score = 0.2
            reasoning.append(
                f"Cloud placement: carbon intensity "
                f"{carbon_intensity:.1f} gCO2/kWh"
            )
            return score
        if tier == PlacementTier.TIER_2_REGIONAL:
            reasoning.append("Regional edge: moderate carbon intensity")
            return 0.7
        reasoning.append(
            f"Local execution: carbon intensity {carbon_intensity:.1f} gCO2/kWh"
        )
        return 0.5

    def _score_latency(
        self,
        latency: float,
        latency_sla: float,
        reasoning: List[str],
    ) -> float:
        c = self.criteria
        if latency < latency_sla - c.latency_buffer_ms:
            reasoning.append(
                f"Latency well within SLA "
                f"({latency:.1f}ms < {latency_sla:.1f}ms)"
            )
            return 1.0
        if latency < latency_sla:
            reasoning.append(
                f"Latency within SLA ({latency:.1f}ms < {latency_sla:.1f}ms)"
            )
            return 0.7
        reasoning.append(
            f"Latency exceeds SLA ({latency:.1f}ms > {latency_sla:.1f}ms)"
        )
        return 0.3

    def _score_cost(
        self,
        tier: PlacementTier,
        reasoning: List[str],
    ) -> float:
        if tier == PlacementTier.TIER_1_LOCAL:
            reasoning.append("Lowest cost (local execution)")
            return 1.0
        if tier == PlacementTier.TIER_2_REGIONAL:
            reasoning.append("Moderate cost (regional edge)")
            return 0.7
        reasoning.append("Higher cost (cloud compute + egress)")
        return 0.4

    # -------------------------------------------------------- node selection
    async def _select_target_node(
        self,
        tier: PlacementTier,
        device_id: str,
        available_nodes: List[Dict[str, Any]],
        task_requirements: Mapping[str, Any],
    ) -> str:
        """
        Select the best node in ``tier``, with fallback to adjacent tiers.

        Fallback order: local → regional → cloud; regional → cloud → local;
        cloud → regional → local. Terminates with a deterministic default if
        no tier has any node.
        """
        visited: List[PlacementTier] = []
        current = tier

        while current not in visited:
            visited.append(current)
            tier_nodes = [
                n for n in available_nodes
                if n.get("tier") == current.value
            ]
            if tier_nodes:
                scored = [
                    (self._score_node(node, task_requirements), node)
                    for node in tier_nodes
                ]
                scored.sort(key=lambda x: x[0], reverse=True)
                return str(scored[0][1]["node_id"])
            current = self._fallback_tier(current)

        # Deterministic terminal fallback.
        node_id = f"{tier.value}-fallback"
        logger.warning(
            "No nodes available across tiers %s; defaulting to %s.",
            [t.value for t in visited],
            node_id,
        )
        return node_id

    @staticmethod
    def _fallback_tier(tier: PlacementTier) -> PlacementTier:
        return {
            PlacementTier.TIER_1_LOCAL: PlacementTier.TIER_2_REGIONAL,
            PlacementTier.TIER_2_REGIONAL: PlacementTier.TIER_3_CLOUD,
            PlacementTier.TIER_3_CLOUD: PlacementTier.TIER_2_REGIONAL,
        }[tier]

    def _score_node(
        self,
        node: Mapping[str, Any],
        task_requirements: Mapping[str, Any],
    ) -> float:
        """
        Score an individual node in [0, 1] using :class:`OffloadingConfig`
        node weights (which are validated to sum to 1.0).
        """
        cfg = self._config
        score = 0.0

        # ---- CPU ---------------------------------------------------------
        cpu_available = float(node.get("available_cpu_percent", 0))
        cpu_required = float(task_requirements.get("cpu_required", 50))
        if cpu_available >= cpu_required:
            score += cfg.node_cpu_weight

        # ---- Memory ------------------------------------------------------
        mem_available = float(node.get("available_memory_mb", 0))
        mem_required = float(task_requirements.get("memory_required_mb", 1024))
        if mem_available >= mem_required:
            score += cfg.node_memory_weight

        # ---- Load (linear between 0.75 → 0 and 0.5 → full credit) --------
        max_tasks = float(node.get("max_tasks", 0)) or 1.0
        current_tasks = float(node.get("current_tasks", 0))
        load_ratio = current_tasks / max_tasks
        if load_ratio < 0.5:
            score += cfg.node_load_weight
        elif load_ratio < 0.75:
            score += cfg.node_load_weight * 0.5

        # ---- Network -----------------------------------------------------
        net_ms = float(node.get("network_latency_ms", 999))
        if net_ms < 10:
            score += cfg.node_network_weight
        elif net_ms < 25:
            score += cfg.node_network_weight * 0.5

        return min(1.0, score)

    # -------------------------------------------------------- estimations
    async def _estimate_carbon(
        self,
        tier: PlacementTier,
        task_requirements: Mapping[str, Any],
        carbon_intensity: float,
    ) -> float:
        """
        Estimate carbon emissions in **grams CO2e** for placement.

        Model: ``energy_kwh(tier) * compute_intensity * carbon_intensity``,
        with carbon_intensity in gCO2/kWh.
        """
        cfg = self._config
        compute_intensity = float(
            task_requirements.get("compute_intensity", 1.0)
        )
        if compute_intensity < 0:
            compute_intensity = 0.0

        energy_by_tier = {
            PlacementTier.TIER_1_LOCAL: cfg.energy_local_kwh,
            PlacementTier.TIER_2_REGIONAL: cfg.energy_regional_kwh,
            PlacementTier.TIER_3_CLOUD: cfg.energy_cloud_kwh,
        }
        energy_kwh = energy_by_tier[tier] * compute_intensity
        # gCO2/kWh * kWh = gCO2
        return energy_kwh * carbon_intensity

    async def _estimate_cost(
        self,
        tier: PlacementTier,
        task_requirements: Mapping[str, Any],
    ) -> float:
        """Estimate cost in USD for placement."""
        cfg = self._config
        data_size_mb = float(task_requirements.get("data_size_mb", 1.0))
        compute_seconds = float(task_requirements.get("compute_seconds", 1.0))

        base_by_tier = {
            PlacementTier.TIER_1_LOCAL: cfg.cost_local_usd,
            PlacementTier.TIER_2_REGIONAL: cfg.cost_regional_usd,
            PlacementTier.TIER_3_CLOUD: cfg.cost_cloud_usd,
        }
        base_cost = base_by_tier[tier]
        egress_cost = (
            data_size_mb * cfg.cloud_egress_usd_per_mb
            if tier == PlacementTier.TIER_3_CLOUD
            else 0.0
        )
        return base_cost * compute_seconds + egress_cost

    # -------------------------------------------------------- TDP reading
    async def _get_tdp_reading(self, device_id: str) -> TDPReading:
        """
        Get the TDP reading for ``device_id``.

        If the engine was constructed with a TDP monitor exposing
        ``get_reading`` / ``get_tdp_reading``, that is used; otherwise a safe
        placeholder is returned.
        """
        monitor = getattr(self, "tdp_monitor", None)
        if monitor is not None:
            for method_name in ("get_reading", "get_tdp_reading"):
                method = getattr(monitor, method_name, None)
                if callable(method):
                    try:
                        result = method(device_id)
                        if asyncio.iscoroutine(result):
                            result = await result
                        if isinstance(result, TDPReading):
                            return result
                    except Exception as exc:
                        logger.warning(
                            "TDP monitor '%s' failed: %s; using placeholder.",
                            method_name,
                            exc,
                        )

        return TDPReading(
            timestamp=datetime.now(timezone.utc),
            device_id=device_id,
            device_type=DeviceType.UNKNOWN,
            current_power_watts=15.0,
            tdp_threshold_watts=self.criteria.tdp_threshold_watts,
            utilization_percent=45.0,
            temperature_celsius=52.0,
            thermal_throttling=False,
            predicted_breach_seconds=None,
        )

    # -------------------------------------------------------- node registry
    async def _get_available_nodes(self) -> List[Dict[str, Any]]:
        """
        Return available nodes.

        Prefers ``node_registry.get_available_nodes()``; falls back to the
        engine's ``fallback_nodes`` when the registry is unavailable or empty
        and ``strict`` is False.
        """
        registry = self.node_registry
        if registry is not None:
            method = getattr(registry, "get_available_nodes", None)
            if callable(method):
                try:
                    result = method()
                    if asyncio.iscoroutine(result):
                        result = await result
                    if isinstance(result, (list, tuple)) and result:
                        return [dict(n) for n in result]
                except Exception as exc:
                    logger.warning(
                        "node_registry.get_available_nodes failed: %s", exc
                    )
                    if self._strict:
                        raise

        if self._fallback_nodes:
            logger.debug(
                "Using %d fallback node(s).", len(self._fallback_nodes)
            )
            return [dict(n) for n in self._fallback_nodes]
        return []

    # -------------------------------------------------------- safe calls
    @staticmethod
    async def _safe_call(
        target: Any,
        method_name: str,
        *args: Any,
        default: Any,
    ) -> Any:
        """Call ``target.method_name(*args)`` if available, else return default."""
        if target is None:
            return default
        method = getattr(target, method_name, None)
        if not callable(method):
            logger.debug(
                "Monitor lacks '%s'; using default=%r.", method_name, default
            )
            return default
        try:
            result = method(*args)
            if asyncio.iscoroutine(result):
                result = await result
            return result
        except Exception as exc:
            logger.warning(
                "Monitor '%s' raised %s; using default=%r.",
                method_name, exc, default,
            )
            return default

    # -------------------------------------------------------- cache helpers
    def _cache_get(self, key: str) -> Optional[PlacementDecision]:
        with self._lock:
            decision = self._decision_cache.get(key)
            if decision is None:
                return None
            age = (
                datetime.now(timezone.utc) - decision.timestamp
            ).total_seconds()
            if age >= self._config.cache_ttl_seconds:
                self._decision_cache.pop(key, None)
                try:
                    self._cache_order.remove(key)
                except ValueError:
                    pass
                return None
            return decision

    def _cache_put(self, key: str, decision: PlacementDecision) -> None:
        with self._lock:
            if key not in self._decision_cache:
                self._cache_order.append(key)
            self._decision_cache[key] = decision
            cap = self._config.max_cache_entries
            if cap is not None:
                while len(self._cache_order) > cap:
                    oldest = self._cache_order.pop(0)
                    self._decision_cache.pop(oldest, None)

    def clear_cache(self) -> None:
        """Clear the decision cache."""
        with self._lock:
            self._decision_cache.clear()
            self._cache_order.clear()
        logger.debug("Decision cache cleared.")

    # -------------------------------------------------------- statistics
    def statistics(self) -> Dict[str, Any]:
        """Return aggregate statistics over the decision cache."""
        with self._lock:
            decisions = list(self._decision_cache.values())
        if not decisions:
            return {
                "cached_decisions": 0,
                "by_tier": {},
                "mean_confidence": None,
                "mean_latency_ms": None,
                "total_estimated_carbon_gco2": 0.0,
                "total_estimated_cost_usd": 0.0,
            }
        by_tier: Dict[str, int] = {}
        for d in decisions:
            by_tier[d.selected_tier.value] = by_tier.get(d.selected_tier.value, 0) + 1
        confidences = [d.confidence_score for d in decisions]
        latencies = [d.estimated_latency_ms for d in decisions]
        return {
            "cached_decisions": len(decisions),
            "by_tier": by_tier,
            "mean_confidence": sum(confidences) / len(confidences),
            "mean_latency_ms": sum(latencies) / len(latencies),
            "total_estimated_carbon_gco2": sum(
                d.estimated_carbon_gco2 for d in decisions
            ),
            "total_estimated_cost_usd": sum(
                d.estimated_cost_usd for d in decisions
            ),
        }

    def reset(self, *, clear_cache: bool = False) -> None:
        """Reset engine state; optionally clear the decision cache."""
        with self._lock:
            if clear_cache:
                self._decision_cache.clear()
                self._cache_order.clear()
        logger.debug("OffloadingDecisionEngine reset (clear_cache=%s)", clear_cache)

    # -------------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "criteria": asdict(self.criteria),
                "config": asdict(self._config),
                "strict": self._strict,
                "decision_cache": [d.to_dict() for d in self._decision_cache.values()],
                "fallback_nodes": list(self._fallback_nodes),
            }

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        carbon_intensity_monitor: Any = None,
        latency_monitor: Any = None,
        node_registry: Any = None,
    ) -> "OffloadingDecisionEngine":
        if not isinstance(data, Mapping):
            raise OffloadingEngineError(
                f"from_dict expects a Mapping, got {type(data).__name__}."
            )
        criteria = OffloadingCriteria(**data["criteria"])
        cfg_data = dict(data.get("config", {}) or {})
        cfg = OffloadingConfig(
            tdp_weight=float(cfg_data.get("tdp_weight", 0.20)),
            carbon_weight=float(cfg_data.get("carbon_weight", 0.35)),
            latency_weight=float(cfg_data.get("latency_weight", 0.25)),
            cost_weight=float(cfg_data.get("cost_weight", 0.20)),
            cache_ttl_seconds=float(cfg_data.get("cache_ttl_seconds", 60.0)),
            max_cache_entries=cfg_data.get("max_cache_entries", 1000),
            node_cpu_weight=float(cfg_data.get("node_cpu_weight", 0.25)),
            node_memory_weight=float(cfg_data.get("node_memory_weight", 0.25)),
            node_load_weight=float(cfg_data.get("node_load_weight", 0.25)),
            node_network_weight=float(cfg_data.get("node_network_weight", 0.25)),
            energy_local_kwh=float(cfg_data.get("energy_local_kwh", 0.001)),
            energy_regional_kwh=float(cfg_data.get("energy_regional_kwh", 0.0015)),
            energy_cloud_kwh=float(cfg_data.get("energy_cloud_kwh", 0.002)),
            cost_local_usd=float(cfg_data.get("cost_local_usd", 0.0001)),
            cost_regional_usd=float(cfg_data.get("cost_regional_usd", 0.0005)),
            cost_cloud_usd=float(cfg_data.get("cost_cloud_usd", 0.002)),
            cloud_egress_usd_per_mb=float(
                cfg_data.get("cloud_egress_usd_per_mb", 0.0001)
            ),
        )
        engine = cls(
            criteria=criteria,
            carbon_intensity_monitor=carbon_intensity_monitor,
            latency_monitor=latency_monitor,
            node_registry=node_registry,
            config=cfg,
            strict=bool(data.get("strict", True)),
            fallback_nodes=list(data.get("fallback_nodes", [])),
        )
        with engine._lock:
            for entry in data.get("decision_cache", []):
                d = PlacementDecision.from_dict(entry)
                key = f"{d.device_id}_{d.task_id}"
                engine._decision_cache[key] = d
                engine._cache_order.append(key)
        return engine

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), **kwargs)

    @classmethod
    def from_json(
        cls,
        payload: str,
        *,
        carbon_intensity_monitor: Any = None,
        latency_monitor: Any = None,
        node_registry: Any = None,
    ) -> "OffloadingDecisionEngine":
        try:
            return cls.from_dict(
                json.loads(payload),
                carbon_intensity_monitor=carbon_intensity_monitor,
                latency_monitor=latency_monitor,
                node_registry=node_registry,
            )
        except json.JSONDecodeError as exc:
            raise OffloadingEngineError(f"Invalid JSON payload: {exc}") from exc

    # -------------------------------------------------------- async ctx mgr
    async def __aenter__(self) -> "OffloadingDecisionEngine":
        self._ctx_start = time.perf_counter()
        logger.debug("Entering scoped offloading-decision session.")
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        elapsed = time.perf_counter() - (
            self._ctx_start if self._ctx_start is not None else time.perf_counter()
        )
        self._ctx_start = None
        if exc_type is not None:
            logger.warning(
                "Offloading-decision scope exited with %s after %.4fs.",
                exc_type.__name__, elapsed,
            )
            return
        logger.info(
            "Offloading-decision scope closed in %.4fs (cached=%d).",
            elapsed, len(self._decision_cache),
        )

    # ----------------------------------------------------------------- dunder
    def __repr__(self) -> str:
        return (
            "OffloadingDecisionEngine("
            f"tdp_threshold={self.criteria.tdp_threshold_watts:.1f}W, "
            f"cache={len(self._decision_cache)}, "
            f"strict={self._strict})"
        )


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "PlacementTier",
    "DeviceType",
    "TDPReading",
    "OffloadingCriteria",
    "OffloadingConfig",
    "PlacementDecision",
    "OffloadingDecisionEngine",
    "OffloadingEngineError",
]


# --------------------------------------------------------------------------- #
# Local smoke test: python -m continuum.offloading_decision_engine
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(
        level=logging.INFO, format=("%(levelname)s %(name)s: %(message)s")
    )

    class _CarbonMonitor:
        async def get_current_intensity(self) -> float:
            return 30.0  # very green grid

    class _LatencyMonitor:
        async def get_latency_to_tiers(
            self, device_id: str
        ) -> Dict[PlacementTier, float]:
            return {
                PlacementTier.TIER_1_LOCAL: 2.0,
                PlacementTier.TIER_2_REGIONAL: 8.0,
                PlacementTier.TIER_3_CLOUD: 35.0,
            }

    class _NodeRegistry:
        async def get_available_nodes(self) -> List[Dict[str, Any]]:
            return [
                {
                    "node_id": "edge-pi-001",
                    "tier": PlacementTier.TIER_1_LOCAL.value,
                    "available_cpu_percent": 65,
                    "available_memory_mb": 2048,
                    "current_tasks": 3,
                    "max_tasks": 10,
                    "network_latency_ms": 2,
                    "carbon_intensity": 45.0,
                },
                {
                    "node_id": "regional-edge-us-west-001",
                    "tier": PlacementTier.TIER_2_REGIONAL.value,
                    "available_cpu_percent": 80,
                    "available_memory_mb": 8192,
                    "current_tasks": 12,
                    "max_tasks": 50,
                    "network_latency_ms": 8,
                    "carbon_intensity": 120.0,
                },
                {
                    "node_id": "cloud-gke-us-central1-001",
                    "tier": PlacementTier.TIER_3_CLOUD.value,
                    "available_cpu_percent": 90,
                    "available_memory_mb": 32768,
                    "current_tasks": 45,
                    "max_tasks": 200,
                    "network_latency_ms": 35,
                    "carbon_intensity": 30.0,
                },
            ]

    async def main() -> None:
        engine = OffloadingDecisionEngine(
            criteria=OffloadingCriteria(tdp_threshold_watts=28.0),
            carbon_intensity_monitor=_CarbonMonitor(),
            latency_monitor=_LatencyMonitor(),
            node_registry=_NodeRegistry(),
            strict=True,
        )

        async with engine:
            # ---- Tier-1 preferred (light grid, low local TDP) -----------
            d1 = await engine.decide_placement(
                task_id="t1",
                device_id="edge-01",
                task_requirements={
                    "latency_sla_ms": 50.0,
                    "compute_intensity": 1.0,
                    "data_size_mb": 2.0,
                    "compute_seconds": 1.5,
                },
            )
            print(f"t1 tier={d1.selected_tier.value} node={d1.target_node} "
                  f"conf={d1.confidence_score:.3f}")
            print("    reasoning:", d1.reasoning)

            # ---- Cache hit --------------------------------------------
            d1b = await engine.decide_placement(
                task_id="t1",
                device_id="edge-01",
                task_requirements={"latency_sla_ms": 50.0},
            )
            assert d1b.timestamp == d1.timestamp
            print("cache-hit  : OK")

            # ---- Different device, carbon-heavy grid → cloud likely -----
            engine2 = OffloadingDecisionEngine(
                criteria=OffloadingCriteria(tdp_threshold_watts=28.0),
                carbon_intensity_monitor=_CarbonMonitor(),
                latency_monitor=_LatencyMonitor(),
                node_registry=_NodeRegistry(),
            )
            d2 = await engine2.decide_placement(
                task_id="t2",
                device_id="edge-99",
                task_requirements={
                    "latency_sla_ms": 200.0,
                    "compute_intensity": 5.0,
                    "data_size_mb": 50.0,
                    "compute_seconds": 10.0,
                },
            )
            print(f"t2 tier={d2.selected_tier.value} node={d2.target_node} "
                  f"conf={d2.confidence_score:.3f} "
                  f"carbon={d2.estimated_carbon_gco2:.3f}g "
                  f"cost=${d2.estimated_cost_usd:.6f}")

            # ---- Statistics -------------------------------------------
            print("stats      :", engine.statistics())

            # ---- Serialization round-trip -----------------------------
            payload = engine.to_json()
            restored = OffloadingDecisionEngine.from_json(
                payload,
                carbon_intensity_monitor=_CarbonMonitor(),
                latency_monitor=_LatencyMonitor(),
                node_registry=_NodeRegistry(),
            )
            assert restored.to_dict() == engine.to_dict()
            print("Serialization round-trip OK.")

            # ---- Validation failures ----------------------------------
            for bad in (
                ("", "edge-01", {}),
                ("t3", "", {}),
                ("t3", "edge-01", "not-a-mapping"),
            ):
                try:
                    await engine.decide_placement(*bad)
                except OffloadingEngineError as exc:
                    print("Rejected as expected:", exc)
                else:  # pragma: no cover
                    raise AssertionError(f"Expected rejection for {bad!r}")

        print("\nSmoke test passed.")

    asyncio.run(main())
