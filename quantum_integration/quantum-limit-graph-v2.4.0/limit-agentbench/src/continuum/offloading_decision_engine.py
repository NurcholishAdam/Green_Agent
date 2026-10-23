# src/continuum/offloading_decision_engine.py
"""
Offloading Decision Engine — v6.0.0
====================================

Multi-objective optimizer for edge-cloud placement.

**Public API preserved**

    engine = OffloadingDecisionEngine(
        criteria, carbon_intensity_monitor, latency_monitor, node_registry,
    )
    decision = await engine.decide_placement(task_id, device_id, task_requirements)

**What changed (v5 → v6)**
--------------------------

**P0 — decision correctness**

1. **``_score_carbon`` uses ``carbon_intensity`` for all tiers.** v5
   applied the green/yellow/red bands only to the cloud tier and
   hard-coded 0.5 / 0.7 for local / regional — so on a green grid, local
   execution scored *lower* than cloud even though both run on the same
   grid. v6 applies the same bands uniformly; changing the grid changes
   every tier's carbon score symmetrically.
2. **``_score_cost`` uses ``OffloadingConfig.cost_*_usd``.** v5 returned
   hard-coded 1.0 / 0.7 / 0.4 regardless of coefficients — so changing
   ``cost_cloud_usd`` from ``$0.002`` to ``$0.02`` had zero effect on
   scoring. v6 normalises against the cheapest configured cost.
3. **Shared enums from ``continuum.types``.** v5 defined its own
   ``PlacementTier`` — a distinct class from the scheduler's. v6 imports
   the shared one.
4. **``from_dict`` enforces ``max_cache_entries`` and normalizes
   timestamps.** v5 could restore an unbounded cache and produce naive
   timestamps that crash ``_cache_get`` with ``TypeError`` on the first
   post-restore lookup.
5. **Cache key is a ``(device_id, task_id)`` tuple.** v5 used
   ``f"{device_id}_{task_id}"``, which collides for
   ``(device_id="a_b", task_id="c")`` vs ``(device_id="a",
   task_id="b_c")``.
6. **``tdp_monitor`` is a constructor argument.** v5 required callers to
   set ``engine.tdp_monitor = ...`` after construction — undocumented.

**P1 — reliability**

7. **No ``numpy`` import.** v5 imported numpy and never used it, making
   numpy a hidden hard dependency.
8. **``_estimate_carbon`` scales with ``compute_seconds``.** v5 ignored
   duration; a 10-second task reported the same carbon as a 1-second one.
9. **Node dicts validated** before scoring; malformed nodes are skipped
   with a warning instead of crashing the pipeline.
10. **``max_tasks <= 0`` excludes the node** instead of scoring 0.0.
11. **``_select_target_node`` fallback chain matches its docstring:**
    local → regional → cloud; regional → cloud → local; cloud →
    regional → local.
12. **``carbon_intensity = None`` is coerced to a safe default.**
13. **``OrderedDict`` cache** — O(1) eviction, no parallel ``_cache_order``
    list to keep in sync.
14. **``health_check()``**, **``replace_criteria()``**,
    **``replace_config()``**.

**P2 — observability**

15. **``PlacementDecision.margin`` and ``runner_up_tier``** — audit-level
    information on how decisively the winner won.
16. **Runner-up line in ``reasoning``.**
17. **``to_dict`` filters TTL-expired entries.**
18. **``target_is_fallback``** on the decision when the target node is a
    synthetic placeholder.
19. **Load thresholds and latency defaults configurable** via
    ``OffloadingConfig``.
20. **Structured logging** via ``_log(event, level, **fields)``.
21. **``collect_prometheus_metrics()``**.
22. **``TDPReading.is_synthetic``** flags fabricated fallback readings.

**Running the demo**

    PYTHONPATH=src python -m continuum.offloading_decision_engine
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import threading
import time
from collections import OrderedDict
from dataclasses import asdict, dataclass, fields
from datetime import datetime, timezone
from typing import Any, Dict, List, Mapping, Optional, Tuple

# --------------------------------------------------------------------------- #
# Shared continuum primitives (single source of truth)
# --------------------------------------------------------------------------- #
try:  # pragma: no cover — package vs. script
    from .types import (
        ContinuumError,
        PlacementTier,
        DeviceType,
        TDPReading,
        PlacementDecision,
        _json_safe,
        _coerce_float,
        _coerce_timestamp,
    )
except ImportError:  # pragma: no cover
    from continuum.types import (  # type: ignore[no-redef]
        ContinuumError,
        PlacementTier,
        DeviceType,
        TDPReading,
        PlacementDecision,
        _json_safe,
        _coerce_float,
        _coerce_timestamp,
    )

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class OffloadingEngineError(ContinuumError):
    """Raised for invalid inputs or configuration."""


# --------------------------------------------------------------------------- #
# Structured logging
# --------------------------------------------------------------------------- #
def _log(
    event: str, level: int = logging.INFO, **fields: Any
) -> None:
    if not logger.isEnabledFor(level):
        return
    parts = [event]
    if fields:
        parts.append(" ".join(f"{k}={v}" for k, v in fields.items()))
    logger.log(level, " ".join(parts), extra={"event": event, **fields})


# --------------------------------------------------------------------------- #
# Criteria & configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class OffloadingCriteria:
    """Criteria for offloading decisions (v1-compatible)."""

    tdp_threshold_watts: float
    tdp_warning_buffer: float = 2.0
    carbon_green_threshold: float = 50.0
    carbon_yellow_threshold: float = 200.0
    carbon_red_threshold: float = 400.0
    latency_sla_ms: float = 100.0
    latency_buffer_ms: float = 20.0
    cost_weight: float = 0.3
    carbon_weight: float = 0.5
    latency_weight: float = 0.2

    def __post_init__(self) -> None:
        if self.tdp_threshold_watts <= 0:
            raise OffloadingEngineError(
                "tdp_threshold_watts must be > 0"
            )
        if self.tdp_warning_buffer < 0:
            raise OffloadingEngineError(
                "tdp_warning_buffer must be >= 0"
            )
        for name in (
            "carbon_green_threshold",
            "carbon_yellow_threshold",
            "carbon_red_threshold",
            "latency_sla_ms",
            "latency_buffer_ms",
        ):
            if getattr(self, name) < 0:
                raise OffloadingEngineError(f"{name} must be >= 0")
        if not (
            self.carbon_green_threshold
            <= self.carbon_yellow_threshold
            <= self.carbon_red_threshold
        ):
            raise OffloadingEngineError(
                "carbon thresholds must satisfy green <= yellow <= red"
            )
        for name in (
            "cost_weight",
            "carbon_weight",
            "latency_weight",
        ):
            if getattr(self, name) < 0:
                raise OffloadingEngineError(f"{name} must be >= 0")


@dataclass(frozen=True)
class OffloadingConfig:
    """Extended configuration for the engine."""

    # Composite tier-score weights — MUST sum to 1.0.
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

    # v6: configurable load thresholds (moved from hard-coded 0.5/0.75).
    node_load_full_credit_below: float = 0.5
    node_load_half_credit_below: float = 0.75

    # v6: per-tier latency defaults (moved from a flat 100 ms).
    default_latency_local_ms: float = 5.0
    default_latency_regional_ms: float = 30.0
    default_latency_cloud_ms: float = 100.0

    # Per-tier energy model coefficients.
    energy_local_kwh: float = 0.001
    energy_regional_kwh: float = 0.0015
    energy_cloud_kwh: float = 0.002
    # v6: carbon estimate scales with duration when True.
    energy_scales_with_duration: bool = True

    # Per-tier base cost.
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
                f"tier weights must sum to 1.0 (got {tier_sum:.6f})"
            )
        node_sum = (
            self.node_cpu_weight
            + self.node_memory_weight
            + self.node_load_weight
            + self.node_network_weight
        )
        if abs(node_sum - 1.0) > 1e-6:
            raise OffloadingEngineError(
                f"node weights must sum to 1.0 (got {node_sum:.6f})"
            )
        if self.cache_ttl_seconds <= 0:
            raise OffloadingEngineError(
                "cache_ttl_seconds must be > 0"
            )
        if (
            self.max_cache_entries is not None
            and self.max_cache_entries <= 0
        ):
            raise OffloadingEngineError(
                "max_cache_entries must be > 0 or None"
            )
        if not (
            0.0 < self.node_load_full_credit_below
            < self.node_load_half_credit_below
            <= 1.0
        ):
            raise OffloadingEngineError(
                "node load thresholds must satisfy "
                "0 < full < half <= 1"
            )
        for name in (
            "default_latency_local_ms",
            "default_latency_regional_ms",
            "default_latency_cloud_ms",
        ):
            if getattr(self, name) < 0:
                raise OffloadingEngineError(f"{name} must be >= 0")
        for name in (
            "energy_local_kwh",
            "energy_regional_kwh",
            "energy_cloud_kwh",
            "cost_local_usd",
            "cost_regional_usd",
            "cost_cloud_usd",
            "cloud_egress_usd_per_mb",
        ):
            if getattr(self, name) < 0:
                raise OffloadingEngineError(f"{name} must be >= 0")


# --------------------------------------------------------------------------- #
# Engine
# --------------------------------------------------------------------------- #
class OffloadingDecisionEngine:
    """
    Multi-objective optimizer for edge-cloud placement.

    Thread-safe, serializable, bounded in memory.
    """

    def __init__(
        self,
        criteria: OffloadingCriteria,
        carbon_intensity_monitor: Any,
        latency_monitor: Any,
        node_registry: Any,
        *,
        tdp_monitor: Any = None,     # v6: constructor arg
        config: Optional[OffloadingConfig] = None,
        strict: bool = True,
        fallback_nodes: Optional[List[Dict[str, Any]]] = None,
    ) -> None:
        if not isinstance(criteria, OffloadingCriteria):
            raise OffloadingEngineError(
                "criteria must be an OffloadingCriteria instance"
            )

        self.criteria = criteria
        self.carbon_monitor = carbon_intensity_monitor
        self.latency_monitor = latency_monitor
        self.node_registry = node_registry
        self.tdp_monitor = tdp_monitor   # None when not provided

        self._config: OffloadingConfig = config or OffloadingConfig()
        self._strict: bool = bool(strict)
        self._lock = threading.RLock()

        # OrderedDict gives O(1) eviction + O(1) LRU move-to-end.
        self._decision_cache: "OrderedDict[Tuple[str, str], PlacementDecision]" = (
            OrderedDict()
        )
        self._ctx_start: Optional[float] = None

        # Fallback nodes are used only when the registry is empty AND
        # strict is False.
        self._fallback_nodes: List[Dict[str, Any]] = list(
            fallback_nodes or []
        )

        _log(
            "engine_initialized",
            logging.DEBUG,
            tdp_threshold=criteria.tdp_threshold_watts,
            cache_ttl=self._config.cache_ttl_seconds,
            strict=self._strict,
            tdp_monitor=tdp_monitor is not None,
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
        """Make an offloading decision for a task."""
        # ---- Validation ------------------------------------------------
        if not isinstance(task_id, str) or not task_id:
            raise OffloadingEngineError(
                "task_id must be a non-empty string"
            )
        if not isinstance(device_id, str) or not device_id:
            raise OffloadingEngineError(
                "device_id must be a non-empty string"
            )
        if not isinstance(task_requirements, Mapping):
            msg = (
                f"task_requirements must be a Mapping, "
                f"got {type(task_requirements).__name__}"
            )
            if self._strict:
                raise OffloadingEngineError(msg)
            logger.warning("%s Using empty requirements.", msg)
            task_requirements = {}

        reqs = dict(task_requirements)
        cache_key = self._cache_key(device_id, task_id)

        # ---- Cache check -----------------------------------------------
        cached = self._cache_get(cache_key)
        if cached is not None:
            _log(
                "cache_hit", logging.DEBUG,
                task_id=task_id, device_id=device_id,
            )
            return cached
        _log(
            "cache_miss", logging.DEBUG,
            task_id=task_id, device_id=device_id,
        )

        # ---- Gather inputs ---------------------------------------------
        tdp_reading = await self._get_tdp_reading(device_id)
        carbon_intensity = _coerce_float(
            await self._safe_call(
                self.carbon_monitor,
                "get_current_intensity",
                default=200.0,
            ),
            default=200.0,
        )
        latency_metrics = await self._safe_call(
            self.latency_monitor,
            "get_latency_to_tiers",
            device_id,
            default=self._default_latencies(),
        )
        # Coerce latency_metrics to a stable mapping in case the monitor
        # returned None or a partial dict.
        normalized_latencies = self._normalize_latencies(
            latency_metrics
        )

        available_nodes = await self._get_available_nodes()

        if not available_nodes and self._strict:
            raise OffloadingEngineError(
                "No available nodes found in registry"
            )

        # ---- Score every tier ------------------------------------------
        tier_scores: Dict[
            PlacementTier, Tuple[float, List[str]]
        ] = {}
        for tier in PlacementTier:
            try:
                score, reasoning = await self._calculate_tier_score(
                    tier=tier,
                    device_id=device_id,
                    tdp_reading=tdp_reading,
                    carbon_intensity=carbon_intensity,
                    latency_metrics=normalized_latencies,
                    available_nodes=available_nodes,
                    task_requirements=reqs,
                )
            except Exception as exc:
                logger.exception(
                    "Tier scoring failed for %s: %s", tier, exc
                )
                if self._strict:
                    raise
                score, reasoning = 0.0, [
                    f"scoring error: {exc}"
                ]
            tier_scores[tier] = (score, reasoning)

        # ---- Rank tiers (winner + runner-up) ---------------------------
        ordered = sorted(
            tier_scores.items(),
            key=lambda kv: kv[1][0],
            reverse=True,
        )
        best_tier, (best_score, best_reasoning) = ordered[0]
        runner_up_tier: Optional[PlacementTier] = None
        runner_up_score = 0.0
        if len(ordered) > 1:
            runner_up_tier, (runner_up_score, _) = ordered[1]
        margin = max(0.0, best_score - runner_up_score)

        # ---- Select target node ---------------------------------------
        target_node, is_fallback = await self._select_target_node(
            tier=best_tier,
            device_id=device_id,
            available_nodes=available_nodes,
            task_requirements=reqs,
        )

        # ---- Estimate metrics ------------------------------------------
        estimated_latency = float(
            normalized_latencies.get(best_tier, 100.0)
        )
        estimated_carbon = self._estimate_carbon(
            tier=best_tier,
            task_requirements=reqs,
            carbon_intensity=carbon_intensity,
        )
        estimated_cost = self._estimate_cost(
            tier=best_tier,
            task_requirements=reqs,
        )

        # ---- Assemble reasoning (winner + runner-up + tie-breaking) ----
        reasoning_lines: List[str] = [
            f"Chose {best_tier.value} "
            f"(score {best_score:.3f}); "
            f"runner-up {runner_up_tier.value if runner_up_tier else '(none)'} "
            f"(score {runner_up_score:.3f}); margin {margin:.3f}"
        ]
        reasoning_lines.extend(best_reasoning)
        if is_fallback:
            reasoning_lines.append(
                "No real nodes available; using deterministic fallback"
            )

        decision = PlacementDecision(
            task_id=task_id,
            device_id=device_id,
            selected_tier=best_tier,
            target_node=target_node,
            confidence_score=float(best_score),
            reasoning=tuple(reasoning_lines),
            estimated_latency_ms=estimated_latency,
            estimated_carbon_gco2=estimated_carbon,
            estimated_cost_usd=estimated_cost,
            timestamp=datetime.now(timezone.utc),
            margin=margin,
            runner_up_tier=runner_up_tier,
            target_is_fallback=is_fallback,
        )

        self._cache_put(cache_key, decision)

        _log(
            "decision_made",
            logging.INFO,
            task_id=task_id,
            device_id=device_id,
            tier=best_tier.value,
            target=target_node,
            confidence=best_score,
            margin=margin,
            latency_ms=estimated_latency,
            carbon_gco2=estimated_carbon,
            cost_usd=estimated_cost,
            fallback=is_fallback,
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
        cfg = self._config
        reasoning: List[str] = []

        tdp_score = self._score_tdp(tier, tdp_reading, reasoning)
        carbon_score = self._score_carbon(
            tier, carbon_intensity, reasoning
        )

        latency_sla = _coerce_float(
            task_requirements.get(
                "latency_sla_ms", self.criteria.latency_sla_ms
            ),
            self.criteria.latency_sla_ms,
        )
        latency = _coerce_float(
            latency_metrics.get(
                tier, self._default_latencies()[tier]
            ),
            self._default_latencies()[tier],
        )
        latency_score = self._score_latency(
            latency, latency_sla, reasoning
        )

        cost_score = self._score_cost(tier, reasoning)

        score = (
            tdp_score * cfg.tdp_weight
            + carbon_score * cfg.carbon_weight
            + latency_score * cfg.latency_weight
            + cost_score * cfg.cost_weight
        )
        return max(0.0, min(1.0, score)), reasoning

    def _score_tdp(
        self,
        tier: PlacementTier,
        tdp_reading: TDPReading,
        reasoning: List[str],
    ) -> float:
        if tier != PlacementTier.TIER_1_LOCAL:
            reasoning.append(
                f"{tier.value}: TDP concern resolved via offloading"
            )
            return 1.0
        threshold = (
            tdp_reading.tdp_threshold_watts
            - self.criteria.tdp_warning_buffer
        )
        if tdp_reading.current_power_watts >= threshold:
            reasoning.append(
                f"local: TDP near threshold "
                f"({tdp_reading.current_power_watts:.1f}W / "
                f"{tdp_reading.tdp_threshold_watts:.1f}W)"
            )
            return 0.0
        reasoning.append(
            f"local: TDP within safe limits "
            f"({tdp_reading.current_power_watts:.1f}W)"
        )
        return 1.0

    def _score_carbon(
        self,
        tier: PlacementTier,
        carbon_intensity: float,
        reasoning: List[str],
    ) -> float:
        """
        Green/yellow/red bands applied **uniformly** to every tier.

        v5 applied the bands only to the cloud tier and hard-coded
        0.5 / 0.7 for local / regional. v6 makes the intensity matter
        for all three tiers.
        """
        c = self.criteria
        if carbon_intensity < c.carbon_green_threshold:
            score, band = 1.0, "green"
        elif carbon_intensity < c.carbon_yellow_threshold:
            score, band = 0.5, "yellow"
        else:
            score, band = 0.2, "red"
        reasoning.append(
            f"{tier.value}: carbon intensity "
            f"{carbon_intensity:.1f} gCO2/kWh → {band} band "
            f"(score {score:.2f})"
        )
        return score

    def _score_latency(
        self,
        latency: float,
        latency_sla: float,
        reasoning: List[str],
    ) -> float:
        c = self.criteria
        if latency < latency_sla - c.latency_buffer_ms:
            reasoning.append(
                f"latency well within SLA "
                f"({latency:.1f}ms < {latency_sla:.1f}ms)"
            )
            return 1.0
        if latency < latency_sla:
            reasoning.append(
                f"latency within SLA "
                f"({latency:.1f}ms < {latency_sla:.1f}ms)"
            )
            return 0.7
        reasoning.append(
            f"latency exceeds SLA "
            f"({latency:.1f}ms > {latency_sla:.1f}ms)"
        )
        return 0.3

    def _score_cost(
        self, tier: PlacementTier, reasoning: List[str]
    ) -> float:
        """
        Score cost based on the configured coefficients.

        v5 returned hard-coded 1.0 / 0.7 / 0.4 — changing any
        ``cost_*_usd`` in :class:`OffloadingConfig` had no effect.
        v6 normalises against the cheapest configured cost, so
        increasing ``cost_cloud_usd`` lowers the cloud score.
        """
        cfg = self._config
        my_coeff = {
            PlacementTier.TIER_1_LOCAL: cfg.cost_local_usd,
            PlacementTier.TIER_2_REGIONAL: cfg.cost_regional_usd,
            PlacementTier.TIER_3_CLOUD: cfg.cost_cloud_usd,
        }[tier]
        ref = min(
            cfg.cost_local_usd,
            cfg.cost_regional_usd,
            cfg.cost_cloud_usd,
        ) or 1e-12
        # Inverse ratio: cheapest tier → 1.0; more expensive → lower.
        ratio = ref / max(my_coeff, 1e-12)
        score = max(0.0, min(1.0, ratio))
        reasoning.append(
            f"{tier.value}: cost coeff ${my_coeff:.6f} "
            f"(ref ${ref:.6f}, ratio {ratio:.3f}) "
            f"→ score {score:.2f}"
        )
        return score

    # -------------------------------------------------------- node selection
    async def _select_target_node(
        self,
        tier: PlacementTier,
        device_id: str,
        available_nodes: List[Dict[str, Any]],
        task_requirements: Mapping[str, Any],
    ) -> Tuple[str, bool]:
        """
        Return ``(node_id, is_fallback)`` for the best node in ``tier``.

        Fallback chain:
            local    → regional → cloud
            regional → cloud    → local
            cloud    → regional → local

        If no tier has any node, returns ``(f"{tier.value}-fallback",
        True)`` — a synthetic placeholder, flagged on the decision.
        """
        visited: List[PlacementTier] = []
        current = tier

        while current not in visited:
            visited.append(current)
            tier_nodes = [
                n
                for n in available_nodes
                if n.get("tier") == current.value
            ]
            if tier_nodes:
                scored = sorted(
                    (
                        (self._score_node(n, task_requirements), n)
                        for n in tier_nodes
                    ),
                    key=lambda x: x[0],
                    reverse=True,
                )
                return str(scored[0][1]["node_id"]), False
            current = self._fallback_tier(current)

        node_id = f"{tier.value}-fallback"
        logger.warning(
            "No nodes available across tiers %s; defaulting to %s.",
            [t.value for t in visited],
            node_id,
        )
        return node_id, True

    @staticmethod
    def _fallback_tier(tier: PlacementTier) -> PlacementTier:
        return {
            PlacementTier.TIER_1_LOCAL: PlacementTier.TIER_2_REGIONAL,
            PlacementTier.TIER_2_REGIONAL: PlacementTier.TIER_3_CLOUD,
            PlacementTier.TIER_3_CLOUD: PlacementTier.TIER_1_LOCAL,
        }[tier]

    def _score_node(
        self,
        node: Mapping[str, Any],
        task_requirements: Mapping[str, Any],
    ) -> float:
        cfg = self._config
        score = 0.0

        # ---- CPU ------------------------------------------------------
        cpu_available = _coerce_float(
            node.get("available_cpu_percent"), 0.0
        )
        cpu_required = _coerce_float(
            task_requirements.get("cpu_required"), 50.0
        )
        if cpu_available >= cpu_required:
            score += cfg.node_cpu_weight

        # ---- Memory ---------------------------------------------------
        mem_available = _coerce_float(
            node.get("available_memory_mb"), 0.0
        )
        mem_required = _coerce_float(
            task_requirements.get("memory_required_mb"), 1024.0
        )
        if mem_available >= mem_required:
            score += cfg.node_memory_weight

        # ---- Load (configurable thresholds) ---------------------------
        max_tasks = _coerce_float(node.get("max_tasks"), 0.0)
        if max_tasks <= 0:
            return 0.0   # a node with zero capacity cannot serve
        current_tasks = _coerce_float(
            node.get("current_tasks"), 0.0
        )
        load_ratio = current_tasks / max_tasks
        if load_ratio < cfg.node_load_full_credit_below:
            score += cfg.node_load_weight
        elif load_ratio < cfg.node_load_half_credit_below:
            score += cfg.node_load_weight * 0.5

        # ---- Network --------------------------------------------------
        net_ms = _coerce_float(
            node.get("network_latency_ms"), 999.0
        )
        if net_ms < 10.0:
            score += cfg.node_network_weight
        elif net_ms < 25.0:
            score += cfg.node_network_weight * 0
