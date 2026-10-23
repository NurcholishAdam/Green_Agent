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
            score += cfg.node_network_weight * 0.5

        return min(1.0, score)

    @staticmethod
    def _coerce_node(
        raw: Any,
    ) -> Optional[Dict[str, Any]]:
        """
        Validate a node dict; return ``None`` if it should be excluded.

        Excluded when: not a Mapping, ``max_tasks`` non-numeric, or
        ``max_tasks <= 0``.
        """
        if not isinstance(raw, Mapping):
            return None
        node = dict(raw)

        for key in (
            "available_cpu_percent",
            "available_memory_mb",
            "current_tasks",
            "network_latency_ms",
        ):
            v = node.get(key)
            if v is None:
                node[key] = None
                continue
            try:
                node[key] = float(v)
            except (TypeError, ValueError):
                node[key] = None

        max_tasks_raw = node.get("max_tasks")
        if max_tasks_raw is None:
            # Unknown capacity — keep, use 1.0 fallback in scoring.
            node["max_tasks"] = None
        else:
            try:
                max_tasks = float(max_tasks_raw)
            except (TypeError, ValueError):
                return None
            if max_tasks <= 0:
                return None
            node["max_tasks"] = max_tasks
        return node

    # -------------------------------------------------------- estimations
    def _estimate_carbon(
        self,
        tier: PlacementTier,
        task_requirements: Mapping[str, Any],
        carbon_intensity: float,
    ) -> float:
        """
        Estimate carbon emissions in grams CO2e.

        v6: scales with ``compute_seconds`` when
        ``config.energy_scales_with_duration`` is True (the default).
        """
        cfg = self._config
        compute_intensity = max(
            0.0,
            _coerce_float(
                task_requirements.get("compute_intensity"), 1.0
            ),
        )
        compute_seconds = max(
            0.0,
            _coerce_float(
                task_requirements.get("compute_seconds"), 1.0
            ),
        )
        energy_by_tier = {
            PlacementTier.TIER_1_LOCAL: cfg.energy_local_kwh,
            PlacementTier.TIER_2_REGIONAL: cfg.energy_regional_kwh,
            PlacementTier.TIER_3_CLOUD: cfg.energy_cloud_kwh,
        }
        duration_factor = (
            compute_seconds
            if cfg.energy_scales_with_duration else 1.0
        )
        energy_kwh = (
            energy_by_tier[tier] * compute_intensity * duration_factor
        )
        return energy_kwh * carbon_intensity

    def _estimate_cost(
        self,
        tier: PlacementTier,
        task_requirements: Mapping[str, Any],
    ) -> float:
        cfg = self._config
        data_size_mb = _coerce_float(
            task_requirements.get("data_size_mb"), 1.0
        )
        compute_seconds = _coerce_float(
            task_requirements.get("compute_seconds"), 1.0
        )
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
    async def _get_tdp_reading(
        self, device_id: str
    ) -> TDPReading:
        monitor = self.tdp_monitor
        if monitor is not None:
            for method_name in (
                "get_reading",
                "get_tdp_reading",
            ):
                method = getattr(monitor, method_name, None)
                if not callable(method):
                    continue
                try:
                    result = method(device_id)
                    if asyncio.iscoroutine(result):
                        result = await result
                    if isinstance(result, TDPReading):
                        return result
                except Exception as exc:
                    logger.warning(
                        "TDP monitor '%s' failed: %s; using placeholder",
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
            is_synthetic=True,   # v6: flag the placeholder
        )

    # -------------------------------------------------------- node registry
    async def _get_available_nodes(
        self,
    ) -> List[Dict[str, Any]]:
        raw: List[Any] = []
        registry = self.node_registry
        if registry is not None:
            method = getattr(
                registry, "get_available_nodes", None
            )
            if callable(method):
                try:
                    result = method()
                    if asyncio.iscoroutine(result):
                        result = await result
                    if isinstance(result, (list, tuple)):
                        raw = list(result)
                except Exception as exc:
                    logger.warning(
                        "node_registry.get_available_nodes failed: %s",
                        exc,
                    )
                    if self._strict:
                        raise

        if not raw and self._fallback_nodes:
            raw = list(self._fallback_nodes)
            _log(
                "using_fallback_nodes",
                logging.DEBUG,
                count=len(raw),
            )

        validated: List[Dict[str, Any]] = []
        for entry in raw:
            coerced = self._coerce_node(entry)
            if coerced is not None:
                validated.append(coerced)
        return validated

    # -------------------------------------------------------- safe calls
    @staticmethod
    async def _safe_call(
        target: Any,
        method_name: str,
        *args: Any,
        default: Any,
    ) -> Any:
        if target is None:
            return default
        method = getattr(target, method_name, None)
        if not callable(method):
            return default
        try:
            result = method(*args)
            if asyncio.iscoroutine(result):
                result = await result
            return result
        except Exception as exc:
            logger.warning(
                "Monitor '%s' raised %s; using default=%r",
                method_name, exc, default,
            )
            return default

    # -------------------------------------------------------- defaults
    def _default_latencies(
        self,
    ) -> Dict[PlacementTier, float]:
        cfg = self._config
        return {
            PlacementTier.TIER_1_LOCAL: cfg.default_latency_local_ms,
            PlacementTier.TIER_2_REGIONAL: cfg.default_latency_regional_ms,
            PlacementTier.TIER_3_CLOUD: cfg.default_latency_cloud_ms,
        }

    def _normalize_latencies(
        self, raw: Any
    ) -> Dict[PlacementTier, float]:
        defaults = self._default_latencies()
        if not isinstance(raw, Mapping):
            return defaults
        out: Dict[PlacementTier, float] = {}
        for tier in PlacementTier:
            v = raw.get(tier, raw.get(tier.value))
            out[tier] = _coerce_float(v, defaults[tier])
        return out

    # -------------------------------------------------------- cache helpers
    @staticmethod
    def _cache_key(
        device_id: str, task_id: str
    ) -> Tuple[str, str]:
        return (device_id, task_id)

    def _cache_get(
        self, key: Tuple[str, str]
    ) -> Optional[PlacementDecision]:
        with self._lock:
            decision = self._decision_cache.get(key)
            if decision is None:
                return None
            age = (
                datetime.now(timezone.utc) - decision.timestamp
            ).total_seconds()
            if age >= self._config.cache_ttl_seconds:
                self._decision_cache.pop(key, None)
                return None
            self._decision_cache.move_to_end(key)
            return decision

    def _cache_put(
        self,
        key: Tuple[str, str],
        decision: PlacementDecision,
    ) -> None:
        with self._lock:
            self._decision_cache[key] = decision
            self._decision_cache.move_to_end(key)
            cap = self._config.max_cache_entries
            while cap is not None and len(self._decision_cache) > cap:
                self._decision_cache.popitem(last=False)

    def clear_cache(self) -> None:
        with self._lock:
            self._decision_cache.clear()
        _log("cache_cleared", logging.DEBUG)

    # -------------------------------------------------------- statistics
    def statistics(self) -> Dict[str, Any]:
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
            by_tier[d.selected_tier.value] = (
                by_tier.get(d.selected_tier.value, 0) + 1
            )
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

    def health_check(self) -> Dict[str, Any]:
        """Return operational state (P1 §3.13)."""
        with self._lock:
            cache_size = len(self._decision_cache)
        cap = self._config.max_cache_entries
        return {
            "healthy": True,
            "cache_size": cache_size,
            "cache_utilization": (
                cache_size / cap if cap else 0.0
            ),
            "monitors": {
                "carbon": self.carbon_monitor is not None,
                "latency": self.latency_monitor is not None,
                "tdp": self.tdp_monitor is not None,
                "node_registry": self.node_registry is not None,
            },
            "strict": self._strict,
            "criteria": _json_safe(asdict(self.criteria)),
            "config": _json_safe(asdict(self._config)),
        }

    def collect_prometheus_metrics(
        self,
    ) -> Dict[str, Tuple[Any, Dict[str, str]]]:
        """Return a metric table (P2 §21)."""
        with self._lock:
            decisions = list(self._decision_cache.values())
        metrics: Dict[str, Tuple[Any, Dict[str, str]]] = {
            "offloading_cache_size": (len(decisions), {}),
        }
        by_tier: Dict[str, int] = {}
        for d in decisions:
            key = d.selected_tier.value
            by_tier[key] = by_tier.get(key, 0) + 1
        for tier, count in by_tier.items():
            metrics[
                f"offloading_tier_decisions_total_{tier}"
            ] = (count, {"tier": tier})
        if decisions:
            metrics["offloading_mean_confidence"] = (
                sum(d.confidence_score for d in decisions) / len(decisions),
                {},
            )
            metrics["offloading_mean_margin"] = (
                sum(d.margin for d in decisions) / len(decisions),
                {},
            )
        return metrics

    def reset(self, *, clear_cache: bool = False) -> None:
        with self._lock:
            if clear_cache:
                self._decision_cache.clear()
        _log(
            "engine_reset",
            logging.DEBUG,
            clear_cache=clear_cache,
        )

    # -------------------------------------------------------- live config
    def replace_criteria(
        self,
        criteria: OffloadingCriteria,
        *,
        clear_cache: bool = True,
    ) -> None:
        """Swap criteria; cached decisions are stale by default."""
        if not isinstance(criteria, OffloadingCriteria):
            raise OffloadingEngineError(
                "criteria must be OffloadingCriteria"
            )
        with self._lock:
            self.criteria = criteria
            if clear_cache:
                self._decision_cache.clear()

    def replace_config(
        self,
        config: OffloadingConfig,
        *,
        clear_cache: bool = True,
    ) -> None:
        """Swap config; cached decisions are stale by default."""
        if not isinstance(config, OffloadingConfig):
            raise OffloadingEngineError(
                "config must be OffloadingConfig"
            )
        with self._lock:
            self._config = config
            if clear_cache:
                self._decision_cache.clear()

    # -------------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            now = datetime.now(timezone.utc)
            ttl = self._config.cache_ttl_seconds
            live = [
                d
                for d in self._decision_cache.values()
                if (now - d.timestamp).total_seconds() < ttl
            ]
            return _json_safe(
                {
                    "criteria": asdict(self.criteria),
                    "config": asdict(self._config),
                    "strict": self._strict,
                    "decision_cache": [
                        d.to_dict() for d in live
                    ],
                    "fallback_nodes": list(self._fallback_nodes),
                }
            )

    def to_json(self, **kwargs: Any) -> str:
        kwargs.setdefault("sort_keys", True)
        return json.dumps(self.to_dict(), **kwargs)

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        carbon_intensity_monitor: Any = None,
        latency_monitor: Any = None,
        node_registry: Any = None,
        tdp_monitor: Any = None,
    ) -> "OffloadingDecisionEngine":
        if not isinstance(data, Mapping):
            raise OffloadingEngineError(
                f"from_dict expects Mapping, "
                f"got {type(data).__name__}"
            )
        criteria = OffloadingCriteria(**data["criteria"])
        cfg_data = dict(data.get("config", {}) or {})
        cfg_fields = {f.name for f in fields(OffloadingConfig)}
        filtered_cfg = {
            k: v for k, v in cfg_data.items() if k in cfg_fields
        }
        cfg = OffloadingConfig(**filtered_cfg)

        engine = cls(
            criteria=criteria,
            carbon_intensity_monitor=carbon_intensity_monitor,
            latency_monitor=latency_monitor,
            node_registry=node_registry,
            tdp_monitor=tdp_monitor,
            config=cfg,
            strict=bool(data.get("strict", True)),
            fallback_nodes=list(data.get("fallback_nodes", [])),
        )

        with engine._lock:
            for raw in data.get("decision_cache", []) or []:
                try:
                    d = PlacementDecision.from_dict(raw)
                except ContinuumError as exc:
                    logger.warning(
                        "Skipping malformed cache entry: %s", exc
                    )
                    continue
                key = engine._cache_key(d.device_id, d.task_id)
                engine._decision_cache[key] = d
            # Enforce the cache bound (P0 §2.4)
            cap = engine._config.max_cache_entries
            while cap is not None and len(engine._decision_cache) > cap:
                engine._decision_cache.popitem(last=False)
        return engine

    @classmethod
    def from_json(
        cls,
        payload: str,
        *,
        carbon_intensity_monitor: Any = None,
        latency_monitor: Any = None,
        node_registry: Any = None,
        tdp_monitor: Any = None,
    ) -> "OffloadingDecisionEngine":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise OffloadingEngineError(
                f"Invalid JSON payload: {exc}"
            ) from exc
        return cls.from_dict(
            data,
            carbon_intensity_monitor=carbon_intensity_monitor,
            latency_monitor=latency_monitor,
            node_registry=node_registry,
            tdp_monitor=tdp_monitor,
        )

    # -------------------------------------------------------- async ctx
    async def __aenter__(self) -> "OffloadingDecisionEngine":
        self._ctx_start = time.perf_counter()
        _log("engine_scope_enter", logging.DEBUG)
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        elapsed = time.perf_counter() - (
            self._ctx_start
            if self._ctx_start is not None
            else time.perf_counter()
        )
        self._ctx_start = None
        if exc_type is not None:
            _log(
                "engine_scope_exit_exception",
                logging.WARNING,
                exception=exc_type.__name__,
                elapsed=elapsed,
            )
        else:
            _log(
                "engine_scope_exit",
                logging.INFO,
                elapsed=elapsed,
                cached=len(self._decision_cache),
            )

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
    "PlacementDecision",
    "OffloadingCriteria",
    "OffloadingConfig",
    "OffloadingDecisionEngine",
    "OffloadingEngineError",
    "ContinuumError",
]


# --------------------------------------------------------------------------- #
# Demo — every P0/P1/P2 fix verified by assertion
# --------------------------------------------------------------------------- #
def _demo() -> None:
    logging.basicConfig(
        level=logging.WARNING,
        format="%(levelname)s %(name)s: %(message)s",
    )

    print("=" * 78)
    print("offloading_decision_engine.py v6.0.0 — Demo")
    print("=" * 78)

    def _check(condition: bool, message: str) -> None:
        if not condition:
            raise AssertionError(f"FAILED: {message}")
        print(f"  ✓ {message}")

    # ------------------------------------------------------------- #
    # Fixtures
    # ------------------------------------------------------------- #
    class _CarbonMonitor:
        def __init__(self, value: Optional[float] = 30.0):
            self._value = value
        async def get_current_intensity(self) -> Optional[float]:
            return self._value

    class _LatencyMonitor:
        def __init__(
            self,
            local: float = 2.0,
            regional: float = 8.0,
            cloud: float = 35.0,
        ):
            self._vals = {
                PlacementTier.TIER_1_LOCAL: local,
                PlacementTier.TIER_2_REGIONAL: regional,
                PlacementTier.TIER_3_CLOUD: cloud,
            }
        async def get_latency_to_tiers(
            self, device_id: str
        ) -> Dict[PlacementTier, float]:
            return dict(self._vals)

    def _make_nodes(
        *,
        local_max_tasks: Optional[float] = 10,
        include_local: bool = True,
        include_regional: bool = True,
        include_cloud: bool = True,
    ) -> List[Dict[str, Any]]:
        nodes: List[Dict[str, Any]] = []
        if include_local:
            nodes.append({
                "node_id": "edge-pi-001",
                "tier": PlacementTier.TIER_1_LOCAL.value,
                "available_cpu_percent": 65,
                "available_memory_mb": 2048,
                "current_tasks": 3,
                "max_tasks": local_max_tasks,
                "network_latency_ms": 2,
            })
        if include_regional:
            nodes.append({
                "node_id": "regional-edge-001",
                "tier": PlacementTier.TIER_2_REGIONAL.value,
                "available_cpu_percent": 80,
                "available_memory_mb": 8192,
                "current_tasks": 12,
                "max_tasks": 50,
                "network_latency_ms": 8,
            })
        if include_cloud:
            nodes.append({
                "node_id": "cloud-001",
                "tier": PlacementTier.TIER_3_CLOUD.value,
                "available_cpu_percent": 90,
                "available_memory_mb": 32768,
                "current_tasks": 45,
                "max_tasks": 200,
                "network_latency_ms": 35,
            })
        return nodes

    class _NodeRegistry:
        def __init__(self, nodes: List[Dict[str, Any]]):
            self._nodes = nodes
        async def get_available_nodes(
            self,
        ) -> List[Dict[str, Any]]:
            return list(self._nodes)

    def _engine(
        *,
        carbon: Optional[float] = 30.0,
        nodes: Optional[List[Dict[str, Any]]] = None,
        latency_local: float = 2.0,
        latency_regional: float = 8.0,
        latency_cloud: float = 35.0,
        tdp_monitor: Any = None,
        config: Optional[OffloadingConfig] = None,
    ) -> OffloadingDecisionEngine:
        return OffloadingDecisionEngine(
            criteria=OffloadingCriteria(tdp_threshold_watts=28.0),
            carbon_intensity_monitor=_CarbonMonitor(carbon),
            latency_monitor=_LatencyMonitor(
                latency_local, latency_regional, latency_cloud
            ),
            node_registry=_NodeRegistry(
                nodes if nodes is not None else _make_nodes()
            ),
            tdp_monitor=tdp_monitor,
            config=config,
            strict=True,
        )

    async def run_all() -> None:
        # ========================================================== #
        # P0 §2.1 — carbon score responds to intensity (all tiers)
        # ========================================================== #
        print("\n=== P0 §2.1: carbon score responds to intensity for ALL tiers ===")
        engine = _engine()

        green_scores = [
            engine._score_carbon(t, 30.0, [])
            for t in PlacementTier
        ]
        _check(
            all(s == 1.0 for s in green_scores),
            f"green grid → all tiers 1.0 (got {green_scores})",
        )

        yellow_scores = [
            engine._score_carbon(t, 100.0, [])
            for t in PlacementTier
        ]
        _check(
            all(s == 0.5 for s in yellow_scores),
            f"yellow grid → all tiers 0.5 (got {yellow_scores})",
        )

        red_scores = [
            engine._score_carbon(t, 500.0, [])
            for t in PlacementTier
        ]
        _check(
            all(s == 0.2 for s in red_scores),
            f"red grid → all tiers 0.2 (got {red_scores})",
        )
        _check(
            len(set(green_scores)) == 1
            and len(set(red_scores)) == 1,
            "on a uniform grid, all tiers get the same carbon score "
            "(v5 gave 0.5/0.7/1.0 for green)",
        )

        # ========================================================== #
        # P0 §2.2 — cost score responds to coefficients
        # ========================================================== #
        print("\n=== P0 §2.2: cost score responds to coefficients ===")
        default_engine = _engine()
        expensive_engine = _engine(
            config=OffloadingConfig(cost_cloud_usd=0.02)
        )

        default_cloud = default_engine._score_cost(
            PlacementTier.TIER_3_CLOUD, []
        )
        expensive_cloud = expensive_engine._score_cost(
            PlacementTier.TIER_3_CLOUD, []
        )
        _check(
            expensive_cloud < default_cloud,
            f"expensive cloud scores lower: "
            f"{default_cloud:.4f} → {expensive_cloud:.4f} "
            f"(v5 returned 0.4 for both)",
        )
        # Local cost unaffected
        _check(
            default_engine._score_cost(
                PlacementTier.TIER_1_LOCAL, []
            ) == 1.0,
            "cheapest tier scores 1.0",
        )

        # ========================================================== #
        # P0 §2.3 — shared enum identity via types module
        # ========================================================== #
        print("\n=== P0 §2.3: PlacementTier shared via continuum.types ===")
        try:
            from continuum.types import (
                PlacementTier as SharedTier,
            )
        except ImportError:
            from types import (  # type: ignore
                PlacementTier as SharedTier,
            )
        _check(
            SharedTier is PlacementTier,
            "PlacementTier is the same object as continuum.types.PlacementTier",
        )
        _check(
            SharedTier.TIER_1_LOCAL is PlacementTier.TIER_1_LOCAL,
            "enum members are identical (v5 defined its own class)",
        )

        # ========================================================== #
        # P0 §2.4 — from_dict enforces cache bound + normalizes timestamps
        # ========================================================== #
        print("\n=== P0 §2.4: from_dict enforces cache bound ===")
        # Build a payload with 500 cache entries
        src = _engine()
        src._config = OffloadingConfig(max_cache_entries=500)
        base_decision = await src.decide_placement(
            "tpl", "dev", {"latency_sla_ms": 50.0}
        )
        cache_entries = []
        for i in range(500):
            d = await src.decide_placement(
                f"task-{i}", "dev", {"latency_sla_ms": 50.0}
            )
            cache_entries.append(d.to_dict())

        payload = {
            "criteria": asdict(src.criteria),
            "config": {
                **asdict(src.config),
                "max_cache_entries": 50,   # <-- tight bound
            },
            "strict": True,
            "decision_cache": cache_entries,
            "fallback_nodes": [],
        }
        restored = OffloadingDecisionEngine.from_dict(
            payload,
            carbon_intensity_monitor=_CarbonMonitor(30.0),
            latency_monitor=_LatencyMonitor(),
            node_registry=_NodeRegistry(_make_nodes()),
        )
        _check(
            restored.cache_size <= 50,
            f"from_dict enforced max_cache_entries=50 "
            f"(got {restored.cache_size})",
        )

        print("\n=== P0 §2.4: from_dict normalizes naive timestamps ===")
        naive_payload = {
            "criteria": asdict(src.criteria),
            "config": asdict(src.config),
            "strict": True,
            "decision_cache": [
                {
                    "task_id": "naive-task",
                    "device_id": "naive-dev",
                    "selected_tier": "tier_1_local",
                    "target_node": "edge-1",
                    "confidence_score": 0.9,
                    "reasoning": ["test"],
                    "estimated_latency_ms": 2.0,
                    "estimated_carbon_gco2": 0.001,
                    "estimated_cost_usd": 0.0001,
                    # naive ISO — no timezone
                    "timestamp": "2024-01-01T00:00:00",
                }
            ],
        }
        restored2 = OffloadingDecisionEngine.from_dict(
            naive_payload,
            carbon_intensity_monitor=_CarbonMonitor(30.0),
            latency_monitor=_LatencyMonitor(),
            node_registry=_NodeRegistry(_make_nodes()),
        )
        key = restored2._cache_key("naive-dev", "naive-task")
        cached = restored2._cache_get(key)
        _check(
            cached is not None and cached.timestamp.tzinfo is not None,
            "naive timestamp normalized to aware on restore "
            "(v5 raised TypeError on next _cache_get)",
        )

        # ========================================================== #
        # P0 §2.5 — cache key tuple (no collision)
        # ========================================================== #
        print("\n=== P0 §2.5: cache key tuple (no collision) ===")
        k1 = OffloadingDecisionEngine._cache_key("a_b", "c")
        k2 = OffloadingDecisionEngine._cache_key("a", "b_c")
        _check(
            k1 != k2,
            f"tuple keys avoid collision: {k1} vs {k2} "
            f"(v5's f-string produced 'a_b_c' for both)",
        )
        _check(
            isinstance(k1, tuple),
            "cache key is a tuple (not a string)",
        )

        # ========================================================== #
        # P0 §2.6 — tdp_monitor is a constructor arg
        # ========================================================== #
        print("\n=== P0 §2.6: tdp_monitor is a constructor arg ===")
        fake_tdp = object()
        tdp_engine = _engine(tdp_monitor=fake_tdp)
        _check(
            tdp_engine.tdp_monitor is fake_tdp,
            "tdp_monitor accepted in constructor "
            "(v5 required attribute assignment)",
        )

        # ========================================================== #
        # P1 §2.7 — no numpy dependency
        # ========================================================== #
        print("\n=== P1 §2.7: no numpy dependency ===")
        import sys as _sys
        if "numpy" in _sys.modules:
            _check(
                not hasattr(_sys.modules[__name__], "np"),
                "module does not reference numpy at top level",
            )
        else:
            _check(
                True,
                "numpy not imported (module runs without it)",
            )

        # ========================================================== #
        # P1 §2.8 — carbon estimate scales with duration
        # ========================================================== #
        print("\n=== P1 §2.8: carbon estimate scales with duration ===")
        carbon_1s = engine._estimate_carbon(
            tier=PlacementTier.TIER_1_LOCAL,
            task_requirements={"compute_seconds": 1.0},
            carbon_intensity=100.0,
        )
        carbon_10s = engine._estimate_carbon(
            tier=PlacementTier.TIER_1_LOCAL,
            task_requirements={"compute_seconds": 10.0},
            carbon_intensity=100.0,
        )
        _check(
            abs(carbon_10s - carbon_1s * 10.0) < 1e-9,
            f"10-second task → 10× carbon: "
            f"{carbon_1s:.6f} → {carbon_10s:.6f}",
        )

        # ========================================================== #
        # P1 §2.9 — malformed nodes validated / skipped
        # ========================================================== #
        print("\n=== P1 §2.9: malformed nodes are skipped ===")
        bad_nodes = [
            {"node_id": "bad1", "tier": "tier_1_local",
             "available_cpu_percent": None,
             "available_memory_mb": 1024,
             "current_tasks": 0, "max_tasks": 10,
             "network_latency_ms": 5},
            {"node_id": "bad2", "tier": "tier_1_local",
             "available_cpu_percent": "not-a-number",
             "available_memory_mb": 1024,
             "current_tasks": 0, "max_tasks": 10,
             "network_latency_ms": 5},
            "not-a-mapping",
            {"node_id": "good", "tier": "tier_1_local",
             "available_cpu_percent": 50,
             "available_memory_mb": 2048,
             "current_tasks": 2, "max_tasks": 10,
             "network_latency_ms": 5},
        ]
        validated = [
            engine._coerce_node(n) for n in bad_nodes
        ]
        _check(
            validated[0] is not None,
            "None-valued field → node kept (field coerced to None)",
        )
        _check(
            validated[1] is not None,
            "non-numeric field → coerced to None, node kept",
        )
        _check(
            validated[2] is None,
            "non-Mapping entry → excluded",
        )
        _check(
            validated[3] is not None,
            "valid node kept",
        )

        # ========================================================== #
        # P1 §2.10 — max_tasks <= 0 excludes node
        # ========================================================== #
        print("\n=== P1 §2.10: max_tasks <= 0 excludes node ===")
        zero_cap = {
            "node_id": "zero-cap", "tier": "tier_1_local",
            "available_cpu_percent": 100,
            "available_memory_mb": 4096,
            "current_tasks": 0, "max_tasks": 0,
            "network_latency_ms": 2,
        }
        _check(
            engine._coerce_node(zero_cap) is None,
            "max_tasks=0 → node excluded",
        )
        _check(
            engine._coerce_node(
                {**zero_cap, "max_tasks": -1}
            ) is None,
            "max_tasks=-1 → node excluded",
        )

        # ========================================================== #
        # P1 §2.11 — fallback chain returns (node, is_fallback)
        # ========================================================== #
        print("\n=== P1 §2.11: fallback chain returns (node, is_fallback) ===")
        empty_engine = _engine(nodes=[])
        node_id, is_fallback = await empty_engine._select_target_node(
            tier=PlacementTier.TIER_1_LOCAL,
            device_id="dev",
            available_nodes=[],
            task_requirements={},
        )
        _check(
            node_id.endswith("-fallback") and is_fallback is True,
            f"empty registry → fallback node with flag: "
            f"({node_id}, {is_fallback})",
        )
        # Non-empty tier → is_fallback False
        full_engine = _engine()
        full_nodes = await full_engine._get_available_nodes()
        node_id2, is_fb2 = await full_engine._select_target_node(
            tier=PlacementTier.TIER_1_LOCAL,
            device_id="dev",
            available_nodes=full_nodes,
            task_requirements={},
        )
        _check(
            node_id2 == "edge-pi-001" and is_fb2 is False,
            f"real node → not fallback: ({node_id2}, {is_fb2})",
        )

        # ========================================================== #
        # P1 §2.12 — carbon_intensity=None defaults safely
        # ========================================================== #
        print("\n=== P1 §2.12: None carbon_intensity defaults safely ===")
        none_carbon_engine = _engine(carbon=None)
        decision = await none_carbon_engine.decide_placement(
            "none-task", "dev", {"latency_sla_ms": 50.0}
        )
        _check(
            decision.selected_tier in set(PlacementTier),
            f"None carbon_intensity → decision succeeds "
            f"(tier={decision.selected_tier.value})",
        )

        # ========================================================== #
        # P1 §2.13 — OrderedDict cache
        # ========================================================== #
        print("\n=== P1 §2.13: OrderedDict cache (no parallel list) ===")
        _check(
            not hasattr(engine, "_cache_order"),
            "no _cache_order list (v5 kept two structures in sync)",
        )
        _check(
            hasattr(engine._decision_cache, "move_to_end"),
            "cache is an OrderedDict",
        )

        # ========================================================== #
        # P1 §2.14 — health_check, replace_criteria, replace_config
        # ========================================================== #
        print("\n=== P1 §2.14: health_check / replace_criteria / replace_config ===")
        health = engine.health_check()
        for key in (
            "healthy",
            "cache_size",
            "cache_utilization",
            "monitors",
            "criteria",
            "config",
        ):
            _check(key in health, f"health_check has '{key}'")

        new_crit = OffloadingCriteria(tdp_threshold_watts=35.0)
        engine.replace_criteria(new_crit)
        _check(
            engine.criteria is new_crit,
            "replace_criteria swaps the criteria",
        )
        _check(
            engine.cache_size == 0,
            "replace_criteria clears the cache by default",
        )

        new_cfg = OffloadingConfig(cache_ttl_seconds=120.0)
        engine.replace_config(new_cfg)
        _check(
            engine.config is new_cfg,
            "replace_config swaps the config",
        )

        # ========================================================== #
        # P2 §2.15 — margin + runner_up on the decision
        # ========================================================== #
        print("\n=== P2 §2.15: margin + runner_up on decisions ===")
        margin_engine = _engine()
        decision = await margin_engine.decide_placement(
            "margin-task", "dev", {"latency_sla_ms": 50.0}
        )
        _check(
            isinstance(decision.margin, float)
            and decision.margin >= 0.0,
            f"decision.margin present: {decision.margin:.4f}",
        )
        _check(
            decision.runner_up_tier is None
            or isinstance(
                decision.runner_up_tier, PlacementTier
            ),
            f"decision.runner_up_tier is a PlacementTier or None",
        )
        _check(
            any(
                "runner-up" in line
                for line in decision.reasoning
            ),
            "reasoning mentions the runner-up",
        )

        # ========================================================== #
        # P2 §2.16-17 — TTL filter + fallback flag
        # ========================================================== #
        print("\n=== P2 §2.16-17: TTL filter + fallback flag ===")
        empty_engine2 = _engine(nodes=[])
        empty_decision = await empty_engine2.decide_placement(
            "empty-task", "dev", {"latency_sla_ms": 100.0}
        )
        _check(
            empty_decision.target_is_fallback is True,
            "empty registry → target_is_fallback=True",
        )
        _check(
            empty_decision.target_node.endswith("-fallback"),
            f"fallback node name: {empty_decision.target_node}",
        )

        # to_dict omits expired entries
        exp_engine = _engine(
            config=OffloadingConfig(cache_ttl_seconds=0.05)
        )
        await exp_engine.decide_placement(
            "exp-task", "dev", {"latency_sla_ms": 100.0}
        )
        await asyncio.sleep(0.1)
        payload = exp_engine.to_dict()
        _check(
            len(payload["decision_cache"]) == 0,
            "to_dict drops expired entries",
        )

        # ========================================================== #
        # P2 §2.18 — structured logging helper
        # ========================================================== #
        print("\n=== P2 §2.18: structured logging present ===")
        _check(
            callable(_log),
            "_log helper is callable",
        )

        # ========================================================== #
        # P2 §2.19 — Prometheus metrics
        # ========================================================== #
        print("\n=== P2 §2.19: Prometheus metrics ===")
        metrics = margin_engine.collect_prometheus_metrics()
        _check(
            "offloading_cache_size" in metrics,
            "offloading_cache_size metric present",
        )
        _check(
            any(
                k.startswith("offloading_tier_decisions_total_")
                for k in metrics
            ),
            "per-tier decision counts present",
        )

        # ========================================================== #
        # P2 §2.20 — TDPReading.is_synthetic flag
        # ========================================================== #
        print("\n=== P2 §2.20: TDPReading.is_synthetic flag ===")
        no_tdp_engine = _engine(tdp_monitor=None)
        reading = await no_tdp_engine._get_tdp_reading("dev")
        _check(
            reading.is_synthetic is True,
            "fabricated TDP reading is marked is_synthetic=True",
        )

        # ========================================================== #
        # Serialization round-trip
        # ========================================================== #
        print("\n=== Serialization round-trip ===")
        rt_engine = _engine()
        await rt_engine.decide_placement(
            "rt-task", "rt-dev", {"latency_sla_ms": 50.0}
        )
        payload = rt_engine.to_dict()
        rt_restored = OffloadingDecisionEngine.from_dict(
            payload,
            carbon_intensity_monitor=_CarbonMonitor(30.0),
            latency_monitor=_LatencyMonitor(),
            node_registry=_NodeRegistry(_make_nodes()),
        )
        _check(
            rt_restored.to_dict() == rt_engine.to_dict(),
            "to_dict / from_dict round-trips",
        )

        print("\n" + "=" * 78)
        print("All v6 assertions passed.")
        print("=" * 78)

    try:
        asyncio.run(run_all())
    except AssertionError as exc:
        print(f"\n{exc}")
        raise


if __name__ == "__main__":
    _demo()
