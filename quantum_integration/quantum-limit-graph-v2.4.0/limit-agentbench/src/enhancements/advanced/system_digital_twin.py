# src/quantum_integration/digital_twin/system_digital_twin.py

"""Orchestrator for the System Digital Twin."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import random
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Mapping, Optional, Tuple

import numpy as np

from .digital_twin_config import DigitalTwinConfig, SCHEMA_VERSION
from .digital_twin_distillation import (
    ACTION_SPACE,
    DistillationTwinOptimizer,
    TwinOptimizationState,
)
from .digital_twin_errors import (
    DigitalTwinError,
    DigitalTwinInputError,
    DigitalTwinSimulationError,
)
from .digital_twin_helpers import _LazyLock, _iso_now, _percentile
from .digital_twin_limit_graph import LimitGraphManager
from .digital_twin_modp import MODPOptimizer
from .digital_twin_persistence import DigitalTwinPersistenceManager
from .digital_twin_pso import ParticleSwarmOptimizer
from .digital_twin_rlhf import RLHFTrainer
from .digital_twin_schemas import (
    DigitalTwinResult,
    ResourceProjection,
    SimulationScenario,
)
from .digital_twin_telemetry import DigitalTwinTelemetry
from .digital_twin_validation import ScenarioParameterValidator

try:
    from .digital_twin_moe import MoEGatingNetwork
    _MOE_AVAILABLE = True
except Exception:  # pragma: no cover
    MoEGatingNetwork = None  # type: ignore[assignment]
    _MOE_AVAILABLE = False

try:
    from .digital_twin_resilience import CircuitBreaker
    _CIRCUIT_BREAKER_AVAILABLE = True
except Exception:  # pragma: no cover
    CircuitBreaker = None  # type: ignore[assignment]
    _CIRCUIT_BREAKER_AVAILABLE = False

logger = logging.getLogger(__name__)
__version__ = "7.0.0"

#: Default resource set tracked by the twin.
_RESOURCES: Tuple[str, ...] = (
    "carbon", "helium", "energy", "circularity", "biodiversity",
)


class SystemDigitalTwin:
    """Orchestrates the simulation, subsystems, and feedback loop.

    Subsystems (LIMIT graph, MODP, RLHF, PSO, MoE, distillation,
    telemetry, persistence) are constructed in ``__init__`` based on the
    config flags and reachable from ``self``. ``close()`` shuts them
    down in reverse order.
    """

    def __init__(
        self,
        config: Optional[DigitalTwinConfig] = None,
        *,
        storage: Optional[Any] = None,
        message_queue: Optional[Any] = None,
        adaptive_cost: Optional[Any] = None,
        pareto_gating: Optional[Any] = None,
        drift_detector: Optional[Any] = None,
        metrics: Optional[Any] = None,
        strict: bool = False,
        **kwargs: Any,
    ) -> None:
        self.config = config or DigitalTwinConfig()
        self.strict = bool(strict)

        # Deterministic RNGs.
        self._rng = random.Random(self.config.seed)
        self._np_rng = np.random.default_rng(self.config.seed)

        # Shared async locks (lazy-bound).
        self._lock = _LazyLock()
        self._cache_lock = _LazyLock()

        # External collaborators.
        self.storage = storage
        self.queue = message_queue
        self.adaptive_cost = adaptive_cost
        self.pareto = pareto_gating
        self.drift = drift_detector
        self.metrics = metrics

        # State.
        self.scenario_results: List[DigitalTwinResult] = []
        self.resource_projections: Dict[str, ResourceProjection] = {}
        self.substitution_options: Dict[str, List[str]] = {
            "helium": ["hydrogen_cooling", "nitrogen_cooling", "cryogenic_alternative"],
            "carbon": ["renewable_energy", "carbon_offset", "carbon_capture"],
            "energy": ["solar", "wind", "geothermal", "nuclear"],
        }
        self.resource_correlation = self._init_correlation_matrix()
        self.priority_weights = dict(self.config.user_priorities)
        self.simulation_cache: "OrderedDict" = self._new_cache()

        # Latency ring.
        self._latency_ring: List[float] = []
        self._started_at = time.monotonic()

        # Subsystems.
        self.distillation = DistillationTwinOptimizer(
            config=self.config, seed=self.config.seed,
        )
        self.limit_graph: Optional[LimitGraphManager] = (
            LimitGraphManager(storage) if self.config.enable_limit_graph else None
        )
        self.modp: Optional[MODPOptimizer] = (
            MODPOptimizer(storage) if self.config.enable_modp_solver else None
        )
        self.rlhf: Optional[RLHFTrainer] = (
            RLHFTrainer(storage, seed=self.config.seed)
            if self.config.enable_rlhf else None
        )
        self.pso: Optional[ParticleSwarmOptimizer] = (
            ParticleSwarmOptimizer(
                storage,
                num_particles=self.config.pso_particles,
                max_iter=self.config.pso_iterations,
                seed=self.config.seed,
            )
            if self.config.enable_pso_tuning else None
        )
        self.moe: Optional[Any] = None
        if self.config.enable_moe_gating and _MOE_AVAILABLE and MoEGatingNetwork is not None:
            self.moe = MoEGatingNetwork(
                storage,
                num_experts=self.config.moe_expert_count,
                seed=self.config.seed,
            )
        self.telemetry: Optional[DigitalTwinTelemetry] = (
            DigitalTwinTelemetry(
                prometheus_port=self.config.prometheus_port,
                ring_size=self.config.latency_ring_size,
            )
            if metrics is None else None
        )
        self.persistence: Optional[DigitalTwinPersistenceManager] = (
            DigitalTwinPersistenceManager(
                self.config.persistence_path,
                atomic_writes=self.config.atomic_writes,
            )
            if storage is None else None
        )

        # Circuit breaker.
        self._circuit_breaker = None
        if _CIRCUIT_BREAKER_AVAILABLE and CircuitBreaker is not None:
            self._circuit_breaker = CircuitBreaker(
                failure_threshold=self.config.circuit_breaker_threshold,
                recovery_timeout=self.config.circuit_breaker_recovery_timeout,
            )

        logger.info("SystemDigitalTwin v%s initialized", __version__)

    # ------------------------------------------------------------------ #
    @staticmethod
    def _new_cache():
        from collections import OrderedDict
        return OrderedDict()

    def _init_correlation_matrix(self) -> Dict[str, Dict[str, float]]:
        if self.config.correlation_matrix_override:
            return {
                k: dict(v)
                for k, v in self.config.correlation_matrix_override.items()
            }
        return {
            "carbon": {"carbon": 1.0, "helium": 0.3, "energy": 0.7, "circularity": -0.4, "biodiversity": -0.6},
            "helium": {"carbon": 0.3, "helium": 1.0, "energy": 0.5, "circularity": -0.2, "biodiversity": -0.3},
            "energy": {"carbon": 0.7, "helium": 0.5, "energy": 1.0, "circularity": -0.3, "biodiversity": -0.4},
            "circularity": {"carbon": -0.4, "helium": -0.2, "energy": -0.3, "circularity": 1.0, "biodiversity": 0.3},
            "biodiversity": {"carbon": -0.6, "helium": -0.3, "energy": -0.4, "circularity": 0.3, "biodiversity": 1.0},
        }

    # ------------------------------------------------------------------ #
    async def initialize(self) -> None:
        if self.persistence is not None:
            try:
                await self.persistence.load_state(self)
            except DigitalTwinError:
                if self.strict:
                    raise
                logger.warning("Failed to load state; continuing fresh.")
        if self.limit_graph is not None and not self.limit_graph.has_graph("main_graph"):
            self.limit_graph.create_graph(
                "main_graph", "Resource Dependency Graph", {},
            )
            for resource in _RESOURCES:
                self.limit_graph.add_node(
                    "main_graph", f"node_{resource}", resource,
                    {"current_level": 0.5},
                )
            for edge_id, src, tgt, w in (
                ("edge_carbon_energy", "node_carbon", "node_energy", 0.7),
                ("edge_helium_energy", "node_helium", "node_energy", 0.5),
                ("edge_circularity_biodiversity", "node_circularity", "node_biodiversity", 0.3),
            ):
                self.limit_graph.add_edge("main_graph", edge_id, src, tgt, w, {})

    async def close(self, *, flush: bool = True) -> None:
        """Flush state and shut every subsystem down in reverse order."""
        logger.info("Shutting down SystemDigitalTwin...")
        try:
            await self.save_state()
        except DigitalTwinError:
            if self.strict:
                raise
        for comp in (
            self.moe, self.pso, self.rlhf, self.modp, self.limit_graph,
            self.distillation, self.telemetry, self.persistence,
        ):
            if comp is None:
                continue
            close = getattr(comp, "close", None)
            if not callable(close):
                continue
            try:
                close(flush=flush)
            except TypeError:
                close()
            except Exception as exc:  # noqa: BLE001 - defensive
                logger.warning("close failed for %r: %s", type(comp).__name__, exc)
        logger.info("Shutdown complete.")

    async def __aenter__(self) -> "SystemDigitalTwin":
        await self.initialize()
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()

    # ------------------------------------------------------------------ #
    async def save_state(self) -> bool:
        if self.persistence is not None:
            return await self.persistence.save_state(self)
        if self.storage is not None and hasattr(self.storage, "save_state"):
            state = self._snapshot_for_storage()
            try:
                self.storage.save_state(
                    "digital_twin_state",
                    json.dumps(state, default=str),
                )
                return True
            except Exception as exc:  # noqa: BLE001
                logger.error("Central storage save failed: %s", exc)
                return False
        return False

    async def delete_state(self) -> bool:
        if self.persistence is not None:
            return await self.persistence.delete_state()
        return False

    def _snapshot_for_storage(self) -> Dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "scenario_results": [r.to_dict() for r in self.scenario_results],
            "priority_weights": dict(self.priority_weights),
            "resource_correlation": self.resource_correlation,
            "substitution_options": self.substitution_options,
            "distillation": self.distillation.to_dict(),
            "last_saved_at": _iso_now(),
        }

    # ------------------------------------------------------------------ #
    def inject_modules(self, **modules: Any) -> None:
        allowed = {
            "quantum_limits", "biodiversity", "expert_registry",
            "circular_manager", "carbon_manager", "helium_tracker",
            "predictive_analyzer",
        }
        for name, module in modules.items():
            if name not in allowed:
                raise DigitalTwinInputError(
                    f"unknown module name {name!r}; expected one of "
                    f"{sorted(allowed)}."
                )
            setattr(self, name, module)
            logger.info("Injected module: %s", name)

    # ------------------------------------------------------------------ #
    async def get_health_status(self) -> Dict[str, Any]:
        cb = self._circuit_breaker
        return {
            "status": "healthy" if (cb is None or not cb.is_open) else "degraded",
            "score": min(1.0, len(self.scenario_results) / 10),
            "central_components": {
                "storage": self.storage is not None,
                "queue": self.queue is not None,
                "metrics": self.metrics is not None,
                "drift": self.drift is not None,
            },
            "new_components": {
                "limit_graph": self.limit_graph is not None,
                "modp_solver": self.modp is not None,
                "rlhf_trainer": self.rlhf is not None,
                "pso_optimizer": self.pso is not None,
                "moe_gating": self.moe is not None,
            },
            "scenario_results": len(self.scenario_results),
            "cached_scenarios": len(self.simulation_cache),
            "uptime_seconds": time.monotonic() - self._started_at,
        }

    def statistics(self) -> Dict[str, Any]:
        latencies = list(self._latency_ring)
        return {
            "schema_version": SCHEMA_VERSION,
            "scenario_results": len(self.scenario_results),
            "cache_size": len(self.simulation_cache),
            "cache_capacity": self.config.cache_max_size,
            "mean_latency_ms": (
                sum(latencies) / len(latencies) if latencies else 0.0
            ),
            "p50_latency_ms": _percentile(latencies, 50),
            "p95_latency_ms": _percentile(latencies, 95),
            "max_latency_ms": max(latencies) if latencies else 0.0,
            "uptime_seconds": time.monotonic() - self._started_at,
        }

    def snapshot(self) -> Dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "config": self.config.to_dict(),
            "priority_weights": dict(self.priority_weights),
            "resource_correlation": self.resource_correlation,
            "substitution_options": self.substitution_options,
            "scenario_results": [r.to_dict() for r in self.scenario_results],
            "resource_projections": {
                k: v.to_dict() for k, v in self.resource_projections.items()
            },
            "statistics": self.statistics(),
        }

    def reset(
        self,
        *,
        clear_results: bool = True,
        clear_cache: bool = True,
    ) -> Dict[str, int]:
        out: Dict[str, int] = {}
        if clear_results:
            out["scenario_results"] = len(self.scenario_results)
            self.scenario_results.clear()
        if clear_cache:
            out["cache"] = len(self.simulation_cache)
            self.simulation_cache.clear()
        self._latency_ring.clear()
        self._started_at = time.monotonic()
        return out

    # ------------------------------------------------------------------ #
    async def run_scenario(
        self,
        scenario_type: SimulationScenario,
        parameters: Mapping[str, Any],
        *,
        time_horizon_years: Optional[int] = None,
        n_simulations: Optional[int] = None,
        container_tag: Optional[str] = None,
    ) -> DigitalTwinResult:
        """Run one scenario end-to-end."""
        validation = ScenarioParameterValidator.validate(scenario_type, parameters)
        if not validation.ok:
            raise DigitalTwinInputError(
                f"invalid scenario parameters: {list(validation.errors)}"
            )
        scenario = SimulationScenario.coerce(scenario_type)
        params = dict(validation.normalized)

        scenario_id = self._generate_scenario_id(scenario, params)

        async with self._cache_lock:
            cached = self.simulation_cache.get(scenario_id)
            if cached is not None:
                self.simulation_cache.move_to_end(scenario_id)
                if self.telemetry is not None:
                    self.telemetry.increment("dt_cache_hits")
                return cached
        if self.telemetry is not None:
            self.telemetry.increment("dt_cache_misses")

        horizon = time_horizon_years or self.config.time_horizon_years
        sims = n_simulations or self.config.n_simulations

        start = time.perf_counter()
        projections, confidence_intervals, subst_effects = self._run_simulation(
            scenario, params, horizon, sims,
        )
        sustainability_score = self._compute_sustainability_score(projections)
        weighted_score = self._compute_weighted_score(
            projections, confidence_intervals,
        )

        state = self._state_from_simulation(params, projections)
        strategy, action_idx, strategy_source = await self._select_strategy(state)

        recommendations = self._recommendations_for(
            strategy, scenario, projections, params,
        )
        risk_factors = self._risk_factors_for(projections)
        interdependent = self._interdependent_for(projections)

        # Reward = improvement potential given the strategy.
        improvement_factor = {
            "aggressive_carbon": 0.15,
            "helium_preservation": 0.12,
            "circularity_boost": 0.10,
            "renewable_acceleration": 0.13,
            "balanced": 0.08,
        }.get(strategy, 0.05)
        reward = improvement_factor * (1.0 - weighted_score)

        result = DigitalTwinResult(
            scenario_id=scenario_id,
            scenario_type=scenario,
            metrics={
                "n_simulations": sims,
                "time_horizon_years": horizon,
                "strategy_source": strategy_source,
            },
            projections={
                k: tuple(v) for k, v in projections.items()
            },
            confidence_intervals={
                k: tuple(v) for k, v in confidence_intervals.items()
            },
            risk_factors=tuple(risk_factors),
            recommendations=tuple(dict(r) for r in recommendations),
            sustainability_score=sustainability_score,
            interdependent_factors=tuple(interdependent),
            substitution_effects=subst_effects,
            weighted_score=weighted_score,
            strategy_used=strategy,
            reward=reward,
            container_tag=container_tag or "org:green-agent",
        )

        # Distillation update when distillation was the source.
        if strategy_source == "distillation":
            next_state = self._state_from_simulation(params, projections)
            await self.distillation.update(
                state.to_feature_vector(),
                action_idx,
                reward,
                next_state.to_feature_vector(),
                np.array([1.0 / len(ACTION_SPACE)] * len(ACTION_SPACE)),
            )

        # Publish feedback event.
        if self.queue is not None:
            await self._publish_feedback(scenario, strategy, reward, result)

        # RLHF.
        if self.rlhf is not None:
            self._record_rlhf(scenario, strategy, reward, scenario_id)

        # Drift check.
        await self._check_drift()

        # Cache.
        async with self._cache_lock:
            self.simulation_cache[scenario_id] = result
            if len(self.simulation_cache) > self.config.cache_max_size:
                self.simulation_cache.popitem(last=False)

        async with self._lock:
            self.scenario_results.append(result)

        latency_ms = (time.perf_counter() - start) * 1000.0
        self._latency_ring.append(latency_ms)

        if self.telemetry is not None:
            self.telemetry.increment("dt_scenarios_run")
            self.telemetry.gauge("dt_sustainability_score", sustainability_score)
            self.telemetry.gauge("dt_weighted_score", weighted_score)

        return result

    async def run_scenarios(
        self,
        scenarios: List[Tuple[SimulationScenario, Mapping[str, Any]]],
        *,
        max_concurrency: int = 4,
    ) -> List[DigitalTwinResult]:
        sem = asyncio.Semaphore(max(1, max_concurrency))

        async def _run(st: SimulationScenario, params: Mapping[str, Any]):
            async with sem:
                return await self.run_scenario(st, params)

        return await asyncio.gather(*[_run(st, p) for st, p in scenarios])

    # ------------------------------------------------------------------ #
    # Strategy selection
    # ------------------------------------------------------------------ #
    async def _select_strategy(
        self, state: TwinOptimizationState,
    ) -> Tuple[str, int, str]:
        if self.moe is not None:
            try:
                moe_result = await self.moe.select_expert(state.__dict__)
                if moe_result.selected_action in ACTION_SPACE:
                    return (
                        moe_result.selected_action,
                        moe_result.selected_action_idx,
                        "moe",
                    )
            except Exception as exc:  # noqa: BLE001 - defensive
                logger.warning("MoE selection failed: %s", exc)

        if (
            self.adaptive_cost is not None
            and self.pareto is not None
            and self.modp is not None
        ):
            try:
                return await self._select_strategy_modp(state)
            except Exception as exc:  # noqa: BLE001 - defensive
                logger.warning("MODP selection failed: %s", exc)

        strategy, action_idx, _, _ = await self.distillation.select_strategy(
            state, exploration=True,
        )
        return strategy, action_idx, "distillation"

    async def _select_strategy_modp(
        self, state: TwinOptimizationState,
    ) -> Tuple[str, int, str]:
        candidates: List[Dict[str, Any]] = []
        for idx, strategy in enumerate(ACTION_SPACE):
            carbon_g = 400.0 + 200.0 * idx
            latency_ms = 100.0 + 20.0 * idx
            energy_j = 500.0 + 100.0 * idx
            quality = 0.9 - 0.05 * idx
            score = (
                self.adaptive_cost.compute(
                    quality=quality,
                    carbon_g=carbon_g,
                    latency_ms=latency_ms,
                    energy_joules=energy_j,
                    health=0.8,
                    atp=0.5,
                )
                if hasattr(self.adaptive_cost, "compute")
                else 0.5
            )
            candidates.append({
                "strategy": strategy,
                "score": float(score),
                "carbon_g": carbon_g,
                "latency_ms": latency_ms,
                "quality": quality,
            })

        filtered = candidates
        if self.pareto is not None and hasattr(self.pareto, "filter"):
            try:
                filtered = list(self.pareto.filter(candidates))
            except Exception as exc:  # noqa: BLE001 - defensive
                logger.warning("Pareto filter failed: %s", exc)

        if not filtered:
            strategy, action_idx, _, _ = await self.distillation.select_strategy(
                state, exploration=True,
            )
            return strategy, action_idx, "distillation"

        best = max(filtered, key=lambda x: x["score"])
        return best["strategy"], ACTION_SPACE.index(best["strategy"]), "modp"

    # ------------------------------------------------------------------ #
    # Simulation
    # ------------------------------------------------------------------ #
    def _run_simulation(
        self,
        scenario: SimulationScenario,
        params: Mapping[str, Any],
        time_horizon_years: int,
        n_simulations: int,
    ) -> Tuple[Dict[str, List[float]], Dict[str, Tuple[float, float]], Dict[str, Dict[str, Any]]]:
        """Deterministic simulation using the correlation matrix."""
        steps = max(1, (time_horizon_years * 365) // self.config.time_step_days)
        base_levels = {r: 0.5 for r in _RESOURCES}
        variances = {r: self.config.resource_variances.get(r, 0.02) for r in _RESOURCES}

        # Apply scenario shocks.
        if scenario in (SimulationScenario.POLICY_CHANGE, SimulationScenario.POLICY_AND_TECHNOLOGY):
            base_levels["carbon"] = max(0.0, base_levels["carbon"] - params.get("carbon_reduction_rate", 0.0))
        if scenario in (SimulationScenario.MARKET_SHOCK, SimulationScenario.MARKET_AND_REGULATORY):
            shock = params.get("shock_size", 0.0)
            for r in _RESOURCES:
                base_levels[r] = max(0.0, min(1.0, base_levels[r] + shock * 0.1))
        if scenario in (SimulationScenario.RESOURCE_DEPLETION, SimulationScenario.RESOURCE_AND_CLIMATE):
            r_type = params.get("resource_type", "carbon")
            if r_type in base_levels:
                base_levels[r_type] = max(
                    0.0,
                    base_levels[r_type] - params.get("depletion_rate", 0.0) * 5.0,
                )
        if scenario in (SimulationScenario.TECHNOLOGY_ADOPTION, SimulationScenario.POLICY_AND_TECHNOLOGY):
            adoption = params.get("adoption_rate", 0.0)
            base_levels["circularity"] = min(1.0, base_levels["circularity"] + adoption * 0.2)
        if scenario == SimulationScenario.CLIMATE_EVENT:
            severity = params.get("severity", 0.0)
            base_levels["biodiversity"] = max(0.0, base_levels["biodiversity"] - severity * 0.3)

        # Trajectories using correlation-coupled random walk.
        projections: Dict[str, List[float]] = {r: [] for r in _RESOURCES}
        lower: Dict[str, List[float]] = {r: [] for r in _RESOURCES}
        upper: Dict[str, List[float]] = {r: [] for r in _RESOURCES}

        current = dict(base_levels)
        for step in range(steps):
            # Use correlation coupling to modulate each step.
            new_levels: Dict[str, float] = {}
            for r in _RESOURCES:
                coupled_drift = 0.0
                for other in _RESOURCES:
                    coupled_drift += (
                        self.resource_correlation[r][other]
                        * (base_levels[other] - current[other])
                    ) * 0.01
                noise = float(self._np_rng.normal(0.0, variances[r]))
                current[r] = max(0.0, min(1.0, current[r] + coupled_drift + noise))
                new_levels[r] = current[r]
            for r in _RESOURCES:
                projections[r].append(new_levels[r])
                # Confidence interval grows with horizon.
                spread = variances[r] * math.sqrt(step + 1) * 2.0
                lower[r].append(max(0.0, new_levels[r] - spread))
                upper[r].append(min(1.0, new_levels[r] + spread))

        confidence_intervals: Dict[str, Tuple[float, float]] = {}
        for r in _RESOURCES:
            confidence_intervals[r] = (lower[r][-1], upper[r][-1])

        substitution_effects = self._compute_substitution_effects(projections)
        return (
            {r: [float(x) for x in projections[r]] for r in _RESOURCES},
            confidence_intervals,
            substitution_effects,
        )

    def _compute_substitution_effects(
        self, projections: Dict[str, List[float]],
    ) -> Dict[str, Dict[str, Any]]:
        out: Dict[str, Dict[str, Any]] = {}
        if not self.config.resource_substitution_enabled:
            return out
        for resource, levels in projections.items():
            if resource not in self.substitution_options:
                continue
            depletion = 1.0 - (levels[-1] if levels else 0.5)
            availability = self.config.substitution_availability_default.get(resource, 0.3)
            ramp_start = self.config.substitution_ramp_start_step
            ramp = max(0.0, min(1.0, (len(levels) - ramp_start) * self.config.substitution_ramp_rate))
            out[resource] = {
                "availability": availability,
                "cost_factor": self.config.substitution_cost_factor_default.get(resource, 1.0),
                "ramp_progress": ramp,
                "alternatives": self.substitution_options.get(resource, []),
                "triggered": depletion > 0.3 and availability > 0.2,
            }
        return out

    def _compute_sustainability_score(
        self, projections: Dict[str, List[float]],
    ) -> float:
        if not projections:
            return 0.0
        finals = [levels[-1] for levels in projections.values() if levels]
        if not finals:
            return 0.0
        return float(max(0.0, min(1.0, sum(finals) / len(finals))))

    def _compute_weighted_score(
        self,
        projections: Dict[str, List[float]],
        confidence_intervals: Dict[str, Tuple[float, float]],
    ) -> float:
        if not projections:
            return 0.0
        total_w = 0.0
        score = 0.0
        for r, levels in projections.items():
            w = self.priority_weights.get(r, 0.0)
            if not levels:
                continue
            final = levels[-1]
            lo, hi = confidence_intervals.get(r, (final, final))
            spread = abs(hi - lo)
            penalty = 1.0 - min(1.0, spread)
            score += w * final * penalty
            total_w += w
        return float(max(0.0, min(1.0, score / total_w))) if total_w > 0 else 0.0

    def _state_from_simulation(
        self,
        params: Mapping[str, Any],
        projections: Dict[str, List[float]],
    ) -> TwinOptimizationState:
        finals = {r: (levels[-1] if levels else 0.5) for r, levels in projections.items()}
        return TwinOptimizationState(
            carbon_emissions=finals.get("carbon", 0.5),
            helium_depletion=1.0 - finals.get("helium", 0.5),
            energy_consumption=1.0 - finals.get("energy", 0.5),
            circularity_index=finals.get("circularity", 0.5),
            biodiversity_impact=1.0 - finals.get("biodiversity", 0.5),
            carbon_reduction_rate=float(params.get("carbon_reduction_rate", 0.0)),
            helium_reduction_rate=float(params.get("helium_conservation_rate", 0.0)),
            adoption_rate=float(params.get("adoption_rate", 0.0)),
            shock_size=float(params.get("shock_size", 0.0)),
            recent_success_rate=0.7,
            avg_roi=0.5,
            circuit_breaker_state=1.0 if (self._circuit_breaker and self._circuit_breaker.is_open) else 0.0,
            cache_usage=len(self.simulation_cache) / max(1, self.config.cache_max_size),
            scenario_count=float(len(self.scenario_results)),
        )

    def _recommendations_for(
        self,
        strategy: str,
        scenario: SimulationScenario,
        projections: Dict[str, List[float]],
        params: Mapping[str, Any],
    ) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        carbon_final = projections.get("carbon", [0.5])[-1]
        helium_final = projections.get("helium", [0.5])[-1]
        if carbon_final < 0.4:
            out.append({
                "action": "aggressive_carbon",
                "impact": "positive",
                "confidence": 0.85,
                "reason": "Carbon reserves projected below 0.4.",
            })
        if helium_final < 0.3:
            out.append({
                "action": "helium_preservation",
                "impact": "positive",
                "confidence": 0.8,
                "reason": "Helium reserves near depletion.",
            })
        if not out:
            out.append({
                "action": strategy,
                "impact": "neutral",
                "confidence": 0.6,
                "reason": f"Continue {strategy} policy.",
            })
        return out

    def _risk_factors_for(
        self, projections: Dict[str, List[float]],
    ) -> List[str]:
        risks: List[str] = []
        for r, levels in projections.items():
            if levels and levels[-1] < 0.3:
                risks.append(f"low_{r}")
            if levels and len(levels) > 1 and levels[-1] < levels[0] - 0.2:
                risks.append(f"declining_{r}")
        return risks

    def _interdependent_for(
        self, projections: Dict[str, List[float]],
    ) -> List[str]:
        out: List[str] = []
        for r1 in projections:
            for r2 in projections:
                if r1 >= r2:
                    continue
                corr = self.resource_correlation.get(r1, {}).get(r2, 0.0)
                if abs(corr) > 0.5:
                    out.append(f"{r1}:{r2}")
        return out

    # ------------------------------------------------------------------ #
    # Feedback + drift + RLHF
    # ------------------------------------------------------------------ #
    async def _publish_feedback(
        self,
        scenario: SimulationScenario,
        strategy: str,
        reward: float,
        result: DigitalTwinResult,
    ) -> None:
        try:
            from ...schemas.feedback_event import FeedbackEvent
        except ImportError:
            return
        try:
            event = FeedbackEvent.create_with_context(
                task_id=result.scenario_id,
                selected_action=strategy,
                quality_score=result.sustainability_score,
                energy_joules=0.0,
                carbon_g=0.0,
                feedback_type="digital_twin",
                adaptive_cost_value=reward,
                state={
                    "scenario_type": scenario.value,
                    "strategy": strategy,
                    "reward": reward,
                },
                candidates=[{"action": s} for s in ACTION_SPACE],
                source="system_digital_twin",
                tags=["digital_twin", "simulation"],
            )
            await self.queue.publish("feedback_events", event.to_json())
        except Exception as exc:  # noqa: BLE001 - defensive
            logger.error("Failed to publish feedback: %s", exc)

    def _record_rlhf(
        self,
        scenario: SimulationScenario,
        strategy: str,
        reward: float,
        scenario_id: str,
    ) -> None:
        if self.rlhf is None:
            return
        rejected_options = [s for s in ACTION_SPACE if s != strategy]
        if not rejected_options:
            return
        rejected = self._rng.choice(rejected_options)
        try:
            self.rlhf.record_pair(
                pair_id=f"{scenario_id}_pref",
                prompt=f"Which strategy is better for {scenario.value}?",
                chosen=strategy,
                rejected=rejected,
                reward_diff=reward,
                metadata={"scenario_id": scenario_id},
            )
        except DigitalTwinInputError:
            # Already recorded — expected on cache hits.
            pass
        except Exception as exc:  # noqa: BLE001 - defensive
            logger.error("RLHF recording failed: %s", exc)

    async def _check_drift(self) -> None:
        if self.drift is None or self.adaptive_cost is None:
            return
        try:
            weights = (
                self.adaptive_cost.get_current_weights()
                if hasattr(self.adaptive_cost, "get_current_weights")
                else {}
            )
            drift_score = await self.drift.check_drift(weights)
            if drift_score and drift_score > 0.7:
                logger.warning(
                    "High drift detected (%.3f); adjusting priorities.",
                    drift_score,
                )
                async with self._lock:
                    self.priority_weights["carbon"] = min(
                        0.5, self.priority_weights["carbon"] + 0.05,
                    )
                    total = sum(self.priority_weights.values())
                    for k in self.priority_weights:
                        self.priority_weights[k] /= total
        except Exception as exc:  # noqa: BLE001 - defensive
            logger.error("Drift check failed: %s", exc)

    # ------------------------------------------------------------------ #
    def _generate_scenario_id(
        self,
        scenario_type: SimulationScenario,
        parameters: Mapping[str, Any],
    ) -> str:
        param_str = json.dumps(dict(parameters), sort_keys=True)
        digest = hashlib.sha256(
            f"{scenario_type.value}:{param_str}".encode()
        ).hexdigest()[:12]
        return f"{scenario_type.value}_{digest}"


__all__ = ["SystemDigitalTwin", "__version__"]
