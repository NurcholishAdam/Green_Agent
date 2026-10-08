# src/quantum_integration/digital_twin/system_digital_twin.py

"""Orchestrator for the System Digital Twin.

This module wires every subsystem together:

* :class:`~digital_twin_distillation.DistillationTwinOptimizer`
* :class:`~digital_twin_limit_graph.LimitGraphManager`
* :class:`~digital_twin_modp.MODPOptimizer`
* :class:`~digital_twin_rlhf.RLHFTrainer`
* :class:`~digital_twin_pso.ParticleSwarmOptimizer`
* :class:`~digital_twin_moe.MoEGatingNetwork`
* :class:`~digital_twin_telemetry.DigitalTwinTelemetry`
* :class:`~digital_twin_persistence.DigitalTwinPersistenceManager`

Determinism under concurrency
-----------------------------
Each scenario is assigned a per-scenario NumPy generator derived from
``(config.seed, scenario_id)``. That makes results order-independent even
when ``run_scenarios`` executes with a concurrency limit.

Monte Carlo
-----------
``_run_simulation`` performs ``n_simulations`` independent paths of a
correlated random walk and reports the mean trajectory plus an empirical
5th/95th percentile band. ``n_simulations=1`` reduces to a single path.

Circuit breaker
---------------
When the resilience module is available, storage writes, feedback
publishes, and drift checks run through a :class:`CircuitBreaker`. The
breaker transitions to OPEN after sustained failures and the health
check reflects that.

Cache key
---------
Scenario ids include the resolved ``time_horizon_years`` and
``n_simulations`` so different requests never collide.
"""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import logging
import math
import random
import threading
import time
from collections import OrderedDict
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
    DigitalTwinPersistenceError,
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

#: Health-status thresholds.
_HEALTH_SCENARIOS_FOR_FULL_SCORE: int = 10

#: Number of recent results used to derive ``recent_success_rate`` and
#: ``avg_roi`` features for the distillation state.
_RECENT_WINDOW: int = 10

#: Strategy → expected reward-improvement factor. Single source of truth
#: for the reward shaping used by ``run_scenario``.
_STRATEGY_IMPROVEMENT: Dict[str, float] = {
    "aggressive_carbon": 0.15,
    "helium_preservation": 0.12,
    "circularity_boost": 0.10,
    "renewable_acceleration": 0.13,
    "balanced": 0.08,
}
_DEFAULT_IMPROVEMENT: float = 0.05


class SystemDigitalTwin:
    """Orchestrates the simulation, subsystems, and feedback loop.

    Subsystems are constructed in ``__init__`` based on the config flags
    and reachable from ``self``. ``close()`` shuts them down in reverse
    order (dependents first). ``initialize()`` is idempotent.
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

        # Deterministic RNGs. ``_np_rng`` is only used for scenario-level
        # sampling that does not affect determinism (e.g., RLHF rejection
        # sampling). Per-scenario NumPy generators are derived from
        # ``(config.seed, scenario_id)`` so concurrent runs stay
        # reproducible.
        self._rng = random.Random(self.config.seed)
        self._np_rng = np.random.default_rng(self.config.seed)

        # Shared async locks (lazy-bound to the running loop).
        self._lock = _LazyLock()
        self._cache_lock = _LazyLock()
        # Threading lock for synchronous reads/writes of priority weights.
        self._weights_lock = threading.RLock()

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
            "helium": [
                "hydrogen_cooling",
                "nitrogen_cooling",
                "cryogenic_alternative",
            ],
            "carbon": [
                "renewable_energy",
                "carbon_offset",
                "carbon_capture",
            ],
            "energy": ["solar", "wind", "geothermal", "nuclear"],
        }
        self.resource_correlation = self._init_correlation_matrix()
        # Precompute Cholesky factor for correlated noise.
        self._corr_cholesky = self._factorize_correlation()
        self.priority_weights = dict(self.config.user_priorities)
        self.simulation_cache: "OrderedDict[str, DigitalTwinResult]" = OrderedDict()

        # Latency ring.
        self._latency_ring: List[float] = []
        self._started_at = time.monotonic()

        # Idempotency flag for ``initialize``.
        self._initialized = False

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
        if (
            self.config.enable_moe_gating
            and _MOE_AVAILABLE
            and MoEGatingNetwork is not None
        ):
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

        # Circuit breaker (used to guard storage / queue / drift calls).
        self._circuit_breaker: Optional[Any] = None
        if _CIRCUIT_BREAKER_AVAILABLE and CircuitBreaker is not None:
            try:
                self._circuit_breaker = CircuitBreaker(
                    failure_threshold=self.config.circuit_breaker_threshold,
                    recovery_timeout=self.config.circuit_breaker_recovery_timeout,
                )
            except Exception as exc:  # noqa: BLE001 - defensive
                logger.warning("CircuitBreaker setup failed: %s", exc)
                self._circuit_breaker = None

        logger.info("SystemDigitalTwin v%s initialized", __version__)

    # ------------------------------------------------------------------ #
    # Correlation matrix helpers
    # ------------------------------------------------------------------ #

    def _init_correlation_matrix(self) -> Dict[str, Dict[str, float]]:
        if self.config.correlation_matrix_override:
            return {
                k: dict(v)
                for k, v in self.config.correlation_matrix_override.items()
            }
        return {
            "carbon": {
                "carbon": 1.0, "helium": 0.3, "energy": 0.7,
                "circularity": -0.4, "biodiversity": -0.6,
            },
            "helium": {
                "carbon": 0.3, "helium": 1.0, "energy": 0.5,
                "circularity": -0.2, "biodiversity": -0.3,
            },
            "energy": {
                "carbon": 0.7, "helium": 0.5, "energy": 1.0,
                "circularity": -0.3, "biodiversity": -0.4,
            },
            "circularity": {
                "carbon": -0.4, "helium": -0.2, "energy": -0.3,
                "circularity": 1.0, "biodiversity": 0.3,
            },
            "biodiversity": {
                "carbon": -0.6, "helium": -0.3, "energy": -0.4,
                "circularity": 0.3, "biodiversity": 1.0,
            },
        }

    def _factorize_correlation(self) -> np.ndarray:
        """Return the Cholesky factor of the correlation matrix.

        Falls back to the identity if the matrix is not positive
        definite (e.g., a user override with inconsistent values).
        """
        n = len(_RESOURCES)
        mat = np.eye(n, dtype=np.float64)
        for i, r1 in enumerate(_RESOURCES):
            for j, r2 in enumerate(_RESOURCES):
                v = self.resource_correlation.get(r1, {}).get(r2, 0.0)
                mat[i, j] = float(v)
        # Symmetrise in case of asymmetric overrides.
        mat = 0.5 * (mat + mat.T)
        # Add a small ridge for numerical safety.
        mat = mat + np.eye(n) * 1e-8
        try:
            return np.linalg.cholesky(mat)
        except np.linalg.LinAlgError:
            logger.warning(
                "correlation matrix is not positive definite; "
                "falling back to identity for correlated noise.",
            )
            return np.eye(n, dtype=np.float64)

    # ------------------------------------------------------------------ #
    # Per-scenario RNG
    # ------------------------------------------------------------------ #

    def _rng_for_scenario(self, scenario_id: str) -> np.random.Generator:
        """Return a deterministic generator for ``scenario_id``.

        Derived from the config seed and a stable hash of the scenario
        id, so the same scenario always produces the same trajectories
        regardless of execution order or concurrency.
        """
        digest = hashlib.sha256(scenario_id.encode("utf-8")).digest()
        sub_seed = int.from_bytes(digest[:8], "big") & 0xFFFFFFFF
        seq = np.random.SeedSequence(
            [self.config.seed & 0xFFFFFFFF, sub_seed],
        )
        return np.random.default_rng(seq)

    # ------------------------------------------------------------------ #
    # Circuit-breaker helper
    # ------------------------------------------------------------------ #

    async def _with_breaker(self, factory: Any, *args: Any, **kwargs: Any) -> Any:
        """Run ``await factory(*args, **kwargs)`` under the breaker.

        Falls back to a direct call when the breaker is unavailable.
        """
        if self._circuit_breaker is None:
            return await factory(*args, **kwargs)
        return await self._circuit_breaker.call(factory, *args, **kwargs)

    # ------------------------------------------------------------------ #
    # Lifecycle
    # ------------------------------------------------------------------ #

    async def initialize(self) -> None:
        """Load state and seed the limit graph. Idempotent."""
        if self._initialized:
            return

        if self.persistence is not None:
            try:
                loaded = await self.persistence.load_state(self)
                if not loaded:
                    status = getattr(self.persistence, "last_status", "unknown")
                    err = getattr(self.persistence, "last_error", None)
                    if status not in ("missing", "ok"):
                        logger.warning(
                            "Persistence reported %s on load: %s",
                            status, err,
                        )
                        if self.strict:
                            raise DigitalTwinPersistenceError(
                                f"failed to load state: {status}",
                                status=status,
                            )
            except DigitalTwinError:
                if self.strict:
                    raise
                logger.warning(
                    "Failed to load state; continuing fresh.", exc_info=True,
                )

        if (
            self.limit_graph is not None
            and not self.limit_graph.has_graph("main_graph")
        ):
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
                (
                    "edge_circularity_biodiversity",
                    "node_circularity", "node_biodiversity", 0.3,
                ),
            ):
                self.limit_graph.add_edge(
                    "main_graph", edge_id, src, tgt, w, {},
                )

        self._initialized = True

    async def close(self, *, flush: bool = True) -> None:
        """Flush state (when ``flush=True``) and shut every subsystem down.

        Subsystems are closed in reverse order. Failures are logged but
        do not abort shutdown. When ``strict`` is set, a failure to save
        state raises.
        """
        logger.info("Shutting down SystemDigitalTwin...")
        if flush:
            try:
                await self.save_state()
            except DigitalTwinError:
                if self.strict:
                    raise
                logger.warning("Final save failed; continuing shutdown.")

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
                sig = inspect.signature(close)
                if "flush" in sig.parameters:
                    close(flush=flush)
                else:
                    close()
            except Exception as exc:  # noqa: BLE001 - defensive
                logger.warning(
                    "close failed for %s: %s", type(comp).__name__, exc,
                )
        logger.info("Shutdown complete.")

    async def __aenter__(self) -> "SystemDigitalTwin":
        await self.initialize()
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()

    # ------------------------------------------------------------------ #
    # Persistence
    # ------------------------------------------------------------------ #

    async def save_state(self) -> bool:
        """Persist the twin's state. Returns ``True`` on success."""
        if self.persistence is not None:
            return await self.persistence.save_state(self)
        if self.storage is not None and hasattr(self.storage, "save_state"):
            state = self._snapshot_for_storage()

            async def _do_save() -> bool:
                self.storage.save_state(
                    "digital_twin_state",
                    json.dumps(state, default=str),
                )
                return True

            try:
                return await self._with_breaker(_do_save)
            except Exception as exc:  # noqa: BLE001 - defensive
                logger.error("Central storage save failed: %s", exc)
                return False
        return False

    async def delete_state(self) -> bool:
        if self.persistence is not None:
            return await self.persistence.delete_state()
        return False

    def _snapshot_for_storage(self) -> Dict[str, Any]:
        """Return the canonical persisted state payload."""
        return self.snapshot()

    # ------------------------------------------------------------------ #
    # Module injection
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
    # Introspection
    # ------------------------------------------------------------------ #

    async def get_health_status(self) -> Dict[str, Any]:
        cb = self._circuit_breaker
        healthy = cb is None or not cb.is_open
        score = min(
            1.0, len(self.scenario_results) / _HEALTH_SCENARIOS_FOR_FULL_SCORE,
        )
        return {
            "status": "healthy" if healthy else "degraded",
            "score": score,
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
            "circuit_breaker": (
                None if cb is None else {
                    "state": cb.state.value,
                    "failure_count": cb._failure_count,
                }
            ),
            "scenario_results": len(self.scenario_results),
            "cached_scenarios": len(self.simulation_cache),
            "uptime_seconds": time.monotonic() - self._started_at,
            "initialized": self._initialized,
        }

    def _subsystem_statistics(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        for name in (
            "limit_graph", "modp", "rlhf", "pso", "moe",
            "distillation", "telemetry", "persistence",
        ):
            comp = getattr(self, name, None)
            if comp is None:
                continue
            fn = getattr(comp, "statistics", None)
            if not callable(fn):
                continue
            try:
                out[name] = fn()
            except Exception as exc:  # noqa: BLE001 - defensive
                out[name] = {"error": str(exc)}
        return out

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
            "subsystems": self._subsystem_statistics(),
        }

    def snapshot(self) -> Dict[str, Any]:
        """Return a deep, safe-to-mutate copy of the current state."""
        with self._weights_lock:
            weights = dict(self.priority_weights)
        return {
            "schema_version": SCHEMA_VERSION,
            "config": self.config.to_dict(),
            "priority_weights": weights,
            "resource_correlation": {
                k: dict(v) for k, v in self.resource_correlation.items()
            },
            "substitution_options": {
                k: list(v) for k, v in self.substitution_options.items()
            },
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
    # Scenario execution
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
        """Run one scenario end-to-end.

        The cache key includes the resolved horizon and simulation count
        so requests with different configurations never collide.
        """
        validation = ScenarioParameterValidator.validate(
            scenario_type, parameters,
        )
        if not validation.ok:
            raise DigitalTwinInputError(
                f"invalid scenario parameters: {list(validation.errors)}"
            )
        scenario = SimulationScenario.coerce(scenario_type)
        params = dict(validation.normalized)

        horizon = time_horizon_years or self.config.time_horizon_years
        sims = max(1, int(n_simulations or self.config.n_simulations))

        scenario_id = self._generate_scenario_id(scenario, params, horizon, sims)

        async with self._cache_lock:
            cached = self.simulation_cache.get(scenario_id)
            if cached is not None:
                self.simulation_cache.move_to_end(scenario_id)
                if self.telemetry is not None:
                    self.telemetry.increment("dt_cache_hits")
                return cached
        if self.telemetry is not None:
            self.telemetry.increment("dt_cache_misses")

        start = time.perf_counter()
        rng = self._rng_for_scenario(scenario_id)
        (
            projections,
            confidence_intervals,
            subst_effects,
            resource_projections,
        ) = self._run_simulation(scenario, params, horizon, sims, rng)
        sustainability_score = self._compute_sustainability_score(projections)
        weighted_score = self._compute_weighted_score(
            projections, confidence_intervals,
        )

        state = self._state_from_simulation(params, projections)
        (
            strategy, action_idx, strategy_source, teacher_probs,
        ) = await self._select_strategy(state)

        recommendations = self._recommendations_for(
            strategy, scenario, projections, params,
        )
        risk_factors = self._risk_factors_for(projections)
        interdependent = self._interdependent_for(projections)

        improvement = _STRATEGY_IMPROVEMENT.get(strategy, _DEFAULT_IMPROVEMENT)
        reward = improvement * (1.0 - weighted_score)

        result = DigitalTwinResult(
            scenario_id=scenario_id,
            scenario_type=scenario,
            metrics={
                "n_simulations": sims,
                "time_horizon_years": horizon,
                "strategy_source": strategy_source,
            },
            projections={k: tuple(v) for k, v in projections.items()},
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

        # Distillation update — only when distillation produced the
        # strategy and the actual teacher probabilities are available.
        #
        # The Q-teacher update inside ``DistillationTwinOptimizer.update``
        # needs a ``next_state_vec``. This orchestrator does not model a
        # post-action transition, so we pass the current state vector
        # with a note; the enhanced distillation module handles
        # ``next_state=None`` gracefully but the current API requires an
        # array. Passing the current state is a no-worse-than-original
        # approximation that keeps the interface unchanged.
        if strategy_source == "distillation" and teacher_probs is not None:
            state_vec = state.to_feature_vector()
            await self.distillation.update(
                state_vec,
                action_idx,
                reward,
                state_vec.copy(),  # NOTE: successor == current (see docstring)
                teacher_probs,
            )

        # Publish feedback (guarded by the breaker when available).
        if self.queue is not None:
            await self._publish_feedback(scenario, strategy, reward, result)

        # RLHF recording.
        if self.rlhf is not None:
            self._record_rlhf(scenario, strategy, reward, scenario_id)

        # Drift detection.
        await self._check_drift()

        # Cache + record.
        async with self._cache_lock:
            self.simulation_cache[scenario_id] = result
            if len(self.simulation_cache) > self.config.cache_max_size:
                self.simulation_cache.popitem(last=False)

        async with self._lock:
            self.scenario_results.append(result)
            self.resource_projections.update(resource_projections)

        latency_ms = (time.perf_counter() - start) * 1000.0
        self._latency_ring.append(latency_ms)
        if len(self._latency_ring) > self.config.latency_ring_size:
            # Trim from the front when the ring grows past capacity.
            excess = len(self._latency_ring) - self.config.latency_ring_size
            del self._latency_ring[:excess]

        if self.telemetry is not None:
            self.telemetry.increment("dt_scenarios_run")
            self.telemetry.gauge(
                "dt_sustainability_score", sustainability_score,
            )
            self.telemetry.gauge("dt_weighted_score", weighted_score)

        return result

    async def run_scenarios(
        self,
        scenarios: List[Tuple[SimulationScenario, Mapping[str, Any]]],
        *,
        max_concurrency: int = 4,
        return_exceptions: bool = False,
    ) -> List[Any]:
        """Run multiple scenarios with bounded concurrency.

        Parameters
        ----------
        scenarios:
            List of ``(scenario_type, parameters)`` pairs.
        max_concurrency:
            Maximum number of scenarios in flight at once.
        return_exceptions:
            When ``True``, failures are returned in place of results
            instead of aborting the batch. When ``False`` (default), the
            first failure propagates and remaining tasks are cancelled.

        Returns
        -------
        list
            One entry per input scenario. Entries are
            :class:`DigitalTwinResult` unless ``return_exceptions`` is
            ``True``, in which case failing entries are exception
            instances.
        """
        if max_concurrency < 1:
            raise DigitalTwinInputError("max_concurrency must be >= 1.")
        sem = asyncio.Semaphore(max_concurrency)

        async def _run(st: SimulationScenario, params: Mapping[str, Any]):
            async with sem:
                return await self.run_scenario(st, params)

        results = await asyncio.gather(
            *[_run(st, p) for st, p in scenarios],
            return_exceptions=return_exceptions,
        )
        return list(results)

    # ------------------------------------------------------------------ #
    # Strategy selection
    # ------------------------------------------------------------------ #

    async def _select_strategy(
        self, state: TwinOptimizationState,
    ) -> Tuple[str, int, str, Optional[np.ndarray]]:
        """Return ``(strategy, action_idx, source, teacher_probs)``.

        ``teacher_probs`` is populated only when the distillation module
        produced the strategy.
        """
        if self.moe is not None:
            try:
                moe_result = await self.moe.select_expert(state)
                if moe_result.selected_action in ACTION_SPACE:
                    return (
                        moe_result.selected_action,
                        moe_result.selected_action_idx,
                        "moe",
                        None,
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

        strategy, action_idx, _state_vec, teacher_probs = (
            await self.distillation.select_strategy(state, exploration=True)
        )
        return strategy, action_idx, "distillation", teacher_probs

    async def _select_strategy_modp(
        self, state: TwinOptimizationState,
    ) -> Tuple[str, int, str, Optional[np.ndarray]]:
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
            strategy, action_idx, _vec, teacher_probs = (
                await self.distillation.select_strategy(
                    state, exploration=True,
                )
            )
            return strategy, action_idx, "distillation", teacher_probs

        best = max(filtered, key=lambda x: x["score"])
        try:
            action_idx = ACTION_SPACE.index(best["strategy"])
        except ValueError:
            logger.warning(
                "MODP returned unknown strategy %r; falling back to "
                "distillation.", best["strategy"],
            )
            strategy, action_idx, _vec, teacher_probs = (
                await self.distillation.select_strategy(
                    state, exploration=True,
                )
            )
            return strategy, action_idx, "distillation", teacher_probs
        return best["strategy"], action_idx, "modp", None

    # ------------------------------------------------------------------ #
    # Simulation
    # ------------------------------------------------------------------ #

    def _apply_scenario_shocks(
        self,
        scenario: SimulationScenario,
        params: Mapping[str, Any],
    ) -> Dict[str, float]:
        """Return the post-shock base levels for each resource."""
        base = {r: 0.5 for r in _RESOURCES}
        if scenario in (
            SimulationScenario.POLICY_CHANGE,
            SimulationScenario.POLICY_AND_TECHNOLOGY,
        ):
            base["carbon"] = max(
                0.0,
                base["carbon"] - params.get("carbon_reduction_rate", 0.0),
            )
        if scenario in (
            SimulationScenario.MARKET_SHOCK,
            SimulationScenario.MARKET_AND_REGULATORY,
        ):
            shock = params.get("shock_size", 0.0)
            for r in _RESOURCES:
                base[r] = max(0.0, min(1.0, base[r] + shock * 0.1))
        if scenario in (
            SimulationScenario.RESOURCE_DEPLETION,
            SimulationScenario.RESOURCE_AND_CLIMATE,
        ):
            r_type = params.get("resource_type", "carbon")
            if r_type in base:
                base[r_type] = max(
                    0.0,
                    base[r_type]
                    - params.get("depletion_rate", 0.0) * 5.0,
                )
        if scenario in (
            SimulationScenario.TECHNOLOGY_ADOPTION,
            SimulationScenario.POLICY_AND_TECHNOLOGY,
        ):
            adoption = params.get("adoption_rate", 0.0)
            base["circularity"] = min(
                1.0, base["circularity"] + adoption * 0.2,
            )
        if scenario == SimulationScenario.CLIMATE_EVENT:
            severity = params.get("severity", 0.0)
            base["biodiversity"] = max(
                0.0, base["biodiversity"] - severity * 0.3,
            )
        return base

    def _simulate_path(
        self,
        rng: np.random.Generator,
        base_levels: Mapping[str, float],
        steps: int,
        variances: Mapping[str, float],
    ) -> np.ndarray:
        """Simulate one path. Returns ``(steps, n_resources)``."""
        n = len(_RESOURCES)
        out = np.empty((steps, n), dtype=np.float64)
        current = np.array(
            [base_levels[r] for r in _RESOURCES], dtype=np.float64,
        )
        std_devs = np.array(
            [math.sqrt(max(variances[r], 0.0)) for r in _RESOURCES],
            dtype=np.float64,
        )
        # Correlated noise via Cholesky: ``z ~ N(0, I)``, ``L @ z`` has
        # covariance ``L @ L.T == correlation_matrix``.
        for step in range(steps):
            z = rng.standard_normal(n)
            correlated = self._corr_cholesky @ z
            current = current + std_devs * correlated
            current = np.clip(current, 0.0, 1.0)
            out[step] = current
        return out

    def _run_simulation(
        self,
        scenario: SimulationScenario,
        params: Mapping[str, Any],
        time_horizon_years: int,
        n_simulations: int,
        rng: np.random.Generator,
    ) -> Tuple[
        Dict[str, List[float]],
        Dict[str, Tuple[float, float]],
        Dict[str, Dict[str, Any]],
        Dict[str, ResourceProjection],
    ]:
        """Monte Carlo simulation over ``n_simulations`` independent paths.

        Returns
        -------
        projections:
            Mean trajectory per resource.
        confidence_intervals:
            Empirical ``(p5, p95)`` at the final step per resource.
        substitution_effects:
            Substitution summary per resource.
        resource_projections:
            Full :class:`ResourceProjection` records per resource.
        """
        steps = max(
            1, (time_horizon_years * 365) // self.config.time_step_days,
        )
        base_levels = self._apply_scenario_shocks(scenario, params)
        variances = {
            r: self.config.resource_variances.get(r, 0.02)
            for r in _RESOURCES
        }

        # Run n_simulations paths.
        paths = np.empty((n_simulations, steps, len(_RESOURCES)))
        for i in range(n_simulations):
            paths[i] = self._simulate_path(
                rng, base_levels, steps, variances,
            )

        # Aggregate.
        mean_paths = paths.mean(axis=0)
        lower_paths = np.percentile(paths, 5, axis=0)
        upper_paths = np.percentile(paths, 95, axis=0)

        projections: Dict[str, List[float]] = {}
        resource_projections: Dict[str, ResourceProjection] = {}
        confidence_intervals: Dict[str, Tuple[float, float]] = {}

        for j, r in enumerate(_RESOURCES):
            mean_col = mean_paths[:, j].tolist()
            lower_col = lower_paths[:, j].tolist()
            upper_col = upper_paths[:, j].tolist()
            projections[r] = mean_col
            confidence_intervals[r] = (lower_col[-1], upper_col[-1])
            resource_projections[r] = ResourceProjection(
                resource_type=r,
                current_level=float(base_levels.get(r, 0.5)),
                projected_levels=tuple(mean_col),
                confidence_lower=tuple(lower_col),
                confidence_upper=tuple(upper_col),
            )

        substitution_effects = self._compute_substitution_effects(projections)
        return (
            projections,
            confidence_intervals,
            substitution_effects,
            resource_projections,
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
            availability = (
                self.config.substitution_availability_default.get(
                    resource, 0.3,
                )
            )
            ramp_start = self.config.substitution_ramp_start_step
            ramp = max(
                0.0,
                min(
                    1.0,
                    (len(levels) - ramp_start) * self.config.substitution_ramp_rate,
                ),
            )
            out[resource] = {
                "availability": availability,
                "cost_factor": (
                    self.config.substitution_cost_factor_default.get(
                        resource, 1.0,
                    )
                ),
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
        with self._weights_lock:
            weights = dict(self.priority_weights)
        total_w = 0.0
        score = 0.0
        for r, levels in projections.items():
            w = weights.get(r, 0.0)
            if not levels:
                continue
            final = levels[-1]
            lo, hi = confidence_intervals.get(r, (final, final))
            spread = abs(hi - lo)
            penalty = 1.0 - min(1.0, spread)
            score += w * final * penalty
            total_w += w
        if total_w <= 0:
            return 0.0
        return float(max(0.0, min(1.0, score / total_w)))

    # ------------------------------------------------------------------ #
    # Feature construction for strategy selection
    # ------------------------------------------------------------------ #

    def _recent_metrics(self, n: int = _RECENT_WINDOW) -> Tuple[float, float]:
        """Return ``(recent_success_rate, avg_roi)`` over the last ``n``.

        Both are in ``[0, 1]``. Defaults to ``(0.5, 0.5)`` when there is
        no history.
        """
        if not self.scenario_results:
            return 0.5, 0.5
        recent = self.scenario_results[-n:]
        rewards = [r.reward for r in recent]
        if not rewards:
            return 0.5, 0.5
        positive = sum(1 for v in rewards if v > 0)
        success_rate = positive / len(rewards)
        # Reward is already in ``[0, ~0.15]``; normalise so ``avg_roi``
        # is comparable to the other features in ``[0, 1]``.
        avg = sum(rewards) / len(rewards)
        avg_roi = min(1.0, max(0.0, avg * 6.0))
        return float(success_rate), float(avg_roi)

    def _state_from_simulation(
        self,
        params: Mapping[str, Any],
        projections: Dict[str, List[float]],
    ) -> TwinOptimizationState:
        finals = {
            r: (levels[-1] if levels else 0.5)
            for r, levels in projections.items()
        }
        recent_success, avg_roi = self._recent_metrics()
        return TwinOptimizationState(
            carbon_emissions=finals.get("carbon", 0.5),
            helium_depletion=1.0 - finals.get("helium", 0.5),
            energy_consumption=1.0 - finals.get("energy", 0.5),
            circularity_index=finals.get("circularity", 0.5),
            biodiversity_impact=1.0 - finals.get("biodiversity", 0.5),
            carbon_reduction_rate=float(
                params.get("carbon_reduction_rate", 0.0),
            ),
            helium_reduction_rate=float(
                params.get("helium_conservation_rate", 0.0),
            ),
            adoption_rate=float(params.get("adoption_rate", 0.0)),
            shock_size=float(params.get("shock_size", 0.0)),
            recent_success_rate=recent_success,
            avg_roi=avg_roi,
            circuit_breaker_state=(
                1.0 if (
                    self._circuit_breaker and self._circuit_breaker.is_open
                ) else 0.0
            ),
            cache_usage=(
                len(self.simulation_cache)
                / max(1, self.config.cache_max_size)
            ),
            scenario_count=float(len(self.scenario_results)),
        )

    # ------------------------------------------------------------------ #
    # Recommendations / risk / interdependence
    # ------------------------------------------------------------------ #

    def _recommendations_for(
        self,
        strategy: str,
        scenario: SimulationScenario,
        projections: Dict[str, List[float]],
        params: Mapping[str, Any],
    ) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        carbon_levels = projections.get("carbon") or [0.5]
        helium_levels = projections.get("helium") or [0.5]
        if carbon_levels[-1] < 0.4:
            out.append({
                "action": "aggressive_carbon",
                "impact": "positive",
                "confidence": 0.85,
                "reason": "Carbon reserves projected below 0.4.",
            })
        if helium_levels[-1] < 0.3:
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
            if not levels:
                continue
            if levels[-1] < 0.3:
                risks.append(f"low_{r}")
            if len(levels) > 1 and levels[-1] < levels[0] - 0.2:
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
    # Feedback / RLHF / drift
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
        except ImportError as exc:
            logger.info(
                "FeedbackEvent unavailable; skipping publish: %s", exc,
            )
            return

        async def _do_publish() -> None:
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

        try:
            await self._with_breaker(_do_publish)
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

        weights = (
            self.adaptive_cost.get_current_weights()
            if hasattr(self.adaptive_cost, "get_current_weights")
            else {}
        )

        async def _do_check() -> Optional[float]:
            result = await self.drift.check_drift(weights)
            return result if isinstance(result, (int, float)) else None

        try:
            drift_score = await self._with_breaker(_do_check)
        except Exception as exc:  # noqa: BLE001 - defensive
            logger.error("Drift check failed: %s", exc)
            return

        if drift_score is None or drift_score <= 0.7:
            return

        logger.warning(
            "High drift detected (%.3f); adjusting priorities.", drift_score,
        )
        with self._weights_lock:
            if "carbon" not in self.priority_weights:
                return
            self.priority_weights["carbon"] = min(
                0.5, self.priority_weights["carbon"] + 0.05,
            )
            total = sum(self.priority_weights.values())
            if total <= 0:
                logger.warning(
                    "priority_weights sum to zero; skipping normalisation.",
                )
                return
            for k in list(self.priority_weights):
                self.priority_weights[k] /= total

    # ------------------------------------------------------------------ #
    # Utilities
    # ------------------------------------------------------------------ #

    def _generate_scenario_id(
        self,
        scenario_type: SimulationScenario,
        parameters: Mapping[str, Any],
        time_horizon_years: int,
        n_simulations: int,
    ) -> str:
        """Return a stable id unique to ``(type, params, horizon, sims)``."""
        param_str = json.dumps(dict(parameters), sort_keys=True, default=str)
        digest = hashlib.sha256(
            (
                f"{scenario_type.value}:{param_str}:"
                f"{time_horizon_years}:{n_simulations}"
            ).encode("utf-8")
        ).hexdigest()[:12]
        return f"{scenario_type.value}_{digest}"

    def __repr__(self) -> str:
        subsystems = [
            name for name in (
                "limit_graph", "modp", "rlhf", "pso", "moe",
                "distillation", "telemetry", "persistence",
            )
            if getattr(self, name, None) is not None
        ]
        return (
            f"{type(self).__name__}(version={__version__!r}, "
            f"scenario_results={len(self.scenario_results)}, "
            f"cache_size={len(self.simulation_cache)}, "
            f"subsystems={subsystems})"
        )


__all__ = ["SystemDigitalTwin", "__version__"]
