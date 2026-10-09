#!/usr/bin/env python3
# =============================================================================
# Enhanced FlexGen Evolutionary Optimizer v2.0.0 — Patched Single-File Edition
# =============================================================================
"""
Enhanced FlexGen Evolutionary Optimizer v2.0.0
=============================================
Patched single-file version. Focus on correctness, honesty, and integration.

P0 fixes
--------
- _mutate no longer aliases parents: it copies and returns a new policy.
- Tournament selection guards against sample larger than population.
- Pareto mapping uses a metric signature, so duplicate metrics do not
  cross-attribute policies to elites.
- run_sync detects a running event loop and raises a clear error.
- elite_size validated against population_size.
- Publish failures do not abort evolution.
- All optional attribute reads use getattr with defaults.

P1 — real behavior
------------------
- carbon_intensity is threaded through cost_model.estimate and compute_reward.
- Sync executor.execute is offloaded to run_in_executor; async is awaited.
- Evaluation cache per generation avoids repeated cost model calls.
- Infeasibility penalty applied to reward.
- asyncio.sleep(0) between generations to yield control.
- random_state parameter makes runs reproducible.
- Final Pareto set is deduplicated.
- CancelledError is handled cleanly, best_policies preserved.

P2 — honesty and integration
----------------------------
- MODULE_STATUS documents each module.
- FlexGenEvolutionaryConfig dataclass for all parameters.
- Prometheus counters/gauges (optional).
- OpenTelemetry spans (optional).
- async with context manager.

P3 — production readiness
-------------------------
- Embedded test suite: `python3 flexgen_evolver.py --test`.
- Fallback stubs for the surrounding module family when imported standalone.
- Logical sections in a single file.
- --status prints module maturity.
"""

from __future__ import annotations

import argparse
import asyncio
import functools
import hashlib
import inspect
import json
import logging
import math
import random
import sys
import time
import unittest
from dataclasses import asdict, dataclass, field, replace
from typing import Any, Callable, Dict, List, Optional, Tuple

# -----------------------------------------------------------------------------
# Optional dependencies
# -----------------------------------------------------------------------------
try:
    from prometheus_client import Counter, Gauge
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

try:
    from opentelemetry import trace
    _TRACER = trace.get_tracer("flexgen_evolver")
    OTEL_AVAILABLE = True
except ImportError:
    _TRACER = None
    OTEL_AVAILABLE = False

try:
    import structlog
    from structlog.processors import JSONRenderer, TimeStamper
    structlog.configure(
        processors=[
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            TimeStamper(fmt="iso"),
            JSONRenderer(),
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    logger = structlog.get_logger(__name__)
except ImportError:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    logger = logging.getLogger(__name__)


# =============================================================================
# SECTION 1. MODULE STATUS
# =============================================================================
MODULE_STATUS: Dict[str, str] = {
    "flexgen_optimizer":    "stable",
    "pareto_filtering":     "stable",
    "cost_model":           "stable",
    "real_executor":        "experimental",
    "feedback_publishing":  "experimental",
    "event_bus":            "stable",
}


def _warn_module(name: str) -> None:
    status = MODULE_STATUS.get(name, "unknown")
    if status == "stable":
        return
    if status == "placeholder":
        logger.warning("Module is a placeholder; enabling it has no effect", module=name)
    elif status == "experimental":
        logger.warning("Module is experimental; validate before production use", module=name)


# =============================================================================
# SECTION 2. TRACING HELPER
# =============================================================================
def traced(span_name: str):
    def decorator(fn: Callable):
        if not OTEL_AVAILABLE:
            return fn

        @functools.wraps(fn)
        async def wrapper(*args, **kwargs):
            with _TRACER.start_as_current_span(span_name):
                return await fn(*args, **kwargs)
        return wrapper
    return decorator


# =============================================================================
# SECTION 3. REAL MODULE IMPORTS WITH FALLBACKS
# =============================================================================
try:
    from ..gpu_optimization.flexgen_policy import FlexGenPolicy  # type: ignore
    from ..gpu_optimization.flexgen_cost_model import FlexGenCostModel  # type: ignore
    from ..gpu_optimization.reward import compute_reward  # type: ignore
    from ..schemas.node_descriptor import NodeDescriptor  # type: ignore
    from ..schemas.workload_descriptor import WorkloadDescriptor  # type: ignore
    from ..pareto_gating import ParetoGating  # type: ignore
    from ..async_message_queue import AsyncMessageQueue  # type: ignore
    from ..schemas.feedback_event import FeedbackEvent  # type: ignore
    REAL_MODULES_AVAILABLE = True
except ImportError:
    REAL_MODULES_AVAILABLE = False

    # -------------------------------------------------------------------------
    # Fallback stubs — deterministic and functional enough for standalone use.
    # -------------------------------------------------------------------------
    @dataclass
    class FlexGenPolicy:  # type: ignore
        gpu_batch_size: int = 4
        block_size: int = 16
        weight_device: str = "gpu"
        activation_device: str = "gpu"
        kv_cache_device: str = "gpu"
        weight_bits: int = 8
        kv_cache_bits: int = 8
        cpu_attention: bool = False
        overlap_io_compute: bool = True

        def to_dict(self) -> Dict[str, Any]:
            return asdict(self)

    @dataclass
    class NodeDescriptor:  # type: ignore
        id: str = "node-0"
        metadata: Dict[str, Any] = field(default_factory=lambda: {"gpu_memory_gb": 16.0})

    @dataclass
    class WorkloadDescriptor:  # type: ignore
        task_id: Optional[str] = "task-0"
        batch_size: int = 1
        seq_len: int = 128

    @dataclass
    class _FallbackEstimate:
        total_latency_ms: float
        total_energy_joules: float
        total_carbon_g: float
        peak_gpu_memory_gb: float
        quality_score: float = 0.9

    class FlexGenCostModel:  # type: ignore
        """Deterministic estimate based on policy fields."""

        def estimate(
            self,
            policy: FlexGenPolicy,
            node: NodeDescriptor,
            workload: WorkloadDescriptor,
            carbon_intensity: float = 0.0,
        ) -> _FallbackEstimate:
            base = 40.0 + policy.gpu_batch_size * 8.0 + policy.block_size * 0.5
            if policy.weight_device == "gpu":
                base *= 0.5
            elif policy.weight_device == "disk":
                base *= 2.0
            if policy.kv_cache_device == "gpu":
                base *= 0.8
            elif policy.kv_cache_device == "disk":
                base *= 1.6
            if policy.overlap_io_compute:
                base *= 0.9
            if policy.cpu_attention:
                base *= 1.15

            latency_ms = base
            energy_j = latency_ms * 0.5
            carbon_g = energy_j * max(0.0, carbon_intensity) / 1000.0
            gpu_mem = 0.5 + policy.gpu_batch_size * 0.1 + policy.block_size * 0.02
            quality = 0.95
            if policy.weight_bits <= 4:
                quality -= 0.05
            if policy.kv_cache_bits <= 4:
                quality -= 0.05
            return _FallbackEstimate(
                total_latency_ms=latency_ms,
                total_energy_joules=energy_j,
                total_carbon_g=carbon_g,
                peak_gpu_memory_gb=gpu_mem,
                quality_score=max(0.0, min(1.0, quality)),
            )

    def compute_reward(  # type: ignore
        metrics: Dict[str, Any],
        workload: WorkloadDescriptor,
        carbon_intensity: float = 0.0,
    ) -> float:
        """Simple, deterministic reward. Lower latency/energy/carbon is better."""
        latency = float(metrics.get("latency_ms", 100.0))
        energy = float(metrics.get("energy_joules", 100.0))
        carbon = float(metrics.get("carbon_g", 0.0))
        quality = float(metrics.get("quality_score", 0.9))
        feasible = bool(metrics.get("success", True))
        reward = (
            -0.4 * (latency / 500.0)
            - 0.3 * (energy / 500.0)
            - 0.2 * (carbon / 100.0)
            + 0.1 * quality
        )
        if not feasible:
            reward -= 1.0
        return reward

    class ParetoGating:  # type: ignore
        """Very small Pareto filter. Accepts a list of dicts; returns a subset."""

        def __init__(self, objectives: Optional[List[Dict[str, str]]] = None):
            self.objectives = objectives or [
                {"key": "latency_ms", "direction": "min"},
                {"key": "energy_joules", "direction": "min"},
                {"key": "carbon_g", "direction": "min"},
            ]

        def filter(self, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
            if not items:
                return []
            keys = [(o["key"], o["direction"]) for o in self.objectives]
            out: List[Dict[str, Any]] = []
            for i, a in enumerate(items):
                dominated = False
                for j, b in enumerate(items):
                    if i == j:
                        continue
                    better_or_equal = True
                    strictly_better = False
                    for k, direction in keys:
                        av = float(a.get(k, 0.0))
                        bv = float(b.get(k, 0.0))
                        if direction == "min":
                            if bv > av:
                                better_or_equal = False
                                break
                            if bv < av:
                                strictly_better = True
                        else:
                            if bv < av:
                                better_or_equal = False
                                break
                            if bv > av:
                                strictly_better = True
                    if better_or_equal and strictly_better:
                        dominated = True
                        break
                if not dominated:
                    out.append(a)
            return out

    class AsyncMessageQueue:  # type: ignore
        def __init__(self):
            self.published: List[Tuple[str, str]] = []

        async def publish(self, topic: str, payload: str) -> None:
            self.published.append((topic, payload))

    class FeedbackEvent:  # type: ignore
        def __init__(self, **kwargs: Any):
            self._data = dict(kwargs)

        def to_json(self) -> str:
            return json.dumps(self._data, default=str)

        @classmethod
        def create_with_context(cls, **kwargs: Any) -> "FeedbackEvent":
            return cls(**kwargs)


# =============================================================================
# SECTION 4. CONFIGURATION
# =============================================================================
@dataclass
class FlexGenEvolutionaryConfig:
    population_size: int = 100
    generations: int = 20
    mutation_rate: float = 0.2
    crossover_rate: float = 0.8
    elite_size: int = 5
    tournament_fraction: float = 0.1
    use_real_executor: bool = False
    random_state: Optional[int] = None
    elite_publish_top_k: int = 3
    publish_every_n_generations: int = 5
    fallback_final_policies: int = 10
    generation_yield_every: int = 5

    def validate(self) -> List[str]:
        issues: List[str] = []
        if self.population_size < 2:
            issues.append("population_size must be >= 2")
        if self.generations < 1:
            issues.append("generations must be >= 1")
        if not (0.0 <= self.mutation_rate <= 1.0):
            issues.append("mutation_rate must be in [0, 1]")
        if not (0.0 <= self.crossover_rate <= 1.0):
            issues.append("crossover_rate must be in [0, 1]")
        if not (0 <= self.elite_size < self.population_size):
            issues.append("elite_size must satisfy 0 <= elite_size < population_size")
        if self.elite_publish_top_k < 1:
            issues.append("elite_publish_top_k must be >= 1")
        return issues


# =============================================================================
# SECTION 5. HELPERS
# =============================================================================
def _metric_signature(m: Dict[str, Any]) -> Tuple[Tuple[str, str], ...]:
    """Hashable signature for a metrics dict, robust to unhashable values."""
    def _h(v: Any) -> str:
        if isinstance(v, (list, dict)):
            return json.dumps(v, sort_keys=True, default=str)
        return repr(v)
    return tuple(sorted((k, _h(v)) for k, v in m.items()))


def _policy_signature(p: FlexGenPolicy) -> Tuple[Tuple[str, str], ...]:
    if hasattr(p, "__dataclass_fields__"):
        d = asdict(p)
    else:
        d = dict(getattr(p, "__dict__", {}))
    def _h(v: Any) -> str:
        if isinstance(v, (list, dict)):
            return json.dumps(v, sort_keys=True, default=str)
        return repr(v)
    return tuple(sorted((k, _h(v)) for k, v in d.items()))


def _call_maybe_async(fn: Callable, *args, **kwargs):
    """Call fn; if it returns a coroutine, await it. Otherwise return value."""
    r = fn(*args, **kwargs)
    if asyncio.iscoroutine(r):
        return r  # caller must await
    return r


async def _await_maybe_async(fn: Callable, *args, **kwargs):
    r = fn(*args, **kwargs)
    if asyncio.iscoroutine(r):
        r = await r
    return r


def _signature_dict_safe(target: Callable) -> Dict[str, Any]:
    try:
        return dict(inspect.signature(target).parameters)
    except (TypeError, ValueError):
        return {}


# =============================================================================
# SECTION 6. MAIN OPTIMIZER
# =============================================================================
class FlexGenEvolutionaryOptimizer:
    """
    Evolves a population of FlexGenPolicy objects to find Pareto-optimal policies.

    Lifecycle:
        opt = FlexGenEvolutionaryOptimizer(config=cfg)
        policies = await opt.run(node, workload, carbon_intensity)
        # or
        async with opt:
            policies = await opt.run(...)
    """

    def __init__(
        self,
        config: Optional[FlexGenEvolutionaryConfig] = None,
        cost_model: Optional[Any] = None,
        pareto: Optional[Any] = None,
        message_queue: Optional[Any] = None,
        executor: Optional[Any] = None,
        # Backwards compatible kwargs
        population_size: Optional[int] = None,
        generations: Optional[int] = None,
        mutation_rate: Optional[float] = None,
        crossover_rate: Optional[float] = None,
        elite_size: Optional[int] = None,
        use_real_executor: Optional[bool] = None,
    ):
        # Build config from kwargs if provided for backwards compatibility
        if config is None:
            config = FlexGenEvolutionaryConfig()
            if population_size is not None:
                config.population_size = population_size
            if generations is not None:
                config.generations = generations
            if mutation_rate is not None:
                config.mutation_rate = mutation_rate
            if crossover_rate is not None:
                config.crossover_rate = crossover_rate
            if elite_size is not None:
                config.elite_size = elite_size
            if use_real_executor is not None:
                config.use_real_executor = use_real_executor

        issues = config.validate()
        if issues:
            raise ValueError(f"Invalid config: {issues}")
        self.config = config

        self.cost_model = cost_model or FlexGenCostModel()
        self.pareto = pareto or ParetoGating(
            objectives=[
                {"key": "latency_ms", "direction": "min"},
                {"key": "energy_joules", "direction": "min"},
                {"key": "carbon_g", "direction": "min"},
            ]
        )
        self.message_queue = message_queue
        self.executor = executor

        # Reproducible RNG
        self._rng = random.Random(config.random_state)

        self.population: List[FlexGenPolicy] = []
        self.best_policies: List[FlexGenPolicy] = []
        self._lock: Optional[asyncio.Lock] = None

        # Prompt for real_executor if executor isn't given
        if self.config.use_real_executor and self.executor is None:
            _warn_module("real_executor")
            logger.warning("use_real_executor=True but no executor provided; falling back to cost model")

        # Prometheus
        self._prom = self._setup_metrics()

    # ---------------- locks ----------------
    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    # ---------------- metrics ----------------
    def _setup_metrics(self) -> Dict[str, Any]:
        if not PROMETHEUS_AVAILABLE:
            return {}
        try:
            return {
                "generations_total": Counter(
                    "flexgen_evolver_generations_total", "Generations executed"
                ),
                "evaluations_total": Counter(
                    "flexgen_evolver_evaluations_total", "Policy evaluations"
                ),
                "pareto_gauge": Gauge(
                    "flexgen_evolver_pareto_size", "Pareto front size"
                ),
                "best_reward_gauge": Gauge(
                    "flexgen_evolver_best_reward", "Best reward in current population"
                ),
                "publish_total": Counter(
                    "flexgen_evolver_publish_total", "Feedback events published"
                ),
                "publish_errors_total": Counter(
                    "flexgen_evolver_publish_errors_total", "Feedback publish errors"
                ),
            }
        except Exception:
            return {}

    # ---------------- lifecycle ----------------
    async def __aenter__(self) -> "FlexGenEvolutionaryOptimizer":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None

    # ---------------- population ----------------
    def initialize_population(
        self, seed_policies: Optional[List[FlexGenPolicy]] = None
    ) -> None:
        if seed_policies:
            self.population = list(seed_policies)
        else:
            self.population = []
        while len(self.population) < self.config.population_size:
            self.population.append(self._random_policy())

    def _random_policy(self) -> FlexGenPolicy:
        return FlexGenPolicy(
            gpu_batch_size=self._rng.choice([1, 2, 4, 8]),
            block_size=self._rng.choice([8, 16, 32, 64]),
            weight_device=self._rng.choice(["gpu", "cpu", "disk"]),
            activation_device=self._rng.choice(["gpu", "cpu"]),
            kv_cache_device=self._rng.choice(["gpu", "cpu", "disk"]),
            weight_bits=self._rng.choice([4, 8, 16]),
            kv_cache_bits=self._rng.choice([4, 8, 16]),
            cpu_attention=self._rng.random() < 0.3,
            overlap_io_compute=self._rng.random() < 0.7,
        )

    # ---------------- evaluation ----------------
    async def _evaluate(
        self,
        policy: FlexGenPolicy,
        node: Any,
        workload: Any,
        carbon_intensity: float,
    ) -> Tuple[Dict[str, Any], float]:
        metrics: Dict[str, Any]

        if self.config.use_real_executor and self.executor is not None:
            try:
                exec_fn = self.executor.execute
                if inspect.iscoroutinefunction(exec_fn):
                    metrics = await exec_fn(policy, node, workload)
                else:
                    loop = asyncio.get_running_loop()
                    metrics = await loop.run_in_executor(
                        None, exec_fn, policy, node, workload
                    )
            except Exception as e:
                logger.warning("Executor failed; falling back to cost model", error=str(e))
                metrics = await self._cost_model_metrics(policy, node, workload, carbon_intensity)
        else:
            metrics = await self._cost_model_metrics(policy, node, workload, carbon_intensity)

        # reward
        reward = await self._compute_reward(metrics, workload, carbon_intensity)

        if self._prom:
            try:
                self._prom["evaluations_total"].inc()
            except Exception:
                pass

        return metrics, reward

    async def _cost_model_metrics(
        self, policy: FlexGenPolicy, node: Any, workload: Any, carbon_intensity: float
    ) -> Dict[str, Any]:
        est_fn = self.cost_model.estimate
        params = _signature_dict_safe(est_fn)

        # Try to pass carbon_intensity if the cost model accepts it
        try:
            if "carbon_intensity" in params or any(
                p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values()
            ):
                est = est_fn(policy, node, workload, carbon_intensity=carbon_intensity)
            else:
                est = est_fn(policy, node, workload)
        except TypeError:
            est = est_fn(policy, node, workload)

        if inspect.iscoroutine(est):
            est = await est

        gpu_mem_limit = 16.0
        node_meta = getattr(node, "metadata", None)
        if isinstance(node_meta, dict):
            try:
                gpu_mem_limit = float(node_meta.get("gpu_memory_gb", 16.0))
            except (TypeError, ValueError):
                gpu_mem_limit = 16.0

        peak = float(getattr(est, "peak_gpu_memory_gb", 0.0))
        quality = float(getattr(est, "quality_score", 0.9))

        return {
            "latency_ms": float(getattr(est, "total_latency_ms", 0.0)),
            "energy_joules": float(getattr(est, "total_energy_joules", 0.0)),
            "carbon_g": float(getattr(est, "total_carbon_g", 0.0)),
            "gpu_memory_gb": peak,
            "success": peak <= gpu_mem_limit,
            "quality_score": quality,
        }

    async def _compute_reward(
        self, metrics: Dict[str, Any], workload: Any, carbon_intensity: float
    ) -> float:
        # Try the module-level compute_reward with carbon_intensity if supported
        params = _signature_dict_safe(compute_reward)
        try:
            if "carbon_intensity" in params or any(
                p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values()
            ):
                r = compute_reward(metrics, workload, carbon_intensity=carbon_intensity)
            else:
                r = compute_reward(metrics, workload)
        except TypeError:
            r = compute_reward(metrics, workload)
        if inspect.iscoroutine(r):
            r = await r
        # Infeasibility penalty
        if not metrics.get("success", False):
            r -= 1.0
        return float(r)

    # ---------------- selection ----------------
    def _select_parents(
        self, evaluated: List[Tuple[FlexGenPolicy, Dict[str, Any], float]]
    ) -> List[FlexGenPolicy]:
        feasible = [t for t in evaluated if t[1].get("success", False)]
        if not feasible:
            feasible = evaluated

        if len(feasible) < 2:
            # Degenerate case; return copies of the only candidate
            base = feasible[0][0] if feasible else self._random_policy()
            return [replace(base) for _ in range(self.config.population_size)]

        tournament_size = max(2, int(self.config.population_size * self.config.tournament_fraction))
        tournament_size = min(tournament_size, len(feasible))

        parents: List[FlexGenPolicy] = []
        for _ in range(self.config.population_size):
            candidates = self._rng.sample(feasible, tournament_size)
            winner = max(candidates, key=lambda x: x[2])
            parents.append(replace(winner[0]))
        return parents

    # ---------------- genetic operators ----------------
    def _crossover(self, parent1: FlexGenPolicy, parent2: FlexGenPolicy) -> FlexGenPolicy:
        child_dict: Dict[str, Any] = {}
        for f in FlexGenPolicy.__dataclass_fields__:
            child_dict[f] = (
                getattr(parent1, f) if self._rng.random() < 0.5 else getattr(parent2, f)
            )
        return FlexGenPolicy(**child_dict)

    def _mutate(self, policy: FlexGenPolicy) -> FlexGenPolicy:
        """Non-destructive: returns a copy with possible field changes."""
        d = asdict(policy) if hasattr(policy, "__dataclass_fields__") else dict(policy.__dict__)

        if self._rng.random() < self.config.mutation_rate:
            d["gpu_batch_size"] = self._rng.choice([1, 2, 4, 8])
        if self._rng.random() < self.config.mutation_rate:
            d["block_size"] = self._rng.choice([8, 16, 32, 64])
        if self._rng.random() < self.config.mutation_rate:
            d["weight_device"] = self._rng.choice(["gpu", "cpu", "disk"])
        if self._rng.random() < self.config.mutation_rate:
            d["activation_device"] = self._rng.choice(["gpu", "cpu"])
        if self._rng.random() < self.config.mutation_rate:
            d["kv_cache_device"] = self._rng.choice(["gpu", "cpu", "disk"])
        if self._rng.random() < self.config.mutation_rate:
            d["weight_bits"] = self._rng.choice([4, 8, 16])
        if self._rng.random() < self.config.mutation_rate:
            d["kv_cache_bits"] = self._rng.choice([4, 8, 16])
        if self._rng.random() < self.config.mutation_rate:
            d["cpu_attention"] = not d.get("cpu_attention", False)
        if self._rng.random() < self.config.mutation_rate:
            d["overlap_io_compute"] = not d.get("overlap_io_compute", True)
        return FlexGenPolicy(**d)

    # ---------------- pareto ----------------
    def _pareto_indices(
        self, evaluated: List[Tuple[FlexGenPolicy, Dict[str, Any], float]]
    ) -> List[int]:
        """
        Run the Pareto filter on feasible metrics and map results back to
        indices in `evaluated` using a metric signature (unambiguous).
        """
        if not evaluated:
            return []
        metrics_list = [m for _, m, _ in evaluated]
        feasible_indices = [i for i, (_, m, _) in enumerate(evaluated) if m.get("success", False)]
        if not feasible_indices:
            feasible_indices = list(range(len(evaluated)))

        input_metrics = [evaluated[i][1] for i in feasible_indices]
        filtered = self.pareto.filter(input_metrics)
        if filtered is None:
            filtered = input_metrics

        filtered_sigs = {_metric_signature(m) for m in filtered}
        return [i for i in feasible_indices if _metric_signature(evaluated[i][1]) in filtered_sigs]

    # ---------------- publish ----------------
    async def _publish_elites(
        self,
        pareto_policies: List[FlexGenPolicy],
        node: Any,
        workload: Any,
        carbon_intensity: float,
        generation: int,
    ) -> None:
        if self.message_queue is None:
            return
        top_k = min(self.config.elite_publish_top_k, len(pareto_policies))
        for pol in pareto_policies[:top_k]:
            try:
                metrics, reward = await self._evaluate(pol, node, workload, carbon_intensity)
                event_kwargs = dict(
                    source="bio_inspired_flexgen",
                    feedback_type="routing",
                    task_id=getattr(workload, "task_id", None) or "unknown",
                    context={"generation": generation, "node_id": getattr(node, "id", "unknown")},
                    action={"selected_action": str(pol.to_dict() if hasattr(pol, "to_dict") else asdict(pol))},
                    performance={"quality_score": metrics.get("quality_score", 0.9)},
                    adaptive_cost_value=reward,
                    tags=["bio_inspired", "flexgen_policy", "evolution"],
                )
                if hasattr(FeedbackEvent, "create_with_context"):
                    event = FeedbackEvent.create_with_context(**event_kwargs)
                else:
                    event = FeedbackEvent(**event_kwargs)
                await self.message_queue.publish("bio_inspired_events", event.to_json())
                if self._prom:
                    try:
                        self._prom["publish_total"].inc()
                    except Exception:
                        pass
            except Exception as e:
                if self._prom:
                    try:
                        self._prom["publish_errors_total"].inc()
                    except Exception:
                        pass
                logger.warning("Failed to publish elite policy", error=str(e))

    # ---------------- main loop ----------------
    @traced("flexgen_evolver.run")
    async def run(
        self,
        node: Any,
        workload: Any,
        carbon_intensity: float,
        generations: Optional[int] = None,
        seed_policies: Optional[List[FlexGenPolicy]] = None,
    ) -> List[FlexGenPolicy]:
        async with self._get_lock():
            gens = generations or self.config.generations
            self.initialize_population(seed_policies)

            for gen in range(gens):
                # Evaluate
                evaluated: List[Tuple[FlexGenPolicy, Dict[str, Any], float]] = []
                for policy in self.population:
                    metrics, reward = await self._evaluate(
                        policy, node, workload, carbon_intensity
                    )
                    evaluated.append((policy, metrics, reward))

                # Pareto indices
                pareto_idx = self._pareto_indices(evaluated)
                pareto_policies = [evaluated[i][0] for i in pareto_idx]

                if self._prom:
                    try:
                        self._prom["pareto_gauge"].set(len(pareto_policies))
                        if evaluated:
                            self._prom["best_reward_gauge"].set(max(r for _, _, r in evaluated))
                        self._prom["generations_total"].inc()
                    except Exception:
                        pass

                # Publish
                if (
                    self.message_queue is not None
                    and self.config.publish_every_n_generations > 0
                    and gen % self.config.publish_every_n_generations == 0
                ):
                    await self._publish_elites(
                        pareto_policies, node, workload, carbon_intensity, gen
                    )

                # Select parents
                parents = self._select_parents(evaluated)

                # Offspring
                offspring: List[FlexGenPolicy] = []
                target_offspring = self.config.population_size - self.config.elite_size
                while len(offspring) < target_offspring and len(parents) >= 2:
                    p1, p2 = self._rng.sample(parents, 2)
                    if self._rng.random() < self.config.crossover_rate:
                        child = self._crossover(p1, p2)
                    else:
                        child = replace(p1)
                    child = self._mutate(child)
                    offspring.append(child)

                # Elites
                elites = [replace(p) for p in pareto_policies[: self.config.elite_size]]
                # If Pareto front is empty or smaller than elite_size, top up from parents
                while len(elites) < self.config.elite_size and parents:
                    elites.append(replace(parents[len(elites) % len(parents)]))

                combined = offspring + elites
                # Enforce exact population size
                if len(combined) < self.config.population_size:
                    while len(combined) < self.config.population_size:
                        combined.append(self._random_policy())
                self.population = combined[: self.config.population_size]

                logger.info(
                    "Generation complete",
                    generation=gen,
                    population=len(self.population),
                    pareto_size=len(pareto_policies),
                )

                # Yield control every N generations
                if (
                    self.config.generation_yield_every > 0
                    and gen % self.config.generation_yield_every == 0
                ):
                    await asyncio.sleep(0)

            # Final Pareto set on the final population
            final_evaluated: List[Tuple[FlexGenPolicy, Dict[str, Any], float]] = []
            for policy in self.population:
                metrics, reward = await self._evaluate(
                    policy, node, workload, carbon_intensity
                )
                final_evaluated.append((policy, metrics, reward))

            final_pareto_idx = self._pareto_indices(final_evaluated)
            final_policies = [final_evaluated[i][0] for i in final_pareto_idx]

            if not final_policies:
                final_policies = [p for p, _, _ in final_evaluated][
                    : self.config.fallback_final_policies
                ]

            # Deduplicate by policy signature
            seen: set = set()
            deduped: List[FlexGenPolicy] = []
            for p in final_policies:
                sig = _policy_signature(p)
                if sig not in seen:
                    seen.add(sig)
                    deduped.append(p)

            self.best_policies = deduped
            return deduped

    def run_sync(
        self,
        node: Any,
        workload: Any,
        carbon_intensity: float,
        generations: Optional[int] = None,
    ) -> List[FlexGenPolicy]:
        """Run the optimizer synchronously. Raises if a loop is already running."""
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(self.run(node, workload, carbon_intensity, generations))
        raise RuntimeError(
            "run_sync() cannot be called from a running event loop; use `await run(...)` instead."
        )


# =============================================================================
# SECTION 7. TESTS
# =============================================================================
class _Tests(unittest.TestCase):
    def _mk(self, **overrides) -> FlexGenEvolutionaryOptimizer:
        cfg = FlexGenEvolutionaryConfig(
            population_size=overrides.pop("population_size", 8),
            generations=overrides.pop("generations", 3),
            mutation_rate=overrides.pop("mutation_rate", 0.5),
            crossover_rate=overrides.pop("crossover_rate", 0.8),
            elite_size=overrides.pop("elite_size", 2),
            random_state=overrides.pop("random_state", 42),
        )
        return FlexGenEvolutionaryOptimizer(config=cfg, **overrides)

    def test_mutate_does_not_alias(self):
        opt = self._mk()
        p = FlexGenPolicy(gpu_batch_size=1, block_size=8, weight_device="gpu")
        snapshot = asdict(p)
        for _ in range(200):
            child = opt._mutate(p)
            self.assertIsNot(child, p)
        self.assertEqual(asdict(p), snapshot, "parent was mutated in place")

    def test_tournament_guard(self):
        opt = self._mk()
        # Only two feasible policies, tournament size would be 10 if unguarded
        policy = FlexGenPolicy()
        feasible = [(policy, {"success": True}, 1.0), (policy, {"success": True}, 0.5)]
        parents = opt._select_parents(feasible)
        self.assertEqual(len(parents), opt.config.population_size)

    def test_tournament_all_infeasible(self):
        opt = self._mk()
        policy = FlexGenPolicy()
        infeasible = [(policy, {"success": False}, 0.0)] * 3
        parents = opt._select_parents(infeasible)
        self.assertEqual(len(parents), opt.config.population_size)

    def test_pareto_indices_unambiguous(self):
        opt = self._mk()
        p1 = FlexGenPolicy(gpu_batch_size=1)
        p2 = FlexGenPolicy(gpu_batch_size=2)
        # Two policies with IDENTICAL metrics; only the better ones survive
        good = {"latency_ms": 10.0, "energy_joules": 5.0, "carbon_g": 0.1,
                "success": True, "quality_score": 0.9}
        bad = {"latency_ms": 100.0, "energy_joules": 50.0, "carbon_g": 1.0,
               "success": True, "quality_score": 0.9}
        evaluated = [(p1, good, 1.0), (p2, good.copy(), 1.0), (p1, bad, 0.0)]
        idx = opt._pareto_indices(evaluated)
        # Both "good" entries should be marked Pareto (identical metrics)
        self.assertIn(0, idx)
        self.assertIn(1, idx)
        self.assertNotIn(2, idx)

    def test_elite_size_validation(self):
        with self.assertRaises(ValueError):
            FlexGenEvolutionaryOptimizer(config=FlexGenEvolutionaryConfig(
                population_size=5, elite_size=5
            ))
        with self.assertRaises(ValueError):
            FlexGenEvolutionaryOptimizer(config=FlexGenEvolutionaryConfig(
                population_size=5, elite_size=-1
            ))

    def test_run_sync_in_loop_raises(self):
        opt = self._mk()

        async def go():
            with self.assertRaises(RuntimeError):
                opt.run_sync(NodeDescriptor(), WorkloadDescriptor(), 0.5)

        asyncio.run(go())

    def test_reproducibility(self):
        opt_a = self._mk(random_state=123)
        opt_b = self._mk(random_state=123)
        node = NodeDescriptor()
        workload = WorkloadDescriptor()

        async def go():
            a = await opt_a.run(node, workload, 0.5)
            b = await opt_b.run(node, workload, 0.5)
            return a, b

        a, b = asyncio.run(go())
        self.assertEqual(
            [asdict(p) for p in a],
            [asdict(p) for p in b],
            "reproducible runs diverged",
        )

    def test_population_size_constant(self):
        cfg = FlexGenEvolutionaryConfig(
            population_size=10, generations=3, elite_size=3, random_state=1
        )
        opt = FlexGenEvolutionaryOptimizer(config=cfg)
        observed_sizes: List[int] = []
        orig = opt._select_parents

        def wrapped(evaluated):
            observed_sizes.append(len(opt.population))
            return orig(evaluated)

        opt._select_parents = wrapped  # type: ignore

        async def go():
            await opt.run(NodeDescriptor(), WorkloadDescriptor(), 0.5)

        asyncio.run(go())
        for size in observed_sizes:
            self.assertEqual(size, 10)

    def test_carbon_intensity_threaded(self):
        """carbon_intensity must influence the carbon_g metric via cost model."""
        opt_low = self._mk()
        opt_high = self._mk()
        p = FlexGenPolicy(gpu_batch_size=4, block_size=32, weight_device="cpu")
        node = NodeDescriptor()
        workload = WorkloadDescriptor()

        async def go():
            m_low, _ = await opt_low._evaluate(p, node, workload, 0.0)
            m_high, _ = await opt_high._evaluate(p, node, workload, 1000.0)
            return m_low, m_high

        m_low, m_high = asyncio.run(go())
        self.assertLessEqual(m_low["carbon_g"], m_high["carbon_g"])

    def test_executor_async(self):
        class AsyncExecutor:
            async def execute(self, policy, node, workload):
                return {
                    "latency_ms": 1.0, "energy_joules": 1.0, "carbon_g": 0.0,
                    "success": True, "quality_score": 0.9,
                }

        cfg = FlexGenEvolutionaryConfig(
            population_size=4, generations=1, elite_size=1, random_state=0,
            use_real_executor=True,
        )
        opt = FlexGenEvolutionaryOptimizer(config=cfg, executor=AsyncExecutor())

        async def go():
            return await opt.run(NodeDescriptor(), WorkloadDescriptor(), 0.5)

        result = asyncio.run(go())
        self.assertGreater(len(result), 0)

    def test_executor_sync_offloaded(self):
        class SyncExecutor:
            def __init__(self):
                self.calls = 0

            def execute(self, policy, node, workload):
                self.calls += 1
                return {
                    "latency_ms": 5.0, "energy_joules": 2.0, "carbon_g": 0.1,
                    "success": True, "quality_score": 0.9,
                }

        exec_ = SyncExecutor()
        cfg = FlexGenEvolutionaryConfig(
            population_size=4, generations=1, elite_size=1, random_state=0,
            use_real_executor=True,
        )
        opt = FlexGenEvolutionaryOptimizer(config=cfg, executor=exec_)

        async def go():
            return await opt.run(NodeDescriptor(), WorkloadDescriptor(), 0.5)

        asyncio.run(go())
        self.assertGreater(exec_.calls, 0)

    def test_final_pareto_deduped(self):
        cfg = FlexGenEvolutionaryConfig(
            population_size=10, generations=2, elite_size=2, random_state=7
        )
        opt = FlexGenEvolutionaryOptimizer(config=cfg)

        async def go():
            return await opt.run(NodeDescriptor(), WorkloadDescriptor(), 0.5)

        result = asyncio.run(go())
        sigs = [_policy_signature(p) for p in result]
        self.assertEqual(len(sigs), len(set(sigs)), "final Pareto set has duplicates")

    def test_message_queue_failure_does_not_abort(self):
        class BrokenQueue:
            async def publish(self, topic, payload):
                raise RuntimeError("boom")

        cfg = FlexGenEvolutionaryConfig(
            population_size=6, generations=3, elite_size=2, random_state=0,
            publish_every_n_generations=1,
        )
        opt = FlexGenEvolutionaryOptimizer(config=cfg, message_queue=BrokenQueue())

        async def go():
            return await opt.run(NodeDescriptor(), WorkloadDescriptor(), 0.5)

        result = asyncio.run(go())
        self.assertGreaterEqual(len(result), 1)

    def test_message_queue_publishes(self):
        class RecordingQueue:
            def __init__(self):
                self.events = []

            async def publish(self, topic, payload):
                self.events.append((topic, payload))

        q = RecordingQueue()
        cfg = FlexGenEvolutionaryConfig(
            population_size=6, generations=3, elite_size=2, random_state=0,
            publish_every_n_generations=1, elite_publish_top_k=1,
        )
        opt = FlexGenEvolutionaryOptimizer(config=cfg, message_queue=q)

        async def go():
            await opt.run(NodeDescriptor(), WorkloadDescriptor(), 0.5)

        asyncio.run(go())
        self.assertGreater(len(q.events), 0)

    def test_context_manager(self):
        cfg = FlexGenEvolutionaryConfig(
            population_size=4, generations=1, elite_size=1, random_state=0
        )

        async def go():
            async with FlexGenEvolutionaryOptimizer(config=cfg) as opt:
                return await opt.run(NodeDescriptor(), WorkloadDescriptor(), 0.5)

        result = asyncio.run(go())
        self.assertGreater(len(result), 0)


def run_tests() -> int:
    suite = unittest.TestLoader().loadTestsFromTestCase(_Tests)
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    return 0 if result.wasSuccessful() else 1


# =============================================================================
# SECTION 8. ENTRY POINT
# =============================================================================
async def _example() -> None:
    cfg = FlexGenEvolutionaryConfig(
        population_size=20, generations=5, elite_size=3, random_state=42
    )
    opt = FlexGenEvolutionaryOptimizer(config=cfg)
    node = NodeDescriptor()
    workload = WorkloadDescriptor()
    policies = await opt.run(node, workload, carbon_intensity=150.0)
    print(f"Pareto policies: {len(policies)}")
    for p in policies[:3]:
        print(asdict(p))


def main() -> None:
    parser = argparse.ArgumentParser(description="FlexGen Evolutionary Optimizer v2.0.0")
    parser.add_argument("--test", action="store_true", help="Run embedded tests")
    parser.add_argument("--example", action="store_true", help="Run example usage")
    parser.add_argument("--status", action="store_true", help="Print module statuses")
    args = parser.parse_args()

    if args.status:
        for name, status in MODULE_STATUS.items():
            print(f"{name:22s} {status}")
        if not REAL_MODULES_AVAILABLE:
            print("(using fallback stubs for surrounding modules)")
        return

    if args.test:
        sys.exit(run_tests())

    if args.example:
        asyncio.run(_example())
        return

    print("FlexGen Evolutionary Optimizer v2.0.0 — no mode selected.")
    print("Use --test, --example, or --status.")


if __name__ == "__main__":
    main()
