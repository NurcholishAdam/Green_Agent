#!/usr/bin/env python3
"""
Evolutionary policy search for FlexGen policies (Enhanced v2.5.0).
==================================================================
Uses a genetic algorithm with crossover, elitism, and a scalar reward to
evolve candidate policies, evaluated via cost model or real execution.
Integrates with Green Agent modules (ParetoGating, AsyncMessageQueue,
FeedbackEvent, reward computation) for closed-loop sustainability-aware
orchestration.

FIXES OVER v2.0:
- Relative imports wrapped with fallbacks; module imports cleanly even when
  optional siblings are missing.
- `run_sync()` refuses to run inside a live event loop.
- `_evaluate` is async and supports sync AND async executors.
- Policy evaluation wrapped in try/except; failures marked infeasible.
- `_publish_event` is fire-and-forget; a `_drain_publishes` step ensures
  pending events flush before `run()` returns.
- `carbon_intensity` is used in the reward (was previously unused).
- The returned Pareto front always includes `best_policy`.
- Duplicate policies in the returned front are removed.
- Evaluations are cached per policy, so elites are not re-evaluated.
- `generation_history` is bounded.
- Per-instance RNGs seeded from `seed`; global `random`/`np.random` untouched.
- Convergence check (`convergence_patience` + `convergence_min_delta`).
- Drift detection can also boost mutation.
- `.get()` semantics throughout; missing metric keys are defaulted.
- Rewards serialized into `FeedbackEvent.performance` are JSON-safe floats.
- Optional Prometheus metrics.
- `get_stats()` and `get_pareto_front()` helpers added.
"""

from __future__ import annotations

import asyncio
import copy
import json
import logging
import math
import random
import time
import uuid
from collections import deque
from dataclasses import dataclass, replace as dataclass_replace
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

import numpy as np

# ---------- Prometheus ----------
try:
    from prometheus_client import Counter as _PromCounter, Gauge, Histogram
    PROMETHEUS_AVAILABLE = True
except ImportError:  # pragma: no cover
    PROMETHEUS_AVAILABLE = False

# ---------- structlog / stdlib logger ----------
try:
    import structlog
    _logger = structlog.get_logger(__name__)
    _STRUCTLOG_AVAILABLE = True
except ImportError:  # pragma: no cover
    _logger = logging.getLogger(__name__)
    _STRUCTLOG_AVAILABLE = False
    logging.basicConfig(level=logging.INFO)


def log_event(level: str, message: str, **kwargs: Any) -> None:
    """Logging shim that works with structlog or stdlib logging."""
    if _STRUCTLOG_AVAILABLE:
        getattr(_logger, level)(message, **kwargs)
    else:
        if kwargs:
            extra = " ".join(f"{k}={v!r}" for k, v in kwargs.items())
            message = f"{message} | {extra}"
        getattr(_logger, level)(message)


# ---------- Relative imports with fallbacks ----------
try:
    from .flexgen_policy import FlexGenPolicy  # type: ignore
except ImportError:  # pragma: no cover
    FlexGenPolicy = Any  # type: ignore

try:
    from .flexgen_cost_model import FlexGenCostModel  # type: ignore
except ImportError:  # pragma: no cover
    FlexGenCostModel = Any  # type: ignore

try:
    from ..schemas.node_descriptor import NodeDescriptor  # type: ignore
except ImportError:  # pragma: no cover
    NodeDescriptor = Any  # type: ignore

try:
    from ..schemas.workload_descriptor import WorkloadDescriptor  # type: ignore
except ImportError:  # pragma: no cover
    WorkloadDescriptor = Any  # type: ignore

try:
    from ..pareto_gating import ParetoGating  # type: ignore
except ImportError:  # pragma: no cover
    class ParetoGating:  # type: ignore
        """Fallback that performs no filtering."""
        def __init__(self, objectives: Optional[List[Dict[str, Any]]] = None):
            self.objectives = objectives or []

        def filter(self, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
            return list(items)

try:
    from ..async_message_queue import AsyncMessageQueue  # type: ignore
except ImportError:  # pragma: no cover
    AsyncMessageQueue = Any  # type: ignore

try:
    from ..schemas.feedback_event import FeedbackEvent  # type: ignore
except ImportError:  # pragma: no cover
    FeedbackEvent = Any  # type: ignore

try:
    from ..logger import logger  # type: ignore
except ImportError:  # pragma: no cover
    logger = _logger  # type: ignore


# ---------- Reward ----------
try:
    from ..gpu_optimization.reward import compute_reward  # type: ignore
except ImportError:  # pragma: no cover
    def compute_reward(metrics: Dict[str, Any], workload: Any) -> float:
        """Fallback reward: quality, latency satisfaction, energy, carbon, memory."""
        weights = {
            "quality": 0.3,
            "throughput": 0.25,
            "energy": 0.2,
            "carbon": 0.15,
            "memory": 0.1,
        }
        latency_target = max(float(getattr(workload, "latency_target", 1.0) or 1.0), 1.0)
        latency_ms = float(metrics.get("latency_ms", latency_target) or latency_target)
        energy_joules = float(metrics.get("energy_joules", 100.0) or 100.0)
        carbon_g = float(metrics.get("carbon_g", 10.0) or 10.0)

        latency_score = max(0.0, 1.0 - latency_ms / latency_target)
        energy_score = max(0.0, 1.0 - energy_joules / 100.0)
        carbon_score = max(0.0, 1.0 - carbon_g / 10.0)
        memory_score = 1.0 if metrics.get("success", True) else 0.0
        quality = float(metrics.get("quality_score", 0.9))

        reward = (
            weights["quality"] * quality
            + weights["throughput"] * latency_score
            + weights["energy"] * energy_score
            + weights["carbon"] * carbon_score
            + weights["memory"] * memory_score
        )
        return max(0.0, min(1.0, reward))


# ==============================================================================
# BioPolicySearch
# ==============================================================================
class BioPolicySearch:
    """
    Enhanced evolutionary optimizer for FlexGen policies.

    Features:
    - Crossover and elitism.
    - Evaluation via cost model or real/mock executor (sync or async).
    - Scalar reward for selection pressure, carbon-intensity-aware.
    - FeedbackEvent emission via AsyncMessageQueue (non-blocking).
    - Population diversity monitoring and drift detection.
    - Adaptive mutation rate.
    - Infeasible policy handling.
    - Per-instance RNG for reproducibility.
    """

    # Search-space bounds used by `_random_policy` and `_mutate`.
    GPU_BATCH_SIZES: Tuple[int, ...] = (1, 2, 4, 8)
    BLOCK_SIZES: Tuple[int, ...] = (8, 16, 32, 64)
    DEVICE_CHOICES: Tuple[str, ...] = ("gpu", "cpu", "disk")
    ACTIVATION_DEVICES: Tuple[str, ...] = ("gpu", "cpu")
    PRECISIONS: Tuple[int, ...] = (4, 8, 16)

    def __init__(
        self,
        node: NodeDescriptor,
        workload: WorkloadDescriptor,
        cost_model: FlexGenCostModel,
        population_size: int = 50,
        generations: int = 10,
        mutation_rate: float = 0.2,
        crossover_rate: float = 0.8,
        elite_size: int = 5,
        use_real_executor: bool = False,
        executor: Optional[Callable] = None,
        carbon_intensity: float = 400.0,
        message_queue: Optional[AsyncMessageQueue] = None,
        drift_threshold: float = 0.3,
        diversity_threshold: float = 0.05,
        *,
        seed: Optional[int] = None,
        reward_fn: Optional[Callable[[Dict[str, Any], Any], float]] = None,
        convergence_patience: int = 5,
        convergence_min_delta: float = 1e-4,
        generation_history_max: int = 1000,
        enable_prometheus: bool = True,
    ):
        self.node = node
        self.workload = workload
        self.cost_model = cost_model
        self.population_size = int(population_size)
        self.generations = int(generations)
        self.mutation_rate = float(mutation_rate)
        self.crossover_rate = float(crossover_rate)
        self.elite_size = min(int(elite_size), self.population_size)
        self.use_real_executor = bool(use_real_executor)
        self.executor = executor
        self.carbon_intensity = float(carbon_intensity)
        self.message_queue = message_queue
        self.drift_threshold = float(drift_threshold)
        self.diversity_threshold = float(diversity_threshold)

        self._reward_fn = reward_fn
        self.convergence_patience = int(convergence_patience)
        self.convergence_min_delta = float(convergence_min_delta)
        self._generation_history_max = max(1, int(generation_history_max))

        # Per-instance RNG (never touches global state).
        self._seed = seed if seed is not None else random.randint(0, 2**31 - 1)
        self._rng = random.Random(self._seed)
        self._np_rng = np.random.default_rng(self._seed)

        # Public state (preserved from v2.0).
        self.population: List[Any] = []
        self.pareto = ParetoGating(objectives=[
            {"key": "latency_ms", "direction": "min"},
            {"key": "energy_joules", "direction": "min"},
            {"key": "carbon_g", "direction": "min"},
        ])
        self.best_policy: Optional[Any] = None
        self.best_reward: float = -float("inf")
        self.generation_history: List[List[float]] = []

        # Internal caches / bookkeeping.
        self._eval_cache: Dict[str, Tuple[Dict[str, Any], float]] = {}
        self._pending_publish_tasks: set = set()
        self._node_gpu_memory_gb = self._extract_gpu_memory_gb(node)

        # Optional Prometheus metrics.
        self.metrics: Optional[Dict[str, Any]] = None
        if PROMETHEUS_AVAILABLE and enable_prometheus:
            self.metrics = {
                "evaluations": _PromCounter(
                    "bio_policy_search_evaluations_total",
                    "Policy evaluations",
                ),
                "evaluation_failures": _PromCounter(
                    "bio_policy_search_evaluation_failures_total",
                    "Policy evaluation failures",
                ),
                "evaluation_seconds": Histogram(
                    "bio_policy_search_evaluation_seconds",
                    "Policy evaluation latency",
                ),
                "best_reward": Gauge(
                    "bio_policy_search_best_reward",
                    "Best reward seen so far",
                ),
                "diversity": Gauge(
                    "bio_policy_search_diversity",
                    "Population diversity (mean pairwise distance)",
                ),
                "pareto_size": Gauge(
                    "bio_policy_search_pareto_size",
                    "Pareto front size",
                ),
                "drift_events": _PromCounter(
                    "bio_policy_search_drift_events_total",
                    "Detected population drift events",
                ),
                "publish_failures": _PromCounter(
                    "bio_policy_search_publish_failures_total",
                    "Failed FeedbackEvent publishes",
                ),
            }

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _extract_gpu_memory_gb(node: Any) -> float:
        md = getattr(node, "metadata", None)
        if md is None:
            return 16.0
        if isinstance(md, dict):
            try:
                return float(md.get("gpu_memory_gb", 16.0))
            except (TypeError, ValueError):
                return 16.0
        try:
            return float(getattr(md, "gpu_memory_gb", 16.0))
        except (TypeError, ValueError):
            return 16.0

    def _policy_cache_key(self, policy: Any) -> str:
        """Stable, hashable key for a policy."""
        try:
            d = policy.to_dict()
        except Exception:
            d = getattr(policy, "__dict__", {})
        try:
            return json.dumps(d, sort_keys=True, default=str)
        except Exception:
            return repr(d)

    def _clone_policy(self, policy: Any) -> Any:
        try:
            return dataclass_replace(policy)
        except Exception:
            try:
                return copy.deepcopy(policy)
            except Exception:
                return policy

    @staticmethod
    def _same_policy(a: Any, b: Any) -> bool:
        try:
            return a == b
        except Exception:
            return False

    @staticmethod
    def _metrics_hashable(metrics: Dict[str, Any]) -> Tuple:
        items = []
        for k in sorted(metrics.keys()):
            v = metrics[k]
            if isinstance(v, (int, float, str, bool)) or v is None:
                items.append((k, v))
            else:
                items.append((k, str(v)))
        return tuple(items)

    # ------------------------------------------------------------------
    # search space
    # ------------------------------------------------------------------
    def _random_policy(self) -> Any:
        return FlexGenPolicy(
            gpu_batch_size=self._rng.choice(self.GPU_BATCH_SIZES),
            block_size=self._rng.choice(self.BLOCK_SIZES),
            weight_device=self._rng.choice(self.DEVICE_CHOICES),
            activation_device=self._rng.choice(self.ACTIVATION_DEVICES),
            kv_cache_device=self._rng.choice(self.DEVICE_CHOICES),
            weight_bits=self._rng.choice(self.PRECISIONS),
            kv_cache_bits=self._rng.choice(self.PRECISIONS),
            cpu_attention=self._rng.random() < 0.3,
            overlap_io_compute=self._rng.random() < 0.7,
        )

    def _crossover(self, parent1: Any, parent2: Any) -> Any:
        child_dict: Dict[str, Any] = {}
        for field_name in FlexGenPolicy.__dataclass_fields__:
            if self._rng.random() < 0.5:
                child_dict[field_name] = getattr(parent1, field_name)
            else:
                child_dict[field_name] = getattr(parent2, field_name)
        return FlexGenPolicy(**child_dict)

    def _mutate(self, policy: Any, mutation_rate: Optional[float] = None) -> Any:
        rate = self.mutation_rate if mutation_rate is None else mutation_rate
        new_policy = self._clone_policy(policy)

        if self._rng.random() < rate:
            new_policy.gpu_batch_size = self._rng.choice(self.GPU_BATCH_SIZES)
        if self._rng.random() < rate:
            new_policy.block_size = self._rng.choice(self.BLOCK_SIZES)
        if self._rng.random() < rate:
            new_policy.weight_device = self._rng.choice(self.DEVICE_CHOICES)
        if self._rng.random() < rate:
            new_policy.activation_device = self._rng.choice(self.ACTIVATION_DEVICES)
        if self._rng.random() < rate:
            new_policy.kv_cache_device = self._rng.choice(self.DEVICE_CHOICES)
        if self._rng.random() < rate:
            new_policy.weight_bits = self._rng.choice(self.PRECISIONS)
        if self._rng.random() < rate:
            new_policy.kv_cache_bits = self._rng.choice(self.PRECISIONS)
        if self._rng.random() < rate:
            new_policy.cpu_attention = not getattr(new_policy, "cpu_attention", False)
        if self._rng.random() < rate:
            new_policy.overlap_io_compute = not getattr(
                new_policy, "overlap_io_compute", True
            )
        return new_policy

    # ------------------------------------------------------------------
    # evaluation
    # ------------------------------------------------------------------
    async def _evaluate(self, policy: Any) -> Tuple[Dict[str, Any], float]:
        """Evaluate a policy. Never raises — failures are marked infeasible."""
        key = self._policy_cache_key(policy)
        if key in self._eval_cache:
            return self._eval_cache[key]

        start = time.time()
        try:
            if self.use_real_executor and self.executor is not None:
                result = self.executor(policy, self.node, self.workload)
                if asyncio.iscoroutine(result):
                    metrics = await result
                else:
                    metrics = result
            else:
                metrics = await asyncio.to_thread(self._estimate_sync, policy)

            if not isinstance(metrics, dict):
                raise ValueError(f"Executor returned non-dict: {type(metrics)}")

            # Ensure required keys exist.
            metrics.setdefault("latency_ms", float("inf"))
            metrics.setdefault("energy_joules", float("inf"))
            metrics.setdefault("carbon_g", float("inf"))
            metrics.setdefault("gpu_memory_gb", 0.0)
            metrics.setdefault("quality_score", 0.9)
            if "success" not in metrics:
                try:
                    metrics["success"] = (
                        float(metrics["gpu_memory_gb"]) <= self._node_gpu_memory_gb
                    )
                except (TypeError, ValueError):
                    metrics["success"] = False

            reward = self._compute_reward(metrics)
        except Exception as exc:
            log_event("warning", f"Policy evaluation failed: {exc}")
            metrics = {
                "latency_ms": float("inf"),
                "energy_joules": float("inf"),
                "carbon_g": float("inf"),
                "gpu_memory_gb": float("inf"),
                "quality_score": 0.0,
                "success": False,
                "error": str(exc),
            }
            reward = 0.0
            if self.metrics:
                try:
                    self.metrics["evaluation_failures"].inc()
                except Exception:
                    pass

        elapsed = time.time() - start
        if self.metrics:
            try:
                self.metrics["evaluations"].inc()
                self.metrics["evaluation_seconds"].observe(elapsed)
            except Exception:
                pass

        self._eval_cache[key] = (metrics, reward)
        return metrics, reward

    def _estimate_sync(self, policy: Any) -> Dict[str, Any]:
        """Synchronous wrapper around the cost model."""
        est = self.cost_model.estimate(policy, self.node, self.workload)
        return {
            "latency_ms": float(getattr(est, "total_latency_ms", float("inf"))),
            "energy_joules": float(getattr(est, "total_energy_joules", float("inf"))),
            "carbon_g": float(getattr(est, "total_carbon_g", float("inf"))),
            "gpu_memory_gb": float(getattr(est, "peak_gpu_memory_gb", float("inf"))),
            "quality_score": 0.9,
        }

    def _compute_reward(self, metrics: Dict[str, Any]) -> float:
        """Compute the scalar reward with a carbon-intensity-aware adjustment."""
        if self._reward_fn is not None:
            try:
                base = float(self._reward_fn(metrics, self.workload))
            except Exception as exc:
                log_event("warning", f"Custom reward_fn failed: {exc}")
                base = float(compute_reward(metrics, self.workload))
        else:
            base = float(compute_reward(metrics, self.workload))

        # Carbon-aware scaling: high grid intensity penalizes carbon-heavy runs.
        try:
            carbon_g = float(metrics.get("carbon_g", 0.0) or 0.0)
            if carbon_g > 0 and self.carbon_intensity > 0:
                intensity_factor = min(2.0, self.carbon_intensity / 400.0)
                carbon_penalty = min(0.3, carbon_g / 100.0) * intensity_factor
                base = base * (1.0 - carbon_penalty)
        except (TypeError, ValueError):
            pass

        return max(0.0, min(1.0, base))

    # ------------------------------------------------------------------
    # selection
    # ------------------------------------------------------------------
    def _select_parents(
        self,
        evaluated: List[Tuple[Any, Dict[str, Any], float]],
    ) -> List[Any]:
        if not evaluated:
            return []
        tournament_size = min(
            max(2, int(self.population_size * 0.1)),
            len(evaluated),
        )
        parents: List[Any] = []
        for _ in range(self.population_size):
            candidates = self._rng.sample(evaluated, tournament_size)
            winner = max(candidates, key=lambda x: x[2])
            parents.append(winner[0])
        return parents

    # ------------------------------------------------------------------
    # diversity / drift
    # ------------------------------------------------------------------
    def _policy_to_vector(self, policy: Any) -> List[float]:
        return [
            getattr(policy, "gpu_batch_size", 0) / 8.0,
            getattr(policy, "block_size", 0) / 64.0,
            1.0 if getattr(policy, "weight_device", "") == "gpu" else 0.0,
            1.0 if getattr(policy, "weight_device", "") == "cpu" else 0.0,
            1.0 if getattr(policy, "activation_device", "") == "gpu" else 0.0,
            1.0 if getattr(policy, "kv_cache_device", "") == "gpu" else 0.0,
            1.0 if getattr(policy, "kv_cache_device", "") == "cpu" else 0.0,
            getattr(policy, "weight_bits", 0) / 16.0,
            getattr(policy, "kv_cache_bits", 0) / 16.0,
            1.0 if getattr(policy, "cpu_attention", False) else 0.0,
            1.0 if getattr(policy, "overlap_io_compute", False) else 0.0,
        ]

    def _compute_diversity(self) -> float:
        if len(self.population) < 2:
            return 1.0
        vectors = np.asarray(
            [self._policy_to_vector(p) for p in self.population],
            dtype=np.float32,
        )
        n = len(vectors)
        if n > 200:
            # Sampled pairwise distances — O(n).
            idx = self._np_rng.integers(0, n, size=(n, 2))
            diffs = vectors[idx[:, 0]] - vectors[idx[:, 1]]
            dists = np.linalg.norm(diffs, axis=1)
        else:
            diffs = vectors[:, None, :] - vectors[None, :, :]
            dists_matrix = np.linalg.norm(diffs, axis=2)
            iu = np.triu_indices(n, k=1)
            dists = dists_matrix[iu]
        if dists.size == 0:
            return 0.0
        return float(np.mean(dists))

    def _detect_drift(self, new_population: List[Any]) -> bool:
        """Compare the centroid of `new_population` against the last stored centroid."""
        if not self.generation_history or not new_population:
            return False
        prev_vec = np.asarray(self.generation_history[-1], dtype=np.float32)
        curr_vec = np.mean(
            np.asarray(
                [self._policy_to_vector(p) for p in new_population],
                dtype=np.float32,
            ),
            axis=0,
        )
        dist = float(np.linalg.norm(curr_vec - prev_vec))
        return dist > self.drift_threshold

    # ------------------------------------------------------------------
    # Pareto
    # ------------------------------------------------------------------
    def _pareto_filter(
        self,
        evaluated: List[Tuple[Any, Dict[str, Any], float]],
    ) -> List[Any]:
        """Return Pareto-optimal policies (deduplicated, mapped back via metrics hash)."""
        if not evaluated:
            return []

        metric_key_to_policy: Dict[Tuple, Any] = {}
        for policy, metrics, _ in evaluated:
            k = self._metrics_hashable(metrics)
            metric_key_to_policy.setdefault(k, policy)

        successful = [m for _, m, _ in evaluated if m.get("success", False)]
        if not successful:
            successful = [m for _, m, _ in evaluated]
        if not successful:
            return []

        try:
            filtered = list(self.pareto.filter(successful))
        except Exception as exc:
            log_event("warning", f"Pareto filter failed: {exc}")
            filtered = successful

        policies: List[Any] = []
        seen = set()
        for m in filtered:
            k = self._metrics_hashable(m)
            p = metric_key_to_policy.get(k)
            if p is None:
                continue
            pk = self._policy_cache_key(p)
            if pk in seen:
                continue
            seen.add(pk)
            policies.append(p)
        return policies

    # ------------------------------------------------------------------
    # feedback publishing
    # ------------------------------------------------------------------
    async def _publish_event(
        self,
        policy: Any,
        metrics: Dict[str, Any],
        reward: float,
        generation: int,
    ) -> None:
        if not self.message_queue:
            return
        try:
            event = FeedbackEvent(
                source="bio_policy_search",
                feedback_type="routing",
                task_id=getattr(self.workload, "task_id", None) or None,
                context={
                    "generation": int(generation),
                    "node_id": getattr(self.node, "id", None),
                    "carbon_intensity": float(self.carbon_intensity),
                    "population_size": int(self.population_size),
                    "seed": int(self._seed),
                },
                action={
                    "selected_action": str(policy.to_dict()),
                    "selected_rank": int(generation),
                    "confidence_score": 0.5,
                },
                performance={
                    "quality_score": float(metrics.get("quality_score", 0.9)),
                    "latency_ms": float(metrics.get("latency_ms", 0.0) or 0.0),
                    "energy_joules": float(metrics.get("energy_joules", 0.0) or 0.0),
                    "carbon_g": float(metrics.get("carbon_g", 0.0) or 0.0),
                    "helium_cost": 0.0,
                    "duration_ms": 0.0,
                },
                adaptive_cost_value=float(reward),
                tags=["bio_inspired", "flexgen_policy", "evolution"],
            )
            await self.message_queue.publish("bio_inspired_events", event.to_json())
        except Exception as exc:
            log_event("warning", f"Failed to publish FeedbackEvent: {exc}")
            if self.metrics:
                try:
                    self.metrics["publish_failures"].inc()
                except Exception:
                    pass

    def _spawn_publish(
        self,
        policy: Any,
        metrics: Dict[str, Any],
        reward: float,
        generation: int,
    ) -> None:
        """Fire-and-forget publish; tracked so we can drain before returning."""
        if not self.message_queue:
            return
        try:
            task = asyncio.create_task(
                self._publish_event(policy, metrics, reward, generation)
            )
        except RuntimeError:
            return
        self._pending_publish_tasks.add(task)
        task.add_done_callback(self._pending_publish_tasks.discard)

    async def _drain_publishes(self) -> None:
        if not self._pending_publish_tasks:
            return
        pending = list(self._pending_publish_tasks)
        await asyncio.gather(*pending, return_exceptions=True)
        self._pending_publish_tasks.clear()

    # ------------------------------------------------------------------
    # main loop
    # ------------------------------------------------------------------
    async def run(self) -> List[Any]:
        """
        Run evolutionary search asynchronously. Returns Pareto-optimal policies.
        Call with `await optimizer.run()` (or `optimizer.run_sync()` from sync).
        """
        # Reset state so `run()` can be called more than once.
        self.population = [self._random_policy() for _ in range(self.population_size)]
        self.best_policy = None
        self.best_reward = -float("inf")
        self.generation_history = []
        self._eval_cache = {}
        self._pending_publish_tasks = set()

        patience_counter = 0

        for gen in range(self.generations):
            evaluated: List[Tuple[Any, Dict[str, Any], float]] = []
            for policy in self.population:
                metrics, reward = await self._evaluate(policy)
                evaluated.append((policy, metrics, reward))

            if not evaluated:
                log_event("warning", f"Generation {gen} produced no evaluations.")
                break

            best_in_gen = max(evaluated, key=lambda x: x[2])
            improved = best_in_gen[2] > (self.best_reward + self.convergence_min_delta)
            if improved:
                self.best_reward = float(best_in_gen[2])
                self.best_policy = best_in_gen[0]
                patience_counter = 0
            else:
                patience_counter += 1

            pareto_policies = self._pareto_filter(evaluated)

            # Publish the best in this generation (off the critical path).
            self._spawn_publish(
                best_in_gen[0], best_in_gen[1], best_in_gen[2], gen
            )

            # Centroid of the current population.
            centroid = np.mean(
                np.asarray(
                    [self._policy_to_vector(p) for p in self.population],
                    dtype=np.float32,
                ),
                axis=0,
            )
            self.generation_history.append(centroid.tolist())
            if len(self.generation_history) > self._generation_history_max:
                self.generation_history = self.generation_history[
                    -self._generation_history_max:
                ]

            # Diversity + adaptive mutation.
            diversity = self._compute_diversity()
            current_mutation = self.mutation_rate
            if diversity < self.diversity_threshold:
                current_mutation = min(0.5, self.mutation_rate * 1.5)

            # Drift detection (compares against the last stored centroid).
            drift = self._detect_drift(self.population)
            if drift:
                log_event("warning", f"Generation {gen}: population drift detected.")
                current_mutation = min(0.5, current_mutation * 1.3)
                if self.metrics:
                    try:
                        self.metrics["drift_events"].inc()
                    except Exception:
                        pass

            if self.metrics:
                try:
                    self.metrics["best_reward"].set(self.best_reward)
                    self.metrics["diversity"].set(diversity)
                    self.metrics["pareto_size"].set(len(pareto_policies))
                except Exception:
                    pass

            log_event(
                "info",
                f"Generation {gen}: best_reward={self.best_reward:.3f} "
                f"pareto_size={len(pareto_policies)} diversity={diversity:.3f} "
                f"mutation={current_mutation:.3f}",
            )

            # Early stopping.
            if patience_counter >= self.convergence_patience:
                log_event(
                    "info",
                    f"Early stop: no improvement for "
                    f"{self.convergence_patience} generations.",
                )
                break

            # Selection + reproduction.
            parents = self._select_parents(evaluated)
            elite_candidates = sorted(
                evaluated, key=lambda x: x[2], reverse=True
            )[: self.elite_size]
            offspring: List[Any] = [p for p, _, _ in elite_candidates]

            while len(offspring) < self.population_size:
                if len(parents) < 2:
                    offspring.append(self._random_policy())
                    continue
                p1, p2 = self._rng.sample(parents, 2)
                if self._rng.random() < self.crossover_rate:
                    child = self._crossover(p1, p2)
                else:
                    child = self._clone_policy(self._rng.choice([p1, p2]))
                child = self._mutate(child, current_mutation)
                offspring.append(child)

            self.population = offspring[: self.population_size]

        # Drain pending publishes before returning.
        await self._drain_publishes()

        # Final evaluation (cached; free for survivors).
        final_evaluated: List[Tuple[Any, Dict[str, Any], float]] = []
        for policy in self.population:
            metrics, reward = await self._evaluate(policy)
            final_evaluated.append((policy, metrics, reward))

        final_policies = self._pareto_filter(final_evaluated)

        # Fallback: top-K by reward if the Pareto front is empty.
        if not final_policies:
            final_sorted = sorted(
                final_evaluated, key=lambda x: x[2], reverse=True
            )
            final_policies = [p for p, _, _ in final_sorted[:10]]

        # Ensure the best policy is included.
        if self.best_policy is not None:
            already = any(
                self._same_policy(self.best_policy, p) for p in final_policies
            )
            if not already:
                final_policies.append(self.best_policy)

        log_event(
            "info",
            f"Evolution finished. best_reward={self.best_reward:.3f} "
            f"final_pareto={len(final_policies)}",
        )
        return final_policies

    # ------------------------------------------------------------------
    # sync wrapper
    # ------------------------------------------------------------------
    def run_sync(self) -> List[Any]:
        """
        Run the search synchronously.

        Raises RuntimeError if called from within a running event loop; use
        `await optimizer.run()` instead.
        """
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(self.run())
        raise RuntimeError(
            "run_sync() was called from a running event loop. "
            "Use `await optimizer.run()` instead."
        )

    # ------------------------------------------------------------------
    # introspection
    # ------------------------------------------------------------------
    def get_pareto_front(self) -> List[Any]:
        """Return the current population (the last generation's candidates)."""
        return list(self.population)

    def get_stats(self) -> Dict[str, Any]:
        return {
            "seed": self._seed,
            "population_size": self.population_size,
            "generations": self.generations,
            "best_reward": self.best_reward,
            "generation_history_length": len(self.generation_history),
            "eval_cache_size": len(self._eval_cache),
            "pending_publishes": len(self._pending_publish_tasks),
            "carbon_intensity": self.carbon_intensity,
        }


# ==============================================================================
# Example usage
# ==============================================================================
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    class _FakeNode:
        id = "node-1"
        metadata = {"gpu_memory_gb": 24.0}

    class _FakeWorkload:
        task_id = "task-1"
        latency_target = 500.0

    class _FakeCostModel:
        def estimate(self, policy, node, workload):
            # A trivial cost model: bigger batches and higher precision cost more.
            batch = getattr(policy, "gpu_batch_size", 1)
            bits = getattr(policy, "weight_bits", 8)
            latency = 100.0 + batch * 10.0
            energy = 50.0 + bits * 2.0
            carbon = energy / 100.0
            gpu_mem = 4.0 + batch * 0.5
            class _Est:
                pass
            est = _Est()
            est.total_latency_ms = latency
            est.total_energy_joules = energy
            est.total_carbon_g = carbon
            est.peak_gpu_memory_gb = gpu_mem
            return est

    async def _demo() -> None:
        optimizer = BioPolicySearch(
            node=_FakeNode(),
            workload=_FakeWorkload(),
            cost_model=_FakeCostModel(),
            population_size=12,
            generations=4,
            seed=42,
            carbon_intensity=200.0,
        )
        policies = await optimizer.run()
        print(f"Returned {len(policies)} Pareto policies.")
        print("Best reward:", optimizer.best_reward)
        print("Stats:", optimizer.get_stats())

    asyncio.run(_demo())
