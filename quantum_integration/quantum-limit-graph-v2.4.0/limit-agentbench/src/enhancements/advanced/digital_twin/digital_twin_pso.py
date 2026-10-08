# src/quantum_integration/digital_twin/digital_twin_pso.py

"""Particle Swarm Optimization with a deterministic, noiseless fitness."""

from __future__ import annotations

import logging
import random
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from .digital_twin_errors import DigitalTwinInputError

logger = logging.getLogger(__name__)
SCHEMA_VERSION: int = 1


@dataclass(frozen=True)
class PSOResult:
    best_position: Dict[str, Any]
    best_fitness: float
    iterations_run: int
    converged: bool
    fitness_history: tuple = ()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "best_position": dict(self.best_position),
            "best_fitness": self.best_fitness,
            "iterations_run": self.iterations_run,
            "converged": self.converged,
            "fitness_history": list(self.fitness_history),
        }


class ParticleSwarmOptimizer:
    """PSO for tuning simulation hyperparameters.

    ``fitness_fn`` is a deterministic callable that maps a parameter dict
    to a float in ``[0, 1]``. If not provided, a heuristic default is used.
    The optimizer never adds noise to the fitness.
    """

    def __init__(
        self,
        storage: Optional[Any] = None,
        *,
        num_particles: int = 10,
        max_iter: int = 20,
        seed: int = 0,
        tolerance: float = 1e-4,
        max_stagnation: int = 5,
        fitness_fn: Optional[Callable[[Dict[str, Any]], float]] = None,
        param_bounds: Optional[Dict[str, tuple]] = None,
    ) -> None:
        if num_particles < 2:
            raise DigitalTwinInputError("num_particles must be >= 2.")
        if max_iter < 1:
            raise DigitalTwinInputError("max_iter must be >= 1.")
        self.storage = storage
        self.num_particles = int(num_particles)
        self.max_iter = int(max_iter)
        self._rng = random.Random(seed)
        self.tolerance = float(tolerance)
        self.max_stagnation = max(1, int(max_stagnation))
        self.fitness_fn = fitness_fn or self._default_fitness
        self.param_bounds = param_bounds or {
            "distillation_learning_rate": (1e-5, 1e-2),
            "distill_weight": (0.1, 0.9),
            "rl_weight": (0.1, 0.9),
            "distillation_train_every": (5, 20),
        }
        self._log_params = ("distillation_learning_rate",)

    # ------------------------------------------------------------------ #
    def _sample_initial(self, key: str) -> float:
        low, high = self.param_bounds[key]
        if key in self._log_params:
            return 10 ** self._rng.uniform(
                __import__("math").log10(low),
                __import__("math").log10(high),
            )
        if key == "distillation_train_every":
            return float(self._rng.randint(int(low), int(high)))
        return self._rng.uniform(low, high)

    def _sample_velocity(self, key: str) -> float:
        low, high = self.param_bounds[key]
        return self._rng.uniform(-(high - low) / 10, (high - low) / 10)

    def _init_particles(self) -> List[Dict[str, Any]]:
        particles = []
        for _ in range(self.num_particles):
            pos = {k: self._sample_initial(k) for k in self.param_bounds}
            vel = {k: self._sample_velocity(k) for k in self.param_bounds}
            particles.append({
                "position": pos,
                "velocity": vel,
                "best_position": dict(pos),
                "best_fitness": float("inf"),
            })
        return particles

    def _apply_bounds(self, key: str, value: float) -> float:
        low, high = self.param_bounds[key]
        if key in self._log_params:
            return 10 ** max(
                __import__("math").log10(low),
                min(__import__("math").log10(high), value),
            )
        if key == "distillation_train_every":
            return float(max(low, min(high, round(value))))
        return max(low, min(high, value))

    def _default_fitness(self, params: Dict[str, Any]) -> float:
        """Deterministic heuristic fitness."""
        score = 0.5
        if params.get("distillation_learning_rate", 1e-2) < 1e-3:
            score += 0.2
        if params.get("distill_weight", 0.5) > 0.4:
            score += 0.15
        if abs(params.get("distill_weight", 0.5) + params.get("rl_weight", 0.5) - 1.0) < 0.05:
            score += 0.15
        return max(0.0, min(1.0, score))

    # ------------------------------------------------------------------ #
    async def optimize(self) -> PSOResult:
        particles = self._init_particles()
        global_best_pos: Dict[str, Any] = dict(particles[0]["position"])
        global_best_fitness = float("inf")
        w, c1, c2 = 0.7, 1.5, 1.5
        history: List[float] = []
        stagnation = 0
        converged = False

        for iteration in range(self.max_iter):
            for p in particles:
                fitness = float(self.fitness_fn(p["position"]))
                if fitness < p["best_fitness"]:
                    p["best_fitness"] = fitness
                    p["best_position"] = dict(p["position"])
                if fitness < global_best_fitness:
                    global_best_fitness = fitness
                    global_best_pos = dict(p["position"])

            history.append(global_best_fitness)

            if iteration > 0:
                delta = abs(history[-2] - history[-1])
                if delta < self.tolerance:
                    stagnation += 1
                    if stagnation >= self.max_stagnation:
                        converged = True
                        break
                else:
                    stagnation = 0

            for p in particles:
                for key in self.param_bounds:
                    r1 = self._rng.random()
                    r2 = self._rng.random()
                    cognitive = c1 * r1 * (p["best_position"][key] - p["position"][key])
                    social = c2 * r2 * (global_best_pos[key] - p["position"][key])
                    new_vel = w * p["velocity"][key] + cognitive + social
                    p["velocity"][key] = new_vel
                    new_pos = p["position"][key] + new_vel
                    p["position"][key] = self._apply_bounds(key, new_pos)

        # Persist only the final best result.
        if self.storage and hasattr(self.storage, "save_bio_run"):
            try:
                self.storage.save_bio_run(
                    run_id=f"pso_{uuid.uuid4()}",
                    algorithm="pso",
                    problem_id="digital_twin_tuning",
                    parameters={
                        "num_particles": self.num_particles,
                        "max_iter": self.max_iter,
                        "seed": self._rng.random(),
                    },
                    best_solution=global_best_pos,
                    best_fitness=global_best_fitness,
                )
            except Exception as exc:  # noqa: BLE001 - best-effort
                logger.warning("PSO storage write failed: %s", exc)

        return PSOResult(
            best_position=global_best_pos,
            best_fitness=global_best_fitness,
            iterations_run=len(history),
            converged=converged,
            fitness_history=tuple(history),
        )

    def statistics(self) -> Dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "num_particles": self.num_particles,
            "max_iter": self.max_iter,
            "param_bounds": {k: list(v) for k, v in self.param_bounds.items()},
        }

    def close(self, *, flush: bool = True) -> None:
        return None


__all__ = ["SCHEMA_VERSION", "PSOResult", "ParticleSwarmOptimizer"]
