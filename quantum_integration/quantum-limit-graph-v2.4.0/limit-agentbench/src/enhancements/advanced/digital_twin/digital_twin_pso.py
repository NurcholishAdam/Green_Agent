# src/quantum_integration/digital_twin/digital_twin_pso.py

"""Particle Swarm Optimization with a deterministic, noiseless fitness.

Optimization direction
----------------------
By default the optimizer **maximizes** the fitness function, which
matches the natural reading of "fitness in ``[0, 1]``". Pass
``minimize=True`` to minimize instead. The direction is stored on the
optimizer, reported in :meth:`ParticleSwarmOptimizer.statistics`, and
embedded in :class:`PSOResult` as ``direction``.

The ``converged`` field on :class:`PSOResult` is preserved for backward
compatibility. It is ``True`` iff the run stopped early due to the
stagnation tolerance. The more precise ``stop_reason`` is one of
``"max_iter"``, ``"tolerance"``, or ``"not_started"``.

Validation
----------
* ``param_bounds`` entries must be ``(low, high)`` pairs of finite
  numbers with ``low < high``.
* ``fitness_fn`` return values must be finite numeric. ``NaN`` and
  ``inf`` raise :class:`DigitalTwinInputError` rather than silently
  corrupting the swarm.
* All constructor hyperparameters are validated.

Reproducibility
---------------
The constructor ``seed`` is stored on the instance as ``seed`` and is
persisted in :meth:`to_dict`. The full RNG state is also serialized so
a partially-completed run can be resumed exactly.
"""

from __future__ import annotations

import logging
import math
import random
import threading
import uuid
from collections.abc import Mapping as ABCMapping
from dataclasses import dataclass
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Literal,
    Mapping,
    Optional,
    Sequence,
    Tuple,
)

from .digital_twin_errors import DigitalTwinInputError

logger = logging.getLogger(__name__)
SCHEMA_VERSION: int = 2

# --------------------------------------------------------------------------- #
# Defaults
# --------------------------------------------------------------------------- #

_DEFAULT_INERTIA: float = 0.7
_DEFAULT_COGNITIVE: float = 1.5
_DEFAULT_SOCIAL: float = 1.5
_DEFAULT_V_MAX_FRACTION: float = 0.2

_DEFAULT_PARAM_BOUNDS: Dict[str, Tuple[float, float]] = {
    "distillation_learning_rate": (1e-5, 1e-2),
    "distill_weight": (0.1, 0.9),
    "rl_weight": (0.1, 0.9),
    "distillation_train_every": (5, 20),
}

_DEFAULT_LOG_PARAMS: Tuple[str, ...] = ("distillation_learning_rate",)
_DEFAULT_INT_PARAMS: Tuple[str, ...] = ("distillation_train_every",)

#: Stop reasons returned on :class:`PSOResult`.
StopReason = Literal["max_iter", "tolerance", "not_started"]

#: Direction labels.
Direction = Literal["maximize", "minimize"]


# --------------------------------------------------------------------------- #
# Validation helpers
# --------------------------------------------------------------------------- #

def _require_real_int(name: str, value: Any, *, minimum: int) -> int:
    """Return ``value`` if it is a real ``int`` >= ``minimum``."""
    if isinstance(value, bool) or not isinstance(value, int):
        raise DigitalTwinInputError(f"{name} must be an integer.")
    if value < minimum:
        raise DigitalTwinInputError(f"{name} must be >= {minimum}.")
    return int(value)


def _require_finite_float(name: str, value: Any) -> float:
    """Return ``value`` as a finite ``float`` (``bool`` rejected)."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise DigitalTwinInputError(f"{name} must be a finite number.")
    v = float(value)
    if not math.isfinite(v):
        raise DigitalTwinInputError(f"{name} must be finite.")
    return v


def _require_non_negative_float(name: str, value: Any) -> float:
    v = _require_finite_float(name, value)
    if v < 0:
        raise DigitalTwinInputError(f"{name} must be >= 0.")
    return v


def _validate_param_bounds(
    bounds: Mapping[str, Any],
) -> Dict[str, Tuple[float, float]]:
    """Return a normalized ``{str: (low, high)}`` mapping.

    Each entry must be a ``(low, high)`` pair of finite numbers with
    ``low < high``. Keys must be non-empty strings.
    """
    if not isinstance(bounds, ABCMapping):
        raise DigitalTwinInputError("param_bounds must be a Mapping.")
    out: Dict[str, Tuple[float, float]] = {}
    for k, v in bounds.items():
        if not isinstance(k, str) or not k.strip():
            raise DigitalTwinInputError(
                "param_bounds keys must be non-empty strings."
            )
        if not isinstance(v, (tuple, list)) or len(v) != 2:
            raise DigitalTwinInputError(
                f"param_bounds[{k!r}] must be a (low, high) pair."
            )
        low = _require_finite_float(f"param_bounds[{k!r}].low", v[0])
        high = _require_finite_float(f"param_bounds[{k!r}].high", v[1])
        if not (low < high):
            raise DigitalTwinInputError(
                f"param_bounds[{k!r}] must satisfy low < high."
            )
        out[k] = (low, high)
    if not out:
        raise DigitalTwinInputError("param_bounds must not be empty.")
    return out


def _validate_fitness_return(value: Any) -> float:
    """Return ``value`` as a finite ``float``, else raise."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise DigitalTwinInputError(
            f"fitness_fn returned non-numeric value: {value!r}."
        )
    f = float(value)
    if not math.isfinite(f):
        raise DigitalTwinInputError(
            f"fitness_fn returned non-finite value: {f!r}."
        )
    return f


# --------------------------------------------------------------------------- #
# Null lock (used when thread_safe=False)
# --------------------------------------------------------------------------- #

class _NullLock:
    def __enter__(self) -> "_NullLock":
        return self

    def __exit__(self, *exc: Any) -> None:
        return None


# --------------------------------------------------------------------------- #
# Result
# --------------------------------------------------------------------------- #

@dataclass(frozen=True)
class PSOResult:
    """Outcome of a single :meth:`ParticleSwarmOptimizer.optimize` run.

    ``best_position`` is a plain ``dict``. Callers should not mutate it;
    the dataclass is frozen but the inner mapping is not.
    """

    best_position: Dict[str, Any]
    best_fitness: float
    iterations_run: int
    converged: bool
    fitness_history: Tuple[float, ...] = ()

    # New, backward-compatible additions:
    stop_reason: StopReason = "not_started"
    direction: Direction = "maximize"

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "fitness_history",
            tuple(float(x) for x in self.fitness_history),
        )
        if self.stop_reason not in ("max_iter", "tolerance", "not_started"):
            raise DigitalTwinInputError(
                f"invalid stop_reason {self.stop_reason!r}."
            )
        if self.direction not in ("maximize", "minimize"):
            raise DigitalTwinInputError(
                f"invalid direction {self.direction!r}."
            )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "best_position": dict(self.best_position),
            "best_fitness": self.best_fitness,
            "iterations_run": self.iterations_run,
            "converged": self.converged,
            "fitness_history": list(self.fitness_history),
            "stop_reason": self.stop_reason,
            "direction": self.direction,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "PSOResult":
        return cls(
            best_position=dict(data.get("best_position") or {}),
            best_fitness=float(data.get("best_fitness", 0.0)),
            iterations_run=int(data.get("iterations_run", 0)),
            converged=bool(data.get("converged", False)),
            fitness_history=tuple(
                float(x) for x in data.get("fitness_history", ())
            ),
            stop_reason=data.get("stop_reason", "not_started"),
            direction=data.get("direction", "maximize"),
        )


# --------------------------------------------------------------------------- #
# Optimizer
# --------------------------------------------------------------------------- #

class ParticleSwarmOptimizer:
    """PSO for tuning simulation hyperparameters.

    ``fitness_fn`` is a deterministic callable that maps a parameter dict
    to a finite float. The optimizer never adds noise to the fitness.

    Direction
    ---------
    By default the optimizer **maximizes** the fitness function. Pass
    ``minimize=True`` to minimize instead.

    Parameters
    ----------
    storage:
        Optional backend exposing ``save_bio_run`` (best-effort).
    num_particles, max_iter:
        Swarm size and maximum number of iterations.
    seed:
        Seed for the deterministic RNG. Stored on the instance and
        persisted in :meth:`to_dict`.
    tolerance, max_stagnation:
        Early-stop when ``|f[k] - f[k-1]| < tolerance`` for
        ``max_stagnation`` consecutive iterations.
    fitness_fn:
        Callable mapping a parameter dict to a finite float. Defaults to
        a heuristic in ``[0, 1]``.
    param_bounds:
        Mapping of parameter name to ``(low, high)``. Defaults cover the
        distillation config.
    minimize:
        Direction. ``False`` (default) maximizes.
    inertia, cognitive, social:
        PSO coefficients. Defaults are the canonical ``0.7, 1.5, 1.5``.
    v_max_fraction:
        Velocity clamp as a fraction of each parameter's range. ``0``
        disables clamping.
    log_params:
        Parameter names sampled log-uniformly for the initial positions.
    int_params:
        Parameter names rounded to integers on every update.
    thread_safe:
        When ``True`` (default), mutations are guarded by an ``RLock``.
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
        param_bounds: Optional[Mapping[str, Tuple[float, float]]] = None,
        # New keyword-only arguments (all optional, defaults preserve
        # the pre-enhancement behavior except for the direction fix):
        minimize: bool = False,
        inertia: float = _DEFAULT_INERTIA,
        cognitive: float = _DEFAULT_COGNITIVE,
        social: float = _DEFAULT_SOCIAL,
        v_max_fraction: float = _DEFAULT_V_MAX_FRACTION,
        log_params: Optional[Sequence[str]] = None,
        int_params: Optional[Sequence[str]] = None,
        thread_safe: bool = True,
    ) -> None:
        # ---- Validate scalar hyperparameters -------------------------- #
        self.num_particles = _require_real_int(
            "num_particles", num_particles, minimum=2,
        )
        self.max_iter = _require_real_int(
            "max_iter", max_iter, minimum=1,
        )
        if isinstance(seed, bool) or not isinstance(seed, int):
            raise DigitalTwinInputError("seed must be an integer.")
        self._seed = int(seed)
        self.tolerance = _require_non_negative_float("tolerance", tolerance)
        self.max_stagnation = _require_real_int(
            "max_stagnation", max_stagnation, minimum=1,
        )
        self.minimize = bool(minimize)
        self.direction: Direction = (
            "minimize" if self.minimize else "maximize"
        )
        self.inertia = _require_non_negative_float("inertia", inertia)
        self.cognitive = _require_non_negative_float("cognitive", cognitive)
        self.social = _require_non_negative_float("social", social)
        self.v_max_fraction = _require_non_negative_float(
            "v_max_fraction", v_max_fraction,
        )

        # ---- Bounds and typed parameters ------------------------------ #
        self.param_bounds: Dict[str, Tuple[float, float]] = (
            _validate_param_bounds(param_bounds)
            if param_bounds is not None
            else _validate_param_bounds(_DEFAULT_PARAM_BOUNDS)
        )
        log_set = set(log_params if log_params is not None else _DEFAULT_LOG_PARAMS)
        int_set = set(int_params if int_params is not None else _DEFAULT_INT_PARAMS)
        self.log_params: Tuple[str, ...] = tuple(
            k for k in self.param_bounds if k in log_set
        )
        self.int_params: Tuple[str, ...] = tuple(
            k for k in self.param_bounds if k in int_set
        )
        # Sanity: log params must have strictly positive bounds.
        for k in self.log_params:
            low, _ = self.param_bounds[k]
            if low <= 0:
                raise DigitalTwinInputError(
                    f"log parameter {k!r} must have low > 0."
                )

        # ---- Collaborators -------------------------------------------- #
        self.storage = storage
        self.fitness_fn: Callable[[Dict[str, Any]], float] = (
            fitness_fn if fitness_fn is not None else self._default_fitness
        )

        # ---- RNG and threading ---------------------------------------- #
        self._rng = random.Random(self._seed)
        self._lock: Any = (
            threading.RLock() if thread_safe else _NullLock()
        )
        self._thread_safe = bool(thread_safe)

        # ---- Run diagnostics ------------------------------------------ #
        self._last_result: Optional[PSOResult] = None

    # ------------------------------------------------------------------ #
    # Properties
    # ------------------------------------------------------------------ #

    @property
    def seed(self) -> int:
        """The constructor seed, exposed for reproducibility."""
        return self._seed

    # ------------------------------------------------------------------ #
    # Sampling / bounds
    # ------------------------------------------------------------------ #

    def _sample_initial(self, key: str) -> float:
        """Sample an initial position for ``key``.

        ``log_params`` are sampled log-uniformly, ``int_params`` are
        sampled as integers, everything else is sampled uniformly.
        """
        low, high = self.param_bounds[key]
        if key in self.log_params:
            return 10 ** self._rng.uniform(
                math.log10(low), math.log10(high),
            )
        if key in self.int_params:
            return float(self._rng.randint(int(low), int(high)))
        return self._rng.uniform(low, high)

    def _sample_velocity(self, key: str) -> float:
        """Sample an initial velocity in ``[-span/10, span/10]``."""
        low, high = self.param_bounds[key]
        span = high - low
        return self._rng.uniform(-span / 10.0, span / 10.0)

    def _apply_bounds(self, key: str, value: float) -> float:
        """Clamp (and optionally round) ``value`` to ``param_bounds[key]``."""
        low, high = self.param_bounds[key]
        if key in self.int_params:
            rounded = float(round(value))
            return max(float(low), min(float(high), rounded))
        return max(low, min(high, value))

    # ------------------------------------------------------------------ #
    # Fitness helpers
    # ------------------------------------------------------------------ #

    def _is_better(self, candidate: float, incumbent: float) -> bool:
        """Return ``True`` if ``candidate`` improves on ``incumbent``."""
        return candidate < incumbent if self.minimize else candidate > incumbent

    def _worst_fitness(self) -> float:
        """Return the sentinel fitness value worse than any real value."""
        return float("inf") if self.minimize else float("-inf")

    def _evaluate(self, position: Dict[str, Any]) -> float:
        """Evaluate ``fitness_fn`` and validate its return value."""
        raw = self.fitness_fn(position)
        return _validate_fitness_return(raw)

    def _default_fitness(self, params: Dict[str, Any]) -> float:
        """Deterministic heuristic fitness in ``[0, 1]``.

        Higher is better; the optimizer maximizes it by default.
        """
        score = 0.5
        if params.get("distillation_learning_rate", 1e-2) < 1e-3:
            score += 0.2
        if params.get("distill_weight", 0.5) > 0.4:
            score += 0.15
        if abs(
            params.get("distill_weight", 0.5)
            + params.get("rl_weight", 0.5)
            - 1.0
        ) < 0.05:
            score += 0.15
        return max(0.0, min(1.0, score))

    # ------------------------------------------------------------------ #
    # Initialization
    # ------------------------------------------------------------------ #

    def _init_particles(self) -> List[Dict[str, Any]]:
        """Create a fresh swarm with sampled positions and velocities."""
        worst = self._worst_fitness()
        particles: List[Dict[str, Any]] = []
        for _ in range(self.num_particles):
            pos = {k: self._sample_initial(k) for k in self.param_bounds}
            vel = {k: self._sample_velocity(k) for k in self.param_bounds}
            particles.append({
                "position": pos,
                "velocity": vel,
                "best_position": dict(pos),
                "best_fitness": worst,
            })
        return particles

    # ------------------------------------------------------------------ #
    # Main loop
    # ------------------------------------------------------------------ #

    async def optimize(self) -> PSOResult:
        """Run the swarm. Deterministic given the RNG state and inputs.

        Returns
        -------
        PSOResult
            Includes ``stop_reason`` (``"max_iter"`` or ``"tolerance"``)
            and ``direction`` (``"maximize"`` or ``"minimize"``).
        """
        with self._lock:
            particles = self._init_particles()
            worst = self._worst_fitness()
            global_best_pos: Dict[str, Any] = dict(particles[0]["position"])
            global_best_fitness = worst
            history: List[float] = []
            stagnation = 0
            converged = False
            stop_reason: StopReason = "max_iter"

            for iteration in range(self.max_iter):
                # ---- Evaluate every particle ----------------------------- #
                for p in particles:
                    fitness = self._evaluate(p["position"])
                    if self._is_better(fitness, p["best_fitness"]):
                        p["best_fitness"] = fitness
                        p["best_position"] = dict(p["position"])
                    if self._is_better(fitness, global_best_fitness):
                        global_best_fitness = fitness
                        global_best_pos = dict(p["position"])

                history.append(global_best_fitness)

                # ---- Early stopping ------------------------------------- #
                if iteration > 0:
                    delta = abs(history[-2] - history[-1])
                    if delta < self.tolerance:
                        stagnation += 1
                        if stagnation >= self.max_stagnation:
                            converged = True
                            stop_reason = "tolerance"
                            break
                    else:
                        stagnation = 0

                # ---- Update positions and velocities -------------------- #
                for p in particles:
                    for key, (low, high) in self.param_bounds.items():
                        r1 = self._rng.random()
                        r2 = self._rng.random()
                        cognitive = self.cognitive * r1 * (
                            p["best_position"][key] - p["position"][key]
                        )
                        social = self.social * r2 * (
                            global_best_pos[key] - p["position"][key]
                        )
                        new_vel = (
                            self.inertia * p["velocity"][key]
                            + cognitive + social
                        )

                        # Velocity clamp (defensive; v_max >= 0).
                        if self.v_max_fraction > 0:
                            v_max = self.v_max_fraction * (high - low)
                            new_vel = max(-v_max, min(v_max, new_vel))

                        p["velocity"][key] = new_vel
                        new_pos = p["position"][key] + new_vel
                        clipped = self._apply_bounds(key, new_pos)
                        if clipped != new_pos:
                            # Zero momentum when the bounds bit.
                            p["velocity"][key] = 0.0
                        p["position"][key] = clipped

            # ---- Persist (best-effort) -------------------------------- #
            self._persist(global_best_pos, global_best_fitness, stop_reason)

            result = PSOResult(
                best_position=global_best_pos,
                best_fitness=global_best_fitness,
                iterations_run=len(history),
                converged=converged,
                fitness_history=tuple(history),
                stop_reason=stop_reason,
                direction=self.direction,
            )
            self._last_result = result
            return result

    def _persist(
        self,
        best_position: Dict[str, Any],
        best_fitness: float,
        stop_reason: StopReason,
    ) -> None:
        """Write the run summary to ``self.storage`` if available."""
        if self.storage is None:
            return
        fn = getattr(self.storage, "save_bio_run", None)
        if fn is None:
            return
        try:
            fn(
                run_id=f"pso_{uuid.uuid4().hex}",
                algorithm="pso",
                problem_id="digital_twin_tuning",
                parameters={
                    "num_particles": self.num_particles,
                    "max_iter": self.max_iter,
                    "seed": self._seed,
                    "minimize": self.minimize,
                    "inertia": self.inertia,
                    "cognitive": self.cognitive,
                    "social": self.social,
                    "v_max_fraction": self.v_max_fraction,
                    "tolerance": self.tolerance,
                    "max_stagnation": self.max_stagnation,
                    "param_bounds": {
                        k: list(v) for k, v in self.param_bounds.items()
                    },
                    "stop_reason": stop_reason,
                },
                best_solution=dict(best_position),
                best_fitness=float(best_fitness),
            )
        except Exception as exc:  # noqa: BLE001 - best-effort
            logger.warning("PSO storage write failed: %s", exc)

    # ------------------------------------------------------------------ #
    # Serialization
    # ------------------------------------------------------------------ #

    def to_dict(self) -> Dict[str, Any]:
        """Serialize the optimizer's configuration and last result.

        The RNG state is included so a partially-completed run can be
        resumed exactly by :meth:`from_dict`.
        """
        with self._lock:
            rng_state: Optional[List[Any]] = None
            try:
                state = self._rng.getstate()
                rng_state = [state[0], list(state[1]), state[2]]
            except Exception:  # noqa: BLE001 - defensive
                rng_state = None
            return {
                "schema_version": SCHEMA_VERSION,
                "num_particles": self.num_particles,
                "max_iter": self.max_iter,
                "seed": self._seed,
                "tolerance": self.tolerance,
                "max_stagnation": self.max_stagnation,
                "minimize": self.minimize,
                "inertia": self.inertia,
                "cognitive": self.cognitive,
                "social": self.social,
                "v_max_fraction": self.v_max_fraction,
                "param_bounds": {
                    k: list(v) for k, v in self.param_bounds.items()
                },
                "log_params": list(self.log_params),
                "int_params": list(self.int_params),
                "rng_state": rng_state,
                "last_result": (
                    self._last_result.to_dict()
                    if self._last_result is not None else None
                ),
            }

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        storage: Optional[Any] = None,
        fitness_fn: Optional[Callable[[Dict[str, Any]], float]] = None,
    ) -> "ParticleSwarmOptimizer":
        """Rebuild an optimizer from :meth:`to_dict` output.

        ``fitness_fn`` must be re-supplied because callables are not
        serializable.
        """
        version = int(data.get("schema_version", 1))
        if version > SCHEMA_VERSION:
            raise DigitalTwinInputError(
                f"schema_version {version} is newer than supported "
                f"({SCHEMA_VERSION})."
            )
        obj = cls(
            storage=storage,
            num_particles=int(data.get("num_particles", 10)),
            max_iter=int(data.get("max_iter", 20)),
            seed=int(data.get("seed", 0)),
            tolerance=float(data.get("tolerance", 1e-4)),
            max_stagnation=int(data.get("max_stagnation", 5)),
            fitness_fn=fitness_fn,
            param_bounds=data.get("param_bounds"),
            minimize=bool(data.get("minimize", False)),
            inertia=float(data.get("inertia", _DEFAULT_INERTIA)),
            cognitive=float(data.get("cognitive", _DEFAULT_COGNITIVE)),
            social=float(data.get("social", _DEFAULT_SOCIAL)),
            v_max_fraction=float(
                data.get("v_max_fraction", _DEFAULT_V_MAX_FRACTION),
            ),
            log_params=data.get("log_params"),
            int_params=data.get("int_params"),
        )
        # Restore RNG state if present.
        rng_state = data.get("rng_state")
        if rng_state is not None:
            try:
                obj._rng.setstate((
                    int(rng_state[0]),
                    tuple(int(x) for x in rng_state[1]),
                    rng_state[2],
                ))
            except (IndexError, TypeError, ValueError) as exc:
                logger.warning("Could not restore PSO RNG state: %s", exc)
        # Restore last result if present.
        last = data.get("last_result")
        if isinstance(last, ABCMapping):
            try:
                obj._last_result = PSOResult.from_dict(last)
            except Exception as exc:  # noqa: BLE001 - best-effort
                logger.warning("Could not restore last PSOResult: %s", exc)
        return obj

    # ------------------------------------------------------------------ #
    # Introspection / lifecycle
    # ------------------------------------------------------------------ #

    def statistics(self) -> Dict[str, Any]:
        """Return an enriched snapshot of the optimizer's state."""
        with self._lock:
            last = self._last_result
            return {
                "schema_version": SCHEMA_VERSION,
                "num_particles": self.num_particles,
                "max_iter": self.max_iter,
                "seed": self._seed,
                "tolerance": self.tolerance,
                "max_stagnation": self.max_stagnation,
                "minimize": self.minimize,
                "direction": self.direction,
                "inertia": self.inertia,
                "cognitive": self.cognitive,
                "social": self.social,
                "v_max_fraction": self.v_max_fraction,
                "param_bounds": {
                    k: list(v) for k, v in self.param_bounds.items()
                },
                "log_params": list(self.log_params),
                "int_params": list(self.int_params),
                "thread_safe": self._thread_safe,
                "has_run": last is not None,
                "last_result": last.to_dict() if last else None,
                "uses_storage": self.storage is not None,
                "storage_type": (
                    type(self.storage).__name__
                    if self.storage is not None else None
                ),
            }

    def close(self, *, flush: bool = True) -> None:
        """Flush and/or close the storage backend, if any."""
        if self.storage is None:
            return
        if flush:
            fn = getattr(self.storage, "flush", None)
            if fn is not None:
                fn()
        fn = getattr(self.storage, "close", None)
        if fn is not None:
            fn()

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}("
            f"num_particles={self.num_particles}, "
            f"max_iter={self.max_iter}, "
            f"direction={self.direction!r}, "
            f"seed={self._seed})"
        )


__all__ = [
    "SCHEMA_VERSION",
    "Direction",
    "PSOResult",
    "ParticleSwarmOptimizer",
    "StopReason",
]
