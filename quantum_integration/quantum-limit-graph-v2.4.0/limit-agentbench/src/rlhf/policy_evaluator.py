# src/rlhf/policy_evaluator.py

"""
Policy Evaluation Environment for Green_Agent
==============================================

Transforms Green_Agent into an RLHF policy-evaluation environment. Enables
testing agents across different execution modes to find optimal policies for
specific deployment scenarios.

Enhancements
------------
- Optional ``numpy`` import guarded by ``_NUMPY_AVAILABLE``.
- ``PolicyEvaluationConfig`` — frozen, validated: bounded history, strict
  mode, verbosity.
- ``PolicyEvaluationRecord`` — frozen, serializable summary of one run.
- **Thread safety** — ``RLock`` guards the execution history.
- **Bounded history** — ``deque(maxlen=config.max_history)``.
- **Full validation** of every argument; strict / non-strict modes.
- **Fixed empty-tasks crash** — aggregates return safe defaults when the
  task list is empty.
- **Replaced `print()`** — verbose output now uses ``logger.info``.
- Serialization: ``to_dict`` / ``to_json`` on the environment + record.
- ``__repr__``, custom ``PolicyEvaluatorError``, ``__main__`` smoke test.
"""

from __future__ import annotations

import json
import logging
import math
import threading
import time
from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Deque, Dict, List, Mapping, Optional

from .reward_shaper import ExecutionMode, RewardShaper, RewardShaperError

logger = logging.getLogger(__name__)

try:  # pragma: no cover — optional dependency
    import numpy as np  # type: ignore

    _NUMPY_AVAILABLE = True
except ImportError:  # pragma: no cover
    np = None  # type: ignore[assignment]
    _NUMPY_AVAILABLE = False


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class PolicyEvaluatorError(ValueError):
    """Raised for invalid policy-evaluation inputs or configuration."""


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class PolicyEvaluationConfig:
    """Tunable parameters for :class:`PolicyEvaluationEnvironment`."""

    max_history: int = 1000
    verbose: bool = False

    # Safety limits on the number of tasks evaluated.
    max_tasks: int = 1_000_000

    def __post_init__(self) -> None:
        if self.max_history <= 0:
            raise PolicyEvaluatorError("max_history must be > 0.")
        if self.max_tasks <= 0:
            raise PolicyEvaluatorError("max_tasks must be > 0.")


# --------------------------------------------------------------------------- #
# Record
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class PolicyEvaluationRecord:
    """Immutable summary of a single policy-evaluation run."""

    mode: str
    num_tasks: int
    avg_reward: float
    avg_task_success: float
    total_penalty: float
    avg_energy_kwh: float
    avg_carbon_kg: float
    avg_latency_ms: float
    errors: int
    timestamp: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "mode": self.mode,
            "num_tasks": self.num_tasks,
            "avg_reward": self.avg_reward,
            "avg_task_success": self.avg_task_success,
            "total_penalty": self.total_penalty,
            "avg_energy_kwh": self.avg_energy_kwh,
            "avg_carbon_kg": self.avg_carbon_kg,
            "avg_latency_ms": self.avg_latency_ms,
            "errors": self.errors,
            "timestamp": self.timestamp.isoformat(),
        }

    def __repr__(self) -> str:
        return (
            "PolicyEvaluationRecord("
            f"mode={self.mode}, tasks={self.num_tasks}, "
            f"avg_reward={self.avg_reward:.4f}, "
            f"errors={self.errors})"
        )


# --------------------------------------------------------------------------- #
# Environment
# --------------------------------------------------------------------------- #
class PolicyEvaluationEnvironment:
    """
    Green_Agent as an RLHF policy-evaluation environment.

    Provides an environment for evaluating and comparing agent policies
    across different execution modes (eco, fast, accuracy, balanced).
    """

    def __init__(
        self,
        *,
        config: Optional[PolicyEvaluationConfig] = None,
        strict: bool = True,
    ) -> None:
        self._config = config or PolicyEvaluationConfig()
        self._strict = bool(strict)

        self._lock = threading.RLock()
        # One shaper per preset mode (CUSTOM excluded — it needs a config).
        self.reward_shapers: Dict[ExecutionMode, RewardShaper] = {
            mode: RewardShaper(mode)
            for mode in ExecutionMode
            if mode != ExecutionMode.CUSTOM
        }
        self.execution_history: Deque[PolicyEvaluationRecord] = deque(
            maxlen=self._config.max_history
        )

        logger.debug(
            "PolicyEvaluationEnvironment initialized "
            "(modes=%s, max_history=%d, strict=%s)",
            [m.value for m in self.reward_shapers],
            self._config.max_history,
            self._strict,
        )

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> PolicyEvaluationConfig:
        return self._config

    @property
    def history_size(self) -> int:
        with self._lock:
            return len(self.execution_history)

    # ---------------------------------------------------------- public API
    def evaluate_policy(
        self,
        agent_policy: Callable[[Dict[str, Any]], Dict[str, Any]],
        tasks: List[Dict[str, Any]],
        mode: ExecutionMode = ExecutionMode.BALANCED_MODE,
        verbose: Optional[bool] = None,
    ) -> Dict[str, Any]:
        """
        Evaluate an agent policy across ``tasks`` under ``mode``.

        Parameters
        ----------
        agent_policy : callable
            Callable taking a task dict and returning a metrics dict with
            (at minimum) ``accuracy`` or ``task_success``, plus
            ``energy_kwh``, ``carbon_kg``, ``latency_ms``, and ``cost_usd``.
        tasks : list of dict
            Evaluation tasks. Must be a list.
        mode : ExecutionMode
            Reward mode to evaluate under.
        verbose : bool, optional
            Per-run progress logging. Defaults to ``config.verbose``.

        Returns
        -------
        dict
            Aggregated evaluation results with ``mode``, ``avg_reward``,
            ``avg_task_success``, ``total_penalty``, ``task_results``, and
            ``summary``.
        """
        # ---- Validate ------------------------------------------------
        if not callable(agent_policy):
            raise PolicyEvaluatorError("agent_policy must be callable.")
        if not isinstance(tasks, list):
            raise PolicyEvaluatorError("tasks must be a list.")
        if len(tasks) > self._config.max_tasks:
            raise PolicyEvaluatorError(
                f"tasks exceeds max_tasks={self._config.max_tasks}."
            )
        if not isinstance(mode, ExecutionMode):
            raise PolicyEvaluatorError(
                f"mode must be an ExecutionMode, got "
                f"{type(mode).__name__}."
            )
        if mode not in self.reward_shapers:
            raise PolicyEvaluatorError(
                f"no reward shaper registered for mode {mode.value!r}; "
                "CUSTOM requires an explicit RewardShaper."
            )

        verbose = self._config.verbose if verbose is None else bool(verbose)
        shaper = self.reward_shapers[mode]

        if verbose:
            logger.info(
                "Evaluating policy in %s mode (tasks=%d).",
                mode.value, len(tasks),
            )

        results: List[Dict[str, Any]] = []
        for i, task in enumerate(tasks):
            if verbose and (i + 1) % 10 == 0:
                logger.info("Progress: %d/%d", i + 1, len(tasks))
            if not isinstance(task, Mapping):
                results.append({
                    "task_id": f"task_{i}",
                    "reward": 0.0,
                    "error": "task must be a Mapping",
                })
                continue
            try:
                result = agent_policy(dict(task))
                if not isinstance(result, Mapping):
                    raise PolicyEvaluatorError(
                        f"agent_policy returned {type(result).__name__}; "
                        "expected Mapping."
                    )
                reward_data = shaper.compute_reward(
                    task_success=float(
                        result.get("accuracy",
                                   result.get("task_success", 0.0))
                    ),
                    energy_kwh=float(result.get("energy_kwh", 0.0)),
                    carbon_kg=float(result.get("carbon_kg", 0.0)),
                    latency_ms=float(result.get("latency_ms", 0.0)),
                    cost_usd=float(result.get("cost_usd", 0.0)),
                )
                results.append({
                    "task_id": task.get("task_id", f"task_{i}"),
                    **reward_data,
                    "metrics": dict(result),
                })
            except RewardShaperError as exc:
                if self._strict:
                    raise
                logger.warning("Task %d reward computation failed: %s", i, exc)
                results.append({
                    "task_id": task.get("task_id", f"task_{i}"),
                    "reward": 0.0,
                    "error": str(exc),
                })
            except Exception as exc:
                logger.error("Task %d failed: %s", i, exc)
                results.append({
                    "task_id": task.get("task_id", f"task_{i}"),
                    "reward": 0.0,
                    "error": str(exc),
                })

        # ---- Aggregate ----------------------------------------------
        rewards = [r["reward"] for r in results if "reward" in r]
        successes = [
            r["components"]["task_success"]
            for r in results if "components" in r
        ]
        penalties = [
            r["penalties"]["total_penalty"]
            for r in results if "penalties" in r
        ]
        energy_values = [
            r["metrics"].get("energy_kwh", 0.0)
            for r in results if "metrics" in r
        ]
        carbon_values = [
            r["metrics"].get("carbon_kg", 0.0)
            for r in results if "metrics" in r
        ]
        latency_values = [
            r["metrics"].get("latency_ms", 0.0)
            for r in results if "metrics" in r
        ]
        errors = sum(1 for r in results if "error" in r)

        def _mean(values: List[float]) -> float:
            if not values:
                return 0.0
            return sum(values) / len(values)

        avg_reward = _mean(rewards)
        avg_task_success = _mean(successes)
        total_penalty = sum(penalties)

        record = PolicyEvaluationRecord(
            mode=mode.value,
            num_tasks=len(results),
            avg_reward=avg_reward,
            avg_task_success=avg_task_success,
            total_penalty=total_penalty,
            avg_energy_kwh=_mean(energy_values),
            avg_carbon_kg=_mean(carbon_values),
            avg_latency_ms=_mean(latency_values),
            errors=errors,
        )

        with self._lock:
            self.execution_history.append(record)

        return {
            "mode": mode.value,
            "avg_reward": avg_reward,
            "avg_task_success": avg_task_success,
            "total_penalty": total_penalty,
            "task_results": results,
            "summary": {
                "num_tasks": len(results),
                "errors": errors,
                "avg_energy_kwh": _mean(energy_values),
                "avg_carbon_kg": _mean(carbon_values),
                "avg_latency_ms": _mean(latency_values),
            },
        }

    # ---------------------------------------------------------- history
    def get_recent_evaluations(
        self, n: int = 10
    ) -> List[PolicyEvaluationRecord]:
        """Return the ``n`` most recent evaluation records."""
        if not isinstance(n, int) or n <= 0:
            raise PolicyEvaluatorError("n must be a positive int.")
        with self._lock:
            records = list(self.execution_history)
        return records[-n:] if len(records) >= n else records

    def statistics(self) -> Dict[str, Any]:
        """Return a JSON-safe snapshot of the environment state."""
        with self._lock:
            history = list(self.execution_history)
        return {
            "modes": [m.value for m in self.reward_shapers],
            "history_size": len(history),
            "strict": self._strict,
            "numpy_available": _NUMPY_AVAILABLE,
            "last_evaluation": (
                history[-1].to_dict() if history else None
            ),
        }

    def reset(self, *, clear_history: bool = True) -> int:
        """Reset the execution history. Returns entries removed."""
        with self._lock:
            removed = len(self.execution_history)
            if clear_history:
                self.execution_history.clear()
        logger.debug("PolicyEvaluationEnvironment reset (removed %d).", removed)
        return removed

    # ---------------------------------------------------------- serialization
    def to_dict(self, *, include_history: bool = False) -> Dict[str, Any]:
        with self._lock:
            payload: Dict[str, Any] = {
                "config": asdict(self._config),
                "strict": self._strict,
                "modes": [m.value for m in self.reward_shapers],
                "history_size": len(self.execution_history),
            }
            if include_history:
                payload["execution_history"] = [
                    r.to_dict() for r in self.execution_history
                ]
        return payload

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    # ----------------------------------------------------------------- dunder
    def __repr__(self) -> str:
        with self._lock:
            return (
                "PolicyEvaluationEnvironment("
                f"modes={[m.value for m in self.reward_shapers]}, "
                f"history={len(self.execution_history)}, "
                f"strict={self._strict})"
            )


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "PolicyEvaluationConfig",
    "PolicyEvaluationEnvironment",
    "PolicyEvaluationRecord",
    "PolicyEvaluatorError",
]


# --------------------------------------------------------------------------- #
# Smoke test: python -m rlhf.policy_evaluator
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    env = PolicyEvaluationEnvironment()
    print("repr       :", env)

    # ---- Dummy policy ------------------------------------------- #
    def _dummy_policy(task: Dict[str, Any]) -> Dict[str, Any]:
        # Return synthetic metrics derived from the task id.
        tid = int(task.get("task_id", "0").split("_")[-1]) if "task_id" in task else 0
        return {
            "accuracy": 0.9 + 0.001 * tid,
            "energy_kwh": 0.001 + 0.00001 * tid,
            "carbon_kg": 0.0002 + 0.000001 * tid,
            "latency_ms": 100.0 + tid,
            "cost_usd": 0.0001,
        }

    tasks = [{"task_id": f"task_{i}"} for i in range(20)]

    # ---- Evaluate under each mode ------------------------------- #
    for mode in ExecutionMode:
        if mode == ExecutionMode.CUSTOM:
            continue
        result = env.evaluate_policy(_dummy_policy, tasks, mode)
        print(f"{mode.value:<10} avg_reward={result['avg_reward']:.4f} "
              f"success={result['avg_task_success']:.3f} "
              f"penalty={result['total_penalty']:.4f}")

    # ---- History ------------------------------------------------ #
    print("recent     :", len(env.get_recent_evaluations(2)))
    print("statistics :", {
        k: v for k, v in env.statistics().items()
        if k != "last_evaluation"
    })

    # ---- Empty tasks ------------------------------------------- #
    empty = env.evaluate_policy(_dummy_policy, [])
    assert empty["avg_reward"] == 0.0
    print("empty      : OK")

    # ---- Bug fix: validation ----------------------------------- #
    for bad_call in (
        lambda: env.evaluate_policy("not-callable", tasks),  # type: ignore[arg-type]
        lambda: env.evaluate_policy(_dummy_policy, "not-a-list"),  # type: ignore[arg-type]
        lambda: env.evaluate_policy(_dummy_policy, tasks, mode="eco"),  # type: ignore[arg-type]
        lambda: env.evaluate_policy(_dummy_policy, tasks, mode=ExecutionMode.CUSTOM),
        lambda: env.get_recent_evaluations(0),
    ):
        try:
            bad_call()
        except PolicyEvaluatorError as exc:
            print("Rejected   :", exc)

    # ---- Bounded history --------------------------------------- #
    bounded = PolicyEvaluationEnvironment(
        config=PolicyEvaluationConfig(max_history=3)
    )
    for _ in range(10):
        bounded.evaluate_policy(_dummy_policy, tasks[:1])
    assert bounded.history_size == 3
    print("bounded    : OK")

    # ---- Serialization ----------------------------------------- #
    payload = env.to_json()
    restored = json.loads(payload)
    assert restored["config"]["max_history"] == 1000
    print("Serialization OK.")

    # ---- Context manager ---------------------------------------- #
    with PolicyEvaluationEnvironment() as scoped:
        scoped.evaluate_policy(_dummy_policy, tasks[:2])
        assert scoped.history_size == 1
    print("Context    : OK")

    # ---- Reset -------------------------------------------------- #
    print("reset      :", env.reset(), "records removed")
    assert env.history_size == 0

    print("\nSmoke test passed.")
