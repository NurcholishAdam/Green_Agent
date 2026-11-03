# src/rlhf/policy_evaluator.py

"""
Policy Evaluation Environment for Green_Agent
==============================================

Transforms Green_Agent into an RLHF policy-evaluation environment. Enables
testing agents across different execution modes to find optimal policies for
specific deployment scenarios.

Design
------
- Frozen, validated :class:`PolicyEvaluationConfig`: bounded history,
  max tasks, verbose flag (validated as bool).
- :class:`PolicyEvaluationRecord` — frozen, validated, serializable.
  Non-finite values, negative counts, and non-string modes raise at
  construction. Both ``from_dict`` and ``from_json`` round-trip.
- **Consistent strict mode.** ``strict=True`` propagates any failure
  (framework :class:`PolicyEvaluatorError`, agent exception, reward
  shaper error). ``strict=False`` collects failures into
  ``results[i]["error"]`` and continues.
- **Uniform result shape.** Every per-task result carries an ``ok``
  bool; successes carry ``components`` / ``penalties`` / ``metrics``;
  failures carry ``error`` and ``error_kind``.
- **Thread-safe.** ``RLock`` guards the execution history.
- **Bounded history.** ``deque(maxlen=config.max_history)``.
- **Serialization.** ``to_dict`` / ``from_dict`` / ``to_json`` /
  ``from_json`` on the config, the record, and the environment.
  ``to_json`` uses ``allow_nan=False``.

Version: 2.0.0
"""

from __future__ import annotations

import json
import logging
import math
import numbers
import threading
import time
from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from types import TracebackType
from typing import (
    Any,
    Callable,
    Deque,
    Dict,
    List,
    Mapping,
    Optional,
    Tuple,
)

from .reward_shaper import ExecutionMode, RewardShaper, RewardShaperError

logger = logging.getLogger(__name__)

__all__ = [
    "VERSION",
    "PolicyEvaluationConfig",
    "PolicyEvaluationEnvironment",
    "PolicyEvaluationRecord",
    "PolicyEvaluatorError",
]

VERSION = "2.0.0"


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class PolicyEvaluatorError(ValueError):
    """Raised for invalid policy-evaluation inputs or configuration."""


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def _mean(values: List[float]) -> float:
    """Return the arithmetic mean of ``values`` (0.0 if empty)."""
    if not values:
        return 0.0
    return sum(values) / len(values)


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class PolicyEvaluationConfig:
    """Tunable parameters for :class:`PolicyEvaluationEnvironment`."""

    max_history: int = 1000
    verbose: bool = False
    max_tasks: int = 1_000_000

    def __post_init__(self) -> None:
        if (
            isinstance(self.max_history, bool)
            or not isinstance(self.max_history, numbers.Integral)
            or self.max_history <= 0
        ):
            raise PolicyEvaluatorError(
                "max_history must be a positive int."
            )
        if not isinstance(self.verbose, bool):
            raise PolicyEvaluatorError(
                f"verbose must be bool, got "
                f"{type(self.verbose).__name__}."
            )
        if (
            isinstance(self.max_tasks, bool)
            or not isinstance(self.max_tasks, numbers.Integral)
            or self.max_tasks <= 0
        ):
            raise PolicyEvaluatorError(
                "max_tasks must be a positive int."
            )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any],
    ) -> "PolicyEvaluationConfig":
        if not isinstance(data, Mapping):
            raise PolicyEvaluatorError(
                f"PolicyEvaluationConfig.from_dict expects a "
                f"Mapping, got {type(data).__name__}."
            )
        try:
            valid = set(cls.__dataclass_fields__.keys())
            unknown = set(data) - valid
            if unknown:
                logger.debug(
                    "PolicyEvaluationConfig.from_dict dropping "
                    "unknown fields: %s", sorted(unknown),
                )
            kwargs = {k: v for k, v in data.items() if k in valid}
            return cls(**kwargs)
        except (TypeError, ValueError) as exc:
            if isinstance(exc, PolicyEvaluatorError):
                raise
            raise PolicyEvaluatorError(
                f"invalid PolicyEvaluationConfig payload: {exc}"
            ) from exc


# --------------------------------------------------------------------------- #
# Record
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class PolicyEvaluationRecord:
    """Immutable, validated summary of a single policy-evaluation run."""

    mode: str
    num_tasks: int
    avg_reward: float
    avg_task_success: float
    total_penalty: float
    avg_energy_kwh: float
    avg_carbon_kg: float
    avg_latency_ms: float
    errors: int
    config_snapshot: Mapping[str, Any] = field(
        default_factory=dict
    )
    timestamp: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    def __post_init__(self) -> None:
        if not isinstance(self.mode, str) or not self.mode:
            raise PolicyEvaluatorError(
                "mode must be a non-empty string."
            )
        for name in ("num_tasks", "errors"):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, numbers.Integral)
                or value < 0
            ):
                raise PolicyEvaluatorError(
                    f"{name} must be a non-negative int."
                )
        for name in (
            "avg_reward",
            "avg_task_success",
            "total_penalty",
            "avg_energy_kwh",
            "avg_carbon_kg",
            "avg_latency_ms",
        ):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, numbers.Real)
                or not math.isfinite(float(value))
            ):
                raise PolicyEvaluatorError(
                    f"{name} must be finite and numeric, "
                    f"got {value!r}."
                )
        for name in (
            "total_penalty",
            "avg_energy_kwh",
            "avg_carbon_kg",
            "avg_latency_ms",
        ):
            if getattr(self, name) < 0:
                raise PolicyEvaluatorError(
                    f"{name} must be >= 0."
                )
        if self.errors > self.num_tasks:
            raise PolicyEvaluatorError(
                "errors must not exceed num_tasks."
            )
        if not isinstance(self.config_snapshot, Mapping):
            raise PolicyEvaluatorError(
                "config_snapshot must be a Mapping."
            )

    @property
    def num_succeeded(self) -> int:
        """Number of tasks that produced a reward."""
        return self.num_tasks - self.errors

    def to_dict(self) -> Dict[str, Any]:
        return {
            "version": VERSION,
            "mode": self.mode,
            "num_tasks": self.num_tasks,
            "num_succeeded": self.num_succeeded,
            "avg_reward": self.avg_reward,
            "avg_task_success": self.avg_task_success,
            "total_penalty": self.total_penalty,
            "avg_energy_kwh": self.avg_energy_kwh,
            "avg_carbon_kg": self.avg_carbon_kg,
            "avg_latency_ms": self.avg_latency_ms,
            "errors": self.errors,
            "config_snapshot": dict(self.config_snapshot),
            "timestamp": self.timestamp.isoformat(),
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any],
    ) -> "PolicyEvaluationRecord":
        if not isinstance(data, Mapping):
            raise PolicyEvaluatorError(
                "PolicyEvaluationRecord.from_dict expects a Mapping."
            )
        try:
            ts = data.get("timestamp")
            if isinstance(ts, str):
                timestamp = datetime.fromisoformat(
                    ts.replace("Z", "+00:00")
                )
                if not timestamp.tzinfo:
                    timestamp = timestamp.replace(
                        tzinfo=timezone.utc,
                    )
            elif isinstance(ts, datetime):
                timestamp = ts
            else:
                timestamp = datetime.now(timezone.utc)

            snapshot = data.get("config_snapshot") or {}
            if not isinstance(snapshot, Mapping):
                raise PolicyEvaluatorError(
                    "config_snapshot must be a Mapping."
                )

            return cls(
                mode=str(data["mode"]),
                num_tasks=int(data["num_tasks"]),
                avg_reward=float(data["avg_reward"]),
                avg_task_success=float(
                    data["avg_task_success"]
                ),
                total_penalty=float(data["total_penalty"]),
                avg_energy_kwh=float(data["avg_energy_kwh"]),
                avg_carbon_kg=float(data["avg_carbon_kg"]),
                avg_latency_ms=float(data["avg_latency_ms"]),
                errors=int(data.get("errors", 0)),
                config_snapshot=dict(snapshot),
                timestamp=timestamp,
            )
        except KeyError as exc:
            raise PolicyEvaluatorError(
                f"PolicyEvaluationRecord payload missing "
                f"{exc.args[0]!r}."
            ) from exc
        except (TypeError, ValueError) as exc:
            if isinstance(exc, PolicyEvaluatorError):
                raise
            raise PolicyEvaluatorError(
                f"invalid PolicyEvaluationRecord payload: {exc}"
            ) from exc

    def to_json(self, **kwargs: Any) -> str:
        kwargs.setdefault("allow_nan", False)
        kwargs.setdefault("sort_keys", True)
        return json.dumps(self.to_dict(), **kwargs)

    @classmethod
    def from_json(cls, payload: str) -> "PolicyEvaluationRecord":
        try:
            raw = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise PolicyEvaluatorError(
                f"invalid JSON payload: {exc}"
            ) from exc
        return cls.from_dict(raw)

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
        if config is not None:
            if not isinstance(config, PolicyEvaluationConfig):
                raise PolicyEvaluatorError(
                    f"config must be a PolicyEvaluationConfig, "
                    f"got {type(config).__name__}."
                )
            self._config = config
        else:
            self._config = PolicyEvaluationConfig()
        if not isinstance(strict, bool):
            raise PolicyEvaluatorError(
                f"strict must be bool, got "
                f"{type(strict).__name__}."
            )
        self._strict = strict

        self._lock = threading.RLock()
        self.reward_shapers: Dict[ExecutionMode, RewardShaper] = {
            mode: RewardShaper(mode)
            for mode in ExecutionMode
            if mode != ExecutionMode.CUSTOM
        }
        self.execution_history: Deque[PolicyEvaluationRecord] = (
            deque(maxlen=self._config.max_history)
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
        agent_policy: Callable[
            [Dict[str, Any]], Dict[str, Any]
        ],
        tasks: List[Dict[str, Any]],
        mode: ExecutionMode = ExecutionMode.BALANCED_MODE,
        verbose: Optional[bool] = None,
    ) -> Dict[str, Any]:
        """
        Evaluate an agent policy across ``tasks`` under ``mode``.

        In strict mode, any per-task failure raises. In non-strict
        mode, failures are collected into ``results`` with
        ``ok=False`` and an ``error`` field.
        """
        # ---- Validate ------------------------------------------------
        if not callable(agent_policy):
            raise PolicyEvaluatorError(
                "agent_policy must be callable."
            )
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
                f"no reward shaper registered for mode "
                f"{mode.value!r}; CUSTOM requires an explicit "
                f"RewardShaper."
            )
        if verbose is None:
            verbose = self._config.verbose
        elif not isinstance(verbose, bool):
            raise PolicyEvaluatorError(
                f"verbose must be bool or None, got "
                f"{type(verbose).__name__}."
            )

        shaper = self.reward_shapers[mode]
        if verbose:
            logger.info(
                "Evaluating policy in %s mode (tasks=%d).",
                mode.value, len(tasks),
            )

        results: List[Dict[str, Any]] = []
        for i, task in enumerate(tasks):
            if verbose and (i + 1) % 10 == 0:
                logger.debug(
                    "Progress: %d/%d", i + 1, len(tasks),
                )

            if not isinstance(task, Mapping):
                failure = {
                    "task_id": f"task_{i}",
                    "reward": 0.0,
                    "ok": False,
                    "error": "task must be a Mapping",
                    "error_kind": "invalid_task",
                }
                if self._strict:
                    raise PolicyEvaluatorError(
                        failure["error"]
                    )
                results.append(failure)
                continue

            task_id = task.get("task_id", f"task_{i}")

            try:
                result = agent_policy(dict(task))
            except PolicyEvaluatorError:
                raise
            except Exception as exc:
                # Agent failed — strict propagates, lenient collects.
                if self._strict:
                    raise PolicyEvaluatorError(
                        f"agent_policy raised on task "
                        f"{task_id!r}: {exc}"
                    ) from exc
                logger.warning(
                    "Task %r agent_policy failed: %s",
                    task_id, exc,
                )
                results.append({
                    "task_id": task_id,
                    "reward": 0.0,
                    "ok": False,
                    "error": str(exc),
                    "error_kind": "agent_exception",
                })
                continue

            if not isinstance(result, Mapping):
                msg = (
                    f"agent_policy returned "
                    f"{type(result).__name__}; expected Mapping."
                )
                if self._strict:
                    raise PolicyEvaluatorError(msg)
                logger.warning(
                    "Task %r %s", task_id, msg,
                )
                results.append({
                    "task_id": task_id,
                    "reward": 0.0,
                    "ok": False,
                    "error": msg,
                    "error_kind": "invalid_result",
                })
                continue

            try:
                reward_data = shaper.compute_reward(
                    task_success=float(
                        result.get(
                            "accuracy",
                            result.get("task_success", 0.0),
                        )
                    ),
                    energy_kwh=float(
                        result.get("energy_kwh", 0.0)
                    ),
                    carbon_kg=float(
                        result.get("carbon_kg", 0.0)
                    ),
                    latency_ms=float(
                        result.get("latency_ms", 0.0)
                    ),
                    cost_usd=float(
                        result.get("cost_usd", 0.0)
                    ),
                )
            except RewardShaperError:
                if self._strict:
                    raise
                logger.warning(
                    "Task %r reward computation failed.",
                    task_id,
                )
                results.append({
                    "task_id": task_id,
                    "reward": 0.0,
                    "ok": False,
                    "error": "reward shaper failed",
                    "error_kind": "reward_shaper",
                })
                continue
            except Exception as exc:
                if self._strict:
                    raise PolicyEvaluatorError(
                        f"reward computation raised on task "
                        f"{task_id!r}: {exc}"
                    ) from exc
                logger.warning(
                    "Task %r reward computation failed: %s",
                    task_id, exc,
                )
                results.append({
                    "task_id": task_id,
                    "reward": 0.0,
                    "ok": False,
                    "error": str(exc),
                    "error_kind": "reward_exception",
                })
                continue

            results.append({
                "task_id": task_id,
                "reward": float(
                    reward_data.get("reward", 0.0)
                ),
                "ok": True,
                "components": reward_data.get(
                    "components", {}
                ),
                "penalties": reward_data.get(
                    "penalties", {}
                ),
                "metrics": dict(result),
            })

        # ---- Aggregate ----------------------------------------------
        rewards = [
            r["reward"] for r in results if r["ok"]
        ]
        successes = [
            r["components"]["task_success"]
            for r in results
            if r["ok"] and "task_success" in r["components"]
        ]
        penalties = [
            r["penalties"]["total_penalty"]
            for r in results
            if r["ok"] and "total_penalty" in r["penalties"]
        ]
        energy_values = [
            float(r["metrics"].get("energy_kwh", 0.0))
            for r in results if r["ok"]
        ]
        carbon_values = [
            float(r["metrics"].get("carbon_kg", 0.0))
            for r in results if r["ok"]
        ]
        latency_values = [
            float(r["metrics"].get("latency_ms", 0.0))
            for r in results if r["ok"]
        ]
        errors = sum(1 for r in results if not r["ok"])

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
            config_snapshot=self._config.to_dict(),
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
                "num_succeeded": len(results) - errors,
                "errors": errors,
                "avg_energy_kwh": _mean(energy_values),
                "avg_carbon_kg": _mean(carbon_values),
                "avg_latency_ms": _mean(latency_values),
            },
        }

    # ---------------------------------------------------------- history
    def get_recent_evaluations(
        self, n: int = 10,
    ) -> List[PolicyEvaluationRecord]:
        """Return the ``n`` most recent evaluation records."""
        if (
            isinstance(n, bool)
            or not isinstance(n, numbers.Integral)
            or n <= 0
        ):
            raise PolicyEvaluatorError(
                "n must be a positive int."
            )
        with self._lock:
            records = list(self.execution_history)
        return records[-n:]

    def statistics(self) -> Dict[str, Any]:
        """Return a JSON-safe snapshot of the environment state."""
        with self._lock:
            history = list(self.execution_history)

        by_mode: Dict[str, Dict[str, Any]] = {}
        for record in history:
            entry = by_mode.setdefault(
                record.mode,
                {
                    "count": 0,
                    "total_tasks": 0,
                    "total_errors": 0,
                    "rewards": [],
                },
            )
            entry["count"] += 1
            entry["total_tasks"] += record.num_tasks
            entry["total_errors"] += record.errors
            entry["rewards"].append(record.avg_reward)

        by_mode_summary = {
            mode: {
                "count": e["count"],
                "total_tasks": e["total_tasks"],
                "total_errors": e["total_errors"],
                "mean_avg_reward": _mean(e["rewards"]),
            }
            for mode, e in by_mode.items()
        }

        return {
            "modes": [m.value for m in self.reward_shapers],
            "history_size": len(history),
            "strict": self._strict,
            "by_mode": by_mode_summary,
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
        logger.debug(
            "PolicyEvaluationEnvironment reset (removed %d).",
            removed,
        )
        return removed

    # ---------------------------------------------------------- serialization
    def to_dict(
        self, *, include_history: bool = False,
    ) -> Dict[str, Any]:
        with self._lock:
            payload: Dict[str, Any] = {
                "version": VERSION,
                "config": self._config.to_dict(),
                "strict": self._strict,
                "modes": [m.value for m in self.reward_shapers],
                "history_size": len(self.execution_history),
            }
            if include_history:
                payload["execution_history"] = [
                    r.to_dict()
                    for r in self.execution_history
                ]
        return payload

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any],
    ) -> "PolicyEvaluationEnvironment":
        """
        Rebuild an environment from :meth:`to_dict` output.

        Restores configuration, ``strict``, and (when present) the
        execution history. The per-mode ``RewardShaper`` instances
        are freshly constructed — no state is carried across a
        round-trip since :class:`RewardShaper` is stateless.
        """
        if not isinstance(data, Mapping):
            raise PolicyEvaluatorError(
                f"from_dict expects a Mapping, got "
                f"{type(data).__name__}."
            )
        try:
            raw_config = data.get("config") or {}
            if not isinstance(raw_config, Mapping):
                raise PolicyEvaluatorError(
                    "config must be a Mapping."
                )
            cfg = PolicyEvaluationConfig.from_dict(
                dict(raw_config)
            )

            strict_raw = data.get("strict", True)
            if not isinstance(strict_raw, bool):
                raise PolicyEvaluatorError(
                    f"strict must be bool, got "
                    f"{type(strict_raw).__name__}."
                )

            env = cls(config=cfg, strict=strict_raw)

            raw_history = (
                data.get("execution_history") or []
            )
            if not isinstance(raw_history, list):
                raise PolicyEvaluatorError(
                    "execution_history must be a list."
                )

            with env._lock:
                for entry in raw_history:
                    if not isinstance(entry, Mapping):
                        raise PolicyEvaluatorError(
                            "execution_history entries must be "
                            "Mappings."
                        )
                    env.execution_history.append(
                        PolicyEvaluationRecord.from_dict(entry)
                    )
            return env
        except PolicyEvaluatorError:
            raise
        except (TypeError, ValueError, KeyError) as exc:
            raise PolicyEvaluatorError(
                f"invalid PolicyEvaluationEnvironment payload: "
                f"{exc}"
            ) from exc

    def to_json(
        self, *, include_history: bool = False,
        **kwargs: Any,
    ) -> str:
        kwargs.setdefault("allow_nan", False)
        kwargs.setdefault("sort_keys", True)
        return json.dumps(
            self.to_dict(include_history=include_history),
            **kwargs,
        )

    @classmethod
    def from_json(
        cls, payload: str,
    ) -> "PolicyEvaluationEnvironment":
        try:
            raw = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise PolicyEvaluatorError(
                f"invalid JSON payload: {exc}"
            ) from exc
        return cls.from_dict(raw)

    # ---------------------------------------------------------- context mgr
    def __enter__(self) -> "PolicyEvaluationEnvironment":
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> Optional[bool]:
        if exc_type is not None:
            logger.warning(
                "PolicyEvaluationEnvironment scope exited with "
                "%s.", exc_type.__name__,
            )
        return None

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
# __all__ completeness assertion
# --------------------------------------------------------------------------- #
_missing_names = [n for n in __all__ if n not in globals()]
if _missing_names:
    raise PolicyEvaluatorError(
        f"policy_evaluator __all__ references missing names: "
        f"{sorted(_missing_names)}."
    )


# --------------------------------------------------------------------------- #
# Smoke test: python -m rlhf.policy_evaluator
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    print("VERSION    :", VERSION)

    env = PolicyEvaluationEnvironment()
    print("repr       :", env)

    # ---- Dummy policy ------------------------------------------- #
    def _dummy_policy(
        task: Dict[str, Any],
    ) -> Dict[str, Any]:
        tid = (
            int(task.get("task_id", "0").split("_")[-1])
            if "task_id" in task else 0
        )
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
        result = env.evaluate_policy(
            _dummy_policy, tasks, mode,
        )
        print(
            f"{mode.value:<10} "
            f"avg_reward={result['avg_reward']:.4f} "
            f"success={result['avg_task_success']:.3f} "
            f"penalty={result['total_penalty']:.4f} "
            f"ok={result['summary']['num_succeeded']}/"
            f"{result['summary']['num_tasks']}"
        )

    # ---- History ------------------------------------------------ #
    print(
        "recent     :",
        len(env.get_recent_evaluations(2)),
    )
    print("statistics :", {
        k: v for k, v in env.statistics().items()
        if k not in ("last_evaluation",)
    })

    # ---- Empty tasks ------------------------------------------- #
    empty = env.evaluate_policy(_dummy_policy, [])
    assert empty["avg_reward"] == 0.0
    assert empty["summary"]["num_tasks"] == 0
    print("empty      : OK")

    # ---- Strict mode: any failure raises ----------------------- #
    strict_env = PolicyEvaluationEnvironment(strict=True)
    for bad_call in (
        lambda: strict_env.evaluate_policy(
            "not-callable", tasks,  # type: ignore[arg-type]
        ),
        lambda: strict_env.evaluate_policy(
            _dummy_policy, "not-a-list",  # type: ignore[arg-type]
        ),
        lambda: strict_env.evaluate_policy(
            _dummy_policy, tasks,
            mode="eco",  # type: ignore[arg-type]
        ),
        lambda: strict_env.evaluate_policy(
            _dummy_policy, tasks,
            mode=ExecutionMode.CUSTOM,
        ),
        lambda: strict_env.evaluate_policy(
            lambda t: "not-a-mapping",  # returns str, not Mapping
            tasks,
        ),
        lambda: strict_env.evaluate_policy(
            lambda t: (_ for _ in ()).throw(
                ValueError("boom")
            ),
            tasks,
        ),
        lambda: strict_env.evaluate_policy(
            _dummy_policy, tasks,
            verbose="yes",  # type: ignore[arg-type]
        ),
    ):
        try:
            bad_call()
        except PolicyEvaluatorError as exc:
            print("Rejected   :", str(exc)[:60])

    # ---- Non-strict mode: failures collected ------------------- #
    lenient = PolicyEvaluationEnvironment(strict=False)
    mixed = lenient.evaluate_policy(
        lambda t: (
            {"accuracy": 0.9}
            if t.get("task_id") == "task_0"
            else (_ for _ in ()).throw(
                RuntimeError("boom")
            )
        ),
        tasks[:3],
    )
    assert mixed["summary"]["errors"] == 2
    assert mixed["summary"]["num_succeeded"] == 1
    for r in mixed["task_results"]:
        assert "ok" in r
        if not r["ok"]:
            assert "error_kind" in r
    print(
        "lenient    :",
        f"succeeded={mixed['summary']['num_succeeded']}, "
        f"errors={mixed['summary']['errors']}",
    )

    # ---- Bounded history --------------------------------------- #
    bounded = PolicyEvaluationEnvironment(
        config=PolicyEvaluationConfig(max_history=3),
    )
    for _ in range(10):
        bounded.evaluate_policy(_dummy_policy, tasks[:1])
    assert bounded.history_size == 3
    print("bounded    : OK")

    # ---- Serialization round-trip ------------------------------ #
    payload = env.to_json(include_history=True)
    restored = PolicyEvaluationEnvironment.from_json(payload)
    assert (
        restored.to_dict(include_history=True)
        == env.to_dict(include_history=True)
    )
    print("env rt     : OK")

    # ---- Record round-trip ------------------------------------- #
    rec = env.get_recent_evaluations(1)[0]
    rec_rt = PolicyEvaluationRecord.from_json(rec.to_json())
    assert rec_rt.to_dict() == rec.to_dict()
    print("record rt  : OK")

    # ---- Record validation ------------------------------------- #
    for bad_record in (
        dict(
            mode="", num_tasks=0, avg_reward=0.0,
            avg_task_success=0.0, total_penalty=0.0,
            avg_energy_kwh=0.0, avg_carbon_kg=0.0,
            avg_latency_ms=0.0, errors=0,
        ),
        dict(
            mode="eco", num_tasks=-1, avg_reward=0.0,
            avg_task_success=0.0, total_penalty=0.0,
            avg_energy_kwh=0.0, avg_carbon_kg=0.0,
            avg_latency_ms=0.0, errors=0,
        ),
        dict(
            mode="eco", num_tasks=5, avg_reward=float("nan"),
            avg_task_success=0.0, total_penalty=0.0,
            avg_energy_kwh=0.0, avg_carbon_kg=0.0,
            avg_latency_ms=0.0, errors=0,
        ),
        dict(
            mode="eco", num_tasks=3, avg_reward=0.0,
            avg_task_success=0.0, total_penalty=0.0,
            avg_energy_kwh=0.0, avg_carbon_kg=0.0,
            avg_latency_ms=0.0, errors=5,  # errors > num_tasks
        ),
    ):
        try:
            PolicyEvaluationRecord(**bad_record)  # type: ignore[arg-type]
        except PolicyEvaluatorError as exc:
            print("Rejected r :", str(exc)[:60])

    # ---- Config validation ------------------------------------- #
    for bad_cfg in (
        dict(max_history=0),
        dict(max_history=-1),
        dict(verbose="yes"),  # type: ignore[arg-type]
        dict(max_tasks=0),
    ):
        try:
            PolicyEvaluationConfig(**bad_cfg)  # type: ignore[arg-type]
        except PolicyEvaluatorError as exc:
            print("Rejected c :", str(exc)[:60])

    # ---- to_json rejects NaN ----------------------------------- #
    tainted = PolicyEvaluationEnvironment()
    object.__setattr__(
        tainted._config, "max_history", float("nan"),
    )
    try:
        tainted.to_json()
    except ValueError as exc:
        print("NaN json   :", str(exc)[:60])

    # ---- from_dict error wrapping ------------------------------ #
    for bad in (
        {"strict": "yes"},
        {"config": {"max_history": "abc"}},
        {"execution_history": "not-a-list"},
        {"execution_history": [{"mode": "eco"}]},  # missing fields
    ):
        try:
            PolicyEvaluationEnvironment.from_dict(bad)
        except PolicyEvaluatorError as exc:
            print("Rejected p :", str(exc)[:60])

    # ---- Context manager (previously broken) ------------------- #
    with PolicyEvaluationEnvironment() as scoped:
        scoped.evaluate_policy(_dummy_policy, tasks[:2])
        assert scoped.history_size == 1
    print("context    : OK (previously broken)")

    # ---- Reset -------------------------------------------------- #
    print(
        "reset      :", env.reset(),
        "records removed",
    )
    assert env.history_size == 0

    print("\nSmoke test passed.")
