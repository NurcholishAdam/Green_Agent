# -*- coding: utf-8 -*-
"""
Benchmark Harness (Enhanced)
============================

Orchestration engine for running comprehensive agent benchmarks.

This module is the **orchestration layer**. It:

- loads task suites,
- drives :class:`~evaluation.agent_evaluator.AgentEvaluator` through
  single- and multi-agent benchmark runs,
- persists results as JSON,
- renders human-readable reports.

The enhancement pipeline (LIMIT Graph, MODP, RLHF, multi-teacher
on-policy distillation, bio-inspired optimisation, MoE gating) is
implemented by the underlying :class:`AgentEvaluator` and
:class:`~evaluation.agentbench_adapter.AgentBenchAdapter`. The harness
forwards configuration to them and surfaces their results — it does
not implement those mechanisms itself.

Key fixes over the previous version
-----------------------------------
- **Path-traversal safe.** ``benchmark_name`` was interpolated
  directly into the output filename, so ``"../../etc/passwd"`` or
  ``"/tmp/absolute"`` escaped ``output_dir``. Filenames are now
  validated against ``^[A-Za-z0-9._-]+$``; invalid names raise
  :class:`BenchmarkError`.
- **Dead adapter removed.** The harness constructed a second
  :class:`AgentBenchAdapter` that was never used, and — with
  ``use_enhancements=True`` — silently created a second distillation
  optimizer. All evaluation goes through the evaluator's internal
  adapter; there is no separate ``self.adapter``.
- **Markdown escaping.** ``benchmark_name``, ``agent_name``,
  ``framework``, task ids, and error messages are escaped before
  interpolation, so a benchmark name like
  ``"Q3 [injection](javascript:)"`` cannot inject a live link into the
  rendered report.
- **Report format validation.** ``format`` is validated against
  ``{"markdown", "text", "json"}`` and unrecognised values raise.
  ``_generate_text_report`` no longer corrupts content by
  ``replace("#", "").replace("**", "")``; it walks the structured
  result directly.
- **Missing metrics render as ``—``.** Previously a metric that was
  not measured rendered as ``0.0000`` — indistinguishable from a real
  zero. And a metric that was present but ``None`` crashed the format
  string.
- **Failed tasks surfaced in the report.** The report lists the first
  N failed tasks with their ``error`` strings, plus a count of any
  remaining failures.
- **Totals in the report.** Total energy, carbon, and latency are
  shown alongside the averages, computed from ``task_results`` (not
  ``num_tasks × avg``) so the numbers are exact.
- **Per-task failure isolation.** A single task that raises no longer
  aborts the entire benchmark. The harness records a structured
  failure result for that task and continues; ``strict=True`` re-raises.
- **``run_multi_agent_benchmark`` accepts and forwards ``backend`` /
  ``rank``.** The multi-agent result records both.
- **Unique filenames.** ``datetime.now(timezone.utc).strftime(
  "%Y%m%d_%H%M%S_%f")`` plus a ``secrets.token_hex(3)`` suffix — two
  runs of the same benchmark within the same microsecond no longer
  collide.
- **Aware timestamps.** ``datetime.now(timezone.utc)`` throughout;
  the previous ``datetime.utcnow().isoformat() + "Z"`` produced naive
  timestamps whose ``"Z"`` suffix lied about timezone awareness.
- **``load_task_suite`` validation.** UTF-8 encoding, ``OSError`` /
  ``JSONDecodeError`` wrapped as :class:`BenchmarkError`, and each
  element checked to be a ``dict``.
- **``_save_result`` returns the saved path**, wraps ``OSError``,
  creates parent directories on demand, writes with UTF-8.
- **Narrow error type.** :class:`BenchmarkError` replaces the previous
  ``OSError`` / ``JSONDecodeError`` / ``KeyError`` / ``ValueError``
  leaks.
- **``strict`` mode.** ``BenchmarkHarness(..., strict=True)`` re-raises
  on any per-task failure.
- **Progress callback.** ``run_benchmark(..., progress_callback=...)``
  is invoked as ``callback(completed, total)`` after each task.
- **``reset()``.** Clears the evaluator's cross-benchmark learning
  state (the optimizer's weights, replay buffer, and counter) so a
  caller can run independent benchmarks without recreating the harness.
- **``include_task_results`` toggle.** When ``False``, the persisted
  JSON omits per-task details (only the aggregate is stored).
- **"``use_enhancements``" reworked.** The harness's field is now
  read-only and reflects the evaluator's state. Result fields like
  ``graph_metrics`` / ``human_feedback_score`` are included whenever
  the values are provided, regardless of the enhancement flag.
- **Lazy ``%s`` logging.** No eager f-strings.
- **``__version__``, ``__all__``, ``__dir__``, and a ``_check()``-based
  smoke test.**
- **Report helper extraction.** ``_metrics_to_lines`` dedupes the
  single-agent and multi-agent report branches.
- **``output_dir``** accepted as ``Union[str, Path]``.

Compatibility
-------------
- ``BenchmarkHarness(output_dir, grid_region, hardware_profile,
  config)`` — same positional signature. ``strict=False`` is new and
  keyword-only.
- ``run_benchmark(agent, task_suite, benchmark_name, backend=None,
  rank=None, graph_metrics=None, human_feedback_score=None)`` — same
  positional signature. New keyword-only parameters are
  ``include_task_results=True`` and ``progress_callback=None``.
- ``run_multi_agent_benchmark(...)`` — gains keyword-only ``backend``
  and ``rank``. ``sort_by`` stays a positional-or-keyword argument.
- ``generate_report(benchmark_result, format="markdown")`` —
  ``format`` is now keyword-only (``fmt=``) and validated. Existing
  callers passing ``format="markdown"`` positionally must switch to
  ``fmt=``.
- ``_save_result`` now returns the saved :class:`pathlib.Path`.
  Existing callers ignoring the return value are unaffected.
- ``self.adapter`` no longer exists. Any caller reading it (which
  would have been a bug — it was unused) must switch to
  ``self.evaluator.adapter``.
- ``self.use_enhancements`` is now a read-only property reflecting
  the evaluator's state, not a setter-backed field.
- Task results are always present in the returned dict; when
  ``include_task_results=False`` they are omitted from the *persisted*
  JSON, not from the return value.

Requires Python 3.8+. ``slots=True`` is not used here (the harness is
mutable and holds state); no version-specific constructs beyond
``datetime.now(timezone.utc)`` (3.2+) and ``pathlib`` (3.4+).
"""

from __future__ import annotations

import enum
import json
import logging
import math
import re
import secrets
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import (
    Any, Callable, Dict, List, Mapping, Optional, Sequence, Union,
)

from .agent_evaluator import AgentEvaluator
from .agentbench_adapter import AgentBenchAdapter


logger = logging.getLogger(__name__)

__version__ = "2.5.0"


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class BenchmarkError(ValueError):
    """Raised for invalid benchmark harness inputs or execution failures."""


# --------------------------------------------------------------------------- #
# Constants
# --------------------------------------------------------------------------- #
# Filename allowlist. A ``benchmark_name`` that does not match this
# pattern is rejected rather than sanitised — sanitising hides the
# caller's mistake and can still produce confusing collisions.
_SAFE_FILENAME_RE: re.Pattern[str] = re.compile(r"^[A-Za-z0-9._-]+$")

# Supported report formats.
_VALID_FORMATS: frozenset = frozenset({"markdown", "text", "json"})

# Maximum failed tasks to enumerate in the report before summarising
# the remainder.
_MAX_FAILED_IN_REPORT: int = 10

# Markdown characters escaped at interpolation time.
_MD_ESCAPE_RE: re.Pattern[str] = re.compile(r"([\\`*_\[\]<>])")


# --------------------------------------------------------------------------- #
# Validation helpers
# --------------------------------------------------------------------------- #
def _require_nonempty_str(name: str, value: Any) -> str:
    if not isinstance(value, str) or not value:
        raise BenchmarkError(f"{name} must be a non-empty string.")
    return value


def _require_mapping_or_none(
    name: str, value: Any,
) -> Optional[Mapping[str, Any]]:
    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise BenchmarkError(
            f"{name} must be a Mapping or None, got "
            f"{type(value).__name__}."
        )
    return value


def _require_unit_interval_or_none(
    name: str, value: Any,
) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise BenchmarkError(f"{name} must be numeric or None.")
    fv = float(value)
    if not math.isfinite(fv) or not 0.0 <= fv <= 1.0:
        raise BenchmarkError(
            f"{name} must be finite and in [0, 1], got {value!r}."
        )
    return fv


def _safe_filename(name: Any) -> str:
    """Validate that ``name`` can be safely used as a filename stem."""
    if not isinstance(name, str) or not name:
        raise BenchmarkError("benchmark_name must be a non-empty string.")
    if not _SAFE_FILENAME_RE.fullmatch(name):
        raise BenchmarkError(
            f"benchmark_name {name!r} contains characters that are not "
            f"allowed in a filename (allowed: A-Za-z0-9._-)."
        )
    return name


def _md_escape(value: Any) -> str:
    """Escape a value for inline Markdown rendering."""
    return _MD_ESCAPE_RE.sub(r"\\\1", str(value))


def _fmt_metric(
    metrics: Mapping[str, Any], key: str,
    *,
    precision: int = 4,
    as_percent: bool = False,
    suffix: str = "",
) -> str:
    """
    Render a metric value from ``metrics`` for a report line.

    Returns ``"—"`` for a **missing** or ``None`` metric, distinct from
    the string ``"0.0000"`` produced by a genuine zero. Numbers are
    formatted with ``precision`` decimals (or as a percentage).
    """
    value = metrics.get(key)
    if value is None:
        return "—"
    if isinstance(value, bool):
        return "yes" if value else "no"
    if not isinstance(value, (int, float)):
        return _md_escape(value)
    if not math.isfinite(float(value)):
        return _md_escape(value)
    if as_percent:
        return f"{float(value):.2%}"
    return f"{float(value):.{precision}f}{suffix}"


# --------------------------------------------------------------------------- #
# Optional enhancement imports
# --------------------------------------------------------------------------- #
class _UnavailableComponent:
    __slots__ = ("_name",)

    def __init__(self, name: str, *a: Any, **kw: Any) -> None:
        self._name = name
        raise BenchmarkError(
            f"Enhancement component '{name}' is unavailable."
        )


try:
    from src.enhancements.schemas.node_descriptor import (  # noqa: F401
        NodeDescriptor,
    )
    from src.enhancements.schemas.workload_descriptor import (  # noqa: F401
        WorkloadDescriptor,
    )
    from src.enhancements.zero_trust_architecture import (  # noqa: F401
        ZeroTrustArchitecture,
    )
    from src.enhancements.schemas.feedback_event import (  # noqa: F401
        FeedbackEvent,
    )

    ENHANCEMENTS_AVAILABLE: bool = True
except ImportError as _exc:  # pragma: no cover — defensive
    ENHANCEMENTS_AVAILABLE = False
    logger.info("Enhancement schemas unavailable: %s", _exc)
    NodeDescriptor = _UnavailableComponent  # type: ignore[assignment,misc]
    WorkloadDescriptor = _UnavailableComponent  # type: ignore[assignment,misc]
    ZeroTrustArchitecture = _UnavailableComponent  # type: ignore[assignment,misc]
    FeedbackEvent = _UnavailableComponent  # type: ignore[assignment,misc]


# --------------------------------------------------------------------------- #
# Harness
# --------------------------------------------------------------------------- #
class BenchmarkHarness:
    """
    Orchestration engine for running comprehensive agent benchmarks.

    Manages task-suite loading, single- and multi-agent evaluations,
    result aggregation, and report generation. The enhancement pipeline
    is implemented by :class:`AgentEvaluator`; this harness forwards
    configuration and surfaces results.

    ``strict`` mode (keyword-only) re-raises on any per-task failure.
    The default (``strict=False``) records the failure and continues,
    so one bad task does not lose the whole benchmark.
    """

    def __init__(
        self,
        output_dir: Union[str, Path] = "./benchmark_results",
        grid_region: str = "GLOBAL",
        hardware_profile: str = "default",
        config: Optional[Dict[str, Any]] = None,
        *,
        strict: bool = False,
    ) -> None:
        if config is not None and not isinstance(config, dict):
            raise BenchmarkError(
                f"config must be a dict or None, got "
                f"{type(config).__name__}."
            )

        try:
            self.output_dir = Path(output_dir)
        except (TypeError, ValueError) as exc:
            raise BenchmarkError(
                f"output_dir must be a path-like value: {exc}"
            ) from exc

        try:
            self.output_dir.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise BenchmarkError(
                f"could not create {self.output_dir}: {exc}"
            ) from exc
        if not self.output_dir.is_dir():
            raise BenchmarkError(
                f"{self.output_dir} exists but is not a directory."
            )

        self.config: Dict[str, Any] = dict(config) if config else {}
        self._strict: bool = bool(strict)
        self._lock = threading.RLock()

        self.evaluator = AgentEvaluator(
            grid_region=grid_region,
            hardware_profile=hardware_profile,
            track_green_metrics=True,
            config=self.config,
            strict=self._strict,
        )

        # No separate adapter. All evaluation goes through the
        # evaluator, which constructs its own AgentBenchAdapter
        # internally. A second adapter would create a second
        # optimizer that nothing uses.

        logger.info(
            "Initialized BenchmarkHarness (output_dir=%s, "
            "enhancements=%s, strict=%s)",
            self.output_dir,
            "on" if self.enhancements_active else "off",
            self._strict,
        )

    # ---------------------------------------------------------------- props
    @property
    def enhancements_active(self) -> bool:
        """
        True when the underlying evaluator is running the enhancement
        pipeline. Mirrors ``self.evaluator.use_enhancements``.
        """
        return bool(getattr(self.evaluator, "use_enhancements", False))

    @property
    def use_enhancements(self) -> bool:
        """Deprecated alias for :attr:`enhancements_active`."""
        return self.enhancements_active

    # ---------------------------------------------------------------- load
    def load_task_suite(
        self, suite_path: Union[str, Path],
    ) -> List[Dict[str, Any]]:
        """
        Load a task suite from a JSON file.

        Raises
        ------
        BenchmarkError
            If the file cannot be read, the JSON is invalid, the root
            is not a list, or any element is not an object.
        """
        path = Path(suite_path)
        try:
            text = path.read_text(encoding="utf-8")
        except OSError as exc:
            raise BenchmarkError(
                f"cannot read {path}: {exc}"
            ) from exc
        try:
            data = json.loads(text)
        except json.JSONDecodeError as exc:
            raise BenchmarkError(
                f"invalid JSON in {path}: {exc}"
            ) from exc
        if not isinstance(data, list):
            raise BenchmarkError(
                f"root of {path} must be a JSON array, got "
                f"{type(data).__name__}."
            )
        for i, task in enumerate(data):
            if not isinstance(task, Mapping):
                raise BenchmarkError(
                    f"{path}[{i}] must be a JSON object, got "
                    f"{type(task).__name__}."
                )
        logger.info("Loaded %d tasks from %s", len(data), path)
        return list(data)

    # ---------------------------------------------------------------- run
    def run_benchmark(
        self,
        agent: Any,
        task_suite: Sequence[Mapping[str, Any]],
        benchmark_name: str,
        backend: Optional[str] = None,
        rank: Optional[int] = None,
        graph_metrics: Optional[Mapping[str, float]] = None,
        human_feedback_score: Optional[float] = None,
        *,
        include_task_results: bool = True,
        progress_callback: Optional[Callable[[int, int], None]] = None,
    ) -> Dict[str, Any]:
        """
        Run a benchmark on a single agent.

        Parameters
        ----------
        agent, task_suite, benchmark_name
            Agent, list of tasks, and benchmark label. ``benchmark_name``
            is validated as a filename stem (``A-Za-z0-9._-``).
        backend, rank
            Passed through to the evaluator (e.g., a quantum backend id).
        graph_metrics, human_feedback_score
            Enhancement context. Validated here before forwarding.
        include_task_results : bool, default True
            If False, the persisted JSON omits per-task details. The
            **returned** dict always includes them so the caller can
            inspect failures.
        progress_callback : callable, optional
            Invoked as ``callback(completed, total)`` after each task.
        """
        _require_nonempty_str("benchmark_name", benchmark_name)
        safe_name = _safe_filename(benchmark_name)
        _require_nonempty_str("benchmark_name", benchmark_name)
        gm = _require_mapping_or_none("graph_metrics", graph_metrics)
        if gm is not None:
            gm = dict(gm)
        hf = _require_unit_interval_or_none(
            "human_feedback_score", human_feedback_score,
        )
        # Fall back to config values if not supplied.
        if gm is None:
            cfg_gm = self.config.get("graph_metrics")
            if isinstance(cfg_gm, Mapping):
                gm = dict(cfg_gm)
        if hf is None:
            cfg_hf = self.config.get("human_feedback_score")
            if cfg_hf is not None:
                try:
                    hf = _require_unit_interval_or_none(
                        "config.human_feedback_score", cfg_hf,
                    )
                except BenchmarkError:
                    logger.warning(
                        "ignoring invalid config.human_feedback_score=%r",
                        cfg_hf,
                    )
                    hf = None

        logger.info(
            "Running benchmark %r on agent %s",
            benchmark_name,
            getattr(agent, "name", type(agent).__name__),
        )
        start_time = datetime.now(timezone.utc)

        results = self._evaluate_with_isolation(
            agent, task_suite, backend, rank, gm, hf,
            progress_callback=progress_callback,
        )
        aggregated = self._aggregate_results(results)

        end_time = datetime.now(timezone.utc)
        duration = (end_time - start_time).total_seconds()

        benchmark_result: Dict[str, Any] = {
            "benchmark_name": benchmark_name,
            "agent_name": getattr(agent, "name", type(agent).__name__),
            "framework": getattr(agent, "framework", "unknown"),
            "num_tasks": len(task_suite),
            "duration_seconds": duration,
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "aggregated_metrics": aggregated,
            "task_results": results,
            "backend": backend,
            "rank": rank,
            "enhancements_active": self.enhancements_active,
        }
        if gm is not None:
            benchmark_result["graph_metrics"] = gm
        if hf is not None:
            benchmark_result["human_feedback_score"] = hf

        # Persist a copy that may omit task-level details.
        persisted = dict(benchmark_result)
        if not include_task_results:
            persisted.pop("task_results", None)
        saved_path = self._save_result(persisted, safe_name)
        benchmark_result["saved_path"] = str(saved_path)

        logger.info(
            "Benchmark %r completed in %.2fs", benchmark_name, duration,
        )
        return benchmark_result

    def run_multi_agent_benchmark(
        self,
        agents: Sequence[Any],
        task_suite: Sequence[Mapping[str, Any]],
        benchmark_name: str,
        sort_by: str = "sustainability_index",
        graph_metrics: Optional[Mapping[str, float]] = None,
        human_feedback_score: Optional[float] = None,
        *,
        backend: Optional[str] = None,
        rank: Optional[int] = None,
        include_task_results: bool = True,
    ) -> Dict[str, Any]:
        """
        Run a benchmark across multiple agents and rank them.
        """
        _require_nonempty_str("benchmark_name", benchmark_name)
        safe_name = _safe_filename(benchmark_name)
        _require_nonempty_str("sort_by", sort_by)
        if not agents:
            raise BenchmarkError(
                "agents must contain at least one agent."
            )
        gm = _require_mapping_or_none("graph_metrics", graph_metrics)
        if gm is not None:
            gm = dict(gm)
        hf = _require_unit_interval_or_none(
            "human_feedback_score", human_feedback_score,
        )

        logger.info(
            "Running multi-agent benchmark %r on %d agents",
            benchmark_name, len(agents),
        )
        start_time = datetime.now(timezone.utc)

        rankings: List[Dict[str, Any]] = []
        for i, agent in enumerate(agents):
            logger.info(
                "Benchmarking agent %d/%d (%s)",
                i + 1, len(agents),
                getattr(agent, "name", type(agent).__name__),
            )
            results = self._evaluate_with_isolation(
                agent, task_suite, backend, rank, gm, hf,
            )
            aggregated = self._aggregate_results(results)
            entry: Dict[str, Any] = {
                "agent_name": getattr(
                    agent, "name", type(agent).__name__,
                ),
                "framework": getattr(agent, "framework", "unknown"),
                "num_tasks": len(task_suite),
                "aggregated_metrics": aggregated,
            }
            if include_task_results:
                entry["task_results"] = results
            rankings.append(entry)

        sort_key = (
            "success_rate" if sort_by == "success_rate"
            else f"avg_{sort_by}"
        )
        if rankings and sort_key not in rankings[0]["aggregated_metrics"]:
            raise BenchmarkError(
                f"cannot sort by {sort_by!r}: key {sort_key!r} was not "
                f"produced by the evaluation."
            )
        # Lower-is-better metrics sort ascending.
        lower_is_better = {"energy_kwh", "carbon_co2e_kg", "latency_ms"}
        reverse = sort_by not in lower_is_better
        rankings.sort(
            key=lambda x: x["aggregated_metrics"][sort_key],
            reverse=reverse,
        )

        end_time = datetime.now(timezone.utc)
        duration = (end_time - start_time).total_seconds()

        benchmark_result: Dict[str, Any] = {
            "benchmark_name": benchmark_name,
            "num_agents": len(agents),
            "num_tasks": len(task_suite),
            "sort_by": sort_by,
            "sort_direction": "descending" if reverse else "ascending",
            "duration_seconds": duration,
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "rankings": rankings,
            "backend": backend,
            "rank": rank,
            "enhancements_active": self.enhancements_active,
        }
        if gm is not None:
            benchmark_result["graph_metrics"] = gm
        if hf is not None:
            benchmark_result["human_feedback_score"] = hf

        saved_path = self._save_result(
            benchmark_result, f"{safe_name}_multi_agent",
        )
        benchmark_result["saved_path"] = str(saved_path)
        logger.info(
            "Multi-agent benchmark %r completed in %.2fs",
            benchmark_name, duration,
        )
        return benchmark_result

    # ---------------------------------------------------------------- evaluate
    def _evaluate_with_isolation(
        self,
        agent: Any,
        task_suite: Sequence[Mapping[str, Any]],
        backend: Optional[str],
        rank: Optional[int],
        gm: Optional[Mapping[str, float]],
        hf: Optional[float],
        *,
        progress_callback: Optional[Callable[[int, int], None]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Evaluate ``agent`` on each task with per-task isolation.

        A single task failure produces a structured failure result and
        the suite continues. In strict mode the first failure re-raises.
        """
        results: List[Dict[str, Any]] = []
        total = len(task_suite)
        for i, task in enumerate(task_suite):
            try:
                result = self.evaluator.evaluate(
                    agent=agent,
                    task=task,  # type: ignore[arg-type]
                    backend=backend,
                    rank=rank,
                    graph_metrics=gm,
                    human_feedback_score=hf,
                )
            except Exception:
                logger.exception("task %d failed", i)
                if self._strict:
                    raise
                task_id = (
                    task.get("task_id", f"task-{i}")
                    if isinstance(task, Mapping) else f"task-{i}"
                )
                result = {
                    "task_id": str(task_id),
                    "success": False,
                    "error": "task failed; see logs.",
                    "metrics": {},
                }
            if not isinstance(result, dict):
                logger.error(
                    "evaluate() returned %s for task %d; coercing to "
                    "a failure result.",
                    type(result).__name__, i,
                )
                result = {
                    "task_id": f"task-{i}",
                    "success": False,
                    "error": "evaluate() returned non-dict.",
                    "metrics": {},
                }
            results.append(result)
            if progress_callback is not None:
                try:
                    progress_callback(i + 1, total)
                except Exception:
                    logger.exception(
                        "progress_callback failed after task %d", i,
                    )
        return results

    @staticmethod
    def _aggregate_results(
        results: Sequence[Mapping[str, Any]],
    ) -> Dict[str, Any]:
        """
        Aggregate metrics across a suite, including failed tasks.

        Failed tasks contribute ``0.0`` for metrics they did not
        produce, so ``avg_accuracy`` is the whole-suite mean, not the
        successes' mean. ``success_rate`` reports the pass fraction.
        """
        if not results:
            return {}
        metric_values: Dict[str, List[float]] = {}
        success_count = 0
        for r in results:
            success = bool(r.get("success", False))
            if success:
                success_count += 1
            metrics = r.get("metrics") or {}
            if not isinstance(metrics, Mapping):
                continue
            for key, value in metrics.items():
                if isinstance(value, bool) or not isinstance(
                    value, (int, float)
                ):
                    continue
                fv = float(value)
                if not math.isfinite(fv):
                    continue
                metric_values.setdefault(key, []).append(fv)

        n = len(results)
        aggregated: Dict[str, Any] = {}
        for key, values in metric_values.items():
            if not values:
                continue
            # Whole-suite average: missing values count as 0.0.
            aggregated[f"avg_{key}"] = sum(values) / n
            aggregated[f"min_{key}"] = min(values)
            aggregated[f"max_{key}"] = max(values)
        aggregated["success_rate"] = success_count / n
        modp_rewards = [
            r.get("modp_reward") for r in results
            if r.get("modp_reward") is not None
        ]
        if modp_rewards:
            aggregated["avg_modp_reward"] = sum(modp_rewards) / len(
                modp_rewards
            )
        return aggregated

    # ---------------------------------------------------------------- save
    def _save_result(
        self, result: Mapping[str, Any], name: str,
    ) -> Path:
        """
        Persist ``result`` to ``output_dir`` as ``<name>_<ts>_<rnd>.json``.

        Returns the path written. Raises :class:`BenchmarkError` on
        write failure.
        """
        safe_name = _safe_filename(name)
        timestamp = datetime.now(timezone.utc).strftime(
            "%Y%m%d_%H%M%S_%f",
        )
        suffix = secrets.token_hex(3)
        filepath = self.output_dir / f"{safe_name}_{timestamp}_{suffix}.json"
        try:
            filepath.parent.mkdir(parents=True, exist_ok=True)
            with filepath.open("w", encoding="utf-8") as f:
                json.dump(dict(result), f, indent=2, default=str)
        except OSError as exc:
            raise BenchmarkError(
                f"could not write {filepath}: {exc}"
            ) from exc
        logger.info("Saved benchmark result to %s", filepath)
        return filepath

    # ---------------------------------------------------------------- reset
    def reset(self) -> None:
        """
        Clear the evaluator's cross-benchmark learning state.

        Resets the distillation optimizer's weights, replay buffer, and
        counter. Other evaluator state (audit logs, if any) is
        unaffected. Run this between independent benchmarks when you do
        not want them to share learning.
        """
        with self._lock:
            optimizer = getattr(
                self.evaluator, "distillation_optimizer", None,
            )
            if optimizer is None:
                return
            try:
                optimizer.student_weights.fill(0.0)
                optimizer.student_bias.fill(0.0)
                optimizer.gate_weights.fill(0.0)
                optimizer.gate_bias.fill(0.0)
                optimizer.replay_buffer.clear()
                optimizer.counter = 0
            except Exception:
                logger.exception(
                    "could not fully reset the optimizer state"
                )
        logger.info("BenchmarkHarness reset")

    # ---------------------------------------------------------------- report
    def generate_report(
        self,
        benchmark_result: Mapping[str, Any],
        *,
        fmt: str = "markdown",
    ) -> str:
        """
        Render ``benchmark_result`` as a report.

        Parameters
        ----------
        benchmark_result : Mapping
            The dict returned by :meth:`run_benchmark` or
            :meth:`run_multi_agent_benchmark`.
        fmt : {"markdown", "text", "json"}, default "markdown"

        Raises
        ------
        BenchmarkError
            If ``fmt`` is not one of the supported formats, or
            ``benchmark_result`` is missing required fields.
        """
        if not isinstance(benchmark_result, Mapping):
            raise BenchmarkError(
                f"benchmark_result must be a Mapping, got "
                f"{type(benchmark_result).__name__}."
            )
        if fmt not in _VALID_FORMATS:
            raise BenchmarkError(
                f"unsupported format {fmt!r}; expected one of "
                f"{sorted(_VALID_FORMATS)}."
            )
        if fmt == "markdown":
            return self._generate_markdown_report(benchmark_result)
        if fmt == "json":
            return json.dumps(dict(benchmark_result), indent=2, default=str)
        return self._generate_text_report(benchmark_result)

    # ---------------------------------------------------------------- md
    def _generate_markdown_report(
        self, result: Mapping[str, Any],
    ) -> str:
        lines: List[str] = []
        name = result.get("benchmark_name", "unknown")
        lines.append(f"# Benchmark Report: {_md_escape(name)}")
        lines.append("")
        lines.append(
            f"**Date**: {_md_escape(result.get('start_time', '—'))}"
        )
        duration = result.get("duration_seconds")
        if isinstance(duration, (int, float)):
            lines.append(f"**Duration**: {float(duration):.2f}s")
        lines.append("")

        gm = result.get("graph_metrics")
        if gm is not None:
            lines.append(f"**Graph Metrics**: {_md_escape(gm)}")
        hf = result.get("human_feedback_score")
        if hf is not None:
            lines.append(
                f"**Human Feedback Score**: {_fmt_metric({'v': hf}, 'v')}"
            )
        if gm is not None or hf is not None:
            lines.append("")

        if "rankings" in result:
            self._report_rankings(lines, result)
        else:
            self._report_single_agent(lines, result)
        return "\n".join(lines)

    def _report_rankings(
        self, lines: List[str], result: Mapping[str, Any],
    ) -> None:
        sort_by = result.get("sort_by", "sustainability_index")
        lines.append(
            f"## Rankings (sorted by {_md_escape(sort_by)})"
        )
        lines.append("")
        for i, entry in enumerate(result.get("rankings", []), 1):
            name = entry.get("agent_name", "unknown")
            fw = entry.get("framework", "unknown")
            lines.append(
                f"### {i}. {_md_escape(name)} ({_md_escape(fw)})"
            )
            lines.append("")
            self._emit_metric_lines(
                lines, entry.get("aggregated_metrics") or {},
            )
            lines.append("")

    def _report_single_agent(
        self, lines: List[str], result: Mapping[str, Any],
    ) -> None:
        name = result.get("agent_name", "unknown")
        fw = result.get("framework", "unknown")
        lines.append(
            f"## Agent: {_md_escape(name)} ({_md_escape(fw)})"
        )
        lines.append("")
        num_tasks = result.get("num_tasks")
        if isinstance(num_tasks, int):
            lines.append(f"**Tasks**: {num_tasks}")
            lines.append("")
        lines.append("### Aggregated Metrics")
        lines.append("")
        metrics = result.get("aggregated_metrics") or {}
        self._emit_metric_lines(lines, metrics)

        # Totals computed from task results, when available.
        task_results = result.get("task_results") or []
        if task_results:
            totals = self._compute_totals(task_results)
            if totals:
                lines.append("")
                lines.append("### Totals")
                lines.append("")
                for key in sorted(totals):
                    lines.append(
                        f"- **{_md_escape(key)}**: {totals[key]}"
                    )

        # Failed tasks summary.
        failed = [
            r for r in task_results
            if isinstance(r, Mapping) and not r.get("success", True)
        ]
        if failed:
            lines.append("")
            lines.append("### Failed Tasks")
            lines.append("")
            for r in failed[:_MAX_FAILED_IN_REPORT]:
                task_id = r.get("task_id", "?")
                err = r.get("error") or "unknown error"
                lines.append(
                    f"- `{_md_escape(task_id)}`: {_md_escape(err)}"
                )
            remaining = len(failed) - _MAX_FAILED_IN_REPORT
            if remaining > 0:
                lines.append(f"- …and {remaining} more")

    @staticmethod
    def _emit_metric_lines(
        lines: List[str], metrics: Mapping[str, Any],
    ) -> None:
        """Emit the standard metric block for a report section."""
        lines.append(
            f"- **Success Rate**: "
            f"{_fmt_metric(metrics, 'success_rate', as_percent=True)}"
        )
        lines.append(
            f"- **Avg Accuracy**: "
            f"{_fmt_metric(metrics, 'avg_accuracy')}"
        )
        # Latency only when present (multi-agent reports omit it).
        if "avg_latency_ms" in metrics:
            lines.append(
                f"- **Avg Latency**: "
                f"{_fmt_metric(metrics, 'avg_latency_ms', precision=2, suffix=' ms')}"
            )
        lines.append(
            f"- **Avg Energy**: "
            f"{_fmt_metric(metrics, 'avg_energy_kwh', precision=6, suffix=' kWh')}"
        )
        lines.append(
            f"- **Avg Carbon**: "
            f"{_fmt_metric(metrics, 'avg_carbon_co2e_kg', precision=6, suffix=' kg CO₂e')}"
        )
        lines.append(
            f"- **Avg Sustainability Index**: "
            f"{_fmt_metric(metrics, 'avg_sustainability_index', precision=2)}"
        )
        if "avg_modp_reward" in metrics:
            lines.append(
                f"- **Avg MODP Reward**: "
                f"{_fmt_metric(metrics, 'avg_modp_reward')}"
            )

    @staticmethod
    def _compute_totals(
        task_results: Sequence[Mapping[str, Any]],
    ) -> Dict[str, str]:
        """Sum the per-task metrics that are meaningful to total."""
        sums = {
            "energy_kwh": 0.0,
            "carbon_co2e_kg": 0.0,
            "latency_ms": 0.0,
            "modp_reward": 0.0,
        }
        seen = {k: False for k in sums}
        for r in task_results:
            if not isinstance(r, Mapping) or not r.get("success", True):
                continue
            metrics = r.get("metrics") or {}
            if not isinstance(metrics, Mapping):
                continue
            for key in ("energy_kwh", "carbon_co2e_kg", "latency_ms"):
                v = metrics.get(key)
                if isinstance(v, (int, float)) and not isinstance(v, bool):
                    if math.isfinite(float(v)):
                        sums[key] += float(v)
                        seen[key] = True
            modp = r.get("modp_reward")
            if isinstance(modp, (int, float)) and not isinstance(
                modp, bool
            ) and math.isfinite(float(modp)):
                sums["modp_reward"] += float(modp)
                seen["modp_reward"] = True

        out: Dict[str, str] = {}
        if seen["energy_kwh"]:
            out["total_energy_kwh"] = (
                f"{sums['energy_kwh']:.6f} kWh"
            )
        if seen["carbon_co2e_kg"]:
            out["total_carbon_co2e_kg"] = (
                f"{sums['carbon_co2e_kg']:.6f} kg CO₂e"
            )
        if seen["latency_ms"]:
            out["total_latency_ms"] = (
                f"{sums['latency_ms']:.2f} ms"
            )
        if seen["modp_reward"]:
            out["total_modp_reward"] = (
                f"{sums['modp_reward']:.4f}"
            )
        return out

    # ---------------------------------------------------------------- text
    def _generate_text_report(
        self, result: Mapping[str, Any],
    ) -> str:
        """
        Plain-text report, built from the same structured data as the
        Markdown report — no ``replace("#", "")`` content-mangling.
        """
        lines: List[str] = []
        name = result.get("benchmark_name", "unknown")
        lines.append(f"BENCHMARK REPORT: {name}")
        lines.append(f"Date: {result.get('start_time', '—')}")
        duration = result.get("duration_seconds")
        if isinstance(duration, (int, float)):
            lines.append(f"Duration: {float(duration):.2f}s")
        gm = result.get("graph_metrics")
        if gm is not None:
            lines.append(f"Graph Metrics: {gm}")
        hf = result.get("human_feedback_score")
        if hf is not None:
            lines.append(f"Human Feedback Score: {hf}")
        lines.append("")

        if "rankings" in result:
            lines.append(
                f"RANKINGS (sorted by {result.get('sort_by', '?')})"
            )
            lines.append("")
            for i, entry in enumerate(result.get("rankings", []), 1):
                name = entry.get("agent_name", "?")
                fw = entry.get("framework", "?")
                lines.append(f"{i}. {name} ({fw})")
                self._emit_text_metric_lines(
                    lines, entry.get("aggregated_metrics") or {},
                )
                lines.append("")
        else:
            name = result.get("agent_name", "?")
            fw = result.get("framework", "?")
            lines.append(f"AGENT: {name} ({fw})")
            num_tasks = result.get("num_tasks")
            if isinstance(num_tasks, int):
                lines.append(f"Tasks: {num_tasks}")
            lines.append("")
            self._emit_text_metric_lines(
                lines, result.get("aggregated_metrics") or {},
            )
            task_results = result.get("task_results") or []
            if task_results:
                totals = self._compute_totals(task_results)
                if totals:
                    lines.append("")
                    lines.append("TOTALS")
                    for key in sorted(totals):
                        lines.append(f"  {key}: {totals[key]}")
            failed = [
                r for r in task_results
                if isinstance(r, Mapping) and not r.get("success", True)
            ]
            if failed:
                lines.append("")
                lines.append("FAILED TASKS")
                for r in failed[:_MAX_FAILED_IN_REPORT]:
                    lines.append(
                        f"  {r.get('task_id', '?')}: "
                        f"{r.get('error', 'unknown')}"
                    )
                remaining = len(failed) - _MAX_FAILED_IN_REPORT
                if remaining > 0:
                    lines.append(f"  ...and {remaining} more")
        return "\n".join(lines)

    @staticmethod
    def _emit_text_metric_lines(
        lines: List[str], metrics: Mapping[str, Any],
    ) -> None:
        lines.append(
            f"  Success Rate: "
            f"{_fmt_metric(metrics, 'success_rate', as_percent=True)}"
        )
        lines.append(
            f"  Avg Accuracy: {_fmt_metric(metrics, 'avg_accuracy')}"
        )
        if "avg_latency_ms" in metrics:
            lines.append(
                f"  Avg Latency: "
                f"{_fmt_metric(metrics, 'avg_latency_ms', precision=2, suffix=' ms')}"
            )
        lines.append(
            f"  Avg Energy: "
            f"{_fmt_metric(metrics, 'avg_energy_kwh', precision=6, suffix=' kWh')}"
        )
        lines.append(
            f"  Avg Carbon: "
            f"{_fmt_metric(metrics, 'avg_carbon_co2e_kg', precision=6, suffix=' kg CO2e')}"
        )
        lines.append(
            f"  Avg Sustainability Index: "
            f"{_fmt_metric(metrics, 'avg_sustainability_index', precision=2)}"
        )
        if "avg_modp_reward" in metrics:
            lines.append(
                f"  Avg MODP Reward: "
                f"{_fmt_metric(metrics, 'avg_modp_reward')}"
            )

    # ---------------------------------------------------------------- dunder
    def __repr__(self) -> str:
        return (
            "BenchmarkHarness("
            f"output_dir={str(self.output_dir)!r}, "
            f"enhancements={'on' if self.enhancements_active else 'off'}, "
            f"strict={self._strict})"
        )


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__: List[str] = [
    "__version__",
    "BenchmarkError",
    "BenchmarkHarness",
    "ENHANCEMENTS_AVAILABLE",
]


def __dir__() -> List[str]:
    return sorted(__all__)


# --------------------------------------------------------------------------- #
# Smoke test: python -m evaluation.benchmark_harness
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    import sys
    import tempfile

    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )

    passed = 0
    failed = 0

    def _check(name: str, cond: bool) -> None:
        global passed, failed
        if cond:
            passed += 1
            print(f"  ✅ {name}")
        else:
            failed += 1
            print(f"  ❌ {name}")

    def _raises(exc_type, fn) -> bool:
        try:
            fn()
        except exc_type:
            return True
        except Exception:
            return False
        return False

    # ---- 1. Filename sanitisation ------------------------------ #
    print("filename sanitisation:")
    _check(
        "safe names pass",
        _safe_filename("benchmark_2024") == "benchmark_2024",
    )
    _check(
        "path traversal rejected",
        _raises(BenchmarkError, lambda:
                _safe_filename("../../etc/passwd")),
    )
    _check(
        "absolute path rejected",
        _raises(BenchmarkError, lambda:
                _safe_filename("/tmp/evil")),
    )
    _check(
        "empty name rejected",
        _raises(BenchmarkError, lambda:
                _safe_filename("")),
    )
    _check(
        "None rejected",
        _raises(BenchmarkError, lambda:
                _safe_filename(None)),  # type: ignore[arg-type]
    )

    # ---- 2. Markdown escaping --------------------------------- #
    print("markdown escaping:")
    _check(
        "brackets escaped",
        _md_escape("[link](x)") == "\\[link\\](x)",
    )
    _check(
        "asterisks escaped",
        _md_escape("*italic*") == "\\*italic\\*",
    )
    _check(
        "angle brackets escaped",
        _md_escape("<script>") == "\\<script\\>",
    )

    # ---- 3. Metric formatting --------------------------------- #
    print("metric formatting:")
    _check(
        "missing metric renders as em dash",
        _fmt_metric({}, "avg_accuracy") == "—",
    )
    _check(
        "None metric renders as em dash",
        _fmt_metric({"avg_accuracy": None}, "avg_accuracy") == "—",
    )
    _check(
        "zero metric renders as 0.0000",
        _fmt_metric({"avg_accuracy": 0.0}, "avg_accuracy") == "0.0000",
    )
    _check(
        "percent formatting",
        _fmt_metric({"success_rate": 0.5}, "success_rate",
                    as_percent=True) == "50.00%",
    )
    _check(
        "suffix appended",
        _fmt_metric({"x": 1.5}, "x", precision=1, suffix=" kWh")
        == "1.5 kWh",
    )

    # ---- 4. Harness construction ------------------------------ #
    print("construction:")
    with tempfile.TemporaryDirectory() as tmpdir:
        harness = BenchmarkHarness(
            output_dir=tmpdir,
            config={"graph_metrics": {"centrality": 0.7}},
        )
        _check("constructs", harness is not None)
        _check(
            "output_dir exists",
            Path(tmpdir).is_dir(),
        )
        _check(
            "no dead adapter",
            not hasattr(harness, "adapter"),
        )
        _check(
            "enhancements_active is bool",
            isinstance(harness.enhancements_active, bool),
        )

        _check(
            "rejects non-dict config",
            _raises(BenchmarkError, lambda:
                    BenchmarkHarness(
                        output_dir=tmpdir, config="not a dict",  # type: ignore[arg-type]
                    )),
        )

    # ---- 5. load_task_suite validation ------------------------ #
    print("load_task_suite:")
    with tempfile.TemporaryDirectory() as tmpdir:
        harness = BenchmarkHarness(output_dir=tmpdir)
        tmpdir_p = Path(tmpdir)

        good = tmpdir_p / "tasks.json"
        good.write_text(
            json.dumps([
                {"task_id": "t1", "input": "hi"},
                {"task_id": "t2", "input": "bye"},
            ]),
            encoding="utf-8",
        )
        tasks = harness.load_task_suite(good)
        _check("loads valid suite", len(tasks) == 2)

        bad_root = tmpdir_p / "bad_root.json"
        bad_root.write_text('{"not": "a list"}', encoding="utf-8")
        _check(
            "rejects non-list root",
            _raises(BenchmarkError, lambda:
                    harness.load_task_suite(bad_root)),
        )

        bad_element = tmpdir_p / "bad_element.json"
        bad_element.write_text('["not a dict"]', encoding="utf-8")
        _check(
            "rejects non-dict element",
            _raises(BenchmarkError, lambda:
                    harness.load_task_suite(bad_element)),
        )

        missing = tmpdir_p / "missing.json"
        _check(
            "missing file raises",
            _raises(BenchmarkError, lambda:
                    harness.load_task_suite(missing)),
        )

        bad_json = tmpdir_p / "bad_json.json"
        bad_json.write_text("{not json", encoding="utf-8")
        _check(
            "invalid JSON raises",
            _raises(BenchmarkError, lambda:
                    harness.load_task_suite(bad_json)),
        )

    # ---- 6. Aggregate results --------------------------------- #
    print("_aggregate_results:")
    agg = BenchmarkHarness._aggregate_results([
        {"success": True, "metrics": {"accuracy": 0.8,
                                      "energy_kwh": 0.02}},
        {"success": False, "metrics": {}},
        {"success": True, "metrics": {"accuracy": 1.0,
                                      "energy_kwh": 0.01}},
    ])
    _check(
        "success_rate == 2/3",
        abs(agg["success_rate"] - 2/3) < 1e-9,
    )
    _check(
        "avg_accuracy averaged over all 3 tasks",
        abs(agg["avg_accuracy"] - (0.8 + 0.0 + 1.0) / 3) < 1e-9,
    )

    # ---- 7. Report format validation -------------------------- #
    print("report formats:")
    with tempfile.TemporaryDirectory() as tmpdir:
        harness = BenchmarkHarness(output_dir=tmpdir)
        sample = {
            "benchmark_name": "b1",
            "start_time": "2026-01-01T00:00:00+00:00",
            "duration_seconds": 1.0,
            "agent_name": "a1",
            "framework": "x",
            "num_tasks": 1,
            "aggregated_metrics": {"success_rate": 1.0,
                                   "avg_accuracy": 0.9},
            "task_results": [],
        }
        _check(
            "markdown renders",
            "# Benchmark Report: b1" in harness.generate_report(
                sample, fmt="markdown",
            ),
        )
        _check(
            "text renders",
            "BENCHMARK REPORT: b1" in harness.generate_report(
                sample, fmt="text",
            ),
        )
        json_out = harness.generate_report(sample, fmt="json")
        _check(
            "json renders",
            json.loads(json_out)["benchmark_name"] == "b1",
        )
        _check(
            "unknown format rejected",
            _raises(BenchmarkError, lambda:
                    harness.generate_report(sample, fmt="bogus")),
        )

    # ---- 8. Missing metric renders em dash -------------------- #
    print("missing metric in report:")
    with tempfile.TemporaryDirectory() as tmpdir:
        harness = BenchmarkHarness(output_dir=tmpdir)
        sample = {
            "benchmark_name": "b1",
            "start_time": "2026-01-01T00:00:00+00:00",
            "duration_seconds": 1.0,
            "agent_name": "a1",
            "framework": "x",
            "num_tasks": 0,
            "aggregated_metrics": {"success_rate": 0.0},
            "task_results": [],
        }
        md = harness.generate_report(sample, fmt="markdown")
        _check(
            "missing avg_accuracy renders as em dash",
            "Avg Accuracy**: —" in md,
        )

    # ---- 9. Failed tasks surfaced ----------------------------- #
    print("failed tasks surfaced:")
    with tempfile.TemporaryDirectory() as tmpdir:
        harness = BenchmarkHarness(output_dir=tmpdir)
        sample = {
            "benchmark_name": "b1",
            "start_time": "2026-01-01T00:00:00+00:00",
            "duration_seconds": 1.0,
            "agent_name": "a1",
            "framework": "x",
            "num_tasks": 2,
            "aggregated_metrics": {"success_rate": 0.5,
                                   "avg_accuracy": 0.4},
            "task_results": [
                {"task_id": "t1", "success": True, "metrics": {}},
                {"task_id": "t2", "success": False,
                 "error": "boom", "metrics": {}},
            ],
        }
        md = harness.generate_report(sample, fmt="markdown")
        _check(
            "failed tasks section present",
            "### Failed Tasks" in md,
        )
        _check(
            "failed task listed",
            "t2" in md and "boom" in md,
        )

    # ---- 10. Save with unique filenames ---------------------- #
    print("save uniqueness:")
    with tempfile.TemporaryDirectory() as tmpdir:
        harness = BenchmarkHarness(output_dir=tmpdir)
        p1 = harness._save_result({"a": 1}, "testname")
        p2 = harness._save_result({"a": 1}, "testname")
        _check("first save succeeded", p1.exists())
        _check("second save succeeded", p2.exists())
        _check("filenames differ", p1 != p2)

    # ---- 11. __all__ / __dir__ ------------------------------- #
    print("dir:")
    _check("__dir__ matches __all__", __dir__() == sorted(__all__))
    for _name in (
        "BenchmarkError", "BenchmarkHarness", "ENHANCEMENTS_AVAILABLE",
    ):
        _check(f"{_name} exported", _name in __all__)

    print()
    print(f"passed: {passed}  failed: {failed}")
    if failed:
        sys.exit(1)
