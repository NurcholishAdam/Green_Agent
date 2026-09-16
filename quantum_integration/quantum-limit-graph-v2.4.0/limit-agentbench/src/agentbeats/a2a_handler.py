# src/agentbeats/a2a_handler.py

"""
A2A Handler with Budget Enforcement

Wraps A2A task dispatch with a budget-enforcement gate. Before forwarding a
task to a remote agent, the handler estimates the task's resource
consumption and asks the :class:`BudgetEnforcer` whether the run is
permitted. If not, it short-circuits and returns a structured
``budget_exceeded`` response without ever calling the remote agent.

Original behaviour preserved
----------------------------
- ``A2AHandler(budget=...)`` — budget is optional; without it the handler
  simply forwards tasks without enforcement.
- ``send_task_with_budget(agent_url, task)`` — returns
  ``{"status": "budget_exceeded", "violations": [...]}`` on breach, or
  delegates to ``send_task`` otherwise.

Enhancements
------------
- Thread-safe via ``RLock``.
- Configurable estimation heuristics via :class:`A2AHandlerConfig`.
- Immutable ``DispatchRecord`` history with bounded ring-buffer.
- Strict / non-strict mode for malformed task payloads.
- Structured serialization: ``to_dict`` / ``from_dict`` / ``to_json`` / ``from_json``.
- Context-manager support for scoped dispatch batches.
- Custom :class:`A2AHandlerError`.
- Lazy ``%s`` logging, ``__repr__``, and a smoke test under ``__main__``.
- Graceful degradation if the ``constraints`` package is unavailable.
"""

from __future__ import annotations

import json
import logging
import math
import threading
import time
from dataclasses import asdict, dataclass, field
from typing import Any, Awaitable, Callable, Dict, List, Mapping, Optional

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Optional import of the budget-enforcement layer
# --------------------------------------------------------------------------- #
# The constraints package is the canonical home of BudgetEnforcer / Budget.
# We import it defensively so this module remains usable (with the
# enforcement gate disabled) in environments where constraints is absent.
try:  # pragma: no cover — import-time branch depends on environment
    from constraints.budget_enforcer import BudgetEnforcer  # type: ignore
    from constraints.budget_manager import Budget  # type: ignore

    _CONSTRAINTS_AVAILABLE = True
except ImportError:  # pragma: no cover
    BudgetEnforcer = None  # type: ignore[assignment]
    Budget = None  # type: ignore[assignment]
    _CONSTRAINTS_AVAILABLE = False
    logger.debug("constraints package unavailable; budget enforcement disabled.")


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class A2AHandlerError(ValueError):
    """Raised for invalid inputs or configuration."""


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class A2AHandlerConfig:
    """
    Tunable parameters for the A2A budget gate.

    Centralizes the estimation heuristics so callers can calibrate them per
    deployment without editing the handler.
    """

    # Per-unit heuristics used by the default ``_estimate_consumption``.
    default_energy_wh: float = 1.0
    default_carbon_g: float = 0.5
    default_latency_ms: float = 1000.0
    default_cost_usd: float = 0.0

    # Multipliers applied to the per-unit heuristics based on task metadata.
    token_scaling_factor: float = 1e-3      # 1 Wh per 1000 tokens (example)
    step_scaling_factor: float = 1.0         # 1 Wh per reasoning step

    # Default timeout for the delegated ``send_task`` call (seconds).
    default_timeout_seconds: float = 30.0

    def __post_init__(self) -> None:
        for name in (
            "default_energy_wh",
            "default_carbon_g",
            "default_latency_ms",
            "default_cost_usd",
            "token_scaling_factor",
            "step_scaling_factor",
            "default_timeout_seconds",
        ):
            value = getattr(self, name)
            if value < 0:
                raise A2AHandlerError(f"{name} must be non-negative, got {value!r}.")


# --------------------------------------------------------------------------- #
# Immutable dispatch sample
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class DispatchRecord:
    """Immutable snapshot of one ``send_task_with_budget`` call."""

    timestamp: float
    agent_url: str
    outcome: str  # "forwarded" | "budget_exceeded" | "error"
    estimated_consumption: Mapping[str, float]
    violations: List[str] = field(default_factory=list)
    latency_ms: Optional[float] = None
    label: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["estimated_consumption"] = dict(self.estimated_consumption)
        d["violations"] = list(self.violations)
        return d


# --------------------------------------------------------------------------- #
# Handler
# --------------------------------------------------------------------------- #
class A2AHandler:
    """
    A2A task dispatcher with an optional budget-enforcement gate.

    The handler is intentionally thin: it estimates consumption, asks the
    enforcer, and either forwards or short-circuits. All task-shape heuristics
    live in :meth:`_estimate_consumption` so subclasses can override them
    without touching the enforcement logic.

    Parameters
    ----------
    budget : Budget, optional
        Budget constraints to enforce. If ``None``, enforcement is disabled.
    config : A2AHandlerConfig, optional
        Estimation heuristics and timeout. Defaults to ``A2AHandlerConfig()``.
    strict : bool, default True
        If True, malformed task payloads raise :class:`A2AHandlerError`.
        If False, they are logged and treated as empty dicts.
    max_history : int | None, default 1000
        Ring-buffer cap on recorded :class:`DispatchRecord` samples.
    send_task_fn : callable, optional
        Async callable ``(agent_url, task) -> dict`` used to forward tasks.
        If omitted, ``self.send_task`` must be implemented by a subclass.
    """

    def __init__(
        self,
        budget: Any = None,
        *,
        config: Optional[A2AHandlerConfig] = None,
        strict: bool = True,
        max_history: Optional[int] = 1000,
        send_task_fn: Optional[
            Callable[[str, Mapping[str, Any]], Awaitable[Dict[str, Any]]]
        ] = None,
    ) -> None:
        if max_history is not None and max_history <= 0:
            raise A2AHandlerError("max_history must be > 0 or None.")

        self._config: A2AHandlerConfig = config or A2AHandlerConfig()
        self._strict: bool = bool(strict)
        self._max_history: Optional[int] = max_history
        self._send_task_fn = send_task_fn

        self._lock = threading.RLock()
        self._history: List[DispatchRecord] = []
        self._ctx_start: Optional[float] = None

        # Build the enforcer only when a budget was provided AND the
        # constraints package is importable.
        if budget is not None and not _CONSTRAINTS_AVAILABLE:
            raise A2AHandlerError(
                "A budget was supplied but the constraints package is not "
                "importable. Install it or pass budget=None."
            )

        self.budget_enforcer = (
            BudgetEnforcer(budget) if budget is not None else None
        )

        logger.debug(
            "A2AHandler initialized (enforcement=%s, strict=%s, max_history=%s)",
            self.budget_enforcer is not None,
            self._strict,
            self._max_history,
        )

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> A2AHandlerConfig:
        return self._config

    @property
    def enforcement_enabled(self) -> bool:
        return self.budget_enforcer is not None

    @property
    def history(self) -> List[DispatchRecord]:
        with self._lock:
            return list(self._history)

    @property
    def dispatch_count(self) -> int:
        with self._lock:
            return len(self._history)

    # ------------------------------------------------------------ public API
    async def send_task_with_budget(
        self,
        agent_url: str,
        task: Mapping[str, Any],
        *,
        label: Optional[str] = None,
        record: bool = True,
    ) -> Dict[str, Any]:
        """
        Send a task to ``agent_url`` subject to the configured budget.

        Parameters
        ----------
        agent_url : str
            Remote agent endpoint.
        task : Mapping
            Task payload.
        label : str, optional
            Optional label stored with the dispatch record.
        record : bool, default True
            If True, append a :class:`DispatchRecord` to history.

        Returns
        -------
        dict
            Either the remote agent's response, or
            ``{"status": "budget_exceeded", "violations": [...]}``.
        """
        if not isinstance(agent_url, str) or not agent_url:
            raise A2AHandlerError("agent_url must be a non-empty string.")

        safe_task = self._coerce_task(task)

        start = time.perf_counter()
        estimated: Dict[str, float] = {}
        violations: List[str] = []

        # ---- Budget gate -------------------------------------------------
        if self.budget_enforcer is not None:
            try:
                estimated = self._estimate_consumption(safe_task)
                can_execute, violations = self._check_budget(estimated)
            except A2AHandlerError:
                raise
            except Exception as exc:  # enforcer raised unexpectedly
                logger.exception("Budget enforcer failure: %s", exc)
                self._record(
                    agent_url=agent_url,
                    outcome="error",
                    estimated=estimated,
                    violations=[f"enforcer_error: {exc}"],
                    latency_ms=(time.perf_counter() - start) * 1000.0,
                    label=label,
                    record=record,
                )
                raise A2AHandlerError(
                    f"Budget enforcement failed: {exc}"
                ) from exc

            if not can_execute:
                elapsed_ms = (time.perf_counter() - start) * 1000.0
                logger.warning(
                    "Budget exceeded for %s: %d violation(s).",
                    agent_url,
                    len(violations),
                )
                self._record(
                    agent_url=agent_url,
                    outcome="budget_exceeded",
                    estimated=estimated,
                    violations=list(violations),
                    latency_ms=elapsed_ms,
                    label=label,
                    record=record,
                )
                return {
                    "status": "budget_exceeded",
                    "violations": list(violations),
                }

        # ---- Forward ----------------------------------------------------
        try:
            result = await self._dispatch(agent_url, safe_task)
        except Exception:
            elapsed_ms = (time.perf_counter() - start) * 1000.0
            self._record(
                agent_url=agent_url,
                outcome="error",
                estimated=estimated,
                violations=[],
                latency_ms=elapsed_ms,
                label=label,
                record=record,
            )
            raise

        elapsed_ms = (time.perf_counter() - start) * 1000.0
        self._record(
            agent_url=agent_url,
            outcome="forwarded",
            estimated=estimated,
            violations=[],
            latency_ms=elapsed_ms,
            label=label,
            record=record,
        )
        logger.debug(
            "Forwarded task to %s in %.2f ms (estimated=%s).",
            agent_url,
            elapsed_ms,
            estimated,
        )
        return result

    # ------------------------------------------------------ overridable hooks
    def _estimate_consumption(self, task: Mapping[str, Any]) -> Dict[str, float]:
        """
        Estimate the resource consumption of ``task``.

        Default heuristic (override in subclasses for domain-specific logic):
        - ``tokens`` key → energy scaled by ``token_scaling_factor``
        - ``steps`` key → energy scaled by ``step_scaling_factor``
        - Falls back to the configured defaults when keys are absent.

        Returns a mapping with keys ``energy_wh``, ``carbon_g``,
        ``latency_ms``, and ``cost_usd`` (the exact shape ``BudgetManager``
        expects).
        """
        cfg = self._config
        tokens = self._coerce_number(task.get("tokens", 0), "tokens", 0.0)
        steps = self._coerce_number(task.get("steps", 0), "steps", 0.0)

        energy = cfg.default_energy_wh
        energy += tokens * cfg.token_scaling_factor
        energy += steps * cfg.step_scaling_factor

        return {
            "energy_wh": energy,
            "carbon_g": cfg.default_carbon_g,
            "latency_ms": cfg.default_latency_ms,
            "cost_usd": cfg.default_cost_usd,
        }

    async def send_task(
        self, agent_url: str, task: Mapping[str, Any]
    ) -> Dict[str, Any]:
        """
        Forward ``task`` to ``agent_url``.

        The default implementation delegates to ``send_task_fn`` if one was
        supplied at construction; otherwise it raises so that subclasses must
        provide a transport.
        """
        if self._send_task_fn is None:
            raise A2AHandlerError(
                "No transport configured. Pass `send_task_fn=` to the "
                "constructor or override `send_task` in a subclass."
            )
        return await self._send_task_fn(agent_url, task)

    # -------------------------------------------------------------- internals
    async def _dispatch(
        self, agent_url: str, task: Mapping[str, Any]
    ) -> Dict[str, Any]:
        """Invoke ``send_task`` defensively (result must be a Mapping)."""
        result = await self.send_task(agent_url, task)
        if not isinstance(result, Mapping):
            raise A2AHandlerError(
                f"send_task must return a Mapping, got {type(result).__name__}."
            )
        return dict(result)

    def _check_budget(
        self, estimated: Mapping[str, float]
    ) -> tuple[bool, List[str]]:
        """
        Ask the enforcer whether ``estimated`` is admissible.

        Handles both the ``(can_execute, violations)`` tuple shape and the
        single ``can_execute`` shape for forward compatibility.
        """
        result = self.budget_enforcer.manager.can_execute(dict(estimated))
        if isinstance(result, tuple):
            can_execute, violations = result
        else:
            can_execute, violations = bool(result), []
        return bool(can_execute), list(violations or [])

    def _coerce_task(self, task: Any) -> Dict[str, Any]:
        """Validate / normalize the task payload according to strict mode."""
        if isinstance(task, Mapping):
            return dict(task)
        msg = f"task must be a Mapping, got {type(task).__name__}."
        if self._strict:
            raise A2AHandlerError(msg)
        logger.warning("%s Treating as empty dict.", msg)
        return {}

    @staticmethod
    def _coerce_number(value: Any, name: str, fallback: float) -> float:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return fallback
        v = float(value)
        if math.isnan(v) or math.isinf(v) or v < 0:
            return fallback
        return v

    def _record(
        self,
        *,
        agent_url: str,
        outcome: str,
        estimated: Mapping[str, float],
        violations: List[str],
        latency_ms: Optional[float],
        label: Optional[str],
        record: bool,
    ) -> None:
        if not record:
            return
        sample = DispatchRecord(
            timestamp=time.time(),
            agent_url=agent_url,
            outcome=outcome,
            estimated_consumption=dict(estimated),
            violations=list(violations),
            latency_ms=latency_ms,
            label=label,
        )
        with self._lock:
            self._history.append(sample)
            if self._max_history is not None and len(self._history) > self._max_history:
                del self._history[0]

    # --------------------------------------------------------------- helpers
    def statistics(self) -> Dict[str, Any]:
        """Return aggregate statistics over the recorded dispatch history."""
        with self._lock:
            history = list(self._history)

        if not history:
            return {
                "count": 0,
                "forwarded": 0,
                "budget_exceeded": 0,
                "errors": 0,
                "success_rate": None,
                "mean_latency_ms": None,
            }

        forwarded = sum(1 for s in history if s.outcome == "forwarded")
        exceeded = sum(1 for s in history if s.outcome == "budget_exceeded")
        errors = sum(1 for s in history if s.outcome == "error")
        latencies = [s.latency_ms for s in history if s.latency_ms is not None]

        return {
            "count": len(history),
            "forwarded": forwarded,
            "budget_exceeded": exceeded,
            "errors": errors,
            "success_rate": forwarded / len(history),
            "mean_latency_ms": (sum(latencies) / len(latencies)) if latencies else None,
        }

    def reset(self, *, clear_history: bool = False) -> None:
        """Reset the handler state; optionally clear history."""
        with self._lock:
            if clear_history:
                self._history.clear()
        logger.debug("A2AHandler reset (clear_history=%s)", clear_history)

    # ---------------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "config": asdict(self._config),
                "strict": self._strict,
                "max_history": self._max_history,
                "enforcement_enabled": self.budget_enforcer is not None,
                "history": [s.to_dict() for s in self._history],
            }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "A2AHandler":
        """
        Rebuild an ``A2AHandler`` from a dict produced by :meth:`to_dict`.

        Note: the ``budget`` object itself is not serialized (it is a runtime
        constraint, not state); pass it explicitly after reconstruction if
        enforcement is required.
        """
        if not isinstance(data, Mapping):
            raise A2AHandlerError(
                f"from_dict expects a Mapping, got {type(data).__name__}."
            )
        cfg_data = dict(data.get("config", {}) or {})
        cfg = A2AHandlerConfig(
            default_energy_wh=float(cfg_data.get("default_energy_wh", 1.0)),
            default_carbon_g=float(cfg_data.get("default_carbon_g", 0.5)),
            default_latency_ms=float(cfg_data.get("default_latency_ms", 1000.0)),
            default_cost_usd=float(cfg_data.get("default_cost_usd", 0.0)),
            token_scaling_factor=float(cfg_data.get("token_scaling_factor", 1e-3)),
            step_scaling_factor=float(cfg_data.get("step_scaling_factor", 1.0)),
            default_timeout_seconds=float(
                cfg_data.get("default_timeout_seconds", 30.0)
            ),
        )
        handler = cls(
            budget=None,
            config=cfg,
            strict=bool(data.get("strict", True)),
            max_history=data.get("max_history", 1000),
        )
        with handler._lock:
            for entry in data.get("history", []):
                handler._history.append(
                    DispatchRecord(
                        timestamp=float(entry["timestamp"]),
                        agent_url=str(entry["agent_url"]),
                        outcome=str(entry["outcome"]),
                        estimated_consumption=dict(entry["estimated_consumption"]),
                        violations=list(entry.get("violations", [])),
                        latency_ms=entry.get("latency_ms"),
                        label=entry.get("label"),
                    )
                )
        return handler

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), **kwargs)

    @classmethod
    def from_json(cls, payload: str) -> "A2AHandler":
        try:
            return cls.from_dict(json.loads(payload))
        except json.JSONDecodeError as exc:
            raise A2AHandlerError(f"Invalid JSON payload: {exc}") from exc

    # ----------------------------------------------------------- context mgr
    def __enter__(self) -> "A2AHandler":
        self._ctx_start = time.perf_counter()
        logger.debug("Entering scoped A2A dispatch batch.")
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        elapsed = time.perf_counter() - (
            self._ctx_start if self._ctx_start is not None else time.perf_counter()
        )
        self._ctx_start = None
        if exc_type is not None:
            logger.warning(
                "A2A dispatch scope exited with %s after %.4fs.",
                exc_type.__name__,
                elapsed,
            )
            return
        logger.info(
            "A2A dispatch scope closed in %.4fs (%d dispatch(es)).",
            elapsed,
            len(self._history),
        )

    # ----------------------------------------------------------------- dunder
    def __repr__(self) -> str:
        return (
            "A2AHandler("
            f"enforcement={self.budget_enforcer is not None}, "
            f"strict={self._strict}, "
            f"dispatches={len(self._history)}, "
            f"max_history={self._max_history})"
        )


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "A2AHandler",
    "A2AHandlerError",
    "A2AHandlerConfig",
    "DispatchRecord",
]


# --------------------------------------------------------------------------- #
# Local smoke test: python -m agentbeats.a2a_handler
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    import asyncio

    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )

    async def _fake_transport(agent_url: str, task: Mapping[str, Any]) -> Dict[str, Any]:
        await asyncio.sleep(0.01)
        return {"status": "ok", "agent_url": agent_url, "echo": dict(task)}

    async def main() -> None:
        # ---- Enforcement disabled (no budget) ---------------------------
        plain = A2AHandler(send_task_fn=_fake_transport)
        out = await plain.send_task_with_budget(
            "http://agent.example/task", {"tokens": 500, "steps": 2}, label="no-budget"
        )
        print("No-budget forward :", out)
        print("Statistics        :", plain.statistics())

        # ---- Enforcement enabled (requires constraints package) ---------
        if _CONSTRAINTS_AVAILABLE:
            budget = Budget(
                max_energy_wh=5.0,
                max_carbon_g=5.0,
                max_latency_ms=5_000.0,
            )
            gated = A2AHandler(budget=budget, send_task_fn=_fake_transport)

            # Small task → forwarded
            small = await gated.send_task_with_budget(
                "http://agent.example/task",
                {"tokens": 100, "steps": 1},
                label="small",
            )
            print("Gated (small)     :", small)

            # Huge task → estimate exceeds 5 Wh → budget_exceeded
            huge = await gated.send_task_with_budget(
                "http://agent.example/task",
                {"tokens": 10_000, "steps": 100},
                label="huge",
            )
            print("Gated (huge)      :", huge)
            print("Enforcer stats    :", gated.statistics())
        else:  # pragma: no cover
            print("constraints package not importable; skipping gated test.")

        # ---- Serialization round-trip -----------------------------------
        payload = plain.to_json()
        restored = A2AHandler.from_json(payload)
        assert restored.to_dict() == plain.to_dict()
        print("Serialization round-trip OK.")

        # ---- Context manager --------------------------------------------
        with A2AHandler(send_task_fn=_fake_transport) as scoped:
            await scoped.send_task_with_budget(
                "http://agent.example/task", {"tokens": 10}
            )
        print("Context-managed batch OK.")

        # ---- Validation failures ----------------------------------------
        for bad in ("", None, 123):
            try:
                await plain.send_task_with_budget(bad, {})  # type: ignore[arg-type]
            except A2AHandlerError as exc:
                print("Rejected as expected:", exc)
            else:  # pragma: no cover
                raise AssertionError(f"Expected rejection for agent_url={bad!r}")

    asyncio.run(main())
