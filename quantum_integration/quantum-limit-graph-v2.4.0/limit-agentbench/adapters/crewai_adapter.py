# -*- coding: utf-8 -*-
"""
CrewAI Agent Adapter (Enhanced)
===============================

Adapter for CrewAI role-based agents with optional advanced
decision-making and sustainability tracking.

Behaviour notes
---------------
**Strategy switching.** This adapter does **not** implement FlexGen-style
execution strategies — CrewAI agents and crews always run natively. When
the base class's enhancement pipeline is enabled, the distillation
optimizer still trains on MODP rewards; but because the action never
changes the actual execution, those rewards are independent of the
optimizer's choice and the training signal is noise.

To make this limitation **visible** rather than silent:

- ``CrewAIAdapter(..., config={"use_enhancements": True})`` **without**
  ``crewai_strategy_aware=True`` logs a ``WARNING`` and disables
  enhancements (falls back to legacy). In ``strict=True`` mode it raises
  :class:`AdapterError` instead.
- Callers who have wrapped the crew with their own strategy logic can set
  ``crewai_strategy_aware=True`` to suppress the guard; the adapter will
  still report ``strategy_used="native"`` accurately in the output.
- Every ``_execute`` result carries ``"strategy_used"`` (what actually
  ran) and ``"strategy_requested"`` (what the optimizer asked for), so a
  consumer cannot be misled by ``output["execution_strategy"]`` set by
  the base class.

**Crew vs Agent.** ``Crew.kickoff()`` runs the crew's pre-configured
tasks and ignores the caller-supplied ``task_input``. When the adapter
receives a ``Crew`` with no configured tasks, it raises rather than
silently executing on a string. When the crew **has** tasks, it logs at
``INFO`` that ``task_input`` is not passed through.

**Result normalisation.** ``Crew.kickoff()`` returns a ``CrewOutput``
object in current CrewAI versions. The adapter normalises it to a string
(via ``.raw`` when present, otherwise ``str(...)``) so that
``output["answer"]`` is ``.strip()``-safe, matching the base class's
contract.

Enhancements
------------
- **Strategy no-op surfaced.** ``use_enhancements=True`` without
  ``crewai_strategy_aware=True`` warns and disables enhancements; in
  ``strict`` mode raises. (§2.1)
- **``kickoff()`` task-input handling.** Detects the "crew with no
  tasks" case and raises; logs at ``INFO`` when the crew has tasks and
  ``task_input`` is not passed. (§2.2)
- **`CrewOutput` normalisation.** ``_normalize_result`` flattens
  ``.raw`` / ``str(...)`` so ``output["answer"]`` is always a string. (§2.4)
- **``_get_agent_name`` hardened.** Handles ``None`` role/name and
  non-string values; falls back to ``"CrewAIAgent"``. (§2.3)
- **``strict`` forwarded.** ``__init__`` accepts keyword-only
  ``strict=False`` and forwards to the base class. (§2.5)
- **``task_input=None`` validation.** Raises :class:`AdapterError`
  instead of executing on ``"None"``. (§2.7)
- **Narrow error type.** :class:`AdapterError` replaces ``ValueError``.
  (§2.6)
- **``execute_task`` preferred over ``kickoff``.** A hypothetical agent
  exposing both methods uses the one that actually consumes the task. (§2.2)
- **Accurate debug logging.** Logs ``strategy_requested`` and
  ``strategy_applied`` separately. (§2.14)
- **Docstring corrected.** No longer promises an ``accuracy`` field the
  code never returns. (§2.9)
- **Lazy ``%s`` logging.** (§2.8)
- ``__version__``, ``__all__``, ``__dir__``, and a ``_check()``-based
  smoke test that survives ``python -O``.

Compatibility
-------------
- ``CrewAIAdapter(agent, config=None)`` — same positional signature.
  ``strict=False`` is new and keyword-only.
- ``_execute(task_input, strategy="native")`` — same signature. The
  output now includes ``"strategy_used"`` and ``"strategy_requested"``
  (additive).
- **Behaviour change**: ``CrewAIAdapter(agent, {"use_enhancements":
  True})`` now warns and disables enhancements by default. Callers who
  relied on the previous silent no-op must either set
  ``crewai_strategy_aware=True`` (opt-in) or investigate why their
  optimizer was training on noise.
- **Behaviour change**: a ``Crew`` with no configured tasks now raises
  :class:`AdapterError` instead of silently executing ``kickoff()``.
- **Behaviour change**: ``task_input=None`` raises instead of executing
  on ``"None"``.

Requires Python 3.8+.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from .base_adapter import AdapterError, BaseAgentAdapter


logger = logging.getLogger(__name__)

__version__ = "2.5.0"


# --------------------------------------------------------------------------- #
# CrewAI adapter
# --------------------------------------------------------------------------- #
class CrewAIAdapter(BaseAgentAdapter):
    """
    Adapter for CrewAI agents.

    Supports:

    - CrewAI Agent (``execute_task``).
    - CrewAI Crew (``kickoff``) — with the caveat that ``kickoff()``
      ignores the caller-supplied ``task_input``; see the module
      docstring.
    - Role-based task execution.

    When ``use_enhancements`` is enabled in the config **and**
    ``crewai_strategy_aware=True`` is also set, the adapter delegates to
    the enhanced pipeline defined in :class:`BaseAgentAdapter`. Without
    ``crewai_strategy_aware=True``, the adapter warns (or raises in
    ``strict`` mode) because this adapter does not implement strategy
    switching and the distillation optimizer would train on noise.
    """

    def __init__(
        self,
        agent: Any,
        config: Optional[Dict[str, Any]] = None,
        *,
        strict: bool = False,
    ) -> None:
        """
        Initialize the CrewAI adapter.

        Parameters
        ----------
        agent : CrewAI Agent or Crew
            The underlying CrewAI object.
        config : dict, optional
            Configuration dictionary. Recognised keys include
            ``use_enhancements`` (bool), ``crewai_strategy_aware``
            (bool — required to enable enhancements), and every key the
            base adapter understands.
        strict : bool, default False
            If True, raise instead of warning / downgrading on
            configuration inconsistencies.
        """
        if agent is None:
            raise AdapterError("agent must not be None.")

        cfg: Dict[str, Any] = dict(config) if config else {}
        strategy_aware = bool(cfg.get("crewai_strategy_aware", False))

        # §2.1 — the adapter cannot switch strategies, so enablement
        # without an explicit opt-in produces silent noise training.
        if cfg.get("use_enhancements") and not strategy_aware:
            if strict:
                raise AdapterError(
                    "use_enhancements=True requires "
                    "crewai_strategy_aware=True for the CrewAI adapter: "
                    "this adapter does not implement strategy switching, "
                    "so the distillation optimizer would train on "
                    "rewards that do not depend on its actions. Set "
                    "crewai_strategy_aware=True if you have wrapped the "
                    "crew with your own strategy logic, or set "
                    "use_enhancements=False."
                )
            logger.warning(
                "CrewAI adapter: use_enhancements=True with "
                "crewai_strategy_aware=False. The distillation optimizer "
                "would train on rewards independent of its actions; "
                "disabling enhancements and falling back to the legacy "
                "path. Set crewai_strategy_aware=True to opt in.",
            )
            cfg["use_enhancements"] = False

        self._strategy_aware: bool = strategy_aware

        super().__init__(agent, "crewai", cfg, strict=strict)

    # ---------------------------------------------------------------- execute
    def _execute(
        self,
        task_input: Dict[str, Any],
        strategy: str = "native",
    ) -> Dict[str, Any]:
        """
        Execute the CrewAI agent on ``task_input``.

        Parameters
        ----------
        task_input : dict
            Task input. Expected to be a dict (the base class validates
            this); a string is accepted for defensiveness and wrapped as
            ``{"question": <str>}``.
        strategy : str
            Requested execution strategy. Accepted for interface
            consistency but **not applied** — this adapter always runs
            natively. The output's ``strategy_used`` field reflects what
            actually ran.

        Returns
        -------
        dict
            Contains ``"answer"`` (always a string or ``None``),
            ``"framework"`` (``"crewai"``), ``"strategy_used"``
            (``"native"``), and ``"strategy_requested"`` (the value of
            ``strategy``).

        Raises
        ------
        AdapterError
            If ``task_input`` is ``None``, if the agent exposes neither
            ``execute_task`` nor ``kickoff``, or if a Crew has no
            configured tasks.
        """
        if task_input is None:
            raise AdapterError("task_input must not be None.")

        # Defensive: the base class guarantees a dict, but _execute can
        # be called directly by external code or by tests.
        if not isinstance(task_input, dict):
            task_input = {"question": str(task_input)}

        # Falsy-string fallback: an empty 'question' or 'input' falls
        # through to the next candidate. A whitespace-only string does
        # not (it is truthy). Callers who need exact task_desc control
        # should pass a single non-empty field.
        task_desc = (
            task_input.get("question")
            or task_input.get("input")
            or str(task_input)
        )

        logger.debug(
            "Running CrewAI agent (strategy_requested=%s, "
            "strategy_applied=native)",
            strategy,
        )

        raw = self._dispatch(task_desc)

        return {
            "answer": self._normalize_result(raw),
            "framework": "crewai",
            "strategy_used": "native",
            "strategy_requested": strategy,
        }

    def _dispatch(self, task_desc: str) -> Any:
        """
        Invoke the underlying CrewAI object.

        Prefers ``execute_task`` (which consumes ``task_desc``) over
        ``kickoff`` (which ignores it and runs configured tasks).
        """
        # §2.2 — execute_task first: it consumes task_desc.
        execute_task = getattr(self.agent, "execute_task", None)
        if callable(execute_task):
            return execute_task(task_desc)

        kickoff = getattr(self.agent, "kickoff", None)
        if callable(kickoff):
            tasks = getattr(self.agent, "tasks", None)
            # Empty list / tuple / None → crew has no configured tasks.
            if not tasks:
                raise AdapterError(
                    "CrewAI crew has no configured tasks; "
                    "kickoff() ignores task_desc and the adapter "
                    "cannot pass task_input through. Use an Agent "
                    "(with execute_task) or configure tasks on the "
                    "crew before calling run()."
                )
            logger.info(
                "CrewAI: crew has %d configured task(s); task_input "
                "is not passed to kickoff().",
                len(tasks),
            )
            return kickoff()

        raise AdapterError(
            f"Unsupported CrewAI agent type: "
            f"{type(self.agent).__name__} (expected 'execute_task' or "
            f"'kickoff')."
        )

    @staticmethod
    def _normalize_result(result: Any) -> Any:
        """
        Flatten a CrewAI return value into a string (or ``None``).

        ``Crew.kickoff()`` returns a ``CrewOutput`` object in current
        CrewAI versions; its ``.raw`` attribute is the plain text.
        Falls back to ``str(result)`` when ``.raw`` is unavailable.
        """
        if result is None or isinstance(result, str):
            return result
        raw = getattr(result, "raw", None)
        if isinstance(raw, str):
            return raw
        return str(result)

    # ---------------------------------------------------------------- name
    def _get_agent_name(self) -> str:
        """
        Return a non-empty string name for the CrewAI agent.

        Prefers ``role`` (prefixed with ``"CrewAI-"``), then ``name``,
        then a generic fallback. Handles ``None`` and non-string values.
        """
        role = getattr(self.agent, "role", None)
        if isinstance(role, str) and role:
            return f"CrewAI-{role}"
        name = getattr(self.agent, "name", None)
        if isinstance(name, str) and name:
            return name
        return "CrewAIAgent"


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__: List[str] = [
    "__version__",
    "CrewAIAdapter",
]


def __dir__() -> List[str]:
    return sorted(__all__)


# --------------------------------------------------------------------------- #
# Smoke test: python -m integrations.crewai_adapter
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    import sys

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

    # ---- Fake CrewAI objects -------------------------------------- #
    class _AgentWithExecuteTask:
        role = "Researcher"

        def execute_task(self, task_desc: str) -> str:
            return f"executed: {task_desc}"

    class _CrewWithTasks:
        name = "MyCrew"
        tasks = [object(), object()]

        def kickoff(self) -> str:
            return "crew result"

    class _CrewWithoutTasks:
        name = "EmptyCrew"
        tasks = []

        def kickoff(self) -> str:
            return "should not reach"  # pragma: no cover

    class _CrewOutput:
        def __init__(self) -> None:
            self.raw = "raw text from crew output"

        def __str__(self) -> str:  # pragma: no cover — not hit
            return "str of crew output"

    class _CrewReturningCrewOutput:
        tasks = [object()]

        def kickoff(self) -> _CrewOutput:
            return _CrewOutput()

    class _UnsupportedAgent:
        """Exposes neither execute_task nor kickoff."""

    # ---- 1. Construction ----------------------------------------- #
    print("construction:")
    a = CrewAIAdapter(_AgentWithExecuteTask())
    _check("constructs", a is not None)
    _check(
        "agent_name from role",
        a.agent_name == "CrewAI-Researcher",
    )
    _check(
        "strict not forwarded by default",
        a._strict is False,
    )

    _check(
        "rejects agent=None",
        _raises(AdapterError, lambda: CrewAIAdapter(None)),
    )

    # ---- 2. _get_agent_name fallbacks ---------------------------- #
    print("_get_agent_name fallbacks:")

    class _AgentWithName:
        name = "PlainName"

    _check(
        "agent_name from name",
        CrewAIAdapter(_AgentWithName()).agent_name == "PlainName",
    )

    class _AgentWithNoneRole:
        role = None
        name = "FallbackName"

    _check(
        "None role falls through to name",
        CrewAIAdapter(_AgentWithNoneRole()).agent_name == "FallbackName",
    )

    class _AgentWithNonStringName:
        role = None
        name = 42

    _check(
        "non-string name falls back to generic",
        CrewAIAdapter(_AgentWithNonStringName()).agent_name
        == "CrewAIAgent",
    )

    class _Bare:
        pass

    _check(
        "no role / name → generic",
        CrewAIAdapter(_Bare()).agent_name == "CrewAIAgent",
    )

    # ---- 3. _execute with an Agent ------------------------------- #
    print("_execute with Agent:")
    a = CrewAIAdapter(_AgentWithExecuteTask())
    out = a._execute({"question": "What is AI?"})
    _check(
        "answer contains task",
        "executed: What is AI?" in out["answer"],
    )
    _check("framework == crewai", out["framework"] == "crewai")
    _check("strategy_used == native", out["strategy_used"] == "native")
    _check(
        "strategy_requested defaults to native",
        out["strategy_requested"] == "native",
    )

    # ---- 4. _execute with a Crew (tasks present) ----------------- #
    print("_execute with Crew (has tasks):")
    a = CrewAIAdapter(_CrewWithTasks())
    out = a._execute({"question": "irrelevant"})
    _check("crew result used", out["answer"] == "crew result")
    _check(
        "strategy_used still native",
        out["strategy_used"] == "native",
    )

    # ---- 5. _execute with a Crew (no tasks) raises --------------- #
    print("_execute with Crew (no tasks):")
    a = CrewAIAdapter(_CrewWithoutTasks())
    _check(
        "raises when crew has no tasks",
        _raises(AdapterError, lambda: a._execute({"question": "x"})),
    )

    # ---- 6. _execute with an unsupported agent ------------------- #
    print("_execute with unsupported agent:")
    a = CrewAIAdapter(_UnsupportedAgent())
    _check(
        "raises when agent has no known method",
        _raises(AdapterError, lambda: a._execute({"question": "x"})),
    )

    # ---- 7. _execute with CrewOutput normalisation --------------- #
    print("CrewOutput normalisation:")
    a = CrewAIAdapter(_CrewReturningCrewOutput())
    out = a._execute({"question": "x"})
    _check(
        "CrewOutput.raw is extracted",
        out["answer"] == "raw text from crew output",
    )
    # Prove it is a string (not the object).
    _check("answer is a str", isinstance(out["answer"], str))

    # ---- 8. _execute with task_input=None raises ----------------- #
    print("task_input=None:")
    a = CrewAIAdapter(_AgentWithExecuteTask())
    _check(
        "raises on None",
        _raises(AdapterError, lambda: a._execute(None)),  # type: ignore[arg-type]
    )

    # ---- 9. _execute with non-dict task_input -------------------- #
    print("non-dict task_input (defensive):")
    out = a._execute("plain string")  # type: ignore[arg-type]
    _check(
        "string wrapped as question",
        "executed: plain string" in out["answer"],
    )

    # ---- 10. strategy reporting ---------------------------------- #
    print("strategy reporting:")
    a = CrewAIAdapter(_AgentWithExecuteTask())
    out = a._execute({"question": "x"}, strategy="flexgen_low_precision")
    _check(
        "strategy_used is native (accurate)",
        out["strategy_used"] == "native",
    )
    _check(
        "strategy_requested echoes input",
        out["strategy_requested"] == "flexgen_low_precision",
    )

    # ---- 11. use_enhancements guard ------------------------------ #
    print("use_enhancements guard:")
    a = CrewAIAdapter(
        _AgentWithExecuteTask(),
        {"use_enhancements": True},
    )
    _check(
        "enhancements disabled without strategy_aware",
        a.use_enhancements is False,
    )

    _check(
        "raises in strict mode",
        _raises(AdapterError, lambda: CrewAIAdapter(
            _AgentWithExecuteTask(),
            {"use_enhancements": True},
            strict=True,
        )),
    )

    a = CrewAIAdapter(
        _AgentWithExecuteTask(),
        {"use_enhancements": True, "crewai_strategy_aware": True},
    )
    _check(
        "strategy_aware opt-in preserves enhancements",
        a.use_enhancements is True,
    )
    _check(
        "distillation_optimizer constructed",
        a.distillation_optimizer is not None,
    )

    # ---- 12. run() via the base class ---------------------------- #
    print("run() via base class:")
    a = CrewAIAdapter(_AgentWithExecuteTask())
    out = a.run({"question": "What is AI?"})
    _check("run works", "executed: What is AI?" in out["answer"])
    _check(
        "run adds latency_ms",
        "latency_ms" in out,
    )
    _check(
        "run adds energy_kwh when tracking on",
        "energy_kwh" in out,
    )

    # ---- 13. __all__ / __dir__ ----------------------------------- #
    print("dir:")
    _check("__dir__ matches __all__", __dir__() == sorted(__all__))
    _check("CrewAIAdapter exported", "CrewAIAdapter" in __all__)
    _check("__version__ exported", "__version__" in __all__)

    print()
    print(f"passed: {passed}  failed: {failed}")
    if failed:
        sys.exit(1)
