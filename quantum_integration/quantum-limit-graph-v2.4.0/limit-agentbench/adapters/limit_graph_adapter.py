# -*- coding: utf-8 -*-
"""
LIMIT-GRAPH Agent Adapter (Enhanced)
====================================

Adapter for native LIMIT-GRAPH quantum agents with optional advanced
decision-making and sustainability tracking.

Behaviour notes
---------------
**Dispatch priority.** The adapter dispatches in this order:

1. ``execute(task_input)`` — preferred; matches the base class's
   ``_execute`` naming.
2. ``run(task_input)`` — legacy / convenience entry point.
3. ``process(task_input)`` — fallback.
4. A **callable instance** (not a class) — invoked as ``agent(task_input)``.
5. Otherwise → :class:`AdapterError`.

Passing a **class** instead of an instance raises :class:`AdapterError`
with a clear message; the previous version silently called the class
constructor and returned the wrong result.

**Strategy switching.** This adapter does **not** implement FlexGen-style
execution strategies on its own. When the base class's enhancement
pipeline is enabled, the distillation optimizer still trains on MODP
rewards; but because the action never changes the actual execution, those
rewards are independent of the optimizer's choice and the training signal
is noise.

To make this limitation **visible** rather than silent:

- ``LimitGraphAdapter(..., config={"use_enhancements": True})``
  **without** ``limit_graph_strategy_aware=True`` **and** a callable
  ``limit_graph_strategy_executor`` logs a ``WARNING`` and disables
  enhancements (falls back to legacy). In ``strict=True`` mode it raises
  :class:`AdapterError` instead.
- A caller who supplies a ``limit_graph_strategy_executor`` callable
  promises that the executor honours the requested strategy; the
  adapter invokes the executor for every ``_execute`` call, passing
  ``strategy=<value>``. The executor is responsible for routing (native
  vs FlexGen vs a quantum-specific lowering such as reduced circuit
  depth / shots). The adapter does not inspect the executor's
  behaviour.
- The output's ``strategy_used`` reflects what the adapter delegated
  (``strategy`` when an executor is present, ``"native"`` otherwise).
  ``strategy_requested`` echoes what the optimizer asked for. When the
  two differ, :meth:`run` promotes the actual value into
  ``output["execution_strategy"]`` and preserves the requested value
  under ``output["execution_strategy_requested"]``.
- When ``use_enhancements`` is downgraded, the reason is surfaced in
  ``get_metadata()["enhancements_downgraded_reason"]``.

**Return-shape normalisation.** The adapter produces a **string-safe
``output["answer"]``** for every framework return shape:

1. ``None`` → ``None``;
2. ``str`` → itself;
3. ``dict`` → first string value among ``answer`` / ``output`` /
   ``result`` / ``content`` / ``text``; otherwise ``str(dict)``;
4. ``list`` — **last** item with a string ``.content`` (chat-history
   convention); otherwise ``str(list)``;
5. objects with a string ``.content`` → ``.content``;
6. objects with a string ``.raw`` → ``.raw``;
7. anything else → ``str(...)``.

The original framework return value is preserved under
``output["raw_result"]``. For dict results, a **shallow copy** is stored
to avoid aliasing the framework's mutable top-level dict; nested values
remain shared (documented).

**Framework dict handling.** The framework's return dict is **never
mutated**. Its keys are copied into the adapter's output, minus any keys
the adapter owns (``answer``, ``framework``, ``strategy_used``,
``strategy_requested``, ``raw_result``). If the framework set a
``"framework"`` key itself, the value is preserved under
``output["inner_framework"]`` and the outer value stays
``"limit_graph"``.

Enhancements
------------
- **Class-vs-instance footgun closed.** Passing a class raises
  :class:`AdapterError` with an actionable message instead of silently
  calling the constructor.
- **Framework dict no longer mutated.** Copied before adding adapter
  keys.
- **Strategy executor is required for enhancements.** ``use_enhancements``
  requires ``limit_graph_strategy_aware=True`` **and** a callable
  ``limit_graph_strategy_executor``; the executor is invoked with
  ``strategy=<value>``.
- **Downgrade reason surfaced in metadata.**
  ``get_metadata()["enhancements_downgraded_reason"]`` when the pipeline
  is downgraded.
- **``run()`` override reconciles ``execution_strategy`` vs
  ``strategy_used``.** When they differ, the actual value wins and the
  requested value is preserved under
  ``execution_strategy_requested``.
- **Unified return shape.** Every ``_execute`` result has ``answer`` /
  ``framework`` / ``strategy_used`` / ``strategy_requested`` /
  ``raw_result``; framework dict keys are additive.
- **Harden ``_get_agent_name``.** Handles ``None`` / non-string
  ``.name`` and ``.agent_id``; single unified prefix
  (``LimitGraph-*``).
- **``task_input is not None`` validated.** Raises
  :class:`AdapterError`.
- **``strict`` forwarded** to the base class.
- **Narrow error type.** :class:`AdapterError` replaces ``ValueError``.
- **Dispatch priority documented and inverted** — ``execute`` before
  ``run``, matching the base class's ``_execute`` naming.
- **Lazy ``%s`` logging.** Logs ``strategy_requested`` and
  ``strategy_applied`` separately.
- **Docstring corrected.** No longer promises strategy switching.
- ``__version__``, ``__all__``, ``__dir__``, and a ``_check()``-based
  smoke test that survives ``python -O``.

Compatibility
-------------
- ``LimitGraphAdapter(agent, config=None)`` — same positional signature.
  ``strict=False`` is new and keyword-only.
- ``_execute(task_input, strategy="native")`` — same signature. The
  output gains ``strategy_used``, ``strategy_requested``, and
  ``raw_result`` (additive); every previously preserved framework dict
  key is still present.
- **Behaviour change for ``use_enhancements=True``**: the adapter now
  requires a callable ``limit_graph_strategy_executor``. Callers with
  only ``limit_graph_strategy_aware=True`` will see a downgrade warning
  (unless they also pass an executor).
- **Behaviour change for callable agents**: passing a **class** raises
  :class:`AdapterError`. Passing a callable *instance* works as before.
- **Behaviour change for dispatch order**: ``execute`` is now preferred
  over ``run`` when both are present. An agent that expected ``run`` to
  win over ``execute`` must override ``_dispatch`` or set
  ``limit_graph_dispatch_order`` in the config (see the implementation).
- **Behaviour change for the ``framework`` key**: the adapter's outer
  value is always ``"limit_graph"``; a framework-supplied ``"framework"``
  is preserved under ``output["inner_framework"]``.
- ``_get_agent_name`` no longer returns non-string values.

Requires Python 3.8+.
"""

from __future__ import annotations

import logging
from typing import Any, Callable, Dict, List, Optional, Tuple

from .base_adapter import AdapterError, BaseAgentAdapter


logger = logging.getLogger(__name__)

__version__ = "2.5.0"


# Default dispatch order — ``execute`` matches the base class's
# ``_execute`` naming and is preferred. Override via
# ``config["limit_graph_dispatch_order"]``.
_DEFAULT_DISPATCH_ORDER: Tuple[str, ...] = ("execute", "run", "process")


# --------------------------------------------------------------------------- #
# LimitGraph adapter
# --------------------------------------------------------------------------- #
class LimitGraphAdapter(BaseAgentAdapter):
    """
    Adapter for native LIMIT-GRAPH agents.

    Supports:

    - Quantum-enhanced agents (``execute`` / ``run`` / ``process``);
    - Level 3/4/5 maturity agents;
    - NSN-integrated agents;
    - Multilingual agents;
    - Callable instances (not classes) via ``agent(task_input)``.

    To enable the base class's enhancement pipeline, callers must set
    **both** ``use_enhancements=True`` and
    ``limit_graph_strategy_aware=True`` **and** supply a callable
    ``limit_graph_strategy_executor(task_input, strategy=<value>)``. The
    executor is responsible for honouring the requested strategy; the
    adapter does not inspect its behaviour. Without all three,
    enhancements are downgraded to legacy and the reason is surfaced in
    :meth:`get_metadata`.

    See the module docstring for the return-shape normalisation rules
    and the strategy-switching limitation.
    """

    def __init__(
        self,
        agent: Any,
        config: Optional[Dict[str, Any]] = None,
        *,
        strict: bool = False,
    ) -> None:
        """
        Initialize the LIMIT-GRAPH adapter.

        Parameters
        ----------
        agent : Any
            LIMIT-GRAPH agent **instance** exposing ``execute`` /
            ``run`` / ``process``, or a callable instance. A class is
            rejected — instantiate the agent first.
        config : dict, optional
            Configuration dictionary. Recognised keys:

            - ``use_enhancements`` (bool);
            - ``limit_graph_strategy_aware`` (bool) — required alongside
              ``use_enhancements``;
            - ``limit_graph_strategy_executor`` (callable) — required
              alongside ``limit_graph_strategy_aware=True``; called as
              ``executor(task_input, strategy=<value>)``;
            - ``limit_graph_dispatch_order`` (tuple of str) — override
              the default dispatch order; must be a subset of
              ``{"execute", "run", "process"}``.
        strict : bool, default False
            If True, raise instead of warning / downgrading on
            configuration inconsistencies.
        """
        if agent is None:
            raise AdapterError("agent must not be None.")
        if isinstance(agent, type):
            raise AdapterError(
                f"LimitGraphAdapter expects an agent *instance*, got "
                f"the class {agent.__name__}. Instantiate it first "
                f"(e.g. LimitGraphAdapter(MyAgent(...)))."
            )

        cfg: Dict[str, Any] = dict(config) if config else {}

        # ---- strategy executor ---------------------------------- #
        strategy_aware = bool(
            cfg.get("limit_graph_strategy_aware", False)
        )
        strategy_executor = cfg.get("limit_graph_strategy_executor")
        if strategy_executor is not None and not callable(
            strategy_executor
        ):
            raise AdapterError(
                "limit_graph_strategy_executor must be callable, got "
                f"{type(strategy_executor).__name__}."
            )
        self._strategy_aware: bool = strategy_aware
        self._strategy_executor: Optional[Callable[..., Any]] = (
            strategy_executor
        )

        # ---- dispatch order ------------------------------------- #
        raw_order = cfg.get("limit_graph_dispatch_order")
        if raw_order is None:
            self._dispatch_order: Tuple[str, ...] = _DEFAULT_DISPATCH_ORDER
        else:
            if not isinstance(raw_order, (tuple, list)):
                raise AdapterError(
                    "limit_graph_dispatch_order must be a tuple or list "
                    f"of method names, got {type(raw_order).__name__}."
                )
            order = tuple(str(x) for x in raw_order)
            if not order or not all(
                x in _DEFAULT_DISPATCH_ORDER for x in order
            ):
                raise AdapterError(
                    "limit_graph_dispatch_order must be a non-empty "
                    f"subset of {_DEFAULT_DISPATCH_ORDER}, got {order!r}."
                )
            self._dispatch_order = order

        # ---- strategy guard ------------------------------------- #
        self._enhancements_downgraded_reason: Optional[str] = None
        downgrade_reason: Optional[str] = None
        if cfg.get("use_enhancements"):
            if not strategy_aware:
                downgrade_reason = (
                    "use_enhancements=True requires "
                    "limit_graph_strategy_aware=True for the "
                    "LimitGraphAdapter: this adapter does not implement "
                    "strategy switching on its own, so the distillation "
                    "optimizer would train on rewards that do not "
                    "depend on its actions."
                )
            elif not callable(strategy_executor):
                downgrade_reason = (
                    "limit_graph_strategy_aware=True requires a "
                    "callable limit_graph_strategy_executor so the "
                    "adapter can verify the promise (the executor is "
                    "responsible for honouring the requested strategy); "
                    "without it the adapter runs natively regardless of "
                    "the strategy and the optimizer trains on noise."
                )

        if downgrade_reason is not None:
            if strict:
                raise AdapterError(downgrade_reason)
            logger.warning("%s Disabling enhancements.", downgrade_reason)
            cfg["use_enhancements"] = False
            self._enhancements_downgraded_reason = downgrade_reason

        super().__init__(agent, "limit_graph", cfg, strict=strict)

    # ---------------------------------------------------------------- execute
    def _execute(
        self,
        task_input: Dict[str, Any],
        strategy: str = "native",
    ) -> Dict[str, Any]:
        """
        Execute the LIMIT-GRAPH agent (or the strategy executor) on
        ``task_input``.

        Parameters
        ----------
        task_input : dict | any
            Task input. Passed through to the agent or executor
            unchanged; ``None`` raises.
        strategy : str
            Requested execution strategy. When a strategy executor is
            configured, the executor is invoked with
            ``strategy=<value>`` and the return value is used as-is;
            otherwise the adapter dispatches natively and reports
            ``strategy_used="native"``.

        Returns
        -------
        dict
            Keys: ``answer`` (``str | None``), ``framework``
            (``"limit_graph"``), ``strategy_used``, ``strategy_requested``,
            and ``raw_result``. When the framework returns a dict, its
            keys are copied into the output (excluding the keys the
            adapter owns); a framework-supplied ``"framework"`` key is
            preserved under ``inner_framework``.

        Raises
        ------
        AdapterError
            If ``task_input`` is ``None``, or if the agent exposes no
            recognised dispatch method.
        """
        if task_input is None:
            raise AdapterError("task_input must not be None.")

        logger.debug(
            "Running LIMIT-GRAPH agent (strategy_requested=%s, "
            "executor=%s)",
            strategy,
            "present" if self._strategy_executor is not None else "absent",
        )

        if self._strategy_executor is not None:
            raw = self._strategy_executor(task_input, strategy=strategy)
            strategy_used = strategy
        else:
            raw = self._dispatch(task_input)
            strategy_used = "native"

        raw_snapshot = dict(raw) if isinstance(raw, dict) else raw

        out: Dict[str, Any] = {
            "answer": self._coerce_answer(raw),
            "framework": "limit_graph",
            "strategy_used": strategy_used,
            "strategy_requested": strategy,
            "raw_result": raw_snapshot,
        }

        if isinstance(raw, dict):
            # Preserve any framework-supplied "framework" under a
            # distinct key; the outer adapter is authoritative.
            inner_fw = raw.get("framework")
            if isinstance(inner_fw, str) and inner_fw != "limit_graph":
                out["inner_framework"] = inner_fw
            for k, v in raw.items():
                if k not in out:
                    out[k] = v

        return out

    # ---------------------------------------------------------------- run
    def run(
        self,
        task_input: Dict[str, Any],
        track_green_metrics: Optional[bool] = None,
    ) -> Dict[str, Any]:
        """
        Run the agent on ``task_input``.

        Delegates to :meth:`BaseAgentAdapter.run` and post-processes the
        output so the base class's ``execution_strategy`` field (which
        reflects what the optimizer *requested*) matches the adapter's
        ``strategy_used`` (what actually ran). When they differ,
        ``execution_strategy`` is set to ``strategy_used`` and the
        requested value is preserved under
        ``execution_strategy_requested``.
        """
        out = super().run(
            task_input, track_green_metrics=track_green_metrics,
        )
        requested = out.get("strategy_requested")
        used = out.get("strategy_used")
        if (
            requested is not None
            and used is not None
            and requested != used
        ):
            out["execution_strategy"] = used
            out["execution_strategy_requested"] = requested
        return out

    # ---------------------------------------------------------------- dispatch
    def _dispatch(self, task_input: Any) -> Any:
        """
        Invoke the underlying LIMIT-GRAPH agent.

        Dispatch order is ``config["limit_graph_dispatch_order"]``
        (default ``("execute", "run", "process")``). A callable
        *instance* is invoked as a last resort. A **class** is rejected
        with an actionable message.
        """
        for method_name in self._dispatch_order:
            method = getattr(self.agent, method_name, None)
            if callable(method):
                return method(task_input)

        if callable(self.agent) and not isinstance(self.agent, type):
            return self.agent(task_input)

        # ``isinstance(self.agent, type)`` was already rejected in
        # __init__; this is a defensive branch for a subclass that
        # somehow sets ``self.agent`` to a class after construction.
        if isinstance(self.agent, type):
            raise AdapterError(
                f"LimitGraphAdapter expects an agent *instance*, got "
                f"the class {self.agent.__name__}."
            )

        raise AdapterError(
            f"Unsupported LIMIT-GRAPH agent type: "
            f"{type(self.agent).__name__} (expected an instance "
            f"exposing {' / '.join(self._dispatch_order)}, or a "
            f"callable instance)."
        )

    # ---------------------------------------------------------------- coerce
    @staticmethod
    def _coerce_answer(raw: Any) -> Optional[str]:
        """
        Flatten a LIMIT-GRAPH return value into a string (or ``None``).

        Same rules as the LangChain adapter (see module docstring):

        1. ``None`` → ``None``; 2. ``str`` → itself; 3. ``dict`` →
        first string among ``answer`` / ``output`` / ``result`` /
        ``content`` / ``text``, else ``str(dict)``; 4. ``list`` → last
        item with string ``.content``, else ``str(list)``; 5. objects
        with string ``.content`` → ``.content``; 6. objects with string
        ``.raw`` → ``.raw``; 7. otherwise ``str(...)``.
        """
        if raw is None:
            return None
        if isinstance(raw, str):
            return raw
        if isinstance(raw, dict):
            for key in ("answer", "output", "result", "content", "text"):
                v = raw.get(key)
                if isinstance(v, str):
                    return v
            return str(raw)
        if isinstance(raw, list):
            for item in reversed(raw):
                content = getattr(item, "content", None)
                if isinstance(content, str):
                    return content
            return str(raw)
        content = getattr(raw, "content", None)
        if isinstance(content, str):
            return content
        raw_attr = getattr(raw, "raw", None)
        if isinstance(raw_attr, str):
            return raw_attr
        return str(raw)

    # ---------------------------------------------------------------- name
    def _get_agent_name(self) -> str:
        """
        Return a non-empty string name for the LIMIT-GRAPH agent.

        Prefers a non-empty string ``name`` (prefixed with
        ``"LimitGraph-"``), then a non-``None`` ``agent_id``, else the
        generic ``"LimitGraphAgent"``. Never returns ``None`` or a
        non-string value.
        """
        name = getattr(self.agent, "name", None)
        if isinstance(name, str) and name:
            return f"LimitGraph-{name}"
        agent_id = getattr(self.agent, "agent_id", None)
        if agent_id is not None and isinstance(agent_id, (str, int)):
            return f"LimitGraph-{agent_id}"
        return "LimitGraphAgent"

    # ---------------------------------------------------------------- metadata
    def get_metadata(self) -> Dict[str, Any]:
        """
        Return adapter metadata, including the strategy-guard state and
        any downgrade reason.

        Adds:

        - ``limit_graph_strategy_aware`` (bool);
        - ``limit_graph_strategy_executor`` (bool — present / absent);
        - ``limit_graph_dispatch_order`` (list of str);
        - ``enhancements_downgraded_reason`` (str, only when the
          enhancement pipeline was downgraded).
        """
        meta = super().get_metadata()
        meta["limit_graph_strategy_aware"] = self._strategy_aware
        meta["limit_graph_strategy_executor"] = (
            self._strategy_executor is not None
        )
        meta["limit_graph_dispatch_order"] = list(self._dispatch_order)
        if self._enhancements_downgraded_reason is not None:
            meta["enhancements_downgraded_reason"] = (
                self._enhancements_downgraded_reason
            )
        return meta


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__: List[str] = [
    "__version__",
    "LimitGraphAdapter",
]


def __dir__() -> List[str]:
    return sorted(__all__)


# --------------------------------------------------------------------------- #
# Smoke test: python -m integrations.limit_graph_adapter
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

    # ---- Fake LIMIT-GRAPH agent shapes ---------------------------- #
    class _AgentWithExecute:
        name = "execute_agent"

        def execute(self, task_input: Any) -> str:
            return f"executed: {task_input}"

    class _AgentWithRun:
        name = "run_agent"

        def run(self, task_input: Any) -> str:
            return f"ran: {task_input}"

    class _AgentWithProcess:
        name = "process_agent"

        def process(self, task_input: Any) -> str:
            return f"processed: {task_input}"

    class _AgentWithExecuteAndRun:
        name = "both_agent"

        def execute(self, task_input: Any) -> str:
            return f"execute-wins: {task_input}"

        def run(self, task_input: Any) -> str:
            return f"run-loses: {task_input}"

    class _CallableInstance:
        name = "callable_agent"

        def __call__(self, task_input: Any) -> str:
            return f"called: {task_input}"

    class _UnsupportedAgent:
        name = "unsupported"

    class _AgentWithFramework:
        name = "fw_agent"

        def execute(self, task_input: Any) -> Dict[str, Any]:
            return {
                "framework": "custom-fw",
                "answer": "the answer",
                "extra": 42,
            }

    class _AgentWithNonStringName:
        agent_id = 12345

        def execute(self, task_input: Any) -> str:
            return "ok"

    class _AgentWithNoneName:
        name = None
        agent_id = "abc"

        def execute(self, task_input: Any) -> str:
            return "ok"

    class _AgentWithContentObject:
        name = "content_agent"

        class _Msg:
            content = "message content"

        def execute(self, task_input: Any) -> Any:
            return _AgentWithContentObject._Msg()

    class _AgentWithList:
        name = "list_agent"

        class _Msg:
            def __init__(self, content: str) -> None:
                self.content = content

        def execute(self, task_input: Any) -> list:
            return [
                _AgentWithList._Msg("first"),
                _AgentWithList._Msg("second"),
            ]

    # ---- 1. Construction ------------------------------------------ #
    print("construction:")
    a = LimitGraphAdapter(_AgentWithExecute())
    _check("constructs", a is not None)
    _check(
        "agent_name prefixed",
        a.agent_name == "LimitGraph-execute_agent",
    )

    # ---- 2. Class-vs-instance footgun ---------------------------- #
    print("class-vs-instance footgun:")
    _check(
        "class rejected with actionable message",
        _raises(AdapterError, lambda:
                LimitGraphAdapter(_AgentWithExecute)),
    )
    try:
        LimitGraphAdapter(_AgentWithExecute)
    except AdapterError as exc:
        _check(
            "message mentions *instance*",
            "instance" in str(exc).lower(),
        )

    _check(
        "None rejected",
        _raises(AdapterError, lambda: LimitGraphAdapter(None)),
    )

    # ---- 3. Dispatch order ---------------------------------------- #
    print("dispatch order:")
    a_exec = LimitGraphAdapter(_AgentWithExecute())
    _check(
        "execute-only works",
        "executed:" in a_exec._execute({"q": "hi"})["answer"],
    )

    a_run = LimitGraphAdapter(_AgentWithRun())
    _check(
        "run-only works",
        "ran:" in a_run._execute({"q": "hi"})["answer"],
    )

    a_proc = LimitGraphAdapter(_AgentWithProcess())
    _check(
        "process-only works",
        "processed:" in a_proc._execute({"q": "hi"})["answer"],
    )

    a_both = LimitGraphAdapter(_AgentWithExecuteAndRun())
    out_both = a_both._execute({"q": "hi"})
    _check(
        "execute wins over run",
        "execute-wins:" in out_both["answer"],
    )

    # Dispatch-order override — run wins when execute is listed last.
    a_over = LimitGraphAdapter(
        _AgentWithExecuteAndRun(),
        {"limit_graph_dispatch_order": ("run", "execute")},
    )
    out_over = a_over._execute({"q": "hi"})
    _check(
        "dispatch-order override honoured",
        "run-loses:" in out_over["answer"],
    )

    _check(
        "invalid dispatch order rejected",
        _raises(AdapterError, lambda: LimitGraphAdapter(
            _AgentWithExecute(),
            {"limit_graph_dispatch_order": ("bogus",)},
        )),
    )

    # ---- 4. Callable instance (not class) ------------------------ #
    print("callable instance:")
    a_call = LimitGraphAdapter(_CallableInstance())
    out = a_call._execute({"q": "hi"})
    _check(
        "callable instance invoked",
        "called:" in out["answer"],
    )

    # ---- 5. Unsupported agent ------------------------------------ #
    print("unsupported agent:")
    a_unsup = LimitGraphAdapter(_UnsupportedAgent())
    _check(
        "unsupported raises AdapterError",
        _raises(AdapterError, lambda:
                a_unsup._execute({"q": "hi"})),
    )

    # ---- 6. Framework dict not mutated --------------------------- #
    print("framework dict not mutated:")
    fw_agent = _AgentWithFramework()
    original = fw_agent.execute({"q": "hi"})   # capture a fresh dict
    # Re-inject a new dict to prove the adapter doesn't mutate it.
    fw_agent.execute = lambda task_input: original  # type: ignore[assignment]
    a_fw = LimitGraphAdapter(fw_agent)
    out_fw = a_fw._execute({"q": "hi"})
    _check(
        "framework dict not mutated by adapter",
        "framework" in original
        and original["framework"] == "custom-fw",
    )
    _check(
        "outer framework is limit_graph",
        out_fw["framework"] == "limit_graph",
    )
    _check(
        "inner framework preserved",
        out_fw["inner_framework"] == "custom-fw",
    )
    _check(
        "extra keys preserved",
        out_fw["extra"] == 42,
    )

    # ---- 7. Normalised output shape ------------------------------ #
    print("output shape:")
    for agent, label in (
        (_AgentWithExecute(), "execute"),
        (_AgentWithContentObject(), "content-object"),
        (_AgentWithList(), "list-of-messages"),
    ):
        out = LimitGraphAdapter(agent)._execute({"q": "hi"})
        _check(
            f"{label}: answer is str",
            isinstance(out["answer"], str),
        )
        _check(
            f"{label}: framework present",
            out["framework"] == "limit_graph",
        )
        _check(
            f"{label}: strategy_used present",
            "strategy_used" in out,
        )
        _check(
            f"{label}: raw_result present",
            "raw_result" in out,
        )

    # ---- 8. Content-object and list normalisation ---------------- #
    print("content / list normalisation:")
    out = LimitGraphAdapter(_AgentWithContentObject())._execute(
        {"q": "hi"},
    )
    _check(
        "content extracted",
        out["answer"] == "message content",
    )
    out = LimitGraphAdapter(_AgentWithList())._execute({"q": "hi"})
    _check(
        "list last content used",
        out["answer"] == "second",
    )

    # ---- 9. _get_agent_name fallbacks ---------------------------- #
    print("_get_agent_name fallbacks:")
    a = LimitGraphAdapter(_AgentWithNonStringName())
    _check(
        "agent_id fallback",
        a.agent_name == "LimitGraph-12345",
    )
    a = LimitGraphAdapter(_AgentWithNoneName())
    _check(
        "None name falls to agent_id",
        a.agent_name == "LimitGraph-abc",
    )

    class _BareAgent:
        def execute(self, task_input: Any) -> str:  # pragma: no cover
            return "ok"

    a = LimitGraphAdapter(_BareAgent())
    _check(
        "bare agent falls to generic",
        a.agent_name == "LimitGraphAgent",
    )

    # ---- 10. task_input = None ----------------------------------- #
    print("task_input = None:")
    a = LimitGraphAdapter(_AgentWithExecute())
    _check(
        "raises on None",
        _raises(AdapterError, lambda: a._execute(None)),  # type: ignore[arg-type]
    )

    # ---- 11. Strategy reporting (no executor) ------------------- #
    print("strategy reporting (no executor):")
    a = LimitGraphAdapter(_AgentWithExecute())
    out = a._execute(
        {"q": "hi"}, strategy="flexgen_low_precision",
    )
    _check(
        "strategy_used is native",
        out["strategy_used"] == "native",
    )
    _check(
        "strategy_requested echoed",
        out["strategy_requested"] == "flexgen_low_precision",
    )

    # ---- 12. Strategy executor ---------------------------------- #
    print("strategy executor:")
    calls: List[Tuple[Any, str]] = []

    def _executor(task_input: Any, *, strategy: str) -> str:
        calls.append((task_input, strategy))
        return f"executor:{strategy}"

    a = LimitGraphAdapter(
        _AgentWithExecute(),
        {
            "use_enhancements": True,
            "limit_graph_strategy_aware": True,
            "limit_graph_strategy_executor": _executor,
        },
    )
    _check("enhancements preserved", a.use_enhancements is True)
    out = a._execute(
        {"q": "hi"}, strategy="flexgen_low_precision",
    )
    _check("executor called once", len(calls) == 1)
    _check(
        "executor receives strategy",
        calls[0][1] == "flexgen_low_precision",
    )
    _check(
        "strategy_used reflects delegation",
        out["strategy_used"] == "flexgen_low_precision",
    )
    _check(
        "answer from executor",
        out["answer"] == "executor:flexgen_low_precision",
    )

    # ---- 13. Strategy guards ------------------------------------ #
    print("strategy guards:")
    a = LimitGraphAdapter(
        _AgentWithExecute(),
        {"use_enhancements": True},
    )
    _check(
        "no strategy_aware → downgraded",
        a.use_enhancements is False,
    )
    _check(
        "downgrade reason recorded",
        "enhancements_downgraded_reason" in a.get_metadata(),
    )
    _check(
        "strict raises without strategy_aware",
        _raises(AdapterError, lambda: LimitGraphAdapter(
            _AgentWithExecute(),
            {"use_enhancements": True},
            strict=True,
        )),
    )

    a = LimitGraphAdapter(
        _AgentWithExecute(),
        {"use_enhancements": True, "limit_graph_strategy_aware": True},
    )
    _check(
        "strategy_aware without executor → downgraded",
        a.use_enhancements is False,
    )
    _check(
        "reason mentions executor",
        "executor" in a.get_metadata().get(
            "enhancements_downgraded_reason", "",
        ),
    )
    _check(
        "strict raises without executor",
        _raises(AdapterError, lambda: LimitGraphAdapter(
            _AgentWithExecute(),
            {"use_enhancements": True, "limit_graph_strategy_aware": True},
            strict=True,
        )),
    )

    _check(
        "non-callable executor rejected",
        _raises(AdapterError, lambda: LimitGraphAdapter(
            _AgentWithExecute(),
            {
                "use_enhancements": True,
                "limit_graph_strategy_aware": True,
                "limit_graph_strategy_executor": "not-callable",
            },
        )),
    )

    # ---- 14. run() via base class ------------------------------- #
    print("run() via base class:")
    a = LimitGraphAdapter(_AgentWithExecute())
    out = a.run({"q": "hi"})
    _check(
        "run works",
        "executed:" in out["answer"],
    )
    _check("latency_ms present", "latency_ms" in out)
    _check(
        "energy_kwh present by default",
        "energy_kwh" in out,
    )
    _check(
        "execution_strategy reconciled",
        out["execution_strategy"] == out["strategy_used"],
    )

    # ---- 15. run() with enhancements ---------------------------- #
    print("run() with enhancements:")
    a = LimitGraphAdapter(
        _AgentWithExecute(),
        {
            "use_enhancements": True,
            "limit_graph_strategy_aware": True,
            "limit_graph_strategy_executor": _executor,
        },
    )
    out = a.run({"q": "hi"})
    _check(
        "run works with enhancements",
        isinstance(out["answer"], str),
    )
    _check(
        "execution_strategy matches strategy_used",
        out["execution_strategy"] == out["strategy_used"],
    )

    # ---- 16. metadata ------------------------------------------- #
    print("metadata:")
    a = LimitGraphAdapter(_AgentWithExecute())
    meta = a.get_metadata()
    _check(
        "strategy_aware surfaced",
        meta["limit_graph_strategy_aware"] is False,
    )
    _check(
        "strategy_executor surfaced",
        meta["limit_graph_strategy_executor"] is False,
    )
    _check(
        "dispatch order surfaced",
        meta["limit_graph_dispatch_order"] == ["execute", "run", "process"],
    )
    _check(
        "adapter_version is __version__",
        meta["adapter_version"] == __version__,
    )

    # ---- 17. __all__ / __dir__ ---------------------------------- #
    print("dir:")
    _check("__dir__ matches __all__", __dir__() == sorted(__all__))
    _check("LimitGraphAdapter exported", "LimitGraphAdapter" in __all__)
    _check("__version__ exported", "__version__" in __all__)

    print()
    print(f"passed: {passed}  failed: {failed}")
    if failed:
        sys.exit(1)
