# -*- coding: utf-8 -*-
"""
LangChain Agent Adapter (Enhanced)
==================================

Adapter for LangChain / LangGraph agents with optional advanced
decision-making and sustainability tracking.

Behaviour notes
---------------
**Strategy switching.** This adapter does **not** implement FlexGen-style
execution strategies — LangChain agents always run natively. When the
base class's enhancement pipeline is enabled, the distillation optimizer
still trains on MODP rewards; but because the action never changes the
actual execution, those rewards are independent of the optimizer's
choice and the training signal is noise.

To make this limitation **visible** rather than silent:

- ``LangChainAdapter(..., config={"use_enhancements": True})``
  **without** ``langchain_strategy_aware=True`` logs a ``WARNING`` and
  disables enhancements (falls back to legacy). In ``strict=True`` mode
  it raises :class:`AdapterError` instead.
- Callers who have wrapped the chain with their own strategy logic can
  set ``langchain_strategy_aware=True`` to suppress the guard; the
  adapter will still report ``strategy_used="native"`` accurately.
- Every ``_execute`` result carries ``"strategy_used"`` (what actually
  ran) and ``"strategy_requested"`` (what the optimizer asked for), so
  a consumer cannot be misled by ``output["execution_strategy"]`` set
  by the base class.

**Return-shape normalisation.** LangChain's ``invoke()`` returns one of:

- ``dict`` — LCEL chain output or LangGraph final state;
- ``str`` — LCEL chain returning a string;
- ``AIMessage`` / ``AIMessageChunk`` — chat model runnable;
- ``list[BaseMessage]`` — chat history chain.

This adapter always produces a string-safe ``output["answer"]``:

1. ``dict``: prefers ``answer`` / ``output`` / ``result`` / ``content`` /
   ``text`` (first string-valued key wins), falls back to ``str(dict)``;
2. objects with a string ``.content`` (LangChain messages): uses
   ``.content``;
3. objects with a string ``.raw`` (CrewOutput-like): uses ``.raw``;
4. ``str``: passes through;
5. anything else: ``str(...)``.

The *original* object is preserved under ``output["raw_result"]`` so a
caller who needs the full message structure can access it.

**Async agents.** The base adapter is synchronous. Async LangChain
agents (``ainvoke`` / ``arun``) are **not** supported — the adapter
raises ``AdapterError`` with a clear message. Use a synchronous wrapper
(e.g. ``asyncio.run(agent.ainvoke(...))``) in the caller.

**Legacy ``run`` path.** For the legacy AgentExecutor convention, the
task description is extracted as ``question`` → ``input`` →
``str(task_input)``, matching every other adapter in the tree. (The
previous version used only ``input``, so ``{"question": ...}`` reached
the agent as a Python dict repr.)

Enhancements
------------
- **Strategy no-op surfaced.** ``use_enhancements=True`` without
  ``langchain_strategy_aware=True`` warns and disables enhancements; in
  ``strict`` mode raises. (§2.1)
- **Message-aware normalisation.** ``_coerce_answer`` extracts
  ``.content`` / ``.raw`` / dict keys; ``output["answer"]`` is always
  ``str | None``. (§2.2, §2.3)
- **Unified return shape.** Every ``_execute`` result has the same
  keys: ``answer``, ``framework``, ``strategy_used``,
  ``strategy_requested``, ``raw_result``. (§2.15)
- **Legacy path fixed.** Uses ``question`` → ``input`` → ``str(...)``.
  (§2.4)
- **``task_input = None`` validation.** Raises :class:`AdapterError`.
  (§2.5)
- **``_get_agent_name`` hardened.** Dead branch removed; handles
  ``None`` / non-string ``.name``. (§2.6)
- **Narrow error type.** :class:`AdapterError` replaces ``ValueError``.
  (§2.7)
- **``strict`` forwarded.** Keyword-only ``strict=False`` on
  ``__init__``. (§2.8)
- **Docstring corrected.** No longer promises strategy switching.
  (§2.10)
- **Async gap documented.** Clear error for ``ainvoke`` / ``arun``
  agents. (§2.12)
- **``invoke({})`` documented.** Empty dict runs the chain on empty
  state. (§2.11)
- **Lazy ``%s`` logging.** Logs ``strategy_requested`` and
  ``strategy_applied`` separately. (§2.13)
- ``__version__``, ``__all__``, ``__dir__``, and a ``_check()``-based
  smoke test that survives ``python -O``.

Compatibility
-------------
- ``LangChainAdapter(agent, config=None)`` — same positional
  signature. ``strict=False`` is new and keyword-only.
- ``_execute(task_input, strategy="native")`` — same signature. The
  output gains ``"strategy_used"``, ``"strategy_requested"``, and
  ``"raw_result"`` (additive). For dict results, all original keys are
  preserved in the output.
- **Behaviour change for ``use_enhancements=True``**: the adapter now
  warns and disables enhancements unless ``langchain_strategy_aware=True``
  is also set. Callers with ``use_enhancements=True`` in their config
  will see a log line on first construction and fall back to legacy.
  This is the intended fix (§2.1) — the previous behaviour trained a
  distillation optimizer on rewards that were noise.
- **Behaviour change for ``task_input=None``**: raises
  :class:`AdapterError` instead of ``invoke(None)`` / ``run("None")``.
- **Behaviour change for legacy ``run`` path**: ``{"question": ...}``
  now reaches the agent as the question text, not ``str(dict)``.
- ``_get_agent_name`` no longer returns non-string values.

Requires Python 3.8+.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from .base_adapter import AdapterError, BaseAgentAdapter


logger = logging.getLogger(__name__)

__version__ = "2.5.0"


# --------------------------------------------------------------------------- #
# LangChain adapter
# --------------------------------------------------------------------------- #
class LangChainAdapter(BaseAgentAdapter):
    """
    Adapter for LangChain and LangGraph agents.

    Supports:

    - LangChain AgentExecutor (legacy ``run`` method);
    - LangGraph ``StateGraph`` agents (``invoke``);
    - LCEL chains (``invoke``).

    When ``use_enhancements`` is enabled **and**
    ``langchain_strategy_aware=True`` is also set, the adapter delegates
    to the enhanced pipeline defined in :class:`BaseAgentAdapter`.
    Without ``langchain_strategy_aware=True``, the adapter warns (or
    raises in ``strict`` mode) because this adapter does not implement
    strategy switching and the distillation optimizer would train on
    noise.

    See the module docstring for the return-shape normalisation rules
    and the async-adapter gap.
    """

    def __init__(
        self,
        agent: Any,
        config: Optional[Dict[str, Any]] = None,
        *,
        strict: bool = False,
    ) -> None:
        """
        Initialize the LangChain adapter.

        Parameters
        ----------
        agent : Any
            LangChain / LangGraph / LCEL runnable. Must expose either
            ``invoke`` or ``run`` (sync — async variants are not
            supported by this sync adapter).
        config : dict, optional
            Configuration dictionary. Recognised keys include
            ``use_enhancements`` (bool), ``langchain_strategy_aware``
            (bool — required to enable enhancements), and every key the
            base adapter understands.
        strict : bool, default False
            If True, raise instead of warning / downgrading on
            configuration inconsistencies.
        """
        if agent is None:
            raise AdapterError("agent must not be None.")

        cfg: Dict[str, Any] = dict(config) if config else {}
        strategy_aware = bool(cfg.get("langchain_strategy_aware", False))

        # §2.1 — the adapter cannot switch strategies; enablement
        # without an explicit opt-in produces silent noise training.
        if cfg.get("use_enhancements") and not strategy_aware:
            if strict:
                raise AdapterError(
                    "use_enhancements=True requires "
                    "langchain_strategy_aware=True for the LangChain "
                    "adapter: this adapter does not implement strategy "
                    "switching, so the distillation optimizer would "
                    "train on rewards that do not depend on its "
                    "actions. Set langchain_strategy_aware=True if you "
                    "have wrapped the chain with your own strategy "
                    "logic, or set use_enhancements=False."
                )
            logger.warning(
                "LangChain adapter: use_enhancements=True with "
                "langchain_strategy_aware=False. The distillation "
                "optimizer would train on rewards independent of its "
                "actions; disabling enhancements and falling back to "
                "the legacy path. Set langchain_strategy_aware=True "
                "to opt in.",
            )
            cfg["use_enhancements"] = False

        self._strategy_aware: bool = strategy_aware

        super().__init__(agent, "langchain", cfg, strict=strict)

    # ---------------------------------------------------------------- execute
    def _execute(
        self,
        task_input: Dict[str, Any],
        strategy: str = "native",
    ) -> Dict[str, Any]:
        """
        Execute the LangChain agent on ``task_input``.

        Parameters
        ----------
        task_input : dict
            Task input. Expected to be a dict; other types are accepted
            defensively (a string is wrapped as ``{"question": ...}``,
            anything else goes to ``invoke`` untouched for the ``invoke``
            path, or is stringified for the ``run`` path).
        strategy : str
            Requested execution strategy. Accepted for interface
            consistency but **not applied** — this adapter always runs
            natively. The output's ``strategy_used`` field reflects
            what actually ran.

        Returns
        -------
        dict
            Keys: ``answer`` (``str | None``), ``framework``
            (``"langchain"``), ``strategy_used`` (``"native"``),
            ``strategy_requested`` (echoes ``strategy``), and
            ``raw_result`` (the original framework return value).
            When the framework returns a dict, its keys are also copied
            into the output (excluding the keys this adapter sets).

        Raises
        ------
        AdapterError
            If ``task_input`` is ``None``, if the agent exposes neither
            ``invoke`` nor ``run``, or if the agent only exposes async
            methods (``ainvoke`` / ``arun``).
        """
        if task_input is None:
            raise AdapterError("task_input must not be None.")

        logger.debug(
            "Running LangChain agent (strategy_requested=%s, "
            "strategy_applied=native)",
            strategy,
        )

        raw = self._dispatch(task_input)

        out: Dict[str, Any] = {
            "answer": self._coerce_answer(raw),
            "framework": "langchain",
            "strategy_used": "native",
            "strategy_requested": strategy,
            "raw_result": raw,
        }

        # Preserve any extra dict keys from the framework result —
        # these are typically useful state (e.g. LangGraph final state).
        if isinstance(raw, dict):
            for k, v in raw.items():
                if k not in out:
                    out[k] = v

        return out

    def _dispatch(self, task_input: Any) -> Any:
        """
        Invoke the underlying LangChain object.

        Prefers ``invoke`` (works with LangGraph state dicts and LCEL
        chains) over ``run`` (legacy AgentExecutor). Raises
        :class:`AdapterError` for unsupported agent shapes — including
        async-only agents, which this sync adapter cannot invoke.
        """
        invoke = getattr(self.agent, "invoke", None)
        if callable(invoke):
            return invoke(task_input)

        run = getattr(self.agent, "run", None)
        if callable(run):
            return run(self._extract_task_desc(task_input))

        # Report the async gap clearly rather than as a generic
        # "unsupported" message.
        if callable(getattr(self.agent, "ainvoke", None)) or callable(
            getattr(self.agent, "arun", None)
        ):
            raise AdapterError(
                f"LangChain agent {type(self.agent).__name__} only exposes "
                f"async methods (ainvoke / arun); this adapter is "
                f"synchronous. Use a synchronous wrapper, or call the "
                f"agent directly outside the adapter."
            )

        raise AdapterError(
            f"Unsupported LangChain agent type: "
            f"{type(self.agent).__name__} (expected 'invoke' or 'run')."
        )

    @staticmethod
    def _extract_task_desc(task_input: Any) -> str:
        """
        Extract a task description string for the legacy ``run`` path.

        Matches the base-class extraction used by every other adapter:
        ``question`` → ``input`` → ``str(task_input)``. Empty strings
        fall through; whitespace-only strings do not.
        """
        if isinstance(task_input, dict):
            return (
                task_input.get("question")
                or task_input.get("input")
                or str(task_input)
            )
        return str(task_input)

    @staticmethod
    def _coerce_answer(raw: Any) -> Optional[str]:
        """
        Flatten a LangChain return value into a string (or ``None``).

        Order of preference:

        1. ``None`` → ``None``.
        2. ``str`` → itself.
        3. ``dict`` → first string value among
           ``answer`` / ``output`` / ``result`` / ``content`` / ``text``;
           otherwise ``str(dict)``.
        4. Objects with a string ``.content`` (LangChain ``AIMessage`` /
           ``AIMessageChunk`` / ``BaseMessage``) → ``.content``.
        5. Objects with a string ``.raw`` (CrewOutput-like) → ``.raw``.
        6. Everything else (including ``list[BaseMessage]``) →
           ``str(...)``.
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
        Return a non-empty string name for the LangChain agent.

        Prefers ``name`` (when it is a non-empty string), else the
        class name. ``__class__`` is always present in Python, so the
        previous ``hasattr(self.agent, '__class__')`` check was
        effectively an unconditional fallback — the branch is now
        explicit.
        """
        name = getattr(self.agent, "name", None)
        if isinstance(name, str) and name:
            return name
        return self.agent.__class__.__name__


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__: List[str] = [
    "__version__",
    "LangChainAdapter",
]


def __dir__() -> List[str]:
    return sorted(__all__)


# --------------------------------------------------------------------------- #
# Smoke test: python -m integrations.langchain_adapter
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

    # ---- Fake LangChain objects ------------------------------------ #
    class _LCELStringChain:
        def invoke(self, task_input: Any) -> str:
            if isinstance(task_input, dict):
                return f"answer to {task_input.get('question', task_input)!r}"
            return f"answer to {task_input!r}"

    class _LangGraphStateGraph:
        """Returns a dict without an 'answer' key (typical graph state)."""

        def invoke(self, task_input: Any) -> Dict[str, Any]:
            return {
                "messages": ["some history"],
                "result": "the graph output",
            }

    class _ChatModel:
        """Returns an AIMessage-like object with .content."""

        class _Msg:
            content = "chat model answer"

            def __str__(self) -> str:  # pragma: no cover
                return "AI_MESSAGE_STR"

        def invoke(self, task_input: Any) -> Any:
            return _ChatModel._Msg()

    class _LegacyExecutor:
        def __init__(self) -> None:
            self.last_input: Optional[str] = None

        def run(self, task_desc: str) -> str:
            self.last_input = task_desc
            return f"executed: {task_desc}"

    class _AsyncOnlyAgent:
        async def ainvoke(self, task_input: Any) -> Any:  # pragma: no cover
            return None

    class _UnsupportedAgent:
        """Neither invoke nor run nor ainvoke / arun."""

    class _AIMessage:
        def __init__(self, content: str) -> None:
            self.content = content

    class _BaseMessageListChain:
        def invoke(self, task_input: Any) -> list:
            return [_AIMessage("first"), _AIMessage("second")]

    # ---- 1. Construction ------------------------------------------ #
    print("construction:")
    a = LangChainAdapter(_LCELStringChain())
    _check("constructs", a is not None)
    _check(
        "rejects agent=None",
        _raises(AdapterError, lambda: LangChainAdapter(None)),
    )

    # ---- 2. agent_name fallbacks ---------------------------------- #
    print("agent_name fallbacks:")

    class _NamedChain:
        name = "MyNamedChain"

        def invoke(self, x: Any) -> str:  # pragma: no cover
            return ""

    _check(
        "agent_name from .name",
        LangChainAdapter(_NamedChain()).agent_name == "MyNamedChain",
    )

    class _UnnamedChain:
        def invoke(self, x: Any) -> str:  # pragma: no cover
            return ""

    _check(
        "agent_name falls back to class name",
        LangChainAdapter(_UnnamedChain()).agent_name
        == "_UnnamedChain",
    )

    class _NoneNamedChain:
        name = None

        def invoke(self, x: Any) -> str:  # pragma: no cover
            return ""

    _check(
        "None .name falls back to class name",
        LangChainAdapter(_NoneNamedChain()).agent_name
        == "_NoneNamedChain",
    )

    # ---- 3. LCEL string chain ------------------------------------- #
    print("LCEL string chain:")
    a = LangChainAdapter(_LCELStringChain())
    out = a._execute({"question": "What is AI?"})
    _check("answer is a str", isinstance(out["answer"], str))
    _check(
        "answer content",
        "answer to 'What is AI?'" in out["answer"],
    )
    _check("strategy_used == native", out["strategy_used"] == "native")
    _check(
        "strategy_requested defaults to native",
        out["strategy_requested"] == "native",
    )
    _check("raw_result preserved", out["raw_result"] is not None)

    # ---- 4. LangGraph state dict ---------------------------------- #
    print("LangGraph state dict:")
    a = LangChainAdapter(_LangGraphStateGraph())
    out = a._execute({"question": "hello"})
    _check(
        "answer extracted from 'result' key",
        out["answer"] == "the graph output",
    )
    _check(
        "dict keys preserved in output",
        "messages" in out and "result" in out,
    )
    _check(
        "raw_result is the dict",
        isinstance(out["raw_result"], dict),
    )

    # ---- 5. AIMessage via .content -------------------------------- #
    print("AIMessage via .content:")
    a = LangChainAdapter(_ChatModel())
    out = a._execute({"question": "hi"})
    _check(
        "answer is AIMessage.content",
        out["answer"] == "chat model answer",
    )
    _check("raw_result is the message", out["raw_result"] is not None)

    # ---- 6. list[BaseMessage] fallback --------------------------- #
    print("list[BaseMessage] fallback:")
    a = LangChainAdapter(_BaseMessageListChain())
    out = a._execute({"question": "hi"})
    _check(
        "answer is str(list) fallback",
        isinstance(out["answer"], str)
        and "AIMessage" in out["answer"] or "first" in (out["answer"] or ""),
    )

    # ---- 7. Legacy AgentExecutor path ---------------------------- #
    print("legacy AgentExecutor path:")
    ex = _LegacyExecutor()
    a = LangChainAdapter(ex)
    out = a._execute({"question": "What is AI?"})
    _check(
        "legacy path uses question",
        "executed: What is AI?" in out["answer"],
    )
    _check(
        "executor received question text",
        ex.last_input == "What is AI?",
    )

    ex2 = _LegacyExecutor()
    a = LangChainAdapter(ex2)
    out = a._execute({"input": "fallback input"})
    _check(
        "legacy path uses input when question absent",
        "executed: fallback input" in out["answer"],
    )

    ex3 = _LegacyExecutor()
    a = LangChainAdapter(ex3)
    out = a._execute("a bare string")
    _check(
        "legacy path falls back to str(task_input)",
        "executed: a bare string" in out["answer"],
    )

    # ---- 8. task_input = None ------------------------------------ #
    print("task_input = None:")
    a = LangChainAdapter(_LCELStringChain())
    _check(
        "raises on None",
        _raises(AdapterError, lambda: a._execute(None)),  # type: ignore[arg-type]
    )

    # ---- 9. Unsupported agent ------------------------------------ #
    print("unsupported agent:")
    a = LangChainAdapter(_UnsupportedAgent())
    _check(
        "raises with clear message",
        _raises(AdapterError, lambda: a._execute({"question": "x"})),
    )

    # ---- 10. Async-only agent ------------------------------------ #
    print("async-only agent:")
    a = LangChainAdapter(_AsyncOnlyAgent())
    _check(
        "raises with async-specific message",
        _raises(AdapterError, lambda: a._execute({"question": "x"})),
    )

    # ---- 11. Strategy reporting ---------------------------------- #
    print("strategy reporting:")
    a = LangChainAdapter(_LCELStringChain())
    out = a._execute(
        {"question": "x"}, strategy="flexgen_low_precision",
    )
    _check(
        "strategy_used is native (accurate)",
        out["strategy_used"] == "native",
    )
    _check(
        "strategy_requested echoes input",
        out["strategy_requested"] == "flexgen_low_precision",
    )

    # ---- 12. use_enhancements guard ------------------------------ #
    print("use_enhancements guard:")
    a = LangChainAdapter(
        _LCELStringChain(),
        {"use_enhancements": True},
    )
    _check(
        "enhancements disabled without strategy_aware",
        a.use_enhancements is False,
    )

    _check(
        "raises in strict mode",
        _raises(AdapterError, lambda: LangChainAdapter(
            _LCELStringChain(),
            {"use_enhancements": True},
            strict=True,
        )),
    )

    a = LangChainAdapter(
        _LCELStringChain(),
        {"use_enhancements": True, "langchain_strategy_aware": True},
    )
    _check(
        "strategy_aware opt-in preserves enhancements",
        a.use_enhancements is True,
    )
    _check(
        "distillation_optimizer constructed",
        a.distillation_optimizer is not None,
    )

    # ---- 13. run() via base class -------------------------------- #
    print("run() via base class:")
    a = LangChainAdapter(_LCELStringChain())
    out = a.run({"question": "What is AI?"})
    _check("run works", isinstance(out["answer"], str))
    _check("run adds latency_ms", "latency_ms" in out)
    _check(
        "run adds energy_kwh when tracking on",
        "energy_kwh" in out,
    )

    # ---- 14. __all__ / __dir__ ----------------------------------- #
    print("dir:")
    _check("__dir__ matches __all__", __dir__() == sorted(__all__))
    _check("LangChainAdapter exported", "LangChainAdapter" in __all__)
    _check("__version__ exported", "__version__" in __all__)

    print()
    print(f"passed: {passed}  failed: {failed}")
    if failed:
        sys.exit(1)
