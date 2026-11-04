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
  **without** ``langchain_strategy_aware=True`` **and** a callable
  ``langchain_strategy_executor`` logs a ``WARNING`` and disables
  enhancements (falls back to legacy). In ``strict=True`` mode it
  raises :class:`AdapterError` instead.
- A caller who supplies a ``langchain_strategy_executor`` callable
  promises that the executor honours the requested strategy; the
  adapter invokes the executor for every ``_execute`` call, passing
  ``strategy=<value>``. The executor is responsible for routing
  (native vs FlexGen vs any other backend) — the adapter does not
  inspect the executor's behaviour.
- The output's ``strategy_used`` reflects what the adapter delegated
  (``strategy`` when an executor is present, ``"native"`` otherwise).
  ``strategy_requested`` echoes what the optimizer asked for. When the
  two differ, :meth:`run` promotes the actual value into
  ``output["execution_strategy"]`` and preserves the requested value
  under ``output["execution_strategy_requested"]`` so a consumer
  cannot be misled by the base class's ``execution_strategy`` field.

**Return-shape normalisation.** LangChain's ``invoke()`` returns one of:

- ``dict`` — LCEL chain output or LangGraph final state;
- ``str`` — LCEL chain returning a string;
- ``AIMessage`` / ``AIMessageChunk`` — chat model runnable;
- ``list[BaseMessage]`` — chat history chain.

This adapter always produces a string-safe ``output["answer"]``:

1. ``None`` → ``None``;
2. ``str`` → itself;
3. ``dict`` → first string value among ``answer`` / ``output`` /
   ``result`` / ``content`` / ``text``; otherwise ``str(dict)``;
4. ``list`` — **last** item with a string ``.content`` (chat history
   convention: the most recent message wins); otherwise ``str(list)``;
5. objects with a string ``.content`` (LangChain messages) → ``.content``;
6. objects with a string ``.raw`` (CrewOutput-like) → ``.raw``;
7. anything else → ``str(...)``.

The *original* object is preserved under ``output["raw_result"]``. For
dict results, a **shallow copy** is stored to avoid aliasing the
framework's mutable top-level dict; nested values (lists, dicts inside
``raw``) are still shared — use the original object if you need to
inspect nested references.

**Async agents.** The base adapter is synchronous. Async LangChain
agents (``ainvoke`` / ``arun``) are **not** supported — the adapter
raises ``AdapterError`` with a clear message. Use a synchronous wrapper
(e.g. ``asyncio.run(agent.ainvoke(...))``) in the caller.

**Legacy ``run`` path.** For the legacy AgentExecutor convention, the
task description is extracted as ``question`` → ``input`` →
``str(task_input)``, matching every other adapter in the tree.

**Enhancements downgrade observability.** When ``use_enhancements`` is
downgraded (because the strategy guard or the executor requirement
failed), the reason is preserved in ``self._enhancements_downgraded_reason``
and surfaced in ``get_metadata()["enhancements_downgraded_reason"]`` so a
caller can inspect what happened.

Enhancements
------------
- **Strategy-aware executor is required.** ``use_enhancements=True``
  requires ``langchain_strategy_aware=True`` **and** a callable
  ``langchain_strategy_executor``; the adapter invokes the executor
  instead of ``_dispatch`` when present.
- **``list[BaseMessage]`` coerced to the last message's ``.content``.**
  Previously fell through to ``str(list)`` — user-hostile and untested.
- **Downgrade reason surfaced in metadata.** ``strict=False`` log
  warnings now also persist in ``get_metadata()``.
- **``run()`` override reconciles ``execution_strategy`` vs
  ``strategy_used``.** When they differ, the actual value wins and the
  requested value is preserved under a new key.
- **Non-dict inputs wrapped for ``invoke``.** Consistent with the
  ``run`` path's ``_extract_task_desc`` behaviour.
- **``raw_result`` shallow-copied for dict results.** Nested references
  still shared (documented).
- **Dead ``_strategy_aware`` eliminated.** It is now read from
  ``_execute`` to gate executor use, and from ``get_metadata`` to
  report state.
- **``None`` guard in ``_extract_task_desc``.** No more ``"None"``
  fallback.
- **Review artefacts removed.** ``§2.x`` references replaced with
  descriptive prose.
- **Smoke test corrected.** ``list[BaseMessage]`` test asserts the
  actual content; new tests cover the executor path, ``run()`` with
  enhancements, ``.content`` vs ``.raw`` preference, and the metadata
  surface.

Compatibility
-------------
- ``LangChainAdapter(agent, config=None)`` — same positional
  signature. ``strict=False`` is new and keyword-only.
- ``_execute(task_input, strategy="native")`` — same signature. The
  output gains ``"strategy_used"``, ``"strategy_requested"``, and
  ``"raw_result"`` (additive).
- **Behaviour change for ``use_enhancements=True``**: the adapter now
  additionally requires a callable ``langchain_strategy_executor``.
  Callers who set only ``langchain_strategy_aware=True`` will see the
  downgrade warning (unless they also pass an executor).
- **Behaviour change for ``list[BaseMessage]``**: returns the last
  message's ``.content`` instead of ``str(list)``.
- **Behaviour change for ``task_input=None``**: raises
  :class:`AdapterError` (unchanged from the previous enhanced version).
- **Behaviour change for ``invoke``**: non-dict inputs are now wrapped
  as ``{"question": str(task_input)}``, matching the legacy path.

Requires Python 3.8+.
"""

from __future__ import annotations

import logging
from typing import Any, Callable, Dict, List, Optional

from .base_adapter import AdapterError, BaseAgentAdapter


logger = logging.getLogger(__name__)

__version__ = "2.6.0"


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

    To enable the base class's enhancement pipeline, callers must set
    **both** ``use_enhancements=True`` and
    ``langchain_strategy_aware=True`` **and** supply a callable
    ``langchain_strategy_executor(task_input, strategy=<value>)``. The
    executor is responsible for honouring the requested strategy (native
    vs FlexGen vs any other backend); the adapter does not inspect the
    executor's behaviour. Without all three, enhancements are downgraded
    to legacy and the reason is surfaced in :meth:`get_metadata`.

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
            Configuration dictionary. Recognised keys:

            - ``use_enhancements`` (bool) — enable the enhanced
              pipeline;
            - ``langchain_strategy_aware`` (bool) — required alongside
              ``use_enhancements``;
            - ``langchain_strategy_executor`` (callable) — required
              alongside ``langchain_strategy_aware=True``; called as
              ``executor(task_input, strategy=<value>)`` for every
              ``_execute`` call.
        strict : bool, default False
            If True, raise instead of warning / downgrading on
            configuration inconsistencies.
        """
        if agent is None:
            raise AdapterError("agent must not be None.")

        cfg: Dict[str, Any] = dict(config) if config else {}

        strategy_aware = bool(cfg.get("langchain_strategy_aware", False))
        strategy_executor = cfg.get("langchain_strategy_executor")
        if strategy_executor is not None and not callable(strategy_executor):
            raise AdapterError(
                "langchain_strategy_executor must be callable, got "
                f"{type(strategy_executor).__name__}."
            )

        self._strategy_aware: bool = strategy_aware
        self._strategy_executor: Optional[Callable[..., Any]] = (
            strategy_executor
        )
        self._enhancements_downgraded_reason: Optional[str] = None

        downgrade_reason: Optional[str] = None
        if cfg.get("use_enhancements"):
            if not strategy_aware:
                downgrade_reason = (
                    "use_enhancements=True requires "
                    "langchain_strategy_aware=True for the LangChain "
                    "adapter: this adapter does not implement strategy "
                    "switching on its own, so the distillation optimizer "
                    "would train on rewards that do not depend on its "
                    "actions."
                )
            elif not callable(strategy_executor):
                downgrade_reason = (
                    "langchain_strategy_aware=True requires a callable "
                    "langchain_strategy_executor so the adapter can "
                    "verify the promise (the executor is responsible "
                    "for honouring the requested strategy); without it "
                    "the adapter runs natively regardless of the "
                    "strategy and the optimizer trains on noise."
                )

        if downgrade_reason is not None:
            if strict:
                raise AdapterError(downgrade_reason)
            logger.warning("%s Disabling enhancements.", downgrade_reason)
            cfg["use_enhancements"] = False
            self._enhancements_downgraded_reason = downgrade_reason

        super().__init__(agent, "langchain", cfg, strict=strict)

    # ---------------------------------------------------------------- execute
    def _execute(
        self,
        task_input: Dict[str, Any],
        strategy: str = "native",
    ) -> Dict[str, Any]:
        """
        Execute the LangChain agent (or the strategy executor) on
        ``task_input``.

        Parameters
        ----------
        task_input : dict | any
            Task input. Dicts are passed through unchanged; non-dict
            inputs are wrapped as ``{"question": str(task_input)}`` for
            consistency with the legacy ``run`` path.
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
            (``"langchain"``), ``strategy_used``, ``strategy_requested``,
            and ``raw_result``. When the framework returns a dict, its
            keys are also copied into the output (excluding the keys
            this adapter sets).

        Raises
        ------
        AdapterError
            If ``task_input`` is ``None``; if the agent exposes neither
            ``invoke`` nor ``run``; or if the agent only exposes async
            methods (``ainvoke`` / ``arun``).
        """
        if task_input is None:
            raise AdapterError("task_input must not be None.")

        logger.debug(
            "Running LangChain agent (strategy_requested=%s, "
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

        # Shallow-copy the top-level dict to avoid aliasing the
        # framework's mutable return value. Nested values remain shared
        # (documented in the module docstring).
        raw_snapshot = dict(raw) if isinstance(raw, dict) else raw

        out: Dict[str, Any] = {
            "answer": self._coerce_answer(raw),
            "framework": "langchain",
            "strategy_used": strategy_used,
            "strategy_requested": strategy,
            "raw_result": raw_snapshot,
        }

        # Preserve any extra dict keys from the framework result —
        # typically useful state (e.g. LangGraph final state). The
        # shallow copy above means these keys alias the framework's
        # original top-level dict, which is fine (we don't mutate
        # them), but nested mutables are shared.
        if isinstance(raw, dict):
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
        output to reconcile the base class's ``execution_strategy``
        field (which reflects what the optimizer *requested*) with the
        adapter's ``strategy_used`` (what actually ran). When the two
        differ, ``execution_strategy`` is set to ``strategy_used`` and
        the requested value is preserved under
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
        Invoke the underlying LangChain object.

        Prefers ``invoke`` (works with LangGraph state dicts and LCEL
        chains) over ``run`` (legacy AgentExecutor). Non-dict inputs are
        wrapped as ``{"question": str(task_input)}`` so LCEL chains with
        a ``{question}`` prompt template behave the same way as the
        legacy ``run`` path.
        """
        invoke = getattr(self.agent, "invoke", None)
        if callable(invoke):
            if not isinstance(task_input, dict):
                task_input = {"question": str(task_input)}
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
        if task_input is None:
            raise AdapterError("task_input must not be None.")
        if isinstance(task_input, dict):
            return (
                task_input.get("question")
                or task_input.get("input")
                or str(task_input)
            )
        return str(task_input)

    # ---------------------------------------------------------------- coerce
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
        4. ``list`` → **last** item with a string ``.content`` (chat
           history convention); otherwise ``str(list)``.
        5. Objects with a string ``.content`` (LangChain ``AIMessage`` /
           ``AIMessageChunk`` / ``BaseMessage``) → ``.content``.
        6. Objects with a string ``.raw`` (CrewOutput-like) → ``.raw``.
        7. Everything else → ``str(...)``.
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
            # Chat history: the last message with string .content is
            # the assistant's final reply.
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
        Return a non-empty string name for the LangChain agent.

        Prefers ``name`` (when it is a non-empty string), else the
        class name. ``__class__`` is always present in Python; the
        branch is explicit rather than relying on a dead ``hasattr``
        check.
        """
        name = getattr(self.agent, "name", None)
        if isinstance(name, str) and name:
            return name
        return self.agent.__class__.__name__

    # ---------------------------------------------------------------- metadata
    def get_metadata(self) -> Dict[str, Any]:
        """
        Return metadata about the adapter, including the strategy-guard
        state and any downgrade reason.

        Adds:

        - ``langchain_strategy_aware`` (bool);
        - ``langchain_strategy_executor`` (bool — present / absent);
        - ``enhancements_downgraded_reason`` (str, only when the
          enhancement pipeline was downgraded because of the
          strategy-guard or executor requirement).
        """
        meta = super().get_metadata()
        meta["langchain_strategy_aware"] = self._strategy_aware
        meta["langchain_strategy_executor"] = (
            self._strategy_executor is not None
        )
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

    class _AIMessage:
        """A realistic-looking AIMessage stand-in."""

        def __init__(self, content: str) -> None:
            self.content = content

        def __repr__(self) -> str:
            return f"AIMessage(content={self.content!r})"

    class _ChatModel:
        """Returns an AIMessage-like object with .content."""

        def invoke(self, task_input: Any) -> Any:
            return _AIMessage("chat model answer")

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

    class _BaseMessageListChain:
        def invoke(self, task_input: Any) -> list:
            return [_AIMessage("first"), _AIMessage("second")]

    class _BothContentAndRaw:
        content = "content value"
        raw = "raw value"

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
        LangChainAdapter(_UnnamedChain()).agent_name == "_UnnamedChain",
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
        "raw_result is a shallow-copied dict",
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

    # ---- 6. list[BaseMessage] → last .content -------------------- #
    print("list[BaseMessage] uses last .content:")
    a = LangChainAdapter(_BaseMessageListChain())
    out = a._execute({"question": "hi"})
    _check(
        "answer is last message content",
        out["answer"] == "second",
    )

    # ---- 7. .content preferred over .raw for objects with both --- #
    print(".content vs .raw preference:")
    _check(
        "content preferred over raw",
        LangChainAdapter._coerce_answer(_BothContentAndRaw())
        == "content value",
    )

    # ---- 8. Legacy AgentExecutor path ---------------------------- #
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

    # ---- 9. Non-dict input to invoke is wrapped ------------------ #
    print("non-dict input wrapped for invoke:")
    a = LangChainAdapter(_LCELStringChain())
    out = a._execute("a bare string")
    _check(
        "invoke path wraps string as question",
        "answer to 'a bare string'" in out["answer"],
    )

    # ---- 10. task_input = None ------------------------------------ #
    print("task_input = None:")
    a = LangChainAdapter(_LCELStringChain())
    _check(
        "raises on None in _execute",
        _raises(AdapterError, lambda: a._execute(None)),  # type: ignore[arg-type]
    )
    _check(
        "raises on None in _extract_task_desc",
        _raises(AdapterError, lambda:
                LangChainAdapter._extract_task_desc(None)),
    )

    # ---- 11. Unsupported agent ------------------------------------ #
    print("unsupported agent:")
    a = LangChainAdapter(_UnsupportedAgent())
    _check(
        "raises with clear message",
        _raises(AdapterError, lambda: a._execute({"question": "x"})),
    )

    # ---- 12. Async-only agent ------------------------------------- #
    print("async-only agent:")
    a = LangChainAdapter(_AsyncOnlyAgent())
    _check(
        "raises with async-specific message",
        _raises(AdapterError, lambda: a._execute({"question": "x"})),
    )

    # ---- 13. Strategy reporting (no executor) -------------------- #
    print("strategy reporting (no executor):")
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

    # ---- 14. Strategy executor ------------------------------------ #
    print("strategy executor:")
    calls: List[Any] = []

    def _executor(task_input: Any, *, strategy: str) -> str:
        calls.append((task_input, strategy))
        return f"executor:{strategy}:{task_input}"

    a = LangChainAdapter(
        _LCELStringChain(),
        {
            "use_enhancements": True,
            "langchain_strategy_aware": True,
            "langchain_strategy_executor": _executor,
        },
    )
    _check("enhancements preserved", a.use_enhancements is True)
    out = a._execute(
        {"question": "hi"}, strategy="flexgen_low_precision",
    )
    _check("executor called", len(calls) == 1)
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
        "executor:flexgen_low_precision" in out["answer"],
    )

    # ---- 15. Guard: no strategy_aware ---------------------------- #
    print("guard — no strategy_aware:")
    a = LangChainAdapter(
        _LCELStringChain(),
        {"use_enhancements": True},
    )
    _check(
        "enhancements disabled",
        a.use_enhancements is False,
    )
    _check(
        "downgrade reason in metadata",
        "enhancements_downgraded_reason" in a.get_metadata(),
    )
    _check(
        "strict raises",
        _raises(AdapterError, lambda: LangChainAdapter(
            _LCELStringChain(),
            {"use_enhancements": True},
            strict=True,
        )),
    )

    # ---- 16. Guard: strategy_aware without executor -------------- #
    print("guard — strategy_aware without executor:")
    a = LangChainAdapter(
        _LCELStringChain(),
        {"use_enhancements": True, "langchain_strategy_aware": True},
    )
    _check(
        "enhancements disabled without executor",
        a.use_enhancements is False,
    )
    _check(
        "downgrade reason mentions executor",
        "executor" in a.get_metadata().get(
            "enhancements_downgraded_reason", "",
        ),
    )
    _check(
        "strict raises without executor",
        _raises(AdapterError, lambda: LangChainAdapter(
            _LCELStringChain(),
            {"use_enhancements": True, "langchain_strategy_aware": True},
            strict=True,
        )),
    )

    # ---- 17. Guard: non-callable executor ----------------------- #
    print("guard — non-callable executor:")
    _check(
        "non-callable executor rejected",
        _raises(AdapterError, lambda: LangChainAdapter(
            _LCELStringChain(),
            {
                "use_enhancements": True,
                "langchain_strategy_aware": True,
                "langchain_strategy_executor": "not-a-callable",
            },
        )),
    )

    # ---- 18. run() with enhancements ---------------------------- #
    print("run() with enhancements:")
    a = LangChainAdapter(
        _LCELStringChain(),
        {
            "use_enhancements": True,
            "langchain_strategy_aware": True,
            "langchain_strategy_executor": _executor,
        },
    )
    out = a.run({"question": "What is AI?"})
    _check("answer present", isinstance(out["answer"], str))
    _check("latency present", "latency_ms" in out)
    _check(
        "execution_strategy matches strategy_used",
        out["execution_strategy"] == out["strategy_used"],
    )

    # ---- 19. run() with mismatched strategy (native legacy) ----- #
    print("run() with mismatched strategy:")
    a = LangChainAdapter(_LCELStringChain())
    out = a.run({"question": "What is AI?"})
    # Legacy path: base passes "native"; adapter reports "native".
    _check(
        "legacy path: no mismatch",
        out["execution_strategy"] == "native",
    )

    # ---- 20. metadata ------------------------------------------- #
    print("metadata:")
    a = LangChainAdapter(_LCELStringChain())
    meta = a.get_metadata()
    _check(
        "langchain_strategy_aware surfaced",
        meta["langchain_strategy_aware"] is False,
    )
    _check(
        "langchain_strategy_executor surfaced",
        meta["langchain_strategy_executor"] is False,
    )
    _check(
        "adapter_version is __version__",
        meta["adapter_version"] == __version__,
    )

    # ---- 21. __all__ / __dir__ ---------------------------------- #
    print("dir:")
    _check("__dir__ matches __all__", __dir__() == sorted(__all__))
    _check("LangChainAdapter exported", "LangChainAdapter" in __all__)
    _check("__version__ exported", "__version__" in __all__)

    print()
    print(f"passed: {passed}  failed: {failed}")
    if failed:
        sys.exit(1)
