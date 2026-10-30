# src/qcs/classical_token_translator.py

"""
Quantum-to-classical token translation (v2).

Translates quantum measurement outcomes into classical tokens for
downstream consumption by Layer 3.

Breaking changes from v1
------------------------

* ``translate_outcomes`` and ``validate_tokens`` are now *synchronous*.
  They perform CPU-bound work (counting, filtering, formatting) with no
  I/O. Marking them ``async`` in v1 did not make them non-blocking; it
  merely blocked the event loop behind a misleading interface. For
  callers that need to run translation off the main thread, use the new
  ``translate_outcomes_async`` / ``validate_tokens_async`` wrappers,
  which use ``asyncio.to_thread``.

* ``token_mapping_config`` is now actually consulted. A ``handlers``
  sub-key may contain per-task-type callables with signature
  ``(outcome: int, context: Mapping) -> str``. Custom handlers override
  the built-in ``optimization`` / ``classification`` / ``default`` paths.

* ``classical_context`` may no longer contain the reserved keys
  ``thought_signature``, ``outcome_count``, or ``total_outcomes``.
  Previous versions silently overwrote them; v2 raises ``ValueError``.

* ``TranslatedToken`` and ``TranslationResult`` are frozen dataclasses
  with validation. Existing consumers that mutated tokens in place will
  raise on attribute assignment.

* ``token_id`` is now globally unique (``token_<request_id>_<outcome>``),
  not just per-run (``token_<outcome>``).

* numpy is now an *optional* dependency — ``collections.Counter`` is
  used for outcome counting, so this module imports on pure-Python
  deployments. Any iterable of integers (including ``np.ndarray``) is
  accepted.

New public API
--------------

* ``translate_outcomes_detailed(...)`` returns a ``TranslationResult``
  with the token tuple, dropped outcomes, threshold used, and request id.
* ``translate_outcomes_detailed_async(...)`` is the async counterpart.
* ``ClassicalTokenTranslator.min_confidence`` can be overridden per call
  via the ``min_confidence=`` kwarg.
* ``TranslatedToken.frequency`` is a semantically accurate alias for
  ``confidence`` — the value is the empirical measurement frequency, not
  a posterior confidence.
"""

from __future__ import annotations

import asyncio
import json
import logging
import types
import uuid
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
)

logger = logging.getLogger(__name__)

__version__ = "2.0.0"


# =============================================================================
# Helpers
# =============================================================================

_RESERVED_CONTEXT_KEYS: frozenset = frozenset({
    "thought_signature",
    "outcome_count",
    "total_outcomes",
})


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _utcnow_iso() -> str:
    return _utcnow().isoformat()


def _is_json_serializable(obj: Any) -> Tuple[bool, Optional[str]]:
    """Return ``(ok, error_message)``. Does not mutate ``obj``."""
    try:
        json.dumps(obj)
        return True, None
    except (TypeError, ValueError) as e:
        return False, str(e)


# =============================================================================
# Data classes
# =============================================================================

@dataclass(frozen=True)
class TranslatedToken:
    """A classical token translated from a quantum measurement outcome.

    Attributes
    ----------
    token_id
        Globally unique identifier of the form
        ``token_<request_id>_<outcome>``. Two calls to
        :meth:`ClassicalTokenTranslator.translate_outcomes` with the same
        outcomes produce distinct IDs.
    token_value
        The translated classical value (e.g. ``"solution_1011"``).
    confidence
        Empirical measurement frequency of the underlying quantum outcome
        (``count / total_outcomes``). This is *not* a posterior
        "confidence" — it approaches ``|amplitude|^2`` in the limit of
        many shots. Use :attr:`frequency` for a semantically accurate
        name.
    quantum_origin
        Human-readable description of the underlying measurement outcome,
        e.g. ``"outcome_5"`` or (aggregated) ``"outcomes_[0, 2, 4]"``.
    classical_semantics
        Immutable mapping of context. Contains the reserved keys
        ``thought_signature``, ``outcome_count``, and ``total_outcomes``,
        plus every non-reserved key from the caller's
        ``classical_context``.
    """

    token_id: str
    token_value: str
    confidence: float
    quantum_origin: str
    classical_semantics: Mapping[str, Any]

    def __post_init__(self) -> None:
        if not isinstance(self.token_id, str) or not self.token_id:
            raise ValueError("token_id must be a non-empty string")
        if not isinstance(self.token_value, str) or not self.token_value:
            raise ValueError("token_value must be a non-empty string")
        if not isinstance(self.confidence, (int, float)):
            raise TypeError("confidence must be numeric")
        if not (0.0 <= float(self.confidence) <= 1.0):
            raise ValueError(
                f"confidence must be in [0, 1], got {self.confidence}"
            )
        if not isinstance(self.quantum_origin, str) or not self.quantum_origin:
            raise ValueError("quantum_origin must be a non-empty string")
        if not isinstance(self.classical_semantics, Mapping):
            raise TypeError("classical_semantics must be a Mapping")
        if not isinstance(self.classical_semantics, types.MappingProxyType):
            object.__setattr__(
                self,
                "classical_semantics",
                types.MappingProxyType(dict(self.classical_semantics)),
            )

    @property
    def frequency(self) -> float:
        """Empirical measurement frequency — alias for :attr:`confidence`."""
        return self.confidence

    def to_dict(self) -> Dict[str, Any]:
        return {
            "token_id": self.token_id,
            "token_value": self.token_value,
            "confidence": self.confidence,
            "quantum_origin": self.quantum_origin,
            "classical_semantics": dict(self.classical_semantics),
        }


@dataclass(frozen=True)
class TranslationResult:
    """Full translation result — tokens + diagnostics.

    Returned by :meth:`ClassicalTokenTranslator.translate_outcomes_detailed`.
    """

    tokens: Tuple[TranslatedToken, ...]
    dropped: Tuple[Tuple[int, float], ...]
    """Outcomes filtered below the confidence threshold: ``(outcome, confidence)``."""

    total_outcomes: int
    total_unique: int
    threshold_used: float
    request_id: str
    at: str = field(default_factory=_utcnow_iso)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "tokens": [t.to_dict() for t in self.tokens],
            "dropped": [
                {"outcome": int(o), "confidence": float(c)}
                for o, c in self.dropped
            ],
            "total_outcomes": self.total_outcomes,
            "total_unique": self.total_unique,
            "threshold_used": self.threshold_used,
            "request_id": self.request_id,
            "at": self.at,
        }


# =============================================================================
# Translator
# =============================================================================

class ClassicalTokenTranslator:
    """Quantum-to-classical translation engine.

    Responsibilities
    ----------------
    * Translate quantum measurement outcomes into classical tokens.
    * Preserve meta-cognitive context (``thought_signature``) and any
      caller-supplied ``classical_context``.
    * Ensure tokens are interpretable by Layer 3.

    Parameters
    ----------
    token_mapping_config
        Optional mapping. Supported keys:

        * ``handlers``: a ``Mapping[str, Callable[[int, Mapping], str]]``
          keyed by ``task_type``. A matching handler is invoked in
          preference to the built-in ``optimization`` / ``classification``
          behavior.
    min_confidence_threshold
        Minimum empirical frequency for an outcome to produce a token.
        Must be in ``[0, 1]``.
    strict_serialization
        If True, raise ``ValueError`` when ``classical_context`` is not
        JSON-serializable. If False (default), log a warning.
    strict_handlers
        If True, a failing custom handler raises. If False (default), the
        handler's failure is logged and the built-in path is used.
    aggregate_by_value_default
        Default value for the per-call ``aggregate_by_value`` kwarg.
    """

    def __init__(
        self,
        token_mapping_config: Optional[Mapping[str, Any]] = None,
        min_confidence_threshold: float = 0.80,
        *,
        strict_serialization: bool = False,
        strict_handlers: bool = False,
        aggregate_by_value_default: bool = False,
    ):
        # --- min_confidence_threshold ---
        if not isinstance(min_confidence_threshold, (int, float)):
            raise TypeError("min_confidence_threshold must be numeric")
        if not (0.0 <= float(min_confidence_threshold) <= 1.0):
            raise ValueError(
                f"min_confidence_threshold must be in [0, 1], got "
                f"{min_confidence_threshold}"
            )

        # --- token_mapping_config ---
        if token_mapping_config is None:
            token_mapping_config = {}
        if not isinstance(token_mapping_config, Mapping):
            raise TypeError("token_mapping_config must be a Mapping")
        self.token_mapping: Mapping[str, Any] = types.MappingProxyType(
            dict(token_mapping_config)
        )

        handlers_raw = self.token_mapping.get("handlers", {})
        if not isinstance(handlers_raw, Mapping):
            raise TypeError(
                "token_mapping_config['handlers'] must be a Mapping"
            )
        for name, fn in handlers_raw.items():
            if not callable(fn):
                raise TypeError(
                    f"handler for task_type={name!r} is not callable"
                )
        self._handlers: Mapping[str, Callable[[int, Mapping], str]] = \
            types.MappingProxyType(dict(handlers_raw))

        # --- settings ---
        self.min_confidence: float = float(min_confidence_threshold)
        self.strict_serialization: bool = bool(strict_serialization)
        self.strict_handlers: bool = bool(strict_handlers)
        self.aggregate_by_value_default: bool = bool(
            aggregate_by_value_default
        )

        logger.debug(
            "ClassicalTokenTranslator initialized "
            "(min_confidence=%.2f, handlers=%s, strict_serialization=%s)",
            self.min_confidence,
            sorted(self._handlers.keys()),
            self.strict_serialization,
        )

    # ------------------------------------------------------------------
    # Public sync API
    # ------------------------------------------------------------------

    def translate_outcomes(
        self,
        measurement_outcomes: Sequence[int],
        classical_context: Mapping[str, Any],
        thought_signature: str,
        *,
        min_confidence: Optional[float] = None,
        aggregate_by_value: Optional[bool] = None,
        request_id: Optional[str] = None,
    ) -> List[TranslatedToken]:
        """Translate measurement outcomes to classical tokens.

        Parameters
        ----------
        measurement_outcomes
            Iterable of non-negative integers (``list``, ``tuple``,
            ``np.ndarray``, etc.). Must not be empty.
        classical_context
            Caller-supplied context. May not contain reserved keys.
        thought_signature
            Meta-cognitive signature — non-empty string.
        min_confidence
            Override the instance-level threshold for this call.
        aggregate_by_value
            If True, outcomes that translate to the same ``token_value``
            are merged into a single token with summed confidence.
            Defaults to ``self.aggregate_by_value_default``.
        request_id
            Optional explicit request id; auto-generated when omitted.
            Used to make ``token_id`` globally unique.

        Returns
        -------
        List[TranslatedToken]
            Empty list if no outcome met the confidence threshold (the
            dropped outcomes are still tracked — use
            :meth:`translate_outcomes_detailed` to inspect them).

        Raises
        ------
        ValueError
            On empty input, negative outcomes, reserved-key collisions in
            ``classical_context``, invalid ``n_bits`` overflow, or invalid
            ``min_confidence`` override.
        TypeError
            On non-integer outcome values or invalid argument types.
        """
        result = self.translate_outcomes_detailed(
            measurement_outcomes=measurement_outcomes,
            classical_context=classical_context,
            thought_signature=thought_signature,
            min_confidence=min_confidence,
            aggregate_by_value=aggregate_by_value,
            request_id=request_id,
        )
        return list(result.tokens)

    def translate_outcomes_detailed(
        self,
        measurement_outcomes: Sequence[int],
        classical_context: Mapping[str, Any],
        thought_signature: str,
        *,
        min_confidence: Optional[float] = None,
        aggregate_by_value: Optional[bool] = None,
        request_id: Optional[str] = None,
    ) -> TranslationResult:
        """Like :meth:`translate_outcomes`, but returns a
        :class:`TranslationResult` including dropped outcomes and
        diagnostics."""
        # ---- Validate inputs ----
        arr = self._coerce_outcomes(measurement_outcomes)
        if not isinstance(thought_signature, str) or not thought_signature:
            raise ValueError("thought_signature must be a non-empty string")
        if not isinstance(classical_context, Mapping):
            raise TypeError("classical_context must be a Mapping")

        overlap = _RESERVED_CONTEXT_KEYS & set(classical_context.keys())
        if overlap:
            raise ValueError(
                f"classical_context contains reserved keys: "
                f"{sorted(overlap)}. These are set by the translator: "
                f"{sorted(_RESERVED_CONTEXT_KEYS)}"
            )

        if self.strict_serialization:
            ok, err = _is_json_serializable(dict(classical_context))
            if not ok:
                raise ValueError(
                    f"classical_context is not JSON-serializable: {err}"
                )
        else:
            ok, err = _is_json_serializable(dict(classical_context))
            if not ok:
                logger.warning(
                    "classical_context is not JSON-serializable "
                    "(%s) — downstream serialization may fail",
                    err,
                )

        # ---- Resolve per-call options ----
        threshold = (
            self.min_confidence
            if min_confidence is None
            else float(min_confidence)
        )
        if not (0.0 <= threshold <= 1.0):
            raise ValueError(
                f"min_confidence must be in [0, 1], got {threshold}"
            )
        do_aggregate = (
            self.aggregate_by_value_default
            if aggregate_by_value is None
            else bool(aggregate_by_value)
        )
        if request_id is None:
            request_id = uuid.uuid4().hex[:12]
        elif not isinstance(request_id, str) or not request_id:
            raise ValueError("request_id must be a non-empty string")

        # ---- Count and filter ----
        counter = Counter(arr)
        total = len(arr)

        surviving: List[Tuple[int, float, str]] = []
        dropped: List[Tuple[int, float]] = []
        for outcome in sorted(counter.keys()):
            conf = counter[outcome] / total
            if conf < threshold:
                dropped.append((outcome, conf))
                continue
            token_value = self._outcome_to_token(outcome, classical_context)
            surviving.append((outcome, conf, token_value))

        # ---- Build tokens ----
        tokens: List[TranslatedToken] = []

        if do_aggregate:
            by_value: Dict[str, List[Tuple[int, float]]] = defaultdict(list)
            for outcome, conf, value in surviving:
                by_value[value].append((outcome, conf))

            # Detect collisions before aggregation (they're the reason
            # aggregation matters).
            for value, items in by_value.items():
                if len(items) > 1:
                    logger.info(
                        "aggregated %d outcomes into token_value=%r: %s",
                        len(items), value, sorted(o for o, _ in items),
                    )

            for value, items in by_value.items():
                outcomes = [o for o, _ in items]
                combined_conf = sum(c for _, c in items)
                combined_count = sum(counter[o] for o in outcomes)
                tokens.append(self._make_token(
                    request_id=request_id,
                    identifier=value,
                    token_value=value,
                    confidence=min(1.0, combined_conf),
                    origin_prefix="outcomes",
                    outcomes=outcomes,
                    count=combined_count,
                    total=total,
                    thought_signature=thought_signature,
                    classical_context=classical_context,
                ))
        else:
            # Detect token_value collisions (multiple outcomes mapping to
            # the same value) and warn — the caller may prefer
            # ``aggregate_by_value=True``.
            value_to_outcomes: Dict[str, List[int]] = defaultdict(list)
            for outcome, _, value in surviving:
                value_to_outcomes[value].append(outcome)
            for value, outcomes in value_to_outcomes.items():
                if len(outcomes) > 1:
                    logger.warning(
                        "token_value=%r produced by multiple outcomes %s — "
                        "consider aggregate_by_value=True",
                        value, sorted(outcomes),
                    )

            for outcome, conf, value in surviving:
                tokens.append(self._make_token(
                    request_id=request_id,
                    identifier=str(outcome),
                    token_value=value,
                    confidence=conf,
                    origin_prefix="outcome",
                    outcomes=[outcome],
                    count=counter[outcome],
                    total=total,
                    thought_signature=thought_signature,
                    classical_context=classical_context,
                ))

        # ---- Sort: confidence DESC, then token_id ASC for stable order ----
        tokens.sort(key=lambda t: (-t.confidence, t.token_id))

        # ---- Log + warn ----
        logger.debug(
            "Translated %d outcomes → %d tokens "
            "(dropped=%d, threshold=%.2f, request_id=%s)",
            total, len(tokens), len(dropped), threshold, request_id,
        )
        if total > 0 and not tokens:
            logger.warning(
                "No tokens produced: all %d outcomes below threshold %.2f",
                total, threshold,
            )

        return TranslationResult(
            tokens=tuple(tokens),
            dropped=tuple(dropped),
            total_outcomes=total,
            total_unique=len(counter),
            threshold_used=threshold,
            request_id=request_id,
        )

    def validate_tokens(
        self,
        tokens: Sequence[TranslatedToken],
        expected_semantics: Mapping[str, Any],
    ) -> bool:
        """Validate that every token carries the expected semantic fields.

        For each token and each ``(key, expected_value)`` pair in
        ``expected_semantics``:

        * If ``key`` is missing from ``token.classical_semantics``, returns
          False (with a warning).
        * If the value doesn't match, returns False (with a warning).

        Also asserts ``token_value`` is non-empty (defensive — the
        ``TranslatedToken`` constructor already enforces this).

        Returns
        -------
        bool
            True if every check passed, False otherwise.
        """
        if not isinstance(tokens, Sequence):
            raise TypeError("tokens must be a Sequence")
        if not isinstance(expected_semantics, Mapping):
            raise TypeError("expected_semantics must be a Mapping")

        for i, token in enumerate(tokens):
            if not isinstance(token, TranslatedToken):
                logger.warning(
                    "tokens[%d] is not a TranslatedToken: %r",
                    i, type(token).__name__,
                )
                return False

            for key, expected_value in expected_semantics.items():
                if key not in token.classical_semantics:
                    logger.warning(
                        "token[%d] (id=%s) missing required semantic "
                        "key %r",
                        i, token.token_id, key,
                    )
                    return False
                actual = token.classical_semantics[key]
                if actual != expected_value:
                    logger.warning(
                        "token[%d] (id=%s) semantic %r: expected %r, got %r",
                        i, token.token_id, key, expected_value, actual,
                    )
                    return False

            if not token.token_value:
                logger.warning(
                    "token[%d] (id=%s) has empty token_value",
                    i, token.token_id,
                )
                return False

        return True

    # ------------------------------------------------------------------
    # Public async API (thin wrappers — CPU work runs in a thread)
    # ------------------------------------------------------------------

    async def translate_outcomes_async(
        self,
        measurement_outcomes: Sequence[int],
        classical_context: Mapping[str, Any],
        thought_signature: str,
        **kwargs: Any,
    ) -> List[TranslatedToken]:
        """Async wrapper around :meth:`translate_outcomes`."""
        return await asyncio.to_thread(
            self.translate_outcomes,
            measurement_outcomes,
            classical_context,
            thought_signature,
            **kwargs,
        )

    async def translate_outcomes_detailed_async(
        self,
        measurement_outcomes: Sequence[int],
        classical_context: Mapping[str, Any],
        thought_signature: str,
        **kwargs: Any,
    ) -> TranslationResult:
        """Async wrapper around :meth:`translate_outcomes_detailed`."""
        return await asyncio.to_thread(
            self.translate_outcomes_detailed,
            measurement_outcomes,
            classical_context,
            thought_signature,
            **kwargs,
        )

    async def validate_tokens_async(
        self,
        tokens: Sequence[TranslatedToken],
        expected_semantics: Mapping[str, Any],
    ) -> bool:
        """Async wrapper around :meth:`validate_tokens`."""
        return await asyncio.to_thread(
            self.validate_tokens, tokens, expected_semantics,
        )

    # ------------------------------------------------------------------
    # Internals — outcome preparation
    # ------------------------------------------------------------------

    @staticmethod
    def _coerce_outcomes(measurement_outcomes: Sequence[int]) -> List[int]:
        """Cast to a non-empty list of non-negative Python ints."""
        if measurement_outcomes is None:
            raise ValueError("measurement_outcomes must not be None")

        try:
            arr = [int(x) for x in measurement_outcomes]
        except (TypeError, ValueError) as e:
            raise TypeError(
                f"measurement_outcomes must be integer-valued: {e}"
            ) from e

        if not arr:
            raise ValueError("measurement_outcomes must not be empty")

        negatives = [x for x in arr if x < 0]
        if negatives:
            raise ValueError(
                f"measurement_outcomes must be non-negative; got "
                f"negative values {negatives[:5]}"
                + (" …" if len(negatives) > 5 else "")
            )

        return arr

    # ------------------------------------------------------------------
    # Internals — outcome → token_value
    # ------------------------------------------------------------------

    def _outcome_to_token(
        self,
        outcome: int,
        classical_context: Mapping[str, Any],
    ) -> str:
        """Translate a single integer outcome to a token value string.

        Custom handlers registered under ``token_mapping_config["handlers"]``
        take precedence over the built-in path. If a handler raises (and
        ``strict_handlers=False``), the failure is logged and the built-in
        path is used instead.
        """
        task_type = classical_context.get("task_type", "default")

        handler = self._handlers.get(task_type)
        if handler is not None:
            try:
                result = handler(outcome, classical_context)
            except Exception as e:
                logger.warning(
                    "custom handler for task_type=%r failed: %s",
                    task_type, e, exc_info=True,
                )
                if self.strict_handlers:
                    raise
            else:
                if isinstance(result, str) and result:
                    return result
                msg = (
                    f"custom handler for task_type={task_type!r} returned "
                    f"invalid value {result!r} (must be non-empty string)"
                )
                if self.strict_handlers:
                    raise TypeError(msg)
                logger.warning(msg)

        return self._default_outcome_to_token(
            outcome, task_type, classical_context,
        )

    @staticmethod
    def _default_outcome_to_token(
        outcome: int,
        task_type: str,
        classical_context: Mapping[str, Any],
    ) -> str:
        if task_type == "optimization":
            try:
                n_bits = int(classical_context.get("n_bits", 8))
            except (TypeError, ValueError) as e:
                raise ValueError(
                    f"n_bits must be an integer, got "
                    f"{classical_context.get('n_bits')!r}"
                ) from e
            if n_bits <= 0:
                raise ValueError(f"n_bits must be positive, got {n_bits}")
            max_val = 1 << n_bits
            if outcome >= max_val:
                raise ValueError(
                    f"outcome {outcome} does not fit in {n_bits} bits "
                    f"(valid range [0, {max_val}))"
                )
            binary = format(outcome, f"0{n_bits}b")
            return f"solution_{binary}"

        if task_type == "classification":
            classes = classical_context.get(
                "classes", ["class_0", "class_1"],
            )
            if not isinstance(classes, Sequence) or not classes:
                raise ValueError(
                    "classes must be a non-empty Sequence when "
                    "task_type='classification'"
                )
            class_idx = outcome % len(classes)
            return str(classes[class_idx])

        return f"outcome_{outcome}"

    # ------------------------------------------------------------------
    # Internals — token construction
    # ------------------------------------------------------------------

    @staticmethod
    def _make_token(
        *,
        request_id: str,
        identifier: str,
        token_value: str,
        confidence: float,
        origin_prefix: str,
        outcomes: List[int],
        count: int,
        total: int,
        thought_signature: str,
        classical_context: Mapping[str, Any],
    ) -> TranslatedToken:
        if len(outcomes) == 1:
            origin = f"{origin_prefix}_{outcomes[0]}"
        else:
            origin = f"{origin_prefix}_{sorted(outcomes)}"

        # Reserved keys first, then caller context (never overrides them —
        # the caller's context is validated against _RESERVED_CONTEXT_KEYS
        # at entry, but keep this defensive in case the caller mutates).
        semantics: Dict[str, Any] = {
            "thought_signature": thought_signature,
            "outcome_count": int(count),
            "total_outcomes": int(total),
        }
        for k, v in classical_context.items():
            if k not in _RESERVED_CONTEXT_KEYS:
                semantics[k] = v

        return TranslatedToken(
            token_id=f"token_{request_id}_{identifier}",
            token_value=token_value,
            confidence=min(1.0, max(0.0, float(confidence))),
            quantum_origin=origin,
            classical_semantics=semantics,
        )

    # ------------------------------------------------------------------
    # Repr
    # ------------------------------------------------------------------

    def __repr__(self) -> str:
        return (
            f"ClassicalTokenTranslator("
            f"min_confidence={self.min_confidence:.2f}, "
            f"handlers={sorted(self._handlers.keys())})"
        )


__all__ = [
    "ClassicalTokenTranslator",
    "TranslatedToken",
    "TranslationResult",
]


# =============================================================================
# Smoke test (python -m qcs.classical_token_translator)
# =============================================================================

if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    def _expect_raises(fn, exc_type, label: str) -> None:
        try:
            fn()
        except exc_type as e:
            print(f"  ✓ {label} raised {exc_type.__name__}: {e}")
        except Exception as e:
            print(f"  ✗ {label} raised {type(e).__name__} (expected "
                  f"{exc_type.__name__}): {e}")
        else:
            print(f"  ✗ {label} did not raise (expected {exc_type.__name__})")

    # ---- Basic translation ----
    print("\n=== Basic translation (optimization) ===")
    translator = ClassicalTokenTranslator(min_confidence_threshold=0.10)
    outcomes = [0, 1, 1, 2, 2, 2, 3, 3, 3, 3]
    tokens = translator.translate_outcomes(
        outcomes,
        {"task_type": "optimization", "n_bits": 4},
        "sig-basic",
    )
    for t in tokens:
        print(
            f"  {t.token_id:<24} value={t.token_value:<14} "
            f"conf={t.confidence:.2f} origin={t.quantum_origin}"
        )
    assert len(tokens) == 4
    assert all(t.token_id.startswith("token_") for t in tokens)
    # token IDs are unique
    assert len({t.token_id for t in tokens}) == 4

    # ---- Detailed result with dropped outcomes ----
    print("\n=== Detailed result (high threshold, some dropped) ===")
    result = translator.translate_outcomes_detailed(
        outcomes,
        {"task_type": "optimization", "n_bits": 4},
        "sig-detailed",
        min_confidence=0.25,  # drops outcome 0 (0.10) and 1 (0.20)
    )
    print(f"  request_id:      {result.request_id}")
    print(f"  tokens:          {len(result.tokens)}")
    print(f"  dropped:         {result.dropped}")
    print(f"  total_outcomes:  {result.total_outcomes}")
    print(f"  total_unique:    {result.total_unique}")
    print(f"  threshold_used:  {result.threshold_used}")
    assert len(result.tokens) == 2
    assert len(result.dropped) == 2

    # ---- Aggregation ----
    print("\n=== Aggregation (classification with collisions) ===")
    agg_translator = ClassicalTokenTranslator(
        min_confidence_threshold=0.10,
        aggregate_by_value_default=True,
    )
    # outcomes 0, 2, 4 all map to class_0 (with classes=['class_0','class_1'])
    class_outcomes = [0, 0, 1, 2, 2, 2, 4, 4, 4, 4]
    tokens = agg_translator.translate_outcomes(
        class_outcomes,
        {"task_type": "classification", "classes": ["class_0", "class_1"]},
        "sig-agg",
    )
    for t in tokens:
        print(
            f"  {t.token_id:<24} value={t.token_value:<10} "
            f"conf={t.confidence:.2f} origin={t.quantum_origin}"
        )
    assert len(tokens) == 2
    assert {t.token_value for t in tokens} == {"class_0", "class_1"}

    # ---- Custom handler ----
    print("\n=== Custom handler ===")

    def _custom_handler(outcome: int, ctx: Mapping[str, Any]) -> str:
        return f"custom_{outcome}_{ctx.get('tag', 'untagged')}"

    custom = ClassicalTokenTranslator(
        token_mapping_config={"handlers": {"custom_task": _custom_handler}},
        min_confidence_threshold=0.10,
    )
    tokens = custom.translate_outcomes(
        [0, 1, 2, 2, 2],
        {"task_type": "custom_task", "tag": "alpha"},
        "sig-custom",
    )
    for t in tokens:
        print(f"  {t.token_id:<24} value={t.token_value}")
    assert any(t.token_value.startswith("custom_") for t in tokens)

    # ---- Validation ----
    print("\n=== validate_tokens (real validation) ===")
    det = translator.translate_outcomes_detailed(
        outcomes,
        {"task_type": "optimization", "n_bits": 4, "region": "us-ca"},
        "sig-validate",
    )
    ok = translator.validate_tokens(
        det.tokens,
        {"task_type": "optimization", "region": "us-ca"},
    )
    print(f"  expected pass:  {ok}")
    assert ok is True

    ok = translator.validate_tokens(
        det.tokens,
        {"region": "eu-de"},  # wrong value
    )
    print(f"  expected fail:  {ok}")
    assert ok is False

    ok = translator.validate_tokens(
        det.tokens,
        {"missing_key": "anything"},  # missing key
    )
    print(f"  expected fail:  {ok}")
    assert ok is False

    # ---- Error cases ----
    print("\n=== Error cases ===")
    _expect_raises(
        lambda: translator.translate_outcomes(
            [], {}, "sig",
        ),
        ValueError, "empty outcomes",
    )
    _expect_raises(
        lambda: translator.translate_outcomes(
            [-1, 0], {}, "sig",
        ),
        ValueError, "negative outcome",
    )
    _expect_raises(
        lambda: translator.translate_outcomes(
            [0],
            {"thought_signature": "reserved_clash"},
            "sig",
        ),
        ValueError, "reserved-key collision",
    )
    _expect_raises(
        lambda: translator.translate_outcomes(
            [255],
            {"task_type": "optimization", "n_bits": 4},
            "sig",
        ),
        ValueError, "n_bits overflow",
    )
    _expect_raises(
        lambda: translator.translate_outcomes(
            [0], {}, "",
        ),
        ValueError, "empty thought_signature",
    )
    _expect_raises(
        lambda: ClassicalTokenTranslator(min_confidence_threshold=1.5),
        ValueError, "invalid min_confidence_threshold",
    )

    # ---- Async wrapper ----
    print("\n=== Async wrapper ===")

    async def _async_test() -> None:
        tokens = await translator.translate_outcomes_async(
            outcomes,
            {"task_type": "optimization", "n_bits": 4},
            "sig-async",
        )
        print(f"  async tokens:   {len(tokens)}")
        assert len(tokens) == 4
        ok = await translator.validate_tokens_async(
            tokens, {"task_type": "optimization"},
        )
        print(f"  async validate: {ok}")
        assert ok is True

    asyncio.run(_async_test())

    # ---- token_id uniqueness across calls ----
    print("\n=== token_id uniqueness across calls ===")
    t1 = translator.translate_outcomes(
        [0, 1], {"task_type": "optimization", "n_bits": 4}, "sig-run1",
    )
    t2 = translator.translate_outcomes(
        [0, 1], {"task_type": "optimization", "n_bits": 4}, "sig-run2",
    )
    ids1 = {t.token_id for t in t1}
    ids2 = {t.token_id for t in t2}
    print(f"  run1 ids: {sorted(ids1)}")
    print(f"  run2 ids: {sorted(ids2)}")
    assert ids1.isdisjoint(ids2), "token IDs must not collide across runs"

    # ---- Immutability ----
    print("\n=== Immutability ===")
    try:
        det.tokens[0].confidence = 0.99  # type: ignore[misc]
    except (AttributeError, Exception) as e:
        print(f"  ✓ token is frozen: {type(e).__name__}")
    try:
        det.tokens[0].classical_semantics["x"] = 1  # type: ignore[index]
    except TypeError as e:
        print(f"  ✓ classical_semantics is read-only: {type(e).__name__}")

    print("\nSmoke test passed.")
