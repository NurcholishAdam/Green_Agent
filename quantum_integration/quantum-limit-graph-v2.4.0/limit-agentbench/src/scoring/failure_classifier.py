# src/scoring/failure_classifier.py

"""
Failure Classifier
==================

Classifies agent failures for better debugging and RLHF reward shaping.

Categories
----------
- ``timeout``           — Agent exceeded time limit.
- ``oom``               — Out of memory during execution.
- ``tool_error``        — Failed to use required tools.
- ``invalid_output``    — Output format incorrect.
- ``hallucination``     — Generated false information.
- ``energy_exceeded``   — Exceeded energy budget.
- ``unknown``           — Fallback / unclassified failure.

Enhancements
------------
- ``FailureCategory`` — StrEnum (with a 3.8/3.9/3.10 fallback shim) so
  categories are type-safe and JSON-friendly without ``default=str``.
- ``FailureClassifierConfig`` — frozen, fully validated: category allowlist
  validated against real enum values; ``default_category`` must be
  ``UNKNOWN`` or a member of ``allowed_categories``; ``keyword_overrides``
  keys must be real categories; ``from_dict`` supports ``strict=True``.
- **Real ``total_classifications`` counter** and ``dropped_count`` —
  ``len(history)`` is decoupled from the lifetime total.
- **Word-boundary matching** — ``"oom"`` no longer matches ``"boom"`` or
  ``"room"``; ``"tool"`` no longer matches ``"toolbar"``.
- **Round-trip preserves history** — ``ClassificationRecord.from_dict``
  added; ``FailureClassifier.from_dict`` restores the deque.
- **Empty ``allowed_categories`` round-trips correctly** — "missing" is
  distinguished from "empty".
- **``CATEGORIES`` / ``FAILURE_CATEGORIES`` are frozen** via
  ``MappingProxyType``.
- ``classify_detailed()`` — returns ``ClassifyResult`` with
  ``matched_keyword``, ``matched_in``, ``used_default``.
- ``classify_batch()`` — amortises lock acquisition over many calls.
- Atomic ``statistics()`` — every field read under the lock.
- ``_started_at`` uptime semantics documented for restored classifiers.
- ``classify()`` still returns ``str`` (backward-compatible); strict mode
  now requires ``error`` to be a ``BaseException`` instance.
- ``__eq__`` / ``__hash__`` / ``__repr__`` / ``__slots__`` on the
  dataclasses and the classifier.
- ``to_json`` no longer uses ``default=str``.
- ``context`` accepted keys documented; unknown keys ignored.

Compatibility
-------------
- ``classify(error=None, context=None) -> str`` unchanged.
- ``CATEGORIES`` and ``FAILURE_CATEGORIES`` still exist — now read-only.
- ``ClassificationRecord.error_type`` is ``Optional[str]``; ``None``
  means "no exception was passed" (was previously the string
  ``"unknown"``). A restored record whose ``error_type`` was ``"unknown"``
  remains readable.

Requires Python 3.8+. ``slots=True`` on dataclasses (used here) requires
Python 3.10+; drop it if targeting older versions.
"""

from __future__ import annotations

import json
import logging
import re
import threading
import time
from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from types import MappingProxyType
from typing import (
    Any, Deque, Dict, Iterable, List, Mapping, Optional, Pattern, Sequence,
    Tuple, TypedDict,
)

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# StrEnum compatibility shim
# --------------------------------------------------------------------------- #
try:  # pragma: no cover — Python 3.11+
    from enum import StrEnum  # type: ignore[attr-defined]
except ImportError:  # pragma: no cover — 3.10 and earlier
    class StrEnum(str, Enum):  # type: ignore[no-redef]
        """``str`` + ``Enum`` with ``__str__``/``__format__`` delegating to ``str``."""

        __str__ = str.__str__
        __format__ = str.__format__


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class FailureClassifierError(ValueError):
    """Raised for invalid classifier inputs or configuration."""


# --------------------------------------------------------------------------- #
# Typed return shapes
# --------------------------------------------------------------------------- #
class ClassifyResult(TypedDict):
    """Structured result of :meth:`FailureClassifier.classify_detailed`."""

    category: str
    matched_keyword: Optional[str]
    matched_in: Optional[str]           # e.g. "message", "context.status"
    error_type: Optional[str]
    used_default: bool


class ClassifierStatistics(TypedDict):
    """Structured result of :meth:`FailureClassifier.statistics`."""

    total_classifications: int
    dropped_classifications: int
    buffered_classifications: int
    by_category: Dict[str, int]
    default_category: str
    strict: bool
    categories: Dict[str, str]
    uptime_seconds: float


# --------------------------------------------------------------------------- #
# Category enum
# --------------------------------------------------------------------------- #
_DESCRIPTIONS = MappingProxyType({
    "timeout":         "Agent exceeded time limit",
    "oom":             "Out of memory during execution",
    "tool_error":      "Failed to use required tools",
    "invalid_output":  "Output format incorrect",
    "hallucination":   "Generated false information",
    "energy_exceeded": "Exceeded energy budget",
    "unknown":         "Unclassified failure",
})


class FailureCategory(StrEnum):
    """Enumeration of agent failure categories."""

    TIMEOUT = "timeout"
    OOM = "oom"
    TOOL_ERROR = "tool_error"
    INVALID_OUTPUT = "invalid_output"
    HALLUCINATION = "hallucination"
    ENERGY_EXCEEDED = "energy_exceeded"
    UNKNOWN = "unknown"

    @property
    def description(self) -> str:
        return _DESCRIPTIONS[self.value]


_VALID_CATEGORY_VALUES = frozenset(cat.value for cat in FailureCategory)

# Read-only public mapping (backward-compatible with the original dict).
FAILURE_CATEGORIES: Mapping[str, str] = MappingProxyType(
    {cat.value: cat.description for cat in FailureCategory}
)


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
_DEFAULT_ALLOWED_CATEGORIES: Tuple[str, ...] = (
    FailureCategory.TIMEOUT.value,
    FailureCategory.OOM.value,
    FailureCategory.TOOL_ERROR.value,
    FailureCategory.INVALID_OUTPUT.value,
    FailureCategory.HALLUCINATION.value,
    FailureCategory.ENERGY_EXCEEDED.value,
)

# Context keys inspected for signal (documented and stable).
_CONTEXT_KEYS: Tuple[str, ...] = (
    "error_type", "exception_type", "status",
    "reason", "message", "detail", "description",
)


@dataclass(frozen=True, slots=True)
class FailureClassifierConfig:
    """Tunable parameters for :class:`FailureClassifier`."""

    default_category: FailureCategory = FailureCategory.UNKNOWN
    max_history: int = 1_000
    allowed_categories: Tuple[str, ...] = _DEFAULT_ALLOWED_CATEGORIES
    keyword_overrides: Optional[Mapping[str, Tuple[str, ...]]] = None

    def __post_init__(self) -> None:
        # --- default_category --------------------------------------- #
        if not isinstance(self.default_category, FailureCategory):
            raise FailureClassifierError(
                "default_category must be a FailureCategory."
            )

        # --- max_history -------------------------------------------- #
        if isinstance(self.max_history, bool) or not isinstance(
            self.max_history, int
        ):
            raise FailureClassifierError("max_history must be an int.")
        if self.max_history <= 0:
            raise FailureClassifierError("max_history must be > 0.")

        # --- allowed_categories ------------------------------------- #
        for c in self.allowed_categories:
            if not isinstance(c, str) or not c:
                raise FailureClassifierError(
                    "allowed_categories entries must be non-empty strings."
                )
            if c not in _VALID_CATEGORY_VALUES:
                raise FailureClassifierError(
                    f"allowed_categories entry {c!r} is not a valid "
                    f"FailureCategory."
                )

        # --- default_category must be UNKNOWN or in allowed --------- #
        if (
            self.default_category is not FailureCategory.UNKNOWN
            and self.default_category.value not in self.allowed_categories
        ):
            raise FailureClassifierError(
                f"default_category={self.default_category.value!r} is not "
                f"in allowed_categories and is not UNKNOWN."
            )

        # --- keyword_overrides -------------------------------------- #
        if self.keyword_overrides is not None:
            if not isinstance(self.keyword_overrides, Mapping):
                raise FailureClassifierError(
                    "keyword_overrides must be a Mapping or None."
                )
            for k, v in self.keyword_overrides.items():
                if not isinstance(k, str) or not k:
                    raise FailureClassifierError(
                        "keyword_overrides keys must be non-empty strings."
                    )
                if k not in _VALID_CATEGORY_VALUES:
                    raise FailureClassifierError(
                        f"keyword_overrides key {k!r} is not a valid "
                        f"FailureCategory."
                    )
                if not isinstance(v, (list, tuple)):
                    raise FailureClassifierError(
                        f"keyword_overrides[{k!r}] must be a list/tuple."
                    )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "default_category": self.default_category.value,
            "max_history": self.max_history,
            "allowed_categories": list(self.allowed_categories),
            "keyword_overrides": (
                None if self.keyword_overrides is None
                else {k: list(v) for k, v in self.keyword_overrides.items()}
            ),
        }

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        strict: bool = True,
    ) -> "FailureClassifierConfig":
        if not isinstance(data, Mapping):
            raise FailureClassifierError(
                "FailureClassifierConfig.from_dict expects a Mapping."
            )
        try:
            if strict:
                valid = set(cls.__dataclass_fields__)
                unknown = [k for k in data if k not in valid]
                if unknown:
                    raise FailureClassifierError(
                        f"unknown FailureClassifierConfig keys: {unknown}"
                    )

            # Preserve "missing" vs "empty" for allowed_categories.
            if "allowed_categories" in data:
                allowed = tuple(data["allowed_categories"] or ())
                if not isinstance(data["allowed_categories"], (list, tuple)):
                    raise FailureClassifierError(
                        "allowed_categories must be a list/tuple."
                    )
            else:
                allowed = _DEFAULT_ALLOWED_CATEGORIES

            dc_raw = data.get("default_category", "unknown")
            default_category = (
                dc_raw if isinstance(dc_raw, FailureCategory)
                else FailureCategory(str(dc_raw))
            )

            return cls(
                default_category=default_category,
                max_history=int(data.get("max_history", 1000)),
                allowed_categories=allowed,
                keyword_overrides=data.get("keyword_overrides"),
            )
        except FailureClassifierError:
            raise
        except (TypeError, ValueError) as exc:
            raise FailureClassifierError(
                f"invalid FailureClassifierConfig payload: {exc}"
            ) from exc

    def __repr__(self) -> str:
        return (
            "FailureClassifierConfig("
            f"default={self.default_category.value}, "
            f"allowed={list(self.allowed_categories)}, "
            f"history={self.max_history})"
        )


# --------------------------------------------------------------------------- #
# Classification record
# --------------------------------------------------------------------------- #
@dataclass(frozen=True, slots=True)
class ClassificationRecord:
    """Immutable record of one classification."""

    error_type: Optional[str]
    category: str
    matched_keyword: Optional[str]
    timestamp: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    def __post_init__(self) -> None:
        if self.error_type is not None and not isinstance(self.error_type, str):
            raise FailureClassifierError("error_type must be str or None.")
        if not isinstance(self.category, str) or self.category not in (
            _VALID_CATEGORY_VALUES
        ):
            raise FailureClassifierError(
                f"category {self.category!r} is not a valid FailureCategory."
            )
        if self.matched_keyword is not None and not isinstance(
            self.matched_keyword, str
        ):
            raise FailureClassifierError(
                "matched_keyword must be str or None."
            )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "error_type": self.error_type,
            "category": self.category,
            "matched_keyword": self.matched_keyword,
            "timestamp": self.timestamp.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ClassificationRecord":
        if not isinstance(data, Mapping):
            raise FailureClassifierError(
                "ClassificationRecord.from_dict expects a Mapping."
            )
        try:
            ts = data.get("timestamp")
            if ts is None:
                timestamp = datetime.now(timezone.utc)
            elif isinstance(ts, str):
                timestamp = datetime.fromisoformat(ts)
                if timestamp.tzinfo is None:
                    timestamp = timestamp.replace(tzinfo=timezone.utc)
            elif isinstance(ts, datetime):
                timestamp = ts
            else:
                raise FailureClassifierError(
                    f"timestamp must be str|datetime|None, "
                    f"got {type(ts).__name__}."
                )
            return cls(
                error_type=data.get("error_type"),
                category=str(data.get("category", "unknown")),
                matched_keyword=data.get("matched_keyword"),
                timestamp=timestamp,
            )
        except FailureClassifierError:
            raise
        except (TypeError, ValueError) as exc:
            raise FailureClassifierError(
                f"invalid ClassificationRecord payload: {exc}"
            ) from exc

    def __repr__(self) -> str:
        return (
            "ClassificationRecord("
            f"error_type={self.error_type!r}, "
            f"category={self.category!r}, "
            f"keyword={self.matched_keyword!r})"
        )


# --------------------------------------------------------------------------- #
# Classifier
# --------------------------------------------------------------------------- #
def _compile_pattern(keyword: str) -> Pattern[str]:
    """
    Compile a keyword into a word-ish boundary pattern.

    ``(?<![A-Za-z0-9])...(?![A-Za-z0-9])`` is preferred over ``\\b`` because
    it treats ``_`` (common in error class names) as a separator, so
    ``"oom"`` matches ``"CUDA_OOM"`` while still *not* matching ``"boom"``.
    The haystack and keywords are pre-lowercased, so no ``IGNORECASE`` flag
    is needed.
    """
    return re.compile(
        rf"(?<![A-Za-z0-9]){re.escape(keyword)}(?![A-Za-z0-9])"
    )


class FailureClassifier:
    """
    Classifies agent failures for better debugging.

    Thread-safe, serializable, and bounded in memory. The original public
    API (``classify(error, context) -> str``) is preserved; new parameters
    are keyword-only and all new features are additive.
    """

    CATEGORIES: Mapping[str, str] = FAILURE_CATEGORIES

    def __init__(
        self,
        *,
        config: Optional[FailureClassifierConfig] = None,
        strict: bool = True,
    ) -> None:
        if config is not None and not isinstance(
            config, FailureClassifierConfig
        ):
            raise FailureClassifierError(
                f"config must be a FailureClassifierConfig, got "
                f"{type(config).__name__}."
            )
        self._config = config or FailureClassifierConfig()
        self._strict = bool(strict)

        self._lock = threading.RLock()
        self._history: Deque[ClassificationRecord] = deque(
            maxlen=self._config.max_history
        )
        # Uptime anchor. When restored from a serialized payload, this is
        # the *historical* creation time — call ``reset()`` to make
        # ``statistics()["uptime_seconds"]`` reflect the current instance.
        self._started_at: float = time.time()

        # Lifecycle counters — cumulative over the object's lifetime.
        self._total_classifications: int = 0
        self._dropped_count: int = 0

        # Compile keyword patterns once, up-front.
        base_keywords: Dict[str, Tuple[str, ...]] = {
            FailureCategory.TIMEOUT.value: (
                "timeout", "timed out", "deadline exceeded",
                "timeoutexpired", "read timeout",
            ),
            FailureCategory.OOM.value: (
                "out of memory", "oom", "memoryerror", "cuda oom",
                "cannot allocate memory",
            ),
            FailureCategory.TOOL_ERROR.value: (
                "tool", "tool error", "toolerror", "tool call failed",
                "function call failed",
            ),
            FailureCategory.INVALID_OUTPUT.value: (
                "invalid output", "format error", "validation error",
                "schema", "malformed", "parse error",
            ),
            FailureCategory.HALLUCINATION.value: (
                "hallucination", "fabricat", "made up", "false information",
            ),
            FailureCategory.ENERGY_EXCEEDED.value: (
                "energy", "budget exceeded", "energy exceeded",
                "watt", "carbon budget",
            ),
        }
        if self._config.keyword_overrides:
            for k, v in self._config.keyword_overrides.items():
                base_keywords[k] = tuple(str(x).lower() for x in v if x)
        self._keywords: Dict[str, Tuple[str, ...]] = {
            k: tuple(str(x).lower() for x in v if x)
            for k, v in base_keywords.items()
        }
        self._patterns: Dict[str, Tuple[Pattern[str], ...]] = {
            cat: tuple(_compile_pattern(kw) for kw in kws)
            for cat, kws in self._keywords.items()
        }

        logger.debug(
            "FailureClassifier initialized "
            "(default=%s, history=%d, strict=%s)",
            self._config.default_category.value,
            self._config.max_history,
            self._strict,
        )

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> FailureClassifierConfig:
        return self._config

    @property
    def strict(self) -> bool:
        return self._strict

    @property
    def history(self) -> List[ClassificationRecord]:
        with self._lock:
            return list(self._history)

    # ---------------------------------------------------------- public API
    def classify(
        self,
        error: Any = None,
        context: Optional[Mapping[str, Any]] = None,
    ) -> str:
        """
        Classify a failure from an exception and/or a context dict.

        Classification pipeline (documented order):

        1. Strict mode requires ``error`` to be a ``BaseException`` instance.
           Non-strict coerces arbitrary objects via ``str()``.
        2. Signals are collected from the error's class name, the error's
           message, and the following context fields (any that are present):
           ``error_type``, ``exception_type``, ``status``, ``reason``,
           ``message``, ``detail``, ``description``.
        3. Categories are tested in insertion order of the internal
           keyword map (``_keywords``) — this is the documented keyword
           priority. Within each category, keywords are tested in their
           declaration order. First match wins.
        4. Categories not in ``config.allowed_categories`` are skipped.
        5. If nothing matches, ``config.default_category`` is returned.

        Returns
        -------
        str
            The category value (e.g. ``"timeout"``).
        """
        category, _kw, _field, _etype = self._classify_core(error, context)
        with self._lock:
            self._record_locked(category, _kw, _etype)
        return category

    def classify_detailed(
        self,
        error: Any = None,
        context: Optional[Mapping[str, Any]] = None,
    ) -> ClassifyResult:
        """
        Like :meth:`classify`, but returns the full classification detail
        (which field matched, which keyword fired, whether the default was
        used).
        """
        category, keyword, field_name, error_type = self._classify_core(
            error, context
        )
        with self._lock:
            self._record_locked(category, keyword, error_type)
        return {
            "category": category,
            "matched_keyword": keyword,
            "matched_in": field_name,
            "error_type": error_type,
            "used_default": keyword is None,
        }

    def classify_batch(
        self,
        errors: Sequence[Any],
        contexts: Optional[Sequence[Optional[Mapping[str, Any]]]] = None,
    ) -> List[str]:
        """
        Classify multiple errors with one lock acquisition.

        Validation and matching happen outside the lock (so a validation
        failure leaves the runtime untouched); history appends happen
        under one acquisition for efficiency.
        """
        if contexts is None:
            contexts = [None] * len(errors)
        if len(errors) != len(contexts):
            raise FailureClassifierError(
                f"errors and contexts must have the same length "
                f"({len(errors)} vs {len(contexts)})."
            )
        results: List[Tuple[str, Optional[str], Optional[str], Optional[str]]] = []
        for error, ctx in zip(errors, contexts):
            results.append(self._classify_core(error, ctx))
        with self._lock:
            for category, keyword, _field, error_type in results:
                self._record_locked(category, keyword, error_type)
        return [r[0] for r in results]

    # ---------------------------------------------------------- internals
    def _classify_core(
        self,
        error: Any,
        context: Optional[Mapping[str, Any]],
    ) -> Tuple[str, Optional[str], Optional[str], Optional[str]]:
        """
        Return ``(category, matched_keyword, matched_field, error_type)``.

        Does **not** touch history. May raise ``FailureClassifierError``
        in strict mode for bad input.
        """
        error_type: Optional[str] = None
        error_message = ""

        if error is not None:
            if not isinstance(error, BaseException):
                msg = (
                    f"error must be an exception instance, "
                    f"got {type(error).__name__}."
                )
                if self._strict:
                    raise FailureClassifierError(msg)
                logger.warning("%s Coercing via str().", msg)
            error_type = type(error).__name__
            try:
                error_message = str(error)
            except Exception:  # pragma: no cover — defensive
                error_message = ""

        if context is not None and not isinstance(context, Mapping):
            msg = (
                f"context must be a Mapping or None, "
                f"got {type(context).__name__}."
            )
            if self._strict:
                raise FailureClassifierError(msg)
            logger.warning("%s Ignoring context.", msg)
            context = None

        # Signals, each tagged with the source field, for match reporting.
        signals: List[Tuple[str, str]] = []
        if error_type is not None:
            signals.append(("error_type", error_type.lower()))
        if error_message:
            signals.append(("message", error_message.lower()))
        if context is not None:
            for key in _CONTEXT_KEYS:
                value = context.get(key)
                if value is not None:
                    signals.append((f"context.{key}", str(value).lower()))

        # Match — category insertion order, then field order, then keyword order.
        allowed = set(self._config.allowed_categories)
        matched: Optional[Tuple[str, str, str]] = None
        for category_value, patterns in self._patterns.items():
            if category_value not in allowed:
                continue
            for field_name, text in signals:
                for pat in patterns:
                    m = pat.search(text)
                    if m:
                        matched = (
                            category_value,
                            m.group(0).lower(),
                            field_name,
                        )
                        break
                if matched:
                    break
            if matched:
                break

        if matched is None:
            return (
                self._config.default_category.value,
                None,
                None,
                error_type,
            )
        category, keyword, field_name = matched
        return category, keyword, field_name, error_type

    def _record_locked(
        self,
        category: str,
        matched_keyword: Optional[str],
        error_type: Optional[str],
    ) -> None:
        """Append a record + bump counters. Caller must hold ``_lock``."""
        record = ClassificationRecord(
            error_type=error_type,
            category=category,
            matched_keyword=matched_keyword,
        )
        self._total_classifications += 1
        if len(self._history) == self._config.max_history:
            self._dropped_count += 1
        self._history.append(record)

    # ---------------------------------------------------------- statistics
    def statistics(self) -> ClassifierStatistics:
        """
        Return a JSON-safe, **atomic** snapshot of the classifier state.
        """
        with self._lock:
            by_category: Dict[str, int] = {}
            for r in self._history:
                by_category[r.category] = by_category.get(r.category, 0) + 1
            return {
                "total_classifications": self._total_classifications,
                "dropped_classifications": self._dropped_count,
                "buffered_classifications": len(self._history),
                "by_category": by_category,
                "default_category": self._config.default_category.value,
                "strict": self._strict,
                "categories": dict(self.CATEGORIES),
                "uptime_seconds": time.time() - self._started_at,
            }

    def reset(self, *, clear_history: bool = True) -> int:
        """
        Reset the classifier history and uptime anchor.

        Returns the number of *buffered* history entries removed.
        Lifetime counters (``total_classifications``, ``dropped_count``)
        are **not** reset.
        """
        with self._lock:
            removed = len(self._history)
            if clear_history:
                self._history.clear()
            self._started_at = time.time()
        logger.debug("FailureClassifier reset (removed %d).", removed)
        return removed

    # ---------------------------------------------------------- serialization
    def to_dict(self, *, include_history: bool = False) -> Dict[str, Any]:
        with self._lock:
            payload: Dict[str, Any] = {
                "config": self._config.to_dict(),
                "strict": self._strict,
                "started_at": self._started_at,
                "total_classifications": self._total_classifications,
                "dropped_classifications": self._dropped_count,
                "categories": dict(self.CATEGORIES),
            }
            if include_history:
                payload["history"] = [r.to_dict() for r in self._history]
        return payload

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "FailureClassifier":
        if not isinstance(data, Mapping):
            raise FailureClassifierError(
                f"from_dict expects a Mapping, got {type(data).__name__}."
            )
        try:
            cfg = FailureClassifierConfig.from_dict(
                dict(data.get("config") or {}), strict=True
            )
            clf = cls(config=cfg, strict=bool(data.get("strict", True)))
            with clf._lock:
                if data.get("started_at") is not None:
                    clf._started_at = float(data["started_at"])
                for entry in data.get("history", []) or []:
                    clf._history.append(
                        ClassificationRecord.from_dict(entry)
                    )
                if "total_classifications" in data:
                    clf._total_classifications = int(
                        data["total_classifications"]
                    )
                elif clf._history:
                    clf._total_classifications = len(clf._history)
                if "dropped_classifications" in data:
                    clf._dropped_count = int(data["dropped_classifications"])
            return clf
        except FailureClassifierError:
            raise
        except (TypeError, ValueError) as exc:
            raise FailureClassifierError(
                f"invalid FailureClassifier payload: {exc}"
            ) from exc

    def to_json(
        self, *, include_history: bool = False, **kwargs: Any
    ) -> str:
        # Payload is primitive-only; no default= needed.
        return json.dumps(
            self.to_dict(include_history=include_history), **kwargs
        )

    @classmethod
    def from_json(cls, payload: str) -> "FailureClassifier":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise FailureClassifierError(f"Invalid JSON: {exc}") from exc
        return cls.from_dict(data)

    # ----------------------------------------------------------------- dunder
    def __repr__(self) -> str:
        with self._lock:
            return (
                "FailureClassifier("
                f"categories={len(self.CATEGORIES)}, "
                f"classifications={self._total_classifications}, "
                f"buffered={len(self._history)}, "
                f"default={self._config.default_category.value}, "
                f"strict={self._strict})"
            )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, FailureClassifier):
            return NotImplemented
        return self._config == other._config and self._strict == other._strict

    def __hash__(self) -> int:
        return hash((self._config, self._strict))


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "ClassificationRecord",
    "ClassifyResult",
    "ClassifierStatistics",
    "FAILURE_CATEGORIES",
    "FailureCategory",
    "FailureClassifier",
    "FailureClassifierConfig",
    "FailureClassifierError",
]


# --------------------------------------------------------------------------- #
# Smoke test: python -m scoring.failure_classifier
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )

    clf = FailureClassifier()
    print("repr       :", clf)

    # ---- Happy path -------------------------------------------------- #
    cases = [
        (TimeoutError("Request timed out after 30s"), {}),
        (MemoryError("CUDA OOM"), {}),
        (RuntimeError("tool call failed"), {}),
        (ValueError("invalid output format"), {}),
        (RuntimeError("model generated false information"), {}),
        (RuntimeError("energy budget exceeded"), {}),
        (RuntimeError("totally unknown error"), {}),
    ]
    for exc, ctx in cases:
        category = clf.classify(exc, ctx)
        print(f"{type(exc).__name__:<16} -> {category}")

    # ---- Word-boundary matching (no false positives) ----------------- #
    assert clf.classify(RuntimeError("the room was empty")) == "unknown"
    assert clf.classify(RuntimeError("boom!")) == "unknown"
    assert clf.classify(RuntimeError("CUDA_OOM occurred")) == "oom"
    print("boundaries : OK")

    # ---- Context-driven classification ------------------------------- #
    assert clf.classify(None, {"status": "timeout"}) == "timeout"
    assert clf.classify(None, {"error_type": "MemoryError"}) == "oom"
    print("context     : OK")

    # ---- classify_detailed ------------------------------------------- #
    detail = clf.classify_detailed(
        RuntimeError("deadline exceeded while running tool")
    )
    print("detailed   :", detail)
    assert detail["category"] == "timeout"
    assert detail["matched_keyword"] == "deadline exceeded"
    assert detail["matched_in"] == "message"
    assert detail["used_default"] is False

    # ---- classify_batch ---------------------------------------------- #
    batch = clf.classify_batch(
        [TimeoutError("x"), MemoryError("oom"), RuntimeError("junk")],
    )
    print("batch      :", batch)
    assert batch == ["timeout", "oom", "unknown"]

    # ---- Statistics (real counters, not capped by max_history) ------ #
    stats = clf.statistics()
    print("stats      :", {k: v for k, v in stats.items()
                          if k != "uptime_seconds"})
    assert stats["total_classifications"] == stats["buffered_classifications"] \
        + stats["dropped_classifications"]

    # ---- Serialization round-trip (with history) -------------------- #
    payload = clf.to_json(include_history=True)
    restored = FailureClassifier.from_json(payload)
    assert restored.to_dict() == clf.to_dict()
    assert restored == clf
    assert len(restored.history) == len(clf.history)
    assert restored.statistics()["total_classifications"] \
        == clf.statistics()["total_classifications"]
    print("round-trip : OK")

    # ---- Empty allowed_categories survives round-trip ---------------- #
    cfg_empty = FailureClassifierConfig(
        allowed_categories=(),  # explicitly empty
        default_category=FailureCategory.UNKNOWN,
    )
    rt_cfg = FailureClassifierConfig.from_dict(cfg_empty.to_dict())
    assert rt_cfg.allowed_categories == ()
    print("empty allow: OK")

    # ---- Validation failures ----------------------------------------- #
    for bad_cfg in (
        dict(default_category="not-a-category"),
        dict(default_category=FailureCategory.OOM,
             allowed_categories=("timeout",)),                 # not allowed
        dict(allowed_categories=("not_a_real_category",)),
        dict(keyword_overrides={"not_a_real_category": ("x",)}),
        dict(max_history=0),
        dict(max_history="100"),
    ):
        try:
            FailureClassifierConfig(**bad_cfg)  # type: ignore[arg-type]
        except FailureClassifierError as exc:
            print("Rejected cfg:", exc)

    # ---- Strict input validation ------------------------------------- #
    strict = FailureClassifier(strict=True)
    try:
        strict.classify(TimeoutError("x"), "not-a-mapping")  # type: ignore[arg-type]
    except FailureClassifierError as exc:
        print("Rejected   :", exc)

    try:
        strict.classify("plain string")  # not an exception
    except FailureClassifierError as exc:
        print("Rejected   :", exc)

    # ---- Non-strict coerces ------------------------------------------ #
    lenient = FailureClassifier(strict=False)
    print("lenient    :", lenient.classify(None, "not-a-mapping"))  # type: ignore[arg-type]
    print("lenient    :", lenient.classify("timeout string"))       # type: ignore[arg-type]

    # ---- JSON enum safety -------------------------------------------- #
    payload = json.dumps({"cat": FailureCategory.TIMEOUT})
    assert payload == '{"cat": "timeout"}', f"unexpected: {payload}"
    print("enum json  : OK")

    # ---- from_dict rejects unknown config keys ----------------------- #
    try:
        FailureClassifier.from_dict(
            {"config": {"typo_field": 1}}
        )
    except FailureClassifierError as exc:
        print("Rejected   :", exc)

    # ---- from_dict wraps parse errors ------------------------------- #
    try:
        FailureClassifier.from_dict({"started_at": "not-a-number"})
    except FailureClassifierError as exc:
        print("Rejected   :", exc)

    try:
        FailureClassifier.from_dict(
            {"history": [{"category": "not_a_category"}]}
        )
    except FailureClassifierError as exc:
        print("Rejected   :", exc)

    print("\nSmoke test passed.")
