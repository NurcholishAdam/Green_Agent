# src/scoring/failure_classifier.py

"""
Failure Classifier
==================

Classifies agent failures for better debugging.

Categories
----------
- ``timeout``           — Agent exceeded time limit.
- ``oom``               — Out of memory during execution.
- ``tool_error``        — Failed to use required tools.
- ``invalid_output``    — Output format incorrect.
- ``hallucination``     — Generated false information.
- ``energy_exceeded``   — Exceeded energy budget.

Enhancements
------------
- ``FailureCategory`` — StrEnum so categories are type-safe and JSON-friendly.
- ``FailureClassifierConfig`` — frozen, validated: category allowlist, custom
  keyword maps, strict mode.
- **Implemented ``classify()``** — the original was a stub returning ``None``.
- **Full validation** of every argument; strict / non-strict modes.
- **Thread safety** — ``RLock`` guards internal counters and history.
- **Bounded classification history** — ``deque(maxlen=config.max_history)``.
- Serialization: ``to_dict`` / ``from_dict`` / ``to_json`` on the classifier.
- ``statistics()``, ``__repr__``, custom ``FailureClassifierError``, lazy
  ``%s`` logging, and a ``__main__`` smoke test.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Deque, Dict, List, Mapping, Optional

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class FailureClassifierError(ValueError):
    """Raised for invalid classifier inputs or configuration."""


# --------------------------------------------------------------------------- #
# Category enum
# --------------------------------------------------------------------------- #
class FailureCategory(str, Enum):
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
        return {
            FailureCategory.TIMEOUT: "Agent exceeded time limit",
            FailureCategory.OOM: "Out of memory during execution",
            FailureCategory.TOOL_ERROR: "Failed to use required tools",
            FailureCategory.INVALID_OUTPUT: "Output format incorrect",
            FailureCategory.HALLUCINATION: "Generated false information",
            FailureCategory.ENERGY_EXCEEDED: "Exceeded energy budget",
            FailureCategory.UNKNOWN: "Unclassified failure",
        }[self]


# Backward-compatible: original code used a plain dict.
FAILURE_CATEGORIES: Dict[str, str] = {
    cat.value: cat.description for cat in FailureCategory
}


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class FailureClassifierConfig:
    """Tunable parameters for :class:`FailureClassifier`."""

    # Fallback category when nothing else matches.
    default_category: FailureCategory = FailureCategory.UNKNOWN

    # Bounded classification history.
    max_history: int = 1_000

    # Allowlist of categories the classifier may return (excluding UNKNOWN
    # which is always permitted as a fallback).
    allowed_categories: tuple = (
        FailureCategory.TIMEOUT.value,
        FailureCategory.OOM.value,
        FailureCategory.TOOL_ERROR.value,
        FailureCategory.INVALID_OUTPUT.value,
        FailureCategory.HALLUCINATION.value,
        FailureCategory.ENERGY_EXCEEDED.value,
    )

    # Custom keyword maps: category value -> tuple of lowercase substrings.
    keyword_overrides: Optional[Mapping[str, tuple]] = None

    def __post_init__(self) -> None:
        if not isinstance(self.default_category, FailureCategory):
            raise FailureClassifierError(
                "default_category must be a FailureCategory."
            )
        if self.max_history <= 0:
            raise FailureClassifierError("max_history must be > 0.")
        for c in self.allowed_categories:
            if not isinstance(c, str) or not c:
                raise FailureClassifierError(
                    "allowed_categories entries must be non-empty strings."
                )
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
                {k: list(v) for k, v in self.keyword_overrides.items()}
                if self.keyword_overrides else None
            ),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "FailureClassifierConfig":
        if not isinstance(data, Mapping):
            raise FailureClassifierError(
                "FailureClassifierConfig.from_dict expects a Mapping."
            )
        return cls(
            default_category=FailureCategory(
                str(data.get("default_category", "unknown"))
            ),
            max_history=int(data.get("max_history", 1000)),
            allowed_categories=tuple(
                data.get("allowed_categories", ())
            ) or cls.allowed_categories,
            keyword_overrides=data.get("keyword_overrides"),
        )


# --------------------------------------------------------------------------- #
# Classification record
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class ClassificationRecord:
    """Immutable record of one classification."""

    error_type: str
    category: str
    matched_keyword: Optional[str]
    timestamp: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "error_type": self.error_type,
            "category": self.category,
            "matched_keyword": self.matched_keyword,
            "timestamp": self.timestamp.isoformat(),
        }


# --------------------------------------------------------------------------- #
# Classifier
# --------------------------------------------------------------------------- #
class FailureClassifier:
    """
    Classifies agent failures for better debugging.

    Thread-safe, serializable, and bounded in memory. The original public API
    (``classify(error, context) -> str``) is preserved; new parameters are
    keyword-only.
    """

    CATEGORIES: Dict[str, str] = dict(FAILURE_CATEGORIES)

    def __init__(
        self,
        *,
        config: Optional[FailureClassifierConfig] = None,
        strict: bool = True,
    ) -> None:
        self._config = config or FailureClassifierConfig()
        self._strict = bool(strict)

        self._lock = threading.RLock()
        self._history: Deque[ClassificationRecord] = deque(
            maxlen=self._config.max_history
        )
        self._started_at: float = time.time()

        # Compile the keyword maps. The default maps recognize common error
        # types by inspecting the exception class name and its message.
        base_keywords: Dict[str, tuple] = {
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
                base_keywords[k] = tuple(str(x).lower() for x in v)
        self._keywords: Dict[str, tuple] = {
            k: tuple(str(x).lower() for x in v)
            for k, v in base_keywords.items()
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

        The classification pipeline is:

        1. Inspect ``error`` (if any) for its class name and message.
        2. Inspect ``context`` for an explicit ``"error_type"``,
           ``"exception_type"``, or ``"status"`` field.
        3. Match against the compiled keyword maps in ``self._keywords``.
        4. Fall back to ``config.default_category``.

        Returns
        -------
        str
            The category value (e.g. ``"timeout"``).
        """
        # ---- Normalize inputs ---------------------------------------
        error_type = "unknown"
        error_message = ""
        matched_keyword: Optional[str] = None

        if error is not None:
            error_type = type(error).__name__
            try:
                error_message = str(error)
            except Exception:  # pragma: no cover — defensive
                error_message = ""

        if context is not None and not isinstance(context, Mapping):
            msg = f"context must be a Mapping or None, got {type(context).__name__}."
            if self._strict:
                raise FailureClassifierError(msg)
            logger.warning("%s Ignoring context.", msg)
            context = None

        # Merge all signal strings into one haystack for matching.
        haystack_parts: List[str] = [error_type.lower(), error_message.lower()]
        if context is not None:
            for key in ("error_type", "exception_type", "status", "reason",
                        "message", "detail", "description"):
                value = context.get(key)
                if value is not None:
                    haystack_parts.append(str(value).lower())
        haystack = " | ".join(haystack_parts)

        # ---- Keyword matching ---------------------------------------
        for category_value, keywords in self._keywords.items():
            if category_value not in self._config.allowed_categories:
                continue
            for kw in keywords:
                if kw and kw in haystack:
                    matched_keyword = kw
                    category = category_value
                    break
            if matched_keyword is not None:
                break
        else:
            category = self._config.default_category.value

        record = ClassificationRecord(
            error_type=error_type,
            category=category,
            matched_keyword=matched_keyword,
        )
        with self._lock:
            self._history.append(record)

        logger.debug(
            "Classified failure: error_type=%s category=%s keyword=%s",
            error_type, category, matched_keyword,
        )
        return category

    # ---------------------------------------------------------- statistics
    def statistics(self) -> Dict[str, Any]:
        """Return a JSON-safe snapshot of the classifier state."""
        with self._lock:
            history = list(self._history)
        by_category: Dict[str, int] = {}
        for r in history:
            by_category[r.category] = by_category.get(r.category, 0) + 1
        return {
            "total_classifications": len(history),
            "by_category": by_category,
            "default_category": self._config.default_category.value,
            "strict": self._strict,
            "categories": dict(self.CATEGORIES),
            "uptime_seconds": time.time() - self._started_at,
        }

    def reset(self, *, clear_history: bool = True) -> int:
        """Reset the classifier history. Returns entries removed."""
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
        cfg = FailureClassifierConfig.from_dict(
            dict(data.get("config", {}) or {})
        )
        clf = cls(config=cfg, strict=bool(data.get("strict", True)))
        clf._started_at = float(data.get("started_at", time.time()))
        return clf

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    @classmethod
    def from_json(cls, payload: str) -> "FailureClassifier":
        try:
            return cls.from_dict(json.loads(payload))
        except json.JSONDecodeError as exc:
            raise FailureClassifierError(f"Invalid JSON: {exc}") from exc

    # ----------------------------------------------------------------- dunder
    def __repr__(self) -> str:
        with self._lock:
            return (
                "FailureClassifier("
                f"categories={len(self.CATEGORIES)}, "
                f"classifications={len(self._history)}, "
                f"strict={self._strict})"
            )


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "ClassificationRecord",
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

    # ---- Happy path ------------------------------------------------ #
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

    # ---- Context-driven classification ---------------------------- #
    category = clf.classify(None, {"status": "timeout"})
    print("context     :", category)
    assert category == "timeout"

    # ---- Bug fix: previously returned None ------------------------ #
    assert clf.classify(TimeoutError("x")) == "timeout"
    print("no-none    : OK")

    # ---- Statistics ----------------------------------------------- #
    print("statistics :", {
        k: v for k, v in clf.statistics().items()
        if k != "uptime_seconds"
    })

    # ---- Serialization -------------------------------------------- #
    payload = clf.to_json()
    restored = FailureClassifier.from_json(payload)
    assert restored.to_dict() == clf.to_dict()
    print("Round-trip OK.")

    # ---- Validation failures -------------------------------------- #
    for bad_cfg in (
        dict(default_category="not-a-category"),
        dict(max_history=0),
    ):
        try:
            FailureClassifierConfig(**bad_cfg)  # type: ignore[arg-type]
        except FailureClassifierError as exc:
            print("Rejected cfg:", exc)

    strict = FailureClassifier(strict=True)
    try:
        strict.classify(TimeoutError("x"), "not-a-mapping")  # type: ignore[arg-type]
    except FailureClassifierError as exc:
        print("Rejected   :", exc)

    # ---- Non-strict coerces --------------------------------------- #
    lenient = FailureClassifier(strict=False)
    print("lenient    :", lenient.classify(None, "not-a-mapping"))  # type: ignore[arg-type]

    print("\nSmoke test passed.")
