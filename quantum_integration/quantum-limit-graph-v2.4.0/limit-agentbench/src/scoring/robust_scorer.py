# src/scoring/robust_scorer.py

"""
Robust Sustainability Scorer
============================

Handles all failure modes gracefully when scoring agent results.

Given a result dict and a ground-truth dict, produces a consistent scoring
dictionary regardless of whether the agent succeeded, timed out, ran out of
memory, errored, or reported an unknown status.

Enhancements
------------
- ``RobustScorerConfig`` — frozen, ``slots``, ``__repr__``; every numeric
  field bounded (``*_max`` fields must be in ``[0, 1]``; penalties must be
  finite and non-negative); new fields ``unknown_penalty``,
  ``case_sensitive``, ``max_similarity_length``, ``max_energy_ratio``.
  ``from_dict(..., strict=True)`` rejects unknown keys.
- ``ScoringResult`` — frozen, ``slots``, ``__repr__``; ``failure_category``
  validated against the known vocabulary; ``accuracy`` /
  ``partial_credit`` / ``sustainability_index`` bounded to ``[0, 1]``.
- **Round-trip with history** — ``to_json(include_history=True)`` no
  longer raises ``TypeError``; ``from_dict`` restores the deque.
- **``strict=False`` is actually lenient** — non-Mapping ``result`` /
  ``ground_truth`` are coerced instead of raising.
- **Unknown status handling** — statuses outside
  ``{success, timeout, error, oom}`` either raise (strict) or fall through
  to a dedicated ``"unknown"`` category with a warning (non-strict),
  instead of silently granting full credit.
- **Bounded rewards** — ``*_max`` fields bounded to ``[0, 1]``;
  ``max_energy_ratio`` optionally bounds ``energy_efficiency``; penalties
  must be finite.
- **Lifecycle counters** — ``total_scored`` and ``dropped_count`` are
  decoupled from ``len(history)``.
- **Atomic ``statistics()``** — every field read under the lock.
- **``score_detailed() -> ScoringResult``** and **``score_batch()``** —
  typed record return and one-lock batch scoring.
- **Similarity hardening** — ``max_similarity_length`` truncation and
  configurable ``case_sensitive`` (default ``False``); both metrics
  normalise consistently.
- ``from_dict`` wraps all parse errors as ``RobustScorerError``.
- ``to_json`` no longer uses ``default=str``.
- ``__eq__`` / ``__hash__`` / ``__dir__`` added.
- ``_started_at`` uptime semantics documented for restored instances.
- ``energy_efficiency`` documented as a ratio (lower-is-better); the
  misleading name is retained for backward compatibility, with the
  clamped form available via ``max_energy_ratio``.

Compatibility
-------------
- ``score(result, ground_truth) -> dict`` unchanged; keys preserved.
- ``RobustSustainabilityScorer(strict=True)`` still raises on
  non-Mapping inputs, but ``strict=False`` now actually coerces them.
- ``ScoringResult.failure_category`` now accepts only
  ``{None, "timeout", "error", "oom", "unknown"}``. Payloads with any
  other value raise ``RobustScorerError``.
- ``energy_efficiency`` is unchanged by default. Set
  ``config.max_energy_ratio`` to bound it for RL training.
- ``to_json`` signature is now ``to_json(*, include_history=False,
  **kwargs)`` — the ``include_history`` kwarg is routed correctly to
  ``to_dict`` (previously it crashed by reaching ``json.dumps``).

Requires Python 3.8+. ``slots=True`` on dataclasses requires 3.10+;
drop it for older versions.
"""

from __future__ import annotations

import json
import logging
import math
import threading
import time
from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from difflib import SequenceMatcher
from types import MappingProxyType
from typing import (
    Any, Deque, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple,
    TypedDict,
)

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class RobustScorerError(ValueError):
    """Raised for invalid robust-scorer inputs or configuration."""


# --------------------------------------------------------------------------- #
# Typed return shapes
# --------------------------------------------------------------------------- #
class ScoreMetrics(TypedDict):
    """Legacy dict shape returned by :meth:`score` / :meth:`score_batch`."""

    accuracy: float
    partial_credit: float
    energy_efficiency: float
    sustainability_index: float
    failure_category: Optional[str]
    penalty: float


class ScorerStatistics(TypedDict):
    """Return shape of :meth:`RobustSustainabilityScorer.statistics`."""

    total_scored: int
    dropped_scored: int
    buffered_scored: int
    by_status: Dict[str, int]
    mean_accuracy: float
    mean_sustainability: float
    config: Dict[str, Any]
    strict: bool
    uptime_seconds: float


# --------------------------------------------------------------------------- #
# Known vocabularies
# --------------------------------------------------------------------------- #
_KNOWN_STATUSES = frozenset({"success", "timeout", "error", "oom"})
_UNKNOWN_STATUS = "unknown"

# Categories that may appear in ``ScoringResult.failure_category``.
# ``"error"`` is not a FailureCategory in :mod:`failure_classifier` (the
# closest is ``"tool_error"``); the mapping is documented for cross-module
# consumers.
_KNOWN_FAILURE_CATEGORIES: frozenset = frozenset({
    "timeout", "error", "oom", "unknown",
})

# Read-only public vocabulary for external consumers.
KNOWN_FAILURE_CATEGORIES: Mapping[str, str] = MappingProxyType({
    "timeout": "Agent exceeded time limit",
    "error":   "Agent raised an error",
    "oom":     "Out of memory during execution",
    "unknown": "Unrecognised status; treated conservatively",
})


# --------------------------------------------------------------------------- #
# Config helpers
# --------------------------------------------------------------------------- #
def _require_number(name: str, value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise RobustScorerError(f"{name} must be numeric.")
    fv = float(value)
    if not math.isfinite(fv):
        raise RobustScorerError(f"{name} must be finite, got {value!r}.")
    return fv


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True, slots=True)
class RobustScorerConfig:
    """Tunable parameters for :class:`RobustSustainabilityScorer`."""

    # Partial-credit ceilings per failure mode. Bounded to [0, 1].
    timeout_partial_credit_max: float = 0.5
    error_partial_credit_max: float = 0.2
    oom_partial_credit_max: float = 0.0

    # Sustainability-index ceilings per failure mode. Bounded to [0, 1].
    timeout_sustainability_max: float = 0.25
    error_sustainability_max: float = 0.10
    oom_sustainability_max: float = 0.0

    # Penalties subtracted from accuracy per failure mode. Must be finite
    # and >= 0. A penalty of 0 disables the offset for that mode.
    timeout_penalty: float = 0.5
    error_penalty: float = 0.7
    oom_penalty: float = 1.0
    unknown_penalty: float = 0.7

    # Similarity function.
    similarity_metric: str = "sequence_matcher"  # "sequence_matcher" | "exact"
    case_sensitive: bool = False
    max_similarity_length: int = 10_000  # 0 disables truncation

    # Optional clamp for ``energy_used / time_limit``. When set, the
    # returned ``energy_efficiency`` is ``min(ratio, max_energy_ratio)``.
    # ``None`` preserves legacy behaviour (unbounded).
    max_energy_ratio: Optional[float] = None

    # Default time limit and energy used when the result omits them.
    default_time_limit: float = 30.0
    default_energy_used: float = 0.0

    # Bounded scoring history.
    max_history: int = 1_000

    def __post_init__(self) -> None:
        # --- [0, 1] fields ------------------------------------------ #
        for name in (
            "timeout_partial_credit_max", "error_partial_credit_max",
            "oom_partial_credit_max", "timeout_sustainability_max",
            "error_sustainability_max", "oom_sustainability_max",
        ):
            fv = _require_number(name, getattr(self, name))
            if fv < 0.0 or fv > 1.0:
                raise RobustScorerError(
                    f"{name} must be in [0, 1], got {fv}."
                )

        # --- non-negative fields ------------------------------------ #
        for name in (
            "timeout_penalty", "error_penalty", "oom_penalty",
            "unknown_penalty",
        ):
            fv = _require_number(name, getattr(self, name))
            if fv < 0.0:
                raise RobustScorerError(f"{name} must be >= 0, got {fv}.")

        if self.similarity_metric not in ("sequence_matcher", "exact"):
            raise RobustScorerError(
                "similarity_metric must be 'sequence_matcher' or 'exact'."
            )
        if not isinstance(self.case_sensitive, bool):
            raise RobustScorerError("case_sensitive must be a bool.")
        if isinstance(self.max_similarity_length, bool) or not isinstance(
            self.max_similarity_length, int
        ):
            raise RobustScorerError("max_similarity_length must be an int.")
        if self.max_similarity_length < 0:
            raise RobustScorerError(
                "max_similarity_length must be >= 0."
            )

        if self.max_energy_ratio is not None:
            fv = _require_number("max_energy_ratio", self.max_energy_ratio)
            if fv <= 0:
                raise RobustScorerError(
                    "max_energy_ratio must be > 0 or None."
                )

        fv = _require_number("default_time_limit", self.default_time_limit)
        if fv <= 0:
            raise RobustScorerError("default_time_limit must be > 0.")
        fv = _require_number("default_energy_used", self.default_energy_used)
        if fv < 0:
            raise RobustScorerError("default_energy_used must be >= 0.")

        if isinstance(self.max_history, bool) or not isinstance(
            self.max_history, int
        ):
            raise RobustScorerError("max_history must be an int.")
        if self.max_history <= 0:
            raise RobustScorerError("max_history must be > 0.")

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        strict: bool = True,
    ) -> "RobustScorerConfig":
        if not isinstance(data, Mapping):
            raise RobustScorerError(
                "RobustScorerConfig.from_dict expects a Mapping."
            )
        try:
            if strict:
                valid = set(cls.__dataclass_fields__)
                unknown = [k for k in data if k not in valid]
                if unknown:
                    raise RobustScorerError(
                        f"unknown RobustScorerConfig keys: {unknown}"
                    )
            valid = set(cls.__dataclass_fields__)
            kwargs = {k: v for k, v in data.items() if k in valid}
            return cls(**kwargs)
        except RobustScorerError:
            raise
        except (TypeError, ValueError) as exc:
            raise RobustScorerError(
                f"invalid RobustScorerConfig payload: {exc}"
            ) from exc

    def __repr__(self) -> str:
        return (
            "RobustScorerConfig("
            f"metric={self.similarity_metric!r}, "
            f"case_sensitive={self.case_sensitive}, "
            f"timeout_penalty={self.timeout_penalty:.3g}, "
            f"error_penalty={self.error_penalty:.3g}, "
            f"oom_penalty={self.oom_penalty:.3g}, "
            f"history={self.max_history})"
        )


# --------------------------------------------------------------------------- #
# Scoring result
# --------------------------------------------------------------------------- #
@dataclass(frozen=True, slots=True)
class ScoringResult:
    """Immutable scoring result returned by :meth:`score_detailed`."""

    accuracy: float
    partial_credit: float
    energy_efficiency: float
    sustainability_index: float
    failure_category: Optional[str]
    penalty: float
    status: str
    timestamp: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    def __post_init__(self) -> None:
        # --- [0, 1] fields ------------------------------------------ #
        for name in ("accuracy", "partial_credit", "sustainability_index"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(
                value, (int, float)
            ):
                raise RobustScorerError(f"{name} must be numeric.")
            fv = float(value)
            if not math.isfinite(fv) or fv < 0.0 or fv > 1.0:
                raise RobustScorerError(
                    f"{name} must be finite and in [0, 1], got {value!r}."
                )
        # --- non-negative fields ------------------------------------ #
        for name in ("energy_efficiency", "penalty"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(
                value, (int, float)
            ):
                raise RobustScorerError(f"{name} must be numeric.")
            fv = float(value)
            if not math.isfinite(fv) or fv < 0.0:
                raise RobustScorerError(
                    f"{name} must be finite and >= 0, got {value!r}."
                )
        # --- failure_category --------------------------------------- #
        if self.failure_category is not None:
            if not isinstance(self.failure_category, str) or (
                not self.failure_category
            ):
                raise RobustScorerError(
                    "failure_category must be a non-empty string or None."
                )
            if self.failure_category not in _KNOWN_FAILURE_CATEGORIES:
                raise RobustScorerError(
                    f"failure_category {self.failure_category!r} is not in "
                    f"{sorted(_KNOWN_FAILURE_CATEGORIES)}."
                )
        # --- status ------------------------------------------------- #
        if not isinstance(self.status, str) or not self.status:
            raise RobustScorerError("status must be a non-empty string.")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "accuracy": float(self.accuracy),
            "partial_credit": float(self.partial_credit),
            "energy_efficiency": float(self.energy_efficiency),
            "sustainability_index": float(self.sustainability_index),
            "failure_category": self.failure_category,
            "penalty": float(self.penalty),
            "status": self.status,
            "timestamp": self.timestamp.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ScoringResult":
        if not isinstance(data, Mapping):
            raise RobustScorerError(
                "ScoringResult.from_dict expects a Mapping."
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
                raise RobustScorerError(
                    f"timestamp must be str|datetime|None, "
                    f"got {type(ts).__name__}."
                )
            return cls(
                accuracy=float(data.get("accuracy", 0.0)),
                partial_credit=float(data.get("partial_credit", 0.0)),
                energy_efficiency=float(
                    data.get("energy_efficiency", 0.0)
                ),
                sustainability_index=float(
                    data.get("sustainability_index", 0.0)
                ),
                failure_category=data.get("failure_category"),
                penalty=float(data.get("penalty", 0.0)),
                status=str(data.get("status", _UNKNOWN_STATUS)),
                timestamp=timestamp,
            )
        except RobustScorerError:
            raise
        except (TypeError, ValueError) as exc:
            raise RobustScorerError(
                f"invalid ScoringResult payload: {exc}"
            ) from exc

    def __repr__(self) -> str:
        return (
            "ScoringResult("
            f"accuracy={self.accuracy:.3f}, "
            f"partial={self.partial_credit:.3f}, "
            f"sustainability={self.sustainability_index:.3f}, "
            f"status={self.status!r}, "
            f"failure={self.failure_category!r})"
        )


# --------------------------------------------------------------------------- #
# Scorer
# --------------------------------------------------------------------------- #
class RobustSustainabilityScorer:
    """
    Handles all failure modes gracefully when scoring agent results.

    Thread-safe, serializable, and bounded in memory. The original public
    API (``score(result, ground_truth) -> dict``) is preserved; new
    parameters are keyword-only.

    ``strict`` behaviour
    --------------------
    - ``strict=True`` — raises :class:`RobustScorerError` on non-Mapping
      ``result`` / ``ground_truth``, and on statuses outside
      ``{success, timeout, error, oom}``.
    - ``strict=False`` — coerces non-Mapping inputs (``result`` →
      ``{"status": "error", "partial_output": <str>}``; ``ground_truth``
      → ``{}``) and treats unknown statuses as ``"unknown"``; both paths
      log a warning.

    ``_started_at`` lifecycle
    -------------------------
    ``_started_at`` is set at construction. :meth:`reset` resets it to
    ``time.time()``. :meth:`from_dict` restores the serialized value, so
    ``statistics()["uptime_seconds"]`` on a restored scorer reports the
    **lifetime of the serialized object**, not the current instance. Call
    :meth:`reset` after restoring if you want uptime relative to now.
    """

    def __init__(
        self,
        *,
        config: Optional[RobustScorerConfig] = None,
        strict: bool = True,
    ) -> None:
        if config is not None and not isinstance(config, RobustScorerConfig):
            raise RobustScorerError(
                f"config must be a RobustScorerConfig, got "
                f"{type(config).__name__}."
            )
        self._config = config or RobustScorerConfig()
        self._strict = bool(strict)

        self._lock = threading.RLock()
        self._history: Deque[ScoringResult] = deque(
            maxlen=self._config.max_history
        )
        self._started_at: float = time.time()

        # Lifetime counters — cumulative; not reset by reset().
        self._total_scored: int = 0
        self._dropped_count: int = 0

        logger.debug(
            "RobustSustainabilityScorer initialized "
            "(metric=%s, case_sensitive=%s, strict=%s)",
            self._config.similarity_metric,
            self._config.case_sensitive,
            self._strict,
        )

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> RobustScorerConfig:
        return self._config

    @property
    def strict(self) -> bool:
        return self._strict

    @property
    def history(self) -> List[ScoringResult]:
        with self._lock:
            return list(self._history)

    # ---------------------------------------------------------- public API
    def score(
        self,
        result: Mapping[str, Any],
        ground_truth: Mapping[str, Any],
    ) -> ScoreMetrics:
        """
        Score an agent result against the ground truth.

        See the class docstring for ``strict``-mode input handling.
        Returns a :class:`ScoreMetrics` dict with keys:
        ``accuracy``, ``partial_credit``, ``energy_efficiency``,
        ``sustainability_index``, ``failure_category``, ``penalty``.
        """
        scoring, record = self._score_impl(result, ground_truth)
        with self._lock:
            self._append_record_locked(record)
        logger.debug(
            "Scored result: status=%s accuracy=%.3f partial=%.3f "
            "sustainability=%.3f",
            record.status, record.accuracy, record.partial_credit,
            record.sustainability_index,
        )
        return scoring

    def score_detailed(
        self,
        result: Mapping[str, Any],
        ground_truth: Mapping[str, Any],
    ) -> ScoringResult:
        """
        Like :meth:`score`, but returns the full immutable
        :class:`ScoringResult` (including ``timestamp``).
        """
        _scoring, record = self._score_impl(result, ground_truth)
        with self._lock:
            self._append_record_locked(record)
        return record

    def score_batch(
        self,
        pairs: Sequence[Tuple[Mapping[str, Any], Mapping[str, Any]]],
    ) -> List[ScoreMetrics]:
        """
        Score many ``(result, ground_truth)`` pairs with one lock
        acquisition. All validation and computation happens before any
        history mutation, so a failure midway leaves the scorer
        untouched.
        """
        if not isinstance(pairs, Sequence):
            raise RobustScorerError("pairs must be a Sequence.")
        computed: List[Tuple[ScoreMetrics, ScoringResult]] = []
        for pair in pairs:
            if not (isinstance(pair, tuple) and len(pair) == 2):
                raise RobustScorerError(
                    "each pair must be a (result, ground_truth) tuple."
                )
            computed.append(self._score_impl(pair[0], pair[1]))
        with self._lock:
            for _scoring, record in computed:
                self._append_record_locked(record)
        return [scoring for scoring, _record in computed]

    # ---------------------------------------------------------- internals
    def _score_impl(
        self,
        result: Any,
        ground_truth: Any,
    ) -> Tuple[ScoreMetrics, ScoringResult]:
        """
        Return ``(scoring_dict, record)`` without touching history.

        May raise :class:`RobustScorerError` in strict mode.
        """
        # ---- Coerce / validate inputs ----------------------------- #
        if not isinstance(result, Mapping):
            msg = (
                f"result must be a Mapping, got {type(result).__name__}."
            )
            if self._strict:
                raise RobustScorerError(msg)
            logger.warning("%s Coercing to an error result.", msg)
            result = {"status": "error", "partial_output": str(result)}
        if not isinstance(ground_truth, Mapping):
            msg = (
                f"ground_truth must be a Mapping, "
                f"got {type(ground_truth).__name__}."
            )
            if self._strict:
                raise RobustScorerError(msg)
            logger.warning("%s Coercing to empty ground truth.", msg)
            ground_truth = {}

        # ---- Normalise status ------------------------------------- #
        raw_status = result.get("status", "success")
        status = (
            "success" if raw_status is None
            else str(raw_status).lower()
        )

        if status not in _KNOWN_STATUSES:
            msg = (
                f"unknown status {raw_status!r}; "
                f"expected one of {sorted(_KNOWN_STATUSES)}."
            )
            if self._strict:
                raise RobustScorerError(msg)
            logger.warning("%s Treating as 'unknown'.", msg)
            status = _UNKNOWN_STATUS

        # ---- Dispatch --------------------------------------------- #
        if status == "timeout":
            scoring = self._handle_timeout(result, ground_truth)
        elif status == "error":
            scoring = self._handle_error(result, ground_truth)
        elif status == "oom":
            scoring = self._handle_oom(result, ground_truth)
        elif status == "success":
            scoring = self._score_success(result, ground_truth)
        else:  # _UNKNOWN_STATUS
            scoring = self._handle_unknown_status(result, ground_truth)

        # ---- Build the record ------------------------------------- #
        record = ScoringResult(
            accuracy=scoring["accuracy"],
            partial_credit=scoring["partial_credit"],
            energy_efficiency=scoring["energy_efficiency"],
            sustainability_index=scoring["sustainability_index"],
            failure_category=scoring["failure_category"],
            penalty=scoring["penalty"],
            status=status,
        )

        if status == "success" and record.accuracy == 0.0:
            logger.warning(
                "Status=success but accuracy=0.0; "
                "check output vs ground_truth."
            )

        return scoring, record

    def _append_record_locked(self, record: ScoringResult) -> None:
        """Append + bump counters. Caller must hold ``_lock``."""
        self._total_scored += 1
        if len(self._history) == self._config.max_history:
            self._dropped_count += 1
        self._history.append(record)

    # ---------------------------------------------------------- handlers
    def _handle_timeout(
        self,
        result: Mapping[str, Any],
        ground_truth: Mapping[str, Any],
    ) -> ScoreMetrics:
        """Award partial credit for a timeout with partial output."""
        cfg = self._config
        partial_output = str(result.get("partial_output", ""))
        ground_truth_text = str(ground_truth.get("output", ""))
        similarity = self._compute_similarity(
            partial_output, ground_truth_text
        )
        return {
            "accuracy": 0.0,
            "partial_credit": similarity * cfg.timeout_partial_credit_max,
            "energy_efficiency": self._energy_efficiency(result),
            "sustainability_index": (
                similarity * cfg.timeout_sustainability_max
            ),
            "failure_category": "timeout",
            "penalty": cfg.timeout_penalty,
        }

    def _handle_error(
        self,
        result: Mapping[str, Any],
        ground_truth: Mapping[str, Any],
    ) -> ScoreMetrics:
        """Award minimal partial credit when the agent errored."""
        cfg = self._config
        partial_output = str(result.get("partial_output", ""))
        ground_truth_text = str(ground_truth.get("output", ""))
        similarity = self._compute_similarity(
            partial_output, ground_truth_text
        )
        return {
            "accuracy": 0.0,
            "partial_credit": similarity * cfg.error_partial_credit_max,
            "energy_efficiency": self._energy_efficiency(result),
            "sustainability_index": (
                similarity * cfg.error_sustainability_max
            ),
            "failure_category": "error",
            "penalty": cfg.error_penalty,
        }

    def _handle_oom(
        self,
        result: Mapping[str, Any],
        ground_truth: Mapping[str, Any],  # noqa: ARG002 - uniform signature
    ) -> ScoreMetrics:
        """
        OOM is a hard failure — no partial credit.

        Signature kept consistent with the other handlers for uniform
        dispatch; ``ground_truth`` is accepted but unused.
        """
        cfg = self._config
        return {
            "accuracy": 0.0,
            "partial_credit": 0.0,
            "energy_efficiency": self._energy_efficiency(result),
            "sustainability_index": 0.0,
            "failure_category": "oom",
            "penalty": cfg.oom_penalty,
        }

    def _handle_unknown_status(
        self,
        result: Mapping[str, Any],
        ground_truth: Mapping[str, Any],
    ) -> ScoreMetrics:
        """
        Unknown status — treat conservatively as an error, using error's
        partial-credit and sustainability ceilings, with the
        ``unknown_penalty``.
        """
        cfg = self._config
        partial_output = str(result.get("partial_output", ""))
        ground_truth_text = str(ground_truth.get("output", ""))
        similarity = self._compute_similarity(
            partial_output, ground_truth_text
        )
        return {
            "accuracy": 0.0,
            "partial_credit": similarity * cfg.error_partial_credit_max,
            "energy_efficiency": self._energy_efficiency(result),
            "sustainability_index": (
                similarity * cfg.error_sustainability_max
            ),
            "failure_category": "unknown",
            "penalty": cfg.unknown_penalty,
        }

    def _score_success(
        self,
        result: Mapping[str, Any],
        ground_truth: Mapping[str, Any],
    ) -> ScoreMetrics:
        """Score a successful result."""
        output = str(result.get("output", ""))
        ground_truth_text = str(ground_truth.get("output", ""))
        accuracy = self._compute_similarity(output, ground_truth_text)
        return {
            "accuracy": accuracy,
            "partial_credit": 0.0,
            "energy_efficiency": self._energy_efficiency(result),
            "sustainability_index": accuracy,
            "failure_category": None,
            "penalty": 0.0,
        }

    # ---------------------------------------------------------- helpers
    def _energy_efficiency(self, result: Mapping[str, Any]) -> float:
        """
        Return ``energy_used / time_limit`` (lower is better).

        .. note::
            The name is retained for backward compatibility; this is a
            raw ratio, **not** an efficiency in the "higher is better"
            sense. Set ``config.max_energy_ratio`` to bound it for RL
            training.

        Guards against zero ``time_limit`` by falling back to
        ``config.default_time_limit``; guards against NaN / negative
        energy inputs by returning 0 for those components.
        """
        try:
            energy_used = float(
                result.get("energy_used", self._config.default_energy_used)
            )
        except (TypeError, ValueError):
            energy_used = self._config.default_energy_used
        try:
            time_limit = float(
                result.get("time_limit", self._config.default_time_limit)
            )
        except (TypeError, ValueError):
            time_limit = self._config.default_time_limit

        if not math.isfinite(energy_used) or energy_used < 0:
            energy_used = 0.0
        if not math.isfinite(time_limit) or time_limit <= 0:
            time_limit = self._config.default_time_limit

        ratio = energy_used / time_limit
        if self._config.max_energy_ratio is not None:
            ratio = min(ratio, self._config.max_energy_ratio)
        return ratio

    def _compute_similarity(self, a: str, b: str) -> float:
        """
        Compute similarity in ``[0, 1]``.

        Normalisation order: strip whitespace → lowercase (unless
        ``case_sensitive``) → truncate to ``max_similarity_length``.
        """
        a = a.strip()
        b = b.strip()
        if not self._config.case_sensitive:
            a = a.lower()
            b = b.lower()
        max_len = self._config.max_similarity_length
        if max_len > 0:
            a = a[:max_len]
            b = b[:max_len]

        if not a and not b:
            return 1.0
        if not a or not b:
            return 0.0
        if self._config.similarity_metric == "exact":
            return 1.0 if a == b else 0.0
        return float(SequenceMatcher(None, a, b).ratio())

    # ---------------------------------------------------------- statistics
    def statistics(self) -> ScorerStatistics:
        """Return a JSON-safe, atomic snapshot of the scorer state."""
        with self._lock:
            by_status: Dict[str, int] = {}
            for r in self._history:
                by_status[r.status] = by_status.get(r.status, 0) + 1
            n = len(self._history)
            mean_accuracy = (
                sum(r.accuracy for r in self._history) / n if n else 0.0
            )
            mean_sustainability = (
                sum(r.sustainability_index for r in self._history) / n
                if n else 0.0
            )
            return {
                "total_scored": self._total_scored,
                "dropped_scored": self._dropped_count,
                "buffered_scored": n,
                "by_status": by_status,
                "mean_accuracy": mean_accuracy,
                "mean_sustainability": mean_sustainability,
                "config": self._config.to_dict(),
                "strict": self._strict,
                "uptime_seconds": time.time() - self._started_at,
            }

    def reset(self, *, clear_history: bool = True) -> int:
        """
        Reset the scorer history and uptime anchor.

        Returns the number of *buffered* history entries removed.
        Lifetime counters (``total_scored``, ``dropped_count``) are
        **not** reset.
        """
        with self._lock:
            removed = len(self._history)
            if clear_history:
                self._history.clear()
            self._started_at = time.time()
        logger.debug("RobustSustainabilityScorer reset (removed %d).", removed)
        return removed

    # ---------------------------------------------------------- serialization
    def to_dict(self, *, include_history: bool = False) -> Dict[str, Any]:
        with self._lock:
            payload: Dict[str, Any] = {
                "config": self._config.to_dict(),
                "strict": self._strict,
                "started_at": self._started_at,
                "total_scored": self._total_scored,
                "dropped_count": self._dropped_count,
            }
            if include_history:
                payload["history"] = [
                    r.to_dict() for r in self._history
                ]
        return payload

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "RobustSustainabilityScorer":
        if not isinstance(data, Mapping):
            raise RobustScorerError(
                f"from_dict expects a Mapping, got {type(data).__name__}."
            )
        try:
            cfg = RobustScorerConfig.from_dict(
                dict(data.get("config") or {}), strict=True
            )
            scorer = cls(config=cfg, strict=bool(data.get("strict", True)))
            with scorer._lock:
                if data.get("started_at") is not None:
                    scorer._started_at = float(data["started_at"])
                for entry in data.get("history", []) or []:
                    scorer._history.append(
                        ScoringResult.from_dict(entry)
                    )
                if "total_scored" in data:
                    scorer._total_scored = int(data["total_scored"])
                elif scorer._history:
                    scorer._total_scored = len(scorer._history)
                if "dropped_count" in data:
                    scorer._dropped_count = int(data["dropped_count"])
            return scorer
        except RobustScorerError:
            raise
        except (TypeError, ValueError) as exc:
            raise RobustScorerError(
                f"invalid RobustSustainabilityScorer payload: {exc}"
            ) from exc

    def to_json(
        self, *, include_history: bool = False, **kwargs: Any
    ) -> str:
        # Payload is primitive-only; no default= needed.
        return json.dumps(
            self.to_dict(include_history=include_history), **kwargs
        )

    @classmethod
    def from_json(cls, payload: str) -> "RobustSustainabilityScorer":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise RobustScorerError(f"Invalid JSON: {exc}") from exc
        return cls.from_dict(data)

    # ----------------------------------------------------------------- dunder
    def __repr__(self) -> str:
        with self._lock:
            return (
                "RobustSustainabilityScorer("
                f"scored={self._total_scored}, "
                f"buffered={len(self._history)}, "
                f"metric={self._config.similarity_metric!r}, "
                f"strict={self._strict})"
            )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, RobustSustainabilityScorer):
            return NotImplemented
        return (
            self._config == other._config
            and self._strict == other._strict
        )

    def __hash__(self) -> int:
        return hash((self._config, self._strict))


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "KNOWN_FAILURE_CATEGORIES",
    "RobustScorerConfig",
    "RobustScorerError",
    "RobustSustainabilityScorer",
    "ScoreMetrics",
    "ScorerStatistics",
    "ScoringResult",
]


def __dir__() -> List[str]:  # pragma: no cover — trivial
    return sorted(__all__)


# --------------------------------------------------------------------------- #
# Smoke test: python -m scoring.robust_scorer
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )

    scorer = RobustSustainabilityScorer()
    print("repr       :", scorer)

    gt = {"output": "The answer is 42."}

    # ---- Success --------------------------------------------------- #
    s = scorer.score({"status": "success", "output": "The answer is 42.",
                      "energy_used": 0.01, "time_limit": 30.0}, gt)
    print("success    :", s)
    assert s["accuracy"] == 1.0

    # ---- Timeout (partial credit) ---------------------------------- #
    s = scorer.score({"status": "timeout", "partial_output": "The answer is",
                      "energy_used": 0.02, "time_limit": 30.0}, gt)
    print("timeout    :", s)
    assert s["failure_category"] == "timeout"
    assert 0 < s["partial_credit"] <= 0.5

    # ---- Error ----------------------------------------------------- #
    s = scorer.score({"status": "error", "partial_output": "The answer",
                      "energy_used": 0.005, "time_limit": 30.0}, gt)
    print("error      :", s)
    assert s["failure_category"] == "error"

    # ---- OOM (no partial credit) ----------------------------------- #
    s = scorer.score({"status": "oom"}, gt)
    print("oom        :", s)
    assert s["partial_credit"] == 0.0 and s["penalty"] == 1.0

    # ---- Division by zero ----------------------------------------- #
    s = scorer.score({"status": "success", "output": "x",
                      "energy_used": 0.01, "time_limit": 0}, gt)
    print("zero time  :", s["energy_efficiency"], "(no crash)")

    # ---- Missing status ------------------------------------------- #
    s = scorer.score({"output": "The answer is 42."}, gt)
    print("no status  :", s["accuracy"], "(defaults to success)")

    # ---- Unknown status raises in strict mode --------------------- #
    try:
        scorer.score({"status": "partial", "output": "x"}, gt)
    except RobustScorerError as exc:
        print("Rejected   :", exc)

    # ---- strict=False is actually lenient ------------------------- #
    lenient = RobustSustainabilityScorer(strict=False)
    s = lenient.score("not-a-mapping", gt)  # type: ignore[arg-type]
    print("lenient    :", s["failure_category"])
    assert s["failure_category"] == "error"

    s = lenient.score({"status": "partial", "partial_output": "The answer"}, gt)
    print("unknown    :", s["failure_category"], "(non-strict only)")
    assert s["failure_category"] == "unknown"

    # ---- score_detailed ------------------------------------------- #
    detailed = scorer.score_detailed(
        {"status": "success", "output": "The answer is 42."}, gt
    )
    print("detailed   :", detailed)
    assert isinstance(detailed, ScoringResult)
    assert detailed.accuracy == 1.0

    # ---- score_batch ---------------------------------------------- #
    batch = scorer.score_batch([
        ({"status": "success", "output": "The answer is 42."}, gt),
        ({"status": "timeout", "partial_output": "The answer"}, gt),
        ({"status": "oom"}, gt),
    ])
    print("batch      :", [b["failure_category"] for b in batch])

    # ---- Bounded config ------------------------------------------- #
    for bad_cfg in (
        dict(timeout_partial_credit_max=2.0),    # > 1
        dict(error_sustainability_max=5.0),      # > 1
        dict(timeout_penalty=-1.0),              # negative
        dict(similarity_metric="bogus"),
        dict(default_time_limit=0),
        dict(max_history=0),
        dict(max_energy_ratio=0),                # must be > 0
        dict(max_similarity_length=-1),
        dict(case_sensitive="yes"),              # not a bool
    ):
        try:
            RobustScorerConfig(**bad_cfg)  # type: ignore[arg-type]
        except RobustScorerError as exc:
            print("Rejected cfg:", exc)

    # ---- Bounded ScoringResult ------------------------------------ #
    try:
        ScoringResult(
            accuracy=2.0, partial_credit=0.0, energy_efficiency=0.0,
            sustainability_index=0.0, failure_category=None,
            penalty=0.0, status="success",
        )
    except RobustScorerError as exc:
        print("Rejected res:", exc)

    try:
        ScoringResult(
            accuracy=0.5, partial_credit=0.0, energy_efficiency=0.0,
            sustainability_index=0.0, failure_category="not-a-category",
            penalty=0.0, status="success",
        )
    except RobustScorerError as exc:
        print("Rejected res:", exc)

    # ---- Round-trip with history (bug fix) ------------------------ #
    payload = scorer.to_json(include_history=True)
    restored = RobustSustainabilityScorer.from_json(payload)
    assert len(restored.history) == len(scorer.history), (
        f"history not restored: {len(restored.history)} vs "
        f"{len(scorer.history)}"
    )
    assert restored.to_dict(include_history=True) == (
        scorer.to_dict(include_history=True)
    )
    assert restored == scorer
    print("round-trip : OK")

    # ---- Case sensitivity ----------------------------------------- #
    default_case = RobustSustainabilityScorer()
    s = default_case.score(
        {"status": "success", "output": "THE ANSWER IS 42."}, gt
    )
    print("insensitive:", f"{s['accuracy']:.3f}")
    assert s["accuracy"] == 1.0

    case_cfg = RobustScorerConfig(case_sensitive=True)
    case_scorer = RobustSustainabilityScorer(config=case_cfg)
    s = case_scorer.score(
        {"status": "success", "output": "THE ANSWER IS 42."}, gt
    )
    print("sensitive  :", f"{s['accuracy']:.3f}")
    assert s["accuracy"] < 1.0

    # ---- Energy clamping ------------------------------------------ #
    clamp_cfg = RobustScorerConfig(max_energy_ratio=5.0)
    clamp_scorer = RobustSustainabilityScorer(config=clamp_cfg)
    s = clamp_scorer.score(
        {"status": "success", "output": "x",
         "energy_used": 1000.0, "time_limit": 1.0},
        gt,
    )
    print("clamp      :", s["energy_efficiency"])
    assert s["energy_efficiency"] == 5.0

    # ---- max_similarity_length truncation ------------------------- #
    long_a = "a" * 20_000
    long_b = "a" * 20_000
    trunc_cfg = RobustScorerConfig(max_similarity_length=100)
    trunc_scorer = RobustSustainabilityScorer(config=trunc_cfg)
    s = trunc_scorer.score(
        {"status": "success", "output": long_a}, {"output": long_b}
    )
    print("truncated  :", f"{s['accuracy']:.3f}")
    assert s["accuracy"] == 1.0

    # ---- Statistics counters invariant ---------------------------- #
    stats = scorer.statistics()
    print("stats      :", {
        k: v for k, v in stats.items()
        if k not in ("config", "uptime_seconds", "by_status")
    })
    assert (
        stats["total_scored"]
        == stats["buffered_scored"] + stats["dropped_scored"]
    ), "counter invariant violated"

    # ---- from_dict wraps parse errors ----------------------------- #
    try:
        RobustSustainabilityScorer.from_dict({"started_at": "not-a-number"})
    except RobustScorerError as exc:
        print("Rejected   :", exc)

    try:
        RobustSustainabilityScorer.from_dict({
            "history": [{"accuracy": 5.0}]  # out of bounds
        })
    except RobustScorerError as exc:
        print("Rejected   :", exc)

    # ---- Unknown config keys rejected ----------------------------- #
    try:
        RobustScorerConfig.from_dict({"typo": 1}, strict=True)
    except RobustScorerError as exc:
        print("Rejected   :", exc)

    # ---- to_json without history still works ---------------------- #
    small = scorer.to_json()
    assert "history" not in small
    print("to_json    : OK")

    print("\nSmoke test passed.")
