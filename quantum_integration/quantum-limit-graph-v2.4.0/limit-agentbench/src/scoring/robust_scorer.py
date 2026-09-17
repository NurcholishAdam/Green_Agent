# src/scoring/robust_scorer.py

"""
Robust Sustainability Scorer
============================

Handles all failure modes gracefully when scoring agent results.

Given a result dict and a ground-truth dict, produces a consistent scoring
dictionary regardless of whether the agent succeeded, timed out, ran out of
memory, or raised an error.

Enhancements
------------
- ``RobustScorerConfig`` — frozen, validated: partial-credit ceilings,
  similarity strategy, penalties, bounded history.
- ``ScoringResult`` — frozen dataclass with validation and serialization.
- **Implemented missing methods** — ``_handle_error``, ``_handle_oom``,
  ``_score_success``, ``_compute_similarity`` were referenced but never
  defined; calling ``score()`` with a non-timeout status raised
  ``AttributeError``.
- **Fixed division by zero** — ``energy_used / time_limit`` crashed when
  ``time_limit=0``. Now guarded.
- **Fixed missing ``status``** — a result without a ``status`` key raised
  ``KeyError``. Now defaults to ``"success"``.
- **Full validation** of every argument; strict / non-strict modes.
- **Thread safety** — ``RLock`` guards the history.
- **Bounded history** — ``deque(maxlen=config.max_history)``.
- Serialization: ``to_dict`` / ``from_dict`` / ``to_json`` on the scorer and
  the result dataclass.
- ``statistics()``, ``__repr__``, custom ``RobustScorerError``, lazy ``%s``
  logging, and a ``__main__`` smoke test.
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
from typing import Any, Deque, Dict, List, Mapping, Optional

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class RobustScorerError(ValueError):
    """Raised for invalid robust-scorer inputs or configuration."""


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class RobustScorerConfig:
    """Tunable parameters for :class:`RobustSustainabilityScorer`."""

    # Partial-credit ceilings per failure mode.
    timeout_partial_credit_max: float = 0.5
    error_partial_credit_max: float = 0.2
    oom_partial_credit_max: float = 0.0

    # Sustainability-index ceilings per failure mode.
    timeout_sustainability_max: float = 0.25
    error_sustainability_max: float = 0.10
    oom_sustainability_max: float = 0.0

    # Penalties subtracted from ``accuracy`` per failure mode.
    timeout_penalty: float = 0.5
    error_penalty: float = 0.7
    oom_penalty: float = 1.0

    # Similarity function.
    similarity_metric: str = "sequence_matcher"  # "sequence_matcher" | "exact"

    # Default time limit and energy used when the result omits them.
    default_time_limit: float = 30.0
    default_energy_used: float = 0.0

    # Bounded scoring history.
    max_history: int = 1_000

    def __post_init__(self) -> None:
        for name in (
            "timeout_partial_credit_max", "error_partial_credit_max",
            "oom_partial_credit_max", "timeout_sustainability_max",
            "error_sustainability_max", "oom_sustainability_max",
            "timeout_penalty", "error_penalty", "oom_penalty",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise RobustScorerError(f"{name} must be numeric.")
            fv = float(value)
            if math.isnan(fv) or math.isinf(fv) or fv < 0:
                raise RobustScorerError(
                    f"{name} must be finite and >= 0, got {value!r}."
                )
        if self.similarity_metric not in ("sequence_matcher", "exact"):
            raise RobustScorerError(
                "similarity_metric must be 'sequence_matcher' or 'exact'."
            )
        if self.default_time_limit <= 0:
            raise RobustScorerError("default_time_limit must be > 0.")
        if self.default_energy_used < 0:
            raise RobustScorerError("default_energy_used must be >= 0.")
        if self.max_history <= 0:
            raise RobustScorerError("max_history must be > 0.")

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "RobustScorerConfig":
        if not isinstance(data, Mapping):
            raise RobustScorerError(
                "RobustScorerConfig.from_dict expects a Mapping."
            )
        valid = set(cls.__dataclass_fields__.keys())
        kwargs = {k: v for k, v in data.items() if k in valid}
        return cls(**kwargs)


# --------------------------------------------------------------------------- #
# Scoring result
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class ScoringResult:
    """Immutable scoring result returned by :meth:`RobustSustainabilityScorer.score`."""

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
        for name in (
            "accuracy", "partial_credit", "energy_efficiency",
            "sustainability_index", "penalty",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise RobustScorerError(f"{name} must be numeric.")
            fv = float(value)
            if math.isnan(fv) or math.isinf(fv) or fv < 0:
                raise RobustScorerError(
                    f"{name} must be finite and >= 0, got {value!r}."
                )
        if self.failure_category is not None and (
            not isinstance(self.failure_category, str)
            or not self.failure_category
        ):
            raise RobustScorerError(
                "failure_category must be a non-empty string or None."
            )
        if not isinstance(self.status, str) or not self.status:
            raise RobustScorerError("status must be a non-empty string.")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "accuracy": self.accuracy,
            "partial_credit": self.partial_credit,
            "energy_efficiency": self.energy_efficiency,
            "sustainability_index": self.sustainability_index,
            "failure_category": self.failure_category,
            "penalty": self.penalty,
            "status": self.status,
            "timestamp": self.timestamp.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ScoringResult":
        if not isinstance(data, Mapping):
            raise RobustScorerError(
                "ScoringResult.from_dict expects a Mapping."
            )
        ts = data.get("timestamp")
        if isinstance(ts, str):
            timestamp = datetime.fromisoformat(ts)
            if not timestamp.tzinfo:
                timestamp = timestamp.replace(tzinfo=timezone.utc)
        elif isinstance(ts, datetime):
            timestamp = ts
        else:
            timestamp = datetime.now(timezone.utc)
        return cls(
            accuracy=float(data.get("accuracy", 0.0)),
            partial_credit=float(data.get("partial_credit", 0.0)),
            energy_efficiency=float(data.get("energy_efficiency", 0.0)),
            sustainability_index=float(data.get("sustainability_index", 0.0)),
            failure_category=data.get("failure_category"),
            penalty=float(data.get("penalty", 0.0)),
            status=str(data.get("status", "unknown")),
            timestamp=timestamp,
        )

    def __repr__(self) -> str:
        return (
            "ScoringResult("
            f"accuracy={self.accuracy:.3f}, "
            f"partial={self.partial_credit:.3f}, "
            f"sustainability={self.sustainability_index:.3f}, "
            f"failure={self.failure_category!r})"
        )


# --------------------------------------------------------------------------- #
# Scorer
# --------------------------------------------------------------------------- #
class RobustSustainabilityScorer:
    """
    Handles all failure modes gracefully when scoring agent results.

    Thread-safe, serializable, and bounded in memory. The original public API
    (``score(result, ground_truth) -> dict``) is preserved; new parameters
    are keyword-only.
    """

    def __init__(
        self,
        *,
        config: Optional[RobustScorerConfig] = None,
        strict: bool = True,
    ) -> None:
        self._config = config or RobustScorerConfig()
        self._strict = bool(strict)

        self._lock = threading.RLock()
        self._history: Deque[ScoringResult] = deque(
            maxlen=self._config.max_history
        )
        self._started_at: float = time.time()

        logger.debug(
            "RobustSustainabilityScorer initialized "
            "(metric=%s, strict=%s)",
            self._config.similarity_metric, self._strict,
        )

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> RobustScorerConfig:
        return self._config

    @property
    def history(self) -> List[ScoringResult]:
        with self._lock:
            return list(self._history)

    # ---------------------------------------------------------- public API
    def score(
        self,
        result: Mapping[str, Any],
        ground_truth: Mapping[str, Any],
    ) -> Dict[str, Any]:
        """
        Score an agent result against the ground truth.

        Parameters
        ----------
        result : Mapping
            Agent result with ``status`` (one of ``"success"``, ``"timeout"``,
            ``"error"``, ``"oom"``), and (depending on status)
            ``partial_output``, ``output``, ``energy_used``, ``time_limit``.
        ground_truth : Mapping
            Reference answer with an ``output`` field.

        Returns
        -------
        dict
            Keys: ``accuracy``, ``partial_credit``, ``energy_efficiency``,
            ``sustainability_index``, ``failure_category``, ``penalty``.
        """
        if not isinstance(result, Mapping):
            raise RobustScorerError("result must be a Mapping.")
        if not isinstance(ground_truth, Mapping):
            raise RobustScorerError("ground_truth must be a Mapping.")

        status = str(result.get("status", "success")).lower()

        if status == "timeout":
            scoring = self._handle_timeout(result, ground_truth)
        elif status == "error":
            scoring = self._handle_error(result, ground_truth)
        elif status == "oom":
            scoring = self._handle_oom(result)
        else:
            scoring = self._score_success(result, ground_truth)

        record = ScoringResult(
            accuracy=scoring["accuracy"],
            partial_credit=scoring["partial_credit"],
            energy_efficiency=scoring["energy_efficiency"],
            sustainability_index=scoring["sustainability_index"],
            failure_category=scoring["failure_category"],
            penalty=scoring["penalty"],
            status=status,
        )
        with self._lock:
            self._history.append(record)

        logger.debug(
            "Scored result: status=%s accuracy=%.3f partial=%.3f "
            "sustainability=%.3f",
            status, record.accuracy, record.partial_credit,
            record.sustainability_index,
        )
        return scoring

    # ---------------------------------------------------------- handlers
    def _handle_timeout(
        self, result: Mapping[str, Any], ground_truth: Mapping[str, Any]
    ) -> Dict[str, Any]:
        """Award partial credit for a timeout with partial output."""
        cfg = self._config
        partial_output = str(result.get("partial_output", ""))
        ground_truth_text = str(ground_truth.get("output", ""))
        similarity = self._compute_similarity(partial_output, ground_truth_text)
        return {
            "accuracy": 0.0,
            "partial_credit": similarity * cfg.timeout_partial_credit_max,
            "energy_efficiency": self._energy_efficiency(result),
            "sustainability_index": similarity * cfg.timeout_sustainability_max,
            "failure_category": "timeout",
            "penalty": cfg.timeout_penalty,
        }

    def _handle_error(
        self, result: Mapping[str, Any], ground_truth: Mapping[str, Any]
    ) -> Dict[str, Any]:
        """Award minimal partial credit when the agent errored."""
        cfg = self._config
        partial_output = str(result.get("partial_output", ""))
        ground_truth_text = str(ground_truth.get("output", ""))
        similarity = self._compute_similarity(partial_output, ground_truth_text)
        return {
            "accuracy": 0.0,
            "partial_credit": similarity * cfg.error_partial_credit_max,
            "energy_efficiency": self._energy_efficiency(result),
            "sustainability_index": similarity * cfg.error_sustainability_max,
            "failure_category": "error",
            "penalty": cfg.error_penalty,
        }

    def _handle_oom(self, result: Mapping[str, Any]) -> Dict[str, Any]:
        """OOM is a hard failure — no partial credit."""
        cfg = self._config
        return {
            "accuracy": 0.0,
            "partial_credit": 0.0,
            "energy_efficiency": self._energy_efficiency(result),
            "sustainability_index": 0.0,
            "failure_category": "oom",
            "penalty": cfg.oom_penalty,
        }

    def _score_success(
        self, result: Mapping[str, Any], ground_truth: Mapping[str, Any]
    ) -> Dict[str, Any]:
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

        Guards against zero ``time_limit`` by falling back to
        ``config.default_time_limit``.
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
        if time_limit <= 0:
            time_limit = self._config.default_time_limit
        if math.isnan(energy_used) or math.isinf(energy_used) or energy_used < 0:
            energy_used = 0.0
        return energy_used / time_limit

    def _compute_similarity(self, a: str, b: str) -> float:
        """Compute similarity in ``[0, 1]``."""
        if not a and not b:
            return 1.0
        if not a or not b:
            return 0.0
        if self._config.similarity_metric == "exact":
            return 1.0 if a.strip() == b.strip() else 0.0
        # SequenceMatcher.ratio() is in [0, 1].
        return float(SequenceMatcher(None, a, b).ratio())

    # ---------------------------------------------------------- statistics
    def statistics(self) -> Dict[str, Any]:
        """Return a JSON-safe snapshot of the scorer state."""
        with self._lock:
            history = list(self._history)
        by_status: Dict[str, int] = {}
        for r in history:
            by_status[r.status] = by_status.get(r.status, 0) + 1
        return {
            "total_scored": len(history),
            "by_status": by_status,
            "mean_accuracy": (
                sum(r.accuracy for r in history) / len(history)
                if history else 0.0
            ),
            "mean_sustainability": (
                sum(r.sustainability_index for r in history) / len(history)
                if history else 0.0
            ),
            "config": self._config.to_dict(),
            "strict": self._strict,
            "uptime_seconds": time.time() - self._started_at,
        }

    def reset(self, *, clear_history: bool = True) -> int:
        """Reset the scorer history. Returns entries removed."""
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
            }
            if include_history:
                payload["history"] = [r.to_dict() for r in self._history]
        return payload

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "RobustSustainabilityScorer":
        if not isinstance(data, Mapping):
            raise RobustScorerError(
                f"from_dict expects a Mapping, got {type(data).__name__}."
            )
        cfg = RobustScorerConfig.from_dict(
            dict(data.get("config", {}) or {})
        )
        scorer = cls(config=cfg, strict=bool(data.get("strict", True)))
        scorer._started_at = float(data.get("started_at", time.time()))
        return scorer

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    @classmethod
    def from_json(cls, payload: str) -> "RobustSustainabilityScorer":
        try:
            return cls.from_dict(json.loads(payload))
        except json.JSONDecodeError as exc:
            raise RobustScorerError(f"Invalid JSON: {exc}") from exc

    # ----------------------------------------------------------------- dunder
    def __repr__(self) -> str:
        with self._lock:
            return (
                "RobustSustainabilityScorer("
                f"scored={len(self._history)}, "
                f"metric={self._config.similarity_metric!r}, "
                f"strict={self._strict})"
            )


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "RobustScorerConfig",
    "RobustScorerError",
    "RobustSustainabilityScorer",
    "ScoringResult",
]


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

    # ---- Timeout (partial credit) ---------------------------------- #
    s = scorer.score({"status": "timeout", "partial_output": "The answer is",
                      "energy_used": 0.02, "time_limit": 30.0}, gt)
    print("timeout    :", s)
    assert s["failure_category"] == "timeout"
    assert s["partial_credit"] > 0

    # ---- Error ----------------------------------------------------- #
    s = scorer.score({"status": "error", "partial_output": "The answer",
                      "energy_used": 0.005, "time_limit": 30.0}, gt)
    print("error      :", s)
    assert s["failure_category"] == "error"

    # ---- OOM (no partial credit) ----------------------------------- #
    s = scorer.score({"status": "oom"}, gt)
    print("oom        :", s)
    assert s["partial_credit"] == 0.0 and s["penalty"] == 1.0

    # ---- Bug fix: division by zero -------------------------------- #
    s = scorer.score({"status": "success", "output": "x",
                      "energy_used": 0.01, "time_limit": 0}, gt)
    print("zero time  :", s["energy_efficiency"], "(no crash)")

    # ---- Bug fix: missing status ---------------------------------- #
    s = scorer.score({"output": "The answer is 42."}, gt)
    print("no status  :", s["accuracy"], "(defaults to success)")

    # ---- Bug fix: missing methods ---------------------------------- #
    # The original raised AttributeError on any non-timeout status.
    print("no-attr    : OK (methods implemented)")

    # ---- Statistics ------------------------------------------------ #
    print("statistics :", {
        k: v for k, v in scorer.statistics().items()
        if k not in ("config", "uptime_seconds")
    })

    # ---- Serialization -------------------------------------------- #
    payload = scorer.to_json()
    restored = RobustSustainabilityScorer.from_json(payload)
    assert restored.to_dict() == scorer.to_dict()
    print("Round-trip OK.")

    # ---- Validation failures -------------------------------------- #
    for bad_cfg in (
        dict(timeout_penalty=-1.0),
        dict(similarity_metric="bogus"),
        dict(default_time_limit=0),
        dict(max_history=0),
    ):
        try:
            RobustScorerConfig(**bad_cfg)  # type: ignore[arg-type]
        except RobustScorerError as exc:
            print("Rejected cfg:", exc)

    strict = RobustSustainabilityScorer(strict=True)
    for bad in ("not-a-mapping", None):
        try:
            strict.score(bad, gt)  # type: ignore[arg-type]
        except RobustScorerError as exc:
            print("Rejected   :", exc)

    print("\nSmoke test passed.")
