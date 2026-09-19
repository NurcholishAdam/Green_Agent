# src/memory/feedback_loop_guard.py

"""
Feedback Loop Guard
===================

Implements "a recalled recommendation is evidence, not ground truth; always
compare it to current telemetry and policy."

Enhancements
------------
- ``FeedbackLoopGuardConfig`` — frozen, validated: staleness, drift
  tolerances, escalation thresholds, telemetry keys, trusted truth-level
  vocabulary, minimum measured-evidence count, container-tag expectation,
  drift statistic, truncation flags. Full serialization symmetry plus
  ``with_overrides``/``merge``.
- ``GuardVerdict`` — truly frozen: reasons/reason-codes as tuples,
  per-metric drift as ``MappingProxyType``, hashable, with
  ``to_dict``/``from_dict``/``to_json``/``from_json`` and
  ``to_episode_payload()`` for persistence via ``EpisodicMemory`` or
  ``SupermemoryAdapter``.
- Detects stale recalls (with explicit unknown-freshness handling),
  policy drift, telemetry drift, untrusted-evidence shortfalls,
  truncated/clamped evidence, and container-tag mismatches.
- Escalates to human approval separately from blocking
  (``block_on_policy_drift`` vs ``escalate_on_policy_drift``).
- Structured error hierarchy: ``FeedbackLoopGuardError`` →
  ``FeedbackLoopGuardInputError``, ``FeedbackLoopGuardConfigError``.
- Async sibling (``check_async``) and batch helper (``check_many``).
- Observability: per-reason counters, per-check latency ring with
  p50/p95/max, ``last_verdict``, ``last_error``.
- ``reset()`` returns an int; ``close()`` + context-manager support.
- ``__version__`` exported via ``__all__``.
- ``__main__`` smoke test coexisting with the enhanced ``RecallBundle``
  validation (memories/scores/citations lengths aligned).
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import threading
import time
from collections import deque
from collections.abc import Mapping as ABCMapping
from dataclasses import dataclass, field, fields, replace
from datetime import datetime, timezone
from types import MappingProxyType
from typing import (
    Any,
    Deque,
    Dict,
    Iterable,
    List,
    Literal,
    Mapping,
    Optional,
    Sequence,
    Tuple,
)

from .bounded_recall import RecallBundle
from .memory_schemas import PolicyRecord

logger = logging.getLogger(__name__)

__version__ = "6.0.0"


# --------------------------------------------------------------------------- #
# Reason codes (stable identifiers; human strings live alongside)
# --------------------------------------------------------------------------- #
_REASON_INSUFFICIENT_EVIDENCE = "insufficient_evidence"
_REASON_EXCESS_EVIDENCE = "excess_evidence"
_REASON_UNTRUSTED_EVIDENCE = "untrusted_evidence"
_REASON_STALE_EVIDENCE = "stale_evidence"
_REASON_UNKNOWN_STALENESS = "unknown_staleness"
_REASON_POLICY_DRIFT = "policy_drift"
_REASON_TELEMETRY_DRIFT = "telemetry_drift"
_REASON_TRUNCATED_EVIDENCE = "truncated_evidence"
_REASON_CLAMPED_K = "clamped_k"
_REASON_CONTAINER_MISMATCH = "container_tag_mismatch"

_DRIFT_STATISTICS: Tuple[str, ...] = ("median", "mean", "p95", "max")


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class FeedbackLoopGuardError(ValueError):
    """Base class for feedback-loop guard problems."""


class FeedbackLoopGuardInputError(FeedbackLoopGuardError):
    """Invalid input to ``check``/``check_many``."""


class FeedbackLoopGuardConfigError(FeedbackLoopGuardError):
    """Invalid configuration."""


# --------------------------------------------------------------------------- #
# Validation helpers — mirror the other modules
# --------------------------------------------------------------------------- #
def _is_real_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _is_finite_nonneg(value: Any) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
        and value >= 0
    )


def _is_positive_finite(value: Any) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
        and value > 0
    )


def _parse_iso_datetime(value: Any) -> Optional[datetime]:
    """Parse ISO 8601 timestamps; tolerate ``Z`` and numeric epochs."""
    if value is None:
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        ts = float(value)
        if ts > 1e12:
            ts /= 1000.0
        try:
            return datetime.fromtimestamp(ts, tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None
    if not isinstance(value, str):
        return None
    s = value.strip()
    if not s:
        return None
    if s.endswith("Z") or s.endswith("z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _normalize_string_tuple(
    value: Any,
    *,
    name: str,
    allow_empty: bool = False,
) -> Tuple[str, ...]:
    if isinstance(value, str) or not isinstance(
        value, (tuple, list, set, frozenset)
    ):
        raise FeedbackLoopGuardConfigError(
            f"{name} must be a sequence of strings."
        )
    out: List[str] = []
    seen: set = set()
    for item in value:
        if not isinstance(item, str) or not item:
            raise FeedbackLoopGuardConfigError(
                f"{name} entries must be non-empty strings."
            )
        if item in seen:
            raise FeedbackLoopGuardConfigError(
                f"{name} contains duplicate {item!r}."
            )
        seen.add(item)
        out.append(item)
    if not out and not allow_empty:
        raise FeedbackLoopGuardConfigError(f"{name} must be non-empty.")
    return tuple(out)


# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class FeedbackLoopGuardConfig:
    """Tunable parameters for the feedback-loop guard."""

    # --- Staleness --------------------------------------------------- #
    staleness_seconds: float = 24 * 3600.0
    staleness_timestamp_key: str = "observed_at"
    treat_missing_observed_at_as_stale: bool = True

    # --- Policy drift ------------------------------------------------ #
    block_on_policy_drift: bool = True
    escalate_on_policy_drift: bool = True

    # --- Telemetry drift --------------------------------------------- #
    telemetry_drift_tolerance: float = 0.25
    human_escalation_threshold: float = 0.5
    telemetry_keys: Tuple[str, ...] = (
        "energy_wh", "carbon_gco2e", "latency_ms",
    )
    drift_statistic: Literal["median", "mean", "p95", "max"] = "median"

    # --- Evidence volume & trust ------------------------------------- #
    min_evidence: int = 1
    max_evidence: int = 50
    trim_excess_evidence: bool = False
    trusted_truth_levels: Tuple[str, ...] = ("measured",)
    min_measured_evidence: int = 0

    # --- Evidence quality flags -------------------------------------- #
    flag_truncated_evidence: bool = True
    flag_clamped_k: bool = True

    # --- Container alignment ----------------------------------------- #
    expected_container_tag: Optional[str] = None

    # ------------------------------------------------------------------ #
    def __post_init__(self) -> None:
        # staleness
        if not _is_positive_finite(self.staleness_seconds):
            raise FeedbackLoopGuardConfigError(
                "staleness_seconds must be a finite number > 0."
            )
        if (
            not isinstance(self.staleness_timestamp_key, str)
            or not self.staleness_timestamp_key
        ):
            raise FeedbackLoopGuardConfigError(
                "staleness_timestamp_key must be a non-empty string."
            )

        # numeric tolerances
        if not _is_finite_nonneg(self.telemetry_drift_tolerance):
            raise FeedbackLoopGuardConfigError(
                "telemetry_drift_tolerance must be finite and >= 0."
            )
        if not (0.0 < self.telemetry_drift_tolerance < 1.0):
            raise FeedbackLoopGuardConfigError(
                "telemetry_drift_tolerance must be in (0, 1)."
            )
        if not _is_finite_nonneg(self.human_escalation_threshold):
            raise FeedbackLoopGuardConfigError(
                "human_escalation_threshold must be finite and >= 0."
            )
        if not (0.0 <= self.human_escalation_threshold <= 1.0):
            raise FeedbackLoopGuardConfigError(
                "human_escalation_threshold must be in [0, 1]."
            )
        if self.human_escalation_threshold < self.telemetry_drift_tolerance:
            raise FeedbackLoopGuardConfigError(
                "human_escalation_threshold must be >= "
                "telemetry_drift_tolerance."
            )

        # evidence counts
        for name in (
            "min_evidence", "max_evidence", "min_measured_evidence",
        ):
            v = getattr(self, name)
            if not _is_real_int(v) or v < 0:
                raise FeedbackLoopGuardConfigError(
                    f"{name} must be a non-negative int (got {v!r})."
                )
        if self.min_evidence <= 0:
            raise FeedbackLoopGuardConfigError(
                "min_evidence must be > 0."
            )
        if self.max_evidence < self.min_evidence:
            raise FeedbackLoopGuardConfigError(
                "max_evidence must be >= min_evidence."
            )
        if self.min_measured_evidence > self.max_evidence:
            raise FeedbackLoopGuardConfigError(
                "min_measured_evidence must be <= max_evidence."
            )

        # boolean flags
        for name in (
            "block_on_policy_drift",
            "escalate_on_policy_drift",
            "trim_excess_evidence",
            "treat_missing_observed_at_as_stale",
            "flag_truncated_evidence",
            "flag_clamped_k",
        ):
            if not isinstance(getattr(self, name), bool):
                raise FeedbackLoopGuardConfigError(
                    f"{name} must be a bool."
                )

        # telemetry keys
        object.__setattr__(
            self,
            "telemetry_keys",
            _normalize_string_tuple(
                self.telemetry_keys, name="telemetry_keys",
            ),
        )

        # trusted truth levels
        object.__setattr__(
            self,
            "trusted_truth_levels",
            _normalize_string_tuple(
                self.trusted_truth_levels, name="trusted_truth_levels",
            ),
        )

        # drift statistic
        if self.drift_statistic not in _DRIFT_STATISTICS:
            raise FeedbackLoopGuardConfigError(
                f"drift_statistic must be one of {_DRIFT_STATISTICS}, "
                f"got {self.drift_statistic!r}."
            )

        # expected container tag
        if self.expected_container_tag is not None:
            if (
                not isinstance(self.expected_container_tag, str)
                or not self.expected_container_tag
            ):
                raise FeedbackLoopGuardConfigError(
                    "expected_container_tag must be None or a non-empty "
                    "string."
                )

    # ------------------------------------------------------------------ #
    def to_dict(self) -> Dict[str, Any]:
        return {
            "staleness_seconds": self.staleness_seconds,
            "staleness_timestamp_key": self.staleness_timestamp_key,
            "treat_missing_observed_at_as_stale":
                self.treat_missing_observed_at_as_stale,
            "block_on_policy_drift": self.block_on_policy_drift,
            "escalate_on_policy_drift": self.escalate_on_policy_drift,
            "telemetry_drift_tolerance": self.telemetry_drift_tolerance,
            "human_escalation_threshold":
                self.human_escalation_threshold,
            "telemetry_keys": list(self.telemetry_keys),
            "drift_statistic": self.drift_statistic,
            "min_evidence": self.min_evidence,
            "max_evidence": self.max_evidence,
            "trim_excess_evidence": self.trim_excess_evidence,
            "trusted_truth_levels": list(self.trusted_truth_levels),
            "min_measured_evidence": self.min_measured_evidence,
            "flag_truncated_evidence": self.flag_truncated_evidence,
            "flag_clamped_k": self.flag_clamped_k,
            "expected_container_tag": self.expected_container_tag,
        }

    def to_json(self, *, indent: Optional[int] = None) -> str:
        return json.dumps(self.to_dict(), indent=indent, sort_keys=True)

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        strict: bool = False,
    ) -> "FeedbackLoopGuardConfig":
        if not isinstance(data, ABCMapping):
            raise FeedbackLoopGuardConfigError(
                "FeedbackLoopGuardConfig.from_dict expects a Mapping."
            )
        valid = {f.name for f in fields(cls)}
        unknown = set(data) - valid
        if strict and unknown:
            raise FeedbackLoopGuardConfigError(
                f"Unknown config key(s): {sorted(unknown)}."
            )
        kwargs: Dict[str, Any] = {}
        for k, v in data.items():
            if k not in valid:
                continue
            if k in ("telemetry_keys", "trusted_truth_levels"):
                if isinstance(v, str) or not isinstance(
                    v, (list, tuple, set, frozenset)
                ):
                    raise FeedbackLoopGuardConfigError(
                        f"{k} must be a sequence of strings."
                    )
                kwargs[k] = tuple(v)
            else:
                kwargs[k] = v
        try:
            return cls(**kwargs)
        except FeedbackLoopGuardError:
            raise
        except (TypeError, ValueError) as exc:
            raise FeedbackLoopGuardConfigError(
                f"failed to build FeedbackLoopGuardConfig: {exc}"
            ) from exc

    @classmethod
    def from_json(
        cls,
        payload: str,
        *,
        strict: bool = False,
    ) -> "FeedbackLoopGuardConfig":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise FeedbackLoopGuardConfigError(
                f"from_json received invalid JSON: {exc}"
            ) from exc
        if not isinstance(data, ABCMapping):
            raise FeedbackLoopGuardConfigError(
                "from_json expected a JSON object at the top level."
            )
        return cls.from_dict(data, strict=strict)

    # ------------------------------------------------------------------ #
    def with_overrides(self, **kwargs: Any) -> "FeedbackLoopGuardConfig":
        valid = {f.name for f in fields(self)}
        unknown = set(kwargs) - valid
        if unknown:
            raise FeedbackLoopGuardConfigError(
                f"Unknown config field(s): {sorted(unknown)}."
            )
        return replace(self, **kwargs)

    def merge(
        self, other: "FeedbackLoopGuardConfig"
    ) -> "FeedbackLoopGuardConfig":
        """Return a new config where ``other``'s non-defaults win."""
        defaults = FeedbackLoopGuardConfig()
        overrides: Dict[str, Any] = {}
        for f in fields(self):
            other_val = getattr(other, f.name)
            default_val = getattr(defaults, f.name)
            if other_val != default_val:
                overrides[f.name] = other_val
        return self.with_overrides(**overrides)


# --------------------------------------------------------------------------- #
# Verdict
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class GuardVerdict:
    """Immutable result of a feedback-loop check."""

    allowed: bool
    reasons: Tuple[str, ...] = ()
    reason_codes: Tuple[str, ...] = ()
    staleness_seconds: Optional[float] = None
    staleness_known: bool = False
    policy_drift: bool = False
    telemetry_drift: float = 0.0
    telemetry_drift_by_key: Mapping[str, float] = field(
        default_factory=dict,
    )
    requires_human: bool = False
    evidence_count: int = 0
    trusted_evidence_count: int = 0
    untrusted_evidence_count: int = 0
    truncated_evidence: bool = False
    clamped_k: bool = False
    container_tag_mismatch: bool = False
    timestamp: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    # ------------------------------------------------------------------ #
    def __post_init__(self) -> None:
        if not isinstance(self.allowed, bool):
            raise FeedbackLoopGuardInputError("allowed must be a bool.")

        object.__setattr__(self, "reasons", tuple(str(r) for r in self.reasons))
        object.__setattr__(
            self, "reason_codes",
            tuple(str(c) for c in self.reason_codes),
        )

        if self.staleness_seconds is not None:
            if not _is_finite_nonneg(self.staleness_seconds):
                raise FeedbackLoopGuardInputError(
                    "staleness_seconds must be None or a finite >= 0 number."
                )
        if not isinstance(self.staleness_known, bool):
            raise FeedbackLoopGuardInputError("staleness_known must be a bool.")
        if not _is_finite_nonneg(self.telemetry_drift):
            raise FeedbackLoopGuardInputError(
                "telemetry_drift must be a finite number >= 0."
            )

        frozen_drift = MappingProxyType(
            {str(k): float(v) for k, v in dict(self.telemetry_drift_by_key).items()}
        )
        object.__setattr__(self, "telemetry_drift_by_key", frozen_drift)

        for name in (
            "policy_drift", "requires_human", "truncated_evidence",
            "clamped_k", "container_tag_mismatch",
        ):
            if not isinstance(getattr(self, name), bool):
                raise FeedbackLoopGuardInputError(f"{name} must be a bool.")

        for name in (
            "evidence_count", "trusted_evidence_count",
            "untrusted_evidence_count",
        ):
            v = getattr(self, name)
            if not _is_real_int(v) or v < 0:
                raise FeedbackLoopGuardInputError(
                    f"{name} must be a non-negative int."
                )

        if not isinstance(self.timestamp, datetime):
            raise FeedbackLoopGuardInputError("timestamp must be a datetime.")

    # ------------------------------------------------------------------ #
    # Convenience
    # ------------------------------------------------------------------ #
    def is_empty(self) -> bool:
        return self.evidence_count == 0

    def has_reason(self, code: str) -> bool:
        return code in self.reason_codes

    def _summarize(self) -> str:
        status = "ALLOWED" if self.allowed else "BLOCKED"
        parts = [f"Guard verdict: {status}"]
        if self.reason_codes:
            parts.append(f"reasons={','.join(self.reason_codes)}")
        parts.append(f"evidence={self.evidence_count}")
        parts.append(f"trusted={self.trusted_evidence_count}")
        if self.staleness_known and self.staleness_seconds is not None:
            parts.append(f"staleness={self.staleness_seconds:.0f}s")
        parts.append(f"telemetry_drift={self.telemetry_drift:.2%}")
        if self.policy_drift:
            parts.append("policy_drift=true")
        if self.requires_human:
            parts.append("requires_human=true")
        return "; ".join(parts)

    # ------------------------------------------------------------------ #
    # Serialization
    # ------------------------------------------------------------------ #
    def to_dict(self) -> Dict[str, Any]:
        return {
            "allowed": self.allowed,
            "reasons": list(self.reasons),
            "reason_codes": list(self.reason_codes),
            "staleness_seconds": self.staleness_seconds,
            "staleness_known": self.staleness_known,
            "policy_drift": self.policy_drift,
            "telemetry_drift": self.telemetry_drift,
            "telemetry_drift_by_key": dict(self.telemetry_drift_by_key),
            "requires_human": self.requires_human,
            "evidence_count": self.evidence_count,
            "trusted_evidence_count": self.trusted_evidence_count,
            "untrusted_evidence_count": self.untrusted_evidence_count,
            "truncated_evidence": self.truncated_evidence,
            "clamped_k": self.clamped_k,
            "container_tag_mismatch": self.container_tag_mismatch,
            "timestamp": self.timestamp.isoformat(),
        }

    def to_json(self, *, indent: Optional[int] = None) -> str:
        return json.dumps(self.to_dict(), default=str, indent=indent)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "GuardVerdict":
        if not isinstance(data, ABCMapping):
            raise FeedbackLoopGuardInputError(
                "GuardVerdict.from_dict expects a Mapping."
            )
        ts_raw = data.get("timestamp")
        ts = _parse_iso_datetime(ts_raw) if ts_raw else None
        if ts is None:
            ts = datetime.now(timezone.utc)
        return cls(
            allowed=bool(data.get("allowed", False)),
            reasons=tuple(data.get("reasons", ())),
            reason_codes=tuple(data.get("reason_codes", ())),
            staleness_seconds=data.get("staleness_seconds"),
            staleness_known=bool(data.get("staleness_known", False)),
            policy_drift=bool(data.get("policy_drift", False)),
            telemetry_drift=float(data.get("telemetry_drift", 0.0)),
            telemetry_drift_by_key=dict(
                data.get("telemetry_drift_by_key", {})
            ),
            requires_human=bool(data.get("requires_human", False)),
            evidence_count=int(data.get("evidence_count", 0)),
            trusted_evidence_count=int(
                data.get("trusted_evidence_count", 0)
            ),
            untrusted_evidence_count=int(
                data.get("untrusted_evidence_count", 0)
            ),
            truncated_evidence=bool(
                data.get("truncated_evidence", False)
            ),
            clamped_k=bool(data.get("clamped_k", False)),
            container_tag_mismatch=bool(
                data.get("container_tag_mismatch", False)
            ),
            timestamp=ts,
        )

    @classmethod
    def from_json(cls, payload: str) -> "GuardVerdict":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise FeedbackLoopGuardInputError(
                f"GuardVerdict.from_json received invalid JSON: {exc}"
            ) from exc
        if not isinstance(data, ABCMapping):
            raise FeedbackLoopGuardInputError(
                "from_json expected a JSON object at the top level."
            )
        return cls.from_dict(data)

    # ------------------------------------------------------------------ #
    # Persistence bridge
    # ------------------------------------------------------------------ #
    def to_episode_payload(
        self,
        *,
        container_tag: Optional[str] = None,
        content: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Return a payload shaped for ``EpisodicMemory.store`` /
        ``SupermemoryAdapter.remember``.

        The ``content`` field defaults to a short human-readable summary
        of the verdict. ``metadata`` carries the structured fields.
        """
        meta: Dict[str, Any] = {
            "kind": "guard_verdict",
            "observed_at": self.timestamp.isoformat(),
            "allowed": self.allowed,
            "requires_human": self.requires_human,
            "policy_drift": self.policy_drift,
            "telemetry_drift": self.telemetry_drift,
            "telemetry_drift_by_key": dict(self.telemetry_drift_by_key),
            "staleness_seconds": self.staleness_seconds,
            "staleness_known": self.staleness_known,
            "evidence_count": self.evidence_count,
            "trusted_evidence_count": self.trusted_evidence_count,
            "untrusted_evidence_count": self.untrusted_evidence_count,
            "truncated_evidence": self.truncated_evidence,
            "clamped_k": self.clamped_k,
            "container_tag_mismatch": self.container_tag_mismatch,
            "reason_codes": list(self.reason_codes),
            "reason_count": len(self.reason_codes),
        }
        if container_tag is not None:
            if not isinstance(container_tag, str) or not container_tag:
                raise FeedbackLoopGuardInputError(
                    "container_tag must be None or a non-empty string."
                )
            meta["container_tag"] = container_tag
        return {
            "content": content if content is not None else self._summarize(),
            "container_tag": container_tag,
            "metadata": meta,
        }

    # ------------------------------------------------------------------ #
    def __hash__(self) -> int:
        return hash((
            self.allowed,
            self.reasons,
            self.reason_codes,
            self.staleness_seconds,
            self.staleness_known,
            self.policy_drift,
            self.telemetry_drift,
            tuple(sorted(self.telemetry_drift_by_key.items())),
            self.requires_human,
            self.evidence_count,
            self.trusted_evidence_count,
            self.untrusted_evidence_count,
            self.truncated_evidence,
            self.clamped_k,
            self.container_tag_mismatch,
            self.timestamp,
        ))

    def __repr__(self) -> str:
        return (
            "GuardVerdict("
            f"allowed={self.allowed}, "
            f"reasons={len(self.reason_codes)}, "
            f"requires_human={self.requires_human}, "
            f"drift={self.telemetry_drift:.2%}, "
            f"evidence={self.evidence_count})"
        )


# --------------------------------------------------------------------------- #
# Guard
# --------------------------------------------------------------------------- #
class FeedbackLoopGuard:
    """Compares recalled evidence against current telemetry and policy."""

    _LATENCY_RING_SIZE: int = 200

    def __init__(
        self,
        *,
        config: Optional[FeedbackLoopGuardConfig] = None,
        strict: bool = True,
    ) -> None:
        self._config = config or FeedbackLoopGuardConfig()
        self._strict = bool(strict)

        self._lock = threading.RLock()
        self._total_checks = 0
        self._allowed = 0
        self._blocked = 0
        self._escalated = 0
        self._reason_counts: Dict[str, int] = {}
        self._latency_ring: Deque[float] = deque(
            maxlen=self._LATENCY_RING_SIZE
        )
        self._last_verdict: Optional[GuardVerdict] = None
        self._last_error: Optional[str] = None
        self._started_at = time.monotonic()

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> FeedbackLoopGuardConfig:
        return self._config

    @property
    def strict(self) -> bool:
        return self._strict

    # ---------------------------------------------------------- public API
    def check(
        self,
        *,
        recalled: RecallBundle,
        current_telemetry: Mapping[str, float],
        current_policy: Optional[PolicyRecord] = None,
        now: Optional[float] = None,
    ) -> GuardVerdict:
        """Return a ``GuardVerdict`` for the given evidence."""
        # ------------------------------------------------------- validate
        if not isinstance(recalled, RecallBundle):
            raise FeedbackLoopGuardInputError(
                "recalled must be a RecallBundle."
            )
        if not isinstance(current_telemetry, ABCMapping):
            raise FeedbackLoopGuardInputError(
                "current_telemetry must be a Mapping."
            )
        for k, v in current_telemetry.items():
            if isinstance(v, bool) or not isinstance(v, (int, float)):
                raise FeedbackLoopGuardInputError(
                    f"current_telemetry[{k!r}] must be numeric."
                )
            if not math.isfinite(float(v)):
                raise FeedbackLoopGuardInputError(
                    f"current_telemetry[{k!r}] must be finite."
                )
        if current_policy is not None and not isinstance(
            current_policy, PolicyRecord
        ):
            raise FeedbackLoopGuardInputError(
                "current_policy must be a PolicyRecord or None."
            )
        if now is None:
            now = time.time()
        elif not _is_finite_nonneg(now):
            raise FeedbackLoopGuardInputError(
                "now must be a finite non-negative number."
            )

        start = time.perf_counter()

        # ------------------------------------------------------- evidence
        memories = list(recalled.memories)
        evidence_count = len(memories)

        trusted_levels = self._config.trusted_truth_levels
        trusted_count = 0
        for m in memories:
            level = self._truth_level_of(m)
            if level is not None and level in trusted_levels:
                trusted_count += 1
        untrusted_count = evidence_count - trusted_count

        # ------------------------------------------------------- staleness
        staleness, staleness_known = self._staleness_of(memories, now=now)

        # ------------------------------------------------------- policy
        policy_drift = False
        if current_policy is not None:
            current_version = getattr(current_policy, "version", None)
            if isinstance(current_version, str) and current_version:
                for m in memories:
                    v = self._policy_version_of(m)
                    if v is not None and v != current_version:
                        policy_drift = True
                        break

        # ------------------------------------------------------- telemetry
        drift_by_key = self._telemetry_drift_by_key(
            memories, current_telemetry,
        )
        telemetry_drift = (
            max(drift_by_key.values()) if drift_by_key else 0.0
        )

        # ------------------------------------------------------- container
        container_mismatch = False
        expected_tag = self._config.expected_container_tag
        if expected_tag is not None:
            for m in memories:
                tag = self._container_tag_of(m)
                if tag is not None and tag != expected_tag:
                    container_mismatch = True
                    break

        # ------------------------------------------------------- reasons
        reasons: List[str] = []
        codes: List[str] = []

        if evidence_count < self._config.min_evidence:
            reasons.append(
                f"insufficient evidence: {evidence_count} < "
                f"{self._config.min_evidence}"
            )
            codes.append(_REASON_INSUFFICIENT_EVIDENCE)
        elif (
            evidence_count > self._config.max_evidence
            and not self._config.trim_excess_evidence
        ):
            reasons.append(
                f"too much evidence: {evidence_count} > "
                f"{self._config.max_evidence}"
            )
            codes.append(_REASON_EXCESS_EVIDENCE)

        if (
            self._config.min_measured_evidence > 0
            and trusted_count < self._config.min_measured_evidence
        ):
            reasons.append(
                f"insufficient trusted evidence: {trusted_count} < "
                f"{self._config.min_measured_evidence}"
            )
            codes.append(_REASON_UNTRUSTED_EVIDENCE)

        if not staleness_known and evidence_count > 0:
            if self._config.treat_missing_observed_at_as_stale:
                reasons.append(
                    "evidence freshness unknown "
                    "(no parseable timestamp in evidence)"
                )
                codes.append(_REASON_UNKNOWN_STALENESS)
        elif (
            staleness_known
            and staleness is not None
            and staleness > self._config.staleness_seconds
        ):
            reasons.append(
                f"evidence stale: {staleness:.0f}s > "
                f"{self._config.staleness_seconds:.0f}s"
            )
            codes.append(_REASON_STALE_EVIDENCE)

        if policy_drift and self._config.block_on_policy_drift:
            reasons.append("policy drift detected")
            codes.append(_REASON_POLICY_DRIFT)

        if telemetry_drift > self._config.telemetry_drift_tolerance:
            if drift_by_key:
                worst_key = max(
                    drift_by_key.items(), key=lambda kv: kv[1],
                )[0]
                detail = f" (worst: {worst_key})"
            else:
                detail = ""
            reasons.append(
                f"telemetry drift: {telemetry_drift:.2%}{detail} > "
                f"{self._config.telemetry_drift_tolerance:.2%}"
            )
            codes.append(_REASON_TELEMETRY_DRIFT)

        if (
            recalled.truncated
            and self._config.flag_truncated_evidence
        ):
            reasons.append(
                "evidence bundle was truncated by the token budget"
            )
            codes.append(_REASON_TRUNCATED_EVIDENCE)

        if recalled.clamped_k and self._config.flag_clamped_k:
            reasons.append(
                "evidence bundle was clamped by max_k"
            )
            codes.append(_REASON_CLAMPED_K)

        if container_mismatch:
            reasons.append(
                f"container tag mismatch "
                f"(expected {expected_tag!r})"
            )
            codes.append(_REASON_CONTAINER_MISMATCH)

        # ------------------------------------------------------- escalation
        requires_human = (
            telemetry_drift >= self._config.human_escalation_threshold
        )
        if policy_drift and self._config.escalate_on_policy_drift:
            requires_human = True

        allowed = not reasons

        verdict = GuardVerdict(
            allowed=allowed,
            reasons=tuple(reasons),
            reason_codes=tuple(codes),
            staleness_seconds=staleness if staleness_known else None,
            staleness_known=staleness_known,
            policy_drift=policy_drift,
            telemetry_drift=telemetry_drift,
            telemetry_drift_by_key=drift_by_key,
            requires_human=requires_human,
            evidence_count=evidence_count,
            trusted_evidence_count=trusted_count,
            untrusted_evidence_count=untrusted_count,
            truncated_evidence=bool(recalled.truncated),
            clamped_k=bool(recalled.clamped_k),
            container_tag_mismatch=container_mismatch,
        )

        elapsed_ms = (time.perf_counter() - start) * 1000.0
        with self._lock:
            self._total_checks += 1
            if allowed:
                self._allowed += 1
            else:
                self._blocked += 1
            if requires_human:
                self._escalated += 1
            for code in codes:
                self._reason_counts[code] = (
                    self._reason_counts.get(code, 0) + 1
                )
            self._latency_ring.append(elapsed_ms)
            self._last_verdict = verdict
            self._last_error = None

        logger.debug(
            "Guard verdict: allowed=%s codes=%s requires_human=%s",
            verdict.allowed, list(verdict.reason_codes),
            verdict.requires_human,
        )
        return verdict

    async def check_async(
        self,
        *,
        recalled: RecallBundle,
        current_telemetry: Mapping[str, float],
        current_policy: Optional[PolicyRecord] = None,
        now: Optional[float] = None,
    ) -> GuardVerdict:
        return await asyncio.to_thread(
            self.check,
            recalled=recalled,
            current_telemetry=current_telemetry,
            current_policy=current_policy,
            now=now,
        )

    def check_many(
        self,
        items: Iterable[Mapping[str, Any]],
        *,
        stop_on_error: bool = False,
    ) -> List[GuardVerdict]:
        """Run ``check()`` for each item.

        Each item must be a Mapping with keys:

        - ``recalled`` (required)
        - ``current_telemetry`` (required)
        - ``current_policy`` (optional)
        - ``now`` (optional)
        """
        results: List[GuardVerdict] = []
        for idx, item in enumerate(items):
            if not isinstance(item, ABCMapping):
                if stop_on_error:
                    raise FeedbackLoopGuardInputError(
                        f"check_many[{idx}] must be a Mapping."
                    )
                logger.warning(
                    "check_many[%d] is not a Mapping; skipping.", idx,
                )
                continue
            try:
                recalled = item["recalled"]
                telemetry = item["current_telemetry"]
            except KeyError as exc:
                if stop_on_error:
                    raise FeedbackLoopGuardInputError(
                        f"check_many[{idx}] missing key {exc.args[0]!r}."
                    ) from exc
                logger.warning(
                    "check_many[%d] missing key %r; skipping.",
                    idx, exc.args[0],
                )
                continue
            try:
                verdict = self.check(
                    recalled=recalled,
                    current_telemetry=telemetry,
                    current_policy=item.get("current_policy"),
                    now=item.get("now"),
                )
            except FeedbackLoopGuardError as exc:
                if stop_on_error:
                    raise
                logger.warning("check_many[%d] failed: %s", idx, exc)
                with self._lock:
                    self._last_error = f"check_many[{idx}]: {exc}"
                verdict = GuardVerdict(
                    allowed=False,
                    reasons=(f"check failed: {exc}",),
                    reason_codes=("check_failed",),
                )
            results.append(verdict)
        return results

    # ---------------------------------------------------------- internals
    def _truth_level_of(self, memory: Mapping[str, Any]) -> Optional[str]:
        meta = memory.get("metadata")
        if isinstance(meta, ABCMapping):
            v = meta.get("truth_level")
            if isinstance(v, str) and v:
                return v
        v = memory.get("truth_level")
        return v if isinstance(v, str) and v else None

    def _policy_version_of(
        self, memory: Mapping[str, Any]
    ) -> Optional[str]:
        meta = memory.get("metadata")
        if isinstance(meta, ABCMapping):
            v = meta.get("policy_version")
            if isinstance(v, str) and v:
                return v
        v = memory.get("policy_version")
        return v if isinstance(v, str) and v else None

    def _container_tag_of(
        self, memory: Mapping[str, Any]
    ) -> Optional[str]:
        meta = memory.get("metadata")
        if isinstance(meta, ABCMapping):
            v = meta.get("container_tag")
            if isinstance(v, str) and v:
                return v
        v = memory.get("container_tag")
        return v if isinstance(v, str) and v else None

    def _staleness_of(
        self,
        memories: Sequence[Mapping[str, Any]],
        *,
        now: float,
    ) -> Tuple[Optional[float], bool]:
        """Return ``(newest_age_seconds, known)``.

        ``known`` is False when the evidence contains memories but none
        of them carry a parseable timestamp under the configured key.
        """
        if not memories:
            return None, False
        key = self._config.staleness_timestamp_key
        ages: List[float] = []
        for memory in memories:
            meta = memory.get("metadata")
            raw: Any = None
            if isinstance(meta, ABCMapping):
                raw = meta.get(key)
            if raw is None:
                raw = memory.get(key)
            dt = _parse_iso_datetime(raw)
            if dt is None:
                continue
            age = max(0.0, now - dt.timestamp())
            ages.append(age)
        if not ages:
            return None, False
        return min(ages), True

    def _compute_statistic(self, values: Sequence[float]) -> float:
        if not values:
            return 0.0
        ordered = sorted(values)
        n = len(ordered)
        stat = self._config.drift_statistic
        if stat == "median":
            mid = n // 2
            if n % 2 == 0:
                return (ordered[mid - 1] + ordered[mid]) / 2.0
            return ordered[mid]
        if stat == "mean":
            return sum(ordered) / n
        if stat == "p95":
            idx = max(0, min(n - 1, int(round(0.95 * (n - 1)))))
            return ordered[idx]
        if stat == "max":
            return ordered[-1]
        return ordered[n // 2]

    def _telemetry_drift_by_key(
        self,
        memories: Sequence[Mapping[str, Any]],
        current: Mapping[str, float],
    ) -> Dict[str, float]:
        drift: Dict[str, float] = {}
        if not memories or not current:
            return drift
        for key in self._config.telemetry_keys:
            if key not in current:
                continue
            values: List[float] = []
            for m in memories:
                meta = m.get("metadata")
                raw: Any = None
                if isinstance(meta, ABCMapping):
                    raw = meta.get(key)
                if raw is None:
                    raw = m.get(key)
                if (
                    isinstance(raw, (int, float))
                    and not isinstance(raw, bool)
                    and math.isfinite(float(raw))
                ):
                    values.append(float(raw))
            if not values:
                continue
            stat = self._compute_statistic(values)
            cur = float(current[key])
            denom = abs(stat) if abs(stat) > 1e-9 else 1.0
            drift[key] = abs(cur - stat) / denom
        return drift

    def _percentile(self, values: Sequence[float], pct: float) -> float:
        if not values:
            return 0.0
        ordered = sorted(values)
        k = max(
            0,
            min(
                len(ordered) - 1,
                int(round((pct / 100.0) * (len(ordered) - 1))),
            ),
        )
        return ordered[k]

    # ---------------------------------------------------------- statistics
    def statistics(self) -> Dict[str, Any]:
        with self._lock:
            lats = list(self._latency_ring)
            mean_latency = (sum(lats) / len(lats)) if lats else 0.0
            return {
                "checks": self._total_checks,
                "allowed": self._allowed,
                "blocked": self._blocked,
                "escalated": self._escalated,
                "reason_counts": dict(self._reason_counts),
                "mean_latency_ms": mean_latency,
                "p50_latency_ms": self._percentile(lats, 50),
                "p95_latency_ms": self._percentile(lats, 95),
                "max_latency_ms": max(lats) if lats else 0.0,
                "last_verdict": (
                    self._last_verdict.to_dict()
                    if self._last_verdict is not None else None
                ),
                "last_error": self._last_error,
                "config": self._config.to_dict(),
                "strict": self._strict,
                "uptime_seconds": time.monotonic() - self._started_at,
            }

    def reset(self) -> int:
        """Reset counters. Returns the number of checks cleared."""
        with self._lock:
            cleared = self._total_checks
            self._total_checks = 0
            self._allowed = 0
            self._blocked = 0
            self._escalated = 0
            self._reason_counts = {}
            self._latency_ring.clear()
            self._last_verdict = None
            self._last_error = None
            self._started_at = time.monotonic()
        return cleared

    # ---------------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        return {
            "config": self._config.to_dict(),
            "strict": self._strict,
            "statistics": self.statistics(),
        }

    def to_json(self, *, indent: Optional[int] = None) -> str:
        return json.dumps(
            self.to_dict(), default=str, indent=indent, sort_keys=True,
        )

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        strict: Optional[bool] = None,
    ) -> "FeedbackLoopGuard":
        if not isinstance(data, ABCMapping):
            raise FeedbackLoopGuardInputError(
                "FeedbackLoopGuard.from_dict expects a Mapping."
            )
        cfg_blob = data.get("config", {})
        config = (
            cfg_blob
            if isinstance(cfg_blob, FeedbackLoopGuardConfig)
            else FeedbackLoopGuardConfig.from_dict(cfg_blob)
        )
        resolved_strict = (
            bool(data.get("strict", True))
            if strict is None
            else bool(strict)
        )
        return cls(config=config, strict=resolved_strict)

    @classmethod
    def from_json(
        cls,
        payload: str,
        *,
        strict: Optional[bool] = None,
    ) -> "FeedbackLoopGuard":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise FeedbackLoopGuardInputError(
                f"from_json received invalid JSON: {exc}"
            ) from exc
        if not isinstance(data, ABCMapping):
            raise FeedbackLoopGuardInputError(
                "from_json expected a JSON object at the top level."
            )
        return cls.from_dict(data, strict=strict)

    # ---------------------------------------------------------- lifecycle
    def close(self) -> None:
        """Best-effort no-op. Kept for symmetry with sibling classes."""
        return None

    def __enter__(self) -> "FeedbackLoopGuard":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def __repr__(self) -> str:
        with self._lock:
            return (
                "FeedbackLoopGuard("
                f"checks={self._total_checks}, "
                f"blocked={self._blocked}, "
                f"escalated={self._escalated}, "
                f"strict={self._strict})"
            )


__all__ = [
    "FeedbackLoopGuard",
    "FeedbackLoopGuardConfig",
    "FeedbackLoopGuardError",
    "FeedbackLoopGuardInputError",
    "FeedbackLoopGuardConfigError",
    "GuardVerdict",
    "__version__",
]


# --------------------------------------------------------------------------- #
# Smoke test: python -m memory.feedback_loop_guard
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    from .bounded_recall import RecallBundle

    guard = FeedbackLoopGuard()
    print("repr         :", guard)

    # --------------------------------------------------- helpers
    def _memory(
        i: int,
        *,
        observed_at: Optional[str] = None,
        policy_version: str = "v0.3",
        truth_level: str = "measured",
        container_tag: str = "org:green-agent",
    ) -> Dict[str, Any]:
        meta: Dict[str, Any] = {
            "run_id": f"run-{i:03d}",
            "policy_version": policy_version,
            "truth_level": truth_level,
            "container_tag": container_tag,
            "energy_wh": 8.6,
            "carbon_gco2e": 4.0,
            "latency_ms": 438.0,
        }
        if observed_at is not None:
            meta["observed_at"] = observed_at
        return {"id": f"m-{i}", "content": f"run {i}", "metadata": meta}

    def _bundle(
        n: int,
        *,
        observed_at: Optional[str] = None,
        policy_version: str = "v0.3",
        truth_level: str = "measured",
        container_tag: str = "org:green-agent",
        truncated: bool = False,
        clamped_k: bool = False,
    ) -> RecallBundle:
        memories = [
            _memory(
                i,
                observed_at=observed_at,
                policy_version=policy_version,
                truth_level=truth_level,
                container_tag=container_tag,
            )
            for i in range(n)
        ]
        citations = tuple(m["metadata"]["run_id"] for m in memories)
        return RecallBundle(
            query="vision_inference arm_edge",
            memories=tuple(memories),
            scores=tuple(0.9 - 0.05 * i for i in range(n)),
            citations=citations,
            token_cost=100 * n,
            latency_ms=12.0,
            truncated=truncated,
            clamped_k=clamped_k,
        )

    now_iso = datetime.now(timezone.utc).isoformat()

    current_policy = PolicyRecord(
        version="v0.3",
        content={"max_carbon_gco2e": 200.0},
    )
    newer_policy = PolicyRecord(
        version="v0.4",
        content={"max_carbon_gco2e": 150.0},
    )

    # --------------------------------------------------- 1. aligned
    v1 = guard.check(
        recalled=_bundle(3, observed_at=now_iso),
        current_telemetry={
            "energy_wh": 8.6, "carbon_gco2e": 4.0, "latency_ms": 438.0,
        },
        current_policy=current_policy,
    )
    print("aligned      :", v1.allowed, list(v1.reason_codes))
    assert v1.allowed is True
    assert v1.trusted_evidence_count == 3

    # --------------------------------------------------- 2. telemetry drift
    v2 = guard.check(
        recalled=_bundle(3, observed_at=now_iso),
        current_telemetry={
            "energy_wh": 20.0, "carbon_gco2e": 12.0, "latency_ms": 900.0,
        },
        current_policy=current_policy,
    )
    print(
        "drifted      :", v2.allowed,
        f"drift={v2.telemetry_drift:.2%}",
        f"human={v2.requires_human}",
        list(v2.reason_codes),
    )
    assert v2.allowed is False
    assert v2.requires_human is True
    assert "telemetry_drift" in v2.reason_codes
    assert "carbon_gco2e" in v2.telemetry_drift_by_key

    # --------------------------------------------------- 3. policy drift
    v3 = guard.check(
        recalled=_bundle(3, observed_at=now_iso),
        current_telemetry={"energy_wh": 8.6},
        current_policy=newer_policy,
    )
    print("pol drift    :", v3.policy_drift, list(v3.reason_codes))
    assert v3.policy_drift is True
    assert "policy_drift" in v3.reason_codes
    assert v3.allowed is False
    assert v3.requires_human is True

    # --------------------------------------------------- 4. stale
    v4 = guard.check(
        recalled=_bundle(2, observed_at="2020-01-01T00:00:00+00:00"),
        current_telemetry={"energy_wh": 8.6},
        current_policy=current_policy,
    )
    print("stale        :", v4.allowed, list(v4.reason_codes))
    assert "stale_evidence" in v4.reason_codes

    # --------------------------------------------------- 5. unknown staleness
    v5 = guard.check(
        recalled=_bundle(2, observed_at=None),  # no observed_at
        current_telemetry={"energy_wh": 8.6},
        current_policy=current_policy,
    )
    print("unknown      :", v5.allowed, list(v5.reason_codes))
    assert "unknown_staleness" in v5.reason_codes

    # --------------------------------------------------- 6. truncated
    v6 = guard.check(
        recalled=_bundle(3, observed_at=now_iso, truncated=True),
        current_telemetry={"energy_wh": 8.6},
        current_policy=current_policy,
    )
    assert "truncated_evidence" in v6.reason_codes
    print("truncated    :", list(v6.reason_codes))

    # --------------------------------------------------- 7. clamped k
    v7 = guard.check(
        recalled=_bundle(3, observed_at=now_iso, clamped_k=True),
        current_telemetry={"energy_wh": 8.6},
        current_policy=current_policy,
    )
    assert "clamped_k" in v7.reason_codes
    print("clamped k    :", list(v7.reason_codes))

    # --------------------------------------------------- 8. untrusted evidence
    strict_truth = FeedbackLoopGuard(
        config=FeedbackLoopGuardConfig(min_measured_evidence=2),
    )
    v8 = strict_truth.check(
        recalled=_bundle(3, observed_at=now_iso, truth_level="simulated"),
        current_telemetry={"energy_wh": 8.6},
        current_policy=current_policy,
    )
    assert "untrusted_evidence" in v8.reason_codes
    print(
        "untrusted    :", list(v8.reason_codes),
        f"trusted={v8.trusted_evidence_count}",
    )

    # --------------------------------------------------- 9. container mismatch
    tagged_guard = FeedbackLoopGuard(
        config=FeedbackLoopGuardConfig(
            expected_container_tag="org:green-agent",
        ),
    )
    v9 = tagged_guard.check(
        recalled=_bundle(
            3, observed_at=now_iso, container_tag="org:other",
        ),
        current_telemetry={"energy_wh": 8.6},
        current_policy=current_policy,
    )
    assert v9.container_tag_mismatch is True
    assert "container_tag_mismatch" in v9.reason_codes
    print("container    :", list(v9.reason_codes))

    # --------------------------------------------------- 10. async
    async def _run_async():
        return await guard.check_async(
            recalled=_bundle(3, observed_at=now_iso),
            current_telemetry={"energy_wh": 8.6},
            current_policy=current_policy,
        )

    v10 = asyncio.run(_run_async())
    print("async        :", v10.allowed)

    # --------------------------------------------------- 11. check_many
    many = guard.check_many([
        {
            "recalled": _bundle(3, observed_at=now_iso),
            "current_telemetry": {"energy_wh": 8.6},
            "current_policy": current_policy,
        },
        {
            "recalled": _bundle(3, observed_at=now_iso),
            "current_telemetry": {"energy_wh": 100.0},
            "current_policy": current_policy,
        },
    ])
    print("check_many   :", [v.allowed for v in many])
    assert [v.allowed for v in many] == [True, False]

    # --------------------------------------------------- 12. persistence bridge
    payload = v1.to_episode_payload(container_tag="org:green-agent")
    assert "content" in payload and "metadata" in payload
    assert payload["metadata"]["kind"] == "guard_verdict"
    print("ep payload   :", payload["content"][:60], "...")

    # --------------------------------------------------- 13. serialization
    cfg = FeedbackLoopGuardConfig()
    assert FeedbackLoopGuardConfig.from_dict(cfg.to_dict()) == cfg
    assert FeedbackLoopGuardConfig.from_json(cfg.to_json()) == cfg
    assert (
        FeedbackLoopGuardConfig().with_overrides(min_evidence=3).min_evidence
        == 3
    )
    print("cfg RT       : OK")

    v_dict = v1.to_dict()
    v_restored = GuardVerdict.from_dict(v_dict)
    assert v_restored.allowed == v1.allowed
    assert v_restored.reason_codes == v1.reason_codes
    assert dict(v_restored.telemetry_drift_by_key) == dict(
        v1.telemetry_drift_by_key
    )
    hash(v1)
    print("verdict RT   : OK (hashable)")

    guard_dict = guard.to_dict()
    guard2 = FeedbackLoopGuard.from_dict(guard_dict)
    assert guard2.config == guard.config
    print("guard RT     : OK")

    # --------------------------------------------------- 14. stats / reset
    stats = guard.statistics()
    print("statistics   :", {
        k: v for k, v in stats.items()
        if k not in ("config", "last_verdict")
    })
    assert stats["checks"] >= 10
    cleared = guard.reset()
    assert cleared >= 10 and guard.statistics()["checks"] == 0
    print("reset        :", cleared, "checks cleared")

    # --------------------------------------------------- 15. config validation
    bad_configs = [
        dict(staleness_seconds=0),
        dict(staleness_seconds=float("nan")),
        dict(staleness_seconds=float("inf")),
        dict(telemetry_drift_tolerance=0.0),
        dict(telemetry_drift_tolerance=1.0),
        dict(telemetry_drift_tolerance=float("nan")),
        dict(human_escalation_threshold=-0.1),
        dict(human_escalation_threshold=1.5),
        dict(human_escalation_threshold=0.1,
             telemetry_drift_tolerance=0.5),  # escalation < tolerance
        dict(min_evidence=0),
        dict(min_evidence=True),
        dict(max_evidence=0, min_evidence=1),
        dict(min_measured_evidence=100, max_evidence=5),
        dict(telemetry_keys="energy_wh"),
        dict(telemetry_keys=()),
        dict(telemetry_keys=("a", "a")),
        dict(trusted_truth_levels=()),
        dict(trusted_truth_levels=("measured", "measured")),
        dict(drift_statistic="bogus"),
        dict(block_on_policy_drift="yes"),
        dict(expected_container_tag=""),
    ]
    for bad in bad_configs:
        try:
            FeedbackLoopGuardConfig(**bad)
        except FeedbackLoopGuardError as exc:
            print(f"reject cfg   : {list(bad)[0]} -> {exc}")
        else:
            raise AssertionError(f"expected rejection for {bad!r}")

    # --------------------------------------------------- 16. input validation
    for bad in (
        lambda: guard.check(
            recalled="not-a-bundle",  # type: ignore[arg-type]
            current_telemetry={},
        ),
        lambda: guard.check(
            recalled=_bundle(1, observed_at=now_iso),
            current_telemetry="nope",  # type: ignore[arg-type]
        ),
        lambda: guard.check(
            recalled=_bundle(1, observed_at=now_iso),
            current_telemetry={"x": float("nan")},
        ),
        lambda: guard.check(
            recalled=_bundle(1, observed_at=now_iso),
            current_telemetry={"x": True},
        ),
        lambda: guard.check(
            recalled=_bundle(1, observed_at=now_iso),
            current_telemetry={},
            current_policy="not-a-policy",  # type: ignore[arg-type]
        ),
        lambda: guard.check(
            recalled=_bundle(1, observed_at=now_iso),
            current_telemetry={},
            now=float("nan"),
        ),
    ):
        try:
            bad()
        except FeedbackLoopGuardError as exc:
            print("reject input :", exc)

    # --------------------------------------------------- 17. context manager
    with FeedbackLoopGuard() as scoped:
        scoped.check(
            recalled=_bundle(2, observed_at=now_iso),
            current_telemetry={"energy_wh": 8.6},
            current_policy=current_policy,
        )
    print("ctx mgr      : OK")

    print("\nSmoke test passed.")
