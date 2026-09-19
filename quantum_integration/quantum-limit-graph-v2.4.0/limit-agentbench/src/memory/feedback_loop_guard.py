# src/memory/feedback_loop_guard.py

"""
Feedback Loop Guard
===================

Implements "a recalled recommendation is evidence, not ground truth; always
compare it to current telemetry and policy."

Enhancements
------------
- ``FeedbackLoopGuardConfig`` — frozen, validated: staleness, drift
  tolerances, escalation thresholds.
- ``GuardVerdict`` — frozen, serializable.
- Detects stale recalls, policy drift, and telemetry drift.
- Escalates to human approval when drift exceeds tolerance.
- Custom ``FeedbackLoopGuardError(ValueError)``.
- ``__main__`` smoke test with synthetic recall and telemetry.
"""

from __future__ import annotations

import json
import logging
import math
import threading
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Mapping, Optional, Tuple

from .bounded_recall import RecallBundle
from .memory_schemas import PolicyRecord

logger = logging.getLogger(__name__)

__version__ = "5.0.0"


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class FeedbackLoopGuardError(ValueError):
    """Raised for invalid guard inputs or configuration."""


# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class FeedbackLoopGuardConfig:
    """Tunable parameters for the feedback-loop guard."""

    # Staleness: reject evidence older than this (seconds).
    staleness_seconds: float = 24 * 3600.0

    # Policy drift: block if a recalled policy version differs from current.
    block_on_policy_drift: bool = True
    # Telemetry drift: relative tolerance for median vs current reading.
    telemetry_drift_tolerance: float = 0.25  # 25%
    # Human escalation: trigger if drift exceeds this fraction.
    human_escalation_threshold: float = 0.5

    # Minimum recall size to be considered evidence.
    min_evidence: int = 1
    max_evidence: int = 50

    def __post_init__(self) -> None:
        if self.staleness_seconds <= 0:
            raise FeedbackLoopGuardError("staleness_seconds must be > 0.")
        if not 0.0 < self.telemetry_drift_tolerance < 1.0:
            raise FeedbackLoopGuardError(
                "telemetry_drift_tolerance must be in (0, 1)."
            )
        if not 0.0 <= self.human_escalation_threshold <= 1.0:
            raise FeedbackLoopGuardError(
                "human_escalation_threshold must be in [0, 1]."
            )
        if self.min_evidence <= 0:
            raise FeedbackLoopGuardError("min_evidence must be > 0.")
        if self.max_evidence < self.min_evidence:
            raise FeedbackLoopGuardError(
                "max_evidence must be >= min_evidence."
            )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# --------------------------------------------------------------------------- #
# Verdict
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class GuardVerdict:
    """Immutable result of a feedback-loop check."""

    allowed: bool
    reasons: Tuple[str, ...]
    staleness_seconds: float
    policy_drift: bool
    telemetry_drift: float
    requires_human: bool
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "allowed": self.allowed,
            "reasons": list(self.reasons),
            "staleness_seconds": self.staleness_seconds,
            "policy_drift": self.policy_drift,
            "telemetry_drift": self.telemetry_drift,
            "requires_human": self.requires_human,
            "timestamp": self.timestamp.isoformat(),
        }


# --------------------------------------------------------------------------- #
# Guard
# --------------------------------------------------------------------------- #
class FeedbackLoopGuard:
    """Compares recalled evidence against current telemetry and policy."""

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
        self._started_at = time.time()

    @property
    def config(self) -> FeedbackLoopGuardConfig:
        return self._config

    # ---------------------------------------------------------- public API
    def check(
        self,
        *,
        recalled: RecallBundle,
        current_telemetry: Mapping[str, float],
        current_policy: Optional[PolicyRecord] = None,
    ) -> GuardVerdict:
        """Return a ``GuardVerdict``."""
        if not isinstance(recalled, RecallBundle):
            raise FeedbackLoopGuardError("recalled must be a RecallBundle.")
        if not isinstance(current_telemetry, Mapping):
            raise FeedbackLoopGuardError(
                "current_telemetry must be a Mapping."
            )
        for k, v in current_telemetry.items():
            if isinstance(v, bool) or not isinstance(v, (int, float)):
                raise FeedbackLoopGuardError(
                    f"current_telemetry[{k!r}] must be numeric."
                )
            if math.isnan(float(v)) or math.isinf(float(v)):
                raise FeedbackLoopGuardError(
                    f"current_telemetry[{k!r}] must be finite."
                )

        reasons = []

        # ---- Evidence volume --------------------------------------
        if len(recalled.memories) < self._config.min_evidence:
            reasons.append(
                f"insufficient evidence: {len(recalled.memories)} < "
                f"{self._config.min_evidence}"
            )
        elif len(recalled.memories) > self._config.max_evidence:
            reasons.append(
                f"too much evidence: {len(recalled.memories)} > "
                f"{self._config.max_evidence}"
            )

        # ---- Staleness --------------------------------------------
        staleness = self._staleness_of(recalled)
        if staleness > self._config.staleness_seconds:
            reasons.append(
                f"evidence stale: {staleness:.0f}s > "
                f"{self._config.staleness_seconds:.0f}s"
            )

        # ---- Policy drift -----------------------------------------
        policy_drift = False
        if current_policy is not None:
            for memory in recalled.memories:
                meta = memory.get("metadata") or {}
                v = meta.get("policy_version")
                if v is not None and v != current_policy.version:
                    policy_drift = True
                    reasons.append(
                        f"policy drift: recalled={v} current="
                        f"{current_policy.version}"
                    )
                    break

        # ---- Telemetry drift --------------------------------------
        telemetry_drift = self._telemetry_drift(recalled, current_telemetry)
        if telemetry_drift > self._config.telemetry_drift_tolerance:
            reasons.append(
                f"telemetry drift: {telemetry_drift:.2%} > "
                f"{self._config.telemetry_drift_tolerance:.2%}"
            )

        # ---- Escalation -------------------------------------------
        requires_human = telemetry_drift >= self._config.human_escalation_threshold
        if policy_drift and self._config.block_on_policy_drift:
            requires_human = True

        allowed = not reasons
        with self._lock:
            self._total_checks += 1
            if allowed:
                self._allowed += 1
            else:
                self._blocked += 1
            if requires_human:
                self._escalated += 1

        verdict = GuardVerdict(
            allowed=allowed,
            reasons=tuple(reasons),
            staleness_seconds=staleness,
            policy_drift=policy_drift,
            telemetry_drift=telemetry_drift,
            requires_human=requires_human,
        )
        logger.debug(
            "Guard verdict: allowed=%s reasons=%d requires_human=%s",
            verdict.allowed, len(verdict.reasons), verdict.requires_human,
        )
        return verdict

    # ---------------------------------------------------------- internals
    def _staleness_of(self, bundle: RecallBundle) -> float:
        """Return the age (seconds) of the newest recalled memory."""
        now = time.time()
        newest = 0.0
        for memory in bundle.memories:
            meta = memory.get("metadata") or {}
            ts = meta.get("observed_at")
            if not ts:
                continue
            try:
                dt = datetime.fromisoformat(str(ts))
                if not dt.tzinfo:
                    dt = dt.replace(tzinfo=timezone.utc)
            except Exception:
                continue
            age = max(0.0, now - dt.timestamp())
            if age < newest or newest == 0.0:
                newest = age
        if newest == 0.0 and not bundle.memories:
            return 0.0
        return newest

    def _telemetry_drift(
        self,
        bundle: RecallBundle,
        current: Mapping[str, float],
    ) -> float:
        """Return the maximum relative drift between recalled and current."""
        if not bundle.memories or not current:
            return 0.0
        # Compare the medians of recalled energy/carbon/latency to current.
        recalled_values: Dict[str, list] = {}
        for memory in bundle.memories:
            meta = memory.get("metadata") or {}
            for key in ("energy_wh", "carbon_gco2e", "latency_ms"):
                if key in meta and isinstance(meta[key], (int, float)):
                    recalled_values.setdefault(key, []).append(float(meta[key]))
        if not recalled_values:
            return 0.0
        max_drift = 0.0
        for key, values in recalled_values.items():
            if key not in current:
                continue
            med = sorted(values)[len(values) // 2]
            cur = float(current[key])
            if med <= 0:
                continue
            drift = abs(cur - med) / med
            max_drift = max(max_drift, drift)
        return max_drift

    # ---------------------------------------------------------- statistics
    def statistics(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "checks": self._total_checks,
                "allowed": self._allowed,
                "blocked": self._blocked,
                "escalated": self._escalated,
                "config": self._config.to_dict(),
                "strict": self._strict,
                "uptime_seconds": time.time() - self._started_at,
            }

    def reset(self) -> None:
        with self._lock:
            self._total_checks = 0
            self._allowed = 0
            self._blocked = 0
            self._escalated = 0
            self._started_at = time.time()

    def to_json(self, **kw: Any) -> str:
        return json.dumps(self.statistics(), default=str, **kw)

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
    "GuardVerdict",
]


# --------------------------------------------------------------------------- #
# Smoke test: python -m memory.feedback_loop_guard
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    from .bounded_recall import RecallBundle
    from .memory_schemas import PolicyRecord

    guard = FeedbackLoopGuard()
    print("repr       :", guard)

    # Build a recall bundle manually (fresh evidence, matching policy).
    now = datetime.now(timezone.utc).isoformat()
    fresh_bundle = RecallBundle(
        query="vision_inference arm_edge",
        memories=tuple({
            "id": f"mock-{i}",
            "metadata": {
                "run_id": f"run-{i:03d}",
                "policy_version": "v0.3",
                "observed_at": now,
                "energy_wh": 8.6,
                "carbon_gco2e": 4.0,
                "latency_ms": 438.0,
            },
        } for i in range(3)),
        scores=(0.9, 0.8, 0.7),
        citations=("run-000", "run-001", "run-002"),
        token_cost=100,
        latency_ms=12.0,
    )

    current_policy = PolicyRecord(
        version="v0.3",
        content={"max_carbon_gco2e": 200.0},
    )

    # Aligned telemetry.
    verdict = guard.check(
        recalled=fresh_bundle,
        current_telemetry={"energy_wh": 8.6, "carbon_gco2e": 4.0, "latency_ms": 438.0},
        current_policy=current_policy,
    )
    print("aligned    :", verdict.allowed, verdict.reasons)
    assert verdict.allowed is True

    # Drifted telemetry.
    verdict2 = guard.check(
        recalled=fresh_bundle,
        current_telemetry={"energy_wh": 20.0, "carbon_gco2e": 12.0, "latency_ms": 900.0},
        current_policy=current_policy,
    )
    print("drifted    :", verdict2.allowed, f"drift={verdict2.telemetry_drift:.2%}",
          f"human={verdict2.requires_human}")
    assert verdict2.allowed is False

    # Policy drift.
    newer_policy = PolicyRecord(
        version="v0.4",
        content={"max_carbon_gco2e": 150.0},
    )
    verdict3 = guard.check(
        recalled=fresh_bundle,
        current_telemetry={"energy_wh": 8.6},
        current_policy=newer_policy,
    )
    print("pol drift  :", verdict3.policy_drift, verdict3.reasons)
    assert verdict3.policy_drift is True

    # Stale evidence.
    stale_bundle = RecallBundle(
        query="q",
        memories=tuple({
            "id": "old",
            "metadata": {
                "run_id": "run-old",
                "policy_version": "v0.3",
                "observed_at": "2020-01-01T00:00:00+00:00",
                "energy_wh": 8.6,
            },
        } for _ in range(2)),
        scores=(0.5, 0.5),
        citations=("run-old",),
        token_cost=10,
        latency_ms=5.0,
    )
    verdict4 = guard.check(
        recalled=stale_bundle,
        current_telemetry={"energy_wh": 8.6},
        current_policy=current_policy,
    )
    print("stale      :", verdict4.allowed, verdict4.reasons)

    print("statistics :", {
        k: v for k, v in guard.statistics().items()
        if k not in ("config", "uptime_seconds")
    })

    print("\nSmoke test passed.")
