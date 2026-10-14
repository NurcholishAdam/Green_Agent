# src/enhancements/active_rlhf.py
"""
Active RLHF Querying with Asynchronous Human Feedback (v2.0.0)
==============================================================

Monitors student confidence and queries humans for feedback when uncertainty
is high.  Integrates with HumanReviewPortal and PolicyFeedback.  Supports
timeout-based fallback.

CHANGES (v2.0.0 over v1.0.0):
- Fixed the critical correctness bug where ``update_student`` reinforced the
  student's current argmax instead of the decision that was actually reviewed.
  The reviewed decision is now persisted per request and returned in the
  feedback dict.
- Enriched the feedback dict on every path with ``request_id``, ``decision``,
  ``status``, and ``autonomous``.
- ``submit_for_review`` failures are caught and returned as
  ``{"status": "submit_failed", "autonomous": True}`` instead of raising.
- ``task_id`` is validated at submission time.
- Entropy is renormalized after clipping so ``normalized_entropy`` is in
  ``[0, 1]``.
- Event/future based waiting replaces polling; a polling fallback is used if
  the portal does not support callbacks.
- Deduplication via state-hash cache and a per-session token-bucket rate
  limiter; both are configurable.
- Explicit student adapter contract: ``apply_human_feedback(features, action,
  reward)`` (preferred) or ``update_with_feedback(features, action, reward)``
  (legacy).  Ambiguous ``update`` fallback removed.
- Emits a ``FeedbackEvent`` when the schema is importable.
- Observability: ``get_stats()`` returns counters for submissions, approvals,
  rejections, timeouts, submit failures, dedup hits, rate-limit rejections,
  and mean latency.
- ``update_student`` returns a status dict instead of silently returning.
- Feature extraction handles ``dict``, ``np.ndarray``, ``pydantic``-style
  objects with ``to_feature_vector``, and explicit callables.
- Lazy logging via parameterized calls.
- ``__all__`` and ``__version__`` added.
- Built-in test suite at the bottom.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import time
import uuid
from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, TypeVar, Union

import numpy as np

logger = logging.getLogger(__name__)

__version__ = "2.0.0"
__all__ = [
    "__version__",
    "ActiveRLHF",
    "PendingRequest",
    "ActiveRLHFStats",
    "TokenBucket",
]

StateT = TypeVar("StateT")

# Optional FeedbackEvent import (try both common module paths).
try:
    from ..schemas.feedback_event import FeedbackEvent  # type: ignore
except ImportError:
    try:
        from src.enhancements.schemas.feedback_event import FeedbackEvent  # type: ignore
    except ImportError:
        FeedbackEvent = None  # type: ignore


# ============================================================================
# Utilities
# ============================================================================
def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _hash_features(features: np.ndarray) -> str:
    """Stable 16-hex-char hash of a float vector, for deduplication."""
    b = np.asarray(features, dtype=np.float64).tobytes()
    return hashlib.sha256(b).hexdigest()[:16]


# ============================================================================
# Token bucket (per-session rate limiter)
# ============================================================================
class TokenBucket:
    """Simple monotonic-time token bucket."""

    def __init__(self, rate_per_second: float, capacity: int) -> None:
        self.rate = float(rate_per_second)
        self.capacity = int(capacity)
        self.tokens = float(capacity)
        self.last_refill = time.monotonic()

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self.last_refill
        if elapsed > 0:
            self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
            self.last_refill = now

    def try_acquire(self) -> bool:
        self._refill()
        if self.tokens >= 1.0:
            self.tokens -= 1.0
            return True
        return False


# ============================================================================
# Pending request bookkeeping
# ============================================================================
@dataclass
class PendingRequest:
    request_id: str
    task_id: str
    decision: int
    state: Any
    features: np.ndarray
    submitted_at: float
    future: Optional[asyncio.Future] = None


@dataclass
class ActiveRLHFStats:
    submitted: int = 0
    submit_failed: int = 0
    approved: int = 0
    rejected: int = 0
    timeout: int = 0
    dedup_hits: int = 0
    rate_limited: int = 0
    applied_updates: int = 0
    skipped_updates: int = 0
    total_latency_s: float = 0.0
    latency_samples: int = 0

    def as_dict(self) -> Dict[str, Any]:
        mean_latency = (
            self.total_latency_s / self.latency_samples
            if self.latency_samples else 0.0
        )
        return {
            "submitted": self.submitted,
            "submit_failed": self.submit_failed,
            "approved": self.approved,
            "rejected": self.rejected,
            "timeout": self.timeout,
            "dedup_hits": self.dedup_hits,
            "rate_limited": self.rate_limited,
            "applied_updates": self.applied_updates,
            "skipped_updates": self.skipped_updates,
            "mean_latency_s": round(mean_latency, 4),
        }


# ============================================================================
# ActiveRLHF
# ============================================================================
class ActiveRLHF:
    """
    Active learning from human feedback.

    Attributes:
        portal: HumanReviewPortal instance for submitting review requests.
        student: Model with ``predict_proba(features)`` and one of
            ``apply_human_feedback(features, action, reward)`` or
            ``update_with_feedback(features, action, reward)``.
        uncertainty_threshold: Normalized entropy above which human feedback
            is requested (0.0 to 1.0).
        timeout: Default seconds to wait for human feedback before falling
            back to autonomous action.
        reward_approved: Reward given when human approves the decision.
        reward_rejected: Reward (penalty) given when human rejects the decision.
        feature_extractor: Optional callable that converts a state to a
            feature vector.  If ``None``, feature extraction is inferred.
        max_queries_per_minute: Token-bucket rate limit (0 disables).
        dedup_window: Number of recent state hashes to remember (0 disables).
        dedup_ttl_seconds: How long a hash remains in the dedup cache.
    """

    def __init__(
        self,
        portal: Any,
        student: Any,
        uncertainty_threshold: float = 0.4,
        timeout: float = 30.0,
        reward_approved: float = 1.0,
        reward_rejected: float = -0.2,
        feature_extractor: Optional[Callable[[Any], np.ndarray]] = None,
        max_queries_per_minute: int = 60,
        dedup_window: int = 128,
        dedup_ttl_seconds: float = 300.0,
        timeout_reward: float = 0.0,
        poll_interval_seconds: float = 0.5,
        emit_feedback_events: bool = True,
    ) -> None:
        if not 0.0 <= uncertainty_threshold <= 1.0:
            raise ValueError("uncertainty_threshold must be in [0, 1]")
        if timeout <= 0:
            raise ValueError("timeout must be positive")

        self.portal = portal
        self.student = student
        self.threshold = float(uncertainty_threshold)
        self.timeout = float(timeout)
        self.reward_approved = float(reward_approved)
        self.reward_rejected = float(reward_rejected)
        self.reward_timeout = float(timeout_reward)
        self.poll_interval = float(poll_interval_seconds)
        self.emit_feedback_events = bool(emit_feedback_events)

        if feature_extractor is not None:
            self.feature_extractor = feature_extractor
        else:
            self.feature_extractor = _infer_feature_extractor(student)

        # Per-request bookkeeping.
        self._pending: Dict[str, PendingRequest] = {}

        # Deduplication cache: state_hash -> timestamp (monotonic).
        self._dedup_window = int(dedup_window)
        self._dedup_ttl = float(dedup_ttl_seconds)
        self._dedup: "OrderedDict[str, float]" = OrderedDict()

        # Rate limiting.
        if max_queries_per_minute > 0:
            rate = max_queries_per_minute / 60.0
            self._bucket: Optional[TokenBucket] = TokenBucket(rate, max_queries_per_minute)
        else:
            self._bucket = None

        # Stats.
        self.stats = ActiveRLHFStats()

    # ------------------------------------------------------------------
    # Feature extraction
    # ------------------------------------------------------------------
    def _get_features(self, state: StateT) -> np.ndarray:
        return np.asarray(self.feature_extractor(state), dtype=np.float32).reshape(-1)

    # ------------------------------------------------------------------
    # Uncertainty
    # ------------------------------------------------------------------
    def normalized_entropy(self, state: StateT) -> float:
        """Return the normalized entropy of the student's action distribution."""
        features = self._get_features(state)
        probs = np.asarray(self.student.predict_proba(features), dtype=np.float64)
        if probs.ndim != 1 or probs.size == 0:
            return 0.0
        # Clip, then renormalize so the distribution is valid.
        probs = np.clip(probs, 0.0, None)
        total = probs.sum()
        if total <= 0:
            return 0.0
        probs = probs / total
        # Entropy via a safe computation that avoids log(0).
        nz = probs > 0
        entropy = float(-np.sum(probs[nz] * np.log(probs[nz])))
        max_entropy = math.log(probs.size) if probs.size > 1 else 0.0
        if max_entropy <= 0:
            return 0.0
        return float(min(1.0, max(0.0, entropy / max_entropy)))

    def should_query(self, state: StateT) -> bool:
        """Return True if the student's normalized entropy exceeds the threshold."""
        return self.normalized_entropy(state) > self.threshold

    # ------------------------------------------------------------------
    # Rate limiting + deduplication
    # ------------------------------------------------------------------
    def _check_rate_limit(self) -> bool:
        if self._bucket is None:
            return True
        return self._bucket.try_acquire()

    def _check_dedup(self, features: np.ndarray) -> bool:
        """
        Return True if this state hash was recently submitted, i.e. the
        caller should skip.  Maintains the LRU cache on every call.
        """
        if self._dedup_window <= 0:
            return False
        now = time.monotonic()
        # Purge expired entries.
        expired = [k for k, t in self._dedup.items() if now - t > self._dedup_ttl]
        for k in expired:
            self._dedup.pop(k, None)

        h = _hash_features(features)
        if h in self._dedup:
            # Refresh recency.
            self._dedup.move_to_end(h)
            self._dedup[h] = now
            return True

        self._dedup[h] = now
        while len(self._dedup) > self._dedup_window:
            self._dedup.popitem(last=False)
        return False

    # ------------------------------------------------------------------
    # Submit + await
    # ------------------------------------------------------------------
    async def request_feedback(
        self,
        task: Dict[str, Any],
        state: StateT,
        decision: int,
        timeout: Optional[float] = None,
    ) -> Dict[str, Any]:
        """
        Submit a review request and wait for a response.

        Returns a dict with at least:
            request_id, status, autonomous, decision
        ``status`` is one of: ``approved``, ``rejected``, ``timeout``,
        ``submit_failed``, ``rate_limited``, ``duplicate``, ``no_query``.
        """
        timeout = float(timeout) if timeout is not None else self.timeout
        task_id = task.get("task_id") if isinstance(task, dict) else None
        if not task_id or not isinstance(task_id, str):
            raise ValueError("task must contain a non-empty string 'task_id'")

        features = self._get_features(state)

        # 1. Rate limit
        if not self._check_rate_limit():
            self.stats.rate_limited += 1
            logger.info("Rate limited; skipping submission for task=%s", task_id)
            return _make_result(
                request_id=None, decision=int(decision),
                status="rate_limited", autonomous=True,
            )

        # 2. Deduplication
        if self._check_dedup(features):
            self.stats.dedup_hits += 1
            logger.info("Duplicate state; skipping submission for task=%s", task_id)
            return _make_result(
                request_id=None, decision=int(decision),
                status="duplicate", autonomous=True,
            )

        # 3. Submit
        try:
            request_id = self.portal.submit_for_review(
                task_id=task_id,
                agent_output={"decision": int(decision), "state": features.tolist()},
                escalation_reasons=["high_uncertainty"],
                criticality_level="medium",
            )
        except Exception as e:
            self.stats.submit_failed += 1
            logger.warning(
                "submit_for_review failed for task=%s: %s", task_id, e
            )
            return _make_result(
                request_id=None, decision=int(decision),
                status="submit_failed", autonomous=True,
            )

        if not request_id:
            self.stats.submit_failed += 1
            logger.warning("submit_for_review returned empty id for task=%s", task_id)
            return _make_result(
                request_id=None, decision=int(decision),
                status="submit_failed", autonomous=True,
            )

        self.stats.submitted += 1
        submitted_at = time.monotonic()

        pending = PendingRequest(
            request_id=request_id,
            task_id=task_id,
            decision=int(decision),
            state=state,
            features=features,
            submitted_at=submitted_at,
        )
        self._pending[request_id] = pending

        # 4. Await feedback with timeout
        try:
            raw = await asyncio.wait_for(
                self._await_feedback(request_id, pending),
                timeout=timeout,
            )
            latency = time.monotonic() - submitted_at
            self.stats.total_latency_s += latency
            self.stats.latency_samples += 1
            status = _normalize_status(raw)
            if status == "approved":
                self.stats.approved += 1
            elif status == "rejected":
                self.stats.rejected += 1
            result = _make_result(
                request_id=request_id,
                decision=int(decision),
                status=status,
                autonomous=False,
                raw=raw,
            )
            logger.info(
                "Feedback for request=%s: status=%s latency=%.2fs",
                request_id, status, latency,
            )
            return result
        except asyncio.TimeoutError:
            self.stats.timeout += 1
            logger.warning(
                "Timeout waiting for feedback on request=%s (task=%s); "
                "falling back to autonomous decision.",
                request_id, task_id,
            )
            return _make_result(
                request_id=request_id, decision=int(decision),
                status="timeout", autonomous=True,
            )
        finally:
            # The request is now complete; the caller must call update_student
            # (which also clears the pending entry).
            pass

    # ------------------------------------------------------------------
    async def _await_feedback(self, request_id: str, pending: PendingRequest
                              ) -> Dict[str, Any]:
        """
        Wait for the portal's feedback.  Prefers a future/callback if the
        portal exposes one; otherwise falls back to polling.
        """
        # Preferred: portal provides an async getter.
        getter = getattr(self.portal, "get_feedback", None)
        if callable(getter):
            try:
                maybe = getter(request_id)
                if asyncio.iscoroutine(maybe):
                    return await maybe
                return maybe
            except Exception as e:
                logger.warning("portal.get_feedback failed: %s", e)
                # fall through to polling

        # Preferred: portal provides a future/event.
        fut_factory = getattr(self.portal, "future_for", None)
        if callable(fut_factory):
            try:
                fut = fut_factory(request_id)
                if isinstance(fut, asyncio.Future):
                    pending.future = fut
                    return await fut
            except Exception as e:
                logger.warning("portal.future_for failed: %s", e)

        # Fallback: poll.
        while True:
            checker = getattr(self.portal, "get_feedback_if_available", None)
            if callable(checker):
                try:
                    fb = checker(request_id)
                    if fb is not None:
                        return fb
                except Exception as e:
                    logger.warning("portal polling raised: %s", e)
            await asyncio.sleep(self.poll_interval)

    # ------------------------------------------------------------------
    # Student update
    # ------------------------------------------------------------------
    def update_student(
        self,
        state: StateT,
        feedback: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Update the student using human (or fallback) feedback.

        Returns ``{"applied": bool, "reason": str, "reward": float}``.
        """
        status = feedback.get("status")
        decision = feedback.get("decision")
        request_id = feedback.get("request_id")

        if decision is None:
            self.stats.skipped_updates += 1
            logger.warning("Feedback missing 'decision'; skipping update.")
            return {"applied": False, "reason": "missing_decision", "reward": 0.0}
        try:
            decision = int(decision)
        except (TypeError, ValueError):
            self.stats.skipped_updates += 1
            logger.warning("Feedback 'decision' is not an integer; skipping update.")
            return {"applied": False, "reason": "invalid_decision", "reward": 0.0}

        # Determine reward.
        if status == "approved":
            reward = self.reward_approved
        elif status == "rejected":
            reward = self.reward_rejected
        elif status == "timeout":
            reward = self.reward_timeout
            # A zero (default) timeout reward is a no-op; still clear pending.
            if reward == 0.0:
                self._clear_pending(request_id)
                self.stats.skipped_updates += 1
                logger.info(
                    "Timeout with neutral reward; no update applied (request=%s).",
                    request_id,
                )
                return {"applied": False, "reason": "timeout_neutral", "reward": 0.0}
        elif status in ("submit_failed", "rate_limited", "duplicate", "no_query"):
            self._clear_pending(request_id)
            self.stats.skipped_updates += 1
            logger.info("Status=%s; no update applied.", status)
            return {"applied": False, "reason": status, "reward": 0.0}
        else:
            self._clear_pending(request_id)
            self.stats.skipped_updates += 1
            logger.warning("Unknown feedback status=%s; no update applied.", status)
            return {"applied": False, "reason": "unknown_status", "reward": 0.0}

        # Apply the update to the reviewed decision, not to the current argmax.
        features = self._get_features(state)
        applied = self._apply_student_update(features, decision, reward)
        self._clear_pending(request_id)

        if applied:
            self.stats.applied_updates += 1
        else:
            self.stats.skipped_updates += 1

        # Emit cross-module feedback event.
        if self.emit_feedback_events:
            self._emit_feedback_event(feedback, decision, reward)

        return {
            "applied": applied,
            "reason": "ok" if applied else "student_update_failed",
            "reward": float(reward),
        }

    def _apply_student_update(self, features: np.ndarray, action: int,
                              reward: float) -> bool:
        apply_fn = getattr(self.student, "apply_human_feedback", None)
        if callable(apply_fn):
            try:
                apply_fn(features, action, reward)
                return True
            except Exception as e:
                logger.warning("apply_human_feedback failed: %s", e)
                return False

        legacy_fn = getattr(self.student, "update_with_feedback", None)
        if callable(legacy_fn):
            try:
                legacy_fn(features, action, reward)
                return True
            except Exception as e:
                logger.warning("update_with_feedback failed: %s", e)
                return False

        logger.warning(
            "Student does not implement apply_human_feedback or "
            "update_with_feedback; no update applied."
        )
        return False

    def _clear_pending(self, request_id: Optional[str]) -> None:
        if not request_id:
            return
        pending = self._pending.pop(request_id, None)
        if pending is not None and pending.future is not None and not pending.future.done():
            # Best-effort cancel so the polling loop (if any) exits.
            try:
                pending.future.cancel()
            except Exception:
                pass

    # ------------------------------------------------------------------
    # FeedbackEvent
    # ------------------------------------------------------------------
    def _emit_feedback_event(
        self, feedback: Dict[str, Any], decision: int, reward: float
    ) -> None:
        if FeedbackEvent is None:
            return
        task_id = feedback.get("task_id") or self._pending_task_id(feedback)
        try:
            event = FeedbackEvent(
                source="active_rlhf",
                feedback_type="rlhf",
                context={
                    "task_id": task_id or "unknown",
                    "teacher_ids": [],
                },
                action={
                    "selected_action": str(decision),
                    "selected_rank": 1,
                    "confidence_score": 0.5,
                },
                performance={
                    "quality_score": float(max(0.0, min(1.0, (reward + 1.0) / 2.0))),
                    "latency_ms": 0.0,
                    "energy_joules": 0.0,
                    "carbon_g": 0.0,
                    "helium_cost": 0.0,
                    "duration_ms": 0.0,
                },
                adaptive_cost_value=float(reward),
                tags=["rlhf", str(feedback.get("status", "unknown"))],
                envelope={"trace_id": str(uuid.uuid4())},
                rlhf={
                    "human_feedback_score": float(
                        max(0.0, min(1.0, (reward + 1.0) / 2.0))
                    ),
                    "reward_model_score": float(reward),
                },
            )
            logger.debug("FeedbackEvent emitted: %s", getattr(event, "event_id", "?"))
        except Exception as e:
            logger.warning("Failed to emit FeedbackEvent: %s", e)

    def _pending_task_id(self, feedback: Dict[str, Any]) -> Optional[str]:
        rid = feedback.get("request_id")
        if rid and rid in self._pending:
            return self._pending[rid].task_id
        return None

    # ------------------------------------------------------------------
    # Combined helper
    # ------------------------------------------------------------------
    async def maybe_query(
        self,
        task: Dict[str, Any],
        state: StateT,
        decision: int,
        timeout: Optional[float] = None,
    ) -> Dict[str, Any]:
        """
        If uncertainty is above the threshold, submit a request and wait for
        feedback.  Otherwise return a ``no_query`` result.

        The returned dict always includes ``request_id``, ``status``,
        ``autonomous``, and ``decision``.
        """
        if not self.should_query(state):
            return _make_result(
                request_id=None, decision=int(decision),
                status="no_query", autonomous=True,
            )
        return await self.request_feedback(task, state, decision, timeout=timeout)

    # ------------------------------------------------------------------
    # Observability
    # ------------------------------------------------------------------
    def get_stats(self) -> Dict[str, Any]:
        return {
            "version": __version__,
            "threshold": self.threshold,
            "timeout": self.timeout,
            "pending": len(self._pending),
            **self.stats.as_dict(),
        }

    def reset_stats(self) -> None:
        self.stats = ActiveRLHFStats()

    def pending_count(self) -> int:
        return len(self._pending)


# ============================================================================
# Helpers
# ============================================================================
def _infer_feature_extractor(student: Any) -> Callable[[Any], np.ndarray]:
    """
    Build a feature extractor based on common state shapes.

    The returned callable handles:
      - objects exposing ``to_feature_vector`` (may take a scaler)
      - numpy arrays / lists / tuples of numbers
      - dicts of numeric or bool values (sorted by key)
    """
    def _extract(state: Any) -> np.ndarray:
        if state is None:
            raise ValueError("state is None; cannot extract features.")

        # 1. Explicit method on the state object.
        fn = getattr(state, "to_feature_vector", None)
        if callable(fn):
            try:
                return np.asarray(fn(), dtype=np.float32)
            except TypeError:
                # Some descriptors require a scaler argument.
                return np.asarray(fn(None), dtype=np.float32)

        # 2. numpy array / list / tuple.
        if isinstance(state, (np.ndarray, list, tuple)):
            return np.asarray(state, dtype=np.float32)

        # 3. dict of numbers.
        if isinstance(state, dict):
            try:
                items = sorted(state.items())
                return np.asarray(
                    [float(v) for _, v in items], dtype=np.float32
                )
            except (TypeError, ValueError):
                raise ValueError(
                    "dict states must contain numeric or bool values."
                )

        # 4. Generic fallback: float(state) if possible.
        try:
            return np.asarray([float(state)], dtype=np.float32)
        except (TypeError, ValueError):
            raise ValueError(
                f"Cannot extract features from state of type {type(state).__name__}."
            )

    return _extract


def _make_result(
    request_id: Optional[str],
    decision: int,
    status: str,
    autonomous: bool,
    raw: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "request_id": request_id,
        "status": status,
        "autonomous": bool(autonomous),
        "decision": int(decision),
    }
    if raw is not None:
        result["raw"] = raw
        # Preserve any portal-provided fields except our reserved keys.
        for k, v in raw.items():
            if k in result:
                continue
            result[k] = v
    return result


def _normalize_status(raw: Any) -> str:
    if isinstance(raw, dict):
        s = raw.get("status")
        if isinstance(s, str):
            s = s.lower()
            if s in ("approved", "rejected", "timeout"):
                return s
            if s in ("approve", "accept", "ok"):
                return "approved"
            if s in ("reject", "deny", "decline"):
                return "rejected"
        # Boolean shorthands.
        if raw.get("approved") is True:
            return "approved"
        if raw.get("approved") is False:
            return "rejected"
    return "approved" if raw is True else "rejected" if raw is False else "unknown"


# ============================================================================
# Tests
# ============================================================================
class _FakeStudent:
    """Student that records applied updates."""

    def __init__(self, n_actions: int = 3, seed: int = 0) -> None:
        rng = np.random.default_rng(seed)
        self.n_actions = n_actions
        self.weights = rng.normal(0.0, 0.1, size=(4, n_actions))
        self.updates: List[Tuple[np.ndarray, int, float]] = []

    def predict_proba(self, features: np.ndarray) -> np.ndarray:
        z = features @ self.weights
        z = z - np.max(z)
        e = np.exp(z)
        return e / e.sum()

    def apply_human_feedback(self, features: np.ndarray, action: int,
                             reward: float) -> None:
        self.updates.append((np.asarray(features), int(action), float(reward)))
        # Simple gradient step.
        probs = self.predict_proba(features)
        one_hot = np.zeros_like(probs)
        one_hot[int(action)] = 1.0
        grad = -reward * (one_hot - probs)
        self.weights -= 0.05 * np.outer(features, grad)


class _FakeLegacyStudent:
    """Student that only implements the legacy update method."""

    def __init__(self) -> None:
        self.updates: List[Tuple[np.ndarray, int, float]] = []

    def predict_proba(self, features: np.ndarray) -> np.ndarray:
        return np.ones(3) / 3

    def update_with_feedback(self, features: np.ndarray, action: int,
                             reward: float) -> None:
        self.updates.append((np.asarray(features), int(action), float(reward)))


class _FakePortal:
    """Portal with configurable feedback, submit failures, and rate limiting."""

    def __init__(
        self,
        response: Optional[Dict[str, Any]] = None,
        response_after: float = 0.0,
        raise_on_submit: bool = False,
        never_respond: bool = False,
        use_async_getter: bool = False,
    ) -> None:
        self.response = response
        self.response_after = response_after
        self.raise_on_submit = raise_on_submit
        self.never_respond = never_respond
        self.use_async_getter = use_async_getter
        self._submitted: Dict[str, float] = {}
        self._submitted_count = 0
        self._next_id = 0

    def submit_for_review(self, task_id: str, agent_output: Dict[str, Any],
                          escalation_reasons: List[str],
                          criticality_level: str) -> str:
        if self.raise_on_submit:
            raise RuntimeError("portal down")
        self._next_id += 1
        rid = f"req-{self._next_id}"
        self._submitted[rid] = time.monotonic()
        self._submitted_count += 1
        return rid

    def get_feedback_if_available(self, request_id: str) -> Optional[Dict[str, Any]]:
        if self.never_respond:
            return None
        start = self._submitted.get(request_id)
        if start is None:
            return None
        if time.monotonic() - start < self.response_after:
            return None
        if self.response is None:
            return None
        return dict(self.response)

    async def get_feedback(self, request_id: str) -> Dict[str, Any]:
        if self.never_respond:
            await asyncio.Event().wait()  # hang forever
        await asyncio.sleep(self.response_after)
        return dict(self.response or {"status": "approved"})


def _run_async(coro):
    return asyncio.new_event_loop().run_until_complete(coro)


def _test_entropy_bounds() -> None:
    student = _FakeStudent()
    active = ActiveRLHF(portal=_FakePortal(), student=student)
    state = np.zeros(4, dtype=np.float32)
    e = active.normalized_entropy(state)
    assert 0.0 <= e <= 1.0


def _test_threshold_behaviour() -> None:
    # Force a uniform distribution so entropy is ~1.0.
    class Uniform:
        def predict_proba(self, features):
            return np.ones(4) / 4
    active = ActiveRLHF(portal=_FakePortal(), student=Uniform(),
                        uncertainty_threshold=0.5)
    state = np.zeros(4)
    assert active.should_query(state) is True
    active.threshold = 0.99
    assert active.should_query(state) is False


def _test_approve_updates_reviewed_action() -> None:
    student = _FakeStudent()
    portal = _FakePortal(response={"status": "approved"}, response_after=0.0)
    active = ActiveRLHF(portal=portal, student=student, timeout=2.0)
    state = np.zeros(4, dtype=np.float32)

    result = _run_async(active.request_feedback(
        {"task_id": "t1"}, state, decision=2,
    ))
    assert result["status"] == "approved"
    assert result["decision"] == 2
    assert result["autonomous"] is False

    status = active.update_student(state, result)
    assert status["applied"] is True
    assert student.updates[-1][1] == 2  # reviewed decision, not argmax
    assert student.updates[-1][2] == 1.0


def _test_reject_uses_negative_reward() -> None:
    student = _FakeStudent()
    portal = _FakePortal(response={"status": "rejected"}, response_after=0.0)
    active = ActiveRLHF(portal=portal, student=student, timeout=2.0)
    state = np.zeros(4, dtype=np.float32)
    result = _run_async(active.request_feedback({"task_id": "t2"}, state, 1))
    assert result["status"] == "rejected"
    status = active.update_student(state, result)
    assert status["applied"] is True
    assert student.updates[-1][2] == -0.2
    assert student.updates[-1][1] == 1


def _test_timeout_fallback() -> None:
    student = _FakeStudent()
    portal = _FakePortal(never_respond=True)
    active = ActiveRLHF(portal=portal, student=student, timeout=0.2,
                        poll_interval_seconds=0.05)
    state = np.zeros(4, dtype=np.float32)
    result = _run_async(active.request_feedback({"task_id": "t3"}, state, 0))
    assert result["status"] == "timeout"
    assert result["autonomous"] is True
    status = active.update_student(state, result)
    assert status["applied"] is False
    assert status["reason"] == "timeout_neutral"


def _test_timeout_with_reward() -> None:
    student = _FakeStudent()
    portal = _FakePortal(never_respond=True)
    active = ActiveRLHF(portal=portal, student=student, timeout=0.2,
                        poll_interval_seconds=0.05, timeout_reward=0.05)
    state = np.zeros(4, dtype=np.float32)
    result = _run_async(active.request_feedback({"task_id": "t3b"}, state, 0))
    assert result["status"] == "timeout"
    status = active.update_student(state, result)
    assert status["applied"] is True
    assert status["reward"] == 0.05


def _test_submit_failure() -> None:
    student = _FakeStudent()
    portal = _FakePortal(raise_on_submit=True)
    active = ActiveRLHF(portal=portal, student=student)
    state = np.zeros(4, dtype=np.float32)
    result = _run_async(active.request_feedback({"task_id": "t4"}, state, 0))
    assert result["status"] == "submit_failed"
    assert result["autonomous"] is True
    status = active.update_student(state, result)
    assert status["applied"] is False


def _test_missing_task_id() -> None:
    student = _FakeStudent()
    active = ActiveRLHF(portal=_FakePortal(), student=student)
    state = np.zeros(4, dtype=np.float32)
    try:
        _run_async(active.request_feedback({}, state, 0))
    except ValueError:
        pass
    else:
        raise AssertionError("missing task_id should raise")


def _test_async_getter_shortcut() -> None:
    student = _FakeStudent()
    portal = _FakePortal(
        response={"status": "approved"}, response_after=0.05,
        use_async_getter=True,
    )
    active = ActiveRLHF(portal=portal, student=student, timeout=2.0)
    state = np.zeros(4, dtype=np.float32)
    result = _run_async(active.request_feedback({"task_id": "t5"}, state, 0))
    assert result["status"] == "approved"


def _test_legacy_student_adapter() -> None:
    student = _FakeLegacyStudent()
    portal = _FakePortal(response={"status": "approved"}, response_after=0.0)
    active = ActiveRLHF(portal=portal, student=student, timeout=1.0)
    state = np.zeros(4, dtype=np.float32)
    result = _run_async(active.request_feedback({"task_id": "t6"}, state, 0))
    status = active.update_student(state, result)
    assert status["applied"] is True
    assert student.updates[-1][1] == 0


def _test_dedup_and_rate_limit() -> None:
    student = _FakeStudent()
    portal = _FakePortal(response={"status": "approved"}, response_after=0.0)
    active = ActiveRLHF(
        portal=portal, student=student, timeout=1.0,
        max_queries_per_minute=1, dedup_window=4,
    )
    state_a = np.array([0.1, 0.2, 0.3, 0.4], dtype=np.float32)
    state_b = np.array([0.9, 0.8, 0.7, 0.6], dtype=np.float32)

    r1 = _run_async(active.request_feedback({"task_id": "d1"}, state_a, 0))
    assert r1["status"] == "approved"
    # Second identical state is deduplicated.
    r2 = _run_async(active.request_feedback({"task_id": "d1"}, state_a, 0))
    assert r2["status"] == "duplicate"
    # Third, distinct state hits the rate limit (capacity = 1).
    r3 = _run_async(active.request_feedback({"task_id": "d1"}, state_b, 0))
    assert r3["status"] == "rate_limited"


def _test_maybe_query_no_query() -> None:
    class Uniform:
        def predict_proba(self, features):
            return np.ones(4) / 4
    active = ActiveRLHF(portal=_FakePortal(), student=Uniform(),
                        uncertainty_threshold=0.99)
    result = _run_async(active.maybe_query(
        {"task_id": "mq"}, np.zeros(4), decision=1,
    ))
    assert result["status"] == "no_query"
    assert result["decision"] == 1


def _test_feature_extractor_shapes() -> None:
    class WithMethod:
        def to_feature_vector(self):
            return np.array([1.0, 2.0, 3.0], dtype=np.float32)
    class DictState:
        pass
    student = _FakeStudent(n_actions=3)
    active = ActiveRLHF(portal=_FakePortal(), student=student)

    f1 = active._get_features(WithMethod())
    assert f1.shape == (3,)
    f2 = active._get_features([1.0, 2.0, 3.0, 4.0])
    assert f2.shape == (4,)
    f3 = active._get_features({"a": 1.0, "b": 2.0})
    assert f3.shape == (2,)


def _test_stats() -> None:
    student = _FakeStudent()
    portal = _FakePortal(response={"status": "approved"}, response_after=0.0)
    active = ActiveRLHF(portal=portal, student=student, timeout=1.0,
                        max_queries_per_minute=0, dedup_window=0)
    state = np.zeros(4, dtype=np.float32)
    result = _run_async(active.request_feedback({"task_id": "s1"}, state, 0))
    active.update_student(state, result)
    stats = active.get_stats()
    assert stats["submitted"] == 1
    assert stats["approved"] == 1
    assert stats["applied_updates"] == 1


def _test_feedback_event_emission() -> None:
    """If the schema is available, an event is emitted; otherwise the call
    is a no-op."""
    student = _FakeStudent()
    portal = _FakePortal(response={"status": "approved"}, response_after=0.0)
    active = ActiveRLHF(portal=portal, student=student, timeout=1.0,
                        emit_feedback_events=True)
    state = np.zeros(4, dtype=np.float32)
    result = _run_async(active.request_feedback({"task_id": "ev"}, state, 0))
    # Should not raise whether or not FeedbackEvent exists.
    active.update_student(state, result)


def _test_pending_cleared() -> None:
    student = _FakeStudent()
    portal = _FakePortal(response={"status": "approved"}, response_after=0.0)
    active = ActiveRLHF(portal=portal, student=student, timeout=1.0)
    state = np.zeros(4, dtype=np.float32)
    result = _run_async(active.request_feedback({"task_id": "pc"}, state, 0))
    assert active.pending_count() == 1
    active.update_student(state, result)
    assert active.pending_count() == 0


def _run_all_tests() -> None:
    _test_entropy_bounds()
    _test_threshold_behaviour()
    _test_approve_updates_reviewed_action()
    _test_reject_uses_negative_reward()
    _test_timeout_fallback()
    _test_timeout_with_reward()
    _test_submit_failure()
    _test_missing_task_id()
    _test_async_getter_shortcut()
    _test_legacy_student_adapter()
    _test_dedup_and_rate_limit()
    _test_maybe_query_no_query()
    _test_feature_extractor_shapes()
    _test_stats()
    _test_feedback_event_emission()
    _test_pending_cleared()
    print("All ActiveRLHF tests passed.")


if __name__ == "__main__":
    import argparse
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(description="ActiveRLHF")
    parser.add_argument("--test", action="store_true",
                        help="Run the built-in test suite.")
    args = parser.parse_args()
    if args.test:
        _run_all_tests()
    else:
        parser.print_help()
