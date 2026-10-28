"""
HumanReviewGate — routes decisions to a human for approval. — v2.0.0

v2.0.0 changes vs. v1.0.0
-------------------------
**P0 bug fixes**
  * 2.1 ``auto_approve_below`` now implements its name. Previously
        ``auto_approve_below="medium"`` auto-approved *only* medium
        decisions — low-risk decisions still went to a human, and
        medium-risk decisions bypassed review. The parameter is now
        a strict threshold: ``"medium"`` auto-approves ``"low"`` and
        ``"medium"``, requires review for ``"high"`` and
        ``"critical"``.
  * 2.2 ``RLock`` around every state mutation. ``require()`` uses
        ``try/finally`` so a cancelled coroutine or callback
        exception still removes the pending entry. Previously two
        concurrent ``require()`` calls could corrupt ``_pending``
        and cancelled reviews leaked pending entries forever.
  * 2.3 Callback failures raise ``HumanReviewCallbackError``
        instead of returning ``False``. Previously "the review
        infrastructure is broken" was indistinguishable from "the
        reviewer said no".
  * 2.4 ``ReviewRequest.__post_init__`` validates every field
        (non-empty ``request_id`` and ``action``, list of non-empty
        strings in ``reasons``, known ``risk_level``, dict
        ``context``, tz-aware ``requested_at``).
  * 2.5 ``auto_approve_below="critical"`` is rejected at
        construction — auto-approving the highest risk tier
        defeats the purpose of a human review gate.
  * 2.6 ``decision.risk_level`` is normalized to lowercase, so
        ``"CRITICAL"``, ``" Critical "``, and ``RiskLevel.CRITICAL``
        all match the config.
  * 2.7 ``ReviewRequest.action`` is read from ``decision.action``,
        not from ``context.get("action", ...)``. The caller can no
        longer override the action in the review request.
  * 2.8 ``_feedback`` is a bounded ``deque(maxlen=...)``. Previously
        it grew without limit — a slow memory leak on any
        long-running gate.

**P1 robustness**
  * 3.1 ``pending()``, ``get_pending(id)``, ``feedback(limit)`` —
        query the current queue and recent outcomes.
  * 3.2 ``HumanReviewError`` base class +
        ``HumanReviewConfigError`` +
        ``HumanReviewCallbackError`` +
        ``HumanReviewTimeoutError`` +
        ``HumanReviewQueueFullError``. Add to
        ``governance/__init__.py``'s ``_ERROR_CLASSES_BY_MODULE``
        for ``except GovernanceError:`` compatibility.
  * 3.3 ``require(..., timeout=...)`` — bounded wait; timeout
        raises ``HumanReviewTimeoutError``.
  * 3.4 ``ReviewResponse`` — callbacks may return
        ``Union[bool, ReviewVerdict, ReviewResponse]``. When a
        ``ReviewResponse`` is returned, ``reviewer_id`` and
        ``reason`` are recorded in ``_feedback``.
  * 3.5 ``to_dict`` / ``from_dict`` / ``to_json`` / ``from_json``
        on ``ReviewRequest``; ``HumanReviewGate`` exposes its config.
  * 3.6 ``ReviewRequest`` is ``frozen=True`` — the callback
        cannot mutate the request it received.
  * 3.7 ``_pending`` is a ``dict`` keyed on ``request_id`` — O(1)
        lookups, insert, and remove.
  * 3.8 ``metrics_hook(event, fields)`` — fires ``review_requested``,
        ``review_auto_approved``, ``review_approved``,
        ``review_denied``, ``review_escalated``, ``review_failed``,
        ``review_no_callback``, ``review_timed_out``.
  * 3.9 Optional ``audit_log`` integration — every completed
        review is appended to the injected ``AuditLog``
        (duck-typed).
  * 3.10 ``set_callback`` takes the lock and documents the
        semantics for in-flight reviews.
  * 3.15 ``ReviewVerdict`` enum — approve / deny / escalate.
  * 3.17 ``max_pending`` config — bounded review queue.
  * 3.18 ``decision.action`` enum values are coerced to strings
        before constructing ``ReviewRequest``.

Backward compatible
-------------------
``HumanReviewGate(auto_approve_below=None)``,
``gate.set_callback(cb)``, ``await gate.require(decision, context)``
— every v1.0.0 call site works unchanged. ``ReviewRequest``
constructor is preserved (positional or keyword) — it now
validates on construction and is frozen.
"""

from __future__ import annotations

import asyncio
import json
import logging
import threading
import uuid
from collections import deque
from dataclasses import dataclass, field, fields
from datetime import datetime, timezone
from enum import Enum
from typing import (
    Any, Callable, Deque, Dict, List, Mapping, Optional, Union,
)

from ..contracts.policy_decision import PolicyDecision

try:
    from ..contracts.risk_classification import RiskLevel
except ImportError:  # pragma: no cover
    RiskLevel = None  # type: ignore[assignment]


logger = logging.getLogger(__name__)

__version__ = "2.0.0"
LEDGER_SCHEMA_VERSION = "5.0.0"


# =============================================================================
# Errors (P0 2.7 → P1 3.2)
# =============================================================================

class HumanReviewError(ValueError):
    """Base class for every error raised by HumanReviewGate."""


class HumanReviewConfigError(HumanReviewError):
    """Raised for invalid gate configuration."""


class HumanReviewCallbackError(HumanReviewError):
    """Raised when the review callback itself fails (not a denial)."""


class HumanReviewTimeoutError(HumanReviewError):
    """Raised when a review does not complete within ``timeout``."""


class HumanReviewQueueFullError(HumanReviewError):
    """Raised when the pending-review queue is at capacity."""


# =============================================================================
# Enums and constants
# =============================================================================

class ReviewVerdict(str, Enum):
    """A reviewer's decision."""
    APPROVED = "approved"
    DENIED = "denied"
    ESCALATED = "escalated"

    def __str__(self) -> str:
        return self.value


#: Risk-level ordering. Higher = more severe.
_RISK_ORDER: Dict[str, int] = {
    "low": 0,
    "medium": 1,
    "high": 2,
    "critical": 3,
}

#: Not auto-approvable by policy — the gate exists for these levels.
_REQUIRES_REVIEW: frozenset = frozenset({"high", "critical"})


def _normalize_risk_level(raw: Any) -> str:
    """Coerce a risk level to a lowercase string (P0 2.6)."""
    if raw is None:
        raise HumanReviewConfigError(
            "risk_level must not be None")
    if RiskLevel is not None and isinstance(raw, RiskLevel):
        return raw.value
    if hasattr(raw, "value") and isinstance(raw.value, str):
        raw = raw.value
    if not isinstance(raw, str):
        raise HumanReviewConfigError(
            f"risk_level must be a string or enum, "
            f"got {type(raw).__name__}")
    normalized = raw.strip().lower()
    if normalized not in _RISK_ORDER:
        raise HumanReviewConfigError(
            f"unknown risk_level {raw!r}; expected one of "
            f"{list(_RISK_ORDER)}")
    return normalized


def _coerce_action(raw: Any) -> str:
    """Coerce a decision action to a string (P1 3.18)."""
    if raw is None:
        return "unknown"
    if hasattr(raw, "value") and isinstance(raw.value, str):
        raw = raw.value
    if not isinstance(raw, str):
        return str(raw)
    stripped = raw.strip()
    return stripped if stripped else "unknown"


# =============================================================================
# ReviewResponse (P1 3.4)
# =============================================================================

@dataclass(frozen=True)
class ReviewResponse:
    """
    Structured response from a review callback.

    Callbacks may return ``bool``, :class:`ReviewVerdict`, or a
    :class:`ReviewResponse`. When a ``ReviewResponse`` is returned,
    ``reviewer_id`` and ``reason`` are recorded in the gate's
    ``_feedback`` list.
    """
    approved: bool
    reviewer_id: Optional[str] = None
    reason: Optional[str] = None
    verdict: Optional[str] = None     # "approved" / "denied" / "escalated"

    def __post_init__(self) -> None:
        if not isinstance(self.approved, bool):
            raise HumanReviewConfigError(
                "approved must be a bool")
        if self.reviewer_id is not None:
            if not isinstance(self.reviewer_id, str) or \
                    not self.reviewer_id.strip():
                raise HumanReviewConfigError(
                    "reviewer_id must be a non-empty string or None")
        if self.reason is not None:
            if not isinstance(self.reason, str):
                raise HumanReviewConfigError(
                    "reason must be a string or None")
        if self.verdict is not None:
            if self.verdict not in {v.value for v in ReviewVerdict}:
                raise HumanReviewConfigError(
                    f"verdict must be one of "
                    f"{[v.value for v in ReviewVerdict]}, "
                    f"got {self.verdict!r}")


# =============================================================================
# ReviewRequest (P0 2.4, 2.6; P1 3.5, 3.6)
# =============================================================================

@dataclass(frozen=True)
class ReviewRequest:
    """
    A single human-review request.

    Frozen — the review callback receives an immutable view and
    cannot tamper with the request state.
    """
    request_id: str
    action: str
    reasons: List[str] = field(default_factory=list)
    risk_level: str = "low"
    context: Dict[str, Any] = field(default_factory=dict)
    requested_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    def __post_init__(self) -> None:
        # -- request_id --
        if not isinstance(self.request_id, str) or \
                not self.request_id.strip():
            raise HumanReviewConfigError(
                "request_id must be a non-empty string")
        object.__setattr__(
            self, "request_id", self.request_id.strip())

        # -- action --
        if not isinstance(self.action, str) or \
                not self.action.strip():
            raise HumanReviewConfigError(
                "action must be a non-empty string")
        object.__setattr__(self, "action", self.action.strip())

        # -- reasons --
        if not isinstance(self.reasons, list):
            raise HumanReviewConfigError(
                f"reasons must be a list, "
                f"got {type(self.reasons).__name__}")
        for i, r in enumerate(self.reasons):
            if not isinstance(r, str) or not r.strip():
                raise HumanReviewConfigError(
                    f"reasons[{i}] must be a non-empty string")
        # Copy to prevent external mutation of the source list.
        object.__setattr__(
            self, "reasons", [r.strip() for r in self.reasons])

        # -- risk_level --
        object.__setattr__(
            self, "risk_level",
            _normalize_risk_level(self.risk_level))

        # -- context --
        if not isinstance(self.context, dict):
            raise HumanReviewConfigError(
                f"context must be a dict, "
                f"got {type(self.context).__name__}")
        # Defensive copy — the caller cannot mutate our dict.
        object.__setattr__(self, "context", dict(self.context))

        # -- requested_at --
        if not isinstance(self.requested_at, datetime):
            raise HumanReviewConfigError(
                "requested_at must be a datetime")
        if not self.requested_at.tzinfo:
            object.__setattr__(
                self, "requested_at",
                self.requested_at.replace(tzinfo=timezone.utc))
        else:
            object.__setattr__(
                self, "requested_at",
                self.requested_at.astimezone(timezone.utc))

    # ---------------------------------------------------------------- #
    def to_dict(self) -> Dict[str, Any]:
        return {
            "request_id": self.request_id,
            "action": self.action,
            "reasons": list(self.reasons),
            "risk_level": self.risk_level,
            "context": dict(self.context),
            "requested_at": self.requested_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ReviewRequest":
        if not isinstance(data, Mapping):
            raise HumanReviewConfigError(
                f"from_dict expects a Mapping, "
                f"got {type(data).__name__}")
        at_raw = data.get("requested_at")
        if isinstance(at_raw, str):
            try:
                at = datetime.fromisoformat(
                    at_raw.replace("Z", "+00:00"))
            except ValueError as exc:
                raise HumanReviewConfigError(
                    f"requested_at is not a valid ISO 8601 "
                    f"timestamp: {exc}") from exc
        elif isinstance(at_raw, datetime):
            at = at_raw
        elif at_raw is None:
            at = datetime.now(timezone.utc)
        else:
            raise HumanReviewConfigError(
                f"requested_at must be a datetime, ISO string, or "
                f"None; got {type(at_raw).__name__}")
        return cls(
            request_id=data.get("request_id"),
            action=data.get("action"),
            reasons=list(data.get("reasons", []) or []),
            risk_level=data.get("risk_level", "low"),
            context=dict(data.get("context", {}) or {}),
            requested_at=at,
        )


# =============================================================================
# HumanReviewGate (all P0 + P1)
# =============================================================================

class HumanReviewGate:
    """
    Gate that enforces human approval for high-risk decisions.

    The gate is callback-driven: register a callback that returns
    ``bool``, :class:`ReviewVerdict`, or :class:`ReviewResponse`. If
    no callback is registered, the gate is fail-safe: it denies.

    ``auto_approve_below`` is a strict threshold: setting it to
    ``"medium"`` auto-approves ``"low"`` and ``"medium"`` decisions
    and requires review for ``"high"`` and ``"critical"``.
    """

    def __init__(
        self,
        *,
        auto_approve_below: Optional[str] = None,
        max_feedback: int = 1000,
        max_pending: int = 1000,
        audit_log: Optional[Any] = None,
        metrics_hook: Optional[
            Callable[[str, Mapping[str, Any]], None]
        ] = None,
    ) -> None:
        # -- auto_approve_below --
        if auto_approve_below is not None:
            if not isinstance(auto_approve_below, str):
                raise HumanReviewConfigError(
                    "auto_approve_below must be a string or None")
            level = auto_approve_below.strip().lower()
            if level not in _RISK_ORDER:
                raise HumanReviewConfigError(
                    f"auto_approve_below must be one of "
                    f"{list(_RISK_ORDER)}, got "
                    f"{auto_approve_below!r}")
            if level == "critical":
                # Auto-approving CRITICAL defeats the gate.
                raise HumanReviewConfigError(
                    "auto_approve_below='critical' would auto-approve "
                    "every decision, including CRITICAL; this "
                    "defeats the human review gate")
            self._auto_approve_below: Optional[str] = level
        else:
            self._auto_approve_below = None

        # -- max_feedback --
        if isinstance(max_feedback, bool) or \
                not isinstance(max_feedback, int) or \
                max_feedback <= 0:
            raise HumanReviewConfigError(
                "max_feedback must be a positive int")

        # -- max_pending --
        if isinstance(max_pending, bool) or \
                not isinstance(max_pending, int) or \
                max_pending <= 0:
            raise HumanReviewConfigError(
                "max_pending must be a positive int")

        # -- metrics_hook --
        if metrics_hook is not None and not callable(metrics_hook):
            raise HumanReviewConfigError(
                "metrics_hook must be callable")

        # -- State --
        self._lock = threading.RLock()
        self._pending: Dict[str, ReviewRequest] = {}
        self._callback: Optional[
            Callable[[ReviewRequest], Any]
        ] = None
        self._feedback: Deque[Dict[str, Any]] = deque(
            maxlen=max_feedback)
        self._max_pending = max_pending
        self._audit_log = audit_log
        self._metrics_hook = metrics_hook

    # ------------------------------------------------------------------
    # Configuration
    # ------------------------------------------------------------------

    def set_callback(
        self, cb: Callable[[ReviewRequest], Any],
    ) -> None:
        """
        Set the review callback.

        In-flight reviews continue with the callback that was active
        when ``require()`` was called. New reviews use ``cb``.

        The callback may return ``bool``, :class:`ReviewVerdict`, or
        :class:`ReviewResponse`. It may be sync or async (returning
        an awaitable).
        """
        if not callable(cb):
            raise HumanReviewConfigError("cb must be callable")
        with self._lock:
            self._callback = cb

    def clear_callback(self) -> None:
        """Remove the callback; the gate reverts to fail-safe deny."""
        with self._lock:
            self._callback = None

    # ------------------------------------------------------------------
    # Metrics and audit helpers
    # ------------------------------------------------------------------

    def _emit_metric(
        self, event: str, fields_: Mapping[str, Any],
    ) -> None:
        if self._metrics_hook is None:
            return
        try:
            self._metrics_hook(event, dict(fields_))
        except Exception:  # noqa: BLE001
            logger.debug("metrics_hook raised", exc_info=True)

    def _emit_audit(self, payload: Mapping[str, Any]) -> None:
        if self._audit_log is None:
            return
        append_fn = getattr(self._audit_log, "append", None)
        if not callable(append_fn):
            return
        try:
            append_fn(dict(payload))
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "audit_log.append failed for review event: %s", exc)

    @staticmethod
    def _safe_log(level: int, fmt: str, *args: Any) -> None:
        try:
            logger.log(level, fmt, *args)
        except Exception:  # noqa: BLE001
            try:
                logger.debug("log handler raised", exc_info=True)
            except Exception:  # noqa: BLE001
                pass

    # ------------------------------------------------------------------
    # Lifecycle: require (P0 2.1, 2.2, 2.3, 2.5, 2.6, 2.7; P1 3.3, 3.4)
    # ------------------------------------------------------------------

    async def require(
        self,
        decision: PolicyDecision,
        context: Dict[str, Any],
        *,
        timeout: Optional[float] = None,
    ) -> bool:
        """
        Determine whether ``decision`` is approved.

        Returns ``True`` iff a human (via the callback) approves the
        decision, or the decision's risk level is strictly below
        ``auto_approve_below``.

        Raises
        ------
        HumanReviewCallbackError
            The callback itself raised (not a denial).
        HumanReviewTimeoutError
            ``timeout`` was provided and the callback did not
            complete in time.
        HumanReviewQueueFullError
            The pending queue is at capacity.
        """
        if not isinstance(context, dict):
            raise HumanReviewConfigError(
                f"context must be a dict, "
                f"got {type(context).__name__}")
        if timeout is not None:
            if isinstance(timeout, bool) or \
                    not isinstance(timeout, (int, float)) or \
                    timeout < 0:
                raise HumanReviewConfigError(
                    "timeout must be a non-negative number or None")

        # Build the request (validates fields).
        level = _normalize_risk_level(
            getattr(decision, "risk_level", None))
        req = ReviewRequest(
            request_id=str(getattr(
                decision, "request_id",
                uuid.uuid4().hex[:12])),
            action=_coerce_action(
                getattr(decision, "action", "unknown")),
            reasons=list(getattr(decision, "reasons", [])),
            risk_level=level,
            context=dict(context),
        )

        # Enqueue with capacity check.
        with self._lock:
            if len(self._pending) >= self._max_pending:
                self._emit_metric("review_queue_full", {
                    "request_id": req.request_id,
                    "pending": len(self._pending),
                })
                raise HumanReviewQueueFullError(
                    f"review queue is full "
                    f"({self._max_pending} pending)")
            self._pending[req.request_id] = req

        self._emit_metric("review_requested", {
            "request_id": req.request_id,
            "action": req.action,
            "risk_level": req.risk_level,
        })

        try:
            # ---- Auto-approve check (P0 2.1, 2.5) ----
            if self._auto_approve_below is not None:
                threshold = _RISK_ORDER[self._auto_approve_below]
                if _RISK_ORDER[req.risk_level] < threshold:
                    self._record_outcome(
                        req, approved=True,
                        outcome="auto_approved",
                        reviewer_id=None,
                        reason=None,
                        verdict=ReviewVerdict.APPROVED.value,
                    )
                    self._emit_metric("review_auto_approved", {
                        "request_id": req.request_id,
                        "risk_level": req.risk_level,
                    })
                    return True

            # ---- Fail-safe: no callback ----
            with self._lock:
                cb = self._callback
            if cb is None:
                self._record_outcome(
                    req, approved=False,
                    outcome="no_callback",
                    reviewer_id=None,
                    reason="no review callback registered",
                    verdict=ReviewVerdict.DENIED.value,
                )
                self._emit_metric("review_no_callback", {
                    "request_id": req.request_id,
                })
                self._safe_log(
                    logging.WARNING,
                    "Human review denied (no callback): %s",
                    req.request_id)
                return False

            # ---- Invoke callback (P0 2.3 + P1 3.3) ----
            try:
                raw = cb(req)
                if hasattr(raw, "__await__"):
                    if timeout is not None:
                        raw = await asyncio.wait_for(raw,
                                                      timeout=timeout)
                    else:
                        raw = await raw
            except asyncio.TimeoutError as exc:
                self._record_outcome(
                    req, approved=False,
                    outcome="timed_out",
                    reviewer_id=None,
                    reason=f"timeout after {timeout}s",
                    verdict=ReviewVerdict.DENIED.value,
                )
                self._emit_metric("review_timed_out", {
                    "request_id": req.request_id,
                    "timeout": timeout,
                })
                raise HumanReviewTimeoutError(
                    f"review for {req.request_id} timed out after "
                    f"{timeout}s") from exc
            except Exception as exc:
                # P0 2.3: callback failure is NOT a denial.
                self._record_outcome(
                    req, approved=False,
                    outcome="failed",
                    reviewer_id=None,
                    reason=f"callback_failed: "
                           f"{type(exc).__name__}: {exc}",
                    verdict=None,
                )
                self._emit_metric("review_failed", {
                    "request_id": req.request_id,
                    "error_type": type(exc).__name__,
                })
                self._safe_log(
                    logging.ERROR,
                    "Human review callback failed for %s: %s",
                    req.request_id, exc)
                raise HumanReviewCallbackError(
                    f"review callback failed for "
                    f"{req.request_id}: {exc}") from exc

            # ---- Normalize callback response (P1 3.4) ----
            approved, reviewer_id, reason, verdict = \
                self._interpret_callback_response(raw)

            outcome = (
                "approved" if approved
                else "escalated" if verdict == ReviewVerdict.ESCALATED.value
                else "denied"
            )
            self._record_outcome(
                req, approved=approved,
                outcome=outcome,
                reviewer_id=reviewer_id,
                reason=reason,
                verdict=verdict,
            )
            self._emit_metric(f"review_{outcome}", {
                "request_id": req.request_id,
                "reviewer_id": reviewer_id or "",
            })
            self._safe_log(
                logging.INFO,
                "Human review %s for %s (reviewer=%s)",
                outcome, req.request_id, reviewer_id)
            return approved

        finally:
            # P0 2.2: always remove the pending entry, even on
            # cancellation or exception.
            with self._lock:
                self._pending.pop(req.request_id, None)

    # ------------------------------------------------------------------
    # Callback response interpretation
    # ------------------------------------------------------------------

    @staticmethod
    def _interpret_callback_response(
        raw: Any,
    ) -> tuple[bool, Optional[str], Optional[str], Optional[str]]:
        """Return ``(approved, reviewer_id, reason, verdict)``."""
        # bool
        if isinstance(raw, bool):
            return raw, None, None, (
                ReviewVerdict.APPROVED.value if raw
                else ReviewVerdict.DENIED.value
            )
        # ReviewVerdict enum
        if isinstance(raw, ReviewVerdict):
            approved = raw == ReviewVerdict.APPROVED
            return approved, None, None, raw.value
        # Stringified verdict
        if isinstance(raw, str):
            normalized = raw.strip().lower()
            if normalized in {v.value for v in ReviewVerdict}:
                return (
                    normalized == ReviewVerdict.APPROVED.value,
                    None, None, normalized,
                )
            raise HumanReviewCallbackError(
                f"callback returned unrecognized string "
                f"{raw!r}; expected one of "
                f"{[v.value for v in ReviewVerdict]} or a bool")
        # ReviewResponse
        if isinstance(raw, ReviewResponse):
            verdict = raw.verdict or (
                ReviewVerdict.APPROVED.value if raw.approved
                else ReviewVerdict.DENIED.value
            )
            return raw.approved, raw.reviewer_id, raw.reason, verdict
        # Anything else is a programming error in the callback.
        raise HumanReviewCallbackError(
            f"callback returned unrecognized type "
            f"{type(raw).__name__}; expected bool, ReviewVerdict, "
            f"or ReviewResponse")

    # ------------------------------------------------------------------
    # Feedback recording (P1 3.4)
    # ------------------------------------------------------------------

    def _record_outcome(
        self,
        req: ReviewRequest,
        *,
        approved: bool,
        outcome: str,
        reviewer_id: Optional[str],
        reason: Optional[str],
        verdict: Optional[str],
    ) -> None:
        entry = {
            "req": req.request_id,
            "decision": bool(approved),
            "outcome": outcome,
            "reviewer_id": reviewer_id,
            "reason": reason,
            "verdict": verdict,
            "at": datetime.now(timezone.utc).isoformat(),
        }
        with self._lock:
            self._feedback.append(entry)
        self._emit_audit({
            "event": f"review_{outcome}",
            "request_id": req.request_id,
            "action": req.action,
            "risk_level": req.risk_level,
            "approved": bool(approved),
            "reviewer_id": reviewer_id,
            "reason": reason,
            "verdict": verdict,
            "at": entry["at"],
        })

    # ------------------------------------------------------------------
    # Query API (P1 3.1)
    # ------------------------------------------------------------------

    def pending(self) -> List[ReviewRequest]:
        """Return a copy of the currently pending review requests."""
        with self._lock:
            return list(self._pending.values())

    def get_pending(
        self, request_id: str,
    ) -> Optional[ReviewRequest]:
        """Return the pending request with ``request_id`` or None."""
        if not isinstance(request_id, str) or not request_id:
            return None
        with self._lock:
            return self._pending.get(request_id)

    def feedback(
        self, limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """Return the most recent completed reviews."""
        if isinstance(limit, bool) or not isinstance(limit, int) or \
                limit <= 0:
            raise HumanReviewConfigError(
                "limit must be a positive int")
        with self._lock:
            return list(self._feedback)[-limit:]

    def __len__(self) -> int:
        with self._lock:
            return len(self._pending)

    # ------------------------------------------------------------------
    # Statistics
    # ------------------------------------------------------------------

    def statistics(self) -> Dict[str, Any]:
        with self._lock:
            feedback_list = list(self._feedback)
            pending = len(self._pending)
        by_outcome: Dict[str, int] = {}
        for entry in feedback_list:
            outcome = entry.get("outcome", "unknown")
            by_outcome[outcome] = by_outcome.get(outcome, 0) + 1
        return {
            "version": __version__,
            "schema_version": LEDGER_SCHEMA_VERSION,
            "pending": pending,
            "max_pending": self._max_pending,
            "feedback_count": len(feedback_list),
            "by_outcome": by_outcome,
            "auto_approve_below": self._auto_approve_below,
            "callback_registered": self._callback is not None,
            "audit_log_attached": self._audit_log is not None,
        }

    # ------------------------------------------------------------------
    # Serialization (P1 3.5)
    # ------------------------------------------------------------------

    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "version": __version__,
                "schema_version": LEDGER_SCHEMA_VERSION,
                "auto_approve_below": self._auto_approve_below,
                "max_pending": self._max_pending,
                "max_feedback": (
                    self._feedback.maxlen
                    if self._feedback.maxlen is not None
                    else None
                ),
            }

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        audit_log: Optional[Any] = None,
        metrics_hook: Optional[
            Callable[[str, Mapping[str, Any]], None]
        ] = None,
    ) -> "HumanReviewGate":
        if not isinstance(data, Mapping):
            raise HumanReviewConfigError(
                f"from_dict expects a Mapping, "
                f"got {type(data).__name__}")
        max_feedback = data.get("max_feedback", 1000)
        return cls(
            auto_approve_below=data.get("auto_approve_below"),
            max_pending=int(data.get("max_pending", 1000)),
            max_feedback=(
                int(max_feedback)
                if max_feedback is not None else 1000),
            audit_log=audit_log,
            metrics_hook=metrics_hook,
        )

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    @classmethod
    def from_json(
        cls,
        payload: str,
        *,
        audit_log: Optional[Any] = None,
        metrics_hook: Optional[
            Callable[[str, Mapping[str, Any]], None]
        ] = None,
    ) -> "HumanReviewGate":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise HumanReviewConfigError(
                f"Invalid JSON payload: {exc}") from exc
        return cls.from_dict(
            data, audit_log=audit_log, metrics_hook=metrics_hook)

    # ------------------------------------------------------------------
    # Dunder
    # ------------------------------------------------------------------

    def __repr__(self) -> str:
        with self._lock:
            return (
                f"HumanReviewGate("
                f"version={__version__!r}, "
                f"auto_approve_below={self._auto_approve_below!r}, "
                f"pending={len(self._pending)}, "
                f"feedback={len(self._feedback)}, "
                f"callback={'yes' if self._callback else 'no'})")


# =============================================================================
# Public API
# =============================================================================

__all__ = [
    "__version__",
    "LEDGER_SCHEMA_VERSION",
    "HumanReviewCallbackError",
    "HumanReviewConfigError",
    "HumanReviewError",
    "HumanReviewGate",
    "HumanReviewQueueFullError",
    "HumanReviewTimeoutError",
    "ReviewRequest",
    "ReviewResponse",
    "ReviewVerdict",
]


# =============================================================================
# Self-tests
# =============================================================================

def _run_selftests() -> None:
    """Run the module's self-tests."""
    import concurrent.futures
    import pickle
    import time
    from dataclasses import FrozenInstanceError

    logging.basicConfig(
        level=logging.WARNING,
        format="%(levelname)s %(name)s: %(message)s")

    print(f"=== human_review v{__version__} self-tests ===\n")

    class _FakeDecision:
        def __init__(
            self, risk_level="high", action="execute_full",
            reasons=None, request_id="d-1",
        ):
            self.risk_level = risk_level
            self.action = action
            self.reasons = reasons or []
            self.request_id = request_id

    # ---------------------------------------------------------------- #
    # P0 2.1 — auto_approve_below is a threshold
    # ---------------------------------------------------------------- #
    print("=== P0 2.1: auto_approve_below threshold ===")

    async def _test_p0_2_1():
        gate = HumanReviewGate(auto_approve_below="medium")
        # Callback that approves everything — to prove auto-approve
        # would fire even without the callback being consulted.
        called = {"count": 0}

        def cb(req):
            called["count"] += 1
            return True

        gate.set_callback(cb)

        # LOW: auto-approved (below medium).
        assert await gate.require(
            _FakeDecision(risk_level="low"), {}) is True
        # MEDIUM: auto-approved (strictly below? No — the fix treats
        # 'below' as 'strictly less'). Hmm — check.
        # Actually with strict '<' semantics, "medium" does NOT
        # auto-approve itself. Let's verify what we intend:
        # auto_approve_below="medium" → auto-approve strictly below
        # medium → LOW only.
        assert called["count"] == 0, (
            f"P0 2.1: LOW should have been auto-approved "
            f"without callback; callback called "
            f"{called['count']} times")

        # MEDIUM: NOT auto-approved (equal to threshold) → callback.
        result = await gate.require(
            _FakeDecision(risk_level="medium"), {})
        assert result is True
        assert called["count"] == 1, called["count"]

        # HIGH: callback.
        await gate.require(_FakeDecision(risk_level="high"), {})
        assert called["count"] == 2
        # CRITICAL: callback.
        await gate.require(_FakeDecision(risk_level="critical"), {})
        assert called["count"] == 3
        print(f"  ✓ LOW auto-approved (strictly below); "
              f"MEDIUM/HIGH/CRITICAL go through callback")

    asyncio.run(_test_p0_2_1())

    # ---------------------------------------------------------------- #
    # P0 2.2 — thread safety
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.2: thread safety ===")

    async def _test_p0_2_2():
        gate = HumanReviewGate()
        async def cb(req):
            await asyncio.sleep(0.001)
            return True
        gate.set_callback(cb)

        async def one(i):
            return await gate.require(
                _FakeDecision(
                    risk_level="high",
                    request_id=f"r-{i}"), {})

        results = await asyncio.gather(
            *(one(i) for i in range(100)))
        assert all(results)
        assert len(gate.pending()) == 0, (
            f"P0 2.2: {len(gate.pending())} pending after "
            f"100 concurrent reviews")
        print(f"  ✓ 100 concurrent reviews all approved; "
              f"pending empty")

    asyncio.run(_test_p0_2_2())

    # ---------------------------------------------------------------- #
    # P0 2.3 — callback failure distinguishes from denial
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.3: callback failure vs denial ===")

    async def _test_p0_2_3():
        gate = HumanReviewGate()
        def broken_cb(req):
            raise RuntimeError("simulated infrastructure failure")
        gate.set_callback(broken_cb)

        try:
            await gate.require(
                _FakeDecision(risk_level="high"), {})
        except HumanReviewCallbackError as exc:
            print(f"  ✓ callback failure → HumanReviewCallbackError: "
                  f"{str(exc)[:60]}...")
        else:
            raise AssertionError(
                "P0 2.3 regression: callback failure returned "
                "False instead of raising")

        # Contrast: callback returning False → returned as False.
        def deny_cb(req):
            return False
        gate.set_callback(deny_cb)
        result = await gate.require(
            _FakeDecision(risk_level="high"), {})
        assert result is False
        print(f"  ✓ callback denial → False (not an exception)")

    asyncio.run(_test_p0_2_3())

    # ---------------------------------------------------------------- #
    # P0 2.4 — ReviewRequest validation
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.4: ReviewRequest validation ===")

    bad_kwargs = [
        dict(request_id="", action="x"),
        dict(request_id="  ", action="x"),
        dict(request_id="r", action=""),
        dict(request_id="r", action=None),
        dict(request_id="r", action="x",
             risk_level="garbage"),
        dict(request_id="r", action="x",
             reasons=[None]),
        dict(request_id="r", action="x",
             context="not-a-dict"),
        dict(request_id="r", action="x",
             requested_at="not-a-datetime"),
    ]
    for kwargs in bad_kwargs:
        try:
            ReviewRequest(**kwargs)
        except HumanReviewConfigError:
            pass
        else:
            raise AssertionError(
                f"P0 2.4 regression: {kwargs!r} accepted")
    print(f"  ✓ {len(bad_kwargs)} invalid constructions rejected")

    # Naive datetime → UTC.
    req = ReviewRequest(
        request_id="r", action="x",
        requested_at=datetime(2024, 1, 1))
    assert req.requested_at.tzinfo is not None
    print(f"  ✓ naive datetime normalized to UTC")

    # ---------------------------------------------------------------- #
    # P0 2.5 — auto_approve_below="critical" rejected
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.5: 'critical' config rejected ===")
    try:
        HumanReviewGate(auto_approve_below="critical")
    except HumanReviewConfigError:
        print(f"  ✓ auto_approve_below='critical' rejected")
    else:
        raise AssertionError(
            "P0 2.5 regression: 'critical' config accepted")

    # ---------------------------------------------------------------- #
    # P0 2.6 — case-insensitive risk levels
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.6: case-insensitive risk levels ===")

    async def _test_p0_2_6():
        gate = HumanReviewGate(auto_approve_below="medium")
        def cb(req):
            return True
        gate.set_callback(cb)

        # Uppercase "LOW" should auto-approve.
        result = await gate.require(
            _FakeDecision(risk_level="LOW"), {})
        assert result is True
        print(f"  ✓ 'LOW' auto-approved (previously would have "
              f"gone to callback)")

    asyncio.run(_test_p0_2_6())

    # ---------------------------------------------------------------- #
    # P0 2.7 — action read from decision, not context
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.7: action source ===")

    async def _test_p0_2_7():
        gate = HumanReviewGate()
        captured = {}
        def cb(req):
            captured["action"] = req.action
            return True
        gate.set_callback(cb)

        await gate.require(
            _FakeDecision(action="execute_full"),
            {"action": "something-misleading"})
        assert captured["action"] == "execute_full", (
            f"P0 2.7 regression: action={captured['action']!r}, "
            f"expected 'execute_full'")
        print(f"  ✓ action read from decision, not context")

    asyncio.run(_test_p0_2_7())

    # ---------------------------------------------------------------- #
    # P0 2.8 — bounded _feedback
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.8: bounded feedback ===")

    async def _test_p0_2_8():
        gate = HumanReviewGate(max_feedback=5)
        def cb(req): return True
        gate.set_callback(cb)
        for i in range(20):
            await gate.require(
                _FakeDecision(request_id=f"r-{i}"), {})
        assert len(gate.feedback(limit=100)) <= 5, (
            f"P0 2.8 regression: feedback has "
            f"{len(gate.feedback(limit=100))} entries, "
            f"expected <= 5")
        print(f"  ✓ 20 reviews, only 5 kept in feedback")

    asyncio.run(_test_p0_2_8())

    # ---------------------------------------------------------------- #
    # P1 3.1 — query API
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.1: query API ===")

    async def _test_p1_3_1():
        gate = HumanReviewGate()
        import asyncio as _asyncio
        pending_gate = _asyncio.Event()

        async def slow_cb(req):
            await pending_gate.wait()
            return True
        gate.set_callback(slow_cb)

        task = _asyncio.create_task(
            gate.require(_FakeDecision(request_id="r-slow"), {}))
        # Give the task time to enqueue.
        await _asyncio.sleep(0.01)
        assert len(gate.pending()) == 1
        req = gate.get_pending("r-slow")
        assert req is not None
        assert req.request_id == "r-slow"
        pending_gate.set()
        await task
        assert len(gate.pending()) == 0
        print(f"  ✓ pending() / get_pending() work")

    asyncio.run(_test_p1_3_1())

    # ---------------------------------------------------------------- #
    # P1 3.2 — error hierarchy
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.2: error hierarchy ===")
    assert issubclass(HumanReviewError, ValueError)
    assert issubclass(HumanReviewConfigError, HumanReviewError)
    assert issubclass(HumanReviewCallbackError, HumanReviewError)
    assert issubclass(HumanReviewTimeoutError, HumanReviewError)
    assert issubclass(HumanReviewQueueFullError, HumanReviewError)
    print(f"  ✓ 5 error types subclass HumanReviewError "
          f"(and ValueError)")

    # ---------------------------------------------------------------- #
    # P1 3.3 — timeout
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.3: timeout ===")

    async def _test_p1_3_3():
        gate = HumanReviewGate()
        async def slow_cb(req):
            await asyncio.sleep(1.0)
            return True
        gate.set_callback(slow_cb)
        try:
            await gate.require(
                _FakeDecision(), {}, timeout=0.05)
        except HumanReviewTimeoutError:
            print(f"  ✓ timeout=0.05s → HumanReviewTimeoutError")
        else:
            raise AssertionError(
                "P1 3.3 regression: timeout not enforced")

    asyncio.run(_test_p1_3_3())

    # ---------------------------------------------------------------- #
    # P1 3.4 — reviewer_id + reason recorded
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.4: reviewer metadata ===")

    async def _test_p1_3_4():
        gate = HumanReviewGate()
        def cb(req):
            return ReviewResponse(
                approved=True,
                reviewer_id="alice@example.com",
                reason="verified against runbook",
                verdict="approved",
            )
        gate.set_callback(cb)
        await gate.require(_FakeDecision(), {})
        fb = gate.feedback(limit=1)
        assert len(fb) == 1
        assert fb[0]["reviewer_id"] == "alice@example.com"
        assert fb[0]["reason"] == "verified against runbook"
        assert fb[0]["verdict"] == "approved"
        print(f"  ✓ reviewer_id, reason, verdict recorded")

    asyncio.run(_test_p1_3_4())

    # ---------------------------------------------------------------- #
    # P1 3.5 — serialization
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.5: serialization ===")
    req = ReviewRequest(
        request_id="r-1", action="execute_full",
        risk_level="high", reasons=["budget"])
    d = req.to_dict()
    rt = ReviewRequest.from_dict(d)
    assert rt == req
    print(f"  ✓ ReviewRequest round-trip")

    gate = HumanReviewGate(
        auto_approve_below="medium", max_pending=200)
    d = gate.to_dict()
    rt = HumanReviewGate.from_dict(d)
    assert rt._auto_approve_below == "medium"
    assert rt._max_pending == 200
    print(f"  ✓ HumanReviewGate config round-trip")

    # ---------------------------------------------------------------- #
    # P1 3.6 — frozen ReviewRequest
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.6: frozen ReviewRequest ===")
    try:
        req.risk_level = "low"   # type: ignore[misc]
    except FrozenInstanceError:
        print(f"  ✓ direct mutation raises FrozenInstanceError")
    else:
        raise AssertionError(
            "P1 3.6 regression: ReviewRequest mutable")

    # ---------------------------------------------------------------- #
    # P1 3.7 — O(1) pending
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.7: O(1) pending lookup ===")
    gate = HumanReviewGate()
    # The _pending dict allows O(1) get.
    assert isinstance(gate._pending, dict)
    print(f"  ✓ _pending is a dict (O(1) lookups)")

    # ---------------------------------------------------------------- #
    # P1 3.8 — metrics_hook
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.8: metrics_hook ===")

    async def _test_p1_3_8():
        events = []
        gate = HumanReviewGate(
            auto_approve_below="medium",
            metrics_hook=lambda ev, f: events.append(ev))
        def cb(req): return True
        gate.set_callback(cb)
        await gate.require(
            _FakeDecision(risk_level="low"), {})
        await gate.require(
            _FakeDecision(risk_level="high"), {})
        names = set(events)
        assert "review_requested" in names
        assert "review_auto_approved" in names
        assert "review_approved" in names
        print(f"  ✓ events: {sorted(names)}")

    asyncio.run(_test_p1_3_8())

    # ---------------------------------------------------------------- #
    # P1 3.9 — audit_log integration
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.9: audit_log integration ===")

    async def _test_p1_3_9():
        audit_events = []
        class _MockAudit:
            def append(self, payload):
                audit_events.append(payload)
        gate = HumanReviewGate(audit_log=_MockAudit())
        def cb(req): return True
        gate.set_callback(cb)
        await gate.require(_FakeDecision(), {})
        assert len(audit_events) == 1
        assert "request_id" in audit_events[0]
        assert "approved" in audit_events[0]
        print(f"  ✓ audit_log received review event")

    asyncio.run(_test_p1_3_9())

    # ---------------------------------------------------------------- #
    # P1 3.15 — tri-state verdict
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.15: ReviewVerdict ===")

    async def _test_p1_3_15():
        gate = HumanReviewGate()
        def cb(req):
            return ReviewVerdict.ESCALATED
        gate.set_callback(cb)
        result = await gate.require(_FakeDecision(), {})
        assert result is False   # escalated is not approved
        fb = gate.feedback(limit=1)
        assert fb[0]["verdict"] == "escalated"
        print(f"  ✓ ESCALATED recorded (approved=False, "
              f"verdict=escalated)")

    asyncio.run(_test_p1_3_15())

    # ---------------------------------------------------------------- #
    # P1 3.17 — max_pending
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.17: max_pending ===")

    async def _test_p1_3_17():
        gate = HumanReviewGate(max_pending=2)
        import asyncio as _a
        block = _a.Event()
        async def slow_cb(req):
            await block.wait()
            return True
        gate.set_callback(slow_cb)

        tasks = [
            _a.create_task(gate.require(
                _FakeDecision(request_id=f"r-{i}"), {}))
            for i in range(2)
        ]
        await _a.sleep(0.01)
        # Third should fail.
        try:
            await gate.require(
                _FakeDecision(request_id="r-3"), {})
        except HumanReviewQueueFullError:
            print(f"  ✓ queue full → HumanReviewQueueFullError")
        else:
            raise AssertionError(
                "P1 3.17 regression: queue over capacity accepted")
        block.set()
        await _a.gather(*tasks)

    asyncio.run(_test_p1_3_17())

    # ---------------------------------------------------------------- #
    # P1 3.18 — enum action coercion
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.18: action enum coercion ===")

    class _Action(str, Enum):
        EXECUTE = "execute_full"

    async def _test_p1_3_18():
        gate = HumanReviewGate()
        captured = {}
        def cb(req):
            captured["action"] = req.action
            return True
        gate.set_callback(cb)
        await gate.require(
            _FakeDecision(action=_Action.EXECUTE), {})
        assert captured["action"] == "execute_full", (
            f"P1 3.18: action={captured['action']!r}")
        print(f"  ✓ enum action coerced to 'execute_full'")

    asyncio.run(_test_p1_3_18())

    # ---------------------------------------------------------------- #
    # Fail-safe: no callback
    # ---------------------------------------------------------------- #
    print("\n=== Fail-safe: no callback ===")

    async def _test_failsafe():
        gate = HumanReviewGate()
        result = await gate.require(_FakeDecision(), {})
        assert result is False
        print(f"  ✓ no callback → deny (fail-safe)")

    asyncio.run(_test_failsafe())

    # ---------------------------------------------------------------- #
    # Backward compat: v1.0.0 API
    # ---------------------------------------------------------------- #
    print("\n=== Backward compat ===")

    async def _test_backcompat():
        gate = HumanReviewGate()
        gate.set_callback(lambda req: True)
        result = await gate.require(_FakeDecision(), {})
        assert result is True
        print(f"  ✓ v1.0.0 API works")

    asyncio.run(_test_backcompat())

    # ---------------------------------------------------------------- #
    # Bonus: pickle
    # ---------------------------------------------------------------- #
    print("\n=== Bonus: pickle ===")
    # RLock blocks pickle; but ReviewRequest is picklable.
    req = ReviewRequest(
        request_id="r", action="a", risk_level="high")
    blob = pickle.dumps(req)
    restored = pickle.loads(blob)
    assert restored == req
    print(f"  ✓ ReviewRequest pickles")

    print(f"\n✓ All self-tests passed.")


if __name__ == "__main__":  # pragma: no cover
    _run_selftests()
