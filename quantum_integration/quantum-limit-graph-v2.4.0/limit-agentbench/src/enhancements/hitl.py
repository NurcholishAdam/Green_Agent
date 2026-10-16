#!/usr/bin/env python3
# src/enhancements/hitl.py
# Version 18.0 — HITL + Active Learning (P0/P1/P2 hardened)
"""
hitl.py — Human-in-the-loop and active learning (Enhancement 10).

P0 fixes over v1:
 - `timeout_s` and `auto_approve_on_timeout` are actually honored
 - production-safe default: fail closed when no approver is configured
 - pending-request registry with external `resolve()`
 - thread-safe shared state (locks in UncertaintyEstimator, gate, sampler)
 - `is_anomalous` exposes two-sided vs one-sided semantics
 - `composite_uncertainty` averages over ALL keys, including zero-variance ones
 - `ActiveLearningSampler` is uncertainty-weighted, seeded, and evicts by
   uncertainty rather than FIFO

P1 hardening:
 - async bridge (`request_async`, `resolve_async`)
 - Prometheus metrics for HITL decisions, pending count, latency, overflow
 - structured `HITLAuditEntry` with mode, latency, role
 - request priority queue
 - optional safety-monitor coupling (duck-typed)
 - token-bucket rate limiting
 - audit persistence

P2 advanced:
 - per-expert model disagreement
 - predictive entropy
 - split-conformal calibration for prediction intervals
 - uncertainty calibration via Platt scaling (sklearn, optional)
 - diversity-aware sampling via KMeans (sklearn, optional)
 - hash-chained audit ledger with tamper detection
 - HITL state machine (pending -> resolved -> archived)
 - multi-tier approval (role registry)
 - backpressure on the pending queue
 - time-bounded active learning buffer

This module has no dependency on the larger green-agent files; it is a
self-contained drop-in for the async and sync stacks.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import hashlib
import json
import logging
import math
import os
import pickle
import random
import threading
import time
import uuid
from collections import deque
from dataclasses import dataclass, field, asdict
from enum import Enum
from pathlib import Path
from typing import (
    Any, Awaitable, Callable, Dict, Iterable, List, Optional, Protocol,
    Sequence, Tuple, Union, runtime_checkable,
)

import numpy as np

# --------------------------------------------------------------------
# Optional dependencies
# --------------------------------------------------------------------
try:
    from sklearn.linear_model import LogisticRegression
    from sklearn.cluster import KMeans
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

try:
    from prometheus_client import Counter, Gauge, Histogram, CollectorRegistry
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False


logger = logging.getLogger(__name__)
if not logging.getLogger().handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")


# --------------------------------------------------------------------
# Prometheus metrics (with dummy fallbacks)
# --------------------------------------------------------------------
if PROMETHEUS_AVAILABLE:
    REGISTRY = CollectorRegistry()
    HITL_APPROVALS = Counter(
        "hitl_approvals_total", "HITL decisions", ["decision"], registry=REGISTRY)
    HITL_PENDING = Gauge(
        "hitl_pending", "Pending HITL requests", registry=REGISTRY)
    HITL_LATENCY = Histogram(
        "hitl_latency_seconds", "HITL request latency", registry=REGISTRY)
    HITL_RATE_LIMITED = Counter(
        "hitl_rate_limited_total", "Rate-limited HITL calls", registry=REGISTRY)
    HITL_QUEUE_OVERFLOW = Counter(
        "hitl_queue_overflow_total", "Pending queue overflows", registry=REGISTRY)
    HITL_AUDIT_LEN = Gauge(
        "hitl_audit_length", "Audit ledger length", registry=REGISTRY)
    ACTIVE_LEARNING_STORED = Counter(
        "active_learning_stored_total", "Stored samples", registry=REGISTRY)
    ACTIVE_LEARNING_EVICTED = Counter(
        "active_learning_evicted_total", "Evicted samples", registry=REGISTRY)
    ACTIVE_LEARNING_SIZE = Gauge(
        "active_learning_buffer_size", "Buffer size", registry=REGISTRY)
else:
    class _Dummy:
        def inc(self, *a, **kw): pass
        def set(self, *a, **kw): pass
        def observe(self, *a, **kw): pass
        def labels(self, *a, **kw): return self
    HITL_APPROVALS = HITL_PENDING = HITL_LATENCY = _Dummy()
    HITL_RATE_LIMITED = HITL_QUEUE_OVERFLOW = HITL_AUDIT_LEN = _Dummy()
    ACTIVE_LEARNING_STORED = ACTIVE_LEARNING_EVICTED = ACTIVE_LEARNING_SIZE = _Dummy()


# --------------------------------------------------------------------
# Exceptions
# --------------------------------------------------------------------
class HITLError(Exception):
    """Base class for HITL errors."""


class HITLTimeout(HITLError):
    """Raised when an approval request times out."""


class HITLRateLimited(HITLError):
    """Raised when the rate limiter rejects a request."""


class HITLQueueFull(HITLError):
    """Raised when the pending queue is at capacity."""


class HITLNoApprover(HITLError):
    """Raised when no approver is configured and production-safe is on."""


# --------------------------------------------------------------------
# Enums
# --------------------------------------------------------------------
class HITLMode(str, Enum):
    APPROVER = "approver"
    AUTO = "auto"
    TIMEOUT = "timeout"
    BLOCKED = "blocked"
    ERROR = "error"
    RESOLVED = "resolved"
    QUEUE_FULL = "queue_full"
    RATE_LIMITED = "rate_limited"


class HITLState(str, Enum):
    PENDING = "pending"
    RESOLVED = "resolved"
    ARCHIVED = "archived"


# --------------------------------------------------------------------
# Dataclasses
# --------------------------------------------------------------------
@dataclass
class HITLRequest:
    """
    An approval request.

    The original positional signature (request_id, reason, metric_name,
    value, uncertainty, carbon_price, timestamp) is preserved; the extra
    fields have defaults.
    """
    request_id: str
    reason: str
    metric_name: str
    value: float
    uncertainty: float
    carbon_price: float = 0.0
    priority: int = 0
    requires_role: Optional[str] = None
    candidates: List[Dict[str, Any]] = field(default_factory=list)
    chosen_idx: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)

    @classmethod
    def new(
        cls,
        reason: str,
        metric_name: str,
        value: float,
        uncertainty: float,
        carbon_price: float = 0.0,
        priority: int = 0,
        requires_role: Optional[str] = None,
        candidates: Optional[List[Dict[str, Any]]] = None,
        chosen_idx: int = 0,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> "HITLRequest":
        return cls(
            request_id=uuid.uuid4().hex[:12],
            reason=reason,
            metric_name=metric_name,
            value=float(value),
            uncertainty=float(uncertainty),
            carbon_price=float(carbon_price),
            priority=int(priority),
            requires_role=requires_role,
            candidates=list(candidates or []),
            chosen_idx=int(chosen_idx),
            metadata=dict(metadata or {}),
        )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class HITLAuditEntry:
    request_id: str
    approved: bool
    reason: str
    mode: str
    latency_ms: float = 0.0
    role: Optional[str] = None
    prev_hash: str = ""
    entry_hash: str = ""
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def compute_hash(self) -> str:
        payload = json.dumps({
            "request_id": self.request_id,
            "approved": bool(self.approved),
            "reason": self.reason,
            "mode": self.mode,
            "latency_ms": round(float(self.latency_ms), 6),
            "role": self.role,
            "timestamp": round(float(self.timestamp), 6),
            "prev_hash": self.prev_hash,
        }, sort_keys=True, default=str)
        return hashlib.sha256(payload.encode()).hexdigest()


# --------------------------------------------------------------------
# Hash-chained audit ledger
# --------------------------------------------------------------------
class HITLAuditLedger:
    """
    Append-only, hash-chained audit ledger with optional persistence.

    Each entry stores `prev_hash` (the previous entry's hash) and
    `entry_hash` (SHA-256 over the entry's canonical fields plus prev_hash).
    Tampering with an entry breaks the chain.
    """

    def __init__(self, capacity: int = 4096, persist_path: Optional[str] = None):
        self.capacity = int(capacity)
        self.persist_path = Path(persist_path) if persist_path else None
        self._entries: deque = deque(maxlen=self.capacity)
        self._lock = threading.Lock()
        self._load()
        HITL_AUDIT_LEN.set(len(self._entries))

    # -- persistence --
    def _load(self):
        if self.persist_path and self.persist_path.exists():
            try:
                with open(self.persist_path, "rb") as f:
                    data = pickle.load(f)
                entries = data.get("entries", [])
                if isinstance(entries, list):
                    self._entries = deque(entries, maxlen=self.capacity)
            except Exception as e:
                logger.warning(f"Audit ledger load failed: {e}")

    def _save(self):
        if not self.persist_path:
            return
        try:
            self.persist_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.persist_path, "wb") as f:
                pickle.dump({"entries": list(self._entries)}, f)
        except Exception as e:
            logger.warning(f"Audit ledger save failed: {e}")

    # -- API --
    def append(self, entry: HITLAuditEntry) -> HITLAuditEntry:
        with self._lock:
            prev_hash = self._entries[-1].entry_hash if self._entries else ""
            entry.prev_hash = prev_hash
            entry.entry_hash = entry.compute_hash()
            self._entries.append(entry)
            self._save()
        HITL_AUDIT_LEN.set(len(self._entries))
        return entry

    def entries(self) -> List[HITLAuditEntry]:
        with self._lock:
            return list(self._entries)

    def verify_chain(self) -> bool:
        with self._lock:
            entries = list(self._entries)
        prev_hash = ""
        for e in entries:
            if e.prev_hash != prev_hash:
                return False
            if e.compute_hash() != e.entry_hash:
                return False
            prev_hash = e.entry_hash
        return True

    def __len__(self) -> int:
        with self._lock:
            return len(self._entries)


# --------------------------------------------------------------------
# Role registry (multi-tier approval)
# --------------------------------------------------------------------
class ApproverRoleRegistry:
    """
    Maps role names to approver callables. Falls back to a default role.
    """

    def __init__(self, default_role: str = "default"):
        self.default_role = default_role
        self._approvers: Dict[str, Callable[[HITLRequest], bool]] = {}
        self._lock = threading.Lock()

    def register(self, role: str, approver: Callable[[HITLRequest], bool]) -> None:
        with self._lock:
            self._approvers[role] = approver

    def unregister(self, role: str) -> None:
        with self._lock:
            self._approvers.pop(role, None)

    def get(self, role: Optional[str]) -> Optional[Callable[[HITLRequest], bool]]:
        with self._lock:
            if role and role in self._approvers:
                return self._approvers[role]
            return self._approvers.get(self.default_role)

    def roles(self) -> List[str]:
        with self._lock:
            return list(self._approvers.keys())


# --------------------------------------------------------------------
# Token bucket rate limiter
# --------------------------------------------------------------------
class TokenBucket:
    """Thread-safe token bucket."""

    def __init__(self, rate: float, per_seconds: float = 60.0):
        self.rate = float(rate)
        self.per_seconds = float(per_seconds)
        self._tokens = float(rate)
        self._last_refill = time.time()
        self._lock = threading.Lock()

    def acquire(self, cost: float = 1.0) -> bool:
        with self._lock:
            now = time.time()
            dt = now - self._last_refill
            self._tokens = min(
                self.rate,
                self._tokens + dt * (self.rate / max(self.per_seconds, 1e-6)))
            self._last_refill = now
            if self._tokens >= cost:
                self._tokens -= cost
                return True
            return False

    def remaining(self) -> float:
        with self._lock:
            return float(self._tokens)


# --------------------------------------------------------------------
# Uncertainty estimator
# --------------------------------------------------------------------
class UncertaintyEstimator:
    """
    Thread-safe rolling-window statistics per metric.

    Also tracks per-expert forecast disagreement and provides entropy and
    a combined uncertainty score.
    """

    def __init__(self, window: int = 32):
        self.window = int(window)
        self._history: Dict[str, deque] = {}
        self._expert: Dict[str, deque] = {}
        self._lock = threading.RLock()

    # -- observation --
    def observe(self, metrics: Dict[str, Any]) -> None:
        with self._lock:
            for k, v in metrics.items():
                if isinstance(v, (int, float)) and not isinstance(v, bool):
                    d = self._history.setdefault(k, deque(maxlen=self.window))
                    d.append(float(v))

    def observe_expert_forecasts(self, key: str,
                                 forecasts: Sequence[float]) -> None:
        with self._lock:
            d = self._expert.setdefault(key, deque(maxlen=self.window))
            d.append([float(f) for f in forecasts])

    # -- scalar statistics --
    def _snapshot(self, key: str) -> Optional[List[float]]:
        with self._lock:
            d = self._history.get(key)
            return list(d) if d else None

    def std(self, key: str) -> float:
        d = self._snapshot(key)
        if not d or len(d) < 3:
            return 0.0
        return float(np.std(np.asarray(d, dtype=np.float64)))

    def mean(self, key: str) -> float:
        d = self._snapshot(key)
        if not d:
            return 0.0
        return float(np.mean(np.asarray(d, dtype=np.float64)))

    def z_score(self, key: str, value: float) -> float:
        s = self.std(key)
        if s < 1e-9:
            return 0.0
        return (value - self.mean(key)) / s

    def is_anomalous(self, key: str, value: float,
                     z_thresh: float = 3.0,
                     two_sided: bool = True) -> Tuple[bool, float]:
        """
        Two-sided by default: |z| > z_thresh. Set two_sided=False for a
        one-sided upper-tail test (z > z_thresh).
        """
        z = self.z_score(key, value)
        stat = abs(z) if two_sided else z
        return stat > z_thresh, float(z)

    def disagreement(self, key: str) -> float:
        with self._lock:
            d = self._expert.get(key)
            recent = list(d)[-1] if d else None
        if not recent or len(recent) < 2:
            return 0.0
        arr = np.asarray(recent, dtype=np.float64)
        m = abs(float(arr.mean())) + 1e-9
        return float(arr.std() / m)

    def entropy(self, probs: Sequence[float]) -> float:
        p = np.clip(np.asarray(probs, dtype=np.float64), 1e-12, 1.0)
        if len(p) < 2:
            return 0.0
        h = -float(np.sum(p * np.log(p)))
        return float(h / math.log(len(p)))

    def composite_uncertainty(self, keys: Sequence[str]) -> float:
        """
        Mean std over ALL keys, including zero-variance keys. A stable
        key contributes 0 to the average (instead of being skipped).
        """
        if not keys:
            return 0.0
        vals = [self.std(k) for k in keys]
        return float(np.mean(vals))

    def combined_score(self, key: str, value: float,
                       probs: Optional[Sequence[float]] = None,
                       weights: Tuple[float, float, float] = (0.5, 0.3, 0.2)) -> float:
        """
        Blend |z| / z_thresh, disagreement, and entropy into [0, 1].
        """
        _, z = self.is_anomalous(key, value, z_thresh=3.0, two_sided=True)
        z_norm = min(1.0, abs(z) / 3.0)
        d = min(1.0, self.disagreement(key))
        h = self.entropy(probs) if probs is not None else 0.0
        w_z, w_d, w_h = weights
        total = w_z + w_d + w_h
        if total <= 0:
            return 0.0
        return float((w_z * z_norm + w_d * d + w_h * h) / total)

    def snapshot_stats(self) -> Dict[str, Dict[str, float]]:
        with self._lock:
            items = {k: list(v) for k, v in self._history.items()}
        out: Dict[str, Dict[str, float]] = {}
        for k, d in items.items():
            if len(d) < 2:
                continue
            arr = np.asarray(d, dtype=np.float64)
            out[k] = {
                "mean": float(arr.mean()),
                "std": float(arr.std()),
                "count": len(arr),
            }
        return out


# --------------------------------------------------------------------
# Conformal calibrator
# --------------------------------------------------------------------
class ConformalCalibrator:
    """
    Split-conformal calibrator. Call `observe(residual)` as ground truth
    becomes available. `quantile(alpha)` returns the finite-sample
    corrected (1-alpha) empirical quantile of |residual|.
    """

    def __init__(self, capacity: int = 2000):
        self._scores: deque = deque(maxlen=int(capacity))
        self._lock = threading.Lock()

    def observe(self, residual: float) -> None:
        with self._lock:
            self._scores.append(abs(float(residual)))

    def quantile(self, alpha: float = 0.1) -> float:
        if not (0.0 < alpha < 1.0):
            raise ValueError("alpha must be in (0, 1)")
        with self._lock:
            scores = sorted(self._scores)
        n = len(scores)
        if n == 0:
            return float("inf")
        idx = min(n - 1, int(math.ceil((n + 1) * (1 - alpha))) - 1)
        return float(scores[idx])

    def interval(self, y_hat: float, alpha: float = 0.1) -> Tuple[float, float]:
        q = self.quantile(alpha)
        return (float(y_hat) - q, float(y_hat) + q)

    def size(self) -> int:
        with self._lock:
            return len(self._scores)


# --------------------------------------------------------------------
# Uncertainty calibration (Platt scaling)
# --------------------------------------------------------------------
class UncertaintyCalibrator:
    """
    Map raw uncertainty scores to empirical error probabilities.

    `observe(score, was_correct)` accumulates labels; `fit()` trains a
    logistic regression on the accumulated data. `calibrated_uncertainty(score)`
    returns the predicted error probability (0 = very confident,
    1 = very likely wrong). Falls back to the raw score if unfit.
    """

    def __init__(self, min_samples: int = 20):
        self.min_samples = int(min_samples)
        self._scores: List[float] = []
        self._labels: List[int] = []
        self._model = None
        self._lock = threading.Lock()

    def observe(self, score: float, was_correct: bool) -> None:
        with self._lock:
            self._scores.append(float(score))
            self._labels.append(1 if was_correct else 0)

    def fit(self) -> bool:
        with self._lock:
            X = list(self._scores)
            y = list(self._labels)
        if len(X) < self.min_samples or len(set(y)) < 2:
            return False
        if not SKLEARN_AVAILABLE:
            return False
        try:
            model = LogisticRegression(max_iter=500)
            model.fit(np.asarray(X, dtype=np.float64).reshape(-1, 1),
                      np.asarray(y, dtype=np.int64))
            with self._lock:
                self._model = model
            return True
        except Exception as e:
            logger.warning(f"Calibrator fit failed: {e}")
            return False

    def calibrated_uncertainty(self, score: float) -> float:
        with self._lock:
            model = self._model
        if model is None:
            return float(score)
        try:
            p = float(model.predict_proba(np.asarray([[float(score)]]))[0][1])
            return max(0.0, min(1.0, p))
        except Exception:
            return float(score)

    def is_fitted(self) -> bool:
        with self._lock:
            return self._model is not None


# --------------------------------------------------------------------
# Safety monitor protocol (duck-typed coupling to a STL monitor)
# --------------------------------------------------------------------
@runtime_checkable
class SafetyMonitor(Protocol):
    def verify(self) -> Dict[str, bool]: ...
    def recent_violations(self, n: int = 10) -> List[Dict[str, Any]]: ...


# --------------------------------------------------------------------
# Human-in-the-loop gate
# --------------------------------------------------------------------
class HumanInTheLoopGate:
    """
    Approval gate with:
      * pending-request registry and external `resolve()`
      * timeout enforcement and production-safe fail-closed default
      * priority queue and backpressure (max_pending)
      * rate limiting (token bucket)
      * structured audit with hash chain and optional persistence
      * multi-tier approval via ApproverRoleRegistry
      * optional safety-monitor coupling (records violations on the request)
      * async bridge (`request_async`, `resolve_async`)
    """

    def __init__(
        self,
        approver: Optional[Callable[[HITLRequest], bool]] = None,
        timeout_s: float = 30.0,
        auto_approve_on_timeout: bool = False,
        production_safe: bool = True,
        max_pending: int = 128,
        rate_limit_per_minute: int = 600,
        audit_capacity: int = 4096,
        audit_persist_path: Optional[str] = None,
        roles: Optional[ApproverRoleRegistry] = None,
        safety_monitor: Optional[SafetyMonitor] = None,
        executor_workers: int = 4,
    ):
        self.approver = approver
        self.timeout_s = float(timeout_s)
        self.auto_approve_on_timeout = bool(auto_approve_on_timeout)
        self.production_safe = bool(production_safe)
        self.max_pending = int(max_pending)
        self.roles = roles or ApproverRoleRegistry()
        if approver is not None:
            self.roles.register(self.roles.default_role, approver)
        self.safety_monitor = safety_monitor

        self.ledger = HITLAuditLedger(
            capacity=audit_capacity, persist_path=audit_persist_path)
        self._rate_limiter = TokenBucket(rate=rate_limit_per_minute,
                                         per_seconds=60.0)

        self._pending: Dict[str, HITLRequest] = {}
        self._futures: Dict[str, concurrent.futures.Future] = {}
        self._state: Dict[str, HITLState] = {}
        self._lock = threading.RLock()
        self._executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=executor_workers,
            thread_name_prefix="hitl-approver")

    # -------------------- lifecycle --------------------
    def close(self) -> None:
        self._executor.shutdown(wait=False, cancel_futures=True)

    def __enter__(self): return self
    def __exit__(self, *exc): self.close()

    # -------------------- internal helpers --------------------
    def _audit(
        self,
        req: Optional[HITLRequest],
        approved: bool,
        mode: HITLMode,
        start: float,
        error: Optional[str] = None,
    ) -> HITLAuditEntry:
        latency_ms = (time.time() - start) * 1000.0
        HITL_LATENCY.observe(latency_ms / 1000.0)
        reason = req.reason if req else "unknown"
        if error:
            reason = f"{reason}: {error}"
        entry = HITLAuditEntry(
            request_id=req.request_id if req else "unknown",
            approved=bool(approved),
            reason=reason,
            mode=mode.value,
            latency_ms=latency_ms,
            role=(req.requires_role if req else None),
        )
        self.ledger.append(entry)
        return entry

    def _select_approver(
        self, req: HITLRequest
    ) -> Optional[Callable[[HITLRequest], bool]]:
        if req.requires_role:
            selected = self.roles.get(req.requires_role)
            if selected is not None:
                return selected
        return self.roles.get(self.roles.default_role)

    def _enrich_with_safety(self, req: HITLRequest) -> None:
        if self.safety_monitor is None:
            return
        try:
            verdict = self.safety_monitor.verify()
            violations = [k for k, ok in verdict.items() if not ok]
            if violations:
                req.metadata.setdefault("safety_violations", violations)
            try:
                recent = self.safety_monitor.recent_violations(5)
                if recent:
                    req.metadata.setdefault("safety_recent", recent)
            except Exception:
                pass
        except Exception as e:
            logger.debug(f"Safety monitor verify failed: {e}")

    def _archive(self, request_id: str) -> None:
        with self._lock:
            self._pending.pop(request_id, None)
            self._futures.pop(request_id, None)
            self._state[request_id] = HITLState.ARCHIVED
            HITL_PENDING.set(len(self._pending))

    # -------------------- public sync API --------------------
    def submit(self, req: HITLRequest) -> None:
        """Register a pending request without blocking."""
        with self._lock:
            if len(self._pending) >= self.max_pending:
                HITL_QUEUE_OVERFLOW.inc()
                raise HITLQueueFull(
                    f"Pending queue full ({len(self._pending)} >= {self.max_pending})")
            self._pending[req.request_id] = req
            self._futures[req.request_id] = concurrent.futures.Future()
            self._state[req.request_id] = HITLState.PENDING
            HITL_PENDING.set(len(self._pending))

    def resolve(self, request_id: str, approved: bool,
                role: Optional[str] = None) -> bool:
        """External resolution. Returns True if the request was pending."""
        with self._lock:
            state = self._state.get(request_id)
            if state != HITLState.PENDING:
                return False
            fut = self._futures.get(request_id)
            req = self._pending.get(request_id)
            if fut is None or fut.done():
                return False
            if not self._rate_limiter.acquire():
                HITL_RATE_LIMITED.inc()
                self._audit(req, approved=False, mode=HITLMode.RATE_LIMITED,
                            start=time.time())
                return False
            fut.set_result(bool(approved))
            self._state[request_id] = HITLState.RESOLVED

        self._audit(req, approved=bool(approved), mode=HITLMode.RESOLVED,
                    start=time.time())
        HITL_APPROVALS.labels(
            decision="approved" if approved else "rejected").inc()
        self._archive(request_id)
        return True

    def request_sync(self, req: HITLRequest) -> bool:
        """
        Blocking approval request. Runs the approver in a worker thread
        with a hard timeout.
        """
        start = time.time()

        # Rate limit
        if not self._rate_limiter.acquire():
            HITL_RATE_LIMITED.inc()
            self._audit(req, approved=False, mode=HITLMode.RATE_LIMITED,
                        start=start)
            return False

        # Safety coupling
        self._enrich_with_safety(req)

        # Select approver
        selected = self._select_approver(req)

        if selected is None:
            # Production-safe: fail closed. Non-prod: auto-approve.
            if self.production_safe:
                self._audit(req, approved=False, mode=HITLMode.BLOCKED,
                            start=start)
                HITL_APPROVALS.labels(decision="blocked").inc()
                return False
            else:
                self._audit(req, approved=True, mode=HITLMode.AUTO,
                            start=start)
                HITL_APPROVALS.labels(decision="auto_approved").inc()
                return True

        # Register request (backpressure may raise)
        try:
            self.submit(req)
        except HITLQueueFull:
            self._audit(req, approved=False, mode=HITLMode.QUEUE_FULL,
                        start=start)
            return False

        try:
            future = self._executor.submit(selected, req)
        except RuntimeError as e:
            self._resolve_internal(req.request_id, False, HITLMode.ERROR,
                                   start, error=str(e))
            return False

        try:
            approved = bool(future.result(timeout=self.timeout_s))
            self._resolve_internal(req.request_id, approved,
                                   HITLMode.APPROVER, start)
            return approved
        except concurrent.futures.TimeoutError:
            approved = bool(self.auto_approve_on_timeout)
            self._resolve_internal(req.request_id, approved,
                                   HITLMode.TIMEOUT, start)
            return approved
        except Exception as e:
            self._resolve_internal(req.request_id, False, HITLMode.ERROR,
                                   start, error=str(e))
            return False

    def _resolve_internal(self, request_id: str, approved: bool,
                          mode: HITLMode, start: float,
                          error: Optional[str] = None) -> None:
        """Resolve a request only if still pending (prevents double-count)."""
        with self._lock:
            state = self._state.get(request_id)
            if state != HITLState.PENDING:
                return
            fut = self._futures.get(request_id)
            if fut is not None and not fut.done():
                fut.set_result(bool(approved))
            self._state[request_id] = HITLState.RESOLVED
            req = self._pending.get(request_id)

        self._audit(req, approved=bool(approved), mode=mode, start=start,
                    error=error)
        HITL_APPROVALS.labels(
            decision="approved" if approved else "rejected").inc()
        self._archive(request_id)

    # -------------------- async bridge --------------------
    async def request_async(self, req: HITLRequest) -> bool:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self.request_sync, req)

    async def resolve_async(self, request_id: str, approved: bool,
                            role: Optional[str] = None) -> bool:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None, lambda: self.resolve(request_id, approved, role))

    # -------------------- observability --------------------
    def pending_requests(self) -> List[Dict[str, Any]]:
        with self._lock:
            pending = list(self._pending.values())
            state = dict(self._state)
        pending.sort(key=lambda r: (-r.priority, r.timestamp))
        return [
            {
                "request_id": r.request_id,
                "reason": r.reason,
                "metric_name": r.metric_name,
                "value": r.value,
                "uncertainty": r.uncertainty,
                "carbon_price": r.carbon_price,
                "priority": r.priority,
                "requires_role": r.requires_role,
                "state": state.get(r.request_id, HITLState.PENDING).value,
                "age_s": max(0.0, time.time() - r.timestamp),
            }
            for r in pending
        ]

    def audit_trail(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        entries = self.ledger.entries()
        if limit is not None:
            entries = entries[-int(limit):]
        return [e.to_dict() for e in entries]

    def verify_audit(self) -> bool:
        return self.ledger.verify_chain()

    def stats(self) -> Dict[str, Any]:
        with self._lock:
            state_counts: Dict[str, int] = {}
            for s in self._state.values():
                state_counts[s.value] = state_counts.get(s.value, 0) + 1
            pending = len(self._pending)
        return {
            "pending": pending,
            "max_pending": self.max_pending,
            "state_counts": state_counts,
            "rate_limit_tokens": self._rate_limiter.remaining(),
            "rate_limit_capacity": self._rate_limiter.rate,
            "audit_len": len(self.ledger),
            "audit_verified": self.ledger.verify_chain(),
            "production_safe": self.production_safe,
            "auto_approve_on_timeout": self.auto_approve_on_timeout,
            "timeout_s": self.timeout_s,
            "roles": self.roles.roles(),
        }


# --------------------------------------------------------------------
# Active learning sampler
# --------------------------------------------------------------------
class ActiveLearningSampler:
    """
    Uncertainty-weighted, diversity-aware, time-bounded buffer.

    Sampling weight = (1 - w) * uncertainty + w * diversity, where
    diversity is 1 / (1 + cluster_size) from a KMeans fit when sklearn
    is available, else a seeded random tie-breaker.

    If at capacity, the lowest-uncertainty sample is evicted (unless
    `evict_by_uncertainty=False`, in which case it is FIFO).
    """

    def __init__(
        self,
        capacity: int = 256,
        seed: int = 0,
        feature_dim: int = 8,
        diversity_weight: float = 0.4,
        n_clusters: int = 4,
        max_age_s: float = 0.0,
        evict_by_uncertainty: bool = True,
    ):
        self.capacity = int(capacity)
        self.feature_dim = int(feature_dim)
        self.diversity_weight = float(diversity_weight)
        self.n_clusters = int(n_clusters)
        self.max_age_s = float(max_age_s)
        self.evict_by_uncertainty = bool(evict_by_uncertainty)

        self._buffer: List[Dict[str, Any]] = []
        self._features: List[np.ndarray] = []
        self._uncertainties: List[float] = []
        self._lock = threading.RLock()

        self._rng = random.Random(int(seed))
        self._embed_fn: Optional[Callable[[Dict[str, Any]], np.ndarray]] = None
        self._calibrator: Optional[UncertaintyCalibrator] = None
        self._kmeans = KMeans(n_clusters=self.n_clusters, n_init=5) \
            if SKLEARN_AVAILABLE and self.n_clusters > 1 else None

        ACTIVE_LEARNING_SIZE.set(0)

    # -------------------- configuration --------------------
    def set_embed_fn(self, fn: Callable[[Dict[str, Any]], np.ndarray]) -> None:
        self._embed_fn = fn

    def set_calibrator(self, calibrator: UncertaintyCalibrator) -> None:
        self._calibrator = calibrator

    # -------------------- embedding --------------------
    def _embed(self, record: Dict[str, Any]) -> np.ndarray:
        if self._embed_fn is not None:
            v = np.asarray(self._embed_fn(record), dtype=np.float64).reshape(-1)
        else:
            vals: List[float] = []
            for k in sorted(record.keys()):
                val = record[k]
                if isinstance(val, (int, float)) and not isinstance(val, bool):
                    vals.append(float(val))
            if not vals:
                vals = [0.0]
            v = np.asarray(vals, dtype=np.float64)
        if len(v) < self.feature_dim:
            v = np.pad(v, (0, self.feature_dim - len(v)))
        else:
            v = v[: self.feature_dim]
        n = float(np.linalg.norm(v)) + 1e-9
        return v / n

    # -------------------- store --------------------
    def maybe_store(self, record: Dict[str, Any], uncertainty: float,
                    threshold: float = 0.3) -> bool:
        if uncertainty < threshold:
            return False
        feat = self._embed(record)
        with self._lock:
            if len(self._buffer) >= self.capacity:
                if self.evict_by_uncertainty and self._uncertainties:
                    idx = int(np.argmin(self._uncertainties))
                else:
                    idx = 0
                self._buffer.pop(idx)
                self._features.pop(idx)
                self._uncertainties.pop(idx)
                ACTIVE_LEARNING_EVICTED.inc()
            self._buffer.append({
                **record,
                "uncertainty": float(uncertainty),
                "t": time.time(),
            })
            self._features.append(feat)
            self._uncertainties.append(float(uncertainty))
            size = len(self._buffer)
        ACTIVE_LEARNING_STORED.inc()
        ACTIVE_LEARNING_SIZE.set(size)
        return True

    # -------------------- sample --------------------
    def _filter_fresh(self) -> Tuple[List[int], List[Dict[str, Any]]]:
        if self.max_age_s <= 0:
            return list(range(len(self._buffer))), list(self._buffer)
        now = time.time()
        idx: List[int] = []
        items: List[Dict[str, Any]] = []
        for i, rec in enumerate(self._buffer):
            if now - rec.get("t", now) <= self.max_age_s:
                idx.append(i)
                items.append(rec)
        return idx, items

    def sample(self, k: int = 8) -> List[Dict[str, Any]]:
        with self._lock:
            if not self._buffer:
                return []
            indices, items = self._filter_fresh()
            if not items:
                return []
            k = min(int(k), len(items))
            if k <= 0:
                return []

            # Uncertainty (calibrated if a calibrator is set)
            if self._calibrator is not None and self._calibrator.is_fitted():
                uncs = np.asarray([
                    self._calibrator.calibrated_uncertainty(self._uncertainties[i])
                    for i in indices
                ], dtype=np.float64)
            else:
                uncs = np.asarray(
                    [self._uncertainties[i] for i in indices], dtype=np.float64)

            # Diversity
            if (self._kmeans is not None and
                    len(indices) >= max(2 * self.n_clusters, 8)):
                try:
                    X = np.stack([self._features[i] for i in indices])
                    labels = self._kmeans.fit_predict(X)
                    cluster_sizes = np.bincount(labels)
                    diversity = np.asarray([
                        1.0 / (1.0 + float(cluster_sizes[lab]))
                        for lab in labels
                    ], dtype=np.float64)
                except Exception:
                    diversity = np.asarray(
                        [self._rng.random() for _ in indices], dtype=np.float64)
            else:
                diversity = np.asarray(
                    [self._rng.random() for _ in indices], dtype=np.float64)

            w = max(0.0, min(1.0, self.diversity_weight))
            scores = (1.0 - w) * uncs + w * diversity
            order = np.argsort(-scores)[:k].tolist()
            return [items[j] for j in order]

    # -------------------- observability --------------------
    def __len__(self) -> int:
        with self._lock:
            return len(self._buffer)

    def stats(self) -> Dict[str, Any]:
        with self._lock:
            uncs = list(self._uncertainties)
            ages = [max(0.0, time.time() - r.get("t", time.time()))
                    for r in self._buffer]
        return {
            "size": len(uncs),
            "capacity": self.capacity,
            "mean_uncertainty": float(np.mean(uncs)) if uncs else 0.0,
            "max_uncertainty": float(np.max(uncs)) if uncs else 0.0,
            "min_uncertainty": float(np.min(uncs)) if uncs else 0.0,
            "max_age_s": float(np.max(ages)) if ages else 0.0,
            "diversity_weight": self.diversity_weight,
            "max_age_limit_s": self.max_age_s,
            "sklearn_available": SKLEARN_AVAILABLE,
        }


# --------------------------------------------------------------------
# Optional FastAPI integration
# --------------------------------------------------------------------
def install_fastapi_routes(
    app: Any,
    gate: HumanInTheLoopGate,
    auth_dep: Optional[Callable[..., Any]] = None,
) -> bool:
    """
    Mount HITL endpoints on an existing FastAPI app.

    Returns True if mounted, False if FastAPI is unavailable.
    The `auth_dep` is optional; if provided, it must be a FastAPI dependency
    returning a truthy value for authorized callers.
    """
    try:
        from fastapi import Depends, HTTPException
    except ImportError:
        return False

    dep = auth_dep or (lambda: True)

    @app.get("/hitl/pending")
    async def _pending(_=Depends(dep)):
        return {"pending": gate.pending_requests()}

    @app.post("/hitl/approval")
    async def _resolve(request_id: str, approved: bool,
                       role: Optional[str] = None, _=Depends(dep)):
        ok = await gate.resolve_async(request_id, approved, role)
        if not ok:
            raise HTTPException(status_code=404, detail="not_found")
        return {"status": "resolved", "request_id": request_id,
                "approved": approved, "role": role}

    @app.get("/hitl/audit")
    async def _audit(limit: int = 100, _=Depends(dep)):
        return {"audit": gate.audit_trail(limit=limit),
                "verified": gate.verify_audit()}

    @app.get("/hitl/stats")
    async def _stats(_=Depends(dep)):
        return gate.stats()

    return True


# --------------------------------------------------------------------
# Public surface
# --------------------------------------------------------------------
__all__ = [
    "HITLRequest",
    "HITLAuditEntry",
    "HITLAuditLedger",
    "HITLMode",
    "HITLState",
    "HITLNoApprover",
    "HITLQueueFull",
    "HITLRateLimited",
    "HITLTimeout",
    "HITLError",
    "UncertaintyEstimator",
    "ConformalCalibrator",
    "UncertaintyCalibrator",
    "ApproverRoleRegistry",
    "TokenBucket",
    "SafetyMonitor",
    "HumanInTheLoopGate",
    "ActiveLearningSampler",
    "install_fastapi_routes",
]


# --------------------------------------------------------------------
# Self-test
# --------------------------------------------------------------------
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")
    rng = random.Random(0)

    # --- UncertaintyEstimator ---
    est = UncertaintyEstimator(window=32)
    for _ in range(40):
        est.observe({"scarcity": 0.5 + 0.02 * rng.gauss(0, 1),
                     "price": 200 + 5 * rng.gauss(0, 1)})
    est.observe_expert_forecasts("scarcity", [0.48, 0.52, 0.55, 0.47])
    print("std(scarcity):", round(est.std("scarcity"), 4))
    print("z(0.9):", round(est.z_score("scarcity", 0.9), 2))
    print("anomalous(0.9):", est.is_anomalous("scarcity", 0.9, 3.0))
    print("disagreement:", round(est.disagreement("scarcity"), 4))
    print("entropy([0.5, 0.5]):", round(est.entropy([0.5, 0.5]), 4))
    print("composite:", round(est.composite_uncertainty(["scarcity", "price"]), 4))
    print("combined:", round(est.combined_score("scarcity", 0.9, [0.6, 0.4]), 4))

    # --- ConformalCalibrator ---
    cp = ConformalCalibrator()
    for _ in range(200):
        cp.observe(rng.gauss(0, 1))
    print("conformal q(0.1):", round(cp.quantile(0.1), 4))
    print("interval:", tuple(round(x, 3) for x in cp.interval(0.5, 0.1)))

    # --- HITL gate ---
    def simple_approver(req: HITLRequest) -> bool:
        return req.uncertainty < 0.5

    gate = HumanInTheLoopGate(
        approver=simple_approver, timeout_s=1.0,
        auto_approve_on_timeout=False, production_safe=True, max_pending=4)

    r1 = HITLRequest.new("uncertain", "scarcity", 0.5, 0.8)
    r2 = HITLRequest.new("uncertain", "scarcity", 0.5, 0.2)
    print("r1 (unc 0.8, low-prio):", gate.request_sync(r1))
    print("r2 (unc 0.2, low-prio):", gate.request_sync(r2))

    # Async bridge
    async def _async_demo():
        r3 = HITLRequest.new("async", "scarcity", 0.5, 0.1)
        return await gate.request_async(r3)

    print("r3 (async):", asyncio.run(_async_demo()))
    print("audit verified:", gate.verify_audit())
    print("gate stats:", gate.stats())

    # External resolution demo
    gate.submit(HITLRequest.new("external", "price", 1.0, 0.9,
                                priority=10))
    pending = gate.pending_requests()
    if pending:
        pid = pending[0]["request_id"]
        print("external resolve:", gate.resolve(pid, True, role="operator"))
    print("audit verified after resolve:", gate.verify_audit())

    # --- ActiveLearningSampler ---
    sampler = ActiveLearningSampler(capacity=8, seed=42,
                                    diversity_weight=0.4,
                                    max_age_s=0.0)
    for i in range(20):
        sampler.maybe_store({"x": i, "y": 0.1 * i},
                            uncertainty=rng.random(),
                            threshold=0.3)
    print("sampler size:", len(sampler))
    print("sample:", [r["x"] for r in sampler.sample(k=3)])
    print("sampler stats:", sampler.stats())

    gate.close()
