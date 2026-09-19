# src/memory/write_governor.py

"""
Write Governor
==============

Implements the "Govern writes: only validated agents or a post-run
evaluator should promote a result into durable memory" constraint.

Enhancements
------------
- ``WriteGovernorConfig`` — frozen, validated: per-kind capabilities,
  quarantine cap, audit history size.
- ``ApprovalDecision`` — frozen, serializable.
- ``RLock``-guarded registry, quarantine queue, audit log.
- Full validation of ``writer_id``, ``scope``, and record kinds.
- Custom ``WriteGovernorError(ValueError)``.
- ``__main__`` smoke test.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Deque, Dict, FrozenSet, List, Mapping, Optional, Set

logger = logging.getLogger(__name__)

__version__ = "5.0.0"


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class WriteGovernorError(ValueError):
    """Raised for invalid governor inputs or configuration."""


# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class WriteGovernorConfig:
    """Tunable parameters for the write governor."""

    # Per-kind capabilities required to promote a record.
    required_capability: Mapping[str, str] = field(
        default_factory=lambda: {
            "decision_outcome": "write:decision",
            "policy": "write:policy",
            "incident": "write:incident",
            "outcome": "write:outcome",
        }
    )
    quarantine_capacity: int = 1_000
    audit_history_capacity: int = 10_000

    def __post_init__(self) -> None:
        if not isinstance(self.required_capability, Mapping):
            raise WriteGovernorError("required_capability must be a Mapping.")
        for k, v in self.required_capability.items():
            if not isinstance(k, str) or not isinstance(v, str):
                raise WriteGovernorError(
                    "required_capability keys/values must be strings."
                )
        for name in ("quarantine_capacity", "audit_history_capacity"):
            v = getattr(self, name)
            if not isinstance(v, int) or v <= 0:
                raise WriteGovernorError(f"{name} must be a positive int.")

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["required_capability"] = dict(self.required_capability)
        return d


# --------------------------------------------------------------------------- #
# Approval decision
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class ApprovalDecision:
    """Immutable record of one write-approval check."""

    writer_id: str
    kind: str
    approved: bool
    reason: str
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "writer_id": self.writer_id,
            "kind": self.kind,
            "approved": self.approved,
            "reason": self.reason,
            "timestamp": self.timestamp.isoformat(),
        }


# --------------------------------------------------------------------------- #
# Governor
# --------------------------------------------------------------------------- #
class WriteGovernor:
    """Decides which writers may promote which kinds of records."""

    def __init__(
        self,
        *,
        config: Optional[WriteGovernorConfig] = None,
        strict: bool = True,
    ) -> None:
        self._config = config or WriteGovernorConfig()
        self._strict = bool(strict)

        self._lock = threading.RLock()
        self._writers: Dict[str, Set[str]] = {}
        self._scopes: Dict[str, str] = {}
        self._quarantine: Deque[Dict[str, Any]] = deque(
            maxlen=self._config.quarantine_capacity
        )
        self._audit: Deque[ApprovalDecision] = deque(
            maxlen=self._config.audit_history_capacity
        )
        self._started_at: float = time.time()

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> WriteGovernorConfig:
        return self._config

    @property
    def quarantine_size(self) -> int:
        with self._lock:
            return len(self._quarantine)

    @property
    def audit_size(self) -> int:
        with self._lock:
            return len(self._audit)

    # ---------------------------------------------------------- writers
    def register_writer(
        self,
        writer_id: str,
        *,
        scope: str = "default",
        capabilities: Optional[Set[str]] = None,
    ) -> None:
        if not isinstance(writer_id, str) or not writer_id:
            raise WriteGovernorError("writer_id must be a non-empty string.")
        if not isinstance(scope, str) or not scope:
            raise WriteGovernorError("scope must be a non-empty string.")
        caps = set(capabilities or ())
        for c in caps:
            if not isinstance(c, str) or not c:
                raise WriteGovernorError(
                    "capabilities entries must be non-empty strings."
                )
        with self._lock:
            self._writers[writer_id] = caps
            self._scopes[writer_id] = scope
        logger.debug("Registered writer %s (scope=%s, caps=%s).",
                     writer_id, scope, sorted(caps))

    def unregister_writer(self, writer_id: str) -> bool:
        with self._lock:
            existed = writer_id in self._writers
            self._writers.pop(writer_id, None)
            self._scopes.pop(writer_id, None)
        return existed

    def capabilities(self, writer_id: str) -> FrozenSet[str]:
        with self._lock:
            return frozenset(self._writers.get(writer_id, set()))

    # ---------------------------------------------------------- approvals
    def approve(
        self,
        writer_id: str,
        record: Any,
        *,
        kind: Optional[str] = None,
    ) -> ApprovalDecision:
        """Return an ``ApprovalDecision`` for ``record`` written by ``writer_id``."""
        if not isinstance(writer_id, str) or not writer_id:
            raise WriteGovernorError("writer_id must be a non-empty string.")

        resolved_kind = kind or self._infer_kind(record)
        if resolved_kind is None:
            decision = ApprovalDecision(
                writer_id=writer_id,
                kind="unknown",
                approved=False,
                reason="unable to infer record kind",
            )
            self._record(decision)
            return decision

        required = self._config.required_capability.get(resolved_kind)
        if required is None:
            decision = ApprovalDecision(
                writer_id=writer_id,
                kind=resolved_kind,
                approved=False,
                reason=f"no capability configured for kind {resolved_kind!r}",
            )
            self._record(decision)
            return decision

        with self._lock:
            caps = self._writers.get(writer_id)
        if caps is None:
            decision = ApprovalDecision(
                writer_id=writer_id,
                kind=resolved_kind,
                approved=False,
                reason=f"writer {writer_id!r} is not registered",
            )
        elif required not in caps:
            decision = ApprovalDecision(
                writer_id=writer_id,
                kind=resolved_kind,
                approved=False,
                reason=f"writer {writer_id!r} lacks capability {required!r}",
            )
        else:
            decision = ApprovalDecision(
                writer_id=writer_id,
                kind=resolved_kind,
                approved=True,
                reason=f"capability {required!r} present",
            )
        self._record(decision)
        return decision

    def promote(
        self,
        record: Any,
        *,
        writer_id: str,
        kind: Optional[str] = None,
    ) -> bool:
        """Return True if the record is approved for durable memory."""
        decision = self.approve(writer_id, record, kind=kind)
        if decision.approved:
            return True
        self.quarantine(record, reason=decision.reason)
        return False

    def quarantine(self, record: Any, *, reason: str) -> None:
        if not isinstance(reason, str) or not reason:
            raise WriteGovernorError("reason must be a non-empty string.")
        entry = {
            "reason": reason,
            "timestamp": time.time(),
            "record": self._safe(record),
        }
        with self._lock:
            self._quarantine.append(entry)
        logger.warning("Quarantined record: %s", reason)

    # ---------------------------------------------------------- helpers
    def _infer_kind(self, record: Any) -> Optional[str]:
        # Try class name first.
        name = type(record).__name__
        for candidate in (
            "DecisionRecord", "PolicyRecord", "IncidentRecord", "OutcomeRecord",
        ):
            if name == candidate:
                return {
                    "DecisionRecord": "decision_outcome",
                    "PolicyRecord": "policy",
                    "IncidentRecord": "incident",
                    "OutcomeRecord": "outcome",
                }[candidate]
        # Try a to_supermemory_payload() helper.
        to_payload = getattr(record, "to_supermemory_payload", None)
        if callable(to_payload):
            try:
                payload = to_payload()
            except Exception:
                return None
            return ((payload.get("metadata") or {}).get("type")) if isinstance(
                payload, Mapping
            ) else None
        return None

    def _safe(self, record: Any) -> Any:
        to_dict = getattr(record, "to_dict", None)
        if callable(to_dict):
            try:
                return to_dict()
            except Exception:
                pass
        return repr(record)

    def _record(self, decision: ApprovalDecision) -> None:
        with self._lock:
            self._audit.append(decision)

    # ---------------------------------------------------------- statistics
    def statistics(self) -> Dict[str, Any]:
        with self._lock:
            audit = list(self._audit)
            quarantine_size = len(self._quarantine)
            writers = len(self._writers)
        approvals = sum(1 for d in audit if d.approved)
        rejections = len(audit) - approvals
        return {
            "writers": writers,
            "approvals": approvals,
            "rejections": rejections,
            "quarantine_size": quarantine_size,
            "audit_size": len(audit),
            "config": self._config.to_dict(),
            "strict": self._strict,
            "uptime_seconds": time.time() - self._started_at,
        }

    def to_json(self, **kw: Any) -> str:
        return json.dumps(self.statistics(), default=str, **kw)

    def __repr__(self) -> str:
        with self._lock:
            return (
                "WriteGovernor("
                f"writers={len(self._writers)}, "
                f"audit={len(self._audit)}, "
                f"quarantine={len(self._quarantine)}, "
                f"strict={self._strict})"
            )


__all__ = [
    "ApprovalDecision",
    "WriteGovernor",
    "WriteGovernorConfig",
    "WriteGovernorError",
]


# --------------------------------------------------------------------------- #
# Smoke test: python -m memory.write_governor
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    from .memory_schemas import DecisionRecord

    gov = WriteGovernor()
    print("repr       :", gov)

    # Register two writers with different capabilities.
    gov.register_writer("orchestrator", capabilities={"write:decision"})
    gov.register_writer("ops", capabilities={"write:policy", "write:incident"})

    d = DecisionRecord(
        run_id="run-001", workload_type="vision_inference",
        device_class="arm_edge", route="edge_int8",
        policy_version="v0.3", truth_level="measured",
        predicted_energy_wh=8.1, predicted_carbon_gco2e=3.7,
        predicted_latency_ms=410.0, reason="SLA met.",
    )

    # Approved write.
    assert gov.promote(d, writer_id="orchestrator") is True
    print("approved   : orchestrator -> decision")

    # Rejected write (unknown writer).
    assert gov.promote(d, writer_id="ghost") is False
    print("rejected   : ghost")

    # Rejected write (writer lacks capability).
    assert gov.promote(d, writer_id="ops") is False
    print("rejected   : ops (no decision capability)")

    print("quarantine :", gov.quarantine_size)
    print("statistics :", {
        k: v for k, v in gov.statistics().items()
        if k not in ("config", "uptime_seconds")
    })

    # Validation.
    for bad in (
        lambda: gov.register_writer(""),
        lambda: gov.register_writer("w", scope=""),
        lambda: gov.promote(d, writer_id=""),
        lambda: gov.quarantine(d, reason=""),
    ):
        try:
            bad()
        except WriteGovernorError as exc:
            print("Rejected   :", exc)

    print("\nSmoke test passed.")
