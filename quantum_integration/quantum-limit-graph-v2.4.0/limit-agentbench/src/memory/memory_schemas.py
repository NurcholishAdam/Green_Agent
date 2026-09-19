# src/memory/memory_schemas.py

"""
Memory Schemas
==============

Canonical event schemas persisted to Supermemory. These records are the
stable contract for the whole memory layer; the graph UI and MCP tools
consume them directly.

Schemas
-------
- ``TruthLevel`` — one of ``measured`` / ``estimated`` / ``simulated`` /
  ``user-reported``.
- ``DecisionRecord`` — one routing decision.
- ``PolicyRecord`` — one versioned policy.
- ``IncidentRecord`` — one failed or unsafe event.
- ``OutcomeRecord`` — predicted vs measured outcome of a run.

Enhancements
------------
- Frozen dataclasses with ``__post_init__`` validation.
- ``to_dict`` / ``from_dict`` / ``to_json`` / ``from_json`` on each.
- ``to_supermemory_payload()`` produces the
  ``{"content", "container_tag", "metadata"}`` triple.
- UTC timestamps.
- ``__main__`` smoke test.
"""

from __future__ import annotations

import json
import logging
import math
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, Mapping, Optional, Tuple

logger = logging.getLogger(__name__)

__version__ = "5.0.0"


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class MemorySchemaError(ValueError):
    """Raised for invalid memory records."""


# --------------------------------------------------------------------------- #
# Truth level
# --------------------------------------------------------------------------- #
class TruthLevel(str, Enum):
    """Provenance classification for a memory record."""

    MEASURED = "measured"
    ESTIMATED = "estimated"
    SIMULATED = "simulated"
    USER_REPORTED = "user-reported"

    @property
    def description(self) -> str:
        return {
            TruthLevel.MEASURED: "Directly observed from instrumentation",
            TruthLevel.ESTIMATED: "Derived from a model or heuristic",
            TruthLevel.SIMULATED: "Produced by a simulation",
            TruthLevel.USER_REPORTED: "Provided by a human operator",
        }[self]


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def _parse_iso(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str):
        dt = datetime.fromisoformat(value)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    return datetime.now(timezone.utc)


def _finite(name: str, value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise MemorySchemaError(f"{name} must be numeric.")
    fv = float(value)
    if math.isnan(fv) or math.isinf(fv):
        raise MemorySchemaError(f"{name} must be finite, got {value!r}.")
    return fv


# --------------------------------------------------------------------------- #
# Decision record
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class DecisionRecord:
    """One routing decision with evidence provenance."""

    run_id: str
    workload_type: str
    device_class: str
    route: str
    policy_version: str
    truth_level: str
    predicted_energy_wh: float
    predicted_carbon_gco2e: float
    predicted_latency_ms: float
    reason: str
    candidates: Tuple[str, ...] = ()
    site_id: Optional[str] = None
    quality_score: Optional[float] = None
    container_tag: str = "org:green-agent"
    observed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    metrics_hash: Optional[str] = None
    superseded_by: Optional[str] = None

    def __post_init__(self) -> None:
        for name in (
            "run_id", "workload_type", "device_class", "route",
            "policy_version", "truth_level", "reason", "container_tag",
        ):
            value = getattr(self, name)
            if not isinstance(value, str) or not value:
                raise MemorySchemaError(f"{name} must be a non-empty string.")
        if self.truth_level not in {t.value for t in TruthLevel}:
            raise MemorySchemaError(
                f"truth_level must be one of "
                f"{sorted(t.value for t in TruthLevel)}, got {self.truth_level!r}."
            )
        _finite("predicted_energy_wh", self.predicted_energy_wh)
        _finite("predicted_carbon_gco2e", self.predicted_carbon_gco2e)
        _finite("predicted_latency_ms", self.predicted_latency_ms)
        if self.predicted_energy_wh < 0:
            raise MemorySchemaError("predicted_energy_wh must be >= 0.")
        if self.predicted_carbon_gco2e < 0:
            raise MemorySchemaError("predicted_carbon_gco2e must be >= 0.")
        if self.predicted_latency_ms < 0:
            raise MemorySchemaError("predicted_latency_ms must be >= 0.")
        if not isinstance(self.candidates, tuple):
            object.__setattr__(self, "candidates", tuple(self.candidates))
        for c in self.candidates:
            if not isinstance(c, str) or not c:
                raise MemorySchemaError(
                    "candidates entries must be non-empty strings."
                )
        if self.quality_score is not None:
            q = _finite("quality_score", self.quality_score)
            if not 0.0 <= q <= 1.0:
                raise MemorySchemaError("quality_score must be in [0, 1].")
        if not isinstance(self.observed_at, datetime):
            raise MemorySchemaError("observed_at must be a datetime.")

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["observed_at"] = _iso(self.observed_at)
        d["candidates"] = list(self.candidates)
        return d

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "DecisionRecord":
        if not isinstance(data, Mapping):
            raise MemorySchemaError("DecisionRecord.from_dict expects a Mapping.")
        return cls(
            run_id=str(data["run_id"]),
            workload_type=str(data["workload_type"]),
            device_class=str(data["device_class"]),
            route=str(data["route"]),
            policy_version=str(data["policy_version"]),
            truth_level=str(data["truth_level"]),
            predicted_energy_wh=float(data["predicted_energy_wh"]),
            predicted_carbon_gco2e=float(data["predicted_carbon_gco2e"]),
            predicted_latency_ms=float(data["predicted_latency_ms"]),
            reason=str(data.get("reason", "")),
            candidates=tuple(data.get("candidates", ())),
            site_id=data.get("site_id"),
            quality_score=data.get("quality_score"),
            container_tag=str(data.get("container_tag", "org:green-agent")),
            observed_at=_parse_iso(data.get("observed_at")),
            metrics_hash=data.get("metrics_hash"),
            superseded_by=data.get("superseded_by"),
        )

    def to_json(self, **kw: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kw)

    @classmethod
    def from_json(cls, payload: str) -> "DecisionRecord":
        try:
            return cls.from_dict(json.loads(payload))
        except json.JSONDecodeError as exc:
            raise MemorySchemaError(f"invalid JSON: {exc}") from exc

    def to_supermemory_payload(self) -> Dict[str, Any]:
        """Return the ``{"content", "container_tag", "metadata"}`` triple."""
        return {
            "content": self._render_content(),
            "container_tag": self.container_tag,
            "metadata": {
                "type": "decision_outcome",
                "run_id": self.run_id,
                "workload_type": self.workload_type,
                "device_class": self.device_class,
                "route": self.route,
                "policy_version": self.policy_version,
                "truth_level": self.truth_level,
                "quality_score": self.quality_score,
                "metrics_hash": self.metrics_hash,
                "site_id": self.site_id,
                "observed_at": _iso(self.observed_at),
            },
        }

    def _render_content(self) -> str:
        quality = (
            f"Quality: {self.quality_score:.3f}"
            if self.quality_score is not None else "Quality: n/a"
        )
        return (
            f"Green Agent decision outcome:\n"
            f"Run: {self.run_id}\n"
            f"Workload: {self.workload_type}, device_class={self.device_class}\n"
            f"Candidates: {', '.join(self.candidates) or 'n/a'}\n"
            f"Chosen: {self.route}\n"
            f"Predicted: {self.predicted_energy_wh:.4g} Wh, "
            f"{self.predicted_carbon_gco2e:.4g} gCO2e, "
            f"{self.predicted_latency_ms:.1f} ms\n"
            f"{quality}; truth_level={self.truth_level}; "
            f"policy={self.policy_version}\n"
            f"Reason: {self.reason}"
        )

    def with_superseded_by(self, new_run_id: str) -> "DecisionRecord":
        """Return a copy marked as superseded by ``new_run_id``."""
        return DecisionRecord(
            run_id=self.run_id,
            workload_type=self.workload_type,
            device_class=self.device_class,
            route=self.route,
            policy_version=self.policy_version,
            truth_level=self.truth_level,
            predicted_energy_wh=self.predicted_energy_wh,
            predicted_carbon_gco2e=self.predicted_carbon_gco2e,
            predicted_latency_ms=self.predicted_latency_ms,
            reason=self.reason,
            candidates=self.candidates,
            site_id=self.site_id,
            quality_score=self.quality_score,
            container_tag=self.container_tag,
            observed_at=self.observed_at,
            metrics_hash=self.metrics_hash,
            superseded_by=new_run_id,
        )


# --------------------------------------------------------------------------- #
# Policy record
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class PolicyRecord:
    """A versioned policy snapshot."""

    version: str
    content: Mapping[str, Any]
    policy_id: str = "default"
    approved_by: str = "system"
    container_tag: str = "policy:current"
    published_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    superseded_by: Optional[str] = None

    def __post_init__(self) -> None:
        for name in ("version", "policy_id", "approved_by", "container_tag"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value:
                raise MemorySchemaError(f"{name} must be a non-empty string.")
        if not isinstance(self.content, Mapping):
            raise MemorySchemaError("content must be a Mapping.")
        if not isinstance(self.published_at, datetime):
            raise MemorySchemaError("published_at must be a datetime.")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "version": self.version,
            "content": dict(self.content),
            "policy_id": self.policy_id,
            "approved_by": self.approved_by,
            "container_tag": self.container_tag,
            "published_at": _iso(self.published_at),
            "superseded_by": self.superseded_by,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "PolicyRecord":
        if not isinstance(data, Mapping):
            raise MemorySchemaError("PolicyRecord.from_dict expects a Mapping.")
        return cls(
            version=str(data["version"]),
            content=dict(data.get("content", {})),
            policy_id=str(data.get("policy_id", "default")),
            approved_by=str(data.get("approved_by", "system")),
            container_tag=str(data.get("container_tag", "policy:current")),
            published_at=_parse_iso(data.get("published_at")),
            superseded_by=data.get("superseded_by"),
        )

    def to_json(self, **kw: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kw)

    @classmethod
    def from_json(cls, payload: str) -> "PolicyRecord":
        try:
            return cls.from_dict(json.loads(payload))
        except json.JSONDecodeError as exc:
            raise MemorySchemaError(f"invalid JSON: {exc}") from exc

    def to_supermemory_payload(self) -> Dict[str, Any]:
        return {
            "content": (
                f"Policy {self.policy_id} {self.version} approved by "
                f"{self.approved_by}:\n{json.dumps(dict(self.content), indent=2)}"
            ),
            "container_tag": self.container_tag,
            "metadata": {
                "type": "policy",
                "version": self.version,
                "policy_id": self.policy_id,
                "approved_by": self.approved_by,
                "published_at": _iso(self.published_at),
                "superseded_by": self.superseded_by,
            },
        }


# --------------------------------------------------------------------------- #
# Incident record
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class IncidentRecord:
    """One failed or unsafe event."""

    incident_id: str
    severity: str                    # "low" | "medium" | "high" | "critical"
    description: str
    run_id: Optional[str] = None
    mitigation: Optional[str] = None
    container_tag: str = "incident:default"
    observed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    _VALID_SEVERITIES = ("low", "medium", "high", "critical")

    def __post_init__(self) -> None:
        for name in ("incident_id", "severity", "description", "container_tag"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value:
                raise MemorySchemaError(f"{name} must be a non-empty string.")
        if self.severity not in self._VALID_SEVERITIES:
            raise MemorySchemaError(
                f"severity must be one of {self._VALID_SEVERITIES}."
            )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "incident_id": self.incident_id,
            "severity": self.severity,
            "description": self.description,
            "run_id": self.run_id,
            "mitigation": self.mitigation,
            "container_tag": self.container_tag,
            "observed_at": _iso(self.observed_at),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "IncidentRecord":
        if not isinstance(data, Mapping):
            raise MemorySchemaError("IncidentRecord.from_dict expects a Mapping.")
        return cls(
            incident_id=str(data["incident_id"]),
            severity=str(data["severity"]),
            description=str(data["description"]),
            run_id=data.get("run_id"),
            mitigation=data.get("mitigation"),
            container_tag=str(data.get("container_tag", "incident:default")),
            observed_at=_parse_iso(data.get("observed_at")),
        )

    def to_supermemory_payload(self) -> Dict[str, Any]:
        return {
            "content": (
                f"Incident {self.incident_id} ({self.severity}): "
                f"{self.description}\n"
                f"Run: {self.run_id or 'n/a'}\n"
                f"Mitigation: {self.mitigation or 'n/a'}"
            ),
            "container_tag": self.container_tag,
            "metadata": {
                "type": "incident",
                "incident_id": self.incident_id,
                "severity": self.severity,
                "run_id": self.run_id,
                "observed_at": _iso(self.observed_at),
            },
        }


# --------------------------------------------------------------------------- #
# Outcome record
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class OutcomeRecord:
    """Predicted vs measured outcome of a run."""

    run_id: str
    predicted: Mapping[str, float]
    measured: Mapping[str, float]
    truth_level: str = TruthLevel.MEASURED.value
    container_tag: str = "org:green-agent"
    observed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def __post_init__(self) -> None:
        if not isinstance(self.run_id, str) or not self.run_id:
            raise MemorySchemaError("run_id must be a non-empty string.")
        for name in ("predicted", "measured"):
            mapping = getattr(self, name)
            if not isinstance(mapping, Mapping):
                raise MemorySchemaError(f"{name} must be a Mapping.")
            for k, v in mapping.items():
                _finite(f"{name}[{k!r}]", v)
        if self.truth_level not in {t.value for t in TruthLevel}:
            raise MemorySchemaError("truth_level is invalid.")

    def delta(self) -> Dict[str, float]:
        """Return ``measured - predicted`` per key."""
        keys = set(self.predicted) & set(self.measured)
        return {
            k: float(self.measured[k]) - float(self.predicted[k])
            for k in keys
        }

    def to_dict(self) -> Dict[str, Any]:
        return {
            "run_id": self.run_id,
            "predicted": dict(self.predicted),
            "measured": dict(self.measured),
            "truth_level": self.truth_level,
            "container_tag": self.container_tag,
            "observed_at": _iso(self.observed_at),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "OutcomeRecord":
        if not isinstance(data, Mapping):
            raise MemorySchemaError("OutcomeRecord.from_dict expects a Mapping.")
        return cls(
            run_id=str(data["run_id"]),
            predicted=dict(data.get("predicted", {})),
            measured=dict(data.get("measured", {})),
            truth_level=str(data.get("truth_level", TruthLevel.MEASURED.value)),
            container_tag=str(data.get("container_tag", "org:green-agent")),
            observed_at=_parse_iso(data.get("observed_at")),
        )

    def to_supermemory_payload(self) -> Dict[str, Any]:
        delta = self.delta()
        lines = [f"Outcome for run {self.run_id}:"]
        for k in sorted(set(self.predicted) | set(self.measured)):
            p = self.predicted.get(k)
            m = self.measured.get(k)
            d = delta.get(k)
            lines.append(
                f"  {k}: predicted={p} measured={m} delta={d}"
            )
        return {
            "content": "\n".join(lines),
            "container_tag": self.container_tag,
            "metadata": {
                "type": "outcome",
                "run_id": self.run_id,
                "truth_level": self.truth_level,
                "observed_at": _iso(self.observed_at),
            },
        }


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "DecisionRecord",
    "IncidentRecord",
    "MemorySchemaError",
    "OutcomeRecord",
    "PolicyRecord",
    "TruthLevel",
]


# --------------------------------------------------------------------------- #
# Smoke test: python -m memory.memory_schemas
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    d = DecisionRecord(
        run_id="run-001",
        workload_type="vision_inference",
        device_class="arm_edge",
        route="edge_int8",
        policy_version="v0.3",
        truth_level=TruthLevel.MEASURED.value,
        predicted_energy_wh=8.1,
        predicted_carbon_gco2e=3.7,
        predicted_latency_ms=410.0,
        reason="Met SLA with lower carbon and avoided unreliable network.",
        candidates=("edge_int8", "cloud_gpu", "defer"),
        quality_score=0.91,
    )
    print("decision   :", d.run_id, d.route)
    print("payload    :", d.to_supermemory_payload()["metadata"])

    # Round-trip.
    payload = d.to_json()
    assert DecisionRecord.from_json(payload).to_dict() == d.to_dict()
    print("Round-trip : OK")

    # Supersede.
    d2 = d.with_superseded_by("run-002")
    assert d2.superseded_by == "run-002"
    print("superseded :", d2.superseded_by)

    # Outcome.
    o = OutcomeRecord(
        run_id="run-001",
        predicted={"energy_wh": 8.1, "carbon_g": 3.7, "latency_ms": 410.0},
        measured={"energy_wh": 8.6, "carbon_g": 4.0, "latency_ms": 438.0},
    )
    print("delta      :", o.delta())

    # Policy.
    p = PolicyRecord(
        version="v0.3",
        content={"max_carbon_gco2e": 200.0, "allowed_regions": ["eu-west-1"]},
        approved_by="ops",
    )
    print("policy     :", p.version)

    # Incident.
    i = IncidentRecord(
        incident_id="inc-1",
        severity="high",
        description="Cloud route exceeded carbon budget.",
        run_id="run-002",
        mitigation="Rolled back to edge INT8.",
    )
    print("incident   :", i.incident_id)

    # Validation.
    for bad in (
        dict(run_id="", workload_type="x", device_class="x", route="x",
             policy_version="x", truth_level="measured",
             predicted_energy_wh=1.0, predicted_carbon_gco2e=1.0,
             predicted_latency_ms=1.0, reason="x"),
        dict(run_id="x", workload_type="x", device_class="x", route="x",
             policy_version="x", truth_level="bogus",
             predicted_energy_wh=1.0, predicted_carbon_gco2e=1.0,
             predicted_latency_ms=1.0, reason="x"),
    ):
        try:
            DecisionRecord(**bad)  # type: ignore[arg-type]
        except MemorySchemaError as exc:
            print("Rejected   :", exc)

    print("\nSmoke test passed.")
