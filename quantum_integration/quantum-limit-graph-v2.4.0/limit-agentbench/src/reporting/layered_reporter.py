# src/r1/layered_reporter.py

"""
Layered Reporting System for Green Agent
========================================

Transforms analysis outputs into a versioned, immutable EvidenceBundle
that can be signed, hashed, redacted, and rendered for any audience.

Public API
----------
Original (preserved):

    Layer1RawMetrics, Layer2NormalizedMetrics, Layer3ScenarioScore
    LayeredReporter
        .generate_layer1(result, timestamp=None)
        .generate_layer2(result, complexity)
        .generate_layer3(layer2_metrics, ...)
        .generate_full_report(result, complexity=None)

Enhanced:

    EvidenceBundle            # the shared, versioned, signed contract
    CarbonInstruments         # operational vs. contractual separation
    MetricProvenance          # source + trust per metric
    ExplanationCard           # XAI rationale per decision
    VerificationEvidence      # formal verification summary
    LayeredReporter
        .build_evidence_bundle(result, ...) -> EvidenceBundle
        .sign_bundle(bundle, key) -> EvidenceBundle
        .verify_signature(bundle, key) -> bool
        .redact(bundle, audience) -> EvidenceBundle

Design
------
- **Canonical hash.** :func:`_canonical_payload` is the single
  serialisation used by ``compute_hash``. ``content_hash``,
  ``signature``, and ``signer`` are all excluded from the hash payload
  — signing sets ``signer`` *after* freezing, so including it would
  invalidate the hash by construction.
- **Immutability.** :meth:`EvidenceBundle.freeze` computes the hash
  the first time; on a re-freeze it recomputes and raises
  :class:`ImmutabilityError` if the bundle was mutated since the
  original freeze. Signing calls ``freeze()`` first, so a
  post-signing mutation surfaces at sign time or verification time.
- **Signature verification.** :meth:`LayeredReporter.verify_signature`
  re-checks both the content hash (detects post-freeze mutation) and
  the HMAC (detects forgery), using ``hmac.compare_digest``.
- **Redaction.** :meth:`LayeredReporter.redact` deep-copies the
  payload, clears ``content_hash``/``signature``/``signer``, and
  returns a new bundle. The source bundle is not mutated; the
  redacted bundle is unsigned until the caller re-signs.

Version: 2.0.0
"""

from __future__ import annotations

import copy
import hashlib
import hmac
import json
import logging
import math
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Literal, Mapping, Optional

logger = logging.getLogger(__name__)

__all__ = [
    "VERSION",
    "LayeredReporterError",
    "ImmutabilityError",
    "Audience",
    "ProvenanceKind",
    "TrustLevel",
    "Layer1RawMetrics",
    "Layer2NormalizedMetrics",
    "Layer3ScenarioScore",
    "CarbonInstruments",
    "MetricProvenance",
    "ExplanationCard",
    "VerificationEvidence",
    "EvidenceBundle",
    "BundleBuilder",
    "LayeredReporter",
]

VERSION = "2.0.0"


# =============================================================================
# Errors
# =============================================================================

class LayeredReporterError(ValueError):
    """Base error for the layered reporter."""


class ImmutabilityError(LayeredReporterError):
    """Raised when a frozen bundle is mutated and re-frozen."""


# =============================================================================
# Enums
# =============================================================================

class Audience(Enum):
    """Stakeholder audiences for report generation."""
    OPERATOR = "operator"
    TECHNICAL = "technical"
    GOVERNANCE = "governance"
    EXECUTIVE = "executive"


class ProvenanceKind(Enum):
    MEASURED = "measured"
    ESTIMATED = "estimated"
    SIMULATED = "simulated"
    FEDERATED = "federated"
    UNKNOWN = "unknown"


class TrustLevel(Enum):
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


# =============================================================================
# Canonical serialisation
# =============================================================================

def _json_default(value: Any) -> Any:
    """
    Deterministic coercion for hash payloads.

    Only ``datetime`` and ``Enum`` are accepted; everything else raises
    so callers learn they have an unhashable payload instead of getting
    an order-dependent hash.
    """
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    raise TypeError(
        f"cannot hash non-JSON-native value of type "
        f"{type(value).__name__}"
    )


def _canonical_payload(
    data: Mapping[str, Any], *, strip: tuple[str, ...] = ()
) -> str:
    """
    Deterministic canonical JSON: sorted keys, compact separators,
    ASCII-only, no ``NaN``/``Inf``. ``strip`` names are removed before
    serialising.
    """
    payload = dict(data)
    for k in strip:
        payload.pop(k, None)
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
        default=_json_default,
    )


# =============================================================================
# Layer dataclasses
# =============================================================================

@dataclass
class Layer1RawMetrics:
    """
    Layer 1: Raw, unprocessed metrics directly from execution.

    Original 5 fields preserved; enhancement fields are optional with
    defaults so existing construction sites keep working.
    """
    # --- Original fields ---
    accuracy: float
    energy_wh: float
    carbon_co2_g: float
    latency_ms: float
    timestamp: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    # --- Enhancement: provenance ---
    run_id: Optional[str] = None
    task_id: Optional[str] = None
    agent_id: Optional[str] = None
    deployment_id: Optional[str] = None
    policy_version: Optional[str] = None
    system_version: Optional[str] = None

    # --- Enhancement: context ---
    precision: Optional[str] = None
    hardware: Optional[str] = None
    region: Optional[str] = None

    # --- Enhancement: carbon separation ---
    carbon_operational_co2_g: Optional[float] = None
    carbon_contractual_co2_g: float = 0.0

    # --- Enhancement: trust flags ---
    simulated: bool = False
    chaos_injected: bool = False
    source: str = ProvenanceKind.UNKNOWN.value

    # --- Enhancement: latency-splitting ---
    tool_calls: int = 0
    conversation_depth: int = 0
    llm_calls: int = 0
    tokens: int = 0

    def __post_init__(self) -> None:
        for name in (
            "accuracy", "energy_wh", "carbon_co2_g", "latency_ms",
        ):
            v = getattr(self, name)
            if isinstance(v, bool) or not isinstance(
                v, (int, float)
            ):
                raise LayeredReporterError(
                    f"{name} must be a real number, "
                    f"got {type(v).__name__}."
                )
            if not math.isfinite(float(v)):
                raise LayeredReporterError(
                    f"{name} must be finite, got {v!r}."
                )
        for name in ("energy_wh", "carbon_co2_g", "latency_ms"):
            if getattr(self, name) < 0:
                raise LayeredReporterError(
                    f"{name} must be >= 0, got {getattr(self, name)}."
                )

    @classmethod
    def from_result(
        cls,
        result: Dict[str, Any],
        timestamp: Optional[datetime] = None,
        *,
        strict: bool = False,
    ) -> "Layer1RawMetrics":
        """
        Build a Layer1 from an analysis-package result dict.

        ``strict=True`` raises ``KeyError`` on missing required keys
        (``accuracy``, ``energy_kwh``, ``carbon_kg``, ``latency_ms``).
        """
        def _require(key: str, default: float = 0.0) -> float:
            if key in result:
                try:
                    v = float(result[key])
                    return v if math.isfinite(v) else default
                except (TypeError, ValueError):
                    return default
            if strict:
                raise KeyError(f"required result key missing: {key}")
            return default

        def _opt_float(key: str) -> Optional[float]:
            if key not in result:
                return None
            v = result[key]
            if v is None:
                return None
            try:
                f = float(v)
            except (TypeError, ValueError):
                return None
            return f if math.isfinite(f) else None

        def _int(key: str, default: int = 0) -> int:
            v = result.get(key, default)
            try:
                return int(v)
            except (TypeError, ValueError, OverflowError):
                return default

        accuracy = _require("accuracy")
        energy_kwh = _require("energy_kwh")
        carbon_kg = _require("carbon_kg")
        latency_ms = _require("latency_ms")

        # Carbon separation: prefer explicit field, fall back to derived
        carbon_operational_kg = _opt_float("carbon_operational_kg")
        if carbon_operational_kg is None:
            carbon_operational_kg = carbon_kg

        carbon_contractual_kg = _opt_float("carbon_contractual_kg") or 0.0

        return cls(
            accuracy=accuracy,
            energy_wh=energy_kwh * 1000.0,
            carbon_co2_g=carbon_kg * 1000.0,
            latency_ms=latency_ms,
            timestamp=timestamp or datetime.now(timezone.utc),
            run_id=result.get("run_id"),
            task_id=result.get("task_id"),
            agent_id=result.get("agent_id"),
            deployment_id=result.get("deployment_id"),
            policy_version=result.get("policy_version"),
            system_version=result.get("system_version"),
            precision=result.get("precision"),
            hardware=result.get("hardware"),
            region=result.get("region"),
            carbon_operational_co2_g=(
                carbon_operational_kg * 1000.0
            ),
            carbon_contractual_co2_g=carbon_contractual_kg * 1000.0,
            simulated=bool(result.get("simulated", False)),
            chaos_injected=bool(
                result.get("chaos_event")
                or result.get("chaos_injected")
            ),
            source=result.get("source", ProvenanceKind.UNKNOWN.value),
            tool_calls=_int("tool_calls"),
            conversation_depth=_int("conversation_depth"),
            llm_calls=_int("llm_calls"),
            tokens=_int("tokens"),
        )

    def to_dict(self) -> Dict[str, Any]:
        out = asdict(self)
        out["timestamp"] = self.timestamp.isoformat()
        return out


@dataclass
class Layer2NormalizedMetrics:
    """Layer 2: Complexity-normalized metrics for fair comparison."""
    energy_per_task: float
    carbon_per_correct_answer: float
    latency_per_reasoning_step: float
    efficiency_score: float
    task_complexity: float
    complexity_tier: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class Layer3ScenarioScore:
    """Layer 3: Scenario-specific weighted scoring and ranking."""
    weighted_score: float
    scenario_name: str
    weights_used: Dict[str, float]
    rank: Optional[int] = None
    percentile: Optional[float] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# =============================================================================
# Enhancement dataclasses
# =============================================================================

@dataclass
class CarbonInstruments:
    """
    Contractual carbon instruments (RECs, offsets, credits).

    Kept strictly separate from operational emissions.
    """
    instruments: List[Dict[str, Any]] = field(default_factory=list)

    def add(
        self,
        instrument_id: str,
        kind: str,
        quantity: float,
        unit: str,
        vintage_year: Optional[int] = None,
        matching_period: Optional[str] = None,
        producer: Optional[str] = None,
        region: Optional[str] = None,
        retired: bool = False,
        provenance: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.instruments.append({
            "instrument_id": instrument_id,
            "kind": kind,
            "quantity": quantity,
            "unit": unit,
            "vintage_year": vintage_year,
            "matching_period": matching_period,
            "producer": producer,
            "region": region,
            "retired": retired,
            "provenance": provenance or {},
        })

    def contractual_kgco2e(self) -> float:
        """Sum of offset instruments denominated in kgCO2e."""
        return sum(
            i["quantity"] for i in self.instruments
            if i["kind"] == "offset" and i["unit"] == "kgCO2e"
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "instruments": copy.deepcopy(self.instruments),
            "contractual_kgco2e": self.contractual_kgco2e(),
            "count": len(self.instruments),
        }


@dataclass
class MetricProvenance:
    """Per-metric source, freshness, and trust."""
    metric: str
    source_kind: str = ProvenanceKind.UNKNOWN.value
    source_name: Optional[str] = None
    age_seconds: Optional[float] = None
    uncertainty: Optional[float] = None
    simulated: bool = False

    def __post_init__(self) -> None:
        # Normalise Enum → value for consistent string comparisons.
        if isinstance(self.source_kind, Enum):
            object.__setattr__(
                self, "source_kind", self.source_kind.value
            )

    def trust_level(self) -> str:
        if (
            self.simulated
            or self.source_kind == ProvenanceKind.SIMULATED.value
        ):
            return TrustLevel.LOW.value
        if self.source_kind == ProvenanceKind.MEASURED.value:
            return TrustLevel.HIGH.value
        if self.source_kind in (
            ProvenanceKind.ESTIMATED.value,
            ProvenanceKind.FEDERATED.value,
        ):
            return TrustLevel.MEDIUM.value
        return TrustLevel.LOW.value

    def to_dict(self) -> Dict[str, Any]:
        out = asdict(self)
        out["trust_level"] = self.trust_level()
        return out


@dataclass
class ExplanationCard:
    """Structured XAI record for a single decision."""
    decision_id: str
    selected_action: str
    rationale: List[str] = field(default_factory=list)
    alternatives_rejected: List[Dict[str, Any]] = field(
        default_factory=list
    )
    constraints_applied: List[str] = field(default_factory=list)
    confidence: Optional[float] = None
    trade_offs: Dict[str, float] = field(default_factory=dict)
    provenance: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.confidence is not None:
            if (
                isinstance(self.confidence, bool)
                or not isinstance(self.confidence, (int, float))
            ):
                raise LayeredReporterError(
                    "confidence must be a real number or None."
                )
            if not 0.0 <= float(self.confidence) <= 1.0:
                raise LayeredReporterError(
                    f"confidence must be in [0, 1], "
                    f"got {self.confidence}."
                )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class VerificationEvidence:
    """Summary of formal-verification results for a policy."""
    policy_version: str
    verdict: Literal["passed", "failed", "unverified"] = "unverified"
    properties_checked: List[str] = field(default_factory=list)
    properties_passed: List[str] = field(default_factory=list)
    properties_failed: List[str] = field(default_factory=list)
    counterexamples: List[Dict[str, Any]] = field(default_factory=list)
    coverage_pct: float = 0.0
    verifier: Optional[str] = None
    verifier_version: Optional[str] = None

    def __post_init__(self) -> None:
        if self.verdict not in (
            "passed", "failed", "unverified",
        ):
            raise LayeredReporterError(
                f"verdict must be 'passed', 'failed', or 'unverified', "
                f"got {self.verdict!r}."
            )
        if not 0.0 <= float(self.coverage_pct) <= 1.0:
            raise LayeredReporterError(
                f"coverage_pct must be in [0, 1], "
                f"got {self.coverage_pct}."
            )
        checked = set(self.properties_checked)
        if not set(self.properties_passed) <= checked:
            raise LayeredReporterError(
                "properties_passed must be a subset of "
                "properties_checked."
            )
        if not set(self.properties_failed) <= checked:
            raise LayeredReporterError(
                "properties_failed must be a subset of "
                "properties_checked."
            )
        if set(self.properties_passed) & set(self.properties_failed):
            raise LayeredReporterError(
                "properties_passed and properties_failed must be "
                "disjoint."
            )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# =============================================================================
# EvidenceBundle
# =============================================================================

_HASH_STRIP = ("content_hash", "signature", "signer")


@dataclass
class EvidenceBundle:
    """
    Versioned, immutable, signable record of a single run.

    Mutating any content field after :meth:`freeze` raises
    :class:`ImmutabilityError` on the next :meth:`freeze` or
    :meth:`LayeredReporter.verify_signature`.
    """
    # --- Original contract fields ---
    run_id: str
    timestamp: datetime
    system_version: str
    policy_version: str
    dataset_or_workload_id: str
    decision_trace_id: str
    metric_provenance: Dict[str, Any]
    energy_kwh: float
    operational_co2e_kg: float
    quality_metrics: Dict[str, float]
    latency_metrics: Dict[str, float]
    safety_verdict: Dict[str, Any]
    explanation: Dict[str, Any]
    human_review: Optional[Dict[str, Any]]
    carbon_instruments: List[Dict[str, Any]]
    artifact_hashes: Dict[str, str]

    # --- Enhancement extensions ---
    deployment_id: Optional[str] = None
    agent_id: Optional[str] = None
    task_id: Optional[str] = None
    precision: Optional[str] = None
    region: Optional[str] = None

    contractual_co2e_kg: float = 0.0

    helium_units: float = 0.0

    simulated: bool = False
    chaos_injected: bool = False

    verification: Optional[Dict[str, Any]] = None

    federated_contributors: List[str] = field(default_factory=list)

    # --- Metadata ---
    bundle_version: str = "5.0.0"
    created_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    content_hash: Optional[str] = None
    signature: Optional[str] = None
    signer: Optional[str] = None

    def __post_init__(self) -> None:
        for name in ("energy_kwh", "operational_co2e_kg",
                     "contractual_co2e_kg", "helium_units"):
            v = getattr(self, name)
            if isinstance(v, bool) or not isinstance(v, (int, float)):
                raise LayeredReporterError(
                    f"{name} must be a real number, "
                    f"got {type(v).__name__}."
                )
            if not math.isfinite(float(v)):
                raise LayeredReporterError(
                    f"{name} must be finite, got {v!r}."
                )
        for name in ("energy_kwh", "operational_co2e_kg",
                     "contractual_co2e_kg", "helium_units"):
            if getattr(self, name) < 0:
                raise LayeredReporterError(
                    f"{name} must be >= 0, got {getattr(self, name)}."
                )
        for name in ("run_id", "system_version", "policy_version",
                     "dataset_or_workload_id", "decision_trace_id"):
            v = getattr(self, name)
            if not isinstance(v, str) or not v:
                raise LayeredReporterError(
                    f"{name} must be a non-empty string."
                )

    # ---- serialisation ----------------------------------------------

    def to_dict(self) -> Dict[str, Any]:
        """
        Serialise with ISO timestamps and original field names.

        Nested dicts/lists are deep-copied so callers cannot mutate the
        bundle through the returned dictionary.
        """
        return {
            "run_id": self.run_id,
            "timestamp": self.timestamp.isoformat(),
            "system_version": self.system_version,
            "policy_version": self.policy_version,
            "dataset_or_workload_id": self.dataset_or_workload_id,
            "decision_trace_id": self.decision_trace_id,
            "metric_provenance": copy.deepcopy(self.metric_provenance),
            "energy_kwh": self.energy_kwh,
            "operational_co2e_kg": self.operational_co2e_kg,
            "quality_metrics": copy.deepcopy(self.quality_metrics),
            "latency_metrics": copy.deepcopy(self.latency_metrics),
            "safety_verdict": copy.deepcopy(self.safety_verdict),
            "explanation": copy.deepcopy(self.explanation),
            "human_review": (
                copy.deepcopy(self.human_review)
                if self.human_review is not None else None
            ),
            "carbon_instruments": copy.deepcopy(self.carbon_instruments),
            "artifact_hashes": copy.deepcopy(self.artifact_hashes),
            "deployment_id": self.deployment_id,
            "agent_id": self.agent_id,
            "task_id": self.task_id,
            "precision": self.precision,
            "region": self.region,
            "contractual_co2e_kg": self.contractual_co2e_kg,
            "helium_units": self.helium_units,
            "simulated": self.simulated,
            "chaos_injected": self.chaos_injected,
            "verification": (
                copy.deepcopy(self.verification)
                if self.verification is not None else None
            ),
            "federated_contributors": list(self.federated_contributors),
            "bundle_version": self.bundle_version,
            "created_at": self.created_at.isoformat(),
            "content_hash": self.content_hash,
            "signature": self.signature,
            "signer": self.signer,
        }

    @classmethod
    def from_dict(
        cls, payload: Mapping[str, Any]
    ) -> "EvidenceBundle":
        """
        Reconstruct an :class:`EvidenceBundle` from its ``to_dict()``
        representation.

        Unknown fields are logged and dropped (forward compatibility).
        Malformed timestamps raise :class:`LayeredReporterError`.
        Missing required fields raise :class:`LayeredReporterError`.
        """
        if not isinstance(payload, Mapping):
            raise LayeredReporterError(
                f"EvidenceBundle.from_dict expects a Mapping, "
                f"got {type(payload).__name__}."
            )
        d = dict(payload)

        for key in ("timestamp", "created_at"):
            v = d.get(key)
            if v is None:
                continue
            if isinstance(v, str):
                try:
                    dt = datetime.fromisoformat(
                        v.replace("Z", "+00:00")
                    )
                except ValueError as exc:
                    raise LayeredReporterError(
                        f"invalid {key} {v!r}: {exc}"
                    ) from exc
                if not dt.tzinfo:
                    dt = dt.replace(tzinfo=timezone.utc)
                d[key] = dt
            elif isinstance(v, datetime):
                if not v.tzinfo:
                    d[key] = v.replace(tzinfo=timezone.utc)
            else:
                raise LayeredReporterError(
                    f"{key} must be datetime or ISO string, "
                    f"got {type(v).__name__}."
                )

        known = set(EvidenceBundle.__dataclass_fields__)
        unknown = set(d) - known
        if unknown:
            logger.warning(
                "EvidenceBundle.from_dict dropping unknown fields: %s",
                sorted(unknown),
            )

        try:
            return cls(**{k: v for k, v in d.items() if k in known})
        except TypeError as exc:
            raise LayeredReporterError(
                f"EvidenceBundle.from_dict: {exc}"
            ) from exc

    # ---- hashing / freezing -----------------------------------------

    def compute_hash(self) -> str:
        """SHA-256 over the canonical payload (sans hash/signature/signer)."""
        canonical = _canonical_payload(
            self.to_dict(), strip=_HASH_STRIP,
        )
        return "sha256:" + hashlib.sha256(
            canonical.encode("ascii")
        ).hexdigest()

    def freeze(self) -> "EvidenceBundle":
        """
        Compute and store :attr:`content_hash`.

        Idempotent when the bundle is unchanged. Raises
        :class:`ImmutabilityError` when the bundle already has a
        ``content_hash`` that no longer matches its current content
        — i.e. it was mutated after being frozen.
        """
        current = self.compute_hash()
        if self.content_hash is None:
            self.content_hash = current
        elif self.content_hash != current:
            raise ImmutabilityError(
                "bundle was mutated after freeze(); construct a new "
                "bundle to change content fields."
            )
        return self


# =============================================================================
# BundleBuilder
# =============================================================================

class BundleBuilder:
    """Construct an :class:`EvidenceBundle` from a raw analysis result."""

    DEFAULT_SYSTEM_VERSION = "5.0.0"

    @classmethod
    def build(
        cls,
        result: Dict[str, Any],
        *,
        run_id: Optional[str] = None,
        system_version: Optional[str] = None,
        policy_version: Optional[str] = None,
        dataset_or_workload_id: Optional[str] = None,
        verification: Optional[VerificationEvidence] = None,
        explanation: Optional[ExplanationCard] = None,
        human_review: Optional[Dict[str, Any]] = None,
        carbon_instruments: Optional[CarbonInstruments] = None,
        artifact_hashes: Optional[Dict[str, str]] = None,
        contractual_kg_override: Optional[float] = None,
    ) -> EvidenceBundle:
        # ---- numeric fields -----------------------------------------
        energy_kwh = cls._f(result, "energy_kwh")
        operational_kg = cls._f(
            result, "carbon_operational_kg",
            default=cls._f(result, "carbon_kg"),
        )
        helium_units = cls._f(result, "helium_units")

        # ---- contractual: distinguish "not set" from explicit 0 ----
        if contractual_kg_override is not None:
            contractual_kg = float(contractual_kg_override)
        else:
            raw = result.get("carbon_contractual_kg")
            if raw is not None:
                try:
                    f = float(raw)
                    contractual_kg = f if math.isfinite(f) else 0.0
                except (TypeError, ValueError):
                    contractual_kg = 0.0
            elif carbon_instruments is not None:
                contractual_kg = carbon_instruments.contractual_kgco2e()
            else:
                contractual_kg = 0.0

        # ---- provenance ---------------------------------------------
        provenance = cls._build_provenance(result)

        # ---- carbon instruments summary -----------------------------
        instruments_list: List[Dict[str, Any]] = []
        if carbon_instruments is not None:
            instruments_list = copy.deepcopy(
                carbon_instruments.instruments
            )

        # ---- safety verdict -----------------------------------------
        safety_verdict: Dict[str, Any] = {
            "verdict": result.get("safety_verdict", "n/a"),
            "evidence_id": result.get("safety_evidence_id"),
            "violations": copy.deepcopy(
                result.get("safety_violations", [])
            ),
        }
        if verification is not None:
            safety_verdict["verification"] = verification.to_dict()

        # ---- explanation --------------------------------------------
        explanation_dict = (
            explanation.to_dict() if explanation is not None
            else copy.deepcopy(result.get("explanation") or {})
        )

        # ---- quality / latency bags ---------------------------------
        quality_metrics = {
            "accuracy": cls._f(result, "accuracy"),
            "quality_score": cls._f(
                result, "quality_score",
                default=cls._f(result, "accuracy"),
            ),
        }
        latency_metrics = {
            "latency_ms": cls._f(result, "latency_ms"),
            "tool_calls": cls._i(result, "tool_calls"),
            "conversation_depth": cls._i(result, "conversation_depth"),
            "llm_calls": cls._i(result, "llm_calls"),
            "tokens": cls._i(result, "tokens"),
        }

        bundle = EvidenceBundle(
            run_id=(
                run_id
                or result.get("run_id")
                or f"run-{uuid.uuid4().hex[:16]}"
            ),
            timestamp=cls._dt(result.get("timestamp")),
            system_version=(
                system_version or cls.DEFAULT_SYSTEM_VERSION
            ),
            policy_version=(
                policy_version
                or result.get("policy_version", "unknown")
            ),
            dataset_or_workload_id=(
                dataset_or_workload_id
                or result.get("dataset_or_workload_id")
                or result.get("task_id", "unknown")
            ),
            decision_trace_id=(
                result.get("decision_trace_id")
                or result.get("trace_id")
                or f"trace-{uuid.uuid4().hex[:16]}"
            ),
            metric_provenance=provenance,
            energy_kwh=energy_kwh,
            operational_co2e_kg=operational_kg,
            quality_metrics=quality_metrics,
            latency_metrics=latency_metrics,
            safety_verdict=safety_verdict,
            explanation=explanation_dict,
            human_review=copy.deepcopy(human_review),
            carbon_instruments=instruments_list,
            artifact_hashes=dict(artifact_hashes or {}),
            deployment_id=result.get("deployment_id"),
            agent_id=result.get("agent_id"),
            task_id=result.get("task_id"),
            precision=result.get("precision"),
            region=result.get("region"),
            contractual_co2e_kg=contractual_kg,
            helium_units=helium_units,
            simulated=bool(result.get("simulated", False)),
            chaos_injected=bool(
                result.get("chaos_event")
                or result.get("chaos_injected")
            ),
            verification=(
                verification.to_dict() if verification else None
            ),
            federated_contributors=list(
                result.get("federated_contributors", [])
            ),
        )
        return bundle.freeze()

    # ---- helpers ---------------------------------------------------

    @staticmethod
    def _f(
        d: Dict[str, Any], key: str, default: float = 0.0
    ) -> float:
        v = d.get(key, default)
        try:
            f = float(v)
            return f if math.isfinite(f) else default
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _i(
        d: Dict[str, Any], key: str, default: int = 0
    ) -> int:
        v = d.get(key, default)
        try:
            return int(v)
        except (TypeError, ValueError, OverflowError):
            return default

    @staticmethod
    def _dt(value: Any) -> datetime:
        if isinstance(value, datetime):
            return (
                value if value.tzinfo
                else value.replace(tzinfo=timezone.utc)
            )
        if isinstance(value, str):
            try:
                dt = datetime.fromisoformat(
                    value.replace("Z", "+00:00")
                )
            except ValueError:
                logger.warning(
                    "_dt: unparseable timestamp %r; using now().",
                    value,
                )
                return datetime.now(timezone.utc)
            return (
                dt if dt.tzinfo
                else dt.replace(tzinfo=timezone.utc)
            )
        if (
            isinstance(value, (int, float))
            and not isinstance(value, bool)
            and math.isfinite(float(value))
        ):
            try:
                return datetime.fromtimestamp(
                    float(value), tz=timezone.utc
                )
            except (OverflowError, OSError) as exc:
                logger.warning(
                    "_dt: out-of-range epoch %r: %s", value, exc,
                )
                return datetime.now(timezone.utc)
        return datetime.now(timezone.utc)

    @staticmethod
    def _build_provenance(result: Dict[str, Any]) -> Dict[str, Any]:
        prov: Dict[str, Any] = {}
        energy_src = (
            result.get("energy_source")
            or result.get("source")
            or "unknown"
        )
        prov["energy"] = MetricProvenance(
            metric="energy",
            source_kind=(
                ProvenanceKind.MEASURED.value
                if energy_src == "codecarbon"
                else ProvenanceKind.SIMULATED.value
                if energy_src == "fallback"
                else ProvenanceKind.UNKNOWN.value
            ),
            source_name=energy_src,
            simulated=(energy_src == "fallback"),
        ).to_dict()

        carbon_src = result.get("carbon_source", "estimated")
        prov["carbon"] = MetricProvenance(
            metric="carbon",
            source_kind=(
                ProvenanceKind.MEASURED.value
                if carbon_src == "operational"
                else ProvenanceKind.ESTIMATED.value
            ),
            source_name=carbon_src,
        ).to_dict()

        prov["latency"] = MetricProvenance(
            metric="latency",
            source_kind=ProvenanceKind.MEASURED.value,
            source_name="wall_clock",
        ).to_dict()

        prov["memory"] = MetricProvenance(
            metric="memory",
            source_kind=ProvenanceKind.MEASURED.value,
            source_name="psutil",
        ).to_dict()

        return prov


# =============================================================================
# LayeredReporter
# =============================================================================

class LayeredReporter:
    """
    Enhanced three-layer reporter that also produces signed
    EvidenceBundles for downstream renderers.

    Backward-compatible: all original methods and signatures are
    preserved.
    """

    DEFAULT_SCENARIOS: Dict[str, Dict[str, float]] = {
        "performance": {
            "accuracy": 0.5, "energy": 0.2,
            "carbon": 0.2, "latency": 0.1,
        },
        "sustainability": {
            "accuracy": 0.2, "energy": 0.35,
            "carbon": 0.35, "latency": 0.1,
        },
        "balanced": {
            "accuracy": 0.35, "energy": 0.25,
            "carbon": 0.25, "latency": 0.15,
        },
    }

    def __init__(
        self,
        scenarios: Optional[Dict[str, Dict[str, float]]] = None,
        *,
        run_id: Optional[str] = None,
        system_version: str = "5.0.0",
    ):
        if scenarios is None:
            self.scenarios = self.DEFAULT_SCENARIOS
        else:
            if not isinstance(scenarios, dict):
                raise LayeredReporterError(
                    f"scenarios must be a dict, "
                    f"got {type(scenarios).__name__}."
                )
            self.scenarios = scenarios
        self.run_id = run_id or f"run-{uuid.uuid4().hex[:16]}"
        self.system_version = system_version

    # ---- original methods ------------------------------------------

    def generate_layer1(
        self,
        result: Dict[str, Any],
        timestamp: Optional[datetime] = None,
    ) -> Layer1RawMetrics:
        return Layer1RawMetrics.from_result(result, timestamp)

    def generate_layer2(
        self,
        result: Dict[str, Any],
        complexity: Any,
    ) -> Layer2NormalizedMetrics:
        energy_kwh = float(result.get("energy_kwh", 0.0))
        carbon_kg = float(result.get("carbon_kg", 0.0))
        latency_ms = float(result.get("latency_ms", 0.0))
        accuracy = float(result.get("accuracy", 0.0))

        complexity_score = 1.0
        tier = "unknown"
        if complexity is not None:
            try:
                complexity_score = float(
                    complexity.compute_composite_score()
                )
                tier = (
                    str(complexity.tier)
                    if hasattr(complexity, "tier") else "unknown"
                )
            except Exception as exc:
                logger.debug(
                    "complexity.compute_composite_score() failed: %s",
                    exc,
                )
        complexity_score = max(complexity_score, 1e-6)

        energy_per_task = energy_kwh / complexity_score
        carbon_per_correct = (
            carbon_kg / max(accuracy, 1e-6)
        ) * 1000.0
        latency_per_step = latency_ms / max(complexity_score, 1e-6)
        efficiency = accuracy / max(energy_per_task + 1e-9, 1e-9)

        return Layer2NormalizedMetrics(
            energy_per_task=energy_per_task,
            carbon_per_correct_answer=carbon_per_correct,
            latency_per_reasoning_step=latency_per_step,
            efficiency_score=efficiency,
            task_complexity=complexity_score,
            complexity_tier=tier,
        )

    def generate_layer3(
        self,
        layer2_metrics: Layer2NormalizedMetrics,
        scenario: str = "balanced",
    ) -> Layer3ScenarioScore:
        weights = self.scenarios.get(
            scenario, self.scenarios["balanced"]
        )

        energy_score = 1.0 / (
            1.0 + layer2_metrics.energy_per_task
        )
        carbon_score = 1.0 / (
            1.0 + layer2_metrics.carbon_per_correct_answer
        )
        latency_score = 1.0 / (
            1.0 + layer2_metrics.latency_per_reasoning_step
        )
        accuracy_score = min(
            1.0, layer2_metrics.efficiency_score / 1000.0
        )

        weighted_score = (
            weights["accuracy"] * accuracy_score
            + weights["energy"] * energy_score
            + weights["carbon"] * carbon_score
            + weights["latency"] * latency_score
        )

        return Layer3ScenarioScore(
            weighted_score=weighted_score,
            scenario_name=scenario,
            weights_used=dict(weights),
        )

    def generate_full_report(
        self,
        result: Dict[str, Any],
        complexity: Any = None,
        scenario: str = "balanced",
    ) -> Dict[str, Any]:
        layer1 = self.generate_layer1(result)
        layer2 = self.generate_layer2(result, complexity)
        layer3 = self.generate_layer3(layer2, scenario)
        return {
            "layer1": layer1.to_dict(),
            "layer2": layer2.to_dict(),
            "layer3": layer3.to_dict(),
        }

    # ---- enhanced methods ------------------------------------------

    def build_evidence_bundle(
        self,
        result: Dict[str, Any],
        *,
        complexity: Any = None,
        verification: Optional[VerificationEvidence] = None,
        explanation: Optional[ExplanationCard] = None,
        human_review: Optional[Dict[str, Any]] = None,
        carbon_instruments: Optional[CarbonInstruments] = None,
        artifact_hashes: Optional[Dict[str, str]] = None,
        policy_version: Optional[str] = None,
    ) -> EvidenceBundle:
        """Build a complete, hashed EvidenceBundle from a raw result."""
        return BundleBuilder.build(
            result,
            run_id=self.run_id,
            system_version=self.system_version,
            policy_version=policy_version,
            verification=verification,
            explanation=explanation,
            human_review=human_review,
            carbon_instruments=carbon_instruments,
            artifact_hashes=artifact_hashes,
        )

    def sign_bundle(
        self,
        bundle: EvidenceBundle,
        key: str | bytes,
        signer: Optional[str] = None,
    ) -> EvidenceBundle:
        """
        Attach an HMAC-SHA256 signature to a bundle.

        ``key`` must be a non-empty ``str`` or ``bytes``. The bundle is
        re-frozen first, so a post-freeze mutation raises
        :class:`ImmutabilityError`.
        """
        if isinstance(key, str):
            if not key:
                raise LayeredReporterError("key must be non-empty.")
            key_bytes = key.encode("utf-8")
        elif isinstance(key, bytes):
            if not key:
                raise LayeredReporterError("key must be non-empty.")
            key_bytes = key
        else:
            raise LayeredReporterError(
                f"key must be str or bytes, "
                f"got {type(key).__name__}."
            )

        # Re-freeze (raises ImmutabilityError if content changed).
        bundle.freeze()
        sig = hmac.new(
            key_bytes,
            bundle.content_hash.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        bundle.signature = f"hmac-sha256:{sig}"
        bundle.signer = signer or "local"
        return bundle

    def verify_signature(
        self,
        bundle: EvidenceBundle,
        key: str | bytes,
    ) -> bool:
        """
        Return True iff:

        - ``bundle.content_hash`` matches the current content
          (detects post-freeze mutation).
        - ``bundle.signature`` is a valid HMAC-SHA256 over
          ``bundle.content_hash`` with ``key`` (detects forgery).
        """
        if not bundle.content_hash or not bundle.signature:
            return False

        # (1) Content hash matches current content.
        try:
            current_hash = bundle.compute_hash()
        except (TypeError, ValueError):
            return False
        if current_hash != bundle.content_hash:
            return False

        # (2) Signature is valid over the content hash.
        if not bundle.signature.startswith("hmac-sha256:"):
            return False
        key_bytes = (
            key.encode("utf-8") if isinstance(key, str) else key
        )
        expected = hmac.new(
            key_bytes,
            bundle.content_hash.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        return hmac.compare_digest(
            bundle.signature[len("hmac-sha256:"):],
            expected,
        )

    def redact(
        self,
        bundle: EvidenceBundle,
        audience: Audience | str,
    ) -> EvidenceBundle:
        """
        Return a bundle with audience-inappropriate fields redacted.

        The returned bundle has ``content_hash``, ``signature``, and
        ``signer`` set to ``None`` — callers must re-sign if they want
        a verifiable redacted bundle. The source bundle is **not**
        mutated.
        """
        if isinstance(audience, str):
            try:
                audience = Audience(audience.lower())
            except ValueError as exc:
                raise LayeredReporterError(
                    f"unknown audience {audience!r}: {exc}"
                ) from exc
        if not isinstance(audience, Audience):
            raise LayeredReporterError(
                f"audience must be Audience or str, "
                f"got {type(audience).__name__}."
            )

        payload = copy.deepcopy(bundle.to_dict())

        if audience in (Audience.OPERATOR, Audience.EXECUTIVE):
            payload["artifact_hashes"] = {}
            payload["verification"] = None
            for _, p in list(
                payload.get("metric_provenance", {}).items()
            ):
                if isinstance(p, dict):
                    p.pop("source_name", None)
                    p.pop("uncertainty", None)
            if isinstance(payload.get("safety_verdict"), dict):
                payload["safety_verdict"].pop("violations", None)

        # Redaction invalidates the original hash and signature.
        payload["content_hash"] = None
        payload["signature"] = None
        payload["signer"] = None

        return EvidenceBundle.from_dict(payload)


# =============================================================================
# Backwards-compat shim
# =============================================================================

def _bundle_from_dict(payload: Dict[str, Any]) -> EvidenceBundle:
    """Deprecated: use :meth:`EvidenceBundle.from_dict`."""
    return EvidenceBundle.from_dict(payload)


# =============================================================================
# Demo
# =============================================================================

if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    print("VERSION:", VERSION)

    # ---- Original behavior -----------------------------------------
    print("\n=== Original behavior ===")
    reporter = LayeredReporter()

    result = {
        "accuracy": 0.92,
        "energy_kwh": 0.045,
        "carbon_kg": 0.018,
        "latency_ms": 120.0,
        "task_id": "task-1",
    }

    layer1 = reporter.generate_layer1(result)
    print(
        f"  Layer1 accuracy: {layer1.accuracy}, "
        f"energy_wh: {layer1.energy_wh}"
    )

    full = reporter.generate_full_report(result)
    print(f"  Full report keys: {list(full.keys())}")
    print(
        f"  Layer3 weighted_score: "
        f"{full['layer3']['weighted_score']:.4f}"
    )

    # ---- Enhanced: EvidenceBundle ----------------------------------
    print("\n=== Enhanced EvidenceBundle ===")
    explanation = ExplanationCard(
        decision_id="dec-1",
        selected_action="route_to_lora",
        rationale=[
            "lowest carbon route",
            "quality above threshold",
        ],
        alternatives_rejected=[
            {"action": "full_finetuning", "reason": "policy violation"},
        ],
        constraints_applied=[
            "max_energy_per_task_wh", "policy_v5.0.1",
        ],
        confidence=0.92,
        trade_offs={"energy_saved_pct": 45.0, "accuracy_delta": -0.02},
    )

    verification = VerificationEvidence(
        policy_version="v5.0.1",
        verdict="passed",
        properties_checked=[
            "DeadlineRespected", "CarbonBudgetNotExceeded",
        ],
        properties_passed=[
            "DeadlineRespected", "CarbonBudgetNotExceeded",
        ],
        coverage_pct=1.0,
        verifier="temporal_logic_monitor",
        verifier_version="5.0.0",
    )

    instruments = CarbonInstruments()
    instruments.add(
        instrument_id="rec-2026-001",
        kind="rec",
        quantity=5.0,
        unit="MWh",
        vintage_year=2026,
        matching_period="2026-03",
        producer="Pacific Wind Co.",
    )
    instruments.add(
        instrument_id="offset-2026-042",
        kind="offset",
        quantity=10.0,
        unit="kgCO2e",
        vintage_year=2026,
    )

    result_enriched = {
        **result,
        "run_id": "run-001",
        "deployment_id": "us-ca-prod-01",
        "agent_id": "agent-A",
        "policy_version": "v5.0.1",
        "precision": "int8",
        "region": "US-CA",
        "carbon_operational_kg": 0.018,
        "carbon_contractual_kg": 0.010,
        "helium_units": 0.0003,
        "energy_source": "codecarbon",
        "source": "measured",
    }

    bundle = reporter.build_evidence_bundle(
        result_enriched,
        explanation=explanation,
        verification=verification,
        carbon_instruments=instruments,
        policy_version="v5.0.1",
        artifact_hashes={"model.bin": "sha256:abc123"},
    )

    print(f"  Bundle run_id:       {bundle.run_id}")
    print(f"  Content hash:        {bundle.content_hash[:32]}...")
    print(
        f"  Operational CO2e:    {bundle.operational_co2e_kg:.4f} kg"
    )
    print(
        f"  Contractual CO2e:    {bundle.contractual_co2e_kg:.4f} kg"
    )
    print("  (Never summed — kept separate per proposal)")
    print(f"  Helium units:        {bundle.helium_units:.6f}")
    print(f"  Simulated:           {bundle.simulated}")
    print(
        f"  Verification:        "
        f"{bundle.verification.get('verdict', 'unknown')}"
    )
    print(f"  Carbon instruments:  {len(bundle.carbon_instruments)}")

    # ---- Signing ---------------------------------------------------
    print("\n=== Signing ===")
    KEY = "secret-key"
    bundle = reporter.sign_bundle(
        bundle, key=KEY, signer="us-ca-prod-01"
    )
    print(f"  Signature: {bundle.signature[:32]}...")
    print(f"  Signer:    {bundle.signer}")
    print(
        f"  verify_signature: "
        f"{reporter.verify_signature(bundle, KEY)}"
    )

    # ---- Mutation detection ---------------------------------------
    print("\n=== Mutation detection ===")
    try:
        bundle.policy_version = "tampered"
        bundle.freeze()
    except ImmutabilityError as exc:
        print(f"  freeze() correctly raised: {exc}")
    # Reset for subsequent steps.
    bundle.policy_version = "v5.0.1"

    # Simulate a post-signing mutation in a way that DOESN'T trigger
    # the freeze check (no re-freeze).
    saved_hash = bundle.content_hash
    bundle.policy_version = "tampered"
    is_verified = reporter.verify_signature(bundle, KEY)
    print(f"  verify_signature on mutated bundle: {is_verified}")
    bundle.policy_version = "v5.0.1"

    # ---- Redaction -------------------------------------------------
    print("\n=== Redaction (executive view) ===")
    original_hashes = dict(bundle.artifact_hashes)
    original_signature = bundle.signature
    redacted = reporter.redact(bundle, Audience.EXECUTIVE)

    print(
        f"  Source artifact_hashes unchanged: "
        f"{bundle.artifact_hashes == original_hashes}"
    )
    print(
        f"  Source signature unchanged: "
        f"{bundle.signature == original_signature}"
    )
    print(f"  Redacted artifact_hashes: {redacted.artifact_hashes}")
    print(f"  Redacted verification:    {bool(redacted.verification)}")
    print(f"  Redacted content_hash:    {redacted.content_hash}")
    print(f"  Redacted signature:       {redacted.signature}")

    # ---- Re-signing redacted bundle --------------------------------
    print("\n=== Re-sign redacted bundle ===")
    redacted = reporter.sign_bundle(
        redacted, key=KEY, signer="redaction-gateway",
    )
    print(
        f"  verify_signature: "
        f"{reporter.verify_signature(redacted, KEY)}"
    )
    print(f"  Re-hash: {redacted.content_hash[:32]}...")

    # ---- from_dict round-trip -------------------------------------
    print("\n=== from_dict round-trip ===")
    payload = bundle.to_dict()
    restored = EvidenceBundle.from_dict(payload)
    print(
        f"  content_hash match: "
        f"{restored.content_hash == bundle.content_hash}"
    )
    print(
        f"  signature match:    "
        f"{restored.signature == bundle.signature}"
    )

    # ---- Validation examples --------------------------------------
    print("\n=== Validation ===")
    for bad in (
        dict(
            policy_version="v", verdict="bogus",  # type: ignore[arg-type]
        ),
        dict(
            policy_version="v", verdict="passed",
            coverage_pct=1.5,
        ),
        dict(
            policy_version="v", verdict="passed",
            properties_checked=["A"],
            properties_passed=["B"],
        ),
    ):
        try:
            VerificationEvidence(**bad)  # type: ignore[arg-type]
        except LayeredReporterError as exc:
            print(f"  Rejected VerificationEvidence: {exc}")

    try:
        ExplanationCard(
            decision_id="d", selected_action="a", confidence=1.5,
        )
    except LayeredReporterError as exc:
        print(f"  Rejected ExplanationCard: {exc}")

    try:
        reporter.redact(bundle, "bogus")  # type: ignore[arg-type]
    except LayeredReporterError as exc:
        print(f"  Rejected redact audience: {exc}")

    print("\nSmoke test passed.")
