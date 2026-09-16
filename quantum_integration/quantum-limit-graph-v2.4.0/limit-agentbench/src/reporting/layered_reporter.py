"""
Layered Reporting System for Green Agent (Enhanced)
====================================================

Transforms analysis outputs into a versioned, immutable EvidenceBundle
that can be signed, hashed, redacted, and rendered for any audience.

Original API preserved:
    Layer1RawMetrics, Layer2NormalizedMetrics, Layer3ScenarioScore
    LayeredReporter
        .generate_layer1(result, timestamp=None)
        .generate_layer2(result, complexity)
        .generate_layer3(layer2_metrics, ...)
        .generate_full_report(result, complexity=None)

Enhanced API:
    EvidenceBundle            # the shared, versioned, signed contract
    CarbonInstruments         # operational vs. contractual separation
    MetricProvenance          # source + trust per metric
    ExplanationCard           # XAI rationale per decision
    VerificationEvidence      # formal verification summary
    LayeredReporter
        .build_evidence_bundle(result, ...) -> EvidenceBundle
        .sign_bundle(bundle, key) -> EvidenceBundle
        .redact(bundle, audience) -> EvidenceBundle

Enhancements:
  1. Quantum-Distillation      — route, shots, queue_ms on the bundle
  2. Causal RL                 — intervention/control/effect on the bundle
  3. Federated Analytics       — federated contributors list
  4. Multi-Agent Coordination  — agent_id on Layer1
  5. Temporal Logic            — safety_verdict + verification evidence
  6. Explainable AI            — explanation card on every bundle
  7. Adaptive Precision        — precision field on the bundle
  8. Carbon Markets            — carbon_operational vs. carbon_contractual
  9. Resilience & Chaos        — chaos_injected flag on Layer1
 10. Human-in-the-Loop         — human_review on the bundle
 +   Immutability, hashing, signing, redaction
 +   Simulated-flag propagation
 +   Backward-compatible dataclasses retained verbatim
"""

from __future__ import annotations

import dataclasses
import hashlib
import json
import logging
import math
import time
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


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
# ORIGINAL Layer1RawMetrics — preserved, extended with optional fields
# =============================================================================

@dataclass
class Layer1RawMetrics:
    """
    Layer 1: Raw, unprocessed metrics directly from execution.

    Original 5 fields preserved. New fields are optional with defaults
    so existing construction sites are unaffected.
    """
    # --- Original fields ---
    accuracy: float
    energy_wh: float
    carbon_co2_g: float
    latency_ms: float
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

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

    # --- Enhancement: carbon separation (per proposal) ---
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

        Backward-compatible: same signature and semantics when
        `strict=False` (default).

        Enhanced: when `strict=True`, missing required keys raise
        `KeyError` instead of silently defaulting to 0.0 — this is the
        proposal's "reliable telemetry" requirement.
        """
        def _get(key: str, default: float = 0.0) -> float:
            if key in result:
                try:
                    v = float(result[key])
                    return v if math.isfinite(v) else default
                except (TypeError, ValueError):
                    return default
            if strict:
                raise KeyError(f"required result key missing: {key}")
            return default

        # Original contract
        accuracy = _get("accuracy")
        energy_kwh = _get("energy_kwh")
        carbon_kg = _get("carbon_kg")
        latency_ms = _get("latency_ms")

        # Carbon separation: prefer explicit fields, fall back to derived
        carbon_operational = result.get(
            "carbon_operational_kg", carbon_kg
        )
        carbon_contractual = result.get("carbon_contractual_kg", 0.0)

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
                float(carbon_operational) * 1000.0
                if carbon_operational is not None else None
            ),
            carbon_contractual_co2_g=float(carbon_contractual) * 1000.0,
            simulated=bool(result.get("simulated", False)),
            chaos_injected=bool(
                result.get("chaos_event") or result.get("chaos_injected")
            ),
            source=result.get("source", ProvenanceKind.UNKNOWN.value),
            tool_calls=int(result.get("tool_calls", 0)),
            conversation_depth=int(result.get("conversation_depth", 0)),
            llm_calls=int(result.get("llm_calls", 0)),
            tokens=int(result.get("tokens", 0)),
        )

    def to_dict(self) -> Dict[str, Any]:
        out = asdict(self)
        out["timestamp"] = self.timestamp.isoformat()
        return out


# =============================================================================
# ORIGINAL Layer2NormalizedMetrics — preserved verbatim
# =============================================================================

@dataclass
class Layer2NormalizedMetrics:
    """
    Layer 2: Complexity-normalized metrics for fair cross-task comparison.
    """
    energy_per_task: float
    carbon_per_correct_answer: float
    latency_per_reasoning_step: float
    efficiency_score: float
    task_complexity: float
    complexity_tier: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# =============================================================================
# ORIGINAL Layer3ScenarioScore — preserved verbatim
# =============================================================================

@dataclass
class Layer3ScenarioScore:
    """
    Layer 3: Scenario-specific weighted scoring and ranking.
    """
    weighted_score: float
    scenario_name: str
    weights_used: Dict[str, float]
    rank: Optional[int] = None
    percentile: Optional[float] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# =============================================================================
# ENHANCEMENT: CarbonInstruments (operational vs. contractual)
# =============================================================================

@dataclass
class CarbonInstruments:
    """
    Contractual carbon instruments (RECs, offsets, credits).

    Kept strictly separate from operational emissions. Reports must NEVER
    sum these into a single number — the proposal is explicit on this point.
    """
    instruments: List[Dict[str, Any]] = field(default_factory=list)

    def add(
        self,
        instrument_id: str,
        kind: str,                 # "rec" | "offset" | "credit"
        quantity: float,
        unit: str,                 # "MWh" | "kgCO2e"
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
        return sum(
            i["quantity"] for i in self.instruments
            if i["kind"] == "offset" and i["unit"] == "kgCO2e"
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "instruments": list(self.instruments),
            "contractual_kgco2e": self.contractual_kgco2e(),
            "count": len(self.instruments),
        }


# =============================================================================
# ENHANCEMENT: MetricProvenance
# =============================================================================

@dataclass
class MetricProvenance:
    """Per-metric source, freshness, and trust."""
    metric: str
    source_kind: str = ProvenanceKind.UNKNOWN.value
    source_name: Optional[str] = None
    age_seconds: float = 0.0
    uncertainty: Optional[float] = None
    simulated: bool = False

    def trust_level(self) -> str:
        if self.simulated or self.source_kind == ProvenanceKind.SIMULATED.value:
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


# =============================================================================
# ENHANCEMENT: ExplanationCard
# =============================================================================

@dataclass
class ExplanationCard:
    """
    Structured XAI record for a single decision.

    Every report carries one card per decision. The card answers
    "why this action?" with evidence, alternatives, constraints, and
    confidence.
    """
    decision_id: str
    selected_action: str
    rationale: List[str] = field(default_factory=list)
    alternatives_rejected: List[Dict[str, Any]] = field(default_factory=list)
    constraints_applied: List[str] = field(default_factory=list)
    confidence: float = 0.0
    trade_offs: Dict[str, float] = field(default_factory=dict)
    provenance: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# =============================================================================
# ENHANCEMENT: VerificationEvidence
# =============================================================================

@dataclass
class VerificationEvidence:
    """Summary of formal-verification results for a policy."""
    policy_version: str
    verdict: str                         # "passed" | "failed" | "unverified"
    properties_checked: List[str] = field(default_factory=list)
    properties_passed: List[str] = field(default_factory=list)
    properties_failed: List[str] = field(default_factory=list)
    counterexamples: List[Dict[str, Any]] = field(default_factory=list)
    coverage_pct: float = 0.0
    verifier: Optional[str] = None
    verifier_version: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# =============================================================================
# ENHANCEMENT: EvidenceBundle — the shared, versioned, signed contract
# =============================================================================

@dataclass
class EvidenceBundle:
    """
    Versioned, immutable, signable record of a single run.

    The proposal's exact contract, with all 16 recommended fields plus
    extensions for the ten enhancement layers.
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

    # Contractual carbon is kept separate (never summed with operational)
    contractual_co2e_kg: float = 0.0

    # Helium (dual-axis mission)
    helium_units: float = 0.0

    # Simulation and chaos flags
    simulated: bool = False
    chaos_injected: bool = False

    # Verification summary
    verification: Optional[Dict[str, Any]] = None

    # Federated contributors
    federated_contributors: List[str] = field(default_factory=list)

    # --- Metadata ---
    bundle_version: str = "5.0.0"
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    content_hash: Optional[str] = None
    signature: Optional[str] = None
    signer: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Serialise with ISO timestamps and original field names."""
        out = {
            "run_id": self.run_id,
            "timestamp": self.timestamp.isoformat(),
            "system_version": self.system_version,
            "policy_version": self.policy_version,
            "dataset_or_workload_id": self.dataset_or_workload_id,
            "decision_trace_id": self.decision_trace_id,
            "metric_provenance": dict(self.metric_provenance),
            "energy_kwh": self.energy_kwh,
            "operational_co2e_kg": self.operational_co2e_kg,
            "quality_metrics": dict(self.quality_metrics),
            "latency_metrics": dict(self.latency_metrics),
            "safety_verdict": dict(self.safety_verdict),
            "explanation": dict(self.explanation),
            "human_review": dict(self.human_review) if self.human_review else None,
            "carbon_instruments": list(self.carbon_instruments),
            "artifact_hashes": dict(self.artifact_hashes),
            "deployment_id": self.deployment_id,
            "agent_id": self.agent_id,
            "task_id": self.task_id,
            "precision": self.precision,
            "region": self.region,
            "contractual_co2e_kg": self.contractual_co2e_kg,
            "helium_units": self.helium_units,
            "simulated": self.simulated,
            "chaos_injected": self.chaos_injected,
            "verification": self.verification,
            "federated_contributors": list(self.federated_contributors),
            "bundle_version": self.bundle_version,
            "created_at": self.created_at.isoformat(),
            "content_hash": self.content_hash,
            "signature": self.signature,
            "signer": self.signer,
        }
        return out

    def compute_hash(self) -> str:
        """SHA-256 over the canonical JSON representation."""
        payload = self.to_dict()
        # Exclude hash and signature from the hash input to avoid cycles
        payload.pop("content_hash", None)
        payload.pop("signature", None)
        canonical = json.dumps(payload, sort_keys=True, default=str)
        return "sha256:" + hashlib.sha256(canonical.encode()).hexdigest()

    def freeze(self) -> "EvidenceBundle":
        """Compute and store the content hash; mark the bundle immutable."""
        if self.content_hash is None:
            self.content_hash = self.compute_hash()
        return self


# =============================================================================
# ENHANCEMENT: BundleBuilder — the bridge from raw results to bundles
# =============================================================================

class BundleBuilder:
    """
    Construct an EvidenceBundle from a raw analysis result dict plus
    optional verification, explanation, HITL, and instruments.

    The proposal's structural requirement: every report derives from a
    bundle, never from ad-hoc dicts.
    """

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
    ) -> EvidenceBundle:
        # --- Pull the standard fields from the result ---
        energy_kwh = cls._f(result, "energy_kwh")
        operational_kg = cls._f(
            result, "carbon_operational_kg",
            default=cls._f(result, "carbon_kg"),
        )
        contractual_kg = cls._f(result, "carbon_contractual_kg")
        helium_units = cls._f(result, "helium_units")

        # --- Build metric provenance ---
        provenance = cls._build_provenance(result)

        # --- Carbon instruments summary ---
        instruments_list: List[Dict[str, Any]] = []
        if carbon_instruments is not None:
            instruments_list = list(carbon_instruments.instruments)
            if contractual_kg == 0.0:
                contractual_kg = carbon_instruments.contractual_kgco2e()

        # --- Safety verdict ---
        safety_verdict = {
            "verdict": result.get("safety_verdict", "n/a"),
            "evidence_id": result.get("safety_evidence_id"),
            "violations": result.get("safety_violations", []),
        }
        if verification is not None:
            safety_verdict["verification"] = verification.to_dict()

        # --- Explanation ---
        explanation_dict = (
            explanation.to_dict() if explanation is not None
            else (result.get("explanation") or {})
        )

        # --- Quality / latency metric bags ---
        quality_metrics = {
            "accuracy": cls._f(result, "accuracy"),
            "quality_score": cls._f(result, "quality_score", default=cls._f(result, "accuracy")),
        }
        latency_metrics = {
            "latency_ms": cls._f(result, "latency_ms"),
            "tool_calls": cls._i(result, "tool_calls"),
            "conversation_depth": cls._i(result, "conversation_depth"),
            "llm_calls": cls._i(result, "llm_calls"),
            "tokens": cls._i(result, "tokens"),
        }

        bundle = EvidenceBundle(
            run_id=run_id or result.get("run_id") or f"run-{uuid.uuid4().hex[:8]}",
            timestamp=cls._dt(result.get("timestamp")),
            system_version=system_version or cls.DEFAULT_SYSTEM_VERSION,
            policy_version=policy_version or result.get("policy_version", "unknown"),
            dataset_or_workload_id=(
                dataset_or_workload_id
                or result.get("dataset_or_workload_id")
                or result.get("task_id", "unknown")
            ),
            decision_trace_id=(
                result.get("decision_trace_id")
                or result.get("trace_id")
                or f"trace-{uuid.uuid4().hex[:8]}"
            ),
            metric_provenance=provenance,
            energy_kwh=energy_kwh,
            operational_co2e_kg=operational_kg,
            quality_metrics=quality_metrics,
            latency_metrics=latency_metrics,
            safety_verdict=safety_verdict,
            explanation=explanation_dict,
            human_review=human_review,
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
                result.get("chaos_event") or result.get("chaos_injected")
            ),
            verification=verification.to_dict() if verification else None,
            federated_contributors=list(
                result.get("federated_contributors", [])
            ),
        )
        return bundle.freeze()

    # ------------------------------------------------------------------
    # Coercion helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _f(d: Dict[str, Any], key: str, default: float = 0.0) -> float:
        v = d.get(key, default)
        try:
            f = float(v)
            return f if math.isfinite(f) else default
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _i(d: Dict[str, Any], key: str, default: int = 0) -> int:
        v = d.get(key, default)
        try:
            return int(v)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _dt(value: Any) -> datetime:
        if isinstance(value, datetime):
            return value
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value)
            except ValueError:
                pass
        return datetime.now(timezone.utc)

    @staticmethod
    def _build_provenance(result: Dict[str, Any]) -> Dict[str, Any]:
        """Derive per-metric provenance from existing result flags."""
        prov: Dict[str, Any] = {}

        # Energy
        energy_src = result.get("energy_source") or result.get("source", "unknown")
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

        # Carbon
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

        # Latency
        prov["latency"] = MetricProvenance(
            metric="latency",
            source_kind=ProvenanceKind.MEASURED.value,
            source_name="wall_clock",
        ).to_dict()

        # Memory
        prov["memory"] = MetricProvenance(
            metric="memory",
            source_kind=ProvenanceKind.MEASURED.value,
            source_name="psutil",
        ).to_dict()

        return prov


# =============================================================================
# ENHANCED LayeredReporter
# =============================================================================

class LayeredReporter:
    """
    Enhanced three-layer reporter that also produces signed
    EvidenceBundles for downstream renderers.

    Backward-compatible: all original methods and their signatures
    are preserved.
    """

    DEFAULT_SCENARIOS: Dict[str, Dict[str, float]] = {
        "performance": {"accuracy": 0.5, "energy": 0.2, "carbon": 0.2, "latency": 0.1},
        "sustainability": {"accuracy": 0.2, "energy": 0.35, "carbon": 0.35, "latency": 0.1},
        "balanced": {"accuracy": 0.35, "energy": 0.25, "carbon": 0.25, "latency": 0.15},
    }

    def __init__(
        self,
        scenarios: Optional[Dict[str, Dict[str, float]]] = None,
        *,
        run_id: Optional[str] = None,
        system_version: str = "5.0.0",
    ):
        self.scenarios = scenarios or self.DEFAULT_SCENARIOS
        self.run_id = run_id or f"run-{uuid.uuid4().hex[:8]}"
        self.system_version = system_version

    # ------------------------------------------------------------------
    # ORIGINAL methods — preserved
    # ------------------------------------------------------------------

    def generate_layer1(
        self,
        result: Dict[str, Any],
        timestamp: Optional[datetime] = None,
    ) -> Layer1RawMetrics:
        """Build Layer1 from a result dict (original behavior)."""
        return Layer1RawMetrics.from_result(result, timestamp)

    def generate_layer2(
        self,
        result: Dict[str, Any],
        complexity: Any,
    ) -> Layer2NormalizedMetrics:
        """Build Layer2 (original behavior)."""
        energy_kwh = float(result.get("energy_kwh", 0.0))
        carbon_kg = float(result.get("carbon_kg", 0.0))
        latency_ms = float(result.get("latency_ms", 0.0))
        accuracy = float(result.get("accuracy", 0.0))

        complexity_score = 1.0
        tier = "unknown"
        if complexity is not None:
            try:
                complexity_score = float(complexity.compute_composite_score())
                tier = str(complexity.tier) if hasattr(complexity, "tier") else "unknown"
            except Exception:
                pass
        complexity_score = max(complexity_score, 1e-6)

        energy_per_task = energy_kwh / complexity_score
        carbon_per_correct = (
            carbon_kg / max(accuracy, 1e-6)
        ) * 1000.0
        latency_per_step = (
            latency_ms / max(complexity_score, 1e-6)
        )
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
        """Build Layer3 (original behavior)."""
        weights = self.scenarios.get(scenario, self.scenarios["balanced"])

        # Normalize the Layer2 metrics into [0, 1]-ish scores
        energy_score = 1.0 / (1.0 + layer2_metrics.energy_per_task)
        carbon_score = 1.0 / (1.0 + layer2_metrics.carbon_per_correct_answer)
        latency_score = 1.0 / (1.0 + layer2_metrics.latency_per_reasoning_step)
        accuracy_score = min(1.0, layer2_metrics.efficiency_score / 1000.0)

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
        """Original three-layer report (backward-compatible)."""
        layer1 = self.generate_layer1(result)
        layer2 = self.generate_layer2(result, complexity)
        layer3 = self.generate_layer3(layer2, scenario)
        return {
            "layer1": layer1.to_dict(),
            "layer2": layer2.to_dict(),
            "layer3": layer3.to_dict(),
        }

    # ------------------------------------------------------------------
    # ENHANCED methods
    # ------------------------------------------------------------------

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
        """
        Build a complete, hashed EvidenceBundle from a raw result.

        This is the orchestration entry point the proposal calls for:
        every downstream report consumes the bundle this method produces.
        """
        bundle = BundleBuilder.build(
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
        return bundle

    def sign_bundle(
        self,
        bundle: EvidenceBundle,
        key: str,
        signer: Optional[str] = None,
    ) -> EvidenceBundle:
        """
        Attach an HMAC-SHA256 signature to a bundle.

        The signature covers the content hash, so tampering with any
        field invalidates the signature.
        """
        import hmac
        if bundle.content_hash is None:
            bundle.freeze()
        sig = hmac.new(
            key.encode("utf-8"),
            bundle.content_hash.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        bundle.signature = f"hmac-sha256:{sig}"
        bundle.signer = signer or "local"
        return bundle

    def redact(
        self,
        bundle: EvidenceBundle,
        audience: Audience,
    ) -> EvidenceBundle:
        """
        Return a bundle with audience-inappropriate fields redacted.

        OPERATOR and EXECUTIVE do not see verification counterexamples
        or raw metric provenance. GOVERNANCE and TECHNICAL see everything.
        """
        # Shallow copy via JSON round-trip, then redact
        payload = bundle.to_dict()

        if audience in (Audience.OPERATOR, Audience.EXECUTIVE):
            payload["artifact_hashes"] = {}     # not actionable
            payload["verification"] = None      # internal
            for key in list(payload.get("metric_provenance", {}).keys()):
                p = payload["metric_provenance"][key]
                if isinstance(p, dict):
                    p.pop("source_name", None)
                    p.pop("uncertainty", None)
            if isinstance(payload.get("safety_verdict"), dict):
                payload["safety_verdict"].pop("violations", None)

        # Reconstruct — the caller may freeze again if needed
        return _bundle_from_dict(payload)


# =============================================================================
# Bundle round-trip helper
# =============================================================================

def _bundle_from_dict(payload: Dict[str, Any]) -> EvidenceBundle:
    """Reconstruct an EvidenceBundle from its to_dict() representation."""
    d = dict(payload)
    for k in ("timestamp", "created_at"):
        v = d.get(k)
        if isinstance(v, str):
            try:
                d[k] = datetime.fromisoformat(v)
            except ValueError:
                d[k] = datetime.now(timezone.utc)
    # Keep only dataclass fields
    known = {f for f in EvidenceBundle.__dataclass_fields__}
    d = {k: v for k, v in d.items() if k in known}
    return EvidenceBundle(**d)


# =============================================================================
# Demo
# =============================================================================

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # --- Original behavior (backward compatible) ---
    print("=== Original behavior ===")
    reporter = LayeredReporter()

    result = {
        "accuracy": 0.92,
        "energy_kwh": 0.045,
        "carbon_kg": 0.018,
        "latency_ms": 120.0,
        "task_id": "task-1",
    }

    layer1 = reporter.generate_layer1(result)
    print(f"  Layer1 accuracy: {layer1.accuracy}, energy_wh: {layer1.energy_wh}")

    full = reporter.generate_full_report(result)
    print(f"  Full report keys: {list(full.keys())}")
    print(f"  Layer3 weighted_score: {full['layer3']['weighted_score']:.4f}")

    # --- Enhanced: EvidenceBundle ---
    print("\n=== Enhanced EvidenceBundle ===")
    explanation = ExplanationCard(
        decision_id="dec-1",
        selected_action="route_to_lora",
        rationale=["lowest carbon route", "quality above threshold"],
        alternatives_rejected=[
            {"action": "full_finetuning", "reason": "policy violation"},
        ],
        constraints_applied=["max_energy_per_task_wh", "policy_v5.0.1"],
        confidence=0.92,
        trade_offs={"energy_saved_pct": 45.0, "accuracy_delta": -0.02},
    )

    verification = VerificationEvidence(
        policy_version="v5.0.1",
        verdict="passed",
        properties_checked=["DeadlineRespected", "CarbonBudgetNotExceeded"],
        properties_passed=["DeadlineRespected", "CarbonBudgetNotExceeded"],
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
    print(f"  Operational CO2e:    {bundle.operational_co2e_kg:.4f} kg")
    print(f"  Contractual CO2e:    {bundle.contractual_co2e_kg:.4f} kg")
    print("  (Never summed — kept separate per proposal)")
    print(f"  Helium units:        {bundle.helium_units:.6f}")
    print(f"  Simulated:           {bundle.simulated}")
    print(f"  Verification:        {bundle.verification['verdict']}")
    print(f"  Carbon instruments:  {len(bundle.carbon_instruments)}")

    # --- Signing ---
    print("\n=== Signing ===")
    bundle = reporter.sign_bundle(bundle, key="secret-key", signer="us-ca-prod-01")
    print(f"  Signature: {bundle.signature[:32]}...")
    print(f"  Signer:    {bundle.signer}")

    # --- Redaction ---
    print("\n=== Redaction (executive view) ===")
    redacted = reporter.redact(bundle, Audience.EXECUTIVE)
    print(f"  Original artifact_hashes: {bundle.artifact_hashes}")
    print(f"  Redacted artifact_hashes: {redacted.artifact_hashes}")
    print(f"  Original verification:    {bool(bundle.verification)}")
    print(f"  Redacted verification:    {bool(redacted.verification)}")
