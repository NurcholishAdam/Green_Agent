"""
Governance package for Green Agent
====================================

Non-negotiable guardrail and accountability layer. Defines what agents
are allowed to do, approves or blocks high-impact actions, records
accountable evidence, and enforces safe fallback behavior.

Public API
----------
Contracts:
    DecisionRequest, PolicyDecision, GovernanceEvent, RiskLevel

Decision point:
    PolicyDecisionPoint        — the single gate every action passes through
    PolicyEnforcementPoint     — the executors for the decision

Policy:
    SustainabilityPolicy, SafetyPolicy, PrecisionPolicy,
    QuantumPolicy, FederationPolicy, HumanOversightPolicy

Verification:
    TemporalLogic, PolicyVerifier, VerificationEvidence,
    CounterexampleHandler

Accountability:
    CarbonLedger, CarbonInstrumentsLedger,
    ProvenanceRegistry, AuditLog, RetentionPolicy

Approvals:
    EscalationRouter, HumanReviewGate,
    EmergencyStop, RollbackController

Benchmarks:
    BenchmarkEngine, ReproducibilityRules, MetricGovernance

Hardware:
    HeliumPolicyAdapter, HardwareCapabilityPolicy
"""

from __future__ import annotations

# --- Contracts --------------------------------------------------------------
from .contracts.decision_request import DecisionRequest
from .contracts.policy_decision import PolicyDecision, DecisionVerdict
from .contracts.governance_event import GovernanceEvent, GovernanceEventKind
from .contracts.risk_classification import RiskLevel, RiskClassification

# --- Enforcement ------------------------------------------------------------
from .enforcement.policy_decision_point import PolicyDecisionPoint
from .enforcement.policy_enforcement_point import PolicyEnforcementPoint
from .enforcement.fallback_policy import FallbackPolicy, FallbackAction
from .enforcement.permission_guard import PermissionGuard

# --- Policy -----------------------------------------------------------------
from .policy.sustainability_policy import SustainabilityPolicy
from .policy.safety_policy import SafetyPolicy
from .policy.precision_policy import PrecisionPolicy
from .policy.quantum_policy import QuantumPolicy
from .policy.federation_policy import FederationPolicy
from .policy.human_oversight_policy import HumanOversightPolicy

# --- Verification -----------------------------------------------------------
from .verification.temporal_logic import TemporalLogic
from .verification.policy_verifier import PolicyVerifier
from .verification.verification_evidence import VerificationEvidence
from .verification.counterexample_handler import CounterexampleHandler

# --- Accountability ---------------------------------------------------------
from .accountability.carbon_ledger import CarbonLedger
from .accountability.carbon_instruments_ledger import CarbonInstrumentsLedger
from .accountability.provenance_registry import ProvenanceRegistry
from .accountability.audit_log import AuditLog
from .accountability.retention_policy import RetentionPolicy

# --- Approvals --------------------------------------------------------------
from .approvals.escalation_router import EscalationRouter
from .approvals.human_review import HumanReviewGate, ReviewRequest
from .approvals.emergency_stop import EmergencyStop
from .approvals.rollback_controller import RollbackController

# --- Benchmarks -------------------------------------------------------------
from .benchmarks.benchmark_engine import BenchmarkEngine
from .benchmarks.reproducibility_rules import ReproducibilityRules
from .benchmarks.metric_governance import MetricGovernance

# --- Hardware ---------------------------------------------------------------
from .hardware.helium_policy_adapter import HeliumPolicyAdapter
from .hardware.hardware_capability_policy import HardwareCapabilityPolicy

__all__ = [
    # Contracts
    "DecisionRequest", "PolicyDecision", "DecisionVerdict",
    "GovernanceEvent", "GovernanceEventKind",
    "RiskLevel", "RiskClassification",
    # Enforcement
    "PolicyDecisionPoint", "PolicyEnforcementPoint",
    "FallbackPolicy", "FallbackAction", "PermissionGuard",
    # Policy
    "SustainabilityPolicy", "SafetyPolicy", "PrecisionPolicy",
    "QuantumPolicy", "FederationPolicy", "HumanOversightPolicy",
    # Verification
    "TemporalLogic", "PolicyVerifier", "VerificationEvidence",
    "CounterexampleHandler",
    # Accountability
    "CarbonLedger", "CarbonInstrumentsLedger",
    "ProvenanceRegistry", "AuditLog", "RetentionPolicy",
    # Approvals
    "EscalationRouter", "HumanReviewGate", "ReviewRequest",
    "EmergencyStop", "RollbackController",
    # Benchmarks
    "BenchmarkEngine", "ReproducibilityRules", "MetricGovernance",
    # Hardware
    "HeliumPolicyAdapter", "HardwareCapabilityPolicy",
]

__version__ = "5.0.0"
