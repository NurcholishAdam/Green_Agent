"""
QCS — Quantum Context/Continuity Service for Green Agent
=========================================================

A bounded Quantum Expert service that decides whether a quantum or
quantum-distilled route is justified versus a classical route. It
returns recommendations and evidence — never final policy authority.

Boundaries
----------
QCS owns:
    - Quantum task representation and classical-to-quantum translation
    - Backend discovery, selection, execution lifecycle monitoring
    - Quantum state/result metadata, signatures, reproducibility
    - Quantum-specific uncertainty, queue, shot, noise information
    - A decision interface that returns evidence, not decisions

QCS does NOT own:
    - Carbon ledgers              → governance/
    - Federated training          → federation/
    - Temporal logic verification → governance/verification/
    - Human approvals             → governance/approvals/
    - Global coordination         → orchestration/
    - Charts & dashboards         → visualization/
"""

from __future__ import annotations

# --- Contracts --------------------------------------------------------------
from .contracts.task_spec import QuantumTaskSpec, ProblemFamily
from .contracts.execution_request import QuantumExecutionRequest, BackendPreference
from .contracts.execution_result import (
    QuantumExecutionResult, ExecutionStatus, FallbackReason,
)
from .contracts.backend_profile import (
    BackendProfile, BackendKind, QubitTechnology,
)
from .contracts.quantum_evidence import (
    QuantumEvidence, RouteJustification,
)

# --- Translation ------------------------------------------------------------
from .translation.classical_token_translator import ClassicalTokenTranslator
from .translation.encoding_strategy import EncodingStrategy, AmplitudeEncoding
from .translation.decoding_strategy import DecodingStrategy, SampleDecoding

# --- Execution --------------------------------------------------------------
from .execution.backend_router import BackendRouter
from .execution.quantum_execution_monitor import QuantumExecutionMonitor
from .execution.shot_budget import ShotBudget, ShotBudgetAllocation
from .execution.noise_characterization import NoiseCharacterization
from .execution.classical_fallback import ClassicalFallback, FallbackPlan

# --- State ------------------------------------------------------------------
from .state.state_buffer import StateBuffer
from .state.coherence_manager import CoherenceManager
from .state.signature_preserver import SignaturePreserver
from .state.experiment_registry import ExperimentRegistry, ExperimentRecord

# --- Distillation -----------------------------------------------------------
from .distillation.quantum_distiller import QuantumDistiller
from .distillation.student_model_evaluator import StudentModelEvaluator
from .distillation.fidelity_quality_checker import FidelityQualityChecker

# --- Safety -----------------------------------------------------------------
from .safety.quantum_guardrails import QuantumGuardrails, GuardrailVerdict
from .safety.input_validation import InputValidator, ValidationResult


__all__ = [
    # Contracts
    "QuantumTaskSpec", "ProblemFamily",
    "QuantumExecutionRequest", "BackendPreference",
    "QuantumExecutionResult", "ExecutionStatus", "FallbackReason",
    "BackendProfile", "BackendKind", "QubitTechnology",
    "QuantumEvidence", "RouteJustification",
    # Translation
    "ClassicalTokenTranslator",
    "EncodingStrategy", "AmplitudeEncoding",
    "DecodingStrategy", "SampleDecoding",
    # Execution
    "BackendRouter",
    "QuantumExecutionMonitor",
    "ShotBudget", "ShotBudgetAllocation",
    "NoiseCharacterization",
    "ClassicalFallback", "FallbackPlan",
    # State
    "StateBuffer",
    "CoherenceManager",
    "SignaturePreserver",
    "ExperimentRegistry", "ExperimentRecord",
    # Distillation
    "QuantumDistiller",
    "StudentModelEvaluator",
    "FidelityQualityChecker",
    # Safety
    "QuantumGuardrails", "GuardrailVerdict",
    "InputValidator", "ValidationResult",
]

__version__ = "0.1.0"
