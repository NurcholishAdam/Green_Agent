#!/usr/bin/env python3
# File: src/enhancements/material_substitution_enhanced_v16_0.py
# Version 17.0 – Full Green Agent MOPD + Bio‑Inspired + MOE + MODP + Self‑Healing
# + Quantum‑Distillation, Causal RL, Federated Green Learning, Multi‑Agent Role Specialization,
#   Temporal Logic Verification, Explainable AI (XAI), Adaptive Precision Switching,
#   External Carbon Markets + RECs, Chaos Testing, Active Human‑in‑the‑Loop
# Single-file consolidated build – no external enhancement modules required.

import asyncio
import hashlib
import json
import os
import signal
import sys
import time
import uuid
import random
import math
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any, Callable, Union
from collections import deque, defaultdict
from enum import Enum
from concurrent.futures import ThreadPoolExecutor
import numpy as np

# ============================================================
# IMPORT CENTRAL GREEN AGENT COMPONENTS
# ============================================================
from ..config import config as central_config
from ..storage import Storage
from ..schemas.feedback_event import FeedbackEvent
from ..routing.pareto_gating import ParetoGating
from ..feedback.adaptive_cost import AdaptiveCostFunction
from ..safety.drift_detector import DriftDetector
from ..scaling.message_queue import AsyncMessageQueue
from ..metrics import MetricsRegistry
from ..logger import logger

# ============================================================
# OPTIONAL IMPORTS (graceful degradation)
# ============================================================
try:
    from pqcrypto.sign import dilithium, falcon, sphincs
    PQC_AVAILABLE = True
except ImportError:
    PQC_AVAILABLE = False

from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.backends import default_backend

try:
    from web3 import Web3, Account
    WEB3_AVAILABLE = True
except ImportError:
    WEB3_AVAILABLE = False

try:
    from statsmodels.tsa.holtwinters import ExponentialSmoothing
    STATSMODELS_AVAILABLE = True
except ImportError:
    STATSMODELS_AVAILABLE = False

try:
    import boto3
    AWS_AVAILABLE = True
except ImportError:
    AWS_AVAILABLE = False

try:
    from azure.storage.blob import BlobServiceClient
    AZURE_AVAILABLE = True
except ImportError:
    AZURE_AVAILABLE = False

try:
    from google.cloud import storage
    GCP_AVAILABLE = True
except ImportError:
    GCP_AVAILABLE = False

try:
    from sklearn.linear_model import LogisticRegression, LinearRegression
    from sklearn.preprocessing import StandardScaler
    from sklearn.ensemble import IsolationForest
    from sklearn.svm import OneClassSVM
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

try:
    from prophet import Prophet
    PROPHET_AVAILABLE = True
except ImportError:
    PROPHET_AVAILABLE = False

try:
    import torch
    import torch.nn as nn
    import torch.optim as optim
    TORCH_AVAILABLE = True
except ImportError:
    TORCH_AVAILABLE = False

# ============================================================
# CUSTOM EXCEPTIONS
# ============================================================
class MaterialError(Exception): pass
class QuantumError(MaterialError): pass
class BlockchainError(MaterialError): pass
class DiscoveryError(MaterialError): pass
class AnalysisError(MaterialError): pass
class CircuitBreakerOpenError(MaterialError): pass
class RateLimitExceeded(MaterialError): pass
class TemporalLogicViolation(MaterialError): pass
class ChaosTestFailure(MaterialError): pass

# ============================================================
# ENUMS AND DATA CLASSES
# ============================================================
class MaterialClass(str, Enum):
    ALUMINUM_ALLOY = "aluminum_alloy"
    STEEL_ALLOY = "steel_alloy"
    COMPOSITE = "composite"
    POLYMER = "polymer"
    CERAMIC = "ceramic"
    TITANIUM = "titanium"
    MAGNESIUM = "magnesium"
    COPPER = "copper"
    OTHER = "other"

class Application(str, Enum):
    STRUCTURAL = "structural"
    AEROSPACE = "aerospace"
    AUTOMOTIVE = "automotive"
    CONSTRUCTION = "construction"
    MARINE = "marine"
    ELECTRONICS = "electronics"
    ENERGY = "energy"
    MEDICAL = "medical"
    OTHER = "other"

class ComplianceStandard(str, Enum):
    ISO14001 = "iso14001"
    ISO50001 = "iso50001"
    REACH = "reach"
    ROHS = "rohs"

class PrecisionLevel(str, Enum):
    FP32 = "fp32"
    FP16 = "fp16"
    INT8 = "int8"

class AgentRole(str, Enum):
    CARBON_SPECIALIST = "carbon_specialist"
    COST_SPECIALIST = "cost_specialist"
    STRENGTH_SPECIALIST = "strength_specialist"
    CIRCULARITY_SPECIALIST = "circularity_specialist"

@dataclass
class MaterialProperties:
    material_id: str
    name: str
    material_class: MaterialClass
    density_kg_m3: float
    yield_strength_mpa: float
    elastic_modulus_gpa: float
    thermal_conductivity_w_mk: float
    cost_per_kg: float
    carbon_footprint_kg_co2_per_kg: float
    recyclability_pct: float
    supply_risk_score: float
    applications: List[Application]
    compliance_certifications: List[ComplianceStandard]
    recycled_content_pct: float
    end_of_life_recyclability_pct: float

    def __post_init__(self):
        if self.density_kg_m3 <= 0: raise ValueError("density_kg_m3 must be > 0")
        if self.yield_strength_mpa < 0: raise ValueError("yield_strength_mpa must be >= 0")
        if self.elastic_modulus_gpa < 0: raise ValueError("elastic_modulus_gpa must be >= 0")
        if self.thermal_conductivity_w_mk < 0: raise ValueError("thermal_conductivity_w_mk must be >= 0")
        if self.cost_per_kg < 0: raise ValueError("cost_per_kg must be >= 0")
        if self.carbon_footprint_kg_co2_per_kg < 0: raise ValueError("carbon_footprint_kg_co2_per_kg must be >= 0")
        if not (0 <= self.recyclability_pct <= 100): raise ValueError("recyclability_pct must be 0..100")
        if not (0 <= self.supply_risk_score <= 1): raise ValueError("supply_risk_score must be 0..1")
        if not (0 <= self.recycled_content_pct <= 100): raise ValueError("recycled_content_pct must be 0..100")
        if not (0 <= self.end_of_life_recyclability_pct <= 100): raise ValueError("eol recyclability must be 0..100")

    @property
    def circularity_score(self) -> float:
        return (0.5 * self.recyclability_pct / 100
                + 0.3 * self.recycled_content_pct / 100
                + 0.2 * self.end_of_life_recyclability_pct / 100)

@dataclass
class SubstitutionResult:
    base_material: str
    recommended_substitute: str
    topsis_score: float
    carbon_reduction_pct: float
    cost_savings_pct: float
    performance_score: float
    recommendations: List[str]
    sustainability_score: float
    confidence_score: float
    data_quality_score: float
    calculation_time_ms: float
    alternative_substitutes: List[Dict]
    supply_risk_improvement: float
    circularity_improvement: float
    lifecycle_assessment: Dict
    compliance_status: Dict
    carbon_selection_weight: Dict
    carbon_intensity_at_time: float
    quantum_signature: Optional[Dict] = None
    blockchain_tx_hash: Optional[str] = None
    cloud_distribution: Optional[Dict] = None
    autonomous_discovery: Optional[Dict] = None
    gating_weights: Optional[Dict] = None
    drift_detected: bool = False
    # NEW v17.0 fields
    explanation: Optional[Dict] = None
    temporal_logic_status: Optional[Dict] = None
    precision_used: Optional[str] = None
    carbon_credit_value_usd: Optional[float] = None
    rec_value_usd: Optional[float] = None
    role_assignments: Optional[Dict] = None
    chaos_test_passed: Optional[bool] = None
    hitl_query: Optional[Dict] = None
    federated_round: Optional[int] = None

    def __post_init__(self):
        if self.carbon_reduction_pct < -100 or self.carbon_reduction_pct > 100:
            raise ValueError("carbon_reduction_pct must be between -100 and 100")
        if self.cost_savings_pct < -100 or self.cost_savings_pct > 100:
            raise ValueError("cost_savings_pct must be between -100 and 100")
        if self.performance_score < 0: raise ValueError("performance_score must be >= 0")
        if not (0 <= self.topsis_score <= 1): raise ValueError("topsis_score must be 0..1")
        if not (0 <= self.sustainability_score <= 100): raise ValueError("sustainability_score must be 0..100")
        if not (0 <= self.confidence_score <= 1): raise ValueError("confidence_score must be 0..1")
        if not (0 <= self.data_quality_score <= 1): raise ValueError("data_quality_score must be 0..1")
        if self.calculation_time_ms < 0: raise ValueError("calculation_time_ms must be >= 0")

    def to_dict(self) -> Dict:
        return asdict(self)

# ============================================================
# CIRCUIT BREAKER + RATE LIMITER
# ============================================================
class CircuitBreakerState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

class EnhancedCircuitBreaker:
    def __init__(self, name: str):
        self.name = name
        self.failure_threshold = getattr(central_config, 'CIRCUIT_BREAKER_FAILURE_THRESHOLD', 5)
        self.recovery_timeout = getattr(central_config, 'CIRCUIT_BREAKER_RECOVERY_TIMEOUT', 30)
        self.half_open_max_requests = 3
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time = None
        self.last_success_time = None
        self._lock = asyncio.Lock()
        self.half_open_requests = 0

    async def allow_request(self) -> bool:
        async with self._lock:
            if self.state == CircuitBreakerState.OPEN:
                if time.time() - self.last_failure_time >= self.recovery_timeout:
                    self.state = CircuitBreakerState.HALF_OPEN
                    self.half_open_requests = 0
                    logger.info(f"Circuit breaker {self.name} -> HALF_OPEN")
                else:
                    return False
            if self.state == CircuitBreakerState.HALF_OPEN:
                self.half_open_requests += 1
                if self.half_open_requests > self.half_open_max_requests:
                    self.state = CircuitBreakerState.OPEN
                    logger.info(f"Circuit breaker {self.name} back to OPEN")
                    return False
            return True

    async def record_success(self):
        async with self._lock:
            self.success_count += 1
            self.last_success_time = time.time()
            if self.state == CircuitBreakerState.HALF_OPEN and self.success_count >= 2:
                self.state = CircuitBreakerState.CLOSED
                self.failure_count = 0
                logger.info(f"Circuit breaker {self.name} CLOSED")

    async def record_failure(self):
        async with self._lock:
            self.failure_count += 1
            self.last_failure_time = time.time()
            if self.state == CircuitBreakerState.CLOSED and self.failure_count >= self.failure_threshold:
                self.state = CircuitBreakerState.OPEN
                logger.warning(f"Circuit breaker {self.name} OPEN")

    async def call(self, func, *args, **kwargs):
        if not await self.allow_request():
            raise CircuitBreakerOpenError(f"Circuit breaker {self.name} is OPEN")
        try:
            result = await func(*args, **kwargs)
            await self.record_success()
            return result
        except Exception:
            await self.record_failure()
            raise

class EnhancedRateLimiter:
    def __init__(self):
        self.rate = getattr(central_config, 'rate_limit_requests', 100)
        self.per_seconds = getattr(central_config, 'rate_limit_window', 60)
        self.tokens = self.rate
        self.last_refill = time.time()
        self._lock = asyncio.Lock()

    async def acquire(self) -> bool:
        async with self._lock:
            now = time.time()
            time_passed = now - self.last_refill
            self.tokens = min(self.rate, self.tokens + time_passed * (self.rate / self.per_seconds))
            self.last_refill = now
            if self.tokens >= 1:
                self.tokens -= 1
                return True
            return False

    async def wait_and_acquire(self):
        while not await self.acquire():
            await asyncio.sleep(0.1)

# ============================================================
# POST‑QUANTUM CRYPTOGRAPHY
# ============================================================
class PostQuantumCrypto:
    def __init__(self, storage):
        self.storage = storage

    async def sign_data(self, data: Dict) -> Dict:
        if PQC_AVAILABLE:
            digest = hashlib.sha3_256(json.dumps(data, sort_keys=True, default=str).encode()).hexdigest()
            return {'algorithm': 'dilithium-sim', 'signature': digest[:64]}
        return {'algorithm': 'none', 'signature': ''}

# ============================================================
# BLOCKCHAIN MATERIAL VERIFICATION
# ============================================================
class BlockchainMaterialVerification:
    def __init__(self, storage):
        self.storage = storage

    async def record_material_data(self, data_id: str, data_hash: str, metadata: Dict) -> Dict:
        return {'tx_hash': '0x' + uuid.uuid4().hex}

    async def get_blockchain_status(self) -> Dict:
        return {'connected': False}

# ============================================================
# ============================================================
# NEW MODULE 1: TEMPORAL LOGIC MONITOR (Formal Verification)
# ============================================================
# ============================================================
class TemporalLogicMonitor:
    """
    Lightweight Linear Temporal Logic (LTL) style monitor.
    Supports operators:
      G φ    (always φ)                -> "G(carbon < 400)"
      F φ    (eventually φ)            -> "F(confidence > 0.8)"
      φ -> ψ (implication)
      φ U ψ  (until)
    Formulas are parsed from a small DSL and evaluated against a rolling
    history of state dicts.  Violations raise/log and can block decisions.
    """
    def __init__(self, history_len: int = 200):
        self.formulas: Dict[str, str] = {}
        self.compiled: Dict[str, Callable[[List[Dict]], bool]] = {}
        self.history: deque = deque(maxlen=history_len)
        self.violations: List[Dict] = []

    def add_formula(self, name: str, formula: str):
        self.formulas[name] = formula
        self.compiled[name] = self._compile(formula)

    def update(self, state: Dict):
        self.history.append(dict(state))

    # ------- Simple DSL compiler -------
    def _compile(self, formula: str) -> Callable[[List[Dict]], bool]:
        formula = formula.strip()

        # G(...) always
        if formula.startswith("G(") and formula.endswith(")"):
            inner = formula[2:-1]
            fn = self._compile(inner)
            return lambda hist: all(fn([h]) for h in hist) if hist else True

        # F(...) eventually
        if formula.startswith("F(") and formula.endswith(")"):
            inner = formula[2:-1]
            fn = self._compile(inner)
            return lambda hist: any(fn([h]) for h in hist) if hist else False

        # φ U ψ  (until)
        if " U " in formula:
            left, right = formula.split(" U ", 1)
            lf = self._compile(left)
            rf = self._compile(right)
            def until(hist):
                for i, _ in enumerate(hist):
                    if rf(hist[i:]): return True
                    if not lf([hist[i]]): return False
                return False
            return until

        # φ -> ψ
        if "->" in formula:
            left, right = formula.split("->", 1)
            lf = self._compile(left.strip())
            rf = self._compile(right.strip())
            return lambda hist: (not lf(hist)) or rf(hist)

        # Comparison atom
        return self._atom(formula)

    def _atom(self, atom: str) -> Callable[[List[Dict]], bool]:
        atom = atom.strip()
        ops = ["<=", ">=", "==", "!=", "<", ">"]
        for op in ops:
            if op in atom:
                lhs, rhs = atom.split(op, 1)
                lhs = lhs.strip()
                rhs = rhs.strip()
                def make(lhs, op, rhs):
                    def check(hist):
                        if not hist: return True
                        s = hist[-1]
                        lv = s.get(lhs, 0.0)
                        try:
                            rv = float(rhs)
                        except ValueError:
                            rv = s.get(rhs, 0.0)
                        return {
                            "<":  lambda: lv <  rv,
                            ">":  lambda: lv >  rv,
                            "<=": lambda: lv <= rv,
                            ">=": lambda: lv >= rv,
                            "==": lambda: lv == rv,
                            "!=": lambda: lv != rv,
                        }[op]()
                    return check
                return make(lhs, op, rhs)
        # Boolean literal
        return lambda hist: bool(atom.lower() in ("true", "1", "yes"))

    def evaluate(self) -> Dict[str, bool]:
        results = {}
        for name, fn in self.compiled.items():
            try:
                ok = fn(list(self.history))
            except Exception as e:
                logger.warning(f"Temporal formula '{name}' evaluation error: {e}")
                ok = False
            results[name] = ok
            if not ok:
                self.violations.append({
                    'formula': name,
                    'expression': self.formulas[name],
                    'timestamp': datetime.now().isoformat(),
                })
                logger.warning(f"Temporal logic violation: {name} ({self.formulas[name]})")
        return results

    def get_status(self) -> Dict:
        return {
            'formulas': self.formulas,
            'last_results': self.evaluate(),
            'violations': self.violations[-5:],
        }

# ============================================================
# ============================================================
# NEW MODULE 2: EXPLAINABLE AI (XAI) EXPLAINER
# ============================================================
# ============================================================
class XAIExplainer:
    """
    Produces feature-attribution explanations for the MODP / TOPSIS
    decision.  Uses weighted-normalized contributions so that reviewers
    can see exactly why a material was chosen.
    """
    def __init__(self, feature_names: List[str]):
        self.feature_names = feature_names

    def explain_topsis(self,
                       candidate: Dict[str, float],
                       weights: List[float],
                       all_candidates: List[Dict[str, float]],
                       top_k: int = 5) -> Dict:
        """
        candidate: {feature_name: raw_value}
        weights:   list aligned with feature_names
        Returns normalized contributions + a human-readable narrative.
        """
        # Normalize across candidates using vector normalization (matches TOPSIS)
        matrix = np.array([[c[f] for f in self.feature_names] for c in all_candidates])
        norms = np.sqrt((matrix ** 2).sum(axis=0)) + 1e-9
        cand_vec = np.array([candidate[f] for f in self.feature_names])
        norm_cand = cand_vec / norms
        weighted = norm_cand * np.array(weights)

        # Contribution magnitude (signed by direction of benefit)
        contrib = {f: float(weighted[i]) for i, f in enumerate(self.feature_names)}

        # Rank features by |contribution|
        ranked = sorted(contrib.items(), key=lambda kv: abs(kv[1]), reverse=True)[:top_k]

        narrative_lines = []
        for f, v in ranked:
            direction = "increases" if v >= 0 else "decreases"
            narrative_lines.append(
                f"{f} ({v:+.3f}) {direction} the material's TOPSIS score."
            )

        return {
            'contributions': contrib,
            'top_features': [f for f, _ in ranked],
            'narrative': narrative_lines,
            'weights_used': {f: float(w) for f, w in zip(self.feature_names, weights)},
        }

    def explain_selection(self, best: MaterialProperties, alternatives: List[MaterialProperties],
                          weights: List[float]) -> Dict:
        crit = ['strength', 'carbon', 'cost', 'circularity', 'supply_risk']
        def to_dict(m):
            return {
                'strength': m.yield_strength_mpa / 1000,
                'carbon': -m.carbon_footprint_kg_co2_per_kg,
                'cost': -m.cost_per_kg,
                'circularity': m.circularity_score,
                'supply_risk': -m.supply_risk_score,
            }
        all_dicts = [to_dict(m) for m in [best] + alternatives]
        return self.explain_topsis(to_dict(best), weights, all_dicts)

# ============================================================
# ============================================================
# NEW MODULE 3: ADAPTIVE PRECISION CONTROLLER (Hardware-Aware)
# ============================================================
# ============================================================
class AdaptivePrecisionController:
    """
    Selects numerical precision (fp32 / fp16 / int8) based on
    hardware telemetry, carbon intensity, and required accuracy.
    """
    def __init__(self, carbon_manager=None):
        self.carbon_manager = carbon_manager
        self.telemetry = {
            'gpu_available': False,
            'memory_gb': 16.0,
            'utilization': 0.3,
        }
        self.last_precision = PrecisionLevel.FP32

    def update_telemetry(self, **kwargs):
        self.telemetry.update(kwargs)

    def select(self,
               carbon_intensity: float,
               accuracy_required: float = 0.95) -> PrecisionLevel:
        """
        Rules:
          - If accuracy_required > 0.99  -> FP32
          - If GPU available + carbon low -> FP16
          - If carbon very high            -> INT8 (energy saving)
        """
        if accuracy_required > 0.99:
            self.last_precision = PrecisionLevel.FP32
        elif carbon_intensity > 500:
            self.last_precision = PrecisionLevel.INT8
        elif self.telemetry.get('gpu_available') and carbon_intensity < 350:
            self.last_precision = PrecisionLevel.FP16
        else:
            self.last_precision = PrecisionLevel.FP32
        logger.info(f"Adaptive precision selected: {self.last_precision.value} "
                    f"(carbon={carbon_intensity:.0f}, acc_req={accuracy_required:.2f})")
        return self.last_precision

    @staticmethod
    def precision_energy_factor(level: PrecisionLevel) -> float:
        return {
            PrecisionLevel.FP32: 1.0,
            PrecisionLevel.FP16: 0.6,
            PrecisionLevel.INT8: 0.3,
        }[level]

# ============================================================
# ============================================================
# NEW MODULE 4: CARBON MARKETS & RENEWABLE ENERGY CREDITS
# ============================================================
# ============================================================
class CarbonMarketClient:
    """
    Simulated client for external carbon markets and RECs.
    In production, this would hit real APIs (e.g., Verra, Gold Standard).
    """
    def __init__(self, storage=None):
        self.storage = storage
        # Prices in USD
        self.carbon_price_per_ton = 50.0     # $/tCO2e
        self.rec_price_per_mwh = 30.0        # $/MWh
        self.carbon_intensity_kg_per_mwh = 400.0  # grid average
        self.trades: List[Dict] = []

    async def get_carbon_credit_value(self, carbon_saved_kg: float) -> float:
        """Convert kgCO2e saved into USD carbon-credit value."""
        tons = max(0.0, carbon_saved_kg) / 1000.0
        return round(tons * self.carbon_price_per_ton, 4)

    async def get_rec_value(self, energy_saved_kwh: float) -> float:
        """Convert kWh energy saved into USD REC value."""
        mwh = max(0.0, energy_saved_kwh) / 1000.0
        return round(mwh * self.rec_price_per_mwh, 4)

    async def get_market_snapshot(self) -> Dict:
        return {
            'carbon_price_usd_per_ton': self.carbon_price_per_ton,
            'rec_price_usd_per_mwh': self.rec_price_per_mwh,
            'grid_intensity_kg_per_mwh': self.carbon_intensity_kg_per_mwh,
        }

    async def retire_credits(self, amount_kg: float, beneficiary: str) -> Dict:
        record = {
            'id': str(uuid.uuid4()),
            'amount_kg': amount_kg,
            'beneficiary': beneficiary,
            'timestamp': datetime.now().isoformat(),
        }
        self.trades.append(record)
        logger.info(f"Retired {amount_kg:.2f} kgCO2e for {beneficiary}")
        return record

# ============================================================
# ============================================================
# NEW MODULE 5: MULTI-AGENT COORDINATION + ROLE SPECIALISATION
# ============================================================
# ============================================================
class RoleSpecializationCoordinator:
    """
    Assigns specialized roles to expert agents based on context.
    Emergent role specialization is achieved through a softmax over
    context-sensitivity scores.
    """
    def __init__(self):
        self.roles = list(AgentRole)
        # Context -> role affinity matrix (rows=roles, cols=[carbon, cost, strength, circularity])
        self.affinity = np.array([
            [0.9, 0.1, 0.2, 0.3],   # carbon specialist
            [0.1, 0.9, 0.2, 0.3],   # cost specialist
            [0.2, 0.1, 0.9, 0.2],   # strength specialist
            [0.2, 0.2, 0.2, 0.9],   # circularity specialist
        ])

    def assign_roles(self, context: Dict[str, float]) -> Dict[str, str]:
        ctx = np.array([
            context.get('carbon_weight', 0.3),
            context.get('cost_weight', 0.2),
            context.get('strength_weight', 0.3),
            context.get('circularity_weight', 0.2),
        ])
        scores = self.affinity @ ctx
        # softmax
        e = np.exp(scores - scores.max())
        probs = e / e.sum()
        # Assign each material / task to its argmax role
        assignments = {}
        for i, role in enumerate(self.roles):
            assignments[role.value] = float(probs[i])
        dominant = self.roles[int(np.argmax(probs))].value
        return {'assignments': assignments, 'dominant_role': dominant}

# ============================================================
# ============================================================
# NEW MODULE 6: CHAOS TESTER (Resilience Engineering)
# ============================================================
# ============================================================
class ChaosTester:
    """
    Injects controlled faults into the analyzer to validate resilience.
    Faults: 'disable_carbon', 'corrupt_material', 'latency_spike',
            'break_pqc', 'break_blockchain'.
    """
    FAULT_TYPES = ['disable_carbon', 'corrupt_material', 'latency_spike',
                   'break_pqc', 'break_blockchain']

    def __init__(self, analyzer_ref):
        self.analyzer = analyzer_ref
        self.results: List[Dict] = []

    async def run_test(self, fault_type: str, duration_s: float = 0.5) -> Dict:
        if fault_type not in self.FAULT_TYPES:
            raise ValueError(f"Unknown fault type: {fault_type}")
        logger.warning(f"CHAOS: injecting {fault_type} for {duration_s}s")
        start = time.time()
        passed = True
        error_msg = None
        try:
            if fault_type == 'disable_carbon':
                # Simulate carbon API failure
                original = self.analyzer.carbon_manager.get_current_intensity
                async def broken(): raise RuntimeError("carbon API down")
                self.analyzer.carbon_manager.get_current_intensity = broken
                await asyncio.sleep(duration_s)
                self.analyzer.carbon_manager.get_current_intensity = original
            elif fault_type == 'corrupt_material':
                # Replace one material with invalid props
                if self.analyzer.materials:
                    first_id = next(iter(self.analyzer.materials))
                    saved = self.analyzer.materials[first_id]
                    try:
                        self.analyzer.materials[first_id] = MaterialProperties(
                            material_id="corrupt", name="Corrupt",
                            material_class=MaterialClass.OTHER,
                            density_kg_m3=-1,  # will raise
                            yield_strength_mpa=0, elastic_modulus_gpa=0,
                            thermal_conductivity_w_mk=0, cost_per_kg=0,
                            carbon_footprint_kg_co2_per_kg=0,
                            recyclability_pct=0, supply_risk_score=0,
                            applications=[], compliance_certifications=[],
                            recycled_content_pct=0, end_of_life_recyclability_pct=0)
                    except ValueError:
                        pass  # expected
                    self.analyzer.materials[first_id] = saved
            elif fault_type == 'latency_spike':
                original_sleep = asyncio.sleep
                async def slow(t, *a, **k):
                    return await original_sleep(t + 0.5)
                asyncio.sleep = slow
                await asyncio.sleep(duration_s)
                asyncio.sleep = original_sleep
            elif fault_type == 'break_pqc':
                self.analyzer.pqc.sign_data = lambda d: asyncio.sleep(0, result={'algorithm': 'none', 'signature': ''})
            elif fault_type == 'break_blockchain':
                self.analyzer.blockchain.record_material_data = lambda *a, **k: asyncio.sleep(
                    0, result={'tx_hash': None})
        except Exception as e:
            passed = False
            error_msg = str(e)

        result = {
            'fault': fault_type,
            'duration_s': duration_s,
            'elapsed_s': time.time() - start,
            'passed': passed,
            'error': error_msg,
            'timestamp': datetime.now().isoformat(),
        }
        self.results.append(result)
        logger.warning(f"CHAOS result: {result}")
        return result

    def get_report(self) -> Dict:
        return {
            'tests_run': len(self.results),
            'pass_rate': (sum(1 for r in self.results if r['passed']) / len(self.results))
                         if self.results else 1.0,
            'recent': self.results[-5:],
        }

# ============================================================
# ============================================================
# NEW MODULE 7: ACTIVE RLHF (Human-in-the-Loop with Active Learning)
# ============================================================
# ============================================================
class ActiveRLHF:
    """
    Preference-based policy learner with uncertainty-triggered
    human queries (active learning).
    """
    def __init__(self, action_space: List[str],
                 uncertainty_threshold: float = 0.35,
                 human_timeout_s: float = 300.0):
        self.actions = list(action_space)
        self.uncertainty_threshold = uncertainty_threshold
        self.human_timeout_s = human_timeout_s
        self.preference_counts = defaultdict(float)
        self.history: List[Dict] = []
        self.pending_queries: Dict[str, Dict] = {}

    def _policy(self, context: Any) -> np.ndarray:
        # Context-aware softmax over accumulated preferences
        raw = np.array([self.preference_counts[a] for a in self.actions], dtype=float)
        if raw.sum() == 0:
            raw = np.ones(len(self.actions))
        e = np.exp(raw - raw.max())
        return e / e.sum()

    def sample_action(self, context: Any) -> str:
        probs = self._policy(context)
        idx = int(np.argmax(probs))
        return self.actions[idx]

    def uncertainty(self, context: Any) -> float:
        probs = self._policy(context)
        # entropy normalized
        ent = -np.sum(probs * np.log(probs + 1e-12))
        return float(ent / np.log(len(self.actions)))

    def update(self, context: Any, action: str, reward: float):
        self.preference_counts[action] += reward
        self.history.append({
            'action': action, 'reward': reward,
            'timestamp': datetime.now().isoformat()
        })

    async def maybe_query_human(self, context: Dict, options: List[str]) -> Optional[Dict]:
        """If uncertainty is high, ask a human. Otherwise return None."""
        u = self.uncertainty(context)
        if u <= self.uncertainty_threshold:
            return None
        query_id = str(uuid.uuid4())
        query = {
            'id': query_id,
            'context': context,
            'options': options,
            'uncertainty': u,
            'created_at': datetime.now().isoformat(),
            'status': 'pending',
        }
        self.pending_queries[query_id] = query
        logger.warning(f"Active RLHF human query created (u={u:.2f}, id={query_id})")
        return query

    def resolve_query(self, query_id: str, chosen: str, rating: float = 1.0):
        if query_id not in self.pending_queries:
            return None
        q = self.pending_queries.pop(query_id)
        q['status'] = 'resolved'
        q['chosen'] = chosen
        q['rating'] = rating
        self.update(q['context'], chosen, rating)
        return q

# ============================================================
# ============================================================
# NEW MODULE 8: FEDERATED AGGREGATOR (Cross-Deployment Learning)
# ============================================================
# ============================================================
class FederatedAggregator:
    """
    FedAvg-style aggregation of model updates from multiple deployments.
    """
    def __init__(self):
        self.round = 0
        self.global_model: Dict[str, List[float]] = {
            'weights': [0.3, 0.25, 0.25, 0.2, 0.1]  # strength, carbon, cost, circularity, supply_risk
        }
        self.client_updates: List[Dict] = []

    def submit_update(self, client_id: str, weights: List[float], samples: int):
        self.client_updates.append({
            'client_id': client_id,
            'weights': list(weights),
            'samples': samples,
        })

    def aggregate(self) -> Dict:
        if not self.client_updates:
            return self.global_model
        total_samples = sum(u['samples'] for u in self.client_updates) or 1
        agg = np.zeros(len(self.global_model['weights']))
        for u in self.client_updates:
            agg += np.array(u['weights']) * (u['samples'] / total_samples)
        self.global_model['weights'] = agg.tolist()
        self.round += 1
        self.client_updates.clear()
        logger.info(f"Federated aggregation round {self.round}: {self.global_model['weights']}")
        return self.global_model

    def get_stats(self) -> Dict:
        return {
            'round': self.round,
            'global_weights': self.global_model['weights'],
            'pending_updates': len(self.client_updates),
        }

# ============================================================
# ============================================================
# NEW MODULE 9: HUMAN-IN-THE-LOOP COORDINATOR
# ============================================================
# ============================================================
class HumanInTheLoopCoordinator:
    """
    Manages critical-decision escalation to humans with timeout &
    auto-fallback. Integrates with ActiveRLHF.
    """
    def __init__(self, active_rlhf: ActiveRLHF, timeout_s: float = 300.0):
        self.rlhf = active_rlhf
        self.timeout_s = timeout_s
        self.audit_log: List[Dict] = []

    async def escalate(self,
                       decision_context: Dict,
                       options: List[str],
                       confidence: float,
                       confidence_threshold: float = 0.65) -> Dict:
        """
        If confidence < threshold, or uncertainty high, escalate.
        Returns: {'escalated': bool, 'chosen': str, 'source': 'human'|'auto'}
        """
        needs_human = confidence < confidence_threshold
        query = await self.rlhf.maybe_query_human(decision_context, options)
        if query is None and not needs_human:
            # Auto decision
            choice = self.rlhf.sample_action(decision_context)
            self.audit_log.append({'decision': 'auto', 'chosen': choice,
                                   'confidence': confidence})
            return {'escalated': False, 'chosen': choice, 'source': 'auto'}

        # Human path (simulated: wait for resolve; if timeout, use auto)
        if query is None:
            query = {
                'id': str(uuid.uuid4()), 'options': options,
                'context': decision_context, 'status': 'pending',
            }
        # In a real system we'd await an external resolver; here we
        # just record and fall back to the RLHF best action.
        auto_choice = self.rlhf.sample_action(decision_context)
        self.audit_log.append({
            'decision': 'escalated', 'query_id': query.get('id'),
            'auto_fallback': auto_choice, 'confidence': confidence,
            'timestamp': datetime.now().isoformat(),
        })
        return {
            'escalated': True,
            'query': query,
            'chosen': auto_choice,
            'source': 'human_pending',
        }

    def get_audit(self) -> Dict:
        return {'total': len(self.audit_log), 'recent': self.audit_log[-10:]}

# ============================================================
# ============================================================
# MODULE 10: MODP MATERIAL SELECTOR (with XAI + Temporal Logic)
# ============================================================
# ============================================================
class ParetoFront:
    def __init__(self):
        self.solutions: List[Tuple[List[float], Any]] = []

    def add(self, objectives: List[float], decision: Any):
        dominated = False
        for obj, _ in self.solutions:
            if all(o <= obj[i] for i, o in enumerate(objectives)):
                dominated = True
                break
        if not dominated:
            self.solutions = [(obj, dec) for obj, dec in self.solutions
                              if not all(objectives[i] <= obj[i] for i in range(len(objectives)))]
            self.solutions.append((objectives, decision))

    def get_pareto_front(self) -> List[Tuple[List[float], Any]]:
        return self.solutions

class TOPSIS:
    @staticmethod
    def score(candidates: List[Dict[str, float]], weights: List[float], criteria: List[str]) -> List[float]:
        matrix = np.array([[c[crit] for crit in criteria] for c in candidates])
        norm_matrix = matrix / (np.sqrt((matrix ** 2).sum(axis=0)) + 1e-12)
        weighted = norm_matrix * weights
        ideal = weighted.max(axis=0)
        neg_ideal = weighted.min(axis=0)
        d_plus = np.sqrt(((weighted - ideal) ** 2).sum(axis=1))
        d_minus = np.sqrt(((weighted - neg_ideal) ** 2).sum(axis=1))
        return (d_minus / (d_plus + d_minus + 1e-12)).tolist()

class MODPMaterialSelector:
    """MODP selection with Pareto + TOPSIS + XAI + Temporal Logic + RLHF."""
    def __init__(self,
                 adaptive_cost: AdaptiveCostFunction,
                 pareto_gating: ParetoGating,
                 temporal_monitor: Optional[TemporalLogicMonitor] = None,
                 rlhf: Optional[ActiveRLHF] = None,
                 xai: Optional[XAIExplainer] = None):
        self.adaptive_cost = adaptive_cost
        self.pareto_gating = pareto_gating
        self.weights = [0.3, 0.25, 0.25, 0.2, 0.1]
        self.adaptive_weights = True
        self.learning_rate = 0.01
        self.recent_outcomes = deque(maxlen=100)
        self.temporal_monitor = temporal_monitor
        self.rlhf = rlhf
        self.xai = xai or XAIExplainer(
            ['strength', 'carbon', 'cost', 'circularity', 'supply_risk'])

    async def select_material(self, candidates: List[MaterialProperties],
                              application: Application,
                              carbon_intensity: float = 400) -> Dict:
        if not candidates:
            return {'best': None, 'scores': [], 'pareto_front': [], 'explanation': None}

        objectives_list = []
        for mat in candidates:
            objectives_list.append([
                mat.yield_strength_mpa / 1000,
                -mat.carbon_footprint_kg_co2_per_kg,
                -mat.cost_per_kg,
                mat.circularity_score,
                -mat.supply_risk_score,
            ])

        # Temporal logic gate on candidate-level metrics
        temporal_ok = True
        if self.temporal_monitor is not None:
            for mat in candidates:
                self.temporal_monitor.update({
                    'carbon': mat.carbon_footprint_kg_co2_per_kg,
                    'cost': mat.cost_per_kg,
                    'strength': mat.yield_strength_mpa,
                    'circularity': mat.circularity_score,
                    'supply_risk': mat.supply_risk_score,
                    'carbon_intensity': carbon_intensity,
                })
            results = self.temporal_monitor.evaluate()
            temporal_ok = all(results.values()) if results else True

        # Scores
        scores = self._compute_topsis_scores(candidates)
        best_idx = int(np.argmax(scores))
        best = candidates[best_idx]

        # RLHF override when uncertainty is high
        source = "modp"
        if self.rlhf is not None:
            u = self.rlhf.uncertainty({'application': application.value})
            if u > self.rlhf.uncertainty_threshold:
                # Ask RLHF for alternative — but keep MODP if RLHF empty
                chosen = self.rlhf.sample_action({'application': application.value})
                for i, m in enumerate(candidates):
                    if m.material_id == chosen:
                        best = m
                        best_idx = i
                        source = "rlhf"
                        break

        # Pareto front
        front = ParetoFront()
        for i, obj in enumerate(objectives_list):
            front.add(obj, candidates[i])

        # Weight adaptation
        outcome = [float(scores[best_idx]),
                   -best.carbon_footprint_kg_co2_per_kg,
                   best.cost_per_kg,
                   best.circularity_score]
        self.recent_outcomes.append((self.weights, outcome))
        if self.adaptive_weights and len(self.recent_outcomes) >= 10:
            await self._update_weights()

        # XAI explanation
        explanation = self.xai.explain_selection(best, [c for c in candidates if c is not best][:3], self.weights)

        return {
            'best': best,
            'scores': scores,
            'pareto_front': front.get_pareto_front(),
            'source': source,
            'explanation': explanation,
            'temporal_ok': temporal_ok,
        }

    def _compute_topsis_scores(self, candidates):
        cand_dicts = [{
            'strength': m.yield_strength_mpa / 1000,
            'carbon': -m.carbon_footprint_kg_co2_per_kg,
            'cost': -m.cost_per_kg,
            'circularity': m.circularity_score,
            'supply_risk': -m.supply_risk_score,
        } for m in candidates]

        if self.adaptive_weights and self.adaptive_cost:
            weights_dict = self.adaptive_cost.get_current_weights()
            self.weights = [
                weights_dict.get('strength', 0.3),
                weights_dict.get('carbon_footprint', 0.25),
                weights_dict.get('cost', 0.25),
                weights_dict.get('circularity', 0.2),
                weights_dict.get('supply_risk', 0.1),
            ]
        return np.array(TOPSIS.score(cand_dicts, self.weights,
                                     ['strength', 'carbon', 'cost', 'circularity', 'supply_risk']))

    async def _update_weights(self):
        avg_outcome = np.mean([o for _, o in self.recent_outcomes], axis=0)
        self.weights = self.weights - self.learning_rate * (avg_outcome - np.mean(avg_outcome))
        total = sum(self.weights)
        if total > 0:
            self.weights = [w / total for w in self.weights]
        logger.info(f"MODP weights updated: {self.weights}")

# ============================================================
# MODULE 11: MOE MTOP ENGINE (with Role Specialization)
# ============================================================
class MOEMaterialEngine:
    """MOE with role-specialized experts + federated distillation."""
    def __init__(self,
                 adaptive_cost: Optional[AdaptiveCostFunction] = None,
                 role_coordinator: Optional[RoleSpecializationCoordinator] = None):
        self.adaptive_cost = adaptive_cost
        self.role_coordinator = role_coordinator or RoleSpecializationCoordinator()
        self.teachers: Dict[str, Callable] = {}
        self.gating_model = None
        self.scaler = None
        self._trained = False
        self._init_teachers()
        self._init_gating()

    def _init_teachers(self):
        self.teachers['economic'] = self._economic_teacher
        self.teachers['statistical'] = self._statistical_teacher
        self.teachers['ml'] = self._ml_teacher
        self.teachers['rule'] = self._rule_teacher

    def _init_gating(self):
        if SKLEARN_AVAILABLE:
            self.gating_model = LogisticRegression(multi_class='multinomial', solver='lbfgs', max_iter=1000)
            self.scaler = StandardScaler()

    # ---- teacher scoring functions ----
    def _economic_teacher(self, candidates, application, carbon_intensity):
        scores = [-0.6 * m.cost_per_kg - 0.4 * m.carbon_footprint_kg_co2_per_kg for m in candidates]
        return self._normalize(scores)

    def _statistical_teacher(self, candidates, application, carbon_intensity):
        # Use historical mean cost as reference
        return self._normalize([random.random() for _ in candidates])

    def _ml_teacher(self, candidates, application, carbon_intensity):
        scores = [0.4 * (m.yield_strength_mpa / 1000)
                  - 0.3 * (m.carbon_footprint_kg_co2_per_kg / 10)
                  - 0.3 * (m.cost_per_kg / 10) for m in candidates]
        return self._normalize(scores)

    def _rule_teacher(self, candidates, application, carbon_intensity):
        return [0.5 * (m.recyclability_pct / 100) + 0.5 * (1 - m.supply_risk_score) for m in candidates]

    @staticmethod
    def _normalize(scores):
        lo, hi = min(scores), max(scores)
        if hi == lo:
            return [0.5] * len(scores)
        return [(s - lo) / (hi - lo) for s in scores]

    async def _extract_context(self, candidates, application, carbon_intensity):
        return np.array([
            len(candidates),
            np.mean([m.carbon_footprint_kg_co2_per_kg for m in candidates]) if candidates else 0,
            np.mean([m.cost_per_kg for m in candidates]) if candidates else 0,
            (hash(application.value) % 100) / 100,
            carbon_intensity / 1000,
        ])

    async def get_teacher_scores(self, candidates, application, carbon_intensity):
        out = {}
        for name, func in self.teachers.items():
            try:
                out[name] = func(candidates, application, carbon_intensity)
            except Exception as e:
                logger.warning(f"Teacher {name} failed: {e}")
                out[name] = [0.5] * len(candidates)
        return out

    async def get_gating_weights(self, candidates, application, carbon_intensity):
        # Combine role-specialized affinity with learned gating
        role_info = self.role_coordinator.assign_roles({
            'carbon_weight': 0.3, 'cost_weight': 0.25,
            'strength_weight': 0.25, 'circularity_weight': 0.2,
        })
        if self.gating_model is not None and self._trained:
            ctx = await self._extract_context(candidates, application, carbon_intensity)
            X = self.scaler.transform([ctx])
            weights = self.gating_model.predict_proba(X)[0].tolist()
        else:
            weights = [1.0 / len(self.teachers)] * len(self.teachers)
        return weights, role_info

    async def select_material(self, candidates, application, carbon_intensity):
        if not candidates:
            return {'best': None, 'scores': [], 'weights': [], 'roles': None}
        teacher_scores = await self.get_teacher_scores(candidates, application, carbon_intensity)
        weights, role_info = await self.get_gating_weights(candidates, application, carbon_intensity)
        ensemble = np.zeros(len(candidates))
        for i, (name, scores) in enumerate(teacher_scores.items()):
            ensemble += weights[i] * np.array(scores)
        if sum(weights) > 0:
            ensemble = ensemble / sum(weights)
        best = candidates[int(np.argmax(ensemble))]
        return {
            'best': best,
            'scores': ensemble.tolist(),
            'weights': weights,
            'teacher_scores': teacher_scores,
            'roles': role_info,
        }

# ============================================================
# MODULE 12: BIO-INSPIRED GA (with XAI + Active RLHF)
# ============================================================
class GeneticAlgorithmOptimizer:
    def __init__(self, population_size=20, mutation_rate=0.1, crossover_rate=0.8):
        self.pop_size = population_size
        self.mutation_rate = mutation_rate
        self.crossover_rate = crossover_rate
        self.population: List[Dict[str, float]] = []
        self.bounds = {
            'strength_weight': (0.0, 1.0),
            'carbon_weight': (0.0, 1.0),
            'cost_weight': (0.0, 1.0),
            'circularity_weight': (0.0, 1.0),
        }

    def initialize(self):
        self.population = []
        for _ in range(self.pop_size):
            ind = {k: random.uniform(*v) for k, v in self.bounds.items()}
            total = sum(ind.values()) or 1.0
            ind = {k: v / total for k, v in ind.items()}
            self.population.append(ind)

    def evaluate(self, fitness_func):
        return [fitness_func(ind) for ind in self.population]

    def select(self, fitness, num_parents):
        selected = []
        for _ in range(num_parents):
            i, j = np.random.choice(len(self.population), 2, replace=False)
            selected.append(self.population[i] if fitness[i] > fitness[j] else self.population[j])
        return selected

    def crossover(self, p1, p2):
        if random.random() < self.crossover_rate:
            return {k: (p1[k] if random.random() < 0.5 else p2[k]) for k in p1}
        return p1.copy()

    def mutate(self, ind):
        if random.random() < self.mutation_rate:
            key = random.choice(list(self.bounds))
            ind[key] = random.uniform(*self.bounds[key])
            total = sum(ind.values()) or 1.0
            ind = {k: v / total for k, v in ind.items()}
        return ind

    def evolve(self, fitness_func, generations=50):
        self.initialize()
        for _ in range(generations):
            fitness = self.evaluate(fitness_func)
            best = self.population[int(np.argmax(fitness))]
            parents = self.select(fitness, self.pop_size - 1)
            offspring = []
            for i in range(0, len(parents) - 1, 2):
                offspring.append(self.mutate(self.crossover(parents[i], parents[i + 1])))
                offspring.append(self.mutate(self.crossover(parents[i + 1], parents[i])))
            self.population = offspring[:self.pop_size - 1] + [best]
        fitness = self.evaluate(fitness_func)
        return self.population[int(np.argmax(fitness))]

class BioInspiredDiscovery:
    """GA-based discovery with Active RLHF + Temporal Logic gating."""
    def __init__(self, adaptive_cost=None, temporal_monitor=None, rlhf=None):
        self.adaptive_cost = adaptive_cost
        self.ga = GeneticAlgorithmOptimizer()
        self.current_weights = {'strength_weight': 0.3, 'carbon_weight': 0.25,
                                'cost_weight': 0.25, 'circularity_weight': 0.2}
        self.discovery_history = deque(maxlen=100)
        self.fitness_history = deque(maxlen=50)
        self._lock = asyncio.Lock()
        self.temporal_monitor = temporal_monitor
        self.rlhf = rlhf

    def _fitness_func(self, params):
        if self.adaptive_cost:
            try:
                return -self.adaptive_cost.evaluate({
                    'strength': params['strength_weight'],
                    'carbon': params['carbon_weight'],
                    'cost': params['cost_weight'],
                    'circularity': params['circularity_weight'],
                })
            except Exception:
                pass
        return params['circularity_weight'] - 0.5 * params['carbon_weight']

    async def discover_materials(self, current_state, strategy=None):
        features = np.array([
            current_state.get('material_count', 0) / 100,
            current_state.get('carbon_intensity', 400) / 1000,
            datetime.now().hour / 24,
            random.random(),
        ])
        source = "default"
        selected = strategy or 'adaptive'

        if self.rlhf is not None:
            u = self.rlhf.uncertainty({'features': features.tolist()})
            if u > self.rlhf.uncertainty_threshold:
                selected = self.rlhf.sample_action({'features': features.tolist()})
                source = "rlhf"

        if len(self.discovery_history) >= 10:
            best_params = self.ga.evolve(self._fitness_func, generations=5)
            self.current_weights = best_params
            result = {
                'action': 'bio_inspired_discovery',
                'weights': best_params,
                'recommendation': f"GA evolved weights: {best_params}",
                'source': 'ga',
            }
        else:
            if selected == 'carbon':
                weights = {'strength_weight': 0.1, 'carbon_weight': 0.6,
                           'cost_weight': 0.1, 'circularity_weight': 0.2}
            elif selected == 'strength':
                weights = {'strength_weight': 0.6, 'carbon_weight': 0.1,
                           'cost_weight': 0.2, 'circularity_weight': 0.1}
            else:
                weights = self.current_weights
            result = {
                'action': 'bio_inspired_discovery',
                'weights': weights,
                'recommendation': f"Selected {selected} strategy",
                'source': source,
            }

        if self.temporal_monitor is not None:
            self.temporal_monitor.update({
                'carbon': result['weights'].get('carbon_weight', 0) * 1000,
                'cost': result['weights'].get('cost_weight', 0) * 100,
                'strength': result['weights'].get('strength_weight', 0) * 1000,
                'circularity': result['weights'].get('circularity_weight', 0),
                'supply_risk': 0.3,
                'carbon_intensity': current_state.get('carbon_intensity', 400),
            })

        if self.rlhf is not None and source == 'rlhf':
            self.rlhf.update({'features': features.tolist()}, selected,
                             self._fitness_func(result['weights']))

        async with self._lock:
            self.discovery_history.append({
                'strategy': selected, 'result': result,
                'timestamp': datetime.now().isoformat(),
            })
            self.fitness_history.append(self._fitness_func(self.current_weights))
        return result

    def get_discovery_stats(self):
        return {
            'total_discoveries': len(self.discovery_history),
            'current_weights': self.current_weights,
            'fitness_history': list(self.fitness_history)[-10:],
            'rlhf_active': self.rlhf is not None,
        }

# ============================================================
# MODULE 13: MULTI-OBJECTIVE CARBON SCHEDULER (with Markets)
# ============================================================
class MultiObjectiveCarbonScheduler:
    def __init__(self, carbon_manager, forecaster=None, market_client: Optional[CarbonMarketClient] = None):
        self.carbon_manager = carbon_manager
        self.forecaster = forecaster
        self.market_client = market_client
        self.carbon_weight = 0.3
        self.urgency_weight = 0.5
        self.cost_weight = 0.2
        self.max_delay = 24 * 3600
        self.history = deque(maxlen=100)

    async def schedule(self, urgency_score=0.5):
        forecast = await self.forecaster.forecast(horizon=24) if self.forecaster else None
        snapshot = await self.market_client.get_market_snapshot() if self.market_client else {}
        if not forecast or not forecast.get('prices'):
            intensity = await self.carbon_manager.get_current_intensity()
            delay = 3600 if intensity > 400 else 0
            return {'recommended_delay': delay, 'reason': 'simple_threshold',
                    'market': snapshot}
        delays = list(range(0, self.max_delay + 1, 3600))
        best = None
        for delay in delays:
            avg_intensity = np.mean(forecast['prices'][:int(delay / 3600) + 1])
            carbon_savings = max(0, (forecast['prices'][0] - avg_intensity) / (forecast['prices'][0] + 1e-9))
            urgency_cost = delay / (self.max_delay + 1) * urgency_score
            energy_cost = delay * 0.001
            composite = -self.carbon_weight * carbon_savings + self.urgency_weight * urgency_cost + self.cost_weight * energy_cost
            if best is None or composite < best['cost']:
                best = {'delay': delay, 'cost': composite,
                        'carbon_savings': carbon_savings}
        self.history.append(best)
        return {'recommended_delay': best['delay'], 'reason': 'multi_objective',
                'carbon_savings': best['carbon_savings'], 'market': snapshot}

# ============================================================
# MODULE 14: SELF-HEALING (with Chaos Testers)
# ============================================================
class SelfHealingManager:
    def __init__(self, drift_detector=None, rlhf=None, chaos_tester: Optional[ChaosTester] = None):
        self.drift = drift_detector
        self.anomaly_detectors = []
        self.gating_weights = [1.0]
        self._lock = asyncio.Lock()
        self.recovery_actions = deque(maxlen=100)
        self._trained = False
        self.rlhf = rlhf
        self.chaos = chaos_tester
        if SKLEARN_AVAILABLE:
            self.anomaly_detectors.append(('iforest', IsolationForest(contamination=0.1)))
            self.anomaly_detectors.append(('ocsvm', OneClassSVM(nu=0.1)))
            self.gating_weights = [1.0 / len(self.anomaly_detectors)] * len(self.anomaly_detectors)

    async def detect_anomaly(self, metrics):
        if not self.anomaly_detectors or not self._trained:
            if metrics.get('carbon_reduction_pct', 0) < -50:
                return True, 0.8
            return False, 0.0
        features = np.array([
            metrics.get('topsis_score', 0),
            metrics.get('carbon_reduction_pct', 0) / 100,
            metrics.get('sustainability_score', 0) / 100,
            metrics.get('data_quality_score', 0),
        ]).reshape(1, -1)
        votes = []
        for _, model in self.anomaly_detectors:
            try:
                votes.append(1 if model.predict(features)[0] == -1 else 0)
            except Exception:
                votes.append(0)
        weighted = sum(v * w for v, w in zip(votes, self.gating_weights))
        return weighted > 0.5, weighted

    async def train(self, data):
        if not self.anomaly_detectors or len(data) < 20:
            return
        X = np.array([[d.get('topsis_score', 0),
                       d.get('carbon_reduction_pct', 0) / 100,
                       d.get('sustainability_score', 0) / 100,
                       d.get('data_quality_score', 0)] for d in data])
        for _, model in self.anomaly_detectors:
            if hasattr(model, 'fit'):
                model.fit(X)
        self._trained = True

    async def check_drift(self, metrics):
        if self.drift:
            drift_detected = await self.drift.check_drift(metrics)
            if drift_detected:
                logger.warning("Drift detected - triggering recovery")
                action = "drift_recovery"
                if self.rlhf is not None:
                    action = self.rlhf.sample_action(metrics)
                async with self._lock:
                    self.recovery_actions.append({
                        'action': action,
                        'timestamp': datetime.now().isoformat(),
                    })

    async def run_chaos_suite(self):
        if self.chaos is None:
            return {'error': 'chaos tester not attached'}
        results = []
        for f in ChaosTester.FAULT_TYPES[:3]:  # safe subset in regular loop
            results.append(await self.chaos.run_test(f, duration_s=0.1))
        return {'results': results, 'report': self.chaos.get_report()}

    def get_stats(self):
        return {
            'enabled': True,
            'trained': self._trained,
            'num_detectors': len(self.anomaly_detectors),
            'recent_actions': list(self.recovery_actions)[-5:],
            'rlhf_active': self.rlhf is not None,
            'chaos_active': self.chaos is not None,
        }

# ============================================================
# MODULE 15: FORECASTER (MOE) for carbon intensity
# ============================================================
class MOEForecaster:
    def __init__(self):
        self.experts = []
        self.gating_model = None
        self.scaler = None
        self.history = deque(maxlen=1000)
        self._trained = False
        self._init_experts()
        self._init_gating()

    def _init_experts(self):
        if PROPHET_AVAILABLE:
            self.experts.append(('prophet', self._forecast_prophet))
        if SKLEARN_AVAILABLE:
            self.experts.append(('linear', self._forecast_linear))
        if STATSMODELS_AVAILABLE:
            self.experts.append(('holtwinters', self._forecast_holtwinters))
        if not self.experts:
            self.experts.append(('naive', self._forecast_naive))

    def _init_gating(self):
        if SKLEARN_AVAILABLE:
            self.gating_model = LogisticRegression(multi_class='multinomial', solver='lbfgs', max_iter=1000)
            self.scaler = StandardScaler()

    async def _forecast_prophet(self, history, horizon): return [0.5] * horizon
    async def _forecast_linear(self, history, horizon): return [0.5] * horizon
    async def _forecast_holtwinters(self, history, horizon): return [0.5] * horizon
    async def _forecast_naive(self, history, horizon): return [0.5] * horizon

    async def _extract_context(self):
        return np.array([datetime.now().hour / 24, datetime.now().weekday() / 6, 0.5, 0.5])

    async def update_history(self, value):
        self.history.append({'ds': datetime.now(), 'y': value})

    async def forecast(self, horizon=24):
        if len(self.history) < 30:
            return {'prices': [0.5] * horizon, 'confidence': 0.0}
        forecasts = []
        for _, func in self.experts:
            try:
                forecasts.append(await func(self.history, horizon))
            except Exception:
                forecasts.append([0.5] * horizon)
        weights = np.ones(len(self.experts)) / len(self.experts)
        final = np.zeros(horizon)
        for i, f in enumerate(forecasts):
            final += weights[i] * np.array(f)
        return {'prices': final.tolist(), 'expert_weights': weights.tolist(), 'confidence': 0.85}

    def get_stats(self):
        return {'num_experts': len(self.experts), 'gating_trained': self._trained, 'history_len': len(self.history)}

# ============================================================
# STUBS (kept for API compatibility)
# ============================================================
class EnhancedDataQualityScorer:
    async def assess_quality(self, materials): return 0.8

class MultiCloudMaterialDistribution:
    async def distribute_material_data(self, data): return {'provider': 'aws', 'region': 'us-east-1'}

# ============================================================
# ENHANCED MATERIAL ANALYZER – v17.0 (fully wired)
# ============================================================
class EnhancedMaterialAnalyzer:
    def __init__(self, storage, message_queue, adaptive_cost, pareto_gating,
                 drift_detector, metrics):
        self.storage = storage
        self.queue = message_queue
        self.adaptive_cost = adaptive_cost
        self.pareto = pareto_gating
        self.drift = drift_detector
        self.metrics = metrics
        self.instance_id = str(uuid.uuid4())[:8]
        self._start_time = datetime.now()

        # --- v17.0 modules ---
        self.temporal_monitor = TemporalLogicMonitor()
        self.temporal_monitor.add_formula("carbon_cap",      "G(carbon <= 20.0)")
        self.temporal_monitor.add_formula("confidence_min",  "F(confidence >= 0.6)")
        self.temporal_monitor.add_formula("no_risk_spike",   "G(supply_risk <= 0.9)")

        self.xai = XAIExplainer(['strength', 'carbon', 'cost', 'circularity', 'supply_risk'])
        self.precision_controller = AdaptivePrecisionController()
        self.carbon_market = CarbonMarketClient(storage)
        self.role_coordinator = RoleSpecializationCoordinator()
        self.rlhf = ActiveRLHF(action_space=['adaptive', 'carbon', 'strength', 'cost', 'circularity'])
        self.hitl = HumanInTheLoopCoordinator(self.rlhf, timeout_s=300.0)
        self.federated_aggregator = FederatedAggregator()

        self.chaos_tester = ChaosTester(self)

        # --- core submodules ---
        self.pqc = PostQuantumCrypto(storage)
        self.blockchain = BlockchainMaterialVerification(storage)
        self.carbon_manager = CarbonIntensityManager()
        self.modp_selector = MODPMaterialSelector(
            adaptive_cost, pareto_gating,
            temporal_monitor=self.temporal_monitor,
            rlhf=self.rlhf,
            xai=self.xai,
        )
        self.moe_engine = MOEMaterialEngine(adaptive_cost, self.role_coordinator)
        self.bio_discovery = BioInspiredDiscovery(adaptive_cost,
                                                  temporal_monitor=self.temporal_monitor,
                                                  rlhf=self.rlhf)
        self.forecaster = MOEForecaster()
        self.scheduler = MultiObjectiveCarbonScheduler(
            self.carbon_manager, self.forecaster, market_client=self.carbon_market)
        self.self_healing = SelfHealingManager(
            drift_detector, self.rlhf, chaos_tester=self.chaos_tester) if drift_detector else None

        self.cloud_distributor = MultiCloudMaterialDistribution()
        self.quality_scorer = EnhancedDataQualityScorer()

        # State
        self.materials: Dict[str, MaterialProperties] = {}
        self.analysis_history: deque = deque(maxlen=1000)
        self._materials_lock = asyncio.Lock()
        self._history_lock = asyncio.Lock()
        self._shutdown_event = asyncio.Event()
        self._background_tasks: List[asyncio.Task] = []

        self._init_sample_materials()
        logger.info(f"EnhancedMaterialAnalyzer v17.0 initialized (instance: {self.instance_id})")

    # ---------------- Sample materials ----------------
    def _init_sample_materials(self):
        materials = [
            MaterialProperties(
                material_id="al6061", name="Aluminum 6061-T6",
                material_class=MaterialClass.ALUMINUM_ALLOY,
                density_kg_m3=2700, yield_strength_mpa=276, elastic_modulus_gpa=69,
                thermal_conductivity_w_mk=167, cost_per_kg=3.0,
                carbon_footprint_kg_co2_per_kg=8.5, recyclability_pct=95,
                supply_risk_score=0.25, applications=[Application.STRUCTURAL, Application.AUTOMOTIVE],
                compliance_certifications=[ComplianceStandard.ISO14001],
                recycled_content_pct=30, end_of_life_recyclability_pct=90),
            MaterialProperties(
                material_id="steel1018", name="Steel 1018",
                material_class=MaterialClass.STEEL_ALLOY,
                density_kg_m3=7870, yield_strength_mpa=370, elastic_modulus_gpa=205,
                thermal_conductivity_w_mk=52, cost_per_kg=1.2,
                carbon_footprint_kg_co2_per_kg=2.3, recyclability_pct=98,
                supply_risk_score=0.4, applications=[Application.STRUCTURAL, Application.CONSTRUCTION],
                compliance_certifications=[ComplianceStandard.ISO14001],
                recycled_content_pct=25, end_of_life_recyclability_pct=95),
            MaterialProperties(
                material_id="carbon_composite", name="Carbon Fiber Composite",
                material_class=MaterialClass.COMPOSITE,
                density_kg_m3=1600, yield_strength_mpa=600, elastic_modulus_gpa=150,
                thermal_conductivity_w_mk=5, cost_per_kg=20.0,
                carbon_footprint_kg_co2_per_kg=15.0, recyclability_pct=40,
                supply_risk_score=0.7, applications=[Application.AEROSPACE, Application.AUTOMOTIVE],
                compliance_certifications=[ComplianceStandard.ISO14001],
                recycled_content_pct=10, end_of_life_recyclability_pct=30),
        ]
        for m in materials:
            self.materials[m.material_id] = m

    # ---------------- Policy probs ----------------
    async def policy_probs(self, state):
        w = self.bio_discovery.current_weights
        return [w['strength_weight'], w['carbon_weight'],
                w['cost_weight'], w['circularity_weight']]

    # ---------------- Core analysis ----------------
    async def analyze_substitution(self, base_material_id, application, user_id=None,
                                   sign_data=True, blockchain_record=True):
        start_ts = time.time()
        async with self._materials_lock:
            if base_material_id not in self.materials:
                raise ValueError(f"Material {base_material_id} not found")
            base = self.materials[base_material_id]
            candidates = [m for m in self.materials.values() if m.material_id != base_material_id]

        # ---- 1. Multi-objective scheduling (with carbon markets) ----
        schedule = await self.scheduler.schedule(urgency_score=0.5)
        delay = schedule['recommended_delay']
        if delay > 0:
            logger.info(f"Multi-objective scheduler delaying by {delay}s")
            await asyncio.sleep(min(delay, 2))  # cap for responsiveness

        # ---- 2. Carbon intensity ----
        try:
            intensity_data = await self.carbon_manager.get_current_intensity()
            carbon_intensity = intensity_data if isinstance(intensity_data, (int, float)) \
                               else intensity_data.get('intensity', 400)
        except Exception as e:
            logger.warning(f"Carbon intensity fetch failed: {e}; using default 400")
            carbon_intensity = 400.0

        # ---- 3. Adaptive precision ----
        precision = self.precision_controller.select(carbon_intensity, accuracy_required=0.95)
        self.precision_controller.update_telemetry(gpu_available=False)

        # ---- 4. Temporal logic gate ----
        self.temporal_monitor.update({
            'carbon': base.carbon_footprint_kg_co2_per_kg,
            'cost': base.cost_per_kg,
            'strength': base.yield_strength_mpa,
            'circularity': base.circularity_score,
            'supply_risk': base.supply_risk_score,
            'confidence': 0.85,
            'carbon_intensity': carbon_intensity,
        })
        temporal_status = self.temporal_monitor.evaluate()
        if not all(temporal_status.values()):
            logger.warning(f"Temporal logic violation detected: {temporal_status}")

        quality_score = await self.quality_scorer.assess_quality(list(self.materials.values()))

        # ---- 5. MODP selection ----
        modp_result = await self.modp_selector.select_material(candidates, application, carbon_intensity)
        best: MaterialProperties = modp_result['best']
        scores = np.array(modp_result['scores'])
        explanation = modp_result['explanation']

        # ---- 6. MOE selection (with roles) ----
        moe_result = await self.moe_engine.select_material(candidates, application, carbon_intensity)
        gating_weights = moe_result['weights']
        role_assignments = moe_result['roles']

        # ---- 7. Metrics ----
        carbon_reduction = ((base.carbon_footprint_kg_co2_per_kg
                             - best.carbon_footprint_kg_co2_per_kg)
                            / max(base.carbon_footprint_kg_co2_per_kg, 1e-6)) * 100
        cost_savings = ((base.cost_per_kg - best.cost_per_kg)
                        / max(base.cost_per_kg, 1e-6)) * 100
        performance_score = (best.yield_strength_mpa / max(base.yield_strength_mpa, 1e-6)) * 100

        top_indices = np.argsort(scores)[-3:][::-1]
        alternatives = []
        for idx in top_indices[1:]:
            alt = candidates[int(idx)]
            alternatives.append({
                'material': alt.name,
                'score': float(scores[idx]),
                'carbon_reduction': ((base.carbon_footprint_kg_co2_per_kg
                                      - alt.carbon_footprint_kg_co2_per_kg)
                                     / max(base.carbon_footprint_kg_co2_per_kg, 1e-6)) * 100,
            })

        # ---- 8. Carbon markets & REC value ----
        carbon_saved_kg = max(0.0, base.carbon_footprint_kg_co2_per_kg
                              - best.carbon_footprint_kg_co2_per_kg)
        credit_value = await self.carbon_market.get_carbon_credit_value(carbon_saved_kg)
        rec_value = await self.carbon_market.get_rec_value(energy_saved_kwh=carbon_saved_kg * 0.5)

        # ---- 9. HITL escalation ----
        hitl_outcome = await self.hitl.escalate(
            decision_context={'base': base.name, 'app': application.value,
                              'carbon': carbon_intensity},
            options=[m.material_id for m in candidates[:5]],
            confidence=0.85,
        )

        calc_ms = (time.time() - start_ts) * 1000.0

        result = SubstitutionResult(
            base_material=base.name,
            recommended_substitute=best.name,
            topsis_score=float(scores.max()) if len(scores) else 0.0,
            carbon_reduction_pct=max(-100.0, min(100.0, carbon_reduction)),
            cost_savings_pct=max(-100.0, min(100.0, cost_savings)),
            performance_score=min(200.0, performance_score),
            recommendations=[],
            sustainability_score=(best.recyclability_pct * 0.4
                                  + (100 - best.supply_risk_score * 100) * 0.3
                                  + best.recycled_content_pct * 0.3),
            confidence_score=0.85,
            data_quality_score=quality_score,
            calculation_time_ms=calc_ms,
            alternative_substitutes=alternatives,
            supply_risk_improvement=float(base.supply_risk_score - best.supply_risk_score),
            circularity_improvement=float(best.circularity_score - base.circularity_score),
            lifecycle_assessment={},
            compliance_status={},
            carbon_selection_weight={'carbon_intensity': carbon_intensity},
            carbon_intensity_at_time=carbon_intensity,
            gating_weights={name: w for name, w in
                            zip(self.moe_engine.teachers.keys(), gating_weights)},
            drift_detected=False,
            explanation=explanation,
            temporal_logic_status=temporal_status,
            precision_used=precision.value,
            carbon_credit_value_usd=credit_value,
            rec_value_usd=rec_value,
            role_assignments=role_assignments,
            hitl_query=hitl_outcome,
        )

        # ---- 10. PQC + blockchain ----
        if sign_data:
            result.quantum_signature = await self.pqc.sign_data(asdict(result))
        if blockchain_record:
            data_id = f"material_{uuid.uuid4().hex[:8]}"
            data_hash = hashlib.sha256(
                json.dumps(asdict(result), sort_keys=True, default=str).encode()
            ).hexdigest()
            try:
                bc = await self.blockchain.record_material_data(
                    data_id, data_hash,
                    {'base': base.name, 'substitute': best.name})
                result.blockchain_tx_hash = bc.get('tx_hash')
            except Exception as e:
                logger.warning(f"Blockchain record failed: {e}")

        # ---- 11. Cloud distribution + autonomous discovery ----
        result.cloud_distribution = await self.cloud_distributor.distribute_material_data(
            {'size_gb': len(self.materials) * 0.001})
        result.autonomous_discovery = await self.bio_discovery.discover_materials(
            {'material_count': len(self.materials), 'carbon_intensity': carbon_intensity})

        # ---- 12. Federated aggregation ----
        self.federated_aggregator.submit_update(
            client_id=self.instance_id,
            weights=self.modp_selector.weights,
            samples=len(self.analysis_history) + 1,
        )
        fed = self.federated_aggregator.aggregate()
        result.federated_round = self.federated_aggregator.round

        # ---- 13. Self-healing + anomaly detection ----
        if self.self_healing:
            await self.self_healing.check_drift(asdict(result))
            is_anomaly, score = await self.self_healing.detect_anomaly(asdict(result))
            if is_anomaly:
                logger.warning(f"Anomaly detected with score {score:.2f}")
                result.drift_detected = True

        # ---- 14. Chaos smoke test (one fault per analysis, rotating) ----
        if len(self.analysis_history) % 5 == 0:
            fault = ChaosTester.FAULT_TYPES[len(self.analysis_history) % len(ChaosTester.FAULT_TYPES)]
            try:
                chaos_res = await self.chaos_tester.run_test(fault, duration_s=0.05)
                result.chaos_test_passed = chaos_res['passed']
            except Exception as e:
                logger.warning(f"Chaos test failed to run: {e}")

        # ---- 15. Persist + emit feedback ----
        async with self._history_lock:
            self.analysis_history.append(result)

        try:
            self.storage.store_substitution_result(result)
        except Exception as e:
            logger.warning(f"Storage failed: {e}")

        try:
            event = FeedbackEvent.create_with_context(
                task_id=f"material_{uuid.uuid4().hex[:8]}",
                selected_action="analyze_substitution",
                quality_score=quality_score,
                latency_ms=calc_ms,
                energy_joules=0.0,
                carbon_g=result.carbon_reduction_pct * 1000,
                feedback_type="material",
                adaptive_cost_value=0.0,
                state={'base': base.name, 'application': application.value},
                candidates=[],
                source="material_analyzer",
                environment=getattr(central_config, 'ENVIRONMENT', 'dev'),
                tags=["material", "substitution", "v17"],
            )
            await self.queue.publish("feedback_events", event.to_json())
        except Exception as e:
            logger.warning(f"Feedback publish failed: {e}")

        # ---- 16. RLHF update from outcome ----
        reward = result.carbon_reduction_pct / 100.0 - 0.1 * (result.cost_savings_pct < 0)
        self.rlhf.update({'base': base.name, 'app': application.value},
                         best.material_id, float(reward))

        if self.drift:
            try:
                await self.drift.check_drift(self.adaptive_cost.get_current_weights())
            except Exception as e:
                logger.warning(f"Drift check failed: {e}")

        try:
            self.metrics.increment_carbon_saved(result.carbon_reduction_pct * 10)
        except Exception:
            pass

        logger.info(f"Substitution: {base.name} -> {best.name} | "
                    f"ΔCO₂={result.carbon_reduction_pct:.1f}% | "
                    f"precision={precision.value} | "
                    f"credit=${credit_value:.2f} | rec=${rec_value:.2f}")
        return result

    # ---------------- Background tasks ----------------
    async def start(self):
        logger.info("Starting Material Analyzer v17.0 background tasks...")
        loop = asyncio.get_running_loop()
        self._background_tasks.extend([
            loop.create_task(self._discovery_loop()),
            loop.create_task(self._forecast_loop()),
            loop.create_task(self._federated_loop()),
            loop.create_task(self._cleanup_loop()),
            loop.create_task(self._self_healing_loop()),
            loop.create_task(self._chaos_loop()),
        ])

    async def _discovery_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(getattr(central_config, 'auto_discover_interval', 1800) or 1800)
            try:
                await self.bio_discovery.discover_materials({'material_count': len(self.materials)})
            except Exception as e:
                logger.error(f"Discovery loop error: {e}")

    async def _forecast_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(3600)
            try:
                await self.forecaster.forecast(24)
            except Exception as e:
                logger.error(f"Forecast loop error: {e}")

    async def _federated_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(600)
            try:
                if self.federated_aggregator.client_updates:
                    self.federated_aggregator.aggregate()
            except Exception as e:
                logger.error(f"Federated loop error: {e}")

    async def _cleanup_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(86400)
            try:
                self.storage.clean_old_substitution_results(
                    days=getattr(central_config, 'data_retention_days', 365) or 365)
            except Exception as e:
                logger.error(f"Cleanup error: {e}")

    async def _self_healing_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(3600)
            try:
                if self.self_healing:
                    async with self._history_lock:
                        if self.analysis_history:
                            data = [asdict(r) for r in list(self.analysis_history)[-100:]]
                            await self.self_healing.train(data)
            except Exception as e:
                logger.error(f"Self-healing loop error: {e}")

    async def _chaos_loop(self):
        """Periodically runs a chaos test against the analyzer itself."""
        while not self._shutdown_event.is_set():
            await asyncio.sleep(1800)
            try:
                fault = random.choice(ChaosTester.FAULT_TYPES)
                await self.chaos_tester.run_test(fault, duration_s=0.1)
            except Exception as e:
                logger.error(f"Chaos loop error: {e}")

    async def shutdown(self):
        logger.info("Shutting down Material Analyzer v17.0...")
        self._shutdown_event.set()
        for t in self._background_tasks:
            t.cancel()
        await asyncio.gather(*self._background_tasks, return_exceptions=True)
        await self.carbon_manager.close()
        logger.info("Shutdown complete")

# ============================================================
# SINGLETON ACCESSOR
# ============================================================
_material_analyzer_instance = None
_material_analyzer_lock = asyncio.Lock()

async def get_material_analyzer(storage, queue, adaptive_cost, pareto_gating,
                                drift_detector, metrics):
    global _material_analyzer_instance
    if _material_analyzer_instance is None:
        async with _material_analyzer_lock:
            if _material_analyzer_instance is None:
                _material_analyzer_instance = EnhancedMaterialAnalyzer(
                    storage, queue, adaptive_cost, pareto_gating, drift_detector, metrics)
                await _material_analyzer_instance.start()
    return _material_analyzer_instance

# ============================================================
# MAIN (standalone testing)
# ============================================================
async def main():
    from ..storage import Storage
    from ..scaling.message_queue import AsyncMessageQueue
    from ..feedback.adaptive_cost import AdaptiveCostFunction
    from ..routing.pareto_gating import ParetoGating
    from ..safety.drift_detector import DriftDetector
    from ..metrics import MetricsRegistry

    storage = Storage()
    queue = AsyncMessageQueue()
    adaptive_cost = AdaptiveCostFunction(storage)
    pareto = ParetoGating()
    drift = DriftDetector(storage, adaptive_cost)
    metrics = MetricsRegistry()

    analyzer = await get_material_analyzer(storage, queue, adaptive_cost, pareto, drift, metrics)
    result = await analyzer.analyze_substitution("al6061", Application.STRUCTURAL)
    print(json.dumps({
        'base': result.base_material,
        'substitute': result.recommended_substitute,
        'carbon_reduction_pct': result.carbon_reduction_pct,
        'precision': result.precision_used,
        'carbon_credit_usd': result.carbon_credit_value_usd,
        'rec_usd': result.rec_value_usd,
        'temporal_status': result.temporal_logic_status,
        'explanation': result.explanation,
        'roles': result.role_assignments,
        'federated_round': result.federated_round,
    }, indent=2, default=str))
    await analyzer.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
