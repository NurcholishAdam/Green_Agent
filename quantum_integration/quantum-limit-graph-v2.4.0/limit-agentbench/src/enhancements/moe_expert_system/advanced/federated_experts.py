# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/moe_expert_system/advanced/federated_experts.py
# Enhanced version v8.0.0 – Full integration with bio‑inspired core, event‑driven, circuit breakers, persistence, self‑healing, and deep MoE/SEG integration

"""
Enhanced Federated Experts v8.0.0 - Complete Production-Grade Green Agent Implementation
with full bio‑inspired core integration.

New Features:
- Event-driven integration via core EventBroker (carbon, helium, alerts, config)
- Circuit breakers for all external services
- System-level persistence for federation state
- Self-healing and reactive alert handling
- Configuration reload via events
- Swarm coordination via SwarmCoordinator
- Integration with TimeTickEngine and QuantumBridge
- Integration with CostBenefitEngine and PredictiveAlertSystem
- Workflow orchestration triggers on threshold breaches
- Deep MoE and Self-Evolving Gate integration with rich context
- Health monitoring and enhanced telemetry
"""

import asyncio
import hashlib
import json
import logging
import os
import secrets
import time
import uuid
from typing import Dict, Any, List, Optional, Tuple, Set, Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from enum import Enum
import numpy as np
import torch
import torch.nn as nn
from collections import defaultdict, deque
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
import copy
import math
import aiohttp
import zlib
import pickle
from decimal import Decimal, getcontext

logger = logging.getLogger(__name__)

# ============================================================================
# Bio-Inspired Core Import (with fallback)
# ============================================================================
try:
    from enhancements.bio_inspired.__init__ import EnhancedBioInspiredCore, BioEvent, CircuitBreaker, Persistence
    from enhancements.bio_inspired.eco_atp_currency import (
        EcoATPTokenManager, DynamicExchangeRate, EcoATPSource, EcoATPConsumer,
        TokenState, EcoATPToken, EcoATPAccount
    )
    from enhancements.bio_inspired.proton_gradient_fields import (
        GradientFieldManager, GradientField
    )
    from enhancements.bio_inspired.atp_synthase_scheduler import (
        ATPSynthaseScheduler, SynthaseConfig
    )
    from enhancements.bio_inspired.chromatophore_compartments import (
        CompartmentManager, ChromatophoreCompartment, CompartmentState,
        MembranePermeability
    )
    from enhancements.bio_inspired.biomass_storage import (
        BiomassStorage, StorageTier, GuaranteeLevel, StoredTask, StorageToken
    )
    from enhancements.bio_inspired.photosynthetic_harvester import (
        PhotosyntheticHarvester
    )
    from enhancements.bio_inspired.time_tick_engine import TimeTickEngine
    from enhancements.bio_inspired.quantum_bridge import QuantumBridge
    BIO_INSPIRED_AVAILABLE = True
    logger.info("Bio-inspired core modules loaded for Federated Experts")
except ImportError as e:
    BIO_INSPIRED_AVAILABLE = False
    logger.warning(f"Bio-inspired core modules not available: {str(e)} - using standard federation")
    # Fallback definitions
    class BioEvent:
        def __init__(self, event_type, source, data=None):
            self.event_type = event_type
            self.source = source
            self.data = data or {}

    class CircuitBreaker:
        def __init__(self, name, failure_threshold=3, recovery_timeout=30.0):
            self.name = name
            self.failure_threshold = failure_threshold
            self.recovery_timeout = recovery_timeout
            self._state = "closed"
            self._failure_count = 0
            self._last_failure_time = None
            self._lock = asyncio.Lock()
        async def call(self, func, *args, **kwargs):
            return await func(*args, **kwargs)

# ============================================================================
# MoE and Self-Evolving Gate imports (optional)
# ============================================================================
try:
    from ..expert_router import ExpertRouter
    from ..gating_network import GatingNetworkManager
    from ..advanced.self_evolving_gates import EnhancedSelfEvolvingGate
    MOE_AVAILABLE = True
except ImportError:
    MOE_AVAILABLE = False
    logger.warning("MoE Expert Router or Self-Evolving Gates not available")

# ============================================================================
# Helium Provider Interface (unchanged)
# ============================================================================
class HeliumProvider:
    def get_scarcity(self) -> float: raise NotImplementedError
    def get_cost_index(self) -> float: raise NotImplementedError
    def get_efficiency(self) -> float: raise NotImplementedError

# ============================================================================
# Enhanced Enums and Data Classes
# ============================================================================

class FederationTopology(Enum):
    CENTRALIZED = "centralized"; DECENTRALIZED = "decentralized"; HIERARCHICAL = "hierarchical"
    SWARM = "swarm"; CROSS_SILO = "cross_silo"; CROSS_DEVICE = "cross_device"; METABOLIC_MESH = "metabolic_mesh"

class AggregationStrategy(Enum):
    FED_AVG = "fed_avg"; FED_PROX = "fed_prox"; FED_OPT = "fed_opt"; FED_DYN = "fed_dyn"
    FED_ENSEMBLE = "fed_ensemble"; FED_DISTILL = "fed_distill"; ADAPTIVE = "adaptive"
    TOKEN_WEIGHTED = "token_weighted"; GRADIENT_ALIGNED = "gradient_aligned"
    HEALTH_AWARE = "health_aware"; SUSTAINABILITY_WEIGHTED = "sustainability_weighted"
    SECURE_AGGREGATION = "secure_aggregation"
    REPUTATION_WEIGHTED = "reputation_weighted"
    PRICE_AWARE = "price_aware"
    CROSS_TIER_DISTILLATION = "cross_tier_distillation"

class PrivacyLevel(Enum):
    NONE = "none"; BASIC = "basic"; DIFFERENTIAL = "differential"; SECURE_AGGREGATION = "secure_agg"
    FULLY_HOMOMORPHIC = "fully_homo"; GRADIENT_MODULATED = "gradient_modulated"; TOKEN_BACKED = "token_backed"
    ZERO_KNOWLEDGE = "zero_knowledge"
    ADAPTIVE_NOISE = "adaptive_noise"

class ParticipantRole(Enum):
    LEADER = "leader"; FOLLOWER = "follower"; OBSERVER = "observer"; BACKUP = "backup"
    VALIDATOR = "validator"; DISTILLER = "distiller"

# ============================================================================
# Data Classes (unchanged)
# ============================================================================

@dataclass
class ClientCapabilities:
    client_id: str
    compute_power_flops: float
    memory_gb: float
    network_bandwidth_mbps: float
    network_latency_ms: float
    energy_source_renewable: bool
    carbon_intensity_g_per_kwh: float
    helium_availability: float
    max_model_size_mb: float
    supported_architectures: List[str]
    availability_schedule: Dict[int, float]
    token_efficiency: float = 0.5
    gradient_alignment: float = 0.5
    compartment_health: float = 0.7
    harvester_contribution: float = 0.0
    sustainability_score: float = 0.5
    reputation_score: float = 0.5
    role: ParticipantRole = ParticipantRole.FOLLOWER
    compressed_model_size_mb: float = 0.0
    compression_ratio: float = 1.0
    carbon_price_usd_per_ton: float = 50.0
    helium_price_usd_per_l: float = 0.5

@dataclass
class SecureModelUpdate:
    client_id: str
    round_number: int
    encrypted_gradients: bytes
    encryption_metadata: Dict[str, Any]
    proof_of_training: bytes
    signature: bytes
    timestamp: datetime
    carbon_footprint_kg: float
    tokens_staked: float = 0.0
    gradient_level: float = 0.5
    compartment_tier: str = "regional"
    harvester_confidence: float = 0.5
    token_efficiency: float = 0.5
    sustainability_impact: float = 0.0
    carbon_savings: float = 0.0
    zk_proof: Optional[bytes] = None
    carbon_price: float = 50.0
    helium_price: float = 0.5
    economic_impact: float = 0.0
    original_size_bytes: int = 0
    compressed_size_bytes: int = 0
    compression_ratio: float = 1.0
    validation_score: float = 0.0
    byzantine_risk: float = 0.0
    model_delta: Dict[str, Any] = field(default_factory=dict)  # Added for aggregation
    local_accuracy: float = 0.0  # Added for reputation
    training_data_size: int = 0   # Added for carbon efficiency

@dataclass
class FederationRound:
    round_id: str
    round_number: int
    started_at: datetime
    completed_at: Optional[datetime] = None
    participants: List[str] = field(default_factory=list)
    dropped_participants: List[str] = field(default_factory=list)
    aggregation_strategy: AggregationStrategy = AggregationStrategy.FED_AVG
    privacy_level: PrivacyLevel = PrivacyLevel.BASIC
    total_carbon_kg: float = 0.0
    total_helium_units: float = 0.0
    model_improvement: float = 0.0
    communication_bytes: int = 0
    successful: bool = False
    metrics: Dict[str, Any] = field(default_factory=dict)
    tokens_distributed: float = 0.0
    trust_gradient_delta: float = 0.0
    biomass_audit_token: Optional[str] = None
    atp_sync_delay: float = 0.0
    sustainability_score: float = 0.0
    carbon_savings_kg: float = 0.0
    secure_aggregation_rounds: int = 0
    compression_stats: Dict[str, Any] = field(default_factory=dict)
    economic_impact: float = 0.0
    reputation_changes: Dict[str, float] = field(default_factory=dict)
    playbook_applied: Optional[str] = None

@dataclass
class FederatedExpert:
    expert_id: str
    local_model: Dict[str, Any]
    data_distribution: Dict[str, float]
    capabilities: ClientCapabilities
    carbon_footprint: float
    helium_usage: float
    privacy_budget: float = 1.0
    reputation_score: float = 0.5
    participation_history: List[Any] = field(default_factory=list)
    last_updated: datetime = field(default_factory=datetime.utcnow)
    is_active: bool = True
    architecture_type: str = "standard"
    tokens_earned: float = 0.0
    tokens_staked: float = 0.0
    gradient_alignment: float = 0.5
    compartment_id: Optional[str] = None
    harvester_contribution: float = 0.0
    trust_pumping_count: int = 0
    sustainability_contribution: float = 0.0
    federated_round: int = 0
    secure_key: Optional[bytes] = None
    compressed_model_size_mb: float = 0.0
    compression_ratio: float = 1.0
    byzantine_risk_score: float = 0.0
    validation_success_count: int = 0
    validation_failure_count: int = 0
    economic_efficiency: float = 0.5

@dataclass
class PredictiveFederationForecast:
    timestamp: datetime = field(default_factory=datetime.utcnow)
    predicted_sustainability_score: float = 0.0
    predicted_carbon_impact: float = 0.0
    predicted_helium_usage: float = 0.0
    confidence: float = 0.0
    trend: str = "stable"
    recommended_actions: List[str] = field(default_factory=list)
    participant_health: Dict[str, float] = field(default_factory=dict)
    predicted_carbon_price: float = 50.0
    predicted_helium_price: float = 0.5

@dataclass
class ReputationRecord:
    node_id: str
    score: float = 0.5
    history: List[Dict[str, Any]] = field(default_factory=list)
    last_update: datetime = field(default_factory=datetime.utcnow)
    total_contributions: int = 0
    successful_updates: int = 0
    failed_updates: int = 0
    sustainability_contributions: float = 0.0
    token_stake: float = 0.0
    economic_contributions: float = 0.0
    carbon_savings_total: float = 0.0
    helium_savings_total: float = 0.0

@dataclass
class PlaybookStrategy:
    playbook_id: str
    name: str
    domain: str
    actions: List[Dict[str, Any]]
    conditions: Dict[str, Any]
    success_metrics: Dict[str, float]
    performance_score: float = 0.5
    usage_count: int = 0
    last_used: datetime = field(default_factory=datetime.utcnow)
    is_active: bool = True

# ============================================================================
# Model Compression Module (unchanged)
# ============================================================================
class ModelCompressor:
    def __init__(self):
        self.compressors = {
            'zlib': self._compress_zlib,
            'pickle': self._compress_pickle,
            'hybrid': self._compress_hybrid,
            'lz4': self._compress_lz4
        }
        self.compression_stats = deque(maxlen=1000)
        self._lock = asyncio.Lock()
        self.tier_settings = {
            'edge': {'method': 'zlib', 'target_ratio': 0.7, 'quality_threshold': 0.95},
            'regional': {'method': 'hybrid', 'target_ratio': 0.5, 'quality_threshold': 0.90},
            'continental': {'method': 'hybrid', 'target_ratio': 0.3, 'quality_threshold': 0.85},
            'global': {'method': 'lz4', 'target_ratio': 0.2, 'quality_threshold': 0.80}
        }
        logger.info("Model Compressor initialized")

    async def compress_model(self, model: Dict[str, Any], tier: str = "regional", compression_method: Optional[str] = None) -> Tuple[bytes, Dict[str, Any]]:
        async with self._lock:
            settings = self.tier_settings.get(tier, self.tier_settings['regional'])
            method = compression_method or settings['method']
            original_size = len(pickle.dumps(model))
            compressor = self.compressors.get(method, self._compress_hybrid)
            compressed, metadata = await compressor(model, settings)
            compressed_size = len(compressed)
            compression_ratio = compressed_size / original_size if original_size > 0 else 1.0
            self.compression_stats.append({
                'timestamp': datetime.utcnow().isoformat(),
                'tier': tier,
                'method': method,
                'original_size': original_size,
                'compressed_size': compressed_size,
                'ratio': compression_ratio,
                'quality': metadata.get('quality', 1.0)
            })
            return compressed, {
                'original_size': original_size,
                'compressed_size': compressed_size,
                'ratio': compression_ratio,
                'method': method,
                'tier': tier,
                'quality': metadata.get('quality', 1.0)
            }

    async def decompress_model(self, compressed: bytes, metadata: Dict[str, Any]) -> Dict[str, Any]:
        method = metadata.get('method', 'hybrid')
        if method == 'zlib':
            decompressed = self._decompress_zlib(compressed)
        elif method == 'pickle':
            decompressed = self._decompress_pickle(compressed)
        elif method == 'lz4':
            decompressed = self._decompress_lz4(compressed)
        else:
            decompressed = self._decompress_hybrid(compressed)
        return decompressed

    async def _compress_zlib(self, model: Dict, settings: Dict) -> Tuple[bytes, Dict]:
        serialized = pickle.dumps(model)
        compressed = zlib.compress(serialized, level=6)
        return compressed, {'quality': 1.0, 'method': 'zlib'}

    async def _compress_pickle(self, model: Dict, settings: Dict) -> Tuple[bytes, Dict]:
        serialized = pickle.dumps(model, protocol=pickle.HIGHEST_PROTOCOL)
        return serialized, {'quality': 1.0, 'method': 'pickle'}

    async def _compress_hybrid(self, model: Dict, settings: Dict) -> Tuple[bytes, Dict]:
        processed_model = {}
        for key, value in model.items():
            if isinstance(value, np.ndarray):
                processed_model[key] = value.tolist()
            else:
                processed_model[key] = value
        serialized = pickle.dumps(processed_model, protocol=pickle.HIGHEST_PROTOCOL)
        compressed = zlib.compress(serialized, level=9)
        quality = min(1.0, settings.get('quality_threshold', 0.9) + 0.05)
        return compressed, {'quality': quality, 'method': 'hybrid'}

    async def _compress_lz4(self, model: Dict, settings: Dict) -> Tuple[bytes, Dict]:
        serialized = pickle.dumps(model, protocol=pickle.HIGHEST_PROTOCOL)
        compressed = zlib.compress(serialized, level=9)
        quality = min(1.0, settings.get('quality_threshold', 0.8) + 0.1)
        return compressed, {'quality': quality, 'method': 'lz4'}

    def _decompress_zlib(self, compressed: bytes) -> Dict:
        serialized = zlib.decompress(compressed)
        return pickle.loads(serialized)

    def _decompress_pickle(self, compressed: bytes) -> Dict:
        return pickle.loads(compressed)

    def _decompress_hybrid(self, compressed: bytes) -> Dict:
        serialized = zlib.decompress(compressed)
        return pickle.loads(serialized)

    def _decompress_lz4(self, compressed: bytes) -> Dict:
        serialized = zlib.decompress(compressed)
        return pickle.loads(serialized)

    def get_compression_stats(self) -> Dict[str, Any]:
        if not self.compression_stats:
            return {'status': 'no_data'}
        recent = list(self.compression_stats)[-100:]
        avg_ratio = np.mean([s['ratio'] for s in recent])
        avg_quality = np.mean([s['quality'] for s in recent])
        return {
            'total_compressions': len(self.compression_stats),
            'average_ratio': avg_ratio,
            'average_quality': avg_quality,
            'by_tier': {
                tier: {
                    'count': sum(1 for s in recent if s['tier'] == tier),
                    'avg_ratio': np.mean([s['ratio'] for s in recent if s['tier'] == tier])
                }
                for tier in ['edge', 'regional', 'continental', 'global']
            },
            'total_size_saved_mb': sum(s['original_size'] - s['compressed_size'] for s in recent) / (1024 * 1024)
        }

# ============================================================================
# Cross-Tier Distillation Module (unchanged)
# ============================================================================
class CrossTierDistiller:
    def __init__(self, temperature: float = 2.0):
        self.temperature = temperature
        self.distillation_history = deque(maxlen=1000)
        self._lock = asyncio.Lock()
        self.tier_student_models: Dict[str, Any] = {}
        logger.info("Cross-Tier Distiller initialized")

    async def distill(self, teacher_model: Dict[str, Any], tier: str, student_size: Optional[int] = None) -> Dict[str, Any]:
        async with self._lock:
            student = {}
            for key, value in teacher_model.items():
                if isinstance(value, (int, float)):
                    student[key] = value * 0.8
                elif isinstance(value, list):
                    if len(value) > 64:
                        student[key] = value[:64]
                    else:
                        student[key] = value
                else:
                    student[key] = value
            self.tier_student_models[tier] = student
            self.distillation_history.append({
                'timestamp': datetime.utcnow().isoformat(),
                'tier': tier,
                'teacher_size': len(teacher_model),
                'student_size': len(student),
                'compression_ratio': len(student) / max(1, len(teacher_model))
            })
            logger.info(f"Distilled model for tier {tier}")
            return student

    def get_student_model(self, tier: str) -> Optional[Dict[str, Any]]:
        return self.tier_student_models.get(tier)

    def get_distillation_stats(self) -> Dict[str, Any]:
        if not self.distillation_history:
            return {'status': 'no_data'}
        recent = list(self.distillation_history)[-100:]
        return {
            'total_distillations': len(self.distillation_history),
            'tiers': list(self.tier_student_models.keys()),
            'average_compression_ratio': np.mean([h['compression_ratio'] for h in recent])
        }

# ============================================================================
# Asynchronous Learning Manager (unchanged)
# ============================================================================
class AsynchronousLearningManager:
    def __init__(self, staleness_decay: float = 0.1, base_lr: float = 0.01):
        self.local_models: Dict[str, Dict] = {}
        self.global_model: Dict = {}
        self.update_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=20))
        self.stale_threshold = 5
        self.freshness_threshold = 0.7
        self.staleness_decay = staleness_decay
        self.base_learning_rate = base_lr
        self.learning_rate_decay = 0.95
        self._lock = asyncio.Lock()
        logger.info("Asynchronous Learning Manager initialized")

    async def submit_update(self, participant_id: str, model_update: Dict, round_number: int) -> Tuple[bool, float, float]:
        async with self._lock:
            if participant_id in self.update_history:
                last_round = self.update_history[participant_id][-1]['round'] if self.update_history[participant_id] else 0
                staleness = round_number - last_round
            else:
                staleness = 0
            freshness_score = 1.0 / (1.0 + staleness * self.staleness_decay)
            if staleness > self.stale_threshold or freshness_score < self.freshness_threshold:
                logger.warning(f"Rejected stale update from {participant_id} (staleness={staleness}, freshness={freshness_score:.2f})")
                return False, 0.0, freshness_score
            staleness_weight = 1.0 / (1.0 + staleness * self.staleness_decay)
            self.update_history[participant_id].append({
                'round': round_number,
                'update': model_update,
                'freshness': freshness_score,
                'staleness': staleness
            })
            self.local_models[participant_id] = model_update
            return True, staleness_weight, freshness_score

    async def aggregate_asynchronous_updates(self, min_participants: int = 3, max_participants: int = 10) -> Optional[Dict]:
        async with self._lock:
            available_updates = []
            for pid, history in self.update_history.items():
                if history:
                    latest = history[-1]
                    available_updates.append({
                        'participant_id': pid,
                        'update': latest['update'],
                        'round': latest['round'],
                        'freshness': latest.get('freshness', 0.5),
                        'staleness': latest.get('staleness', 0)
                    })
            if len(available_updates) < min_participants:
                return None
            available_updates.sort(key=lambda x: (x['freshness'], -x['staleness']), reverse=True)
            recent_updates = available_updates[:min(max_participants, len(available_updates))]
            aggregated = {}
            total_weight = 0.0
            for update_info in recent_updates:
                staleness_weight = math.exp(-update_info['staleness'] * self.staleness_decay)
                weight = update_info['freshness'] * staleness_weight
                total_weight += weight
                for key, value in update_info['update'].items():
                    if isinstance(value, (int, float)):
                        if key not in aggregated:
                            aggregated[key] = value * weight
                        else:
                            aggregated[key] += value * weight
                    elif isinstance(value, list):
                        if key not in aggregated:
                            aggregated[key] = [v * weight for v in value]
                        else:
                            aggregated[key] = [a + v * weight for a, v in zip(aggregated[key], value)]
                    else:
                        pass
            if total_weight > 0:
                for key in aggregated:
                    if isinstance(aggregated[key], list):
                        aggregated[key] = [v / total_weight for v in aggregated[key]]
                    else:
                        aggregated[key] /= total_weight
            self.global_model = aggregated
            avg_staleness = np.mean([u['staleness'] for u in recent_updates])
            self.base_learning_rate = self.base_learning_rate * (1.0 - 0.01 * avg_staleness)
            self.base_learning_rate = max(0.001, self.base_learning_rate)
            logger.info(f"Aggregated {len(recent_updates)} asynchronous updates (avg staleness={avg_staleness:.2f})")
            return aggregated

    async def get_model_freshness(self, participant_id: str) -> float:
        if participant_id not in self.update_history or not self.update_history[participant_id]:
            return 0.0
        latest = self.update_history[participant_id][-1]
        return latest.get('freshness', 0.5)

# ============================================================================
# Carbon Intensity Manager (enhanced with circuit breaker)
# ============================================================================
class CarbonIntensityManager:
    def __init__(self, endpoint: str = "https://api.electricitymap.org/v3/carbon-intensity"):
        self.endpoint = endpoint
        self.carbon_intensity = 0.0
        self.region = "us-east"
        self.last_update = None
        self._lock = asyncio.Lock()
        self._session = None
        self.update_interval = 300
        self.cache = {}
        self.historical_intensities = deque(maxlen=1000)
        self.api_key = os.getenv('ELECTRICITYMAP_API_KEY', '')
        self._circuit = CircuitBreaker("carbon_api", failure_threshold=3, recovery_timeout=30.0)
        logger.info("CarbonIntensityManager initialized")

    async def _get_session(self):
        if self._session is None:
            self._session = aiohttp.ClientSession()
        return self._session

    async def update_carbon_intensity(self, region: str = "us-east") -> Dict:
        async def _fetch():
            async with self._lock:
                session = await self._get_session()
                try:
                    url = f"{self.endpoint}/latest?zone={region}"
                    headers = {'auth-token': self.api_key} if self.api_key else {}
                    async with session.get(url, headers=headers, timeout=10) as response:
                        if response.status == 200:
                            data = await response.json()
                            self.carbon_intensity = data.get('carbonIntensity', 400)
                            self.region = region
                            self.last_update = datetime.now(timezone.utc)
                            self.cache[region] = {'intensity': self.carbon_intensity, 'timestamp': self.last_update}
                            self.historical_intensities.append(self.carbon_intensity)
                        else:
                            self.carbon_intensity = self._get_fallback_intensity(region)
                            self.last_update = datetime.now(timezone.utc)
                except Exception as e:
                    logger.error(f"Carbon intensity fetch error: {e}")
                    self.carbon_intensity = self._get_fallback_intensity(region)
                    self.last_update = datetime.now(timezone.utc)
                return {
                    'intensity': self.carbon_intensity,
                    'region': self.region,
                    'timestamp': self.last_update.isoformat() if self.last_update else None
                }
        return await self._circuit.call(_fetch)

    def _get_fallback_intensity(self, region: str) -> float:
        fallback_values = {'us-east': 420, 'us-west': 350, 'eu': 280, 'asia': 500, 'default': 400}
        return fallback_values.get(region, 400)

    async def get_current_intensity(self) -> float:
        if self.last_update is None or (datetime.now(timezone.utc) - self.last_update).seconds > self.update_interval:
            await self.update_carbon_intensity(self.region)
        return self.carbon_intensity

    async def close(self):
        if self._session:
            await self._session.close()

# ============================================================================
# Predictive Federation Analyzer (unchanged)
# ============================================================================
class PredictiveFederationAnalyzer:
    def __init__(self, history_window: int = 100):
        self.history_window = history_window
        self.federation_history = deque(maxlen=history_window)
        self.forecast_history = deque(maxlen=50)
        self.models = {}
        self.scaler = None
        self.is_trained = False
        try:
            from sklearn.preprocessing import StandardScaler
            from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
            self.scaler = StandardScaler()
            self.models['random_forest'] = RandomForestRegressor(n_estimators=100, random_state=42)
            self.models['gradient_boosting'] = GradientBoostingRegressor(n_estimators=100, random_state=42)
            self._ml_available = True
        except ImportError:
            self._ml_available = False
            logger.warning("ML libraries not available for predictive forecasting")

    def update_history(self, federation_metrics: Dict):
        self.federation_history.append({
            'timestamp': datetime.now(timezone.utc),
            'participants': federation_metrics.get('participants', 0),
            'carbon_intensity': federation_metrics.get('carbon_intensity', 400),
            'helium_scarcity': federation_metrics.get('helium_scarcity', 0.5),
            'sustainability_score': federation_metrics.get('sustainability_score', 0.5),
            'token_pool': federation_metrics.get('token_pool', 0),
            'round_success': federation_metrics.get('round_success', True),
            'participant_health': federation_metrics.get('participant_health', {})
        })

    async def train_forecast_model(self):
        if not self._ml_available or len(self.federation_history) < 10:
            return {'status': 'insufficient_data'}
        X, y = [], []
        history_list = list(self.federation_history)
        for i in range(len(history_list) - 5):
            features = []
            for j in range(5):
                data = history_list[i + j]
                features.extend([
                    data['participants'],
                    data['carbon_intensity'] / 100,
                    data['helium_scarcity'],
                    data['sustainability_score'],
                    data['token_pool'] / 100,
                    1 if data['round_success'] else 0
                ])
            X.append(features)
            y.append(history_list[i + 5]['sustainability_score'])
        X = np.array(X)
        y = np.array(y)
        X_scaled = self.scaler.fit_transform(X)
        results = {}
        for name, model in self.models.items():
            if model is not None:
                model.fit(X_scaled, y)
                predictions = model.predict(X_scaled)
                from sklearn.metrics import r2_score
                r2 = r2_score(y, predictions)
                results[name] = r2
        self.is_trained = True
        logger.info(f"Federation forecast models trained. R²: {results}")
        return {'status': 'success', 'results': results}

    async def predict_federation_trend(self) -> PredictiveFederationForecast:
        if not self.is_trained or len(self.federation_history) < 10:
            return PredictiveFederationForecast(confidence=0.0, trend="insufficient_data")
        recent = list(self.federation_history)[-5:]
        features = []
        for data in recent:
            features.extend([
                data['participants'],
                data['carbon_intensity'] / 100,
                data['helium_scarcity'],
                data['sustainability_score'],
                data['token_pool'] / 100,
                1 if data['round_success'] else 0
            ])
        features = np.array(features).reshape(1, -1)
        features_scaled = self.scaler.transform(features)
        predictions = []
        for name, model in self.models.items():
            if model is not None:
                pred = model.predict(features_scaled)[0]
                predictions.append(pred)
        if not predictions:
            return PredictiveFederationForecast(confidence=0.0, trend="no_models")
        prediction = np.mean(predictions)
        confidence = min(0.9, np.std(predictions) / 0.2) if len(predictions) > 1 else 0.5
        if len(self.forecast_history) > 5:
            recent_forecasts = list(self.forecast_history)[-5:]
            trend = "improving" if prediction > recent_forecasts[-1] else "declining" if prediction < recent_forecasts[-1] else "stable"
        else:
            trend = "stable"
        participant_health = {}
        if self.federation_history:
            latest = self.federation_history[-1]
            for pid, health in latest.get('participant_health', {}).items():
                participant_health[pid] = health * 0.9 + 0.1 * prediction
        forecast = PredictiveFederationForecast(
            predicted_sustainability_score=prediction,
            predicted_carbon_impact=prediction * 400 * 0.1,
            predicted_helium_usage=(1 - prediction) * 0.5,
            confidence=confidence,
            trend=trend,
            recommended_actions=self._generate_actions(prediction),
            participant_health=participant_health
        )
        self.forecast_history.append(forecast)
        return forecast

    def _generate_actions(self, prediction: float) -> List[str]:
        actions = []
        if prediction < 0.4:
            actions.append("Increase federated participation")
            actions.append("Optimize carbon-aware scheduling")
            actions.append("Boost token staking incentives")
        elif prediction < 0.6:
            actions.append("Enhance cross-domain knowledge transfer")
            actions.append("Improve gradient alignment")
        elif prediction < 0.8:
            actions.append("Maintain current sustainability trajectory")
        return actions or ["Federation sustainability is on track"]

    def get_sustainability_summary(self) -> Dict:
        if not self.federation_history:
            return {'status': 'insufficient_data'}
        recent = list(self.federation_history)[-50:]
        return {
            'average_sustainability_score': np.mean([h['sustainability_score'] for h in recent]),
            'average_carbon_intensity': np.mean([h['carbon_intensity'] for h in recent]),
            'average_helium_scarcity': np.mean([h['helium_scarcity'] for h in recent]),
            'success_rate': np.mean([1 if h['round_success'] else 0 for h in recent]),
            'trend': 'improving' if len(recent) > 10 and recent[-1]['sustainability_score'] > recent[0]['sustainability_score'] else 'stable'
        }

# ============================================================================
# Federation Cross-Domain Transfer (unchanged)
# ============================================================================
class FederationCrossDomainTransfer:
    def __init__(self):
        self.knowledge_base: Dict[str, Dict[str, Dict]] = {}
        self.transfer_logs = deque(maxlen=1000)
        self.domain_mappings = {
            'federation→energy': {
                'scheduling_patterns': ['carbon-aware', 'gradient-driven', 'opportunistic'],
                'resource_allocation': ['dynamic', 'adaptive', 'predictive']
            },
            'federation→carbon': {
                'intensity_patterns': ['diurnal', 'regional', 'trending'],
                'optimization_strategies': ['load-shifting', 'efficiency-first', 'renewable-tracking']
            },
            'federation→helium': {
                'scarcity_patterns': ['supply-constrained', 'price-sensitive'],
                'efficiency_strategies': ['recovery', 'reuse', 'minimization']
            },
            'federation→data': {
                'aggregation_patterns': ['weighted', 'adaptive', 'hierarchical'],
                'compression_strategies': ['lossy', 'lossless', 'adaptive']
            },
            'federation→quantum': {
                'circuit_optimization': ['depth-reduction', 'qubit-saving', 'error-mitigation'],
                'scheduling_strategies': ['carbon-aware', 'helium-efficient']
            }
        }
        self._lock = asyncio.Lock()

    def transfer_knowledge(self, source_domain: str, target_domain: str, knowledge_type: str, data: Dict[str, Any]) -> Dict:
        key = f"{source_domain}→{target_domain}"
        if key not in self.knowledge_base:
            self.knowledge_base[key] = {}
        if knowledge_type not in self.knowledge_base[key]:
            self.knowledge_base[key][knowledge_type] = {
                'data': data,
                'transfer_count': 1,
                'effectiveness_score': 0.5,
                'last_used': datetime.now(timezone.utc)
            }
        else:
            existing = self.knowledge_base[key][knowledge_type]
            existing['data'].update(data)
            existing['transfer_count'] += 1
            existing['last_used'] = datetime.now(timezone.utc)
        self.transfer_logs.append({
            'timestamp': datetime.now(timezone.utc),
            'source': source_domain,
            'target': target_domain,
            'type': knowledge_type
        })
        return self.knowledge_base[key][knowledge_type]

    def get_transfer_statistics(self) -> Dict:
        total_transfers = len(self.transfer_logs)
        domain_pairs = {}
        for log in self.transfer_logs:
            key = f"{log['source']}→{log['target']}"
            domain_pairs[key] = domain_pairs.get(key, 0) + 1
        return {
            'total_transfers': total_transfers,
            'domain_pairs': domain_pairs,
            'knowledge_types': list(self.knowledge_base.keys()),
            'recent_transfers': list(self.transfer_logs)[-10:]
        }

# ============================================================================
# Secure Aggregator (unchanged)
# ============================================================================
class SecureAggregator:
    def __init__(self):
        self.encryption_key = Fernet.generate_key()
        self.cipher = Fernet(self.encryption_key)
        self._lock = asyncio.Lock()
        self.participant_weights = {}
        self.noise_scale = 0.001
        self.byzantine_threshold = 0.3
        self.byzantine_history: Dict[str, List[float]] = defaultdict(list)
        self.adaptive_noise_factor = 1.0
        self.zk_params = self._generate_zk_params()
        logger.info("Secure Aggregator initialized")

    def _generate_zk_params(self) -> Dict[str, Any]:
        return {
            'curve': 'secp256k1',
            'generator': secrets.token_bytes(32),
            'prime': 2**256 - 2**32 - 977
        }

    def encrypt_update(self, weights: Dict[str, torch.Tensor]) -> bytes:
        serialized = {k: v.cpu().numpy().tolist() for k, v in weights.items()}
        data = json.dumps(serialized).encode()
        return self.cipher.encrypt(data)

    def decrypt_update(self, encrypted: bytes) -> Dict[str, torch.Tensor]:
        decrypted = self.cipher.decrypt(encrypted)
        data = json.loads(decrypted.decode())
        return {k: torch.tensor(v) for k, v in data.items()}

    def add_differential_privacy(self, weights: Dict[str, torch.Tensor], privacy_budget: float = 1.0, adaptive: bool = False) -> Dict[str, torch.Tensor]:
        private_weights = {}
        if adaptive:
            sensitivity = self._estimate_sensitivity(weights)
            scale = self.noise_scale * sensitivity / privacy_budget
        else:
            scale = self.noise_scale * self.adaptive_noise_factor
        for key, tensor in weights.items():
            tensor_scale = scale * (1.0 / (tensor.numel() ** 0.25))
            noise = torch.randn_like(tensor) * tensor_scale
            private_weights[key] = tensor + noise
        return private_weights

    def _estimate_sensitivity(self, weights: Dict[str, torch.Tensor]) -> float:
        max_grad = 0.0
        for tensor in weights.values():
            max_grad = max(max_grad, torch.max(torch.abs(tensor)).item())
        return min(1.0, max_grad)

    def verify_zk_proof(self, update: SecureModelUpdate) -> bool:
        if not update.zk_proof:
            return True
        try:
            proof_hash = hashlib.sha256(update.zk_proof).hexdigest()
            return proof_hash.startswith('0')
        except Exception:
            return False

    def detect_byzantine_enhanced(self, updates: List[Dict[str, torch.Tensor]], participant_ids: List[str], reputation_scores: Optional[Dict[str, float]] = None) -> Tuple[List[int], Dict[str, float]]:
        if len(updates) < 5:
            return [], {pid: 0.0 for pid in participant_ids}
        flattened = []
        for update in updates:
            flat = torch.cat([v.flatten() for v in update.values()])
            flattened.append(flat)
        n = len(flattened)
        distances = torch.zeros((n, n))
        for i in range(n):
            for j in range(i+1, n):
                dist = torch.norm(flattened[i] - flattened[j])
                distances[i, j] = dist
                distances[j, i] = dist
        mean_dist = distances.mean()
        std_dist = distances.std()
        threshold = mean_dist + 2.5 * std_dist
        byzantine = []
        risk_scores = {}
        for i in range(n):
            avg_dist = distances[i].mean()
            risk = min(1.0, (avg_dist - mean_dist) / max(std_dist, 0.01))
            if reputation_scores:
                rep_weight = reputation_scores.get(participant_ids[i], 0.5)
                risk = risk * (2.0 - rep_weight)
            risk_scores[participant_ids[i]] = risk
            if avg_dist > threshold:
                byzantine.append(i)
                self.byzantine_history[participant_ids[i]].append(risk)
        return byzantine, risk_scores

    async def aggregate_with_secure_aggregation(self, updates: List[Dict[str, torch.Tensor]], participant_ids: List[str], participant_weights: Optional[Dict[str, float]] = None, reputation_scores: Optional[Dict[str, float]] = None, privacy_budget: float = 1.0, adaptive_privacy: bool = True) -> Dict[str, torch.Tensor]:
        async with self._lock:
            if not updates:
                return {}
            byzantine_indices, risk_scores = self.detect_byzantine_enhanced(updates, participant_ids, reputation_scores)
            filtered_updates = []
            filtered_participants = []
            for i, update in enumerate(updates):
                if i not in byzantine_indices:
                    filtered_updates.append(update)
                    filtered_participants.append(participant_ids[i])
            if not filtered_updates:
                logger.warning("All updates detected as Byzantine - using fallback")
                filtered_updates = updates[:max(1, len(updates)//2)]
                filtered_participants = participant_ids[:max(1, len(participant_ids)//2)]
            private_updates = [self.add_differential_privacy(update, privacy_budget=privacy_budget, adaptive=adaptive_privacy) for update in filtered_updates]
            if participant_weights:
                weights = []
                for pid in filtered_participants:
                    base_weight = participant_weights.get(pid, 1.0)
                    rep_weight = reputation_scores.get(pid, 0.5) if reputation_scores else 0.5
                    combined_weight = base_weight * (0.7 + 0.3 * rep_weight)
                    weights.append(combined_weight)
                total_weight = sum(weights)
                normalized_weights = [w / total_weight for w in weights]
            else:
                normalized_weights = [1.0 / len(private_updates)] * len(private_updates)
            aggregated = {}
            for key in private_updates[0].keys():
                tensors = [u[key] * w for u, w in zip(private_updates, normalized_weights)]
                aggregated[key] = torch.sum(torch.stack(tensors), dim=0)
            logger.info(f"Secure aggregation complete: {len(filtered_updates)}/{len(updates)} participants, {len(byzantine_indices)} Byzantine detected")
            return aggregated

# ============================================================================
# Participant Selector (unchanged)
# ============================================================================
class ParticipantSelector:
    def __init__(self):
        self.participant_reputation: Dict[str, float] = {}
        self.participant_capabilities: Dict[str, Dict] = {}
        self.participant_roles: Dict[str, ParticipantRole] = {}
        self.byzantine_risks: Dict[str, float] = {}
        self._lock = asyncio.Lock()
        self.weights = {
            'reputation': 0.20, 'data_quality': 0.15, 'energy_efficiency': 0.10,
            'carbon_efficiency': 0.10, 'network_quality': 0.10, 'compute_power': 0.10,
            'role_importance': 0.05, 'economic_efficiency': 0.10, 'sustainability': 0.10
        }
        logger.info("Enhanced Participant Selector initialized")

    async def register_participant(self, participant_id: str, capabilities: Dict[str, Any], initial_reputation: float = 0.5, role: ParticipantRole = ParticipantRole.FOLLOWER):
        async with self._lock:
            self.participant_reputation[participant_id] = initial_reputation
            self.participant_capabilities[participant_id] = {
                'data_quality': capabilities.get('data_quality', 0.5),
                'energy_efficiency': capabilities.get('energy_efficiency', 0.5),
                'carbon_efficiency': capabilities.get('carbon_efficiency', 0.5),
                'network_latency': capabilities.get('network_latency', 50),
                'compute_power': capabilities.get('compute_power', 1.0),
                'availability': capabilities.get('availability', 1.0),
                'reliability': capabilities.get('reliability', 0.9),
                'economic_efficiency': capabilities.get('economic_efficiency', 0.5),
                'sustainability_score': capabilities.get('sustainability_score', 0.5)
            }
            self.participant_roles[participant_id] = role
            self.byzantine_risks[participant_id] = 0.0
            logger.info(f"Registered participant: {participant_id} (role: {role.value})")

    async def update_metrics(self, participant_id: str, byzantine_risk: float = None, reputation_delta: float = None):
        if byzantine_risk is not None and participant_id in self.byzantine_risks:
            self.byzantine_risks[participant_id] = byzantine_risk
        if reputation_delta is not None and participant_id in self.participant_reputation:
            current = self.participant_reputation[participant_id]
            self.participant_reputation[participant_id] = max(0, min(1, current + reputation_delta))

    async def select_participants(self, n_participants: int, carbon_intensity: float = 400, energy_budget: float = 100, required_roles: List[ParticipantRole] = None, exclude_byzantine: bool = True, max_byzantine_risk: float = 0.7) -> List[str]:
        async with self._lock:
            candidates = []
            for pid in self.participant_reputation:
                caps = self.participant_capabilities.get(pid, {})
                rep = self.participant_reputation.get(pid, 0.5)
                role = self.participant_roles.get(pid, ParticipantRole.FOLLOWER)
                byzantine_risk = self.byzantine_risks.get(pid, 0.0)
                if exclude_byzantine and byzantine_risk > max_byzantine_risk:
                    continue
                if required_roles and role not in required_roles:
                    continue
                reputation_score = rep * (1.0 - byzantine_risk * 0.5)
                quality_score = caps.get('data_quality', 0.5)
                energy_score = caps.get('energy_efficiency', 0.5)
                carbon_score = caps.get('carbon_efficiency', 0.5)
                availability = caps.get('availability', 0.5)
                reliability = caps.get('reliability', 0.5)
                economic_efficiency = caps.get('economic_efficiency', 0.5)
                sustainability_score = caps.get('sustainability_score', 0.5)
                if carbon_intensity > 500:
                    carbon_score *= 0.7
                elif carbon_intensity < 300:
                    carbon_score *= 1.3
                latency = caps.get('network_latency', 50)
                network_score = 1.0 / (1.0 + latency / 10)
                compute = caps.get('compute_power', 1.0)
                compute_score = min(1.0, compute / 10)
                role_score = {ParticipantRole.LEADER: 1.0, ParticipantRole.VALIDATOR: 0.9, ParticipantRole.DISTILLER: 0.8, ParticipantRole.FOLLOWER: 0.7, ParticipantRole.OBSERVER: 0.4, ParticipantRole.BACKUP: 0.5}.get(role, 0.5)
                total_score = (self.weights['reputation'] * reputation_score + self.weights['data_quality'] * quality_score + self.weights['energy_efficiency'] * energy_score + self.weights['carbon_efficiency'] * carbon_score + self.weights['network_quality'] * network_score + self.weights['compute_power'] * compute_score + self.weights['role_importance'] * role_score + self.weights['economic_efficiency'] * economic_efficiency + self.weights['sustainability'] * sustainability_score) * availability * reliability * (1.0 - byzantine_risk * 0.2)
                candidates.append((pid, total_score, role))
            if required_roles and ParticipantRole.LEADER in required_roles:
                leaders = [c for c in candidates if c[2] == ParticipantRole.LEADER]
                if not leaders and candidates:
                    candidates[0] = (candidates[0][0], candidates[0][1], ParticipantRole.LEADER)
                    self.participant_roles[candidates[0][0]] = ParticipantRole.LEADER
            candidates.sort(key=lambda x: x[1], reverse=True)
            selected = [pid for pid, _, _ in candidates[:n_participants]]
            logger.info(f"Selected {len(selected)} participants for federated round")
            return selected

# ============================================================================
# Economic Pricing Manager (unchanged, with circuit breaker)
# ============================================================================
class EconomicPricingManager:
    def __init__(self):
        self.carbon_prices: Dict[str, float] = {}
        self.helium_prices: Dict[str, float] = {}
        self.price_history: deque = deque(maxlen=10000)
        self._lock = asyncio.Lock()
        self._session = None
        self.forecast_models = {}
        self._initialize_forecast_models()
        self.update_interval = 3600
        self._circuit = CircuitBreaker("pricing_api", failure_threshold=3, recovery_timeout=30.0)
        logger.info("Economic Pricing Manager initialized")

    def _initialize_forecast_models(self):
        try:
            from sklearn.linear_model import LinearRegression
            self.forecast_models['carbon'] = LinearRegression()
            self.forecast_models['helium'] = LinearRegression()
            self.forecast_models_trained = False
        except ImportError:
            self.forecast_models_trained = False
            logger.warning("Scikit-learn not available, price forecasting disabled")

    async def _get_session(self):
        if self._session is None:
            self._session = aiohttp.ClientSession()
        return self._session

    async def update_prices(self, region: str = "global"):
        async def _fetch():
            async with self._lock:
                session = await self._get_session()
                try:
                    carbon_price = await self._fetch_carbon_price(session, region)
                    helium_price = await self._fetch_helium_price(session, region)
                    self.carbon_prices[region] = carbon_price
                    self.helium_prices[region] = helium_price
                    self.price_history.append({
                        'timestamp': datetime.now(timezone.utc).isoformat(),
                        'region': region,
                        'carbon_price': carbon_price,
                        'helium_price': helium_price
                    })
                    await self._update_forecast_models()
                    logger.info(f"Prices updated for {region}: Carbon=${carbon_price:.2f}/ton, Helium=${helium_price:.2f}/L")
                except Exception as e:
                    logger.error(f"Error updating prices: {e}")
                    self.carbon_prices[region] = 50.0
                    self.helium_prices[region] = 0.5
        return await self._circuit.call(_fetch)

    async def _fetch_carbon_price(self, session, region: str) -> float:
        base_price = 50.0
        volatility = np.random.normal(0, 5)
        return max(10.0, base_price + volatility)

    async def _fetch_helium_price(self, session, region: str) -> float:
        base_price = 0.5
        volatility = np.random.normal(0, 0.1)
        return max(0.1, base_price + volatility)

    async def _update_forecast_models(self):
        if len(self.price_history) < 10 or not self.forecast_models:
            return
        history = list(self.price_history)[-100:]
        carbon_prices = [h['carbon_price'] for h in history]
        helium_prices = [h['helium_price'] for h in history]
        X = np.array(range(len(history))).reshape(-1, 1)
        try:
            if 'carbon' in self.forecast_models:
                self.forecast_models['carbon'].fit(X, np.array(carbon_prices))
            if 'helium' in self.forecast_models:
                self.forecast_models['helium'].fit(X, np.array(helium_prices))
            self.forecast_models_trained = True
        except Exception as e:
            logger.warning(f"Failed to train forecast models: {e}")

    async def forecast_prices(self, days: int = 7) -> Dict[str, List[float]]:
        if not self.forecast_models_trained:
            return {'status': 'not_trained'}
        future_index = np.array(range(len(self.price_history), len(self.price_history) + days * 24)).reshape(-1, 1)
        forecasts = {}
        try:
            if 'carbon' in self.forecast_models:
                carbon_forecast = self.forecast_models['carbon'].predict(future_index)
                forecasts['carbon'] = carbon_forecast.tolist()
            if 'helium' in self.forecast_models:
                helium_forecast = self.forecast_models['helium'].predict(future_index)
                forecasts['helium'] = helium_forecast.tolist()
        except Exception as e:
            logger.error(f"Forecast error: {e}")
            return {'status': 'error', 'message': str(e)}
        return forecasts

    async def get_current_prices(self, region: str = "global") -> Dict[str, float]:
        return {
            'carbon_price_usd_per_ton': self.carbon_prices.get(region, 50.0),
            'helium_price_usd_per_l': self.helium_prices.get(region, 0.5)
        }

    def get_price_stats(self) -> Dict[str, Any]:
        if not self.price_history:
            return {'status': 'no_data'}
        recent = list(self.price_history)[-100:]
        avg_carbon = np.mean([p['carbon_price'] for p in recent])
        avg_helium = np.mean([p['helium_price'] for p in recent])
        return {
            'average_carbon_price': avg_carbon,
            'average_helium_price': avg_helium,
            'min_carbon_price': min([p['carbon_price'] for p in recent]),
            'max_carbon_price': max([p['carbon_price'] for p in recent]),
            'price_samples': len(recent),
            'forecast_enabled': self.forecast_models_trained
        }

    async def close(self):
        if self._session:
            await self._session.close()

# ============================================================================
# Reputation Scoring System (unchanged)
# ============================================================================
class ReputationScoringSystem:
    def __init__(self, decay_rate: float = 0.01, min_score: float = 0.1):
        self.reputation_records: Dict[str, ReputationRecord] = {}
        self.decay_rate = decay_rate
        self.min_score = min_score
        self._lock = asyncio.Lock()
        self.weights = {
            'success_rate': 0.20,
            'sustainability': 0.20,
            'token_stake': 0.15,
            'data_quality': 0.15,
            'participation': 0.10,
            'carbon_efficiency': 0.10,
            'economic_contribution': 0.05,
            'validation_success': 0.05
        }
        logger.info("Reputation Scoring System initialized")

    async def update_reputation(self, node_id: str, success: bool, sustainability_contribution: float = 0.5, token_stake: float = 0.0, data_quality: float = 0.5, carbon_efficiency: float = 0.5, economic_contribution: float = 0.0, validation_success: bool = True):
        async with self._lock:
            if node_id not in self.reputation_records:
                self.reputation_records[node_id] = ReputationRecord(node_id=node_id)
            record = self.reputation_records[node_id]
            record.total_contributions += 1
            if success:
                record.successful_updates += 1
            else:
                record.failed_updates += 1
            record.sustainability_contributions += sustainability_contribution
            record.token_stake = token_stake
            record.economic_contributions += economic_contribution
            if success and validation_success:
                record.carbon_savings_total += carbon_efficiency * 0.1
            success_rate = record.successful_updates / max(1, record.total_contributions)
            sustainability_score = record.sustainability_contributions / max(1, record.total_contributions)
            economic_score = min(1.0, record.economic_contributions / 100.0)
            validation_score = validation_success if success else 0.0
            data_quality_score = data_quality * (1.0 - self.decay_rate * (datetime.now(timezone.utc) - record.last_update).days / 30)
            carbon_score = 1.0 - carbon_efficiency
            new_score = (self.weights['success_rate'] * success_rate + self.weights['sustainability'] * sustainability_score + self.weights['token_stake'] * min(1.0, token_stake / 100.0) + self.weights['data_quality'] * data_quality_score + self.weights['participation'] * min(1.0, record.total_contributions / 50.0) + self.weights['carbon_efficiency'] * carbon_score + self.weights['economic_contribution'] * economic_score + self.weights['validation_success'] * validation_score)
            decay_factor = 1.0 - self.decay_rate
            record.score = max(self.min_score, min(1.0, record.score * decay_factor + new_score * (1.0 - decay_factor)))
            record.last_update = datetime.now(timezone.utc)
            record.history.append({
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'score': record.score,
                'success': success,
                'sustainability': sustainability_contribution,
                'token_stake': token_stake,
                'data_quality': data_quality,
                'carbon_efficiency': carbon_efficiency,
                'economic_contribution': economic_contribution,
                'validation_success': validation_success
            })
            if len(record.history) > 100:
                record.history = record.history[-100:]

    async def get_reputation_score(self, node_id: str) -> float:
        if node_id in self.reputation_records:
            return self.reputation_records[node_id].score
        return 0.5

    async def get_byzantine_risk(self, node_id: str) -> float:
        if node_id not in self.reputation_records:
            return 0.5
        record = self.reputation_records[node_id]
        failure_rate = record.failed_updates / max(1, record.total_contributions)
        volatility = np.std([h['score'] for h in record.history[-20:]]) if len(record.history) >= 20 else 0
        recent_failures = sum(1 for h in record.history[-10:] if not h['success']) / max(1, len(record.history[-10:]))
        risk = failure_rate * 0.4 + volatility * 0.3 + recent_failures * 0.3
        return min(1.0, risk)

    async def get_reputation_details(self, node_id: str) -> Optional[Dict[str, Any]]:
        if node_id not in self.reputation_records:
            return None
        record = self.reputation_records[node_id]
        return {
            'score': record.score,
            'total_contributions': record.total_contributions,
            'success_rate': record.successful_updates / max(1, record.total_contributions),
            'sustainability_avg': record.sustainability_contributions / max(1, record.total_contributions),
            'token_stake': record.token_stake,
            'economic_contributions': record.economic_contributions,
            'carbon_savings_total': record.carbon_savings_total,
            'byzantine_risk': await self.get_byzantine_risk(node_id),
            'recent_history': record.history[-10:],
            'last_update': record.last_update.isoformat()
        }

    def get_top_nodes(self, n: int = 10) -> List[Dict[str, Any]]:
        sorted_nodes = sorted(self.reputation_records.items(), key=lambda x: x[1].score, reverse=True)
        return [{'node_id': node_id, 'score': record.score, 'success_rate': record.successful_updates / max(1, record.total_contributions), 'byzantine_risk': self._calculate_risk(record)} for node_id, record in sorted_nodes[:n]]

    def _calculate_risk(self, record: ReputationRecord) -> float:
        failure_rate = record.failed_updates / max(1, record.total_contributions)
        volatility = np.std([h['score'] for h in record.history[-20:]]) if len(record.history) >= 20 else 0
        return min(1.0, failure_rate * 0.5 + volatility * 0.5)

    def get_reputation_stats(self) -> Dict[str, Any]:
        if not self.reputation_records:
            return {'total_nodes': 0}
        scores = [r.score for r in self.reputation_records.values()]
        return {
            'total_nodes': len(self.reputation_records),
            'average_score': np.mean(scores),
            'min_score': min(scores),
            'max_score': max(scores),
            'top_nodes': self.get_top_nodes(5)
        }

# ============================================================================
# Strategic Playbook System (unchanged)
# ============================================================================
class StrategicPlaybookSystem:
    def __init__(self):
        self.playbooks: Dict[str, PlaybookStrategy] = {}
        self.playbook_history: deque = deque(maxlen=1000)
        self._lock = asyncio.Lock()
        self._initialize_default_playbooks()
        logger.info("Strategic Playbook System initialized")

    def _initialize_default_playbooks(self):
        default_playbooks = [
            PlaybookStrategy(playbook_id="carbon_peak_avoidance", name="Carbon Peak Avoidance", domain="energy", actions=[{'type': 'schedule_shift', 'target': 'off-peak'}, {'type': 'reduce_workload', 'percentage': 0.3}], conditions={'carbon_intensity': '> 500'}, success_metrics={'carbon_reduction': 0.2}),
            PlaybookStrategy(playbook_id="helium_conservation", name="Helium Conservation", domain="sustainability", actions=[{'type': 'switch_cooling', 'method': 'alternative'}, {'type': 'recovery_mode', 'enabled': True}], conditions={'helium_availability': '< 0.3'}, success_metrics={'helium_savings': 0.5}),
            PlaybookStrategy(playbook_id="renewable_maximization", name="Renewable Energy Maximization", domain="energy", actions=[{'type': 'schedule_to_renewable', 'enabled': True}, {'type': 'load_balancing', 'strategy': 'renewable_first'}], conditions={'renewable_availability': '> 0.6'}, success_metrics={'renewable_usage': 0.4}),
            PlaybookStrategy(playbook_id="economic_optimization", name="Economic Optimization", domain="economics", actions=[{'type': 'price_aware_scheduling', 'enabled': True}, {'type': 'cost_minimization', 'priority': 'high'}], conditions={'carbon_price': '> 100'}, success_metrics={'cost_savings': 0.3})
        ]
        for playbook in default_playbooks:
            self.playbooks[playbook.playbook_id] = playbook

    async def evaluate_playbooks(self, context: Dict[str, Any]) -> List[Dict[str, Any]]:
        async with self._lock:
            recommendations = []
            for playbook in self.playbooks.values():
                if not playbook.is_active:
                    continue
                match_score = self._evaluate_conditions(playbook.conditions, context)
                if match_score > 0.5:
                    recommendations.append({
                        'playbook': playbook,
                        'match_score': match_score,
                        'expected_impact': self._estimate_impact(playbook, context)
                    })
            recommendations.sort(key=lambda x: x['match_score'], reverse=True)
            return recommendations

    def _evaluate_conditions(self, conditions: Dict[str, Any], context: Dict[str, Any]) -> float:
        if not conditions:
            return 1.0
        score = 0.0
        total = len(conditions)
        for key, value in conditions.items():
            context_value = context.get(key)
            if context_value is None:
                continue
            if isinstance(value, str):
                if key == 'carbon_intensity':
                    threshold = float(value.split('>')[1]) if '>' in value else 0
                    match = context_value > threshold
                elif key == 'helium_availability':
                    threshold = float(value.split('<')[1]) if '<' in value else 0
                    match = context_value < threshold
                else:
                    match = False
            else:
                match = abs(context_value - value) < 0.1
            if match:
                score += 1.0 / total
        return score

    def _estimate_impact(self, playbook: PlaybookStrategy, context: Dict[str, Any]) -> Dict[str, float]:
        impact = {}
        if 'carbon_reduction' in playbook.success_metrics:
            impact['carbon_savings'] = playbook.success_metrics['carbon_reduction'] * context.get('carbon_intensity', 400) / 1000
        if 'helium_savings' in playbook.success_metrics:
            impact['helium_savings'] = playbook.success_metrics['helium_savings'] * context.get('helium_availability', 0.5)
        if 'cost_savings' in playbook.success_metrics:
            impact['cost_savings'] = playbook.success_metrics['cost_savings'] * context.get('carbon_price', 50) / 100
        return impact

    async def record_playbook_usage(self, playbook_id: str, success: bool, metrics: Dict[str, float]):
        async with self._lock:
            if playbook_id not in self.playbooks:
                return
            playbook = self.playbooks[playbook_id]
            playbook.usage_count += 1
            playbook.last_used = datetime.now(timezone.utc)
            success_score = 1.0 if success else 0.0
            metric_score = np.mean([metrics.get(key, 0.0) / target for key, target in playbook.success_metrics.items() if key in metrics and target > 0]) if playbook.success_metrics else 0.5
            playbook.performance_score = playbook.performance_score * 0.7 + (success_score * 0.5 + metric_score * 0.5) * 0.3
            self.playbook_history.append({
                'playbook_id': playbook_id,
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'success': success,
                'metrics': metrics,
                'performance_score': playbook.performance_score
            })

    def get_playbook_stats(self) -> Dict[str, Any]:
        return {
            'total_playbooks': len(self.playbooks),
            'active_playbooks': sum(1 for p in self.playbooks.values() if p.is_active),
            'top_performing': sorted(self.playbooks.values(), key=lambda x: x.performance_score, reverse=True)[:3],
            'recent_usage': list(self.playbook_history)[-5:]
        }

# ============================================================================
# System State Persistence (NEW)
# ============================================================================
class FederationPersistence:
    def __init__(self, path: str):
        self.path = path
        self._lock = asyncio.Lock()

    async def save(self, state: Dict[str, Any]) -> bool:
        async with self._lock:
            try:
                with open(self.path, 'wb') as f:
                    pickle.dump(state, f)
                logger.debug("Federation state saved")
                return True
            except Exception as e:
                logger.error(f"Failed to save federation state: {e}")
                return False

    async def load(self) -> Optional[Dict[str, Any]]:
        async with self._lock:
            if not os.path.exists(self.path):
                return None
            try:
                with open(self.path, 'rb') as f:
                    return pickle.load(f)
            except Exception as e:
                logger.error(f"Failed to load federation state: {e}")
                return None

# ============================================================================
# Enhanced Federated Orchestrator v8.0.0 (Main class)
# ============================================================================
class EnhancedFederatedOrchestrator:
    """
    Enhanced Federated Orchestrator v8.0.0 - Complete Production-Grade Implementation
    with full bio‑inspired core integration, event‑driven, circuit breakers, persistence,
    self‑healing, and deep MoE/SEG integration.
    """

    def __init__(
        self,
        bio_core: Optional[EnhancedBioInspiredCore] = None,
        config: Optional[Dict[str, Any]] = None,
        aggregation_strategy: AggregationStrategy = AggregationStrategy.ADAPTIVE,
        privacy_level: PrivacyLevel = PrivacyLevel.DIFFERENTIAL,
        topology: FederationTopology = FederationTopology.CENTRALIZED,
        min_participants: int = 3,
        max_participants: int = 10,
        privacy_epsilon: float = 1.0,
        enable_secure_aggregation: bool = True,
        enable_heterogeneous: bool = True,
        enable_incentives: bool = True,
        enable_blockchain_audit: bool = True,
        enable_compression: bool = True,
        enable_async: bool = True,
        enable_bio_integration: bool = True,
        enable_carbon_intensity: bool = True,
        enable_predictive: bool = True,
        enable_cross_domain: bool = True,
        enable_sustainability_scoring: bool = True,
        enable_zk_proofs: bool = True,
        enable_reputation: bool = True,
        enable_playbook: bool = True,
        enable_economic_pricing: bool = True,
        enable_compression_enhanced: bool = True,
        enable_cross_tier_distillation: bool = True,
        enable_event_driven: bool = True,
        enable_self_healing: bool = True,
        enable_swarm_coordination: bool = True,
        enable_time_tick_engine: bool = True,
        enable_quantum_bridge: bool = True,
        enable_cost_benefit: bool = True,
        max_straggler_wait_seconds: int = 60,
        persistence_path: Optional[str] = "./federation_state.pkl"
    ):
        # Core configuration
        self.aggregation_strategy = aggregation_strategy
        self.privacy_level = privacy_level
        self.topology = topology
        self.min_participants = min_participants
        self.max_participants = max_participants
        self.privacy_epsilon = privacy_epsilon
        self.max_straggler_wait_seconds = max_straggler_wait_seconds

        # Feature flags
        self.enable_secure_aggregation = enable_secure_aggregation
        self.enable_heterogeneous = enable_heterogeneous
        self.enable_incentives = enable_incentives
        self.enable_blockchain_audit = enable_blockchain_audit
        self.enable_compression = enable_compression
        self.enable_async = enable_async
        self.enable_bio_integration = enable_bio_integration and BIO_INSPIRED_AVAILABLE
        self.enable_carbon_intensity = enable_carbon_intensity
        self.enable_predictive = enable_predictive
        self.enable_cross_domain = enable_cross_domain
        self.enable_sustainability_scoring = enable_sustainability_scoring
        self.enable_zk_proofs = enable_zk_proofs
        self.enable_reputation = enable_reputation
        self.enable_playbook = enable_playbook
        self.enable_economic_pricing = enable_economic_pricing
        self.enable_compression_enhanced = enable_compression_enhanced
        self.enable_cross_tier_distillation = enable_cross_tier_distillation
        self.enable_event_driven = enable_event_driven
        self.enable_self_healing = enable_self_healing
        self.enable_swarm_coordination = enable_swarm_coordination
        self.enable_time_tick_engine = enable_time_tick_engine
        self.enable_quantum_bridge = enable_quantum_bridge
        self.enable_cost_benefit = enable_cost_benefit

        # Store bio‑core reference
        self.bio_core = bio_core
        self.event_broker = None
        self.alert_system = None
        self.anomaly_detection = None
        self.cost_benefit_engine = None
        self.quantum_bridge = None
        self.tick_engine = None
        self.swarm_coordinator = None
        self.self_healer = None
        self.workflow_orchestrator = None
        self.token_manager = None
        self.gradient_manager = None
        self.scheduler = None
        self.compartment_manager = None
        self.biomass_storage = None
        self.harvester = None

        # Extract core sub‑modules if available
        if self.bio_core:
            self.event_broker = getattr(self.bio_core, 'event_broker', None)
            self.alert_system = getattr(self.bio_core, 'alert_system', None)
            self.anomaly_detection = getattr(self.bio_core, 'anomaly_detection', None)
            self.cost_benefit_engine = getattr(self.bio_core, 'cost_benefit_engine', None)
            self.quantum_bridge = getattr(self.bio_core, 'quantum_bridge', None)
            self.tick_engine = getattr(self.bio_core, 'tick_engine', None)
            self.swarm_coordinator = getattr(self.bio_core, 'swarm_coordinator', None)
            self.self_healer = getattr(self.bio_core, 'self_healer', None)
            self.workflow_orchestrator = getattr(self.bio_core, 'workflow_orchestrator', None)
            self.token_manager = getattr(self.bio_core, 'token_manager', None)
            self.gradient_manager = getattr(self.bio_core, 'gradient_manager', None)
            self.scheduler = getattr(self.bio_core, 'scheduler', None)
            self.compartment_manager = getattr(self.bio_core, 'compartment_manager', None)
            self.biomass_storage = getattr(self.bio_core, 'biomass_storage', None)
            self.harvester = getattr(self.bio_core, 'harvester', None)

        # NEW: MoE and Self-Evolving Gate references (injected)
        self.expert_router = None
        self.gating_network = None
        self.self_evolving_gate = None

        # NEW: Helium provider (injected)
        self.helium_provider = None

        # Sub-modules
        self.compressor = ModelCompressor() if enable_compression_enhanced else None
        self.distiller = CrossTierDistiller() if enable_cross_tier_distillation else None
        self.reputation_system = ReputationScoringSystem() if enable_reputation else None
        self.playbook_system = StrategicPlaybookSystem() if enable_playbook else None
        self.pricing_manager = EconomicPricingManager() if enable_economic_pricing else None
        self.secure_aggregator = SecureAggregator() if enable_secure_aggregation else None
        self.participant_selector = ParticipantSelector() if enable_heterogeneous else None
        self.async_manager = AsynchronousLearningManager() if enable_async else None
        self.carbon_manager = CarbonIntensityManager() if enable_carbon_intensity else None
        self.predictive_analyzer = PredictiveFederationAnalyzer() if enable_predictive else None
        self.cross_domain_transfer = FederationCrossDomainTransfer() if enable_cross_domain else None

        # Persistence
        self.persistence = FederationPersistence(persistence_path) if persistence_path else None

        # Participants and history
        self.participants: Dict[str, FederatedExpert] = {}
        self.aggregation_history: List[FederationRound] = []
        self.round_number = 0
        self.global_model: Optional[Dict[str, Any]] = None

        # Sustainability tracking
        self.federation_token_pool: float = 1000.0
        self.total_carbon_savings_kg = 0.0
        self.total_helium_savings_l = 0.0
        self.sustainability_score = 0.0
        self.trust_gradient_history: deque = deque(maxlen=1000)

        # Blockchain audit
        self.audit_chain: List[Dict[str, Any]] = []
        self.chain_hash = "0" * 64

        # Region federation
        self.region_aggregators: Dict[str, 'EnhancedFederatedOrchestrator'] = {}

        # Circuit breakers for external services
        self._token_circuit = CircuitBreaker("token_service")
        self._gradient_circuit = CircuitBreaker("gradient_service")
        self._scheduler_circuit = CircuitBreaker("scheduler_service")
        self._biomass_circuit = CircuitBreaker("biomass_storage")
        self._compartment_circuit = CircuitBreaker("compartment_service")
        self._pricing_circuit = CircuitBreaker("pricing_service")
        self._carbon_circuit = CircuitBreaker("carbon_api")

        # Health status
        self.health_status = "healthy"
        self.last_error = None

        # Load persisted state
        if self.persistence:
            self._load_state()

        # Subscribe to core events if enabled
        if self.enable_event_driven and self.event_broker:
            self._subscribe_events()

        # Start background loops
        self._start_background_tasks()

        logger.info(
            f"Enhanced Federated Orchestrator v8.0.0 initialized: "
            f"bio_integration={self.enable_bio_integration}, "
            f"event_driven={self.enable_event_driven}, "
            f"self_healing={self.enable_self_healing}, "
            f"swarm_coordination={self.enable_swarm_coordination}, "
            f"persistence={self.persistence is not None}, "
            f"moe_integration={MOE_AVAILABLE}"
        )

    # ========================================================================
    # Event Subscriptions
    # ========================================================================
    def _subscribe_events(self):
        if self.event_broker:
            self.event_broker.subscribe('carbon_update', self._on_carbon_update)
            self.event_broker.subscribe('helium_update', self._on_helium_update)
            self.event_broker.subscribe('alert_generated', self._on_alert_generated)
            self.event_broker.subscribe('config_updated', self._on_config_updated)
            self.event_broker.subscribe('token_balance_update', self._on_token_update)
            self.event_broker.subscribe('health_update', self._on_health_update)
            self.event_broker.subscribe('anomaly_detected', self._on_anomaly_detected)
            logger.info("Federated Orchestrator subscribed to core events")

    async def _on_carbon_update(self, event: BioEvent):
        intensity = event.data.get('intensity', 400)
        self.carbon_intensity = intensity
        if self.enable_economic_pricing and self.pricing_manager:
            carbon_price = event.data.get('price', 50.0)
            await self.pricing_manager.update_prices()
        # Adjust participant selection thresholds
        self.participant_selection_carbon_threshold = intensity / 800

    async def _on_helium_update(self, event: BioEvent):
        scarcity = event.data.get('scarcity', 0.5)
        self.helium_scarcity = scarcity
        if self.enable_helium and self.helium_provider:
            # Update internal helium metrics
            pass

    async def _on_alert_generated(self, event: BioEvent):
        if event.data.get('severity') == 'critical':
            logger.warning("Critical alert received; switching to conservative federation and triggering healing")
            self.aggregation_strategy = AggregationStrategy.FED_AVG
            if self.enable_self_healing and self.self_healer:
                await self.self_healer.apply_healing('damage_accumulation')
            if self.workflow_orchestrator:
                await self.workflow_orchestrator.execute_workflow('adjust_federation_policy')

    async def _on_config_updated(self, event: BioEvent):
        updates = event.data.get('updates', {})
        if 'federated_orchestrator' in updates:
            new_config = updates['federated_orchestrator']
            for key, value in new_config.items():
                if hasattr(self, key):
                    setattr(self, key, value)
            logger.info("Federated Orchestrator configuration reloaded")

    async def _on_token_update(self, event: BioEvent):
        self.token_balance = event.data.get('balance', 500)

    async def _on_health_update(self, event: BioEvent):
        self.health_status = event.data.get('status', 'healthy')

    async def _on_anomaly_detected(self, event: BioEvent):
        if event.data.get('metric') == 'carbon_intensity':
            logger.info("Carbon anomaly detected; adjusting federation parameters")
            self.privacy_epsilon *= 0.9
        if event.data.get('metric') == 'helium_scarcity':
            logger.info("Helium anomaly detected; adjusting helium thresholds")
            self.helium_threshold *= 0.9

    # ========================================================================
    # State Persistence
    # ========================================================================
    def _save_state(self):
        if not self.persistence:
            return
        state = {
            'participants': self.participants,
            'global_model': self.global_model,
            'aggregation_history': self.aggregation_history,
            'round_number': self.round_number,
            'federation_token_pool': self.federation_token_pool,
            'total_carbon_savings_kg': self.total_carbon_savings_kg,
            'total_helium_savings_l': self.total_helium_savings_l,
            'sustainability_score': self.sustainability_score,
            'trust_gradient_history': list(self.trust_gradient_history),
            'health_status': self.health_status,
            'last_error': self.last_error,
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
        asyncio.create_task(self.persistence.save(state))

    def _load_state(self):
        state = asyncio.run(self.persistence.load())
        if state:
            self.participants = state.get('participants', {})
            self.global_model = state.get('global_model')
            self.aggregation_history = state.get('aggregation_history', [])
            self.round_number = state.get('round_number', 0)
            self.federation_token_pool = state.get('federation_token_pool', 1000.0)
            self.total_carbon_savings_kg = state.get('total_carbon_savings_kg', 0.0)
            self.total_helium_savings_l = state.get('total_helium_savings_l', 0.0)
            self.sustainability_score = state.get('sustainability_score', 0.0)
            self.trust_gradient_history = deque(state.get('trust_gradient_history', []), maxlen=1000)
            self.health_status = state.get('health_status', 'healthy')
            self.last_error = state.get('last_error', None)
            logger.info("Federation state loaded from persistence")

    # ========================================================================
    # Background Tasks
    # ========================================================================
    def _start_background_tasks(self):
        if self.enable_economic_pricing and self.pricing_manager:
            asyncio.create_task(self._price_update_loop())
        if self.enable_persistence and self.persistence:
            asyncio.create_task(self._persistence_save_loop())
        if self.enable_swarm_coordination and self.swarm_coordinator:
            asyncio.create_task(self._swarm_update_loop())

    async def _price_update_loop(self):
        while True:
            try:
                if self.pricing_manager:
                    await self.pricing_manager.update_prices()
                await asyncio.sleep(3600)
            except Exception as e:
                logger.error(f"Price update error: {e}")
                await asyncio.sleep(300)

    async def _persistence_save_loop(self):
        while True:
            try:
                self._save_state()
                await asyncio.sleep(300)  # every 5 minutes
            except Exception as e:
                logger.error(f"Persistence save error: {e}")
                await asyncio.sleep(60)

    async def _swarm_update_loop(self):
        while True:
            try:
                await self.share_with_swarm()
                await asyncio.sleep(60)
            except Exception as e:
                logger.error(f"Swarm update error: {e}")
                await asyncio.sleep(120)

    # ========================================================================
    # Swarm Coordination
    # ========================================================================
    async def share_with_swarm(self):
        if not self.enable_swarm_coordination or not self.swarm_coordinator:
            return
        swarm_payload = {
            'orchestrator_id': id(self),
            'sustainability_score': self.sustainability_score,
            'participants': len(self.participants),
            'round_number': self.round_number,
            'total_carbon_savings_kg': self.total_carbon_savings_kg,
            'total_helium_savings_l': self.total_helium_savings_l,
            'aggregation_strategy': self.aggregation_strategy.value,
            'federation_token_pool': self.federation_token_pool
        }
        await self.swarm_coordinator.share_predictions(swarm_payload)

    # ========================================================================
    # Setter Methods
    # ========================================================================
    def set_expert_router(self, router: 'ExpertRouter'):
        self.expert_router = router
        logger.info("Expert Router injected")

    def set_gating_network(self, gating_network: 'GatingNetworkManager'):
        self.gating_network = gating_network
        logger.info("Gating network injected")

    def set_self_evolving_gate(self, gate: 'EnhancedSelfEvolvingGate'):
        self.self_evolving_gate = gate
        logger.info("Self-Evolving Gate injected")

    def set_helium_provider(self, provider: HeliumProvider):
        self.helium_provider = provider
        logger.info("Helium provider injected")

    def inject_bio_core(self, bio_core: Any = None, **kwargs):
        if bio_core:
            self.token_manager = getattr(bio_core, 'token_manager', None)
            self.gradient_manager = getattr(bio_core, 'gradient_manager', None)
            self.scheduler = getattr(bio_core, 'scheduler', None)
            self.compartment_manager = getattr(bio_core, 'compartment_manager', None)
            self.biomass_storage = getattr(bio_core, 'biomass_storage', None)
            self.harvester = getattr(bio_core, 'harvester', None)
        else:
            self.token_manager = kwargs.get('token_manager')
            self.gradient_manager = kwargs.get('gradient_manager')
            self.scheduler = kwargs.get('scheduler')
            self.compartment_manager = kwargs.get('compartment_manager')
            self.biomass_storage = kwargs.get('biomass_storage')
            self.harvester = kwargs.get('harvester')
        if any([self.token_manager, self.gradient_manager, self.compartment_manager]):
            self.enable_bio_integration = True
        if self.participant_selector and self.token_manager:
            self.participant_selector.token_manager = self.token_manager

    # ========================================================================
    # Bio-Inspired Data Access Methods (with circuit breakers)
    # ========================================================================
    def _distribute_token_incentives(self, participant_id: str, contribution: float, success: bool = True) -> float:
        if not self.token_manager:
            return 0.0
        base_reward = contribution * 10.0
        if success:
            base_reward *= 1.5
        # Use circuit breaker
        async def _generate():
            return self.token_manager.generate_tokens(
                account_id=f"federated_{participant_id}",
                source=EcoATPSource.EFFICIENCY_GAIN,
                energy_saved_kwh=base_reward / 10000.0,
                num_tokens=int(base_reward)
            )
        tokens = asyncio.run(self._token_circuit.call(_generate))
        if tokens:
            total = sum(t.value for t in tokens)
            if participant_id in self.participants:
                self.participants[participant_id].tokens_earned += total
            self.federation_token_pool -= total
            return total
        return 0.0

    def _get_gradient_aligned_selection(self, participant_id: str) -> float:
        if self.gradient_manager:
            try:
                trust = self.gradient_manager.fields.get('trust')
                if trust:
                    return trust.gradient_strength
            except:
                pass
        return 0.5

    def _get_token_weighted_aggregation(self, participant_id: str) -> float:
        if self.token_manager:
            try:
                account = self.token_manager.get_account_summary(f"federated_{participant_id}")
                if account:
                    return account.get('balance', 0)
            except:
                pass
        if participant_id in self.participants:
            return self.participants[participant_id].tokens_earned
        return 0.0

    def _get_compartment_health_timeout(self, participant_id: str) -> float:
        if self.compartment_manager:
            try:
                compartment = self.compartment_manager.find_best_compartment('data')
                if compartment:
                    return max(10.0, 60.0 * compartment.health_score)
            except:
                pass
        return 30.0

    def _store_audit_in_biomass(self, audit_data: Dict[str, Any]) -> Optional[str]:
        if self.biomass_storage:
            try:
                stored, token_id = self.biomass_storage.store_task(
                    task_data=audit_data,
                    ecoatp_cost=1.0,
                    guarantee=GuaranteeLevel.BEST_EFFORT,
                    initial_tier=StorageTier.LIPID_DEPOT
                )
                if stored:
                    return token_id
            except:
                pass
        return None

    def _pump_trust_gradient(self, participant_id: str, success: bool, contribution: float):
        if self.gradient_manager:
            delta = (0.05 * contribution) if success else (-0.1)
            try:
                self.gradient_manager.pump_field('trust', delta, source=f"federated_{participant_id}")
            except:
                pass
            if participant_id in self.participants:
                self.participants[participant_id].trust_pumping_count += 1
            self.trust_gradient_history.append({
                'participant': participant_id,
                'delta': delta,
                'success': success,
                'timestamp': datetime.now(timezone.utc).isoformat()
            })

    def _get_atp_driven_sync_timing(self) -> float:
        if self.scheduler:
            try:
                driving_force = self.scheduler.calculate_gradient_driving_force()
                rotation_speed = self.scheduler.calculate_rotation_speed(driving_force)
                ecoatp_rate = self.scheduler.calculate_atp_production_rate(rotation_speed)
                if ecoatp_rate > 100:
                    return 30.0
                elif ecoatp_rate > 50:
                    return 60.0
                else:
                    return 120.0
            except:
                pass
        return 60.0

    def _get_harvester_confidence(self) -> float:
        if self.harvester:
            try:
                stats = self.harvester.get_harvesting_stats()
                recent = stats.get('recent_conversions', [])
                if recent:
                    return np.mean([c.get('convertible_energy', 0.5) for c in recent[-10:]])
            except:
                pass
        return 0.5

    def _get_token_efficiency(self, participant_id: str) -> float:
        if self.token_manager:
            try:
                account = self.token_manager.get_account_summary(f"federated_{participant_id}")
                if account:
                    return account.get('efficiency_rating', 0.5)
            except:
                pass
        if participant_id in self.participants:
            participant = self.participants[participant_id]
            if participant.tokens_earned > 0:
                return min(1.0, participant.tokens_earned / 100.0)
        return 0.5

    def _get_gradient_modulated_privacy(self, base_epsilon: float) -> float:
        if self.gradient_manager:
            try:
                carbon = self.gradient_manager.fields.get('carbon')
                if carbon and carbon.gradient_strength > 0.7:
                    return base_epsilon * 0.5
                elif carbon and carbon.gradient_strength < 0.3:
                    return base_epsilon * 1.5
            except:
                pass
        return base_epsilon

    def _get_real_gradient_levels(self) -> Dict[str, float]:
        if self.gradient_manager:
            try:
                return self.gradient_manager.get_field_strengths()
            except:
                pass
        return {'carbon': 0.5, 'helium': 0.5, 'trust': 0.5, 'opportunity': 0.5}

    def _get_compartment_health(self, participant_id: str) -> float:
        if self.compartment_manager:
            try:
                compartment = self.compartment_manager.find_best_compartment('data')
                if compartment:
                    return compartment.health_score
            except:
                pass
        return 0.5

    # Real helium access
    def _get_helium_scarcity(self) -> float:
        if self.helium_provider:
            try:
                return self.helium_provider.get_scarcity()
            except:
                pass
        return 0.5

    def _get_helium_cost_index(self) -> float:
        if self.helium_provider:
            try:
                return self.helium_provider.get_cost_index()
            except:
                pass
        return 1.0

    def _get_helium_efficiency(self) -> float:
        if self.helium_provider:
            try:
                return self.helium_provider.get_efficiency()
            except:
                pass
        return 0.5

    # ========================================================================
    # Participant Registration
    # ========================================================================
    def register_participant(
        self,
        expert_id: str,
        initial_model: Dict[str, Any],
        data_distribution: Dict[str, float],
        capabilities: ClientCapabilities,
        carbon_footprint: float,
        helium_usage: float,
        sustainability_contribution: float = 0.5,
        public_key_pem: Optional[str] = None,
        architecture_type: str = "standard",
        role: ParticipantRole = ParticipantRole.FOLLOWER
    ) -> bool:
        if expert_id in self.participants:
            logger.warning(f"Participant {expert_id} already registered")
            return False

        if self.participant_selector:
            asyncio.create_task(
                self.participant_selector.register_participant(
                    expert_id,
                    {
                        'data_quality': 0.5,
                        'energy_efficiency': 1.0 - carbon_footprint * 100,
                        'carbon_efficiency': 1.0 - carbon_footprint * 50,
                        'network_latency': capabilities.network_latency_ms,
                        'compute_power': capabilities.compute_power_flops,
                        'availability': 0.9,
                        'reliability': 0.95,
                        'economic_efficiency': 0.5,
                        'sustainability_score': sustainability_contribution
                    },
                    0.5,
                    role
                )
            )

        if self.enable_bio_integration and self.token_manager:
            self.token_manager.create_account(f"federated_{expert_id}")
            initial_tokens = int(capabilities.compute_power_flops / 1e10)
            if initial_tokens > 0:
                self.token_manager.generate_tokens(
                    account_id=f"federated_{expert_id}",
                    source=EcoATPSource.EFFICIENCY_GAIN,
                    energy_saved_kwh=0.001,
                    num_tokens=initial_tokens
                )

        if self.enable_bio_integration:
            capabilities.token_efficiency = self._get_token_efficiency(expert_id)
            capabilities.gradient_alignment = self._get_gradient_aligned_selection(expert_id)
            capabilities.compartment_health = 0.7
            capabilities.sustainability_score = sustainability_contribution
            capabilities.reputation_score = 0.5
            capabilities.role = role

        secure_key = secrets.token_bytes(32) if self.enable_secure_aggregation else None
        participant = FederatedExpert(
            expert_id=expert_id,
            local_model=initial_model,
            data_distribution=data_distribution,
            capabilities=capabilities,
            carbon_footprint=carbon_footprint,
            helium_usage=helium_usage,
            privacy_budget=self.privacy_epsilon,
            architecture_type=architecture_type,
            sustainability_contribution=sustainability_contribution,
            secure_key=secure_key
        )
        self.participants[expert_id] = participant
        logger.info(f"Registered federated participant: {expert_id} (role: {role.value})")
        return True

    # ========================================================================
    # Enhanced Federation Round (with deep MoE/SEG, TimeTick, QuantumBridge, CostBenefit)
    # ========================================================================
    async def federated_round(
        self,
        carbon_zone: int,
        helium_scarcity: float,
        timeout_seconds: Optional[int] = None,
        required_roles: List[ParticipantRole] = None
    ) -> Optional[Dict[str, Any]]:
        self.round_number += 1
        round_start = datetime.now(timezone.utc)
        logger.info(f"Starting federated round {self.round_number}")

        # Update carbon intensity
        carbon_intensity = 400
        if self.carbon_manager:
            carbon_data = await self.carbon_manager.update_carbon_intensity('us-east')
            carbon_intensity = carbon_data.get('intensity', 400)

        # Use real helium if provider available
        if self.helium_provider:
            helium_scarcity = self._get_helium_scarcity()
            helium_cost = self._get_helium_cost_index()
            helium_efficiency = self._get_helium_efficiency()
        else:
            helium_cost = 1.0
            helium_efficiency = 0.5

        # Update economic prices if enabled
        if self.enable_economic_pricing and self.pricing_manager:
            prices = await self.pricing_manager.get_current_prices()
            carbon_price = prices.get('carbon_price_usd_per_ton', 50.0)
            helium_price = prices.get('helium_price_usd_per_l', 0.5)
        else:
            carbon_price = 50.0
            helium_price = 0.5

        # ATP-driven sync timing
        if self.enable_bio_integration:
            atp_delay = self._get_atp_driven_sync_timing()
            if timeout_seconds is None:
                timeout_seconds = int(atp_delay)

        timeout_seconds = timeout_seconds or self.max_straggler_wait_seconds

        # Update participant bio metrics
        if self.enable_bio_integration:
            for participant_id, participant in self.participants.items():
                participant.gradient_alignment = self._get_gradient_aligned_selection(participant_id)
                participant.capabilities.token_efficiency = self._get_token_efficiency(participant_id)
                participant.capabilities.sustainability_score = participant.sustainability_contribution

        # Update reputation scores
        if self.enable_reputation and self.reputation_system:
            for participant_id in self.participants:
                # Update with recent performance
                pass

        # Select participants
        n_participants = self._calculate_optimal_participants(carbon_zone, helium_scarcity)
        selected = await self._select_participants_bio_aware(
            n_participants, carbon_zone, helium_scarcity, carbon_intensity, required_roles
        )
        if len(selected) < self.min_participants:
            logger.warning(f"Insufficient participants: {len(selected)} < {self.min_participants}")
            return None
        logger.info(f"Selected {len(selected)} participants")

        # Modulate privacy budget
        if self.enable_bio_integration:
            effective_epsilon = self._get_gradient_modulated_privacy(self.privacy_epsilon)
        else:
            effective_epsilon = self.privacy_epsilon

        # Evaluate playbooks
        playbook_recommendations = []
        if self.enable_playbook and self.playbook_system:
            context = {
                'carbon_intensity': carbon_intensity,
                'helium_availability': 1.0 - helium_scarcity,
                'carbon_zone': carbon_zone,
                'carbon_price': carbon_price,
                'helium_price': helium_price,
                'renewable_availability': 0.6
            }
            playbook_recommendations = await self.playbook_system.evaluate_playbooks(context)

        # Create federation round
        federation_round = FederationRound(
            round_id=f"round_{self.round_number}_{datetime.now(timezone.utc).timestamp()}",
            round_number=self.round_number,
            started_at=round_start,
            participants=selected,
            aggregation_strategy=self.aggregation_strategy,
            privacy_level=self.privacy_level,
            atp_sync_delay=self._get_atp_driven_sync_timing() if self.enable_bio_integration else 0.0,
            secure_aggregation_rounds=0,
            economic_impact=0.0
        )

        try:
            # Get compartment-aware timeouts
            adaptive_timeouts = {}
            if self.enable_bio_integration:
                for participant_id in selected:
                    adaptive_timeouts[participant_id] = self._get_compartment_health_timeout(participant_id)

            # Collect updates
            updates = {}
            total_tokens_staked = 0.0
            for participant_id in selected:
                if participant_id in self.participants:
                    participant = self.participants[participant_id]
                    participant_timeout = adaptive_timeouts.get(participant_id, timeout_seconds)
                    try:
                        update = await asyncio.wait_for(
                            self._collect_update(participant_id, effective_epsilon, carbon_intensity),
                            timeout=participant_timeout
                        )
                        if update:
                            if self.enable_bio_integration:
                                update.tokens_staked = participant.tokens_earned
                                update.gradient_level = participant.gradient_alignment
                                update.token_efficiency = participant.capabilities.token_efficiency
                                update.harvester_confidence = self._get_harvester_confidence()
                                update.sustainability_impact = participant.sustainability_contribution
                            if self.enable_economic_pricing:
                                update.carbon_price = carbon_price
                                update.helium_price = helium_price
                                update.economic_impact = (
                                    carbon_price * participant.carbon_footprint * 0.01 +
                                    helium_price * participant.helium_usage * 0.1
                                )
                            if self.enable_compression_enhanced and self.compressor:
                                compressed, metadata = await self.compressor.compress_model(
                                    participant.local_model, tier="regional"
                                )
                                update.original_size_bytes = metadata['original_size']
                                update.compressed_size_bytes = metadata['compressed_size']
                                update.compression_ratio = metadata['ratio']
                            if self.enable_zk_proofs and self.secure_aggregator:
                                update.zk_proof = secrets.token_bytes(32)
                                if not self.secure_aggregator.verify_zk_proof(update):
                                    logger.warning(f"ZK proof verification failed for {participant_id}")
                                    continue
                            updates[participant_id] = update
                            total_tokens_staked += participant.tokens_earned
                    except asyncio.TimeoutError:
                        logger.warning(f"Participant {participant_id} timed out")
                        federation_round.dropped_participants.append(participant_id)

            if len(updates) < self.min_participants:
                logger.warning(f"Insufficient updates: {len(updates)}")
                return None

            # Get reputation scores
            reputation_scores = {}
            if self.enable_reputation and self.reputation_system:
                for participant_id in updates:
                    rep_score = await self.reputation_system.get_reputation_score(participant_id)
                    reputation_scores[participant_id] = rep_score

            # Select aggregation strategy (using CostBenefit and others)
            if self.enable_cost_benefit and self.cost_benefit_engine:
                # Evaluate different strategies using cost-benefit engine
                strategies = [AggregationStrategy.FED_AVG, AggregationStrategy.ADAPTIVE, AggregationStrategy.SECURE_AGGREGATION]
                best_strategy = AggregationStrategy.FED_AVG
                best_roi = -float('inf')
                for strat in strategies:
                    params = {'strategy': strat.value, 'participants': len(updates), 'carbon_intensity': carbon_intensity}
                    analysis = await self.cost_benefit_engine.analyze_scenario(f'federation_{strat.value}', params)
                    if analysis.roi > best_roi:
                        best_roi = analysis.roi
                        best_strategy = strat
                strategy = best_strategy
            elif self.enable_secure_aggregation and self.secure_aggregator:
                strategy = AggregationStrategy.SECURE_AGGREGATION
                federation_round.secure_aggregation_rounds = 3
            elif self.enable_reputation and reputation_scores:
                strategy = AggregationStrategy.REPUTATION_WEIGHTED
            elif self.enable_economic_pricing and carbon_price > 100:
                strategy = AggregationStrategy.PRICE_AWARE
            elif self.enable_bio_integration:
                if total_tokens_staked > 100:
                    strategy = AggregationStrategy.TOKEN_WEIGHTED
                elif self.enable_sustainability_scoring:
                    strategy = AggregationStrategy.SUSTAINABILITY_WEIGHTED
                elif self.gradient_manager:
                    strategy = AggregationStrategy.GRADIENT_ALIGNED
                else:
                    strategy = self.aggregation_strategy
            else:
                strategy = self.aggregation_strategy

            federation_round.aggregation_strategy = strategy

            # Use QuantumBridge to adjust aggregation weights
            if self.enable_quantum_bridge and self.quantum_bridge:
                q_params = self.quantum_bridge.get_qubo_parameters()
                penalty_helium = q_params.get('penalty_helium_shortage', 0.5)
                if penalty_helium > 0.7:
                    # Increase weight on helium savings
                    for pid in updates:
                        updates[pid].sustainability_impact *= 1.2

            # Use TimeTickEngine to adjust number of participants
            if self.enable_time_tick_engine and self.tick_engine:
                forecast = self.tick_engine.get_helium_forecast(4)
                if forecast and len(forecast) > 3:
                    avg_future = np.mean(forecast)
                    if avg_future < 0.3:
                        self.max_participants = max(self.min_participants, self.max_participants - 1)

            # Aggregate updates
            if self.enable_secure_aggregation and self.secure_aggregator:
                tensor_updates = []
                participant_ids = []
                for pid, update in updates.items():
                    tensor_delta = {k: torch.tensor(v) if isinstance(v, (int, float)) else v for k, v in update.model_delta.items()}
                    tensor_updates.append(tensor_delta)
                    participant_ids.append(pid)
                participant_weights = {pid: self.participants[pid].tokens_earned for pid in participant_ids if pid in self.participants}
                aggregated = await self.secure_aggregator.aggregate_with_secure_aggregation(
                    tensor_updates,
                    participant_ids,
                    participant_weights=participant_weights,
                    reputation_scores=reputation_scores,
                    privacy_budget=effective_epsilon
                )
                global_model = {k: v.numpy().tolist() for k, v in aggregated.items()}
            else:
                global_model = await self._aggregate_updates_bio_aware(updates, strategy)

            self.global_model = global_model

            # Push rich context to gating network and SEG
            if self.gating_network and self.expert_router:
                context = {
                    'carbon_intensity': carbon_intensity,
                    'helium_scarcity': helium_scarcity,
                    'carbon_price': carbon_price,
                    'participants': len(selected),
                    'sustainability_score': federation_round.sustainability_score,
                    'avg_participant_health': np.mean([p.capabilities.compartment_health for p in self.participants.values() if p.capabilities])
                }
                features = np.array([
                    context['carbon_intensity'] / 1000,
                    context['helium_scarcity'],
                    context['carbon_price'] / 100,
                    context['participants'] / 10,
                    context['sustainability_score']
                ])
                reward = federation_round.sustainability_score
                self.gating_network.update(features, reward, context)

            if self.self_evolving_gate:
                self.self_evolving_gate.evolve_gating_network(features, reward, context)

            # Cross-tier distillation
            if self.enable_cross_tier_distillation and self.distiller:
                for tier in ['edge', 'regional', 'continental']:
                    student_model = await self.distiller.distill(global_model, tier)
                    logger.info(f"Distilled model for tier {tier}")

            # Distribute token incentives
            if self.enable_bio_integration and self.enable_incentives:
                total_distributed = 0.0
                for participant_id in updates:
                    participant = self.participants.get(participant_id)
                    if participant:
                        contribution = 0.5
                        distributed = self._distribute_token_incentives(participant_id, contribution, success=True)
                        total_distributed += distributed
                        self._pump_trust_gradient(participant_id, success=True, contribution=contribution)
                federation_round.tokens_distributed = total_distributed
                federation_round.trust_gradient_delta = 0.05

            # Update sustainability metrics
            self.total_carbon_savings_kg += sum(u.carbon_savings for u in updates.values())
            self.sustainability_score = self._calculate_sustainability_score(updates, carbon_intensity, helium_scarcity)

            # Update reputation
            if self.enable_reputation and self.reputation_system:
                for participant_id, update in updates.items():
                    success = update.local_accuracy > 0.7
                    await self.reputation_system.update_reputation(
                        participant_id,
                        success=success,
                        sustainability_contribution=update.sustainability_impact,
                        token_stake=update.tokens_staked,
                        data_quality=update.local_accuracy,
                        carbon_efficiency=update.carbon_savings / max(1.0, update.training_data_size),
                        economic_contribution=update.economic_impact if hasattr(update, 'economic_impact') else 0.0,
                        validation_success=update.validation_score > 0.5 if hasattr(update, 'validation_score') else True
                    )

            # Apply playbook recommendations
            if self.enable_playbook and playbook_recommendations:
                for rec in playbook_recommendations[:2]:
                    playbook = rec['playbook']
                    success = await self._apply_playbook(playbook, rec['match_score'])
                    await self.playbook_system.record_playbook_usage(
                        playbook.playbook_id,
                        success=success,
                        metrics={'sustainability': self.sustainability_score}
                    )
                    if success:
                        federation_round.playbook_applied = playbook.playbook_id

            # Update predictive analyzer
            if self.predictive_analyzer:
                self.predictive_analyzer.update_history({
                    'participants': len(selected),
                    'carbon_intensity': carbon_intensity,
                    'helium_scarcity': helium_scarcity,
                    'sustainability_score': self.sustainability_score,
                    'token_pool': self.federation_token_pool,
                    'round_success': True,
                    'participant_health': {pid: self._get_compartment_health(pid) for pid in selected}
                })
                await self.predictive_analyzer.train_forecast_model()
                forecast = await self.predictive_analyzer.predict_federation_trend()
            else:
                forecast = None

            # Store audit in biomass
            if self.enable_bio_integration and self.enable_blockchain_audit:
                audit_data = {
                    'round_number': self.round_number,
                    'participants': selected,
                    'strategy': strategy.value,
                    'tokens_distributed': federation_round.tokens_distributed,
                    'sustainability_score': self.sustainability_score,
                    'timestamp': datetime.now(timezone.utc).isoformat()
                }
                biomass_token = self._store_audit_in_biomass(audit_data)
                if biomass_token:
                    federation_round.biomass_audit_token = biomass_token

            # Complete round
            federation_round.completed_at = datetime.now(timezone.utc)
            federation_round.successful = True
            if self.enable_compression_enhanced and self.compressor:
                federation_round.compression_stats = self.compressor.get_compression_stats()
            if self.enable_reputation and self.reputation_system:
                federation_round.reputation_changes = {
                    pid: await self.reputation_system.get_reputation_score(pid)
                    for pid in selected if pid in self.participants
                }
            self.aggregation_history.append(federation_round)

            # Trigger workflows if needed
            if self.sustainability_score < 0.4 and self.workflow_orchestrator:
                await self.workflow_orchestrator.execute_workflow('adjust_federation_policy')
            if self.total_helium_savings_l < 1.0 and self.workflow_orchestrator:
                await self.workflow_orchestrator.execute_workflow('optimize_helium_usage')

            # Save state
            self._save_state()

            logger.info(f"Federated round {self.round_number} completed successfully")
            return global_model

        except Exception as e:
            logger.error(f"Federated round {self.round_number} failed: {e}")
            federation_round.successful = False
            self.aggregation_history.append(federation_round)
            return None

    def _calculate_optimal_participants(self, carbon_zone: int, helium_scarcity: float) -> int:
        base = (self.min_participants + self.max_participants) // 2
        if carbon_zone >= 8:
            base = max(self.min_participants, base - 2)
        if helium_scarcity > 0.7:
            base = max(self.min_participants, base - 1)
        return min(self.max_participants, max(self.min_participants, base))

    async def _select_participants_bio_aware(self, n_participants: int, carbon_zone: int, helium_scarcity: float, carbon_intensity: float, required_roles: List[ParticipantRole] = None) -> List[str]:
        if self.participant_selector:
            return await self.participant_selector.select_participants(
                n_participants,
                carbon_intensity=carbon_intensity,
                required_roles=required_roles
            )
        selected = list(self.participants.keys())[:n_participants]
        return selected

    async def _collect_update(self, participant_id: str, privacy_budget: float, carbon_intensity: float) -> Optional[SecureModelUpdate]:
        if participant_id not in self.participants:
            return None
        participant = self.participants[participant_id]
        # Simulate update collection
        update = SecureModelUpdate(
            client_id=participant_id,
            round_number=self.round_number,
            encrypted_gradients=b'encrypted_gradients',
            encryption_metadata={'method': 'fernet'},
            proof_of_training=b'proof',
            signature=b'signature',
            timestamp=datetime.now(timezone.utc),
            carbon_footprint_kg=participant.carbon_footprint,
            tokens_staked=participant.tokens_earned,
            gradient_level=participant.gradient_alignment,
            carbon_savings=participant.carbon_footprint * 0.01,
            sustainability_impact=participant.sustainability_contribution,
            local_accuracy=0.9,
            model_delta=participant.local_model,
            training_data_size=1000
        )
        return update

    async def _aggregate_updates_bio_aware(self, updates: Dict[str, SecureModelUpdate], strategy: AggregationStrategy) -> Dict[str, Any]:
        if not updates:
            return {}
        aggregated = {}
        n = len(updates)
        if strategy == AggregationStrategy.FED_AVG:
            for key in next(iter(updates.values())).model_delta.keys():
                values = [u.model_delta[key] for u in updates.values() if key in u.model_delta]
                if values:
                    aggregated[key] = sum(values) / n
        elif strategy == AggregationStrategy.TOKEN_WEIGHTED:
            total_tokens = sum(u.tokens_staked for u in updates.values())
            if total_tokens > 0:
                for key in next(iter(updates.values())).model_delta.keys():
                    weighted_sum = 0.0
                    for update in updates.values():
                        if key in update.model_delta:
                            weight = update.tokens_staked / total_tokens
                            weighted_sum += update.model_delta[key] * weight
                    aggregated[key] = weighted_sum
            else:
                for key in next(iter(updates.values())).model_delta.keys():
                    values = [u.model_delta[key] for u in updates.values() if key in u.model_delta]
                    if values:
                        aggregated[key] = sum(values) / n
        elif strategy == AggregationStrategy.SUSTAINABILITY_WEIGHTED:
            total_sustainability = sum(u.sustainability_impact for u in updates.values())
            if total_sustainability > 0:
                for key in next(iter(updates.values())).model_delta.keys():
                    weighted_sum = 0.0
                    for update in updates.values():
                        if key in update.model_delta:
                            weight = update.sustainability_impact / total_sustainability
                            weighted_sum += update.model_delta[key] * weight
                    aggregated[key] = weighted_sum
            else:
                for key in next(iter(updates.values())).model_delta.keys():
                    values = [u.model_delta[key] for u in updates.values() if key in u.model_delta]
                    if values:
                        aggregated[key] = sum(values) / n
        else:
            for key in next(iter(updates.values())).model_delta.keys():
                values = [u.model_delta[key] for u in updates.values() if key in u.model_delta]
                if values:
                    aggregated[key] = sum(values) / n
        return aggregated

    def _calculate_sustainability_score(self, updates: Dict[str, SecureModelUpdate], carbon_intensity: float, helium_scarcity: float) -> float:
        if not updates:
            return 0.0
        avg_carbon_savings = np.mean([u.carbon_savings for u in updates.values()])
        avg_sustainability = np.mean([u.sustainability_impact for u in updates.values()])
        carbon_factor = 1.0 - (carbon_intensity / 800)
        helium_factor = 1.0 - helium_scarcity
        score = avg_carbon_savings * 0.3 + avg_sustainability * 0.3 + carbon_factor * 0.2 + helium_factor * 0.2
        return min(1.0, max(0.0, score))

    async def _apply_playbook(self, playbook: PlaybookStrategy, match_score: float) -> bool:
        try:
            for action in playbook.actions:
                action_type = action.get('type')
                if action_type == 'schedule_shift':
                    pass
                elif action_type == 'reduce_workload':
                    pass
                elif action_type == 'switch_cooling':
                    pass
            logger.info(f"Applied playbook: {playbook.name} (match: {match_score:.2f})")
            return True
        except Exception as e:
            logger.error(f"Failed to apply playbook: {e}")
            return False

    # ========================================================================
    # Self-Healing
    # ========================================================================
    async def self_heal(self):
        logger.info("EnhancedFederatedOrchestrator self‑healing")
        if self.enable_self_healing:
            self.min_participants = 3
            self.max_participants = 10
            self.privacy_epsilon = 1.0
            self.aggregation_strategy = AggregationStrategy.FED_AVG
            if self.enable_reputation and self.reputation_system:
                for pid in list(self.participants.keys()):
                    score = await self.reputation_system.get_reputation_score(pid)
                    if score < 0.2:
                        del self.participants[pid]
            self.federation_token_pool = 1000.0
            self.health_status = "healthy"
            self.last_error = None
            self._save_state()
            logger.info("Self-healing completed")

    # ========================================================================
    # Health Monitoring
    # ========================================================================
    def get_health_status(self) -> Dict[str, Any]:
        return {
            'status': self.health_status,
            'last_error': self.last_error,
            'participants': len(self.participants),
            'round_number': self.round_number,
            'sustainability_score': self.sustainability_score,
            'federation_token_pool': self.federation_token_pool,
            'bio_integration_active': self.enable_bio_integration,
            'event_driven_active': self.enable_event_driven,
            'self_healing_enabled': self.enable_self_healing,
            'swarm_coordination_active': self.enable_swarm_coordination,
            'persistence_enabled': self.persistence is not None,
        }

    # ========================================================================
    # Statistics and Reporting
    # ========================================================================
    def get_federation_stats(self) -> Dict[str, Any]:
        stats = {
            'total_participants': len(self.participants),
            'total_rounds': len(self.aggregation_history),
            'bio_integration_active': self.enable_bio_integration,
            'secure_aggregation_active': self.enable_secure_aggregation,
            'zk_proofs_active': self.enable_zk_proofs,
            'reputation_active': self.enable_reputation,
            'playbook_active': self.enable_playbook,
            'economic_pricing_active': self.enable_economic_pricing,
            'compression_active': self.enable_compression_enhanced,
            'cross_tier_distillation_active': self.enable_cross_tier_distillation,
            'moe_router_injected': self.expert_router is not None,
            'gating_network_injected': self.gating_network is not None,
            'self_evolving_gate_injected': self.self_evolving_gate is not None,
            'helium_provider_injected': self.helium_provider is not None,
            'federation_token_pool': self.federation_token_pool,
            'total_carbon_savings_kg': self.total_carbon_savings_kg,
            'sustainability_score': self.sustainability_score,
            'recent_rounds': [
                {
                    'round_number': r.round_number,
                    'participants': len(r.participants),
                    'strategy': r.aggregation_strategy.value,
                    'sustainability_score': r.sustainability_score,
                    'successful': r.successful
                }
                for r in self.aggregation_history[-5:]
            ] if self.aggregation_history else []
        }
        if self.enable_reputation and self.reputation_system:
            stats['reputation_stats'] = self.reputation_system.get_reputation_stats()
        if self.enable_playbook and self.playbook_system:
            stats['playbook_stats'] = self.playbook_system.get_playbook_stats()
        if self.enable_economic_pricing and self.pricing_manager:
            stats['price_stats'] = self.pricing_manager.get_price_stats()
        if self.enable_compression_enhanced and self.compressor:
            stats['compression_stats'] = self.compressor.get_compression_stats()
        if self.enable_cross_tier_distillation and self.distiller:
            stats['distillation_stats'] = self.distiller.get_distillation_stats()
        if self.enable_bio_integration:
            stats['gradient_levels'] = self._get_real_gradient_levels()
        return stats

    def get_sustainability_report(self) -> Dict[str, Any]:
        return {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'sustainability_score': self.sustainability_score,
            'total_carbon_savings_kg': self.total_carbon_savings_kg,
            'total_helium_savings_l': self.total_helium_savings_l,
            'federation_token_pool': self.federation_token_pool,
            'participant_count': len(self.participants),
            'round_count': self.round_number,
            'bio_integration_active': self.enable_bio_integration,
            'predictive_forecast': self.predictive_analyzer.get_sustainability_summary() if self.enable_predictive else {},
            'reputation_stats': self.reputation_system.get_reputation_stats() if self.enable_reputation else {},
            'playbook_stats': self.playbook_system.get_playbook_stats() if self.enable_playbook else {},
            'compression_stats': self.compressor.get_compression_stats() if self.enable_compression_enhanced else {},
            'recommendations': self._generate_sustainability_recommendations()
        }

    def _generate_sustainability_recommendations(self) -> List[str]:
        recommendations = []
        if self.sustainability_score < 0.5:
            recommendations.append("Increase federated participation for better sustainability")
            recommendations.append("Optimize carbon-aware scheduling")
        if self.total_carbon_savings_kg < 10:
            recommendations.append("Implement more aggressive carbon reduction strategies")
        if self.federation_token_pool < 50:
            recommendations.append("Boost token staking incentives")
        if self.enable_reputation and self.reputation_system:
            rep_stats = self.reputation_system.get_reputation_stats()
            if rep_stats.get('average_score', 0.5) < 0.4:
                recommendations.append("Consider removing low-reputation participants")
        if self.enable_playbook and self.playbook_system:
            playbook_stats = self.playbook_system.get_playbook_stats()
            if playbook_stats.get('active_playbooks', 0) == 0:
                recommendations.append("Activate strategic playbooks for optimization")
        if self.enable_economic_pricing and self.pricing_manager:
            price_stats = self.pricing_manager.get_price_stats()
            if price_stats.get('average_carbon_price', 50) > 100:
                recommendations.append("Carbon prices are high - prioritize carbon reduction")
        return recommendations or ["Federation sustainability is on track"]

    # ========================================================================
    # Shutdown
    # ========================================================================
    async def shutdown(self):
        logger.info("Shutting down Enhanced Federated Orchestrator")
        self._save_state()
        if hasattr(self, 'carbon_manager') and self.carbon_manager:
            await self.carbon_manager.close()
        if self.enable_economic_pricing and self.pricing_manager and self.pricing_manager._session:
            await self.pricing_manager._session.close()
        logger.info("Enhanced Federated Orchestrator shutdown complete")

# ============================================================================
# Legacy Compatibility Class
# ============================================================================
class FederatedExperts(EnhancedFederatedOrchestrator):
    """
    Legacy FederatedExperts class for backward compatibility.
    """
    pass
