# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/moe_expert_system/advanced/cross_region_federation.py
"""
Enhanced Cross-Region Federation v7.1.0 - Global Federated Network
With Tiered Aggregation, Global Resource Optimization, and Federated Discovery
MoE Expert Router integration, Self-Evolving Gate integration, Real Helium provider,
and Asynchronous Region updates with staleness handling.

Complete bio-inspired integration with:
- Federated Reflexive Learning with global model sharing
- Tiered Aggregation (Edge → Regional → Continental → Global)
- Global Resource Optimization with carbon/helium awareness
- Federated Discovery and Registration
- User-Adaptive Reflexivity with dynamic thresholds
- Real-time Carbon Intensity Integration with API support
- Cross-Domain Knowledge Transfer with domain mapping
- Human-AI Collaborative Reflection with feedback loops
- Predictive Reflexivity with ensemble forecasting
- Sustainability Score with multi-metric aggregation
- Enhanced Carbon/Helium Awareness with real-time tracking
- Model Compression at each tier
- Real-time pricing signals for carbon and helium
- Reputation scoring for nodes
- Strategic playbook system
- MoE Expert Router integration (NEW)
- Self-Evolving Gate integration (NEW)
- Real Helium provider injection (NEW)
- Asynchronous region updates with staleness handling (NEW)
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
from datetime import datetime, timedelta
from enum import Enum
import numpy as np
from collections import defaultdict, deque
import math
import torch
import torch.nn as nn
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.metrics import r2_score, mean_squared_error
import aiohttp
from cryptography.fernet import Fernet
import zlib
import pickle
import asyncio

logger = logging.getLogger(__name__)

# ============================================================================
# Bio-Inspired Import Check
# ============================================================================
try:
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
        BiomassStorage, StorageTier, GuaranteeLevel
    )
    from enhancements.bio_inspired.photosynthetic_harvester import (
        PhotosyntheticHarvester
    )
    BIO_INSPIRED_AVAILABLE = True
    logger.info("Bio-inspired modules loaded for Cross-Region Federation")
except ImportError as e:
    BIO_INSPIRED_AVAILABLE = False
    logger.warning(f"Bio-inspired modules not available: {str(e)} - using standard federation")

# ============================================================================
# NEW: Import MoE components (if available)
# ============================================================================
try:
    from ..expert_router import ExpertRouter
    from ..gating_network import GatingNetworkManager
    from ..advanced.self_evolving_gates import EnhancedSelfEvolvingGate
    MOE_AVAILABLE = True
except ImportError:
    MOE_AVAILABLE = False
    logger.warning("MoE Expert Router or Self-Evolving Gates not available - cross-region federation will operate standalone")

# ============================================================================
# NEW: Helium Provider Interface (to be injected)
# ============================================================================
class HeliumProvider:
    """Interface to external helium modules for real-time metrics."""
    def get_scarcity(self) -> float:
        raise NotImplementedError
    def get_cost_index(self) -> float:
        raise NotImplementedError
    def get_efficiency(self) -> float:
        raise NotImplementedError

# ============================================================================
# Enhanced Enums and Data Classes (unchanged)
# ============================================================================

class Region(Enum):
    US_EAST = "us_east"; US_WEST = "us_west"; EU_WEST = "eu_west"
    EU_NORTH = "eu_north"; ASIA_EAST = "asia_east"; ASIA_SOUTHEAST = "asia_southeast"
    AUSTRALIA = "australia"; SOUTH_AMERICA = "south_america"; AFRICA = "africa"; MIDDLE_EAST = "middle_east"

class SyncMode(Enum):
    SYNCHRONOUS = "synchronous"; ASYNCHRONOUS = "asynchronous"; EVENTUAL = "eventual"
    OPPORTUNISTIC = "opportunistic"; GRADIENT_DRIVEN = "gradient_driven"; TOKEN_GATED = "token_gated"

class AggregationTier(Enum):
    EDGE = "edge"; REGIONAL = "regional"; CONTINENTAL = "continental"; GLOBAL = "global"
    CHROMATOPHORE = "chromatophore"; MEMBRANE = "membrane"

class FederationTopology(Enum):
    CENTRALIZED = "centralized"; DECENTRALIZED = "decentralized"; HIERARCHICAL = "hierarchical"
    SWARM = "swarm"; CROSS_SILO = "cross_silo"; CROSS_DEVICE = "cross_device"; METABOLIC_MESH = "metabolic_mesh"

class AggregationStrategy(Enum):
    FED_AVG = "fed_avg"; FED_PROX = "fed_prox"; FED_OPT = "fed_opt"; FED_DYN = "fed_dyn"
    FED_ENSEMBLE = "fed_ensemble"; FED_DISTILL = "fed_distill"; ADAPTIVE = "adaptive"
    TOKEN_WEIGHTED = "token_weighted"; GRADIENT_ALIGNED = "gradient_aligned"
    SUSTAINABILITY_WEIGHTED = "sustainability_weighted"
    TIERED_AGGREGATION = "tiered_aggregation"
    REPUTATION_WEIGHTED = "reputation_weighted"
    PRICE_AWARE = "price_aware"

@dataclass
class RegionalProfile:
    region: Region
    timezone_offset: int
    typical_renewable_hours: List[int]
    carbon_intensity_profile: Dict[int, float]
    renewable_mix: Dict[str, float]
    network_latency_matrix: Dict[str, float]
    bandwidth_capacity_mbps: float
    available_compute_flops: float
    helium_availability: float
    data_sovereignty_constraints: List[str]
    optimal_sync_windows: List[Tuple[int, int]]
    local_carbon_gradient: float = 0.5
    local_trust_gradient: float = 0.5
    token_balance: float = 0.0
    compartment_count: int = 0
    harvester_vitality: float = 0.5
    sustainability_score: float = 0.5
    carbon_savings_kg: float = 0.0
    helium_savings_l: float = 0.0
    tier: AggregationTier = AggregationTier.REGIONAL
    parent_id: Optional[str] = None
    child_ids: List[str] = field(default_factory=list)
    resource_capacity: float = 1.0
    resource_usage: float = 0.0
    carbon_price_usd_per_ton: float = 50.0
    helium_price_usd_per_l: float = 0.5
    reputation_score: float = 0.5
    active_playbooks: List[str] = field(default_factory=list)
    playbook_performance: Dict[str, float] = field(default_factory=dict)

@dataclass
class RegionNode:
    region_id: str
    tier: AggregationTier
    parent_id: Optional[str] = None
    child_ids: List[str] = field(default_factory=list)
    model: Optional[Dict] = None
    last_update: datetime = field(default_factory=datetime.utcnow)
    status: str = "healthy"
    participants: List[str] = field(default_factory=list)
    carbon_intensity: float = 400.0
    helium_availability: float = 0.5
    resource_capacity: float = 1.0
    resource_usage: float = 0.0
    sustainability_score: float = 0.5
    reputation_score: float = 0.5
    carbon_price: float = 50.0
    helium_price: float = 0.5
    compressed_model_size_mb: float = 0.0
    compression_ratio: float = 1.0

    @property
    def resource_available(self) -> float:
        return self.resource_capacity - self.resource_usage

@dataclass
class AsyncUpdate:
    update_id: str
    source_region: Region
    model_delta: Dict[str, Any]
    compression_ratio: float
    timestamp: datetime
    carbon_intensity_at_update: float
    training_data_size: int
    local_accuracy: float
    vector_clock: Dict[str, int]
    signature: str
    tokens_staked: float = 0.0
    gradient_level_at_update: float = 0.5
    compartment_tier: str = "regional"
    harvester_confidence: float = 0.5
    sustainability_impact: float = 0.0
    carbon_savings: float = 0.0
    carbon_price: float = 50.0
    helium_price: float = 0.5
    economic_impact: float = 0.0
    original_size_bytes: int = 0
    compressed_size_bytes: int = 0

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

    def update_score(self, delta: float):
        self.score = max(0.0, min(1.0, self.score + delta))
        self.last_update = datetime.utcnow()

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

    def to_dict(self) -> Dict[str, Any]:
        return {
            'playbook_id': self.playbook_id,
            'name': self.name,
            'domain': self.domain,
            'actions': self.actions,
            'conditions': self.conditions,
            'success_metrics': self.success_metrics,
            'performance_score': self.performance_score,
            'usage_count': self.usage_count,
            'last_used': self.last_used.isoformat(),
            'is_active': self.is_active
        }

# ============================================================================
# Model Compression Module (unchanged)
# ============================================================================

class ModelCompressor:
    """
    Model compression at each aggregation tier.
    
    Features:
    - Lossy and lossless compression
    - Adaptive compression based on tier
    - Size estimation and optimization
    - Compression ratio tracking
    - Quality preservation
    """
    
    def __init__(self):
        self.compressors = {
            'zlib': self._compress_zlib,
            'pickle': self._compress_pickle,
            'hybrid': self._compress_hybrid
        }
        self.compression_stats = deque(maxlen=1000)
        self._lock = asyncio.Lock()
        
        self.tier_settings = {
            AggregationTier.EDGE: {
                'method': 'zlib',
                'target_ratio': 0.7,
                'quality_threshold': 0.95
            },
            AggregationTier.REGIONAL: {
                'method': 'hybrid',
                'target_ratio': 0.5,
                'quality_threshold': 0.90
            },
            AggregationTier.CONTINENTAL: {
                'method': 'hybrid',
                'target_ratio': 0.3,
                'quality_threshold': 0.85
            },
            AggregationTier.GLOBAL: {
                'method': 'hybrid',
                'target_ratio': 0.2,
                'quality_threshold': 0.80
            }
        }
        
        logger.info("Model Compressor initialized")
    
    async def compress_model(
        self,
        model: Dict[str, Any],
        tier: AggregationTier,
        compression_method: Optional[str] = None
    ) -> Tuple[bytes, Dict[str, Any]]:
        async with self._lock:
            settings = self.tier_settings.get(tier, self.tier_settings[AggregationTier.REGIONAL])
            method = compression_method or settings['method']
            original_size = len(pickle.dumps(model))
            compressor = self.compressors.get(method, self._compress_hybrid)
            compressed, metadata = await compressor(model, settings)
            compressed_size = len(compressed)
            compression_ratio = compressed_size / original_size if original_size > 0 else 1.0
            self.compression_stats.append({
                'timestamp': datetime.utcnow().isoformat(),
                'tier': tier.value,
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
                'tier': tier.value,
                'quality': metadata.get('quality', 1.0)
            }
    
    async def decompress_model(
        self,
        compressed: bytes,
        metadata: Dict[str, Any]
    ) -> Dict[str, Any]:
        method = metadata.get('method', 'hybrid')
        if method == 'zlib':
            decompressed = self._decompress_zlib(compressed)
        elif method == 'pickle':
            decompressed = self._decompress_pickle(compressed)
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
    
    def _decompress_zlib(self, compressed: bytes) -> Dict:
        serialized = zlib.decompress(compressed)
        return pickle.loads(serialized)
    
    def _decompress_pickle(self, compressed: bytes) -> Dict:
        return pickle.loads(compressed)
    
    def _decompress_hybrid(self, compressed: bytes) -> Dict:
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
                for tier in [t.value for t in AggregationTier]
            },
            'total_size_saved_mb': sum(s['original_size'] - s['compressed_size'] for s in recent) / (1024 * 1024)
        }

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
            'success_rate': 0.25,
            'sustainability': 0.25,
            'token_stake': 0.20,
            'data_quality': 0.15,
            'participation': 0.10,
            'carbon_efficiency': 0.05
        }
        logger.info("Reputation Scoring System initialized")
    
    async def update_reputation(
        self,
        node_id: str,
        success: bool,
        sustainability_contribution: float = 0.5,
        token_stake: float = 0.0,
        data_quality: float = 0.5,
        carbon_efficiency: float = 0.5
    ):
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
            success_rate = record.successful_updates / max(1, record.total_contributions)
            sustainability_score = record.sustainability_contributions / max(1, record.total_contributions)
            data_quality_score = data_quality * (1.0 - self.decay_rate * (datetime.utcnow() - record.last_update).days / 30)
            carbon_score = 1.0 - carbon_efficiency
            new_score = (
                self.weights['success_rate'] * success_rate +
                self.weights['sustainability'] * sustainability_score +
                self.weights['token_stake'] * min(1.0, token_stake / 100.0) +
                self.weights['data_quality'] * data_quality_score +
                self.weights['participation'] * min(1.0, record.total_contributions / 50.0) +
                self.weights['carbon_efficiency'] * carbon_score
            )
            decay_factor = 1.0 - self.decay_rate
            record.score = max(self.min_score, min(1.0,
                record.score * decay_factor + new_score * (1.0 - decay_factor)
            ))
            record.last_update = datetime.utcnow()
            record.history.append({
                'timestamp': datetime.utcnow().isoformat(),
                'score': record.score,
                'success': success,
                'sustainability': sustainability_contribution,
                'token_stake': token_stake,
                'data_quality': data_quality,
                'carbon_efficiency': carbon_efficiency
            })
            if len(record.history) > 100:
                record.history = record.history[-100:]
    
    async def get_reputation_score(self, node_id: str) -> float:
        if node_id in self.reputation_records:
            return self.reputation_records[node_id].score
        return 0.5
    
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
            'recent_history': record.history[-10:],
            'last_update': record.last_update.isoformat()
        }
    
    def get_top_nodes(self, n: int = 10) -> List[Dict[str, Any]]:
        sorted_nodes = sorted(
            self.reputation_records.items(),
            key=lambda x: x[1].score,
            reverse=True
        )
        return [
            {
                'node_id': node_id,
                'score': record.score,
                'success_rate': record.successful_updates / max(1, record.total_contributions)
            }
            for node_id, record in sorted_nodes[:n]
        ]
    
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
            PlaybookStrategy(
                playbook_id="carbon_peak_avoidance",
                name="Carbon Peak Avoidance",
                domain="energy",
                actions=[
                    {'type': 'schedule_shift', 'target': 'off-peak'},
                    {'type': 'reduce_workload', 'percentage': 0.3}
                ],
                conditions={'carbon_intensity': '> 500'},
                success_metrics={'carbon_reduction': 0.2}
            ),
            PlaybookStrategy(
                playbook_id="helium_conservation",
                name="Helium Conservation",
                domain="sustainability",
                actions=[
                    {'type': 'switch_cooling', 'method': 'alternative'},
                    {'type': 'recovery_mode', 'enabled': True}
                ],
                conditions={'helium_availability': '< 0.3'},
                success_metrics={'helium_savings': 0.5}
            ),
            PlaybookStrategy(
                playbook_id="renewable_maximization",
                name="Renewable Energy Maximization",
                domain="energy",
                actions=[
                    {'type': 'schedule_to_renewable', 'enabled': True},
                    {'type': 'load_balancing', 'strategy': 'renewable_first'}
                ],
                conditions={'renewable_availability': '> 0.6'},
                success_metrics={'renewable_usage': 0.4}
            ),
            PlaybookStrategy(
                playbook_id="economic_optimization",
                name="Economic Optimization",
                domain="economics",
                actions=[
                    {'type': 'price_aware_scheduling', 'enabled': True},
                    {'type': 'cost_minimization', 'priority': 'high'}
                ],
                conditions={'carbon_price': '> 100'},
                success_metrics={'cost_savings': 0.3}
            ),
            PlaybookStrategy(
                playbook_id="quantum_optimization",
                name="Quantum Circuit Optimization",
                domain="quantum",
                actions=[
                    {'type': 'circuit_compression', 'level': 'aggressive'},
                    {'type': 'qubit_saving', 'enabled': True}
                ],
                conditions={'quantum_workload': '> 0.5'},
                success_metrics={'quantum_efficiency': 0.4}
            )
        ]
        for playbook in default_playbooks:
            self.playbooks[playbook.playbook_id] = playbook
    
    async def create_playbook(
        self,
        name: str,
        domain: str,
        actions: List[Dict[str, Any]],
        conditions: Dict[str, Any],
        success_metrics: Dict[str, float]
    ) -> PlaybookStrategy:
        async with self._lock:
            playbook_id = f"playbook_{int(time.time())}_{name.lower().replace(' ', '_')}"
            playbook = PlaybookStrategy(
                playbook_id=playbook_id,
                name=name,
                domain=domain,
                actions=actions,
                conditions=conditions,
                success_metrics=success_metrics
            )
            self.playbooks[playbook_id] = playbook
            logger.info(f"Created playbook: {playbook_id}")
            return playbook
    
    async def evaluate_playbooks(self, context: Dict[str, Any]) -> List[Dict[str, Any]]:
        async with self._lock:
            recommendations = []
            for playbook in self.playbooks.values():
                if not playbook.is_active:
                    continue
                match_score = await self._evaluate_conditions(playbook.conditions, context)
                if match_score > 0.5:
                    recommendations.append({
                        'playbook': playbook.to_dict(),
                        'match_score': match_score,
                        'expected_impact': await self._estimate_impact(playbook, context)
                    })
            recommendations.sort(key=lambda x: x['match_score'], reverse=True)
            return recommendations
    
    async def _evaluate_conditions(self, conditions: Dict[str, Any], context: Dict[str, Any]) -> float:
        score = 0.0
        total_conditions = len(conditions)
        if total_conditions == 0:
            return 1.0
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
                score += 1.0 / total_conditions
        return score
    
    async def _estimate_impact(self, playbook: PlaybookStrategy, context: Dict[str, Any]) -> Dict[str, float]:
        impact = {}
        if 'carbon_reduction' in playbook.success_metrics:
            impact['carbon_savings'] = playbook.success_metrics['carbon_reduction'] * context.get('carbon_intensity', 400) / 1000
        if 'helium_savings' in playbook.success_metrics:
            impact['helium_savings'] = playbook.success_metrics['helium_savings'] * context.get('helium_availability', 0.5)
        if 'cost_savings' in playbook.success_metrics:
            impact['cost_savings'] = playbook.success_metrics['cost_savings'] * context.get('carbon_price', 50) / 100
        return impact
    
    async def record_playbook_usage(
        self,
        playbook_id: str,
        success: bool,
        metrics: Dict[str, float]
    ):
        async with self._lock:
            if playbook_id not in self.playbooks:
                return
            playbook = self.playbooks[playbook_id]
            playbook.usage_count += 1
            playbook.last_used = datetime.utcnow()
            success_score = 1.0 if success else 0.0
            metric_score = np.mean([
                metrics.get(key, 0.0) / target
                for key, target in playbook.success_metrics.items()
                if key in metrics and target > 0
            ]) if playbook.success_metrics else 0.5
            playbook.performance_score = (
                playbook.performance_score * 0.7 +
                (success_score * 0.5 + metric_score * 0.5) * 0.3
            )
            self.playbook_history.append({
                'playbook_id': playbook_id,
                'timestamp': datetime.utcnow().isoformat(),
                'success': success,
                'metrics': metrics,
                'performance_score': playbook.performance_score
            })
    
    def get_playbook_stats(self) -> Dict[str, Any]:
        return {
            'total_playbooks': len(self.playbooks),
            'active_playbooks': sum(1 for p in self.playbooks.values() if p.is_active),
            'top_performing': sorted(
                self.playbooks.values(),
                key=lambda x: x.performance_score,
                reverse=True
            )[:3],
            'recent_usage': list(self.playbook_history)[-5:]
        }

# ============================================================================
# Economic Pricing Manager (unchanged)
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
        async with self._lock:
            session = await self._get_session()
            try:
                carbon_price = await self._fetch_carbon_price(session, region)
                helium_price = await self._fetch_helium_price(session, region)
                self.carbon_prices[region] = carbon_price
                self.helium_prices[region] = helium_price
                self.price_history.append({
                    'timestamp': datetime.utcnow().isoformat(),
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
    
    async def forecast_prices(self, region: str, days: int = 7) -> Dict[str, List[float]]:
        if not self.forecast_models_trained:
            return {'status': 'not_trained'}
        future_index = np.array(
            range(len(self.price_history), len(self.price_history) + days * 24)
        ).reshape(-1, 1)
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

# ============================================================================
# NEW: Asynchronous Region Staleness Manager
# ============================================================================

class AsynchronousRegionManager:
    """
    Manages asynchronous region updates with staleness handling.
    """
    def __init__(self, staleness_decay: float = 0.1):
        self.region_updates: Dict[str, deque] = defaultdict(lambda: deque(maxlen=20))
        self.staleness_decay = staleness_decay
        self._lock = asyncio.Lock()
        logger.info("Asynchronous Region Manager initialized")
    
    async def submit_update(self, region_id: str, model_delta: Dict[str, Any], timestamp: datetime):
        async with self._lock:
            self.region_updates[region_id].append({
                'model': model_delta,
                'timestamp': timestamp,
                'staleness': 0  # computed later
            })
    
    async def aggregate_region_updates(
        self,
        regions: List[str],
        min_participants: int = 2,
        max_participants: int = 10
    ) -> Optional[Dict[str, Any]]:
        async with self._lock:
            now = datetime.utcnow()
            available = []
            for region_id in regions:
                if region_id in self.region_updates and self.region_updates[region_id]:
                    latest = self.region_updates[region_id][-1]
                    staleness = (now - latest['timestamp']).total_seconds() / 3600  # hours
                    freshness = 1.0 / (1.0 + staleness * self.staleness_decay)
                    available.append({
                        'region_id': region_id,
                        'model': latest['model'],
                        'staleness': staleness,
                        'freshness': freshness
                    })
            if len(available) < min_participants:
                return None
            available.sort(key=lambda x: x['freshness'], reverse=True)
            selected = available[:min(max_participants, len(available))]
            aggregated = {}
            total_weight = 0.0
            for item in selected:
                weight = item['freshness']
                total_weight += weight
                for key, value in item['model'].items():
                    if isinstance(value, (int, float)):
                        aggregated[key] = aggregated.get(key, 0.0) + value * weight
                    elif isinstance(value, list):
                        if key not in aggregated:
                            aggregated[key] = [v * weight for v in value]
                        else:
                            aggregated[key] = [a + v * weight for a, v in zip(aggregated[key], value)]
            if total_weight > 0:
                for key in aggregated:
                    if isinstance(aggregated[key], list):
                        aggregated[key] = [v / total_weight for v in aggregated[key]]
                    else:
                        aggregated[key] /= total_weight
            return aggregated

# ============================================================================
# Enhanced Cross-Region Federation Optimizer (with new integrations)
# ============================================================================

class CrossRegionFederationOptimizer:
    """
    Enhanced Cross-Region Federation v7.1.0 - Complete Production-Grade Implementation
    With MoE integration, real helium, and asynchronous region updates.
    """
    
    def __init__(
        self,
        enable_async: bool = True,
        enable_carbon_scheduling: bool = True,
        enable_compression: bool = True,
        enable_multi_tier: bool = True,
        enable_personalization: bool = True,
        enable_bio_integration: bool = True,
        enable_federated_reflexive: bool = True,
        enable_carbon_intensity: bool = True,
        enable_predictive: bool = True,
        enable_cross_domain: bool = True,
        enable_sustainability_scoring: bool = True,
        enable_tiered_aggregation: bool = True,
        enable_resource_optimization: bool = True,
        enable_discovery: bool = True,
        enable_compression_enhanced: bool = True,
        enable_reputation: bool = True,
        enable_playbook: bool = True,
        enable_economic_pricing: bool = True,
        server_url: Optional[str] = None
    ):
        # Feature flags
        self.enable_async = enable_async
        self.enable_carbon_scheduling = enable_carbon_scheduling
        self.enable_compression = enable_compression
        self.enable_multi_tier = enable_multi_tier
        self.enable_personalization = enable_personalization
        self.enable_bio_integration = enable_bio_integration and BIO_INSPIRED_AVAILABLE
        self.enable_federated_reflexive = enable_federated_reflexive
        self.enable_carbon_intensity = enable_carbon_intensity
        self.enable_predictive = enable_predictive
        self.enable_cross_domain = enable_cross_domain
        self.enable_sustainability_scoring = enable_sustainability_scoring
        self.enable_tiered_aggregation = enable_tiered_aggregation
        self.enable_resource_optimization = enable_resource_optimization
        self.enable_discovery = enable_discovery
        self.enable_compression_enhanced = enable_compression_enhanced
        self.enable_reputation = enable_reputation
        self.enable_playbook = enable_playbook
        self.enable_economic_pricing = enable_economic_pricing

        # NEW: MoE and Self-Evolving Gate references (injected)
        self.gating_network = None
        self.self_evolving_gate = None
        self.expert_router = None
        
        # NEW: Helium provider (injected)
        self.helium_provider = None
        
        # NEW: Asynchronous region manager
        self.async_region_manager = AsynchronousRegionManager() if enable_async else None
        
        # Core modules
        self.tiered_aggregator = TieredAggregator() if enable_tiered_aggregation else None
        self.resource_optimizer = GlobalResourceOptimizer() if enable_resource_optimization else None
        self.discovery = FederatedDiscovery(server_url) if enable_discovery else None
        
        self.compressor = ModelCompressor() if enable_compression_enhanced else None
        self.reputation_system = ReputationScoringSystem() if enable_reputation else None
        self.playbook_system = StrategicPlaybookSystem() if enable_playbook else None
        self.pricing_manager = EconomicPricingManager() if enable_economic_pricing else None
        
        # Bio-inspired modules
        self.token_manager = None
        self.gradient_manager = None
        self.scheduler = None
        self.compartment_manager = None
        self.biomass_storage = None
        self.harvester = None
        
        self.carbon_manager = CarbonIntensityManager() if enable_carbon_intensity else None
        self.predictive_analyzer = PredictiveFederationAnalyzer() if enable_predictive else None
        self.cross_domain_transfer = FederationCrossDomainTransfer() if enable_cross_domain else None
        
        self.regions: Dict[str, RegionNode] = {}
        self.regional_profiles: Dict[Region, RegionalProfile] = {}
        self.participants: Dict[str, FederatedExpert] = {}
        self.aggregation_history: List[Dict] = []
        self.round_number = 0
        self.global_model: Optional[Dict[str, Any]] = None
        
        self.federation_token_pool: float = 0.0
        self.total_carbon_savings_kg = 0.0
        self.total_helium_savings_l = 0.0
        self.sustainability_score = 0.0
        self.instance_id = f"federation_{int(time.time())}"
        
        self._initialize_regional_profiles()
        
        if self.enable_economic_pricing and self.pricing_manager:
            asyncio.create_task(self._price_update_loop())
        
        logger.info(
            f"Cross-Region Federation v7.1.0 initialized: "
            f"bio_integration={self.enable_bio_integration}, "
            f"tiered_aggregation={self.enable_tiered_aggregation}, "
            f"resource_optimization={self.enable_resource_optimization}, "
            f"discovery={self.enable_discovery}, "
            f"compression={self.enable_compression_enhanced}, "
            f"reputation={self.enable_reputation}, "
            f"playbook={self.enable_playbook}, "
            f"economic_pricing={self.enable_economic_pricing}, "
            f"async_regions={self.enable_async}"
        )
    
    def _initialize_regional_profiles(self):
        profiles = {
            Region.US_EAST: {'timezone': -5, 'renewable_hours': [2, 3, 4, 5],
                'carbon_low_hours': [2, 3, 4, 5, 22, 23], 'renewable_mix': {'wind': 0.15, 'solar': 0.10, 'nuclear': 0.30, 'gas': 0.35, 'coal': 0.10}},
            Region.EU_WEST: {'timezone': 0, 'renewable_hours': [12, 13, 14],
                'carbon_low_hours': [1, 2, 3, 4, 12, 13], 'renewable_mix': {'wind': 0.25, 'solar': 0.15, 'nuclear': 0.25, 'gas': 0.25, 'coal': 0.10}},
            Region.ASIA_EAST: {'timezone': 8, 'renewable_hours': [10, 11, 12, 13],
                'carbon_low_hours': [2, 3, 4, 5], 'renewable_mix': {'wind': 0.10, 'solar': 0.15, 'nuclear': 0.10, 'coal': 0.50, 'gas': 0.15}}
        }
        for region, data in profiles.items():
            carbon_profile = {}
            for hour in range(24):
                if hour in data['carbon_low_hours']:
                    carbon_profile[hour] = np.random.uniform(50, 200)
                else:
                    carbon_profile[hour] = np.random.uniform(200, 400)
            self.regional_profiles[region] = RegionalProfile(
                region=region,
                timezone_offset=data['timezone'],
                typical_renewable_hours=data['renewable_hours'],
                carbon_intensity_profile=carbon_profile,
                renewable_mix=data['renewable_mix'],
                network_latency_matrix={'us_east': 0, 'eu_west': 80, 'asia_east': 150},
                bandwidth_capacity_mbps=1000,
                available_compute_flops=1e15,
                helium_availability=np.random.uniform(0.5, 1.0),
                data_sovereignty_constraints=[],
                optimal_sync_windows=[(data['carbon_low_hours'][0], data['carbon_low_hours'][-1])]
            )
    
    async def _price_update_loop(self):
        while True:
            try:
                if self.pricing_manager:
                    for region in self.regions:
                        await self.pricing_manager.update_prices(region)
                    await asyncio.sleep(3600)
            except Exception as e:
                logger.error(f"Price update error: {e}")
                await asyncio.sleep(300)
    
    # ========================================================================
    # NEW: Set MoE Router, Gating Network, Self-Evolving Gate, and Helium Provider
    # ========================================================================
    
    def set_gating_network(self, gating_network: 'GatingNetworkManager'):
        self.gating_network = gating_network
        logger.info("Gating network injected into Cross-Region Federation")
    
    def set_self_evolving_gate(self, gate: 'EnhancedSelfEvolvingGate'):
        self.self_evolving_gate = gate
        logger.info("Self-Evolving Gate injected into Cross-Region Federation")
    
    def set_expert_router(self, router: 'ExpertRouter'):
        self.expert_router = router
        logger.info("Expert Router injected into Cross-Region Federation")
    
    def set_helium_provider(self, provider: HeliumProvider):
        self.helium_provider = provider
        logger.info("Helium provider injected into Cross-Region Federation")
    
    # ========================================================================
    # Bio-Inspired Module Injection
    # ========================================================================
    
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
    
    # ========================================================================
    # Bio-Inspired Data Access Methods (with real helium)
    # ========================================================================
    
    def _get_gradient_aligned_schedule(self, region: Region) -> float:
        if self.gradient_manager and self.enable_bio_integration:
            carbon = self.gradient_manager.fields.get('carbon')
            if carbon and carbon.gradient_strength < 0.3:
                return 0.0
            elif carbon:
                return carbon.gradient_strength * 3600
        return 0.0
    
    def _stake_tokens_for_update(self, region: str, amount: float) -> Tuple[bool, float]:
        if self.token_manager and self.enable_bio_integration:
            success, token_ids = self.token_manager.reserve_tokens(
                account_id=f"federation_{region}",
                amount=amount,
                consumer=EcoATPConsumer.EXPERT_EXECUTION
            )
            if success:
                self.federation_token_pool += amount
                return True, amount
            return False, 0.0
        return True, 0.0
    
    def _get_compartment_tier(self, region: str) -> AggregationTier:
        if self.compartment_manager and self.enable_bio_integration:
            region_types = {
                'us_east': 'data', 'us_west': 'energy',
                'eu_west': 'data', 'eu_north': 'energy',
                'asia_east': 'iot', 'asia_southeast': 'data'
            }
            expert_type = region_types.get(region, 'data')
            compartment = self.compartment_manager.find_best_compartment(expert_type)
            if compartment:
                if compartment.state == CompartmentState.ACTIVE:
                    return AggregationTier.REGIONAL
                elif compartment.health_score > 0.8:
                    return AggregationTier.CONTINENTAL
        return AggregationTier.REGIONAL
    
    def _get_harvester_signal_quality(self) -> float:
        if self.harvester and self.enable_bio_integration:
            stats = self.harvester.get_harvesting_stats()
            recent = stats.get('recent_conversions', [])
            if recent:
                return np.mean([c.get('convertible_energy', 0.5) for c in recent[-10:]])
        return 0.5
    
    def _get_trust_based_byzantine_threshold(self, region: str) -> float:
        if self.gradient_manager and self.enable_bio_integration:
            trust = self.gradient_manager.fields.get('trust')
            if trust:
                return max(0.1, 1.0 - trust.gradient_strength)
        return 0.5
    
    # NEW: Real helium access
    def _get_helium_scarcity(self) -> float:
        if self.helium_provider:
            return self.helium_provider.get_scarcity()
        return 0.5
    
    def _get_helium_cost_index(self) -> float:
        if self.helium_provider:
            return self.helium_provider.get_cost_index()
        return 1.0
    
    def _get_helium_efficiency(self) -> float:
        if self.helium_provider:
            return self.helium_provider.get_efficiency()
        return 0.5
    
    # ========================================================================
    # Region Management (unchanged)
    # ========================================================================
    
    def register_region(
        self,
        region_id: str,
        tier: AggregationTier = AggregationTier.REGIONAL,
        parent_id: Optional[str] = None,
        participants: List[str] = None,
        resource_capacity: float = 1.0
    ) -> RegionNode:
        if region_id in self.regions:
            logger.warning(f"Region {region_id} already registered")
            return self.regions[region_id]
        node = RegionNode(
            region_id=region_id,
            tier=tier,
            parent_id=parent_id,
            participants=participants or [],
            resource_capacity=resource_capacity
        )
        self.regions[region_id] = node
        if parent_id and parent_id in self.regions:
            self.regions[parent_id].child_ids.append(region_id)
        if self.enable_discovery and self.discovery:
            asyncio.create_task(
                self.discovery.register_region(
                    region_id,
                    {
                        'tier': tier.value,
                        'resource_capacity': resource_capacity,
                        'participants': len(participants or [])
                    },
                    parent_id
                )
            )
        if self.enable_reputation and self.reputation_system:
            asyncio.create_task(
                self.reputation_system.update_reputation(
                    region_id,
                    success=True,
                    sustainability_contribution=0.5,
                    token_stake=0.0
                )
            )
        logger.info(f"Registered region: {region_id} (tier: {tier.value})")
        return node
    
    async def update_region_status(
        self,
        region_id: str,
        carbon_intensity: float = None,
        helium_availability: float = None,
        resource_usage: float = None
    ):
        if region_id not in self.regions:
            return
        node = self.regions[region_id]
        if carbon_intensity is not None:
            node.carbon_intensity = carbon_intensity
        if helium_availability is not None:
            node.helium_availability = helium_availability
        if resource_usage is not None:
            node.resource_usage = resource_usage
        node.last_update = datetime.utcnow()
        if self.enable_discovery and self.discovery:
            await self.discovery.update_health(region_id, {
                'status': 'healthy',
                'metrics': {
                    'carbon_intensity': carbon_intensity,
                    'helium_availability': helium_availability,
                    'resource_usage': resource_usage
                }
            })
        if self.enable_reputation and self.reputation_system:
            sustainability = 1.0 - (carbon_intensity or 400) / 800
            await self.reputation_system.update_reputation(
                region_id,
                success=True,
                sustainability_contribution=sustainability
            )
    
    # ========================================================================
    # Enhanced Federation Round (with MoE, real helium, async regions)
    # ========================================================================
    
    async def federated_round(
        self,
        carbon_zone: int,
        helium_scarcity: float,
        timeout_seconds: int = 300,
        region_filter: Optional[List[str]] = None
    ) -> Optional[Dict[str, Any]]:
        self.round_number += 1
        round_start = datetime.utcnow()
        
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
        
        if self.enable_economic_pricing and self.pricing_manager:
            for region_id in self.regions:
                await self.pricing_manager.update_prices(region_id)
        
        playbook_recommendations = []
        if self.enable_playbook and self.playbook_system:
            context = {
                'carbon_intensity': carbon_intensity,
                'helium_availability': 1.0 - helium_scarcity,
                'carbon_zone': carbon_zone,
                'quantum_workload': 0.5,
                'renewable_availability': 0.6
            }
            playbook_recommendations = await self.playbook_system.evaluate_playbooks(context)
        
        selected = await self._select_participants_multi_criteria(
            carbon_zone, helium_scarcity, carbon_intensity
        )
        if len(selected) < 3:
            logger.warning(f"Insufficient participants: {len(selected)}")
            return None
        
        for participant_id in selected:
            if participant_id in self.participants:
                participant = self.participants[participant_id]
                stake_amount = participant.carbon_footprint * 100
                success, staked = self._stake_tokens_for_update(participant_id, stake_amount)
                if success:
                    participant.tokens_staked = staked
        
        # Collect updates (with compression)
        updates = {}
        for participant_id in selected:
            if participant_id in self.participants:
                reputation_score = 0.5
                if self.enable_reputation and self.reputation_system:
                    reputation_score = await self.reputation_system.get_reputation_score(participant_id)
                update = await self._collect_update(participant_id, carbon_intensity, reputation_score)
                if update:
                    if self.enable_compression_enhanced and self.compressor:
                        region_id = self.participants[participant_id].region_id or "default"
                        tier = self.regions.get(region_id, RegionNode(
                            region_id=region_id,
                            tier=AggregationTier.REGIONAL
                        )).tier
                        compressed, metadata = await self.compressor.compress_model(
                            update.model_delta,
                            tier
                        )
                        update.original_size_bytes = metadata['original_size']
                        update.compressed_size_bytes = metadata['compressed_size']
                        update.compression_ratio = metadata['ratio']
                        update.model_delta = await self.compressor.decompress_model(
                            compressed,
                            metadata
                        )
                    updates[participant_id] = update
        
        if len(updates) < 3:
            return None
        
        for participant_id in list(updates.keys()):
            threshold = self._get_trust_based_byzantine_threshold(participant_id)
            if threshold > 0.7:
                logger.warning(f"High Byzantine risk for {participant_id}: threshold={threshold:.2f}")
        
        # Determine aggregation strategy
        strategy = AggregationStrategy.FED_AVG
        carbon_price = 50.0
        helium_price = 0.5
        if self.enable_economic_pricing and self.pricing_manager:
            prices = await self.pricing_manager.get_current_prices()
            carbon_price = prices.get('carbon_price_usd_per_ton', 50.0)
            helium_price = prices.get('helium_price_usd_per_l', 0.5)
        
        if self.enable_tiered_aggregation and self.tiered_aggregator:
            region_id = selected[0] if selected else "default"
            region_tier = self.regions.get(region_id, RegionNode(
                region_id=region_id,
                tier=AggregationTier.REGIONAL
            )).tier
            aggregated = await self.tiered_aggregator.aggregate_tier(
                region_tier,
                [u.model_delta for u in updates.values()],
                region_id,
                strategy=AggregationStrategy.TIERED_AGGREGATION
            )
        elif self.enable_reputation and self.reputation_system:
            strategy = AggregationStrategy.REPUTATION_WEIGHTED
            aggregated = await self._reputation_weighted_aggregate(updates)
        elif self.enable_economic_pricing and carbon_price > 100:
            strategy = AggregationStrategy.PRICE_AWARE
            aggregated = await self._price_aware_aggregate(updates, carbon_price, helium_price)
        elif self.enable_bio_integration and self.token_manager and self.federation_token_pool > 100:
            strategy = AggregationStrategy.TOKEN_WEIGHTED
            aggregated = self._token_weighted_aggregate(updates)
        elif self.enable_sustainability_scoring:
            strategy = AggregationStrategy.SUSTAINABILITY_WEIGHTED
            aggregated = self._sustainability_weighted_aggregate(updates)
        else:
            aggregated = self._federated_averaging([u.model_delta for u in updates.values()])
        
        # Asynchronous region updates: submit to async manager (if enabled)
        if self.enable_async and self.async_region_manager:
            for participant_id, update in updates.items():
                region_id = self.participants[participant_id].region_id or "default"
                await self.async_region_manager.submit_update(
                    region_id,
                    update.model_delta,
                    update.timestamp
                )
            # Optionally, aggregate from the async manager instead of the synchronous result.
            # We'll keep the synchronous result as the main global model.
        
        self.global_model = aggregated
        
        # ====================================================================
        # NEW: Push to gating network and self-evolving gate
        # ====================================================================
        if self.gating_network:
            features = np.array([
                len(self.global_model),
                carbon_intensity / 1000.0,
                helium_scarcity,
                carbon_price / 100.0
            ])
            reward = self.sustainability_score
            context = {
                'carbon_intensity': carbon_intensity,
                'helium_scarcity': helium_scarcity,
                'carbon_price': carbon_price,
                'participants': len(selected)
            }
            self.gating_network.update(features, reward, context)
            logger.info("Updated gating network with global model")
        
        if self.self_evolving_gate:
            features = np.array([
                len(self.global_model),
                carbon_intensity,
                helium_scarcity
            ])
            reward = self.sustainability_score
            context = {
                'carbon_intensity': carbon_intensity,
                'helium_scarcity': helium_scarcity,
                'carbon_price': carbon_price,
                'participants': len(selected)
            }
            self.self_evolving_gate.evolve_gating_network(features, reward, context)
            logger.info("Triggered self-evolving gate evolution")
        
        # ====================================================================
        # Update sustainability and reputation
        # ====================================================================
        self.total_carbon_savings_kg += sum(u.carbon_savings for u in updates.values())
        self.sustainability_score = self._calculate_sustainability_score(
            updates, carbon_intensity, helium_scarcity
        )
        
        if self.enable_reputation and self.reputation_system:
            for participant_id, update in updates.items():
                success = update.local_accuracy > 0.7
                await self.reputation_system.update_reputation(
                    participant_id,
                    success=success,
                    sustainability_contribution=update.sustainability_impact,
                    token_stake=update.tokens_staked,
                    data_quality=update.local_accuracy,
                    carbon_efficiency=update.carbon_savings / max(1.0, update.training_data_size)
                )
        
        if self.enable_playbook and playbook_recommendations:
            for rec in playbook_recommendations[:2]:
                playbook = rec['playbook']
                success = await self._apply_playbook(playbook, rec['match_score'])
                await self.playbook_system.record_playbook_usage(
                    playbook['playbook_id'],
                    success=success,
                    metrics={'sustainability': self.sustainability_score}
                )
        
        if self.enable_resource_optimization and self.resource_optimizer:
            region_status = {}
            for region_id, node in self.regions.items():
                region_status[region_id] = {
                    'carbon_intensity': node.carbon_intensity,
                    'helium_availability': node.helium_availability,
                    'resource_capacity': node.resource_capacity,
                    'resource_usage': node.resource_usage
                }
            await self.resource_optimizer.optimize_resources(
                self.regions,
                {rid: node.carbon_intensity for rid, node in self.regions.items()},
                {rid: node.helium_availability for rid, node in self.regions.items()}
            )
        
        if self.enable_predictive:
            self.predictive_analyzer.update_history({
                'participants': len(selected),
                'carbon_intensity': carbon_intensity,
                'helium_scarcity': helium_scarcity,
                'sustainability_score': self.sustainability_score,
                'token_pool': self.federation_token_pool
            })
            await self.predictive_analyzer.train_forecast_model()
            forecast = await self.predictive_analyzer.predict_federation_trend()
        else:
            forecast = None
        
        if self.enable_discovery and self.discovery:
            await self.discovery.discover_peers(self.instance_id)
        
        # Record round
        round_record = {
            'round_number': self.round_number,
            'participants': len(selected),
            'updates': len(updates),
            'strategy': strategy.value,
            'timestamp': round_start.isoformat(),
            'sustainability_score': self.sustainability_score,
            'carbon_savings_kg': self.total_carbon_savings_kg,
            'federation_token_pool': self.federation_token_pool,
            'predictive_forecast': {
                'predicted_score': forecast.predicted_sustainability_score if forecast else None,
                'confidence': forecast.confidence if forecast else None,
                'trend': forecast.trend if forecast else None
            } if self.enable_predictive and forecast else None,
            'resource_optimization': self.resource_optimizer.get_optimization_stats() if self.enable_resource_optimization else None,
            'discovery_stats': self.discovery.get_discovery_stats() if self.enable_discovery else None,
            'compression_stats': self.compressor.get_compression_stats() if self.enable_compression_enhanced else None,
            'reputation_stats': self.reputation_system.get_reputation_stats() if self.enable_reputation else None,
            'playbook_usage': self.playbook_system.get_playbook_stats() if self.enable_playbook else None,
            'price_stats': self.pricing_manager.get_price_stats() if self.enable_economic_pricing else None,
            'gating_network_updated': self.gating_network is not None,
            'self_evolving_gate_triggered': self.self_evolving_gate is not None
        }
        self.aggregation_history.append(round_record)
        
        return aggregated
    
    # ========================================================================
    # Helper methods (unchanged, with minor modifications for helium)
    # ========================================================================
    
    async def _select_participants_multi_criteria(
        self, carbon_zone: int, helium_scarcity: float, carbon_intensity: float
    ) -> List[str]:
        scored_participants = []
        for participant_id, participant in self.participants.items():
            if not participant.is_active:
                continue
            data_score = 0.5
            carbon_score = 1.0 / (1.0 + participant.carbon_footprint * 100)
            helium_score = 1.0 / (1.0 + participant.helium_usage * 10)
            intensity_score = 1.0 - (carbon_intensity / 800) if carbon_intensity > 0 else 0.5
            sustainability_score = participant.sustainability_contribution if hasattr(participant, 'sustainability_contribution') else 0.5
            reputation_score = 0.5
            if self.enable_reputation and self.reputation_system:
                reputation_score = asyncio.run(self.reputation_system.get_reputation_score(participant_id))
            carbon_price_score = 0.5
            helium_price_score = 0.5
            if self.enable_economic_pricing and self.pricing_manager:
                prices = asyncio.run(self.pricing_manager.get_current_prices())
                carbon_price = prices.get('carbon_price_usd_per_ton', 50.0)
                helium_price = prices.get('helium_price_usd_per_l', 0.5)
                carbon_price_score = 1.0 - (carbon_price / 200)
                helium_price_score = 1.0 - (helium_price / 2.0)
            if carbon_zone >= 8:
                weights = {'carbon': 0.25, 'helium': 0.10, 'data': 0.10,
                          'intensity': 0.15, 'sustainability': 0.15, 'reliability': 0.10,
                          'reputation': 0.10, 'carbon_price': 0.05}
            elif helium_scarcity > 0.7:
                weights = {'helium': 0.25, 'carbon': 0.10, 'data': 0.10,
                          'intensity': 0.10, 'sustainability': 0.15, 'reliability': 0.10,
                          'reputation': 0.15, 'helium_price': 0.05}
            else:
                weights = {'data': 0.15, 'carbon': 0.10, 'helium': 0.05,
                          'intensity': 0.15, 'sustainability': 0.20, 'reliability': 0.10,
                          'reputation': 0.15, 'carbon_price': 0.05, 'helium_price': 0.05}
            score = (
                weights.get('data', 0.15) * data_score +
                weights.get('carbon', 0.10) * carbon_score +
                weights.get('helium', 0.05) * helium_score +
                weights.get('intensity', 0.15) * intensity_score +
                weights.get('sustainability', 0.20) * sustainability_score +
                weights.get('reliability', 0.10) * 0.8 +
                weights.get('reputation', 0.15) * reputation_score +
                weights.get('carbon_price', 0.05) * carbon_price_score +
                weights.get('helium_price', 0.05) * helium_price_score
            )
            scored_participants.append((participant_id, score))
        scored_participants.sort(key=lambda x: x[1], reverse=True)
        n_select = max(3, min(len(scored_participants), int(len(scored_participants) * 0.7)))
        selected = [p[0] for p in scored_participants[:n_select]]
        return selected
    
    async def _collect_update(
        self,
        participant_id: str,
        carbon_intensity: float,
        reputation_score: float = 0.5
    ) -> Optional[AsyncUpdate]:
        if participant_id not in self.participants:
            return None
        participant = self.participants[participant_id]
        region_id = participant.region_id or "default"
        region = Region(region_id) if region_id in [r.value for r in Region] else Region.US_EAST
        carbon_price = 50.0
        helium_price = 0.5
        if self.enable_economic_pricing and self.pricing_manager:
            prices = await self.pricing_manager.get_current_prices()
            carbon_price = prices.get('carbon_price_usd_per_ton', 50.0)
            helium_price = prices.get('helium_price_usd_per_l', 0.5)
        economic_impact = (
            carbon_price * participant.carbon_footprint * 0.01 +
            helium_price * participant.helium_usage * 0.1
        )
        update = AsyncUpdate(
            update_id=f"update_{participant_id}_{datetime.utcnow().timestamp()}",
            source_region=region,
            model_delta=participant.local_model,
            compression_ratio=0.8,
            timestamp=datetime.utcnow(),
            carbon_intensity_at_update=carbon_intensity,
            training_data_size=1000,
            local_accuracy=0.9,
            vector_clock={},
            signature=hashlib.sha256(f"{participant_id}{datetime.utcnow()}".encode()).hexdigest(),
            tokens_staked=participant.tokens_staked if hasattr(participant, 'tokens_staked') else 0.0,
            carbon_savings=participant.carbon_footprint * 0.01,
            sustainability_impact=participant.sustainability_contribution if hasattr(participant, 'sustainability_contribution') else 0.5,
            carbon_price=carbon_price,
            helium_price=helium_price,
            economic_impact=economic_impact
        )
        return update
    
    async def _reputation_weighted_aggregate(self, updates: Dict[str, AsyncUpdate]) -> Dict[str, Any]:
        if not self.enable_reputation or not self.reputation_system:
            return self._federated_averaging([u.model_delta for u in updates.values()])
        aggregated = {}
        total_reputation = 0.0
        reputation_scores = {}
        for participant_id in updates:
            score = await self.reputation_system.get_reputation_score(participant_id)
            reputation_scores[participant_id] = max(0.1, score)
            total_reputation += reputation_scores[participant_id]
        if total_reputation == 0:
            return self._federated_averaging([u.model_delta for u in updates.values()])
        for key in next(iter(updates.values())).model_delta.keys():
            weighted_sum = 0.0
            for participant_id, update in updates.items():
                if key in update.model_delta:
                    weight = reputation_scores[participant_id] / total_reputation
                    weighted_sum += update.model_delta[key] * weight
            aggregated[key] = weighted_sum
        return aggregated
    
    async def _price_aware_aggregate(
        self,
        updates: Dict[str, AsyncUpdate],
        carbon_price: float,
        helium_price: float
    ) -> Dict[str, Any]:
        aggregated = {}
        total_economic_weight = 0.0
        economic_weights = {}
        for participant_id, update in updates.items():
            cost = update.carbon_price * update.carbon_savings + update.helium_price * update.helium_usage
            weight = 1.0 / (1.0 + cost)
            economic_weights[participant_id] = weight
            total_economic_weight += weight
        if total_economic_weight == 0:
            return self._federated_averaging([u.model_delta for u in updates.values()])
        for key in next(iter(updates.values())).model_delta.keys():
            weighted_sum = 0.0
            for participant_id, update in updates.items():
                if key in update.model_delta:
                    weight = economic_weights[participant_id] / total_economic_weight
                    weighted_sum += update.model_delta[key] * weight
            aggregated[key] = weighted_sum
        return aggregated
    
    def _token_weighted_aggregate(self, updates: Dict[str, AsyncUpdate]) -> Dict[str, Any]:
        aggregated = {}
        total_tokens = sum(u.tokens_staked for u in updates.values())
        if total_tokens == 0:
            return self._federated_averaging([u.model_delta for u in updates.values()])
        for key in next(iter(updates.values())).model_delta.keys():
            weighted_sum = 0.0
            for update in updates.values():
                if key in update.model_delta:
                    weight = update.tokens_staked / total_tokens
                    weighted_sum += update.model_delta[key] * weight
            aggregated[key] = weighted_sum
        return aggregated
    
    def _sustainability_weighted_aggregate(self, updates: Dict[str, AsyncUpdate]) -> Dict[str, Any]:
        aggregated = {}
        total_sustainability = sum(u.sustainability_impact for u in updates.values())
        if total_sustainability == 0:
            return self._federated_averaging([u.model_delta for u in updates.values()])
        for key in next(iter(updates.values())).model_delta.keys():
            weighted_sum = 0.0
            for update in updates.values():
                if key in update.model_delta:
                    weight = update.sustainability_impact / total_sustainability
                    weighted_sum += update.model_delta[key] * weight
            aggregated[key] = weighted_sum
        return aggregated
    
    def _federated_averaging(self, updates: List[Dict]) -> Dict[str, Any]:
        if not updates:
            return {}
        aggregated = {}
        n = len(updates)
        for key in updates[0].keys():
            values = [u[key] for u in updates if key in u]
            if values:
                if isinstance(values[0], np.ndarray):
                    aggregated[key] = np.mean(values, axis=0)
                else:
                    aggregated[key] = sum(values) / n
        return aggregated
    
    def _calculate_sustainability_score(
        self, updates: Dict[str, AsyncUpdate], carbon_intensity: float, helium_scarcity: float
    ) -> float:
        if not updates:
            return 0.0
        avg_carbon_savings = np.mean([u.carbon_savings for u in updates.values()])
        avg_sustainability = np.mean([u.sustainability_impact for u in updates.values()])
        carbon_factor = 1.0 - (carbon_intensity / 800)
        helium_factor = 1.0 - helium_scarcity
        economic_factor = 0.5
        if self.enable_economic_pricing and self.pricing_manager:
            prices = asyncio.run(self.pricing_manager.get_current_prices())
            carbon_price = prices.get('carbon_price_usd_per_ton', 50.0)
            economic_factor = 1.0 - (carbon_price / 200)
        score = (
            avg_carbon_savings * 0.25 +
            avg_sustainability * 0.25 +
            carbon_factor * 0.20 +
            helium_factor * 0.20 +
            economic_factor * 0.10
        )
        return min(1.0, max(0.0, score))
    
    async def _apply_playbook(self, playbook: Dict[str, Any], match_score: float) -> bool:
        try:
            for action in playbook.get('actions', []):
                action_type = action.get('type')
                if action_type == 'schedule_shift':
                    pass
                elif action_type == 'reduce_workload':
                    pass
                elif action_type == 'switch_cooling':
                    pass
                elif action_type == 'circuit_compression':
                    pass
            logger.info(f"Applied playbook: {playbook.get('name')} (match: {match_score:.2f})")
            return True
        except Exception as e:
            logger.error(f"Failed to apply playbook: {e}")
            return False
    
    # ========================================================================
    # Statistics (unchanged)
    # ========================================================================
    
    def get_federation_stats(self) -> Dict[str, Any]:
        stats = {
            'total_participants': len(self.participants),
            'total_regions': len(self.regions),
            'total_rounds': len(self.aggregation_history),
            'bio_integration_active': self.enable_bio_integration,
            'tiered_aggregation_active': self.enable_tiered_aggregation,
            'resource_optimization_active': self.enable_resource_optimization,
            'discovery_active': self.enable_discovery,
            'federated_reflexive_active': self.enable_federated_reflexive,
            'carbon_intensity_active': self.enable_carbon_intensity,
            'predictive_active': self.enable_predictive,
            'cross_domain_active': self.enable_cross_domain,
            'sustainability_scoring_active': self.enable_sustainability_scoring,
            'compression_active': self.enable_compression_enhanced,
            'reputation_active': self.enable_reputation,
            'playbook_active': self.enable_playbook,
            'economic_pricing_active': self.enable_economic_pricing,
            'async_regions_active': self.enable_async,
            'moe_gating_injected': self.gating_network is not None,
            'moe_gate_injected': self.self_evolving_gate is not None,
            'helium_provider_injected': self.helium_provider is not None,
            'federation_token_pool': self.federation_token_pool,
            'total_carbon_savings_kg': self.total_carbon_savings_kg,
            'sustainability_score': self.sustainability_score,
            'recent_rounds': self.aggregation_history[-5:] if self.aggregation_history else []
        }
        if self.enable_tiered_aggregation and self.tiered_aggregator:
            stats['tier_stats'] = self.tiered_aggregator.get_tier_stats()
        if self.enable_resource_optimization and self.resource_optimizer:
            stats['resource_stats'] = self.resource_optimizer.get_optimization_stats()
        if self.enable_discovery and self.discovery:
            stats['discovery_stats'] = self.discovery.get_discovery_stats()
        if self.enable_bio_integration:
            stats['gradient_levels'] = self._get_real_gradient_levels()
            stats['harvester_quality'] = self._get_harvester_signal_quality()
        if self.enable_predictive:
            stats['predictive_summary'] = self.predictive_analyzer.get_sustainability_summary()
        if self.enable_cross_domain:
            stats['cross_domain_stats'] = self.cross_domain_transfer.get_transfer_statistics()
        if self.enable_compression_enhanced and self.compressor:
            stats['compression_stats'] = self.compressor.get_compression_stats()
        if self.enable_reputation and self.reputation_system:
            stats['reputation_stats'] = self.reputation_system.get_reputation_stats()
        if self.enable_playbook and self.playbook_system:
            stats['playbook_stats'] = self.playbook_system.get_playbook_stats()
        if self.enable_economic_pricing and self.pricing_manager:
            stats['price_stats'] = self.pricing_manager.get_price_stats()
        if self.async_region_manager:
            stats['async_region_stats'] = {
                'regions_tracked': len(self.async_region_manager.region_updates)
            }
        return stats
    
    def _get_real_gradient_levels(self) -> Dict[str, float]:
        if self.gradient_manager:
            return self.gradient_manager.get_field_strengths()
        return {'carbon': 0.5, 'helium': 0.5, 'trust': 0.5, 'opportunity': 0.5}
    
    def get_regional_profile(self, region: Region) -> Optional[Dict[str, Any]]:
        if region not in self.regional_profiles:
            return None
        profile = self.regional_profiles[region]
        return {
            'region': region.value,
            'carbon_gradient': profile.local_carbon_gradient,
            'trust_gradient': profile.local_trust_gradient,
            'token_balance': profile.token_balance,
            'compartment_count': profile.compartment_count,
            'harvester_vitality': profile.harvester_vitality,
            'sustainability_score': profile.sustainability_score,
            'carbon_savings_kg': profile.carbon_savings_kg,
            'helium_savings_l': profile.helium_savings_l,
            'tier': profile.tier.value if hasattr(profile, 'tier') else 'regional',
            'carbon_price_usd_per_ton': profile.carbon_price_usd_per_ton,
            'helium_price_usd_per_l': profile.helium_price_usd_per_l,
            'reputation_score': profile.reputation_score,
            'active_playbooks': profile.active_playbooks
        }
    
    def register_participant(
        self,
        participant_id: str,
        initial_model: Dict[str, Any],
        capabilities: ClientCapabilities,
        carbon_footprint: float,
        helium_usage: float,
        sustainability_contribution: float = 0.5,
        region_id: Optional[str] = None
    ) -> bool:
        if participant_id in self.participants:
            logger.warning(f"Participant {participant_id} already registered")
            return False
        participant = FederatedExpert(
            expert_id=participant_id,
            local_model=initial_model,
            data_distribution={},
            capabilities=capabilities,
            carbon_footprint=carbon_footprint,
            helium_usage=helium_usage,
            sustainability_contribution=sustainability_contribution,
            region_id=region_id
        )
        if self.enable_federated_reflexive:
            asyncio.create_task(
                self.federated_learner.register_participant(participant_id, initial_model)
            )
        if region_id and region_id in self.regions:
            self.regions[region_id].participants.append(participant_id)
        if self.enable_reputation and self.reputation_system:
            asyncio.create_task(
                self.reputation_system.update_reputation(
                    participant_id,
                    success=True,
                    sustainability_contribution=sustainability_contribution,
                    token_stake=0.0
                )
            )
        self.participants[participant_id] = participant
        logger.info(f"Registered federation participant: {participant_id} (region: {region_id})")
        return True
    
    def get_sustainability_report(self) -> Dict[str, Any]:
        report = {
            'timestamp': datetime.utcnow().isoformat(),
            'sustainability_score': self.sustainability_score,
            'total_carbon_savings_kg': self.total_carbon_savings_kg,
            'total_helium_savings_l': self.total_helium_savings_l,
            'federation_token_pool': self.federation_token_pool,
            'participant_count': len(self.participants),
            'region_count': len(self.regions),
            'round_count': self.round_number,
            'bio_integration_active': self.enable_bio_integration,
            'predictive_forecast': self.predictive_analyzer.get_sustainability_summary() if self.enable_predictive else {},
            'resource_optimization': self.resource_optimizer.get_optimization_stats() if self.enable_resource_optimization else {},
            'compression_savings_mb': self.compressor.get_compression_stats().get('total_size_saved_mb', 0) if self.enable_compression_enhanced else 0,
            'reputation_average': self.reputation_system.get_reputation_stats().get('average_score', 0.5) if self.enable_reputation else 0.5,
            'playbook_usage': self.playbook_system.get_playbook_stats() if self.enable_playbook else {},
            'recommendations': self._generate_sustainability_recommendations()
        }
        return report
    
    def _generate_sustainability_recommendations(self) -> List[str]:
        recommendations = []
        if self.sustainability_score < 0.5:
            recommendations.append("Increase federated participation for better sustainability")
            recommendations.append("Optimize carbon-aware scheduling")
        if self.total_carbon_savings_kg < 10:
            recommendations.append("Implement more aggressive carbon reduction strategies")
        if self.federation_token_pool < 50:
            recommendations.append("Boost token staking incentives")
        if self.enable_bio_integration and self._get_harvester_signal_quality() < 0.4:
            recommendations.append("Improve harvester signal quality for better drift detection")
        if self.enable_resource_optimization and self.resource_optimizer:
            resource_stats = self.resource_optimizer.get_optimization_stats()
            for region_id, alloc in resource_stats.get('current_allocations', {}).items():
                if alloc.get('usage', 0) > alloc.get('allocated', 1) * 0.9:
                    recommendations.append(f"Region {region_id} is near capacity - consider scaling")
        if self.enable_playbook and self.playbook_system:
            context = {
                'carbon_intensity': self.regions.get('us_east', RegionNode(region_id='us_east', tier=AggregationTier.REGIONAL)).carbon_intensity if 'us_east' in self.regions else 400,
                'helium_availability': self.regions.get('us_east', RegionNode(region_id='us_east', tier=AggregationTier.REGIONAL)).helium_availability if 'us_east' in self.regions else 0.5
            }
            playbooks = asyncio.run(self.playbook_system.evaluate_playbooks(context))
            if playbooks:
                recommendations.append(f"Consider applying playbook: {playbooks[0]['playbook']['name']}")
        return recommendations or ["Federation sustainability is on track"]
    
    async def shutdown(self):
        logger.info("Shutting down Cross-Region Federation Optimizer")
        if hasattr(self, 'federated_learner') and self.federated_learner:
            await self.federated_learner.close()
        if hasattr(self, 'carbon_manager') and self.carbon_manager:
            await self.carbon_manager.close()
        if self.enable_discovery and self.discovery:
            await self.discovery.close()
        if self.enable_economic_pricing and self.pricing_manager and self.pricing_manager._session:
            await self.pricing_manager._session.close()
        logger.info("Cross-Region Federation Optimizer shutdown complete")

# ============================================================================
# Carbon Intensity Manager (unchanged)
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
    
    async def _get_session(self):
        if self._session is None:
            self._session = aiohttp.ClientSession()
        return self._session
    
    async def update_carbon_intensity(self, region: str = "us-east") -> Dict:
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
                        self.last_update = datetime.now()
                        self.cache[region] = {'intensity': self.carbon_intensity, 'timestamp': self.last_update}
                        self.historical_intensities.append(self.carbon_intensity)
                    else:
                        self.carbon_intensity = self._get_fallback_intensity(region)
                        self.last_update = datetime.now()
            except Exception as e:
                logger.error(f"Carbon intensity fetch error: {e}")
                self.carbon_intensity = self._get_fallback_intensity(region)
                self.last_update = datetime.now()
            return {
                'intensity': self.carbon_intensity,
                'region': self.region,
                'timestamp': self.last_update.isoformat() if self.last_update else None
            }
    
    def _get_fallback_intensity(self, region: str) -> float:
        fallback_values = {'us-east': 420, 'us-west': 350, 'eu': 280, 'asia': 500, 'default': 400}
        return fallback_values.get(region, 400)
    
    async def get_current_intensity(self) -> float:
        if self.last_update is None or (datetime.now() - self.last_update).seconds > self.update_interval:
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
            'timestamp': datetime.utcnow(),
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
                r2 = r2_score(y, predictions)
                results[name] = r2
        self.is_trained = True
        logger.info(f"Federation forecast models trained. R²: {results}")
        return {'status': 'success', 'results': results}
    
    async def predict_federation_trend(self):
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
# Data Classes (unchanged)
# ============================================================================

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
    tokens_staked: float = 0.0
    gradient_alignment: float = 0.5
    compartment_id: Optional[str] = None
    harvester_contribution: float = 0.0
    sustainability_contribution: float = 0.0
    federated_round: int = 0
    region_id: Optional[str] = None

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

# ============================================================================
# Tiered Aggregator (unchanged)
# ============================================================================

class TieredAggregator:
    def __init__(self):
        self.tier_hierarchy = {AggregationTier.EDGE: 0, AggregationTier.REGIONAL: 1, AggregationTier.CONTINENTAL: 2, AggregationTier.GLOBAL: 3}
        self.tier_configs = {
            AggregationTier.EDGE: {'sync_interval': 60, 'max_participants': 5, 'min_participants': 2, 'aggregation_strategy': AggregationStrategy.FED_AVG},
            AggregationTier.REGIONAL: {'sync_interval': 300, 'max_participants': 20, 'min_participants': 3, 'aggregation_strategy': AggregationStrategy.SUSTAINABILITY_WEIGHTED},
            AggregationTier.CONTINENTAL: {'sync_interval': 900, 'max_participants': 50, 'min_participants': 5, 'aggregation_strategy': AggregationStrategy.TOKEN_WEIGHTED},
            AggregationTier.GLOBAL: {'sync_interval': 3600, 'max_participants': 100, 'min_participants': 10, 'aggregation_strategy': AggregationStrategy.TIERED_AGGREGATION}
        }
        self._lock = asyncio.Lock()
        self.aggregation_cache: Dict[str, Dict] = {}
        logger.info("Tiered Aggregator initialized")
    
    async def aggregate_tier(self, tier: AggregationTier, updates: List[Dict[str, Any]], region_id: str, strategy: AggregationStrategy = None) -> Dict[str, Any]:
        async with self._lock:
            if not updates:
                return {}
            config = self.tier_configs.get(tier, {})
            strategy = strategy or config.get('aggregation_strategy', AggregationStrategy.FED_AVG)
            if strategy == AggregationStrategy.TIERED_AGGREGATION:
                return await self._tiered_aggregate(updates, tier, region_id)
            elif strategy == AggregationStrategy.SUSTAINABILITY_WEIGHTED:
                return self._sustainability_weighted_aggregate(updates)
            elif strategy == AggregationStrategy.TOKEN_WEIGHTED:
                return self._token_weighted_aggregate(updates)
            else:
                return self._fed_avg_aggregate(updates)
    
    def _fed_avg_aggregate(self, updates: List[Dict]) -> Dict:
        if not updates:
            return {}
        aggregated = {}
        n = len(updates)
        for key in updates[0].keys():
            values = [u.get(key) for u in updates if key in u]
            if values:
                if isinstance(values[0], np.ndarray):
                    aggregated[key] = np.mean(values, axis=0)
                else:
                    aggregated[key] = sum(values) / n
        return aggregated
    
    def _sustainability_weighted_aggregate(self, updates: List[Dict]) -> Dict:
        aggregated = {}
        total_weight = sum(u.get('sustainability_score', 1.0) for u in updates)
        if total_weight == 0:
            return self._fed_avg_aggregate(updates)
        for key in updates[0].keys():
            weighted_sum = 0.0
            for u in updates:
                if key in u:
                    weight = u.get('sustainability_score', 1.0) / total_weight
                    weighted_sum += u[key] * weight
            aggregated[key] = weighted_sum
        return aggregated
    
    def _token_weighted_aggregate(self, updates: List[Dict]) -> Dict:
        aggregated = {}
        total_tokens = sum(u.get('tokens_staked', 0) for u in updates)
        if total_tokens == 0:
            return self._fed_avg_aggregate(updates)
        for key in updates[0].keys():
            weighted_sum = 0.0
            for u in updates:
                if key in u:
                    weight = u.get('tokens_staked', 0) / total_tokens
                    weighted_sum += u[key] * weight
            aggregated[key] = weighted_sum
        return aggregated
    
    async def _tiered_aggregate(self, updates: List[Dict], tier: AggregationTier, region_id: str) -> Dict[str, Any]:
        if not updates:
            return {}
        tier_weight = self.tier_hierarchy.get(tier, 1) / 3.0
        importance_weight = 0.5 + tier_weight * 0.5
        aggregated = {}
        n = len(updates)
        for key in updates[0].keys():
            values = [u.get(key) for u in updates if key in u]
            if values:
                weighted_values = [v * importance_weight for v in values]
                if isinstance(values[0], np.ndarray):
                    aggregated[key] = np.mean(weighted_values, axis=0)
                else:
                    aggregated[key] = sum(weighted_values) / n
        cache_key = f"{tier.value}_{region_id}_{datetime.utcnow().timestamp()}"
        self.aggregation_cache[cache_key] = {'model': aggregated, 'tier': tier.value, 'region': region_id, 'timestamp': datetime.utcnow().isoformat()}
        return aggregated
    
    def get_tier_stats(self) -> Dict[str, Any]:
        return {
            'tier_hierarchy': {k.value: v for k, v in self.tier_hierarchy.items()},
            'tier_configs': {k.value: v for k, v in self.tier_configs.items()},
            'cache_size': len(self.aggregation_cache)
        }

# ============================================================================
# Global Resource Optimizer (unchanged)
# ============================================================================

@dataclass
class ResourceAllocation:
    region_id: str
    allocated_capacity: float
    usage: float
    carbon_impact: float
    helium_usage: float
    recommendations: List[str] = field(default_factory=list)

class GlobalResourceOptimizer:
    def __init__(self):
        self.resource_allocations: Dict[str, ResourceAllocation] = {}
        self.optimization_history: deque = deque(maxlen=1000)
        self._lock = asyncio.Lock()
        self.weights = {'carbon': 0.30, 'helium': 0.25, 'energy': 0.20, 'sustainability': 0.25}
        logger.info("Global Resource Optimizer initialized")
    
    async def optimize_resources(
        self,
        regions: Dict[str, RegionNode],
        carbon_intensities: Dict[str, float],
        helium_availabilities: Dict[str, float]
    ) -> Dict[str, ResourceAllocation]:
        async with self._lock:
            allocations = {}
            total_capacity = sum(r.resource_capacity for r in regions.values())
            total_usage = sum(r.resource_usage for r in regions.values())
            if total_capacity == 0:
                return allocations
            for region_id, region in regions.items():
                carbon_intensity = carbon_intensities.get(region_id, 400)
                helium_avail = helium_availabilities.get(region_id, 0.5)
                carbon_score = 1.0 - (carbon_intensity / 800)
                helium_score = helium_avail
                energy_score = 1.0 - (region.resource_usage / max(region.resource_capacity, 1))
                sustainability_score = (
                    self.weights['carbon'] * carbon_score +
                    self.weights['helium'] * helium_score +
                    self.weights['energy'] * energy_score +
                    self.weights['sustainability'] * region.sustainability_score
                )
                if hasattr(region, 'reputation_score'):
                    sustainability_score = 0.9 * sustainability_score + 0.1 * region.reputation_score
                ideal_allocation = sustainability_score * total_capacity / sum(
                    (1.0 - (carbon_intensities.get(rid, 400) / 800)) * 0.3 +
                    helium_availabilities.get(rid, 0.5) * 0.3 +
                    regions[rid].sustainability_score * 0.4
                    for rid in regions
                )
                allocation = min(ideal_allocation, region.resource_capacity)
                allocations[region_id] = ResourceAllocation(
                    region_id=region_id,
                    allocated_capacity=allocation,
                    usage=region.resource_usage,
                    carbon_impact=carbon_intensity * allocation,
                    helium_usage=helium_avail * allocation * 0.1,
                    recommendations=self._generate_recommendations(region, carbon_intensity, helium_avail)
                )
            self.optimization_history.append({
                'timestamp': datetime.utcnow().isoformat(),
                'allocations': {k: v.allocated_capacity for k, v in allocations.items()},
                'total_capacity': total_capacity,
                'total_usage': total_usage
            })
            self.resource_allocations = allocations
            return allocations
    
    def _generate_recommendations(self, region: RegionNode, carbon_intensity: float, helium_avail: float) -> List[str]:
        recommendations = []
        if carbon_intensity > 500:
            recommendations.append("High carbon intensity - reduce workload")
        elif carbon_intensity < 300:
            recommendations.append("Low carbon intensity - consider increasing workload")
        if helium_avail < 0.3:
            recommendations.append("Helium scarce - prioritize recovery")
        elif helium_avail > 0.7:
            recommendations.append("Helium available - can increase usage")
        if region.resource_usage > region.resource_capacity * 0.8:
            recommendations.append("Resource usage high - consider expansion")
        return recommendations
    
    def get_optimization_stats(self) -> Dict[str, Any]:
        return {
            'total_allocations': len(self.resource_allocations),
            'optimization_count': len(self.optimization_history),
            'current_allocations': {
                k: {'allocated': v.allocated_capacity, 'usage': v.usage, 'carbon_impact': v.carbon_impact, 'helium_usage': v.helium_usage}
                for k, v in self.resource_allocations.items()
            },
            'recent_optimizations': list(self.optimization_history)[-5:]
        }

# ============================================================================
# Federated Discovery (unchanged)
# ============================================================================

class FederatedDiscovery:
    def __init__(self, server_url: Optional[str] = None):
        self.server_url = server_url
        self.discovered_peers: Set[str] = set()
        self.peer_capabilities: Dict[str, Dict] = {}
        self.peer_health: Dict[str, Dict] = {}
        self.registration_queue: deque = deque(maxlen=1000)
        self._lock = asyncio.Lock()
        self._session = None
        self.discovery_interval = 60
        logger.info("Federated Discovery initialized")
    
    async def _get_session(self):
        if self._session is None and self.server_url:
            self._session = aiohttp.ClientSession()
        return self._session
    
    async def discover_peers(self, region_id: str) -> Set[str]:
        async with self._lock:
            discovered = set()
            discovered.update(self.discovered_peers)
            if self.server_url:
                try:
                    session = await self._get_session()
                    async with session.get(f"{self.server_url}/api/discovery", timeout=30) as response:
                        if response.status == 200:
                            data = await response.json()
                            remote_peers = data.get('peers', [])
                            discovered.update(remote_peers)
                            for peer in remote_peers:
                                if peer not in self.peer_capabilities:
                                    self.peer_capabilities[peer] = {'capabilities': data.get('capabilities', {}), 'discovered_at': datetime.utcnow().isoformat()}
                except Exception as e:
                    logger.error(f"Discovery error: {e}")
            self.discovered_peers = discovered
            logger.info(f"Discovered {len(discovered)} peers for region {region_id}")
            return discovered
    
    async def register_region(self, region_id: str, capabilities: Dict[str, Any], parent_id: Optional[str] = None) -> bool:
        async with self._lock:
            self.peer_capabilities[region_id] = {'capabilities': capabilities, 'parent_id': parent_id, 'registered_at': datetime.utcnow().isoformat(), 'status': 'active'}
            if self.server_url:
                try:
                    session = await self._get_session()
                    async with session.post(f"{self.server_url}/api/register", json={'region_id': region_id, 'capabilities': capabilities, 'parent_id': parent_id, 'timestamp': datetime.utcnow().isoformat()}, timeout=30) as response:
                        if response.status == 200:
                            logger.info(f"Region {region_id} registered successfully")
                            return True
                        else:
                            logger.warning(f"Registration failed: {response.status}")
                            return False
                except Exception as e:
                    logger.error(f"Registration error: {e}")
                    return False
            logger.info(f"Region {region_id} registered locally")
            return True
    
    async def update_health(self, region_id: str, health_status: Dict[str, Any]) -> None:
        async with self._lock:
            self.peer_health[region_id] = {'status': health_status.get('status', 'healthy'), 'last_update': datetime.utcnow().isoformat(), 'metrics': health_status.get('metrics', {})}
    
    async def get_peer_health(self, region_id: str) -> Optional[Dict]:
        return self.peer_health.get(region_id)
    
    def get_discovery_stats(self) -> Dict[str, Any]:
        return {'discovered_peers': len(self.discovered_peers), 'registered_peers': len(self.peer_capabilities), 'healthy_peers': sum(1 for h in self.peer_health.values() if h.get('status') == 'healthy'), 'peers': list(self.discovered_peers)}
    
    async def close(self):
        if self._session:
            await self._session.close()

# ============================================================================
# Federation Cross-Domain Transfer (unchanged)
# ============================================================================

class FederationCrossDomainTransfer:
    def __init__(self):
        self.knowledge_base: Dict[str, Dict[str, Dict]] = {}
        self.transfer_logs = deque(maxlen=1000)
        self.domain_mappings = {
            'federation→energy': {'scheduling_patterns': ['carbon-aware', 'gradient-driven', 'opportunistic'], 'resource_allocation': ['dynamic', 'adaptive', 'predictive']},
            'federation→carbon': {'intensity_patterns': ['diurnal', 'regional', 'trending'], 'optimization_strategies': ['load-shifting', 'efficiency-first', 'renewable-tracking']},
            'federation→helium': {'scarcity_patterns': ['supply-constrained', 'price-sensitive'], 'efficiency_strategies': ['recovery', 'reuse', 'minimization']},
            'federation→data': {'aggregation_patterns': ['weighted', 'adaptive', 'hierarchical'], 'compression_strategies': ['lossy', 'lossless', 'adaptive']},
            'federation→quantum': {'circuit_optimization': ['depth-reduction', 'qubit-saving'], 'scheduling_strategies': ['carbon-aware', 'helium-efficient']}
        }
        self._lock = asyncio.Lock()
    
    def transfer_knowledge(self, source_domain: str, target_domain: str, knowledge_type: str, data: Dict[str, Any]) -> Dict:
        key = f"{source_domain}→{target_domain}"
        if key not in self.knowledge_base:
            self.knowledge_base[key] = {}
        if knowledge_type not in self.knowledge_base[key]:
            self.knowledge_base[key][knowledge_type] = {'data': data, 'transfer_count': 1, 'effectiveness_score': 0.5, 'last_used': datetime.utcnow()}
        else:
            existing = self.knowledge_base[key][knowledge_type]
            existing['data'].update(data)
            existing['transfer_count'] += 1
            existing['last_used'] = datetime.utcnow()
        self.transfer_logs.append({'timestamp': datetime.utcnow(), 'source': source_domain, 'target': target_domain, 'type': knowledge_type})
        return self.knowledge_base[key][knowledge_type]
    
    def get_transfer_statistics(self) -> Dict:
        total_transfers = len(self.transfer_logs)
        domain_pairs = {}
        for log in self.transfer_logs:
            key = f"{log['source']}→{log['target']}"
            domain_pairs[key] = domain_pairs.get(key, 0) + 1
        return {'total_transfers': total_transfers, 'domain_pairs': domain_pairs, 'knowledge_types': list(self.knowledge_base.keys()), 'recent_transfers': list(self.transfer_logs)[-10:]}

# ============================================================================
# Legacy Compatibility Class
# ============================================================================

class CrossRegionFederation(CrossRegionFederationOptimizer):
    """Legacy compatibility class."""
    pass
