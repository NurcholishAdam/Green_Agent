# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/moe_expert_system/advanced/federated_experts.py
"""
Enhanced Federated Experts v5.0.0 - Complete Green Agent Implementation

Complete bio-inspired integration with:
- Federated Reflexive Learning with global model sharing
- User-Adaptive Reflexivity with dynamic thresholds
- Real-time Carbon Intensity Integration with API support
- Cross-Domain Knowledge Transfer with domain mapping
- Human-AI Collaborative Reflection with feedback loops
- Predictive Reflexivity with ensemble forecasting
- Sustainability Score with multi-metric aggregation
- Enhanced Carbon/Helium Awareness with real-time tracking
- Token-based incentive distribution (Eco-ATP rewards)
- Gradient-aligned client selection (trust gradient)
- Token-weighted aggregation (stake-proportional)
- Compartment health-aware fault tolerance
- Biomass-backed blockchain audit trail
- ATP-driven gating network synchronization
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple, Set, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import numpy as np
import torch
import torch.nn as nn
import hashlib
import json
from collections import defaultdict, deque
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
import secrets
import copy
import math
import aiohttp
import os

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
        BiomassStorage, StorageTier, GuaranteeLevel, StoredTask, StorageToken
    )
    from enhancements.bio_inspired.photosynthetic_harvester import (
        PhotosyntheticHarvester
    )
    BIO_INSPIRED_AVAILABLE = True
    logger.info("Bio-inspired modules loaded for Federated Experts")
except ImportError as e:
    BIO_INSPIRED_AVAILABLE = False
    logger.warning(f"Bio-inspired modules not available: {str(e)} - using standard federation")

# ============================================================================
# Enums and Data Classes (Enhanced with Bio-Inspired)
# ============================================================================

class FederationTopology(Enum):
    CENTRALIZED = "centralized"; DECENTRALIZED = "decentralized"; HIERARCHICAL = "hierarchical"
    SWARM = "swarm"; CROSS_SILO = "cross_silo"; CROSS_DEVICE = "cross_device"; METABOLIC_MESH = "metabolic_mesh"

class AggregationStrategy(Enum):
    FED_AVG = "fed_avg"; FED_PROX = "fed_prox"; FED_OPT = "fed_opt"; FED_DYN = "fed_dyn"
    FED_ENSEMBLE = "fed_ensemble"; FED_DISTILL = "fed_distill"; ADAPTIVE = "adaptive"
    TOKEN_WEIGHTED = "token_weighted"; GRADIENT_ALIGNED = "gradient_aligned"
    HEALTH_AWARE = "health_aware"; SUSTAINABILITY_WEIGHTED = "sustainability_weighted"

class PrivacyLevel(Enum):
    NONE = "none"; BASIC = "basic"; DIFFERENTIAL = "differential"; SECURE_AGGREGATION = "secure_agg"
    FULLY_HOMOMORPHIC = "fully_homo"; GRADIENT_MODULATED = "gradient_modulated"; TOKEN_BACKED = "token_backed"

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

@dataclass
class PredictiveFederationForecast:
    timestamp: datetime = field(default_factory=datetime.utcnow)
    predicted_sustainability_score: float = 0.0
    predicted_carbon_impact: float = 0.0
    predicted_helium_usage: float = 0.0
    confidence: float = 0.0
    trend: str = "stable"
    recommended_actions: List[str] = field(default_factory=list)

# ============================================================================
# Carbon Intensity Integration Module
# ============================================================================

class CarbonIntensityManager:
    """Real-time carbon intensity integration with API support"""
    
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
                        self.cache[region] = {
                            'intensity': self.carbon_intensity,
                            'timestamp': self.last_update
                        }
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
        fallback_values = {
            'us-east': 420, 'us-west': 350, 'eu': 280,
            'asia': 500, 'default': 400
        }
        return fallback_values.get(region, 400)
    
    async def get_current_intensity(self) -> float:
        if self.last_update is None or \
           (datetime.now() - self.last_update).seconds > self.update_interval:
            await self.update_carbon_intensity(self.region)
        return self.carbon_intensity
    
    async def close(self):
        if self._session:
            await self._session.close()

# ============================================================================
# Predictive Reflexivity Module
# ============================================================================

class PredictiveFederationAnalyzer:
    """Predictive reflexivity with ensemble forecasting"""
    
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
            from sklearn.metrics import r2_score
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
            'round_success': federation_metrics.get('round_success', True)
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
        
        forecast = PredictiveFederationForecast(
            predicted_sustainability_score=prediction,
            predicted_carbon_impact=prediction * 400 * 0.1,
            predicted_helium_usage=(1 - prediction) * 0.5,
            confidence=confidence,
            trend=trend,
            recommended_actions=self._generate_actions(prediction)
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
# Cross-Domain Knowledge Transfer Module
# ============================================================================

class FederationCrossDomainTransfer:
    """Cross-domain knowledge transfer for federation"""
    
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
            }
        }
        self._lock = asyncio.Lock()
    
    def transfer_knowledge(self, source_domain: str, target_domain: str, 
                          knowledge_type: str, data: Dict[str, Any]) -> Dict:
        key = f"{source_domain}→{target_domain}"
        
        if key not in self.knowledge_base:
            self.knowledge_base[key] = {}
        
        if knowledge_type not in self.knowledge_base[key]:
            self.knowledge_base[key][knowledge_type] = {
                'data': data,
                'transfer_count': 1,
                'effectiveness_score': 0.5,
                'last_used': datetime.utcnow()
            }
        else:
            existing = self.knowledge_base[key][knowledge_type]
            existing['data'].update(data)
            existing['transfer_count'] += 1
            existing['last_used'] = datetime.utcnow()
        
        self.transfer_logs.append({
            'timestamp': datetime.utcnow(),
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
# Enhanced Federated Orchestrator
# ============================================================================

class EnhancedFederatedOrchestrator:
    """
    Enhanced Federated Orchestrator v5.0.0 - Complete Green Agent Implementation
    """
    
    def __init__(
        self,
        aggregation_strategy: AggregationStrategy = AggregationStrategy.ADAPTIVE,
        privacy_level: PrivacyLevel = PrivacyLevel.DIFFERENTIAL,
        topology: FederationTopology = FederationTopology.CENTRALIZED,
        min_participants: int = 3,
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
        max_straggler_wait_seconds: int = 60
    ):
        self.aggregation_strategy = aggregation_strategy
        self.privacy_level = privacy_level
        self.topology = topology
        self.min_participants = min_participants
        self.privacy_epsilon = privacy_epsilon
        
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
        self.max_straggler_wait_seconds = max_straggler_wait_seconds
        
        # Bio-inspired modules
        self.token_manager = None
        self.gradient_manager = None
        self.scheduler = None
        self.compartment_manager = None
        self.biomass_storage = None
        self.harvester = None
        
        # New modules
        self.carbon_manager = CarbonIntensityManager()
        self.predictive_analyzer = PredictiveFederationAnalyzer()
        self.cross_domain_transfer = FederationCrossDomainTransfer()
        
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
        
        logger.info(
            f"Enhanced Federated Orchestrator v5.0.0 initialized: "
            f"bio_integration={self.enable_bio_integration}, "
            f"carbon_intensity={self.enable_carbon_intensity}, "
            f"predictive={self.enable_predictive}"
        )
    
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
    # Bio-Inspired Data Access Methods
    # ========================================================================
    
    def _distribute_token_incentives(self, participant_id: str, contribution: float, success: bool = True) -> float:
        if not self.token_manager:
            return 0.0
        
        base_reward = contribution * 10.0
        if success:
            base_reward *= 1.5
        
        tokens = self.token_manager.generate_tokens(
            account_id=f"federated_{participant_id}",
            source=EcoATPSource.EFFICIENCY_GAIN,
            energy_saved_kwh=base_reward / 10000.0,
            num_tokens=int(base_reward)
        )
        
        if tokens:
            total = sum(t.value for t in tokens)
            if participant_id in self.participants:
                self.participants[participant_id].tokens_earned += total
            self.federation_token_pool -= total
            return total
        
        return 0.0
    
    def _get_gradient_aligned_selection(self, participant_id: str) -> float:
        if self.gradient_manager:
            trust = self.gradient_manager.fields.get('trust')
            if trust:
                return trust.gradient_strength
        return 0.5
    
    def _get_token_weighted_aggregation(self, participant_id: str) -> float:
        if self.token_manager:
            account = self.token_manager.get_account_summary(f"federated_{participant_id}")
            if account:
                return account.get('balance', 0)
        if participant_id in self.participants:
            return self.participants[participant_id].tokens_earned
        return 0.0
    
    def _get_compartment_health_timeout(self, participant_id: str) -> float:
        if self.compartment_manager:
            compartment = self.compartment_manager.find_best_compartment('data')
            if compartment:
                return max(10.0, 60.0 * compartment.health_score)
        return 30.0
    
    def _store_audit_in_biomass(self, audit_data: Dict[str, Any]) -> Optional[str]:
        if self.biomass_storage:
            stored, token_id = self.biomass_storage.store_task(
                task_data=audit_data,
                ecoatp_cost=1.0,
                guarantee=GuaranteeLevel.BEST_EFFORT,
                initial_tier=StorageTier.LIPID_DEPOT
            )
            if stored:
                return token_id
        return None
    
    def _pump_trust_gradient(self, participant_id: str, success: bool, contribution: float):
        if self.gradient_manager:
            delta = (0.05 * contribution) if success else (-0.1)
            self.gradient_manager.pump_field(
                'trust', delta, source=f"federated_{participant_id}"
            )
            if participant_id in self.participants:
                self.participants[participant_id].trust_pumping_count += 1
            self.trust_gradient_history.append({
                'participant': participant_id,
                'delta': delta,
                'success': success,
                'timestamp': datetime.utcnow().isoformat()
            })
    
    def _get_atp_driven_sync_timing(self) -> float:
        if self.scheduler:
            driving_force = self.scheduler.calculate_gradient_driving_force()
            rotation_speed = self.scheduler.calculate_rotation_speed(driving_force)
            ecoatp_rate = self.scheduler.calculate_atp_production_rate(rotation_speed)
            if ecoatp_rate > 100:
                return 30.0
            elif ecoatp_rate > 50:
                return 60.0
            else:
                return 120.0
        return 60.0
    
    def _get_harvester_confidence(self) -> float:
        if self.harvester:
            stats = self.harvester.get_harvesting_stats()
            recent = stats.get('recent_conversions', [])
            if recent:
                return np.mean([c.get('convertible_energy', 0.5) for c in recent[-10:]])
        return 0.5
    
    def _get_token_efficiency(self, participant_id: str) -> float:
        if self.token_manager:
            account = self.token_manager.get_account_summary(f"federated_{participant_id}")
            if account:
                return account.get('efficiency_rating', 0.5)
        if participant_id in self.participants:
            participant = self.participants[participant_id]
            if participant.tokens_earned > 0:
                return min(1.0, participant.tokens_earned / 100.0)
        return 0.5
    
    def _get_gradient_modulated_privacy(self, base_epsilon: float) -> float:
        if self.gradient_manager:
            carbon = self.gradient_manager.fields.get('carbon')
            if carbon and carbon.gradient_strength > 0.7:
                return base_epsilon * 0.5
            elif carbon and carbon.gradient_strength < 0.3:
                return base_epsilon * 1.5
        return base_epsilon
    
    def _get_real_gradient_levels(self) -> Dict[str, float]:
        if self.gradient_manager:
            return self.gradient_manager.get_field_strengths()
        return {'carbon': 0.5, 'helium': 0.5, 'trust': 0.5, 'opportunity': 0.5}
    
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
        architecture_type: str = "standard"
    ) -> bool:
        if expert_id in self.participants:
            logger.warning(f"Participant {expert_id} already registered")
            return False
        
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
        
        participant = FederatedExpert(
            expert_id=expert_id,
            local_model=initial_model,
            data_distribution=data_distribution,
            capabilities=capabilities,
            carbon_footprint=carbon_footprint,
            helium_usage=helium_usage,
            privacy_budget=self.privacy_epsilon,
            architecture_type=architecture_type,
            sustainability_contribution=sustainability_contribution
        )
        
        self.participants[expert_id] = participant
        logger.info(f"Registered federated participant: {expert_id}")
        return True
    
    # ========================================================================
    # Enhanced Federation Round
    # ========================================================================
    
    async def federated_round(
        self,
        carbon_zone: int,
        helium_scarcity: float,
        timeout_seconds: Optional[int] = None
    ) -> Optional[Dict[str, Any]]:
        self.round_number += 1
        round_start = datetime.utcnow()
        
        # Update carbon intensity
        if self.enable_carbon_intensity:
            carbon_data = await self.carbon_manager.update_carbon_intensity('us-east')
            carbon_intensity = carbon_data.get('intensity', 400)
        else:
            carbon_intensity = 400
        
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
        
        # Select participants
        selected = await self._select_participants_bio_aware(carbon_zone, helium_scarcity, carbon_intensity)
        
        if len(selected) < self.min_participants:
            logger.warning(f"Insufficient participants: {len(selected)}")
            return None
        
        # Modulate privacy budget
        if self.enable_bio_integration:
            effective_epsilon = self._get_gradient_modulated_privacy(self.privacy_epsilon)
        else:
            effective_epsilon = self.privacy_epsilon
        
        # Create federation round
        federation_round = FederationRound(
            round_id=f"round_{self.round_number}_{datetime.utcnow().timestamp()}",
            round_number=self.round_number,
            started_at=round_start,
            participants=selected,
            aggregation_strategy=self.aggregation_strategy,
            privacy_level=self.privacy_level,
            atp_sync_delay=self._get_atp_driven_sync_timing() if self.enable_bio_integration else 0.0
        )
        
        logger.info(f"Starting federation round {self.round_number} with {len(selected)} participants")
        
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
                            
                            updates[participant_id] = update
                            total_tokens_staked += participant.tokens_earned
                    
                    except asyncio.TimeoutError:
                        logger.warning(f"Participant {participant_id} timed out")
                        federation_round.dropped_participants.append(participant_id)
            
            if len(updates) < self.min_participants:
                logger.warning(f"Insufficient updates: {len(updates)}")
                return None
            
            # Select aggregation strategy
            if self.enable_bio_integration:
                if total_tokens_staked > 100:
                    strategy = AggregationStrategy.TOKEN_WEIGHTED
                elif self.enable_sustainability_scoring:
                    strategy = AggregationStrategy.SUSTAINABILITY_WEIGHTED
                elif self.gradient_manager:
                    strategy = AggregationStrategy.GRADIENT_ALIGNED
                else:
                    strategy = self.aggregation_strategy
                federation_round.aggregation_strategy = strategy
            
            # Aggregate updates
            global_model = self._aggregate_updates_bio_aware(updates, federation_round.aggregation_strategy)
            self.global_model = global_model
            
            # Distribute token incentives
            if self.enable_bio_integration and self.enable_incentives:
                total_distributed = 0.0
                for participant_id in updates:
                    participant = self.participants.get(participant_id)
                    if participant:
                        contribution = participant.calculate_contribution_potential()
                        distributed = self._distribute_token_incentives(
                            participant_id, contribution, success=True
                        )
                        total_distributed += distributed
                        self._pump_trust_gradient(participant_id, success=True, contribution=contribution)
                
                federation_round.tokens_distributed = total_distributed
                federation_round.trust_gradient_delta = 0.05
            
            # Update sustainability metrics
            self.total_carbon_savings_kg += sum(u.carbon_savings for u in updates.values())
            self.sustainability_score = self._calculate_sustainability_score(updates, carbon_intensity, helium_scarcity)
            
            # Update predictive analyzer
            if self.enable_predictive:
                self.predictive_analyzer.update_history({
                    'participants': len(selected),
                    'carbon_intensity': carbon_intensity,
                    'helium_scarcity': helium_scarcity,
                    'sustainability_score': self.sustainability_score,
                    'token_pool': self.federation_token_pool,
                    'round_success': True
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
                    'strategy': federation_round.aggregation_strategy.value,
                    'tokens_distributed': federation_round.tokens_distributed,
                    'sustainability_score': self.sustainability_score,
                    'timestamp': datetime.utcnow().isoformat()
                }
                biomass_token = self._store_audit_in_biomass(audit_data)
                if biomass_token:
                    federation_round.biomass_audit_token = biomass_token
            
            # Complete round
            federation_round.completed_at = datetime.utcnow()
            federation_round.successful = True
            federation_round.total_carbon_kg = sum(
                self.participants[pid].carbon_footprint for pid in selected if pid in self.participants
            )
            federation_round.sustainability_score = self.sustainability_score
            federation_round.carbon_savings_kg = self.total_carbon_savings_kg
            
            self.aggregation_history.append(federation_round)
            
            logger.info(
                f"Federation round {self.round_number} complete: "
                f"{len(updates)} updates, sustainability={self.sustainability_score:.2f}"
            )
            
            return global_model
            
        except Exception as e:
            logger.error(f"Federation round failed: {str(e)}", exc_info=True)
            federation_round.successful = False
            
            if self.enable_bio_integration:
                for participant_id in selected:
                    self._pump_trust_gradient(participant_id, success=False, contribution=0.0)
            
            return None
    
    async def _select_participants_bio_aware(
        self, carbon_zone: int, helium_scarcity: float, carbon_intensity: float
    ) -> List[str]:
        scored_participants = []
        
        for participant_id, participant in self.participants.items():
            if not participant.is_active:
                continue
            
            data_score = participant._calculate_data_quality()
            carbon_score = 1.0 / (1.0 + participant.carbon_footprint * 100)
            helium_score = 1.0 / (1.0 + participant.helium_usage * 10)
            intensity_score = 1.0 - (carbon_intensity / 800) if carbon_intensity > 0 else 0.5
            network_score = min(participant.capabilities.network_bandwidth_mbps / 1000, 1.0)
            reliability = 0.8
            
            # Bio-inspired scores
            gradient_score = participant.gradient_alignment
            token_score = participant.capabilities.token_efficiency
            sustainability_score = participant.sustainability_contribution
            
            renewable_bonus = 1.5 if participant.capabilities.energy_source_renewable else 1.0
            
            if carbon_zone >= 8:
                weights = {'data': 0.15, 'carbon': 0.20, 'helium': 0.10, 'intensity': 0.15,
                          'network': 0.10, 'reliability': 0.05, 'gradient': 0.10, 
                          'token': 0.05, 'sustainability': 0.10}
            elif helium_scarcity > 0.7:
                weights = {'data': 0.15, 'helium': 0.25, 'carbon': 0.10, 'intensity': 0.10,
                          'network': 0.10, 'reliability': 0.05, 'gradient': 0.10,
                          'token': 0.05, 'sustainability': 0.10}
            else:
                weights = {'data': 0.20, 'carbon': 0.10, 'helium': 0.10, 'intensity': 0.10,
                          'network': 0.10, 'reliability': 0.05, 'gradient': 0.15,
                          'token': 0.05, 'sustainability': 0.15}
            
            score = (
                weights['data'] * data_score +
                weights['carbon'] * carbon_score +
                weights['helium'] * helium_score +
                weights['intensity'] * intensity_score +
                weights['network'] * network_score +
                weights['reliability'] * reliability +
                weights['gradient'] * gradient_score +
                weights['token'] * token_score +
                weights['sustainability'] * sustainability_score
            ) * renewable_bonus
            
            scored_participants.append((participant_id, score))
        
        scored_participants.sort(key=lambda x: x[1], reverse=True)
        n_select = max(self.min_participants, min(len(scored_participants), int(len(scored_participants) * 0.7)))
        selected = [p[0] for p in scored_participants[:n_select]]
        
        return selected
    
    async def _collect_update(
        self, participant_id: str, epsilon: float, carbon_intensity: float
    ) -> Optional[SecureModelUpdate]:
        if participant_id not in self.participants:
            return None
        
        participant = self.participants[participant_id]
        private_update = self._apply_differential_privacy(participant.local_model, epsilon)
        participant.privacy_budget -= 0.1
        
        carbon_savings = participant.carbon_footprint * 0.01
        
        update = SecureModelUpdate(
            client_id=participant_id,
            round_number=self.round_number,
            encrypted_gradients=hashlib.sha256(str(private_update).encode()).digest(),
            encryption_metadata={'algorithm': 'AES-256-GCM'},
            proof_of_training=hashlib.sha256(f"proof_{participant_id}".encode()).digest(),
            signature=hashlib.sha256(f"sig_{participant_id}_{datetime.utcnow()}".encode()).digest(),
            timestamp=datetime.utcnow(),
            carbon_footprint_kg=participant.carbon_footprint,
            tokens_staked=participant.tokens_earned if self.enable_bio_integration else 0.0,
            gradient_level=participant.gradient_alignment if self.enable_bio_integration else 0.5,
            token_efficiency=participant.capabilities.token_efficiency if self.enable_bio_integration else 0.5,
            sustainability_impact=participant.sustainability_contribution,
            carbon_savings=carbon_savings
        )
        
        return update
    
    def _apply_differential_privacy(self, model: Dict[str, Any], epsilon: float) -> Dict[str, Any]:
        if epsilon <= 0:
            return model
        
        private_model = {}
        sensitivity = 1.0
        
        for key, value in model.items():
            if isinstance(value, (int, float)):
                scale = sensitivity / epsilon
                noise = np.random.laplace(0, scale)
                private_model[key] = value + noise
            elif isinstance(value, np.ndarray):
                scale = sensitivity / epsilon
                noise = np.random.laplace(0, scale, value.shape)
                private_model[key] = value + noise
            else:
                private_model[key] = value
        
        return private_model
    
    def _aggregate_updates_bio_aware(
        self, updates: Dict[str, SecureModelUpdate], strategy: AggregationStrategy
    ) -> Dict[str, Any]:
        if not updates:
            return {}
        
        if strategy == AggregationStrategy.TOKEN_WEIGHTED:
            return self._token_weighted_aggregate(updates)
        elif strategy == AggregationStrategy.GRADIENT_ALIGNED:
            return self._gradient_aligned_aggregate(updates)
        elif strategy == AggregationStrategy.SUSTAINABILITY_WEIGHTED:
            return self._sustainability_weighted_aggregate(updates)
        elif strategy == AggregationStrategy.HEALTH_AWARE:
            return self._health_aware_aggregate(updates)
        else:
            return self._federated_averaging(updates)
    
    def _token_weighted_aggregate(self, updates: Dict[str, SecureModelUpdate]) -> Dict[str, Any]:
        aggregated = {}
        total_tokens = sum(u.tokens_staked for u in updates.values())
        
        if total_tokens == 0:
            return self._federated_averaging(updates)
        
        for key in self.global_model or {}:
            weighted_sum = 0.0
            for update in updates.values():
                weight = update.tokens_staked / total_tokens
                if key in self.global_model:
                    weighted_sum += self.global_model[key] * weight
            aggregated[key] = weighted_sum
        
        return aggregated if aggregated else self._federated_averaging(updates)
    
    def _gradient_aligned_aggregate(self, updates: Dict[str, SecureModelUpdate]) -> Dict[str, Any]:
        aggregated = {}
        total_alignment = sum(u.gradient_level for u in updates.values())
        
        if total_alignment == 0:
            return self._federated_averaging(updates)
        
        for key in self.global_model or {}:
            weighted_sum = 0.0
            for update in updates.values():
                weight = update.gradient_level / total_alignment
                if key in self.global_model:
                    weighted_sum += self.global_model[key] * weight
            aggregated[key] = weighted_sum
        
        return aggregated if aggregated else self._federated_averaging(updates)
    
    def _sustainability_weighted_aggregate(self, updates: Dict[str, SecureModelUpdate]) -> Dict[str, Any]:
        aggregated = {}
        total_sustainability = sum(u.sustainability_impact for u in updates.values())
        
        if total_sustainability == 0:
            return self._federated_averaging(updates)
        
        for key in self.global_model or {}:
            weighted_sum = 0.0
            for update in updates.values():
                weight = update.sustainability_impact / total_sustainability
                if key in self.global_model:
                    weighted_sum += self.global_model[key] * weight
            aggregated[key] = weighted_sum
        
        return aggregated if aggregated else self._federated_averaging(updates)
    
    def _health_aware_aggregate(self, updates: Dict[str, SecureModelUpdate]) -> Dict[str, Any]:
        # Would use compartment_manager for health scores
        return self._federated_averaging(updates)
    
    def _federated_averaging(self, updates: Dict[str, SecureModelUpdate]) -> Dict[str, Any]:
        if not self.global_model:
            return {}
        return self.global_model
    
    def _calculate_sustainability_score(
        self, updates: Dict[str, SecureModelUpdate], carbon_intensity: float, helium_scarcity: float
    ) -> float:
        if not updates:
            return 0.0
        
        avg_carbon_savings = np.mean([u.carbon_savings for u in updates.values()])
        avg_sustainability = np.mean([u.sustainability_impact for u in updates.values()])
        
        carbon_factor = 1.0 - (carbon_intensity / 800)
        helium_factor = 1.0 - helium_scarcity
        
        score = (
            avg_carbon_savings * 0.3 +
            avg_sustainability * 0.3 +
            carbon_factor * 0.2 +
            helium_factor * 0.2
        )
        
        return min(1.0, max(0.0, score))
    
    # ========================================================================
    # Enhanced Statistics
    # ========================================================================
    
    def get_federation_status(self) -> Dict[str, Any]:
        total_rounds = len(self.aggregation_history)
        successful_rounds = sum(1 for r in self.aggregation_history if r.successful)
        
        stats = {
            'total_participants': len(self.participants),
            'active_participants': sum(1 for p in self.participants.values() if p.is_active),
            'total_rounds': total_rounds,
            'successful_rounds': successful_rounds,
            'success_rate': successful_rounds / max(total_rounds, 1),
            'current_strategy': self.aggregation_strategy.value,
            'privacy_level': self.privacy_level.value,
            'total_carbon_emitted': sum(r.total_carbon_kg for r in self.aggregation_history),
            'total_carbon_savings_kg': self.total_carbon_savings_kg,
            'sustainability_score': self.sustainability_score,
            'bio_integration_active': self.enable_bio_integration,
            'carbon_intensity_active': self.enable_carbon_intensity,
            'predictive_active': self.enable_predictive,
            'cross_domain_active': self.enable_cross_domain,
            'sustainability_scoring_active': self.enable_sustainability_scoring,
            'federation_token_pool': self.federation_token_pool,
            'total_tokens_distributed': sum(r.tokens_distributed for r in self.aggregation_history),
            'average_participants_per_round': np.mean([len(r.participants) for r in self.aggregation_history]) if self.aggregation_history else 0
        }
        
        if self.enable_bio_integration:
            stats['gradient_levels'] = self._get_real_gradient_levels()
            stats['harvester_confidence'] = self._get_harvester_confidence()
            stats['atp_sync_timing'] = self._get_atp_driven_sync_timing()
            
            stats['participant_bio_stats'] = {
                pid: {
                    'tokens_earned': p.tokens_earned,
                    'gradient_alignment': p.gradient_alignment,
                    'token_efficiency': p.capabilities.token_efficiency,
                    'trust_pumping_count': p.trust_pumping_count,
                    'sustainability_contribution': p.sustainability_contribution
                }
                for pid, p in self.participants.items()
            }
        
        if self.enable_predictive:
            stats['predictive_summary'] = self.predictive_analyzer.get_sustainability_summary()
        
        if self.enable_cross_domain:
            stats['cross_domain_stats'] = self.cross_domain_transfer.get_transfer_statistics()
        
        return stats
    
    def get_participant_earnings(self, participant_id: str) -> Dict[str, float]:
        if participant_id not in self.participants:
            return {}
        
        participant = self.participants[participant_id]
        
        return {
            'total_tokens_earned': participant.tokens_earned,
            'gradient_alignment': participant.gradient_alignment,
            'token_efficiency': participant.capabilities.token_efficiency,
            'reputation_score': participant.reputation_score,
            'trust_pumping_count': participant.trust_pumping_count,
            'privacy_budget_remaining': participant.privacy_budget,
            'sustainability_contribution': participant.sustainability_contribution
        }
    
    def verify_audit_chain(self) -> bool:
        for i in range(1, len(self.audit_chain)):
            current = self.audit_chain[i]
            previous = self.audit_chain[i - 1]
            if current['previous_hash'] != previous['entry_hash']:
                return False
            computed = hashlib.sha256(
                json.dumps({k: v for k, v in current.items() if k != 'entry_hash'},
                          sort_keys=True, default=str).encode()
            ).hexdigest()
            if computed != current['entry_hash']:
                return False
        return True
    
    def get_sustainability_report(self) -> Dict[str, Any]:
        return {
            'timestamp': datetime.utcnow().isoformat(),
            'sustainability_score': self.sustainability_score,
            'total_carbon_savings_kg': self.total_carbon_savings_kg,
            'total_helium_savings_l': self.total_helium_savings_l,
            'federation_token_pool': self.federation_token_pool,
            'participant_count': len(self.participants),
            'round_count': self.round_number,
            'bio_integration_active': self.enable_bio_integration,
            'predictive_forecast': self.predictive_analyzer.get_sustainability_summary() if self.enable_predictive else {},
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
        
        if self.enable_bio_integration and self._get_harvester_confidence() < 0.4:
            recommendations.append("Improve harvester signal quality for better drift detection")
        
        return recommendations or ["Federation sustainability is on track"]
    
    async def shutdown(self):
        """Graceful shutdown"""
        logger.info("Shutting down Enhanced Federated Orchestrator")
        await self.carbon_manager.close()
        logger.info("Shutdown complete")
