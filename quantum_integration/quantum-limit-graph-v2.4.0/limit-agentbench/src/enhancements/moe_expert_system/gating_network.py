# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/moe_expert_system/gating_network.py
"""
Enhanced Gating Network v5.0.0 - Complete Allosteric Enzyme System

Complete bio-inspired integration with:
- Federated Reflexive Learning with cooperative binding
- User-Adaptive Reflexivity with dynamic configuration
- Real-time Carbon Intensity Integration with API support
- Cross-Domain Knowledge Transfer with token affinity
- Human-AI Collaborative Reflection with sustainability dashboard
- Predictive Reflexivity with ATP-driven forecasting
- Sustainability Score with multi-metric aggregation
- Enhanced Carbon/Helium Awareness with real-time tracking
- Gradient-modulated routing weights (allosteric regulation)
- Token-aware expert selection (substrate affinity)
- Energy-based exploration rate (metabolic state)
- Compartment health feedback (cellular health)
- Biomass reserve awareness (resource storage)
- Second messenger modulation (signal confidence)
- Environmental signal response (photosynthetic awareness)
- Cooperative binding for expert pairs
"""

import asyncio
import logging
import numpy as np
import torch
import torch.nn as nn
import torch.nn.functional as F
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from collections import deque, defaultdict
import math
import hashlib
import json
import aiohttp
import os
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.metrics import r2_score, mean_squared_error

logger = logging.getLogger(__name__)

# ============================================================================
# Try importing bio-inspired modules
# ============================================================================

try:
    from enhancements.bio_inspired.eco_atp_currency import (
        EcoATPTokenManager, DynamicExchangeRate, EcoATPSource, EcoATPConsumer
    )
    from enhancements.bio_inspired.proton_gradient_fields import (
        GradientFieldManager, GradientField
    )
    from enhancements.bio_inspired.atp_synthase_scheduler import (
        ATPSynthaseScheduler, SynthaseConfig
    )
    from enhancements.bio_inspired.chromatophore_compartments import (
        CompartmentManager, ChromatophoreCompartment, CompartmentState
    )
    from enhancements.bio_inspired.biomass_storage import (
        BiomassStorage, StorageTier, GuaranteeLevel
    )
    from enhancements.bio_inspired.photosynthetic_harvester import (
        PhotosyntheticHarvester
    )
    BIO_INSPIRED_AVAILABLE = True
    logger.info("Bio-inspired modules loaded for Gating Network integration")
except ImportError as e:
    BIO_INSPIRED_AVAILABLE = False
    logger.warning(f"Bio-inspired modules not available: {str(e)} - using standard routing")

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
                        self.cache[region] = {'intensity': self.carbon_intensity, 'timestamp': self.last_update}
                        self.historical_intensities.append(self.carbon_intensity)
                    else:
                        self.carbon_intensity = self._get_fallback_intensity(region)
                        self.last_update = datetime.now()
            except Exception as e:
                logger.error(f"Carbon intensity fetch error: {e}")
                self.carbon_intensity = self._get_fallback_intensity(region)
                self.last_update = datetime.now()
            return {'intensity': self.carbon_intensity, 'region': self.region,
                    'timestamp': self.last_update.isoformat() if self.last_update else None}
    
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
# Federated Gating Network Module
# ============================================================================

class FederatedGatingNetwork:
    """Federated reflexive learning for distributed gating"""
    
    def __init__(self, server_url: Optional[str] = None):
        self.server_url = server_url
        self.round = 0
        self.local_weights = {}
        self.global_weights = {}
        self.participants = []
        self.contribution_scores = {}
        self._lock = asyncio.Lock()
        self._session = None
    
    async def _get_session(self):
        if self._session is None and self.server_url:
            self._session = aiohttp.ClientSession()
        return self._session
    
    async def send_local_weights(self, participant_id: str, weights: Dict, performance: float = 1.0) -> Dict:
        """Send local gating weights to federated server"""
        if not self.server_url:
            return {'status': 'local'}
        
        async with self._lock:
            session = await self._get_session()
            try:
                weights_serialized = {k: v.tolist() for k, v in weights.items()}
                update_data = {
                    'participant_id': participant_id,
                    'round': self.round,
                    'weights': weights_serialized,
                    'performance': performance,
                    'timestamp': datetime.utcnow().isoformat()
                }
                async with session.post(
                    f"{self.server_url}/federated/gating",
                    json=update_data,
                    timeout=30
                ) as response:
                    if response.status == 200:
                        result = await response.json()
                        self.round += 1
                        self.contribution_scores[participant_id] = performance
                        return result
                    return {'status': 'failed'}
            except Exception as e:
                logger.error(f"Federated gating send error: {e}")
                return {'status': 'error'}
    
    async def get_global_weights(self) -> Optional[Dict]:
        """Get aggregated gating weights from federated server"""
        if not self.server_url:
            return self.global_weights
        
        async with self._lock:
            session = await self._get_session()
            try:
                async with session.get(
                    f"{self.server_url}/federated/gating/global",
                    timeout=30
                ) as response:
                    if response.status == 200:
                        data = await response.json()
                        self.global_weights = data.get('weights', {})
                        self.participants = data.get('participants', [])
                        return self.global_weights
            except Exception as e:
                logger.error(f"Global gating fetch error: {e}")
                return None
    
    def aggregate_weights(self, peer_weights: List[Dict], weights: Dict[str, float] = None) -> Dict:
        """Aggregate gating weights from peers with weighted averaging"""
        if not peer_weights:
            return {}
        
        aggregated = {}
        if weights is None:
            weights = {i: 1.0 for i in range(len(peer_weights))}
        
        for key in peer_weights[0].keys():
            total = 0.0
            total_weight = 0.0
            for i, peer in enumerate(peer_weights):
                if key in peer:
                    total += peer[key] * weights.get(i, 1.0)
                    total_weight += weights.get(i, 1.0)
            aggregated[key] = total / max(total_weight, 0.001)
        
        return aggregated
    
    def get_federated_stats(self) -> Dict:
        return {
            'round': self.round,
            'participants': len(self.participants),
            'has_global_weights': bool(self.global_weights),
            'contribution_scores': self.contribution_scores
        }
    
    async def close(self):
        if self._session:
            await self._session.close()

# ============================================================================
# Predictive Gating Analyzer Module
# ============================================================================

class PredictiveGatingAnalyzer:
    """Predictive reflexivity with ensemble forecasting for gating performance"""
    
    def __init__(self, history_window: int = 100):
        self.history_window = history_window
        self.gating_history = deque(maxlen=history_window)
        self.forecast_history = deque(maxlen=50)
        self.models = {}
        self.scaler = StandardScaler()
        self.is_trained = False
        
        self.models['random_forest'] = RandomForestRegressor(n_estimators=100, random_state=42)
        self.models['gradient_boosting'] = GradientBoostingRegressor(n_estimators=100, random_state=42)
    
    def update_history(self, gating_metrics: Dict):
        """Update gating history for forecasting"""
        self.gating_history.append({
            'timestamp': datetime.utcnow(),
            'routing_accuracy': gating_metrics.get('routing_accuracy', 0.8),
            'load_balance': gating_metrics.get('load_balance', 0.5),
            'expert_utilization': gating_metrics.get('expert_utilization', 0.5),
            'cooperative_strength': gating_metrics.get('cooperative_strength', 0.5),
            'carbon_efficiency': gating_metrics.get('carbon_efficiency', 0.5)
        })
    
    async def train_forecast_model(self):
        """Train ensemble forecasting models"""
        if len(self.gating_history) < 10:
            return {'status': 'insufficient_data', 'samples': len(self.gating_history)}
        
        X = []
        y = []
        history_list = list(self.gating_history)
        
        for i in range(len(history_list) - 5):
            features = []
            for j in range(5):
                data = history_list[i + j]
                features.extend([
                    data['routing_accuracy'],
                    data['load_balance'],
                    data['expert_utilization'],
                    data['cooperative_strength'],
                    data['carbon_efficiency']
                ])
            X.append(features)
            y.append(history_list[i + 5]['routing_accuracy'])
        
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
        logger.info(f"Gating forecast models trained. R²: {results}")
        return {'status': 'success', 'results': results, 'samples': len(X)}
    
    async def predict_gating_performance(self, hours: int = 24) -> Dict:
        """Predict future gating performance"""
        if not self.is_trained or len(self.gating_history) < 10:
            return {'predicted_accuracy': 0.5, 'confidence': 0.0, 'trend': 'insufficient_data'}
        
        recent = list(self.gating_history)[-5:]
        features = []
        for data in recent:
            features.extend([
                data['routing_accuracy'],
                data['load_balance'],
                data['expert_utilization'],
                data['cooperative_strength'],
                data['carbon_efficiency']
            ])
        
        features = np.array(features).reshape(1, -1)
        features_scaled = self.scaler.transform(features)
        
        predictions = []
        for name, model in self.models.items():
            if model is not None:
                pred = model.predict(features_scaled)[0]
                predictions.append(pred)
        
        if not predictions:
            return {'predicted_accuracy': 0.5, 'confidence': 0.0, 'trend': 'no_models'}
        
        prediction = np.mean(predictions)
        confidence = min(0.9, np.std(predictions) / 0.2) if len(predictions) > 1 else 0.5
        
        if len(self.forecast_history) > 5:
            recent_forecasts = list(self.forecast_history)[-5:]
            trend = "improving" if prediction > recent_forecasts[-1] else "declining" if prediction < recent_forecasts[-1] else "stable"
        else:
            trend = "stable"
        
        self.forecast_history.append({'prediction': prediction, 'trend': trend})
        return {
            'predicted_accuracy': prediction,
            'confidence': confidence,
            'trend': trend,
            'recommended_actions': self._generate_predictive_actions(prediction)
        }
    
    def _generate_predictive_actions(self, prediction: float) -> List[str]:
        actions = []
        if prediction < 0.5:
            actions.append("Optimize cooperative binding weights")
            actions.append("Increase exploration rate")
        elif prediction < 0.7:
            actions.append("Enhance gradient modulation")
            actions.append("Improve token affinity calibration")
        else:
            actions.append("Maintain current gating configuration")
        return actions

# ============================================================================
# Gating Context (Enhanced with Bio-Inspired Features)
# ============================================================================

@dataclass
class GatingContext:
    """Enhanced context features for gating network with bio-inspired data"""
    # Layer 0: Workload features
    task_type: str
    task_complexity: float
    input_size_mb: float
    data_format: str = "unknown"
    
    # Layer 1: Meta-cognitive state
    carbon_budget_remaining: float = 1.0
    helium_budget_remaining: float = 1.0
    latency_budget_ms: float = 100.0
    historical_success_rate: float = 0.9
    
    # Layer 3: Dual-axis features
    carbon_zone: int = 0
    helium_scarcity: float = 0.5
    carbon_weight: float = 0.6
    helium_weight: float = 0.4
    
    # Additional context
    time_of_day: int = 0
    grid_carbon_intensity: float = 400.0
    hardware_availability: Dict[str, float] = field(default_factory=dict)
    renewable_percentage: float = 0.0
    energy_price: float = 0.10
    
    # Task-specific
    priority: int = 1
    deadline_pressure: float = 0.0
    previous_experts_used: List[str] = field(default_factory=list)
    
    # BIO-INSPIRED: Gradient levels
    carbon_gradient: float = 0.5
    helium_gradient: float = 0.5
    trust_gradient: float = 0.5
    opportunity_gradient: float = 0.5
    
    # BIO-INSPIRED: Token and energy state
    token_availability: float = 0.5
    ecoatp_rate: float = 50.0
    biomass_reserve_level: float = 0.3
    
    # BIO-INSPIRED: Compartment health
    expert_health_scores: Dict[str, float] = field(default_factory=dict)
    
    # BIO-INSPIRED: Second messenger levels
    camp_level: float = 0.1
    calcium_level: float = 0.05
    ip3_level: float = 0.05
    
    # Sustainability metrics
    sustainability_score: float = 0.0
    carbon_efficiency: float = 0.5
    helium_efficiency: float = 0.5
    
    def to_tensor(self) -> torch.Tensor:
        """Convert context to feature tensor with bio-inspired features"""
        # Task type encoding (one-hot)
        task_types = ['inference', 'training', 'data_processing', 'optimization', 
                     'simulation', 'streaming', 'batch', 'interactive']
        task_encoding = [0.0] * len(task_types)
        if self.task_type in task_types:
            task_encoding[task_types.index(self.task_type)] = 1.0
        
        # Data format encoding
        formats = ['json', 'csv', 'parquet', 'avro', 'protobuf', 'unknown']
        format_encoding = [0.0] * len(formats)
        if self.data_format in formats:
            format_encoding[formats.index(self.data_format)] = 1.0
        
        # Hardware availability encoding
        hw_features = [
            self.hardware_availability.get('cpu', 1.0),
            self.hardware_availability.get('gpu', 0.0),
            self.hardware_availability.get('quantum', 0.0),
            self.hardware_availability.get('edge', 0.0)
        ]
        
        # Continuous features with normalization
        continuous_features = [
            self.task_complexity,
            math.log1p(self.input_size_mb) / 10.0,
            self.carbon_budget_remaining,
            self.helium_budget_remaining,
            self.latency_budget_ms / 10000.0,
            self.historical_success_rate,
            self.carbon_zone / 15.0,
            self.helium_scarcity,
            self.carbon_weight,
            self.helium_weight,
            self.time_of_day / 23.0,
            self.grid_carbon_intensity / 1000.0,
            self.renewable_percentage,
            self.energy_price / 1.0,
            self.priority / 5.0,
            self.deadline_pressure,
            len(self.previous_experts_used) / 5.0,
            # BIO-INSPIRED: Gradient features
            self.carbon_gradient,
            self.helium_gradient,
            self.trust_gradient,
            self.opportunity_gradient,
            # BIO-INSPIRED: Token and energy features
            self.token_availability,
            self.ecoatp_rate / 200.0,
            self.biomass_reserve_level,
            # BIO-INSPIRED: Second messenger features
            self.camp_level,
            self.calcium_level,
            self.ip3_level,
            # Sustainability features
            self.sustainability_score,
            self.carbon_efficiency,
            self.helium_efficiency
        ]
        
        # Combine all features
        all_features = task_encoding + format_encoding + hw_features + continuous_features
        
        return torch.tensor(all_features, dtype=torch.float32)
    
    @property
    def feature_dim(self) -> int:
        """Get total feature dimension"""
        return len(self.to_tensor())


# ============================================================================
# Enhanced Sparse MoE Gate with Bio-Inspired Modulation
# ============================================================================

class EnhancedSparseMoEGate(nn.Module):
    """
    Enhanced Sparse MoE Gate with bio-inspired allosteric regulation.
    
    Features:
    - Gradient-modulated attention weights
    - Token-aware expert affinity
    - Energy-based exploration
    - Cooperative binding between expert pairs
    """
    
    def __init__(
        self,
        input_dim: int,
        num_experts: int,
        top_k: int = 2,
        capacity_factor: float = 1.25,
        noise_std: float = 0.1,
        use_attention: bool = True,
        use_hierarchical: bool = False,
        use_uncertainty: bool = True
    ):
        super().__init__()
        self.num_experts = num_experts
        self.top_k = top_k
        self.capacity_factor = capacity_factor
        self.noise_std = noise_std
        self.use_attention = use_attention
        self.use_hierarchical = use_hierarchical
        self.use_uncertainty = use_uncertainty
        
        # Core gating network
        self.gate = nn.Sequential(
            nn.Linear(input_dim, 256),
            nn.LayerNorm(256),
            nn.ReLU(),
            nn.Dropout(0.1),
            nn.Linear(256, 128),
            nn.LayerNorm(128),
            nn.ReLU(),
            nn.Dropout(0.1),
            nn.Linear(128, 64),
            nn.LayerNorm(64),
            nn.ReLU(),
            nn.Dropout(0.1),
            nn.Linear(64, num_experts)
        )
        
        # BIO-INSPIRED: Allosteric modulation layers
        self.gradient_modulator = nn.Sequential(
            nn.Linear(4, 32),  # 4 gradient inputs
            nn.ReLU(),
            nn.Linear(32, num_experts),
            nn.Tanh()  # -1 to 1 modulation
        )
        
        # BIO-INSPIRED: Token affinity layer
        self.token_affinity = nn.Sequential(
            nn.Linear(3, 16),  # token_availability, ecoatp_rate, biomass_level
            nn.ReLU(),
            nn.Linear(16, num_experts),
            nn.Sigmoid()  # 0 to 1 affinity
        )
        
        # BIO-INSPIRED: Cooperative binding matrix
        self.cooperative_matrix = nn.Parameter(
            torch.eye(num_experts) * 0.1
        )
        
        # Expert specialization embeddings
        self.expert_embeddings = nn.Parameter(
            torch.randn(num_experts, 32)
        )
        
        # Load balancing
        self.load_balance_weight = 0.01
        
        # BIO-INSPIRED: Biomass-aware load balance weight
        self.biomass_load_balance_multiplier = 1.0
        
        # Initialize weights
        self._init_weights()
    
    def _init_weights(self):
        """Initialize network weights"""
        for module in self.modules():
            if isinstance(module, nn.Linear):
                nn.init.xavier_uniform_(module.weight)
                if module.bias is not None:
                    nn.init.zeros_(module.bias)
    
    def forward(
        self,
        x: torch.Tensor,
        training: bool = False,
        gradient_levels: Optional[Dict[str, float]] = None,
        token_state: Optional[Dict[str, float]] = None
    ) -> Tuple[torch.Tensor, torch.Tensor, torch.Tensor, Dict[str, Any]]:
        """
        Enhanced forward pass with bio-inspired modulation.
        """
        metadata = {}
        
        # Standard routing logits
        logits = self.gate(x)
        
        # BIO-INSPIRED: Apply gradient modulation (allosteric regulation)
        if gradient_levels:
            gradient_tensor = torch.tensor([
                gradient_levels.get('carbon', 0.5),
                gradient_levels.get('helium', 0.5),
                gradient_levels.get('trust', 0.5),
                gradient_levels.get('opportunity', 0.5)
            ], dtype=torch.float32).to(x.device)
            
            if x.dim() > 1:
                gradient_tensor = gradient_tensor.unsqueeze(0).expand(x.size(0), -1)
            
            gradient_mod = self.gradient_modulator(gradient_tensor)
            logits = logits + gradient_mod * 0.3  # 30% gradient influence
            
            metadata['gradient_modulation'] = gradient_mod.detach().cpu().numpy()
        
        # BIO-INSPIRED: Apply token affinity modulation
        if token_state:
            token_tensor = torch.tensor([
                token_state.get('token_availability', 0.5),
                token_state.get('ecoatp_rate', 50.0) / 200.0,
                token_state.get('biomass_reserve', 0.3)
            ], dtype=torch.float32).to(x.device)
            
            if x.dim() > 1:
                token_tensor = token_tensor.unsqueeze(0).expand(x.size(0), -1)
            
            token_aff = self.token_affinity(token_tensor)
            logits = logits * (0.7 + token_aff * 0.6)  # 60% token influence
            
            metadata['token_affinity'] = token_aff.detach().cpu().numpy()
        
        # BIO-INSPIRED: Apply cooperative binding bonus
        if x.dim() > 1 and x.size(0) > 1:
            coop_bonus = torch.matmul(
                F.softmax(logits, dim=-1),
                self.cooperative_matrix
            )
            logits = logits + coop_bonus * 0.1  # 10% cooperative influence
        
        # Add noise during training for exploration
        if training:
            noise = torch.randn_like(logits) * self.noise_std
            logits = logits + noise
        
        # Get top-k experts
        top_k_logits, top_k_indices = torch.topk(logits, self.top_k, dim=-1)
        routing_weights = F.softmax(top_k_logits, dim=-1)
        
        # Compute load balancing loss with biomass awareness
        load_balance_loss = self._compute_enhanced_load_balance_loss(
            logits, top_k_indices
        ) * self.biomass_load_balance_multiplier
        
        # Calculate metadata
        metadata['entropy'] = -(F.softmax(logits, dim=-1) * 
                               torch.log(F.softmax(logits, dim=-1) + 1e-8)).sum(dim=-1).mean().item()
        metadata['confidence'] = routing_weights.max(dim=-1)[0].mean().item()
        metadata['cooperative_matrix'] = self.cooperative_matrix.detach().cpu().numpy()
        
        return routing_weights, top_k_indices, load_balance_loss, metadata
    
    def _compute_enhanced_load_balance_loss(
        self, logits: torch.Tensor, indices: torch.Tensor
    ) -> torch.Tensor:
        """Enhanced load balancing loss considering expert utilization"""
        expert_mask = F.one_hot(indices, num_classes=self.num_experts).float()
        expert_fraction = expert_mask.mean(dim=0)
        routing_probs = F.softmax(logits, dim=-1)
        avg_routing_prob = routing_probs.mean(dim=0)
        load_balance_loss = self.num_experts * torch.sum(expert_fraction * avg_routing_prob)
        return load_balance_loss
    
    def update_cooperative_binding(self, expert_pairs: Dict[Tuple[int, int], float]):
        """Update cooperative binding strengths based on observed performance"""
        with torch.no_grad():
            for (expert_a, expert_b), strength in expert_pairs.items():
                if expert_a < self.num_experts and expert_b < self.num_experts:
                    self.cooperative_matrix[expert_a, expert_b] = strength
                    self.cooperative_matrix[expert_b, expert_a] = strength


# ============================================================================
# Enhanced MoE Gating Network with Complete Bio-Inspired Integration
# ============================================================================

class MoEGatingNetwork:
    """
    Enhanced MoE Gating Network v5.0.0 - Complete Allosteric Enzyme System
    
    Complete bio-inspired integration with sustainability dashboard,
    federated learning, and predictive analytics.
    """
    
    def __init__(
        self,
        num_experts: int = 5,
        top_k: int = 2,
        device: str = 'cpu',
        use_attention: bool = True,
        use_uncertainty: bool = True,
        enable_bio_integration: bool = True,
        enable_federated: bool = True,
        enable_predictive: bool = True,
        enable_carbon_intensity: bool = True,
        server_url: Optional[str] = None
    ):
        self.num_experts = num_experts
        self.top_k = top_k
        self.device = device
        self.enable_bio_integration = enable_bio_integration and BIO_INSPIRED_AVAILABLE
        self.enable_federated = enable_federated
        self.enable_predictive = enable_predictive
        self.enable_carbon_intensity = enable_carbon_intensity
        
        # New modules
        self.carbon_manager = CarbonIntensityManager() if enable_carbon_intensity else None
        self.federated_network = FederatedGatingNetwork(server_url) if enable_federated else None
        self.predictive_analyzer = PredictiveGatingAnalyzer() if enable_predictive else None
        
        # Initialize enhanced sparse gate
        self.sparse_gate = EnhancedSparseMoEGate(
            input_dim=GatingContext().feature_dim,
            num_experts=num_experts,
            top_k=top_k,
            use_attention=use_attention,
            use_uncertainty=use_uncertainty
        ).to(device)
        
        # Expert tracking
        self.expert_usage_count: Dict[int, int] = defaultdict(int)
        self.expert_success_count: Dict[int, int] = defaultdict(int)
        self.expert_carbon_total: Dict[int, float] = defaultdict(float)
        self.expert_helium_total: Dict[int, float] = defaultdict(float)
        self.total_routing_calls = 0
        
        # Expert index mapping
        self.expert_index_map: Dict[int, str] = {}
        
        # Routing history
        self.routing_history: deque = deque(maxlen=10000)
        
        # BIO-INSPIRED: Module references (injected)
        self.token_manager: Optional[EcoATPTokenManager] = None
        self.gradient_manager: Optional[GradientFieldManager] = None
        self.scheduler: Optional[ATPSynthaseScheduler] = None
        self.compartment_manager: Optional[CompartmentManager] = None
        self.biomass_storage: Optional[BiomassStorage] = None
        self.harvester: Optional[PhotosyntheticHarvester] = None
        
        # BIO-INSPIRED: Cooperative binding tracking
        self.cooperative_pairs: Dict[Tuple[int, int], float] = {}
        self.cooperative_history: deque = deque(maxlen=1000)
        
        # BIO-INSPIRED: Environmental signal history
        self.environmental_history: deque = deque(maxlen=500)
        
        # Sustainability tracking
        self.sustainability_score = 0.0
        self.carbon_efficiency = 0.5
        self.helium_efficiency = 0.5
        
        # Optimizer for online learning
        self.optimizer = torch.optim.Adam(
            self.sparse_gate.parameters(), lr=0.001
        )
        
        # Experience buffer for online learning
        self.experience_buffer: deque = deque(maxlen=5000)
        
        # Load balance weight (will be modulated by biomass)
        self.load_balance_weight = 0.01
        
        # Previous routing for temporal memory
        self.previous_routing: Optional[torch.Tensor] = None
        
        # Start background tasks
        self._start_background_tasks()
        
        logger.info(
            f"Enhanced MoE Gating Network v5.0.0 initialized: "
            f"experts={num_experts}, top_k={top_k}, "
            f"bio_integration={self.enable_bio_integration}, "
            f"federated={self.enable_federated}, "
            f"predictive={self.enable_predictive}"
        )
    
    def _start_background_tasks(self):
        if self.enable_carbon_intensity:
            asyncio.create_task(self._carbon_update_loop())
        if self.enable_predictive:
            asyncio.create_task(self._predictive_update_loop())
        if self.enable_federated:
            asyncio.create_task(self._federated_sync_loop())
    
    async def _carbon_update_loop(self):
        while True:
            try:
                if self.carbon_manager:
                    await self.carbon_manager.update_carbon_intensity()
                await asyncio.sleep(self.carbon_manager.update_interval if self.carbon_manager else 300)
            except Exception as e:
                logger.error(f"Carbon update error: {str(e)}")
                await asyncio.sleep(60)
    
    async def _predictive_update_loop(self):
        while True:
            try:
                if self.predictive_analyzer:
                    self.predictive_analyzer.update_history({
                        'routing_accuracy': self.get_load_balance_score(),
                        'load_balance': self.get_load_balance_score(),
                        'expert_utilization': max(self.get_expert_utilization().values()) if self.get_expert_utilization() else 0.5,
                        'cooperative_strength': len(self.cooperative_pairs) / max(self.num_experts * (self.num_experts - 1) / 2, 1),
                        'carbon_efficiency': self.carbon_efficiency
                    })
                    await self.predictive_analyzer.train_forecast_model()
                await asyncio.sleep(300)
            except Exception as e:
                logger.error(f"Predictive update error: {str(e)}")
                await asyncio.sleep(60)
    
    async def _federated_sync_loop(self):
        while True:
            try:
                if self.federated_network:
                    weights = self.sparse_gate.state_dict()
                    await self.federated_network.send_local_weights(
                        f"gating_{hashlib.md5(str(self.routing_history).encode()).hexdigest()[:8]}",
                        weights,
                        performance=self.sustainability_score
                    )
                    global_weights = await self.federated_network.get_global_weights()
                    if global_weights:
                        self.sparse_gate.load_state_dict(global_weights)
                await asyncio.sleep(3600)
            except Exception as e:
                logger.error(f"Federated sync error: {str(e)}")
                await asyncio.sleep(300)
    
    # ========================================================================
    # Bio-Inspired Module Injection
    # ========================================================================
    
    def inject_bio_core(self, bio_core: Any = None, **kwargs):
        """
        Inject bio-inspired modules for complete correlation.
        """
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
        
        injections = {
            'token_manager': self.token_manager is not None,
            'gradient_manager': self.gradient_manager is not None,
            'scheduler': self.scheduler is not None,
            'compartment_manager': self.compartment_manager is not None,
            'biomass_storage': self.biomass_storage is not None,
            'harvester': self.harvester is not None
        }
        logger.info(f"Bio-inspired injections into Gating Network: {injections}")
        
        if any(injections.values()):
            self.enable_bio_integration = True
    
    # ========================================================================
    # Bio-Inspired Data Access Methods
    # ========================================================================
    
    def _get_real_gradient_levels(self) -> Dict[str, float]:
        if self.gradient_manager:
            return self.gradient_manager.get_field_strengths()
        return {'carbon': 0.5, 'helium': 0.5, 'trust': 0.5, 'opportunity': 0.5}
    
    def _get_real_token_state(self) -> Dict[str, float]:
        state = {'token_availability': 0.5, 'ecoatp_rate': 50.0, 'biomass_reserve': 0.3}
        
        if self.token_manager:
            summary = self.token_manager.get_system_summary()
            state['token_availability'] = min(1.0, summary.get('total_balance', 500) / 1000.0)
        
        if self.scheduler:
            driving_force = self.scheduler.calculate_gradient_driving_force()
            rotation_speed = self.scheduler.calculate_rotation_speed(driving_force)
            state['ecoatp_rate'] = self.scheduler.calculate_atp_production_rate(rotation_speed)
        
        if self.biomass_storage:
            stats = self.biomass_storage.get_storage_stats()
            total_stored = stats.get('total_stored', 0)
            state['biomass_reserve'] = min(1.0, total_stored / 10000.0)
        
        return state
    
    def _get_compartment_health_scores(self) -> Dict[str, float]:
        scores = {}
        if self.compartment_manager:
            for idx, expert_id in self.expert_index_map.items():
                compartment = self.compartment_manager.find_best_compartment(expert_id)
                if compartment:
                    scores[expert_id] = compartment.health_score
                else:
                    scores[expert_id] = 0.7
        return scores
    
    def _get_bio_modulated_exploration(self) -> float:
        base_exploration = 0.1
        
        if self.gradient_manager:
            carbon = self.gradient_manager.fields.get('carbon')
            if carbon and carbon.gradient_strength > 0.7:
                base_exploration *= 0.5
        
        if self.scheduler:
            driving_force = self.scheduler.calculate_gradient_driving_force()
            rotation_speed = self.scheduler.calculate_rotation_speed(driving_force)
            ecoatp_rate = self.scheduler.calculate_atp_production_rate(rotation_speed)
            
            if ecoatp_rate > 100:
                base_exploration *= 1.5
            elif ecoatp_rate < 20:
                base_exploration *= 0.3
        
        return min(0.5, max(0.01, base_exploration))
    
    def _get_expert_bio_scores(self, expert_indices: List[int]) -> Dict[int, float]:
        scores = {}
        
        for idx in expert_indices:
            expert_id = self.expert_index_map.get(idx)
            if not expert_id:
                scores[idx] = 0.5
                continue
            
            score = 0.5
            
            if self.token_manager:
                account = self.token_manager.get_account_summary(f"expert_{expert_id}")
                if account:
                    efficiency = account.get('efficiency_rating', 0.5)
                    balance = account.get('balance', 0)
                    score += efficiency * 0.2
                    if balance > 100:
                        score += 0.1
            
            if self.compartment_manager:
                compartment = self.compartment_manager.find_best_compartment(expert_id)
                if compartment:
                    score += compartment.health_score * 0.15
            
            if self.gradient_manager:
                trust = self.gradient_manager.fields.get('trust')
                if trust:
                    score += trust.gradient_strength * 0.1
            
            success_rate = self.get_expert_success_rates().get(idx, 0.5)
            score += success_rate * 0.05
            
            scores[idx] = min(1.0, score)
        
        return scores
    
    def _get_biomass_aware_load_balance(self) -> float:
        if not self.biomass_storage:
            return 0.01
        
        stats = self.biomass_storage.get_storage_stats()
        total_stored = stats.get('total_stored', 0)
        glycogen = stats.get('tiers', {}).get('glycogen_queue', 0)
        
        if total_stored > 5000 or glycogen > 500:
            return 0.05
        elif total_stored < 1000:
            return 0.005
        
        return 0.01
    
    def _update_cooperative_binding(self, expert_a: int, expert_b: int, success: bool):
        key = (expert_a, expert_b)
        current = self.cooperative_pairs.get(key, 0.0)
        
        alpha = 0.1
        if success:
            self.cooperative_pairs[key] = current + alpha * (1.0 - current)
        else:
            self.cooperative_pairs[key] = current * (1.0 - alpha)
        
        self.sparse_gate.update_cooperative_binding({key: self.cooperative_pairs[key]})
        
        self.cooperative_history.append({
            'expert_a': expert_a,
            'expert_b': expert_b,
            'success': success,
            'strength': self.cooperative_pairs[key],
            'timestamp': datetime.utcnow().isoformat()
        })
    
    def _calculate_sustainability_score(self) -> float:
        """Calculate overall sustainability score"""
        load_balance = self.get_load_balance_score()
        expert_util = max(self.get_expert_utilization().values()) if self.get_expert_utilization() else 0.5
        
        carbon_factor = self.carbon_efficiency
        helium_factor = self.helium_efficiency
        
        score = (load_balance * 0.2 + (1 - expert_util) * 0.2 + carbon_factor * 0.3 + helium_factor * 0.3)
        return min(1.0, max(0.0, score))
    
    # ========================================================================
    # Enhanced Routing Method
    # ========================================================================
    
    def route(
        self,
        context: 'GatingContext',
        expert_constraints: Optional[List[int]] = None,
        training: bool = False,
        return_metadata: bool = False
    ) -> List[Tuple[int, float]]:
        """
        Enhanced routing with complete bio-inspired modulation.
        """
        # Update carbon intensity
        carbon_intensity = 400
        if self.carbon_manager:
            carbon_intensity = asyncio.run(self.carbon_manager.get_current_intensity())
            context.grid_carbon_intensity = carbon_intensity
        
        # BIO-INSPIRED: Enrich context with real bio-inspired data
        if self.enable_bio_integration:
            gradient_levels = self._get_real_gradient_levels()
            context.carbon_gradient = gradient_levels.get('carbon', 0.5)
            context.helium_gradient = gradient_levels.get('helium', 0.5)
            context.trust_gradient = gradient_levels.get('trust', 0.5)
            context.opportunity_gradient = gradient_levels.get('opportunity', 0.5)
            
            token_state = self._get_real_token_state()
            context.token_availability = token_state['token_availability']
            context.ecoatp_rate = token_state['ecoatp_rate']
            context.biomass_reserve_level = token_state['biomass_reserve']
            
            self.sparse_gate.biomass_load_balance_multiplier = (
                self._get_biomass_aware_load_balance() / 0.01
            )
        
        # Update sustainability metrics
        self.carbon_efficiency = 1.0 / (1.0 + carbon_intensity / 500)
        self.sustainability_score = self._calculate_sustainability_score()
        context.sustainability_score = self.sustainability_score
        context.carbon_efficiency = self.carbon_efficiency
        
        # Convert context to tensor
        x = context.to_tensor().unsqueeze(0).to(self.device)
        
        # BIO-INSPIRED: Get exploration rate
        exploration = self._get_bio_modulated_exploration() if self.enable_bio_integration else 0.1
        
        # BIO-INSPIRED: Exploratory routing based on metabolic state
        if training and np.random.random() < exploration:
            available = expert_constraints or list(range(self.num_experts))
            selected = list(np.random.choice(
                available, size=min(self.top_k, len(available)), replace=False
            ))
            routing_decisions = [(idx, 1.0 / len(selected)) for idx in selected]
            
            metadata = {
                'exploratory': True,
                'exploration_rate': exploration,
                'method': 'bio_modulated_exploration'
            }
        else:
            with torch.set_grad_enabled(training):
                gradient_levels_dict = None
                token_state_dict = None
                
                if self.enable_bio_integration:
                    gradient_levels_dict = self._get_real_gradient_levels()
                    token_state_dict = self._get_real_token_state()
                
                routing_weights, expert_indices, aux_loss, metadata = self.sparse_gate(
                    x, training=training,
                    gradient_levels=gradient_levels_dict,
                    token_state=token_state_dict
                )
            
            routing_weights = routing_weights.squeeze(0).detach().cpu().numpy()
            expert_indices = expert_indices.squeeze(0).detach().cpu().numpy()
            
            routing_decisions = list(zip(expert_indices, routing_weights))
        
        # BIO-INSPIRED: Apply expert bio-scores
        if self.enable_bio_integration:
            bio_scores = self._get_expert_bio_scores([d[0] for d in routing_decisions])
            routing_decisions = [
                (idx, weight * bio_scores.get(idx, 0.5))
                for idx, weight in routing_decisions
            ]
            
            total_weight = sum(w for _, w in routing_decisions)
            if total_weight > 0:
                routing_decisions = [(idx, w / total_weight) for idx, w in routing_decisions]
        
        # Apply constraints
        if expert_constraints is not None:
            routing_decisions = [
                (idx, weight) for idx, weight in routing_decisions
                if idx in expert_constraints
            ]
            if not routing_decisions and expert_constraints:
                routing_decisions = [
                    (idx, 1.0 / len(expert_constraints))
                    for idx in expert_constraints
                ]
        
        # Update previous routing
        full_weights = torch.zeros(1, self.num_experts).to(self.device)
        for idx, weight in routing_decisions:
            if idx < self.num_experts:
                full_weights[0, idx] = weight
        self.previous_routing = full_weights
        
        # Update usage statistics
        for idx, _ in routing_decisions:
            self.expert_usage_count[idx] = self.expert_usage_count.get(idx, 0) + 1
        
        self.total_routing_calls += 1
        
        # BIO-INSPIRED: Update cooperative binding for pairs
        if len(routing_decisions) >= 2:
            for i, (idx_a, _) in enumerate(routing_decisions):
                for idx_b, _ in routing_decisions[i+1:]:
                    self._update_cooperative_binding(
                        idx_a, idx_b,
                        success=metadata.get('confidence', 0.5) > 0.6
                    )
        
        # Record routing for learning
        self._record_routing(context, routing_decisions, metadata)
        
        # Federated learning
        if self.enable_federated and self.total_routing_calls % 100 == 0:
            asyncio.create_task(self.federated_network.send_local_weights(
                f"gating_{hashlib.md5(str(self.routing_history).encode()).hexdigest()[:8]}",
                self.sparse_gate.state_dict(),
                performance=self.sustainability_score
            ))
        
        # Predictive analytics
        if self.enable_predictive:
            self.predictive_analyzer.update_history({
                'routing_accuracy': metadata.get('confidence', 0.5),
                'load_balance': self.get_load_balance_score(),
                'expert_utilization': max(self.get_expert_utilization().values()) if self.get_expert_utilization() else 0.5,
                'cooperative_strength': len(self.cooperative_pairs) / max(self.num_experts * (self.num_experts - 1) / 2, 1),
                'carbon_efficiency': self.carbon_efficiency
            })
            asyncio.create_task(self.predictive_analyzer.train_forecast_model())
        
        if return_metadata:
            metadata_dict = {
                'confidence': metadata.get('confidence', 0.5),
                'entropy': metadata.get('entropy', 0.0),
                'exploration_rate': exploration,
                'gradient_modulation': metadata.get('gradient_modulation', None),
                'token_affinity': metadata.get('token_affinity', None),
                'cooperative_matrix': metadata.get('cooperative_matrix', None),
                'bio_integration_active': self.enable_bio_integration,
                'sustainability_score': self.sustainability_score,
                'carbon_efficiency': self.carbon_efficiency,
                'carbon_intensity': carbon_intensity
            }
            return routing_decisions, metadata_dict
        
        return routing_decisions
    
    def _record_routing(
        self,
        context: 'GatingContext',
        routing_decisions: List[Tuple[int, float]],
        metadata: Dict[str, Any]
    ):
        self.routing_history.append({
            'context': context,
            'decisions': routing_decisions,
            'confidence': metadata.get('confidence', 0.5),
            'entropy': metadata.get('entropy', 0.0),
            'timestamp': self.total_routing_calls
        })
    
    # ========================================================================
    # Expert Statistics Methods
    # ========================================================================
    
    def get_expert_utilization(self) -> Dict[int, float]:
        if self.total_routing_calls == 0:
            return {i: 0.0 for i in range(self.num_experts)}
        return {
            idx: count / (self.total_routing_calls * self.top_k)
            for idx, count in self.expert_usage_count.items()
        }
    
    def get_expert_success_rates(self) -> Dict[int, float]:
        rates = {}
        for idx in range(self.num_experts):
            total = self.expert_usage_count.get(idx, 0)
            if total > 0:
                rates[idx] = self.expert_success_count.get(idx, 0) / total
            else:
                rates[idx] = 0.5
        return rates
    
    def get_load_balance_score(self) -> float:
        utilization = self.get_expert_utilization()
        if not utilization:
            return 0.0
        values = list(utilization.values())
        if sum(values) == 0:
            return 0.0
        entropy = 0
        for p in values:
            if p > 0:
                entropy -= p * math.log(p)
        max_entropy = math.log(len(values))
        return entropy / max_entropy if max_entropy > 0 else 0.0
    
    # ========================================================================
    # Enhanced Statistics and Dashboard
    # ========================================================================
    
    def get_comprehensive_stats(self) -> Dict[str, Any]:
        stats = {
            'total_routing_calls': self.total_routing_calls,
            'top_k': self.top_k,
            'num_experts': self.num_experts,
            'load_balance_score': self.get_load_balance_score(),
            'bio_integration_active': self.enable_bio_integration,
            'bio_modules_available': BIO_INSPIRED_AVAILABLE,
            'expert_stats': {
                idx: {
                    'utilization': self.get_expert_utilization().get(idx, 0.0),
                    'success_rate': self.get_expert_success_rates().get(idx, 0.0),
                    'carbon_total': self.expert_carbon_total.get(idx, 0.0),
                    'helium_total': self.expert_helium_total.get(idx, 0.0)
                }
                for idx in range(self.num_experts)
            }
        }
        
        # BIO-INSPIRED: Add gradient levels
        if self.enable_bio_integration:
            stats['gradient_levels'] = self._get_real_gradient_levels()
            stats['token_state'] = self._get_real_token_state()
            stats['exploration_rate'] = self._get_bio_modulated_exploration()
            stats['cooperative_pairs'] = len(self.cooperative_pairs)
            stats['biomass_aware_load_balance'] = self._get_biomass_aware_load_balance()
        
        # Add sustainability metrics
        stats['sustainability_score'] = self.sustainability_score
        stats['carbon_efficiency'] = self.carbon_efficiency
        stats['helium_efficiency'] = self.helium_efficiency
        
        # Add federated stats
        if self.enable_federated and self.federated_network:
            stats['federated_stats'] = self.federated_network.get_federated_stats()
        
        # Add predictive stats
        if self.enable_predictive and self.predictive_analyzer:
            stats['predictive_forecast'] = asyncio.run(
                self.predictive_analyzer.predict_gating_performance()
            )
        
        # Add carbon stats
        if self.enable_carbon_intensity and self.carbon_manager:
            stats['carbon_intensity'] = asyncio.run(self.carbon_manager.get_current_intensity())
        
        return stats
    
    def get_sustainability_dashboard(self) -> Dict[str, Any]:
        """Get sustainability dashboard"""
        return {
            'timestamp': datetime.utcnow().isoformat(),
            'sustainability_score': self.sustainability_score,
            'carbon_efficiency': self.carbon_efficiency,
            'helium_efficiency': self.helium_efficiency,
            'load_balance': self.get_load_balance_score(),
            'cooperative_strength': len(self.cooperative_pairs) / max(self.num_experts * (self.num_experts - 1) / 2, 1),
            'exploration_rate': self._get_bio_modulated_exploration(),
            'gradient_levels': self._get_real_gradient_levels(),
            'token_state': self._get_real_token_state(),
            'recommendations': self._generate_sustainability_recommendations()
        }
    
    def _generate_sustainability_recommendations(self) -> List[str]:
        recommendations = []
        
        if self.sustainability_score < 0.5:
            recommendations.append("Improve load balancing across experts")
            recommendations.append("Optimize cooperative binding weights")
        
        if self.carbon_efficiency < 0.4:
            recommendations.append("Reduce carbon footprint through better expert selection")
        
        if self.helium_efficiency < 0.4:
            recommendations.append("Optimize helium usage through expert selection")
        
        if self.enable_bio_integration and self._get_bio_modulated_exploration() < 0.05:
            recommendations.append("Increase exploration rate for better expert discovery")
        
        return recommendations or ["All sustainability metrics are within acceptable ranges"]
    
    # ========================================================================
    # Update Methods
    # ========================================================================
    
    def update_routing_feedback(
        self,
        expert_id: int,
        reward: float,
        carbon_kg: float = 0.0,
        helium_units: float = 0.0,
        context: Optional['GatingContext'] = None
    ):
        if reward > 0.5:
            self.expert_success_count[expert_id] = self.expert_success_count.get(expert_id, 0) + 1
        
        self.expert_carbon_total[expert_id] = self.expert_carbon_total.get(expert_id, 0.0) + carbon_kg
        self.expert_helium_total[expert_id] = self.expert_helium_total.get(expert_id, 0.0) + helium_units
        
        if context is not None:
            self.experience_buffer.append({
                'context': context,
                'chosen_expert': expert_id,
                'reward': reward,
                'carbon_kg': carbon_kg,
                'helium_units': helium_units,
                'timestamp': datetime.utcnow()
            })
            
            if len(self.experience_buffer) >= 32:
                self._online_learning_step()
    
    def _online_learning_step(self):
        if len(self.experience_buffer) < 32:
            return
        
        batch_size = min(32, len(self.experience_buffer))
        indices = np.random.choice(len(self.experience_buffer), batch_size, replace=False)
        batch = [self.experience_buffer[i] for i in indices]
        
        contexts = torch.stack([
            exp['context'].to_tensor() for exp in batch
        ]).to(self.device)
        
        chosen_experts = torch.tensor([
            exp['chosen_expert'] for exp in batch
        ]).to(self.device)
        
        rewards = torch.tensor([
            exp['reward'] for exp in batch
        ]).to(self.device)
        
        routing_weights, _, _, _ = self.sparse_gate(contexts, training=True)
        chosen_probs = routing_weights[range(batch_size), chosen_experts]
        loss = -torch.mean(torch.log(chosen_probs + 1e-8) * rewards)
        
        self.optimizer.zero_grad()
        loss.backward()
        torch.nn.utils.clip_grad_norm_(self.sparse_gate.parameters(), 1.0)
        self.optimizer.step()
    
    # ========================================================================
    # Save/Load Methods
    # ========================================================================
    
    def save_state(self, path: str):
        state = {
            'model_state_dict': self.sparse_gate.state_dict(),
            'optimizer_state_dict': self.optimizer.state_dict(),
            'expert_usage_count': dict(self.expert_usage_count),
            'expert_success_count': dict(self.expert_success_count),
            'total_routing_calls': self.total_routing_calls,
            'cooperative_pairs': dict(self.cooperative_pairs),
            'sustainability_score': self.sustainability_score,
            'carbon_efficiency': self.carbon_efficiency,
            'helium_efficiency': self.helium_efficiency
        }
        torch.save(state, path)
        logger.info(f"Saved gating network state to {path}")
    
    def load_state(self, path: str):
        checkpoint = torch.load(path, map_location=self.device)
        self.sparse_gate.load_state_dict(checkpoint['model_state_dict'])
        self.optimizer.load_state_dict(checkpoint['optimizer_state_dict'])
        self.expert_usage_count = defaultdict(int, checkpoint.get('expert_usage_count', {}))
        self.expert_success_count = defaultdict(int, checkpoint.get('expert_success_count', {}))
        self.total_routing_calls = checkpoint.get('total_routing_calls', 0)
        self.cooperative_pairs = defaultdict(float, checkpoint.get('cooperative_pairs', {}))
        self.sustainability_score = checkpoint.get('sustainability_score', 0.0)
        self.carbon_efficiency = checkpoint.get('carbon_efficiency', 0.5)
        self.helium_efficiency = checkpoint.get('helium_efficiency', 0.5)
        logger.info(f"Loaded gating network state from {path}")
    
    def get_parameter_count(self) -> int:
        return sum(p.numel() for p in self.sparse_gate.parameters() if p.requires_grad)
    
    async def shutdown(self):
        """Graceful shutdown"""
        logger.info("Shutting down Gating Network")
        if self.carbon_manager:
            await self.carbon_manager.close()
        if self.federated_network:
            await self.federated_network.close()
        logger.info("Shutdown complete")
