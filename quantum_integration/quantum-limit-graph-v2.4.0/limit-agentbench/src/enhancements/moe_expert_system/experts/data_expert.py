# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/moe_expert_system/experts/data_expert.py
"""
Enhanced Data Expert v7.0.0 - Complete Metabolic Data Processor
With Causal Analysis, Natural Language Explanations, Quality Reporting,
Federated Reflexive Learning, Predictive Reflexivity, Cross-Domain Knowledge Transfer,
Human-AI Collaborative Reflection, Enhanced Sustainability Features,
Cross-Expert Optimization, Predictive Sustainability, and Advanced Analytics
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple, Union, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import numpy as np
from collections import deque
import hashlib
import json
import zlib
from concurrent.futures import ThreadPoolExecutor
import random
import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import DataLoader, TensorDataset
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.metrics import mean_squared_error, r2_score
import aiohttp
import asyncio

logger = logging.getLogger(__name__)

# ============================================================================
# Bio-Inspired Import Check
# ============================================================================
try:
    from enhancements.bio_inspired.eco_atp_currency import EcoATPTokenManager, EcoATPConsumer
    from enhancements.bio_inspired.proton_gradient_fields import GradientFieldManager
    from enhancements.bio_inspired.atp_synthase_scheduler import ATPSynthaseScheduler
    from enhancements.bio_inspired.chromatophore_compartments import CompartmentManager, MembranePermeability
    from enhancements.bio_inspired.biomass_storage import BiomassStorage, StorageTier, GuaranteeLevel
    from enhancements.bio_inspired.photosynthetic_harvester import PhotosyntheticHarvester
    BIO_INSPIRED_AVAILABLE = True
except ImportError:
    BIO_INSPIRED_AVAILABLE = False

try:
    from ..expert_registry import ExpertProfile, ExpertDomain, HardwareProfile
except ImportError:
    class ExpertDomain(Enum): DATA = "data_engineering"
    class HardwareProfile(Enum): HYBRID = "hybrid_cpu_gpu"

# ============================================================================
# Enums and Data Classes
# ============================================================================
class DataTier(Enum): HOT = "hot"; WARM = "warm"; COLD = "cold"; FROZEN = "frozen"
class DataQuality(Enum): EXCELLENT = "excellent"; GOOD = "good"; FAIR = "fair"; POOR = "poor"; UNUSABLE = "unusable"
class StreamingMode(Enum): REALTIME = "realtime"; NEAR_REALTIME = "near_realtime"; MICRO_BATCH = "micro_batch"; BATCH = "batch"
class PipelineStatus(Enum): HEALTHY = "healthy"; DEGRADED = "degraded"; RECOVERING = "recovering"; FAILED = "failed"; PAUSED = "paused"

@dataclass
class DataQualityMetrics:
    completeness: float = 0.0; accuracy: float = 0.0; consistency: float = 0.0
    timeliness: float = 0.0; uniqueness: float = 0.0; validity: float = 0.0
    overall_score: float = 0.0; harvester_confidence: float = 0.5
    sustainability_impact: float = 0.0  # Sustainability metric
    
    def __post_init__(self):
        weights = {'completeness': 0.25, 'accuracy': 0.25, 'consistency': 0.15,
                   'timeliness': 0.15, 'uniqueness': 0.10, 'validity': 0.10}
        self.overall_score = (self.completeness * weights['completeness'] + self.accuracy * weights['accuracy'] +
                             self.consistency * weights['consistency'] + self.timeliness * weights['timeliness'] +
                             self.uniqueness * weights['uniqueness'] + self.validity * weights['validity'])
        self.sustainability_impact = self.overall_score * 0.7 + self.harvester_confidence * 0.3

@dataclass
class DataLineage:
    lineage_id: str; source: str
    transformations: List[Dict[str, Any]] = field(default_factory=list)
    quality_at_source: Optional[DataQualityMetrics] = None
    carbon_footprint_kg: float = 0.0; helium_consumed: float = 0.0
    created_at: datetime = field(default_factory=datetime.utcnow); checksum: str = ""
    biomass_storage_token: Optional[str] = None; ecoatp_cost: float = 0.0
    federated_round: int = 0
    cross_domain_transfers: List[str] = field(default_factory=list)
    cross_expert_optimization: Dict[str, Any] = field(default_factory=dict)
    
    def add_transformation(self, transform_name: str, params: Dict[str, Any]):
        self.transformations.append({'name': transform_name, 'params': params,
                                     'timestamp': datetime.utcnow().isoformat(), 'checksum_before': self.checksum})
    
    def add_cross_domain_transfer(self, source_domain: str, target_domain: str):
        self.cross_domain_transfers.append(f"{source_domain}→{target_domain}")
    
    def add_cross_expert_optimization(self, expert_type: str, optimization: Dict):
        self.cross_expert_optimization[expert_type] = optimization

@dataclass
class FederatedLearningState:
    """State for federated reflexive learning"""
    round: int = 0
    local_model_weights: Dict = field(default_factory=dict)
    global_model_weights: Dict = field(default_factory=dict)
    contribution_score: float = 0.0
    participants: List[str] = field(default_factory=list)
    last_aggregation: Optional[datetime] = None
    peer_contributions: List[Dict] = field(default_factory=list)

@dataclass
class PredictiveQualityForecast:
    """Predictive quality forecast"""
    timestamp: datetime = field(default_factory=datetime.utcnow)
    predicted_score: float = 0.0
    confidence: float = 0.0
    trend: str = "stable"
    factors: List[Dict[str, Any]] = field(default_factory=list)
    carbon_forecast: Optional[Dict] = None
    helium_forecast: Optional[Dict] = None
    recommended_actions: List[str] = field(default_factory=list)

@dataclass
class CrossDomainKnowledge:
    """Cross-domain knowledge transfer structure"""
    source_domain: str
    target_domain: str
    knowledge_type: str
    data: Dict[str, Any]
    effectiveness_score: float = 0.0
    transfer_count: int = 0
    last_used: Optional[datetime] = None

@dataclass
class CrossExpertOptimization:
    """Cross-expert optimization result"""
    expert_type: str
    optimization_id: str
    score: float = 0.0
    decisions: Dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.utcnow)
    sustainability_impact: float = 0.0

@dataclass
class PredictiveSustainabilityMetrics:
    """Predictive sustainability metrics"""
    timestamp: datetime = field(default_factory=datetime.utcnow)
    predicted_carbon_impact_24h: float = 0.0
    predicted_helium_consumption_24h: float = 0.0
    predicted_energy_consumption_24h: float = 0.0
    confidence_level: float = 0.0
    recommended_actions: List[str] = field(default_factory=list)
    risk_factors: List[str] = field(default_factory=list)

# ============================================================================
# Enhanced Federated Reflexive Learning Module
# ============================================================================

class EnhancedFederatedReflexiveDataLearner:
    """Enhanced federated reflexive learning with peer aggregation"""
    
    def __init__(self, expert_id: str, server_url: Optional[str] = None):
        self.expert_id = expert_id
        self.server_url = server_url
        self.state = FederatedLearningState()
        self._lock = asyncio.Lock()
        self._session = None
        self.local_quality_model = None
        self.global_quality_model = None
        self.ensemble_models = {}
        self.peer_cache = {}
        
        # Initialize local model
        self._init_quality_model()
    
    def _init_quality_model(self):
        """Initialize local quality prediction model"""
        class QualityPredictor(nn.Module):
            def __init__(self, input_size=10, hidden_size=64):
                super().__init__()
                self.network = nn.Sequential(
                    nn.Linear(input_size, hidden_size),
                    nn.ReLU(),
                    nn.BatchNorm1d(hidden_size),
                    nn.Linear(hidden_size, hidden_size // 2),
                    nn.ReLU(),
                    nn.BatchNorm1d(hidden_size // 2),
                    nn.Linear(hidden_size // 2, 1)
                )
            
            def forward(self, x):
                return self.network(x)
        
        self.local_quality_model = QualityPredictor()
        self.global_quality_model = QualityPredictor()
    
    async def _get_session(self):
        if self._session is None and self.server_url:
            self._session = aiohttp.ClientSession()
        return self._session
    
    async def train_local_model(self, quality_data: List[Dict[str, float]], epochs: int = 10) -> float:
        """Train local quality prediction model on new data"""
        if not quality_data:
            return 0.0
        
        X = []
        y = []
        for item in quality_data:
            X.append([
                item.get('completeness', 0.5),
                item.get('accuracy', 0.5),
                item.get('consistency', 0.5),
                item.get('timeliness', 0.5),
                item.get('uniqueness', 0.5),
                item.get('validity', 0.5),
                item.get('harvester_confidence', 0.5),
                item.get('size_mb', 100) / 1000,
                item.get('compression_ratio', 0.5),
                item.get('ecoatp_cost', 0) / 10
            ])
            y.append(item.get('overall_score', 0.5))
        
        X = torch.FloatTensor(X)
        y = torch.FloatTensor(y).unsqueeze(1)
        
        dataset = TensorDataset(X, y)
        dataloader = DataLoader(dataset, batch_size=32, shuffle=True)
        
        optimizer = optim.Adam(self.local_quality_model.parameters(), lr=0.001)
        criterion = nn.MSELoss()
        
        total_loss = 0
        for epoch in range(epochs):
            epoch_loss = 0
            for batch_X, batch_y in dataloader:
                optimizer.zero_grad()
                output = self.local_quality_model(batch_X)
                loss = criterion(output, batch_y)
                loss.backward()
                torch.nn.utils.clip_grad_norm_(self.local_quality_model.parameters(), 1.0)
                optimizer.step()
                epoch_loss += loss.item()
            total_loss += epoch_loss
        
        avg_loss = total_loss / epochs
        logger.info(f"Local quality model trained. Loss: {avg_loss:.4f}")
        return avg_loss
    
    async def send_local_update(self, performance_metric: float = 1.0) -> Dict:
        """Send local model update to federated server"""
        if not self.server_url:
            return {'status': 'disabled'}
        
        async with self._lock:
            session = await self._get_session()
            
            try:
                weights = self.local_quality_model.state_dict()
                weights_serialized = {k: v.tolist() for k, v in weights.items()}
                
                update_data = {
                    'expert_id': self.expert_id,
                    'round': self.state.round,
                    'weights': weights_serialized,
                    'performance': performance_metric,
                    'timestamp': datetime.utcnow().isoformat()
                }
                
                async with session.post(
                    f"{self.server_url}/federated/update",
                    json=update_data,
                    timeout=30
                ) as response:
                    if response.status == 200:
                        result = await response.json()
                        self.state.round += 1
                        self.state.contribution_score += performance_metric
                        logger.info(f"Federated update sent. Round: {self.state.round}")
                        return result
                    else:
                        logger.error(f"Federated update failed: {response.status}")
                        return {'status': 'failed'}
                        
            except Exception as e:
                logger.error(f"Federated update error: {e}")
                return {'status': 'error'}
    
    async def get_global_model(self) -> Optional[Dict]:
        """Get global model from federated server"""
        if not self.server_url:
            return None
        
        async with self._lock:
            session = await self._get_session()
            
            try:
                async with session.get(
                    f"{self.server_url}/federated/global/quality",
                    timeout=30
                ) as response:
                    if response.status == 200:
                        data = await response.json()
                        weights = data.get('weights', {})
                        self.state.global_model_weights = weights
                        self.state.round = data.get('round', 0)
                        self.state.participants = data.get('participants', [])
                        
                        for k, v in weights.items():
                            self.global_quality_model.state_dict()[k] = torch.FloatTensor(v)
                        
                        return weights
                        
            except Exception as e:
                logger.error(f"Global model fetch error: {e}")
                return None
    
    async def aggregate_peer_models(self, peer_updates: List[Dict]) -> Dict:
        """Aggregate model updates from peers"""
        if not peer_updates:
            return {}
        
        aggregated = {}
        for key in peer_updates[0].keys():
            peer_weights = [p[key] for p in peer_updates]
            aggregated[key] = np.mean(peer_weights, axis=0)
        
        return aggregated
    
    async def participate_in_round(self, quality_data: List[Dict[str, float]], 
                                  performance: float = 1.0) -> Dict:
        """Full participation in federated learning round"""
        await self.train_local_model(quality_data)
        result = await self.send_local_update(performance)
        global_weights = await self.get_global_model()
        
        if global_weights:
            self.state.global_model_weights = global_weights
            self.state.participants.append(self.expert_id)
        
        return {
            'round': self.state.round,
            'participated': bool(global_weights),
            'contribution_score': self.state.contribution_score,
            'performance': performance,
            'peer_count': len(self.state.participants),
            'timestamp': datetime.utcnow().isoformat()
        }
    
    def get_federated_insights(self) -> Dict:
        """Get insights from federated learning"""
        return {
            'round': self.state.round,
            'contribution_score': self.state.contribution_score,
            'participants': len(self.state.participants),
            'has_global_model': bool(self.state.global_model_weights),
            'last_aggregation': self.state.last_aggregation.isoformat() if self.state.last_aggregation else None,
            'peer_contributions': len(self.state.peer_contributions)
        }
    
    async def close(self):
        if self._session:
            await self._session.close()

# ============================================================================
# Enhanced Predictive Reflexivity Module
# ============================================================================

class EnhancedPredictiveQualityForecaster:
    """Enhanced predictive reflexivity with ensemble forecasting"""
    
    def __init__(self, history_window: int = 100):
        self.history_window = history_window
        self.quality_history = deque(maxlen=history_window)
        self.forecast_history = deque(maxlen=50)
        self.models = {}
        self.scaler = StandardScaler()
        self.is_trained = False
        self.ensemble_models = []
        
        # Multiple models for ensemble
        self.models['random_forest'] = RandomForestRegressor(n_estimators=100, random_state=42)
        self.models['gradient_boosting'] = GradientBoostingRegressor(n_estimators=100, random_state=42)
        self.models['linear'] = None  # Will be initialized later
        
        self.anomaly_threshold = 0.3
        self.sustainability_models = {}
    
    def update_history(self, quality_metrics: DataQualityMetrics):
        """Update quality history for forecasting"""
        self.quality_history.append({
            'timestamp': datetime.utcnow(),
            'score': quality_metrics.overall_score,
            'completeness': quality_metrics.completeness,
            'accuracy': quality_metrics.accuracy,
            'consistency': quality_metrics.consistency,
            'timeliness': quality_metrics.timeliness,
            'uniqueness': quality_metrics.uniqueness,
            'validity': quality_metrics.validity,
            'sustainability_impact': quality_metrics.sustainability_impact
        })
    
    async def train_forecast_model(self):
        """Train ensemble of predictive models"""
        if len(self.quality_history) < 10:
            return {'status': 'insufficient_data', 'samples': len(self.quality_history)}
        
        X = []
        y = []
        history_list = list(self.quality_history)
        
        for i in range(len(history_list) - 5):
            features = []
            for j in range(5):
                data = history_list[i + j]
                features.extend([
                    data['score'],
                    data['completeness'],
                    data['accuracy'],
                    data['consistency'],
                    data['timeliness'],
                    data['sustainability_impact']
                ])
            X.append(features)
            y.append(history_list[i + 5]['score'])
        
        X = np.array(X)
        y = np.array(y)
        
        X_scaled = self.scaler.fit_transform(X)
        
        # Train ensemble models
        results = {}
        for name, model in self.models.items():
            if model is not None:
                model.fit(X_scaled, y)
                predictions = model.predict(X_scaled)
                r2 = r2_score(y, predictions)
                results[name] = r2
        
        self.is_trained = True
        logger.info(f"Ensemble forecast models trained. R² scores: {results}")
        return {'status': 'success', 'results': results, 'samples': len(X)}
    
    async def predict_quality_trend(self, hours: int = 24) -> PredictiveQualityForecast:
        """Predict future quality trends with ensemble"""
        if not self.is_trained or len(self.quality_history) < 10:
            return PredictiveQualityForecast(
                predicted_score=0.5,
                confidence=0.0,
                trend="insufficient_data"
            )
        
        recent = list(self.quality_history)[-5:]
        features = []
        for data in recent:
            features.extend([
                data['score'],
                data['completeness'],
                data['accuracy'],
                data['consistency'],
                data['timeliness'],
                data['sustainability_impact']
            ])
        
        features = np.array(features).reshape(1, -1)
        features_scaled = self.scaler.transform(features)
        
        # Ensemble predictions
        predictions = []
        for name, model in self.models.items():
            if model is not None:
                pred = model.predict(features_scaled)[0]
                predictions.append(pred)
        
        if not predictions:
            return PredictiveQualityForecast(predicted_score=0.5, confidence=0.0, trend="no_models")
        
        # Weighted average
        prediction = np.mean(predictions)
        confidence = min(0.9, np.std(predictions) / 0.2) if len(predictions) > 1 else 0.5
        
        # Determine trend
        if len(self.forecast_history) > 5:
            recent_forecasts = list(self.forecast_history)[-5:]
            trend = "increasing" if prediction > recent_forecasts[-1] else "decreasing" if prediction < recent_forecasts[-1] else "stable"
        else:
            trend = "stable"
        
        # Generate sustainability forecast
        carbon_forecast = self._predict_carbon_impact()
        helium_forecast = self._predict_helium_consumption()
        recommended_actions = self._generate_predictive_actions(prediction, carbon_forecast, helium_forecast)
        
        forecast = PredictiveQualityForecast(
            predicted_score=prediction,
            confidence=confidence,
            trend=trend,
            factors=[
                {'name': 'Ensemble average', 'value': prediction, 'weight': 0.6},
                {'name': 'Model confidence', 'value': confidence, 'weight': 0.4}
            ],
            carbon_forecast=carbon_forecast,
            helium_forecast=helium_forecast,
            recommended_actions=recommended_actions
        )
        
        self.forecast_history.append(forecast)
        return forecast
    
    def _predict_carbon_impact(self) -> Dict:
        """Predict future carbon impact"""
        if len(self.quality_history) < 10:
            return {'predicted': 0.5, 'confidence': 0.0}
        
        recent_scores = [q['sustainability_impact'] for q in list(self.quality_history)[-10:]]
        trend = np.polyfit(range(len(recent_scores)), recent_scores, 1)[0]
        
        return {
            'predicted': recent_scores[-1] + trend * 24,  # 24-hour forecast
            'trend': 'improving' if trend > 0 else 'declining',
            'confidence': 0.7 if len(recent_scores) > 20 else 0.5
        }
    
    def _predict_helium_consumption(self) -> Dict:
        """Predict future helium consumption"""
        if len(self.quality_history) < 10:
            return {'predicted': 0.5, 'confidence': 0.0}
        
        # Simulate helium consumption prediction based on data size
        recent_sizes = [q.get('size_mb', 100) for q in list(self.quality_history)[-10:]]
        avg_size = np.mean(recent_sizes)
        
        return {
            'predicted': avg_size * 0.01,  # Rough estimate
            'confidence': 0.6,
            'units': 'liters'
        }
    
    def _generate_predictive_actions(self, quality_score: float, carbon_forecast: Dict, helium_forecast: Dict) -> List[str]:
        """Generate recommended actions based on forecasts"""
        actions = []
        
        if quality_score < 0.7:
            actions.append("Improve data quality through enhanced validation")
        
        if carbon_forecast and carbon_forecast.get('trend') == 'declining':
            actions.append("Optimize processing to reduce carbon footprint")
        
        if helium_forecast and helium_forecast.get('predicted', 0) > 0.5:
            actions.append("Implement helium-efficient compression strategies")
        
        return actions or ["Current trends are sustainable"]
    
    def detect_anomaly(self, current_quality: DataQualityMetrics) -> Tuple[bool, float]:
        """Detect quality anomalies"""
        if len(self.quality_history) < 10:
            return False, 0.0
        
        recent_scores = [q['score'] for q in list(self.quality_history)[-10:]]
        mean_score = np.mean(recent_scores)
        std_score = np.std(recent_scores)
        
        z_score = abs(current_quality.overall_score - mean_score) / max(std_score, 0.01)
        is_anomaly = z_score > 3
        
        return is_anomaly, z_score

# ============================================================================
# Enhanced Cross-Domain Knowledge Transfer Module
# ============================================================================

class EnhancedCrossDomainKnowledgeTransfer:
    """Enhanced cross-domain knowledge transfer with sustainability focus"""
    
    def __init__(self):
        self.knowledge_base: Dict[str, Dict[str, CrossDomainKnowledge]] = {}
        self.transfer_logs = deque(maxlen=1000)
        self.domain_mappings = {
            'data→energy': {
                'compression_strategies': ['adaptive', 'greedy', 'bio-inspired', 'predictive'],
                'quality_patterns': ['cyclic', 'event-driven', 'continuous', 'burst'],
                'sustainability_patterns': ['efficiency-first', 'carbon-aware', 'helium-efficient']
            },
            'data→quantum': {
                'encoding_schemes': ['amplitude', 'basis', 'angle', 'phase'],
                'preprocessing_techniques': ['normalization', 'feature_extraction', 'dimensionality_reduction'],
                'quantum_advantage': ['hybrid', 'pure', 'simulated']
            },
            'data→carbon': {
                'intensity_patterns': ['diurnal', 'seasonal', 'event-based', 'trending'],
                'optimization_strategies': ['load-shifting', 'efficiency-first', 'renewable-tracking'],
                'forecasting_methods': ['time-series', 'ml', 'ensemble']
            },
            'data→helium': {
                'scarcity_patterns': ['supply-constrained', 'price-sensitive', 'demand-driven'],
                'efficiency_strategies': ['recovery', 'reuse', 'minimization', 'optimization'],
                'market_indicators': ['price-index', 'supply-risk', 'substitution']
            }
        }
        self._lock = asyncio.Lock()
        self.effectiveness_history = deque(maxlen=100)
    
    def transfer_knowledge(self, source_domain: str, target_domain: str, 
                          knowledge_type: str, data: Dict[str, Any]) -> CrossDomainKnowledge:
        """Transfer knowledge between domains with effectiveness tracking"""
        key = f"{source_domain}→{target_domain}"
        
        knowledge = CrossDomainKnowledge(
            source_domain=source_domain,
            target_domain=target_domain,
            knowledge_type=knowledge_type,
            data=data,
            transfer_count=1,
            last_used=datetime.utcnow()
        )
        
        if key not in self.knowledge_base:
            self.knowledge_base[key] = {}
        
        if knowledge_type not in self.knowledge_base[key]:
            self.knowledge_base[key][knowledge_type] = knowledge
        else:
            existing = self.knowledge_base[key][knowledge_type]
            existing.transfer_count += 1
            existing.data.update(data)
            existing.last_used = datetime.utcnow()
            knowledge = existing
        
        self.transfer_logs.append({
            'timestamp': datetime.utcnow(),
            'source': source_domain,
            'target': target_domain,
            'type': knowledge_type,
            'effectiveness': knowledge.effectiveness_score
        })
        
        logger.info(f"Knowledge transferred: {source_domain}→{target_domain} ({knowledge_type})")
        return knowledge
    
    def get_transferred_knowledge(self, source_domain: str, target_domain: str, 
                                 knowledge_type: str) -> Optional[CrossDomainKnowledge]:
        """Retrieve transferred knowledge"""
        key = f"{source_domain}→{target_domain}"
        if key in self.knowledge_base and knowledge_type in self.knowledge_base[key]:
            return self.knowledge_base[key][knowledge_type]
        return None
    
    async def apply_energy_knowledge(self, compression_data: Dict) -> Dict:
        """Apply knowledge from energy domain to data compression"""
        energy_knowledge = self.get_transferred_knowledge('energy', 'data', 'compression_strategies')
        if energy_knowledge:
            strategies = energy_knowledge.data.get('strategies', [])
            if strategies:
                return {
                    'applied_strategy': strategies[0],
                    'expected_savings': energy_knowledge.effectiveness_score * 0.1,
                    'source': 'energy_domain',
                    'confidence': min(1.0, energy_knowledge.transfer_count / 10)
                }
        return {'applied_strategy': 'default', 'source': 'local', 'confidence': 0.5}
    
    async def apply_carbon_knowledge(self, quality_data: Dict) -> Dict:
        """Apply knowledge from carbon domain to data quality"""
        carbon_knowledge = self.get_transferred_knowledge('carbon', 'data', 'optimization_strategies')
        if carbon_knowledge:
            return {
                'carbon_aware_processing': True,
                'optimization_impact': carbon_knowledge.effectiveness_score,
                'source': 'carbon_domain',
                'strategies': carbon_knowledge.data.get('strategies', [])
            }
        return {'carbon_aware_processing': False, 'source': 'local'}
    
    async def apply_helium_knowledge(self, compression_data: Dict) -> Dict:
        """Apply knowledge from helium domain to data compression"""
        helium_knowledge = self.get_transferred_knowledge('helium', 'data', 'efficiency_strategies')
        if helium_knowledge:
            return {
                'helium_efficient': True,
                'efficiency_gain': helium_knowledge.effectiveness_score * 0.15,
                'source': 'helium_domain',
                'strategies': helium_knowledge.data.get('strategies', [])
            }
        return {'helium_efficient': False, 'source': 'local'}
    
    def update_effectiveness(self, source_domain: str, target_domain: str, 
                            knowledge_type: str, effectiveness: float):
        """Update effectiveness of knowledge transfer"""
        key = f"{source_domain}→{target_domain}"
        if key in self.knowledge_base and knowledge_type in self.knowledge_base[key]:
            knowledge = self.knowledge_base[key][knowledge_type]
            knowledge.effectiveness_score = (knowledge.effectiveness_score * knowledge.transfer_count + effectiveness) / (knowledge.transfer_count + 1)
            self.effectiveness_history.append({
                'timestamp': datetime.utcnow(),
                'transfer': key,
                'type': knowledge_type,
                'effectiveness': knowledge.effectiveness_score
            })
    
    def get_transfer_statistics(self) -> Dict:
        """Get cross-domain transfer statistics"""
        total_transfers = len(self.transfer_logs)
        domain_pairs = {}
        
        for log in self.transfer_logs:
            key = f"{log['source']}→{log['target']}"
            domain_pairs[key] = domain_pairs.get(key, 0) + 1
        
        avg_effectiveness = np.mean([log.get('effectiveness', 0.5) for log in self.transfer_logs[-50:]]) if self.transfer_logs else 0
        
        return {
            'total_transfers': total_transfers,
            'domain_pairs': domain_pairs,
            'knowledge_types': list(self.knowledge_base.keys()),
            'average_effectiveness': avg_effectiveness,
            'recent_transfers': list(self.transfer_logs)[-10:],
            'active_domains': len(self.domain_mappings)
        }

# ============================================================================
# Enhanced Human-AI Collaborative Reflection Module
# ============================================================================

class EnhancedHumanAICollaborativeReflector:
    """Enhanced human-AI collaborative reflection with sustainability focus"""
    
    def __init__(self):
        self.feedback_history = deque(maxlen=1000)
        self.reflection_logs = deque(maxlen=100)
        self.quality_thresholds = {
            'excellent': 0.9,
            'good': 0.7,
            'fair': 0.5,
            'poor': 0.3
        }
        self.user_preferences = {}
        self._lock = asyncio.Lock()
        self.sustainability_feedback = deque(maxlen=100)
        self.action_tracking = {}
    
    def collect_feedback(self, user_id: str, feedback: Dict) -> Dict:
        """Collect human feedback with sustainability context"""
        feedback_entry = {
            'user_id': user_id,
            'timestamp': datetime.utcnow(),
            'feedback': feedback,
            'sustainability_context': feedback.get('sustainability', {})
        }
        self.feedback_history.append(feedback_entry)
        
        if feedback.get('sustainability'):
            self.sustainability_feedback.append({
                'timestamp': datetime.utcnow(),
                'user_id': user_id,
                'sustainability_concern': feedback['sustainability']
            })
        
        if 'preference' in feedback:
            self.user_preferences[user_id] = feedback['preference']
        
        reflection = self._generate_reflection(feedback)
        self.reflection_logs.append(reflection)
        
        return reflection
    
    def _generate_reflection(self, feedback: Dict) -> Dict:
        """Generate AI reflection with sustainability insights"""
        reflection = {
            'timestamp': datetime.utcnow().isoformat(),
            'acknowledgment': f"Feedback received on {feedback.get('topic', 'data quality')}",
            'insights': [],
            'actions': [],
            'sustainability_insights': []
        }
        
        if 'concern' in feedback:
            if feedback['concern'] == 'accuracy':
                reflection['insights'].append("Accuracy can be improved through enhanced validation")
                reflection['actions'].append("Implement additional validation rules")
            elif feedback['concern'] == 'completeness':
                reflection['insights'].append("Completeness issues may indicate data collection gaps")
                reflection['actions'].append("Review data collection processes")
            elif feedback['concern'] == 'timeliness':
                reflection['insights'].append("Timeliness can be improved through streaming optimization")
                reflection['actions'].append("Implement near-realtime processing")
        
        if 'sustainability' in feedback:
            sustainability = feedback['sustainability']
            if sustainability.get('carbon_concern'):
                reflection['sustainability_insights'].append("Carbon footprint optimization is a priority")
                reflection['actions'].append("Implement carbon-aware processing")
            if sustainability.get('helium_concern'):
                reflection['sustainability_insights'].append("Helium efficiency improvements needed")
                reflection['actions'].append("Optimize compression for helium conservation")
        
        if 'suggestion' in feedback:
            reflection['actions'].append(f"Implementing suggestion: {feedback['suggestion']}")
            reflection['insights'].append("User suggestion incorporated into improvement plan")
        
        reflection['action_items'] = self._prioritize_actions(reflection['actions'])
        return reflection
    
    def _prioritize_actions(self, actions: List[str]) -> List[Dict]:
        """Prioritize action items with sustainability weighting"""
        priorities = []
        for action in actions:
            priority = 'low'
            impact = 0.3
            effort = 'medium'
            
            # Check for sustainability keywords
            if any(keyword in action.lower() for keyword in ['carbon', 'helium', 'sustain']):
                priority = 'high'
                impact = 0.9
            
            if any(keyword in action.lower() for keyword in ['critical', 'immediate']):
                priority = 'high'
                impact = 0.9
            
            if any(keyword in action.lower() for keyword in ['optimize', 'improve']):
                priority = 'medium'
                impact = 0.6
            
            priorities.append({
                'action': action,
                'priority': priority,
                'impact': impact,
                'estimated_effort': effort,
                'sustainability_weight': 0.5 if 'sustain' in action.lower() else 0.0
            })
        
        return sorted(priorities, key=lambda x: (x['impact'] + x['sustainability_weight']), reverse=True)
    
    def get_collaborative_insights(self) -> Dict:
        """Get collaborative insights with sustainability focus"""
        if len(self.feedback_history) < 5:
            return {'status': 'insufficient_feedback'}
        
        recent_feedback = list(self.feedback_history)[-20:]
        topics = {}
        sustainability_concerns = {}
        
        for f in recent_feedback:
            topic = f['feedback'].get('topic', 'general')
            topics[topic] = topics.get(topic, 0) + 1
            
            if 'sustainability' in f['feedback']:
                concern = f['feedback']['sustainability'].get('concern', 'general')
                sustainability_concerns[concern] = sustainability_concerns.get(concern, 0) + 1
        
        most_common = max(topics.items(), key=lambda x: x[1]) if topics else ('none', 0)
        top_sustainability = max(sustainability_concerns.items(), key=lambda x: x[1]) if sustainability_concerns else ('none', 0)
        
        return {
            'total_feedback': len(self.feedback_history),
            'top_topics': topics,
            'most_common_topic': most_common[0],
            'sustainability_concerns': sustainability_concerns,
            'top_sustainability_concern': top_sustainability[0],
            'engagement_score': min(1.0, len(self.feedback_history) / 100),
            'user_count': len(set(f['user_id'] for f in self.feedback_history))
        }
    
    def get_quality_rating(self, quality_score: float) -> str:
        """Get human-friendly quality rating"""
        if quality_score >= self.quality_thresholds['excellent']:
            return "EXCELLENT"
        elif quality_score >= self.quality_thresholds['good']:
            return "GOOD"
        elif quality_score >= self.quality_thresholds['fair']:
            return "FAIR"
        elif quality_score >= self.quality_thresholds['poor']:
            return "POOR"
        else:
            return "UNUSABLE"

# ============================================================================
# Cross-Expert Optimization Module
# ============================================================================

class CrossExpertOptimizer:
    """Cross-expert optimization across multiple domains"""
    
    def __init__(self):
        self.optimization_history = deque(maxlen=1000)
        self.expert_hints = {}
        self._lock = asyncio.Lock()
        self.cross_expert_scores = {}
    
    def register_expert_hint(self, expert_type: str, hint: Dict[str, Any]):
        """Register optimization hint from an expert"""
        if expert_type not in self.expert_hints:
            self.expert_hints[expert_type] = []
        self.expert_hints[expert_type].append({
            'timestamp': datetime.utcnow(),
            'hint': hint
        })
    
    async def optimize_cross_expert(self, expert_hints: Dict) -> CrossExpertOptimization:
        """Optimize across multiple experts"""
        optimization_id = hashlib.md5(f"{expert_hints}{datetime.utcnow()}".encode()).hexdigest()[:12]
        
        # Calculate combined score
        score = 0.0
        decisions = {}
        sustainability_impact = 0.0
        
        for expert_type, hint in expert_hints.items():
            weight = self._get_expert_weight(expert_type)
            score += hint.get('score', 0.5) * weight
            
            if expert_type == 'energy':
                decisions['energy_efficiency'] = hint.get('efficiency', 0.5)
                sustainability_impact += hint.get('carbon_savings', 0) * 0.3
            
            if expert_type == 'carbon':
                decisions['carbon_aware'] = hint.get('carbon_aware', False)
                sustainability_impact += hint.get('savings', 0) * 0.4
            
            if expert_type == 'helium':
                decisions['helium_efficient'] = hint.get('helium_efficient', False)
                sustainability_impact += hint.get('savings', 0) * 0.3
        
        # Normalize
        score = score / len(expert_hints) if expert_hints else 0.5
        sustainability_impact = min(1.0, sustainability_impact)
        
        optimization = CrossExpertOptimization(
            expert_type='combined',
            optimization_id=optimization_id,
            score=score,
            decisions=decisions,
            sustainability_impact=sustainability_impact
        )
        
        self.optimization_history.append(optimization)
        self.cross_expert_scores[optimization_id] = score
        
        return optimization
    
    def _get_expert_weight(self, expert_type: str) -> float:
        """Get weight for expert type"""
        weights = {
            'energy': 0.3,
            'carbon': 0.35,
            'helium': 0.25,
            'quantum': 0.1
        }
        return weights.get(expert_type, 0.25)
    
    def get_optimization_summary(self) -> Dict:
        """Get cross-expert optimization summary"""
        if not self.optimization_history:
            return {'status': 'no_optimizations'}
        
        recent = list(self.optimization_history)[-100:]
        avg_score = np.mean([o.score for o in recent])
        avg_sustainability = np.mean([o.sustainability_impact for o in recent])
        
        return {
            'total_optimizations': len(self.optimization_history),
            'average_score': avg_score,
            'average_sustainability_impact': avg_sustainability,
            'best_score': max([o.score for o in recent]) if recent else 0,
            'expert_hints_received': {k: len(v) for k, v in self.expert_hints.items()},
            'recent_optimizations': [{'id': o.optimization_id, 'score': o.score} for o in recent[-5:]]
        }
    
    def get_sustainability_impact(self, optimization_id: str) -> Optional[float]:
        """Get sustainability impact of an optimization"""
        for opt in self.optimization_history:
            if opt.optimization_id == optimization_id:
                return opt.sustainability_impact
        return None

# ============================================================================
# Predictive Sustainability Module
# ============================================================================

class PredictiveSustainabilityAnalyzer:
    """Predictive sustainability analytics"""
    
    def __init__(self):
        self.sustainability_history = deque(maxlen=1000)
        self.prediction_models = {}
        self._lock = asyncio.Lock()
        self.forecast_horizon = 24  # hours
    
    def update_sustainability_metrics(self, metrics: Dict):
        """Update sustainability metrics history"""
        self.sustainability_history.append({
            'timestamp': datetime.utcnow(),
            'carbon_intensity': metrics.get('carbon_intensity', 0),
            'helium_efficiency': metrics.get('helium_efficiency', 0),
            'energy_consumption': metrics.get('energy_consumption', 0),
            'sustainability_score': metrics.get('sustainability_score', 0)
        })
    
    async def predict_sustainability_impact(self, future_workload: Dict) -> PredictiveSustainabilityMetrics:
        """Predict future sustainability impact"""
        if len(self.sustainability_history) < 10:
            return PredictiveSustainabilityMetrics(
                confidence_level=0.0,
                recommended_actions=['Insufficient data for prediction'],
                risk_factors=['Limited historical data']
            )
        
        recent = list(self.sustainability_history)[-50:]
        
        # Calculate trends
        carbon_trend = np.polyfit(range(len(recent)), [r['carbon_intensity'] for r in recent], 1)[0]
        helium_trend = np.polyfit(range(len(recent)), [r['helium_efficiency'] for r in recent], 1)[0]
        energy_trend = np.polyfit(range(len(recent)), [r['energy_consumption'] for r in recent], 1)[0]
        
        # Predict 24-hour impact
        predicted_carbon = recent[-1]['carbon_intensity'] + carbon_trend * 24
        predicted_helium = recent[-1]['helium_efficiency'] + helium_trend * 24
        predicted_energy = recent[-1]['energy_consumption'] + energy_trend * 24
        
        # Confidence based on data stability
        std_dev = np.std([r['sustainability_score'] for r in recent])
        confidence = max(0.1, min(0.9, 1.0 - std_dev))
        
        # Generate actions and risks
        actions = []
        risks = []
        
        if carbon_trend > 0:
            actions.append("Optimize for carbon reduction")
            risks.append("Increasing carbon footprint")
        
        if helium_trend < 0:
            actions.append("Improve helium efficiency")
            risks.append("Declining helium efficiency")
        
        if energy_trend > 0:
            actions.append("Reduce energy consumption")
            risks.append("Rising energy consumption")
        
        if not actions:
            actions.append("Current sustainability trends are positive")
        
        return PredictiveSustainabilityMetrics(
            predicted_carbon_impact_24h=predicted_carbon,
            predicted_helium_consumption_24h=predicted_helium,
            predicted_energy_consumption_24h=predicted_energy,
            confidence_level=confidence,
            recommended_actions=actions,
            risk_factors=risks
        )
    
    def get_sustainability_summary(self) -> Dict:
        """Get sustainability summary"""
        if not self.sustainability_history:
            return {'status': 'insufficient_data'}
        
        recent = list(self.sustainability_history)[-50:]
        
        return {
            'average_carbon_intensity': np.mean([r['carbon_intensity'] for r in recent]),
            'average_helium_efficiency': np.mean([r['helium_efficiency'] for r in recent]),
            'average_energy_consumption': np.mean([r['energy_consumption'] for r in recent]),
            'current_sustainability_score': recent[-1]['sustainability_score'] if recent else 0,
            'trend': 'improving' if len(recent) > 10 and recent[-1]['sustainability_score'] > recent[0]['sustainability_score'] else 'stable'
        }

# ============================================================================
# Enhanced Data Expert (Main Class)
# ============================================================================

class DataExpert:
    """Enhanced Data Expert v7.0.0 with all green agent capabilities"""
    
    def __init__(self, expert_id: str = "data_engineer_v7", max_workers: int = 4,
                 enable_streaming: bool = True, enable_quality: bool = True,
                 enable_lineage: bool = True, enable_bio_integration: bool = True,
                 enable_federated: bool = True, enable_cross_domain: bool = True,
                 enable_human_ai: bool = True, enable_cross_expert: bool = True,
                 enable_predictive_sustainability: bool = True):
        self.expert_id = expert_id
        self.version = "7.0.0"
        self.max_workers = max_workers
        self.enable_streaming = enable_streaming
        self.enable_quality = enable_quality
        self.enable_lineage = enable_lineage
        self.enable_bio_integration = enable_bio_integration and BIO_INSPIRED_AVAILABLE
        self.enable_federated = enable_federated
        self.enable_cross_domain = enable_cross_domain
        self.enable_human_ai = enable_human_ai
        self.enable_cross_expert = enable_cross_expert
        self.enable_predictive_sustainability = enable_predictive_sustainability
        
        # Bio-inspired components
        self.token_manager = None
        self.gradient_manager = None
        self.scheduler = None
        self.compartment_manager = None
        self.biomass_storage = None
        self.harvester = None
        
        # Enhanced modules
        self.federated_learner = EnhancedFederatedReflexiveDataLearner(expert_id)
        self.quality_forecaster = EnhancedPredictiveQualityForecaster()
        self.cross_domain = EnhancedCrossDomainKnowledgeTransfer()
        self.human_ai_reflector = EnhancedHumanAICollaborativeReflector()
        self.cross_expert_optimizer = CrossExpertOptimizer()
        self.sustainability_analyzer = PredictiveSustainabilityAnalyzer()
        
        # Expert profile
        self.profile = ExpertProfile(
            expert_id=expert_id,
            domain=ExpertDomain.DATA,
            hardware_profile=HardwareProfile.HYBRID,
            helium_per_inference=0.015,
            carbon_per_inference=0.00015,
            energy_per_inference=0.0015,
            avg_latency_ms=20.0,
            accuracy_score=0.99,
            reliability_score=0.99,
            efficiency_score=0.97,
            supported_task_types=['data_processing', 'streaming', 'etl', 'data_quality', 'training']
        )
        
        # Enhanced compression algorithms with sustainability metrics
        self.compression_algorithms = {
            'none': {'ratio': 1.0, 'energy_overhead': 0.0, 'latency_impact_ms': 0, 'ecoatp_cost': 0,
                     'carbon_impact': 0.0, 'helium_impact': 0.0, 'sustainability_score': 0.5},
            'snappy': {'ratio': 0.45, 'energy_overhead': 0.0003, 'latency_impact_ms': 1, 'ecoatp_cost': 1,
                       'carbon_impact': 0.00012, 'helium_impact': 0.002, 'sustainability_score': 0.8},
            'lz4': {'ratio': 0.40, 'energy_overhead': 0.0004, 'latency_impact_ms': 2, 'ecoatp_cost': 2,
                    'carbon_impact': 0.00016, 'helium_impact': 0.003, 'sustainability_score': 0.75},
            'gzip': {'ratio': 0.30, 'energy_overhead': 0.0008, 'latency_impact_ms': 5, 'ecoatp_cost': 3,
                     'carbon_impact': 0.00032, 'helium_impact': 0.005, 'sustainability_score': 0.65},
            'zstd': {'ratio': 0.22, 'energy_overhead': 0.0015, 'latency_impact_ms': 8, 'ecoatp_cost': 5,
                     'carbon_impact': 0.0006, 'helium_impact': 0.008, 'sustainability_score': 0.55},
            'brotli': {'ratio': 0.18, 'energy_overhead': 0.0025, 'latency_impact_ms': 15, 'ecoatp_cost': 8,
                       'carbon_impact': 0.001, 'helium_impact': 0.012, 'sustainability_score': 0.45},
            'lzma': {'ratio': 0.15, 'energy_overhead': 0.003, 'latency_impact_ms': 25, 'ecoatp_cost': 10,
                     'carbon_impact': 0.0012, 'helium_impact': 0.015, 'sustainability_score': 0.35}
        }
        
        self.storage_tiers = {
            DataTier.HOT: {'max_latency_ms': 5, 'compression': 'snappy', 
                          'biomass_tier': StorageTier.ATP_CACHE if BIO_INSPIRED_AVAILABLE else None},
            DataTier.WARM: {'max_latency_ms': 50, 'compression': 'lz4',
                           'biomass_tier': StorageTier.GLYCOGEN_QUEUE if BIO_INSPIRED_AVAILABLE else None},
            DataTier.COLD: {'max_latency_ms': 500, 'compression': 'zstd',
                           'biomass_tier': StorageTier.STARCH_RESERVE if BIO_INSPIRED_AVAILABLE else None},
            DataTier.FROZEN: {'max_latency_ms': 5000, 'compression': 'lzma',
                             'biomass_tier': StorageTier.LIPID_DEPOT if BIO_INSPIRED_AVAILABLE else None}
        }
        
        self.active_streams: Dict[str, Any] = {}
        self.lineage_records: Dict[str, DataLineage] = {}
        self.optimization_history: deque = deque(maxlen=10000)
        self.pipeline_status: Dict[str, PipelineStatus] = {}
        self.quality_cache: Dict[str, DataQualityMetrics] = {}
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        
        self.total_processed_gb = 0.0
        self.total_saved_carbon_kg = 0.0
        self.total_saved_helium = 0.0
        self.total_ecoatp_saved = 0.0
        self.biomass_lineage_tokens: Dict[str, str] = {}
        
        logger.info(f"Data Expert v{self.version} initialized with all green agent features")
    
    def inject_bio_core(self, bio_core=None, **kwargs):
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
    # Bio-Inspired Data Access
    # ========================================================================
    
    def _get_token_efficient_compression(self, latency_budget_ms: float) -> str:
        if self.token_manager:
            summary = self.token_manager.get_system_summary()
            balance = summary.get('total_balance', 500)
            if balance < 100:
                return 'zstd' if latency_budget_ms > 10 else 'lz4'
            elif balance < 300:
                return 'lz4' if latency_budget_ms < 10 else 'gzip'
            else:
                return 'snappy' if latency_budget_ms < 5 else 'lz4'
        return 'lz4'
    
    def _get_gradient_backpressure(self) -> float:
        if self.gradient_manager:
            carbon = self.gradient_manager.fields.get('carbon')
            if carbon:
                return carbon.gradient_strength
        return 0.5
    
    def _get_harvester_quality_confidence(self) -> float:
        if self.harvester:
            stats = self.harvester.get_harvesting_stats()
            recent = stats.get('recent_conversions', [])
            if recent:
                return np.mean([c.get('convertible_energy', 0.5) for c in recent[-10:]])
        return 0.5
    
    def _get_atp_parallelism_level(self) -> int:
        if self.scheduler:
            df = self.scheduler.calculate_gradient_driving_force()
            rs = self.scheduler.calculate_rotation_speed(df)
            rate = self.scheduler.calculate_atp_production_rate(rs)
            return min(8, self.max_workers * 2) if rate > 100 else self.max_workers if rate > 50 else max(1, self.max_workers // 2)
        return self.max_workers
    
    # ========================================================================
    # Primary Optimization
    # ========================================================================
    
    async def optimize_data_pipeline(self, input_size_mb: float, helium_scarcity: float,
                                    latency_budget_ms: float, data_format: str = 'auto',
                                    streaming_mode: Optional[str] = None,
                                    quality_requirements: Optional[Dict[str, float]] = None,
                                    carbon_budget_kg: Optional[float] = None,
                                    enable_parallel: bool = True, tier_preference: Optional[str] = None,
                                    cross_expert_hints: Optional[Dict[str, Any]] = None,
                                    ecoatp_budget: Optional[float] = None) -> Dict[str, Any]:
        start_time = datetime.utcnow()
        optimization_id = hashlib.md5(f"{input_size_mb}{helium_scarcity}{latency_budget_ms}{start_time}".encode()).hexdigest()[:12]
        
        data_profile = await self._profile_data(input_size_mb, data_format, streaming_mode)
        
        quality_metrics = None
        if self.enable_quality:
            quality_metrics = await self._assess_data_quality(input_size_mb, quality_requirements)
            if self.enable_bio_integration:
                quality_metrics.harvester_confidence = self._get_harvester_quality_confidence()
            self.quality_forecaster.update_history(quality_metrics)
            self.sustainability_analyzer.update_sustainability_metrics({
                'carbon_intensity': quality_metrics.sustainability_impact * 400,
                'helium_efficiency': quality_metrics.harvester_confidence * 0.8,
                'energy_consumption': input_size_mb * 0.01,
                'sustainability_score': quality_metrics.sustainability_impact
            })
        
        # Apply cross-domain knowledge
        if self.enable_cross_domain:
            energy_knowledge = await self.cross_domain.apply_energy_knowledge({'size': input_size_mb})
            carbon_knowledge = await self.cross_domain.apply_carbon_knowledge({'quality': quality_metrics})
            helium_knowledge = await self.cross_domain.apply_helium_knowledge({'size': input_size_mb})
            
            if energy_knowledge.get('applied_strategy') != 'default':
                logger.info(f"Applied energy knowledge: {energy_knowledge['applied_strategy']}")
            if helium_knowledge.get('helium_efficient'):
                logger.info(f"Applied helium knowledge: {helium_knowledge['strategies']}")
        
        # Cross-expert optimization
        if self.enable_cross_expert and cross_expert_hints:
            cross_optimization = await self.cross_expert_optimizer.optimize_cross_expert(cross_expert_hints)
            logger.info(f"Cross-expert optimization score: {cross_optimization.score:.2f}")
        
        compression_algo = self._get_token_efficient_compression(latency_budget_ms) if self.enable_bio_integration else 'lz4'
        compression_plan = {
            'algorithm': compression_algo,
            'ratio': self.compression_algorithms[compression_algo]['ratio'],
            'energy_overhead': self.compression_algorithms[compression_algo]['energy_overhead'],
            'latency_impact_ms': self.compression_algorithms[compression_algo]['latency_impact_ms'],
            'ecoatp_cost': self.compression_algorithms[compression_algo].get('ecoatp_cost', 0),
            'carbon_impact': self.compression_algorithms[compression_algo].get('carbon_impact', 0),
            'helium_impact': self.compression_algorithms[compression_algo].get('helium_impact', 0),
            'sustainability_score': self.compression_algorithms[compression_algo].get('sustainability_score', 0.5)
        }
        
        parallel_workers = self._get_atp_parallelism_level() if enable_parallel and self.enable_bio_integration else (self.max_workers if enable_parallel else 1)
        
        stream_backpressure = 0.5 + self._get_gradient_backpressure() * 0.5 if self.enable_bio_integration and streaming_mode else 0.8
        
        ecoatp_cost = input_size_mb * 0.1 + compression_plan['ecoatp_cost'] if self.enable_bio_integration else 0
        if ecoatp_budget and ecoatp_cost > ecoatp_budget and self.enable_bio_integration:
            compression_algo = 'snappy'
            compression_plan = {
                'algorithm': compression_algo,
                'ratio': self.compression_algorithms[compression_algo]['ratio'],
                'ecoatp_cost': self.compression_algorithms[compression_algo].get('ecoatp_cost', 0),
                'carbon_impact': self.compression_algorithms[compression_algo].get('carbon_impact', 0),
                'helium_impact': self.compression_algorithms[compression_algo].get('helium_impact', 0),
                'sustainability_score': self.compression_algorithms[compression_algo].get('sustainability_score', 0.5)
            }
            ecoatp_cost = input_size_mb * 0.1 + compression_plan['ecoatp_cost']
        
        plan = {
            'expert_id': self.expert_id,
            'optimization_id': optimization_id,
            'version': self.version,
            'compression': compression_plan['algorithm'],
            'compression_ratio': compression_plan['ratio'],
            'original_size_mb': input_size_mb,
            'compressed_size_mb': input_size_mb * compression_plan['ratio'],
            'estimated_latency_ms': compression_plan['latency_impact_ms'] + (input_size_mb * 0.01),
            'estimated_energy_kwh': input_size_mb * compression_plan['energy_overhead'],
            'estimated_carbon_kg': input_size_mb * compression_plan['carbon_impact'],
            'estimated_helium_liters': input_size_mb * compression_plan['helium_impact'],
            'estimated_ecoatp_cost': ecoatp_cost,
            'sustainability_score': compression_plan['sustainability_score'],
            'strategy': 'bio_optimized' if self.enable_bio_integration else 'standard',
            'parallel_workers': parallel_workers,
            'stream_backpressure': stream_backpressure,
            'bio_integration_active': self.enable_bio_integration,
            'federated_active': self.enable_federated,
            'cross_domain_active': self.enable_cross_domain,
            'human_ai_active': self.enable_human_ai,
            'cross_expert_active': self.enable_cross_expert,
            'predictive_sustainability_active': self.enable_predictive_sustainability,
            'gradient_backpressure': self._get_gradient_backpressure() if self.enable_bio_integration else 0.5,
            'harvester_confidence': self._get_harvester_quality_confidence() if self.enable_bio_integration else 0.5,
            'quality_assessment': quality_metrics.__dict__ if quality_metrics else None,
            'predictive_forecast': None,
            'predictive_sustainability': None,
            'cross_expert_optimization': None,
            'recommendations': self._generate_recommendations(data_profile, quality_metrics, ecoatp_cost),
            'timestamp': datetime.utcnow().isoformat()
        }
        
        # Generate predictive forecast
        if self.enable_quality:
            forecast = await self.quality_forecaster.predict_quality_trend()
            plan['predictive_forecast'] = {
                'predicted_score': forecast.predicted_score,
                'confidence': forecast.confidence,
                'trend': forecast.trend,
                'factors': forecast.factors,
                'carbon_forecast': forecast.carbon_forecast,
                'helium_forecast': forecast.helium_forecast,
                'recommended_actions': forecast.recommended_actions
            }
            
            if quality_metrics:
                is_anomaly, z_score = self.quality_forecaster.detect_anomaly(quality_metrics)
                plan['anomaly_detected'] = is_anomaly
                plan['anomaly_score'] = z_score
        
        # Generate predictive sustainability
        if self.enable_predictive_sustainability:
            sustainability_forecast = await self.sustainability_analyzer.predict_sustainability_impact({
                'size_mb': input_size_mb,
                'helium_scarcity': helium_scarcity
            })
            plan['predictive_sustainability'] = {
                'predicted_carbon_24h': sustainability_forecast.predicted_carbon_impact_24h,
                'predicted_helium_24h': sustainability_forecast.predicted_helium_consumption_24h,
                'predicted_energy_24h': sustainability_forecast.predicted_energy_consumption_24h,
                'confidence': sustainability_forecast.confidence_level,
                'recommended_actions': sustainability_forecast.recommended_actions,
                'risk_factors': sustainability_forecast.risk_factors
            }
        
        # Store lineage
        if self.enable_lineage:
            lineage = DataLineage(
                lineage_id=f"lineage_{optimization_id}",
                source=data_profile.get('format', 'unknown'),
                quality_at_source=quality_metrics,
                carbon_footprint_kg=plan['estimated_carbon_kg'],
                helium_consumed=plan['estimated_helium_liters'],
                ecoatp_cost=ecoatp_cost,
                federated_round=self.federated_learner.state.round
            )
            lineage.add_transformation('compression', {'algorithm': compression_algo})
            
            if self.enable_cross_domain:
                for transfer in self.cross_domain.transfer_logs:
                    if transfer.get('source') and transfer.get('target'):
                        lineage.add_cross_domain_transfer(transfer['source'], transfer['target'])
            
            if self.enable_cross_expert and cross_expert_hints:
                lineage.add_cross_expert_optimization('combined', cross_expert_hints)
                plan['cross_expert_optimization'] = cross_expert_hints
            
            if self.enable_bio_integration and self.biomass_storage:
                stored, token_id = self.biomass_storage.store_task(
                    task_data={'lineage_id': lineage.lineage_id, 'transformations': lineage.transformations[-5:]},
                    ecoatp_cost=1.0,
                    guarantee=GuaranteeLevel.SILVER,
                    initial_tier=StorageTier.LIPID_DEPOT
                )
                if stored:
                    lineage.biomass_storage_token = token_id
                    self.biomass_lineage_tokens[lineage.lineage_id] = token_id
            
            self.lineage_records[lineage.lineage_id] = lineage
            plan['lineage'] = lineage.__dict__
        
        # Human-AI collaboration
        if self.enable_human_ai and quality_metrics:
            rating = self.human_ai_reflector.get_quality_rating(quality_metrics.overall_score)
            plan['human_ai_rating'] = rating
            
            insights = self.human_ai_reflector.get_collaborative_insights()
            if insights.get('status') != 'insufficient_feedback':
                plan['collaborative_insights'] = insights
        
        self.optimization_history.append({
            'timestamp': start_time,
            'compression': compression_algo,
            'ecoatp_cost': ecoatp_cost,
            'carbon_saved': plan['estimated_carbon_kg'],
            'sustainability_score': plan['sustainability_score'],
            'plan': plan
        })
        self.total_processed_gb += input_size_mb / 1000
        self.total_ecoatp_saved += max(0, 10.0 - ecoatp_cost)
        self.total_saved_carbon_kg += plan['estimated_carbon_kg']
        self.total_saved_helium += plan['estimated_helium_liters']
        
        # Federated learning
        if self.enable_federated and quality_metrics:
            federated_result = await self.federated_learner.participate_in_round(
                [quality_metrics.__dict__],
                performance=quality_metrics.overall_score
            )
            plan['federated_round'] = federated_result.get('round', 0)
            plan['federated_contribution'] = federated_result.get('contribution_score', 0)
        
        return plan
    
    async def _profile_data(self, input_size_mb: float, data_format: str, streaming_mode: Optional[str]) -> Dict[str, Any]:
        profile = {'size_mb': input_size_mb, 'is_streaming': streaming_mode is not None or input_size_mb > 1000,
                   'is_compressible': True, 'estimated_entropy': 0.0, 'recommended_processing': 'batch'}
        if data_format == 'auto':
            data_format = 'json' if input_size_mb < 100 else 'parquet' if input_size_mb > 1000 else 'csv'
        profile['format'] = data_format
        if streaming_mode == 'realtime' or (input_size_mb > 0 and input_size_mb < 10):
            profile['recommended_processing'] = 'realtime'
        elif input_size_mb < 100:
            profile['recommended_processing'] = 'near_realtime'
        elif input_size_mb < 1000:
            profile['recommended_processing'] = 'batch'
        else:
            profile['recommended_processing'] = 'bulk'
        return profile
    
    async def _assess_data_quality(self, input_size_mb: float, requirements: Optional[Dict[str, float]] = None) -> DataQualityMetrics:
        base_quality = 0.90
        size_penalty = min(input_size_mb / 10000, 0.1)
        metrics = DataQualityMetrics(
            completeness=base_quality - size_penalty * 0.3,
            accuracy=base_quality - size_penalty * 0.2,
            consistency=base_quality - size_penalty * 0.1,
            timeliness=base_quality - size_penalty * 0.15,
            uniqueness=base_quality - size_penalty * 0.25,
            validity=base_quality - size_penalty * 0.1
        )
        if self.enable_bio_integration:
            metrics.harvester_confidence = self._get_harvester_quality_confidence()
        return metrics
    
    def _generate_recommendations(self, data_profile: Dict, quality_metrics: Optional[DataQualityMetrics], ecoatp_cost: float) -> List[str]:
        recs = []
        if quality_metrics and quality_metrics.overall_score < 0.7:
            recs.append("Data quality below threshold. Consider cleansing.")
        if ecoatp_cost > 5.0:
            recs.append(f"High Eco-ATP cost ({ecoatp_cost:.1f}). Consider deferring.")
        if data_profile.get('is_streaming'):
            recs.append("Streaming data detected. Backpressure handling active.")
        if self.enable_federated:
            federated_insights = self.federated_learner.get_federated_insights()
            if federated_insights.get('contribution_score', 0) > 10:
                recs.append(f"Federated learning active. Contribution score: {federated_insights['contribution_score']:.1f}")
        if self.enable_cross_domain:
            transfer_stats = self.cross_domain.get_transfer_statistics()
            if transfer_stats.get('total_transfers', 0) > 0:
                recs.append(f"Cross-domain knowledge transferred: {transfer_stats['total_transfers']} transfers")
        if self.enable_cross_expert:
            opt_summary = self.cross_expert_optimizer.get_optimization_summary()
            if opt_summary.get('total_optimizations', 0) > 0:
                recs.append(f"Cross-expert optimizations: {opt_summary['total_optimizations']}")
        if self.enable_predictive_sustainability:
            summary = self.sustainability_analyzer.get_sustainability_summary()
            if summary.get('status') != 'insufficient_data':
                recs.append(f"Sustainability trend: {summary.get('trend', 'stable')}")
        return recs if recs else ["Data configuration is optimal."]
    
    # ========================================================================
    # Causal Analysis with Enhanced Factors
    # ========================================================================
    
    def analyze_causal_factors(self, optimization_result: Dict[str, Any]) -> Dict[str, Any]:
        plan = optimization_result
        compression = plan.get('compression', 'unknown')
        ecoatp_cost = plan.get('estimated_ecoatp_cost', 0)
        gradient_bp = plan.get('gradient_backpressure', 0.5)
        harvester_conf = plan.get('harvester_confidence', 0.5)
        quality = plan.get('quality_assessment', {})
        quality_score = quality.get('overall_score', 0) if quality else 0
        federated_round = plan.get('federated_round', 0)
        cross_domain_active = plan.get('cross_domain_active', False)
        cross_expert_active = plan.get('cross_expert_active', False)
        sustainability_score = plan.get('sustainability_score', 0.5)
        predictive_sustainability = plan.get('predictive_sustainability', {})
        
        causal_chain = []
        
        # EcoATP factors
        if ecoatp_cost > 5:
            causal_chain.append({'factor': 'Token Scarcity', 'impact': 'HIGH',
                                'effect': f'Forced {compression} compression to reduce Eco-ATP cost to {ecoatp_cost:.1f}', 
                                'strength': 0.8})
        elif ecoatp_cost > 2:
            causal_chain.append({'factor': 'Token Budget', 'impact': 'MODERATE',
                                'effect': f'Selected {compression} balancing cost ({ecoatp_cost:.1f}) and performance', 
                                'strength': 0.5})
        
        # Gradient factors
        if gradient_bp > 0.7:
            causal_chain.append({'factor': 'High Carbon Gradient', 'impact': 'HIGH',
                                'effect': f'Backpressure {gradient_bp:.2f} forced conservative processing', 
                                'strength': 0.7})
        
        # Harvester factors
        if harvester_conf < 0.4:
            causal_chain.append({'factor': 'Low Harvester Confidence', 'impact': 'MODERATE',
                                'effect': 'Increased quality validation due to uncertain signals', 
                                'strength': 0.5})
        
        # Quality factors
        if quality_score < 0.7:
            causal_chain.append({'factor': 'Poor Data Quality', 'impact': 'HIGH',
                                'effect': f'Low quality ({quality_score:.2f}) triggered additional validation', 
                                'strength': 0.6})
        
        # Federated learning factors
        if federated_round > 0:
            causal_chain.append({'factor': 'Federated Learning', 'impact': 'MODERATE',
                                'effect': f'Used global model from round {federated_round} to improve quality', 
                                'strength': 0.4})
        
        # Cross-domain factors
        if cross_domain_active:
            transfer_stats = self.cross_domain.get_transfer_statistics()
            if transfer_stats.get('total_transfers', 0) > 0:
                causal_chain.append({'factor': 'Cross-Domain Knowledge', 'impact': 'MODERATE',
                                    'effect': f'Applied knowledge from {len(transfer_stats.get("domain_pairs", {}))} domains', 
                                    'strength': 0.4})
        
        # Cross-expert factors
        if cross_expert_active:
            opt_summary = self.cross_expert_optimizer.get_optimization_summary()
            if opt_summary.get('total_optimizations', 0) > 0:
                causal_chain.append({'factor': 'Cross-Expert Optimization', 'impact': 'HIGH',
                                    'effect': f'Optimized across {len(self.cross_expert_optimizer.expert_hints)} experts', 
                                    'strength': 0.6})
        
        # Sustainability factors
        if sustainability_score < 0.5:
            causal_chain.append({'factor': 'Low Sustainability', 'impact': 'HIGH',
                                'effect': f'Sustainability score {sustainability_score:.2f} requires improvement', 
                                'strength': 0.7})
        
        # Predictive sustainability factors
        if predictive_sustainability:
            predicted_carbon = predictive_sustainability.get('predicted_carbon_24h', 0)
            if predicted_carbon > 100:
                causal_chain.append({'factor': 'Rising Carbon Trend', 'impact': 'MODERATE',
                                    'effect': f'Predicted carbon increase of {predicted_carbon:.1f} in 24h', 
                                    'strength': 0.5})
        
        causal_chain.sort(key=lambda x: (x['impact'] == 'HIGH', x['strength']), reverse=True)
        
        recommendations = []
        for f in causal_chain:
            if f['impact'] == 'HIGH':
                if 'Token' in f['factor']:
                    recommendations.append("Increase token generation or reduce consumption.")
                if 'Carbon' in f['factor']:
                    recommendations.append("Defer processing to lower-carbon windows.")
                if 'Quality' in f['factor']:
                    recommendations.append("Implement data cleansing pipeline.")
                if 'Sustainability' in f['factor']:
                    recommendations.append("Review and improve sustainability practices.")
                if 'Optimization' in f['factor']:
                    recommendations.append("Apply cross-expert recommendations.")
        
        return {
            'decision_type': 'data_pipeline',
            'causal_chain': causal_chain,
            'primary_driver': causal_chain[0] if causal_chain else None,
            'recommendations': recommendations,
            'federated_round': federated_round,
            'sustainability_score': sustainability_score,
            'timestamp': datetime.utcnow().isoformat()
        }
    
    # ========================================================================
    # Enhanced Natural Language Explanations
    # ========================================================================
    
    def explain_compression_choice(self, compression: str, context: Dict[str, Any]) -> Dict[str, Any]:
        algo = self.compression_algorithms.get(compression, {})
        factors = []
        token_balance = context.get('token_balance', 500)
        
        if token_balance < 100:
            factors.append({'name': 'Token Scarcity', 'weight': 0.5,
                          'description': f'Low token balance ({token_balance:.0f}) required aggressive compression'})
        elif token_balance > 500:
            factors.append({'name': 'Token Abundance', 'weight': 0.1,
                          'description': f'High token balance ({token_balance:.0f}) allowed quality-focused choice'})
        
        latency_budget = context.get('latency_budget_ms', 100)
        latency_impact = algo.get('latency_impact_ms', 0)
        if latency_impact > latency_budget * 0.5:
            factors.append({'name': 'Latency Constraint', 'weight': 0.3,
                          'description': f'Compression latency ({latency_impact}ms) within budget ({latency_budget}ms)'})
        
        ratio = algo.get('ratio', 0.5)
        factors.append({'name': 'Compression Efficiency', 'weight': 0.2, 'description': f'Achieves {ratio:.0%} ratio'})
        
        carbon_impact = algo.get('carbon_impact', 0)
        if carbon_impact > 0:
            factors.append({'name': 'Carbon Impact', 'weight': 0.15, 
                          'description': f'Carbon footprint: {carbon_impact:.4f} kg CO2 per MB'})
        
        helium_impact = algo.get('helium_impact', 0)
        if helium_impact > 0:
            factors.append({'name': 'Helium Impact', 'weight': 0.1,
                          'description': f'Helium consumption: {helium_impact:.4f} L per MB'})
        
        sustainability_score = algo.get('sustainability_score', 0.5)
        factors.append({'name': 'Sustainability Score', 'weight': 0.1,
                       'description': f'Sustainability rating: {sustainability_score:.1%}'})
        
        primary = max(factors, key=lambda f: f['weight']) if factors else {'name': 'Default'}
        executive = f"Selected {compression} primarily due to {primary['name'].lower()}. Achieves {ratio:.0%} reduction with {algo.get('latency_impact_ms', 0)}ms latency."
        
        sustainability = f"Carbon: {algo.get('carbon_impact', 0):.4f} kg/MB, Helium: {algo.get('helium_impact', 0):.4f} L/MB, Score: {sustainability_score:.1%}"
        
        alternatives = []
        for name, a in self.compression_algorithms.items():
            if name != compression:
                alternatives.append({
                    'algorithm': name,
                    'ratio': a['ratio'],
                    'latency_ms': a['latency_impact_ms'],
                    'ecoatp_cost': a.get('ecoatp_cost', 0),
                    'carbon_impact': a.get('carbon_impact', 0),
                    'sustainability_score': a.get('sustainability_score', 0.5),
                    'tradeoff': f"{'Better' if a['ratio'] < algo['ratio'] else 'Worse'} compression, "
                               f"{'faster' if a['latency_impact_ms'] < algo['latency_impact_ms'] else 'slower'}"
                })
        
        return {
            'compression': compression,
            'executive_summary': executive,
            'sustainability_summary': sustainability,
            'decision_factors': factors,
            'algorithm_details': {
                'ratio': algo.get('ratio', 0),
                'latency_ms': algo.get('latency_impact_ms', 0),
                'ecoatp_cost': algo.get('ecoatp_cost', 0),
                'carbon_impact': algo.get('carbon_impact', 0),
                'helium_impact': algo.get('helium_impact', 0),
                'sustainability_score': algo.get('sustainability_score', 0.5)
            },
            'alternatives': sorted(alternatives, key=lambda a: a['sustainability_score'], reverse=True)[:3]
        }
    
    def get_data_quality_explanation(self, metrics: DataQualityMetrics) -> Dict[str, Any]:
        score = metrics.overall_score
        if score > 0.9:
            level, assessment = "EXCELLENT", "Data quality is excellent."
        elif score > 0.7:
            level, assessment = "GOOD", "Data quality is good. Minor issues present."
        elif score > 0.5:
            level, assessment = "FAIR", "Data quality is fair. Some issues may affect accuracy."
        elif score > 0.3:
            level, assessment = "POOR", "Data quality is poor. Significant issues detected."
        else:
            level, assessment = "UNUSABLE", "Data quality is unusable. Cleansing required."
        
        dimensions = [
            {'name': 'Completeness', 'score': metrics.completeness, 'weight': 0.25,
             'issue': 'Missing values' if metrics.completeness < 0.8 else None},
            {'name': 'Accuracy', 'score': metrics.accuracy, 'weight': 0.25,
             'issue': 'Inaccurate values' if metrics.accuracy < 0.8 else None},
            {'name': 'Consistency', 'score': metrics.consistency, 'weight': 0.15,
             'issue': 'Inconsistent patterns' if metrics.consistency < 0.8 else None},
            {'name': 'Timeliness', 'score': metrics.timeliness, 'weight': 0.15,
             'issue': 'Stale data' if metrics.timeliness < 0.8 else None},
            {'name': 'Uniqueness', 'score': metrics.uniqueness, 'weight': 0.10,
             'issue': 'Duplicates found' if metrics.uniqueness < 0.9 else None},
            {'name': 'Validity', 'score': metrics.validity, 'weight': 0.10,
             'issue': 'Format violations' if metrics.validity < 0.9 else None}
        ]
        
        worst = sorted(dimensions, key=lambda d: d['score'])[:2]
        recommendations = [f"Address {d['name'].lower()}: {d['issue']}." for d in worst if d['issue']]
        
        if metrics.sustainability_impact < 0.7:
            recommendations.append("Improve sustainability impact through better data quality practices")
        
        return {
            'quality_level': level,
            'overall_score': score,
            'assessment': assessment,
            'dimension_breakdown': dimensions,
            'worst_dimensions': [d['name'] for d in worst],
            'recommendations': recommendations,
            'harvester_confidence': metrics.harvester_confidence,
            'sustainability_impact': metrics.sustainability_impact
        }
    
    # ========================================================================
    # Enhanced Expert Statistics
    # ========================================================================
    
    def get_expert_statistics(self) -> Dict[str, Any]:
        recent = list(self.optimization_history)[-100:]
        stats = {
            'expert_id': self.expert_id,
            'version': self.version,
            'total_processed_gb': self.total_processed_gb,
            'total_saved_carbon_kg': self.total_saved_carbon_kg,
            'total_saved_helium_l': self.total_saved_helium,
            'total_ecoatp_saved': self.total_ecoatp_saved,
            'bio_integration_active': self.enable_bio_integration,
            'federated_active': self.enable_federated,
            'cross_domain_active': self.enable_cross_domain,
            'human_ai_active': self.enable_human_ai,
            'cross_expert_active': self.enable_cross_expert,
            'predictive_sustainability_active': self.enable_predictive_sustainability,
            'lineage_records': len(self.lineage_records),
            'biomass_lineage_tokens': len(self.biomass_lineage_tokens),
            'recent_optimizations': [
                {
                    'timestamp': str(h['timestamp']),
                    'compression': h['compression'],
                    'ecoatp_cost': h.get('ecoatp_cost', 0),
                    'carbon_saved': h.get('carbon_saved', 0),
                    'sustainability_score': h.get('sustainability_score', 0.5)
                } for h in recent[-10:]
            ]
        }
        
        if self.enable_federated:
            stats['federated_insights'] = self.federated_learner.get_federated_insights()
        
        if self.enable_cross_domain:
            stats['cross_domain_stats'] = self.cross_domain.get_transfer_statistics()
        
        if self.enable_human_ai:
            stats['human_ai_insights'] = self.human_ai_reflector.get_collaborative_insights()
        
        if self.enable_cross_expert:
            stats['cross_expert_summary'] = self.cross_expert_optimizer.get_optimization_summary()
        
        if self.enable_predictive_sustainability:
            stats['sustainability_summary'] = self.sustainability_analyzer.get_sustainability_summary()
        
        if self.enable_bio_integration:
            stats['bio_metrics'] = {
                'gradient_backpressure': self._get_gradient_backpressure(),
                'harvester_confidence': self._get_harvester_quality_confidence(),
                'atp_parallelism': self._get_atp_parallelism_level()
            }
        
        return stats
    
    def reset_metrics(self):
        self.optimization_history.clear()
        self.quality_cache.clear()
        self.lineage_records.clear()
        self.active_streams.clear()
        self.pipeline_status.clear()
        self.biomass_lineage_tokens.clear()
        self.total_processed_gb = 0.0
        self.total_ecoatp_saved = 0.0
        self.total_saved_carbon_kg = 0.0
        self.total_saved_helium = 0.0
    
    # ========================================================================
    # Async Shutdown
    # ========================================================================
    
    async def shutdown(self):
        """Graceful shutdown of all components"""
        logger.info(f"Shutting down Data Expert {self.expert_id}")
        await self.federated_learner.close()
        if self.executor:
            self.executor.shutdown(wait=True)
        logger.info("Data Expert shutdown complete")
