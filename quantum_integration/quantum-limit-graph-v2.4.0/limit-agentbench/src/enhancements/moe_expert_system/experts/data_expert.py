# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/moe_expert_system/experts/data_expert.py
"""
Enhanced Data Expert v6.0.0 - Complete Metabolic Data Processor
With Causal Analysis, Natural Language Explanations, Quality Reporting,
Federated Reflexive Learning, Predictive Reflexivity, Cross-Domain Knowledge Transfer,
Human-AI Collaborative Reflection, and Enhanced Sustainability Features
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
from sklearn.ensemble import RandomForestRegressor
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
    sustainability_impact: float = 0.0  # New sustainability metric
    
    def __post_init__(self):
        weights = {'completeness': 0.25, 'accuracy': 0.25, 'consistency': 0.15,
                   'timeliness': 0.15, 'uniqueness': 0.10, 'validity': 0.10}
        self.overall_score = (self.completeness * weights['completeness'] + self.accuracy * weights['accuracy'] +
                             self.consistency * weights['consistency'] + self.timeliness * weights['timeliness'] +
                             self.uniqueness * weights['uniqueness'] + self.validity * weights['validity'])
        # Sustainability impact combines all dimensions
        self.sustainability_impact = self.overall_score * 0.7 + self.harvester_confidence * 0.3

@dataclass
class DataLineage:
    lineage_id: str; source: str
    transformations: List[Dict[str, Any]] = field(default_factory=list)
    quality_at_source: Optional[DataQualityMetrics] = None
    carbon_footprint_kg: float = 0.0; helium_consumed: float = 0.0
    created_at: datetime = field(default_factory=datetime.utcnow); checksum: str = ""
    biomass_storage_token: Optional[str] = None; ecoatp_cost: float = 0.0
    federated_round: int = 0  # New federated learning tracking
    cross_domain_transfers: List[str] = field(default_factory=list)  # New cross-domain tracking
    
    def add_transformation(self, transform_name: str, params: Dict[str, Any]):
        self.transformations.append({'name': transform_name, 'params': params,
                                     'timestamp': datetime.utcnow().isoformat(), 'checksum_before': self.checksum})
    
    def add_cross_domain_transfer(self, source_domain: str, target_domain: str):
        self.cross_domain_transfers.append(f"{source_domain}→{target_domain}")

@dataclass
class FederatedLearningState:
    """State for federated reflexive learning"""
    round: int = 0
    local_model_weights: Dict = field(default_factory=dict)
    global_model_weights: Dict = field(default_factory=dict)
    contribution_score: float = 0.0
    participants: List[str] = field(default_factory=list)
    last_aggregation: Optional[datetime] = None

@dataclass
class PredictiveQualityForecast:
    """Predictive quality forecast"""
    timestamp: datetime = field(default_factory=datetime.utcnow)
    predicted_score: float = 0.0
    confidence: float = 0.0
    trend: str = "stable"
    factors: List[Dict[str, Any]] = field(default_factory=list)

@dataclass
class CrossDomainKnowledge:
    """Cross-domain knowledge transfer structure"""
    source_domain: str
    target_domain: str
    knowledge_type: str  # 'compression', 'quality', 'optimization'
    data: Dict[str, Any]
    effectiveness_score: float = 0.0
    transfer_count: int = 0

# ============================================================================
# Federated Reflexive Learning Module
# ============================================================================

class FederatedReflexiveDataLearner:
    """Federated reflexive learning for distributed data quality optimization"""
    
    def __init__(self, expert_id: str, server_url: Optional[str] = None):
        self.expert_id = expert_id
        self.server_url = server_url
        self.state = FederatedLearningState()
        self._lock = asyncio.Lock()
        self._session = None
        self.local_quality_model = None
        self.global_quality_model = None
        
        # Initialize local model for quality prediction
        self._init_quality_model()
    
    def _init_quality_model(self):
        """Initialize local quality prediction model"""
        class QualityPredictor(nn.Module):
            def __init__(self, input_size=10, hidden_size=64):
                super().__init__()
                self.network = nn.Sequential(
                    nn.Linear(input_size, hidden_size),
                    nn.ReLU(),
                    nn.Linear(hidden_size, hidden_size // 2),
                    nn.ReLU(),
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
        
        # Prepare training data
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
                item.get('size_mb', 100) / 1000,  # Normalize
                item.get('compression_ratio', 0.5),
                item.get('ecoatp_cost', 0) / 10  # Normalize
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
                # Get local model weights
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
                        
                        # Convert weights back to tensor format
                        for k, v in weights.items():
                            self.global_quality_model.state_dict()[k] = torch.FloatTensor(v)
                        
                        return weights
                        
            except Exception as e:
                logger.error(f"Global model fetch error: {e}")
                return None
    
    async def participate_in_round(self, quality_data: List[Dict[str, float]], 
                                  performance: float = 1.0) -> Dict:
        """Full participation in federated learning round"""
        # Step 1: Train local model
        await self.train_local_model(quality_data)
        
        # Step 2: Send local update
        result = await self.send_local_update(performance)
        
        # Step 3: Get global model
        global_weights = await self.get_global_model()
        
        # Step 4: Apply global model
        if global_weights:
            self.state.global_model_weights = global_weights
            self.state.participants.append(self.expert_id)
        
        return {
            'round': self.state.round,
            'participated': bool(global_weights),
            'contribution_score': self.state.contribution_score,
            'performance': performance,
            'timestamp': datetime.utcnow().isoformat()
        }
    
    def get_federated_insights(self) -> Dict:
        """Get insights from federated learning"""
        return {
            'round': self.state.round,
            'contribution_score': self.state.contribution_score,
            'participants': len(self.state.participants),
            'has_global_model': bool(self.state.global_model_weights),
            'last_aggregation': self.state.last_aggregation.isoformat() if self.state.last_aggregation else None
        }
    
    async def close(self):
        if self._session:
            await self._session.close()

# ============================================================================
# Predictive Reflexivity Module
# ============================================================================

class PredictiveQualityForecaster:
    """Predictive reflexivity for data quality forecasting"""
    
    def __init__(self, history_window: int = 100):
        self.history_window = history_window
        self.quality_history = deque(maxlen=history_window)
        self.forecast_history = deque(maxlen=50)
        self.models = {}
        self.scaler = StandardScaler()
        self.is_trained = False
        
        # Quality trend models
        self.trend_model = RandomForestRegressor(n_estimators=100, random_state=42)
        self.anomaly_threshold = 0.3
    
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
            'validity': quality_metrics.validity
        })
    
    async def train_forecast_model(self):
        """Train predictive model on historical quality data"""
        if len(self.quality_history) < 10:
            return {'status': 'insufficient_data', 'samples': len(self.quality_history)}
        
        # Prepare training data
        X = []
        y = []
        history_list = list(self.quality_history)
        
        for i in range(len(history_list) - 5):
            # Use last 5 data points as features
            features = []
            for j in range(5):
                data = history_list[i + j]
                features.extend([
                    data['score'],
                    data['completeness'],
                    data['accuracy'],
                    data['consistency'],
                    data['timeliness']
                ])
            X.append(features)
            y.append(history_list[i + 5]['score'])
        
        X = np.array(X)
        y = np.array(y)
        
        # Scale features
        X_scaled = self.scaler.fit_transform(X)
        
        # Train model
        self.trend_model.fit(X_scaled, y)
        self.is_trained = True
        
        # Calculate training accuracy
        predictions = self.trend_model.predict(X_scaled)
        r2 = r2_score(y, predictions)
        
        logger.info(f"Quality forecast model trained. R²: {r2:.3f}")
        return {'status': 'success', 'r2': r2, 'samples': len(X)}
    
    async def predict_quality_trend(self, hours: int = 24) -> PredictiveQualityForecast:
        """Predict future quality trends"""
        if not self.is_trained or len(self.quality_history) < 10:
            return PredictiveQualityForecast(
                predicted_score=0.5,
                confidence=0.0,
                trend="insufficient_data"
            )
        
        # Get recent data for prediction
        recent = list(self.quality_history)[-5:]
        features = []
        for data in recent:
            features.extend([
                data['score'],
                data['completeness'],
                data['accuracy'],
                data['consistency'],
                data['timeliness']
            ])
        
        features = np.array(features).reshape(1, -1)
        features_scaled = self.scaler.transform(features)
        
        # Make prediction
        prediction = self.trend_model.predict(features_scaled)[0]
        
        # Calculate confidence based on model accuracy
        confidence = min(0.9, self.trend_model.score(
            self.scaler.transform(np.array([features[0]])),
            np.array([prediction])
        ))
        
        # Determine trend
        if len(self.forecast_history) > 5:
            recent_forecasts = list(self.forecast_history)[-5:]
            trend = "increasing" if prediction > recent_forecasts[-1] else "decreasing" if prediction < recent_forecasts[-1] else "stable"
        else:
            trend = "stable"
        
        forecast = PredictiveQualityForecast(
            predicted_score=prediction,
            confidence=confidence,
            trend=trend,
            factors=[
                {'name': 'Recent quality', 'value': recent[-1]['score'], 'weight': 0.6},
                {'name': 'Model confidence', 'value': confidence, 'weight': 0.4}
            ]
        )
        
        self.forecast_history.append(forecast)
        return forecast
    
    def detect_anomaly(self, current_quality: DataQualityMetrics) -> Tuple[bool, float]:
        """Detect quality anomalies"""
        if len(self.quality_history) < 10:
            return False, 0.0
        
        # Calculate expected quality based on recent trend
        recent_scores = [q['score'] for q in list(self.quality_history)[-10:]]
        mean_score = np.mean(recent_scores)
        std_score = np.std(recent_scores)
        
        # Check if current quality is outside 3-sigma range
        z_score = abs(current_quality.overall_score - mean_score) / max(std_score, 0.01)
        is_anomaly = z_score > 3
        
        return is_anomaly, z_score

# ============================================================================
# Cross-Domain Knowledge Transfer Module
# ============================================================================

class CrossDomainKnowledgeTransfer:
    """Cross-domain knowledge transfer between data and other domains"""
    
    def __init__(self):
        self.knowledge_base: Dict[str, Dict[str, CrossDomainKnowledge]] = {}
        self.transfer_logs = deque(maxlen=1000)
        self.domain_mappings = {
            'data→energy': {
                'compression_strategies': ['adaptive', 'greedy', 'bio-inspired'],
                'quality_patterns': ['cyclic', 'event-driven', 'continuous']
            },
            'data→quantum': {
                'encoding_schemes': ['amplitude', 'basis', 'angle'],
                'preprocessing_techniques': ['normalization', 'feature_extraction']
            },
            'data→carbon': {
                'intensity_patterns': ['diurnal', 'seasonal', 'event-based'],
                'optimization_strategies': ['load-shifting', 'efficiency-first']
            },
            'data→helium': {
                'scarcity_patterns': ['supply-constrained', 'price-sensitive'],
                'efficiency_strategies': ['recovery', 'reuse', 'minimization']
            }
        }
        self._lock = asyncio.Lock()
    
    def transfer_knowledge(self, source_domain: str, target_domain: str, 
                          knowledge_type: str, data: Dict[str, Any]) -> CrossDomainKnowledge:
        """Transfer knowledge between domains"""
        key = f"{source_domain}→{target_domain}"
        
        knowledge = CrossDomainKnowledge(
            source_domain=source_domain,
            target_domain=target_domain,
            knowledge_type=knowledge_type,
            data=data,
            transfer_count=1
        )
        
        if key not in self.knowledge_base:
            self.knowledge_base[key] = {}
        
        if knowledge_type not in self.knowledge_base[key]:
            self.knowledge_base[key][knowledge_type] = knowledge
        else:
            existing = self.knowledge_base[key][knowledge_type]
            existing.transfer_count += 1
            existing.data.update(data)
            knowledge = existing
        
        self.transfer_logs.append({
            'timestamp': datetime.utcnow(),
            'source': source_domain,
            'target': target_domain,
            'type': knowledge_type
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
            # Apply energy-optimized compression strategies
            strategies = energy_knowledge.data.get('strategies', [])
            if strategies:
                return {
                    'applied_strategy': strategies[0],
                    'expected_savings': energy_knowledge.effectiveness_score * 0.1,
                    'source': 'energy_domain'
                }
        return {'applied_strategy': 'default', 'source': 'local'}
    
    async def apply_carbon_knowledge(self, quality_data: Dict) -> Dict:
        """Apply knowledge from carbon domain to data quality"""
        carbon_knowledge = self.get_transferred_knowledge('carbon', 'data', 'optimization_strategies')
        if carbon_knowledge:
            return {
                'carbon_aware_processing': True,
                'optimization_impact': carbon_knowledge.effectiveness_score,
                'source': 'carbon_domain'
            }
        return {'carbon_aware_processing': False, 'source': 'local'}
    
    def get_transfer_statistics(self) -> Dict:
        """Get cross-domain transfer statistics"""
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
# Human-AI Collaborative Reflection Module
# ============================================================================

class HumanAICollaborativeReflector:
    """Human-AI collaborative reflection for data quality management"""
    
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
    
    def collect_feedback(self, user_id: str, feedback: Dict) -> Dict:
        """Collect human feedback on data quality"""
        feedback_entry = {
            'user_id': user_id,
            'timestamp': datetime.utcnow(),
            'feedback': feedback
        }
        self.feedback_history.append(feedback_entry)
        
        # Update user preferences
        if 'preference' in feedback:
            self.user_preferences[user_id] = feedback['preference']
        
        # Generate reflection
        reflection = self._generate_reflection(feedback)
        self.reflection_logs.append(reflection)
        
        return reflection
    
    def _generate_reflection(self, feedback: Dict) -> Dict:
        """Generate AI reflection based on feedback"""
        reflection = {
            'timestamp': datetime.utcnow().isoformat(),
            'acknowledgment': f"Feedback received on {feedback.get('topic', 'data quality')}",
            'insights': [],
            'actions': []
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
        
        if 'suggestion' in feedback:
            reflection['actions'].append(f"Implementing suggestion: {feedback['suggestion']}")
            reflection['insights'].append("User suggestion incorporated into improvement plan")
        
        reflection['action_items'] = self._prioritize_actions(reflection['actions'])
        return reflection
    
    def _prioritize_actions(self, actions: List[str]) -> List[Dict]:
        """Prioritize action items based on impact and effort"""
        priorities = []
        for action in actions:
            # Simple heuristic: actions with keywords get higher priority
            if 'critical' in action.lower() or 'immediate' in action.lower():
                priority = 'high'
                impact = 0.9
            elif 'optimize' in action.lower() or 'improve' in action.lower():
                priority = 'medium'
                impact = 0.6
            else:
                priority = 'low'
                impact = 0.3
            
            priorities.append({
                'action': action,
                'priority': priority,
                'impact': impact,
                'estimated_effort': 'medium'
            })
        
        return sorted(priorities, key=lambda x: x['impact'], reverse=True)
    
    def get_collaborative_insights(self) -> Dict:
        """Get collaborative insights from feedback history"""
        if len(self.feedback_history) < 5:
            return {'status': 'insufficient_feedback'}
        
        # Analyze feedback patterns
        recent_feedback = list(self.feedback_history)[-20:]
        topics = {}
        for f in recent_feedback:
            topic = f['feedback'].get('topic', 'general')
            topics[topic] = topics.get(topic, 0) + 1
        
        most_common = max(topics.items(), key=lambda x: x[1]) if topics else ('none', 0)
        
        return {
            'total_feedback': len(self.feedback_history),
            'top_topics': topics,
            'most_common_topic': most_common[0],
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
# Enhanced Data Expert (Main Class)
# ============================================================================

class DataExpert:
    """Enhanced Data Expert v6.0.0 with all green agent capabilities"""
    
    def __init__(self, expert_id: str = "data_engineer_v6", max_workers: int = 4,
                 enable_streaming: bool = True, enable_quality: bool = True,
                 enable_lineage: bool = True, enable_bio_integration: bool = True,
                 enable_federated: bool = True, enable_cross_domain: bool = True,
                 enable_human_ai: bool = True):
        self.expert_id = expert_id
        self.version = "6.0.0"
        self.max_workers = max_workers
        self.enable_streaming = enable_streaming
        self.enable_quality = enable_quality
        self.enable_lineage = enable_lineage
        self.enable_bio_integration = enable_bio_integration and BIO_INSPIRED_AVAILABLE
        self.enable_federated = enable_federated
        self.enable_cross_domain = enable_cross_domain
        self.enable_human_ai = enable_human_ai
        
        # Bio-inspired components
        self.token_manager = None
        self.gradient_manager = None
        self.scheduler = None
        self.compartment_manager = None
        self.biomass_storage = None
        self.harvester = None
        
        # New modules
        self.federated_learner = FederatedReflexiveDataLearner(expert_id)
        self.quality_forecaster = PredictiveQualityForecaster()
        self.cross_domain = CrossDomainKnowledgeTransfer()
        self.human_ai_reflector = HumanAICollaborativeReflector()
        
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
        
        # Compression algorithms with sustainability metrics
        self.compression_algorithms = {
            'none': {'ratio': 1.0, 'energy_overhead': 0.0, 'latency_impact_ms': 0, 'ecoatp_cost': 0,
                     'carbon_impact': 0.0, 'helium_impact': 0.0},
            'snappy': {'ratio': 0.45, 'energy_overhead': 0.0003, 'latency_impact_ms': 1, 'ecoatp_cost': 1,
                       'carbon_impact': 0.00012, 'helium_impact': 0.002},
            'lz4': {'ratio': 0.40, 'energy_overhead': 0.0004, 'latency_impact_ms': 2, 'ecoatp_cost': 2,
                    'carbon_impact': 0.00016, 'helium_impact': 0.003},
            'gzip': {'ratio': 0.30, 'energy_overhead': 0.0008, 'latency_impact_ms': 5, 'ecoatp_cost': 3,
                     'carbon_impact': 0.00032, 'helium_impact': 0.005},
            'zstd': {'ratio': 0.22, 'energy_overhead': 0.0015, 'latency_impact_ms': 8, 'ecoatp_cost': 5,
                     'carbon_impact': 0.0006, 'helium_impact': 0.008},
            'brotli': {'ratio': 0.18, 'energy_overhead': 0.0025, 'latency_impact_ms': 15, 'ecoatp_cost': 8,
                       'carbon_impact': 0.001, 'helium_impact': 0.012},
            'lzma': {'ratio': 0.15, 'energy_overhead': 0.003, 'latency_impact_ms': 25, 'ecoatp_cost': 10,
                     'carbon_impact': 0.0012, 'helium_impact': 0.015}
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
        
        # Apply cross-domain knowledge if available
        if self.enable_cross_domain:
            energy_knowledge = await self.cross_domain.apply_energy_knowledge({'size': input_size_mb})
            carbon_knowledge = await self.cross_domain.apply_carbon_knowledge({'quality': quality_metrics})
            if energy_knowledge.get('applied_strategy') != 'default':
                logger.info(f"Applied energy knowledge: {energy_knowledge['applied_strategy']}")
        
        compression_algo = self._get_token_efficient_compression(latency_budget_ms) if self.enable_bio_integration else 'lz4'
        compression_plan = {
            'algorithm': compression_algo,
            'ratio': self.compression_algorithms[compression_algo]['ratio'],
            'energy_overhead': self.compression_algorithms[compression_algo]['energy_overhead'],
            'latency_impact_ms': self.compression_algorithms[compression_algo]['latency_impact_ms'],
            'ecoatp_cost': self.compression_algorithms[compression_algo].get('ecoatp_cost', 0),
            'carbon_impact': self.compression_algorithms[compression_algo].get('carbon_impact', 0),
            'helium_impact': self.compression_algorithms[compression_algo].get('helium_impact', 0)
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
                'helium_impact': self.compression_algorithms[compression_algo].get('helium_impact', 0)
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
            'strategy': 'bio_optimized' if self.enable_bio_integration else 'standard',
            'parallel_workers': parallel_workers,
            'stream_backpressure': stream_backpressure,
            'bio_integration_active': self.enable_bio_integration,
            'federated_active': self.enable_federated,
            'cross_domain_active': self.enable_cross_domain,
            'human_ai_active': self.enable_human_ai,
            'gradient_backpressure': self._get_gradient_backpressure() if self.enable_bio_integration else 0.5,
            'harvester_confidence': self._get_harvester_quality_confidence() if self.enable_bio_integration else 0.5,
            'quality_assessment': quality_metrics.__dict__ if quality_metrics else None,
            'predictive_forecast': None,
            'recommendations': self._generate_recommendations(data_profile, quality_metrics, ecoatp_cost),
            'timestamp': datetime.utcnow().isoformat()
        }
        
        # Generate predictive forecast if enabled
        if self.enable_quality:
            forecast = await self.quality_forecaster.predict_quality_trend()
            plan['predictive_forecast'] = {
                'predicted_score': forecast.predicted_score,
                'confidence': forecast.confidence,
                'trend': forecast.trend,
                'factors': forecast.factors
            }
            
            # Detect anomalies
            if quality_metrics:
                is_anomaly, z_score = self.quality_forecaster.detect_anomaly(quality_metrics)
                plan['anomaly_detected'] = is_anomaly
                plan['anomaly_score'] = z_score
        
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
                # Add cross-domain transfers to lineage
                for transfer in self.cross_domain.transfer_logs:
                    if transfer.get('source') and transfer.get('target'):
                        lineage.add_cross_domain_transfer(transfer['source'], transfer['target'])
            
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
        
        # Human-AI collaboration feedback
        if self.enable_human_ai and quality_metrics:
            rating = self.human_ai_reflector.get_quality_rating(quality_metrics.overall_score)
            plan['human_ai_rating'] = rating
            
            # Get collaborative insights
            insights = self.human_ai_reflector.get_collaborative_insights()
            if insights.get('status') != 'insufficient_feedback':
                plan['collaborative_insights'] = insights
        
        self.optimization_history.append({
            'timestamp': start_time,
            'compression': compression_algo,
            'ecoatp_cost': ecoatp_cost,
            'carbon_saved': plan['estimated_carbon_kg'],
            'plan': plan
        })
        self.total_processed_gb += input_size_mb / 1000
        self.total_ecoatp_saved += max(0, 10.0 - ecoatp_cost)
        self.total_saved_carbon_kg += plan['estimated_carbon_kg']
        self.total_saved_helium += plan['estimated_helium_liters']
        
        # Participate in federated learning if enabled
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
        return recs if recs else ["Data configuration is optimal."]
    
    # ========================================================================
    # Causal Analysis
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
        
        return {
            'decision_type': 'data_pipeline',
            'causal_chain': causal_chain,
            'primary_driver': causal_chain[0] if causal_chain else None,
            'recommendations': recommendations,
            'federated_round': federated_round,
            'timestamp': datetime.utcnow().isoformat()
        }
    
    # ========================================================================
    # Natural Language Explanations
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
        
        # Add sustainability factors
        carbon_impact = algo.get('carbon_impact', 0)
        if carbon_impact > 0:
            factors.append({'name': 'Carbon Impact', 'weight': 0.15, 
                          'description': f'Carbon footprint: {carbon_impact:.4f} kg CO2 per MB'})
        
        helium_impact = algo.get('helium_impact', 0)
        if helium_impact > 0:
            factors.append({'name': 'Helium Impact', 'weight': 0.1,
                          'description': f'Helium consumption: {helium_impact:.4f} L per MB'})
        
        primary = max(factors, key=lambda f: f['weight']) if factors else {'name': 'Default'}
        executive = f"Selected {compression} primarily due to {primary['name'].lower()}. Achieves {ratio:.0%} reduction with {algo.get('latency_impact_ms', 0)}ms latency."
        
        # Add sustainability summary
        sustainability = f"Carbon: {algo.get('carbon_impact', 0):.4f} kg/MB, Helium: {algo.get('helium_impact', 0):.4f} L/MB"
        
        alternatives = []
        for name, a in self.compression_algorithms.items():
            if name != compression:
                alternatives.append({
                    'algorithm': name,
                    'ratio': a['ratio'],
                    'latency_ms': a['latency_impact_ms'],
                    'ecoatp_cost': a.get('ecoatp_cost', 0),
                    'carbon_impact': a.get('carbon_impact', 0),
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
                'helium_impact': algo.get('helium_impact', 0)
            },
            'alternatives': sorted(alternatives, key=lambda a: a['ecoatp_cost'])[:3]
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
        
        # Add sustainability recommendation
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
    # Expert Statistics
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
            'lineage_records': len(self.lineage_records),
            'biomass_lineage_tokens': len(self.biomass_lineage_tokens),
            'recent_optimizations': [
                {
                    'timestamp': str(h['timestamp']),
                    'compression': h['compression'],
                    'ecoatp_cost': h.get('ecoatp_cost', 0),
                    'carbon_saved': h.get('carbon_saved', 0)
                } for h in recent[-10:]
            ]
        }
        
        if self.enable_federated:
            stats['federated_insights'] = self.federated_learner.get_federated_insights()
        
        if self.enable_cross_domain:
            stats['cross_domain_stats'] = self.cross_domain.get_transfer_statistics()
        
        if self.enable_human_ai:
            stats['human_ai_insights'] = self.human_ai_reflector.get_collaborative_insights()
        
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
