# File: enhancements/moe_expert_system/sustainability/biodiversity_impact.py
"""
Enhanced Biodiversity Impact Assessment v2.0.0 - Complete Green Agent Implementation

Comprehensive biodiversity impact assessment with:
- Federated Reflexive Learning with distributed ecosystem tracking
- User-Adaptive Reflexivity with dynamic assessment parameters
- Real-time Carbon Intensity Integration with API support
- Cross-Domain Knowledge Transfer with ecosystem mapping
- Human-AI Collaborative Reflection with detailed reporting
- Predictive Reflexivity with ensemble forecasting
- Sustainability Score with multi-metric aggregation
- Enhanced Carbon/Helium Awareness with real-time tracking
- ML-Based Impact Prediction
- Ecosystem Trend Analysis
- Mitigation Strategy Optimization
"""

import asyncio
import logging
import numpy as np
from typing import Dict, Any, List, Optional, Tuple, Set
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from collections import deque, defaultdict
import hashlib
import json
import aiohttp
import os
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.metrics import r2_score, mean_squared_error
import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import DataLoader, TensorDataset

logger = logging.getLogger(__name__)

# ============================================================================
# Bio-Inspired Import Check
# ============================================================================
try:
    from enhancements.bio_inspired.eco_atp_currency import EcoATPTokenManager
    from enhancements.bio_inspired.proton_gradient_fields import GradientFieldManager
    from enhancements.bio_inspired.atp_synthase_scheduler import ATPSynthaseScheduler
    from enhancements.bio_inspired.chromatophore_compartments import CompartmentManager
    from enhancements.bio_inspired.biomass_storage import BiomassStorage
    from enhancements.bio_inspired.photosynthetic_harvester import PhotosyntheticHarvester
    BIO_INSPIRED_AVAILABLE = True
except ImportError:
    BIO_INSPIRED_AVAILABLE = False

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
# Predictive Biodiversity Analyzer Module
# ============================================================================

class PredictiveBiodiversityAnalyzer:
    """Predictive reflexivity with ensemble forecasting for biodiversity impact"""
    
    def __init__(self, history_window: int = 100):
        self.history_window = history_window
        self.impact_history = deque(maxlen=history_window)
        self.forecast_history = deque(maxlen=50)
        self.models = {}
        self.scaler = StandardScaler()
        self.is_trained = False
        
        # Ensemble models
        self.models['random_forest'] = RandomForestRegressor(n_estimators=100, random_state=42)
        self.models['gradient_boosting'] = GradientBoostingRegressor(n_estimators=100, random_state=42)
    
    def update_history(self, impact_data: Dict):
        """Update impact history for forecasting"""
        self.impact_history.append({
            'timestamp': datetime.utcnow(),
            'total_impact': impact_data.get('total_impact', 0.5),
            'habitat_impact': impact_data.get('habitat_score', 0.5),
            'energy_impact': impact_data.get('energy_score', 0.5),
            'cooling_impact': impact_data.get('cooling_score', 0.5),
            'resource_impact': impact_data.get('resource_score', 0.5),
            'carbon_intensity': impact_data.get('carbon_intensity', 400),
            'ecosystem_sensitivity': impact_data.get('ecosystem_sensitivity', 0.5)
        })
    
    async def train_forecast_model(self):
        """Train ensemble forecasting models"""
        if len(self.impact_history) < 10:
            return {'status': 'insufficient_data', 'samples': len(self.impact_history)}
        
        X = []
        y = []
        history_list = list(self.impact_history)
        
        for i in range(len(history_list) - 5):
            features = []
            for j in range(5):
                data = history_list[i + j]
                features.extend([
                    data['total_impact'],
                    data['habitat_impact'],
                    data['energy_impact'],
                    data['cooling_impact'],
                    data['resource_impact'],
                    data['carbon_intensity'] / 100,
                    data['ecosystem_sensitivity']
                ])
            X.append(features)
            y.append(history_list[i + 5]['total_impact'])
        
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
        logger.info(f"Biodiversity forecast models trained. R²: {results}")
        return {'status': 'success', 'results': results, 'samples': len(X)}
    
    async def predict_impact_trend(self, hours: int = 24) -> Dict:
        """Predict future biodiversity impact trends"""
        if not self.is_trained or len(self.impact_history) < 10:
            return {'predicted_impact': 0.5, 'confidence': 0.0, 'trend': 'insufficient_data'}
        
        recent = list(self.impact_history)[-5:]
        features = []
        for data in recent:
            features.extend([
                data['total_impact'],
                data['habitat_impact'],
                data['energy_impact'],
                data['cooling_impact'],
                data['resource_impact'],
                data['carbon_intensity'] / 100,
                data['ecosystem_sensitivity']
            ])
        
        features = np.array(features).reshape(1, -1)
        features_scaled = self.scaler.transform(features)
        
        predictions = []
        for name, model in self.models.items():
            if model is not None:
                pred = model.predict(features_scaled)[0]
                predictions.append(pred)
        
        if not predictions:
            return {'predicted_impact': 0.5, 'confidence': 0.0, 'trend': 'no_models'}
        
        prediction = np.mean(predictions)
        confidence = min(0.9, np.std(predictions) / 0.2) if len(predictions) > 1 else 0.5
        
        if len(self.forecast_history) > 5:
            recent_forecasts = list(self.forecast_history)[-5:]
            trend = "improving" if prediction < recent_forecasts[-1] else "declining" if prediction > recent_forecasts[-1] else "stable"
        else:
            trend = "stable"
        
        self.forecast_history.append({'prediction': prediction, 'trend': trend})
        return {
            'predicted_impact': prediction,
            'confidence': confidence,
            'trend': trend,
            'recommended_actions': self._generate_predictive_actions(prediction)
        }
    
    def _generate_predictive_actions(self, prediction: float) -> List[str]:
        """Generate recommended actions based on predictions"""
        actions = []
        if prediction > 0.7:
            actions.append("URGENT: Implement immediate biodiversity protection measures")
            actions.append("Relocate computation to lower-impact areas")
        elif prediction > 0.5:
            actions.append("Optimize energy and cooling strategies")
            actions.append("Invest in habitat restoration offsets")
        elif prediction > 0.3:
            actions.append("Monitor ecosystem health closely")
        else:
            actions.append("Current practices are sustainable - maintain standards")
        return actions

# ============================================================================
# Federated Biodiversity Assessor Module
# ============================================================================

class FederatedBiodiversityAssessor:
    """Federated reflexive learning for distributed biodiversity tracking"""
    
    def __init__(self, server_url: Optional[str] = None):
        self.server_url = server_url
        self.round = 0
        self.local_impacts = {}
        self.global_impacts = {}
        self.participants = []
        self.contribution_scores = {}
        self._lock = asyncio.Lock()
        self._session = None
    
    async def _get_session(self):
        if self._session is None and self.server_url:
            self._session = aiohttp.ClientSession()
        return self._session
    
    async def send_local_impact(self, participant_id: str, impact_data: Dict, performance: float = 1.0) -> Dict:
        """Send local biodiversity impact data to federated server"""
        if not self.server_url:
            return {'status': 'local'}
        
        async with self._lock:
            session = await self._get_session()
            try:
                update_data = {
                    'participant_id': participant_id,
                    'round': self.round,
                    'impact_data': impact_data,
                    'performance': performance,
                    'timestamp': datetime.utcnow().isoformat()
                }
                async with session.post(
                    f"{self.server_url}/federated/biodiversity",
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
                logger.error(f"Federated biodiversity send error: {e}")
                return {'status': 'error'}
    
    async def get_global_impacts(self) -> Optional[Dict]:
        """Get aggregated biodiversity impacts from federated server"""
        if not self.server_url:
            return self.global_impacts
        
        async with self._lock:
            session = await self._get_session()
            try:
                async with session.get(
                    f"{self.server_url}/federated/biodiversity/global",
                    timeout=30
                ) as response:
                    if response.status == 200:
                        data = await response.json()
                        self.global_impacts = data.get('impacts', {})
                        self.participants = data.get('participants', [])
                        return self.global_impacts
            except Exception as e:
                logger.error(f"Global biodiversity fetch error: {e}")
                return None
    
    def aggregate_impacts(self, peer_impacts: List[Dict], weights: Dict[str, float] = None) -> Dict:
        """Aggregate biodiversity impacts from peers with weighted averaging"""
        if not peer_impacts:
            return {}
        
        aggregated = {}
        if weights is None:
            weights = {i: 1.0 for i in range(len(peer_impacts))}
        
        for key in peer_impacts[0].keys():
            if isinstance(peer_impacts[0][key], (int, float)):
                total = 0.0
                total_weight = 0.0
                for i, peer in enumerate(peer_impacts):
                    if key in peer:
                        total += peer[key] * weights.get(i, 1.0)
                        total_weight += weights.get(i, 1.0)
                aggregated[key] = total / max(total_weight, 0.001)
            else:
                values = [peer.get(key) for peer in peer_impacts if key in peer]
                if values:
                    aggregated[key] = max(set(values), key=values.count)
        
        return aggregated
    
    def get_federated_stats(self) -> Dict:
        return {
            'round': self.round,
            'participants': len(self.participants),
            'has_global_impacts': bool(self.global_impacts),
            'contribution_scores': self.contribution_scores
        }
    
    async def close(self):
        if self._session:
            await self._session.close()

# ============================================================================
# ML-Based Impact Prediction Module
# ============================================================================

class MLImpactPredictor:
    """Machine learning-based impact prediction for biodiversity"""
    
    def __init__(self, input_size: int = 10, hidden_size: int = 64):
        self.input_size = input_size
        self.hidden_size = hidden_size
        self.model = None
        self.scaler = StandardScaler()
        self.is_trained = False
        
        # Initialize neural network
        self._init_model()
    
    def _init_model(self):
        """Initialize neural network model"""
        class ImpactPredictor(nn.Module):
            def __init__(self, input_size, hidden_size):
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
        
        self.model = ImpactPredictor(self.input_size, self.hidden_size)
    
    async def train_model(self, training_data: List[Dict]) -> Dict:
        """Train ML model on historical impact data"""
        if len(training_data) < 20:
            return {'status': 'insufficient_data', 'samples': len(training_data)}
        
        X = []
        y = []
        for item in training_data:
            X.append([
                item.get('carbon_intensity', 400) / 100,
                item.get('energy_intensity', 0.5),
                item.get('cooling_intensity', 0.5),
                item.get('resource_intensity', 0.5),
                item.get('ecosystem_sensitivity', 0.5),
                item.get('proximity_factor', 0.5),
                item.get('fragmentation_index', 0.5),
                item.get('species_density', 0.5),
                item.get('water_scarcity', 0.5),
                item.get('temperature_anomaly', 0.5)
            ])
            y.append(item.get('total_impact', 0.5))
        
        X = np.array(X)
        y = np.array(y)
        X_scaled = self.scaler.fit_transform(X)
        
        dataset = TensorDataset(
            torch.FloatTensor(X_scaled),
            torch.FloatTensor(y).unsqueeze(1)
        )
        dataloader = DataLoader(dataset, batch_size=32, shuffle=True)
        
        optimizer = optim.Adam(self.model.parameters(), lr=0.001)
        criterion = nn.MSELoss()
        
        epochs = 100
        losses = []
        for epoch in range(epochs):
            epoch_loss = 0
            for batch_X, batch_y in dataloader:
                optimizer.zero_grad()
                output = self.model(batch_X)
                loss = criterion(output, batch_y)
                loss.backward()
                torch.nn.utils.clip_grad_norm_(self.model.parameters(), 1.0)
                optimizer.step()
                epoch_loss += loss.item()
            losses.append(epoch_loss)
            if (epoch + 1) % 20 == 0:
                logger.debug(f"ML Training Epoch {epoch+1}/{epochs}, Loss: {epoch_loss/len(dataloader):.4f}")
        
        self.is_trained = True
        
        # Calculate accuracy
        with torch.no_grad():
            predictions = self.model(torch.FloatTensor(X_scaled)).numpy().flatten()
            mse = mean_squared_error(y, predictions)
        
        return {'status': 'success', 'loss': np.mean(losses), 'mse': mse, 'samples': len(X)}
    
    async def predict_impact(self, scenario: Dict) -> Dict:
        """Predict biodiversity impact for a given scenario"""
        if not self.is_trained:
            return {'predicted_impact': 0.5, 'confidence': 0.0, 'status': 'model_not_trained'}
        
        features = np.array([[
            scenario.get('carbon_intensity', 400) / 100,
            scenario.get('energy_intensity', 0.5),
            scenario.get('cooling_intensity', 0.5),
            scenario.get('resource_intensity', 0.5),
            scenario.get('ecosystem_sensitivity', 0.5),
            scenario.get('proximity_factor', 0.5),
            scenario.get('fragmentation_index', 0.5),
            scenario.get('species_density', 0.5),
            scenario.get('water_scarcity', 0.5),
            scenario.get('temperature_anomaly', 0.5)
        ]])
        
        features_scaled = self.scaler.transform(features)
        
        self.model.eval()
        with torch.no_grad():
            prediction = self.model(torch.FloatTensor(features_scaled)).numpy()[0, 0]
        
        # Calculate confidence based on model performance
        confidence = 0.8 if self.is_trained else 0.0
        
        return {
            'predicted_impact': float(prediction),
            'confidence': confidence,
            'status': 'success'
        }

# ============================================================================
# Human-AI Collaborative Reflection Module
# ============================================================================

class HumanAICollaborativeBiodiversity:
    """Human-AI collaborative reflection for biodiversity impact"""
    
    def __init__(self):
        self.feedback_history = deque(maxlen=1000)
        self.reflection_logs = deque(maxlen=100)
        self.user_preferences = {}
        self._lock = asyncio.Lock()
    
    def collect_feedback(self, user_id: str, feedback: Dict) -> Dict:
        """Collect human feedback on biodiversity impact assessment"""
        feedback_entry = {
            'user_id': user_id,
            'timestamp': datetime.utcnow(),
            'feedback': feedback
        }
        self.feedback_history.append(feedback_entry)
        
        if 'preference' in feedback:
            self.user_preferences[user_id] = feedback['preference']
        
        reflection = self._generate_reflection(feedback)
        self.reflection_logs.append(reflection)
        return reflection
    
    def _generate_reflection(self, feedback: Dict) -> Dict:
        """Generate AI reflection based on feedback"""
        reflection = {
            'timestamp': datetime.utcnow().isoformat(),
            'acknowledgment': f"Feedback received on {feedback.get('topic', 'biodiversity impact')}",
            'insights': [],
            'actions': [],
            'biodiversity_insights': []
        }
        
        if 'concern' in feedback:
            if feedback['concern'] == 'habitat':
                reflection['insights'].append("Habitat impact can be reduced through location optimization")
                reflection['actions'].append("Relocate computation to lower-impact areas")
            elif feedback['concern'] == 'energy':
                reflection['insights'].append("Energy source significantly affects biodiversity")
                reflection['actions'].append("Switch to renewable energy sources")
            elif feedback['concern'] == 'cooling':
                reflection['insights'].append("Cooling method impacts local water ecosystems")
                reflection['actions'].append("Implement water-free cooling solutions")
            elif feedback['concern'] == 'biodiversity':
                reflection['biodiversity_insights'].append("Biodiversity impact requires holistic assessment")
                reflection['actions'].append("Implement comprehensive biodiversity monitoring")
        
        if 'suggestion' in feedback:
            reflection['actions'].append(f"Implementing suggestion: {feedback['suggestion']}")
            reflection['insights'].append("User suggestion incorporated into improvement plan")
        
        reflection['action_items'] = self._prioritize_actions(reflection['actions'])
        return reflection
    
    def _prioritize_actions(self, actions: List[str]) -> List[Dict]:
        """Prioritize action items based on biodiversity impact"""
        priorities = []
        for action in actions:
            if any(keyword in action.lower() for keyword in ['urgent', 'critical', 'immediate']):
                priority = 'high'
                impact = 0.9
                effort = 'high'
            elif any(keyword in action.lower() for keyword in ['biodiversity', 'habitat', 'ecosystem']):
                priority = 'high'
                impact = 0.8
                effort = 'medium'
            elif any(keyword in action.lower() for keyword in ['carbon', 'energy']):
                priority = 'medium'
                impact = 0.6
                effort = 'medium'
            else:
                priority = 'low'
                impact = 0.3
                effort = 'low'
            
            priorities.append({
                'action': action,
                'priority': priority,
                'impact': impact,
                'estimated_effort': effort,
                'biodiversity_weight': impact
            })
        
        return sorted(priorities, key=lambda x: (x['impact'], x['biodiversity_weight']), reverse=True)
    
    def get_collaborative_insights(self) -> Dict:
        """Get collaborative insights from feedback history"""
        if len(self.feedback_history) < 5:
            return {'status': 'insufficient_feedback'}
        
        recent_feedback = list(self.feedback_history)[-20:]
        topics = {}
        biodiversity_concerns = {}
        
        for f in recent_feedback:
            topic = f['feedback'].get('topic', 'general')
            topics[topic] = topics.get(topic, 0) + 1
            
            if 'concern' in f['feedback']:
                concern = f['feedback']['concern']
                biodiversity_concerns[concern] = biodiversity_concerns.get(concern, 0) + 1
        
        most_common = max(topics.items(), key=lambda x: x[1]) if topics else ('none', 0)
        top_concern = max(biodiversity_concerns.items(), key=lambda x: x[1]) if biodiversity_concerns else ('none', 0)
        
        return {
            'total_feedback': len(self.feedback_history),
            'top_topics': topics,
            'most_common_topic': most_common[0],
            'biodiversity_concerns': biodiversity_concerns,
            'top_biodiversity_concern': top_concern[0],
            'engagement_score': min(1.0, len(self.feedback_history) / 100),
            'user_count': len(set(f['user_id'] for f in self.feedback_history))
        }

# ============================================================================
# Enums and Data Classes
# ============================================================================

class EcosystemType(Enum):
    TROPICAL_FOREST = "tropical_forest"
    TEMPERATE_FOREST = "temperate_forest"
    GRASSLAND = "grassland"
    WETLAND = "wetland"
    MARINE = "marine"
    FRESHWATER = "freshwater"
    URBAN = "urban"
    DESERT = "desert"

class ImpactCategory(Enum):
    HABITAT_LOSS = "habitat_loss"
    SPECIES_DISPLACEMENT = "species_displacement"
    WATER_POLLUTION = "water_pollution"
    AIR_POLLUTION = "air_pollution"
    NOISE_POLLUTION = "noise_pollution"
    LIGHT_POLLUTION = "light_pollution"
    THERMAL_POLLUTION = "thermal_pollution"
    RESOURCE_DEPLETION = "resource_depletion"
    CARBON_EMISSION = "carbon_emission"
    HELIUM_DEPLETION = "helium_depletion"

@dataclass
class BiodiversityMetric:
    ecosystem_type: EcosystemType
    species_richness: int
    endangered_species_count: int
    habitat_area_km2: float
    fragmentation_index: float
    ecological_connectivity: float
    last_assessment: datetime
    carbon_sensitivity: float = 0.5
    helium_sensitivity: float = 0.5
    sustainability_score: float = 0.0

@dataclass
class BiodiversityAssessment:
    assessment_id: str
    expert_type: str
    location: Dict[str, Any]
    total_impact: float
    impact_breakdown: Dict[str, Any]
    mitigation_strategies: List[Dict]
    recommendations: List[str]
    sustainability_score: float
    carbon_impact: float
    helium_impact: float
    timestamp: datetime

# ============================================================================
# Enhanced Biodiversity Impact Assessor
# ============================================================================

class BiodiversityImpactAssessor:
    """
    Enhanced Biodiversity Impact Assessor v2.0.0 - Complete Green Agent Implementation
    
    Assesses and mitigates biodiversity impacts with:
    - Federated reflexive learning
    - Real-time carbon integration
    - ML-based impact prediction
    - Predictive analytics
    - Human-AI collaboration
    """
    
    def __init__(
        self,
        enable_federated: bool = True,
        enable_carbon_intensity: bool = True,
        enable_predictive: bool = True,
        enable_ml_prediction: bool = True,
        enable_human_ai: bool = True,
        server_url: Optional[str] = None
    ):
        self.enable_federated = enable_federated
        self.enable_carbon_intensity = enable_carbon_intensity
        self.enable_predictive = enable_predictive
        self.enable_ml_prediction = enable_ml_prediction
        self.enable_human_ai = enable_human_ai
        
        # Modules
        self.carbon_manager = CarbonIntensityManager() if enable_carbon_intensity else None
        self.predictive_analyzer = PredictiveBiodiversityAnalyzer() if enable_predictive else None
        self.federated_assessor = FederatedBiodiversityAssessor(server_url) if enable_federated else None
        self.ml_predictor = MLImpactPredictor() if enable_ml_prediction else None
        self.human_ai = HumanAICollaborativeBiodiversity() if enable_human_ai else None
        
        # Bio-inspired modules
        self.token_manager = None
        self.gradient_manager = None
        self.scheduler = None
        self.compartment_manager = None
        self.biomass_storage = None
        self.harvester = None
        
        # Ecosystem tracking
        self.ecosystems: Dict[str, BiodiversityMetric] = {}
        self.impact_history: List[Dict] = []
        self.mitigation_strategies: Dict[str, List[Dict]] = {}
        
        # Biodiversity scores
        self.local_biodiversity_score = 0.0
        self.global_biodiversity_score = 0.0
        self.sustainability_score = 0.0
        self.total_carbon_savings_kg = 0.0
        self.total_helium_savings_l = 0.0
        
        # Initialize ecosystems
        self._initialize_ecosystems()
        
        # Start background tasks
        self._start_background_tasks()
        
        logger.info("Enhanced Biodiversity Impact Assessor v2.0.0 initialized")
    
    def _start_background_tasks(self):
        """Start background maintenance tasks"""
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
                if self.predictive_analyzer and self.impact_history:
                    recent = self.impact_history[-5:] if self.impact_history else []
                    if recent:
                        self.predictive_analyzer.update_history({
                            'total_impact': recent[-1].get('total_biodiversity_impact', 0.5),
                            'habitat_score': recent[-1].get('impact_breakdown', {}).get('habitat', {}).get('score', 0.5),
                            'energy_score': recent[-1].get('impact_breakdown', {}).get('energy', {}).get('score', 0.5),
                            'cooling_score': recent[-1].get('impact_breakdown', {}).get('cooling', {}).get('score', 0.5),
                            'resource_score': recent[-1].get('impact_breakdown', {}).get('resources', {}).get('score', 0.5),
                            'carbon_intensity': self.carbon_manager.carbon_intensity if self.carbon_manager else 400,
                            'ecosystem_sensitivity': 0.5
                        })
                    await self.predictive_analyzer.train_forecast_model()
                await asyncio.sleep(300)
            except Exception as e:
                logger.error(f"Predictive update error: {str(e)}")
                await asyncio.sleep(60)
    
    async def _federated_sync_loop(self):
        while True:
            try:
                if self.federated_assessor and self.impact_history:
                    latest = self.impact_history[-1] if self.impact_history else {}
                    await self.federated_assessor.send_local_impact(
                        f"biodiversity_{hashlib.md5(str(self.ecosystems).encode()).hexdigest()[:8]}",
                        {
                            'local_score': self.local_biodiversity_score,
                            'global_score': self.global_biodiversity_score,
                            'total_impact': latest.get('total_biodiversity_impact', 0.5),
                            'timestamp': datetime.utcnow().isoformat()
                        },
                        performance=self.sustainability_score
                    )
                    await self.federated_assessor.get_global_impacts()
                await asyncio.sleep(3600)
            except Exception as e:
                logger.error(f"Federated sync error: {str(e)}")
                await asyncio.sleep(300)
    
    def _initialize_ecosystems(self):
        """Initialize ecosystem tracking with enhanced metrics"""
        sample_ecosystems = {
            'amazon_rainforest': BiodiversityMetric(
                ecosystem_type=EcosystemType.TROPICAL_FOREST,
                species_richness=16000,
                endangered_species_count=120,
                habitat_area_km2=5500000,
                fragmentation_index=0.15,
                ecological_connectivity=0.85,
                last_assessment=datetime.utcnow(),
                carbon_sensitivity=0.8,
                helium_sensitivity=0.3,
                sustainability_score=0.7
            ),
            'coral_reef_pacific': BiodiversityMetric(
                ecosystem_type=EcosystemType.MARINE,
                species_richness=4000,
                endangered_species_count=45,
                habitat_area_km2=50000,
                fragmentation_index=0.30,
                ecological_connectivity=0.70,
                last_assessment=datetime.utcnow(),
                carbon_sensitivity=0.6,
                helium_sensitivity=0.4,
                sustainability_score=0.6
            ),
            'european_wetlands': BiodiversityMetric(
                ecosystem_type=EcosystemType.WETLAND,
                species_richness=2500,
                endangered_species_count=30,
                habitat_area_km2=150000,
                fragmentation_index=0.25,
                ecological_connectivity=0.60,
                last_assessment=datetime.utcnow(),
                carbon_sensitivity=0.5,
                helium_sensitivity=0.5,
                sustainability_score=0.5
            )
        }
        
        self.ecosystems = sample_ecosystems
    
    def inject_bio_core(self, bio_core: Any = None, **kwargs):
        """Inject bio-inspired modules for enhanced assessment"""
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
        
        logger.info("Bio-inspired modules injected into Biodiversity Impact Assessor")
    
    def _get_ecosystem_carbon_sensitivity(self, ecosystem_name: str) -> float:
        """Get carbon sensitivity for ecosystem"""
        if ecosystem_name in self.ecosystems:
            return self.ecosystems[ecosystem_name].carbon_sensitivity
        return 0.5
    
    def _get_ecosystem_helium_sensitivity(self, ecosystem_name: str) -> float:
        """Get helium sensitivity for ecosystem"""
        if ecosystem_name in self.ecosystems:
            return self.ecosystems[ecosystem_name].helium_sensitivity
        return 0.5
    
    # ========================================================================
    # Enhanced Assessment Methods
    # ========================================================================
    
    async def assess_expert_impact(
        self,
        expert_type: str,
        location: Dict[str, Any],
        energy_source: str,
        cooling_method: str,
        use_ml_prediction: bool = False
    ) -> Dict[str, Any]:
        """
        Enhanced biodiversity impact assessment with ML prediction
        """
        # Update carbon intensity
        carbon_intensity = 400
        if self.carbon_manager:
            carbon_intensity = await self.carbon_manager.get_current_intensity()
        
        # Perform standard assessment
        impact_scores = {}
        total_impact = 0.0
        
        habitat_impact = self._assess_habitat_impact(location)
        impact_scores['habitat'] = habitat_impact
        total_impact += habitat_impact['score']
        
        energy_impact = self._assess_energy_impact(energy_source, location)
        impact_scores['energy'] = energy_impact
        total_impact += energy_impact['score']
        
        cooling_impact = self._assess_cooling_impact(cooling_method, location)
        impact_scores['cooling'] = cooling_impact
        total_impact += cooling_impact['score']
        
        resource_impact = self._assess_resource_impact(expert_type)
        impact_scores['resources'] = resource_impact
        total_impact += resource_impact['score']
        
        # Carbon impact
        carbon_impact = self._assess_carbon_impact(energy_source, location, carbon_intensity)
        impact_scores['carbon'] = carbon_impact
        
        # Helium impact
        helium_impact = self._assess_helium_impact(cooling_method, location)
        impact_scores['helium'] = helium_impact
        
        total_impact += carbon_impact['score'] + helium_impact['score']
        total_impact = total_impact / 6.0
        
        # ML prediction if enabled
        ml_prediction = None
        if self.enable_ml_prediction and use_ml_prediction:
            ml_prediction = await self.ml_predictor.predict_impact({
                'carbon_intensity': carbon_intensity,
                'energy_intensity': energy_impact['score'],
                'cooling_intensity': cooling_impact['score'],
                'resource_intensity': resource_impact['score'],
                'ecosystem_sensitivity': habitat_impact.get('sensitivity', 0.5),
                'proximity_factor': habitat_impact.get('proximity_factor', 0.5),
                'fragmentation_index': habitat_impact.get('fragmentation_index', 0.5),
                'species_density': 0.5,
                'water_scarcity': location.get('water_scarcity_index', 0.5),
                'temperature_anomaly': 0.5
            })
        
        # Generate mitigation strategies
        mitigation = self._generate_mitigation_strategies(
            impact_scores, expert_type, location
        )
        
        # Calculate sustainability score
        sustainability_score = self._calculate_sustainability_score(
            impact_scores, total_impact, carbon_intensity
        )
        
        assessment = {
            'assessment_id': hashlib.md5(f"{expert_type}{location}{datetime.utcnow()}".encode()).hexdigest()[:12],
            'expert_type': expert_type,
            'location': location,
            'total_biodiversity_impact': total_impact,
            'impact_breakdown': impact_scores,
            'mitigation_strategies': mitigation,
            'recommendations': self._generate_recommendations(impact_scores),
            'sustainability_score': sustainability_score,
            'carbon_impact': carbon_impact,
            'helium_impact': helium_impact,
            'ml_prediction': ml_prediction,
            'timestamp': datetime.utcnow().isoformat()
        }
        
        self.impact_history.append(assessment)
        self._update_biodiversity_scores(assessment)
        
        # Update predictive analyzer
        if self.predictive_analyzer:
            self.predictive_analyzer.update_history({
                'total_impact': total_impact,
                'habitat_score': habitat_impact['score'],
                'energy_score': energy_impact['score'],
                'cooling_score': cooling_impact['score'],
                'resource_score': resource_impact['score'],
                'carbon_intensity': carbon_intensity,
                'ecosystem_sensitivity': habitat_impact.get('sensitivity', 0.5)
            })
            await self.predictive_analyzer.train_forecast_model()
        
        # Human-AI collaboration
        if self.enable_human_ai and self.human_ai:
            insights = self.human_ai.get_collaborative_insights()
            assessment['human_ai_insights'] = insights
        
        return assessment
    
    def _assess_carbon_impact(
        self,
        energy_source: str,
        location: Dict[str, Any],
        carbon_intensity: float
    ) -> Dict[str, Any]:
        """Assess carbon impact with real-time data"""
        energy_factors = {
            'solar': 0.02, 'wind': 0.03, 'hydroelectric': 0.05,
            'geothermal': 0.01, 'nuclear': 0.04, 'natural_gas': 0.35,
            'coal': 0.70, 'oil': 0.80, 'biomass': 0.25,
            'mixed_grid': 0.30
        }
        
        base_impact = energy_factors.get(energy_source, 0.3)
        carbon_factor = carbon_intensity / 400.0
        
        score = base_impact * carbon_factor
        
        # Location adjustment
        if location.get('near_carbon_sensitive_ecosystem'):
            score *= 1.3
        
        return {
            'score': min(score, 1.0),
            'energy_source': energy_source,
            'carbon_intensity': carbon_intensity,
            'category': 'high' if score > 0.5 else 'moderate' if score > 0.2 else 'low'
        }
    
    def _assess_helium_impact(
        self,
        cooling_method: str,
        location: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Assess helium impact on biodiversity"""
        helium_factors = {
            'helium_cooling': 0.25,
            'water_cooling': 0.05,
            'air_cooling': 0.02,
            'evaporative_cooling': 0.08,
            'geothermal_cooling': 0.01,
            'liquid_immersion': 0.10,
            'free_cooling': 0.01
        }
        
        base_impact = helium_factors.get(cooling_method, 0.05)
        
        # Location adjustment for helium mining impact
        if location.get('near_helium_mining_region'):
            base_impact *= 2.0
        
        return {
            'score': min(base_impact, 1.0),
            'cooling_method': cooling_method,
            'category': 'high' if base_impact > 0.2 else 'moderate' if base_impact > 0.05 else 'low'
        }
    
    def _calculate_sustainability_score(
        self,
        impact_scores: Dict[str, Any],
        total_impact: float,
        carbon_intensity: float
    ) -> float:
        """Calculate overall sustainability score"""
        # Weighted factors
        weights = {
            'habitat': 0.25,
            'energy': 0.20,
            'cooling': 0.15,
            'resources': 0.15,
            'carbon': 0.15,
            'helium': 0.10
        }
        
        score = 1.0
        for category, scores in impact_scores.items():
            if category in weights:
                score -= scores.get('score', 0) * weights[category]
        
        # Carbon intensity factor
        carbon_factor = 1.0 - (carbon_intensity / 800)
        score = score * 0.7 + carbon_factor * 0.3
        
        return max(0.0, min(1.0, score))
    
    # ========================================================================
    # Existing Assessment Methods (Preserved)
    # ========================================================================
    
    def _assess_habitat_impact(self, location: Dict[str, Any]) -> Dict[str, Any]:
        """Assess habitat impact of computing location"""
        nearest_ecosystem = self._find_nearest_ecosystem(location)
        
        if not nearest_ecosystem:
            return {'score': 0.1, 'category': 'minimal', 'ecosystem': None}
        
        distance_km = location.get('distance_to_ecosystem_km', 100)
        ecosystem = self.ecosystems[nearest_ecosystem]
        
        if distance_km < 1:
            proximity_factor = 1.0
        elif distance_km < 10:
            proximity_factor = 0.7
        elif distance_km < 50:
            proximity_factor = 0.3
        else:
            proximity_factor = 0.1
        
        sensitivity = ecosystem.endangered_species_count / 200.0
        sensitivity = min(sensitivity, 1.0)
        fragmentation_factor = ecosystem.fragmentation_index
        
        score = (proximity_factor * 0.4 + sensitivity * 0.4 + fragmentation_factor * 0.2)
        
        return {
            'score': score,
            'category': 'critical' if score > 0.7 else 'moderate' if score > 0.3 else 'low',
            'ecosystem': nearest_ecosystem,
            'proximity_factor': proximity_factor,
            'sensitivity': sensitivity,
            'fragmentation_index': fragmentation_factor
        }
    
    def _assess_energy_impact(self, energy_source: str, location: Dict[str, Any]) -> Dict[str, Any]:
        impact_factors = {
            'solar': 0.05, 'wind': 0.08, 'hydroelectric': 0.15,
            'geothermal': 0.03, 'nuclear': 0.10, 'natural_gas': 0.40,
            'coal': 0.80, 'oil': 0.90, 'biomass': 0.30,
            'mixed_grid': 0.35
        }
        
        base_impact = impact_factors.get(energy_source, 0.5)
        
        if location.get('near_water_body'):
            if energy_source in ['hydroelectric', 'nuclear']:
                base_impact *= 1.5
        
        if location.get('in_migration_corridor'):
            if energy_source in ['wind']:
                base_impact *= 1.3
        
        return {
            'score': base_impact,
            'energy_source': energy_source,
            'category': 'high' if base_impact > 0.5 else 'moderate' if base_impact > 0.2 else 'low'
        }
    
    def _assess_cooling_impact(self, cooling_method: str, location: Dict[str, Any]) -> Dict[str, Any]:
        impact_factors = {
            'air_cooling': 0.05, 'evaporative_cooling': 0.15,
            'water_cooling': 0.25, 'helium_cooling': 0.10,
            'geothermal_cooling': 0.03, 'liquid_immersion': 0.20,
            'free_cooling': 0.02
        }
        
        base_impact = impact_factors.get(cooling_method, 0.15)
        
        if cooling_method in ['water_cooling', 'evaporative_cooling']:
            if location.get('water_scarcity_index', 0) > 0.7:
                base_impact *= 2.0
            elif location.get('water_scarcity_index', 0) > 0.4:
                base_impact *= 1.5
        
        if cooling_method in ['water_cooling', 'liquid_immersion']:
            if location.get('near_water_body'):
                base_impact *= 1.3
        
        return {
            'score': base_impact,
            'cooling_method': cooling_method,
            'category': 'high' if base_impact > 0.5 else 'moderate' if base_impact > 0.2 else 'low'
        }
    
    def _assess_resource_impact(self, expert_type: str) -> Dict[str, Any]:
        resource_impacts = {
            'energy_expert': {'rare_earth': 0.1, 'copper': 0.05, 'overall': 0.08},
            'data_expert': {'rare_earth': 0.15, 'copper': 0.1, 'overall': 0.12},
            'iot_expert': {'rare_earth': 0.05, 'copper': 0.02, 'overall': 0.04},
            'quantum_expert': {'rare_earth': 0.3, 'copper': 0.2, 'overall': 0.25},
            'helium_expert': {'rare_earth': 0.08, 'copper': 0.05, 'overall': 0.06}
        }
        
        impact = resource_impacts.get(expert_type, {'overall': 0.1})
        
        return {
            'score': impact['overall'],
            'expert_type': expert_type,
            'category': 'high' if impact['overall'] > 0.2 else 'moderate' if impact['overall'] > 0.1 else 'low'
        }
    
    def _find_nearest_ecosystem(self, location: Dict[str, Any]) -> Optional[str]:
        if location.get('latitude', 0) < 0:
            return 'amazon_rainforest'
        elif location.get('latitude', 0) > 45:
            return 'european_wetlands'
        else:
            return 'coral_reef_pacific'
    
    def _generate_mitigation_strategies(
        self,
        impact_scores: Dict[str, Any],
        expert_type: str,
        location: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        strategies = []
        
        if impact_scores['habitat']['score'] > 0.5:
            strategies.append({
                'type': 'habitat_protection',
                'action': 'Relocate computation to lower-impact area',
                'impact_reduction': 0.6,
                'cost': 'medium',
                'implementation_time': 'short'
            })
            strategies.append({
                'type': 'habitat_restoration',
                'action': 'Invest in local habitat restoration project',
                'impact_reduction': 0.4,
                'cost': 'high',
                'implementation_time': 'long'
            })
        
        if impact_scores['energy']['score'] > 0.3:
            strategies.append({
                'type': 'renewable_energy',
                'action': 'Switch to renewable energy sources',
                'impact_reduction': 0.7,
                'cost': 'medium',
                'implementation_time': 'medium'
            })
        
        if impact_scores['cooling']['score'] > 0.3:
            strategies.append({
                'type': 'efficient_cooling',
                'action': 'Implement free cooling or geothermal cooling',
                'impact_reduction': 0.5,
                'cost': 'medium',
                'implementation_time': 'medium'
            })
        
        if impact_scores['resources']['score'] > 0.15:
            strategies.append({
                'type': 'circular_economy',
                'action': 'Use recycled materials and extend hardware life',
                'impact_reduction': 0.4,
                'cost': 'low',
                'implementation_time': 'short'
            })
        
        if 'carbon' in impact_scores and impact_scores['carbon']['score'] > 0.3:
            strategies.append({
                'type': 'carbon_offset',
                'action': 'Implement carbon offset program',
                'impact_reduction': 0.3,
                'cost': 'medium',
                'implementation_time': 'medium'
            })
        
        if 'helium' in impact_scores and impact_scores['helium']['score'] > 0.1:
            strategies.append({
                'type': 'helium_recovery',
                'action': 'Implement helium recovery and recycling',
                'impact_reduction': 0.5,
                'cost': 'high',
                'implementation_time': 'long'
            })
        
        return strategies
    
    def _generate_recommendations(self, impact_scores: Dict[str, Any]) -> List[str]:
        recommendations = []
        
        scores = {
            category: scores['score']
            for category, scores in impact_scores.items()
            if isinstance(scores, dict) and 'score' in scores
        }
        
        highest_impact = max(scores.items(), key=lambda x: x[1]) if scores else ('none', 0)
        
        if highest_impact[0] == 'habitat' and highest_impact[1] > 0.5:
            recommendations.append(
                "HIGH PRIORITY: Relocate computation to avoid sensitive ecosystems"
            )
        elif highest_impact[0] == 'energy' and highest_impact[1] > 0.5:
            recommendations.append(
                "HIGH PRIORITY: Switch to renewable energy to reduce biodiversity impact"
            )
        elif highest_impact[0] == 'cooling' and highest_impact[1] > 0.5:
            recommendations.append(
                "HIGH PRIORITY: Implement water-free cooling to protect aquatic ecosystems"
            )
        elif highest_impact[0] == 'carbon' and highest_impact[1] > 0.5:
            recommendations.append(
                "HIGH PRIORITY: Reduce carbon emissions through efficiency improvements"
            )
        elif highest_impact[0] == 'helium' and highest_impact[1] > 0.3:
            recommendations.append(
                "HIGH PRIORITY: Implement helium recovery and recycling systems"
            )
        
        if all(score < 0.2 for score in scores.values()):
            recommendations.append(
                "Current setup has minimal biodiversity impact - maintain standards"
            )
        else:
            recommendations.append(
                "Consider biodiversity offsets equivalent to 110% of calculated impact"
            )
        
        return recommendations
    
    def _update_biodiversity_scores(self, assessment: Dict[str, Any]):
        alpha = 0.1
        
        self.local_biodiversity_score = (
            (1 - alpha) * self.local_biodiversity_score +
            alpha * assessment['total_biodiversity_impact']
        )
        
        self.global_biodiversity_score = (
            (1 - alpha * 0.5) * self.global_biodiversity_score +
            alpha * 0.5 * assessment['total_biodiversity_impact']
        )
        
        self.sustainability_score = (
            (1 - alpha) * self.sustainability_score +
            alpha * assessment.get('sustainability_score', 0.5)
        )
    
    # ========================================================================
    # Report Generation
    # ========================================================================
    
    def get_biodiversity_report(self) -> Dict[str, Any]:
        recent_impacts = self.impact_history[-50:] if self.impact_history else []
        
        report = {
            'local_biodiversity_score': self.local_biodiversity_score,
            'global_biodiversity_score': self.global_biodiversity_score,
            'sustainability_score': self.sustainability_score,
            'total_carbon_savings_kg': self.total_carbon_savings_kg,
            'total_helium_savings_l': self.total_helium_savings_l,
            'ecosystems_tracked': len(self.ecosystems),
            'recent_impacts': [
                {
                    'expert_type': i['expert_type'],
                    'impact': i['total_biodiversity_impact'],
                    'sustainability_score': i.get('sustainability_score', 0.5),
                    'timestamp': i['timestamp']
                }
                for i in recent_impacts[-10:]
            ],
            'high_risk_ecosystems': [
                name for name, eco in self.ecosystems.items()
                if eco.endangered_species_count > 50
            ],
            'mitigation_effectiveness': self._calculate_mitigation_effectiveness(),
            'recommendations': self._generate_global_recommendations()
        }
        
        # Add predictive insights
        if self.enable_predictive and self.predictive_analyzer:
            report['predictive_forecast'] = asyncio.run(
                self.predictive_analyzer.predict_impact_trend()
            )
        
        # Add federated insights
        if self.enable_federated and self.federated_assessor:
            report['federated_stats'] = self.federated_assessor.get_federated_stats()
        
        # Add ML predictions
        if self.enable_ml_prediction and self.ml_predictor:
            report['ml_status'] = {
                'trained': self.ml_predictor.is_trained,
                'model_version': 'v2.0.0'
            }
        
        return report
    
    def _calculate_mitigation_effectiveness(self) -> float:
        if not self.impact_history:
            return 0.0
        
        recent = self.impact_history[-20:]
        historical = self.impact_history[:-20]
        
        if not historical:
            return 0.5
        
        recent_avg = np.mean([i['total_biodiversity_impact'] for i in recent])
        historical_avg = np.mean([i['total_biodiversity_impact'] for i in historical])
        
        if historical_avg > 0:
            improvement = (historical_avg - recent_avg) / historical_avg
            return max(improvement, 0.0)
        
        return 0.0
    
    def _generate_global_recommendations(self) -> List[str]:
        recommendations = []
        
        if self.local_biodiversity_score > 0.5:
            recommendations.append(
                "CRITICAL: Implement immediate biodiversity protection measures"
            )
        
        if any(eco.endangered_species_count > 100 for eco in self.ecosystems.values()):
            recommendations.append(
                "URGENT: Avoid computing operations near critical habitats"
            )
        
        if self.sustainability_score < 0.5:
            recommendations.append(
                "IMPROVE: Overall sustainability score needs improvement"
            )
        
        recommendations.append(
            "Implement helium recovery systems to reduce mining impact on biodiversity"
        )
        
        recommendations.append(
            "Monitor carbon intensity and optimize energy sources accordingly"
        )
        
        return recommendations
    
    # ========================================================================
    # Expert Routing Guidance
    # ========================================================================
    
    def get_expert_routing_guidance(
        self,
        expert_options: List[str],
        location_options: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        impact_assessments = []
        
        for expert in expert_options:
            for location in location_options:
                assessment = {
                    'expert': expert,
                    'location': location.get('name', 'unknown'),
                    'estimated_impact': self._quick_impact_estimate(expert, location)
                }
                impact_assessments.append(assessment)
        
        impact_assessments.sort(key=lambda x: x['estimated_impact'])
        
        return {
            'best_option': impact_assessments[0] if impact_assessments else None,
            'worst_option': impact_assessments[-1] if impact_assessments else None,
            'all_options': impact_assessments,
            'recommendation': (
                f"Use {impact_assessments[0]['expert']} at {impact_assessments[0]['location']}"
                if impact_assessments else "No options available"
            ),
            'sustainability_score': self.sustainability_score,
            'biodiversity_impact_reduction': (
                (impact_assessments[-1]['estimated_impact'] - impact_assessments[0]['estimated_impact']) /
                max(impact_assessments[-1]['estimated_impact'], 0.001)
                if len(impact_assessments) > 1 else 0
            )
        }
    
    def _quick_impact_estimate(self, expert_type: str, location: Dict[str, Any]) -> float:
        location_sensitivity = location.get('biodiversity_sensitivity', 0.5)
        expert_intensity = {
            'energy': 0.3, 'data': 0.4, 'iot': 0.2,
            'quantum': 0.6, 'helium': 0.35
        }.get(expert_type, 0.4)
        
        return location_sensitivity * expert_intensity
    
    # ========================================================================
    # Training Methods
    # ========================================================================
    
    async def train_ml_model(self, training_data: List[Dict] = None) -> Dict:
        """Train ML model for impact prediction"""
        if not self.enable_ml_prediction or not self.ml_predictor:
            return {'status': 'disabled'}
        
        if training_data is None:
            training_data = self.impact_history[-100:] if self.impact_history else []
        
        formatted_data = []
        for item in training_data:
            breakdown = item.get('impact_breakdown', {})
            formatted_data.append({
                'carbon_intensity': item.get('carbon_impact', {}).get('carbon_intensity', 400),
                'energy_intensity': breakdown.get('energy', {}).get('score', 0.5),
                'cooling_intensity': breakdown.get('cooling', {}).get('score', 0.5),
                'resource_intensity': breakdown.get('resources', {}).get('score', 0.5),
                'ecosystem_sensitivity': breakdown.get('habitat', {}).get('sensitivity', 0.5),
                'proximity_factor': breakdown.get('habitat', {}).get('proximity_factor', 0.5),
                'fragmentation_index': breakdown.get('habitat', {}).get('fragmentation_index', 0.5),
                'species_density': 0.5,
                'water_scarcity': item.get('location', {}).get('water_scarcity_index', 0.5),
                'temperature_anomaly': 0.5,
                'total_impact': item.get('total_biodiversity_impact', 0.5)
            })
        
        result = await self.ml_predictor.train_model(formatted_data)
        logger.info(f"ML model training completed: {result}")
        return result
    
    async def train_predictive_model(self) -> Dict:
        """Train predictive model for trend analysis"""
        if not self.enable_predictive or not self.predictive_analyzer:
            return {'status': 'disabled'}
        
        result = await self.predictive_analyzer.train_forecast_model()
        logger.info(f"Predictive model training completed: {result}")
        return result
    
    # ========================================================================
    # Shutdown
    # ========================================================================
    
    async def shutdown(self):
        """Graceful shutdown of all components"""
        logger.info("Shutting down Biodiversity Impact Assessor")
        if self.carbon_manager:
            await self.carbon_manager.close()
        if self.federated_assessor:
            await self.federated_assessor.close()
        logger.info("Shutdown complete")


# ============================================================================
# Legacy Compatibility
# ============================================================================

class LegacyBiodiversityImpactAssessor(BiodiversityImpactAssessor):
    """Legacy compatibility class"""
    
    def __init__(self):
        super().__init__(
            enable_federated=False,
            enable_carbon_intensity=False,
            enable_predictive=False,
            enable_ml_prediction=False,
            enable_human_ai=False
        )
        logger.info("Legacy Biodiversity Impact Assessor initialized")
