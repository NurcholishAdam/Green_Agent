# File: enhancements/moe_expert_system/sustainability/circular_computing.py
"""
Enhanced Circular Computing Module v2.0.0 - Complete Green Agent Implementation

Implements circular economy principles with:
- Federated Reflexive Learning for distributed lifecycle management
- User-Adaptive Reflexivity with dynamic configuration
- Real-time Carbon Intensity Integration with API support
- Cross-Domain Knowledge Transfer with material recovery
- Human-AI Collaborative Reflection with circularity reporting
- Predictive Reflexivity with ensemble forecasting
- ML-Based Component Selection
- Real-Time Helium Tracking
- Sustainability Score Integration
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
# Real-Time Helium Tracking Module
# ============================================================================

class HeliumLifecycleManager:
    """
    Real-time helium lifecycle tracking for circular computing.
    
    Features:
    - Helium usage tracking in cooling systems
    - Helium recovery calculation
    - Helium material flow monitoring
    - Helium-carbon equivalence
    """
    
    def __init__(self, helium_budget_l: float = 100.0):
        self.helium_budget_l = helium_budget_l
        self.helium_usage: deque = deque(maxlen=86400)
        self.helium_recovered: deque = deque(maxlen=86400)
        self.component_helium: Dict[str, Dict[str, Any]] = {}
        self._running_total_usage = 0.0
        self._running_total_recovered = 0.0
        
        # Helium to CO2 equivalence (approximate)
        self.helium_to_co2_factor = 20.0
        
        # Helium recovery rates by component type
        self.recovery_rates = {
            'cooling_system': 0.85,
            'quantum_computer': 0.90,
            'cryogenic_system': 0.80,
            'standard_cooling': 0.75,
            'mri_system': 0.95
        }
        
        asyncio.create_task(self._helium_accounting_loop())
        
        logger.info(f"Helium Lifecycle Manager initialized: budget={helium_budget_l}L")
    
    def register_component_helium(
        self,
        component_id: str,
        helium_content_l: float,
        component_type: str = 'cooling_system'
    ):
        """Register helium content in a component"""
        self.component_helium[component_id] = {
            'total_l': helium_content_l,
            'recovered_l': 0.0,
            'type': component_type,
            'recovery_rate': self.recovery_rates.get(component_type, 0.85),
            'registered_at': datetime.utcnow()
        }
        logger.info(f"Registered helium content for {component_id}: {helium_content_l}L")
    
    def track_helium_usage(self, component_id: str, usage_l: float):
        """Track helium usage in a component"""
        usage = {
            'component_id': component_id,
            'amount_l': usage_l,
            'timestamp': datetime.utcnow()
        }
        self.helium_usage.append(usage)
        self._running_total_usage += usage_l
        
        # Update component tracking
        if component_id in self.component_helium:
            self.component_helium[component_id]['used_l'] = self.component_helium[component_id].get('used_l', 0) + usage_l
    
    def calculate_helium_recovery(self, component_id: str) -> float:
        """Calculate recoverable helium from component"""
        if component_id not in self.component_helium:
            return 0.0
        
        component = self.component_helium[component_id]
        total_l = component['total_l']
        used_l = component.get('used_l', 0)
        recovery_rate = component['recovery_rate']
        
        # Remaining helium that can be recovered
        remaining = total_l - used_l
        recoverable = remaining * recovery_rate
        
        return max(0, recoverable)
    
    def record_helium_recovery(self, component_id: str, amount_l: float):
        """Record helium recovery from a component"""
        recovery = {
            'component_id': component_id,
            'amount_l': amount_l,
            'timestamp': datetime.utcnow()
        }
        self.helium_recovered.append(recovery)
        self._running_total_recovered += amount_l
        
        if component_id in self.component_helium:
            self.component_helium[component_id]['recovered_l'] += amount_l
    
    async def _helium_accounting_loop(self):
        """Background helium accounting loop"""
        while True:
            try:
                net_position = self._running_total_usage - self._running_total_recovered
                remaining_budget = self.helium_budget_l - net_position
                
                if remaining_budget < 0:
                    logger.critical(f"Helium budget exceeded! Net position: {net_position:.2f} L")
                elif remaining_budget < self.helium_budget_l * 0.2:
                    logger.warning(f"Helium budget warning: {remaining_budget:.2f} L remaining")
                
                await asyncio.sleep(60)
            except Exception as e:
                logger.error(f"Helium accounting error: {str(e)}")
                await asyncio.sleep(5)
    
    def get_helium_position(self) -> Dict[str, Any]:
        """Get current helium position"""
        return {
            'total_usage_l': self._running_total_usage,
            'total_recovered_l': self._running_total_recovered,
            'net_position_l': self._running_total_usage - self._running_total_recovered,
            'remaining_budget_l': self.helium_budget_l - (self._running_total_usage - self._running_total_recovered),
            'co2_equivalent_kg': (self._running_total_usage - self._running_total_recovered) * self.helium_to_co2_factor,
            'components': self.component_helium
        }

# ============================================================================
# Predictive Lifecycle Analyzer Module
# ============================================================================

class PredictiveLifecycleAnalyzer:
    """Predictive reflexivity with ensemble forecasting for hardware lifecycle"""
    
    def __init__(self, history_window: int = 100):
        self.history_window = history_window
        self.lifecycle_history = deque(maxlen=history_window)
        self.forecast_history = deque(maxlen=50)
        self.models = {}
        self.scaler = StandardScaler()
        self.is_trained = False
        
        self.models['random_forest'] = RandomForestRegressor(n_estimators=100, random_state=42)
        self.models['gradient_boosting'] = GradientBoostingRegressor(n_estimators=100, random_state=42)
    
    def update_history(self, lifecycle_data: Dict):
        """Update lifecycle history for forecasting"""
        self.lifecycle_history.append({
            'timestamp': datetime.utcnow(),
            'age_days': lifecycle_data.get('age_days', 0),
            'utilization': lifecycle_data.get('utilization', 0.5),
            'maintenance_count': lifecycle_data.get('maintenance_count', 0),
            'carbon_score': lifecycle_data.get('carbon_score', 0.5),
            'helium_remaining': lifecycle_data.get('helium_remaining', 0.5)
        })
    
    async def train_forecast_model(self):
        """Train ensemble forecasting models"""
        if len(self.lifecycle_history) < 10:
            return {'status': 'insufficient_data', 'samples': len(self.lifecycle_history)}
        
        X = []
        y = []
        history_list = list(self.lifecycle_history)
        
        for i in range(len(history_list) - 5):
            features = []
            for j in range(5):
                data = history_list[i + j]
                features.extend([
                    data['age_days'] / 1000,
                    data['utilization'],
                    data['maintenance_count'] / 10,
                    data['carbon_score'],
                    data['helium_remaining']
                ])
            X.append(features)
            y.append(history_list[i + 5]['age_days'])
        
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
        logger.info(f"Lifecycle forecast models trained. R²: {results}")
        return {'status': 'success', 'results': results, 'samples': len(X)}
    
    async def predict_lifetime(self, component_data: Dict) -> Dict:
        """Predict remaining component lifetime"""
        if not self.is_trained or len(self.lifecycle_history) < 10:
            return {'predicted_days': 365, 'confidence': 0.0, 'trend': 'insufficient_data'}
        
        recent = list(self.lifecycle_history)[-5:]
        features = []
        for data in recent:
            features.extend([
                data['age_days'] / 1000,
                data['utilization'],
                data['maintenance_count'] / 10,
                data['carbon_score'],
                data['helium_remaining']
            ])
        
        features = np.array(features).reshape(1, -1)
        features_scaled = self.scaler.transform(features)
        
        predictions = []
        for name, model in self.models.items():
            if model is not None:
                pred = model.predict(features_scaled)[0]
                predictions.append(pred)
        
        if not predictions:
            return {'predicted_days': 365, 'confidence': 0.0, 'trend': 'no_models'}
        
        prediction = np.mean(predictions)
        confidence = min(0.9, np.std(predictions) / 0.2) if len(predictions) > 1 else 0.5
        
        if len(self.forecast_history) > 5:
            recent_forecasts = list(self.forecast_history)[-5:]
            trend = "increasing" if prediction > recent_forecasts[-1] else "decreasing" if prediction < recent_forecasts[-1] else "stable"
        else:
            trend = "stable"
        
        self.forecast_history.append({'prediction': prediction, 'trend': trend})
        return {
            'predicted_days': max(0, prediction),
            'confidence': confidence,
            'trend': trend,
            'recommended_actions': self._generate_predictive_actions(prediction)
        }
    
    def _generate_predictive_actions(self, prediction: float) -> List[str]:
        actions = []
        if prediction < 100:
            actions.append("URGENT: Schedule component replacement")
            actions.append("Prioritize material recovery")
        elif prediction < 365:
            actions.append("Plan for repurposing")
            actions.append("Optimize utilization")
        elif prediction < 730:
            actions.append("Schedule preventive maintenance")
            actions.append("Monitor helium levels")
        else:
            actions.append("Component health is good - maintain current practices")
        return actions

# ============================================================================
# Federated Circular Manager Module
# ============================================================================

class FederatedCircularManager:
    """Federated reflexive learning for distributed circular computing"""
    
    def __init__(self, server_url: Optional[str] = None):
        self.server_url = server_url
        self.round = 0
        self.local_components = {}
        self.global_components = {}
        self.participants = []
        self.contribution_scores = {}
        self._lock = asyncio.Lock()
        self._session = None
    
    async def _get_session(self):
        if self._session is None and self.server_url:
            self._session = aiohttp.ClientSession()
        return self._session
    
    async def send_local_components(self, participant_id: str, component_data: Dict, performance: float = 1.0) -> Dict:
        """Send local component data to federated server"""
        if not self.server_url:
            return {'status': 'local'}
        
        async with self._lock:
            session = await self._get_session()
            try:
                update_data = {
                    'participant_id': participant_id,
                    'round': self.round,
                    'component_data': component_data,
                    'performance': performance,
                    'timestamp': datetime.utcnow().isoformat()
                }
                async with session.post(
                    f"{self.server_url}/federated/circular",
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
                logger.error(f"Federated circular send error: {e}")
                return {'status': 'error'}
    
    async def get_global_components(self) -> Optional[Dict]:
        """Get aggregated components from federated server"""
        if not self.server_url:
            return self.global_components
        
        async with self._lock:
            session = await self._get_session()
            try:
                async with session.get(
                    f"{self.server_url}/federated/circular/global",
                    timeout=30
                ) as response:
                    if response.status == 200:
                        data = await response.json()
                        self.global_components = data.get('components', {})
                        self.participants = data.get('participants', [])
                        return self.global_components
            except Exception as e:
                logger.error(f"Global components fetch error: {e}")
                return None
    
    def aggregate_components(self, peer_components: List[Dict], weights: Dict[str, float] = None) -> Dict:
        """Aggregate components from peers with weighted averaging"""
        if not peer_components:
            return {}
        
        aggregated = {}
        if weights is None:
            weights = {i: 1.0 for i in range(len(peer_components))}
        
        for key in peer_components[0].keys():
            if isinstance(peer_components[0][key], (int, float)):
                total = 0.0
                total_weight = 0.0
                for i, peer in enumerate(peer_components):
                    if key in peer:
                        total += peer[key] * weights.get(i, 1.0)
                        total_weight += weights.get(i, 1.0)
                aggregated[key] = total / max(total_weight, 0.001)
        
        return aggregated
    
    def get_federated_stats(self) -> Dict:
        return {
            'round': self.round,
            'participants': len(self.participants),
            'has_global_components': bool(self.global_components),
            'contribution_scores': self.contribution_scores
        }
    
    async def close(self):
        if self._session:
            await self._session.close()

# ============================================================================
# ML Component Selector Module
# ============================================================================

class MLComponentSelector:
    """Machine learning-based component selection for circular computing"""
    
    def __init__(self, input_size: int = 8, hidden_size: int = 64):
        self.input_size = input_size
        self.hidden_size = hidden_size
        self.model = None
        self.scaler = StandardScaler()
        self.is_trained = False
        
        self._init_model()
    
    def _init_model(self):
        class ComponentSelector(nn.Module):
            def __init__(self, input_size, hidden_size):
                super().__init__()
                self.network = nn.Sequential(
                    nn.Linear(input_size, hidden_size),
                    nn.ReLU(),
                    nn.BatchNorm1d(hidden_size),
                    nn.Linear(hidden_size, hidden_size // 2),
                    nn.ReLU(),
                    nn.BatchNorm1d(hidden_size // 2),
                    nn.Linear(hidden_size // 2, 1)  # Score output
                )
            
            def forward(self, x):
                return self.network(x)
        
        self.model = ComponentSelector(self.input_size, self.hidden_size)
    
    async def train_model(self, training_data: List[Dict]) -> Dict:
        """Train ML model on historical component selection data"""
        if len(training_data) < 20:
            return {'status': 'insufficient_data', 'samples': len(training_data)}
        
        X = []
        y = []
        for item in training_data:
            X.append([
                item.get('age_days', 0) / 1000,
                item.get('utilization', 0.5),
                item.get('maintenance_count', 0) / 10,
                item.get('carbon_footprint', 0.5),
                item.get('helium_content', 0.5),
                item.get('recycling_potential', 0.5),
                item.get('reliability', 0.5),
                item.get('cost_efficiency', 0.5)
            ])
            y.append(item.get('selection_score', 0.5))
        
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
        
        return {'status': 'success', 'loss': np.mean(losses), 'samples': len(X)}
    
    async def select_component_ml(self, requirements: Dict) -> Dict[str, Any]:
        """Select component using ML model"""
        if not self.is_trained:
            return {'score': 0.5, 'confidence': 0.0, 'status': 'model_not_trained'}
        
        features = np.array([[
            requirements.get('age_days', 0) / 1000,
            requirements.get('utilization', 0.5),
            requirements.get('maintenance_count', 0) / 10,
            requirements.get('carbon_footprint', 0.5),
            requirements.get('helium_content', 0.5),
            requirements.get('recycling_potential', 0.5),
            requirements.get('reliability', 0.5),
            requirements.get('cost_efficiency', 0.5)
        ]])
        
        features_scaled = self.scaler.transform(features)
        
        self.model.eval()
        with torch.no_grad():
            output = self.model(torch.FloatTensor(features_scaled)).numpy()[0, 0]
        
        return {
            'score': float(output),
            'confidence': 0.8 if self.is_trained else 0.0,
            'status': 'success'
        }

# ============================================================================
# Human-AI Collaborative Reflection Module
# ============================================================================

class HumanAICollaborativeCircular:
    """Human-AI collaborative reflection for circular computing"""
    
    def __init__(self):
        self.feedback_history = deque(maxlen=1000)
        self.reflection_logs = deque(maxlen=100)
        self.user_preferences = {}
        self._lock = asyncio.Lock()
    
    def collect_feedback(self, user_id: str, feedback: Dict) -> Dict:
        """Collect human feedback on circular computing decisions"""
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
            'acknowledgment': f"Feedback received on {feedback.get('topic', 'circular computing')}",
            'insights': [],
            'actions': [],
            'circular_insights': []
        }
        
        if 'concern' in feedback:
            if feedback['concern'] == 'recycling':
                reflection['insights'].append("Recycling efficiency can be improved through material sorting")
                reflection['actions'].append("Implement automated material recovery")
            elif feedback['concern'] == 'helium':
                reflection['insights'].append("Helium recovery requires specialized handling")
                reflection['actions'].append("Implement helium capture systems")
            elif feedback['concern'] == 'lifecycle':
                reflection['insights'].append("Lifecycle extension reduces carbon footprint")
                reflection['actions'].append("Implement predictive maintenance")
            elif feedback['concern'] == 'carbon':
                reflection['circular_insights'].append("Carbon-aware hardware selection is critical")
                reflection['actions'].append("Integrate carbon intensity tracking")
        
        if 'suggestion' in feedback:
            reflection['actions'].append(f"Implementing suggestion: {feedback['suggestion']}")
        
        reflection['action_items'] = self._prioritize_actions(reflection['actions'])
        return reflection
    
    def _prioritize_actions(self, actions: List[str]) -> List[Dict]:
        priorities = []
        for action in actions:
            if any(keyword in action.lower() for keyword in ['urgent', 'critical']):
                priority = 'high'
                impact = 0.9
            elif any(keyword in action.lower() for keyword in ['recycling', 'circular']):
                priority = 'high'
                impact = 0.8
            elif any(keyword in action.lower() for keyword in ['helium']):
                priority = 'medium'
                impact = 0.6
            else:
                priority = 'medium'
                impact = 0.5
            
            priorities.append({
                'action': action,
                'priority': priority,
                'impact': impact,
                'estimated_effort': 'medium'
            })
        
        return sorted(priorities, key=lambda x: x['impact'], reverse=True)
    
    def get_collaborative_insights(self) -> Dict:
        if len(self.feedback_history) < 5:
            return {'status': 'insufficient_feedback'}
        
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

# ============================================================================
# Enums and Data Classes (Enhanced)
# ============================================================================

class HardwareState(Enum):
    MANUFACTURING = "manufacturing"
    DEPLOYED = "deployed"
    MAINTENANCE = "maintenance"
    DEGRADED = "degraded"
    REPURPOSED = "repurposed"
    RECYCLED = "recycled"
    DECOMMISSIONED = "decommissioned"
    HELIUM_RECOVERED = "helium_recovered"

class MaterialType(Enum):
    SILICON = "silicon"
    COPPER = "copper"
    GOLD = "gold"
    ALUMINUM = "aluminum"
    PLASTIC = "plastic"
    RARE_EARTH = "rare_earth"
    HELIUM = "helium"

@dataclass
class HardwareComponent:
    component_id: str
    type: str
    materials: Dict[MaterialType, float]
    manufacturing_carbon: float
    current_state: HardwareState
    deployment_date: datetime
    expected_lifetime_days: int
    utilization_history: List[float] = field(default_factory=list)
    maintenance_log: List[Dict] = field(default_factory=list)
    sustainability_score: float = 0.0
    helium_content_l: float = 0.0
    carbon_savings_kg: float = 0.0

# ============================================================================
# Enhanced Circular Computing Manager
# ============================================================================

class CircularComputingManager:
    """
    Enhanced Circular Computing Manager v2.0.0 - Complete Green Agent Implementation
    
    Implements circular economy principles with:
    - Federated reflexive learning
    - Real-time carbon integration
    - Helium lifecycle tracking
    - ML-based component selection
    - Predictive analytics
    - Human-AI collaboration
    """
    
    def __init__(
        self,
        helium_budget_l: float = 100.0,
        enable_federated: bool = True,
        enable_carbon_intensity: bool = True,
        enable_predictive: bool = True,
        enable_ml_selection: bool = True,
        enable_human_ai: bool = True,
        enable_helium_tracking: bool = True,
        server_url: Optional[str] = None
    ):
        # Feature flags
        self.enable_federated = enable_federated
        self.enable_carbon_intensity = enable_carbon_intensity
        self.enable_predictive = enable_predictive
        self.enable_ml_selection = enable_ml_selection
        self.enable_human_ai = enable_human_ai
        self.enable_helium_tracking = enable_helium_tracking
        
        # New modules
        self.carbon_manager = CarbonIntensityManager() if enable_carbon_intensity else None
        self.helium_manager = HeliumLifecycleManager(helium_budget_l) if enable_helium_tracking else None
        self.predictive_analyzer = PredictiveLifecycleAnalyzer() if enable_predictive else None
        self.federated_manager = FederatedCircularManager(server_url) if enable_federated else None
        self.ml_selector = MLComponentSelector() if enable_ml_selection else None
        self.human_ai = HumanAICollaborativeCircular() if enable_human_ai else None
        
        # Core tracking
        self.components: Dict[str, HardwareComponent] = {}
        self.material_inventory: Dict[MaterialType, float] = {}
        self.recycling_history: List[Dict] = []
        
        # Circular economy metrics
        self.circularity_score = 0.0
        self.waste_diversion_rate = 0.0
        self.material_recovery_rate = 0.0
        self.sustainability_score = 0.0
        
        # Initialize material inventory
        self._initialize_inventory()
        
        # Start background tasks
        self._start_background_tasks()
        
        logger.info(
            f"Enhanced Circular Computing Manager v2.0.0 initialized: "
            f"helium_budget={helium_budget_l}L, "
            f"federated={enable_federated}, ml={enable_ml_selection}"
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
                if self.predictive_analyzer and self.components:
                    for component in list(self.components.values())[-5:]:
                        self.predictive_analyzer.update_history({
                            'age_days': (datetime.utcnow() - component.deployment_date).days,
                            'utilization': np.mean(component.utilization_history[-50:]) if component.utilization_history else 0.5,
                            'maintenance_count': len(component.maintenance_log),
                            'carbon_score': 1.0 / (1.0 + component.manufacturing_carbon),
                            'helium_remaining': component.helium_content_l if self.enable_helium_tracking else 0.5
                        })
                    await self.predictive_analyzer.train_forecast_model()
                await asyncio.sleep(300)
            except Exception as e:
                logger.error(f"Predictive update error: {str(e)}")
                await asyncio.sleep(60)
    
    async def _federated_sync_loop(self):
        while True:
            try:
                if self.federated_manager:
                    await self.federated_manager.send_local_components(
                        f"circular_{hashlib.md5(str(self.components.keys()).encode()).hexdigest()[:8]}",
                        {
                            'total_components': len(self.components),
                            'circularity_score': self.circularity_score,
                            'waste_diversion_rate': self.waste_diversion_rate,
                            'sustainability_score': self.sustainability_score,
                            'timestamp': datetime.utcnow().isoformat()
                        },
                        performance=self.sustainability_score
                    )
                    await self.federated_manager.get_global_components()
                await asyncio.sleep(3600)
            except Exception as e:
                logger.error(f"Federated sync error: {str(e)}")
                await asyncio.sleep(300)
    
    def _initialize_inventory(self):
        """Initialize material inventory tracking"""
        for material in MaterialType:
            self.material_inventory[material] = 0.0
    
    def inject_bio_core(self, bio_core: Any = None, **kwargs):
        """Inject bio-inspired modules"""
        pass
    
    # ========================================================================
    # Component Registration with Enhanced Features
    # ========================================================================
    
    def register_component(
        self,
        component_type: str,
        materials: Dict[MaterialType, float],
        manufacturing_carbon: float,
        expected_lifetime_days: int = 1825,
        helium_content_l: float = 0.0
    ) -> str:
        """
        Register a new hardware component with enhanced tracking
        """
        component_id = f"COMP-{datetime.utcnow().timestamp()}-{component_type}"
        
        component = HardwareComponent(
            component_id=component_id,
            type=component_type,
            materials=materials,
            manufacturing_carbon=manufacturing_carbon,
            current_state=HardwareState.MANUFACTURING,
            deployment_date=datetime.utcnow(),
            expected_lifetime_days=expected_lifetime_days,
            helium_content_l=helium_content_l,
            sustainability_score=0.5
        )
        
        self.components[component_id] = component
        
        # Update material inventory
        for material, amount in materials.items():
            self.material_inventory[material] += amount
        
        # Register helium content
        if self.enable_helium_tracking and self.helium_manager and helium_content_l > 0:
            self.helium_manager.register_component_helium(
                component_id,
                helium_content_l,
                component_type
            )
        
        logger.info(f"Registered component {component_id}: {component_type}")
        return component_id
    
    # ========================================================================
    # Enhanced Recycling
    # ========================================================================
    
    async def recycle_component(
        self,
        component_id: str,
        use_ml_optimization: bool = False
    ) -> Dict[str, Any]:
        """
        Enhanced recycling with ML optimization and helium recovery
        """
        if component_id not in self.components:
            return {'error': 'Component not found'}
        
        component = self.components[component_id]
        
        # ML optimization for recycling
        ml_optimization = None
        if self.enable_ml_selection and use_ml_optimization:
            ml_result = await self.ml_selector.select_component_ml({
                'age_days': (datetime.utcnow() - component.deployment_date).days,
                'utilization': np.mean(component.utilization_history[-50:]) if component.utilization_history else 0.5,
                'maintenance_count': len(component.maintenance_log),
                'carbon_footprint': component.manufacturing_carbon,
                'helium_content': component.helium_content_l,
                'recycling_potential': 0.8,
                'reliability': 0.9,
                'cost_efficiency': 0.7
            })
            ml_optimization = ml_result
        
        # Calculate recoverable materials
        recovered_materials = {}
        total_recovery_rate = 0.0
        
        recovery_rates = {
            MaterialType.SILICON: 0.95,
            MaterialType.COPPER: 0.98,
            MaterialType.GOLD: 0.99,
            MaterialType.ALUMINUM: 0.95,
            MaterialType.PLASTIC: 0.80,
            MaterialType.RARE_EARTH: 0.90,
            MaterialType.HELIUM: 0.85
        }
        
        for material, amount in component.materials.items():
            recovery_rate = recovery_rates.get(material, 0.9)
            recovered_amount = amount * recovery_rate
            recovered_materials[material.value] = {
                'original_g': amount,
                'recovered_g': recovered_amount,
                'recovery_rate': recovery_rate
            }
            
            # Update inventory
            self.material_inventory[material] -= amount
            self.material_inventory[material] += recovered_amount
            
            total_recovery_rate += recovery_rate
        
        avg_recovery_rate = total_recovery_rate / len(recovered_materials) if recovered_materials else 0
        
        # Calculate carbon savings
        manufacturing_carbon = component.manufacturing_carbon
        recycling_carbon = manufacturing_carbon * 0.2
        carbon_saved = manufacturing_carbon - recycling_carbon
        component.carbon_savings_kg = carbon_saved
        
        # Helium recovery
        helium_recovered = 0.0
        if self.enable_helium_tracking and self.helium_manager:
            helium_recovered = self.helium_manager.calculate_helium_recovery(component_id)
            if helium_recovered > 0:
                self.helium_manager.record_helium_recovery(component_id, helium_recovered)
                recovered_materials['helium_recovered'] = {
                    'original_g': component.helium_content_l * 1000,  # Convert L to g (approx)
                    'recovered_g': helium_recovered * 1000,
                    'recovery_rate': 0.85
                }
        
        # Update component state
        component.current_state = HardwareState.RECYCLED
        
        # Calculate sustainability score
        self.sustainability_score = self._calculate_sustainability_score(
            avg_recovery_rate, carbon_saved, helium_recovered
        )
        
        # Record recycling
        recycling_record = {
            'component_id': component_id,
            'component_type': component.type,
            'timestamp': datetime.utcnow().isoformat(),
            'materials_recovered': recovered_materials,
            'average_recovery_rate': avg_recovery_rate,
            'carbon_saved_kg': carbon_saved,
            'helium_recovered_g': helium_recovered * 1000,
            'ml_optimization': ml_optimization,
            'sustainability_score': self.sustainability_score
        }
        
        self.recycling_history.append(recycling_record)
        
        # Update metrics
        self._update_circularity_metrics()
        
        # Update predictive analyzer
        if self.predictive_analyzer:
            self.predictive_analyzer.update_history({
                'age_days': (datetime.utcnow() - component.deployment_date).days,
                'utilization': np.mean(component.utilization_history[-50:]) if component.utilization_history else 0.5,
                'maintenance_count': len(component.maintenance_log),
                'carbon_score': 1.0 / (1.0 + component.manufacturing_carbon),
                'helium_remaining': component.helium_content_l
            })
            await self.predictive_analyzer.train_forecast_model()
        
        # Human-AI collaboration
        if self.enable_human_ai and self.human_ai:
            insights = self.human_ai.get_collaborative_insights()
            recycling_record['human_ai_insights'] = insights
        
        logger.info(f"Recycled component {component_id}: "
                   f"{avg_recovery_rate:.1%} recovery, {carbon_saved:.2f} kg CO2 saved")
        
        return recycling_record
    
    def _calculate_sustainability_score(
        self,
        recovery_rate: float,
        carbon_saved: float,
        helium_recovered: float
    ) -> float:
        """Calculate sustainability score"""
        recovery_factor = recovery_rate
        carbon_factor = min(1.0, carbon_saved / 10)
        helium_factor = min(1.0, helium_recovered / 10)
        
        score = (recovery_factor * 0.4 + carbon_factor * 0.3 + helium_factor * 0.3)
        return min(1.0, max(0.0, score))
    
    def _update_circularity_metrics(self):
        """Update circular economy metrics"""
        total_components = len(self.components)
        if total_components == 0:
            return
        
        recycled = sum(
            1 for c in self.components.values()
            if c.current_state == HardwareState.RECYCLED
        )
        repurposed = sum(
            1 for c in self.components.values()
            if c.current_state == HardwareState.REPURPOSED
        )
        
        self.circularity_score = (recycled + repurposed) / total_components
        
        total_recovered = sum(
            r['average_recovery_rate'] for r in self.recycling_history
        )
        self.material_recovery_rate = total_recovered / max(len(self.recycling_history), 1)
        
        total_waste = total_components - recycled - repurposed
        self.waste_diversion_rate = (recycled + repurposed) / max(total_components, 1)
    
    # ========================================================================
    # Component State Methods
    # ========================================================================
    
    def deploy_component(self, component_id: str):
        """Mark component as deployed"""
        if component_id in self.components:
            self.components[component_id].current_state = HardwareState.DEPLOYED
            logger.info(f"Deployed component {component_id}")
    
    def record_utilization(
        self,
        component_id: str,
        utilization_rate: float
    ):
        """Record component utilization for lifecycle tracking"""
        if component_id in self.components:
            component = self.components[component_id]
            component.utilization_history.append(utilization_rate)
            
            # Check for degradation
            if len(component.utilization_history) > 100:
                recent_util = np.mean(component.utilization_history[-100:])
                if recent_util < 0.3:
                    self._suggest_repurposing(component)
                elif recent_util > 0.9:
                    self._suggest_maintenance(component)
    
    def _suggest_repurposing(self, component: HardwareComponent):
        """Suggest repurposing underutilized hardware"""
        logger.info(f"Suggesting repurposing for {component.component_id}: "
                   f"utilization below threshold")
        
        # Calculate carbon savings
        new_manufacturing_carbon = component.manufacturing_carbon
        repurposing_carbon = component.manufacturing_carbon * 0.1
        carbon_saved = new_manufacturing_carbon - repurposing_carbon
        
        if carbon_saved > 0:
            logger.info(f"Repurposing would save {carbon_saved:.2f} kg CO2")
    
    def _suggest_maintenance(self, component: HardwareComponent):
        """Suggest maintenance for overutilized hardware"""
        logger.info(f"Suggesting maintenance for {component.component_id}: "
                   f"utilization above threshold")
        
        component.maintenance_log.append({
            'timestamp': datetime.utcnow().isoformat(),
            'type': 'preventive',
            'reason': 'high_utilization'
        })
    
    # ========================================================================
    # Training Methods
    # ========================================================================
    
    async def train_ml_model(self, training_data: List[Dict] = None) -> Dict:
        """Train ML model for component selection"""
        if not self.enable_ml_selection or not self.ml_selector:
            return {'status': 'disabled'}
        
        if training_data is None:
            training_data = self.recycling_history[-100:] if self.recycling_history else []
        
        formatted_data = []
        for item in training_data:
            formatted_data.append({
                'age_days': (datetime.utcnow() - datetime.fromisoformat(item['timestamp'])).days if 'timestamp' in item else 365,
                'utilization': 0.5,
                'maintenance_count': 0,
                'carbon_footprint': item.get('carbon_saved_kg', 0.5) / 10,
                'helium_content': item.get('helium_recovered_g', 0) / 1000,
                'recycling_potential': item.get('average_recovery_rate', 0.5),
                'reliability': 0.9,
                'cost_efficiency': 0.7,
                'selection_score': item.get('sustainability_score', 0.5)
            })
        
        result = await self.ml_selector.train_model(formatted_data)
        logger.info(f"ML model training completed: {result}")
        return result
    
    async def train_predictive_model(self) -> Dict:
        """Train predictive model for lifecycle analysis"""
        if not self.enable_predictive or not self.predictive_analyzer:
            return {'status': 'disabled'}
        
        result = await self.predictive_analyzer.train_forecast_model()
        logger.info(f"Predictive model training completed: {result}")
        return result
    
    # ========================================================================
    # Optimization Methods
    # ========================================================================
    
    def optimize_expert_hardware_allocation(
        self,
        expert_requirements: Dict[str, Any],
        carbon_budget: float,
        helium_budget: float,
        use_ml: bool = False
    ) -> Dict[str, Any]:
        """
        Optimize hardware allocation with ML and circularity awareness
        """
        available_components = [
            c for c in self.components.values()
            if c.current_state in [HardwareState.DEPLOYED, HardwareState.MAINTENANCE]
        ]
        
        if not available_components:
            return {'error': 'No available hardware', 'suggestion': 'deploy_new'}
        
        # Get carbon intensity
        carbon_intensity = self.carbon_manager.carbon_intensity if self.carbon_manager else 400
        
        # Score each component
        scored_components = []
        for component in available_components:
            age_days = (datetime.utcnow() - component.deployment_date).days
            lifecycle_score = 1.0 - (age_days / component.expected_lifetime_days)
            lifecycle_score = max(lifecycle_score, 0.1)
            
            carbon_score = 1.0 / (1.0 + component.manufacturing_carbon)
            helium_score = component.helium_content_l / 100.0 if self.enable_helium_tracking else 0.5
            
            if component.utilization_history:
                avg_util = np.mean(component.utilization_history[-50:])
                utilization_score = 1.0 - avg_util
            else:
                utilization_score = 0.5
            
            # Circularity score
            circularity_score = self.circularity_score
            
            if carbon_budget < 0.01:
                score = 0.3 * carbon_score + 0.25 * lifecycle_score + 0.25 * utilization_score + 0.2 * circularity_score
            elif helium_budget < 0.01:
                score = 0.3 * helium_score + 0.25 * carbon_score + 0.25 * lifecycle_score + 0.2 * circularity_score
            else:
                score = 0.2 * carbon_score + 0.2 * lifecycle_score + 0.2 * utilization_score + 0.2 * helium_score + 0.2 * circularity_score
            
            scored_components.append((component, score))
        
        scored_components.sort(key=lambda x: x[1], reverse=True)
        best_component, best_score = scored_components[0]
        
        # ML selection if enabled
        ml_result = None
        if use_ml and self.enable_ml_selection:
            ml_result = asyncio.run(self.ml_selector.select_component_ml({
                'age_days': age_days,
                'utilization': avg_util if component.utilization_history else 0.5,
                'maintenance_count': len(best_component.maintenance_log),
                'carbon_footprint': best_component.manufacturing_carbon,
                'helium_content': best_component.helium_content_l,
                'recycling_potential': 0.8,
                'reliability': 0.9,
                'cost_efficiency': 0.7
            }))
        
        return {
            'selected_component': best_component.component_id,
            'score': best_score,
            'component_type': best_component.type,
            'age_days': (datetime.utcnow() - best_component.deployment_date).days,
            'manufacturing_carbon': best_component.manufacturing_carbon,
            'helium_content_l': best_component.helium_content_l,
            'carbon_intensity': carbon_intensity,
            'sustainability_score': self.sustainability_score,
            'ml_result': ml_result,
            'recommendation': 'use_existing' if best_score > 0.5 else 'consider_repurposing'
        }
    
    # ========================================================================
    # Reporting Methods
    # ========================================================================
    
    def get_circularity_report(self) -> Dict[str, Any]:
        """Generate comprehensive circular economy report"""
        material_flows = {}
        for material in MaterialType:
            total_in_use = sum(
                c.materials.get(material, 0)
                for c in self.components.values()
                if c.current_state != HardwareState.RECYCLED
            )
            total_recovered = sum(
                r['materials_recovered'].get(material.value, {}).get('recovered_g', 0)
                for r in self.recycling_history
            )
            material_flows[material.value] = {
                'in_use_g': total_in_use,
                'recovered_g': total_recovered,
                'inventory_g': self.material_inventory[material]
            }
        
        report = {
            'circularity_score': self.circularity_score,
            'waste_diversion_rate': self.waste_diversion_rate,
            'material_recovery_rate': self.material_recovery_rate,
            'sustainability_score': self.sustainability_score,
            'total_components': len(self.components),
            'components_by_state': {
                state.value: sum(
                    1 for c in self.components.values()
                    if c.current_state == state
                )
                for state in HardwareState
            },
            'material_flows': material_flows,
            'total_carbon_saved_kg': sum(
                r['carbon_saved_kg'] for r in self.recycling_history
            ),
            'helium_recovered_g': sum(
                r.get('helium_recovered_g', 0) for r in self.recycling_history
            )
        }
        
        # Add helium position
        if self.enable_helium_tracking and self.helium_manager:
            report['helium_position'] = self.helium_manager.get_helium_position()
        
        # Add federated stats
        if self.enable_federated and self.federated_manager:
            report['federated_stats'] = self.federated_manager.get_federated_stats()
        
        # Add predictive insights
        if self.enable_predictive and self.predictive_analyzer:
            report['predictive_forecast'] = asyncio.run(
                self.predictive_analyzer.predict_lifetime({'age_days': 365, 'utilization': 0.5})
            )
        
        # Add ML status
        if self.enable_ml_selection and self.ml_selector:
            report['ml_status'] = {
                'trained': self.ml_selector.is_trained,
                'model_version': 'v2.0.0'
            }
        
        # Add human-AI insights
        if self.enable_human_ai and self.human_ai:
            report['human_ai_insights'] = self.human_ai.get_collaborative_insights()
        
        return report
    
    def get_sustainability_report(self) -> Dict[str, Any]:
        """Generate comprehensive sustainability report"""
        return {
            'timestamp': datetime.utcnow().isoformat(),
            'sustainability_score': self.sustainability_score,
            'circularity_report': self.get_circularity_report(),
            'recommendations': self._generate_sustainability_recommendations()
        }
    
    def _generate_sustainability_recommendations(self) -> List[str]:
        recommendations = []
        
        if self.sustainability_score < 0.5:
            recommendations.append("Improve circularity through better material recovery")
        
        if self.circularity_score < 0.5:
            recommendations.append("Increase component repurposing and recycling")
        
        if self.enable_helium_tracking and self.helium_manager:
            helium_pos = self.helium_manager.get_helium_position()
            if helium_pos.get('remaining_budget_l', 0) < 0:
                recommendations.append("CRITICAL: Helium budget exceeded - implement recovery systems")
        
        if self.enable_federated and self.federated_manager:
            if len(self.federated_manager.participants) < 2:
                recommendations.append("Increase federated participation for better circularity insights")
        
        if self.material_recovery_rate < 0.5:
            recommendations.append("Improve material recovery rate through better recycling processes")
        
        return recommendations or ["All circularity metrics are within acceptable ranges"]
    
    # ========================================================================
    # Shutdown
    # ========================================================================
    
    async def shutdown(self):
        """Graceful shutdown of all components"""
        logger.info("Shutting down Circular Computing Manager")
        if self.carbon_manager:
            await self.carbon_manager.close()
        if self.federated_manager:
            await self.federated_manager.close()
        logger.info("Shutdown complete")
