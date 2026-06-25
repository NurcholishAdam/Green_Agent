# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/moe_expert_system/experts/energy_expert.py
"""
Enhanced Energy Expert v6.0.0 - Complete Metabolic Energy Producer
With Natural Language Explanations, Renewable Forecast Analysis, Decision Explanations,
Federated Reflexive Learning, Cross-Domain Knowledge Transfer, Predictive Sustainability,
Enhanced Human-AI Collaboration, and Advanced Sustainability Features
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple, Union
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import numpy as np
from collections import deque
import math
import hashlib
import json
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
    from enhancements.bio_inspired.eco_atp_currency import EcoATPTokenManager, EcoATPSource, EcoATPConsumer
    from enhancements.bio_inspired.proton_gradient_fields import GradientFieldManager
    from enhancements.bio_inspired.atp_synthase_scheduler import ATPSynthaseScheduler
    from enhancements.bio_inspired.chromatophore_compartments import CompartmentManager
    from enhancements.bio_inspired.biomass_storage import BiomassStorage
    from enhancements.bio_inspired.photosynthetic_harvester import PhotosyntheticHarvester
    BIO_INSPIRED_AVAILABLE = True
except ImportError:
    BIO_INSPIRED_AVAILABLE = False

try:
    from ..expert_registry import ExpertProfile, ExpertDomain, HardwareProfile
except ImportError:
    class ExpertDomain(Enum): ENERGY = "energy_optimization"
    class HardwareProfile(Enum): CPU_EFFICIENT = "cpu_low_power"

# ============================================================================
# Enums and Data Classes
# ============================================================================
class EnergySource(Enum):
    SOLAR = "solar"; WIND = "wind"; HYDRO = "hydro"; GEOTHERMAL = "geothermal"
    NUCLEAR = "nuclear"; NATURAL_GAS = "natural_gas"; COAL = "coal"
    GRID_MIX = "grid_mix"; BATTERY = "battery"; HYDROGEN = "hydrogen"; GRADIENT_DRIVEN = "gradient_driven"
    
    @property
    def carbon_intensity_g_per_kwh(self) -> float:
        intensities = {EnergySource.SOLAR: 0, EnergySource.WIND: 0, EnergySource.HYDRO: 0,
                      EnergySource.GEOTHERMAL: 0, EnergySource.NUCLEAR: 12, EnergySource.NATURAL_GAS: 490,
                      EnergySource.COAL: 820, EnergySource.GRID_MIX: 400, EnergySource.BATTERY: 0,
                      EnergySource.HYDROGEN: 0, EnergySource.GRADIENT_DRIVEN: 200}
        return intensities.get(self, 400)
    
    @property
    def is_renewable(self) -> bool:
        return self in [EnergySource.SOLAR, EnergySource.WIND, EnergySource.HYDRO,
                       EnergySource.GEOTHERMAL, EnergySource.BATTERY, EnergySource.HYDROGEN]

class PowerState(Enum):
    PERFORMANCE = "performance"; BALANCED = "balanced"; POWER_SAVE = "power_save"
    ULTRA_LOW = "ultra_low"; DYNAMIC = "dynamic"; ATP_DRIVEN = "atp_driven"
    FEDERATED = "federated"  # New: Federated learning power state

class CoolingMethod(Enum):
    AIR_COOLING = "air"; LIQUID_COOLING = "liquid"; IMMERSION_COOLING = "immersion"
    FREE_COOLING = "free"; GEOTHERMAL_COOLING = "geothermal"; HELIUM_COOLING = "helium"
    COMPARTMENT_AWARE = "compartment_aware"; FEDERATED_COOLING = "federated_cooling"

@dataclass
class RenewableProfile:
    solar_available_kw: float = 0.0; wind_available_kw: float = 0.0
    battery_level_kwh: float = 0.0; battery_capacity_kwh: float = 100.0
    hydrogen_level_kg: float = 0.0; renewable_percentage: float = 0.0
    forecast_next_hour: float = 0.0; peak_solar_time: bool = False
    harvester_contribution_kw: float = 0.0; biomass_reserve_kwh: float = 0.0
    federated_energy_sharing: float = 0.0  # New: Shared energy from federated system
    
    def can_use_renewable(self, required_kw: float) -> bool:
        return (self.solar_available_kw + self.wind_available_kw + self.battery_level_kwh) >= required_kw

@dataclass
class ThermalProfile:
    current_temp_c: float = 35.0; max_temp_c: float = 85.0; throttle_temp_c: float = 75.0
    ambient_temp_c: float = 25.0; cooling_efficiency: float = 0.9
    requires_throttling: bool = False; compartment_health: float = 0.7
    federated_cooling_contribution: float = 0.0  # New: Cooling from federated system
    
    @property
    def thermal_headroom_c(self) -> float: return self.throttle_temp_c - self.current_temp_c

@dataclass
class EnergyOptimizationHistory:
    timestamp: datetime; strategy: str; energy_source: str; power_state: str
    energy_saved_kwh: float; carbon_saved_kg: float; cost_saved: float
    renewable_used: bool; success: bool
    metrics: Dict[str, float] = field(default_factory=dict)
    ecoatp_generated: float = 0.0; gradient_level: float = 0.5
    federated_round: int = 0  # New: Federated learning round
    cross_domain_transfers: List[str] = field(default_factory=list)  # New

@dataclass
class FederatedLearningState:
    """State for federated reflexive learning"""
    round: int = 0
    local_model_weights: Dict = field(default_factory=dict)
    global_model_weights: Dict = field(default_factory=dict)
    contribution_score: float = 0.0
    participants: List[str] = field(default_factory=list)
    last_aggregation: Optional[datetime] = None
    energy_sharing_ratio: float = 0.0

@dataclass
class PredictiveEnergyForecast:
    """Predictive energy forecast"""
    timestamp: datetime = field(default_factory=datetime.utcnow)
    predicted_energy_kwh: float = 0.0
    predicted_carbon_kg: float = 0.0
    confidence: float = 0.0
    trend: str = "stable"
    factors: List[Dict[str, Any]] = field(default_factory=list)
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
class SustainabilityMetrics:
    """Sustainability metrics for energy optimization"""
    carbon_savings_kg: float = 0.0
    helium_savings_l: float = 0.0
    energy_savings_kwh: float = 0.0
    renewable_percentage: float = 0.0
    sustainability_score: float = 0.0
    ecoatp_generated: float = 0.0

# ============================================================================
# Federated Reflexive Learning Module
# ============================================================================

class FederatedEnergyLearner:
    """Federated reflexive learning for distributed energy optimization"""
    
    def __init__(self, expert_id: str, server_url: Optional[str] = None):
        self.expert_id = expert_id
        self.server_url = server_url
        self.state = FederatedLearningState()
        self._lock = asyncio.Lock()
        self._session = None
        self.local_model = None
        self.global_model = None
        self.energy_history = deque(maxlen=1000)
        
        # Initialize local model
        self._init_energy_model()
    
    def _init_energy_model(self):
        """Initialize local energy optimization model"""
        class EnergyOptimizerModel(nn.Module):
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
        
        self.local_model = EnergyOptimizerModel()
        self.global_model = EnergyOptimizerModel()
    
    async def _get_session(self):
        if self._session is None and self.server_url:
            self._session = aiohttp.ClientSession()
        return self._session
    
    async def train_local_model(self, energy_data: List[Dict[str, float]], epochs: int = 10) -> float:
        """Train local energy optimization model"""
        if not energy_data:
            return 0.0
        
        X = []
        y = []
        for item in energy_data:
            X.append([
                item.get('energy_consumption', 0.5),
                item.get('carbon_intensity', 0.5),
                item.get('renewable_percentage', 0.5),
                item.get('thermal_load', 0.5),
                item.get('helium_scarcity', 0.5),
                item.get('grid_carbon', 0.5),
                item.get('solar_available', 0.5),
                item.get('wind_available', 0.5),
                item.get('battery_level', 0.5),
                item.get('ecoatp_balance', 0.5)
            ])
            y.append(item.get('optimization_score', 0.5))
        
        X = torch.FloatTensor(X)
        y = torch.FloatTensor(y).unsqueeze(1)
        
        dataset = TensorDataset(X, y)
        dataloader = DataLoader(dataset, batch_size=32, shuffle=True)
        
        optimizer = optim.Adam(self.local_model.parameters(), lr=0.001)
        criterion = nn.MSELoss()
        
        total_loss = 0
        for epoch in range(epochs):
            epoch_loss = 0
            for batch_X, batch_y in dataloader:
                optimizer.zero_grad()
                output = self.local_model(batch_X)
                loss = criterion(output, batch_y)
                loss.backward()
                torch.nn.utils.clip_grad_norm_(self.local_model.parameters(), 1.0)
                optimizer.step()
                epoch_loss += loss.item()
            total_loss += epoch_loss
        
        avg_loss = total_loss / epochs
        logger.info(f"Local energy model trained. Loss: {avg_loss:.4f}")
        return avg_loss
    
    async def send_local_update(self, performance_metric: float = 1.0) -> Dict:
        """Send local model update to federated server"""
        if not self.server_url:
            return {'status': 'disabled'}
        
        async with self._lock:
            session = await self._get_session()
            
            try:
                weights = self.local_model.state_dict()
                weights_serialized = {k: v.tolist() for k, v in weights.items()}
                
                update_data = {
                    'expert_id': self.expert_id,
                    'round': self.state.round,
                    'weights': weights_serialized,
                    'performance': performance_metric,
                    'energy_sharing_ratio': self.state.energy_sharing_ratio,
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
                        logger.info(f"Federated energy update sent. Round: {self.state.round}")
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
                    f"{self.server_url}/federated/global/energy",
                    timeout=30
                ) as response:
                    if response.status == 200:
                        data = await response.json()
                        weights = data.get('weights', {})
                        self.state.global_model_weights = weights
                        self.state.round = data.get('round', 0)
                        self.state.participants = data.get('participants', [])
                        self.state.energy_sharing_ratio = data.get('sharing_ratio', 0.0)
                        
                        for k, v in weights.items():
                            self.global_model.state_dict()[k] = torch.FloatTensor(v)
                        
                        return weights
                        
            except Exception as e:
                logger.error(f"Global model fetch error: {e}")
                return None
    
    async def participate_in_round(self, energy_data: List[Dict[str, float]], 
                                  performance: float = 1.0) -> Dict:
        """Full participation in federated learning round"""
        await self.train_local_model(energy_data)
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
            'energy_sharing_ratio': self.state.energy_sharing_ratio,
            'timestamp': datetime.utcnow().isoformat()
        }
    
    def get_federated_insights(self) -> Dict:
        """Get insights from federated learning"""
        return {
            'round': self.state.round,
            'contribution_score': self.state.contribution_score,
            'participants': len(self.state.participants),
            'has_global_model': bool(self.state.global_model_weights),
            'energy_sharing_ratio': self.state.energy_sharing_ratio,
            'last_aggregation': self.state.last_aggregation.isoformat() if self.state.last_aggregation else None
        }
    
    async def close(self):
        if self._session:
            await self._session.close()

# ============================================================================
# Cross-Domain Knowledge Transfer Module
# ============================================================================

class EnergyCrossDomainTransfer:
    """Cross-domain knowledge transfer for energy optimization"""
    
    def __init__(self):
        self.knowledge_base: Dict[str, Dict[str, CrossDomainKnowledge]] = {}
        self.transfer_logs = deque(maxlen=1000)
        self.domain_mappings = {
            'energy→data': {
                'compression_strategies': ['adaptive', 'greedy', 'bio-inspired', 'predictive'],
                'efficiency_patterns': ['peak-shaving', 'load-balancing', 'demand-response'],
                'sustainability_patterns': ['carbon-aware', 'helium-efficient', 'renewable-optimized']
            },
            'energy→carbon': {
                'optimization_patterns': ['load-shifting', 'efficiency-first', 'renewable-tracking'],
                'forecasting_methods': ['time-series', 'ml', 'ensemble', 'gradient-based']
            },
            'energy→helium': {
                'efficiency_strategies': ['recovery', 'reuse', 'minimization', 'optimization'],
                'cooling_patterns': ['passive', 'active', 'hybrid', 'federated']
            },
            'energy→quantum': {
                'scheduling_strategies': ['cooldown-aware', 'energy-budget', 'performance-optimized'],
                'resource_allocation': ['dynamic', 'static', 'predictive']
            }
        }
        self._lock = asyncio.Lock()
        self.effectiveness_history = deque(maxlen=100)
    
    def transfer_knowledge(self, source_domain: str, target_domain: str, 
                          knowledge_type: str, data: Dict[str, Any]) -> CrossDomainKnowledge:
        """Transfer knowledge between domains"""
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
        
        logger.info(f"Energy knowledge transferred: {source_domain}→{target_domain} ({knowledge_type})")
        return knowledge
    
    def get_transferred_knowledge(self, source_domain: str, target_domain: str, 
                                 knowledge_type: str) -> Optional[CrossDomainKnowledge]:
        """Retrieve transferred knowledge"""
        key = f"{source_domain}→{target_domain}"
        if key in self.knowledge_base and knowledge_type in self.knowledge_base[key]:
            return self.knowledge_base[key][knowledge_type]
        return None
    
    async def apply_data_knowledge(self, energy_data: Dict) -> Dict:
        """Apply knowledge from data domain to energy optimization"""
        data_knowledge = self.get_transferred_knowledge('data', 'energy', 'compression_strategies')
        if data_knowledge:
            strategies = data_knowledge.data.get('strategies', [])
            if strategies:
                return {
                    'applied_strategy': strategies[0],
                    'expected_savings': data_knowledge.effectiveness_score * 0.15,
                    'source': 'data_domain',
                    'confidence': min(1.0, data_knowledge.transfer_count / 10)
                }
        return {'applied_strategy': 'default', 'source': 'local', 'confidence': 0.5}
    
    async def apply_carbon_knowledge(self, energy_data: Dict) -> Dict:
        """Apply knowledge from carbon domain to energy optimization"""
        carbon_knowledge = self.get_transferred_knowledge('carbon', 'energy', 'optimization_patterns')
        if carbon_knowledge:
            patterns = carbon_knowledge.data.get('patterns', [])
            return {
                'carbon_aware_optimization': True,
                'optimization_impact': carbon_knowledge.effectiveness_score,
                'source': 'carbon_domain',
                'patterns': patterns
            }
        return {'carbon_aware_optimization': False, 'source': 'local'}
    
    async def apply_helium_knowledge(self, energy_data: Dict) -> Dict:
        """Apply knowledge from helium domain to energy optimization"""
        helium_knowledge = self.get_transferred_knowledge('helium', 'energy', 'efficiency_strategies')
        if helium_knowledge:
            return {
                'helium_aware': True,
                'efficiency_gain': helium_knowledge.effectiveness_score * 0.2,
                'source': 'helium_domain',
                'strategies': helium_knowledge.data.get('strategies', [])
            }
        return {'helium_aware': False, 'source': 'local'}
    
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
# Predictive Sustainability Module
# ============================================================================

class PredictiveEnergySustainability:
    """Predictive sustainability analytics for energy optimization"""
    
    def __init__(self, history_window: int = 100):
        self.history_window = history_window
        self.energy_history = deque(maxlen=history_window)
        self.sustainability_history = deque(maxlen=history_window)
        self.forecast_history = deque(maxlen=50)
        self.models = {}
        self.scaler = StandardScaler()
        self.is_trained = False
        
        # Multiple models for ensemble forecasting
        self.models['random_forest'] = RandomForestRegressor(n_estimators=100, random_state=42)
        self.models['gradient_boosting'] = GradientBoostingRegressor(n_estimators=100, random_state=42)
    
    def update_history(self, energy_data: Dict, sustainability_metrics: Dict):
        """Update energy and sustainability history"""
        self.energy_history.append({
            'timestamp': datetime.utcnow(),
            'energy_kwh': energy_data.get('energy_kwh', 0),
            'carbon_kg': energy_data.get('carbon_kg', 0),
            'renewable_pct': energy_data.get('renewable_pct', 0),
            'helium_usage': energy_data.get('helium_usage', 0)
        })
        
        self.sustainability_history.append({
            'timestamp': datetime.utcnow(),
            'carbon_savings': sustainability_metrics.get('carbon_savings_kg', 0),
            'helium_savings': sustainability_metrics.get('helium_savings_l', 0),
            'energy_savings': sustainability_metrics.get('energy_savings_kwh', 0),
            'sustainability_score': sustainability_metrics.get('sustainability_score', 0)
        })
    
    async def train_forecast_model(self):
        """Train ensemble forecasting models"""
        if len(self.energy_history) < 10:
            return {'status': 'insufficient_data', 'samples': len(self.energy_history)}
        
        X = []
        y = []
        history_list = list(self.energy_history)
        
        for i in range(len(history_list) - 5):
            features = []
            for j in range(5):
                data = history_list[i + j]
                features.extend([
                    data['energy_kwh'],
                    data['carbon_kg'],
                    data['renewable_pct'],
                    data['helium_usage']
                ])
            X.append(features)
            y.append(history_list[i + 5]['energy_kwh'])
        
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
        logger.info(f"Energy forecast models trained. R² scores: {results}")
        return {'status': 'success', 'results': results, 'samples': len(X)}
    
    async def predict_energy_trend(self, hours: int = 24) -> PredictiveEnergyForecast:
        """Predict future energy consumption and sustainability impact"""
        if not self.is_trained or len(self.energy_history) < 10:
            return PredictiveEnergyForecast(
                predicted_energy_kwh=0.5,
                predicted_carbon_kg=0.5,
                confidence=0.0,
                trend="insufficient_data"
            )
        
        recent = list(self.energy_history)[-5:]
        features = []
        for data in recent:
            features.extend([
                data['energy_kwh'],
                data['carbon_kg'],
                data['renewable_pct'],
                data['helium_usage']
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
            return PredictiveEnergyForecast(
                predicted_energy_kwh=0.5,
                predicted_carbon_kg=0.5,
                confidence=0.0,
                trend="no_models"
            )
        
        # Weighted average
        prediction = np.mean(predictions)
        confidence = min(0.9, np.std(predictions) / 0.2) if len(predictions) > 1 else 0.5
        
        # Predict carbon impact
        carbon_prediction = prediction * 0.4  # Rough estimate
        
        # Determine trend
        if len(self.forecast_history) > 5:
            recent_forecasts = list(self.forecast_history)[-5:]
            trend = "increasing" if prediction > recent_forecasts[-1] else "decreasing" if prediction < recent_forecasts[-1] else "stable"
        else:
            trend = "stable"
        
        # Generate recommended actions
        recommended_actions = self._generate_predictive_actions(prediction, carbon_prediction)
        
        forecast = PredictiveEnergyForecast(
            predicted_energy_kwh=prediction,
            predicted_carbon_kg=carbon_prediction,
            confidence=confidence,
            trend=trend,
            factors=[
                {'name': 'Ensemble average', 'value': prediction, 'weight': 0.6},
                {'name': 'Model confidence', 'value': confidence, 'weight': 0.4}
            ],
            recommended_actions=recommended_actions
        )
        
        self.forecast_history.append(forecast)
        return forecast
    
    def _generate_predictive_actions(self, energy_prediction: float, carbon_prediction: float) -> List[str]:
        """Generate recommended actions based on predictions"""
        actions = []
        
        if energy_prediction > 1.0:
            actions.append("Optimize energy consumption through power state reduction")
        
        if carbon_prediction > 0.5:
            actions.append("Shift workload to lower-carbon energy sources")
            actions.append("Increase renewable energy integration")
        
        if len(self.energy_history) > 20:
            recent_trend = np.mean([h['energy_kwh'] for h in list(self.energy_history)[-10:]])
            if energy_prediction > recent_trend * 1.2:
                actions.append("Implement peak-shaving strategies")
        
        return actions or ["Current energy trends are sustainable"]
    
    def get_sustainability_summary(self) -> Dict:
        """Get sustainability summary"""
        if not self.sustainability_history:
            return {'status': 'insufficient_data'}
        
        recent = list(self.sustainability_history)[-50:]
        
        return {
            'average_carbon_savings': np.mean([h['carbon_savings'] for h in recent]),
            'average_helium_savings': np.mean([h['helium_savings'] for h in recent]),
            'average_energy_savings': np.mean([h['energy_savings'] for h in recent]),
            'current_sustainability_score': recent[-1]['sustainability_score'] if recent else 0,
            'trend': 'improving' if len(recent) > 10 and recent[-1]['sustainability_score'] > recent[0]['sustainability_score'] else 'stable'
        }

# ============================================================================
# Enhanced Energy Expert (Main Class)
# ============================================================================

class EnergyExpert:
    """Enhanced Energy Expert v6.0.0 with all green agent capabilities"""
    
    def __init__(self, expert_id: str = "energy_optimizer_v6", enable_renewable: bool = True,
                 enable_storage: bool = True, enable_thermal: bool = True, enable_dvfs: bool = True,
                 enable_forecasting: bool = True, enable_bio_integration: bool = True,
                 enable_federated: bool = True, enable_cross_domain: bool = True,
                 enable_predictive_sustainability: bool = True):
        self.expert_id = expert_id
        self.version = "6.0.0"
        self.enable_renewable = enable_renewable
        self.enable_storage = enable_storage
        self.enable_thermal = enable_thermal
        self.enable_dvfs = enable_dvfs
        self.enable_forecasting = enable_forecasting
        self.enable_bio_integration = enable_bio_integration and BIO_INSPIRED_AVAILABLE
        self.enable_federated = enable_federated
        self.enable_cross_domain = enable_cross_domain
        self.enable_predictive_sustainability = enable_predictive_sustainability
        
        # Bio-inspired components
        self.token_manager = None
        self.gradient_manager = None
        self.scheduler = None
        self.compartment_manager = None
        self.biomass_storage = None
        self.harvester = None
        
        # New modules
        self.federated_learner = FederatedEnergyLearner(expert_id)
        self.cross_domain_transfer = EnergyCrossDomainTransfer()
        self.predictive_sustainability = PredictiveEnergySustainability()
        
        self.profile = ExpertProfile(
            expert_id=expert_id,
            domain=ExpertDomain.ENERGY,
            hardware_profile=HardwareProfile.CPU_EFFICIENT,
            helium_per_inference=0.008,
            carbon_per_inference=0.00008,
            energy_per_inference=0.0008,
            avg_latency_ms=40.0,
            accuracy_score=0.94,
            reliability_score=0.97,
            efficiency_score=0.99,
            supported_task_types=['inference', 'training', 'optimization', 'energy_management']
        )
        
        # Enhanced power states with federated support
        self.power_states = {
            PowerState.PERFORMANCE: {'frequency_percent': 100, 'energy_factor': 1.0, 'performance_factor': 1.0},
            PowerState.BALANCED: {'frequency_percent': 70, 'energy_factor': 0.7, 'performance_factor': 0.85},
            PowerState.POWER_SAVE: {'frequency_percent': 50, 'energy_factor': 0.5, 'performance_factor': 0.65},
            PowerState.ULTRA_LOW: {'frequency_percent': 25, 'energy_factor': 0.25, 'performance_factor': 0.35},
            PowerState.DYNAMIC: {'frequency_percent': 0, 'energy_factor': 0.0, 'performance_factor': 0.0},
            PowerState.ATP_DRIVEN: {'frequency_percent': 0, 'energy_factor': 0.0, 'performance_factor': 0.0},
            PowerState.FEDERATED: {'frequency_percent': 0, 'energy_factor': 0.5, 'performance_factor': 0.75}
        }
        
        self.quantization_levels = {
            'fp32': {'energy_factor': 1.0, 'accuracy_impact': 0.0, 'ecoatp_cost': 10, 'sustainability_score': 0.5},
            'fp16': {'energy_factor': 0.5, 'accuracy_impact': 0.01, 'ecoatp_cost': 5, 'sustainability_score': 0.7},
            'bf16': {'energy_factor': 0.5, 'accuracy_impact': 0.005, 'ecoatp_cost': 5, 'sustainability_score': 0.7},
            'int8': {'energy_factor': 0.25, 'accuracy_impact': 0.03, 'ecoatp_cost': 2, 'sustainability_score': 0.85},
            'int4': {'energy_factor': 0.125, 'accuracy_impact': 0.05, 'ecoatp_cost': 1, 'sustainability_score': 0.9}
        }
        
        self.cooling_methods = {
            CoolingMethod.AIR_COOLING: {'energy_overhead': 0.02, 'max_cooling_capacity_kw': 50, 'helium_usage': 0.0, 'sustainability_score': 0.6},
            CoolingMethod.LIQUID_COOLING: {'energy_overhead': 0.05, 'max_cooling_capacity_kw': 200, 'helium_usage': 0.0, 'sustainability_score': 0.5},
            CoolingMethod.IMMERSION_COOLING: {'energy_overhead': 0.03, 'max_cooling_capacity_kw': 500, 'helium_usage': 0.0, 'sustainability_score': 0.7},
            CoolingMethod.FREE_COOLING: {'energy_overhead': 0.0, 'max_cooling_capacity_kw': 30, 'helium_usage': 0.0, 'sustainability_score': 0.9},
            CoolingMethod.GEOTHERMAL_COOLING: {'energy_overhead': 0.01, 'max_cooling_capacity_kw': 100, 'helium_usage': 0.0, 'sustainability_score': 0.85},
            CoolingMethod.HELIUM_COOLING: {'energy_overhead': 0.10, 'max_cooling_capacity_kw': 1000, 'helium_usage': 0.05, 'sustainability_score': 0.4},
            CoolingMethod.COMPARTMENT_AWARE: {'energy_overhead': 0.0, 'max_cooling_capacity_kw': 0, 'helium_usage': 0.0, 'sustainability_score': 0.8},
            CoolingMethod.FEDERATED_COOLING: {'energy_overhead': 0.0, 'max_cooling_capacity_kw': 0, 'helium_usage': 0.0, 'sustainability_score': 0.75}
        }
        
        self.optimization_history: deque = deque(maxlen=10000)
        self.total_energy_saved_kwh = 0.0
        self.total_carbon_saved_kg = 0.0
        self.total_cost_saved = 0.0
        self.total_ecoatp_generated = 0.0
        self.total_helium_saved = 0.0
        
        self.adaptive_thresholds = {
            'renewable_switch_threshold': 0.3,
            'battery_use_threshold': 0.5,
            'thermal_throttle_threshold': 75.0,
            'dvfs_aggressiveness': 0.5,
            'federated_sharing_threshold': 0.3,
            'carbon_budget_buffer': 0.2
        }
        
        self.sustainability_metrics = SustainabilityMetrics()
        
        logger.info(f"Energy Expert v{self.version} initialized with all green agent features")
    
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
    
    def _get_gradient_energy_source(self) -> EnergySource:
        if self.gradient_manager:
            carbon = self.gradient_manager.fields.get('carbon')
            if carbon and carbon.gradient_strength > 0.7:
                return EnergySource.BATTERY
            elif carbon and carbon.gradient_strength < 0.3:
                return EnergySource.GRID_MIX
        return EnergySource.SOLAR
    
    def _get_atp_driven_dvfs(self) -> PowerState:
        if self.scheduler:
            df = self.scheduler.calculate_gradient_driving_force()
            rs = self.scheduler.calculate_rotation_speed(df)
            rate = self.scheduler.calculate_atp_production_rate(rs)
            if rate > 100:
                return PowerState.PERFORMANCE
            elif rate > 50:
                return PowerState.BALANCED
            elif rate > 20:
                return PowerState.POWER_SAVE
            else:
                return PowerState.ULTRA_LOW
        return PowerState.ATP_DRIVEN
    
    def _get_token_efficient_quantization(self, task_type: str = 'inference') -> str:
        if self.token_manager:
            summary = self.token_manager.get_system_summary()
            balance = summary.get('total_balance', 500)
            if balance < 100:
                return 'int4'
            elif balance < 300:
                return 'int8'
            else:
                return 'fp16' if task_type == 'training' else 'int8'
        return 'int8'
    
    def _get_compartment_thermal_state(self) -> ThermalProfile:
        if self.compartment_manager:
            compartment = self.compartment_manager.find_best_compartment('energy')
            if compartment:
                health = compartment.health_score
                temp = 35.0 + (1.0 - health) * 40.0
                return ThermalProfile(
                    current_temp_c=temp,
                    compartment_health=health,
                    requires_throttling=health < 0.3
                )
        return ThermalProfile(current_temp_c=40.0, compartment_health=0.7)
    
    def _get_harvester_renewable_forecast(self) -> Dict[str, float]:
        if self.harvester:
            stats = self.harvester.get_harvesting_stats()
            total = stats.get('total_harvested', 0)
            recent = stats.get('recent_conversions', [])
            avg = np.mean([c.get('convertible_energy', 0.5) for c in recent[-10:]]) if recent else 0.5
            return {
                'solar_kw': total * 0.6 * avg,
                'wind_kw': total * 0.4 * avg,
                'total_renewable_kw': total * avg,
                'confidence': avg
            }
        return {'solar_kw': 5.0, 'wind_kw': 3.0, 'total_renewable_kw': 8.0, 'confidence': 0.5}
    
    def _generate_offset_tokens(self, carbon_kg: float) -> float:
        if self.token_manager:
            tokens = self.token_manager.generate_tokens(
                account_id='energy_expert_offsets',
                source=EcoATPSource.CARBON_OFFSET,
                carbon_saved_kg=carbon_kg,
                num_tokens=int(carbon_kg * 100)
            )
            if tokens:
                total = sum(t.value for t in tokens)
                self.total_ecoatp_generated += total
                return total
        return 0.0
    
    def _get_gradient_levels(self) -> Dict[str, float]:
        if self.gradient_manager:
            return self.gradient_manager.get_field_strengths()
        return {'carbon': 0.5, 'helium': 0.5, 'trust': 0.5, 'opportunity': 0.5}
    
    # ========================================================================
    # Primary Optimization with Enhanced Features
    # ========================================================================
    
    async def optimize_energy(self, task_config: Dict[str, Any], carbon_budget: float,
                             latency_requirement_ms: float, grid_carbon_intensity: Optional[float] = None,
                             renewable_profile: Optional[RenewableProfile] = None,
                             thermal_profile: Optional[ThermalProfile] = None,
                             time_of_day: Optional[int] = None, energy_price_per_kwh: Optional[float] = None,
                             helium_scarcity: float = 0.0, cross_expert_hints: Optional[Dict[str, Any]] = None,
                             ecoatp_budget: Optional[float] = None) -> Dict[str, Any]:
        start_time = datetime.utcnow()
        optimization_id = hashlib.md5(f"{task_config}{carbon_budget}{latency_requirement_ms}{start_time}".encode()).hexdigest()[:12]
        
        gradient_levels = self._get_gradient_levels() if self.enable_bio_integration else {}
        
        # Apply cross-domain knowledge
        if self.enable_cross_domain:
            data_knowledge = await self.cross_domain_transfer.apply_data_knowledge(task_config)
            carbon_knowledge = await self.cross_domain_transfer.apply_carbon_knowledge({'budget': carbon_budget})
            helium_knowledge = await self.cross_domain_transfer.apply_helium_knowledge({'scarcity': helium_scarcity})
            
            if data_knowledge.get('applied_strategy') != 'default':
                logger.info(f"Applied data knowledge: {data_knowledge['applied_strategy']}")
        
        # Determine energy source
        energy_source = self._get_gradient_energy_source() if self.enable_bio_integration else (
            EnergySource.SOLAR if renewable_profile and renewable_profile.can_use_renewable(10) else EnergySource.GRID_MIX)
        
        # Federated learning optimization
        if self.enable_federated:
            federated_insights = self.federated_learner.get_federated_insights()
            if federated_insights.get('has_global_model', False):
                power_state = PowerState.FEDERATED
            else:
                power_state = self._get_atp_driven_dvfs() if self.enable_bio_integration else (
                    PowerState.PERFORMANCE if latency_requirement_ms < 10 else PowerState.BALANCED)
        else:
            power_state = self._get_atp_driven_dvfs() if self.enable_bio_integration else (
                PowerState.PERFORMANCE if latency_requirement_ms < 10 else PowerState.BALANCED)
        
        power_config = self.power_states[power_state]
        
        quant_level = self._get_token_efficient_quantization(task_config.get('task_type', 'inference')) if self.enable_bio_integration else (
            'int8' if carbon_budget < 0.01 else 'fp16')
        quant_config = self.quantization_levels[quant_level]
        
        thermal = self._get_compartment_thermal_state() if self.enable_bio_integration else (thermal_profile or ThermalProfile())
        
        energy_factor = power_config['energy_factor'] * quant_config['energy_factor']
        base_energy = task_config.get('base_energy_kwh', 0.01)
        estimated_energy = base_energy * energy_factor
        estimated_carbon = estimated_energy * energy_source.carbon_intensity_g_per_kwh / 1000
        
        ecoatp_cost = estimated_energy * 1000
        if ecoatp_budget and ecoatp_cost > ecoatp_budget and self.enable_bio_integration:
            power_state = PowerState.POWER_SAVE
            quant_level = 'int8'
            energy_factor = self.power_states[power_state]['energy_factor'] * self.quantization_levels[quant_level]['energy_factor']
            estimated_energy = base_energy * energy_factor
            ecoatp_cost = estimated_energy * 1000
        
        ecoatp_generated = self._generate_offset_tokens(estimated_carbon) if self.enable_bio_integration and estimated_carbon > 0 else 0.0
        
        # Update sustainability metrics
        self.sustainability_metrics.carbon_savings_kg += max(0, base_energy * 400 / 1000 - estimated_carbon)
        self.sustainability_metrics.energy_savings_kwh += max(0, base_energy - estimated_energy)
        self.sustainability_metrics.renewable_percentage = 100 if energy_source.is_renewable else 25
        self.sustainability_metrics.ecoatp_generated += ecoatp_generated
        self.sustainability_metrics.sustainability_score = min(1.0, (
            self.sustainability_metrics.renewable_percentage / 100 * 0.3 +
            min(1.0, self.sustainability_metrics.carbon_savings_kg / 10) * 0.4 +
            min(1.0, self.sustainability_metrics.ecoatp_generated / 10) * 0.3
        ))
        
        # Update predictive sustainability history
        if self.enable_predictive_sustainability:
            self.predictive_sustainability.update_history(
                {'energy_kwh': estimated_energy, 'carbon_kg': estimated_carbon,
                 'renewable_pct': self.sustainability_metrics.renewable_percentage,
                 'helium_usage': helium_scarcity * 0.01},
                self.sustainability_metrics.__dict__
            )
        
        # Generate predictive forecast
        predictive_forecast = None
        if self.enable_predictive_sustainability:
            forecast = await self.predictive_sustainability.predict_energy_trend()
            predictive_forecast = {
                'predicted_energy': forecast.predicted_energy_kwh,
                'predicted_carbon': forecast.predicted_carbon_kg,
                'confidence': forecast.confidence,
                'trend': forecast.trend,
                'recommended_actions': forecast.recommended_actions
            }
        
        # Participate in federated learning
        federated_result = None
        if self.enable_federated and estimated_carbon > 0:
            federated_result = await self.federated_learner.participate_in_round(
                [{'energy_consumption': estimated_energy, 'carbon_intensity': estimated_carbon,
                  'renewable_percentage': self.sustainability_metrics.renewable_percentage,
                  'optimization_score': self.sustainability_metrics.sustainability_score}],
                performance=self.sustainability_metrics.sustainability_score
            )
        
        plan = {
            'expert_id': self.expert_id,
            'optimization_id': optimization_id,
            'version': self.version,
            'energy_source': energy_source.value,
            'renewable_percentage': 100 if energy_source.is_renewable else 25,
            'power_state': power_state.value,
            'frequency_percent': power_config['frequency_percent'],
            'quantization': quant_level,
            'accuracy_impact': quant_config['accuracy_impact'],
            'estimated_energy_kwh': estimated_energy,
            'estimated_carbon_kg': estimated_carbon,
            'estimated_ecoatp_cost': ecoatp_cost,
            'estimated_latency_ms': 40.0,
            'carbon_budget_compliant': estimated_carbon <= carbon_budget,
            'bio_integration_active': self.enable_bio_integration,
            'federated_active': self.enable_federated,
            'cross_domain_active': self.enable_cross_domain,
            'predictive_sustainability_active': self.enable_predictive_sustainability,
            'gradient_levels': gradient_levels,
            'ecoatp_generated': ecoatp_generated,
            'sustainability_score': self.sustainability_metrics.sustainability_score,
            'predictive_forecast': predictive_forecast,
            'federated_round': federated_result.get('round', 0) if federated_result else 0,
            'federated_contribution': federated_result.get('contribution_score', 0) if federated_result else 0,
            'harvester_forecast': self._get_harvester_renewable_forecast() if self.enable_bio_integration else {},
            'recommendations': self._generate_recommendations(gradient_levels, ecoatp_generated),
            'strategy': 'bio_optimized' if self.enable_bio_integration else 'standard',
            'timestamp': datetime.utcnow().isoformat()
        }
        
        history = EnergyOptimizationHistory(
            timestamp=start_time,
            strategy=plan['strategy'],
            energy_source=energy_source.value,
            power_state=power_state.value,
            energy_saved_kwh=max(0, base_energy - estimated_energy),
            carbon_saved_kg=max(0, base_energy * 400 / 1000 - estimated_carbon),
            cost_saved=0.0,
            renewable_used=energy_source.is_renewable,
            success=True,
            ecoatp_generated=ecoatp_generated,
            gradient_level=gradient_levels.get('carbon', 0.5),
            federated_round=plan['federated_round'],
            cross_domain_transfers=list(self.cross_domain_transfer.transfer_logs)[-5:] if self.enable_cross_domain else []
        )
        self.optimization_history.append(history)
        self.total_energy_saved_kwh += history.energy_saved_kwh
        self.total_carbon_saved_kg += history.carbon_saved_kg
        self.total_ecoatp_generated += ecoatp_generated
        self.total_helium_saved += helium_scarcity * 0.01
        
        return plan
    
    def _generate_recommendations(self, gradient_levels: Dict[str, float], ecoatp_generated: float) -> List[str]:
        recs = []
        carbon = gradient_levels.get('carbon', 0.5)
        if carbon > 0.7:
            recs.append(f"High carbon gradient ({carbon:.2f}) - using stored energy.")
        elif carbon < 0.3:
            recs.append(f"Low carbon gradient ({carbon:.2f}) - grid energy is clean.")
        
        if ecoatp_generated > 0:
            recs.append(f"Generated {ecoatp_generated:.1f} Eco-ATP from carbon offsets.")
        
        if self.enable_federated:
            federated_insights = self.federated_learner.get_federated_insights()
            if federated_insights.get('participants', 0) > 1:
                recs.append(f"Federated learning active with {federated_insights['participants']} participants.")
        
        if self.enable_cross_domain:
            transfer_stats = self.cross_domain_transfer.get_transfer_statistics()
            if transfer_stats.get('total_transfers', 0) > 0:
                recs.append(f"Cross-domain knowledge transferred: {transfer_stats['total_transfers']} transfers.")
        
        if self.enable_predictive_sustainability:
            summary = self.predictive_sustainability.get_sustainability_summary()
            if summary.get('status') != 'insufficient_data':
                recs.append(f"Sustainability trend: {summary.get('trend', 'stable')}")
        
        return recs if recs else ["Energy configuration is optimal."]
    
    # ========================================================================
    # Natural Language Explanations with Enhanced Features
    # ========================================================================
    
    def explain_energy_decision(self, optimization_result: Dict[str, Any]) -> Dict[str, Any]:
        plan = optimization_result
        energy_source = plan.get('energy_source', 'unknown')
        power_state = plan.get('power_state', 'unknown')
        quantization = plan.get('quantization', 'unknown')
        carbon_kg = plan.get('estimated_carbon_kg', 0)
        ecoatp_generated = plan.get('ecoatp_generated', 0)
        sustainability_score = plan.get('sustainability_score', 0.5)
        federated_round = plan.get('federated_round', 0)
        
        if plan.get('bio_integration_active'):
            if energy_source in ['solar', 'wind', 'battery']:
                executive = f"Selected {energy_source} energy with {power_state} power and {quantization} quantization. Carbon: {carbon_kg:.6f}kg."
            else:
                executive = f"Using {energy_source} energy (gradient-driven). Generated {ecoatp_generated:.1f} Eco-ATP from offsets."
        else:
            executive = f"Standard optimization: {power_state} power, {quantization} precision. Carbon: {carbon_kg:.6f}kg."
        
        technical = [
            f"Energy source: {energy_source}",
            f"Power state: {power_state}",
            f"Quantization: {quantization}",
            f"Estimated carbon: {carbon_kg:.6f} kg CO2",
            f"Estimated energy: {plan.get('estimated_energy_kwh', 0):.6f} kWh",
            f"Eco-ATP cost: {plan.get('estimated_ecoatp_cost', 0):.1f}",
            f"Eco-ATP generated: {ecoatp_generated:.1f}",
            f"Sustainability score: {sustainability_score:.2f}"
        ]
        
        if plan.get('bio_integration_active'):
            gradients = plan.get('gradient_levels', {})
            technical.append(f"Carbon gradient: {gradients.get('carbon', 0):.2f}")
        
        if federated_round > 0:
            technical.append(f"Federated learning round: {federated_round}")
        
        if plan.get('predictive_forecast'):
            forecast = plan['predictive_forecast']
            technical.append(f"Predicted energy: {forecast.get('predicted_energy', 0):.3f} kWh")
            technical.append(f"Forecast confidence: {forecast.get('confidence', 0):.2f}")
        
        if energy_source == 'battery':
            counterfactual = "If carbon gradient were below 0.3, grid energy would be preferred."
        elif quantization == 'int8':
            counterfactual = "If token balance were above 500, fp16 precision would be used for 1% accuracy improvement."
        else:
            counterfactual = "If carbon budget were tighter, quantization would downgrade to int4 for 50% energy reduction."
        
        if sustainability_score < 0.5:
            counterfactual += " Sustainability score is low. Consider increasing renewable energy usage."
        
        confidence = 0.85
        if plan.get('bio_integration_active'):
            harvester_conf = plan.get('harvester_forecast', {}).get('confidence', 0.5)
            confidence = 0.7 + harvester_conf * 0.2
        
        return {
            'decision_type': 'energy_optimization',
            'executive_summary': executive,
            'technical_details': technical,
            'counterfactual': counterfactual,
            'confidence': confidence,
            'federated_round': federated_round,
            'sustainability_score': sustainability_score,
            'timestamp': datetime.utcnow().isoformat()
        }
    
    def explain_renewable_forecast(self) -> Dict[str, Any]:
        if not self.enable_bio_integration:
            return {'error': 'Bio-integration not enabled'}
        
        forecast = self._get_harvester_renewable_forecast()
        gradient_levels = self._get_gradient_levels()
        confidence = forecast.get('confidence', 0.5)
        solar = forecast.get('solar_kw', 0)
        wind = forecast.get('wind_kw', 0)
        total = forecast.get('total_renewable_kw', 0)
        
        if confidence > 0.7 and total > 10:
            outlook = "EXCELLENT: Strong renewable energy expected."
        elif confidence > 0.5 and total > 5:
            outlook = "GOOD: Adequate renewable energy available."
        elif confidence > 0.3:
            outlook = "FAIR: Limited renewable energy."
        else:
            outlook = "POOR: Minimal renewable energy."
        
        explanation = {
            'outlook': outlook,
            'forecast': {
                'solar_kw': f"{solar:.1f}",
                'wind_kw': f"{wind:.1f}",
                'total_kw': f"{total:.1f}",
                'confidence': f"{confidence:.0%}"
            },
            'gradient_context': {
                'carbon': f"{gradient_levels.get('carbon', 0):.2f}",
                'helium': f"{gradient_levels.get('helium', 0):.2f}"
            },
            'federated_context': {
                'sharing_ratio': f"{self.federated_learner.state.energy_sharing_ratio:.2f}" if self.enable_federated else "N/A"
            },
            'recommendations': []
        }
        
        if solar > wind and solar > 3:
            explanation['recommendations'].append("Solar dominant. Schedule tasks during daylight.")
        if wind > solar and wind > 3:
            explanation['recommendations'].append("Wind dominant. Night-time processing optimal.")
        if confidence < 0.4:
            explanation['recommendations'].append("Low confidence. Maintain battery reserves above 50%.")
        if total < 5:
            explanation['recommendations'].append("Insufficient renewable. Activate conservation measures.")
        if self.enable_federated and self.federated_learner.state.energy_sharing_ratio > 0.5:
            explanation['recommendations'].append("Federated energy sharing active. Optimize distribution.")
        
        return explanation
    
    def get_decision_explanation(self, optimization_id: str) -> Optional[Dict[str, Any]]:
        for entry in self.optimization_history:
            plan = entry.plan if hasattr(entry, 'plan') else entry.get('plan', {})
            if plan.get('optimization_id') == optimization_id:
                return self.explain_energy_decision(plan)
        return None
    
    async def suggest_carbon_offset(self, carbon_impact: float, energy_source_plan: Optional[Dict[str, Any]] = None,
                                   renewable_profile: Optional[RenewableProfile] = None) -> Dict[str, Any]:
        strategies = [
            {'type': 'helium_offset', 'amount_kg': carbon_impact * 0.3, 'cost_per_kg': 0.05, 'sustainability_score': 0.7},
            {'type': 'renewable_certificates', 'amount_kg': carbon_impact * 0.5, 'cost_per_kg': 0.02, 'sustainability_score': 0.9},
            {'type': 'direct_air_capture', 'amount_kg': carbon_impact * 0.4, 'cost_per_kg': 0.15, 'sustainability_score': 0.5},
            {'type': 'reforestation', 'amount_kg': carbon_impact * 0.35, 'cost_per_kg': 0.01, 'sustainability_score': 0.85}
        ]
        
        # Apply federated learning insights if available
        if self.enable_federated:
            federated_insights = self.federated_learner.get_federated_insights()
            if federated_insights.get('participants', 0) > 1:
                for strategy in strategies:
                    strategy['sustainability_score'] *= (1 + 0.05 * federated_insights['participants'])
        
        best = min(strategies, key=lambda s: s['cost_per_kg'] * s['amount_kg'] / s['sustainability_score'])
        ecoatp_generated = self._generate_offset_tokens(carbon_impact) if self.enable_bio_integration else 0.0
        
        return {
            'carbon_impact_kg': carbon_impact,
            'strategies': strategies,
            'recommended_strategy': best,
            'ecoatp_generated': ecoatp_generated,
            'bio_integration_active': self.enable_bio_integration,
            'federated_active': self.enable_federated,
            'federated_participants': len(self.federated_learner.state.participants) if self.enable_federated else 0
        }
    
    # ========================================================================
    # Expert Statistics with Enhanced Features
    # ========================================================================
    
    def get_expert_statistics(self) -> Dict[str, Any]:
        recent = list(self.optimization_history)[-100:]
        stats = {
            'expert_id': self.expert_id,
            'version': self.version,
            'total_energy_saved_kwh': self.total_energy_saved_kwh,
            'total_carbon_saved_kg': self.total_carbon_saved_kg,
            'total_ecoatp_generated': self.total_ecoatp_generated,
            'total_helium_saved_l': self.total_helium_saved,
            'bio_integration_active': self.enable_bio_integration,
            'federated_active': self.enable_federated,
            'cross_domain_active': self.enable_cross_domain,
            'predictive_sustainability_active': self.enable_predictive_sustainability,
            'optimizations_performed': len(self.optimization_history),
            'renewable_usage_rate': sum(1 for r in recent if r.renewable_used) / max(len(recent), 1) if recent else 0,
            'average_ecoatp_generated': np.mean([r.ecoatp_generated for r in recent]) if recent else 0,
            'current_sustainability_score': self.sustainability_metrics.sustainability_score,
            'sustainability_metrics': self.sustainability_metrics.__dict__
        }
        
        if self.enable_federated:
            stats['federated_insights'] = self.federated_learner.get_federated_insights()
        
        if self.enable_cross_domain:
            stats['cross_domain_stats'] = self.cross_domain_transfer.get_transfer_statistics()
        
        if self.enable_predictive_sustainability:
            stats['sustainability_summary'] = self.predictive_sustainability.get_sustainability_summary()
        
        if self.enable_bio_integration:
            stats['bio_metrics'] = {
                'gradient_levels': self._get_gradient_levels(),
                'harvester_forecast': self._get_harvester_renewable_forecast(),
                'atp_power_state': self._get_atp_driven_dvfs().value,
                'token_quantization': self._get_token_efficient_quantization()
            }
        
        return stats
    
    def reset_metrics(self):
        self.optimization_history.clear()
        self.total_energy_saved_kwh = 0.0
        self.total_carbon_saved_kg = 0.0
        self.total_cost_saved = 0.0
        self.total_ecoatp_generated = 0.0
        self.total_helium_saved = 0.0
        self.sustainability_metrics = SustainabilityMetrics()
        self.federated_learner.state = FederatedLearningState()
    
    # ========================================================================
    # Async Shutdown
    # ========================================================================
    
    async def shutdown(self):
        """Graceful shutdown of all components"""
        logger.info(f"Shutting down Energy Expert {self.expert_id}")
        await self.federated_learner.close()
        logger.info("Energy Expert shutdown complete")
