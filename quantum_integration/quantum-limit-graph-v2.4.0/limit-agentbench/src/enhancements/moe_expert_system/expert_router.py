# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/moe_expert_system/expert_router.py
"""
Enhanced Expert Router v7.0.0 - Complete Signal Transduction Cascade with Causal Constraints
With Federated Learning, Predictive Analytics, Carbon/Helium Optimization,
What-If Analysis, Causal Inference, Natural Language Explanations,
and Production-Grade Causal Constraint Modeling
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple, Set, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import numpy as np
from collections import defaultdict, deque
import time
import hashlib
import json
import math
import uuid
import aiohttp
import os
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.metrics import r2_score, mean_squared_error
import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import DataLoader, TensorDataset
import networkx as nx
from typing import TypeVar, Generic

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
# Helium Efficiency Optimization Module
# ============================================================================

class HeliumEfficiencyOptimizer:
    """Optimize helium allocation across experts and routing"""
    
    def __init__(self, helium_budget_l: float = 100.0):
        self.helium_budget_l = helium_budget_l
        self.helium_usage: Dict[str, float] = defaultdict(float)
        self.helium_allocation: Dict[str, float] = defaultdict(float)
        self.helium_efficiency_scores: Dict[str, float] = defaultdict(lambda: 0.5)
        self._lock = asyncio.Lock()
        self.optimization_history = deque(maxlen=1000)
        
        logger.info(f"Helium Efficiency Optimizer initialized: budget={helium_budget_l}L")
    
    def record_helium_usage(self, expert_id: str, amount_l: float):
        """Record helium usage for an expert"""
        self.helium_usage[expert_id] += amount_l
    
    def set_helium_allocation(self, expert_id: str, amount_l: float):
        """Set helium allocation for an expert"""
        self.helium_allocation[expert_id] = amount_l
    
    def update_efficiency_score(self, expert_id: str, score: float):
        """Update helium efficiency score for an expert"""
        self.helium_efficiency_scores[expert_id] = score
    
    async def optimize_helium_allocation(self, expert_requirements: Dict[str, float]) -> Dict[str, float]:
        async with self._lock:
            total_required = sum(expert_requirements.values())
            if total_required <= self.helium_budget_l:
                return expert_requirements
            
            optimized = {}
            total_efficiency = sum(self.helium_efficiency_scores.get(eid, 0.5) for eid in expert_requirements)
            
            if total_efficiency == 0:
                ratio = self.helium_budget_l / total_required
                for expert_id, required in expert_requirements.items():
                    optimized[expert_id] = required * ratio
            else:
                for expert_id, required in expert_requirements.items():
                    efficiency_weight = self.helium_efficiency_scores.get(expert_id, 0.5) / total_efficiency
                    optimized[expert_id] = self.helium_budget_l * efficiency_weight
            
            self.optimization_history.append({
                'timestamp': datetime.utcnow().isoformat(),
                'total_required': total_required,
                'total_allocated': self.helium_budget_l,
                'allocations': optimized
            })
            
            return optimized
    
    def get_helium_status(self) -> Dict[str, Any]:
        total_usage = sum(self.helium_usage.values())
        total_allocated = sum(self.helium_allocation.values())
        return {
            'budget_l': self.helium_budget_l,
            'total_usage_l': total_usage,
            'total_allocated_l': total_allocated,
            'remaining_budget_l': self.helium_budget_l - total_usage,
            'expert_usage': dict(self.helium_usage),
            'expert_allocation': dict(self.helium_allocation),
            'efficiency_scores': dict(self.helium_efficiency_scores),
            'optimization_count': len(self.optimization_history)
        }

# ============================================================================
# Federated Routing Learner Module
# ============================================================================

class FederatedRoutingLearner:
    """Federated reflexive learning for routing decisions"""
    
    def __init__(self, server_url: Optional[str] = None):
        self.server_url = server_url
        self.round = 0
        self.local_model = None
        self.global_model = None
        self.participants = []
        self.contribution_scores = {}
        self._lock = asyncio.Lock()
        self._session = None
        self.routing_history = deque(maxlen=10000)
        self._init_routing_model()
        logger.info("Federated Routing Learner initialized")
    
    def _init_routing_model(self):
        class RoutingModel(nn.Module):
            def __init__(self, input_size=10, hidden_size=64):
                super().__init__()
                self.network = nn.Sequential(
                    nn.Linear(input_size, hidden_size),
                    nn.ReLU(),
                    nn.BatchNorm1d(hidden_size),
                    nn.Linear(hidden_size, hidden_size // 2),
                    nn.ReLU(),
                    nn.BatchNorm1d(hidden_size // 2),
                    nn.Linear(hidden_size // 2, 5)
                )
            def forward(self, x):
                return self.network(x)
        
        self.local_model = RoutingModel()
        self.global_model = RoutingModel()
    
    async def _get_session(self):
        if self._session is None and self.server_url:
            self._session = aiohttp.ClientSession()
        return self._session
    
    async def train_local_model(self, routing_data: List[Dict], epochs: int = 10) -> float:
        if not routing_data:
            return 0.0
        
        X, y = [], []
        for item in routing_data:
            X.append([
                item.get('carbon_zone', 0) / 10,
                item.get('helium_scarcity', 0.5),
                item.get('task_complexity', 0.5),
                item.get('token_balance', 500) / 1000,
                item.get('carbon_gradient', 0.5),
                item.get('trust_gradient', 0.5),
                item.get('opportunity_gradient', 0.5),
                item.get('stress_level', 0.5),
                item.get('latency_budget', 100) / 1000,
                item.get('energy_budget', 100) / 1000
            ])
            selected = [0] * 5
            expert_idx = item.get('selected_expert_idx', 0)
            if expert_idx < 5:
                selected[expert_idx] = 1
            y.append(selected)
        
        X = torch.FloatTensor(X)
        y = torch.FloatTensor(y)
        
        dataset = TensorDataset(X, y)
        dataloader = DataLoader(dataset, batch_size=32, shuffle=True)
        optimizer = optim.Adam(self.local_model.parameters(), lr=0.001)
        criterion = nn.CrossEntropyLoss()
        
        total_loss = 0
        for epoch in range(epochs):
            epoch_loss = 0
            for batch_X, batch_y in dataloader:
                optimizer.zero_grad()
                output = self.local_model(batch_X)
                loss = criterion(output, torch.argmax(batch_y, dim=1))
                loss.backward()
                torch.nn.utils.clip_grad_norm_(self.local_model.parameters(), 1.0)
                optimizer.step()
                epoch_loss += loss.item()
            total_loss += epoch_loss
        
        avg_loss = total_loss / epochs
        logger.info(f"Local routing model trained. Loss: {avg_loss:.4f}")
        return avg_loss
    
    async def send_local_update(self, performance_metric: float = 1.0) -> Dict:
        if not self.server_url:
            return {'status': 'disabled'}
        
        async with self._lock:
            session = await self._get_session()
            try:
                weights = self.local_model.state_dict()
                weights_serialized = {k: v.tolist() for k, v in weights.items()}
                update_data = {
                    'router_id': 'expert_router',
                    'round': self.round,
                    'weights': weights_serialized,
                    'performance': performance_metric,
                    'timestamp': datetime.utcnow().isoformat()
                }
                async with session.post(
                    f"{self.server_url}/federated/routing/update",
                    json=update_data,
                    timeout=30
                ) as response:
                    if response.status == 200:
                        result = await response.json()
                        self.round += 1
                        self.contribution_scores['router'] = performance_metric
                        return result
                    return {'status': 'failed'}
            except Exception as e:
                logger.error(f"Federated update error: {e}")
                return {'status': 'error'}
    
    async def get_global_model(self) -> Optional[Dict]:
        if not self.server_url:
            return None
        
        async with self._lock:
            session = await self._get_session()
            try:
                async with session.get(
                    f"{self.server_url}/federated/routing/global",
                    timeout=30
                ) as response:
                    if response.status == 200:
                        data = await response.json()
                        weights = data.get('weights', {})
                        self.round = data.get('round', 0)
                        self.participants = data.get('participants', [])
                        for k, v in weights.items():
                            self.global_model.state_dict()[k] = torch.FloatTensor(v)
                        return weights
            except Exception as e:
                logger.error(f"Global model fetch error: {e}")
                return None
    
    async def participate_in_round(self, routing_data: List[Dict], performance: float = 1.0) -> Dict:
        await self.train_local_model(routing_data)
        result = await self.send_local_update(performance)
        global_weights = await self.get_global_model()
        if global_weights:
            self.global_model.load_state_dict(global_weights)
            if 'router' not in self.participants:
                self.participants.append('router')
        return {
            'round': self.round,
            'participated': bool(global_weights),
            'contribution_score': self.contribution_scores.get('router', 0),
            'performance': performance,
            'peer_count': len(self.participants),
            'timestamp': datetime.utcnow().isoformat()
        }
    
    def get_federated_insights(self) -> Dict:
        return {
            'round': self.round,
            'contribution_score': self.contribution_scores.get('router', 0),
            'participants': len(self.participants),
            'has_global_model': bool(self.global_model),
            'local_model_trained': self.local_model is not None
        }
    
    async def close(self):
        if self._session:
            await self._session.close()

# ============================================================================
# Predictive Routing Analytics Module
# ============================================================================

class PredictiveRoutingAnalyzer:
    """Predictive reflexivity with ensemble forecasting for routing performance"""
    
    def __init__(self, history_window: int = 100):
        self.history_window = history_window
        self.routing_history = deque(maxlen=history_window)
        self.forecast_history = deque(maxlen=50)
        self.models = {}
        self.scaler = StandardScaler()
        self.is_trained = False
        self.models['random_forest'] = RandomForestRegressor(n_estimators=100, random_state=42)
        self.models['gradient_boosting'] = GradientBoostingRegressor(n_estimators=100, random_state=42)
    
    def update_history(self, routing_metrics: Dict):
        self.routing_history.append({
            'timestamp': datetime.utcnow(),
            'success_rate': routing_metrics.get('success_rate', 0.8),
            'avg_latency_ms': routing_metrics.get('avg_latency_ms', 100),
            'carbon_efficiency': routing_metrics.get('carbon_efficiency', 0.5),
            'helium_efficiency': routing_metrics.get('helium_efficiency', 0.5),
            'expert_utilization': routing_metrics.get('expert_utilization', 0.5)
        })
    
    async def train_forecast_model(self):
        if len(self.routing_history) < 10:
            return {'status': 'insufficient_data', 'samples': len(self.routing_history)}
        
        X, y = [], []
        history_list = list(self.routing_history)
        for i in range(len(history_list) - 5):
            features = []
            for j in range(5):
                data = history_list[i + j]
                features.extend([
                    data['success_rate'],
                    data['avg_latency_ms'] / 1000,
                    data['carbon_efficiency'],
                    data['helium_efficiency'],
                    data['expert_utilization']
                ])
            X.append(features)
            y.append(history_list[i + 5]['success_rate'])
        
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
        logger.info(f"Routing forecast models trained. R²: {results}")
        return {'status': 'success', 'results': results, 'samples': len(X)}
    
    async def predict_routing_performance(self, hours: int = 24) -> Dict:
        if not self.is_trained or len(self.routing_history) < 10:
            return {'predicted_success_rate': 0.5, 'confidence': 0.0, 'trend': 'insufficient_data'}
        
        recent = list(self.routing_history)[-5:]
        features = []
        for data in recent:
            features.extend([
                data['success_rate'],
                data['avg_latency_ms'] / 1000,
                data['carbon_efficiency'],
                data['helium_efficiency'],
                data['expert_utilization']
            ])
        
        features = np.array(features).reshape(1, -1)
        features_scaled = self.scaler.transform(features)
        
        predictions = []
        for name, model in self.models.items():
            if model is not None:
                pred = model.predict(features_scaled)[0]
                predictions.append(pred)
        
        if not predictions:
            return {'predicted_success_rate': 0.5, 'confidence': 0.0, 'trend': 'no_models'}
        
        prediction = np.mean(predictions)
        confidence = min(0.9, np.std(predictions) / 0.2) if len(predictions) > 1 else 0.5
        
        if len(self.forecast_history) > 5:
            recent_forecasts = list(self.forecast_history)[-5:]
            trend = "improving" if prediction > recent_forecasts[-1] else "declining" if prediction < recent_forecasts[-1] else "stable"
        else:
            trend = "stable"
        
        self.forecast_history.append({'prediction': prediction, 'trend': trend})
        return {
            'predicted_success_rate': prediction,
            'confidence': confidence,
            'trend': trend,
            'recommended_actions': self._generate_predictive_actions(prediction)
        }
    
    def _generate_predictive_actions(self, prediction: float) -> List[str]:
        actions = []
        if prediction < 0.5:
            actions.append("Optimize expert selection criteria")
            actions.append("Increase carbon budget allocation")
        elif prediction < 0.7:
            actions.append("Enhance signal transduction sensitivity")
            actions.append("Improve allosteric regulation")
        else:
            actions.append("Maintain current routing configuration")
        return actions

# ============================================================================
# CAUSAL CONSTRAINT MODELING MODULE (NEW)
# ============================================================================

class CausalConstraintModel:
    """
    Causal constraint modeling for cross-domain reasoning.
    
    Features:
    - Causal relationship detection and graph construction
    - Constraint propagation across domains
    - Impact prediction with confidence scoring
    - Trade-off analysis with Pareto optimization
    - Counterfactual reasoning
    """
    
    def __init__(self):
        self.causal_graph = nx.DiGraph()
        self.constraints = {}
        self.impact_history = deque(maxlen=1000)
        self.causal_strengths = {}
        self._lock = asyncio.Lock()
        
        # Initialize causal relationships
        self._init_causal_relationships()
        
        # Domain mapping for cross-domain reasoning
        self.domain_mapping = {
            'carbon': ['energy', 'helium', 'biodiversity'],
            'helium': ['quantum', 'cooling', 'energy'],
            'energy': ['carbon', 'helium', 'latency'],
            'quantum': ['helium', 'energy', 'accuracy'],
            'biodiversity': ['carbon', 'land_use'],
            'latency': ['energy', 'performance'],
            'accuracy': ['quantum', 'performance']
        }
        
        # Constraint thresholds
        self.constraint_thresholds = {
            'carbon': {'max_per_inference': 0.001, 'min_zone': 0},
            'helium': {'max_usage_per_inference': 0.01, 'min_availability': 0.2},
            'energy': {'max_per_inference': 0.01, 'min_efficiency': 0.5},
            'quantum': {'min_qubits': 10, 'max_depth': 100},
            'biodiversity': {'min_impact_score': 0.3}
        }
        
        logger.info("Causal Constraint Model initialized")
    
    def _init_causal_relationships(self):
        """Initialize known causal relationships across domains"""
        # Carbon domain
        self.causal_graph.add_edge('carbon', 'energy', weight=0.7)
        self.causal_graph.add_edge('carbon', 'helium', weight=0.5)
        self.causal_graph.add_edge('carbon', 'biodiversity', weight=0.6)
        
        # Helium domain
        self.causal_graph.add_edge('helium', 'quantum', weight=0.8)
        self.causal_graph.add_edge('helium', 'cooling', weight=0.6)
        self.causal_graph.add_edge('helium', 'energy', weight=0.4)
        
        # Energy domain
        self.causal_graph.add_edge('energy', 'carbon', weight=0.7)
        self.causal_graph.add_edge('energy', 'helium', weight=0.3)
        self.causal_graph.add_edge('energy', 'latency', weight=0.5)
        
        # Quantum domain
        self.causal_graph.add_edge('quantum', 'helium', weight=0.9)
        self.causal_graph.add_edge('quantum', 'energy', weight=0.6)
        self.causal_graph.add_edge('quantum', 'accuracy', weight=0.8)
        
        # Store causal strengths
        for u, v, data in self.causal_graph.edges(data=True):
            self.causal_strengths[(u, v)] = data.get('weight', 0.5)
    
    def add_causal_relationship(self, source: str, target: str, strength: float = 0.5):
        """Add a causal relationship between domains"""
        with self._lock:
            self.causal_graph.add_edge(source, target, weight=strength)
            self.causal_strengths[(source, target)] = strength
            logger.info(f"Added causal relationship: {source} → {target} (strength={strength:.2f})")
    
    async def propagate_constraints(
        self,
        source_domain: str,
        value: float,
        constraints: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Propagate constraints from one domain to its effects.
        
        Args:
            source_domain: Source domain name
            value: Current value in source domain
            constraints: Constraints to propagate
            
        Returns:
            Propagated constraints with impacts
        """
        async with self._lock:
            propagated = constraints.copy()
            
            if source_domain not in self.domain_mapping:
                return propagated
            
            effects = self.domain_mapping.get(source_domain, [])
            for effect in effects:
                if effect not in propagated:
                    propagated[effect] = {}
                
                # Calculate impact based on causal strength
                strength = self.causal_strengths.get((source_domain, effect), 0.5)
                impact = strength * value
                propagated[effect]['causal_impact'] = impact
                propagated[effect]['causal_strength'] = strength
                propagated[effect]['source'] = source_domain
                
                # Apply domain-specific transformations
                if effect == 'energy':
                    propagated[effect]['expected_change'] = impact * value * 0.1
                elif effect == 'helium':
                    propagated[effect]['expected_change'] = impact * value * 0.2
                elif effect == 'carbon':
                    propagated[effect]['expected_change'] = impact * value * 0.15
                elif effect == 'quantum':
                    propagated[effect]['feasibility'] = 1.0 if value < 0.8 else 0.5
                elif effect == 'biodiversity':
                    propagated[effect]['impact_score'] = min(1.0, impact * 0.5)
                
                # Apply threshold constraints
                if effect in self.constraint_thresholds:
                    threshold = self.constraint_thresholds[effect]
                    for key, limit in threshold.items():
                        if key in propagated[effect]:
                            propagated[effect][f'{key}_limit'] = limit
                            propagated[effect][f'{key}_compliant'] = propagated[effect][key] <= limit
            
            self.impact_history.append({
                'timestamp': datetime.utcnow().isoformat(),
                'source': source_domain,
                'value': value,
                'propagated': propagated
            })
            
            return propagated
    
    async def analyze_tradeoffs(
        self,
        scenarios: List[Dict[str, Any]],
        weights: Dict[str, float] = None
    ) -> List[Dict[str, Any]]:
        """
        Analyze trade-offs between different scenarios.
        
        Args:
            scenarios: List of scenario configurations
            weights: Weight for each domain in sustainability scoring
            
        Returns:
            Trade-off analysis with recommendations
        """
        async with self._lock:
            if weights is None:
                weights = {
                    'carbon': 0.25,
                    'helium': 0.20,
                    'energy': 0.15,
                    'quantum': 0.15,
                    'biodiversity': 0.15,
                    'latency': 0.10
                }
            
            results = []
            for scenario in scenarios:
                impacts = {}
                sustainability_score = 0.0
                risk_factors = []
                
                for domain, value in scenario.items():
                    if domain in self.domain_mapping:
                        propagated = await self.propagate_constraints(
                            domain, value, scenario
                        )
                        impacts[domain] = propagated
                        
                        # Calculate domain score
                        domain_score = 1.0 - min(1.0, value)
                        sustainability_score += domain_score * weights.get(domain, 0.1)
                
                # Identify risk factors
                for domain, impact_data in impacts.items():
                    if 'causal_impact' in impact_data:
                        if impact_data['causal_impact'] > 0.7:
                            risk_factors.append(f"{domain} has high causal impact")
                
                results.append({
                    'scenario': scenario,
                    'impacts': impacts,
                    'sustainability_score': min(1.0, sustainability_score),
                    'risk_factors': risk_factors,
                    'recommendations': self._generate_tradeoff_recommendations(impacts, risk_factors)
                })
            
            # Sort by sustainability score
            results.sort(key=lambda x: x['sustainability_score'], reverse=True)
            
            return results
    
    def _generate_tradeoff_recommendations(
        self,
        impacts: Dict,
        risk_factors: List[str]
    ) -> List[str]:
        """Generate recommendations based on trade-off analysis"""
        recommendations = []
        
        for domain, impact in impacts.items():
            if 'causal_impact' in impact and impact['causal_impact'] > 0.6:
                if domain == 'carbon':
                    recommendations.append("Carbon impact high - consider carbon offset or reduction")
                elif domain == 'helium':
                    recommendations.append("Helium impact high - optimize helium usage")
                elif domain == 'energy':
                    recommendations.append("Energy impact high - improve energy efficiency")
        
        if risk_factors:
            recommendations.append(f"Monitor these risk factors: {', '.join(risk_factors[:3])}")
        
        return recommendations or ["No critical trade-offs identified"]
    
    async def get_causal_path(
        self,
        source: str,
        target: str
    ) -> List[Tuple[str, str, float]]:
        """
        Get the causal path between two domains.
        
        Returns:
            List of (source, target, strength) tuples
        """
        async with self._lock:
            if source not in self.causal_graph or target not in self.causal_graph:
                return []
            
            try:
                path = nx.shortest_path(self.causal_graph, source, target)
                path_edges = []
                for i in range(len(path) - 1):
                    u, v = path[i], path[i + 1]
                    strength = self.causal_strengths.get((u, v), 0.5)
                    path_edges.append((u, v, strength))
                return path_edges
            except nx.NetworkXNoPath:
                return []
    
    async def get_causal_strength(self, source: str, target: str) -> float:
        """Get causal strength between two domains"""
        return self.causal_strengths.get((source, target), 0.0)
    
    def get_causal_graph_summary(self) -> Dict[str, Any]:
        """Get summary of causal graph"""
        return {
            'nodes': list(self.causal_graph.nodes()),
            'edges': list(self.causal_graph.edges()),
            'edge_count': len(self.causal_graph.edges()),
            'node_count': len(self.causal_graph.nodes()),
            'causal_strengths': self.causal_strengths,
            'recent_impacts': list(self.impact_history)[-10:]
        }

# ============================================================================
# Enums and Data Classes (Preserved)
# ============================================================================

class SignalType(Enum):
    ENDOCRINE = "endocrine"
    PARACRINE = "paracrine"
    AUTOCRINE = "autocrine"
    JUXTACRINE = "juxtacrine"
    NEUROTRANSMITTER = "neurotransmitter"
    NEUROMODULATOR = "neuromodulator"

class SecondMessenger(Enum):
    cAMP = "camp"
    cGMP = "cgmp"
    IP3 = "ip3"
    DAG = "dag"
    CALCIUM = "calcium"
    NITRIC_OXIDE = "nitric_oxide"

class ReceptorState(Enum):
    INACTIVE = "inactive"
    BOUND = "bound"
    ACTIVATED = "activated"
    DESENSITIZED = "desensitized"
    INTERNALIZED = "internalized"
    RESENSITIZED = "resensitized"

class AmplificationLevel(Enum):
    NONE = 0
    LOW = 1
    MODERATE = 2
    HIGH = 3
    MAXIMUM = 4

class CircuitBreakerState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

@dataclass
class SignalReceptor:
    receptor_id: str
    signal_type: SignalType
    ligand: str
    affinity: float = 0.5
    state: ReceptorState = ReceptorState.INACTIVE
    bound_ligands: int = 0
    desensitization_time: float = 0.0
    resensitization_rate: float = 0.1
    amplification: AmplificationLevel = AmplificationLevel.MODERATE
    downstream_effectors: List[str] = field(default_factory=list)
    last_activated: Optional[datetime] = None
    activation_count: int = 0

@dataclass
class SecondMessengerSystem:
    messenger_type: SecondMessenger
    concentration: float = 0.0
    baseline: float = 0.1
    threshold: float = 0.3
    max_concentration: float = 1.0
    synthesis_rate: float = 0.1
    degradation_rate: float = 0.05
    amplification_factor: float = 100.0
    target_proteins: List[str] = field(default_factory=list)
    half_life_seconds: float = 5.0

@dataclass
class AllostericSite:
    site_id: str
    modulator: str
    effect: str = "modulation"
    binding_affinity: float = 0.5
    current_occupancy: float = 0.0
    conformational_change: float = 0.0

@dataclass
class MetabolicPathway:
    pathway_id: str
    input_substrate: str
    enzymes: List[str]
    intermediates: List[str]
    final_product: str
    rate_limiting_step: Optional[str] = None
    allosteric_regulators: List[AllostericSite] = field(default_factory=list)
    energy_cost_ecoatp: float = 10.0
    throughput_rate: float = 1.0
    is_active: bool = True

@dataclass
class RoutingMetrics:
    total_routes: int = 0
    successful_routes: int = 0
    failed_routes: int = 0
    fallback_routes: int = 0
    biomass_stored_routes: int = 0
    average_latency_ms: float = 0.0
    carbon_savings_kg: float = 0.0
    helium_savings_l: float = 0.0
    
    @property
    def success_rate(self) -> float:
        return self.successful_routes / max(self.total_routes, 1)

@dataclass
class ExpertCircuitBreaker:
    expert_id: str
    state: CircuitBreakerState = CircuitBreakerState.CLOSED
    failure_count: int = 0
    success_count: int = 0
    last_failure_time: Optional[datetime] = None
    failure_threshold: int = 5
    recovery_timeout_seconds: int = 30
    half_open_max_requests: int = 3
    half_open_requests: int = 0
    
    def record_success(self):
        self.success_count += 1
        if self.state == CircuitBreakerState.HALF_OPEN:
            self.half_open_requests += 1
            if self.half_open_requests >= self.half_open_max_requests:
                self.state = CircuitBreakerState.CLOSED
                self.failure_count = 0
    
    def record_failure(self):
        self.failure_count += 1
        self.last_failure_time = datetime.utcnow()
        if self.state == CircuitBreakerState.CLOSED and self.failure_count >= self.failure_threshold:
            self.state = CircuitBreakerState.OPEN
        elif self.state == CircuitBreakerState.HALF_OPEN:
            self.state = CircuitBreakerState.OPEN
    
    def can_execute(self) -> bool:
        if self.state == CircuitBreakerState.CLOSED:
            return True
        if self.state == CircuitBreakerState.OPEN:
            if self.last_failure_time:
                elapsed = (datetime.utcnow() - self.last_failure_time).total_seconds()
                if elapsed >= self.recovery_timeout_seconds:
                    self.state = CircuitBreakerState.HALF_OPEN
                    self.half_open_requests = 0
                    return True
            return False
        return True

# ============================================================================
# Signal Transduction Engine (Preserved)
# ============================================================================

class SignalTransductionEngine:
    """Biological signal transduction engine for expert routing"""
    
    def __init__(self):
        self.receptors: Dict[str, SignalReceptor] = {}
        self.second_messengers: Dict[SecondMessenger, SecondMessengerSystem] = {}
        self.amplification_history: deque = deque(maxlen=1000)
        self.crosstalk_matrix: Dict[Tuple[str, str], float] = {}
        self._initialize_signaling_systems()
        asyncio.create_task(self._signal_degradation_loop())
        logger.info("Signal Transduction Engine initialized")
    
    def _initialize_signaling_systems(self):
        self.second_messengers[SecondMessenger.cAMP] = SecondMessengerSystem(
            messenger_type=SecondMessenger.cAMP, baseline=0.1, threshold=0.3,
            synthesis_rate=0.15, degradation_rate=0.08, amplification_factor=100.0,
            half_life_seconds=3.0, target_proteins=['energy_expert', 'routing_kinase']
        )
        self.second_messengers[SecondMessenger.CALCIUM] = SecondMessengerSystem(
            messenger_type=SecondMessenger.CALCIUM, baseline=0.05, threshold=0.2,
            synthesis_rate=0.2, degradation_rate=0.1, amplification_factor=1000.0,
            half_life_seconds=1.0, target_proteins=['all_experts', 'emergency_response']
        )
        self.second_messengers[SecondMessenger.IP3] = SecondMessengerSystem(
            messenger_type=SecondMessenger.IP3, baseline=0.05, threshold=0.25,
            synthesis_rate=0.1, degradation_rate=0.06, amplification_factor=500.0,
            half_life_seconds=4.0, target_proteins=['gradient_effectors', 'compartment_activation']
        )
        self.second_messengers[SecondMessenger.NITRIC_OXIDE] = SecondMessengerSystem(
            messenger_type=SecondMessenger.NITRIC_OXIDE, baseline=0.02, threshold=0.15,
            synthesis_rate=0.12, degradation_rate=0.15, amplification_factor=200.0,
            half_life_seconds=2.0, target_proteins=['neighboring_compartments', 'vascular_signaling']
        )
    
    def create_receptor(self, receptor_id: str, signal_type: SignalType,
                        ligand: str, affinity: float = 0.5,
                        amplification: AmplificationLevel = AmplificationLevel.MODERATE) -> SignalReceptor:
        receptor = SignalReceptor(receptor_id=receptor_id, signal_type=signal_type,
                                  ligand=ligand, affinity=affinity, amplification=amplification)
        self.receptors[receptor_id] = receptor
        return receptor
    
    def bind_ligand(self, receptor_id: str, ligand_concentration: float) -> bool:
        if receptor_id not in self.receptors:
            return False
        receptor = self.receptors[receptor_id]
        if receptor.state == ReceptorState.DESENSITIZED:
            return False
        binding_prob = receptor.affinity * ligand_concentration
        if np.random.random() < binding_prob:
            receptor.state = ReceptorState.BOUND
            receptor.bound_ligands += 1
            receptor.last_activated = datetime.utcnow()
            if receptor.bound_ligands >= 2:
                receptor.state = ReceptorState.ACTIVATED
                receptor.activation_count += 1
                self._activate_cascade(receptor)
                receptor.desensitization_time = 5.0
                receptor.state = ReceptorState.DESENSITIZED
                return True
        return False
    
    def _activate_cascade(self, receptor: SignalReceptor):
        if receptor.ligand in ['carbon_gradient', 'energy_signal']:
            messenger = SecondMessenger.cAMP
        elif receptor.ligand in ['emergency', 'stress_signal']:
            messenger = SecondMessenger.CALCIUM
        elif receptor.ligand in ['gradient_change', 'opportunity']:
            messenger = SecondMessenger.IP3
        else:
            messenger = SecondMessenger.NITRIC_OXIDE
        
        if messenger in self.second_messengers:
            sm = self.second_messengers[messenger]
            amp_factors = {AmplificationLevel.NONE: 1, AmplificationLevel.LOW: 10,
                          AmplificationLevel.MODERATE: 100, AmplificationLevel.HIGH: 1000,
                          AmplificationLevel.MAXIMUM: 10000}
            amp = amp_factors.get(receptor.amplification, 100)
            synthesis = sm.synthesis_rate * amp / 100.0
            sm.concentration = min(sm.max_concentration, sm.concentration + synthesis)
            self.amplification_history.append({
                'receptor': receptor.receptor_id, 'messenger': messenger.value,
                'amplification': amp, 'concentration': sm.concentration,
                'timestamp': datetime.utcnow().isoformat()
            })
    
    def get_second_messenger_level(self, messenger: SecondMessenger) -> float:
        if messenger in self.second_messengers:
            return self.second_messengers[messenger].concentration
        return 0.0
    
    def is_pathway_active(self, messenger: SecondMessenger) -> bool:
        if messenger in self.second_messengers:
            return self.second_messengers[messenger].concentration > self.second_messengers[messenger].threshold
        return False
    
    async def _signal_degradation_loop(self):
        while True:
            try:
                for sm in self.second_messengers.values():
                    sm.concentration = max(0.0, sm.concentration - sm.degradation_rate)
                for receptor in self.receptors.values():
                    if receptor.state == ReceptorState.DESENSITIZED:
                        receptor.desensitization_time -= 1.0
                        if receptor.desensitization_time <= 0:
                            receptor.state = ReceptorState.RESENSITIZED
                            receptor.bound_ligands = 0
                await asyncio.sleep(1.0)
            except Exception as e:
                logger.error(f"Signal degradation error: {str(e)}")
                await asyncio.sleep(5.0)
    
    def setup_crosstalk(self, pathway_a: SecondMessenger, pathway_b: SecondMessenger, strength: float):
        self.crosstalk_matrix[(pathway_a.value, pathway_b.value)] = strength
        self.crosstalk_matrix[(pathway_b.value, pathway_a.value)] = strength * 0.7
    
    def apply_crosstalk(self):
        for (path_a, path_b), strength in self.crosstalk_matrix.items():
            messenger_a = SecondMessenger(path_a)
            messenger_b = SecondMessenger(path_b)
            if messenger_a in self.second_messengers and messenger_b in self.second_messengers:
                sm_a = self.second_messengers[messenger_a]
                sm_b = self.second_messengers[messenger_b]
                if sm_a.concentration > sm_a.threshold:
                    sm_b.concentration = min(sm_b.max_concentration,
                        sm_b.concentration + sm_a.concentration * strength * 0.1)
    
    def get_signaling_status(self) -> Dict[str, Any]:
        return {
            'receptors': {rid: {'state': r.state.value, 'ligand': r.ligand,
                                'activations': r.activation_count}
                         for rid, r in self.receptors.items()},
            'second_messengers': {sm.value: {'concentration': m.concentration,
                                              'active': m.concentration > m.threshold}
                                  for sm, m in self.second_messengers.items()}
        }

# ============================================================================
# Allosteric Regulation System (Preserved)
# ============================================================================

class AllostericRegulationSystem:
    """Allosteric regulation for routing decisions"""
    
    def __init__(self):
        self.allosteric_sites: Dict[str, AllostericSite] = {}
        self.conformational_state: float = 0.5
        self.cooperativity: Dict[Tuple[str, str], float] = {}
        self.regulation_history: deque = deque(maxlen=1000)
        self._initialize_allosteric_sites()
        logger.info("Allosteric Regulation System initialized")
    
    def _initialize_allosteric_sites(self):
        self.allosteric_sites['carbon_site'] = AllostericSite('carbon_site', 'carbon_gradient', 'modulation', 0.7)
        self.allosteric_sites['helium_site'] = AllostericSite('helium_site', 'helium_gradient', 'inhibitory', 0.6)
        self.allosteric_sites['token_site'] = AllostericSite('token_site', 'token_availability', 'activating', 0.8)
        self.allosteric_sites['trust_site'] = AllostericSite('trust_site', 'trust_gradient', 'activating', 0.5)
        self.allosteric_sites['stress_site'] = AllostericSite('stress_site', 'stress_signal', 'inhibitory', 0.9)
    
    def bind_modulator(self, site_id: str, modulator_concentration: float) -> float:
        if site_id not in self.allosteric_sites:
            return 0.0
        site = self.allosteric_sites[site_id]
        n = 2.0
        Kd = 1.0 - site.binding_affinity
        occupancy = (modulator_concentration ** n) / (Kd ** n + modulator_concentration ** n)
        site.current_occupancy = occupancy
        if site.effect == 'activating':
            change = occupancy * 0.2
        elif site.effect == 'inhibitory':
            change = -occupancy * 0.2
        else:
            change = (occupancy - 0.5) * 0.1
        site.conformational_change = change
        self.conformational_state = max(0.0, min(1.0, self.conformational_state + change))
        self.regulation_history.append({
            'site': site_id, 'modulator': site.modulator,
            'concentration': modulator_concentration, 'occupancy': occupancy,
            'new_state': self.conformational_state, 'timestamp': datetime.utcnow().isoformat()
        })
        return change
    
    def get_routing_modulation(self) -> Dict[str, float]:
        state = self.conformational_state
        return {
            'exploration_rate': state * 0.3, 'exploitation_rate': 1.0 - state * 0.3,
            'risk_tolerance': state * 0.5, 'conservation_mode': (1.0 - state) * 0.8,
            'cooperativity_factor': state * 0.4, 'competition_factor': (1.0 - state) * 0.3
        }
    
    def setup_cooperativity(self, expert_a: str, expert_b: str, strength: float):
        self.cooperativity[(expert_a, expert_b)] = strength
        self.cooperativity[(expert_b, expert_a)] = strength
    
    def get_cooperativity_bonus(self, expert_a: str, expert_b: str) -> float:
        return self.cooperativity.get((expert_a, expert_b), 0.0)
    
    def get_regulation_status(self) -> Dict[str, Any]:
        return {
            'conformational_state': self.conformational_state,
            'state_description': 'relaxed' if self.conformational_state > 0.6 else
                                'tense' if self.conformational_state < 0.4 else 'intermediate',
            'routing_modulation': self.get_routing_modulation()
        }

# ============================================================================
# Metabolic Pathway Router (Preserved)
# ============================================================================

class MetabolicPathwayRouter:
    """Routes tasks through optimal metabolic pathways"""
    
    def __init__(self):
        self.pathways: Dict[str, MetabolicPathway] = {}
        self.enzyme_kinetics: Dict[str, Dict[str, float]] = {}
        self.product_levels: Dict[str, float] = defaultdict(float)
        self.throughput_history: deque = deque(maxlen=1000)
        self._initialize_pathways()
        logger.info("Metabolic Pathway Router initialized")
    
    def _initialize_pathways(self):
        self.pathways['energy_optimization'] = MetabolicPathway(
            'energy_optimization', 'optimization_task', ['energy_expert'],
            ['energy_analysis', 'optimization_plan', 'execution_strategy'],
            'optimized_energy_plan', 'optimization_plan', 10.0,
            [AllostericSite('energy_carbon_site', 'carbon_gradient', 'inhibitory', 0.6),
             AllostericSite('energy_token_site', 'token_availability', 'activating', 0.8)]
        )
        self.pathways['data_processing'] = MetabolicPathway(
            'data_processing', 'data_task', ['data_expert'],
            ['data_ingestion', 'transformation', 'analysis', 'output'],
            'processed_data', 'transformation', 8.0,
            [AllostericSite('data_helium_site', 'helium_gradient', 'inhibitory', 0.5),
             AllostericSite('data_trust_site', 'trust_gradient', 'activating', 0.7)]
        )
        self.pathways['edge_computing'] = MetabolicPathway(
            'edge_computing', 'edge_task', ['iot_expert'],
            ['local_processing', 'mesh_routing', 'result_aggregation'],
            'edge_result', 'mesh_routing', 5.0,
            [AllostericSite('edge_opportunity_site', 'opportunity_gradient', 'activating', 0.9)]
        )
        self.pathways['quantum_computing'] = MetabolicPathway(
            'quantum_computing', 'quantum_task', ['quantum_expert'],
            ['circuit_preparation', 'execution', 'error_mitigation', 'measurement'],
            'quantum_result', 'execution', 50.0,
            [AllostericSite('quantum_complexity_site', 'task_complexity', 'activating', 0.4)]
        )
        for pathway in self.pathways.values():
            for enzyme in pathway.enzymes:
                self.enzyme_kinetics[enzyme] = {'Km': 0.5, 'Vmax': 1.0, 'kcat': 10.0, 'specificity': 0.8}
    
    def calculate_reaction_rate(self, enzyme: str, substrate_concentration: float) -> float:
        if enzyme not in self.enzyme_kinetics:
            return 0.0
        kinetics = self.enzyme_kinetics[enzyme]
        return kinetics['Vmax'] * substrate_concentration / (kinetics['Km'] + substrate_concentration)
    
    def apply_competitive_inhibition(self, enzyme: str, inhibitor_concentration: float,
                                     inhibition_constant: float = 0.1) -> float:
        if enzyme not in self.enzyme_kinetics:
            return 1.0
        kinetics = self.enzyme_kinetics[enzyme]
        apparent_Km = kinetics['Km'] * (1 + inhibitor_concentration / inhibition_constant)
        return kinetics['Km'] / apparent_Km
    
    def apply_allosteric_regulation(self, pathway_id: str, modulator_levels: Dict[str, float]) -> float:
        if pathway_id not in self.pathways:
            return 1.0
        pathway = self.pathways[pathway_id]
        throughput_multiplier = 1.0
        for site in pathway.allosteric_regulators:
            if site.modulator in modulator_levels:
                concentration = modulator_levels[site.modulator]
                n = 1.5
                Kd = 1.0 - site.binding_affinity
                occupancy = concentration ** n / (Kd ** n + concentration ** n)
                if site.effect == 'activating':
                    throughput_multiplier *= (1.0 + occupancy * 0.5)
                elif site.effect == 'inhibitory':
                    throughput_multiplier *= (1.0 - occupancy * 0.5)
        return max(0.1, throughput_multiplier)
    
    def select_optimal_pathway(self, task_type: str, substrate_concentration: float,
                               modulator_levels: Dict[str, float], energy_budget: float) -> Tuple[Optional[str], float]:
        candidates = []
        for pathway_id, pathway in self.pathways.items():
            if task_type not in pathway.input_substrate and pathway.input_substrate not in task_type:
                continue
            if not pathway.is_active:
                continue
            total_rate = 0.0
            for enzyme in pathway.enzymes:
                rate = self.calculate_reaction_rate(enzyme, substrate_concentration)
                inhibitor_level = sum(self.product_levels.get(p.final_product, 0)
                                     for p in self.pathways.values() if p.pathway_id != pathway_id)
                inhibition = self.apply_competitive_inhibition(enzyme, inhibitor_level)
                rate *= inhibition
                total_rate += rate
            avg_rate = total_rate / max(len(pathway.enzymes), 1)
            allosteric_multiplier = self.apply_allosteric_regulation(pathway_id, modulator_levels)
            regulated_rate = avg_rate * allosteric_multiplier
            energy_efficiency = regulated_rate / max(pathway.energy_cost_ecoatp, 1)
            if pathway.energy_cost_ecoatp > energy_budget:
                energy_efficiency *= 0.3
            candidates.append((pathway_id, energy_efficiency))
        if not candidates:
            return None, 0.0
        candidates.sort(key=lambda x: x[1], reverse=True)
        return candidates[0]
    
    def record_throughput(self, pathway_id: str, actual_rate: float, energy_used: float):
        self.throughput_history.append({
            'pathway': pathway_id, 'rate': actual_rate, 'energy': energy_used,
            'timestamp': datetime.utcnow().isoformat()
        })
        if pathway_id in self.pathways:
            product = self.pathways[pathway_id].final_product
            self.product_levels[product] += actual_rate * 0.1
    
    def apply_product_inhibition(self):
        for product, level in self.product_levels.items():
            for pathway in self.pathways.values():
                if pathway.final_product == product and level > 5.0:
                    pathway.throughput_rate *= 0.9
                    self.product_levels[product] *= 0.8
    
    def get_pathway_stats(self) -> Dict[str, Any]:
        return {pid: {'throughput_rate': p.throughput_rate, 'energy_cost': p.energy_cost_ecoatp,
                      'is_active': p.is_active} for pid, p in self.pathways.items()}

# ============================================================================
# Enhanced Expert Router with Causal Constraints
# ============================================================================

class ExpertRouter:
    """
    Enhanced Expert Router v7.0.0 - Complete Signal Transduction Cascade with Causal Constraints
    
    Features:
    - Signal transduction for task routing
    - Allosteric regulation by gradient fields
    - Metabolic pathway selection
    - What-if analysis for routing scenarios
    - Causal inference for decision factors
    - Natural language explanations
    - Routing forecasts
    - Federated learning integration
    - Predictive analytics
    - Carbon/Helium optimization
    - Causal constraint modeling (NEW)
    - Constraint propagation (NEW)
    - Trade-off analysis (NEW)
    """
    
    def __init__(
        self,
        enable_quantum: bool = False,
        metrics_collector: Optional[Any] = None,
        enable_signal_transduction: bool = True,
        enable_allosteric: bool = True,
        enable_metabolic_pathways: bool = True,
        enable_cooperative_binding: bool = True,
        enable_homeostasis: bool = True,
        enable_bio_integration: bool = True,
        enable_federated: bool = True,
        enable_predictive: bool = True,
        enable_carbon_intensity: bool = True,
        enable_helium_optimization: bool = True,
        enable_causal_constraints: bool = True,
        server_url: Optional[str] = None,
        helium_budget_l: float = 100.0
    ):
        # Feature flags
        self.enable_signal_transduction = enable_signal_transduction
        self.enable_allosteric = enable_allosteric
        self.enable_metabolic_pathways = enable_metabolic_pathways
        self.enable_cooperative_binding = enable_cooperative_binding
        self.enable_homeostasis = enable_homeostasis
        self.enable_bio_integration = enable_bio_integration
        self.enable_federated = enable_federated
        self.enable_predictive = enable_predictive
        self.enable_carbon_intensity = enable_carbon_intensity
        self.enable_helium_optimization = enable_helium_optimization
        self.enable_causal_constraints = enable_causal_constraints
        
        # New modules
        self.carbon_manager = CarbonIntensityManager() if enable_carbon_intensity else None
        self.helium_optimizer = HeliumEfficiencyOptimizer(helium_budget_l) if enable_helium_optimization else None
        self.federated_learner = FederatedRoutingLearner(server_url) if enable_federated else None
        self.predictive_analyzer = PredictiveRoutingAnalyzer() if enable_predictive else None
        self.causal_model = CausalConstraintModel() if enable_causal_constraints else None
        
        # Bio-inspired subsystems
        self.signal_engine = SignalTransductionEngine() if enable_signal_transduction else None
        self.allosteric_system = AllostericRegulationSystem() if enable_allosteric else None
        self.metabolic_router = MetabolicPathwayRouter() if enable_metabolic_pathways else None
        
        # Bio-inspired module references (injected)
        self.gradient_manager = None
        self.token_manager = None
        self.scheduler = None
        self.compartment_manager = None
        self.biomass_storage = None
        self.harvester = None
        self.bio_core = None
        
        # Initialize signal receptors
        if self.signal_engine:
            self.signal_engine.create_receptor('carbon_receptor', SignalType.ENDOCRINE,
                'carbon_gradient', affinity=0.7, amplification=AmplificationLevel.HIGH)
            self.signal_engine.create_receptor('helium_receptor', SignalType.ENDOCRINE,
                'helium_gradient', affinity=0.6, amplification=AmplificationLevel.MODERATE)
            self.signal_engine.create_receptor('task_receptor', SignalType.NEUROTRANSMITTER,
                'task_signal', affinity=0.9, amplification=AmplificationLevel.HIGH)
            self.signal_engine.create_receptor('stress_receptor', SignalType.AUTOCRINE,
                'stress_signal', affinity=0.8, amplification=AmplificationLevel.MAXIMUM)
            self.signal_engine.create_receptor('trust_receptor', SignalType.PARACRINE,
                'trust_gradient', affinity=0.5, amplification=AmplificationLevel.LOW)
            self.signal_engine.setup_crosstalk(SecondMessenger.cAMP, SecondMessenger.IP3, 0.3)
            self.signal_engine.setup_crosstalk(SecondMessenger.CALCIUM, SecondMessenger.cAMP, 0.5)
        
        if self.allosteric_system:
            self.allosteric_system.setup_cooperativity('energy', 'data', 0.4)
            self.allosteric_system.setup_cooperativity('energy', 'helium', 0.3)
            self.allosteric_system.setup_cooperativity('data', 'iot', 0.5)
        
        self.metrics_collector = metrics_collector
        self.metrics = RoutingMetrics()
        self.experts: Dict[str, Any] = {}
        self.expert_index_map: Dict[int, str] = {}
        self.circuit_breakers: Dict[str, ExpertCircuitBreaker] = {}
        self.gating_network = None
        self.active_routes = 0
        self.max_concurrent_routes = 100
        self._route_lock = asyncio.Lock()
        self.routing_history: deque = deque(maxlen=10000)
        
        self._initialize_experts(enable_quantum)
        self._start_background_tasks()
        
        logger.info(f"Expert Router v7.0.0 initialized with causal constraints")
    
    def _initialize_experts(self, enable_quantum: bool):
        try:
            from .experts.energy_expert import EnergyExpert
            from .experts.data_expert import DataExpert
            from .experts.iot_expert import IoTExpert
            from .experts.helium_expert import HeliumExpert
            
            self.experts = {
                'energy': EnergyExpert(),
                'data': DataExpert(),
                'iot': IoTExpert(),
                'helium': HeliumExpert()
            }
            if enable_quantum:
                from .experts.quantum_expert import QuantumExpert
                self.experts['quantum'] = QuantumExpert()
            
            for idx, (expert_id, expert) in enumerate(self.experts.items()):
                self.expert_index_map[idx] = expert_id
                self.circuit_breakers[expert_id] = ExpertCircuitBreaker(expert_id=expert_id)
            logger.info(f"Initialized {len(self.experts)} experts")
        except Exception as e:
            logger.error(f"Failed to initialize experts: {str(e)}")
    
    def _start_background_tasks(self):
        asyncio.create_task(self._signal_transduction_loop())
        asyncio.create_task(self._homeostasis_loop())
        asyncio.create_task(self._product_inhibition_loop())
        if self.enable_carbon_intensity:
            asyncio.create_task(self._carbon_update_loop())
        if self.enable_federated:
            asyncio.create_task(self._federated_sync_loop())
        if self.enable_predictive:
            asyncio.create_task(self._predictive_update_loop())
    
    # ========================================================================
    # Background Loops
    # ========================================================================
    
    async def _carbon_update_loop(self):
        while True:
            try:
                if self.carbon_manager:
                    await self.carbon_manager.update_carbon_intensity()
                await asyncio.sleep(self.carbon_manager.update_interval if self.carbon_manager else 300)
            except Exception as e:
                logger.error(f"Carbon update error: {str(e)}")
                await asyncio.sleep(60)
    
    async def _federated_sync_loop(self):
        while True:
            try:
                if self.federated_learner and self.routing_history:
                    routing_data = []
                    for record in list(self.routing_history)[-100:]:
                        routing_data.append({
                            'carbon_zone': record.get('context', {}).get('carbon_zone', 0),
                            'helium_scarcity': record.get('context', {}).get('helium_scarcity', 0.5),
                            'task_complexity': record.get('context', {}).get('task_complexity', 0.5),
                            'token_balance': 500,
                            'carbon_gradient': 0.5,
                            'trust_gradient': 0.5,
                            'opportunity_gradient': 0.5,
                            'stress_level': 0.3,
                            'latency_budget': 100,
                            'energy_budget': 100,
                            'selected_expert_idx': 0
                        })
                    await self.federated_learner.participate_in_round(
                        routing_data,
                        performance=self.metrics.success_rate
                    )
                await asyncio.sleep(3600)
            except Exception as e:
                logger.error(f"Federated sync error: {str(e)}")
                await asyncio.sleep(300)
    
    async def _predictive_update_loop(self):
        while True:
            try:
                if self.predictive_analyzer:
                    self.predictive_analyzer.update_history({
                        'success_rate': self.metrics.success_rate,
                        'avg_latency_ms': self.metrics.average_latency_ms,
                        'carbon_efficiency': 0.5,
                        'helium_efficiency': 0.5,
                        'expert_utilization': self.active_routes / max(self.max_concurrent_routes, 1)
                    })
                    await self.predictive_analyzer.train_forecast_model()
                await asyncio.sleep(300)
            except Exception as e:
                logger.error(f"Predictive update error: {str(e)}")
                await asyncio.sleep(60)
    
    async def _signal_transduction_loop(self):
        while True:
            try:
                if self.signal_engine:
                    gradient_levels = self._get_real_gradient_levels()
                    self.signal_engine.bind_ligand('carbon_receptor', gradient_levels.get('carbon', 0.5))
                    self.signal_engine.bind_ligand('helium_receptor', gradient_levels.get('helium', 0.5))
                    self.signal_engine.bind_ligand('trust_receptor', gradient_levels.get('trust', 0.5))
                    token_level = self._get_real_token_availability()
                    stress_level = self._get_real_stress_level()
                    if stress_level > 0.5:
                        self.signal_engine.bind_ligand('stress_receptor', stress_level)
                    self.signal_engine.apply_crosstalk()
                    if self.allosteric_system:
                        self.allosteric_system.bind_modulator('carbon_site', gradient_levels.get('carbon', 0.5))
                        self.allosteric_system.bind_modulator('helium_site', gradient_levels.get('helium', 0.5))
                        self.allosteric_system.bind_modulator('trust_site', gradient_levels.get('trust', 0.5))
                        self.allosteric_system.bind_modulator('token_site', token_level)
                        if stress_level > 0.3:
                            self.allosteric_system.bind_modulator('stress_site', stress_level)
                await asyncio.sleep(2.0)
            except Exception as e:
                logger.error(f"Signal transduction error: {str(e)}")
                await asyncio.sleep(5.0)
    
    async def _homeostasis_loop(self):
        while True:
            try:
                if self.enable_homeostasis and self.allosteric_system:
                    modulation = self.allosteric_system.get_routing_modulation()
                    if modulation['conservation_mode'] > 0.7:
                        if np.random.random() < 0.1:
                            self.allosteric_system.bind_modulator('token_site', 0.8)
                    if modulation['risk_tolerance'] > 0.4:
                        self.allosteric_system.bind_modulator('stress_site', 0.3)
                await asyncio.sleep(10.0)
            except Exception as e:
                logger.error(f"Homeostasis error: {str(e)}")
                await asyncio.sleep(30.0)
    
    async def _product_inhibition_loop(self):
        while True:
            try:
                if self.metabolic_router:
                    self.metabolic_router.apply_product_inhibition()
                await asyncio.sleep(30.0)
            except Exception as e:
                logger.error(f"Product inhibition error: {str(e)}")
                await asyncio.sleep(60.0)
    
    # ========================================================================
    # Bio-Inspired Module Injection
    # ========================================================================
    
    def inject_bio_core(self, bio_core: Any = None, **kwargs):
        if bio_core:
            self.bio_core = bio_core
            self.gradient_manager = getattr(bio_core, 'gradient_manager', None)
            self.token_manager = getattr(bio_core, 'token_manager', None)
            self.scheduler = getattr(bio_core, 'scheduler', None)
            self.compartment_manager = getattr(bio_core, 'compartment_manager', None)
            self.biomass_storage = getattr(bio_core, 'biomass_storage', None)
            self.harvester = getattr(bio_core, 'harvester', None)
        else:
            self.gradient_manager = kwargs.get('gradient_manager')
            self.token_manager = kwargs.get('token_manager')
            self.scheduler = kwargs.get('scheduler')
            self.compartment_manager = kwargs.get('compartment_manager')
            self.biomass_storage = kwargs.get('biomass_storage')
            self.harvester = kwargs.get('harvester')
        logger.info("Bio-inspired core injected into Expert Router")
    
    # ========================================================================
    # Real Data Access Methods
    # ========================================================================
    
    def _get_real_gradient_levels(self) -> Dict[str, float]:
        if self.gradient_manager:
            return self.gradient_manager.get_field_strengths()
        return {'carbon': 0.5, 'helium': 0.5, 'trust': 0.5, 'opportunity': 0.5}
    
    def _get_real_token_availability(self) -> float:
        if self.token_manager:
            summary = self.token_manager.get_system_summary()
            return min(1.0, summary.get('total_balance', 500) / 1000.0)
        return 0.5
    
    def _get_real_stress_level(self) -> float:
        stress = 0.0
        if self.gradient_manager:
            carbon = self.gradient_manager.fields.get('carbon')
            if carbon and carbon.gradient_strength > 0.7:
                stress += 0.4
        if self.token_manager:
            summary = self.token_manager.get_system_summary()
            if summary.get('total_balance', 1000) < 200:
                stress += 0.3
        utilization = self.active_routes / max(self.max_concurrent_routes, 1)
        if utilization > 0.8:
            stress += 0.3
        return min(1.0, stress)
    
    def _get_compartment_health(self, expert_id: str) -> float:
        if self.compartment_manager:
            compartment = self.compartment_manager.find_best_compartment(expert_id)
            if compartment:
                return compartment.health_score
        return 0.7
    
    # ========================================================================
    # Main Routing Method with Causal Constraints
    # ========================================================================
    
    async def route_and_execute(
        self, workload_profile: Dict[str, Any], meta_cognitive_state: Dict[str, Any],
        dual_axis_context: Dict[str, Any], symbolic_constraints: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> Dict[str, Any]:
        start_time = time.time()
        route_id = hashlib.md5(f"{workload_profile}{start_time}".encode()).hexdigest()[:12]
        
        async with self._route_lock:
            self.active_routes += 1
        self.metrics.total_routes += 1
        
        try:
            gradient_levels = self._get_real_gradient_levels()
            carbon_intensity = 400
            if self.carbon_manager:
                carbon_intensity = await self.carbon_manager.get_current_intensity()
            
            # Apply causal constraints if enabled
            causal_constraints = {}
            if self.enable_causal_constraints and self.causal_model:
                # Propagate constraints from context
                if dual_axis_context.get('carbon_zone', 0) > 8:
                    carbon_constraints = await self.causal_model.propagate_constraints(
                        'carbon',
                        dual_axis_context['carbon_zone'] / 10,
                        {'max_carbon': 0.0005}
                    )
                    causal_constraints.update(carbon_constraints)
                
                if dual_axis_context.get('helium_scarcity', 0) > 0.6:
                    helium_constraints = await self.causal_model.propagate_constraints(
                        'helium',
                        dual_axis_context['helium_scarcity'],
                        {'max_helium': 0.005}
                    )
                    causal_constraints.update(helium_constraints)
                
                # Validate constraints against workload
                if not await self._validate_causal_constraints(causal_constraints, workload_profile):
                    logger.warning("Causal constraints violated - adjusting routing")
            
            signal_activated = False
            if self.signal_engine:
                task_signal = workload_profile.get('complexity', 0.5)
                signal_activated = self.signal_engine.bind_ligand('task_receptor', task_signal)
            
            routing_modulation = {}
            if self.allosteric_system:
                routing_modulation = self.allosteric_system.get_routing_modulation()
            
            modulator_levels = {
                'carbon_gradient': gradient_levels.get('carbon', 0.5),
                'helium_gradient': gradient_levels.get('helium', 0.5),
                'token_availability': self._get_real_token_availability(),
                'trust_gradient': gradient_levels.get('trust', 0.5),
                'opportunity_gradient': gradient_levels.get('opportunity', 0.5),
                'task_complexity': workload_profile.get('complexity', 0.5)
            }
            
            selected_pathway = None
            pathway_efficiency = 0.0
            if self.metabolic_router:
                task_type = workload_profile.get('task_type', 'general')
                substrate_conc = workload_profile.get('complexity', 0.5)
                energy_budget = meta_cognitive_state.get('carbon_budget_remaining', 100.0)
                selected_pathway, pathway_efficiency = self.metabolic_router.select_optimal_pathway(
                    task_type, substrate_conc, modulator_levels, energy_budget
                )
            
            # Optimize helium allocation
            if self.helium_optimizer and selected_pathway:
                expert_requirements = {}
                for expert_id in self.experts:
                    expert_requirements[expert_id] = 0.01
                optimized_allocation = await self.helium_optimizer.optimize_helium_allocation(expert_requirements)
                for expert_id, allocation in optimized_allocation.items():
                    self.helium_optimizer.set_helium_allocation(expert_id, allocation)
            
            gating_context = self._build_gating_context(workload_profile, meta_cognitive_state, dual_axis_context)
            routing_result = self.gating_network.route(gating_context) if self.gating_network else {
                'expert_indices': [0, 1], 'weights': [0.6, 0.4], 'confidence': 0.8
            }
            
            if routing_modulation:
                exploration = routing_modulation.get('exploration_rate', 0.1)
                if np.random.random() < exploration:
                    all_indices = list(range(len(self.experts)))
                    routing_result['expert_indices'] = list(np.random.choice(
                        all_indices, size=min(2, len(all_indices)), replace=False
                    ))
            
            expert_plans = await self._execute_experts(
                routing_result, workload_profile, meta_cognitive_state, dual_axis_context
            )
            
            # Apply causal constraint validation to plans
            if self.enable_causal_constraints and self.causal_model:
                validated_plans = []
                for plan in expert_plans:
                    if await self._validate_plan_constraints(plan, causal_constraints):
                        validated_plans.append(plan)
                    else:
                        logger.warning(f"Plan from {plan.get('expert_id')} violated constraints")
                if validated_plans:
                    expert_plans = validated_plans
            
            if selected_pathway and self.metabolic_router:
                for plan in expert_plans:
                    plan['pathway_efficiency'] = pathway_efficiency
                self.metabolic_router.record_throughput(
                    selected_pathway, pathway_efficiency,
                    sum(p.get('estimated_energy_kwh', 0) for p in expert_plans)
                )
            
            final_plan = await self._aggregate_plans(expert_plans, dual_axis_context, gating_context)
            
            # Apply carbon-aware decisions
            if self.carbon_manager:
                current_intensity = await self.carbon_manager.get_current_intensity()
                if current_intensity > 500 and final_plan.get('action') == 'execute_full':
                    final_plan['action'] = 'execute_throttled'
                    final_plan['reason'] = 'High carbon intensity'
            
            if self.enable_homeostasis and final_plan.get('action') == 'execute_full':
                carbon_zone = dual_axis_context.get('carbon_zone', 0)
                conservation = routing_modulation.get('conservation_mode', 0)
                if carbon_zone >= 10 and conservation > 0.6:
                    final_plan['action'] = 'execute_throttled'
            
            self.metrics.successful_routes += 1
            execution_time = (time.time() - start_time) * 1000
            self.metrics.average_latency_ms = (self.metrics.average_latency_ms * 0.9 + execution_time * 0.1)
            
            for plan in expert_plans:
                expert_id = plan.get('expert_id')
                if expert_id in self.circuit_breakers:
                    self.circuit_breakers[expert_id].record_success()
            
            response = {
                'success': True, 'route_id': route_id, 'plans': expert_plans,
                'final_plan': final_plan, 'execution_time_ms': execution_time,
                'bio_inspired_metadata': {
                    'signal_activated': signal_activated, 'selected_pathway': selected_pathway,
                    'pathway_efficiency': pathway_efficiency, 'gradient_levels': gradient_levels,
                    'token_availability': self._get_real_token_availability(),
                    'stress_level': self._get_real_stress_level(),
                    'carbon_intensity': carbon_intensity,
                    'routing_modulation': routing_modulation,
                    'helium_allocation': self.helium_optimizer.get_helium_status() if self.helium_optimizer else {},
                    'second_messenger_levels': {
                        sm.value: self.signal_engine.get_second_messenger_level(sm)
                        for sm in SecondMessenger
                    } if self.signal_engine else {},
                    'causal_constraints': causal_constraints if self.enable_causal_constraints else {},
                    'timestamp': datetime.utcnow().isoformat()
                }
            }
            
            # Store in routing history
            self.routing_history.append({
                'route_id': route_id, 'decisions': list(zip(routing_result['expert_indices'], routing_result['weights'])),
                'context': gating_context, 'confidence': routing_result.get('confidence', 0.5),
                'causal_constraints': causal_constraints if self.enable_causal_constraints else {},
                'timestamp': datetime.utcnow()
            })
            
            # Federated learning participation
            if self.federated_learner and len(self.routing_history) % 100 == 0:
                federated_result = await self.federated_learner.participate_in_round(
                    [{'success_rate': self.metrics.success_rate, 'avg_latency_ms': self.metrics.average_latency_ms}],
                    performance=self.metrics.success_rate
                )
                response['federated_round'] = federated_result.get('round', 0)
            
            # Predictive analytics update
            if self.predictive_analyzer:
                self.predictive_analyzer.update_history({
                    'success_rate': self.metrics.success_rate,
                    'avg_latency_ms': self.metrics.average_latency_ms,
                    'carbon_efficiency': 0.5,
                    'helium_efficiency': 0.5,
                    'expert_utilization': self.active_routes / max(self.max_concurrent_routes, 1)
                })
                await self.predictive_analyzer.train_forecast_model()
                forecast = await self.predictive_analyzer.predict_routing_performance()
                response['predictive_forecast'] = forecast
            
            # Update causal model with routing result
            if self.enable_causal_constraints and self.causal_model and final_plan.get('success', False):
                await self.causal_model.propagate_constraints(
                    'routing',
                    self.metrics.success_rate,
                    {'routing_success': final_plan.get('action', 'unknown')}
                )
            
            return response
            
        except Exception as e:
            logger.error(f"Routing failed: {str(e)}", exc_info=True)
            self.metrics.failed_routes += 1
            return {'success': False, 'error': str(e), 'fallback': True, 'action': 'execute_minimal'}
        finally:
            async with self._route_lock:
                self.active_routes -= 1
    
    async def _validate_causal_constraints(
        self,
        constraints: Dict[str, Any],
        workload: Dict[str, Any]
    ) -> bool:
        """Validate causal constraints against workload"""
        if not constraints:
            return True
        
        # Check carbon constraints
        if 'carbon' in constraints and 'estimated_carbon_kg' in workload:
            carbon_limit = constraints['carbon'].get('max_carbon', float('inf'))
            if workload['estimated_carbon_kg'] > carbon_limit:
                logger.warning(f"Carbon constraint violated: {workload['estimated_carbon_kg']} > {carbon_limit}")
                return False
        
        # Check helium constraints
        if 'helium' in constraints and 'estimated_helium_units' in workload:
            helium_limit = constraints['helium'].get('max_helium', float('inf'))
            if workload['estimated_helium_units'] > helium_limit:
                logger.warning(f"Helium constraint violated: {workload['estimated_helium_units']} > {helium_limit}")
                return False
        
        # Check quantum constraints
        if 'quantum' in constraints and workload.get('task_type') == 'quantum':
            feasibility = constraints['quantum'].get('feasibility', 1.0)
            if feasibility < 0.5:
                logger.warning(f"Quantum feasibility low: {feasibility}")
                return False
        
        return True
    
    async def _validate_plan_constraints(
        self,
        plan: Dict[str, Any],
        constraints: Dict[str, Any]
    ) -> bool:
        """Validate a single expert plan against constraints"""
        if not constraints:
            return True
        
        expert_id = plan.get('expert_id', '')
        
        # Check domain-specific constraints
        for domain, constraint_data in constraints.items():
            if domain in expert_id.lower():
                if 'causal_impact' in constraint_data and constraint_data['causal_impact'] > 0.7:
                    logger.warning(f"Plan {expert_id} has high causal impact: {constraint_data['causal_impact']}")
                    return False
        
        return True
    
    def _build_gating_context(self, workload_profile, meta_cognitive_state, dual_axis_context):
        from .gating_network import GatingContext
        return GatingContext(
            task_type=workload_profile.get('task_type', 'inference'),
            task_complexity=workload_profile.get('complexity', 0.5),
            input_size_mb=workload_profile.get('input_size_mb', 1.0),
            carbon_budget_remaining=meta_cognitive_state.get('carbon_budget_remaining', 1.0),
            helium_budget_remaining=meta_cognitive_state.get('helium_budget_remaining', 1.0),
            latency_budget_ms=meta_cognitive_state.get('latency_budget_ms', 100.0),
            historical_success_rate=meta_cognitive_state.get('historical_success_rate', 0.9),
            carbon_zone=dual_axis_context.get('carbon_zone', 0),
            helium_scarcity=dual_axis_context.get('helium_scarcity', 0.5),
            time_of_day=datetime.utcnow().hour,
            grid_carbon_intensity=workload_profile.get('grid_carbon_intensity', 400.0),
            hardware_availability=workload_profile.get('hardware_availability', {
                'cpu': 1.0, 'gpu': 0.8, 'quantum': 0.0, 'edge': 0.5
            })
        )
    
    async def _execute_experts(self, routing_result, workload_profile, meta_cognitive_state, dual_axis_context):
        plans = []
        for expert_idx, weight in zip(routing_result['expert_indices'], routing_result['weights']):
            expert_id = self.expert_index_map.get(expert_idx)
            if not expert_id or expert_id not in self.experts:
                continue
            expert = self.experts[expert_id]
            try:
                inhibition_factor = 1.0
                if self.metabolic_router and expert_id in self.metabolic_router.enzyme_kinetics:
                    inhibitor_level = sum(self.metabolic_router.product_levels.values())
                    inhibition_factor = self.metabolic_router.apply_competitive_inhibition(expert_id, inhibitor_level)
                if inhibition_factor < 0.3:
                    continue
                plan = {
                    'expert_id': expert_id, 'routing_weight': float(weight),
                    'estimated_carbon_kg': getattr(expert.profile, 'carbon_per_inference', 0.0001),
                    'estimated_helium_units': getattr(expert.profile, 'helium_per_inference', 0.01),
                    'estimated_energy_kwh': getattr(expert.profile, 'energy_per_inference', 0.001),
                    'estimated_latency_ms': getattr(expert.profile, 'avg_latency_ms', 50.0),
                    'inhibition_factor': inhibition_factor
                }
                plans.append(plan)
            except Exception as e:
                logger.error(f"Expert {expert_id} failed: {str(e)}")
        return plans
    
    async def _aggregate_plans(self, expert_plans, dual_axis_context, gating_context):
        if not expert_plans:
            return {'action': 'defer', 'reason': 'No expert plans available'}
        total_weight = sum(p.get('routing_weight', 0) for p in expert_plans)
        if total_weight > 0:
            for plan in expert_plans:
                plan['normalized_weight'] = plan.get('routing_weight', 0) / total_weight
        carbon_zone = dual_axis_context.get('carbon_zone', 0)
        helium_scarcity = dual_axis_context.get('helium_scarcity', 0.5)
        if carbon_zone >= 12 and helium_scarcity > 0.8:
            action = 'defer'
        elif carbon_zone >= 8 or helium_scarcity > 0.6:
            action = 'execute_minimal'
        elif carbon_zone >= 4 or helium_scarcity > 0.3:
            action = 'execute_throttled'
        else:
            action = 'execute_full'
        return {
            'action': action,
            'aggregate_carbon_kg': sum(p.get('estimated_carbon_kg', 0) * p.get('normalized_weight', 0) for p in expert_plans),
            'aggregate_helium': sum(p.get('estimated_helium_units', 0) * p.get('normalized_weight', 0) for p in expert_plans),
            'expert_count': len(expert_plans)
        }
    
    # ========================================================================
    # What-If Analysis with Causal Constraints
    # ========================================================================
    
    def run_what_if_routing(self, task: Dict[str, Any], alternative_scenarios: List[Dict[str, Any]]) -> Dict[str, Any]:
        results = {'task': task, 'timestamp': datetime.utcnow().isoformat(), 'scenarios': []}
        baseline = self._simulate_routing(task, {})
        results['baseline'] = baseline
        
        # Add causal analysis to baseline
        if self.enable_causal_constraints and self.causal_model:
            causal_path = asyncio.run(
                self.causal_model.get_causal_path('carbon', 'energy')
            )
            baseline['causal_path'] = causal_path
        
        for scenario in alternative_scenarios:
            scenario_result = self._simulate_routing(task, scenario)
            
            # Add causal analysis to scenario
            if self.enable_causal_constraints and self.causal_model:
                propagations = asyncio.run(
                    self.causal_model.propagate_constraints(
                        'carbon',
                        scenario.get('carbon_zone', 0) / 10,
                        scenario
                    )
                )
                scenario_result['causal_propagations'] = propagations
            
            results['scenarios'].append({
                'scenario': scenario,
                'routing': scenario_result,
                'differs_from_baseline': (
                    set(scenario_result.get('selected_experts', [])) != 
                    set(baseline.get('selected_experts', []))
                ),
                'causal_impact': scenario_result.get('causal_propagations', {})
            })
        
        results['recommendations'] = self._generate_what_if_recommendations(baseline, results['scenarios'])
        return results
    
    def _simulate_routing(self, task: Dict[str, Any], overrides: Dict[str, Any]) -> Dict[str, Any]:
        context = {
            'task_type': task.get('task_type', 'inference'),
            'complexity': overrides.get('complexity', task.get('complexity', 0.5)),
            'carbon_zone': overrides.get('carbon_zone', task.get('carbon_zone', 3)),
            'helium_scarcity': overrides.get('helium_scarcity', task.get('helium_dependency', 0.2)),
            'token_balance': overrides.get('token_balance', 500),
            'carbon_gradient': overrides.get('carbon_gradient', 0.5)
        }
        expert_scores = {}
        for expert_id, expert in self.experts.items():
            score = 0.5
            if hasattr(expert, 'profile'):
                if context['carbon_zone'] > 8:
                    score += (1.0 - expert.profile.carbon_per_inference * 10000) * 0.3
            if context['token_balance'] < 100:
                score += (1.0 - getattr(expert.profile, 'energy_per_inference', 0.001) * 1000) * 0.3
            if self.enable_bio_integration:
                score += context['carbon_gradient'] * 0.2
            expert_scores[expert_id] = score
        sorted_experts = sorted(expert_scores.items(), key=lambda x: x[1], reverse=True)
        top_experts = sorted_experts[:2]
        return {
            'context': context,
            'selected_experts': [e[0] for e in top_experts],
            'scores': {e[0]: round(e[1], 3) for e in top_experts},
            'confidence': top_experts[0][1] / sum(e[1] for e in top_experts) if top_experts else 0.5
        }
    
    def _generate_what_if_recommendations(self, baseline: Dict, scenarios: List[Dict]) -> List[str]:
        recommendations = []
        changed = [s for s in scenarios if s['differs_from_baseline']]
        if changed:
            recommendations.append(f"Routing sensitive to: {', '.join(list(changed[0]['scenario'].keys())[:3])}")
        baseline_conf = baseline.get('confidence', 0.5)
        for s in scenarios:
            scenario_conf = s['routing'].get('confidence', 0.5)
            if scenario_conf < baseline_conf * 0.7:
                recommendations.append(f"Confidence drops significantly under: {s['scenario']}")
        if not recommendations:
            recommendations.append("Routing is robust across all tested scenarios.")
        return recommendations
    
    # ========================================================================
    # Causal Inference for Routing
    # ========================================================================
    
    def analyze_causal_factors(self, route_id: str) -> Optional[Dict[str, Any]]:
        for record in self.routing_history:
            if record.get('route_id') == route_id:
                decisions = record.get('decisions', [])
                context = record.get('context', {})
                causal_constraints = record.get('causal_constraints', {})
                
                if not decisions:
                    return None
                
                causal_chain = []
                
                # Add causal constraints to analysis
                if causal_constraints:
                    for domain, constraints in causal_constraints.items():
                        if 'causal_impact' in constraints:
                            causal_chain.append({
                                'factor': f"{domain.capitalize()} Impact",
                                'impact': 'HIGH' if constraints['causal_impact'] > 0.6 else 'MODERATE',
                                'effect': f"Causal impact of {domain}: {constraints['causal_impact']:.2f}",
                                'strength': min(1.0, constraints['causal_impact'])
                            })
                
                if hasattr(context, 'carbon_zone') and context.carbon_zone > 8:
                    causal_chain.append({
                        'factor': 'High Carbon Zone',
                        'impact': 'HIGH',
                        'effect': f'Carbon zone {context.carbon_zone} forced selection of low-carbon experts',
                        'strength': 0.8
                    })
                if hasattr(context, 'helium_scarcity') and context.helium_scarcity > 0.6:
                    causal_chain.append({
                        'factor': 'Helium Scarcity',
                        'impact': 'HIGH',
                        'effect': 'Restricted helium-intensive expert selection',
                        'strength': 0.7
                    })
                if hasattr(context, 'task_complexity') and context.task_complexity > 0.7:
                    causal_chain.append({
                        'factor': 'High Task Complexity',
                        'impact': 'MODERATE',
                        'effect': 'Required specialized expert handling',
                        'strength': 0.5
                    })
                if not causal_chain:
                    causal_chain.append({
                        'factor': 'Balanced Conditions',
                        'impact': 'LOW',
                        'effect': 'Standard routing based on performance scores',
                        'strength': 0.3
                    })
                causal_chain.sort(key=lambda x: x['strength'], reverse=True)
                
                selected = []
                for expert_idx, weight in decisions:
                    expert_id = self.expert_index_map.get(expert_idx, 'unknown')
                    selected.append({'expert': expert_id, 'weight': f"{weight:.2f}"})
                
                return {
                    'route_id': route_id,
                    'causal_chain': causal_chain,
                    'primary_driver': causal_chain[0] if causal_chain else None,
                    'selected_experts': selected,
                    'confidence': record.get('confidence', 0.5),
                    'causal_constraints_used': bool(causal_constraints)
                }
        return None
    
    # ========================================================================
    # Natural Language Explanations
    # ========================================================================
    
    def explain_routing_decision(self, route_id: str) -> Optional[Dict[str, Any]]:
        for record in self.routing_history:
            if record.get('route_id') == route_id:
                decisions = record.get('decisions', [])
                context = record.get('context', {})
                causal_constraints = record.get('causal_constraints', {})
                
                if not decisions:
                    return None
                
                selected = []
                for expert_idx, weight in decisions:
                    expert_id = self.expert_index_map.get(expert_idx, 'unknown')
                    selected.append({'expert': expert_id, 'weight': f"{weight:.2f}"})
                
                factors = []
                if hasattr(context, 'carbon_zone') and context.carbon_zone > 8:
                    factors.append("High carbon zone favored efficient experts")
                if hasattr(context, 'helium_scarcity') and context.helium_scarcity > 0.7:
                    factors.append("Helium scarcity restricted cooling options")
                if hasattr(context, 'task_complexity') and context.task_complexity > 0.7:
                    factors.append("High complexity required specialized handling")
                
                # Add causal constraint factors
                if causal_constraints:
                    for domain, constraints in causal_constraints.items():
                        if 'causal_impact' in constraints:
                            factors.append(f"Causal impact of {domain}: {constraints['causal_impact']:.2f}")
                
                executive = (
                    f"Selected {selected[0]['expert']} (weight: {selected[0]['weight']}) "
                    f"{'and ' + selected[1]['expert'] if len(selected) > 1 else ''} "
                    f"based on {len(factors)} primary factors."
                )
                
                counterfactual = "If carbon zone were lower, more experts would be available for selection."
                
                if causal_constraints:
                    counterfactual += " Causal constraints were applied to ensure cross-domain sustainability."
                
                return {
                    'route_id': route_id,
                    'executive_summary': executive,
                    'selected_experts': selected,
                    'decision_factors': factors,
                    'counterfactual': counterfactual,
                    'confidence': record.get('confidence', 0.5),
                    'causal_constraints_applied': bool(causal_constraints),
                    'timestamp': record.get('timestamp', datetime.utcnow()).isoformat()
                }
        return None
    
    # ========================================================================
    # Routing Forecast
    # ========================================================================
    
    def get_routing_forecast(self, task_type: str, horizon_minutes: int = 30) -> Dict[str, Any]:
        gradient_trends = {}
        current = self._get_real_gradient_levels()
        gradient_trends = {k: 'rising' if v > 0.6 else 'falling' if v < 0.4 else 'stable' for k, v in current.items()}
        
        # Get causal insights for forecast
        causal_insights = {}
        if self.enable_causal_constraints and self.causal_model:
            causal_graph = self.causal_model.get_causal_graph_summary()
            causal_insights = {
                'causal_edges': causal_graph.get('edge_count', 0),
                'domains': causal_graph.get('nodes', [])
            }
        
        utilization = self.gating_network.get_expert_utilization() if self.gating_network else {}
        
        predictions = {}
        for expert_id in self.experts:
            if gradient_trends.get('carbon') == 'rising':
                trend = 'increasing' if expert_id in ['energy', 'helium'] else 'stable'
            elif gradient_trends.get('carbon') == 'falling':
                trend = 'stable'
            else:
                trend = 'stable'
            predictions[expert_id] = {'predicted_trend': trend, 'confidence': 0.7}
        
        increasing = [k for k, v in predictions.items() if v['predicted_trend'] == 'increasing']
        recommendation = (
            f"Prepare {', '.join(increasing)} expert(s) for increased load." if increasing
            else "Routing patterns expected to remain stable."
        )
        
        return {
            'task_type': task_type, 'horizon_minutes': horizon_minutes,
            'gradient_trends': gradient_trends, 'expert_predictions': predictions,
            'recommendation': recommendation,
            'causal_insights': causal_insights if self.enable_causal_constraints else {}
        }
    
    # ========================================================================
    # Statistics
    # ========================================================================
    
    def get_routing_stats(self) -> Dict[str, Any]:
        stats = {
            'metrics': {
                'total_routes': self.metrics.total_routes,
                'successful_routes': self.metrics.successful_routes,
                'failed_routes': self.metrics.failed_routes,
                'success_rate': self.metrics.success_rate,
                'active_routes': self.active_routes,
                'carbon_savings_kg': self.metrics.carbon_savings_kg,
                'helium_savings_l': self.metrics.helium_savings_l
            },
            'gradient_levels': self._get_real_gradient_levels(),
            'token_availability': self._get_real_token_availability(),
            'stress_level': self._get_real_stress_level(),
            'carbon_intensity': self.carbon_manager.get_current_intensity() if self.carbon_manager else 400
        }
        
        if self.signal_engine:
            stats['signal_transduction'] = self.signal_engine.get_signaling_status()
        if self.allosteric_system:
            stats['allosteric_regulation'] = self.allosteric_system.get_regulation_status()
        if self.metabolic_router:
            stats['metabolic_pathways'] = self.metabolic_router.get_pathway_stats()
        if self.helium_optimizer:
            stats['helium_status'] = self.helium_optimizer.get_helium_status()
        if self.federated_learner:
            stats['federated_insights'] = self.federated_learner.get_federated_insights()
        if self.predictive_analyzer:
            stats['predictive_forecast'] = asyncio.run(self.predictive_analyzer.predict_routing_performance())
        if self.enable_causal_constraints and self.causal_model:
            stats['causal_graph'] = self.causal_model.get_causal_graph_summary()
        
        return stats
    
    def get_helium_efficiency_report(self) -> Dict[str, Any]:
        if self.helium_optimizer:
            return self.helium_optimizer.get_helium_status()
        return {'status': 'helium_optimization_not_enabled'}
    
    def get_predictive_forecast(self) -> Dict[str, Any]:
        if self.predictive_analyzer:
            return asyncio.run(self.predictive_analyzer.predict_routing_performance())
        return {'status': 'predictive_analysis_not_enabled'}
    
    def get_causal_insights(self) -> Dict[str, Any]:
        """Get causal insights from the causal model"""
        if self.enable_causal_constraints and self.causal_model:
            return self.causal_model.get_causal_graph_summary()
        return {'status': 'causal_model_not_enabled'}
    
    async def analyze_causal_tradeoffs(self, scenarios: List[Dict]) -> List[Dict]:
        """Analyze trade-offs between different scenarios using causal model"""
        if self.enable_causal_constraints and self.causal_model:
            return await self.causal_model.analyze_tradeoffs(scenarios)
        return [{'status': 'causal_model_not_enabled'}]
    
    def trigger_stress_response(self, stress_level: float):
        if self.signal_engine:
            self.signal_engine.bind_ligand('stress_receptor', stress_level)
    
    def reset_desensitization(self):
        if self.signal_engine:
            for receptor in self.signal_engine.receptors.values():
                receptor.state = ReceptorState.RESENSITIZED
                receptor.bound_ligands = 0
    
    async def shutdown(self):
        """Graceful shutdown"""
        logger.info("Shutting down Expert Router")
        if self.carbon_manager:
            await self.carbon_manager.close()
        if self.federated_learner:
            await self.federated_learner.close()
        logger.info("Shutdown complete")
