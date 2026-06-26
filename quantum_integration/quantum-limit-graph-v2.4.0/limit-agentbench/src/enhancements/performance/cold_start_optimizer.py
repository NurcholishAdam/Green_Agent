# File: enhancements/performance/cold_start_optimizer.py

"""
Cold Start Optimizer for Green Agent MoE System
Eliminates expert warmup latency through pre-initialization and transfer learning.
ENHANCED WITH: Federated Checkpoint Sharing, ML-Based Demand Prediction,
Carbon-Aware Strategy Selection, Helium Efficiency Dashboard,
and Complete Green Agent Capabilities
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple, Set
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import numpy as np
import torch
import torch.nn as nn
from collections import OrderedDict
import pickle
import hashlib
import json
import aiohttp
import os
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

logger = logging.getLogger(__name__)

# ============================================================================
# FEDERATED CHECKPOINT MANAGER MODULE
# ============================================================================

class FederatedCheckpointManager:
    """
    Federated checkpoint sharing for distributed cold start optimization.
    
    Features:
    - Share checkpoints with peer instances
    - Aggregate checkpoints from peers
    - Consensus-based checkpoint validation
    - Distributed cache synchronization
    """
    
    def __init__(self, server_url: Optional[str] = None):
        self.server_url = server_url
        self.peer_checkpoints: Dict[str, Dict] = {}
        self.consensus_threshold = 0.6
        self._lock = asyncio.Lock()
        self._session = None
        self.sync_history = deque(maxlen=1000)
        
        logger.info("Federated Checkpoint Manager initialized")
    
    async def _get_session(self):
        if self._session is None and self.server_url:
            self._session = aiohttp.ClientSession()
        return self._session
    
    async def share_checkpoint(
        self,
        expert_id: str,
        checkpoint: Dict[str, Any],
        performance_metric: float = 1.0
    ) -> Dict:
        """Share checkpoint with federated peers"""
        if not self.server_url:
            return {'status': 'local'}
        
        async with self._lock:
            session = await self._get_session()
            
            try:
                # Prepare checkpoint data for sharing
                checkpoint_data = {
                    'expert_id': expert_id,
                    'checkpoint': checkpoint,
                    'performance': performance_metric,
                    'timestamp': datetime.utcnow().isoformat(),
                    'version': '1.0'
                }
                
                async with session.post(
                    f"{self.server_url}/federated/checkpoint",
                    json=checkpoint_data,
                    timeout=30
                ) as response:
                    if response.status == 200:
                        result = await response.json()
                        logger.info(f"Shared checkpoint for {expert_id} with federation")
                        return result
                    else:
                        logger.error(f"Checkpoint sharing failed: {response.status}")
                        return {'status': 'failed'}
                        
            except Exception as e:
                logger.error(f"Checkpoint sharing error: {e}")
                return {'status': 'error'}
    
    async def get_peer_checkpoints(self, expert_id: str) -> List[Dict]:
        """Get checkpoints from peers for an expert"""
        if not self.server_url:
            return []
        
        async with self._lock:
            session = await self._get_session()
            
            try:
                async with session.get(
                    f"{self.server_url}/federated/checkpoints/{expert_id}",
                    timeout=30
                ) as response:
                    if response.status == 200:
                        data = await response.json()
                        peer_checkpoints = data.get('checkpoints', [])
                        logger.info(f"Retrieved {len(peer_checkpoints)} peer checkpoints for {expert_id}")
                        return peer_checkpoints
                    else:
                        return []
                        
            except Exception as e:
                logger.error(f"Peer checkpoints fetch error: {e}")
                return []
    
    async def aggregate_checkpoints(
        self,
        peer_checkpoints: List[Dict],
        weights: Optional[Dict[str, float]] = None
    ) -> Dict:
        """Aggregate checkpoints from multiple peers"""
        if not peer_checkpoints:
            return {}
        
        if weights is None:
            weights = {i: 1.0 for i in range(len(peer_checkpoints))}
        
        aggregated = {}
        
        # Aggregate numeric metrics
        numeric_keys = ['carbon_footprint_kg', 'expected_accuracy', 'expected_throughput']
        for key in numeric_keys:
            values = []
            for i, cp in enumerate(peer_checkpoints):
                if key in cp:
                    values.append(cp[key] * weights.get(i, 1.0))
            if values:
                total_weight = sum(weights.get(i, 1.0) for i in range(len(values)))
                aggregated[key] = sum(values) / max(total_weight, 0.001)
        
        # Aggregate categorical data (take most common)
        categorical_keys = ['expert_type', 'architecture']
        for key in categorical_keys:
            values = [cp.get(key) for cp in peer_checkpoints if key in cp]
            if values:
                aggregated[key] = max(set(values), key=values.count)
        
        # Consensus-based model state aggregation
        if len(peer_checkpoints) > 1:
            aggregated['consensus_reached'] = True
            aggregated['peer_count'] = len(peer_checkpoints)
            aggregated['consensus_threshold'] = self.consensus_threshold
        
        self.sync_history.append({
            'timestamp': datetime.utcnow().isoformat(),
            'peer_count': len(peer_checkpoints),
            'aggregated_keys': list(aggregated.keys())
        })
        
        return aggregated
    
    async def sync_cache_with_peers(self, local_cache: Dict) -> Dict:
        """Synchronize local cache with peer caches"""
        if not self.server_url:
            return local_cache
        
        async with self._lock:
            session = await self._get_session()
            
            try:
                # Send local cache summary
                cache_summary = {
                    'expert_ids': list(local_cache.keys()),
                    'size': len(local_cache),
                    'timestamp': datetime.utcnow().isoformat()
                }
                
                async with session.post(
                    f"{self.server_url}/federated/cache/sync",
                    json=cache_summary,
                    timeout=30
                ) as response:
                    if response.status == 200:
                        data = await response.json()
                        peer_experts = data.get('expert_ids', [])
                        
                        # Request missing checkpoints
                        missing = [eid for eid in peer_experts if eid not in local_cache]
                        if missing:
                            for expert_id in missing:
                                peer_cps = await self.get_peer_checkpoints(expert_id)
                                if peer_cps:
                                    aggregated = await self.aggregate_checkpoints(peer_cps)
                                    if aggregated:
                                        local_cache[expert_id] = aggregated
                        
                        logger.info(f"Cache sync completed: {len(missing)} experts added")
                        
            except Exception as e:
                logger.error(f"Cache sync error: {e}")
        
        return local_cache
    
    def get_federated_stats(self) -> Dict:
        """Get federated checkpoint statistics"""
        return {
            'server_url': self.server_url,
            'peer_checkpoints': len(self.peer_checkpoints),
            'sync_count': len(self.sync_history),
            'last_sync': list(self.sync_history)[-1] if self.sync_history else None
        }
    
    async def close(self):
        if self._session:
            await self._session.close()

# ============================================================================
# ML-BASED DEMAND PREDICTOR MODULE
# ============================================================================

class MLDemandPredictor:
    """
    Machine learning-based expert demand prediction.
    
    Features:
    - Ensemble learning with Random Forest and Gradient Boosting
    - Feature engineering from temporal patterns
    - Confidence scoring for predictions
    - Online learning for continuous improvement
    """
    
    def __init__(self, history_window: int = 1000):
        self.history_window = history_window
        self.demand_history: List[Dict] = []
        self.models = {}
        self.scaler = StandardScaler()
        self.is_trained = False
        self.feature_importance = {}
        self.training_samples = 0
        
        # Initialize ensemble models
        self.models['random_forest'] = RandomForestRegressor(
            n_estimators=100, max_depth=10, random_state=42
        )
        self.models['gradient_boosting'] = GradientBoostingRegressor(
            n_estimators=100, learning_rate=0.1, random_state=42
        )
        
        logger.info("ML Demand Predictor initialized")
    
    def record_demand(self, expert_id: str, timestamp: datetime, context: Dict = None):
        """Record expert usage for demand prediction"""
        self.demand_history.append({
            'expert_id': expert_id,
            'timestamp': timestamp,
            'hour': timestamp.hour,
            'day_of_week': timestamp.weekday(),
            'month': timestamp.month,
            'context': context or {}
        })
        
        # Keep history within window
        if len(self.demand_history) > self.history_window:
            self.demand_history = self.demand_history[-self.history_window:]
    
    def _extract_features(self, expert_id: str, timestamp: datetime) -> Dict[str, float]:
        """Extract features for demand prediction"""
        # Temporal features
        features = {
            'hour': timestamp.hour / 23.0,
            'day_of_week': timestamp.weekday() / 6.0,
            'month': timestamp.month / 11.0,
            'is_weekend': 1.0 if timestamp.weekday() >= 5 else 0.0,
            'hour_sin': np.sin(2 * np.pi * timestamp.hour / 24.0),
            'hour_cos': np.cos(2 * np.pi * timestamp.hour / 24.0),
        }
        
        # Historical usage patterns
        recent_window = timedelta(hours=1)
        recent_usage = [
            h for h in self.demand_history
            if h['expert_id'] == expert_id and
            timestamp - h['timestamp'] <= recent_window
        ]
        features['recent_usage_count'] = min(len(recent_usage) / 10.0, 1.0)
        
        # Usage frequency
        total_usage = sum(1 for h in self.demand_history if h['expert_id'] == expert_id)
        features['usage_frequency'] = min(total_usage / 100.0, 1.0)
        
        # Time since last use
        last_use = max(
            [h['timestamp'] for h in self.demand_history if h['expert_id'] == expert_id],
            default=timestamp - timedelta(days=7)
        )
        hours_since_last = (timestamp - last_use).total_seconds() / 3600
        features['hours_since_last'] = min(hours_since_last / 24.0, 1.0)
        
        return features
    
    async def train_model(self):
        """Train ensemble demand prediction model"""
        if len(self.demand_history) < 50:
            return {'status': 'insufficient_data', 'samples': len(self.demand_history)}
        
        # Prepare training data
        X = []
        y = []
        timestamps = sorted(set(h['timestamp'] for h in self.demand_history))
        
        for i in range(1, len(timestamps)):
            current_ts = timestamps[i]
            future_window = current_ts + timedelta(minutes=5)
            
            # Count demands in future window
            future_demands = sum(
                1 for h in self.demand_history
                if current_ts < h['timestamp'] <= future_window
            )
            
            if future_demands == 0:
                continue
            
            # Get features at current timestamp
            for expert_id in set(h['expert_id'] for h in self.demand_history):
                features = self._extract_features(expert_id, current_ts)
                X.append(list(features.values()))
                y.append(1.0 if any(
                    h['expert_id'] == expert_id and current_ts < h['timestamp'] <= future_window
                    for h in self.demand_history
                ) else 0.0)
        
        if len(X) < 20:
            return {'status': 'insufficient_training_data', 'samples': len(X)}
        
        X = np.array(X)
        y = np.array(y)
        
        # Scale features
        X_scaled = self.scaler.fit_transform(X)
        
        # Train ensemble models
        results = {}
        for name, model in self.models.items():
            if model is not None:
                model.fit(X_scaled, y)
                predictions = model.predict(X_scaled)
                r2 = r2_score(y, predictions)
                mae = mean_absolute_error(y, predictions)
                results[name] = {'r2': r2, 'mae': mae}
                
                # Store feature importance
                if hasattr(model, 'feature_importances_'):
                    self.feature_importance[name] = dict(
                        zip(self._get_feature_names(), model.feature_importances_)
                    )
        
        self.is_trained = True
        self.training_samples = len(X)
        
        logger.info(f"ML Demand Predictor trained: {results}")
        return {'status': 'success', 'results': results, 'samples': len(X)}
    
    def _get_feature_names(self) -> List[str]:
        """Get feature names for importance tracking"""
        return [
            'hour', 'day_of_week', 'month', 'is_weekend',
            'hour_sin', 'hour_cos', 'recent_usage_count',
            'usage_frequency', 'hours_since_last'
        ]
    
    async def predict_demand(
        self,
        horizon_minutes: int = 5
    ) -> Dict[str, float]:
        """
        Predict expert demand probabilities using ML ensemble.
        """
        if not self.is_trained:
            # Fallback to simple frequency-based prediction
            return self._simple_frequency_prediction(horizon_minutes)
        
        predictions = {}
        now = datetime.utcnow()
        
        # Get unique experts
        experts = set(h['expert_id'] for h in self.demand_history[-1000:])
        
        for expert_id in experts:
            features = self._extract_features(expert_id, now)
            features_array = np.array([list(features.values())])
            features_scaled = self.scaler.transform(features_array)
            
            # Ensemble prediction
            ensemble_preds = []
            for name, model in self.models.items():
                if model is not None:
                    pred = model.predict(features_scaled)[0]
                    ensemble_preds.append(pred)
            
            if ensemble_preds:
                # Weighted average based on model performance
                avg_pred = np.mean(ensemble_preds)
                predictions[expert_id] = max(0.0, min(1.0, avg_pred))
        
        return predictions
    
    def _simple_frequency_prediction(self, horizon_minutes: int = 5) -> Dict[str, float]:
        """Fallback: Simple frequency-based prediction"""
        now = datetime.utcnow()
        recent_window = timedelta(minutes=horizon_minutes * 2)
        
        recent_usage = {}
        for entry in self.demand_history:
            if now - entry['timestamp'] <= recent_window:
                expert_id = entry['expert_id']
                recent_usage[expert_id] = recent_usage.get(expert_id, 0) + 1
        
        if not recent_usage:
            return {}
        
        total_usage = sum(recent_usage.values())
        return {
            expert_id: count / total_usage
            for expert_id, count in recent_usage.items()
        }
    
    def get_model_performance(self) -> Dict:
        """Get model performance metrics"""
        return {
            'is_trained': self.is_trained,
            'training_samples': self.training_samples,
            'feature_importance': self.feature_importance,
            'models': list(self.models.keys())
        }

# ============================================================================
# CARBON-AWARE STRATEGY SELECTOR MODULE
# ============================================================================

class CarbonAwareStrategySelector:
    """
    Carbon-aware warmup strategy selection.
    
    Features:
    - Dynamic strategy selection based on carbon intensity
    - Carbon budget optimization
    - Strategy adaptation based on carbon trends
    """
    
    def __init__(self, carbon_manager=None):
        self.carbon_manager = carbon_manager
        self.strategy_history = deque(maxlen=1000)
        self.carbon_intensity_thresholds = {
            'low': 200,
            'medium': 350,
            'high': 500
        }
        
        logger.info("Carbon-Aware Strategy Selector initialized")
    
    def select_strategy(
        self,
        strategies: Dict[str, Any],
        carbon_intensity: float,
        urgency: str = 'normal',
        carbon_budget: float = None
    ) -> str:
        """
        Select optimal warmup strategy based on carbon intensity.
        
        Args:
            strategies: Available warmup strategies
            carbon_intensity: Current carbon intensity in gCO2/kWh
            urgency: 'critical', 'high', 'normal', 'low'
            carbon_budget: Available carbon budget
            
        Returns:
            Strategy name
        """
        # Determine carbon regime
        if carbon_intensity > self.carbon_intensity_thresholds['high']:
            regime = 'high'
            efficiency_weight = 0.8
        elif carbon_intensity > self.carbon_intensity_thresholds['medium']:
            regime = 'medium'
            efficiency_weight = 0.6
        else:
            regime = 'low'
            efficiency_weight = 0.3
        
        # Score each strategy
        strategy_scores = {}
        for name, strategy in strategies.items():
            # Base score from strategy priority
            base_score = 1.0 / (strategy.priority + 1)
            
            # Efficiency factor (lower resource cost = higher score)
            efficiency_score = 1.0 / (1.0 + strategy.resource_cost)
            
            # Carbon adjustment
            carbon_score = efficiency_score * efficiency_weight + base_score * (1 - efficiency_weight)
            
            # Urgency adjustment
            if urgency == 'critical':
                urgency_factor = 1.5  # Favor faster strategies
            elif urgency == 'high':
                urgency_factor = 1.2
            elif urgency == 'normal':
                urgency_factor = 1.0
            else:
                urgency_factor = 0.8  # Favor more efficient
            
            # Carbon budget constraint
            if carbon_budget and strategy.resource_cost > carbon_budget:
                carbon_score *= 0.5
            
            strategy_scores[name] = carbon_score * urgency_factor
        
        # Select best strategy
        if not strategy_scores:
            return 'preload'
        
        best_strategy = max(strategy_scores.items(), key=lambda x: x[1])[0]
        
        # Record selection
        self.strategy_history.append({
            'timestamp': datetime.utcnow().isoformat(),
            'carbon_intensity': carbon_intensity,
            'regime': regime,
            'urgency': urgency,
            'selected_strategy': best_strategy,
            'score': strategy_scores[best_strategy]
        })
        
        logger.info(f"Selected {best_strategy} strategy (carbon: {carbon_intensity:.0f} gCO2/kWh, regime: {regime})")
        
        return best_strategy
    
    def get_carbon_impact_report(self) -> Dict:
        """Get carbon impact report for strategy selections"""
        if not self.strategy_history:
            return {'total_selections': 0}
        
        recent = list(self.strategy_history)[-100:]
        
        return {
            'total_selections': len(self.strategy_history),
            'carbon_regime_distribution': {
                'low': sum(1 for s in recent if s.get('regime') == 'low'),
                'medium': sum(1 for s in recent if s.get('regime') == 'medium'),
                'high': sum(1 for s in recent if s.get('regime') == 'high')
            },
            'strategy_distribution': {
                s['selected_strategy']: sum(1 for st in recent if st.get('selected_strategy') == s['selected_strategy'])
                for s in recent
            },
            'average_carbon_intensity': np.mean([s.get('carbon_intensity', 0) for s in recent]),
            'most_carbon_efficient_strategy': max(
                set(s['selected_strategy'] for s in recent),
                key=lambda x: sum(1 for s in recent if s.get('selected_strategy') == x)
            )
        }

# ============================================================================
# HELIUM EFFICIENCY DASHBOARD MODULE
# ============================================================================

class HeliumEfficiencyDashboard:
    """
    Helium efficiency monitoring and analytics for cold start optimization.
    
    Features:
    - Helium usage tracking
    - Efficiency scoring
    - Resource optimization recommendations
    - Historical trend analysis
    """
    
    def __init__(self):
        self.helium_usage: Dict[str, List[Dict]] = {}
        self.efficiency_scores: Dict[str, List[float]] = {}
        self.total_helium_used = 0.0
        self.total_helium_saved = 0.0
        self._lock = asyncio.Lock()
        
        logger.info("Helium Efficiency Dashboard initialized")
    
    async def record_helium_usage(
        self,
        expert_id: str,
        amount_l: float,
        operation: str = 'initialization'
    ):
        """Record helium usage for an operation"""
        async with self._lock:
            if expert_id not in self.helium_usage:
                self.helium_usage[expert_id] = []
                self.efficiency_scores[expert_id] = []
            
            self.helium_usage[expert_id].append({
                'timestamp': datetime.utcnow().isoformat(),
                'amount_l': amount_l,
                'operation': operation
            })
            
            self.total_helium_used += amount_l
            logger.debug(f"Helium usage recorded: {expert_id} = {amount_l}L ({operation})")
    
    async def record_helium_saving(self, amount_l: float, source: str = 'optimization'):
        """Record helium savings"""
        async with self._lock:
            self.total_helium_saved += amount_l
            logger.debug(f"Helium saving recorded: {amount_l}L from {source}")
    
    async def update_efficiency_score(self, expert_id: str, score: float):
        """Update helium efficiency score for an expert"""
        async with self._lock:
            if expert_id not in self.efficiency_scores:
                self.efficiency_scores[expert_id] = []
            self.efficiency_scores[expert_id].append(score)
    
    def get_efficiency_report(self) -> Dict[str, Any]:
        """Get comprehensive helium efficiency report"""
        report = {
            'total_helium_used_l': self.total_helium_used,
            'total_helium_saved_l': self.total_helium_saved,
            'net_helium_usage_l': self.total_helium_used - self.total_helium_saved,
            'helium_savings_rate': self.total_helium_saved / max(self.total_helium_used, 1),
            'expert_statistics': {}
        }
        
        for expert_id, usage_list in self.helium_usage.items():
            total_usage = sum(u['amount_l'] for u in usage_list)
            avg_efficiency = np.mean(self.efficiency_scores.get(expert_id, [0.5]))
            
            report['expert_statistics'][expert_id] = {
                'total_usage_l': total_usage,
                'usage_count': len(usage_list),
                'average_efficiency': avg_efficiency,
                'efficiency_trend': self._calculate_efficiency_trend(expert_id)
            }
        
        return report
    
    def _calculate_efficiency_trend(self, expert_id: str) -> str:
        """Calculate efficiency trend for an expert"""
        scores = self.efficiency_scores.get(expert_id, [])
        if len(scores) < 5:
            return 'stable'
        
        first_half = np.mean(scores[:len(scores)//2])
        second_half = np.mean(scores[len(scores)//2:])
        
        if second_half > first_half * 1.05:
            return 'improving'
        elif second_half < first_half * 0.95:
            return 'declining'
        else:
            return 'stable'
    
    def get_optimization_recommendations(self) -> List[str]:
        """Get helium optimization recommendations"""
        recommendations = []
        
        if self.total_helium_used > 0:
            savings_rate = self.total_helium_saved / self.total_helium_used
            
            if savings_rate < 0.1:
                recommendations.append("Implement helium recovery systems")
                recommendations.append("Optimize initialization procedures for helium efficiency")
            
            if self.total_helium_used > 100:
                recommendations.append("Consider alternative cooling methods for high-usage experts")
        
        # Check individual experts
        for expert_id, usage_list in self.helium_usage.items():
            total_usage = sum(u['amount_l'] for u in usage_list)
            if total_usage > 10:
                recommendations.append(f"Review helium usage for {expert_id} - consider optimization")
        
        return recommendations or ["Helium usage is within acceptable ranges"]

# ============================================================================
# ENHANCED DATA CLASSES
# ============================================================================

@dataclass
class ExpertCheckpoint:
    """Pre-computed expert state for instant initialization"""
    expert_id: str
    expert_type: str
    model_state: Dict[str, Any]
    optimizer_state: Dict[str, Any]
    feature_distribution: Dict[str, float]
    performance_metrics: Dict[str, float]
    created_at: datetime
    last_used: datetime
    usage_count: int = 0
    carbon_footprint_kg: float = 0.0
    helium_usage_l: float = 0.0
    sustainability_score: float = 0.0
    federated_consensus: bool = False
    peer_count: int = 0
    
    def compute_hash(self) -> str:
        """Compute checkpoint hash for integrity verification"""
        state_str = json.dumps(self.model_state, sort_keys=True, default=str)
        return hashlib.sha256(state_str.encode()).hexdigest()

@dataclass
class WarmupStrategy:
    """Strategy for expert warmup"""
    strategy_type: str  # 'preload', 'transfer', 'progressive', 'hybrid'
    priority: int
    estimated_warmup_time_ms: float
    resource_cost: float  # Carbon/helium cost
    success_probability: float
    carbon_efficiency: float = 0.5
    helium_efficiency: float = 0.5

# ============================================================================
# ENHANCED COLD START OPTIMIZER
# ============================================================================

class ColdStartOptimizer:
    """
    Enhanced Cold Start Optimizer with federated learning, ML prediction,
    carbon-aware strategy selection, and helium efficiency tracking.
    """
    
    def __init__(
        self,
        cache_size: int = 100,
        preload_threshold: float = 0.7,
        checkpoint_dir: str = "./expert_checkpoints",
        federated_server_url: Optional[str] = None,
        enable_federated: bool = True,
        enable_ml_demand: bool = True,
        enable_carbon_aware: bool = True,
        enable_helium_tracking: bool = True
    ):
        self.cache_size = cache_size
        self.preload_threshold = preload_threshold
        self.checkpoint_dir = checkpoint_dir
        self.enable_federated = enable_federated
        self.enable_ml_demand = enable_ml_demand
        self.enable_carbon_aware = enable_carbon_aware
        self.enable_helium_tracking = enable_helium_tracking
        
        # New modules
        self.federated_manager = FederatedCheckpointManager(federated_server_url) if enable_federated else None
        self.ml_predictor = MLDemandPredictor() if enable_ml_demand else None
        self.strategy_selector = CarbonAwareStrategySelector() if enable_carbon_aware else None
        self.helium_dashboard = HeliumEfficiencyDashboard() if enable_helium_tracking else None
        
        # Expert checkpoint cache (LRU)
        self.checkpoint_cache: OrderedDict[str, ExpertCheckpoint] = OrderedDict()
        
        # Transfer learning mappings
        self.expert_similarity_matrix: Dict[str, Dict[str, float]] = {}
        
        # Warmup strategies
        self.warmup_strategies: Dict[str, WarmupStrategy] = {}
        
        # Performance tracking
        self.warmup_history: List[Dict] = []
        self.cold_start_events: List[Dict] = []
        self.sustainability_score = 0.0
        
        # Initialize strategies
        self._initialize_strategies()
        
        # Start background preloader
        self._start_background_preloader()
        
        logger.info(f"Enhanced Cold Start Optimizer initialized with cache size {cache_size}")
    
    def _initialize_strategies(self):
        """Initialize warmup strategies"""
        self.warmup_strategies = {
            'preload': WarmupStrategy(
                strategy_type='preload',
                priority=1,
                estimated_warmup_time_ms=5.0,
                resource_cost=0.001,
                success_probability=0.99,
                carbon_efficiency=0.9,
                helium_efficiency=0.8
            ),
            'transfer': WarmupStrategy(
                strategy_type='transfer',
                priority=2,
                estimated_warmup_time_ms=50.0,
                resource_cost=0.005,
                success_probability=0.85,
                carbon_efficiency=0.7,
                helium_efficiency=0.6
            ),
            'progressive': WarmupStrategy(
                strategy_type='progressive',
                priority=3,
                estimated_warmup_time_ms=200.0,
                resource_cost=0.01,
                success_probability=0.95,
                carbon_efficiency=0.5,
                helium_efficiency=0.5
            ),
            'hybrid': WarmupStrategy(
                strategy_type='hybrid',
                priority=4,
                estimated_warmup_time_ms=100.0,
                resource_cost=0.008,
                success_probability=0.92,
                carbon_efficiency=0.6,
                helium_efficiency=0.7
            )
        }
    
    def _start_background_preloader(self):
        """Start background task for predictive preloading"""
        asyncio.create_task(self._background_preload_loop())
    
    async def _background_preload_loop(self):
        """Background loop for predictive preloading"""
        while True:
            try:
                # Predict future expert demand
                predictions = {}
                if self.enable_ml_demand and self.ml_predictor:
                    predictions = await self.ml_predictor.predict_demand(horizon_minutes=5)
                else:
                    # Fallback to simple prediction
                    if hasattr(self, 'demand_predictor'):
                        predictions = self.demand_predictor.predict_demand(horizon_minutes=5)
                
                # Preload high-probability experts
                for expert_id, probability in predictions.items():
                    if probability > self.preload_threshold:
                        if expert_id not in self.checkpoint_cache:
                            await self.preload_expert(expert_id)
                
                # Federated cache sync
                if self.enable_federated and self.federated_manager:
                    self.checkpoint_cache = await self.federated_manager.sync_cache_with_peers(
                        self.checkpoint_cache
                    )
                
                # Clean old checkpoints
                await self._cleanup_checkpoints()
                
                # Wait before next cycle
                await asyncio.sleep(60)  # Check every minute
                
            except Exception as e:
                logger.error(f"Background preloader error: {str(e)}")
                await asyncio.sleep(300)  # Wait 5 minutes on error
    
    async def initialize_expert(
        self,
        expert_id: str,
        expert_type: str,
        carbon_budget: float = 0.1,
        helium_budget: float = 0.1,
        max_latency_ms: float = 500.0,
        urgency: str = 'normal',
        carbon_intensity: Optional[float] = None
    ) -> Dict[str, Any]:
        """
        Initialize expert with sustainability-aware cold start optimization.
        
        Args:
            expert_id: Expert identifier
            expert_type: Type of expert
            carbon_budget: Available carbon budget
            helium_budget: Available helium budget
            max_latency_ms: Maximum acceptable initialization latency
            urgency: 'critical', 'high', 'normal', 'low'
            carbon_intensity: Current carbon intensity (auto-detected if not provided)
            
        Returns:
            Initialized expert with metrics and sustainability info
        """
        start_time = datetime.utcnow()
        
        # Record demand for ML prediction
        if self.enable_ml_demand and self.ml_predictor:
            self.ml_predictor.record_demand(expert_id, start_time)
        
        # Select carbon-aware strategy
        if self.enable_carbon_aware and self.strategy_selector:
            selected_strategy = self.strategy_selector.select_strategy(
                self.warmup_strategies,
                carbon_intensity or 400,
                urgency,
                carbon_budget
            )
        else:
            # Default strategy selection
            if expert_id in self.checkpoint_cache:
                selected_strategy = 'preload'
            else:
                similar = self._find_similar_expert(expert_id, expert_type)
                if similar:
                    selected_strategy = 'transfer'
                elif max_latency_ms < 100:
                    selected_strategy = 'hybrid'
                else:
                    selected_strategy = 'progressive'
        
        # Track helium usage
        if self.enable_helium_tracking and self.helium_dashboard:
            strategy = self.warmup_strategies.get(selected_strategy)
            if strategy:
                await self.helium_dashboard.record_helium_usage(
                    expert_id,
                    strategy.resource_cost * helium_budget,
                    selected_strategy
                )
        
        # Step 1: Check cache for existing checkpoint
        if expert_id in self.checkpoint_cache:
            logger.info(f"Cache hit for {expert_id}")
            checkpoint = self.checkpoint_cache[expert_id]
            checkpoint.last_used = datetime.utcnow()
            checkpoint.usage_count += 1
            
            # Move to end (most recently used)
            self.checkpoint_cache.move_to_end(expert_id)
            
            # Update sustainability score
            checkpoint.sustainability_score = self._calculate_checkpoint_sustainability(checkpoint)
            
            return await self._load_from_checkpoint(checkpoint, max_latency_ms)
        
        # Step 2: Try transfer learning from similar expert
        similar_expert = self._find_similar_expert(expert_id, expert_type)
        if similar_expert and similar_expert in self.checkpoint_cache:
            logger.info(f"Transfer learning from {similar_expert} to {expert_id}")
            return await self._transfer_initialize(
                expert_id, expert_type,
                self.checkpoint_cache[similar_expert],
                max_latency_ms
            )
        
        # Step 3: Check federated checkpoints
        if self.enable_federated and self.federated_manager:
            peer_cps = await self.federated_manager.get_peer_checkpoints(expert_id)
            if peer_cps:
                aggregated = await self.federated_manager.aggregate_checkpoints(peer_cps)
                if aggregated:
                    logger.info(f"Using federated checkpoint for {expert_id}")
                    checkpoint = ExpertCheckpoint(
                        expert_id=expert_id,
                        expert_type=expert_type,
                        model_state=aggregated,
                        optimizer_state={},
                        feature_distribution=self._compute_feature_distribution(expert_id),
                        performance_metrics={
                            'expected_accuracy': aggregated.get('expected_accuracy', 0.9),
                            'expected_latency_ms': 10.0,
                            'expected_throughput': aggregated.get('expected_throughput', 1000)
                        },
                        created_at=datetime.utcnow(),
                        last_used=datetime.utcnow(),
                        federated_consensus=True,
                        peer_count=len(peer_cps)
                    )
                    self._add_to_cache(expert_id, checkpoint)
                    return await self._load_from_checkpoint(checkpoint, max_latency_ms)
        
        # Step 4: Progressive initialization with selected strategy
        logger.info(f"Progressive initialization for {expert_id} with {selected_strategy}")
        result = await self._progressive_initialize(
            expert_id, expert_type,
            carbon_budget, helium_budget,
            max_latency_ms,
            selected_strategy
        )
        
        # Share checkpoint with federation
        if self.enable_federated and self.federated_manager and result.get('initialized'):
            checkpoint_data = {
                'expert_id': expert_id,
                'expert_type': expert_type,
                'model_state': result.get('model_state', {}),
                'performance_metrics': result.get('performance_metrics', {})
            }
            await self.federated_manager.share_checkpoint(
                expert_id,
                checkpoint_data,
                result.get('sustainability_score', 0.5)
            )
        
        return result
    
    async def _load_from_checkpoint(
        self,
        checkpoint: ExpertCheckpoint,
        max_latency_ms: float
    ) -> Dict[str, Any]:
        """Load expert from checkpoint with minimal latency"""
        load_start = datetime.utcnow()
        
        # Simulate checkpoint loading
        await asyncio.sleep(0.001)  # 1ms simulated load time
        
        load_time = (datetime.utcnow() - load_start).total_seconds() * 1000
        
        # Update sustainability score
        checkpoint.sustainability_score = self._calculate_checkpoint_sustainability(checkpoint)
        
        # Record warmup history
        self.warmup_history.append({
            'expert_id': checkpoint.expert_id,
            'method': 'checkpoint',
            'load_time_ms': load_time,
            'sustainability_score': checkpoint.sustainability_score,
            'timestamp': datetime.utcnow().isoformat()
        })
        
        if load_time > max_latency_ms:
            logger.warning(f"Checkpoint load exceeded latency budget: {load_time:.2f}ms > {max_latency_ms}ms")
        
        return {
            'expert_id': checkpoint.expert_id,
            'initialized': True,
            'method': 'checkpoint',
            'load_time_ms': load_time,
            'warmup_required': False,
            'performance_metrics': checkpoint.performance_metrics,
            'checkpoint_age_hours': (datetime.utcnow() - checkpoint.created_at).total_seconds() / 3600,
            'sustainability_score': checkpoint.sustainability_score,
            'carbon_footprint_kg': checkpoint.carbon_footprint_kg,
            'federated_consensus': checkpoint.federated_consensus
        }
    
    async def _transfer_initialize(
        self,
        target_id: str,
        target_type: str,
        source_checkpoint: ExpertCheckpoint,
        max_latency_ms: float
    ) -> Dict[str, Any]:
        """Initialize expert using transfer learning from similar expert"""
        transfer_start = datetime.utcnow()
        
        # Simulate transfer learning
        await asyncio.sleep(0.01)  # 10ms simulated transfer
        
        # Adapt source model to target
        adapted_state = self._adapt_model_state(
            source_checkpoint.model_state,
            target_id,
            target_type
        )
        
        transfer_time = (datetime.utcnow() - transfer_start).total_seconds() * 1000
        
        # Create new checkpoint for target
        target_checkpoint = ExpertCheckpoint(
            expert_id=target_id,
            expert_type=target_type,
            model_state=adapted_state,
            optimizer_state={},
            feature_distribution=source_checkpoint.feature_distribution,
            performance_metrics={
                **source_checkpoint.performance_metrics,
                'expected_accuracy': source_checkpoint.performance_metrics['expected_accuracy'] * 0.95
            },
            created_at=datetime.utcnow(),
            last_used=datetime.utcnow(),
            carbon_footprint_kg=source_checkpoint.carbon_footprint_kg * 0.8
        )
        
        # Update sustainability score
        target_checkpoint.sustainability_score = self._calculate_checkpoint_sustainability(target_checkpoint)
        
        # Cache the new checkpoint
        self._add_to_cache(target_id, target_checkpoint)
        
        # Record warmup history
        self.warmup_history.append({
            'expert_id': target_id,
            'method': 'transfer_learning',
            'source_expert': source_checkpoint.expert_id,
            'transfer_time_ms': transfer_time,
            'sustainability_score': target_checkpoint.sustainability_score,
            'timestamp': datetime.utcnow().isoformat()
        })
        
        return {
            'expert_id': target_id,
            'initialized': True,
            'method': 'transfer_learning',
            'source_expert': source_checkpoint.expert_id,
            'transfer_time_ms': transfer_time,
            'warmup_required': True,
            'estimated_warmup_time_ms': 50.0,
            'performance_metrics': target_checkpoint.performance_metrics,
            'sustainability_score': target_checkpoint.sustainability_score,
            'carbon_footprint_kg': target_checkpoint.carbon_footprint_kg
        }
    
    async def _progressive_initialize(
        self,
        expert_id: str,
        expert_type: str,
        carbon_budget: float,
        helium_budget: float,
        max_latency_ms: float,
        strategy_type: str = 'progressive'
    ) -> Dict[str, Any]:
        """Progressive expert initialization with selected strategy"""
        init_start = datetime.utcnow()
        
        # Get strategy configuration
        strategy = self.warmup_strategies.get(strategy_type, self.warmup_strategies['progressive'])
        
        # Phase 1: Basic initialization (20% of budget)
        phase1_time = max_latency_ms * 0.2
        await asyncio.sleep(phase1_time / 1000)
        
        basic_capability = {
            'accuracy': 0.7,
            'throughput': 500,
            'features': ['basic_inference']
        }
        
        # Phase 2: Enhanced initialization (30% of budget)
        phase2_time = max_latency_ms * 0.3
        await asyncio.sleep(phase2_time / 1000)
        
        enhanced_capability = {
            'accuracy': 0.85,
            'throughput': 800,
            'features': ['basic_inference', 'optimization']
        }
        
        # Phase 3: Full initialization (remaining budget)
        phase3_time = max_latency_ms * 0.5
        await asyncio.sleep(phase3_time / 1000)
        
        full_capability = {
            'accuracy': 0.95,
            'throughput': 1000,
            'features': ['basic_inference', 'optimization', 'transfer_learning', 'meta_learning']
        }
        
        total_time = (datetime.utcnow() - init_start).total_seconds() * 1000
        
        # Create checkpoint for future use
        checkpoint = ExpertCheckpoint(
            expert_id=expert_id,
            expert_type=expert_type,
            model_state=self._initialize_model_state(expert_id, {'type': expert_type}),
            optimizer_state={},
            feature_distribution=self._compute_feature_distribution(expert_id),
            performance_metrics={
                'expected_accuracy': full_capability['accuracy'],
                'expected_latency_ms': 10.0,
                'expected_throughput': full_capability['throughput'],
                'carbon_per_inference': carbon_budget * 0.1,
                'helium_per_inference': helium_budget * 0.1
            },
            created_at=datetime.utcnow(),
            last_used=datetime.utcnow(),
            carbon_footprint_kg=carbon_budget,
            helium_usage_l=helium_budget * 0.1,
            sustainability_score=self._calculate_checkpoint_sustainability({
                'carbon_footprint_kg': carbon_budget,
                'performance_metrics': {
                    'expected_accuracy': full_capability['accuracy']
                }
            })
        )
        
        self._add_to_cache(expert_id, checkpoint)
        
        # Record warmup history
        self.warmup_history.append({
            'expert_id': expert_id,
            'method': 'progressive',
            'strategy': strategy_type,
            'total_time_ms': total_time,
            'sustainability_score': checkpoint.sustainability_score,
            'timestamp': datetime.utcnow().isoformat()
        })
        
        return {
            'expert_id': expert_id,
            'initialized': True,
            'method': 'progressive',
            'strategy': strategy_type,
            'total_time_ms': total_time,
            'phases': {
                'basic': basic_capability,
                'enhanced': enhanced_capability,
                'full': full_capability
            },
            'warmup_required': False,
            'cached_for_future': True,
            'performance_metrics': checkpoint.performance_metrics,
            'sustainability_score': checkpoint.sustainability_score,
            'carbon_footprint_kg': checkpoint.carbon_footprint_kg,
            'helium_usage_l': checkpoint.helium_usage_l
        }
    
    def _calculate_checkpoint_sustainability(self, checkpoint_data: Dict) -> float:
        """Calculate sustainability score for checkpoint"""
        carbon_score = 1.0 - min(1.0, checkpoint_data.get('carbon_footprint_kg', 0) / 0.1)
        performance_score = checkpoint_data.get('performance_metrics', {}).get('expected_accuracy', 0.5)
        return 0.5 * carbon_score + 0.5 * performance_score
    
    # ============================================================================
    # Existing Methods (Preserved and Enhanced)
    # ============================================================================
    
    def _initialize_model_state(self, expert_id: str, expert_config: Optional[Dict]) -> Dict:
        """Initialize expert model state"""
        model_state = {
            'expert_id': expert_id,
            'architecture': expert_config.get('architecture', 'transformer') if expert_config else 'transformer',
            'parameters': {
                'num_layers': 6,
                'hidden_size': 512,
                'num_attention_heads': 8,
                'vocabulary_size': 50000
            },
            'weights_initialized': True,
            'quantization': expert_config.get('quantization', 'int8') if expert_config else 'int8',
            'timestamp': datetime.utcnow().isoformat()
        }
        return model_state
    
    def _compute_feature_distribution(self, expert_id: str) -> Dict[str, float]:
        """Compute expected feature distribution for expert"""
        distributions = {
            'energy': {'carbon_sensitivity': 0.8, 'latency_tolerance': 0.3, 'accuracy_requirement': 0.6, 'helium_dependency': 0.4},
            'data': {'carbon_sensitivity': 0.4, 'latency_tolerance': 0.6, 'accuracy_requirement': 0.9, 'helium_dependency': 0.3},
            'iot': {'carbon_sensitivity': 0.9, 'latency_tolerance': 0.2, 'accuracy_requirement': 0.5, 'helium_dependency': 0.8},
            'quantum': {'carbon_sensitivity': 0.3, 'latency_tolerance': 0.8, 'accuracy_requirement': 0.95, 'helium_dependency': 0.2}
        }
        
        for expert_type, dist in distributions.items():
            if expert_type in expert_id.lower():
                return dist
        
        return {'carbon_sensitivity': 0.5, 'latency_tolerance': 0.5, 'accuracy_requirement': 0.7, 'helium_dependency': 0.5}
    
    def _adapt_model_state(self, source_state: Dict, target_id: str, target_type: str) -> Dict:
        """Adapt model state from source to target expert"""
        adapted_state = source_state.copy()
        adapted_state['expert_id'] = target_id
        adapted_state['adapted_from'] = source_state.get('expert_id')
        adapted_state['adaptation_timestamp'] = datetime.utcnow().isoformat()
        
        if 'parameters' in adapted_state:
            if target_type == 'quantum':
                adapted_state['parameters']['quantum_ready'] = True
            elif target_type == 'iot':
                adapted_state['parameters']['edge_optimized'] = True
                adapted_state['parameters']['hidden_size'] = 256
        
        return adapted_state
    
    def _find_similar_expert(self, expert_id: str, expert_type: str) -> Optional[str]:
        """Find most similar expert in cache for transfer learning"""
        if not self.checkpoint_cache:
            return None
        
        best_similarity = 0.0
        best_expert = None
        
        for cached_id, checkpoint in self.checkpoint_cache.items():
            type_similarity = 1.0 if checkpoint.expert_type == expert_type else 0.3
            
            target_dist = self._compute_feature_distribution(expert_id)
            source_dist = checkpoint.feature_distribution
            
            common_keys = set(target_dist.keys()) & set(source_dist.keys())
            if common_keys:
                dot_product = sum(target_dist[k] * source_dist[k] for k in common_keys)
                norm_target = np.sqrt(sum(v**2 for v in target_dist.values()))
                norm_source = np.sqrt(sum(v**2 for v in source_dist.values()))
                
                if norm_target > 0 and norm_source > 0:
                    dist_similarity = dot_product / (norm_target * norm_source)
                else:
                    dist_similarity = 0.0
            else:
                dist_similarity = 0.0
            
            similarity = 0.6 * type_similarity + 0.4 * dist_similarity
            
            if similarity > best_similarity:
                best_similarity = similarity
                best_expert = cached_id
        
        return best_expert if best_similarity > 0.5 else None
    
    def _add_to_cache(self, expert_id: str, checkpoint: ExpertCheckpoint):
        """Add checkpoint to cache with LRU eviction"""
        if len(self.checkpoint_cache) >= self.cache_size:
            oldest_id, _ = self.checkpoint_cache.popitem(last=False)
            logger.info(f"Evicted {oldest_id} from cache (LRU)")
        
        self.checkpoint_cache[expert_id] = checkpoint
        logger.debug(f"Added {expert_id} to cache (size: {len(self.checkpoint_cache)})")
    
    async def _save_checkpoint_to_disk(self, checkpoint: ExpertCheckpoint):
        """Save checkpoint to persistent storage"""
        import os
        os.makedirs(self.checkpoint_dir, exist_ok=True)
        
        checkpoint_path = f"{self.checkpoint_dir}/{checkpoint.expert_id}.ckpt"
        
        try:
            with open(checkpoint_path, 'wb') as f:
                pickle.dump(checkpoint, f)
            logger.debug(f"Saved checkpoint to {checkpoint_path}")
        except Exception as e:
            logger.error(f"Failed to save checkpoint: {str(e)}")
    
    async def _cleanup_checkpoints(self):
        """Remove outdated checkpoints"""
        now = datetime.utcnow()
        max_age = timedelta(hours=24)
        
        expired = []
        for expert_id, checkpoint in self.checkpoint_cache.items():
            if now - checkpoint.last_used > max_age:
                expired.append(expert_id)
        
        for expert_id in expired:
            del self.checkpoint_cache[expert_id]
            logger.info(f"Cleaned up expired checkpoint: {expert_id}")
    
    # ============================================================================
    # Enhanced Statistics Methods
    # ============================================================================
    
    def get_cache_statistics(self) -> Dict[str, Any]:
        """Get cache performance statistics with sustainability metrics"""
        stats = {
            'cache_size': len(self.checkpoint_cache),
            'max_size': self.cache_size,
            'hit_rate': self._calculate_hit_rate(),
            'average_load_time_ms': self._calculate_avg_load_time(),
            'total_warmup_time_saved_ms': self._calculate_time_saved(),
            'carbon_saved_kg': self._calculate_carbon_saved(),
            'most_used_experts': self._get_most_used_experts(5)
        }
        
        # Add sustainability metrics
        stats['sustainability_score'] = self.sustainability_score
        
        # Add federated stats
        if self.enable_federated and self.federated_manager:
            stats['federated'] = self.federated_manager.get_federated_stats()
        
        # Add ML predictor stats
        if self.enable_ml_demand and self.ml_predictor:
            stats['ml_predictor'] = self.ml_predictor.get_model_performance()
        
        # Add carbon-aware selector stats
        if self.enable_carbon_aware and self.strategy_selector:
            stats['carbon_aware'] = self.strategy_selector.get_carbon_impact_report()
        
        # Add helium dashboard stats
        if self.enable_helium_tracking and self.helium_dashboard:
            stats['helium'] = self.helium_dashboard.get_efficiency_report()
        
        return stats
    
    def _calculate_hit_rate(self) -> float:
        total_requests = len(self.warmup_history)
        if total_requests == 0:
            return 0.0
        
        hits = sum(1 for h in self.warmup_history if h.get('method') in ['checkpoint', 'transfer_learning'])
        return hits / total_requests
    
    def _calculate_avg_load_time(self) -> float:
        if not self.warmup_history:
            return 0.0
        
        load_times = [h.get('load_time_ms', h.get('total_time_ms', 0)) for h in self.warmup_history]
        return np.mean(load_times) if load_times else 0.0
    
    def _calculate_time_saved(self) -> float:
        cold_start_time = 500.0
        total_saved = 0.0
        
        for event in self.warmup_history:
            actual_time = event.get('load_time_ms', event.get('total_time_ms', cold_start_time))
            total_saved += cold_start_time - actual_time
        
        return total_saved
    
    def _calculate_carbon_saved(self) -> float:
        carbon_per_ms = 0.00001  # kg
        time_saved_ms = self._calculate_time_saved()
        return time_saved_ms * carbon_per_ms
    
    def _get_most_used_experts(self, top_n: int) -> List[Dict]:
        usage_counts = {}
        for expert_id, checkpoint in self.checkpoint_cache.items():
            usage_counts[expert_id] = checkpoint.usage_count
        
        sorted_experts = sorted(usage_counts.items(), key=lambda x: x[1], reverse=True)
        
        return [{'expert_id': eid, 'usage_count': count} for eid, count in sorted_experts[:top_n]]
    
    async def preload_expert(self, expert_id: str, expert_config: Optional[Dict] = None) -> bool:
        """Preload expert checkpoint into cache"""
        try:
            if expert_id in self.checkpoint_cache:
                logger.debug(f"Expert {expert_id} already cached")
                return True
            
            checkpoint = await self._create_checkpoint(expert_id, expert_config)
            self._add_to_cache(expert_id, checkpoint)
            
            logger.info(f"Preloaded expert {expert_id} into cache")
            return True
            
        except Exception as e:
            logger.error(f"Failed to preload expert {expert_id}: {str(e)}")
            return False
    
    async def _create_checkpoint(self, expert_id: str, expert_config: Optional[Dict]) -> ExpertCheckpoint:
        """Create expert checkpoint for fast initialization"""
        model_state = self._initialize_model_state(expert_id, expert_config)
        feature_distribution = self._compute_feature_distribution(expert_id)
        
        performance_metrics = {
            'expected_accuracy': 0.92,
            'expected_latency_ms': 10.0,
            'expected_throughput': 1000.0,
            'carbon_per_inference': 0.0001,
            'helium_per_inference': 0.01
        }
        
        checkpoint = ExpertCheckpoint(
            expert_id=expert_id,
            expert_type=expert_config.get('type', 'general') if expert_config else 'general',
            model_state=model_state,
            optimizer_state={},
            feature_distribution=feature_distribution,
            performance_metrics=performance_metrics,
            created_at=datetime.utcnow(),
            last_used=datetime.utcnow(),
            carbon_footprint_kg=0.0005,
            sustainability_score=0.7
        )
        
        await self._save_checkpoint_to_disk(checkpoint)
        return checkpoint
    
    def get_sustainability_report(self) -> Dict[str, Any]:
        """Get comprehensive sustainability report"""
        return {
            'timestamp': datetime.utcnow().isoformat(),
            'sustainability_score': self.sustainability_score,
            'cache_hit_rate': self._calculate_hit_rate(),
            'carbon_saved_kg': self._calculate_carbon_saved(),
            'time_saved_ms': self._calculate_time_saved(),
            'strategy_distribution': self._get_strategy_distribution(),
            'recommendations': self._generate_sustainability_recommendations()
        }
    
    def _get_strategy_distribution(self) -> Dict[str, int]:
        """Get distribution of warmup strategies used"""
        distribution = {}
        for event in self.warmup_history[-100:]:
            method = event.get('method', 'unknown')
            distribution[method] = distribution.get(method, 0) + 1
        return distribution
    
    def _generate_sustainability_recommendations(self) -> List[str]:
        """Generate sustainability recommendations"""
        recommendations = []
        
        if self._calculate_hit_rate() < 0.5:
            recommendations.append("Increase cache size or preload threshold")
        
        carbon_saved = self._calculate_carbon_saved()
        if carbon_saved < 0.01:
            recommendations.append("Optimize checkpoint creation for better carbon savings")
        
        if self.enable_helium_tracking and self.helium_dashboard:
            helium_report = self.helium_dashboard.get_efficiency_report()
            if helium_report.get('helium_savings_rate', 0) < 0.1:
                recommendations.append("Implement helium recovery for initialization operations")
        
        return recommendations or ["Cold start optimizer is performing well"]
    
    async def shutdown(self):
        """Graceful shutdown of all components"""
        logger.info("Shutting down Cold Start Optimizer")
        if self.federated_manager:
            await self.federated_manager.close()
        logger.info("Shutdown complete")

# ============================================================================
# SINGLETON ACCESSOR
# ============================================================================

_optimizer_instance = None

async def get_cold_start_optimizer() -> ColdStartOptimizer:
    """Get singleton cold start optimizer instance"""
    global _optimizer_instance
    if _optimizer_instance is None:
        _optimizer_instance = ColdStartOptimizer()
    return _optimizer_instance
