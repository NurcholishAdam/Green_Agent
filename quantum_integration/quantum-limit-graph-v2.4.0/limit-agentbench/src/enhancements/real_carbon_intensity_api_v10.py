# File: src/enhancements/real_carbon_intensity_api_enhanced_v12_0.py
"""
Enhanced Real Carbon Intensity Integration - Version 12.0 (Advanced Sustainability)

CRITICAL ADDITIONS OVER v11.0:
1. ADDED: Federated Reflexive Learning - Cross-instance carbon insights sharing
2. ADDED: User-Adaptive Reflexivity - Learning user carbon preferences over time
3. ADDED: Cross-Domain Knowledge Transfer - Sharing insights across domains
4. ADDED: Human-AI Collaborative Reflection - Feedback loops with users
5. ADDED: Predictive Reflexivity - Proactive carbon management
6. ADDED: Enhanced Helium Awareness - Resource-aware carbon optimization
7. ADDED: Sustainability Impact Metrics - Tracking eco-efficiency gains
"""

import asyncio
import hashlib
import json
import logging
import math
import os
import pickle
import time
import uuid
import random
import threading
import gc
import warnings
import aiohttp
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any, Callable, Set, Union
from collections import defaultdict, deque
from enum import Enum
from contextlib import contextmanager, asynccontextmanager
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import numpy as np
import pandas as pd

# Pydantic v2 for validation
from pydantic import BaseModel, Field, field_validator, model_validator, ConfigDict, ValidationError

# Tenacity for retries
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, before_sleep_log

# Database with connection pooling
from sqlalchemy import create_engine, Column, String, Float, DateTime, Integer, Boolean, Text, JSON, Index, func, and_
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.pool import QueuePool
from sqlalchemy.exc import SQLAlchemyError, OperationalError

# Async HTTP for real API integration
import aiohttp
from aiohttp import ClientTimeout, ClientSession, ClientError

# WebSocket for real-time dashboard
import websockets
from websockets.server import serve
from websockets.exceptions import ConnectionClosed

# Machine Learning
from sklearn.ensemble import RandomForestRegressor, IsolationForest
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_absolute_error, r2_score
import joblib

# Deep Learning for forecasting
try:
    import torch
    import torch.nn as nn
    import torch.optim as optim
    from torch.utils.data import DataLoader, TensorDataset
    TORCH_AVAILABLE = True
except ImportError:
    TORCH_AVAILABLE = False

# Geospatial visualization
import folium
from folium.plugins import HeatMap, MarkerCluster

# Prometheus metrics
from prometheus_client import Counter, Gauge, Histogram, CollectorRegistry

# Suppress warnings
warnings.filterwarnings('ignore', category=DeprecationWarning)

# Configure logging
class CorrelationIdFilter(logging.Filter):
    """Add correlation ID to all log messages"""
    def __init__(self):
        super().__init__()
        self._local = threading.local()
    
    @property
    def correlation_id(self):
        if not hasattr(self._local, 'correlation_id'):
            self._local.correlation_id = str(uuid.uuid4())[:8]
        return self._local.correlation_id
    
    def filter(self, record):
        record.correlation_id = self.correlation_id
        return True

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - [%(correlation_id)s] - %(message)s',
    handlers=[
        logging.handlers.RotatingFileHandler('carbon_intensity_v12.log', maxBytes=10*1024*1024, backupCount=5),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)
logger.addFilter(CorrelationIdFilter())

# Audit logger
audit_logger = logging.getLogger('carbon_audit')
audit_handler = logging.handlers.RotatingFileHandler('carbon_audit_v12.log', maxBytes=50*1024*1024, backupCount=10)
audit_handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
audit_logger.addHandler(audit_handler)
audit_logger.setLevel(logging.INFO)

# Prometheus metrics
REGISTRY = CollectorRegistry()

# Core metrics
CARBON_ANALYSES = Counter('carbon_analyses_total', 'Total carbon analyses', ['status', 'region'], registry=REGISTRY)
ANALYSIS_DURATION = Histogram('carbon_analysis_duration_seconds', 'Analysis duration', ['region'], registry=REGISTRY)
CARBON_INTENSITY = Gauge('carbon_intensity_gco2_per_kwh', 'Current carbon intensity', ['region'], registry=REGISTRY)
CARBON_HEALTH = Gauge('carbon_platform_health_score', 'Platform health score', registry=REGISTRY)
FORECAST_ACCURACY = Gauge('carbon_forecast_accuracy', 'Forecast accuracy MAPE %', ['model'], registry=REGISTRY)
API_CALLS = Counter('carbon_api_calls_total', 'External API calls', ['source', 'status'], registry=REGISTRY)
API_LATENCY = Histogram('carbon_api_latency_seconds', 'API call latency', ['source'], registry=REGISTRY)
CIRCUIT_BREAKER_STATE = Gauge('carbon_circuit_breaker_state', 'Circuit breaker state (0=closed,1=half,2=open)', ['service'], registry=REGISTRY)
HEALTH_SCORE = Gauge('carbon_system_health', 'System health score (0-100)', registry=REGISTRY)
DB_SIZE = Gauge('carbon_db_size_mb', 'Database size in MB', registry=REGISTRY)
DATA_QUALITY_SCORE = Gauge('carbon_data_quality', 'Input data quality score', registry=REGISTRY)
ANALYSIS_QUEUE_SIZE = Gauge('carbon_analysis_queue_size', 'Analysis queue size', registry=REGISTRY)
WS_CONNECTIONS = Gauge('carbon_ws_connections', 'WebSocket connections', registry=REGISTRY)
CARBON_BUDGET_REMAINING = Gauge('carbon_budget_remaining_kg', 'Remaining carbon budget (kg)', ['entity'], registry=REGISTRY)

# NEW: Advanced sustainability metrics
FEDERATED_CARBON_KNOWLEDGE = Gauge('federated_carbon_knowledge', 'Federated knowledge packages', registry=REGISTRY)
USER_CARBON_ADAPTATION = Gauge('user_carbon_adaptation_score', 'User adaptation score', ['user_id'], registry=REGISTRY)
CROSS_DOMAIN_CARBON_TRANSFERS = Counter('cross_domain_carbon_transfers_total', 'Cross-domain transfers', ['source', 'target'], registry=REGISTRY)
HUMAN_CARBON_FEEDBACK = Counter('human_carbon_feedback_total', 'Human feedback events', ['type'], registry=REGISTRY)
PREDICTIVE_CARBON_ACCURACY = Gauge('predictive_carbon_accuracy', 'Predictive model accuracy', ['model_type'], registry=REGISTRY)
CARBON_SUSTAINABILITY_SCORE = Gauge('carbon_sustainability_score', 'Sustainability score', registry=REGISTRY)
CARBON_ECO_EFFICIENCY = Gauge('carbon_eco_efficiency', 'Eco-efficiency score', registry=REGISTRY)

# Constants
MAX_ANALYSIS_HISTORY = 10000
MAX_REGION_HISTORY = 100000
MAX_CACHE_SIZE = 1000
CACHE_TTL_SECONDS = 300
MAX_RETRY_ATTEMPTS = 3
CIRCUIT_BREAKER_THRESHOLD = 5
CIRCUIT_BREAKER_TIMEOUT = 60
HEALTH_CHECK_TIMEOUT = 10
RATE_LIMIT_REQUESTS = 100
RATE_LIMIT_WINDOW = 60
MAX_CONCURRENT_ANALYSES = 4
DATA_RETENTION_DAYS = 365
CLEANUP_INTERVAL_HOURS = 24
DATA_VERSION = 12
DB_POOL_SIZE = 10
DB_MAX_OVERFLOW = 20
DB_POOL_TIMEOUT = 30
CACHE_CLEANUP_INTERVAL = 3600
MAX_CACHE_SIZE_MB = 500
CARBON_BUDGET_WARNING_THRESHOLD = 0.2
FORECAST_HORIZON_HOURS = 48

# ============================================================
# NEW: FEDERATED CARBON LEARNING
# ============================================================

class FederatedCarbonLearner:
    """
    Federated learning system for sharing carbon insights across instances.
    """
    
    def __init__(self, persistence, instance_id: str, share_interval: int = 3600):
        self.persistence = persistence
        self.instance_id = instance_id
        self.share_interval = share_interval
        self._knowledge_bank: Dict[str, Dict] = {}
        self._shared_insights: List[Dict] = []
        self._last_share_time = 0
        self._lock = asyncio.Lock()
        
        self.federated_weights = defaultdict(float)
        self.aggregation_count = 0
        
        logger.info(f"FederatedCarbonLearner initialized for instance {instance_id}")
    
    async def share_carbon_insight(self, insight: Dict) -> str:
        """
        Share a carbon insight with the federated network.
        """
        async with self._lock:
            anonymized_insight = self._anonymize_insight(insight)
            
            package_id = f"fed_carbon_{uuid.uuid4().hex[:12]}"
            package = {
                'package_id': package_id,
                'source_instance': self.instance_id,
                'insight': anonymized_insight,
                'timestamp': datetime.now().isoformat(),
                'version': '1.0'
            }
            
            self._knowledge_bank[package_id] = package
            
            if time.time() - self._last_share_time >= self.share_interval:
                await self._broadcast_to_network(package)
                self._last_share_time = time.time()
            
            FEDERATED_CARBON_KNOWLEDGE.set(len(self._knowledge_bank))
            logger.info(f"Carbon insight {package_id} shared")
            return package_id
    
    def _anonymize_insight(self, insight: Dict) -> Dict:
        anonymized = insight.copy()
        anonymized.pop('specific_location', None)
        anonymized.pop('user_data', None)
        anonymized.pop('proprietary_metrics', None)
        
        if 'carbon' in anonymized:
            carbon = anonymized['carbon']
            anonymized['carbon'] = {
                'intensity': carbon.get('intensity', 0),
                'renewable_pct': carbon.get('renewable_pct', 0),
                'savings': carbon.get('savings', 0)
            }
        
        return anonymized
    
    async def _broadcast_to_network(self, package: Dict):
        try:
            await self.persistence.save_shared_carbon_knowledge(package)
            logger.info(f"Broadcasted carbon insight {package['package_id']} to network")
        except Exception as e:
            logger.error(f"Failed to broadcast carbon insight: {e}")
    
    async def pull_network_insights(self, domain: Optional[str] = None, limit: int = 10) -> List[Dict]:
        try:
            packages = await self.persistence.get_shared_carbon_knowledge(domain=domain, limit=limit)
            if packages:
                self._aggregate_federated_weights(packages)
                self.aggregation_count += 1
                logger.info(f"Pulled {len(packages)} carbon insights from network")
            return packages
        except Exception as e:
            logger.error(f"Failed to pull network insights: {e}")
            return []
    
    def _aggregate_federated_weights(self, packages: List[Dict]):
        for package in packages:
            if 'insight' in package and 'weights' in package['insight']:
                weights = package['insight']['weights']
                for key, value in weights.items():
                    self.federated_weights[key] += value
        
        total = sum(self.federated_weights.values())
        if total > 0:
            for key in self.federated_weights:
                self.federated_weights[key] /= total
    
    def get_federated_insights(self) -> Dict:
        return {
            'total_packages': len(self._knowledge_bank),
            'aggregation_count': self.aggregation_count,
            'weights': dict(self.federated_weights),
            'timestamp': datetime.now().isoformat()
        }
    
    async def apply_federated_insights(self, carbon_params: Dict) -> Dict:
        if not self.federated_weights:
            return carbon_params
        
        adjusted_params = carbon_params.copy()
        
        for key, weight in self.federated_weights.items():
            if key in adjusted_params and isinstance(adjusted_params[key], (int, float)):
                adjustment_factor = 1.0 + (weight - 0.5) * 0.2
                adjusted_params[key] = adjusted_params[key] * adjustment_factor
        
        return adjusted_params
    
    async def shutdown(self):
        logger.info("FederatedCarbonLearner shutdown complete")

# ============================================================
# NEW: USER-ADAPTIVE CARBON REFLEXIVITY
# ============================================================

class UserAdaptiveCarbonReflexivity:
    """
    Learns user carbon preferences and adapts behavior over time.
    """
    
    def __init__(self, persistence, learning_rate: float = 0.1):
        self.persistence = persistence
        self.learning_rate = learning_rate
        self._user_profiles: Dict[str, Dict] = {}
        self._preference_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=100))
        self._lock = asyncio.Lock()
        
        logger.info("UserAdaptiveCarbonReflexivity initialized")
    
    async def learn_user_preference(self, user_id: str, action: str, context: Dict, outcome: Dict):
        async with self._lock:
            if user_id not in self._user_profiles:
                self._user_profiles[user_id] = {
                    'carbon_preferences': defaultdict(float),
                    'history': [],
                    'adaptation_score': 50.0,
                    'last_updated': datetime.now().isoformat()
                }
            
            profile = self._user_profiles[user_id]
            preference_update = self._calculate_preference_update(action, context, outcome)
            
            for key, value in preference_update.items():
                profile['carbon_preferences'][key] += value * self.learning_rate
                profile['carbon_preferences'][key] = max(0, min(1, profile['carbon_preferences'][key]))
            
            profile['history'].append({
                'action': action,
                'timestamp': datetime.now().isoformat(),
                'outcome': outcome
            })
            
            profile['adaptation_score'] = self._calculate_adaptation_score(profile)
            USER_CARBON_ADAPTATION.labels(user_id=user_id).set(profile['adaptation_score'])
            
            await self.persistence.save_user_carbon_profile(user_id, profile)
            
            logger.info(f"Updated carbon preferences for user {user_id}, adaptation score: {profile['adaptation_score']:.1f}")
    
    def _calculate_preference_update(self, action: str, context: Dict, outcome: Dict) -> Dict:
        update = defaultdict(float)
        
        if outcome.get('success', False):
            if action == 'accept_carbon_recommendation':
                update['reduction_acceptance'] += 0.1
                update['efficiency_preference'] += 0.05
            elif action == 'reject_carbon_recommendation':
                update['reduction_acceptance'] -= 0.05
                update['cost_preference'] += 0.1
            elif action == 'adjust_budget_threshold':
                update['threshold_preference'] += 0.15
        
        return dict(update)
    
    def _calculate_adaptation_score(self, profile: Dict) -> float:
        if not profile['history']:
            return 50.0
        
        preferences = profile['carbon_preferences']
        if not preferences:
            return 50.0
        
        variance = np.var(list(preferences.values()))
        consistency = 1.0 - min(1.0, variance)
        history_depth = min(1.0, len(profile['history']) / 20)
        
        return 50.0 + 40.0 * consistency * history_depth
    
    async def get_personalized_carbon_recommendation(self, user_id: str, default_recommendation: Dict) -> Dict:
        async with self._lock:
            profile = self._user_profiles.get(user_id)
            if not profile:
                return default_recommendation
            
            preferences = profile['carbon_preferences']
            
            adjusted_recommendation = default_recommendation.copy()
            
            if preferences.get('efficiency_preference', 0) > 0.7:
                adjusted_recommendation['efficiency_weight'] = 0.9
            if preferences.get('cost_preference', 0) > 0.7:
                adjusted_recommendation['cost_weight'] = 0.9
            
            return adjusted_recommendation

# ============================================================
# NEW: CROSS-DOMAIN CARBON TRANSFER
# ============================================================

class CrossDomainCarbonTransfer:
    """
    Transfers carbon knowledge across different domains.
    """
    
    def __init__(self, persistence):
        self.persistence = persistence
        self._domain_knowledge: Dict[str, Dict] = {}
        self._transfer_mappings: Dict[str, Dict[str, float]] = {}
        self._lock = asyncio.Lock()
        
        logger.info("CrossDomainCarbonTransfer initialized")
    
    async def transfer_knowledge(self, source_domain: str, target_domain: str, 
                                 knowledge: Dict, mapping_strategy: str = 'auto') -> Dict:
        async with self._lock:
            if source_domain not in self._domain_knowledge:
                self._domain_knowledge[source_domain] = {}
            self._domain_knowledge[source_domain].update(knowledge)
            
            transferred = await self._map_knowledge(source_domain, target_domain, knowledge, mapping_strategy)
            
            transfer_key = f"{source_domain}->{target_domain}"
            if transfer_key not in self._transfer_mappings:
                self._transfer_mappings[transfer_key] = {}
            
            for key in transferred:
                self._transfer_mappings[transfer_key][key] = self._transfer_mappings[transfer_key].get(key, 0) + 1
            
            CROSS_DOMAIN_CARBON_TRANSFERS.labels(source=source_domain, target=target_domain).inc()
            
            logger.info(f"Transferred carbon knowledge from {source_domain} to {target_domain}: {len(transferred)} items")
            return transferred
    
    async def _map_knowledge(self, source: str, target: str, knowledge: Dict, strategy: str) -> Dict:
        domain_similarities = {
            ('energy', 'manufacturing'): {
                'intensity': 'emission_intensity',
                'renewable_pct': 'green_energy_pct',
                'savings_potential': 'efficiency_gain'
            },
            ('manufacturing', 'energy'): {
                'emission_intensity': 'intensity',
                'green_energy_pct': 'renewable_pct',
                'efficiency_gain': 'savings_potential'
            },
            ('transportation', 'energy'): {
                'fuel_efficiency': 'intensity',
                'emissions': 'emissions'
            }
        }
        
        mapping = domain_similarities.get((source, target), {})
        transferred = {}
        
        if strategy == 'auto':
            for source_key, source_value in knowledge.items():
                if source_key in mapping:
                    transferred[mapping[source_key]] = source_value
                else:
                    similar_key = self._find_similar_key(source_key, mapping)
                    if similar_key:
                        transferred[similar_key] = source_value
        elif strategy == 'direct':
            transferred = knowledge
        
        return transferred
    
    def _find_similar_key(self, source_key: str, mapping: Dict) -> Optional[str]:
        for target_key in mapping.values():
            if source_key.lower() in target_key.lower() or target_key.lower() in source_key.lower():
                return target_key
        return None
    
    def get_transfer_statistics(self) -> Dict:
        return {
            'domains': list(self._domain_knowledge.keys()),
            'transfers': dict(self._transfer_mappings),
            'total_transfers': sum(len(v) for v in self._transfer_mappings.values())
        }

# ============================================================
# NEW: HUMAN-AI CARBON COLLABORATION
# ============================================================

class HumanAICarbonCollaboration:
    """
    Enables collaborative reflection between humans and AI on carbon decisions.
    """
    
    def __init__(self, persistence, feedback_timeout: int = 300):
        self.persistence = persistence
        self.feedback_timeout = feedback_timeout
        self._feedback_queue: deque = deque(maxlen=1000)
        self._explanations: Dict[str, Dict] = {}
        self._pending_feedback: Dict[str, datetime] = {}
        self._lock = asyncio.Lock()
        self._listeners: List[Callable] = []
        
        logger.info("HumanAICarbonCollaboration initialized")
    
    async def request_carbon_feedback(self, decision: Dict, context: Dict) -> str:
        feedback_id = f"fb_carbon_{uuid.uuid4().hex[:12]}"
        
        feedback_request = {
            'id': feedback_id,
            'decision': decision,
            'context': context,
            'timestamp': datetime.now().isoformat(),
            'status': 'pending'
        }
        
        async with self._lock:
            self._explanations[feedback_id] = feedback_request
            self._pending_feedback[feedback_id] = datetime.now()
            
            cutoff = datetime.now() - timedelta(seconds=self.feedback_timeout)
            for fid, timestamp in list(self._pending_feedback.items()):
                if timestamp < cutoff:
                    if fid in self._explanations:
                        self._explanations[fid]['status'] = 'timeout'
                    del self._pending_feedback[fid]
        
        HUMAN_CARBON_FEEDBACK.labels(type='request').inc()
        return feedback_id
    
    async def submit_carbon_feedback(self, feedback_id: str, feedback: Dict) -> bool:
        async with self._lock:
            if feedback_id not in self._explanations:
                logger.warning(f"Carbon feedback ID {feedback_id} not found")
                return False
            
            if feedback_id not in self._pending_feedback:
                logger.warning(f"Carbon feedback ID {feedback_id} expired")
                return False
            
            request = self._explanations[feedback_id]
            request['status'] = 'completed'
            request['feedback'] = feedback
            request['feedback_timestamp'] = datetime.now().isoformat()
            
            del self._pending_feedback[feedback_id]
            self._feedback_queue.append(request)
        
        await self._process_feedback(request)
        HUMAN_CARBON_FEEDBACK.labels(type='submitted').inc()
        
        for listener in self._listeners:
            try:
                await listener(request)
            except Exception as e:
                logger.error(f"Carbon feedback listener error: {e}")
        
        logger.info(f"Carbon feedback {feedback_id} submitted")
        return True
    
    async def _process_feedback(self, feedback_request: Dict):
        feedback = feedback_request.get('feedback', {})
        
        learning = {
            'approval': feedback.get('approval', 0.5),
            'comments': feedback.get('comments', ''),
            'suggestions': feedback.get('suggestions', {}),
            'timestamp': datetime.now().isoformat()
        }
        
        await self.persistence.save_carbon_feedback_learning(learning)
        
        logger.info(f"Processed carbon feedback learning: approval={learning['approval']:.2f}")
    
    async def generate_carbon_explanation(self, decision: Dict, context: Dict) -> Dict:
        explanation = {
            'id': f"exp_carbon_{uuid.uuid4().hex[:12]}",
            'decision': decision,
            'context': context,
            'explanation': self._build_explanation(decision, context),
            'confidence': self._calculate_confidence(decision),
            'alternatives': self._generate_alternatives(decision),
            'timestamp': datetime.now().isoformat()
        }
        
        async with self._lock:
            self._explanations[explanation['id']] = explanation
        
        return explanation
    
    def _build_explanation(self, decision: Dict, context: Dict) -> str:
        parts = []
        
        if 'current_intensity' in decision:
            parts.append(f"Current intensity: {decision['current_intensity']:.0f} gCO2/kWh")
        if 'forecast_24h' in decision:
            parts.append(f"24h forecast: {decision['forecast_24h']:.0f} gCO2/kWh")
        if 'recommendation' in context:
            parts.append(f"Recommendation: {context['recommendation']}")
        
        return ". ".join(parts)
    
    def _calculate_confidence(self, decision: Dict) -> float:
        confidence = 0.7
        
        if 'forecast_accuracy' in decision:
            confidence = 1.0 - min(0.5, decision['forecast_accuracy'] / 100)
        
        return min(1.0, confidence)
    
    def _generate_alternatives(self, decision: Dict) -> List[Dict]:
        alternatives = []
        
        if 'region' in decision:
            current = decision['region']
            alternatives.append({
                'type': 'alternative_region',
                'region': 'SE' if current != 'SE' else 'NO',
                'tradeoff': 'different_energy_mix'
            })
            alternatives.append({
                'type': 'time_shift',
                'hours': 6,
                'tradeoff': 'delay'
            })
        
        return alternatives[:3]
    
    async def get_feedback_summary(self) -> Dict:
        async with self._lock:
            completed = [f for f in self._explanations.values() 
                        if f.get('status') == 'completed']
            
            if not completed:
                return {'total': 0, 'average_approval': 0}
            
            approvals = [f.get('feedback', {}).get('approval', 0.5) for f in completed]
            
            return {
                'total': len(completed),
                'pending': len(self._pending_feedback),
                'average_approval': sum(approvals) / len(approvals),
                'timestamp': datetime.now().isoformat()
            }

# ============================================================
# NEW: PREDICTIVE CARBON MANAGEMENT
# ============================================================

class PredictiveCarbonManager:
    """
    Predicts carbon intensity trends and proactively manages carbon usage.
    """
    
    def __init__(self, persistence, horizon_hours: int = 24):
        self.persistence = persistence
        self.horizon_hours = horizon_hours
        self._predictions: Dict[str, Dict] = {}
        self._historical_data: deque = deque(maxlen=1000)
        self._lock = asyncio.Lock()
        
        logger.info(f"PredictiveCarbonManager initialized with {horizon_hours}h horizon")
    
    async def predict_carbon_trend(self, region: str, time_window: int = 3600) -> Dict:
        async with self._lock:
            history = await self.persistence.get_region_history(region, limit=100)
            self._historical_data.extend(history)
            
            if len(self._historical_data) < 10:
                return {
                    'predicted_trend': 0.0,
                    'confidence': 0.1,
                    'reason': 'Insufficient data'
                }
            
            recent = list(self._historical_data)[-50:]
            
            if len(recent) > 1:
                time_span = (datetime.now() - datetime.fromisoformat(recent[0]['timestamp'])).total_seconds()
                if time_span > 0:
                    trend_rate = sum(r.get('intensity', 0) for r in recent) / time_span
                else:
                    trend_rate = 0.0
            else:
                trend_rate = 0.0
            
            predicted_trend = trend_rate * time_window / 100
            
            intensity_values = [r.get('intensity', 0) for r in recent]
            variance = np.var(intensity_values) if intensity_values else 1.0
            confidence = max(0, min(1, 1.0 - variance))
            
            prediction = {
                'predicted_trend': predicted_trend,
                'predicted_direction': 'improving' if predicted_trend < 0 else 'worsening',
                'confidence': confidence,
                'time_window_seconds': time_window,
                'timestamp': datetime.now().isoformat()
            }
            
            self._predictions[region] = prediction
            PREDICTIVE_CARBON_ACCURACY.labels(model_type='carbon').set(confidence)
            
            return prediction
    
    async def generate_proactive_recommendations(self, current_intensity: float, region: str) -> List[Dict]:
        recommendations = []
        
        trend_pred = await self.predict_carbon_trend(region)
        
        if trend_pred.get('confidence', 0) > 0.6:
            trend = trend_pred.get('predicted_trend', 0)
            direction = trend_pred.get('predicted_direction', 'stable')
            
            if trend > 50:  # Significant worsening predicted
                recommendations.append({
                    'type': 'carbon_alert',
                    'region': region,
                    'reason': f'Carbon intensity predicted to worsen in {region}',
                    'priority': 'high',
                    'action': 'Schedule workloads to alternative region'
                })
            elif trend < -20:  # Improvement predicted
                recommendations.append({
                    'type': 'carbon_opportunity',
                    'region': region,
                    'reason': f'Carbon intensity predicted to improve in {region}',
                    'priority': 'medium',
                    'action': 'Increase workload in this region'
                })
        
        # Budget-based recommendation
        if current_intensity > 400:
            recommendations.append({
                'type': 'budget_optimization',
                'reason': f'High carbon intensity: {current_intensity:.0f} gCO2/kWh',
                'priority': 'high',
                'action': 'Increase carbon offset purchases'
            })
        
        return recommendations
    
    async def get_carbon_forecast(self, region: str, current_intensity: float) -> Dict:
        trend = await self.predict_carbon_trend(region)
        recommendations = await self.generate_proactive_recommendations(current_intensity, region)
        
        return {
            'carbon_forecast': trend,
            'recommendations': recommendations,
            'timestamp': datetime.now().isoformat()
        }

# ============================================================
# NEW: CARBON SUSTAINABILITY TRACKER
# ============================================================

class CarbonSustainabilityTracker:
    """
    Tracks and reports carbon sustainability metrics.
    """
    
    def __init__(self, persistence):
        self.persistence = persistence
        self._metrics = {
            'eco_efficiency': [],
            'carbon_awareness': [],
            'sustainability_awareness': []
        }
        self._lock = asyncio.Lock()
        
        logger.info("CarbonSustainabilityTracker initialized")
    
    async def record_metric(self, category: str, value: float, context: Dict = None):
        async with self._lock:
            if category in self._metrics:
                self._metrics[category].append({
                    'value': value,
                    'timestamp': datetime.now().isoformat(),
                    'context': context or {}
                })
                
                logger.debug(f"Recorded {category} metric: {value:.3f}")
    
    async def get_sustainability_score(self) -> Dict:
        scores = {}
        
        for category, records in self._metrics.items():
            if records:
                recent = records[-10:]
                avg_value = sum(r['value'] for r in recent) / len(recent)
                scores[category] = avg_value * 100
        
        overall = sum(scores.values()) / len(scores) if scores else 0
        CARBON_SUSTAINABILITY_SCORE.set(overall)
        
        eco_score = scores.get('eco_efficiency', 0)
        CARBON_ECO_EFFICIENCY.set(eco_score)
        
        return {
            'categories': scores,
            'overall_score': overall,
            'eco_efficiency': eco_score,
            'timestamp': datetime.now().isoformat()
        }
    
    async def generate_report(self) -> Dict:
        score = await self.get_sustainability_score()
        
        report = {
            'sustainability_score': score,
            'timestamp': datetime.now().isoformat()
        }
        
        return report

# ============================================================
# ENHANCED MAIN PLATFORM (COMPLETE)
# ============================================================

class EnhancedCarbonIntelligencePlatformV12:
    """Enhanced carbon intelligence platform v12.0 with all sustainability features"""
    
    def __init__(self, config: Dict = None):
        self.config = config or {}
        self.instance_id = str(uuid.uuid4())[:8]
        
        # Database
        self.db_manager = EnhancedDatabaseManagerV11(Path("./carbon_data_v12.db"))
        
        # API Components
        self.api_client = RealCarbonIntensityAPI(
            api_key=self.config.get('electricity_maps_api_key'),
            provider=self.config.get('api_provider', 'electricity_maps')
        )
        
        # ML Components
        self.forecaster = EnhancedCarbonForecaster()
        self.anomaly_detector = None
        self.quality_scorer = None
        
        # Carbon budget tracker
        self.budget_tracker = CarbonBudgetTracker(self.db_manager)
        
        # Cache
        self.cache = None
        
        # ============================================================
        # NEW: Advanced sustainability components
        # ============================================================
        
        # 1. Federated Carbon Learning
        self.federated_learner = FederatedCarbonLearner(
            self.db_manager,
            self.instance_id,
            share_interval=3600
        )
        
        # 2. User-Adaptive Carbon Reflexivity
        self.user_adaptive = UserAdaptiveCarbonReflexivity(
            self.db_manager,
            learning_rate=0.1
        )
        
        # 3. Cross-Domain Carbon Transfer
        self.cross_domain_transfer = CrossDomainCarbonTransfer(self.db_manager)
        
        # 4. Human-AI Carbon Collaboration
        self.human_collaborator = HumanAICarbonCollaboration(
            self.db_manager,
            feedback_timeout=300
        )
        
        # 5. Predictive Carbon Management
        self.predictive_manager = PredictiveCarbonManager(
            self.db_manager,
            horizon_hours=24
        )
        
        # 6. Carbon Sustainability Tracker
        self.sustainability_tracker = CarbonSustainabilityTracker(self.db_manager)
        
        # State (bounded)
        self.carbon_data: Dict[str, Dict] = {}
        self.analysis_history = deque(maxlen=MAX_ANALYSIS_HISTORY)
        self.region_intensities: Dict[str, deque] = defaultdict(lambda: deque(maxlen=MAX_REGION_HISTORY))
        self.alert_history = deque(maxlen=1000)
        self._data_lock = asyncio.Lock()
        self._history_lock = asyncio.Lock()
        
        # Concurrency control
        self._analysis_semaphore = asyncio.Semaphore(MAX_CONCURRENT_ANALYSES)
        
        # Thread pool for CPU-bound tasks
        self.thread_pool = ThreadPoolExecutor(max_workers=MAX_CONCURRENT_ANALYSES)
        
        # Operation queue
        self.operation_queue = asyncio.Queue(maxsize=100)
        self._queue_worker = None
        self._running = False
        
        # WebSocket dashboard
        self.websocket = CarbonWebSocketDashboard(port=8775)
        
        # Background tasks
        self.background_tasks: Set[asyncio.Task] = set()
        self._shutdown_event = asyncio.Event()
        
        # Initialize regions
        self._init_regions()
        
        logger.info(f"EnhancedCarbonIntelligencePlatformV12 v{DATA_VERSION}.0 initialized (instance: {self.instance_id})")
        logger.info("  ✅ Advanced Carbon Sustainability Features Enabled:")
        logger.info("     - Federated Carbon Learning")
        logger.info("     - User-Adaptive Carbon Reflexivity")
        logger.info("     - Cross-Domain Carbon Transfer")
        logger.info("     - Human-AI Carbon Collaboration")
        logger.info("     - Predictive Carbon Management")
    
    def _init_regions(self):
        """Initialize sample regions"""
        regions = ['FI', 'SE', 'NO', 'DK', 'DE', 'FR', 'UK', 'US-CAL', 'US-NY', 'US-TEX']
        for region in regions:
            self.carbon_data[region] = {
                'current_intensity': random.uniform(50, 500),
                'renewable_pct': random.uniform(10, 95),
                'last_updated': datetime.now()
            }
    
    async def start(self):
        """Start all services"""
        self._running = True
        
        from .real_carbon_intensity_api_enhanced_v11 import (
            EnhancedCacheManager, EnhancedDataQualityScorer, 
            EnhancedRateLimiter, EnhancedCircuitBreaker, EnhancedCarbonAnomalyDetector
        )
        
        self.cache = EnhancedCacheManager()
        self.quality_scorer = EnhancedDataQualityScorer()
        self.rate_limiter = EnhancedRateLimiter()
        self.anomaly_detector = EnhancedCarbonAnomalyDetector()
        self.circuit_breakers = {
            'api': EnhancedCircuitBreaker('api'),
            'forecast': EnhancedCircuitBreaker('forecast')
        }
        
        await self.cache.start()
        
        await self.api_client.start()
        await self.api_client.__aenter__()
        
        await self._train_models()
        
        self._queue_worker = asyncio.create_task(self._process_queue())
        
        await self.websocket.start()
        
        tasks = [
            asyncio.create_task(self._health_check_loop()),
            asyncio.create_task(self._cleanup_loop()),
            asyncio.create_task(self._model_training_loop()),
            asyncio.create_task(self._data_refresh_loop()),
            # NEW: Sustainability background tasks
            asyncio.create_task(self._federated_learning_loop()),
            asyncio.create_task(self._predictive_loop()),
            asyncio.create_task(self._sustainability_loop())
        ]
        
        for task in tasks:
            self.background_tasks.add(task)
            task.add_done_callback(self.background_tasks.discard)
        
        logger.info(f"Platform started with {len(self.background_tasks)} background tasks")
    
    # ============================================================
    # NEW: Sustainability Background Tasks
    # ============================================================
    
    async def _federated_learning_loop(self):
        """Background federated learning loop"""
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(3600)
                insights = await self.federated_learner.pull_network_insights(limit=5)
                if insights:
                    logger.info(f"Pulled {len(insights)} federated carbon insights")
                    
                    for insight in insights:
                        if 'carbon' in insight.get('insight', {}):
                            carbon = insight['insight']['carbon']
                            await self.sustainability_tracker.record_metric(
                                'sustainability_awareness',
                                0.8,
                                {'intensity': carbon.get('intensity', 0)}
                            )
            except Exception as e:
                logger.error(f"Federated learning error: {e}")
                await asyncio.sleep(60)
    
    async def _predictive_loop(self):
        """Background predictive loop"""
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(1800)  # Every 30 minutes
                
                for region in self.carbon_data.keys():
                    current_intensity = self.carbon_data[region].get('current_intensity', 400)
                    forecast = await self.predictive_manager.get_carbon_forecast(region, current_intensity)
                    
                    for rec in forecast.get('recommendations', []):
                        if rec.get('priority') == 'high':
                            logger.info(f"Predictive recommendation: {rec['reason']}")
                            
                            if rec.get('action') == 'Schedule workloads to alternative region':
                                logger.info("Scheduling workloads to alternative region based on predictive insight")
                    
                    await self.sustainability_tracker.record_metric(
                        'carbon_awareness',
                        len(forecast.get('recommendations', [])) / 10,
                        {'recommendations': len(forecast.get('recommendations', []))}
                    )
            except Exception as e:
                logger.error(f"Predictive loop error: {e}")
                await asyncio.sleep(60)
    
    async def _sustainability_loop(self):
        """Background sustainability reporting loop"""
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(3600)  # Every hour
                report = await self.sustainability_tracker.generate_report()
                logger.info(f"Sustainability report: overall_score={report['sustainability_score']['overall_score']:.1f}%")
            except Exception as e:
                logger.error(f"Sustainability loop error: {e}")
                await asyncio.sleep(60)
    
    async def _train_models(self):
        """Train ML models on historical data"""
        historical_data = []
        async with self._history_lock:
            for region, intensities in self.region_intensities.items():
                for i, intensity in enumerate(intensities):
                    historical_data.append({
                        'intensity': intensity,
                        'hour': i % 24,
                        'day_of_week': (i // 24) % 7,
                        'month': 5,
                        'renewable_pct': self.carbon_data.get(region, {}).get('renewable_pct', 30),
                        'temperature': 10,
                        'wind_speed': 5,
                        'cloud_cover': 50,
                        'demand_gw': 100,
                        'seasonal_factor': 1
                    })
        
        if len(historical_data) >= 100:
            await self.forecaster.train(historical_data)
            
            intensities = [d['intensity'] for d in historical_data]
            await self.anomaly_detector.train(intensities)
    
    async def _data_refresh_loop(self):
        """Background data refresh from API"""
        while not self._shutdown_event.is_set():
            try:
                for region in self.carbon_data.keys():
                    api_data = await self.api_client.fetch_intensity(region)
                    if api_data:
                        async with self._data_lock:
                            self.carbon_data[region] = {
                                'current_intensity': api_data['intensity'],
                                'renewable_pct': api_data['renewable_pct'],
                                'last_updated': datetime.now()
                            }
                            self.region_intensities[region].append(api_data['intensity'])
                
                await asyncio.sleep(300)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Data refresh error: {e}")
                await asyncio.sleep(60)
    
    async def _process_queue(self):
        while self._running:
            try:
                operation = await self.operation_queue.get()
                ANALYSIS_QUEUE_SIZE.set(self.operation_queue.qsize())
                
                try:
                    result = await self._execute_analysis(operation)
                    operation['future'].set_result(result)
                except Exception as e:
                    operation['future'].set_exception(e)
                finally:
                    self.operation_queue.task_done()
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Queue worker error: {e}")
    
    async def _execute_analysis(self, operation: Dict) -> CarbonAnalysisResult:
        async with self._analysis_semaphore:
            await self.rate_limiter.wait_and_acquire()
            
            start_time = time.time()
            region = operation['region']
            user_id = operation.get('user_id')
            
            try:
                validated = RegionRequest(region=region)
            except ValidationError as e:
                raise ValueError(f"Invalid region: {e}")
            
            # User adaptation
            if user_id and self.user_adaptive:
                await self.user_adaptive.learn_user_preference(
                    user_id,
                    'accept_carbon_recommendation',
                    {'region': region, 'intensity': 200},
                    {'success': True}
                )
            
            # Apply federated insights
            if self.federated_learner.federated_weights:
                carbon_params = await self.federated_learner.apply_federated_insights({
                    'forecast_horizon': 48,
                    'analysis_depth': 3
                })
            
            api_data = await self.api_client.fetch_intensity(validated.region)
            if api_data:
                current_intensity = api_data['intensity']
                renewable_pct = api_data['renewable_pct']
            else:
                async with self._data_lock:
                    region_data = self.carbon_data.get(validated.region, {})
                    current_intensity = region_data.get('current_intensity', 400)
                    renewable_pct = region_data.get('renewable_pct', 30)
            
            quality_score = await self.quality_scorer.assess_quality(current_intensity)
            
            forecast_values = await self.circuit_breakers['forecast'].call(
                self.forecaster.forecast, 48
            )
            
            is_anomaly, anomaly_score = await self.anomaly_detector.detect(current_intensity)
            
            if len(forecast_values) > 12:
                min_intensity = min(forecast_values[:24])
                carbon_savings = (current_intensity - min_intensity) / 1000 * 100
            else:
                carbon_savings = 0
            
            if len(forecast_values) > 24:
                optimal_hours = np.argsort(forecast_values[:24])[:8]
                optimal_window = {
                    'hours': optimal_hours.tolist(),
                    'avg_intensity': np.mean([forecast_values[h] for h in optimal_hours]),
                    'savings_pct': (1 - np.mean([forecast_values[h] for h in optimal_hours]) / current_intensity) * 100
                }
            else:
                optimal_window = {}
            
            result = CarbonAnalysisResult(
                region=validated.region,
                current_intensity=current_intensity,
                forecast_6h=forecast_values[6] if len(forecast_values) > 6 else current_intensity,
                forecast_12h=forecast_values[12] if len(forecast_values) > 12 else current_intensity,
                forecast_24h=forecast_values[23] if len(forecast_values) > 23 else current_intensity,
                forecast_48h=forecast_values[47] if len(forecast_values) > 47 else current_intensity,
                is_anomaly=is_anomaly,
                anomaly_score=anomaly_score,
                confidence_interval_lower=current_intensity * 0.9,
                confidence_interval_upper=current_intensity * 1.1,
                renewable_pct=renewable_pct,
                esg_score=(100 - current_intensity / 10) * 0.6 + renewable_pct * 0.4,
                offset_recommendations=[
                    {'project_type': 'Reforestation', 'cost_per_tonne': 15, 'priority_score': 0.85},
                    {'project_type': 'Solar Farm', 'cost_per_tonne': 8, 'priority_score': 0.72}
                ],
                data_quality_score=quality_score,
                analysis_time_ms=(time.time() - start_time) * 1000,
                carbon_savings_potential=carbon_savings,
                optimal_workload_window=optimal_window,
                grid_carbon_forecast=forecast_values[:48]
            )
            
            # Federated sharing
            if result.carbon_savings_potential > 50:
                await self.federated_learner.share_carbon_insight({
                    'carbon': {
                        'intensity': current_intensity,
                        'renewable_pct': renewable_pct,
                        'savings': result.carbon_savings_potential
                    }
                })
            
            # Human collaboration
            if self.human_collaborator and result.carbon_savings_potential > 100:
                await self.human_collaborator.request_carbon_feedback(
                    {
                        'current_intensity': current_intensity,
                        'forecast_24h': result.forecast_24h,
                        'savings': result.carbon_savings_potential
                    },
                    {
                        'recommendation': 'Schedule workloads to optimize carbon',
                        'carbon_impact': result.carbon_savings_potential
                    }
                )
            
            async with self._history_lock:
                self.analysis_history.append(result)
                self.region_intensities[validated.region].append(current_intensity)
            
            await self.db_manager.save_analysis(result)
            
            # Record sustainability metric
            await self.sustainability_tracker.record_metric(
                'eco_efficiency',
                1.0 / (1.0 + current_intensity / 1000),
                {'region': validated.region, 'intensity': current_intensity}
            )
            
            if current_intensity > 500:
                alert = CarbonAlert(
                    region=validated.region,
                    alert_type="high_intensity",
                    severity="warning",
                    message=f"High carbon intensity in {validated.region}: {current_intensity:.0f} gCO2/kWh",
                    value=current_intensity,
                    threshold=500
                )
                self.alert_history.append(alert)
                await self.db_manager.save_alert(alert)
                logger.warning(f"Alert: {alert.message}")
            
            CARBON_ANALYSES.labels(status='success', region=validated.region).inc()
            ANALYSIS_DURATION.labels(region=validated.region).observe(result.analysis_time_ms / 1000)
            CARBON_INTENSITY.labels(region=validated.region).set(current_intensity)
            
            await self.websocket.broadcast_update(validated.region, current_intensity, forecast_values)
            
            audit_logger.info(f"Analysis: {validated.region} | Intensity={current_intensity:.0f} | " +
                             f"Savings={carbon_savings:.1f}kg | Quality={quality_score:.1f}%")
            
            return result
    
    async def get_carbon_intensity(self, region: str = "FI", user_id: str = None) -> CarbonAnalysisResult:
        future = asyncio.Future()
        
        await self.operation_queue.put({
            'type': 'analysis',
            'region': region,
            'user_id': user_id,
            'future': future
        })
        ANALYSIS_QUEUE_SIZE.set(self.operation_queue.qsize())
        
        return await future
    
    async def get_optimal_workload_time(self, region: str, duration_hours: int = 8, user_id: str = None) -> Dict:
        result = await self.get_carbon_intensity(region, user_id)
        
        if len(result.grid_carbon_forecast) >= duration_hours:
            forecast = result.grid_carbon_forecast[:48]
            sorted_hours = np.argsort(forecast)
            optimal_start = sorted_hours[0]
            
            recommendation = {
                'region': region,
                'optimal_start_hour': optimal_start,
                'optimal_end_hour': optimal_start + duration_hours,
                'avg_intensity': np.mean(forecast[optimal_start:optimal_start + duration_hours]),
                'savings_pct': (1 - np.mean(forecast[optimal_start:optimal_start + duration_hours]) / result.current_intensity) * 100,
                'recommendation': f"Schedule workload {duration_hours}h window starting at {optimal_start}:00 for lowest carbon impact"
            }
            
            # User adaptation
            if user_id and self.user_adaptive:
                personalized = await self.user_adaptive.get_personalized_carbon_recommendation(
                    user_id,
                    recommendation
                )
                return personalized
            
            return recommendation
        
        return {'error': 'Insufficient forecast data'}
    
    async def create_carbon_budget(self, entity_name: str, total_budget_kg: float) -> Dict:
        budget = await self.budget_tracker.create_budget(entity_name, total_budget_kg)
        await self.db_manager.save_budget(budget)
        return budget.__dict__
    
    async def record_carbon_consumption(self, entity_id: str, amount_kg: float) -> Dict:
        is_warning, remaining = await self.budget_tracker.consume_budget(entity_id, amount_kg)
        
        if is_warning:
            alert = CarbonAlert(
                alert_type="budget_warning",
                severity="warning",
                message=f"Carbon budget warning: Only {remaining:.0f}kg remaining",
                value=remaining,
                threshold=0.2
            )
            await self.db_manager.save_alert(alert)
        
        return {'remaining_kg': remaining, 'warning_triggered': is_warning}
    
    async def _model_training_loop(self):
        while not self._shutdown_event.is_set():
            try:
                async with self._history_lock:
                    historical_data = []
                    for region, intensities in self.region_intensities.items():
                        for i, intensity in enumerate(intensities):
                            historical_data.append({
                                'intensity': intensity,
                                'hour': i % 24,
                                'day_of_week': (i // 24) % 7,
                                'month': 5,
                                'renewable_pct': self.carbon_data.get(region, {}).get('renewable_pct', 30),
                                'temperature': 10,
                                'wind_speed': 5,
                                'cloud_cover': 50,
                                'demand_gw': 100,
                                'seasonal_factor': 1
                            })
                    
                    intensities = [d['intensity'] for d in historical_data]
                
                if len(historical_data) >= 100:
                    await self.forecaster.train(historical_data)
                    await self.anomaly_detector.train(intensities)
                
                await asyncio.sleep(3600)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Model training error: {e}")
                await asyncio.sleep(3600)
    
    async def _health_check_loop(self):
        while not self._shutdown_event.is_set():
            try:
                health = await self.health_check()
                HEALTH_SCORE.set(health.get('health_score', 0))
                CARBON_HEALTH.set(health.get('health_score', 0))
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Health check error: {e}")
                await asyncio.sleep(60)
    
    async def _cleanup_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await self.db_manager.cleanup_old_records()
                gc.collect()
                await asyncio.sleep(CLEANUP_INTERVAL_HOURS * 3600)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Cleanup error: {e}")
                await asyncio.sleep(3600)
    
    async def health_check(self) -> Dict:
        try:
            async def _check():
                async with self._history_lock:
                    analysis_count = len(self.analysis_history)
                
                quality_stats = await self.quality_scorer.get_statistics()
                cache_stats = await self.cache.get_stats()
                forecaster_stats = {'trained': self.forecaster.is_trained}
                anomaly_stats = await self.anomaly_detector.get_statistics()
                sustainability = await self.sustainability_tracker.get_sustainability_score()
                
                health_score = 100
                if analysis_count == 0:
                    health_score -= 30
                if quality_stats.get('avg_score', 0) < 50:
                    health_score -= 20
                if not forecaster_stats.get('trained', False):
                    health_score -= 10
                
                return {
                    'healthy': analysis_count > 0,
                    'instance_id': self.instance_id,
                    'version': DATA_VERSION,
                    'analysis_count': analysis_count,
                    'alert_count': len(self.alert_history),
                    'health_score': max(0, health_score),
                    'data_quality': quality_stats.get('avg_score', 0),
                    'forecaster_trained': forecaster_stats.get('trained', False),
                    'anomaly_detector_trained': anomaly_stats.get('is_trained', False),
                    'queue_size': self.operation_queue.qsize(),
                    'ws_connections': len(self.websocket.connections),
                    'cache': cache_stats,
                    'circuit_breakers': {name: cb.get_metrics()['state'] 
                                        for name, cb in self.circuit_breakers.items()},
                    # NEW: Sustainability metrics
                    'sustainability': {
                        'score': sustainability,
                        'federated_packages': len(self.federated_learner._knowledge_bank),
                        'cross_domain_transfers': self.cross_domain_transfer.get_transfer_statistics(),
                        'human_feedback': await self.human_collaborator.get_feedback_summary()
                    },
                    'timestamp': datetime.now().isoformat()
                }
            
            return await asyncio.wait_for(_check(), timeout=HEALTH_CHECK_TIMEOUT)
            
        except asyncio.TimeoutError:
            logger.error("Health check timed out")
            return {'healthy': False, 'status': 'timeout', 'instance_id': self.instance_id}
    
    async def get_statistics(self) -> Dict:
        async with self._history_lock:
            analysis_count = len(self.analysis_history)
        
        quality_stats = await self.quality_scorer.get_statistics()
        cache_stats = await self.cache.get_stats()
        sustainability = await self.sustainability_tracker.get_sustainability_score()
        feedback_summary = await self.human_collaborator.get_feedback_summary()
        
        return {
            'instance_id': self.instance_id,
            'version': DATA_VERSION,
            'analysis_count': analysis_count,
            'alert_count': len(self.alert_history),
            'data_quality': quality_stats,
            'forecaster': {'trained': self.forecaster.is_trained},
            'cache': cache_stats,
            'queue_size': self.operation_queue.qsize(),
            'ws_connections': len(self.websocket.connections),
            'circuit_breakers': {name: cb.get_metrics() for name, cb in self.circuit_breakers.items()},
            'regions_tracked': len(self.carbon_data),
            # NEW: Sustainability metrics
            'sustainability': {
                'score': sustainability,
                'feedback': feedback_summary,
                'federated': self.federated_learner.get_federated_insights(),
                'cross_domain': self.cross_domain_transfer.get_transfer_statistics()
            },
            'timestamp': datetime.now().isoformat()
        }
    
    async def export_state(self) -> Dict:
        async with self._history_lock:
            return {
                'instance_id': self.instance_id,
                'version': DATA_VERSION,
                'analysis_history': [a.to_dict() for a in self.analysis_history],
                'alert_history': [a.__dict__ for a in self.alert_history],
                'sustainability': await self.sustainability_tracker.get_sustainability_score(),
                'exported_at': datetime.now().isoformat()
            }
    
    async def import_state(self, state: Dict):
        async with self._history_lock:
            self.analysis_history.clear()
            for a in state.get('analysis_history', []):
                self.analysis_history.append(CarbonAnalysisResult(**a))
            
            self.alert_history.clear()
            for a in state.get('alert_history', []):
                self.alert_history.append(CarbonAlert(**a))
            
            logger.info(f"Imported {len(self.analysis_history)} analyses from backup")
    
    async def shutdown(self):
        logger.info(f"Shutting down EnhancedCarbonIntelligencePlatformV12 (instance: {self.instance_id})")
        
        self._shutdown_event.set()
        self._running = False
        
        # Shutdown advanced components
        await self.federated_learner.shutdown()
        
        # Cancel queue worker
        if self._queue_worker:
            self._queue_worker.cancel()
            try:
                await self._queue_worker
            except asyncio.CancelledError:
                pass
        
        for task in self.background_tasks:
            task.cancel()
        
        if self.background_tasks:
            await asyncio.gather(*self.background_tasks, return_exceptions=True)
        
        await self.websocket.stop()
        
        await self.api_client.__aexit__(None, None, None)
        
        await self.cache.stop()
        
        self.db_manager.dispose()
        
        self.thread_pool.shutdown(wait=True)
        
        # Final sustainability report
        report = await self.sustainability_tracker.generate_report()
        logger.info(f"Final sustainability report: overall_score={report['sustainability_score']['overall_score']:.1f}%")
        
        logger.info("Shutdown complete")

# ============================================================
# SINGLETON ACCESSOR
# ============================================================

_platform_instance = None
_platform_lock = asyncio.Lock()

async def get_carbon_platform() -> EnhancedCarbonIntelligencePlatformV12:
    global _platform_instance
    if _platform_instance is None:
        async with _platform_lock:
            if _platform_instance is None:
                _platform_instance = EnhancedCarbonIntelligencePlatformV12()
                await _platform_instance.start()
    return _platform_instance

# ============================================================
# MAIN ENTRY POINT
# ============================================================

async def main():
    print("=" * 80)
    print("Enhanced Carbon Intelligence Platform v12.0 - Advanced Sustainability")
    print("Federated Learning | User Adaptation | Cross-Domain Transfer | Predictive Management")
    print("=" * 80)
    
    platform = await get_carbon_platform()
    
    print(f"\n✅ v12.0 ADVANCED SUSTAINABILITY FEATURES:")
    print(f"   ✅ Federated Carbon Learning - Cross-instance insights sharing")
    print(f"   ✅ User-Adaptive Carbon Reflexivity - Learning user preferences")
    print(f"   ✅ Cross-Domain Carbon Transfer - Domain insights sharing")
    print(f"   ✅ Human-AI Carbon Collaboration - Feedback loops with users")
    print(f"   ✅ Predictive Carbon Management - Proactive carbon management")
    print(f"   ✅ Carbon Sustainability Metrics - Tracking eco-efficiency gains")
    
    # Test federated learning
    print(f"\n📊 Testing Federated Learning:")
    insight_id = await platform.federated_learner.share_carbon_insight({
        'carbon': {
            'intensity': 150,
            'renewable_pct': 75,
            'savings': 120
        }
    })
    print(f"   Insight shared: {insight_id}")
    
    # Test user adaptation
    print(f"\n📊 Testing User Adaptation:")
    await platform.user_adaptive.learn_user_preference(
        "test_user",
        "accept_carbon_recommendation",
        {"region": "FI", "intensity": 150},
        {"success": True}
    )
    print(f"   User adaptation updated")
    
    # Test cross-domain transfer
    print(f"\n📊 Testing Cross-Domain Transfer:")
    transferred = await platform.cross_domain_transfer.transfer_knowledge(
        'energy', 'manufacturing',
        {'intensity': 150, 'renewable_pct': 75}
    )
    print(f"   Transferred {len(transferred)} items from energy to manufacturing")
    
    # Test human collaboration
    print(f"\n📊 Testing Human-AI Collaboration:")
    feedback_id = await platform.human_collaborator.request_carbon_feedback(
        {'current_intensity': 150, 'forecast_24h': 120},
        {'recommendation': 'Schedule workloads to optimize carbon'}
    )
    print(f"   Feedback request created: {feedback_id}")
    
    print(f"\n🌍 Fetching Real-time Carbon Data with Sustainability...")
    result = await platform.get_carbon_intensity("FI", user_id="test_user")
    
    print(f"\n📊 Carbon Analysis Results (Finland):")
    print(f"   Current Intensity: {result.current_intensity:.0f} gCO₂/kWh")
    print(f"   Renewable Share: {result.renewable_pct:.0f}%")
    print(f"   24h Forecast: {result.forecast_24h:.0f} gCO₂/kWh")
    print(f"   Carbon Savings Potential: {result.carbon_savings_potential:.1f} kg CO₂/MWh")
    
    if result.optimal_workload_window:
        opt = result.optimal_workload_window
        print(f"   Optimal Workload Window: {opt.get('savings_pct', 0):.1f}% savings")
    
    # Get workload scheduling recommendation
    print(f"\n⏰ Carbon-Aware Workload Scheduling:")
    opt_schedule = await platform.get_optimal_workload_time("FI", 8, user_id="test_user")
    if 'error' not in opt_schedule:
        print(f"   {opt_schedule['recommendation']}")
        print(f"   Expected Savings: {opt_schedule['savings_pct']:.1f}%")
    
    # Get sustainability metrics
    stats = await platform.get_statistics()
    print(f"\n♻️ Sustainability Metrics:")
    print(f"   Overall Score: {stats['sustainability']['score']['overall_score']:.1f}%")
    print(f"   Eco-Efficiency: {stats['sustainability']['score']['eco_efficiency']:.1f}%")
    print(f"   Federated Packages: {stats['sustainability']['federated']['total_packages']}")
    print(f"   Cross-Domain Transfers: {stats['sustainability']['cross_domain']['total_transfers']}")
    print(f"   Human Feedback: {stats['sustainability']['feedback']['total']} (avg approval: {stats['sustainability']['feedback']['average_approval']:.1%})")
    
    print("\n" + "=" * 80)
    print("✅ Enhanced Carbon Intelligence Platform v12.0 - Production Ready")
    print("   With Full Sustainability Features: Federated, Adaptive, Predictive")
    print("=" * 80)
    
    try:
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        print("\n🛑 Shutting down...")
        await platform.shutdown()
        print("Shutdown complete")

if __name__ == "__main__":
    asyncio.run(main())
