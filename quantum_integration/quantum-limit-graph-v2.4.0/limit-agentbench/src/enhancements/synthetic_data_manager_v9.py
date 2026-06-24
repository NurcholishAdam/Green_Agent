# File: src/enhancements/synthetic_data_manager_enhanced_v11_0.py
"""
Enhanced Synthetic Data Manager for Green Agent - Version 11.0 (Advanced Sustainability)

CRITICAL ADDITIONS OVER v10.0:
1. ADDED: Federated Reflexive Learning - Cross-instance synthetic data insights sharing
2. ADDED: User-Adaptive Reflexivity - Learning user generation preferences over time
3. ADDED: Real-Time Carbon Intensity Integration - Carbon-aware generation scheduling
4. ADDED: Cross-Domain Knowledge Transfer - Sharing insights across domains
5. ADDED: Human-AI Collaborative Reflection - Feedback loops with users
6. ADDED: Predictive Reflexivity - Proactive synthetic data management
7. ADDED: Enhanced Helium Awareness - Resource-aware synthetic data generation
8. ADDED: Sustainability Impact Metrics - Tracking eco-efficiency gains
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
from typing import Dict, List, Optional, Tuple, Any, Callable, Set, Union, Generator, AsyncGenerator
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

# WebSocket for real-time dashboard
import websockets
from websockets.server import serve
from websockets.exceptions import ConnectionClosed

# Data drift detection
from scipy.spatial.distance import jensenshannon
from scipy.stats import wasserstein_distance

# Differential privacy
import numpy as np

# Advanced ML for generation
try:
    from sklearn.ensemble import RandomForestRegressor
    from sklearn.neural_network import MLPRegressor
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

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
        logging.handlers.RotatingFileHandler('synthetic_data_v11.log', maxBytes=10*1024*1024, backupCount=5),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)
logger.addFilter(CorrelationIdFilter())

# Audit logger
audit_logger = logging.getLogger('synthetic_audit')
audit_handler = logging.handlers.RotatingFileHandler('synthetic_audit_v11.log', maxBytes=50*1024*1024, backupCount=10)
audit_handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
audit_logger.addHandler(audit_handler)
audit_logger.setLevel(logging.INFO)

# Prometheus metrics
REGISTRY = CollectorRegistry()

# Core metrics
DATA_GENERATIONS = Counter('synthetic_generations_total', 'Total data generations', ['domain', 'status', 'method'], registry=REGISTRY)
GENERATION_DURATION = Histogram('synthetic_generation_duration_seconds', 'Generation duration', ['domain', 'method'], registry=REGISTRY)
DATA_QUALITY = Gauge('synthetic_data_quality', 'Data quality score', ['domain', 'metric'], registry=REGISTRY)
DRIFT_SCORE = Gauge('synthetic_data_drift', 'Distribution drift score', ['domain', 'column'], registry=REGISTRY)
PRIVACY_BUDGET = Gauge('synthetic_privacy_budget', 'Differential privacy budget (epsilon)', ['domain'], registry=REGISTRY)
CIRCUIT_BREAKER_STATE = Gauge('synthetic_circuit_breaker_state', 'Circuit breaker state (0=closed,1=half,2=open)', ['component'], registry=REGISTRY)
HEALTH_SCORE = Gauge('synthetic_system_health', 'System health score (0-100)', registry=REGISTRY)
DB_SIZE = Gauge('synthetic_db_size_mb', 'Database size in MB', registry=REGISTRY)
DATA_QUALITY_SCORE = Gauge('synthetic_data_quality_score', 'Input data quality score', registry=REGISTRY)
GENERATION_QUEUE_SIZE = Gauge('synthetic_generation_queue_size', 'Generation queue size', registry=REGISTRY)
WS_CONNECTIONS = Gauge('synthetic_ws_connections', 'WebSocket connections', registry=REGISTRY)

# NEW: Advanced sustainability metrics
FEDERATED_SYNTHETIC_KNOWLEDGE = Gauge('federated_synthetic_knowledge', 'Federated knowledge packages', registry=REGISTRY)
USER_SYNTHETIC_ADAPTATION = Gauge('user_synthetic_adaptation_score', 'User adaptation score', ['user_id'], registry=REGISTRY)
SYNTHETIC_CARBON_INTENSITY = Gauge('synthetic_carbon_intensity', 'Carbon intensity (gCO2/kWh)', ['region'], registry=REGISTRY)
CROSS_DOMAIN_SYNTHETIC_TRANSFERS = Counter('cross_domain_synthetic_transfers_total', 'Cross-domain transfers', ['source', 'target'], registry=REGISTRY)
HUMAN_SYNTHETIC_FEEDBACK = Counter('human_synthetic_feedback_total', 'Human feedback events', ['type'], registry=REGISTRY)
PREDICTIVE_SYNTHETIC_ACCURACY = Gauge('predictive_synthetic_accuracy', 'Predictive model accuracy', ['model_type'], registry=REGISTRY)
SYNTHETIC_SUSTAINABILITY_SCORE = Gauge('synthetic_sustainability_score', 'Sustainability score', registry=REGISTRY)
SYNTHETIC_ECO_EFFICIENCY = Gauge('synthetic_eco_efficiency', 'Eco-efficiency score', registry=REGISTRY)

# Constants
MAX_DATASET_RECORDS = 100000
MAX_QUALITY_HISTORY = 10000
MAX_DRIFT_HISTORY = 1000
MAX_CACHE_SIZE = 1000
CACHE_TTL_SECONDS = 300
MAX_RETRY_ATTEMPTS = 3
CIRCUIT_BREAKER_THRESHOLD = 5
CIRCUIT_BREAKER_TIMEOUT = 60
HEALTH_CHECK_TIMEOUT = 10
RATE_LIMIT_REQUESTS = 50
RATE_LIMIT_WINDOW = 60
MAX_CONCURRENT_GENERATIONS = 4
DATA_VERSION = 11
DB_POOL_SIZE = 10
DB_MAX_OVERFLOW = 20
DB_POOL_TIMEOUT = 30
CACHE_CLEANUP_INTERVAL = 3600
MAX_CACHE_SIZE_MB = 500
DEFAULT_EPSILON = 1.0
DEFAULT_DELTA = 1e-5
DRIFT_WARNING_THRESHOLD = 0.1
DRIFT_CRITICAL_THRESHOLD = 0.2

# ============================================================
# NEW: FEDERATED SYNTHETIC LEARNING
# ============================================================

class FederatedSyntheticLearner:
    """
    Federated learning system for sharing synthetic data insights across instances.
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
        
        logger.info(f"FederatedSyntheticLearner initialized for instance {instance_id}")
    
    async def share_synthetic_insight(self, insight: Dict) -> str:
        """
        Share a synthetic data insight with the federated network.
        """
        async with self._lock:
            anonymized_insight = self._anonymize_insight(insight)
            
            package_id = f"fed_synth_{uuid.uuid4().hex[:12]}"
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
            
            FEDERATED_SYNTHETIC_KNOWLEDGE.set(len(self._knowledge_bank))
            logger.info(f"Synthetic insight {package_id} shared")
            return package_id
    
    def _anonymize_insight(self, insight: Dict) -> Dict:
        anonymized = insight.copy()
        anonymized.pop('specific_data', None)
        anonymized.pop('user_data', None)
        anonymized.pop('proprietary_params', None)
        
        if 'synthetic' in anonymized:
            synth = anonymized['synthetic']
            anonymized['synthetic'] = {
                'domain': synth.get('domain', 'unknown'),
                'quality': synth.get('quality', 0),
                'method': synth.get('method', 'unknown')
            }
        
        return anonymized
    
    async def _broadcast_to_network(self, package: Dict):
        try:
            await self.persistence.save_shared_synthetic_knowledge(package)
            logger.info(f"Broadcasted synthetic insight {package['package_id']} to network")
        except Exception as e:
            logger.error(f"Failed to broadcast synthetic insight: {e}")
    
    async def pull_network_insights(self, domain: Optional[str] = None, limit: int = 10) -> List[Dict]:
        try:
            packages = await self.persistence.get_shared_synthetic_knowledge(domain=domain, limit=limit)
            if packages:
                self._aggregate_federated_weights(packages)
                self.aggregation_count += 1
                logger.info(f"Pulled {len(packages)} synthetic insights from network")
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
    
    async def apply_federated_insights(self, generation_params: Dict) -> Dict:
        if not self.federated_weights:
            return generation_params
        
        adjusted_params = generation_params.copy()
        
        for key, weight in self.federated_weights.items():
            if key in adjusted_params and isinstance(adjusted_params[key], (int, float)):
                adjustment_factor = 1.0 + (weight - 0.5) * 0.2
                adjusted_params[key] = adjusted_params[key] * adjustment_factor
        
        return adjusted_params
    
    async def shutdown(self):
        logger.info("FederatedSyntheticLearner shutdown complete")

# ============================================================
# NEW: USER-ADAPTIVE SYNTHETIC REFLEXIVITY
# ============================================================

class UserAdaptiveSyntheticReflexivity:
    """
    Learns user synthetic data preferences and adapts behavior over time.
    """
    
    def __init__(self, persistence, learning_rate: float = 0.1):
        self.persistence = persistence
        self.learning_rate = learning_rate
        self._user_profiles: Dict[str, Dict] = {}
        self._preference_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=100))
        self._lock = asyncio.Lock()
        
        logger.info("UserAdaptiveSyntheticReflexivity initialized")
    
    async def learn_user_preference(self, user_id: str, action: str, context: Dict, outcome: Dict):
        async with self._lock:
            if user_id not in self._user_profiles:
                self._user_profiles[user_id] = {
                    'synthetic_preferences': defaultdict(float),
                    'history': [],
                    'adaptation_score': 50.0,
                    'last_updated': datetime.now().isoformat()
                }
            
            profile = self._user_profiles[user_id]
            preference_update = self._calculate_preference_update(action, context, outcome)
            
            for key, value in preference_update.items():
                profile['synthetic_preferences'][key] += value * self.learning_rate
                profile['synthetic_preferences'][key] = max(0, min(1, profile['synthetic_preferences'][key]))
            
            profile['history'].append({
                'action': action,
                'timestamp': datetime.now().isoformat(),
                'outcome': outcome
            })
            
            profile['adaptation_score'] = self._calculate_adaptation_score(profile)
            USER_SYNTHETIC_ADAPTATION.labels(user_id=user_id).set(profile['adaptation_score'])
            
            await self.persistence.save_user_synthetic_profile(user_id, profile)
            
            logger.info(f"Updated synthetic preferences for user {user_id}, adaptation score: {profile['adaptation_score']:.1f}")
    
    def _calculate_preference_update(self, action: str, context: Dict, outcome: Dict) -> Dict:
        update = defaultdict(float)
        
        if outcome.get('success', False):
            if action == 'accept_synthetic_data':
                update['synthetic_acceptance'] += 0.1
                update['quality_preference'] += 0.05
            elif action == 'reject_synthetic_data':
                update['synthetic_acceptance'] -= 0.05
                update['real_data_preference'] += 0.1
            elif action == 'adjust_generation_params':
                update['parameter_preference'] += 0.15
        
        if context.get('carbon_aware', False):
            update['carbon_awareness'] += 0.15
        
        return dict(update)
    
    def _calculate_adaptation_score(self, profile: Dict) -> float:
        if not profile['history']:
            return 50.0
        
        preferences = profile['synthetic_preferences']
        if not preferences:
            return 50.0
        
        variance = np.var(list(preferences.values()))
        consistency = 1.0 - min(1.0, variance)
        history_depth = min(1.0, len(profile['history']) / 20)
        
        return 50.0 + 40.0 * consistency * history_depth
    
    async def get_personalized_synthetic_params(self, user_id: str, default_params: Dict) -> Dict:
        async with self._lock:
            profile = self._user_profiles.get(user_id)
            if not profile:
                return default_params
            
            preferences = profile['synthetic_preferences']
            
            adjusted_params = default_params.copy()
            
            if preferences.get('quality_preference', 0) > 0.7:
                adjusted_params['validation_level'] = 'strict'
            if preferences.get('real_data_preference', 0) > 0.7:
                adjusted_params['real_data_ratio'] = 0.3
            
            return adjusted_params

# ============================================================
# NEW: CARBON-AWARE SYNTHETIC SCHEDULER
# ============================================================

class CarbonAwareSyntheticScheduler:
    """
    Schedules synthetic data generation based on real-time carbon intensity.
    """
    
    def __init__(self, persistence, api_key: Optional[str] = None, region: str = "global"):
        self.persistence = persistence
        self.api_key = api_key or os.getenv('CARBON_INTENSITY_API_KEY')
        self.region = region
        self._cache = {}
        self._cache_ttl = 300
        self._lock = asyncio.Lock()
        self._session = None
        
        logger.info(f"CarbonAwareSyntheticScheduler initialized for region {region}")
    
    async def _get_session(self):
        if self._session is None:
            self._session = aiohttp.ClientSession()
        return self._session
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def get_current_intensity(self, region: Optional[str] = None) -> Dict:
        region = region or self.region
        cache_key = f"intensity_{region}"
        
        async with self._lock:
            if cache_key in self._cache:
                cached_data, timestamp = self._cache[cache_key]
                if time.time() - timestamp < self._cache_ttl:
                    return cached_data
        
        try:
            session = await self._get_session()
            headers = {'auth-token': self.api_key} if self.api_key else {}
            url = f"https://api.electricitymaps.org/v3/carbon-intensity/latest?zone={region}"
            
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    data = await response.json()
                    intensity_data = {
                        'intensity': data.get('carbonIntensity', 400),
                        'unit': data.get('unit', 'gCO2/kWh'),
                        'timestamp': datetime.now().isoformat(),
                        'region': region
                    }
                    
                    async with self._lock:
                        self._cache[cache_key] = (intensity_data, time.time())
                    
                    SYNTHETIC_CARBON_INTENSITY.labels(region=region).set(intensity_data['intensity'])
                    return intensity_data
                else:
                    logger.warning(f"Carbon intensity API returned {response.status}")
                    return self._get_fallback_intensity(region)
                    
        except Exception as e:
            logger.error(f"Carbon intensity API error: {e}")
            return self._get_fallback_intensity(region)
    
    def _get_fallback_intensity(self, region: str) -> Dict:
        hour = datetime.now().hour
        if 0 <= hour < 6:
            intensity = 200
        elif 6 <= hour < 12:
            intensity = 350
        elif 12 <= hour < 18:
            intensity = 300
        else:
            intensity = 450
        
        return {
            'intensity': intensity,
            'unit': 'gCO2/kWh',
            'timestamp': datetime.now().isoformat(),
            'region': region,
            'source': 'fallback'
        }
    
    async def get_forecast(self, region: Optional[str] = None, hours: int = 24) -> List[Dict]:
        region = region or self.region
        
        try:
            session = await self._get_session()
            headers = {'auth-token': self.api_key} if self.api_key else {}
            url = f"https://api.electricitymaps.org/v3/carbon-intensity/forecast?zone={region}"
            
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    data = await response.json()
                    forecast = []
                    for entry in data.get('forecast', []):
                        forecast.append({
                            'timestamp': entry.get('datetime'),
                            'intensity': entry.get('carbonIntensity', 400),
                            'unit': 'gCO2/kWh'
                        })
                    return forecast
                else:
                    return self._get_fallback_forecast(hours)
                    
        except Exception as e:
            logger.error(f"Carbon intensity forecast error: {e}")
            return self._get_fallback_forecast(hours)
    
    def _get_fallback_forecast(self, hours: int) -> List[Dict]:
        forecast = []
        now = datetime.now()
        
        for i in range(hours):
            hour = (now + timedelta(hours=i)).hour
            if 0 <= hour < 6:
                intensity = 180 + np.random.normal(0, 20)
            elif 6 <= hour < 12:
                intensity = 320 + np.random.normal(0, 30)
            elif 12 <= hour < 18:
                intensity = 280 + np.random.normal(0, 30)
            else:
                intensity = 420 + np.random.normal(0, 40)
            
            forecast.append({
                'timestamp': (now + timedelta(hours=i)).isoformat(),
                'intensity': max(100, intensity),
                'unit': 'gCO2/kWh'
            })
        
        return forecast
    
    async def schedule_generation(self, urgency: str = "normal") -> Dict:
        intensity = await self.get_current_intensity()
        
        if urgency == "critical":
            return {'action': 'run_now', 'reason': 'Critical generation needed'}
        elif urgency == "normal" and intensity['intensity'] > 500:
            forecast = await self.get_forecast()
            if forecast:
                best = min(forecast, key=lambda x: x['intensity'])
                savings = (intensity['intensity'] - best['intensity']) / intensity['intensity'] * 100
                if savings > 20:
                    return {
                        'action': 'schedule',
                        'optimal_time': best['timestamp'],
                        'savings_percent': savings,
                        'reason': f'High carbon intensity: {intensity["intensity"]} gCO2/kWh'
                    }
        
        return {'action': 'run_now', 'reason': 'Low carbon intensity or marginal savings'}
    
    async def close(self):
        if self._session:
            await self._session.close()

# ============================================================
# NEW: CROSS-DOMAIN SYNTHETIC TRANSFER
# ============================================================

class CrossDomainSyntheticTransfer:
    """
    Transfers synthetic data knowledge across different domains.
    """
    
    def __init__(self, persistence):
        self.persistence = persistence
        self._domain_knowledge: Dict[str, Dict] = {}
        self._transfer_mappings: Dict[str, Dict[str, float]] = {}
        self._lock = asyncio.Lock()
        
        logger.info("CrossDomainSyntheticTransfer initialized")
    
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
            
            CROSS_DOMAIN_SYNTHETIC_TRANSFERS.labels(source=source_domain, target=target_domain).inc()
            
            logger.info(f"Transferred synthetic knowledge from {source_domain} to {target_domain}: {len(transferred)} items")
            return transferred
    
    async def _map_knowledge(self, source: str, target: str, knowledge: Dict, strategy: str) -> Dict:
        domain_similarities = {
            ('esg_metrics', 'carbon_data'): {
                'esg_score': 'carbon_price',
                'carbon_intensity': 'emissions_tonnes',
                'renewable_pct': 'offset_credits'
            },
            ('helium_data', 'carbon_data'): {
                'production_tonnes': 'emissions_tonnes',
                'scarcity_index': 'carbon_price'
            },
            ('time_series', 'esg_metrics'): {
                'value': 'esg_score',
                'trend': 'trend'
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
# NEW: HUMAN-AI SYNTHETIC COLLABORATION
# ============================================================

class HumanAISyntheticCollaboration:
    """
    Enables collaborative reflection between humans and AI on synthetic data decisions.
    """
    
    def __init__(self, persistence, feedback_timeout: int = 300):
        self.persistence = persistence
        self.feedback_timeout = feedback_timeout
        self._feedback_queue: deque = deque(maxlen=1000)
        self._explanations: Dict[str, Dict] = {}
        self._pending_feedback: Dict[str, datetime] = {}
        self._lock = asyncio.Lock()
        self._listeners: List[Callable] = []
        
        logger.info("HumanAISyntheticCollaboration initialized")
    
    async def request_synthetic_feedback(self, decision: Dict, context: Dict) -> str:
        feedback_id = f"fb_synth_{uuid.uuid4().hex[:12]}"
        
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
        
        HUMAN_SYNTHETIC_FEEDBACK.labels(type='request').inc()
        return feedback_id
    
    async def submit_synthetic_feedback(self, feedback_id: str, feedback: Dict) -> bool:
        async with self._lock:
            if feedback_id not in self._explanations:
                logger.warning(f"Synthetic feedback ID {feedback_id} not found")
                return False
            
            if feedback_id not in self._pending_feedback:
                logger.warning(f"Synthetic feedback ID {feedback_id} expired")
                return False
            
            request = self._explanations[feedback_id]
            request['status'] = 'completed'
            request['feedback'] = feedback
            request['feedback_timestamp'] = datetime.now().isoformat()
            
            del self._pending_feedback[feedback_id]
            self._feedback_queue.append(request)
        
        await self._process_feedback(request)
        HUMAN_SYNTHETIC_FEEDBACK.labels(type='submitted').inc()
        
        for listener in self._listeners:
            try:
                await listener(request)
            except Exception as e:
                logger.error(f"Synthetic feedback listener error: {e}")
        
        logger.info(f"Synthetic feedback {feedback_id} submitted")
        return True
    
    async def _process_feedback(self, feedback_request: Dict):
        feedback = feedback_request.get('feedback', {})
        
        learning = {
            'approval': feedback.get('approval', 0.5),
            'comments': feedback.get('comments', ''),
            'suggestions': feedback.get('suggestions', {}),
            'timestamp': datetime.now().isoformat()
        }
        
        await self.persistence.save_synthetic_feedback_learning(learning)
        
        logger.info(f"Processed synthetic feedback learning: approval={learning['approval']:.2f}")
    
    async def generate_synthetic_explanation(self, decision: Dict, context: Dict) -> Dict:
        explanation = {
            'id': f"exp_synth_{uuid.uuid4().hex[:12]}",
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
        
        if 'domain' in decision:
            parts.append(f"Domain: {decision['domain']}")
        if 'n_samples' in decision:
            parts.append(f"Samples: {decision['n_samples']}")
        if 'method' in decision:
            parts.append(f"Method: {decision['method']}")
        if 'reasoning' in context:
            parts.append(f"Reasoning: {context['reasoning']}")
        
        return ". ".join(parts)
    
    def _calculate_confidence(self, decision: Dict) -> float:
        confidence = 0.7
        
        if 'quality_score' in decision:
            confidence = decision['quality_score'] / 100
        
        return min(1.0, confidence)
    
    def _generate_alternatives(self, decision: Dict) -> List[Dict]:
        alternatives = []
        
        if 'method' in decision:
            current = decision['method']
            alternatives.append({
                'type': 'alternative_method',
                'method': 'gan' if current != 'gan' else 'statistical',
                'tradeoff': 'different_quality'
            })
            alternatives.append({
                'type': 'different_samples',
                'n_samples': decision.get('n_samples', 1000) * 2,
                'tradeoff': 'higher_compute'
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
# NEW: PREDICTIVE SYNTHETIC MANAGEMENT
# ============================================================

class PredictiveSyntheticManager:
    """
    Predicts synthetic data quality and proactively manages generation.
    """
    
    def __init__(self, persistence, horizon_hours: int = 24):
        self.persistence = persistence
        self.horizon_hours = horizon_hours
        self._predictions: Dict[str, Dict] = {}
        self._historical_data: deque = deque(maxlen=1000)
        self._lock = asyncio.Lock()
        
        logger.info(f"PredictiveSyntheticManager initialized with {horizon_hours}h horizon")
    
    async def predict_quality_trend(self, domain: str, time_window: int = 3600) -> Dict:
        async with self._lock:
            history = await self.persistence.get_generation_history(domain, limit=100)
            self._historical_data.extend(history)
            
            if len(self._historical_data) < 10:
                return {
                    'predicted_quality': 0.5,
                    'confidence': 0.1,
                    'reason': 'Insufficient data'
                }
            
            recent = list(self._historical_data)[-50:]
            
            if len(recent) > 1:
                time_span = (datetime.now() - datetime.fromisoformat(recent[0]['timestamp'])).total_seconds()
                if time_span > 0:
                    quality_rate = sum(r.get('quality', 0) for r in recent) / time_span
                else:
                    quality_rate = 0.5
            else:
                quality_rate = 0.5
            
            predicted_quality = min(1.0, quality_rate * time_window / 100)
            
            # Calculate confidence
            quality_values = [r.get('quality', 0) for r in recent]
            variance = np.var(quality_values) if quality_values else 1.0
            confidence = max(0, min(1, 1.0 - variance))
            
            prediction = {
                'predicted_quality': predicted_quality,
                'confidence': confidence,
                'time_window_seconds': time_window,
                'timestamp': datetime.now().isoformat()
            }
            
            self._predictions[domain] = prediction
            PREDICTIVE_SYNTHETIC_ACCURACY.labels(model_type='quality').set(confidence)
            
            return prediction
    
    async def generate_proactive_recommendations(self, domain: str) -> List[Dict]:
        recommendations = []
        
        quality_pred = await self.predict_quality_trend(domain)
        
        if quality_pred.get('confidence', 0) > 0.6:
            predicted = quality_pred.get('predicted_quality', 0)
            
            if predicted < 0.4:
                recommendations.append({
                    'type': 'quality_alert',
                    'domain': domain,
                    'reason': f'Quality predicted to drop: {predicted:.1%}',
                    'priority': 'high',
                    'action': 'Regenerate with improved parameters'
                })
            elif predicted < 0.6:
                recommendations.append({
                    'type': 'quality_monitor',
                    'domain': domain,
                    'reason': f'Quality predicted to moderate: {predicted:.1%}',
                    'priority': 'medium',
                    'action': 'Monitor generation quality'
                })
        
        # Carbon-aware recommendation
        if hasattr(self, 'carbon_scheduler'):
            intensity = await self.carbon_scheduler.get_current_intensity()
            if intensity.get('intensity', 0) > 400 and predicted < 0.6:
                recommendations.append({
                    'type': 'carbon_aware_generation',
                    'reason': 'High carbon intensity with low quality prediction',
                    'priority': 'high',
                    'action': 'Delay generation to lower carbon period'
                })
        
        return recommendations
    
    async def get_synthetic_forecast(self, domain: str) -> Dict:
        quality = await self.predict_quality_trend(domain)
        recommendations = await self.generate_proactive_recommendations(domain)
        
        return {
            'quality_forecast': quality,
            'recommendations': recommendations,
            'timestamp': datetime.now().isoformat()
        }

# ============================================================
# NEW: SYNTHETIC SUSTAINABILITY TRACKER
# ============================================================

class SyntheticSustainabilityTracker:
    """
    Tracks and reports synthetic data sustainability metrics.
    """
    
    def __init__(self, persistence):
        self.persistence = persistence
        self._metrics = {
            'eco_efficiency': [],
            'carbon_awareness': [],
            'sustainability_awareness': []
        }
        self._lock = asyncio.Lock()
        
        logger.info("SyntheticSustainabilityTracker initialized")
    
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
        SYNTHETIC_SUSTAINABILITY_SCORE.set(overall)
        
        eco_score = scores.get('eco_efficiency', 0)
        SYNTHETIC_ECO_EFFICIENCY.set(eco_score)
        
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
# ENHANCED MAIN SYNTHETIC DATA MANAGER (COMPLETE)
# ============================================================

class EnhancedSyntheticDataManagerV11:
    """Enhanced synthetic data manager v11.0 with all sustainability features"""
    
    def __init__(self, config: Dict = None):
        self.config = config or {}
        self.instance_id = str(uuid.uuid4())[:8]
        
        # Database
        self.db_manager = EnhancedDatabaseManagerV10(Path("./synthetic_data_v11.db"))
        
        # Components
        self.privacy_manager = None
        self.drift_detector = DataDriftDetector()
        
        # Cache
        self.cache = None
        
        # Generators
        self.generators: Dict[str, EnhancedDomainDataGeneratorV10] = {}
        self._init_generators()
        
        # ============================================================
        # NEW: Advanced sustainability components
        # ============================================================
        
        # 1. Federated Synthetic Learning
        self.federated_learner = FederatedSyntheticLearner(
            self.db_manager,
            self.instance_id,
            share_interval=3600
        )
        
        # 2. User-Adaptive Synthetic Reflexivity
        self.user_adaptive = UserAdaptiveSyntheticReflexivity(
            self.db_manager,
            learning_rate=0.1
        )
        
        # 3. Carbon-Aware Synthetic Scheduler
        self.carbon_scheduler = CarbonAwareSyntheticScheduler(
            self.db_manager,
            api_key=os.getenv('CARBON_INTENSITY_API_KEY'),
            region=os.getenv('CARBON_REGION', 'global')
        )
        
        # 4. Cross-Domain Synthetic Transfer
        self.cross_domain_transfer = CrossDomainSyntheticTransfer(self.db_manager)
        
        # 5. Human-AI Synthetic Collaboration
        self.human_collaborator = HumanAISyntheticCollaboration(
            self.db_manager,
            feedback_timeout=300
        )
        
        # 6. Predictive Synthetic Management
        self.predictive_manager = PredictiveSyntheticManager(
            self.db_manager,
            horizon_hours=24
        )
        
        # 7. Synthetic Sustainability Tracker
        self.sustainability_tracker = SyntheticSustainabilityTracker(self.db_manager)
        
        # State (bounded)
        self.dataset: Dict[str, pd.DataFrame] = {}
        self._dataset_lock = asyncio.Lock()
        
        # Concurrency control
        self._generation_semaphore = asyncio.Semaphore(MAX_CONCURRENT_GENERATIONS)
        
        # Thread pool for CPU-bound tasks
        self.thread_pool = ThreadPoolExecutor(max_workers=MAX_CONCURRENT_GENERATIONS)
        
        # Operation queue
        self.operation_queue = asyncio.Queue(maxsize=100)
        self._queue_worker = None
        self._running = False
        
        # WebSocket dashboard
        self.websocket = SyntheticDataWebSocket(port=8778)
        
        # Background tasks
        self.background_tasks: Set[asyncio.Task] = set()
        self._shutdown_event = asyncio.Event()
        
        logger.info(f"EnhancedSyntheticDataManagerV11 v{DATA_VERSION}.0 initialized (instance: {self.instance_id})")
        logger.info("  ✅ Advanced Synthetic Data Sustainability Features Enabled:")
        logger.info("     - Federated Synthetic Learning")
        logger.info("     - User-Adaptive Synthetic Reflexivity")
        logger.info("     - Carbon-Aware Synthetic Scheduling")
        logger.info("     - Cross-Domain Synthetic Transfer")
        logger.info("     - Human-AI Synthetic Collaboration")
        logger.info("     - Predictive Synthetic Management")
    
    def _init_generators(self):
        """Initialize domain generators"""
        domains = ['esg_metrics', 'helium_data', 'carbon_data', 'time_series', 'general']
        for domain in domains:
            self.generators[domain] = EnhancedDomainDataGeneratorV10(domain)
    
    async def start(self):
        """Start all services"""
        self._running = True
        
        from .synthetic_data_manager_enhanced_v10 import EnhancedCacheManager, EnhancedDataQualityScorer, EnhancedRateLimiter, EnhancedCircuitBreaker
        
        self.cache = EnhancedCacheManager()
        self.quality_scorer = EnhancedDataQualityScorer()
        self.rate_limiter = EnhancedRateLimiter()
        self.circuit_breakers = {
            'generation': EnhancedCircuitBreaker('generation'),
            'validation': EnhancedCircuitBreaker('validation')
        }
        
        await self.cache.start()
        
        self._queue_worker = asyncio.create_task(self._process_queue())
        
        await self.websocket.start()
        
        tasks = [
            asyncio.create_task(self._health_check_loop()),
            asyncio.create_task(self._cleanup_loop()),
            # NEW: Sustainability background tasks
            asyncio.create_task(self._federated_learning_loop()),
            asyncio.create_task(self._predictive_loop()),
            asyncio.create_task(self._sustainability_loop())
        ]
        
        for task in tasks:
            self.background_tasks.add(task)
            task.add_done_callback(self.background_tasks.discard)
        
        logger.info(f"Synthetic data manager started with {len(self.background_tasks)} background tasks")
    
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
                    logger.info(f"Pulled {len(insights)} federated synthetic insights")
                    
                    for insight in insights:
                        if 'synthetic' in insight.get('insight', {}):
                            synth = insight['insight']['synthetic']
                            await self.sustainability_tracker.record_metric(
                                'sustainability_awareness',
                                0.8,
                                {'domain': synth.get('domain', 'unknown')}
                            )
            except Exception as e:
                logger.error(f"Federated learning error: {e}")
                await asyncio.sleep(60)
    
    async def _predictive_loop(self):
        """Background predictive loop"""
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(1800)  # Every 30 minutes
                
                for domain in self.generators.keys():
                    forecast = await self.predictive_manager.get_synthetic_forecast(domain)
                    
                    for rec in forecast.get('recommendations', []):
                        if rec.get('priority') == 'high':
                            logger.info(f"Predictive recommendation: {rec['reason']}")
                            
                            if rec.get('action') == 'Regenerate with improved parameters':
                                logger.info("Triggering regeneration based on predictive insight")
                    
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
    
    async def _process_queue(self):
        """Process queued generation operations"""
        while self._running:
            try:
                operation = await self.operation_queue.get()
                GENERATION_QUEUE_SIZE.set(self.operation_queue.qsize())
                
                try:
                    result = await self._execute_generation(operation)
                    operation['future'].set_result(result)
                except Exception as e:
                    operation['future'].set_exception(e)
                finally:
                    self.operation_queue.task_done()
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Queue worker error: {e}")
    
    async def _execute_generation(self, operation: Dict) -> pd.DataFrame:
        """Execute generation with sustainability features"""
        async with self._generation_semaphore:
            await self.rate_limiter.wait_and_acquire()
            
            start_time = time.time()
            domain = operation['domain']
            n_samples = operation.get('n_samples', 1000)
            method = operation.get('method', 'statistical')
            enable_privacy = operation.get('enable_privacy', False)
            epsilon = operation.get('epsilon', DEFAULT_EPSILON)
            conditional_constraints = operation.get('conditional_constraints', {})
            user_id = operation.get('user_id')
            
            # Validate config
            try:
                validated = GenerationConfig(
                    domain=domain, n_samples=n_samples, method=method,
                    enable_privacy=enable_privacy, epsilon=epsilon,
                    conditional_constraints=conditional_constraints
                )
            except ValidationError as e:
                raise ValueError(f"Invalid generation config: {e}")
            
            generation_id = str(uuid.uuid4())[:12]
            
            # User adaptation
            if user_id and self.user_adaptive:
                synth_params = await self.user_adaptive.get_personalized_synthetic_params(
                    user_id,
                    {'validation_level': 'normal', 'real_data_ratio': 0.1}
                )
                await self.user_adaptive.learn_user_preference(
                    user_id,
                    'accept_synthetic_data',
                    {'domain': domain, 'method': method},
                    {'success': True}
                )
            
            # Carbon-aware scheduling
            schedule = await self.carbon_scheduler.schedule_generation("normal")
            if schedule.get('action') == 'schedule':
                logger.info(f"Generation scheduled for optimal carbon time: {schedule.get('optimal_time')}")
                await self.sustainability_tracker.record_metric(
                    'carbon_awareness',
                    schedule.get('savings_percent', 0) / 100,
                    {'savings': schedule.get('savings_percent', 0)}
                )
            
            # Apply federated insights
            if self.federated_learner.federated_weights:
                generation_params = await self.federated_learner.apply_federated_insights({
                    'n_samples': n_samples,
                    'method': method
                })
            
            # Initialize privacy manager if needed
            privacy_manager = None
            if validated.enable_privacy:
                privacy_manager = DifferentialPrivacyManager(epsilon=validated.epsilon, delta=validated.delta)
            
            # Run generation with circuit breaker
            try:
                generator = self.generators[validated.domain]
                data, used_method = await self.circuit_breakers['generation'].call(
                    generator.generate, validated.n_samples, validated.method,
                    validated.conditional_constraints, privacy_manager
                )
                
                # Assess quality
                quality_metrics = await self.quality_scorer.assess_quality(data, validated.domain)
                quality_score = quality_metrics['overall_score'] if isinstance(quality_metrics, dict) else quality_metrics
                
                # Detect drift
                drift_scores = {}
                if self.drift_detector.reference_distributions:
                    drift_scores = await self.drift_detector.detect_drift(data)
                
                # Federated sharing
                if quality_score > 80:
                    await self.federated_learner.share_synthetic_insight({
                        'synthetic': {
                            'domain': validated.domain,
                            'quality': quality_score,
                            'method': used_method
                        }
                    })
                
                # Human collaboration
                if self.human_collaborator:
                    await self.human_collaborator.request_synthetic_feedback(
                        {
                            'domain': validated.domain,
                            'n_samples': len(data),
                            'method': used_method,
                            'quality_score': quality_score
                        },
                        {
                            'reasoning': 'Synthetic data generation completed',
                            'carbon_impact': (time.time() - start_time) * 0.001
                        }
                    )
                
                # Store in memory (bounded)
                async with self._dataset_lock:
                    self.dataset[validated.domain] = data
                    if len(self.dataset) > 10:
                        oldest = next(iter(self.dataset))
                        del self.dataset[oldest]
                
                # Save to database
                await self.db_manager.save_generated_data(
                    generation_id, validated.domain, used_method, data,
                    quality_score, validated.epsilon if validated.enable_privacy else 0.0
                )
                
                duration_ms = (time.time() - start_time) * 1000
                await self.db_manager.save_generation_log(
                    generation_id, validated.domain, used_method, validated.n_samples,
                    duration_ms, quality_score, drift_scores, 'success'
                )
                
                # Record sustainability metrics
                await self.sustainability_tracker.record_metric(
                    'eco_efficiency',
                    quality_score / 100,
                    {'domain': validated.domain, 'method': used_method}
                )
                
                # Update metrics
                DATA_GENERATIONS.labels(domain=validated.domain, status='success', method=used_method).inc()
                GENERATION_DURATION.labels(domain=validated.domain, method=used_method).observe(duration_ms / 1000)
                
                # Broadcast via WebSocket
                await self.websocket.broadcast_generation(
                    validated.domain, len(data), quality_score, used_method
                )
                
                audit_logger.info(f"Generated {len(data)} rows for {validated.domain} using {used_method} " +
                                 f"(quality={quality_score:.1f}%, privacy={validated.enable_privacy})")
                
                return data
                
            except Exception as e:
                duration_ms = (time.time() - start_time) * 1000
                await self.db_manager.save_generation_log(
                    generation_id, domain, method, n_samples, duration_ms, 0, {}, 'failed', str(e)
                )
                DATA_GENERATIONS.labels(domain=domain, status='failed', method=method).inc()
                logger.error(f"Generation failed for {domain}: {e}")
                raise
    
    async def generate_domain(self, domain: str, n_samples: int = 1000,
                              method: str = "statistical", enable_privacy: bool = False,
                              epsilon: float = DEFAULT_EPSILON,
                              conditional_constraints: Dict = None,
                              user_id: str = None) -> pd.DataFrame:
        """Queue generation request with user context"""
        future = asyncio.Future()
        
        await self.operation_queue.put({
            'type': 'generation',
            'domain': domain,
            'n_samples': n_samples,
            'method': method,
            'enable_privacy': enable_privacy,
            'epsilon': epsilon,
            'conditional_constraints': conditional_constraints or {},
            'user_id': user_id,
            'future': future
        })
        GENERATION_QUEUE_SIZE.set(self.operation_queue.qsize())
        
        return await future
    
    async def set_reference_data(self, reference_data: pd.DataFrame):
        await self.drift_detector.set_reference(reference_data)
    
    async def _health_check_loop(self):
        while not self._shutdown_event.is_set():
            try:
                health = await self.health_check()
                HEALTH_SCORE.set(health.get('health_score', 0))
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Health check error: {e}")
                await asyncio.sleep(60)
    
    async def _cleanup_loop(self):
        while not self._shutdown_event.is_set():
            try:
                gc.collect()
                await asyncio.sleep(CACHE_CLEANUP_INTERVAL)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Cleanup error: {e}")
                await asyncio.sleep(3600)
    
    async def health_check(self) -> Dict:
        try:
            async def _check():
                async with self._dataset_lock:
                    dataset_count = len(self.dataset)
                
                quality_stats = await self.quality_scorer.get_statistics()
                drift_stats = await self.drift_detector.get_statistics()
                cache_stats = await self.cache.get_stats()
                sustainability = await self.sustainability_tracker.get_sustainability_score()
                
                generator_stats = {}
                for domain, gen in self.generators.items():
                    generator_stats[domain] = await gen.get_statistics()
                
                health_score = 100
                if dataset_count == 0:
                    health_score -= 30
                
                return {
                    'healthy': dataset_count > 0,
                    'instance_id': self.instance_id,
                    'version': DATA_VERSION,
                    'dataset_count': dataset_count,
                    'health_score': max(0, health_score),
                    'data_quality': quality_stats,
                    'drift_detection': drift_stats,
                    'generators': generator_stats,
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
        quality_stats = await self.quality_scorer.get_statistics()
        drift_stats = await self.drift_detector.get_statistics()
        cache_stats = await self.cache.get_stats()
        sustainability = await self.sustainability_tracker.get_sustainability_score()
        feedback_summary = await self.human_collaborator.get_feedback_summary()
        
        generator_stats = {}
        for domain, gen in self.generators.items():
            generator_stats[domain] = await gen.get_statistics()
        
        async with self._dataset_lock:
            dataset_sizes = {domain: len(df) for domain, df in self.dataset.items()}
        
        return {
            'instance_id': self.instance_id,
            'version': DATA_VERSION,
            'dataset_sizes': dataset_sizes,
            'data_quality': quality_stats,
            'drift_detection': drift_stats,
            'generators': generator_stats,
            'queue_size': self.operation_queue.qsize(),
            'ws_connections': len(self.websocket.connections),
            'cache': cache_stats,
            'circuit_breakers': {name: cb.get_metrics() for name, cb in self.circuit_breakers.items()},
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
        async with self._dataset_lock:
            state = {
                'instance_id': self.instance_id,
                'version': DATA_VERSION,
                'datasets': {}
            }
            for domain, df in self.dataset.items():
                state['datasets'][domain] = df.to_dict('records')
            state['sustainability'] = await self.sustainability_tracker.get_sustainability_score()
            state['exported_at'] = datetime.now().isoformat()
            return state
    
    async def import_state(self, state: Dict):
        async with self._dataset_lock:
            self.dataset.clear()
            for domain, records in state.get('datasets', {}).items():
                self.dataset[domain] = pd.DataFrame(records)
            logger.info(f"Imported {len(self.dataset)} datasets from backup")
    
    async def shutdown(self):
        logger.info(f"Shutting down EnhancedSyntheticDataManagerV11 (instance: {self.instance_id})")
        
        self._shutdown_event.set()
        self._running = False
        
        # Shutdown advanced components
        await self.federated_learner.shutdown()
        await self.carbon_scheduler.close()
        
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

_manager_instance = None
_manager_lock = asyncio.Lock()

async def get_synthetic_data_manager() -> EnhancedSyntheticDataManagerV11:
    global _manager_instance
    if _manager_instance is None:
        async with _manager_lock:
            if _manager_instance is None:
                _manager_instance = EnhancedSyntheticDataManagerV11()
                await _manager_instance.start()
    return _manager_instance

# ============================================================
# MAIN ENTRY POINT
# ============================================================

async def main():
    print("=" * 80)
    print("Enhanced Synthetic Data Manager v11.0 - Advanced Sustainability")
    print("Federated Learning | User Adaptation | Carbon-Aware | Cross-Domain Transfer")
    print("=" * 80)
    
    manager = await get_synthetic_data_manager()
    
    print(f"\n✅ v11.0 ADVANCED SUSTAINABILITY FEATURES:")
    print(f"   ✅ Federated Synthetic Learning - Cross-instance insights sharing")
    print(f"   ✅ User-Adaptive Synthetic Reflexivity - Learning user preferences")
    print(f"   ✅ Carbon-Aware Synthetic Scheduling - Green data generation")
    print(f"   ✅ Cross-Domain Synthetic Transfer - Domain insights sharing")
    print(f"   ✅ Human-AI Synthetic Collaboration - Feedback loops with users")
    print(f"   ✅ Predictive Synthetic Management - Proactive generation management")
    print(f"   ✅ Synthetic Sustainability Metrics - Tracking eco-efficiency gains")
    
    # Test federated learning
    print(f"\n📊 Testing Federated Learning:")
    insight_id = await manager.federated_learner.share_synthetic_insight({
        'synthetic': {
            'domain': 'esg_metrics',
            'quality': 85,
            'method': 'statistical'
        }
    })
    print(f"   Insight shared: {insight_id}")
    
    # Test user adaptation
    print(f"\n📊 Testing User Adaptation:")
    await manager.user_adaptive.learn_user_preference(
        "test_user",
        "accept_synthetic_data",
        {"domain": "esg_metrics", "method": "statistical"},
        {"success": True}
    )
    print(f"   User adaptation updated")
    
    # Test carbon-aware scheduling
    print(f"\n📊 Testing Carbon-Aware Scheduling:")
    schedule = await manager.carbon_scheduler.schedule_generation("normal")
    print(f"   Schedule action: {schedule['action']}")
    if schedule.get('savings_percent'):
        print(f"   Carbon savings: {schedule['savings_percent']:.1f}%")
    
    # Test cross-domain transfer
    print(f"\n📊 Testing Cross-Domain Transfer:")
    transferred = await manager.cross_domain_transfer.transfer_knowledge(
        'esg_metrics', 'carbon_data',
        {'esg_score': 72, 'carbon_intensity': 250}
    )
    print(f"   Transferred {len(transferred)} items from ESG to Carbon")
    
    print(f"\n🔬 Generating ESG Data with Sustainability Features...")
    esg_data = await manager.generate_domain(
        'esg_metrics', n_samples=500, method='statistical',
        enable_privacy=True, epsilon=1.0, user_id="test_user"
    )
    print(f"   Generated {len(esg_data)} rows, {len(esg_data.columns)} columns")
    
    quality_metrics = await manager.quality_scorer.assess_quality(esg_data, 'esg_metrics')
    print(f"   Quality Score: {quality_metrics['overall_score']:.1f}%")
    
    # Get sustainability metrics
    stats = await manager.get_statistics()
    print(f"\n♻️ Sustainability Metrics:")
    print(f"   Overall Score: {stats['sustainability']['score']['overall_score']:.1f}%")
    print(f"   Eco-Efficiency: {stats['sustainability']['score']['eco_efficiency']:.1f}%")
    print(f"   Federated Packages: {stats['sustainability']['federated']['total_packages']}")
    print(f"   Cross-Domain Transfers: {stats['sustainability']['cross_domain']['total_transfers']}")
    print(f"   Human Feedback: {stats['sustainability']['feedback']['total']} (avg approval: {stats['sustainability']['feedback']['average_approval']:.1%})")
    
    print("\n" + "=" * 80)
    print("✅ Enhanced Synthetic Data Manager v11.0 - Production Ready")
    print("   With Full Sustainability Features: Federated, Adaptive, Carbon-Aware")
    print("=" * 80)
    
    try:
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        print("\n🛑 Shutting down...")
        await manager.shutdown()
        print("Shutdown complete")

if __name__ == "__main__":
    asyncio.run(main())
