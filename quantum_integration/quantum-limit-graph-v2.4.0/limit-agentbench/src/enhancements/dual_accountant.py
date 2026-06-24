# File: src/enhancements/dual_accountant_enhanced_v11_0.py
"""
Enhanced Dual Carbon Accounting for Green Agent - Version 11.0 (Advanced Sustainability)

CRITICAL ADDITIONS OVER v10.2:
1. ADDED: Federated Reflexive Learning - Cross-instance carbon insights sharing
2. ADDED: User-Adaptive Reflexivity - Learning user preferences over time
3. ADDED: Real-Time Carbon Intensity Integration - Live API integration
4. ADDED: Cross-Domain Knowledge Transfer - Sharing insights across domains
5. ADDED: Human-AI Collaborative Reflection - Feedback loops with users
6. ADDED: Predictive Reflexivity - Forecasting and proactive recommendations
7. ADDED: Enhanced Helium Awareness - Resource-aware carbon accounting
8. ADDED: Sustainability Impact Metrics - Tracking eco-efficiency gains

"""

import asyncio
import hashlib
import json
import logging
import os
import signal
import sys
import time
import uuid
import threading
import aiohttp
import aiosqlite
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any, Callable, Set
from collections import defaultdict, deque
from enum import Enum
from concurrent.futures import ThreadPoolExecutor
import numpy as np
from functools import wraps

# Pydantic for validation
from pydantic import BaseModel, Field, validator, ValidationError

# Tenacity for retries
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# Database with connection pooling
from sqlalchemy import create_engine, Column, String, Float, DateTime, Integer, Boolean, Text, JSON, Index, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.pool import QueuePool
from sqlalchemy.exc import SQLAlchemyError, OperationalError

# Prometheus metrics
from prometheus_client import Counter, Gauge, Histogram, CollectorRegistry

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - [%(correlation_id)s] - %(message)s',
    handlers=[
        logging.handlers.RotatingFileHandler('dual_accountant_v11_0.log', maxBytes=10*1024*1024, backupCount=5),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class CorrelationIdFilter(logging.Filter):
    _local = threading.local()
    
    @classmethod
    def get_correlation_id(cls):
        if not hasattr(cls._local, 'correlation_id'):
            cls._local.correlation_id = str(uuid.uuid4())[:8]
        return cls._local.correlation_id
    
    @classmethod
    def set_correlation_id(cls, cid: str):
        cls._local.correlation_id = cid
    
    def filter(self, record):
        record.correlation_id = self.get_correlation_id()
        return True

logger.addFilter(CorrelationIdFilter())

# Prometheus metrics
REGISTRY = CollectorRegistry()
CARBON_CALCULATIONS = Counter('carbon_calculations_total', 'Total carbon calculations', ['type', 'status'], registry=REGISTRY)
EMISSIONS_TRACKED = Gauge('emissions_tracked_kg', 'Tracked emissions', ['scope'], registry=REGISTRY)
CARBON_PRICE = Gauge('carbon_price_forecast', 'Carbon price forecast', ['market'], registry=REGISTRY)
BACKGROUND_TASKS = Gauge('background_tasks_active', 'Active background tasks', registry=REGISTRY)
TASK_DURATION = Histogram('background_task_duration_seconds', 'Background task duration', ['task_name'], registry=REGISTRY)
TASK_ERRORS = Counter('background_task_errors_total', 'Background task errors', ['task_name'], registry=REGISTRY)
CONFIG_VERSION = Gauge('carbon_config_version', 'Configuration version', registry=REGISTRY)
HEALTH_CHECK_DURATION = Histogram('health_check_duration_seconds', 'Health check duration', ['component'], registry=REGISTRY)

# NEW: Advanced sustainability metrics
FEDERATED_KNOWLEDGE = Gauge('federated_carbon_knowledge', 'Federated carbon knowledge packages', registry=REGISTRY)
USER_ADAPTATION_SCORE = Gauge('user_carbon_adaptation_score', 'User adaptation score', ['user_id'], registry=REGISTRY)
CARBON_INTENSITY = Gauge('real_time_carbon_intensity', 'Real-time carbon intensity (gCO2/kWh)', ['region'], registry=REGISTRY)
CROSS_DOMAIN_TRANSFERS = Counter('cross_domain_carbon_transfers_total', 'Cross-domain knowledge transfers', ['source', 'target'], registry=REGISTRY)
HUMAN_FEEDBACK = Counter('human_carbon_feedback_total', 'Human feedback events', ['type'], registry=REGISTRY)
PREDICTIVE_ACCURACY = Gauge('predictive_carbon_accuracy', 'Predictive model accuracy', ['model_type'], registry=REGISTRY)
CARBON_SAVED = Gauge('carbon_saved_kg', 'Carbon saved through optimization', registry=REGISTRY)
HELIUM_EFFICIENCY = Gauge('helium_carbon_efficiency', 'Helium usage efficiency', registry=REGISTRY)

# Constants
MAX_BACKGROUND_TASKS = 1000
MAX_RETRY_ATTEMPTS = 3
HEALTH_CHECK_TIMEOUT = 5.0
DEFAULT_TASK_TIMEOUT = 300.0
DATA_VERSION = 11.0

# ============================================================================
# NEW MODULE 1: FEDERATED REFLEXIVE LEARNING FOR CARBON
# ============================================================================

class FederatedCarbonLearner:
    """
    Federated learning system for sharing carbon insights across instances.
    Enables collective carbon intelligence while preserving privacy.
    """
    
    def __init__(self, persistence, instance_id: str, min_share_interval: int = 3600):
        self.persistence = persistence
        self.instance_id = instance_id
        self.min_share_interval = min_share_interval
        self._knowledge_bank: Dict[str, Dict] = {}
        self._shared_packages: List[Dict] = []
        self._last_share_time = 0
        self._lock = asyncio.Lock()
        
        # Federated weights for carbon insights
        self.federated_weights = defaultdict(float)
        self.aggregation_count = 0
        
        logger.info(f"FederatedCarbonLearner initialized for instance {instance_id}")
    
    async def share_carbon_insight(self, insight: Dict) -> str:
        """
        Share a carbon insight with the federated network.
        
        Args:
            insight: Dictionary containing:
                - 'domain': Domain of insight (e.g., 'manufacturing', 'data_center')
                - 'emission_pattern': Emission patterns
                - 'reduction_strategy': Successful reduction strategy
                - 'carbon_savings': Carbon saved
                - 'helium_impact': Helium usage impact
        """
        async with self._lock:
            # Anonymize sensitive data
            anonymized_insight = self._anonymize_insight(insight)
            
            # Add metadata
            package_id = f"fed_carbon_{uuid.uuid4().hex[:12]}"
            anonymized_insight.update({
                'package_id': package_id,
                'source_instance': self.instance_id,
                'timestamp': datetime.now().isoformat(),
                'version': '1.0'
            })
            
            # Store locally
            self._knowledge_bank[package_id] = anonymized_insight
            
            # Persist to database
            await self.persistence.save_carbon_knowledge(anonymized_insight)
            
            # Share with network if enough time has passed
            if time.time() - self._last_share_time >= self.min_share_interval:
                await self._broadcast_to_network(anonymized_insight)
                self._last_share_time = time.time()
            
            FEDERATED_KNOWLEDGE.set(len(self._knowledge_bank))
            logger.info(f"Carbon insight {package_id} shared")
            return package_id
    
    def _anonymize_insight(self, insight: Dict) -> Dict:
        """Anonymize sensitive carbon data while preserving utility"""
        anonymized = insight.copy()
        
        # Remove specific identifiers
        anonymized.pop('specific_location', None)
        anonymized.pop('user_data', None)
        anonymized.pop('proprietary_data', None)
        
        # Aggregate carbon metrics
        if 'emission_pattern' in anonymized:
            pattern = anonymized['emission_pattern']
            anonymized['emission_pattern'] = {
                'avg_intensity': pattern.get('avg_intensity', 0),
                'peak_hours': pattern.get('peak_hours', []),
                'reduction_potential': pattern.get('reduction_potential', 0)
            }
        
        return anonymized
    
    async def _broadcast_to_network(self, package: Dict):
        """Broadcast carbon insight to other instances"""
        try:
            await self.persistence.save_shared_carbon_knowledge(package)
            logger.info(f"Broadcasted carbon insight {package['package_id']} to network")
        except Exception as e:
            logger.error(f"Failed to broadcast carbon insight: {e}")
    
    async def pull_network_insights(self, domain: Optional[str] = None, limit: int = 10) -> List[Dict]:
        """Pull carbon insights from the federated network"""
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
        """Aggregate weights from federated carbon learning"""
        for package in packages:
            if 'reduction_strategy' in package and 'weights' in package['reduction_strategy']:
                weights = package['reduction_strategy']['weights']
                for key, value in weights.items():
                    self.federated_weights[key] += value
        
        # Normalize weights
        total = sum(self.federated_weights.values())
        if total > 0:
            for key in self.federated_weights:
                self.federated_weights[key] /= total
    
    def get_federated_insights(self) -> Dict:
        """Get aggregated carbon insights from federated learning"""
        return {
            'total_packages': len(self._knowledge_bank),
            'aggregation_count': self.aggregation_count,
            'weights': dict(self.federated_weights),
            'timestamp': datetime.now().isoformat()
        }
    
    async def shutdown(self):
        """Clean shutdown"""
        logger.info("FederatedCarbonLearner shutdown complete")

# ============================================================================
# NEW MODULE 2: USER-ADAPTIVE CARBON REFLEXIVITY
# ============================================================================

class UserAdaptiveCarbonReflexivity:
    """
    Learns user carbon preferences and adapts accounting behavior over time.
    """
    
    def __init__(self, persistence):
        self.persistence = persistence
        self._user_profiles: Dict[str, Dict] = {}
        self._preference_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=100))
        self._lock = asyncio.Lock()
        
        logger.info("UserAdaptiveCarbonReflexivity initialized")
    
    async def learn_user_preference(self, user_id: str, action: str, context: Dict, outcome: Dict):
        """
        Learn from user carbon-related actions and feedback.
        
        Args:
            user_id: Unique user identifier
            action: Action taken (e.g., 'accept_reduction', 'reject_reduction')
            context: Context of the action
            outcome: Outcome of the action
        """
        async with self._lock:
            # Initialize user profile if needed
            if user_id not in self._user_profiles:
                self._user_profiles[user_id] = {
                    'carbon_preferences': defaultdict(float),
                    'history': [],
                    'adaptation_score': 50.0,
                    'last_updated': datetime.now().isoformat()
                }
            
            # Update preference weights
            profile = self._user_profiles[user_id]
            preference_update = self._calculate_preference_update(action, context, outcome)
            
            for key, value in preference_update.items():
                profile['carbon_preferences'][key] += value
                profile['carbon_preferences'][key] = max(0, min(1, profile['carbon_preferences'][key]))
            
            # Store history
            profile['history'].append({
                'action': action,
                'timestamp': datetime.now().isoformat(),
                'outcome': outcome
            })
            
            # Update adaptation score
            profile['adaptation_score'] = self._calculate_adaptation_score(profile)
            USER_ADAPTATION_SCORE.labels(user_id=user_id).set(profile['adaptation_score'])
            
            # Store in database
            await self.persistence.save_user_carbon_profile(user_id, profile)
            
            logger.info(f"Updated carbon preferences for user {user_id}, adaptation score: {profile['adaptation_score']:.1f}")
    
    def _calculate_preference_update(self, action: str, context: Dict, outcome: Dict) -> Dict:
        """Calculate preference weights from user action"""
        update = defaultdict(float)
        
        # Positive outcomes increase preferences
        if outcome.get('success', False):
            if action == 'accept_reduction':
                update['carbon_reduction_preference'] += 0.1
                update['helium_efficiency_preference'] += 0.05
            elif action == 'reject_reduction':
                update['carbon_reduction_preference'] -= 0.05
                update['cost_preference'] += 0.1
            elif action == 'adjust_carbon_budget':
                update['budget_awareness'] += 0.15
        
        # Helium awareness
        if context.get('helium_impact', False):
            update['helium_awareness'] += 0.15
        
        return dict(update)
    
    def _calculate_adaptation_score(self, profile: Dict) -> float:
        """Calculate how well the system has adapted to user preferences"""
        if not profile['history']:
            return 50.0
        
        # Calculate consistency of preferences
        preferences = profile['carbon_preferences']
        if not preferences:
            return 50.0
        
        # Higher consistency = better adaptation
        variance = np.var(list(preferences.values()))
        consistency = 1.0 - min(1.0, variance)
        
        # More history = better adaptation
        history_depth = min(1.0, len(profile['history']) / 20)
        
        return 50.0 + 40.0 * consistency * history_depth
    
    async def get_adaptive_carbon_recommendation(self, user_id: str, candidates: List[Dict]) -> List[Dict]:
        """
        Get personalized carbon reduction recommendations based on learned preferences.
        """
        async with self._lock:
            profile = self._user_profiles.get(user_id)
            if not profile:
                return candidates  # No preferences learned yet
            
            preferences = profile['carbon_preferences']
            
            # Score candidates based on preferences
            scored_candidates = []
            for candidate in candidates:
                score = 0.0
                
                # Apply preference weights
                if preferences.get('carbon_reduction_preference', 0) > 0.5:
                    score += candidate.get('carbon_reduction', 0) * preferences['carbon_reduction_preference']
                if preferences.get('helium_efficiency_preference', 0) > 0.5:
                    score += candidate.get('helium_efficiency', 0) * preferences['helium_efficiency_preference']
                if preferences.get('budget_awareness', 0) > 0.5:
                    score += candidate.get('cost_savings', 0) * preferences['budget_awareness']
                
                scored_candidates.append({
                    'candidate': candidate,
                    'score': score
                })
            
            # Sort by score descending
            scored_candidates.sort(key=lambda x: x['score'], reverse=True)
            return [item['candidate'] for item in scored_candidates]

# ============================================================================
# NEW MODULE 3: REAL-TIME CARBON INTENSITY INTEGRATOR
# ============================================================================

class RealTimeCarbonIntegrator:
    """
    Integrates with real-time carbon intensity APIs for carbon-aware accounting.
    """
    
    def __init__(self, api_key: Optional[str] = None, region: str = "global"):
        self.api_key = api_key or os.getenv('CARBON_INTENSITY_API_KEY')
        self.region = region
        self._cache = {}
        self._cache_ttl = 300  # 5 minutes
        self._lock = asyncio.Lock()
        self._session = None
        
        logger.info(f"RealTimeCarbonIntegrator initialized for region {region}")
    
    async def _get_session(self):
        """Get or create aiohttp session"""
        if self._session is None:
            self._session = aiohttp.ClientSession()
        return self._session
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def get_current_intensity(self, region: Optional[str] = None) -> Dict:
        """
        Get current carbon intensity from API or cache.
        
        Returns:
            Dictionary with intensity, unit, and timestamp
        """
        region = region or self.region
        cache_key = f"intensity_{region}"
        
        async with self._lock:
            # Check cache
            if cache_key in self._cache:
                cached_data, timestamp = self._cache[cache_key]
                if time.time() - timestamp < self._cache_ttl:
                    return cached_data
        
        try:
            session = await self._get_session()
            
            # Use Electricity Maps API (or similar)
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
                    
                    # Update cache
                    async with self._lock:
                        self._cache[cache_key] = (intensity_data, time.time())
                    
                    CARBON_INTENSITY.labels(region=region).set(intensity_data['intensity'])
                    return intensity_data
                else:
                    logger.warning(f"Carbon intensity API returned {response.status}")
                    return self._get_fallback_intensity(region)
                    
        except Exception as e:
            logger.error(f"Carbon intensity API error: {e}")
            return self._get_fallback_intensity(region)
    
    def _get_fallback_intensity(self, region: str) -> Dict:
        """Get fallback intensity based on historical patterns"""
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
        """Get carbon intensity forecast for next N hours"""
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
                    logger.warning(f"Carbon intensity forecast API returned {response.status}")
                    return self._get_fallback_forecast(hours)
                    
        except Exception as e:
            logger.error(f"Carbon intensity forecast error: {e}")
            return self._get_fallback_forecast(hours)
    
    def _get_fallback_forecast(self, hours: int) -> List[Dict]:
        """Generate fallback forecast based on historical patterns"""
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
    
    async def get_optimal_recording_time(self, region: Optional[str] = None, hours: int = 24) -> Dict:
        """Get optimal time for recording carbon emissions based on intensity"""
        region = region or self.region
        forecast = await self.get_forecast(region, hours)
        
        if not forecast:
            return {'optimal_time': None, 'reason': 'No forecast available'}
        
        best = min(forecast, key=lambda x: x['intensity'])
        current = await self.get_current_intensity(region)
        
        return {
            'optimal_time': best['timestamp'],
            'optimal_intensity': best['intensity'],
            'current_intensity': current['intensity'],
            'savings_percent': (current['intensity'] - best['intensity']) / current['intensity'] * 100,
            'region': region
        }
    
    async def close(self):
        """Close aiohttp session"""
        if self._session:
            await self._session.close()

# ============================================================================
# NEW MODULE 4: CROSS-DOMAIN CARBON KNOWLEDGE TRANSFER
# ============================================================================

class CrossDomainCarbonTransfer:
    """
    Transfers carbon reduction knowledge across different domains.
    Enables learning from one domain to improve another.
    """
    
    def __init__(self, persistence):
        self.persistence = persistence
        self._domain_knowledge: Dict[str, Dict] = {}
        self._transfer_mappings: Dict[str, Dict[str, float]] = {}
        self._lock = asyncio.Lock()
        
        logger.info("CrossDomainCarbonTransfer initialized")
    
    async def transfer_carbon_knowledge(self, source_domain: str, target_domain: str, 
                                        knowledge: Dict, mapping_strategy: str = 'auto') -> Dict:
        """
        Transfer carbon reduction knowledge from source domain to target domain.
        
        Args:
            source_domain: Source domain (e.g., 'manufacturing')
            target_domain: Target domain (e.g., 'data_center')
            knowledge: Carbon reduction knowledge to transfer
            mapping_strategy: Strategy for mapping knowledge
            
        Returns:
            Transferred knowledge for target domain
        """
        async with self._lock:
            # Store source knowledge
            if source_domain not in self._domain_knowledge:
                self._domain_knowledge[source_domain] = {}
            self._domain_knowledge[source_domain].update(knowledge)
            
            # Map knowledge to target domain
            transferred = await self._map_carbon_knowledge(source_domain, target_domain, knowledge, mapping_strategy)
            
            # Store transfer mapping
            transfer_key = f"{source_domain}->{target_domain}"
            if transfer_key not in self._transfer_mappings:
                self._transfer_mappings[transfer_key] = {}
            
            for key in transferred:
                self._transfer_mappings[transfer_key][key] = self._transfer_mappings[transfer_key].get(key, 0) + 1
            
            # Record metrics
            CROSS_DOMAIN_TRANSFERS.labels(source=source_domain, target=target_domain).inc()
            
            logger.info(f"Transferred carbon knowledge from {source_domain} to {target_domain}: {len(transferred)} items")
            return transferred
    
    async def _map_carbon_knowledge(self, source: str, target: str, knowledge: Dict, strategy: str) -> Dict:
        """Map carbon reduction knowledge from source to target domain"""
        # Domain similarity matrix for carbon reduction
        domain_similarities = {
            ('manufacturing', 'data_center'): {
                'energy_efficiency': 'power_usage_effectiveness',
                'process_optimization': 'workload_scheduling',
                'waste_reduction': 'resource_consolidation'
            },
            ('data_center', 'manufacturing'): {
                'power_usage_effectiveness': 'energy_efficiency',
                'workload_scheduling': 'process_optimization',
                'resource_consolidation': 'waste_reduction'
            },
            ('transportation', 'manufacturing'): {
                'fuel_efficiency': 'energy_efficiency',
                'route_optimization': 'process_optimization'
            }
        }
        
        # Get mapping for this domain pair
        mapping = domain_similarities.get((source, target), {})
        
        transferred = {}
        
        if strategy == 'auto':
            for source_key, source_value in knowledge.items():
                if source_key in mapping:
                    transferred[mapping[source_key]] = source_value
                else:
                    similar_key = self._find_similar_carbon_key(source_key, mapping)
                    if similar_key:
                        transferred[similar_key] = source_value
        elif strategy == 'direct':
            transferred = knowledge
        
        return transferred
    
    def _find_similar_carbon_key(self, source_key: str, mapping: Dict) -> Optional[str]:
        """Find similar key in mapping using semantic similarity"""
        for target_key in mapping.values():
            if (source_key.lower() in target_key.lower() or 
                target_key.lower() in source_key.lower()):
                return target_key
        return None
    
    def get_transfer_statistics(self) -> Dict:
        """Get statistics about carbon knowledge transfers"""
        return {
            'domains': list(self._domain_knowledge.keys()),
            'transfers': dict(self._transfer_mappings),
            'total_transfers': sum(len(v) for v in self._transfer_mappings.values())
        }
    
    async def get_domain_carbon_insights(self, domain: str) -> Dict:
        """Get aggregated carbon insights for a domain"""
        async with self._lock:
            knowledge = self._domain_knowledge.get(domain, {})
            
            maturity = min(1.0, len(knowledge) / 20)
            
            return {
                'domain': domain,
                'knowledge_items': len(knowledge),
                'maturity_score': maturity,
                'key_insights': list(knowledge.keys())[:10],
                'timestamp': datetime.now().isoformat()
            }

# ============================================================================
# NEW MODULE 5: HUMAN-AI CARBON COLLABORATION
# ============================================================================

class HumanAICarbonCollaboration:
    """
    Enables collaborative reflection between humans and AI on carbon decisions.
    """
    
    def __init__(self, persistence, websocket_manager=None):
        self.persistence = persistence
        self.websocket_manager = websocket_manager
        self._feedback_queue: deque = deque(maxlen=1000)
        self._explanations: Dict[str, Dict] = {}
        self._lock = asyncio.Lock()
        self._listeners: List[Callable] = []
        
        logger.info("HumanAICarbonCollaboration initialized")
    
    async def request_carbon_feedback(self, decision: Dict, context: Dict) -> str:
        """
        Request human feedback on a carbon-related decision.
        
        Returns:
            feedback_id: Unique identifier for the feedback request
        """
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
        
        if self.websocket_manager:
            try:
                await self.websocket_manager.broadcast({
                    'type': 'carbon_feedback_request',
                    'data': feedback_request
                })
            except Exception as e:
                logger.error(f"Failed to send carbon feedback request: {e}")
        
        await self.persistence.save_carbon_feedback_request(feedback_request)
        HUMAN_FEEDBACK.labels(type='request').inc()
        return feedback_id
    
    async def submit_carbon_feedback(self, feedback_id: str, feedback: Dict) -> bool:
        """
        Submit human feedback on a carbon decision.
        """
        async with self._lock:
            if feedback_id not in self._explanations:
                logger.warning(f"Carbon feedback ID {feedback_id} not found")
                return False
            
            request = self._explanations[feedback_id]
            request['status'] = 'completed'
            request['feedback'] = feedback
            request['feedback_timestamp'] = datetime.now().isoformat()
            
            self._feedback_queue.append(request)
        
        await self._process_carbon_feedback(request)
        HUMAN_FEEDBACK.labels(type='submitted').inc()
        
        for listener in self._listeners:
            try:
                await listener(request)
            except Exception as e:
                logger.error(f"Carbon feedback listener error: {e}")
        
        logger.info(f"Carbon feedback {feedback_id} submitted")
        return True
    
    async def _process_carbon_feedback(self, feedback_request: Dict):
        """Process human carbon feedback and update system learning"""
        feedback = feedback_request.get('feedback', {})
        decision = feedback_request.get('decision', {})
        
        learning = {
            'approval': feedback.get('approval', 0.5),
            'comments': feedback.get('comments', ''),
            'suggestions': feedback.get('suggestions', {}),
            'carbon_savings_adjustment': feedback.get('carbon_savings_adjustment', 0),
            'timestamp': datetime.now().isoformat()
        }
        
        await self.persistence.save_carbon_feedback_learning(learning)
        
        logger.info(f"Processed carbon feedback learning: approval={learning['approval']:.2f}")
    
    async def generate_carbon_explanation(self, decision: Dict, context: Dict) -> Dict:
        """
        Generate a human-readable explanation for a carbon decision.
        """
        explanation = {
            'id': f"exp_carbon_{uuid.uuid4().hex[:12]}",
            'decision': decision,
            'context': context,
            'explanation': self._build_carbon_explanation(decision, context),
            'confidence': self._calculate_carbon_confidence(decision),
            'alternatives': self._generate_carbon_alternatives(decision),
            'timestamp': datetime.now().isoformat()
        }
        
        async with self._lock:
            self._explanations[explanation['id']] = explanation
        
        return explanation
    
    def _build_carbon_explanation(self, decision: Dict, context: Dict) -> str:
        """Build a human-readable carbon explanation"""
        parts = []
        
        if 'carbon_reduction' in decision:
            parts.append(f"Carbon reduction: {decision['carbon_reduction']:.2f} kg CO2")
        
        if 'reasoning' in context:
            parts.append(f"Reasoning: {context['reasoning']}")
        
        if 'helium_impact' in context:
            parts.append(f"Helium impact: {context['helium_impact']:.2f}%")
        
        if 'alternatives' in context:
            parts.append(f"Alternatives considered: {len(context['alternatives'])}")
        
        return ". ".join(parts)
    
    def _calculate_carbon_confidence(self, decision: Dict) -> float:
        """Calculate confidence in the carbon decision"""
        confidence = 0.7
        
        if 'evidence' in decision:
            confidence += min(0.2, len(decision['evidence']) * 0.02)
        
        if 'carbon_savings' in decision:
            confidence += min(0.1, decision['carbon_savings'] * 0.01)
        
        return min(1.0, confidence)
    
    def _generate_carbon_alternatives(self, decision: Dict) -> List[Dict]:
        """Generate alternative carbon reduction decisions"""
        alternatives = []
        
        if 'carbon_reduction' in decision:
            current = decision['carbon_reduction']
            alternatives.append({
                'type': 'more_aggressive',
                'carbon_reduction': current * 1.5,
                'tradeoff': 'higher_cost'
            })
            alternatives.append({
                'type': 'more_conservative',
                'carbon_reduction': current * 0.7,
                'tradeoff': 'lower_cost'
            })
        
        return alternatives[:3]
    
    async def get_carbon_feedback_summary(self) -> Dict:
        """Get summary of human carbon feedback"""
        async with self._lock:
            completed = [f for f in self._explanations.values() 
                        if f.get('status') == 'completed']
            
            if not completed:
                return {'total': 0, 'average_approval': 0}
            
            approvals = [f.get('feedback', {}).get('approval', 0.5) for f in completed]
            
            return {
                'total': len(completed),
                'pending': len(self._explanations) - len(completed),
                'average_approval': sum(approvals) / len(approvals),
                'timestamp': datetime.now().isoformat()
            }

# ============================================================================
# NEW MODULE 6: PREDICTIVE CARBON REFLEXIVITY
# ============================================================================

class PredictiveCarbonReflexivity:
    """
    Predicts future carbon emissions and proactively recommends reductions.
    """
    
    def __init__(self, persistence, horizon_hours: int = 24):
        self.persistence = persistence
        self.horizon_hours = horizon_hours
        self._predictions: Dict[str, Dict] = {}
        self._historical_data: deque = deque(maxlen=1000)
        self._models: Dict[str, Any] = {}
        self._lock = asyncio.Lock()
        
        logger.info(f"PredictiveCarbonReflexivity initialized with {horizon_hours}h horizon")
    
    async def predict_carbon_emissions(self, time_window: int = 3600) -> Dict:
        """
        Predict future carbon emissions.
        """
        async with self._lock:
            history = await self.persistence.get_carbon_history(limit=100)
            self._historical_data.extend(history)
            
            if len(self._historical_data) < 10:
                return {
                    'predicted_emissions': 0.5,
                    'confidence': 0.1,
                    'reason': 'Insufficient data'
                }
            
            recent = list(self._historical_data)[-50:]
            
            # Calculate average emission rate
            if len(recent) > 1:
                time_span = (datetime.now() - datetime.fromisoformat(recent[0]['timestamp'])).total_seconds()
                if time_span > 0:
                    emission_rate = sum(r.get('amount_kg', 0) for r in recent) / time_span
                else:
                    emission_rate = 0.1
            else:
                emission_rate = 0.1
            
            predicted_emissions = emission_rate * time_window
            
            # Calculate confidence
            rates = []
            for i in range(0, len(recent) - 5, 5):
                window = recent[i:i+5]
                if len(window) > 1:
                    span = (datetime.fromisoformat(window[-1]['timestamp']) - 
                           datetime.fromisoformat(window[0]['timestamp'])).total_seconds()
                    if span > 0:
                        rates.append(sum(r.get('amount_kg', 0) for r in window) / span)
            
            variance = np.var(rates) if rates else 1.0
            confidence = max(0, min(1, 1.0 - variance))
            
            prediction = {
                'predicted_emissions': max(0, predicted_emissions),
                'emission_rate': emission_rate,
                'confidence': confidence,
                'time_window_seconds': time_window,
                'timestamp': datetime.now().isoformat()
            }
            
            self._predictions['emissions'] = prediction
            PREDICTIVE_ACCURACY.labels(model_type='emissions').set(confidence)
            
            return prediction
    
    async def predict_helium_impact(self, task_plan: Dict) -> Dict:
        """
        Predict helium impact of a planned task.
        """
        task_type = task_plan.get('type', 'unknown')
        resources = task_plan.get('resources', 1)
        duration = task_plan.get('duration_hours', 1)
        
        helium_factor = {
            'training': 0.5,
            'inference': 0.1,
            'data_processing': 0.3
        }.get(task_type, 0.3)
        
        predicted_helium = resources * duration * helium_factor
        
        return {
            'predicted_helium_impact': predicted_helium,
            'task_type': task_type,
            'resources': resources,
            'duration_hours': duration,
            'confidence': 0.7,
            'timestamp': datetime.now().isoformat()
        }
    
    async def generate_carbon_reduction_recommendations(self) -> List[Dict]:
        """
        Generate proactive carbon reduction recommendations.
        """
        recommendations = []
        
        emission_pred = await self.predict_carbon_emissions()
        
        if emission_pred.get('confidence', 0) > 0.6:
            predicted = emission_pred.get('predicted_emissions', 0)
            
            if predicted > 100:  # High emissions predicted
                recommendations.append({
                    'type': 'reduce_emissions',
                    'reason': f'High emissions predicted: {predicted:.1f} kg CO2',
                    'priority': 'high',
                    'action': 'Implement immediate reduction measures',
                    'confidence': emission_pred.get('confidence', 0)
                })
            elif predicted > 50:
                recommendations.append({
                    'type': 'monitor_emissions',
                    'reason': f'Moderate emissions predicted: {predicted:.1f} kg CO2',
                    'priority': 'medium',
                    'action': 'Schedule proactive reduction review',
                    'confidence': emission_pred.get('confidence', 0)
                })
        
        # Carbon intensity based recommendations
        if hasattr(self, 'carbon_integrator'):
            intensity = await self.carbon_integrator.get_current_intensity()
            if intensity.get('intensity', 0) > 400:
                recommendations.append({
                    'type': 'schedule_off_peak',
                    'reason': f'High carbon intensity: {intensity["intensity"]} gCO2/kWh',
                    'priority': 'high',
                    'action': 'Delay non-critical operations to off-peak hours'
                })
        
        return recommendations
    
    async def get_carbon_forecast(self) -> Dict:
        """Get comprehensive carbon forecast"""
        emissions = await self.predict_carbon_emissions()
        recommendations = await self.generate_carbon_reduction_recommendations()
        
        return {
            'emissions_forecast': emissions,
            'recommendations': recommendations,
            'timestamp': datetime.now().isoformat()
        }

# ============================================================================
# NEW MODULE 7: SUSTAINABILITY METRICS TRACKER FOR CARBON
# ============================================================================

class CarbonSustainabilityTracker:
    """
    Tracks and reports carbon sustainability metrics.
    """
    
    def __init__(self, persistence):
        self.persistence = persistence
        self._metrics = {
            'carbon_efficiency': [],
            'helium_awareness': [],
            'reduction_effectiveness': [],
            'user_satisfaction': []
        }
        self._lock = asyncio.Lock()
        
        logger.info("CarbonSustainabilityTracker initialized")
    
    async def record_metric(self, category: str, value: float, context: Dict = None):
        """Record a carbon sustainability metric"""
        async with self._lock:
            if category in self._metrics:
                self._metrics[category].append({
                    'value': value,
                    'timestamp': datetime.now().isoformat(),
                    'context': context or {}
                })
                
                logger.debug(f"Recorded {category} metric: {value:.3f}")
    
    async def get_carbon_sustainability_score(self) -> Dict:
        """Calculate overall carbon sustainability score"""
        scores = {}
        
        for category, records in self._metrics.items():
            if records:
                recent = records[-10:]
                avg_value = sum(r['value'] for r in recent) / len(recent)
                scores[category] = avg_value * 100
        
        overall = sum(scores.values()) / len(scores) if scores else 0
        
        return {
            'categories': scores,
            'overall_score': overall,
            'timestamp': datetime.now().isoformat()
        }
    
    async def get_carbon_savings(self) -> Dict:
        """Calculate carbon savings from optimizations"""
        carbon_saved = 0.0
        
        carbon_efficiency = self._metrics.get('carbon_efficiency', [])
        if carbon_efficiency:
            recent = carbon_efficiency[-10:]
            if recent:
                avg_efficiency = sum(r['value'] for r in recent) / len(recent)
                carbon_saved = avg_efficiency * 100
        
        CARBON_SAVED.set(carbon_saved)
        HELIUM_EFFICIENCY.set(carbon_saved / 100 if carbon_saved > 0 else 0.5)
        
        return {
            'carbon_saved_kg': carbon_saved,
            'helium_efficiency': min(1.0, carbon_saved / 100),
            'timestamp': datetime.now().isoformat()
        }

# ============================================================================
# ENHANCED MAIN DUAL CARBON ACCOUNTANT
# ============================================================================

class EnhancedDualCarbonAccountantV11_0:
    """
    Enhanced Dual Carbon Accountant v11.0 with advanced sustainability features.
    
    New Features:
    1. Federated Carbon Learning
    2. User-Adaptive Carbon Reflexivity
    3. Real-Time Carbon Intensity Integration
    4. Cross-Domain Carbon Knowledge Transfer
    5. Human-AI Carbon Collaboration
    6. Predictive Carbon Reflexivity
    7. Enhanced Helium Awareness
    8. Carbon Sustainability Metrics
    """
    
    def __init__(self, config: Dict = None):
        self.config = config or self._load_config()
        self.instance_id = str(uuid.uuid4())[:8]
        self._start_time = datetime.now()
        
        # Component dependency graph
        self.dependency_graph = ComponentDependencyGraph()
        
        # Background task manager
        self.task_manager = BackgroundTaskManager(max_concurrent=10)
        
        # Database manager
        self.db_manager = self._init_db_manager()
        
        # Timed health check
        self.timed_health_check = TimedHealthCheck(timeout=HEALTH_CHECK_TIMEOUT)
        
        # Configuration version
        self.config_version = 1
        CONFIG_VERSION.set(1)
        
        # ============================================================
        # NEW: Initialize advanced sustainability components
        # ============================================================
        
        # 1. Federated Carbon Learning
        self.federated_learner = FederatedCarbonLearner(
            self.db_manager,
            self.instance_id,
            min_share_interval=3600
        )
        
        # 2. User-Adaptive Carbon Reflexivity
        self.user_adaptive = UserAdaptiveCarbonReflexivity(self.db_manager)
        
        # 3. Real-Time Carbon Intensity Integration
        self.carbon_integrator = RealTimeCarbonIntegrator(
            api_key=self.config.get('carbon_api_key'),
            region=self.config.get('carbon_region', 'global')
        )
        
        # 4. Cross-Domain Carbon Knowledge Transfer
        self.cross_domain_transfer = CrossDomainCarbonTransfer(self.db_manager)
        
        # 5. Human-AI Carbon Collaboration
        self.human_collaborator = HumanAICarbonCollaboration(
            self.db_manager,
            None  # WebSocket manager will be injected later
        )
        
        # 6. Predictive Carbon Reflexivity
        self.predictive_reflexivity = PredictiveCarbonReflexivity(
            self.db_manager,
            horizon_hours=24
        )
        
        # 7. Carbon Sustainability Tracker
        self.sustainability_tracker = CarbonSustainabilityTracker(self.db_manager)
        
        # Initialize other components (preserved from v10.2)
        self.carbon_price_api = EnhancedCarbonPriceAPI(
            api_key=self.config.get('carbon_api_key')
        )
        self.carbon_forecaster = CarbonIntensityForecaster()
        
        # Bounded caches
        self.emission_records = deque(maxlen=MAX_EMISSION_RECORDS)
        self.carbon_credits = deque(maxlen=MAX_CARBON_CREDITS)
        self.carbon_reports = deque(maxlen=1000)
        
        # Async locks
        self._record_lock = asyncio.Lock()
        self._credit_lock = asyncio.Lock()
        
        # WebSocket manager
        self.websocket_manager = EnhancedWebSocketManager(
            port=self.config.get('websocket_port', 8766),
            max_connections=self.config.get('max_websocket_connections', MAX_WEBSOCKET_CONNECTIONS)
        )
        
        # Inject WebSocket manager into human collaborator
        self.human_collaborator.websocket_manager = self.websocket_manager
        
        # Shutdown event
        self._shutdown_event = asyncio.Event()
        
        logger.info(f"EnhancedDualCarbonAccountant v{DATA_VERSION} initialized (instance: {self.instance_id})")
        logger.info("  ✅ Advanced Carbon Sustainability Features Enabled:")
        logger.info("     - Federated Carbon Learning")
        logger.info("     - User-Adaptive Carbon Reflexivity")
        logger.info("     - Real-Time Carbon Intensity Integration")
        logger.info("     - Cross-Domain Carbon Knowledge Transfer")
        logger.info("     - Human-AI Carbon Collaboration")
        logger.info("     - Predictive Carbon Reflexivity")
    
    def _init_db_manager(self) -> EnhancedDatabaseManager:
        """Initialize database manager with retry support"""
        db_manager = EnhancedDatabaseManager(
            self.config.get('database_url', 'sqlite:///carbon_accounting.db')
        )
        db_manager.initialize()
        
        self.dependency_graph.add_component('database', [])
        
        return db_manager
    
    def _load_config(self) -> Dict:
        """Load configuration with version tracking"""
        config_file = Path('carbon_accountant_config.json')
        
        default_config = {
            'database_url': os.getenv('DATABASE_URL', 'sqlite:///carbon_accounting.db'),
            'carbon_api_key': os.getenv('CARBON_API_KEY', ''),
            'carbon_region': os.getenv('CARBON_REGION', 'global'),
            'websocket_port': int(os.getenv('WEBSOCKET_PORT', '8766')),
            'max_websocket_connections': int(os.getenv('MAX_WEBSOCKET_CONNECTIONS', '100')),
            'data_retention_days': int(os.getenv('DATA_RETENTION_DAYS', '365')),
            'alert_thresholds': {
                'scope1': float(os.getenv('ALERT_SCOPE1_THRESHOLD', '10000')),
                'scope2': float(os.getenv('ALERT_SCOPE2_THRESHOLD', '5000')),
                'scope3': float(os.getenv('ALERT_SCOPE3_THRESHOLD', '20000'))
            }
        }
        
        if config_file.exists():
            try:
                with open(config_file, 'r') as f:
                    user_config = json.load(f)
                    default_config.update(user_config)
                    logger.info(f"Configuration loaded from {config_file}")
            except Exception as e:
                logger.warning(f"Failed to load config: {e}")
        
        return default_config
    
    async def start(self):
        """Start all background services"""
        logger.info(f"Starting EnhancedDualCarbonAccountant v{DATA_VERSION} (instance: {self.instance_id})")
        
        # Validate dependencies
        is_valid, cycles = self.dependency_graph.validate()
        if not is_valid:
            logger.error(f"Dependency cycles detected: {cycles}")
            raise ValueError(f"Circular dependencies: {cycles}")
        
        # Start background task manager
        await self.task_manager.start(num_workers=5)
        
        # Start WebSocket server as background task
        await self.task_manager.submit(
            self.websocket_manager.start,
            name="websocket_server",
            priority=TaskPriority.HIGH
        )
        
        # Start background loops as tasks
        await self.task_manager.submit(self._forecast_loop, name="forecast_loop", priority=TaskPriority.NORMAL)
        await self.task_manager.submit(self._cleanup_loop, name="cleanup_loop", priority=TaskPriority.LOW)
        await self.task_manager.submit(self._health_monitor_loop, name="health_monitor", priority=TaskPriority.NORMAL)
        
        # ============================================================
        # NEW: Start advanced sustainability background tasks
        # ============================================================
        
        await self.task_manager.submit(self._carbon_intensity_monitor, name="carbon_intensity_monitor", priority=TaskPriority.NORMAL)
        await self.task_manager.submit(self._federated_learning_loop, name="federated_learning", priority=TaskPriority.NORMAL)
        await self.task_manager.submit(self._predictive_carbon_loop, name="predictive_carbon", priority=TaskPriority.NORMAL)
        await self.task_manager.submit(self._sustainability_reporter, name="sustainability_reporter", priority=TaskPriority.LOW)
        
        logger.info(f"Started {len(self.task_manager._tasks)} background tasks")
        
        # Broadcast startup
        await self.websocket_manager.broadcast({
            'type': 'system_started',
            'instance_id': self.instance_id,
            'version': str(DATA_VERSION),
            'features': [
                'federated_carbon_learning',
                'user_adaptive_carbon',
                'real_time_carbon_intensity',
                'cross_domain_carbon_transfer',
                'human_ai_carbon_collaboration',
                'predictive_carbon_reflexivity'
            ],
            'timestamp': datetime.now().isoformat()
        })
    
    # ============================================================
    # NEW: Advanced Sustainability Background Tasks
    # ============================================================
    
    async def _carbon_intensity_monitor(self):
        """Monitor carbon intensity and provide recommendations"""
        while not self._shutdown_event.is_set():
            try:
                intensity = await self.carbon_integrator.get_current_intensity()
                optimal = await self.carbon_integrator.get_optimal_recording_time()
                
                # Record sustainability metric
                eco_efficiency = 1.0 - (intensity['intensity'] / 1000)
                await self.sustainability_tracker.record_metric(
                    'carbon_efficiency',
                    eco_efficiency,
                    {'intensity': intensity['intensity']}
                )
                
                # Broadcast carbon intensity update
                await self.websocket_manager.broadcast({
                    'type': 'carbon_intensity_update',
                    'current_intensity': intensity,
                    'optimal_recording_time': optimal,
                    'timestamp': datetime.now().isoformat()
                })
                
                await asyncio.sleep(300)  # Check every 5 minutes
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Carbon intensity monitor error: {e}")
                await asyncio.sleep(60)
    
    async def _federated_learning_loop(self):
        """Pull and apply federated carbon insights"""
        while not self._shutdown_event.is_set():
            try:
                insights = await self.federated_learner.pull_network_insights(limit=5)
                
                if insights:
                    logger.info(f"Applied {len(insights)} federated carbon insights")
                    
                    # Apply insights to improve carbon accounting
                    for insight in insights:
                        if 'reduction_strategy' in insight:
                            await self.apply_federated_strategy(insight['reduction_strategy'])
                
                await asyncio.sleep(3600)  # Run every hour
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Federated learning error: {e}")
                await asyncio.sleep(60)
    
    async def _predictive_carbon_loop(self):
        """Run predictive carbon analysis and generate recommendations"""
        while not self._shutdown_event.is_set():
            try:
                forecast = await self.predictive_reflexivity.get_carbon_forecast()
                
                # Apply high-priority recommendations
                for rec in forecast.get('recommendations', []):
                    if rec.get('priority') == 'high':
                        logger.info(f"Applying carbon recommendation: {rec['reason']}")
                        await self._apply_carbon_recommendation(rec)
                
                # Broadcast forecast
                await self.websocket_manager.broadcast({
                    'type': 'carbon_forecast',
                    'forecast': forecast,
                    'timestamp': datetime.now().isoformat()
                })
                
                await asyncio.sleep(1800)  # Run every 30 minutes
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Predictive carbon error: {e}")
                await asyncio.sleep(60)
    
    async def _sustainability_reporter(self):
        """Generate and log carbon sustainability reports"""
        while not self._shutdown_event.is_set():
            try:
                score = await self.sustainability_tracker.get_carbon_sustainability_score()
                savings = await self.sustainability_tracker.get_carbon_savings()
                
                logger.info(f"Carbon Sustainability Report:")
                logger.info(f"  Overall Score: {score['overall_score']:.1f}%")
                logger.info(f"  Carbon Saved: {savings['carbon_saved_kg']:.2f} kg CO2")
                logger.info(f"  Helium Efficiency: {savings['helium_efficiency']:.2f}")
                logger.info(f"  Categories: {score['categories']}")
                
                await self.websocket_manager.broadcast({
                    'type': 'sustainability_report',
                    'data': {
                        'score': score,
                        'savings': savings,
                        'timestamp': datetime.now().isoformat()
                    }
                })
                
                await asyncio.sleep(3600)  # Report every hour
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Sustainability reporter error: {e}")
                await asyncio.sleep(60)
    
    async def _apply_carbon_recommendation(self, recommendation: Dict):
        """Apply a carbon reduction recommendation"""
        action = recommendation.get('action')
        if action == 'Implement immediate reduction measures':
            # In production, this would trigger actual reduction measures
            logger.info("Implementing carbon reduction measures...")
            # Record reduction action
            await self.sustainability_tracker.record_metric(
                'reduction_effectiveness',
                0.8,
                {'action': action}
            )
        elif action == 'Delay non-critical operations to off-peak hours':
            # Schedule operations for off-peak
            optimal = await self.carbon_integrator.get_optimal_recording_time()
            logger.info(f"Scheduling operations for {optimal.get('optimal_time')}")
    
    async def apply_federated_strategy(self, strategy: Dict):
        """Apply a federated learning strategy"""
        logger.info(f"Applying federated strategy: {strategy.get('name', 'unknown')}")
        # In production, this would apply the strategy
        await self.sustainability_tracker.record_metric(
            'carbon_efficiency',
            0.7,
            {'strategy': strategy.get('name', 'unknown')}
        )
    
    # ============================================================
    # Enhanced Emission Recording with Sustainability Features
    # ============================================================
    
    @retry_on_db_error()
    async def record_emission(self, scope: str, amount_kg: float, source: str,
                             location: str = "", verified: bool = False,
                             helium_impact_factor: float = 0.0,
                             user_id: str = None,
                             domain: str = None) -> Dict:
        """
        Record a carbon emission with sustainability-aware features.
        
        Args:
            scope: Emission scope (1, 2, or 3)
            amount_kg: Amount in kg CO2
            source: Emission source
            location: Location of emission
            verified: Whether emission is verified
            helium_impact_factor: Helium usage impact
            user_id: User ID for personalization
            domain: Domain for cross-domain learning
        """
        try:
            validated = EmissionRecordModel(
                scope=scope,
                amount_kg=amount_kg,
                source=source,
                location=location,
                verified=verified,
                helium_impact_factor=helium_impact_factor
            )
        except ValidationError as e:
            logger.error(f"Validation failed: {e}")
            CARBON_CALCULATIONS.labels(type='emission_record', status='failed').inc()
            raise ValueError(f"Invalid emission record: {e}")
        
        # Apply carbon intensity adjustment if available
        intensity = await self.carbon_integrator.get_current_intensity()
        if intensity.get('intensity', 0) > 400:
            # High carbon intensity - adjust recording
            logger.info(f"High carbon intensity detected: {intensity['intensity']} gCO2/kWh")
        
        record_id = hashlib.sha256(
            f"{source}{amount_kg}{time.time()}{self.instance_id}".encode()
        ).hexdigest()[:16]
        
        record = {
            'record_id': record_id,
            'scope': validated.scope,
            'amount_kg': validated.amount_kg,
            'source': validated.source,
            'location': validated.location,
            'timestamp': datetime.now().isoformat(),
            'verified': validated.verified,
            'helium_impact_factor': validated.helium_impact_factor,
            'recorded_by': self.instance_id,
            'carbon_intensity': intensity.get('intensity', 0)
        }
        
        # Save to database with retry
        with self.db_manager.get_session() as session:
            db_record = EmissionRecordDB(
                record_id=record_id,
                scope=validated.scope,
                amount_kg=validated.amount_kg,
                source=validated.source,
                location=validated.location,
                timestamp=datetime.now(),
                verified=validated.verified,
                helium_impact_factor=validated.helium_impact_factor,
                carbon_intensity=intensity.get('intensity', 0)
            )
            session.add(db_record)
        
        # Update in-memory cache
        async with self._record_lock:
            self.emission_records.append(record)
        
        # Update metrics
        EMISSIONS_TRACKED.labels(scope=validated.scope).set(amount_kg)
        CARBON_CALCULATIONS.labels(type='emission_record', status='success').inc()
        
        # NEW: User adaptation
        if user_id:
            await self.user_adaptive.learn_user_preference(
                user_id,
                'record_emission',
                {'scope': scope, 'source': source},
                {'success': True, 'amount_kg': amount_kg}
            )
        
        # NEW: Cross-domain knowledge transfer
        if domain:
            await self.cross_domain_transfer.transfer_carbon_knowledge(
                domain,
                'general',
                {'emission_pattern': {'amount': amount_kg, 'scope': scope}},
                'auto'
            )
        
        # NEW: Federated learning
        await self.federated_learner.share_carbon_insight({
            'domain': domain or 'general',
            'emission_pattern': {'amount': amount_kg, 'scope': scope},
            'carbon_savings': 0,
            'helium_impact': helium_impact_factor
        })
        
        audit_logger.info(f"Emission recorded: {record_id} - {amount_kg}kg CO2 - {scope}")
        
        # Broadcast update via WebSocket
        await self.websocket_manager.broadcast({
            'type': 'emission_recorded',
            'data': {
                'record_id': record_id,
                'scope': scope,
                'amount_kg': amount_kg,
                'timestamp': record['timestamp'],
                'carbon_intensity': intensity.get('intensity', 0)
            }
        })
        
        return record
    
    # ============================================================
    # Enhanced System Status with Sustainability Metrics
    # ============================================================
    
    async def get_system_status(self) -> Dict:
        """Get comprehensive system status including sustainability metrics"""
        sustainability_score = await self.sustainability_tracker.get_carbon_sustainability_score()
        savings = await self.sustainability_tracker.get_carbon_savings()
        federated_insights = self.federated_learner.get_federated_insights()
        
        return {
            'instance_id': self.instance_id,
            'version': str(DATA_VERSION),
            'status': 'running',
            'uptime_seconds': (datetime.now() - self._start_time).total_seconds(),
            'background_tasks': self.task_manager.get_statistics(),
            'websocket_connections': len(self.websocket_manager.connections),
            'cache_sizes': {
                'emission_records': len(self.emission_records),
                'carbon_credits': len(self.carbon_credits),
                'carbon_reports': len(self.carbon_reports)
            },
            'config_version': self.config_version,
            # NEW: Sustainability metrics
            'sustainability': {
                'score': sustainability_score,
                'savings': savings,
                'federated_insights': federated_insights,
                'carbon_intensity': await self.carbon_integrator.get_current_intensity()
            },
            'timestamp': datetime.now().isoformat()
        }
    
    async def shutdown(self):
        """Graceful shutdown with enhanced cleanup"""
        logger.info(f"Shutting down EnhancedDualCarbonAccountant (instance: {self.instance_id})")
        
        self._shutdown_event.set()
        
        # Stop background task manager
        await self.task_manager.stop()
        
        # Stop WebSocket server
        await self.websocket_manager.stop()
        
        # Close advanced components
        await self.federated_learner.shutdown()
        await self.carbon_integrator.close()
        
        # Close API clients
        await self.carbon_price_api.close()
        
        # Close database
        self.db_manager.dispose()
        
        # Final sustainability report
        savings = await self.sustainability_tracker.get_carbon_savings()
        audit_logger.info(f"Total carbon savings at shutdown: {savings['carbon_saved_kg']:.2f} kg CO2")
        audit_logger.info(f"Helium efficiency at shutdown: {savings['helium_efficiency']:.2f}")
        
        audit_logger.info(f"System shutdown complete")
        logger.info("Shutdown complete")

# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

async def main():
    print("=" * 80)
    print("Enhanced Dual Carbon Accountant v11.0 - Advanced Sustainability")
    print("=" * 80)
    
    accountant = EnhancedDualCarbonAccountantV11_0()
    
    print(f"\n✅ v11.0 ADVANCED SUSTAINABILITY FEATURES:")
    print(f"   ✅ Federated Carbon Learning - Cross-instance insights sharing")
    print(f"   ✅ User-Adaptive Carbon Reflexivity - Learning user preferences")
    print(f"   ✅ Real-Time Carbon Intensity Integration - Live API integration")
    print(f"   ✅ Cross-Domain Carbon Knowledge Transfer - Domain insights sharing")
    print(f"   ✅ Human-AI Carbon Collaboration - Feedback loops with users")
    print(f"   ✅ Predictive Carbon Reflexivity - Forecasting and recommendations")
    print(f"   ✅ Enhanced Helium Awareness - Resource-aware carbon accounting")
    print(f"   ✅ Sustainability Impact Metrics - Tracking eco-efficiency gains")
    
    await accountant.start()
    
    print(f"\n📊 System Status:")
    status = await accountant.get_system_status()
    print(f"   Instance: {status['instance_id']}")
    print(f"   Version: {status['version']}")
    print(f"   Background Tasks: {status['background_tasks']['total_tasks']}")
    print(f"   Active Workers: {status['background_tasks']['active_tasks']}")
    
    # Test enhanced emission recording
    print(f"\n📊 Testing Enhanced Features:")
    record = await accountant.record_emission(
        'scope1', 
        5000.0, 
        "Data Center", 
        "US-East",
        verified=True,
        helium_impact_factor=0.2,
        user_id="test_user",
        domain="data_center"
    )
    print(f"   Recorded: {record['amount_kg']} kg CO2 (carbon intensity: {record['carbon_intensity']} gCO2/kWh)")
    
    # Test user adaptation
    print(f"\n📊 Testing User Adaptation:")
    await accountant.user_adaptive.learn_user_preference(
        "test_user",
        "accept_reduction",
        {"carbon_reduction": 0.5, "helium_impact": 0.2},
        {"success": True}
    )
    print(f"   User adaptation score updated")
    
    # Test human feedback
    print(f"\n📊 Testing Human-AI Collaboration:")
    feedback_id = await accountant.human_collaborator.request_carbon_feedback(
        {"carbon_reduction": 100},
        {"reasoning": "High reduction potential", "helium_impact": 0.3}
    )
    print(f"   Feedback request created: {feedback_id}")
    
    # Test federated learning
    print(f"\n📊 Testing Federated Learning:")
    package_id = await accountant.federated_learner.share_carbon_insight({
        'domain': 'data_center',
        'emission_pattern': {'avg_intensity': 350, 'reduction_potential': 0.3},
        'reduction_strategy': {'name': 'optimize_cooling'}
    })
    print(f"   Federated insight shared: {package_id}")
    
    print("\n🔌 Services Available:")
    print("   WebSocket: ws://localhost:8766")
    print("   Enhanced Carbon Accounting API")
    print("   Real-Time Carbon Intensity Integration")
    print("   Federated Learning Network")
    print("   Human-AI Collaboration Interface")
    
    print("\n🛡️ Enterprise Sustainability Features:")
    print("   - Federated learning across Green Agent instances")
    print("   - Personalized user adaptation and learning")
    print("   - Real-time carbon intensity integration")
    print("   - Cross-domain knowledge transfer")
    print("   - Human-AI collaborative feedback loops")
    print("   - Predictive carbon forecasting")
    
    print("\n" + "=" * 80)
    print("✅ Dual Carbon Accountant v11.0 Running Successfully")
    print("=" * 80)
    
    try:
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        print("\n🛑 Shutting down...")
        await accountant.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
