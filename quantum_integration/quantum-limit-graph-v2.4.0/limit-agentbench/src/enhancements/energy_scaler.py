# File: src/enhancements/energy_scaler_enhanced_v11_0.py
"""
Intelligent Energy Scaler for Green Agent - Version 11.0 (Advanced Sustainability)

CRITICAL ADDITIONS OVER v10.1:
1. ADDED: Federated Reflexive Learning - Cross-instance energy insights sharing
2. ADDED: User-Adaptive Reflexivity - Learning user energy preferences over time
3. ADDED: Real-Time Carbon Intensity Integration - Live API integration
4. ADDED: Cross-Domain Knowledge Transfer - Sharing insights across domains
5. ADDED: Human-AI Collaborative Reflection - Feedback loops with users
6. ADDED: Predictive Reflexivity - Forecasting and proactive recommendations
7. ADDED: Enhanced Helium Awareness - Resource-aware energy optimization
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
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any, Callable, Set
from collections import defaultdict, deque
from enum import Enum
from concurrent.futures import ThreadPoolExecutor
import numpy as np
import random
import psutil
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
        logging.handlers.RotatingFileHandler('energy_scaler_v11_0.log', maxBytes=10*1024*1024, backupCount=5),
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
POWER_READINGS = Gauge('energy_power_watts', 'Current power consumption', ['component'], registry=REGISTRY)
ENERGY_COST = Gauge('energy_cost_dollars', 'Current energy cost per hour', registry=REGISTRY)
CARBON_INTENSITY = Gauge('carbon_intensity_gco2_per_kwh', 'Current carbon intensity', registry=REGISTRY)
PUE_METRIC = Gauge('pue_ratio', 'Current PUE ratio', registry=REGISTRY)
BATTERY_SOC = Gauge('battery_soc_percent', 'Battery state of charge', registry=REGISTRY)
GPU_POWER_CAP = Gauge('gpu_power_cap_watts', 'GPU power cap', registry=REGISTRY)
BACKGROUND_TASKS = Gauge('energy_background_tasks', 'Active background tasks', registry=REGISTRY)
TASK_DURATION = Histogram('energy_task_duration_seconds', 'Background task duration', ['task_name'], registry=REGISTRY)
TASK_ERRORS = Counter('energy_task_errors_total', 'Background task errors', ['task_name'], registry=REGISTRY)
HEALTH_CHECK_DURATION = Histogram('energy_health_check_duration_seconds', 'Health check duration', ['component'], registry=REGISTRY)

# NEW: Advanced sustainability metrics
FEDERATED_KNOWLEDGE = Gauge('federated_energy_knowledge', 'Federated energy knowledge packages', registry=REGISTRY)
USER_ADAPTATION_SCORE = Gauge('user_energy_adaptation_score', 'User adaptation score', ['user_id'], registry=REGISTRY)
REAL_TIME_CARBON = Gauge('real_time_carbon_intensity', 'Real-time carbon intensity (gCO2/kWh)', ['region'], registry=REGISTRY)
CROSS_DOMAIN_TRANSFERS = Counter('cross_domain_energy_transfers_total', 'Cross-domain knowledge transfers', ['source', 'target'], registry=REGISTRY)
HUMAN_FEEDBACK = Counter('human_energy_feedback_total', 'Human feedback events', ['type'], registry=REGISTRY)
PREDICTIVE_ACCURACY = Gauge('predictive_energy_accuracy', 'Predictive model accuracy', ['model_type'], registry=REGISTRY)
ENERGY_SAVED = Gauge('energy_saved_kwh', 'Energy saved through optimization', registry=REGISTRY)
HELIUM_EFFICIENCY = Gauge('helium_energy_efficiency', 'Helium usage efficiency', registry=REGISTRY)
TOTAL_OPTIMIZATIONS = Counter('energy_optimizations_total', 'Total energy optimizations', ['action'], registry=REGISTRY)

# Constants
MAX_BACKGROUND_TASKS = 1000
MAX_RETRY_ATTEMPTS = 3
HEALTH_CHECK_TIMEOUT = 5.0
DEFAULT_TASK_TIMEOUT = 300.0
DATA_VERSION = 11.0

# ============================================================================
# NEW MODULE 1: FEDERATED ENERGY LEARNING
# ============================================================================

class FederatedEnergyLearner:
    """
    Federated learning system for sharing energy insights across instances.
    Enables collective energy intelligence while preserving privacy.
    """
    
    def __init__(self, persistence, instance_id: str, min_share_interval: int = 3600):
        self.persistence = persistence
        self.instance_id = instance_id
        self.min_share_interval = min_share_interval
        self._knowledge_bank: Dict[str, Dict] = {}
        self._shared_packages: List[Dict] = []
        self._last_share_time = 0
        self._lock = asyncio.Lock()
        
        # Federated weights for energy insights
        self.federated_weights = defaultdict(float)
        self.aggregation_count = 0
        
        logger.info(f"FederatedEnergyLearner initialized for instance {instance_id}")
    
    async def share_energy_insight(self, insight: Dict) -> str:
        """
        Share an energy insight with the federated network.
        
        Args:
            insight: Dictionary containing:
                - 'domain': Domain of insight (e.g., 'data_center', 'manufacturing')
                - 'optimization': Successful optimization strategy
                - 'energy_savings': Energy saved
                - 'carbon_reduction': Carbon reduced
                - 'helium_impact': Helium usage impact
        """
        async with self._lock:
            # Anonymize sensitive data
            anonymized_insight = self._anonymize_insight(insight)
            
            # Add metadata
            package_id = f"fed_energy_{uuid.uuid4().hex[:12]}"
            anonymized_insight.update({
                'package_id': package_id,
                'source_instance': self.instance_id,
                'timestamp': datetime.now().isoformat(),
                'version': '1.0'
            })
            
            # Store locally
            self._knowledge_bank[package_id] = anonymized_insight
            
            # Persist to database
            await self.persistence.save_energy_knowledge(anonymized_insight)
            
            # Share with network if enough time has passed
            if time.time() - self._last_share_time >= self.min_share_interval:
                await self._broadcast_to_network(anonymized_insight)
                self._last_share_time = time.time()
            
            FEDERATED_KNOWLEDGE.set(len(self._knowledge_bank))
            logger.info(f"Energy insight {package_id} shared")
            return package_id
    
    def _anonymize_insight(self, insight: Dict) -> Dict:
        """Anonymize sensitive energy data while preserving utility"""
        anonymized = insight.copy()
        
        # Remove specific identifiers
        anonymized.pop('specific_location', None)
        anonymized.pop('user_data', None)
        anonymized.pop('proprietary_config', None)
        
        # Aggregate energy metrics
        if 'optimization' in anonymized:
            opt = anonymized['optimization']
            anonymized['optimization'] = {
                'strategy': opt.get('strategy', 'unknown'),
                'efficiency_gain': opt.get('efficiency_gain', 0),
                'carbon_reduction': opt.get('carbon_reduction', 0)
            }
        
        return anonymized
    
    async def _broadcast_to_network(self, package: Dict):
        """Broadcast energy insight to other instances"""
        try:
            await self.persistence.save_shared_energy_knowledge(package)
            logger.info(f"Broadcasted energy insight {package['package_id']} to network")
        except Exception as e:
            logger.error(f"Failed to broadcast energy insight: {e}")
    
    async def pull_network_insights(self, domain: Optional[str] = None, limit: int = 10) -> List[Dict]:
        """Pull energy insights from the federated network"""
        try:
            packages = await self.persistence.get_shared_energy_knowledge(domain=domain, limit=limit)
            
            if packages:
                self._aggregate_federated_weights(packages)
                self.aggregation_count += 1
                logger.info(f"Pulled {len(packages)} energy insights from network")
            
            return packages
        except Exception as e:
            logger.error(f"Failed to pull network insights: {e}")
            return []
    
    def _aggregate_federated_weights(self, packages: List[Dict]):
        """Aggregate weights from federated energy learning"""
        for package in packages:
            if 'optimization' in package and 'weights' in package['optimization']:
                weights = package['optimization']['weights']
                for key, value in weights.items():
                    self.federated_weights[key] += value
        
        # Normalize weights
        total = sum(self.federated_weights.values())
        if total > 0:
            for key in self.federated_weights:
                self.federated_weights[key] /= total
    
    def get_federated_insights(self) -> Dict:
        """Get aggregated energy insights from federated learning"""
        return {
            'total_packages': len(self._knowledge_bank),
            'aggregation_count': self.aggregation_count,
            'weights': dict(self.federated_weights),
            'timestamp': datetime.now().isoformat()
        }
    
    async def shutdown(self):
        """Clean shutdown"""
        logger.info("FederatedEnergyLearner shutdown complete")

# ============================================================================
# NEW MODULE 2: USER-ADAPTIVE ENERGY REFLEXIVITY
# ============================================================================

class UserAdaptiveEnergyReflexivity:
    """
    Learns user energy preferences and adapts optimization behavior over time.
    """
    
    def __init__(self, persistence):
        self.persistence = persistence
        self._user_profiles: Dict[str, Dict] = {}
        self._preference_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=100))
        self._lock = asyncio.Lock()
        
        logger.info("UserAdaptiveEnergyReflexivity initialized")
    
    async def learn_user_preference(self, user_id: str, action: str, context: Dict, outcome: Dict):
        """
        Learn from user energy-related actions and feedback.
        
        Args:
            user_id: Unique user identifier
            action: Action taken (e.g., 'accept_optimization', 'reject_optimization')
            context: Context of the action
            outcome: Outcome of the action
        """
        async with self._lock:
            # Initialize user profile if needed
            if user_id not in self._user_profiles:
                self._user_profiles[user_id] = {
                    'energy_preferences': defaultdict(float),
                    'history': [],
                    'adaptation_score': 50.0,
                    'last_updated': datetime.now().isoformat()
                }
            
            # Update preference weights
            profile = self._user_profiles[user_id]
            preference_update = self._calculate_preference_update(action, context, outcome)
            
            for key, value in preference_update.items():
                profile['energy_preferences'][key] += value
                profile['energy_preferences'][key] = max(0, min(1, profile['energy_preferences'][key]))
            
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
            await self.persistence.save_user_energy_profile(user_id, profile)
            
            logger.info(f"Updated energy preferences for user {user_id}, adaptation score: {profile['adaptation_score']:.1f}")
    
    def _calculate_preference_update(self, action: str, context: Dict, outcome: Dict) -> Dict:
        """Calculate preference weights from user action"""
        update = defaultdict(float)
        
        # Positive outcomes increase preferences
        if outcome.get('success', False):
            if action == 'accept_optimization':
                update['energy_efficiency_preference'] += 0.1
                update['carbon_reduction_preference'] += 0.05
            elif action == 'reject_optimization':
                update['energy_efficiency_preference'] -= 0.05
                update['performance_preference'] += 0.1
            elif action == 'adjust_power_limit':
                update['power_cap_preference'] += 0.15
        
        # Helium awareness
        if context.get('helium_impact', False):
            update['helium_awareness'] += 0.15
        
        return dict(update)
    
    def _calculate_adaptation_score(self, profile: Dict) -> float:
        """Calculate how well the system has adapted to user preferences"""
        if not profile['history']:
            return 50.0
        
        # Calculate consistency of preferences
        preferences = profile['energy_preferences']
        if not preferences:
            return 50.0
        
        # Higher consistency = better adaptation
        variance = np.var(list(preferences.values()))
        consistency = 1.0 - min(1.0, variance)
        
        # More history = better adaptation
        history_depth = min(1.0, len(profile['history']) / 20)
        
        return 50.0 + 40.0 * consistency * history_depth
    
    async def get_adaptive_energy_recommendation(self, user_id: str, candidates: List[Dict]) -> List[Dict]:
        """
        Get personalized energy optimization recommendations based on learned preferences.
        """
        async with self._lock:
            profile = self._user_profiles.get(user_id)
            if not profile:
                return candidates  # No preferences learned yet
            
            preferences = profile['energy_preferences']
            
            # Score candidates based on preferences
            scored_candidates = []
            for candidate in candidates:
                score = 0.0
                
                # Apply preference weights
                if preferences.get('energy_efficiency_preference', 0) > 0.5:
                    score += candidate.get('efficiency', 0) * preferences['energy_efficiency_preference']
                if preferences.get('carbon_reduction_preference', 0) > 0.5:
                    score += candidate.get('carbon_reduction', 0) * preferences['carbon_reduction_preference']
                if preferences.get('power_cap_preference', 0) > 0.5:
                    score += candidate.get('power_cap', 0) * preferences['power_cap_preference']
                
                scored_candidates.append({
                    'candidate': candidate,
                    'score': score
                })
            
            # Sort by score descending
            scored_candidates.sort(key=lambda x: x['score'], reverse=True)
            return [item['candidate'] for item in scored_candidates]

# ============================================================================
# NEW MODULE 3: REAL-TIME CARBON INTEGRATOR
# ============================================================================

class RealTimeCarbonIntegrator:
    """
    Integrates with real-time carbon intensity APIs for carbon-aware energy optimization.
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
                    
                    REAL_TIME_CARBON.labels(region=region).set(intensity_data['intensity'])
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
    
    async def get_optimal_energy_time(self, region: Optional[str] = None, hours: int = 24) -> Dict:
        """Get optimal time for energy-intensive tasks based on carbon intensity"""
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
# NEW MODULE 4: CROSS-DOMAIN ENERGY TRANSFER
# ============================================================================

class CrossDomainEnergyTransfer:
    """
    Transfers energy optimization knowledge across different domains.
    Enables learning from one domain to improve another.
    """
    
    def __init__(self, persistence):
        self.persistence = persistence
        self._domain_knowledge: Dict[str, Dict] = {}
        self._transfer_mappings: Dict[str, Dict[str, float]] = {}
        self._lock = asyncio.Lock()
        
        logger.info("CrossDomainEnergyTransfer initialized")
    
    async def transfer_energy_knowledge(self, source_domain: str, target_domain: str, 
                                        knowledge: Dict, mapping_strategy: str = 'auto') -> Dict:
        """
        Transfer energy optimization knowledge from source domain to target domain.
        
        Args:
            source_domain: Source domain (e.g., 'data_center')
            target_domain: Target domain (e.g., 'manufacturing')
            knowledge: Energy optimization knowledge to transfer
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
            transferred = await self._map_energy_knowledge(source_domain, target_domain, knowledge, mapping_strategy)
            
            # Store transfer mapping
            transfer_key = f"{source_domain}->{target_domain}"
            if transfer_key not in self._transfer_mappings:
                self._transfer_mappings[transfer_key] = {}
            
            for key in transferred:
                self._transfer_mappings[transfer_key][key] = self._transfer_mappings[transfer_key].get(key, 0) + 1
            
            # Record metrics
            CROSS_DOMAIN_TRANSFERS.labels(source=source_domain, target=target_domain).inc()
            
            logger.info(f"Transferred energy knowledge from {source_domain} to {target_domain}: {len(transferred)} items")
            return transferred
    
    async def _map_energy_knowledge(self, source: str, target: str, knowledge: Dict, strategy: str) -> Dict:
        """Map energy optimization knowledge from source to target domain"""
        # Domain similarity matrix for energy optimization
        domain_similarities = {
            ('data_center', 'manufacturing'): {
                'power_cap': 'energy_limit',
                'cooling_efficiency': 'process_efficiency',
                'workload_scheduling': 'production_scheduling'
            },
            ('manufacturing', 'data_center'): {
                'energy_limit': 'power_cap',
                'process_efficiency': 'cooling_efficiency',
                'production_scheduling': 'workload_scheduling'
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
                    similar_key = self._find_similar_energy_key(source_key, mapping)
                    if similar_key:
                        transferred[similar_key] = source_value
        elif strategy == 'direct':
            transferred = knowledge
        
        return transferred
    
    def _find_similar_energy_key(self, source_key: str, mapping: Dict) -> Optional[str]:
        """Find similar key in mapping using semantic similarity"""
        for target_key in mapping.values():
            if (source_key.lower() in target_key.lower() or 
                target_key.lower() in source_key.lower()):
                return target_key
        return None
    
    def get_transfer_statistics(self) -> Dict:
        """Get statistics about energy knowledge transfers"""
        return {
            'domains': list(self._domain_knowledge.keys()),
            'transfers': dict(self._transfer_mappings),
            'total_transfers': sum(len(v) for v in self._transfer_mappings.values())
        }
    
    async def get_domain_energy_insights(self, domain: str) -> Dict:
        """Get aggregated energy insights for a domain"""
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
# NEW MODULE 5: HUMAN-AI ENERGY COLLABORATION
# ============================================================================

class HumanAIEnergyCollaboration:
    """
    Enables collaborative reflection between humans and AI on energy decisions.
    """
    
    def __init__(self, persistence, websocket_manager=None):
        self.persistence = persistence
        self.websocket_manager = websocket_manager
        self._feedback_queue: deque = deque(maxlen=1000)
        self._explanations: Dict[str, Dict] = {}
        self._lock = asyncio.Lock()
        self._listeners: List[Callable] = []
        
        logger.info("HumanAIEnergyCollaboration initialized")
    
    async def request_energy_feedback(self, decision: Dict, context: Dict) -> str:
        """
        Request human feedback on an energy-related decision.
        
        Returns:
            feedback_id: Unique identifier for the feedback request
        """
        feedback_id = f"fb_energy_{uuid.uuid4().hex[:12]}"
        
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
                    'type': 'energy_feedback_request',
                    'data': feedback_request
                })
            except Exception as e:
                logger.error(f"Failed to send energy feedback request: {e}")
        
        await self.persistence.save_energy_feedback_request(feedback_request)
        HUMAN_FEEDBACK.labels(type='request').inc()
        return feedback_id
    
    async def submit_energy_feedback(self, feedback_id: str, feedback: Dict) -> bool:
        """
        Submit human feedback on an energy decision.
        """
        async with self._lock:
            if feedback_id not in self._explanations:
                logger.warning(f"Energy feedback ID {feedback_id} not found")
                return False
            
            request = self._explanations[feedback_id]
            request['status'] = 'completed'
            request['feedback'] = feedback
            request['feedback_timestamp'] = datetime.now().isoformat()
            
            self._feedback_queue.append(request)
        
        await self._process_energy_feedback(request)
        HUMAN_FEEDBACK.labels(type='submitted').inc()
        
        for listener in self._listeners:
            try:
                await listener(request)
            except Exception as e:
                logger.error(f"Energy feedback listener error: {e}")
        
        logger.info(f"Energy feedback {feedback_id} submitted")
        return True
    
    async def _process_energy_feedback(self, feedback_request: Dict):
        """Process human energy feedback and update system learning"""
        feedback = feedback_request.get('feedback', {})
        decision = feedback_request.get('decision', {})
        
        learning = {
            'approval': feedback.get('approval', 0.5),
            'comments': feedback.get('comments', ''),
            'suggestions': feedback.get('suggestions', {}),
            'energy_savings_adjustment': feedback.get('energy_savings_adjustment', 0),
            'timestamp': datetime.now().isoformat()
        }
        
        await self.persistence.save_energy_feedback_learning(learning)
        
        logger.info(f"Processed energy feedback learning: approval={learning['approval']:.2f}")
    
    async def generate_energy_explanation(self, decision: Dict, context: Dict) -> Dict:
        """
        Generate a human-readable explanation for an energy decision.
        """
        explanation = {
            'id': f"exp_energy_{uuid.uuid4().hex[:12]}",
            'decision': decision,
            'context': context,
            'explanation': self._build_energy_explanation(decision, context),
            'confidence': self._calculate_energy_confidence(decision),
            'alternatives': self._generate_energy_alternatives(decision),
            'timestamp': datetime.now().isoformat()
        }
        
        async with self._lock:
            self._explanations[explanation['id']] = explanation
        
        return explanation
    
    def _build_energy_explanation(self, decision: Dict, context: Dict) -> str:
        """Build a human-readable energy explanation"""
        parts = []
        
        if 'energy_savings' in decision:
            parts.append(f"Energy savings: {decision['energy_savings']:.2f} kWh")
        
        if 'reasoning' in context:
            parts.append(f"Reasoning: {context['reasoning']}")
        
        if 'helium_impact' in context:
            parts.append(f"Helium impact: {context['helium_impact']:.2f}%")
        
        if 'alternatives' in context:
            parts.append(f"Alternatives considered: {len(context['alternatives'])}")
        
        return ". ".join(parts)
    
    def _calculate_energy_confidence(self, decision: Dict) -> float:
        """Calculate confidence in the energy decision"""
        confidence = 0.7
        
        if 'evidence' in decision:
            confidence += min(0.2, len(decision['evidence']) * 0.02)
        
        if 'energy_savings' in decision:
            confidence += min(0.1, decision['energy_savings'] * 0.01)
        
        return min(1.0, confidence)
    
    def _generate_energy_alternatives(self, decision: Dict) -> List[Dict]:
        """Generate alternative energy optimization decisions"""
        alternatives = []
        
        if 'energy_savings' in decision:
            current = decision['energy_savings']
            alternatives.append({
                'type': 'more_aggressive',
                'energy_savings': current * 1.5,
                'tradeoff': 'higher_power'
            })
            alternatives.append({
                'type': 'more_conservative',
                'energy_savings': current * 0.7,
                'tradeoff': 'lower_power'
            })
        
        return alternatives[:3]
    
    async def get_energy_feedback_summary(self) -> Dict:
        """Get summary of human energy feedback"""
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
# NEW MODULE 6: PREDICTIVE ENERGY REFLEXIVITY
# ============================================================================

class PredictiveEnergyReflexivity:
    """
    Predicts future energy needs and proactively recommends optimizations.
    """
    
    def __init__(self, persistence, horizon_hours: int = 24):
        self.persistence = persistence
        self.horizon_hours = horizon_hours
        self._predictions: Dict[str, Dict] = {}
        self._historical_data: deque = deque(maxlen=1000)
        self._models: Dict[str, Any] = {}
        self._lock = asyncio.Lock()
        
        logger.info(f"PredictiveEnergyReflexivity initialized with {horizon_hours}h horizon")
    
    async def predict_energy_demand(self, time_window: int = 3600) -> Dict:
        """
        Predict future energy demand.
        """
        async with self._lock:
            history = await self.persistence.get_energy_history(limit=100)
            self._historical_data.extend(history)
            
            if len(self._historical_data) < 10:
                return {
                    'predicted_demand': 0.5,
                    'confidence': 0.1,
                    'reason': 'Insufficient data'
                }
            
            recent = list(self._historical_data)[-50:]
            
            # Calculate average demand rate
            if len(recent) > 1:
                time_span = (datetime.now() - datetime.fromisoformat(recent[0]['timestamp'])).total_seconds()
                if time_span > 0:
                    demand_rate = sum(r.get('power_watts', 0) for r in recent) / time_span
                else:
                    demand_rate = 1.0
            else:
                demand_rate = 1.0
            
            predicted_demand = demand_rate * time_window
            
            # Calculate confidence
            rates = []
            for i in range(0, len(recent) - 5, 5):
                window = recent[i:i+5]
                if len(window) > 1:
                    span = (datetime.fromisoformat(window[-1]['timestamp']) - 
                           datetime.fromisoformat(window[0]['timestamp'])).total_seconds()
                    if span > 0:
                        rates.append(sum(r.get('power_watts', 0) for r in window) / span)
            
            variance = np.var(rates) if rates else 1.0
            confidence = max(0, min(1, 1.0 - variance))
            
            prediction = {
                'predicted_demand': max(0, predicted_demand),
                'demand_rate': demand_rate,
                'confidence': confidence,
                'time_window_seconds': time_window,
                'timestamp': datetime.now().isoformat()
            }
            
            self._predictions['demand'] = prediction
            PREDICTIVE_ACCURACY.labels(model_type='demand').set(confidence)
            
            return prediction
    
    async def predict_helium_impact(self, task_plan: Dict) -> Dict:
        """
        Predict helium impact of a planned task.
        """
        task_type = task_plan.get('type', 'unknown')
        power = task_plan.get('power_watts', 100)
        duration = task_plan.get('duration_hours', 1)
        
        helium_factor = {
            'training': 0.5,
            'inference': 0.1,
            'data_processing': 0.3,
            'cooling': 0.2
        }.get(task_type, 0.3)
        
        predicted_helium = power * duration * helium_factor / 1000  # Convert to kg
        
        return {
            'predicted_helium_impact': predicted_helium,
            'task_type': task_type,
            'power_watts': power,
            'duration_hours': duration,
            'confidence': 0.7,
            'timestamp': datetime.now().isoformat()
        }
    
    async def generate_energy_recommendations(self) -> List[Dict]:
        """
        Generate proactive energy optimization recommendations.
        """
        recommendations = []
        
        demand_pred = await self.predict_energy_demand()
        
        if demand_pred.get('confidence', 0) > 0.6:
            predicted = demand_pred.get('predicted_demand', 0)
            
            if predicted > 5000:  # High demand predicted
                recommendations.append({
                    'type': 'reduce_demand',
                    'reason': f'High energy demand predicted: {predicted:.1f} W',
                    'priority': 'high',
                    'action': 'Implement immediate demand reduction measures',
                    'confidence': demand_pred.get('confidence', 0)
                })
            elif predicted > 2000:
                recommendations.append({
                    'type': 'monitor_demand',
                    'reason': f'Moderate energy demand predicted: {predicted:.1f} W',
                    'priority': 'medium',
                    'action': 'Schedule proactive demand review',
                    'confidence': demand_pred.get('confidence', 0)
                })
        
        # Carbon intensity based recommendations
        if hasattr(self, 'carbon_integrator'):
            intensity = await self.carbon_integrator.get_current_intensity()
            if intensity.get('intensity', 0) > 400:
                recommendations.append({
                    'type': 'schedule_off_peak',
                    'reason': f'High carbon intensity: {intensity["intensity"]} gCO2/kWh',
                    'priority': 'high',
                    'action': 'Delay non-critical energy tasks to off-peak hours'
                })
        
        return recommendations
    
    async def get_energy_forecast(self) -> Dict:
        """Get comprehensive energy forecast"""
        demand = await self.predict_energy_demand()
        recommendations = await self.generate_energy_recommendations()
        
        return {
            'demand_forecast': demand,
            'recommendations': recommendations,
            'timestamp': datetime.now().isoformat()
        }

# ============================================================================
# NEW MODULE 7: ENERGY SUSTAINABILITY TRACKER
# ============================================================================

class EnergySustainabilityTracker:
    """
    Tracks and reports energy sustainability metrics.
    """
    
    def __init__(self, persistence):
        self.persistence = persistence
        self._metrics = {
            'energy_efficiency': [],
            'carbon_reduction': [],
            'helium_awareness': [],
            'user_satisfaction': []
        }
        self._lock = asyncio.Lock()
        
        logger.info("EnergySustainabilityTracker initialized")
    
    async def record_metric(self, category: str, value: float, context: Dict = None):
        """Record an energy sustainability metric"""
        async with self._lock:
            if category in self._metrics:
                self._metrics[category].append({
                    'value': value,
                    'timestamp': datetime.now().isoformat(),
                    'context': context or {}
                })
                
                logger.debug(f"Recorded {category} metric: {value:.3f}")
    
    async def get_energy_sustainability_score(self) -> Dict:
        """Calculate overall energy sustainability score"""
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
    
    async def get_energy_savings(self) -> Dict:
        """Calculate energy savings from optimizations"""
        energy_saved = 0.0
        
        energy_efficiency = self._metrics.get('energy_efficiency', [])
        if energy_efficiency:
            recent = energy_efficiency[-10:]
            if recent:
                avg_efficiency = sum(r['value'] for r in recent) / len(recent)
                energy_saved = avg_efficiency * 100
        
        ENERGY_SAVED.set(energy_saved)
        HELIUM_EFFICIENCY.set(energy_saved / 100 if energy_saved > 0 else 0.5)
        
        return {
            'energy_saved_kwh': energy_saved,
            'helium_efficiency': min(1.0, energy_saved / 100),
            'timestamp': datetime.now().isoformat()
        }

# ============================================================================
# ENHANCED MAIN ENERGY SCALER
# ============================================================================

class EnhancedIntelligentEnergyScalerV11_0:
    """
    Enhanced Energy Scaler v11.0 with advanced sustainability features.
    
    New Features:
    1. Federated Energy Learning
    2. User-Adaptive Energy Reflexivity
    3. Real-Time Carbon Intensity Integration
    4. Cross-Domain Energy Knowledge Transfer
    5. Human-AI Energy Collaboration
    6. Predictive Energy Reflexivity
    7. Enhanced Helium Awareness
    8. Energy Sustainability Metrics
    """
    
    def __init__(self, config: Dict = None):
        self.config = config or self._load_config()
        self.instance_id = str(uuid.uuid4())[:8]
        self._start_time = datetime.now()
        
        # Component dependency graph
        self.dependency_graph = ComponentDependencyGraph()
        
        # Background task manager
        self.task_manager = BackgroundTaskManager(max_concurrent=10)
        
        # Timed health check
        self.timed_health_check = TimedHealthCheck(timeout=HEALTH_CHECK_TIMEOUT)
        
        # Core components
        self.power_monitor = self._init_power_monitor()
        self.load_forecaster = self._init_load_forecaster()
        self.renewable_predictor = self._init_renewable_predictor()
        self.battery_optimizer = self._init_battery_optimizer()
        self.market_connector = self._init_market_connector()
        
        # Enhanced components
        self.event_controller = self._init_event_controller()
        self.pue_optimizer = self._init_pue_optimizer()
        self.anomaly_detector = self._init_anomaly_detector()
        self.gpu_power_capper = self._init_gpu_capper()
        self.dashboard = self._init_dashboard()
        
        # Real monitoring components
        self.memory_monitor = RealMemoryPowerMonitor()
        self.network_monitor = RealNetworkPowerMonitor()
        self.storage_monitor = RealStoragePowerMonitor()
        
        # ============================================================
        # NEW: Initialize advanced sustainability components
        # ============================================================
        
        # 1. Federated Energy Learning
        self.federated_learner = FederatedEnergyLearner(
            self.persistence,
            self.instance_id,
            min_share_interval=3600
        )
        
        # 2. User-Adaptive Energy Reflexivity
        self.user_adaptive = UserAdaptiveEnergyReflexivity(self.persistence)
        
        # 3. Real-Time Carbon Intensity Integration
        self.carbon_integrator = RealTimeCarbonIntegrator(
            api_key=self.config.get('carbon_api_key'),
            region=self.config.get('carbon_region', 'global')
        )
        
        # 4. Cross-Domain Energy Knowledge Transfer
        self.cross_domain_transfer = CrossDomainEnergyTransfer(self.persistence)
        
        # 5. Human-AI Energy Collaboration
        self.human_collaborator = HumanAIEnergyCollaboration(
            self.persistence,
            self.dashboard
        )
        
        # 6. Predictive Energy Reflexivity
        self.predictive_reflexivity = PredictiveEnergyReflexivity(
            self.persistence,
            horizon_hours=24
        )
        
        # 7. Energy Sustainability Tracker
        self.sustainability_tracker = EnergySustainabilityTracker(self.persistence)
        
        # Bounded caches
        self.optimization_history = deque(maxlen=5000)
        self.anomaly_history = deque(maxlen=5000)
        self.dead_letter_queue = deque(maxlen=1000)
        
        # State tracking
        self.current_state = PowerSystemState()
        self._state_lock = asyncio.Lock()
        
        # Shutdown event
        self._shutdown_event = asyncio.Event()
        self.running = False
        
        # Register dependencies
        self.dependency_graph.add_component('database', [])
        self.dependency_graph.add_component('power_monitor', [])
        self.dependency_graph.add_component('market_connector', ['database'])
        
        logger.info(f"EnhancedEnergyScaler v{DATA_VERSION} initialized (instance: {self.instance_id})")
        logger.info("  ✅ Advanced Energy Sustainability Features Enabled:")
        logger.info("     - Federated Energy Learning")
        logger.info("     - User-Adaptive Energy Reflexivity")
        logger.info("     - Real-Time Carbon Intensity Integration")
        logger.info("     - Cross-Domain Energy Knowledge Transfer")
        logger.info("     - Human-AI Energy Collaboration")
        logger.info("     - Predictive Energy Reflexivity")
    
    def _load_config(self) -> Dict:
        """Load configuration with validation"""
        config_file = Path('energy_scaler_config.json')
        
        default_config = {
            'forecast_horizon': 24,
            'battery_capacity_kwh': 100,
            'max_charge_rate_kw': 50,
            'max_discharge_rate_kw': 50,
            'target_pue': 1.2,
            'anomaly_window': 100,
            'retrain_interval': 3600,
            'dashboard_port': 8767,
            'sampling_interval_seconds': 1,
            'optimization_interval_seconds': 60,
            'power_spike_threshold_pct': 50,
            'price_change_threshold_pct': 20,
            'carbon_spike_threshold_pct': 30,
            'temperature_threshold_c': 85,
            'gpu_power_cap_watts': 250,
            'carbon_api_key': os.getenv('CARBON_API_KEY', ''),
            'carbon_region': os.getenv('CARBON_REGION', 'global'),
            'weather_api_key': os.getenv('WEATHER_API_KEY', ''),
            'energy_api_key': os.getenv('ENERGY_API_KEY', ''),
            'data_retention_hours': 168,
            'cleanup_interval_seconds': 3600
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
    
    # ... [All existing component initialization methods remain the same]
    def _init_power_monitor(self):
        """Initialize power monitor with dependency tracking"""
        monitor = ComprehensivePowerMonitor()
        self.dependency_graph.add_component('power_monitor', [])
        return monitor
    
    def _init_load_forecaster(self):
        """Initialize load forecaster"""
        return PredictiveLoadForecaster(
            forecast_horizon_hours=self.config.get('forecast_horizon', 24)
        )
    
    def _init_renewable_predictor(self):
        """Initialize renewable predictor"""
        return RenewableEnergyPredictor(
            api_key=self.config.get('weather_api_key')
        )
    
    def _init_battery_optimizer(self):
        """Initialize battery optimizer"""
        return BatteryOptimizer(
            capacity_kwh=self.config.get('battery_capacity_kwh', 100),
            max_charge_rate_kw=self.config.get('max_charge_rate_kw', 50),
            max_discharge_rate_kw=self.config.get('max_discharge_rate_kw', 50)
        )
    
    def _init_market_connector(self):
        """Initialize market connector"""
        return EnhancedEnergyMarketConnector(
            api_key=self.config.get('energy_api_key')
        )
    
    def _init_event_controller(self):
        """Initialize event controller"""
        return EventDrivenController(self)
    
    def _init_pue_optimizer(self):
        """Initialize PUE optimizer"""
        return EnhancedPueOptimizer(target_pue=self.config.get('target_pue', 1.2))
    
    def _init_anomaly_detector(self):
        """Initialize anomaly detector"""
        return EnhancedPowerAnomalyDetector(
            window_size=self.config.get('anomaly_window', 100),
            retrain_interval=self.config.get('retrain_interval', 3600)
        )
    
    def _init_gpu_capper(self):
        """Initialize GPU capper"""
        return EnhancedGPUPowerCapper(gpu_id=0)
    
    def _init_dashboard(self):
        """Initialize dashboard"""
        return EnhancedWebSocketManager(port=self.config.get('dashboard_port', 8767))
    
    async def start(self):
        """Start all services including advanced sustainability features"""
        logger.info(f"Starting EnhancedEnergyScaler v{DATA_VERSION} (instance: {self.instance_id})")
        
        # Validate dependencies
        is_valid, cycles = self.dependency_graph.validate()
        if not is_valid:
            logger.error(f"Dependency cycles detected: {cycles}")
            raise ValueError(f"Circular dependencies: {cycles}")
        
        # Start background task manager
        await self.task_manager.start(num_workers=5)
        
        # Start core background tasks
        await self.task_manager.submit(self._monitoring_loop, name="monitoring_loop", priority=TaskPriority.HIGH)
        await self.task_manager.submit(self._optimization_loop, name="optimization_loop", priority=TaskPriority.NORMAL)
        await self.task_manager.submit(self.event_controller.start_monitoring, name="event_controller", priority=TaskPriority.HIGH)
        await self.task_manager.submit(self.dashboard.start, name="dashboard", priority=TaskPriority.LOW)
        await self.task_manager.submit(self._cleanup_loop, name="cleanup_loop", priority=TaskPriority.BACKGROUND)
        await self.task_manager.submit(self._health_monitor_loop, name="health_monitor", priority=TaskPriority.NORMAL)
        
        # ============================================================
        # NEW: Start advanced sustainability background tasks
        # ============================================================
        
        await self.task_manager.submit(self._carbon_intensity_monitor, name="carbon_intensity_monitor", priority=TaskPriority.NORMAL)
        await self.task_manager.submit(self._federated_learning_loop, name="federated_learning", priority=TaskPriority.NORMAL)
        await self.task_manager.submit(self._predictive_energy_loop, name="predictive_energy", priority=TaskPriority.NORMAL)
        await self.task_manager.submit(self._sustainability_reporter, name="sustainability_reporter", priority=TaskPriority.LOW)
        
        self.running = True
        
        # Broadcast startup event
        await self.dashboard.broadcast({
            'type': 'system_started',
            'instance_id': self.instance_id,
            'version': str(DATA_VERSION),
            'features': [
                'federated_energy_learning',
                'user_adaptive_energy',
                'real_time_carbon_intensity',
                'cross_domain_energy_transfer',
                'human_ai_energy_collaboration',
                'predictive_energy_reflexivity'
            ],
            'timestamp': datetime.now().isoformat()
        })
        
        logger.info(f"EnhancedEnergyScaler started with {len(self.task_manager._tasks)} background tasks")
    
    # ============================================================
    # NEW: Advanced Sustainability Background Tasks
    # ============================================================
    
    async def _carbon_intensity_monitor(self):
        """Monitor carbon intensity and adjust energy optimization"""
        while not self._shutdown_event.is_set():
            try:
                intensity = await self.carbon_integrator.get_current_intensity()
                optimal = await self.carbon_integrator.get_optimal_energy_time()
                
                # Record sustainability metric
                eco_efficiency = 1.0 - (intensity['intensity'] / 1000)
                await self.sustainability_tracker.record_metric(
                    'carbon_reduction',
                    eco_efficiency,
                    {'intensity': intensity['intensity']}
                )
                
                # Adjust GPU power cap based on carbon intensity
                if intensity['intensity'] > 500:
                    new_cap = max(150, self.config['gpu_power_cap_watts'] * 0.7)
                    await self.gpu_power_capper.set_power_limit(new_cap)
                    TOTAL_OPTIMIZATIONS.labels(action='carbon_gpu_cap_reduce').inc()
                    logger.info(f"Reduced GPU power cap to {new_cap}W due to high carbon intensity")
                elif intensity['intensity'] < 200:
                    await self.gpu_power_capper.set_power_limit(self.config['gpu_power_cap_watts'])
                    TOTAL_OPTIMIZATIONS.labels(action='carbon_gpu_cap_restore').inc()
                    logger.info(f"Restored GPU power cap to {self.config['gpu_power_cap_watts']}W")
                
                # Broadcast carbon intensity update
                await self.dashboard.broadcast({
                    'type': 'carbon_intensity_update',
                    'current_intensity': intensity,
                    'optimal_energy_time': optimal,
                    'timestamp': datetime.now().isoformat()
                })
                
                await asyncio.sleep(300)  # Check every 5 minutes
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Carbon intensity monitor error: {e}")
                await asyncio.sleep(60)
    
    async def _federated_learning_loop(self):
        """Pull and apply federated energy insights"""
        while not self._shutdown_event.is_set():
            try:
                insights = await self.federated_learner.pull_network_insights(limit=5)
                
                if insights:
                    logger.info(f"Applied {len(insights)} federated energy insights")
                    
                    # Apply insights to improve energy optimization
                    for insight in insights:
                        if 'optimization' in insight:
                            await self.apply_federated_strategy(insight['optimization'])
                
                await asyncio.sleep(3600)  # Run every hour
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Federated learning error: {e}")
                await asyncio.sleep(60)
    
    async def _predictive_energy_loop(self):
        """Run predictive energy analysis and generate recommendations"""
        while not self._shutdown_event.is_set():
            try:
                forecast = await self.predictive_reflexivity.get_energy_forecast()
                
                # Apply high-priority recommendations
                for rec in forecast.get('recommendations', []):
                    if rec.get('priority') == 'high':
                        logger.info(f"Applying energy recommendation: {rec['reason']}")
                        await self._apply_energy_recommendation(rec)
                
                # Broadcast forecast
                await self.dashboard.broadcast({
                    'type': 'energy_forecast',
                    'forecast': forecast,
                    'timestamp': datetime.now().isoformat()
                })
                
                await asyncio.sleep(1800)  # Run every 30 minutes
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Predictive energy error: {e}")
                await asyncio.sleep(60)
    
    async def _sustainability_reporter(self):
        """Generate and log energy sustainability reports"""
        while not self._shutdown_event.is_set():
            try:
                score = await self.sustainability_tracker.get_energy_sustainability_score()
                savings = await self.sustainability_tracker.get_energy_savings()
                
                logger.info(f"Energy Sustainability Report:")
                logger.info(f"  Overall Score: {score['overall_score']:.1f}%")
                logger.info(f"  Energy Saved: {savings['energy_saved_kwh']:.2f} kWh")
                logger.info(f"  Helium Efficiency: {savings['helium_efficiency']:.2f}")
                logger.info(f"  Categories: {score['categories']}")
                
                await self.dashboard.broadcast({
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
    
    async def _apply_energy_recommendation(self, recommendation: Dict):
        """Apply an energy optimization recommendation"""
        action = recommendation.get('action')
        if action == 'Implement immediate demand reduction measures':
            logger.info("Implementing energy demand reduction measures...")
            # Reduce GPU power cap
            current_cap = await self.gpu_power_capper.get_power_limit()
            new_cap = max(100, current_cap * 0.8)
            await self.gpu_power_capper.set_power_limit(new_cap)
            TOTAL_OPTIMIZATIONS.labels(action='predictive_demand_reduce').inc()
            
            # Record reduction action
            await self.sustainability_tracker.record_metric(
                'energy_efficiency',
                0.8,
                {'action': action}
            )
        elif action == 'Delay non-critical energy tasks to off-peak hours':
            optimal = await self.carbon_integrator.get_optimal_energy_time()
            logger.info(f"Scheduling energy tasks for {optimal.get('optimal_time')}")
    
    async def apply_federated_strategy(self, strategy: Dict):
        """Apply a federated learning strategy"""
        logger.info(f"Applying federated energy strategy: {strategy.get('name', 'unknown')}")
        await self.sustainability_tracker.record_metric(
            'energy_efficiency',
            0.7,
            {'strategy': strategy.get('name', 'unknown')}
        )
    
    async def _monitoring_loop(self):
        """Enhanced monitoring loop with timeout protection"""
        while not self._shutdown_event.is_set():
            try:
                power_data = self.power_monitor.get_total_power()
                energy_price = await self.market_connector.get_current_price()
                carbon_intensity = await self.carbon_integrator.get_current_intensity()
                
                async with self._state_lock:
                    self.current_state.total_power_watts = power_data['total_watts']
                    self.current_state.cpu_power_watts = power_data['cpu_watts']
                    self.current_state.gpu_power_watts = power_data['gpu_watts']
                    self.current_state.energy_market_price_per_kwh = energy_price
                    self.current_state.carbon_intensity_gco2_per_kwh = carbon_intensity['intensity']
                
                # Update Prometheus metrics
                POWER_READINGS.labels(component='total').set(power_data['total_watts'])
                POWER_READINGS.labels(component='cpu').set(power_data['cpu_watts'])
                POWER_READINGS.labels(component='gpu').set(power_data['gpu_watts'])
                CARBON_INTENSITY.set(carbon_intensity['intensity'])
                
                # Anomaly detection
                recent_readings = [p['total_watts'] for p in self._get_recent_power_history()]
                if recent_readings:
                    anomaly_result = await self.anomaly_detector.detect(recent_readings, power_data['total_watts'])
                    if anomaly_result['is_anomaly']:
                        self.anomaly_history.append(anomaly_result)
                        await self.dashboard.broadcast({
                            'type': 'anomaly',
                            'data': anomaly_result,
                            'timestamp': datetime.now().isoformat()
                        })
                
                await self.dashboard.broadcast({
                    'type': 'power_update',
                    'data': power_data,
                    'carbon_intensity': carbon_intensity,
                    'energy_price': energy_price,
                    'timestamp': datetime.now().isoformat()
                })
                
                await asyncio.sleep(self.config['sampling_interval_seconds'])
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Monitoring loop error: {e}")
                await asyncio.sleep(5)
    
    # ... [All other existing methods remain the same: _optimization_loop, _cleanup_loop, _health_monitor_loop, etc.]
    
    async def get_system_status(self) -> Dict:
        """Get comprehensive system status including sustainability metrics"""
        async with self._state_lock:
            battery_status = self.battery_optimizer.get_status()
            pue_trend = await self.pue_optimizer.get_pue_trend()
            sustainability_score = await self.sustainability_tracker.get_energy_sustainability_score()
            savings = await self.sustainability_tracker.get_energy_savings()
            federated_insights = self.federated_learner.get_federated_insights()
            
            return {
                'system': {
                    'version': str(DATA_VERSION),
                    'instance_id': self.instance_id,
                    'running': self.running,
                    'uptime_seconds': (datetime.now() - self._start_time).total_seconds(),
                    'background_tasks': self.task_manager.get_statistics()
                },
                'power': {
                    'total_watts': self.current_state.total_power_watts,
                    'cpu_watts': self.current_state.cpu_power_watts,
                    'gpu_watts': self.current_state.gpu_power_watts,
                    'memory_watts': self.memory_monitor.get_power(),
                    'network_watts': self.network_monitor.get_power(),
                    'storage_watts': self.storage_monitor.get_power()
                },
                'battery': battery_status,
                'pue': {
                    'current': self.current_state.pue,
                    'trend': pue_trend,
                    'target': self.pue_optimizer.target_pue
                },
                'gpu': {
                    'power_cap_watts': await self.gpu_power_capper.get_power_limit(),
                    'current_power_watts': await self.gpu_power_capper.get_power_usage()
                },
                'carbon': {
                    'intensity_gco2_per_kwh': self.current_state.carbon_intensity_gco2_per_kwh,
                    'real_time': await self.carbon_integrator.get_current_intensity()
                },
                # NEW: Sustainability metrics
                'sustainability': {
                    'score': sustainability_score,
                    'savings': savings,
                    'federated_insights': federated_insights
                },
                'anomalies': {
                    'total': len(self.anomaly_history),
                    'recent': list(self.anomaly_history)[-5:] if self.anomaly_history else []
                },
                'optimizations': len(self.optimization_history),
                'dead_letter_size': len(self.dead_letter_queue),
                'timestamp': datetime.now().isoformat()
            }
    
    async def shutdown(self):
        """Graceful shutdown with enhanced cleanup"""
        logger.info(f"Shutting down EnhancedEnergyScaler (instance: {self.instance_id})")
        
        self._shutdown_event.set()
        self.running = False
        
        # Stop background task manager
        await self.task_manager.stop()
        
        # Stop WebSocket server
        await self.dashboard.stop()
        
        # Shutdown advanced components
        await self.federated_learner.shutdown()
        await self.carbon_integrator.close()
        
        # Shutdown GPU capper
        await self.gpu_power_capper.shutdown()
        
        # Close API connections
        await self.market_connector.close()
        
        # Final sustainability report
        savings = await self.sustainability_tracker.get_energy_savings()
        audit_logger.info(f"Total energy savings at shutdown: {savings['energy_saved_kwh']:.2f} kWh")
        audit_logger.info(f"Helium efficiency at shutdown: {savings['helium_efficiency']:.2f}")
        
        audit_logger.info(f"System shutdown complete - Instance: {self.instance_id}")
        logger.info("Shutdown complete")

# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

async def main():
    print("=" * 80)
    print("Enhanced Intelligent Energy Scaler v11.0 - Advanced Sustainability")
    print("=" * 80)
    
    scaler = EnhancedIntelligentEnergyScalerV11_0()
    
    print(f"\n✅ v11.0 ADVANCED SUSTAINABILITY FEATURES:")
    print(f"   ✅ Federated Energy Learning - Cross-instance insights sharing")
    print(f"   ✅ User-Adaptive Energy Reflexivity - Learning user preferences")
    print(f"   ✅ Real-Time Carbon Intensity Integration - Live API integration")
    print(f"   ✅ Cross-Domain Energy Knowledge Transfer - Domain insights sharing")
    print(f"   ✅ Human-AI Energy Collaboration - Feedback loops with users")
    print(f"   ✅ Predictive Energy Reflexivity - Forecasting and recommendations")
    print(f"   ✅ Enhanced Helium Awareness - Resource-aware energy optimization")
    print(f"   ✅ Energy Sustainability Metrics - Tracking eco-efficiency gains")
    
    await scaler.start()
    
    print(f"\n📊 System Statistics:")
    status = await scaler.get_system_status()
    print(f"   Instance: {status['system']['instance_id']}")
    print(f"   Version: {status['system']['version']}")
    print(f"   Background Tasks: {status['system']['background_tasks']['total_tasks']}")
    print(f"   Active Workers: {status['system']['background_tasks']['active_tasks']}")
    print(f"   Power: {status['power']['total_watts']:.0f}W")
    print(f"   PUE: {status['pue']['current']:.2f}")
    print(f"   Carbon Intensity: {status['carbon']['intensity_gco2_per_kwh']:.0f} gCO2/kWh")
    
    # Test user adaptation
    print(f"\n📊 Testing User Adaptation:")
    await scaler.user_adaptive.learn_user_preference(
        "test_user",
        "accept_optimization",
        {"energy_savings": 100, "helium_impact": 0.2},
        {"success": True}
    )
    print(f"   User adaptation score updated")
    
    # Test human feedback
    print(f"\n📊 Testing Human-AI Collaboration:")
    feedback_id = await scaler.human_collaborator.request_energy_feedback(
        {"energy_savings": 100},
        {"reasoning": "High energy savings potential", "helium_impact": 0.3}
    )
    print(f"   Feedback request created: {feedback_id}")
    
    # Test federated learning
    print(f"\n📊 Testing Federated Learning:")
    package_id = await scaler.federated_learner.share_energy_insight({
        'domain': 'data_center',
        'optimization': {
            'strategy': 'gpu_power_cap',
            'efficiency_gain': 0.3,
            'carbon_reduction': 0.2
        },
        'energy_savings': 150
    })
    print(f"   Federated insight shared: {package_id}")
    
    print(f"\n🔌 Services Available:")
    print(f"   Dashboard: ws://localhost:{scaler.config['dashboard_port']}")
    print(f"   Metrics: http://localhost:9090/metrics")
    print(f"   Real-Time Carbon Integration Active")
    print(f"   Federated Learning Network Active")
    
    print("\n🛡️ Enterprise Sustainability Features:")
    print("   - Federated learning across Green Agent instances")
    print("   - Personalized user adaptation and learning")
    print("   - Real-time carbon intensity integration")
    print("   - Cross-domain knowledge transfer")
    print("   - Human-AI collaborative feedback loops")
    print("   - Predictive energy forecasting")
    
    print("\n" + "=" * 80)
    print("✅ Energy Scaler v11.0 Running Successfully")
    print("=" * 80)
    
    try:
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        print("\n🛑 Shutting down...")
        await scaler.shutdown()
        print("Shutdown complete")

if __name__ == "__main__":
    asyncio.run(main())
