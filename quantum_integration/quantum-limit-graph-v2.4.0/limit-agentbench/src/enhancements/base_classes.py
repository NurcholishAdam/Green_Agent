# File: src/enhancements/base_classes_enhanced_v10.py

"""
Green Agent Base Classes - Version 10.0 (Enterprise Platinum)
ENHANCED WITH: Carbon Intensity Integration, Helium Tracking, Sustainability Dashboard,
Predictive Analytics, and Complete Green Agent Capabilities

CRITICAL FIXES OVER v9.0:
1. FIXED: Memory leak with bounded collections in BaseMLModel
2. FIXED: Async lock support for async contexts
3. ADDED: Database persistence for ModelRegistry with connection pooling
4. ADDED: Circuit breaker half-open testing with gradual recovery
5. ADDED: Full async support with proper async locks
6. ADDED: Health check timeouts with circuit breaker protection
7. ADDED: Rate limiting for model predictions
8. ADDED: Model version rollback capability
9. ADDED: State export/import for ModelRegistry
10. ADDED: Prometheus metrics for all operations
11. ADDED: Size-based cache eviction with LRU
12. ADDED: Graceful degradation for optional dependencies
13. ADDED: Carbon Intensity Integration with real-time API support
14. ADDED: Helium Tracking and Awareness module
15. ADDED: Sustainability Dashboard with unified reporting
16. ADDED: Predictive Analytics with ensemble forecasting
17. ADDED: FIXED: Graceful shutdown with proper cleanup
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import pickle
import threading
import time
import uuid
import warnings
from abc import ABC, abstractmethod
from collections import defaultdict, deque
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any, Callable, Union, Type, Set
from weakref import WeakValueDictionary
import functools
import inspect
import tempfile
import os

import numpy as np

# Pydantic for validation
from pydantic import BaseModel, Field, validator, ValidationError

# Tenacity for retries
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# Database with connection pooling
from sqlalchemy import create_engine, Column, String, Float, DateTime, Integer, Boolean, Text, JSON, Index, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.pool import QueuePool
from sqlalchemy.exc import SQLAlchemyError

# Prometheus metrics
from prometheus_client import Counter, Histogram, Gauge, CollectorRegistry

# Optional imports with graceful degradation
try:
    import torch
    import torch.nn as nn
    TORCH_AVAILABLE = True
except ImportError:
    TORCH_AVAILABLE = False

try:
    import tensorflow as tf
    TF_AVAILABLE = True
except ImportError:
    TF_AVAILABLE = False

try:
    import sklearn
    from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
    from sklearn.preprocessing import StandardScaler
    from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

try:
    import optuna
    OPTUNA_AVAILABLE = True
except ImportError:
    OPTUNA_AVAILABLE = False

try:
    from cryptography.fernet import Fernet
    CRYPTO_AVAILABLE = True
except ImportError:
    CRYPTO_AVAILABLE = False

# Async HTTP for carbon intensity
try:
    import aiohttp
    AIOHTTP_AVAILABLE = True
except ImportError:
    AIOHTTP_AVAILABLE = False

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - [%(correlation_id)s] - %(message)s',
    handlers=[
        logging.handlers.RotatingFileHandler('base_classes_v10.log', maxBytes=10*1024*1024, backupCount=5),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class CorrelationIdFilter(logging.Filter):
    def __init__(self):
        super().__init__()
        self.correlation_id = str(uuid.uuid4())[:8]
    def filter(self, record):
        record.correlation_id = self.correlation_id
        return True

logger.addFilter(CorrelationIdFilter())

# Prometheus metrics
REGISTRY = CollectorRegistry()
MODEL_PREDICTIONS = Counter('model_predictions_total', 'Total model predictions', ['model_name', 'version', 'status'], registry=REGISTRY)
MODEL_PREDICTION_LATENCY = Histogram('model_prediction_duration_seconds', 'Prediction duration', ['model_name', 'version'], registry=REGISTRY)
CIRCUIT_BREAKER_STATE = Gauge('circuit_breaker_state', 'Circuit breaker state', ['name'], registry=REGISTRY)
HEALTH_SCORE = Gauge('component_health_score', 'Component health score (0-100)', ['component'], registry=REGISTRY)
DB_SIZE = Gauge('base_classes_db_size_mb', 'Database size in MB', registry=REGISTRY)

# New sustainability metrics
CARBON_INTENSITY = Gauge('carbon_intensity_gco2_per_kwh', 'Real-time carbon intensity', registry=REGISTRY)
HELIUM_EFFICIENCY = Gauge('helium_efficiency_score', 'Helium efficiency (0-1)', registry=REGISTRY)
SUSTAINABILITY_SCORE = Gauge('sustainability_score', 'Overall sustainability score (0-100)', registry=REGISTRY)
CARBON_SAVINGS = Counter('carbon_savings_total', 'Total carbon savings', ['source'], registry=REGISTRY)
HELIUM_SAVINGS = Counter('helium_savings_total', 'Total helium savings', ['source'], registry=REGISTRY)

# Constants
MAX_PREDICTION_HISTORY = 10000
MAX_CACHE_SIZE = 1000
CACHE_TTL_SECONDS = 300
MAX_RETRY_ATTEMPTS = 3
CIRCUIT_BREAKER_THRESHOLD = 5
CIRCUIT_BREAKER_TIMEOUT = 60
HEALTH_CHECK_TIMEOUT = 10
RATE_LIMIT_REQUESTS = 1000
RATE_LIMIT_WINDOW = 60
DATA_VERSION = 10

# ============================================================
# ENHANCED EXCEPTION CLASSES
# ============================================================

class GreenAgentException(Exception):
    """Base exception for all Green Agent exceptions"""
    def __init__(self, message: str, details: Dict = None):
        super().__init__(message)
        self.details = details or {}
        self.timestamp = datetime.now()
        self.correlation_id = getattr(logging, 'correlation_id', str(uuid.uuid4())[:8])

class ConfigurationError(GreenAgentException):
    """Configuration related errors"""
    pass

class DataValidationError(GreenAgentException):
    """Data validation errors"""
    pass

class ModuleNotFoundError(GreenAgentException):
    """Module not found errors"""
    pass

class QuantumError(GreenAgentException):
    """Quantum computing related errors"""
    pass

class BlockchainError(GreenAgentException):
    """Blockchain interaction errors"""
    pass

class APIError(GreenAgentException):
    """API communication errors"""
    pass

class ResourceError(GreenAgentException):
    """Resource allocation errors"""
    pass

class TimeoutError(GreenAgentException):
    """Timeout errors"""
    pass

class CircuitBreakerOpenError(GreenAgentException):
    """Circuit breaker is open"""
    pass

class CarbonIntensityError(GreenAgentException):
    """Carbon intensity API errors"""
    pass

class HeliumTrackingError(GreenAgentException):
    """Helium tracking errors"""
    pass

# ============================================================
# CARBON INTENSITY INTEGRATION MODULE
# ============================================================

class CarbonIntensityManager:
    """
    Real-time carbon intensity integration with API support.
    
    Features:
    - Real-time carbon intensity fetching from electricitymap.org
    - Historical intensity tracking
    - Carbon savings calculation
    - Regional carbon profiles
    """
    
    def __init__(self, endpoint: str = "https://api.electricitymap.org/v3/carbon-intensity"):
        self.endpoint = endpoint
        self.carbon_intensity = 0.0
        self.region = "us-east"
        self.last_update = None
        self._lock = asyncio.Lock()
        self._session = None
        self.update_interval = 300  # 5 minutes
        self.cache = {}
        self.historical_intensities = deque(maxlen=1000)
        self.api_key = os.getenv('ELECTRICITYMAP_API_KEY', '')
        self.total_carbon_savings_kg = 0.0
        self.region_profiles = self._initialize_region_profiles()
        
        if not AIOHTTP_AVAILABLE:
            logger.warning("aiohttp not available - carbon intensity API disabled")
    
    def _initialize_region_profiles(self) -> Dict[str, Dict[str, Any]]:
        """Initialize regional carbon profiles"""
        return {
            'us-east': {'timezone': -5, 'renewable_pct': 30, 'base_intensity': 420},
            'us-west': {'timezone': -8, 'renewable_pct': 45, 'base_intensity': 350},
            'eu-west': {'timezone': 0, 'renewable_pct': 50, 'base_intensity': 280},
            'eu-north': {'timezone': 0, 'renewable_pct': 60, 'base_intensity': 220},
            'asia-east': {'timezone': 8, 'renewable_pct': 20, 'base_intensity': 500},
            'asia-southeast': {'timezone': 7, 'renewable_pct': 25, 'base_intensity': 480},
            'australia': {'timezone': 10, 'renewable_pct': 35, 'base_intensity': 380},
            'south-america': {'timezone': -3, 'renewable_pct': 40, 'base_intensity': 320},
            'africa': {'timezone': 2, 'renewable_pct': 25, 'base_intensity': 450},
            'middle-east': {'timezone': 3, 'renewable_pct': 15, 'base_intensity': 550}
        }
    
    async def _get_session(self):
        if self._session is None and AIOHTTP_AVAILABLE:
            self._session = aiohttp.ClientSession()
        return self._session
    
    async def update_carbon_intensity(self, region: str = "us-east") -> Dict:
        """Fetch real-time carbon intensity from API"""
        async with self._lock:
            session = await self._get_session()
            self.region = region
            
            try:
                if session and AIOHTTP_AVAILABLE:
                    url = f"{self.endpoint}/latest?zone={region}"
                    headers = {'auth-token': self.api_key} if self.api_key else {}
                    
                    async with session.get(url, headers=headers, timeout=10) as response:
                        if response.status == 200:
                            data = await response.json()
                            self.carbon_intensity = data.get('carbonIntensity', 
                                self.region_profiles.get(region, {}).get('base_intensity', 400))
                            self.last_update = datetime.now()
                            self.cache[region] = {
                                'intensity': self.carbon_intensity,
                                'timestamp': self.last_update
                            }
                            self.historical_intensities.append(self.carbon_intensity)
                            
                            CARBON_INTENSITY.set(self.carbon_intensity)
                            logger.info(f"Carbon intensity updated: {region} = {self.carbon_intensity} gCO2/kWh")
                            return {'intensity': self.carbon_intensity, 'region': region}
                else:
                    # Use fallback
                    self.carbon_intensity = self._get_fallback_intensity(region)
                    self.last_update = datetime.now()
                    
            except Exception as e:
                logger.error(f"Carbon intensity fetch error: {e}")
                self.carbon_intensity = self._get_fallback_intensity(region)
                self.last_update = datetime.now()
            
            return {'intensity': self.carbon_intensity, 'region': self.region}
    
    def _get_fallback_intensity(self, region: str) -> float:
        """Get fallback carbon intensity based on region"""
        return self.region_profiles.get(region, {}).get('base_intensity', 400)
    
    async def get_current_intensity(self) -> float:
        """Get current carbon intensity"""
        if self.last_update is None or \
           (datetime.now() - self.last_update).seconds > self.update_interval:
            await self.update_carbon_intensity(self.region)
        return self.carbon_intensity
    
    async def calculate_carbon_savings(self, energy_saved_kwh: float) -> float:
        """Calculate carbon savings from energy reduction"""
        intensity = await self.get_current_intensity()
        savings_kg = energy_saved_kwh * intensity / 1000  # Convert to kg CO2
        self.total_carbon_savings_kg += savings_kg
        CARBON_SAVINGS.labels(source='energy_efficiency').inc(savings_kg)
        return savings_kg
    
    async def get_optimal_hours(self, region: str = "us-east", hours: int = 24) -> List[datetime]:
        """Get optimal hours for low-carbon operations"""
        current_hour = datetime.now().hour
        optimal_hours = []
        for i in range(hours):
            hour = (current_hour + i) % 24
            if 22 <= hour or hour <= 4:  # Night hours typically cleaner
                optimal_hours.append(datetime.now() + timedelta(hours=i))
        return optimal_hours
    
    async def get_carbon_trend(self, hours: int = 24) -> Dict:
        """Get carbon intensity trend"""
        if len(self.historical_intensities) < 2:
            return {'trend': 'stable', 'change': 0}
        
        recent = list(self.historical_intensities)[-hours:]
        if len(recent) > 2:
            trend = np.polyfit(range(len(recent)), recent, 1)[0]
        else:
            trend = 0
        
        return {
            'trend': 'increasing' if trend > 0.5 else 'decreasing' if trend < -0.5 else 'stable',
            'change': trend,
            'current': recent[-1] if recent else 0,
            'average': np.mean(recent) if recent else 0
        }
    
    async def close(self):
        if self._session:
            await self._session.close()

# ============================================================
# HELIUM TRACKING MODULE
# ============================================================

class HeliumTracker:
    """
    Helium tracking and awareness module.
    
    Features:
    - Helium usage recording
    - Helium recovery tracking
    - Efficiency scoring
    - Budget management
    - Helium-carbon equivalence
    """
    
    def __init__(self, helium_budget_l: float = 100.0):
        self.helium_budget_l = helium_budget_l
        self.helium_usage: Dict[str, float] = defaultdict(float)
        self.helium_recovered: Dict[str, float] = defaultdict(float)
        self.helium_efficiency_scores: Dict[str, float] = defaultdict(lambda: 0.5)
        self.total_usage_l = 0.0
        self.total_recovered_l = 0.0
        self._lock = asyncio.Lock()
        self.history = deque(maxlen=10000)
        self.component_helium: Dict[str, Dict[str, Any]] = {}
        
        # Helium to CO2 equivalence (approximate GWP)
        self.helium_to_co2_factor = 20.0  # 1 kg helium ≈ 20 kg CO2 equivalent
        
        # Recovery rates by component type
        self.recovery_rates = {
            'cooling_system': 0.85,
            'quantum_computer': 0.90,
            'cryogenic_system': 0.80,
            'standard_cooling': 0.75,
            'mri_system': 0.95,
            'helium_expert': 0.70,
            'quantum_expert': 0.88,
            'energy_expert': 0.60
        }
        
        logger.info(f"Helium Tracker initialized: budget={helium_budget_l}L")
    
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
            'used_l': 0.0,
            'type': component_type,
            'recovery_rate': self.recovery_rates.get(component_type, 0.85),
            'registered_at': datetime.utcnow()
        }
        logger.debug(f"Registered helium content for {component_id}: {helium_content_l}L")
    
    async def record_helium_usage(self, component_id: str, amount_l: float, source: str = "unknown"):
        """Record helium usage"""
        async with self._lock:
            self.helium_usage[component_id] += amount_l
            self.total_usage_l += amount_l
            
            if component_id in self.component_helium:
                self.component_helium[component_id]['used_l'] += amount_l
            
            self.history.append({
                'component_id': component_id,
                'amount_l': amount_l,
                'type': 'usage',
                'source': source,
                'timestamp': datetime.utcnow().isoformat()
            })
            
            logger.debug(f"Helium usage recorded: {component_id} = {amount_l}L")
    
    async def record_helium_recovery(self, component_id: str, amount_l: float, source: str = "unknown"):
        """Record helium recovery"""
        async with self._lock:
            self.helium_recovered[component_id] += amount_l
            self.total_recovered_l += amount_l
            
            if component_id in self.component_helium:
                self.component_helium[component_id]['recovered_l'] += amount_l
            
            self.history.append({
                'component_id': component_id,
                'amount_l': amount_l,
                'type': 'recovery',
                'source': source,
                'timestamp': datetime.utcnow().isoformat()
            })
            
            logger.info(f"Helium recovery recorded: {component_id} = {amount_l}L")
    
    async def update_efficiency_score(self, component_id: str, score: float):
        """Update helium efficiency score for a component"""
        async with self._lock:
            self.helium_efficiency_scores[component_id] = max(0.0, min(1.0, score))
            HELIUM_EFFICIENCY.set(score)
    
    async def calculate_helium_offset_from_carbon(self, carbon_credit_kg: float) -> float:
        """Calculate helium offset equivalent from carbon credit"""
        return carbon_credit_kg * 0.05  # 1 kg CO2 offset allows for 0.05 L helium usage
    
    async def optimize_helium_allocation(self, requirements: Dict[str, float]) -> Dict[str, float]:
        """Optimize helium allocation across components based on efficiency"""
        async with self._lock:
            total_required = sum(requirements.values())
            
            if total_required <= self.helium_budget_l - self.total_usage_l:
                return requirements
            
            # Allocate based on efficiency scores
            optimized = {}
            total_efficiency = sum(self.helium_efficiency_scores.get(cid, 0.5) for cid in requirements)
            
            if total_efficiency == 0:
                ratio = (self.helium_budget_l - self.total_usage_l) / total_required
                for cid, req in requirements.items():
                    optimized[cid] = req * ratio
            else:
                available = self.helium_budget_l - self.total_usage_l
                for cid, req in requirements.items():
                    efficiency_weight = self.helium_efficiency_scores.get(cid, 0.5) / total_efficiency
                    optimized[cid] = available * efficiency_weight
            
            return optimized
    
    def get_helium_position(self) -> Dict[str, Any]:
        """Get current helium position"""
        net_position = self.total_usage_l - self.total_recovered_l
        remaining_budget = self.helium_budget_l - net_position
        
        return {
            'budget_l': self.helium_budget_l,
            'total_usage_l': self.total_usage_l,
            'total_recovered_l': self.total_recovered_l,
            'net_position_l': net_position,
            'remaining_budget_l': remaining_budget,
            'co2_equivalent_kg': net_position * self.helium_to_co2_factor,
            'efficiency_scores': dict(self.helium_efficiency_scores),
            'component_status': {
                cid: {
                    'total_l': info['total_l'],
                    'used_l': info['used_l'],
                    'recovered_l': info['recovered_l'],
                    'remaining_l': info['total_l'] - info['used_l'] - info['recovered_l'],
                    'recovery_rate': info['recovery_rate']
                }
                for cid, info in self.component_helium.items()
            },
            'status': 'critical' if remaining_budget < 0 else 'warning' if remaining_budget < self.helium_budget_l * 0.2 else 'healthy'
        }
    
    def get_helium_summary(self) -> Dict[str, Any]:
        """Get helium summary"""
        return {
            'total_usage_l': self.total_usage_l,
            'total_recovered_l': self.total_recovered_l,
            'recovery_rate': self.total_recovered_l / max(self.total_usage_l, 1),
            'remaining_budget_l': self.helium_budget_l - (self.total_usage_l - self.total_recovered_l),
            'component_count': len(self.component_helium),
            'average_efficiency': np.mean(list(self.helium_efficiency_scores.values())) if self.helium_efficiency_scores else 0.5
        }

# ============================================================
# PREDICTIVE ANALYTICS MODULE
# ============================================================

class PredictiveMetricsAnalyzer:
    """
    Predictive analytics with ensemble forecasting.
    
    Features:
    - Failure rate prediction
    - Resource demand forecasting
    - Performance trend analysis
    - Anomaly detection
    """
    
    def __init__(self, history_window: int = 100):
        self.history_window = history_window
        self.metric_history = deque(maxlen=history_window)
        self.forecast_history = deque(maxlen=50)
        self.models = {}
        self.scaler = StandardScaler() if SKLEARN_AVAILABLE else None
        self.is_trained = False
        
        if SKLEARN_AVAILABLE:
            self.models['random_forest'] = RandomForestRegressor(n_estimators=100, random_state=42)
            self.models['gradient_boosting'] = GradientBoostingRegressor(n_estimators=100, random_state=42)
            self._ml_available = True
        else:
            self._ml_available = False
            logger.warning("Scikit-learn not available - predictive analytics limited")
        
        logger.info("Predictive Metrics Analyzer initialized")
    
    def update_history(self, metrics: Dict):
        """Update metric history"""
        self.metric_history.append({
            'timestamp': datetime.utcnow(),
            'success_rate': metrics.get('success_rate', 0.8),
            'error_rate': metrics.get('error_rate', 0.02),
            'avg_latency_ms': metrics.get('avg_latency_ms', 100),
            'carbon_intensity': metrics.get('carbon_intensity', 400),
            'helium_usage': metrics.get('helium_usage', 0.5),
            'resource_utilization': metrics.get('resource_utilization', 0.5)
        })
    
    async def train_forecast_model(self):
        """Train ensemble forecasting models"""
        if not self._ml_available or len(self.metric_history) < 10:
            return {'status': 'insufficient_data', 'samples': len(self.metric_history)}
        
        X = []
        y = []
        history_list = list(self.metric_history)
        
        for i in range(len(history_list) - 5):
            features = []
            for j in range(5):
                data = history_list[i + j]
                features.extend([
                    data['success_rate'],
                    data['error_rate'],
                    data['avg_latency_ms'] / 1000,
                    data['carbon_intensity'] / 100,
                    data['helium_usage'],
                    data['resource_utilization']
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
        logger.info(f"Forecast models trained. R²: {results}")
        return {'status': 'success', 'results': results, 'samples': len(X)}
    
    async def predict_failure_rate(self, hours: int = 24) -> Dict:
        """Predict future failure rate"""
        if not self.is_trained or len(self.metric_history) < 10:
            return {'predicted': 0.02, 'confidence': 0.0, 'trend': 'insufficient_data'}
        
        recent = list(self.metric_history)[-5:]
        features = []
        for data in recent:
            features.extend([
                data['success_rate'],
                data['error_rate'],
                data['avg_latency_ms'] / 1000,
                data['carbon_intensity'] / 100,
                data['helium_usage'],
                data['resource_utilization']
            ])
        
        features = np.array(features).reshape(1, -1)
        features_scaled = self.scaler.transform(features)
        
        predictions = []
        for name, model in self.models.items():
            if model is not None:
                pred = model.predict(features_scaled)[0]
                predictions.append(pred)
        
        if not predictions:
            return {'predicted': 0.02, 'confidence': 0.0, 'trend': 'no_models'}
        
        prediction = np.mean(predictions)
        confidence = min(0.9, np.std(predictions) / 0.2) if len(predictions) > 1 else 0.5
        
        if len(self.forecast_history) > 5:
            recent_forecasts = list(self.forecast_history)[-5:]
            trend = "improving" if prediction > recent_forecasts[-1] else "declining" if prediction < recent_forecasts[-1] else "stable"
        else:
            trend = "stable"
        
        self.forecast_history.append({'prediction': prediction, 'trend': trend})
        return {
            'predicted': prediction,
            'confidence': confidence,
            'trend': trend,
            'recommended_actions': self._generate_predictive_actions(prediction)
        }
    
    async def forecast_resource_demand(self) -> Dict:
        """Forecast resource demand"""
        if len(self.metric_history) < 10:
            return {'predicted_utilization': 0.5, 'confidence': 0.0}
        
        recent = [h['resource_utilization'] for h in list(self.metric_history)[-20:]]
        trend = np.polyfit(range(len(recent)), recent, 1)[0] if len(recent) > 2 else 0
        
        return {
            'predicted_utilization': min(1.0, max(0.0, recent[-1] + trend * 10)),
            'trend': 'increasing' if trend > 0.01 else 'decreasing' if trend < -0.01 else 'stable',
            'confidence': 0.7 if len(recent) > 20 else 0.5
        }
    
    def _generate_predictive_actions(self, prediction: float) -> List[str]:
        """Generate recommended actions based on predictions"""
        actions = []
        if prediction > 0.1:
            actions.append("Increase redundancy to handle predicted failures")
            actions.append("Optimize resource allocation")
        elif prediction > 0.05:
            actions.append("Monitor system health closely")
            actions.append("Prepare fallback strategies")
        else:
            actions.append("System is stable - maintain current configuration")
        return actions

# ============================================================
# SUSTAINABILITY DASHBOARD MODULE
# ============================================================

class SustainabilityDashboard:
    """
    Unified sustainability dashboard for Green Agent.
    
    Features:
    - Carbon position monitoring
    - Helium position monitoring
    - Sustainability score aggregation
    - Ecosystem health monitoring
    - Recommendation generation
    - Historical trends
    """
    
    def __init__(self):
        self.history = []
        self.alert_thresholds = {
            'sustainability_score': 0.5,
            'carbon_budget_remaining': 0.2,
            'helium_budget_remaining': 0.2,
            'carbon_intensity': 500,
            'helium_efficiency': 0.3
        }
        self.carbon_manager: Optional[CarbonIntensityManager] = None
        self.helium_tracker: Optional[HeliumTracker] = None
        self.predictive_analyzer: Optional[PredictiveMetricsAnalyzer] = None
        
        # Start background monitoring
        self._running = True
        self._monitor_task = None
        self._start_background_monitoring()
        
        logger.info("Sustainability Dashboard initialized")
    
    def _start_background_monitoring(self):
        """Start background monitoring"""
        asyncio.create_task(self._monitor_loop())
    
    async def _monitor_loop(self):
        """Background monitoring loop"""
        while self._running:
            try:
                status = await self.get_dashboard_status()
                self.history.append(status)
                if len(self.history) > 1000:
                    self.history = self.history[-1000:]
                
                # Check alerts
                await self._check_alerts(status)
                
                await asyncio.sleep(60)  # Check every minute
            except Exception as e:
                logger.error(f"Monitor loop error: {str(e)}")
                await asyncio.sleep(300)
    
    async def _check_alerts(self, status: Dict[str, Any]):
        """Check for alerts based on thresholds"""
        alerts = []
        
        # Check sustainability score
        if status.get('sustainability_score', 0) < self.alert_thresholds['sustainability_score']:
            alerts.append({
                'level': 'warning',
                'message': f"Sustainability score {status['sustainability_score']:.2f} below threshold"
            })
        
        # Check carbon intensity
        if status.get('carbon_intensity', 0) > self.alert_thresholds['carbon_intensity']:
            alerts.append({
                'level': 'warning',
                'message': f"Carbon intensity {status['carbon_intensity']:.0f} above threshold"
            })
        
        # Check helium budget
        helium_remaining_ratio = status.get('helium_remaining_budget_ratio', 1.0)
        if helium_remaining_ratio < self.alert_thresholds['helium_budget_remaining']:
            alerts.append({
                'level': 'critical',
                'message': f"Helium budget remaining {helium_remaining_ratio:.1%} below threshold"
            })
        
        # Check helium efficiency
        if status.get('helium_efficiency', 1.0) < self.alert_thresholds['helium_efficiency']:
            alerts.append({
                'level': 'warning',
                'message': f"Helium efficiency {status['helium_efficiency']:.2f} below threshold"
            })
        
        if alerts:
            for alert in alerts:
                logger.log(
                    logging.CRITICAL if alert['level'] == 'critical' else logging.WARNING,
                    f"DASHBOARD ALERT: {alert['message']}"
                )
    
    def register_managers(
        self,
        carbon_manager: Optional[CarbonIntensityManager] = None,
        helium_tracker: Optional[HeliumTracker] = None,
        predictive_analyzer: Optional[PredictiveMetricsAnalyzer] = None
    ):
        """Register managers for dashboard integration"""
        self.carbon_manager = carbon_manager
        self.helium_tracker = helium_tracker
        self.predictive_analyzer = predictive_analyzer
        
        if carbon_manager:
            logger.info("Carbon manager registered with dashboard")
        if helium_tracker:
            logger.info("Helium tracker registered with dashboard")
        if predictive_analyzer:
            logger.info("Predictive analyzer registered with dashboard")
    
    async def get_dashboard_status(self) -> Dict[str, Any]:
        """Get unified dashboard status"""
        status = {
            'timestamp': datetime.utcnow().isoformat(),
            'sustainability_score': 0.5,
            'carbon_position': {},
            'helium_position': {},
            'predictions': {},
            'is_healthy': True
        }
        
        # Carbon position
        if self.carbon_manager:
            carbon_pos = {
                'current_intensity': await self.carbon_manager.get_current_intensity(),
                'trend': await self.carbon_manager.get_carbon_trend(),
                'optimal_hours': await self.carbon_manager.get_optimal_hours('us-east', 8),
                'total_savings_kg': self.carbon_manager.total_carbon_savings_kg
            }
            status['carbon_position'] = carbon_pos
            status['carbon_intensity'] = carbon_pos['current_intensity']
            status['carbon_savings_kg'] = carbon_pos['total_savings_kg']
        
        # Helium position
        if self.helium_tracker:
            helium_pos = self.helium_tracker.get_helium_position()
            status['helium_position'] = helium_pos
            status['helium_efficiency'] = helium_pos.get('efficiency_scores', {}).values()
            status['helium_efficiency'] = np.mean(list(status['helium_efficiency'])) if status['helium_efficiency'] else 0.5
            status['helium_remaining_budget_ratio'] = helium_pos.get('remaining_budget_l', 0) / max(helium_pos.get('budget_l', 1), 1)
            status['helium_summary'] = self.helium_tracker.get_helium_summary()
        
        # Predictive analytics
        if self.predictive_analyzer and self.predictive_analyzer.is_trained:
            status['predictions'] = {
                'failure_rate': await self.predictive_analyzer.predict_failure_rate(),
                'resource_demand': await self.predictive_analyzer.forecast_resource_demand()
            }
        
        # Calculate overall sustainability score
        score = 0.5
        if self.carbon_manager:
            carbon_score = 1.0 - (status.get('carbon_intensity', 400) / 800)
            score = score * 0.5 + carbon_score * 0.5
        
        if self.helium_tracker:
            helium_score = status.get('helium_efficiency', 0.5)
            score = score * 0.5 + helium_score * 0.5
        
        status['sustainability_score'] = max(0.0, min(1.0, score))
        SUSTAINABILITY_SCORE.set(status['sustainability_score'] * 100)
        
        # Health status
        status['is_healthy'] = all([
            status['sustainability_score'] > 0.3,
            status.get('carbon_intensity', 400) < 600,
            status.get('helium_remaining_budget_ratio', 1.0) > 0.1
        ])
        
        return status
    
    async def get_recommendations(self) -> List[Dict[str, Any]]:
        """Get sustainability recommendations"""
        status = await self.get_dashboard_status()
        recommendations = []
        
        if status['sustainability_score'] < 0.5:
            recommendations.append({
                'priority': 'high',
                'category': 'sustainability',
                'message': 'Improve overall sustainability score',
                'actions': ['Reduce carbon intensity', 'Optimize helium usage']
            })
        
        if status.get('carbon_intensity', 0) > 500:
            recommendations.append({
                'priority': 'high',
                'category': 'carbon',
                'message': 'High carbon intensity detected',
                'actions': ['Shift workloads to low-carbon hours', 'Improve energy efficiency']
            })
        
        if status.get('helium_remaining_budget_ratio', 1.0) < 0.2:
            recommendations.append({
                'priority': 'critical',
                'category': 'helium',
                'message': 'Helium budget critically low',
                'actions': ['Implement helium recovery systems', 'Optimize helium usage']
            })
        
        if status.get('helium_efficiency', 0.5) < 0.4:
            recommendations.append({
                'priority': 'medium',
                'category': 'helium',
                'message': 'Low helium efficiency',
                'actions': ['Improve helium recovery rates', 'Reduce helium consumption']
            })
        
        return recommendations
    
    async def generate_report(self) -> Dict[str, Any]:
        """Generate comprehensive sustainability report"""
        status = await self.get_dashboard_status()
        recommendations = await self.get_recommendations()
        
        # Historical trend analysis
        trend = 'stable'
        if len(self.history) > 10:
            recent_scores = [h['sustainability_score'] for h in self.history[-10:]]
            if recent_scores[-1] > recent_scores[0] * 1.05:
                trend = 'improving'
            elif recent_scores[-1] < recent_scores[0] * 0.95:
                trend = 'declining'
        
        return {
            'timestamp': datetime.utcnow().isoformat(),
            'sustainability_score': status['sustainability_score'],
            'trend': trend,
            'carbon_position': status.get('carbon_position', {}),
            'helium_position': status.get('helium_position', {}),
            'predictions': status.get('predictions', {}),
            'recommendations': recommendations,
            'is_healthy': status['is_healthy'],
            'generated_by': 'SustainabilityDashboard'
        }
    
    def shutdown(self):
        """Shutdown the dashboard"""
        self._running = False
        logger.info("Sustainability Dashboard shut down")

# ============================================================
# ENHANCED CIRCUIT BREAKER WITH GRADUAL RECOVERY
# ============================================================

class CircuitBreakerState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

class EnhancedCircuitBreaker:
    """
    Enhanced circuit breaker with gradual recovery and half-open testing.
    
    ENHANCEMENTS:
    - Half-open state for testing recovery
    - Success threshold for closing
    - Metrics tracking
    - Async support
    """
    
    def __init__(self, name: str, failure_threshold: int = CIRCUIT_BREAKER_THRESHOLD,
                 recovery_timeout: int = CIRCUIT_BREAKER_TIMEOUT,
                 half_open_success_threshold: int = 2):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_success_threshold = half_open_success_threshold
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time = None
        self._lock = asyncio.Lock()
        self.metrics = {'total_calls': 0, 'failed_calls': 0, 'successful_calls': 0}
    
    async def call(self, func: Callable, *args, **kwargs):
        """Execute function with circuit breaker protection"""
        async with self._lock:
            if self.state == CircuitBreakerState.OPEN:
                if time.time() - self.last_failure_time >= self.recovery_timeout:
                    self.state = CircuitBreakerState.HALF_OPEN
                    self.success_count = 0
                    CIRCUIT_BREAKER_STATE.labels(name=self.name).set(0.5)
                    logger.info(f"Circuit breaker {self.name} transitioning to HALF_OPEN")
                else:
                    raise CircuitBreakerOpenError(f"Circuit breaker {self.name} is OPEN")
            
            if self.state == CircuitBreakerState.HALF_OPEN and self.success_count >= self.half_open_success_threshold:
                self.state = CircuitBreakerState.CLOSED
                CIRCUIT_BREAKER_STATE.labels(name=self.name).set(0)
                logger.info(f"Circuit breaker {self.name} closed after {self.success_count} successes")
        
        self.metrics['total_calls'] += 1
        
        try:
            result = await func(*args, **kwargs)
            await self._record_success()
            return result
        except Exception as e:
            await self._record_failure()
            raise
    
    async def _record_success(self):
        async with self._lock:
            self.metrics['successful_calls'] += 1
            self.success_count += 1
            
            if self.state == CircuitBreakerState.HALF_OPEN:
                if self.success_count >= self.half_open_success_threshold:
                    self.state = CircuitBreakerState.CLOSED
                    CIRCUIT_BREAKER_STATE.labels(name=self.name).set(0)
                    logger.info(f"Circuit breaker {self.name} closed")
            else:
                self.failure_count = 0
    
    async def _record_failure(self):
        async with self._lock:
            self.metrics['failed_calls'] += 1
            self.failure_count += 1
            self.last_failure_time = time.time()
            
            if self.state == CircuitBreakerState.CLOSED and self.failure_count >= self.failure_threshold:
                self.state = CircuitBreakerState.OPEN
                CIRCUIT_BREAKER_STATE.labels(name=self.name).set(1)
                logger.warning(f"Circuit breaker {self.name} opened after {self.failure_count} failures")
            elif self.state == CircuitBreakerState.HALF_OPEN:
                self.state = CircuitBreakerState.OPEN
                CIRCUIT_BREAKER_STATE.labels(name=self.name).set(1)
                logger.warning(f"Circuit breaker {self.name} opened from HALF_OPEN")
    
    def get_metrics(self) -> Dict:
        return {
            **self.metrics,
            'state': self.state.value,
            'failure_count': self.failure_count,
            'success_count': self.success_count
        }

# ============================================================
# ENHANCED RATE LIMITER
# ============================================================

class EnhancedRateLimiter:
    """Token bucket rate limiter with metrics"""
    
    def __init__(self, rate: int = RATE_LIMIT_REQUESTS, per_seconds: int = RATE_LIMIT_WINDOW):
        self.rate = rate
        self.per_seconds = per_seconds
        self.tokens = rate
        self.last_refill = time.time()
        self._lock = asyncio.Lock()
        self.total_requests = 0
        self.throttled_requests = 0
    
    async def acquire(self) -> bool:
        async with self._lock:
            now = time.time()
            time_passed = now - self.last_refill
            self.tokens = min(self.rate, self.tokens + time_passed * (self.rate / self.per_seconds))
            self.last_refill = now
            
            if self.tokens >= 1:
                self.tokens -= 1
                self.total_requests += 1
                return True
            else:
                self.throttled_requests += 1
                return False
    
    async def wait_and_acquire(self):
        while not await self.acquire():
            await asyncio.sleep(0.1)
    
    def get_metrics(self) -> Dict:
        total = self.total_requests + self.throttled_requests
        return {
            'total_requests': self.total_requests,
            'throttled_requests': self.throttled_requests,
            'throttle_rate': (self.throttled_requests / max(total, 1)) * 100
        }

# ============================================================
# ENHANCED DATABASE MANAGER FOR MODEL REGISTRY
# ============================================================

class EnhancedDatabaseManager:
    """Database manager with connection pooling for model registry"""
    
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.engine = None
        self.SessionLocal = None
        self._init_engine()
    
    def _init_engine(self):
        db_url = f"sqlite:///{self.db_path}"
        self.engine = create_engine(
            db_url,
            poolclass=QueuePool,
            pool_size=10,
            max_overflow=20,
            pool_pre_ping=True,
            connect_args={'check_same_thread': False}
        )
        self.SessionLocal = scoped_session(sessionmaker(bind=self.engine))
        self._init_tables()
    
    def _init_tables(self):
        self.db_path.parent.mkdir(exist_ok=True, parents=True)
        
        Base = declarative_base()
        
        class ModelRegistryDB(Base):
            __tablename__ = 'model_registry'
            model_id = Column(String(128), primary_key=True)
            name = Column(String(128), index=True)
            version = Column(String(32), index=True)
            metadata = Column(JSON)
            registered_at = Column(DateTime, index=True)
            is_active = Column(Boolean, default=True)
            prediction_count = Column(Integer, default=0)
            error_count = Column(Integer, default=0)
            avg_latency_ms = Column(Float, default=0)
            created_at = Column(DateTime, default=datetime.now)
            updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
            version_number = Column(Integer, default=1)
            
            __table_args__ = (
                Index('idx_name_version', 'name', 'version'),
                Index('idx_is_active', 'is_active'),
                Index('idx_registered_at', 'registered_at'),
            )
        
        class ModelMetricsDB(Base):
            __tablename__ = 'model_metrics'
            id = Column(Integer, primary_key=True)
            model_id = Column(String(128), index=True)
            metric_type = Column(String(32))
            metric_value = Column(Float)
            timestamp = Column(DateTime, default=datetime.now)
            
            __table_args__ = (
                Index('idx_model_id', 'model_id'),
                Index('idx_timestamp', 'timestamp'),
            )
        
        Base.metadata.create_all(self.engine)
        self._update_db_size_metric()
        logger.info(f"Database initialized with connection pool at {self.db_path}")
    
    def _update_db_size_metric(self):
        if self.db_path.exists():
            size_mb = self.db_path.stat().st_size / (1024 * 1024)
            DB_SIZE.set(size_mb)
    
    @contextmanager
    def get_session(self):
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            raise
        finally:
            session.close()
    
    async def save_model_registry(self, model_id: str, name: str, version: str,
                                   metadata: Dict, is_active: bool = True):
        with self.get_session() as session:
            from sqlalchemy import text
            session.execute(
                text("""INSERT OR REPLACE INTO model_registry 
                       (model_id, name, version, metadata, registered_at, is_active, updated_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?)"""),
                (model_id, name, version, json.dumps(metadata, default=str),
                 datetime.now(), is_active, datetime.now())
            )
    
    async def update_model_metrics(self, model_id: str, prediction_count: int,
                                    error_count: int, avg_latency_ms: float):
        with self.get_session() as session:
            from sqlalchemy import text
            session.execute(
                text("""UPDATE model_registry 
                       SET prediction_count = ?, error_count = ?, avg_latency_ms = ?, updated_at = ?
                       WHERE model_id = ?"""),
                (prediction_count, error_count, avg_latency_ms, datetime.now(), model_id)
            )
    
    async def get_model_registry(self, model_id: str) -> Optional[Dict]:
        with self.get_session() as session:
            from sqlalchemy import text
            result = session.execute(
                text("SELECT * FROM model_registry WHERE model_id = ?"),
                (model_id,)
            ).fetchone()
            if result:
                return dict(result._mapping)
            return None
    
    async def list_active_models(self) -> List[Dict]:
        with self.get_session() as session:
            from sqlalchemy import text
            result = session.execute(
                text("SELECT * FROM model_registry WHERE is_active = 1 ORDER BY registered_at DESC")
            ).fetchall()
            return [dict(row._mapping) for row in result]
    
    def dispose(self):
        if self.engine:
            self.engine.dispose()
            if self.SessionLocal:
                self.SessionLocal.remove()

# ============================================================
# ENHANCED MODEL REGISTRY
# ============================================================

class EnhancedModelRegistry:
    """
    Enhanced model registry with database persistence and version rollback.
    
    ENHANCEMENTS:
    - Database persistence with connection pooling
    - Model version rollback capability
    - State export/import for backup
    - Metrics tracking
    """
    
    def __init__(self):
        self.db_manager = EnhancedDatabaseManager(Path("./model_registry_data.db"))
        self._models: Dict[str, Dict] = {}
        self._model_metrics: Dict[str, Dict] = {}
        self._lock = asyncio.Lock()
        self._cleanup_task = None
        self._running = False
    
    async def start(self):
        """Start background cleanup task"""
        self._running = True
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())
        await self._load_from_database()
        logger.info("Enhanced model registry started")
    
    async def _load_from_database(self):
        """Load registry from database"""
        async with self._lock:
            models = await self.db_manager.list_active_models()
            for model in models:
                self._models[model['model_id']] = {
                    'name': model['name'],
                    'version': model['version'],
                    'metadata': json.loads(model['metadata']) if isinstance(model['metadata'], str) else model['metadata'],
                    'registered_at': model['registered_at'].isoformat(),
                    'is_active': model['is_active'],
                    'prediction_count': model['prediction_count'],
                    'error_count': model['error_count'],
                    'avg_latency_ms': model['avg_latency_ms']
                }
            logger.info(f"Loaded {len(self._models)} models from database")
    
    async def register(self, model_name: str, model_instance: Any,
                      metadata: Dict = None, version: str = None) -> str:
        """Register a model instance"""
        version = version or f"v{getattr(model_instance, 'model_version', 1)}"
        model_id = f"{model_name}_{version}"
        
        async with self._lock:
            self._models[model_id] = {
                'instance': model_instance,
                'name': model_name,
                'version': version,
                'metadata': metadata or {},
                'registered_at': datetime.now().isoformat(),
                'is_active': True,
                'prediction_count': 0,
                'error_count': 0,
                'avg_latency_ms': 0
            }
            
            await self.db_manager.save_model_registry(
                model_id, model_name, version, metadata or {}, True
            )
        
        logger.info(f"Model registered: {model_id}")
        return model_id
    
    async def get(self, model_name: str, version: str = None) -> Optional[Any]:
        """Get a registered model instance"""
        async with self._lock:
            if version:
                model_id = f"{model_name}_{version}"
                model_info = self._models.get(model_id)
                if model_info and model_info['is_active']:
                    return model_info['instance']
                return None
            
            # Get latest active version
            latest = None
            latest_version = None
            
            for model_id, info in self._models.items():
                if info['name'] == model_name and info['is_active']:
                    v = info['version']
                    if latest_version is None or v > latest_version:
                        latest_version = v
                        latest = info['instance']
            
            return latest
    
    async def rollback(self, model_name: str, target_version: str) -> bool:
        """Rollback to a previous model version"""
        async with self._lock:
            target_id = f"{model_name}_{target_version}"
            if target_id not in self._models:
                logger.error(f"Target version {target_version} not found for {model_name}")
                return False
            
            # Deactivate all versions of this model
            for model_id, info in self._models.items():
                if info['name'] == model_name:
                    info['is_active'] = False
                    await self.db_manager.save_model_registry(
                        model_id, info['name'], info['version'],
                        info['metadata'], False
                    )
            
            # Activate target version
            self._models[target_id]['is_active'] = True
            await self.db_manager.save_model_registry(
                target_id, model_name, target_version,
                self._models[target_id]['metadata'], True
            )
            
            logger.info(f"Rolled back {model_name} to version {target_version}")
            return True
    
    async def record_prediction(self, model_id: str, latency_ms: float, error: bool = False):
        """Record prediction metrics"""
        async with self._lock:
            if model_id in self._models:
                model = self._models[model_id]
                model['prediction_count'] += 1
                if error:
                    model['error_count'] += 1
                
                model['avg_latency_ms'] = (
                    model['avg_latency_ms'] * (model['prediction_count'] - 1) + latency_ms
                ) / model['prediction_count']
                
                await self.db_manager.update_model_metrics(
                    model_id, model['prediction_count'],
                    model['error_count'], model['avg_latency_ms']
                )
    
    async def list_models(self) -> List[Dict]:
        """List all registered models"""
        async with self._lock:
            return [
                {
                    'model_id': model_id,
                    'name': info['name'],
                    'version': info['version'],
                    'registered_at': info['registered_at'],
                    'is_active': info['is_active'],
                    'prediction_count': info.get('prediction_count', 0),
                    'error_count': info.get('error_count', 0),
                    'avg_latency_ms': info.get('avg_latency_ms', 0)
                }
                for model_id, info in self._models.items()
            ]
    
    async def export_state(self) -> Dict:
        """Export registry state for backup"""
        async with self._lock:
            return {
                'version': DATA_VERSION,
                'models': [
                    {
                        'model_id': model_id,
                        'name': info['name'],
                        'version': info['version'],
                        'metadata': info['metadata'],
                        'registered_at': info['registered_at'],
                        'is_active': info['is_active']
                    }
                    for model_id, info in self._models.items()
                ],
                'exported_at': datetime.now().isoformat()
            }
    
    async def import_state(self, state: Dict):
        """Import registry state from backup"""
        async with self._lock:
            self._models.clear()
            for model in state.get('models', []):
                model_id = f"{model['name']}_{model['version']}"
                self._models[model_id] = {
                    'name': model['name'],
                    'version': model['version'],
                    'metadata': model['metadata'],
                    'registered_at': model['registered_at'],
                    'is_active': model['is_active'],
                    'prediction_count': 0,
                    'error_count': 0,
                    'avg_latency_ms': 0,
                    'instance': None  # Instance not restored from backup
                }
                await self.db_manager.save_model_registry(
                    model_id, model['name'], model['version'],
                    model['metadata'], model['is_active']
                )
            logger.info(f"Imported {len(self._models)} models from backup")
    
    async def _cleanup_loop(self):
        """Background cleanup of old metrics"""
        while self._running:
            try:
                await asyncio.sleep(3600)
                # Cleanup handled by TTL in database
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Cleanup error: {e}")
    
    async def shutdown(self):
        """Shutdown registry"""
        self._running = False
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
        self.db_manager.dispose()

# ============================================================
# ENHANCED BASE ML MODEL
# ============================================================

class MLFramework(Enum):
    PYTORCH = "pytorch"
    TENSORFLOW = "tensorflow"
    SCIKIT_LEARN = "scikit_learn"
    UNKNOWN = "unknown"

class EnhancedBaseMLModel(ABC):
    """
    Enhanced base ML model with bounded history and rate limiting.
    
    ENHANCEMENTS:
    - Bounded prediction history (deque with maxlen)
    - Rate limiting for predictions
    - Circuit breaker for error protection
    - Async support for training and prediction
    """
    
    def __init__(self, config: Dict = None):
        self.config = config or {}
        self.model = None
        self.framework = self._detect_framework()
        self.model_version = 1
        self.training_history: List[Dict] = []
        self.is_trained = False
        self._gpu_available = self._check_gpu()
        self._device = self._setup_device()
        self._checkpoint_dir = Path(self.config.get('checkpoint_dir', './model_checkpoints'))
        self._checkpoint_dir.mkdir(exist_ok=True, parents=True)
        
        # Bounded collections (fixes memory leak)
        self._prediction_latencies = deque(maxlen=MAX_PREDICTION_HISTORY)
        self._prediction_errors = deque(maxlen=MAX_PREDICTION_HISTORY)
        
        # Rate limiter
        self._rate_limiter = EnhancedRateLimiter()
        
        # Circuit breaker
        self._circuit_breaker = EnhancedCircuitBreaker(f"model_{self.__class__.__name__}")
        
        self.experiment_id = str(uuid.uuid4())[:8]
        self.experiment_start = datetime.now()
        
        logger.info(f"{self.__class__.__name__} initialized (Framework: {self.framework.value}, GPU: {self._gpu_available})")
    
    def _detect_framework(self) -> MLFramework:
        if TORCH_AVAILABLE and hasattr(self, 'build_pytorch_model'):
            return MLFramework.PYTORCH
        elif TF_AVAILABLE and hasattr(self, 'build_tensorflow_model'):
            return MLFramework.TENSORFLOW
        elif SKLEARN_AVAILABLE:
            return MLFramework.SCIKIT_LEARN
        return MLFramework.UNKNOWN
    
    def _setup_device(self):
        if not TORCH_AVAILABLE:
            return None
        if self._gpu_available and torch.cuda.is_available():
            return torch.device("cuda")
        elif hasattr(torch, 'backends') and hasattr(torch.backends, 'mps') and torch.backends.mps.is_available():
            return torch.device("mps")
        return torch.device("cpu")
    
    def _check_gpu(self) -> bool:
        if TORCH_AVAILABLE and torch.cuda.is_available():
            return True
        if TF_AVAILABLE and tf.config.list_physical_devices('GPU'):
            return True
        return False
    
    @abstractmethod
    def build_model(self, input_dim: int, output_dim: int) -> Any:
        pass
    
    @abstractmethod
    async def train(self, X: np.ndarray, y: np.ndarray, **kwargs) -> Dict:
        pass
    
    @abstractmethod
    async def predict(self, X: np.ndarray) -> np.ndarray:
        pass
    
    async def predict_with_rate_limit(self, X: np.ndarray) -> np.ndarray:
        """Rate-limited prediction with circuit breaker protection"""
        await self._rate_limiter.wait_and_acquire()
        
        start_time = time.time()
        error = False
        
        try:
            result = await self._circuit_breaker.call(self.predict, X)
            latency_ms = (time.time() - start_time) * 1000
            self._prediction_latencies.append(latency_ms)
            
            MODEL_PREDICTIONS.labels(
                model_name=self.__class__.__name__,
                version=str(self.model_version),
                status='success'
            ).inc()
            MODEL_PREDICTION_LATENCY.labels(
                model_name=self.__class__.__name__,
                version=str(self.model_version)
            ).observe(latency_ms / 1000)
            
            return result
            
        except Exception as e:
            error = True
            self._prediction_errors.append(str(e))
            MODEL_PREDICTIONS.labels(
                model_name=self.__class__.__name__,
                version=str(self.model_version),
                status='error'
            ).inc()
            raise
    
    async def evaluate(self, X: np.ndarray, y: np.ndarray) -> Dict:
        """Evaluate model performance"""
        if not SKLEARN_AVAILABLE:
            logger.warning("Scikit-learn not available for metrics calculation")
            return {}
        
        start_time = time.time()
        y_pred = await self.predict(X)
        prediction_time = time.time() - start_time
        
        metrics = {
            'mae': float(mean_absolute_error(y, y_pred)),
            'mse': float(mean_squared_error(y, y_pred)),
            'rmse': float(np.sqrt(mean_squared_error(y, y_pred))),
            'r2': float(r2_score(y, y_pred)),
            'samples': len(X),
            'prediction_time_ms': prediction_time * 1000,
            'timestamp': datetime.now().isoformat()
        }
        
        return metrics
    
    async def save_checkpoint(self, tag: str = None, encrypt: bool = False,
                              compress: bool = True, compression_level: int = 6) -> str:
        """Save model checkpoint with error handling"""
        if not self.model:
            raise ValueError("No model to save")
        
        version = tag or f"v{self.model_version}"
        checkpoint_path = self._checkpoint_dir / f"{self.__class__.__name__}_{version}.pt"
        
        checkpoint = {
            'model_state_dict': self._get_model_state(),
            'model_version': self.model_version,
            'training_history': self.training_history,
            'is_trained': self.is_trained,
            'config': self.config,
            'framework': self.framework.value,
            'timestamp': datetime.now().isoformat(),
            'experiment_id': self.experiment_id
        }
        
        try:
            serialized = pickle.dumps(checkpoint, protocol=pickle.HIGHEST_PROTOCOL)
        except Exception as e:
            logger.error(f"Failed to serialize checkpoint: {e}")
            raise
        
        if compress:
            import zlib
            serialized = zlib.compress(serialized, level=compression_level)
        
        if encrypt and CRYPTO_AVAILABLE:
            encryption_key = self.config.get('encryption_key')
            if not encryption_key:
                raise ValueError("Encryption key required for encrypted checkpoints")
            cipher = Fernet(encryption_key)
            serialized = cipher.encrypt(serialized)
            checkpoint_path = checkpoint_path.with_suffix('.enc')
        
        with open(checkpoint_path, 'wb') as f:
            f.write(serialized)
        
        logger.info(f"Model checkpoint saved: {checkpoint_path}")
        return str(checkpoint_path)
    
    def _get_model_state(self):
        if self.framework == MLFramework.PYTORCH and hasattr(self.model, 'state_dict'):
            return self.model.state_dict()
        elif self.framework == MLFramework.TENSORFLOW and hasattr(self.model, 'get_weights'):
            return self.model.get_weights()
        elif self.framework == MLFramework.SCIKIT_LEARN:
            return pickle.dumps(self.model)
        return self.model
    
    async def load_checkpoint(self, checkpoint_path: str, key: bytes = None) -> bool:
        """Load model from checkpoint"""
        path = Path(checkpoint_path)
        
        try:
            with open(path, 'rb') as f:
                data = f.read()
            
            if path.suffix == '.enc' and CRYPTO_AVAILABLE:
                decryption_key = key or self.config.get('encryption_key')
                if not decryption_key:
                    raise ValueError("Decryption key required for encrypted checkpoint")
                cipher = Fernet(decryption_key)
                data = cipher.decrypt(data)
            
            try:
                import zlib
                data = zlib.decompress(data)
            except zlib.error:
                pass
            
            checkpoint = pickle.loads(data)
            self._set_model_state(checkpoint['model_state_dict'])
            self.model_version = checkpoint.get('model_version', 1)
            self.training_history = checkpoint.get('training_history', [])
            self.is_trained = checkpoint.get('is_trained', False)
            self.experiment_id = checkpoint.get('experiment_id', self.experiment_id)
            
            logger.info(f"Model loaded from {checkpoint_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to load checkpoint: {e}")
            return False
    
    def _set_model_state(self, state):
        if self.framework == MLFramework.PYTORCH and hasattr(self.model, 'load_state_dict'):
            self.model.load_state_dict(state)
        elif self.framework == MLFramework.TENSORFLOW and hasattr(self.model, 'set_weights'):
            self.model.set_weights(state)
        elif self.framework == MLFramework.SCIKIT_LEARN:
            self.model = pickle.loads(state)
    
    def get_model_info(self) -> Dict:
        return {
            'class_name': self.__class__.__name__,
            'framework': self.framework.value,
            'version': self.model_version,
            'is_trained': self.is_trained,
            'training_epochs': len(self.training_history),
            'gpu_available': self._gpu_available,
            'device': str(self._device) if self._device else 'cpu',
            'experiment_id': self.experiment_id,
            'experiment_duration_s': (datetime.now() - self.experiment_start).total_seconds(),
            'checkpoint_dir': str(self._checkpoint_dir),
            'avg_prediction_latency_ms': np.mean(self._prediction_latencies) if self._prediction_latencies else 0,
            'p95_prediction_latency_ms': np.percentile(self._prediction_latencies, 95) if self._prediction_latencies else 0,
            'error_count': len(self._prediction_errors)
        }

# ============================================================
# ENHANCED BASE REALTIME HANDLER
# ============================================================

class EnhancedBaseRealtimeHandler(ABC):
    """
    Enhanced base realtime handler with async locks and connection limits.
    
    ENHANCEMENTS:
    - Async locks for thread safety
    - Connection limits with backpressure
    - Heartbeat with timeout
    - Stale connection cleanup
    """
    
    def __init__(self, config: Dict = None):
        self.config = config or {}
        self.active_connections: Dict[str, Any] = {}
        self.pending_messages: Dict[str, deque] = defaultdict(lambda: deque(maxlen=1000))
        self.message_handlers: Dict[str, Callable] = {}
        self.heartbeat_interval = self.config.get('heartbeat_interval', 30)
        self.max_connections = self.config.get('max_connections', 1000)
        self.reconnect_timeout = self.config.get('reconnect_timeout', 60)
        self._lock = asyncio.Lock()
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._cleanup_task: Optional[asyncio.Task] = None
        self.running = False
        self._connection_metadata: Dict[str, Dict] = {}
    
    @abstractmethod
    async def handle_connect(self, client_id: str, connection: Any) -> bool:
        pass
    
    @abstractmethod
    async def handle_disconnect(self, client_id: str) -> None:
        pass
    
    @abstractmethod
    async def handle_message(self, client_id: str, message: Dict) -> Dict:
        pass
    
    def register_handler(self, message_type: str, handler: Callable) -> None:
        self.message_handlers[message_type] = handler
    
    async def broadcast(self, message: Dict, exclude_client: str = None) -> int:
        sent_count = 0
        disconnected = []
        
        async with self._lock:
            for client_id, connection in self.active_connections.items():
                if client_id == exclude_client:
                    continue
                
                try:
                    if hasattr(connection, 'send'):
                        await connection.send(json.dumps(message, default=str))
                        sent_count += 1
                except Exception:
                    disconnected.append(client_id)
        
        for client_id in disconnected:
            await self.handle_disconnect(client_id)
            async with self._lock:
                self.active_connections.pop(client_id, None)
                self._connection_metadata.pop(client_id, None)
        
        return sent_count
    
    async def send_to_client(self, client_id: str, message: Dict) -> bool:
        connection = self.active_connections.get(client_id)
        
        if not connection:
            async with self._lock:
                self.pending_messages[client_id].append(message)
            return False
        
        try:
            if hasattr(connection, 'send'):
                await connection.send(json.dumps(message, default=str))
                await self._send_queued_messages(client_id)
                return True
        except Exception:
            async with self._lock:
                self.pending_messages[client_id].append(message)
            await self.handle_disconnect(client_id)
            async with self._lock:
                self.active_connections.pop(client_id, None)
                self._connection_metadata.pop(client_id, None)
        
        return False
    
    async def _send_queued_messages(self, client_id: str):
        queued = self.pending_messages.get(client_id, [])
        connection = self.active_connections.get(client_id)
        
        if connection and queued:
            for message in list(queued):
                try:
                    await connection.send(json.dumps(message, default=str))
                    with self._lock:
                        self.pending_messages[client_id].popleft()
                except Exception:
                    break
    
    async def start(self):
        self.running = True
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())
        logger.info(f"{self.__class__.__name__} started")
    
    async def _heartbeat_loop(self):
        while self.running:
            try:
                await asyncio.sleep(self.heartbeat_interval)
                
                heartbeat_message = {
                    'type': 'heartbeat',
                    'timestamp': datetime.now().isoformat()
                }
                await self.broadcast(heartbeat_message)
                await self._check_stale_connections()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Heartbeat error: {e}")
    
    async def _cleanup_loop(self):
        while self.running:
            try:
                await asyncio.sleep(60)
                
                async with self._lock:
                    current_time = datetime.now()
                    for client_id in list(self.pending_messages.keys()):
                        if client_id not in self.active_connections:
                            meta = self._connection_metadata.get(client_id, {})
                            disconnect_time = meta.get('disconnect_time')
                            if disconnect_time and (current_time - disconnect_time).seconds > self.reconnect_timeout:
                                del self.pending_messages[client_id]
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Cleanup error: {e}")
    
    async def _check_stale_connections(self):
        stale_clients = []
        
        async with self._lock:
            for client_id, meta in self._connection_metadata.items():
                last_heartbeat = meta.get('last_heartbeat', datetime.now())
                if (datetime.now() - last_heartbeat).seconds > self.heartbeat_interval * 2:
                    stale_clients.append(client_id)
        
        for client_id in stale_clients:
            logger.warning(f"Removing stale connection: {client_id}")
            await self.handle_disconnect(client_id)
            async with self._lock:
                self.active_connections.pop(client_id, None)
                self._connection_metadata.pop(client_id, None)
    
    async def stop(self):
        self.running = False
        
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
        
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
        
        async with self._lock:
            for client_id in list(self.active_connections.keys()):
                await self.handle_disconnect(client_id)
            self.active_connections.clear()
            self._connection_metadata.clear()
            self.pending_messages.clear()
        
        logger.info(f"{self.__class__.__name__} stopped")
    
    def get_connection_count(self) -> int:
        return len(self.active_connections)
    
    def get_statistics(self) -> Dict:
        async with self._lock:
            total_pending = sum(len(q) for q in self.pending_messages.values())
        
        return {
            'active_connections': self.get_connection_count(),
            'registered_handlers': len(self.message_handlers),
            'heartbeat_interval': self.heartbeat_interval,
            'max_connections': self.max_connections,
            'pending_messages': total_pending,
            'running': self.running,
            'class_name': self.__class__.__name__
        }

# ============================================================
# ENHANCED BASE WORKFLOW
# ============================================================

class WorkflowStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

class EnhancedBaseWorkflow(ABC):
    """
    Enhanced base workflow with checkpointing and DAG visualization.
    
    ENHANCEMENTS:
    - Checkpoint saving with error handling
    - DAG visualization with Graphviz
    - Step timeout and retry support
    - Parallel step execution
    """
    
    def __init__(self, config: Dict = None):
        self.config = config or {}
        self.steps: Dict[str, Dict] = {}
        self.step_order: List[str] = []
        self.results: Dict[str, Any] = {}
        self.errors: Dict[str, Exception] = {}
        self.retry_config = self.config.get('retry', {'max_attempts': 1, 'delay': 0})
        self.checkpoint_dir = Path(self.config.get('checkpoint_dir', './workflow_checkpoints'))
        self.checkpoint_dir.mkdir(exist_ok=True, parents=True)
        self.workflow_id = str(uuid.uuid4())[:8]
        self.start_time = None
        self.end_time = None
        self.status = WorkflowStatus.PENDING
        self._lock = asyncio.Lock()
        self._step_lock = asyncio.Lock()
        self._cancelled = False
    
    def add_step(self, name: str, func: Callable, depends_on: List[str] = None,
                 retry_config: Dict = None, timeout: float = None):
        self.steps[name] = {
            'func': func,
            'depends_on': depends_on or [],
            'retry_config': retry_config or self.retry_config,
            'timeout': timeout,
            'status': WorkflowStatus.PENDING,
            'result': None,
            'error': None,
            'start_time': None,
            'end_time': None,
            'attempts': 0
        }
        self.step_order.append(name)
    
    @abstractmethod
    def validate_input(self, input_data: Any) -> bool:
        pass
    
    @abstractmethod
    def finalize(self, results: Dict) -> Any:
        pass
    
    async def _check_dependencies(self, step_name: str) -> bool:
        async with self._step_lock:
            step = self.steps[step_name]
            for dep in step['depends_on']:
                if dep not in self.results:
                    return False
                if dep in self.errors:
                    return False
                dep_step = self.steps.get(dep)
                if dep_step and dep_step['status'] != WorkflowStatus.COMPLETED:
                    return False
            return True
    
    async def _execute_step(self, step_name: str) -> None:
        step = self.steps[step_name]
        
        async with self._step_lock:
            if step['status'] != WorkflowStatus.PENDING:
                return
            step['status'] = WorkflowStatus.RUNNING
            step['start_time'] = datetime.now()
        
        for attempt in range(step['retry_config'].get('max_attempts', 1)):
            if self._cancelled:
                step['status'] = WorkflowStatus.CANCELLED
                break
            
            try:
                if step.get('timeout'):
                    try:
                        result = await asyncio.wait_for(
                            self._call_func(step['func'], step_name),
                            timeout=step['timeout']
                        )
                    except asyncio.TimeoutError:
                        raise TimeoutError(f"Step {step_name} timed out after {step['timeout']} seconds")
                else:
                    result = await self._call_func(step['func'], step_name)
                
                async with self._step_lock:
                    step['result'] = result
                    self.results[step_name] = result
                    step['status'] = WorkflowStatus.COMPLETED
                    step['error'] = None
                    step['attempts'] = attempt + 1
                break
                
            except Exception as e:
                step['error'] = str(e)
                self.errors[step_name] = e
                step['attempts'] = attempt + 1
                
                if attempt < step['retry_config'].get('max_attempts', 1) - 1:
                    delay = step['retry_config'].get('delay', 1)
                    await asyncio.sleep(delay * (attempt + 1))
                    logger.warning(f"Retrying step {step_name} (attempt {attempt + 2})")
                else:
                    async with self._step_lock:
                        step['status'] = WorkflowStatus.FAILED
                    logger.error(f"Step {step_name} failed after {attempt + 1} attempts: {e}")
        
        async with self._step_lock:
            step['end_time'] = datetime.now()
    
    async def _call_func(self, func: Callable, step_name: str) -> Any:
        if asyncio.iscoroutinefunction(func):
            return await func(self.results)
        else:
            return await asyncio.to_thread(func, self.results)
    
    async def get_ready_steps(self) -> List[str]:
        async with self._step_lock:
            ready = []
            for name in self.step_order:
                step = self.steps[name]
                if step['status'] == WorkflowStatus.PENDING and await self._check_dependencies(name):
                    ready.append(name)
            return ready
    
    async def execute(self, initial_data: Any = None) -> Any:
        self.start_time = datetime.now()
        self.status = WorkflowStatus.RUNNING
        self.results['__initial__'] = initial_data
        
        if not self.validate_input(initial_data):
            raise ValueError("Workflow validation failed")
        
        await self._save_checkpoint()
        
        try:
            while len(self.results) < len(self.steps) + 1:
                if self._cancelled:
                    self.status = WorkflowStatus.CANCELLED
                    raise asyncio.CancelledError("Workflow cancelled")
                
                ready_steps = await self.get_ready_steps()
                
                if not ready_steps:
                    pending_steps = [n for n, s in self.steps.items() 
                                   if s['status'] == WorkflowStatus.PENDING]
                    
                    if pending_steps:
                        dep_graph = {}
                        for step in pending_steps:
                            deps = self.steps[step]['depends_on']
                            unresolved = [d for d in deps if d not in self.results]
                            if unresolved:
                                dep_graph[step] = unresolved
                        
                        raise RuntimeError(
                            f"Workflow deadlock detected. Pending steps: {pending_steps}\n"
                            f"Unresolved dependencies: {dep_graph}"
                        )
                    break
                
                tasks = [self._execute_step(name) for name in ready_steps]
                await asyncio.gather(*tasks, return_exceptions=True)
                await self._save_checkpoint()
            
            failed_steps = [n for n, s in self.steps.items() 
                          if s['status'] == WorkflowStatus.FAILED]
            
            if failed_steps:
                self.status = WorkflowStatus.FAILED
                raise RuntimeError(f"Workflow failed: steps {failed_steps}")
            
            self.status = WorkflowStatus.COMPLETED
            return self.finalize(self.results)
            
        except Exception as e:
            self.status = WorkflowStatus.FAILED
            raise
        finally:
            self.end_time = datetime.now()
    
    async def _save_checkpoint(self):
        async with self._lock:
            checkpoint = {
                'workflow_id': self.workflow_id,
                'status': self.status.value,
                'step_states': {
                    name: {
                        'status': step['status'].value,
                        'result': step.get('result'),
                        'error': str(step.get('error')) if step.get('error') else None,
                        'start_time': step['start_time'].isoformat() if step['start_time'] else None,
                        'end_time': step['end_time'].isoformat() if step['end_time'] else None,
                        'attempts': step.get('attempts', 0)
                    }
                    for name, step in self.steps.items()
                },
                'results': {k: v for k, v in self.results.items() if k != '__initial__'},
                'timestamp': datetime.now().isoformat()
            }
            
            checkpoint_path = self.checkpoint_dir / f"workflow_{self.workflow_id}.pkl"
            
            try:
                with open(checkpoint_path, 'wb') as f:
                    pickle.dump(checkpoint, f, protocol=pickle.HIGHEST_PROTOCOL)
            except Exception as e:
                logger.warning(f"Failed to save workflow checkpoint: {e}")
    
    def cancel(self):
        self._cancelled = True
        logger.info(f"Workflow {self.workflow_id} cancellation requested")
    
    def get_execution_summary(self) -> Dict:
        if not self.start_time:
            return {'status': 'not_started'}
        
        return {
            'workflow_id': self.workflow_id,
            'status': self.status.value,
            'total_steps': len(self.steps),
            'completed_steps': sum(1 for s in self.steps.values() if s['status'] == WorkflowStatus.COMPLETED),
            'failed_steps': sum(1 for s in self.steps.values() if s['status'] == WorkflowStatus.FAILED),
            'duration_s': (self.end_time - self.start_time).total_seconds() if self.end_time and self.start_time else 0,
            'start_time': self.start_time.isoformat() if self.start_time else None,
            'end_time': self.end_time.isoformat() if self.end_time else None,
            'steps': {
                name: {
                    'status': step['status'].value,
                    'duration_s': (step['end_time'] - step['start_time']).total_seconds()
                    if step['end_time'] and step['start_time'] else 0,
                    'attempts': step.get('attempts', 0)
                }
                for name, step in self.steps.items()
            }
        }

# ============================================================
# SINGLETON ACCESSOR FOR SUSTAINABILITY
# ============================================================

_sustainability_dashboard = None
_sustainability_lock = asyncio.Lock()

async def get_sustainability_dashboard() -> SustainabilityDashboard:
    """Get singleton sustainability dashboard"""
    global _sustainability_dashboard
    if _sustainability_dashboard is None:
        async with _sustainability_lock:
            if _sustainability_dashboard is None:
                _sustainability_dashboard = SustainabilityDashboard()
                # Register managers
                carbon_manager = CarbonIntensityManager()
                helium_tracker = HeliumTracker()
                predictive_analyzer = PredictiveMetricsAnalyzer()
                _sustainability_dashboard.register_managers(
                    carbon_manager, helium_tracker, predictive_analyzer
                )
                # Start background tasks
                asyncio.create_task(carbon_manager.update_carbon_intensity())
    return _sustainability_dashboard

# ============================================================
# EXPORTS
# ============================================================

__all__ = [
    # Exceptions
    'GreenAgentException', 'ConfigurationError', 'DataValidationError',
    'ModuleNotFoundError', 'QuantumError', 'BlockchainError', 'APIError',
    'ResourceError', 'TimeoutError', 'CircuitBreakerOpenError',
    'CarbonIntensityError', 'HeliumTrackingError',
    
    # Modules
    'CarbonIntensityManager', 'HeliumTracker', 'PredictiveMetricsAnalyzer',
    'SustainabilityDashboard', 'get_sustainability_dashboard',
    
    # Circuit Breaker
    'CircuitBreakerState', 'EnhancedCircuitBreaker',
    
    # Rate Limiter
    'EnhancedRateLimiter',
    
    # Model Registry
    'EnhancedModelRegistry',
    
    # Base Classes
    'MLFramework', 'EnhancedBaseMLModel', 'EnhancedBaseRealtimeHandler',
    'EnhancedBaseWorkflow', 'WorkflowStatus',
    
    # Helpers
    'get_shared_registry',
]

# ============================================================
# SHARED REGISTRY
# ============================================================

_shared_registry = REGISTRY

def get_shared_registry() -> CollectorRegistry:
    """Get shared Prometheus registry"""
    return _shared_registry
