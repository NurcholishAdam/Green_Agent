"""
Enhanced Expert Metrics Collector v6.1.0 - Complete Green Agent Implementation

Complete bio-inspired integration with:
- Federated Reflexive Learning with distributed metrics aggregation
- User-Adaptive Reflexivity with dynamic configuration
- Real-time Carbon Intensity Integration with API support
- Cross-Domain Knowledge Transfer with correlation analysis
- Human-AI Collaborative Reflection with decision explanations
- Predictive Reflexivity with ensemble forecasting
- Sustainability Score with multi-metric aggregation
- Enhanced Carbon/Helium Awareness with real-time tracking
- Gradient field metrics (trust, carbon, helium as Prometheus metrics)
- Token economy observability (Eco-ATP balance, efficiency, consumption)
- Biomass storage metrics (total stored, tier distribution, collateral)
- Harvester vitality metrics (photosynthetic activity, excitation levels)
- Compartment health metrics (membrane permeability, population counts)
- Closed-loop feedback (metrics → gradient pumping → routing adaptation)
- Token-aware SLO tracking (Eco-ATP cost as SLO dimension)
- Gradient-modulated alerting (dynamic thresholds based on gradient state)
- Unified bio-inspired dashboard (all metabolic metrics in one view)
- Metabolic Pareto frontier (energy × tokens × time optimization)
- Machine learning-based anomaly detection
- Digital twin integration for scenario-based optimization
- Predictive SLO violation forecasting
- Interactive dashboard for real-time monitoring
- Differential privacy for federated metrics

New in v6.1.0:
- Configuration dataclass for centralized settings
- Resilient carbon manager with retry & circuit breaker
- Online learning for predictive analyzer (SGDRegressor)
- Model compression (top-k sparsification) for federated aggregator
- Incremental ML anomaly detection with adaptive thresholds
- Enhanced SLO forecasting with exponential smoothing
- Persistence for metrics, alerts, and SLO states
- Prometheus-style telemetry exporter
- Proper async/sync separation
- Stub for digital twin integration
- Configurable thresholds and SLOs
- Health status reporting
- Improved cross-domain transfer with effectiveness tracking
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple, Set, Callable, Union
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import numpy as np
from collections import defaultdict, deque
import time
import threading
import json
import hashlib
import math
import aiohttp
import os
import random
import pickle
import zlib
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import SGDRegressor

logger = logging.getLogger(__name__)

# ============================================================================
# Configuration Dataclass (NEW)
# ============================================================================

@dataclass
class ExpertMetricsConfig:
    """Centralized configuration for the Expert Metrics Collector."""
    # Feature flags
    enable_anomaly_detection: bool = True
    enable_slo_tracking: bool = True
    enable_cost_attribution: bool = True
    enable_alerting: bool = True
    enable_predictive: bool = True
    enable_bio_integration: bool = True
    enable_carbon_intensity: bool = True
    enable_federated: bool = True
    enable_cross_domain: bool = True
    enable_human_ai: bool = True
    enable_sustainability_scoring: bool = True
    enable_ml_anomaly_detection: bool = True
    enable_digital_twin_integration: bool = True
    enable_differential_privacy: bool = True
    enable_persistence: bool = True
    enable_telemetry: bool = True

    # Tunable parameters
    retention_hours: float = 24.0
    privacy_epsilon: float = 1.0
    federated_sparsity_ratio: float = 0.1  # top-k% of metrics to keep
    carbon_api_region: str = "us-east"
    carbon_update_interval: int = 300
    max_retries: int = 3
    retry_base_delay_ms: float = 100.0
    retry_max_delay_ms: float = 5000.0
    circuit_breaker_threshold: int = 5
    circuit_breaker_recovery_timeout: float = 30.0
    persistence_path: str = "metrics_state.pkl"
    telemetry_export_interval: int = 60
    token_exchange_rate: float = 1000.0  # Eco-ATP per kWh
    slo_config: Dict[str, Dict[str, Any]] = field(default_factory=lambda: {
        'latency_slo': {'metric_name': 'expert_latency_ms', 'target_value': 100.0, 'target_percentile': 99.0, 'evaluation_window_hours': 24.0},
        'availability_slo': {'metric_name': 'expert_success_rate', 'target_value': 0.999, 'target_percentile': 99.9, 'evaluation_window_hours': 24.0},
        'carbon_slo': {'metric_name': 'carbon_per_inference', 'target_value': 0.0005, 'target_percentile': 95.0, 'evaluation_window_hours': 24.0},
        'token_efficiency_slo': {'metric_name': 'token_efficiency', 'target_value': 0.8, 'target_percentile': 90.0, 'evaluation_window_hours': 24.0},
        'sustainability_slo': {'metric_name': 'sustainability_score', 'target_value': 0.7, 'target_percentile': 95.0, 'evaluation_window_hours': 24.0}
    })
    threshold_config: Dict[str, Dict[str, Any]] = field(default_factory=lambda: {
        'latency_p95': {'metric_name': 'latency_p95', 'warning_threshold': 100.0, 'critical_threshold': 500.0, 'comparison': 'greater_than', 'gradient_modulated': True, 'cooldown_seconds': 300.0},
        'error_rate': {'metric_name': 'error_rate', 'warning_threshold': 0.05, 'critical_threshold': 0.10, 'comparison': 'greater_than', 'gradient_modulated': True},
        'carbon_per_inference': {'metric_name': 'carbon_per_inference', 'warning_threshold': 0.0005, 'critical_threshold': 0.001, 'comparison': 'greater_than', 'gradient_modulated': True},
        'token_balance': {'metric_name': 'token_balance', 'warning_threshold': 200.0, 'critical_threshold': 50.0, 'comparison': 'less_than', 'gradient_modulated': True},
        'gradient_health': {'metric_name': 'gradient_health', 'warning_threshold': 0.3, 'critical_threshold': 0.1, 'comparison': 'less_than', 'gradient_modulated': True},
        'biomass_level': {'metric_name': 'biomass_level', 'warning_threshold': 8000.0, 'critical_threshold': 9500.0, 'comparison': 'greater_than', 'gradient_modulated': True},
        'sustainability_score': {'metric_name': 'sustainability_score', 'warning_threshold': 0.7, 'critical_threshold': 0.4, 'comparison': 'less_than', 'gradient_modulated': True}
    })

    def __post_init__(self):
        # Ensure boolean flags
        for key, value in self.__dict__.items():
            if isinstance(value, bool):
                setattr(self, key, bool(value))

# ============================================================================
# Bio-Inspired Module Imports (unchanged)
# ============================================================================

try:
    from enhancements.bio_inspired.eco_atp_currency import (
        EcoATPTokenManager, DynamicExchangeRate, EcoATPSource, EcoATPConsumer,
        TokenState, EcoATPToken, EcoATPAccount
    )
    from enhancements.bio_inspired.proton_gradient_fields import (
        GradientFieldManager, GradientField
    )
    from enhancements.bio_inspired.atp_synthase_scheduler import (
        ATPSynthaseScheduler, SynthaseConfig
    )
    from enhancements.bio_inspired.chromatophore_compartments import (
        CompartmentManager, ChromatophoreCompartment, CompartmentState,
        MembranePermeability
    )
    from enhancements.bio_inspired.biomass_storage import (
        BiomassStorage, StorageTier, GuaranteeLevel
    )
    from enhancements.bio_inspired.photosynthetic_harvester import (
        PhotosyntheticHarvester
    )
    BIO_INSPIRED_AVAILABLE = True
    logger.info("Bio-inspired modules loaded for Expert Metrics")
except ImportError as e:
    BIO_INSPIRED_AVAILABLE = False
    logger.warning(f"Bio-inspired modules not available: {str(e)} - using standard metrics")

# ============================================================================
# Enhanced Carbon Intensity Manager with Retry & Circuit Breaker
# ============================================================================

class CarbonIntensityManager:
    """Real-time carbon intensity integration with retry, circuit breaker, and caching."""

    def __init__(self, config: ExpertMetricsConfig):
        self.config = config
        self.endpoint = "https://api.electricitymap.org/v3/carbon-intensity"
        self.region = config.carbon_api_region
        self.carbon_intensity = 0.0
        self.last_update = None
        self._lock = asyncio.Lock()
        self._session = None
        self.cache = {}
        self.historical_intensities = deque(maxlen=1000)
        self.api_key = os.getenv('ELECTRICITYMAP_API_KEY', '')
        self.failure_count = 0
        self.circuit_open = False
        self.circuit_open_until: Optional[datetime] = None
        self.circuit_breaker_threshold = config.circuit_breaker_threshold
        self.max_retries = config.max_retries
        logger.info(f"CarbonIntensityManager initialized (region={self.region}, retries={self.max_retries})")

    async def _get_session(self):
        if self._session is None:
            self._session = aiohttp.ClientSession()
        return self._session

    async def update_carbon_intensity(self, region: Optional[str] = None) -> Dict:
        """Update carbon intensity with retry and circuit breaker."""
        if region is not None:
            self.region = region

        # Circuit breaker check
        if self.circuit_open:
            if datetime.utcnow() < self.circuit_open_until:
                logger.warning("Circuit breaker open, using fallback data")
                return self._get_fallback_response()
            else:
                self.circuit_open = False
                self.failure_count = 0
                logger.info("Circuit breaker reset for CarbonIntensityManager")

        # Cache check
        cache_key = f"{self.region}_{datetime.utcnow().hour}"
        if cache_key in self.cache and self.last_update and (datetime.utcnow() - self.last_update).seconds < self.config.carbon_update_interval:
            return self.cache[cache_key]

        for attempt in range(self.max_retries):
            try:
                session = await self._get_session()
                url = f"{self.endpoint}/latest?zone={self.region}"
                headers = {'auth-token': self.api_key} if self.api_key else {}
                async with session.get(url, headers=headers, timeout=10) as response:
                    if response.status == 200:
                        data = await response.json()
                        self.carbon_intensity = data.get('carbonIntensity', 400)
                        self.last_update = datetime.now()
                        self.cache[cache_key] = {
                            'intensity': self.carbon_intensity,
                            'timestamp': self.last_update.isoformat()
                        }
                        self.historical_intensities.append(self.carbon_intensity)
                        self.failure_count = 0
                        return {'intensity': self.carbon_intensity, 'region': self.region,
                                'timestamp': self.last_update.isoformat()}
                    else:
                        logger.warning(f"Carbon API returned {response.status}, attempt {attempt+1}")
                        if attempt == self.max_retries - 1:
                            self.failure_count += 1
                            if self.failure_count >= self.circuit_breaker_threshold:
                                self.circuit_open = True
                                self.circuit_open_until = datetime.utcnow() + timedelta(seconds=self.config.circuit_breaker_recovery_timeout)
                                logger.error("Circuit breaker opened for CarbonIntensityManager")
                            return self._get_fallback_response()
                        await asyncio.sleep(2 ** attempt)
            except Exception as e:
                logger.error(f"Carbon API error: {e}, attempt {attempt+1}")
                if attempt == self.max_retries - 1:
                    self.failure_count += 1
                    if self.failure_count >= self.circuit_breaker_threshold:
                        self.circuit_open = True
                        self.circuit_open_until = datetime.utcnow() + timedelta(seconds=self.config.circuit_breaker_recovery_timeout)
                    return self._get_fallback_response()
                await asyncio.sleep(2 ** attempt)

        # Should never reach here
        return self._get_fallback_response()

    def _get_fallback_response(self) -> Dict:
        fallback_intensities = {'us-east': 420, 'us-west': 350, 'eu': 280, 'asia': 500}
        intensity = fallback_intensities.get(self.region, 400)
        self.carbon_intensity = intensity
        self.last_update = datetime.now()
        return {'intensity': intensity, 'region': self.region,
                'timestamp': self.last_update.isoformat(), 'is_fallback': True}

    async def get_current_intensity(self) -> float:
        if self.last_update is None or (datetime.utcnow() - self.last_update).seconds > self.config.carbon_update_interval:
            await self.update_carbon_intensity(self.region)
        return self.carbon_intensity

    async def close(self):
        if self._session:
            await self._session.close()

# ============================================================================
# Predictive Metrics Analyzer with Online Learning (Enhanced)
# ============================================================================

class PredictiveMetricsAnalyzer:
    """Predictive reflexivity with online learning (SGD) and SLO violation prediction."""

    def __init__(self, config: ExpertMetricsConfig, history_window: int = 100):
        self.config = config
        self.history_window = history_window
        self.metric_history = deque(maxlen=history_window)
        self.forecast_history = deque(maxlen=50)
        self.scaler = StandardScaler()
        self.model = None
        self.is_trained = False
        self.violation_model = None
        self.slo_violation_history = deque(maxlen=1000)
        self._ml_available = False
        self._init_models()

    def _init_models(self):
        try:
            self.model = SGDRegressor(
                learning_rate='constant',
                eta0=0.01,
                penalty='l2',
                alpha=0.0001,
                max_iter=1,
                random_state=42,
                warm_start=True
            )
            self.violation_model = SGDRegressor(
                learning_rate='constant',
                eta0=0.01,
                penalty='l2',
                alpha=0.0001,
                max_iter=1,
                random_state=42,
                warm_start=True
            )
            self._ml_available = True
        except ImportError:
            logger.warning("SGDRegressor not available; using fallback moving average")

    def update_history(self, metric_data: Dict):
        self.metric_history.append({
            'timestamp': datetime.utcnow(),
            'success_rate': metric_data.get('success_rate', 0.8),
            'avg_latency_ms': metric_data.get('avg_latency_ms', 100),
            'carbon_intensity': metric_data.get('carbon_intensity', 400),
            'token_efficiency': metric_data.get('token_efficiency', 0.5),
            'health_score': metric_data.get('health_score', 0.5),
            'slo_compliant': metric_data.get('slo_compliant', 1.0)
        })

    async def train_forecast_model(self):
        """Train or update the model incrementally."""
        if not self._ml_available:
            return {'status': 'ml_not_available'}
        if len(self.metric_history) < 10:
            return {'status': 'insufficient_data'}

        # Prepare features
        X, y = [], []
        history_list = list(self.metric_history)
        for i in range(len(history_list) - 5):
            features = []
            for j in range(5):
                data = history_list[i + j]
                features.extend([data['success_rate'], data['avg_latency_ms'] / 1000,
                                 data['carbon_intensity'] / 100, data['token_efficiency'],
                                 data['health_score'], data.get('slo_compliant', 1.0)])
            X.append(features)
            y.append(history_list[i + 5]['health_score'])

        X = np.array(X); y = np.array(y)
        if self.scaler.mean_ is None:
            X_scaled = self.scaler.fit_transform(X)
        else:
            X_scaled = self.scaler.transform(X)

        # Incremental training
        for _ in range(3):
            self.model.partial_fit(X_scaled, y)
        self.is_trained = True

        # Compute R2 for diagnostics
        pred = self.model.predict(X_scaled)
        r2 = r2_score(y, pred) if len(y) > 5 else 0.0
        return {'status': 'success', 'r2': r2, 'samples': len(X)}

    async def predict_slo_violation(self, features: Dict[str, float]) -> float:
        """Predict probability of SLO violation using online model."""
        if not self._ml_available or self.violation_model is None:
            return 0.5
        try:
            X = np.array([[
                features.get('success_rate', 0.8),
                features.get('avg_latency_ms', 100) / 1000,
                features.get('carbon_intensity', 400) / 100,
                features.get('token_efficiency', 0.5),
                features.get('health_score', 0.5)
            ]])
            if self.scaler.mean_ is not None:
                X_scaled = self.scaler.transform(X)
            else:
                X_scaled = X
            # Predict violation probability (regression to 0/1)
            prob = self.violation_model.predict(X_scaled)[0]
            prob = max(0.0, min(1.0, prob))
            self.slo_violation_history.append({
                'timestamp': datetime.utcnow().isoformat(),
                'probability': prob,
                'features': features
            })
            return prob
        except Exception as e:
            logger.warning(f"SLO violation prediction failed: {e}")
            return 0.5

    def _generate_actions(self, prediction: float) -> List[str]:
        actions = []
        if prediction < 0.4:
            actions.append("Increase token allocation for critical experts")
            actions.append("Optimize carbon-aware scheduling")
        elif prediction < 0.6:
            actions.append("Enhance gradient health monitoring")
            actions.append("Improve compartment health")
        return actions or ["Metric trends are on track"]

# ============================================================================
# Enhanced Cross-Domain Transfer with Effectiveness Tracking
# ============================================================================

class MetricsCrossDomainTransfer:
    """Cross-domain knowledge transfer with effectiveness tracking and pruning."""

    def __init__(self):
        self.knowledge_base: Dict[str, Dict[str, Dict]] = {}
        self.transfer_logs = deque(maxlen=1000)
        self.domain_mappings = {
            'metrics→energy': {'efficiency_strategies': ['token-based', 'gradient-driven'],
                               'resource_allocation': ['dynamic', 'adaptive']},
            'metrics→carbon': {'optimization_strategies': ['load-shifting', 'efficiency-first']},
            'metrics→helium': {'scarcity_strategies': ['efficiency-first', 'conservation']}
        }

    def transfer_knowledge(self, source_domain: str, target_domain: str,
                          knowledge_type: str, data: Dict[str, Any]) -> Dict:
        key = f"{source_domain}→{target_domain}"
        if key not in self.knowledge_base:
            self.knowledge_base[key] = {}
        if knowledge_type not in self.knowledge_base[key]:
            self.knowledge_base[key][knowledge_type] = {'data': data, 'transfer_count': 1,
                'effectiveness_score': 0.5, 'last_used': datetime.utcnow()}
        else:
            existing = self.knowledge_base[key][knowledge_type]
            existing['data'].update(data); existing['transfer_count'] += 1
            existing['last_used'] = datetime.utcnow()
        self.transfer_logs.append({'timestamp': datetime.utcnow(), 'source': source_domain,
                                   'target': target_domain, 'type': knowledge_type,
                                   'effectiveness': self.knowledge_base[key][knowledge_type]['effectiveness_score']})
        # Prune stale knowledge
        self._prune_stale()
        return self.knowledge_base[key][knowledge_type]

    def update_effectiveness(self, source_domain: str, target_domain: str,
                            knowledge_type: str, effectiveness: float):
        key = f"{source_domain}→{target_domain}"
        if key in self.knowledge_base and knowledge_type in self.knowledge_base[key]:
            entry = self.knowledge_base[key][knowledge_type]
            old = entry['effectiveness_score']
            count = entry['transfer_count']
            entry['effectiveness_score'] = (old * count + effectiveness) / (count + 1)

    def _prune_stale(self, max_age_days: int = 7):
        now = datetime.utcnow()
        for key, domain_data in list(self.knowledge_base.items()):
            for ktype, entry in list(domain_data.items()):
                age = (now - entry['last_used']).days
                if age > max_age_days:
                    del self.knowledge_base[key][ktype]
            if not self.knowledge_base[key]:
                del self.knowledge_base[key]

    def get_transfer_statistics(self) -> Dict:
        total_transfers = len(self.transfer_logs)
        domain_pairs = {}
        for log in self.transfer_logs:
            key = f"{log['source']}→{log['target']}"
            domain_pairs[key] = domain_pairs.get(key, 0) + 1
        avg_effectiveness = np.mean([l.get('effectiveness', 0.5) for l in self.transfer_logs[-50:]]) if self.transfer_logs else 0.5
        return {'total_transfers': total_transfers, 'domain_pairs': domain_pairs,
                'knowledge_types': list(self.knowledge_base.keys()),
                'average_effectiveness': avg_effectiveness}

# ============================================================================
# Enhanced Federated Metrics Aggregator with Compression & Retry
# ============================================================================

class FederatedMetricsAggregator:
    """Federated metrics aggregation with differential privacy and model compression."""

    def __init__(self, config: ExpertMetricsConfig):
        self.config = config
        self.server_url = None  # To be set later
        self.round = 0
        self.local_metrics = {}
        self.global_metrics = {}
        self.participants = []
        self.contribution_scores = {}
        self._lock = asyncio.Lock()
        self._session = None
        self.privacy_epsilon = config.privacy_epsilon
        self.noise_scale = 0.001
        self.sparsity_ratio = config.federated_sparsity_ratio
        self.failure_count = 0
        self.circuit_open = False
        self.circuit_open_until: Optional[datetime] = None

    async def _get_session(self):
        if self._session is None and self.server_url:
            self._session = aiohttp.ClientSession()
        return self._session

    def _add_differential_privacy(self, metrics: Dict) -> Dict:
        if self.privacy_epsilon <= 0:
            return metrics
        private = {}
        sensitivity = 1.0
        for key, value in metrics.items():
            if isinstance(value, (int, float)):
                scale = (2 * sensitivity) / self.privacy_epsilon
                noise = np.random.normal(0, scale * self.noise_scale)
                private[key] = value + noise
            else:
                private[key] = value
        return private

    def _compress_metrics(self, metrics: Dict) -> Dict:
        """Keep only top-k% of metric values (by absolute magnitude)."""
        if not self.config.enable_federated or self.sparsity_ratio == 1.0:
            return metrics
        # For simplicity, we compress only numeric metrics
        numeric_metrics = {k: v for k, v in metrics.items() if isinstance(v, (int, float))}
        if not numeric_metrics:
            return metrics
        # Sort by absolute value and keep top-k%
        sorted_items = sorted(numeric_metrics.items(), key=lambda x: abs(x[1]), reverse=True)
        k = max(1, int(len(sorted_items) * self.sparsity_ratio))
        kept_keys = {item[0] for item in sorted_items[:k]}
        compressed = {k: v for k, v in metrics.items() if k in kept_keys or not isinstance(v, (int, float))}
        return compressed

    async def send_local_metrics(self, participant_id: str, metrics: Dict, performance: float = 1.0) -> Dict:
        if not self.server_url:
            return {'status': 'local'}

        # Circuit breaker check
        if self.circuit_open:
            if datetime.utcnow() < self.circuit_open_until:
                logger.warning("Circuit breaker open, skipping send")
                return {'status': 'circuit_open'}
            else:
                self.circuit_open = False
                self.failure_count = 0

        for attempt in range(self.config.max_retries):
            try:
                async with self._lock:
                    session = await self._get_session()
                    private = self._add_differential_privacy(metrics)
                    compressed = self._compress_metrics(private)
                    update_data = {
                        'participant_id': participant_id,
                        'round': self.round,
                        'metrics': compressed,
                        'performance': performance,
                        'privacy_epsilon': self.privacy_epsilon,
                        'sparsity_ratio': self.sparsity_ratio,
                        'timestamp': datetime.utcnow().isoformat()
                    }
                    async with session.post(
                        f"{self.server_url}/federated/metrics",
                        json=update_data,
                        timeout=30
                    ) as response:
                        if response.status == 200:
                            result = await response.json()
                            self.round += 1
                            self.contribution_scores[participant_id] = performance
                            self.failure_count = 0
                            return result
                        else:
                            logger.warning(f"Federated metrics send failed (attempt {attempt+1}): {response.status}")
            except Exception as e:
                logger.error(f"Federated metrics send error (attempt {attempt+1}): {e}")
            await asyncio.sleep(2 ** attempt)

        self.failure_count += 1
        if self.failure_count >= self.config.circuit_breaker_threshold:
            self.circuit_open = True
            self.circuit_open_until = datetime.utcnow() + timedelta(seconds=self.config.circuit_breaker_recovery_timeout)
            logger.error("Circuit breaker opened for FederatedMetricsAggregator")
        return {'status': 'failed'}

    async def get_global_metrics(self) -> Optional[Dict]:
        if not self.server_url:
            return self.global_metrics
        for attempt in range(self.config.max_retries):
            try:
                async with self._lock:
                    session = await self._get_session()
                    async with session.get(
                        f"{self.server_url}/federated/metrics/global",
                        timeout=30
                    ) as response:
                        if response.status == 200:
                            data = await response.json()
                            self.global_metrics = data.get('metrics', {})
                            self.participants = data.get('participants', [])
                            return self.global_metrics
                        else:
                            logger.warning(f"Global metrics fetch failed (attempt {attempt+1}): {response.status}")
            except Exception as e:
                logger.error(f"Global metrics fetch error (attempt {attempt+1}): {e}")
            await asyncio.sleep(2 ** attempt)
        return None

    def aggregate_metrics(self, peer_metrics: List[Dict], weights: Dict[str, float] = None) -> Dict:
        if not peer_metrics:
            return {}
        aggregated = {}
        if weights is None:
            weights = {i: 1.0 for i in range(len(peer_metrics))}
        for metric_key in peer_metrics[0].keys():
            if isinstance(peer_metrics[0][metric_key], (int, float)):
                total = 0.0
                total_weight = 0.0
                for i, peer in enumerate(peer_metrics):
                    if metric_key in peer:
                        total += peer[metric_key] * weights.get(i, 1.0)
                        total_weight += weights.get(i, 1.0)
                aggregated[metric_key] = total / max(total_weight, 0.001)
            else:
                values = [peer.get(metric_key) for peer in peer_metrics if metric_key in peer]
                if values:
                    aggregated[metric_key] = max(set(values), key=values.count)
        return aggregated

    def get_federated_stats(self) -> Dict:
        return {
            'round': self.round,
            'participants': len(self.participants),
            'has_global_metrics': bool(self.global_metrics),
            'contribution_scores': self.contribution_scores,
            'privacy_epsilon': self.privacy_epsilon,
            'sparsity_ratio': self.sparsity_ratio,
            'circuit_open': self.circuit_open
        }

    async def close(self):
        if self._session:
            await self._session.close()

# ============================================================================
# Enhanced Human-AI Collaborative Support (with async methods)
# ============================================================================

class HumanAICollaborativeSupport:
    """Human-AI collaborative reflection with async dashboard methods."""

    def __init__(self):
        self.decision_history = deque(maxlen=1000)
        self.explanation_cache = {}
        self.feedback_history = deque(maxlen=500)
        self._lock = asyncio.Lock()
        self.dashboard_data = {
            'metrics': deque(maxlen=1000),
            'alerts': deque(maxlen=1000),
            'insights': deque(maxlen=1000)
        }

    def generate_explanation(self, expert_id: str, metrics: Dict[str, Any],
                            anomalies: List[Any]) -> Dict[str, Any]:
        explanation = {
            'expert_id': expert_id,
            'timestamp': datetime.utcnow().isoformat(),
            'summary': '',
            'details': [],
            'anomalies': [],
            'recommendations': []
        }
        success_rate = metrics.get('success_rate', 0.5)
        if success_rate > 0.9:
            summary = f"Expert {expert_id} is performing excellently with {success_rate:.1%} success rate."
        elif success_rate > 0.7:
            summary = f"Expert {expert_id} is performing well with {success_rate:.1%} success rate."
        elif success_rate > 0.5:
            summary = f"Expert {expert_id} has moderate performance with {success_rate:.1%} success rate."
        else:
            summary = f"Expert {expert_id} performance needs attention with {success_rate:.1%} success rate."
        explanation['summary'] = summary

        latency = metrics.get('avg_latency_ms', 0)
        if latency > 100:
            explanation['details'].append(f"High latency detected: {latency:.1f}ms (threshold: 100ms)")
            explanation['recommendations'].append("Consider scaling resources or optimizing execution path")

        carbon = metrics.get('carbon_per_inference', 0)
        if carbon > 0.001:
            explanation['details'].append(f"High carbon footprint: {carbon:.6f} kg CO2 per inference")
            explanation['recommendations'].append("Optimize for carbon efficiency or use renewable energy")

        token_cost = metrics.get('token_cost', 0)
        if token_cost > 50:
            explanation['details'].append(f"High Eco-ATP cost: {token_cost:.1f} tokens per execution")
            explanation['recommendations'].append("Consider token-efficient alternatives or batching")

        for anomaly in anomalies:
            explanation['anomalies'].append({
                'type': anomaly.anomaly_type.value if hasattr(anomaly, 'anomaly_type') else 'unknown',
                'severity': anomaly.severity.value if hasattr(anomaly, 'severity') else 'info',
                'description': f"Anomaly detected: expected {anomaly.expected_value:.2f}, actual {anomaly.actual_value:.2f}"
            })

        if metrics.get('sustainability_score', 0.5) < 0.6:
            explanation['recommendations'].append("Improve sustainability score through optimization")

        self.explanation_cache[f"{expert_id}_{datetime.utcnow().timestamp()}"] = explanation
        self.dashboard_data['insights'].append({
            'timestamp': datetime.utcnow().isoformat(),
            'expert_id': expert_id,
            'summary': summary,
            'recommendations': explanation['recommendations']
        })
        return explanation

    def process_feedback(self, expert_id: str, feedback: Dict) -> Dict:
        feedback_entry = {'expert_id': expert_id, 'timestamp': datetime.utcnow().isoformat(), 'feedback': feedback}
        self.feedback_history.append(feedback_entry)
        reflection = {
            'acknowledgment': f"Feedback received for expert {expert_id}",
            'actions': [],
            'insights': []
        }
        if feedback.get('concern') == 'performance':
            reflection['actions'].append("Review performance metrics and optimize execution")
            reflection['insights'].append("Performance optimization may require resource scaling")
        if feedback.get('concern') == 'sustainability':
            reflection['actions'].append("Implement sustainability improvements")
            reflection['insights'].append("Focus on carbon and helium efficiency")
        return reflection

    def get_decision_insights(self, expert_id: str, hours: int = 24) -> Dict:
        recent = [exp for exp in self.explanation_cache.values()
                  if exp.get('expert_id') == expert_id and
                  datetime.fromisoformat(exp['timestamp']) > datetime.utcnow() - timedelta(hours=hours)]
        return {
            'expert_id': expert_id,
            'total_decisions': len(recent),
            'recent_summary': recent[-1].get('summary', 'No recent decisions') if recent else 'No decisions',
            'feedback_count': len([f for f in self.feedback_history if f.get('expert_id') == expert_id]),
            'recommendations': [r for exp in recent for r in exp.get('recommendations', [])][:5]
        }

    async def get_dashboard_data(self) -> Dict[str, Any]:
        async with self._lock:
            return {
                'recent_insights': list(self.dashboard_data['insights'])[-10:],
                'recent_alerts': list(self.dashboard_data['alerts'])[-10:],
                'recent_metrics': list(self.dashboard_data['metrics'])[-10:],
                'feedback_count': len(self.feedback_history),
                'explanation_count': len(self.explanation_cache)
            }

# ============================================================================
# Enums and Data Classes (unchanged, but we include them for completeness)
# ============================================================================

class MetricSeverity(Enum):
    INFO = "info"; WARNING = "warning"; CRITICAL = "critical"; EMERGENCY = "emergency"

class MetricType(Enum):
    COUNTER = "counter"; GAUGE = "gauge"; HISTOGRAM = "histogram"; SUMMARY = "summary"
    TREND = "trend"; GRADIENT = "gradient"; TOKEN = "token"; BIOMASS = "biomass"
    HARVESTER = "harvester"; COMPARTMENT = "compartment"

class AnomalyType(Enum):
    SPIKE = "spike"; DIP = "dip"; TREND_CHANGE = "trend_change"; LEVEL_SHIFT = "level_shift"
    VARIANCE_CHANGE = "variance_change"; OUTLIER = "outlier"; GRADIENT_ANOMALY = "gradient_anomaly"
    TOKEN_EXHAUSTION = "token_exhaustion"; BIOMASS_OVERFLOW = "biomass_overflow"; ML_DETECTED = "ml_detected"

class SLOStatus(Enum):
    COMPLIANT = "compliant"; AT_RISK = "at_risk"; BREACHED = "breached"; UNKNOWN = "unknown"

@dataclass
class MetricThreshold:
    metric_name: str
    warning_threshold: float
    critical_threshold: float
    comparison: str = "greater_than"
    duration_seconds: float = 60.0
    cooldown_seconds: float = 300.0
    gradient_modulated: bool = False
    sustainability_impact: float = 0.0

    def is_exceeded(self, value: float, gradient_modulation: float = 1.0) -> Tuple[bool, MetricSeverity]:
        effective_warning = self.warning_threshold * gradient_modulation
        effective_critical = self.critical_threshold * gradient_modulation
        if self.comparison == "greater_than":
            if value >= effective_critical:
                return True, MetricSeverity.CRITICAL
            elif value >= effective_warning:
                return True, MetricSeverity.WARNING
        elif self.comparison == "less_than":
            if value <= effective_critical:
                return True, MetricSeverity.CRITICAL
            elif value <= effective_warning:
                return True, MetricSeverity.WARNING
        return False, MetricSeverity.INFO

@dataclass
class ServiceLevelObjective:
    slo_id: str
    metric_name: str
    target_value: float
    target_percentile: float = 99.0
    evaluation_window_hours: float = 24.0
    min_samples: int = 100
    current_value: float = 0.0
    status: SLOStatus = SLOStatus.UNKNOWN
    error_budget_remaining: float = 1.0
    last_evaluated: datetime = field(default_factory=datetime.utcnow)
    token_cost_per_violation: float = 10.0
    sustainability_weight: float = 0.5
    predicted_violation_probability: float = 0.0
    next_predicted_violation: Optional[datetime] = None

@dataclass
class AnomalyEvent:
    event_id: str
    metric_name: str
    anomaly_type: AnomalyType
    detected_at: datetime
    expected_value: float
    actual_value: float
    deviation_std: float
    severity: MetricSeverity
    expert_id: Optional[str] = None
    details: Dict[str, Any] = field(default_factory=dict)
    gradient_level: float = 0.5
    sustainability_impact: float = 0.0
    ml_confidence: float = 0.0

@dataclass
class MetricSample:
    timestamp: datetime
    value: float
    labels: Dict[str, str] = field(default_factory=dict)
    correlation_id: Optional[str] = None
    trace_id: Optional[str] = None
    expert_id: Optional[str] = None
    token_cost: float = 0.0
    gradient_level: float = 0.5
    sustainability_score: float = 0.0

@dataclass
class CostAttribution:
    expert_id: str
    time_period: str
    total_carbon_kg: float = 0.0
    total_helium_units: float = 0.0
    total_energy_kwh: float = 0.0
    total_ecoatp_cost: float = 0.0
    cost_per_operation: float = 0.0
    carbon_efficiency_score: float = 0.0
    helium_efficiency_score: float = 0.0
    token_efficiency_score: float = 0.0
    trend: str = "stable"
    sustainability_score: float = 0.0

@dataclass
class PredictiveMetricForecast:
    timestamp: datetime = field(default_factory=datetime.utcnow)
    predicted_health: float = 0.0
    predicted_success_rate: float = 0.0
    predicted_latency_ms: float = 0.0
    confidence: float = 0.0
    trend: str = "stable"
    recommended_actions: List[str] = field(default_factory=list)
    slo_violation_probability: float = 0.0
    predicted_violation_time: Optional[datetime] = None

# ============================================================================
# ML Anomaly Detector with Incremental Learning (Enhanced)
# ============================================================================

class MLAnomalyDetector:
    """
    Machine learning-based anomaly detection with incremental learning.
    Uses a rolling window and periodic retraining.
    """

    def __init__(self, contamination: float = 0.1, n_estimators: int = 100, window_size: int = 100):
        self.model = IsolationForest(contamination=contamination, n_estimators=n_estimators, random_state=42)
        self.scaler = StandardScaler()
        self.is_trained = False
        self.training_window: List[List[float]] = []
        self.window_size = window_size
        self._lock = asyncio.Lock()
        logger.info("ML Anomaly Detector initialized with incremental retraining")

    async def add_sample(self, metrics: Dict[str, float]):
        """Add a new sample and retrain if window is full."""
        feature_vector = [
            metrics.get('success_rate', 0.5),
            metrics.get('latency_ms', 100) / 1000,
            metrics.get('carbon_per_inference', 0.001) * 1000,
            metrics.get('helium_per_inference', 0.01),
            metrics.get('token_efficiency', 0.5),
            metrics.get('health_score', 0.5),
            metrics.get('gradient_level', 0.5)
        ]
        async with self._lock:
            self.training_window.append(feature_vector)
            if len(self.training_window) >= self.window_size:
                await self._retrain()

    async def _retrain(self):
        """Retrain the model on the current window."""
        if len(self.training_window) < 10:
            return
        X = np.array(self.training_window)
        X_scaled = self.scaler.fit_transform(X)
        self.model.fit(X_scaled)
        self.is_trained = True
        # Keep only the most recent window (for memory)
        self.training_window = self.training_window[-self.window_size:]
        logger.debug(f"ML Anomaly Detector retrained on {len(self.training_window)} samples")

    async def detect_anomaly(self, metrics: Dict[str, float]) -> Tuple[bool, float, str]:
        if not self.is_trained:
            return False, 0.0, "Model not trained"

        feature_vector = [
            metrics.get('success_rate', 0.5),
            metrics.get('latency_ms', 100) / 1000,
            metrics.get('carbon_per_inference', 0.001) * 1000,
            metrics.get('helium_per_inference', 0.01),
            metrics.get('token_efficiency', 0.5),
            metrics.get('health_score', 0.5),
            metrics.get('gradient_level', 0.5)
        ]
        X = np.array([feature_vector])
        X_scaled = self.scaler.transform(X)
        prediction = self.model.predict(X_scaled)[0]
        is_anomaly = prediction == -1
        decision = self.model.decision_function(X_scaled)[0]
        confidence = abs(decision) / (abs(decision) + 1)
        description = "ML-detected anomaly"
        if is_anomaly:
            if decision < -0.5:
                description = "Severe anomaly detected (high deviation)"
            elif decision < -0.2:
                description = "Moderate anomaly detected"
            else:
                description = "Slight anomaly detected"
        return is_anomaly, confidence, description

# ============================================================================
# Enhanced SLOTracker with Exponential Smoothing Forecasting
# ============================================================================

class SLOTracker:
    """SLO tracking with exponential smoothing for violation forecasting."""

    def __init__(self, config: ExpertMetricsConfig):
        self.config = config
        self.slos: Dict[str, ServiceLevelObjective] = {}
        self.metric_samples: Dict[str, List[float]] = defaultdict(list)
        self.violation_history: Dict[str, List[datetime]] = defaultdict(list)
        self._lock = asyncio.Lock()
        # Holt-Winters parameters (alpha, beta, gamma) - we use simple exponential smoothing for now
        self.alpha = 0.3  # smoothing factor
        self.last_value: Dict[str, float] = {}
        self.trend: Dict[str, float] = {}
        logger.info("SLOTracker initialized with exponential smoothing")

    def define_slo(self, slo_id: str, metric_name: str, target_value: float,
                   target_percentile: float = 99.0, evaluation_window_hours: float = 24.0) -> bool:
        if slo_id in self.slos:
            return False
        self.slos[slo_id] = ServiceLevelObjective(
            slo_id=slo_id, metric_name=metric_name, target_value=target_value,
            target_percentile=target_percentile, evaluation_window_hours=evaluation_window_hours
        )
        logger.info(f"SLO defined: {slo_id} (target: {target_value})")
        return True

    def record_metric(self, slo_id: str, value: float):
        if slo_id not in self.slos:
            return
        self.metric_samples[slo_id].append(value)
        if len(self.metric_samples[slo_id]) > 10000:
            self.metric_samples[slo_id] = self.metric_samples[slo_id][-10000:]

    async def evaluate_slos(self) -> Dict[str, Dict[str, Any]]:
        async with self._lock:
            results = {}
            for slo_id, slo in self.slos.items():
                samples = self.metric_samples.get(slo_id, [])
                if len(samples) < slo.min_samples:
                    results[slo_id] = {'status': 'insufficient_data', 'samples': len(samples)}
                    continue

                current = np.percentile(samples, slo.target_percentile)
                slo.current_value = current

                if current <= slo.target_value:
                    status = SLOStatus.COMPLIANT
                elif current <= slo.target_value * 1.2:
                    status = SLOStatus.AT_RISK
                else:
                    status = SLOStatus.BREACHED
                slo.status = status

                # Exponential smoothing forecast
                if slo_id not in self.last_value:
                    self.last_value[slo_id] = current
                    self.trend[slo_id] = 0.0
                else:
                    prev = self.last_value[slo_id]
                    # Update smoothed value
                    self.last_value[slo_id] = self.alpha * current + (1 - self.alpha) * prev
                    # Trend (using double exponential)
                    if len(samples) > 5:
                        # simple trend = recent slope
                        recent = samples[-5:]
                        x = np.arange(len(recent))
                        slope = np.polyfit(x, recent, 1)[0]
                        self.trend[slo_id] = 0.5 * slope + 0.5 * self.trend.get(slo_id, 0.0)

                # Predict next value
                forecast = self.last_value[slo_id] + self.trend.get(slo_id, 0.0) * 1  # 1 step ahead
                if forecast > slo.target_value * 1.2:
                    violation_prob = 0.8
                elif forecast > slo.target_value * 1.05:
                    violation_prob = 0.4
                else:
                    violation_prob = 0.1
                slo.predicted_violation_probability = violation_prob

                # Estimate time to violation
                if violation_prob > 0.3:
                    # Use trend to estimate time
                    if self.trend.get(slo_id, 0.0) > 0:
                        time_to_breach = (slo.target_value * 1.05 - current) / (self.trend.get(slo_id, 0.0) * 10)  # rough seconds
                        slo.next_predicted_violation = datetime.utcnow() + timedelta(seconds=max(30, min(3600, time_to_breach)))
                    else:
                        slo.next_predicted_violation = datetime.utcnow() + timedelta(hours=1)
                else:
                    slo.next_predicted_violation = None

                if status == SLOStatus.BREACHED:
                    self.violation_history[slo_id].append(datetime.utcnow())

                results[slo_id] = {
                    'status': status.value,
                    'current_value': current,
                    'target_value': slo.target_value,
                    'violation_probability': violation_prob,
                    'next_predicted_violation': slo.next_predicted_violation.isoformat() if slo.next_predicted_violation else None,
                    'samples': len(samples),
                    'violations': len(self.violation_history.get(slo_id, []))
                }
            return results

# ============================================================================
# Cost Attribution Engine (unchanged, but we keep it)
# ============================================================================

class CostAttributionEngine:
    def __init__(self):
        self.costs: Dict[str, CostAttribution] = {}
        self._lock = asyncio.Lock()

    def record_cost(self, expert_id: str, carbon_kg: float, helium_units: float, energy_kwh: float):
        if expert_id not in self.costs:
            self.costs[expert_id] = CostAttribution(
                expert_id=expert_id, time_period=datetime.utcnow().isoformat()
            )
        cost = self.costs[expert_id]
        cost.total_carbon_kg += carbon_kg
        cost.total_helium_units += helium_units
        cost.total_energy_kwh += energy_kwh

    def get_cost_attribution(self, expert_id: str) -> Optional[CostAttribution]:
        return self.costs.get(expert_id)

    def get_all_costs(self) -> Dict[str, CostAttribution]:
        return self.costs.copy()

# ============================================================================
# Telemetry Exporter (NEW)
# ============================================================================

class TelemetryExporter:
    """Exports metrics in Prometheus format."""

    def __init__(self):
        self.metrics: Dict[str, Any] = defaultdict(lambda: defaultdict(int))
        self._lock = asyncio.Lock()

    def increment(self, metric_name: str, tags: Optional[Dict[str, str]] = None, value: float = 1.0):
        key = self._make_key(metric_name, tags)
        self.metrics['counters'][key] += value

    def gauge(self, metric_name: str, value: float, tags: Optional[Dict[str, str]] = None):
        key = self._make_key(metric_name, tags)
        self.metrics['gauges'][key] = value

    def histogram(self, metric_name: str, value: float, tags: Optional[Dict[str, str]] = None):
        key = self._make_key(metric_name, tags)
        if key not in self.metrics['histograms']:
            self.metrics['histograms'][key] = []
        self.metrics['histograms'][key].append(value)
        if len(self.metrics['histograms'][key]) > 1000:
            self.metrics['histograms'][key] = self.metrics['histograms'][key][-1000:]

    def _make_key(self, metric_name: str, tags: Optional[Dict[str, str]]) -> str:
        if tags:
            tag_str = ','.join(f"{k}={v}" for k, v in sorted(tags.items()))
            return f"{metric_name}{{{tag_str}}}"
        return metric_name

    async def export(self) -> str:
        # Prometheus text format
        output = []
        for key, value in self.metrics['counters'].items():
            output.append(f"# TYPE {key} counter\n{key} {value}")
        for key, value in self.metrics['gauges'].items():
            output.append(f"# TYPE {key} gauge\n{key} {value}")
        for key, values in self.metrics['histograms'].items():
            output.append(f"# TYPE {key} histogram\n{key}_count {len(values)}\n{key}_sum {sum(values)}")
        return "\n".join(output)

    def reset(self):
        self.metrics.clear()
        self.metrics['counters'] = defaultdict(int)
        self.metrics['gauges'] = {}
        self.metrics['histograms'] = defaultdict(list)

# ============================================================================
# Persistence Manager (NEW)
# ============================================================================

class MetricsPersistenceManager:
    """Manages persistence of metrics, alerts, and SLO states."""

    def __init__(self, config: ExpertMetricsConfig):
        self.config = config
        self.path = config.persistence_path
        self._lock = asyncio.Lock()
        logger.info(f"MetricsPersistenceManager initialized (path={self.path})")

    async def save_state(self, state: Dict[str, Any]) -> bool:
        async with self._lock:
            try:
                serialized = pickle.dumps(state)
                compressed = zlib.compress(serialized)
                with open(self.path, 'wb') as f:
                    f.write(compressed)
                logger.info(f"Metrics state saved to {self.path}")
                return True
            except Exception as e:
                logger.error(f"Failed to save metrics state: {e}")
                return False

    async def load_state(self) -> Optional[Dict]:
        async with self._lock:
            if not os.path.exists(self.path):
                logger.warning(f"Persistence file {self.path} not found")
                return None
            try:
                with open(self.path, 'rb') as f:
                    compressed = f.read()
                serialized = zlib.decompress(compressed)
                state = pickle.loads(serialized)
                logger.info(f"Metrics state loaded from {self.path}")
                return state
            except Exception as e:
                logger.error(f"Failed to load metrics state: {e}")
                return None

    async def delete_state(self):
        async with self._lock:
            if os.path.exists(self.path):
                os.remove(self.path)
                logger.info(f"Persistence file {self.path} deleted")
                return True
            return False

# ============================================================================
# Enhanced Expert Metrics Collector (Main Class)
# ============================================================================

class ExpertMetricsCollector:
    """
    Enhanced Expert Metrics Collector v6.1.0 - Complete Green Agent Implementation
    """

    def __init__(
        self,
        config: Optional[ExpertMetricsConfig] = None,
        **kwargs
    ):
        """
        Initialize the metrics collector.

        Args:
            config: Configuration dataclass (preferred)
            **kwargs: Legacy arguments for backward compatibility
        """
        if config is None:
            # Build config from kwargs
            config = ExpertMetricsConfig(
                enable_anomaly_detection=kwargs.get('enable_anomaly_detection', True),
                enable_slo_tracking=kwargs.get('enable_slo_tracking', True),
                enable_cost_attribution=kwargs.get('enable_cost_attribution', True),
                enable_alerting=kwargs.get('enable_alerting', True),
                enable_predictive=kwargs.get('enable_predictive', True),
                enable_bio_integration=kwargs.get('enable_bio_integration', True),
                enable_carbon_intensity=kwargs.get('enable_carbon_intensity', True),
                enable_federated=kwargs.get('enable_federated', True),
                enable_cross_domain=kwargs.get('enable_cross_domain', True),
                enable_human_ai=kwargs.get('enable_human_ai', True),
                enable_sustainability_scoring=kwargs.get('enable_sustainability_scoring', True),
                enable_ml_anomaly_detection=kwargs.get('enable_ml_anomaly_detection', True),
                enable_digital_twin_integration=kwargs.get('enable_digital_twin_integration', True),
                enable_differential_privacy=kwargs.get('enable_differential_privacy', True),
                enable_persistence=kwargs.get('enable_persistence', True),
                enable_telemetry=kwargs.get('enable_telemetry', True),
                retention_hours=kwargs.get('retention_hours', 24.0),
                privacy_epsilon=kwargs.get('privacy_epsilon', 1.0),
                carbon_api_region=kwargs.get('carbon_api_region', 'us-east'),
                persistence_path=kwargs.get('persistence_path', 'metrics_state.pkl')
            )
        self.config = config

        # Feature flags
        self.enable_anomaly_detection = config.enable_anomaly_detection
        self.enable_slo_tracking = config.enable_slo_tracking
        self.enable_cost_attribution = config.enable_cost_attribution
        self.enable_alerting = config.enable_alerting
        self.enable_predictive = config.enable_predictive
        self.enable_bio_integration = config.enable_bio_integration and BIO_INSPIRED_AVAILABLE
        self.enable_carbon_intensity = config.enable_carbon_intensity
        self.enable_federated = config.enable_federated
        self.enable_cross_domain = config.enable_cross_domain
        self.enable_human_ai = config.enable_human_ai
        self.enable_sustainability_scoring = config.enable_sustainability_scoring
        self.enable_ml_anomaly_detection = config.enable_ml_anomaly_detection
        self.enable_digital_twin_integration = config.enable_digital_twin_integration
        self.enable_differential_privacy = config.enable_differential_privacy
        self.enable_persistence = config.enable_persistence
        self.enable_telemetry = config.enable_telemetry
        self.retention_hours = config.retention_hours

        # Bio-inspired modules
        self.token_manager = None
        self.gradient_manager = None
        self.scheduler = None
        self.compartment_manager = None
        self.biomass_storage = None
        self.harvester = None

        # Initialize sub-modules
        self.carbon_manager = CarbonIntensityManager(config) if self.enable_carbon_intensity else None
        self.predictive_analyzer = PredictiveMetricsAnalyzer(config) if self.enable_predictive else None
        self.cross_domain_transfer = MetricsCrossDomainTransfer() if self.enable_cross_domain else None
        self.federated_aggregator = FederatedMetricsAggregator(config) if self.enable_federated else None
        self.human_ai_support = HumanAICollaborativeSupport() if self.enable_human_ai else None
        self.ml_anomaly_detector = MLAnomalyDetector() if self.enable_ml_anomaly_detection else None
        self.slo_tracker = SLOTracker(config) if self.enable_slo_tracking else None
        self.cost_engine = CostAttributionEngine() if self.enable_cost_attribution else None
        self.telemetry = TelemetryExporter() if self.enable_telemetry else None
        self.persistence = MetricsPersistenceManager(config) if self.enable_persistence else None

        # Expert usage and metric storage
        self.expert_usage: Dict[str, int] = defaultdict(int)
        self.expert_success: Dict[str, int] = defaultdict(int)
        self.expert_failures: Dict[str, int] = defaultdict(int)
        self.expert_latency: Dict[str, deque] = defaultdict(lambda: deque(maxlen=10000))
        self.expert_energy: Dict[str, float] = defaultdict(float)
        self.expert_carbon: Dict[str, float] = defaultdict(float)
        self.expert_helium: Dict[str, float] = defaultdict(float)
        self.expert_ecoatp: Dict[str, float] = defaultdict(float)
        self.routing_decisions: deque = deque(maxlen=10000)
        self.routing_latency: deque = deque(maxlen=10000)
        self.pareto_points: deque = deque(maxlen=10000)
        self.bio_metrics_history: deque = deque(maxlen=10000)
        self.health_scores: Dict[str, float] = {}
        self.predictions: Dict[str, Dict[str, Any]] = {}
        self.correlation_map: Dict[str, List[str]] = defaultdict(list)

        # Sustainability
        self.total_carbon_savings_kg = 0.0
        self.total_helium_savings_l = 0.0
        self.sustainability_score = 0.0

        # Alerts
        self.active_alerts: Dict[str, Dict[str, Any]] = {}
        self.alert_history: deque = deque(maxlen=5000)
        self.alert_cooldowns: Dict[str, datetime] = {}

        # Thresholds (from config)
        self.thresholds: Dict[str, MetricThreshold] = self._build_thresholds_from_config()

        # SLOs (from config)
        if self.slo_tracker:
            self._build_slos_from_config()

        # Thread safety
        self._lock = threading.RLock()

        # Start background tasks
        self._start_background_tasks()

        # Load persisted state
        if self.enable_persistence:
            asyncio.create_task(self._load_persisted_state())

        logger.info(
            f"Enhanced Expert Metrics Collector v6.1.0 initialized: "
            f"bio_integration={self.enable_bio_integration}, "
            f"carbon_intensity={self.enable_carbon_intensity}, "
            f"predictive={self.enable_predictive}, "
            f"federated={self.enable_federated}, "
            f"ml_anomaly={self.enable_ml_anomaly_detection}, "
            f"digital_twin={self.enable_digital_twin_integration}, "
            f"differential_privacy={self.enable_differential_privacy}, "
            f"persistence={self.enable_persistence}, "
            f"telemetry={self.enable_telemetry}"
        )

    def _build_thresholds_from_config(self) -> Dict[str, MetricThreshold]:
        thresholds = {}
        for key, params in self.config.threshold_config.items():
            thresholds[key] = MetricThreshold(
                metric_name=params['metric_name'],
                warning_threshold=params['warning_threshold'],
                critical_threshold=params['critical_threshold'],
                comparison=params.get('comparison', 'greater_than'),
                gradient_modulated=params.get('gradient_modulated', False),
                cooldown_seconds=params.get('cooldown_seconds', 300.0)
            )
        return thresholds

    def _build_slos_from_config(self):
        for slo_id, params in self.config.slo_config.items():
            self.slo_tracker.define_slo(
                slo_id=slo_id,
                metric_name=params['metric_name'],
                target_value=params['target_value'],
                target_percentile=params.get('target_percentile', 99.0),
                evaluation_window_hours=params.get('evaluation_window_hours', 24.0)
            )

    def _start_background_tasks(self):
        if self.enable_carbon_intensity:
            asyncio.create_task(self._carbon_update_loop())
        if self.enable_predictive:
            asyncio.create_task(self._predictive_update_loop())
        if self.enable_federated:
            asyncio.create_task(self._federated_sync_loop())
        if self.enable_ml_anomaly_detection:
            asyncio.create_task(self._ml_anomaly_loop())
        if self.enable_telemetry:
            asyncio.create_task(self._telemetry_export_loop())

    async def _carbon_update_loop(self):
        while True:
            try:
                await self.carbon_manager.update_carbon_intensity()
                if self.telemetry:
                    intensity = await self.carbon_manager.get_current_intensity()
                    self.telemetry.gauge('carbon_intensity', intensity)
                await asyncio.sleep(self.config.carbon_update_interval)
            except Exception as e:
                logger.error(f"Carbon update error: {e}")
                await asyncio.sleep(60)

    async def _predictive_update_loop(self):
        while True:
            try:
                summary = self.get_metrics_summary()
                # Compute aggregate metrics for training
                success_rates = list(summary.get('success_rates', {}).values())
                avg_success = np.mean(success_rates) if success_rates else 0.5
                latencies = [s.get('avg_ms', 0) for s in summary.get('latency_stats', {}).values()]
                avg_latency = np.mean(latencies) if latencies else 100
                carbon_intensity = await self.carbon_manager.get_current_intensity() if self.enable_carbon_intensity else 400
                self.predictive_analyzer.update_history({
                    'success_rate': avg_success,
                    'avg_latency_ms': avg_latency,
                    'carbon_intensity': carbon_intensity,
                    'token_efficiency': self._get_token_efficiency(),
                    'health_score': np.mean(list(self.health_scores.values())) if self.health_scores else 0.5,
                    'slo_compliant': 1.0
                })
                await self.predictive_analyzer.train_forecast_model()
                await asyncio.sleep(300)
            except Exception as e:
                logger.error(f"Predictive update error: {e}")
                await asyncio.sleep(60)

    async def _federated_sync_loop(self):
        while True:
            try:
                if self.enable_federated and self.federated_aggregator:
                    summary = self.get_metrics_summary()
                    # Extract relevant metrics for federation
                    metrics = {
                        'avg_success_rate': np.mean(list(summary.get('success_rates', {}).values())),
                        'avg_latency_ms': np.mean([s.get('avg_ms', 0) for s in summary.get('latency_stats', {}).values()]),
                        'sustainability_score': self.sustainability_score,
                        'total_carbon_savings_kg': self.total_carbon_savings_kg
                    }
                    await self.federated_aggregator.send_local_metrics(
                        f"metrics_{self._get_instance_id()}",
                        metrics,
                        performance=self.sustainability_score
                    )
                    await self.federated_aggregator.get_global_metrics()
                await asyncio.sleep(3600)
            except Exception as e:
                logger.error(f"Federated sync error: {e}")
                await asyncio.sleep(300)

    async def _ml_anomaly_loop(self):
        while True:
            try:
                if self.enable_ml_anomaly_detection and self.ml_anomaly_detector:
                    # Collect recent metrics for training
                    for expert_id in self.health_scores:
                        latencies = list(self.expert_latency[expert_id])[-10:]
                        if latencies:
                            avg_latency = np.mean([l['value'] if isinstance(l, dict) else l for l in latencies])
                            metrics = {
                                'success_rate': self.get_expert_success_rate().get(expert_id, 0.5),
                                'latency_ms': avg_latency,
                                'carbon_per_inference': self.expert_carbon.get(expert_id, 0) / max(self.expert_usage.get(expert_id, 1), 1),
                                'helium_per_inference': self.expert_helium.get(expert_id, 0) / max(self.expert_usage.get(expert_id, 1), 1),
                                'token_efficiency': self._get_token_efficiency(),
                                'health_score': self.health_scores.get(expert_id, 0.5),
                                'gradient_level': self._get_gradient_modulation()
                            }
                            await self.ml_anomaly_detector.add_sample(metrics)
                await asyncio.sleep(300)
            except Exception as e:
                logger.error(f"ML anomaly loop error: {e}")
                await asyncio.sleep(600)

    async def _telemetry_export_loop(self):
        while True:
            try:
                if self.enable_telemetry and self.telemetry:
                    # Export metrics (could be written to a file or pushed to an endpoint)
                    export_data = await self.telemetry.export()
                    # For demonstration, log a sample
                    logger.debug(f"Telemetry export: {len(export_data)} bytes")
                await asyncio.sleep(self.config.telemetry_export_interval)
            except Exception as e:
                logger.error(f"Telemetry export error: {e}")
                await asyncio.sleep(60)

    async def _load_persisted_state(self):
        if self.persistence:
            state = await self.persistence.load_state()
            if state:
                # Restore state: we'll only restore non-transient data
                self.total_carbon_savings_kg = state.get('total_carbon_savings_kg', 0.0)
                self.total_helium_savings_l = state.get('total_helium_savings_l', 0.0)
                self.sustainability_score = state.get('sustainability_score', 0.0)
                # Restore SLO states if any
                if self.slo_tracker and 'slo_metrics' in state:
                    for slo_id, samples in state['slo_metrics'].items():
                        self.slo_tracker.metric_samples[slo_id] = deque(samples, maxlen=10000)
                # Restore alerts history
                if 'alert_history' in state:
                    self.alert_history = deque(state['alert_history'], maxlen=5000)
                logger.info("Restored persisted metrics state")

    async def save_state(self):
        if self.persistence:
            state = {
                'total_carbon_savings_kg': self.total_carbon_savings_kg,
                'total_helium_savings_l': self.total_helium_savings_l,
                'sustainability_score': self.sustainability_score,
                'slo_metrics': {
                    slo_id: list(samples)
                    for slo_id, samples in self.slo_tracker.metric_samples.items()
                } if self.slo_tracker else {},
                'alert_history': list(self.alert_history)
            }
            await self.persistence.save_state(state)

    def _get_instance_id(self) -> str:
        return hashlib.md5(f"{datetime.utcnow()}_{id(self)}".encode()).hexdigest()[:8]

    def _get_token_efficiency(self) -> float:
        if self.token_manager:
            summary = self.token_manager.get_system_summary()
            return summary.get('system_efficiency', 0.5)
        return 0.5

    def _get_gradient_modulation(self) -> float:
        if self.gradient_manager:
            carbon = self.gradient_manager.fields.get('carbon')
            if carbon and carbon.gradient_strength > 0.7:
                return 0.7
        return 1.0

    def _pump_trust_gradient(self, expert_id: str, success: bool):
        if self.gradient_manager:
            delta = 0.05 if success else -0.1
            self.gradient_manager.pump_field('trust', delta, source=f"expert_{expert_id}")

    def _record_token_consumption(self, expert_id: str, energy_kwh: float, success: bool):
        if self.token_manager:
            ecoatp_cost = energy_kwh * self.config.token_exchange_rate
            self.expert_ecoatp[expert_id] += ecoatp_cost
            if hasattr(self.token_manager, 'consume_tokens'):
                try:
                    self.token_manager.consume_tokens(
                        token_ids=[f"expert_{expert_id}"],
                        consumer=EcoATPConsumer.EXPERT_EXECUTION,
                        operation_success=success
                    )
                except Exception:
                    pass

    def _get_bio_metrics(self) -> Dict[str, Any]:
        metrics = {'timestamp': datetime.utcnow().isoformat()}
        if self.gradient_manager:
            metrics['gradients'] = self.gradient_manager.get_field_strengths()
        if self.token_manager:
            metrics['token_economy'] = self.token_manager.get_system_summary()
        if self.biomass_storage:
            metrics['biomass'] = self.biomass_storage.get_storage_stats()
        if self.harvester:
            metrics['harvester'] = self.harvester.get_harvesting_stats()
        if self.compartment_manager:
            metrics['compartments'] = {
                'total': len(self.compartment_manager.compartments),
                'viable': sum(1 for c in self.compartment_manager.compartments.values() if c.is_viable)
            }
        if self.scheduler:
            metrics['atp_synthase'] = self.scheduler.get_scheduler_stats()
        return metrics

    def _calculate_sustainability_score(self) -> float:
        if not self.health_scores:
            return 0.5
        avg_health = np.mean(list(self.health_scores.values()))
        token_eff = self._get_token_efficiency()
        carbon_factor = 1.0 - (self.carbon_manager.carbon_intensity / 800) if self.enable_carbon_intensity else 0.5
        success_rates = self.get_expert_success_rate()
        avg_success = np.mean(list(success_rates.values())) if success_rates else 0.5
        score = (avg_health * 0.25 + token_eff * 0.2 + carbon_factor * 0.25 + avg_success * 0.3)
        return min(1.0, max(0.0, score))

    # ========================================================================
    # Enhanced Metric Recording
    # ========================================================================

    def record_routing(
        self, routing_decisions: List[Tuple[int, float]], gating_context: Any,
        execution_time: float, success: bool, correlation_id: Optional[str] = None
    ):
        with self._lock:
            for expert_idx, weight in routing_decisions:
                self.expert_usage[expert_idx] = self.expert_usage.get(expert_idx, 0) + 1
                if success:
                    self.expert_success[expert_idx] = self.expert_success.get(expert_idx, 0) + 1
                else:
                    self.expert_failures[expert_idx] = self.expert_failures.get(expert_idx, 0) + 1

            self.routing_latency.append(execution_time)
            decision_record = {
                'decisions': routing_decisions, 'context': str(gating_context)[:200],
                'execution_time': execution_time, 'success': success,
                'timestamp': datetime.utcnow(), 'correlation_id': correlation_id
            }
            self.routing_decisions.append(decision_record)
            if correlation_id:
                self.correlation_map[correlation_id].append('routing')

    def record_expert_execution(
        self, expert_id: str, execution_time: float, energy_kwh: float,
        carbon_kg: float, helium_units: float, success: bool,
        correlation_id: Optional[str] = None, metadata: Optional[Dict[str, Any]] = None
    ):
        with self._lock:
            # Latency
            self.expert_latency[expert_id].append({'value': execution_time, 'timestamp': datetime.utcnow()})

            # Resources
            self.expert_energy[expert_id] += energy_kwh
            self.expert_carbon[expert_id] += carbon_kg
            self.expert_helium[expert_id] += helium_units

            # Token consumption
            if self.enable_bio_integration:
                self._record_token_consumption(expert_id, energy_kwh, success)

            # Success tracking
            if success:
                self.expert_success[expert_id] = self.expert_success.get(expert_id, 0) + 1
            else:
                self.expert_failures[expert_id] = self.expert_failures.get(expert_id, 0) + 1

            # Pump trust gradient
            if self.enable_bio_integration:
                self._pump_trust_gradient(expert_id, success)

            # Pareto point
            self.pareto_points.append({
                'expert_id': expert_id, 'energy': energy_kwh, 'time': execution_time,
                'helium': helium_units, 'carbon': carbon_kg,
                'ecoatp': self.expert_ecoatp.get(expert_id, 0),
                'timestamp': datetime.utcnow()
            })

            # Statistical anomaly detection
            if self.enable_anomaly_detection:
                anomalies = self.anomaly_detector.detect_anomalies(
                    f"{expert_id}_latency", execution_time, expert_id
                )
                for anomaly in anomalies:
                    self._process_anomaly(anomaly)

            # ML anomaly detection
            if self.enable_ml_anomaly_detection and self.ml_anomaly_detector and self.ml_anomaly_detector.is_trained:
                metrics = {
                    'success_rate': self.get_expert_success_rate().get(expert_id, 0.5),
                    'latency_ms': execution_time,
                    'carbon_per_inference': carbon_kg,
                    'helium_per_inference': helium_units,
                    'token_efficiency': self._get_token_efficiency(),
                    'health_score': self.health_scores.get(expert_id, 0.5),
                    'gradient_level': self._get_gradient_modulation()
                }
                is_anomaly, confidence, description = asyncio.run(
                    self.ml_anomaly_detector.detect_anomaly(metrics)
                )
                if is_anomaly:
                    anomaly = AnomalyEvent(
                        event_id=f"ml_anomaly_{datetime.utcnow().timestamp()}_{expert_id}",
                        metric_name=f"{expert_id}_complex_pattern",
                        anomaly_type=AnomalyType.ML_DETECTED,
                        detected_at=datetime.utcnow(),
                        expected_value=0.5,
                        actual_value=0.5,
                        deviation_std=1.0,
                        severity=MetricSeverity.WARNING if confidence > 0.7 else MetricSeverity.INFO,
                        expert_id=expert_id,
                        gradient_level=self._get_gradient_modulation(),
                        sustainability_impact=1.0 - confidence,
                        ml_confidence=confidence,
                        details={'description': description}
                    )
                    self._process_anomaly(anomaly)

            # Bio metrics snapshot
            if self.enable_bio_integration and len(self.bio_metrics_history) % 100 == 0:
                self.bio_metrics_history.append(self._get_bio_metrics())

            # SLO tracking
            if self.enable_slo_tracking and self.slo_tracker:
                self.slo_tracker.record_metric('latency_slo', execution_time)
                self.slo_tracker.record_metric('carbon_slo', carbon_kg)
                if self.enable_bio_integration:
                    self.slo_tracker.record_metric('token_efficiency_slo',
                        self.expert_ecoatp.get(expert_id, 0) / max(self.expert_usage.get(expert_id, 1), 1))

            # Cost attribution
            if self.enable_cost_attribution:
                self.cost_engine.record_cost(expert_id, carbon_kg, helium_units, energy_kwh)

            # Correlation tracking
            if correlation_id:
                self.correlation_map[correlation_id].append(f'expert_{expert_id}')

            # Threshold checking
            if self.enable_alerting:
                self._check_bio_thresholds(expert_id, execution_time, success)

            # Health score update
            self._update_health_score(expert_id)

            # Predictive analytics
            if self.enable_predictive:
                self._update_predictions(expert_id)

            # Cross-domain knowledge transfer
            if self.enable_cross_domain:
                self.cross_domain_transfer.transfer_knowledge(
                    'metrics', 'energy',
                    'efficiency_strategies',
                    {'expert_id': expert_id, 'energy': energy_kwh, 'carbon': carbon_kg}
                )

            # Human-AI collaboration
            if self.enable_human_ai:
                self.human_ai_support.generate_explanation(
                    expert_id,
                    {'success_rate': self.get_expert_success_rate().get(expert_id, 0.5),
                     'avg_latency_ms': execution_time,
                     'carbon_per_inference': carbon_kg,
                     'token_cost': self.expert_ecoatp.get(expert_id, 0) / max(self.expert_usage.get(expert_id, 1), 1)},
                    []
                )

            # Sustainability
            if self.enable_sustainability_scoring:
                self.sustainability_score = self._calculate_sustainability_score()
                self.total_carbon_savings_kg += max(0, 0.001 - carbon_kg) if carbon_kg < 0.001 else 0

            # Telemetry
            if self.telemetry:
                self.telemetry.increment('expert_executions', {'expert_id': expert_id})
                self.telemetry.histogram('execution_latency_ms', execution_time, {'expert_id': expert_id})
                self.telemetry.gauge('sustainability_score', self.sustainability_score)

    def _process_anomaly(self, anomaly: AnomalyEvent):
        logger.warning(f"Anomaly detected: {anomaly.metric_name} - {anomaly.anomaly_type.value} "
                      f"(severity={anomaly.severity.value}, gradient={anomaly.gradient_level:.2f})")
        if anomaly.severity in [MetricSeverity.CRITICAL, MetricSeverity.EMERGENCY]:
            self._create_alert(
                f"anomaly_{anomaly.event_id}", anomaly.metric_name,
                f"Anomaly: {anomaly.anomaly_type.value}. Expected={anomaly.expected_value:.2f}, "
                f"Actual={anomaly.actual_value:.2f}, Gradient={anomaly.gradient_level:.2f}",
                anomaly.severity
            )
        # Update dashboard
        if self.enable_human_ai:
            self.human_ai_support.dashboard_data['alerts'].append({
                'timestamp': datetime.utcnow().isoformat(),
                'metric': anomaly.metric_name,
                'type': anomaly.anomaly_type.value,
                'severity': anomaly.severity.value
            })

    def _check_bio_thresholds(self, expert_id: str, execution_time: float, success: bool):
        gradient_mod = self._get_gradient_modulation() if self.enable_bio_integration else 1.0

        if 'latency_p95' in self.thresholds:
            p95 = self.get_expert_latency_stats().get(expert_id, {}).get('p95_ms', 0)
            exceeded, severity = self.thresholds['latency_p95'].is_exceeded(p95, gradient_mod)
            if exceeded:
                self._create_alert(f"latency_{expert_id}", 'latency_p95',
                    f"Expert {expert_id} P95 latency {p95:.1f}ms exceeded threshold", severity)

        if 'token_balance' in self.thresholds and self.enable_bio_integration:
            if self.token_manager:
                summary = self.token_manager.get_system_summary()
                balance = summary.get('total_balance', 500)
                exceeded, severity = self.thresholds['token_balance'].is_exceeded(balance, gradient_mod)
                if exceeded:
                    self._create_alert('token_balance', 'token_balance',
                        f"System token balance {balance:.0f} below threshold", severity)

        if 'sustainability_score' in self.thresholds:
            exceeded, severity = self.thresholds['sustainability_score'].is_exceeded(
                self.sustainability_score, gradient_mod)
            if exceeded:
                self._create_alert('sustainability_score', 'sustainability_score',
                    f"Sustainability score {self.sustainability_score:.2f} below threshold", severity)

    def _create_alert(self, alert_id: str, metric_name: str, message: str, severity: MetricSeverity):
        if alert_id in self.alert_cooldowns:
            last_alert = self.alert_cooldowns[alert_id]
            threshold = self.thresholds.get(metric_name)
            if threshold:
                cooldown = threshold.cooldown_seconds
                if (datetime.utcnow() - last_alert).total_seconds() < cooldown:
                    return

        alert = {
            'alert_id': alert_id, 'metric': metric_name, 'message': message,
            'severity': severity.value, 'timestamp': datetime.utcnow().isoformat(),
            'acknowledged': False,
            'gradient_level': self._get_gradient_modulation() if self.enable_bio_integration else 1.0,
            'sustainability_impact': self.sustainability_score
        }

        self.active_alerts[alert_id] = alert
        self.alert_history.append(alert)
        self.alert_cooldowns[alert_id] = datetime.utcnow()

        log_level = logging.CRITICAL if severity == MetricSeverity.CRITICAL else logging.WARNING
        logger.log(log_level, f"ALERT: {message}")

    def _update_health_score(self, expert_id: str):
        success_rate = self.get_expert_success_rate().get(expert_id, 0.5)
        latency_stats = self.get_expert_latency_stats().get(expert_id, {})
        p95 = latency_stats.get('p95_ms', 100)
        latency_score = 1.0 / (1.0 + p95 / 100)

        total_carbon = self.expert_carbon.get(expert_id, 0)
        total_usage = max(self.expert_usage.get(expert_id, 1), 1)
        carbon_score = 1.0 / (1.0 + total_carbon / total_usage * 10000)

        token_score = 0.5
        if self.enable_bio_integration:
            ecoatp = self.expert_ecoatp.get(expert_id, 0)
            token_score = 1.0 / (1.0 + ecoatp / max(total_usage, 1) / 100)

        health = 0.35 * success_rate + 0.25 * latency_score + 0.25 * carbon_score + 0.15 * token_score
        self.health_scores[expert_id] = health

    def _update_predictions(self, expert_id: str):
        latencies = list(self.expert_latency.get(expert_id, []))
        if len(latencies) < 10:
            return
        values = [l['value'] if isinstance(l, dict) else l for l in latencies[-50:]]
        x = np.arange(len(values))
        y = np.array(values)
        try:
            slope, intercept = np.polyfit(x, y, 1)
            prediction = intercept + slope * (len(values) + 10)
            self.predictions[expert_id] = {
                'predicted_latency_ms': max(0, prediction),
                'trend': 'increasing' if slope > 0.01 else 'decreasing' if slope < -0.01 else 'stable',
                'confidence': 0.7 if len(values) > 30 else 0.4,
                'updated_at': datetime.utcnow().isoformat(),
                'token_cost_trend': 'increasing' if slope > 0 else 'stable'
            }
        except Exception:
            pass

    # ========================================================================
    # Metric Queries (Enhanced)
    # ========================================================================

    def get_expert_usage(self) -> Dict[int, float]:
        total_usage = sum(self.expert_usage.values())
        if total_usage == 0:
            return {}
        return {expert: count / total_usage for expert, count in self.expert_usage.items()}

    def get_expert_success_rate(self) -> Dict[int, float]:
        rates = {}
        for expert_id in set(list(self.expert_success.keys()) + list(self.expert_failures.keys())):
            successes = self.expert_success.get(expert_id, 0)
            failures = self.expert_failures.get(expert_id, 0)
            total = successes + failures
            rates[expert_id] = successes / total if total > 0 else 0.5
        return rates

    def get_expert_latency_stats(self) -> Dict[str, Dict[str, float]]:
        stats = {}
        for expert_id, latencies in self.expert_latency.items():
            values = [l['value'] if isinstance(l, dict) else l for l in latencies]
            if values:
                arr = np.array(values)
                stats[expert_id] = {
                    'avg_ms': float(np.mean(arr)), 'p50_ms': float(np.median(arr)),
                    'p95_ms': float(np.percentile(arr, 95)), 'p99_ms': float(np.percentile(arr, 99)),
                    'min_ms': float(np.min(arr)), 'max_ms': float(np.max(arr)),
                    'std_ms': float(np.std(arr)), 'samples': len(values)
                }
        return stats

    def get_resource_consumption(self) -> Dict[str, Dict[str, float]]:
        consumption = {}
        for expert_id in set(list(self.expert_energy.keys()) + list(self.expert_carbon.keys())):
            consumption[expert_id] = {
                'total_energy_kwh': self.expert_energy.get(expert_id, 0.0),
                'total_carbon_kg': self.expert_carbon.get(expert_id, 0.0),
                'total_helium_units': self.expert_helium.get(expert_id, 0.0),
                'total_ecoatp': self.expert_ecoatp.get(expert_id, 0.0),
                'carbon_per_use_kg': self.expert_carbon.get(expert_id, 0.0) / max(self.expert_usage.get(expert_id, 1), 1),
                'ecoatp_per_use': self.expert_ecoatp.get(expert_id, 0.0) / max(self.expert_usage.get(expert_id, 1), 1)
            }
        return consumption

    def get_pareto_frontier(self) -> List[Dict]:
        if not self.pareto_points:
            return []
        recent = list(self.pareto_points)[-1000:]
        pareto_optimal = []
        for i, point in enumerate(recent):
            dominated = False
            for j, other in enumerate(recent):
                if i != j:
                    if (other['energy'] <= point['energy'] and other['time'] <= point['time'] and
                        other['helium'] <= point['helium'] and other.get('ecoatp', 0) <= point.get('ecoatp', 0) and
                        (other['energy'] < point['energy'] or other['time'] < point['time'] or
                         other['helium'] < point['helium'] or other.get('ecoatp', 0) < point.get('ecoatp', 0))):
                        dominated = True
                        break
            if not dominated:
                pareto_optimal.append(point)
        return pareto_optimal

    def get_health_scores(self) -> Dict[str, float]:
        return self.health_scores.copy()

    def get_alerts(self, acknowledged: Optional[bool] = None,
                   severity: Optional[MetricSeverity] = None, limit: int = 50) -> List[Dict[str, Any]]:
        alerts = list(self.alert_history)
        if acknowledged is not None:
            alerts = [a for a in alerts if a.get('acknowledged') == acknowledged]
        if severity:
            alerts = [a for a in alerts if a.get('severity') == severity.value]
        return alerts[-limit:]

    def acknowledge_alert(self, alert_id: str) -> bool:
        if alert_id in self.active_alerts:
            self.active_alerts[alert_id]['acknowledged'] = True
            return True
        return False

    def get_predictions(self) -> Dict[str, Dict[str, Any]]:
        return self.predictions.copy()

    async def get_slo_status(self) -> Dict[str, Dict[str, Any]]:
        if self.slo_tracker:
            return await self.slo_tracker.evaluate_slos()
        return {}

    # ========================================================================
    # Metrics Summary with Bio-Inspired Data
    # ========================================================================

    def get_metrics_summary(self) -> Dict[str, Any]:
        summary = {
            'timestamp': datetime.utcnow().isoformat(),
            'expert_usage': self.get_expert_usage(),
            'success_rates': self.get_expert_success_rate(),
            'latency_stats': self.get_expert_latency_stats(),
            'resource_consumption': self.get_resource_consumption(),
            'pareto_frontier_size': len(self.get_pareto_frontier()),
            'total_routes': len(self.routing_decisions),
            'avg_routing_latency_ms': np.mean(list(self.routing_latency)) if self.routing_latency else 0,
            'health_scores': self.get_health_scores(),
            'active_alerts': len([a for a in self.active_alerts.values() if not a.get('acknowledged')]),
            'total_alerts': len(self.alert_history),
            'bio_integration_active': self.enable_bio_integration,
            'bio_modules_available': BIO_INSPIRED_AVAILABLE,
            'carbon_intensity_active': self.enable_carbon_intensity,
            'federated_active': self.enable_federated,
            'cross_domain_active': self.enable_cross_domain,
            'human_ai_active': self.enable_human_ai,
            'sustainability_scoring_active': self.enable_sustainability_scoring,
            'ml_anomaly_active': self.enable_ml_anomaly_detection,
            'digital_twin_active': self.enable_digital_twin_integration,
            'differential_privacy_active': self.enable_differential_privacy,
            'persistence_active': self.enable_persistence,
            'telemetry_active': self.enable_telemetry,
            'sustainability_score': self.sustainability_score,
            'total_carbon_savings_kg': self.total_carbon_savings_kg
        }

        if self.slo_tracker:
            # We need to await the result, but this method is sync. Provide an async version.
            # For now, we'll leave it as is and let the caller use async method.
            pass

        if self.anomaly_detector:
            summary['anomaly_stats'] = self.anomaly_detector.get_detection_stats()

        if self.ml_anomaly_detector:
            summary['ml_anomaly_trained'] = self.ml_anomaly_detector.is_trained

        if self.enable_predictive:
            summary['predictions'] = self.get_predictions()

        if self.enable_federated:
            summary['federated_stats'] = self.federated_aggregator.get_federated_stats()

        if self.enable_human_ai:
            summary['human_ai_insights'] = self.human_ai_support.get_decision_insights('all', 24)
            # dashboard data can be async; skip in sync method

        if self.enable_bio_integration:
            summary['bio_metrics'] = self._get_bio_metrics()
            summary['gradient_modulation'] = self._get_gradient_modulation()

        if self.enable_cross_domain:
            summary['cross_domain_stats'] = self.cross_domain_transfer.get_transfer_statistics()

        return summary

    async def get_metrics_summary_async(self) -> Dict[str, Any]:
        """Async version of get_metrics_summary that includes async calls."""
        summary = self.get_metrics_summary()
        if self.slo_tracker:
            summary['slo_status'] = await self.slo_tracker.evaluate_slos()
        if self.enable_human_ai:
            summary['dashboard_data'] = await self.human_ai_support.get_dashboard_data()
        return summary

    def get_expert_performance_report(self, expert_id: str) -> Dict[str, Any]:
        latency_stats = self.get_expert_latency_stats().get(expert_id, {})
        success_rate = self.get_expert_success_rate().get(expert_id, 0)
        health = self.health_scores.get(expert_id, 0.5)
        resource_consumption = self.get_resource_consumption().get(expert_id, {})
        predictions = self.predictions.get(expert_id, {})
        return {
            'expert_id': expert_id,
            'success_rate': success_rate,
            'latency_stats': latency_stats,
            'health_score': health,
            'resource_consumption': resource_consumption,
            'predictions': predictions,
            'usage_count': self.expert_usage.get(expert_id, 0),
            'failure_count': self.expert_failures.get(expert_id, 0),
            'sustainability_score': self.sustainability_score
        }

    # ========================================================================
    # Health Status (NEW)
    # ========================================================================

    async def get_health_status(self) -> Dict[str, Any]:
        """Report the health status of the metrics collector itself."""
        return {
            'status': 'healthy',
            'score': min(1.0, self.sustainability_score),
            'details': {
                'components': {
                    'carbon_manager': self.carbon_manager is not None,
                    'predictive_analyzer': self.predictive_analyzer is not None,
                    'federated_aggregator': self.federated_aggregator is not None,
                    'slo_tracker': self.slo_tracker is not None,
                    'ml_anomaly_detector': self.ml_anomaly_detector is not None,
                    'persistence': self.persistence is not None,
                    'telemetry': self.telemetry is not None
                },
                'active_alerts': len([a for a in self.active_alerts.values() if not a.get('acknowledged')]),
                'total_samples': sum(len(v) for v in self.expert_latency.values())
            }
        }

    # ========================================================================
    # Bio-Inspired Module Injection
    # ========================================================================

    def inject_bio_core(self, bio_core: Any = None, **kwargs):
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

        injections = {
            'token_manager': self.token_manager is not None,
            'gradient_manager': self.gradient_manager is not None,
            'scheduler': self.scheduler is not None,
            'compartment_manager': self.compartment_manager is not None,
            'biomass_storage': self.biomass_storage is not None,
            'harvester': self.harvester is not None
        }
        logger.info(f"Bio-inspired injections into Expert Metrics: {injections}")
        if any(injections.values()):
            self.enable_bio_integration = True

    # ========================================================================
    # Shutdown
    # ========================================================================

    async def shutdown(self):
        """Graceful shutdown."""
        logger.info("Shutting down Expert Metrics Collector")
        if self.enable_persistence:
            await self.save_state()
        if self.carbon_manager:
            await self.carbon_manager.close()
        if self.federated_aggregator:
            await self.federated_aggregator.close()
        logger.info("Shutdown complete")
