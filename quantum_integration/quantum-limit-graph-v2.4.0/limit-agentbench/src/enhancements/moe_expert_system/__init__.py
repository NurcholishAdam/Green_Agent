#!/usr/bin/env python3
"""
Green Agent MoE Expert System v7.0.0 - Unified Metabolic Ecosystem
Full Green Agent MOPD Integration

ENHANCEMENTS OVER v6.3.0:
1. INTEGRATED with central Config, Storage, Logger, MetricsRegistry, AsyncMessageQueue.
2. ADDED teacher interface (`policy_probs`) for MTPD optimizer.
3. PUBLISHES FeedbackEvent for every task processing, expert selection, health state changes.
4. USES central AdaptiveCostFunction, ParetoGating, and DriftDetector.
5. REMOVED custom persistence; now uses central Storage.
6. REMOVED custom Prometheus; now uses central MetricsRegistry.
7. REMOVED custom logging; now uses central structlog.
8. All optional dependencies (PyTorch, scikit-learn, etc.) still gracefully degrade.
"""

import asyncio
import hashlib
import json
import os
import random
import time
import zlib
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

# -----------------------------------------------------------------------------
# IMPORT CENTRAL GREEN AGENT COMPONENTS
# -----------------------------------------------------------------------------
from ..config import config as central_config
from ..storage import Storage
from ..schemas.feedback_event import FeedbackEvent
from ..routing.pareto_gating import ParetoGating
from ..feedback.adaptive_cost import AdaptiveCostFunction
from ..safety.drift_detector import DriftDetector
from ..scaling.message_queue import AsyncMessageQueue
from ..metrics import MetricsRegistry
from ..logger import logger

# Optional dependencies (graceful degradation)
try:
    import aiofiles
except ImportError:
    aiofiles = None

try:
    from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator
except ImportError:
    BaseModel = None

try:
    from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential
except ImportError:
    def retry(*args, **kwargs):
        return lambda f: f
    stop_after_attempt = lambda x: None
    wait_exponential = lambda **k: None
    retry_if_exception_type = lambda e: None

# PyTorch (optional)
try:
    import torch
    import torch.nn as nn
    import torch.optim as optim
    from torch.utils.data import DataLoader, TensorDataset
    TORCH_AVAILABLE = True
except ImportError:
    TORCH_AVAILABLE = False

# Bio-inspired modules (optional)
try:
    from enhancements.bio_inspired.eco_atp_currency import EcoATPTokenManager
    from enhancements.bio_inspired.proton_gradient_fields import GradientFieldManager
    from enhancements.bio_inspired.chromatophore_compartments import CompartmentManager
    from enhancements.bio_inspired.biomass_storage import BiomassStorage
    BIO_INSPIRED_AVAILABLE = True
except ImportError:
    BIO_INSPIRED_AVAILABLE = False

# Digital Twin and Sustainability Engine (optional)
DIGITAL_TWIN_AVAILABLE = False
SUSTAINABILITY_ENGINE_AVAILABLE = False
try:
    from enhancements.advanced.system_digital_twin import (
        SystemDigitalTwin,
        DigitalTwinConfig,
        SimulationResult,
        SimulationScenario,
        ResourceProjection
    )
    DIGITAL_TWIN_AVAILABLE = True
except ImportError:
    pass
try:
    from enhancements.sustainability.unified_sustainability_engine import (
        UnifiedSustainabilityEngine,
        UnifiedSustainabilityScore,
        SustainabilityDimension,
        SustainabilityThreshold
    )
    SUSTAINABILITY_ENGINE_AVAILABLE = True
except ImportError:
    pass

# Carbon/helium managers (stubs if not available)
try:
    from .carbon_intensity import CarbonIntensityManager
    from .helium_optimizer import HeliumEfficiencyOptimizer
    CARBON_HELIUM_AVAILABLE = True
except ImportError:
    CARBON_HELIUM_AVAILABLE = False

# Core MoE components (relative imports)
from .expert_registry import (
    ExpertRegistry,
    ExpertProfile,
    ExpertDomain,
    ExpertLifecycleState,
    ExpertVersion,
    HardwareProfile,
    HealthMetrics,
    ExpertCertification,
    CertificationLevel,
    FitnessScore
)
from .gating_network import MoEGatingNetwork, GatingContext, EnhancedSparseMoEGate
from .expert_router import ExpertRouter, RoutingMetrics, ExpertCircuitBreaker, CircuitBreakerState
from .experts.energy_expert import EnergyExpert
from .experts.data_expert import DataExpert
from .experts.iot_expert import IoTExpert

# Optional experts
QUANTUM_AVAILABLE = False
try:
    from .experts.quantum_expert import QuantumExpert
    QUANTUM_AVAILABLE = True
except ImportError:
    pass
HELIUM_AVAILABLE = False
try:
    from .experts.helium_expert import HeliumExpert
    HELIUM_AVAILABLE = True
except ImportError:
    pass

# Advanced modules (optional)
EVOLVING_GATES_AVAILABLE = False
try:
    from .advanced.self_evolving_gates import EnhancedSelfEvolvingGate
    EVOLVING_GATES_AVAILABLE = True
except ImportError:
    pass
FEDERATED_AVAILABLE = False
try:
    from .advanced.federated_experts import EnhancedFederatedOrchestrator
    FEDERATED_AVAILABLE = True
except ImportError:
    pass
CROSS_REGION_AVAILABLE = False
try:
    from .advanced.cross_region_federation import CrossRegionFederationOptimizer
    CROSS_REGION_AVAILABLE = True
except ImportError:
    pass

# Integration layers
try:
    from .integration.layer_integrator import EnhancedLayerIntegrator
    LAYER_INTEGRATOR_AVAILABLE = True
except ImportError:
    LAYER_INTEGRATOR_AVAILABLE = False
try:
    from .integration.enhanced_work_integration import EnhancedWorkIntegrator
    WORK_INTEGRATOR_AVAILABLE = True
except ImportError:
    WORK_INTEGRATOR_AVAILABLE = False
try:
    from .integration.quantum_limit_integration import QuantumLimitGraphIntegrator
    QUANTUM_LIMIT_INTEGRATOR_AVAILABLE = True
except ImportError:
    QUANTUM_LIMIT_INTEGRATOR_AVAILABLE = False

# Monitoring
try:
    from .monitoring.expert_metrics import ExpertMetricsCollector
    METRICS_COLLECTOR_AVAILABLE = True
except ImportError:
    METRICS_COLLECTOR_AVAILABLE = False

# Sustainability modules
try:
    from .sustainability.carbon_sequestration import CarbonSequestrationManager
    CARBON_SEQUESTRATION_AVAILABLE = True
except ImportError:
    CARBON_SEQUESTRATION_AVAILABLE = False
try:
    from .sustainability.circular_computing import CircularComputingManager
    CIRCULAR_COMPUTING_AVAILABLE = True
except ImportError:
    CIRCULAR_COMPUTING_AVAILABLE = False
try:
    from .sustainability.carbon_offset_verification import AutomatedCarbonOffsetVerification
    CARBON_OFFSET_AVAILABLE = True
except ImportError:
    CARBON_OFFSET_AVAILABLE = False
try:
    from .sustainability.biodiversity_impact import BiodiversityImpactAssessor
    BIODIVERSITY_AVAILABLE = True
except ImportError:
    BIODIVERSITY_AVAILABLE = False

# -----------------------------------------------------------------------------
# Configuration – now built from central_config
# -----------------------------------------------------------------------------
@dataclass
class UnifiedEcosystemConfig:
    """Configuration for Unified Metabolic Ecosystem, built from central_config."""
    # Feature flags
    enable_quantum: bool = getattr(central_config, "enable_quantum", False)
    enable_helium: bool = getattr(central_config, "enable_helium", False)
    enable_bio_inspired: bool = getattr(central_config, "enable_bio_inspired", True) and BIO_INSPIRED_AVAILABLE
    enable_evolving_gates: bool = getattr(central_config, "enable_evolving_gates", True)
    enable_federated: bool = getattr(central_config, "enable_federated", False)
    enable_cross_region: bool = getattr(central_config, "enable_cross_region", False)
    enable_sustainability_dashboard: bool = getattr(central_config, "enable_sustainability_dashboard", True)
    enable_predictive_maintenance: bool = getattr(central_config, "enable_predictive_maintenance", True)
    enable_digital_twin: bool = getattr(central_config, "enable_digital_twin", True) and DIGITAL_TWIN_AVAILABLE
    enable_unified_sustainability: bool = getattr(central_config, "enable_unified_sustainability", True) and SUSTAINABILITY_ENGINE_AVAILABLE
    enable_health_checks: bool = getattr(central_config, "enable_health_checks", True)
    enable_self_healing: bool = getattr(central_config, "enable_self_healing", True)
    enable_alert_escalation: bool = getattr(central_config, "enable_alert_escalation", True)
    enable_dynamic_reconfig: bool = getattr(central_config, "enable_dynamic_reconfig", True)
    enable_telemetry: bool = False  # now using central MetricsRegistry

    # Tunable parameters
    twin_time_horizon_years: int = getattr(central_config, "twin_time_horizon_years", 10)
    twin_n_simulations: int = getattr(central_config, "twin_n_simulations", 1000)
    twin_confidence: float = getattr(central_config, "twin_confidence", 0.95)
    health_check_interval: int = getattr(central_config, "health_check_interval", 60)
    recovery_max_attempts: int = getattr(central_config, "recovery_max_attempts", 5)
    telemetry_export_interval: int = getattr(central_config, "telemetry_export_interval", 60)
    alert_escalation_timeout: int = getattr(central_config, "alert_escalation_timeout", 300)
    rate_limit_per_minute: int = getattr(central_config, "rate_limit_requests", 60)
    carbon_api_region: str = getattr(central_config, "carbon_api_region", "us-east")
    carbon_update_interval: int = getattr(central_config, "carbon_update_interval", 300)

    def __post_init__(self):
        if self.health_check_interval < 1:
            raise ValueError("health_check_interval must be >= 1")
        if self.recovery_max_attempts < 1:
            raise ValueError("recovery_max_attempts must be >= 1")
        if self.rate_limit_per_minute < 1:
            raise ValueError("rate_limit_per_minute must be >= 1")

# -----------------------------------------------------------------------------
# Rate Limiter (unchanged)
# -----------------------------------------------------------------------------
class RateLimiter:
    def __init__(self, rate_per_minute: int):
        self.rate = rate_per_minute / 60.0
        self.tokens = float(rate_per_minute)
        self.last_update = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> bool:
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self.last_update
            self.tokens += elapsed * self.rate
            if self.tokens > self.rate * 60:
                self.tokens = self.rate * 60
            self.last_update = now
            if self.tokens >= 1:
                self.tokens -= 1
                return True
            return False

# -----------------------------------------------------------------------------
# Health Check System (unchanged, but uses central logger)
# -----------------------------------------------------------------------------
class HealthCheckSystem:
    def __init__(self, config: UnifiedEcosystemConfig):
        self.config = config
        self.component_health: Dict[str, Dict] = {}
        self.health_history: Dict[str, List[Dict]] = defaultdict(list)
        self._lock = asyncio.Lock()
        self._running = True
        self._check_task: Optional[asyncio.Task] = None
        self._start_health_check_loop()
        logger.info("HealthCheckSystem initialized")

    def _start_health_check_loop(self):
        async def health_loop():
            while self._running:
                try:
                    await self._perform_health_checks()
                    await asyncio.sleep(self.config.health_check_interval)
                except Exception as e:
                    logger.error(f"Health check loop error: {e}")
                    await asyncio.sleep(60)
        self._check_task = asyncio.create_task(health_loop())

    async def _perform_health_checks(self):
        async with self._lock:
            for component_name, data in self.component_health.items():
                component = data.get('component')
                if component is None:
                    continue
                try:
                    if hasattr(component, 'get_health_status'):
                        health_result = await component.get_health_status()
                        data['status'] = health_result.get('status', 'unknown')
                        data['score'] = health_result.get('score', 0.5)
                    else:
                        data['status'] = self._default_health_check(component_name)
                        data['score'] = self._calculate_default_health(component_name)
                except Exception as e:
                    logger.warning(f"Health check for {component_name} failed: {e}")
                    data['status'] = 'unhealthy'
                    data['score'] = 0.0
                data['last_check'] = datetime.utcnow().isoformat()
                self.health_history[component_name].append({
                    'timestamp': data['last_check'],
                    'status': data['status'],
                    'score': data['score']
                })
                if len(self.health_history[component_name]) > 100:
                    self.health_history[component_name] = self.health_history[component_name][-100:]

    def _default_health_check(self, component_name: str) -> str:
        return random.choice(['healthy', 'degraded', 'unhealthy']) if random.random() > 0.3 else 'healthy'

    def _calculate_default_health(self, component_name: str) -> float:
        return random.uniform(0.3, 1.0)

    def register_component(self, component_name: str, component: Any):
        async with self._lock:
            self.component_health[component_name] = {
                'component': component,
                'status': 'unknown',
                'score': 0.5,
                'last_check': None,
                'registered_at': datetime.utcnow().isoformat()
            }
            logger.debug(f"Registered component for health checks: {component_name}")

    async def get_component_health(self, component_name: str) -> Optional[Dict]:
        async with self._lock:
            return self.component_health.get(component_name)

    async def get_system_health(self) -> Dict[str, Any]:
        async with self._lock:
            total_score = 0.0
            component_statuses = {}
            for name, data in self.component_health.items():
                status = data.get('status', 'unknown')
                score = data.get('score', 0.5)
                component_statuses[name] = {'status': status, 'score': score}
                total_score += score
            avg_score = total_score / max(len(self.component_health), 1)
            system_status = "healthy" if avg_score > 0.8 else "degraded" if avg_score > 0.5 else "unhealthy"
            return {
                'timestamp': datetime.utcnow().isoformat(),
                'system_status': system_status,
                'system_score': avg_score,
                'components': component_statuses,
                'total_components': len(self.component_health)
            }

    async def shutdown(self):
        self._running = False
        if self._check_task:
            self._check_task.cancel()
            try:
                await self._check_task
            except asyncio.CancelledError:
                pass
        logger.info("HealthCheckSystem shut down")

# -----------------------------------------------------------------------------
# Self-Healing System (unchanged, uses central logger)
# -----------------------------------------------------------------------------
class SelfHealingSystem:
    def __init__(self, config: UnifiedEcosystemConfig, health_system: Optional[HealthCheckSystem] = None):
        self.config = config
        self.health_system = health_system
        self.recovery_handlers: Dict[str, Callable] = {}
        self.failure_history: Dict[str, List[Dict]] = defaultdict(list)
        self.recovery_attempts: Dict[str, int] = defaultdict(int)
        self.max_attempts = config.recovery_max_attempts
        self._lock = asyncio.Lock()
        self._running = True
        self._monitor_task: Optional[asyncio.Task] = None
        self._start_monitor_loop()
        logger.info("SelfHealingSystem initialized")

    def _start_monitor_loop(self):
        async def monitor_loop():
            while self._running:
                try:
                    if self.health_system:
                        health = await self.health_system.get_system_health()
                        for comp_name, data in health.get('components', {}).items():
                            if data.get('status') in ['degraded', 'unhealthy']:
                                await self._attempt_recovery(comp_name)
                    await asyncio.sleep(30)
                except Exception as e:
                    logger.error(f"Recovery monitor loop error: {e}")
                    await asyncio.sleep(60)
        self._monitor_task = asyncio.create_task(monitor_loop())

    async def _attempt_recovery(self, component_name: str):
        async with self._lock:
            if self.recovery_attempts[component_name] >= self.max_attempts:
                logger.warning(f"Component {component_name} exceeded max recovery attempts")
                return

            logger.info(f"Attempting recovery for component: {component_name}")
            self.recovery_attempts[component_name] += 1

            success = False
            handler = self.recovery_handlers.get(component_name)
            if handler:
                try:
                    if asyncio.iscoroutinefunction(handler):
                        success = await handler()
                    else:
                        success = handler()
                except Exception as e:
                    logger.error(f"Recovery handler for {component_name} failed: {e}")
                    success = False
            else:
                success = await self._generic_restart(component_name)

            self.failure_history[component_name].append({
                'timestamp': datetime.utcnow().isoformat(),
                'attempt': self.recovery_attempts[component_name],
                'success': success
            })

            if success:
                logger.info(f"Successfully recovered component: {component_name}")
                if self.health_system:
                    async with self.health_system._lock:
                        if component_name in self.health_system.component_health:
                            self.health_system.component_health[component_name]['status'] = 'healthy'
                            self.health_system.component_health[component_name]['score'] = 1.0
            else:
                logger.warning(f"Failed to recover component: {component_name} (attempt {self.recovery_attempts[component_name]})")

    async def _generic_restart(self, component_name: str) -> bool:
        await asyncio.sleep(0.5)
        return random.random() > 0.3

    def register_recovery_handler(self, component_name: str, handler: Callable):
        async with self._lock:
            self.recovery_handlers[component_name] = handler
            logger.debug(f"Registered recovery handler for {component_name}")

    async def get_recovery_stats(self) -> Dict[str, Any]:
        async with self._lock:
            total_attempts = sum(self.recovery_attempts.values())
            total_failures = sum(
                1 for history in self.failure_history.values()
                for h in history if not h.get('success', False)
            )
            return {
                'total_recovery_attempts': total_attempts,
                'total_failures': total_failures,
                'success_rate': (total_attempts - total_failures) / max(total_attempts, 1),
                'component_attempts': dict(self.recovery_attempts),
                'recent_failures': {
                    name: history[-5:]
                    for name, history in self.failure_history.items()
                    if history
                }
            }

    async def shutdown(self):
        self._running = False
        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass
        logger.info("SelfHealingSystem shut down")

# -----------------------------------------------------------------------------
# Alert Escalation System (unchanged, uses central logger)
# -----------------------------------------------------------------------------
class AlertEscalationSystem:
    def __init__(self, config: UnifiedEcosystemConfig):
        self.config = config
        self.alerts: List[Dict] = []
        self.escalation_chains: Dict[str, List[Dict]] = {}
        self.alert_history: deque = deque(maxlen=1000)
        self._lock = asyncio.Lock()
        self._init_default_escalations()
        logger.info("AlertEscalationSystem initialized")

    def _init_default_escalations(self):
        self.escalation_chains = {
            'critical': [
                {'level': 'critical', 'action': 'notify_all', 'timeout': 0},
                {'level': 'escalated', 'action': 'call_manager', 'timeout': 300},
                {'level': 'emergency', 'action': 'system_override', 'timeout': 900}
            ],
            'warning': [
                {'level': 'warning', 'action': 'notify_team', 'timeout': 0},
                {'level': 'critical', 'action': 'notify_manager', 'timeout': 600},
                {'level': 'escalated', 'action': 'schedule_maintenance', 'timeout': 1800}
            ],
            'info': [
                {'level': 'info', 'action': 'log_alert', 'timeout': 0},
                {'level': 'warning', 'action': 'notify_team', 'timeout': 3600}
            ]
        }

    async def add_alert(self, alert: Dict[str, Any]) -> str:
        async with self._lock:
            alert_id = hashlib.md5(
                f"{alert.get('source')}_{datetime.utcnow().timestamp()}".encode()
            ).hexdigest()[:12]

            alert['alert_id'] = alert_id
            alert['timestamp'] = datetime.utcnow().isoformat()
            alert['status'] = 'active'
            alert['escalation_level'] = 0

            self.alerts.append(alert)
            self.alert_history.append(alert)

            asyncio.create_task(self._process_escalation(alert))
            return alert_id

    async def _process_escalation(self, alert: Dict):
        severity = alert.get('severity', 'info')
        chain = self.escalation_chains.get(severity, self.escalation_chains['info'])
        level = alert.get('escalation_level', 0)
        if level < len(chain):
            step = chain[level]
            await self._execute_escalation_action(alert, step)
            timeout = step.get('timeout', 0)
            if timeout > 0:
                await asyncio.sleep(timeout)
                async with self._lock:
                    if alert.get('status') == 'active':
                        alert['escalation_level'] = level + 1
                        asyncio.create_task(self._process_escalation(alert))

    async def _execute_escalation_action(self, alert: Dict, step: Dict):
        action = step.get('action')
        if action == 'notify_all':
            logger.warning(f"ALERT [{alert.get('severity')}]: {alert.get('message')}")
        elif action == 'call_manager':
            logger.error(f"ESCALATED ALERT: {alert.get('message')} - Manager notified")
        elif action == 'system_override':
            logger.critical(f"EMERGENCY OVERRIDE: {alert.get('message')}")
        elif action == 'notify_team':
            logger.warning(f"TEAM NOTIFICATION: {alert.get('message')}")
        elif action == 'notify_manager':
            logger.error(f"MANAGER NOTIFICATION: {alert.get('message')}")
        elif action == 'schedule_maintenance':
            logger.info(f"SCHEDULING MAINTENANCE for alert: {alert.get('message')}")
        elif action == 'log_alert':
            logger.info(f"ALERT LOGGED: {alert.get('message')}")

    async def resolve_alert(self, alert_id: str):
        async with self._lock:
            for alert in self.alerts:
                if alert.get('alert_id') == alert_id:
                    alert['status'] = 'resolved'
                    alert['resolved_at'] = datetime.utcnow().isoformat()
                    logger.info(f"Alert {alert_id} resolved")
                    break

    async def get_active_alerts(self) -> List[Dict]:
        async with self._lock:
            return [a for a in self.alerts if a.get('status') == 'active']

    async def get_alert_stats(self) -> Dict[str, Any]:
        async with self._lock:
            total = len(self.alerts)
            active = sum(1 for a in self.alerts if a.get('status') == 'active')
            resolved = sum(1 for a in self.alerts if a.get('status') == 'resolved')
            severities = defaultdict(int)
            for alert in self.alerts:
                severities[alert.get('severity', 'info')] += 1
            return {
                'total_alerts': total,
                'active_alerts': active,
                'resolved_alerts': resolved,
                'severity_distribution': dict(severities),
                'escalation_rates': {
                    severity: sum(1 for a in self.alerts if a.get('severity') == severity and a.get('escalation_level', 0) > 0)
                    for severity in severities
                }
            }

# -----------------------------------------------------------------------------
# Dynamic Reconfiguration System (unchanged)
# -----------------------------------------------------------------------------
class DynamicReconfigurationSystem:
    def __init__(self, config: UnifiedEcosystemConfig):
        self.config = config
        self.current_config: Dict[str, Any] = {
            'version': '1.0.0',
            'last_update': datetime.utcnow().isoformat(),
            'components': {}
        }
        self.config_history: List[Dict] = []
        self._lock = asyncio.Lock()
        self.reconfiguration_triggers: Dict[str, float] = {
            'low_sustainability': 0.4,
            'medium_sustainability': 0.6,
            'high_sustainability': 0.8
        }
        logger.info("DynamicReconfigurationSystem initialized")

    async def update_component_config(self, component_name: str, config: Dict):
        async with self._lock:
            self.current_config['components'][component_name] = config
            self.current_config['last_update'] = datetime.utcnow().isoformat()
            self.config_history.append({
                'timestamp': datetime.utcnow().isoformat(),
                'component': component_name,
                'config': config,
                'version': self.current_config['version']
            })
            logger.info(f"Updated configuration for {component_name}")

    async def reconfigure_by_metrics(self, metrics: Dict[str, float]):
        async with self._lock:
            sustainability_score = metrics.get('sustainability_score', 0.5)
            if sustainability_score < self.reconfiguration_triggers['low_sustainability']:
                self._apply_aggressive_reconfiguration()
            elif sustainability_score < self.reconfiguration_triggers['medium_sustainability']:
                self._apply_moderate_reconfiguration()
            else:
                self._apply_conservative_reconfiguration()
            self.current_config['version'] = f"{sustainability_score:.2f}_{datetime.utcnow().timestamp()}"

    def _apply_aggressive_reconfiguration(self):
        logger.info("Applying aggressive reconfiguration (low sustainability)")
        for comp in self.current_config['components']:
            self.current_config['components'][comp]['scale'] = 0.5
            self.current_config['components'][comp]['priority'] = 'reduced'

    def _apply_moderate_reconfiguration(self):
        logger.info("Applying moderate reconfiguration")
        for comp in self.current_config['components']:
            self.current_config['components'][comp]['scale'] = 0.8
            self.current_config['components'][comp]['priority'] = 'normal'

    def _apply_conservative_reconfiguration(self):
        logger.info("Applying conservative reconfiguration (high sustainability)")
        for comp in self.current_config['components']:
            self.current_config['components'][comp]['scale'] = 1.0
            self.current_config['components'][comp]['priority'] = 'optimized'

    async def get_current_config(self) -> Dict:
        async with self._lock:
            return self.current_config.copy()

    async def get_config_history(self, n: int = 10) -> List[Dict]:
        async with self._lock:
            return self.config_history[-n:]

# -----------------------------------------------------------------------------
# Sustainability Dashboard (unchanged, but uses central logger)
# -----------------------------------------------------------------------------
class UnifiedSustainabilityDashboard:
    def __init__(self, ecosystem: 'UnifiedMetabolicEcosystem'):
        self.ecosystem = ecosystem
        self.history = []
        self.alert_thresholds = {
            'sustainability_score': 0.5,
            'carbon_budget_remaining': 0.2,
            'helium_budget_remaining': 0.2,
            'circularity_score': 0.4
        }
        self._lock = asyncio.Lock()
        self._running = True
        self._monitor_task = asyncio.create_task(self._monitor_loop())
        logger.info("UnifiedSustainabilityDashboard initialized")

    async def _monitor_loop(self):
        while self._running:
            try:
                status = await self.get_dashboard_status()
                async with self._lock:
                    self.history.append(status)
                    if len(self.history) > 1000:
                        self.history = self.history[-1000:]
                await self._check_alerts(status)
                await asyncio.sleep(60)
            except Exception as e:
                logger.error(f"Monitor loop error: {e}")
                await asyncio.sleep(300)

    async def _check_alerts(self, status: Dict[str, Any]):
        alerts = []
        if status.get('sustainability_score', 0) < self.alert_thresholds['sustainability_score']:
            alerts.append({
                'level': 'warning',
                'message': f"Sustainability score {status['sustainability_score']:.2f} below threshold"
            })
        carbon_pos = status.get('carbon_position', {})
        carbon_remaining_ratio = carbon_pos.get('remaining_budget_ratio', 1.0)
        if carbon_remaining_ratio < self.alert_thresholds['carbon_budget_remaining']:
            alerts.append({
                'level': 'critical',
                'message': f"Carbon budget remaining {carbon_remaining_ratio:.1%} below threshold"
            })
        helium_pos = status.get('helium_position', {})
        helium_remaining_ratio = helium_pos.get('remaining_budget_ratio', 1.0)
        if helium_remaining_ratio < self.alert_thresholds['helium_budget_remaining']:
            alerts.append({
                'level': 'critical',
                'message': f"Helium budget remaining {helium_remaining_ratio:.1%} below threshold"
            })
        if status.get('circularity_score', 0) < self.alert_thresholds['circularity_score']:
            alerts.append({
                'level': 'warning',
                'message': f"Circularity score {status['circularity_score']:.2f} below threshold"
            })
        for alert in alerts:
            if alert['level'] == 'critical':
                logger.critical(f"DASHBOARD ALERT: {alert['message']}")
            else:
                logger.warning(f"DASHBOARD ALERT: {alert['message']}")
            if self.ecosystem.alert_system:
                await self.ecosystem.alert_system.add_alert({
                    'source': 'sustainability_dashboard',
                    'severity': alert['level'],
                    'message': alert['message']
                })

    async def get_dashboard_status(self) -> Dict[str, Any]:
        ecosystem = self.ecosystem
        carbon_pos = {}
        if hasattr(ecosystem, 'metrics') and ecosystem.metrics:
            metrics_summary = ecosystem.metrics.get_metrics_summary()
            carbon_pos = {
                'total_carbon_kg': metrics_summary.get('resource_consumption', {}).get('total_carbon_kg', 0),
                'carbon_per_inference': metrics_summary.get('resource_consumption', {}).get('carbon_per_inference', 0),
                'savings_kg': getattr(ecosystem.metrics, 'total_carbon_savings_kg', 0)
            }
        helium_pos = {}
        if hasattr(ecosystem, 'helium_tracker') and ecosystem.helium_tracker:
            pos = ecosystem.helium_tracker.get_helium_position()
            helium_pos = {
                'total_usage_l': pos.get('total_usage_l', 0),
                'total_recovered_l': pos.get('total_recovered_l', 0),
                'remaining_budget_l': pos.get('remaining_budget_l', 0),
                'remaining_budget_ratio': pos.get('remaining_budget_l', 0) / max(ecosystem.helium_tracker.helium_budget_l, 1)
            }
        sustainability_score = 0.5
        if hasattr(ecosystem, 'sustainability_score'):
            sustainability_score = ecosystem.sustainability_score
        circularity_score = 0.0
        if hasattr(ecosystem, 'circular_manager') and ecosystem.circular_manager:
            report = ecosystem.circular_manager.get_circularity_report()
            circularity_score = report.get('circularity_score', 0.0)
        ecosystem_health = 0.5
        if hasattr(ecosystem, 'health_system') and ecosystem.health_system:
            health_status = await ecosystem.health_system.get_system_health()
            ecosystem_health = health_status.get('system_score', 0.5)

        return {
            'timestamp': datetime.utcnow().isoformat(),
            'sustainability_score': sustainability_score,
            'carbon_position': carbon_pos,
            'helium_position': helium_pos,
            'circularity_score': circularity_score,
            'ecosystem_health': ecosystem_health,
            'expert_count': len(ecosystem.experts) if hasattr(ecosystem, 'experts') else 0,
            'is_healthy': all([
                sustainability_score > 0.3,
                carbon_pos.get('remaining_budget_ratio', 0) > 0.1,
                helium_pos.get('remaining_budget_ratio', 0) > 0.1
            ])
        }

    async def get_recommendations(self) -> List[Dict[str, Any]]:
        status = await self.get_dashboard_status()
        recommendations = []
        if status['sustainability_score'] < 0.5:
            recommendations.append({
                'priority': 'high',
                'category': 'sustainability',
                'message': 'Improve sustainability score through optimization',
                'actions': ['Reduce carbon intensity', 'Increase renewable energy usage']
            })
        if status['carbon_position'].get('remaining_budget_ratio', 1.0) < 0.2:
            recommendations.append({
                'priority': 'critical',
                'category': 'carbon',
                'message': 'Carbon budget critically low',
                'actions': ['Implement immediate carbon reduction', 'Purchase carbon offsets']
            })
        if status['helium_position'].get('remaining_budget_ratio', 1.0) < 0.2:
            recommendations.append({
                'priority': 'critical',
                'category': 'helium',
                'message': 'Helium budget critically low',
                'actions': ['Implement helium recovery systems', 'Optimize helium usage']
            })
        if status['circularity_score'] < 0.4:
            recommendations.append({
                'priority': 'medium',
                'category': 'circularity',
                'message': 'Improve circularity score',
                'actions': ['Increase component recycling', 'Extend hardware lifecycle']
            })
        return recommendations

    async def generate_report(self) -> Dict[str, Any]:
        status = await self.get_dashboard_status()
        recommendations = await self.get_recommendations()
        trend = 'stable'
        async with self._lock:
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
            'carbon_position': status['carbon_position'],
            'helium_position': status['helium_position'],
            'circularity_score': status['circularity_score'],
            'ecosystem_health': status['ecosystem_health'],
            'recommendations': recommendations,
            'is_healthy': status['is_healthy'],
            'generated_by': 'UnifiedSustainabilityDashboard'
        }

    async def shutdown(self):
        self._running = False
        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass
        logger.info("UnifiedSustainabilityDashboard shut down")

# -----------------------------------------------------------------------------
# Predictive Maintenance Integrator (unchanged, uses central logger)
# -----------------------------------------------------------------------------
class PredictiveMaintenanceIntegrator:
    def __init__(self, ecosystem: 'UnifiedMetabolicEcosystem'):
        self.ecosystem = ecosystem
        self.predictions: Dict[str, Any] = {}
        self.anomaly_history = deque(maxlen=1000)
        self._lock = asyncio.Lock()
        self._running = True
        self._predict_task = asyncio.create_task(self._predict_loop())
        logger.info("PredictiveMaintenanceIntegrator initialized")

    async def _predict_loop(self):
        while self._running:
            try:
                insights = await self.get_predictive_insights()
                async with self._lock:
                    self.predictions = insights
                await asyncio.sleep(300)
            except Exception as e:
                logger.error(f"Prediction loop error: {e}")
                await asyncio.sleep(600)

    async def get_predictive_insights(self) -> Dict[str, Any]:
        ecosystem = self.ecosystem
        insights = {
            'timestamp': datetime.utcnow().isoformat(),
            'lifecycle_predictions': {},
            'carbon_forecast': {},
            'helium_forecast': {},
            'workload_predictions': {},
            'anomaly_detections': []
        }
        if hasattr(ecosystem, 'circular_manager') and ecosystem.circular_manager:
            if hasattr(ecosystem.circular_manager, 'predictive_analyzer'):
                analyzer = ecosystem.circular_manager.predictive_analyzer
                if analyzer and analyzer.is_trained:
                    for component_id in list(ecosystem.circular_manager.components.keys())[:5]:
                        prediction = await analyzer.predict_lifetime({'age_days': 365, 'utilization': 0.5})
                        insights['lifecycle_predictions'][component_id] = prediction
        if hasattr(ecosystem, 'metrics') and ecosystem.metrics:
            if hasattr(ecosystem.metrics, 'predictive_analyzer'):
                forecast = await ecosystem.metrics.predictive_analyzer.predict_metric_trend()
                insights['carbon_forecast'] = {
                    'predicted_health': forecast.get('predicted_health', 0.5),
                    'confidence': forecast.get('confidence', 0.0),
                    'trend': forecast.get('trend', 'stable')
                }
        if hasattr(ecosystem, 'helium_tracker') and ecosystem.helium_tracker:
            helium_pos = ecosystem.helium_tracker.get_helium_position()
            insights['helium_forecast'] = {
                'current_position_l': helium_pos.get('net_position_l', 0),
                'remaining_budget_l': helium_pos.get('remaining_budget_l', 0),
                'days_remaining': helium_pos.get('remaining_budget_l', 0) / max(0.1, abs(helium_pos.get('net_position_l', 0) / 365))
            }
        if hasattr(ecosystem, 'work_integrator') and ecosystem.work_integrator:
            work_stats = ecosystem.work_integrator.get_work_statistics()
            insights['workload_predictions'] = {
                'active_works': work_stats.get('active_works', 0),
                'queued_works': work_stats.get('queued_works', 0),
                'success_rate': work_stats.get('success_rate', 0.5),
                'predicted_bottlenecks': ['energy'] if work_stats.get('active_works', 0) > 10 else []
            }
        if hasattr(ecosystem, 'metrics') and ecosystem.metrics:
            if hasattr(ecosystem.metrics, 'anomaly_detector'):
                detection_stats = ecosystem.metrics.anomaly_detector.get_detection_stats()
                for detection in detection_stats.get('recent_detections', [])[-10:]:
                    insights['anomaly_detections'].append({
                        'metric': detection.get('metric', 'unknown'),
                        'type': detection.get('type', 'unknown'),
                        'severity': detection.get('severity', 'info'),
                        'timestamp': detection.get('timestamp', datetime.utcnow().isoformat())
                    })
        return insights

    async def get_anomaly_alerts(self, severity: Optional[str] = None) -> List[Dict[str, Any]]:
        async with self._lock:
            alerts = [a for a in self.predictions.get('anomaly_detections', [])
                     if severity is None or a.get('severity') == severity]
            return alerts

    async def get_lifecycle_recommendations(self) -> List[Dict[str, Any]]:
        recommendations = []
        async with self._lock:
            for component_id, prediction in self.predictions.get('lifecycle_predictions', {}).items():
                predicted_days = prediction.get('predicted_days', 365)
                if predicted_days < 30:
                    recommendations.append({
                        'component_id': component_id,
                        'priority': 'critical',
                        'action': 'Immediate replacement recommended',
                        'predicted_remaining_days': predicted_days
                    })
                elif predicted_days < 90:
                    recommendations.append({
                        'component_id': component_id,
                        'priority': 'high',
                        'action': 'Plan for replacement soon',
                        'predicted_remaining_days': predicted_days
                    })
                elif predicted_days < 180:
                    recommendations.append({
                        'component_id': component_id,
                        'priority': 'medium',
                        'action': 'Consider maintenance',
                        'predicted_remaining_days': predicted_days
                    })
        return recommendations

    async def shutdown(self):
        self._running = False
        if self._predict_task:
            self._predict_task.cancel()
            try:
                await self._predict_task
            except asyncio.CancelledError:
                pass
        logger.info("PredictiveMaintenanceIntegrator shut down")

# -----------------------------------------------------------------------------
# Core Unified Metabolic Ecosystem – Fully Integrated
# -----------------------------------------------------------------------------
class UnifiedMetabolicEcosystem:
    """
    Central Nervous Control Plane for Green Agent MoE Expert System.
    Orchestrates routing, carbon-aware signal transduction, health loops, and resilience.
    Fully integrated with Green Agent MOPD ecosystem.
    """

    def __init__(
        self,
        storage: Storage,
        message_queue: AsyncMessageQueue,
        adaptive_cost: AdaptiveCostFunction,
        pareto_gating: ParetoGating,
        drift_detector: DriftDetector,
        metrics: MetricsRegistry
    ):
        self.storage = storage
        self.queue = message_queue
        self.adaptive_cost = adaptive_cost
        self.pareto = pareto_gating
        self.drift = drift_detector
        self.metrics = metrics

        self.config = UnifiedEcosystemConfig()
        self.sustainability_score: float = 1.0

        # Rate limiter
        self.rate_limiter = RateLimiter(self.config.rate_limit_per_minute)

        # Health & Healing (optional)
        self.health_system = HealthCheckSystem(self.config) if self.config.enable_health_checks else None
        self.self_healing = SelfHealingSystem(self.config, self.health_system) if (self.config.enable_health_checks and self.config.enable_self_healing) else None
        self.alert_system = AlertEscalationSystem(self.config) if self.config.enable_alert_escalation else None
        self.reconfig_system = DynamicReconfigurationSystem(self.config) if self.config.enable_dynamic_reconfig else None

        # Expert Registry
        self.registry = ExpertRegistry(enable_genetics=self.config.enable_bio_inspired)
        self.router = ExpertRouter(enable_quantum=self.config.enable_quantum, enable_signal_transduction=self.config.enable_bio_inspired)

        # Experts
        self.experts: Dict[str, Any] = {}
        self._init_experts()

        # Gating Network
        self.gating_network = MoEGatingNetwork(num_experts=len(self.experts), enable_bio_integration=self.config.enable_bio_inspired)

        # Connect router and gating
        for idx, expert_id in enumerate(self.experts.keys()):
            self.router.expert_index_map[idx] = expert_id
            self.router.experts[expert_id] = self.experts[expert_id]
            self.router.circuit_breakers[expert_id] = ExpertCircuitBreaker(expert_id=expert_id)
            self.gating_network.expert_index_map[idx] = expert_id

        # Advanced modules
        self.evolving_gates = None
        self.federated = None
        self.cross_region = None
        if self.config.enable_evolving_gates and EVOLVING_GATES_AVAILABLE:
            self.evolving_gates = EnhancedSelfEvolvingGate(num_experts=len(self.experts))
        if self.config.enable_federated and FEDERATED_AVAILABLE:
            self.federated = EnhancedFederatedOrchestrator()
        if self.config.enable_cross_region and CROSS_REGION_AVAILABLE:
            self.cross_region = CrossRegionFederationOptimizer()

        # Integration layers
        self.layer_integrator = EnhancedLayerIntegrator() if LAYER_INTEGRATOR_AVAILABLE else None
        self.work_integrator = EnhancedWorkIntegrator(self.router) if WORK_INTEGRATOR_AVAILABLE else None
        self.quantum_limits = QuantumLimitGraphIntegrator() if QUANTUM_LIMIT_INTEGRATOR_AVAILABLE else None

        # Monitoring
        self.metrics_collector = ExpertMetricsCollector() if METRICS_COLLECTOR_AVAILABLE else None

        # Sustainability modules
        self.carbon_manager = CarbonSequestrationManager() if CARBON_SEQUESTRATION_AVAILABLE else None
        self.circular_manager = CircularComputingManager() if CIRCULAR_COMPUTING_AVAILABLE else None
        self.offset_verifier = AutomatedCarbonOffsetVerification() if CARBON_OFFSET_AVAILABLE else None
        self.biodiversity = BiodiversityImpactAssessor() if BIODIVERSITY_AVAILABLE else None

        # Digital Twin & Sustainability Engine
        self.digital_twin = None
        self.sustainability_engine = None
        if self.config.enable_digital_twin and DIGITAL_TWIN_AVAILABLE:
            twin_config = DigitalTwinConfig(
                time_horizon_years=self.config.twin_time_horizon_years,
                n_simulations=self.config.twin_n_simulations,
                confidence_level=self.config.twin_confidence
            )
            self.digital_twin = SystemDigitalTwin(twin_config)
        if self.config.enable_unified_sustainability and SUSTAINABILITY_ENGINE_AVAILABLE:
            self.sustainability_engine = UnifiedSustainabilityEngine()

        # Bio-inspired core
        self.bio_core = None
        self.bio_available = False
        if self.config.enable_bio_inspired and BIO_INSPIRED_AVAILABLE:
            from enhancements.bio_inspired import BioInspiredGreenCore
            self.bio_core = BioInspiredGreenCore()
            self.bio_available = True

        # Sustainability Dashboard & Predictive Maintenance
        self.sustainability_dashboard = UnifiedSustainabilityDashboard(self) if self.config.enable_sustainability_dashboard else None
        self.predictive_maintenance = PredictiveMaintenanceIntegrator(self) if self.config.enable_predictive_maintenance else None

        # Register health checks
        if self.health_system:
            for name, comp in [
                ('registry', self.registry),
                ('gating', self.gating_network),
                ('router', self.router),
                ('metrics', self.metrics_collector),
                ('work_integrator', self.work_integrator),
                ('layer_integrator', self.layer_integrator),
                ('quantum_limits', self.quantum_limits)
            ]:
                if comp:
                    self.health_system.register_component(name, comp)
            self.health_system.start()

        if self.self_healing:
            self.self_healing.register_recovery_handler('router', self._recover_router)
            self.self_healing.start()

        # Load state from central storage
        asyncio.create_task(self._load_state())

        # Start background tasks
        self._bg_tasks = []
        if self.config.enable_health_checks:
            self._bg_tasks.append(asyncio.create_task(self._carbon_update_loop()))
        if self.config.enable_sustainability_dashboard:
            self._bg_tasks.append(asyncio.create_task(self._dashboard_monitor_loop()))

        logger.info("UnifiedMetabolicEcosystem v7.0.0 initialized successfully.")

    # --------------------------------------------------------------------------
    # Expert Initialization
    # --------------------------------------------------------------------------
    def _init_experts(self):
        self.experts['energy'] = EnergyExpert()
        self.experts['data'] = DataExpert()
        self.experts['iot'] = IoTExpert()
        if self.config.enable_quantum and QUANTUM_AVAILABLE:
            self.experts['quantum'] = QuantumExpert()
        if self.config.enable_helium and HELIUM_AVAILABLE:
            self.experts['helium'] = HeliumExpert()
        # Register experts with registry
        for eid, expert in self.experts.items():
            if hasattr(expert, 'profile'):
                self.registry.register_expert(expert.profile, validate=False, auto_certify=True)

    # --------------------------------------------------------------------------
    # State Persistence using central Storage
    # --------------------------------------------------------------------------
    async def _load_state(self):
        try:
            data = self.storage.get_state("moe_ecosystem_state")
            if data:
                state = json.loads(data)
                self.sustainability_score = state.get("sustainability_score", 1.0)
                # Restore gating network if possible
                gating_state = state.get("gating_state")
                if gating_state and hasattr(self.gating_network, 'load_state_dict'):
                    self.gating_network.load_state_dict(gating_state)
                logger.info("Loaded MoE ecosystem state from storage")
        except Exception as e:
            logger.error(f"Failed to load ecosystem state: {e}")

    async def save_state(self):
        try:
            state = {
                "sustainability_score": self.sustainability_score,
                "gating_state": self.gating_network.get_state_dict() if hasattr(self.gating_network, 'get_state_dict') else {},
            }
            self.storage.save_state("moe_ecosystem_state", json.dumps(state))
            logger.info("Saved MoE ecosystem state to storage")
        except Exception as e:
            logger.error(f"Failed to save ecosystem state: {e}")

    # --------------------------------------------------------------------------
    # Recovery Handler
    # --------------------------------------------------------------------------
    async def _recover_router(self) -> bool:
        logger.info("Attempting to recover expert router...")
        await asyncio.sleep(0.5)
        return True

    # --------------------------------------------------------------------------
    # Carbon Update Loop
    # --------------------------------------------------------------------------
    async def _carbon_update_loop(self):
        while True:
            try:
                # Use central carbon manager if available; otherwise stub
                # This is a placeholder; actual carbon manager integration would be separate
                await asyncio.sleep(self.config.carbon_update_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Carbon update loop error: {e}")
                await asyncio.sleep(60)

    # --------------------------------------------------------------------------
    # Dashboard Monitor Loop
    # --------------------------------------------------------------------------
    async def _dashboard_monitor_loop(self):
        while True:
            try:
                if self.sustainability_dashboard:
                    await self.sustainability_dashboard.get_dashboard_status()
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Dashboard monitor loop error: {e}")
                await asyncio.sleep(60)

    # --------------------------------------------------------------------------
    # Teacher Interface for MOPD
    # --------------------------------------------------------------------------
    async def policy_probs(self, state: Dict[str, Any]) -> List[float]:
        """
        Return a probability distribution over experts.
        This allows the MTPD optimizer to treat this module as a teacher.
        """
        return await self.gating_network.predict(state)

    # --------------------------------------------------------------------------
    # Core Task Processing
    # --------------------------------------------------------------------------
    async def process_task(self, task: Dict[str, Any], pipeline_type: str = 'standard') -> Dict[str, Any]:
        start_time = time.monotonic()

        # Rate limiting
        if not await self.rate_limiter.acquire():
            self.metrics.increment("rate_limit_exceeded")
            return {'success': False, 'error': 'Rate limit exceeded'}

        # Validate input if Pydantic available
        if BaseModel is not None:
            try:
                task_input = TaskInput(**task)
                task = task_input.model_dump()
            except ValidationError as e:
                return {'success': False, 'error': f'Invalid task: {e}'}

        self.metrics.increment("tasks_received")

        try:
            # Enrich context (stub – real enrichment would come from external sources)
            context = task.get('context', {})

            # Get gating weights
            weights = await self.gating_network.predict(context)

            # Apply Pareto gating to filter experts
            if self.pareto:
                candidates = []
                for eid, weight in weights.items():
                    expert = self.experts[eid]
                    health = await expert.get_health_status()
                    candidates.append({
                        'expert_id': eid,
                        'quality_score': weight,
                        'carbon_g': 0.0,
                        'latency_ms': 0.0,
                        'energy_joules': 0.0,
                        'health_score': health.get('score', 1.0)
                    })
                filtered = self.pareto.filter(candidates)
                if filtered:
                    allowed_ids = {c['expert_id'] for c in filtered}
                    for eid in list(weights.keys()):
                        if eid not in allowed_ids:
                            weights[eid] = 0.0

            # Normalize
            total = sum(weights.values())
            if total > 0:
                weights = {k: v / total for k, v in weights.items()}
            else:
                weights = {eid: 1.0 / len(self.experts) for eid in self.experts}

            # Select expert
            selected_expert_id = max(weights, key=weights.get)
            selected_expert = self.experts[selected_expert_id]

            # Expert health guard
            exp_health = await selected_expert.get_health_status()
            if exp_health.get("status") == "unhealthy":
                logger.warning(f"Target expert {selected_expert.name} unhealthy. Rerouting...")
                selected_expert = self.experts["data"]

            # Execute task
            execution_res = await selected_expert.execute(task.get('params', {}), context)

            # Update sustainability score (stub)
            self.sustainability_score = 0.8  # placeholder

            elapsed = time.monotonic() - start_time

            # Update central metrics
            self.metrics.increment("tasks_completed_success")
            self.metrics.observe("task_latency_seconds", elapsed)
            self.metrics.set_sustainability_score(self.sustainability_score)

            # Publish FeedbackEvent
            event = FeedbackEvent.create_with_context(
                task_id=f"moe_{hashlib.sha256(json.dumps(context).encode()).hexdigest()[:8]}",
                selected_action=selected_expert.name,
                quality_score=weights[selected_expert_id],
                energy_joules=0.0,
                carbon_g=0.0,
                feedback_type="moe_routing",
                adaptive_cost_value=0.0,
                state={'task_type': task.get('type', 'generic'), 'context': context},
                candidates=[{'expert': eid, 'weight': w} for eid, w in weights.items()],
                source="green_agent_moe",
                environment=getattr(central_config, "ENVIRONMENT", "production"),
                tags=["moe", "routing"]
            )
            await self.queue.publish("feedback_events", event.to_json())

            # Check drift
            if self.drift:
                await self.drift.check_drift(self.adaptive_cost.get_current_weights())

            return {
                "success": True,
                "route": {
                    "assigned_expert": selected_expert.name,
                    "domain": selected_expert.domain,
                    "weight": weights[selected_expert_id]
                },
                "execution": execution_res,
                "sustainability_score": round(self.sustainability_score, 4),
                "latency_ms": round(elapsed * 1000, 2)
            }

        except Exception as e:
            logger.error(f"Error processing task: {e}", exc_info=True)
            self.metrics.increment("task_failures")
            if self.alert_system:
                await self.alert_system.add_alert({
                    'source': 'moe_processor',
                    'severity': 'error',
                    'message': f"Task processing failure: {str(e)}"
                })
            return {"success": False, "error": str(e)}

    # --------------------------------------------------------------------------
    # Health Check Endpoint
    # --------------------------------------------------------------------------
    async def health_check(self) -> Dict[str, Any]:
        status = {
            "version": "7.0.0",
            "timestamp": datetime.utcnow().isoformat(),
            "sustainability_score": self.sustainability_score,
            "expert_count": len(self.experts),
            "gating_trained": self.gating_network.is_trained,
            "circuit_breaker_state": self.router.get_circuit_breaker_state() if hasattr(self.router, 'get_circuit_breaker_state') else 'unknown'
        }
        if self.health_system:
            status["system_health"] = await self.health_system.get_system_health()
        # Update central metrics
        self.metrics.set_expert_count(len(self.experts))
        self.metrics.set_sustainability_score(self.sustainability_score)
        return status

    # --------------------------------------------------------------------------
    # Shutdown
    # --------------------------------------------------------------------------
    async def shutdown(self):
        logger.info("Initiating system shutdown sequence...")
        for task in self._bg_tasks:
            task.cancel()
        await asyncio.gather(*self._bg_tasks, return_exceptions=True)
        if self.health_system:
            await self.health_system.shutdown()
        if self.self_healing:
            await self.self_healing.shutdown()
        if self.sustainability_dashboard:
            await self.sustainability_dashboard.shutdown()
        if self.predictive_maintenance:
            await self.predictive_maintenance.shutdown()
        if self.digital_twin:
            await self.digital_twin.shutdown()
        await self.save_state()
        logger.info("UnifiedMetabolicEcosystem shutdown complete.")

# -----------------------------------------------------------------------------
# Example Usage (if run directly)
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)

    async def main():
        from ..storage import Storage
        from ..scaling.message_queue import AsyncMessageQueue
        from ..feedback.adaptive_cost import AdaptiveCostFunction
        from ..routing.pareto_gating import ParetoGating
        from ..safety.drift_detector import DriftDetector
        from ..metrics import MetricsRegistry

        storage = Storage()
        queue = AsyncMessageQueue()
        adaptive_cost = AdaptiveCostFunction(storage)
        pareto = ParetoGating()
        drift = DriftDetector(storage, adaptive_cost)
        metrics = MetricsRegistry()

        ecosystem = UnifiedMetabolicEcosystem(storage, queue, adaptive_cost, pareto, drift, metrics)

        task = {"type": "energy_optimization", "params": {"grid_target": "renewable_solar"}}
        result = await ecosystem.process_task(task)
        print(json.dumps(result, indent=2))

        health = await ecosystem.health_check()
        print(json.dumps(health, indent=2))

        await ecosystem.shutdown()

    asyncio.run(main())
