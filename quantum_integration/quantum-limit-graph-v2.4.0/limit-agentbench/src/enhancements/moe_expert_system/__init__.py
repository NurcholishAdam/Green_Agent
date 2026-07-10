"""
Green Agent MoE Expert System v6.1.0 - Unified Metabolic Ecosystem with Enhanced Resilience

ENHANCED WITH: System Digital Twin, Unified Sustainability Engine, Health Checks, Self-Healing,
Alert Escalation, Dynamic Reconfiguration, Telemetry Export, and State Persistence.

Complete integration with bio-inspired modules providing:
- Eco-ATP currency system for unified resource accounting
- Proton gradient fields for distributed potential accumulation
- ATP synthase scheduling for energy-driven task dispatching
- Chromatophore compartments for modular expert isolation
- Biomass storage for deferred computation queuing
- Photosynthetic harvesting for environmental opportunity detection
- Unified Sustainability Dashboard (Ecosystem Health Monitor)
- Predictive Maintenance Integration (Future State Predictor)
- System Digital Twin (Strategic Simulation Engine)
- Unified Sustainability Engine (Authoritative Global Score)
- Health Checks and Self-Healing
- Dynamic Reconfiguration
- Alert Escalation and Automated Response
- Telemetry Export (Prometheus)
- State Persistence

This module serves as the central nervous system connecting:
- Expert Registry (Genome Repository)
- Gating Network (Allosteric Enzyme System)
- Expert Router (Signal Transduction Cascade)
- All specialized experts (Metabolic Organs)
- Monitoring system (Metabolic Observatory)
- Sustainability Dashboard (Ecosystem Health Monitor)
- Predictive Analytics (Future State Predictor)
- Digital Twin (Strategic Simulator)
- Sustainability Engine (Valuation Core)
"""

import logging
from typing import Dict, Any, List, Optional, Tuple, Union, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import asyncio
import threading
import numpy as np
from collections import deque, defaultdict
import importlib
import json
import hashlib
import os
import pickle
import zlib
import time
import random
from enum import Enum

logger = logging.getLogger(__name__)

# ============================================================================
# Configuration Dataclass (NEW)
# ============================================================================

@dataclass
class UnifiedEcosystemConfig:
    """Centralized configuration for the Unified Metabolic Ecosystem."""
    # Feature flags
    enable_quantum: bool = False
    enable_helium: bool = False
    enable_bio_inspired: bool = True
    enable_evolving_gates: bool = True
    enable_federated: bool = False
    enable_cross_region: bool = False
    enable_sustainability_dashboard: bool = True
    enable_predictive_maintenance: bool = True
    enable_digital_twin: bool = True
    enable_unified_sustainability: bool = True
    enable_health_checks: bool = True
    enable_self_healing: bool = True
    enable_alert_escalation: bool = True
    enable_dynamic_reconfig: bool = True
    enable_telemetry: bool = True
    enable_persistence: bool = True

    # Tunable parameters
    twin_time_horizon_years: int = 10
    twin_n_simulations: int = 1000
    twin_confidence: float = 0.95
    health_check_interval: int = 60
    recovery_max_attempts: int = 5
    persistence_path: str = "ecosystem_state.pkl"
    telemetry_export_interval: int = 60
    alert_escalation_timeout: int = 300

    def __post_init__(self):
        # Ensure boolean flags
        for key, value in self.__dict__.items():
            if isinstance(value, bool):
                setattr(self, key, bool(value))

# ============================================================================
# Bio-Inspired Module Availability Check
# ============================================================================

BIO_INSPIRED_AVAILABLE = False
try:
    from enhancements.bio_inspired import (
        EcoATPTokenManager,
        DynamicExchangeRate,
        GradientFieldManager,
        ATPSynthaseScheduler,
        CompartmentManager,
        BiomassStorage,
        PhotosyntheticHarvester,
        BioInspiredGreenCore,
        BioInspiredMoEIntegrator
    )
    BIO_INSPIRED_AVAILABLE = True
    logger.info("Bio-inspired modules available for MoE Expert System integration")
except ImportError as e:
    logger.info(f"Bio-inspired modules not available: {str(e)} - using standard MoE system")

# ============================================================================
# Digital Twin and Sustainability Engine Imports
# ============================================================================

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
    logger.info("System Digital Twin available")
except ImportError as e:
    logger.info(f"System Digital Twin not available: {str(e)}")

try:
    from enhancements.sustainability.unified_sustainability_engine import (
        UnifiedSustainabilityEngine,
        UnifiedSustainabilityScore,
        SustainabilityDimension,
        SustainabilityThreshold
    )
    SUSTAINABILITY_ENGINE_AVAILABLE = True
    logger.info("Unified Sustainability Engine available")
except ImportError as e:
    logger.info(f"Unified Sustainability Engine not available: {str(e)}")

# ============================================================================
# Core MoE Components
# ============================================================================

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

from .gating_network import (
    MoEGatingNetwork,
    GatingContext,
    EnhancedSparseMoEGate
)

from .expert_router import (
    ExpertRouter,
    RoutingMetrics,
    ExpertCircuitBreaker,
    CircuitBreakerState,
    SignalTransductionEngine,
    AllostericRegulationSystem,
    MetabolicPathwayRouter
)

# ============================================================================
# Specialized Experts (Metabolic Organs)
# ============================================================================

from .experts.energy_expert import (
    EnergyExpert,
    EnergySource,
    PowerState,
    CoolingMethod,
    RenewableProfile,
    ThermalProfile
)

from .experts.data_expert import (
    DataExpert,
    DataTier,
    DataQuality,
    DataQualityMetrics,
    DataLineage,
    DataStream,
    StreamingMode,
    PipelineStatus
)

from .experts.iot_expert import (
    IoTExpert,
    DeviceType,
    ConnectionType,
    EnergySource as IoTEnergySource,
    ProcessingMode,
    MeshRole,
    EdgeDevice,
    MeshNetwork
)

# ============================================================================
# Optional Experts
# ============================================================================

QUANTUM_AVAILABLE = False
try:
    from .experts.quantum_expert import QuantumExpert
    QUANTUM_AVAILABLE = True
except ImportError:
    logger.info("Quantum Expert not available")

HELIUM_AVAILABLE = False
try:
    from .experts.helium_expert import HeliumExpert
    HELIUM_AVAILABLE = True
except ImportError:
    logger.info("Helium Expert not available")

# ============================================================================
# Advanced Modules
# ============================================================================

EVOLVING_GATES_AVAILABLE = False
try:
    from .advanced.self_evolving_gates import (
        EnhancedSelfEvolvingGate,
        SelfEvolvingGate
    )
    EVOLVING_GATES_AVAILABLE = True
except ImportError:
    logger.info("Self-Evolving Gates not available")

FEDERATED_AVAILABLE = False
try:
    from .advanced.federated_experts import (
        EnhancedFederatedOrchestrator,
        FederatedExpert
    )
    FEDERATED_AVAILABLE = True
except ImportError:
    logger.info("Federated Learning not available")

CROSS_REGION_AVAILABLE = False
try:
    from .advanced.cross_region_federation import (
        CrossRegionFederationOptimizer,
        Region,
        SyncMode,
        AggregationTier
    )
    CROSS_REGION_AVAILABLE = True
except ImportError:
    logger.info("Cross-Region Federation not available")

# ============================================================================
# Integration Modules
# ============================================================================

from .integration.layer_integrator import (
    EnhancedLayerIntegrator,
    LayerIntegrator,
    LayerInfo,
    LayerStatus,
    IntegrationMode,
    CircuitState
)

from .integration.enhanced_work_integration import (
    EnhancedWorkIntegrator,
    EnhancedWorkContext,
    WorkState,
    WorkPriority,
    WorkSLA,
    SLALevel
)

from .integration.quantum_limit_integration import (
    QuantumLimitGraphIntegrator,
    QuantumBackend,
    QuantumAlgorithm,
    QuantumErrorMitigation,
    QuantumResource,
    QuantumCircuitJob,
    AdaptiveBoundary,
    QuantumNode
)

# ============================================================================
# Monitoring Module
# ============================================================================

from .monitoring.expert_metrics import (
    ExpertMetricsCollector,
    MetricSeverity,
    MetricType,
    AnomalyType,
    SLOStatus,
    MetricThreshold,
    ServiceLevelObjective,
    AnomalyEvent,
    CostAttribution
)

# ============================================================================
# Sustainability Modules
# ============================================================================

BIODIVERSITY_AVAILABLE = False
try:
    from .sustainability.biodiversity_impact import (
        BiodiversityImpactAssessor,
        EcosystemType,
        ImpactCategory,
        BiodiversityMetric
    )
    BIODIVERSITY_AVAILABLE = True
except ImportError:
    logger.info("Biodiversity Impact Assessor not available")

SEQUESTRATION_AVAILABLE = False
try:
    from .sustainability.carbon_sequestration import (
        CarbonSequestrationManager,
        CarbonCredit
    )
    SEQUESTRATION_AVAILABLE = True
except ImportError:
    logger.info("Carbon Sequestration Manager not available")

CIRCULAR_AVAILABLE = False
try:
    from .sustainability.circular_computing import (
        CircularComputingManager,
        HardwareComponent,
        HardwareState,
        MaterialType
    )
    CIRCULAR_AVAILABLE = True
except ImportError:
    logger.info("Circular Computing Manager not available")

OFFSET_AVAILABLE = False
try:
    from .sustainability.carbon_offset_verification import (
        AutomatedCarbonOffsetVerification,
        OffsetRegistry,
        ProjectType,
        VerificationStatus
    )
    OFFSET_AVAILABLE = True
except ImportError:
    logger.info("Carbon Offset Verification not available")

# ============================================================================
# Telemetry Collector (NEW)
# ============================================================================

class TelemetryCollector:
    """
    Simple telemetry collector for exporting metrics (Prometheus-style).
    """

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

class EcosystemPersistenceManager:
    """
    Manages saving and loading of the ecosystem state.
    """

    def __init__(self, config: UnifiedEcosystemConfig):
        self.config = config
        self.path = config.persistence_path
        self._lock = asyncio.Lock()
        logger.info(f"EcosystemPersistenceManager initialized (path={self.path})")

    async def save_state(self, ecosystem: 'UnifiedMetabolicEcosystem'):
        async with self._lock:
            try:
                state = {
                    'sustainability_score': ecosystem.sustainability_score,
                    'registry_state': ecosystem.registry.get_registry_stats(),
                    'router_state': ecosystem.router.get_routing_stats(),
                    'config': ecosystem.config,
                    # Additional state can be added as needed
                }
                serialized = pickle.dumps(state)
                compressed = zlib.compress(serialized)
                with open(self.path, 'wb') as f:
                    f.write(compressed)
                logger.info(f"Ecosystem state saved to {self.path}")
                return True
            except Exception as e:
                logger.error(f"Failed to save ecosystem state: {e}")
                return False

    async def load_state(self, ecosystem: 'UnifiedMetabolicEcosystem') -> bool:
        async with self._lock:
            if not os.path.exists(self.path):
                logger.warning(f"Persistence file {self.path} not found")
                return False
            try:
                with open(self.path, 'rb') as f:
                    compressed = f.read()
                serialized = zlib.decompress(compressed)
                state = pickle.loads(serialized)

                # Restore simple attributes
                ecosystem.sustainability_score = state.get('sustainability_score', 0.0)
                # Restore more complex state as needed (e.g., registry, router)
                # For simplicity, we assume the registry and router are already initialized.
                logger.info(f"Ecosystem state loaded from {self.path}")
                return True
            except Exception as e:
                logger.error(f"Failed to load ecosystem state: {e}")
                return False

    async def delete_state(self):
        async with self._lock:
            if os.path.exists(self.path):
                os.remove(self.path)
                logger.info(f"Persistence file {self.path} deleted")
                return True
            return False

# ============================================================================
# Enhanced Health Check System (Asyncio-based)
# ============================================================================

class HealthCheckSystem:
    """
    Asyncio-based health check system for ecosystem components.
    """

    def __init__(self, config: UnifiedEcosystemConfig):
        self.config = config
        self.component_health: Dict[str, Dict] = {}
        self.health_history: Dict[str, List[Dict]] = defaultdict(list)
        self._lock = asyncio.Lock()
        self._running = True
        self._check_task: Optional[asyncio.Task] = None
        self._start_health_check_loop()
        logger.info("HealthCheckSystem initialized (asyncio)")

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
                # Delegate health check to component if it provides a health method
                try:
                    if hasattr(component, 'get_health_status'):
                        health_result = await component.get_health_status()
                        data['status'] = health_result.get('status', 'unknown')
                        data['score'] = health_result.get('score', 0.5)
                    else:
                        # Fallback: call a generic health check method or use default
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
        # Simulate a generic health check
        # Could be overridden per component
        return random.choice(['healthy', 'degraded', 'unhealthy']) if random.random() > 0.3 else 'healthy'

    def _calculate_default_health(self, component_name: str) -> float:
        # Simulate a health score
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

# ============================================================================
# Enhanced Self-Healing System (Asyncio-based)
# ============================================================================

class SelfHealingSystem:
    """
    Asyncio-based self-healing system with component-specific recovery strategies.
    """

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
        logger.info("SelfHealingSystem initialized (asyncio)")

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

            # Execute recovery handler if registered
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
                # Fallback: generic restart
                success = await self._generic_restart(component_name)

            self.failure_history[component_name].append({
                'timestamp': datetime.utcnow().isoformat(),
                'attempt': self.recovery_attempts[component_name],
                'success': success
            })

            if success:
                logger.info(f"Successfully recovered component: {component_name}")
                # Update health system
                if self.health_system:
                    async with self.health_system._lock:
                        if component_name in self.health_system.component_health:
                            self.health_system.component_health[component_name]['status'] = 'healthy'
                            self.health_system.component_health[component_name]['score'] = 1.0
            else:
                logger.warning(f"Failed to recover component: {component_name} (attempt {self.recovery_attempts[component_name]})")

    async def _generic_restart(self, component_name: str) -> bool:
        # Simulate restart
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

# ============================================================================
# Enhanced Alert Escalation System (Asyncio-based)
# ============================================================================

class AlertEscalationSystem:
    """
    Asyncio-based alert escalation and automated response system.
    """

    def __init__(self, config: UnifiedEcosystemConfig):
        self.config = config
        self.alerts: List[Dict] = []
        self.escalation_chains: Dict[str, List[Dict]] = {}
        self.alert_history: deque = deque(maxlen=1000)
        self._lock = asyncio.Lock()
        self._init_default_escalations()
        logger.info("AlertEscalationSystem initialized (asyncio)")

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

            # Start escalation asynchronously
            asyncio.create_task(self._process_escalation(alert))
            return alert_id

    async def _process_escalation(self, alert: Dict):
        severity = alert.get('severity', 'info')
        chain = self.escalation_chains.get(severity, self.escalation_chains['info'])
        level = alert.get('escalation_level', 0)
        if level < len(chain):
            step = chain[level]
            await self._execute_escalation_action(alert, step)
            # Schedule next escalation if timeout > 0
            timeout = step.get('timeout', 0)
            if timeout > 0:
                await asyncio.sleep(timeout)
                # Check if alert is still active before escalating
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

# ============================================================================
# Enhanced Dynamic Reconfiguration System (Data-driven)
# ============================================================================

class DynamicReconfigurationSystem:
    """
    Dynamic reconfiguration based on sustainability metrics and telemetry.
    """

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
        """Reconfigure based on comprehensive metrics."""
        async with self._lock:
            # Compute a weighted score from metrics
            sustainability_score = metrics.get('sustainability_score', 0.5)
            carbon_efficiency = metrics.get('carbon_efficiency', 0.5)
            helium_efficiency = metrics.get('helium_efficiency', 0.5)
            # More sophisticated logic can be added
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

# ============================================================================
# Enhanced Bio-Inspired Integrator (Updated)
# ============================================================================

class EnhancedBioInspiredIntegrator:
    """
    Enhanced Bio-Inspired Integration for sustainability across all components.
    """

    def __init__(self, bio_core=None):
        self.bio_core = bio_core
        self.sustainability_core = None
        self.component_registry = {}
        self._lock = asyncio.Lock()
        logger.info("EnhancedBioInspiredIntegrator initialized")

    def inject_sustainability_core(self, sustainability_core: Any):
        self.sustainability_core = sustainability_core
        asyncio.create_task(self._inject_to_all_components())

    async def _inject_to_all_components(self):
        async with self._lock:
            for component_name, component in self.component_registry.items():
                if hasattr(component, 'inject_sustainability_core'):
                    try:
                        if asyncio.iscoroutinefunction(component.inject_sustainability_core):
                            await component.inject_sustainability_core(self.sustainability_core)
                        else:
                            component.inject_sustainability_core(self.sustainability_core)
                        logger.debug(f"Injected sustainability core into {component_name}")
                    except Exception as e:
                        logger.warning(f"Failed to inject into {component_name}: {e}")

    def register_component(self, component_name: str, component: Any):
        async with self._lock:
            self.component_registry[component_name] = component
            if self.sustainability_core and hasattr(component, 'inject_sustainability_core'):
                # Will be injected in the background loop
                pass

    async def get_sustainability_status(self) -> Dict[str, Any]:
        status = {'timestamp': datetime.utcnow().isoformat(), 'components': {}}
        async with self._lock:
            for component_name, component in self.component_registry.items():
                if hasattr(component, 'get_sustainability_status'):
                    try:
                        if asyncio.iscoroutinefunction(component.get_sustainability_status):
                            status['components'][component_name] = await component.get_sustainability_status()
                        else:
                            status['components'][component_name] = component.get_sustainability_status()
                    except Exception as e:
                        status['components'][component_name] = {'error': str(e)}
                elif hasattr(component, 'sustainability_score'):
                    status['components'][component_name] = {
                        'sustainability_score': getattr(component, 'sustainability_score', 0.0)
                    }
        return status

# ============================================================================
# Unified Sustainability Dashboard (Enhanced)
# ============================================================================

class UnifiedSustainabilityDashboard:
    """
    Unified Sustainability Dashboard for the Green Agent Ecosystem.
    """

    def __init__(self, ecosystem: 'UnifiedMetabolicEcosystem'):
        self.ecosystem = ecosystem
        self.history = []
        self.alert_thresholds = {
            'sustainability_score': 0.5,
            'carbon_budget_remaining': 0.2,
            'helium_budget_remaining': 0.2,
            'circularity_score': 0.4
        }
        self._running = True
        self._monitor_task = asyncio.create_task(self._monitor_loop())
        logger.info("UnifiedSustainabilityDashboard initialized")

    async def _monitor_loop(self):
        while self._running:
            try:
                status = await self.get_dashboard_status()
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
            if hasattr(ecosystem.metrics, 'accountant'):
                carbon_pos['remaining_budget_ratio'] = (
                    ecosystem.metrics.accountant.get_current_position().carbon_budget_remaining_kg /
                    max(ecosystem.metrics.accountant.carbon_budget_kg, 1)
                )
        helium_pos = {}
        if hasattr(ecosystem, 'helium_tracker') and ecosystem.helium_tracker:
            helium_pos = ecosystem.helium_tracker.get_helium_position()
            helium_pos = {
                'total_usage_l': helium_pos.get('total_usage_l', 0),
                'total_recovered_l': helium_pos.get('total_recovered_l', 0),
                'remaining_budget_l': helium_pos.get('remaining_budget_l', 0),
                'remaining_budget_ratio': helium_pos.get('remaining_budget_l', 0) / max(ecosystem.helium_tracker.helium_budget_l, 1)
            }
        sustainability_score = 0.5
        if hasattr(ecosystem, 'sustainability_score'):
            sustainability_score = ecosystem.sustainability_score
        elif hasattr(ecosystem, 'metrics') and ecosystem.metrics and hasattr(ecosystem.metrics, 'sustainability_score'):
            sustainability_score = ecosystem.metrics.sustainability_score
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

# ============================================================================
# Predictive Maintenance Integrator (Asyncio-enhanced)
# ============================================================================

class PredictiveMaintenanceIntegrator:
    """
    Predictive Maintenance Integration for the Green Agent Ecosystem.
    """

    def __init__(self, ecosystem: 'UnifiedMetabolicEcosystem'):
        self.ecosystem = ecosystem
        self.predictions = {}
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

# ============================================================================
# Unified Metabolic Ecosystem - Enhanced Main Entry Point
# ============================================================================

class UnifiedMetabolicEcosystem:
    """
    Unified Metabolic Ecosystem v6.1.0 with Enhanced Resilience and Sustainability.
    """

    def __init__(
        self,
        config: Optional[UnifiedEcosystemConfig] = None,
        **kwargs
    ):
        """
        Initialize the unified metabolic ecosystem.

        Args:
            config: Configuration dataclass (preferred)
            **kwargs: Legacy arguments for backward compatibility
        """
        if config is None:
            # Build config from kwargs
            config = UnifiedEcosystemConfig(
                enable_quantum=kwargs.get('enable_quantum', False),
                enable_helium=kwargs.get('enable_helium', False),
                enable_bio_inspired=kwargs.get('enable_bio_inspired', True),
                enable_evolving_gates=kwargs.get('enable_evolving_gates', True),
                enable_federated=kwargs.get('enable_federated', False),
                enable_cross_region=kwargs.get('enable_cross_region', False),
                enable_sustainability_dashboard=kwargs.get('enable_sustainability_dashboard', True),
                enable_predictive_maintenance=kwargs.get('enable_predictive_maintenance', True),
                enable_digital_twin=kwargs.get('enable_digital_twin', True),
                enable_unified_sustainability=kwargs.get('enable_unified_sustainability', True),
                enable_health_checks=kwargs.get('enable_health_checks', True),
                enable_self_healing=kwargs.get('enable_self_healing', True),
                enable_alert_escalation=kwargs.get('enable_alert_escalation', True),
                enable_dynamic_reconfig=kwargs.get('enable_dynamic_reconfig', True),
                enable_telemetry=kwargs.get('enable_telemetry', True),
                enable_persistence=kwargs.get('enable_persistence', True),
                twin_time_horizon_years=kwargs.get('twin_time_horizon_years', 10),
                twin_n_simulations=kwargs.get('twin_n_simulations', 1000),
                twin_confidence=kwargs.get('twin_confidence', 0.95),
                health_check_interval=kwargs.get('health_check_interval', 60),
                recovery_max_attempts=kwargs.get('recovery_max_attempts', 5),
                persistence_path=kwargs.get('persistence_path', 'ecosystem_state.pkl'),
                telemetry_export_interval=kwargs.get('telemetry_export_interval', 60),
                alert_escalation_timeout=kwargs.get('alert_escalation_timeout', 300)
            )
        self.config = config

        self.initialization_status: Dict[str, bool] = {}
        self.sustainability_score = 0.0
        self.helium_tracker = None
        self.circular_manager = None

        # New systems (initialized later)
        self.health_system: Optional[HealthCheckSystem] = None
        self.self_healing: Optional[SelfHealingSystem] = None
        self.alert_system: Optional[AlertEscalationSystem] = None
        self.reconfig_system: Optional[DynamicReconfigurationSystem] = None
        self.telemetry: Optional[TelemetryCollector] = None
        self.persistence: Optional[EcosystemPersistenceManager] = None
        self.sustainability_dashboard: Optional[UnifiedSustainabilityDashboard] = None
        self.predictive_maintenance: Optional[PredictiveMaintenanceIntegrator] = None
        self.digital_twin: Optional[Any] = None
        self.sustainability_engine: Optional[Any] = None

        logger.info("=" * 70)
        logger.info("Initializing Unified Metabolic Ecosystem v6.1.0")
        logger.info(f"  Config: {self.config}")
        logger.info("=" * 70)

        # Step 1: Initialize Bio-Inspired Core
        self.bio_core = None
        self.bio_available = False
        if config.enable_bio_inspired and BIO_INSPIRED_AVAILABLE:
            try:
                from enhancements.bio_inspired import BioInspiredGreenCore
                self.bio_core = BioInspiredGreenCore()
                self.bio_available = True
                self.initialization_status['bio_inspired_core'] = True
                logger.info("[BIO] Bio-Inspired Core initialized")
            except Exception as e:
                logger.error(f"[BIO] Failed to initialize Bio-Inspired Core: {e}")
                self.initialization_status['bio_inspired_core'] = False
        else:
            logger.info("[BIO] Bio-inspired architecture disabled or unavailable")
            self.initialization_status['bio_inspired_core'] = False

        # Step 2: Initialize Expert Registry
        try:
            self.registry = ExpertRegistry(
                enable_genetics=self.bio_available,
                enable_evolution=self.bio_available,
                enable_ecosystem=self.bio_available
            )
            if self.bio_available:
                self.registry.inject_bio_core(self.bio_core)
            self.initialization_status['expert_registry'] = True
            logger.info("[REGISTRY] Expert Registry initialized")
        except Exception as e:
            logger.error(f"[REGISTRY] Failed to initialize Expert Registry: {e}")
            self.initialization_status['expert_registry'] = False
            raise

        # Step 3: Initialize Gating Network
        try:
            from .gating_network import MoEGatingNetwork, GatingContext
            self.gating_network = MoEGatingNetwork(
                num_experts=5 + (1 if config.enable_quantum else 0) + (1 if config.enable_helium else 0),
                enable_bio_integration=self.bio_available
            )
            if self.bio_available:
                self.gating_network.inject_bio_core(self.bio_core)
            self.initialization_status['gating_network'] = True
            logger.info("[GATING] Gating Network initialized")
        except Exception as e:
            logger.error(f"[GATING] Failed to initialize Gating Network: {e}")
            self.initialization_status['gating_network'] = False
            raise

        # Step 4: Initialize Expert Router
        try:
            self.router = ExpertRouter(
                enable_quantum=config.enable_quantum,
                enable_signal_transduction=self.bio_available,
                enable_allosteric=self.bio_available,
                enable_metabolic_pathways=self.bio_available
            )
            self.router.gating_network = self.gating_network
            if self.bio_available:
                self.router.inject_bio_core(self.bio_core)
            self.initialization_status['expert_router'] = True
            logger.info("[ROUTER] Expert Router initialized")
        except Exception as e:
            logger.error(f"[ROUTER] Failed to initialize Expert Router: {e}")
            self.initialization_status['expert_router'] = False
            raise

        # Step 5: Initialize Metabolic Experts
        self.experts: Dict[str, Any] = {}
        # Energy Expert
        try:
            from .experts.energy_expert import EnergyExpert
            self.experts['energy'] = EnergyExpert(enable_bio_integration=self.bio_available)
            if self.bio_available:
                self.experts['energy'].inject_bio_core(self.bio_core)
            logger.info("[EXPERT] Energy Expert initialized")
        except Exception as e:
            logger.error(f"[EXPERT] Energy Expert failed: {e}")

        # Data Expert
        try:
            from .experts.data_expert import DataExpert
            self.experts['data'] = DataExpert(enable_bio_integration=self.bio_available)
            if self.bio_available:
                self.experts['data'].inject_bio_core(self.bio_core)
            logger.info("[EXPERT] Data Expert initialized")
        except Exception as e:
            logger.error(f"[EXPERT] Data Expert failed: {e}")

        # IoT Expert
        try:
            from .experts.iot_expert import IoTExpert
            self.experts['iot'] = IoTExpert(enable_bio_integration=self.bio_available)
            if self.bio_available:
                self.experts['iot'].inject_bio_core(self.bio_core)
            logger.info("[EXPERT] IoT Expert initialized")
        except Exception as e:
            logger.error(f"[EXPERT] IoT Expert failed: {e}")

        # Quantum Expert (Optional)
        if config.enable_quantum and QUANTUM_AVAILABLE:
            try:
                from .experts.quantum_expert import QuantumExpert
                self.experts['quantum'] = QuantumExpert()
                logger.info("[EXPERT] Quantum Expert initialized")
            except Exception as e:
                logger.error(f"[EXPERT] Quantum Expert failed: {e}")

        # Helium Expert (Optional)
        if config.enable_helium and HELIUM_AVAILABLE:
            try:
                from .experts.helium_expert import HeliumExpert
                self.experts['helium'] = HeliumExpert()
                logger.info("[EXPERT] Helium Expert initialized")
            except Exception as e:
                logger.error(f"[EXPERT] Helium Expert failed: {e}")

        # Register all experts
        for expert_id, expert in self.experts.items():
            try:
                if hasattr(expert, 'profile'):
                    self.registry.register_expert(expert.profile, validate=False, auto_certify=True)
            except Exception as e:
                logger.warning(f"[REGISTRY] Failed to register {expert_id}: {e}")
        self.initialization_status['experts'] = len(self.experts) > 0
        logger.info(f"[EXPERTS] {len(self.experts)} metabolic experts initialized")

        # Step 6: Wire Router and Gating Network
        for idx, (expert_id, expert) in enumerate(self.experts.items()):
            self.router.expert_index_map[idx] = expert_id
            self.router.experts[expert_id] = expert
            self.router.circuit_breakers[expert_id] = ExpertCircuitBreaker(expert_id=expert_id)
        for idx, expert_id in self.router.expert_index_map.items():
            self.gating_network.expert_index_map[idx] = expert_id

        # Step 7: Initialize Advanced Modules (if available)
        self.evolving_gates = None
        self.federated = None
        self.cross_region = None
        if config.enable_evolving_gates and EVOLVING_GATES_AVAILABLE:
            try:
                from .advanced.self_evolving_gates import EnhancedSelfEvolvingGate
                self.evolving_gates = EnhancedSelfEvolvingGate(
                    input_dim=GatingContext().feature_dim,
                    num_experts=len(self.experts),
                    enable_bio_integration=self.bio_available
                )
                if self.bio_available:
                    self.evolving_gates.inject_bio_core(self.bio_core)
                self.initialization_status['evolving_gates'] = True
                logger.info("[EVOLVE] Self-Evolving Gates initialized")
            except Exception as e:
                logger.error(f"[EVOLVE] Failed to initialize Self-Evolving Gates: {e}")
                self.initialization_status['evolving_gates'] = False

        if config.enable_federated and FEDERATED_AVAILABLE:
            try:
                from .advanced.federated_experts import EnhancedFederatedOrchestrator
                self.federated = EnhancedFederatedOrchestrator(enable_bio_integration=self.bio_available)
                if self.bio_available:
                    self.federated.inject_bio_core(self.bio_core)
                self.initialization_status['federated'] = True
                logger.info("[FEDERATED] Federated Learning initialized")
            except Exception as e:
                logger.error(f"[FEDERATED] Failed to initialize Federated Learning: {e}")
                self.initialization_status['federated'] = False

        if config.enable_cross_region and CROSS_REGION_AVAILABLE:
            try:
                from .advanced.cross_region_federation import CrossRegionFederationOptimizer
                self.cross_region = CrossRegionFederationOptimizer(enable_bio_integration=self.bio_available)
                if self.bio_available:
                    self.cross_region.inject_bio_core(self.bio_core)
                self.initialization_status['cross_region'] = True
                logger.info("[CROSS-REGION] Cross-Region Federation initialized")
            except Exception as e:
                logger.error(f"[CROSS-REGION] Failed to initialize Cross-Region Federation: {e}")
                self.initialization_status['cross_region'] = False

        # Step 8: Initialize Integration Layers
        self.layer_integrator = None
        self.work_integrator = None
        self.quantum_limits = None
        try:
            from .integration.layer_integrator import EnhancedLayerIntegrator
            self.layer_integrator = EnhancedLayerIntegrator(enable_bio_integration=self.bio_available)
            if self.bio_available:
                self.layer_integrator.inject_bio_core(self.bio_core)
            self.initialization_status['layer_integrator'] = True
            logger.info("[LAYER] Layer Integrator initialized")
        except Exception as e:
            logger.error(f"[LAYER] Failed to initialize Layer Integrator: {e}")
            self.initialization_status['layer_integrator'] = False

        try:
            from .integration.enhanced_work_integration import EnhancedWorkIntegrator
            self.work_integrator = EnhancedWorkIntegrator(
                expert_router=self.router,
                enable_bio_integration=self.bio_available
            )
            if self.bio_available:
                self.work_integrator.inject_bio_core(self.bio_core)
            self.initialization_status['work_integrator'] = True
            logger.info("[WORK] Work Integrator initialized")
        except Exception as e:
            logger.error(f"[WORK] Failed to initialize Work Integrator: {e}")
            self.initialization_status['work_integrator'] = False

        try:
            from .integration.quantum_limit_integration import QuantumLimitGraphIntegrator
            self.quantum_limits = QuantumLimitGraphIntegrator(enable_bio_integration=self.bio_available)
            if self.bio_available:
                self.quantum_limits.inject_bio_core(self.bio_core)
            self.initialization_status['quantum_limits'] = True
            logger.info("[QUANTUM] Quantum Limit Integrator initialized")
        except Exception as e:
            logger.error(f"[QUANTUM] Failed to initialize Quantum Limit Integrator: {e}")
            self.initialization_status['quantum_limits'] = False

        # Step 9: Initialize Monitoring
        try:
            from .monitoring.expert_metrics import ExpertMetricsCollector
            self.metrics = ExpertMetricsCollector(enable_bio_integration=self.bio_available)
            if self.bio_available:
                self.metrics.inject_bio_core(self.bio_core)
            self.initialization_status['metrics'] = True
            logger.info("[METRICS] Expert Metrics initialized")
        except Exception as e:
            logger.error(f"[METRICS] Failed to initialize Expert Metrics: {e}")
            self.initialization_status['metrics'] = False

        # Step 10: Initialize Sustainability Modules
        self.carbon_manager = None
        self.circular_manager = None
        self.offset_verifier = None
        self.biodiversity = None
        if SEQUESTRATION_AVAILABLE:
            try:
                from .sustainability.carbon_sequestration import CarbonSequestrationManager
                self.carbon_manager = CarbonSequestrationManager()
                self.initialization_status['carbon_sequestration'] = True
                logger.info("[SUSTAINABILITY] Carbon Sequestration Manager initialized")
            except Exception as e:
                logger.error(f"[SUSTAINABILITY] Carbon Sequestration failed: {e}")
                self.initialization_status['carbon_sequestration'] = False
        if CIRCULAR_AVAILABLE:
            try:
                from .sustainability.circular_computing import CircularComputingManager
                self.circular_manager = CircularComputingManager()
                self.initialization_status['circular_computing'] = True
                logger.info("[SUSTAINABILITY] Circular Computing Manager initialized")
            except Exception as e:
                logger.error(f"[SUSTAINABILITY] Circular Computing failed: {e}")
                self.initialization_status['circular_computing'] = False
        if OFFSET_AVAILABLE:
            try:
                from .sustainability.carbon_offset_verification import AutomatedCarbonOffsetVerification
                self.offset_verifier = AutomatedCarbonOffsetVerification()
                self.initialization_status['carbon_offset'] = True
                logger.info("[SUSTAINABILITY] Carbon Offset Verification initialized")
            except Exception as e:
                logger.error(f"[SUSTAINABILITY] Carbon Offset failed: {e}")
                self.initialization_status['carbon_offset'] = False
        if BIODIVERSITY_AVAILABLE:
            try:
                from .sustainability.biodiversity_impact import BiodiversityImpactAssessor
                self.biodiversity = BiodiversityImpactAssessor()
                self.initialization_status['biodiversity'] = True
                logger.info("[SUSTAINABILITY] Biodiversity Impact Assessor initialized")
            except Exception as e:
                logger.error(f"[SUSTAINABILITY] Biodiversity failed: {e}")
                self.initialization_status['biodiversity'] = False

        # Step 11: Initialize Enhanced Bio-Inspired Integrator
        try:
            self.bio_integrator = EnhancedBioInspiredIntegrator(self.bio_core)
            components_to_register = [
                ('registry', self.registry),
                ('gating_network', self.gating_network),
                ('router', self.router),
                ('metrics', self.metrics),
                ('work_integrator', self.work_integrator),
                ('layer_integrator', self.layer_integrator),
                ('quantum_limits', self.quantum_limits)
            ]
            for name, comp in components_to_register:
                if comp:
                    self.bio_integrator.register_component(name, comp)
            self.initialization_status['bio_integrator'] = True
            logger.info("[BIO-INTEGRATOR] Enhanced Bio-Inspired Integrator initialized")
        except Exception as e:
            logger.error(f"[BIO-INTEGRATOR] Failed to initialize Bio-Integrator: {e}")
            self.initialization_status['bio_integrator'] = False

        # Step 12: Initialize New Systems (Health, Self-Healing, Alerts, Reconfig, Telemetry, Persistence)
        if config.enable_health_checks:
            self.health_system = HealthCheckSystem(config)
            self.initialization_status['health_checks'] = True
            # Register components with health system
            for name, comp in [
                ('expert_registry', self.registry),
                ('gating_network', self.gating_network),
                ('expert_router', self.router),
                ('metrics', self.metrics),
                ('work_integrator', self.work_integrator),
                ('layer_integrator', self.layer_integrator),
                ('quantum_limits', self.quantum_limits)
            ]:
                if comp:
                    self.health_system.register_component(name, comp)
            logger.info("[HEALTH] Health Check System initialized")
        else:
            self.initialization_status['health_checks'] = False

        if config.enable_self_healing:
            self.self_healing = SelfHealingSystem(config, self.health_system)
            self.initialization_status['self_healing'] = True
            # Register recovery handlers (example)
            self.self_healing.register_recovery_handler('expert_router', self._recover_router)
            logger.info("[SELF-HEALING] Self-Healing System initialized")
        else:
            self.initialization_status['self_healing'] = False

        if config.enable_alert_escalation:
            self.alert_system = AlertEscalationSystem(config)
            self.initialization_status['alert_escalation'] = True
            logger.info("[ALERT] Alert Escalation System initialized")
        else:
            self.initialization_status['alert_escalation'] = False

        if config.enable_dynamic_reconfig:
            self.reconfig_system = DynamicReconfigurationSystem(config)
            self.initialization_status['dynamic_reconfig'] = True
            logger.info("[RECONFIG] Dynamic Reconfiguration System initialized")
        else:
            self.initialization_status['dynamic_reconfig'] = False

        if config.enable_telemetry:
            self.telemetry = TelemetryCollector()
            self.initialization_status['telemetry'] = True
            logger.info("[TELEMETRY] Telemetry Collector initialized")
        else:
            self.initialization_status['telemetry'] = False

        if config.enable_persistence:
            self.persistence = EcosystemPersistenceManager(config)
            self.initialization_status['persistence'] = True
            logger.info("[PERSISTENCE] Ecosystem Persistence Manager initialized")
            asyncio.create_task(self._load_persistence())
        else:
            self.initialization_status['persistence'] = False

        # Step 13: Initialize Sustainability Dashboard
        if config.enable_sustainability_dashboard:
            self.sustainability_dashboard = UnifiedSustainabilityDashboard(self)
            self.initialization_status['sustainability_dashboard'] = True
            logger.info("[DASHBOARD] Unified Sustainability Dashboard initialized")
        else:
            self.initialization_status['sustainability_dashboard'] = False

        # Step 14: Initialize Predictive Maintenance
        if config.enable_predictive_maintenance:
            self.predictive_maintenance = PredictiveMaintenanceIntegrator(self)
            self.initialization_status['predictive_maintenance'] = True
            logger.info("[PREDICTIVE] Predictive Maintenance Integrator initialized")
        else:
            self.initialization_status['predictive_maintenance'] = False

        # Step 15: Wire Router Metrics
        if hasattr(self.router, 'metrics_collector'):
            self.router.metrics_collector = self.metrics

        # Step 16: Initialize Digital Twin and Sustainability Engine (async)
        self._init_digital_twin_and_sustainability_task = asyncio.create_task(
            self._async_init_digital_twin_and_sustainability()
        )

        logger.info("=" * 70)
        logger.info("Unified Metabolic Ecosystem Initialization Complete")
        logger.info(f"  Bio-Inspired: {self.bio_available}")
        logger.info(f"  Experts: {len(self.experts)}")
        logger.info(f"  Status: {sum(self.initialization_status.values())}/{len(self.initialization_status)} components")
        logger.info("=" * 70)

    # --------------------------------------------------------------------------
    # Async Initialization for Digital Twin and Sustainability Engine
    # --------------------------------------------------------------------------

    async def _async_init_digital_twin_and_sustainability(self):
        if self.config.enable_digital_twin and DIGITAL_TWIN_AVAILABLE:
            try:
                from enhancements.advanced.system_digital_twin import SystemDigitalTwin, DigitalTwinConfig
                twin_config = DigitalTwinConfig(
                    time_horizon_years=self.config.twin_time_horizon_years,
                    n_simulations=self.config.twin_n_simulations,
                    confidence_level=self.config.twin_confidence
                )
                self.digital_twin = SystemDigitalTwin(twin_config)
                self.digital_twin.inject_modules(
                    quantum_limits=self.quantum_limits,
                    biodiversity=self.biodiversity,
                    expert_registry=self.registry,
                    circular_manager=self.circular_manager,
                    carbon_manager=self.carbon_manager,
                    helium_tracker=self.helium_tracker
                )
                self.initialization_status['digital_twin'] = True
                logger.info("[DIGITAL-TWIN] System Digital Twin initialized")
            except Exception as e:
                logger.error(f"[DIGITAL-TWIN] Failed to initialize Digital Twin: {e}")
                self.initialization_status['digital_twin'] = False

        if self.config.enable_unified_sustainability and SUSTAINABILITY_ENGINE_AVAILABLE:
            try:
                from enhancements.sustainability.unified_sustainability_engine import UnifiedSustainabilityEngine
                self.sustainability_engine = UnifiedSustainabilityEngine()
                self.sustainability_engine.inject_modules(
                    carbon_manager=self.carbon_manager,
                    helium_tracker=self.helium_tracker,
                    circular_manager=self.circular_manager,
                    biodiversity=self.biodiversity,
                    expert_registry=self.registry,
                    quantum_limits=self.quantum_limits
                )
                self.initialization_status['sustainability_engine'] = True
                logger.info("[SUSTAINABILITY-ENGINE] Unified Sustainability Engine initialized")
                # Initial update
                score = await self.sustainability_engine.update_sustainability_score()
                self.sustainability_score = score.total_score
                logger.info(f"[SUSTAINABILITY] Initial score: {self.sustainability_score:.3f}")
            except Exception as e:
                logger.error(f"[SUSTAINABILITY-ENGINE] Failed to initialize Sustainability Engine: {e}")
                self.initialization_status['sustainability_engine'] = False

    # --------------------------------------------------------------------------
    # Persistence Methods
    # --------------------------------------------------------------------------

    async def _load_persistence(self):
        if self.persistence:
            await self.persistence.load_state(self)

    async def save_state(self):
        if self.persistence:
            await self.persistence.save_state(self)

    async def delete_state(self):
        if self.persistence:
            await self.persistence.delete_state()

    # --------------------------------------------------------------------------
    # Recovery Handlers for Self-Healing
    # --------------------------------------------------------------------------

    async def _recover_router(self) -> bool:
        """Recovery handler for expert router."""
        logger.info("Attempting to recover expert router...")
        # Simulate restart
        await asyncio.sleep(0.5)
        # In real scenario, reinitialize or fallback
        return True

    # --------------------------------------------------------------------------
    # Public Methods
    # --------------------------------------------------------------------------

    async def run_sustainability_scenario(
        self,
        scenario_type: str,
        parameters: Dict[str, Any],
        time_horizon_years: Optional[int] = None
    ) -> Dict[str, Any]:
        if not self.config.enable_digital_twin or not self.digital_twin:
            return {'status': 'digital_twin_not_enabled'}
        from enhancements.advanced.system_digital_twin import SimulationScenario
        scenario_map = {
            'policy_change': SimulationScenario.POLICY_CHANGE,
            'market_shock': SimulationScenario.MARKET_SHOCK,
            'resource_depletion': SimulationScenario.RESOURCE_DEPLETION,
            'technology_adoption': SimulationScenario.TECHNOLOGY_ADOPTION,
            'regulatory_change': SimulationScenario.REGULATORY_CHANGE,
            'climate_event': SimulationScenario.CLIMATE_EVENT
        }
        scenario_enum = scenario_map.get(scenario_type, SimulationScenario.POLICY_CHANGE)
        result = await self.digital_twin.run_scenario(scenario_enum, parameters, time_horizon_years)
        return {
            'scenario_id': result.scenario_id,
            'sustainability_score': result.sustainability_score,
            'risk_factors': result.risk_factors,
            'recommendations': result.recommendations,
            'projections': result.projections,
            'confidence_intervals': result.confidence_intervals
        }

    async def get_twin_projections(self) -> Dict[str, Any]:
        if not self.config.enable_digital_twin or not self.digital_twin:
            return {'status': 'digital_twin_not_enabled'}
        return await self.digital_twin.export_projections()

    async def get_sustainability_status(self) -> Dict[str, Any]:
        if not self.config.enable_unified_sustainability or not self.sustainability_engine:
            return {'status': 'sustainability_engine_not_enabled'}
        return await self.sustainability_engine.get_sustainability_report()

    async def get_sustainability_score(self) -> float:
        if not self.config.enable_unified_sustainability or not self.sustainability_engine:
            return self.sustainability_score
        return await self.sustainability_engine.get_current_score()

    async def get_sustainability_dimensions(self) -> Dict[str, Any]:
        if not self.config.enable_unified_sustainability or not self.sustainability_engine:
            return {'status': 'sustainability_engine_not_enabled'}
        status = await self.sustainability_engine.get_sustainability_report()
        return status.get('dimensions', {})

    async def update_sustainability_score(self) -> float:
        if not self.config.enable_unified_sustainability or not self.sustainability_engine:
            return self.sustainability_score
        score = await self.sustainability_engine.update_sustainability_score()
        self.sustainability_score = score.total_score
        return self.sustainability_score

    # --------------------------------------------------------------------------
    # Ecosystem Status
    # --------------------------------------------------------------------------

    def get_ecosystem_status(self) -> Dict[str, Any]:
        status = {
            'ecosystem_version': '6.1.0',
            'bio_inspired_available': self.bio_available,
            'initialization_status': self.initialization_status,
            'expert_count': len(self.experts),
            'expert_types': list(self.experts.keys()),
            'sustainability_score': self.sustainability_score
        }
        if hasattr(self, 'registry'):
            status['registry'] = self.registry.get_registry_stats()
        if hasattr(self, 'router'):
            status['router'] = self.router.get_routing_stats()
        if hasattr(self, 'gating_network'):
            status['gating'] = self.gating_network.get_comprehensive_stats()
        if self.bio_available and self.bio_core:
            status['bio_system'] = self.bio_core.get_system_status()
        if hasattr(self, 'metrics'):
            status['metrics'] = self.metrics.get_metrics_summary()
        if self.sustainability_dashboard:
            status['dashboard'] = asyncio.run(self.sustainability_dashboard.get_dashboard_status())
        if self.predictive_maintenance:
            status['predictive'] = asyncio.run(self.predictive_maintenance.get_predictive_insights())
        if self.config.enable_digital_twin and self.digital_twin:
            status['digital_twin'] = self.digital_twin.get_simulation_stats()
        if self.config.enable_unified_sustainability and self.sustainability_engine:
            status['sustainability_dimensions'] = asyncio.run(self.sustainability_engine.get_dimension_status())
        if self.health_system:
            status['health'] = asyncio.run(self.health_system.get_system_health())
        if self.self_healing:
            status['recovery'] = asyncio.run(self.self_healing.get_recovery_stats())
        if self.alert_system:
            status['alerts'] = asyncio.run(self.alert_system.get_alert_stats())
        if self.reconfig_system:
            status['configuration'] = asyncio.run(self.reconfig_system.get_current_config())
        return status

    # --------------------------------------------------------------------------
    # Task Processing
    # --------------------------------------------------------------------------

    def process_task(self, task: Dict[str, Any], pipeline_type: str = 'standard') -> Dict[str, Any]:
        if hasattr(self, 'work_integrator') and self.work_integrator:
            result = self.work_integrator.process_work(task, pipeline_type)
            if result:
                self.sustainability_score = self._update_sustainability_score(result)
            return result
        elif hasattr(self, 'router'):
            return self.router.route_and_execute(
                workload_profile=task,
                meta_cognitive_state={},
                dual_axis_context={}
            )
        else:
            return {'success': False, 'error': 'No work processor available'}

    def _update_sustainability_score(self, result: Dict[str, Any]) -> float:
        if hasattr(self, 'metrics') and self.metrics:
            return self.metrics.sustainability_score
        return self.sustainability_score

    # --------------------------------------------------------------------------
    # Expert Management
    # --------------------------------------------------------------------------

    def get_expert(self, expert_type: str) -> Optional[Any]:
        return self.experts.get(expert_type)

    def register_expert(self, expert_type: str, expert_instance: Any):
        self.experts[expert_type] = expert_instance
        idx = len(self.router.expert_index_map)
        self.router.expert_index_map[idx] = expert_type
        self.router.experts[expert_type] = expert_instance
        self.router.circuit_breakers[expert_type] = ExpertCircuitBreaker(expert_id=expert_type)
        self.gating_network.expert_index_map[idx] = expert_type
        if hasattr(expert_instance, 'profile'):
            self.registry.register_expert(expert_instance.profile, validate=False)
        if hasattr(self, 'bio_integrator'):
            self.bio_integrator.register_component(f"expert_{expert_type}", expert_instance)
        if self.health_system:
            self.health_system.register_component(f"expert_{expert_type}", expert_instance)
        if self.config.enable_unified_sustainability and self.sustainability_engine:
            asyncio.create_task(self.sustainability_engine.update_sustainability_score())
        logger.info(f"Dynamic expert registered: {expert_type}")

    def inject_external_module(self, module_name: str, module_instance: Any):
        if module_name == 'token_manager':
            for expert in self.experts.values():
                if hasattr(expert, 'token_manager'):
                    expert.token_manager = module_instance
            if hasattr(self, 'router') and hasattr(self.router, 'token_manager'):
                self.router.token_manager = module_instance
        elif module_name == 'gradient_manager':
            for expert in self.experts.values():
                if hasattr(expert, 'gradient_manager'):
                    expert.gradient_manager = module_instance
        elif module_name == 'compartment_manager':
            for expert in self.experts.values():
                if hasattr(expert, 'compartment_manager'):
                    expert.compartment_manager = module_instance
        logger.info(f"External module injected: {module_name}")

    # --------------------------------------------------------------------------
    # Health, Alerts, Reconfiguration
    # --------------------------------------------------------------------------

    def add_health_check(self, component_name: str, component: Any):
        if self.health_system:
            self.health_system.register_component(component_name, component)
            logger.info(f"Health check added for component: {component_name}")

    async def get_health_status(self) -> Dict[str, Any]:
        if self.health_system:
            return await self.health_system.get_system_health()
        return {'status': 'health_system_not_enabled'}

    async def get_alerts(self, active_only: bool = True) -> List[Dict]:
        if self.alert_system:
            if active_only:
                return await self.alert_system.get_active_alerts()
            return self.alert_system.alerts
        return []

    async def resolve_alert(self, alert_id: str):
        if self.alert_system:
            await self.alert_system.resolve_alert(alert_id)
            logger.info(f"Alert {alert_id} resolved")

    async def reconfigure_by_metrics(self, metrics: Dict[str, float]):
        if not self.config.enable_dynamic_reconfig or not self.reconfig_system:
            return {'status': 'reconfiguration_not_enabled'}
        await self.reconfig_system.reconfigure_by_metrics(metrics)
        return {
            'status': 'reconfiguration_applied',
            'config': await self.reconfig_system.get_current_config()
        }

    # --------------------------------------------------------------------------
    # Shutdown
    # --------------------------------------------------------------------------

    async def shutdown(self):
        logger.info("Shutting down Unified Metabolic Ecosystem...")
        tasks = []
        if self.sustainability_dashboard:
            tasks.append(self.sustainability_dashboard.shutdown())
        if self.predictive_maintenance:
            tasks.append(self.predictive_maintenance.shutdown())
        if self.health_system:
            tasks.append(self.health_system.shutdown())
        if self.self_healing:
            tasks.append(self.self_healing.shutdown())
        if self.digital_twin:
            tasks.append(self.digital_twin.shutdown())
        if self.persistence:
            tasks.append(self.save_state())
        await asyncio.gather(*tasks)
        logger.info("Unified Metabolic Ecosystem shutdown complete")
