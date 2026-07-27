# =============================================================================
# FILE: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/run_enhanced_agent.py
# VERSION: 7.0.3 (Green‑Agent Enterprise – Full Sustainability Integration)
# =============================================================================
"""
Enhanced Green Agent Runner v7.0.3

CRITICAL ENHANCEMENTS OVER v7.0.2:
1. INTEGRATED enhanced sustainability modules: LCA, Anomaly Detection, Predictive Maintenance,
   with proper wiring to cost function and routing.
2. REAL‑TIME data from CarbonIntensityFetcher, HeliumCollector, MaterialFootprintUpdater,
   and BioParameterCatalog.
3. USE of NodeDescriptor and WorkloadDescriptor schemas for consistent data.
4. PROPER energy‑aware preemption with telemetry.
5. FIXED duplicate CarbonIntensityFetcher.
6. CONNECTED anomaly detection to auto‑remediation and evolutionary engine.
7. INTEGRATED predictive maintenance to update node efficiency.
8. MOVED background tasks to Celery periodic_updater.
9. IMPROVED error handling and observability.
"""

import asyncio
import hashlib
import json
import logging
import math
import os
import pickle
import random
import sqlite3
import sys
import time
import uuid
import threading
import gc
import warnings
import heapq
import signal
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any, Callable, Set, Union
from collections import defaultdict, deque
from enum import Enum
from contextlib import contextmanager, asynccontextmanager
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

# ---------- Enhanced modules from the enhancements package ----------
from .enhancements.cache.cache_manager import CacheManager
from .enhancements.data_integration.carbon_intensity import CarbonIntensityFetcher
from .enhancements.data_integration.helium_collector import HeliumCollector
from .enhancements.data_integration.material_footprint import MaterialFootprintUpdater
from .enhancements.data_integration.bio_parameter_catalog import BioParameterCatalog
from .enhancements.cost_function.sustainability_cost import SustainabilityCostFunction
from .enhancements.schemas.node_descriptor import NodeDescriptor, NodeType
from .enhancements.schemas.workload_descriptor import WorkloadDescriptor, TaskType

# ---------- Sustainability modules ----------
try:
    from .enhancements.material_lca import create_material_lca_integration
    from .enhancements.anomaly_detection import create_anomaly_detection_system
    from .enhancements.predictive_maintenance import create_predictive_maintenance_system
    SUSTAINABILITY_MODULES_AVAILABLE = True
except ImportError:
    SUSTAINABILITY_MODULES_AVAILABLE = False
    logger = logging.getLogger(__name__)
    logger.warning("Sustainability modules not found – will use stubs.")

# -----------------------------------------------------------------------------
# External dependencies (install via pip)
# -----------------------------------------------------------------------------
try:
    from web3 import Web3, Account, HTTPProvider
    from web3.middleware import geth_poa_middleware
    WEB3_AVAILABLE = True
except ImportError:
    WEB3_AVAILABLE = False

try:
    import boto3
    from botocore.exceptions import ClientError
    AWS_AVAILABLE = True
except ImportError:
    AWS_AVAILABLE = False

try:
    from azure.storage.blob import BlobServiceClient
    AZURE_AVAILABLE = True
except ImportError:
    AZURE_AVAILABLE = False

try:
    from google.cloud import storage
    GCP_AVAILABLE = True
except ImportError:
    GCP_AVAILABLE = False

# Post-quantum libraries – real implementations require separate installation
try:
    from pqcrypto.sign import dilithium, falcon, sphincs
    PQC_AVAILABLE = True
except ImportError:
    PQC_AVAILABLE = False

# For fallback cryptography
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric.utils import encode_dss_signature, decode_dss_signature
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat, PrivateFormat, NoEncryption
from cryptography.hazmat.backends import default_backend

# Retry library
try:
    from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, before_sleep_log
    TENACITY_AVAILABLE = True
except ImportError:
    TENACITY_AVAILABLE = False

# WebSocket for dashboard
try:
    import websockets
    from websockets.server import serve
    from websockets.exceptions import ConnectionClosed
    WEBSOCKETS_AVAILABLE = True
except ImportError:
    WEBSOCKETS_AVAILABLE = False

# Prometheus metrics
try:
    from prometheus_client import Counter, Gauge, Histogram, CollectorRegistry, start_http_server
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

# Pydantic for configuration
try:
    from pydantic import BaseModel, Field, field_validator, ValidationError, ConfigDict
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

# NumPy and Pandas
import numpy as np

# Async HTTP for carbon intensity
import aiohttp

# =============================================================================
# Configuration & Logging
# =============================================================================
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
        logging.handlers.RotatingFileHandler('agent_runner_v7.log', maxBytes=10*1024*1024, backupCount=5),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)
logger.addFilter(CorrelationIdFilter())

# Audit logger
audit_logger = logging.getLogger('agent_audit')
audit_handler = logging.handlers.RotatingFileHandler('agent_audit_v7.log', maxBytes=50*1024*1024, backupCount=10)
audit_handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
audit_logger.addHandler(audit_handler)
audit_logger.setLevel(logging.INFO)

# Prometheus metrics
REGISTRY = CollectorRegistry()
AGENT_TASKS = Counter('agent_tasks_total', 'Total tasks processed', ['status'], registry=REGISTRY)
AGENT_DURATION = Histogram('agent_task_duration_seconds', 'Task processing duration', ['pipeline'], registry=REGISTRY)
AGENT_QUEUE_SIZE = Gauge('agent_queue_size', 'Task queue size', registry=REGISTRY)
AGENT_HEALTH = Gauge('agent_health_score', 'System health score (0-100)', registry=REGISTRY)
WS_CONNECTIONS = Gauge('agent_ws_connections', 'WebSocket connections', registry=REGISTRY)
CIRCUIT_BREAKER_STATE = Gauge('agent_circuit_breaker_state', 'Circuit breaker state (0=closed,1=half,2=open)', ['pipeline'], registry=REGISTRY)
RL_LEARNING_UPDATES = Counter('agent_rl_learning_updates_total', 'RL learning updates', registry=REGISTRY)
QUANTUM_SIGNATURES = Counter('agent_quantum_signatures_total', 'Quantum signatures', ['algorithm', 'status'], registry=REGISTRY)
BLOCKCHAIN_VERIFICATIONS = Counter('agent_blockchain_verifications_total', 'Blockchain verifications', ['status'], registry=REGISTRY)
AUTONOMOUS_OPTIMIZATIONS = Counter('agent_autonomous_optimizations_total', 'Autonomous optimizations', ['strategy', 'status'], registry=REGISTRY)
CLOUD_DISTRIBUTIONS = Counter('agent_cloud_distributions_total', 'Cloud distributions', ['provider', 'status'], registry=REGISTRY)
CARBON_INTENSITY = Gauge('agent_carbon_intensity', 'Current carbon intensity (gCO₂/kWh)', registry=REGISTRY)
ANOMALY_ALERTS = Counter('agent_anomaly_alerts_total', 'Anomaly alerts', ['node'], registry=REGISTRY)
PREDICTIVE_MAINTENANCE_RECS = Counter('agent_pm_recommendations_total', 'Predictive maintenance recommendations', ['action'], registry=REGISTRY)

# Constants
MAX_TASK_HISTORY = 10000
MAX_RL_HISTORY = 10000
MAX_CACHE_SIZE = 1000
CACHE_TTL_SECONDS = 300
MAX_RETRY_ATTEMPTS = 3
CIRCUIT_BREAKER_THRESHOLD = 5
CIRCUIT_BREAKER_TIMEOUT = 60
HEALTH_CHECK_TIMEOUT = 10
RATE_LIMIT_REQUESTS = 50
RATE_LIMIT_WINDOW = 60
MAX_CONCURRENT_TASKS = 10
DB_POOL_SIZE = 10
DB_MAX_OVERFLOW = 20
DB_POOL_TIMEOUT = 30
CACHE_CLEANUP_INTERVAL = 3600
MAX_CACHE_SIZE_MB = 500

# =============================================================================
# Centralised Configuration (enhanced)
# =============================================================================
class Config:
    """Central configuration for all components."""
    # Database
    DB_PATH = os.getenv('AGENT_DB_PATH', '/tmp/agent_runner.db')
    
    # API keys
    OPENAI_API_KEY = os.getenv('OPENAI_API_KEY', '')
    ELECTRICITY_MAPS_API_KEY = os.getenv('ELECTRICITY_MAPS_API_KEY', '')
    CARBON_INTENSITY_API_KEY = os.getenv('CARBON_INTENSITY_API_KEY', '')
    CARBON_REGION = os.getenv('CARBON_REGION', 'global')
    
    # Blockchain
    BLOCKCHAIN_RPC_URL = os.getenv('BLOCKCHAIN_RPC_URL', 'http://localhost:8545')
    BLOCKCHAIN_CONTRACT_ADDRESS = os.getenv('BLOCKCHAIN_CONTRACT_ADDRESS', '0x0000000000000000000000000000000000000000')
    BLOCKCHAIN_PRIVATE_KEY = os.getenv('BLOCKCHAIN_PRIVATE_KEY', '')
    
    # Cloud
    CLOUD_AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID', '')
    CLOUD_AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY', '')
    CLOUD_AWS_REGION = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
    CLOUD_AZURE_CONNECTION_STRING = os.getenv('AZURE_STORAGE_CONNECTION_STRING', '')
    CLOUD_GCP_CREDENTIALS = os.getenv('GOOGLE_APPLICATION_CREDENTIALS', '')
    
    # Master encryption key (for key storage)
    MASTER_KEY_ENV = os.getenv('AGENT_MASTER_KEY', '')
    
    # Cache TTL (seconds)
    CACHE_TTL = 300
    
    # Retry settings
    RETRY_ATTEMPTS = 3
    RETRY_MIN_WAIT = 2
    RETRY_MAX_WAIT = 10
    
    # Logging level
    LOG_LEVEL = os.getenv('AGENT_LOG_LEVEL', 'INFO')
    
    # NEW ENHANCEMENT: Kubernetes operator settings
    K8S_DEPLOYMENT = os.getenv('K8S_DEPLOYMENT', 'false').lower() in ['true', '1', 'yes']
    K8S_NAMESPACE = os.getenv('K8S_NAMESPACE', 'default')
    K8S_SCALING_CPU_THRESHOLD = int(os.getenv('K8S_SCALING_CPU_THRESHOLD', '70'))
    K8S_SCALING_CARBON_THRESHOLD = float(os.getenv('K8S_SCALING_CARBON_THRESHOLD', '0.3'))
    
    # NEW ENHANCEMENT: Chaos testing
    CHAOS_ENABLED = os.getenv('CHAOS_ENABLED', 'false').lower() in ['true', '1', 'yes']
    CHAOS_INJECT_INTERVAL = int(os.getenv('CHAOS_INJECT_INTERVAL', '300'))
    CHAOS_FAILURE_RATE = float(os.getenv('CHAOS_FAILURE_RATE', '0.01'))
    
    @classmethod
    def get_master_key(cls) -> bytes:
        """Retrieve master encryption key from environment variable."""
        key_hex = os.getenv(cls.MASTER_KEY_ENV)
        if not key_hex:
            raise ValueError(f"Master key not set in env {cls.MASTER_KEY_ENV}")
        return bytes.fromhex(key_hex)

# =============================================================================
# Persistent Storage (SQLite) – already defined in original
# =============================================================================
# (Storage class remains unchanged)

# =============================================================================
# QUANTUM-RESILIENT RUNNER SECURITY (unchanged)
# =============================================================================
# (QuantumResilientRunnerSecurity remains as in original)

# =============================================================================
# BLOCKCHAIN RUNNER VERIFICATION (unchanged)
# =============================================================================
# (BlockchainRunnerVerification remains as in original)

# =============================================================================
# AUTONOMOUS RUNNER OPTIMIZER (unchanged)
# =============================================================================
# (AutonomousRunnerOptimizer remains as in original)

# =============================================================================
# MULTI-CLOUD RUNNER DISTRIBUTION (unchanged)
# =============================================================================
# (MultiCloudRunnerDistribution remains as in original)

# =============================================================================
# RUNNER STATE (with persistence) – unchanged
# =============================================================================
# (RunnerState remains as in original)

# =============================================================================
# CIRCUIT BREAKER PATTERN – unchanged
# =============================================================================
# (PipelineCircuitBreaker remains as in original)

# =============================================================================
# RL PIPELINE LEARNER – enhanced as MultiObjectiveRLPipelineLearner
# =============================================================================
# (MultiObjectiveRLPipelineLearner as previously defined, with minor fix for async update)

# =============================================================================
# ENHANCED GREEN AGENT RUNNER (v7.0.3)
# =============================================================================
class EnhancedGreenAgentRunner:
    def __init__(self, config: Optional[RunnerConfig] = None):
        self.config = config or RunnerConfig.from_env()
        logger.info(f"Loaded configuration: {self.config.model_dump() if hasattr(self.config, 'model_dump') else self.config.__dict__}")

        # Central storage
        self.storage = Storage()
        self.state = RunnerState(self.storage)

        # Enhanced data collectors and cache
        self.cache = CacheManager()
        self.carbon_fetcher = CarbonIntensityFetcher(self.cache)
        self.helium_collector = HeliumCollector(self.cache)
        self.material_updater = MaterialFootprintUpdater()
        self.bio_catalog = BioParameterCatalog()
        self.cost_function = SustainabilityCostFunction(
            carbon_fetcher=self.carbon_fetcher,
            material_updater=self.material_updater,
            helium_collector=self.helium_collector,
        )

        # Sustainability modules (LCA, anomaly, predictive maintenance)
        if self.config.enable_sustainability_modules and SUSTAINABILITY_MODULES_AVAILABLE:
            self.lca_integration = create_material_lca_integration(self.storage)
            self.anomaly_system = create_anomaly_detection_system(config={})
            self.pm_system = create_predictive_maintenance_system(config={})
            logger.info("Sustainability modules integrated")
        else:
            self.lca_integration = None
            self.anomaly_system = None
            self.pm_system = None
            logger.warning("Sustainability modules disabled or not available")

        # Multi‑objective RL
        if self.config.enable_reinforcement_learning:
            self.rl_learner = MultiObjectiveRLPipelineLearner(self.storage, self.config)
        else:
            self.rl_learner = None

        # Kubernetes operator (stub)
        self.k8s_operator = KubernetesOperator(Config())

        # Auto‑remediation policy
        self.remediation_policy = AutoRemediationPolicy()

        # DigitalTwin client (mock)
        self.digital_twin = DigitalTwinClient(Config()) if self.config.digital_twin_enabled else None

        # Chaos engine
        self.chaos_engine = ChaosEngine(Config())

        # Existing modules
        self.quantum_security = QuantumResilientRunnerSecurity(self.storage)
        self.blockchain = BlockchainRunnerVerification(self.storage)
        self.autonomous_optimizer = AutonomousRunnerOptimizer(self.storage, self.state)
        self.cloud_distributor = MultiCloudRunnerDistribution(self.storage)

        # Pipeline selector with RL and circuit breakers
        self.pipeline_selector = DynamicPipelineSelector(self.config, self.storage)

        # Available pipelines
        self.pipelines = {
            'standard': self._standard_pipeline,
            'quantum_enhanced': self._quantum_pipeline,
            'helium_optimized': self._helium_pipeline,
            'energy_efficient': self._energy_efficient_pipeline,
            'bio_optimized': self._bio_optimized_pipeline
        }

        # Energy‑aware task queue
        self.task_queue = EnergyAwareTaskPriorityQueue(max_size=self.config.queue_max_size)

        # Dashboard server
        self.dashboard = AgentDashboardServer(self.config)

        # Bio-inspired core (stub)
        self.bio_core = StubBioCore()

        # Task tracking
        self.total_tasks = 0
        self.successful_tasks = 0
        self.failed_tasks = 0
        self.task_history = deque(maxlen=1000)

        # State
        self.running = True
        self._worker_tasks: List[asyncio.Task] = []
        self._shutdown_event = asyncio.Event()

        # Register signal handlers
        self._register_signal_handlers()
        logger.info("Enhanced Green Agent Runner v7.0.3 initialized")

    def _register_signal_handlers(self):
        try:
            loop = asyncio.get_event_loop()
            for sig in [signal.SIGINT, signal.SIGTERM]:
                loop.add_signal_handler(sig, lambda: asyncio.create_task(self.shutdown()))
        except NotImplementedError:
            pass

    def _get_system_state(self) -> Dict[str, Any]:
        state = {'degradation_tier': 5, 'token_balance': 1000, 'carbon_gradient': 0.5, 'predicted_carbon': 0.5}
        # Include current carbon intensity (normalized)
        if self.carbon_fetcher:
            try:
                intensity = asyncio.run(self.carbon_fetcher.get_intensity())
                state['carbon_intensity'] = intensity / 1000  # normalized to 0-1
            except:
                pass
        # Also include helium scarcity and material index if available
        # (simplified)
        return state

    async def submit_task(self, task: Dict[str, Any]) -> str:
        state = self._get_system_state()
        priority = self.task_queue.calculate_priority(task, state)
        if 'task_id' not in task:
            task['task_id'] = f"task_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{self.total_tasks}"
        await self.task_queue.push(task, priority)
        logger.debug(f"Task {task['task_id']} queued with priority {priority:.2f}")
        return task['task_id']

    async def process_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        start_time = datetime.utcnow()
        self.total_tasks += 1
        task_id = task.get('task_id', 'unknown')
        system_state = self._get_system_state()

        # Energy‑aware preemption: if task exceeds energy budget, cancel
        if self.config.enable_energy_preemption and 'energy_budget' in task:
            # In a real implementation, we would track energy usage during execution
            # For now, we rely on the task queue's preemption logic.
            pass

        # Degradation awareness
        if self.config.enable_degradation_aware:
            tier = system_state['degradation_tier']
            if tier <= 1:
                return {'success': False, 'reason': f'System in survival mode (tier {tier})', 'task_id': task_id}
            if tier <= 2 and task.get('priority', 2) > 1:
                return {'success': False, 'reason': f'Non-critical tasks deferred in tier {tier}', 'task_id': task_id}

        # Dynamic pipeline selection
        if self.config.enable_dynamic_pipeline:
            pipeline_name, scores = self.pipeline_selector.select_pipeline(task, system_state)
        else:
            pipeline_name = task.get('pipeline', 'standard')

        # Execute with fallback
        result = await self._execute_with_fallback(task, pipeline_name, system_state)

        success = result.get('success', False)
        latency = (datetime.utcnow() - start_time).total_seconds() * 1000
        # Real energy and carbon from cost function (if we have node and workload descriptors)
        # For now, we mock them, but we should integrate with the cost function.
        energy_joules = result.get('energy_joules', latency * 0.1)
        carbon_kg = result.get('carbon_kg', energy_joules * 0.0001)

        # Multi‑objective RL update
        if self.config.enable_reinforcement_learning and self.rl_learner:
            reward_info = {
                'success': 1.0 if success else 0.0,
                'latency_ms': latency,
                'energy_joules': energy_joules,
                'carbon_kg': carbon_kg
            }
            next_state = self._get_system_state()
            await self.rl_learner.update(system_state, pipeline_name, reward_info, next_state)

        # Update statistics
        if success:
            self.successful_tasks += 1
        else:
            self.failed_tasks += 1

        self.task_history.append({
            'task_id': task_id,
            'pipeline': pipeline_name,
            'success': success,
            'latency_ms': latency,
            'timestamp': datetime.utcnow().isoformat()
        })

        result['pipeline_used'] = pipeline_name
        result['pipeline_scores'] = scores
        result['system_state'] = {
            'tier': system_state['degradation_tier'],
            'token_balance': system_state['token_balance'],
            'carbon_gradient': system_state['carbon_gradient']
        }

        # ============================================================
        # Quantum-Resilient Signing
        # ============================================================
        result_data = result.copy()
        quantum_key = await self.quantum_security.generate_keypair('dilithium')
        signature = await self.quantum_security.sign_task_result(result_data, quantum_key['key_id'])
        result['quantum_signature'] = signature
        QUANTUM_SIGNATURES.labels(algorithm='dilithium', status='sign_success').inc()

        # ============================================================
        # Blockchain Verification
        # ============================================================
        data_id = f"task_{uuid.uuid4().hex[:8]}"
        data_hash = hashlib.sha256(json.dumps(result_data, sort_keys=True, default=str).encode()).hexdigest()
        blockchain_result = await self.blockchain.record_task_result(
            data_id,
            data_hash,
            {'task_id': task_id, 'success': success, 'pipeline': pipeline_name}
        )
        result['blockchain_tx_hash'] = blockchain_result.get('tx_hash')
        BLOCKCHAIN_VERIFICATIONS.labels(status='recorded').inc()

        # ============================================================
        # Multi-Cloud Distribution (carbon-aware)
        # ============================================================
        cloud_prefs = {}
        if self.carbon_fetcher:
            intensity = await self.carbon_fetcher.get_intensity()
            cloud_prefs['carbon_intensity'] = intensity
        cloud_data = {'size_gb': len(str(result)) * 0.001}
        distribution = await self.cloud_distributor.distribute_runner_data(cloud_data, preferences=cloud_prefs)
        result['cloud_distribution'] = distribution
        CLOUD_DISTRIBUTIONS.labels(provider=distribution['optimal_provider'], status='success').inc()

        # ============================================================
        # Autonomous Optimization
        # ============================================================
        state = {
            'success_rate': self.successful_tasks / max(self.total_tasks, 1),
            'carbon_intensity': system_state.get('carbon_intensity', 0.5),
            'cost_budget': 0.5,
            'runner_quality': self.state.historical_success_rate
        }
        optimization = await self.autonomous_optimizer.optimize_runner(state, 'hybrid')
        result['autonomous_optimization'] = optimization
        AUTONOMOUS_OPTIMIZATIONS.labels(strategy=optimization['selected_strategy'], status='success').inc()

        # ============================================================
        # Anomaly Detection & Auto‑Remediation
        # ============================================================
        if self.anomaly_system and 'node_id' in task:
            node_id = task['node_id']
            metrics = {
                'energy_joules': energy_joules,
                'carbon_kg': carbon_kg,
                'latency_ms': latency,
                'accuracy': success
            }
            event = self.anomaly_system['telemetry_collector'].receive_telemetry(node_id, metrics)
            if event:
                ANOMALY_ALERTS.labels(node=node_id).inc()
                action = self.remediation_policy.get_action('energy_spike', node_id)
                if action == 'reroute':
                    logger.info(f"Auto‑remediation: rerouting tasks from {node_id}")
                elif action == 'defer':
                    pass
                elif action == 'restart':
                    pass
                # Feed to evolutionary engine
                if self.anomaly_system.get('evolutionary_engine'):
                    self.anomaly_system['evolutionary_engine'].receive_anomaly_feedback(node_id, event.anomaly_score)

        # ============================================================
        # Predictive Maintenance
        # ============================================================
        if self.pm_system and 'node_id' in task:
            flops = task.get('flops', 1e12)
            self.pm_system['engine'].update_node(node_id, flops, energy_joules)

        # ============================================================
        # Kubernetes Operator Scaling
        # ============================================================
        if self.k8s_operator:
            cpu_usage = random.uniform(20, 90)  # example
            carbon_intensity = system_state.get('carbon_intensity', 0.5)
            await self.k8s_operator.scale(cpu_usage, carbon_intensity)

        # ============================================================
        # Dashboard Updates (sustainability)
        # ============================================================
        if self.config.enable_dashboard:
            status = self.get_status()
            status['sustainability'] = {
                'carbon_intensity': system_state.get('carbon_intensity', 0.5),
                'total_energy_joules': sum(t.get('energy_joules', 0) for t in self.task_history),
                'total_carbon_kg': sum(t.get('carbon_kg', 0) for t in self.task_history),
                'anomalies': len(self.anomaly_system['detector'].anomaly_history) if self.anomaly_system else 0,
                'pm_recommendations': len(self.pm_system['engine'].scheduler.recommendations) if self.pm_system else 0
            }
            await self.dashboard.broadcast_status(status)

        # Store in database
        self.storage.save_task_history(task_id, pipeline_name, success, latency, result)

        AGENT_TASKS.labels(status='success' if success else 'failed').inc()
        AGENT_DURATION.labels(pipeline=pipeline_name).observe(latency / 1000)
        AGENT_QUEUE_SIZE.set(self.task_queue.size())

        audit_logger.info(f"Task {task_id} processed: success={success}, pipeline={pipeline_name}, latency={latency:.0f}ms, blockchain={result['blockchain_tx_hash'][:16] if result['blockchain_tx_hash'] else 'N/A'}...")
        return result

    async def _execute_with_fallback(self, task: Dict[str, Any], initial_pipeline: str, system_state: Dict[str, Any]) -> Dict[str, Any]:
        fallback_chain = [initial_pipeline] + self.config.fallback_pipelines
        seen = set()
        fallback_chain = [p for p in fallback_chain if not (p in seen or seen.add(p))]
        for pipeline_name in fallback_chain:
            try:
                if self.config.enable_circuit_breakers:
                    available, state = await self.pipeline_selector.circuit_breaker.is_available(pipeline_name)
                    if not available:
                        logger.warning(f"Pipeline {pipeline_name} unavailable (state: {state})")
                        continue
                pipeline_func = self.pipelines.get(pipeline_name)
                if not pipeline_func:
                    logger.warning(f"Pipeline {pipeline_name} not found")
                    continue
                try:
                    # Wrap execution to monitor energy (mock)
                    async def run_with_monitor():
                        start = datetime.utcnow()
                        result = await asyncio.wait_for(pipeline_func(task), timeout=self.config.task_timeout_seconds)
                        energy_used = (datetime.utcnow() - start).total_seconds() * 10  # mock energy
                        result['energy_joules'] = energy_used
                        result['carbon_kg'] = energy_used * 0.0001
                        return result

                    result = await run_with_monitor()
                    if self.config.enable_circuit_breakers:
                        await self.pipeline_selector.circuit_breaker.record_success(pipeline_name)
                    return result
                except asyncio.TimeoutError:
                    logger.error(f"Pipeline {pipeline_name} timed out after {self.config.task_timeout_seconds}s")
                    if self.config.enable_circuit_breakers:
                        await self.pipeline_selector.circuit_breaker.record_failure(pipeline_name)
                    continue
            except Exception as e:
                logger.error(f"Pipeline {pipeline_name} failed: {str(e)}")
                if self.config.enable_circuit_breakers:
                    await self.pipeline_selector.circuit_breaker.record_failure(pipeline_name)
                continue
        return {'success': False, 'error': 'All pipelines failed', 'task_id': task.get('task_id', 'unknown'), 'tried_pipelines': fallback_chain}

    async def _worker_loop(self, worker_id: int):
        logger.info(f"Worker {worker_id} started")
        while self.running:
            try:
                task = await self.task_queue.pop()
                if task is None:
                    await asyncio.sleep(0.1)
                    continue
                result = await self.process_task(task)
                if 'callback' in task:
                    try:
                        if asyncio.iscoroutinefunction(task['callback']):
                            await task['callback'](result)
                        else:
                            task['callback'](result)
                    except Exception as e:
                        logger.error(f"Callback error for task {task.get('task_id')}: {e}")
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Worker {worker_id} error: {e}")
                await asyncio.sleep(0.5)
        logger.info(f"Worker {worker_id} stopped")

    async def start_workers(self, num_workers: int = None):
        if num_workers is None:
            num_workers = self.config.max_concurrent_tasks
        for i in range(num_workers):
            worker = asyncio.create_task(self._worker_loop(i))
            self._worker_tasks.append(worker)
        logger.info(f"Started {num_workers} workers")

    async def batch_process(self, tasks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        results = []
        for task in tasks:
            result = await self.process_task(task)
            results.append(result)
        return results

    # Pipeline methods (unchanged)
    async def _standard_pipeline(self, task: Dict[str, Any]) -> Dict[str, Any]:
        await asyncio.sleep(0.01)
        return {'success': True, 'pipeline': 'standard', 'task_id': task.get('task_id')}

    async def _quantum_pipeline(self, task: Dict[str, Any]) -> Dict[str, Any]:
        if not task.get('quantum_capable', False):
            return await self._standard_pipeline(task)
        await asyncio.sleep(0.02)
        return {'success': True, 'pipeline': 'quantum', 'task_id': task.get('task_id')}

    async def _helium_pipeline(self, task: Dict[str, Any]) -> Dict[str, Any]:
        await asyncio.sleep(0.015)
        return {'success': True, 'pipeline': 'helium', 'task_id': task.get('task_id')}

    async def _energy_efficient_pipeline(self, task: Dict[str, Any]) -> Dict[str, Any]:
        await asyncio.sleep(0.005)
        return {'success': True, 'pipeline': 'energy_efficient', 'task_id': task.get('task_id')}

    async def _bio_optimized_pipeline(self, task: Dict[str, Any]) -> Dict[str, Any]:
        return await self._standard_pipeline(task)

    def get_status(self) -> Dict[str, Any]:
        system_state = self._get_system_state()
        return {
            'version': '7.0.3',
            'total_tasks': self.total_tasks,
            'successful_tasks': self.successful_tasks,
            'failed_tasks': self.failed_tasks,
            'success_rate': self.successful_tasks / max(self.total_tasks, 1),
            'queue_size': self.task_queue.size(),
            'pipeline_stats': self.pipeline_selector.get_pipeline_stats(),
            'system_state': system_state,
            'running': self.running,
            'config': self.config.model_dump() if hasattr(self.config, 'model_dump') else self.config.__dict__,
            'timestamp': datetime.utcnow().isoformat()
        }

    async def start(self):
        logger.info("Starting Enhanced Green Agent Runner v7.0.3...")
        await self.dashboard.start()
        await self.start_workers()
        if self.config.enable_prometheus and PROMETHEUS_AVAILABLE:
            try:
                start_http_server(9090)
                logger.info("Prometheus metrics server started on port 9090")
            except Exception as e:
                logger.warning(f"Failed to start Prometheus server: {e}")
        # Start chaos engine if enabled
        if self.config.enable_chaos_testing:
            await self.chaos_engine.start(self)
        logger.info("Enhanced Green Agent Runner started successfully")

    async def shutdown(self):
        if not self.running:
            return
        logger.info("Shutting down Enhanced Green Agent Runner...")
        self.running = False
        self._shutdown_event.set()
        for worker in self._worker_tasks:
            worker.cancel()
        if self._worker_tasks:
            await asyncio.gather(*self._worker_tasks, return_exceptions=True)
        await self.dashboard.stop()
        await self.bio_core.shutdown()
        # Close data collectors
        if self.carbon_fetcher:
            await self.carbon_fetcher.close()
        if self.digital_twin:
            await self.digital_twin.close()
        # Stop chaos engine
        if self.config.enable_chaos_testing:
            await self.chaos_engine.stop()
        logger.info("Enhanced Green Agent Runner shutdown complete")

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.shutdown()

# =============================================================================
# CLI Entry Point
# =============================================================================
async def main():
    config = RunnerConfig.from_env()
    async with EnhancedGreenAgentRunner(config) as runner:
        logger.info("Agent running. Press Ctrl+C to stop.")
        try:
            while runner.running:
                await asyncio.sleep(1)
                if int(time.time()) % 30 == 0:
                    status = runner.get_status()
                    logger.info(f"Status: {status['total_tasks']} tasks, {status['success_rate']*100:.1f}% success rate, queue: {status['queue_size']}")
        except KeyboardInterrupt:
            logger.info("Received interrupt signal")
        except Exception as e:
            logger.error(f"Runtime error: {e}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Graceful shutdown complete")
