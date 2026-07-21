# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/bio_inspired/bio_integrated_agent.py
# Enhanced version v9.0.0 – Full integration with bio‑inspired core, event‑driven, analytics‑aware

"""
Bio‑Integrated Green Agent v9.0.0
Full integration of bio‑inspired modules with:
- Energy‑aware RL strategy selector (Q‑learning)
- Persistent PQC keys for verifiable identity
- Selective blockchain auditing based on sustainability impact
- Event‑driven state updates via core EventBroker
- Reward enrichment from core analytics (alerts, anomalies, cost‑benefit)
- Strategy propagation to core configuration, scheduler tier, workflows
- Circuit breakers for resilient service calls
- Automatic Q‑table recovery on startup
- Correlation ID tracing
- Self‑healing trigger on critical alerts
"""

import asyncio
import logging
import json
import os
import hashlib
import uuid
from typing import Dict, Any, List, Optional, Tuple, Callable, Union
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from collections import defaultdict
import numpy as np
import pickle

# ============================================================================
# Try optional dependencies
# ============================================================================
try:
    from pydantic import BaseModel, Field, validator
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

try:
    import structlog
    logger = structlog.get_logger(__name__)
except ImportError:
    logger = logging.getLogger(__name__)

try:
    from prometheus_client import Gauge, Counter, Histogram
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

# ============================================================================
# Local imports from bio‑inspired core (with fallback)
# ============================================================================
try:
    from .eco_atp_currency import EcoATPTokenManager, EcoATPConsumer, EcoATPSource
    TOKEN_AVAILABLE = True
except ImportError:
    TOKEN_AVAILABLE = False

try:
    from .proton_gradient_fields import GradientFieldManager
    GRADIENT_AVAILABLE = True
except ImportError:
    GRADIENT_AVAILABLE = False

try:
    from .atp_synthase_scheduler import ATPSynthaseScheduler
    ATP_AVAILABLE = True
except ImportError:
    ATP_AVAILABLE = False

try:
    from .chromatophore_compartments import HierarchicalCompartmentManager
    COMPARTMENT_AVAILABLE = True
except ImportError:
    COMPARTMENT_AVAILABLE = False

try:
    from .biomass_storage import BiomassStorage
    BIOMASS_AVAILABLE = True
except ImportError:
    BIOMASS_AVAILABLE = False

try:
    from .photosynthetic_harvester import PhotosyntheticHarvester
    HARVESTER_AVAILABLE = True
except ImportError:
    HARVESTER_AVAILABLE = False

try:
    from .time_tick_engine import TimeTickEngine
    TICK_ENGINE_AVAILABLE = True
except ImportError:
    TICK_AVAILABLE = False

try:
    from .quantum_bridge import QuantumBridge
    QUANTUM_BRIDGE_AVAILABLE = True
except ImportError:
    QUANTUM_BRIDGE_AVAILABLE = False

try:
    from .__init__ import EnhancedBioInspiredCore, BioEvent, CircuitBreaker
    CORE_AVAILABLE = True
except ImportError:
    CORE_AVAILABLE = False

# PQC (post‑quantum cryptography) – use a simple fallback if not available
try:
    from pqcrypto.sign import falcon
    PQC_AVAILABLE = True
except ImportError:
    PQC_AVAILABLE = False

# ============================================================================
# Fallback definitions if core not available
# ============================================================================
if not CORE_AVAILABLE:
    class CircuitBreaker:
        """Fallback circuit breaker."""
        def __init__(self, name: str, failure_threshold: int = 3, recovery_timeout: float = 30.0):
            self.name = name
            self.failure_threshold = failure_threshold
            self.recovery_timeout = recovery_timeout
            self._state = "closed"
            self._failure_count = 0
            self._last_failure_time = None
            self._lock = asyncio.Lock()

        async def call(self, func: Callable, *args, **kwargs):
            async with self._lock:
                if self._state == "open":
                    if (datetime.now(timezone.utc) - self._last_failure_time).total_seconds() > self.recovery_timeout:
                        self._state = "half_open"
                    else:
                        raise Exception(f"Circuit breaker {self.name} is OPEN")
            try:
                result = await func(*args, **kwargs)
                async with self._lock:
                    self._state = "closed"
                    self._failure_count = 0
                return result
            except Exception as e:
                async with self._lock:
                    self._failure_count += 1
                    self._last_failure_time = datetime.now(timezone.utc)
                    if self._failure_count >= self.failure_threshold:
                        self._state = "open"
                raise e

    @dataclass
    class BioEvent:
        event_type: str
        source: str
        timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
        data: Dict[str, Any] = field(default_factory=dict)
        correlation_id: Optional[str] = None
        priority: int = 0

# ============================================================================
# Configuration (Pydantic or dataclass)
# ============================================================================
if PYDANTIC_AVAILABLE:
    class AgentConfig(BaseModel):
        """Configuration for the Bio‑Integrated Agent."""
        # General
        agent_id: str = Field(default_factory=lambda: f"agent_{uuid.uuid4().hex[:8]}")
        enable_energy_aware_rl: bool = True
        enable_quantum_bridge: bool = True
        enable_time_tick_engine: bool = True

        # RL strategy
        rl_learning_rate: float = 0.1
        rl_discount_factor: float = 0.9
        rl_epsilon: float = 0.1
        rl_state_bins: Dict[str, List[str]] = Field(
            default_factory=lambda: {
                'load': ['low', 'medium', 'high'],
                'health': ['poor', 'medium', 'good'],
                'token': ['scarce', 'adequate', 'abundant'],
                'energy': ['light', 'normal', 'heavy'],
                'helium': ['scarce', 'normal', 'abundant']
            }
        )
        rl_strategies: List[str] = ['conservative', 'balanced', 'performance']

        # Energy policies (strategy → config overrides)
        strategy_policies: Dict[str, Dict[str, Any]] = Field(
            default_factory=lambda: {
                'conservative': {
                    'state_save_interval_seconds': 600,
                    'health_check_interval_seconds': 60,
                    'task_throughput': 0.3,
                    'token_base_generation_rate': 0.5,
                    'biomass_storage_preference': True,
                    'genetic_evolution_interval': 86400 * 2,
                    'competition_interval': 7200,
                },
                'balanced': {
                    'state_save_interval_seconds': 300,
                    'health_check_interval_seconds': 30,
                    'task_throughput': 1.0,
                    'token_base_generation_rate': 1.0,
                    'biomass_storage_preference': False,
                    'genetic_evolution_interval': 86400,
                    'competition_interval': 3600,
                },
                'performance': {
                    'state_save_interval_seconds': 60,
                    'health_check_interval_seconds': 10,
                    'task_throughput': 2.0,
                    'token_base_generation_rate': 1.5,
                    'biomass_storage_preference': False,
                    'genetic_evolution_interval': 43200,
                    'competition_interval': 1800,
                }
            }
        )

        # PQC keys
        pqc_key_dir: str = Field("./pqc_keys", description="Directory for PQC key storage")

        # Blockchain audit policy
        blockchain_audit_events: List[str] = Field(
            default_factory=lambda: ['strategy_change', 'anomaly', 'module_retirement', 'daily_snapshot']
        )
        blockchain_audit_min_importance: float = 0.5  # 0–1 threshold

        # Persistence
        state_save_interval_seconds: int = 300
        state_save_path: str = "./agent_state.pkl"

        # Feature flags
        enable_prometheus: bool = False

        class Config:
            env_prefix = "AGENT_"
else:
    @dataclass
    class AgentConfig:
        agent_id: str = field(default_factory=lambda: f"agent_{uuid.uuid4().hex[:8]}")
        enable_energy_aware_rl: bool = True
        enable_quantum_bridge: bool = True
        enable_time_tick_engine: bool = True
        rl_learning_rate: float = 0.1
        rl_discount_factor: float = 0.9
        rl_epsilon: float = 0.1
        rl_state_bins: Dict[str, List[str]] = field(default_factory=lambda: {
            'load': ['low', 'medium', 'high'],
            'health': ['poor', 'medium', 'good'],
            'token': ['scarce', 'adequate', 'abundant'],
            'energy': ['light', 'normal', 'heavy'],
            'helium': ['scarce', 'normal', 'abundant']
        })
        rl_strategies: List[str] = field(default_factory=lambda: ['conservative', 'balanced', 'performance'])
        strategy_policies: Dict[str, Dict[str, Any]] = field(default_factory=lambda: {
            'conservative': {
                'state_save_interval_seconds': 600,
                'health_check_interval_seconds': 60,
                'task_throughput': 0.3,
                'token_base_generation_rate': 0.5,
                'biomass_storage_preference': True,
                'genetic_evolution_interval': 86400 * 2,
                'competition_interval': 7200,
            },
            'balanced': {
                'state_save_interval_seconds': 300,
                'health_check_interval_seconds': 30,
                'task_throughput': 1.0,
                'token_base_generation_rate': 1.0,
                'biomass_storage_preference': False,
                'genetic_evolution_interval': 86400,
                'competition_interval': 3600,
            },
            'performance': {
                'state_save_interval_seconds': 60,
                'health_check_interval_seconds': 10,
                'task_throughput': 2.0,
                'token_base_generation_rate': 1.5,
                'biomass_storage_preference': False,
                'genetic_evolution_interval': 43200,
                'competition_interval': 1800,
            }
        })
        pqc_key_dir: str = "./pqc_keys"
        blockchain_audit_events: List[str] = field(default_factory=lambda: ['strategy_change', 'anomaly', 'module_retirement', 'daily_snapshot'])
        blockchain_audit_min_importance: float = 0.5
        state_save_interval_seconds: int = 300
        state_save_path: str = "./agent_state.pkl"
        enable_prometheus: bool = False

# ============================================================================
# Quantum‑Resilient Security (with persistent keys)
# ============================================================================
class QuantumResilientSecurity:
    """
    Provides post‑quantum signature capabilities with persistent key storage.
    """
    def __init__(self, config: AgentConfig):
        self.config = config
        self.pqc_key_dir = Path(config.pqc_key_dir)
        self.pqc_key_dir.mkdir(parents=True, exist_ok=True)
        self.private_key = None
        self.public_key = None
        self._load_or_generate_keys()

    def _load_or_generate_keys(self):
        """Load existing PQC keys or generate new ones."""
        priv_path = self.pqc_key_dir / "private.key"
        pub_path = self.pqc_key_dir / "public.key"
        if priv_path.exists() and pub_path.exists():
            try:
                with open(priv_path, 'rb') as f:
                    self.private_key = f.read()
                with open(pub_path, 'rb') as f:
                    self.public_key = f.read()
                logger.info("Loaded existing PQC keys")
                return
            except Exception as e:
                logger.warning(f"Failed to load PQC keys: {e}")

        # Generate new keys
        if PQC_AVAILABLE:
            self.private_key, self.public_key = falcon.generate_keypair()
            with open(priv_path, 'wb') as f:
                f.write(self.private_key)
            with open(pub_path, 'wb') as f:
                f.write(self.public_key)
            logger.info("Generated and saved new PQC keys")
        else:
            # Fallback: use SHA‑256 HMAC as a placeholder
            self.private_key = os.urandom(32)
            self.public_key = hashlib.sha256(self.private_key).digest()
            with open(priv_path, 'wb') as f:
                f.write(self.private_key)
            with open(pub_path, 'wb') as f:
                f.write(self.public_key)
            logger.warning("PQC library not available; using fallback HMAC keys")

    def sign_data(self, data: Dict[str, Any]) -> str:
        """Sign data with the persistent private key."""
        payload = json.dumps(data, sort_keys=True, default=str).encode()
        if PQC_AVAILABLE:
            signature = falcon.sign(payload, self.private_key)
            return signature.hex()
        else:
            import hmac
            signature = hmac.new(self.private_key, payload, hashlib.sha256).hexdigest()
            return signature

    def verify_signature(self, data: Dict[str, Any], signature: str) -> bool:
        """Verify a signature with the persistent public key."""
        payload = json.dumps(data, sort_keys=True, default=str).encode()
        if PQC_AVAILABLE:
            try:
                falcon.verify(payload, bytes.fromhex(signature), self.public_key)
                return True
            except Exception:
                return False
        else:
            import hmac
            expected = hmac.new(self.private_key, payload, hashlib.sha256).hexdigest()
            return hmac.compare_digest(expected, signature)

# ============================================================================
# Blockchain Auditor (with selective auditing)
# ============================================================================
class BlockchainAuditor:
    """
    Records important events on a simulated blockchain with selective auditing.
    """
    def __init__(self, config: AgentConfig, security: QuantumResilientSecurity):
        self.config = config
        self.security = security
        self.ledger = []
        self._lock = asyncio.Lock()

    async def record_event(self, event_type: str, payload: Dict[str, Any], importance: float = 0.5) -> bool:
        """
        Record an event if it meets the audit policy.
        Returns True if recorded.
        """
        if event_type not in self.config.blockchain_audit_events:
            logger.debug(f"Event {event_type} not in audit list; skipping")
            return False

        if importance < self.config.blockchain_audit_min_importance:
            logger.debug(f"Event importance {importance} below threshold; skipping")
            return False

        signature = self.security.sign_data(payload)
        entry = {
            'event_type': event_type,
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'payload': payload,
            'signature': signature,
            'hash': hashlib.sha256(json.dumps(payload, default=str).encode()).hexdigest()
        }
        async with self._lock:
            self.ledger.append(entry)
        logger.info(f"Audit recorded: {event_type} (importance {importance})")
        return True

    def get_ledger(self, limit: int = 100) -> List[Dict]:
        return self.ledger[-limit:]

    def verify_entry(self, entry: Dict) -> bool:
        payload = entry['payload']
        signature = entry['signature']
        return self.security.verify_signature(payload, signature)

# ============================================================================
# RL Strategy Selector (Q‑Learning with extended state)
# ============================================================================
class RLStrategySelector:
    """
    Q‑learning strategy selector that is energy‑aware.
    State includes energy_intensity, helium_level, carbon_leakage_proxy.
    """
    def __init__(self, config: AgentConfig):
        self.config = config
        self.q_table: Dict[str, Dict[str, float]] = defaultdict(lambda: {s: 0.0 for s in config.rl_strategies})
        self.learning_rate = config.rl_learning_rate
        self.discount_factor = config.rl_discount_factor
        self.epsilon = config.rl_epsilon
        self.last_state_key = None
        self.last_action = None
        self.actions = config.rl_strategies
        self.state_bins = config.rl_state_bins

    def _state_to_key(self, state: Dict[str, float]) -> str:
        """
        Discretize continuous state values into bins.
        State keys:
        - system_load
        - health_score
        - token_balance
        - energy_intensity  (new)
        - helium_level      (new)
        - carbon_leakage_proxy (new)
        """
        load = state.get('system_load', 0.5)
        health = state.get('health_score', 0.8)
        token = state.get('token_balance', 0)
        energy = state.get('energy_intensity', 0.5)
        helium = state.get('helium_level', 0.5)
        carbon = state.get('carbon_leakage_proxy', 0.3)

        load_bin = 'high' if load > 0.7 else 'medium' if load > 0.4 else 'low'
        health_bin = 'good' if health > 0.7 else 'medium' if health > 0.4 else 'poor'
        token_bin = 'abundant' if token > 1000 else 'adequate' if token > 100 else 'scarce'
        energy_bin = 'heavy' if energy > 0.7 else 'normal' if energy > 0.4 else 'light'
        helium_bin = 'scarce' if helium < 0.3 else 'normal' if helium < 0.7 else 'abundant'
        carbon_bin = 'high' if carbon > 0.6 else 'medium' if carbon > 0.3 else 'low'

        return f"{load_bin}_{health_bin}_{token_bin}_{energy_bin}_{helium_bin}_{carbon_bin}"

    def select_action(self, state: Dict[str, float]) -> str:
        key = self._state_to_key(state)
        if key not in self.q_table:
            self.q_table[key] = {s: 0.0 for s in self.actions}

        if np.random.random() < self.epsilon:
            action = np.random.choice(self.actions)
        else:
            q_vals = self.q_table[key]
            max_q = max(q_vals.values())
            best_actions = [a for a, q in q_vals.items() if q == max_q]
            action = np.random.choice(best_actions)

        self.last_state_key = key
        self.last_action = action
        return action

    def update(self, state: Dict[str, float], action: str, reward: float, next_state: Dict[str, float]):
        if self.last_state_key is None or self.last_action is None:
            return
        key = self._state_to_key(state)
        next_key = self._state_to_key(next_state)

        if key not in self.q_table:
            self.q_table[key] = {s: 0.0 for s in self.actions}
        if next_key not in self.q_table:
            self.q_table[next_key] = {s: 0.0 for s in self.actions}

        max_next = max(self.q_table[next_key].values())
        current_q = self.q_table[key][action]
        self.q_table[key][action] = current_q + self.learning_rate * (
            reward + self.discount_factor * max_next - current_q
        )

    def get_q_table_size(self) -> int:
        return len(self.q_table)

    def get_best_strategy(self, state: Dict[str, float]) -> str:
        key = self._state_to_key(state)
        if key not in self.q_table:
            return 'balanced'
        q_vals = self.q_table[key]
        return max(q_vals, key=q_vals.get)

# ============================================================================
# Task Manager (for background loops)
# ============================================================================
class TaskManager:
    """Manages background tasks with restart and exponential backoff."""
    def __init__(self):
        self.tasks: Dict[str, asyncio.Task] = {}
        self.shutdown_event = asyncio.Event()
        self._lock = asyncio.Lock()

    def start_task(self, name: str, coro_func, *args, **kwargs):
        async def wrapper():
            backoff = 1
            max_backoff = 300
            while not self.shutdown_event.is_set():
                try:
                    await coro_func(*args, **kwargs)
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error("Task crashed", name=name, error=str(e), exc_info=True)
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, max_backoff)
        task = asyncio.create_task(wrapper(), name=name)
        async with self._lock:
            self.tasks[name] = task
        return task

    async def stop_all(self):
        self.shutdown_event.set()
        async with self._lock:
            for task in self.tasks.values():
                task.cancel()
            await asyncio.gather(*self.tasks.values(), return_exceptions=True)
            self.tasks.clear()
        logger.info("All background tasks stopped")

# ============================================================================
# Core Bio‑Integrated Agent
# ============================================================================
class BioIntegratedAgent:
    """
    Main agent that orchestrates all bio‑inspired modules, with energy‑aware RL,
    persistent PQC keys, selective blockchain auditing, and full integration with
    the bio‑inspired core's event, analytics, and self‑healing capabilities.
    """

    def __init__(
        self,
        bio_core: Optional[Any] = None,  # EnhancedBioInspiredCore instance
        config: Optional[Union[AgentConfig, Dict[str, Any]]] = None,
        csv_path: Optional[str] = None,
        quantum_graph: Optional[Any] = None,
        token_manager: Optional[Any] = None,
        gradient_manager: Optional[Any] = None,
        scheduler: Optional[Any] = None,
        compartment_manager: Optional[Any] = None,
        biomass_storage: Optional[Any] = None,
        harvester: Optional[Any] = None,
        tick_engine: Optional[Any] = None,
        quantum_bridge: Optional[Any] = None,
    ):
        # Load config
        if isinstance(config, dict):
            if PYDANTIC_AVAILABLE:
                self.config = AgentConfig(**config)
            else:
                self.config = AgentConfig(**config)
        elif isinstance(config, AgentConfig):
            self.config = config
        else:
            self.config = AgentConfig()

        # Store core reference
        self.bio_core = bio_core

        # Inject dependencies or create defaults
        self.token_manager = token_manager or (EcoATPTokenManager() if TOKEN_AVAILABLE else None)
        self.gradient_manager = gradient_manager or (GradientFieldManager() if GRADIENT_AVAILABLE else None)
        self.scheduler = scheduler or (ATPSynthaseScheduler(self.token_manager, self.gradient_manager) if ATP_AVAILABLE else None)
        self.compartment_manager = compartment_manager or (HierarchicalCompartmentManager(self.token_manager) if COMPARTMENT_AVAILABLE else None)
        self.biomass_storage = biomass_storage or (BiomassStorage(self.token_manager, self.gradient_manager) if BIOMASS_AVAILABLE else None)
        self.harvester = harvester or (PhotosyntheticHarvester(self.token_manager) if HARVESTER_AVAILABLE else None)

        # Optional advanced modules
        self.tick_engine = tick_engine
        if self.config.enable_time_tick_engine and csv_path and TICK_ENGINE_AVAILABLE:
            from .time_tick_engine import TimeTickEngine
            self.tick_engine = TimeTickEngine(
                csv_path=csv_path,
                harvester=self.harvester,
                translator_class=HeliumEnvironmentTranslator
            )
        self.quantum_bridge = quantum_bridge
        if self.config.enable_quantum_bridge and QUANTUM_BRIDGE_AVAILABLE and quantum_graph:
            from .quantum_bridge import QuantumBridge
            self.quantum_bridge = QuantumBridge(self.gradient_manager, quantum_graph)

        # Security and auditing
        self.security = QuantumResilientSecurity(self.config)
        self.auditor = BlockchainAuditor(self.config, self.security)

        # RL strategy selector
        self.strategy_selector = RLStrategySelector(self.config) if self.config.enable_energy_aware_rl else None
        self.current_strategy = 'balanced'
        self.strategy_change_time = datetime.now(timezone.utc)

        # State and metrics
        self.state = self._get_initial_state()
        self.metrics = {
            'strategy_changes': 0,
            'total_reward': 0.0,
            'energy_efficiency': 0.0,
            'helium_efficiency': 0.0,
        }

        # Circuit breakers for external services
        self._token_circuit = CircuitBreaker("token_service")
        self._gradient_circuit = CircuitBreaker("gradient_service")

        # Correlation ID for tracing
        self.correlation_id = str(uuid.uuid4())

        # Access to core sub‑modules if available
        if self.bio_core:
            self.event_broker = getattr(self.bio_core, 'event_broker', None)
            self.self_healer = getattr(self.bio_core, 'self_healer', None)
            self.alert_system = getattr(self.bio_core, 'alert_system', None)
            self.anomaly_detection = getattr(self.bio_core, 'anomaly_detection', None)
            self.cost_benefit_engine = getattr(self.bio_core, 'cost_benefit_engine', None)
            self.workflow_orchestrator = getattr(self.bio_core, 'workflow_orchestrator', None)
            self.swarm_coordinator = getattr(self.bio_core, 'swarm_coordinator', None)

            # Subscribe to core events
            if self.event_broker:
                self._subscribe_events()
        else:
            self.event_broker = None
            self.self_healer = None
            self.alert_system = None
            self.anomaly_detection = None
            self.cost_benefit_engine = None
            self.workflow_orchestrator = None
            self.swarm_coordinator = None

        # Background tasks
        self._task_manager = TaskManager()
        self._task_manager.start_task("strategy_loop", self._strategy_update_loop)
        self._task_manager.start_task("state_save", self._state_save_loop)
        self._task_manager.start_task("daily_snapshot", self._daily_snapshot_loop)

        # Load saved state (including Q‑table) on startup
        asyncio.create_task(self.load_state())

        logger.info(f"BioIntegratedAgent initialized with ID {self.config.agent_id}, correlation_id={self.correlation_id}")

    def _subscribe_events(self):
        """Subscribe to core events for state updates."""
        if self.event_broker:
            self.event_broker.subscribe('token_balance_update', self._on_token_update)
            self.event_broker.subscribe('gradient_update', self._on_gradient_update)
            self.event_broker.subscribe('alert_generated', self._on_alert_generated)
            self.event_broker.subscribe('helium_update', self._on_helium_update)
            self.event_broker.subscribe('anomaly_detected', self._on_anomaly_detected)
            logger.info("Subscribed to core events")

    async def _on_token_update(self, event: BioEvent):
        self.state['token_balance'] = event.data.get('balance', 500)
        await self._maybe_update_strategy()

    async def _on_gradient_update(self, event: BioEvent):
        field = event.data.get('field', 'carbon')
        strength = event.data.get('strength', 0.5)
        if field == 'helium':
            self.state['helium_level'] = strength
        elif field == 'carbon':
            self.state['carbon_leakage_proxy'] = strength
        await self._maybe_update_strategy()

    async def _on_helium_update(self, event: BioEvent):
        self.state['helium_level'] = event.data.get('helium_level', 0.5)
        await self._maybe_update_strategy()

    async def _on_anomaly_detected(self, event: BioEvent):
        # Anomalies may influence strategy
        await self._maybe_update_strategy()

    async def _on_alert_generated(self, event: BioEvent):
        if event.data.get('severity') == 'critical':
            logger.warning("Critical alert received; switching to conservative and triggering healing")
            await self.apply_strategy('conservative')
            if self.self_healer:
                await self.self_healer.apply_healing('damage_accumulation')

    async def _maybe_update_strategy(self):
        """Check if strategy should be re‑evaluated immediately based on state changes."""
        # This can be called on each event; we may want to throttle.
        # For simplicity, we let the strategy loop handle it periodically.
        pass

    def _get_initial_state(self) -> Dict[str, float]:
        return {
            'system_load': 0.5,
            'health_score': 0.8,
            'token_balance': 500,
            'energy_intensity': 0.5,
            'helium_level': 0.5,
            'carbon_leakage_proxy': 0.3,
        }

    async def get_strategy_state(self) -> Dict[str, float]:
        """
        Aggregate current system metrics into a state vector for RL.
        Includes energy and helium signals from services and tick engine.
        Uses circuit breakers for resilience.
        """
        state = {}

        # Token balance
        if self.token_manager:
            try:
                summary = await self._token_circuit.call(self.token_manager.get_system_summary)
                state['token_balance'] = summary.get('total_balance', 500)
            except Exception as e:
                logger.warning(f"Failed to get token summary: {e}")
                state['token_balance'] = self.state.get('token_balance', 500)
        else:
            state['token_balance'] = 500

        # System load
        if self.scheduler:
            try:
                stats = await self._token_circuit.call(self.scheduler.get_scheduler_stats)
                state['system_load'] = stats.get('demand_level', 0.5)
            except Exception:
                state['system_load'] = self.state.get('system_load', 0.5)
        elif self.compartment_manager:
            try:
                stats = await self._token_circuit.call(self.compartment_manager.get_ecosystem_stats)
                state['system_load'] = 1.0 - stats.get('viable_compartments', 0) / max(stats.get('total_compartments', 1), 1)
            except Exception:
                state['system_load'] = self.state.get('system_load', 0.5)
        else:
            state['system_load'] = 0.5

        # Health score
        if self.harvester:
            try:
                stats = await self._token_circuit.call(self.harvester.get_harvesting_stats)
                health = stats.get('pigment_health', {})
                avg_health = np.mean([h.get('efficiency', 0.5) for h in health.values()]) if health else 0.8
                state['health_score'] = avg_health
            except Exception:
                state['health_score'] = self.state.get('health_score', 0.8)
        else:
            state['health_score'] = 0.8

        # Energy intensity
        if self.token_manager:
            try:
                summary = await self._token_circuit.call(self.token_manager.get_system_summary)
                total_generated = summary.get('total_generated', 0)
                total_consumed = summary.get('total_consumed', 0)
                if total_generated > 0:
                    state['energy_intensity'] = total_consumed / total_generated
                else:
                    state['energy_intensity'] = 0.5
            except Exception:
                state['energy_intensity'] = self.state.get('energy_intensity', 0.5)
        else:
            state['energy_intensity'] = 0.5

        # Helium level
        helium_level = 0.5
        if self.tick_engine:
            try:
                if hasattr(self.tick_engine, 'get_current_helium'):
                    helium_level = self.tick_engine.get_current_helium()
                elif hasattr(self.tick_engine, 'current_data'):
                    helium_level = self.tick_engine.current_data.get('helium_supply', 0.5)
            except Exception:
                helium_level = self.state.get('helium_level', 0.5)
        elif self.gradient_manager:
            try:
                strengths = await self._gradient_circuit.call(self.gradient_manager.get_field_strengths)
                helium_level = strengths.get('helium', 0.5)
            except Exception:
                helium_level = self.state.get('helium_level', 0.5)
        state['helium_level'] = max(0.0, min(1.0, helium_level))

        # Carbon leakage proxy
        if self.gradient_manager:
            try:
                strengths = await self._gradient_circuit.call(self.gradient_manager.get_field_strengths)
                carbon = strengths.get('carbon', 0.5)
                state['carbon_leakage_proxy'] = max(0.0, min(1.0, carbon))
            except Exception:
                state['carbon_leakage_proxy'] = self.state.get('carbon_leakage_proxy', 0.3)
        else:
            state['carbon_leakage_proxy'] = 0.3

        return state

    async def _compute_reward(self, state: Dict[str, float]) -> float:
        """
        Compute reward based on sustainability metrics and core analytics.
        """
        # Base reward from energy, helium, carbon
        base = (
            + 0.4 * (1.0 - state.get('energy_intensity', 0.5))
            + 0.2 * state.get('helium_level', 0.5)
            - 0.2 * state.get('carbon_leakage_proxy', 0.3)
        )

        # Add core analytics if available
        if self.alert_system:
            alerts = await self.alert_system.get_active_alerts(severity='critical')
            base -= 0.2 * len(alerts)

        if self.anomaly_detection:
            anomalies = await self.anomaly_detection.get_recent_anomalies(limit=5)
            base -= 0.1 * len(anomalies)

        if self.cost_benefit_engine:
            stats = await self.cost_benefit_engine.get_analysis_stats()
            if stats.get('average_roi', 0) > 0.5:
                base += 0.1

        return base

    async def _strategy_update_loop(self):
        """Background loop that periodically updates RL strategy."""
        while True:
            try:
                # Get current state
                state = await self.get_strategy_state()
                self.state = state

                # Select strategy
                if self.strategy_selector:
                    action = self.strategy_selector.select_action(state)
                    await self.apply_strategy(action)
                    self.current_strategy = action
                    self.strategy_change_time = datetime.now(timezone.utc)
                    self.metrics['strategy_changes'] += 1

                    # After a delay, compute reward and update Q‑table
                    await asyncio.sleep(self.config.state_save_interval_seconds)
                    next_state = await self.get_strategy_state()
                    reward = await self._compute_reward(next_state)
                    self.metrics['total_reward'] += reward
                    self.strategy_selector.update(state, action, reward, next_state)

                    # Audit strategy change if important
                    importance = 0.7 if action != 'balanced' else 0.3
                    await self.auditor.record_event(
                        'strategy_change',
                        {'new_strategy': action, 'state': state, 'reward': reward},
                        importance=importance
                    )
                else:
                    # No RL: use balanced
                    await self.apply_strategy('balanced')

                # Update metrics
                await self._update_metrics(state)

                # Sleep until next cycle
                await asyncio.sleep(self.config.state_save_interval_seconds)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Strategy loop error", error=str(e))
                await asyncio.sleep(30)

    async def apply_strategy(self, strategy: str):
        """
        Apply the policies associated with the strategy to the system.
        Overrides relevant config parameters and propagates to modules.
        Also updates core configuration if available, sets degradation tier,
        and triggers workflows.
        """
        policy = self.config.strategy_policies.get(strategy, self.config.strategy_policies['balanced'])
        logger.info(f"Applying strategy '{strategy}' with policy: {policy}")

        # Update agent config
        for key, value in policy.items():
            if hasattr(self.config, key):
                setattr(self.config, key, value)

        # Propagate to core if available
        if self.bio_core and hasattr(self.bio_core, 'update_configuration'):
            await self.bio_core.update_configuration(policy)

        # Adjust scheduler degradation tier
        if self.scheduler and hasattr(self.scheduler, 'set_degradation_tier'):
            tier = 5 if strategy == 'conservative' else 3 if strategy == 'balanced' else 1
            self.scheduler.set_degradation_tier(tier)

        # Trigger workflow for performance mode
        if self.workflow_orchestrator and strategy == 'performance':
            await self.workflow_orchestrator.execute_workflow('token_generation')

        # Notify other modules as needed

    async def _state_save_loop(self):
        """Periodically save agent state to disk."""
        while True:
            try:
                await self.save_state()
                await asyncio.sleep(self.config.state_save_interval_seconds)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("State save error", error=str(e))
                await asyncio.sleep(60)

    async def _daily_snapshot_loop(self):
        """Take a daily snapshot and record on blockchain if important."""
        while True:
            try:
                await asyncio.sleep(86400)  # 24 hours
                snapshot = {
                    'timestamp': datetime.now(timezone.utc).isoformat(),
                    'state': self.state,
                    'metrics': self.metrics,
                    'strategy': self.current_strategy,
                    'agent_id': self.config.agent_id
                }
                # Record daily snapshot with importance 0.6 (above threshold)
                await self.auditor.record_event('daily_snapshot', snapshot, importance=0.6)
                logger.info("Daily snapshot recorded.")
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Daily snapshot error", error=str(e))
                await asyncio.sleep(3600)

    async def _update_metrics(self, state: Dict[str, float]):
        self.metrics['energy_efficiency'] = 1.0 - state.get('energy_intensity', 0.5)
        self.metrics['helium_efficiency'] = state.get('helium_level', 0.5)

    async def save_state(self):
        """Serialize agent state to disk."""
        state_data = {
            'agent_id': self.config.agent_id,
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'state': self.state,
            'metrics': self.metrics,
            'current_strategy': self.current_strategy,
            'strategy_change_time': self.strategy_change_time.isoformat(),
            'q_table': dict(self.strategy_selector.q_table) if self.strategy_selector else None,
            'correlation_id': self.correlation_id,
        }
        try:
            with open(self.config.state_save_path, 'wb') as f:
                pickle.dump(state_data, f)
            logger.debug("State saved.")
        except Exception as e:
            logger.error("Failed to save state", error=str(e))

    async def load_state(self, path: Optional[str] = None):
        """Load agent state from disk, including Q‑table."""
        path = path or self.config.state_save_path
        if not os.path.exists(path):
            logger.info("No saved state found; starting fresh.")
            return
        try:
            with open(path, 'rb') as f:
                data = pickle.load(f)
            self.state = data.get('state', self.state)
            self.metrics = data.get('metrics', self.metrics)
            self.current_strategy = data.get('current_strategy', 'balanced')
            if data.get('q_table') and self.strategy_selector:
                self.strategy_selector.q_table = defaultdict(dict, data['q_table'])
            self.correlation_id = data.get('correlation_id', self.correlation_id)
            logger.info("State loaded.")
        except Exception as e:
            logger.error("Failed to load state", error=str(e))

    async def shutdown(self):
        """Gracefully shut down all components."""
        logger.info("Shutting down BioIntegratedAgent")
        await self._task_manager.stop_all()
        # Save final state
        await self.save_state()
        # Close tick engine if any
        if self.tick_engine and hasattr(self.tick_engine, 'shutdown'):
            await self.tick_engine.shutdown()
        # Close quantum bridge if any
        if self.quantum_bridge and hasattr(self.quantum_bridge, 'shutdown'):
            await self.quantum_bridge.shutdown()
        logger.info("Agent shutdown complete")

# ============================================================================
# Example usage
# ============================================================================
async def example():
    """Example usage of the BioIntegratedAgent with a mock core."""
    # Create a mock core (if real core not available)
    class MockCore:
        def __init__(self):
            self.event_broker = None
            self.self_healer = None
            self.alert_system = None
            self.anomaly_detection = None
            self.cost_benefit_engine = None
            self.workflow_orchestrator = None
            self.swarm_coordinator = None
        async def update_configuration(self, policy):
            pass

    config = {
        'enable_energy_aware_rl': True,
        'enable_time_tick_engine': True,
        'enable_quantum_bridge': True,
        'pqc_key_dir': './pqc_keys',
        'state_save_path': './agent_state.pkl',
    }
    agent = BioIntegratedAgent(
        bio_core=MockCore(),
        config=config,
        csv_path="helium_data.csv"
    )
    # Let it run for a few seconds
    await asyncio.sleep(10)
    # Get current state
    state = await agent.get_strategy_state()
    print("Current state:", state)
    print("Current strategy:", agent.current_strategy)
    print("Metrics:", agent.metrics)
    await agent.shutdown()

if __name__ == "__main__":
    asyncio.run(example())
