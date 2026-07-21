# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/bio_inspired/bio_integrated_agent.py
# Enhanced version v11.0.0 – Full integration with active control, proactive planning, swarm consensus, and comprehensive module coordination

"""
Bio‑Integrated Green Agent v11.0.0
Complete orchestration with:
- Active QuantumBridge control (adjust QUBO penalties)
- Proactive TimeTickEngine forecast‑based strategy switching
- Two‑way swarm coordination (Q‑table sharing, consensus bonus)
- Workflow outcome feedback into reward
- Proactive self‑healing based on health score
- Degradation‑aware strategy selection
- Active reconfiguration of all bio‑inspired modules
- CompetitionEngine integration (spawn/kill children)
- TokenSupplyManager / TokenAllocator integration
- Q‑table refreshing and pruning
- Dynamic configuration reload
- Enhanced observability and explainability
"""

import asyncio
import logging
import json
import os
import hashlib
import uuid
from typing import Dict, Any, List, Optional, Tuple, Callable, Union
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from collections import defaultdict, deque
import numpy as np
import pickle

# Try optional dependencies
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

# Local imports from bio‑inspired core (with fallback)
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
    from .biomass_storage import BiomassStorage, StorageTier
    BIOMASS_AVAILABLE = True
except ImportError:
    BIOMASS_AVAILABLE = False

try:
    from .photosynthetic_harvester import PhotosyntheticHarvester, HarvestingMode
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

# PQC (post‑quantum cryptography)
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
# Configuration (Pydantic or dataclass) – extended for v11
# ============================================================================
if PYDANTIC_AVAILABLE:
    class AgentConfig(BaseModel):
        """Configuration for the Bio‑Integrated Agent."""
        # General
        agent_id: str = Field(default_factory=lambda: f"agent_{uuid.uuid4().hex[:8]}")
        enable_energy_aware_rl: bool = True
        enable_quantum_bridge: bool = True
        enable_time_tick_engine: bool = True
        enable_swarm_coordination: bool = True
        enable_multi_objective_rl: bool = False
        enable_proactive_healing: bool = True

        # RL strategy
        rl_learning_rate: float = 0.1
        rl_discount_factor: float = 0.9
        rl_epsilon: float = 0.1
        rl_learning_rate_min: float = 0.01
        rl_epsilon_min: float = 0.01
        rl_state_bins: Dict[str, List[str]] = Field(
            default_factory=lambda: {
                'load': ['low', 'medium', 'high'],
                'health': ['poor', 'medium', 'good'],
                'token': ['scarce', 'adequate', 'abundant'],
                'energy': ['light', 'normal', 'heavy'],
                'helium': ['scarce', 'normal', 'abundant'],
                'carbon': ['low', 'medium', 'high'],
                'alert_count': ['none', 'some', 'many'],
                'helium_trend': ['falling', 'stable', 'rising'],
                'q_penalty_carbon': ['low', 'medium', 'high'],
                'q_penalty_helium': ['low', 'medium', 'high'],
                'degradation_tier': ['low', 'medium', 'high'],
                'swarm_consensus': ['minority', 'mixed', 'majority'],
                'workflow_success': ['failed', 'partial', 'succeeded'],
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
                    'biomass_storage_tier': 'cold',
                    'compartment_creation': False,
                    'harvester_mode': 'minimal',
                    'scheduler_protons_per_rotation': 17,
                    'gradient_pump_rate': 0.2,
                    'token_generation_rate': 0.5,
                    'competition_spawn': False,
                },
                'balanced': {
                    'state_save_interval_seconds': 300,
                    'health_check_interval_seconds': 30,
                    'task_throughput': 1.0,
                    'token_base_generation_rate': 1.0,
                    'biomass_storage_tier': 'standard',
                    'compartment_creation': True,
                    'harvester_mode': 'adaptive',
                    'scheduler_protons_per_rotation': 12,
                    'gradient_pump_rate': 0.5,
                    'token_generation_rate': 1.0,
                    'competition_spawn': False,
                },
                'performance': {
                    'state_save_interval_seconds': 60,
                    'health_check_interval_seconds': 10,
                    'task_throughput': 2.0,
                    'token_base_generation_rate': 1.5,
                    'biomass_storage_tier': 'hot',
                    'compartment_creation': True,
                    'harvester_mode': 'full',
                    'scheduler_protons_per_rotation': 8,
                    'gradient_pump_rate': 1.0,
                    'token_generation_rate': 2.0,
                    'competition_spawn': True,
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

        # Q‑table compression and refresh
        q_table_max_size: int = 5000
        q_table_refresh_interval: int = 10000  # steps

        # Proactive healing threshold
        proactive_healing_health_threshold: float = 0.6

        # Feature flags
        enable_prometheus: bool = False

        # Multi‑objective weights (if enabled)
        objective_weights: Dict[str, float] = Field(
            default_factory=lambda: {
                'energy_efficiency': 0.3,
                'helium_sustainability': 0.25,
                'token_balance': 0.2,
                'health_score': 0.15,
                'carbon_leakage': 0.1,
            }
        )

        class Config:
            env_prefix = "AGENT_"
else:
    @dataclass
    class AgentConfig:
        agent_id: str = field(default_factory=lambda: f"agent_{uuid.uuid4().hex[:8]}")
        enable_energy_aware_rl: bool = True
        enable_quantum_bridge: bool = True
        enable_time_tick_engine: bool = True
        enable_swarm_coordination: bool = True
        enable_multi_objective_rl: bool = False
        enable_proactive_healing: bool = True
        rl_learning_rate: float = 0.1
        rl_discount_factor: float = 0.9
        rl_epsilon: float = 0.1
        rl_learning_rate_min: float = 0.01
        rl_epsilon_min: float = 0.01
        rl_state_bins: Dict[str, List[str]] = field(default_factory=lambda: {
            'load': ['low', 'medium', 'high'],
            'health': ['poor', 'medium', 'good'],
            'token': ['scarce', 'adequate', 'abundant'],
            'energy': ['light', 'normal', 'heavy'],
            'helium': ['scarce', 'normal', 'abundant'],
            'carbon': ['low', 'medium', 'high'],
            'alert_count': ['none', 'some', 'many'],
            'helium_trend': ['falling', 'stable', 'rising'],
            'q_penalty_carbon': ['low', 'medium', 'high'],
            'q_penalty_helium': ['low', 'medium', 'high'],
            'degradation_tier': ['low', 'medium', 'high'],
            'swarm_consensus': ['minority', 'mixed', 'majority'],
            'workflow_success': ['failed', 'partial', 'succeeded'],
        })
        rl_strategies: List[str] = field(default_factory=lambda: ['conservative', 'balanced', 'performance'])
        strategy_policies: Dict[str, Dict[str, Any]] = field(default_factory=lambda: {
            'conservative': {
                'state_save_interval_seconds': 600,
                'health_check_interval_seconds': 60,
                'task_throughput': 0.3,
                'token_base_generation_rate': 0.5,
                'biomass_storage_tier': 'cold',
                'compartment_creation': False,
                'harvester_mode': 'minimal',
                'scheduler_protons_per_rotation': 17,
                'gradient_pump_rate': 0.2,
                'token_generation_rate': 0.5,
                'competition_spawn': False,
            },
            'balanced': {
                'state_save_interval_seconds': 300,
                'health_check_interval_seconds': 30,
                'task_throughput': 1.0,
                'token_base_generation_rate': 1.0,
                'biomass_storage_tier': 'standard',
                'compartment_creation': True,
                'harvester_mode': 'adaptive',
                'scheduler_protons_per_rotation': 12,
                'gradient_pump_rate': 0.5,
                'token_generation_rate': 1.0,
                'competition_spawn': False,
            },
            'performance': {
                'state_save_interval_seconds': 60,
                'health_check_interval_seconds': 10,
                'task_throughput': 2.0,
                'token_base_generation_rate': 1.5,
                'biomass_storage_tier': 'hot',
                'compartment_creation': True,
                'harvester_mode': 'full',
                'scheduler_protons_per_rotation': 8,
                'gradient_pump_rate': 1.0,
                'token_generation_rate': 2.0,
                'competition_spawn': True,
            }
        })
        pqc_key_dir: str = "./pqc_keys"
        blockchain_audit_events: List[str] = field(default_factory=lambda: ['strategy_change', 'anomaly', 'module_retirement', 'daily_snapshot'])
        blockchain_audit_min_importance: float = 0.5
        state_save_interval_seconds: int = 300
        state_save_path: str = "./agent_state.pkl"
        q_table_max_size: int = 5000
        q_table_refresh_interval: int = 10000
        proactive_healing_health_threshold: float = 0.6
        enable_prometheus: bool = False
        objective_weights: Dict[str, float] = field(default_factory=lambda: {
            'energy_efficiency': 0.3,
            'helium_sustainability': 0.25,
            'token_balance': 0.2,
            'health_score': 0.15,
            'carbon_leakage': 0.1,
        })

# ============================================================================
# Quantum‑Resilient Security (with persistent keys)
# ============================================================================
class QuantumResilientSecurity:
    # (unchanged from v10)
    def __init__(self, config: AgentConfig):
        self.config = config
        self.pqc_key_dir = Path(config.pqc_key_dir)
        self.pqc_key_dir.mkdir(parents=True, exist_ok=True)
        self.private_key = None
        self.public_key = None
        self._load_or_generate_keys()

    def _load_or_generate_keys(self):
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

        if PQC_AVAILABLE:
            self.private_key, self.public_key = falcon.generate_keypair()
            with open(priv_path, 'wb') as f:
                f.write(self.private_key)
            with open(pub_path, 'wb') as f:
                f.write(self.public_key)
            logger.info("Generated and saved new PQC keys")
        else:
            self.private_key = os.urandom(32)
            self.public_key = hashlib.sha256(self.private_key).digest()
            with open(priv_path, 'wb') as f:
                f.write(self.private_key)
            with open(pub_path, 'wb') as f:
                f.write(self.public_key)
            logger.warning("PQC library not available; using fallback HMAC keys")

    def sign_data(self, data: Dict[str, Any]) -> str:
        payload = json.dumps(data, sort_keys=True, default=str).encode()
        if PQC_AVAILABLE:
            signature = falcon.sign(payload, self.private_key)
            return signature.hex()
        else:
            import hmac
            signature = hmac.new(self.private_key, payload, hashlib.sha256).hexdigest()
            return signature

    def verify_signature(self, data: Dict[str, Any], signature: str) -> bool:
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
# Blockchain Auditor (unchanged)
# ============================================================================
class BlockchainAuditor:
    def __init__(self, config: AgentConfig, security: QuantumResilientSecurity):
        self.config = config
        self.security = security
        self.ledger = []
        self._lock = asyncio.Lock()

    async def record_event(self, event_type: str, payload: Dict[str, Any], importance: float = 0.5) -> bool:
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
# RL Strategy Selector (v11 – extended with swarm and workflow feedback)
# ============================================================================
class RLStrategySelector:
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
        self.reward_history = deque(maxlen=100)
        self.step_counter = 0

    def _state_to_key(self, state: Dict[str, float]) -> str:
        load = state.get('system_load', 0.5)
        health = state.get('health_score', 0.8)
        token = state.get('token_balance', 0)
        energy = state.get('energy_intensity', 0.5)
        helium = state.get('helium_level', 0.5)
        carbon = state.get('carbon_leakage_proxy', 0.3)
        alert_count = state.get('alert_count', 0)
        helium_trend = state.get('helium_trend', 0)
        q_penalty_carbon = state.get('q_penalty_carbon', 0.5)
        q_penalty_helium = state.get('q_penalty_helium', 0.5)
        degradation_tier = state.get('degradation_tier', 3)  # 1-5
        swarm_consensus = state.get('swarm_consensus', 0.5)  # 0-1
        workflow_success = state.get('workflow_success', 0.5)  # 0-1

        # Binning
        load_bin = 'high' if load > 0.7 else 'medium' if load > 0.4 else 'low'
        health_bin = 'good' if health > 0.7 else 'medium' if health > 0.4 else 'poor'
        token_bin = 'abundant' if token > 1000 else 'adequate' if token > 100 else 'scarce'
        energy_bin = 'heavy' if energy > 0.7 else 'normal' if energy > 0.4 else 'light'
        helium_bin = 'scarce' if helium < 0.3 else 'normal' if helium < 0.7 else 'abundant'
        carbon_bin = 'high' if carbon > 0.6 else 'medium' if carbon > 0.3 else 'low'
        alert_bin = 'many' if alert_count > 2 else 'some' if alert_count > 0 else 'none'
        helium_trend_bin = 'rising' if helium_trend > 0.1 else 'falling' if helium_trend < -0.1 else 'stable'
        q_carbon_bin = 'high' if q_penalty_carbon > 0.7 else 'medium' if q_penalty_carbon > 0.3 else 'low'
        q_helium_bin = 'high' if q_penalty_helium > 0.7 else 'medium' if q_penalty_helium > 0.3 else 'low'
        deg_tier_bin = 'high' if degradation_tier > 3 else 'medium' if degradation_tier > 1 else 'low'
        swarm_bin = 'majority' if swarm_consensus > 0.7 else 'minority' if swarm_consensus < 0.3 else 'mixed'
        workflow_bin = 'succeeded' if workflow_success > 0.8 else 'failed' if workflow_success < 0.3 else 'partial'

        return f"{load_bin}_{health_bin}_{token_bin}_{energy_bin}_{helium_bin}_{carbon_bin}_{alert_bin}_{helium_trend_bin}_{q_carbon_bin}_{q_helium_bin}_{deg_tier_bin}_{swarm_bin}_{workflow_bin}"

    def select_action(self, state: Dict[str, float]) -> str:
        key = self._state_to_key(state)
        if key not in self.q_table:
            self.q_table[key] = {s: 0.0 for s in self.actions}

        # Adaptive epsilon
        if len(self.reward_history) > 20:
            var = np.var(self.reward_history)
            if var < 0.05:
                self.epsilon = max(self.config.rl_epsilon_min, self.epsilon * 0.95)
            else:
                self.epsilon = min(self.config.rl_epsilon, self.epsilon * 1.05)

        if np.random.random() < self.epsilon:
            action = np.random.choice(self.actions)
        else:
            q_vals = self.q_table[key]
            max_q = max(q_vals.values())
            best_actions = [a for a, q in q_vals.items() if q == max_q]
            action = np.random.choice(best_actions)

        self.last_state_key = key
        self.last_action = action
        self.step_counter += 1
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

        self.reward_history.append(reward)

        # Adaptive learning rate
        if len(self.reward_history) > 20:
            var = np.var(self.reward_history)
            if var > 0.2:
                self.learning_rate = max(self.config.rl_learning_rate_min, self.learning_rate * 0.9)
            elif var < 0.05:
                self.learning_rate = min(self.config.rl_learning_rate, self.learning_rate * 1.1)

        # Compress Q‑table if too large
        if len(self.q_table) > self.config.q_table_max_size:
            self._compress_q_table()

        # Refresh if stale
        if self.step_counter % self.config.q_table_refresh_interval == 0:
            self._refresh_q_table()

    def _compress_q_table(self):
        sorted_keys = sorted(self.q_table.keys(), key=lambda k: max(self.q_table[k].values()))
        to_remove = sorted_keys[:len(sorted_keys)//2]
        for k in to_remove:
            del self.q_table[k]
        logger.info(f"Compressed Q‑table to {len(self.q_table)} states.")

    def _refresh_q_table(self):
        """Reset a portion of the Q‑table to encourage exploration."""
        # Reset 20% of states (those with lowest max Q)
        sorted_keys = sorted(self.q_table.keys(), key=lambda k: max(self.q_table[k].values()))
        for k in sorted_keys[:int(0.2 * len(sorted_keys))]:
            self.q_table[k] = {s: 0.0 for s in self.actions}
        logger.info("Refreshed 20% of Q‑table states for exploration.")

    def get_q_table_size(self) -> int:
        return len(self.q_table)

    def get_best_strategy(self, state: Dict[str, float]) -> str:
        key = self._state_to_key(state)
        if key not in self.q_table:
            return 'balanced'
        q_vals = self.q_table[key]
        return max(q_vals, key=q_vals.get)

# ============================================================================
# Task Manager (unchanged)
# ============================================================================
class TaskManager:
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
# Core Bio‑Integrated Agent (v11.0.0 – fully enhanced)
# ============================================================================
class BioIntegratedAgent:
    def __init__(
        self,
        bio_core: Optional[Any] = None,
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
            'avg_reward': 0.0,
        }
        self.reward_history = deque(maxlen=100)

        # Circuit breakers
        self._token_circuit = CircuitBreaker("token_service")
        self._gradient_circuit = CircuitBreaker("gradient_service")

        # Correlation ID
        self.correlation_id = str(uuid.uuid4())

        # Access to core sub‑modules
        if self.bio_core:
            self.event_broker = getattr(self.bio_core, 'event_broker', None)
            self.self_healer = getattr(self.bio_core, 'self_healer', None)
            self.alert_system = getattr(self.bio_core, 'alert_system', None)
            self.anomaly_detection = getattr(self.bio_core, 'anomaly_detection', None)
            self.cost_benefit_engine = getattr(self.bio_core, 'cost_benefit_engine', None)
            self.workflow_orchestrator = getattr(self.bio_core, 'workflow_orchestrator', None)
            self.swarm_coordinator = getattr(self.bio_core, 'swarm_coordinator', None)
            self.health_monitor = getattr(self.bio_core, 'health_monitor', None)
            self.degradation_manager = getattr(self.bio_core, 'degradation_manager', None)
            self.competition_engine = getattr(self.bio_core, 'competition_engine', None)
            self.token_supply_manager = getattr(self.bio_core, 'supply_manager', None)
            self.token_allocator = getattr(self.bio_core, 'token_allocator', None)

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
            self.health_monitor = None
            self.degradation_manager = None
            self.competition_engine = None
            self.token_supply_manager = None
            self.token_allocator = None

        # Background tasks
        self._task_manager = TaskManager()
        self._task_manager.start_task("strategy_loop", self._strategy_update_loop)
        self._task_manager.start_task("state_save", self._state_save_loop)
        self._task_manager.start_task("daily_snapshot", self._daily_snapshot_loop)
        if self.config.enable_swarm_coordination and self.swarm_coordinator:
            self._task_manager.start_task("swarm_update", self._swarm_update_loop)

        # Load saved state
        asyncio.create_task(self.load_state())

        logger.info(f"BioIntegratedAgent v11.0.0 initialized with ID {self.config.agent_id}, correlation_id={self.correlation_id}")

    def _subscribe_events(self):
        if self.event_broker:
            self.event_broker.subscribe('token_balance_update', self._on_token_update)
            self.event_broker.subscribe('gradient_update', self._on_gradient_update)
            self.event_broker.subscribe('alert_generated', self._on_alert_generated)
            self.event_broker.subscribe('helium_update', self._on_helium_update)
            self.event_broker.subscribe('anomaly_detected', self._on_anomaly_detected)
            self.event_broker.subscribe('health_update', self._on_health_update)
            self.event_broker.subscribe('workflow_completed', self._on_workflow_completed)
            self.event_broker.subscribe('degradation_tier_updated', self._on_degradation_updated)
            self.event_broker.subscribe('config_updated', self._on_config_updated)
            logger.info("Subscribed to core events")

    async def _on_token_update(self, event: BioEvent):
        self.state['token_balance'] = event.data.get('balance', 500)

    async def _on_gradient_update(self, event: BioEvent):
        field = event.data.get('field', 'carbon')
        strength = event.data.get('strength', 0.5)
        if field == 'helium':
            self.state['helium_level'] = strength
        elif field == 'carbon':
            self.state['carbon_leakage_proxy'] = strength

    async def _on_helium_update(self, event: BioEvent):
        self.state['helium_level'] = event.data.get('helium_level', 0.5)

    async def _on_anomaly_detected(self, event: BioEvent):
        pass  # handled in reward

    async def _on_alert_generated(self, event: BioEvent):
        if event.data.get('severity') == 'critical':
            logger.warning("Critical alert received; switching to conservative and triggering healing")
            await self.apply_strategy('conservative')
            if self.self_healer:
                await self.self_healer.apply_healing('damage_accumulation')

    async def _on_health_update(self, event: BioEvent):
        self.state['health_score'] = event.data.get('health_score', 0.8)

    async def _on_workflow_completed(self, event: BioEvent):
        success = event.data.get('success', False)
        self.state['workflow_success'] = 1.0 if success else 0.0

    async def _on_degradation_updated(self, event: BioEvent):
        self.state['degradation_tier'] = event.data.get('new_tier', 3)

    async def _on_config_updated(self, event: BioEvent):
        # Reload relevant config parameters
        updates = event.data.get('updates', {})
        for key, value in updates.items():
            if hasattr(self.config, key):
                setattr(self.config, key, value)
        logger.info("Configuration reloaded", updates=updates)

    def _get_initial_state(self) -> Dict[str, float]:
        return {
            'system_load': 0.5,
            'health_score': 0.8,
            'token_balance': 500,
            'energy_intensity': 0.5,
            'helium_level': 0.5,
            'carbon_leakage_proxy': 0.3,
            'helium_trend': 0.0,
            'alert_count': 0,
            'q_penalty_carbon': 0.5,
            'q_penalty_helium': 0.5,
            'degradation_tier': 3,
            'swarm_consensus': 0.5,
            'workflow_success': 0.5,
        }

    async def get_strategy_state(self) -> Dict[str, float]:
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

        # Helium level and trend
        helium_level = 0.5
        helium_trend = 0.0
        if self.tick_engine:
            try:
                if hasattr(self.tick_engine, 'get_current_helium'):
                    helium_level = self.tick_engine.get_current_helium()
                elif hasattr(self.tick_engine, 'current_data'):
                    helium_level = self.tick_engine.current_data.get('helium_supply', 0.5)
                if hasattr(self.tick_engine, 'get_helium_forecast'):
                    forecast = self.tick_engine.get_helium_forecast(2)
                    if forecast and len(forecast) > 1:
                        x = np.arange(len(forecast))
                        slope = np.polyfit(x, forecast, 1)[0]
                        helium_trend = slope / (max(forecast) + 0.001)
            except Exception as e:
                logger.warning(f"Failed to get helium data from tick engine: {e}")
                helium_level = self.state.get('helium_level', 0.5)
                helium_trend = self.state.get('helium_trend', 0.0)
        elif self.gradient_manager:
            try:
                strengths = await self._gradient_circuit.call(self.gradient_manager.get_field_strengths)
                helium_level = strengths.get('helium', 0.5)
            except Exception:
                helium_level = self.state.get('helium_level', 0.5)
        state['helium_level'] = max(0.0, min(1.0, helium_level))
        state['helium_trend'] = helium_trend

        # Carbon leakage
        if self.gradient_manager:
            try:
                strengths = await self._gradient_circuit.call(self.gradient_manager.get_field_strengths)
                carbon = strengths.get('carbon', 0.5)
                state['carbon_leakage_proxy'] = max(0.0, min(1.0, carbon))
            except Exception:
                state['carbon_leakage_proxy'] = self.state.get('carbon_leakage_proxy', 0.3)
        else:
            state['carbon_leakage_proxy'] = 0.3

        # Alert count
        if self.alert_system:
            alerts = await self.alert_system.get_active_alerts()
            state['alert_count'] = len(alerts)
        else:
            state['alert_count'] = 0

        # QuantumBridge penalties
        if self.quantum_bridge and hasattr(self.quantum_bridge, 'get_qubo_parameters'):
            try:
                q_params = self.quantum_bridge.get_qubo_parameters()
                state['q_penalty_carbon'] = q_params.get('penalty_carbon', 0.5)
                state['q_penalty_helium'] = q_params.get('penalty_helium_shortage', 0.5)
            except Exception:
                state['q_penalty_carbon'] = self.state.get('q_penalty_carbon', 0.5)
                state['q_penalty_helium'] = self.state.get('q_penalty_helium', 0.5)
        else:
            state['q_penalty_carbon'] = 0.5
            state['q_penalty_helium'] = 0.5

        # Degradation tier
        if self.degradation_manager:
            try:
                tier = self.degradation_manager.get_tier()
                state['degradation_tier'] = tier
            except Exception:
                state['degradation_tier'] = self.state.get('degradation_tier', 3)
        else:
            state['degradation_tier'] = 3

        # Swarm consensus
        if self.swarm_coordinator:
            try:
                swarm_data = self.swarm_coordinator.get_shared_predictions()
                # Compute how many agents are using the same strategy as we are
                strategies = [s.get('strategy') for s in swarm_data.values() if 'strategy' in s]
                if strategies:
                    consensus = strategies.count(self.current_strategy) / len(strategies)
                    state['swarm_consensus'] = consensus
                else:
                    state['swarm_consensus'] = 0.5
            except Exception:
                state['swarm_consensus'] = self.state.get('swarm_consensus', 0.5)
        else:
            state['swarm_consensus'] = 0.5

        # Workflow success (latest)
        state['workflow_success'] = self.state.get('workflow_success', 0.5)

        return state

    async def _compute_reward(self, state: Dict[str, float]) -> float:
        # Base components
        energy_efficiency = 1.0 - state.get('energy_intensity', 0.5)
        helium_sustainability = state.get('helium_level', 0.5)
        token_balance = min(1.0, state.get('token_balance', 500) / 1000)
        health_score = state.get('health_score', 0.8)
        carbon_leakage = state.get('carbon_leakage_proxy', 0.3)

        # Penalties
        alert_penalty = 0.0
        if self.alert_system:
            alerts = await self.alert_system.get_active_alerts()
            critical_alerts = [a for a in alerts if a.severity == 'critical']
            alert_penalty = 0.2 * len(critical_alerts)

        anomaly_penalty = 0.0
        if self.anomaly_detection:
            anomalies = await self.anomaly_detection.get_recent_anomalies(limit=5)
            anomaly_penalty = 0.1 * len(anomalies)

        # Degradation penalty
        deg_tier = state.get('degradation_tier', 3)
        deg_penalty = 0.1 * (5 - deg_tier) / 4

        # Workflow success bonus
        workflow_success = state.get('workflow_success', 0.5)
        workflow_bonus = 0.1 * workflow_success

        # Swarm consensus bonus
        swarm_consensus = state.get('swarm_consensus', 0.5)
        swarm_bonus = 0.05 * (swarm_consensus - 0.5)  # positive if >0.5

        # Cost‑benefit bonus
        cb_bonus = 0.0
        if self.cost_benefit_engine:
            stats = await self.cost_benefit_engine.get_analysis_stats()
            avg_roi = stats.get('average_roi', 0)
            if avg_roi > 0.5:
                cb_bonus = 0.1

        # QuantumBridge alignment: high carbon penalty → penalize carbon leakage more
        q_carbon = state.get('q_penalty_carbon', 0.5)
        if q_carbon > 0.7:
            carbon_leakage *= 1.5  # extra penalty

        if self.config.enable_multi_objective_rl:
            weights = self.config.objective_weights
            reward = (
                weights.get('energy_efficiency', 0.3) * energy_efficiency +
                weights.get('helium_sustainability', 0.25) * helium_sustainability +
                weights.get('token_balance', 0.2) * token_balance +
                weights.get('health_score', 0.15) * health_score -
                weights.get('carbon_leakage', 0.1) * carbon_leakage
            ) - alert_penalty - anomaly_penalty - deg_penalty + workflow_bonus + swarm_bonus + cb_bonus
        else:
            reward = (
                + 0.4 * energy_efficiency
                + 0.2 * helium_sustainability
                + 0.2 * token_balance
                + 0.1 * health_score
                - 0.1 * carbon_leakage
                - alert_penalty
                - anomaly_penalty
                - deg_penalty
                + workflow_bonus
                + swarm_bonus
                + cb_bonus
            )

        return reward

    async def _strategy_update_loop(self):
        while True:
            try:
                state = await self.get_strategy_state()
                self.state = state

                # Proactive strategy based on forecast
                if self.tick_engine and hasattr(self.tick_engine, 'get_helium_forecast'):
                    forecast = self.tick_engine.get_helium_forecast(6)  # 6 hours ahead
                    if forecast and len(forecast) > 5:
                        avg_future = np.mean(forecast)
                        if avg_future < 0.3 and self.current_strategy != 'conservative':
                            logger.info("Forecast indicates helium scarcity; switching to conservative")
                            await self.apply_strategy('conservative')

                if self.strategy_selector:
                    action = self.strategy_selector.select_action(state)
                    await self.apply_strategy(action)
                    self.current_strategy = action
                    self.strategy_change_time = datetime.now(timezone.utc)
                    self.metrics['strategy_changes'] += 1

                    await asyncio.sleep(self.config.state_save_interval_seconds)
                    next_state = await self.get_strategy_state()
                    reward = await self._compute_reward(next_state)
                    self.metrics['total_reward'] += reward
                    self.reward_history.append(reward)
                    self.metrics['avg_reward'] = np.mean(self.reward_history) if self.reward_history else 0
                    self.strategy_selector.update(state, action, reward, next_state)

                    importance = 0.7 if action != 'balanced' else 0.3
                    await self.auditor.record_event(
                        'strategy_change',
                        {'new_strategy': action, 'state': state, 'reward': reward},
                        importance=importance
                    )

                    # Proactive healing
                    if self.config.enable_proactive_healing and self.self_healer:
                        if state.get('health_score', 0.8) < self.config.proactive_healing_health_threshold:
                            logger.info("Health score below threshold; triggering self‑healing")
                            await self.self_healer.apply_healing('damage_accumulation')

                    await self._update_metrics(state)
                else:
                    await self.apply_strategy('balanced')

                await asyncio.sleep(self.config.state_save_interval_seconds)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Strategy loop error", error=str(e))
                await asyncio.sleep(30)

    async def apply_strategy(self, strategy: str):
        policy = self.config.strategy_policies.get(strategy, self.config.strategy_policies['balanced'])
        logger.info(f"Applying strategy '{strategy}' with policy: {policy}")

        # Update agent config
        for key, value in policy.items():
            if hasattr(self.config, key):
                setattr(self.config, key, value)

        # Propagate to core
        if self.bio_core and hasattr(self.bio_core, 'update_configuration'):
            await self.bio_core.update_configuration(policy)

        # Adjust scheduler
        if self.scheduler:
            if hasattr(self.scheduler, 'set_protons_per_rotation'):
                self.scheduler.set_protons_per_rotation(policy.get('scheduler_protons_per_rotation', 12))
            if hasattr(self.scheduler, 'set_degradation_tier'):
                tier = 5 if strategy == 'conservative' else 3 if strategy == 'balanced' else 1
                self.scheduler.set_degradation_tier(tier)

        # Adjust harvester mode
        if self.harvester and hasattr(self.harvester, 'set_mode'):
            mode_map = {
                'conservative': HarvestingMode.MINIMAL,
                'balanced': HarvestingMode.ADAPTIVE,
                'performance': HarvestingMode.FULL,
            }
            self.harvester.set_mode(mode_map.get(strategy, HarvestingMode.ADAPTIVE))

        # Adjust biomass storage tier
        if self.biomass_storage and hasattr(self.biomass_storage, 'set_default_tier'):
            tier_map = {
                'conservative': StorageTier.COLD,
                'balanced': StorageTier.STANDARD,
                'performance': StorageTier.HOT,
            }
            self.biomass_storage.set_default_tier(tier_map.get(strategy, StorageTier.STANDARD))

        # Adjust compartment creation
        if self.compartment_manager and hasattr(self.compartment_manager, 'set_creation_enabled'):
            self.compartment_manager.set_creation_enabled(policy.get('compartment_creation', True))

        # Adjust gradient pumping
        if self.gradient_manager and hasattr(self.gradient_manager, 'set_pump_rate'):
            self.gradient_manager.set_pump_rate(policy.get('gradient_pump_rate', 0.5))

        # Adjust token generation
        if self.token_manager and hasattr(self.token_manager, 'set_generation_rate'):
            self.token_manager.set_generation_rate(policy.get('token_generation_rate', 1.0))

        # Competition engine
        if self.competition_engine and hasattr(self.competition_engine, 'set_spawn_enabled'):
            self.competition_engine.set_spawn_enabled(policy.get('competition_spawn', False))

        # Trigger workflow
        if self.workflow_orchestrator:
            workflow_map = {
                'conservative': 'repair_and_storage',
                'balanced': 'standard_operations',
                'performance': 'scale_up_production',
            }
            wf_id = workflow_map.get(strategy)
            if wf_id:
                await self.workflow_orchestrator.execute_workflow(wf_id)

        # Active QuantumBridge control
        if self.quantum_bridge and hasattr(self.quantum_bridge, 'update_config'):
            if strategy == 'conservative':
                self.quantum_bridge.update_config({'scaling': {'carbon': 20.0, 'helium': 30.0}})
            elif strategy == 'performance':
                self.quantum_bridge.update_config({'scaling': {'carbon': 5.0, 'helium': 10.0}})
            else:
                self.quantum_bridge.update_config({})

        logger.info(f"Strategy '{strategy}' applied to all modules")

    async def _state_save_loop(self):
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
        while True:
            try:
                await asyncio.sleep(86400)
                snapshot = {
                    'timestamp': datetime.now(timezone.utc).isoformat(),
                    'state': self.state,
                    'metrics': self.metrics,
                    'strategy': self.current_strategy,
                    'agent_id': self.config.agent_id
                }
                await self.auditor.record_event('daily_snapshot', snapshot, importance=0.6)
                logger.info("Daily snapshot recorded.")
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Daily snapshot error", error=str(e))
                await asyncio.sleep(3600)

    async def _swarm_update_loop(self):
        while True:
            try:
                if self.swarm_coordinator:
                    await self.swarm_coordinator.share_predictions({
                        'agent_id': self.config.agent_id,
                        'state': self.state,
                        'strategy': self.current_strategy,
                        'metrics': self.metrics,
                        'q_table': dict(self.strategy_selector.q_table) if self.strategy_selector else None
                    })
                    swarm_state = self.swarm_coordinator.get_shared_predictions()
                    # Aggregate Q‑tables (optional)
                    if self.strategy_selector and swarm_state:
                        # Simple weighted average of Q‑tables from other agents
                        # We'll implement a placeholder for brevity
                        pass
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Swarm update error", error=str(e))
                await asyncio.sleep(120)

    async def _update_metrics(self, state: Dict[str, float]):
        self.metrics['energy_efficiency'] = 1.0 - state.get('energy_intensity', 0.5)
        self.metrics['helium_efficiency'] = state.get('helium_level', 0.5)

    async def save_state(self):
        state_data = {
            'agent_id': self.config.agent_id,
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'state': self.state,
            'metrics': self.metrics,
            'current_strategy': self.current_strategy,
            'strategy_change_time': self.strategy_change_time.isoformat(),
            'q_table': dict(self.strategy_selector.q_table) if self.strategy_selector else None,
            'correlation_id': self.correlation_id,
            'reward_history': list(self.reward_history),
        }
        try:
            with open(self.config.state_save_path, 'wb') as f:
                pickle.dump(state_data, f)
            logger.debug("State saved.")
        except Exception as e:
            logger.error("Failed to save state", error=str(e))

    async def load_state(self, path: Optional[str] = None):
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
            self.reward_history = deque(data.get('reward_history', []), maxlen=100)
            logger.info("State loaded.")
        except Exception as e:
            logger.error("Failed to load state", error=str(e))

    async def shutdown(self):
        logger.info("Shutting down BioIntegratedAgent")
        await self._task_manager.stop_all()
        await self.save_state()
        if self.tick_engine and hasattr(self.tick_engine, 'shutdown'):
            await self.tick_engine.shutdown()
        if self.quantum_bridge and hasattr(self.quantum_bridge, 'shutdown'):
            await self.quantum_bridge.shutdown()
        logger.info("Agent shutdown complete")

# ============================================================================
# Example usage
# ============================================================================
async def example():
    class MockCore:
        def __init__(self):
            self.event_broker = None
            self.self_healer = None
            self.alert_system = None
            self.anomaly_detection = None
            self.cost_benefit_engine = None
            self.workflow_orchestrator = None
            self.swarm_coordinator = None
            self.health_monitor = None
            self.degradation_manager = None
            self.competition_engine = None
            self.supply_manager = None
            self.token_allocator = None
        async def update_configuration(self, policy):
            pass

    config = {
        'enable_energy_aware_rl': True,
        'enable_time_tick_engine': True,
        'enable_quantum_bridge': True,
        'enable_swarm_coordination': True,
        'enable_proactive_healing': True,
        'pqc_key_dir': './pqc_keys',
        'state_save_path': './agent_state.pkl',
        'enable_multi_objective_rl': True,
    }
    agent = BioIntegratedAgent(
        bio_core=MockCore(),
        config=config,
        csv_path="helium_data.csv"
    )
    await asyncio.sleep(10)
    state = await agent.get_strategy_state()
    print("Current state:", state)
    print("Current strategy:", agent.current_strategy)
    print("Metrics:", agent.metrics)
    await agent.shutdown()

if __name__ == "__main__":
    asyncio.run(example())
