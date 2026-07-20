# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/bio_inspired/bio_integrated_agent.py
# Enhanced version v8.0.0 – Energy‑aware RL, persistent PQC keys, blockchain audit policy

"""
Bio‑Integrated Green Agent v8.0.0
Full integration of bio‑inspired modules with:
- Energy‑aware RL strategy selector (Q‑learning)
- Persistent PQC keys for verifiable identity
- Selective blockchain auditing based on sustainability impact
- Helium/carbon coupling via TimeTickEngine
- Configurable energy policies
"""

import asyncio
import logging
import json
import os
import hashlib
import uuid
from typing import Dict, Any, List, Optional, Tuple, Callable
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from collections import defaultdict
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

# Local imports (with fallback)
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
    TICK_ENGINE_AVAILABLE = False

try:
    from .quantum_bridge import QuantumBridge
    QUANTUM_BRIDGE_AVAILABLE = True
except ImportError:
    QUANTUM_BRIDGE_AVAILABLE = False

# PQC (post‑quantum cryptography) – use a simple fallback if not available
try:
    from pqcrypto.sign import falcon
    PQC_AVAILABLE = True
except ImportError:
    PQC_AVAILABLE = False

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
            # Fallback HMAC
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
        # Check if event type is allowed
        if event_type not in self.config.blockchain_audit_events:
            logger.debug(f"Event {event_type} not in audit list; skipping")
            return False

        # Check importance threshold
        if importance < self.config.blockchain_audit_min_importance:
            logger.debug(f"Event importance {importance} below threshold; skipping")
            return False

        # Sign the payload
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
        """Verify the signature of a ledger entry."""
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

        # Bin each dimension
        load_bin = 'high' if load > 0.7 else 'medium' if load > 0.4 else 'low'
        health_bin = 'good' if health > 0.7 else 'medium' if health > 0.4 else 'poor'
        token_bin = 'abundant' if token > 1000 else 'adequate' if token > 100 else 'scarce'
        energy_bin = 'heavy' if energy > 0.7 else 'normal' if energy > 0.4 else 'light'
        helium_bin = 'scarce' if helium < 0.3 else 'normal' if helium < 0.7 else 'abundant'
        # Carbon leakage: low is good
        carbon_bin = 'high' if carbon > 0.6 else 'medium' if carbon > 0.3 else 'low'

        return f"{load_bin}_{health_bin}_{token_bin}_{energy_bin}_{helium_bin}_{carbon_bin}"

    def select_action(self, state: Dict[str, float]) -> str:
        """Select an action (strategy) using epsilon‑greedy."""
        key = self._state_to_key(state)
        if key not in self.q_table:
            self.q_table[key] = {s: 0.0 for s in self.actions}

        if np.random.random() < self.epsilon:
            action = np.random.choice(self.actions)
        else:
            # Choose action with max Q‑value
            q_vals = self.q_table[key]
            max_q = max(q_vals.values())
            best_actions = [a for a, q in q_vals.items() if q == max_q]
            action = np.random.choice(best_actions)

        self.last_state_key = key
        self.last_action = action
        return action

    def update(self, state: Dict[str, float], action: str, reward: float, next_state: Dict[str, float]):
        """Update Q‑table using Q‑learning."""
        if self.last_state_key is None or self.last_action is None:
            return
        key = self._state_to_key(state)
        next_key = self._state_to_key(next_state)

        # Ensure tables exist
        if key not in self.q_table:
            self.q_table[key] = {s: 0.0 for s in self.actions}
        if next_key not in self.q_table:
            self.q_table[next_key] = {s: 0.0 for s in self.actions}

        # Q‑learning update
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
# Core Bio‑Integrated Agent
# ============================================================================

class BioIntegratedAgent:
    """
    Main agent that orchestrates all bio‑inspired modules, with energy‑aware RL,
    persistent PQC keys, and selective blockchain auditing.
    """

    def __init__(
        self,
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

        # Background tasks
        self._task_manager = TaskManager()
        self._task_manager.start_task("strategy_loop", self._strategy_update_loop)
        self._task_manager.start_task("state_save", self._state_save_loop)
        self._task_manager.start_task("daily_snapshot", self._daily_snapshot_loop)

        logger.info(f"BioIntegratedAgent initialized with ID {self.config.agent_id}")

    def _get_initial_state(self) -> Dict[str, float]:
        """Initial state placeholder."""
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
        """
        state = {}
        # Token balance
        if self.token_manager:
            summary = self.token_manager.get_system_summary()
            state['token_balance'] = summary.get('total_balance', 500)
        else:
            state['token_balance'] = 500

        # System load (approximate from scheduler or compartments)
        if self.scheduler:
            stats = self.scheduler.get_scheduler_stats()
            state['system_load'] = stats.get('demand_level', 0.5)
        elif self.compartment_manager:
            stats = self.compartment_manager.get_ecosystem_stats()
            state['system_load'] = 1.0 - stats.get('viable_compartments', 0) / max(stats.get('total_compartments', 1), 1)
        else:
            state['system_load'] = 0.5

        # Health score
        if self.harvester:
            stats = self.harvester.get_harvesting_stats()
            health = stats.get('pigment_health', {})
            avg_health = np.mean([h.get('efficiency', 0.5) for h in health.values()]) if health else 0.8
            state['health_score'] = avg_health
        else:
            state['health_score'] = 0.8

        # Energy intensity: average tokens consumed per task or gradient utilization
        if self.token_manager:
            summary = self.token_manager.get_system_summary()
            total_generated = summary.get('total_generated', 0)
            total_consumed = summary.get('total_consumed', 0)
            if total_generated > 0:
                state['energy_intensity'] = total_consumed / total_generated
            else:
                state['energy_intensity'] = 0.5
        else:
            state['energy_intensity'] = 0.5

        # Helium level: from tick engine or gradient manager
        helium_level = 0.5
        if self.tick_engine:
            # Assume tick_engine has a method to get current helium level
            if hasattr(self.tick_engine, 'get_current_helium'):
                helium_level = self.tick_engine.get_current_helium()
            elif hasattr(self.tick_engine, 'current_data'):
                # Fallback: use last row
                helium_level = self.tick_engine.current_data.get('helium_supply', 0.5)
        elif self.gradient_manager:
            strengths = self.gradient_manager.get_field_strengths()
            helium_level = strengths.get('helium', 0.5)
        state['helium_level'] = max(0.0, min(1.0, helium_level))

        # Carbon leakage proxy: from gradient carbon or anomaly rate
        if self.gradient_manager:
            strengths = self.gradient_manager.get_field_strengths()
            carbon = strengths.get('carbon', 0.5)
            state['carbon_leakage_proxy'] = max(0.0, min(1.0, carbon))
        else:
            state['carbon_leakage_proxy'] = 0.3

        return state

    async def _compute_reward(self, state: Dict[str, float]) -> float:
        """
        Compute reward based on sustainability metrics.
        Positive: high sustainability_score, high token_efficiency.
        Negative: high energy_intensity, high helium leakage, high carbon leakage.
        """
        sustainability_score = 0.0
        if self.token_manager:
            summary = self.token_manager.get_system_summary()
            balance = summary.get('total_balance', 0)
            if balance > 1000:
                sustainability_score += 0.2
            elif balance > 500:
                sustainability_score += 0.1

        token_efficiency = 1.0 - state.get('energy_intensity', 0.5)

        energy_intensity_penalty = state.get('energy_intensity', 0.5)
        helium_leakage_penalty = 1.0 - state.get('helium_level', 0.5)
        carbon_leakage_penalty = state.get('carbon_leakage_proxy', 0.3)

        reward = (
            + 0.4 * sustainability_score
            + 0.2 * token_efficiency
            - 0.2 * energy_intensity_penalty
            - 0.2 * helium_leakage_penalty
            - 0.2 * carbon_leakage_penalty
        )
        return reward

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
                    # Apply strategy policies
                    self.apply_strategy(action)
                    self.current_strategy = action
                    self.strategy_change_time = datetime.now(timezone.utc)
                    self.metrics['strategy_changes'] += 1

                    # After a delay, compute reward and update Q‑table
                    # We'll compute reward based on the next state after applying strategy
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
                    self.apply_strategy('balanced')

                # Update metrics
                await self._update_metrics(state)

                # Sleep until next cycle (configurable)
                await asyncio.sleep(self.config.state_save_interval_seconds)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Strategy loop error", error=str(e))
                await asyncio.sleep(30)

    def apply_strategy(self, strategy: str):
        """
        Apply the policies associated with the strategy to the system.
        Overrides relevant config parameters and propagates to modules.
        """
        policy = self.config.strategy_policies.get(strategy, self.config.strategy_policies['balanced'])
        logger.info(f"Applying strategy '{strategy}' with policy: {policy}")

        # Update agent config
        for key, value in policy.items():
            if hasattr(self.config, key):
                setattr(self.config, key, value)

        # Propagate to modules
        # Example: adjust state_save_interval, health_check_interval, etc.
        # This could also adjust scheduler parameters, competition interval, etc.
        if self.scheduler:
            # Adjust competition interval (if scheduler has method)
            if hasattr(self.scheduler, 'set_competition_interval'):
                self.scheduler.set_competition_interval(policy.get('competition_interval', 3600))
        if self.harvester:
            # Adjust harvest rate or mode based on throughput
            pass
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
        """Update internal metrics."""
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
        }
        try:
            with open(self.config.state_save_path, 'wb') as f:
                pickle.dump(state_data, f)
            logger.debug("State saved.")
        except Exception as e:
            logger.error("Failed to save state", error=str(e))

    async def load_state(self, path: Optional[str] = None):
        """Load agent state from disk."""
        path = path or self.config.state_save_path
        if not os.path.exists(path):
            return
        try:
            with open(path, 'rb') as f:
                data = pickle.load(f)
            self.state = data.get('state', self.state)
            self.metrics = data.get('metrics', self.metrics)
            self.current_strategy = data.get('current_strategy', 'balanced')
            if data.get('q_table') and self.strategy_selector:
                self.strategy_selector.q_table = defaultdict(dict, data['q_table'])
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
# Task Manager (copied from previous modules)
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
# Example usage
# ============================================================================

async def example():
    """Example usage of the BioIntegratedAgent."""
    config = {
        'enable_energy_aware_rl': True,
        'enable_time_tick_engine': True,
        'enable_quantum_bridge': True,
        'pqc_key_dir': './pqc_keys',
        'state_save_path': './agent_state.pkl',
    }
    agent = BioIntegratedAgent(config=config, csv_path="helium_data.csv")
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
