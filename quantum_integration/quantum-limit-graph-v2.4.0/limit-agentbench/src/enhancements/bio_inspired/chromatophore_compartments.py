# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/bio_inspired/chromatophore_compartments.py

"""
Chromatophore Compartment System for Green Agent
Version: 1.0.0

Full modular isolation for MoE experts inspired by bacterial chromatophores.
Each compartment is a self-contained execution environment with:
- Selective membrane (API gateway)
- Local Eco-ATP pool
- Internal gradient fields
- Independent lifecycle management
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import numpy as np
from collections import deque
import hashlib
import uuid

logger = logging.getLogger(__name__)

class CompartmentState(Enum):
    """Compartment lifecycle states"""
    GENESIS = "genesis"          # Being created
    MATURING = "maturing"        # Building trust
    ACTIVE = "active"            # Fully operational
    STRESSED = "stressed"        # Resource constrained
    SENESCENT = "senescent"      # Declining
    APOPTOTIC = "apoptotic"      # Programmed death
    DECOMMISSIONED = "decommissioned"

class MembranePermeability(Enum):
    """Membrane permeability levels"""
    IMPERMEABLE = 0.0     # Complete isolation
    RESTRICTIVE = 0.3     # Minimal communication
    SELECTIVE = 0.6       # Balanced
    PERMEABLE = 0.8       # Open collaboration
    POROUS = 1.0          # Full transparency

@dataclass
class CompartmentResource:
    """Resource allocation for a compartment"""
    cpu_cores: float = 1.0
    memory_mb: float = 256.0
    storage_mb: float = 1024.0
    network_mbps: float = 100.0
    max_tokens: float = 1000.0
    
    @property
    def utilization(self) -> float:
        return (self.cpu_cores + self.memory_mb/256 + self.storage_mb/1024) / 3

@dataclass
class MembraneGate:
    """Controls what crosses the compartment membrane"""
    compartment_id: str
    permeability: MembranePermeability = MembranePermeability.SELECTIVE
    inbound_rate_limit: float = 100.0  # requests/second
    outbound_rate_limit: float = 200.0
    trusted_peers: List[str] = field(default_factory=list)
    blocked_peers: List[str] = field(default_factory=list)
    
    # Traffic tracking
    inbound_count: int = 0
    outbound_count: int = 0
    rejected_count: int = 0
    
    def can_pass(self, source_id: str, direction: str = 'inbound') -> bool:
        """Check if communication can cross membrane"""
        if source_id in self.blocked_peers:
            self.rejected_count += 1
            return False
        
        if self.permeability == MembranePermeability.IMPERMEABLE:
            self.rejected_count += 1
            return False
        
        if self.permeability == MembranePermeability.RESTRICTIVE:
            if source_id not in self.trusted_peers:
                self.rejected_count += 1
                return False
        
        if direction == 'inbound':
            self.inbound_count += 1
        else:
            self.outbound_count += 1
        
        return True
    
    def adjust_permeability(self, trust_score: float, token_balance: float):
        """Dynamically adjust membrane permeability"""
        if trust_score > 0.8 and token_balance > 500:
            self.permeability = MembranePermeability.PERMEABLE
        elif trust_score > 0.5 and token_balance > 200:
            self.permeability = MembranePermeability.SELECTIVE
        elif trust_score > 0.2:
            self.permeability = MembranePermeability.RESTRICTIVE
        else:
            self.permeability = MembranePermeability.IMPERMEABLE

class ChromatophoreCompartment:
    """
    Self-contained expert execution compartment.
    
    Inspired by bacterial chromatophore vesicles.
    """
    
    def __init__(
        self,
        compartment_id: str,
        expert_type: str,
        expert_instance: Any = None,
        resources: Optional[CompartmentResource] = None
    ):
        self.compartment_id = compartment_id
        self.expert_type = expert_type
        self.expert = expert_instance
        self.resources = resources or CompartmentResource()
        
        # Lifecycle
        self.state = CompartmentState.GENESIS
        self.birth_time = datetime.utcnow()
        self.generation = 1
        self.parent_id: Optional[str] = None
        
        # Membrane
        self.membrane = MembraneGate(compartment_id)
        
        # Local Eco-ATP pool
        self.token_balance: float = 100.0  # Initial endowment
        self.total_earned: float = 0.0
        self.total_spent: float = 0.0
        
        # Local gradient fields (simplified)
        self.trust_gradient: float = 0.1  # Starts low, builds with success
        self.efficiency_gradient: float = 0.5
        
        # Performance tracking
        self.tasks_completed: int = 0
        self.tasks_failed: int = 0
        self.total_latency_ms: float = 0.0
        self.carbon_emitted_kg: float = 0.0
        
        # Biomass storage (local)
        self.atp_cache: deque = deque(maxlen=100)
        self.glycogen_queue: deque = deque(maxlen=1000)
        self.starch_reserve: deque = deque(maxlen=5000)
        self.lipid_depot: deque = deque(maxlen=10000)
        
        # Communication history
        self.signal_history: deque = deque(maxlen=500)
        
        logger.info(f"Compartment {compartment_id} created: {expert_type}")
    
    @property
    def success_rate(self) -> float:
        total = self.tasks_completed + self.tasks_failed
        return self.tasks_completed / max(total, 1)
    
    @property
    def efficiency_score(self) -> float:
        if self.tasks_completed == 0:
            return 0.5
        return self.token_balance / max(self.total_earned, 1)
    
    @property
    def health_score(self) -> float:
        """Composite health score"""
        return (
            self.success_rate * 0.4 +
            self.efficiency_score * 0.3 +
            self.trust_gradient * 0.3
        )
    
    @property
    def is_viable(self) -> bool:
        """Check if compartment is viable"""
        return (
            self.state in [CompartmentState.MATURING, CompartmentState.ACTIVE] and
            self.health_score > 0.2 and
            self.token_balance > 0
        )
    
    def receive_tokens(self, amount: float, source: str = "scheduler") -> bool:
        """Receive Eco-ATP tokens through membrane"""
        if not self.membrane.can_pass(source, 'inbound'):
            return False
        
        self.token_balance += amount
        self.total_earned += amount
        return True
    
    def spend_tokens(self, amount: float, purpose: str = "execution") -> bool:
        """Spend Eco-ATP tokens for task execution"""
        if self.token_balance < amount:
            return False
        
        self.token_balance -= amount
        self.total_spent += amount
        return True
    
    def record_task_result(
        self,
        success: bool,
        latency_ms: float,
        carbon_kg: float,
        tokens_consumed: float
    ):
        """Record task execution result"""
        if success:
            self.tasks_completed += 1
            self.trust_gradient = min(1.0, self.trust_gradient + 0.05)
            self.efficiency_gradient = min(1.0, 
                self.efficiency_gradient + 0.02 * (1 - tokens_consumed / max(self.token_balance, 1))
            )
        else:
            self.tasks_failed += 1
            self.trust_gradient = max(0.0, self.trust_gradient - 0.1)
            self.efficiency_gradient = max(0.1, self.efficiency_gradient - 0.05)
        
        self.total_latency_ms += latency_ms
        self.carbon_emitted_kg += carbon_kg
        
        # Adjust membrane permeability
        self.membrane.adjust_permeability(self.trust_gradient, self.token_balance)
        
        # Check for state transitions
        self._evaluate_lifecycle()
    
    def _evaluate_lifecycle(self):
        """Evaluate and transition lifecycle state"""
        if self.health_score < 0.1 and self.state == CompartmentState.ACTIVE:
            self.state = CompartmentState.SENESCENT
            logger.warning(f"Compartment {self.compartment_id} entering senescence")
        
        elif self.health_score < 0.05:
            self.state = CompartmentState.APOPTOTIC
            logger.warning(f"Compartment {self.compartment_id} marked for apoptosis")
        
        elif self.health_score > 0.3 and self.state == CompartmentState.MATURING:
            self.state = CompartmentState.ACTIVE
            logger.info(f"Compartment {self.compartment_id} now active")
    
    def spawn_child(self, expert_type: Optional[str] = None) -> 'ChromatophoreCompartment':
        """Spawn a child compartment (reproduction)"""
        child_id = f"{self.compartment_id}_child_{self.generation}"
        child_type = expert_type or self.expert_type
        
        # Transfer some tokens to child (endowment)
        endowment = self.token_balance * 0.2
        self.token_balance -= endowment
        
        child = ChromatophoreCompartment(
            compartment_id=child_id,
            expert_type=child_type,
            resources=CompartmentResource(
                cpu_cores=self.resources.cpu_cores * 0.5,
                memory_mb=self.resources.memory_mb * 0.5
            )
        )
        
        child.parent_id = self.compartment_id
        child.generation = self.generation + 1
        child.token_balance = endowment
        child.trust_gradient = self.trust_gradient * 0.5  # Inherit partial trust
        
        self.generation += 1
        
        logger.info(f"Compartment {self.compartment_id} spawned child {child_id}")
        
        return child
    
    def prepare_apoptosis(self) -> Tuple[float, Dict[str, Any]]:
        """Prepare for programmed cell death"""
        # Distill knowledge before death
        knowledge_summary = {
            'expert_type': self.expert_type,
            'tasks_completed': self.tasks_completed,
            'success_rate': self.success_rate,
            'efficiency_score': self.efficiency_score,
            'learned_patterns': list(self.atp_cache)[-10:],
            'best_practices': {
                'avg_latency_ms': self.total_latency_ms / max(self.tasks_completed, 1),
                'carbon_per_task_kg': self.carbon_emitted_kg / max(self.tasks_completed, 1)
            }
        }
        
        # Return remaining tokens to pool
        remaining_tokens = self.token_balance
        
        self.state = CompartmentState.DECOMMISSIONED
        
        return remaining_tokens, knowledge_summary
    
    def get_status(self) -> Dict[str, Any]:
        """Get compartment status"""
        return {
            'compartment_id': self.compartment_id,
            'expert_type': self.expert_type,
            'state': self.state.value,
            'generation': self.generation,
            'health_score': self.health_score,
            'token_balance': self.token_balance,
            'trust_gradient': self.trust_gradient,
            'efficiency_gradient': self.efficiency_gradient,
            'success_rate': self.success_rate,
            'membrane_permeability': self.membrane.permeability.value,
            'tasks_completed': self.tasks_completed,
            'storage': {
                'atp_cache': len(self.atp_cache),
                'glycogen_queue': len(self.glycogen_queue),
                'starch_reserve': len(self.starch_reserve),
                'lipid_depot': len(self.lipid_depot)
            }
        }


class CompartmentManager:
    """
    Manages the ecosystem of chromatophore compartments.
    
    Handles compartment lifecycle, resource allocation, and inter-compartment coordination.
    """
    
    def __init__(self, token_manager=None):
        self.token_manager = token_manager
        self.compartments: Dict[str, ChromatophoreCompartment] = {}
        self.global_resource_pool = CompartmentResource(
            cpu_cores=16.0, memory_mb=4096.0, storage_mb=10240.0
        )
        
        # Ecosystem metrics
        self.total_compartments_created = 0
        self.total_apoptosis_events = 0
        self.knowledge_bank: Dict[str, List[Dict]] = defaultdict(list)
        
        # Market for inter-compartment trading
        self.market_orders: List[Dict] = []
        
        # Start maintenance tasks
        asyncio.create_task(self._ecosystem_maintenance())
        
        logger.info("Compartment Manager initialized")
    
    def create_compartment(
        self,
        expert_type: str,
        expert_instance: Any = None,
        resources: Optional[CompartmentResource] = None,
        parent_id: Optional[str] = None
    ) -> ChromatophoreCompartment:
        """Create a new chromatophore compartment"""
        compartment_id = f"comp_{expert_type}_{uuid.uuid4().hex[:8]}"
        
        # Allocate resources from global pool
        if resources is None:
            resources = CompartmentResource(
                cpu_cores=min(2.0, self.global_resource_pool.cpu_cores * 0.1),
                memory_mb=min(256.0, self.global_resource_pool.memory_mb * 0.1),
                storage_mb=min(512.0, self.global_resource_pool.storage_mb * 0.05)
            )
        
        # Deduct from global pool
        self.global_resource_pool.cpu_cores -= resources.cpu_cores
        self.global_resource_pool.memory_mb -= resources.memory_mb
        self.global_resource_pool.storage_mb -= resources.storage_mb
        
        compartment = ChromatophoreCompartment(
            compartment_id=compartment_id,
            expert_type=expert_type,
            expert_instance=expert_instance,
            resources=resources
        )
        
        if parent_id:
            compartment.parent_id = parent_id
        
        # Initial token endowment
        if self.token_manager:
            self.token_manager.create_account(compartment_id)
            tokens = self.token_manager.generate_tokens(
                account_id=compartment_id,
                source=EcoATPSource.EFFICIENCY_GAIN,
                energy_saved_kwh=0.001,
                num_tokens=10
            )
            if tokens:
                compartment.receive_tokens(sum(t.value for t in tokens))
        
        self.compartments[compartment_id] = compartment
        self.total_compartments_created += 1
        
        compartment.state = CompartmentState.MATURING
        
        return compartment
    
    def decommission_compartment(self, compartment_id: str) -> Dict[str, Any]:
        """Decommission a compartment (apoptosis)"""
        if compartment_id not in self.compartments:
            return {}
        
        compartment = self.compartments[compartment_id]
        
        # Prepare apoptosis
        remaining_tokens, knowledge = compartment.prepare_apoptosis()
        
        # Store knowledge
        self.knowledge_bank[compartment.expert_type].append(knowledge)
        
        # Return resources to global pool
        self.global_resource_pool.cpu_cores += compartment.resources.cpu_cores
        self.global_resource_pool.memory_mb += compartment.resources.memory_mb
        self.global_resource_pool.storage_mb += compartment.resources.storage_mb
        
        # Return tokens if token manager available
        if self.token_manager and remaining_tokens > 0:
            main_account = "green_agent_core"
            self.token_manager.generate_tokens(
                account_id=main_account,
                source=EcoATPSource.EFFICIENCY_GAIN,
                energy_saved_kwh=remaining_tokens / 10000.0,
                num_tokens=int(remaining_tokens / 10)
            )
        
        del self.compartments[compartment_id]
        self.total_apoptosis_events += 1
        
        logger.info(f"Compartment {compartment_id} decommissioned")
        
        return knowledge
    
    def find_best_compartment(
        self,
        expert_type: str,
        task_complexity: float = 1.0
    ) -> Optional[ChromatophoreCompartment]:
        """Find the best compartment for a task"""
        candidates = [
            c for c in self.compartments.values()
            if c.expert_type == expert_type and c.is_viable
        ]
        
        if not candidates:
            return None
        
        # Score by health, efficiency, and token balance
        scored = []
        for c in candidates:
            score = (
                c.health_score * 0.4 +
                c.efficiency_score * 0.3 +
                min(c.token_balance / (task_complexity * 10), 1.0) * 0.3
            )
            scored.append((c, score))
        
        scored.sort(key=lambda x: x[1], reverse=True)
        return scored[0][0] if scored else None
    
    def balance_load(self):
        """Balance load across compartments"""
        overloaded = [
            c for c in self.compartments.values()
            if c.is_viable and len(c.glycogen_queue) > 500
        ]
        
        underloaded = [
            c for c in self.compartments.values()
            if c.is_viable and len(c.glycogen_queue) < 100 and c.expert_type in [oc.expert_type for oc in overloaded]
        ]
        
        for ol in overloaded:
            for ul in underloaded:
                if ol.expert_type == ul.expert_type:
                    # Transfer some tasks
                    transfer_count = min(50, len(ol.glycogen_queue) - 500)
                    for _ in range(transfer_count):
                        if ol.glycogen_queue:
                            task = ol.glycogen_queue.popleft()
                            ul.glycogen_queue.append(task)
    
    def spawn_if_needed(self):
        """Spawn new compartments if demand exceeds supply"""
        expert_types = set(c.expert_type for c in self.compartments.values())
        
        for etype in expert_types:
            active = [
                c for c in self.compartments.values()
                if c.expert_type == etype and c.is_viable
            ]
            
            if len(active) < 2:  # Minimum 2 compartments per type
                self.create_compartment(etype)
            
            # Check load
            avg_queue = np.mean([len(c.glycogen_queue) for c in active]) if active else 0
            if avg_queue > 300 and self.global_resource_pool.cpu_cores > 2:
                self.create_compartment(etype)
    
    def cull_unhealthy(self):
        """Remove unhealthy compartments"""
        for cid in list(self.compartments.keys()):
            compartment = self.compartments[cid]
            
            if compartment.state == CompartmentState.APOPTOTIC:
                self.decommission_compartment(cid)
            elif compartment.state == CompartmentState.SENESCENT:
                # Give time to recover
                if compartment.health_score < 0.05:
                    compartment.state = CompartmentState.APOPTOTIC
    
    async def _ecosystem_maintenance(self):
        """Periodic ecosystem maintenance"""
        while True:
            try:
                self.balance_load()
                self.spawn_if_needed()
                self.cull_unhealthy()
                await asyncio.sleep(30)
            except Exception as e:
                logger.error(f"Ecosystem maintenance error: {str(e)}")
                await asyncio.sleep(60)
    
    def get_ecosystem_stats(self) -> Dict[str, Any]:
        """Get ecosystem statistics"""
        compartments_by_type = defaultdict(list)
        for c in self.compartments.values():
            compartments_by_type[c.expert_type].append(c)
        
        return {
            'total_compartments': len(self.compartments),
            'total_created': self.total_compartments_created,
            'total_apoptosis': self.total_apoptosis_events,
            'global_resources': {
                'cpu_cores': self.global_resource_pool.cpu_cores,
                'memory_mb': self.global_resource_pool.memory_mb,
                'storage_mb': self.global_resource_pool.storage_mb
            },
            'by_type': {
                etype: {
                    'count': len(comps),
                    'avg_health': np.mean([c.health_score for c in comps]),
                    'total_tokens': sum(c.token_balance for c in comps),
                    'total_tasks': sum(c.tasks_completed for c in comps)
                }
                for etype, comps in compartments_by_type.items()
            },
            'knowledge_bank_size': sum(len(v) for v in self.knowledge_bank.values())
        }
