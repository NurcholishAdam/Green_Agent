# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/moe_expert_system/expert_router.py
# Complete enhanced file with all bio-inspired integration and gap fixes

"""
Enhanced Expert Router v5.0.0 - Complete Signal Transduction Cascade
With What-If Analysis, Causal Inference, and Natural Language Explanations
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple, Set, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import numpy as np
from collections import defaultdict, deque
import time
import hashlib
import json
import math
import uuid

logger = logging.getLogger(__name__)

# ============================================================================
# Enums and Data Classes
# ============================================================================

class SignalType(Enum):
    ENDOCRINE = "endocrine"
    PARACRINE = "paracrine"
    AUTOCRINE = "autocrine"
    JUXTACRINE = "juxtacrine"
    NEUROTRANSMITTER = "neurotransmitter"
    NEUROMODULATOR = "neuromodulator"

class SecondMessenger(Enum):
    cAMP = "camp"
    cGMP = "cgmp"
    IP3 = "ip3"
    DAG = "dag"
    CALCIUM = "calcium"
    NITRIC_OXIDE = "nitric_oxide"

class ReceptorState(Enum):
    INACTIVE = "inactive"
    BOUND = "bound"
    ACTIVATED = "activated"
    DESENSITIZED = "desensitized"
    INTERNALIZED = "internalized"
    RESENSITIZED = "resensitized"

class AmplificationLevel(Enum):
    NONE = 0
    LOW = 1
    MODERATE = 2
    HIGH = 3
    MAXIMUM = 4

class CircuitBreakerState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

@dataclass
class SignalReceptor:
    receptor_id: str
    signal_type: SignalType
    ligand: str
    affinity: float = 0.5
    state: ReceptorState = ReceptorState.INACTIVE
    bound_ligands: int = 0
    desensitization_time: float = 0.0
    resensitization_rate: float = 0.1
    amplification: AmplificationLevel = AmplificationLevel.MODERATE
    downstream_effectors: List[str] = field(default_factory=list)
    last_activated: Optional[datetime] = None
    activation_count: int = 0

@dataclass
class SecondMessengerSystem:
    messenger_type: SecondMessenger
    concentration: float = 0.0
    baseline: float = 0.1
    threshold: float = 0.3
    max_concentration: float = 1.0
    synthesis_rate: float = 0.1
    degradation_rate: float = 0.05
    amplification_factor: float = 100.0
    target_proteins: List[str] = field(default_factory=list)
    half_life_seconds: float = 5.0

@dataclass
class AllostericSite:
    site_id: str
    modulator: str
    effect: str = "modulation"
    binding_affinity: float = 0.5
    current_occupancy: float = 0.0
    conformational_change: float = 0.0

@dataclass
class MetabolicPathway:
    pathway_id: str
    input_substrate: str
    enzymes: List[str]
    intermediates: List[str]
    final_product: str
    rate_limiting_step: Optional[str] = None
    allosteric_regulators: List[AllostericSite] = field(default_factory=list)
    energy_cost_ecoatp: float = 10.0
    throughput_rate: float = 1.0
    is_active: bool = True

@dataclass
class RoutingMetrics:
    total_routes: int = 0
    successful_routes: int = 0
    failed_routes: int = 0
    fallback_routes: int = 0
    biomass_stored_routes: int = 0
    average_latency_ms: float = 0.0
    
    @property
    def success_rate(self) -> float:
        return self.successful_routes / max(self.total_routes, 1)

@dataclass
class ExpertCircuitBreaker:
    expert_id: str
    state: CircuitBreakerState = CircuitBreakerState.CLOSED
    failure_count: int = 0
    success_count: int = 0
    last_failure_time: Optional[datetime] = None
    failure_threshold: int = 5
    recovery_timeout_seconds: int = 30
    half_open_max_requests: int = 3
    half_open_requests: int = 0
    
    def record_success(self):
        self.success_count += 1
        if self.state == CircuitBreakerState.HALF_OPEN:
            self.half_open_requests += 1
            if self.half_open_requests >= self.half_open_max_requests:
                self.state = CircuitBreakerState.CLOSED
                self.failure_count = 0
    
    def record_failure(self):
        self.failure_count += 1
        self.last_failure_time = datetime.utcnow()
        if self.state == CircuitBreakerState.CLOSED and self.failure_count >= self.failure_threshold:
            self.state = CircuitBreakerState.OPEN
        elif self.state == CircuitBreakerState.HALF_OPEN:
            self.state = CircuitBreakerState.OPEN
    
    def can_execute(self) -> bool:
        if self.state == CircuitBreakerState.CLOSED:
            return True
        if self.state == CircuitBreakerState.OPEN:
            if self.last_failure_time:
                elapsed = (datetime.utcnow() - self.last_failure_time).total_seconds()
                if elapsed >= self.recovery_timeout_seconds:
                    self.state = CircuitBreakerState.HALF_OPEN
                    self.half_open_requests = 0
                    return True
            return False
        return True

# ============================================================================
# Signal Transduction Engine
# ============================================================================

class SignalTransductionEngine:
    """Biological signal transduction engine for expert routing"""
    
    def __init__(self):
        self.receptors: Dict[str, SignalReceptor] = {}
        self.second_messengers: Dict[SecondMessenger, SecondMessengerSystem] = {}
        self.amplification_history: deque = deque(maxlen=1000)
        self.crosstalk_matrix: Dict[Tuple[str, str], float] = {}
        self._initialize_signaling_systems()
        asyncio.create_task(self._signal_degradation_loop())
        logger.info("Signal Transduction Engine initialized")
    
    def _initialize_signaling_systems(self):
        self.second_messengers[SecondMessenger.cAMP] = SecondMessengerSystem(
            messenger_type=SecondMessenger.cAMP, baseline=0.1, threshold=0.3,
            synthesis_rate=0.15, degradation_rate=0.08, amplification_factor=100.0,
            half_life_seconds=3.0, target_proteins=['energy_expert', 'routing_kinase']
        )
        self.second_messengers[SecondMessenger.CALCIUM] = SecondMessengerSystem(
            messenger_type=SecondMessenger.CALCIUM, baseline=0.05, threshold=0.2,
            synthesis_rate=0.2, degradation_rate=0.1, amplification_factor=1000.0,
            half_life_seconds=1.0, target_proteins=['all_experts', 'emergency_response']
        )
        self.second_messengers[SecondMessenger.IP3] = SecondMessengerSystem(
            messenger_type=SecondMessenger.IP3, baseline=0.05, threshold=0.25,
            synthesis_rate=0.1, degradation_rate=0.06, amplification_factor=500.0,
            half_life_seconds=4.0, target_proteins=['gradient_effectors', 'compartment_activation']
        )
        self.second_messengers[SecondMessenger.NITRIC_OXIDE] = SecondMessengerSystem(
            messenger_type=SecondMessenger.NITRIC_OXIDE, baseline=0.02, threshold=0.15,
            synthesis_rate=0.12, degradation_rate=0.15, amplification_factor=200.0,
            half_life_seconds=2.0, target_proteins=['neighboring_compartments', 'vascular_signaling']
        )
    
    def create_receptor(self, receptor_id: str, signal_type: SignalType,
                        ligand: str, affinity: float = 0.5,
                        amplification: AmplificationLevel = AmplificationLevel.MODERATE) -> SignalReceptor:
        receptor = SignalReceptor(receptor_id=receptor_id, signal_type=signal_type,
                                  ligand=ligand, affinity=affinity, amplification=amplification)
        self.receptors[receptor_id] = receptor
        return receptor
    
    def bind_ligand(self, receptor_id: str, ligand_concentration: float) -> bool:
        if receptor_id not in self.receptors:
            return False
        receptor = self.receptors[receptor_id]
        if receptor.state == ReceptorState.DESENSITIZED:
            return False
        binding_prob = receptor.affinity * ligand_concentration
        if np.random.random() < binding_prob:
            receptor.state = ReceptorState.BOUND
            receptor.bound_ligands += 1
            receptor.last_activated = datetime.utcnow()
            if receptor.bound_ligands >= 2:
                receptor.state = ReceptorState.ACTIVATED
                receptor.activation_count += 1
                self._activate_cascade(receptor)
                receptor.desensitization_time = 5.0
                receptor.state = ReceptorState.DESENSITIZED
                return True
        return False
    
    def _activate_cascade(self, receptor: SignalReceptor):
        if receptor.ligand in ['carbon_gradient', 'energy_signal']:
            messenger = SecondMessenger.cAMP
        elif receptor.ligand in ['emergency', 'stress_signal']:
            messenger = SecondMessenger.CALCIUM
        elif receptor.ligand in ['gradient_change', 'opportunity']:
            messenger = SecondMessenger.IP3
        else:
            messenger = SecondMessenger.NITRIC_OXIDE
        
        if messenger in self.second_messengers:
            sm = self.second_messengers[messenger]
            amp_factors = {AmplificationLevel.NONE: 1, AmplificationLevel.LOW: 10,
                          AmplificationLevel.MODERATE: 100, AmplificationLevel.HIGH: 1000,
                          AmplificationLevel.MAXIMUM: 10000}
            amp = amp_factors.get(receptor.amplification, 100)
            synthesis = sm.synthesis_rate * amp / 100.0
            sm.concentration = min(sm.max_concentration, sm.concentration + synthesis)
            self.amplification_history.append({
                'receptor': receptor.receptor_id, 'messenger': messenger.value,
                'amplification': amp, 'concentration': sm.concentration,
                'timestamp': datetime.utcnow().isoformat()
            })
    
    def get_second_messenger_level(self, messenger: SecondMessenger) -> float:
        if messenger in self.second_messengers:
            return self.second_messengers[messenger].concentration
        return 0.0
    
    def is_pathway_active(self, messenger: SecondMessenger) -> bool:
        if messenger in self.second_messengers:
            return self.second_messengers[messenger].concentration > self.second_messengers[messenger].threshold
        return False
    
    async def _signal_degradation_loop(self):
        while True:
            try:
                for sm in self.second_messengers.values():
                    sm.concentration = max(0.0, sm.concentration - sm.degradation_rate)
                for receptor in self.receptors.values():
                    if receptor.state == ReceptorState.DESENSITIZED:
                        receptor.desensitization_time -= 1.0
                        if receptor.desensitization_time <= 0:
                            receptor.state = ReceptorState.RESENSITIZED
                            receptor.bound_ligands = 0
                await asyncio.sleep(1.0)
            except Exception as e:
                logger.error(f"Signal degradation error: {str(e)}")
                await asyncio.sleep(5.0)
    
    def setup_crosstalk(self, pathway_a: SecondMessenger, pathway_b: SecondMessenger, strength: float):
        self.crosstalk_matrix[(pathway_a.value, pathway_b.value)] = strength
        self.crosstalk_matrix[(pathway_b.value, pathway_a.value)] = strength * 0.7
    
    def apply_crosstalk(self):
        for (path_a, path_b), strength in self.crosstalk_matrix.items():
            messenger_a = SecondMessenger(path_a)
            messenger_b = SecondMessenger(path_b)
            if messenger_a in self.second_messengers and messenger_b in self.second_messengers:
                sm_a = self.second_messengers[messenger_a]
                sm_b = self.second_messengers[messenger_b]
                if sm_a.concentration > sm_a.threshold:
                    sm_b.concentration = min(sm_b.max_concentration,
                        sm_b.concentration + sm_a.concentration * strength * 0.1)
    
    def get_signaling_status(self) -> Dict[str, Any]:
        return {
            'receptors': {rid: {'state': r.state.value, 'ligand': r.ligand,
                                'activations': r.activation_count}
                         for rid, r in self.receptors.items()},
            'second_messengers': {sm.value: {'concentration': m.concentration,
                                              'active': m.concentration > m.threshold}
                                  for sm, m in self.second_messengers.items()}
        }

# ============================================================================
# Allosteric Regulation System
# ============================================================================

class AllostericRegulationSystem:
    """Allosteric regulation for routing decisions"""
    
    def __init__(self):
        self.allosteric_sites: Dict[str, AllostericSite] = {}
        self.conformational_state: float = 0.5
        self.cooperativity: Dict[Tuple[str, str], float] = {}
        self.regulation_history: deque = deque(maxlen=1000)
        self._initialize_allosteric_sites()
        logger.info("Allosteric Regulation System initialized")
    
    def _initialize_allosteric_sites(self):
        self.allosteric_sites['carbon_site'] = AllostericSite('carbon_site', 'carbon_gradient', 'modulation', 0.7)
        self.allosteric_sites['helium_site'] = AllostericSite('helium_site', 'helium_gradient', 'inhibitory', 0.6)
        self.allosteric_sites['token_site'] = AllostericSite('token_site', 'token_availability', 'activating', 0.8)
        self.allosteric_sites['trust_site'] = AllostericSite('trust_site', 'trust_gradient', 'activating', 0.5)
        self.allosteric_sites['stress_site'] = AllostericSite('stress_site', 'stress_signal', 'inhibitory', 0.9)
    
    def bind_modulator(self, site_id: str, modulator_concentration: float) -> float:
        if site_id not in self.allosteric_sites:
            return 0.0
        site = self.allosteric_sites[site_id]
        n = 2.0
        Kd = 1.0 - site.binding_affinity
        occupancy = (modulator_concentration ** n) / (Kd ** n + modulator_concentration ** n)
        site.current_occupancy = occupancy
        if site.effect == 'activating':
            change = occupancy * 0.2
        elif site.effect == 'inhibitory':
            change = -occupancy * 0.2
        else:
            change = (occupancy - 0.5) * 0.1
        site.conformational_change = change
        self.conformational_state = max(0.0, min(1.0, self.conformational_state + change))
        self.regulation_history.append({
            'site': site_id, 'modulator': site.modulator,
            'concentration': modulator_concentration, 'occupancy': occupancy,
            'new_state': self.conformational_state, 'timestamp': datetime.utcnow().isoformat()
        })
        return change
    
    def get_routing_modulation(self) -> Dict[str, float]:
        state = self.conformational_state
        return {
            'exploration_rate': state * 0.3, 'exploitation_rate': 1.0 - state * 0.3,
            'risk_tolerance': state * 0.5, 'conservation_mode': (1.0 - state) * 0.8,
            'cooperativity_factor': state * 0.4, 'competition_factor': (1.0 - state) * 0.3
        }
    
    def setup_cooperativity(self, expert_a: str, expert_b: str, strength: float):
        self.cooperativity[(expert_a, expert_b)] = strength
        self.cooperativity[(expert_b, expert_a)] = strength
    
    def get_cooperativity_bonus(self, expert_a: str, expert_b: str) -> float:
        return self.cooperativity.get((expert_a, expert_b), 0.0)
    
    def get_regulation_status(self) -> Dict[str, Any]:
        return {
            'conformational_state': self.conformational_state,
            'state_description': 'relaxed' if self.conformational_state > 0.6 else
                                'tense' if self.conformational_state < 0.4 else 'intermediate',
            'routing_modulation': self.get_routing_modulation()
        }

# ============================================================================
# Metabolic Pathway Router
# ============================================================================

class MetabolicPathwayRouter:
    """Routes tasks through optimal metabolic pathways"""
    
    def __init__(self):
        self.pathways: Dict[str, MetabolicPathway] = {}
        self.enzyme_kinetics: Dict[str, Dict[str, float]] = {}
        self.product_levels: Dict[str, float] = defaultdict(float)
        self.throughput_history: deque = deque(maxlen=1000)
        self._initialize_pathways()
        logger.info("Metabolic Pathway Router initialized")
    
    def _initialize_pathways(self):
        self.pathways['energy_optimization'] = MetabolicPathway(
            'energy_optimization', 'optimization_task', ['energy_expert'],
            ['energy_analysis', 'optimization_plan', 'execution_strategy'],
            'optimized_energy_plan', 'optimization_plan', 10.0,
            [AllostericSite('energy_carbon_site', 'carbon_gradient', 'inhibitory', 0.6),
             AllostericSite('energy_token_site', 'token_availability', 'activating', 0.8)]
        )
        self.pathways['data_processing'] = MetabolicPathway(
            'data_processing', 'data_task', ['data_expert'],
            ['data_ingestion', 'transformation', 'analysis', 'output'],
            'processed_data', 'transformation', 8.0,
            [AllostericSite('data_helium_site', 'helium_gradient', 'inhibitory', 0.5),
             AllostericSite('data_trust_site', 'trust_gradient', 'activating', 0.7)]
        )
        self.pathways['edge_computing'] = MetabolicPathway(
            'edge_computing', 'edge_task', ['iot_expert'],
            ['local_processing', 'mesh_routing', 'result_aggregation'],
            'edge_result', 'mesh_routing', 5.0,
            [AllostericSite('edge_opportunity_site', 'opportunity_gradient', 'activating', 0.9)]
        )
        self.pathways['quantum_computing'] = MetabolicPathway(
            'quantum_computing', 'quantum_task', ['quantum_expert'],
            ['circuit_preparation', 'execution', 'error_mitigation', 'measurement'],
            'quantum_result', 'execution', 50.0,
            [AllostericSite('quantum_complexity_site', 'task_complexity', 'activating', 0.4)]
        )
        for pathway in self.pathways.values():
            for enzyme in pathway.enzymes:
                self.enzyme_kinetics[enzyme] = {'Km': 0.5, 'Vmax': 1.0, 'kcat': 10.0, 'specificity': 0.8}
    
    def calculate_reaction_rate(self, enzyme: str, substrate_concentration: float) -> float:
        if enzyme not in self.enzyme_kinetics:
            return 0.0
        kinetics = self.enzyme_kinetics[enzyme]
        return kinetics['Vmax'] * substrate_concentration / (kinetics['Km'] + substrate_concentration)
    
    def apply_competitive_inhibition(self, enzyme: str, inhibitor_concentration: float,
                                     inhibition_constant: float = 0.1) -> float:
        if enzyme not in self.enzyme_kinetics:
            return 1.0
        kinetics = self.enzyme_kinetics[enzyme]
        apparent_Km = kinetics['Km'] * (1 + inhibitor_concentration / inhibition_constant)
        return kinetics['Km'] / apparent_Km
    
    def apply_allosteric_regulation(self, pathway_id: str, modulator_levels: Dict[str, float]) -> float:
        if pathway_id not in self.pathways:
            return 1.0
        pathway = self.pathways[pathway_id]
        throughput_multiplier = 1.0
        for site in pathway.allosteric_regulators:
            if site.modulator in modulator_levels:
                concentration = modulator_levels[site.modulator]
                n = 1.5
                Kd = 1.0 - site.binding_affinity
                occupancy = concentration ** n / (Kd ** n + concentration ** n)
                if site.effect == 'activating':
                    throughput_multiplier *= (1.0 + occupancy * 0.5)
                elif site.effect == 'inhibitory':
                    throughput_multiplier *= (1.0 - occupancy * 0.5)
        return max(0.1, throughput_multiplier)
    
    def select_optimal_pathway(self, task_type: str, substrate_concentration: float,
                               modulator_levels: Dict[str, float], energy_budget: float) -> Tuple[Optional[str], float]:
        candidates = []
        for pathway_id, pathway in self.pathways.items():
            if task_type not in pathway.input_substrate and pathway.input_substrate not in task_type:
                continue
            if not pathway.is_active:
                continue
            total_rate = 0.0
            for enzyme in pathway.enzymes:
                rate = self.calculate_reaction_rate(enzyme, substrate_concentration)
                inhibitor_level = sum(self.product_levels.get(p.final_product, 0)
                                     for p in self.pathways.values() if p.pathway_id != pathway_id)
                inhibition = self.apply_competitive_inhibition(enzyme, inhibitor_level)
                rate *= inhibition
                total_rate += rate
            avg_rate = total_rate / max(len(pathway.enzymes), 1)
            allosteric_multiplier = self.apply_allosteric_regulation(pathway_id, modulator_levels)
            regulated_rate = avg_rate * allosteric_multiplier
            energy_efficiency = regulated_rate / max(pathway.energy_cost_ecoatp, 1)
            if pathway.energy_cost_ecoatp > energy_budget:
                energy_efficiency *= 0.3
            candidates.append((pathway_id, energy_efficiency))
        if not candidates:
            return None, 0.0
        candidates.sort(key=lambda x: x[1], reverse=True)
        return candidates[0]
    
    def record_throughput(self, pathway_id: str, actual_rate: float, energy_used: float):
        self.throughput_history.append({
            'pathway': pathway_id, 'rate': actual_rate, 'energy': energy_used,
            'timestamp': datetime.utcnow().isoformat()
        })
        if pathway_id in self.pathways:
            product = self.pathways[pathway_id].final_product
            self.product_levels[product] += actual_rate * 0.1
    
    def apply_product_inhibition(self):
        for product, level in self.product_levels.items():
            for pathway in self.pathways.values():
                if pathway.final_product == product and level > 5.0:
                    pathway.throughput_rate *= 0.9
                    self.product_levels[product] *= 0.8
    
    def get_pathway_stats(self) -> Dict[str, Any]:
        return {pid: {'throughput_rate': p.throughput_rate, 'energy_cost': p.energy_cost_ecoatp,
                      'is_active': p.is_active} for pid, p in self.pathways.items()}

# ============================================================================
# Enhanced Expert Router with Complete Features
# ============================================================================

class ExpertRouter:
    """
    Enhanced Expert Router v5.0.0 - Complete Signal Transduction Cascade
    
    Features:
    - Signal transduction for task routing
    - Allosteric regulation by gradient fields
    - Metabolic pathway selection
    - What-if analysis for routing scenarios
    - Causal inference for decision factors
    - Natural language explanations
    - Routing forecasts
    """
    
    def __init__(
        self,
        enable_quantum: bool = False,
        metrics_collector: Optional[Any] = None,
        enable_signal_transduction: bool = True,
        enable_allosteric: bool = True,
        enable_metabolic_pathways: bool = True,
        enable_cooperative_binding: bool = True,
        enable_homeostasis: bool = True,
        enable_bio_integration: bool = True
    ):
        self.enable_signal_transduction = enable_signal_transduction
        self.enable_allosteric = enable_allosteric
        self.enable_metabolic_pathways = enable_metabolic_pathways
        self.enable_cooperative_binding = enable_cooperative_binding
        self.enable_homeostasis = enable_homeostasis
        self.enable_bio_integration = enable_bio_integration
        
        # Bio-inspired subsystems
        self.signal_engine = SignalTransductionEngine() if enable_signal_transduction else None
        self.allosteric_system = AllostericRegulationSystem() if enable_allosteric else None
        self.metabolic_router = MetabolicPathwayRouter() if enable_metabolic_pathways else None
        
        # Bio-inspired module references (injected)
        self.gradient_manager = None
        self.token_manager = None
        self.scheduler = None
        self.compartment_manager = None
        self.biomass_storage = None
        self.harvester = None
        self.bio_core = None
        
        # Initialize signal receptors
        if self.signal_engine:
            self.signal_engine.create_receptor('carbon_receptor', SignalType.ENDOCRINE,
                'carbon_gradient', affinity=0.7, amplification=AmplificationLevel.HIGH)
            self.signal_engine.create_receptor('helium_receptor', SignalType.ENDOCRINE,
                'helium_gradient', affinity=0.6, amplification=AmplificationLevel.MODERATE)
            self.signal_engine.create_receptor('task_receptor', SignalType.NEUROTRANSMITTER,
                'task_signal', affinity=0.9, amplification=AmplificationLevel.HIGH)
            self.signal_engine.create_receptor('stress_receptor', SignalType.AUTOCRINE,
                'stress_signal', affinity=0.8, amplification=AmplificationLevel.MAXIMUM)
            self.signal_engine.create_receptor('trust_receptor', SignalType.PARACRINE,
                'trust_gradient', affinity=0.5, amplification=AmplificationLevel.LOW)
            self.signal_engine.setup_crosstalk(SecondMessenger.cAMP, SecondMessenger.IP3, 0.3)
            self.signal_engine.setup_crosstalk(SecondMessenger.CALCIUM, SecondMessenger.cAMP, 0.5)
        
        if self.allosteric_system:
            self.allosteric_system.setup_cooperativity('energy', 'data', 0.4)
            self.allosteric_system.setup_cooperativity('energy', 'helium', 0.3)
            self.allosteric_system.setup_cooperativity('data', 'iot', 0.5)
        
        self.metrics_collector = metrics_collector
        self.metrics = RoutingMetrics()
        self.experts: Dict[str, Any] = {}
        self.expert_index_map: Dict[int, str] = {}
        self.circuit_breakers: Dict[str, ExpertCircuitBreaker] = {}
        self.gating_network = None
        self.active_routes = 0
        self.max_concurrent_routes = 100
        self._route_lock = asyncio.Lock()
        self.routing_history: deque = deque(maxlen=10000)
        
        self._initialize_experts(enable_quantum)
        self._start_background_tasks()
        
        logger.info(f"Expert Router v5.0.0 initialized with all features")
    
    def _initialize_experts(self, enable_quantum: bool):
        try:
            from .experts.energy_expert import EnergyExpert
            from .experts.data_expert import DataExpert
            from .experts.iot_expert import IoTExpert
            from .experts.helium_expert import HeliumExpert
            
            self.experts = {
                'energy': EnergyExpert(),
                'data': DataExpert(),
                'iot': IoTExpert(),
                'helium': HeliumExpert()
            }
            if enable_quantum:
                from .experts.quantum_expert import QuantumExpert
                self.experts['quantum'] = QuantumExpert()
            
            for idx, (expert_id, expert) in enumerate(self.experts.items()):
                self.expert_index_map[idx] = expert_id
                self.circuit_breakers[expert_id] = ExpertCircuitBreaker(expert_id=expert_id)
            logger.info(f"Initialized {len(self.experts)} experts")
        except Exception as e:
            logger.error(f"Failed to initialize experts: {str(e)}")
    
    def _start_background_tasks(self):
        asyncio.create_task(self._signal_transduction_loop())
        asyncio.create_task(self._homeostasis_loop())
        asyncio.create_task(self._product_inhibition_loop())
    
    # ========================================================================
    # Bio-Inspired Module Injection
    # ========================================================================
    
    def inject_bio_core(self, bio_core: Any = None, **kwargs):
        if bio_core:
            self.bio_core = bio_core
            self.gradient_manager = getattr(bio_core, 'gradient_manager', None)
            self.token_manager = getattr(bio_core, 'token_manager', None)
            self.scheduler = getattr(bio_core, 'scheduler', None)
            self.compartment_manager = getattr(bio_core, 'compartment_manager', None)
            self.biomass_storage = getattr(bio_core, 'biomass_storage', None)
            self.harvester = getattr(bio_core, 'harvester', None)
        else:
            self.gradient_manager = kwargs.get('gradient_manager')
            self.token_manager = kwargs.get('token_manager')
            self.scheduler = kwargs.get('scheduler')
            self.compartment_manager = kwargs.get('compartment_manager')
            self.biomass_storage = kwargs.get('biomass_storage')
            self.harvester = kwargs.get('harvester')
        logger.info("Bio-inspired core injected into Expert Router")
    
    # ========================================================================
    # Real Data Access Methods
    # ========================================================================
    
    def _get_real_gradient_levels(self) -> Dict[str, float]:
        if self.gradient_manager:
            return self.gradient_manager.get_field_strengths()
        return {'carbon': 0.5, 'helium': 0.5, 'trust': 0.5, 'opportunity': 0.5}
    
    def _get_real_token_availability(self) -> float:
        if self.token_manager:
            summary = self.token_manager.get_system_summary()
            return min(1.0, summary.get('total_balance', 500) / 1000.0)
        return 0.5
    
    def _get_real_stress_level(self) -> float:
        stress = 0.0
        if self.gradient_manager:
            carbon = self.gradient_manager.fields.get('carbon')
            if carbon and carbon.gradient_strength > 0.7:
                stress += 0.4
        if self.token_manager:
            summary = self.token_manager.get_system_summary()
            if summary.get('total_balance', 1000) < 200:
                stress += 0.3
        utilization = self.active_routes / max(self.max_concurrent_routes, 1)
        if utilization > 0.8:
            stress += 0.3
        return min(1.0, stress)
    
    def _get_compartment_health(self, expert_id: str) -> float:
        if self.compartment_manager:
            compartment = self.compartment_manager.find_best_compartment(expert_id)
            if compartment:
                return compartment.health_score
        return 0.7
    
    # ========================================================================
    # Background Loops
    # ========================================================================
    
    async def _signal_transduction_loop(self):
        while True:
            try:
                if self.signal_engine:
                    gradient_levels = self._get_real_gradient_levels()
                    self.signal_engine.bind_ligand('carbon_receptor', gradient_levels.get('carbon', 0.5))
                    self.signal_engine.bind_ligand('helium_receptor', gradient_levels.get('helium', 0.5))
                    self.signal_engine.bind_ligand('trust_receptor', gradient_levels.get('trust', 0.5))
                    token_level = self._get_real_token_availability()
                    stress_level = self._get_real_stress_level()
                    if stress_level > 0.5:
                        self.signal_engine.bind_ligand('stress_receptor', stress_level)
                    self.signal_engine.apply_crosstalk()
                    if self.allosteric_system:
                        self.allosteric_system.bind_modulator('carbon_site', gradient_levels.get('carbon', 0.5))
                        self.allosteric_system.bind_modulator('helium_site', gradient_levels.get('helium', 0.5))
                        self.allosteric_system.bind_modulator('trust_site', gradient_levels.get('trust', 0.5))
                        self.allosteric_system.bind_modulator('token_site', token_level)
                        if stress_level > 0.3:
                            self.allosteric_system.bind_modulator('stress_site', stress_level)
                await asyncio.sleep(2.0)
            except Exception as e:
                logger.error(f"Signal transduction error: {str(e)}")
                await asyncio.sleep(5.0)
    
    async def _homeostasis_loop(self):
        while True:
            try:
                if self.enable_homeostasis and self.allosteric_system:
                    modulation = self.allosteric_system.get_routing_modulation()
                    if modulation['conservation_mode'] > 0.7:
                        if np.random.random() < 0.1:
                            self.allosteric_system.bind_modulator('token_site', 0.8)
                    if modulation['risk_tolerance'] > 0.4:
                        self.allosteric_system.bind_modulator('stress_site', 0.3)
                await asyncio.sleep(10.0)
            except Exception as e:
                logger.error(f"Homeostasis error: {str(e)}")
                await asyncio.sleep(30.0)
    
    async def _product_inhibition_loop(self):
        while True:
            try:
                if self.metabolic_router:
                    self.metabolic_router.apply_product_inhibition()
                await asyncio.sleep(30.0)
            except Exception as e:
                logger.error(f"Product inhibition error: {str(e)}")
                await asyncio.sleep(60.0)
    
    # ========================================================================
    # Main Routing Method
    # ========================================================================
    
    async def route_and_execute(
        self, workload_profile: Dict[str, Any], meta_cognitive_state: Dict[str, Any],
        dual_axis_context: Dict[str, Any], symbolic_constraints: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> Dict[str, Any]:
        start_time = time.time()
        route_id = hashlib.md5(f"{workload_profile}{start_time}".encode()).hexdigest()[:12]
        
        async with self._route_lock:
            self.active_routes += 1
        self.metrics.total_routes += 1
        
        try:
            gradient_levels = self._get_real_gradient_levels()
            
            signal_activated = False
            if self.signal_engine:
                task_signal = workload_profile.get('complexity', 0.5)
                signal_activated = self.signal_engine.bind_ligand('task_receptor', task_signal)
            
            routing_modulation = {}
            if self.allosteric_system:
                routing_modulation = self.allosteric_system.get_routing_modulation()
            
            modulator_levels = {
                'carbon_gradient': gradient_levels.get('carbon', 0.5),
                'helium_gradient': gradient_levels.get('helium', 0.5),
                'token_availability': self._get_real_token_availability(),
                'trust_gradient': gradient_levels.get('trust', 0.5),
                'opportunity_gradient': gradient_levels.get('opportunity', 0.5),
                'task_complexity': workload_profile.get('complexity', 0.5)
            }
            
            selected_pathway = None
            pathway_efficiency = 0.0
            if self.metabolic_router:
                task_type = workload_profile.get('task_type', 'general')
                substrate_conc = workload_profile.get('complexity', 0.5)
                energy_budget = meta_cognitive_state.get('carbon_budget_remaining', 100.0)
                selected_pathway, pathway_efficiency = self.metabolic_router.select_optimal_pathway(
                    task_type, substrate_conc, modulator_levels, energy_budget
                )
            
            gating_context = self._build_gating_context(workload_profile, meta_cognitive_state, dual_axis_context)
            routing_result = self.gating_network.route(gating_context) if self.gating_network else {
                'expert_indices': [0, 1], 'weights': [0.6, 0.4], 'confidence': 0.8
            }
            
            if routing_modulation:
                exploration = routing_modulation.get('exploration_rate', 0.1)
                if np.random.random() < exploration:
                    all_indices = list(range(len(self.experts)))
                    routing_result['expert_indices'] = list(np.random.choice(
                        all_indices, size=min(2, len(all_indices)), replace=False
                    ))
            
            expert_plans = await self._execute_experts(routing_result, workload_profile, meta_cognitive_state, dual_axis_context)
            
            if selected_pathway and self.metabolic_router:
                for plan in expert_plans:
                    plan['pathway_efficiency'] = pathway_efficiency
                self.metabolic_router.record_throughput(
                    selected_pathway, pathway_efficiency,
                    sum(p.get('estimated_energy_kwh', 0) for p in expert_plans)
                )
            
            final_plan = await self._aggregate_plans(expert_plans, dual_axis_context, gating_context)
            
            if self.enable_homeostasis and final_plan.get('action') == 'execute_full':
                carbon_zone = dual_axis_context.get('carbon_zone', 0)
                conservation = routing_modulation.get('conservation_mode', 0)
                if carbon_zone >= 10 and conservation > 0.6:
                    final_plan['action'] = 'execute_throttled'
            
            self.metrics.successful_routes += 1
            execution_time = (time.time() - start_time) * 1000
            self.metrics.average_latency_ms = (self.metrics.average_latency_ms * 0.9 + execution_time * 0.1)
            
            for plan in expert_plans:
                expert_id = plan.get('expert_id')
                if expert_id in self.circuit_breakers:
                    self.circuit_breakers[expert_id].record_success()
            
            response = {
                'success': True, 'route_id': route_id, 'plans': expert_plans,
                'final_plan': final_plan, 'execution_time_ms': execution_time,
                'bio_inspired_metadata': {
                    'signal_activated': signal_activated, 'selected_pathway': selected_pathway,
                    'pathway_efficiency': pathway_efficiency, 'gradient_levels': gradient_levels,
                    'token_availability': self._get_real_token_availability(),
                    'stress_level': self._get_real_stress_level(),
                    'routing_modulation': routing_modulation,
                    'second_messenger_levels': {
                        sm.value: self.signal_engine.get_second_messenger_level(sm)
                        for sm in SecondMessenger
                    } if self.signal_engine else {},
                    'timestamp': datetime.utcnow().isoformat()
                }
            }
            
            # Store in routing history
            self.routing_history.append({
                'route_id': route_id, 'decisions': list(zip(routing_result['expert_indices'], routing_result['weights'])),
                'context': gating_context, 'confidence': routing_result.get('confidence', 0.5),
                'timestamp': datetime.utcnow()
            })
            
            return response
            
        except Exception as e:
            logger.error(f"Routing failed: {str(e)}", exc_info=True)
            self.metrics.failed_routes += 1
            return {'success': False, 'error': str(e), 'fallback': True, 'action': 'execute_minimal'}
        finally:
            async with self._route_lock:
                self.active_routes -= 1
    
    def _build_gating_context(self, workload_profile, meta_cognitive_state, dual_axis_context):
        from .gating_network import GatingContext
        return GatingContext(
            task_type=workload_profile.get('task_type', 'inference'),
            task_complexity=workload_profile.get('complexity', 0.5),
            input_size_mb=workload_profile.get('input_size_mb', 1.0),
            carbon_budget_remaining=meta_cognitive_state.get('carbon_budget_remaining', 1.0),
            helium_budget_remaining=meta_cognitive_state.get('helium_budget_remaining', 1.0),
            latency_budget_ms=meta_cognitive_state.get('latency_budget_ms', 100.0),
            historical_success_rate=meta_cognitive_state.get('historical_success_rate', 0.9),
            carbon_zone=dual_axis_context.get('carbon_zone', 0),
            helium_scarcity=dual_axis_context.get('helium_scarcity', 0.5),
            time_of_day=datetime.utcnow().hour,
            grid_carbon_intensity=workload_profile.get('grid_carbon_intensity', 400.0),
            hardware_availability=workload_profile.get('hardware_availability', {
                'cpu': 1.0, 'gpu': 0.8, 'quantum': 0.0, 'edge': 0.5
            })
        )
    
    async def _execute_experts(self, routing_result, workload_profile, meta_cognitive_state, dual_axis_context):
        plans = []
        for expert_idx, weight in zip(routing_result['expert_indices'], routing_result['weights']):
            expert_id = self.expert_index_map.get(expert_idx)
            if not expert_id or expert_id not in self.experts:
                continue
            expert = self.experts[expert_id]
            try:
                inhibition_factor = 1.0
                if self.metabolic_router and expert_id in self.metabolic_router.enzyme_kinetics:
                    inhibitor_level = sum(self.metabolic_router.product_levels.values())
                    inhibition_factor = self.metabolic_router.apply_competitive_inhibition(expert_id, inhibitor_level)
                if inhibition_factor < 0.3:
                    continue
                plan = {
                    'expert_id': expert_id, 'routing_weight': float(weight),
                    'estimated_carbon_kg': getattr(expert.profile, 'carbon_per_inference', 0.0001),
                    'estimated_helium_units': getattr(expert.profile, 'helium_per_inference', 0.01),
                    'estimated_energy_kwh': getattr(expert.profile, 'energy_per_inference', 0.001),
                    'estimated_latency_ms': getattr(expert.profile, 'avg_latency_ms', 50.0),
                    'inhibition_factor': inhibition_factor
                }
                plans.append(plan)
            except Exception as e:
                logger.error(f"Expert {expert_id} failed: {str(e)}")
        return plans
    
    async def _aggregate_plans(self, expert_plans, dual_axis_context, gating_context):
        if not expert_plans:
            return {'action': 'defer', 'reason': 'No expert plans available'}
        total_weight = sum(p.get('routing_weight', 0) for p in expert_plans)
        if total_weight > 0:
            for plan in expert_plans:
                plan['normalized_weight'] = plan.get('routing_weight', 0) / total_weight
        carbon_zone = dual_axis_context.get('carbon_zone', 0)
        helium_scarcity = dual_axis_context.get('helium_scarcity', 0.5)
        if carbon_zone >= 12 and helium_scarcity > 0.8:
            action = 'defer'
        elif carbon_zone >= 8 or helium_scarcity > 0.6:
            action = 'execute_minimal'
        elif carbon_zone >= 4 or helium_scarcity > 0.3:
            action = 'execute_throttled'
        else:
            action = 'execute_full'
        return {
            'action': action,
            'aggregate_carbon_kg': sum(p.get('estimated_carbon_kg', 0) * p.get('normalized_weight', 0) for p in expert_plans),
            'aggregate_helium': sum(p.get('estimated_helium_units', 0) * p.get('normalized_weight', 0) for p in expert_plans),
            'expert_count': len(expert_plans)
        }
    
    # ========================================================================
    # What-If Analysis
    # ========================================================================
    
    def run_what_if_routing(self, task: Dict[str, Any], alternative_scenarios: List[Dict[str, Any]]) -> Dict[str, Any]:
        results = {'task': task, 'timestamp': datetime.utcnow().isoformat(), 'scenarios': []}
        baseline = self._simulate_routing(task, {})
        results['baseline'] = baseline
        for scenario in alternative_scenarios:
            scenario_result = self._simulate_routing(task, scenario)
            results['scenarios'].append({
                'scenario': scenario,
                'routing': scenario_result,
                'differs_from_baseline': (
                    set(scenario_result.get('selected_experts', [])) != 
                    set(baseline.get('selected_experts', []))
                )
            })
        results['recommendations'] = self._generate_what_if_recommendations(baseline, results['scenarios'])
        return results
    
    def _simulate_routing(self, task: Dict[str, Any], overrides: Dict[str, Any]) -> Dict[str, Any]:
        context = {
            'task_type': task.get('task_type', 'inference'),
            'complexity': overrides.get('complexity', task.get('complexity', 0.5)),
            'carbon_zone': overrides.get('carbon_zone', task.get('carbon_zone', 3)),
            'helium_scarcity': overrides.get('helium_scarcity', task.get('helium_dependency', 0.2)),
            'token_balance': overrides.get('token_balance', 500),
            'carbon_gradient': overrides.get('carbon_gradient', 0.5)
        }
        expert_scores = {}
        for expert_id, expert in self.experts.items():
            score = 0.5
            if hasattr(expert, 'profile'):
                if context['carbon_zone'] > 8:
                    score += (1.0 - expert.profile.carbon_per_inference * 10000) * 0.3
            if context['token_balance'] < 100:
                score += (1.0 - getattr(expert.profile, 'energy_per_inference', 0.001) * 1000) * 0.3
            if self.enable_bio_integration:
                score += context['carbon_gradient'] * 0.2
            expert_scores[expert_id] = score
        sorted_experts = sorted(expert_scores.items(), key=lambda x: x[1], reverse=True)
        top_experts = sorted_experts[:2]
        return {
            'context': context,
            'selected_experts': [e[0] for e in top_experts],
            'scores': {e[0]: round(e[1], 3) for e in top_experts},
            'confidence': top_experts[0][1] / sum(e[1] for e in top_experts) if top_experts else 0.5
        }
    
    def _generate_what_if_recommendations(self, baseline: Dict, scenarios: List[Dict]) -> List[str]:
        recommendations = []
        changed = [s for s in scenarios if s['differs_from_baseline']]
        if changed:
            recommendations.append(f"Routing sensitive to: {', '.join(list(changed[0]['scenario'].keys())[:3])}")
        baseline_conf = baseline.get('confidence', 0.5)
        for s in scenarios:
            scenario_conf = s['routing'].get('confidence', 0.5)
            if scenario_conf < baseline_conf * 0.7:
                recommendations.append(f"Confidence drops significantly under: {s['scenario']}")
        if not recommendations:
            recommendations.append("Routing is robust across all tested scenarios.")
        return recommendations
    
    # ========================================================================
    # Causal Inference for Routing
    # ========================================================================
    
    def analyze_causal_factors(self, route_id: str) -> Optional[Dict[str, Any]]:
        for record in self.routing_history:
            if record.get('route_id') == route_id:
                decisions = record.get('decisions', [])
                context = record.get('context', {})
                if not decisions:
                    return None
                causal_chain = []
                if hasattr(context, 'carbon_zone') and context.carbon_zone > 8:
                    causal_chain.append({
                        'factor': 'High Carbon Zone',
                        'impact': 'HIGH',
                        'effect': f'Carbon zone {context.carbon_zone} forced selection of low-carbon experts',
                        'strength': 0.8
                    })
                if hasattr(context, 'helium_scarcity') and context.helium_scarcity > 0.6:
                    causal_chain.append({
                        'factor': 'Helium Scarcity',
                        'impact': 'HIGH',
                        'effect': 'Restricted helium-intensive expert selection',
                        'strength': 0.7
                    })
                if hasattr(context, 'task_complexity') and context.task_complexity > 0.7:
                    causal_chain.append({
                        'factor': 'High Task Complexity',
                        'impact': 'MODERATE',
                        'effect': 'Required specialized expert handling',
                        'strength': 0.5
                    })
                if not causal_chain:
                    causal_chain.append({
                        'factor': 'Balanced Conditions',
                        'impact': 'LOW',
                        'effect': 'Standard routing based on performance scores',
                        'strength': 0.3
                    })
                causal_chain.sort(key=lambda x: x['strength'], reverse=True)
                
                selected = []
                for expert_idx, weight in decisions:
                    expert_id = self.expert_index_map.get(expert_idx, 'unknown')
                    selected.append({'expert': expert_id, 'weight': f"{weight:.2f}"})
                
                return {
                    'route_id': route_id,
                    'causal_chain': causal_chain,
                    'primary_driver': causal_chain[0] if causal_chain else None,
                    'selected_experts': selected,
                    'confidence': record.get('confidence', 0.5)
                }
        return None
    
    # ========================================================================
    # Natural Language Explanations
    # ========================================================================
    
    def explain_routing_decision(self, route_id: str) -> Optional[Dict[str, Any]]:
        for record in self.routing_history:
            if record.get('route_id') == route_id:
                decisions = record.get('decisions', [])
                context = record.get('context', {})
                if not decisions:
                    return None
                
                selected = []
                for expert_idx, weight in decisions:
                    expert_id = self.expert_index_map.get(expert_idx, 'unknown')
                    selected.append({'expert': expert_id, 'weight': f"{weight:.2f}"})
                
                factors = []
                if hasattr(context, 'carbon_zone') and context.carbon_zone > 8:
                    factors.append("High carbon zone favored efficient experts")
                if hasattr(context, 'helium_scarcity') and context.helium_scarcity > 0.7:
                    factors.append("Helium scarcity restricted cooling options")
                if hasattr(context, 'task_complexity') and context.task_complexity > 0.7:
                    factors.append("High complexity required specialized handling")
                
                executive = (
                    f"Selected {selected[0]['expert']} (weight: {selected[0]['weight']}) "
                    f"{'and ' + selected[1]['expert'] if len(selected) > 1 else ''} "
                    f"based on {len(factors)} primary factors."
                )
                
                counterfactual = "If carbon zone were lower, more experts would be available for selection."
                
                return {
                    'route_id': route_id,
                    'executive_summary': executive,
                    'selected_experts': selected,
                    'decision_factors': factors,
                    'counterfactual': counterfactual,
                    'confidence': record.get('confidence', 0.5),
                    'timestamp': record.get('timestamp', datetime.utcnow()).isoformat()
                }
        return None
    
    # ========================================================================
    # Routing Forecast
    # ========================================================================
    
    def get_routing_forecast(self, task_type: str, horizon_minutes: int = 30) -> Dict[str, Any]:
        gradient_trends = {}
        current = self._get_real_gradient_levels()
        gradient_trends = {k: 'rising' if v > 0.6 else 'falling' if v < 0.4 else 'stable' for k, v in current.items()}
        
        utilization = self.gating_network.get_expert_utilization() if self.gating_network else {}
        
        predictions = {}
        for expert_id in self.experts:
            if gradient_trends.get('carbon') == 'rising':
                trend = 'increasing' if expert_id in ['energy', 'helium'] else 'stable'
            elif gradient_trends.get('carbon') == 'falling':
                trend = 'stable'
            else:
                trend = 'stable'
            predictions[expert_id] = {'predicted_trend': trend, 'confidence': 0.7}
        
        increasing = [k for k, v in predictions.items() if v['predicted_trend'] == 'increasing']
        recommendation = (
            f"Prepare {', '.join(increasing)} expert(s) for increased load." if increasing
            else "Routing patterns expected to remain stable."
        )
        
        return {
            'task_type': task_type, 'horizon_minutes': horizon_minutes,
            'gradient_trends': gradient_trends, 'expert_predictions': predictions,
            'recommendation': recommendation
        }
    
    # ========================================================================
    # Statistics
    # ========================================================================
    
    def get_routing_stats(self) -> Dict[str, Any]:
        stats = {
            'metrics': {
                'total_routes': self.metrics.total_routes,
                'successful_routes': self.metrics.successful_routes,
                'failed_routes': self.metrics.failed_routes,
                'success_rate': self.metrics.success_rate,
                'active_routes': self.active_routes
            },
            'gradient_levels': self._get_real_gradient_levels(),
            'token_availability': self._get_real_token_availability(),
            'stress_level': self._get_real_stress_level()
        }
        if self.signal_engine:
            stats['signal_transduction'] = self.signal_engine.get_signaling_status()
        if self.allosteric_system:
            stats['allosteric_regulation'] = self.allosteric_system.get_regulation_status()
        if self.metabolic_router:
            stats['metabolic_pathways'] = self.metabolic_router.get_pathway_stats()
        return stats
    
    def trigger_stress_response(self, stress_level: float):
        if self.signal_engine:
            self.signal_engine.bind_ligand('stress_receptor', stress_level)
    
    def reset_desensitization(self):
        if self.signal_engine:
            for receptor in self.signal_engine.receptors.values():
                receptor.state = ReceptorState.RESENSITIZED
                receptor.bound_ligands = 0
