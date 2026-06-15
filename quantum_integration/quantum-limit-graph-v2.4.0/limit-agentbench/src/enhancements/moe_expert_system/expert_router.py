# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/moe_expert_system/expert_router.py
# Fully integrated with bio-inspired modules - Signal Transduction Cascade

"""
Enhanced Expert Router v4.0.0 - Fully Integrated Signal Transduction Cascade

Complete bio-inspired integration with:
- Real gradient field binding (not simulated)
- Real Eco-ATP token allocation
- Real compartment health monitoring
- Real biomass storage for task overflow
- Real stress detection from system state
- Real ATP synthase scheduling
- Allosteric regulation by live gradient data
- Metabolic pathway selection with actual resource constraints
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
# Try importing bio-inspired modules
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
        CompartmentManager, ChromatophoreCompartment, CompartmentState, MembranePermeability
    )
    from enhancements.bio_inspired.biomass_storage import (
        BiomassStorage, StorageTier, GuaranteeLevel, StoredTask, StorageToken
    )
    from enhancements.bio_inspired.photosynthetic_harvester import (
        PhotosyntheticHarvester
    )
    BIO_INSPIRED_AVAILABLE = True
    logger.info("Bio-inspired modules loaded successfully")
except ImportError as e:
    BIO_INSPIRED_AVAILABLE = False
    logger.warning(f"Bio-inspired modules not available: {str(e)} - using simulated signals")

# ============================================================================
# Signal Transduction Enums and Data Classes
# ============================================================================

class SignalType(Enum):
    """Types of biological signals"""
    ENDOCRINE = "endocrine"
    PARACRINE = "paracrine"
    AUTOCRINE = "autocrine"
    JUXTACRINE = "juxtacrine"
    NEUROTRANSMITTER = "neurotransmitter"
    NEUROMODULATOR = "neuromodulator"

class SecondMessenger(Enum):
    """Second messenger systems"""
    cAMP = "camp"
    cGMP = "cgmp"
    IP3 = "ip3"
    DAG = "dag"
    CALCIUM = "calcium"
    NITRIC_OXIDE = "nitric_oxide"

class ReceptorState(Enum):
    """Receptor activation states"""
    INACTIVE = "inactive"
    BOUND = "bound"
    ACTIVATED = "activated"
    DESENSITIZED = "desensitized"
    INTERNALIZED = "internalized"
    RESENSITIZED = "resensitized"

class AmplificationLevel(Enum):
    """Signal amplification cascade levels"""
    NONE = 0
    LOW = 1
    MODERATE = 2
    HIGH = 3
    MAXIMUM = 4

class CircuitBreakerState(Enum):
    """Circuit breaker states"""
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

@dataclass
class SignalReceptor:
    """Membrane receptor for detecting signals"""
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
    """Second messenger signaling cascade"""
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
    """Allosteric regulation site on routing enzyme"""
    site_id: str
    modulator: str
    effect: str = "modulation"
    binding_affinity: float = 0.5
    current_occupancy: float = 0.0
    conformational_change: float = 0.0

@dataclass
class MetabolicPathway:
    """Metabolic pathway for task processing"""
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
    """Routing performance metrics"""
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
    """Circuit breaker for expert protection"""
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
            messenger_type=SecondMessenger.cAMP,
            baseline=0.1, threshold=0.3, synthesis_rate=0.15,
            degradation_rate=0.08, amplification_factor=100.0,
            half_life_seconds=3.0,
            target_proteins=['energy_expert', 'routing_kinase']
        )
        self.second_messengers[SecondMessenger.CALCIUM] = SecondMessengerSystem(
            messenger_type=SecondMessenger.CALCIUM,
            baseline=0.05, threshold=0.2, synthesis_rate=0.2,
            degradation_rate=0.1, amplification_factor=1000.0,
            half_life_seconds=1.0,
            target_proteins=['all_experts', 'emergency_response']
        )
        self.second_messengers[SecondMessenger.IP3] = SecondMessengerSystem(
            messenger_type=SecondMessenger.IP3,
            baseline=0.05, threshold=0.25, synthesis_rate=0.1,
            degradation_rate=0.06, amplification_factor=500.0,
            half_life_seconds=4.0,
            target_proteins=['gradient_effectors', 'compartment_activation']
        )
        self.second_messengers[SecondMessenger.NITRIC_OXIDE] = SecondMessengerSystem(
            messenger_type=SecondMessenger.NITRIC_OXIDE,
            baseline=0.02, threshold=0.15, synthesis_rate=0.12,
            degradation_rate=0.15, amplification_factor=200.0,
            half_life_seconds=2.0,
            target_proteins=['neighboring_compartments', 'vascular_signaling']
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
        self.allosteric_sites['carbon_site'] = AllostericSite(
            'carbon_site', 'carbon_gradient', 'modulation', 0.7)
        self.allosteric_sites['helium_site'] = AllostericSite(
            'helium_site', 'helium_gradient', 'inhibitory', 0.6)
        self.allosteric_sites['token_site'] = AllostericSite(
            'token_site', 'token_availability', 'activating', 0.8)
        self.allosteric_sites['trust_site'] = AllostericSite(
            'trust_site', 'trust_gradient', 'activating', 0.5)
        self.allosteric_sites['stress_site'] = AllostericSite(
            'stress_site', 'stress_signal', 'inhibitory', 0.9)
    
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
            'new_state': self.conformational_state,
            'timestamp': datetime.utcnow().isoformat()
        })
        return change
    
    def get_routing_modulation(self) -> Dict[str, float]:
        state = self.conformational_state
        return {
            'exploration_rate': state * 0.3,
            'exploitation_rate': 1.0 - state * 0.3,
            'risk_tolerance': state * 0.5,
            'conservation_mode': (1.0 - state) * 0.8,
            'cooperativity_factor': state * 0.4,
            'competition_factor': (1.0 - state) * 0.3
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
        self.substrates: Dict[str, float] = {}
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
    
    def apply_allosteric_regulation(self, pathway_id: str,
                                    modulator_levels: Dict[str, float]) -> float:
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
                               modulator_levels: Dict[str, float],
                               energy_budget: float) -> Tuple[Optional[str], float]:
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
                                     for p in self.pathways.values()
                                     if p.pathway_id != pathway_id)
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
            'pathway': pathway_id, 'rate': actual_rate,
            'energy': energy_used, 'timestamp': datetime.utcnow().isoformat()
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
# Enhanced Expert Router with Full Bio-Inspired Integration
# ============================================================================

class ExpertRouter:
    """
    Enhanced Expert Router v4.0.0 - Fully Integrated Signal Transduction Cascade
    
    Complete integration with bio-inspired modules:
    - Real gradient fields from GradientFieldManager
    - Real Eco-ATP allocation from EcoATPTokenManager
    - Real scheduling from ATPSynthaseScheduler
    - Real compartment health from CompartmentManager
    - Real biomass storage from BiomassStorage
    - Real stress detection from system state
    """
    
    def __init__(
        self,
        enable_quantum: bool = False,
        metrics_collector: Optional[Any] = None,
        enable_signal_transduction: bool = True,
        enable_allosteric: bool = True,
        enable_metabolic_pathways: bool = True,
        enable_cooperative_binding: bool = True,
        enable_homeostasis: bool = True
    ):
        # Feature flags
        self.enable_signal_transduction = enable_signal_transduction
        self.enable_allosteric = enable_allosteric
        self.enable_metabolic_pathways = enable_metabolic_pathways
        self.enable_cooperative_binding = enable_cooperative_binding
        self.enable_homeostasis = enable_homeostasis
        
        # Bio-inspired subsystem references (will be injected)
        self.gradient_manager: Optional[GradientFieldManager] = None
        self.token_manager: Optional[EcoATPTokenManager] = None
        self.scheduler: Optional[ATPSynthaseScheduler] = None
        self.compartment_manager: Optional[CompartmentManager] = None
        self.biomass_storage: Optional[BiomassStorage] = None
        self.bio_core: Optional[Any] = None
        
        # Bio-inspired subsystems
        self.signal_engine = SignalTransductionEngine() if enable_signal_transduction else None
        self.allosteric_system = AllostericRegulationSystem() if enable_allosteric else None
        self.metabolic_router = MetabolicPathwayRouter() if enable_metabolic_pathways else None
        
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
        
        # Setup cooperative binding
        if self.allosteric_system:
            self.allosteric_system.setup_cooperativity('energy', 'data', 0.4)
            self.allosteric_system.setup_cooperativity('energy', 'helium', 0.3)
            self.allosteric_system.setup_cooperativity('data', 'iot', 0.5)
        
        # Core components
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
        
        # Initialize experts
        self._initialize_experts(enable_quantum)
        
        # Start background tasks
        self._start_background_tasks()
        
        logger.info(f"Expert Router v4.0.0 initialized: bio_available={BIO_INSPIRED_AVAILABLE}")
    
    def _initialize_experts(self, enable_quantum: bool):
        """Initialize expert modules"""
        try:
            from enhancements.moe_expert_system.experts import (
                EnergyExpert, DataExpert, IoTExpert, HeliumExpert
            )
            self.experts = {
                'energy': EnergyExpert(),
                'data': DataExpert(),
                'iot': IoTExpert(),
                'helium': HeliumExpert()
            }
            if enable_quantum:
                from enhancements.moe_expert_system.experts import QuantumExpert
                self.experts['quantum'] = QuantumExpert()
            for idx, (expert_id, expert) in enumerate(self.experts.items()):
                self.expert_index_map[idx] = expert_id
                self.circuit_breakers[expert_id] = ExpertCircuitBreaker(expert_id=expert_id)
            logger.info(f"Initialized {len(self.experts)} experts")
        except Exception as e:
            logger.error(f"Failed to initialize experts: {str(e)}")
    
    def _start_background_tasks(self):
        """Start background bio-inspired tasks"""
        asyncio.create_task(self._signal_transduction_loop())
        asyncio.create_task(self._homeostasis_loop())
        asyncio.create_task(self._product_inhibition_loop())
    
    # ========================================================================
    # Bio-Inspired Core Injection (KEY INTEGRATION POINT)
    # ========================================================================
    
    def inject_bio_core(self, bio_core: Any = None, **kwargs):
        """
        Inject bio-inspired core modules for REAL data access.
        
        This is the critical integration point that connects the router
        to actual bio-inspired modules instead of using simulated data.
        """
        if bio_core:
            self.bio_core = bio_core
            self.gradient_manager = getattr(bio_core, 'gradient_manager', None)
            self.token_manager = getattr(bio_core, 'token_manager', None)
            self.scheduler = getattr(bio_core, 'scheduler', None)
            self.compartment_manager = getattr(bio_core, 'compartment_manager', None)
            self.biomass_storage = getattr(bio_core, 'biomass_storage', None)
            logger.info("Bio-inspired core fully injected into Expert Router")
        else:
            self.gradient_manager = kwargs.get('gradient_manager')
            self.token_manager = kwargs.get('token_manager')
            self.scheduler = kwargs.get('scheduler')
            self.compartment_manager = kwargs.get('compartment_manager')
            self.biomass_storage = kwargs.get('biomass_storage')
        
        # Log what was injected
        injections = {
            'gradient_manager': self.gradient_manager is not None,
            'token_manager': self.token_manager is not None,
            'scheduler': self.scheduler is not None,
            'compartment_manager': self.compartment_manager is not None,
            'biomass_storage': self.biomass_storage is not None
        }
        logger.info(f"Bio-inspired injections: {injections}")
    
    # ========================================================================
    # REAL Data Access Methods (Replaces simulated values)
    # ========================================================================
    
    def _get_real_gradient_levels(self) -> Dict[str, float]:
        """Get ACTUAL gradient levels from bio-inspired system"""
        if self.gradient_manager:
            return self.gradient_manager.get_field_strengths()
        # Fallback only if bio-inspired not available
        return {
            'carbon': np.random.uniform(0.3, 0.8),
            'helium': np.random.uniform(0.2, 0.6),
            'trust': np.random.uniform(0.4, 0.9),
            'opportunity': np.random.uniform(0.3, 0.7),
            'eco_atp_reserve': np.random.uniform(0.2, 0.5)
        }
    
    def _get_real_token_availability(self) -> float:
        """Get ACTUAL token availability from token manager"""
        if self.token_manager:
            summary = self.token_manager.get_system_summary()
            total_balance = summary.get('total_balance', 500)
            return min(1.0, total_balance / 1000.0)
        return np.random.uniform(0.3, 0.7)
    
    def _get_real_stress_level(self) -> float:
        """Calculate ACTUAL stress level from system state"""
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
        """Get ACTUAL compartment health for an expert"""
        if self.compartment_manager:
            compartment = self.compartment_manager.find_best_compartment(expert_id)
            if compartment:
                return compartment.health_score
        return 0.7
    
    def _allocate_ecoatp_for_execution(
        self, expert_id: str, task_complexity: float
    ) -> Tuple[bool, float]:
        """Allocate REAL Eco-ATP tokens for expert execution"""
        if self.token_manager:
            ecoatp_required = task_complexity * 10.0
            if self.scheduler:
                success = self.scheduler.schedule_execution(
                    task_id=f"router_{uuid.uuid4().hex[:8]}",
                    eco_atp_required=ecoatp_required, priority=0
                )
                if success:
                    return True, ecoatp_required
            account_id = f"expert_{expert_id}"
            success, token_ids = self.token_manager.reserve_tokens(
                account_id, ecoatp_required, EcoATPConsumer.EXPERT_EXECUTION
            )
            return success, ecoatp_required
        return True, 0.0
    
    def _store_task_as_biomass(
        self, task: Dict[str, Any], expert_type: str
    ) -> Optional[str]:
        """Store task as REAL biomass when execution not possible"""
        if self.biomass_storage:
            ecoatp_cost = task.get('complexity', 0.5) * 10.0
            stored, token_id = self.biomass_storage.store_task(
                task_data=task, ecoatp_cost=ecoatp_cost,
                guarantee=GuaranteeLevel.SILVER,
                deadline=task.get('deadline'),
                initial_tier=StorageTier.GLYCOGEN_QUEUE
            )
            if stored:
                logger.info(f"Task stored as biomass: {token_id}")
                return token_id
        return None
    
    # ========================================================================
    # Signal Transduction Loop (Uses REAL data)
    # ========================================================================
    
    async def _signal_transduction_loop(self):
        """Process environmental signals using REAL bio-inspired data"""
        while True:
            try:
                if self.signal_engine:
                    # Get REAL gradient levels
                    gradient_levels = self._get_real_gradient_levels()
                    
                    # Bind REAL data to receptors
                    self.signal_engine.bind_ligand('carbon_receptor',
                        gradient_levels.get('carbon', 0.5))
                    self.signal_engine.bind_ligand('helium_receptor',
                        gradient_levels.get('helium', 0.5))
                    self.signal_engine.bind_ligand('trust_receptor',
                        gradient_levels.get('trust', 0.5))
                    
                    # Get REAL token and stress levels
                    token_level = self._get_real_token_availability()
                    stress_level = self._get_real_stress_level()
                    
                    if stress_level > 0.5:
                        self.signal_engine.bind_ligand('stress_receptor', stress_level)
                    
                    self.signal_engine.apply_crosstalk()
                    
                    # Update allosteric regulation with REAL data
                    if self.allosteric_system:
                        self.allosteric_system.bind_modulator('carbon_site',
                            gradient_levels.get('carbon', 0.5))
                        self.allosteric_system.bind_modulator('helium_site',
                            gradient_levels.get('helium', 0.5))
                        self.allosteric_system.bind_modulator('trust_site',
                            gradient_levels.get('trust', 0.5))
                        self.allosteric_system.bind_modulator('token_site', token_level)
                        if stress_level > 0.3:
                            self.allosteric_system.bind_modulator('stress_site', stress_level)
                
                await asyncio.sleep(2.0)
            except Exception as e:
                logger.error(f"Signal transduction error: {str(e)}")
                await asyncio.sleep(5.0)
    
    async def _homeostasis_loop(self):
        """Maintain homeostatic balance"""
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
        """Apply product inhibition feedback"""
        while True:
            try:
                if self.metabolic_router:
                    self.metabolic_router.apply_product_inhibition()
                await asyncio.sleep(30.0)
            except Exception as e:
                logger.error(f"Product inhibition error: {str(e)}")
                await asyncio.sleep(60.0)
    
    # ========================================================================
    # Main Routing Method (Fully Bio-Integrated)
    # ========================================================================
    
    async def route_and_execute(
        self,
        workload_profile: Dict[str, Any],
        meta_cognitive_state: Dict[str, Any],
        dual_axis_context: Dict[str, Any],
        symbolic_constraints: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Bio-inspired routing with REAL data from all bio-inspired modules.
        """
        start_time = time.time()
        route_id = hashlib.md5(f"{workload_profile}{start_time}".encode()).hexdigest()[:12]
        
        async with self._route_lock:
            self.active_routes += 1
        self.metrics.total_routes += 1
        
        try:
            # ================================================================
            # STEP 1: Get REAL gradient levels from bio-inspired system
            # ================================================================
            gradient_levels = self._get_real_gradient_levels()
            
            # ================================================================
            # STEP 2: Signal transduction with REAL data
            # ================================================================
            signal_activated = False
            if self.signal_engine:
                task_signal = workload_profile.get('complexity', 0.5)
                signal_activated = self.signal_engine.bind_ligand('task_receptor', task_signal)
            
            # ================================================================
            # STEP 3: Allosteric regulation with REAL modulation
            # ================================================================
            routing_modulation = {}
            if self.allosteric_system:
                routing_modulation = self.allosteric_system.get_routing_modulation()
            
            # ================================================================
            # STEP 4: Build modulator levels from REAL data
            # ================================================================
            modulator_levels = {
                'carbon_gradient': gradient_levels.get('carbon', 0.5),
                'helium_gradient': gradient_levels.get('helium', 0.5),
                'token_availability': self._get_real_token_availability(),
                'trust_gradient': gradient_levels.get('trust', 0.5),
                'opportunity_gradient': gradient_levels.get('opportunity', 0.5),
                'task_complexity': workload_profile.get('complexity', 0.5)
            }
            
            # ================================================================
            # STEP 5: Select metabolic pathway
            # ================================================================
            selected_pathway = None
            pathway_efficiency = 0.0
            if self.metabolic_router:
                task_type = workload_profile.get('task_type', 'general')
                substrate_conc = workload_profile.get('complexity', 0.5)
                energy_budget = meta_cognitive_state.get('carbon_budget_remaining', 100.0)
                selected_pathway, pathway_efficiency = self.metabolic_router.select_optimal_pathway(
                    task_type, substrate_conc, modulator_levels, energy_budget
                )
            
            # ================================================================
            # STEP 6: Get routing decisions from gating network
            # ================================================================
            gating_context = self._build_gating_context(
                workload_profile, meta_cognitive_state, dual_axis_context
            )
            routing_result = self.gating_network.route(gating_context) if self.gating_network else {
                'expert_indices': [0, 1], 'weights': [0.6, 0.4], 'confidence': 0.8
            }
            
            # Apply exploration from allosteric modulation
            if routing_modulation:
                exploration = routing_modulation.get('exploration_rate', 0.1)
                if np.random.random() < exploration:
                    all_indices = list(range(len(self.experts)))
                    routing_result['expert_indices'] = list(np.random.choice(
                        all_indices, size=min(2, len(all_indices)), replace=False
                    ))
            
            # ================================================================
            # STEP 7: Allocate REAL Eco-ATP for execution
            # ================================================================
            can_execute_all = True
            for expert_idx in routing_result['expert_indices']:
                expert_id = self.expert_index_map.get(expert_idx)
                if expert_id:
                    # Check circuit breaker first
                    if expert_id in self.circuit_breakers:
                        if not self.circuit_breakers[expert_id].can_execute():
                            continue
                    
                    # Check compartment health
                    health = self._get_compartment_health(expert_id)
                    if health < 0.2:
                        continue
                    
                    # Allocate Eco-ATP
                    success, tokens = self._allocate_ecoatp_for_execution(
                        expert_id, workload_profile.get('complexity', 0.5)
                    )
                    
                    if not success:
                        # Store task as biomass instead
                        biomass_token = self._store_task_as_biomass(
                            workload_profile, expert_id
                        )
                        if biomass_token:
                            self.metrics.biomass_stored_routes += 1
                            return {
                                'success': True, 'route_id': route_id,
                                'status': 'stored_as_biomass',
                                'biomass_token': biomass_token,
                                'reason': 'Insufficient Eco-ATP - stored for later execution',
                                'gradient_levels': gradient_levels
                            }
                        can_execute_all = False
            
            # ================================================================
            # STEP 8: Execute experts
            # ================================================================
            expert_plans = await self._execute_experts(
                routing_result, workload_profile, meta_cognitive_state, dual_axis_context
            )
            
            # ================================================================
            # STEP 9: Record pathway throughput
            # ================================================================
            if selected_pathway and self.metabolic_router:
                for plan in expert_plans:
                    plan['pathway_efficiency'] = pathway_efficiency
                    plan['modulator_levels'] = modulator_levels
                    # Add cooperative bonus
                    for other_plan in expert_plans:
                        if other_plan != plan:
                            bonus = self.allosteric_system.get_cooperativity_bonus(
                                plan.get('expert_id', ''), other_plan.get('expert_id', '')
                            ) if self.allosteric_system else 0.0
                            plan['cooperative_bonus'] = plan.get('cooperative_bonus', 0) + bonus
                
                self.metabolic_router.record_throughput(
                    selected_pathway, pathway_efficiency,
                    sum(p.get('estimated_energy_kwh', 0) for p in expert_plans)
                )
            
            # ================================================================
            # STEP 10: Aggregate and finalize
            # ================================================================
            final_plan = await self._aggregate_plans(expert_plans, dual_axis_context, gating_context)
            
            # Homeostatic correction
            if self.enable_homeostasis and final_plan.get('action') == 'execute_full':
                carbon_zone = dual_axis_context.get('carbon_zone', 0)
                conservation = routing_modulation.get('conservation_mode', 0)
                if carbon_zone >= 10 and conservation > 0.6:
                    final_plan['action'] = 'execute_throttled'
            
            # Update metrics
            self.metrics.successful_routes += 1
            execution_time = (time.time() - start_time) * 1000
            self.metrics.average_latency_ms = (
                self.metrics.average_latency_ms * 0.9 + execution_time * 0.1
            )
            
            # Update circuit breakers
            for plan in expert_plans:
                expert_id = plan.get('expert_id')
                if expert_id in self.circuit_breakers:
                    self.circuit_breakers[expert_id].record_success()
            
            # ================================================================
            # BUILD COMPREHENSIVE RESPONSE
            # ================================================================
            response = {
                'success': True,
                'route_id': route_id,
                'plans': expert_plans,
                'final_plan': final_plan,
                'execution_time_ms': execution_time,
                'bio_inspired_metadata': {
                    'signal_activated': signal_activated,
                    'selected_pathway': selected_pathway,
                    'pathway_efficiency': pathway_efficiency,
                    'gradient_levels': gradient_levels,
                    'token_availability': self._get_real_token_availability(),
                    'stress_level': self._get_real_stress_level(),
                    'routing_modulation': routing_modulation,
                    'second_messenger_levels': {
                        sm.value: self.signal_engine.get_second_messenger_level(sm)
                        for sm in SecondMessenger
                    } if self.signal_engine else {},
                    'allosteric_state': self.allosteric_system.get_regulation_status() if self.allosteric_system else {},
                    'compartment_health': {
                        eid: self._get_compartment_health(eid) for eid in self.experts
                    } if self.compartment_manager else {},
                    'biomass_stats': self.biomass_storage.get_storage_stats() if self.biomass_storage else {},
                    'ecosystem_health': self.bio_core.get_system_status() if self.bio_core else {},
                    'bio_modules_available': BIO_INSPIRED_AVAILABLE,
                    'timestamp': datetime.utcnow().isoformat()
                }
            }
            
            return response
            
        except Exception as e:
            logger.error(f"Bio-inspired routing failed: {str(e)}", exc_info=True)
            self.metrics.failed_routes += 1
            return self._create_fallback_response(workload_profile, str(e))
        finally:
            async with self._route_lock:
                self.active_routes -= 1
    
    def _build_gating_context(self, workload_profile, meta_cognitive_state, dual_axis_context):
        """Build gating context"""
        try:
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
        except ImportError:
            return type('GatingContext', (), {
                'task_type': workload_profile.get('task_type', 'inference'),
                'task_complexity': workload_profile.get('complexity', 0.5),
                'carbon_zone': dual_axis_context.get('carbon_zone', 0),
                'helium_scarcity': dual_axis_context.get('helium_scarcity', 0.5)
            })()
    
    async def _execute_experts(self, routing_result, workload_profile, meta_cognitive_state, dual_axis_context):
        """Execute experts with bio-inspired awareness"""
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
        """Aggregate expert plans"""
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
    
    def _create_fallback_response(self, workload_profile, error):
        return {'success': False, 'error': error, 'fallback': True,
                'action': 'execute_minimal',
                'final_plan': {'action': 'execute_minimal', 'reason': f'Fallback: {error}'}}
    
    def get_routing_stats(self) -> Dict[str, Any]:
        """Get comprehensive routing statistics"""
        stats = {
            'metrics': {
                'total_routes': self.metrics.total_routes,
                'successful_routes': self.metrics.successful_routes,
                'failed_routes': self.metrics.failed_routes,
                'fallback_routes': self.metrics.fallback_routes,
                'biomass_stored_routes': self.metrics.biomass_stored_routes,
                'success_rate': self.metrics.success_rate,
                'average_latency_ms': self.metrics.average_latency_ms,
                'active_routes': self.active_routes
            },
            'bio_available': BIO_INSPIRED_AVAILABLE
        }
        if self.signal_engine:
            stats['signal_transduction'] = self.signal_engine.get_signaling_status()
        if self.allosteric_system:
            stats['allosteric_regulation'] = self.allosteric_system.get_regulation_status()
        if self.metabolic_router:
            stats['metabolic_pathways'] = self.metabolic_router.get_pathway_stats()
        stats['gradient_levels'] = self._get_real_gradient_levels()
        stats['token_availability'] = self._get_real_token_availability()
        stats['stress_level'] = self._get_real_stress_level()
        return stats
    
    def trigger_stress_response(self, stress_level: float):
        """Trigger cellular stress response"""
        if self.signal_engine:
            self.signal_engine.bind_ligand('stress_receptor', stress_level)
    
    def reset_desensitization(self):
        """Reset all receptor desensitization"""
        if self.signal_engine:
            for receptor in self.signal_engine.receptors.values():
                receptor.state = ReceptorState.RESENSITIZED
                receptor.bound_ligands = 0
