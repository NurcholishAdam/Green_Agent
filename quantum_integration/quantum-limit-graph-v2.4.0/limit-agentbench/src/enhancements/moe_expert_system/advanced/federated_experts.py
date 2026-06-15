# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/moe_expert_system/advanced/federated_experts.py
# Enhanced with complete bio-inspired integration - Metabolic Federation Network v4.0.0

"""
Enhanced Federated Experts v4.0.0 - Metabolic Federation Network

Complete bio-inspired integration with:
- Token-based incentive distribution (Eco-ATP rewards)
- Gradient-aligned client selection (trust gradient)
- Token-weighted aggregation (stake-proportional)
- Compartment health-aware fault tolerance
- Biomass-backed blockchain audit trail
- Trust gradient reputation system
- ATP-driven gating network synchronization
- Harvester signal quality for defense
- Token efficiency for capability assessment
- Gradient-modulated privacy budget allocation
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple, Set, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import numpy as np
import torch
import torch.nn as nn
import hashlib
import json
from collections import defaultdict, deque
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
import secrets
import copy
import math

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
        CompartmentManager, ChromatophoreCompartment, CompartmentState,
        MembranePermeability
    )
    from enhancements.bio_inspired.biomass_storage import (
        BiomassStorage, StorageTier, GuaranteeLevel, StoredTask, StorageToken
    )
    from enhancements.bio_inspired.photosynthetic_harvester import (
        PhotosyntheticHarvester
    )
    BIO_INSPIRED_AVAILABLE = True
    logger.info("Bio-inspired modules loaded for Federated Experts")
except ImportError as e:
    BIO_INSPIRED_AVAILABLE = False
    logger.warning(f"Bio-inspired modules not available: {str(e)} - using standard federation")

# ============================================================================
# Enums and Data Classes (Enhanced with Bio-Inspired)
# ============================================================================

class FederationTopology(Enum):
    """Federation topology types"""
    CENTRALIZED = "centralized"
    DECENTRALIZED = "decentralized"
    HIERARCHICAL = "hierarchical"
    SWARM = "swarm"
    CROSS_SILO = "cross_silo"
    CROSS_DEVICE = "cross_device"
    METABOLIC_MESH = "metabolic_mesh"  # BIO-INSPIRED

class AggregationStrategy(Enum):
    """Model aggregation strategies"""
    FED_AVG = "fed_avg"
    FED_PROX = "fed_prox"
    FED_OPT = "fed_opt"
    FED_DYN = "fed_dyn"
    FED_ENSEMBLE = "fed_ensemble"
    FED_DISTILL = "fed_distill"
    ADAPTIVE = "adaptive"
    TOKEN_WEIGHTED = "token_weighted"      # BIO-INSPIRED
    GRADIENT_ALIGNED = "gradient_aligned"  # BIO-INSPIRED
    HEALTH_AWARE = "health_aware"          # BIO-INSPIRED

class PrivacyLevel(Enum):
    """Privacy protection levels"""
    NONE = "none"
    BASIC = "basic"
    DIFFERENTIAL = "differential"
    SECURE_AGGREGATION = "secure_agg"
    FULLY_HOMOMORPHIC = "fully_homo"
    GRADIENT_MODULATED = "gradient_modulated"  # BIO-INSPIRED
    TOKEN_BACKED = "token_backed"              # BIO-INSPIRED

@dataclass
class ClientCapabilities:
    """Client hardware and network capabilities with bio-inspired metrics"""
    client_id: str
    compute_power_flops: float
    memory_gb: float
    network_bandwidth_mbps: float
    network_latency_ms: float
    energy_source_renewable: bool
    carbon_intensity_g_per_kwh: float
    helium_availability: float
    max_model_size_mb: float
    supported_architectures: List[str]
    availability_schedule: Dict[int, float]
    
    # BIO-INSPIRED
    token_efficiency: float = 0.5
    gradient_alignment: float = 0.5
    compartment_health: float = 0.7
    harvester_contribution: float = 0.0

@dataclass
class SecureModelUpdate:
    """Encrypted model update for secure aggregation with bio-inspired metadata"""
    client_id: str
    round_number: int
    encrypted_gradients: bytes
    encryption_metadata: Dict[str, Any]
    proof_of_training: bytes
    signature: bytes
    timestamp: datetime
    carbon_footprint_kg: float
    
    # BIO-INSPIRED
    tokens_staked: float = 0.0
    gradient_level: float = 0.5
    compartment_tier: str = "regional"
    harvester_confidence: float = 0.5
    token_efficiency: float = 0.5
    
    def verify_signature(self, public_key) -> bool:
        try:
            public_key.verify(
                self.signature, self.encrypted_gradients,
                padding.PSS(mgf=padding.MGF1(hashes.SHA256()),
                           salt_length=padding.PSS.MAX_LENGTH),
                hashes.SHA256()
            )
            return True
        except Exception:
            return False

@dataclass
class FederationRound:
    """Comprehensive federation round tracking with bio-inspired data"""
    round_id: str
    round_number: int
    started_at: datetime
    completed_at: Optional[datetime] = None
    participants: List[str] = field(default_factory=list)
    dropped_participants: List[str] = field(default_factory=list)
    aggregation_strategy: AggregationStrategy = AggregationStrategy.FED_AVG
    privacy_level: PrivacyLevel = PrivacyLevel.BASIC
    total_carbon_kg: float = 0.0
    total_helium_units: float = 0.0
    model_improvement: float = 0.0
    communication_bytes: int = 0
    successful: bool = False
    metrics: Dict[str, Any] = field(default_factory=dict)
    
    # BIO-INSPIRED
    tokens_distributed: float = 0.0
    trust_gradient_delta: float = 0.0
    biomass_audit_token: Optional[str] = None
    atp_sync_delay: float = 0.0

@dataclass
class FederatedExpert:
    """Enhanced federated expert with bio-inspired tracking"""
    expert_id: str
    local_model: Dict[str, Any]
    data_distribution: Dict[str, float]
    capabilities: ClientCapabilities
    carbon_footprint: float
    helium_usage: float
    privacy_budget: float = 1.0
    reputation_score: float = 0.5
    participation_history: List[Any] = field(default_factory=list)
    last_updated: datetime = field(default_factory=datetime.utcnow)
    is_active: bool = True
    architecture_type: str = "standard"
    
    # BIO-INSPIRED
    tokens_earned: float = 0.0
    tokens_staked: float = 0.0
    gradient_alignment: float = 0.5
    compartment_id: Optional[str] = None
    harvester_contribution: float = 0.0
    trust_pumping_count: int = 0
    
    def get_model_hash(self) -> str:
        model_str = json.dumps(
            {k: v.norm().item() if isinstance(v, torch.Tensor) else str(v)
             for k, v in self.local_model.items()},
            sort_keys=True
        )
        return hashlib.sha256(model_str.encode()).hexdigest()
    
    def calculate_contribution_potential(self) -> float:
        """Calculate potential contribution with bio-inspired factors"""
        data_quality = self._calculate_data_quality()
        compute_power = min(self.capabilities.compute_power_flops / 1e12, 1.0)
        network_score = min(self.capabilities.network_bandwidth_mbps / 1000, 1.0)
        sustainability = 1.0 if self.capabilities.energy_source_renewable else 0.5
        
        # BIO-INSPIRED: Add gradient and token factors
        bio_factors = (self.gradient_alignment * 0.3 + 
                      min(self.tokens_earned / 100.0, 1.0) * 0.2)
        
        return (0.25 * data_quality + 0.20 * compute_power + 
                0.15 * network_score + 0.15 * sustainability + 0.25 * bio_factors)
    
    def _calculate_data_quality(self) -> float:
        if not self.data_distribution:
            return 0.5
        entropy = 0
        for prob in self.data_distribution.values():
            if prob > 0:
                entropy -= prob * math.log(prob)
        max_entropy = math.log(len(self.data_distribution))
        return entropy / max_entropy if max_entropy > 0 else 0.5

# ============================================================================
# Enhanced Federated Orchestrator with Complete Bio-Inspired Integration
# ============================================================================

class EnhancedFederatedOrchestrator:
    """
    Enhanced Federated Orchestrator v4.0.0 - Metabolic Federation Network
    
    Complete bio-inspired integration:
    - Token-based incentive distribution
    - Gradient-aligned client selection
    - Token-weighted aggregation
    - Compartment health-aware fault tolerance
    - Biomass-backed blockchain audit
    - Trust gradient reputation system
    - ATP-driven gating network sync
    - Harvester signal quality for defense
    - Token efficiency for capability assessment
    - Gradient-modulated privacy budget allocation
    """
    
    def __init__(
        self,
        aggregation_strategy: AggregationStrategy = AggregationStrategy.ADAPTIVE,
        privacy_level: PrivacyLevel = PrivacyLevel.DIFFERENTIAL,
        topology: FederationTopology = FederationTopology.CENTRALIZED,
        min_participants: int = 3,
        privacy_epsilon: float = 1.0,
        enable_secure_aggregation: bool = True,
        enable_heterogeneous: bool = True,
        enable_incentives: bool = True,
        enable_blockchain_audit: bool = True,
        enable_compression: bool = True,
        enable_async: bool = True,
        enable_bio_integration: bool = True,
        max_straggler_wait_seconds: int = 60
    ):
        # Core configuration
        self.aggregation_strategy = aggregation_strategy
        self.privacy_level = privacy_level
        self.topology = topology
        self.min_participants = min_participants
        self.privacy_epsilon = privacy_epsilon
        
        # Feature flags
        self.enable_secure_aggregation = enable_secure_aggregation
        self.enable_heterogeneous = enable_heterogeneous
        self.enable_incentives = enable_incentives
        self.enable_blockchain_audit = enable_blockchain_audit
        self.enable_compression = enable_compression
        self.enable_async = enable_async
        self.enable_bio_integration = enable_bio_integration and BIO_INSPIRED_AVAILABLE
        self.max_straggler_wait_seconds = max_straggler_wait_seconds
        
        # BIO-INSPIRED: Module references (injected)
        self.token_manager: Optional[EcoATPTokenManager] = None
        self.gradient_manager: Optional[GradientFieldManager] = None
        self.scheduler: Optional[ATPSynthaseScheduler] = None
        self.compartment_manager: Optional[CompartmentManager] = None
        self.biomass_storage: Optional[BiomassStorage] = None
        self.harvester: Optional[PhotosyntheticHarvester] = None
        
        # Participants
        self.participants: Dict[str, FederatedExpert] = {}
        
        # Aggregation history
        self.aggregation_history: List[FederationRound] = []
        self.round_number = 0
        
        # Global model
        self.global_model: Optional[Dict[str, Any]] = None
        
        # BIO-INSPIRED: Federation token pool
        self.federation_token_pool: float = 1000.0
        
        # BIO-INSPIRED: Trust gradient tracking
        self.trust_gradient_history: deque = deque(maxlen=1000)
        
        # Blockchain audit
        self.audit_chain: List[Dict[str, Any]] = []
        self.chain_hash = "0" * 64
        
        logger.info(
            f"Enhanced Federated Orchestrator v4.0.0 initialized: "
            f"bio_integration={self.enable_bio_integration}, "
            f"bio_available={BIO_INSPIRED_AVAILABLE}"
        )
    
    # ========================================================================
    # Bio-Inspired Module Injection
    # ========================================================================
    
    def inject_bio_core(self, bio_core: Any = None, **kwargs):
        """
        Inject bio-inspired modules for federated learning.
        
        Connects federation to real bio-inspired systems.
        """
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
        logger.info(f"Bio-inspired injections into Federated Experts: {injections}")
        
        if any(injections.values()):
            self.enable_bio_integration = True
    
    # ========================================================================
    # Bio-Inspired Data Access Methods
    # ========================================================================
    
    def _distribute_token_incentives(
        self, participant_id: str, contribution: float, success: bool = True
    ) -> float:
        """
        Distribute Eco-ATP tokens as incentives for participation.
        
        Returns amount distributed.
        """
        if not self.token_manager:
            return 0.0
        
        # Base reward proportional to contribution
        base_reward = contribution * 10.0
        
        # Bonus for successful participation
        if success:
            base_reward *= 1.5
        
        # Generate tokens
        tokens = self.token_manager.generate_tokens(
            account_id=f"federated_{participant_id}",
            source=EcoATPSource.EFFICIENCY_GAIN,
            energy_saved_kwh=base_reward / 10000.0,
            num_tokens=int(base_reward)
        )
        
        if tokens:
            total = sum(t.value for t in tokens)
            if participant_id in self.participants:
                self.participants[participant_id].tokens_earned += total
            
            # Deduct from federation pool
            self.federation_token_pool -= total
            
            return total
        
        return 0.0
    
    def _get_gradient_aligned_selection(self, participant_id: str) -> float:
        """Get trust gradient for client selection weighting"""
        if self.gradient_manager:
            trust = self.gradient_manager.fields.get('trust')
            if trust:
                return trust.gradient_strength
        return 0.5
    
    def _get_token_weighted_aggregation(self, participant_id: str) -> float:
        """Get token balance for aggregation weighting"""
        if self.token_manager:
            account = self.token_manager.get_account_summary(f"federated_{participant_id}")
            if account:
                return account.get('balance', 0)
        if participant_id in self.participants:
            return self.participants[participant_id].tokens_earned
        return 0.0
    
    def _get_compartment_health_timeout(self, participant_id: str) -> float:
        """Get compartment health for adaptive timeout"""
        if self.compartment_manager:
            compartment = self.compartment_manager.find_best_compartment('data')
            if compartment:
                # Healthier compartments get longer timeouts
                return max(10.0, 60.0 * compartment.health_score)
        return 30.0
    
    def _store_audit_in_biomass(self, audit_data: Dict[str, Any]) -> Optional[str]:
        """Store audit record in biomass storage for immutability"""
        if self.biomass_storage:
            stored, token_id = self.biomass_storage.store_task(
                task_data=audit_data,
                ecoatp_cost=1.0,
                guarantee=GuaranteeLevel.BEST_EFFORT,
                initial_tier=StorageTier.LIPID_DEPOT
            )
            if stored:
                return token_id
        return None
    
    def _pump_trust_gradient(self, participant_id: str, success: bool, contribution: float):
        """Pump trust gradient based on participation quality"""
        if self.gradient_manager:
            delta = (0.05 * contribution) if success else (-0.1)
            self.gradient_manager.pump_field(
                'trust', delta, source=f"federated_{participant_id}"
            )
            
            if participant_id in self.participants:
                self.participants[participant_id].trust_pumping_count += 1
            
            self.trust_gradient_history.append({
                'participant': participant_id,
                'delta': delta,
                'success': success,
                'timestamp': datetime.utcnow().isoformat()
            })
    
    def _get_atp_driven_sync_timing(self) -> float:
        """Get ATP-driven sync timing based on energy availability"""
        if self.scheduler:
            driving_force = self.scheduler.calculate_gradient_driving_force()
            rotation_speed = self.scheduler.calculate_rotation_speed(driving_force)
            ecoatp_rate = self.scheduler.calculate_atp_production_rate(rotation_speed)
            
            if ecoatp_rate > 100:
                return 30.0  # Fast sync when energy abundant
            elif ecoatp_rate > 50:
                return 60.0  # Normal sync
            else:
                return 120.0  # Slow sync when energy scarce
        return 60.0
    
    def _get_harvester_confidence(self) -> float:
        """Get harvester signal confidence for gradient defense"""
        if self.harvester:
            stats = self.harvester.get_harvesting_stats()
            recent = stats.get('recent_conversions', [])
            if recent:
                return np.mean([c.get('convertible_energy', 0.5) for c in recent[-10:]])
        return 0.5
    
    def _get_token_efficiency(self, participant_id: str) -> float:
        """Get token efficiency for capability assessment"""
        if self.token_manager:
            account = self.token_manager.get_account_summary(f"federated_{participant_id}")
            if account:
                return account.get('efficiency_rating', 0.5)
        if participant_id in self.participants:
            participant = self.participants[participant_id]
            if participant.tokens_earned > 0:
                return min(1.0, participant.tokens_earned / 100.0)
        return 0.5
    
    def _get_gradient_modulated_privacy(self, base_epsilon: float) -> float:
        """Modulate privacy budget based on carbon gradient"""
        if self.gradient_manager:
            carbon = self.gradient_manager.fields.get('carbon')
            if carbon and carbon.gradient_strength > 0.7:
                return base_epsilon * 0.5  # Less privacy budget in high carbon stress
            elif carbon and carbon.gradient_strength < 0.3:
                return base_epsilon * 1.5  # More privacy budget when relaxed
        return base_epsilon
    
    def _get_real_gradient_levels(self) -> Dict[str, float]:
        """Get all gradient levels"""
        if self.gradient_manager:
            return self.gradient_manager.get_field_strengths()
        return {'carbon': 0.5, 'helium': 0.5, 'trust': 0.5, 'opportunity': 0.5}
    
    # ========================================================================
    # Participant Registration with Bio-Inspired Initialization
    # ========================================================================
    
    def register_participant(
        self,
        expert_id: str,
        initial_model: Dict[str, Any],
        data_distribution: Dict[str, float],
        capabilities: ClientCapabilities,
        carbon_footprint: float,
        helium_usage: float,
        public_key_pem: Optional[str] = None,
        architecture_type: str = "standard"
    ) -> bool:
        """Register federated participant with bio-inspired initialization"""
        if expert_id in self.participants:
            logger.warning(f"Participant {expert_id} already registered")
            return False
        
        # BIO-INSPIRED: Create token account
        if self.enable_bio_integration and self.token_manager:
            self.token_manager.create_account(f"federated_{expert_id}")
            # Initial token endowment
            initial_tokens = int(capabilities.compute_power_flops / 1e10)
            if initial_tokens > 0:
                self.token_manager.generate_tokens(
                    account_id=f"federated_{expert_id}",
                    source=EcoATPSource.EFFICIENCY_GAIN,
                    energy_saved_kwh=0.001,
                    num_tokens=initial_tokens
                )
        
        # BIO-INSPIRED: Update capabilities with bio metrics
        if self.enable_bio_integration:
            capabilities.token_efficiency = self._get_token_efficiency(expert_id)
            capabilities.gradient_alignment = self._get_gradient_aligned_selection(expert_id)
            capabilities.compartment_health = 0.7  # Default healthy
        
        participant = FederatedExpert(
            expert_id=expert_id,
            local_model=initial_model,
            data_distribution=data_distribution,
            capabilities=capabilities,
            carbon_footprint=carbon_footprint,
            helium_usage=helium_usage,
            privacy_budget=self.privacy_epsilon,
            architecture_type=architecture_type
        )
        
        self.participants[expert_id] = participant
        
        logger.info(f"Registered federated participant: {expert_id} (bio_initialized={self.enable_bio_integration})")
        return True
    
    # ========================================================================
    # Enhanced Federation Round with Complete Bio-Inspired Integration
    # ========================================================================
    
    async def federated_round(
        self,
        carbon_zone: int,
        helium_scarcity: float,
        timeout_seconds: Optional[int] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Execute enhanced federated learning round with bio-inspired integration.
        
        Features:
        - Gradient-aligned client selection
        - Token-weighted aggregation
        - Compartment health-aware timeouts
        - Token incentive distribution
        - Trust gradient pumping
        - Biomass audit storage
        - ATP-driven sync timing
        """
        self.round_number += 1
        round_start = datetime.utcnow()
        
        # BIO-INSPIRED: Get ATP-driven sync timing
        if self.enable_bio_integration:
            atp_delay = self._get_atp_driven_sync_timing()
            if timeout_seconds is None:
                timeout_seconds = int(atp_delay)
        
        timeout_seconds = timeout_seconds or self.max_straggler_wait_seconds
        
        # BIO-INSPIRED: Update participant bio metrics
        if self.enable_bio_integration:
            for participant_id, participant in self.participants.items():
                participant.gradient_alignment = self._get_gradient_aligned_selection(participant_id)
                participant.capabilities.token_efficiency = self._get_token_efficiency(participant_id)
        
        # Select participants with bio-inspired criteria
        selected = await self._select_participants_bio_aware(carbon_zone, helium_scarcity)
        
        if len(selected) < self.min_participants:
            logger.warning(f"Insufficient participants: {len(selected)} < {self.min_participants}")
            return None
        
        # BIO-INSPIRED: Modulate privacy budget
        if self.enable_bio_integration:
            effective_epsilon = self._get_gradient_modulated_privacy(self.privacy_epsilon)
        else:
            effective_epsilon = self.privacy_epsilon
        
        # Create federation round
        federation_round = FederationRound(
            round_id=f"round_{self.round_number}_{datetime.utcnow().timestamp()}",
            round_number=self.round_number,
            started_at=round_start,
            participants=selected,
            aggregation_strategy=self.aggregation_strategy,
            privacy_level=self.privacy_level,
            atp_sync_delay=self._get_atp_driven_sync_timing() if self.enable_bio_integration else 0.0
        )
        
        logger.info(f"Starting federation round {self.round_number} with {len(selected)} participants")
        
        try:
            # BIO-INSPIRED: Get compartment-aware timeouts
            adaptive_timeouts = {}
            if self.enable_bio_integration:
                for participant_id in selected:
                    adaptive_timeouts[participant_id] = self._get_compartment_health_timeout(participant_id)
            
            # Collect updates
            updates = {}
            total_tokens_staked = 0.0
            
            for participant_id in selected:
                if participant_id in self.participants:
                    participant = self.participants[participant_id]
                    
                    # BIO-INSPIRED: Get adaptive timeout
                    participant_timeout = adaptive_timeouts.get(participant_id, timeout_seconds)
                    
                    try:
                        update = await asyncio.wait_for(
                            self._collect_update(participant_id, effective_epsilon),
                            timeout=participant_timeout
                        )
                        
                        if update:
                            # BIO-INSPIRED: Add bio metadata to update
                            if self.enable_bio_integration:
                                update.tokens_staked = participant.tokens_earned
                                update.gradient_level = participant.gradient_alignment
                                update.token_efficiency = participant.capabilities.token_efficiency
                                update.harvester_confidence = self._get_harvester_confidence()
                            
                            updates[participant_id] = update
                            total_tokens_staked += participant.tokens_earned
                    
                    except asyncio.TimeoutError:
                        logger.warning(f"Participant {participant_id} timed out")
                        federation_round.dropped_participants.append(participant_id)
            
            if len(updates) < self.min_participants:
                logger.warning(f"Insufficient updates: {len(updates)}")
                return None
            
            # BIO-INSPIRED: Select aggregation strategy based on bio state
            if self.enable_bio_integration:
                if total_tokens_staked > 100:
                    strategy = AggregationStrategy.TOKEN_WEIGHTED
                elif self.gradient_manager:
                    strategy = AggregationStrategy.GRADIENT_ALIGNED
                else:
                    strategy = self.aggregation_strategy
                federation_round.aggregation_strategy = strategy
            
            # Aggregate updates
            global_model = self._aggregate_updates_bio_aware(updates, federation_round.aggregation_strategy)
            
            # Update global model
            self.global_model = global_model
            
            # BIO-INSPIRED: Distribute token incentives
            if self.enable_bio_integration and self.enable_incentives:
                total_distributed = 0.0
                for participant_id in updates:
                    participant = self.participants.get(participant_id)
                    if participant:
                        contribution = participant.calculate_contribution_potential()
                        distributed = self._distribute_token_incentives(
                            participant_id, contribution, success=True
                        )
                        total_distributed += distributed
                        
                        # Pump trust gradient
                        self._pump_trust_gradient(participant_id, success=True, contribution=contribution)
                
                federation_round.tokens_distributed = total_distributed
                federation_round.trust_gradient_delta = 0.05
            
            # BIO-INSPIRED: Store audit in biomass
            if self.enable_bio_integration and self.enable_blockchain_audit:
                audit_data = {
                    'round_number': self.round_number,
                    'participants': selected,
                    'strategy': federation_round.aggregation_strategy.value,
                    'tokens_distributed': federation_round.tokens_distributed,
                    'timestamp': datetime.utcnow().isoformat()
                }
                biomass_token = self._store_audit_in_biomass(audit_data)
                if biomass_token:
                    federation_round.biomass_audit_token = biomass_token
            
            # Complete round
            federation_round.completed_at = datetime.utcnow()
            federation_round.successful = True
            federation_round.total_carbon_kg = sum(
                self.participants[pid].carbon_footprint for pid in selected if pid in self.participants
            )
            
            self.aggregation_history.append(federation_round)
            
            logger.info(
                f"Federation round {self.round_number} complete: "
                f"{len(updates)} updates, tokens={federation_round.tokens_distributed:.1f}"
            )
            
            return global_model
            
        except Exception as e:
            logger.error(f"Federation round failed: {str(e)}", exc_info=True)
            federation_round.successful = False
            
            # BIO-INSPIRED: Penalize failed participants
            if self.enable_bio_integration:
                for participant_id in selected:
                    self._pump_trust_gradient(participant_id, success=False, contribution=0.0)
            
            return None
    
    async def _select_participants_bio_aware(
        self, carbon_zone: int, helium_scarcity: float
    ) -> List[str]:
        """Select participants with bio-inspired criteria"""
        scored_participants = []
        
        for participant_id, participant in self.participants.items():
            if not participant.is_active:
                continue
            
            data_score = participant._calculate_data_quality()
            carbon_score = 1.0 / (1.0 + participant.carbon_footprint * 100)
            helium_score = 1.0 / (1.0 + participant.helium_usage * 10)
            network_score = min(participant.capabilities.network_bandwidth_mbps / 1000, 1.0)
            reliability = 0.8
            
            # BIO-INSPIRED: Gradient alignment score
            gradient_score = participant.gradient_alignment
            
            # BIO-INSPIRED: Token efficiency score
            token_score = participant.capabilities.token_efficiency
            
            renewable_bonus = 1.5 if participant.capabilities.energy_source_renewable else 1.0
            
            if carbon_zone >= 8:
                weights = {'data': 0.20, 'carbon': 0.25, 'helium': 0.10,
                          'network': 0.10, 'reliability': 0.10, 'gradient': 0.15, 'token': 0.10}
            elif helium_scarcity > 0.7:
                weights = {'data': 0.20, 'helium': 0.30, 'carbon': 0.10,
                          'network': 0.10, 'reliability': 0.10, 'gradient': 0.10, 'token': 0.10}
            else:
                weights = {'data': 0.25, 'carbon': 0.15, 'helium': 0.10,
                          'network': 0.15, 'reliability': 0.10, 'gradient': 0.15, 'token': 0.10}
            
            score = (
                weights['data'] * data_score +
                weights['carbon'] * carbon_score +
                weights['helium'] * helium_score +
                weights['network'] * network_score +
                weights['reliability'] * reliability +
                weights['gradient'] * gradient_score +
                weights['token'] * token_score
            ) * renewable_bonus
            
            scored_participants.append((participant_id, score))
        
        scored_participants.sort(key=lambda x: x[1], reverse=True)
        n_select = max(self.min_participants, min(len(scored_participants), int(len(scored_participants) * 0.7)))
        selected = [p[0] for p in scored_participants[:n_select]]
        
        return selected
    
    async def _collect_update(
        self, participant_id: str, epsilon: float
    ) -> Optional[SecureModelUpdate]:
        """Collect update with bio-inspired metadata"""
        if participant_id not in self.participants:
            return None
        
        participant = self.participants[participant_id]
        
        # Apply differential privacy
        private_update = self._apply_differential_privacy(participant.local_model, epsilon)
        
        # Reduce privacy budget
        participant.privacy_budget -= 0.1
        
        # Create secure update
        update = SecureModelUpdate(
            client_id=participant_id,
            round_number=self.round_number,
            encrypted_gradients=hashlib.sha256(str(private_update).encode()).digest(),
            encryption_metadata={'algorithm': 'AES-256-GCM'},
            proof_of_training=hashlib.sha256(f"proof_{participant_id}".encode()).digest(),
            signature=hashlib.sha256(f"sig_{participant_id}_{datetime.utcnow()}".encode()).digest(),
            timestamp=datetime.utcnow(),
            carbon_footprint_kg=participant.carbon_footprint,
            tokens_staked=participant.tokens_earned if self.enable_bio_integration else 0.0,
            gradient_level=participant.gradient_alignment if self.enable_bio_integration else 0.5,
            token_efficiency=participant.capabilities.token_efficiency if self.enable_bio_integration else 0.5
        )
        
        return update
    
    def _apply_differential_privacy(self, model: Dict[str, Any], epsilon: float) -> Dict[str, Any]:
        """Apply differential privacy with noise"""
        if epsilon <= 0:
            return model
        
        private_model = {}
        sensitivity = 1.0
        
        for key, value in model.items():
            if isinstance(value, (int, float)):
                scale = sensitivity / epsilon
                noise = np.random.laplace(0, scale)
                private_model[key] = value + noise
            elif isinstance(value, np.ndarray):
                scale = sensitivity / epsilon
                noise = np.random.laplace(0, scale, value.shape)
                private_model[key] = value + noise            else:
                private_model[key] = value
        
        return private_model
    
    def _aggregate_updates_bio_aware(
        self, updates: Dict[str, SecureModelUpdate], strategy: AggregationStrategy
    ) -> Dict[str, Any]:
        """Aggregate updates with bio-inspired weighting"""
        if not updates:
            return {}
        
        if strategy == AggregationStrategy.TOKEN_WEIGHTED:
            return self._token_weighted_aggregate(updates)
        elif strategy == AggregationStrategy.GRADIENT_ALIGNED:
            return self._gradient_aligned_aggregate(updates)
        elif strategy == AggregationStrategy.HEALTH_AWARE:
            return self._health_aware_aggregate(updates)
        else:
            return self._federated_averaging(updates)
    
    def _token_weighted_aggregate(self, updates: Dict[str, SecureModelUpdate]) -> Dict[str, Any]:
        """Aggregate updates weighted by token stakes"""
        aggregated = {}
        total_tokens = sum(u.tokens_staked for u in updates.values())
        
        if total_tokens == 0:
            return self._federated_averaging(updates)
        
        for key in self.global_model or {}:
            weighted_sum = 0.0
            for update in updates.values():
                weight = update.tokens_staked / total_tokens
                if key in self.global_model:
                    weighted_sum += self.global_model[key] * weight
            aggregated[key] = weighted_sum
        
        return aggregated if aggregated else self._federated_averaging(updates)
    
    def _gradient_aligned_aggregate(self, updates: Dict[str, SecureModelUpdate]) -> Dict[str, Any]:
        """Aggregate updates weighted by gradient alignment"""
        aggregated = {}
        total_alignment = sum(u.gradient_level for u in updates.values())
        
        if total_alignment == 0:
            return self._federated_averaging(updates)
        
        for key in self.global_model or {}:
            weighted_sum = 0.0
            for update in updates.values():
                weight = update.gradient_level / total_alignment
                if key in self.global_model:
                    weighted_sum += self.global_model[key] * weight
            aggregated[key] = weighted_sum
        
        return aggregated if aggregated else self._federated_averaging(updates)
    
    def _health_aware_aggregate(self, updates: Dict[str, SecureModelUpdate]) -> Dict[str, Any]:
        """Aggregate updates weighted by compartment health"""
        # This would use compartment_manager to get health scores
        return self._federated_averaging(updates)
    
    def _federated_averaging(self, updates: Dict[str, SecureModelUpdate]) -> Dict[str, Any]:
        """Standard federated averaging"""
        if not self.global_model:
            return {}
        return self.global_model
    
    # ========================================================================
    # Enhanced Statistics
    # ========================================================================
    
    def get_federation_status(self) -> Dict[str, Any]:
        """Get comprehensive federation status with bio-inspired data"""
        total_rounds = len(self.aggregation_history)
        successful_rounds = sum(1 for r in self.aggregation_history if r.successful)
        
        stats = {
            'total_participants': len(self.participants),
            'active_participants': sum(1 for p in self.participants.values() if p.is_active),
            'total_rounds': total_rounds,
            'successful_rounds': successful_rounds,
            'success_rate': successful_rounds / max(total_rounds, 1),
            'current_strategy': self.aggregation_strategy.value,
            'privacy_level': self.privacy_level.value,
            'total_carbon_emitted': sum(r.total_carbon_kg for r in self.aggregation_history),
            'total_helium_used': sum(r.total_helium_units for r in self.aggregation_history),
            'bio_integration_active': self.enable_bio_integration,
            'bio_modules_available': BIO_INSPIRED_AVAILABLE,
            'federation_token_pool': self.federation_token_pool,
            'total_tokens_distributed': sum(r.tokens_distributed for r in self.aggregation_history),
            'average_participants_per_round': np.mean([len(r.participants) for r in self.aggregation_history]) if self.aggregation_history else 0
        }
        
        # BIO-INSPIRED: Add gradient levels
        if self.enable_bio_integration:
            stats['gradient_levels'] = self._get_real_gradient_levels()
            stats['harvester_confidence'] = self._get_harvester_confidence()
            stats['atp_sync_timing'] = self._get_atp_driven_sync_timing()
            
            # Participant bio stats
            stats['participant_bio_stats'] = {
                pid: {
                    'tokens_earned': p.tokens_earned,
                    'gradient_alignment': p.gradient_alignment,
                    'token_efficiency': p.capabilities.token_efficiency,
                    'trust_pumping_count': p.trust_pumping_count
                }
                for pid, p in self.participants.items()
            }
        
        return stats
    
    def verify_audit_chain(self) -> bool:
        """Verify integrity of blockchain audit chain"""
        for i in range(1, len(self.audit_chain)):
            current = self.audit_chain[i]
            previous = self.audit_chain[i - 1]
            if current['previous_hash'] != previous['entry_hash']:
                return False
            computed = hashlib.sha256(
                json.dumps({k: v for k, v in current.items() if k != 'entry_hash'},
                          sort_keys=True, default=str).encode()
            ).hexdigest()
            if computed != current['entry_hash']:
                return False
        return True
    
    def get_participant_earnings(self, participant_id: str) -> Dict[str, float]:
        """Get total earnings for a participant"""
        if participant_id not in self.participants:
            return {}
        
        participant = self.participants[participant_id]
        
        return {
            'total_tokens_earned': participant.tokens_earned,
            'gradient_alignment': participant.gradient_alignment,
            'token_efficiency': participant.capabilities.token_efficiency,
            'reputation_score': participant.reputation_score,
            'trust_pumping_count': participant.trust_pumping_count,
            'privacy_budget_remaining': participant.privacy_budget
        }
