# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/moe_expert_system/expert_registry.py
# Enhanced with bio-inspired genetic encoding, species management, and evolutionary mechanisms

"""
Enhanced Expert Registry v4.0.0 - Genome Repository
Complete bio-inspired integration with genetic encoding, species taxonomy,
population genetics, and evolutionary tracking.

New Capabilities:
- Genetic encoding of expert profiles (DNA)
- Species taxonomy and population management
- Allele tracking and genetic diversity monitoring
- Inheritance rules for compartment spawning
- Natural selection through fitness scoring
- Gene flow tracking from federated imports
- Evolutionary fossil record via blockchain
- Phenotype validation and expression tracking
- Mutation tracking and adaptation monitoring
- Ecosystem health indicators
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple, Set
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import numpy as np
import hashlib
import json
import networkx as nx
from collections import defaultdict, deque
import uuid
import math

logger = logging.getLogger(__name__)

# ============================================================================
# Genetic Encoding Enums
# ============================================================================

class GeneExpression(Enum):
    """Gene expression states (maps to lifecycle)"""
    SILENCED = "silenced"          # Gene inactive
    ACTIVATING = "activating"      # Gene being turned on
    EXPRESSED = "expressed"        # Gene fully active
    OVEREXPRESSED = "overexpressed"  # Gene amplified
    DOWNREGULATED = "downregulated"  # Gene suppressed
    MUTATED = "mutated"            # Gene altered

class AlleleType(Enum):
    """Types of genetic alleles"""
    DOMINANT = "dominant"          # Always expressed
    RECESSIVE = "recessive"        # Only expressed when homozygous
    CODOMINANT = "codominant"      # Both alleles expressed
    EPISTATIC = "epistatic"        # Modifies other genes

class SpeciesStatus(Enum):
    """Species population status"""
    THRIVING = "thriving"          # Growing population
    STABLE = "stable"              # Equilibrium
    DECLINING = "declining"        # Shrinking population
    ENDANGERED = "endangered"      # Critical low population
    EXTINCT_IN_WILD = "extinct_wild"  # Only in registry
    EXTINCT = "extinct"            # No living instances

class MutationType(Enum):
    """Types of genetic mutations"""
    POINT = "point"                # Single parameter change
    INSERTION = "insertion"        # New capability added
    DELETION = "deletion"          # Capability removed
    DUPLICATION = "duplication"    # Capability copied
    INVERSION = "inversion"        # Capability reordered
    FRAMESHIFT = "frameshift"      # Major structural change

# ============================================================================
# Genetic Data Classes
# ============================================================================

@dataclass
class GeneticMarker:
    """Individual genetic marker (gene)"""
    gene_id: str
    gene_name: str
    trait: str                    # What this gene controls
    allele_type: AlleleType = AlleleType.CODOMINANT
    expression: GeneExpression = GeneExpression.EXPRESSED
    value: float = 0.5            # Expression level
    mutation_rate: float = 0.01   # Probability of mutation
    heritability: float = 0.8     # Probability of inheritance
    epigenetic_factors: Dict[str, float] = field(default_factory=dict)
    mutation_history: List[Dict] = field(default_factory=list)

@dataclass
class Genome:
    """Complete genetic sequence of an expert"""
    genome_id: str
    species: str                  # Expert type
    markers: Dict[str, GeneticMarker] = field(default_factory=dict)
    chromosome_count: int = 1
    ploidy: int = 2              # Diploid by default
    total_genes: int = 0
    gc_content: float = 0.5      # Gene density
    created_at: datetime = field(default_factory=datetime.utcnow)
    parent_genome: Optional[str] = None
    
    def add_gene(self, gene: GeneticMarker):
        """Add a gene to the genome"""
        self.markers[gene.gene_id] = gene
        self.total_genes = len(self.markers)
    
    def express_trait(self, gene_id: str) -> float:
        """Get expressed value of a gene"""
        if gene_id not in self.markers:
            return 0.5
        gene = self.markers[gene_id]
        if gene.expression in [GeneExpression.SILENCED, GeneExpression.DOWNREGULATED]:
            return gene.value * 0.3
        elif gene.expression == GeneExpression.OVEREXPRESSED:
            return gene.value * 1.5
        return gene.value
    
    def mutate(self, mutation_rate: float = 0.01) -> List[str]:
        """Introduce random mutations"""
        mutations = []
        for gene_id, gene in self.markers.items():
            if np.random.random() < mutation_rate * gene.mutation_rate:
                old_value = gene.value
                mutation_type = np.random.choice(list(MutationType))
                
                if mutation_type == MutationType.POINT:
                    gene.value += np.random.normal(0, 0.1)
                    gene.value = max(0.0, min(1.0, gene.value))
                
                gene.mutation_history.append({
                    'type': mutation_type.value,
                    'old_value': old_value,
                    'new_value': gene.value,
                    'timestamp': datetime.utcnow().isoformat()
                })
                mutations.append(gene_id)
        
        return mutations
    
    def recombine(self, other: 'Genome', crossover_rate: float = 0.5) -> 'Genome':
        """Sexual reproduction through genetic recombination"""
        child_genome = Genome(
            genome_id=f"genome_{uuid.uuid4().hex[:12]}",
            species=self.species,
            parent_genome=self.genome_id
        )
        
        all_genes = set(self.markers.keys()) | set(other.markers.keys())
        
        for gene_id in all_genes:
            if gene_id in self.markers and gene_id in other.markers:
                # Both parents have gene - crossover
                if np.random.random() < crossover_rate:
                    child_gene = copy.deepcopy(self.markers[gene_id])
                else:
                    child_gene = copy.deepcopy(other.markers[gene_id])
            elif gene_id in self.markers:
                child_gene = copy.deepcopy(self.markers[gene_id])
            else:
                child_gene = copy.deepcopy(other.markers[gene_id])
            
            child_genome.add_gene(child_gene)
        
        return child_genome

@dataclass
class Species:
    """Species taxonomy and population tracking"""
    species_id: str
    species_name: str
    genus: str = "Unknown"
    family: str = "Unknown"
    order: str = "Unknown"
    class_name: str = "Unknown"
    phylum: str = "MoE_Experts"
    kingdom: str = "GreenAgent"
    
    # Population genetics
    total_population: int = 0
    active_population: int = 0
    allele_frequencies: Dict[str, float] = field(default_factory=dict)
    genetic_diversity: float = 0.0
    status: SpeciesStatus = SpeciesStatus.STABLE
    
    # Evolutionary metrics
    generation_count: int = 0
    speciation_events: int = 0
    extinction_risk: float = 0.0
    
    # Ecosystem role
    trophic_level: int = 1  # 1=producer, 2=consumer, 3=decomposer
    keystone_species: bool = False
    invasive_potential: float = 0.0
    
    def calculate_genetic_diversity(self, genomes: List[Genome]) -> float:
        """Calculate genetic diversity using allele frequency variance"""
        if not genomes:
            return 0.0
        
        all_genes = set()
        for genome in genomes:
            all_genes.update(genome.markers.keys())
        
        if not all_genes:
            return 0.0
        
        diversity_scores = []
        for gene_id in all_genes:
            values = []
            for genome in genomes:
                if gene_id in genome.markers:
                    values.append(genome.markers[gene_id].value)
            
            if values:
                variance = np.var(values)
                diversity_scores.append(min(variance * 10, 1.0))
        
        self.genetic_diversity = np.mean(diversity_scores) if diversity_scores else 0.0
        return self.genetic_diversity
    
    def assess_extinction_risk(self) -> float:
        """Calculate extinction risk based on population and diversity"""
        if self.active_population == 0:
            return 1.0
        
        population_risk = 1.0 / (1.0 + self.active_population)
        diversity_risk = 1.0 - self.genetic_diversity
        
        self.extinction_risk = 0.6 * population_risk + 0.4 * diversity_risk
        
        # Update status
        if self.extinction_risk > 0.8:
            self.status = SpeciesStatus.ENDANGERED
        elif self.extinction_risk > 0.5:
            self.status = SpeciesStatus.DECLINING
        elif self.active_population == 0:
            self.status = SpeciesStatus.EXTINCT
        
        return self.extinction_risk

# ============================================================================
# Fitness Scoring System
# ============================================================================

@dataclass
class FitnessScore:
    """Multi-dimensional fitness scoring"""
    expert_id: str
    overall_fitness: float = 0.5
    survival_fitness: float = 0.5    # Can it survive?
    reproductive_fitness: float = 0.5  # Can it reproduce?
    ecological_fitness: float = 0.5   # Does it fit the ecosystem?
    
    # Component scores
    resource_efficiency: float = 0.5
    adaptation_speed: float = 0.5
    cooperation_score: float = 0.5
    competition_score: float = 0.5
    resilience_score: float = 0.5
    
    # Selection pressure
    selection_coefficient: float = 0.0  # Relative fitness advantage
    reproductive_success: int = 0       # Number of offspring
    
    def calculate_overall(self):
        """Calculate overall fitness from components"""
        self.overall_fitness = (
            self.resource_efficiency * 0.25 +
            self.adaptation_speed * 0.20 +
            self.cooperation_score * 0.15 +
            self.competition_score * 0.15 +
            self.resilience_score * 0.25
        )
        
        self.survival_fitness = (
            self.resource_efficiency * 0.4 +
            self.resilience_score * 0.6
        )
        
        self.reproductive_fitness = (
            self.adaptation_speed * 0.5 +
            self.competition_score * 0.5
        )
        
        self.ecological_fitness = (
            self.cooperation_score * 0.6 +
            self.resource_efficiency * 0.4
        )

# ============================================================================
# Enhanced Expert Registry with Bio-Inspired Features
# ============================================================================

class ExpertRegistry:
    """
    Enhanced Expert Registry v4.0.0 - Genome Repository
    
    Bio-inspired capabilities:
    - Genetic encoding of expert profiles
    - Species taxonomy and population management
    - Natural selection through fitness scoring
    - Inheritance rules for compartment spawning
    - Evolutionary fossil record
    - Ecosystem health monitoring
    """
    
    def __init__(
        self,
        registry_id: str = "default",
        enable_genetics: bool = True,
        enable_evolution: bool = True,
        enable_ecosystem: bool = True,
        enable_blockchain: bool = True,
        enable_marketplace: bool = True
    ):
        self.registry_id = registry_id
        
        # Feature flags
        self.enable_genetics = enable_genetics
        self.enable_evolution = enable_evolution
        self.enable_ecosystem = enable_ecosystem
        self.enable_blockchain = enable_blockchain
        self.enable_marketplace = enable_marketplace
        
        # Core storage
        self._experts: Dict[str, Any] = {}
        
        # Genetic storage
        self.genomes: Dict[str, Genome] = {}
        self.species_registry: Dict[str, Species] = {}
        
        # Fitness tracking
        self.fitness_scores: Dict[str, FitnessScore] = {}
        self.selection_pressure: float = 1.0
        
        # Population tracking
        self.compartment_populations: Dict[str, int] = defaultdict(int)
        self.total_compartments: int = 0
        
        # Evolutionary history
        self.evolutionary_events: deque = deque(maxlen=10000)
        self.speciation_events: List[Dict] = []
        self.extinction_events: List[Dict] = []
        
        # Inheritance rules
        self.inheritance_rules = {
            'base_heritability': 0.8,
            'mutation_rate': 0.01,
            'crossover_enabled': True,
            'epigenetic_inheritance': True
        }
        
        # Ecosystem metrics
        self.ecosystem_health: Dict[str, Any] = {}
        
        # Initialize species taxonomy
        self._initialize_species_taxonomy()
        
        logger.info(
            f"Enhanced Expert Registry v4.0.0 (Genome Repository) initialized: "
            f"genetics={enable_genetics}, evolution={enable_evolution}, "
            f"ecosystem={enable_ecosystem}"
        )
    
    def _initialize_species_taxonomy(self):
        """Initialize biological taxonomy for expert species"""
        species_definitions = {
            'energy': Species(
                species_id='energy_expert',
                species_name='Energy Expert',
                genus='Energy',
                family='Renewable_Harvesters',
                order='Energy_Producers',
                class_name='Metabolic_Experts',
                trophic_level=1,  # Primary producer
                keystone_species=True
            ),
            'data': Species(
                species_id='data_expert',
                species_name='Data Expert',
                genus='Data',
                family='Stream_Processors',
                order='Data_Processors',
                class_name='Metabolic_Experts',
                trophic_level=2  # Primary consumer
            ),
            'iot': Species(
                species_id='iot_expert',
                species_name='IoT Expert',
                genus='IoT',
                family='Edge_Decomposers',
                order='Edge_Processors',
                class_name='Metabolic_Experts',
                trophic_level=3  # Decomposer
            ),
            'quantum': Species(
                species_id='quantum_expert',
                species_name='Quantum Expert',
                genus='Quantum',
                family='Catalytic_Enzymes',
                order='Specialized_Catalysts',
                class_name='Metabolic_Experts',
                trophic_level=1
            ),
            'helium': Species(
                species_id='helium_expert',
                species_name='Helium Expert',
                genus='Helium',
                family='Homeostatic_Regulators',
                order='Resource_Regulators',
                class_name='Metabolic_Experts',
                trophic_level=1,
                keystone_species=True
            )
        }
        
        for species_id, species in species_definitions.items():
            self.species_registry[species_id] = species
    
    # ========================================================================
    # Genetic Registration
    # ========================================================================
    
    def register_expert(
        self,
        profile: Any,
        validate: bool = True,
        auto_certify: bool = False,
        parent_genome_id: Optional[str] = None,
        mutation_history: Optional[List[Dict]] = None
    ) -> Tuple[bool, str]:
        """
        Register expert with genetic encoding.
        
        Creates genome from profile and registers species.
        """
        expert_id = profile.expert_id
        
        if expert_id in self._experts:
            return False, f"Expert {expert_id} already registered"
        
        # Store expert
        self._experts[expert_id] = profile
        
        # Create genetic encoding
        if self.enable_genetics:
            genome = self._profile_to_genome(profile, parent_genome_id)
            self.genomes[expert_id] = genome
            
            # Apply mutations if any
            if mutation_history:
                for mutation in mutation_history:
                    if mutation['gene_id'] in genome.markers:
                        genome.markers[mutation['gene_id']].mutation_history.append(mutation)
            
            # Update species population
            species_id = self._get_species_id(profile)
            if species_id in self.species_registry:
                species = self.species_registry[species_id]
                species.total_population += 1
                species.active_population += 1
                species.generation_count += 1
                
                # Update genetic diversity
                species_genomes = [
                    g for gid, g in self.genomes.items()
                    if self._get_species_id(self._experts.get(gid)) == species_id
                ]
                species.calculate_genetic_diversity(species_genomes)
                species.assess_extinction_risk()
        
        # Fitness scoring
        if self.enable_evolution:
            fitness = self._calculate_initial_fitness(profile)
            self.fitness_scores[expert_id] = fitness
        
        # Record evolutionary event
        if self.enable_blockchain:
            self._record_evolutionary_event(
                'speciation' if not parent_genome_id else 'birth',
                expert_id,
                {'parent_genome': parent_genome_id}
            )
        
        logger.info(
            f"Registered expert {expert_id} with genome: "
            f"{len(genome.markers) if self.enable_genetics else 0} genes"
        )
        
        return True, f"Expert {expert_id} registered with genetic encoding"
    
    def _profile_to_genome(
        self,
        profile: Any,
        parent_genome_id: Optional[str] = None
    ) -> Genome:
        """Convert expert profile to genetic sequence"""
        species = self._get_species_id(profile)
        
        genome = Genome(
            genome_id=f"genome_{profile.expert_id}",
            species=species,
            parent_genome=parent_genome_id
        )
        
        # Metabolic genes
        genome.add_gene(GeneticMarker(
            gene_id=f"{profile.expert_id}_helium_metabolism",
            gene_name="Helium Metabolism",
            trait="helium_per_inference",
            value=min(1.0, 1.0 / (1.0 + getattr(profile, 'helium_per_inference', 0.01) * 100)),
            heritability=0.9
        ))
        
        genome.add_gene(GeneticMarker(
            gene_id=f"{profile.expert_id}_carbon_fixation",
            gene_name="Carbon Fixation Efficiency",
            trait="carbon_per_inference",
            value=min(1.0, 1.0 / (1.0 + getattr(profile, 'carbon_per_inference', 0.0001) * 10000)),
            heritability=0.85
        ))
        
        genome.add_gene(GeneticMarker(
            gene_id=f"{profile.expert_id}_energy_metabolism",
            gene_name="Energy Metabolism Rate",
            trait="energy_per_inference",
            value=min(1.0, 1.0 / (1.0 + getattr(profile, 'energy_per_inference', 0.001) * 1000)),
            heritability=0.8
        ))
        
        # Fitness genes
        genome.add_gene(GeneticMarker(
            gene_id=f"{profile.expert_id}_accuracy",
            gene_name="Functional Accuracy",
            trait="accuracy_score",
            value=getattr(profile, 'accuracy_score', 0.9),
            heritability=0.7
        ))
        
        genome.add_gene(GeneticMarker(
            gene_id=f"{profile.expert_id}_reliability",
            gene_name="Operational Reliability",
            trait="reliability_score",
            value=getattr(profile, 'reliability_score', 0.95),
            heritability=0.75
        ))
        
        genome.add_gene(GeneticMarker(
            gene_id=f"{profile.expert_id}_efficiency",
            gene_name="Resource Efficiency",
            trait="efficiency_score",
            value=getattr(profile, 'efficiency_score', 0.9),
            heritability=0.8
        ))
        
        # Regulatory genes
        genome.add_gene(GeneticMarker(
            gene_id=f"{profile.expert_id}_carbon_tolerance",
            gene_name="Carbon Zone Tolerance",
            trait="min_carbon_zone",
            value=1.0 - (getattr(profile, 'min_carbon_zone', 0) / 15.0),
            heritability=0.6
        ))
        
        genome.add_gene(GeneticMarker(
            gene_id=f"{profile.expert_id}_helium_tolerance",
            gene_name="Helium Scarcity Tolerance",
            trait="max_helium_scarcity",
            value=getattr(profile, 'max_helium_scarcity', 1.0),
            heritability=0.6
        ))
        
        # Adaptation genes
        genome.add_gene(GeneticMarker(
            gene_id=f"{profile.expert_id}_response_time",
            gene_name="Response Time",
            trait="avg_latency_ms",
            value=min(1.0, 1.0 / (1.0 + getattr(profile, 'avg_latency_ms', 50.0) / 100)),
            heritability=0.5,
            mutation_rate=0.02  # Higher mutation rate for adaptation
        ))
        
        return genome
    
    def _get_species_id(self, profile: Any) -> str:
        """Get species ID from profile"""
        if hasattr(profile, 'domain'):
            domain = profile.domain.value if hasattr(profile.domain, 'value') else str(profile.domain)
            if 'energy' in domain.lower():
                return 'energy'
            elif 'data' in domain.lower():
                return 'data'
            elif 'iot' in domain.lower():
                return 'iot'
            elif 'quantum' in domain.lower():
                return 'quantum'
            elif 'helium' in domain.lower():
                return 'helium'
        return 'general'
    
    # ========================================================================
    # Fitness and Natural Selection
    # ========================================================================
    
    def _calculate_initial_fitness(self, profile: Any) -> FitnessScore:
        """Calculate initial fitness score for new expert"""
        fitness = FitnessScore(expert_id=profile.expert_id)
        
        fitness.resource_efficiency = min(1.0, 
            1.0 / (1.0 + getattr(profile, 'carbon_per_inference', 0.0001) * 10000)
        )
        fitness.adaptation_speed = 0.5  # Start neutral
        fitness.cooperation_score = 0.5
        fitness.competition_score = 0.5
        fitness.resilience_score = getattr(profile, 'reliability_score', 0.95)
        
        fitness.calculate_overall()
        
        return fitness
    
    def update_fitness(
        self,
        expert_id: str,
        performance_metrics: Dict[str, float]
    ):
        """Update fitness based on observed performance"""
        if expert_id not in self.fitness_scores:
            return
        
        fitness = self.fitness_scores[expert_id]
        alpha = 0.1  # Learning rate
        
        if 'success_rate' in performance_metrics:
            fitness.resilience_score = (
                fitness.resilience_score * (1 - alpha) +
                performance_metrics['success_rate'] * alpha
            )
        
        if 'carbon_efficiency' in performance_metrics:
            fitness.resource_efficiency = (
                fitness.resource_efficiency * (1 - alpha) +
                performance_metrics['carbon_efficiency'] * alpha
            )
        
        if 'adaptation_speed' in performance_metrics:
            fitness.adaptation_speed = (
                fitness.adaptation_speed * (1 - alpha) +
                performance_metrics['adaptation_speed'] * alpha
            )
        
        fitness.calculate_overall()
        
        # Update selection coefficient
        avg_fitness = np.mean([f.overall_fitness for f in self.fitness_scores.values()])
        if avg_fitness > 0:
            fitness.selection_coefficient = (fitness.overall_fitness - avg_fitness) / avg_fitness
    
    def natural_selection(self):
        """
        Apply natural selection pressure.
        
        Experts with low fitness are deprecated (die).
        Experts with high fitness are encouraged to reproduce.
        """
        if not self.enable_evolution:
            return
        
        # Calculate fitness threshold (bottom 20%)
        fitnesses = [f.overall_fitness for f in self.fitness_scores.values()]
        if not fitnesses:
            return
        
        threshold = np.percentile(fitnesses, 20)
        
        # Deprecate low-fitness experts
        deprecated = []
        for expert_id, fitness in self.fitness_scores.items():
            if fitness.overall_fitness < threshold and fitness.reproductive_success == 0:
                if expert_id in self._experts:
                    self.deprecate_expert(expert_id, reason="natural_selection")
                    deprecated.append(expert_id)
        
        # Identify high-fitness experts for reproduction
        top_threshold = np.percentile(fitnesses, 80)
        reproducers = [
            eid for eid, f in self.fitness_scores.items()
            if f.overall_fitness > top_threshold and f.reproductive_success < 5
        ]
        
        if deprecated:
            logger.info(
                f"Natural selection: {len(deprecated)} deprecated, "
                f"{len(reproducers)} selected for reproduction"
            )
            
            self._record_evolutionary_event(
                'selection',
                'population',
                {'deprecated': len(deprecated), 'reproducers': len(reproducers)}
            )
    
    # ========================================================================
    # Reproduction and Inheritance
    # ========================================================================
    
    def reproduce_expert(
        self,
        parent_id: str,
        mutation_rate: Optional[float] = None
    ) -> Optional[str]:
        """
        Create offspring through genetic reproduction.
        
        Args:
            parent_id: Parent expert ID
            mutation_rate: Override default mutation rate
            
        Returns:
            Child expert ID if successful
        """
        if not self.enable_genetics or parent_id not in self.genomes:
            return None
        
        parent_genome = self.genomes[parent_id]
        parent_profile = self._experts.get(parent_id)
        
        if not parent_profile:
            return None
        
        # Check if parent is fit to reproduce
        if parent_id in self.fitness_scores:
            fitness = self.fitness_scores[parent_id]
            if fitness.reproductive_success >= 5:
                return None  # Limit offspring count
        
        # Create child genome through mutation
        child_genome = copy.deepcopy(parent_genome)
        child_genome.genome_id = f"genome_{uuid.uuid4().hex[:12]}"
        child_genome.parent_genome = parent_genome.genome_id
        child_genome.created_at = datetime.utcnow()
        
        # Apply mutations
        mut_rate = mutation_rate or self.inheritance_rules['mutation_rate']
        mutations = child_genome.mutate(mut_rate)
        
        # Create child profile
        import copy
        child_profile = copy.deepcopy(parent_profile)
        child_profile.expert_id = f"{parent_id}_offspring_{child_genome.genome_id[:8]}"
        
        if hasattr(child_profile, 'version'):
            current = child_profile.version
            child_profile.version = type(current)(
                major=current.major,
                minor=current.minor + 1,
                patch=0
            )
        
        # Apply genetic expression to profile
        for gene_id, gene in child_genome.markers.items():
            trait = gene.trait
            if hasattr(child_profile, trait):
                current_val = getattr(child_profile, trait)
                # Blend parent value with genetic expression
                new_val = current_val * (1 - gene.heritability) + gene.value * gene.heritability
                setattr(child_profile, trait, new_val)
        
        # Register child
        success, message = self.register_expert(
            child_profile,
            parent_genome_id=parent_genome.genome_id,
            mutation_history=[
                {'gene_id': gid, 'type': 'point', 'timestamp': datetime.utcnow().isoformat()}
                for gid in mutations
            ] if mutations else None
        )
        
        if success:
            # Update parent reproductive success
            if parent_id in self.fitness_scores:
                self.fitness_scores[parent_id].reproductive_success += 1
            
            # Update species
            species_id = self._get_species_id(parent_profile)
            if species_id in self.species_registry:
                self.species_registry[species_id].generation_count += 1
            
            logger.info(
                f"Reproduction: {parent_id} → {child_profile.expert_id} "
                f"({len(mutations)} mutations)"
            )
            
            return child_profile.expert_id
        
        return None
    
    def sexual_reproduction(
        self,
        parent1_id: str,
        parent2_id: str
    ) -> Optional[str]:
        """
        Create offspring through sexual reproduction (crossover).
        
        Combines genetic material from two parents.
        """
        if not self.enable_genetics:
            return None
        
        if parent1_id not in self.genomes or parent2_id not in self.genomes:
            return None
        
        # Check same species (can interbreed)
        genome1 = self.genomes[parent1_id]
        genome2 = self.genomes[parent2_id]
        
        if genome1.species != genome2.species:
            # Different species - hybrid possible but rare
            if np.random.random() > 0.1:  # 10% chance of hybrid
                return None
        
        # Genetic recombination
        child_genome = genome1.recombine(genome2)
        
        # Create hybrid profile
        parent1_profile = self._experts.get(parent1_id)
        parent2_profile = self._experts.get(parent2_id)
        
        if not parent1_profile or not parent2_profile:
            return None
        
        import copy
        child_profile = copy.deepcopy(parent1_profile)
        child_profile.expert_id = f"hybrid_{genome1.species}_{child_genome.genome_id[:8]}"
        
        # Blend traits from both parents
        for trait in ['accuracy_score', 'reliability_score', 'efficiency_score']:
            if hasattr(child_profile, trait) and hasattr(parent2_profile, trait):
                blended = (
                    getattr(parent1_profile, trait) * 0.5 +
                    getattr(parent2_profile, trait) * 0.5
                )
                setattr(child_profile, trait, blended)
        
        # Register hybrid
        success, message = self.register_expert(
            child_profile,
            parent_genome_id=f"{genome1.genome_id}+{genome2.genome_id}"
        )
        
        if success:
            logger.info(
                f"Hybrid created: {parent1_id} × {parent2_id} → {child_profile.expert_id}"
            )
            return child_profile.expert_id
        
        return None
    
    # ========================================================================
    # Ecosystem Health Monitoring
    # ========================================================================
    
    def assess_ecosystem_health(self) -> Dict[str, Any]:
        """Assess overall ecosystem health"""
        if not self.enable_ecosystem:
            return {}
        
        # Species richness
        active_species = [
            s for s in self.species_registry.values()
            if s.active_population > 0
        ]
        
        # Genetic diversity
        total_diversity = np.mean([
            s.genetic_diversity for s in self.species_registry.values()
        ])
        
        # Population stability
        populations = [s.active_population for s in self.species_registry.values()]
        population_stability = 1.0 - (np.std(populations) / max(np.mean(populations), 1))
        
        # Extinction risk
        endangered_count = sum(
            1 for s in self.species_registry.values()
            if s.status in [SpeciesStatus.ENDANGERED, SpeciesStatus.DECLINING]
        )
        
        # Keystone species health
        keystone_health = np.mean([
            s.active_population / max(s.total_population, 1)
            for s in self.species_registry.values()
            if s.keystone_species
        ])
        
        health_score = (
            (len(active_species) / max(len(self.species_registry), 1)) * 0.3 +
            total_diversity * 0.25 +
            population_stability * 0.2 +
            (1 - endangered_count / max(len(self.species_registry), 1)) * 0.15 +
            keystone_health * 0.1
        )
        
        self.ecosystem_health = {
            'health_score': health_score,
            'status': 'healthy' if health_score > 0.7 else 'degraded' if health_score > 0.4 else 'critical',
            'species_richness': len(active_species),
            'total_species': len(self.species_registry),
            'genetic_diversity': total_diversity,
            'population_stability': population_stability,
            'endangered_species': endangered_count,
            'keystone_health': keystone_health,
            'trophic_structure': {
                'producers': sum(1 for s in self.species_registry.values() if s.trophic_level == 1 and s.active_population > 0),
                'consumers': sum(1 for s in self.species_registry.values() if s.trophic_level == 2 and s.active_population > 0),
                'decomposers': sum(1 for s in self.species_registry.values() if s.trophic_level == 3 and s.active_population > 0)
            }
        }
        
        return self.ecosystem_health
    
    def update_compartment_population(
        self,
        species_id: str,
        active_count: int,
        total_count: int
    ):
        """Update compartment population counts"""
        if species_id in self.species_registry:
            species = self.species_registry[species_id]
            species.active_population = active_count
            species.total_population = total_count
            species.assess_extinction_risk()
        
        self.compartment_populations[species_id] = active_count
        self.total_compartments = sum(self.compartment_populations.values())
    
    # ========================================================================
    # Evolutionary Event Recording
    # ========================================================================
    
    def _record_evolutionary_event(
        self,
        event_type: str,
        entity_id: str,
        details: Dict[str, Any]
    ):
        """Record evolutionary event for fossil record"""
        event = {
            'event_id': f"evo_{datetime.utcnow().timestamp()}_{uuid.uuid4().hex[:6]}",
            'event_type': event_type,
            'entity_id': entity_id,
            'details': details,
            'timestamp': datetime.utcnow().isoformat(),
            'generation': len(self.evolutionary_events) + 1
        }
        
        self.evolutionary_events.append(event)
        
        if event_type == 'speciation':
            self.speciation_events.append(event)
        elif event_type == 'extinction':
            self.extinction_events.append(event)
    
    # ========================================================================
    # Enhanced Statistics
    # ========================================================================
    
    def get_registry_stats(self) -> Dict[str, Any]:
        """Get enhanced registry statistics with bio-inspired metrics"""
        stats = {
            'registry_id': self.registry_id,
            'total_experts': len(self._experts),
            'active_experts': len([
                e for e in self._experts.values()
                if hasattr(e, 'lifecycle_state') and e.lifecycle_state.is_available()
            ])
        }
        
        # Genetic stats
        if self.enable_genetics:
            stats['genetics'] = {
                'total_genomes': len(self.genomes),
                'total_genes': sum(g.total_genes for g in self.genomes.values()),
                'average_genes_per_genome': np.mean([g.total_genes for g in self.genomes.values()]) if self.genomes else 0
            }
        
        # Species stats
        if self.enable_ecosystem:
            stats['species'] = {
                species_id: {
                    'population': s.active_population,
                    'total_population': s.total_population,
                    'status': s.status.value,
                    'genetic_diversity': s.genetic_diversity,
                    'extinction_risk': s.extinction_risk,
                    'generation': s.generation_count,
                    'trophic_level': s.trophic_level,
                    'keystone': s.keystone_species
                }
                for species_id, s in self.species_registry.items()
            }
            stats['ecosystem_health'] = self.ecosystem_health
        
        # Evolution stats
        if self.enable_evolution:
            stats['evolution'] = {
                'total_evolutionary_events': len(self.evolutionary_events),
                'speciation_events': len(self.speciation_events),
                'extinction_events': len(self.extinction_events),
                'selection_pressure': self.selection_pressure,
                'average_fitness': np.mean([f.overall_fitness for f in self.fitness_scores.values()]) if self.fitness_scores else 0,
                'top_fitness': max([f.overall_fitness for f in self.fitness_scores.values()]) if self.fitness_scores else 0
            }
        
        return stats
    
    def get_species_report(self, species_id: str) -> Dict[str, Any]:
        """Get detailed species report"""
        if species_id not in self.species_registry:
            return {}
        
        species = self.species_registry[species_id]
        
        # Get all genomes for this species
        species_genomes = [
            g for gid, g in self.genomes.items()
            if self._get_species_id(self._experts.get(gid)) == species_id
        ]
        
        return {
            'taxonomy': {
                'species': species.species_name,
                'genus': species.genus,
                'family': species.family,
                'order': species.order,
                'class': species.class_name
            },
            'population': {
                'active': species.active_population,
                'total': species.total_population,
                'status': species.status.value,
                'extinction_risk': species.extinction_risk
            },
            'genetics': {
                'diversity': species.genetic_diversity,
                'genomes_sequenced': len(species_genomes),
                'generation': species.generation_count
            },
            'ecology': {
                'trophic_level': species.trophic_level,
                'keystone': species.keystone_species,
                'invasive_potential': species.invasive_potential
            },
            'evolution': {
                'speciation_events': species.speciation_events,
                'fitness_trend': self._get_fitness_trend(species_id)
            }
        }
    
    def _get_fitness_trend(self, species_id: str) -> str:
        """Get fitness trend for species"""
        species_experts = [
            eid for eid, e in self._experts.items()
            if self._get_species_id(e) == species_id
        ]
        
        if len(species_experts) < 2:
            return 'stable'
        
        recent_fitness = [
            self.fitness_scores[eid].overall_fitness
            for eid in species_experts[-5:]
            if eid in self.fitness_scores
        ]
        
        if len(recent_fitness) < 2:
            return 'stable'
        
        trend = np.polyfit(range(len(recent_fitness)), recent_fitness, 1)[0]
        
        if trend > 0.01:
            return 'improving'
        elif trend < -0.01:
            return 'declining'
        return 'stable'
    
    def trigger_natural_selection(self):
        """Manually trigger natural selection"""
        if self.enable_evolution:
            self.natural_selection()
            self.assess_ecosystem_health()
    
    def get_reproduction_candidates(
        self,
        min_fitness: float = 0.7,
        max_offspring: int = 5
    ) -> List[str]:
        """Get experts eligible for reproduction"""
        candidates = []
        
        for expert_id, fitness in self.fitness_scores.items():
            if (fitness.overall_fitness >= min_fitness and
                fitness.reproductive_success < max_offspring and
                expert_id in self._experts):
                candidates.append(expert_id)
        
        # Sort by fitness
        candidates.sort(
            key=lambda eid: self.fitness_scores[eid].overall_fitness,
            reverse=True
        )
        
        return candidates
