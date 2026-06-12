# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/carbon_nas.py
# Enhanced with expert_registry.py auto-registration

"""
Enhanced Carbon-Aware Neural Architecture Search
Version: 2.0.0

Now integrates with expert_registry.py for:
- Automatic registration of discovered architectures
- Version lineage tracking
- Certification of NAS-discovered experts
- Carbon-efficient architecture deployment
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime
import numpy as np
import copy
import hashlib
import json

logger = logging.getLogger(__name__)

# ============================================================================
# Registry Bridge for Auto-Registration
# ============================================================================

class RegistryBridge:
    """
    Bridge between Carbon NAS and Expert Registry.
    
    Enables automatic registration of NAS-discovered architectures.
    """
    
    def __init__(self):
        self.registry = None  # Will be injected
        self.registered_architectures: Dict[str, str] = {}  # arch_hash -> expert_id
        self.registration_history: List[Dict] = []
        
        logger.info("RegistryBridge initialized for NAS-Registry integration")
    
    def inject_registry(self, registry: Any):
        """Inject expert registry"""
        self.registry = registry
        logger.info("Expert registry injected into NAS bridge")
    
    def register_architecture(
        self,
        architecture: Dict[str, Any],
        performance_metrics: Dict[str, Any],
        carbon_footprint_kg: float,
        version: str = "1.0.0"
    ) -> Optional[str]:
        """
        Register discovered architecture as expert.
        
        Returns expert_id if successful, None otherwise.
        """
        if not self.registry:
            logger.warning("No registry available for architecture registration")
            return None
        
        # Generate unique architecture hash
        arch_hash = self._compute_architecture_hash(architecture)
        
        # Check if already registered
        if arch_hash in self.registered_architectures:
            existing_id = self.registered_architectures[arch_hash]
            logger.info(f"Architecture already registered as {existing_id}")
            return existing_id
        
        try:
            # Create expert profile
            from enhancements.moe_expert_system.expert_registry import (
                ExpertProfile, ExpertDomain, ExpertLifecycleState,
                ExpertVersion, ExpertLineage, HardwareProfile
            )
            
            expert_id = f"nas_expert_{arch_hash[:12]}"
            
            profile = ExpertProfile(
                expert_id=expert_id,
                expert_name=f"NAS-Discovered-{architecture.get('type', 'unknown')}",
                version=ExpertVersion.from_string(version),
                domain=ExpertDomain.GENERAL,
                hardware_profile=HardwareProfile.HYBRID,
                lifecycle_state=ExpertLifecycleState.REGISTERED,
                
                # Resource metrics from NAS evaluation
                carbon_per_inference=performance_metrics.get('carbon_kg', 0.0001),
                energy_per_inference=performance_metrics.get('energy_kwh', 0.001),
                avg_latency_ms=performance_metrics.get('latency_ms', 50.0),
                
                # Performance scores
                accuracy_score=performance_metrics.get('accuracy', 0.9),
                efficiency_score=performance_metrics.get('efficiency', 0.9),
                reliability_score=0.95,
                
                # NAS-specific metadata
                tags=['nas_discovered', architecture.get('type', 'unknown')],
                capabilities=['auto_discovered', 'carbon_optimized'],
                
                # Lineage tracking
                lineage=ExpertLineage(
                    lineage_id=f"nas_{arch_hash[:16]}",
                    created_from='nas',
                    training_carbon_kg=carbon_footprint_kg,
                    model_architecture=json.dumps(architecture),
                    hyperparameters=architecture.get('hyperparameters', {})
                )
            )
            
            # Register with registry
            success, message = self.registry.register_expert(
                profile, validate=True, auto_certify=True
            )
            
            if success:
                # Activate the expert
                self.registry.activate_expert(expert_id)
                
                # Track registration
                self.registered_architectures[arch_hash] = expert_id
                self.registration_history.append({
                    'architecture_hash': arch_hash,
                    'expert_id': expert_id,
                    'timestamp': datetime.utcnow().isoformat(),
                    'carbon_footprint_kg': carbon_footprint_kg,
                    'accuracy': performance_metrics.get('accuracy', 0)
                })
                
                logger.info(
                    f"Architecture registered as expert {expert_id}: "
                    f"accuracy={performance_metrics.get('accuracy', 0):.3f}, "
                    f"carbon={carbon_footprint_kg:.6f}kg"
                )
                
                return expert_id
            else:
                logger.warning(f"Failed to register architecture: {message}")
                return None
                
        except Exception as e:
            logger.error(f"Architecture registration error: {str(e)}")
            return None
    
    def update_architecture_version(
        self,
        expert_id: str,
        improved_architecture: Dict[str, Any],
        performance_metrics: Dict[str, Any],
        new_version: str
    ) -> bool:
        """Register improved version of existing architecture"""
        if not self.registry:
            return False
        
        try:
            # Get existing expert
            existing = self.registry.get_expert(expert_id)
            if not existing:
                return False
            
            # Create new version
            from enhancements.moe_expert_system.expert_registry import (
                ExpertProfile, ExpertVersion
            )
            
            new_profile = copy.deepcopy(existing)
            new_profile.version = ExpertVersion.from_string(new_version)
            new_profile.accuracy_score = performance_metrics.get('accuracy', existing.accuracy_score)
            new_profile.carbon_per_inference = performance_metrics.get('carbon_kg', existing.carbon_per_inference)
            new_profile.replaces_expert = expert_id
            
            # Register new version
            success, _ = self.registry.register_expert(new_profile, validate=True)
            
            if success:
                # Deprecate old version
                self.registry.deprecate_expert(expert_id, replacement_id=new_profile.expert_id)
                
                logger.info(
                    f"Updated {expert_id} to v{new_version}: "
                    f"accuracy={performance_metrics.get('accuracy', 0):.3f}"
                )
                
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Version update error: {str(e)}")
            return False
    
    def _compute_architecture_hash(self, architecture: Dict[str, Any]) -> str:
        """Compute unique hash for architecture"""
        arch_str = json.dumps(architecture, sort_keys=True)
        return hashlib.sha256(arch_str.encode()).hexdigest()
    
    def get_registered_architectures(self) -> List[Dict]:
        """Get all NAS-registered architectures"""
        return self.registration_history.copy()
    
    def get_architecture_expert_id(self, architecture: Dict[str, Any]) -> Optional[str]:
        """Get expert ID for a given architecture"""
        arch_hash = self._compute_architecture_hash(architecture)
        return self.registered_architectures.get(arch_hash)


# ============================================================================
# Enhanced Carbon NAS
# ============================================================================

@dataclass
class ArchitectureGene:
    """Enhanced architecture gene with registry awareness"""
    architecture: Dict[str, Any]
    fitness: float = 0.0
    carbon_cost_kg: float = 0.0
    accuracy: float = 0.0
    registered_expert_id: Optional[str] = None
    version: str = "1.0.0"
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'architecture': self.architecture,
            'fitness': self.fitness,
            'carbon_cost_kg': self.carbon_cost_kg,
            'accuracy': self.accuracy,
            'registered_expert_id': self.registered_expert_id,
            'version': self.version
        }

class EnhancedCarbonNAS:
    """
    Enhanced Carbon-Aware Neural Architecture Search.
    
    Features:
    - Automatic expert registration of discovered architectures
    - Version lineage tracking
    - Carbon-budget-aware search
    - Registry-aware fitness evaluation
    - Incremental architecture improvement
    """
    
    def __init__(
        self,
        expert_registry: Optional[Any] = None,
        population_size: int = 20,
        mutation_rate: float = 0.1,
        carbon_budget_kg: float = 1.0,
        auto_register: bool = True,
        min_accuracy_threshold: float = 0.85
    ):
        self.population_size = population_size
        self.mutation_rate = mutation_rate
        self.carbon_budget_kg = carbon_budget_kg
        self.auto_register = auto_register
        self.min_accuracy_threshold = min_accuracy_threshold
        
        # Registry bridge for auto-registration
        self.registry_bridge = RegistryBridge()
        if expert_registry:
            self.registry_bridge.inject_registry(expert_registry)
        
        # Population
        self.population: List[ArchitectureGene] = []
        self.generation = 0
        
        # Evolution history
        self.evolution_history: List[Dict] = []
        
        # Carbon tracking        self.total_carbon_spent_kg = 0.0
        
        # Initialize population
        self._initialize_population()
        
        logger.info(
            f"Enhanced Carbon NAS initialized: "
            f"population={population_size}, auto_register={auto_register}"
        )
    
    def _initialize_population(self):
        """Initialize population with diverse architectures"""
        architectures = [
            {'type': 'transformer', 'layers': 6, 'hidden': 512, 'heads': 8},
            {'type': 'transformer', 'layers': 4, 'hidden': 384, 'heads': 6},
            {'type': 'transformer', 'layers': 8, 'hidden': 640, 'heads': 10},
            {'type': 'cnn', 'layers': 12, 'filters': [64, 128, 256]},
            {'type': 'cnn', 'layers': 8, 'filters': [32, 64, 128]},
            {'type': 'mlp', 'layers': 5, 'hidden': [512, 256, 128]},
            {'type': 'mlp', 'layers': 3, 'hidden': [256, 128]},
            {'type': 'hybrid', 'cnn_layers': 4, 'transformer_layers': 3},
            {'type': 'efficient', 'layers': 6, 'hidden': 256, 'pruned': True},
            {'type': 'efficient', 'layers': 4, 'hidden': 192, 'quantized': True},
        ]
        
        for arch in architectures[:self.population_size]:
            gene = ArchitectureGene(architecture=arch)
            self.population.append(gene)
        
        # Fill remaining with mutations
        while len(self.population) < self.population_size:
            base = np.random.choice(architectures)
            mutated = self._mutate_architecture(base)
            gene = ArchitectureGene(architecture=mutated)
            self.population.append(gene)
        
        logger.info(f"Initialized population of {len(self.population)} architectures")
    
    def _mutate_architecture(self, architecture: Dict[str, Any]) -> Dict[str, Any]:
        """Mutate architecture parameters"""
        mutated = copy.deepcopy(architecture)
        
        if 'layers' in mutated and np.random.random() < self.mutation_rate:
            mutated['layers'] = max(1, mutated['layers'] + np.random.randint(-2, 3))
        
        if 'hidden' in mutated and np.random.random() < self.mutation_rate:
            mutated['hidden'] = mutated['hidden'] + np.random.choice([-128, -64, 64, 128])
            mutated['hidden'] = max(64, min(1024, mutated['hidden']))
        
        if 'heads' in mutated and np.random.random() < self.mutation_rate:
            mutated['heads'] = max(2, mutated['heads'] + np.random.choice([-2, 2, 4]))
        
        return mutated
    
    async def evolve_generation(
        self,
        fitness_function: Callable,
        auto_register_best: bool = True
    ) -> Dict[str, Any]:
        """
        Evolve one generation and auto-register best architectures.
        
        Args:
            fitness_function: Function to evaluate architecture fitness
            auto_register_best: Automatically register best architecture
            
        Returns:
            Generation metrics
        """
        self.generation += 1
        
        # Evaluate fitness
        for gene in self.population:
            try:
                fitness_result = await fitness_function(gene.architecture)
                gene.fitness = fitness_result.get('fitness', 0.0)
                gene.carbon_cost_kg = fitness_result.get('carbon_kg', 0.001)
                gene.accuracy = fitness_result.get('accuracy', 0.5)
                
                # Track carbon
                self.total_carbon_spent_kg += gene.carbon_cost_kg
                
            except Exception as e:
                logger.error(f"Fitness evaluation error: {str(e)}")
                gene.fitness = 0.0
        
        # Sort by fitness
        self.population.sort(key=lambda g: g.fitness, reverse=True)
        
        # Auto-register best architecture
        best_gene = self.population[0]
        
        if (
            self.auto_register and
            auto_register_best and
            best_gene.accuracy >= self.min_accuracy_threshold and
            best_gene.fitness > 0
        ):
            # Check if already registered
            if not best_gene.registered_expert_id:
                performance_metrics = {
                    'accuracy': best_gene.accuracy,
                    'carbon_kg': best_gene.carbon_cost_kg,
                    'energy_kwh': best_gene.carbon_cost_kg * 2.5,
                    'latency_ms': 100.0 / best_gene.accuracy,
                    'efficiency': best_gene.fitness
                }
                
                expert_id = self.registry_bridge.register_architecture(
                    architecture=best_gene.architecture,
                    performance_metrics=performance_metrics,
                    carbon_footprint_kg=best_gene.carbon_cost_kg,
                    version=f"1.{self.generation}.0"
                )
                
                if expert_id:
                    best_gene.registered_expert_id = expert_id
                    logger.info(
                        f"Auto-registered best architecture as expert {expert_id}"
                    )
        
        # Evolution metrics
        fitnesses = [g.fitness for g in self.population]
        metrics = {
            'generation': self.generation,
            'best_fitness': max(fitnesses),
            'average_fitness': np.mean(fitnesses),
            'best_accuracy': best_gene.accuracy,
            'best_carbon_kg': best_gene.carbon_cost_kg,
            'registered_expert_id': best_gene.registered_expert_id,
            'population_diversity': len(set(
                self.registry_bridge._compute_architecture_hash(g.architecture)
                for g in self.population
            )),
            'total_carbon_spent_kg': self.total_carbon_spent_kg,
            'carbon_budget_remaining': max(0, self.carbon_budget_kg - self.total_carbon_spent_kg)
        }
        
        self.evolution_history.append(metrics)
        
        # Selection and reproduction
        self._evolve_population()
        
        logger.info(
            f"Generation {self.generation}: "
            f"best_fitness={metrics['best_fitness']:.4f}, "
            f"accuracy={metrics['best_accuracy']:.3f}, "
            f"registered={best_gene.registered_expert_id or 'none'}"
        )
        
        return metrics
    
    def _evolve_population(self):
        """Evolve population through selection, crossover, mutation"""
        # Keep elite (top 20%)
        elite_size = max(1, self.population_size // 5)
        elite = self.population[:elite_size]
        
        new_population = elite.copy()
        
        # Fill rest through crossover and mutation
        while len(new_population) < self.population_size:
            if np.random.random() < 0.7:  # Crossover
                parent1, parent2 = np.random.choice(elite, 2, replace=False)
                child_arch = self._crossover_architectures(
                    parent1.architecture, parent2.architecture
                )
            else:  # Mutation
                parent = np.random.choice(elite)
                child_arch = self._mutate_architecture(parent.architecture)
            
            child = ArchitectureGene(architecture=child_arch)
            new_population.append(child)
        
        self.population = new_population
    
    def _crossover_architectures(
        self,
        arch1: Dict[str, Any],
        arch2: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Crossover two architectures"""
        child = {}
        
        all_keys = set(arch1.keys()) | set(arch2.keys())
        for key in all_keys:
            if key in arch1 and key in arch2:
                # Randomly inherit from either parent
                child[key] = arch1[key] if np.random.random() < 0.5 else arch2[key]
            elif key in arch1:
                child[key] = arch1[key]
            else:
                child[key] = arch2[key]
        
        return child
    
    def get_best_architecture(self) -> Optional[ArchitectureGene]:
        """Get best architecture from population"""
        if not self.population:
            return None
        
        return self.population[0]
    
    def get_registered_experts(self) -> List[str]:
        """Get list of NAS-registered expert IDs"""
        return [
            g.registered_expert_id
            for g in self.population
            if g.registered_expert_id
        ]
    
    def get_evolution_summary(self) -> Dict[str, Any]:
        """Get evolution summary"""
        return {
            'generations': self.generation,
            'total_carbon_spent_kg': self.total_carbon_spent_kg,
            'registered_architectures': len(self.registry_bridge.registered_architectures),
            'best_accuracy': self.population[0].accuracy if self.population else 0,
            'evolution_history': self.evolution_history[-10:],
            'registered_experts': self.get_registered_experts()
        }
    
    def inject_registry(self, registry: Any):
        """Inject expert registry for auto-registration"""
        self.registry_bridge.inject_registry(registry)
