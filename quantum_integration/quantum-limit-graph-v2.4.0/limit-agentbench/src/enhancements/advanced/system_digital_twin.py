# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/advanced/system_digital_twin.py
"""
System-Wide Digital Twin for Green Agent v2.0.0
Simulates the entire agent network, expert interactions, and material flows
to forecast long-term sustainability implications.

Enhanced Features:
- Interdependent Scenarios (policy + technology adoption) (NEW)
- Correlated Uncertainty Between Resources (NEW)
- Resource Substitution Modeling (NEW)
- Weighted Scoring Based on User Priorities (NEW)
- Cost-Benefit Analysis for Recommendations (NEW)

Integrates with:
- quantum_limit_integration.py for quantum resource modeling
- biodiversity_impact.py for ecological impact simulations
- expert_registry.py for expert population dynamics
- circular_computing.py for hardware lifecycle modeling
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import numpy as np
from collections import deque, defaultdict
import hashlib
import json
from scipy.stats import multivariate_normal  # For correlated uncertainty

logger = logging.getLogger(__name__)

# ============================================================================
# Enums and Data Classes (Enhanced)
# ============================================================================

class SimulationScenario(Enum):
    """Types of simulation scenarios"""
    POLICY_CHANGE = "policy_change"
    MARKET_SHOCK = "market_shock"
    RESOURCE_DEPLETION = "resource_depletion"
    TECHNOLOGY_ADOPTION = "technology_adoption"
    REGULATORY_CHANGE = "regulatory_change"
    CLIMATE_EVENT = "climate_event"
    # NEW: Interdependent scenarios
    POLICY_AND_TECHNOLOGY = "policy_and_technology"
    MARKET_AND_REGULATORY = "market_and_regulatory"
    RESOURCE_AND_CLIMATE = "resource_and_climate"

@dataclass
class DigitalTwinConfig:
    """Configuration for the digital twin simulation"""
    time_horizon_years: int = 10
    time_step_days: int = 30
    n_simulations: int = 1000
    confidence_level: float = 0.95
    include_stochastic_events: bool = True
    parallel_simulations: int = 4
    expert_population_dynamics: bool = True
    material_flow_tracking: bool = True
    carbon_pricing_scenario: str = "linear_increase"
    helium_depletion_model: str = "exponential"
    # NEW: Enhanced configuration
    correlated_uncertainty: bool = True
    resource_substitution_enabled: bool = True
    user_priorities: Dict[str, float] = field(default_factory=lambda: {
        'carbon': 0.25,
        'helium': 0.20,
        'energy': 0.15,
        'circularity': 0.20,
        'biodiversity': 0.20
    })

@dataclass
class SimulationResult:
    """Result of a digital twin simulation"""
    scenario_id: str
    scenario_type: SimulationScenario
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    metrics: Dict[str, Any] = field(default_factory=dict)
    projections: Dict[str, List[float]] = field(default_factory=dict)
    confidence_intervals: Dict[str, Tuple[float, float]] = field(default_factory=dict)
    risk_factors: List[str] = field(default_factory=list)
    recommendations: List[Dict[str, Any]] = field(default_factory=list)  # Enhanced with cost-benefit
    sustainability_score: float = 0.0
    # NEW: Enhanced fields
    interdependent_factors: List[str] = field(default_factory=list)
    substitution_effects: Dict[str, float] = field(default_factory=dict)
    weighted_score: float = 0.0

@dataclass
class ResourceProjection:
    """Projection for a specific resource with substitution modeling"""
    resource_type: str  # helium, carbon, energy
    current_level: float
    projected_levels: List[float]  # Time series
    depletion_year: Optional[int] = None
    confidence_lower: List[float] = field(default_factory=list)
    confidence_upper: List[float] = field(default_factory=list)
    # NEW: Substitution modeling
    substitution_availability: float = 0.0
    substitution_cost_factor: float = 1.0
    substitution_timeline: Optional[List[float]] = None
    alternative_resources: List[str] = field(default_factory=list)

@dataclass
class RecommendationWithCostBenefit:
    """Recommendation with cost-benefit analysis"""
    action: str
    description: str
    estimated_cost: float  # In Eco-ATP units
    estimated_benefit: float  # In sustainability score points
    roi: float
    time_horizon_months: int
    risk_level: str  # low, medium, high
    prerequisites: List[str] = field(default_factory=list)
    confidence: float = 0.7

# ============================================================================
# ENHANCED SYSTEM DIGITAL TWIN
# ============================================================================

class SystemDigitalTwin:
    """
    System-Wide Digital Twin v2.0.0 for Green Agent.
    
    Features:
    - High-level simulation of the entire agent ecosystem
    - Strategic "what-if" analysis for policies and events
    - Long-term resource depletion forecasting
    - Expert population dynamics simulation
    - Material flow and circularity modeling
    - Interdependent scenarios (NEW)
    - Correlated uncertainty between resources (NEW)
    - Resource substitution modeling (NEW)
    - Weighted scoring based on user priorities (NEW)
    - Cost-benefit analysis for recommendations (NEW)
    """
    
    def __init__(self, config: Optional[DigitalTwinConfig] = None):
        self.config = config or DigitalTwinConfig()
        self.scenario_results: List[SimulationResult] = []
        self.simulation_cache: Dict[str, SimulationResult] = {}
        self._lock = asyncio.Lock()
        
        # Sub-modules (will be injected)
        self.quantum_limits = None
        self.biodiversity = None
        self.expert_registry = None
        self.circular_manager = None
        self.carbon_manager = None
        self.helium_tracker = None
        
        # Resource projections with substitution
        self.resource_projections: Dict[str, ResourceProjection] = {}
        self.substitution_options: Dict[str, List[str]] = {
            'helium': ['hydrogen_cooling', 'nitrogen_cooling', 'cryogenic_alternative'],
            'carbon': ['renewable_energy', 'carbon_offset', 'carbon_capture'],
            'energy': ['solar', 'wind', 'geothermal', 'nuclear']
        }
        
        # Simulation history
        self.simulation_history: deque = deque(maxlen=100)
        
        # NEW: Correlation matrix for resources
        self.resource_correlation = self._init_correlation_matrix()
        
        # NEW: User priority weights
        self.priority_weights = self.config.user_priorities
        
        logger.info("System Digital Twin v2.0.0 initialized")
    
    def _init_correlation_matrix(self) -> Dict[str, Dict[str, float]]:
        """Initialize correlation matrix between resources"""
        return {
            'carbon': {'carbon': 1.0, 'helium': 0.3, 'energy': 0.7, 'circularity': -0.4, 'biodiversity': -0.6},
            'helium': {'carbon': 0.3, 'helium': 1.0, 'energy': 0.5, 'circularity': -0.2, 'biodiversity': -0.3},
            'energy': {'carbon': 0.7, 'helium': 0.5, 'energy': 1.0, 'circularity': -0.3, 'biodiversity': -0.4},
            'circularity': {'carbon': -0.4, 'helium': -0.2, 'energy': -0.3, 'circularity': 1.0, 'biodiversity': 0.3},
            'biodiversity': {'carbon': -0.6, 'helium': -0.3, 'energy': -0.4, 'circularity': 0.3, 'biodiversity': 1.0}
        }
    
    # ========================================================================
    # Module Injection
    # ========================================================================
    
    def inject_modules(self, **modules):
        """Inject required system modules"""
        for name, module in modules.items():
            setattr(self, name, module)
            logger.info(f"Injected module: {name}")
    
    # ========================================================================
    # Enhanced Core Simulation Methods
    # ========================================================================
    
    async def run_scenario(
        self,
        scenario_type: SimulationScenario,
        parameters: Dict[str, Any],
        time_horizon_years: Optional[int] = None,
        n_simulations: Optional[int] = None
    ) -> SimulationResult:
        """
        Run a simulation scenario on the digital twin with enhanced features.
        
        Args:
            scenario_type: Type of scenario to simulate
            parameters: Scenario-specific parameters
            time_horizon_years: Override config time horizon
            n_simulations: Override config simulation count
            
        Returns:
            SimulationResult with projections and recommendations
        """
        async with self._lock:
            scenario_id = self._generate_scenario_id(scenario_type, parameters)
            
            if scenario_id in self.simulation_cache:
                logger.info(f"Returning cached simulation for {scenario_id}")
                return self.simulation_cache[scenario_id]
            
            time_horizon = time_horizon_years or self.config.time_horizon_years
            n_sim = n_simulations or self.config.n_simulations
            
            result = await self._run_simulation(
                scenario_type, parameters, time_horizon, n_sim
            )
            
            self.simulation_cache[scenario_id] = result
            self.scenario_results.append(result)
            
            logger.info(f"Completed scenario: {scenario_id}")
            return result
    
    async def _run_simulation(
        self,
        scenario_type: SimulationScenario,
        parameters: Dict[str, Any],
        time_horizon_years: int,
        n_simulations: int
    ) -> SimulationResult:
        """Internal method to run the simulation with enhanced features"""
        n_steps = int(time_horizon_years * 365 / self.config.time_step_days)
        timestamps = [
            datetime.now() + timedelta(days=i * self.config.time_step_days)
            for i in range(n_steps)
        ]
        
        # Initialize projections
        projections = {
            'carbon_emissions': [0.0] * n_steps,
            'helium_depletion': [0.0] * n_steps,
            'energy_consumption': [0.0] * n_steps,
            'expert_population': [0.0] * n_steps,
            'circularity_index': [0.0] * n_steps,
            'biodiversity_impact': [0.0] * n_steps
        }
        
        # Determine interdependent factors
        interdependent_factors = self._get_interdependent_factors(scenario_type, parameters)
        
        # Run Monte Carlo simulations with correlated uncertainty
        all_simulations = []
        n_parallel = min(n_simulations, self.config.parallel_simulations)
        
        for sim_idx in range(n_parallel):
            sim_result = await self._run_single_simulation_correlated(
                scenario_type, parameters, timestamps, sim_idx, n_parallel
            )
            all_simulations.append(sim_result)
        
        # Aggregate results
        for key in projections.keys():
            values = [sim[key] for sim in all_simulations]
            projections[key] = np.mean(values, axis=0).tolist()
            
            if self.config.confidence_level < 1.0:
                lower = np.percentile(values, (1 - self.config.confidence_level) / 2 * 100, axis=0)
                upper = np.percentile(values, (1 + self.config.confidence_level) / 2 * 100, axis=0)
            else:
                lower = [0.0] * len(projections[key])
                upper = [0.0] * len(projections[key])
            
            # Store with substitution modeling
            substitution = self._get_substitution_effects(key)
            alternative_resources = self.substitution_options.get(key, [])
            
            self.resource_projections[key] = ResourceProjection(
                resource_type=key,
                current_level=projections[key][0] if projections[key] else 0,
                projected_levels=projections[key],
                confidence_lower=lower.tolist() if hasattr(lower, 'tolist') else lower,
                confidence_upper=upper.tolist() if hasattr(upper, 'tolist') else upper,
                depletion_year=self._calculate_depletion_year(projections[key]),
                substitution_availability=substitution.get('availability', 0.0),
                substitution_cost_factor=substitution.get('cost_factor', 1.0),
                alternative_resources=alternative_resources
            )
        
        # Calculate weighted sustainability score
        weighted_score = self._calculate_weighted_sustainability_score(projections)
        sustainability_score = self._calculate_sustainability_score(projections)
        
        # Generate cost-benefit recommendations
        recommendations = self._generate_cost_benefit_recommendations(
            scenario_type, projections, parameters
        )
        
        # Identify risk factors
        risk_factors = self._identify_risk_factors(projections)
        
        return SimulationResult(
            scenario_id=self._generate_scenario_id(scenario_type, parameters),
            scenario_type=scenario_type,
            metrics={
                'time_horizon_years': time_horizon_years,
                'n_simulations': n_simulations,
                'n_steps': n_steps,
                'correlated_uncertainty': self.config.correlated_uncertainty,
                'resource_substitution': self.config.resource_substitution_enabled
            },
            projections=projections,
            confidence_intervals={
                key: (proj.confidence_lower[-1] if proj.confidence_lower else 0,
                      proj.confidence_upper[-1] if proj.confidence_upper else 0)
                for key, proj in self.resource_projections.items()
                if key in self.resource_projections
            },
            risk_factors=risk_factors,
            recommendations=recommendations,
            sustainability_score=sustainability_score,
            interdependent_factors=interdependent_factors,
            substitution_effects=self._get_substitution_effects_all(),
            weighted_score=weighted_score
        )
    
    async def _run_single_simulation_correlated(
        self,
        scenario_type: SimulationScenario,
        parameters: Dict[str, Any],
        timestamps: List[datetime],
        sim_idx: int,
        total_sims: int
    ) -> Dict[str, List[float]]:
        """Run a single simulation with correlated uncertainty"""
        # Get current state
        current_carbon = await self._get_current_carbon()
        current_helium = await self._get_current_helium()
        current_energy = await self._get_current_energy()
        current_experts = await self._get_current_expert_count()
        current_circularity = await self._get_current_circularity()
        
        # Generate correlated noise
        if self.config.correlated_uncertainty:
            # Create correlation matrix
            resources = ['carbon', 'helium', 'energy', 'circularity', 'biodiversity']
            mean = np.zeros(len(resources))
            cov_matrix = self._build_covariance_matrix(resources)
            
            # Generate correlated noise
            noise_samples = multivariate_normal.rvs(mean, cov_matrix, size=len(timestamps))
            carbon_noise = noise_samples[:, 0]
            helium_noise = noise_samples[:, 1]
            energy_noise = noise_samples[:, 2]
            circularity_noise = noise_samples[:, 3]
            biodiversity_noise = noise_samples[:, 4]
        else:
            # Independent noise
            carbon_noise = np.random.normal(0, 0.02, len(timestamps))
            helium_noise = np.random.normal(0, 0.02, len(timestamps))
            energy_noise = np.random.normal(0, 0.01, len(timestamps))
            circularity_noise = np.random.normal(0, 0.01, len(timestamps))
            biodiversity_noise = np.random.normal(0, 0.01, len(timestamps))
        
        carbon_emissions = []
        helium_depletion = []
        energy_consumption = []
        expert_population = []
        circularity_index = []
        biodiversity_impact = []
        
        for i, timestamp in enumerate(timestamps):
            # Apply scenario effects with interdependence
            carbon_effect, helium_effect, energy_effect = self._apply_interdependent_scenario(
                scenario_type, parameters, i
            )
            
            # Apply individual scenario effects if not interdependent
            if scenario_type not in [SimulationScenario.POLICY_AND_TECHNOLOGY,
                                     SimulationScenario.MARKET_AND_REGULATORY,
                                     SimulationScenario.RESOURCE_AND_CLIMATE]:
                carbon_effect = self._apply_scenario_effect(scenario_type, parameters, i, 'carbon')
                helium_effect = self._apply_scenario_effect(scenario_type, parameters, i, 'helium')
                energy_effect = self._apply_scenario_effect(scenario_type, parameters, i, 'energy')
            
            # Apply substitution effects
            substitution_factor = 1.0
            if self.config.resource_substitution_enabled:
                substitution_factor = self._apply_substitution_effects(i, parameters)
            
            # Update state with correlated noise
            noise_factor_carbon = 1.0 + carbon_noise[i] * 0.1
            noise_factor_helium = 1.0 + helium_noise[i] * 0.1
            noise_factor_energy = 1.0 + energy_noise[i] * 0.05
            
            carbon_val = current_carbon * carbon_effect * noise_factor_carbon * substitution_factor
            helium_val = current_helium * helium_effect * noise_factor_helium * substitution_factor
            energy_val = current_energy * energy_effect * noise_factor_energy * substitution_factor
            
            carbon_emissions.append(carbon_val)
            helium_depletion.append(helium_val)
            energy_consumption.append(energy_val)
            expert_population.append(
                current_experts * (1 + np.random.normal(0, 0.005)) * noise_factor_carbon
            )
            circularity_index.append(
                current_circularity * (1 + circularity_noise[i] * 0.05)
            )
            biodiversity_impact.append(
                1.0 - (carbon_val / 1000) * 0.1 + biodiversity_noise[i] * 0.02
            )
        
        return {
            'carbon_emissions': carbon_emissions,
            'helium_depletion': helium_depletion,
            'energy_consumption': energy_consumption,
            'expert_population': expert_population,
            'circularity_index': circularity_index,
            'biodiversity_impact': biodiversity_impact
        }
    
    def _build_covariance_matrix(self, resources: List[str]) -> np.ndarray:
        """Build covariance matrix from correlation matrix"""
        n = len(resources)
        corr_matrix = np.zeros((n, n))
        
        for i, res_i in enumerate(resources):
            for j, res_j in enumerate(resources):
                corr_matrix[i, j] = self.resource_correlation.get(res_i, {}).get(res_j, 0.0)
        
        # Add small variance terms
        variances = [0.02, 0.02, 0.01, 0.01, 0.01]  # Standard deviations
        std_matrix = np.diag(variances[:n])
        cov_matrix = std_matrix @ corr_matrix @ std_matrix
        
        return cov_matrix
    
    # ========================================================================
    # Enhanced Scenario Effect Functions
    # ========================================================================
    
    def _get_interdependent_factors(self, scenario_type: SimulationScenario, parameters: Dict) -> List[str]:
        """Get interdependent factors for a scenario"""
        factors = []
        
        if scenario_type == SimulationScenario.POLICY_AND_TECHNOLOGY:
            if 'carbon_reduction_rate' in parameters:
                factors.append('carbon_policy')
            if 'adoption_rate' in parameters:
                factors.append('technology_adoption')
            if 'carbon_efficiency_gain' in parameters:
                factors.append('carbon_efficiency')
        
        elif scenario_type == SimulationScenario.MARKET_AND_REGULATORY:
            if 'shock_size' in parameters:
                factors.append('market_shock')
            if 'carbon_tax_rate' in parameters:
                factors.append('carbon_regulation')
            if 'helium_quota_reduction' in parameters:
                factors.append('helium_regulation')
        
        elif scenario_type == SimulationScenario.RESOURCE_AND_CLIMATE:
            if 'carbon_depletion_rate' in parameters:
                factors.append('carbon_depletion')
            if 'helium_depletion_rate' in parameters:
                factors.append('helium_depletion')
            if 'event_impact' in parameters:
                factors.append('climate_event')
        
        return factors
    
    def _apply_interdependent_scenario(
        self,
        scenario_type: SimulationScenario,
        parameters: Dict,
        step: int
    ) -> Tuple[float, float, float]:
        """Apply interdependent scenario effects"""
        carbon_effect = 1.0
        helium_effect = 1.0
        energy_effect = 1.0
        
        if scenario_type == SimulationScenario.POLICY_AND_TECHNOLOGY:
            # Combined policy and technology effects
            carbon_reduction = parameters.get('carbon_reduction_rate', 0.05)
            adoption_rate = parameters.get('adoption_rate', 0.1)
            efficiency_gain = parameters.get('carbon_efficiency_gain', 0.3)
            
            # Interdependent effect: technology accelerates policy impact
            tech_factor = 1 - np.exp(-adoption_rate * step)
            carbon_effect = 1.0 - (carbon_reduction * (1 + tech_factor * 0.5)) * (step / 10)
            helium_effect = 1.0 - (carbon_reduction * 0.3 * (1 + tech_factor * 0.3)) * (step / 10)
            energy_effect = 1.0 - efficiency_gain * tech_factor * 0.5
        
        elif scenario_type == SimulationScenario.MARKET_AND_REGULATORY:
            # Combined market and regulatory effects
            shock_size = parameters.get('shock_size', 0.3)
            shock_duration = parameters.get('shock_duration', 5)
            tax_rate = parameters.get('carbon_tax_rate', 0.1)
            quota_reduction = parameters.get('helium_quota_reduction', 0.05)
            
            # Market shock affects carbon
            if step < shock_duration:
                shock_factor = 1.0 + (1.0 - step / shock_duration) * shock_size
                carbon_effect = shock_factor
                helium_effect = shock_factor * 0.5
                energy_effect = shock_factor * 0.3
            
            # Regulatory effect accumulates
            carbon_effect *= (1.0 + tax_rate * (step / 10))
            helium_effect *= (1.0 - quota_reduction * (step / 10))
            energy_effect *= (1.0 + tax_rate * 0.5 * (step / 10))
        
        elif scenario_type == SimulationScenario.RESOURCE_AND_CLIMATE:
            # Combined resource depletion and climate effects
            carbon_depletion = parameters.get('carbon_depletion_rate', 0.02)
            helium_depletion = parameters.get('helium_depletion_rate', 0.03)
            event_impact = parameters.get('event_impact', 0.2)
            event_duration = parameters.get('event_duration', 3)
            recovery_rate = parameters.get('recovery_rate', 0.1)
            
            # Resource depletion
            carbon_effect = 1.0 - carbon_depletion * step
            helium_effect = max(0.1, 1.0 - helium_depletion * step)
            
            # Climate event
            if step < event_duration:
                carbon_effect *= (1.0 + event_impact)
                helium_effect *= (1.0 + event_impact * 0.7)
                energy_effect *= (1.0 + event_impact * 0.5)
            else:
                recovery = np.exp(-recovery_rate * (step - event_duration))
                carbon_effect *= (1.0 + event_impact * recovery * 0.5)
                helium_effect *= (1.0 + event_impact * 0.7 * recovery * 0.5)
                energy_effect *= (1.0 + event_impact * 0.5 * recovery * 0.5)
        
        return carbon_effect, helium_effect, energy_effect
    
    def _apply_scenario_effect(
        self,
        scenario_type: SimulationScenario,
        params: Dict,
        step: int,
        resource: str = 'carbon'
    ) -> float:
        """Apply individual scenario effect"""
        if scenario_type == SimulationScenario.POLICY_CHANGE:
            if resource == 'carbon':
                reduction_rate = params.get('carbon_reduction_rate', 0.05)
                return 1.0 - reduction_rate * (step / 10)
            else:  # helium
                conservation_rate = params.get('helium_conservation_rate', 0.03)
                return 1.0 - conservation_rate * (step / 10)
        
        elif scenario_type == SimulationScenario.MARKET_SHOCK:
            shock_size = params.get('shock_size', 0.3)
            shock_duration = params.get('shock_duration', 5)
            if step < shock_duration:
                return 1.0 + (1.0 - step / shock_duration) * shock_size
            return 1.0
        
        elif scenario_type == SimulationScenario.RESOURCE_DEPLETION:
            if resource == 'carbon':
                depletion_rate = params.get('carbon_depletion_rate', 0.02)
                return 1.0 - depletion_rate * step
            else:  # helium
                depletion_rate = params.get('helium_depletion_rate', 0.03)
                return max(0.1, 1.0 - depletion_rate * step)
        
        elif scenario_type == SimulationScenario.TECHNOLOGY_ADOPTION:
            adoption_rate = params.get('adoption_rate', 0.1)
            if resource == 'carbon':
                efficiency_gain = params.get('carbon_efficiency_gain', 0.3)
                return 1.0 - efficiency_gain * (1 - np.exp(-adoption_rate * step))
            else:  # helium
                efficiency_gain = params.get('helium_efficiency_gain', 0.2)
                return 1.0 - efficiency_gain * (1 - np.exp(-adoption_rate * step))
        
        elif scenario_type == SimulationScenario.REGULATORY_CHANGE:
            if resource == 'carbon':
                tax_rate = params.get('carbon_tax_rate', 0.1)
                return 1.0 + tax_rate * (step / 10)
            else:  # helium
                quota_reduction = params.get('helium_quota_reduction', 0.05)
                return 1.0 - quota_reduction * (step / 10)
        
        elif scenario_type == SimulationScenario.CLIMATE_EVENT:
            event_impact = params.get('event_impact', 0.2)
            event_duration = params.get('event_duration', 3)
            recovery_rate = params.get('recovery_rate', 0.1)
            
            if step < event_duration:
                return 1.0 + event_impact
            else:
                return 1.0 + event_impact * np.exp(-recovery_rate * (step - event_duration))
        
        return 1.0
    
    # ========================================================================
    # Resource Substitution Modeling (NEW)
    # ========================================================================
    
    def _get_substitution_effects(self, resource_type: str) -> Dict[str, float]:
        """Get substitution effects for a resource"""
        effects = {
            'availability': 0.0,
            'cost_factor': 1.0,
            'timeline': 0.0
        }
        
        if resource_type == 'helium':
            # Helium substitution with alternatives
            effects['availability'] = 0.3  # 30% of helium can be substituted
            effects['cost_factor'] = 2.0  # 2x cost for alternatives
            effects['timeline'] = 24.0  # months to implement
        
        elif resource_type == 'carbon':
            # Carbon substitution with renewables
            effects['availability'] = 0.5
            effects['cost_factor'] = 1.5
            effects['timeline'] = 12.0
        
        elif resource_type == 'energy':
            # Energy substitution with renewables
            effects['availability'] = 0.6
            effects['cost_factor'] = 1.3
            effects['timeline'] = 18.0
        
        return effects
    
    def _apply_substitution_effects(self, step: int, parameters: Dict) -> float:
        """Apply substitution effects based on step and parameters"""
        # Simulate substitution ramp-up
        substitution_start = parameters.get('substitution_start_step', 10)
        substitution_rate = parameters.get('substitution_rate', 0.05)
        
        if step < substitution_start:
            return 1.0
        
        ramp_steps = step - substitution_start
        return 1.0 - min(0.5, substitution_rate * ramp_steps)
    
    def _get_substitution_effects_all(self) -> Dict[str, float]:
        """Get substitution effects for all resources"""
        effects = {}
        for resource in ['carbon', 'helium', 'energy', 'circularity', 'biodiversity']:
            effects[resource] = self._get_substitution_effects(resource)
        return effects
    
    # ========================================================================
    # Real Data Access Methods (Preserved)
    # ========================================================================
    
    async def _get_current_carbon(self) -> float:
        if self.carbon_manager:
            if hasattr(self.carbon_manager, 'get_current_intensity'):
                intensity = await self.carbon_manager.get_current_intensity()
                return intensity / 1000
            elif hasattr(self.carbon_manager, 'carbon_intensity'):
                return self.carbon_manager.carbon_intensity / 1000
        return 0.5
    
    async def _get_current_helium(self) -> float:
        if self.helium_tracker:
            if hasattr(self.helium_tracker, 'get_helium_position'):
                position = self.helium_tracker.get_helium_position()
                if position:
                    return position.get('total_usage_l', 0) / position.get('budget_l', 100)
        return 0.5
    
    async def _get_current_energy(self) -> float:
        if self.expert_registry:
            experts = self.expert_registry.get_all_active_experts()
            total_energy = sum(
                getattr(e, 'energy_per_inference', 0.001) 
                for e in experts[:10]
            )
            return min(1.0, total_energy * 100)
        return 0.5
    
    async def _get_current_expert_count(self) -> float:
        if self.expert_registry:
            return len(self.expert_registry.get_all_active_experts())
        return 10
    
    async def _get_current_circularity(self) -> float:
        if self.circular_manager:
            if hasattr(self.circular_manager, 'get_circularity_report'):
                report = self.circular_manager.get_circularity_report()
                if report:
                    return report.get('circularity_score', 0.5)
        return 0.5
    
    # ========================================================================
    # Enhanced Analysis Methods
    # ========================================================================
    
    def _calculate_depletion_year(self, projection: List[float]) -> Optional[int]:
        if len(projection) < 2:
            return None
        
        for i, value in enumerate(projection):
            if value <= 0.0:
                years_from_now = i * self.config.time_step_days / 365.0
                return int(datetime.now().year + years_from_now)
        
        trend = (projection[-1] - projection[0]) / len(projection)
        if trend < 0:
            steps_to_zero = int(projection[-1] / -trend)
            years_from_now = (len(projection) + steps_to_zero) * self.config.time_step_days / 365.0
            return int(datetime.now().year + years_from_now)
        
        return None
    
    def _calculate_sustainability_score(self, projections: Dict) -> float:
        scores = []
        
        if 'carbon_emissions' in projections and projections['carbon_emissions']:
            carbon_end = projections['carbon_emissions'][-1]
            carbon_start = projections['carbon_emissions'][0]
            carbon_score = 1.0 - min(1.0, (carbon_end - carbon_start) / max(carbon_start, 0.1))
            scores.append(carbon_score)
        
        if 'helium_depletion' in projections and projections['helium_depletion']:
            helium_end = projections['helium_depletion'][-1]
            helium_start = projections['helium_depletion'][0]
            helium_score = min(1.0, helium_end / max(helium_start, 0.1))
            scores.append(helium_score)
        
        if 'circularity_index' in projections and projections['circularity_index']:
            circ_end = projections['circularity_index'][-1]
            circ_start = projections['circularity_index'][0]
            circ_score = min(1.0, circ_end / max(circ_start, 0.1))
            scores.append(circ_score)
        
        if 'biodiversity_impact' in projections and projections['biodiversity_impact']:
            bio_end = projections['biodiversity_impact'][-1]
            bio_score = max(0, min(1.0, bio_end))
            scores.append(bio_score)
        
        return np.mean(scores) if scores else 0.5
    
    def _calculate_weighted_sustainability_score(self, projections: Dict) -> float:
        """Calculate sustainability score weighted by user priorities"""
        if not self.priority_weights:
            return self._calculate_sustainability_score(projections)
        
        weighted_scores = []
        total_weight = 0.0
        
        for key, weight in self.priority_weights.items():
            if key == 'carbon' and 'carbon_emissions' in projections:
                carbon_end = projections['carbon_emissions'][-1]
                carbon_start = projections['carbon_emissions'][0]
                score = 1.0 - min(1.0, (carbon_end - carbon_start) / max(carbon_start, 0.1))
                weighted_scores.append(score * weight)
                total_weight += weight
            
            elif key == 'helium' and 'helium_depletion' in projections:
                helium_end = projections['helium_depletion'][-1]
                helium_start = projections['helium_depletion'][0]
                score = min(1.0, helium_end / max(helium_start, 0.1))
                weighted_scores.append(score * weight)
                total_weight += weight
            
            elif key == 'energy' and 'energy_consumption' in projections:
                energy_end = projections['energy_consumption'][-1]
                energy_start = projections['energy_consumption'][0]
                score = 1.0 - min(1.0, (energy_end - energy_start) / max(energy_start, 0.1))
                weighted_scores.append(score * weight)
                total_weight += weight
            
            elif key == 'circularity' and 'circularity_index' in projections:
                circ_end = projections['circularity_index'][-1]
                circ_start = projections['circularity_index'][0]
                score = min(1.0, circ_end / max(circ_start, 0.1))
                weighted_scores.append(score * weight)
                total_weight += weight
            
            elif key == 'biodiversity' and 'biodiversity_impact' in projections:
                bio_end = projections['biodiversity_impact'][-1]
                score = max(0, min(1.0, bio_end))
                weighted_scores.append(score * weight)
                total_weight += weight
        
        return sum(weighted_scores) / max(total_weight, 0.001)
    
    # ========================================================================
    # Enhanced Cost-Benefit Recommendations (NEW)
    # ========================================================================
    
    def _generate_cost_benefit_recommendations(
        self,
        scenario_type: SimulationScenario,
        projections: Dict,
        parameters: Dict
    ) -> List[Dict[str, Any]]:
        """Generate recommendations with cost-benefit analysis"""
        recommendations = []
        
        # Carbon recommendations
        if 'carbon_emissions' in projections and projections['carbon_emissions']:
            trend = projections['carbon_emissions'][-1] - projections['carbon_emissions'][0]
            if trend > 0:
                recommendations.append(self._create_recommendation_with_cost_benefit(
                    action="Reduce Carbon Emissions",
                    description="Implement aggressive carbon reduction strategies",
                    estimated_cost=50.0,
                    estimated_benefit=0.3,
                    time_horizon_months=12,
                    risk_level="medium",
                    prerequisites=["Carbon budget approval", "Expert review"],
                    confidence=0.75
                ))
                recommendations.append(self._create_recommendation_with_cost_benefit(
                    action="Adopt Renewable Energy",
                    description="Increase renewable energy adoption to 50%",
                    estimated_cost=30.0,
                    estimated_benefit=0.2,
                    time_horizon_months=18,
                    risk_level="low",
                    prerequisites=["Renewable vendor selection", "Infrastructure upgrade"],
                    confidence=0.85
                ))
            elif trend < -0.1:
                recommendations.append(self._create_recommendation_with_cost_benefit(
                    action="Maintain Carbon Momentum",
                    description="Continue successful carbon reduction strategies",
                    estimated_cost=10.0,
                    estimated_benefit=0.15,
                    time_horizon_months=6,
                    risk_level="low",
                    confidence=0.9
                ))
        
        # Helium recommendations
        if 'helium_depletion' in projections and projections['helium_depletion']:
            if projections['helium_depletion'][-1] < 0.3:
                recommendations.append(self._create_recommendation_with_cost_benefit(
                    action="CRITICAL: Helium Conservation",
                    description="Implement immediate helium recovery and substitution",
                    estimated_cost=80.0,
                    estimated_benefit=0.5,
                    time_horizon_months=6,
                    risk_level="high",
                    prerequisites=["Helium recovery system", "Substitution research"],
                    confidence=0.7
                ))
            elif projections['helium_depletion'][-1] < 0.5:
                recommendations.append(self._create_recommendation_with_cost_benefit(
                    action="Optimize Helium Usage",
                    description="Improve helium efficiency in quantum cooling",
                    estimated_cost=25.0,
                    estimated_benefit=0.2,
                    time_horizon_months=9,
                    risk_level="medium",
                    prerequisites=["Helium audit", "Efficiency review"],
                    confidence=0.8
                ))
        
        # Circularity recommendations
        if 'circularity_index' in projections and projections['circularity_index']:
            if projections['circularity_index'][-1] < 0.5:
                recommendations.append(self._create_recommendation_with_cost_benefit(
                    action="Improve Circularity",
                    description="Enhance material recovery and recycling",
                    estimated_cost=40.0,
                    estimated_benefit=0.25,
                    time_horizon_months=15,
                    risk_level="medium",
                    prerequisites=["Circularity audit", "Recycling infrastructure"],
                    confidence=0.75
                ))
        
        # Scenario-specific recommendations
        if scenario_type == SimulationScenario.POLICY_CHANGE:
            if parameters.get('carbon_reduction_rate', 0) > 0.05:
                recommendations.append(self._create_recommendation_with_cost_benefit(
                    action="Increase Policy Ambition",
                    description="Consider more aggressive carbon reduction targets",
                    estimated_cost=15.0,
                    estimated_benefit=0.1,
                    time_horizon_months=3,
                    risk_level="low",
                    confidence=0.85
                ))
        
        if scenario_type == SimulationScenario.RESOURCE_DEPLETION:
            recommendations.append(self._create_recommendation_with_cost_benefit(
                action="Resource Diversification",
                description="Diversify resource portfolio to reduce dependency",
                estimated_cost=60.0,
                estimated_benefit=0.35,
                time_horizon_months=24,
                risk_level="medium",
                prerequisites=["Resource audit", "Alternative identification"],
                confidence=0.7
            ))
        
        # Add substitution recommendations
        if self.config.resource_substitution_enabled:
            for resource, alternatives in self.substitution_options.items():
                if resource in self.resource_projections:
                    proj = self.resource_projections[resource]
                    if proj.substitution_availability > 0.3:
                        recommendations.append(self._create_recommendation_with_cost_benefit(
                            action=f"Substitute {resource.capitalize()}",
                            description=f"Transition to {', '.join(alternatives[:2])} as alternatives",
                            estimated_cost=45.0,
                            estimated_benefit=0.3,
                            time_horizon_months=18,
                            risk_level="medium",
                            prerequisites=[f"{resource} substitution study", "Alternative validation"],
                            confidence=0.7
                        ))
        
        # Sort by ROI
        recommendations.sort(key=lambda x: x.get('roi', 0), reverse=True)
        
        return recommendations
    
    def _create_recommendation_with_cost_benefit(
        self,
        action: str,
        description: str,
        estimated_cost: float,
        estimated_benefit: float,
        time_horizon_months: int,
        risk_level: str,
        prerequisites: List[str] = None,
        confidence: float = 0.7
    ) -> Dict[str, Any]:
        """Create a recommendation with cost-benefit analysis"""
        roi = estimated_benefit / max(estimated_cost, 0.01)
        
        return {
            'action': action,
            'description': description,
            'estimated_cost': estimated_cost,
            'estimated_benefit': estimated_benefit,
            'roi': roi,
            'time_horizon_months': time_horizon_months,
            'risk_level': risk_level,
            'prerequisites': prerequisites or [],
            'confidence': confidence,
            'cost_benefit_ratio': f"1:{roi:.2f}"
        }
    
    def _identify_risk_factors(self, projections: Dict) -> List[str]:
        risks = []
        
        for key, values in projections.items():
            if values and len(values) > 1:
                trend = values[-1] - values[0]
                if trend < -0.1:
                    if 'carbon' in key or 'emissions' in key:
                        risks.append(f"Increasing {key} - carbon risk")
                    elif 'helium' in key or 'depletion' in key:
                        risks.append(f"Declining {key} - helium scarcity risk")
        
        for key, values in projections.items():
            if values and len(values) > 10:
                volatility = np.std(values[-10:])
                if volatility > 0.1:
                    risks.append(f"High volatility in {key}")
        
        for key, proj in self.resource_projections.items():
            if proj.depletion_year and proj.depletion_year < datetime.now().year + 5:
                risks.append(f"{key} depletion risk within 5 years")
        
        return risks
    
    # ========================================================================
    # Utility Methods (Preserved)
    # ========================================================================
    
    def _generate_scenario_id(self, scenario_type: SimulationScenario, parameters: Dict) -> str:
        param_str = json.dumps(parameters, sort_keys=True)
        hash_str = hashlib.md5(f"{scenario_type.value}{param_str}".encode()).hexdigest()[:8]
        return f"{scenario_type.value}_{hash_str}"
    
    # ========================================================================
    # Enhanced Statistics and Export
    # ========================================================================
    
    def get_simulation_stats(self) -> Dict[str, Any]:
        """Get simulation statistics with enhanced metrics"""
        return {
            'total_scenarios': len(self.scenario_results),
            'cached_scenarios': len(self.simulation_cache),
            'resource_projections': len(self.resource_projections),
            'correlated_uncertainty_enabled': self.config.correlated_uncertainty,
            'resource_substitution_enabled': self.config.resource_substitution_enabled,
            'recent_results': [
                {
                    'scenario_id': r.scenario_id,
                    'type': r.scenario_type.value,
                    'sustainability_score': r.sustainability_score,
                    'weighted_score': r.weighted_score,
                    'recommendations': [
                        {'action': rec.get('action'), 'roi': rec.get('roi', 0)}
                        for rec in r.recommendations[:2]
                    ]
                }
                for r in self.scenario_results[-5:]
            ] if self.scenario_results else []
        }
    
    def update_user_priorities(self, new_priorities: Dict[str, float]):
        """Update user priority weights"""
        self.priority_weights.update(new_priorities)
        logger.info(f"User priorities updated: {self.priority_weights}")
    
    async def export_projections(self) -> Dict[str, Any]:
        """Export all resource projections with substitution data"""
        return {
            'timestamp': datetime.now().isoformat(),
            'config': {
                'time_horizon_years': self.config.time_horizon_years,
                'correlated_uncertainty': self.config.correlated_uncertainty,
                'resource_substitution': self.config.resource_substitution_enabled,
                'user_priorities': self.priority_weights
            },
            'projections': {
                key: {
                    'current': proj.current_level,
                    'projected': proj.projected_levels[-10:],
                    'depletion_year': proj.depletion_year,
                    'confidence_lower': proj.confidence_lower[-10:] if proj.confidence_lower else [],
                    'confidence_upper': proj.confidence_upper[-10:] if proj.confidence_upper else [],
                    'substitution_availability': proj.substitution_availability,
                    'substitution_cost_factor': proj.substitution_cost_factor,
                    'alternative_resources': proj.alternative_resources
                }
                for key, proj in self.resource_projections.items()
            },
            'user_priorities': self.priority_weights,
            'resource_correlation': self.resource_correlation
        }
    
    async def shutdown(self):
        """Graceful shutdown"""
        logger.info("Shutting down System Digital Twin v2.0.0")
