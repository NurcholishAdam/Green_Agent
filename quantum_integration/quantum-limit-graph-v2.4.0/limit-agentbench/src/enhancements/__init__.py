# src/enhancements/__init__.py

"""
Green Agent Enhancements Module
Integrates all scientific enhancement modules into Green Agent
"""

from .thermal_optimizer import ThermalAwareOptimizer, ThermalDecision
from .phase_energy_model import PhaseAwareEnergyModel, PhaseEnergyProfile
from .energy_scaler import EnergyProportionalScaler, ScaledModel, ScalingDecision
from .marginal_carbon import MarginalCarbonIntensityForecaster, MarginalCarbonForecast
from .dual_accountant import DualCarbonAccountant, CarbonAccounting
from .carbon_nas import CarbonAwareNAS, ArchitectureConfig, ArchitectureMetrics
from .helium_elasticity import HeliumPriceElasticityModel, ElasticityDecision, WorkloadPriority
from .material_substitution import MaterialSubstitutionEngine, SubstitutionDecision
from .helium_circularity import HeliumCircularityTracker, CircularityMetrics
from .regret_optimizer import RegretMinimizationOptimizer, RegretDecision
from .federated_learning import FederatedGreenLearning, FederatedPolicy

__all__ = [
    # Thermal
    'ThermalAwareOptimizer',
    'ThermalDecision',
    
    # Energy
    'PhaseAwareEnergyModel',
    'PhaseEnergyProfile',
    'EnergyProportionalScaler',
    'ScaledModel',
    'ScalingDecision',
    
    # Carbon
    'MarginalCarbonIntensityForecaster',
    'MarginalCarbonForecast',
    'DualCarbonAccountant',
    'CarbonAccounting',
    'CarbonAwareNAS',
    'ArchitectureConfig',
    'ArchitectureMetrics',
    
    # Helium
    'HeliumPriceElasticityModel',
    'ElasticityDecision',
    'WorkloadPriority',
    'MaterialSubstitutionEngine',
    'SubstitutionDecision',
    'HeliumCircularityTracker',
    'CircularityMetrics',
    
    # Decision
    'RegretMinimizationOptimizer',
    'RegretDecision',
    'FederatedGreenLearning',
    'FederatedPolicy'
]

__version__ = '1.0.0'


class GreenAgentEnhancements:
    """
    Unified interface for all Green Agent enhancements.
    Provides centralized access to all scientific optimization modules.
    """
    
    def __init__(self, config: dict = None):
        self.config = config or {}
        self._init_modules()
    
    def _init_modules(self):
        """Initialize all enhancement modules"""
        from .thermal_optimizer import ThermalAwareOptimizer
        from .phase_energy_model import PhaseAwareEnergyModel
        from .energy_scaler import EnergyProportionalScaler
        from .marginal_carbon import MarginalCarbonIntensityForecaster
        from .dual_accountant import DualCarbonAccountant
        from .carbon_nas import CarbonAwareNAS
        from .helium_elasticity import HeliumPriceElasticityModel
        from .material_substitution import MaterialSubstitutionEngine
        from .helium_circularity import HeliumCircularityTracker
        from .regret_optimizer import RegretMinimizationOptimizer
        from .federated_learning import FederatedGreenLearning
        
        self.thermal = ThermalAwareOptimizer(self.config.get('thermal', {}))
        self.phase_energy = PhaseAwareEnergyModel(self.config.get('phase_energy', {}))
        self.energy_scaler = EnergyProportionalScaler(self.config.get('energy_scaler', {}))
        self.marginal_carbon = MarginalCarbonIntensityForecaster(self.config.get('marginal_carbon', {}))
        self.dual_accountant = DualCarbonAccountant(self.config.get('dual_accountant', {}))
        self.carbon_nas = CarbonAwareNAS(self.config.get('carbon_nas', {}))
        self.helium_elasticity = HeliumPriceElasticityModel(self.config.get('helium_elasticity', {}))
        self.material_substitution = MaterialSubstitutionEngine(self.config.get('material_substitution', {}))
        self.helium_circularity = HeliumCircularityTracker(self.config.get('helium_circularity', {}))
        self.regret_optimizer = RegretMinimizationOptimizer(self.config.get('regret_optimizer', {}))
        self.federated_learning = FederatedGreenLearning(self.config.get('federated_learning', {}))
    
    def get_all_metrics(self) -> dict:
        """Get metrics from all enhancement modules"""
        return {
            'thermal': self.thermal.get_thermal_metrics(),
            'helium_market': self.helium_elasticity.get_market_metrics(),
            'substitution': self.material_substitution.get_substitution_metrics(),
            'circularity': self.helium_circularity.get_circularity_metrics().__dict__
        }
    
    def get_circularity_certificate(self, task_id: str):
        """Get circularity certificate for a task"""
        return self.helium_circularity.get_circularity_certificate(task_id)
    
    def get_federated_policy(self) -> FederatedPolicy:
        """Get current federated policy"""
        return self.federated_learning.get_local_policy()
