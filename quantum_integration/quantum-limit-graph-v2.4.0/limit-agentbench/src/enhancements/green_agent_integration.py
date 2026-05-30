# File: src/enhancements/green_agent_integration.py

"""
Green Agent Integration Layer - Version 6.1

Unified integration layer connecting all modules:
- Helium data → Elasticity → Regret Optimizer
- Helium data → Circularity → Sustainability Signals
- Helium data → Thermal Optimizer
- Synthetic Data ↔ All modules
- Blockchain verification for all modules
- Quantum optimization for critical paths
"""

from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional, Tuple, Any
import logging
import time
from datetime import datetime
from pathlib import Path
import json
import uuid

# Import base classes
try:
    from .base_classes import (
        BaseIntegrator, BaseMetrics, GreenAgentConfig, 
        ModuleRegistry, load_module_config
    )
except ImportError:
    from base_classes import (
        BaseIntegrator, BaseMetrics, GreenAgentConfig, 
        ModuleRegistry, load_module_config
    )

# Import modules with fallback
HELIUM_COLLECTOR_AVAILABLE = False
HELIUM_ELASTICITY_AVAILABLE = False
HELIUM_CIRCULARITY_AVAILABLE = False
QUANTUM_AVAILABLE = False
BLOCKCHAIN_AVAILABLE = False

try:
    from .helium_data_collector import get_helium_collector
    HELIUM_COLLECTOR_AVAILABLE = True
except ImportError:
    pass

try:
    from .helium_elasticity import get_helium_elasticity_calculator
    HELIUM_ELASTICITY_AVAILABLE = True
except ImportError:
    pass

try:
    from .helium_circularity import get_helium_circularity_calculator
    HELIUM_CIRCULARITY_AVAILABLE = True
except ImportError:
    pass

try:
    from .quantum_helium_optimizer import QuantumHeliumOptimizer
    QUANTUM_AVAILABLE = True
except ImportError:
    pass

try:
    from .blockchain_helium_verification import (
        HeliumProvenanceTracker, 
        HeliumCarbonCreditTokenizer
    )
    BLOCKCHAIN_AVAILABLE = True
except ImportError:
    pass

logger = logging.getLogger(__name__)

# ============================================================
# INTEGRATION METRICS
# ============================================================

@dataclass
class IntegrationMetrics(BaseMetrics):
    """Metrics from integration run"""
    source_module: str = "green_agent_integration"
    
    # Module availability
    helium_collector_available: bool = False
    helium_elasticity_available: bool = False
    helium_circularity_available: bool = False
    quantum_available: bool = False
    blockchain_available: bool = False
    
    # Integration results
    elasticity_calculated: bool = False
    circularity_calculated: bool = False
    regret_optimizer_ready: bool = False
    sustainability_signals_ready: bool = False
    thermal_optimizer_ready: bool = False
    synthetic_manager_ready: bool = False
    blockchain_verified: bool = False
    quantum_optimized: bool = False
    
    # Performance
    total_integration_time_ms: float = 0.0
    modules_integrated: int = 0

# ============================================================
# MAIN INTEGRATOR
# ============================================================

class GreenAgentIntegrator(BaseIntegrator):
    """
    Unified integration layer for all Green Agent modules.
    
    Data Flow:
    1. HeliumDataCollector → Raw helium market data
    2. HeliumElasticity → Elasticity metrics → Regret/Thermal Optimizers
    3. HeliumCircularity → Circularity metrics → Sustainability Signals
    4. SyntheticDataManager → Extended scenarios
    5. Blockchain → Verification & tokenization
    6. Quantum → Advanced optimization
    """
    
    def __init__(self, config: Dict = None):
        super().__init__(config)
        
        # Initialize all available modules
        self.collector = None
        self.elasticity_calc = None
        self.circularity_calc = None
        self.quantum_optimizer = None
        self.provenance_tracker = None
        self.carbon_tokenizer = None
        
        self._initialize_modules()
        
        # Integration history
        self.integration_runs: List[IntegrationMetrics] = []
        
        logger.info(f"GreenAgentIntegrator initialized with "
                   f"{self._count_available()} modules available")
    
    def _initialize_modules(self):
        """Initialize all available modules"""
        
        if HELIUM_COLLECTOR_AVAILABLE:
            try:
                self.collector = get_helium_collector()
                ModuleRegistry.register('helium_collector', self.collector)
                logger.info("HeliumDataCollector registered")
            except Exception as e:
                logger.warning(f"HeliumDataCollector init failed: {e}")
        
        if HELIUM_ELASTICITY_AVAILABLE:
            try:
                self.elasticity_calc = get_helium_elasticity_calculator()
                ModuleRegistry.register('helium_elasticity', self.elasticity_calc)
                logger.info("HeliumElasticityCalculator registered")
            except Exception as e:
                logger.warning(f"HeliumElasticity init failed: {e}")
        
        if HELIUM_CIRCULARITY_AVAILABLE:
            try:
                self.circularity_calc = get_helium_circularity_calculator()
                ModuleRegistry.register('helium_circularity', self.circularity_calc)
                logger.info("HeliumCircularityCalculator registered")
            except Exception as e:
                logger.warning(f"HeliumCircularity init failed: {e}")
        
        if QUANTUM_AVAILABLE:
            try:
                self.quantum_optimizer = QuantumHeliumOptimizer()
                ModuleRegistry.register('quantum_optimizer', self.quantum_optimizer)
                logger.info("QuantumHeliumOptimizer registered")
            except Exception as e:
                logger.warning(f"Quantum optimizer init failed: {e}")
        
        if BLOCKCHAIN_AVAILABLE:
            try:
                self.provenance_tracker = HeliumProvenanceTracker()
                self.carbon_tokenizer = HeliumCarbonCreditTokenizer()
                ModuleRegistry.register('blockchain_provenance', self.provenance_tracker)
                ModuleRegistry.register('blockchain_carbon', self.carbon_tokenizer)
                logger.info("Blockchain modules registered")
            except Exception as e:
                logger.warning(f"Blockchain init failed: {e}")
    
    def _count_available(self) -> int:
        """Count available modules"""
        return sum([
            HELIUM_COLLECTOR_AVAILABLE,
            HELIUM_ELASTICITY_AVAILABLE,
            HELIUM_CIRCULARITY_AVAILABLE,
            QUANTUM_AVAILABLE,
            BLOCKCHAIN_AVAILABLE
        ])
    
    def integrate(self, source_data: Dict = None, target_module: str = "all") -> Dict:
        """
        Main integration method.
        Runs full integration pipeline across all modules.
        """
        start_time = time.time()
        
        metrics = IntegrationMetrics(
            helium_collector_available=HELIUM_COLLECTOR_AVAILABLE,
            helium_elasticity_available=HELIUM_ELASTICITY_AVAILABLE,
            helium_circularity_available=HELIUM_CIRCULARITY_AVAILABLE,
            quantum_available=QUANTUM_AVAILABLE,
            blockchain_available=BLOCKCHAIN_AVAILABLE
        )
        
        integration_results = {
            'integration_id': str(uuid.uuid4())[:8],
            'timestamp': datetime.now().isoformat()
        }
        
        # Step 1: Get helium data
        helium_data = self._get_helium_data(source_data)
        if helium_data:
            integration_results['helium_data'] = helium_data
        
        # Step 2: Calculate elasticity
        if target_module in ['all', 'regret_optimizer', 'thermal_optimizer']:
            elasticity_result = self._run_elasticity_pipeline(helium_data)
            if elasticity_result:
                integration_results['elasticity'] = elasticity_result
                integration_results['regret_optimizer_data'] = self._prepare_regret_optimizer_data(elasticity_result)
                integration_results['thermal_optimizer_data'] = self._prepare_thermal_optimizer_data(elasticity_result)
                metrics.elasticity_calculated = True
                metrics.regret_optimizer_ready = True
                metrics.thermal_optimizer_ready = True
        
        # Step 3: Calculate circularity
        if target_module in ['all', 'sustainability_signals']:
            circularity_result = self._run_circularity_pipeline(helium_data)
            if circularity_result:
                integration_results['circularity'] = circularity_result
                integration_results['sustainability_data'] = self._prepare_sustainability_data(circularity_result)
                metrics.circularity_calculated = True
                metrics.sustainability_signals_ready = True
        
        # Step 4: Synthetic data generation
        if target_module in ['all', 'synthetic_manager']:
            synthetic_result = self._prepare_synthetic_data(helium_data)
            if synthetic_result:
                integration_results['synthetic_data'] = synthetic_result
                metrics.synthetic_manager_ready = True
        
        # Step 5: Blockchain verification
        if target_module in ['all', 'blockchain'] and BLOCKCHAIN_AVAILABLE:
            blockchain_result = self._run_blockchain_verification(helium_data)
            if blockchain_result:
                integration_results['blockchain'] = blockchain_result
                metrics.blockchain_verified = True
        
        # Step 6: Quantum optimization
        if target_module in ['all', 'quantum'] and QUANTUM_AVAILABLE:
            quantum_result = self._run_quantum_optimization(helium_data)
            if quantum_result:
                integration_results['quantum'] = quantum_result
                metrics.quantum_optimized = True
        
        # Finalize
        elapsed = time.time() - start_time
        metrics.total_integration_time_ms = elapsed * 1000
        metrics.modules_integrated = sum([
            metrics.elasticity_calculated,
            metrics.circularity_calculated,
            metrics.blockchain_verified,
            metrics.quantum_optimized
        ])
        
        self.integration_runs.append(metrics)
        
        integration_results['metrics'] = metrics.to_dict()
        
        logger.info(f"Integration completed in {elapsed:.2f}s with "
                   f"{metrics.modules_integrated} modules integrated")
        
        return integration_results
    
    def _get_helium_data(self, source_data: Dict = None) -> Optional[Dict]:
        """Get helium data from collector or source"""
        if source_data:
            return source_data
        
        if self.collector:
            latest = self.collector.get_latest()
            if latest:
                return latest.to_dict()
        
        return None
    
    def _run_elasticity_pipeline(self, helium_data: Dict) -> Optional[Dict]:
        """Run elasticity calculation pipeline"""
        if not self.elasticity_calc:
            return None
        
        try:
            metrics = self.elasticity_calc.calculate_comprehensive_elasticity(helium_data)
            return self.elasticity_calc.export_all()
        except Exception as e:
            logger.error(f"Elasticity pipeline failed: {e}")
            return None
    
    def _run_circularity_pipeline(self, helium_data: Dict) -> Optional[Dict]:
        """Run circularity calculation pipeline"""
        if not self.circularity_calc:
            return None
        
        try:
            metrics = self.circularity_calc.calculate_comprehensive_circularity(helium_data)
            return self.circularity_calc.export_all()
        except Exception as e:
            logger.error(f"Circularity pipeline failed: {e}")
            return None
    
    def _prepare_regret_optimizer_data(self, elasticity_result: Dict) -> Dict:
        """Prepare data for regret optimizer"""
        return {
            'decision_weights': elasticity_result.get('regret_optimizer', {}).get('decision_weights', {}),
            'scenario_modifiers': elasticity_result.get('regret_optimizer', {}).get('scenario_modifiers', {}),
            'helium_metrics': {
                'scarcity': elasticity_result.get('elasticity_metrics', {}).get('scarcity_elasticity', 0.5),
                'price_elasticity': elasticity_result.get('elasticity_metrics', {}).get('price_elasticity', -0.4)
            }
        }
    
    def _prepare_thermal_optimizer_data(self, elasticity_result: Dict) -> Dict:
        """Prepare data for thermal optimizer"""
        return {
            'thermal_params': elasticity_result.get('thermal_optimizer', {}).get('thermal_params', {}),
            'cooling_recommendations': elasticity_result.get('thermal_optimizer', {}).get('cooling_recommendations', {}),
            'helium_thermal_impact': {
                'thermal_elasticity': elasticity_result.get('elasticity_metrics', {}).get('thermal_elasticity', 0.5)
            }
        }
    
    def _prepare_sustainability_data(self, circularity_result: Dict) -> Dict:
        """Prepare data for sustainability signals"""
        return {
            'circularity_metrics': circularity_result.get('sustainability_signals', {}).get('circularity_metrics', {}),
            'material_flows': circularity_result.get('sustainability_signals', {}).get('material_flows', {}),
            'esg_readiness': circularity_result.get('sustainability_signals', {}).get('esg_readiness', {})
        }
    
    def _prepare_synthetic_data(self, helium_data: Dict) -> Dict:
        """Prepare synthetic data generation parameters"""
        return {
            'helium_features': helium_data,
            'generation_templates': {
                'high_scarcity': {'scarcity': 0.9, 'price': 200},
                'moderate_scarcity': {'scarcity': 0.5, 'price': 140},
                'low_scarcity': {'scarcity': 0.2, 'price': 90}
            }
        }
    
    def _run_blockchain_verification(self, helium_data: Dict) -> Optional[Dict]:
        """Run blockchain verification pipeline"""
        if not self.provenance_tracker:
            return None
        
        try:
            # Register helium batch
            record = self.provenance_tracker.register_helium_batch(
                source="Green Agent Integration",
                volume_liters=10000,
                purity=0.999,
                certification_level="gold"
            )
            
            # Issue carbon credits if available
            carbon_record = None
            if self.carbon_tokenizer and record:
                carbon_record = self.carbon_tokenizer.issue_credits(
                    recipient="0xDefaultAddress",
                    helium_saved_liters=5000,
                    carbon_equivalent_kg=2500
                )
            
            return {
                'provenance_record': record.to_dict() if record else None,
                'carbon_credit': carbon_record.to_dict() if carbon_record else None,
                'blockchain_available': True
            }
        except Exception as e:
            logger.error(f"Blockchain pipeline failed: {e}")
            return None
    
    def _run_quantum_optimization(self, helium_data: Dict) -> Optional[Dict]:
        """Run quantum optimization pipeline"""
        if not self.quantum_optimizer:
            return None
        
        try:
            # Simplified quantum optimization
            demands = [100, 80, 60]
            supplies = [120, 120]
            costs = [[10, 12, 15], [11, 9, 14]]
            
            result = self.quantum_optimizer.optimize_helium_allocation(
                demands, supplies, costs
            )
            
            return result.to_dict() if result else None
        except Exception as e:
            logger.error(f"Quantum pipeline failed: {e}")
            return None
    
    def get_integration_status(self) -> Dict:
        """Get comprehensive integration status"""
        return {
            'modules': ModuleRegistry.get_status(),
            'available_count': self._count_available(),
            'last_integration': self.integration_runs[-1].to_dict() if self.integration_runs else None,
            'total_integrations': len(self.integration_runs),
            'timestamp': datetime.now().isoformat()
        }

# ============================================================
# CONVENIENCE FUNCTIONS
# ============================================================

_integrator = None

def get_green_agent_integrator() -> GreenAgentIntegrator:
    """Get singleton integrator instance"""
    global _integrator
    if _integrator is None:
        _integrator = GreenAgentIntegrator()
    return _integrator

def quick_integration() -> Dict:
    """Quick integration run for all modules"""
    integrator = get_green_agent_integrator()
    return integrator.integrate()

def get_integration_status() -> Dict:
    """Get current integration status"""
    integrator = get_green_agent_integrator()
    return integrator.get_integration_status()

# ============================================================
# MAIN DEMONSTRATION
# ============================================================

def main():
    """Demonstrate full Green Agent integration"""
    print("=" * 80)
    print("Green Agent Integration Layer v6.1 - Full Integration Demo")
    print("=" * 80)
    
    # Initialize integrator
    integrator = GreenAgentIntegrator()
    
    print(f"\n📦 Module Availability:")
    print(f"   Helium Data Collector: {'✅' if HELIUM_COLLECTOR_AVAILABLE else '❌'}")
    print(f"   Helium Elasticity: {'✅' if HELIUM_ELASTICITY_AVAILABLE else '❌'}")
    print(f"   Helium Circularity: {'✅' if HELIUM_CIRCULARITY_AVAILABLE else '❌'}")
    print(f"   Quantum Optimizer: {'✅' if QUANTUM_AVAILABLE else '❌'}")
    print(f"   Blockchain: {'✅' if BLOCKCHAIN_AVAILABLE else '❌'}")
    
    # Run full integration
    print(f"\n🔬 Running Full Integration Pipeline...")
    results = integrator.integrate()
    
    print(f"\n📊 Integration Results:")
    print(f"   Integration ID: {results.get('integration_id')}")
    
    if 'elasticity' in results:
        print(f"   ✅ Elasticity calculated")
    if 'circularity' in results:
        print(f"   ✅ Circularity calculated")
    if 'sustainability_data' in results:
        print(f"   ✅ Sustainability signals ready")
    if 'regret_optimizer_data' in results:
        print(f"   ✅ Regret optimizer ready")
    if 'thermal_optimizer_data' in results:
        print(f"   ✅ Thermal optimizer ready")
    if 'synthetic_data' in results:
        print(f"   ✅ Synthetic data ready")
    if 'blockchain' in results:
        print(f"   ✅ Blockchain verified")
    if 'quantum' in results:
        print(f"   ✅ Quantum optimized")
    
    # Show metrics
    metrics = results.get('metrics', {})
    print(f"\n📈 Integration Metrics:")
    print(f"   Time: {metrics.get('total_integration_time_ms', 0):.0f}ms")
    print(f"   Modules Integrated: {metrics.get('modules_integrated', 0)}")
    
    # Show status
    status = integrator.get_integration_status()
    print(f"\n🔗 Module Status:")
    for module, info in status.get('modules', {}).items():
        print(f"   {module}: {'✅' if info.get('available') else '❌'}")
    
    print("\n" + "=" * 80)
    print("✅ Green Agent Integration Complete")
    print("=" * 80)
    
    return results

if __name__ == "__main__":
    results = main()
