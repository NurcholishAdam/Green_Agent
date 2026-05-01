# run_enhanced_agent.py

"""
Complete runner for enhanced Green Agent with all modules integrated
"""

import sys
import signal
import time
import logging
import json
from datetime import datetime
from typing import Dict, Optional

# Add path
sys.path.insert(0, 'src/enhancements')

from synthetic_data_manager import SyntheticDataSource, DataQuality, ScenarioType
from control_system import ControlSystem, ControlMode
from fallback_manager import FallbackManager, FallbackStrategy
from thermal_optimizer import ThermalAwareOptimizer
from phase_energy_model import PhaseAwareEnergyModel
from energy_scaler import EnergyProportionalScaler
from marginal_carbon import MarginalCarbonIntensityForecaster
from dual_accountant import DualCarbonAccountant
from helium_elasticity import HeliumPriceElasticityModel, WorkloadPriority
from material_substitution import MaterialSubstitutionEngine
from helium_circularity import HeliumCircularityTracker
from regret_optimizer import RegretMinimizationOptimizer
from federated_learning import FederatedGreenLearning

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class EnhancedGreenAgent:
    """
    Complete enhanced Green Agent with all scientific modules
    """
    
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        self.running = False
        
        # Initialize components
        self.data_source = SyntheticDataSource(self.config.get('data', {}))
        self.control_system = ControlSystem(self.config.get('control', {}))
        self.fallback_manager = FallbackManager(self.config.get('fallback', {}))
        
        # Initialize enhancement modules
        self.thermal = ThermalAwareOptimizer(self.config.get('thermal', {}))
        self.phase_energy = PhaseAwareEnergyModel(self.config.get('phase_energy', {}))
        self.energy_scaler = EnergyProportionalScaler(self.config.get('energy_scaler', {}))
        self.marginal_carbon = MarginalCarbonIntensityForecaster(self.config.get('marginal_carbon', {}))
        self.dual_accountant = DualCarbonAccountant(self.config.get('dual_accountant', {}))
        self.helium_elasticity = HeliumPriceElasticityModel(self.config.get('helium_elasticity', {}))
        self.material_substitution = MaterialSubstitutionEngine(self.config.get('material_substitution', {}))
        self.helium_circularity = HeliumCircularityTracker(self.config.get('helium_circularity', {}))
        self.regret_optimizer = RegretMinimizationOptimizer(self.config.get('regret_optimizer', {}))
        self.federated_learning = FederatedGreenLearning(self.config.get('federated_learning', {}))
        
        logger.info("Enhanced Green Agent initialized")
    
    def start(self):
        """Start the enhanced agent"""
        logger.info("Starting Enhanced Green Agent...")
        
        # Start data source
        self.data_source.start()
        
        # Start background control loop
        self.running = True
        signal.signal(signal.SIGINT, self.stop)
        
        # Run main loop
        self._run_main_loop()
    
    def stop(self, *args):
        """Stop the enhanced agent"""
        logger.info("Stopping Enhanced Green Agent...")
        self.running = False
        self.data_source.stop()
    
    def _run_main_loop(self):
        """Main control loop"""
        iteration = 0
        
        while self.running:
            try:
                iteration += 1
                self._process_iteration(iteration)
                time.sleep(5)  # 5 second control loop
            except Exception as e:
                logger.error(f"Loop error: {e}")
                if self.running:
                    time.sleep(1)
    
    def _process_iteration(self, iteration: int):
        """Process one control iteration"""
        
        # 1. Collect data with fallbacks
        temp_data = self._get_temperature_with_fallback()
        grid_data = self._get_grid_with_fallback()
        helium_data = self._get_helium_with_fallback()
        
        # 2. Run enhancements
        thermal_decision = self.thermal.optimize_schedule(None, None)
        elasticity_decision = self.helium_elasticity.get_elasticity_decision(
            WorkloadPriority.MEDIUM,
            10.0,
            None,
            'yellow'
        )
        
        # 3. Apply controls
        if elasticity_decision.action == 'throttle':
            self.control_system.execute('throttle', elasticity_decision.throttle_factor)
        
        if thermal_decision.action == 'cool':
            cooling_power = max(50, min(500, (thermal_decision.target_temp - 20) * 10))
            self.control_system.execute('cooling', cooling_power)
        
        # 4. Log metrics
        if iteration % 12 == 0:  # Every minute
            self._log_metrics(temp_data, grid_data, helium_data)
    
    def _get_temperature_with_fallback(self):
        """Get temperature with fallback handling"""
        def primary():
            return self.data_source.get_temperature_data()
        
        result = self.fallback_manager.execute_with_fallback(
            primary,
            'temperature',
            self._get_fallback_config()
        )
        
        if result.success:
            return result.value
        else:
            logger.error("Temperature data unavailable")
            return {'gpu_temp': 70, 'cpu_temp': 65}
    
    def _get_grid_with_fallback(self):
        """Get grid data with fallback handling"""
        def primary():
            return self.data_source.get_grid_data('us-east')
        
        result = self.fallback_manager.execute_with_fallback(
            primary,
            'grid',
            self._get_fallback_config()
        )
        
        if result.success:
            return result.value
        else:
            logger.error("Grid data unavailable")
            return {'average_intensity': 400, 'renewable_percentage': 0.2}
    
    def _get_helium_with_fallback(self):
        """Get helium data with fallback handling"""
        def primary():
            return self.data_source.get_helium_data()
        
        result = self.fallback_manager.execute_with_fallback(
            primary,
            'helium',
            self._get_fallback_config()
        )
        
        if result.success:
            return result.value
        else:
            logger.error("Helium data unavailable")
            return {'spot_price': 6.0, 'inventory_days': 20}
    
    def _get_fallback_config(self):
        """Get fallback configuration"""
        from fallback_manager import FallbackConfig, FallbackStrategy
        return FallbackConfig(strategy=FallbackStrategy.CASCADE)
    
    def _log_metrics(self, temp_data, grid_data, helium_data):
        """Log current metrics"""
        metrics = {
            'timestamp': datetime.now().isoformat(),
            'temperature': {
                'gpu': temp_data.gpu_temp_c if hasattr(temp_data, 'gpu_temp_c') else temp_data.get('gpu_temp', 0),
                'cpu': temp_data.cpu_temp_c if hasattr(temp_data, 'cpu_temp_c') else temp_data.get('cpu_temp', 0)
            },
            'grid': {
                'intensity': grid_data.average_intensity_gco2_per_kwh if hasattr(grid_data, 'average_intensity_gco2_per_kwh') else grid_data.get('average_intensity', 0)
            },
            'helium': {
                'price': helium_data.spot_price_usd_per_liter if hasattr(helium_data, 'spot_price_usd_per_liter') else helium_data.get('spot_price', 0)
            },
            'control': self.control_system.get_metrics()
        }
        
        logger.info(f"Metrics: {json.dumps(metrics, indent=2)}")
    
    def generate_report(self) -> str:
        """Generate complete system report"""
        report = {
            'status': 'running' if self.running else 'stopped',
            'data_source': self.data_source.get_scenario_metrics(),
            'control_system': self.control_system.get_status(),
            'circuit_breakers': self.fallback_manager.get_circuit_breaker_status(),
            'metrics': self.control_system.get_metrics()
        }
        return json.dumps(report, indent=2)


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Enhanced Green Agent')
    parser.add_argument('--config', type=str, help='Configuration file path')
    parser.add_argument('--scenario', type=str, default='normal', 
                       choices=['normal', 'heatwave', 'helium_crisis', 'high_carbon'])
    parser.add_argument('--quality', type=str, default='perfect',
                       choices=['perfect', 'noisy', 'degraded', 'offline'])
    
    args = parser.parse_args()
    
    # Load configuration
    config = {}
    if args.config:
        with open(args.config, 'r') as f:
            config = json.load(f)
    
    # Apply CLI overrides
    config['data'] = config.get('data', {})
    config['data']['quality'] = args.quality
    
    # Create and start agent
    agent = EnhancedGreenAgent(config)
    
    # Set scenario
    if args.scenario == 'heatwave':
        agent.data_source.set_scenario(ScenarioType.HEATWAVE)
    elif args.scenario == 'helium_crisis':
        agent.data_source.set_scenario(ScenarioType.HELIUM_CRISIS)
    elif args.scenario == 'high_carbon':
        agent.data_source.set_scenario(ScenarioType.HIGH_CARBON)
    
    # Start agent
    agent.start()


if __name__ == "__main__":
    main()
