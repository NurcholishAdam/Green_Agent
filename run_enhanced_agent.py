# run_enhanced_agent.py

"""
Complete runner for enhanced Green Agent with all modules integrated.
Additionally integrates advanced enhancement modules (LIMIT Graph, MODP,
RLHF, Multi‑Teacher On‑Policy Distillation, Bio‑inspired Optimisation,
MoE expert gating, and FlexGen integration) from src/enhancements.
These are optional and gracefully degrade if not installed.
"""

import sys
import signal
import time
import logging
import json
from datetime import datetime
from typing import Dict, Optional, Any

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

# ------------------------------------------------------------------------------
# Optional imports for advanced enhancements (graceful fallback)
# ------------------------------------------------------------------------------
try:
    from src.enhancements.schemas.node_descriptor import NodeDescriptor, NodeType, CoolingType, MaintenanceStatus, RoutingStrategy
    from src.enhancements.schemas.workload_descriptor import WorkloadDescriptor, TaskType, Urgency, Priority, BioMode
    from src.enhancements.schemas.feedback_event import FeedbackEvent
    from src.enhancements.zero_trust_architecture import ZeroTrustArchitecture, ZeroTrustConfig
    from src.enhancements.core.graph_registry import GraphRegistry, GraphType
    from src.enhancements.core.causal_graph import CausalGraph
    from src.enhancements.core.meta_cognition import MetaCognitionLayer
    from src.enhancements.metrics.dag_carbon_ledger import DAGCarbonLedger
    ENHANCEMENTS_AVAILABLE = True
except ImportError:
    ENHANCEMENTS_AVAILABLE = False
    NodeDescriptor = None
    WorkloadDescriptor = None
    FeedbackEvent = None
    ZeroTrustArchitecture = None
    ZeroTrustConfig = None
    GraphRegistry = None
    GraphType = None
    CausalGraph = None
    MetaCognitionLayer = None
    DAGCarbonLedger = None

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class EnhancedGreenAgent:
    """
    Complete enhanced Green Agent with all scientific modules and advanced
    decision-making modules (LIMIT Graph, MODP, RLHF, etc.).
    """

    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        self.running = False

        # Initialize components
        self.data_source = SyntheticDataSource(self.config.get('data', {}))
        self.control_system = ControlSystem(self.config.get('control', {}))
        self.fallback_manager = FallbackManager(self.config.get('fallback', {}))

        # Initialize enhancement modules (original)
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

        # ------------------------------------------------------------------
        # Advanced enhancement initialization (optional)
        # ------------------------------------------------------------------
        self.use_enhancements = self.config.get('use_enhancements', True) and ENHANCEMENTS_AVAILABLE
        self.graph_registry = None
        self.causal_graph = None
        self.meta_cognition = None
        self.node_descriptor = None
        self.workload_descriptor = None
        self.zero_trust = None
        self.dag_carbon_ledger = None

        if self.use_enhancements:
            self._initialize_advanced_modules()

        logger.info("Enhanced Green Agent initialized (with advanced modules: %s)", self.use_enhancements)

    def _initialize_advanced_modules(self):
        """Initialize LIMIT Graph, MODP, RLHF, distillation, MoE, etc."""
        try:
            # Graph registry and causal graph for root-cause
            self.graph_registry = GraphRegistry()
            self.causal_graph = self.graph_registry.get_or_create(GraphType.CAUSAL)
            self.meta_cognition = MetaCognitionLayer(causal_graph=self.causal_graph)

            # Node descriptor for routing (distillation + MoE + RLHF + LIMIT Graph)
            self.node_descriptor = NodeDescriptor(
                id="enhanced_agent_node",
                type=NodeType.EDGE,
                region=self.config.get('region', 'us-east'),
                region_carbon_intensity=self.config.get('carbon_intensity', 400.0),
                energy_per_token=0.00005,
                helium_connectivity_score=0.8,
                uptime=0.99,
                renewable_fraction=0.3,
                cooling_type=CoolingType.AIR,
                hardware_model="cpu",
                graph_metrics=self.config.get('graph_metrics', {'centrality': 0.7, 'connectivity': 0.6}),
                human_feedback_score=self.config.get('human_feedback_score', 0.6),
                use_evolutionary=self.config.get('use_evolutionary', True),
                metadata={
                    "distillation_epsilon": self.config.get('distillation_epsilon', 0.1),
                    "gating_learning_rate": self.config.get('gating_lr', 0.005),
                }
            )

            # Workload descriptor for priority selection (MODP + MoE + RLHF)
            self.workload_descriptor = WorkloadDescriptor(
                task_id="loop_task",
                task_type=TaskType.INFERENCE,
                tokens=1000,
                latency_target=500.0,
                urgency=Urgency.MEDIUM,
                estimated_energy_joules=0.001,
                estimated_carbon_kg=0.0002,
                human_feedback_score=self.config.get('human_feedback_score', 0.6),
                graph_metrics=self.config.get('graph_metrics', {'centrality': 0.7, 'connectivity': 0.6}),
                use_evolutionary=self.config.get('use_evolutionary', True),
                metadata={
                    "latency_weight": 0.4,
                    "carbon_weight": 0.3,
                    "energy_weight": 0.3,
                }
            )

            # Zero Trust (optional)
            if self.config.get('enable_zero_trust', False):
                zt_config = ZeroTrustConfig(
                    use_enhancements=True,
                    use_distillation=True,
                    use_evolutionary=True,
                    human_feedback_score=self.config.get('human_feedback_score', 0.6),
                    graph_metrics=self.config.get('graph_metrics', {'centrality': 0.7, 'connectivity': 0.6})
                )
                self.zero_trust = ZeroTrustArchitecture(config=zt_config)

            # DAG Carbon Ledger for carbon backpropagation
            self.dag_carbon_ledger = DAGCarbonLedger(storage_path="/tmp/enhanced_agent_ledger")

            logger.info("Advanced enhancement modules initialized")
        except Exception as e:
            logger.error(f"Failed to initialize advanced modules: {e}")
            self.use_enhancements = False

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
        """Process one control iteration, including advanced decision-making if enabled."""

        # 1. Collect data with fallbacks
        temp_data = self._get_temperature_with_fallback()
        grid_data = self._get_grid_with_fallback()
        helium_data = self._get_helium_with_fallback()

        # 2. Original enhancements (thermal, helium, etc.)
        thermal_decision = self.thermal.optimize_schedule(None, None)
        elasticity_decision = self.helium_elasticity.get_elasticity_decision(
            WorkloadPriority.MEDIUM,
            10.0,
            None,
            'yellow'
        )

        # 3. Apply controls from original modules
        if elasticity_decision.action == 'throttle':
            self.control_system.execute('throttle', elasticity_decision.throttle_factor)

        if thermal_decision.action == 'cool':
            cooling_power = max(50, min(500, (thermal_decision.target_temp - 20) * 10))
            self.control_system.execute('cooling', cooling_power)

        # 4. Advanced enhancement decision making
        enhanced_decision = {}
        if self.use_enhancements:
            enhanced_decision = self._make_enhanced_decisions(temp_data, grid_data, helium_data)

        # 5. Apply any enhanced controls if they modify throttle/cooling
        if enhanced_decision.get('strategy') == 'carbon_first':
            # Additional throttle based on carbon
            self.control_system.execute('throttle', 0.4)
        elif enhanced_decision.get('priority') == 'green':
            self.control_system.execute('throttle', 0.6)

        # 6. Log metrics
        if iteration % 12 == 0:  # Every minute
            self._log_metrics(temp_data, grid_data, helium_data, enhanced_decision)

    def _make_enhanced_decisions(self, temp_data, grid_data, helium_data) -> Dict[str, Any]:
        """
        Use advanced modules to select routing strategy and workload priority.
        Also run root-cause analysis with meta-cognition.
        Returns a dict with selected strategy, priority, and diagnosis.
        """
        decisions = {}

        # Update node descriptor with current carbon intensity
        if self.node_descriptor:
            carbon = getattr(grid_data, 'average_intensity_gco2_per_kwh', None) or grid_data.get('average_intensity', 400)
            self.node_descriptor.region_carbon_intensity = carbon
            try:
                strategy = asyncio.run(self.node_descriptor.select_routing_strategy(exploration=False))
                decisions['strategy'] = strategy
            except Exception as e:
                logger.warning(f"Node descriptor routing failed: {e}")

        # Update workload descriptor and select priority
        if self.workload_descriptor:
            try:
                priority = asyncio.run(self.workload_descriptor.select_priority(exploration=False))
                decisions['priority'] = priority
            except Exception as e:
                logger.warning(f"Workload descriptor priority failed: {e}")

        # Root-cause analysis with meta-cognition
        if self.meta_cognition and self.causal_graph:
            try:
                self.meta_cognition.observe_snapshot({
                    "CarbonIntensity": getattr(grid_data, 'average_intensity_gco2_per_kwh', 400),
                    "GridStrain": 0.7 if getattr(grid_data, 'average_intensity_gco2_per_kwh', 400) > 500 else 0.4,
                })
                diagnosis = self.meta_cognition.diagnose()
                if diagnosis['status'] == 'anomaly_detected':
                    decisions['diagnosis'] = diagnosis['recommended_action']
            except Exception as e:
                logger.warning(f"Meta-cognition diagnosis failed: {e}")

        # Record a sample outcome to update distillation models (simulated)
        if self.node_descriptor and self.workload_descriptor:
            try:
                carbon_saved = 0.01  # dummy
                latency = 100
                asyncio.run(self.node_descriptor.record_outcome(carbon_saved_kg=carbon_saved, latency_ms=latency, cost_usd=0.001))
                asyncio.run(self.workload_descriptor.record_outcome(latency_achieved_ms=latency, carbon_saved_kg=carbon_saved, energy_used_joules=0.001))
            except Exception as e:
                logger.warning(f"Record outcome failed: {e}")

        # Create FeedbackEvent if ZeroTrust or feedback enabled
        if FeedbackEvent is not None:
            try:
                event = FeedbackEvent(
                    source="run_enhanced_agent",
                    feedback_type="routing",
                    task_id="loop_task",
                    context={"iteration": self._get_current_iteration()},
                    action={"selected_action": decisions.get('strategy', 'unknown')},
                    performance={"quality_score": 0.8, "latency_ms": 100, "energy_joules": 100,
                                 "carbon_g": getattr(grid_data, 'average_intensity_gco2_per_kwh', 400),
                                 "helium_cost": helium_data.spot_price_usd_per_liter if hasattr(helium_data, 'spot_price_usd_per_liter') else 0,
                                 "duration_ms": 100},
                    adaptive_cost_value=0.7,
                    graph_metrics=self.config.get('graph_metrics', {}),
                    human_feedback_score=self.config.get('human_feedback_score', 0.6),
                    modp_score=0.7,
                    distillation_stats={"student_counter": self.node_descriptor._routing_optimizer.counter if self.node_descriptor and hasattr(self.node_descriptor, '_routing_optimizer') else 0}
                )
                logger.debug(f"FeedbackEvent created: {event.event_id}")
            except Exception as e:
                logger.warning(f"FeedbackEvent creation failed: {e}")

        return decisions

    def _get_current_iteration(self):
        # Placeholder; in real system we'd track.
        return 0

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

    def _log_metrics(self, temp_data, grid_data, helium_data, enhanced_decision: Optional[Dict] = None):
        """Log current metrics, including enhanced decisions if available."""
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
        if enhanced_decision:
            metrics['enhanced_decision'] = enhanced_decision

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
        if self.use_enhancements:
            report['enhancement_status'] = {
                'node_descriptor': self.node_descriptor is not None,
                'workload_descriptor': self.workload_descriptor is not None,
                'causal_graph': self.causal_graph is not None,
                'zero_trust': self.zero_trust is not None,
                'dag_carbon_ledger': self.dag_carbon_ledger is not None
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
    parser.add_argument('--enhancements', action='store_true', default=True,
                       help='Enable advanced enhancements (default: true)')

    args = parser.parse_args()

    # Load configuration
    config = {}
    if args.config:
        with open(args.config, 'r') as f:
            config = json.load(f)

    # Apply CLI overrides
    config['data'] = config.get('data', {})
    config['data']['quality'] = args.quality
    if not args.enhancements:
        config['use_enhancements'] = False

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
