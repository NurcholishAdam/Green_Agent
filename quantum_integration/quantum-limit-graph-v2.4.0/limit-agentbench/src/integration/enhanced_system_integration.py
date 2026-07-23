#!/usr/bin/env python3
# File: src/integration/enhanced_system_integration.py
"""
Integration of enhanced Green Agent modules:
- Evolutionary Engine
- Sustainability Cost Function
- Node Registry
- Tokenization Optimizer
- Harvester-aware Router
"""

import asyncio
import logging
from pathlib import Path
from typing import Dict, Any

# Import existing core modules (adjust paths as needed)
from src.expert_registry import ExpertRegistry
from src.expert_router import ExpertRouter
from src.digital_twin import DigitalTwin
from src.mlops_pipeline import MLOpsPipeline
from src.carbon_manager import CarbonIntensityManager
from src.helium_dashboard import HeliumEfficiencyDashboard
from src.bio_inspired import PhotosyntheticHarvester
from src.database.manager import DatabaseManager
from src.task_manager import TaskManager
from src.config import Config  # Assume a global config object

# Import new enhancements
from src.enhancements.evolutionary_engine import EvolutionaryEngine
from src.enhancements.sustainability_cost import SustainabilityCostFunction
from src.enhancements.node_registry import NodeRegistry
from src.enhancements.tokenization_optimizer import TokenizationOptimizer
from src.enhancements.expert_router_harvester import ExpertRouterWithHarvester
from src.enhancements.adaptive_cost_function import AdaptiveCostFunction
from src.enhancements.feedback_collector import FeedbackCollector
from src.enhancements.pareto_router import ParetoRouter
from src.user_preferences import UserPreferences

logger = logging.getLogger(__name__)

class EnhancedSystemIntegrator:
    """
    Handles the initialization and injection of all enhanced modules.
    """

    def __init__(self, config: Dict[str, Any]):
        self.config = config

        # Core components (assumed already initialised)
        self.db_manager: DatabaseManager = None
        self.task_manager: TaskManager = None
        self.registry: ExpertRegistry = None
        self.router: ExpertRouter = None
        self.digital_twin: DigitalTwin = None
        self.mlops: MLOpsPipeline = None
        self.carbon_manager: CarbonIntensityManager = None
        self.helium_dashboard: HeliumEfficiencyDashboard = None
        self.harvester: PhotosyntheticHarvester = None

        # Enhanced components
        self.cost_function: SustainabilityCostFunction = None
        self.node_registry: NodeRegistry = None
        self.evolution_engine: EvolutionaryEngine = None
        self.token_optimizer: TokenizationOptimizer = None

        # Enhanced router (will replace original)
        self.enhanced_router: ExpertRouterWithHarvester = None

    async def initialize(self):
        """Load core components from the existing system."""
        # In a real system, these would be passed or retrieved from a container.
        # Here we simulate their instantiation; adapt to your actual setup.
        logger.info("Initializing core components...")

        # Database and task manager
        self.db_manager = DatabaseManager(self.config.get('database', {}))
        self.task_manager = TaskManager(max_workers=10)

        # Core modules
        self.registry = ExpertRegistry(self.config.get('registry', {}))
        self.digital_twin = DigitalTwin(self.config.get('digital_twin', {}))
        self.mlops = MLOpsPipeline(self.config.get('mlops', {}))
        self.carbon_manager = CarbonIntensityManager(self.config.get('carbon', {}))
        self.helium_dashboard = HeliumEfficiencyDashboard(self.config.get('helium', {}))
        self.harvester = PhotosyntheticHarvester(self.config.get('harvester', {}))

        # Original router – we'll replace it later
        self.router = ExpertRouter(self.config.get('router', {}))

        logger.info("Core components initialised.")
        adaptive_cost = AdaptiveCostFunction(...)

    # 1. Create adaptive cost function
        cost_config = {
        'alpha': 1.0, 'beta': 2.0, 'gamma': 0.5, 'delta': 0.3,
        'epsilon': 0.1, 'zeta': -0.1,
        'learning_rate': 0.01,
        'normalisation_window': 1000,
        'mae_threshold': 1.0,
        'rollback_enabled': True
    }
        adaptive_cost = AdaptiveCostFunction(cost_config)
        adaptive_cost.inject_dependencies(
        db_manager=db_manager,
        registry=registry,
        carbon_manager=carbon_manager,
        helium_dashboard=helium_dashboard,
        node_registry=node_registry
    )

# 2. Start validation loop
await adaptive_cost.start_validation_loop(interval_seconds=3600)

    # 3. Create feedback collector
    feedback_collector = FeedbackCollector(
    cost_function=adaptive_cost,
    registry=registry
    )

# 4. In your router, after executing a task, call:
# await feedback_collector.record(
#     request_id=request_id,
#     expert_id=selected_expert.expert_id,
#     node_id=node_id,
#     actual_energy_joules=measured_energy,
#     actual_carbon_kg=measured_carbon,
#     actual_helium_units=measured_helium,
#     actual_latency_ms=measured_latency,
#     actual_accuracy=measured_accuracy,
# )

# 5. On shutdown:
# await adaptive_cost.stop()
    async def setup_enhancements(self):
        """Create and inject all enhanced modules."""
        logger.info("Setting up enhancements...")

        # 1. Node Registry
        node_config = self.config.get('node_registry', {})
        self.node_registry = NodeRegistry(node_config, self.db_manager)
        await self.node_registry.start(refresh_interval=node_config.get('refresh_interval', 3600))

        # 2. Sustainability Cost Function
        cost_config = self.config.get('cost_function', {
            'alpha': 1.0,
            'beta': 2.0,
            'gamma': 0.5,
            'delta': 0.3,
            'epsilon': 0.1,
            'zeta': -0.1
        })
        self.cost_function = SustainabilityCostFunction(cost_config)
        self.cost_function.inject_dependencies(
            carbon_manager=self.carbon_manager,
            helium_dashboard=self.helium_dashboard,
            node_registry=self.node_registry
        )

        # 3. Replace Router with Harvester‑aware version
        self.enhanced_router = ExpertRouterWithHarvester(
            config=self.config.get('router', {}),
            harvester=self.harvester
        )
        self.enhanced_router.inject_cost_function(self.cost_function)
        # Transfer any existing state from original router (optional)
        self.enhanced_router.registry = self.registry
        # Use the enhanced router from now on
        self.router = self.enhanced_router

        
        pareto_router = ParetoRouter(
        config=router_config,
        cost_function=adaptive_cost,
        node_registry=node_registry,
        user_preferences=user_prefs
    )
        pareto_router.registry = registry

    # Use pareto_router for all routing calls
    #Example: get frontier for visualisation
     
        frontier = await pareto_router.get_frontier(task, context)
        logger.info(f"Pareto frontier: {frontier}")
       
# 4. Tokenization Optimizer
        token_config = self.config.get('tokenization', {})
        self.token_optimizer = TokenizationOptimizer(token_config)

        # 5. Evolutionary Engine
        evolution_config = self.config.get('evolutionary_engine', {
            'prune_threshold': 0.2,
            'merge_similarity_threshold': 0.85,
            'spawn_gap_threshold': 0.3
        })
        self.evolution_engine = EvolutionaryEngine(
            config=evolution_config,
            registry=self.registry,
            cost_function=self.cost_function,
            digital_twin=self.digital_twin,
            mlops=self.mlops,
            db_manager=self.db_manager,
            task_manager=self.task_manager
        )

        # Start the evolutionary engine as a background task
        self.task_manager.start_task(
            "evolution_engine",
            self.evolution_engine.start,
            interval_seconds=evolution_config.get('interval_seconds', 3600)
        )

        logger.info("All enhancements set up and started.")

        user_prefs = UserPreferences({
        'alpha': 0.5,
        'beta': 2.0,
        'gamma': 0.5,
        'delta': 0.3,
        'epsilon': 0.1,
        'zeta': -0.1
    })
        

    async def shutdown(self):
        """Gracefully shut down all enhanced components."""
        logger.info("Shutting down enhanced components...")
        await self.evolution_engine.stop()
        await self.node_registry.stop()
        # Shut down task manager and other core components as needed
        await self.task_manager.stop_all()
        logger.info("Shutdown complete.")

    async def run(self):
        """Example of how to use the enhanced system."""
        # Simulate a request
        task = {"type": "summarize", "text": "Example input text"}
        context = {
            "token_count": 50,
            "target_node_id": "aws-us-east-1",
            "data_source": "photosynthetic_harvester"
        }
        result = await self.router.route(task, context)
        logger.info(f"Routing result: {result}")

        # Tokenization example
        optimized = self.token_optimizer.optimize(task["text"], context)
        logger.info(f"Tokenization: {optimized}")

        # Wait a bit to let evolution run (optional)
        await asyncio.sleep(10)



async def main():
    # Load configuration (could be from environment, file, etc.)
    config = {
        "database": {"db_path": "./green_agent.db"},
        "carbon": {"api_key": "your_key", "region": "global"},
        "helium": {},
        "harvester": {},
        "router": {},
        "node_registry": {"refresh_interval": 3600},
        "cost_function": {"alpha": 1.0, "beta": 2.0, "gamma": 0.5, "delta": 0.3, "epsilon": 0.1, "zeta": -0.1},
        "evolutionary_engine": {"interval_seconds": 3600},
        "tokenization": {},
    }

    integrator = EnhancedSystemIntegrator(config)
    await integrator.initialize()
    await integrator.setup_enhancements()

    # Run demo
    await integrator.run()

    # Keep alive until interrupted
    try:
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        await integrator.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
