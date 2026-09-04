# File: run_enhanced_agent_with_moe.py

"""
Enhanced Green Agent Runner with MoE Integration and Advanced Enhancements
Combines all latest capabilities with Mixture of Experts, LIMIT Graph,
MODP, RLHF, Multi‑Teacher On‑Policy Distillation, Bio‑inspired Optimisation,
and FlexGen integration hooks.
"""

import asyncio
import logging
import sys
import os
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime
import json

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import MoE system
from enhancements.moe_expert_system import (
    ExpertRegistry,
    MoEGatingNetwork,
    ExpertRouter,
    ExpertMetricsCollector,
    LayerIntegrator
)

from enhancements.moe_expert_system.integration.enhanced_work_integration import (
    EnhancedWorkIntegrator,
    WorkContext
)

from enhancements.moe_expert_system.integration.quantum_limit_integration import (
    QuantumLimitGraphIntegrator
)

# ------------------------------------------------------------------------------
# Optional imports for advanced enhancements (graceful fallback)
# ------------------------------------------------------------------------------
try:
    from src.enhancements.schemas.node_descriptor import (
        NodeDescriptor, NodeType, CoolingType, MaintenanceStatus, RoutingStrategy
    )
    from src.enhancements.schemas.workload_descriptor import (
        WorkloadDescriptor, TaskType, Urgency, Priority, BioMode
    )
    from src.enhancements.schemas.feedback_event import FeedbackEvent
    from src.enhancements.zero_trust_architecture import (
        ZeroTrustArchitecture, ZeroTrustConfig
    )
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
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('green_agent_moe.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

class EnhancedGreenAgentWithMoE:
    """
    Enhanced Green Agent with full MoE integration and optional advanced
    decision-making modules from the `enhancements` folder.
    """

    def __init__(
        self,
        enable_quantum: bool = True,
        config_path: Optional[str] = None
    ):
        logger.info("Initializing Enhanced Green Agent with MoE and Advanced Enhancements")

        # Load configuration
        self.config = self._load_config(config_path)

        # Initialize MoE system
        self.metrics_collector = ExpertMetricsCollector()
        self.expert_router = ExpertRouter(
            enable_quantum=enable_quantum,
            metrics_collector=self.metrics_collector
        )

        # Initialize integrations
        self.quantum_limiter = QuantumLimitGraphIntegrator()
        self.work_integrator = EnhancedWorkIntegrator(
            expert_router=self.expert_router,
            quantum_module=self.quantum_limiter if enable_quantum else None
        )

        # Initialize layer integrator
        self.layer_integrator = LayerIntegrator(self.expert_router)

        # ------------------------------------------------------------------
        # Advanced enhancement components (optional)
        # ------------------------------------------------------------------
        self.use_enhancements = self.config.get('use_enhancements', True) and ENHANCEMENTS_AVAILABLE
        self.graph_registry = None
        self.causal_graph = None
        self.meta_cognition = None
        self.node_descriptor = None
        self.workload_descriptor = None
        self.feedback_event = None
        self.zero_trust = None
        self.dag_carbon_ledger = None

        if self.use_enhancements:
            self._initialize_enhancement_modules()

        # Performance tracking
        self.start_time = datetime.utcnow()
        self.processed_tasks = 0

        logger.info("Enhanced Green Agent with MoE and enhancements initialized successfully")

    def _initialize_enhancement_modules(self):
        """Initialize LIMIT Graph, MODP, RLHF, distillation, etc. modules."""
        try:
            # Graph registry for LIMIT Graph
            self.graph_registry = GraphRegistry()
            self.causal_graph = self.graph_registry.get_or_create(GraphType.CAUSAL)
            self.meta_cognition = MetaCognitionLayer(causal_graph=self.causal_graph)

            # Node descriptor for routing (uses distillation + MoE + RLHF + LIMIT Graph)
            region = self.config.get('region', 'us-east')
            carbon_intensity = self.config.get('carbon_intensity', 400.0)
            self.node_descriptor = NodeDescriptor(
                id="main_agent_node",
                type=NodeType.EDGE,
                region=region,
                region_carbon_intensity=carbon_intensity,
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
                task_id="adaptive_task",
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

            # Zero Trust security (optional)
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
            logger.error(f"Failed to initialize enhancement modules: {e}")
            self.use_enhancements = False

    def _load_config(self, config_path: Optional[str]) -> Dict[str, Any]:
        """Load configuration from file"""
        default_config = {
            'enable_quantum': True,
            'max_carbon_budget_kg': 0.1,
            'max_helium_budget': 0.05,
            'max_latency_ms': 1000,
            'reflection_enabled': True,
            'monitoring_enabled': True,
            'pipeline_default': 'standard',
            # Advanced enhancement settings
            'use_enhancements': True,
            'region': 'us-east',
            'carbon_intensity': 400.0,
            'graph_metrics': {'centrality': 0.7, 'connectivity': 0.6},
            'human_feedback_score': 0.6,
            'use_evolutionary': True,
            'distillation_epsilon': 0.1,
            'gating_lr': 0.005,
            'enable_zero_trust': True,
        }

        if config_path and os.path.exists(config_path):
            with open(config_path, 'r') as f:
                user_config = json.load(f)
                default_config.update(user_config)

        return default_config

    async def process_task(
        self,
        task: Dict[str, Any],
        pipeline_type: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Process a task through the enhanced Green Agent with MoE and
        advanced decision modules.
        """
        self.processed_tasks += 1

        # Select pipeline
        if pipeline_type is None:
            pipeline_type = self._select_pipeline(task)

        logger.info(f"Processing task {task.get('task_id')} with {pipeline_type} pipeline")

        # ------------------------------------------------------------------
        # Advanced enhancement pre-processing
        # ------------------------------------------------------------------
        enhanced_context = {}
        if self.use_enhancements:
            enhanced_context = await self._enhance_task_with_modules(task)

        # Process through enhanced work integrator
        result = await self.work_integrator.process_work(
            work_request=task,
            pipeline_type=pipeline_type
        )

        # Add performance metrics
        result['agent_metadata'] = {
            'agent_version': '2.4.0-moe-enhanced',
            'processed_tasks': self.processed_tasks,
            'uptime_seconds': (datetime.utcnow() - self.start_time).total_seconds(),
            'pipeline_used': pipeline_type,
            'enhanced_decision': enhanced_context
        }

        # ------------------------------------------------------------------
        # Advanced enhancement post-processing
        # ------------------------------------------------------------------
        if self.use_enhancements:
            await self._record_outcome_with_enhancements(task, result, pipeline_type)

        return result

    def _select_pipeline(self, task: Dict[str, Any]) -> str:
        """Select appropriate pipeline based on task characteristics."""
        # Use advanced decision if available
        if self.use_enhancements and self.node_descriptor and self.workload_descriptor:
            # Run async selection (we're in sync method; we'll use asyncio.run)
            try:
                strategy = asyncio.run(self.node_descriptor.select_routing_strategy(exploration=False))
                priority = asyncio.run(self.workload_descriptor.select_priority(exploration=False))
                # Map strategy/priority to pipeline
                if strategy == "carbon_first" or priority == "green":
                    return 'helium_optimized'  # green pipeline
                elif strategy == "latency_first" or priority == "accuracy":
                    return 'meta_cognitive'    # high-complexity pipeline
                else:
                    return 'standard'
            except Exception:
                pass  # fallback to heuristics

        # Fallback to original heuristics
        if task.get('quantum_capable') and task.get('use_quantum'):
            return 'quantum_enhanced'
        if task.get('helium_dependency', 0) > 0.5:
            return 'helium_optimized'
        if task.get('complexity', 0) > 0.7:
            return 'meta_cognitive'
        return self.config.get('pipeline_default', 'standard')

    async def _enhance_task_with_modules(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Use advanced modules to enhance task context and decisions."""
        context = {}

        # Update workload descriptor with task info
        if self.workload_descriptor:
            try:
                self.workload_descriptor.estimated_energy_joules = task.get('estimated_energy', 0.001)
                self.workload_descriptor.estimated_carbon_kg = task.get('estimated_carbon', 0.0002)
                self.workload_descriptor.tokens = task.get('token_count', 1000)
                self.workload_descriptor.latency_target = task.get('max_latency_ms', 500.0)
                # Select priority
                priority = await self.workload_descriptor.select_priority(exploration=False)
                context['selected_priority'] = priority
            except Exception as e:
                logger.warning(f"Workload descriptor error: {e}")

        # Update node descriptor with current carbon
        if self.node_descriptor:
            try:
                # Simulate current carbon intensity from task or config
                carbon = task.get('carbon_intensity', self.config.get('carbon_intensity', 400.0))
                self.node_descriptor.region_carbon_intensity = carbon
                strategy = await self.node_descriptor.select_routing_strategy(exploration=False)
                context['selected_strategy'] = strategy
            except Exception as e:
                logger.warning(f"Node descriptor error: {e}")

        # Use causal graph for root-cause if anomaly detected
        if self.meta_cognition and self.causal_graph:
            try:
                # Simulate telemetry update
                self.meta_cognition.observe_snapshot({
                    "CarbonIntensity": task.get('carbon_intensity', 400.0),
                    "CarbonIntensity_high": 400.0,
                    "GridStrain": task.get('grid_strain', 0.5),
                })
                diagnosis = self.meta_cognition.diagnose()
                if diagnosis['status'] == 'anomaly_detected':
                    context['diagnosis'] = diagnosis['recommended_action']
            except Exception as e:
                logger.warning(f"Meta-cognition error: {e}")

        return context

    async def _record_outcome_with_enhancements(self, task, result, pipeline_type):
        """Record outcome in advanced modules for learning and audit."""
        try:
            # Update node descriptor outcome
            if self.node_descriptor:
                await self.node_descriptor.record_outcome(
                    carbon_saved_kg=result.get('carbon_saved', 0.0),
                    latency_ms=result.get('latency_ms', 100),
                    cost_usd=result.get('cost_usd', 0.001)
                )

            # Update workload descriptor outcome
            if self.workload_descriptor:
                await self.workload_descriptor.record_outcome(
                    latency_achieved_ms=result.get('latency_ms', 100),
                    carbon_saved_kg=result.get('carbon_saved', 0.0),
                    energy_used_joules=result.get('energy_joules', 0.001)
                )

            # Add to DAG carbon ledger
            if self.dag_carbon_ledger:
                node_id = self.dag_carbon_ledger.add_execution(
                    task_id=task.get('task_id', 'unknown'),
                    framework='moe_agent',
                    energy_kwh=result.get('energy_kwh', 0.001),
                    carbon_co2e_kg=result.get('carbon_kg', 0.0001),
                    accuracy=result.get('accuracy', 0.9),
                    sustainability_index=result.get('sustainability_index', 0.8),
                    metadata={'pipeline': pipeline_type}
                )
                # Backpropagate carbon to ancestors if any (simplified: no ancestors)
                self.dag_carbon_ledger.backpropagate_carbon(node_id, transfer_rate=0.3)

            # Create FeedbackEvent
            if FeedbackEvent is not None:
                event = FeedbackEvent(
                    source="run_enhanced_agent_with_moe",
                    feedback_type="routing",
                    task_id=task.get('task_id', 'unknown'),
                    context={"pipeline": pipeline_type},
                    action={"selected_action": pipeline_type, "selected_rank": 1},
                    performance={
                        "quality_score": result.get('accuracy', 0.0),
                        "latency_ms": result.get('latency_ms', 100),
                        "energy_joules": result.get('energy_joules', 100),
                        "carbon_g": result.get('carbon_kg', 0.0) * 1000,
                        "helium_cost": result.get('helium_usage', 0.0),
                        "duration_ms": result.get('latency_ms', 100)
                    },
                    adaptive_cost_value=result.get('sustainability_index', 0.0),
                    graph_metrics=self.config.get('graph_metrics', {}),
                    human_feedback_score=self.config.get('human_feedback_score', 0.6),
                    modp_score=result.get('modp_score', 0.5),
                    distillation_stats={
                        "student_counter": self.node_descriptor._routing_optimizer.counter if self.node_descriptor and hasattr(self.node_descriptor, '_routing_optimizer') else 0
                    }
                )
                # In production, publish to message queue; here we just log
                logger.debug(f"FeedbackEvent created: {event.event_id}")

        except Exception as e:
            logger.warning(f"Failed to record outcome in enhancements: {e}")

    async def batch_process(
        self,
        tasks: List[Dict[str, Any]],
        max_concurrent: int = 5
    ) -> List[Dict[str, Any]]:
        """
        Process multiple tasks concurrently
        """
        logger.info(f"Batch processing {len(tasks)} tasks with max {max_concurrent} concurrent")

        # Create semaphore for concurrency control
        semaphore = asyncio.Semaphore(max_concurrent)

        async def process_with_semaphore(task):
            async with semaphore:
                return await self.process_task(task)

        # Process all tasks
        tasks_coroutines = [process_with_semaphore(task) for task in tasks]
        results = await asyncio.gather(*tasks_coroutines, return_exceptions=True)

        # Handle exceptions
        processed_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Task {i} failed: {str(result)}")
                processed_results.append({
                    'success': False,
                    'error': str(result),
                    'task_index': i
                })
            else:
                processed_results.append(result)

        return processed_results

    def get_status(self) -> Dict[str, Any]:
        """Get comprehensive agent status"""
        status = {
            'status': 'running',
            'uptime_seconds': (datetime.utcnow() - self.start_time).total_seconds(),
            'processed_tasks': self.processed_tasks,
            'routing_stats': self.expert_router.get_routing_stats(),
            'work_stats': self.work_integrator.get_work_statistics(),
            'planetary_boundaries': self.quantum_limiter.get_planetary_boundary_status(),
            'integration_status': self.layer_integrator.get_integration_status(),
            'metrics_summary': self.metrics_collector.get_metrics_summary()
        }
        if self.use_enhancements:
            status['enhancement_status'] = {
                'node_descriptor': self.node_descriptor is not None,
                'workload_descriptor': self.workload_descriptor is not None,
                'causal_graph': self.causal_graph is not None,
                'zero_trust': self.zero_trust is not None,
                'dag_carbon_ledger': self.dag_carbon_ledger is not None
            }
        return status

    def export_metrics_prometheus(self) -> str:
        """Export metrics in Prometheus format"""
        return self.metrics_collector.to_prometheus_format()

    async def shutdown(self):
        """Graceful shutdown"""
        logger.info("Shutting down Enhanced Green Agent with MoE and enhancements")

        # Export final metrics
        final_metrics = self.get_status()

        with open('green_agent_moe_final_metrics.json', 'w') as f:
            json.dump(final_metrics, f, indent=2, default=str)

        # Optionally save enhancement state
        if self.use_enhancements and self.node_descriptor:
            # Could save Q-weights, etc.
            pass

        logger.info("Shutdown complete")

async def main():
    """Main entry point for enhanced Green Agent with MoE and advanced enhancements"""

    # Initialize agent
    agent = EnhancedGreenAgentWithMoE(
        enable_quantum=True,
        config_path='config.json'
    )

    # Example tasks
    tasks = [
        {
            'task_id': 'task_001',
            'task_type': 'inference',
            'priority': 1,
            'complexity': 0.3,
            'helium_dependency': 0.2,
            'carbon_zone': 2,
            'max_carbon_budget': 0.05,
            'max_helium_budget': 0.02,
            'max_latency_ms': 100,
            'meta_cognitive_state': {
                'historical_success_rate': 0.95,
                'carbon_budget_remaining': 0.05,
                'helium_budget_remaining': 0.02
            }
        },
        {
            'task_id': 'task_002',
            'task_type': 'optimization',
            'priority': 2,
            'complexity': 0.8,
            'helium_dependency': 0.7,
            'carbon_zone': 8,
            'quantum_capable': True,
            'use_quantum': True,
            'max_carbon_budget': 0.1,
            'max_helium_budget': 0.05,
            'max_latency_ms': 500
        },
        {
            'task_id': 'task_003',
            'task_type': 'data_processing',
            'priority': 1,
            'complexity': 0.5,
            'helium_dependency': 0.4,
            'carbon_zone': 4,
            'max_carbon_budget': 0.08,
            'max_helium_budget': 0.03,
            'max_latency_ms': 200
        }
    ]

    # Process tasks
    print("Processing individual tasks...")
    for task in tasks:
        result = await agent.process_task(task)
        print(f"\nTask {task['task_id']} Result:")
        print(f"  Success: {result.get('success')}")
        print(f"  Action: {result.get('final_plan', {}).get('action')}")
        print(f"  Experts Used: {len(result.get('plans', []))}")
        if result.get('quantum_enhanced'):
            print(f"  Quantum Enhanced: Yes")
        if 'enhanced_decision' in result.get('agent_metadata', {}):
            print(f"  Enhanced Decision: {result['agent_metadata']['enhanced_decision']}")

    # Batch processing
    print("\n\nBatch Processing...")
    batch_results = await agent.batch_process(tasks, max_concurrent=3)
    print(f"Batch Results: {len(batch_results)} tasks completed")

    # Get status
    print("\n\nAgent Status:")
    status = agent.get_status()
    print(f"  Uptime: {status['uptime_seconds']:.2f} seconds")
    print(f"  Tasks Processed: {status['processed_tasks']}")
    print(f"  Load Balance Score: {status['routing_stats'].get('load_balance_score', 0):.2f}")
    print(f"  Success Rate: {status['routing_stats'].get('success_rate', 0):.2%}")
    if 'enhancement_status' in status:
        print(f"  Enhancement Status: {status['enhancement_status']}")

    # Export metrics
    prometheus_metrics = agent.export_metrics_prometheus()
    with open('metrics.prom', 'w') as f:
        f.write(prometheus_metrics)

    print("\nMetrics exported to metrics.prom")

    # Shutdown
    await agent.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
