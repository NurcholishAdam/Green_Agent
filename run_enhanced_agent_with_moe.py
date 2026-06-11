# File: run_enhanced_agent_with_moe.py

"""
Enhanced Green Agent Runner with MoE Integration
Combines all latest capabilities with Mixture of Experts
"""

import asyncio
import logging
import sys
import os
from typing import Dict, Any, Optional
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
    Enhanced Green Agent with full MoE integration
    """
    
    def __init__(
        self,
        enable_quantum: bool = True,
        config_path: Optional[str] = None
    ):
        logger.info("Initializing Enhanced Green Agent with MoE")
        
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
        
        # Performance tracking
        self.start_time = datetime.utcnow()
        self.processed_tasks = 0
        
        logger.info("Enhanced Green Agent with MoE initialized successfully")
    
    def _load_config(self, config_path: Optional[str]) -> Dict[str, Any]:
        """Load configuration from file"""
        default_config = {
            'enable_quantum': True,
            'max_carbon_budget_kg': 0.1,
            'max_helium_budget': 0.05,
            'max_latency_ms': 1000,
            'reflection_enabled': True,
            'monitoring_enabled': True,
            'pipeline_default': 'standard'
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
        Process a task through the enhanced Green Agent with MoE
        
        Args:
            task: Task specification
            pipeline_type: Optional pipeline override
            
        Returns:
            Processing results
        """
        self.processed_tasks += 1
        
        # Select pipeline
        if pipeline_type is None:
            pipeline_type = self._select_pipeline(task)
        
        logger.info(f"Processing task {task.get('task_id')} with {pipeline_type} pipeline")
        
        # Process through enhanced work integrator
        result = await self.work_integrator.process_work(
            work_request=task,
            pipeline_type=pipeline_type
        )
        
        # Add performance metrics
        result['agent_metadata'] = {
            'agent_version': '2.4.0-moe',
            'processed_tasks': self.processed_tasks,
            'uptime_seconds': (datetime.utcnow() - self.start_time).total_seconds(),
            'pipeline_used': pipeline_type
        }
        
        return result
    
    def _select_pipeline(self, task: Dict[str, Any]) -> str:
        """Select appropriate pipeline based on task characteristics"""
        
        # Check for quantum tasks
        if task.get('quantum_capable') and task.get('use_quantum'):
            return 'quantum_enhanced'
        
        # Check for helium-sensitive tasks
        helium_dependency = task.get('helium_dependency', 0)
        if helium_dependency > 0.5:
            return 'helium_optimized'
        
        # Check for tasks requiring reflection
        if task.get('complexity', 0) > 0.7:
            return 'meta_cognitive'
        
        # Default to standard pipeline
        return self.config.get('pipeline_default', 'standard')
    
    async def batch_process(
        self,
        tasks: List[Dict[str, Any]],
        max_concurrent: int = 5
    ) -> List[Dict[str, Any]]:
        """
        Process multiple tasks concurrently
        
        Args:
            tasks: List of task specifications
            max_concurrent: Maximum concurrent tasks
            
        Returns:
            List of results
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
        return {
            'status': 'running',
            'uptime_seconds': (datetime.utcnow() - self.start_time).total_seconds(),
            'processed_tasks': self.processed_tasks,
            'routing_stats': self.expert_router.get_routing_stats(),
            'work_stats': self.work_integrator.get_work_statistics(),
            'planetary_boundaries': self.quantum_limiter.get_planetary_boundary_status(),
            'integration_status': self.layer_integrator.get_integration_status(),
            'metrics_summary': self.metrics_collector.get_metrics_summary()
        }
    
    def export_metrics_prometheus(self) -> str:
        """Export metrics in Prometheus format"""
        return self.metrics_collector.to_prometheus_format()
    
    async def shutdown(self):
        """Graceful shutdown"""
        logger.info("Shutting down Enhanced Green Agent with MoE")
        
        # Export final metrics
        final_metrics = self.get_status()
        
        with open('green_agent_moe_final_metrics.json', 'w') as f:
            json.dump(final_metrics, f, indent=2, default=str)
        
        logger.info("Shutdown complete")

async def main():
    """Main entry point for enhanced Green Agent with MoE"""
    
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
    
    # Export metrics
    prometheus_metrics = agent.export_metrics_prometheus()
    with open('metrics.prom', 'w') as f:
        f.write(prometheus_metrics)
    
    print("\nMetrics exported to metrics.prom")
    
    # Shutdown
    await agent.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
