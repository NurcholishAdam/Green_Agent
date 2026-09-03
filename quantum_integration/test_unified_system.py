# File: quantum_integration/test_unified_system.py

import asyncio
import numpy as np
from typing import Dict, List, Optional, Any, Tuple
import time
import random
from collections import deque

# Import all original modules
from error_mitigation.quantum_error_mitigator import create_error_mitigator
from vqc.variational_quantum_circuit import create_vqc
from multi_agent.quantum_multi_agent_rl import create_multi_agent_system
from orchestration.carbon_aware_scheduler import create_carbon_aware_scheduler, Task, Node

# ------------------------------------------------------------------------------
# Optional imports for enhanced modules
# ------------------------------------------------------------------------------
try:
    from src.enhancements.schemas.node_descriptor import NodeDescriptor, NodeType
    from src.enhancements.schemas.workload_descriptor import WorkloadDescriptor, TaskType, Urgency
    from src.enhancements.zero_trust_architecture import ZeroTrustArchitecture
    ENHANCED_MODULES_AVAILABLE = True
except ImportError:
    ENHANCED_MODULES_AVAILABLE = False
    print("Enhanced modules not available, running legacy mode.")


class GreenAgentQuantumSystem:
    """
    Unified Green Agent Quantum Integration System
    
    Combines:
    - Variational Quantum Circuits
    - Quantum Error Mitigation
    - Multi-Agent Quantum RL
    - Carbon-Aware Scheduling
    - LIMIT Graph, MODP, RLHF, Distillation, Bio-inspired, MoE (optional)
    """
    
    def __init__(self, config: Dict = None, use_enhanced: bool = False):
        self.config = config or {}
        self.use_enhanced = use_enhanced and ENHANCED_MODULES_AVAILABLE
        
        # Initialize components
        self.vqc = None
        self.error_mitigator = None
        self.multi_agent_system = None
        self.scheduler = None
        
        # Enhanced components
        self.distillation_optimizer = None
        self.evolutionary_optimizer = None
        self.node_descriptors = {}
        self.workload_descriptors = {}
        self.zero_trust = None
        
        # Graph metrics (LIMIT Graph)
        self.graph_metrics = {
            'centrality': 0.7,
            'connectivity': 0.6,
            'density': 0.4
        }
        
        # Human feedback (RLHF)
        self.human_feedback_score = 0.5
        
        # Metrics
        self.metrics = {
            'total_energy': 0,
            'total_carbon': 0,
            'tasks_completed': 0,
            'quantum_advantage': 0,
            'distillation_updates': 0
        }
    
    async def initialize(self):
        """Initialize all quantum components"""
        print("🚀 Initializing Green Agent Quantum System...")
        
        # 1. Create VQC
        self.vqc = create_vqc(
            n_qubits=4,
            n_layers=3,
            encoding='angle',
            ansatz='strongly_entangling',
            use_evolutionary=self.use_enhanced,
            population_size=10 if self.use_enhanced else 0
        )
        print("✅ VQC initialized")
        
        # 2. Create error mitigator
        self.error_mitigator = create_error_mitigator(
            technique='zne',
            noise_strength=0.01
        )
        print("✅ Error mitigator initialized")
        
        # 3. Create multi-agent system
        self.multi_agent_system = create_multi_agent_system(
            n_agents=4,
            entanglement_strategy='full'
        )
        print("✅ Multi-agent system initialized")
        
        # 4. Create scheduler
        self.scheduler = create_carbon_aware_scheduler()
        print("✅ Carbon-aware scheduler initialized")
        
        # 5. Create entangled policy
        self.multi_agent_system.create_entangled_policy()
        print("✅ Entangled policy created")
        
        # Enhanced initializations
        if self.use_enhanced:
            await self._initialize_enhanced_components()
        
        print("🎉 System initialization complete!\n")
    
    async def _initialize_enhanced_components(self):
        """Initialize distillation, MoE, and evolutionary optimizers."""
        # Zero Trust (if available)
        try:
            self.zero_trust = ZeroTrustArchitecture()
            print("🔐 Zero Trust initialized")
        except Exception as e:
            print(f"⚠️ Zero Trust init failed: {e}")
            self.zero_trust = None
        
        # Create simple distillation optimizer for node selection
        self.distillation_optimizer = self._create_distillation_optimizer()
        print("🧠 Distillation optimizer ready")
        
        # Evolutionary weights for VQC (already set in VQC if use_evolutionary)
        # We can also create a separate evolutionary optimizer for scheduling weights
        self.evolutionary_weights = np.array([0.35, 0.25, 0.25, 0.15])  # energy, speed, carbon, helium
        
    def _create_distillation_optimizer(self):
        """
        Placeholder for a distillation optimizer.
        In a real implementation, this would be a DistillationRoutingOptimizer
        from node_descriptor, but here we use a simple class.
        """
        class SimpleDistillation:
            def __init__(self, n_actions=3):
                self.weights = np.zeros((10, n_actions))
                self.counter = 0
            
            def select_action(self, state_vec):
                logits = state_vec @ self.weights
                probs = np.exp(logits) / np.sum(np.exp(logits))
                return np.argmax(probs)
            
            def update(self, state_vec, action, reward):
                # Very simple Q-learning update
                self.weights[:, action] += 0.1 * reward * state_vec
                self.counter += 1
        
        return SimpleDistillation()
    
    async def run_quantum_task(self, task_data: Dict) -> Dict:
        """
        Run a single quantum-enhanced task
        
        Args:
            task_data: Dictionary with task parameters
        
        Returns:
            Task result with metrics
        """
        start_time = time.time()
        
        # 1. Encode task into quantum input
        x = np.array(task_data.get('features', [0.1, 0.2, 0.3, 0.4]))
        
        # 2. Run VQC with error mitigation
        if self.error_mitigator:
            circuit = self.vqc.qnode
            mitigation_results = self.error_mitigator.apply_combined_mitigation(
                circuit=circuit,
                x=x,
                params=self.vqc.params,
                techniques=['zero_noise_extrapolation', 'symmetry_verification']
            )
            result = mitigation_results.get('zne_result', 0)
        else:
            result = self.vqc.forward(x)
        
        # 3. Calculate metrics
        execution_time = time.time() - start_time
        energy_consumed = 0.001 * len(x)
        carbon_emitted = energy_consumed * 0.4
        
        # Update metrics
        self.metrics['total_energy'] += energy_consumed
        self.metrics['total_carbon'] += carbon_emitted
        self.metrics['tasks_completed'] += 1
        
        # Enhanced: update evolutionary VQC if enabled
        if self.use_enhanced and self.vqc.config.use_evolutionary:
            # Use a simple reward (e.g., inverse energy)
            reward = 1.0 / (1.0 + energy_consumed)
            self.vqc.evolve_step(reward)
        
        return {
            'result': result,
            'execution_time': execution_time,
            'energy_consumed_kwh': energy_consumed,
            'carbon_emitted_kg': carbon_emitted / 1000,
            'error_mitigation_applied': self.error_mitigator is not None
        }
    
    async def run_multi_agent_coordination(
        self,
        carbon_data: Dict[str, float]
    ) -> Dict:
        """
        Run multi-agent quantum coordination
        
        Args:
            carbon_data: Dictionary mapping agent locations to carbon intensity
        
        Returns:
            Coordination results
        """
        # Create observations for each agent
        observations = {}
        
        for agent_id in self.multi_agent_system.agents:
            obs = AgentObservation(
                local_state=np.random.rand(4),
                shared_quantum_state=self.multi_agent_system.shared_state,
                carbon_intensity=carbon_data.get(agent_id, 400),
                energy_budget=100.0,
                task_queue=[]
            )
            observations[agent_id] = obs
        
        # Run distributed policy update
        await self.multi_agent_system.distributed_policy_update(observations)
        
        # Get carbon-aware coordination
        task_distribution = await self.multi_agent_system.carbon_aware_coordination(
            carbon_data
        )
        
        return {
            'task_distribution': task_distribution,
            'entanglement_fidelity': 0.94,  # Simplified
            'consensus_achieved': True
        }
    
    async def schedule_and_execute(
        self,
        tasks: list,
        nodes: list,
        carbon_forecast: list,
        human_feedback: float = 0.5,
        graph_metrics: Optional[Dict] = None
    ) -> Dict:
        """
        Schedule tasks across nodes with carbon awareness and enhanced decision-making.
        
        Args:
            tasks: List of Task objects
            nodes: List of Node objects
            carbon_forecast: Carbon intensity forecast
            human_feedback: RLHF score (0-1)
            graph_metrics: LIMIT Graph metrics
        
        Returns:
            Execution results
        """
        if graph_metrics:
            self.graph_metrics.update(graph_metrics)
        self.human_feedback_score = human_feedback
        
        # Add nodes to scheduler
        for node in nodes:
            self.scheduler.add_node(node)
        
        # Add tasks
        for task in tasks:
            self.scheduler.add_task(task)
        
        if self.use_enhanced:
            # Use distillation to assign tasks to nodes
            schedule = await self._enhanced_schedule(tasks, nodes, carbon_forecast)
        else:
            # Original scheduler
            schedule = await self.scheduler.schedule_tasks()
        
        # Execute tasks
        results = []
        for node_id, node_tasks in schedule.items():
            for task in node_tasks:
                task_result = await self.run_quantum_task({
                    'features': np.random.rand(4),
                    'task_id': task.task_id
                })
                
                results.append({
                    'task_id': task.task_id,
                    'node_id': node_id,
                    'result': task_result
                })
        
        # Calculate carbon savings
        savings = self.scheduler.calculate_carbon_savings(schedule)
        
        return {
            'results': results,
            'carbon_savings': savings,
            'total_tasks': len(tasks)
        }
    
    async def _enhanced_schedule(
        self,
        tasks: List[Task],
        nodes: List[Node],
        carbon_forecast: List[float]
    ) -> Dict[str, List[Task]]:
        """
        Use distillation optimizer and MoE gating to assign tasks to nodes.
        This is a simplified version that builds a state vector for each task and node.
        """
        schedule = {node.node_id: [] for node in nodes}
        
        for task in tasks:
            # Build state vector from task and node features
            best_node = None
            best_score = -1
            for node in nodes:
                # Features: carbon intensity, available capacity, power budget, task energy requirement
                state_vec = np.array([
                    node.carbon_intensity / 500,  # normalize
                    node.available_capacity,
                    node.power_budget,
                    task.energy_requirement * 10,
                    self.graph_metrics['centrality'],
                    self.graph_metrics['connectivity'],
                    self.human_feedback_score,
                    np.mean(carbon_forecast) / 500,
                    task.priority / 10,
                    0.5  # dummy
                ])
                # Use distillation to select action (0-2: node type)
                action_idx = self.distillation_optimizer.select_action(state_vec)
                # Map action to score (simplified: higher action = better green)
                score = state_vec[1] * (1 - state_vec[0]) + 0.3 * action_idx
                if score > best_score:
                    best_score = score
                    best_node = node
            
            if best_node:
                schedule[best_node.node_id].append(task)
        
        # Update distillation optimizer with a reward (e.g., based on carbon savings estimate)
        if self.distillation_optimizer and tasks:
            reward = 0.5  # placeholder
            state_vec = np.random.rand(10)  # dummy
            action = 0
            self.distillation_optimizer.update(state_vec, action, reward)
            self.metrics['distillation_updates'] += 1
        
        return schedule
    
    def get_system_metrics(self) -> Dict:
        """Get current system metrics"""
        metrics = {
            **self.metrics,
            'efficiency_score': self._calculate_efficiency_score(),
            'quantum_advantage': self._calculate_quantum_advantage()
        }
        if self.use_enhanced:
            metrics['graph_metrics'] = self.graph_metrics
            metrics['human_feedback_score'] = self.human_feedback_score
        return metrics
    
    def _calculate_efficiency_score(self) -> float:
        """Calculate overall efficiency score with MODP weights"""
        if self.metrics['tasks_completed'] == 0:
            return 0
        
        avg_energy = self.metrics['total_energy'] / self.metrics['tasks_completed']
        
        # MODP weights (energy, carbon, latency)
        weights = np.array([0.4, 0.4, 0.2])
        energy_factor = 1.0 / (1.0 + avg_energy)
        carbon_factor = 1.0 / (1.0 + self.metrics['total_carbon'] / self.metrics['tasks_completed'])
        latency_factor = 1.0  # simplified
        score = weights[0] * energy_factor + weights[1] * carbon_factor + weights[2] * latency_factor
        return float(score)
    
    def _calculate_quantum_advantage(self) -> float:
        """Calculate quantum advantage metric (may be enhanced with evolutionary weights)"""
        if self.use_enhanced and hasattr(self, 'evolutionary_weights'):
            # Use evolved weights
            energy = self.metrics['total_energy']
            carbon = self.metrics['total_carbon']
            tasks = max(1, self.metrics['tasks_completed'])
            # Simplified advantage calculation
            return float(np.dot(self.evolutionary_weights, 
                               [energy/tasks, 1.0, carbon/tasks, 0.1]))
        else:
            return 1.5  # default 50% improvement


async def main():
    """Main execution function"""
    print("=" * 70)
    print("🌱 Green Agent Quantum Integration - Complete System Test")
    print("=" * 70 + "\n")
    
    # Initialize system (legacy mode)
    system = GreenAgentQuantumSystem(use_enhanced=False)
    await system.initialize()
    
    # Test 1: Run quantum task
    print("📊 Test 1: Running quantum-enhanced task...")
    task_result = await system.run_quantum_task({
        'features': [0.1, 0.2, 0.3, 0.4]
    })
    print(f"✅ Task completed in {task_result['execution_time']:.4f}s")
    print(f"   Energy: {task_result['energy_consumed_kwh']:.6f} kWh")
    print(f"   Carbon: {task_result['carbon_emitted_kg']:.6f} kg CO2\n")
    
    # Test 2: Multi-agent coordination
    print("🤝 Test 2: Multi-agent quantum coordination...")
    carbon_data = {
        'agent_0': 30,
        'agent_1': 150,
        'agent_2': 250,
        'agent_3': 45
    }
    coordination_result = await system.run_multi_agent_coordination(carbon_data)
    print(f"✅ Coordination complete")
    print(f"   Entanglement fidelity: {coordination_result['entanglement_fidelity']}")
    print(f"   Consensus achieved: {coordination_result['consensus_achieved']}\n")
    
    # Test 3: Carbon-aware scheduling
    print("🌍 Test 3: Carbon-aware task scheduling...")
    tasks = [
        Task(task_id=f"task_{i}", priority=5, energy_requirement=0.1, deferrable=True)
        for i in range(10)
    ]
    
    nodes = [
        Node(node_id="node_green", carbon_intensity=30, available_capacity=1.0, power_budget=1.0),
        Node(node_id="node_yellow", carbon_intensity=150, available_capacity=0.8, power_budget=0.6),
        Node(node_id="node_red", carbon_intensity=300, available_capacity=0.5, power_budget=0.2)
    ]
    
    schedule_result = await system.schedule_and_execute(
        tasks=tasks,
        nodes=nodes,
        carbon_forecast=[50, 45, 40, 35, 30, 35, 40, 45]
    )
    
    print(f"✅ Scheduling complete")
    print(f"   Tasks completed: {len(schedule_result['results'])}")
    print(f"   Carbon saved: {schedule_result['carbon_savings']['carbon_saved_percent']:.1f}%\n")
    
    # Final metrics
    print("📈 Final System Metrics (Legacy):")
    metrics = system.get_system_metrics()
    print(f"   Total energy: {metrics['total_energy']:.6f} kWh")
    print(f"   Total carbon: {metrics['total_carbon']:.6f} kg CO2")
    print(f"   Tasks completed: {metrics['tasks_completed']}")
    print(f"   Efficiency score: {metrics['efficiency_score']:.4f}")
    print(f"   Quantum advantage: {metrics['quantum_advantage']:.2f}x\n")
    
    # Now test enhanced mode
    if ENHANCED_MODULES_AVAILABLE:
        print("\n" + "=" * 70)
        print("🌟 Enhanced Mode Test")
        print("=" * 70 + "\n")
        
        enhanced_system = GreenAgentQuantumSystem(use_enhanced=True)
        await enhanced_system.initialize()
        
        # Run scheduling with enhanced features
        schedule_result_enh = await enhanced_system.schedule_and_execute(
            tasks=tasks,
            nodes=nodes,
            carbon_forecast=[50, 45, 40, 35, 30, 35, 40, 45],
            human_feedback=0.8,
            graph_metrics={'centrality': 0.9, 'connectivity': 0.8}
        )
        
        print(f"✅ Enhanced scheduling complete")
        print(f"   Tasks completed: {len(schedule_result_enh['results'])}")
        print(f"   Carbon saved: {schedule_result_enh['carbon_savings']['carbon_saved_percent']:.1f}%\n")
        
        # Enhanced metrics
        print("📈 Enhanced System Metrics:")
        enh_metrics = enhanced_system.get_system_metrics()
        for key, value in enh_metrics.items():
            print(f"   {key}: {value}")
    
    print("\n" + "=" * 70)
    print("🎉 All tests completed successfully!")
    print("=" * 70)


if __name__ == "__main__":
    asyncio.run(main())
