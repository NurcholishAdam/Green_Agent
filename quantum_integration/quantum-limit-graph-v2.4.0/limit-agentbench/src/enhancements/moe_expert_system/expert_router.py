# File: enhancements/moe_expert_system/expert_router.py

import logging
from typing import Dict, Any, List, Optional, Tuple
import time
from datetime import datetime

from .expert_registry import ExpertRegistry, ExpertDomain, ExpertProfile
from .gating_network import MoEGatingNetwork, GatingContext
from .experts.energy_expert import EnergyExpert
from .experts.data_expert import DataExpert
from .experts.iot_expert import IoTExpert
from .experts.quantum_expert import QuantumExpert
from .experts.helium_expert import HeliumExpert
from .monitoring.expert_metrics import ExpertMetricsCollector

logger = logging.getLogger(__name__)

class ExpertRouter:
    """
    Main Expert Router - Layer 4.5 in the Green Agent architecture.
    
    Orchestrates expert selection and execution while maintaining
    integration with:
    - Layer 1: Meta-Cognition
    - Layer 2: Neuro-Symbolic constraints
    - Layer 3: Dual-Axis Decision Core
    - Layer 7: Dual Monitoring
    - Layer 8: Immutable Ledger
    """
    
    def __init__(
        self,
        enable_quantum: bool = False,
        metrics_collector: Optional[ExpertMetricsCollector] = None
    ):
        # Initialize registry
        self.registry = ExpertRegistry()
        
        # Initialize experts
        self.experts = {
            'energy': EnergyExpert(),
            'data': DataExpert(),
            'iot': IoTExpert(),
            'helium': HeliumExpert()
        }
        
        if enable_quantum:
            self.experts['quantum'] = QuantumExpert()
        
        # Register all experts
        self.expert_index_map = {}  # Maps gating indices to expert IDs
        for idx, (expert_id, expert) in enumerate(self.experts.items()):
            self.registry.register_expert(expert.profile)
            self.expert_index_map[idx] = expert_id
        
        # Initialize gating network
        self.gating_network = MoEGatingNetwork(
            num_experts=len(self.experts),
            top_k=min(2, len(self.experts))
        )
        
        # Initialize metrics collector
        self.metrics_collector = metrics_collector or ExpertMetricsCollector()
        
        # Routing statistics
        self.total_routes = 0
        self.successful_routes = 0
        self.fallback_routes = 0
        
        logger.info(f"Initialized Expert Router with {len(self.experts)} experts")
    
    def route_and_execute(
        self,
        workload_profile: Dict[str, Any],
        meta_cognitive_state: Dict[str, Any],
        dual_axis_context: Dict[str, Any],
        symbolic_constraints: Optional[Dict[str, Any]] = None,
        training_mode: bool = False
    ) -> Dict[str, Any]:
        """
        Main routing and execution method.
        
        This is the primary integration point for the MoE system.
        
        Args:
            workload_profile: From Layer 0 (Workload + Helium Profile)
            meta_cognitive_state: From Layer 1 (Meta-Cognition)
            dual_axis_context: From Layer 3 (Dual-Axis Decision Core)
            symbolic_constraints: From Layer 2 (Neuro-Symbolic)
            training_mode: Whether in training mode
            
        Returns:
            Execution results with expert plans and metadata
        """
        start_time = time.time()
        self.total_routes += 1
        
        try:
            # Step 1: Build gating context from all layers
            gating_context = self._build_gating_context(
                workload_profile,
                meta_cognitive_state,
                dual_axis_context
            )
            
            # Step 2: Apply symbolic constraints from Layer 2
            allowed_expert_indices = self._apply_symbolic_constraints(
                symbolic_constraints
            )
            
            # Step 3: Route through gating network
            routing_decisions = self.gating_network.route(
                gating_context,
                expert_constraints=allowed_expert_indices,
                training=training_mode
            )
            
            # Step 4: Execute top expert plans
            expert_plans = self._execute_experts(
                routing_decisions,
                workload_profile,
                meta_cognitive_state,
                dual_axis_context
            )
            
            # Step 5: Aggregate and score plans
            final_plan = self._aggregate_plans(
                expert_plans,
                dual_axis_context
            )
            
            # Step 6: Record metrics
            execution_time = time.time() - start_time
            self.metrics_collector.record_routing(
                routing_decisions=routing_decisions,
                gating_context=gating_context,
                execution_time=execution_time,
                success=True
            )
            
            self.successful_routes += 1
            
            return {
                'success': True,
                'plans': expert_plans,
                'final_plan': final_plan,
                'routing_decisions': routing_decisions,
                'execution_time': execution_time,
                'metadata': {
                    'expert_count': len(expert_plans),
                    'load_balance': self.gating_network.get_load_balance_score(),
                    'timestamp': datetime.utcnow().isoformat()
                }
            }
            
        except Exception as e:
            logger.error(f"Expert routing failed: {str(e)}", exc_info=True)
            self.fallback_routes += 1
            
            # Return fallback plan
            return self._create_fallback_plan(workload_profile, str(e))
    
    def _build_gating_context(
        self,
        workload_profile: Dict[str, Any],
        meta_cognitive_state: Dict[str, Any],
        dual_axis_context: Dict[str, Any]
    ) -> GatingContext:
        """Build comprehensive gating context from multiple layers"""
        
        # Extract Layer 0 features
        task_type = workload_profile.get('task_type', 'inference')
        task_complexity = workload_profile.get('complexity', 0.5)
        input_size_mb = workload_profile.get('input_size_mb', 1.0)
        
        # Extract Layer 1 features
        carbon_budget = meta_cognitive_state.get('carbon_budget_remaining', 1.0)
        helium_budget = meta_cognitive_state.get('helium_budget_remaining', 1.0)
        latency_budget = meta_cognitive_state.get('latency_budget_ms', 100.0)
        historical_success = meta_cognitive_state.get('historical_success_rate', 0.9)
        
        # Extract Layer 3 features
        carbon_zone = dual_axis_context.get('carbon_zone', 0)
        helium_scarcity = dual_axis_context.get('helium_scarcity', 0.5)
        
        # Additional context
        time_of_day = datetime.utcnow().hour
        grid_intensity = workload_profile.get('grid_carbon_intensity', 400.0)
        hardware_availability = workload_profile.get('hardware_availability', {
            'cpu': 1.0,
            'gpu': 0.8,
            'quantum': 0.0,
            'edge': 0.5
        })
        
        return GatingContext(
            task_type=task_type,
            task_complexity=task_complexity,
            input_size_mb=input_size_mb,
            carbon_budget_remaining=carbon_budget,
            helium_budget_remaining=helium_budget,
            latency_budget_ms=latency_budget,
            historical_success_rate=historical_success,
            carbon_zone=carbon_zone,
            helium_scarcity=helium_scarcity,
            time_of_day=time_of_day,
            grid_carbon_intensity=grid_intensity,
            hardware_availability=hardware_availability
        )
    
    def _apply_symbolic_constraints(
        self,
        symbolic_constraints: Optional[Dict[str, Any]]
    ) -> Optional[List[int]]:
        """Apply neuro-symbolic constraints from Layer 2"""
        if not symbolic_constraints:
            return None
        
        allowed_indices = []
        
        # Apply domain constraints
        blocked_domains = symbolic_constraints.get('blocked_domains', [])
        for idx, expert_id in self.expert_index_map.items():
            if expert_id in self.experts:
                expert = self.experts[expert_id]
                if expert.profile.domain.value not in blocked_domains:
                    allowed_indices.append(idx)
        
        # Apply helium constraints
        max_helium = symbolic_constraints.get('max_helium_per_inference')
        if max_helium is not None:
            allowed_indices = [
                idx for idx in allowed_indices
                if self.experts[self.expert_index_map[idx]].profile.helium_per_inference <= max_helium
            ]
        
        return allowed_indices if allowed_indices else None
    
    def _execute_experts(
        self,
        routing_decisions: List[Tuple[int, float]],
        workload_profile: Dict[str, Any],
        meta_cognitive_state: Dict[str, Any],
        dual_axis_context: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Execute selected experts and collect their plans"""
        plans = []
        
        for expert_idx, weight in routing_decisions:
            expert_id = self.expert_index_map.get(expert_idx)
            if not expert_id or expert_id not in self.experts:
                continue
            
            expert = self.experts[expert_id]
            
            try:
                # Execute expert based on its domain
                if expert_id == 'energy':
                    plan = expert.optimize_energy(
                        task_config=workload_profile,
                        carbon_budget=meta_cognitive_state.get('carbon_budget_remaining', 1.0),
                        latency_requirement_ms=meta_cognitive_state.get('latency_budget_ms', 100.0)
                    )
                elif expert_id == 'data':
                    plan = expert.optimize_data_pipeline(
                        input_size_mb=workload_profile.get('input_size_mb', 1.0),
                        helium_scarcity=dual_axis_context.get('helium_scarcity', 0.5),
                        latency_budget_ms=meta_cognitive_state.get('latency_budget_ms', 100.0)
                    )
                elif expert_id == 'iot':
                    plan = expert.optimize_edge_deployment(
                        device_type=workload_profile.get('device_type', 'edge_node'),
                        carbon_zone=dual_axis_context.get('carbon_zone', 0),
                        helium_scarcity=dual_axis_context.get('helium_scarcity', 0.5)
                    )
                elif expert_id == 'helium':
                    plan = expert.optimize_helium_usage(
                        current_scarcity=dual_axis_context.get('helium_scarcity', 0.5),
                        compute_requirement=workload_profile.get('complexity', 0.5)
                    )
                elif expert_id == 'quantum':
                    plan = expert.solve_quantum_optimization(
                        problem_type=workload_profile.get('task_type', 'optimization'),
                        quantum_available=workload_profile.get('hardware_availability', {}).get('quantum', 0) > 0
                    )
                else:
                    continue
                
                plan['routing_weight'] = float(weight)
                plan['expert_domain'] = expert.profile.domain.value
                plans.append(plan)
                
            except Exception as e:
                logger.error(f"Expert {expert_id} execution failed: {str(e)}")
        
        return plans
    
    def _aggregate_plans(
        self,
        expert_plans: List[Dict[str, Any]],
        dual_axis_context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Aggregate multiple expert plans into a single decision.
        Integrates with Layer 3 (Dual-Axis Decision Core) for final scoring.
        """
        if not expert_plans:
            return {
                'action': 'defer',
                'reason': 'No expert plans available'
            }
        
        # Weight plans by routing weights
        total_weight = sum(plan.get('routing_weight', 0) for plan in expert_plans)
        if total_weight > 0:
            for plan in expert_plans:
                plan['normalized_weight'] = plan.get('routing_weight', 0) / total_weight
        
        # Calculate aggregate metrics
        weighted_carbon = sum(
            plan.get('estimated_carbon_kg', 0) * plan.get('normalized_weight', 0)
            for plan in expert_plans
        )
        
        weighted_energy = sum(
            plan.get('estimated_energy_kwh', 0) * plan.get('normalized_weight', 0)
            for plan in expert_plans
        )
        
        weighted_latency = sum(
            plan.get('estimated_latency_ms', 0) * plan.get('normalized_weight', 0)
            for plan in expert_plans
        )
        
        # Determine execution action based on Layer 3 dual-axis
        carbon_zone = dual_axis_context.get('carbon_zone', 0)
        helium_scarcity = dual_axis_context.get('helium_scarcity', 0.5)
        
        # 16-zone matrix logic
        if carbon_zone >= 12 and helium_scarcity > 0.8:
            action = 'defer'  # Critical zone - defer all
        elif carbon_zone >= 8 or helium_scarcity > 0.6:
            action = 'execute_minimal'
        elif carbon_zone >= 4 or helium_scarcity > 0.3:
            action = 'execute_throttled'
        else:
            action = 'execute_full'
        
        return {
            'action': action,
            'aggregate_carbon_kg': weighted_carbon,
            'aggregate_energy_kwh': weighted_energy,
            'aggregate_latency_ms': weighted_latency,
            'expert_count': len(expert_plans),
            'carbon_zone': carbon_zone,
            'helium_scarcity': helium_scarcity
        }
    
    def _create_fallback_plan(
        self,
        workload_profile: Dict[str, Any],
        error_reason: str
    ) -> Dict[str, Any]:
        """Create fallback plan when routing fails"""
        return {
            'success': False,
            'action': 'execute_minimal',
            'error': error_reason,
            'fallback': True,
            'plans': [],
            'final_plan': {
                'action': 'execute_minimal',
                'reason': f'Fallback due to routing error: {error_reason}',
                'strategy': 'conservative_minimal'
            },
            'metadata': {
                'timestamp': datetime.utcnow().isoformat(),
                'fallback_type': 'routing_failure'
            }
        }
    
    def get_routing_stats(self) -> Dict[str, Any]:
        """Get comprehensive routing statistics"""
        return {
            'total_routes': self.total_routes,
            'successful_routes': self.successful_routes,
            'fallback_routes': self.fallback_routes,
            'success_rate': self.successful_routes / max(self.total_routes, 1),
            'load_balance_score': self.gating_network.get_load_balance_score(),
            'expert_utilization': self.gating_network.get_expert_utilization(),
            'registry_stats': self.registry.get_registry_stats()
        }
