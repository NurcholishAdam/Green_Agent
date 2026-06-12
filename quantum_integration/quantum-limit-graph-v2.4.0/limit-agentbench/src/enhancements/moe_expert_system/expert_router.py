# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/moe_expert_system/expert_router.py

"""
Enhanced Expert Router for Green Agent MoE System
Version: 2.0.0

Advanced expert routing with:
- Adaptive online learning for routing weights
- Multi-objective optimization (carbon, helium, latency, accuracy)
- Expert preheating and predictive loading
- Circuit breaker pattern for expert isolation
- Intelligent load shedding under overload
- Explainable routing decisions
- Collaborative expert execution
- Predictive routing with workload forecasting
- Resource-aware batch processing
- Continuous feedback loop integration
- A/B testing integration for routing strategies
- Dynamic topology adaptation
- Confidence-based routing
- Expert reputation management

Integration Points:
- Layer 0: Workload classification and profiling
- Layer 1: Meta-cognitive feedback and learning
- Layer 2: Neuro-symbolic constraint validation
- Layer 3: Dual-axis multi-objective scoring
- Layer 4: ML model optimization awareness
- Layer 5: Data optimization integration
- Layer 6: Distributed execution coordination
- Layer 7: Comprehensive monitoring
- Layer 8: Immutable routing audit trail
- Layer 9: Pareto-optimal routing analysis
- Layer 10: Quantum routing optimization
- Layer 11: Dashboard and visualization
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple, Set, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from collections import defaultdict, deque
import numpy as np
import time
import hashlib
import json
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger(__name__)

# Try relative imports for integration
try:
    from .expert_registry import (
        ExpertRegistry, ExpertDomain, ExpertProfile, ExpertLifecycleState,
        HardwareProfile
    )
    from .gating_network import MoEGatingNetwork, GatingContext
    from .experts.energy_expert import EnergyExpert
    from .experts.data_expert import DataExpert
    from .experts.iot_expert import IoTExpert
    from .experts.quantum_expert import QuantumExpert
    from .experts.helium_expert import HeliumExpert
    from .monitoring.expert_metrics import ExpertMetricsCollector
except ImportError:
    # Standalone operation
    logger.warning("Running in standalone mode - some features limited")

# ============================================================================
# Enums and Data Classes
# ============================================================================

class RoutingStrategy(Enum):
    """Routing strategies for different scenarios"""
    GREEDY = "greedy"                # Always pick highest scored
    EPSILON_GREEDY = "epsilon_greedy"  # Explore occasionally
    THOMPSON_SAMPLING = "thompson"    # Bayesian exploration
    UCB = "ucb"                      # Upper confidence bound
    MULTI_OBJECTIVE = "multi_objective"  # Pareto-optimal
    ADAPTIVE = "adaptive"            # Dynamic strategy selection

class RouterState(Enum):
    """Router operational states"""
    NORMAL = "normal"
    OVERLOADED = "overloaded"
    DEGRADED = "degraded"
    MAINTENANCE = "maintenance"
    LEARNING = "learning"

class CircuitBreakerState(Enum):
    """Circuit breaker states for expert protection"""
    CLOSED = "closed"          # Normal operation
    OPEN = "open"             # Failing, no requests
    HALF_OPEN = "half_open"   # Testing recovery

@dataclass
class RoutingDecision:
    """Detailed routing decision with explainability"""
    decision_id: str
    task_id: str
    timestamp: datetime
    selected_experts: List[str]
    routing_weights: List[float]
    strategy_used: RoutingStrategy
    confidence_score: float
    alternatives: List[str]
    explanation: Dict[str, Any]
    carbon_estimate_kg: float
    helium_estimate: float
    latency_estimate_ms: float
    dual_axis_score: float

@dataclass
class ExpertCircuitBreaker:
    """Circuit breaker for expert failure protection"""
    expert_id: str
    state: CircuitBreakerState = CircuitBreakerState.CLOSED
    failure_count: int = 0
    success_count: int = 0
    last_failure_time: Optional[datetime] = None
    last_success_time: Optional[datetime] = None
    failure_threshold: int = 5
    recovery_timeout_seconds: int = 30
    half_open_max_requests: int = 3
    half_open_requests: int = 0
    
    def record_success(self):
        """Record successful execution"""
        self.success_count += 1
        self.last_success_time = datetime.utcnow()
        
        if self.state == CircuitBreakerState.HALF_OPEN:
            self.half_open_requests += 1
            if self.half_open_requests >= self.half_open_max_requests:
                self.state = CircuitBreakerState.CLOSED
                self.failure_count = 0
                self.half_open_requests = 0
                logger.info(f"Circuit breaker CLOSED for {self.expert_id}")
    
    def record_failure(self):
        """Record failed execution"""
        self.failure_count += 1
        self.last_failure_time = datetime.utcnow()
        
        if self.state == CircuitBreakerState.CLOSED and self.failure_count >= self.failure_threshold:
            self.state = CircuitBreakerState.OPEN
            logger.warning(f"Circuit breaker OPEN for {self.expert_id} ({self.failure_count} failures)")
        elif self.state == CircuitBreakerState.HALF_OPEN:
            self.state = CircuitBreakerState.OPEN
            logger.warning(f"Circuit breaker RE-OPENED for {self.expert_id}")
    
    def can_execute(self) -> bool:
        """Check if expert can be executed"""
        if self.state == CircuitBreakerState.CLOSED:
            return True
        
        if self.state == CircuitBreakerState.OPEN:
            # Check if recovery timeout has elapsed
            if self.last_failure_time:
                elapsed = (datetime.utcnow() - self.last_failure_time).total_seconds()
                if elapsed >= self.recovery_timeout_seconds:
                    self.state = CircuitBreakerState.HALF_OPEN
                    self.half_open_requests = 0
                    logger.info(f"Circuit breaker HALF_OPEN for {self.expert_id}")
                    return True
            return False
        
        return True  # HALF_OPEN allows requests
    
    def get_health_status(self) -> Dict[str, Any]:
        """Get circuit breaker health status"""
        return {
            'state': self.state.value,
            'failure_count': self.failure_count,
            'success_count': self.success_count,
            'failure_rate': (
                self.failure_count / max(self.failure_count + self.success_count, 1)
            ),
            'can_execute': self.can_execute()
        }

@dataclass
class RoutingMetrics:
    """Comprehensive routing performance metrics"""
    total_routes: int = 0
    successful_routes: int = 0
    failed_routes: int = 0
    fallback_routes: int = 0
    average_latency_ms: float = 0.0
    average_confidence: float = 0.0
    expert_usage: Dict[str, int] = field(default_factory=dict)
    strategy_usage: Dict[str, int] = field(default_factory=dict)
    load_shedding_events: int = 0
    circuit_breaker_events: int = 0
    
    @property
    def success_rate(self) -> float:
        return self.successful_routes / max(self.total_routes, 1)
    
    @property
    def fallback_rate(self) -> float:
        return self.fallback_routes / max(self.total_routes, 1)

# ============================================================================
# Enhanced Expert Router
# ============================================================================

class ExpertRouter:
    """
    Enhanced Expert Router - Layer 4.5 in Green Agent architecture.
    
    Advanced routing capabilities:
    - Adaptive online learning for routing optimization
    - Multi-objective routing (carbon, helium, latency, accuracy)
    - Circuit breaker pattern for expert resilience
    - Intelligent load shedding under overload
    - Explainable routing decisions
    - Predictive expert preheating
    - Collaborative expert execution
    - Continuous feedback integration
    - A/B testing for routing strategies
    - Dynamic topology adaptation
    """
    
    def __init__(
        self,
        enable_quantum: bool = False,
        metrics_collector: Optional['ExpertMetricsCollector'] = None,
        routing_strategy: RoutingStrategy = RoutingStrategy.ADAPTIVE,
        max_concurrent_routes: int = 100,
        enable_circuit_breaker: bool = True,
        enable_load_shedding: bool = True,
        enable_preheating: bool = True,
        enable_explainability: bool = True,
        enable_collaboration: bool = True
    ):
        # Core components
        self.registry = ExpertRegistry()
        self.metrics_collector = metrics_collector
        self.routing_strategy = routing_strategy
        self.state = RouterState.NORMAL
        
        # Configuration
        self.max_concurrent_routes = max_concurrent_routes
        self.enable_circuit_breaker = enable_circuit_breaker
        self.enable_load_shedding = enable_load_shedding
        self.enable_preheating = enable_preheating
        self.enable_explainability = enable_explainability
        self.enable_collaboration = enable_collaboration
        
        # Experts
        self.experts: Dict[str, Any] = {}
        self.expert_index_map: Dict[int, str] = {}
        
        # Circuit breakers
        self.circuit_breakers: Dict[str, ExpertCircuitBreaker] = {}
        
        # Initialize experts
        self._initialize_experts(enable_quantum)
        
        # Gating network
        self.gating_network = MoEGatingNetwork(
            num_experts=len(self.experts),
            top_k=min(2, len(self.experts))
        )
        
        # Routing metrics
        self.metrics = RoutingMetrics()
        
        # Routing history for learning
        self.routing_history: deque = deque(maxlen=10000)
        
        # Preheating queue
        self.preheat_queue: List[str] = []
        
        # Load shedding configuration
        self.load_threshold = 0.8  # Start shedding at 80% capacity
        self.active_routes = 0
        self._route_lock = asyncio.Lock()
        
        # Strategy performance tracking
        self.strategy_performance: Dict[str, List[float]] = defaultdict(list)
        
        # Collaboration graph
        self.collaboration_scores: Dict[Tuple[str, str], float] = {}
        
        # Start background tasks
        self._start_background_tasks()
        
        logger.info(
            f"Enhanced Expert Router initialized with {len(self.experts)} experts, "
            f"strategy={routing_strategy.value}"
        )
    
    def _initialize_experts(self, enable_quantum: bool):
        """Initialize all available experts"""
        try:
            self.experts = {
                'energy': EnergyExpert(),
                'data': DataExpert(),
                'iot': IoTExpert(),
                'helium': HeliumExpert()
            }
            
            if enable_quantum:
                self.experts['quantum'] = QuantumExpert()
            
            # Register and index experts
            for idx, (expert_id, expert) in enumerate(self.experts.items()):
                self.registry.register_expert(expert.profile, validate=False)
                self.registry.activate_expert(expert.profile.expert_id)
                self.expert_index_map[idx] = expert_id
                
                # Initialize circuit breaker
                self.circuit_breakers[expert_id] = ExpertCircuitBreaker(
                    expert_id=expert_id
                )
            
            logger.info(f"Initialized {len(self.experts)} experts")
            
        except Exception as e:
            logger.error(f"Failed to initialize experts: {str(e)}")
            raise
    
    def _start_background_tasks(self):
        """Start background maintenance tasks"""
        asyncio.create_task(self._adaptive_learning_loop())
        asyncio.create_task(self._preheating_loop())
        asyncio.create_task(self._health_check_loop())
        asyncio.create_task(self._metrics_reporting_loop())
    
    # ========================================================================
    # Primary Routing Method
    # ========================================================================
    
    async def route_and_execute(
        self,
        workload_profile: Dict[str, Any],
        meta_cognitive_state: Dict[str, Any],
        dual_axis_context: Dict[str, Any],
        symbolic_constraints: Optional[Dict[str, Any]] = None,
        training_mode: bool = False,
        collaboration_enabled: Optional[bool] = None,
        explainability_required: bool = False
    ) -> Dict[str, Any]:
        """
        Enhanced routing and execution with comprehensive features.
        
        Args:
            workload_profile: Layer 0 workload classification
            meta_cognitive_state: Layer 1 meta-cognitive context
            dual_axis_context: Layer 3 dual-axis parameters
            symbolic_constraints: Layer 2 neuro-symbolic constraints
            training_mode: Whether in training mode
            collaboration_enabled: Override collaboration setting
            explainability_required: Generate detailed explanations
            
        Returns:
            Comprehensive routing and execution results
        """
        start_time = time.time()
        route_id = hashlib.md5(
            f"{workload_profile}{start_time}{np.random.random()}".encode()
        ).hexdigest()[:12]
        
        self.metrics.total_routes += 1
        
        # Check load shedding
        if self.enable_load_shedding and self._should_shed_load():
            self.metrics.load_shedding_events += 1
            return self._create_load_shed_response(workload_profile)
        
        # Acquire route slot
        async with self._route_lock:
            self.active_routes += 1
        
        try:
            # Step 1: Build enhanced gating context
            gating_context = self._build_enhanced_gating_context(
                workload_profile,
                meta_cognitive_state,
                dual_axis_context
            )
            
            # Step 2: Apply symbolic constraints from Layer 2
            allowed_expert_indices = self._apply_symbolic_constraints(
                symbolic_constraints
            )
            
            # Step 3: Filter out circuit-broken experts
            available_indices = self._filter_circuit_broken(allowed_expert_indices)
            
            if not available_indices:
                return self._create_no_experts_response(workload_profile)
            
            # Step 4: Select routing strategy
            strategy = await self._select_routing_strategy(
                gating_context, dual_axis_context
            )
            
            # Step 5: Route through gating network
            routing_result = await self._route_with_strategy(
                gating_context, available_indices, strategy, training_mode
            )
            
            # Step 6: Pre-heat experts if needed
            if self.enable_preheating:
                await self._preheat_experts(routing_result['expert_indices'])
            
            # Step 7: Execute experts
            if collaboration_enabled or (
                collaboration_enabled is None and self.enable_collaboration
            ):
                expert_plans = await self._execute_collaborative(
                    routing_result, workload_profile,
                    meta_cognitive_state, dual_axis_context
                )
            else:
                expert_plans = await self._execute_sequential(
                    routing_result, workload_profile,
                    meta_cognitive_state, dual_axis_context
                )
            
            # Step 8: Multi-objective scoring and aggregation
            final_plan = await self._aggregate_plans_multi_objective(
                expert_plans, dual_axis_context, gating_context
            )
            
            # Step 9: Generate explanation if required
            explanation = None
            if explainability_required or self.enable_explainability:
                explanation = await self._generate_explanation(
                    routing_result, expert_plans, final_plan,
                    gating_context, strategy
                )
            
            # Step 10: Create routing decision record
            decision = RoutingDecision(
                decision_id=route_id,
                task_id=workload_profile.get('task_id', 'unknown'),
                timestamp=datetime.utcnow(),
                selected_experts=[self.expert_index_map[i] for i in routing_result['expert_indices']],
                routing_weights=routing_result['weights'],
                strategy_used=strategy,
                confidence_score=routing_result.get('confidence', 0.5),
                alternatives=routing_result.get('alternatives', []),
                explanation=explanation or {},
                carbon_estimate_kg=final_plan.get('aggregate_carbon_kg', 0),
                helium_estimate=final_plan.get('aggregate_helium', 0),
                latency_estimate_ms=final_plan.get('aggregate_latency_ms', 0),
                dual_axis_score=final_plan.get('dual_axis_score', 0)
            )
            
            # Step 11: Record routing for learning
            self._record_routing_decision(decision)
            
            # Step 12: Record metrics
            execution_time = (time.time() - start_time) * 1000
            
            if self.metrics_collector:
                self.metrics_collector.record_routing(
                    routing_decisions=list(zip(
                        routing_result['expert_indices'],
                        routing_result['weights']
                    )),
                    gating_context=gating_context,
                    execution_time=execution_time,
                    success=final_plan.get('action') != 'defer'
                )
            
            self.metrics.successful_routes += 1
            self.metrics.average_latency_ms = (
                self.metrics.average_latency_ms * 0.9 + execution_time * 0.1
            )
            
            # Update expert usage
            for idx in routing_result['expert_indices']:
                expert_id = self.expert_index_map.get(idx)
                if expert_id:
                    self.metrics.expert_usage[expert_id] = (
                        self.metrics.expert_usage.get(expert_id, 0) + 1
                    )
            
            # Build response
            response = {
                'success': True,
                'route_id': route_id,
                'plans': expert_plans,
                'final_plan': final_plan,
                'routing_decision': decision,
                'execution_time_ms': execution_time,
                'strategy_used': strategy.value,
                'explanation': explanation,
                'metadata': {
                    'expert_count': len(expert_plans),
                    'collaborative': collaboration_enabled or self.enable_collaboration,
                    'circuit_breakers_active': sum(
                        1 for cb in self.circuit_breakers.values()
                        if cb.state != CircuitBreakerState.CLOSED
                    ),
                    'load_shedding_active': self._should_shed_load(),
                    'timestamp': datetime.utcnow().isoformat()
                }
            }
            
            return response
            
        except Exception as e:
            logger.error(f"Routing failed for {route_id}: {str(e)}", exc_info=True)
            self.metrics.failed_routes += 1
            
            return self._create_fallback_response(workload_profile, str(e))
        
        finally:
            async with self._route_lock:
                self.active_routes -= 1
    
    # ========================================================================
    # Enhanced Gating Context Building
    # ========================================================================
    
    def _build_enhanced_gating_context(
        self,
        workload_profile: Dict[str, Any],
        meta_cognitive_state: Dict[str, Any],
        dual_axis_context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Build comprehensive gating context with all available features"""
        return GatingContext(
            # Layer 0 features
            task_type=workload_profile.get('task_type', 'inference'),
            task_complexity=workload_profile.get('complexity', 0.5),
            input_size_mb=workload_profile.get('input_size_mb', 1.0),
            
            # Layer 1 features
            carbon_budget_remaining=meta_cognitive_state.get('carbon_budget_remaining', 1.0),
            helium_budget_remaining=meta_cognitive_state.get('helium_budget_remaining', 1.0),
            latency_budget_ms=meta_cognitive_state.get('latency_budget_ms', 100.0),
            historical_success_rate=meta_cognitive_state.get('historical_success_rate', 0.9),
            
            # Layer 3 features
            carbon_zone=dual_axis_context.get('carbon_zone', 0),
            helium_scarcity=dual_axis_context.get('helium_scarcity', 0.5),
            
            # Additional context
            time_of_day=datetime.utcnow().hour,
            grid_carbon_intensity=workload_profile.get('grid_carbon_intensity', 400.0),
            hardware_availability=workload_profile.get('hardware_availability', {
                'cpu': 1.0, 'gpu': 0.8, 'quantum': 0.0, 'edge': 0.5
            })
        )
    
    # ========================================================================
    # Circuit Breaker Integration
    # ========================================================================
    
    def _filter_circuit_broken(
        self,
        allowed_indices: Optional[List[int]]
    ) -> List[int]:
        """Filter out experts with open circuit breakers"""
        if not self.enable_circuit_breaker:
            return allowed_indices if allowed_indices else list(range(len(self.experts)))
        
        available = []
        for idx in (allowed_indices or range(len(self.experts))):
            expert_id = self.expert_index_map.get(idx)
            if expert_id and expert_id in self.circuit_breakers:
                if self.circuit_breakers[expert_id].can_execute():
                    available.append(idx)
                else:
                    logger.debug(f"Expert {expert_id} blocked by circuit breaker")
            else:
                available.append(idx)
        
        return available if available else (allowed_indices or list(range(len(self.experts))))
    
    def _record_expert_result(
        self,
        expert_id: str,
        success: bool
    ):
        """Record expert execution result for circuit breaker"""
        if expert_id in self.circuit_breakers:
            if success:
                self.circuit_breakers[expert_id].record_success()
            else:
                self.circuit_breakers[expert_id].record_failure()
                self.metrics.circuit_breaker_events += 1
    
    # ========================================================================
    # Load Shedding
    # ========================================================================
    
    def _should_shed_load(self) -> bool:
        """Determine if load should be shed"""
        if not self.enable_load_shedding:
            return False
        
        utilization = self.active_routes / max(self.max_concurrent_routes, 1)
        return utilization > self.load_threshold
    
    def _create_load_shed_response(
        self,
        workload_profile: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Create response for shedded load"""
        return {
            'success': False,
            'action': 'defer',
            'reason': 'load_shedding',
            'message': 'System under high load, request deferred',
            'retry_after_ms': 1000,
            'task_id': workload_profile.get('task_id', 'unknown'),
            'metadata': {
                'active_routes': self.active_routes,
                'max_routes': self.max_concurrent_routes,
                'utilization': self.active_routes / max(self.max_concurrent_routes, 1)
            }
        }
    
    # ========================================================================
    # Adaptive Strategy Selection
    # ========================================================================
    
    async def _select_routing_strategy(
        self,
        gating_context: 'GatingContext',
        dual_axis_context: Dict[str, Any]
    ) -> RoutingStrategy:
        """Select best routing strategy based on context and performance"""
        
        if self.routing_strategy != RoutingStrategy.ADAPTIVE:
            return self.routing_strategy
        
        # Analyze context for strategy selection
        carbon_zone = dual_axis_context.get('carbon_zone', 0)
        helium_scarcity = dual_axis_context.get('helium_scarcity', 0.5)
        
        # High constraint scenarios: use multi-objective
        if carbon_zone >= 8 or helium_scarcity > 0.7:
            return RoutingStrategy.MULTI_OBJECTIVE
        
        # Low experience: use Thompson sampling for exploration
        if len(self.routing_history) < 100:
            return RoutingStrategy.THOMPSON_SAMPLING
        
        # Check strategy performance history
        strategy_scores = {}
        for strategy in RoutingStrategy:
            if strategy == RoutingStrategy.ADAPTIVE:
                continue
            
            scores = self.strategy_performance.get(strategy.value, [])
            if scores:
                # UCB-like scoring
                avg_score = np.mean(scores)
                exploration_bonus = np.sqrt(2 * np.log(len(scores)) / len(scores))
                strategy_scores[strategy] = avg_score + exploration_bonus
            else:
                strategy_scores[strategy] = 0.5  # Default
        
        # Select best strategy
        if strategy_scores:
            best_strategy = max(strategy_scores, key=strategy_scores.get)
            return best_strategy
        
        return RoutingStrategy.EPSILON_GREEDY
    
    async def _route_with_strategy(
        self,
        gating_context: 'GatingContext',
        available_indices: List[int],
        strategy: RoutingStrategy,
        training_mode: bool
    ) -> Dict[str, Any]:
        """Route using selected strategy"""
        
        if strategy == RoutingStrategy.GREEDY:
            return self._greedy_route(gating_context, available_indices)
        elif strategy == RoutingStrategy.EPSILON_GREEDY:
            return self._epsilon_greedy_route(gating_context, available_indices)
        elif strategy == RoutingStrategy.THOMPSON_SAMPLING:
            return self._thompson_sampling_route(gating_context, available_indices)
        elif strategy == RoutingStrategy.UCB:
            return self._ucb_route(gating_context, available_indices)
        elif strategy == RoutingStrategy.MULTI_OBJECTIVE:
            return self._multi_objective_route(gating_context, available_indices)
        else:
            # Default to gating network
            return self._gating_network_route(gating_context, available_indices, training_mode)
    
    def _greedy_route(
        self,
        gating_context: 'GatingContext',
        available_indices: List[int]
    ) -> Dict[str, Any]:
        """Greedy routing: always pick highest scored"""
        routing_decisions = self.gating_network.route(
            gating_context,
            expert_constraints=available_indices
        )
        
        return {
            'expert_indices': [d[0] for d in routing_decisions],
            'weights': [d[1] for d in routing_decisions],
            'confidence': max([d[1] for d in routing_decisions]) if routing_decisions else 0.5,
            'alternatives': []
        }
    
    def _epsilon_greedy_route(
        self,
        gating_context: 'GatingContext',
        available_indices: List[int]
    ) -> Dict[str, Any]:
        """Epsilon-greedy: explore occasionally"""
        epsilon = 0.1
        
        if np.random.random() < epsilon:
            # Explore: random expert
            import random
            selected = random.sample(
                available_indices,
                min(2, len(available_indices))
            )
            return {
                'expert_indices': selected,
                'weights': [1.0 / len(selected)] * len(selected),
                'confidence': 0.3,
                'alternatives': []
            }
        else:
            return self._greedy_route(gating_context, available_indices)
    
    def _thompson_sampling_route(
        self,
        gating_context: 'GatingContext',
        available_indices: List[int]
    ) -> Dict[str, Any]:
        """Thompson sampling for exploration/exploitation"""
        sampled_scores = {}
        
        for idx in available_indices:
            expert_id = self.expert_index_map.get(idx)
            if expert_id:
                # Get success/failure counts
                successes = self.metrics.expert_usage.get(expert_id, 0)
                failures = self.circuit_breakers.get(expert_id, ExpertCircuitBreaker(expert_id)).failure_count
                
                # Sample from Beta distribution
                alpha = successes + 1
                beta = failures + 1
                sampled_scores[idx] = np.random.beta(alpha, beta)
        
        # Select top-k by sampled scores
        sorted_experts = sorted(sampled_scores.items(), key=lambda x: x[1], reverse=True)
        top_k = sorted_experts[:2]
        
        return {
            'expert_indices': [e[0] for e in top_k],
            'weights': [e[1] / sum(e[1] for e in top_k) for e in top_k],
            'confidence': top_k[0][1] if top_k else 0.5,
            'alternatives': [e[0] for e in sorted_experts[2:4]]
        }
    
    def _ucb_route(
        self,
        gating_context: 'GatingContext',
        available_indices: List[int]
    ) -> Dict[str, Any]:
        """Upper Confidence Bound routing"""
        total_uses = sum(
            self.metrics.expert_usage.get(self.expert_index_map.get(i, ''), 0)
            for i in available_indices
        )
        
        ucb_scores = {}
        for idx in available_indices:
            expert_id = self.expert_index_map.get(idx)
            if expert_id:
                usage = self.metrics.expert_usage.get(expert_id, 0)
                success_rate = (
                    usage / max(usage + self.circuit_breakers.get(expert_id, ExpertCircuitBreaker(expert_id)).failure_count, 1)
                    if usage > 0 else 0.5
                )
                
                # UCB formula
                exploration = np.sqrt(2 * np.log(max(total_uses, 1)) / max(usage, 1))
                ucb_scores[idx] = success_rate + exploration
        
        sorted_experts = sorted(ucb_scores.items(), key=lambda x: x[1], reverse=True)
        top_k = sorted_experts[:2]
        
        return {
            'expert_indices': [e[0] for e in top_k],
            'weights': [e[1] / sum(e[1] for e in top_k) for e in top_k],
            'confidence': top_k[0][1] if top_k else 0.5,
            'alternatives': [e[0] for e in sorted_experts[2:4]]
        }
    
    def _multi_objective_route(
        self,
        gating_context: 'GatingContext',
        available_indices: List[int]
    ) -> Dict[str, Any]:
        """Multi-objective routing considering carbon, helium, latency, accuracy"""
        scores = {}
        
        for idx in available_indices:
            expert_id = self.expert_index_map.get(idx)
            if expert_id and expert_id in self.experts:
                expert = self.experts[expert_id]
                profile = expert.profile
                
                # Normalize each objective
                carbon_score = 1.0 / (1.0 + profile.carbon_per_inference * 10000)
                helium_score = 1.0 / (1.0 + profile.helium_per_inference * 100)
                latency_score = 1.0 / (1.0 + profile.avg_latency_ms / 100)
                accuracy_score = profile.accuracy_score
                
                # Weighted sum based on context
                if gating_context.helium_scarcity > 0.7:
                    weights = {'carbon': 0.3, 'helium': 0.4, 'latency': 0.1, 'accuracy': 0.2}
                elif gating_context.carbon_zone >= 8:
                    weights = {'carbon': 0.5, 'helium': 0.2, 'latency': 0.1, 'accuracy': 0.2}
                else:
                    weights = {'carbon': 0.25, 'helium': 0.25, 'latency': 0.2, 'accuracy': 0.3}
                
                scores[idx] = (
                    weights['carbon'] * carbon_score +
                    weights['helium'] * helium_score +
                    weights['latency'] * latency_score +
                    weights['accuracy'] * accuracy_score
                )
        
        sorted_experts = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        top_k = sorted_experts[:2]
        
        return {
            'expert_indices': [e[0] for e in top_k],
            'weights': [e[1] / sum(e[1] for e in top_k) for e in top_k],
            'confidence': top_k[0][1] if top_k else 0.5,
            'alternatives': [e[0] for e in sorted_experts[2:4]],
            'objective_scores': {self.expert_index_map.get(k): v for k, v in scores.items()}
        }
    
    def _gating_network_route(
        self,
        gating_context: 'GatingContext',
        available_indices: List[int],
        training_mode: bool
    ) -> Dict[str, Any]:
        """Use gating network for routing"""
        routing_decisions = self.gating_network.route(
            gating_context,
            expert_constraints=available_indices,
            training=training_mode
        )
        
        return {
            'expert_indices': [d[0] for d in routing_decisions],
            'weights': [d[1] for d in routing_decisions],
            'confidence': max([d[1] for d in routing_decisions]) if routing_decisions else 0.5,
            'alternatives': []
        }
    
    # ========================================================================
    # Expert Preheating
    # ========================================================================
    
    async def _preheat_experts(self, expert_indices: List[int]):
        """Preheat experts to reduce cold start latency"""
        for idx in expert_indices:
            expert_id = self.expert_index_map.get(idx)
            if expert_id and expert_id in self.experts:
                if expert_id not in self.preheat_queue:
                    self.preheat_queue.append(expert_id)
                    logger.debug(f"Added {expert_id} to preheat queue")
    
    async def _preheating_loop(self):
        """Background loop for expert preheating"""
        while True:
            try:
                if self.preheat_queue:
                    expert_id = self.preheat_queue.pop(0)
                    if expert_id in self.experts:
                        # Trigger preheating
                        expert = self.experts[expert_id]
                        if hasattr(expert, 'preload_expert'):
                            await expert.preload_expert(expert_id)
                        logger.debug(f"Preheated expert: {expert_id}")
                
                await asyncio.sleep(1)
                
            except Exception as e:
                logger.error(f"Preheating error: {str(e)}")
                await asyncio.sleep(5)
    
    # ========================================================================
    # Collaborative Expert Execution
    # ========================================================================
    
    async def _execute_collaborative(
        self,
        routing_result: Dict[str, Any],
        workload_profile: Dict[str, Any],
        meta_cognitive_state: Dict[str, Any],
        dual_axis_context: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Execute experts collaboratively, sharing context"""
        plans = []
        shared_context = {}
        
        for expert_idx, weight in zip(
            routing_result['expert_indices'],
            routing_result['weights']
        ):
            expert_id = self.expert_index_map.get(expert_idx)
            if not expert_id or expert_id not in self.experts:
                continue
            
            expert = self.experts[expert_id]
            
            try:
                # Execute expert with shared context
                plan = await self._execute_single_expert(
                    expert, expert_id, workload_profile,
                    meta_cognitive_state, dual_axis_context,
                    shared_context
                )
                
                if plan:
                    plan['routing_weight'] = float(weight)
                    plan['expert_domain'] = getattr(expert.profile, 'domain', 'general')
                    plans.append(plan)
                    
                    # Share results with next experts
                    shared_context[expert_id] = plan
                    
                    # Record success
                    self._record_expert_result(expert_id, True)
                
            except Exception as e:
                logger.error(f"Expert {expert_id} execution failed: {str(e)}")
                self._record_expert_result(expert_id, False)
        
        return plans
    
    async def _execute_sequential(
        self,
        routing_result: Dict[str, Any],
        workload_profile: Dict[str, Any],
        meta_cognitive_state: Dict[str, Any],
        dual_axis_context: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Execute experts sequentially without collaboration"""
        plans = []
        
        for expert_idx, weight in zip(
            routing_result['expert_indices'],
            routing_result['weights']
        ):
            expert_id = self.expert_index_map.get(expert_idx)
            if not expert_id or expert_id not in self.experts:
                continue
            
            expert = self.experts[expert_id]
            
            try:
                plan = await self._execute_single_expert(
                    expert, expert_id, workload_profile,
                    meta_cognitive_state, dual_axis_context
                )
                
                if plan:
                    plan['routing_weight'] = float(weight)
                    plans.append(plan)
                    self._record_expert_result(expert_id, True)
                
            except Exception as e:
                logger.error(f"Expert {expert_id} failed: {str(e)}")
                self._record_expert_result(expert_id, False)
        
        return plans
    
    async def _execute_single_expert(
        self,
        expert: Any,
        expert_id: str,
        workload_profile: Dict[str, Any],
        meta_cognitive_state: Dict[str, Any],
        dual_axis_context: Dict[str, Any],
        shared_context: Optional[Dict[str, Any]] = None
    ) -> Optional[Dict[str, Any]]:
        """Execute a single expert with appropriate method"""
        
        if expert_id == 'energy':
            return await expert.optimize_energy(
                task_config=workload_profile,
                carbon_budget=meta_cognitive_state.get('carbon_budget_remaining', 1.0),
                latency_requirement_ms=meta_cognitive_state.get('latency_budget_ms', 100.0),
                grid_carbon_intensity=workload_profile.get('grid_carbon_intensity'),
                helium_scarcity=dual_axis_context.get('helium_scarcity', 0.5),
                cross_expert_hints=shared_context
            )
        elif expert_id == 'data':
            return await expert.optimize_data_pipeline(
                input_size_mb=workload_profile.get('input_size_mb', 1.0),
                helium_scarcity=dual_axis_context.get('helium_scarcity', 0.5),
                latency_budget_ms=meta_cognitive_state.get('latency_budget_ms', 100.0),
                carbon_budget_kg=meta_cognitive_state.get('carbon_budget_remaining'),
                cross_expert_hints=shared_context
            )
        elif expert_id == 'iot':
            return await expert.optimize_edge_deployment(
                device_type=workload_profile.get('device_type', 'edge_node'),
                carbon_zone=dual_axis_context.get('carbon_zone', 0),
                helium_scarcity=dual_axis_context.get('helium_scarcity', 0.5)
            )
        elif expert_id == 'helium':
            return await expert.optimize_helium_usage(
                current_scarcity=dual_axis_context.get('helium_scarcity', 0.5),
                compute_requirement=workload_profile.get('complexity', 0.5)
            )
        elif expert_id == 'quantum':
            return await expert.solve_quantum_optimization(
                problem_type=workload_profile.get('task_type', 'optimization'),
                quantum_available=workload_profile.get('hardware_availability', {}).get('quantum', 0) > 0
            )
        
        return None
    
    # ========================================================================
    # Multi-Objective Plan Aggregation
    # ========================================================================
    
    async def _aggregate_plans_multi_objective(
        self,
        expert_plans: List[Dict[str, Any]],
        dual_axis_context: Dict[str, Any],
        gating_context: 'GatingContext'
    ) -> Dict[str, Any]:
        """Aggregate expert plans with multi-objective optimization"""
        if not expert_plans:
            return {
                'action': 'defer',
                'reason': 'No expert plans available'
            }
        
        # Normalize weights
        total_weight = sum(p.get('routing_weight', 0) for p in expert_plans)
        if total_weight > 0:
            for plan in expert_plans:
                plan['normalized_weight'] = plan.get('routing_weight', 0) / total_weight
        
        # Calculate weighted aggregates
        weighted_carbon = sum(
            p.get('estimated_carbon_kg', 0) * p.get('normalized_weight', 0)
            for p in expert_plans
        )
        
        weighted_helium = sum(
            p.get('helium_per_inference', p.get('estimated_helium_units', 0)) * p.get('normalized_weight', 0)
            for p in expert_plans
        )
        
        weighted_energy = sum(
            p.get('estimated_energy_kwh', 0) * p.get('normalized_weight', 0)
            for p in expert_plans
        )
        
        weighted_latency = sum(
            p.get('estimated_latency_ms', 0) * p.get('normalized_weight', 0)
            for p in expert_plans
        )
        
        # Calculate dual-axis score
        carbon_zone = dual_axis_context.get('carbon_zone', 0)
        helium_scarcity = dual_axis_context.get('helium_scarcity', 0.5)
        
        carbon_score = 1.0 - min(weighted_carbon / 0.001, 1.0)
        helium_score = 1.0 - min(weighted_helium / 0.1, 1.0)
        
        dual_axis_score = (
            dual_axis_context.get('carbon_weight', 0.6) * carbon_score +
            dual_axis_context.get('helium_weight', 0.4) * helium_score
        )
        
        # Determine action
        if carbon_zone >= 12 and helium_scarcity > 0.8:
            action = 'defer'
        elif carbon_zone >= 8 or helium_scarcity > 0.6:
            action = 'execute_minimal'
        elif carbon_zone >= 4 or helium_scarcity > 0.3:
            action = 'execute_throttled'
        else:
            action = 'execute_full'
        
        return {
            'action': action,
            'aggregate_carbon_kg': weighted_carbon,
            'aggregate_helium': weighted_helium,
            'aggregate_energy_kwh': weighted_energy,
            'aggregate_latency_ms': weighted_latency,
            'dual_axis_score': dual_axis_score,
            'carbon_zone': carbon_zone,
            'helium_scarcity': helium_scarcity,
            'expert_count': len(expert_plans)
        }
    
    # ========================================================================
    # Explainability
    # ========================================================================
    
    async def _generate_explanation(
        self,
        routing_result: Dict[str, Any],
        expert_plans: List[Dict[str, Any]],
        final_plan: Dict[str, Any],
        gating_context: 'GatingContext',
        strategy: RoutingStrategy
    ) -> Dict[str, Any]:
        """Generate detailed explanation of routing decision"""
        explanation = {
            'routing_strategy': strategy.value,
            'why_these_experts': [],
            'why_not_others': [],
            'key_factors': [],
            'confidence_factors': []
        }
        
        # Explain expert selection
        for idx, weight in zip(
            routing_result['expert_indices'],
            routing_result['weights']
        ):
            expert_id = self.expert_index_map.get(idx, 'unknown')
            if expert_id in self.experts:
                profile = self.experts[expert_id].profile
                explanation['why_these_experts'].append({
                    'expert': expert_id,
                    'weight': weight,
                    'reasons': [
                        f"Carbon per inference: {profile.carbon_per_inference:.6f} kg",
                        f"Helium per inference: {profile.helium_per_inference:.4f}",
                        f"Accuracy: {profile.accuracy_score:.2%}",
                        f"Latency: {profile.avg_latency_ms:.1f} ms"
                    ]
                })
        
        # Explain key factors
        if gating_context.helium_scarcity > 0.7:
            explanation['key_factors'].append("High helium scarcity prioritized helium-efficient experts")
        if gating_context.carbon_zone >= 8:
            explanation['key_factors'].append("High carbon zone prioritized low-carbon experts")
        if gating_context.latency_budget_ms < 50:
            explanation['key_factors'].append("Tight latency budget prioritized fast experts")
        
        # Confidence factors
        explanation['confidence_factors'] = [
            f"Strategy confidence: {routing_result.get('confidence', 0.5):.2%}",
            f"Expert agreement: {'High' if len(expert_plans) > 1 else 'Single expert'}",
            f"Historical success rate: {gating_context.historical_success_rate:.2%}"
        ]
        
        return explanation
    
    # ========================================================================
    # Adaptive Learning
    # ========================================================================
    
    def _record_routing_decision(self, decision: RoutingDecision):
        """Record routing decision for adaptive learning"""
        self.routing_history.append({
            'decision': decision,
            'timestamp': decision.timestamp,
            'strategy': decision.strategy_used.value,
            'success': decision.dual_axis_score > 0.3
        })
    
    async def _adaptive_learning_loop(self):
        """Background loop for adaptive learning"""
        while True:
            try:
                if len(self.routing_history) >= 50:
                    # Analyze recent performance
                    recent = list(self.routing_history)[-100:]
                    
                    # Update strategy performance
                    strategy_results = defaultdict(list)
                    for record in recent:
                        strategy = record['strategy']
                        success = 1.0 if record['success'] else 0.0
                        strategy_results[strategy].append(success)
                    
                    for strategy, results in strategy_results.items():
                        avg_performance = np.mean(results)
                        self.strategy_performance[strategy].append(avg_performance)
                        
                        # Keep last 100 scores
                        if len(self.strategy_performance[strategy]) > 100:
                            self.strategy_performance[strategy] = \
                                self.strategy_performance[strategy][-100:]
                    
                    # Update collaboration scores
                    if self.enable_collaboration:
                        await self._update_collaboration_scores(recent)
                
                await asyncio.sleep(10)
                
            except Exception as e:
                logger.error(f"Adaptive learning error: {str(e)}")
                await asyncio.sleep(30)
    
    async def _update_collaboration_scores(self, recent_records: List[Dict]):
        """Update expert collaboration scores"""
        for record in recent_records:
            decision = record['decision']
            experts = decision.selected_experts
            
            if len(experts) > 1:
                for i, e1 in enumerate(experts):
                    for e2 in experts[i+1:]:
                        key = (e1, e2)
                        old_score = self.collaboration_scores.get(key, 0.5)
                        new_score = decision.dual_axis_score
                        self.collaboration_scores[key] = old_score * 0.9 + new_score * 0.1
    
    # ========================================================================
    # Health Check Loop
    # ========================================================================
    
    async def _health_check_loop(self):
        """Background health check loop"""
        while True:
            try:
                # Check circuit breakers
                for expert_id, cb in self.circuit_breakers.items():
                    if cb.state == CircuitBreakerState.OPEN:
                        # Check if should try recovery
                        if cb.last_failure_time:
                            elapsed = (datetime.utcnow() - cb.last_failure_time).total_seconds()
                            if elapsed >= cb.recovery_timeout_seconds:
                                cb.state = CircuitBreakerState.HALF_OPEN
                                logger.info(f"Circuit breaker HALF_OPEN for {expert_id}")
                
                # Update router state based on load
                utilization = self.active_routes / max(self.max_concurrent_routes, 1)
                if utilization > 0.9:
                    self.state = RouterState.OVERLOADED
                elif utilization > 0.7:
                    self.state = RouterState.DEGRADED
                else:
                    self.state = RouterState.NORMAL
                
                await asyncio.sleep(5)
                
            except Exception as e:
                logger.error(f"Health check error: {str(e)}")
                await asyncio.sleep(15)
    
    # ========================================================================
    # Metrics Reporting
    # ========================================================================
    
    async def _metrics_reporting_loop(self):
        """Background metrics reporting loop"""
        while True:
            try:
                if self.metrics_collector:
                    # Update Prometheus metrics
                    for expert_id, usage in self.metrics.expert_usage.items():
                        if hasattr(self.metrics_collector, 'record_expert_usage'):
                            self.metrics_collector.record_expert_usage(expert_id, usage)
                
                await asyncio.sleep(30)
                
            except Exception as e:
                logger.error(f"Metrics reporting error: {str(e)}")
                await asyncio.sleep(60)
    
    # ========================================================================
    # Public API Methods
    # ========================================================================
    
    def get_routing_stats(self) -> Dict[str, Any]:
        """Get comprehensive routing statistics"""
        return {
            'metrics': {
                'total_routes': self.metrics.total_routes,
                'successful_routes': self.metrics.successful_routes,
                'failed_routes': self.metrics.failed_routes,
                'fallback_routes': self.metrics.fallback_routes,
                'success_rate': self.metrics.success_rate,
                'fallback_rate': self.metrics.fallback_rate,
                'average_latency_ms': self.metrics.average_latency_ms,
                'average_confidence': self.metrics.average_confidence
            },
            'router_state': self.state.value,
            'active_routes': self.active_routes,
            'utilization': self.active_routes / max(self.max_concurrent_routes, 1),
            'circuit_breakers': {
                eid: cb.get_health_status()
                for eid, cb in self.circuit_breakers.items()
            },
            'expert_utilization': self.metrics.expert_usage,
            'strategy_performance': {
                s: np.mean(scores[-50:]) if scores else 0
                for s, scores in self.strategy_performance.items()
            },
            'collaboration_scores': dict(self.collaboration_scores),
            'load_shedding_events': self.metrics.load_shedding_events,
            'gating_network': {
                'expert_utilization': self.gating_network.get_expert_utilization(),
                'load_balance_score': self.gating_network.get_load_balance_score()
            }
        }
    
    def get_expert_health(self) -> Dict[str, Dict[str, Any]]:
        """Get health status of all experts"""
        return {
            eid: cb.get_health_status()
            for eid, cb in self.circuit_breakers.items()
        }
    
    def reset_circuit_breaker(self, expert_id: str) -> bool:
        """Manually reset circuit breaker for expert"""
        if expert_id in self.circuit_breakers:
            self.circuit_breakers[expert_id] = ExpertCircuitBreaker(
                expert_id=expert_id
            )
            logger.info(f"Reset circuit breaker for {expert_id}")
            return True
        return False
    
    def get_routing_history(
        self,
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """Get recent routing history"""
        recent = list(self.routing_history)[-limit:]
        return [
            {
                'timestamp': r['timestamp'].isoformat(),
                'strategy': r['strategy'],
                'experts': r['decision'].selected_experts,
                'success': r['success'],
                'dual_axis_score': r['decision'].dual_axis_score
            }
            for r in recent
        ]
