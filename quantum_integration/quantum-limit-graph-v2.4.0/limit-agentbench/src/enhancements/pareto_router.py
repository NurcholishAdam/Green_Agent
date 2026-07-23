# File: src/enhancements/pareto_router.py
"""
Enhanced Pareto Frontier Routing v2.0.0
Multi‑objective optimization for Green Agent with configurable objectives,
caching, constraints, adaptive weights, metrics, and explanations.
"""

import asyncio
import logging
import json
import time
import uuid
from typing import Dict, Any, List, Optional, Tuple, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from collections import OrderedDict
import numpy as np

# ---------- Pydantic ----------
from pydantic import BaseModel, Field, field_validator

# ---------- SQLAlchemy ----------
try:
    from sqlalchemy import Column, String, Float, DateTime, Integer, JSON, text, create_engine
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy.orm import sessionmaker, scoped_session
    SQLALCHEMY_AVAILABLE = True
except ImportError:
    SQLALCHEMY_AVAILABLE = False

# ---------- Prometheus ----------
try:
    from prometheus_client import Counter, Gauge, Histogram, CollectorRegistry
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

# ---------- Tenacity ----------
try:
    from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, before_sleep_log
    TENACITY_AVAILABLE = True
except ImportError:
    TENACITY_AVAILABLE = False

# ---------- Structlog ----------
try:
    import structlog
    logger = structlog.get_logger(__name__)
except ImportError:
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO)

# ---------- Local imports (stubs) ----------
# These would normally be imported from your project.
class ExpertRouter:
    def get_candidate_experts(self, task: Dict, context: Dict) -> List:
        return []
    @property
    def registry(self):
        return None

class ExpertProfile:
    def __init__(self, expert_id, **kwargs):
        self.expert_id = expert_id
        for k, v in kwargs.items():
            setattr(self, k, v)

class NodeRegistry:
    async def get_node(self, node_id: str) -> Optional[Dict]:
        return None

class CarbonIntensityManager:
    async def get_current_intensity(self) -> Dict:
        return {'intensity': 400}

class UserPreferences:
    def get_weights(self) -> Dict[str, float]:
        return {}

class AdaptiveCostFunction:
    async def record_feedback(self, context: Dict, metrics: Dict) -> None:
        pass
    @property
    def weights(self):
        return {}
    @property
    def learning_rate(self):
        return 0.01

# ---------- Configuration ----------
class ParetoRouterConfig(BaseModel):
    """Configuration for ParetoRouter."""
    cache_ttl_seconds: int = Field(300, ge=0)
    use_adaptive_weights: bool = True
    enable_persistence: bool = True
    db_path: str = "pareto_routing.db"
    max_retry_attempts: int = Field(3, ge=0)
    circuit_breaker_threshold: int = Field(5, ge=1)
    circuit_breaker_timeout: int = Field(30, ge=1)
    # Default objective weights (if no user prefs)
    default_weights: Dict[str, float] = Field(
        default_factory=lambda: {
            'energy': 1.0,
            'carbon': 1.0,
            'helium': 0.5,
            'material': 0.3,
            'latency': 0.1,
            'inaccuracy': 0.1
        }
    )
    # Constraints (if any objective must be below a threshold)
    constraints: Dict[str, float] = Field(default_factory=dict)

# ---------- Circuit Breaker ----------
class CircuitBreakerState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

class EnhancedCircuitBreaker:
    def __init__(self, name: str, threshold: int = 5, timeout: int = 30):
        self.name = name
        self.threshold = threshold
        self.timeout = timeout
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.last_failure_time = None
        self._lock = asyncio.Lock()
        self.metrics = {"total_calls": 0, "failed_calls": 0, "successful_calls": 0}

    async def call(self, func, *args, **kwargs):
        async with self._lock:
            if self.state == CircuitBreakerState.OPEN:
                if time.time() - self.last_failure_time >= self.timeout:
                    self.state = CircuitBreakerState.HALF_OPEN
                    self.failure_count = 0
                else:
                    raise Exception(f"Circuit breaker {self.name} is OPEN")
        self.metrics["total_calls"] += 1
        try:
            result = await func(*args, **kwargs)
            await self._record_success()
            return result
        except Exception as e:
            await self._record_failure()
            raise

    async def _record_success(self):
        async with self._lock:
            self.metrics["successful_calls"] += 1
            if self.state == CircuitBreakerState.HALF_OPEN:
                self.state = CircuitBreakerState.CLOSED
            self.failure_count = 0

    async def _record_failure(self):
        async with self._lock:
            self.metrics["failed_calls"] += 1
            self.failure_count += 1
            self.last_failure_time = time.time()
            if self.state == CircuitBreakerState.CLOSED and self.failure_count >= self.threshold:
                self.state = CircuitBreakerState.OPEN
            elif self.state == CircuitBreakerState.HALF_OPEN:
                self.state = CircuitBreakerState.OPEN

# ---------- Retry decorator ----------
def retry_decorator(attempts: int = 3, min_wait: int = 2, max_wait: int = 10):
    if TENACITY_AVAILABLE:
        return retry(
            stop=stop_after_attempt(attempts),
            wait=wait_exponential(multiplier=1, min=min_wait, max=max_wait),
            retry=retry_if_exception_type(Exception),
            before_sleep=before_sleep_log(logger, logging.WARNING)
        )
    else:
        def decorator(func):
            async def wrapper(*args, **kwargs):
                for attempt in range(attempts):
                    try:
                        return await func(*args, **kwargs)
                    except Exception as e:
                        if attempt == attempts - 1:
                            raise
                        await asyncio.sleep(2 ** attempt)
                return None
            return wrapper
        return decorator

# ---------- Database Models ----------
Base = declarative_base() if SQLALCHEMY_AVAILABLE else None

class RoutingDecisionDB(Base):
    __tablename__ = 'routing_decisions'
    id = Column(Integer, primary_key=True)
    request_id = Column(String(128))
    task_id = Column(String(128))
    selected_expert_id = Column(String(128))
    frontier_size = Column(Integer)
    selection_reason = Column(String(256))
    vector_scores = Column(JSON)
    timestamp = Column(DateTime, default=datetime.now)

# ---------- Prometheus Metrics ----------
if PROMETHEUS_AVAILABLE:
    REGISTRY = CollectorRegistry()
    ROUTING_DECISIONS = Counter('routing_decisions_total', 'Total routing decisions', ['status'], registry=REGISTRY)
    FRONTIER_SIZE = Gauge('pareto_frontier_size', 'Size of Pareto frontier', registry=REGISTRY)
    ROUTING_LATENCY = Histogram('routing_latency_seconds', 'Routing selection latency', registry=REGISTRY)
else:
    class DummyMetric:
        def labels(self, **kwargs): return self
        def inc(self, **kwargs): pass
        def set(self, **kwargs): pass
        def observe(self, **kwargs): pass
    ROUTING_DECISIONS = DummyMetric()
    FRONTIER_SIZE = DummyMetric()
    ROUTING_LATENCY = DummyMetric()

# ---------- Objective Function Registry ----------
class ObjectiveFunction:
    """Base class for objective functions."""
    name: str
    async def compute(self, expert: ExpertProfile, context: Dict, dependencies: Dict) -> float:
        raise NotImplementedError

class EnergyObjective(ObjectiveFunction):
    name = "energy"
    async def compute(self, expert, context, deps):
        tokens = context.get('token_count', 1)
        return expert.energy_per_inference * tokens

class CarbonObjective(ObjectiveFunction):
    name = "carbon"
    async def compute(self, expert, context, deps):
        tokens = context.get('token_count', 1)
        carbon_manager = deps.get('carbon_manager')
        if carbon_manager:
            intensity_data = await carbon_manager.get_current_intensity()
            carbon_intensity = intensity_data.get('intensity', 400) / 1000  # g/kWh -> kg/kWh
        else:
            carbon_intensity = 0.4
        return expert.carbon_per_inference * tokens * carbon_intensity

class HeliumObjective(ObjectiveFunction):
    name = "helium"
    async def compute(self, expert, context, deps):
        tokens = context.get('token_count', 1)
        target_node = context.get('target_node_id')
        node_registry = deps.get('node_registry')
        helium_index = 0.0
        if target_node and node_registry:
            desc = await node_registry.get_node(target_node)
            if desc:
                helium_index = desc.helium_index
        return expert.helium_per_inference * tokens * (1 + helium_index)

class MaterialObjective(ObjectiveFunction):
    name = "material"
    async def compute(self, expert, context, deps):
        target_node = context.get('target_node_id')
        node_registry = deps.get('node_registry')
        if target_node and node_registry:
            desc = await node_registry.get_node(target_node)
            if desc:
                return desc.material_index
        return 0.0

class LatencyObjective(ObjectiveFunction):
    name = "latency"
    async def compute(self, expert, context, deps):
        return context.get('expected_latency_ms', 100)

class InaccuracyObjective(ObjectiveFunction):
    name = "inaccuracy"
    async def compute(self, expert, context, deps):
        return 1.0 - expert.accuracy_score

# Registry of objective functions
OBJECTIVE_REGISTRY = {
    'energy': EnergyObjective(),
    'carbon': CarbonObjective(),
    'helium': HeliumObjective(),
    'material': MaterialObjective(),
    'latency': LatencyObjective(),
    'inaccuracy': InaccuracyObjective(),
}

# ---------- Main Pareto Router ----------
class ParetoRouter(ExpertRouter):
    """
    Enhanced multi‑objective router with configurable objectives, caching, constraints,
    adaptive weights, metrics, and explanations.
    """

    def __init__(
        self,
        config: Dict[str, Any],
        cost_function: AdaptiveCostFunction,
        node_registry: NodeRegistry,
        carbon_manager: Optional[CarbonIntensityManager] = None,
        user_preferences: Optional[UserPreferences] = None,
        objectives: Optional[List[str]] = None,
        *args,
        **kwargs
    ):
        super().__init__(config, *args, **kwargs)
        self.cost_function = cost_function
        self.node_registry = node_registry
        self.carbon_manager = carbon_manager
        self.user_prefs = user_preferences

        # Configuration
        self.router_config = ParetoRouterConfig(**config.get('pareto', {}))

        # Objective functions
        self.objective_names = objectives or list(OBJECTIVE_REGISTRY.keys())
        self.objectives = {name: OBJECTIVE_REGISTRY[name] for name in self.objective_names if name in OBJECTIVE_REGISTRY}

        # Vector cache (expert_id -> (vector, timestamp))
        self._cache: Dict[str, Tuple[np.ndarray, datetime]] = {}
        self._cache_lock = asyncio.Lock()

        # Circuit breaker for external calls
        self._circuit_breaker = EnhancedCircuitBreaker(
            "pareto_router",
            threshold=self.router_config.circuit_breaker_threshold,
            timeout=self.router_config.circuit_breaker_timeout
        )

        # Persistence
        self._db_session = None
        if SQLALCHEMY_AVAILABLE and self.router_config.enable_persistence:
            self._init_db()

        logger.info("ParetoRouter initialized", objectives=self.objective_names, cache_ttl=self.router_config.cache_ttl_seconds)

    def _init_db(self):
        engine = create_engine(f"sqlite:///{self.router_config.db_path}")
        Base.metadata.create_all(engine)
        self._db_session = scoped_session(sessionmaker(bind=engine))

    # ------------------------------------------------------------------
    # Core routing
    # ------------------------------------------------------------------

    async def route(self, task: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Route a task by computing the Pareto frontier and then selecting an expert
        based on adaptive weights and user preferences.
        """
        start_time = time.time()
        try:
            # 1. Get candidate experts
            candidates = self.get_candidate_experts(task, context)

            # 2. Compute vectors for each candidate (cached)
            vectors = {}
            for expert in candidates:
                vec = await self._get_vector(expert, context)
                vectors[expert.expert_id] = vec

            # 3. Apply constraints (filter out experts that violate thresholds)
            filtered_ids = self._apply_constraints(vectors, context)
            if not filtered_ids:
                # If no expert meets constraints, fallback to all candidates
                filtered_ids = list(vectors.keys())
                logger.warning("No expert met constraints, using all candidates")

            # 4. Find Pareto frontier among filtered
            frontier = self._pareto_frontier({pid: vectors[pid] for pid in filtered_ids})

            # 5. Select an expert from the frontier
            selected_id = await self._select_from_frontier(frontier, vectors, context)

            # 6. Fallback if no selection
            if selected_id is None and candidates:
                selected_id = candidates[0].expert_id

            # 7. Generate explanation
            explanation = self._generate_explanation(selected_id, frontier, vectors, context)

            # 8. Record decision
            await self._record_decision(context, selected_id, frontier, vectors, explanation)

            # 9. Return result
            selected_expert = self.registry.get_expert(selected_id) if selected_id else None

            # Update metrics
            ROUTING_DECISIONS.labels(status='success').inc()
            FRONTIER_SIZE.set(len(frontier))
            ROUTING_LATENCY.observe(time.time() - start_time)

            logger.info("Routing decision", selected=selected_id, frontier_size=len(frontier), explanation=explanation)

            return {
                'expert': selected_expert,
                'frontier': [
                    {'expert_id': pid, 'vector': vectors[pid].tolist()}
                    for pid in frontier
                ] if frontier else [],
                'selected_id': selected_id,
                'explanation': explanation,
                'timestamp': datetime.now().isoformat()
            }
        except Exception as e:
            logger.error("Routing failed", error=str(e))
            ROUTING_DECISIONS.labels(status='failed').inc()
            raise

    # ------------------------------------------------------------------
    # Vector computation with caching
    # ------------------------------------------------------------------

    async def _get_vector(self, expert: ExpertProfile, context: Dict[str, Any]) -> np.ndarray:
        """
        Compute or retrieve cached vector for an expert.
        """
        expert_id = expert.expert_id
        async with self._cache_lock:
            now = datetime.now()
            if expert_id in self._cache:
                vec, timestamp = self._cache[expert_id]
                if (now - timestamp).total_seconds() < self.router_config.cache_ttl_seconds:
                    return vec

        # Compute vector using registered objectives
        dependencies = {
            'node_registry': self.node_registry,
            'carbon_manager': self.carbon_manager,
            'cost_function': self.cost_function
        }
        vec = []
        for name in self.objective_names:
            obj = self.objectives.get(name)
            if obj:
                try:
                    # Wrap with retry and circuit breaker for external calls
                    @retry_decorator(attempts=self.router_config.max_retry_attempts)
                    async def compute_obj():
                        return await obj.compute(expert, context, dependencies)
                    value = await self._circuit_breaker.call(compute_obj)
                    vec.append(value)
                except Exception as e:
                    logger.warning(f"Objective {name} failed, using default 0", error=str(e))
                    vec.append(0.0)
            else:
                vec.append(0.0)

        vec = np.array(vec)
        async with self._cache_lock:
            self._cache[expert_id] = (vec, datetime.now())
        return vec

    # ------------------------------------------------------------------
    # Constraint filtering
    # ------------------------------------------------------------------

    def _apply_constraints(self, vectors: Dict[str, np.ndarray], context: Dict) -> List[str]:
        """
        Filter out experts that violate any constraint.
        Constraints are defined in config as {objective_name: max_value}.
        """
        if not self.router_config.constraints:
            return list(vectors.keys())

        objective_order = self.objective_names
        valid = []
        for expert_id, vec in vectors.items():
            ok = True
            for idx, name in enumerate(objective_order):
                if name in self.router_config.constraints:
                    if vec[idx] > self.router_config.constraints[name]:
                        ok = False
                        break
            if ok:
                valid.append(expert_id)
        return valid

    # ------------------------------------------------------------------
    # Pareto frontier computation
    # ------------------------------------------------------------------

    def _pareto_frontier(self, vectors: Dict[str, np.ndarray]) -> List[str]:
        """
        Return the list of expert IDs that are Pareto‑optimal.
        A vector A dominates B if all components of A ≤ B and at least one is strictly less.
        """
        expert_ids = list(vectors.keys())
        n = len(expert_ids)
        dominated = [False] * n

        for i in range(n):
            for j in range(n):
                if i == j:
                    continue
                vec_i = vectors[expert_ids[i]]
                vec_j = vectors[expert_ids[j]]
                if self._dominates(vec_i, vec_j):
                    dominated[j] = True

        return [expert_ids[i] for i in range(n) if not dominated[i]]

    def _dominates(self, a: np.ndarray, b: np.ndarray) -> bool:
        """Return True if a dominates b (all components <= and at least one <)."""
        return np.all(a <= b) and np.any(a < b)

    # ------------------------------------------------------------------
    # Selection from frontier
    # ------------------------------------------------------------------

    async def _select_from_frontier(self, frontier: List[str], vectors: Dict[str, np.ndarray], context: Dict) -> Optional[str]:
        """
        Select an expert from the frontier using adaptive weights and user preferences.
        """
        if not frontier:
            return None

        # Determine weights: use adaptive weights if enabled, else default or user weights
        if self.router_config.use_adaptive_weights:
            weights = self.cost_function.weights
            # Map adaptive weight keys to objective names
            # Assume cost_function.weights has keys: alpha, beta, gamma, delta, epsilon, zeta
            # Map to objective_names order
            key_map = {
                'energy': 'alpha',
                'carbon': 'beta',
                'helium': 'gamma',
                'material': 'delta',
                'latency': 'epsilon',
                'inaccuracy': 'zeta'
            }
            # Build weight vector in the same order as objective_names
            w = []
            for obj in self.objective_names:
                key = key_map.get(obj, 'alpha')
                w.append(weights.get(key, 1.0))
            weights = np.array(w)
        else:
            # Use user preferences if available
            if self.user_prefs:
                user_weights = self.user_prefs.get_weights()
                # Map similarly
                w = []
                for obj in self.objective_names:
                    key = obj  # assume user prefs use same names
                    w.append(user_weights.get(key, 1.0))
                weights = np.array(w)
            else:
                # Use default weights from config
                w = []
                for obj in self.objective_names:
                    w.append(self.router_config.default_weights.get(obj, 1.0))
                weights = np.array(w)

        # Normalise weights (sum to 1)
        if np.sum(weights) > 0:
            weights = weights / np.sum(weights)

        # Compute weighted score for each frontier point (lower is better)
        best_id = None
        best_score = float('inf')
        for pid in frontier:
            vec = vectors[pid]
            score = np.dot(weights, vec)
            if score < best_score:
                best_score = score
                best_id = pid

        # If weights are all zero, fallback to knee
        if best_id is None:
            best_id = self._select_knee(frontier, vectors)

        return best_id

    def _select_knee(self, frontier: List[str], vectors: Dict[str, np.ndarray]) -> Optional[str]:
        """
        Select the 'knee' point: the point closest to the ideal point (min of each component)
        using weighted Euclidean distance based on default weights.
        """
        if not frontier:
            return None

        # Compute ideal point (component‑wise minimum)
        vecs = [vectors[pid] for pid in frontier]
        ideal = np.min(vecs, axis=0)

        # Use default weights for distance
        weights = np.array([self.router_config.default_weights.get(obj, 1.0) for obj in self.objective_names])
        if np.sum(weights) > 0:
            weights = weights / np.sum(weights)

        best_id = None
        best_dist = float('inf')
        for pid in frontier:
            vec = vectors[pid]
            diff = (vec - ideal) * weights
            dist = np.linalg.norm(diff)
            if dist < best_dist:
                best_dist = dist
                best_id = pid
        return best_id

    # ------------------------------------------------------------------
    # Explanation generation
    # ------------------------------------------------------------------

    def _generate_explanation(self, selected_id: str, frontier: List[str], vectors: Dict[str, np.ndarray], context: Dict) -> str:
        """
        Generate a human‑readable explanation for the routing decision.
        """
        if selected_id is None:
            return "No expert selected."

        selected_vec = vectors[selected_id]
        objective_names = self.objective_names
        parts = [f"Selected expert {selected_id}"]
        if selected_id in frontier:
            parts.append(" (on the Pareto frontier)")
        parts.append(" with objective values: ")

        values_str = ", ".join([f"{name}={selected_vec[i]:.3f}" for i, name in enumerate(objective_names)])
        parts.append(values_str)

        if len(frontier) > 1:
            # Compare to average of frontier
            frontier_vecs = [vectors[pid] for pid in frontier]
            avg_vec = np.mean(frontier_vecs, axis=0)
            diffs = selected_vec - avg_vec
            # Find objectives where selected is better (lower)
            better = [name for i, name in enumerate(objective_names) if diffs[i] < -0.05]
            worse = [name for i, name in enumerate(objective_names) if diffs[i] > 0.05]
            if better:
                parts.append(f" Better than average on: {', '.join(better)}")
            if worse:
                parts.append(f" Worse than average on: {', '.join(worse)}")

        return "".join(parts)

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    async def _record_decision(self, context: Dict, selected_id: str, frontier: List[str], vectors: Dict[str, np.ndarray], explanation: str):
        """
        Save routing decision to database.
        """
        if not self.router_config.enable_persistence or not self._db_session:
            return
        try:
            session = self._db_session()
            session.execute(
                text("""
                    INSERT INTO routing_decisions
                    (request_id, task_id, selected_expert_id, frontier_size, selection_reason, vector_scores)
                    VALUES (:request_id, :task_id, :selected_expert_id, :frontier_size, :selection_reason, :vector_scores)
                """),
                {
                    'request_id': context.get('request_id'),
                    'task_id': context.get('task_id'),
                    'selected_expert_id': selected_id,
                    'frontier_size': len(frontier),
                    'selection_reason': explanation,
                    'vector_scores': json.dumps({pid: vectors[pid].tolist() for pid in frontier})
                }
            )
            session.commit()
        except Exception as e:
            logger.warning("Failed to persist routing decision", error=str(e))
            session.rollback()
        finally:
            session.close()

    # ------------------------------------------------------------------
    # Public utility methods
    # ------------------------------------------------------------------

    async def get_frontier(self, task: Dict[str, Any], context: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Return only the Pareto frontier without selecting an expert.
        Useful for visualisation or decision support.
        """
        candidates = self.get_candidate_experts(task, context)
        vectors = {}
        for expert in candidates:
            vec = await self._get_vector(expert, context)
            vectors[expert.expert_id] = vec

        frontier = self._pareto_frontier(vectors)
        return [
            {'expert_id': pid, 'vector': vectors[pid].tolist()}
            for pid in frontier
        ]

    async def clear_cache(self):
        """Clear the vector cache."""
        async with self._cache_lock:
            self._cache.clear()
        logger.info("Vector cache cleared")

    # ------------------------------------------------------------------
    # Shutdown
    # ------------------------------------------------------------------

    async def shutdown(self):
        """Clean up resources."""
        if self._db_session:
            self._db_session.remove()
        logger.info("ParetoRouter shut down")

# ---------- Example usage ----------
if __name__ == "__main__":
    import asyncio
    import random

    # Mock dependencies
    class MockExpertRouter(ExpertRouter):
        def get_candidate_experts(self, task, context):
            return [
                ExpertProfile(f"expert_{i}",
                              energy_per_inference=random.uniform(1, 10),
                              carbon_per_inference=random.uniform(0.1, 1.0),
                              helium_per_inference=random.uniform(0, 5),
                              accuracy_score=random.uniform(0.7, 1.0))
                for i in range(5)
            ]
        @property
        def registry(self):
            return None

    class MockCostFunction(AdaptiveCostFunction):
        @property
        def weights(self):
            return {'alpha': 0.8, 'beta': 0.2, 'gamma': 0.5, 'delta': 0.3, 'epsilon': 0.1, 'zeta': 0.1}

    class MockNodeRegistry:
        async def get_node(self, node_id):
            return {'material_index': 1.5, 'helium_index': 0.2}

    async def main():
        config = {
            'pareto': {
                'cache_ttl_seconds': 300,
                'use_adaptive_weights': True,
                'enable_persistence': True,
                'constraints': {'latency': 200}  # only if latency < 200ms
            }
        }
        router = ParetoRouter(
            config=config,
            cost_function=MockCostFunction(),
            node_registry=MockNodeRegistry(),
            carbon_manager=None,
            user_preferences=None,
            objectives=['energy', 'carbon', 'helium', 'material', 'latency', 'inaccuracy']
        )
        # Override get_candidate_experts for testing
        router.get_candidate_experts = lambda task, ctx: [
            ExpertProfile(f"expert_{i}",
                          energy_per_inference=random.uniform(1, 10),
                          carbon_per_inference=random.uniform(0.1, 1.0),
                          helium_per_inference=random.uniform(0, 5),
                          accuracy_score=random.uniform(0.7, 1.0))
            for i in range(5)
        ]

        task = {}
        context = {'token_count': 10, 'expected_latency_ms': 150, 'task_id': 'task_123', 'request_id': 'req_123'}
        result = await router.route(task, context)
        print("Routing result:", result)

        # Get frontier only
        frontier = await router.get_frontier(task, context)
        print("Frontier:", frontier)

        await router.shutdown()

    asyncio.run(main())
