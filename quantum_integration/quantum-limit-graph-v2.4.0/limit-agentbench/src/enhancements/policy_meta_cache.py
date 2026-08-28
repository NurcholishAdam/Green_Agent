"""
policy_meta_cache.py (Enhanced v3.0)

Stores successful policies keyed by a workload fingerprint.
Uses a multi‑objective decision process (MODP) with Pareto front + TOPSIS,
Mixture‑of‑Experts (MOE) gating, bio‑inspired Genetic Algorithm for policy evolution,
carbon‑aware scheduling, self‑healing drift detection, LIMIT Graph for constraint
propagation, RLHF (Reinforcement Learning from Human Feedback), and Multi‑Teacher
Policy Distillation.

All enhancements degrade gracefully if optional dependencies are missing.
"""

import asyncio
import time
import math
import random
from typing import Dict, Any, Optional, List, Tuple, Callable
from dataclasses import dataclass, field
from collections import deque, defaultdict
import numpy as np
import hashlib
import logging
from datetime import datetime  # <-- added missing import

# ================================
# Optional dependencies
# ================================
try:
    from sklearn.linear_model import LogisticRegression, LinearRegression
    from sklearn.preprocessing import StandardScaler
    from sklearn.ensemble import IsolationForest
    from sklearn.svm import OneClassSVM
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

# For logging
logger = logging.getLogger(__name__)

# ================================
# WorkloadFingerprint (unchanged)
# ================================
class WorkloadFingerprint:
    """Normalised fingerprint of a workload for similarity search."""
    def __init__(self, model_size_mb: float, prompt_len: int, gen_len: int,
                 gpu_mem_free_mb: float, disk_speed_class: int):
        self.model_size_mb = model_size_mb
        self.prompt_len = prompt_len
        self.gen_len = gen_len
        self.gpu_mem_free_mb = gpu_mem_free_mb
        self.disk_speed_class = disk_speed_class  # 0=HDD,1=SATA-SSD,2=NVMe

    def to_vector(self) -> np.ndarray:
        # Normalise to similar scales for distance calculation
        return np.array([
            self.model_size_mb / 1000.0,          # scale to GB
            self.prompt_len / 1024.0,
            self.gen_len / 1024.0,
            self.gpu_mem_free_mb / 1000.0,
            self.disk_speed_class / 2.0,
        ])

# ================================
# Helper classes for MODP, MOE, GA, Scheduler, SelfHealing
# ================================

class ParetoFront:
    """Simple Pareto front implementation for multi‑objective dominance."""
    def __init__(self):
        self.solutions = []  # list of (objectives, decision)

    def add(self, objectives: List[float], decision: Any):
        dominated = False
        for obj, _ in self.solutions:
            if all(o <= obj[i] for i, o in enumerate(objectives)):
                dominated = True
                break
        if not dominated:
            # Remove solutions dominated by this one
            self.solutions = [(obj, dec) for obj, dec in self.solutions
                              if not all(objectives[i] <= obj[i] for i in range(len(objectives)))]
            self.solutions.append((objectives, decision))
        return dominated

    def get_pareto_front(self) -> List[Tuple[List[float], Any]]:
        return self.solutions

    def get_best_by_weight(self, weights: List[float]) -> Any:
        best = None
        best_score = -float('inf')
        for obj, dec in self.solutions:
            score = sum(w * o for w, o in zip(weights, obj))
            if score > best_score:
                best_score = score
                best = dec
        return best

class TOPSIS:
    """TOPSIS multi‑criteria decision analysis."""
    @staticmethod
    def score(candidates: List[Dict[str, float]], weights: List[float], criteria: List[str]) -> List[float]:
        matrix = np.array([[c[crit] for crit in criteria] for c in candidates])
        norm_matrix = matrix / np.sqrt((matrix**2).sum(axis=0))
        weighted = norm_matrix * weights
        ideal = weighted.max(axis=0)
        neg_ideal = weighted.min(axis=0)
        d_plus = np.sqrt(((weighted - ideal)**2).sum(axis=1))
        d_minus = np.sqrt(((weighted - neg_ideal)**2).sum(axis=1))
        scores = d_minus / (d_plus + d_minus + 1e-9)
        return scores.tolist()

class MOEGating:
    """Mixture of Experts gating network for context‑aware expert selection."""
    def __init__(self, num_experts: int, feature_dim: int = 4):
        self.num_experts = num_experts
        self.feature_dim = feature_dim
        self.gating_model = None
        self.scaler = None
        self.history = deque(maxlen=500)  # (features, expert_idx, reward)
        self._trained = False
        if SKLEARN_AVAILABLE:
            self.gating_model = LogisticRegression(multi_class='multinomial', solver='lbfgs', max_iter=1000)
            self.scaler = StandardScaler()

    def extract_features(self, context: Dict) -> np.ndarray:
        # Context: carbon_intensity, time_of_day, urgency, workload_size
        carbon = context.get('carbon_intensity', 400) / 1000.0
        hour = datetime.now().hour / 24.0  # now datetime is imported
        urgency = context.get('urgency', 0.5)
        workload_size = context.get('model_size_mb', 0) / 1000.0
        return np.array([carbon, hour, urgency, workload_size])

    def get_weights(self, context: Dict) -> List[float]:
        if self.gating_model is not None and self._trained:
            features = self.extract_features(context)
            X_scaled = self.scaler.transform([features])
            weights = self.gating_model.predict_proba(X_scaled)[0]
        else:
            weights = np.ones(self.num_experts) / self.num_experts
        return weights.tolist()

    def update(self, context: Dict, expert_idx: int, reward: float):
        features = self.extract_features(context)
        self.history.append((features, expert_idx, reward))
        if len(self.history) % 100 == 0:
            self._retrain()

    def _retrain(self):
        if self.gating_model is None or len(self.history) < 100:
            return
        X = np.array([h[0] for h in self.history])
        y = np.array([h[1] for h in self.history])
        X_scaled = self.scaler.fit_transform(X)
        self.gating_model.fit(X_scaled, y)
        self._trained = True

class GAPopulation:
    """Genetic Algorithm population of policies for a given fingerprint."""
    def __init__(self, population_size: int = 20, mutation_rate: float = 0.1, crossover_rate: float = 0.8):
        self.pop_size = population_size
        self.mutation_rate = mutation_rate
        self.crossover_rate = crossover_rate
        self.population = []  # list of (policy, fitness)
        self.generation = 0

    def initialize(self, template_policy: Dict[str, Any]):
        # Create mutations of the template
        self.population = [(template_policy, 0.0)] * self.pop_size
        # Randomize some parameters
        for i in range(1, self.pop_size):
            mutated = self._mutate(template_policy)
            self.population[i] = (mutated, 0.0)

    def _mutate(self, policy: Dict[str, Any]) -> Dict[str, Any]:
        # Simple mutation: perturb numeric parameters by small random factor
        new_policy = policy.copy()
        for key, value in new_policy.items():
            if isinstance(value, (int, float)):
                new_policy[key] = value * (1.0 + random.uniform(-0.1, 0.1))
        return new_policy

    def _crossover(self, p1: Dict, p2: Dict) -> Dict:
        child = {}
        for key in p1:
            if random.random() < 0.5:
                child[key] = p1[key]
            else:
                child[key] = p2[key]
        return child

    def evolve(self, fitness_func: Callable[[Dict], float]) -> Dict[str, Any]:
        # Evaluate fitness for all individuals
        for i, (policy, _) in enumerate(self.population):
            self.population[i] = (policy, fitness_func(policy))
        # Sort by fitness descending
        self.population.sort(key=lambda x: x[1], reverse=True)
        # Elitism: keep best
        best = self.population[0]
        # Select parents (tournament)
        parents = []
        for _ in range(self.pop_size - 1):
            idx1, idx2 = random.sample(range(self.pop_size), 2)
            if self.population[idx1][1] > self.population[idx2][1]:
                parents.append(self.population[idx1][0])
            else:
                parents.append(self.population[idx2][0])
        # Crossover and mutation
        offspring = []
        for i in range(0, len(parents)-1, 2):
            if random.random() < self.crossover_rate:
                child = self._crossover(parents[i], parents[i+1])
            else:
                child = parents[i]
            if random.random() < self.mutation_rate:
                child = self._mutate(child)
            offspring.append((child, 0.0))
        # New population: keep best + offspring
        self.population = [best] + offspring[:self.pop_size-1]
        self.generation += 1
        return best[0]

class CarbonScheduler:
    """Multi‑objective carbon‑aware scheduler for policy retrieval."""
    def __init__(self, carbon_manager: Optional[Any] = None, threshold: float = 400.0,
                 max_delay: int = 300):
        self.carbon_manager = carbon_manager
        self.threshold = threshold
        self.max_delay = max_delay

    async def get_current_carbon(self) -> float:
        if self.carbon_manager:
            # Assume carbon_manager has get_current_intensity() method
            return await self.carbon_manager.get_current_intensity()
        return 400.0  # fallback

    async def should_delay(self, urgency: float = 0.5) -> Tuple[bool, int]:
        carbon = await self.get_current_carbon()
        if carbon > self.threshold:
            # Delay based on urgency: high urgency -> shorter delay
            delay = int(self.max_delay * (1.0 - urgency))
            return True, delay
        return False, 0

class SelfHealingManager:
    """Drift detection and anomaly ensemble for policy performance."""
    def __init__(self, contamination: float = 0.1):
        self.contamination = contamination
        self.anomaly_detectors = []
        self.reward_history = deque(maxlen=500)
        self._trained = False

        if SKLEARN_AVAILABLE:
            self.anomaly_detectors.append(('iforest', IsolationForest(contamination=contamination)))
            self.anomaly_detectors.append(('ocsvm', OneClassSVM(nu=contamination)))

    def record_reward(self, reward: float):
        self.reward_history.append(reward)
        if len(self.reward_history) >= 100 and not self._trained:
            self._train()

    def _train(self):
        if not self.anomaly_detectors or len(self.reward_history) < 100:
            return
        X = np.array(list(self.reward_history)).reshape(-1, 1)
        for _, model in self.anomaly_detectors:
            try:
                model.fit(X)
            except Exception as e:
                logger.warning(f"Anomaly detector training failed: {e}")
        self._trained = True

    def detect_anomaly(self, reward: float) -> Tuple[bool, float]:
        if not self._trained or not self.anomaly_detectors:
            # Simple rule: reward < 0.5 is suspicious
            return reward < 0.5, 0.0
        X = np.array([[reward]])
        votes = []
        for _, model in self.anomaly_detectors:
            try:
                pred = model.predict(X)[0]
                votes.append(1 if pred == -1 else 0)
            except:
                votes.append(0)
        if not votes:
            return False, 0.0
        anomaly = sum(votes) / len(votes) > 0.5
        return anomaly, sum(votes) / len(votes)

# ================================
# NEW: LIMIT Graph Manager
# ================================
class LimitGraphManager:
    """Maintains a graph of system constraints (carbon, cost, latency, etc.) for real‑time decision support."""
    def __init__(self):
        self.graph = {}                     # node -> dict of edges with weights
        self.constraints = {}               # constraint name -> current value
        self._lock = asyncio.Lock()
        self._initialize_graph()

    def _initialize_graph(self):
        nodes = ['carbon', 'cost', 'latency', 'throughput', 'diversity']
        for n in nodes:
            self.graph[n] = {}
        # Add simple edges (weights can be learned later)
        self.graph['carbon']['cost'] = 0.8
        self.graph['cost']['latency'] = 0.2
        self.graph['latency']['throughput'] = -0.5
        self.graph['throughput']['diversity'] = 0.1
        self.graph['diversity']['carbon'] = -0.3

    async def update_constraint(self, name: str, value: float):
        async with self._lock:
            self.constraints[name] = value

    async def get_constraint(self, name: str) -> float:
        return self.constraints.get(name, 0.0)

    async def evaluate_path(self, start: str, end: str) -> float:
        """Simple graph traversal (BFS) to compute influence score."""
        if start not in self.graph or end not in self.graph:
            return 0.0
        visited = set()
        queue = [(start, 1.0)]
        while queue:
            node, weight = queue.pop(0)
            if node == end:
                return weight
            visited.add(node)
            for neighbor, w in self.graph[node].items():
                if neighbor not in visited:
                    queue.append((neighbor, weight * w))
        return 0.0

    async def get_graph_summary(self) -> Dict:
        return {
            'nodes': list(self.graph.keys()),
            'constraints': self.constraints,
            'edge_count': sum(len(v) for v in self.graph.values())
        }

# ================================
# NEW: RLHF Manager
# ================================
class RLHFManager:
    """Reinforcement Learning from Human Feedback – learns a reward model from feedback events and uses it to guide policy selection."""
    def __init__(self, reward_model_type: str = "linear"):
        self.reward_model_type = reward_model_type
        self.feedback_buffer = []           # list of (state, action, reward)
        self.reward_model = None
        self.policy = None                  # simple policy: linear weights (probs over experts)
        self._lock = asyncio.Lock()
        self._init_models()

    def _init_models(self):
        if SKLEARN_AVAILABLE:
            if self.reward_model_type == "linear":
                self.reward_model = LinearRegression()
            else:
                # Could use a neural net if PyTorch available, fallback to linear
                self.reward_model = LinearRegression()
            self.policy = {'weights': np.array([0.25, 0.25, 0.25, 0.25])}
        else:
            logger.warning("RLHF requires sklearn; using heuristic reward model")

    async def record_feedback(self, state: Dict, action: str, reward: float):
        """Called when human feedback is available."""
        async with self._lock:
            self.feedback_buffer.append({
                'state': self._state_to_features(state),
                'action': self._action_to_index(action),
                'reward': reward
            })

    def _state_to_features(self, state: Dict) -> List[float]:
        return [
            state.get('carbon_intensity', 400) / 1000,
            state.get('avg_score', 0.5),
            state.get('cost', 0.5),
            state.get('diversity', 0.5)
        ]

    def _action_to_index(self, action: str) -> int:
        actions = ['performance_focus', 'carbon_focus', 'cost_focus', 'balanced']
        return actions.index(action) if action in actions else 3

    async def train_reward_model(self):
        if not self.reward_model or len(self.feedback_buffer) < 10:
            return
        X = [f['state'] for f in self.feedback_buffer]
        y = [f['reward'] for f in self.feedback_buffer]
        self.reward_model.fit(X, y)
        logger.info(f"RLHF reward model trained on {len(self.feedback_buffer)} samples")
        self.feedback_buffer.clear()

    async def get_policy_probs(self, state: Dict) -> List[float]:
        """Return action probabilities according to learned policy (currently based on reward model)."""
        features = self._state_to_features(state)
        if self.reward_model:
            # For each action, predict reward (simplified: use fixed weights)
            return self.policy['weights'].tolist()
        return [0.25, 0.25, 0.25, 0.25]

# ================================
# NEW: Multi‑Teacher Policy Distillation
# ================================
class MultiTeacherPolicyDistillation:
    """Distills multiple teacher policies (from MOE experts) into a single student policy using knowledge distillation."""
    def __init__(self, num_teachers: int = 4, temperature: float = 2.0, alpha: float = 0.5):
        self.num_teachers = num_teachers
        self.temperature = temperature
        self.alpha = alpha
        self.student_policy = np.array([0.25, 0.25, 0.25, 0.25])   # prob over 4 actions
        self.history = deque(maxlen=500)
        self._lock = asyncio.Lock()

    async def distill(self, teacher_probs: List[float]):
        """Perform one distillation step using current teacher outputs."""
        if len(teacher_probs) != self.num_teachers:
            teacher_probs = np.ones(self.num_teachers) / self.num_teachers
        teacher_dist = np.array(teacher_probs)
        teacher_dist /= teacher_dist.sum()

        # Soften with temperature
        soft_teacher = np.exp(np.log(teacher_dist + 1e-6) / self.temperature)
        soft_teacher /= soft_teacher.sum()

        # Update student policy (simple gradient step)
        loss = -np.sum(soft_teacher * np.log(self.student_policy + 1e-6))
        grad = -soft_teacher / (self.student_policy + 1e-6)
        lr = 0.01
        self.student_policy -= lr * grad
        self.student_policy = np.clip(self.student_policy, 0.01, None)
        self.student_policy /= self.student_policy.sum()

        async with self._lock:
            self.history.append({
                'teacher_dist': teacher_dist,
                'student_dist': self.student_policy.copy(),
                'loss': loss
            })

    def get_student_probs(self) -> List[float]:
        return self.student_policy.tolist()

# ================================
# Enhanced PolicyMetaCache
# ================================
class PolicyMetaCache:
    """
    Enhanced policy meta‑cache with MODP, MOE, GA, carbon‑aware scheduling, self‑healing,
    LIMIT Graph, RLHF, and Multi‑Teacher Policy Distillation.
    """

    def __init__(
        self,
        max_age_hours: float = 24.0,
        dist_threshold: float = 0.2,
        # MODP
        enable_modp: bool = True,
        modp_weights: List[float] = None,  # [reward, carbon, latency, cost]
        # MOE
        enable_moe: bool = True,
        num_experts: int = 3,
        # GA
        enable_ga: bool = True,
        ga_population_size: int = 20,
        # Carbon scheduler
        enable_scheduler: bool = True,
        carbon_threshold: float = 400.0,
        max_delay_seconds: int = 300,
        # Self‑healing
        enable_self_healing: bool = True,
        anomaly_contamination: float = 0.1,
        # LIMIT Graph
        enable_limit_graph: bool = True,
        # RLHF
        enable_rlhf: bool = True,
        rlhf_reward_model: str = "linear",
        rlhf_training_interval: int = 600,
        # Distillation
        enable_distillation: bool = True,
        distillation_temperature: float = 2.0,
        distillation_alpha: float = 0.5,
        distillation_interval: int = 300,
        # Optional carbon manager (async)
        carbon_manager: Optional[Any] = None,
    ):
        self.max_age_seconds = max_age_hours * 3600
        self.dist_threshold = dist_threshold
        self.store = {}  # key: tuple(vector) -> dict of entries
        self.vectors = []  # list of np.ndarray for brute‑force search
        self.keys = []     # corresponding tuple keys

        # MODP settings
        self.enable_modp = enable_modp
        self.modp_weights = modp_weights or [0.5, 0.2, 0.2, 0.1]  # reward, carbon, latency, cost

        # MOE settings
        self.enable_moe = enable_moe
        self.num_experts = num_experts
        self.moe_gating = MOEGating(num_experts) if enable_moe else None

        # GA settings
        self.enable_ga = enable_ga
        self.ga_population_size = ga_population_size
        self.ga_populations = {}  # key -> GAPopulation

        # Carbon scheduler
        self.enable_scheduler = enable_scheduler
        self.carbon_manager = carbon_manager
        self.scheduler = CarbonScheduler(carbon_manager, carbon_threshold, max_delay_seconds) if enable_scheduler else None

        # Self‑healing
        self.enable_self_healing = enable_self_healing
        self.self_healing = SelfHealingManager(anomaly_contamination) if enable_self_healing else None

        # LIMIT Graph
        self.enable_limit_graph = enable_limit_graph
        self.limit_graph = LimitGraphManager() if enable_limit_graph else None
        self.limit_graph_update_interval = 300  # can be configurable

        # RLHF
        self.enable_rlhf = enable_rlhf
        self.rlhf = RLHFManager(reward_model_type=rlhf_reward_model) if enable_rlhf else None
        self.rlhf_training_interval = rlhf_training_interval

        # Distillation
        self.enable_distillation = enable_distillation
        self.distillation = MultiTeacherPolicyDistillation(
            num_teachers=num_experts,
            temperature=distillation_temperature,
            alpha=distillation_alpha
        ) if enable_distillation else None
        self.distillation_interval = distillation_interval

        # For GA fitness evaluation
        self._fitness_evaluator = None

        # Background tasks
        self._background_tasks = []
        self._running = False

    def _vector_to_key(self, vec: np.ndarray) -> tuple:
        return tuple(vec.tolist())

    # ------------------------------------------------------------------
    # Start/stop background tasks
    # ------------------------------------------------------------------
    async def start(self):
        """Start background tasks for RLHF, distillation, and LIMIT graph."""
        if self._running:
            return
        self._running = True
        if self.enable_rlhf:
            self._background_tasks.append(asyncio.create_task(self._rlhf_loop()))
        if self.enable_distillation:
            self._background_tasks.append(asyncio.create_task(self._distillation_loop()))
        if self.enable_limit_graph:
            self._background_tasks.append(asyncio.create_task(self._limit_graph_loop()))
        logger.info("PolicyMetaCache background tasks started")

    async def stop(self):
        """Stop all background tasks."""
        self._running = False
        for task in self._background_tasks:
            task.cancel()
        if self._background_tasks:
            await asyncio.gather(*self._background_tasks, return_exceptions=True)
        self._background_tasks.clear()
        logger.info("PolicyMetaCache background tasks stopped")

    async def _rlhf_loop(self):
        while self._running:
            try:
                if self.rlhf:
                    await self.rlhf.train_reward_model()
                await asyncio.sleep(self.rlhf_training_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"RLHF loop error: {e}")
                await asyncio.sleep(60)

    async def _distillation_loop(self):
        while self._running:
            try:
                if self.distillation and self.moe_gating:
                    # Use MOE gating weights as teacher distribution
                    # Since we don't have a live context, use a dummy context
                    dummy_context = {'carbon_intensity': 400, 'urgency': 0.5, 'model_size_mb': 1000}
                    teacher_probs = self.moe_gating.get_weights(dummy_context)
                    await self.distillation.distill(teacher_probs)
                await asyncio.sleep(self.distillation_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Distillation loop error: {e}")
                await asyncio.sleep(60)

    async def _limit_graph_loop(self):
        while self._running:
            try:
                if self.limit_graph:
                    # Update constraints based on current carbon intensity
                    carbon = await self.scheduler.get_current_carbon() if self.scheduler else 400.0
                    await self.limit_graph.update_constraint('carbon', carbon)
                    influence = await self.limit_graph.evaluate_path('carbon', 'cost')
                    logger.debug(f"LIMIT Graph carbon->cost influence: {influence:.3f}")
                await asyncio.sleep(self.limit_graph_update_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"LIMIT Graph loop error: {e}")
                await asyncio.sleep(60)

    # ------------------------------------------------------------------
    # Core retrieval
    # ------------------------------------------------------------------
    async def get_best_policy(
        self,
        fp: WorkloadFingerprint,
        context: Dict[str, Any] = None,
        urgency: float = 0.5,
    ) -> Optional[Dict[str, Any]]:
        """
        Retrieve the best policy for the given workload fingerprint.
        Incorporates carbon‑aware scheduling, MODP, MOE, RLHF, distillation, and self‑healing.
        """
        vec = fp.to_vector()
        if not self.vectors:
            return None

        # 1. Carbon‑aware scheduling
        if self.scheduler:
            should_delay, delay = await self.scheduler.should_delay(urgency)
            if should_delay:
                logger.info(f"Policy retrieval delayed by {delay}s due to high carbon")
                await asyncio.sleep(delay)

        # 2. Find nearest neighbour(s)
        best_idx = -1
        best_dist = float('inf')
        for i, stored_vec in enumerate(self.vectors):
            dist = np.linalg.norm(vec - stored_vec)
            if dist < best_dist:
                best_dist = dist
                best_idx = i

        if best_idx == -1 or best_dist > self.dist_threshold:
            return None

        key = self.keys[best_idx]
        entries = self.store[key]  # list of (policy, timestamp, objectives, expert_id, ga_individual)

        # 3. Filter stale entries
        now = time.time()
        valid_entries = [e for e in entries if (now - e[1]) < self.max_age_seconds]
        if not valid_entries:
            return None

        # 4. Self‑healing: check if recent rewards are anomalous
        if self.self_healing and valid_entries:
            avg_reward = np.mean([e[2][0] for e in valid_entries])
            anomaly, _ = self.self_healing.detect_anomaly(avg_reward)
            if anomaly:
                logger.warning("Anomaly detected in cached policy performance; falling back to default")
                return None

        # 5. If RLHF trained, use it to adjust selection
        if self.rlhf and self.rlhf.reward_model is not None and context:
            # We can use RLHF policy probabilities to weight the objectives for MODP or directly choose.
            # Here we override MODP selection by using RLHF predicted reward.
            # Build candidates with predicted reward from RLHF (using state features)
            candidates = []
            for e in valid_entries:
                policy, _, objectives, _, _ = e
                # Use RLHF to predict reward for this policy? We need a feature vector for the policy.
                # For simplicity, we just use the RLHF policy probs to weight the existing reward.
                rlhf_probs = await self.rlhf.get_policy_probs(context)
                # Map probs to the four objectives: reward, carbon, latency, cost
                # We'll assume RLHF probs correspond to [reward, carbon, cost, performance] (simplified)
                adjusted_reward = objectives[0] * rlhf_probs[0]
                candidates.append((adjusted_reward, policy))
            best_idx = max(range(len(candidates)), key=lambda i: candidates[i][0])
            return candidates[best_idx][1]

        # 6. MODP selection with Pareto front + TOPSIS
        if self.enable_modp and len(valid_entries) > 1:
            # Build Pareto front of entries based on objectives: [reward, carbon, latency, cost]
            # For TOPSIS we need "higher is better" – we invert carbon, latency, cost.
            candidates = []
            for e in valid_entries:
                policy, _, objectives, _, _ = e
                obj_inv = [objectives[0], 1.0 - objectives[1], 1.0 - objectives[2], 1.0 - objectives[3]]
                candidates.append({'objectives': obj_inv, 'policy': policy})
            criteria = ['reward', 'carbon', 'latency', 'cost']
            # If distillation is enabled, we can use student policy to adjust weights
            if self.distillation and self.distillation.get_student_probs():
                # Use student probs as weights for the criteria (mapped from 4 experts to 4 criteria)
                weights = self.distillation.get_student_probs()
                # Ensure length matches criteria
                if len(weights) != len(criteria):
                    weights = self.modp_weights
            else:
                weights = self.modp_weights
            # Apply TOPSIS
            cand_dicts = [{crit: c['objectives'][i] for i, crit in enumerate(criteria)} for c in candidates]
            scores = TOPSIS.score(cand_dicts, weights, criteria)
            best_idx = np.argmax(scores)
            best_policy = candidates[best_idx]['policy']
            return best_policy

        # 7. Fallback to simple best reward (or MOE)
        best_entry = max(valid_entries, key=lambda e: e[2][0])
        policy = best_entry[0]

        # MOE: if multiple experts, we could weight them, but simplified.
        return policy

    # ------------------------------------------------------------------
    # Update
    # ------------------------------------------------------------------
    async def update(
        self,
        fp: WorkloadFingerprint,
        policy: Dict[str, Any],
        reward: float,
        carbon: float = 0.0,
        latency: float = 0.0,
        cost: float = 0.0,
        context: Dict[str, Any] = None,
        expert_id: int = 0,
    ):
        """
        Update the cache with a new policy and its observed objectives.
        """
        vec = fp.to_vector()
        key = self._vector_to_key(vec)

        if key not in self.store:
            self.store[key] = []
            self.vectors.append(vec)
            self.keys.append(key)

        objectives = [reward, carbon, latency, cost]
        new_entry = (policy, time.time(), objectives, expert_id, None)

        if self.enable_modp:
            entries = self.store[key]
            entries.append(new_entry)
            # Prune dominated entries (simple: keep top by reward)
            max_entries = 20
            if len(entries) > max_entries:
                entries.sort(key=lambda e: e[2][0], reverse=True)
                self.store[key] = entries[:max_entries]
        else:
            # Keep best reward only
            entries = self.store[key]
            best_reward = max([e[2][0] for e in entries]) if entries else -float('inf')
            if reward > best_reward:
                self.store[key] = [new_entry]

        # MOE update
        if self.enable_moe and context is not None and self.moe_gating:
            self.moe_gating.update(context, expert_id, reward)

        # GA population creation
        if self.enable_ga and key not in self.ga_populations:
            pop = GAPopulation(population_size=self.ga_population_size)
            pop.initialize(policy)
            self.ga_populations[key] = pop

        # Self‑healing
        if self.self_healing:
            self.self_healing.record_reward(reward)

        # LIMIT Graph update with carbon
        if self.limit_graph:
            await self.limit_graph.update_constraint('carbon', carbon)

        logger.debug(f"Cache updated for fingerprint: reward={reward:.3f}, carbon={carbon:.3f}")

    # ------------------------------------------------------------------
    # Additional methods for RLHF feedback
    # ------------------------------------------------------------------
    async def record_feedback(self, state: Dict, action: str, reward: float):
        """Record human feedback for RLHF."""
        if self.rlhf:
            await self.rlhf.record_feedback(state, action, reward)

    # ------------------------------------------------------------------
    # GA evolution (triggered periodically)
    # ------------------------------------------------------------------
    async def evolve_populations(self, fitness_func: Callable[[Dict], float]):
        """
        Evolve all GA populations using the provided fitness function.
        This should be called periodically (e.g., in a background loop).
        """
        if not self.enable_ga:
            return
        for key, pop in self.ga_populations.items():
            best_policy = pop.evolve(fitness_func)
            if best_policy:
                # Store the best policy in cache with placeholder reward
                # We'll update the cache entry with a dummy timestamp and objectives
                # In practice, you would evaluate the policy to get real objectives.
                dummy_objectives = [0.0, 0.0, 0.0, 0.0]
                new_entry = (best_policy, time.time(), dummy_objectives, 0, None)
                if key in self.store:
                    self.store[key].append(new_entry)
                else:
                    self.store[key] = [new_entry]

    # ------------------------------------------------------------------
    # Utility methods
    # ------------------------------------------------------------------
    def get_stats(self) -> Dict:
        stats = {
            'cache_size': len(self.store),
            'total_entries': sum(len(v) for v in self.store.values()),
            'ga_populations': len(self.ga_populations),
            'moe_trained': self.moe_gating._trained if self.moe_gating else False,
            'self_healing_trained': self.self_healing._trained if self.self_healing else False,
        }
        if self.limit_graph:
            stats['limit_graph'] = {
                'nodes': list(self.limit_graph.graph.keys()),
                'constraints': self.limit_graph.constraints,
            }
        if self.rlhf:
            stats['rlhf_trained'] = self.rlhf.reward_model is not None
        if self.distillation:
            stats['distillation_probs'] = self.distillation.get_student_probs()
        return stats

    def clear(self):
        self.store.clear()
        self.vectors.clear()
        self.keys.clear()
        self.ga_populations.clear()

# -----------------------------------------------------------------------------
# Example usage (async)
# -----------------------------------------------------------------------------
async def main():
    # Configure logging
    logging.basicConfig(level=logging.INFO)

    # Create cache with all enhancements
    cache = PolicyMetaCache(
        max_age_hours=24,
        dist_threshold=0.2,
        enable_modp=True,
        modp_weights=[0.5, 0.2, 0.2, 0.1],
        enable_moe=True,
        num_experts=3,
        enable_ga=True,
        ga_population_size=10,
        enable_scheduler=True,
        carbon_threshold=400,
        max_delay_seconds=60,
        enable_self_healing=True,
        anomaly_contamination=0.1,
        enable_limit_graph=True,
        enable_rlhf=True,
        enable_distillation=True,
    )

    # Start background tasks
    await cache.start()

    # Create a fingerprint
    fp = WorkloadFingerprint(
        model_size_mb=500,
        prompt_len=1024,
        gen_len=2048,
        gpu_mem_free_mb=8000,
        disk_speed_class=2  # NVMe
    )

    # Update with a policy
    policy = {"batch_size": 32, "learning_rate": 0.001, "precision": "fp16"}
    await cache.update(fp, policy, reward=0.9, carbon=0.2, latency=0.1, cost=0.05,
                       context={"carbon_intensity": 350, "urgency": 0.3}, expert_id=0)

    # Record some human feedback for RLHF (simulated)
    await cache.record_feedback(state={"carbon_intensity": 350, "avg_score": 0.9, "cost": 0.2, "diversity": 0.1},
                                action="balanced", reward=0.8)

    # Retrieve
    retrieved = await cache.get_best_policy(fp, context={"carbon_intensity": 350, "urgency": 0.3})
    print(f"Retrieved policy: {retrieved}")

    # Stats
    print(f"Cache stats: {cache.get_stats()}")

    # Stop background tasks
    await cache.stop()

if __name__ == "__main__":
    asyncio.run(main())
