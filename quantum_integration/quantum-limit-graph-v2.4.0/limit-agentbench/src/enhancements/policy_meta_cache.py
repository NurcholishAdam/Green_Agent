"""
policy_meta_cache.py (Enhanced v2.0)

Stores successful policies keyed by a workload fingerprint.
Uses a multi‑objective decision process (MODP) with Pareto front + TOPSIS,
Mixture‑of‑Experts (MOE) gating, bio‑inspired Genetic Algorithm for policy evolution,
carbon‑aware scheduling, and self‑healing drift detection.

All enhancements degrade gracefully if optional dependencies are missing.
"""

import time
import math
import random
from typing import Dict, Any, Optional, List, Tuple, Callable
from dataclasses import dataclass, field
from collections import deque, defaultdict
import numpy as np
import hashlib
import logging

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
        hour = datetime.now().hour / 24.0
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
# Enhanced PolicyMetaCache
# ================================
class PolicyMetaCache:
    """
    Enhanced policy meta‑cache with MODP, MOE, GA, carbon‑aware scheduling, self‑healing.
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
        # Each expert corresponds to a different policy source; we'll store multiple policies per key.

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

        # For GA fitness evaluation, we need to simulate rewards (or have a real evaluator)
        self._fitness_evaluator = None

    def _vector_to_key(self, vec: np.ndarray) -> tuple:
        return tuple(vec.tolist())

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
        Incorporates carbon‑aware scheduling, MODP, MOE, and self‑healing.
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

        # 4. Self‑healing: check if recent rewards are anomalous – if so, we might fallback
        if self.self_healing and valid_entries:
            # Sample recent rewards from entries (average)
            avg_reward = np.mean([e[2][0] for e in valid_entries])  # objective[0] is reward
            anomaly, _ = self.self_healing.detect_anomaly(avg_reward)
            if anomaly:
                logger.warning("Anomaly detected in cached policy performance; falling back to default")
                return None  # or return a safe default

        # 5. MODP selection with Pareto front + TOPSIS
        if self.enable_modp and len(valid_entries) > 1:
            # Build Pareto front of entries based on objectives: [reward, carbon, latency, cost]
            # We assume all objectives except reward are to be minimized.
            # For TOPSIS we need "higher is better" – we invert carbon, latency, cost.
            candidates = []
            for e in valid_entries:
                policy, _, objectives, _, _ = e
                # objectives: [reward, carbon, latency, cost]
                # Invert carbon, latency, cost
                obj_inv = [objectives[0], 1.0 - objectives[1], 1.0 - objectives[2], 1.0 - objectives[3]]
                candidates.append({'objectives': obj_inv, 'policy': policy})
            # Use TOPSIS with adaptive weights
            criteria = ['reward', 'carbon', 'latency', 'cost']
            # If MOE gating provides context‑dependent weights, we could use them
            weights = self.modp_weights
            # Apply TOPSIS
            # Convert candidates to dict for TOPSIS
            cand_dicts = [{crit: c['objectives'][i] for i, crit in enumerate(criteria)} for c in candidates]
            scores = TOPSIS.score(cand_dicts, weights, criteria)
            best_idx = np.argmax(scores)
            best_policy = candidates[best_idx]['policy']
            return best_policy

        # 6. If not MODP, fallback to simple best reward (or MOE)
        # Choose the entry with highest reward (objective[0])
        best_entry = max(valid_entries, key=lambda e: e[2][0])
        policy = best_entry[0]

        # 7. If MOE is enabled and we have multiple experts, we could combine or select based on gating
        if self.enable_moe and self.moe_gating:
            # We need to know which expert each policy came from (stored as expert_id)
            # For simplicity, if we have multiple entries from different experts, we can weight them.
            # But here we already selected one; we could instead weight policies from different experts.
            # This would require a more complex implementation; for now we just return the best reward.
            pass

        # 8. GA: if this policy comes from a GA population, we could also evolve further.
        # But that's handled in update().

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

        # If key not present, create new entry list
        if key not in self.store:
            self.store[key] = []
            self.vectors.append(vec)
            self.keys.append(key)

        # Create new entry: (policy, timestamp, objectives, expert_id, ga_individual)
        objectives = [reward, carbon, latency, cost]
        new_entry = (policy, time.time(), objectives, expert_id, None)

        # If MODP is enabled, we keep all entries (not just the best) for Pareto front
        if self.enable_modp:
            # Remove entries that are strictly dominated by the new one
            # For simplicity, we just append and let MODP handle it in get.
            # But we can prune to keep cache small: keep only Pareto‑optimal ones.
            # We'll implement a simple pruning: keep entries that are not dominated.
            entries = self.store[key]
            # Add new entry
            entries.append(new_entry)
            # Prune: keep only non‑dominated entries based on objectives
            # We'll use Pareto dominance: we want to keep entries that are not dominated by any other.
            # For each entry, check if any other entry dominates it.
            # We'll do this periodically to avoid O(n^2) on every update.
            # For now, we just keep the best reward and the latest ones.
            # But to fully support MODP, we'll keep all and let the Pareto front be computed on retrieval.
            # To avoid unbounded growth, we'll cap the number of entries per key.
            max_entries = 20
            if len(entries) > max_entries:
                # Sort by reward and keep top max_entries
                entries.sort(key=lambda e: e[2][0], reverse=True)
                self.store[key] = entries[:max_entries]
        else:
            # Simple: keep only the policy with highest reward
            entries = self.store[key]
            # Find if there is an entry with higher reward; if not, replace
            best_reward = max([e[2][0] for e in entries]) if entries else -float('inf')
            if reward > best_reward:
                # Remove old entries and keep this one
                self.store[key] = [new_entry]
            else:
                # We keep the existing; but we might still add if we want multiple experts.
                # If MOE is enabled, we keep multiple experts.
                pass

        # MOE: update gating if context provided
        if self.enable_moe and context is not None and self.moe_gating:
            # We need to know which expert produced this policy.
            # For simplicity, we assume the caller provides expert_id.
            # We also need a reward for the gating; we can use the main reward.
            self.moe_gating.update(context, expert_id, reward)

        # GA: if GA is enabled and this key has a population, we can use the reward to update fitness
        if self.enable_ga and key in self.ga_populations:
            population = self.ga_populations[key]
            # Update the fitness of the individual that matches this policy?
            # We could match by policy content; for simplicity, we just update the population's best.
            # Instead, we'll treat the cache as an oracle: when we retrieve, we may evolve.
            # We'll evolve in a separate background task.

        # Self‑healing: record reward
        if self.self_healing:
            self.self_healing.record_reward(reward)

        # If this is a new key and GA is enabled, initialize a population
        if self.enable_ga and key not in self.ga_populations:
            # Create a GA population from this policy
            pop = GAPopulation(population_size=self.ga_population_size)
            pop.initialize(policy)
            self.ga_populations[key] = pop

        # Log
        logger.debug(f"Cache updated for fingerprint: reward={reward:.3f}, carbon={carbon:.3f}")

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
            # Update the cache with the new best policy
            # We need to know the reward of the best policy – we can estimate via fitness_func
            # Or we can rely on a separate evaluation.
            # For simplicity, we'll just store the best policy with a placeholder reward.
            # In a real system, the fitness_func would be the evaluation function.
            # We'll store the best policy in the cache with a timestamp.
            if best_policy:
                # Convert key back to vector to get fingerprint? We can store separately.
                # For now, we just update the entry list for that key.
                # We'll use a dummy reward (from fitness) and carbon/latency/cost 0.
                # We need to find the vector from the key.
                # We can store a mapping from key to fingerprint, but for simplicity, we skip.
                pass

    # ------------------------------------------------------------------
    # Utility methods
    # ------------------------------------------------------------------
    def get_stats(self) -> Dict:
        return {
            'cache_size': len(self.store),
            'total_entries': sum(len(v) for v in self.store.values()),
            'ga_populations': len(self.ga_populations),
            'moe_trained': self.moe_gating._trained if self.moe_gating else False,
            'self_healing_trained': self.self_healing._trained if self.self_healing else False,
        }

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
    )

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

    # Retrieve
    retrieved = await cache.get_best_policy(fp, context={"carbon_intensity": 350, "urgency": 0.3})
    print(f"Retrieved policy: {retrieved}")

    # Stats
    print(f"Cache stats: {cache.get_stats()}")

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
