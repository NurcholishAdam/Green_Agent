# File: quantum_integration/orchestration/carbon_aware_scheduler.py

from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, field
from enum import Enum
import asyncio
import numpy as np
import random
from collections import deque

class CarbonZone(Enum):
    GREEN = "green"      # <50 gCO2/kWh
    YELLOW = "yellow"    # 50-200 gCO2/kWh
    RED = "red"          # >200 gCO2/kWh

@dataclass
class Task:
    task_id: str
    priority: int
    energy_requirement: float
    deferrable: bool
    deadline: Optional[float] = None
    # Enhanced fields
    latency_target_ms: float = 100.0
    cost_budget: float = 10.0
    human_feedback_score: float = 0.5      # RLHF
    graph_centrality: float = 0.5          # LIMIT Graph
    graph_connectivity: float = 0.5
    data_size_mb: float = 1.0

@dataclass
class Node:
    node_id: str
    carbon_intensity: float  # gCO2/kWh
    available_capacity: float
    power_budget: float
    # Enhanced fields
    latency_ms: float = 10.0
    cost_per_task: float = 1.0
    graph_centrality: float = 0.5
    graph_connectivity: float = 0.5

# ------------------------------------------------------------------------------
# Enhanced State and Distillation Components for Node Selection
# ------------------------------------------------------------------------------

class SchedulerState:
    """State representation for distillation to assign a task to a node."""
    def __init__(self, task: Task, node: Node, carbon_forecast_mean: float = 400.0):
        self.task_priority = task.priority / 10.0
        self.task_energy = min(task.energy_requirement / 10.0, 1.0)
        self.task_deferrable = 1.0 if task.deferrable else 0.0
        self.task_latency = min(task.latency_target_ms / 1000.0, 1.0)
        self.task_cost = min(task.cost_budget / 100.0, 1.0)
        self.task_graph_cent = task.graph_centrality
        self.task_graph_conn = task.graph_connectivity
        self.task_human_feedback = task.human_feedback_score

        self.node_carbon = min(node.carbon_intensity / 500.0, 1.0)
        self.node_capacity = min(node.available_capacity, 1.0)
        self.node_power = min(node.power_budget, 1.0)
        self.node_latency = min(node.latency_ms / 100.0, 1.0)
        self.node_cost = min(node.cost_per_task / 10.0, 1.0)
        self.node_graph_cent = node.graph_centrality
        self.node_graph_conn = node.graph_connectivity

        self.forecast_mean = min(carbon_forecast_mean / 500.0, 1.0)

    def to_feature_vector(self) -> np.ndarray:
        return np.array([
            self.task_priority,
            self.task_energy,
            self.task_deferrable,
            self.task_latency,
            self.task_cost,
            self.task_graph_cent,
            self.task_graph_conn,
            self.task_human_feedback,
            self.node_carbon,
            self.node_capacity,
            self.node_power,
            self.node_latency,
            self.node_cost,
            self.node_graph_cent,
            self.node_graph_conn,
            self.forecast_mean,
        ], dtype=np.float32)

class Teacher:
    """Base teacher for assignment action (0=assign, 1=defer)."""
    def predict(self, state: SchedulerState) -> np.ndarray:
        raise NotImplementedError

    def confidence(self, state: SchedulerState) -> float:
        raise NotImplementedError

class RuleBasedTeacher(Teacher):
    def predict(self, state: SchedulerState) -> np.ndarray:
        # Simple rule: assign if node carbon low or task urgent
        probs = np.ones(2) * 0.1
        if state.node_carbon < 0.1 or state.task_priority > 0.8:
            probs[0] = 0.9
        elif state.task_deferrable > 0.5 and state.forecast_mean > 0.5:
            probs[1] = 0.8
        else:
            probs[0] = 0.5
        return probs / probs.sum()

    def confidence(self, state: SchedulerState) -> float:
        return 0.5

class RLHFTeacher(Teacher):
    def predict(self, state: SchedulerState) -> np.ndarray:
        probs = np.ones(2) / 2
        if state.task_human_feedback > 0.7:
            probs[0] += 0.2  # prefer assign
        elif state.task_human_feedback < 0.3:
            probs[1] += 0.2  # prefer defer
        return probs / probs.sum()

    def confidence(self, state: SchedulerState) -> float:
        return 0.7 if abs(state.task_human_feedback - 0.5) > 0.3 else 0.4

class HistoricalTeacher(Teacher):
    def __init__(self):
        self.model = None  # placeholder

    def predict(self, state: SchedulerState) -> np.ndarray:
        # Simulate model: prefer assign if node capacity high
        if state.node_capacity > 0.7 and state.node_carbon < 0.3:
            return np.array([0.8, 0.2])
        else:
            return np.array([0.3, 0.7])

    def confidence(self, state: SchedulerState) -> float:
        return 0.6

class QTeacher(Teacher):
    def __init__(self, feature_dim: int, lr: float = 0.1):
        self.weights = np.zeros((feature_dim, 2))
        self.lr = lr

    def predict(self, state: SchedulerState) -> np.ndarray:
        x = state.to_feature_vector()
        q = x @ self.weights
        exp_q = np.exp(q - np.max(q))
        return exp_q / exp_q.sum()

    def confidence(self, state: SchedulerState) -> float:
        return 0.5

    def update(self, state_vec, action, reward):
        x = state_vec
        q_current = np.dot(x, self.weights[:, action])
        self.weights[:, action] += self.lr * (reward - q_current) * x

class DistillationStudent:
    def __init__(self, feature_dim: int, n_actions: int = 2, lr: float = 0.01):
        self.feature_dim = feature_dim
        self.n_actions = n_actions
        self.weights = np.zeros((feature_dim, n_actions))
        self.bias = np.zeros(n_actions)
        self.lr = lr
        self.counter = 0

    def predict_proba(self, state_vec):
        logits = state_vec @ self.weights + self.bias
        exp = np.exp(logits - np.max(logits))
        return exp / exp.sum()

    def update(self, state_vec, teacher_probs, reward, action, distill_w, rl_w):
        cur = self.predict_proba(state_vec)
        grad_distill = -(teacher_probs - cur)
        one_hot = np.zeros(self.n_actions)
        one_hot[action] = 1.0
        grad_rl = -reward * (one_hot - cur)
        grad = distill_w * grad_distill + rl_w * grad_rl
        self.weights -= self.lr * np.outer(state_vec, grad)
        self.bias -= self.lr * grad
        self.counter += 1

class MoEGatingNetwork:
    def __init__(self, feature_dim: int, n_experts: int, lr: float = 0.005):
        self.feature_dim = feature_dim
        self.n_experts = n_experts
        self.lr = lr
        self.weights = np.random.randn(feature_dim, n_experts) * 0.01
        self.bias = np.zeros(n_experts)

    def forward(self, state_vec):
        logits = state_vec @ self.weights + self.bias
        exp = np.exp(logits - np.max(logits))
        return exp / exp.sum()

    def update(self, state_vec, teacher_outputs, student_probs):
        gate_probs = self.forward(state_vec)
        combined = np.sum(gate_probs[:, None] * teacher_outputs, axis=0)
        error = combined - student_probs
        grad = np.dot(teacher_outputs, error)
        self.weights -= self.lr * np.outer(state_vec, grad)
        self.bias -= self.lr * grad

class SchedulerDistillationOptimizer:
    """Distillation + MoE for scheduling decisions."""
    def __init__(self, feature_dim: int = 16, config: Dict[str, Any] = None):
        self.config = config or {}
        self.feature_dim = feature_dim
        self.n_actions = 2  # assign or defer
        self.student = DistillationStudent(feature_dim, self.n_actions,
                                           lr=self.config.get('distillation_lr', 0.01))
        self.teachers = [
            RuleBasedTeacher(),
            RLHFTeacher(),
            HistoricalTeacher(),
            QTeacher(feature_dim, lr=self.config.get('q_lr', 0.1))
        ]
        self.gating = MoEGatingNetwork(feature_dim, len(self.teachers),
                                       lr=self.config.get('gating_lr', 0.005))
        self.epsilon = self.config.get('epsilon', 0.1)
        self.distill_w = self.config.get('distill_weight', 0.7)
        self.rl_w = self.config.get('rl_weight', 0.3)
        self.replay = deque(maxlen=self.config.get('replay_size', 2000))
        self.counter = 0
        self.train_every = self.config.get('train_every', 10)

    def select_action(self, state: SchedulerState, exploration=True) -> Tuple[int, np.ndarray, np.ndarray]:
        x = state.to_feature_vector()
        teacher_outputs = []
        for teacher in self.teachers:
            prob = teacher.predict(state)
            if len(prob) != self.n_actions:
                prob = np.pad(prob, (0, max(0, self.n_actions - len(prob))), 'constant')[:self.n_actions]
            teacher_outputs.append(prob)
        teacher_outputs = np.array(teacher_outputs)
        gate_weights = self.gating.forward(x)
        teacher_probs = np.sum(gate_weights[:, None] * teacher_outputs, axis=0)
        teacher_probs /= teacher_probs.sum()

        student_probs = self.student.predict_proba(x)

        if exploration and random.random() < self.epsilon:
            action = random.randint(0, self.n_actions-1)
        else:
            combined = 0.8 * student_probs + 0.2 * teacher_probs
            action = int(np.argmax(combined))

        return action, x, teacher_probs

    def update(self, state_vec, action, reward, next_state_vec, teacher_probs):
        self.replay.append((state_vec, action, reward, next_state_vec, teacher_probs))
        self.counter += 1
        if self.counter % self.train_every == 0 and len(self.replay) >= 8:
            batch = random.sample(self.replay, min(8, len(self.replay)))
            for s, a, r, ns, tp in batch:
                self.student.update(s, tp, r, a, self.distill_w, self.rl_w)
                teacher_outputs = np.tile(tp, (len(self.teachers), 1))
                student_out = self.student.predict_proba(s)
                self.gating.update(s, teacher_outputs, student_out)

# ------------------------------------------------------------------------------
# Evolutionary Optimizer for MODP Weights
# ------------------------------------------------------------------------------
class EvolutionaryWeights:
    """Evolve MODP weights."""
    def __init__(self, n_objectives=4, population_size=10, mutation_rate=0.1, crossover_rate=0.7, elitism=2):
        self.n_objectives = n_objectives
        self.population = [np.random.dirichlet(np.ones(n_objectives)) for _ in range(population_size)]
        self.fitness = np.zeros(population_size)
        self.mutation_rate = mutation_rate
        self.crossover_rate = crossover_rate
        self.elitism = elitism
        self.best_weights = self.population[0]
        self.best_fitness = 0.0

    def get_weights(self):
        return self.best_weights

    def update_fitness(self, reward, index=0):
        self.fitness[index] = reward
        best_idx = int(np.argmax(self.fitness))
        self.best_weights = self.population[best_idx]
        self.best_fitness = self.fitness[best_idx]
        sorted_indices = np.argsort(self.fitness)[::-1]
        new_pop = [self.population[i] for i in sorted_indices[:self.elitism]]
        while len(new_pop) < len(self.population):
            p1 = self.population[random.randint(0, len(self.population)-1)]
            p2 = self.population[random.randint(0, len(self.population)-1)]
            if random.random() < self.crossover_rate:
                alpha = random.random()
                child = alpha * p1 + (1 - alpha) * p2
            else:
                child = p1.copy()
            child += np.random.dirichlet(np.ones(self.n_objectives)) * self.mutation_rate
            child = child / child.sum()
            new_pop.append(child)
        self.population = new_pop
        self.fitness = np.zeros(len(self.population))

# ------------------------------------------------------------------------------
# Enhanced CarbonAwareScheduler
# ------------------------------------------------------------------------------
class CarbonAwareScheduler:
    """
    Carbon-Aware Task Scheduler for Green_Agent
    Enhanced with LIMIT Graph, MODP, RLHF, Multi‑Teacher Distillation, Evolutionary, and MoE.
    """

    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or {}
        self.use_enhanced = self.config.get('use_enhanced', False)
        self.nodes: Dict[str, Node] = {}
        self.task_queue: List[Task] = []
        self.carbon_thresholds = {
            'green': 50,
            'yellow': 200
        }
        # MODP weights default: [carbon, energy, latency, cost]
        self.objective_weights = self.config.get('objective_weights', np.array([0.4, 0.3, 0.2, 0.1]))
        if not isinstance(self.objective_weights, np.ndarray):
            self.objective_weights = np.array(self.objective_weights)
        self.objective_weights = self.objective_weights / self.objective_weights.sum()

        if self.use_enhanced:
            self.distillation_optimizer = SchedulerDistillationOptimizer(config=self.config)
            self.evolutionary_weights = EvolutionaryWeights(
                n_objectives=4,
                population_size=self.config.get('population_size', 10),
                mutation_rate=self.config.get('mutation_rate', 0.1),
                crossover_rate=self.config.get('crossover_rate', 0.7),
                elitism=self.config.get('elitism', 2)
            )
            # Possibly adjust objective_weights from evolutionary
            if self.config.get('use_evolutionary_weights', False):
                self.objective_weights = self.evolutionary_weights.get_weights()
        else:
            self.distillation_optimizer = None
            self.evolutionary_weights = None

    def add_node(self, node: Node):
        self.nodes[node.node_id] = node

    def add_task(self, task: Task):
        self.task_queue.append(task)

    def get_carbon_zone(self, carbon_intensity: float) -> CarbonZone:
        if carbon_intensity < self.carbon_thresholds['green']:
            return CarbonZone.GREEN
        elif carbon_intensity < self.carbon_thresholds['yellow']:
            return CarbonZone.YELLOW
        else:
            return CarbonZone.RED

    def _calculate_reward(self, task: Task, node: Node, action: int, carbon_forecast_mean: float) -> float:
        """
        Calculate reward for scheduling decision using MODP weights.
        action: 0=assign, 1=defer
        """
        if action == 1:  # defer
            # Deferral may be beneficial if future carbon forecast is lower
            carbon_norm = 1.0 - min(carbon_forecast_mean / 500.0, 1.0)
            energy_norm = 0.0  # no energy used yet
            latency_norm = 0.0  # delayed
            cost_norm = 0.0
        else:
            # Assign: compute normalized metrics for this node
            carbon_norm = 1.0 - min(node.carbon_intensity / 500.0, 1.0)
            energy_norm = 1.0 - min(task.energy_requirement / 10.0, 1.0)  # lower energy better
            latency_norm = 1.0 - min(node.latency_ms / 1000.0, 1.0)
            cost_norm = 1.0 - min(node.cost_per_task / 10.0, 1.0)
        values = np.array([carbon_norm, energy_norm, latency_norm, cost_norm])
        return float(np.dot(values, self.objective_weights))

    async def schedule_tasks(self, carbon_forecast: Optional[List[float]] = None) -> Dict[str, List[Task]]:
        """Schedule tasks with optional enhanced decision-making."""
        schedule: Dict[str, List[Task]] = {node_id: [] for node_id in self.nodes}
        sorted_tasks = sorted(self.task_queue, key=lambda t: t.priority, reverse=True)
        sorted_nodes = sorted(self.nodes.values(), key=lambda n: n.carbon_intensity)

        if carbon_forecast is None:
            carbon_forecast_mean = np.mean([n.carbon_intensity for n in self.nodes])
        else:
            carbon_forecast_mean = np.mean(carbon_forecast)

        for task in sorted_tasks:
            assigned = False
            # Enhanced decision: use distillation to choose node and whether to assign
            if self.use_enhanced:
                # For each node, decide if assign or defer using distillation
                best_node = None
                best_action = 1  # default defer
                best_score = -1
                best_info = None
                for node in sorted_nodes:
                    state = SchedulerState(task, node, carbon_forecast_mean)
                    action, state_vec, teacher_probs = self.distillation_optimizer.select_action(state)
                    # Compute a simple score to choose among nodes if action=assign
                    score = node.available_capacity * (1 - node.carbon_intensity/500) * (task.priority/10)
                    if action == 0 and score > best_score:
                        best_score = score
                        best_node = node
                        best_action = 0
                        best_info = (state_vec, teacher_probs, action)
                    elif action == 0 and best_node is None:
                        best_node = node
                        best_action = 0
                        best_info = (state_vec, teacher_probs, action)
                if best_action == 0 and best_node is not None:
                    if best_node.available_capacity >= task.energy_requirement:
                        schedule[best_node.node_id].append(task)
                        best_node.available_capacity -= task.energy_requirement
                        assigned = True
                        # Update distillation after assignment
                        if best_info:
                            state_vec, teacher_probs, action = best_info
                            reward = self._calculate_reward(task, best_node, action, carbon_forecast_mean)
                            next_state_vec = state_vec  # simplified
                            self.distillation_optimizer.update(state_vec, action, reward, next_state_vec, teacher_probs)
                            # Update evolutionary weights occasionally
                            if self.evolutionary_weights and random.random() < 0.1:
                                self.evolutionary_weights.update_fitness(reward, index=0)
                                self.objective_weights = self.evolutionary_weights.get_weights()
                # If not assigned, defer automatically (no update)
            else:
                # Original rule-based logic
                for node in sorted_nodes:
                    zone = self.get_carbon_zone(node.carbon_intensity)
                    if zone == CarbonZone.GREEN:
                        if node.available_capacity >= task.energy_requirement:
                            schedule[node.node_id].append(task)
                            node.available_capacity -= task.energy_requirement
                            assigned = True
                            break
                    elif zone == CarbonZone.YELLOW:
                        if not task.deferrable and node.available_capacity >= task.energy_requirement:
                            schedule[node.node_id].append(task)
                            node.available_capacity -= task.energy_requirement
                            assigned = True
                            break
                    else:  # RED
                        if (not task.deferrable and task.priority > 8 and
                            node.available_capacity >= task.energy_requirement):
                            schedule[node.node_id].append(task)
                            node.available_capacity -= task.energy_requirement
                            assigned = True
                            break
            # If not assigned and deferrable, keep in queue for later
            if not assigned and task.deferrable:
                # In a real system, wait for green window; here we just skip
                pass

        self.task_queue = []  # Clear queue
        return schedule

    def calculate_carbon_savings(self, schedule: Dict[str, List[Task]]) -> Dict:
        total_carbon = 0
        baseline_carbon = 0
        for node_id, tasks in schedule.items():
            node = self.nodes[node_id]
            for task in tasks:
                total_carbon += task.energy_requirement * node.carbon_intensity / 1000
                baseline_carbon += task.energy_requirement * 400 / 1000
        savings = baseline_carbon - total_carbon
        savings_percent = (savings / baseline_carbon * 100) if baseline_carbon > 0 else 0
        return {
            'total_carbon_kg': total_carbon,
            'baseline_carbon_kg': baseline_carbon,
            'carbon_saved_kg': savings,
            'carbon_saved_percent': savings_percent
        }

    async def get_optimal_execution_window(self, task: Task, carbon_forecast: List[float]) -> Dict:
        if not task.deferrable:
            return {'immediate': True, 'carbon_cost': None}
        best_start_time = 0
        best_carbon_cost = float('inf')
        for start_time in range(len(carbon_forecast)):
            carbon_cost = 0
            for t in range(start_time, min(start_time + int(task.energy_requirement), len(carbon_forecast))):
                carbon_cost += carbon_forecast[t]
            if carbon_cost < best_carbon_cost:
                best_carbon_cost = carbon_cost
                best_start_time = start_time
        return {
            'immediate': False,
            'optimal_start_time': best_start_time,
            'expected_carbon_cost': best_carbon_cost
        }


def create_carbon_aware_scheduler(use_enhanced: bool = False,
                                  config: Optional[Dict[str, Any]] = None) -> CarbonAwareScheduler:
    """Factory function to create carbon-aware scheduler."""
    if config is None:
        config = {}
    config['use_enhanced'] = use_enhanced
    return CarbonAwareScheduler(config)
