# File: quantum_integration/multi_agent/quantum_multi_agent_rl.py

import asyncio
from typing import Dict, List, Tuple, Optional, Any, Callable
from dataclasses import dataclass, field
from enum import Enum
import numpy as np
import pennylane as qml
import random
from collections import deque

class AgentRole(Enum):
    SCHEDULER = "scheduler"
    OPTIMIZER = "optimizer"
    MONITOR = "monitor"
    LEARNER = "learner"

@dataclass
class QuantumState:
    """Shared quantum state for multi-agent system"""
    qubits: int
    entanglement_graph: List[Tuple[int, int]]
    state_vector: np.ndarray = None
    
    def __post_init__(self):
        if self.state_vector is None:
            self.state_vector = np.zeros(2 ** self.qubits)
            self.state_vector[0] = 1.0

@dataclass
class AgentObservation:
    """Observation for each agent"""
    local_state: np.ndarray
    shared_quantum_state: QuantumState
    carbon_intensity: float
    energy_budget: float
    task_queue: List[Dict]
    # Enhanced fields
    human_feedback_score: float = 0.5      # RLHF
    graph_centrality: float = 0.5          # LIMIT Graph
    graph_connectivity: float = 0.5
    latency_target: float = 100.0          # MODP objective
    cost_budget: float = 50.0

@dataclass
class MultiAgentConfig:
    """Configuration for multi-agent quantum RL"""
    n_agents: int = 4
    n_qubits_per_agent: int = 4
    entanglement_strategy: str = "full"    # full, ring, star
    communication_rounds: int = 3
    carbon_awareness_weight: float = 0.5
    # Enhanced options
    use_enhanced: bool = False
    # MODP weights: [carbon, energy, latency, cost]
    objective_weights: Optional[List[float]] = None
    # Evolutionary parameters
    population_size: int = 10
    mutation_rate: float = 0.1
    crossover_rate: float = 0.7
    elitism: int = 2
    # Distillation parameters
    distillation_epsilon: float = 0.1
    distillation_lr: float = 0.01
    gating_lr: float = 0.005

# ============================================================================
# Enhanced Distillation Components for Agent Action Selection
# ============================================================================

class AgentState:
    """State representation for distillation."""
    def __init__(self, observation: AgentObservation, agent_id: str, role: AgentRole):
        self.carbon = observation.carbon_intensity / 500.0
        self.energy = observation.energy_budget / 100.0
        self.latency = observation.latency_target / 1000.0
        self.cost = observation.cost_budget / 100.0
        self.graph_cent = observation.graph_centrality
        self.graph_conn = observation.graph_connectivity
        self.human_feedback = observation.human_feedback_score
        self.queue_len = len(observation.task_queue) / 10.0
        self.role_onehot = self._role_to_onehot(role)
        self.agent_id_num = int(agent_id.split('_')[-1]) / 10.0  # simple numeric

    def _role_to_onehot(self, role: AgentRole) -> List[float]:
        roles = [AgentRole.SCHEDULER, AgentRole.OPTIMIZER, AgentRole.MONITOR, AgentRole.LEARNER]
        return [1.0 if role == r else 0.0 for r in roles]

    def to_feature_vector(self) -> np.ndarray:
        return np.array([
            self.carbon,
            self.energy,
            self.latency,
            self.cost,
            self.graph_cent,
            self.graph_conn,
            self.human_feedback,
            self.queue_len,
            *self.role_onehot,
            self.agent_id_num
        ], dtype=np.float32)

class Teacher:
    """Base teacher for action selection."""
    def predict(self, state: AgentState) -> np.ndarray:
        raise NotImplementedError

    def confidence(self, state: AgentState) -> float:
        raise NotImplementedError

class RuleBasedTeacher(Teacher):
    """Rule-based expert for action selection."""
    ACTIONS = 3  # number of possible actions (e.g., 0=compute, 1=defer, 2=communicate)

    def predict(self, state: AgentState) -> np.ndarray:
        probs = np.ones(self.ACTIONS) * 0.1
        if state.carbon > 0.6:  # high carbon
            probs[1] = 0.8  # defer
        elif state.energy < 0.2:  # low energy budget
            probs[2] = 0.7  # communicate/request help
        elif state.latency < 0.1:  # urgent
            probs[0] = 0.6  # compute
        else:
            probs[0] = 0.5
        return probs / probs.sum()

    def confidence(self, state: AgentState) -> float:
        return 0.5

class RLHFTeacher(Teacher):
    """Teacher influenced by human feedback."""
    ACTIONS = 3

    def predict(self, state: AgentState) -> np.ndarray:
        probs = np.ones(self.ACTIONS) / self.ACTIONS
        if state.human_feedback > 0.7:
            probs[0] += 0.2  # prefer compute
        elif state.human_feedback < 0.3:
            probs[1] += 0.2  # prefer defer
        return probs / probs.sum()

    def confidence(self, state: AgentState) -> float:
        return 0.7 if abs(state.human_feedback - 0.5) > 0.3 else 0.4

class HistoricalTeacher(Teacher):
    """Teacher based on historical performance (simulated)."""
    ACTIONS = 3

    def __init__(self):
        self.model = None  # would load trained model

    def predict(self, state: AgentState) -> np.ndarray:
        if state.queue_len > 0.7:
            return np.array([0.1, 0.2, 0.7])
        elif state.carbon < 0.3:
            return np.array([0.7, 0.1, 0.2])
        else:
            return np.array([0.4, 0.3, 0.3])

    def confidence(self, state: AgentState) -> float:
        return 0.6

class QTeacher(Teacher):
    """Stateful Q-learning teacher."""
    ACTIONS = 3

    def __init__(self, feature_dim: int, lr: float = 0.1):
        self.weights = np.zeros((feature_dim, self.ACTIONS))
        self.lr = lr

    def predict(self, state: AgentState) -> np.ndarray:
        x = state.to_feature_vector()
        q = x @ self.weights
        exp_q = np.exp(q - np.max(q))
        return exp_q / exp_q.sum()

    def confidence(self, state: AgentState) -> float:
        return 0.5

    def update(self, state_vec, action, reward):
        x = state_vec
        q_current = np.dot(x, self.weights[:, action])
        self.weights[:, action] += self.lr * (reward - q_current) * x

class DistillationStudent:
    """Linear softmax student."""
    def __init__(self, feature_dim: int, n_actions: int = 3, lr: float = 0.01):
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
    """Gating network for MoE."""
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

class AgentDistillationOptimizer:
    """Distillation + MoE for agent action selection."""
    def __init__(self, feature_dim: int, config: MultiAgentConfig):
        self.feature_dim = feature_dim
        self.n_actions = 3
        self.student = DistillationStudent(feature_dim, self.n_actions, lr=config.distillation_lr)
        self.teachers = [
            RuleBasedTeacher(),
            RLHFTeacher(),
            HistoricalTeacher(),
            QTeacher(feature_dim)
        ]
        self.gating = MoEGatingNetwork(feature_dim, len(self.teachers), lr=config.gating_lr)
        self.epsilon = config.distillation_epsilon
        self.distill_w = 0.7
        self.rl_w = 0.3
        self.replay = deque(maxlen=2000)
        self.counter = 0
        self.train_every = 10

    def select_action(self, state: AgentState, exploration=True) -> Tuple[int, np.ndarray, np.ndarray]:
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
                # For gating update, we need original teacher outputs; we approximate using tp
                teacher_outputs = np.tile(tp, (len(self.teachers), 1))
                student_out = self.student.predict_proba(s)
                self.gating.update(s, teacher_outputs, student_out)

# ============================================================================
# Evolutionary Optimizer for Agent Parameters
# ============================================================================
class EvolutionaryAgentOptimizer:
    """Evolve agent parameters."""
    def __init__(self, param_shape, population_size=10, mutation_rate=0.1,
                 crossover_rate=0.7, elitism=2):
        self.param_shape = param_shape
        self.population = [np.random.normal(0, 0.1, param_shape) for _ in range(population_size)]
        self.fitness = np.zeros(population_size)
        self.mutation_rate = mutation_rate
        self.crossover_rate = crossover_rate
        self.elitism = elitism
        self.best_params = self.population[0]
        self.best_fitness = 0.0

    def get_best(self):
        return self.best_params

    def update_fitness(self, reward, index=0):
        self.fitness[index] = reward
        best_idx = int(np.argmax(self.fitness))
        self.best_params = self.population[best_idx]
        self.best_fitness = self.fitness[best_idx]
        # Evolve
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
            child += np.random.normal(0, self.mutation_rate, self.param_shape)
            new_pop.append(child)
        self.population = new_pop
        self.fitness = np.zeros(len(self.population))

# ============================================================================
# Enhanced QuantumAgent
# ============================================================================
class QuantumAgent:
    """Individual quantum agent with optional enhanced decision-making."""
    
    def __init__(self, agent_id: str, role: AgentRole, n_qubits: int,
                 use_enhanced: bool = False, config: MultiAgentConfig = None):
        self.agent_id = agent_id
        self.role = role
        self.n_qubits = n_qubits
        self.use_enhanced = use_enhanced
        self.config = config or MultiAgentConfig()
        self.params = self._initialize_params()
        if self.use_enhanced:
            feature_dim = self._calculate_feature_dim()
            self.distillation_optimizer = AgentDistillationOptimizer(feature_dim, self.config)
            self.evolutionary_optimizer = EvolutionaryAgentOptimizer(
                self.params.shape,
                population_size=self.config.population_size,
                mutation_rate=self.config.mutation_rate,
                crossover_rate=self.config.crossover_rate,
                elitism=self.config.elitism
            )
        else:
            self.distillation_optimizer = None
            self.evolutionary_optimizer = None
    
    def _initialize_params(self):
        return np.random.normal(0, 0.1, (3, self.n_qubits, 3))

    def _calculate_feature_dim(self) -> int:
        # carbon, energy, latency, cost, graph_cent, graph_conn, human_feedback, queue_len,
        # 4 role one-hot, agent_id_num = total 13
        return 13

    async def select_action(self, obs: AgentObservation, shared_state: QuantumState) -> int:
        """Select action using quantum policy or distillation."""
        if self.use_enhanced and self.distillation_optimizer:
            state = AgentState(obs, self.agent_id, self.role)
            action, _, _ = self.distillation_optimizer.select_action(state)
            return action
        else:
            # Original quantum policy
            encoded = self._encode_observation(obs)
            return self._quantum_policy(encoded)
    
    def _encode_observation(self, obs: AgentObservation) -> np.ndarray:
        features = np.array([
            obs.carbon_intensity / 1000,
            obs.energy_budget / 100,
            len(obs.task_queue) / 10
        ])
        # Append enhanced features if available
        if self.use_enhanced:
            features = np.append(features, [
                obs.human_feedback_score,
                obs.graph_centrality,
                obs.graph_connectivity,
                obs.latency_target / 1000,
                obs.cost_budget / 100
            ])
        return features
    
    def _quantum_policy(self, features: np.ndarray) -> int:
        @qml.qnode(qml.device('default.qubit', wires=self.n_qubits))
        def policy_circuit(x, params):
            for i in range(min(len(x), self.n_qubits)):
                qml.RY(x[i], wires=i)
            for layer in range(3):
                for i in range(self.n_qubits):
                    qml.Rot(params[layer, i, 0], params[layer, i, 1], 
                           params[layer, i, 2], wires=i)
                for i in range(self.n_qubits - 1):
                    qml.CNOT(wires=[i, i + 1])
            return qml.probs(wires=0)
        probs = policy_circuit(features, self.params)
        return int(np.argmax(probs))
    
    async def compute_quantum_gradient(self, obs: AgentObservation) -> np.ndarray:
        features = self._encode_observation(obs)
        @qml.qnode(qml.device('default.qubit', wires=self.n_qubits))
        def circuit(x, params):
            for i in range(min(len(x), self.n_qubits)):
                qml.RY(x[i], wires=i)
            for layer in range(3):
                for i in range(self.n_qubits):
                    qml.Rot(params[layer, i, 0], params[layer, i, 1], 
                           params[layer, i, 2], wires=i)
            return qml.expval(qml.PauliZ(0))
        gradient = qml.grad(circuit, argnum=1)(features, self.params)
        return gradient
    
    async def update_policy(self, gradient: np.ndarray, shared_state: QuantumState):
        learning_rate = 0.01
        self.params += learning_rate * gradient
        # Optional: evolutionary update
        if self.use_enhanced and self.evolutionary_optimizer:
            # Use a simple reward derived from gradient magnitude
            reward = np.linalg.norm(gradient)
            self.evolutionary_optimizer.update_fitness(reward, index=0)
            # Blend with evolved parameters occasionally
            if random.random() < 0.2:
                self.params = self.evolutionary_optimizer.get_best()

    async def update_distillation(self, state_vec, action, reward, next_state_vec, teacher_probs):
        if self.use_enhanced and self.distillation_optimizer:
            self.distillation_optimizer.update(state_vec, action, reward, next_state_vec, teacher_probs)

# ============================================================================
# Enhanced QuantumMultiAgentRL
# ============================================================================
class QuantumMultiAgentRL:
    """
    Multi-Agent Quantum Reinforcement Learning System
    Enhanced with LIMIT Graph, MODP, RLHF, Distillation, Evolutionary, MoE.
    """
    
    def __init__(self, config: MultiAgentConfig):
        self.config = config
        self.agents = {}
        self.shared_state = None
        self.use_enhanced = config.use_enhanced
        self.objective_weights = config.objective_weights
        if self.objective_weights is None:
            self.objective_weights = [0.4, 0.3, 0.2, 0.1]  # carbon, energy, latency, cost
        self._initialize_agents()
    
    def _initialize_agents(self):
        roles = [AgentRole.SCHEDULER, AgentRole.OPTIMIZER, 
                AgentRole.MONITOR, AgentRole.LEARNER]
        for i in range(self.config.n_agents):
            agent_id = f"agent_{i}"
            role = roles[i % len(roles)]
            self.agents[agent_id] = QuantumAgent(
                agent_id=agent_id,
                role=role,
                n_qubits=self.config.n_qubits_per_agent,
                use_enhanced=self.use_enhanced,
                config=self.config
            )
    
    def create_entangled_policy(self):
        total_qubits = self.config.n_agents * self.config.n_qubits_per_agent
        entanglement_graph = self._build_entanglement_graph()
        self.shared_state = QuantumState(
            qubits=total_qubits,
            entanglement_graph=entanglement_graph
        )
        @qml.qnode(qml.device('default.qubit', wires=total_qubits))
        def create_entanglement():
            for i in range(total_qubits):
                qml.Hadamard(wires=i)
            for (q1, q2) in entanglement_graph:
                qml.CNOT(wires=[q1, q2])
            return qml.state()
        self.shared_state.state_vector = create_entanglement()
        return self.shared_state
    
    def _build_entanglement_graph(self) -> List[Tuple[int, int]]:
        graph = []
        n_agents = self.config.n_agents
        qubits_per_agent = self.config.n_qubits_per_agent
        if self.config.entanglement_strategy == "full":
            for i in range(n_agents):
                for j in range(i + 1, n_agents):
                    graph.append((i * qubits_per_agent, j * qubits_per_agent))
        elif self.config.entanglement_strategy == "ring":
            for i in range(n_agents):
                j = (i + 1) % n_agents
                graph.append((i * qubits_per_agent, j * qubits_per_agent))
        return graph
    
    async def distributed_policy_update(self, observations: Dict[str, AgentObservation]):
        """Update policies across all agents using distributed quantum optimization."""
        # Phase 1: Local quantum policy evaluation
        local_gradients = {}
        for agent_id, agent in self.agents.items():
            obs = observations[agent_id]
            local_grad = await agent.compute_quantum_gradient(obs)
            local_gradients[agent_id] = local_grad
        
        # Phase 2: Quantum consensus via entangled measurement
        consensus_gradient = await self._quantum_consensus_gradient(local_gradients)
        
        # Phase 3: Update all agents with consensus
        update_tasks = []
        for agent_id, agent in self.agents.items():
            task = agent.update_policy(consensus_gradient[agent_id], self.shared_state)
            update_tasks.append(task)
        await asyncio.gather(*update_tasks)
        
        # Enhanced: distillation updates based on actions taken
        if self.use_enhanced:
            for agent_id, agent in self.agents.items():
                if hasattr(agent, 'last_state_vec') and hasattr(agent, 'last_action'):
                    # Compute reward using MODP weights
                    obs = observations[agent_id]
                    reward = self._calculate_agent_reward(obs, agent.last_action)
                    # Update distillation
                    await agent.update_distillation(
                        agent.last_state_vec,
                        agent.last_action,
                        reward,
                        agent.last_state_vec,  # next state approximated same
                        agent.last_teacher_probs
                    )
    
    async def _quantum_consensus_gradient(
        self,
        local_gradients: Dict[str, np.ndarray]
    ) -> Dict[str, np.ndarray]:
        n_agents = len(local_gradients)
        gradient_dim = len(list(local_gradients.values())[0])
        @qml.qnode(qml.device('default.qubit', wires=n_agents + gradient_dim))
        def quantum_gradient_averaging():
            for i in range(n_agents):
                qml.Hadamard(wires=i)
            for i, (agent_id, grad) in enumerate(local_gradients.items()):
                for j, g in enumerate(grad):
                    angle = np.arctan(g)
                    qml.RY(angle, wires=n_agents + j)
                    qml.CNOT(wires=[i, n_agents + j])
            for i in range(n_agents):
                qml.Hadamard(wires=i)
            return [qml.expval(qml.PauliZ(i)) for i in range(gradient_dim)]
        consensus = quantum_gradient_averaging()
        return {agent_id: np.array(consensus) for agent_id in self.agents}
    
    async def carbon_aware_coordination(self, global_carbon_data: Dict):
        """Coordinate agents based on carbon intensity and enhanced objectives."""
        if not self.use_enhanced:
            # Original logic
            agent_carbon_scores = {}
            for agent_id, agent in self.agents.items():
                location = getattr(agent, 'location', 'default')
                carbon_intensity = global_carbon_data.get(location, 500)
                agent_carbon_scores[agent_id] = carbon_intensity
            sorted_agents = sorted(agent_carbon_scores.items(), key=lambda x: x[1])
            task_distribution = {}
            for rank, (agent_id, carbon) in enumerate(sorted_agents):
                if rank < len(sorted_agents) * 0.3:
                    task_distribution[agent_id] = {'role': 'primary_compute', 'power_budget': 1.0, 'priority': 'high'}
                elif rank < len(sorted_agents) * 0.7:
                    task_distribution[agent_id] = {'role': 'balanced', 'power_budget': 0.6, 'priority': 'medium'}
                else:
                    task_distribution[agent_id] = {'role': 'deferred', 'power_budget': 0.2, 'priority': 'low'}
            return task_distribution
        else:
            # Enhanced: use distillation to decide roles
            task_distribution = {}
            for agent_id, agent in self.agents.items():
                # Build a simple observation with carbon data and current context
                obs = AgentObservation(
                    local_state=np.random.rand(4),
                    shared_quantum_state=self.shared_state,
                    carbon_intensity=global_carbon_data.get(agent_id, 400),
                    energy_budget=100.0,
                    task_queue=[],
                    human_feedback_score=0.5,
                    graph_centrality=0.5,
                    graph_connectivity=0.5,
                    latency_target=100.0,
                    cost_budget=50.0
                )
                state = AgentState(obs, agent_id, agent.role)
                action, state_vec, teacher_probs = agent.distillation_optimizer.select_action(state)
                # Store for later update
                agent.last_state_vec = state_vec
                agent.last_action = action
                agent.last_teacher_probs = teacher_probs
                # Map action to role
                if action == 0:
                    task_distribution[agent_id] = {'role': 'compute', 'power_budget': 1.0, 'priority': 'high'}
                elif action == 1:
                    task_distribution[agent_id] = {'role': 'deferred', 'power_budget': 0.2, 'priority': 'low'}
                else:
                    task_distribution[agent_id] = {'role': 'balanced', 'power_budget': 0.6, 'priority': 'medium'}
            return task_distribution
    
    def _calculate_agent_reward(self, obs: AgentObservation, action: int) -> float:
        """Calculate reward using MODP weights."""
        carbon_norm = 1.0 - min(obs.carbon_intensity / 500.0, 1.0)
        energy_norm = obs.energy_budget / 100.0
        latency_norm = 1.0 - min(obs.latency_target / 1000.0, 1.0)
        cost_norm = 1.0 - min(obs.cost_budget / 100.0, 1.0)
        # Penalize defer actions in low carbon, etc.
        if action == 0:  # compute
            pass
        elif action == 1:  # defer
            # Deferring may reduce carbon but also energy
            energy_norm *= 0.5
            latency_norm *= 0.5
        else:  # balanced
            carbon_norm *= 0.8
        values = np.array([carbon_norm, energy_norm, latency_norm, cost_norm])
        return float(np.dot(values, self.objective_weights))


def create_multi_agent_system(
    n_agents: int = 4,
    entanglement_strategy: str = "full",
    use_enhanced: bool = False,
    objective_weights: Optional[List[float]] = None,
    **kwargs
) -> QuantumMultiAgentRL:
    """Factory function to create multi-agent quantum RL system."""
    config = MultiAgentConfig(
        n_agents=n_agents,
        entanglement_strategy=entanglement_strategy,
        use_enhanced=use_enhanced,
        objective_weights=objective_weights,
        **kwargs
    )
    return QuantumMultiAgentRL(config)
