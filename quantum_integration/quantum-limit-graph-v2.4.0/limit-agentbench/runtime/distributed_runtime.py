# runtime/distributed_runtime.py (Enhanced v2)
# Adds optional integration with LIMIT Graph, MODP, RLHF,
# Multi-Teacher On-Policy Distillation, Bio-inspired Optimisation, and MoE gating.
#
# ---- Fixes vs. v1 ----------------------------------------------------------
# * np imported; logger defined before the try/except (v1 crashed on import).
# * Ray ObjectRef awaited correctly via asyncio.wrap_future.
# * Distillation optimizer replaced with a real multi-teacher + MoE gate
#   (Adam, replay, persistence) instead of a fixed-step logistic placeholder.
# * MODP composite score re-derived: averages, target-normalized, no always-
#   zero terms, no hardcoded latency.
# * Config deep-copied and validated at construction.
# * Per-episode descriptor refresh (RLHF, carbon, graph metrics).
# * Clean shutdown: close(), atexit, try/finally so finalize always runs.
# * Same return shape from finalize() with or without enhancements.

from __future__ import annotations

import asyncio
import atexit
import copy
import logging
import math
import os
import random
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple

import numpy as np
import ray

from analytics.pareto_analyzer import ParetoAnalyzer
from sustainability.carbon_intensity_provider import CarbonIntensityProvider
from sustainability.eco_mode_controller import EcoModeController
from metrics.quantum_efficiency import QuantumEfficiencyMetric
from rewards.negawatt_reward import NegawattReward
from rl.q_learning import QLearningAgent
from rl.ppo_trainer import PPOTrainer
from policy.policy_engine import PolicyEngine

# ---- Logging must come before the try/except, not after ----
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger(__name__)

# ---- Optional enhancements ----
try:
    from src.enhancements.schemas.node_descriptor import (
        NodeDescriptor, NodeType, CoolingType,
    )
    from src.enhancements.schemas.workload_descriptor import (
        WorkloadDescriptor, TaskType, Urgency,
    )
    from src.enhancements.schemas.feedback_event import FeedbackEvent  # noqa: F401
    from src.enhancements.zero_trust_architecture import (
        ZeroTrustArchitecture, ZeroTrustConfig,  # noqa: F401
    )
    ENHANCEMENTS_AVAILABLE = True
except ImportError as exc:
    ENHANCEMENTS_AVAILABLE = False
    logger.warning("Enhanced modules not available; running legacy mode: %s", exc)


# =============================================================================
# Config — validated, deep-copied
# =============================================================================
@dataclass
class RuntimeConfig:
    energy_budget: float = 10_000.0
    baseline_energy: float = 120.0
    region: str = "ID"

    use_enhancements: bool = False
    modp_weights: List[float] = field(
        default_factory=lambda: [0.4, 0.3, 0.2, 0.1]  # accuracy, energy, carbon, latency
    )
    # Target-normalization constants for the MODP score
    accuracy_target: float = 0.95
    energy_target: float = 100.0
    carbon_target: float = 5.0
    latency_target_ms: float = 500.0

    human_feedback_score: float = 0.5
    graph_metrics: Dict[str, float] = field(
        default_factory=lambda: {"centrality": 0.5, "connectivity": 0.5, "density": 0.5}
    )

    output_dir: str = "."
    run_id: str = field(default_factory=lambda: uuid.uuid4().hex[:8])

    # Distillation
    distillation_lr: float = 0.01
    distillation_batch: int = 8
    distillation_train_every: int = 5
    replay_size: int = 2_000
    epsilon_start: float = 0.10
    epsilon_end: float = 0.01
    epsilon_decay_steps: int = 1_000

    # Optional real-energy sampler (fallback to proxy)
    use_real_energy: bool = False

    def __post_init__(self) -> None:
        if self.modp_weights is None or len(self.modp_weights) != 4:
            raise ValueError("modp_weights must have exactly 4 entries")
        total = float(sum(self.modp_weights))
        if total <= 0:
            raise ValueError("modp_weights must sum to a positive value")
        self.modp_weights = [w / total for w in self.modp_weights]
        if not (0.0 <= self.human_feedback_score <= 1.0):
            raise ValueError("human_feedback_score must be in [0, 1]")
        for k in ("centrality", "connectivity", "density"):
            self.graph_metrics.setdefault(k, 0.5)
            self.graph_metrics[k] = float(np.clip(self.graph_metrics[k], 0.0, 1.0))
        if self.energy_target <= 0 or self.carbon_target <= 0:
            raise ValueError("targets must be positive")
        os.makedirs(self.output_dir, exist_ok=True)


# =============================================================================
# Distillation + MoE (real implementation)
# =============================================================================
class _Adam:
    def __init__(self, shape, lr, betas=(0.9, 0.999), eps=1e-8):
        self.lr = float(lr)
        self.b1, self.b2 = betas
        self.eps = eps
        self.m = np.zeros(shape, dtype=np.float64)
        self.v = np.zeros(shape, dtype=np.float64)
        self.t = 0

    def step(self, param, grad):
        self.t += 1
        self.m = self.b1 * self.m + (1 - self.b1) * grad
        self.v = self.b2 * self.v + (1 - self.b2) * grad * grad
        m_hat = self.m / (1 - self.b1 ** self.t)
        v_hat = self.v / (1 - self.b2 ** self.t)
        return param - self.lr * m_hat / (np.sqrt(v_hat) + self.eps)


class DistillationOptimizer:
    """
    Multi-teacher distillation with MoE gating.
    Actions: 0..n_actions-1 (task tier / offload decision).

    Loss = KL(combined_teacher || student) with reward-weighted RL term.
    Teachers:
      0. Rule-based  (urgency/importance heuristics)
      1. RLHF        (human_feedback_score)
      2. Historical  (energy-efficient pick)
    """

    STATE_DIM = 10

    def __init__(self, n_actions: int = 5, cfg: RuntimeConfig = None,
                 persistence_path: Optional[str] = None):
        self.n_actions = int(n_actions)
        self.state_dim = self.STATE_DIM
        self.cfg = cfg or RuntimeConfig()
        self.persistence_path = persistence_path

        self.student_w = np.zeros((self.state_dim, self.n_actions))
        self.student_b = np.zeros(self.n_actions)
        self._adam_sw = _Adam(self.student_w.shape, self.cfg.distillation_lr)
        self._adam_sb = _Adam(self.student_b.shape, self.cfg.distillation_lr)

        rng = np.random.default_rng(0)
        self.gate_w = rng.normal(0, 0.01, (self.state_dim, 3))
        self.gate_b = np.zeros(3)
        self._adam_gw = _Adam(self.gate_w.shape, self.cfg.distillation_lr)
        self._adam_gb = _Adam(self.gate_b.shape, self.cfg.distillation_lr)

        self.replay = __import__("collections").deque(maxlen=self.cfg.replay_size)
        self.counter = 0

        if persistence_path and os.path.exists(persistence_path):
            self._load()

    # -- teachers -------------------------------------------------------------
    def _teachers(self, s: np.ndarray) -> np.ndarray:
        # s[0]=intensity, s[1]=energy, s[2]=accuracy, s[3]=episode,
        # s[4]=centrality, s[5]=connectivity, s[6]=human_feedback
        intensity, energy, accuracy = s[0], s[1], s[2]
        urgency = 1.0 - intensity  # lower carbon intensity -> less urgent
        hf = s[6]

        # Teacher 0: rule-based — prefer high-urgency when accuracy is high
        t0 = np.zeros(self.n_actions)
        if accuracy > 0.9:
            t0[0] = 0.8; t0[1] = 0.2
        elif urgency > 0.6:
            t0[1] = 0.5; t0[2] = 0.5
        else:
            t0[min(4, self.n_actions - 1)] = 1.0

        # Teacher 1: RLHF — follow human preference
        t1 = np.zeros(self.n_actions)
        t1[min(4, int(np.floor(hf * (self.n_actions - 1))))] = 1.0

        # Teacher 2: historical — energy-efficient pick
        t2 = np.zeros(self.n_actions)
        t2[1 if energy < 0.5 else 3] = 1.0

        return np.stack([t0, t1, t2], axis=0)

    @staticmethod
    def _softmax(x: np.ndarray) -> np.ndarray:
        x = x - x.max()
        e = np.exp(x)
        return e / e.sum()

    def _gate(self, s: np.ndarray) -> np.ndarray:
        return self._softmax(s @ self.gate_w + self.gate_b)

    def current_epsilon(self) -> float:
        t = min(self.counter, self.cfg.epsilon_decay_steps)
        f = t / self.cfg.epsilon_decay_steps
        return self.cfg.epsilon_start + (self.cfg.epsilon_end - self.cfg.epsilon_start) * f

    # -- forward --------------------------------------------------------------
    def select_action(self, s: np.ndarray,
                      exploration: bool = True) -> Tuple[int, np.ndarray, np.ndarray, np.ndarray]:
        teachers = self._teachers(s)
        gate = self._gate(s)
        combined = gate @ teachers
        combined = combined / combined.sum()

        student = self._softmax(s @ self.student_w + self.student_b)
        if exploration and random.random() < self.current_epsilon():
            action = random.randint(0, self.n_actions - 1)
        else:
            action = int(np.argmax(0.8 * student + 0.2 * combined))

        return action, s, teachers, combined

    # -- update ---------------------------------------------------------------
    def update(self, s: np.ndarray, action: int, reward: float,
               teachers: np.ndarray, combined: np.ndarray) -> None:
        self.replay.append((s.copy(), int(action), float(reward),
                            teachers.copy(), combined.copy()))
        self.counter += 1
        if self.counter % self.cfg.distillation_train_every == 0 and len(self.replay) >= 2:
            self._train()

    def _train(self) -> None:
        batch = random.sample(self.replay, min(self.cfg.distillation_batch, len(self.replay)))
        for s, a, r, t_out, t_comb in batch:
            logits = s @ self.student_w + self.student_b
            student = self._softmax(logits)

            grad_distill = student - t_comb          # dKL(t_comb || student)/dlogits
            one_hot = np.zeros(self.n_actions); one_hot[a] = 1.0
            grad_rl = -r * (one_hot - student)
            grad = 0.7 * grad_distill + 0.3 * grad_rl

            self.student_w = self._adam_sw.step(self.student_w, np.outer(s, grad))
            self.student_b = self._adam_sb.step(self.student_b, grad)

            # Gate: proper softmax backprop of KL(combined || student)
            gate = self._gate(s)
            log_student = np.log(np.clip(student, 1e-8, 1.0))
            dL_dg = -(t_out @ log_student)                     # (n_teachers,)
            dL_dlogit = gate * (dL_dg - float(gate @ dL_dg))   # softmax Jacobian
            self.gate_w = self._adam_gw.step(self.gate_w, np.outer(s, dL_dlogit))
            self.gate_b = self._adam_gb.step(self.gate_b, dL_dlogit)

    # -- persistence ----------------------------------------------------------
    def _load(self) -> None:
        try:
            with np.load(self.persistence_path) as z:
                self.student_w = z["student_w"]
                self.student_b = z["student_b"]
                self.gate_w = z["gate_w"]
                self.gate_b = z["gate_b"]
                self.counter = int(z["counter"])
        except Exception as exc:                     # noqa: BLE001
            logger.warning("Could not load distillation checkpoint: %s", exc)

    def save(self) -> None:
        if not self.persistence_path:
            return
        np.savez(
            self.persistence_path,
            student_w=self.student_w, student_b=self.student_b,
            gate_w=self.gate_w, gate_b=self.gate_b,
            counter=self.counter,
        )


# =============================================================================
# Cluster aggregator
# =============================================================================
@ray.remote
class ClusterMetricsAggregator:
    """Aggregates per-episode metrics across all workers."""

    def __init__(self, config: Optional[Dict] = None):
        self.cfg = RuntimeConfig(**copy.deepcopy(config or {}))
        self.use_enhancements = self.cfg.use_enhancements and ENHANCEMENTS_AVAILABLE
        self.pareto = ParetoAnalyzer()

        self.n_reports = 0
        self.total_energy = 0.0
        self.total_accuracy = 0.0
        self.total_carbon = 0.0
        self.total_latency = 0.0

        # Bounded accumulators
        from collections import deque
        self._centrality = deque(maxlen=4_096)
        self._connectivity = deque(maxlen=4_096)
        self._density = deque(maxlen=4_096)

    def report(self, record: Dict) -> None:
        self.n_reports += 1
        self.total_energy += float(record["energy"])
        self.total_accuracy += float(record["accuracy"])
        self.total_carbon += float(record["carbon"])
        self.total_latency += float(record.get("latency_ms", 0.0))

        self.pareto.add_record(
            energy_joules=record["energy"],
            accuracy=record["accuracy"] * 100,
            carbon_grams=record["carbon"],
            label=record["label"],
            metadata={"agent_id": record["agent_id"]},
        )

        if self.use_enhancements and "graph_metrics" in record:
            gm = record["graph_metrics"]
            self._centrality.append(float(gm.get("centrality", 0.5)))
            self._connectivity.append(float(gm.get("connectivity", 0.5)))
            self._density.append(float(gm.get("density", 0.5)))

    def finalize(self) -> Dict[str, Any]:
        try:
            frontier = self.pareto.compute_frontier()
            out_path = os.path.join(self.cfg.output_dir,
                                    f"cluster_pareto_{self.cfg.run_id}.json")
            self.pareto.export_json(out_path)
        except Exception as exc:                     # noqa: BLE001
            logger.exception("Pareto export failed: %s", exc)
            frontier = []

        n = max(self.n_reports, 1)
        avg_accuracy = self.total_accuracy / n
        avg_energy = self.total_energy / n
        avg_carbon = self.total_carbon / n
        avg_latency = self.total_latency / n

        # Normalize each objective to [0,1] against its target
        accuracy_score = float(np.clip(avg_accuracy / self.cfg.accuracy_target, 0, 1))
        energy_score = float(np.clip(1.0 - avg_energy / self.cfg.energy_target, 0, 1))
        carbon_score = float(np.clip(1.0 - avg_carbon / self.cfg.carbon_target, 0, 1))
        latency_score = (float(np.clip(1.0 - avg_latency / self.cfg.latency_target_ms, 0, 1))
                         if self.total_latency > 0 else 0.0)

        w = self.cfg.modp_weights
        modp = (w[0] * accuracy_score + w[1] * energy_score
                + w[2] * carbon_score + w[3] * latency_score)

        summary: Dict[str, Any] = {
            "run_id": self.cfg.run_id,
            "n_reports": self.n_reports,
            "total_energy": self.total_energy,
            "total_accuracy": self.total_accuracy,
            "total_carbon": self.total_carbon,
            "avg_accuracy": avg_accuracy,
            "avg_energy": avg_energy,
            "avg_carbon": avg_carbon,
            "avg_latency_ms": avg_latency,
            "frontier_size": len(frontier),
            "modp_composite_score": modp,        # always present
            "graph_metrics": None,              # always present
        }
        if self.use_enhancements:
            summary["graph_metrics"] = {
                "centrality": float(np.mean(self._centrality)) if self._centrality else 0.5,
                "connectivity": float(np.mean(self._connectivity)) if self._connectivity else 0.5,
                "density": float(np.mean(self._density)) if self._density else 0.5,
            }
        return summary


# =============================================================================
# Worker
# =============================================================================
@ray.remote(max_restarts=3, max_task_retries=2)
class GreenAgentWorker:
    def __init__(self, agent_id: int, config: Dict):
        self.agent_id = agent_id
        self.cfg = RuntimeConfig(**copy.deepcopy(config))
        self.use_enhancements = self.cfg.use_enhancements and ENHANCEMENTS_AVAILABLE
        self.episode = 0

        # Original components
        self.policy_engine = PolicyEngine(
            energy_budget=self.cfg.energy_budget,
            baseline_energy=self.cfg.baseline_energy,
        )
        self.negawatt = NegawattReward(self.cfg.baseline_energy)
        self.q_agent = QLearningAgent(state_space=10, action_space=5,
                                      persistence_path=f"q_table_{agent_id}.pkl")
        self.ppo = PPOTrainer(state_dim=10, action_dim=5)
        self.carbon_provider = CarbonIntensityProvider(region=self.cfg.region)
        self.eco_controller = EcoModeController(self.policy_engine)
        self.quantum_metric = QuantumEfficiencyMetric()

        # Enhanced components
        self.node_descriptor = None
        self.workload_descriptor = None
        self.distillation_optimizer: Optional[DistillationOptimizer] = None

        if self.use_enhancements:
            try:
                self.node_descriptor = NodeDescriptor(
                    id=f"worker_{agent_id}",
                    type=NodeType.EDGE,
                    region=self.cfg.region,
                    region_carbon_intensity=self.carbon_provider.get_current_intensity(),
                    energy_per_token=0.00005,
                    helium_connectivity_score=self.cfg.graph_metrics["connectivity"],
                    uptime=0.99,
                    renewable_fraction=0.3,
                    cooling_type=CoolingType.AIR,
                    hardware_model="cpu",
                    graph_metrics=dict(self.cfg.graph_metrics),
                    human_feedback_score=self.cfg.human_feedback_score,
                )
                self.workload_descriptor = WorkloadDescriptor(
                    task_id=f"worker_{agent_id}_task",
                    task_type=TaskType.INFERENCE,
                    tokens=1000,
                    latency_target=self.cfg.latency_target_ms,
                    urgency=Urgency.MEDIUM,
                    estimated_energy_joules=0.001,
                    estimated_carbon_kg=0.0002,
                    user_id=f"agent_{agent_id}",
                    metadata={"source": "distributed_runtime"},
                )
                self.distillation_optimizer = DistillationOptimizer(
                    n_actions=5,
                    cfg=self.cfg,
                    persistence_path=f"distill_{agent_id}.npz",
                )
            except Exception as exc:                 # noqa: BLE001
                logger.error("Enhanced init failed for agent %s: %s", agent_id, exc)
                self.use_enhancements = False

    # -- helpers --------------------------------------------------------------
    def _build_state(self, intensity: float, energy: float,
                     accuracy: float, centrality: float,
                     connectivity: float, human_feedback: float) -> np.ndarray:
        """Documented 10-dim feature vector (slots 7–9 reserved)."""
        return np.array([
            np.clip(intensity / 1000.0, 0, 1),      # 0 carbon intensity (norm)
            np.clip(energy / 100.0, 0, 1),          # 1 energy (norm)
            np.clip(accuracy, 0, 1),                # 2 accuracy
            np.clip(math.log1p(self.episode) / 5.0, 0, 1),  # 3 episode (log)
            np.clip(centrality, 0, 1),              # 4 centrality
            np.clip(connectivity, 0, 1),            # 5 connectivity
            np.clip(human_feedback, 0, 1),          # 6 human feedback
            0.0,                                    # 7 reserved
            0.0,                                    # 8 reserved
            0.0,                                    # 9 reserved
        ], dtype=np.float64)

    def _refresh_descriptors(self, intensity: float) -> None:
        if not self.use_enhancements or self.node_descriptor is None:
            return
        # Per-episode refresh of dynamic fields
        try:
            self.node_descriptor.region_carbon_intensity = intensity
            self.node_descriptor.human_feedback_score = self.cfg.human_feedback_score
            self.node_descriptor.graph_metrics = dict(self.cfg.graph_metrics)
        except Exception as exc:                     # noqa: BLE001
            logger.debug("descriptor refresh skipped: %s", exc)

    # -- episode --------------------------------------------------------------
    async def run_episode(self, aggregator) -> float:
        self.episode += 1
        intensity = self.carbon_provider.get_current_intensity()
        self.eco_controller.update(intensity)
        self._refresh_descriptors(intensity)

        # --- Simulated workload (replace with real sampler when available) ---
        energy = random.uniform(20, 100)
        accuracy = random.uniform(0.7, 1.0)
        quantum_energy = energy * 0.2
        latency_ms = random.uniform(100, 600)

        # --- Distillation action gates the task tier ---
        action = random.randint(0, 4)
        if self.use_enhancements and self.distillation_optimizer is not None:
            s_vec = self._build_state(
                intensity=intensity,
                energy=energy,
                accuracy=accuracy,
                centrality=self.cfg.graph_metrics["centrality"],
                connectivity=self.cfg.graph_metrics["connectivity"],
                human_feedback=self.cfg.human_feedback_score,
            )
            action, s_vec, teachers, combined = \
                self.distillation_optimizer.select_action(s_vec)
            # Action now influences the workload: high tier → higher energy
            energy = energy * (1.0 + 0.1 * action)
        else:
            s_vec = self._build_state(
                intensity, energy, accuracy,
                self.cfg.graph_metrics["centrality"],
                self.cfg.graph_metrics["connectivity"],
                self.cfg.human_feedback_score,
            )

        # --- Reward, shared by Q-learning and distillation ---
        reward = self.negawatt.combined_reward(accuracy=accuracy, energy=energy)

        # --- RL updates ---
        state = random.randint(0, 9)
        next_state = random.randint(0, 9)
        self.q_agent.update(state, action, reward, next_state)
        self.ppo.store_transition(state, action, reward, next_state, done=True)
        self.quantum_metric.add_quantum_energy(quantum_energy)
        self.quantum_metric.set_task_completion_ratio(accuracy)

        if self.use_enhancements and self.distillation_optimizer is not None:
            self.distillation_optimizer.update(s_vec, action, reward, teachers, combined)

        carbon = energy * intensity / 1000.0

        record = {
            "agent_id": self.agent_id,
            "episode": self.episode,
            "energy": energy,
            "accuracy": accuracy,
            "carbon": carbon,
            "latency_ms": latency_ms,
            "label": f"A{self.agent_id}_E{self.episode}",
            "graph_metrics": dict(self.cfg.graph_metrics) if self.use_enhancements else None,
        }

        # Correct await on a Ray ObjectRef
        await asyncio.wrap_future(aggregator.report.remote(record).future())

        if self.episode % 5 == 0:
            self.ppo.train()
        if self.episode % 50 == 0 and self.distillation_optimizer is not None:
            self.distillation_optimizer.save()

        return reward


# =============================================================================
# Cluster orchestrator
# =============================================================================
class RayDistributedGreenCluster:
    def __init__(self, num_agents: int, config: Dict):
        self.cfg = RuntimeConfig(**copy.deepcopy(config))
        ray.init(ignore_reinit_error=True)
        atexit.register(self.close)

        self.aggregator = ClusterMetricsAggregator.remote(config)
        self.agents = [GreenAgentWorker.remote(i, config)
                       for i in range(num_agents)]
        self.num_agents = num_agents
        self._closed = False

    async def run_async(self, episodes_per_agent: int) -> Dict[str, Any]:
        try:
            for _ in range(episodes_per_agent):
                tasks = [a.run_episode.remote(self.aggregator) for a in self.agents]
                # Proper async gather on Ray ObjectRefs
                await asyncio.gather(*[asyncio.wrap_future(t.future()) for t in tasks])
            summary = await asyncio.wrap_future(
                self.aggregator.finalize.remote().future()
            )
        finally:
            # Always try to persist a final summary
            try:
                summary = ray.get(self.aggregator.finalize.remote(), timeout=30)
            except Exception:                        # noqa: BLE001
                summary = {"error": "finalize failed"}
            logger.info("Cluster summary: %s", summary)
        return summary

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            ray.shutdown()
        except Exception:                            # noqa: BLE001
            pass


# =============================================================================
# Entry point
# =============================================================================
if __name__ == "__main__":
    config = {
        "energy_budget": 10_000.0,
        "baseline_energy": 120.0,
        "region": "ID",
        "use_enhancements": True,
        "modp_weights": [0.4, 0.3, 0.2, 0.1],
        "human_feedback_score": 0.6,
        "graph_metrics": {"centrality": 0.7, "connectivity": 0.5, "density": 0.5},
        "output_dir": "./out",
    }

    cluster = RayDistributedGreenCluster(num_agents=4, config=config)
    try:
        asyncio.run(cluster.run_async(episodes_per_agent=20))
    finally:
        cluster.close()
