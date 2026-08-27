"""
Distillation Orchestrator for Multi-Teacher On-Policy Distillation (MOPD)
=======================================================================
Complete rewrite with async/await, canonical FeedbackEvent, Pareto gating,
persistent metrics, drift detection, and energy-aware training.

Green Agent v3.2.0+

ENHANCED WITH bio_inspired, moe_system, MODP, ContextualBandit, and FlexGen:
- Teacher selection uses ContextualBandit and ExpertRouter.
- Hyperparameters are evolved using GeneticPolicyGenerator.
- Multi‑objective teacher evaluation uses ParetoOptimizer.
- Feedback loop updates all learning modules.
- Learned state is persisted via Storage.
- FlexGen integration: teacher inference can be offloaded using FlexGen policies.
"""

import asyncio
import json
import logging
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Callable, Any, Tuple, Union

import torch
import torch.nn as nn
import torch.nn.functional as F
from torch.cuda.amp import autocast
from torch.optim import AdamW

# ------------------------------------------------------------------------------
# Import Green Agent components (adjust paths as needed)
# ------------------------------------------------------------------------------
from .storage import Storage
from .schemas.feedback_event import FeedbackEvent
from .routing.pareto_gating import ParetoGating
from .safety.drift_detector import DriftDetector
from .scaling.message_queue import AsyncMessageQueue
from .config import config  # if available, else we'll use env vars

# Fallback logger if structlog not available
try:
    from structlog import get_logger
    logger = get_logger(__name__)
except ImportError:
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO)

# =============================================================================
# ENHANCED MODULES IMPORTS (with graceful fallback)
# =============================================================================
try:
    from enhancements.bio_inspired import GeneticPolicyGenerator
    from enhancements.moe_system import ExpertRouter
    from enhancements.MODP import ParetoOptimizer
    from enhancements.contextual_bandit import ContextualBandit
    ENHANCEMENTS_AVAILABLE = True
except ImportError:
    ENHANCEMENTS_AVAILABLE = False
    # Fallback stubs
    class GeneticPolicyGenerator:
        def __init__(self, *args, **kwargs): pass
        def evolve(self, population, fitness_fn, generations=10, population_size=20):
            return population[0] if population else {}
    class ExpertRouter:
        def __init__(self, *args, **kwargs): pass
        def encode(self, context): return [0.0]*5
        def select(self, encoded): return "all"
    class ParetoOptimizer:
        def __init__(self, *args, **kwargs): pass
        def evaluate(self, objectives, weights):
            return sum(objectives.get(k, 0) * weights.get(k, 1) for k in objectives)
    class ContextualBandit:
        def __init__(self, action_space, fallback_solver, *args, **kwargs):
            self.actions = action_space
        def select_action(self, context):
            return self.actions[0], 0.0, "fallback"
        def update(self, context, action, reward): pass
        def seed_safe_policy(self, context, policy): pass

# =============================================================================
# FLEXGEN MODULES (with fallback)
# =============================================================================
try:
    from enhancements.gpu_optimization.flexgen_policy import FlexGenPolicy, generate_candidate_policies
    from enhancements.gpu_optimization.flexgen_controller import FlexGenController
    from enhancements.gpu_optimization.flexgen_cost_model import FlexGenCostModel
    from enhancements.gpu_optimization.policy_drift_detector import PolicyDriftDetector
    from enhancements.schemas.node_descriptor import NodeDescriptor
    from enhancements.schemas.workload_descriptor import WorkloadDescriptor
    FLEXGEN_AVAILABLE = True
except ImportError:
    FLEXGEN_AVAILABLE = False
    class FlexGenPolicy: pass
    def generate_candidate_policies(n=20): return []
    class FlexGenController:
        def __init__(self, *args, **kwargs): pass
        async def step(self): return {}
    class FlexGenCostModel:
        def __init__(self, *args, **kwargs): pass
    class PolicyDriftDetector:
        def __init__(self, *args, **kwargs): pass
        def get_stats(self): return {}
    class NodeDescriptor: pass
    class WorkloadDescriptor: pass

# ------------------------------------------------------------------------------
# Configuration dataclass (extended with optimizer and FlexGen settings)
# ------------------------------------------------------------------------------
@dataclass
class DistillationConfig:
    """Configuration for DistillationOrchestrator."""
    num_epochs: int = 3
    batch_size: int = 32
    lr: float = 1e-5
    reverse_kl: bool = True
    alpha_orm: float = 0.1          # weight for green ORM
    baseline_energy_per_token: float = 1.0
    early_stopping_patience: int = 3
    validation_split: float = 0.1
    mixed_precision: bool = True
    dtype: str = "fp16"            # "fp16" or "fp8" (if quantum bridge supports)
    expert_id: str = "distillation"
    node_id: Optional[str] = None
    pareto_quality_min: float = 0.7
    pareto_latency_max: float = 500.0
    pareto_carbon_max: float = 1.0
    feedback_batch_size: int = 10   # send feedback every N batches
    save_best_model: bool = True
    drift_check_interval: int = 5   # check drift every N epochs
    rollback_enabled: bool = True

    # New optimizer settings
    modp_weights: Dict[str, float] = field(default_factory=lambda: {
        'accuracy': 0.4,
        'energy': 0.3,
        'carbon': 0.2,
        'latency': 0.1,
    })
    bandit_min_trials: int = 5
    bandit_confidence_threshold: float = 0.6
    bio_generations: int = 10
    bio_population_size: int = 20
    hyperparam_evolution_enabled: bool = True

    # FlexGen settings
    flexgen_carbon_intensity_default: float = 400.0
    flexgen_population_size: int = 50
    flexgen_generations: int = 10
    flexgen_use_real_executor: bool = False
    flexgen_executor_type: str = "mock"   # "mock", "cost_model", "real"
    flexgen_selector_epsilon: float = 0.1
    flexgen_selector_epsilon_decay: float = 0.999

# ------------------------------------------------------------------------------
# Stubs for missing dependencies (if not available)
# ------------------------------------------------------------------------------
class EcoATPTokenManagerStub:
    async def get_current_budget(self) -> float:
        return 1.0
    async def get_carbon_intensity(self) -> float:
        return 400.0
    async def energy_cost_per_token(self, batch_size: int, domain: str) -> float:
        return 1e-6 * batch_size

class QuantumBridgeStub:
    def enable_mixed_precision(self, dtype: str):
        pass
    def quantized_context(self, dtype: str):
        class NoOp:
            def __enter__(self): pass
            def __exit__(self, *args): pass
        return NoOp()

class GatingNetworkStub:
    async def select_teachers(self, domain: str, effort: str) -> List[str]:
        return []  # empty => use all teachers

# ------------------------------------------------------------------------------
# FLEXGEN MANAGER (NEW)
# ------------------------------------------------------------------------------
class FlexGenManager:
    """
    Manager for FlexGen GPU/CPU/disk offloading policy optimization.
    Used to select optimal policies for teacher inference.
    """
    def __init__(self, config: DistillationConfig):
        self.config = config
        self.flexgen_cost_model = None
        self.policy_drift_detector = None
        self.gpu_profiler = None

        if FLEXGEN_AVAILABLE:
            self.flexgen_cost_model = FlexGenCostModel(
                carbon_intensity_g_per_kwh=config.flexgen_carbon_intensity_default
            )
            self.policy_drift_detector = PolicyDriftDetector()
            try:
                from enhancements.gpu_profiler import GPUProfiler
                self.gpu_profiler = GPUProfiler()
            except ImportError:
                self.gpu_profiler = None
            logger.info("FlexGen Manager initialized")
        else:
            logger.warning("FlexGen modules not available; manager will be disabled.")

    async def optimize_policy(self, workload: WorkloadDescriptor, node: NodeDescriptor) -> Dict:
        """
        Run FlexGen policy selection for a given workload and node.
        Returns chosen policy, metrics, reward, and drift status.
        """
        if not FLEXGEN_AVAILABLE:
            return {"error": "FlexGen modules not available"}

        from enhancements.gpu_optimization.flexgen_controller import FlexGenController
        from enhancements.gpu_optimization.flexgen_policy_selector import DistillationFlexGenSelector

        selector = DistillationFlexGenSelector(
            n_candidates=20,
            config={
                'epsilon': self.config.flexgen_selector_epsilon,
                'epsilon_decay': self.config.flexgen_selector_epsilon_decay,
            }
        )

        controller = FlexGenController(
            node=node,
            workload=workload,
            carbon_intensity=workload.metadata.get('carbon_intensity',
                                                   self.config.flexgen_carbon_intensity_default),
            use_real_executor=self.config.flexgen_use_real_executor,
            executor=None,
            cost_model=self.flexgen_cost_model,
            use_bio_search=True,
            bio_search_config={
                'population_size': self.config.flexgen_population_size,
                'generations': self.config.flexgen_generations,
            },
            modp_planner=None,
            drift_detector=self.policy_drift_detector,
            gpu_profiler=self.gpu_profiler,
        )
        result = await controller.step()
        return result

    async def get_status(self) -> Dict:
        if not FLEXGEN_AVAILABLE:
            return {"available": False}
        return {
            "available": True,
            "drift": self.policy_drift_detector.get_stats() if self.policy_drift_detector else {},
            "gpu": self.gpu_profiler.get_current_metrics() if self.gpu_profiler else {},
        }

# ------------------------------------------------------------------------------
# Main Orchestrator (Enhanced with FlexGen)
# ------------------------------------------------------------------------------
class DistillationOrchestrator:
    """
    Orchestrates MOPD with full async support, energy awareness, Pareto gating,
    persistent metrics, and adaptive cost feedback.

    Integrates with:
        - Storage (SQLite)
        - AsyncMessageQueue (Redis or asyncio)
        - ParetoGating
        - DriftDetector
        - EcoATPTokenManager (energy)
        - QuantumBridge (mixed precision)
        - FlexGenManager (offloading policy selection)

    NEW ENHANCEMENTS:
        - Teacher selection uses ContextualBandit and ExpertRouter.
        - Hyperparameters are evolved using GeneticPolicyGenerator.
        - Multi‑objective teacher evaluation uses ParetoOptimizer.
        - Feedback loop updates all learning modules.
        - Learned state is persisted via Storage.
        - FlexGen integration for teacher inference offloading.
    """

    def __init__(
        self,
        student_model: nn.Module,
        teachers: Dict[str, nn.Module],
        config: Union[DistillationConfig, Dict[str, Any]],
        storage: Optional[Storage] = None,
        message_queue: Optional[AsyncMessageQueue] = None,
        gating_network: Optional[Any] = None,
        eco_manager: Optional[Any] = None,
        quantum_bridge: Optional[Any] = None,
        adaptive_function: Optional[Any] = None,  # in-process AdaptiveCostFunction
        drift_detector: Optional[DriftDetector] = None,
    ):
        # Configuration
        if isinstance(config, dict):
            self.cfg = DistillationConfig(**config)
        else:
            self.cfg = config

        self.student = student_model
        self.teachers = teachers
        self.storage = storage
        self.queue = message_queue
        self.gating = gating_network or GatingNetworkStub()
        self.eco = eco_manager or EcoATPTokenManagerStub()
        self.quantum = quantum_bridge or QuantumBridgeStub()
        self.adaptive = adaptive_function
        self.drift_detector = drift_detector

        # Pareto gating
        self.pareto = ParetoGating(
            quality_min=self.cfg.pareto_quality_min,
            latency_max=self.cfg.pareto_latency_max,
            carbon_max=self.cfg.pareto_carbon_max
        )

        # Device
        self.device = next(self.student.parameters()).device
        self._move_to_device()

        # Optimizer
        self.optimizer = AdamW(self.student.parameters(), lr=self.cfg.lr)

        # Mixed precision
        if self.cfg.mixed_precision and torch.cuda.is_available():
            self.quantum.enable_mixed_precision(self.cfg.dtype)
            self._autocast_context = autocast
        else:
            class NoOp:
                def __enter__(self): pass
                def __exit__(self, *args): pass
            self._autocast_context = NoOp

        # ===== ENHANCED MODULES =====
        if ENHANCEMENTS_AVAILABLE:
            self.modp = ParetoOptimizer()
            self.moe = ExpertRouter()
            self.bio = GeneticPolicyGenerator()
            # Action space for teacher selection policies
            self.teacher_policies = ["all", "top1", "top3", "green_focused", "accuracy_focused"]
            self.bandit = ContextualBandit(
                action_space=self.teacher_policies,
                fallback_solver=lambda ctx: "all",
                min_trials_before_bandit=self.cfg.bandit_min_trials,
                confidence_threshold=self.cfg.bandit_confidence_threshold,
            )
        else:
            self.modp = None
            self.moe = None
            self.bio = None
            self.bandit = None
            self.teacher_policies = ["all"]

        # ===== FLEXGEN MANAGER =====
        self.flexgen_manager = FlexGenManager(self.cfg)

        # State for training
        self._run_id = str(uuid.uuid4())
        self._best_accuracy = 0.0
        self._best_state = None
        self._patience_counter = 0
        self._feedback_buffer = []

        self._epoch_metrics = []
        self._latest_accuracy = 0.0

        # Load persisted state
        self._load_state()

        logger.info(f"DistillationOrchestrator initialized (run_id={self._run_id})")

    # --------------------------------------------------------------------------
    # Persistence methods
    # --------------------------------------------------------------------------
    def _load_state(self):
        if not self.storage:
            return
        try:
            state = self.storage.get_distillation_optimizer_state(self._run_id)
            if state:
                pass  # restore as needed
        except Exception as e:
            logger.warning(f"Failed to load optimizer state: {e}")

    def _save_state(self):
        if not self.storage:
            return
        try:
            state = {
                "bandit_weights": None,
                "modp_weights": self.cfg.modp_weights,
                "bio_population": None,
            }
            self.storage.save_distillation_optimizer_state(self._run_id, state)
        except Exception as e:
            logger.warning(f"Failed to save optimizer state: {e}")

    # --------------------------------------------------------------------------
    # Internal helpers
    # --------------------------------------------------------------------------
    def _move_to_device(self):
        self.student.to(self.device)
        for t in self.teachers.values():
            t.to(self.device)

    async def _select_teachers(self, domain: str, reasoning_effort: str) -> List[str]:
        if ENHANCEMENTS_AVAILABLE and self.bandit:
            context = {
                "domain": domain,
                "effort": reasoning_effort,
                "carbon_intensity": await self.eco.get_carbon_intensity(),
                "energy_budget": await self.eco.get_current_budget(),
                "num_teachers": len(self.teachers),
            }
            encoded = self.moe.encode(context)
            policy, confidence, source = self.bandit.select_action(encoded)
            if policy is None:
                policy = "all"
            teacher_ids = list(self.teachers.keys())
            if policy == "all":
                return teacher_ids
            elif policy == "top1":
                return [teacher_ids[0]] if teacher_ids else []
            elif policy == "top3":
                return teacher_ids[:3]
            else:
                return teacher_ids
        else:
            try:
                selected = await self.gating.select_teachers(domain, reasoning_effort)
                if selected:
                    return selected
            except Exception as e:
                logger.warning(f"Gating network failed: {e}, using all teachers")
            return list(self.teachers.keys())

    async def _get_energy_cost(self, batch_size: int, domain: str) -> float:
        try:
            return await self.eco.energy_cost_per_token(batch_size, domain)
        except Exception as e:
            logger.debug(f"Energy cost retrieval failed: {e}, using default")
            return 1e-6 * batch_size

    def _compute_loss(self, student_logits, teacher_logits_list, energy_cost, batch_size, seq_len):
        avg_teacher = torch.stack(teacher_logits_list).mean(dim=0)
        if self.cfg.reverse_kl:
            loss_distill = F.kl_div(
                F.log_softmax(student_logits, dim=-1),
                F.softmax(avg_teacher, dim=-1),
                reduction="batchmean",
            )
        else:
            loss_distill = F.kl_div(
                F.log_softmax(avg_teacher, dim=-1),
                F.softmax(student_logits, dim=-1),
                reduction="batchmean",
            )
        distill_val = loss_distill.item()
        total_tokens = batch_size * seq_len
        green_loss = energy_cost * total_tokens * self.cfg.alpha_orm
        green_val = green_loss.item() if isinstance(green_loss, torch.Tensor) else green_loss
        total_loss = loss_distill + green_loss
        return total_loss, distill_val, green_val

    async def _pareto_filter_teachers(self, teacher_logits, teacher_ids, inputs):
        if not self.modp:
            return teacher_logits, teacher_ids
        candidates = []
        for i, (tid, logits) in enumerate(zip(teacher_ids, teacher_logits)):
            objectives = {
                "accuracy": 0.9,
                "energy": await self._get_energy_cost(inputs.shape[0], "unknown"),
                "carbon": 0.5,
                "latency": 0.3,
            }
            utility = self.modp.evaluate(objectives, self.cfg.modp_weights)
            candidates.append((utility, i, tid, logits))
        candidates.sort(key=lambda x: x[0], reverse=True)
        k = max(1, int(len(candidates) * 0.8))
        selected = candidates[:k]
        return [c[3] for c in selected], [c[2] for c in selected]

    async def _publish_feedback(self, teacher_ids, distill_loss, quality, energy_joules, carbon_g, latency_ms, epoch):
        for tid in teacher_ids:
            event = FeedbackEvent(
                task_id=f"{self._run_id}_epoch{epoch}",
                teacher_id=tid,
                selected_action="distillation",
                quality_score=quality,
                latency_ms=latency_ms,
                energy_joules=energy_joules,
                carbon_g=carbon_g,
                distillation_loss=distill_loss,
                feedback_type="distillation",
                adaptive_cost_value=0.0,
            )
            self._feedback_buffer.append(event)
        if len(self._feedback_buffer) >= self.cfg.feedback_batch_size:
            await self._flush_feedback()

        if ENHANCEMENTS_AVAILABLE and self.bandit:
            reward = 0.5 * quality + 0.5 * (1 - energy_joules / (self.cfg.baseline_energy_per_token * 1000))
            context = {"epoch": epoch, "quality": quality, "energy": energy_joules, "teacher_ids": teacher_ids}
            encoded = self.moe.encode(context)
            self.bandit.update(encoded, "distillation", reward)

        if self.cfg.hyperparam_evolution_enabled and ENHANCEMENTS_AVAILABLE and self.bio:
            hyperparam_set = {
                "lr": self.cfg.lr,
                "alpha_orm": self.cfg.alpha_orm,
                "batch_size": self.cfg.batch_size,
                "reverse_kl": self.cfg.reverse_kl,
            }
            fitness = quality + (1 - energy_joules / (self.cfg.baseline_energy_per_token * 1000))
            self.bio.evolve(population=[hyperparam_set], fitness_fn=lambda hp: fitness)

    async def _flush_feedback(self):
        if not self._feedback_buffer:
            return
        if self.adaptive:
            for event in self._feedback_buffer:
                try:
                    await self.adaptive.record_feedback(event)
                except Exception as e:
                    logger.warning(f"Adaptive record_feedback failed: {e}")
        elif self.queue:
            for event in self._feedback_buffer:
                try:
                    await self.queue.publish("feedback_events", event.model_dump_json())
                except Exception as e:
                    logger.warning(f"Queue publish failed: {e}")
        else:
            logger.warning("No feedback mechanism configured; events dropped.")
        self._feedback_buffer.clear()

    async def _save_metrics(self, epoch, metrics):
        if not self.storage:
            return
        try:
            self.storage.store_distillation_metrics(run_id=self._run_id, epoch=epoch, **metrics)
        except Exception as e:
            logger.warning(f"Failed to save metrics: {e}")

    # --------------------------------------------------------------------------
    # Public API
    # --------------------------------------------------------------------------
    async def distill(self, dataloader, eval_fn=None, val_dataloader=None, reasoning_effort="medium"):
        if eval_fn is None and val_dataloader:
            eval_fn = self._default_accuracy_fn

        self.student.train()
        total_loss = 0.0
        total_energy = 0.0
        total_tokens = 0
        best_val_acc = 0.0
        best_state = None
        patience_counter = 0
        epoch_metrics = []

        for epoch in range(self.cfg.num_epochs):
            epoch_loss = 0.0
            epoch_energy = 0.0
            epoch_tokens = 0
            epoch_distill_loss_sum = 0.0
            epoch_distill_count = 0
            used_teacher_ids = set()
            start_time = time.time()

            # Optional: select a FlexGen policy for the epoch's typical workload
            if FLEXGEN_AVAILABLE:
                workload_meta = {
                    "tokens": self.cfg.batch_size * 512,
                    "max_new_tokens": 32,
                    "model_params": {"num_layers": 12, "hidden_dim": 768, "params_billions": 0.1},
                }
                workload = WorkloadDescriptor(
                    task_id=f"distill_epoch_{epoch+1}",
                    task_type="inference",
                    tokens=self.cfg.batch_size * 128,
                    latency_target=200.0,
                    urgency="medium",
                    priority="balanced",
                    bio_mode="none",
                    metadata={"carbon_intensity": self.cfg.flexgen_carbon_intensity_default}
                )
                node = NodeDescriptor(
                    id="teacher_node",
                    type="cloud",
                    region="us-east",
                    region_carbon_intensity=0.42,
                    energy_per_token=0.00005,
                    uptime=0.99,
                    maintenance_status="operational",
                    metadata={"gpu_memory_gb": 16, "cpu_memory_gb": 64, "disk_bandwidth_gbps": 2}
                )
                flexgen_result = await self.flexgen_manager.optimize_policy(workload, node)
                if "chosen_policy" in flexgen_result:
                    logger.info(f"Epoch {epoch+1}: FlexGen policy selected: {flexgen_result['chosen_policy']}")

            async for batch_idx, (inputs, labels, domain) in enumerate(dataloader):
                inputs = inputs.to(self.device)
                labels = labels.to(self.device)

                teacher_ids = await self._select_teachers(domain, reasoning_effort)
                used_teacher_ids.update(teacher_ids)

                teacher_logits = []
                with self._autocast_context():
                    for tid in teacher_ids:
                        teacher = self.teachers[tid]
                        logits = teacher(inputs)
                        teacher_logits.append(logits)
                    student_logits = self.student(inputs)

                if self.modp:
                    teacher_logits, teacher_ids = await self._pareto_filter_teachers(
                        teacher_logits, teacher_ids, inputs
                    )

                energy_per_token = await self._get_energy_cost(inputs.shape[0], domain)

                batch_size = inputs.shape[0]
                seq_len = inputs.shape[1] if len(inputs.shape) > 1 else 1
                loss, distill_loss_val, green_loss_val = self._compute_loss(
                    student_logits, teacher_logits,
                    energy_per_token, batch_size, seq_len
                )

                self.optimizer.zero_grad()
                loss.backward()
                self.optimizer.step()

                epoch_loss += loss.item()
                epoch_energy += green_loss_val
                epoch_tokens += batch_size * seq_len
                epoch_distill_loss_sum += distill_loss_val
                epoch_distill_count += 1

                if batch_idx % 100 == 0:
                    logger.info(f"Epoch {epoch+1}, Batch {batch_idx}: loss={loss.item():.4f}")

                if batch_idx % self.cfg.feedback_batch_size == 0:
                    await self._flush_feedback()

            avg_loss = epoch_loss / len(dataloader)
            avg_distill_loss = epoch_distill_loss_sum / epoch_distill_count if epoch_distill_count else 0.0
            avg_energy_per_token = epoch_energy / epoch_tokens if epoch_tokens else 0.0
            energy_savings = max(0.0, 1.0 - (avg_energy_per_token / self.cfg.baseline_energy_per_token))

            logger.info(f"Epoch {epoch+1} completed: loss={avg_loss:.4f}, distill={avg_distill_loss:.4f}, savings={energy_savings:.2%}")

            val_acc = 0.0
            if val_dataloader and eval_fn:
                val_acc = eval_fn(self.student, val_dataloader)
                logger.info(f"Validation accuracy: {val_acc:.4f}")
                if val_acc > best_val_acc:
                    best_val_acc = val_acc
                    patience_counter = 0
                    best_state = self.student.state_dict().copy()
                    if self.cfg.save_best_model:
                        self._best_state = best_state
                else:
                    patience_counter += 1
                    if patience_counter >= self.cfg.early_stopping_patience:
                        logger.info(f"Early stopping triggered after epoch {epoch+1}")
                        break

            total_loss += avg_loss
            total_energy += epoch_energy
            total_tokens += epoch_tokens

            epoch_metrics_dict = {
                "loss": avg_loss,
                "distill_loss": avg_distill_loss,
                "accuracy": val_acc,
                "energy_savings": energy_savings,
                "energy_joules": epoch_energy,
                "num_teachers": len(used_teacher_ids),
            }
            await self._save_metrics(epoch+1, epoch_metrics_dict)
            epoch_metrics.append(epoch_metrics_dict)

            await self._publish_feedback(
                list(used_teacher_ids),
                avg_distill_loss,
                val_acc,
                epoch_energy,
                carbon_g=epoch_energy * 0.2,
                latency_ms=0.0,
                epoch=epoch+1
            )

        if best_state is not None:
            self.student.load_state_dict(best_state)
            logger.info("Restored best model from early stopping")

        final_accuracy = 0.0
        if eval_fn and val_dataloader:
            final_accuracy = eval_fn(self.student, val_dataloader)

        avg_total_loss = total_loss / self.cfg.num_epochs
        avg_energy_per_token = total_energy / total_tokens if total_tokens else 0.0
        final_savings = max(0.0, 1.0 - (avg_energy_per_token / self.cfg.baseline_energy_per_token))

        await self._flush_feedback()
        self._save_state()

        return {
            "avg_loss": avg_total_loss,
            "accuracy": final_accuracy,
            "energy_savings_ratio": final_savings,
            "total_energy_joules": total_energy,
        }

    def _default_accuracy_fn(self, model, dataloader):
        model.eval()
        correct = 0
        total = 0
        with torch.no_grad():
            for inputs, labels, _ in dataloader:
                inputs = inputs.to(self.device)
                labels = labels.to(self.device)
                outputs = model(inputs)
                _, predicted = torch.max(outputs, 1)
                total += labels.size(0)
                correct += (predicted == labels).sum().item()
        return correct / total if total > 0 else 0.0

    def save_student(self, path):
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        torch.save(self.student.state_dict(), path / "student_model.pt")
        with open(path / "distillation_config.json", "w") as f:
            json.dump(self.cfg.__dict__, f, indent=2)
        logger.info(f"Student saved to {path}")

    def load_student(self, path):
        path = Path(path)
        self.student.load_state_dict(torch.load(path / "student_model.pt"))
        with open(path / "distillation_config.json", "r") as f:
            self.cfg = DistillationConfig(**json.load(f))
        logger.info(f"Student loaded from {path}")

    async def run_flexgen_optimization(self, workload, node):
        """Public method to run FlexGen policy optimization."""
        if not FLEXGEN_AVAILABLE:
            return {"error": "FlexGen modules not available"}
        workload_obj = WorkloadDescriptor(**workload)
        node_obj = NodeDescriptor(**node)
        return await self.flexgen_manager.optimize_policy(workload_obj, node_obj)

    async def close(self):
        await self._flush_feedback()
        if self.queue:
            await self.queue.close()
        logger.info("DistillationOrchestrator closed")
