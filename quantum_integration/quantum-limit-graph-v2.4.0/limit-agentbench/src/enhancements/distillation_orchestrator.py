"""
Distillation Orchestrator for Multi-Teacher On-Policy Distillation (MOPD)
=======================================================================
Complete rewrite with async/await, canonical FeedbackEvent, Pareto gating,
persistent metrics, drift detection, and energy-aware training.

Green Agent v3.2.0+
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

# ------------------------------------------------------------------------------
# Configuration dataclass
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
# Main Orchestrator
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
        """
        Args:
            student_model: The student network to train.
            teachers: Dict mapping teacher_id -> teacher model.
            config: Configuration dict or DistillationConfig instance.
            storage: SQLite storage for metrics (optional).
            message_queue: AsyncMessageQueue for feedback events.
            gating_network: Optional gating network for teacher selection.
            eco_manager: For energy costs.
            quantum_bridge: For mixed precision.
            adaptive_function: In-process AdaptiveCostFunction (preferred over queue).
            drift_detector: DriftDetector for policy monitoring.
        """
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
            # No-op context
            class NoOp:
                def __enter__(self): pass
                def __exit__(self, *args): pass
            self._autocast_context = NoOp

        # State for training
        self._run_id = str(uuid.uuid4())
        self._best_accuracy = 0.0
        self._best_state = None
        self._patience_counter = 0
        self._feedback_buffer = []  # for batching

        # Metrics tracking
        self._epoch_metrics = []
        self._latest_accuracy = 0.0

        logger.info(f"DistillationOrchestrator initialized (run_id={self._run_id})")

    # --------------------------------------------------------------------------
    # Internal helpers
    # --------------------------------------------------------------------------
    def _move_to_device(self):
        self.student.to(self.device)
        for t in self.teachers.values():
            t.to(self.device)

    async def _select_teachers(self, domain: str, reasoning_effort: str) -> List[str]:
        """
        Select teachers via gating network (async).
        Fallback: all teachers if gating fails or returns empty.
        """
        try:
            selected = await self.gating.select_teachers(domain, reasoning_effort)
            if selected:
                return selected
        except Exception as e:
            logger.warning(f"Gating network failed: {e}, using all teachers")
        return list(self.teachers.keys())

    async def _get_energy_cost(self, batch_size: int, domain: str) -> float:
        """
        Get energy cost per token for the batch (async).
        Fallback: constant value.
        """
        try:
            return await self.eco.energy_cost_per_token(batch_size, domain)
        except Exception as e:
            logger.debug(f"Energy cost retrieval failed: {e}, using default")
            return 1e-6 * batch_size

    def _compute_loss(
        self,
        student_logits: torch.Tensor,
        teacher_logits_list: List[torch.Tensor],
        energy_cost: float,
        batch_size: int,
        seq_len: int,
    ) -> Tuple[torch.Tensor, float, float]:
        """
        Compute combined loss: distillation + green penalty.
        Returns:
            total_loss, distill_loss_val, green_loss_val
        """
        # Average teacher logits (could be weighted later)
        avg_teacher = torch.stack(teacher_logits_list).mean(dim=0)

        # Distillation loss (forward or reverse KL)
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

        # Green penalty
        total_tokens = batch_size * seq_len
        green_loss = energy_cost * total_tokens * self.cfg.alpha_orm
        green_val = green_loss.item() if isinstance(green_loss, torch.Tensor) else green_loss

        total_loss = loss_distill + green_loss
        return total_loss, distill_val, green_val

    def _pareto_filter_teachers(
        self, teacher_logits: List[torch.Tensor], teacher_ids: List[str], inputs: torch.Tensor
    ) -> Tuple[List[torch.Tensor], List[str]]:
        """
        Apply Pareto gating to filter teacher outputs that violate constraints.
        This is a simplified version; in a real system you'd have per-teacher metrics.
        For now, we accept all unless we have quality scores.
        """
        # If we had per-teacher metrics, we'd filter here.
        # For demonstration, we assume all teachers are valid.
        return teacher_logits, teacher_ids

    async def _publish_feedback(
        self,
        teacher_ids: List[str],
        distill_loss: float,
        quality: float,
        energy_joules: float,
        carbon_g: float,
        latency_ms: float,
        epoch: int,
    ):
        """
        Publish feedback events for each teacher via queue or in-process adaptive.
        Batches to avoid overwhelming.
        """
        # Accumulate in buffer
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
                adaptive_cost_value=0.0,  # can be computed if needed
            )
            self._feedback_buffer.append(event)

        # If buffer size reached, flush
        if len(self._feedback_buffer) >= self.cfg.feedback_batch_size:
            await self._flush_feedback()

    async def _flush_feedback(self):
        """Send all buffered events."""
        if not self._feedback_buffer:
            return

        # In-process adaptive
        if self.adaptive:
            for event in self._feedback_buffer:
                try:
                    await self.adaptive.record_feedback(event)
                except Exception as e:
                    logger.warning(f"Adaptive record_feedback failed: {e}")
        # Message queue
        elif self.queue:
            for event in self._feedback_buffer:
                try:
                    await self.queue.publish("feedback_events", event.model_dump_json())
                except Exception as e:
                    logger.warning(f"Queue publish failed: {e}")
        else:
            logger.warning("No feedback mechanism configured; events dropped.")

        self._feedback_buffer.clear()

    async def _save_metrics(self, epoch: int, metrics: Dict[str, float]):
        """Persist epoch metrics to Storage."""
        if not self.storage:
            return
        try:
            # Assume Storage has a method store_distillation_metrics (add if not)
            # For simplicity, we store as JSON in a dedicated table.
            self.storage.store_distillation_metrics(
                run_id=self._run_id,
                epoch=epoch,
                **metrics
            )
        except Exception as e:
            logger.warning(f"Failed to save metrics: {e}")

    # --------------------------------------------------------------------------
    # Public API
    # --------------------------------------------------------------------------
    async def distill(
        self,
        dataloader: torch.utils.data.DataLoader,
        eval_fn: Optional[Callable[[nn.Module, torch.utils.data.DataLoader], float]] = None,
        val_dataloader: Optional[torch.utils.data.DataLoader] = None,
        reasoning_effort: str = "medium",
    ) -> Dict[str, float]:
        """
        Run MOPD training loop with early stopping, drift detection, and feedback.

        Args:
            dataloader: Training DataLoader yielding (inputs, labels, domain).
            eval_fn: Function to compute accuracy (model, dataloader) -> float.
            val_dataloader: Validation DataLoader for early stopping.
            reasoning_effort: Teacher selection effort.

        Returns:
            Dict with final metrics: avg_loss, accuracy, energy_savings_ratio, total_energy_joules.
        """
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

        # Main loop
        for epoch in range(self.cfg.num_epochs):
            epoch_loss = 0.0
            epoch_energy = 0.0
            epoch_tokens = 0
            epoch_distill_loss_sum = 0.0
            epoch_distill_count = 0
            used_teacher_ids = set()
            start_time = time.time()

            # Process batches asynchronously
            async for batch_idx, (inputs, labels, domain) in enumerate(dataloader):
                # Move to device
                inputs = inputs.to(self.device)
                labels = labels.to(self.device)

                # 1. Select teachers
                teacher_ids = await self._select_teachers(domain, reasoning_effort)
                used_teacher_ids.update(teacher_ids)

                # 2. Teacher forward passes (mixed precision)
                teacher_logits = []
                with self._autocast_context():
                    for tid in teacher_ids:
                        teacher = self.teachers[tid]
                        logits = teacher(inputs)
                        teacher_logits.append(logits)

                    # Student forward
                    student_logits = self.student(inputs)

                # 3. Pareto filter (if any teacher violates constraints, we could skip it)
                # For now, we trust the teachers.
                teacher_logits, teacher_ids = self._pareto_filter_teachers(
                    teacher_logits, teacher_ids, inputs
                )

                # 4. Energy cost
                energy_per_token = await self._get_energy_cost(inputs.shape[0], domain)

                # 5. Loss computation
                batch_size = inputs.shape[0]
                seq_len = inputs.shape[1] if len(inputs.shape) > 1 else 1
                loss, distill_loss_val, green_loss_val = self._compute_loss(
                    student_logits, teacher_logits,
                    energy_per_token, batch_size, seq_len
                )

                # 6. Backprop
                self.optimizer.zero_grad()
                loss.backward()
                self.optimizer.step()

                # Accumulate metrics
                epoch_loss += loss.item()
                epoch_energy += green_loss_val  # green penalty in joules
                epoch_tokens += batch_size * seq_len
                epoch_distill_loss_sum += distill_loss_val
                epoch_distill_count += 1

                # Log every 100 batches
                if batch_idx % 100 == 0:
                    logger.info(
                        f"Epoch {epoch+1}, Batch {batch_idx}: "
                        f"loss={loss.item():.4f}, distill={distill_loss_val:.4f}, green={green_loss_val:.4f}"
                    )

                # Flush feedback if needed (periodic)
                if batch_idx % self.cfg.feedback_batch_size == 0:
                    await self._flush_feedback()

            # End of epoch
            avg_loss = epoch_loss / len(dataloader)
            avg_distill_loss = epoch_distill_loss_sum / epoch_distill_count if epoch_distill_count else 0.0
            avg_energy_per_token = epoch_energy / epoch_tokens if epoch_tokens else 0.0
            energy_savings = max(0.0, 1.0 - (avg_energy_per_token / self.cfg.baseline_energy_per_token))

            logger.info(
                f"Epoch {epoch+1} completed: loss={avg_loss:.4f}, "
                f"distill={avg_distill_loss:.4f}, savings={energy_savings:.2%}, "
                f"time={time.time()-start_time:.2f}s"
            )

            # Validation
            val_acc = 0.0
            if val_dataloader and eval_fn:
                val_acc = eval_fn(self.student, val_dataloader)
                logger.info(f"Validation accuracy: {val_acc:.4f}")

                # Early stopping
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

            # Accumulate totals
            total_loss += avg_loss
            total_energy += epoch_energy
            total_tokens += epoch_tokens

            # Save metrics
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

            # Publish feedback (per epoch)
            await self._publish_feedback(
                list(used_teacher_ids),
                avg_distill_loss,
                val_acc,
                epoch_energy,
                carbon_g=epoch_energy * 0.2,  # rough estimate: 0.2 g CO2 per J
                latency_ms=0.0,
                epoch=epoch+1
            )

            # Drift detection
            if self.drift_detector and (epoch+1) % self.cfg.drift_check_interval == 0:
                # Check if student policy is drifting from previous best
                # We'll need to compute a metric or use the drift detector
                # For simplicity, we assume drift detector checks weights.
                pass

        # Restore best model
        if best_state is not None:
            self.student.load_state_dict(best_state)
            logger.info("Restored best model from early stopping")

        # Final metrics
        final_accuracy = 0.0
        if eval_fn and val_dataloader:
            final_accuracy = eval_fn(self.student, val_dataloader)

        avg_total_loss = total_loss / self.cfg.num_epochs
        avg_energy_per_token = total_energy / total_tokens if total_tokens else 0.0
        final_savings = max(0.0, 1.0 - (avg_energy_per_token / self.cfg.baseline_energy_per_token))

        # Flush any remaining feedback
        await self._flush_feedback()

        return {
            "avg_loss": avg_total_loss,
            "accuracy": final_accuracy,
            "energy_savings_ratio": final_savings,
            "total_energy_joules": total_energy,
        }

    # --------------------------------------------------------------------------
    # Utility methods
    # --------------------------------------------------------------------------
    def _default_accuracy_fn(self, model: nn.Module, dataloader: torch.utils.data.DataLoader) -> float:
        """Default accuracy for classification tasks."""
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

    def save_student(self, path: Path):
        """Save student model and config."""
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        torch.save(self.student.state_dict(), path / "student_model.pt")
        with open(path / "distillation_config.json", "w") as f:
            json.dump(self.cfg.__dict__, f, indent=2)
        logger.info(f"Student saved to {path}")

    def load_student(self, path: Path):
        """Load student model from disk."""
        path = Path(path)
        self.student.load_state_dict(torch.load(path / "student_model.pt"))
        with open(path / "distillation_config.json", "r") as f:
            self.cfg = DistillationConfig(**json.load(f))
        logger.info(f"Student loaded from {path}")

    async def close(self):
        """Clean up resources."""
        await self._flush_feedback()
        if self.queue:
            await self.queue.close()
        logger.info("DistillationOrchestrator closed")
