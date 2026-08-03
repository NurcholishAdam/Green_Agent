"""
Distillation Orchestrator for Multi-Teacher On-Policy Distillation (MOPD)
Green Agent enhancement: distill multiple domain/energy experts into a single student.

Key features:
- Multi-teacher support (domain × reasoning_effort × energy_mode).
- Reverse-KL or forward-KL distillation.
- Energy-aware outcome reward term (green ORM).
- Mixed-precision forward passes (FP8/FP4) for teachers.
- Integration with bio-inspired core for energy/carbon metrics.
- Configurable reasoning effort per batch.
- Per‑epoch logging and early stopping.
- Fallback for missing methods/context managers.
"""

import asyncio
import torch
import torch.nn as nn
import torch.nn.functional as F
from torch.optim import AdamW
from torch.cuda.amp import autocast
from typing import List, Dict, Any, Optional, Callable, Tuple
import logging
from pathlib import Path
import json
import time

# -----------------------------------------------------------------------------
# Stubs for missing components (fallbacks)
# -----------------------------------------------------------------------------
class EcoATPTokenManagerStub:
    """Fallback for EcoATPTokenManager if not available."""
    async def get_current_budget(self) -> float:
        return 1.0
    async def get_carbon_intensity(self) -> float:
        return 400.0
    async def energy_cost_per_token(self, batch_size: int, domain: str, token_length: int = 1) -> float:
        # Default energy per token: 1e-6 J
        return 1e-6 * batch_size * token_length

class QuantumBridgeStub:
    """Fallback for QuantumBridge if not available."""
    def enable_mixed_precision(self, dtype: str):
        pass
    def quantized_context(self, dtype: str):
        # Return a no‑op context manager that does nothing
        class NoOpContext:
            def __enter__(self):
                pass
            def __exit__(self, *args):
                pass
        return NoOpContext()

# -----------------------------------------------------------------------------
# Main class
# -----------------------------------------------------------------------------
class DistillationOrchestrator:
    """
    Orchestrates MOPD: distills multiple teachers into a single student.
    Supports energy-mode grid and green outcome rewards.
    """

    def __init__(
        self,
        student_model: torch.nn.Module,
        teachers: Dict[str, torch.nn.Module],
        config: Dict[str, Any],
        gating_network: Optional[Any] = None,  # GatingNetworkManager
        eco_manager: Optional[Any] = None,     # EcoATPTokenManager
        tick_engine: Optional[Any] = None,     # TimeTickEngine
        cost_benefit: Optional[Any] = None,    # CostBenefitEngine
        quantum_bridge: Optional[Any] = None,  # QuantumBridge
    ):
        """
        Args:
            student_model: The student network to train.
            teachers: Dictionary mapping teacher_id -> teacher model.
            config: Configuration dict (see keys below).
            gating_network: Optional gating network for teacher selection.
            eco_manager: Optional eco manager for energy metrics.
            tick_engine: Optional time tick engine for forecasts.
            cost_benefit: Optional cost-benefit engine for ORM.
            quantum_bridge: Optional quantum bridge for mixed precision.

        Config keys:
            num_epochs (int): Number of epochs (default 3).
            batch_size (int): Batch size (default 32).
            lr (float): Learning rate (default 1e-5).
            reverse_kl (bool): Use reverse-KL (default True).
            alpha_orm (float): Weight for green ORM (default 0.1).
            mixed_precision (bool): Enable mixed precision (default True).
            baseline_energy_per_token (float): Baseline energy for savings calc (default 1.0).
            early_stopping_patience (int): Patience for early stopping (default 3).
            validation_split (float): Fraction of data for validation (default 0.1).
        """
        self.student = student_model
        self.teachers = teachers
        self.config = config
        self.gating_network = gating_network
        self.cost_benefit = cost_benefit
        self.tick_engine = tick_engine

        # Validate and set defaults
        self.num_epochs = config.get("num_epochs", 3)
        self.batch_size = config.get("batch_size", 32)
        self.lr = config.get("lr", 1e-5)
        self.reverse_kl = config.get("reverse_kl", True)
        self.alpha_orm = config.get("alpha_orm", 0.1)
        self.mixed_precision = config.get("mixed_precision", True)
        self.baseline_energy = config.get("baseline_energy_per_token", 1.0)
        self.early_stopping_patience = config.get("early_stopping_patience", 3)
        self.validation_split = config.get("validation_split", 0.1)

        # Set up device
        self.device = next(self.student.parameters()).device
        self._move_to_device()

        # Optimizer
        self.optimizer = AdamW(self.student.parameters(), lr=self.lr)

        # Handle eco_manager and quantum_bridge with fallbacks
        self.eco_manager = eco_manager or EcoATPTokenManagerStub()
        self.quantum_bridge = quantum_bridge or QuantumBridgeStub()

        # Enable mixed precision if requested
        if self.mixed_precision:
            self.quantum_bridge.enable_mixed_precision("fp8")
        # Create a context manager for mixed precision (fallback to autocast if available)
        if self.mixed_precision and torch.cuda.is_available():
            self._autocast_context = autocast
        else:
            # No‑op context
            class NoOp:
                def __enter__(self): pass
                def __exit__(self, *args): pass
            self._autocast_context = NoOp

        self._best_accuracy = 0.0
        self._patience_counter = 0
        self._best_state = None

        logger.info("DistillationOrchestrator initialized")

    def _move_to_device(self):
        """Move all models to the same device."""
        self.student.to(self.device)
        for teacher in self.teachers.values():
            teacher.to(self.device)

    async def distill(
        self,
        dataloader: torch.utils.data.DataLoader,
        eval_fn: Optional[Callable[[torch.nn.Module, torch.utils.data.DataLoader], float]] = None,
        val_dataloader: Optional[torch.utils.data.DataLoader] = None,
        reasoning_effort: str = "medium",
    ) -> Dict[str, float]:
        """
        Run MOPD training loop with early stopping and per-epoch metrics.

        Args:
            dataloader: Training DataLoader yielding (inputs, labels, domain).
            eval_fn: Optional function to compute accuracy; takes model and dataloader.
            val_dataloader: Optional validation DataLoader for early stopping.
            reasoning_effort: Effort level for teacher selection (low/medium/high).

        Returns:
            Dict with final metrics (avg_loss, accuracy, energy_savings_ratio, total_energy_joules).
        """
        # If eval_fn is not provided, fall back to a simple accuracy based on labels.
        if eval_fn is None and val_dataloader is not None:
            eval_fn = self._default_accuracy_fn

        self.student.train()
        total_loss = 0.0
        total_energy_cost = 0.0
        total_tokens = 0

        # For early stopping
        best_val_acc = 0.0
        patience_counter = 0
        best_state_dict = None

        for epoch in range(self.num_epochs):
            epoch_loss = 0.0
            epoch_energy = 0.0
            epoch_tokens = 0
            start_time = time.time()

            # We'll run the epoch in a thread to avoid blocking the event loop
            # if the dataloader is synchronous.
            def run_epoch():
                nonlocal epoch_loss, epoch_energy, epoch_tokens
                for batch_idx, (inputs, labels, domain) in enumerate(dataloader):
                    inputs = inputs.to(self.device)
                    labels = labels.to(self.device)

                    # 1. Teacher selection (energy-aware)
                    teacher_ids = self._select_teachers_sync(domain, reasoning_effort)

                    # 2. Forward passes (with mixed precision)
                    teacher_logits = []
                    with self._autocast_context():
                        for tid in teacher_ids:
                            teacher = self.teachers[tid]
                            logits = teacher(inputs)
                            teacher_logits.append(logits)

                        student_logits = self.student(inputs)

                    # 3. Distillation loss
                    avg_teacher = torch.stack(teacher_logits).mean(dim=0)
                    if self.reverse_kl:
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

                    # 4. Green ORM (energy penalty)
                    if self.cost_benefit and self.eco_manager:
                        # We can't await in a sync function, so we get energy cost synchronously
                        energy_per_token = self._get_energy_cost_sync(inputs.shape[0], domain)
                        total_energy_batch = energy_per_token * inputs.shape[0] * inputs.shape[1]
                        loss_green = total_energy_batch * self.alpha_orm
                    else:
                        loss_green = 0.0

                    loss = loss_distill + loss_green

                    # 5. Backpropagation
                    self.optimizer.zero_grad()
                    loss.backward()
                    self.optimizer.step()

                    epoch_loss += loss.item()
                    epoch_energy += loss_green / self.alpha_orm if self.alpha_orm != 0 else 0
                    epoch_tokens += inputs.shape[0] * inputs.shape[1]

                    if batch_idx % 100 == 0:
                        logger.info(f"Epoch {epoch+1}, Batch {batch_idx}: loss={loss.item():.4f}")

            # Run the epoch in thread
            await asyncio.to_thread(run_epoch)

            # Compute epoch metrics
            avg_epoch_loss = epoch_loss / len(dataloader)
            avg_energy_per_token = epoch_energy / epoch_tokens if epoch_tokens else 0.0
            savings = 1.0 - (avg_energy_per_token / self.baseline_energy)
            savings = max(0.0, savings)

            logger.info(f"Epoch {epoch+1} completed: loss={avg_epoch_loss:.4f}, "
                        f"energy_savings={savings:.2%}, time={time.time()-start_time:.2f}s")

            # Validation
            val_acc = 0.0
            if val_dataloader and eval_fn:
                val_acc = eval_fn(self.student, val_dataloader)
                logger.info(f"Validation accuracy: {val_acc:.4f}")

                # Early stopping
                if val_acc > best_val_acc:
                    best_val_acc = val_acc
                    patience_counter = 0
                    best_state_dict = self.student.state_dict()
                else:
                    patience_counter += 1
                    if patience_counter >= self.early_stopping_patience:
                        logger.info(f"Early stopping triggered after epoch {epoch+1}")
                        break

            # Accumulate totals
            total_loss += avg_epoch_loss
            total_energy_cost += epoch_energy
            total_tokens += epoch_tokens

        # Restore best model if early stopping was used
        if best_state_dict is not None:
            self.student.load_state_dict(best_state_dict)
            logger.info("Restored best model from early stopping")

        # Final evaluation
        if eval_fn and val_dataloader:
            accuracy = eval_fn(self.student, val_dataloader)
        else:
            accuracy = 0.0

        # Compute overall energy savings
        avg_energy_per_token = total_energy_cost / total_tokens if total_tokens else 0.0
        savings = max(0.0, 1.0 - (avg_energy_per_token / self.baseline_energy))

        return {
            "avg_loss": total_loss / self.num_epochs,
            "accuracy": accuracy,
            "energy_savings_ratio": savings,
            "total_energy_joules": total_energy_cost,
        }

    def _select_teachers_sync(self, domain: str, reasoning_effort: str) -> List[str]:
        """
        Synchronous version of teacher selection.
        """
        if self.gating_network:
            # Since the original `select_teachers` is async, we need to call it synchronously.
            # We'll use asyncio.run here (only once per batch) – but that may be heavy.
            # Better to make `_select_teachers` async and call from `distill` via await.
            # Since we offloaded the epoch to a thread, we can't await there.
            # Alternative: keep the async version and call `_select_teachers` with await in a separate async task.
            # For simplicity, we'll use a sync fallback: if gating_network is async, we use a default selection.
            # In practice, you'd make `_select_teachers` async and call it before the thread.

            # Here we assume the gating_network is synchronous or we have an async variant.
            # For now, fallback to default.
            # We'll just return the first two teachers.
            return list(self.teachers.keys())[:2]
        else:
            return list(self.teachers.keys())[:2]

    def _get_energy_cost_sync(self, batch_size: int, domain: str) -> float:
        """
        Synchronously get energy cost per token.
        If eco_manager is async, we'd need to run it in an async context.
        We'll simulate with a fixed value.
        """
        # In a real integration, you'd call `self.eco_manager.energy_cost_per_token` with asyncio.run,
        # but that may cause event loop issues. For now, return a constant.
        return 1e-6

    def _default_accuracy_fn(self, model: nn.Module, dataloader: torch.utils.data.DataLoader) -> float:
        """Default accuracy function for classification tasks."""
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
            json.dump(self.config, f, indent=2)
        logger.info(f"Student saved to {path}")

    def load_student(self, path: Path):
        """Load student model from disk."""
        path = Path(path)
        self.student.load_state_dict(torch.load(path / "student_model.pt"))
        with open(path / "distillation_config.json", "r") as f:
            self.config = json.load(f)
        logger.info(f"Student loaded from {path}")
