"""
Distillation Orchestrator for Multi-Teacher On-Policy Distillation (MOPD)
Green Agent enhancement: distill multiple domain/energy experts into a single student.

Key features:
- Multi-teacher support (domain × reasoning_effort × energy_mode).
- Reverse-KL or forward-KL distillation.
- Energy-aware outcome reward term (green ORM).
- Mixed-precision forward passes (FP8/FP4) for teachers.
- Integration with bio-inspired core for energy/carbon metrics.
"""

import asyncio
import torch
import torch.nn as nn
import torch.nn.functional as F
from torch.optim import AdamW
from typing import List, Dict, Any, Optional, Callable
import logging
from pathlib import Path
import json

from ..moe_expert_system.expert_router import ExpertRouter
from ..moe_expert_system.gating_network import GatingNetworkManager
from ..bio_inspired.eco_atp_currency import EcoATPTokenManager
from ..bio_inspired.time_tick_engine import TimeTickEngine
from ..enhancements.cost_benefit_engine import CostBenefitEngine
from ..enhancements.quantum_bridge import QuantumBridge  # for mixed precision

logger = logging.getLogger(__name__)

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
        gating_network: Optional[GatingNetworkManager] = None,
        eco_manager: Optional[EcoATPTokenManager] = None,
        tick_engine: Optional[TimeTickEngine] = None,
        cost_benefit: Optional[CostBenefitEngine] = None,
        quantum_bridge: Optional[QuantumBridge] = None,
    ):
        self.student = student_model
        self.teachers = teachers  # dict: teacher_id -> model
        self.config = config
        self.gating_network = gating_network
        self.eco_manager = eco_manager
        self.tick_engine = tick_engine
        self.cost_benefit = cost_benefit
        self.quantum_bridge = quantum_bridge

        # Hyperparameters
        self.num_epochs = config.get("num_epochs", 3)
        self.batch_size = config.get("batch_size", 32)
        self.lr = config.get("lr", 1e-5)
        self.reverse_kl = config.get("reverse_kl", True)  # use reverse-KL
        self.alpha_orm = config.get("alpha_orm", 0.1)  # weight for green ORM
        self.mixed_precision = config.get("mixed_precision", True)

        self.optimizer = AdamW(self.student.parameters(), lr=self.lr)
        self.device = next(self.student.parameters()).device

        # Teacher forward pass mode (FP8/FP4)
        if self.mixed_precision and self.quantum_bridge:
            self.quantum_bridge.enable_mixed_precision("fp8")

    async def distill(
        self,
        dataloader: torch.utils.data.DataLoader,
        eval_fn: Optional[Callable] = None,
    ) -> Dict[str, float]:
        """
        Run MOPD training loop.
        Returns metrics (loss, accuracy, energy_savings).
        """
        self.student.train()
        total_loss = 0.0
        total_energy_cost = 0.0
        total_tokens = 0

        for epoch in range(self.num_epochs):
            for batch_idx, (inputs, labels, domain) in enumerate(dataloader):
                inputs = inputs.to(self.device)
                labels = labels.to(self.device)

                # 1. Teacher selection via gating network (energy-aware)
                teacher_ids = await self._select_teachers(inputs, domain)

                # 2. Forward pass through teachers (with mixed precision)
                teacher_logits = []
                for tid in teacher_ids:
                    teacher = self.teachers[tid]
                    if self.mixed_precision and self.quantum_bridge:
                        with self.quantum_bridge.quantized_context("fp8"):
                            logits = teacher(inputs)
                    else:
                        logits = teacher(inputs)
                    teacher_logits.append(logits)

                # 3. Student forward pass
                student_logits = self.student(inputs)

                # 4. Compute distillation loss (reverse-KL or forward-KL)
                if self.reverse_kl:
                    # Reverse-KL: D_KL(student || teacher_avg)
                    avg_teacher = torch.stack(teacher_logits).mean(dim=0)
                    loss_distill = F.kl_div(
                        F.log_softmax(student_logits, dim=-1),
                        F.softmax(avg_teacher, dim=-1),
                        reduction="batchmean",
                    )
                else:
                    # Forward-KL: D_KL(teacher_avg || student)
                    avg_teacher = torch.stack(teacher_logits).mean(dim=0)
                    loss_distill = F.kl_div(
                        F.log_softmax(avg_teacher, dim=-1),
                        F.softmax(student_logits, dim=-1),
                        reduction="batchmean",
                    )

                # 5. Green outcome reward term (ORM)
                if self.cost_benefit and self.eco_manager:
                    energy_per_token = await self.eco_manager.energy_cost_per_token(
                        inputs.shape[0],
                        domain,
                    )
                    # Estimate total energy for this batch
                    total_energy = energy_per_token * inputs.shape[0] * inputs.shape[1]
                    # Green reward: negative energy cost (we want to minimize)
                    green_reward = -total_energy * self.alpha_orm
                    loss_green = -green_reward  # minimize negative reward
                else:
                    loss_green = 0.0

                loss = loss_distill + loss_green

                # 6. Backpropagation
                self.optimizer.zero_grad()
                loss.backward()
                self.optimizer.step()

                total_loss += loss.item()
                total_energy_cost += total_energy if self.eco_manager else 0
                total_tokens += inputs.shape[0] * inputs.shape[1]

                if batch_idx % 100 == 0:
                    logger.info(f"Epoch {epoch+1}, Batch {batch_idx}: loss={loss.item():.4f}")

        # Final evaluation
        if eval_fn:
            accuracy = eval_fn(self.student)
        else:
            accuracy = 0.0

        # Compute average energy savings
        avg_energy_per_token = total_energy_cost / total_tokens if total_tokens else 0.0
        savings = 1.0 - (avg_energy_per_token / self.config.get("baseline_energy_per_token", 1.0))

        return {
            "avg_loss": total_loss / len(dataloader),
            "accuracy": accuracy,
            "energy_savings_ratio": max(0.0, savings),
            "total_energy_joules": total_energy_cost,
        }

    async def _select_teachers(
        self,
        inputs: torch.Tensor,
        domain: str,
    ) -> List[str]:
        """
        Energy-aware teacher selection.
        If gating network available, use it; else fallback to random.
        """
        if self.gating_network:
            # Get current energy budget and carbon intensity
            if self.eco_manager:
                energy_budget = await self.eco_manager.get_current_budget()
                carbon_intensity = await self.eco_manager.get_carbon_intensity()
            else:
                energy_budget = 1.0
                carbon_intensity = 400

            # Determine energy mode based on budget
            if energy_budget > 0.7 and carbon_intensity < 300:
                energy_mode = "performance"
            elif energy_budget > 0.3 or carbon_intensity < 450:
                energy_mode = "balanced"
            else:
                energy_mode = "eco"

            # Query gating network for teacher IDs
            teacher_ids = await self.gating_network.select_teachers(
                domain=domain,
                reasoning_effort="high",  # could be based on task difficulty
                energy_mode=energy_mode,
                num_teachers=2,  # use top-2 teachers
            )
            return teacher_ids
        else:
            # Fallback: random selection
            return list(self.teachers.keys())[:2]

    async def save_student(self, path: Path):
        torch.save(self.student.state_dict(), path / "student_model.pt")
        with open(path / "distillation_config.json", "w") as f:
            json.dump(self.config, f)

    async def load_student(self, path: Path):
        self.student.load_state_dict(torch.load(path / "student_model.pt"))
