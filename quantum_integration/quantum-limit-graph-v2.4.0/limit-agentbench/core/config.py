from dataclasses import dataclass, field
from typing import Optional, Dict, List, Any


@dataclass
class PPOConfig:
    """PPO hyperparameters."""
    lr: float = 3e-4
    clip_epsilon: float = 0.2
    entropy_coef: float = 0.01
    value_coef: float = 0.5
    max_grad_norm: float = 0.5
    # Enhanced fields for advanced techniques
    use_moe_gating: bool = False          # Mixture-of-Experts for policy/value heads
    use_rlhf: bool = False                # RLHF integration
    use_evolutionary: bool = False        # Evolutionary exploration
    use_distillation: bool = False        # Multi-teacher distillation
    distillation_lr: float = 0.01
    gating_lr: float = 0.005
    modp_weights: Optional[List[float]] = None  # e.g. [carbon, energy, latency]


@dataclass
class SystemConfig:
    """System-level configuration."""
    energy_budget: float = 100.0
    reward_clip_min: float = -10.0
    reward_clip_max: float = 10.0
    coordinator_sync_interval: int = 10
    save_interval: int = 5
    # Enhanced fields
    graph_metrics: Dict[str, float] = field(default_factory=lambda: {
        "centrality": 0.5,
        "connectivity": 0.5,
        "density": 0.4
    })
    human_feedback_score: float = 0.5     # RLHF input
    modp_weights: Optional[List[float]] = None   # if None, default [0.4, 0.3, 0.3]
    use_limit_graph: bool = True          # enable LIMIT Graph integration
    use_modp: bool = True                 # Multi-Objective Decision Process
    use_bio_inspired: bool = False        # Evolutionary optimisation
    use_moe_expert: bool = False          # MoE expert gating
    use_multi_teacher_distillation: bool = False

    # Bio-inspired parameters
    population_size: int = 20
    mutation_rate: float = 0.1
    crossover_rate: float = 0.7
    elitism: int = 2
