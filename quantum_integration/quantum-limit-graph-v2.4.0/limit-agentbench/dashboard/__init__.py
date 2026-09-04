# -*- coding: utf-8 -*-
"""Dashboard and visualization components (Enhanced)

This package provides dashboard and visualization modules with optional
advanced enhancements:
- LIMIT Graph metrics
- MODP (Multi-Objective Decision Process)
- RLHF (Reinforcement Learning from Human Feedback)
- Multi-Teacher On-Policy Distillation with MoE gating
- Bio-inspired Optimisation (Evolutionary)
- FlexGen execution backend

When enhancements are disabled (default), the components behave exactly
as their original counterparts. To enable enhancements, pass a
`DashboardConfig` with `use_enhancements=True` to the component constructors.
"""

from dataclasses import dataclass, field
from typing import Dict, Optional, List, Any

from .green_leaderboard import GreenLeaderboard
from .energy_visualizer import EnergyVisualizer
from .carbon_dashboard import CarbonDashboard
from .comparison_matrix import ComparisonMatrix

__all__ = [
    "GreenLeaderboard",
    "EnergyVisualizer",
    "CarbonDashboard",
    "ComparisonMatrix",
    "DashboardConfig",
    "DashboardManager",
]


# ------------------------------------------------------------------------------
# Enhanced configuration dataclass
# ------------------------------------------------------------------------------
@dataclass
class DashboardConfig:
    """Configuration for dashboard components with enhancement flags."""
    use_enhancements: bool = False
    # LIMIT Graph metrics (defaults; can be overridden per component)
    graph_metrics: Dict[str, float] = field(default_factory=lambda: {
        "centrality": 0.5,
        "connectivity": 0.5,
        "density": 0.4
    })
    # MODP weights: [accuracy, energy, latency, carbon]
    modp_weights: Optional[List[float]] = None
    # RLHF
    human_feedback_score: float = 0.5
    # Distillation / MoE / Evolutionary flags
    use_distillation: bool = True
    distillation_lr: float = 0.01
    gating_lr: float = 0.005
    use_evolutionary: bool = False
    population_size: int = 10
    mutation_rate: float = 0.1
    crossover_rate: float = 0.7
    elitism: int = 2
    # FlexGen
    flexgen_enabled: bool = False


class DashboardManager:
    """
    Convenience wrapper that initializes all dashboard components with a
    shared configuration. This is useful when multiple visualizers need to
    be used together and share enhancement settings.
    """

    def __init__(self, config: Optional[DashboardConfig] = None):
        self.config = config or DashboardConfig()
        # Pass config to each component; they will ignore it if not needed.
        self.leaderboard = GreenLeaderboard(config=self.config.__dict__)
        self.energy_visualizer = EnergyVisualizer(config=self.config.__dict__)
        self.carbon_dashboard = CarbonDashboard(config=self.config.__dict__)
        self.comparison_matrix = ComparisonMatrix(config=self.config.__dict__)

    def get_all_components(self) -> Dict[str, Any]:
        """Return a dict of all initialized components."""
        return {
            "leaderboard": self.leaderboard,
            "energy_visualizer": self.energy_visualizer,
            "carbon_dashboard": self.carbon_dashboard,
            "comparison_matrix": self.comparison_matrix,
        }
