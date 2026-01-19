# -*- coding: utf-8 -*-
"""
LIMIT-AgentBench: Green Agent Benchmarking Platform
Version: 2.4.2
Date: January 19, 2026

A comprehensive benchmarking platform for AI agents with:
- AgentBench protocol compatibility
- Energy consumption and carbon footprint tracking
- Multi-framework support (LangChain, AutoGen, CrewAI, etc.)
- Unified green leaderboard
- Integration with existing LIMIT-GRAPH infrastructure
"""

__version__ = "2.4.2"
__author__ = "AI Research Agent Team"
__license__ = "Apache-2.0"

from .core.agentbench_adapter import AgentBenchAdapter
from .core.green_metrics import GreenMetricsTracker
from .core.agent_evaluator import AgentEvaluator
from .core.benchmark_harness import BenchmarkHarness

from .metrics.energy_tracker import EnergyTracker
from .metrics.carbon_calculator import CarbonCalculator
from .metrics.efficiency_scorer import EfficiencyScorer
from .metrics.sustainability_index import SustainabilityIndex

from .dashboard.green_leaderboard import GreenLeaderboard
from .dashboard.energy_visualizer import EnergyVisualizer
from .dashboard.carbon_dashboard import CarbonDashboard
from .dashboard.comparison_matrix import ComparisonMatrix

__all__ = [
    # Core
    "AgentBenchAdapter",
    "GreenMetricsTracker",
    "AgentEvaluator",
    "BenchmarkHarness",
    
    # Metrics
    "EnergyTracker",
    "CarbonCalculator",
    "EfficiencyScorer",
    "SustainabilityIndex",
    
    # Dashboard
    "GreenLeaderboard",
    "EnergyVisualizer",
    "CarbonDashboard",
    "ComparisonMatrix",
]
