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

Enhanced with optional advanced modules:
- LIMIT Graph integration
- MODP (Multi-Objective Decision Process)
- RLHF (Reinforcement Learning from Human Feedback)
- Multi-Teacher On-Policy Distillation with MoE gating
- Bio-inspired Optimisation (Evolutionary)
- FlexGen execution backend integration hooks
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

# ------------------------------------------------------------------------------
# Optional Advanced Enhancements (LIMIT Graph, MODP, RLHF, Distillation, MoE,
# Bio-inspired, FlexGen)
# ------------------------------------------------------------------------------
# Try to import the enhanced modules from the `src/enhancements` directory.
# If not installed, these are gracefully skipped and ENHANCEMENTS_AVAILABLE = False.
try:
    from .src.enhancements.schemas.feedback_event import FeedbackEvent
    from .src.enhancements.schemas.node_descriptor import (
        NodeDescriptor, NodeType, CoolingType, MaintenanceStatus, RoutingStrategy
    )
    from .src.enhancements.schemas.workload_descriptor import (
        WorkloadDescriptor, TaskType, Urgency, Priority, BioMode
    )
    from .src.enhancements.zero_trust_architecture import (
        ZeroTrustArchitecture, ZeroTrustConfig
    )
    # Also import core graph-related classes (if they exist in enhancements/core)
    try:
        from .src.enhancements.core.graph_registry import GraphRegistry, GraphType
    except ImportError:
        GraphRegistry = None
        GraphType = None

    ENHANCEMENTS_AVAILABLE = True
except ImportError:
    # Advanced enhancements not installed; define dummy variables to avoid NameError
    FeedbackEvent = None
    NodeDescriptor = None
    NodeType = None
    CoolingType = None
    MaintenanceStatus = None
    RoutingStrategy = None
    WorkloadDescriptor = None
    TaskType = None
    Urgency = None
    Priority = None
    BioMode = None
    ZeroTrustArchitecture = None
    ZeroTrustConfig = None
    GraphRegistry = None
    GraphType = None
    ENHANCEMENTS_AVAILABLE = False

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

# Add enhancement classes to __all__ if available
if ENHANCEMENTS_AVAILABLE:
    __all__.extend([
        "FeedbackEvent",
        "NodeDescriptor",
        "NodeType",
        "CoolingType",
        "MaintenanceStatus",
        "RoutingStrategy",
        "WorkloadDescriptor",
        "TaskType",
        "Urgency",
        "Priority",
        "BioMode",
        "ZeroTrustArchitecture",
        "ZeroTrustConfig",
        "GraphRegistry",
        "GraphType",
    ])
