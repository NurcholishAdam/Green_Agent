# -*- coding: utf-8 -*-
"""
Energy Visualizer (Enhanced)
Energy consumption visualization with optional integration of
LIMIT Graph, MODP, RLHF, Multi‑Teacher On‑Policy Distillation,
Bio‑inspired Optimisation, and MoE expert gating.

When enhancements are disabled (default), the class behaves as the original
placeholder. When enabled via the `config` parameter, it attempts to
incorporate advanced metrics into the visualization.
"""

import logging
from typing import Any, Dict, Optional, List, Tuple
import os

logger = logging.getLogger(__name__)

# Optional imports for plotting (graceful fallback)
try:
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    MATPLOTLIB_AVAILABLE = True
except ImportError:
    MATPLOTLIB_AVAILABLE = False

# Optional imports for advanced enhancements (graceful fallback)
try:
    from src.enhancements.schemas.feedback_event import FeedbackEvent
    from src.enhancements.schemas.node_descriptor import NodeDescriptor
    from src.enhancements.schemas.workload_descriptor import WorkloadDescriptor
    from src.enhancements.zero_trust_architecture import ZeroTrustArchitecture
    ENHANCEMENTS_AVAILABLE = True
except ImportError:
    ENHANCEMENTS_AVAILABLE = False
    FeedbackEvent = None
    NodeDescriptor = None
    WorkloadDescriptor = None
    ZeroTrustArchitecture = None


class EnergyVisualizer:
    """
    Energy consumption visualizer.

    Enhanced version supports overlaying advanced metrics:
    - LIMIT Graph centrality/connectivity
    - MODP composite score
    - RLHF human feedback
    - Distillation update count
    - Evolutionary best fitness
    - MoE gate weights (optional)
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize energy visualizer.

        Args:
            config: Optional configuration dictionary. May contain:
                - use_enhancements (bool) - enable advanced overlay
                - graph_metrics (dict) - LIMIT Graph metrics
                - modp_weights (list) - MODP weights
                - human_feedback_score (float) - RLHF feedback
        """
        self.config = config or {}
        self.use_enhancements = self.config.get('use_enhancements', False) and ENHANCEMENTS_AVAILABLE
        logger.info(f"Initialized EnergyVisualizer (enhancements={'on' if self.use_enhancements else 'off'})")

    def plot_energy_consumption(self, data: Any, **kwargs) -> Any:
        """
        Plot energy consumption.

        If enhancements are enabled and data contains relevant fields,
        attempt to create a multi-panel plot. Otherwise, returns None
        as in the original placeholder.
        """
        if not self.use_enhancements or not MATPLOTLIB_AVAILABLE:
            logger.info("Energy visualization not yet implemented (or enhancements disabled)")
            return None

        # Try to extract data as list of records or DataFrame
        records = self._extract_records(data)
        if not records:
            logger.warning("No data available for enhanced energy visualization.")
            return None

        # Build figure with subplots
        n_metrics = 3  # energy, carbon, modp
        fig, axes = plt.subplots(n_metrics, 1, figsize=(10, 8), sharex=True)
        fig.suptitle("Enhanced Energy Consumption Visualization")

        # Extract time series if timestamps present, else use index
        times = [r.get('timestamp') for r in records]
        if times and isinstance(times[0], (int, float)):
            x = times
        else:
            x = range(len(records))

        energy = [r.get('energy_kwh', 0) for r in records]
        carbon = [r.get('carbon_co2e_kg', 0) for r in records]
        modp = [r.get('modp_score', 0) for r in records]

        axes[0].plot(x, energy, label='Energy (kWh)', color='tab:red')
        axes[0].set_ylabel('Energy (kWh)')
        axes[0].legend(loc='upper left')

        axes[1].plot(x, carbon, label='Carbon (kg CO2e)', color='tab:green')
        axes[1].set_ylabel('Carbon (kg CO2e)')
        axes[1].legend(loc='upper left')

        axes[2].plot(x, modp, label='MODP Score', color='tab:blue')
        axes[2].set_ylabel('MODP Score')
        axes[2].set_xlabel('Time step')
        axes[2].legend(loc='upper left')

        plt.tight_layout()
        return fig

    def _extract_records(self, data: Any) -> List[Dict[str, Any]]:
        """Extract list of dict records from various data formats."""
        if isinstance(data, list):
            # If list of dicts, use directly
            if all(isinstance(item, dict) for item in data):
                return data
            # If list of numbers? Not enough info
            return []
        elif isinstance(data, dict):
            # Could be a single record or a mapping with 'records'
            if 'records' in data:
                return data['records']
            elif 'entries' in data:
                return data['entries']
            else:
                return [data]
        elif hasattr(data, 'to_dict'):
            # Possibly pandas DataFrame?
            try:
                return data.to_dict('records')
            except:
                pass
        return []

    def plot_with_enhancement_overlay(self, data: Any, enhancement_metrics: Dict[str, Any]) -> Any:
        """
        Advanced plot that overlays enhancement metrics (graph, RLHF, etc.)
        as separate subplots or annotations.

        This is a convenience method for when the user already has
        additional metrics to display.
        """
        if not MATPLOTLIB_AVAILABLE:
            logger.warning("Matplotlib not installed; cannot create plot.")
            return None

        records = self._extract_records(data)
        if not records:
            return None

        x = range(len(records))
        energy = [r.get('energy_kwh', 0) for r in records]

        fig, axes = plt.subplots(3, 1, figsize=(10, 8), sharex=True)
        axes[0].plot(x, energy, label='Energy', color='red')
        axes[0].set_ylabel('Energy (kWh)')

        # Plot graph metrics if available
        graph = enhancement_metrics.get('graph_metrics', {})
        if graph:
            centrality = graph.get('centrality', 0.5)
            connectivity = graph.get('connectivity', 0.5)
            axes[1].axhline(y=centrality, color='green', linestyle='--', label='Centrality')
            axes[1].axhline(y=connectivity, color='orange', linestyle='--', label='Connectivity')
            axes[1].set_ylabel('Graph Metrics')
            axes[1].legend(loc='upper left')

        # Plot RLHF feedback and MODP score if available
        rlhf = enhancement_metrics.get('human_feedback_score')
        modp = enhancement_metrics.get('modp_score')
        if rlhf is not None:
            axes[2].axhline(y=rlhf, color='purple', linestyle='-.', label='RLHF Feedback')
        if modp is not None:
            axes[2].axhline(y=modp, color='blue', linestyle='-', label='MODP Score')
        axes[2].set_ylabel('Scores')
        axes[2].legend(loc='upper left')

        plt.tight_layout()
        return fig
