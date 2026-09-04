# -*- coding: utf-8 -*-
"""
Comparison Matrix (Enhanced)
Cross-framework comparison visualization with optional integration of
LIMIT Graph, MODP, RLHF, Multi‑Teacher On‑Policy Distillation,
Bio‑inspired Optimisation, and MoE expert gating.

When enhancements are disabled (default), the class behaves as the original
placeholder. When enabled via `config`, it attempts to create comparative
visualizations that include advanced metrics.
"""

import logging
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# Optional imports for plotting (graceful fallback)
try:
    import matplotlib.pyplot as plt
    import numpy as np
    MATPLOTLIB_AVAILABLE = True
except ImportError:
    MATPLOTLIB_AVAILABLE = False
    np = None

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


class ComparisonMatrix:
    """
    Cross-framework comparison matrix.

    Enhanced version can display:
    - Standard comparison (accuracy, energy, carbon)
    - MODP composite scores across frameworks
    - LIMIT Graph metrics (centrality, connectivity)
    - RLHF human feedback scores
    - Distillation update rates
    - Evolutionary best fitness
    - MoE gate weights variance
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize comparison matrix.

        Args:
            config: Optional configuration dictionary. May contain:
                - use_enhancements (bool) - enable advanced overlay
                - graph_metrics (dict) - LIMIT Graph metrics
                - modp_weights (list) - MODP weights
                - human_feedback_score (float) - RLHF feedback
        """
        self.config = config or {}
        self.use_enhancements = self.config.get('use_enhancements', False) and ENHANCEMENTS_AVAILABLE
        logger.info(f"Initialized ComparisonMatrix (enhancements={'on' if self.use_enhancements else 'off'})")

    def plot_comparison(self, data: Any, **kwargs) -> Any:
        """
        Plot comparison matrix.

        If enhancements are enabled and data contains relevant fields,
        attempt to create a multi-panel figure. Otherwise, returns None
        as in the original placeholder.
        """
        if not self.use_enhancements or not MATPLOTLIB_AVAILABLE:
            logger.info("Comparison visualization not yet implemented (or enhancements disabled)")
            return None

        # Extract records (list of dicts expected, one per agent/framework)
        records = self._extract_records(data)
        if not records:
            logger.warning("No data available for enhanced comparison matrix.")
            return None

        # Determine which enhanced fields are available
        available_metrics = []
        if any('modp_score' in r for r in records):
            available_metrics.append('modp_score')
        if any('graph_centrality' in r or 'graph_metrics' in r for r in records):
            available_metrics.append('graph_metrics')
        if any('human_feedback_score' in r or 'rlhf_feedback' in r for r in records):
            available_metrics.append('rlhf_feedback')
        if any('distillation_update_count' in r for r in records):
            available_metrics.append('distillation_update_count')
        if any('evolutionary_best_fitness' in r for r in records):
            available_metrics.append('evolutionary_best_fitness')
        if any('moe_gate_stddev' in r for r in records):
            available_metrics.append('moe_gate_stddev')

        # Create subplots: one row per metric (or group)
        n_metrics = max(1, len(available_metrics))
        fig, axes = plt.subplots(n_metrics, 1, figsize=(10, 3 * n_metrics), sharex=True)
        if n_metrics == 1:
            axes = [axes]
        fig.suptitle("Enhanced Cross-Framework Comparison Matrix")

        frameworks = [r.get('framework', r.get('agent_id', 'unknown')) for r in records]
        x = np.arange(len(records)) if np else range(len(records))

        # For each enhanced metric, create a bar chart
        for i, metric in enumerate(available_metrics):
            ax = axes[i]
            if metric == 'modp_score':
                vals = [r.get('modp_score', 0) for r in records]
                ax.bar(x, vals, color='tab:blue')
                ax.set_ylabel('MODP Score')
            elif metric == 'graph_metrics':
                # Extract centrality if present
                centrality = []
                for r in records:
                    gm = r.get('graph_metrics', {})
                    centrality.append(gm.get('centrality', 0.5))
                ax.bar(x, centrality, color='tab:green')
                ax.set_ylabel('Graph Centrality')
            elif metric == 'rlhf_feedback':
                vals = [r.get('human_feedback_score', r.get('rlhf_feedback', 0.5)) for r in records]
                ax.bar(x, vals, color='tab:purple')
                ax.set_ylabel('RLHF Feedback')
            elif metric == 'distillation_update_count':
                vals = [r.get('distillation_update_count', 0) for r in records]
                ax.bar(x, vals, color='tab:orange')
                ax.set_ylabel('Distillation Updates')
            elif metric == 'evolutionary_best_fitness':
                vals = [r.get('evolutionary_best_fitness', 0) for r in records]
                ax.bar(x, vals, color='tab:red')
                ax.set_ylabel('Evolutionary Fitness')
            elif metric == 'moe_gate_stddev':
                vals = [r.get('moe_gate_stddev', 0) for r in records]
                ax.bar(x, vals, color='tab:brown')
                ax.set_ylabel('MoE Gate Stddev')
            ax.set_xticks(x)
            ax.set_xticklabels(frameworks, rotation=45, ha='right')

        plt.tight_layout()
        return fig

    def _extract_records(self, data: Any) -> List[Dict[str, Any]]:
        """Extract list of dict records from various data formats."""
        if isinstance(data, list):
            if all(isinstance(item, dict) for item in data):
                return data
            return []
        elif isinstance(data, dict):
            if 'records' in data:
                return data['records']
            elif 'entries' in data:
                return data['entries']
            elif 'agents' in data:
                return data['agents']
            else:
                # maybe a single record
                return [data]
        elif hasattr(data, 'to_dict'):
            try:
                return data.to_dict('records')
            except:
                pass
        return []

    def plot_correlation_heatmap(self, data: Any) -> Any:
        """
        Optional: Create a correlation heatmap of metrics including enhanced ones.
        Requires numpy and matplotlib.
        """
        if not MATPLOTLIB_AVAILABLE or not np:
            logger.warning("Matplotlib and NumPy required for heatmap.")
            return None
        records = self._extract_records(data)
        if len(records) < 2:
            return None
        # Extract all numeric fields
        keys = set()
        for r in records:
            keys.update(r.keys())
        # Keep only numeric fields
        numeric_keys = []
        for k in keys:
            try:
                float(records[0][k])
                numeric_keys.append(k)
            except (ValueError, TypeError):
                continue
        if not numeric_keys:
            return None
        matrix = np.zeros((len(numeric_keys), len(records)))
        for j, k in enumerate(numeric_keys):
            for i, r in enumerate(records):
                try:
                    matrix[j, i] = float(r[k])
                except:
                    matrix[j, i] = np.nan
        corr = np.corrcoef(matrix)
        fig, ax = plt.subplots()
        im = ax.imshow(corr, cmap='coolwarm', vmin=-1, vmax=1)
        ax.set_xticks(range(len(records)))
        ax.set_yticks(range(len(numeric_keys)))
        ax.set_xticklabels([r.get('framework', r.get('agent_id', str(i))) for i, r in enumerate(records)], rotation=45)
        ax.set_yticklabels(numeric_keys)
        plt.colorbar(im)
        return fig
