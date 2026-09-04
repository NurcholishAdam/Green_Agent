# -*- coding: utf-8 -*-
"""
Green Leaderboard (Enhanced)
Unified leaderboard with green metrics, now including advanced enhancements:
- LIMIT Graph metrics
- MODP (Multi-Objective Decision Process) scores
- RLHF human feedback
- Multi-Teacher On-Policy Distillation stats
- Bio-inspired Optimisation metrics
- Mixture-of-Experts (MoE) expert gating metrics

When enhancements are disabled (default), the class behaves exactly as the original.
"""

import json
from typing import Dict, List, Any, Optional
from pathlib import Path
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class GreenLeaderboard:
    """
    Unified green leaderboard for agent benchmarking.

    Tracks and ranks agents across frameworks with:
    - Accuracy metrics
    - Energy consumption
    - Carbon footprint
    - Sustainability index
    - MODP composite score (if enhancements enabled)
    - LIMIT Graph metrics (centrality, connectivity)
    - RLHF feedback score
    """

    def __init__(self, storage_path: str = "./leaderboard_data",
                 config: Optional[Dict[str, Any]] = None):
        """
        Initialize green leaderboard.

        Args:
            storage_path: Path for storing leaderboard data
            config: Optional configuration dictionary. May contain:
                - use_enhancements (bool)
                - modp_weights (list of 4 floats)
                - graph_metrics (dict)
                - human_feedback_score (float)
        """
        self.storage_path = Path(storage_path)
        self.storage_path.mkdir(parents=True, exist_ok=True)
        self.entries_file = self.storage_path / "leaderboard_entries.json"

        self.config = config or {}
        self.use_enhancements = self.config.get('use_enhancements', False)

        self.entries = self._load_entries()

        logger.info(f"Initialized GreenLeaderboard with {len(self.entries)} entries "
                    f"(enhancements={'on' if self.use_enhancements else 'off'})")

    def submit(
        self,
        agent_name: str,
        framework: str,
        task_suite: str,
        accuracy: float,
        energy_kwh: float,
        carbon_co2e_kg: float,
        latency_ms: float,
        sustainability_index: Optional[float] = None,
        backend: Optional[str] = None,
        rank: Optional[int] = None,
        metadata: Optional[Dict[str, Any]] = None,
        graph_metrics: Optional[Dict[str, float]] = None,
        human_feedback_score: Optional[float] = None,
        distillation_stats: Optional[Dict[str, Any]] = None,
        modp_score: Optional[float] = None,
    ) -> Dict[str, Any]:
        """
        Submit a result to the leaderboard.

        Args:
            agent_name: Name of the agent
            framework: Framework name
            task_suite: Task suite name
            accuracy: Task accuracy
            energy_kwh: Energy consumption
            carbon_co2e_kg: Carbon emissions
            latency_ms: Execution latency
            sustainability_index: Sustainability index (calculated if not provided)
            backend: Quantum backend (if applicable)
            rank: NSN rank (if applicable)
            metadata: Additional metadata
            graph_metrics: Optional LIMIT Graph metrics (if enhancements enabled)
            human_feedback_score: Optional RLHF feedback score (0-1)
            distillation_stats: Optional distillation statistics
            modp_score: Optional pre-computed MODP score; if not provided and
                        enhancements enabled, it will be calculated.

        Returns:
            Submitted entry
        """
        # Calculate sustainability index if not provided
        if sustainability_index is None:
            from ..metrics.sustainability_index import SustainabilityIndex
            si_calc = SustainabilityIndex()
            sustainability_index = si_calc.calculate(accuracy, energy_kwh, carbon_co2e_kg)

        # Compute MODP score if enhancements enabled and not provided
        if self.use_enhancements and modp_score is None:
            modp_score = self._calculate_modp_score(
                accuracy=accuracy,
                energy_kwh=energy_kwh,
                carbon_kg=carbon_co2e_kg,
                latency_ms=latency_ms,
            )

        entry = {
            "agent_name": agent_name,
            "framework": framework,
            "task_suite": task_suite,
            "metrics": {
                "accuracy": accuracy,
                "energy_kwh": energy_kwh,
                "carbon_co2e_kg": carbon_co2e_kg,
                "latency_ms": latency_ms,
                "sustainability_index": sustainability_index,
            },
            "backend": backend,
            "rank": rank,
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "metadata": metadata or {}
        }

        # Add enhanced fields if present
        if self.use_enhancements:
            entry["enhancements"] = {
                "modp_score": modp_score,
                "graph_metrics": graph_metrics or self.config.get('graph_metrics', {}),
                "human_feedback_score": human_feedback_score or self.config.get('human_feedback_score', 0.5),
                "distillation_stats": distillation_stats or {},
            }

        self.entries.append(entry)
        self._save_entries()

        logger.info(f"Submitted entry for {agent_name} ({framework}) on {task_suite}")
        return entry

    def _calculate_modp_score(self, accuracy: float, energy_kwh: float,
                              carbon_kg: float, latency_ms: float) -> float:
        """
        Calculate a multi-objective composite score (MODP) using configurable weights.
        Default weights: [0.4 (accuracy), 0.3 (energy), 0.2 (latency), 0.1 (carbon)]
        All objectives are normalized to [0,1] with higher = better.
        """
        weights = self.config.get('modp_weights', [0.4, 0.3, 0.2, 0.1])
        # Normalize weights if not already
        total = sum(weights)
        if total <= 0:
            weights = [0.25, 0.25, 0.25, 0.25]
        else:
            weights = [w / total for w in weights]

        # Normalize each metric (lower is better for energy, carbon, latency)
        acc_norm = min(accuracy, 1.0)
        energy_norm = 1.0 - min(energy_kwh / 10.0, 1.0)   # assume 10 kWh max
        carbon_norm = 1.0 - min(carbon_kg / 1.0, 1.0)     # assume 1 kg max
        latency_norm = 1.0 - min(latency_ms / 10000.0, 1.0)  # assume 10s max

        return float(weights[0] * acc_norm + weights[1] * energy_norm +
                     weights[2] * latency_norm + weights[3] * carbon_norm)

    def get_rankings(
        self,
        sort_by: str = "sustainability_index",
        framework_filter: Optional[str] = None,
        task_suite_filter: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Get leaderboard rankings.

        If enhancements are enabled and sort_by is "modp_score", entries are
        sorted by the MODP composite score.

        Args:
            sort_by: Metric to sort by (can be "accuracy", "energy_kwh",
                     "carbon_co2e_kg", "latency_ms", "sustainability_index",
                     or "modp_score" if enhancements enabled)
            framework_filter: Filter by framework (None for all)
            task_suite_filter: Filter by task suite (None for all)
            limit: Maximum number of entries to return

        Returns:
            List of ranked entries
        """
        filtered = self.entries

        if framework_filter:
            filtered = [e for e in filtered if e["framework"] == framework_filter]
        if task_suite_filter:
            filtered = [e for e in filtered if e["task_suite"] == task_suite_filter]

        # Sort entries
        if sort_by in ["accuracy", "energy_kwh", "carbon_co2e_kg", "latency_ms", "sustainability_index"]:
            reverse = sort_by in ["accuracy", "sustainability_index"]
            filtered.sort(
                key=lambda x: x["metrics"].get(sort_by, 0),
                reverse=reverse
            )
        elif sort_by == "modp_score" and self.use_enhancements:
            filtered.sort(
                key=lambda x: x.get("enhancements", {}).get("modp_score", 0),
                reverse=True
            )
        # else: no sorting (original order preserved)

        # Add rank
        for i, entry in enumerate(filtered[:limit], 1):
            entry["rank"] = i

        return filtered[:limit]

    def get_top_agents(
        self,
        n: int = 10,
        sort_by: str = "sustainability_index"
    ) -> List[Dict[str, Any]]:
        """Get top N agents."""
        return self.get_rankings(sort_by=sort_by, limit=n)

    def get_agent_history(self, agent_name: str) -> List[Dict[str, Any]]:
        """Get submission history for an agent."""
        return [e for e in self.entries if e["agent_name"] == agent_name]

    def get_framework_stats(self) -> Dict[str, Dict[str, float]]:
        """Get statistics by framework."""
        frameworks = {}
        for entry in self.entries:
            framework = entry["framework"]
            if framework not in frameworks:
                frameworks[framework] = {
                    "count": 0,
                    "total_accuracy": 0,
                    "total_energy": 0,
                    "total_carbon": 0,
                    "total_sustainability": 0,
                }
                if self.use_enhancements:
                    frameworks[framework]["total_modp"] = 0.0

            frameworks[framework]["count"] += 1
            frameworks[framework]["total_accuracy"] += entry["metrics"]["accuracy"]
            frameworks[framework]["total_energy"] += entry["metrics"]["energy_kwh"]
            frameworks[framework]["total_carbon"] += entry["metrics"]["carbon_co2e_kg"]
            frameworks[framework]["total_sustainability"] += entry["metrics"]["sustainability_index"]
            if self.use_enhancements:
                frameworks[framework]["total_modp"] += entry.get("enhancements", {}).get("modp_score", 0)

        # Calculate averages
        for framework, stats in frameworks.items():
            count = stats["count"]
            stats["avg_accuracy"] = stats["total_accuracy"] / count
            stats["avg_energy"] = stats["total_energy"] / count
            stats["avg_carbon"] = stats["total_carbon"] / count
            stats["avg_sustainability"] = stats["total_sustainability"] / count
            if self.use_enhancements:
                stats["avg_modp_score"] = stats["total_modp"] / count

        return frameworks

    def export_to_json(self, filepath: str):
        """Export leaderboard to JSON file, including enhancement data."""
        export_data = {
            "leaderboard": self.get_rankings(),
            "framework_stats": self.get_framework_stats(),
            "total_entries": len(self.entries),
            "exported_at": datetime.utcnow().isoformat() + "Z",
        }
        if self.use_enhancements:
            export_data["enhancement_settings"] = {
                "modp_weights": self.config.get('modp_weights', [0.4, 0.3, 0.2, 0.1]),
                "graph_metrics": self.config.get('graph_metrics', {}),
                "human_feedback_score": self.config.get('human_feedback_score', 0.5),
            }
        with open(filepath, 'w') as f:
            json.dump(export_data, f, indent=2)

        logger.info(f"Exported leaderboard to {filepath}")

    def _load_entries(self) -> List[Dict[str, Any]]:
        """Load entries from storage."""
        if self.entries_file.exists():
            with open(self.entries_file, 'r') as f:
                return json.load(f)
        return []

    def _save_entries(self):
        """Save entries to storage."""
        with open(self.entries_file, 'w') as f:
            json.dump(self.entries, f, indent=2)
