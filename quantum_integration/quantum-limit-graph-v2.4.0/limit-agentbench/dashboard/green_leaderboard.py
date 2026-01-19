# -*- coding: utf-8 -*-
"""
Green Leaderboard
Unified leaderboard with green metrics
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
    """
    
    def __init__(self, storage_path: str = "./leaderboard_data"):
        """
        Initialize green leaderboard.
        
        Args:
            storage_path: Path for storing leaderboard data
        """
        self.storage_path = Path(storage_path)
        self.storage_path.mkdir(parents=True, exist_ok=True)
        self.entries_file = self.storage_path / "leaderboard_entries.json"
        
        self.entries = self._load_entries()
        
        logger.info(f"Initialized GreenLeaderboard with {len(self.entries)} entries")
    
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
        metadata: Optional[Dict[str, Any]] = None
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
            
        Returns:
            Submitted entry
        """
        # Calculate sustainability index if not provided
        if sustainability_index is None:
            from ..metrics.sustainability_index import SustainabilityIndex
            si_calc = SustainabilityIndex()
            sustainability_index = si_calc.calculate(accuracy, energy_kwh, carbon_co2e_kg)
        
        entry = {
            "agent_name": agent_name,
            "framework": framework,
            "task_suite": task_suite,
            "metrics": {
                "accuracy": accuracy,
                "energy_kwh": energy_kwh,
                "carbon_co2e_kg": carbon_co2e_kg,
                "latency_ms": latency_ms,
                "sustainability_index": sustainability_index
            },
            "backend": backend,
            "rank": rank,
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "metadata": metadata or {}
        }
        
        self.entries.append(entry)
        self._save_entries()
        
        logger.info(f"Submitted entry for {agent_name} ({framework}) on {task_suite}")
        return entry
    
    def get_rankings(
        self,
        sort_by: str = "sustainability_index",
        framework_filter: Optional[str] = None,
        task_suite_filter: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Get leaderboard rankings.
        
        Args:
            sort_by: Metric to sort by
            framework_filter: Filter by framework (None for all)
            task_suite_filter: Filter by task suite (None for all)
            limit: Maximum number of entries to return
            
        Returns:
            List of ranked entries
        """
        # Filter entries
        filtered = self.entries
        
        if framework_filter:
            filtered = [e for e in filtered if e["framework"] == framework_filter]
        
        if task_suite_filter:
            filtered = [e for e in filtered if e["task_suite"] == task_suite_filter]
        
        # Sort entries
        if sort_by in ["accuracy", "energy_kwh", "carbon_co2e_kg", "latency_ms", "sustainability_index"]:
            reverse = sort_by in ["accuracy", "sustainability_index"]  # Higher is better
            filtered.sort(
                key=lambda x: x["metrics"].get(sort_by, 0),
                reverse=reverse
            )
        
        # Add rank
        for i, entry in enumerate(filtered[:limit], 1):
            entry["rank"] = i
        
        return filtered[:limit]
    
    def get_top_agents(
        self,
        n: int = 10,
        sort_by: str = "sustainability_index"
    ) -> List[Dict[str, Any]]:
        """
        Get top N agents.
        
        Args:
            n: Number of top agents to return
            sort_by: Metric to sort by
            
        Returns:
            List of top agents
        """
        return self.get_rankings(sort_by=sort_by, limit=n)
    
    def get_agent_history(self, agent_name: str) -> List[Dict[str, Any]]:
        """
        Get submission history for an agent.
        
        Args:
            agent_name: Name of the agent
            
        Returns:
            List of submissions for the agent
        """
        return [e for e in self.entries if e["agent_name"] == agent_name]
    
    def get_framework_stats(self) -> Dict[str, Dict[str, float]]:
        """
        Get statistics by framework.
        
        Returns:
            Dictionary with framework statistics
        """
        frameworks = {}
        
        for entry in self.entries:
            framework = entry["framework"]
            if framework not in frameworks:
                frameworks[framework] = {
                    "count": 0,
                    "total_accuracy": 0,
                    "total_energy": 0,
                    "total_carbon": 0,
                    "total_sustainability": 0
                }
            
            frameworks[framework]["count"] += 1
            frameworks[framework]["total_accuracy"] += entry["metrics"]["accuracy"]
            frameworks[framework]["total_energy"] += entry["metrics"]["energy_kwh"]
            frameworks[framework]["total_carbon"] += entry["metrics"]["carbon_co2e_kg"]
            frameworks[framework]["total_sustainability"] += entry["metrics"]["sustainability_index"]
        
        # Calculate averages
        for framework, stats in frameworks.items():
            count = stats["count"]
            stats["avg_accuracy"] = stats["total_accuracy"] / count
            stats["avg_energy"] = stats["total_energy"] / count
            stats["avg_carbon"] = stats["total_carbon"] / count
            stats["avg_sustainability"] = stats["total_sustainability"] / count
        
        return frameworks
    
    def export_to_json(self, filepath: str):
        """Export leaderboard to JSON file."""
        with open(filepath, 'w') as f:
            json.dump({
                "leaderboard": self.get_rankings(),
                "framework_stats": self.get_framework_stats(),
                "total_entries": len(self.entries),
                "exported_at": datetime.utcnow().isoformat() + "Z"
            }, f, indent=2)
        
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
