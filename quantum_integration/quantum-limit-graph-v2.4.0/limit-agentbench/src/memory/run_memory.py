# -*- coding: utf-8 -*-
"""
Run Memory Module

Implements memory system for sustained reflection across multiple runs.
Tracks agent performance history and generates meta-policies.
"""

import json
from typing import Dict, List, Any, Optional
from datetime import datetime
import os


class RunMemory:
    """
    Memory system for tracking agent performance across runs.
    
    Responsibilities:
    - Store complete run histories
    - Track performance trends over time
    - Generate meta-policies from historical data
    - Support long-context reasoning
    """
    
    def __init__(self, memory_file: str = "run_memory.json"):
        """
        Initialize run memory.
        
        Args:
            memory_file: Path to persistent memory file
        """
        self.memory_file = memory_file
        self.runs: List[Dict[str, Any]] = []
        self.meta_policies: List[Dict[str, Any]] = []
        self.load_memory()
    
    def load_memory(self):
        """Load memory from persistent storage."""
        if os.path.exists(self.memory_file):
            try:
                with open(self.memory_file, 'r') as f:
                    data = json.load(f)
                    self.runs = data.get("runs", [])
                    self.meta_policies = data.get("meta_policies", [])
            except Exception as e:
                print(f"Warning: Could not load memory: {e}")
                self.runs = []
                self.meta_policies = []
    
    def save_memory(self):
        """Save memory to persistent storage."""
        data = {
            "runs": self.runs,
            "meta_policies": self.meta_policies,
            "last_updated": datetime.now().isoformat()
        }
        with open(self.memory_file, 'w') as f:
            json.dump(data, f, indent=2)
    
    def add_run(self, run_data: Dict[str, Any]):
        """
        Add completed run to memory.
        
        Args:
            run_data: Complete run data including metrics and reflections
        """
        run_entry = {
            "timestamp": datetime.now().isoformat(),
            "run_id": len(self.runs) + 1,
            **run_data
        }
        self.runs.append(run_entry)
        self.save_memory()
    
    def get_recent_runs(self, n: int = 10) -> List[Dict[str, Any]]:
        """
        Get N most recent runs.
        
        Args:
            n: Number of recent runs to retrieve
            
        Returns:
            List of recent run data
        """
        return self.runs[-n:] if len(self.runs) >= n else self.runs
    
    def analyze_performance_trends(self) -> Dict[str, Any]:
        """
        Analyze performance trends across runs.
        
        Returns:
            Trend analysis including improvements and degradations
        """
        if len(self.runs) < 2:
            return {"status": "insufficient_data"}
        
        # Extract key metrics over time
        energy_trend = [
            r.get("cumulative", {}).get("total_energy_wh", 0) 
            for r in self.runs
        ]
        latency_trend = [
            r.get("cumulative", {}).get("total_latency_ms", 0) 
            for r in self.runs
        ]
        carbon_trend = [
            r.get("cumulative", {}).get("total_carbon_kg", 0) 
            for r in self.runs
        ]
        
        return {
            "total_runs": len(self.runs),
            "energy_trend": self._analyze_trend(energy_trend),
            "latency_trend": self._analyze_trend(latency_trend),
            "carbon_trend": self._analyze_trend(carbon_trend),
            "overall_direction": self._determine_overall_direction(energy_trend, latency_trend, carbon_trend)
        }
    
    def _analyze_trend(self, values: List[float]) -> Dict[str, Any]:
        """Analyze trend in a series of values."""
        if len(values) < 2:
            return {"direction": "unknown"}
        
        # Simple trend analysis
        first_half = values[:len(values)//2]
        second_half = values[len(values)//2:]
        
        avg_first = sum(first_half) / len(first_half) if first_half else 0
        avg_second = sum(second_half) / len(second_half) if second_half else 0
        
        if avg_second < avg_first * 0.9:
            direction = "improving"
        elif avg_second > avg_first * 1.1:
            direction = "degrading"
        else:
            direction = "stable"
        
        return {
            "direction": direction,
            "first_half_avg": avg_first,
            "second_half_avg": avg_second,
            "change_pct": ((avg_second - avg_first) / avg_first * 100) if avg_first > 0 else 0
        }
    
    def _determine_overall_direction(
        self,
        energy_trend: List[float],
        latency_trend: List[float],
        carbon_trend: List[float]
    ) -> str:
        """Determine overall performance direction."""
        energy_analysis = self._analyze_trend(energy_trend)
        latency_analysis = self._analyze_trend(latency_trend)
        carbon_analysis = self._analyze_trend(carbon_trend)
        
        improving_count = sum([
            1 for a in [energy_analysis, latency_analysis, carbon_analysis]
            if a.get("direction") == "improving"
        ])
        
        degrading_count = sum([
            1 for a in [energy_analysis, latency_analysis, carbon_analysis]
            if a.get("direction") == "degrading"
        ])
        
        if improving_count >= 2:
            return "improving"
        elif degrading_count >= 2:
            return "degrading"
        else:
            return "stable"
    
    def generate_meta_policy(self) -> Optional[Dict[str, Any]]:
        """
        Generate meta-policy from historical performance.
        
        Returns:
            Meta-policy recommendations or None
        """
        if len(self.runs) < 5:
            return None
        
        trends = self.analyze_performance_trends()
        
        meta_policy = {
            "generated_at": datetime.now().isoformat(),
            "based_on_runs": len(self.runs),
            "recommendations": []
        }
        
        # Energy recommendations
        if trends.get("energy_trend", {}).get("direction") == "degrading":
            meta_policy["recommendations"].append({
                "metric": "energy",
                "action": "tighten_energy_budget",
                "reason": "Energy usage is increasing over time"
            })
        
        # Latency recommendations
        if trends.get("latency_trend", {}).get("direction") == "degrading":
            meta_policy["recommendations"].append({
                "metric": "latency",
                "action": "optimize_execution_speed",
                "reason": "Latency is increasing over time"
            })
        
        # Carbon recommendations
        if trends.get("carbon_trend", {}).get("direction") == "degrading":
            meta_policy["recommendations"].append({
                "metric": "carbon",
                "action": "reduce_carbon_footprint",
                "reason": "Carbon emissions are increasing over time"
            })
        
        # Overall strategy
        overall = trends.get("overall_direction", "stable")
        if overall == "improving":
            meta_policy["recommendations"].append({
                "metric": "overall",
                "action": "continue_current_strategy",
                "reason": "Overall performance is improving"
            })
        elif overall == "degrading":
            meta_policy["recommendations"].append({
                "metric": "overall",
                "action": "review_and_adjust_strategy",
                "reason": "Overall performance is degrading"
            })
        
        if meta_policy["recommendations"]:
            self.meta_policies.append(meta_policy)
            self.save_memory()
            return meta_policy
        
        return None
    
    def get_historical_summary(self) -> Dict[str, Any]:
        """Get summary of all historical runs."""
        if not self.runs:
            return {"total_runs": 0}
        
        total_energy = sum(r.get("cumulative", {}).get("total_energy_wh", 0) for r in self.runs)
        total_carbon = sum(r.get("cumulative", {}).get("total_carbon_kg", 0) for r in self.runs)
        total_latency = sum(r.get("cumulative", {}).get("total_latency_ms", 0) for r in self.runs)
        
        return {
            "total_runs": len(self.runs),
            "total_energy_wh": total_energy,
            "total_carbon_kg": total_carbon,
            "total_latency_ms": total_latency,
            "avg_energy_per_run": total_energy / len(self.runs),
            "avg_carbon_per_run": total_carbon / len(self.runs),
            "avg_latency_per_run": total_latency / len(self.runs),
            "meta_policies_generated": len(self.meta_policies)
        }
