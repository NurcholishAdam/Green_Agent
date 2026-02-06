# -*- coding: utf-8 -*-
"""
Enhanced Metrics Collection Module

Ensures all resource metrics (latency, energy, carbon, memory, tool calls) 
are logged consistently with real-time monitoring hooks.
"""

import time
import psutil
import json
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
from datetime import datetime


@dataclass
class MetricsSnapshot:
    """Single point-in-time metrics snapshot."""
    timestamp: float
    step: int
    latency_ms: float
    energy_wh: float
    carbon_kg: float
    memory_mb: float
    tool_calls: int
    cpu_percent: float
    gpu_utilization: Optional[float] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class MetricsCollector:
    """
    Comprehensive metrics collector with real-time hooks.
    
    Responsibilities:
    - Collect all resource metrics consistently
    - Provide real-time access to current metrics
    - Support mid-execution metric queries
    - Log metrics for post-execution analysis
    """
    
    def __init__(self, grid_intensity_g_kwh: float = 385.0, pue_factor: float = 1.2):
        self.grid_intensity = grid_intensity_g_kwh
        self.pue_factor = pue_factor
        self.metrics_history: List[MetricsSnapshot] = []
        self.current_step = 0
        self.start_time = time.time()
        self.tool_call_count = 0
        self.process = psutil.Process()
        
    def start_step(self):
        """Mark the beginning of a new execution step."""
        self.current_step += 1
        self.step_start_time = time.time()
        
    def record_tool_call(self):
        """Increment tool call counter."""
        self.tool_call_count += 1
        
    def collect_snapshot(self) -> MetricsSnapshot:
        """
        Collect current metrics snapshot.
        
        Returns:
            MetricsSnapshot with all current metrics
        """
        current_time = time.time()
        elapsed = current_time - self.step_start_time if hasattr(self, 'step_start_time') else 0
        
        # Memory metrics
        memory_info = self.process.memory_info()
        memory_mb = memory_info.rss / (1024 * 1024)
        
        # CPU metrics
        cpu_percent = self.process.cpu_percent(interval=0.1)
        
        # Energy estimation (simplified model)
        # Based on CPU usage and elapsed time
        energy_wh = (cpu_percent / 100.0) * 0.05 * (elapsed / 3600.0)
        
        # Carbon calculation
        carbon_kg = (energy_wh * self.grid_intensity * self.pue_factor) / 1000000.0
        
        snapshot = MetricsSnapshot(
            timestamp=current_time,
            step=self.current_step,
            latency_ms=elapsed * 1000,
            energy_wh=energy_wh,
            carbon_kg=carbon_kg,
            memory_mb=memory_mb,
            tool_calls=self.tool_call_count,
            cpu_percent=cpu_percent
        )
        
        self.metrics_history.append(snapshot)
        return snapshot
    
    def get_current_metrics(self) -> Dict[str, Any]:
        """
        Get current metrics for mid-execution access.
        
        Returns:
            Dictionary of current metrics
        """
        snapshot = self.collect_snapshot()
        return snapshot.to_dict()
    
    def get_cumulative_metrics(self) -> Dict[str, Any]:
        """
        Get cumulative metrics across all steps.
        
        Returns:
            Dictionary of aggregated metrics
        """
        if not self.metrics_history:
            return {}
        
        total_latency = sum(m.latency_ms for m in self.metrics_history)
        total_energy = sum(m.energy_wh for m in self.metrics_history)
        total_carbon = sum(m.carbon_kg for m in self.metrics_history)
        avg_memory = sum(m.memory_mb for m in self.metrics_history) / len(self.metrics_history)
        max_memory = max(m.memory_mb for m in self.metrics_history)
        
        return {
            "total_steps": self.current_step,
            "total_latency_ms": total_latency,
            "total_energy_wh": total_energy,
            "total_carbon_kg": total_carbon,
            "avg_memory_mb": avg_memory,
            "max_memory_mb": max_memory,
            "total_tool_calls": self.tool_call_count,
            "elapsed_time_s": time.time() - self.start_time
        }
    
    def export_history(self, filepath: str):
        """Export metrics history to JSON file."""
        data = {
            "snapshots": [m.to_dict() for m in self.metrics_history],
            "cumulative": self.get_cumulative_metrics()
        }
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)
    
    def get_metrics_for_reflection(self) -> Dict[str, Any]:
        """
        Get formatted metrics for reflection checkpoints.
        
        Returns:
            Metrics formatted for reflection analysis
        """
        cumulative = self.get_cumulative_metrics()
        recent = self.metrics_history[-5:] if len(self.metrics_history) >= 5 else self.metrics_history
        
        return {
            "cumulative": cumulative,
            "recent_trend": {
                "avg_latency_ms": sum(m.latency_ms for m in recent) / len(recent) if recent else 0,
                "avg_energy_wh": sum(m.energy_wh for m in recent) / len(recent) if recent else 0,
                "avg_carbon_kg": sum(m.carbon_kg for m in recent) / len(recent) if recent else 0,
            },
            "step_count": self.current_step
        }
