# -*- coding: utf-8 -*-
"""
Energy Tracker
Detailed energy consumption monitoring
"""

import time
import psutil
from typing import Dict, List, Optional
import logging

logger = logging.getLogger(__name__)


class EnergyTracker:
    """
    Detailed energy consumption tracker.
    
    Tracks:
    - Power usage over time
    - Energy per operation
    - Peak power consumption
    - Energy efficiency trends
    """
    
    def __init__(self, hardware_power_watts: float = 100, sampling_interval: float = 0.1):
        """
        Initialize energy tracker.
        
        Args:
            hardware_power_watts: Maximum hardware power consumption
            sampling_interval: Sampling interval in seconds
        """
        self.hardware_power_watts = hardware_power_watts
        self.sampling_interval = sampling_interval
        self.samples: List[Dict[str, float]] = []
        self.is_tracking = False
        
    def start(self):
        """Start energy tracking."""
        self.samples = []
        self.is_tracking = True
        self.start_time = time.time()
        logger.debug("Started energy tracking")
    
    def sample(self):
        """Take a power consumption sample."""
        if not self.is_tracking:
            return
        
        cpu_percent = psutil.cpu_percent(interval=self.sampling_interval)
        power_watts = self.hardware_power_watts * (cpu_percent / 100)
        
        self.samples.append({
            "timestamp": time.time(),
            "cpu_percent": cpu_percent,
            "power_watts": power_watts
        })
    
    def stop(self):
        """Stop energy tracking."""
        self.is_tracking = False
        self.end_time = time.time()
        logger.debug("Stopped energy tracking")
    
    def get_metrics(self) -> Dict[str, float]:
        """
        Get detailed energy metrics.
        
        Returns:
            Dictionary with energy metrics
        """
        if not self.samples:
            return {}
        
        duration_seconds = self.end_time - self.start_time
        duration_hours = duration_seconds / 3600
        
        # Calculate average power
        avg_power_watts = sum(s["power_watts"] for s in self.samples) / len(self.samples)
        
        # Calculate peak power
        peak_power_watts = max(s["power_watts"] for s in self.samples)
        
        # Calculate total energy
        energy_kwh = (avg_power_watts * duration_hours) / 1000
        
        # Calculate energy per sample
        energy_per_sample = energy_kwh / len(self.samples) if self.samples else 0
        
        return {
            "duration_seconds": duration_seconds,
            "num_samples": len(self.samples),
            "avg_power_watts": avg_power_watts,
            "peak_power_watts": peak_power_watts,
            "energy_kwh": energy_kwh,
            "energy_per_sample": energy_per_sample
        }
    
    def get_power_timeline(self) -> List[Dict[str, float]]:
        """Get power consumption timeline."""
        return self.samples
