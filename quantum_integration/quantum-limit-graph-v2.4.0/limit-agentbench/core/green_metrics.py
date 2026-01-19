# -*- coding: utf-8 -*-
"""
Green Metrics Tracker
Monitors energy consumption and carbon footprint during agent execution
"""

import time
import psutil
import platform
from typing import Dict, Optional
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class GreenMetricsTracker:
    """
    Tracks energy consumption and carbon footprint during agent execution.
    
    Provides real-time monitoring of:
    - Energy consumption (kWh)
    - Carbon emissions (CO2e kg)
    - Power usage (Watts)
    - Efficiency metrics
    """
    
    # Carbon intensity by grid region (kg CO2e per kWh)
    CARBON_INTENSITY = {
        "US-CA": 0.2,      # California (clean grid)
        "US-TX": 0.4,      # Texas (mixed grid)
        "US-WV": 0.7,      # West Virginia (coal-heavy)
        "EU-FR": 0.05,     # France (nuclear)
        "EU-DE": 0.35,     # Germany (mixed)
        "CN": 0.6,         # China (coal-heavy)
        "IN": 0.7,         # India (coal-heavy)
        "GLOBAL": 0.475    # Global average
    }
    
    # Hardware power profiles (Watts)
    HARDWARE_POWER = {
        "nvidia_a100": 400,
        "nvidia_v100": 300,
        "nvidia_t4": 70,
        "nvidia_rtx_3090": 350,
        "nvidia_rtx_4090": 450,
        "amd_mi250": 500,
        "google_tpu_v4": 200,
        "cpu_intel_xeon": 150,
        "cpu_amd_epyc": 180,
        "default": 100
    }
    
    def __init__(
        self,
        grid_region: str = "GLOBAL",
        hardware_profile: str = "default",
        track_energy: bool = True,
        track_carbon: bool = True
    ):
        """
        Initialize green metrics tracker.
        
        Args:
            grid_region: Grid region for carbon intensity
            hardware_profile: Hardware profile for power estimation
            track_energy: Whether to track energy consumption
            track_carbon: Whether to track carbon emissions
        """
        self.grid_region = grid_region
        self.hardware_profile = hardware_profile
        self.track_energy = track_energy
        self.track_carbon = track_carbon
        
        self.carbon_intensity = self.CARBON_INTENSITY.get(grid_region, 0.475)
        self.hardware_power = self.HARDWARE_POWER.get(hardware_profile, 100)
        
        self.start_time = None
        self.end_time = None
        self.start_cpu_percent = None
        self.start_memory_percent = None
        
        logger.info(f"Initialized GreenMetricsTracker: region={grid_region}, "
                   f"hardware={hardware_profile}")
    
    def start(self):
        """Start tracking metrics."""
        self.start_time = time.time()
        self.start_cpu_percent = psutil.cpu_percent(interval=0.1)
        self.start_memory_percent = psutil.virtual_memory().percent
        logger.debug("Started green metrics tracking")
    
    def stop(self):
        """Stop tracking metrics."""
        self.end_time = time.time()
        logger.debug("Stopped green metrics tracking")
    
    def get_metrics(self) -> Dict[str, float]:
        """
        Get tracked metrics.
        
        Returns:
            Dictionary with energy and carbon metrics
        """
        if not self.start_time or not self.end_time:
            logger.warning("Metrics requested before tracking completed")
            return {}
        
        duration_seconds = self.end_time - self.start_time
        duration_hours = duration_seconds / 3600
        
        # Get CPU usage
        end_cpu_percent = psutil.cpu_percent(interval=0.1)
        avg_cpu_percent = (self.start_cpu_percent + end_cpu_percent) / 2
        
        # Estimate power usage based on CPU utilization
        power_watts = self.hardware_power * (avg_cpu_percent / 100)
        
        # Calculate energy consumption
        energy_kwh = (power_watts * duration_hours) / 1000
        
        # Calculate carbon emissions
        carbon_co2e_kg = energy_kwh * self.carbon_intensity
        
        # Calculate efficiency score (inverse of energy per second)
        efficiency_score = 1.0 / (energy_kwh / duration_seconds) if energy_kwh > 0 else 0.0
        
        metrics = {
            "duration_seconds": duration_seconds,
            "power_watts": power_watts,
            "cpu_percent": avg_cpu_percent
        }
        
        if self.track_energy:
            metrics["energy_kwh"] = energy_kwh
            metrics["efficiency_score"] = efficiency_score
        
        if self.track_carbon:
            metrics["carbon_co2e_kg"] = carbon_co2e_kg
            metrics["carbon_intensity"] = self.carbon_intensity
        
        logger.info(f"Green metrics: energy={energy_kwh:.6f} kWh, "
                   f"carbon={carbon_co2e_kg:.6f} kg CO2e")
        
        return metrics
    
    def get_sustainability_index(
        self,
        accuracy: float,
        energy_kwh: float,
        carbon_co2e_kg: float
    ) -> float:
        """
        Calculate sustainability index.
        
        Formula: (accuracy × efficiency) / carbon
        Higher is better.
        
        Args:
            accuracy: Task accuracy (0-1)
            energy_kwh: Energy consumption
            carbon_co2e_kg: Carbon emissions
            
        Returns:
            Sustainability index score
        """
        if carbon_co2e_kg == 0:
            return 0.0
        
        efficiency = 1.0 / energy_kwh if energy_kwh > 0 else 0.0
        sustainability_index = (accuracy * efficiency) / carbon_co2e_kg
        
        return sustainability_index
    
    @classmethod
    def estimate_carbon_savings(
        cls,
        baseline_energy_kwh: float,
        optimized_energy_kwh: float,
        grid_region: str = "GLOBAL"
    ) -> Dict[str, float]:
        """
        Estimate carbon savings from optimization.
        
        Args:
            baseline_energy_kwh: Baseline energy consumption
            optimized_energy_kwh: Optimized energy consumption
            grid_region: Grid region for carbon intensity
            
        Returns:
            Dictionary with savings metrics
        """
        carbon_intensity = cls.CARBON_INTENSITY.get(grid_region, 0.475)
        
        energy_saved_kwh = baseline_energy_kwh - optimized_energy_kwh
        carbon_saved_kg = energy_saved_kwh * carbon_intensity
        reduction_percent = (energy_saved_kwh / baseline_energy_kwh * 100 
                            if baseline_energy_kwh > 0 else 0.0)
        
        return {
            "energy_saved_kwh": energy_saved_kwh,
            "carbon_saved_kg": carbon_saved_kg,
            "reduction_percent": reduction_percent,
            "grid_region": grid_region
        }
    
    def __enter__(self):
        """Context manager entry."""
        self.start()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.stop()
