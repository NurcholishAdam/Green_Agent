# -*- coding: utf-8 -*-
"""
Chaos Engineering Module

Provides deterministic chaos and failure injection for testing robustness.
"""

import random
from typing import Dict, Any


def inject_energy_spike(metrics: Dict[str, Any], probability: float = 0.1) -> Dict[str, Any]:
    """
    Inject random energy spike for chaos testing.
    
    Args:
        metrics: Current metrics
        probability: Probability of spike (0.0-1.0)
        
    Returns:
        Modified metrics
    """
    if random.random() < probability:
        metrics["energy"] = metrics.get("energy", 0) * 1.5
        metrics["chaos_event"] = "energy_spike"
    
    return metrics


def inject_latency_delay(metrics: Dict[str, Any], probability: float = 0.1) -> Dict[str, Any]:
    """
    Inject random latency delay for chaos testing.
    
    Args:
        metrics: Current metrics
        probability: Probability of delay (0.0-1.0)
        
    Returns:
        Modified metrics
    """
    if random.random() < probability:
        metrics["latency"] = metrics.get("latency", 0) * 2.0
        metrics["chaos_event"] = "latency_delay"
    
    return metrics
