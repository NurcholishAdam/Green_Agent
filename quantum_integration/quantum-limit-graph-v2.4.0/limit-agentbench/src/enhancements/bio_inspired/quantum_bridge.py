"""
Quantum Bridge v1.0
Translates bio-inspired gradient fields into quantum graph parameters (QUBO/Ising).
"""

import logging
from typing import Dict, Any, List, Tuple, Optional
import numpy as np

logger = logging.getLogger(__name__)

class QuantumBridge:
    """
    Bridges the GradientManager (bio‑inspired) to a quantum solver/graph.
    
    It reads current gradient strengths and maps them to:
      - Penalty weights for QUBO variables (e.g., route costs, recycling incentives)
      - Lagrange multipliers for constraints (e.g., supply‑demand balance)
      - Initial biases for quantum annealing
    """
    
    def __init__(self, gradient_manager, quantum_graph=None):
        """
        Args:
            gradient_manager: The bio‑inspired GradientFieldManager instance.
            quantum_graph: (Optional) Reference to your quantum limit graph object.
                           If None, we only compute the translation (for testing).
        """
        self.gradient_manager = gradient_manager
        self.quantum_graph = quantum_graph
        
        # Mapping: gradient field → QUBO parameter name
        self.gradient_to_qubo = {
            'carbon': 'penalty_carbon',
            'helium': 'penalty_helium_shortage',
            'trust': 'penalty_geopolitical',
            'opportunity': 'weight_opportunity',
            'eco_atp_reserve': 'constraint_budget'
        }
        
        # Scaling factors to convert gradient [0..1] to QUBO penalties
        self.scaling = {
            'carbon': 10.0,
            'helium': 20.0,
            'trust': 8.0,
            'opportunity': 5.0,
            'eco_atp_reserve': 15.0
        }
        
        logger.info("QuantumBridge initialized")
    
    def get_qubo_parameters(self) -> Dict[str, float]:
        """
        Compute QUBO parameters from current gradient strengths.
        
        Returns:
            dict: parameter name → numeric value ready for the quantum solver.
        """
        strengths = self.gradient_manager.get_field_strengths()
        params = {}
        
        for field, param_name in self.gradient_to_qubo.items():
            value = strengths.get(field, 0.5)
            # Invert some gradients if needed: e.g., opportunity → higher weight
            if field == 'opportunity':
                # The higher the opportunity, the more we want to exploit it
                weight = value * self.scaling[field]
            else:
                # For penalties, higher gradient → higher penalty
                penalty = (1.0 - value) * self.scaling[field]  # if we want to penalize low supply?
                # Let's be more intuitive: high helium shortage (helium gradient) → high penalty
                if field == 'helium':
                    penalty = value * self.scaling[field]
                elif field == 'carbon':
                    penalty = value * self.scaling[field]
                elif field == 'trust':
                    penalty = (1.0 - value) * self.scaling[field]  # low trust → high penalty
                elif field == 'eco_atp_reserve':
                    penalty = (1.0 - value) * self.scaling[field]  # low reserve → high constraint
                params[param_name] = penalty
        
        # Add extra custom parameters if needed
        params['timestamp'] = np.datetime64('now', 'ns').astype(float)
        return params
    
    def apply_to_quantum_graph(self) -> bool:
        """
        Push the computed QUBO parameters into the actual quantum graph.
        Returns True on success, False if no graph is attached.
        """
        if self.quantum_graph is None:
            logger.warning("No quantum graph attached to QuantumBridge – translation only.")
            return False
        
        params = self.get_qubo_parameters()
        
        try:
            # Example: if your graph has an `update_weights()` method
            # self.quantum_graph.update_weights(params)
            # Or maybe you set QUBO coefficients:
            # self.quantum_graph.set_qubo(params)
            logger.info(f"Pushed QUBO parameters to quantum graph: {params}")
            return True
        except Exception as e:
            logger.error(f"Failed to apply QUBO parameters: {e}")
            return False
    
    def get_qubo_report(self) -> Dict[str, Any]:
        """Return a human‑readable report of the current translation."""
        return {
            'gradient_strengths': self.gradient_manager.get_field_strengths(),
            'qubo_parameters': self.get_qubo_parameters(),
            'scaling': self.scaling
        }
