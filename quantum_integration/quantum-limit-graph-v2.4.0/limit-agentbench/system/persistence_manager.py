import os
import json
import torch
import pickle
from typing import Any, Dict, Optional, List
from dataclasses import dataclass, field


class PersistenceManager:
    """
    Enhanced persistence manager with atomic saves for JSON, models, and
    advanced component state (LIMIT Graph, MODP, RLHF, distillation, MoE, evolutionary).
    """

    # ------------------------------------------------------------------
    # Original static methods (unchanged)
    # ------------------------------------------------------------------
    @staticmethod
    def atomic_json_save(data: Any, path: str):
        temp_path = path + ".tmp"
        with open(temp_path, "w") as f:
            json.dump(data, f, indent=2)
        os.replace(temp_path, path)

    @staticmethod
    def atomic_model_save(model: torch.nn.Module, path: str):
        temp_path = path + ".tmp"
        torch.save(model.state_dict(), temp_path)
        os.replace(temp_path, path)

    @staticmethod
    def load_model(model: torch.nn.Module, path: str) -> None:
        if os.path.exists(path):
            model.load_state_dict(torch.load(path))

    # ------------------------------------------------------------------
    # Enhanced methods for advanced components
    # ------------------------------------------------------------------
    @staticmethod
    def atomic_pickle_save(obj: Any, path: str):
        temp_path = path + ".tmp"
        with open(temp_path, "wb") as f:
            pickle.dump(obj, f)
        os.replace(temp_path, path)

    @staticmethod
    def load_pickle(path: str) -> Any:
        if os.path.exists(path):
            with open(path, "rb") as f:
                return pickle.load(f)
        return None

    @staticmethod
    def save_enhanced_state(state: Dict[str, Any], path: str):
        """
        Persist a dictionary containing any or all of:
          - 'modp_weights'         : list/array of objective weights
          - 'graph_metrics'        : dict with centrality, connectivity, etc.
          - 'rlhf_feedback_history': list of human feedback scores
          - 'distillation_student' : dict with student weights/bias, replay buffer, etc.
          - 'moe_gating'           : dict with gate weights, bias, etc.
          - 'evolutionary_pop'     : dict with population, best weights, etc.
        Uses JSON if possible, otherwise pickle for complex objects.
        """
        # Try JSON first (works for pure Python types)
        try:
            json.dumps(state)  # test serializability
            PersistenceManager.atomic_json_save(state, path)
        except (TypeError, ValueError):
            # Fallback to pickle for NumPy arrays, etc.
            PersistenceManager.atomic_pickle_save(state, path)

    @staticmethod
    def load_enhanced_state(path: str) -> Dict[str, Any]:
        """
        Load enhanced state from JSON or pickle.
        """
        if not os.path.exists(path):
            return {}
        # Try JSON first
        try:
            with open(path, "r") as f:
                return json.load(f)
        except (json.JSONDecodeError, UnicodeDecodeError):
            # Not JSON, try pickle
            return PersistenceManager.load_pickle(path) or {}

    @staticmethod
    def save_full_checkpoint(
        model: torch.nn.Module,
        enhanced_state: Dict[str, Any],
        model_path: str,
        state_path: str,
    ):
        """
        Convenience method to save both model weights and enhanced state atomically.
        """
        PersistenceManager.atomic_model_save(model, model_path)
        PersistenceManager.save_enhanced_state(enhanced_state, state_path)

    @staticmethod
    def load_full_checkpoint(
        model: torch.nn.Module,
        model_path: str,
        state_path: str,
    ) -> Dict[str, Any]:
        """
        Load model weights and enhanced state. Returns the enhanced state dict.
        """
        PersistenceManager.load_model(model, model_path)
        return PersistenceManager.load_enhanced_state(state_path)


# ------------------------------------------------------------------------------
# Example usage (commented out)
# ------------------------------------------------------------------------------
# if __name__ == "__main__":
#     # Enhanced state example
#     state = {
#         "modp_weights": [0.4, 0.3, 0.2, 0.1],
#         "graph_metrics": {"centrality": 0.7, "connectivity": 0.6},
#         "rlhf_feedback_history": [0.5, 0.6, 0.7],
#         "distillation_student": {"weights": [[0.1, 0.2], [0.3, 0.4]], "bias": [0.0, 0.0]},
#         "moe_gating": {"gate_weights": [[0.5, 0.5]], "gate_bias": [0.1, 0.1]},
#         "evolutionary_pop": {"population": [[0.2, 0.8], [0.6, 0.4]], "best": [0.3, 0.7]},
#     }
#     PersistenceManager.save_enhanced_state(state, "enhanced_state.json")
#     loaded = PersistenceManager.load_enhanced_state("enhanced_state.json")
#     print(loaded)
