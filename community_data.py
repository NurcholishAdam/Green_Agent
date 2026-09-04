# community_data.py

"""
Community-driven data sharing for Green Agent.

Users can optionally contribute anonymized data to improve simulations.
Enhanced with optional fields for advanced modules:
- LIMIT Graph metrics (centrality, connectivity)
- MODP (Multi‑Objective Decision Process) composite score
- RLHF human feedback score
- Multi‑Teacher On‑Policy Distillation statistics
- Bio‑inspired Optimisation / MoE expert gating flags

The enhancements are optional; if the modules are not present or the
parameters are not provided, the original behaviour is preserved.
"""

import json
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any


class CommunityDataHub:
    """
    Optional community data sharing.

    Users can choose to contribute:
    - Anonymized grid carbon observations
    - Helium price observations from invoices
    - Recovery efficiency data

    Enhanced contributions may also include:
    - Graph metrics (centrality, connectivity) from LIMIT Graph
    - MODP composite score
    - RLHF human feedback score
    - Distillation / MoE statistics
    """

    DATA_DIR = Path.home() / '.green_agent' / 'community_data'

    @classmethod
    def contribute_carbon_observation(
        cls,
        region: str,
        intensity: float,
        source: str,
        graph_metrics: Optional[Dict[str, float]] = None,
        human_feedback_score: Optional[float] = None,
        modp_score: Optional[float] = None,
        distillation_stats: Optional[Dict[str, Any]] = None,
        moe_gate_weights: Optional[list] = None,
        use_evolutionary: Optional[bool] = None,
        flexgen_enabled: Optional[bool] = None,
    ):
        """
        Contribute observed carbon intensity (optional).

        Additional parameters allow inclusion of advanced metrics when the
        corresponding Green Agent modules are active. All are optional and
        default to None, so the observation remains backwards compatible.
        """
        observation = {
            'timestamp': datetime.now().isoformat(),
            'region': region,
            'intensity': intensity,
            'source': source,
            'anonymous': True,
        }

        # Add enhanced fields only if provided
        if graph_metrics is not None:
            observation['graph_metrics'] = graph_metrics
        if human_feedback_score is not None:
            observation['human_feedback_score'] = human_feedback_score
        if modp_score is not None:
            observation['modp_score'] = modp_score
        if distillation_stats is not None:
            observation['distillation_stats'] = distillation_stats
        if moe_gate_weights is not None:
            observation['moe_gate_weights'] = moe_gate_weights
        if use_evolutionary is not None:
            observation['use_evolutionary'] = use_evolutionary
        if flexgen_enabled is not None:
            observation['flexgen_enabled'] = flexgen_enabled

        cls.DATA_DIR.mkdir(parents=True, exist_ok=True)
        with open(cls.DATA_DIR / 'carbon_observations.jsonl', 'a') as f:
            f.write(json.dumps(observation) + '\n')

    @classmethod
    def get_community_average(cls, region: str, days: int = 30) -> Optional[float]:
        """Get average intensity from community data (if available)"""
        if not cls.DATA_DIR.exists():
            return None

        observations = []
        cutoff = datetime.now().timestamp() - days * 86400

        with open(cls.DATA_DIR / 'carbon_observations.jsonl', 'r') as f:
            for line in f:
                data = json.loads(line)
                if data['region'] == region and data['timestamp'] > cutoff:
                    observations.append(data['intensity'])

        if observations:
            return sum(observations) / len(observations)
        return None

    # ---------------------------------------------------------------------
    # Optional enhanced retrieval methods
    # ---------------------------------------------------------------------
    @classmethod
    def get_community_enhanced_data(
        cls,
        region: str,
        days: int = 30,
        metric: str = "modp_score",
    ) -> Dict[str, Any]:
        """
        Retrieve aggregated enhanced metrics from community observations.

        Parameters
        ----------
        region : str
            Region filter.
        days : int
            Number of days to look back.
        metric : str
            Which enhanced metric to aggregate. One of:
            'modp_score', 'human_feedback_score', 'graph_metrics',
            'distillation_stats', 'moe_gate_weights'.

        Returns
        -------
        dict
            A dictionary with the aggregated metric value or counts.
        """
        if not cls.DATA_DIR.exists():
            return {}

        observations = []
        cutoff = datetime.now().timestamp() - days * 86400
        file_path = cls.DATA_DIR / 'carbon_observations.jsonl'

        with open(file_path, 'r') as f:
            for line in f:
                data = json.loads(line)
                if data.get('region') == region and data.get('timestamp') > cutoff:
                    observations.append(data)

        if not observations:
            return {}

        if metric == "graph_metrics":
            # Average centrality and connectivity
            centrality_vals = [o.get('graph_metrics', {}).get('centrality') for o in observations if 'graph_metrics' in o]
            connectivity_vals = [o.get('graph_metrics', {}).get('connectivity') for o in observations if 'graph_metrics' in o]
            return {
                'average_centrality': sum(centrality_vals)/len(centrality_vals) if centrality_vals else None,
                'average_connectivity': sum(connectivity_vals)/len(connectivity_vals) if connectivity_vals else None,
            }
        elif metric == "modp_score":
            vals = [o['modp_score'] for o in observations if 'modp_score' in o]
            return {'average_modp_score': sum(vals)/len(vals) if vals else None}
        elif metric == "human_feedback_score":
            vals = [o['human_feedback_score'] for o in observations if 'human_feedback_score' in o]
            return {'average_human_feedback_score': sum(vals)/len(vals) if vals else None}
        elif metric == "distillation_stats":
            # Count of records with distillation stats
            count = sum(1 for o in observations if 'distillation_stats' in o)
            return {'observations_with_distillation_stats': count}
        elif metric == "moe_gate_weights":
            # Count of records with MoE gate weights
            count = sum(1 for o in observations if 'moe_gate_weights' in o)
            return {'observations_with_moe_gate_weights': count}
        else:
            raise ValueError(f"Unsupported metric: {metric}")
