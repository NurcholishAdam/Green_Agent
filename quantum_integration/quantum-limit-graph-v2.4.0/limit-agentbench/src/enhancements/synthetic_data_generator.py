# File: src/enhancements/synthetic_data_generator.py
"""
Advanced Synthetic Data Generator for Green Agent.
Generates realistic workloads, environmental conditions, and edge cases for policy testing.
"""

import random
import numpy as np
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import hashlib

from ..expert_registry import ExpertProfile, ExpertDomain
from ..node_registry import NodeDescriptor
from ..config import Config

@dataclass
class SyntheticTask:
    """A synthetic task with associated metadata."""
    task_id: str
    task_type: str          # e.g., 'summarization', 'classification', 'translation'
    text: str               # dummy text
    token_count: int
    required_accuracy: float
    latency_budget_ms: float
    priority: str           # 'accuracy', 'green', 'balanced'
    user_id: str
    timestamp: datetime

@dataclass
class SyntheticEnvironment:
    """Synthetic environmental conditions."""
    carbon_intensity_g_kwh: float
    helium_scarcity_index: float
    renewable_fraction: float
    harvester_availability: float
    time_of_day: int        # 0-23
    region: str

class SyntheticDataGenerator:
    """
    Generates synthetic data for policy testing and simulation.
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}
        self.seed = self.config.get('seed', 42)
        random.seed(self.seed)
        np.random.seed(self.seed)

        # Task type distributions
        self.task_types = {
            'summarization': 0.25,
            'classification': 0.20,
            'translation': 0.15,
            'question_answering': 0.15,
            'text_generation': 0.15,
            'sentiment_analysis': 0.10
        }

        # User priority profiles
        self.priority_profiles = ['accuracy', 'green', 'balanced']
        self.priority_weights = {
            'accuracy': {'alpha': 0.2, 'beta': 0.1, 'gamma': 0.1, 'delta': 0.1, 'epsilon': 0.1, 'zeta': 0.4},
            'green':    {'alpha': 0.4, 'beta': 0.3, 'gamma': 0.2, 'delta': 0.1, 'epsilon': 0.0, 'zeta': 0.0},
            'balanced': {'alpha': 0.25, 'beta': 0.25, 'gamma': 0.15, 'delta': 0.1, 'epsilon': 0.1, 'zeta': 0.15}
        }

        # Region distribution
        self.regions = ['us-east', 'us-west', 'eu-west', 'eu-north', 'asia-east', 'asia-southeast']
        self.region_carbon = {
            'us-east': 420, 'us-west': 350, 'eu-west': 280,
            'eu-north': 220, 'asia-east': 500, 'asia-southeast': 480
        }

        # Heuristic for token count distribution (log-normal)
        self.token_mean = 5.5  # log scale
        self.token_std = 1.2

    def set_seed(self, seed: int):
        """Set the random seed for reproducibility."""
        self.seed = seed
        random.seed(seed)
        np.random.seed(seed)

    # ------------------------------------------------------------------
    # Task Generation
    # ------------------------------------------------------------------

    def generate_task(self, **kwargs) -> SyntheticTask:
        """Generate a single synthetic task with optional overrides."""
        task_type = kwargs.get('task_type') or self._random_task_type()
        token_count = kwargs.get('token_count') or self._random_token_count()
        required_accuracy = kwargs.get('required_accuracy') or self._random_accuracy()
        latency_budget = kwargs.get('latency_budget_ms') or self._random_latency_budget()
        priority = kwargs.get('priority') or self._random_priority()
        user_id = kwargs.get('user_id') or f"user_{random.randint(1, 1000)}"
        timestamp = kwargs.get('timestamp') or datetime.now()

        # Generate dummy text of approximately token_count words
        text = self._generate_dummy_text(token_count)

        task_id = hashlib.md5(f"{user_id}{timestamp}{text[:50]}".encode()).hexdigest()[:12]

        return SyntheticTask(
            task_id=task_id,
            task_type=task_type,
            text=text,
            token_count=token_count,
            required_accuracy=required_accuracy,
            latency_budget_ms=latency_budget,
            priority=priority,
            user_id=user_id,
            timestamp=timestamp
        )

    def generate_task_batch(self, count: int, **kwargs) -> List[SyntheticTask]:
        """Generate a batch of synthetic tasks."""
        return [self.generate_task(**kwargs) for _ in range(count)]

    def _random_task_type(self) -> str:
        return np.random.choice(
            list(self.task_types.keys()),
            p=list(self.task_types.values())
        )

    def _random_token_count(self) -> int:
        return int(np.exp(np.random.normal(self.token_mean, self.token_std)))

    def _random_accuracy(self) -> float:
        return np.clip(np.random.normal(0.85, 0.1), 0.5, 1.0)

    def _random_latency_budget(self) -> float:
        return np.random.uniform(100, 2000)  # milliseconds

    def _random_priority(self) -> str:
        return np.random.choice(self.priority_profiles)

    def _generate_dummy_text(self, token_count: int) -> str:
        """Generate dummy text with approximately the given token count."""
        # Use a pool of common words; for simplicity, we just repeat a phrase.
        base = "This is a synthetic task for testing Green Agent routing policies. "
        repeats = (token_count // 10) + 1
        return (base * repeats)[:token_count * 5]  # rough token-to-char mapping

    # ------------------------------------------------------------------
    # Environment Generation
    # ------------------------------------------------------------------

    def generate_environment(self, **kwargs) -> SyntheticEnvironment:
        """Generate a synthetic environment with optional overrides."""
        region = kwargs.get('region') or np.random.choice(self.regions)
        carbon = kwargs.get('carbon_intensity_g_kwh') or self._random_carbon(region)
        helium = kwargs.get('helium_scarcity_index') or self._random_helium()
        renewable = kwargs.get('renewable_fraction') or self._random_renewable(region)
        harvester = kwargs.get('harvester_availability') or self._random_harvester()
        hour = kwargs.get('time_of_day') or np.random.randint(0, 24)

        return SyntheticEnvironment(
            carbon_intensity_g_kwh=carbon,
            helium_scarcity_index=helium,
            renewable_fraction=renewable,
            harvester_availability=harvester,
            time_of_day=hour,
            region=region
        )

    def _random_carbon(self, region: str) -> float:
        base = self.region_carbon.get(region, 400)
        # Add diurnal variation: lower at night
        hour = datetime.now().hour
        diurnal = 0.9 + 0.2 * np.sin((hour - 8) / 12 * np.pi)
        return base * diurnal + np.random.normal(0, 20)

    def _random_helium(self) -> float:
        return np.clip(np.random.normal(0.5, 0.2), 0, 1)

    def _random_renewable(self, region: str) -> float:
        base = {'us-east': 0.3, 'us-west': 0.45, 'eu-west': 0.5,
                'eu-north': 0.6, 'asia-east': 0.2, 'asia-southeast': 0.25}
        return base.get(region, 0.3) + np.random.normal(0, 0.05)

    def _random_harvester(self) -> float:
        # Availability varies by time of day (solar)
        hour = datetime.now().hour
        if 6 <= hour <= 18:
            base = 0.6 + 0.3 * (1 - abs(hour - 12) / 6)
        else:
            base = 0.1
        return np.clip(base + np.random.normal(0, 0.1), 0, 1)

    # ------------------------------------------------------------------
    # Edge Case Generation
    # ------------------------------------------------------------------

    def generate_edge_case(self, case_type: str) -> Dict[str, Any]:
        """Generate a specific edge case for stress‑testing."""
        if case_type == 'extreme_carbon':
            return {'carbon_intensity_g_kwh': 800 + np.random.normal(0, 50)}
        elif case_type == 'helium_crisis':
            return {'helium_scarcity_index': 0.9 + np.random.normal(0, 0.05)}
        elif case_type == 'harvester_downtime':
            return {'harvester_availability': 0.0}
        elif case_type == 'latency_spike':
            return {'latency_budget_ms': 50}
        elif case_type == 'renewable_surge':
            return {'renewable_fraction': 0.95}
        else:
            raise ValueError(f"Unknown edge case: {case_type}")

    # ------------------------------------------------------------------
    # Expert and Node Descriptor Generation (for simulation)
    # ------------------------------------------------------------------

    def generate_expert_profile(self, expert_id: Optional[str] = None) -> ExpertProfile:
        """Generate a random expert profile for simulation."""
        domains = list(ExpertDomain)
        return ExpertProfile(
            expert_id=expert_id or f"synth_expert_{uuid.uuid4().hex[:8]}",
            expert_name=f"Synthetic Expert {random.randint(1,100)}",
            domain=np.random.choice(domains),
            accuracy_score=np.random.uniform(0.7, 0.98),
            efficiency_score=np.random.uniform(0.6, 1.0),
            reliability_score=np.random.uniform(0.7, 1.0),
            carbon_per_inference=np.random.uniform(0.0001, 0.001),
            helium_per_inference=np.random.uniform(0.0001, 0.001),
            energy_per_inference=np.random.uniform(0.00001, 0.0001),
            avg_latency_ms=np.random.uniform(10, 200),
        )

    def generate_node_descriptor(self, node_id: Optional[str] = None) -> NodeDescriptor:
        """Generate a random node descriptor for simulation."""
        return NodeDescriptor(
            node_id=node_id or f"synth_node_{uuid.uuid4().hex[:8]}",
            location=np.random.choice(self.regions),
            energy_efficiency=np.random.uniform(100, 500),  # FLOPs/J
            carbon_intensity=self._random_carbon('global'),
            helium_index=np.random.uniform(0, 0.8),
            material_index=np.random.uniform(0, 1),
            cooling_type=np.random.choice(['air', 'liquid', 'cryogenic']),
            renewable_fraction=np.random.uniform(0, 1),
            harvester_type=np.random.choice([None, 'solar', 'bio_photovoltaic']),
            capture_efficiency=np.random.uniform(0, 1) if random.random() > 0.5 else None,
            energy_output_watts=np.random.uniform(0, 500) if random.random() > 0.5 else None,
        )

    # ------------------------------------------------------------------
    # Dataset Generation for Training/Simulation
    # ------------------------------------------------------------------

    def generate_dataset(
        self,
        num_tasks: int = 1000,
        include_edge_cases: bool = True,
        edge_case_fraction: float = 0.1
    ) -> List[Dict[str, Any]]:
        """
        Generate a full dataset consisting of tasks, environments, and optional edge cases.
        Each entry is a dict with 'task', 'environment', and optional 'edge_case'.
        """
        dataset = []
        num_edge = int(num_tasks * edge_case_fraction) if include_edge_cases else 0
        num_normal = num_tasks - num_edge

        # Normal tasks
        for _ in range(num_normal):
            task = self.generate_task()
            env = self.generate_environment()
            dataset.append({'task': task, 'environment': env})

        # Edge cases
        edge_types = ['extreme_carbon', 'helium_crisis', 'harvester_downtime',
                      'latency_spike', 'renewable_surge']
        for _ in range(num_edge):
            case_type = np.random.choice(edge_types)
            task = self.generate_task()
            env = self.generate_environment()
            edge = self.generate_edge_case(case_type)
            # Override environment with edge case
            for k, v in edge.items():
                if hasattr(env, k):
                    setattr(env, k, v)
            dataset.append({'task': task, 'environment': env, 'edge_case': case_type})

        return dataset

    # ------------------------------------------------------------------
    # Utility: Export for Simulation
    # ------------------------------------------------------------------

    def export_for_simulation(self, dataset: List[Dict[str, Any]]) -> List[Dict]:
        """Convert dataset to a format suitable for the DigitalTwin."""
        exported = []
        for item in dataset:
            exported.append({
                'task': {
                    'type': item['task'].task_type,
                    'token_count': item['task'].token_count,
                    'required_accuracy': item['task'].required_accuracy,
                    'latency_budget_ms': item['task'].latency_budget_ms,
                    'priority': item['task'].priority,
                    'user_id': item['task'].user_id,
                },
                'environment': {
                    'carbon_intensity': item['environment'].carbon_intensity_g_kwh,
                    'helium_scarcity': item['environment'].helium_scarcity_index,
                    'renewable_fraction': item['environment'].renewable_fraction,
                    'harvester_availability': item['environment'].harvester_availability,
                    'region': item['environment'].region,
                },
                'edge_case': item.get('edge_case')
            })
        return exported
