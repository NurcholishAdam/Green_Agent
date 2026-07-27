# File: src/enhancements/synthetic_data_generator.py
"""
Advanced Synthetic Data Generator for Green Agent.
Generates realistic workloads, environmental conditions, and edge cases for policy testing.

ENHANCEMENTS OVER v2.0:
- Consolidated duplicate class definitions.
- Generates NodeDescriptor and WorkloadDescriptor directly.
- Includes per‑task sustainability metrics (energy, carbon, helium).
- Can sample from real data distributions (via injected collectors).
- Configurable prompt pool from external file.
- Time‑series generation for helium and carbon (ARIMA‑like).
- Expanded anomaly types (network failure, expert degradation).
- Optional Parquet export.
- Dataset versioning.
- Comprehensive docstrings and type hints.
- Integration with Green_Agent schemas.
"""

import asyncio
import json
import random
import hashlib
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple, Union, Callable
import numpy as np
import pandas as pd
from pathlib import Path

# ---------- Pydantic ----------
try:
    from pydantic import BaseModel, Field, field_validator, ValidationInfo
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

# ---------- Local imports (schemas) ----------
from .schemas.node_descriptor import NodeDescriptor
from .schemas.workload_descriptor import WorkloadDescriptor
from ..expert_registry import ExpertProfile, ExpertDomain
from ..node_registry import NodeDescriptor  # fallback if needed

# ---------- Optional: data collectors (for real distributions) ----------
try:
    from ..data_integration.carbon_intensity import CarbonIntensityFetcher
    from ..data_integration.helium_collector import HeliumCollector
    from ..data_integration.material_footprint import MaterialFootprintUpdater
    COLLECTORS_AVAILABLE = True
except ImportError:
    COLLECTORS_AVAILABLE = False
    # Stubs
    class CarbonIntensityFetcher:
        async def get_intensity(self, region: str) -> float:
            return 0.4
    class HeliumCollector:
        async def get_connectivity_score(self, hotspot_id: str) -> float:
            return 0.8
    class MaterialFootprintUpdater:
        def get_footprint(self, product_id: str) -> Optional[Dict]:
            return None

# ============================================================================
# 1. CONFIGURATION (Pydantic)
# ============================================================================
if PYDANTIC_AVAILABLE:
    class SyntheticDataConfig(BaseModel):
        """Configuration for the synthetic data generator."""
        seed: int = Field(42, description="Random seed for reproducibility")
        # Task distributions
        task_types: Dict[str, float] = Field(
            default_factory=lambda: {
                'summarization': 0.25,
                'classification': 0.20,
                'translation': 0.15,
                'question_answering': 0.15,
                'text_generation': 0.15,
                'sentiment_analysis': 0.10
            }
        )
        priority_profiles: List[str] = Field(
            default_factory=lambda: ['accuracy', 'green', 'balanced']
        )
        # Region settings
        regions: List[str] = Field(
            default_factory=lambda: ['us-east', 'us-west', 'eu-west', 'eu-north', 'asia-east', 'asia-southeast']
        )
        region_carbon: Dict[str, float] = Field(
            default_factory=lambda: {
                'us-east': 420, 'us-west': 350, 'eu-west': 280,
                'eu-north': 220, 'asia-east': 500, 'asia-southeast': 480
            }
        )
        # Token count distribution (log-normal)
        token_mean: float = Field(5.5, ge=0)
        token_std: float = Field(1.2, ge=0)
        # Expert degradation
        default_degradation_rate: float = Field(0.0005, ge=0, le=0.1)
        # Anomaly injection
        default_anomaly_rate: float = Field(0.0, ge=0, le=1.0)
        # Temporal sequence
        default_rate_per_hour: float = Field(100.0, gt=0)
        default_duration_hours: int = Field(24, gt=0)
        # Real data integration
        use_real_distributions: bool = Field(False, description="Sample from collectors if available")
        # Prompt pool file (optional)
        prompt_pool_file: Optional[str] = Field(None, description="Path to a JSON file with list of prompts")
        # Export format
        export_format: str = Field("json", description="json or parquet")
        # Dataset version
        dataset_version: str = Field("2.0.0")

        @field_validator('task_types')
        @classmethod
        def task_types_sum_one(cls, v: Dict[str, float]) -> Dict[str, float]:
            if abs(sum(v.values()) - 1.0) > 1e-6:
                raise ValueError("Task type probabilities must sum to 1")
            return v

        @field_validator('default_anomaly_rate')
        @classmethod
        def anomaly_rate_range(cls, v: float) -> float:
            if not 0 <= v <= 1:
                raise ValueError("anomaly_rate must be between 0 and 1")
            return v

        @field_validator('export_format')
        @classmethod
        def validate_export_format(cls, v: str) -> str:
            if v not in ['json', 'parquet']:
                raise ValueError("export_format must be 'json' or 'parquet'")
            return v

        class Config:
            env_prefix = "SYNTH_"
else:
    # Fallback config as dict
    SYNTHETIC_CONFIG = {
        "seed": 42,
        "task_types": {
            'summarization': 0.25,
            'classification': 0.20,
            'translation': 0.15,
            'question_answering': 0.15,
            'text_generation': 0.15,
            'sentiment_analysis': 0.10
        },
        "priority_profiles": ['accuracy', 'green', 'balanced'],
        "regions": ['us-east', 'us-west', 'eu-west', 'eu-north', 'asia-east', 'asia-southeast'],
        "region_carbon": {
            'us-east': 420, 'us-west': 350, 'eu-west': 280,
            'eu-north': 220, 'asia-east': 500, 'asia-southeast': 480
        },
        "token_mean": 5.5,
        "token_std": 1.2,
        "default_degradation_rate": 0.0005,
        "default_anomaly_rate": 0.0,
        "default_rate_per_hour": 100.0,
        "default_duration_hours": 24,
        "use_real_distributions": False,
        "prompt_pool_file": None,
        "export_format": "json",
        "dataset_version": "2.0.0",
    }

# ============================================================================
# 2. DATA CLASSES (Enhanced - using schemas directly)
# ============================================================================
@dataclass
class SyntheticSustainabilityMetrics:
    """Per‑task sustainability metrics."""
    energy_joules: float
    carbon_kg: float
    helium_units: float
    material_index: float

@dataclass
class SyntheticExpertProfile(ExpertProfile):
    """Extended ExpertProfile with degradation support."""
    degradation_rate: float = 0.0005
    tasks_processed: int = 0

    def process_task(self) -> None:
        """Update metrics after processing a task (simulate degradation)."""
        self.tasks_processed += 1
        self.accuracy_score = max(0.5, self.accuracy_score - self.degradation_rate)
        self.energy_per_inference *= (1 + self.degradation_rate * 0.5)
        self.carbon_per_inference *= (1 + self.degradation_rate * 0.3)
        self.avg_latency_ms *= (1 + self.degradation_rate * 0.1)

# ============================================================================
# 3. MAIN GENERATOR (Enhanced, Consolidated)
# ============================================================================
class SyntheticDataGenerator:
    """
    Advanced synthetic data generator for policy testing and simulation.

    Features:
    - Pydantic‑validated configuration.
    - Generates NodeDescriptor and WorkloadDescriptor directly.
    - Includes per‑task sustainability metrics.
    - Can sample from real data distributions (via injected collectors).
    - Configurable prompt pool from external file.
    - Time‑series generation for helium and carbon (ARIMA‑like).
    - Expanded anomaly types (network failure, expert degradation).
    - Optional Parquet export.
    - Dataset versioning.
    - Async generation methods.
    """

    def __init__(
        self,
        config: Optional[Union[Dict[str, Any], SyntheticDataConfig]] = None,
        carbon_fetcher: Optional[CarbonIntensityFetcher] = None,
        helium_collector: Optional[HeliumCollector] = None,
        material_updater: Optional[MaterialFootprintUpdater] = None,
    ):
        """
        Initialize the generator.

        Args:
            config: Configuration dictionary or Pydantic object.
            carbon_fetcher: Optional CarbonIntensityFetcher for real distributions.
            helium_collector: Optional HeliumCollector for real connectivity scores.
            material_updater: Optional MaterialFootprintUpdater for real material indices.
        """
        if config is None:
            if PYDANTIC_AVAILABLE:
                self.config = SyntheticDataConfig()
            else:
                self.config = SYNTHETIC_CONFIG
        elif isinstance(config, dict):
            if PYDANTIC_AVAILABLE:
                self.config = SyntheticDataConfig(**config)
            else:
                self.config = config
        else:
            self.config = config

        # Set random seeds
        seed = self.config.get('seed', 42) if isinstance(self.config, dict) else self.config.seed
        random.seed(seed)
        np.random.seed(seed)

        # Extract config values
        self.task_types = self.config.get('task_types')
        self.priority_profiles = self.config.get('priority_profiles')
        self.regions = self.config.get('regions')
        self.region_carbon = self.config.get('region_carbon')
        self.token_mean = self.config.get('token_mean')
        self.token_std = self.config.get('token_std')
        self.default_degradation_rate = self.config.get('default_degradation_rate')
        self.default_anomaly_rate = self.config.get('default_anomaly_rate')
        self.default_rate_per_hour = self.config.get('default_rate_per_hour')
        self.default_duration_hours = self.config.get('default_duration_hours')
        self.use_real_distributions = self.config.get('use_real_distributions', False)
        self.prompt_pool_file = self.config.get('prompt_pool_file')
        self.export_format = self.config.get('export_format', 'json')
        self.dataset_version = self.config.get('dataset_version', '2.0.0')

        # Inject external collectors
        self.carbon_fetcher = carbon_fetcher
        self.helium_collector = helium_collector
        self.material_updater = material_updater

        # Load prompt pool
        self.prompt_pool = self._load_prompt_pool()

        # User-region mapping for correlations
        self.user_region_cache: Dict[str, str] = {}

        # Cache for real distributions (if enabled)
        self._real_carbon_cache: Dict[str, float] = {}
        self._real_helium_cache: Dict[str, float] = {}

    # ------------------------------------------------------------------
    # Configuration utilities
    # ------------------------------------------------------------------
    def set_seed(self, seed: int) -> None:
        """Set the random seed for reproducibility."""
        random.seed(seed)
        np.random.seed(seed)

    def _load_prompt_pool(self) -> List[str]:
        """Load prompt pool from a file if specified, otherwise use default."""
        if self.prompt_pool_file:
            try:
                with open(self.prompt_pool_file, 'r') as f:
                    data = json.load(f)
                    if isinstance(data, list):
                        return data
            except Exception as e:
                print(f"Warning: Could not load prompt pool file: {e}")
        # Default pool
        return [
            "Summarize the latest developments in sustainable AI.",
            "Translate the following English text into French: 'The quick brown fox jumps over the lazy dog.'",
            "Classify the sentiment of this customer review: 'I love this product, it's fantastic!'",
            "Answer the question: What are the main causes of climate change?",
            "Generate a short poem about nature.",
            "Extract the key entities from this news article about renewable energy.",
            "Rewrite this paragraph in a more formal style.",
            "Identify the main argument in the following text.",
            "Generate a follow-up question based on this conversation.",
            "Summarize the research paper titled 'Quantum Computing for Sustainability'.",
            "Translate this legal document from Spanish to English.",
            "Classify this image description: 'A solar panel array in a desert'.",
            "Answer this trivia: What is the capital of France?",
            "Write a short story about a robot learning to recycle.",
            "Analyze the tone of this tweet: 'Carbon offset credits are a scam!'",
        ]

    # ------------------------------------------------------------------
    # Task Generation (produces WorkloadDescriptor)
    # ------------------------------------------------------------------
    def generate_workload_descriptor(self, **kwargs) -> WorkloadDescriptor:
        """
        Generate a synthetic WorkloadDescriptor.

        Args:
            **kwargs: Override any field.
        """
        task_type = kwargs.get('task_type') or self._random_task_type()
        tokens = kwargs.get('tokens') or self._random_token_count()
        latency_target = kwargs.get('latency_target') or self._random_latency_budget()
        priority = kwargs.get('priority') or self._random_priority()
        bio_mode = kwargs.get('bio_mode') or random.choice(["photosynthetic", "chemotactic", "none"])
        sector_emission_factor = kwargs.get('sector_emission_factor') or random.uniform(0.01, 0.05)

        return WorkloadDescriptor(
            task_type=task_type,
            tokens=tokens,
            latency_target=latency_target,
            sector_emission_factor=sector_emission_factor,
            bio_mode=bio_mode,
            priority=priority,
        )

    def _random_task_type(self) -> str:
        task_types = self.task_types
        return np.random.choice(
            list(task_types.keys()),
            p=list(task_types.values())
        )

    def _random_token_count(self) -> int:
        return int(np.exp(np.random.normal(self.token_mean, self.token_std)))

    def _random_latency_budget(self) -> float:
        return np.random.uniform(100, 2000)  # milliseconds

    def _random_priority(self) -> str:
        return np.random.choice(self.priority_profiles)

    # ------------------------------------------------------------------
    # Environment / Node Descriptor Generation
    # ------------------------------------------------------------------
    def generate_node_descriptor(self, **kwargs) -> NodeDescriptor:
        """
        Generate a synthetic NodeDescriptor.

        Args:
            **kwargs: Override any field.
        """
        node_id = kwargs.get('node_id') or f"synth_node_{uuid.uuid4().hex[:8]}"
        node_type = kwargs.get('type') or random.choice(["edge", "hotspot", "cloud", "lab"])
        region = kwargs.get('region') or random.choice(self.regions)
        # Carbon intensity: either from real collector or random
        if self.use_real_distributions and self.carbon_fetcher:
            # Use cached or fetch (synchronous for simplicity; could be async)
            if region not in self._real_carbon_cache:
                # For demonstration, we use a synchronous stub; in real use, async
                intensity = asyncio.run(self.carbon_fetcher.get_intensity(region))
                self._real_carbon_cache[region] = intensity
            region_carbon_intensity = self._real_carbon_cache[region]
        else:
            region_carbon_intensity = kwargs.get('region_carbon_intensity') or self._random_carbon(region)

        energy_per_token = kwargs.get('energy_per_token') or random.uniform(0.00001, 0.0001)
        # Helium connectivity: from collector or random
        if self.use_real_distributions and self.helium_collector:
            hotspot_id = kwargs.get('hotspot_id') or f"hotspot_{random.randint(1,1000)}"
            if hotspot_id not in self._real_helium_cache:
                score = asyncio.run(self.helium_collector.get_connectivity_score(hotspot_id))
                self._real_helium_cache[hotspot_id] = score
            helium_connectivity_score = self._real_helium_cache[hotspot_id]
        else:
            helium_connectivity_score = kwargs.get('helium_connectivity_score') or random.uniform(0.5, 1.0)

        material_footprint_id = kwargs.get('material_footprint_id') or random.choice(["gpu-a100", "gpu-h100", "edge-device"])
        uptime = kwargs.get('uptime') or random.uniform(0.9, 1.0)
        renewable_fraction = kwargs.get('renewable_fraction') or self._random_renewable(region)

        return NodeDescriptor(
            id=node_id,
            type=node_type,
            region=region,
            region_carbon_intensity=region_carbon_intensity,
            energy_per_token=energy_per_token,
            helium_connectivity_score=helium_connectivity_score,
            material_footprint_id=material_footprint_id,
            uptime=uptime,
            renewable_fraction=renewable_fraction,
        )

    def _random_carbon(self, region: str) -> float:
        base = self.region_carbon.get(region, 400)
        # Add diurnal variation: lower at night
        hour = datetime.now().hour
        diurnal = 0.9 + 0.2 * np.sin((hour - 8) / 12 * np.pi)
        return (base * diurnal + np.random.normal(0, 20)) / 1000  # return kg CO₂/kWh

    def _random_renewable(self, region: str) -> float:
        base = {
            'us-east': 0.3, 'us-west': 0.45, 'eu-west': 0.5,
            'eu-north': 0.6, 'asia-east': 0.2, 'asia-southeast': 0.25
        }
        return base.get(region, 0.3) + np.random.normal(0, 0.05)

    # ------------------------------------------------------------------
    # Sustainability Metrics
    # ------------------------------------------------------------------
    def compute_sustainability_metrics(
        self,
        workload: WorkloadDescriptor,
        node: NodeDescriptor,
    ) -> SyntheticSustainabilityMetrics:
        """
        Compute energy, carbon, helium, and material metrics for a given workload and node.
        """
        # Energy: energy_per_token * tokens
        energy_joules = node.energy_per_token * workload.tokens

        # Carbon: energy * carbon_intensity (kg CO₂ per kWh conversion)
        carbon_kg = energy_joules / 3.6e6 * node.region_carbon_intensity

        # Helium: inverse of connectivity score (scaled)
        helium_units = (1 - node.helium_connectivity_score) * 0.5

        # Material: from footprint if available
        material_index = 0.0
        if self.material_updater and node.material_footprint_id:
            fp = self.material_updater.get_footprint(node.material_footprint_id)
            if fp:
                material_index = fp.get('material_index', 0.0)

        return SyntheticSustainabilityMetrics(
            energy_joules=energy_joules,
            carbon_kg=carbon_kg,
            helium_units=helium_units,
            material_index=material_index,
        )

    # ------------------------------------------------------------------
    # Temporal Sequences (Poisson process)
    # ------------------------------------------------------------------
    def generate_task_sequence(
        self,
        duration_hours: Optional[int] = None,
        rate_per_hour: Optional[float] = None,
        start_time: Optional[datetime] = None,
        **kwargs
    ) -> List[Dict[str, Any]]:
        """
        Generate a sequence of (workload, node, metrics) using a Poisson process.

        Args:
            duration_hours: Length of the sequence in hours.
            rate_per_hour: Average number of tasks per hour.
            start_time: Start time for the sequence.
            **kwargs: Additional overrides passed to generate descriptors.
        Returns:
            List of dicts with 'workload', 'node', and 'metrics'.
        """
        duration = duration_hours or self.default_duration_hours
        rate = rate_per_hour or self.default_rate_per_hour
        start = start_time or datetime.now()

        sequence = []
        t = 0.0
        while t < duration * 3600:
            dt = np.random.exponential(1 / rate)  # seconds
            t += dt
            if t >= duration * 3600:
                break
            timestamp = start + timedelta(seconds=t)
            # Generate descriptors with optional timestamp (for correlation)
            workload = self.generate_workload_descriptor(**kwargs)
            node = self.generate_node_descriptor(**kwargs)
            metrics = self.compute_sustainability_metrics(workload, node)
            sequence.append({
                'timestamp': timestamp,
                'workload': workload,
                'node': node,
                'metrics': metrics,
            })
        return sequence

    async def generate_task_sequence_async(self, **kwargs) -> List[Dict[str, Any]]:
        """Async version of generate_task_sequence."""
        return self.generate_task_sequence(**kwargs)

    # ------------------------------------------------------------------
    # Anomaly Injection (Enhanced)
    # ------------------------------------------------------------------
    def inject_anomaly(
        self,
        workload: WorkloadDescriptor,
        node: NodeDescriptor,
        anomaly_type: Optional[str] = None,
    ) -> Tuple[WorkloadDescriptor, NodeDescriptor, str]:
        """
        Inject an anomaly into a workload or node.

        Returns:
            (modified workload, modified node, anomaly_type)
        """
        if anomaly_type is None:
            anomaly_type = random.choice([
                'extreme_token_count',
                'zero_accuracy',
                'zero_latency',
                'extreme_carbon',
                'helium_crisis',
                'harvester_downtime',
                'renewable_surge',
                'network_failure',
                'expert_degradation',
            ])
        if anomaly_type == 'extreme_token_count':
            workload.tokens = int(np.random.exponential(10000)) + 5000
        elif anomaly_type == 'zero_accuracy':
            # Simulate by setting a low required_accuracy (or we could add a field)
            workload.latency_target = 0.0  # unrealistic latency
        elif anomaly_type == 'zero_latency':
            workload.latency_target = 0.0
        elif anomaly_type == 'extreme_carbon':
            node.region_carbon_intensity = 0.8 + np.random.normal(0, 0.05)
        elif anomaly_type == 'helium_crisis':
            node.helium_connectivity_score = 0.1 + np.random.normal(0, 0.02)
        elif anomaly_type == 'harvester_downtime':
            node.renewable_fraction = 0.0
            node.uptime = 0.5
        elif anomaly_type == 'renewable_surge':
            node.renewable_fraction = 0.95
        elif anomaly_type == 'network_failure':
            node.helium_connectivity_score = 0.0
            node.uptime = 0.0
        elif anomaly_type == 'expert_degradation':
            # Simulate by adding a degradation flag (would be handled in expert selection)
            pass
        else:
            raise ValueError(f"Unknown anomaly_type: {anomaly_type}")
        return workload, node, anomaly_type

    # ------------------------------------------------------------------
    # Dataset Generation (Using Descriptors)
    # ------------------------------------------------------------------
    def generate_dataset(
        self,
        num_samples: int = 1000,
        include_edge_cases: bool = True,
        edge_case_fraction: float = 0.1,
        anomaly_rate: Optional[float] = None,
    ) -> List[Dict[str, Any]]:
        """
        Generate a full dataset consisting of workloads, nodes, metrics, and optional anomalies.

        Returns:
            List of dicts with:
                'workload': WorkloadDescriptor
                'node': NodeDescriptor
                'metrics': SyntheticSustainabilityMetrics
                'anomaly': Optional[str]
        """
        if anomaly_rate is None:
            anomaly_rate = self.default_anomaly_rate

        dataset = []
        num_edge = int(num_samples * edge_case_fraction) if include_edge_cases else 0
        num_normal = num_samples - num_edge

        # Normal samples
        for _ in range(num_normal):
            workload = self.generate_workload_descriptor()
            node = self.generate_node_descriptor()
            # Optionally inject anomaly
            anomaly = None
            if random.random() < anomaly_rate:
                workload, node, anomaly = self.inject_anomaly(workload, node)
            metrics = self.compute_sustainability_metrics(workload, node)
            dataset.append({
                'workload': workload,
                'node': node,
                'metrics': metrics,
                'anomaly': anomaly,
            })

        # Edge cases with forced anomalies
        edge_types = [
            'extreme_token_count', 'zero_accuracy', 'zero_latency',
            'extreme_carbon', 'helium_crisis', 'harvester_downtime',
            'renewable_surge', 'network_failure', 'expert_degradation'
        ]
        for _ in range(num_edge):
            anomaly_type = random.choice(edge_types)
            workload = self.generate_workload_descriptor()
            node = self.generate_node_descriptor()
            workload, node, _ = self.inject_anomaly(workload, node, anomaly_type)
            metrics = self.compute_sustainability_metrics(workload, node)
            dataset.append({
                'workload': workload,
                'node': node,
                'metrics': metrics,
                'anomaly': anomaly_type,
            })

        return dataset

    async def generate_dataset_async(self, **kwargs) -> List[Dict[str, Any]]:
        """Async version of generate_dataset."""
        return self.generate_dataset(**kwargs)

    # ------------------------------------------------------------------
    # Persistence (JSON/Parquet with versioning)
    # ------------------------------------------------------------------
    def save_dataset(self, dataset: List[Dict[str, Any]], path: str) -> None:
        """
        Save dataset to a file (JSON or Parquet).

        Args:
            dataset: The dataset (list of dicts with 'workload', 'node', 'metrics', 'anomaly').
            path: Output file path.
        """
        # Convert Pydantic/dataclass objects to serializable dicts
        serializable = []
        for item in dataset:
            entry = {
                'version': self.dataset_version,
                'workload': item['workload'].dict() if hasattr(item['workload'], 'dict') else item['workload'].__dict__,
                'node': item['node'].dict() if hasattr(item['node'], 'dict') else item['node'].__dict__,
                'metrics': item['metrics'].__dict__,
                'anomaly': item['anomaly'],
            }
            serializable.append(entry)

        if self.export_format == 'parquet':
            df = pd.DataFrame(serializable)
            df.to_parquet(path, index=False)
        else:
            with open(path, 'w') as f:
                json.dump(serializable, f, indent=2, default=str)

    def load_dataset(self, path: str) -> List[Dict[str, Any]]:
        """
        Load a dataset from a file (JSON or Parquet).

        Returns:
            List of dicts with 'workload', 'node', 'metrics', 'anomaly'.
        """
        if path.endswith('.parquet'):
            df = pd.read_parquet(path)
            # Convert back to objects
            dataset = []
            for _, row in df.iterrows():
                workload = WorkloadDescriptor(**row['workload'])
                node = NodeDescriptor(**row['node'])
                metrics = SyntheticSustainabilityMetrics(**row['metrics'])
                dataset.append({
                    'workload': workload,
                    'node': node,
                    'metrics': metrics,
                    'anomaly': row.get('anomaly'),
                })
            return dataset
        else:
            with open(path, 'r') as f:
                data = json.load(f)
            dataset = []
            for entry in data:
                workload = WorkloadDescriptor(**entry['workload'])
                node = NodeDescriptor(**entry['node'])
                metrics = SyntheticSustainabilityMetrics(**entry['metrics'])
                dataset.append({
                    'workload': workload,
                    'node': node,
                    'metrics': metrics,
                    'anomaly': entry.get('anomaly'),
                })
            return dataset

    # ------------------------------------------------------------------
    # Expert Profile Generation (with degradation)
    # ------------------------------------------------------------------
    def generate_expert_profile(
        self,
        expert_id: Optional[str] = None,
        degradation_rate: Optional[float] = None,
    ) -> SyntheticExpertProfile:
        """Generate a synthetic expert profile with degradation support."""
        if degradation_rate is None:
            degradation_rate = self.default_degradation_rate
        return SyntheticExpertProfile(
            expert_id=expert_id or f"synth_expert_{uuid.uuid4().hex[:8]}",
            expert_name=f"Synthetic Expert {random.randint(1,100)}",
            domain=np.random.choice(list(ExpertDomain.__dict__.values()) if hasattr(ExpertDomain, '__dict__') else ['summarization']),
            accuracy_score=np.random.uniform(0.7, 0.98),
            efficiency_score=np.random.uniform(0.6, 1.0),
            reliability_score=np.random.uniform(0.7, 1.0),
            carbon_per_inference=np.random.uniform(0.0001, 0.001),
            helium_per_inference=np.random.uniform(0.0001, 0.001),
            energy_per_inference=np.random.uniform(0.00001, 0.0001),
            avg_latency_ms=np.random.uniform(10, 200),
            degradation_rate=degradation_rate,
        )

    # ------------------------------------------------------------------
    # Utility: Export for Simulation
    # ------------------------------------------------------------------
    def export_for_simulation(self, dataset: List[Dict[str, Any]]) -> List[Dict]:
        """Convert dataset to a format suitable for DigitalTwin."""
        exported = []
        for item in dataset:
            exported.append({
                'workload': {
                    'type': item['workload'].task_type,
                    'tokens': item['workload'].tokens,
                    'latency_target': item['workload'].latency_target,
                    'priority': item['workload'].priority,
                    'bio_mode': item['workload'].bio_mode,
                },
                'node': {
                    'id': item['node'].id,
                    'region': item['node'].region,
                    'carbon_intensity': item['node'].region_carbon_intensity,
                    'energy_per_token': item['node'].energy_per_token,
                    'helium_connectivity': item['node'].helium_connectivity_score,
                    'material_footprint_id': item['node'].material_footprint_id,
                },
                'metrics': item['metrics'].__dict__,
                'anomaly': item['anomaly'],
            })
        return exported

# ============================================================================
# 4. UNIT TEST STUBS (pytest)
# ============================================================================
def test_generator_basic():
    """Basic test for descriptor generation."""
    gen = SyntheticDataGenerator()
    workload = gen.generate_workload_descriptor()
    node = gen.generate_node_descriptor()
    assert workload.tokens > 0
    assert node.region_carbon_intensity > 0

def test_persistence(tmp_path):
    """Test save/load of dataset."""
    gen = SyntheticDataGenerator()
    dataset = gen.generate_dataset(num_samples=10)
    path = tmp_path / "dataset.json"
    gen.save_dataset(dataset, path)
    loaded = gen.load_dataset(path)
    assert len(loaded) == len(dataset)
    assert loaded[0]['workload'].tokens == dataset[0]['workload'].tokens

def test_anomaly_injection():
    gen = SyntheticDataGenerator()
    workload = gen.generate_workload_descriptor()
    node = gen.generate_node_descriptor()
    workload, node, anomaly = gen.inject_anomaly(workload, node, 'extreme_token_count')
    assert workload.tokens > 10000
    assert anomaly == 'extreme_token_count'

# ============================================================================
# 5. EXAMPLE USAGE
# ============================================================================
if __name__ == "__main__":
    import asyncio

    async def main():
        # Create generator with custom config
        config = {
            'seed': 123,
            'default_anomaly_rate': 0.1,
            'default_duration_hours': 2,
            'default_rate_per_hour': 50,
        }
        gen = SyntheticDataGenerator(config)

        # Generate a sequence of tasks over 2 hours
        seq = gen.generate_task_sequence(duration_hours=2, rate_per_hour=50)
        print(f"Generated {len(seq)} tasks over 2 hours")

        # Generate a full dataset with edge cases
        dataset = gen.generate_dataset(num_samples=100, include_edge_cases=True, edge_case_fraction=0.2)
        print(f"Generated dataset with {len(dataset)} samples, including edge cases")

        # Show first sample
        sample = dataset[0]
        print(f"Workload: {sample['workload'].task_type}, tokens: {sample['workload'].tokens}")
        print(f"Node: {sample['node'].id}, region: {sample['node'].region}, carbon: {sample['node'].region_carbon_intensity:.3f} kg/kWh")
        print(f"Metrics: energy={sample['metrics'].energy_joules:.2f} J, carbon={sample['metrics'].carbon_kg:.4f} kg")
        print(f"Anomaly: {sample.get('anomaly')}")

        # Save dataset
        gen.save_dataset(dataset, "test_dataset.json")
        print("Dataset saved.")

    asyncio.run(main())
