# File: src/enhancements/synthetic_data_generator.py
"""
Advanced Synthetic Data Generator for Green Agent.
Generates realistic workloads, environmental conditions, and edge cases for policy testing.

ENHANCEMENTS OVER v1.0:
- Pydantic‑validated configuration with environment variable support.
- Persistence (save/load datasets to/from JSON).
- Realistic dummy text from a curated pool of prompts.
- Task–environment correlations (region linked to user).
- Temporal sequences via Poisson process for workload bursts.
- Expert degradation over time/usage.
- Anomaly injection with configurable rate.
- Async generation methods.
- Comprehensive docstrings and type hints.
- Unit test stubs.
"""

import asyncio
import json
import random
import hashlib
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple, Union
import numpy as np

# ---------- Pydantic ----------
try:
    from pydantic import BaseModel, Field, field_validator, ValidationInfo
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

# ---------- Local imports (fallback) ----------
try:
    from ..expert_registry import ExpertProfile, ExpertDomain
    from ..node_registry import NodeDescriptor
except ImportError:
    # Fallback definitions for standalone testing
    class ExpertDomain:
        SUMMARIZATION = "summarization"
        CLASSIFICATION = "classification"
        TRANSLATION = "translation"
        QA = "question_answering"
        GENERATION = "text_generation"
        SENTIMENT = "sentiment_analysis"

    @dataclass
    class ExpertProfile:
        expert_id: str
        expert_name: str = ""
        domain: str = ExpertDomain.SUMMARIZATION
        accuracy_score: float = 0.9
        efficiency_score: float = 0.9
        reliability_score: float = 0.9
        carbon_per_inference: float = 0.001
        helium_per_inference: float = 0.0005
        energy_per_inference: float = 0.00005
        avg_latency_ms: float = 100.0

    @dataclass
    class NodeDescriptor:
        node_id: str
        location: str = "global"
        energy_efficiency: float = 300.0
        carbon_intensity: float = 400.0
        helium_index: float = 0.0
        material_index: float = 0.0
        cooling_type: str = "air"
        renewable_fraction: float = 0.0
        harvester_type: Optional[str] = None
        capture_efficiency: Optional[float] = None
        energy_output_watts: Optional[float] = None

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
    }

# ============================================================================
# 2. DATA CLASSES (Enhanced)
# ============================================================================
@dataclass
class SyntheticTask:
    """A synthetic task with associated metadata."""
    task_id: str
    task_type: str
    text: str
    token_count: int
    required_accuracy: float
    latency_budget_ms: float
    priority: str
    user_id: str
    timestamp: datetime

@dataclass
class SyntheticEnvironment:
    """Synthetic environmental conditions."""
    carbon_intensity_g_kwh: float
    helium_scarcity_index: float
    renewable_fraction: float
    harvester_availability: float
    time_of_day: int
    region: str

@dataclass
class SyntheticExpertProfile(ExpertProfile):
    """Extended ExpertProfile with degradation support."""
    degradation_rate: float = 0.0005
    tasks_processed: int = 0

    def process_task(self) -> None:
        """Update metrics after processing a task (simulate degradation)."""
        self.tasks_processed += 1
        # Simulate slight degradation in accuracy and energy efficiency
        self.accuracy_score = max(0.5, self.accuracy_score - self.degradation_rate)
        self.energy_per_inference *= (1 + self.degradation_rate * 0.5)
        self.carbon_per_inference *= (1 + self.degradation_rate * 0.3)
        self.avg_latency_ms *= (1 + self.degradation_rate * 0.1)

# ============================================================================
# 3. MAIN GENERATOR (Enhanced)
# ============================================================================
class SyntheticDataGenerator:
    """
    Advanced synthetic data generator for policy testing and simulation.

    Features:
    - Pydantic‑validated configuration
    - Save/load datasets to/from JSON
    - Realistic dummy text from a prompt pool
    - Task–environment correlations (region linked to user)
    - Temporal sequences via Poisson process
    - Expert degradation over time/usage
    - Anomaly injection
    - Async generation methods
    """

    def __init__(self, config: Optional[Union[Dict[str, Any], SyntheticDataConfig]] = None):
        """
        Initialize the generator.

        Args:
            config: Configuration dictionary or Pydantic object.
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

        # Curated pool of realistic prompts for dummy text
        self.prompt_pool = [
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

        # User-region mapping for correlations
        self.user_region_cache: Dict[str, str] = {}

    # ------------------------------------------------------------------
    # Configuration utilities
    # ------------------------------------------------------------------
    def set_seed(self, seed: int) -> None:
        """Set the random seed for reproducibility."""
        seed = seed if isinstance(seed, int) else 42
        random.seed(seed)
        np.random.seed(seed)

    # ------------------------------------------------------------------
    # Task Generation
    # ------------------------------------------------------------------
    def generate_task(self, **kwargs) -> SyntheticTask:
        """
        Generate a single synthetic task with optional overrides.

        Args:
            **kwargs: Override any task attribute (task_type, token_count, etc.).
        """
        task_type = kwargs.get('task_type') or self._random_task_type()
        token_count = kwargs.get('token_count') or self._random_token_count()
        required_accuracy = kwargs.get('required_accuracy') or self._random_accuracy()
        latency_budget = kwargs.get('latency_budget_ms') or self._random_latency_budget()
        priority = kwargs.get('priority') or self._random_priority()
        user_id = kwargs.get('user_id') or f"user_{random.randint(1, 1000)}"
        timestamp = kwargs.get('timestamp') or datetime.now()

        # Generate dummy text
        text = self._generate_dummy_text(token_count)

        # Create task ID
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

    async def generate_task_batch_async(self, count: int, **kwargs) -> List[SyntheticTask]:
        """Async version of generate_task_batch."""
        # Use asyncio.gather for parallel generation (simulated)
        # In practice, generation is fast, but we keep async for consistency.
        tasks = [self.generate_task(**kwargs) for _ in range(count)]
        return tasks

    def _random_task_type(self) -> str:
        task_types = self.task_types
        return np.random.choice(
            list(task_types.keys()),
            p=list(task_types.values())
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
        """Generate dummy text with approximately token_count words."""
        if token_count <= 0:
            return ""
        # Select a prompt from the pool
        base = random.choice(self.prompt_pool)
        # Repeat the base to reach the desired token count (roughly)
        repeats = max(1, (token_count // len(base.split())) + 1)
        full_text = " ".join([base] * repeats)
        # Trim to approximate token_count words
        words = full_text.split()
        if len(words) > token_count:
            words = words[:token_count]
        return " ".join(words)

    # ------------------------------------------------------------------
    # Environment Generation
    # ------------------------------------------------------------------
    def generate_environment(self, **kwargs) -> SyntheticEnvironment:
        """
        Generate a synthetic environment with optional overrides.

        If a region is not provided, it is inferred from the user (if user_id is given).
        """
        region = kwargs.get('region')
        # If region not provided and user_id is given, look up cached region
        if region is None and 'user_id' in kwargs:
            user_id = kwargs['user_id']
            if user_id in self.user_region_cache:
                region = self.user_region_cache[user_id]
            else:
                # Assign a random region for this user and cache it
                region = np.random.choice(self.regions)
                self.user_region_cache[user_id] = region
        if region is None:
            region = np.random.choice(self.regions)

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
        base = {
            'us-east': 0.3, 'us-west': 0.45, 'eu-west': 0.5,
            'eu-north': 0.6, 'asia-east': 0.2, 'asia-southeast': 0.25
        }
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
    # Temporal Sequences
    # ------------------------------------------------------------------
    def generate_task_sequence(
        self,
        duration_hours: Optional[int] = None,
        rate_per_hour: Optional[float] = None,
        start_time: Optional[datetime] = None,
        **kwargs
    ) -> List[SyntheticTask]:
        """
        Generate a sequence of tasks using a Poisson process.

        Args:
            duration_hours: Length of the sequence in hours.
            rate_per_hour: Average number of tasks per hour.
            start_time: Start time for the sequence.
            **kwargs: Additional overrides passed to generate_task.
        """
        duration = duration_hours or self.default_duration_hours
        rate = rate_per_hour or self.default_rate_per_hour
        start = start_time or datetime.now()

        tasks = []
        t = 0.0
        while t < duration * 3600:
            # Exponential inter-arrival time
            dt = np.random.exponential(1 / rate)  # seconds
            t += dt
            if t >= duration * 3600:
                break
            timestamp = start + timedelta(seconds=t)
            # Generate a task with the given timestamp
            task = self.generate_task(timestamp=timestamp, **kwargs)
            tasks.append(task)
        return tasks

    async def generate_task_sequence_async(
        self,
        duration_hours: Optional[int] = None,
        rate_per_hour: Optional[float] = None,
        start_time: Optional[datetime] = None,
        **kwargs
    ) -> List[SyntheticTask]:
        """Async version of generate_task_sequence."""
        return self.generate_task_sequence(duration_hours, rate_per_hour, start_time, **kwargs)

    # ------------------------------------------------------------------
    # Anomaly Injection
    # ------------------------------------------------------------------
    def generate_anomaly_task(self, anomaly_type: Optional[str] = None, **kwargs) -> SyntheticTask:
        """
        Generate a task with an anomaly (extreme values).

        Args:
            anomaly_type: Type of anomaly ('extreme_carbon', 'helium_crisis',
                           'harvester_downtime', 'latency_spike', 'renewable_surge',
                           'extreme_token_count', 'zero_accuracy').
            **kwargs: Overrides for the task.
        """
        if anomaly_type is None:
            anomaly_type = np.random.choice([
                'extreme_token_count',
                'zero_accuracy',
                'zero_latency',
            ])
        # Base task
        task = self.generate_task(**kwargs)
        # Apply anomaly
        if anomaly_type == 'extreme_token_count':
            task.token_count = int(np.random.exponential(10000)) + 5000
            task.text = self._generate_dummy_text(task.token_count)
        elif anomaly_type == 'zero_accuracy':
            task.required_accuracy = 0.0
        elif anomaly_type == 'zero_latency':
            task.latency_budget_ms = 0.0
        elif anomaly_type == 'extreme_carbon':
            # This would be applied to the environment, not the task.
            # We'll handle this in the dataset generation.
            pass
        else:
            raise ValueError(f"Unknown anomaly_type: {anomaly_type}")
        return task

    def generate_environment_with_anomaly(self, anomaly_type: str, **kwargs) -> SyntheticEnvironment:
        """Generate an environment with an anomaly."""
        env = self.generate_environment(**kwargs)
        if anomaly_type == 'extreme_carbon':
            env.carbon_intensity_g_kwh = 800 + np.random.normal(0, 50)
        elif anomaly_type == 'helium_crisis':
            env.helium_scarcity_index = 0.9 + np.random.normal(0, 0.05)
        elif anomaly_type == 'harvester_downtime':
            env.harvester_availability = 0.0
        elif anomaly_type == 'renewable_surge':
            env.renewable_fraction = 0.95
        else:
            raise ValueError(f"Unknown environmental anomaly: {anomaly_type}")
        return env

    # ------------------------------------------------------------------
    # Dataset Generation
    # ------------------------------------------------------------------
    def generate_dataset(
        self,
        num_tasks: int = 1000,
        include_edge_cases: bool = True,
        edge_case_fraction: float = 0.1,
        anomaly_rate: Optional[float] = None,
    ) -> List[Dict[str, Any]]:
        """
        Generate a full dataset consisting of tasks, environments, and optional edge cases.

        Args:
            num_tasks: Total number of samples.
            include_edge_cases: Whether to include edge cases.
            edge_case_fraction: Fraction of tasks that are edge cases.
            anomaly_rate: Probability of injecting an anomaly into a task/environment.

        Returns:
            A list of dicts, each with 'task', 'environment', and optional 'edge_case'.
        """
        if anomaly_rate is None:
            anomaly_rate = self.default_anomaly_rate

        dataset = []
        num_edge = int(num_tasks * edge_case_fraction) if include_edge_cases else 0
        num_normal = num_tasks - num_edge

        # Normal tasks
        for _ in range(num_normal):
            # Decide if this normal task should have an anomaly (if anomaly_rate > 0)
            if random.random() < anomaly_rate:
                task = self.generate_anomaly_task()
            else:
                task = self.generate_task()
            # Generate environment with optional anomaly
            if random.random() < anomaly_rate:
                # Choose a random environmental anomaly
                env_anomaly = np.random.choice(['extreme_carbon', 'helium_crisis', 'harvester_downtime', 'renewable_surge'])
                env = self.generate_environment_with_anomaly(env_anomaly, user_id=task.user_id)
                # Record the edge case type
                edge_case = env_anomaly
            else:
                env = self.generate_environment(user_id=task.user_id)
                edge_case = None
            dataset.append({'task': task, 'environment': env, 'edge_case': edge_case})

        # Edge cases (with specific anomalies)
        edge_types = ['extreme_carbon', 'helium_crisis', 'harvester_downtime',
                      'latency_spike', 'renewable_surge', 'extreme_token_count',
                      'zero_accuracy', 'zero_latency']
        for _ in range(num_edge):
            case_type = np.random.choice(edge_types)
            if case_type in ['extreme_carbon', 'helium_crisis', 'harvester_downtime', 'renewable_surge']:
                # Environmental edge case
                task = self.generate_task()
                env = self.generate_environment_with_anomaly(case_type, user_id=task.user_id)
                edge_case = case_type
            else:
                # Task edge case
                task = self.generate_anomaly_task(case_type)
                env = self.generate_environment(user_id=task.user_id)
                edge_case = case_type
            dataset.append({'task': task, 'environment': env, 'edge_case': edge_case})

        return dataset

    async def generate_dataset_async(self, **kwargs) -> List[Dict[str, Any]]:
        """Async version of generate_dataset."""
        return self.generate_dataset(**kwargs)

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------
    def save_dataset(self, dataset: List[Dict[str, Any]], path: str) -> None:
        """
        Save dataset to a JSON file.

        Args:
            dataset: The dataset (list of dicts with 'task', 'environment', 'edge_case').
            path: Output file path.
        """
        # Convert dataclasses to serializable dicts
        serializable = []
        for item in dataset:
            entry = {
                'task': {
                    'task_id': item['task'].task_id,
                    'task_type': item['task'].task_type,
                    'text': item['task'].text,
                    'token_count': item['task'].token_count,
                    'required_accuracy': item['task'].required_accuracy,
                    'latency_budget_ms': item['task'].latency_budget_ms,
                    'priority': item['task'].priority,
                    'user_id': item['task'].user_id,
                    'timestamp': item['task'].timestamp.isoformat(),
                },
                'environment': {
                    'carbon_intensity_g_kwh': item['environment'].carbon_intensity_g_kwh,
                    'helium_scarcity_index': item['environment'].helium_scarcity_index,
                    'renewable_fraction': item['environment'].renewable_fraction,
                    'harvester_availability': item['environment'].harvester_availability,
                    'time_of_day': item['environment'].time_of_day,
                    'region': item['environment'].region,
                },
                'edge_case': item.get('edge_case')
            }
            serializable.append(entry)
        with open(path, 'w') as f:
            json.dump(serializable, f, indent=2)

    def load_dataset(self, path: str) -> List[Dict[str, Any]]:
        """
        Load a dataset from a JSON file.

        Args:
            path: Input file path.

        Returns:
            List of dicts with 'task', 'environment', 'edge_case'.
        """
        with open(path, 'r') as f:
            data = json.load(f)
        dataset = []
        for entry in data:
            task = SyntheticTask(
                task_id=entry['task']['task_id'],
                task_type=entry['task']['task_type'],
                text=entry['task']['text'],
                token_count=entry['task']['token_count'],
                required_accuracy=entry['task']['required_accuracy'],
                latency_budget_ms=entry['task']['latency_budget_ms'],
                priority=entry['task']['priority'],
                user_id=entry['task']['user_id'],
                timestamp=datetime.fromisoformat(entry['task']['timestamp'])
            )
            env = SyntheticEnvironment(
                carbon_intensity_g_kwh=entry['environment']['carbon_intensity_g_kwh'],
                helium_scarcity_index=entry['environment']['helium_scarcity_index'],
                renewable_fraction=entry['environment']['renewable_fraction'],
                harvester_availability=entry['environment']['harvester_availability'],
                time_of_day=entry['environment']['time_of_day'],
                region=entry['environment']['region']
            )
            dataset.append({
                'task': task,
                'environment': env,
                'edge_case': entry.get('edge_case')
            })
        return dataset

    # ------------------------------------------------------------------
    # Expert and Node Descriptor Generation
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

    def generate_node_descriptor(self, node_id: Optional[str] = None) -> NodeDescriptor:
        """Generate a random node descriptor for simulation."""
        return NodeDescriptor(
            node_id=node_id or f"synth_node_{uuid.uuid4().hex[:8]}",
            location=np.random.choice(self.regions),
            energy_efficiency=np.random.uniform(100, 500),
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
    # Utility: Export for Simulation
    # ------------------------------------------------------------------
    def export_for_simulation(self, dataset: List[Dict[str, Any]]) -> List[Dict]:
        """Convert dataset to a format suitable for DigitalTwin."""
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


# ============================================================================
# 4. UNIT TEST STUBS (pytest)
# ============================================================================
def test_generator_basic():
    """Basic test for task generation."""
    gen = SyntheticDataGenerator()
    task = gen.generate_task()
    assert task.task_id is not None
    assert task.token_count > 0

def test_persistence(tmp_path):
    """Test save/load of dataset."""
    gen = SyntheticDataGenerator()
    dataset = gen.generate_dataset(num_tasks=10)
    path = tmp_path / "dataset.json"
    gen.save_dataset(dataset, path)
    loaded = gen.load_dataset(path)
    assert len(loaded) == len(dataset)
    assert loaded[0]['task'].task_id == dataset[0]['task'].task_id

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
        dataset = gen.generate_dataset(num_tasks=100, include_edge_cases=True, edge_case_fraction=0.2)
        print(f"Generated dataset with {len(dataset)} samples, including edge cases")

        # Show first sample
        sample = dataset[0]
        print(f"Sample task: {sample['task'].task_type}, tokens: {sample['task'].token_count}")
        print(f"Sample environment: region {sample['environment'].region}, carbon {sample['environment'].carbon_intensity_g_kwh:.0f} g/kWh")
        print(f"Edge case: {sample.get('edge_case')}")

        # Save dataset
        gen.save_dataset(dataset, "test_dataset.json")
        print("Dataset saved.")

    asyncio.run(main())
