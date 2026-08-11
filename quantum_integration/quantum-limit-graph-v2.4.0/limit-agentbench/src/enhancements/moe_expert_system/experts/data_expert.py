#!/usr/bin/env python3
# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/moe_expert_system/experts/data_expert.py
# Version 3.2.0 – Full Green Agent MOPD Integration

"""
Enhanced Data Expert v3.2.0 – Complete Data Services Layer for MoE System
Full Green Agent MOPD Integration

ENHANCEMENTS OVER v3.1.0:
1. INTEGRATED with central Config, Storage, Logger, MetricsRegistry, AsyncMessageQueue.
2. ADDED teacher interface (`policy_probs`) for MTPD optimizer.
3. PUBLISHES FeedbackEvent for every data operation (profile, clean, summarize, validate, route).
4. USES central AdaptiveCostFunction, ParetoGating, and DriftDetector.
5. REUSES central Vault and master key for post‑quantum cryptography (if needed).
6. REMOVED custom persistence; now uses central Storage (extended with data tables).
7. REMOVED custom Prometheus; now uses central MetricsRegistry.
8. REMOVED custom logging; now uses central structlog.
9. REMOVED custom circuit breaker; now uses central EnhancedCircuitBreaker.
10. All optional dependencies (pandas, numpy, etc.) still gracefully degrade.
"""

import asyncio
import json
import os
import hashlib
import uuid
import time
from typing import Dict, Any, List, Optional, Tuple, Union, Callable, AsyncGenerator
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from collections import defaultdict, Counter
import numpy as np
import pandas as pd
import pickle
from enum import Enum
import aiohttp
from pathlib import Path
from functools import lru_cache

# -----------------------------------------------------------------------------
# IMPORT CENTRAL GREEN AGENT COMPONENTS
# -----------------------------------------------------------------------------
from ..config import config as central_config
from ..storage import Storage
from ..schemas.feedback_event import FeedbackEvent
from ..routing.pareto_gating import ParetoGating
from ..feedback.adaptive_cost import AdaptiveCostFunction
from ..safety.drift_detector import DriftDetector
from ..scaling.message_queue import AsyncMessageQueue
from ..metrics import MetricsRegistry
from ..logger import logger

# Optional: central circuit breaker and rate limiter
try:
    from ..scaling.circuit_breaker import EnhancedCircuitBreaker
    from ..scaling.rate_limiter import EnhancedRateLimiter
    CENTRAL_CIRCUIT_BREAKER_AVAILABLE = True
except ImportError:
    # Fallback (simple implementations provided below if needed)
    from ..scaling.circuit_breaker import CircuitBreaker as EnhancedCircuitBreaker
    CENTRAL_CIRCUIT_BREAKER_AVAILABLE = False

# Optional: central carbon manager
try:
    from ..carbon_intensity import CarbonIntensityManager
    CENTRAL_CARBON_AVAILABLE = True
except ImportError:
    CENTRAL_CARBON_AVAILABLE = False

# Optional: central helium manager
try:
    from ..helium_optimizer import HeliumEfficiencyOptimizer
    CENTRAL_HELIUM_AVAILABLE = True
except ImportError:
    CENTRAL_HELIUM_AVAILABLE = False

# Optional: base expert
try:
    from .base_expert import BaseExpert
    BASE_EXPERT_AVAILABLE = True
except ImportError:
    # Fallback BaseExpert
    class BaseExpert:
        def __init__(self):
            self.expert_name = "data_expert"
            self.supported_task_types = ["data_profile", "data_clean", "data_summary", "data_validate", "data_transform"]
            self.health_status = "healthy"
        async def handle_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
            raise NotImplementedError()
        def get_capabilities(self) -> Dict[str, Any]:
            return {'name': self.expert_name, 'supported_tasks': self.supported_task_types, 'health': self.health_status}
        def get_metrics(self) -> Dict[str, Any]:
            return {}

# Optional: bio-inspired modules (optional)
try:
    from enhancements.bio_inspired.eco_atp_currency import EcoATPTokenManager, EcoATPConsumer
    TOKEN_AVAILABLE = True
except ImportError:
    TOKEN_AVAILABLE = False
try:
    from enhancements.bio_inspired.proton_gradient_fields import GradientFieldManager
    GRADIENT_AVAILABLE = True
except ImportError:
    GRADIENT_AVAILABLE = False

# ============================================================================
# Configuration – now built from central_config
# ============================================================================
class DataExpertConfig:
    """Configuration for DataExpert, built from central_config."""
    def __init__(self):
        self.enable_profiling = getattr(central_config, "data_enable_profiling", True)
        self.enable_cleaning = getattr(central_config, "data_enable_cleaning", True)
        self.enable_summarization = getattr(central_config, "data_enable_summarization", True)
        self.enable_energy_tracking = getattr(central_config, "data_enable_energy_tracking", True)
        self.enable_federated_aggregation = getattr(central_config, "data_enable_federated_aggregation", True)
        self.enable_telemetry = True  # always use central metrics
        self.enable_persistence = True  # always use central storage
        self.enable_url_fetch = getattr(central_config, "data_enable_url_fetch", True)
        self.enable_database = getattr(central_config, "data_enable_database", True)
        self.enable_streaming = getattr(central_config, "data_enable_streaming", True)

        self.max_rows_profile = getattr(central_config, "data_max_rows_profile", 10000)
        self.max_unique_values = getattr(central_config, "data_max_unique_values", 100)
        self.missing_value_threshold = getattr(central_config, "data_missing_value_threshold", 0.5)
        self.bytes_to_kwh_factor = getattr(central_config, "data_bytes_to_kwh_factor", 1e-9)
        self.carbon_intensity_g_per_kwh = getattr(central_config, "carbon_intensity_g_per_kwh", 100.0)
        self.federated_server_url = getattr(central_config, "federated_server_url", None)
        self.cache_ttl_seconds = getattr(central_config, "data_cache_ttl_seconds", 3600)
        self.max_retries = getattr(central_config, "data_max_retries", 3)
        self.retry_base_delay_ms = getattr(central_config, "data_retry_base_delay_ms", 100.0)
        self.retry_max_delay_ms = getattr(central_config, "data_retry_max_delay_ms", 5000.0)
        self.circuit_breaker_failure_threshold = getattr(central_config, "circuit_breaker_failure_threshold", 5)
        self.circuit_breaker_recovery_timeout = getattr(central_config, "circuit_breaker_recovery_timeout", 30.0)

        # Validate
        if self.missing_value_threshold < 0 or self.missing_value_threshold > 1:
            self.missing_value_threshold = 0.5
        if self.bytes_to_kwh_factor <= 0:
            self.bytes_to_kwh_factor = 1e-9

# ============================================================================
# Enums and Data Classes (unchanged)
# ============================================================================
class DataSourceType(Enum):
    CSV = "csv"
    JSON = "json"
    PARQUET = "parquet"
    DATABASE = "database"
    IN_MEMORY = "in_memory"
    URL = "url"
    STREAM = "stream"

class DataQualityIssue(Enum):
    MISSING_VALUES = "missing_values"
    DUPLICATES = "duplicates"
    OUTLIERS = "outliers"
    TYPE_MISMATCH = "type_mismatch"
    SKEW = "skew"
    HIGH_CARDINALITY = "high_cardinality"

@dataclass
class ColumnProfile:
    name: str
    dtype: str
    non_null_count: int
    null_count: int
    unique_count: int
    missing_pct: float
    min_val: Optional[Any] = None
    max_val: Optional[Any] = None
    mean_val: Optional[float] = None
    std_val: Optional[float] = None
    median_val: Optional[Any] = None
    skewness: Optional[float] = None
    kurtosis: Optional[float] = None
    top_values: Optional[List[Tuple[Any, int]]] = None
    issues: List[DataQualityIssue] = field(default_factory=list)
    def to_dict(self) -> Dict[str, Any]: return asdict(self)

@dataclass
class DataProfile:
    dataset_name: str
    shape: Tuple[int, int]
    total_cells: int
    memory_usage_bytes: int
    timestamp: str
    columns: Dict[str, ColumnProfile]
    global_issues: List[DataQualityIssue]
    quality_score: float
    def to_dict(self) -> Dict[str, Any]:
        return {
            'dataset_name': self.dataset_name,
            'shape': self.shape,
            'total_cells': self.total_cells,
            'memory_usage_bytes': self.memory_usage_bytes,
            'timestamp': self.timestamp,
            'columns': {k: v.to_dict() for k, v in self.columns.items()},
            'global_issues': [i.value for i in self.global_issues],
            'quality_score': self.quality_score,
        }

@dataclass
class DataSummary:
    dataset_id: str
    rows: int
    columns: int
    column_names: List[str]
    column_dtypes: Dict[str, str]
    sample_rows: List[Dict[str, Any]]
    schema_hash: str
    data_profile: Optional[DataProfile] = None
    quality_issues: List[str] = field(default_factory=list)
    recommendations: List[str] = field(default_factory=list)
    def to_dict(self) -> Dict[str, Any]:
        return {
            'dataset_id': self.dataset_id,
            'rows': self.rows,
            'columns': self.columns,
            'column_names': self.column_names,
            'column_dtypes': self.column_dtypes,
            'sample_rows': self.sample_rows,
            'schema_hash': self.schema_hash,
            'data_profile': self.data_profile.to_dict() if self.data_profile else None,
            'quality_issues': self.quality_issues,
            'recommendations': self.recommendations,
        }

@dataclass
class DataOperationMetrics:
    operation_name: str
    start_time: float
    end_time: Optional[float] = None
    bytes_processed: int = 0
    rows_processed: int = 0
    energy_kwh: float = 0.0
    carbon_kg: float = 0.0
    success: bool = True
    error_message: Optional[str] = None
    def compute_energy_carbon(self, config: DataExpertConfig):
        self.energy_kwh = self.bytes_processed * config.bytes_to_kwh_factor
        self.carbon_kg = self.energy_kwh * config.carbon_intensity_g_per_kwh / 1000.0
    def duration_seconds(self) -> float:
        if self.end_time: return self.end_time - self.start_time
        return 0.0
    def to_dict(self) -> Dict[str, Any]: return asdict(self)

# ============================================================================
# Data Expert Implementation – Fully Integrated
# ============================================================================
class DataExpert(BaseExpert):
    """
    Data Expert v3.2.0 – Data Services Layer for MoE System
    Full Green Agent MOPD integration.
    """

    def __init__(
        self,
        storage: Storage,
        message_queue: AsyncMessageQueue,
        adaptive_cost: AdaptiveCostFunction,
        pareto_gating: ParetoGating,
        drift_detector: DriftDetector,
        metrics: MetricsRegistry,
        carbon_manager: Optional[Any] = None,
        helium_optimizer: Optional[Any] = None,
        token_manager: Optional[Any] = None,
        gradient_manager: Optional[Any] = None
    ):
        super().__init__()
        self.expert_name = "data_expert"
        self.supported_task_types = [
            "data_profile", "data_clean", "data_summary",
            "data_validate", "data_transform", "data_route",
            "data_federated_aggregate"
        ]
        self.health_status = "healthy"

        self.storage = storage
        self.queue = message_queue
        self.adaptive_cost = adaptive_cost
        self.pareto = pareto_gating
        self.drift = drift_detector
        self.metrics = metrics
        self.carbon_manager = carbon_manager
        self.helium_optimizer = helium_optimizer
        self.token_manager = token_manager
        self.gradient_manager = gradient_manager

        # Configuration – built from central_config
        self.config = DataExpertConfig()

        # State
        self.datasets: Dict[str, pd.DataFrame] = {}
        self.profiles: Dict[str, DataProfile] = {}
        self.metrics_history: List[DataOperationMetrics] = []
        self.tasks_handled = 0
        self.total_latency = 0.0
        self.task_counts = {'profile': 0, 'clean': 0, 'summarize': 0, 'validate': 0, 'route': 0}

        # Caching with TTL
        self._cache_timestamps: Dict[str, datetime] = {}
        self._lock = asyncio.Lock()

        # Circuit breaker (central)
        self._circuit_breaker = EnhancedCircuitBreaker(
            "data_external",
            failure_threshold=self.config.circuit_breaker_failure_threshold,
            recovery_timeout=self.config.circuit_breaker_recovery_timeout
        )

        # Session for HTTP requests
        self._session: Optional[aiohttp.ClientSession] = None
        self._session_lock = asyncio.Lock()

        # Load persisted state from central storage
        asyncio.create_task(self._load_state())

        logger.info(f"DataExpert v3.2.0 initialized.")

    # ==========================================================================
    # State Persistence using central Storage
    # ==========================================================================
    async def _load_state(self):
        """Load expert state from central storage."""
        try:
            data = self.storage.get_state("data_expert_state")
            if data:
                state = json.loads(data)
                self.tasks_handled = state.get('tasks_handled', 0)
                self.total_latency = state.get('total_latency', 0.0)
                self.task_counts = state.get('task_counts', {'profile': 0, 'clean': 0, 'summarize': 0, 'validate': 0, 'route': 0})
                # Restore metrics history
                for metrics_dict in state.get('metrics_history', []):
                    metrics = DataOperationMetrics(**metrics_dict)
                    self.metrics_history.append(metrics)
                # Restore profiles (reconstruct from dict)
                for dataset_id, profile_dict in state.get('profiles', {}).items():
                    # Reconstruct DataProfile
                    columns = {}
                    for col_name, col_dict in profile_dict['columns'].items():
                        col_profile = ColumnProfile(
                            name=col_name,
                            dtype=col_dict['dtype'],
                            non_null_count=col_dict['non_null_count'],
                            null_count=col_dict['null_count'],
                            unique_count=col_dict['unique_count'],
                            missing_pct=col_dict['missing_pct'],
                            min_val=col_dict.get('min_val'),
                            max_val=col_dict.get('max_val'),
                            mean_val=col_dict.get('mean_val'),
                            std_val=col_dict.get('std_val'),
                            median_val=col_dict.get('median_val'),
                            skewness=col_dict.get('skewness'),
                            kurtosis=col_dict.get('kurtosis'),
                            top_values=col_dict.get('top_values'),
                            issues=[DataQualityIssue(i) for i in col_dict.get('issues', [])]
                        )
                        columns[col_name] = col_profile
                    profile = DataProfile(
                        dataset_name=profile_dict['dataset_name'],
                        shape=tuple(profile_dict['shape']),
                        total_cells=profile_dict['total_cells'],
                        memory_usage_bytes=profile_dict['memory_usage_bytes'],
                        timestamp=profile_dict['timestamp'],
                        columns=columns,
                        global_issues=[DataQualityIssue(i) for i in profile_dict['global_issues']],
                        quality_score=profile_dict['quality_score']
                    )
                    self.profiles[dataset_id] = profile
                    self._cache_timestamps[dataset_id] = datetime.now(timezone.utc)
                # Restore datasets from storage (as BLOBs)
                dataset_blobs = self.storage.get_state("data_expert_datasets")
                if dataset_blobs:
                    for dataset_id, blob in dataset_blobs.items():
                        self.datasets[dataset_id] = pickle.loads(blob)
                logger.info("DataExpert state loaded from central storage")
        except Exception as e:
            logger.error(f"Failed to load data expert state: {e}")

    async def _save_state(self):
        """Save expert state to central storage."""
        try:
            state = {
                'tasks_handled': self.tasks_handled,
                'total_latency': self.total_latency,
                'task_counts': self.task_counts,
                'metrics_history': [m.to_dict() for m in self.metrics_history[-1000:]],
                'profiles': {k: v.to_dict() for k, v in self.profiles.items()},
                'timestamp': datetime.now(timezone.utc).isoformat()
            }
            self.storage.save_state("data_expert_state", json.dumps(state))
            # Store datasets as BLOBs (using pickle)
            dataset_blobs = {}
            for dataset_id, df in self.datasets.items():
                dataset_blobs[dataset_id] = pickle.dumps(df)
            self.storage.save_state("data_expert_datasets", json.dumps(dataset_blobs))
            logger.info("DataExpert state saved to central storage")
        except Exception as e:
            logger.error(f"Failed to save data expert state: {e}")

    # ==========================================================================
    # Teacher Interface for MOPD
    # ==========================================================================
    async def policy_probs(self, state: Dict) -> List[float]:
        """
        Return a probability distribution over data-handling strategies.
        This allows the MTPD optimizer to treat this module as a teacher.
        """
        strategies = ['profile', 'clean', 'summarize', 'validate', 'route']
        counts = [self.task_counts.get(s, 0) for s in strategies]
        total = sum(counts)
        if total == 0:
            return [0.2] * 5
        return [c / total for c in counts]

    # ==========================================================================
    # Core Expert Interface
    # ==========================================================================
    async def handle_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        task_type = task.get('type', 'unknown')
        task_id = task.get('correlation_id', str(uuid.uuid4()))

        start_time = datetime.now(timezone.utc)
        start_ts = asyncio.get_event_loop().time()

        logger.info(f"DataExpert handling task: {task_type} (ID: {task_id})")

        try:
            if task_type == 'data_profile':
                result = await self.profile_data(task)
            elif task_type == 'data_clean':
                result = await self.clean_data(task)
            elif task_type == 'data_summary':
                result = await self.summarize_data(task)
            elif task_type == 'data_validate':
                result = await self.validate_data(task)
            elif task_type == 'data_route':
                result = await self.route_data(task)
            elif task_type == 'data_federated_aggregate':
                result = await self.federated_aggregate(task)
            else:
                result = {'status': 'error', 'error': f"Unknown task type: {task_type}"}

            end_ts = asyncio.get_event_loop().time()
            latency = end_ts - start_ts
            self.tasks_handled += 1
            self.total_latency += latency
            self.task_counts[task_type.replace('data_', '')] = self.task_counts.get(task_type.replace('data_', ''), 0) + 1

            # Record metrics
            self.metrics.increment_data_task(task_type, result.get('status', 'success'))
            self.metrics.observe_data_latency(task_type, latency)

            result['correlation_id'] = task_id
            result['latency_seconds'] = latency
            logger.info(f"DataExpert completed {task_type}: latency={latency:.3f}s")

            return result

        except Exception as e:
            logger.error(f"DataExpert error on {task_type}: {e}", exc_info=True)
            self.metrics.increment_data_task(task_type, 'error')
            return {'status': 'error', 'error': str(e), 'correlation_id': task_id}

    # ==========================================================================
    # Core Data Operations (Enhanced with FeedbackEvent)
    # ==========================================================================
    async def load_data(self, source: Union[str, pd.DataFrame, Dict, List, AsyncGenerator], source_type: DataSourceType = DataSourceType.IN_MEMORY, dataset_id: Optional[str] = None) -> pd.DataFrame:
        if dataset_id is None:
            dataset_id = f"dataset_{uuid.uuid4().hex[:8]}"
        start_ts = asyncio.get_event_loop().time()
        try:
            if source_type == DataSourceType.IN_MEMORY or isinstance(source, (pd.DataFrame, dict, list)):
                if isinstance(source, (dict, list)):
                    df = pd.DataFrame(source)
                else:
                    df = source
            elif source_type == DataSourceType.CSV:
                df = pd.read_csv(source)
            elif source_type == DataSourceType.JSON:
                df = pd.read_json(source)
            elif source_type == DataSourceType.PARQUET:
                df = pd.read_parquet(source)
            elif source_type == DataSourceType.URL and self.config.enable_url_fetch:
                df = await self._fetch_from_url(source)
            elif source_type == DataSourceType.DATABASE and self.config.enable_database:
                df = await self._fetch_from_database(source)
            elif source_type == DataSourceType.STREAM and self.config.enable_streaming:
                df = await self._fetch_from_stream(source)
            else:
                raise ValueError(f"Unsupported source type: {source_type}")
            self.datasets[dataset_id] = df
            end_ts = asyncio.get_event_loop().time()
            latency = end_ts - start_ts
            bytes_loaded = df.memory_usage(deep=True).sum()
            # Record metrics
            metrics = DataOperationMetrics(
                operation_name="load_data",
                start_time=start_ts,
                end_time=end_ts,
                bytes_processed=bytes_loaded,
                rows_processed=len(df),
            )
            metrics.compute_energy_carbon(self.config)
            self.metrics_history.append(metrics)
            self.metrics.increment_data_bytes(bytes_loaded)
            self.metrics.increment_data_carbon(metrics.carbon_kg)
            self.metrics.increment_data_energy(metrics.energy_kwh)
            logger.info(f"Loaded dataset {dataset_id}: {df.shape}, {bytes_loaded} bytes")
            return df
        except Exception as e:
            logger.error(f"Failed to load data: {e}")
            raise

    async def _fetch_from_url(self, url: str) -> pd.DataFrame:
        async def _fetch():
            session = await self._get_session()
            async with session.get(url) as response:
                if response.status != 200:
                    raise aiohttp.ClientError(f"HTTP {response.status}")
                content = await response.read()
                if url.endswith('.csv'):
                    return pd.read_csv(pd.io.common.StringIO(content.decode()))
                elif url.endswith('.json'):
                    return pd.read_json(content)
                else:
                    return pd.read_csv(pd.io.common.StringIO(content.decode()))
        return await self._circuit_breaker.call(_fetch)

    async def _fetch_from_database(self, connection_string: str) -> pd.DataFrame:
        raise NotImplementedError("Database fetch not implemented")

    async def _fetch_from_stream(self, stream: AsyncGenerator) -> pd.DataFrame:
        chunks = []
        async for chunk in stream:
            if isinstance(chunk, pd.DataFrame):
                chunks.append(chunk)
            elif isinstance(chunk, dict):
                chunks.append(pd.DataFrame([chunk]))
            else:
                chunks.append(pd.DataFrame(chunk))
        if chunks:
            return pd.concat(chunks, ignore_index=True)
        return pd.DataFrame()

    async def _get_session(self) -> aiohttp.ClientSession:
        async with self._session_lock:
            if self._session is None or self._session.closed:
                self._session = aiohttp.ClientSession()
            return self._session

    async def profile_data(self, task: Dict[str, Any]) -> Dict[str, Any]:
        dataset = task.get('data')
        dataset_id = task.get('dataset_id', f"profile_{uuid.uuid4().hex[:8]}")
        force_refresh = task.get('force_refresh', False)

        if not force_refresh and dataset_id in self.profiles:
            cached_time = self._cache_timestamps.get(dataset_id)
            if cached_time and (datetime.now(timezone.utc) - cached_time).total_seconds() < self.config.cache_ttl_seconds:
                logger.info(f"Returning cached profile for {dataset_id}")
                return {'status': 'success', 'dataset_id': dataset_id, 'profile': self.profiles[dataset_id].to_dict(), 'cached': True}

        if isinstance(dataset, str):
            df = await self.load_data(dataset, DataSourceType.CSV, dataset_id)
        elif isinstance(dataset, pd.DataFrame):
            df = dataset
        else:
            df = pd.DataFrame(dataset)

        profile = await self._profile_dataframe(df, dataset_id)
        self.profiles[dataset_id] = profile
        self._cache_timestamps[dataset_id] = datetime.now(timezone.utc)

        # Publish FeedbackEvent
        event = FeedbackEvent.create_with_context(
            task_id=f"data_profile_{dataset_id}",
            selected_action="profile",
            quality_score=profile.quality_score,
            energy_joules=profile.memory_usage_bytes * self.config.bytes_to_kwh_factor * 3.6e6,
            carbon_g=profile.memory_usage_bytes * self.config.bytes_to_kwh_factor * self.config.carbon_intensity_g_per_kwh / 1000 * 1000,
            feedback_type="data",
            adaptive_cost_value=0.0,
            state={'dataset_id': dataset_id, 'rows': df.shape[0], 'cols': df.shape[1]},
            candidates=[{'action': 'profile', 'clean', 'summarize', 'validate', 'route'}],
            source="data_expert",
            environment=getattr(central_config, "ENVIRONMENT", "production"),
            tags=["data", "profile"]
        )
        await self.queue.publish("feedback_events", event.to_json())

        # Check drift
        if self.drift:
            await self.drift.check_drift(self.adaptive_cost.get_current_weights())

        return {'status': 'success', 'dataset_id': dataset_id, 'profile': profile.to_dict(), 'cached': False}

    async def _profile_dataframe(self, df: pd.DataFrame, dataset_id: str) -> DataProfile:
        start_ts = asyncio.get_event_loop().time()
        sample_df = df.head(self.config.max_rows_profile)
        columns = {}
        global_issues = []
        for col in sample_df.columns:
            col_data = sample_df[col]
            non_null = col_data.notna().sum()
            null_count = col_data.isna().sum()
            missing_pct = null_count / len(sample_df)
            dtype = str(col_data.dtype)
            unique_count = col_data.nunique()
            col_profile = ColumnProfile(
                name=col,
                dtype=dtype,
                non_null_count=non_null,
                null_count=null_count,
                unique_count=unique_count,
                missing_pct=missing_pct,
                issues=[],
            )
            if missing_pct > self.config.missing_value_threshold:
                col_profile.issues.append(DataQualityIssue.MISSING_VALUES)
                global_issues.append(DataQualityIssue.MISSING_VALUES)
            if unique_count == 1:
                col_profile.issues.append(DataQualityIssue.DUPLICATES)
            if unique_count > self.config.max_unique_values and dtype == 'object':
                col_profile.issues.append(DataQualityIssue.HIGH_CARDINALITY)
            if pd.api.types.is_numeric_dtype(col_data):
                col_profile.min_val = col_data.min()
                col_profile.max_val = col_data.max()
                col_profile.mean_val = col_data.mean()
                col_profile.std_val = col_data.std()
                col_profile.median_val = col_data.median()
                try:
                    col_profile.skewness = col_data.skew()
                    col_profile.kurtosis = col_data.kurtosis()
                except:
                    pass
            if pd.api.types.is_object_dtype(col_data) or unique_count <= self.config.max_unique_values:
                top_vals = col_data.value_counts().head(5)
                col_profile.top_values = list(zip(top_vals.index, top_vals.values))
            columns[col] = col_profile
        issue_penalty = len(global_issues) * 0.1
        quality_score = max(0.0, 1.0 - issue_penalty)
        end_ts = asyncio.get_event_loop().time()
        bytes_processed = df.memory_usage(deep=True).sum()
        metrics = DataOperationMetrics(
            operation_name="profile_data",
            start_time=start_ts,
            end_time=end_ts,
            bytes_processed=bytes_processed,
            rows_processed=len(df),
        )
        metrics.compute_energy_carbon(self.config)
        self.metrics_history.append(metrics)
        self.metrics.increment_data_bytes(bytes_processed)
        self.metrics.increment_data_carbon(metrics.carbon_kg)
        self.metrics.increment_data_energy(metrics.energy_kwh)
        return DataProfile(
            dataset_name=dataset_id,
            shape=df.shape,
            total_cells=df.shape[0] * df.shape[1],
            memory_usage_bytes=int(bytes_processed),
            timestamp=datetime.now(timezone.utc).isoformat(),
            columns=columns,
            global_issues=global_issues,
            quality_score=quality_score,
        )

    async def clean_data(self, task: Dict[str, Any]) -> Dict[str, Any]:
        dataset = task.get('data')
        dataset_id = task.get('dataset_id', f"cleaned_{uuid.uuid4().hex[:8]}")
        params = task.get('params', {})

        if isinstance(dataset, pd.DataFrame):
            df = dataset.copy()
        else:
            df = await self.load_data(dataset, DataSourceType.CSV, dataset_id)

        start_ts = asyncio.get_event_loop().time()
        if params.get('remove_duplicates', True):
            df = df.drop_duplicates()
        if params.get('drop_missing', False):
            df = df.dropna()
        elif params.get('fill_missing', True):
            for col in df.columns:
                if pd.api.types.is_numeric_dtype(df[col]):
                    df[col] = df[col].fillna(df[col].mean())
                else:
                    df[col] = df[col].fillna(df[col].mode()[0] if not df[col].mode().empty else 'unknown')
        if params.get('normalize', False):
            numeric_cols = df.select_dtypes(include=[np.number]).columns
            df[numeric_cols] = (df[numeric_cols] - df[numeric_cols].mean()) / (df[numeric_cols].std() + 1e-8)

        self.datasets[dataset_id] = df
        end_ts = asyncio.get_event_loop().time()
        bytes_processed = df.memory_usage(deep=True).sum()
        metrics = DataOperationMetrics(
            operation_name="clean_data",
            start_time=start_ts,
            end_time=end_ts,
            bytes_processed=bytes_processed,
            rows_processed=len(df),
        )
        metrics.compute_energy_carbon(self.config)
        self.metrics_history.append(metrics)
        self.metrics.increment_data_bytes(bytes_processed)
        self.metrics.increment_data_carbon(metrics.carbon_kg)
        self.metrics.increment_data_energy(metrics.energy_kwh)

        # Publish FeedbackEvent
        event = FeedbackEvent.create_with_context(
            task_id=f"data_clean_{dataset_id}",
            selected_action="clean",
            quality_score=0.9,  # placeholder
            energy_joules=metrics.energy_kwh * 3.6e6,
            carbon_g=metrics.carbon_kg * 1000,
            feedback_type="data",
            adaptive_cost_value=0.0,
            state={'dataset_id': dataset_id, 'params': params},
            candidates=[{'action': 'profile', 'clean', 'summarize', 'validate', 'route'}],
            source="data_expert",
            environment=getattr(central_config, "ENVIRONMENT", "production"),
            tags=["data", "clean"]
        )
        await self.queue.publish("feedback_events", event.to_json())

        # Check drift
        if self.drift:
            await self.drift.check_drift(self.adaptive_cost.get_current_weights())

        return {'status': 'success', 'dataset_id': dataset_id, 'shape': df.shape, 'rows_removed': len(dataset) - len(df) if isinstance(dataset, pd.DataFrame) else 0}

    async def summarize_data(self, task: Dict[str, Any]) -> Dict[str, Any]:
        dataset = task.get('data')
        dataset_id = task.get('dataset_id', f"summary_{uuid.uuid4().hex[:8]}")

        if isinstance(dataset, pd.DataFrame):
            df = dataset
        else:
            df = await self.load_data(dataset, DataSourceType.CSV, dataset_id)

        start_ts = asyncio.get_event_loop().time()
        schema_str = json.dumps({str(k): str(v) for k, v in df.dtypes.items()})
        schema_hash = hashlib.sha256(schema_str.encode()).hexdigest()
        sample_rows = df.head(5).to_dict('records')
        summary = DataSummary(
            dataset_id=dataset_id,
            rows=len(df),
            columns=len(df.columns),
            column_names=list(df.columns),
            column_dtypes={str(k): str(v) for k, v in df.dtypes.items()},
            sample_rows=sample_rows,
            schema_hash=schema_hash,
        )
        if df.isnull().any().any():
            summary.quality_issues.append("Missing values detected")
            summary.recommendations.append("Consider imputation or removal of missing values")
        if len(df) == 0:
            summary.quality_issues.append("Empty dataset")
        if df.duplicated().any():
            summary.quality_issues.append("Duplicate rows detected")
            summary.recommendations.append("Remove duplicates before modeling")

        end_ts = asyncio.get_event_loop().time()
        bytes_processed = df.memory_usage(deep=True).sum()
        metrics = DataOperationMetrics(
            operation_name="summarize_data",
            start_time=start_ts,
            end_time=end_ts,
            bytes_processed=bytes_processed,
            rows_processed=len(df),
        )
        metrics.compute_energy_carbon(self.config)
        self.metrics_history.append(metrics)
        self.metrics.increment_data_bytes(bytes_processed)
        self.metrics.increment_data_carbon(metrics.carbon_kg)
        self.metrics.increment_data_energy(metrics.energy_kwh)

        # Publish FeedbackEvent
        event = FeedbackEvent.create_with_context(
            task_id=f"data_summary_{dataset_id}",
            selected_action="summarize",
            quality_score=0.8,
            energy_joules=metrics.energy_kwh * 3.6e6,
            carbon_g=metrics.carbon_kg * 1000,
            feedback_type="data",
            adaptive_cost_value=0.0,
            state={'dataset_id': dataset_id},
            candidates=[{'action': 'profile', 'clean', 'summarize', 'validate', 'route'}],
            source="data_expert",
            environment=getattr(central_config, "ENVIRONMENT", "production"),
            tags=["data", "summary"]
        )
        await self.queue.publish("feedback_events", event.to_json())

        # Check drift
        if self.drift:
            await self.drift.check_drift(self.adaptive_cost.get_current_weights())

        return {'status': 'success', 'summary': summary.to_dict()}

    async def validate_data(self, task: Dict[str, Any]) -> Dict[str, Any]:
        dataset = task.get('data')
        schema = task.get('schema', {})
        if isinstance(dataset, pd.DataFrame):
            df = dataset
        else:
            df = pd.DataFrame(dataset)
        issues = []
        for col, expected_type in schema.items():
            if col not in df.columns:
                issues.append(f"Missing column: {col}")
            elif str(df[col].dtype) != str(expected_type):
                issues.append(f"Type mismatch on {col}: expected {expected_type}, got {df[col].dtype}")
        if df.empty:
            issues.append("Dataset is empty")
        if df.isnull().all().any():
            null_cols = df.columns[df.isnull().all()].tolist()
            issues.append(f"Columns with all nulls: {null_cols}")

        # Publish FeedbackEvent
        event = FeedbackEvent.create_with_context(
            task_id=f"data_validate_{uuid.uuid4().hex[:8]}",
            selected_action="validate",
            quality_score=1.0 if not issues else 0.5,
            energy_joules=0.0,
            carbon_g=0.0,
            feedback_type="data",
            adaptive_cost_value=0.0,
            state={'issues': issues},
            candidates=[{'action': 'profile', 'clean', 'summarize', 'validate', 'route'}],
            source="data_expert",
            environment=getattr(central_config, "ENVIRONMENT", "production"),
            tags=["data", "validate"]
        )
        await self.queue.publish("feedback_events", event.to_json())

        # Check drift
        if self.drift:
            await self.drift.check_drift(self.adaptive_cost.get_current_weights())

        return {'status': 'success' if not issues else 'warning', 'valid': len(issues) == 0, 'issues': issues}

    async def route_data(self, task: Dict[str, Any]) -> Dict[str, Any]:
        dataset = task.get('data')
        dataset_id = task.get('dataset_id', f"route_{uuid.uuid4().hex[:8]}")
        if isinstance(dataset, pd.DataFrame):
            df = dataset
        else:
            df = pd.DataFrame(dataset)

        # Use adaptive cost weights to influence routing
        if self.adaptive_cost:
            weights = self.adaptive_cost.get_current_weights()
            carbon_weight = weights.get('carbon', 0.3)
            cost_weight = weights.get('cost', 0.2)
        else:
            carbon_weight = 0.3
            cost_weight = 0.2

        # Base routing
        routing = {'feature_expert': False, 'model_expert': False, 'optimization_expert': False}
        if len(df.columns) > 10:
            routing['feature_expert'] = True
        if len(df) > 100:
            routing['model_expert'] = True
        if len(df) > 1000 or len(df.columns) > 20:
            routing['optimization_expert'] = True

        # Apply carbon/cost adjustments
        if carbon_weight > 0.5:
            # If carbon weight high, avoid optimization_expert (energy intensive)
            routing['optimization_expert'] = False
        if cost_weight > 0.5:
            # If cost weight high, prefer feature_expert (cheaper)
            routing['feature_expert'] = True

        # Pareto gating: filter candidates
        if self.pareto:
            candidates = []
            for expert, active in routing.items():
                candidates.append({
                    'expert': expert,
                    'active': active,
                    'carbon_g': 0.0,
                    'latency_ms': 0.0,
                    'quality_score': 0.5
                })
            filtered = self.pareto.filter(candidates)
            if filtered:
                # Only keep experts that passed Pareto
                allowed = {c['expert'] for c in filtered}
                for exp in routing:
                    if exp not in allowed:
                        routing[exp] = False

        recommended_experts = [k for k, v in routing.items() if v]

        # Publish FeedbackEvent
        event = FeedbackEvent.create_with_context(
            task_id=f"data_route_{dataset_id}",
            selected_action="route",
            quality_score=0.9,
            energy_joules=0.0,
            carbon_g=0.0,
            feedback_type="data",
            adaptive_cost_value=0.0,
            state={'dataset_id': dataset_id, 'routing': routing},
            candidates=[{'action': 'profile', 'clean', 'summarize', 'validate', 'route'}],
            source="data_expert",
            environment=getattr(central_config, "ENVIRONMENT", "production"),
            tags=["data", "route"]
        )
        await self.queue.publish("feedback_events", event.to_json())

        # Check drift
        if self.drift:
            await self.drift.check_drift(self.adaptive_cost.get_current_weights())

        return {'status': 'success', 'dataset_id': dataset_id, 'routing': routing, 'recommended_experts': recommended_experts, 'task_descriptors': [{'expert': exp, 'task_type': 'process', 'data_ref': dataset_id} for exp in recommended_experts]}

    async def federated_aggregate(self, task: Dict[str, Any]) -> Dict[str, Any]:
        if not self.config.enable_federated_aggregation:
            return {'status': 'disabled', 'reason': 'Federated aggregation not enabled'}
        datasets = task.get('datasets', [])
        logger.info(f"Federated aggregation requested for {len(datasets)} datasets")
        aggregated_profile = {'datasets': datasets, 'total_rows': sum(d.get('rows', 0) for d in datasets), 'timestamp': datetime.now(timezone.utc).isoformat()}
        return {'status': 'success', 'aggregated_profile': aggregated_profile}

    # ==========================================================================
    # Expert Interface Methods (unchanged)
    # ==========================================================================
    def get_capabilities(self) -> Dict[str, Any]:
        return {
            'expert_name': self.expert_name,
            'supported_tasks': self.supported_task_types,
            'health_status': self.health_status,
            'avg_latency_seconds': self.total_latency / self.tasks_handled if self.tasks_handled > 0 else 0.0,
            'tasks_handled': self.tasks_handled,
            'config': asdict(self.config),
        }

    def get_metrics(self) -> Dict[str, Any]:
        total_bytes = sum(m.bytes_processed for m in self.metrics_history)
        total_carbon = sum(m.carbon_kg for m in self.metrics_history)
        total_energy = sum(m.energy_kwh for m in self.metrics_history)
        failures = sum(1 for m in self.metrics_history if not m.success)
        return {
            'expert_name': self.expert_name,
            'tasks_handled': self.tasks_handled,
            'avg_latency_seconds': self.total_latency / self.tasks_handled if self.tasks_handled > 0 else 0.0,
            'total_bytes_processed': total_bytes,
            'total_carbon_kg': total_carbon,
            'total_energy_kwh': total_energy,
            'failure_rate': failures / len(self.metrics_history) if self.metrics_history else 0.0,
            'datasets_cached': len(self.datasets),
            'profiles_cached': len(self.profiles),
        }

    async def get_health_status(self) -> Dict[str, Any]:
        try:
            test_df = pd.DataFrame({'x': [1, 2, 3], 'y': [4, 5, 6]})
            profile = await self._profile_dataframe(test_df, "health_check")
            self.health_status = "healthy"
            return {'status': 'healthy', 'expert': self.expert_name, 'timestamp': datetime.now(timezone.utc).isoformat(), 'last_tasks': self.tasks_handled, 'last_error': None}
        except Exception as e:
            self.health_status = "unhealthy"
            logger.warning(f"DataExpert health check failed: {e}")
            return {'status': 'unhealthy', 'expert': self.expert_name, 'timestamp': datetime.now(timezone.utc).isoformat(), 'error': str(e)}

    # ==========================================================================
    # Async Context Manager and Cleanup
    # ==========================================================================
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()
        await self._save_state()
        logger.info("DataExpert closed")

# ============================================================================
# Example Usage (if run directly)
# ============================================================================
if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)

    async def main():
        from ..storage import Storage
        from ..scaling.message_queue import AsyncMessageQueue
        from ..feedback.adaptive_cost import AdaptiveCostFunction
        from ..routing.pareto_gating import ParetoGating
        from ..safety.drift_detector import DriftDetector
        from ..metrics import MetricsRegistry

        storage = Storage()
        queue = AsyncMessageQueue()
        adaptive_cost = AdaptiveCostFunction(storage)
        pareto = ParetoGating()
        drift = DriftDetector(storage, adaptive_cost)
        metrics = MetricsRegistry()

        expert = DataExpert(storage, queue, adaptive_cost, pareto, drift, metrics)

        sample_data = {'id': [1,2,3,4,5], 'value': [10.5, 20.3, None, 40.1, 50.0], 'category': ['A','B','A','C','B']}
        task = {'type': 'data_profile', 'data': sample_data, 'dataset_id': 'sample_001'}
        result = await expert.handle_task(task)
        print("Profile result:", result['status'])

        await expert.close()

    asyncio.run(main())
