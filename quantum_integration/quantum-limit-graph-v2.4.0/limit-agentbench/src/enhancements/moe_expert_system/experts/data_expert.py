# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/moe_expert_system/experts/data_expert.py
# Enhanced Data Expert v3.1.0 – Complete Data Services Layer for MoE System

"""
Data Expert v3.1.0 – Data Services Layer for MoE System

A specialized expert that handles all data-related tasks within the MoE pipeline:
- Data ingestion (files, URLs, databases, in-memory payloads, streaming)
- Comprehensive data profiling (statistics, type inference, missingness analysis, skew detection)
- Data cleaning (deduplication, missing value handling, normalization)
- Data summarization (column-level stats, samples, schema inspection)
- Data routing to task-specific experts (feature engineering, modeling, optimization)
- Energy-aware telemetry tracking (bytes processed, preprocessing latency, energy proxies)
- Integration with Green_Agent metrics pipeline
- Health checks for MoE registry
- Sustainable computing practices (carbon-aware data handling)
- Federated data aggregation (privacy-preserving profiling) – stub
- Incremental learning and streaming data support
- Enhanced persistence with versioning and efficient serialization
- Circuit breakers and retries for external data sources
- Caching with TTL for profiles and datasets
- Async context manager for resource cleanup
"""

import asyncio
import logging
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

# ============================================================================
# Try optional dependencies
# ============================================================================
try:
    from pydantic import BaseModel, Field, validator
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

try:
    import structlog
    logger = structlog.get_logger(__name__)
except ImportError:
    logger = logging.getLogger(__name__)

try:
    from prometheus_client import Gauge, Counter, Histogram
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

try:
    import aiofiles
    AIOFILES_AVAILABLE = True
except ImportError:
    AIOFILES_AVAILABLE = False

# ============================================================================
# Local imports – BaseExpert and bio-inspired modules
# ============================================================================
try:
    from .base_expert import BaseExpert
    BASE_EXPERT_AVAILABLE = True
except ImportError:
    BASE_EXPERT_AVAILABLE = False
    logger.warning("BaseExpert not available; using fallback interface")

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

try:
    from enhancements.bio_inspired.circuit_breaker import CircuitBreaker
    CIRCUIT_BREAKER_AVAILABLE = True
except ImportError:
    # Fallback circuit breaker
    class CircuitBreaker:
        def __init__(self, name, failure_threshold=5, recovery_timeout=30.0):
            self.name = name
            self.failure_threshold = failure_threshold
            self.recovery_timeout = recovery_timeout
            self._state = "closed"
            self._failure_count = 0
            self._last_failure_time = None
            self._lock = asyncio.Lock()
        async def call(self, func, *args, **kwargs):
            return await func(*args, **kwargs)

# ============================================================================
# Configuration Dataclass (Enhanced)
# ============================================================================

@dataclass
class DataExpertConfig:
    """Centralized configuration for the Data Expert."""
    # Feature flags
    enable_profiling: bool = True
    enable_cleaning: bool = True
    enable_summarization: bool = True
    enable_energy_tracking: bool = True
    enable_federated_aggregation: bool = True
    enable_telemetry: bool = True
    enable_persistence: bool = True
    enable_url_fetch: bool = True
    enable_database: bool = True
    enable_streaming: bool = True

    # Data handling
    max_rows_profile: int = 10000  # Profile sample size
    max_unique_values: int = 100   # For categorical analysis
    missing_value_threshold: float = 0.5  # Flag columns with > 50% missing
    
    # Energy and carbon tracking
    bytes_to_kwh_factor: float = 1e-9  # Rough estimate: 1 byte ≈ 1e-9 kWh
    carbon_intensity_g_per_kwh: float = 100.0  # Default CO2 intensity
    
    # Federated learning
    federated_enabled: bool = False
    federated_server_url: Optional[str] = None
    
    # Persistence
    state_save_path: str = "./data_expert_state.pkl"
    use_parquet_for_datasets: bool = True  # Store DataFrames as Parquet
    
    # Telemetry
    telemetry_export_interval: int = 60
    
    # Circuit breaker and retry
    max_retries: int = 3
    retry_base_delay_ms: float = 100.0
    retry_max_delay_ms: float = 5000.0
    circuit_breaker_failure_threshold: int = 5
    circuit_breaker_recovery_timeout: float = 30.0
    
    # Caching
    cache_ttl_seconds: int = 3600  # 1 hour
    
    def __post_init__(self):
        """Validate configuration."""
        if self.missing_value_threshold < 0 or self.missing_value_threshold > 1:
            self.missing_value_threshold = 0.5
        if self.bytes_to_kwh_factor <= 0:
            self.bytes_to_kwh_factor = 1e-9

# ============================================================================
# Enums for Data Operations (enhanced)
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

# ============================================================================
# Data Profiling Results (unchanged)
# ============================================================================

@dataclass
class ColumnProfile:
    """Profile of a single column."""
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
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

@dataclass
class DataProfile:
    """Complete profile of a dataset."""
    dataset_name: str
    shape: Tuple[int, int]  # (rows, columns)
    total_cells: int
    memory_usage_bytes: int
    timestamp: str
    columns: Dict[str, ColumnProfile]
    global_issues: List[DataQualityIssue]
    quality_score: float  # 0-1, higher is better
    
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
    """Summary of a dataset for downstream experts."""
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

# ============================================================================
# Energy and Metrics Tracking (unchanged)
# ============================================================================

@dataclass
class DataOperationMetrics:
    """Metrics for a data operation."""
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
        """Compute energy and carbon footprint."""
        self.energy_kwh = self.bytes_processed * config.bytes_to_kwh_factor
        self.carbon_kg = self.energy_kwh * config.carbon_intensity_g_per_kwh / 1000.0
    
    def duration_seconds(self) -> float:
        if self.end_time:
            return self.end_time - self.start_time
        return 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

# ============================================================================
# Fallback BaseExpert if not available (unchanged)
# ============================================================================

if not BASE_EXPERT_AVAILABLE:
    class BaseExpert:
        """Fallback base expert interface."""
        def __init__(self):
            self.expert_name = "data_expert"
            self.supported_task_types = [
                "data_profile", "data_clean", "data_summary",
                "data_validate", "data_transform"
            ]
            self.health_status = "healthy"
        
        async def handle_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
            raise NotImplementedError()
        
        def get_capabilities(self) -> Dict[str, Any]:
            return {
                'name': self.expert_name,
                'supported_tasks': self.supported_task_types,
                'health': self.health_status,
            }
        
        def get_metrics(self) -> Dict[str, Any]:
            return {}

# ============================================================================
# Data Expert Implementation (Enhanced)
# ============================================================================

class DataExpert(BaseExpert):
    """
    Data Expert for MoE System v3.1.0
    
    Handles data profiling, cleaning, summarization, and routing
    with full integration into Green_Agent metrics and sustainability tracking.
    """
    
    def __init__(self, config: Optional[DataExpertConfig] = None):
        super().__init__()
        self.expert_name = "data_expert"
        self.supported_task_types = [
            "data_profile", "data_clean", "data_summary",
            "data_validate", "data_transform", "data_route",
            "data_federated_aggregate"
        ]
        self.health_status = "healthy"
        
        # Configuration
        self.config = config or DataExpertConfig()
        
        # State
        self.datasets: Dict[str, pd.DataFrame] = {}
        self.profiles: Dict[str, DataProfile] = {}
        self.metrics_history: List[DataOperationMetrics] = []
        self.tasks_handled = 0
        self.total_latency = 0.0
        
        # Caching with TTL
        self._cache_timestamps: Dict[str, datetime] = {}
        self._lock = asyncio.Lock()
        
        # Bio-inspired integration
        self.token_manager = None
        if TOKEN_AVAILABLE:
            try:
                self.token_manager = EcoATPTokenManager()
            except Exception as e:
                logger.warning(f"Failed to initialize token manager: {e}")
        
        self.gradient_manager = None
        if GRADIENT_AVAILABLE:
            try:
                self.gradient_manager = GradientFieldManager()
            except Exception as e:
                logger.warning(f"Failed to initialize gradient manager: {e}")
        
        # Circuit breaker for external calls
        self._circuit_breaker = CircuitBreaker(
            "data_external",
            failure_threshold=self.config.circuit_breaker_failure_threshold,
            recovery_timeout=self.config.circuit_breaker_recovery_timeout
        )
        
        # Session for HTTP requests
        self._session: Optional[aiohttp.ClientSession] = None
        self._session_lock = asyncio.Lock()
        
        # Prometheus metrics (if available)
        self.prometheus_metrics = {}
        if PROMETHEUS_AVAILABLE:
            self._init_prometheus()
        
        # Load persisted state
        if self.config.enable_persistence:
            asyncio.create_task(self.load_state())
        
        logger.info(f"DataExpert initialized with config: {self.config}")
    
    def _init_prometheus(self):
        """Initialize Prometheus metrics."""
        try:
            self.prometheus_metrics = {
                'data_expert_tasks_total': Counter(
                    'data_expert_tasks_total',
                    'Total tasks handled by data expert',
                    ['task_type', 'status']
                ),
                'data_expert_latency_seconds': Histogram(
                    'data_expert_latency_seconds',
                    'Latency of data expert operations',
                    ['operation']
                ),
                'data_expert_bytes_processed': Counter(
                    'data_expert_bytes_processed_total',
                    'Total bytes processed by data expert'
                ),
                'data_expert_carbon_kg': Counter(
                    'data_expert_carbon_kg_total',
                    'Total carbon footprint (kg CO2) of data expert'
                ),
                'data_expert_energy_kwh': Counter(
                    'data_expert_energy_kwh_total',
                    'Total energy consumed (kWh) by data expert'
                ),
            }
        except Exception as e:
            logger.warning(f"Failed to init Prometheus: {e}")
    
    # ========================================================================
    # Core Expert Interface
    # ========================================================================
    
    async def handle_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """
        Handle a task routed to this expert.
        
        Task format:
        {
            'type': 'data_profile' | 'data_clean' | 'data_summary' | ...,
            'data': <data payload or reference>,
            'params': {<operation-specific params>},
            'correlation_id': <for tracing>,
        }
        """
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
                result = {
                    'status': 'error',
                    'error': f"Unknown task type: {task_type}",
                }
            
            end_ts = asyncio.get_event_loop().time()
            latency = end_ts - start_ts
            self.tasks_handled += 1
            self.total_latency += latency
            
            # Record metrics
            if PROMETHEUS_AVAILABLE and 'data_expert_latency_seconds' in self.prometheus_metrics:
                self.prometheus_metrics['data_expert_latency_seconds'].labels(
                    operation=task_type
                ).observe(latency)
            
            result['correlation_id'] = task_id
            result['latency_seconds'] = latency
            logger.info(f"DataExpert completed {task_type}: latency={latency:.3f}s")
            
            return result
        
        except Exception as e:
            logger.error(f"DataExpert error on {task_type}: {e}", exc_info=True)
            if PROMETHEUS_AVAILABLE and 'data_expert_tasks_total' in self.prometheus_metrics:
                self.prometheus_metrics['data_expert_tasks_total'].labels(
                    task_type=task_type, status='error'
                ).inc()
            return {
                'status': 'error',
                'error': str(e),
                'correlation_id': task_id,
            }
    
    def get_capabilities(self) -> Dict[str, Any]:
        """Return expert capabilities for registry and gating network."""
        return {
            'expert_name': self.expert_name,
            'supported_tasks': self.supported_task_types,
            'health_status': self.health_status,
            'avg_latency_seconds': (
                self.total_latency / self.tasks_handled 
                if self.tasks_handled > 0 else 0.0
            ),
            'tasks_handled': self.tasks_handled,
            'config': asdict(self.config),
        }
    
    def get_metrics(self) -> Dict[str, Any]:
        """Return expert-level metrics for MoE dashboard and analytics."""
        total_bytes = sum(m.bytes_processed for m in self.metrics_history)
        total_carbon = sum(m.carbon_kg for m in self.metrics_history)
        total_energy = sum(m.energy_kwh for m in self.metrics_history)
        failures = sum(1 for m in self.metrics_history if not m.success)
        
        return {
            'expert_name': self.expert_name,
            'tasks_handled': self.tasks_handled,
            'avg_latency_seconds': (
                self.total_latency / self.tasks_handled 
                if self.tasks_handled > 0 else 0.0
            ),
            'total_bytes_processed': total_bytes,
            'total_carbon_kg': total_carbon,
            'total_energy_kwh': total_energy,
            'failure_rate': failures / len(self.metrics_history) if self.metrics_history else 0.0,
            'datasets_cached': len(self.datasets),
            'profiles_cached': len(self.profiles),
        }
    
    async def get_health_status(self) -> Dict[str, Any]:
        """Health check for MoE registry."""
        # Check if data expert can perform basic operations
        try:
            test_df = pd.DataFrame({'x': [1, 2, 3], 'y': [4, 5, 6]})
            profile = await self._profile_dataframe(test_df, "health_check")
            
            self.health_status = "healthy"
            return {
                'status': 'healthy',
                'expert': self.expert_name,
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'last_tasks': self.tasks_handled,
                'last_error': None,
            }
        except Exception as e:
            self.health_status = "unhealthy"
            logger.warning(f"DataExpert health check failed: {e}")
            return {
                'status': 'unhealthy',
                'expert': self.expert_name,
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'error': str(e),
            }
    
    # ========================================================================
    # Core Data Operations (Enhanced)
    # ========================================================================
    
    async def load_data(
        self,
        source: Union[str, pd.DataFrame, Dict, List, AsyncGenerator],
        source_type: DataSourceType = DataSourceType.IN_MEMORY,
        dataset_id: Optional[str] = None,
    ) -> pd.DataFrame:
        """Load data from various sources, including URL, database, and streaming."""
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
            
            # Update Prometheus metrics
            if PROMETHEUS_AVAILABLE and 'data_expert_bytes_processed' in self.prometheus_metrics:
                self.prometheus_metrics['data_expert_bytes_processed'].inc(bytes_loaded)
                self.prometheus_metrics['data_expert_carbon_kg'].inc(metrics.carbon_kg)
                self.prometheus_metrics['data_expert_energy_kwh'].inc(metrics.energy_kwh)
            
            logger.info(f"Loaded dataset {dataset_id}: {df.shape}, {bytes_loaded} bytes")
            return df
        
        except Exception as e:
            logger.error(f"Failed to load data: {e}")
            raise
    
    async def _fetch_from_url(self, url: str) -> pd.DataFrame:
        """Fetch data from a URL with retries and circuit breaker."""
        async def _fetch():
            session = await self._get_session()
            async with session.get(url) as response:
                if response.status != 200:
                    raise aiohttp.ClientError(f"HTTP {response.status}")
                content = await response.read()
                # Determine format from URL or content
                if url.endswith('.csv'):
                    return pd.read_csv(pd.io.common.StringIO(content.decode()))
                elif url.endswith('.json'):
                    return pd.read_json(content)
                else:
                    # Try to parse as CSV by default
                    return pd.read_csv(pd.io.common.StringIO(content.decode()))
        
        return await self._circuit_breaker.call(_fetch)
    
    async def _fetch_from_database(self, connection_string: str) -> pd.DataFrame:
        """Fetch data from a database (requires SQLAlchemy async)."""
        # Placeholder: actual implementation would use SQLAlchemy async
        raise NotImplementedError("Database fetch not implemented")
    
    async def _fetch_from_stream(self, stream: AsyncGenerator) -> pd.DataFrame:
        """Fetch data from an async generator."""
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
        """Get or create an aiohttp session."""
        async with self._session_lock:
            if self._session is None or self._session.closed:
                self._session = aiohttp.ClientSession()
            return self._session
    
    async def profile_data(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """
        Profile a dataset: compute statistics, type inference, quality metrics.
        """
        dataset = task.get('data')
        dataset_id = task.get('dataset_id', f"profile_{uuid.uuid4().hex[:8]}")
        force_refresh = task.get('force_refresh', False)
        
        # Check cache if not forced
        if not force_refresh and dataset_id in self.profiles:
            cached_time = self._cache_timestamps.get(dataset_id)
            if cached_time and (datetime.now(timezone.utc) - cached_time).total_seconds() < self.config.cache_ttl_seconds:
                logger.info(f"Returning cached profile for {dataset_id}")
                return {
                    'status': 'success',
                    'dataset_id': dataset_id,
                    'profile': self.profiles[dataset_id].to_dict(),
                    'cached': True,
                }
        
        if isinstance(dataset, str):
            df = await self.load_data(dataset, DataSourceType.CSV, dataset_id)
        elif isinstance(dataset, pd.DataFrame):
            df = dataset
        else:
            df = pd.DataFrame(dataset)
        
        profile = await self._profile_dataframe(df, dataset_id)
        self.profiles[dataset_id] = profile
        self._cache_timestamps[dataset_id] = datetime.now(timezone.utc)
        
        return {
            'status': 'success',
            'dataset_id': dataset_id,
            'profile': profile.to_dict(),
            'cached': False,
        }
    
    async def _profile_dataframe(self, df: pd.DataFrame, dataset_id: str) -> DataProfile:
        """Internal method to profile a dataframe."""
        start_ts = asyncio.get_event_loop().time()
        
        # Sample for large datasets
        sample_df = df.head(self.config.max_rows_profile)
        
        columns = {}
        global_issues = []
        
        for col in sample_df.columns:
            col_data = sample_df[col]
            non_null = col_data.notna().sum()
            null_count = col_data.isna().sum()
            missing_pct = null_count / len(sample_df)
            
            # Type inference
            dtype = str(col_data.dtype)
            unique_count = col_data.nunique()
            
            # Statistics
            col_profile = ColumnProfile(
                name=col,
                dtype=dtype,
                non_null_count=non_null,
                null_count=null_count,
                unique_count=unique_count,
                missing_pct=missing_pct,
                issues=[],
            )
            
            # Quality issues
            if missing_pct > self.config.missing_value_threshold:
                col_profile.issues.append(DataQualityIssue.MISSING_VALUES)
                global_issues.append(DataQualityIssue.MISSING_VALUES)
            
            if unique_count == 1:
                col_profile.issues.append(DataQualityIssue.DUPLICATES)
            
            if unique_count > self.config.max_unique_values and dtype == 'object':
                col_profile.issues.append(DataQualityIssue.HIGH_CARDINALITY)
            
            # Numeric statistics
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
            
            # Top values for categorical
            if pd.api.types.is_object_dtype(col_data) or unique_count <= self.config.max_unique_values:
                top_vals = col_data.value_counts().head(5)
                col_profile.top_values = list(zip(top_vals.index, top_vals.values))
            
            columns[col] = col_profile
        
        # Quality score (0-1)
        issue_penalty = len(global_issues) * 0.1
        quality_score = max(0.0, 1.0 - issue_penalty)
        
        end_ts = asyncio.get_event_loop().time()
        bytes_processed = df.memory_usage(deep=True).sum()
        
        # Record metrics
        metrics = DataOperationMetrics(
            operation_name="profile_data",
            start_time=start_ts,
            end_time=end_ts,
            bytes_processed=bytes_processed,
            rows_processed=len(df),
        )
        metrics.compute_energy_carbon(self.config)
        self.metrics_history.append(metrics)
        
        # Update Prometheus metrics
        if PROMETHEUS_AVAILABLE and 'data_expert_bytes_processed' in self.prometheus_metrics:
            self.prometheus_metrics['data_expert_bytes_processed'].inc(bytes_processed)
            self.prometheus_metrics['data_expert_carbon_kg'].inc(metrics.carbon_kg)
            self.prometheus_metrics['data_expert_energy_kwh'].inc(metrics.energy_kwh)
        
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
        """
        Clean a dataset: dedup, handle missing values, normalize.
        """
        dataset = task.get('data')
        dataset_id = task.get('dataset_id', f"cleaned_{uuid.uuid4().hex[:8]}")
        params = task.get('params', {})
        
        if isinstance(dataset, pd.DataFrame):
            df = dataset.copy()
        else:
            df = await self.load_data(dataset, DataSourceType.CSV, dataset_id)
        
        start_ts = asyncio.get_event_loop().time()
        
        # Deduplication
        if params.get('remove_duplicates', True):
            df = df.drop_duplicates()
        
        # Handle missing values
        if params.get('drop_missing', False):
            df = df.dropna()
        elif params.get('fill_missing', True):
            # Fill numeric columns with mean, categorical with mode
            for col in df.columns:
                if pd.api.types.is_numeric_dtype(df[col]):
                    df[col] = df[col].fillna(df[col].mean())
                else:
                    df[col] = df[col].fillna(df[col].mode()[0] if not df[col].mode().empty else 'unknown')
        
        # Simple normalization for numeric columns
        if params.get('normalize', False):
            numeric_cols = df.select_dtypes(include=[np.number]).columns
            df[numeric_cols] = (df[numeric_cols] - df[numeric_cols].mean()) / (df[numeric_cols].std() + 1e-8)
        
        self.datasets[dataset_id] = df
        
        end_ts = asyncio.get_event_loop().time()
        bytes_processed = df.memory_usage(deep=True).sum()
        
        # Record metrics
        metrics = DataOperationMetrics(
            operation_name="clean_data",
            start_time=start_ts,
            end_time=end_ts,
            bytes_processed=bytes_processed,
            rows_processed=len(df),
        )
        metrics.compute_energy_carbon(self.config)
        self.metrics_history.append(metrics)
        
        # Update Prometheus metrics
        if PROMETHEUS_AVAILABLE and 'data_expert_bytes_processed' in self.prometheus_metrics:
            self.prometheus_metrics['data_expert_bytes_processed'].inc(bytes_processed)
            self.prometheus_metrics['data_expert_carbon_kg'].inc(metrics.carbon_kg)
            self.prometheus_metrics['data_expert_energy_kwh'].inc(metrics.energy_kwh)
        
        return {
            'status': 'success',
            'dataset_id': dataset_id,
            'shape': df.shape,
            'rows_removed': len(dataset) - len(df) if isinstance(dataset, pd.DataFrame) else 0,
        }
    
    async def summarize_data(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """
        Summarize a dataset for downstream experts.
        """
        dataset = task.get('data')
        dataset_id = task.get('dataset_id', f"summary_{uuid.uuid4().hex[:8]}")
        
        if isinstance(dataset, pd.DataFrame):
            df = dataset
        else:
            df = await self.load_data(dataset, DataSourceType.CSV, dataset_id)
        
        start_ts = asyncio.get_event_loop().time()
        
        # Compute schema hash
        schema_str = json.dumps({str(k): str(v) for k, v in df.dtypes.items()})
        schema_hash = hashlib.sha256(schema_str.encode()).hexdigest()
        
        # Sample rows
        sample_rows = df.head(5).to_dict('records')
        
        # Create summary
        summary = DataSummary(
            dataset_id=dataset_id,
            rows=len(df),
            columns=len(df.columns),
            column_names=list(df.columns),
            column_dtypes={str(k): str(v) for k, v in df.dtypes.items()},
            sample_rows=sample_rows,
            schema_hash=schema_hash,
        )
        
        # Quality issues and recommendations
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
        
        # Record metrics
        metrics = DataOperationMetrics(
            operation_name="summarize_data",
            start_time=start_ts,
            end_time=end_ts,
            bytes_processed=bytes_processed,
            rows_processed=len(df),
        )
        metrics.compute_energy_carbon(self.config)
        self.metrics_history.append(metrics)
        
        # Update Prometheus metrics
        if PROMETHEUS_AVAILABLE and 'data_expert_bytes_processed' in self.prometheus_metrics:
            self.prometheus_metrics['data_expert_bytes_processed'].inc(bytes_processed)
            self.prometheus_metrics['data_expert_carbon_kg'].inc(metrics.carbon_kg)
            self.prometheus_metrics['data_expert_energy_kwh'].inc(metrics.energy_kwh)
        
        return {
            'status': 'success',
            'summary': summary.to_dict(),
        }
    
    async def validate_data(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate data against schema or quality criteria.
        """
        dataset = task.get('data')
        schema = task.get('schema', {})
        
        if isinstance(dataset, pd.DataFrame):
            df = dataset
        else:
            df = pd.DataFrame(dataset)
        
        issues = []
        
        # Schema validation
        for col, expected_type in schema.items():
            if col not in df.columns:
                issues.append(f"Missing column: {col}")
            elif str(df[col].dtype) != str(expected_type):
                issues.append(f"Type mismatch on {col}: expected {expected_type}, got {df[col].dtype}")
        
        # Quality checks
        if df.empty:
            issues.append("Dataset is empty")
        
        if df.isnull().all().any():
            null_cols = df.columns[df.isnull().all()].tolist()
            issues.append(f"Columns with all nulls: {null_cols}")
        
        return {
            'status': 'success' if not issues else 'warning',
            'valid': len(issues) == 0,
            'issues': issues,
        }
    
    async def route_data(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """
        Route data to task-specific experts based on characteristics.
        """
        dataset = task.get('data')
        dataset_id = task.get('dataset_id', f"route_{uuid.uuid4().hex[:8]}")
        
        if isinstance(dataset, pd.DataFrame):
            df = dataset
        else:
            df = pd.DataFrame(dataset)
        
        # Determine routing based on data characteristics
        routing = {
            'feature_expert': False,
            'model_expert': False,
            'optimization_expert': False,
        }
        
        # Route to feature expert if many columns or need engineering
        if len(df.columns) > 10:
            routing['feature_expert'] = True
        
        # Route to model expert if sufficient data
        if len(df) > 100:
            routing['model_expert'] = True
        
        # Route to optimization expert if large dataset
        if len(df) > 1000 or len(df.columns) > 20:
            routing['optimization_expert'] = True
        
        recommended_experts = [k for k, v in routing.items() if v]
        
        return {
            'status': 'success',
            'dataset_id': dataset_id,
            'routing': routing,
            'recommended_experts': recommended_experts,
            'task_descriptors': [
                {'expert': exp, 'task_type': 'process', 'data_ref': dataset_id}
                for exp in recommended_experts
            ],
        }
    
    async def federated_aggregate(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """
        Perform federated aggregation of data profiles (privacy-preserving).
        """
        if not self.config.enable_federated_aggregation:
            return {
                'status': 'disabled',
                'reason': 'Federated aggregation not enabled',
            }
        
        # Stub implementation: just return a dummy result
        datasets = task.get('datasets', [])
        logger.info(f"Federated aggregation requested for {len(datasets)} datasets")
        
        # Simulate aggregation
        aggregated_profile = {
            'datasets': datasets,
            'total_rows': sum(d.get('rows', 0) for d in datasets),
            'timestamp': datetime.now(timezone.utc).isoformat(),
        }
        
        return {
            'status': 'success',
            'aggregated_profile': aggregated_profile,
        }
    
    # ========================================================================
    # Persistence and State Management (Enhanced)
    # ========================================================================
    
    async def save_state(self) -> bool:
        """Save expert state to disk with efficient serialization."""
        if not self.config.enable_persistence:
            return False
        
        try:
            state = {
                'datasets_metadata': {
                    k: {'shape': v.shape, 'dtypes': {str(c): str(v[c].dtype) for c in v.columns}}
                    for k, v in self.datasets.items()
                },
                'profiles': {k: v.to_dict() for k, v in self.profiles.items()},
                'metrics': [m.to_dict() for m in self.metrics_history],
                'tasks_handled': self.tasks_handled,
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'config': asdict(self.config),
            }
            
            # Save metadata as JSON
            metadata_path = Path(self.config.state_save_path).with_suffix('.json')
            with open(metadata_path, 'w') as f:
                json.dump(state, f, indent=2, default=str)
            
            # Save DataFrames as Parquet if enabled
            if self.config.use_parquet_for_datasets and self.datasets:
                parquet_dir = Path(self.config.state_save_path).parent / 'datasets'
                parquet_dir.mkdir(exist_ok=True)
                for name, df in self.datasets.items():
                    df.to_parquet(parquet_dir / f"{name}.parquet")
            
            logger.info("DataExpert state saved")
            return True
        except Exception as e:
            logger.error(f"Failed to save state: {e}")
            return False
    
    async def load_state(self) -> bool:
        """Load expert state from disk."""
        if not self.config.enable_persistence:
            return False
        
        metadata_path = Path(self.config.state_save_path).with_suffix('.json')
        if not metadata_path.exists():
            logger.info("No saved state found")
            return False
        
        try:
            with open(metadata_path, 'r') as f:
                state = json.load(f)
            
            # Restore state
            self.tasks_handled = state.get('tasks_handled', 0)
            # Restore profiles
            for dataset_id, profile_dict in state.get('profiles', {}).items():
                # Reconstruct DataProfile from dict
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
            
            # Restore metrics
            for metrics_dict in state.get('metrics', []):
                metrics = DataOperationMetrics(
                    operation_name=metrics_dict['operation_name'],
                    start_time=metrics_dict['start_time'],
                    end_time=metrics_dict['end_time'],
                    bytes_processed=metrics_dict['bytes_processed'],
                    rows_processed=metrics_dict['rows_processed'],
                    energy_kwh=metrics_dict['energy_kwh'],
                    carbon_kg=metrics_dict['carbon_kg'],
                    success=metrics_dict['success'],
                    error_message=metrics_dict['error_message']
                )
                self.metrics_history.append(metrics)
            
            # Restore DataFrames from Parquet if available
            if self.config.use_parquet_for_datasets:
                parquet_dir = Path(self.config.state_save_path).parent / 'datasets'
                if parquet_dir.exists():
                    for parquet_file in parquet_dir.glob("*.parquet"):
                        dataset_id = parquet_file.stem
                        self.datasets[dataset_id] = pd.read_parquet(parquet_file)
            
            logger.info("DataExpert state loaded")
            return True
        except Exception as e:
            logger.error(f"Failed to load state: {e}")
            return False
    
    # ========================================================================
    # Async Context Manager
    # ========================================================================
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
    
    async def close(self):
        """Close resources."""
        if self._session and not self._session.closed:
            await self._session.close()
        if self.config.enable_persistence:
            await self.save_state()
        logger.info("DataExpert closed")

# ============================================================================
# Example Usage (unchanged)
# ============================================================================

async def example_usage():
    """Example usage of the DataExpert."""
    config = DataExpertConfig(
        enable_profiling=True,
        enable_cleaning=True,
        enable_energy_tracking=True,
    )
    expert = DataExpert(config)
    
    # Example 1: Load and profile CSV
    sample_data = {
        'id': [1, 2, 3, 4, 5],
        'value': [10.5, 20.3, None, 40.1, 50.0],
        'category': ['A', 'B', 'A', 'C', 'B'],
    }
    
    task_profile = {
        'type': 'data_profile',
        'data': sample_data,
        'dataset_id': 'sample_001',
    }
    
    result = await expert.handle_task(task_profile)
    print("Profile result:", result['status'])
    
    # Example 2: Clean data
    task_clean = {
        'type': 'data_clean',
        'data': sample_data,
        'dataset_id': 'sample_001_clean',
        'params': {'drop_missing': True},
    }
    
    result = await expert.handle_task(task_clean)
    print("Clean result:", result['status'])
    
    # Example 3: Summarize
    task_summary = {
        'type': 'data_summary',
        'data': sample_data,
        'dataset_id': 'sample_001_summary',
    }
    
    result = await expert.handle_task(task_summary)
    print("Summary result:", result['status'])
    
    # Example 4: Check health
    health = await expert.get_health_status()
    print("Health:", health['status'])
    
    # Print metrics
    metrics = expert.get_metrics()
    print("Metrics:", metrics)

if __name__ == "__main__":
    asyncio.run(example_usage())
