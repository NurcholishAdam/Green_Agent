"""
TimeTickEngine v3.4 – Enhanced simulation driver with evolutionary MOPD support.

Supports:
- CSV data loading with validation and configurable date column.
- Live data feed integration (via callback or async generator).
- Interpolation methods: linear, quadratic, spline, time‑based.
- Checkpoint saving/loading to resume simulations.
- Metrics collection (total harvested, average efficiency, etc.).
- Graceful stop via stop() method.
- Async context manager.
- **Multi‑policy simulation** for Pareto front generation.
- **Evolutionary optimization** of policies using NSGA‑II.
- **Dynamic objective weighting** based on system state.
- **Persistence of Pareto fronts** in checkpoints.
- **Parallel policy evaluation** for speed.
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, Callable, Union, List, Protocol, Awaitable, Tuple
from dataclasses import dataclass, field, asdict
import json
import os
import pickle
import glob
from pathlib import Path
import math
import hashlib
import random
import copy

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

# ============================================================================
# Try importing optional dependencies
# ============================================================================
try:
    from pydantic import BaseModel, Field, validator, root_validator
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False

# ============================================================================
# Configuration (Pydantic or dataclass) – Enhanced with MOPD
# ============================================================================
if PYDANTIC_AVAILABLE:
    class MOPDConfig(BaseModel):
        """Configuration for Multi‑Objective Pareto Decision."""
        enabled: bool = Field(True, description="Enable MOPD‑aware multi‑policy simulation")
        objective_weights: Dict[str, float] = Field(
            default_factory=lambda: {
                'total_harvested': 0.3,
                'avg_efficiency': 0.3,
                'carbon_saved': 0.2,
                'helium_saved': 0.2,
            },
            description="Weights for scalarising Pareto front (must sum to 1)"
        )
        grid_resolution: int = Field(5, description="Number of discrete points for sampling (unused for now)")
        population_size: int = Field(20, ge=4)
        generations: int = Field(5, ge=1)
        mutation_rate: float = Field(0.2, ge=0.0, le=1.0)
        crossover_rate: float = Field(0.8, ge=0.0, le=1.0)
        tournament_size: int = Field(3, ge=2)
        dynamic_weights: bool = Field(True, description="Enable dynamic weighting based on system state")

        @validator('objective_weights')
        def check_weights(cls, v):
            total = sum(v.values())
            if abs(total - 1.0) > 1e-6:
                raise ValueError("objective_weights must sum to 1")
            return v

    class TimeTickConfig(BaseModel):
        """Configuration for TimeTickEngine."""
        data_source: str = Field(..., description="Path to CSV or 'live' for real‑time.")
        csv_path: Optional[str] = Field(None, description="Path to CSV file (if data_source='csv').")
        date_column: str = Field("date", description="Name of the date column in CSV.")
        date_format: Optional[str] = Field(None, description="Date format for parsing (e.g., '%Y-%m-%d').")
        value_columns: List[str] = Field(default_factory=lambda: ["helium_supply", "helium_demand"],
                                         description="Columns to interpolate.")
        start_date: Optional[str] = Field(None, description="Start date for simulation (YYYY-MM-DD).")
        end_date: Optional[str] = Field(None, description="End date for simulation (YYYY-MM-DD).")
        interpolation_method: str = Field("linear", description="Interpolation method: linear, quadratic, spline, time.")
        tick_interval_seconds: float = Field(0.1, ge=0.001, description="Delay between ticks.")
        checkpoint_dir: str = Field("./checkpoints", description="Directory for checkpoint files.")
        enable_checkpointing: bool = True
        checkpoint_interval: int = Field(100, ge=1, description="Save checkpoint every N ticks.")
        metrics_enabled: bool = True
        max_checkpoints: int = Field(5, ge=1, description="Maximum number of checkpoint files to keep.")
        # Live data settings
        live_fetch_interval: float = Field(1.0, ge=0.1, description="Interval (seconds) to fetch live data.")
        live_data_callback: Optional[Callable[[], Awaitable[Dict[str, float]]]] = Field(
            None, description="Async callback to fetch live data."
        )
        # Custom metrics storage limit
        max_custom_metrics_entries: int = Field(1000, ge=1, description="Maximum number of custom metric entries to keep.")
        # MOPD configuration
        mopd: MOPDConfig = Field(default_factory=MOPDConfig, description="MOPD sub‑configuration")

        @validator('interpolation_method')
        def validate_interpolation(cls, v):
            allowed = {'linear', 'quadratic', 'spline', 'time'}
            if v not in allowed:
                raise ValueError(f'interpolation_method must be one of {allowed}')
            return v

        @validator('data_source')
        def validate_data_source(cls, v):
            if v not in ['csv', 'live']:
                raise ValueError('data_source must be "csv" or "live"')
            return v

        @root_validator
        def validate_live_source(cls, values):
            if values.get('data_source') == 'live' and not values.get('live_data_callback'):
                raise ValueError('live_data_callback is required when data_source="live"')
            return values
else:
    @dataclass
    class MOPDConfig:
        enabled: bool = True
        objective_weights: Dict[str, float] = field(default_factory=lambda: {
            'total_harvested': 0.3,
            'avg_efficiency': 0.3,
            'carbon_saved': 0.2,
            'helium_saved': 0.2,
        })
        grid_resolution: int = 5
        population_size: int = 20
        generations: int = 5
        mutation_rate: float = 0.2
        crossover_rate: float = 0.8
        tournament_size: int = 3
        dynamic_weights: bool = True

    @dataclass
    class TimeTickConfig:
        data_source: str = "csv"
        csv_path: Optional[str] = None
        date_column: str = "date"
        date_format: Optional[str] = None
        value_columns: List[str] = field(default_factory=lambda: ["helium_supply", "helium_demand"])
        start_date: Optional[str] = None
        end_date: Optional[str] = None
        interpolation_method: str = "linear"
        tick_interval_seconds: float = 0.1
        checkpoint_dir: str = "./checkpoints"
        enable_checkpointing: bool = True
        checkpoint_interval: int = 100
        metrics_enabled: bool = True
        max_checkpoints: int = 5
        live_fetch_interval: float = 1.0
        live_data_callback: Optional[Callable] = None
        max_custom_metrics_entries: int = 1000
        mopd: MOPDConfig = field(default_factory=MOPDConfig)

# ============================================================================
# Protocols for loose coupling
# ============================================================================
class HarvesterProtocol(Protocol):
    """Protocol for the Photosynthetic Harvester."""
    async def harvest_cycle(self, environmental_data: Dict[str, float]) -> Dict[str, Any]: ...
    def set_mode(self, mode: Any) -> None: ...
    async def get_harvesting_stats(self) -> Dict[str, Any]: ...
    def restore_state(self, state: Dict[str, Any]) -> None: ...
    def set_parameters(self, params: Dict[str, Any]) -> None: ...  # optional, for fine-tuning

class TranslatorProtocol(Protocol):
    """Protocol for translating CSV rows to harvester input."""
    @staticmethod
    def translate_row(row: pd.Series) -> Dict[str, float]: ...

# ============================================================================
# Simulation State for checkpointing (extended)
# ============================================================================
@dataclass
class SimulationState:
    """Serializable state for resuming simulation."""
    current_index: int
    current_date: str
    total_harvested: float
    harvest_cycles: int
    metrics: Dict[str, Any]  # summary from MetricsCollector
    metrics_data: Dict[str, Any]  # full metrics for restoration (custom metrics, etc.)
    harvester_state: Optional[Dict[str, Any]] = None
    data_hash: Optional[str] = None  # hash of the data file for validation
    timestamp: str
    # MOPD additions
    pareto_front: Optional[List[Dict[str, Any]]] = None  # serialised MOPDPoint list
    current_policy_id: Optional[str] = None  # if multi‑policy, which one is active

# ============================================================================
# MOPD Data Classes
# ============================================================================
@dataclass
class MOPDPoint:
    """Represents a single policy with its objective values."""
    policy_id: str
    # Decision variables (can be extended)
    harvester_mode: str
    parameters: Dict[str, Any] = field(default_factory=dict)
    # Objectives (to be maximised)
    total_harvested: float
    avg_efficiency: float
    carbon_saved: float
    helium_saved: float
    # Scalarised score (computed later)
    scalarised_score: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        return d

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'MOPDPoint':
        return cls(**data)

@dataclass
class Policy:
    """A policy defines a simulation scenario with a harvester mode and tunable parameters."""
    policy_id: str
    harvester_mode: str
    parameters: Dict[str, Any] = field(default_factory=dict)  # additional parameters to set on harvester

# ============================================================================
# Metrics Collector (extensible with capped storage)
# ============================================================================
class MetricsCollector:
    """Collect and aggregate simulation metrics with capped storage for custom metrics."""
    def __init__(self, max_custom_entries: int = 1000):
        self.total_harvested = 0.0
        self.harvest_cycles = 0
        self.efficiencies: List[float] = []
        self.modes: List[str] = []
        self.timestamps: List[datetime] = []
        self.custom_metrics: Dict[str, List[Any]] = {}
        self._max_custom_entries = max_custom_entries

    def record(self, result: Dict[str, Any]):
        self.total_harvested += result.get('eco_atp_generated', 0)
        self.harvest_cycles += 1
        self.efficiencies.append(result.get('efficiency', 0))
        self.modes.append(result.get('mode', 'unknown'))
        self.timestamps.append(datetime.now())

        for key, value in result.items():
            if key not in ['eco_atp_generated', 'efficiency', 'mode']:
                if key not in self.custom_metrics:
                    self.custom_metrics[key] = []
                self.custom_metrics[key].append(value)
                if len(self.custom_metrics[key]) > self._max_custom_entries:
                    self.custom_metrics[key] = self.custom_metrics[key][-self._max_custom_entries:]

    def get_summary(self) -> Dict[str, Any]:
        summary = {
            'total_harvested': self.total_harvested,
            'harvest_cycles': self.harvest_cycles,
            'avg_efficiency': np.mean(self.efficiencies) if self.efficiencies else 0,
            'max_efficiency': max(self.efficiencies) if self.efficiencies else 0,
            'mode_counts': {mode: self.modes.count(mode) for mode in set(self.modes)},
            'duration_hours': (self.timestamps[-1] - self.timestamps[0]).total_seconds() / 3600 if self.timestamps else 0
        }
        for key, values in self.custom_metrics.items():
            if values:
                recent = values[-self._max_custom_entries:]
                summary[f'avg_{key}'] = np.mean(recent)
                summary[f'min_{key}'] = np.min(recent)
                summary[f'max_{key}'] = np.max(recent)
        return summary

    def to_dict(self) -> Dict[str, Any]:
        return {
            'total_harvested': self.total_harvested,
            'harvest_cycles': self.harvest_cycles,
            'efficiencies': self.efficiencies,
            'modes': self.modes,
            'timestamps': [ts.isoformat() for ts in self.timestamps],
            'custom_metrics': self.custom_metrics,
            'max_custom_entries': self._max_custom_entries,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'MetricsCollector':
        collector = cls(max_custom_entries=data.get('max_custom_entries', 1000))
        collector.total_harvested = data.get('total_harvested', 0.0)
        collector.harvest_cycles = data.get('harvest_cycles', 0)
        collector.efficiencies = data.get('efficiencies', [])
        collector.modes = data.get('modes', [])
        collector.timestamps = [datetime.fromisoformat(ts) for ts in data.get('timestamps', [])]
        collector.custom_metrics = data.get('custom_metrics', {})
        return collector

# ============================================================================
# Live Data Feed (unchanged)
# ============================================================================
class LiveDataFeed:
    """Handles fetching live data via a callback or async generator."""
    def __init__(self, config: TimeTickConfig):
        self.config = config
        self._callback = config.live_data_callback
        self._running = False
        self._last_data: Optional[Dict[str, float]] = None
        self._backoff = 0.5

    async def fetch(self) -> Dict[str, float]:
        if self._callback:
            try:
                data = await self._callback()
                if data is not None:
                    self._last_data = data
                    self._backoff = 0.5
                    return data
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error("Live data callback failed: %s", e)
                self._backoff = min(self._backoff * 2, 30.0)
                await asyncio.sleep(self._backoff)
        if self._last_data is None:
            return {col: 0.5 for col in self.config.value_columns}
        return self._last_data

    async def run(self):
        self._running = True
        while self._running:
            try:
                await self.fetch()
                await asyncio.sleep(self.config.live_fetch_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Live feed run error: %s", e)
                await asyncio.sleep(self._backoff)
        self._running = False
        logger.info("Live data feed stopped.")

    def stop(self):
        self._running = False

# ============================================================================
# Enhanced TimeTickEngine (with Evolutionary MOPD)
# ============================================================================
class TimeTickEngine:
    """
    Enhanced simulation driver with evolutionary MOPD support.
    """

    def __init__(self,
                 harvester: HarvesterProtocol,
                 translator: Union[TranslatorProtocol, Callable],
                 config: Optional[Union[TimeTickConfig, Dict[str, Any]]] = None):
        """
        Initialize the TimeTickEngine.

        Args:
            harvester: Harvester instance (must implement harvest_cycle).
            translator: Translator class/instance with a translate_row method or a callable.
            config: Configuration dictionary or TimeTickConfig instance.
        """
        self.harvester = harvester
        self.translator = translator

        if not (callable(translator) or hasattr(translator, 'translate_row')):
            raise ValueError("translator must be a callable or have a translate_row method")

        if isinstance(config, dict):
            if PYDANTIC_AVAILABLE:
                self.config = TimeTickConfig(**config)
            else:
                self.config = TimeTickConfig(**config)
        elif isinstance(config, TimeTickConfig):
            self.config = config
        else:
            self.config = TimeTickConfig(data_source="csv", csv_path="helium_data.csv")

        # Internal state
        self.daily_df: Optional[pd.DataFrame] = None
        self.metrics = MetricsCollector(max_custom_entries=self.config.max_custom_metrics_entries)
        self._running = False
        self._stop_event = asyncio.Event()
        self._current_index = 0
        self._checkpoint_path = None
        self._live_feed: Optional[LiveDataFeed] = None
        self._data_hash: Optional[str] = None

        # MOPD state
        self._mopd_results: Dict[str, Dict[str, Any]] = {}  # policy_id -> metrics summary
        self._pareto_front: List[MOPDPoint] = []
        self._policy_cache: Dict[Tuple[str, frozenset], MOPDPoint] = {}  # for caching evaluations

        # Ensure checkpoint directory exists
        if self.config.enable_checkpointing:
            Path(self.config.checkpoint_dir).mkdir(parents=True, exist_ok=True)

        logger.info("TimeTickEngine initialized with config: %s", self.config)

    # ============================================================================
    # Data Loading (unchanged)
    # ============================================================================
    async def load_data(self, csv_path: Optional[str] = None):
        if self.config.data_source == 'live':
            self._live_feed = LiveDataFeed(self.config)
            logger.info("Live data feed initialized.")
            return

        path = csv_path or self.config.csv_path
        if not path:
            raise ValueError("CSV path not provided.")

        logger.info("Loading CSV from %s", path)

        try:
            with open(path, 'rb') as f:
                self._data_hash = hashlib.md5(f.read()).hexdigest()
        except Exception as e:
            logger.warning("Could not compute data hash for checkpoint validation: %s", e)
            self._data_hash = None

        try:
            df = pd.read_csv(path)
        except Exception as e:
            logger.error("Failed to read CSV: %s", e)
            raise

        required = [self.config.date_column] + self.config.value_columns
        missing = [col for col in required if col not in df.columns]
        if missing:
            raise ValueError(f"Missing columns in CSV: {missing}")

        try:
            if self.config.date_format:
                df[self.config.date_column] = pd.to_datetime(df[self.config.date_column],
                                                            format=self.config.date_format)
            else:
                df[self.config.date_column] = pd.to_datetime(df[self.config.date_column])
        except Exception as e:
            raise ValueError(f"Failed to parse date column '{self.config.date_column}': {e}")

        df = df.sort_values(self.config.date_column)

        if self.config.start_date:
            start = pd.to_datetime(self.config.start_date)
            df = df[df[self.config.date_column] >= start]
        if self.config.end_date:
            end = pd.to_datetime(self.config.end_date)
            df = df[df[self.config.date_column] <= end]

        self.df_monthly = df
        self._interpolate_daily()

        logger.info("Loaded %d monthly rows, interpolated to %d daily ticks.",
                    len(self.df_monthly), len(self.daily_df))

    def _interpolate_daily(self):
        if self.df_monthly.empty:
            logger.warning("No data after filtering; daily DataFrame will be empty.")
            self.daily_df = pd.DataFrame(columns=['date'] + self.config.value_columns)
            return

        df_monthly = self.df_monthly.set_index(self.config.date_column)
        daily_index = pd.date_range(
            start=df_monthly.index.min(),
            end=df_monthly.index.max(),
            freq='D'
        )

        numeric_cols = [col for col in self.config.value_columns if col in df_monthly.columns]

        try:
            if self.config.interpolation_method == 'linear':
                self.daily_df = df_monthly[numeric_cols].reindex(daily_index).interpolate(method='linear')
            elif self.config.interpolation_method == 'quadratic':
                self.daily_df = df_monthly[numeric_cols].reindex(daily_index).interpolate(method='quadratic')
            elif self.config.interpolation_method == 'spline':
                try:
                    self.daily_df = df_monthly[numeric_cols].reindex(daily_index).interpolate(method='spline')
                except Exception as e:
                    logger.warning("Spline interpolation failed (%s), falling back to linear.", e)
                    self.daily_df = df_monthly[numeric_cols].reindex(daily_index).interpolate(method='linear')
            elif self.config.interpolation_method == 'time':
                self.daily_df = df_monthly[numeric_cols].reindex(daily_index).interpolate(method='time')
            else:
                raise ValueError(f"Unsupported interpolation method: {self.config.interpolation_method}")
        except Exception as e:
            logger.error("Interpolation failed: %s", e)
            raise

        self.daily_df = self.daily_df.reset_index()
        self.daily_df.rename(columns={'index': 'date'}, inplace=True)
        self.daily_df = self.daily_df.fillna(method='ffill').fillna(method='bfill')

    # ============================================================================
    # Single Policy Simulation (original)
    # ============================================================================
    async def run_simulation(self,
                             start_index: Optional[int] = None,
                             post_tick_callback: Optional[Callable[[int, pd.Series, Dict[str, Any]], Awaitable[None]]] = None):
        """
        Run the simulation over all daily ticks, optionally resuming from a checkpoint.
        This runs a single policy (the current harvester configuration).
        """
        if self.config.data_source == 'csv' and self.daily_df is None:
            raise RuntimeError("Data not loaded. Call load_data() first.")

        if start_index is not None:
            self._current_index = start_index
        else:
            if self.config.enable_checkpointing:
                self._load_checkpoint()

        self._stop_event.clear()
        self._running = True

        if self.config.data_source == 'live' and self._live_feed:
            live_task = asyncio.create_task(self._live_feed.run())
        else:
            live_task = None

        total_ticks = len(self.daily_df) if self.config.data_source == 'csv' else None

        logger.info("Starting simulation from index %d", self._current_index)

        pbar = None
        if TQDM_AVAILABLE and self.config.data_source == 'csv' and total_ticks:
            pbar = tqdm(total=total_ticks, initial=self._current_index, desc="Simulating")

        try:
            while self._running and not self._stop_event.is_set():
                if self.config.data_source == 'csv':
                    if self._current_index >= total_ticks:
                        logger.info("Reached end of data.")
                        break
                    row = self.daily_df.iloc[self._current_index]
                    if pbar:
                        pbar.update(1)
                else:
                    if self._live_feed:
                        data = await self._live_feed.fetch()
                        row = pd.Series({'date': datetime.now()})
                        for k, v in data.items():
                            row[k] = v
                    else:
                        logger.error("No live data feed available.")
                        break

                env_data = self._translate_row(row)
                if env_data is None:
                    if self.config.data_source == 'csv':
                        self._current_index += 1
                    continue

                result = await self.harvester.harvest_cycle(env_data)

                if self.config.metrics_enabled:
                    self.metrics.record(result)

                if post_tick_callback:
                    try:
                        if asyncio.iscoroutinefunction(post_tick_callback):
                            await post_tick_callback(self._current_index, row, result)
                        else:
                            post_tick_callback(self._current_index, row, result)
                    except Exception as e:
                        logger.error("Post-tick callback failed: %s", e)

                if self.config.data_source == 'csv' and self._current_index % 30 == 0:
                    logger.info("Day %d: harvested %.2f Eco‑ATP",
                                self._current_index, result.get('eco_atp_generated', 0))

                if self.config.enable_checkpointing and self.config.data_source == 'csv':
                    if self._current_index % self.config.checkpoint_interval == 0:
                        await self._save_checkpoint(self._current_index)

                await asyncio.sleep(self.config.tick_interval_seconds)

                if self.config.data_source == 'csv':
                    self._current_index += 1

        except asyncio.CancelledError:
            logger.info("Simulation cancelled.")
            self._running = False
            if self.config.enable_checkpointing and self.config.data_source == 'csv':
                await self._save_checkpoint(self._current_index)
            raise

        except Exception as e:
            logger.error("Simulation failed at index %d: %s", self._current_index, e)
            self._running = False
            raise

        finally:
            if pbar:
                pbar.close()
            self._running = False
            self._stop_event.set()
            if live_task:
                live_task.cancel()
                await asyncio.gather(live_task, return_exceptions=True)
            if self._live_feed:
                self._live_feed.stop()
            logger.info("Simulation finished. Total harvested: %.2f", self.metrics.total_harvested)

    def stop(self):
        self._stop_event.set()
        self._running = False
        logger.info("Stop requested.")

    def _translate_row(self, row: pd.Series) -> Optional[Dict[str, float]]:
        try:
            if callable(self.translator):
                return self.translator(row)
            elif hasattr(self.translator, 'translate_row'):
                return self.translator.translate_row(row)
            else:
                raise TypeError("translator is not a callable nor has translate_row method")
        except Exception as e:
            logger.error("Row translation failed: %s", e)
            return None

    # ============================================================================
    # Enhanced MOPD: Policy Evaluation and Evolutionary Optimization
    # ============================================================================
    async def evaluate_policy(self, policy: Policy) -> MOPDPoint:
        """
        Run a full simulation for a given policy and return the resulting MOPDPoint.
        The harvester's mode and additional parameters are set temporarily.
        """
        # Cache check
        params_key = (policy.harvester_mode, frozenset(policy.parameters.items()))
        if params_key in self._policy_cache:
            return self._policy_cache[params_key]

        # Save original state
        original_mode = getattr(self.harvester, 'mode', None)
        original_params = {}
        if hasattr(self.harvester, 'get_parameters'):
            try:
                original_params = self.harvester.get_parameters()
            except:
                pass

        try:
            # Apply policy
            if hasattr(self.harvester, 'set_mode'):
                self.harvester.set_mode(policy.harvester_mode)
            if policy.parameters and hasattr(self.harvester, 'set_parameters'):
                self.harvester.set_parameters(policy.parameters)

            # Reset metrics for this policy
            self.metrics = MetricsCollector(max_custom_entries=self.config.max_custom_metrics_entries)

            # Run simulation from start
            await self.run_simulation(start_index=0)

            # Extract objectives
            summary = self.metrics.get_summary()
            # For demonstration, carbon_saved and helium_saved are derived from custom metrics if available
            carbon_saved = summary.get('avg_carbon_impact', 0.0)
            helium_saved = summary.get('avg_helium_usage', 0.0)

            point = MOPDPoint(
                policy_id=policy.policy_id,
                harvester_mode=policy.harvester_mode,
                parameters=policy.parameters,
                total_harvested=summary['total_harvested'],
                avg_efficiency=summary['avg_efficiency'],
                carbon_saved=carbon_saved,
                helium_saved=helium_saved,
            )
            self._policy_cache[params_key] = point
            return point

        except Exception as e:
            logger.error("Policy %s evaluation failed: %s", policy.policy_id, e)
            raise
        finally:
            # Restore original state
            if hasattr(self.harvester, 'set_mode') and original_mode is not None:
                self.harvester.set_mode(original_mode)
            if original_params and hasattr(self.harvester, 'set_parameters'):
                self.harvester.set_parameters(original_params)

    async def run_multi_policy_simulation(
        self,
        policies: List[Policy],
        post_policy_callback: Optional[Callable[[Policy, Dict[str, Any]], Awaitable[None]]] = None
    ) -> List[MOPDPoint]:
        """
        Run simulations for a list of policies sequentially (or in parallel if possible)
        and return the Pareto front.
        This method is kept for backward compatibility.
        """
        if not self.config.mopd.enabled:
            logger.warning("MOPD is disabled; no Pareto front will be generated.")
            return []

        if self.config.data_source == 'csv' and self.daily_df is None:
            raise RuntimeError("Data not loaded. Call load_data() first.")

        self._mopd_results = {}
        self._pareto_front = []

        logger.info("Evaluating %d policies...", len(policies))
        points = []
        # Parallel evaluation (if possible)
        eval_tasks = [self.evaluate_policy(p) for p in policies]
        results = await asyncio.gather(*eval_tasks, return_exceptions=True)
        for p, res in zip(policies, results):
            if isinstance(res, Exception):
                logger.error("Policy %s failed: %s", p.policy_id, res)
                continue
            points.append(res)

        # Update results map for compatibility
        for point in points:
            self._mopd_results[point.policy_id] = {
                'metrics': self.metrics.get_summary(),  # placeholder
                'point': point,
            }

        # Generate Pareto front
        self._pareto_front = self._filter_pareto(points)
        best_plan = self._select_best_from_pareto(self._pareto_front)
        if best_plan:
            logger.info("Best policy: %s with scalarised score %.3f",
                        best_plan.policy_id, best_plan.scalarised_score)

        # Save checkpoint
        if self.config.enable_checkpointing:
            await self._save_checkpoint(self._current_index, pareto_front=self._pareto_front)

        # Telemetry
        if self.config.metrics_enabled:
            logger.info("MOPD generation: %d policies, Pareto front size: %d",
                        len(policies), len(self._pareto_front))

        return self._pareto_front

    # ---------- New: Evolutionary Optimization ----------
    async def run_evolution(self, policy_space: Dict[str, Any] = None):
        """
        Evolve policies using NSGA-II over the specified parameter space.

        Args:
            policy_space: Dictionary describing the parameter space for policies.
                          Keys are parameter names (e.g., 'harvester_mode', 'conversion_factor',
                          'repair_rate'). Values are either a list of discrete choices or a tuple
                          (low, high) for continuous parameters.

        Returns:
            The Pareto front of evolved policies as a list of MOPDPoint.
        """
        if not self.config.mopd.enabled:
            logger.warning("MOPD is disabled; cannot run evolution.")
            return []

        if self.config.data_source == 'csv' and self.daily_df is None:
            raise RuntimeError("Data not loaded. Call load_data() first.")

        # Create an optimizer instance
        optimizer = NSGAIIOptimizer(
            engine=self,
            policy_space=policy_space,
            population_size=self.config.mopd.population_size,
            generations=self.config.mopd.generations,
            mutation_rate=self.config.mopd.mutation_rate,
            crossover_rate=self.config.mopd.crossover_rate,
            tournament_size=self.config.mopd.tournament_size,
            dynamic_weights=self.config.mopd.dynamic_weights,
            objective_weights=self.config.mopd.objective_weights,
        )
        pareto = await optimizer.evolve()
        self._pareto_front = pareto

        # Save checkpoint
        if self.config.enable_checkpointing:
            await self._save_checkpoint(self._current_index, pareto_front=self._pareto_front)

        return pareto

    # ---------- MOPD Helper Methods ----------
    def _filter_pareto(self, points: List[MOPDPoint]) -> List[MOPDPoint]:
        """Return non‑dominated points."""
        if not points:
            return []

        pareto = []
        objective_keys = ['total_harvested', 'avg_efficiency', 'carbon_saved', 'helium_saved']
        for i, p_i in enumerate(points):
            dominated = False
            for j, p_j in enumerate(points):
                if i == j:
                    continue
                a_vec = [getattr(p_i, k) for k in objective_keys]
                b_vec = [getattr(p_j, k) for k in objective_keys]
                if all(b >= a for a, b in zip(a_vec, b_vec)) and any(b > a for a, b in zip(a_vec, b_vec)):
                    dominated = True
                    break
            if not dominated:
                pareto.append(p_i)
        return pareto

    def _select_best_from_pareto(self, pareto_front: List[MOPDPoint]) -> Optional[MOPDPoint]:
        """Select best point using scalarisation with objective weights."""
        if not pareto_front:
            return None

        weights = self.config.mopd.objective_weights
        objective_keys = list(weights.keys())

        max_vals = {}
        min_vals = {}
        for key in objective_keys:
            vals = [getattr(p, key) for p in pareto_front]
            max_vals[key] = max(vals)
            min_vals[key] = min(vals)
        ranges = {k: max_vals[k] - min_vals[k] if max_vals[k] != min_vals[k] else 1.0 for k in objective_keys}

        best = None
        best_score = -float('inf')
        for point in pareto_front:
            score = 0.0
            for key in objective_keys:
                val = getattr(point, key)
                norm = (val - min_vals[key]) / ranges[key] if ranges[key] > 0 else 1.0
                weight = weights.get(key, 0.0)
                score += weight * norm
            point.scalarised_score = score
            if score > best_score:
                best_score = score
                best = point
        return best

    # ============================================================================
    # Checkpointing (Enhanced with MOPD)
    # ============================================================================
    async def _save_checkpoint(self, current_index: int, pareto_front: Optional[List[MOPDPoint]] = None):
        """Save current simulation state to a checkpoint file."""
        harvester_state = None
        try:
            if hasattr(self.harvester, 'get_harvesting_stats'):
                stats = await self.harvester.get_harvesting_stats()
                harvester_state = stats
        except Exception as e:
            logger.warning("Could not retrieve harvester state for checkpoint: %s", e)

        if self.config.data_source == 'csv' and self.daily_df is not None and current_index < len(self.daily_df):
            current_date_str = self.daily_df.iloc[current_index]['date'].isoformat()
        else:
            current_date_str = datetime.now().isoformat()

        metrics_summary = self.metrics.get_summary()
        metrics_data = self.metrics.to_dict()

        pareto_front_dict = None
        if pareto_front is not None:
            pareto_front_dict = [p.to_dict() for p in pareto_front]
        elif self._pareto_front:
            pareto_front_dict = [p.to_dict() for p in self._pareto_front]

        state = SimulationState(
            current_index=current_index,
            current_date=current_date_str,
            total_harvested=self.metrics.total_harvested,
            harvest_cycles=self.metrics.harvest_cycles,
            metrics=metrics_summary,
            metrics_data=metrics_data,
            harvester_state=harvester_state,
            data_hash=self._data_hash,
            timestamp=datetime.now().isoformat(),
            pareto_front=pareto_front_dict,
            current_policy_id=None
        )

        filename = f"simulation_{self.harvester.__class__.__name__}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pkl"
        checkpoint_path = Path(self.config.checkpoint_dir) / filename
        try:
            with open(checkpoint_path, 'wb') as f:
                pickle.dump(state, f)
            logger.debug("Checkpoint saved at index %d to %s", current_index, checkpoint_path)
            self._cleanup_old_checkpoints()
        except Exception as e:
            logger.warning("Failed to save checkpoint: %s", e)

    def _cleanup_old_checkpoints(self):
        pattern = f"simulation_{self.harvester.__class__.__name__}_*.pkl"
        checkpoint_files = sorted(Path(self.config.checkpoint_dir).glob(pattern), key=lambda p: p.stat().st_mtime)
        if len(checkpoint_files) > self.config.max_checkpoints:
            for old_file in checkpoint_files[:-self.config.max_checkpoints]:
                try:
                    old_file.unlink()
                    logger.debug("Removed old checkpoint: %s", old_file)
                except Exception as e:
                    logger.warning("Failed to remove old checkpoint %s: %s", old_file, e)

    def _load_checkpoint(self) -> bool:
        pattern = f"simulation_{self.harvester.__class__.__name__}_*.pkl"
        checkpoint_files = sorted(Path(self.config.checkpoint_dir).glob(pattern), key=lambda p: p.stat().st_mtime)
        if not checkpoint_files:
            return False

        latest = checkpoint_files[-1]
        try:
            with open(latest, 'rb') as f:
                state = pickle.load(f)

            if self._data_hash is not None and state.data_hash is not None:
                if state.data_hash != self._data_hash:
                    logger.warning("Data hash mismatch: checkpoint may be incompatible. Resuming from start.")
                    return False

            self._current_index = state.current_index
            if state.metrics_data:
                self.metrics = MetricsCollector.from_dict(state.metrics_data)
            else:
                self.metrics.total_harvested = state.total_harvested
                self.metrics.harvest_cycles = state.harvest_cycles

            if state.harvester_state and hasattr(self.harvester, 'restore_state'):
                try:
                    self.harvester.restore_state(state.harvester_state)
                    logger.info("Restored harvester state from checkpoint.")
                except Exception as e:
                    logger.warning("Failed to restore harvester state: %s", e)

            if state.pareto_front:
                self._pareto_front = [MOPDPoint.from_dict(p) for p in state.pareto_front]
                logger.info("Restored Pareto front with %d points.", len(self._pareto_front))

            logger.info("Resumed from checkpoint: index %d, date %s, file %s",
                        state.current_index, state.current_date, latest)
            return True
        except Exception as e:
            logger.warning("Failed to load checkpoint: %s", e)
            return False

    # ============================================================================
    # Public API – MOPD Query Methods
    # ============================================================================
    def get_pareto_front(self) -> List[MOPDPoint]:
        """Return the current Pareto front (if any)."""
        return self._pareto_front.copy()

    def get_mopd_summary(self) -> Dict[str, Any]:
        """Return a summary of MOPD‑related metrics."""
        if not self.config.mopd.enabled:
            return {"enabled": False}
        return {
            "enabled": True,
            "objective_weights": self.config.mopd.objective_weights,
            "grid_resolution": self.config.mopd.grid_resolution,
            "pareto_front_size": len(self._pareto_front),
            "num_policies_evaluated": len(self._mopd_results),
        }

    # ============================================================================
    # General Public Methods (unchanged)
    # ============================================================================
    def get_metrics(self) -> Dict[str, Any]:
        return self.metrics.get_summary()

    async def shutdown(self):
        if self._running:
            self.stop()
            await asyncio.sleep(0.1)
        if self.config.enable_checkpointing and self._current_index > 0 and self.config.data_source == 'csv':
            await self._save_checkpoint(self._current_index)
        logger.info("TimeTickEngine shutdown.")

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.shutdown()


# ============================================================================
# New: NSGA-II Optimizer for Policy Space
# ============================================================================
class NSGAIIOptimizer:
    """
    Multi‑objective optimizer using NSGA‑II to evolve policies for the TimeTickEngine.
    """
    def __init__(self,
                 engine: TimeTickEngine,
                 policy_space: Optional[Dict[str, Any]] = None,
                 population_size: int = 20,
                 generations: int = 5,
                 mutation_rate: float = 0.2,
                 crossover_rate: float = 0.8,
                 tournament_size: int = 3,
                 dynamic_weights: bool = True,
                 objective_weights: Optional[Dict[str, float]] = None):
        self.engine = engine
        self.policy_space = policy_space if policy_space else self._default_policy_space()
        self.population_size = population_size
        self.generations = generations
        self.mutation_rate = mutation_rate
        self.crossover_rate = crossover_rate
        self.tournament_size = tournament_size
        self.dynamic_weights = dynamic_weights
        self.objective_weights = objective_weights or engine.config.mopd.objective_weights
        self.best_individual = None
        self.best_fitness = -float('inf')
        self.evolution_history = []
        self.pareto_front: List[MOPDPoint] = []
        self._eval_cache: Dict[Tuple, MOPDPoint] = {}

        # Determine parameter names and types
        self.param_names = list(self.policy_space.keys())
        self.discrete_params = {k: v for k, v in self.policy_space.items() if isinstance(v, (list, tuple)) and not isinstance(v[0], (int, float))}
        self.continuous_params = {k: v for k, v in self.policy_space.items() if isinstance(v, (list, tuple)) and isinstance(v[0], (int, float)) and len(v) == 2}

        # Convert discrete lists of floats/ints to list
        for k, v in self.policy_space.items():
            if isinstance(v, list) and not isinstance(v[0], (list, tuple)):
                self.discrete_params[k] = v
            elif isinstance(v, tuple) and len(v) == 2 and all(isinstance(x, (int, float)) for x in v):
                self.continuous_params[k] = v

    def _default_policy_space(self) -> Dict[str, Any]:
        # Default: mode choices and a few continuous parameters
        return {
            'harvester_mode': ['standard', 'aggressive', 'conservative'],
            'conversion_factor': (0.5, 1.5),
            'repair_rate': (0.001, 0.02),
            'sensitivity_multiplier': (0.5, 2.0),
        }

    def _random_individual(self) -> Dict[str, Any]:
        ind = {}
        for name in self.param_names:
            if name in self.discrete_params:
                ind[name] = random.choice(self.discrete_params[name])
            elif name in self.continuous_params:
                low, high = self.continuous_params[name]
                ind[name] = random.uniform(low, high)
        return ind

    def _crossover(self, parent1: Dict, parent2: Dict) -> Dict:
        child = {}
        for name in self.param_names:
            if name in self.discrete_params:
                child[name] = random.choice([parent1[name], parent2[name]])
            else:
                low, high = self.continuous_params[name]
                # SBX
                if random.random() < 0.5:
                    u = random.random()
                    if u <= 0.5:
                        beta = (2 * u) ** (1 / (20 + 1))
                    else:
                        beta = (1 / (2 * (1 - u))) ** (1 / (20 + 1))
                    val = 0.5 * ((1 + beta) * parent1[name] + (1 - beta) * parent2[name])
                    child[name] = max(low, min(high, val))
                else:
                    child[name] = parent1[name] if random.random() < 0.5 else parent2[name]
        return child

    def _mutate(self, individual: Dict) -> Dict:
        mutant = copy.deepcopy(individual)
        for name in self.param_names:
            if random.random() < self.mutation_rate:
                if name in self.discrete_params:
                    mutant[name] = random.choice(self.discrete_params[name])
                else:
                    low, high = self.continuous_params[name]
                    u = random.random()
                    if u < 0.5:
                        delta = (2 * u) ** (1 / (20 + 1)) - 1
                    else:
                        delta = 1 - (2 * (1 - u)) ** (1 / (20 + 1))
                    mutant[name] = individual[name] + delta * (high - low)
                    mutant[name] = max(low, min(high, mutant[name]))
        return mutant

    def _individual_to_policy(self, ind: Dict) -> Policy:
        policy_id = "evolved_" + hashlib.md5(str(ind).encode()).hexdigest()[:8]
        mode = ind.get('harvester_mode', 'standard')
        params = {k: v for k, v in ind.items() if k != 'harvester_mode'}
        return Policy(policy_id=policy_id, harvester_mode=mode, parameters=params)

    async def _evaluate_individual(self, ind: Dict) -> MOPDPoint:
        # Cache
        key = tuple(sorted(ind.items()))
        if key in self._eval_cache:
            return self._eval_cache[key]
        policy = self._individual_to_policy(ind)
        point = await self.engine.evaluate_policy(policy)
        self._eval_cache[key] = point
        return point

    def _fast_non_dominated_sort(self, points: List[MOPDPoint]) -> List[List[MOPDPoint]]:
        fronts = []
        domination_count = {id(p): 0 for p in points}
        dominated_solutions = {id(p): [] for p in points}
        objective_keys = ['total_harvested', 'avg_efficiency', 'carbon_saved', 'helium_saved']

        for i, p in enumerate(points):
            for j, q in enumerate(points):
                if i == j:
                    continue
                p_obj = [getattr(p, k) for k in objective_keys]
                q_obj = [getattr(q, k) for k in objective_keys]
                if all(p >= q for p, q in zip(p_obj, q_obj)) and any(p > q for p, q in zip(p_obj, q_obj)):
                    dominated_solutions[id(p)].append(q)
                elif all(q >= p for p, q in zip(p_obj, q_obj)) and any(q > p for p, q in zip(p_obj, q_obj)):
                    domination_count[id(p)] += 1

            if domination_count[id(p)] == 0:
                if not fronts:
                    fronts.append([])
                fronts[0].append(p)

        i = 0
        while i < len(fronts):
            next_front = []
            for p in fronts[i]:
                for q in dominated_solutions[id(p)]:
                    domination_count[id(q)] -= 1
                    if domination_count[id(q)] == 0:
                        next_front.append(q)
            if next_front:
                fronts.append(next_front)
            i += 1
        return fronts

    def _crowding_distance(self, front: List[MOPDPoint]) -> Dict[int, float]:
        if not front:
            return {}
        distances = {id(p): 0.0 for p in front}
        objective_keys = ['total_harvested', 'avg_efficiency', 'carbon_saved', 'helium_saved']
        for obj in objective_keys:
            sorted_front = sorted(front, key=lambda x: getattr(x, obj))
            distances[id(sorted_front[0])] = float('inf')
            distances[id(sorted_front[-1])] = float('inf')
            obj_min = getattr(sorted_front[0], obj)
            obj_max = getattr(sorted_front[-1], obj)
            if obj_max == obj_min:
                continue
            for i in range(1, len(sorted_front) - 1):
                distances[id(sorted_front[i])] += (getattr(sorted_front[i+1], obj) - getattr(sorted_front[i-1], obj)) / (obj_max - obj_min)
        return distances

    def _tournament_selection(self, population: List[Dict], fronts: List[List[MOPDPoint]], crowding: Dict[int, float]) -> Dict:
        # Select based on rank and crowding distance
        # We need mapping from individual dict to point
        # Build mapping id(point) -> individual dict
        point_to_ind = {}
        for ind, point in self._eval_cache.items():
            point_to_ind[id(point)] = ind

        # Randomly choose two individuals
        candidates = random.sample(population, self.tournament_size)
        best = candidates[0]
        best_rank = float('inf')
        best_crowding = -float('inf')
        for cand in candidates:
            # Find rank
            rank = None
            for fi, front in enumerate(fronts):
                # front contains MOPDPoint objects; need to find corresponding individual
                for p in front:
                    if point_to_ind.get(id(p)) == cand:
                        rank = fi
                        break
                if rank is not None:
                    break
            if rank is None:
                rank = len(fronts)
            # Get crowding distance
            cd = crowding.get(id(self._eval_cache.get(tuple(sorted(cand.items())))), 0) if tuple(sorted(cand.items())) in self._eval_cache else 0
            if rank < best_rank or (rank == best_rank and cd > best_crowding):
                best = cand
                best_rank = rank
                best_crowding = cd
        return best

    def _compute_dynamic_weights(self) -> Dict[str, float]:
        """Adjust weights based on current system state."""
        weights = self.objective_weights.copy()
        if not self.dynamic_weights:
            return weights
        # Example: if total harvested is low compared to potential, increase weight on total_harvested
        if self.pareto_front:
            avg_harvest = np.mean([p.total_harvested for p in self.pareto_front])
            max_harvest = max([p.total_harvested for p in self.pareto_front])
            if max_harvest > 0 and avg_harvest < 0.5 * max_harvest:
                weights['total_harvested'] = min(0.5, weights.get('total_harvested', 0.3) * 1.5)
                # Normalize
                total = sum(weights.values())
                weights = {k: v / total for k, v in weights.items()}
        return weights

    async def evolve(self) -> List[MOPDPoint]:
        """Run NSGA-II optimization."""
        population = [self._random_individual() for _ in range(self.population_size)]
        # Evaluate initial population
        points = []
        for ind in population:
            p = await self._evaluate_individual(ind)
            points.append(p)

        # Map point to individual for later
        point_to_ind = {id(p): ind for ind, p in zip(population, points)}

        for gen in range(self.generations):
            # Create offspring
            offspring = []
            # Update population list of individuals (we need mapping)
            # We'll keep a list of (individual, point)
            pairs = list(zip(population, points))
            # Fast non-dominated sort of current points
            fronts = self._fast_non_dominated_sort(points)
            crowding = {}
            for front in fronts:
                front_crowding = self._crowding_distance(front)
                crowding.update(front_crowding)

            while len(offspring) < self.population_size:
                parent1 = self._tournament_selection([p[0] for p in pairs], fronts, crowding)
                parent2 = self._tournament_selection([p[0] for p in pairs], fronts, crowding)
                if random.random() < self.crossover_rate:
                    child = self._crossover(parent1, parent2)
                else:
                    child = copy.deepcopy(parent1)
                child = self._mutate(child)
                offspring.append(child)

            # Evaluate offspring
            child_points = []
            for ind in offspring:
                p = await self._evaluate_individual(ind)
                child_points.append(p)

            # Combine parent and offspring
            combined_inds = population + offspring
            combined_points = points + child_points
            # Remove duplicates based on individual dict
            unique_pairs = {}
            for ind, p in zip(combined_inds, combined_points):
                key = tuple(sorted(ind.items()))
                unique_pairs[key] = (ind, p)
            population = [v[0] for v in unique_pairs.values()]
            points = [v[1] for v in unique_pairs.values()]

            # Non-dominated sorting on combined points
            fronts = self._fast_non_dominated_sort(points)
            new_population = []
            new_points = []
            for front in fronts:
                if len(new_population) + len(front) <= self.population_size:
                    for p in front:
                        # Find corresponding individual
                        for ind, p2 in zip(population, points):
                            if p2 is p:
                                new_population.append(ind)
                                new_points.append(p)
                                break
                else:
                    # Fill remaining with crowding distance
                    crowding = self._crowding_distance(front)
                    sorted_front = sorted(front, key=lambda x: crowding.get(id(x), 0), reverse=True)
                    for p in sorted_front:
                        if len(new_population) >= self.population_size:
                            break
                        for ind, p2 in zip(population, points):
                            if p2 is p:
                                new_population.append(ind)
                                new_points.append(p)
                                break
            population = new_population[:self.population_size]
            points = new_points[:self.population_size]

            # Update Pareto front (first front)
            fronts = self._fast_non_dominated_sort(points)
            if fronts:
                self.pareto_front = fronts[0]
            logger.info(f"Generation {gen+1}/{self.generations}: population={len(population)}, Pareto front size={len(self.pareto_front)}")

        # After generations, compute dynamic weights and select best
        weights = self._compute_dynamic_weights()
        # Use MODP scalarisation on Pareto front
        best_point = self._select_best_from_pareto(self.pareto_front, weights)
        if best_point:
            # Find corresponding individual and set as best
            for ind, p in zip(population, points):
                if p is best_point:
                    self.best_individual = ind
                    self.best_fitness = best_point.scalarised_score
                    break
        return self.pareto_front

    def _select_best_from_pareto(self, pareto_front: List[MOPDPoint], weights: Optional[Dict[str, float]] = None) -> Optional[MOPDPoint]:
        if not pareto_front:
            return None
        if weights is None:
            weights = self.objective_weights
        objective_keys = list(weights.keys())
        max_vals = {k: max(getattr(p, k) for p in pareto_front) for k in objective_keys}
        min_vals = {k: min(getattr(p, k) for p in pareto_front) for k in objective_keys}
        ranges = {k: max_vals[k] - min_vals[k] if max_vals[k] != min_vals[k] else 1.0 for k in objective_keys}

        best = None
        best_score = -float('inf')
        for point in pareto_front:
            score = 0.0
            for key in objective_keys:
                val = getattr(point, key)
                norm = (val - min_vals[key]) / ranges[key] if ranges[key] > 0 else 1.0
                score += weights.get(key, 0.0) * norm
            point.scalarised_score = score
            if score > best_score:
                best_score = score
                best = point
        return best


# ============================================================================
# Example usage
# ============================================================================
if __name__ == "__main__":
    # Mock harvester
    class MockHarvester:
        def __init__(self):
            self.mode = "standard"
            self.parameters = {}

        async def harvest_cycle(self, env_data):
            # Simulate different harvest based on mode and parameters
            base = env_data.get('helium_supply', 0.5) * 10
            factor = 1.0
            if self.mode == "aggressive":
                factor = 1.5
            elif self.mode == "conservative":
                factor = 0.8
            if 'conversion_factor' in self.parameters:
                factor *= self.parameters['conversion_factor']
            if 'sensitivity_multiplier' in self.parameters:
                factor *= self.parameters['sensitivity_multiplier']
            repair_rate = self.parameters.get('repair_rate', 0.01)
            efficiency = 0.85 * factor * (1 - repair_rate * 10)
            return {
                'eco_atp_generated': base * factor,
                'account_balance': 1000,
                'efficiency': efficiency,
                'mode': self.mode,
                'carbon_impact': 0.1 * factor,
                'helium_usage': env_data.get('helium_demand', 0.5) * factor
            }

        async def get_harvesting_stats(self):
            return {'harvester_id': 'mock', 'mode': self.mode}

        def restore_state(self, state):
            pass

        def set_mode(self, mode):
            self.mode = mode

        def set_parameters(self, params):
            self.parameters = params

        def get_parameters(self):
            return self.parameters

    # Mock translator
    class MockTranslator:
        @staticmethod
        def translate_row(row):
            return {
                'renewable_availability': 0.8,
                'carbon_intensity': 200,
                'waste_heat': 0.3,
                'edge_availability': 0.6,
                'system_overload': 0.1,
                'helium_supply': row.get('helium_supply', 0.5),
                'helium_demand': row.get('helium_demand', 0.5)
            }

    # Configuration with MOPD enabled
    config = {
        'data_source': 'csv',
        'csv_path': 'helium_data.csv',
        'value_columns': ['helium_supply', 'helium_demand'],
        'interpolation_method': 'linear',
        'tick_interval_seconds': 0.01,
        'enable_checkpointing': False,
        'metrics_enabled': True,
        'mopd': {
            'enabled': True,
            'population_size': 8,
            'generations': 3,
            'objective_weights': {
                'total_harvested': 0.3,
                'avg_efficiency': 0.3,
                'carbon_saved': 0.2,
                'helium_saved': 0.2,
            },
            'dynamic_weights': True
        }
    }

    async def main():
        harvester = MockHarvester()
        translator = MockTranslator()
        engine = TimeTickEngine(harvester, translator, config)

        try:
            await engine.load_data()

            # Run evolutionary optimization
            policy_space = {
                'harvester_mode': ['standard', 'aggressive', 'conservative'],
                'conversion_factor': (0.5, 1.5),
                'repair_rate': (0.001, 0.02),
                'sensitivity_multiplier': (0.5, 2.0),
            }
            pareto = await engine.run_evolution(policy_space)
            print("Evolved Pareto front size:", len(pareto))
            for p in pareto[:5]:
                print(f"Policy {p.policy_id}: mode={p.harvester_mode}, harvested={p.total_harvested:.2f}, eff={p.avg_efficiency:.2f}")

        finally:
            await engine.shutdown()

    asyncio.run(main())
