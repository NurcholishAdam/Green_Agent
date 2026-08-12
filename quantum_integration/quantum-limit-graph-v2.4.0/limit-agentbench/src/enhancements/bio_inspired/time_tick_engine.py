"""
TimeTickEngine v3.3 – Enhanced simulation driver with MOPD support.

Supports:
- CSV data loading with validation and configurable date column.
- Live data feed integration (via callback or async generator).
- Interpolation methods: linear, quadratic, spline, time‑based.
- Checkpoint saving/loading to resume simulations.
- Metrics collection (total harvested, average efficiency, etc.).
- Graceful stop via stop() method.
- Async context manager.
- **Multi‑policy simulation** for Pareto front generation.
- **MOPD (Multi‑Objective Pareto Decision)** with configurable weights.
- **Persistence of Pareto fronts** in checkpoints.
- **Telemetry** for MOPD generations and Pareto front sizes.
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
# MOPD Data Classes (NEW)
# ============================================================================
@dataclass
class MOPDPoint:
    """Represents a single policy with its objective values."""
    policy_id: str
    # Decision variables (can be extended)
    harvester_mode: str
    # Objectives (to be minimised/maximised)
    total_harvested: float
    avg_efficiency: float
    carbon_saved: float
    helium_saved: float
    # Scalarised score (computed later)
    scalarised_score: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'MOPDPoint':
        return cls(**data)

@dataclass
class Policy:
    """A policy defines a simulation scenario with a harvester mode."""
    policy_id: str
    harvester_mode: str

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
# Enhanced TimeTickEngine (with MOPD)
# ============================================================================
class TimeTickEngine:
    """
    Enhanced simulation driver with MOPD support for multi‑policy evaluation.
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
    # MOPD Multi‑Policy Simulation (NEW)
    # ============================================================================
    async def run_multi_policy_simulation(
        self,
        policies: List[Policy],
        post_policy_callback: Optional[Callable[[Policy, Dict[str, Any]], Awaitable[None]]] = None
    ) -> List[MOPDPoint]:
        """
        Run a simulation for each policy, collect objectives, and generate the Pareto front.
        The harvester's mode is set for each policy, and the simulation runs from start to end.

        Args:
            policies: List of Policy objects (each with a harvester_mode).
            post_policy_callback: Optional callback after each policy completes.

        Returns:
            List of MOPDPoint objects representing the Pareto front.
        """
        if not self.config.mopd.enabled:
            logger.warning("MOPD is disabled; multi‑policy simulation will not generate Pareto front.")
            return []

        if self.config.data_source == 'csv' and self.daily_df is None:
            raise RuntimeError("Data not loaded. Call load_data() first.")

        # Store original harvester mode to restore later
        original_mode = getattr(self.harvester, 'mode', None)

        self._mopd_results = {}
        self._pareto_front = []

        logger.info("Starting multi‑policy simulation with %d policies.", len(policies))

        for policy in policies:
            logger.info("Running policy: %s (mode: %s)", policy.policy_id, policy.harvester_mode)

            # Set harvester mode
            if hasattr(self.harvester, 'set_mode'):
                self.harvester.set_mode(policy.harvester_mode)

            # Reset metrics for this policy
            self.metrics = MetricsCollector(max_custom_entries=self.config.max_custom_metrics_entries)

            # Run simulation (from start, no checkpoint resuming)
            try:
                await self.run_simulation(start_index=0)
            except Exception as e:
                logger.error("Policy %s failed: %s", policy.policy_id, e)
                continue

            # Extract objectives from metrics
            summary = self.metrics.get_summary()
            # Compute carbon savings and helium savings (example: based on custom metrics)
            carbon_saved = summary.get('avg_carbon_impact', 0.0)  # placeholder
            helium_saved = summary.get('avg_helium_usage', 0.0)   # placeholder

            # Build MOPDPoint
            point = MOPDPoint(
                policy_id=policy.policy_id,
                harvester_mode=policy.harvester_mode,
                total_harvested=summary['total_harvested'],
                avg_efficiency=summary['avg_efficiency'],
                carbon_saved=carbon_saved,
                helium_saved=helium_saved,
            )
            self._mopd_results[policy.policy_id] = {
                'metrics': summary,
                'point': point,
            }

            if post_policy_callback:
                try:
                    if asyncio.iscoroutinefunction(post_policy_callback):
                        await post_policy_callback(policy, summary)
                    else:
                        post_policy_callback(policy, summary)
                except Exception as e:
                    logger.error("Post‑policy callback failed: %s", e)

        # Generate Pareto front from all points
        points = [data['point'] for data in self._mopd_results.values()]
        if points:
            self._pareto_front = self._filter_pareto(points)
            # Select best plan using MOPD weights
            best_plan = self._select_best_from_pareto(self._pareto_front)
            if best_plan:
                logger.info("Best policy: %s with scalarised score %.3f",
                            best_plan.policy_id, best_plan.scalarised_score)

        # Restore harvester mode
        if hasattr(self.harvester, 'set_mode') and original_mode is not None:
            self.harvester.set_mode(original_mode)

        # Save checkpoint with Pareto front
        if self.config.enable_checkpointing:
            await self._save_checkpoint(self._current_index, pareto_front=self._pareto_front)

        # Telemetry
        if self.config.metrics_enabled:
            logger.info("MOPD generation: %d policies, Pareto front size: %d",
                        len(policies), len(self._pareto_front))

        return self._pareto_front

    # ---------- MOPD Helper Methods ----------
    def _filter_pareto(self, points: List[MOPDPoint]) -> List[MOPDPoint]:
        """Return non‑dominated points."""
        if not points:
            return []

        pareto = []
        objective_keys = ['total_harvested', 'avg_efficiency', 'carbon_saved', 'helium_saved']
        # For all objectives, higher is better.
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

        # Normalise objectives across Pareto front
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
                # Higher is better, so normalise as (val - min) / range
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

        # Serialise Pareto front if provided
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
            current_policy_id=None  # Not needed for single policy
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

            # Restore Pareto front if present
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
    # Public API – MOPD Query Methods (NEW)
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
# Example usage
# ============================================================================
if __name__ == "__main__":
    # Mock harvester
    class MockHarvester:
        def __init__(self):
            self.mode = "standard"

        async def harvest_cycle(self, env_data):
            # Simulate different harvest based on mode
            if self.mode == "aggressive":
                multiplier = 1.5
            elif self.mode == "conservative":
                multiplier = 0.8
            else:
                multiplier = 1.0
            return {
                'eco_atp_generated': env_data.get('helium_supply', 0.5) * multiplier * 10,
                'account_balance': 1000,
                'efficiency': 0.85 * multiplier,
                'mode': self.mode,
                'carbon_impact': 0.1 * multiplier,
                'helium_usage': env_data.get('helium_demand', 0.5) * multiplier
            }

        async def get_harvesting_stats(self):
            return {'harvester_id': 'mock', 'mode': self.mode}

        def restore_state(self, state):
            pass

        def set_mode(self, mode):
            self.mode = mode

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
        'tick_interval_seconds': 0.05,
        'enable_checkpointing': True,
        'checkpoint_interval': 50,
        'metrics_enabled': True,
        'max_checkpoints': 3,
        'mopd': {
            'enabled': True,
            'objective_weights': {
                'total_harvested': 0.3,
                'avg_efficiency': 0.3,
                'carbon_saved': 0.2,
                'helium_saved': 0.2,
            }
        }
    }

    async def main():
        harvester = MockHarvester()
        translator = MockTranslator()
        engine = TimeTickEngine(harvester, translator, config)

        try:
            await engine.load_data()

            # Run single policy simulation (original)
            # await engine.run_simulation()

            # Run multi‑policy simulation with MOPD
            policies = [
                Policy(policy_id="standard", harvester_mode="standard"),
                Policy(policy_id="aggressive", harvester_mode="aggressive"),
                Policy(policy_id="conservative", harvester_mode="conservative"),
            ]
            pareto_front = await engine.run_multi_policy_simulation(policies)
            print("Pareto front size:", len(pareto_front))
            for p in pareto_front:
                print(f"Policy {p.policy_id}: total_harvested={p.total_harvested:.2f}, avg_efficiency={p.avg_efficiency:.2f}, carbon_saved={p.carbon_saved:.2f}, helium_saved={p.helium_saved:.2f}, scalarised={p.scalarised_score:.3f}")

        finally:
            await engine.shutdown()

    asyncio.run(main())
