"""
TimeTickEngine v2.0
Enhanced simulation driver with configurable data sources, interpolation,
checkpointing, metrics, and graceful shutdown.

Supports:
- CSV data loading with validation and configurable date column.
- Interpolation methods: linear, quadratic, spline, time‑based.
- Checkpoint saving/loading to resume simulations.
- Metrics collection (total harvested, average efficiency, etc.).
- Live data feed integration (optional).
- Async context manager for clean resource management.
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, Callable, Union, List, Protocol
from dataclasses import dataclass, field, asdict
import json
import os
import pickle
from pathlib import Path

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

# ============================================================================
# Try importing optional dependencies
# ============================================================================
try:
    from pydantic import BaseModel, Field, validator
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

# ============================================================================
# Configuration (Pydantic or dataclass)
# ============================================================================
if PYDANTIC_AVAILABLE:
    class TimeTickConfig(BaseModel):
        """Configuration for TimeTickEngine."""
        data_source: str = Field(..., description="Path to CSV or 'live' for real‑time.")
        csv_path: Optional[str] = Field(None, description="Path to CSV file (if data_source='csv').")
        date_column: str = Field("date", description="Name of the date column in CSV.")
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
else:
    @dataclass
    class TimeTickConfig:
        data_source: str = "csv"
        csv_path: Optional[str] = None
        date_column: str = "date"
        value_columns: List[str] = field(default_factory=lambda: ["helium_supply", "helium_demand"])
        start_date: Optional[str] = None
        end_date: Optional[str] = None
        interpolation_method: str = "linear"
        tick_interval_seconds: float = 0.1
        checkpoint_dir: str = "./checkpoints"
        enable_checkpointing: bool = True
        checkpoint_interval: int = 100
        metrics_enabled: bool = True

# ============================================================================
# Protocols for loose coupling
# ============================================================================
class HarvesterProtocol(Protocol):
    """Protocol for the Photosynthetic Harvester."""
    async def harvest_cycle(self, environmental_data: Dict[str, float]) -> Dict[str, Any]: ...
    def set_mode(self, mode: Any) -> None: ...

class TranslatorProtocol(Protocol):
    """Protocol for translating CSV rows to harvester input."""
    @staticmethod
    def translate_row(row: pd.Series) -> Dict[str, float]: ...

# ============================================================================
# Simulation State for checkpointing
# ============================================================================
@dataclass
class SimulationState:
    """Serializable state for resuming simulation."""
    current_index: int
    current_date: str
    total_harvested: float
    harvest_cycles: int
    metrics: Dict[str, Any]
    timestamp: str

# ============================================================================
# Metrics Collector
# ============================================================================
class MetricsCollector:
    """Collect and aggregate simulation metrics."""
    def __init__(self):
        self.total_harvested = 0.0
        self.harvest_cycles = 0
        self.efficiencies: List[float] = []
        self.modes: List[str] = []
        self.timestamps: List[datetime] = []

    def record(self, result: Dict[str, Any]):
        self.total_harvested += result.get('eco_atp_generated', 0)
        self.harvest_cycles += 1
        self.efficiencies.append(result.get('efficiency', 0))
        self.modes.append(result.get('mode', 'unknown'))
        self.timestamps.append(datetime.now())

    def get_summary(self) -> Dict[str, Any]:
        return {
            'total_harvested': self.total_harvested,
            'harvest_cycles': self.harvest_cycles,
            'avg_efficiency': np.mean(self.efficiencies) if self.efficiencies else 0,
            'max_efficiency': max(self.efficiencies) if self.efficiencies else 0,
            'mode_counts': {mode: self.modes.count(mode) for mode in set(self.modes)},
            'duration_hours': (self.timestamps[-1] - self.timestamps[0]).total_seconds() / 3600 if self.timestamps else 0
        }

# ============================================================================
# Enhanced TimeTickEngine
# ============================================================================
class TimeTickEngine:
    """
    Enhanced simulation driver with configurable data sources, interpolation,
    checkpointing, metrics, and graceful shutdown.
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

        # Load configuration
        if isinstance(config, dict):
            if PYDANTIC_AVAILABLE:
                self.config = TimeTickConfig(**config)
            else:
                self.config = TimeTickConfig(**config)
        elif isinstance(config, TimeTickConfig):
            self.config = config
        else:
            # Default config (requires csv_path)
            self.config = TimeTickConfig(data_source="csv", csv_path="helium_data.csv")

        # Internal state
        self.daily_df: Optional[pd.DataFrame] = None
        self.metrics = MetricsCollector()
        self._running = False
        self._current_index = 0
        self._checkpoint_path = Path(self.config.checkpoint_dir) / f"simulation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pkl"

        # Ensure checkpoint directory exists
        if self.config.enable_checkpointing:
            Path(self.config.checkpoint_dir).mkdir(parents=True, exist_ok=True)

        logger.info("TimeTickEngine initialized with config: %s", self.config)

    async def load_data(self, csv_path: Optional[str] = None):
        """
        Load and preprocess data from CSV.
        If csv_path is provided, overrides the config.
        """
        path = csv_path or self.config.csv_path
        if not path:
            raise ValueError("CSV path not provided.")

        logger.info("Loading CSV from %s", path)

        try:
            df = pd.read_csv(path)
        except Exception as e:
            logger.error("Failed to read CSV: %s", e)
            raise

        # Validate columns
        required = [self.config.date_column] + self.config.value_columns
        missing = [col for col in required if col not in df.columns]
        if missing:
            raise ValueError(f"Missing columns in CSV: {missing}")

        # Parse dates
        try:
            df[self.config.date_column] = pd.to_datetime(df[self.config.date_column])
        except Exception as e:
            raise ValueError(f"Failed to parse date column '{self.config.date_column}': {e}")

        df = df.sort_values(self.config.date_column)

        # Filter dates
        if self.config.start_date:
            start = pd.to_datetime(self.config.start_date)
            df = df[df[self.config.date_column] >= start]
        if self.config.end_date:
            end = pd.to_datetime(self.config.end_date)
            df = df[df[self.config.date_column] <= end]

        # Store monthly data
        self.df_monthly = df

        # Interpolate to daily
        self._interpolate_daily()

        logger.info("Loaded %d monthly rows, interpolated to %d daily ticks.",
                    len(self.df_monthly), len(self.daily_df))

    def _interpolate_daily(self):
        """
        Interpolate monthly data to daily using the configured method.
        """
        df_monthly = self.df_monthly.set_index(self.config.date_column)
        daily_index = pd.date_range(
            start=df_monthly.index.min(),
            end=df_monthly.index.max(),
            freq='D'
        )

        # Select only numeric columns for interpolation
        numeric_cols = [col for col in self.config.value_columns if col in df_monthly.columns]

        if self.config.interpolation_method == 'linear':
            self.daily_df = df_monthly[numeric_cols].reindex(daily_index).interpolate(method='linear')
        elif self.config.interpolation_method == 'quadratic':
            self.daily_df = df_monthly[numeric_cols].reindex(daily_index).interpolate(method='quadratic')
        elif self.config.interpolation_method == 'spline':
            self.daily_df = df_monthly[numeric_cols].reindex(daily_index).interpolate(method='spline')
        elif self.config.interpolation_method == 'time':
            self.daily_df = df_monthly[numeric_cols].reindex(daily_index).interpolate(method='time')
        else:
            raise ValueError(f"Unsupported interpolation method: {self.config.interpolation_method}")

        # Reset index to have date as a column
        self.daily_df = self.daily_df.reset_index()
        self.daily_df.rename(columns={'index': 'date'}, inplace=True)

        # Fill any remaining NaNs with forward fill
        self.daily_df = self.daily_df.fillna(method='ffill').fillna(method='bfill')

    async def run_simulation(self,
                             start_index: Optional[int] = None,
                             post_tick_callback: Optional[Callable] = None):
        """
        Run the simulation over all daily ticks, optionally resuming from a checkpoint.

        Args:
            start_index: Resume from this index (overrides checkpoint).
            post_tick_callback: Async function called after each tick with (index, row, result).
        """
        if self.daily_df is None:
            raise RuntimeError("Data not loaded. Call load_data() first.")

        if start_index is not None:
            self._current_index = start_index
        else:
            # Attempt to load checkpoint
            if self.config.enable_checkpointing:
                self._load_checkpoint()

        self._running = True
        total_ticks = len(self.daily_df)

        logger.info("Starting simulation from index %d of %d", self._current_index, total_ticks)

        try:
            for idx in range(self._current_index, total_ticks):
                if not self._running:
                    break

                row = self.daily_df.iloc[idx]

                # Translate row to harvester input
                env_data = self._translate_row(row)
                if env_data is None:
                    continue

                # Run harvest cycle
                result = await self.harvester.harvest_cycle(env_data)

                # Record metrics
                if self.config.metrics_enabled:
                    self.metrics.record(result)

                # Optional callback
                if post_tick_callback:
                    await post_tick_callback(idx, row, result)

                # Log every 30 days
                if idx % 30 == 0:
                    logger.info("Day %d: harvested %.2f Eco‑ATP, balance %.2f",
                                idx, result.get('eco_atp_generated', 0),
                                result.get('account_balance', 0))

                # Save checkpoint
                if self.config.enable_checkpointing and idx % self.config.checkpoint_interval == 0:
                    self._save_checkpoint(idx)

                # Tick delay
                await asyncio.sleep(self.config.tick_interval_seconds)

        except asyncio.CancelledError:
            logger.info("Simulation cancelled.")
            self._running = False
            # Save final checkpoint
            if self.config.enable_checkpointing:
                self._save_checkpoint(self._current_index)
            raise

        except Exception as e:
            logger.error("Simulation failed at index %d: %s", idx, e)
            self._running = False
            raise

        finally:
            self._running = False
            logger.info("Simulation finished. Total harvested: %.2f", self.metrics.total_harvested)

    def _translate_row(self, row: pd.Series) -> Dict[str, float]:
        """
        Translate a row of data into environmental data for the harvester.
        Uses the translator provided at initialization.
        """
        try:
            if callable(self.translator):
                return self.translator(row)
            elif hasattr(self.translator, 'translate_row'):
                return self.translator.translate_row(row)
            else:
                # Fallback: map all value columns to float and prefix with 'helium_'
                env_data = {}
                for col in self.config.value_columns:
                    if col in row:
                        env_data[f"helium_{col}"] = float(row[col])
                return env_data
        except Exception as e:
            logger.error("Row translation failed: %s", e)
            return None

    def _save_checkpoint(self, current_index: int):
        """Save current simulation state to a checkpoint file."""
        state = SimulationState(
            current_index=current_index,
            current_date=self.daily_df.iloc[current_index]['date'].isoformat(),
            total_harvested=self.metrics.total_harvested,
            harvest_cycles=self.metrics.harvest_cycles,
            metrics=self.metrics.get_summary(),
            timestamp=datetime.now().isoformat()
        )
        try:
            with open(self._checkpoint_path, 'wb') as f:
                pickle.dump(state, f)
            logger.debug("Checkpoint saved at index %d", current_index)
        except Exception as e:
            logger.warning("Failed to save checkpoint: %s", e)

    def _load_checkpoint(self) -> bool:
        """Load the latest checkpoint and update state."""
        checkpoint_files = sorted(Path(self.config.checkpoint_dir).glob("simulation_*.pkl"))
        if not checkpoint_files:
            return False

        latest = checkpoint_files[-1]
        try:
            with open(latest, 'rb') as f:
                state = pickle.load(f)
            self._current_index = state.current_index
            self.metrics.total_harvested = state.total_harvested
            self.metrics.harvest_cycles = state.harvest_cycles
            logger.info("Resumed from checkpoint: index %d, date %s",
                        state.current_index, state.current_date)
            return True
        except Exception as e:
            logger.warning("Failed to load checkpoint: %s", e)
            return False

    def get_metrics(self) -> Dict[str, Any]:
        """Return current simulation metrics."""
        return self.metrics.get_summary()

    async def shutdown(self):
        """
        Gracefully shut down the simulation.
        Saves a final checkpoint if enabled.
        """
        if self._running:
            self._running = False
            if self.config.enable_checkpointing:
                self._save_checkpoint(self._current_index)
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
        async def harvest_cycle(self, env_data):
            return {
                'eco_atp_generated': env_data.get('helium_helium_supply', 0) * 10,
                'account_balance': 1000,
                'efficiency': 0.85,
                'mode': 'ADAPTIVE'
            }

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

    # Configuration (example)
    config = {
        'data_source': 'csv',
        'csv_path': 'helium_data.csv',
        'value_columns': ['helium_supply', 'helium_demand'],
        'interpolation_method': 'linear',
        'tick_interval_seconds': 0.05,
        'enable_checkpointing': True,
        'checkpoint_interval': 50,
        'metrics_enabled': True
    }

    async def main():
        harvester = MockHarvester()
        translator = MockTranslator()

        engine = TimeTickEngine(harvester, translator, config)

        try:
            await engine.load_data()
            await engine.run_simulation()
        finally:
            await engine.shutdown()

        print("Final metrics:", engine.get_metrics())

    asyncio.run(main())
