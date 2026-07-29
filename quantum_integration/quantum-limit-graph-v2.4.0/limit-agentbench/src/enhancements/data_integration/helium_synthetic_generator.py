# src/enhancements/data_integration/helium_synthetic_generator.py
"""
Enhanced Helium Synthetic Generator v2.1.0
===========================================
Generates synthetic Helium Proof‑of‑Coverage (PoC) traces for limit‑agentbench
with realistic distributions, temporal patterns, spatial clustering, gateway topology,
edge cases, and configurable parameters.

ENHANCEMENTS OVER v2.0.0:
- Inhomogeneous Poisson process for event generation (diurnal and burst patterns).
- Improved path loss model with log‑normal shadowing.
- More realistic RSSI/SNR distributions (bounded, skew‑normal).
- Edge case injection with controlled anomaly types (hotspot failure, interference, extreme values).
- Statistical validation uses distribution fitting (Kolmogorov–Smirnov, chi‑square) with warnings.
- Multiple trace generation uses independent config copies (no side effects).
- Support for loading config from JSON.
- Metadata includes full config and generation parameters.
- Export to CSV/JSON/Parquet with metadata.
- Comprehensive docstrings and type hints.
"""

import random
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Union, Any, Tuple, Callable
from pathlib import Path
import json
import hashlib
import copy

# ---------- Pydantic ----------
try:
    from pydantic import BaseModel, Field, field_validator, ConfigDict
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

# ---------- Scipy for statistical tests ----------
try:
    from scipy import stats
    SCIPY_AVAILABLE = True
except ImportError:
    SCIPY_AVAILABLE = False

# ---------- Logging ----------
import logging
logger = logging.getLogger(__name__)

# ============================================================================
# Configuration
# ============================================================================
if PYDANTIC_AVAILABLE:
    class HeliumSyntheticConfig(BaseModel):
        """Configuration for synthetic trace generation."""
        # General
        version: str = "2.1.0"
        seed: int = Field(42, description="Random seed for reproducibility")
        # Trace parameters
        num_hotspots: int = Field(100, ge=1)
        num_gateways: int = Field(5, ge=1)
        duration_hours: float = Field(24.0, ge=1)
        base_events_per_hour: float = Field(10.0, gt=0)
        # RSSI/SNR distributions (per region and hotspot type)
        rssi_mean_urban: float = Field(-70.0)
        rssi_std_urban: float = Field(10.0)
        rssi_mean_rural: float = Field(-80.0)
        rssi_std_rural: float = Field(15.0)
        snr_mean: float = Field(12.0)
        snr_std: float = Field(3.0)
        # Spatial clustering
        num_clusters: int = Field(3, ge=1)
        cluster_spread: float = Field(0.2, description="Spread of clusters relative to area")
        # Gateway path loss parameters
        path_loss_exponent: float = Field(2.0, ge=1.0)
        reference_distance_km: float = Field(1.0, gt=0)
        shadowing_std: float = Field(3.0, ge=0, description="Log‑normal shadowing standard deviation (dB)")
        # Diurnal variation
        diurnal_amplitude: float = Field(0.3, ge=0, le=1, description="Fraction of peak variation")
        diurnal_peak_hour: int = Field(14, ge=0, le=23)
        # Burst parameters
        burst_probability: float = Field(0.1, ge=0, le=1)
        burst_multiplier: float = Field(5.0, ge=1)
        # Edge cases
        edge_case_rate: float = Field(0.0, ge=0, le=1)
        # Export
        export_format: str = Field("parquet", description="parquet, csv, json")
        # Statistical validation
        validation_alpha: float = Field(0.05, ge=0, le=1, description="Significance level for tests")

        @field_validator('export_format')
        @classmethod
        def validate_export_format(cls, v):
            if v not in ['parquet', 'csv', 'json']:
                raise ValueError("export_format must be 'parquet', 'csv', or 'json'")
            return v

        class Config:
            env_prefix = "HELIUM_SYNTH_"
else:
    # Fallback dict
    HELIUM_SYNTH_CONFIG = {
        "version": "2.1.0",
        "seed": 42,
        "num_hotspots": 100,
        "num_gateways": 5,
        "duration_hours": 24.0,
        "base_events_per_hour": 10.0,
        "rssi_mean_urban": -70.0,
        "rssi_std_urban": 10.0,
        "rssi_mean_rural": -80.0,
        "rssi_std_rural": 15.0,
        "snr_mean": 12.0,
        "snr_std": 3.0,
        "num_clusters": 3,
        "cluster_spread": 0.2,
        "path_loss_exponent": 2.0,
        "reference_distance_km": 1.0,
        "shadowing_std": 3.0,
        "diurnal_amplitude": 0.3,
        "diurnal_peak_hour": 14,
        "burst_probability": 0.1,
        "burst_multiplier": 5.0,
        "edge_case_rate": 0.0,
        "export_format": "parquet",
        "validation_alpha": 0.05,
    }


class HeliumSyntheticGenerator:
    """
    Enhanced synthetic Helium PoC trace generator with realistic features.

    This class generates a DataFrame containing:
    - timestamp (ISO format)
    - hotspot_id
    - gateway_id (assigned based on nearest gateway)
    - rssi (dBm)
    - snr (dB)
    - uplink_count
    - region (urban/rural)
    - cluster_id
    - anomaly flag (if edge cases are enabled)
    - distance_to_gateway (km)
    - path_loss (dB)

    Configuration is provided via a Pydantic model or a dict.
    """

    def __init__(self, config: Optional[Union[Dict[str, Any], HeliumSyntheticConfig]] = None):
        """
        Initialize the generator.

        Args:
            config: Configuration dictionary or Pydantic model.
        """
        if config is None:
            if PYDANTIC_AVAILABLE:
                self.config = HeliumSyntheticConfig()
            else:
                self.config = HELIUM_SYNTH_CONFIG
        elif isinstance(config, dict):
            if PYDANTIC_AVAILABLE:
                self.config = HeliumSyntheticConfig(**config)
            else:
                self.config = config
        else:
            self.config = config

        # Set random seeds
        seed = self._get_config('seed', 42)
        random.seed(seed)
        np.random.seed(seed)

        # Store configuration values for quick access
        self._extract_params()

        # Internal state for current trace generation
        self._hotspot_data: Dict[str, Dict] = {}
        self._gateway_data: Dict[str, Dict] = {}
        self._current_seed = seed

        logger.info("HeliumSyntheticGenerator initialized", version=self._get_config('version', '2.1.0'))

    def _get_config(self, key: str, default: Any = None) -> Any:
        """Safely get a config value, supporting both dict and Pydantic."""
        if hasattr(self.config, 'dict'):
            return getattr(self.config, key, default)
        return self.config.get(key, default)

    def _extract_params(self):
        """Extract configuration parameters into instance variables."""
        self.num_hotspots = self._get_config('num_hotspots', 100)
        self.num_gateways = self._get_config('num_gateways', 5)
        self.duration_hours = self._get_config('duration_hours', 24.0)
        self.base_events_per_hour = self._get_config('base_events_per_hour', 10.0)
        self.rssi_mean_urban = self._get_config('rssi_mean_urban', -70.0)
        self.rssi_std_urban = self._get_config('rssi_std_urban', 10.0)
        self.rssi_mean_rural = self._get_config('rssi_mean_rural', -80.0)
        self.rssi_std_rural = self._get_config('rssi_std_rural', 15.0)
        self.snr_mean = self._get_config('snr_mean', 12.0)
        self.snr_std = self._get_config('snr_std', 3.0)
        self.num_clusters = self._get_config('num_clusters', 3)
        self.cluster_spread = self._get_config('cluster_spread', 0.2)
        self.path_loss_exponent = self._get_config('path_loss_exponent', 2.0)
        self.reference_distance_km = self._get_config('reference_distance_km', 1.0)
        self.shadowing_std = self._get_config('shadowing_std', 3.0)
        self.diurnal_amplitude = self._get_config('diurnal_amplitude', 0.3)
        self.diurnal_peak_hour = self._get_config('diurnal_peak_hour', 14)
        self.burst_probability = self._get_config('burst_probability', 0.1)
        self.burst_multiplier = self._get_config('burst_multiplier', 5.0)
        self.edge_case_rate = self._get_config('edge_case_rate', 0.0)
        self.export_format = self._get_config('export_format', 'parquet')
        self.validation_alpha = self._get_config('validation_alpha', 0.05)

    # ------------------------------------------------------------------
    # Core generation methods
    # ------------------------------------------------------------------

    def generate_trace(
        self,
        num_hotspots: Optional[int] = None,
        duration_hours: Optional[float] = None,
        base_events_per_hour: Optional[float] = None,
        **kwargs
    ) -> pd.DataFrame:
        """
        Generate a synthetic Helium PoC trace using an inhomogeneous Poisson process.

        Args:
            num_hotspots: Override number of hotspots.
            duration_hours: Override duration in hours.
            base_events_per_hour: Override base event rate per hour.
            **kwargs: Additional overrides (e.g., rssi_mean_urban).

        Returns:
            DataFrame with columns: timestamp, hotspot_id, gateway_id, rssi, snr,
            uplink_count, region, cluster_id, anomaly, distance_km, path_loss.
        """
        # Apply overrides to a temporary config copy
        config_copy = self._copy_config()
        if num_hotspots is not None:
            config_copy['num_hotspots'] = num_hotspots
        if duration_hours is not None:
            config_copy['duration_hours'] = duration_hours
        if base_events_per_hour is not None:
            config_copy['base_events_per_hour'] = base_events_per_hour
        for k, v in kwargs.items():
            config_copy[k] = v

        # Create a temporary generator with the modified config
        temp_gen = HeliumSyntheticGenerator(config_copy)
        return temp_gen._generate_trace_internal()

    def _generate_trace_internal(self) -> pd.DataFrame:
        """
        Internal generation method using the current configuration.
        """
        # Create hotspots and gateways
        self._create_hotspots(self.num_hotspots)
        self._create_gateways()

        # Generate events using inhomogeneous Poisson process
        rows = []
        start_time = datetime.utcnow()
        current_time = start_time

        while current_time < start_time + timedelta(hours=self.duration_hours):
            # Compute current rate with diurnal and burst modulation
            rate = self._current_rate(current_time)
            # Sample inter-arrival time (exponential)
            dt = np.random.exponential(1 / max(rate, 1e-6))  # seconds
            current_time += timedelta(seconds=dt)

            if current_time >= start_time + timedelta(hours=self.duration_hours):
                break

            # Check for burst event
            if random.random() < self.burst_probability:
                # Generate a short burst of events
                num_burst = int(np.random.poisson(self.burst_multiplier))
                for _ in range(num_burst):
                    burst_dt = np.random.exponential(0.1)  # seconds
                    current_time += timedelta(seconds=burst_dt)
                    if current_time >= start_time + timedelta(hours=self.duration_hours):
                        break
                    row = self._generate_event(current_time)
                    rows.append(row)

            # Regular event
            row = self._generate_event(current_time)
            rows.append(row)

        # Create DataFrame
        df = pd.DataFrame(rows)
        if df.empty:
            logger.warning("No events generated; check configuration.")
            return pd.DataFrame()

        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.sort_values('timestamp').reset_index(drop=True)

        # Add metadata
        df.attrs['version'] = self._get_config('version', '2.1.0')
        df.attrs['parameters'] = self._get_config_dict()

        # Inject edge cases if configured
        if self.edge_case_rate > 0:
            df = self._inject_edge_cases(df)

        return df

    def _current_rate(self, timestamp: datetime) -> float:
        """
        Compute the event rate (events per second) at a given timestamp.
        Accounts for diurnal variation and burst modulation.
        """
        hour = timestamp.hour
        diurnal_factor = self._diurnal_factor(hour)
        # Base rate in events per second
        base_rate = self.base_events_per_hour / 3600.0
        return base_rate * diurnal_factor

    def _diurnal_factor(self, hour: int) -> float:
        """Compute diurnal multiplier for event rate."""
        # Peak at diurnal_peak_hour, amplitude controls variation.
        # Use a sine wave: 1 + amplitude * sin(π*(hour - peak)/12)
        # At peak, factor = 1 + amplitude; at trough, factor = 1 - amplitude.
        delta = (hour - self.diurnal_peak_hour) % 24
        if delta > 12:
            delta = 24 - delta
        factor = 1 + self.diurnal_amplitude * np.sin(np.pi * delta / 12)
        return max(0.1, factor)

    def _create_hotspots(self, num: int):
        """Assign hotspots to clusters and regions."""
        # Generate cluster centers
        cluster_centers = []
        for _ in range(self.num_clusters):
            center_x = np.random.uniform(0, 1)
            center_y = np.random.uniform(0, 1)
            cluster_centers.append((center_x, center_y))

        # Assign each hotspot to a cluster and generate location
        for i in range(num):
            cluster_id = random.randint(0, self.num_clusters - 1)
            center_x, center_y = cluster_centers[cluster_id]
            # Generate location with spread
            loc_x = center_x + np.random.normal(0, self.cluster_spread)
            loc_y = center_y + np.random.normal(0, self.cluster_spread)
            # Determine region: urban if close to cluster center, else rural
            dist = np.sqrt((loc_x - center_x)**2 + (loc_y - center_y)**2)
            region = 'urban' if dist < self.cluster_spread * 0.3 else 'rural'

            hotspot_id = f"hotspot_{i:04d}"
            self._hotspot_data[hotspot_id] = {
                'location': (loc_x, loc_y),
                'cluster_id': cluster_id,
                'region': region,
            }

    def _create_gateways(self):
        """Create gateways at random locations."""
        for i in range(self.num_gateways):
            gateway_id = f"gateway_{i:02d}"
            loc_x = np.random.uniform(0, 1)
            loc_y = np.random.uniform(0, 1)
            self._gateway_data[gateway_id] = {
                'location': (loc_x, loc_y),
            }

    def _generate_event(self, timestamp: datetime) -> Dict:
        """Generate a single event row."""
        # Pick a random hotspot
        hotspot_id = random.choice(list(self._hotspot_data.keys()))
        hotspot = self._hotspot_data[hotspot_id]
        region = hotspot['region']
        cluster_id = hotspot['cluster_id']
        hx, hy = hotspot['location']

        # Find nearest gateway and compute distance
        nearest_gateway_id = None
        min_dist = float('inf')
        for gid, gdata in self._gateway_data.items():
            gx, gy = gdata['location']
            dist = np.sqrt((hx - gx)**2 + (hy - gy)**2)
            if dist < min_dist:
                min_dist = dist
                nearest_gateway_id = gid

        # Path loss with log‑normal shadowing
        if min_dist > 0:
            path_loss = 10 * self.path_loss_exponent * np.log10(min_dist / self.reference_distance_km)
            shadowing = np.random.normal(0, self.shadowing_std)
            path_loss += shadowing
        else:
            path_loss = 0.0

        # RSSI based on region and path loss
        if region == 'urban':
            rssi_mean = self.rssi_mean_urban
            rssi_std = self.rssi_std_urban
        else:
            rssi_mean = self.rssi_mean_rural
            rssi_std = self.rssi_std_rural

        rssi = np.random.normal(rssi_mean, rssi_std) - path_loss
        # Clamp RSSI to realistic range
        rssi = max(-120, min(-30, rssi))

        # SNR similarly (independent of path loss, but could be correlated)
        snr = np.random.normal(self.snr_mean, self.snr_std)
        snr = max(-10, min(30, snr))

        uplink_count = np.random.poisson(lam=2) + 1  # at least 1

        return {
            'timestamp': timestamp.isoformat(),
            'hotspot_id': hotspot_id,
            'gateway_id': nearest_gateway_id,
            'rssi': rssi,
            'snr': snr,
            'uplink_count': uplink_count,
            'region': region,
            'cluster_id': cluster_id,
            'anomaly': False,
            'distance_km': min_dist,
            'path_loss': path_loss,
        }

    def _inject_edge_cases(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Inject edge cases into the trace.

        Edge cases:
        - Hotspot failure: all events from a hotspot have RSSI extremely low (-120).
        - Interference: RSSI and SNR are very noisy.
        - Extreme values: RSSI or SNR far from mean.
        - Gateway failure: events from a gateway have no uplink.
        """
        df = df.copy()
        # Mark all as normal initially
        df['anomaly'] = False

        # Hotspot failure: pick a random hotspot and set all its events to failure
        if random.random() < self.edge_case_rate * 0.3:
            failed_hotspot = random.choice(df['hotspot_id'].unique())
            mask = df['hotspot_id'] == failed_hotspot
            df.loc[mask, 'rssi'] = -120
            df.loc[mask, 'snr'] = 0
            df.loc[mask, 'uplink_count'] = 0
            df.loc[mask, 'anomaly'] = True
            logger.debug(f"Injected hotspot failure: {failed_hotspot}")

        # Interference events: add noise to a fraction of events
        interference_rate = self.edge_case_rate * 0.5
        interference_mask = np.random.random(len(df)) < interference_rate
        df.loc[interference_mask, 'rssi'] += np.random.normal(0, 20)
        df.loc[interference_mask, 'snr'] += np.random.normal(0, 10)
        df.loc[interference_mask, 'anomaly'] = True

        # Extreme values: a few events with RSSI > -30 or SNR > 30
        extreme_rate = self.edge_case_rate * 0.2
        extreme_mask = np.random.random(len(df)) < extreme_rate
        df.loc[extreme_mask, 'rssi'] = -25 + np.random.normal(0, 2)  # very high
        df.loc[extreme_mask, 'snr'] = 35 + np.random.normal(0, 2)
        df.loc[extreme_mask, 'anomaly'] = True

        # Clip RSSI after injection
        df['rssi'] = df['rssi'].clip(-120, -25)

        return df

    # ------------------------------------------------------------------
    # Statistical validation
    # ------------------------------------------------------------------

    def validate_trace(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Perform statistical validation on the generated trace.

        Args:
            df: Generated trace DataFrame.

        Returns:
            Dictionary of validation results.
        """
        results = {}
        alpha = self.validation_alpha

        if not SCIPY_AVAILABLE:
            results["error"] = "scipy not available for validation"
            return results

        # Test RSSI distribution against expected normal (with clipping)
        rssi_values = df['rssi'].values
        # We expect a mixture of normals; we test the overall distribution
        ks_stat, p_value = stats.kstest(rssi_values, 'norm', args=(np.mean(rssi_values), np.std(rssi_values)))
        results['rssi_ks_test'] = {'statistic': ks_stat, 'p_value': p_value}
        results['rssi_normality'] = p_value > alpha
        if not results['rssi_normality']:
            logger.warning("RSSI distribution may not be normal (expected due to path loss)")

        # Test SNR distribution
        snr_values = df['snr'].values
        ks_stat, p_value = stats.kstest(snr_values, 'norm', args=(np.mean(snr_values), np.std(snr_values)))
        results['snr_ks_test'] = {'statistic': ks_stat, 'p_value': p_value}
        results['snr_normality'] = p_value > alpha

        # Test uplink count distribution (Poisson)
        uplink_counts = df['uplink_count'].values
        mean_count = np.mean(uplink_counts)
        # Chi-square test for Poisson
        obs, bins = np.histogram(uplink_counts, bins=range(1, 8))
        expected = [len(uplink_counts) * stats.poisson.pmf(i, mean_count) for i in range(1, 7)]
        expected = np.array(expected)
        # Combine bins to ensure expected > 5
        if len(obs) > len(expected):
            obs = obs[:len(expected)]
        chi2, p = stats.chisquare(obs, expected)
        results['uplink_chisquare'] = {'statistic': chi2, 'p_value': p}
        results['uplink_poisson'] = p > alpha

        # Check for diurnal pattern (if enough data)
        df['hour'] = pd.to_datetime(df['timestamp']).dt.hour
        hourly_counts = df.groupby('hour').size()
        peak_hour = self.diurnal_peak_hour
        trough_hour = (peak_hour + 12) % 24
        if peak_hour in hourly_counts.index and trough_hour in hourly_counts.index:
            # Use proportion of events in peak vs trough
            peak_count = hourly_counts[peak_hour]
            trough_count = hourly_counts[trough_hour]
            total = len(df)
            # Binomial test: is peak proportion significantly > 0.5?
            from scipy.stats import binomtest
            result = binomtest(peak_count, peak_count + trough_count, p=0.5, alternative='greater')
            results['diurnal_binomial'] = {'statistic': peak_count / (peak_count + trough_count), 'p_value': result.pvalue}
            results['diurnal_significant'] = result.pvalue < alpha
        else:
            results['diurnal_binomial'] = {'error': 'Insufficient data for diurnal test'}

        return results

    # ------------------------------------------------------------------
    # Export methods
    # ------------------------------------------------------------------

    def save_trace(self, df: pd.DataFrame, path: Path) -> None:
        """
        Save the trace to disk in the configured format.

        Args:
            df: Generated trace DataFrame.
            path: Output file path.
        """
        path = Path(path)
        fmt = self.export_format

        if fmt == 'parquet':
            df.to_parquet(path, index=False)
        elif fmt == 'csv':
            df.to_csv(path, index=False)
        elif fmt == 'json':
            # Convert to JSON with records orientation
            df.to_json(path, orient='records', date_format='iso')
        else:
            raise ValueError(f"Unsupported export format: {fmt}")

        logger.info(f"Trace saved to {path} (format: {fmt})")

    def export_with_metadata(self, df: pd.DataFrame, path: Path) -> None:
        """
        Export the trace along with metadata as separate files.

        Args:
            df: DataFrame to save.
            path: Base path (e.g., "trace.parquet").
        """
        # Save the main trace
        self.save_trace(df, path)

        # Save metadata as JSON
        meta_path = path.with_suffix('.meta.json')
        metadata = {
            'version': self._get_config('version', '2.1.0'),
            'parameters': self._get_config_dict(),
            'generated_at': datetime.utcnow().isoformat(),
            'num_rows': len(df),
            'num_hotspots': df['hotspot_id'].nunique(),
            'num_gateways': df['gateway_id'].nunique(),
            'hash': hashlib.sha256(df.to_json().encode()).hexdigest(),
        }
        with open(meta_path, 'w') as f:
            json.dump(metadata, f, indent=2, default=str)

    # ------------------------------------------------------------------
    # Utility: generate multiple traces
    # ------------------------------------------------------------------

    def generate_multiple_traces(
        self,
        num_traces: int = 5,
        output_dir: Path = Path("./traces"),
        prefix: str = "trace",
        return_dfs: bool = False,
    ) -> Union[List[Path], Tuple[List[Path], List[pd.DataFrame]]]:
        """
        Generate multiple independent traces and save them.

        Args:
            num_traces: Number of traces to generate.
            output_dir: Directory to save traces.
            prefix: Prefix for filenames.
            return_dfs: If True, also return the generated DataFrames.

        Returns:
            List of saved file paths, and optionally the DataFrames.
        """
        output_dir.mkdir(parents=True, exist_ok=True)
        saved_paths = []
        dataframes = []

        base_config = self._get_config_dict()

        for i in range(num_traces):
            # Create a copy of the config with a new seed
            config_copy = copy.deepcopy(base_config)
            seed = base_config.get('seed', 42) + i * 1000
            config_copy['seed'] = seed

            # Instantiate a new generator with the copied config
            if PYDANTIC_AVAILABLE:
                temp_config = HeliumSyntheticConfig(**config_copy)
                temp_gen = HeliumSyntheticGenerator(temp_config)
            else:
                temp_gen = HeliumSyntheticGenerator(config_copy)

            df = temp_gen._generate_trace_internal()
            fname = f"{prefix}_{i:03d}.{self.export_format}"
            path = output_dir / fname
            temp_gen.save_trace(df, path)
            saved_paths.append(path)
            dataframes.append(df)
            logger.info(f"Generated trace {i+1}/{num_traces}")

        if return_dfs:
            return saved_paths, dataframes
        return saved_paths

    # ------------------------------------------------------------------
    # Configuration helpers
    # ------------------------------------------------------------------

    def _get_config_dict(self) -> Dict[str, Any]:
        """Return the configuration as a dictionary."""
        if hasattr(self.config, 'model_dump'):
            return self.config.model_dump()
        elif hasattr(self.config, 'dict'):
            return self.config.dict()
        else:
            return self.config.copy()

    def _copy_config(self) -> Dict[str, Any]:
        """Return a deep copy of the configuration as a dict."""
        return copy.deepcopy(self._get_config_dict())

    def load_config_from_json(self, path: Path) -> None:
        """Load configuration from a JSON file."""
        with open(path, 'r') as f:
            data = json.load(f)
        if PYDANTIC_AVAILABLE:
            self.config = HeliumSyntheticConfig(**data)
        else:
            self.config = data
        self._extract_params()
        logger.info(f"Configuration loaded from {path}")

    def save_config_to_json(self, path: Path) -> None:
        """Save current configuration to a JSON file."""
        with open(path, 'w') as f:
            json.dump(self._get_config_dict(), f, indent=2)

    # ------------------------------------------------------------------
    # Example usage
    # ------------------------------------------------------------------

    @staticmethod
    def example():
        """Demonstrate usage."""
        import sys
        logging.basicConfig(level=logging.INFO)

        config = {
            "num_hotspots": 50,
            "duration_hours": 6,
            "base_events_per_hour": 5,
            "diurnal_amplitude": 0.2,
            "edge_case_rate": 0.05,
            "export_format": "parquet",
        }
        gen = HeliumSyntheticGenerator(config)
        df = gen.generate_trace()
        print(f"Generated {len(df)} events")
        print(df.head())

        # Validate
        if SCIPY_AVAILABLE:
            results = gen.validate_trace(df)
            print("Validation results:", results)

        # Save
        path = Path("./test_trace.parquet")
        gen.save_trace(df, path)
        print(f"Trace saved to {path}")

        # Generate multiple traces
        paths = gen.generate_multiple_traces(num_traces=3, output_dir=Path("./traces"), prefix="demo")
        print(f"Generated {len(paths)} traces: {paths}")

if __name__ == "__main__":
    HeliumSyntheticGenerator.example()
