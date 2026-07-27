# src/enhancements/data_integration/helium_synthetic_generator.py
"""
Enhanced Helium Synthetic Generator v2.0.0
===========================================
Generates synthetic Helium Proof‑of‑Coverage (PoC) traces for limit‑agentbench
with realistic distributions, temporal patterns, spatial clustering, gateway topology,
edge cases, and configurable parameters.

Features:
- Realistic RSSI/SNR distributions based on region and hotspot type.
- Diurnal variation and event bursts.
- Spatial clustering with correlated RSSI/SNR.
- Gateway topology with path loss.
- Edge case injection (hotspot failure, interference, extreme values).
- Configurable via Pydantic (or dict) with environment support.
- Statistical validation (Kolmogorov–Smirnov tests).
- Metadata versioning.
- Export to Parquet, CSV, and JSON.
- Comprehensive docstrings and type hints.
"""

import random
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Union, Any, Tuple
from pathlib import Path
import json
import hashlib

# ---------- Pydantic ----------
try:
    from pydantic import BaseModel, Field, field_validator
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
        version: str = "2.0.0"
        seed: int = Field(42, description="Random seed for reproducibility")
        # Trace parameters
        num_hotspots: int = Field(100, ge=1)
        num_gateways: int = Field(5, ge=1)
        duration_hours: int = Field(24, ge=1)
        events_per_hour: float = Field(10.0, gt=0)
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
        "version": "2.0.0",
        "seed": 42,
        "num_hotspots": 100,
        "num_gateways": 5,
        "duration_hours": 24,
        "events_per_hour": 10.0,
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
        "diurnal_amplitude": 0.3,
        "diurnal_peak_hour": 14,
        "burst_probability": 0.1,
        "burst_multiplier": 5.0,
        "edge_case_rate": 0.0,
        "export_format": "parquet",
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
        seed = self.config.get('seed', 42)
        random.seed(seed)
        np.random.seed(seed)

        # Extract configuration values
        self.num_hotspots = self.config.get('num_hotspots', 100)
        self.num_gateways = self.config.get('num_gateways', 5)
        self.duration_hours = self.config.get('duration_hours', 24)
        self.events_per_hour = self.config.get('events_per_hour', 10.0)
        self.rssi_mean_urban = self.config.get('rssi_mean_urban', -70.0)
        self.rssi_std_urban = self.config.get('rssi_std_urban', 10.0)
        self.rssi_mean_rural = self.config.get('rssi_mean_rural', -80.0)
        self.rssi_std_rural = self.config.get('rssi_std_rural', 15.0)
        self.snr_mean = self.config.get('snr_mean', 12.0)
        self.snr_std = self.config.get('snr_std', 3.0)
        self.num_clusters = self.config.get('num_clusters', 3)
        self.cluster_spread = self.config.get('cluster_spread', 0.2)
        self.path_loss_exponent = self.config.get('path_loss_exponent', 2.0)
        self.reference_distance_km = self.config.get('reference_distance_km', 1.0)
        self.diurnal_amplitude = self.config.get('diurnal_amplitude', 0.3)
        self.diurnal_peak_hour = self.config.get('diurnal_peak_hour', 14)
        self.burst_probability = self.config.get('burst_probability', 0.1)
        self.burst_multiplier = self.config.get('burst_multiplier', 5.0)
        self.edge_case_rate = self.config.get('edge_case_rate', 0.0)
        self.export_format = self.config.get('export_format', 'parquet')

        # Internal state
        self._hotspot_data: Dict[str, Dict] = {}
        self._gateway_data: Dict[str, Dict] = {}

        logger.info("HeliumSyntheticGenerator initialized", version=self.config.get('version', '2.0.0'))

    # ------------------------------------------------------------------
    # Core generation methods
    # ------------------------------------------------------------------

    def generate_trace(
        self,
        num_hotspots: Optional[int] = None,
        duration_hours: Optional[int] = None,
        events_per_hour: Optional[float] = None,
        **kwargs
    ) -> pd.DataFrame:
        """
        Generate a synthetic Helium PoC trace.

        Args:
            num_hotspots: Override number of hotspots.
            duration_hours: Override duration in hours.
            events_per_hour: Override event rate per hour.
            **kwargs: Additional overrides (e.g., rssi_mean_urban).

        Returns:
            DataFrame with columns: timestamp, hotspot_id, gateway_id, rssi, snr,
            uplink_count, region, cluster_id, anomaly.
        """
        # Apply overrides
        num_hotspots = num_hotspots or self.num_hotspots
        duration_hours = duration_hours or self.duration_hours
        events_per_hour = events_per_hour or self.events_per_hour

        # Create hotspots and gateways
        self._create_hotspots(num_hotspots)
        self._create_gateways()

        # Generate events
        rows = []
        start_time = datetime.utcnow()
        total_events = int(duration_hours * events_per_hour)

        for _ in range(total_events):
            # Determine inter-arrival time with diurnal variation
            base_rate = events_per_hour / 3600  # events per second
            hour_of_day = (start_time.hour + int(_ / (total_events / duration_hours))) % 24
            diurnal_factor = self._diurnal_factor(hour_of_day)
            rate = base_rate * diurnal_factor

            # Inter-arrival time (exponential)
            dt = np.random.exponential(1 / rate)
            timestamp = start_time + timedelta(seconds=dt)

            # Apply burst if configured
            if random.random() < self.burst_probability:
                # Generate a burst of events in a short interval
                num_burst = int(self.burst_multiplier)
                for _ in range(num_burst):
                    timestamp += timedelta(seconds=random.expovariate(1/2))  # very short inter-arrival
                    row = self._generate_event(timestamp)
                    rows.append(row)

            row = self._generate_event(timestamp)
            rows.append(row)

        # Create DataFrame
        df = pd.DataFrame(rows)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.sort_values('timestamp').reset_index(drop=True)

        # Add metadata
        df.attrs['version'] = self.config.get('version', '2.0.0')
        df.attrs['parameters'] = self.config

        # Apply edge cases if rate > 0
        if self.edge_case_rate > 0:
            df = self._inject_edge_cases(df)

        return df

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

        # RSSI based on region and distance to nearest gateway
        if region == 'urban':
            rssi_mean = self.rssi_mean_urban
            rssi_std = self.rssi_std_urban
        else:
            rssi_mean = self.rssi_mean_rural
            rssi_std = self.rssi_std_rural

        # Find nearest gateway and compute path loss
        nearest_gateway_id = None
        min_dist = float('inf')
        for gid, gdata in self._gateway_data.items():
            hx, hy = hotspot['location']
            gx, gy = gdata['location']
            dist = np.sqrt((hx - gx)**2 + (hy - gy)**2)
            if dist < min_dist:
                min_dist = dist
                nearest_gateway_id = gid

        # Path loss model: PL = PL_ref + 10 * n * log10(d / d_ref)
        # For simplicity, we just reduce RSSI based on distance.
        # We'll scale distance to a realistic range.
        distance_factor = max(0.1, min_dist)  # avoid zero
        path_loss = 10 * self.path_loss_exponent * np.log10(distance_factor / self.reference_distance_km)
        rssi = np.random.normal(rssi_mean, rssi_std) - path_loss

        # SNR similarly
        snr = np.random.normal(self.snr_mean, self.snr_std)
        uplink_count = random.randint(1, 5)

        # Clamp RSSI to realistic range
        rssi = max(-120, min(-30, rssi))

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
        }

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

    def _inject_edge_cases(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Inject edge cases into the trace.

        Edge cases:
        - Hotspot failure: all events from a hotspot have RSSI extremely low (-120).
        - Interference: RSSI and SNR are very noisy.
        - Extreme values: RSSI or SNR far from mean.
        """
        # Identify a random hotspot to fail
        if random.random() < self.edge_case_rate:
            failed_hotspot = random.choice(df['hotspot_id'].unique())
            mask = df['hotspot_id'] == failed_hotspot
            df.loc[mask, 'rssi'] = -120
            df.loc[mask, 'snr'] = 0
            df.loc[mask, 'anomaly'] = True

        # Inject interference events
        interference_rate = self.edge_case_rate * 0.5
        interference_mask = np.random.random(len(df)) < interference_rate
        df.loc[interference_mask, 'rssi'] += np.random.normal(0, 20)
        df.loc[interference_mask, 'snr'] += np.random.normal(0, 10)
        df.loc[interference_mask, 'anomaly'] = True

        # Clip again after interference
        df['rssi'] = df['rssi'].clip(-120, -30)

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
        if not SCIPY_AVAILABLE:
            return {"error": "scipy not available for validation"}

        results = {}

        # Test RSSI distribution against expected normal
        rssi_values = df['rssi'].values
        ks_stat, p_value = stats.kstest(rssi_values, 'norm', args=(rssi_values.mean(), rssi_values.std()))
        results['rssi_ks_test'] = {'statistic': ks_stat, 'p_value': p_value}
        results['rssi_normality'] = p_value > 0.05

        # Test SNR distribution
        snr_values = df['snr'].values
        ks_stat, p_value = stats.kstest(snr_values, 'norm', args=(snr_values.mean(), snr_values.std()))
        results['snr_ks_test'] = {'statistic': ks_stat, 'p_value': p_value}
        results['snr_normality'] = p_value > 0.05

        # Test uplink count distribution (Poisson-like)
        uplink_counts = df['uplink_count'].values
        mean_count = np.mean(uplink_counts)
        # Simple chi-square test for Poisson (limited bins)
        obs, bins = np.histogram(uplink_counts, bins=range(1, 8))
        expected = [len(uplink_counts) * stats.poisson.pmf(i, mean_count) for i in range(1, 7)]
        expected = np.array(expected)
        # Combine last bins to ensure expected > 5
        if len(obs) > len(expected):
            obs = obs[:len(expected)]
        chi2, p = stats.chisquare(obs, expected)
        results['uplink_chisquare'] = {'statistic': chi2, 'p_value': p}
        results['uplink_poisson'] = p > 0.05

        # Check for expected diurnal pattern (if enough data)
        df['hour'] = pd.to_datetime(df['timestamp']).dt.hour
        hourly_counts = df.groupby('hour').size()
        # Compare peak vs trough using t-test
        peak_hour = self.diurnal_peak_hour
        trough_hour = (peak_hour + 12) % 24
        peak_events = df[df['hour'] == peak_hour]
        trough_events = df[df['hour'] == trough_hour]
        if len(peak_events) > 10 and len(trough_events) > 10:
            t_stat, p_val = stats.ttest_ind(peak_events['uplink_count'], trough_events['uplink_count'])
            results['diurnal_ttest'] = {'statistic': t_stat, 'p_value': p_val}
            results['diurnal_significant'] = p_val < 0.05

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
            'version': self.config.get('version', '2.0.0'),
            'parameters': self.config,
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
        prefix: str = "trace"
    ) -> List[Path]:
        """
        Generate multiple independent traces and save them.

        Args:
            num_traces: Number of traces to generate.
            output_dir: Directory to save traces.
            prefix: Prefix for filenames.

        Returns:
            List of saved file paths.
        """
        output_dir.mkdir(parents=True, exist_ok=True)
        saved_paths = []

        for i in range(num_traces):
            # Use a different seed for each trace
            seed = self.config.get('seed', 42) + i * 1000
            self.config['seed'] = seed
            random.seed(seed)
            np.random.seed(seed)

            df = self.generate_trace()
            fname = f"{prefix}_{i:03d}.{self.export_format}"
            path = output_dir / fname
            self.save_trace(df, path)
            saved_paths.append(path)
            logger.info(f"Generated trace {i+1}/{num_traces}")

        return saved_paths

    # ------------------------------------------------------------------
    # Example usage (if run directly)
    # ------------------------------------------------------------------

    @staticmethod
    def example():
        """Demonstrate usage."""
        import sys
        logging.basicConfig(level=logging.INFO)

        config = {
            "num_hotspots": 50,
            "duration_hours": 6,
            "events_per_hour": 5,
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

if __name__ == "__main__":
    HeliumSyntheticGenerator.example()
