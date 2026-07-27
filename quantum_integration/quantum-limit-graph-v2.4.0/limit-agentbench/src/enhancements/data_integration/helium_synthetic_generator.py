import random
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict

class HeliumSyntheticGenerator:
    """Generates synthetic Helium Proof‑of‑Coverage traces for limit‑agentbench."""
    def __init__(self, seed: int = 42):
        random.seed(seed)
        np.random.seed(seed)

    def generate_trace(
        self,
        num_hotspots: int = 100,
        duration_hours: int = 24,
        events_per_hour: int = 10,
        rssi_mean: float = -70,
        rssi_std: float = 10
    ) -> pd.DataFrame:
        """Generate a synthetic Helium PoC trace."""
        rows = []
        start_time = datetime.utcnow()
        for _ in range(duration_hours * events_per_hour):
            timestamp = start_time + timedelta(seconds=random.expovariate(1/3600))
            hotspot_id = f"hotspot_{random.randint(0, num_hotspots-1):04d}"
            rssi = np.random.normal(rssi_mean, rssi_std)
            snr = np.random.normal(12, 3)
            rows.append({
                'timestamp': timestamp.isoformat(),
                'hotspot_id': hotspot_id,
                'rssi': rssi,
                'snr': snr,
                'uplink_count': random.randint(1, 5)
            })
        df = pd.DataFrame(rows)
        df['rssi'] = df['rssi'].clip(-120, -30)
        return df

    def save_trace(self, df: pd.DataFrame, path: str):
        df.to_parquet(path, index=False)
