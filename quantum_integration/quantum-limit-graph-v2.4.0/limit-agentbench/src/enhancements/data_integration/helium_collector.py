import aiohttp
import asyncio
import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional
from ..cache.cache_manager import CacheManager

class HeliumCollector:
    """Collects Helium hotspot connectivity data from API or snapshots."""
    def __init__(self, cache: CacheManager, snapshot_path: Optional[Path] = None):
        self.cache = cache
        self.snapshot_path = snapshot_path
        self.live_api_url = "https://api.helium.io/v1/"

    async def get_connectivity_score(self, hotspot_id: str) -> float:
        """Compute a connectivity score (0-1) for a hotspot."""
        cache_key = f"helium:score:{hotspot_id}"
        cached = await self.cache.get(cache_key)
        if cached is not None:
            return float(cached)

        data = await self._fetch_hotspot_data(hotspot_id)
        if not data:
            score = 0.5
        else:
            rssi_values = [entry['rssi'] for entry in data if 'rssi' in entry]
            if not rssi_values:
                score = 0.5
            else:
                avg_rssi = sum(rssi_values) / len(rssi_values)
                # RSSI typically -120 to -30 dBm; normalize to 0-1
                score = max(0, min(1, (avg_rssi + 120) / 90))
        await self.cache.set(cache_key, str(score), ttl=600)  # 10 minutes
        return score

    async def _fetch_hotspot_data(self, hotspot_id: str) -> List[Dict]:
        if self.snapshot_path and self.snapshot_path.exists():
            # Load from Parquet snapshot
            df = pd.read_parquet(self.snapshot_path)
            filtered = df[df['hotspot_id'] == hotspot_id]
            if not filtered.empty:
                return filtered.to_dict('records')
        # Fallback to live API (stub)
        await asyncio.sleep(0.1)
        # Simulate a few readings
        return [
            {'hotspot_id': hotspot_id, 'rssi': -70, 'snr': 12, 'timestamp': '2025-01-01T00:00:00'},
            {'hotspot_id': hotspot_id, 'rssi': -65, 'snr': 15, 'timestamp': '2025-01-01T00:01:00'},
        ]

    async def fetch_batch_scores(self, hotspot_ids: List[str]) -> Dict[str, float]:
        """Fetch scores for multiple hotspots concurrently."""
        tasks = [self.get_connectivity_score(hid) for hid in hotspot_ids]
        results = await asyncio.gather(*tasks)
        return {hid: score for hid, score in zip(hotspot_ids, results)}
