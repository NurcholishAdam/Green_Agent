import aiohttp
import sqlite3
import json
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict

class MaterialFootprintUpdater:
    """Fetches and caches product‑level material footprints."""
    def __init__(self, db_path: Path = Path("./material_catalog.db")):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        conn = sqlite3.connect(self.db_path)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS footprints (
                product_id TEXT PRIMARY KEY,
                embodied_carbon_kg REAL,
                rare_earth_kg REAL,
                total_mass_kg REAL,
                material_index REAL,
                source TEXT,
                last_updated TEXT
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_product_id ON footprints(product_id)")
        conn.close()

    async def update_catalog(self):
        """Fetch new data from BONSAI/FOOTPRINTDATA and refresh catalog."""
        # In production: call API or download CSV; here we use mock data.
        mock_data = {
            "gpu-a100": {"embodied_carbon_kg": 200, "rare_earth_kg": 0.01, "total_mass_kg": 2.5, "material_index": 1.2},
            "gpu-h100": {"embodied_carbon_kg": 250, "rare_earth_kg": 0.015, "total_mass_kg": 3.0, "material_index": 1.5},
            "edge-device": {"embodied_carbon_kg": 50, "rare_earth_kg": 0.002, "total_mass_kg": 0.5, "material_index": 0.6},
        }
        conn = sqlite3.connect(self.db_path)
        for pid, data in mock_data.items():
            conn.execute("""
                INSERT OR REPLACE INTO footprints (product_id, embodied_carbon_kg, rare_earth_kg, total_mass_kg, material_index, source, last_updated)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (pid, data['embodied_carbon_kg'], data['rare_earth_kg'], data['total_mass_kg'],
                  data['material_index'], "bonsai_mock", datetime.utcnow().isoformat()))
        conn.commit()
        conn.close()

    def get_footprint(self, product_id: str) -> Optional[Dict]:
        conn = sqlite3.connect(self.db_path)
        row = conn.execute(
            "SELECT embodied_carbon_kg, rare_earth_kg, total_mass_kg, material_index FROM footprints WHERE product_id = ?",
            (product_id,)
        ).fetchone()
        conn.close()
        if row:
            return {
                'embodied_carbon_kg': row[0],
                'rare_earth_kg': row[1],
                'total_mass_kg': row[2],
                'material_index': row[3]
            }
        return None
