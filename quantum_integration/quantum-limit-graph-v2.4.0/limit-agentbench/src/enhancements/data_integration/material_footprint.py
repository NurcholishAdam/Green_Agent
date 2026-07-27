# src/enhancements/data_integration/material_footprint.py
"""
Enhanced Material Footprint Updater v2.0.0
===========================================
Fetches and caches product‑level material footprints from BONSAI/FOOTPRINTDATA.
Provides real API integration, caching with TTL, retries, circuit breaker,
logging, metrics, and configuration via Pydantic.

Features:
- Real API integration with aiohttp (stubbed but ready).
- Async session pooling.
- Retry with exponential backoff using tenacity.
- Circuit breaker per external source.
- Structured logging via structlog.
- Prometheus metrics for calls, errors, cache hits/misses.
- Caching with configurable TTL (via SQLite `last_updated`).
- Pydantic configuration with environment variable support.
- Methods to get, list, refresh, delete, export, import footprints.
- Data validation with Pydantic models.
- Comprehensive docstrings and error handling.
"""

import asyncio
import logging
import time
import json
import sqlite3
from pathlib import Path
from typing import Dict, List, Optional, Any, Union
from datetime import datetime, timedelta
import aiohttp
from aiohttp import ClientTimeout, ClientError

# ---------- Pydantic ----------
try:
    from pydantic import BaseModel, Field, field_validator
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

# ---------- Tenacity (retry) ----------
try:
    from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, before_sleep_log
    TENACITY_AVAILABLE = True
except ImportError:
    TENACITY_AVAILABLE = False

# ---------- Circuit breaker ----------
try:
    from ..circuit_breaker import CircuitBreaker
    CIRCUIT_BREAKER_AVAILABLE = True
except ImportError:
    # Fallback simple circuit breaker
    class CircuitBreaker:
        def __init__(self, name, failure_threshold=5, recovery_timeout=30):
            self.name = name
            self.failure_threshold = failure_threshold
            self.recovery_timeout = recovery_timeout
        async def call(self, func, *args, **kwargs):
            return await func(*args, **kwargs)
    CIRCUIT_BREAKER_AVAILABLE = False

# ---------- Prometheus ----------
try:
    from prometheus_client import Counter, Gauge, Histogram
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

# ---------- Structlog ----------
try:
    import structlog
    logger = structlog.get_logger(__name__)
except ImportError:
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO)

# ============================================================================
# Configuration
# ============================================================================
if PYDANTIC_AVAILABLE:
    class MaterialConfig(BaseModel):
        """Configuration for MaterialFootprintUpdater."""
        # Database
        db_path: Path = Field(Path("./material_catalog.db"))
        # API endpoints (stubs)
        bonsai_api_url: str = Field("https://api.bonsai.uno/v1/footprints")
        footprintdata_api_url: str = Field("https://api.footprintdata.org/v1/products")
        # API keys (optional)
        bonsai_api_key: Optional[str] = None
        footprintdata_api_key: Optional[str] = None
        # Cache TTL (seconds)
        cache_ttl: int = Field(86400 * 7, ge=0)  # 7 days
        # Retry settings
        retry_attempts: int = Field(3, ge=0)
        retry_min_wait: float = Field(1.0, gt=0)
        retry_max_wait: float = Field(10.0, gt=0)
        # Circuit breaker
        circuit_breaker_threshold: int = Field(5, ge=1)
        circuit_breaker_timeout: float = Field(30.0, ge=1)
        # Request timeout (seconds)
        request_timeout: float = Field(10.0, ge=1)
        # Enable metrics
        enable_prometheus: bool = True

        class Config:
            env_prefix = "MATERIAL_"
else:
    # Fallback dict
    MATERIAL_CONFIG = {
        "db_path": Path("./material_catalog.db"),
        "bonsai_api_url": "https://api.bonsai.uno/v1/footprints",
        "footprintdata_api_url": "https://api.footprintdata.org/v1/products",
        "bonsai_api_key": None,
        "footprintdata_api_key": None,
        "cache_ttl": 86400 * 7,
        "retry_attempts": 3,
        "retry_min_wait": 1.0,
        "retry_max_wait": 10.0,
        "circuit_breaker_threshold": 5,
        "circuit_breaker_timeout": 30.0,
        "request_timeout": 10.0,
        "enable_prometheus": True,
    }

# ============================================================================
# Data Models (Pydantic)
# ============================================================================
if PYDANTIC_AVAILABLE:
    class Footprint(BaseModel):
        """Validated footprint data."""
        product_id: str
        embodied_carbon_kg: float
        rare_earth_kg: float
        total_mass_kg: float
        material_index: float
        source: str
        last_updated: datetime

        @field_validator('material_index')
        @classmethod
        def material_index_non_negative(cls, v):
            if v < 0:
                raise ValueError("material_index must be non-negative")
            return v
else:
    # Fallback dataclass
    from dataclasses import dataclass

    @dataclass
    class Footprint:
        product_id: str
        embodied_carbon_kg: float
        rare_earth_kg: float
        total_mass_kg: float
        material_index: float
        source: str
        last_updated: datetime

# ============================================================================
# MaterialFootprintUpdater (Enhanced)
# ============================================================================

class MaterialFootprintUpdater:
    """
    Enhanced material footprint updater with real API integration, caching,
    retries, circuit breaker, logging, and metrics.
    """

    def __init__(
        self,
        config: Optional[Union[Dict[str, Any], MaterialConfig]] = None,
    ):
        """
        Initialize the updater.

        Args:
            config: Configuration dictionary or Pydantic model.
        """
        if config is None:
            if PYDANTIC_AVAILABLE:
                self.config = MaterialConfig()
            else:
                self.config = MATERIAL_CONFIG
        elif isinstance(config, dict):
            if PYDANTIC_AVAILABLE:
                self.config = MaterialConfig(**config)
            else:
                self.config = config
        else:
            self.config = config

        self.db_path = self.config.get('db_path', Path("./material_catalog.db"))
        self.cache_ttl = self.config.get('cache_ttl', 86400 * 7)
        self.bonsai_api_url = self.config.get('bonsai_api_url', "https://api.bonsai.uno/v1/footprints")
        self.bonsai_api_key = self.config.get('bonsai_api_key')
        self.footprintdata_api_url = self.config.get('footprintdata_api_url', "https://api.footprintdata.org/v1/products")
        self.footprintdata_api_key = self.config.get('footprintdata_api_key')
        self.request_timeout = self.config.get('request_timeout', 10.0)

        # Initialize database
        self._init_db()

        # Session management
        self._session: Optional[aiohttp.ClientSession] = None
        self._session_lock = asyncio.Lock()

        # Circuit breakers per source
        self._circuit_breakers = {
            "bonsai": CircuitBreaker(
                name="material_bonsai",
                failure_threshold=self.config.get('circuit_breaker_threshold', 5),
                recovery_timeout=self.config.get('circuit_breaker_timeout', 30.0),
            ),
            "footprintdata": CircuitBreaker(
                name="material_footprintdata",
                failure_threshold=self.config.get('circuit_breaker_threshold', 5),
                recovery_timeout=self.config.get('circuit_breaker_timeout', 30.0),
            ),
        }

        # Prometheus metrics
        if PROMETHEUS_AVAILABLE and self.config.get('enable_prometheus', True):
            self.metrics = {
                'calls': Counter('material_api_calls_total', 'Material API calls', ['source', 'status']),
                'errors': Counter('material_api_errors_total', 'Material API errors', ['source']),
                'latency': Histogram('material_api_latency_seconds', 'Material API latency', ['source']),
                'cache_hits': Counter('material_cache_hits_total', 'Cache hits'),
                'cache_misses': Counter('material_cache_misses_total', 'Cache misses'),
                'cache_size': Gauge('material_cache_size', 'Number of cached footprints'),
            }
        else:
            self.metrics = None

        logger.info("MaterialFootprintUpdater initialized", db_path=str(self.db_path))

    def _init_db(self):
        """Initialize SQLite database."""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
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

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create an aiohttp ClientSession with connection pooling."""
        async with self._session_lock:
            if self._session is None or self._session.closed:
                timeout = ClientTimeout(total=self.request_timeout)
                connector = aiohttp.TCPConnector(limit=10, ttl_dns_cache=300)
                self._session = aiohttp.ClientSession(
                    connector=connector,
                    timeout=timeout,
                    raise_for_status=True,
                )
            return self._session

    async def close(self):
        """Close the underlying session."""
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None

    # ---------- Core methods ----------
    async def update_catalog(self, force_refresh: bool = False) -> int:
        """
        Fetch new data from BONSAI and FOOTPRINTDATA and refresh the catalog.

        Args:
            force_refresh: If True, ignore cache TTL for all products.

        Returns:
            Number of updated entries.
        """
        updated_count = 0

        # Try BONSAI
        try:
            await self._update_from_source("bonsai", force_refresh)
            updated_count += 1
        except Exception as e:
            logger.error("BONSAI update failed", error=str(e))

        # Try FOOTPRINTDATA
        try:
            await self._update_from_source("footprintdata", force_refresh)
            updated_count += 1
        except Exception as e:
            logger.error("FOOTPRINTDATA update failed", error=str(e))

        # If no updates from API, use mock data as fallback
        if updated_count == 0:
            logger.info("No API updates, using mock data")
            self._seed_mock_data()

        # Update cache size metric
        if self.metrics:
            conn = sqlite3.connect(self.db_path)
            count = conn.execute("SELECT COUNT(*) FROM footprints").fetchone()[0]
            conn.close()
            self.metrics['cache_size'].set(count)

        return updated_count

    async def _update_from_source(self, source: str, force_refresh: bool = False):
        """Fetch and update footprints from a specific source."""
        async def fetch():
            session = await self._get_session()
            if source == "bonsai":
                url = self.bonsai_api_url
                headers = {}
                if self.bonsai_api_key:
                    headers["Authorization"] = f"Bearer {self.bonsai_api_key}"
            else:  # footprintdata
                url = self.footprintdata_api_url
                headers = {}
                if self.footprintdata_api_key:
                    headers["Authorization"] = f"Bearer {self.footprintdata_api_key}"

            async with session.get(url, headers=headers) as resp:
                if resp.status != 200:
                    raise aiohttp.ClientError(f"API returned {resp.status}")
                data = await resp.json()
                return data

        # Use retry and circuit breaker
        if TENACITY_AVAILABLE:
            @retry(
                stop=stop_after_attempt(self.config.get('retry_attempts', 3)),
                wait=wait_exponential(
                    multiplier=1,
                    min=self.config.get('retry_min_wait', 1.0),
                    max=self.config.get('retry_max_wait', 10.0),
                ),
                retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError)),
                before_sleep=before_sleep_log(logger, logging.WARNING),
            )
            async def fetch_with_retry():
                return await fetch()
        else:
            async def fetch_with_retry():
                for attempt in range(self.config.get('retry_attempts', 3)):
                    try:
                        return await fetch()
                    except Exception as e:
                        if attempt == self.config.get('retry_attempts', 3) - 1:
                            raise
                        wait = min(
                            self.config.get('retry_min_wait', 1.0) * (2 ** attempt),
                            self.config.get('retry_max_wait', 10.0),
                        )
                        await asyncio.sleep(wait)

        start_time = time.time()
        try:
            data = await self._circuit_breakers[source].call(fetch_with_retry)
            if self.metrics:
                self.metrics['calls'].labels(source=source, status='success').inc()
                self.metrics['latency'].labels(source=source).observe(time.time() - start_time)

            # Parse and store footprints
            conn = sqlite3.connect(self.db_path)
            now = datetime.utcnow().isoformat()
            # Assuming data is a list of footprint objects
            # Adapt to actual API response structure.
            # For demonstration, we handle a list of dicts.
            if isinstance(data, list):
                for item in data:
                    product_id = item.get('product_id')
                    if not product_id:
                        continue
                    # Check if we need to update (TTL)
                    if not force_refresh:
                        row = conn.execute(
                            "SELECT last_updated FROM footprints WHERE product_id = ?",
                            (product_id,)
                        ).fetchone()
                        if row:
                            last_updated = datetime.fromisoformat(row[0])
                            if (datetime.utcnow() - last_updated).total_seconds() < self.cache_ttl:
                                continue
                    # Insert/update
                    conn.execute("""
                        INSERT OR REPLACE INTO footprints
                        (product_id, embodied_carbon_kg, rare_earth_kg, total_mass_kg, material_index, source, last_updated)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, (
                        product_id,
                        item.get('embodied_carbon_kg', 0.0),
                        item.get('rare_earth_kg', 0.0),
                        item.get('total_mass_kg', 0.0),
                        item.get('material_index', 1.0),
                        source,
                        now,
                    ))
            conn.commit()
            conn.close()
            logger.info("Updated catalog from", source=source, count=len(data) if isinstance(data, list) else 0)
        except Exception as e:
            if self.metrics:
                self.metrics['errors'].labels(source=source).inc()
                self.metrics['calls'].labels(source=source, status='error').inc()
            logger.error("Source update failed", source=source, error=str(e))
            raise

    def _seed_mock_data(self):
        """Seed the database with mock data if empty."""
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
            """, (
                pid,
                data['embodied_carbon_kg'],
                data['rare_earth_kg'],
                data['total_mass_kg'],
                data['material_index'],
                "mock",
                datetime.utcnow().isoformat()
            ))
        conn.commit()
        conn.close()
        logger.info("Seeded mock data")

    # ---------- Public methods ----------
    def get_footprint(self, product_id: str) -> Optional[Footprint]:
        """
        Retrieve a footprint from the cache.

        Args:
            product_id: Product identifier.

        Returns:
            Footprint object if found, else None.
        """
        conn = sqlite3.connect(self.db_path)
        row = conn.execute(
            "SELECT embodied_carbon_kg, rare_earth_kg, total_mass_kg, material_index, source, last_updated FROM footprints WHERE product_id = ?",
            (product_id,)
        ).fetchone()
        conn.close()
        if not row:
            if self.metrics:
                self.metrics['cache_misses'].inc()
            return None
        if self.metrics:
            self.metrics['cache_hits'].inc()
        return Footprint(
            product_id=product_id,
            embodied_carbon_kg=row[0],
            rare_earth_kg=row[1],
            total_mass_kg=row[2],
            material_index=row[3],
            source=row[4],
            last_updated=datetime.fromisoformat(row[5]),
        )

    async def get_or_fetch_footprint(self, product_id: str, force_refresh: bool = False) -> Optional[Footprint]:
        """
        Get a footprint, fetching from API if not found or expired.

        Args:
            product_id: Product identifier.
            force_refresh: If True, ignore cache and fetch fresh.

        Returns:
            Footprint object or None.
        """
        # Check cache first
        fp = self.get_footprint(product_id)
        if fp and not force_refresh:
            age = (datetime.utcnow() - fp.last_updated).total_seconds()
            if age < self.cache_ttl:
                return fp

        # Fetch from API
        # For simplicity, we just update the catalog for all products.
        # In production, you'd fetch only this product via a specific API endpoint.
        await self.update_catalog(force_refresh=True)
        return self.get_footprint(product_id)

    def list_products(self) -> List[str]:
        """List all product IDs in the cache."""
        conn = sqlite3.connect(self.db_path)
        rows = conn.execute("SELECT product_id FROM footprints").fetchall()
        conn.close()
        return [row[0] for row in rows]

    def delete_footprint(self, product_id: str) -> bool:
        """Delete a footprint from the cache."""
        conn = sqlite3.connect(self.db_path)
        conn.execute("DELETE FROM footprints WHERE product_id = ?", (product_id,))
        conn.commit()
        deleted = conn.total_changes > 0
        conn.close()
        if deleted:
            logger.info("Deleted footprint", product_id=product_id)
        return deleted

    def clear_cache(self) -> None:
        """Clear all footprints from the cache."""
        conn = sqlite3.connect(self.db_path)
        conn.execute("DELETE FROM footprints")
        conn.commit()
        conn.close()
        logger.info("Cache cleared")

    def export_catalog(self, path: Path) -> None:
        """Export the catalog to a JSON file."""
        conn = sqlite3.connect(self.db_path)
        rows = conn.execute("SELECT product_id, embodied_carbon_kg, rare_earth_kg, total_mass_kg, material_index, source, last_updated FROM footprints").fetchall()
        conn.close()
        data = []
        for row in rows:
            data.append({
                "product_id": row[0],
                "embodied_carbon_kg": row[1],
                "rare_earth_kg": row[2],
                "total_mass_kg": row[3],
                "material_index": row[4],
                "source": row[5],
                "last_updated": row[6],
            })
        with open(path, 'w') as f:
            json.dump(data, f, indent=2)
        logger.info("Catalog exported", path=str(path))

    def import_catalog(self, path: Path) -> int:
        """Import a catalog from a JSON file."""
        with open(path, 'r') as f:
            data = json.load(f)
        conn = sqlite3.connect(self.db_path)
        count = 0
        for item in data:
            conn.execute("""
                INSERT OR REPLACE INTO footprints
                (product_id, embodied_carbon_kg, rare_earth_kg, total_mass_kg, material_index, source, last_updated)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                item['product_id'],
                item['embodied_carbon_kg'],
                item['rare_earth_kg'],
                item['total_mass_kg'],
                item['material_index'],
                item['source'],
                item['last_updated'],
            ))
            count += 1
        conn.commit()
        conn.close()
        logger.info("Catalog imported", path=str(path), count=count)
        return count

    # ---------- Async context manager ----------
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

# ============================================================================
# Convenience factory
# ============================================================================
def create_material_updater(
    config: Optional[Dict[str, Any]] = None,
) -> MaterialFootprintUpdater:
    """
    Factory to create a fully configured MaterialFootprintUpdater.
    """
    return MaterialFootprintUpdater(config)

# ============================================================================
# Example usage
# ============================================================================
if __name__ == "__main__":
    import asyncio
    import sys
    sys.path.append('../')

    async def main():
        config = {
            "db_path": Path("./test_material.db"),
            "cache_ttl": 3600,
            "bonsai_api_url": "https://api.example.com/bonsai",
            "footprintdata_api_url": "https://api.example.com/footprintdata",
        }
        updater = create_material_updater(config)

        # Update catalog (will use mock data if API fails)
        await updater.update_catalog()

        # Get a footprint
        fp = updater.get_footprint("gpu-a100")
        if fp:
            print(f"Footprint: {fp.product_id}, carbon: {fp.embodied_carbon_kg} kg, material index: {fp.material_index}")

        # List products
        products = updater.list_products()
        print("Products:", products)

        await updater.close()

    asyncio.run(main())
