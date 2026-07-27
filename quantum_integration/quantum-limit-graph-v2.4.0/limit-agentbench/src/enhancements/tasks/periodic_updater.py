# src/enhancements/tasks/periodic_updater.py
"""
Enhanced Periodic Updater for Green Agent
=========================================
Celery tasks for periodic updates of sustainability data.

Features:
- Async‑aware tasks using asyncio.run() (with fallback to sync).
- Error handling with retries (Celery built‑in).
- Configuration via environment variables.
- Structured logging (structlog) with fallback.
- Prometheus metrics for task execution (optional).
- Real implementation for helium snapshot download.
- Concurrent fetching for carbon intensity.
- Configurable regions and schedules.
"""

import asyncio
import logging
import os
import time
from typing import List, Optional
from celery import Celery
from celery.signals import task_failure, task_success, task_retry

# ---------- Prometheus ----------
try:
    from prometheus_client import Counter, Histogram
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

# ---------- Local imports ----------
from ..cache.cache_manager import CacheManager
from ..data_integration.carbon_intensity import CarbonIntensityFetcher
from ..data_integration.material_footprint import MaterialFootprintUpdater
from ..data_integration.helium_collector import HeliumCollector

# ============================================================================
# Configuration from environment
# ============================================================================
REDIS_URL = os.getenv('CELERY_BROKER_URL', 'redis://localhost:6379/0')
REGIONS = os.getenv('CARBON_REGIONS', 'us-east,us-west,eu-west,eu-north,asia-east,asia-southeast').split(',')
HELIUM_SNAPSHOT_URL = os.getenv('HELIUM_SNAPSHOT_URL', 'https://example.com/helium_snapshot.parquet')
HELIUM_SNAPSHOT_PATH = os.getenv('HELIUM_SNAPSHOT_PATH', './helium_snapshot.parquet')

# Celery app
app = Celery('green_agent', broker=REDIS_URL)
app.conf.update(
    task_serializer='json',
    result_serializer='json',
    accept_content=['json'],
    timezone='UTC',
    enable_utc=True,
    task_track_started=True,
    task_time_limit=600,          # 10 minutes max
    task_soft_time_limit=540,     # 9 minutes soft limit
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    task_retry_backoff_max=600,   # max 10 minutes between retries
)

# Prometheus metrics (if enabled)
if PROMETHEUS_AVAILABLE:
    task_metrics = {
        'carbon_success': Counter('carbon_update_success_total', 'Carbon update success count'),
        'carbon_failure': Counter('carbon_update_failure_total', 'Carbon update failure count'),
        'material_success': Counter('material_update_success_total', 'Material update success count'),
        'material_failure': Counter('material_update_failure_total', 'Material update failure count'),
        'helium_success': Counter('helium_update_success_total', 'Helium update success count'),
        'helium_failure': Counter('helium_update_failure_total', 'Helium update failure count'),
        'task_duration': Histogram('periodic_task_duration_seconds', 'Task duration', ['task_name']),
    }
else:
    task_metrics = {}


# ============================================================================
# Task signals for logging and metrics
# ============================================================================
@task_success.connect
def task_success_handler(sender, **kwargs):
    """Log task success and update metrics."""
    task_name = sender.name
    logger.info("Task succeeded", task=task_name)
    if PROMETHEUS_AVAILABLE and task_name in task_metrics:
        # Custom: we can increment specific success counters in each task
        pass

@task_failure.connect
def task_failure_handler(sender, **kwargs):
    """Log task failure and update metrics."""
    task_name = sender.name
    logger.error("Task failed", task=task_name, exc_info=kwargs.get('einfo'))
    if PROMETHEUS_AVAILABLE:
        if 'carbon' in task_name:
            task_metrics['carbon_failure'].inc()
        elif 'material' in task_name:
            task_metrics['material_failure'].inc()
        elif 'helium' in task_name:
            task_metrics['helium_failure'].inc()


# ============================================================================
# Celery tasks
# ============================================================================

@app.task(
    bind=True,
    name='src.enhancements.tasks.periodic_updater.update_carbon_intensity',
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_backoff_max=600,
    max_retries=3,
)
def update_carbon_intensity(self):
    """Refresh carbon intensity for all key regions concurrently."""
    start_time = time.time()
    logger.info("Starting carbon intensity update", regions=REGIONS)

    try:
        cache = CacheManager()
        fetcher = CarbonIntensityFetcher(cache)

        async def fetch_all():
            tasks = [fetcher.get_intensity(region) for region in REGIONS]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            # Log results
            for region, result in zip(REGIONS, results):
                if isinstance(result, Exception):
                    logger.error("Carbon intensity fetch failed", region=region, error=str(result))
                else:
                    logger.debug("Carbon intensity fetched", region=region, intensity=result)
            return results

        results = asyncio.run(fetch_all())
        # Count failures
        failures = sum(1 for r in results if isinstance(r, Exception))
        if failures > 0:
            logger.warning("Carbon intensity update completed with failures", total=len(REGIONS), failures=failures)

        if PROMETHEUS_AVAILABLE:
            task_metrics['carbon_success'].inc()
            task_metrics['task_duration'].labels(task_name='update_carbon_intensity').observe(time.time() - start_time)

        return {"status": "success", "regions_updated": len(REGIONS) - failures, "total": len(REGIONS)}

    except Exception as e:
        logger.error("Carbon intensity update failed", error=str(e), exc_info=True)
        # Retry if not exhausted
        raise self.retry(exc=e)


@app.task(
    bind=True,
    name='src.enhancements.tasks.periodic_updater.update_material_catalog',
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_backoff_max=600,
    max_retries=3,
)
def update_material_catalog(self):
    """Refresh material footprint catalog."""
    start_time = time.time()
    logger.info("Starting material catalog update")

    try:
        updater = MaterialFootprintUpdater()

        async def update():
            await updater.update_catalog()

        asyncio.run(update())

        if PROMETHEUS_AVAILABLE:
            task_metrics['material_success'].inc()
            task_metrics['task_duration'].labels(task_name='update_material_catalog').observe(time.time() - start_time)

        logger.info("Material catalog updated successfully")
        return {"status": "success"}

    except Exception as e:
        logger.error("Material catalog update failed", error=str(e), exc_info=True)
        raise self.retry(exc=e)


@app.task(
    bind=True,
    name='src.enhancements.tasks.periodic_updater.update_helium_snapshot',
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_backoff_max=600,
    max_retries=3,
)
def update_helium_snapshot(self):
    """Download the latest Helium snapshot from a remote URL."""
    start_time = time.time()
    logger.info("Starting helium snapshot update", url=HELIUM_SNAPSHOT_URL, dest=HELIUM_SNAPSHOT_PATH)

    try:
        import aiohttp
        import aiofiles

        async def download():
            async with aiohttp.ClientSession() as session:
                async with session.get(HELIUM_SNAPSHOT_URL) as resp:
                    if resp.status != 200:
                        raise Exception(f"Download failed with status {resp.status}")
                    os.makedirs(os.path.dirname(HELIUM_SNAPSHOT_PATH) or '.', exist_ok=True)
                    async with aiofiles.open(HELIUM_SNAPSHOT_PATH, 'wb') as f:
                        async for chunk in resp.content.iter_chunked(8192):
                            await f.write(chunk)
            logger.info("Helium snapshot downloaded", path=HELIUM_SNAPSHOT_PATH)

        asyncio.run(download())

        if PROMETHEUS_AVAILABLE:
            task_metrics['helium_success'].inc()
            task_metrics['task_duration'].labels(task_name='update_helium_snapshot').observe(time.time() - start_time)

        return {"status": "success", "path": HELIUM_SNAPSHOT_PATH}

    except Exception as e:
        logger.error("Helium snapshot update failed", error=str(e), exc_info=True)
        raise self.retry(exc=e)


# ============================================================================
# Celery Beat schedule (can be overridden by environment)
# ============================================================================
def get_beat_schedule():
    """Build beat schedule from environment variables."""
    schedule = {}
    carbon_interval = int(os.getenv('CARBON_UPDATE_INTERVAL', 21600))
    material_interval = int(os.getenv('MATERIAL_UPDATE_INTERVAL', 604800))
    helium_interval = int(os.getenv('HELIUM_UPDATE_INTERVAL', 86400))

    if carbon_interval > 0:
        schedule['update-carbon'] = {
            'task': 'src.enhancements.tasks.periodic_updater.update_carbon_intensity',
            'schedule': carbon_interval,
        }
    if material_interval > 0:
        schedule['update-material'] = {
            'task': 'src.enhancements.tasks.periodic_updater.update_material_catalog',
            'schedule': material_interval,
        }
    if helium_interval > 0:
        schedule['update-helium'] = {
            'task': 'src.enhancements.tasks.periodic_updater.update_helium_snapshot',
            'schedule': helium_interval,
        }
    return schedule

app.conf.beat_schedule = get_beat_schedule()


# ============================================================================
# Example usage (if run directly)
# ============================================================================
if __name__ == "__main__":
    # For testing tasks locally (not for production)
    # You can run `celery -A src.enhancements.tasks.periodic_updater.app worker --loglevel=info`
    print("This file is meant to be used with Celery worker and beat.")
    print("To start worker: celery -A src.enhancements.tasks.periodic_updater.app worker --loglevel=info")
    print("To start beat:   celery -A src.enhancements.tasks.periodic_updater.app beat --loglevel=info")
