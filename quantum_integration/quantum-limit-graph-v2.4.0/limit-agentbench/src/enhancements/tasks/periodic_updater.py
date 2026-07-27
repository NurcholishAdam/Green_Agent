from celery import Celery
from ..cache.cache_manager import CacheManager
from ..data_integration.carbon_intensity import CarbonIntensityFetcher
from ..data_integration.material_footprint import MaterialFootprintUpdater
from ..data_integration.helium_collector import HeliumCollector

app = Celery('green_agent', broker='redis://localhost:6379/0')
app.conf.update(
    task_serializer='json',
    result_serializer='json',
    accept_content=['json'],
    timezone='UTC',
    enable_utc=True,
)

@app.task
def update_carbon_intensity():
    """Refresh carbon intensity for all key regions."""
    cache = CacheManager()
    fetcher = CarbonIntensityFetcher(cache)
    regions = ['us-east', 'us-west', 'eu-west', 'eu-north', 'asia-east', 'asia-southeast']
    for region in regions:
        # Will fetch and cache
        fetcher.get_intensity(region)
    return "Carbon intensity updated"

@app.task
def update_material_catalog():
    """Refresh material footprint catalog."""
    updater = MaterialFootprintUpdater()
    import asyncio
    asyncio.run(updater.update_catalog())
    return "Material catalog updated"

@app.task
def update_helium_snapshot():
    """Download latest Helium snapshot (stub)."""
    # In production: download from a public bucket
    return "Helium snapshot updated"

# Beat schedule (can also be set in celery beat config)
app.conf.beat_schedule = {
    'update-carbon-every-6h': {
        'task': 'src.enhancements.tasks.periodic_updater.update_carbon_intensity',
        'schedule': 21600.0,  # 6 hours
    },
    'update-material-weekly': {
        'task': 'src.enhancements.tasks.periodic_updater.update_material_catalog',
        'schedule': 604800.0,  # 1 week
    },
    'update-helium-daily': {
        'task': 'src.enhancements.tasks.periodic_updater.update_helium_snapshot',
        'schedule': 86400.0,  # daily
    },
}
