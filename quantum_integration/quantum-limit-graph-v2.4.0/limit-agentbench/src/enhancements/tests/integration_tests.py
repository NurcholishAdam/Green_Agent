import pytest
import asyncio
from ..cache.cache_manager import CacheManager
from ..data_integration.carbon_intensity import CarbonIntensityFetcher
from ..data_integration.helium_collector import HeliumCollector
from ..schemas.node_descriptor import NodeDescriptor
from ..schemas.workload_descriptor import WorkloadDescriptor
from ..cost_function.sustainability_cost import SustainabilityCostFunction

@pytest.mark.asyncio
async def test_carbon_fetcher():
    cache = CacheManager()
    fetcher = CarbonIntensityFetcher(cache)
    intensity = await fetcher.get_intensity("us-east")
    assert 0.2 < intensity < 0.6

@pytest.mark.asyncio
async def test_helium_collector():
    cache = CacheManager()
    collector = HeliumCollector(cache)
    score = await collector.get_connectivity_score("hotspot_0001")
    assert 0 <= score <= 1

@pytest.mark.asyncio
async def test_cost_function():
    cache = CacheManager()
    carbon = CarbonIntensityFetcher(cache)
    helium = HeliumCollector(cache)
    # Need a material updater stub for testing
    from ..data_integration.material_footprint import MaterialFootprintUpdater
    material = MaterialFootprintUpdater(db_path=":memory:")
    cost_func = SustainabilityCostFunction(carbon, material, helium)
    node = NodeDescriptor(
        id="test-node",
        type="edge",
        region="us-east",
        region_carbon_intensity=0.42,
        energy_per_token=0.00005,
        helium_connectivity_score=0.9,
        material_footprint_id="gpu-a100"
    )
    workload = WorkloadDescriptor(
        task_type="inference",
        tokens=512,
        latency_target=200.0,
        sector_emission_factor=0.03,
        bio_mode="none",
        priority="balanced"
    )
    cost = await cost_func.compute(node, workload)
    assert cost > 0
