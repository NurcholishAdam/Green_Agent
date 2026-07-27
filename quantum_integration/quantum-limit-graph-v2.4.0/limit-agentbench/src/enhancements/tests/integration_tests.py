# src/enhancements/tests/integration_tests.py
"""
Enhanced Integration Tests for Green Agent Components
======================================================
Comprehensive test suite covering core modules with mocking,
edge cases, error handling, and configuration validation.

Test coverage includes:
- CacheManager
- CarbonIntensityFetcher
- HeliumCollector
- MaterialFootprintUpdater
- BioParameterCatalog
- HeliumSyntheticGenerator
- NodeDescriptor and WorkloadDescriptor helpers
- SustainabilityCostFunction
- Periodic Celery tasks (with mocked dependencies)
- Persistence and error handling
"""

import pytest
import asyncio
import json
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch, mock_open

# ---------- Modules under test ----------
from ..cache.cache_manager import CacheManager
from ..data_integration.carbon_intensity import CarbonIntensityFetcher
from ..data_integration.helium_collector import HeliumCollector
from ..data_integration.material_footprint import MaterialFootprintUpdater
from ..data_integration.bio_parameter_catalog import BioParameterCatalog
from ..data_integration.helium_synthetic_generator import HeliumSyntheticGenerator
from ..schemas.node_descriptor import NodeDescriptor, NodeType, CoolingType, MaintenanceStatus
from ..schemas.workload_descriptor import WorkloadDescriptor, TaskType, Urgency, Priority, BioMode
from ..cost_function.sustainability_cost import SustainabilityCostFunction
from ..tasks.periodic_updater import app as celery_app, update_carbon_intensity, update_material_catalog, update_helium_snapshot

# ============================================================================
# Fixtures for test isolation
# ============================================================================
@pytest.fixture
def temp_cache_dir():
    """Provide a temporary directory for file-based caching."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)

@pytest.fixture
def cache_manager(temp_cache_dir):
    """Create a CacheManager with a temporary file backend."""
    return CacheManager(redis_url="memory://")  # fallback to in-memory

@pytest.fixture
def mock_aiohttp_session():
    """Mock aiohttp.ClientSession for API calls."""
    with patch('aiohttp.ClientSession') as mock_session:
        mock_session.return_value.__aenter__.return_value.get.return_value.__aenter__.return_value.status = 200
        mock_session.return_value.__aenter__.return_value.get.return_value.__aenter__.return_value.json = AsyncMock(
            return_value={"intensity": 0.42}
        )
        yield mock_session

# ============================================================================
# 1. CacheManager Tests
# ============================================================================
@pytest.mark.asyncio
async def test_cache_manager_basic(cache_manager):
    """Test basic set/get operations."""
    await cache_manager.set("key1", "value1", ttl=10)
    val = await cache_manager.get("key1")
    assert val == "value1"

    # Test expiration
    await cache_manager.set("key2", "value2", ttl=1)
    await asyncio.sleep(1.5)
    val = await cache_manager.get("key2")
    assert val is None

@pytest.mark.asyncio
async def test_cache_manager_fallback():
    """Test that CacheManager falls back to memory when Redis is unavailable."""
    with patch('redis.asyncio.Redis') as mock_redis:
        mock_redis.from_url.side_effect = Exception("Redis down")
        cache = CacheManager()
        # Should fallback to memory
        await cache.set("key", "value")
        val = await cache.get("key")
        assert val == "value"

@pytest.mark.asyncio
async def test_cache_manager_delete(cache_manager):
    """Test delete operation."""
    await cache_manager.set("key", "value")
    assert await cache_manager.get("key") == "value"
    deleted = await cache_manager.delete("key")
    assert deleted is True
    assert await cache_manager.get("key") is None

# ============================================================================
# 2. CarbonIntensityFetcher Tests
# ============================================================================
@pytest.mark.asyncio
async def test_carbon_fetcher_basic(cache_manager, mock_aiohttp_session):
    """Test successful fetch with caching."""
    fetcher = CarbonIntensityFetcher(cache_manager)
    intensity = await fetcher.get_intensity("us-east")
    assert 0.2 < intensity < 0.6
    # Ensure caching works
    with patch.object(fetcher, '_fetch_climate_trace') as mock_fetch:
        await fetcher.get_intensity("us-east")
        mock_fetch.assert_not_called()

@pytest.mark.asyncio
async def test_carbon_fetcher_fallback(cache_manager):
    """Test fallback to region average when all providers fail."""
    with patch.object(CarbonIntensityFetcher, '_fetch_climate_trace', AsyncMock(return_value=None)):
        with patch.object(CarbonIntensityFetcher, '_fetch_os_climate', AsyncMock(return_value=None)):
            with patch.object(CarbonIntensityFetcher, '_fetch_electricity_maps', AsyncMock(return_value=None)):
                fetcher = CarbonIntensityFetcher(cache_manager)
                intensity = await fetcher.get_intensity("unknown-region")
                # Should fallback to global average (0.40)
                assert intensity == pytest.approx(0.40, abs=0.01)

@pytest.mark.asyncio
async def test_carbon_fetcher_batch(cache_manager):
    """Test batch fetching."""
    fetcher = CarbonIntensityFetcher(cache_manager)
    # Mock individual get_intensity to return fixed values
    with patch.object(fetcher, 'get_intensity', AsyncMock(side_effect=[0.42, 0.35, 0.28])):
        results = await fetcher.get_intensity_batch(["us-east", "us-west", "eu-west"])
        assert results == {"us-east": 0.42, "us-west": 0.35, "eu-west": 0.28}

# ============================================================================
# 3. HeliumCollector Tests
# ============================================================================
@pytest.mark.asyncio
async def test_helium_collector_basic(cache_manager):
    """Test connectivity score computation."""
    collector = HeliumCollector(cache_manager)
    # Mock _fetch_hotspot_data to return sample data
    with patch.object(collector, '_fetch_hotspot_data', AsyncMock(return_value=[
        {'rssi': -70, 'snr': 12},
        {'rssi': -65, 'snr': 15}
    ])):
        score = await collector.get_connectivity_score("hotspot_001")
        assert 0 <= score <= 1
        # Should cache
        with patch.object(collector, '_fetch_hotspot_data') as mock_fetch:
            await collector.get_connectivity_score("hotspot_001")
            mock_fetch.assert_not_called()

@pytest.mark.asyncio
async def test_helium_collector_empty_data(cache_manager):
    """Test default score when no data."""
    collector = HeliumCollector(cache_manager)
    with patch.object(collector, '_fetch_hotspot_data', AsyncMock(return_value=[])):
        score = await collector.get_connectivity_score("invalid_hotspot")
        assert score == 0.5

@pytest.mark.asyncio
async def test_helium_collector_batch(cache_manager):
    """Test batch fetch with concurrency control."""
    collector = HeliumCollector(cache_manager)
    with patch.object(collector, 'get_connectivity_score', AsyncMock(side_effect=[0.9, 0.7, 0.5])):
        scores = await collector.fetch_batch_scores(["h1", "h2", "h3"], max_concurrency=2)
        assert scores == {"h1": 0.9, "h2": 0.7, "h3": 0.5}

# ============================================================================
# 4. MaterialFootprintUpdater Tests
# ============================================================================
@pytest.mark.asyncio
async def test_material_updater_catalog():
    """Test catalog update and retrieval."""
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        updater = MaterialFootprintUpdater(db_path=Path(tmp.name))
        # Mock API call to return data
        with patch.object(updater, '_update_from_source', AsyncMock()):
            await updater.update_catalog()
        # Since we mocked, catalog should be seeded with mock data
        fp = updater.get_footprint("gpu-a100")
        assert fp is not None
        assert fp['material_index'] == 1.2

@pytest.mark.asyncio
async def test_material_updater_api_failure():
    """Test that API failure does not break catalog and falls back to mock."""
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        updater = MaterialFootprintUpdater(db_path=Path(tmp.name))
        # Force failure in _update_from_source
        with patch.object(updater, '_update_from_source', AsyncMock(side_effect=Exception("API down"))):
            await updater.update_catalog()
        # Should have seeded mock data
        fp = updater.get_footprint("edge-device")
        assert fp is not None
        assert fp['material_index'] == 0.6

# ============================================================================
# 5. BioParameterCatalog Tests
# ============================================================================
def test_bio_catalog_basic():
    """Test catalog initialization and get_parameters."""
    with tempfile.TemporaryDirectory() as tmpdir:
        catalog_path = Path(tmpdir) / "bio_params.json"
        catalog = BioParameterCatalog(catalog_path)
        params = catalog.get_parameters("high_efficiency")
        assert params.get('photosynthetic_efficiency') == 0.8

def test_bio_catalog_add_remove():
    """Test adding and removing organism types."""
    with tempfile.TemporaryDirectory() as tmpdir:
        catalog_path = Path(tmpdir) / "bio_params.json"
        catalog = BioParameterCatalog(catalog_path)
        success = catalog.add_organism_type("ultra_high", {
            "photosynthetic_efficiency": 0.9,
            "resilience_to_stress": 0.8,
            "carbon_fixation_rate": 0.7,
            "helium_affinity": 0.5
        })
        assert success
        assert "ultra_high" in catalog.list_organism_types()
        removed = catalog.remove_organism_type("ultra_high")
        assert removed
        assert "ultra_high" not in catalog.list_organism_types()

def test_bio_catalog_search():
    """Test search with filters."""
    with tempfile.TemporaryDirectory() as tmpdir:
        catalog_path = Path(tmpdir) / "bio_params.json"
        catalog = BioParameterCatalog(catalog_path)
        results = catalog.search(photosynthetic_efficiency__gte=0.7)
        assert "high_efficiency" in results
        assert "low_carbon" in results
        assert "high_robustness" not in results

# ============================================================================
# 6. HeliumSyntheticGenerator Tests
# ============================================================================
def test_helium_synthetic_generator_basic():
    """Test basic trace generation."""
    gen = HeliumSyntheticGenerator()
    df = gen.generate_trace(num_hotspots=5, duration_hours=1, events_per_hour=2)
    assert len(df) > 0
    assert 'rssi' in df.columns
    assert df['rssi'].min() >= -120
    assert df['rssi'].max() <= -30

def test_helium_synthetic_generator_validation():
    """Test statistical validation (if scipy available)."""
    gen = HeliumSyntheticGenerator()
    df = gen.generate_trace(num_hotspots=5, duration_hours=1, events_per_hour=20)
    try:
        results = gen.validate_trace(df)
        assert 'rssi_ks_test' in results
    except ImportError:
        pytest.skip("scipy not available")

# ============================================================================
# 7. NodeDescriptor and WorkloadDescriptor Helper Methods
# ============================================================================
def test_node_descriptor_helpers():
    """Test NodeDescriptor helper methods."""
    node = NodeDescriptor(
        id="test-node",
        type=NodeType.EDGE,
        region="us-east",
        region_carbon_intensity=0.42,
        energy_per_token=0.00005,
        helium_connectivity_score=0.9,
        uptime=0.99,
        maintenance_status=MaintenanceStatus.OPERATIONAL,
        efficiency_score=0.85
    )
    # compute_energy_cost
    energy = node.compute_energy_cost(512)
    assert energy == 0.00005 * 512
    # compute_carbon_cost
    carbon = node.compute_carbon_cost(energy)
    # Energy in J * 2.7778e-7 = kWh * 0.42 = kg CO₂
    expected_carbon = energy * 2.7778e-7 * 0.42
    assert carbon == pytest.approx(expected_carbon)
    # get_health_score
    health = node.get_health_score()
    assert health == 0.99 * 0.85  # uptime * efficiency
    # is_available
    assert node.is_available() is True
    # Test degraded
    node.maintenance_status = MaintenanceStatus.DEGRADED
    assert node.is_available() is True
    node.maintenance_status = MaintenanceStatus.OFFLINE
    assert node.is_available() is False

def test_workload_descriptor_helpers():
    """Test WorkloadDescriptor helper methods."""
    wl = WorkloadDescriptor(
        task_type=TaskType.INFERENCE,
        tokens=512,
        latency_target=200.0,
        urgency=Urgency.HIGH,
        priority=Priority.GREEN,
        bio_mode=BioMode.PHOTOSYNTHETIC
    )
    assert wl.is_critical() is False
    assert wl.is_high_priority() is True
    # compute_energy_cost (placeholder)
    energy = wl.compute_energy_cost(0.00005)
    assert energy == 0.00005 * 512

# ============================================================================
# 8. SustainabilityCostFunction Tests
# ============================================================================
@pytest.mark.asyncio
async def test_cost_function_basic(cache_manager):
    """Test cost function computation with real data."""
    carbon = CarbonIntensityFetcher(cache_manager)
    helium = HeliumCollector(cache_manager)
    # Use in-memory material catalog
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        material = MaterialFootprintUpdater(db_path=Path(tmp.name))
        # Seed mock data
        material._seed_mock_data()
        cost_func = SustainabilityCostFunction(carbon, material, helium)
        node = NodeDescriptor(
            id="test-node",
            type=NodeType.EDGE,
            region="us-east",
            region_carbon_intensity=0.42,
            energy_per_token=0.00005,
            helium_connectivity_score=0.9,
            material_footprint_id="gpu-a100"
        )
        workload = WorkloadDescriptor(
            task_type=TaskType.INFERENCE,
            tokens=512,
            latency_target=200.0,
            sector_emission_factor=0.03,
            bio_mode=BioMode.NONE,
            priority=Priority.BALANCED
        )
        cost = await cost_func.compute(node, workload)
        assert cost > 0

# ============================================================================
# 9. Periodic Celery Tasks Tests (with mocks)
# ============================================================================
def test_update_carbon_intensity_task():
    """Test the carbon update task with mocked dependencies."""
    # Patch the fetcher and cache creation inside the task
    with patch('src.enhancements.tasks.periodic_updater.CacheManager') as mock_cache_cls:
        with patch('src.enhancements.tasks.periodic_updater.CarbonIntensityFetcher') as mock_fetcher_cls:
            mock_fetcher = mock_fetcher_cls.return_value
            mock_fetcher.get_intensity = AsyncMock()
            # Mock async run to actually run the inner coroutine
            # We'll need to patch asyncio.run to execute the fetch_all coroutine
            # For simplicity, we just test that the task runs without error.
            # We'll use a mock task with a custom execute
            task = update_carbon_intensity
            # We can't easily test celery tasks directly, but we can test the wrapped function.
            # We'll create a dummy task object with a retry method.
            class DummyTask:
                retry = MagicMock()
            result = update_carbon_intensity.__wrapped__(DummyTask())
            # If we got here, it executed.
            assert result['status'] == 'success'

# ============================================================================
# 10. Error Handling and Edge Cases
# ============================================================================
@pytest.mark.asyncio
async def test_carbon_fetcher_timeout(cache_manager):
    """Test that timeout raises an exception (but we handle it)."""
    with patch.object(CarbonIntensityFetcher, '_fetch_climate_trace', AsyncMock(side_effect=asyncio.TimeoutError)):
        with patch.object(CarbonIntensityFetcher, '_fetch_os_climate', AsyncMock(return_value=None)):
            with patch.object(CarbonIntensityFetcher, '_fetch_electricity_maps', AsyncMock(return_value=None)):
                fetcher = CarbonIntensityFetcher(cache_manager)
                # Should fallback to region average
                intensity = await fetcher.get_intensity("us-east")
                assert intensity == pytest.approx(0.41, abs=0.01)

@pytest.mark.asyncio
async def test_helium_collector_api_error(cache_manager):
    """Test that API error results in default score."""
    collector = HeliumCollector(cache_manager)
    with patch.object(collector, '_fetch_hotspot_data', AsyncMock(side_effect=Exception("API error"))):
        score = await collector.get_connectivity_score("hotspot_001")
        assert score == 0.5

# ============================================================================
# 11. Configuration Validation Tests
# ============================================================================
def test_node_descriptor_validation():
    """Test that invalid fields raise validation errors."""
    with pytest.raises(ValueError, match="region_carbon_intensity must be non-negative"):
        NodeDescriptor(
            id="test",
            type=NodeType.EDGE,
            region="us-east",
            region_carbon_intensity=-0.1,
            energy_per_token=0.00005
        )
    with pytest.raises(ValueError, match="energy_per_token must be positive"):
        NodeDescriptor(
            id="test",
            type=NodeType.EDGE,
            region="us-east",
            region_carbon_intensity=0.42,
            energy_per_token=0.0
        )

def test_workload_descriptor_validation():
    """Test that invalid fields raise validation errors."""
    with pytest.raises(ValueError, match="latency_target must be positive"):
        WorkloadDescriptor(
            task_type=TaskType.INFERENCE,
            tokens=512,
            latency_target=-1.0
        )

# ============================================================================
# 12. Persistence Tests
# ============================================================================
def test_bio_catalog_persistence():
    """Test that catalog saves and loads correctly."""
    with tempfile.TemporaryDirectory() as tmpdir:
        catalog_path = Path(tmpdir) / "bio_params.json"
        catalog = BioParameterCatalog(catalog_path)
        # Add a new organism
        catalog.add_organism_type("test_type", {
            "photosynthetic_efficiency": 0.75,
            "resilience_to_stress": 0.65,
            "carbon_fixation_rate": 0.55,
            "helium_affinity": 0.45
        })
        catalog.save()
        # Reload from disk
        catalog2 = BioParameterCatalog(catalog_path)
        params = catalog2.get_parameters("test_type")
        assert params.get('photosynthetic_efficiency') == 0.75

def test_material_footprint_persistence():
    """Test that material catalog persists in SQLite."""
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        updater = MaterialFootprintUpdater(db_path=Path(tmp.name))
        # Seed some data
        updater._seed_mock_data()
        # Reopen with same DB
        updater2 = MaterialFootprintUpdater(db_path=Path(tmp.name))
        fp = updater2.get_footprint("gpu-a100")
        assert fp is not None

# ============================================================================
# Run with: pytest -v --asyncio-mode=auto
# ============================================================================
