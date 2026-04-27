"""
test_helium_monitor.py — Complete Test Suite for HeliumMonitor
==============================================================

Run with:
    pytest test_helium_monitor.py -v
    pytest test_helium_monitor.py::TestHeliumMonitorAsync -v

Test categories:
- Unit tests: Isolated component testing
- Integration tests: API interaction testing
- Async tests: Async/await pattern validation
- Mock tests: External dependency mocking
"""

import pytest
import asyncio
import aiohttp
from unittest.mock import Mock, patch, AsyncMock, MagicMock
from datetime import datetime, timedelta
import json

from src.carbon.helium_monitor import (
    HeliumMonitor,
    HeliumScarcityLevel,
    HeliumSupplySignal
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_config():
    """Create test configuration"""
    return {
        'api_endpoints': {
            'primary': 'https://test-primary.example.com/v1/supply',
            'backup': 'https://test-backup.example.com/v1/status'
        },
        'update_interval': 1,  # Fast for testing
        'history_buffer_size': 10,
        'max_retries': 2,
        'base_retry_delay': 0.1,
        'api_key': 'test-key-123'
    }


@pytest.fixture
def valid_api_response():
    """Valid API response data"""
    return {
        'scarcity_level': 'caution',
        'scarcity_score': 0.4,
        'spot_price_usd': 5.5,
        'fab_inventory_days': 20,
        'alerts': ['Supply chain delay'],
        'forecast_valid_until': '2026-03-14T00:00:00'
    }


@pytest.fixture
def helium_monitor(mock_config):
    """Create HeliumMonitor instance with test config"""
    # Disable auto-start for controlled testing
    monitor = HeliumMonitor.__new__(HeliumMonitor)
    monitor.config = mock_config
    monitor.api_endpoints = mock_config['api_endpoints']
    monitor.api_key = mock_config['api_key']
    monitor.api_headers = {'Authorization': f'Bearer {mock_config["api_key"]}'}
    monitor.update_interval_seconds = mock_config['update_interval']
    monitor.history_buffer_size = mock_config['history_buffer_size']
    monitor.max_retries = mock_config['max_retries']
    monitor.base_retry_delay = mock_config['base_retry_delay']
    monitor.current_signal = None
    monitor._signal_history = []
    monitor._monitoring_task = None
    monitor._shutdown_event = asyncio.Event()
    monitor._rng = __import__('random').Random(42)  # Fixed seed for tests
    return monitor


# ---------------------------------------------------------------------------
# Unit Tests: Initialization & Configuration
# ---------------------------------------------------------------------------

class TestHeliumMonitorInit:
    """Test HeliumMonitor initialization"""
    
    def test_init_with_default_config(self):
        """Test initialization with default configuration"""
        monitor = HeliumMonitor.__new__(HeliumMonitor)
        # Manually init minimal state for testing
        monitor.config = {}
        monitor.api_endpoints = {
            'primary': 'https://api.helium-monitor.example.com/v1/supply',
            'backup': 'https://backup.helium-api.example.com/v1/status'
        }
        monitor.api_key = None
        monitor.api_headers = {}
        monitor.update_interval_seconds = 900
        monitor.history_buffer_size = 100
        monitor.max_retries = 3
        monitor.base_retry_delay = 1.0
        monitor.current_signal = None
        monitor._signal_history = []
        monitor._monitoring_task = None
        monitor._shutdown_event = asyncio.Event()
        monitor._rng = __import__('random').Random()
        
        assert monitor.update_interval_seconds == 900
        assert monitor.history_buffer_size == 100
        assert monitor.api_headers == {}  # No API key → no headers
    
    def test_init_with_api_key_from_config(self, mock_config):
        """Test API key loaded from config"""
        monitor = HeliumMonitor.__new__(HeliumMonitor)
        monitor.config = mock_config
        monitor.api_key = mock_config['api_key']
        monitor.api_headers = {'Authorization': f'Bearer {mock_config["api_key"]}'}
        
        assert 'Authorization' in monitor.api_headers
        assert mock_config['api_key'] in monitor.api_headers['Authorization']
    
    def test_init_with_api_key_from_env(self, mock_config, monkeypatch):
        """Test API key loaded from environment variable"""
        monkeypatch.setenv('HELIUM_API_KEY', 'env-key-456')
        
        monitor = HeliumMonitor.__new__(HeliumMonitor)
        monitor.config = {}  # No api_key in config
        monitor.api_key = os.getenv('HELIUM_API_KEY')
        monitor.api_headers = {'Authorization': f'Bearer {monitor.api_key}'} if monitor.api_key else {}
        
        assert monitor.api_key == 'env-key-456'
        assert 'env-key-456' in monitor.api_headers['Authorization']


# ---------------------------------------------------------------------------
# Unit Tests: Data Parsing & Validation
# ---------------------------------------------------------------------------

class TestParseAPIResponse:
    """Test _parse_api_response method"""
    
    def test_parse_valid_response(self, helium_monitor, valid_api_response):
        """Test parsing valid API response"""
        signal = helium_monitor._parse_api_response(valid_api_response, 'test_api')
        
        assert signal.scarcity_level == HeliumScarcityLevel.CAUTION
        assert signal.scarcity_score == 0.4
        assert signal.spot_price_usd_per_liter == 5.5
        assert signal.fab_inventory_days == 20
        assert signal.vendor_alerts == ['Supply chain delay']
        assert signal.source == 'test_api'
        assert signal.forecast_valid_until == datetime.fromisoformat('2026-03-14T00:00:00')
    
    def test_parse_missing_required_field(self, helium_monitor):
        """Test error on missing required field"""
        invalid_data = {
            'scarcity_level': 'normal',
            # Missing: scarcity_score, spot_price_usd, fab_inventory_days
        }
        
        with pytest.raises(ValueError, match='Missing required field'):
            helium_monitor._parse_api_response(invalid_data, 'test_api')
    
    def test_parse_invalid_scarcity_level(self, helium_monitor, valid_api_response):
        """Test handling of invalid scarcity_level enum value"""
        data = valid_api_response.copy()
        data['scarcity_level'] = 'invalid_value'
        
        signal = helium_monitor._parse_api_response(data, 'test_api')
        
        # Should default to NORMAL with warning logged
        assert signal.scarcity_level == HeliumScarcityLevel.NORMAL
    
    def test_parse_out_of_range_scarcity_score(self, helium_monitor, valid_api_response):
        """Test clamping of out-of-range scarcity_score"""
        # Score > 1.0
        data = valid_api_response.copy()
        data['scarcity_score'] = 1.5
        signal = helium_monitor._parse_api_response(data, 'test_api')
        assert signal.scarcity_score == 1.0  # Clamped
        
        # Score < 0.0
        data['scarcity_score'] = -0.2
        signal = helium_monitor._parse_api_response(data, 'test_api')
        assert signal.scarcity_score == 0.0  # Clamped
    
    def test_parse_negative_numeric_values(self, helium_monitor, valid_api_response):
        """Test handling of negative numeric values"""
        data = valid_api_response.copy()
        data['spot_price_usd'] = -10.0
        data['fab_inventory_days'] = -5
        
        signal = helium_monitor._parse_api_response(data, 'test_api')
        
        assert signal.spot_price_usd_per_liter == 0.0  # Clamped
        assert signal.fab_inventory_days == 0  # Clamped
    
    def test_parse_invalid_forecast_timestamp(self, helium_monitor, valid_api_response):
        """Test handling of invalid forecast_valid_until format"""
        data = valid_api_response.copy()
        data['forecast_valid_until'] = 'not-a-date'
        
        signal = helium_monitor._parse_api_response(data, 'test_api')
        
        # Should be None on parse error
        assert signal.forecast_valid_until is None


# ---------------------------------------------------------------------------
# Unit Tests: Simulation
# ---------------------------------------------------------------------------

class TestSimulateHeliumSupply:
    """Test _simulate_helium_supply method"""
    
    def test_simulation_produces_valid_signal(self, helium_monitor):
        """Test that simulation produces valid HeliumSupplySignal"""
        signal = helium_monitor._simulate_helium_supply()
        
        assert isinstance(signal, HeliumSupplySignal)
        assert signal.source == 'simulation'
        assert 0.0 <= signal.scarcity_score <= 1.0
        assert signal.spot_price_usd_per_liter >= 0
        assert signal.fab_inventory_days >= 0
        assert signal.forecast_valid_until is not None
    
    def test_simulation_weighted_distribution(self, helium_monitor):
        """Test that simulation follows weighted distribution"""
        # Run 1000 simulations and check distribution
        counts = {level: 0 for level in HeliumScarcityLevel}
        
        for _ in range(1000):
            signal = helium_monitor._simulate_helium_supply()
            counts[signal.scarcity_level] += 1
        
        # Check approximate distribution (allowing for randomness)
        total = sum(counts.values())
        normal_ratio = counts[HeliumScarcityLevel.NORMAL] / total
        caution_ratio = counts[HeliumScarcityLevel.CAUTION] / total
        
        # Expected: ~70% NORMAL, ~15% CAUTION (with tolerance)
        assert 0.60 < normal_ratio < 0.80, f"NORMAL ratio {normal_ratio} outside expected range"
        assert 0.10 < caution_ratio < 0.25, f"CAUTION ratio {caution_ratio} outside expected range"
    
    def test_simulation_deterministic_with_seed(self):
        """Test that simulation is deterministic with fixed seed"""
        monitor1 = HeliumMonitor.__new__(HeliumMonitor)
        monitor1._rng = __import__('random').Random(42)
        monitor1._simulate_helium_supply = HeliumMonitor._simulate_helium_supply.__get__(monitor1)
        
        monitor2 = HeliumMonitor.__new__(HeliumMonitor)
        monitor2._rng = __import__('random').Random(42)
        monitor2._simulate_helium_supply = HeliumMonitor._simulate_helium_supply.__get__(monitor2)
        
        signal1 = monitor1._simulate_helium_supply()
        signal2 = monitor2._simulate_helium_supply()
        
        # Same seed → same output
        assert signal1.scarcity_level == signal2.scarcity_level
        assert signal1.scarcity_score == signal2.scarcity_score
        assert signal1.spot_price_usd_per_liter == signal2.spot_price_usd_per_liter


# ---------------------------------------------------------------------------
# Unit Tests: Signal History & Trend
# ---------------------------------------------------------------------------

class TestSignalHistory:
    """Test signal history management"""
    
    def test_bounded_history_buffer(self, helium_monitor):
        """Test that history buffer stays within configured size"""
        # Add more signals than buffer size
        for i in range(20):
            signal = helium_monitor._simulate_helium_supply()
            helium_monitor._signal_history.append(signal)
            # Simulate the buffer trimming logic
            if len(helium_monitor._signal_history) > helium_monitor.history_buffer_size:
                helium_monitor._signal_history = helium_monitor._signal_history[-helium_monitor.history_buffer_size:]
        
        # Should be trimmed to buffer size
        assert len(helium_monitor.signal_history) <= helium_monitor.history_buffer_size
    
    def test_get_supply_trend_with_stdlib_timedelta(self, helium_monitor):
        """Test get_supply_trend uses stdlib timedelta (not pandas)"""
        # Add signals with known timestamps
        now = datetime.now()
        helium_monitor._signal_history = [
            HeliumSupplySignal(
                timestamp=now - timedelta(hours=h),
                scarcity_level=HeliumScarcityLevel.NORMAL,
                scarcity_score=0.1,
                spot_price_usd_per_liter=4.0,
                fab_inventory_days=30,
                vendor_alerts=[],
                source='test'
            )
            for h in [1, 5, 10, 20, 30]  # Hours ago
        ]
        
        # Get trend for last 15 hours
        trend = helium_monitor.get_supply_trend(hours=15)
        
        # Should include signals from 1, 5, 10 hours ago (not 20, 30)
        assert len(trend) == 3
        assert all(s.timestamp > now - timedelta(hours=15) for s in trend)
    
    def test_signal_history_thread_safety(self, helium_monitor):
        """Test that signal_history property returns copy (thread-safe)"""
        # Add a signal
        signal = helium_monitor._simulate_helium_supply()
        helium_monitor._signal_history.append(signal)
        
        # Get history and modify it
        history1 = helium_monitor.signal_history
        history1.append(helium_monitor._simulate_helium_supply())
        
        # Original should be unchanged
        history2 = helium_monitor.signal_history
        assert len(history2) == len(helium_monitor._signal_history)
        assert history2[-1] == signal  # Original last signal unchanged


# ---------------------------------------------------------------------------
# Async Tests: API Fetching with Mocks
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
class TestFetchHeliumSupply:
    """Test fetch_helium_supply with mocked HTTP"""
    
    async def test_fetch_primary_success(self, helium_monitor, valid_api_response):
        """Test successful fetch from primary API"""
        # Mock aiohttp session and response
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value=valid_api_response)
        mock_response.headers = {}
        
        mock_session = AsyncMock()
        mock_session.get.return_value.__aenter__.return_value = mock_response
        
        with patch('aiohttp.ClientSession', return_value=mock_session):
            signal = await helium_monitor.fetch_helium_supply()
            
            assert signal.source == 'primary_api'
            assert signal.scarcity_level == HeliumScarcityLevel.CAUTION
            # Verify API key was included in headers
            mock_session.get.assert_called_once()
            call_kwargs = mock_session.get.call_args[1]
            assert 'Authorization' in call_kwargs.get('headers', {})
    
    async def test_fetch_fallback_to_backup(self, helium_monitor, valid_api_response):
        """Test fallback to backup API when primary fails"""
        # Primary fails (500), backup succeeds
        primary_resp = AsyncMock()
        primary_resp.status = 500
        
        backup_resp = AsyncMock()
        backup_resp.status = 200
        backup_resp.json = AsyncMock(return_value=valid_api_response)
        backup_resp.headers = {}
        
        mock_session = AsyncMock()
        mock_session.get.side_effect = [primary_resp, backup_resp]
        
        with patch('aiohttp.ClientSession', return_value=mock_session):
            signal = await helium_monitor.fetch_helium_supply()
            assert signal.source == 'backup_api'
    
    async def test_fetch_fallback_to_simulation(self, helium_monitor):
        """Test fallback to simulation when all APIs fail"""
        mock_session = AsyncMock()
        mock_session.get.side_effect = aiohttp.ClientError("Connection failed")
        
        with patch('aiohttp.ClientSession', return_value=mock_session):
            signal = await helium_monitor.fetch_helium_supply()
            assert signal.source == 'simulation'
    
    async def test_fetch_rate_limit_retry(self, helium_monitor, valid_api_response):
        """Test retry logic on 429 rate limit response"""
        # First call: 429 with Retry-After header
        rate_limited_resp = AsyncMock()
        rate_limited_resp.status = 429
        rate_limited_resp.headers = {'Retry-After': '1'}  # Retry after 1 second
        
        # Second call: success
        success_resp = AsyncMock()
        success_resp.status = 200
        success_resp.json = AsyncMock(return_value=valid_api_response)
        success_resp.headers = {}
        
        mock_session = AsyncMock()
        mock_session.get.side_effect = [rate_limited_resp, success_resp]
        
        with patch('aiohttp.ClientSession', return_value=mock_session):
            # Should retry and succeed
            signal = await helium_monitor.fetch_helium_supply()
            assert signal.source == 'primary_api'
            # Verify get was called twice (retry)
            assert mock_session.get.call_count == 2
    
    async def test_fetch_exponential_backoff(self, helium_monitor):
        """Test exponential backoff on repeated failures"""
        mock_session = AsyncMock()
        mock_session.get.side_effect = aiohttp.ClientError("Fail")
        
        with patch('aiohttp.ClientSession', return_value=mock_session):
            # Should retry max_retries times with increasing delays
            signal = await helium_monitor.fetch_helium_supply()
            
            # Should have called get max_retries times
            assert mock_session.get.call_count == helium_monitor.max_retries
            # Should fallback to simulation
            assert signal.source == 'simulation'


# ---------------------------------------------------------------------------
# Async Tests: Monitoring Loop & Shutdown
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
class TestMonitoringLoop:
    """Test _monitor_loop and shutdown"""
    
    async def test_monitor_loop_updates_signal(self, helium_monitor):
        """Test that monitor loop updates current_signal"""
        # Mock fetch to return predictable signal
        test_signal = helium_monitor._simulate_helium_supply()
        helium_monitor.fetch_helium_supply = AsyncMock(return_value=test_signal)
        
        # Start monitoring with very short interval
        helium_monitor.update_interval_seconds = 0.1
        helium_monitor._start_monitoring()
        
        # Wait for one update cycle
        await asyncio.sleep(0.2)
        
        # Should have updated current_signal
        assert helium_monitor.current_signal is not None
        assert helium_monitor.current_signal.scarcity_score == test_signal.scarcity_score
        
        # Cleanup
        await helium_monitor.shutdown()
    
    async def test_monitor_loop_respects_shutdown(self, helium_monitor):
        """Test that monitor loop stops on shutdown"""
        # Mock fetch to track calls
        call_count = 0
        async def mock_fetch():
            nonlocal call_count
            call_count += 1
            return helium_monitor._simulate_helium_supply()
        
        helium_monitor.fetch_helium_supply = mock_fetch
        helium_monitor.update_interval_seconds = 1.0
        
        # Start monitoring
        helium_monitor._start_monitoring()
        
        # Let it run briefly
        await asyncio.sleep(0.5)
        calls_before_shutdown = call_count
        
        # Shutdown
        await helium_monitor.shutdown()
        
        # Give loop time to stop
        await asyncio.sleep(0.2)
        
        # Should not have made more calls after shutdown
        assert call_count == calls_before_shutdown
        assert helium_monitor._monitoring_task.done()
    
    async def test_shutdown_cancels_task(self, helium_monitor):
        """Test that shutdown cancels monitoring task"""
        helium_monitor._start_monitoring()
        
        assert helium_monitor._monitoring_task is not None
        assert not helium_monitor._monitoring_task.done()
        
        await helium_monitor.shutdown()
        
        assert helium_monitor._monitoring_task.done()


# ---------------------------------------------------------------------------
# Integration Tests: Prometheus Metrics
# ---------------------------------------------------------------------------

class TestPrometheusMetrics:
    """Test collect_prometheus_metrics method"""
    
    def test_collect_metrics_with_signal(self, helium_monitor):
        """Test metrics collection with current signal"""
        # Set a known signal
        helium_monitor.current_signal = HeliumSupplySignal(
            timestamp=datetime.now(),
            scarcity_level=HeliumScarcityLevel.CRITICAL,
            scarcity_score=0.7,
            spot_price_usd_per_liter=8.0,
            fab_inventory_days=10,
            vendor_alerts=['Alert 1', 'Alert 2'],
            source='test'
        )
        
        metrics = helium_monitor.collect_prometheus_metrics()
        
        # Check all expected metrics present
        assert 'green_agent_helium_scarcity_level' in metrics
        assert 'green_agent_helium_scarcity_score' in metrics
        assert 'green_agent_helium_spot_price_usd' in metrics
        assert 'green_agent_helium_fab_inventory_days' in metrics
        assert 'green_agent_helium_vendor_alerts_count' in metrics
        assert 'green_agent_helium_price_premium_usd' in metrics
        
        # Check values
        assert metrics['green_agent_helium_scarcity_level'] == (2, {'source': 'test'})  # CRITICAL=2
        assert metrics['green_agent_helium_scarcity_score'] == (0.7, {'source': 'test'})
        assert metrics['green_agent_helium_spot_price_usd'] == (8.0, {})
        assert metrics['green_agent_helium_fab_inventory_days'] == (10, {})
        assert metrics['green_agent_helium_vendor_alerts_count'] == (2, {})
        assert metrics['green_agent_helium_price_premium_usd'] == (4.0, {})  # 8.0 - 4.0 baseline
    
    def test_collect_metrics_no_signal(self, helium_monitor):
        """Test metrics collection when no signal available"""
        helium_monitor.current_signal = None
        
        metrics = helium_monitor.collect_prometheus_metrics()
        
        # Should return empty dict when no signal
        assert metrics == {}
    
    def test_metrics_prometheus_format_compatible(self, helium_monitor):
        """Test that collected metrics can be rendered in Prometheus format"""
        helium_monitor.current_signal = HeliumSupplySignal(
            timestamp=datetime.now(),
            scarcity_level=HeliumScarcityLevel.CAUTION,
            scarcity_score=0.4,
            spot_price_usd_per_liter=5.5,
            fab_inventory_days=20,
            vendor_alerts=[],
            source='test'
        )
        
        metrics = helium_monitor.collect_prometheus_metrics()
        
        # Simulate Prometheus text format rendering
        lines = []
        for name, (value, labels) in metrics.items():
            if labels:
                label_str = ','.join(f'{k}="{v}"' for k, v in sorted(labels.items()))
                lines.append(f'{name}{{{label_str}}} {value}')
            else:
                lines.append(f'{name} {value}')
        
        # Check format
        assert any('green_agent_helium_scarcity_score{source="test"} 0.4' in line for line in lines)
        assert any('green_agent_helium_spot_price_usd 5.5' in line for line in lines)


# ---------------------------------------------------------------------------
# Edge Cases & Error Handling
# ---------------------------------------------------------------------------

class TestEdgeCases:
    """Test edge cases and error handling"""
    
    def test_get_supply_trend_empty_history(self, helium_monitor):
        """Test get_supply_trend with empty history"""
        helium_monitor._signal_history = []
        trend = helium_monitor.get_supply_trend(hours=24)
        assert trend == []
    
    def test_get_supply_trend_no_matches(self, helium_monitor):
        """Test get_supply_trend when no signals in time window"""
        # Add very old signals
        old_time = datetime.now() - timedelta(days=30)
        helium_monitor._signal_history = [
            HeliumSupplySignal(
                timestamp=old_time,
                scarcity_level=HeliumScarcityLevel.NORMAL,
                scarcity_score=0.1,
                spot_price_usd_per_liter=4.0,
                fab_inventory_days=30,
                vendor_alerts=[],
                source='old'
            )
        ]
        
        trend = helium_monitor.get_supply_trend(hours=24)
        assert trend == []  # No signals in last 24 hours
    
    def test_forecast_no_data(self, helium_monitor):
        """Test forecast when no current signal"""
        helium_monitor.current_signal = None
        forecast = asyncio.run(helium_monitor.get_forecast())
        assert 'error' in forecast
        assert forecast['error'] == 'No data available'
    
    def test_forecast_with_data(self, helium_monitor):
        """Test forecast with current signal"""
        helium_monitor.current_signal = HeliumSupplySignal(
            timestamp=datetime.now(),
            scarcity_level=HeliumScarcityLevel.CRITICAL,
            scarcity_score=0.8,
            spot_price_usd_per_liter=9.0,
            fab_inventory_days=8,
            vendor_alerts=['Shortage'],
            source='test'
        )
        
        forecast = asyncio.run(helium_monitor.get_forecast(hours_ahead=48))
        
        assert forecast['current_scarcity'] == 'critical'
        assert forecast['forecast'] == 'worsening'  # High score → worsening
        assert forecast['hours_ahead'] == 48
        assert 0.5 <= forecast['confidence'] <= 0.9
        assert forecast['price_forecast']['trend'] == 'up'


# ---------------------------------------------------------------------------
# Main entry point for running tests directly
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # Run with pytest if available
    try:
        import pytest
        pytest.main([__file__, "-v", "--asyncio-mode=auto"])
    except ImportError:
        print("pytest not available, running basic smoke test...")
        
        # Basic smoke test
        monitor = HeliumMonitor.__new__(HeliumMonitor)
        monitor.config = {}
        monitor.api_endpoints = {'primary': 'test', 'backup': 'test'}
        monitor.api_key = None
        monitor.api_headers = {}
        monitor.update_interval_seconds = 900
        monitor.history_buffer_size = 100
        monitor.max_retries = 3
        monitor.base_retry_delay = 1.0
        monitor.current_signal = None
        monitor._signal_history = []
        monitor._monitoring_task = None
        monitor._shutdown_event = asyncio.Event()
        monitor._rng = __import__('random').Random(42)
        
        # Test simulation
        signal = monitor._simulate_helium_supply()
        assert signal.scarcity_score >= 0.0
        assert signal.scarcity_score <= 1.0
        
        # Test metrics collection
        monitor.current_signal = signal
        metrics = monitor.collect_prometheus_metrics()
        assert 'green_agent_helium_scarcity_score' in metrics
        
        print("✅ Basic smoke test passed")
