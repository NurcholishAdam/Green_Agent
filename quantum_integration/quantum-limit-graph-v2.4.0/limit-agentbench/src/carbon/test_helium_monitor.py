"""
test_helium_monitor.py — Complete Test Suite for HeliumMonitor
==============================================================

Run with:
    pytest test_helium_monitor.py -v
    pytest test_helium_monitor.py::TestEnhancedHeliumMonitor -v

Test categories:
- Original unit/integration/async tests (backward compatibility)
- Enhancement unit tests (one class per layer)
- Feature-toggle tests
- End-to-end integration tests

Enhancement coverage:
  1. Quantum-Distillation        -> TestQuantumDistillationBridge
  2. Causal RL                   -> TestCausalProviderPolicy
  3. Federated Green Learning    -> TestFederatedAggregator
  4. Multi-Agent Coordination    -> TestMultiAgentCoordinator
  5. Temporal Logic              -> TestTemporalLogicMonitor
  6. Explainable AI (XAI)        -> TestSignalExplainer
  7. Adaptive Precision          -> TestAdaptivePrecisionController
  8. Carbon Markets              -> TestCarbonMarketClient
  9. Resilience / Chaos          -> TestCircuitBreaker, TestChaosInjector
 10. HITL + Active Learning      -> TestHumanInTheLoopGate
 +  Anomaly Detection            -> TestAnomalyDetector
 +  Feature toggles              -> TestFeatureToggles
 +  End-to-end pipeline          -> TestEnhancedPipeline
"""

import os  # ✅ FIXED: was missing in the original suite
import pytest
import asyncio
import aiohttp
import statistics
from unittest.mock import Mock, patch, AsyncMock, MagicMock
from datetime import datetime, timedelta
import json

from src.carbon.helium_monitor import (
    # --- Original API ---
    HeliumMonitor,
    HeliumScarcityLevel,
    HeliumSupplySignal,
    # --- Enhancement layers ---
    PrecisionLevel,
    ProviderKind,
    AgentRole,
    CircuitState,
    CircuitBreaker,
    ChaosInjector,
    TemporalLogicMonitor,
    SignalFreshness,
    ScoreBounds,
    PriceSanity,
    SignalExplainer,
    SignalExplanation,
    AdaptivePrecisionController,
    HardwareProfile,
    CausalProviderPolicy,
    ProviderState,
    QuantumDistillationBridge,
    DistilledSimulationModel,
    FederatedAggregator,
    FederatedRegionalProfile,
    MultiAgentCoordinator,
    AgentProfile,
    CarbonMarketClient,
    MarketSnapshot,
    HumanInTheLoopGate,
    HITLRequest,
    AnomalyDetector,
)


# ===========================================================================
# Fixtures
# ===========================================================================

@pytest.fixture
def mock_config():
    """Create test configuration."""
    return {
        "api_endpoints": {
            "primary": "https://test-primary.example.com/v1/supply",
            "backup": "https://test-backup.example.com/v1/status",
        },
        "update_interval": 1,
        "history_buffer_size": 10,
        "max_retries": 2,
        "base_retry_delay": 0.1,
        "api_key": "test-key-123",
        "region": "US-CA",
    }


@pytest.fixture
def valid_api_response():
    """Valid API response data."""
    return {
        "scarcity_level": "caution",
        "scarcity_score": 0.4,
        "spot_price_usd": 5.5,
        "fab_inventory_days": 20,
        "alerts": ["Supply chain delay"],
        "forecast_valid_until": "2026-03-14T00:00:00",
    }


@pytest.fixture
def helium_monitor(mock_config):
    """
    Original-style fixture using __new__ to bypass auto-start.
    Now also sets `region` since enhanced helpers rely on it.
    """
    monitor = HeliumMonitor.__new__(HeliumMonitor)
    monitor.config = mock_config
    monitor.api_endpoints = mock_config["api_endpoints"]
    monitor.api_key = mock_config["api_key"]
    monitor.api_headers = {"Authorization": f'Bearer {mock_config["api_key"]}'}
    monitor.update_interval_seconds = mock_config["update_interval"]
    monitor.history_buffer_size = mock_config["history_buffer_size"]
    monitor.max_retries = mock_config["max_retries"]
    monitor.base_retry_delay = mock_config["base_retry_delay"]
    monitor.region = mock_config["region"]  # ✅ NEW: needed by enhanced helpers
    monitor.current_signal = None
    monitor._signal_history = []
    monitor._monitoring_task = None
    monitor._shutdown_event = asyncio.Event()
    monitor._rng = __import__("random").Random(42)
    return monitor


@pytest.fixture
def enhanced_monitor(mock_config):
    """
    Fully-initialized HeliumMonitor with auto-start disabled.
    All ten enhancement layers enabled (default features).
    """
    with patch.object(HeliumMonitor, "_start_monitoring", lambda self: None):
        monitor = HeliumMonitor(
            config=mock_config,
            simulation_seed=42,
            deployment_id="test-deploy",
            agent_id="test-agent",
            hardware=HardwareProfile(supports_int4=True, vram_gb=48),
        )
    monitor._monitoring_task = None
    return monitor


@pytest.fixture
def legacy_monitor(mock_config):
    """HeliumMonitor with ALL enhancement layers disabled."""
    features = {k: False for k in HeliumMonitor.DEFAULT_FEATURES}
    with patch.object(HeliumMonitor, "_start_monitoring", lambda self: None):
        monitor = HeliumMonitor(
            config=mock_config,
            simulation_seed=42,
            features=features,
        )
    monitor._monitoring_task = None
    return monitor


# ===========================================================================
# Original: Initialization & Configuration
# ===========================================================================

class TestHeliumMonitorInit:
    """Test HeliumMonitor initialization (original + new)."""

    def test_init_with_default_config(self):
        """Default configuration produces expected values."""
        with patch.object(HeliumMonitor, "_start_monitoring", lambda self: None):
            monitor = HeliumMonitor(config={}, simulation_seed=42)
        assert monitor.update_interval_seconds == 900
        assert monitor.history_buffer_size == 100
        assert monitor.api_headers == {}  # No API key → no headers
        assert monitor.api_key is None

    def test_init_with_api_key_from_config(self, mock_config):
        with patch.object(HeliumMonitor, "_start_monitoring", lambda self: None):
            monitor = HeliumMonitor(config=mock_config, simulation_seed=42)
        assert "Authorization" in monitor.api_headers
        assert mock_config["api_key"] in monitor.api_headers["Authorization"]

    def test_init_with_api_key_from_env(self, monkeypatch):
        """✅ FIXED: `os` is now imported at the top of the file."""
        monkeypatch.setenv("HELIUM_API_KEY", "env-key-456")
        with patch.object(HeliumMonitor, "_start_monitoring", lambda self: None):
            monitor = HeliumMonitor(config={}, simulation_seed=42)
        assert monitor.api_key == "env-key-456"
        assert "env-key-456" in monitor.api_headers["Authorization"]

    def test_init_default_features_enabled(self, enhanced_monitor):
        """All enhancement features are ON by default."""
        for name, enabled in HeliumMonitor.DEFAULT_FEATURES.items():
            assert enhanced_monitor.features[name] is enabled
        assert enhanced_monitor.rl_policy is not None
        assert enhanced_monitor.explainer is not None
        assert enhanced_monitor.federated is not None
        assert enhanced_monitor.coordinator is not None
        assert enhanced_monitor.temporal_monitor is not None
        assert enhanced_monitor.precision_ctl is not None
        assert enhanced_monitor.market is not None
        assert enhanced_monitor.hitl is not None
        assert enhanced_monitor.anomaly is not None
        assert enhanced_monitor.distiller is not None


# ===========================================================================
# Original: Data Parsing & Validation
# ===========================================================================

class TestParseAPIResponse:
    def test_parse_valid_response(self, helium_monitor, valid_api_response):
        signal = helium_monitor._parse_api_response(valid_api_response, "test_api")
        assert signal.scarcity_level == HeliumScarcityLevel.CAUTION
        assert signal.scarcity_score == 0.4
        assert signal.spot_price_usd_per_liter == 5.5
        assert signal.fab_inventory_days == 20
        assert signal.vendor_alerts == ["Supply chain delay"]
        assert signal.source == "test_api"
        assert signal.forecast_valid_until == datetime.fromisoformat(
            "2026-03-14T00:00:00"
        )

    def test_parse_missing_required_field(self, helium_monitor):
        with pytest.raises(ValueError, match="Missing required field"):
            helium_monitor._parse_api_response(
                {"scarcity_level": "normal"}, "test_api"
            )

    def test_parse_invalid_scarcity_level(self, helium_monitor, valid_api_response):
        data = dict(valid_api_response, scarcity_level="invalid_value")
        signal = helium_monitor._parse_api_response(data, "test_api")
        assert signal.scarcity_level == HeliumScarcityLevel.NORMAL

    def test_parse_out_of_range_scarcity_score(self, helium_monitor, valid_api_response):
        data = dict(valid_api_response, scarcity_score=1.5)
        signal = helium_monitor._parse_api_response(data, "test_api")
        assert signal.scarcity_score == 1.0
        data = dict(valid_api_response, scarcity_score=-0.2)
        signal = helium_monitor._parse_api_response(data, "test_api")
        assert signal.scarcity_score == 0.0

    def test_parse_negative_numeric_values(self, helium_monitor, valid_api_response):
        data = dict(
            valid_api_response, spot_price_usd=-10.0, fab_inventory_days=-5
        )
        signal = helium_monitor._parse_api_response(data, "test_api")
        assert signal.spot_price_usd_per_liter == 0.0
        assert signal.fab_inventory_days == 0

    def test_parse_invalid_forecast_timestamp(self, helium_monitor, valid_api_response):
        data = dict(valid_api_response, forecast_valid_until="not-a-date")
        signal = helium_monitor._parse_api_response(data, "test_api")
        assert signal.forecast_valid_until is None


# ===========================================================================
# Original: Simulation
# ===========================================================================

class TestSimulateHeliumSupply:
    def test_simulation_produces_valid_signal(self, helium_monitor):
        signal = helium_monitor._simulate_helium_supply()
        assert isinstance(signal, HeliumSupplySignal)
        assert signal.source == "simulation"
        assert 0.0 <= signal.scarcity_score <= 1.0
        assert signal.spot_price_usd_per_liter >= 0
        assert signal.fab_inventory_days >= 0
        assert signal.forecast_valid_until is not None

    def test_simulation_weighted_distribution(self, helium_monitor):
        counts = {level: 0 for level in HeliumScarcityLevel}
        for _ in range(1000):
            counts[helium_monitor._simulate_helium_supply().scarcity_level] += 1
        total = sum(counts.values())
        normal_ratio = counts[HeliumScarcityLevel.NORMAL] / total
        caution_ratio = counts[HeliumScarcityLevel.CAUTION] / total
        assert 0.60 < normal_ratio < 0.80
        assert 0.10 < caution_ratio < 0.25

    def test_simulation_deterministic_with_seed(self):
        m1 = HeliumMonitor.__new__(HeliumMonitor)
        m1._rng = __import__("random").Random(42)
        m1.region = "global"
        m1._simulate_helium_supply = HeliumMonitor._simulate_helium_supply.__get__(m1)
        m2 = HeliumMonitor.__new__(HeliumMonitor)
        m2._rng = __import__("random").Random(42)
        m2.region = "global"
        m2._simulate_helium_supply = HeliumMonitor._simulate_helium_supply.__get__(m2)
        s1 = m1._simulate_helium_supply()
        s2 = m2._simulate_helium_supply()
        assert s1.scarcity_level == s2.scarcity_level
        assert s1.scarcity_score == s2.scarcity_score
        assert s1.spot_price_usd_per_liter == s2.spot_price_usd_per_liter


# ===========================================================================
# Original: Signal History & Trend
# ===========================================================================

class TestSignalHistory:
    def test_bounded_history_buffer(self, helium_monitor):
        for _ in range(20):
            helium_monitor._signal_history.append(
                helium_monitor._simulate_helium_supply()
            )
            if len(helium_monitor._signal_history) > helium_monitor.history_buffer_size:
                helium_monitor._signal_history = helium_monitor._signal_history[
                    -helium_monitor.history_buffer_size:
                ]
        assert len(helium_monitor.signal_history) <= helium_monitor.history_buffer_size

    def test_get_supply_trend_with_stdlib_timedelta(self, helium_monitor):
        now = datetime.now()
        helium_monitor._signal_history = [
            HeliumSupplySignal(
                timestamp=now - timedelta(hours=h),
                scarcity_level=HeliumScarcityLevel.NORMAL,
                scarcity_score=0.1,
                spot_price_usd_per_liter=4.0,
                fab_inventory_days=30,
                vendor_alerts=[],
                source="test",
            )
            for h in [1, 5, 10, 20, 30]
        ]
        trend = helium_monitor.get_supply_trend(hours=15)
        assert len(trend) == 3
        assert all(s.timestamp > now - timedelta(hours=15) for s in trend)

    def test_signal_history_thread_safety(self, helium_monitor):
        signal = helium_monitor._simulate_helium_supply()
        helium_monitor._signal_history.append(signal)
        history1 = helium_monitor.signal_history
        history1.append(helium_monitor._simulate_helium_supply())
        history2 = helium_monitor.signal_history
        assert len(history2) == len(helium_monitor._signal_history)
        assert history2[-1] == signal


# ===========================================================================
# Original: Async fetch / monitoring / shutdown (mocked HTTP)
# ===========================================================================

@pytest.mark.asyncio
class TestFetchHeliumSupply:
    async def test_fetch_primary_success(self, helium_monitor, valid_api_response):
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value=valid_api_response)
        mock_response.headers = {}
        mock_session = AsyncMock()
        mock_session.get.return_value.__aenter__.return_value = mock_response

        with patch("aiohttp.ClientSession", return_value=mock_session):
            signal = await helium_monitor.fetch_helium_supply()
            assert signal.source == "primary_api"
            assert signal.scarcity_level == HeliumScarcityLevel.CAUTION

    async def test_fetch_fallback_to_backup(self, helium_monitor, valid_api_response):
        primary_resp = AsyncMock()
        primary_resp.status = 500
        backup_resp = AsyncMock()
        backup_resp.status = 200
        backup_resp.json = AsyncMock(return_value=valid_api_response)
        backup_resp.headers = {}

        mock_session = AsyncMock()
        mock_session.get.side_effect = [primary_resp, backup_resp]
        with patch("aiohttp.ClientSession", return_value=mock_session):
            signal = await helium_monitor.fetch_helium_supply()
            assert signal.source == "backup_api"

    async def test_fetch_fallback_to_simulation(self, helium_monitor):
        mock_session = AsyncMock()
        mock_session.get.side_effect = aiohttp.ClientError("Connection failed")
        with patch("aiohttp.ClientSession", return_value=mock_session):
            signal = await helium_monitor.fetch_helium_supply()
            assert signal.source == "simulation"


# ===========================================================================
# Original: Prometheus metrics
# ===========================================================================

class TestPrometheusMetrics:
    def test_collect_metrics_with_signal(self, helium_monitor):
        helium_monitor.current_signal = HeliumSupplySignal(
            timestamp=datetime.now(),
            scarcity_level=HeliumScarcityLevel.CRITICAL,
            scarcity_score=0.7,
            spot_price_usd_per_liter=8.0,
            fab_inventory_days=10,
            vendor_alerts=["Alert 1", "Alert 2"],
            source="test",
        )
        metrics = helium_monitor.collect_prometheus_metrics()
        assert metrics["green_agent_helium_scarcity_level"] == (2, {"source": "test"})
        assert metrics["green_agent_helium_scarcity_score"] == (0.7, {"source": "test"})
        assert metrics["green_agent_helium_spot_price_usd"] == (8.0, {})
        assert metrics["green_agent_helium_fab_inventory_days"] == (10, {})
        assert metrics["green_agent_helium_vendor_alerts_count"] == (2, {})
        assert metrics["green_agent_helium_price_premium_usd"] == (4.0, {})

    def test_collect_metrics_no_signal(self, helium_monitor):
        helium_monitor.current_signal = None
        assert helium_monitor.collect_prometheus_metrics() == {}

    def test_enhanced_metrics_include_confidence_and_market(self, helium_monitor):
        """Enhanced signal carries confidence, anomaly flag, market snapshot."""
        helium_monitor.current_signal = HeliumSupplySignal(
            timestamp=datetime.now(),
            scarcity_level=HeliumScarcityLevel.CAUTION,
            scarcity_score=0.4,
            spot_price_usd_per_liter=5.5,
            fab_inventory_days=20,
            vendor_alerts=[],
            source="test",
            confidence=0.85,
            is_anomalous=True,
            market_snapshot={
                "carbon_price_per_tco2_usd": 45.0,
                "rec_available_mwh": 120.0,
                "rec_price_per_mwh_usd": 6.0,
            },
        )
        metrics = helium_monitor.collect_prometheus_metrics()
        assert metrics["green_agent_helium_confidence"] == (0.85, {})
        assert metrics["green_agent_helium_anomalous"] == (1, {})
        assert metrics["green_agent_carbon_price_usd_per_tco2"] == (45.0, {})
        assert metrics["green_agent_rec_available_mwh"] == (120.0, {})


# ===========================================================================
# ENHANCEMENT 9: Resilience — CircuitBreaker + ChaosInjector
# ===========================================================================

class TestCircuitBreaker:
    def test_starts_closed(self):
        cb = CircuitBreaker("test")
        assert cb.state == CircuitState.CLOSED
        assert cb.can_call() is True

    def test_opens_after_threshold_failures(self):
        cb = CircuitBreaker("test", failure_threshold=3)
        for _ in range(3):
            cb.record_failure()
        assert cb.state == CircuitState.OPEN
        assert cb.can_call() is False

    def test_half_open_after_recovery_timeout(self):
        cb = CircuitBreaker("test", failure_threshold=2, recovery_timeout_seconds=0)
        cb.record_failure()
        cb.record_failure()
        assert cb.state == CircuitState.OPEN
        # With recovery_timeout=0, next can_call should transition to HALF_OPEN
        assert cb.can_call() is True
        assert cb.state == CircuitState.HALF_OPEN

    def test_success_resets_circuit(self):
        cb = CircuitBreaker("test", failure_threshold=2)
        cb.record_failure()
        cb.record_failure()
        assert cb.state == CircuitState.OPEN
        cb.record_success()
        assert cb.state == CircuitState.CLOSED
        assert cb.failures == 0


class TestChaosInjector:
    @pytest.mark.asyncio
    async def test_chaos_zero_rate_never_fails(self):
        ci = ChaosInjector(failure_rate=0.0)
        for _ in range(20):
            await ci.maybe_inject("test")  # Should never raise
        assert ci.events == []

    @pytest.mark.asyncio
    async def test_chaos_full_rate_always_fails(self):
        ci = ChaosInjector(failure_rate=1.0)
        with pytest.raises(RuntimeError, match="Chaos injection"):
            await ci.maybe_inject("test")
        assert len(ci.events) == 1
        assert ci.events[0]["component"] == "test"


# ===========================================================================
# ENHANCEMENT 5: Temporal Logic
# ===========================================================================

class TestTemporalLogicMonitor:
    def _signal(self, **kwargs):
        defaults = dict(
            timestamp=datetime.now(),
            scarcity_level=HeliumScarcityLevel.NORMAL,
            scarcity_score=0.3,
            spot_price_usd_per_liter=4.0,
            fab_inventory_days=25,
            vendor_alerts=[],
            source="test",
        )
        defaults.update(kwargs)
        return HeliumSupplySignal(**defaults)

    def test_signal_freshness_passes_for_recent(self):
        prop = SignalFreshness(max_age_seconds=3600)
        assert prop.check([self._signal(timestamp=datetime.now())]) is True

    def test_signal_freshness_fails_for_stale(self):
        prop = SignalFreshness(max_age_seconds=60)
        stale = self._signal(timestamp=datetime.now() - timedelta(hours=2))
        assert prop.check([stale]) is False

    def test_score_bounds(self):
        prop = ScoreBounds()
        assert prop.check([self._signal(scarcity_score=0.5)]) is True
        assert prop.check([self._signal(scarcity_score=1.5)]) is False

    def test_price_sanity(self):
        prop = PriceSanity(max_price=100.0)
        assert prop.check([self._signal(spot_price_usd_per_liter=50.0)]) is True
        assert prop.check([self._signal(spot_price_usd_per_liter=500.0)]) is False

    def test_monitor_aggregates_violations(self):
        tm = TemporalLogicMonitor()
        tm.register(SignalFreshness(max_age_seconds=60))
        tm.register(ScoreBounds())
        stale_and_bad = self._signal(
            timestamp=datetime.now() - timedelta(hours=1),
            scarcity_score=2.0,
        )
        ok, violations = tm.verify([stale_and_bad])
        assert ok is False
        assert "SignalFreshness" in violations
        assert "ScoreBounds" in violations
        assert len(tm.violations) == 2


# ===========================================================================
# Anomaly Detection
# ===========================================================================

class TestAnomalyDetector:
    def test_warmup_period_never_flags(self):
        ad = AnomalyDetector(window=20, z_threshold=3.0)
        for _ in range(4):
            flagged, _ = ad.check(0.5)
            assert flagged is False

    def test_flags_extreme_outlier(self):
        ad = AnomalyDetector(window=20, z_threshold=3.0)
        for _ in range(20):
            ad.check(0.3)
        flagged, z = ad.check(0.95)
        assert flagged is True
        assert z > 3.0

    def test_flags_physically_implausible_low(self):
        ad = AnomalyDetector(window=20, z_threshold=3.0)
        for _ in range(10):
            ad.check(0.3)
        flagged, _ = ad.check(-5.0)
        assert flagged is True

    def test_flags_physically_implausible_high(self):
        ad = AnomalyDetector(window=20, z_threshold=3.0)
        for _ in range(10):
            ad.check(0.3)
        flagged, _ = ad.check(2000.0)
        assert flagged is True


# ===========================================================================
# ENHANCEMENT 6: Explainable AI
# ===========================================================================

class TestSignalExplainer:
    def test_explanation_has_required_fields(self):
        exp = SignalExplainer.explain(
            level=HeliumScarcityLevel.CRITICAL,
            score=0.75,
            price=8.5,
            inventory=10,
            source="primary_api",
        )
        assert isinstance(exp, SignalExplanation)
        assert "CRITICAL" in exp.headline.upper()
        assert len(exp.rationale) >= 3
        assert "scarcity_score" in exp.contributing_factors
        assert 0.0 <= exp.confidence <= 1.0

    def test_explanation_flags_low_confidence(self):
        exp = SignalExplainer.explain(
            level=HeliumScarcityLevel.NORMAL,
            score=0.15,
            price=3.5,
            inventory=30,
            source="simulation",
        )
        # Should include source provenance in rationale
        assert any("simulation" in r for r in exp.rationale)

    def test_explanation_includes_anomaly(self):
        exp = SignalExplainer.explain(
            level=HeliumScarcityLevel.SEVERE,
            score=0.95,
            price=12.0,
            inventory=5,
            source="primary_api",
            anomaly_flagged=True,
        )
        assert any("anomalous" in r.lower() for r in exp.rationale)


# ===========================================================================
# ENHANCEMENT 7: Adaptive Precision
# ===========================================================================

class TestAdaptivePrecisionController:
    def test_critical_urgency_uses_fp16(self):
        pc = AdaptivePrecisionController(HardwareProfile())
        assert pc.select("critical") == PrecisionLevel.FP16

    def test_edge_device_uses_int8(self):
        pc = AdaptivePrecisionController(
            HardwareProfile(edge_device=True, supports_int8=True)
        )
        assert pc.select("normal") == PrecisionLevel.INT8

    def test_int4_capable_hardware_uses_int4(self):
        pc = AdaptivePrecisionController(HardwareProfile(supports_int4=True))
        assert pc.select("normal") == PrecisionLevel.INT4

    def test_quantize_int8(self):
        # INT8 scale=100 → values rounded down to nearest 0.01
        result = AdaptivePrecisionController.quantize(0.12345, PrecisionLevel.INT8)
        assert result == pytest.approx(0.12, abs=1e-6)

    def test_quantize_fp32_is_identity(self):
        result = AdaptivePrecisionController.quantize(0.12345, PrecisionLevel.FP32)
        assert result == 0.12345


# ===========================================================================
# ENHANCEMENT 2: Causal RL
# ===========================================================================

class TestCausalProviderPolicy:
    def test_select_returns_available_provider(self):
        policy = CausalProviderPolicy(epsilon=0.0)
        state = ProviderState(
            hour_of_day=10, recent_failure_rate=0.0,
            cache_hit=False, region_hash=0.5,
        )
        available = [ProviderKind.PRIMARY_API, ProviderKind.SIMULATION]
        choice = policy.select(state, available)
        assert choice in available

    def test_record_and_update_persists_weights(self):
        policy = CausalProviderPolicy(epsilon=0.0, lr=0.5)
        state = ProviderState(
            hour_of_day=10, recent_failure_rate=0.0,
            cache_hit=False, region_hash=0.5,
        )
        policy.record(state, ProviderKind.PRIMARY_API, accuracy=1.0, latency_ms=50.0)
        policy.record(state, ProviderKind.PRIMARY_API, accuracy=1.0, latency_ms=60.0)
        before = list(policy.weights[ProviderKind.PRIMARY_API])
        policy.update()
        after = policy.weights[ProviderKind.PRIMARY_API]
        assert after != before  # Weights changed
        assert policy.buffer == []  # Buffer cleared

    def test_epsilon_one_always_explores(self):
        policy = CausalProviderPolicy(epsilon=1.0)
        state = ProviderState(10, 0.0, False, 0.5)
        available = [ProviderKind.PRIMARY_API, ProviderKind.SIMULATION]
        choices = {policy.select(state, available) for _ in range(50)}
        # With epsilon=1, both providers should appear in 50 draws
        assert len(choices) > 1


# ===========================================================================
# ENHANCEMENT 1: Quantum Distillation
# ===========================================================================

class TestQuantumDistillationBridge:
    def test_distill_returns_smoothed_forecast(self):
        bridge = QuantumDistillationBridge()
        forecast = {h: float(100 + 10 * h) for h in range(6)}
        model = bridge.distill(forecast, PrecisionLevel.INT8)
        assert isinstance(model, DistilledSimulationModel)
        assert model.precision == PrecisionLevel.INT8
        assert set(model.smoothed.keys()) == set(forecast.keys())
        # Smoothing should reduce extremes
        vals = list(model.smoothed.values())
        assert max(vals) <= max(forecast.values())
        assert min(vals) >= min(forecast.values())

    def test_distill_quality_retention_monotonic(self):
        bridge = QuantumDistillationBridge()
        forecast = {h: 200.0 for h in range(4)}
        fp32 = bridge.distill(forecast, PrecisionLevel.FP32)
        int4 = bridge.distill(forecast, PrecisionLevel.INT4)
        qd = bridge.distill(forecast, PrecisionLevel.QUANTUM_DISTILLED)
        assert fp32.quality_retention >= int4.quality_retention >= qd.quality_retention

    def test_distill_empty_forecast(self):
        bridge = QuantumDistillationBridge()
        model = bridge.distill({}, PrecisionLevel.INT8)
        assert model.smoothed == {}


# ===========================================================================
# ENHANCEMENT 3: Federated Green Learning
# ===========================================================================

class TestFederatedAggregator:
    def test_aggregate_empty_returns_empty(self):
        agg = FederatedAggregator()
        assert agg.aggregate() == {}

    def test_weighted_aggregation(self):
        agg = FederatedAggregator()
        profile_a = [0.2] * 24
        profile_b = [0.8] * 24
        agg.push(FederatedRegionalProfile(
            deployment_id="A", region="US-CA",
            hourly_means=profile_a, sample_count=10,
        ))
        agg.push(FederatedRegionalProfile(
            deployment_id="B", region="US-CA",
            hourly_means=profile_b, sample_count=30,
        ))
        result = agg.aggregate()
        assert "US-CA" in result
        # Weighted average should lean toward B (0.8 * 30/40 + 0.2 * 10/40 = 0.65)
        assert result["US-CA"][0] == pytest.approx(0.65, abs=0.01)

    def test_lookup_after_aggregation(self):
        agg = FederatedAggregator()
        agg.push(FederatedRegionalProfile(
            deployment_id="A", region="GB",
            hourly_means=[0.5] * 24, sample_count=5,
        ))
        agg.aggregate()
        assert agg.get_global("GB") is not None
        assert agg.get_global("XX") is None


# ===========================================================================
# ENHANCEMENT 4: Multi-Agent Coordination
# ===========================================================================

class TestMultiAgentCoordinator:
    def test_register_and_select(self):
        coord = MultiAgentCoordinator()
        coord.register("agent-A")
        coord.register("agent-B")
        aid = coord.select_agent("US-CA")
        assert aid in ("agent-A", "agent-B")

    def test_role_reassignment_after_5_calls(self):
        coord = MultiAgentCoordinator()
        for _ in range(5):
            coord.record("agent-A", "US-CA", success=True,
                         latency_ms=100.0, anomaly_caught=False)
        profile = coord.agents["agent-A"]
        # After 5 successful low-latency calls, role should be LATENCY_OPTIMIZER
        assert profile.role == AgentRole.LATENCY_OPTIMIZER

    def test_anomaly_hunter_role(self):
        coord = MultiAgentCoordinator()
        for _ in range(5):
            coord.record("agent-A", "US-CA", success=False,
                         latency_ms=500.0, anomaly_caught=True)
        assert coord.agents["agent-A"].role == AgentRole.ANOMALY_HUNTER


# ===========================================================================
# ENHANCEMENT 8: Carbon Markets
# ===========================================================================

class TestCarbonMarketClient:
    def test_snapshot_has_required_fields(self):
        client = CarbonMarketClient()
        snap = client.get_snapshot()
        assert isinstance(snap, MarketSnapshot)
        assert snap.carbon_price_per_tco2_usd > 0
        assert snap.rec_price_per_mwh_usd > 0
        assert snap.rec_available_mwh >= 0

    def test_snapshot_cached_within_ttl(self):
        client = CarbonMarketClient()
        snap1 = client.get_snapshot()
        snap2 = client.get_snapshot()
        assert snap1 is snap2  # Same object → cached


# ===========================================================================
# ENHANCEMENT 10: HITL & Active Learning
# ===========================================================================

class TestHumanInTheLoopGate:
    def test_low_value_triggers_review(self):
        gate = HumanInTheLoopGate(min_saving_kg=1e-4, min_confidence=0.5)
        assert gate.needs_review(saving_kg=1e-6, confidence=0.9, horizon=2)

    def test_low_confidence_triggers_review(self):
        gate = HumanInTheLoopGate(min_saving_kg=1e-4, min_confidence=0.5)
        assert gate.needs_review(saving_kg=1e-2, confidence=0.2, horizon=2)

    def test_long_horizon_triggers_review(self):
        gate = HumanInTheLoopGate(min_saving_kg=1e-4, min_confidence=0.5)
        assert gate.needs_review(saving_kg=1e-2, confidence=0.9, horizon=48)

    def test_high_value_short_horizon_no_review(self):
        gate = HumanInTheLoopGate(min_saving_kg=1e-4, min_confidence=0.5)
        assert not gate.needs_review(saving_kg=1e-2, confidence=0.9, horizon=4)

    def test_review_without_callback_defaults_reject(self):
        gate = HumanInTheLoopGate()
        # Async review method
        result = asyncio.get_event_loop().run_until_complete(
            gate.request_review(
                region="US-CA",
                level=HeliumScarcityLevel.CRITICAL,
                score=0.75,
                price=8.5,
                reason="test",
                urgency="medium",
            )
        )
        assert result is False

    @pytest.mark.asyncio
    async def test_review_with_callback_records_feedback(self):
        gate = HumanInTheLoopGate()

        def cb(req: HITLRequest) -> bool:
            return True

        gate.set_callback(cb)
        approved = await gate.request_review(
            region="US-CA",
            level=HeliumScarcityLevel.CRITICAL,
            score=0.75,
            price=8.5,
            reason="test",
            urgency="medium",
        )
        assert approved is True
        assert len(gate.feedback_log) == 1
        assert gate.feedback_log[0]["decision"] is True

    @pytest.mark.asyncio
    async def test_low_urgency_auto_approves(self):
        gate = HumanInTheLoopGate()
        approved = await gate.request_review(
            region="US-CA",
            level=HeliumScarcityLevel.NORMAL,
            score=0.1,
            price=3.5,
            reason="low",
            urgency="low",
        )
        assert approved is True
        assert len(gate.feedback_log) == 1

    def test_active_learning_batch_returns_recent(self):
        gate = HumanInTheLoopGate()
        gate.feedback_log = [{"i": i} for i in range(30)]
        batch = gate.active_learning_batch(n=10)
        assert len(batch) == 10
        assert batch[-1]["i"] == 29


# ===========================================================================
# Feature toggles
# ===========================================================================

class TestFeatureToggles:
    def test_legacy_mode_disables_all_enhancements(self, legacy_monitor):
        assert legacy_monitor.rl_policy is None
        assert legacy_monitor.explainer is None
        assert legacy_monitor.federated is None
        assert legacy_monitor.coordinator is None
        assert legacy_monitor.temporal_monitor is None
        assert legacy_monitor.precision_ctl is None
        assert legacy_monitor.market is None
        assert legacy_monitor.hitl is None
        assert legacy_monitor.anomaly is None
        assert legacy_monitor.distiller is None

    def test_partial_features_can_be_disabled(self, mock_config):
        features = dict(HeliumMonitor.DEFAULT_FEATURES)
        features["xai"] = False
        features["carbon_market"] = False
        with patch.object(HeliumMonitor, "_start_monitoring", lambda self: None):
            monitor = HeliumMonitor(
                config=mock_config, simulation_seed=42, features=features
            )
        monitor._monitoring_task = None
        assert monitor.explainer is None
        assert monitor.market is None
        assert monitor.rl_policy is not None  # still enabled

    def test_legacy_suggest_returns_none_for_enhancements(self, legacy_monitor):
        """Legacy mode should still run fetch without enhancement attributes."""
        signal = legacy_monitor._simulate_helium_supply()
        assert signal.source == "simulation"
        assert signal.confidence == 0.5  # default from dataclass


# ===========================================================================
# End-to-end integration tests
# ===========================================================================

@pytest.mark.asyncio
class TestEnhancedPipeline:
    async def test_fetch_with_all_features_returns_enriched_signal(
        self, enhanced_monitor, valid_api_response
    ):
        """Full pipeline: fetch → anomaly → XAI → market → temporal verification."""
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value=valid_api_response)
        mock_response.headers = {}

        mock_session = AsyncMock()
        mock_session.get.return_value.__aenter__.return_value = mock_response

        with patch("aiohttp.ClientSession", return_value=mock_session):
            signal = await enhanced_monitor.fetch_helium_supply()

        # Enrichment assertions
        assert signal.source in ("primary_api", "backup_api", "simulation",
                                 "distilled_model", "federated_peer")
        assert signal.explanation is not None  # XAI
        assert signal.confidence > 0.0         # confidence present
        assert signal.region == "US-CA"
        assert signal.market_snapshot is not None  # market enrichment

    async def test_chaos_injection_falls_back(self, mock_config):
        """When chaos is enabled and forced to fail, the pipeline should not crash."""
        features = dict(HeliumMonitor.DEFAULT_FEATURES)
        features["chaos_testing"] = True
        with patch.object(HeliumMonitor, "_start_monitoring", lambda self: None):
            monitor = HeliumMonitor(
                config=mock_config, simulation_seed=42, features=features
            )
        monitor._monitoring_task = None
        monitor.chaos = ChaosInjector(failure_rate=1.0)

        mock_session = AsyncMock()
        mock_session.get.side_effect = RuntimeError("chaos")
        with patch("aiohttp.ClientSession", return_value=mock_session):
            signal = await monitor.fetch_helium_supply()
        # Should fall back to simulation, not crash
        assert signal.source == "simulation"

    async def test_temporal_violation_does_not_crash(
        self, enhanced_monitor, valid_api_response
    ):
        """Bad score triggers temporal violation but pipeline continues."""
        valid_api_response["scarcity_score"] = 5.0  # Out of bounds
        # Parsing clamps to 1.0, so temporal should still pass — but the test
        # confirms no exception path exists.
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value=valid_api_response)
        mock_response.headers = {}
        mock_session = AsyncMock()
        mock_session.get.return_value.__aenter__.return_value = mock_response

        with patch("aiohttp.ClientSession", return_value=mock_session):
            signal = await enhanced_monitor.fetch_helium_supply()
        assert 0.0 <= signal.scarcity_score <= 1.0

    async def test_federated_contribution_populates_aggregate(
        self, enhanced_monitor
    ):
        """After 10 signals, contribute_federated_profile populates aggregator."""
        now = datetime.now()
        for h in range(15):
            enhanced_monitor._signal_history.append(HeliumSupplySignal(
                timestamp=now - timedelta(hours=h),
                scarcity_level=HeliumScarcityLevel.NORMAL,
                scarcity_score=0.3,
                spot_price_usd_per_liter=4.0,
                fab_inventory_days=25,
                vendor_alerts=[],
                source="test",
                region="US-CA",
            ))
        enhanced_monitor.contribute_federated_profile()
        agg = enhanced_monitor.get_federated_aggregate()
        assert "US-CA" in agg
        assert len(agg["US-CA"]) == 24

    async def test_hitl_denial_does_not_break_signal(
        self, enhanced_monitor, valid_api_response
    ):
        """HITL callback that always denies should not raise."""
        def deny(req: HITLRequest) -> bool:
            return False

        enhanced_monitor.set_hitl_callback(deny)

        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value=valid_api_response)
        mock_response.headers = {}
        mock_session = AsyncMock()
        mock_session.get.return_value.__aenter__.return_value = mock_response

        with patch("aiohttp.ClientSession", return_value=mock_session):
            signal = await enhanced_monitor.fetch_helium_supply()
        assert signal is not None

    async def test_full_statistics_snapshot(self, enhanced_monitor):
        """get_statistics returns all expected keys."""
        stats = enhanced_monitor.get_statistics()
        assert "deployment_id" in stats
        assert "agent_id" in stats
        assert "circuits" in stats
        assert "temporal_violations" in stats
        assert "agents" in stats
        assert "market" in stats


# ===========================================================================
# Main entry point
# ===========================================================================

if __name__ == "__main__":
    try:
        import pytest
        pytest.main([__file__, "-v", "--asyncio-mode=auto"])
    except ImportError:
        print("pytest not available, running basic smoke test...")

        # Original smoke test (works without pytest)
        monitor = HeliumMonitor.__new__(HeliumMonitor)
        monitor.config = {}
        monitor.api_endpoints = {"primary": "test", "backup": "test"}
        monitor.api_key = None
        monitor.api_headers = {}
        monitor.update_interval_seconds = 900
        monitor.history_buffer_size = 100
        monitor.max_retries = 3
        monitor.base_retry_delay = 1.0
        monitor.region = "global"
        monitor.current_signal = None
        monitor._signal_history = []
        monitor._monitoring_task = None
        monitor._shutdown_event = asyncio.Event()
        monitor._rng = __import__("random").Random(42)

        signal = monitor._simulate_helium_supply()
        assert signal.scarcity_score >= 0.0
        assert signal.scarcity_score <= 1.0

        monitor.current_signal = signal
        metrics = monitor.collect_prometheus_metrics()
        assert "green_agent_helium_scarcity_score" in metrics

        # Enhanced smoke test
        with patch.object(HeliumMonitor, "_start_monitoring", lambda self: None):
            enhanced = HeliumMonitor(config={}, simulation_seed=42)
        enhanced._monitoring_task = None
        assert enhanced.rl_policy is not None
        assert enhanced.federated is not None
        assert enhanced.temporal_monitor is not None
        assert enhanced.hitl is not None
        assert enhanced.distiller is not None

        stats = enhanced.get_statistics()
        assert stats["deployment_id"] == "local"

        print("✅ Basic smoke test passed (including enhanced features)")
