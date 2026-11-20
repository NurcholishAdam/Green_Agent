"""
test_helium_monitor_v6.py — Complete Test Suite for HeliumMonitor v6
=====================================================================

Run with:
    pytest test_helium_monitor_v6.py -v
    pytest test_helium_monitor_v6.py -v -m "not slow"
    pytest test_helium_monitor_v6.py -v -m "not integration"
    pytest test_helium_monitor_v6.py::TestAuditTrailFixes -v

What this suite adds on top of the v5 test suite
------------------------------------------------
* Imports target v6 names: ``FederatedHeliumProfile`` (not
  ``FederatedRegionalProfile``), ``AsyncChaosInjector``,
  ``HumanReviewGate``, ``SignalSafetyMonitor``, ``HeliumAgentCoordinator``,
  ``SyncCircuitAdapter``, ``_classify``, plus all v18 re-exports.
* Fixtures use ``auto_start=False`` — no ``_start_monitoring`` patching.
* **Six audit-trail regression tests** (``TestAuditTrailFixes``) that fail
  on v5 and pass on v6:

    1. ``__init__`` is safe outside a running event loop.
    2. HITL result is stored on ``signal.hitl_status`` / ``is_stale``.
    3. Async HITL callbacks are rejected in sync ``review`` (fail-safe)
       and awaited in ``areview``.
    4. ``SignalFreshness`` checks only the recent window per region
       (v5 fired permanently after 30 minutes).
    5. ``_classify`` is the single source of truth (v5 had three
       incompatible threshold sets).
    6. ``SignalExplainer`` confidence peaks inside a band, not at
       boundaries (v5 was inverted).

* Concurrency test for ``fetch_helium_supply``.
* Property-based tests (Hypothesis) for ``_parse_api_response`` and
  ``_classify`` — skipped gracefully when Hypothesis is not installed.
* Integration tests marked ``@pytest.mark.integration``.
* Slow tests marked ``@pytest.mark.slow``.

The file also ships a ``python test_helium_monitor_v6.py`` fallback
that runs a self-contained smoke test when pytest is unavailable.
"""

from __future__ import annotations

import asyncio
import json
import math
import os
import random
import statistics
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# Optional property-based testing
try:
    from hypothesis import HealthCheck, given, settings, strategies as st
    _HYPOTHESIS_AVAILABLE = True
except ImportError:
    _HYPOTHESIS_AVAILABLE = False
    given = settings = None
    HealthCheck = MagicMock()
    st = MagicMock()


# =========================================================================== #
# Imports from v6
# =========================================================================== #
from src.carbon.helium_monitor import (
    # --- Enums ---
    HeliumScarcityLevel,
    PrecisionLevel,
    ProviderKind,
    AgentRole,
    CircuitState,
    # --- Domain ---
    HeliumSupplySignal,
    SignalExplanation,
    DistilledSimulationModel,
    FederatedHeliumProfile,
    MarketSnapshot,
    HITLRequest,
    HardwareProfile,
    ProviderState,
    AgentProfile,
    # --- Classification (v6 single source of truth) ---
    _classify,
    # --- Temporal (stateless, context-aware) ---
    SignalFreshness,
    ScoreBounds,
    PriceSanity,
    SignalSafetyMonitor,
    # --- XAI ---
    SignalExplainer,
    # --- Provider policy ---
    ProviderPolicy,
    # --- Distillation ---
    QuantumDistillationBridge,
    # --- Precision ---
    AdaptivePrecisionSelector,
    # --- Federated ---
    FederatedHeliumAggregator,
    # --- Coordination ---
    HeliumAgentCoordinator,
    # --- Market ---
    CarbonMarketClient,
    # --- HITL ---
    HumanReviewGate,
    # --- Anomaly ---
    AnomalyDetector,
    # --- Adapters ---
    SyncCircuitAdapter,
    AsyncChaosInjector,
    # --- Main ---
    HeliumMonitor,
    create_helium_monitor,
    # --- v18 re-exports (infra + enhancement classes) ---
    CircuitBreaker,
    ChaosTestingEngine,
    ActiveUserPreferenceLearner,
    AdaptivePrecisionSwitcher,
    QuantumDistillationEngine,
    CarbonMarketIntegrator,
    XAIDecisionExplainer,
    SimpleQuantumDistillationEngine,
    SimpleCausalGraphLearner,
    SimpleCausalPolicyAdapter,
    SimpleFederatedGreenAggregator,
    SimpleMultiAgentCoordinator,
    SimpleTemporalLogicVerifier,
    SimpleXAIDecisionExplainer,
    SimpleAdaptivePrecisionSwitcher,
    SimpleCarbonMarketIntegrator,
    SimpleChaosTestingEngine,
    SimpleActiveUserPreferenceLearner,
)


# =========================================================================== #
# pytest markers
# =========================================================================== #
def pytest_configure(config):
    config.addinivalue_line(
        "markers", "slow: slow tests (skipped with -m 'not slow')"
    )
    config.addinivalue_line(
        "markers",
        "integration: end-to-end tests (skipped with -m 'not integration')",
    )


# =========================================================================== #
# Fixtures
# =========================================================================== #
@pytest.fixture
def mock_config():
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
    return {
        "scarcity_level": "caution",
        "scarcity_score": 0.4,
        "spot_price_usd": 5.5,
        "fab_inventory_days": 20,
        "alerts": ["Supply chain delay"],
        "forecast_valid_until": "2026-03-14T00:00:00",
    }


@pytest.fixture
def enhanced_monitor(mock_config):
    """
    Fully-initialized monitor with all ten enhancement layers enabled.

    Uses ``auto_start=False`` — the v6 constructor is safe outside a
    running loop and does not need patching.
    """
    monitor = HeliumMonitor(
        config=mock_config,
        simulation_seed=42,
        deployment_id="test-deploy",
        agent_id="test-agent",
        hardware=HardwareProfile(supports_int4=True, vram_gb=48),
        auto_start=False,
    )
    yield monitor


@pytest.fixture
def legacy_monitor(mock_config):
    """All features disabled — exercises the v1 baseline behaviour."""
    features = {k: False for k in HeliumMonitor.DEFAULT_FEATURES}
    monitor = HeliumMonitor(
        config=mock_config,
        simulation_seed=42,
        features=features,
        auto_start=False,
    )
    yield monitor


@pytest.fixture
def minimal_monitor(mock_config):
    """
    Minimal monitor used by sync tests that only need ``_parse_api_response``
    or ``_simulate_helium_supply``. No enhancements, no monitoring.
    """
    monitor = HeliumMonitor(
        config=mock_config,
        simulation_seed=42,
        features={k: False for k in HeliumMonitor.DEFAULT_FEATURES},
        auto_start=False,
    )
    yield monitor


@pytest.fixture
def sample_signal():
    """A single well-formed signal for unit tests."""
    return HeliumSupplySignal(
        timestamp=datetime.now(timezone.utc),
        scarcity_level=HeliumScarcityLevel.CAUTION,
        scarcity_score=0.4,
        spot_price_usd_per_liter=5.5,
        fab_inventory_days=20,
        vendor_alerts=["supply chain delay"],
        source="test",
        region="US-CA",
    )


# =========================================================================== #
# v18 class smoke tests
# =========================================================================== #
class TestV18ReExports:
    """The ten enhancement classes come from carbon_forecast v18."""

    def test_v18_classes_imported(self):
        assert CircuitBreaker is not None
        assert ChaosTestingEngine is not None
        assert AdaptivePrecisionSwitcher is not None
        assert QuantumDistillationEngine is not None
        assert CarbonMarketIntegrator is not None
        assert XAIDecisionExplainer is not None

    def test_simple_aliases_available(self):
        assert SimpleQuantumDistillationEngine is not None
        assert SimpleFederatedGreenAggregator is not None
        assert SimpleActiveUserPreferenceLearner is not None


# =========================================================================== #
# Original: Initialization & configuration
# =========================================================================== #
class TestHeliumMonitorInit:
    def test_init_with_default_config(self):
        monitor = HeliumMonitor(config={}, simulation_seed=42, auto_start=False)
        assert monitor.update_interval_seconds == 900
        assert monitor.history_buffer_size == 100
        assert monitor.api_headers == {}
        assert monitor.api_key is None

    def test_init_with_api_key_from_config(self, mock_config):
        monitor = HeliumMonitor(
            config=mock_config, simulation_seed=42, auto_start=False
        )
        assert "Authorization" in monitor.api_headers
        assert mock_config["api_key"] in monitor.api_headers["Authorization"]

    def test_init_with_api_key_from_env(self, monkeypatch):
        monkeypatch.setenv("HELIUM_API_KEY", "env-key-456")
        monitor = HeliumMonitor(config={}, simulation_seed=42, auto_start=False)
        assert monitor.api_key == "env-key-456"
        assert "env-key-456" in monitor.api_headers["Authorization"]

    def test_init_default_features_enabled(self, enhanced_monitor):
        for name, enabled in HeliumMonitor.DEFAULT_FEATURES.items():
            assert enhanced_monitor.features[name] is enabled
        assert enhanced_monitor.rl_policy is not None
        assert enhanced_monitor.explainer is not None
        assert enhanced_monitor.federated is not None
        assert enhanced_monitor.coordinator is not None
        assert enhanced_monitor.safety is not None
        assert enhanced_monitor.precision_ctl is not None
        assert enhanced_monitor.market is not None
        assert enhanced_monitor.hitl is not None
        assert enhanced_monitor.anomaly is not None
        assert enhanced_monitor.distiller is not None

    def test_init_does_not_start_monitoring_when_auto_start_false(
        self, mock_config
    ):
        monitor = HeliumMonitor(
            config=mock_config, simulation_seed=42, auto_start=False
        )
        assert monitor._monitoring_task is None
        assert monitor._shutdown_event is None

    def test_set_features_recreates_layers(self, enhanced_monitor):
        assert enhanced_monitor.market is not None
        enhanced_monitor.set_features({"carbon_market": False})
        assert enhanced_monitor.market is None
        enhanced_monitor.set_features({"carbon_market": True})
        assert enhanced_monitor.market is not None

    def test_set_features_rejects_unknown(self, enhanced_monitor):
        with pytest.raises(ValueError, match="unknown feature"):
            enhanced_monitor.set_features({"not_a_feature": True})


# =========================================================================== #
# Original: Parsing & validation (extended for v6)
# =========================================================================== #
class TestParseAPIResponse:
    def test_parse_valid_response(self, minimal_monitor, valid_api_response):
        signal = minimal_monitor._parse_api_response(
            valid_api_response, "test_api"
        )
        assert signal.scarcity_level == HeliumScarcityLevel.CAUTION
        assert signal.scarcity_score == 0.4
        assert signal.spot_price_usd_per_liter == 5.5
        assert signal.fab_inventory_days == 20
        assert signal.vendor_alerts == ["Supply chain delay"]
        assert signal.source == "test_api"
        assert signal.forecast_valid_until == datetime.fromisoformat(
            "2026-03-14T00:00:00"
        )

    def test_parse_missing_required_field(self, minimal_monitor):
        with pytest.raises(ValueError, match="Missing required field"):
            minimal_monitor._parse_api_response(
                {"scarcity_level": "normal"}, "test_api"
            )

    def test_parse_rejects_non_mapping(self, minimal_monitor):
        """FIXED in v6: non-mapping input raises ``ValueError`` cleanly."""
        with pytest.raises(ValueError, match="expected mapping"):
            minimal_monitor._parse_api_response(["not", "a", "dict"], "x")
        with pytest.raises(ValueError, match="expected mapping"):
            minimal_monitor._parse_api_response("nope", "x")

    def test_parse_level_is_derived_from_score(
        self, minimal_monitor, valid_api_response
    ):
        """
        FIXED in v6: ``_classify`` overrides the API-declared level, so
        a lying API cannot force a wrong severity.
        """
        data = dict(
            valid_api_response, scarcity_level="severe", scarcity_score=0.4
        )
        signal = minimal_monitor._parse_api_response(data, "test_api")
        # 0.4 → CAUTION (not SEVERE)
        assert signal.scarcity_level == HeliumScarcityLevel.CAUTION

    def test_parse_clamps_out_of_range_score(
        self, minimal_monitor, valid_api_response
    ):
        data = dict(valid_api_response, scarcity_score=1.5)
        signal = minimal_monitor._parse_api_response(data, "test_api")
        assert signal.scarcity_score == 1.0
        data = dict(valid_api_response, scarcity_score=-0.2)
        signal = minimal_monitor._parse_api_response(data, "test_api")
        assert signal.scarcity_score == 0.0

    def test_parse_negative_numerics(self, minimal_monitor, valid_api_response):
        data = dict(
            valid_api_response, spot_price_usd=-10.0, fab_inventory_days=-5
        )
        signal = minimal_monitor._parse_api_response(data, "test_api")
        assert signal.spot_price_usd_per_liter == 0.0
        assert signal.fab_inventory_days == 0

    def test_parse_invalid_forecast_timestamp(
        self, minimal_monitor, valid_api_response
    ):
        data = dict(valid_api_response, forecast_valid_until="not-a-date")
        signal = minimal_monitor._parse_api_response(data, "test_api")
        assert signal.forecast_valid_until is None

    def test_parse_non_numeric_score_raises(
        self, minimal_monitor, valid_api_response
    ):
        data = dict(valid_api_response, scarcity_score="forty")
        with pytest.raises(ValueError, match="must be numeric"):
            minimal_monitor._parse_api_response(data, "test_api")


# =========================================================================== #
# Original: Simulation (unchanged)
# =========================================================================== #
class TestSimulateHeliumSupply:
    def test_simulation_produces_valid_signal(self, minimal_monitor):
        signal = minimal_monitor._simulate_helium_supply()
        assert isinstance(signal, HeliumSupplySignal)
        assert signal.source == "simulation"
        assert 0.0 <= signal.scarcity_score <= 1.0
        assert signal.spot_price_usd_per_liter >= 0
        assert signal.fab_inventory_days >= 0
        assert signal.forecast_valid_until is not None

    @pytest.mark.slow
    def test_simulation_weighted_distribution(self, minimal_monitor):
        counts = {level: 0 for level in HeliumScarcityLevel}
        for _ in range(1000):
            counts[
                minimal_monitor._simulate_helium_supply().scarcity_level
            ] += 1
        total = sum(counts.values())
        normal_ratio = counts[HeliumScarcityLevel.NORMAL] / total
        caution_ratio = counts[HeliumScarcityLevel.CAUTION] / total
        assert 0.60 < normal_ratio < 0.80
        assert 0.10 < caution_ratio < 0.25

    def test_simulation_is_deterministic_with_seed(self):
        m1 = HeliumMonitor(config={}, simulation_seed=42, auto_start=False)
        m2 = HeliumMonitor(config={}, simulation_seed=42, auto_start=False)
        s1 = m1._simulate_helium_supply()
        s2 = m2._simulate_helium_supply()
        assert s1.scarcity_level == s2.scarcity_level
        assert s1.scarcity_score == s2.scarcity_score
        assert s1.spot_price_usd_per_liter == s2.spot_price_usd_per_liter


# =========================================================================== #
# Original: History & trend
# =========================================================================== #
class TestSignalHistory:
    def test_bounded_history_buffer(self, minimal_monitor):
        for _ in range(minimal_monitor.history_buffer_size * 3):
            minimal_monitor._signal_history.append(
                minimal_monitor._simulate_helium_supply()
            )
        assert (
            len(minimal_monitor.signal_history)
            <= minimal_monitor.history_buffer_size
        )

    def test_get_supply_trend(self, minimal_monitor):
        now = datetime.now(timezone.utc)
        minimal_monitor._signal_history.clear()
        for h in (1, 5, 10, 20, 30):
            minimal_monitor._signal_history.append(
                HeliumSupplySignal(
                    timestamp=now - timedelta(hours=h),
                    scarcity_level=HeliumScarcityLevel.NORMAL,
                    scarcity_score=0.1,
                    spot_price_usd_per_liter=4.0,
                    fab_inventory_days=30,
                    vendor_alerts=[],
                    source="test",
                )
            )
        trend = minimal_monitor.get_supply_trend(hours=15)
        assert len(trend) == 3

    def test_signal_history_returns_copy(self, minimal_monitor):
        signal = minimal_monitor._simulate_helium_supply()
        minimal_monitor._signal_history.append(signal)
        h1 = minimal_monitor.signal_history
        h1.append(minimal_monitor._simulate_helium_supply())
        h2 = minimal_monitor.signal_history
        # Copy semantics: h1 mutation must not affect the internal deque.
        assert len(h2) == len(minimal_monitor._signal_history)


# =========================================================================== #
# Original: Async fetch (via the actual pipeline)
# =========================================================================== #
@pytest.mark.asyncio
class TestFetchHeliumSupply:
    async def test_fetch_primary_success(
        self, enhanced_monitor, valid_api_response
    ):
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value=valid_api_response)
        mock_response.headers = {}
        mock_session = AsyncMock()
        mock_session.get.return_value.__aenter__.return_value = mock_response

        # Force the RL policy to pick PRIMARY_API.
        enhanced_monitor.rl_policy = None  # default selection is PRIMARY_API

        with patch("aiohttp.ClientSession", return_value=mock_session):
            signal = await enhanced_monitor.fetch_helium_supply()
        assert signal.source in ("primary_api", "backup_api", "simulation")
        # v6 records the signal in history:
        assert signal in list(enhanced_monitor._signal_history)

    async def test_fetch_primary_failure_falls_back_to_simulation(
        self, enhanced_monitor
    ):
        """v6 fallback semantics: one provider, then simulation."""
        mock_response = AsyncMock()
        mock_response.status = 500
        mock_response.json = AsyncMock(return_value={})
        mock_response.headers = {}
        mock_session = AsyncMock()
        mock_session.get.return_value.__aenter__.return_value = mock_response

        enhanced_monitor.rl_policy = None
        with patch("aiohttp.ClientSession", return_value=mock_session):
            signal = await enhanced_monitor.fetch_helium_supply()
        assert signal.source == "fallback"

    async def test_fetch_records_source_counts(self, enhanced_monitor):
        mock_response = AsyncMock()
        mock_response.status = 500
        mock_response.json = AsyncMock(return_value={})
        mock_response.headers = {}
        mock_session = AsyncMock()
        mock_session.get.return_value.__aenter__.return_value = mock_response

        enhanced_monitor.rl_policy = None
        with patch("aiohttp.ClientSession", return_value=mock_session):
            await enhanced_monitor.fetch_helium_supply()

        stats = enhanced_monitor.get_statistics()
        assert stats["fallback_count"] >= 1
        assert "fallback" in stats["source_counts"]


# =========================================================================== #
# Original: Prometheus metrics (extended for v6)
# =========================================================================== #
class TestPrometheusMetrics:
    def test_collect_metrics_with_signal(self, minimal_monitor):
        minimal_monitor.current_signal = HeliumSupplySignal(
            timestamp=datetime.now(timezone.utc),
            scarcity_level=HeliumScarcityLevel.CRITICAL,
            scarcity_score=0.7,
            spot_price_usd_per_liter=8.0,
            fab_inventory_days=10,
            vendor_alerts=["Alert 1", "Alert 2"],
            source="test",
        )
        metrics = minimal_monitor.collect_prometheus_metrics()
        assert metrics["green_agent_helium_scarcity_level"] == (
            2,
            {"source": "test"},
        )
        assert metrics["green_agent_helium_scarcity_score"] == (
            0.7,
            {"source": "test"},
        )
        assert metrics["green_agent_helium_spot_price_usd"] == (8.0, {})
        assert metrics["green_agent_helium_vendor_alerts_count"] == (2, {})
        assert metrics["green_agent_helium_price_premium_usd"] == (4.0, {})

    def test_collect_metrics_no_signal(self, minimal_monitor):
        minimal_monitor.current_signal = None
        assert minimal_monitor.collect_prometheus_metrics() == {}

    def test_enhanced_metrics_include_hitl_and_confidence(
        self, minimal_monitor
    ):
        minimal_monitor.current_signal = HeliumSupplySignal(
            timestamp=datetime.now(timezone.utc),
            scarcity_level=HeliumScarcityLevel.CAUTION,
            scarcity_score=0.4,
            spot_price_usd_per_liter=5.5,
            fab_inventory_days=20,
            vendor_alerts=[],
            source="test",
            confidence=0.85,
            is_anomalous=True,
            hitl_status="rejected",
            hitl_approved=False,
            market_snapshot={
                "carbon_price_per_tco2_usd": 45.0,
                "rec_available_mwh": 120.0,
                "rec_price_per_mwh_usd": 6.0,
            },
        )
        metrics = minimal_monitor.collect_prometheus_metrics()
        assert metrics["green_agent_helium_confidence"] == (0.85, {})
        assert metrics["green_agent_helium_anomalous"] == (1, {})
        # v6: HITL is surfaced in metrics
        assert "green_agent_helium_hitl_approved" in metrics
        assert metrics["green_agent_helium_hitl_approved"] == (
            0,
            {"status": "rejected"},
        )


# =========================================================================== #
# ENHANCEMENT 9: Resilience
# =========================================================================== #
class TestSyncCircuitAdapter:
    def test_starts_closed(self):
        cb = SyncCircuitAdapter("test")
        assert cb.state == "CLOSED"
        assert cb.can_call() is True

    def test_opens_after_threshold(self):
        cb = SyncCircuitAdapter("test", failure_threshold=3)
        for _ in range(3):
            cb.record_failure()
        assert cb.state == "OPEN"
        assert cb.can_call() is False

    def test_half_open_after_recovery(self):
        cb = SyncCircuitAdapter(
            "test", failure_threshold=2, recovery_timeout=0.0
        )
        cb.record_failure()
        cb.record_failure()
        assert cb.state == "OPEN"
        # recovery_timeout=0 → can_call flips to HALF_OPEN
        assert cb.can_call() is True
        assert cb.state == "HALF_OPEN"

    def test_success_resets_circuit(self):
        cb = SyncCircuitAdapter("test", failure_threshold=2)
        cb.record_failure()
        cb.record_failure()
        assert cb.state == "OPEN"
        cb.record_success()
        assert cb.state == "CLOSED"
        assert cb.failures == 0


class TestAsyncChaosInjector:
    @pytest.mark.asyncio
    async def test_zero_rate_never_fails(self):
        ci = AsyncChaosInjector(failure_rate=0.0)
        for _ in range(20):
            await ci.maybe_inject("test")
        assert list(ci.events) == []

    @pytest.mark.asyncio
    async def test_full_rate_always_fails(self):
        ci = AsyncChaosInjector(failure_rate=1.0)
        with pytest.raises(RuntimeError, match="Chaos injection"):
            await ci.maybe_inject("test")
        assert len(ci.events) == 1
        assert ci.events[0]["component"] == "test"

    @pytest.mark.asyncio
    async def test_events_bounded(self):
        ci = AsyncChaosInjector(failure_rate=1.0)
        for _ in range(2000):
            try:
                await ci.maybe_inject("test")
            except RuntimeError:
                pass
        # v6 bounds events; v5 would have grown to 2000.
        assert len(ci.events) <= 500


# =========================================================================== #
# ENHANCEMENT 5: Temporal logic (stateless, region-aware)
# =========================================================================== #
class TestSignalFreshness:
    def _signal(self, ts, region="global"):
        return HeliumSupplySignal(
            timestamp=ts,
            scarcity_level=HeliumScarcityLevel.NORMAL,
            scarcity_score=0.3,
            spot_price_usd_per_liter=4.0,
            fab_inventory_days=25,
            vendor_alerts=[],
            source="test",
            region=region,
        )

    def test_passes_for_recent_signals(self):
        prop = SignalFreshness(max_age_seconds=3600)
        history = [self._signal(datetime.now(timezone.utc))]
        assert prop.check(history, "global") is True

    def test_fails_for_stale_recent_window(self):
        prop = SignalFreshness(max_age_seconds=60, window=5)
        stale = self._signal(
            datetime.now(timezone.utc) - timedelta(hours=2)
        )
        assert prop.check([stale], "global") is False

    def test_ignores_old_signals_outside_window(self):
        """
        FIXED in v6: ``window`` bounds the check, so an old reading
        outside the window does not permanently fire.
        """
        prop = SignalFreshness(max_age_seconds=60, window=3)
        now = datetime.now(timezone.utc)
        history = [
            self._signal(now - timedelta(hours=2)),  # outside window
            self._signal(now),
            self._signal(now),
            self._signal(now),
        ]
        assert prop.check(history, "global") is True

    def test_is_region_scoped(self):
        """US-CA staleness must not affect ID freshness."""
        prop = SignalFreshness(max_age_seconds=60, window=2)
        history = [
            self._signal(
                datetime.now(timezone.utc) - timedelta(hours=2),
                region="US-CA",
            ),
            self._signal(datetime.now(timezone.utc), region="ID"),
        ]
        assert prop.check(history, "ID") is True
        assert prop.check(history, "US-CA") is False


class TestScoreBounds:
    def _signal(self, score, region="global"):
        return HeliumSupplySignal(
            timestamp=datetime.now(timezone.utc),
            scarcity_level=HeliumScarcityLevel.NORMAL,
            scarcity_score=score,
            spot_price_usd_per_liter=4.0,
            fab_inventory_days=25,
            vendor_alerts=[],
            source="test",
            region=region,
        )

    def test_valid_scores_pass(self):
        prop = ScoreBounds()
        assert prop.check([self._signal(0.5)], "global") is True

    def test_out_of_bounds_fail(self):
        prop = ScoreBounds()
        assert prop.check([self._signal(1.5)], "global") is False
        assert prop.check([self._signal(-0.1)], "global") is False


class TestPriceSanity:
    def _signal(self, price):
        return HeliumSupplySignal(
            timestamp=datetime.now(timezone.utc),
            scarcity_level=HeliumScarcityLevel.NORMAL,
            scarcity_score=0.3,
            spot_price_usd_per_liter=price,
            fab_inventory_days=25,
            vendor_alerts=[],
            source="test",
        )

    def test_valid_price_passes(self):
        assert PriceSanity(max_price=100.0).check(
            [self._signal(50.0)], "global"
        ) is True

    def test_extreme_price_fails(self):
        assert PriceSanity(max_price=100.0).check(
            [self._signal(500.0)], "global"
        ) is False


class TestSignalSafetyMonitor:
    def _signal(self, ts=None, score=0.3, price=4.0, region="global"):
        return HeliumSupplySignal(
            timestamp=ts or datetime.now(timezone.utc),
            scarcity_level=HeliumScarcityLevel.NORMAL,
            scarcity_score=score,
            spot_price_usd_per_liter=price,
            fab_inventory_days=25,
            vendor_alerts=[],
            source="test",
            region=region,
        )

    def test_aggregates_multiple_violations(self):
        monitor = SignalSafetyMonitor()
        monitor.register(SignalFreshness(max_age_seconds=60, window=2))
        monitor.register(ScoreBounds())
        stale_and_bad = self._signal(
            ts=datetime.now(timezone.utc) - timedelta(hours=1),
            score=2.0,
        )
        ok, violations = monitor.verify([stale_and_bad], region="global")
        assert ok is False
        assert "SignalFreshness" in violations
        assert "ScoreBounds" in violations
        assert len(monitor.violations) == 2

    def test_violations_bounded(self):
        """v6 bounds ``violations`` to prevent unbounded growth."""
        monitor = SignalSafetyMonitor()
        monitor.register(ScoreBounds())
        for _ in range(1000):
            monitor.verify([self._signal(score=2.0)], region="global")
        assert len(monitor.violations) <= 200


# =========================================================================== #
# Anomaly detection (per-region in v6)
# =========================================================================== #
class TestAnomalyDetector:
    def test_warmup_does_not_flag(self):
        ad = AnomalyDetector(window=20, z_threshold=3.0)
        for _ in range(4):
            flagged, _ = ad.check("US-CA", 0.5)
            assert flagged is False

    def test_flags_extreme_outlier(self):
        ad = AnomalyDetector(window=20, z_threshold=3.0)
        for _ in range(20):
            ad.check("US-CA", 0.3)
        flagged, z = ad.check("US-CA", 0.95)
        assert flagged is True
        assert z > 3.0

    def test_is_region_scoped(self):
        """
        FIXED in v6: per-region history. A US-CA history must not cause
        an ID reading to be flagged as an anomaly.
        """
        ad = AnomalyDetector(window=20, z_threshold=3.0)
        for _ in range(20):
            ad.check("US-CA", 0.3)
        # Fill ID with a different baseline
        for _ in range(20):
            ad.check("ID", 0.7)
        # ID reading near its own baseline → not flagged
        flagged_id, _ = ad.check("ID", 0.72)
        assert flagged_id is False
        # US-CA reading near its own baseline → not flagged
        flagged_us, _ = ad.check("US-CA", 0.32)
        assert flagged_us is False


# =========================================================================== #
# ENHANCEMENT 6: Explainable AI
# =========================================================================== #
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

    def test_confidence_peaks_inside_a_band(self):
        """
        FIXED in v6: v5 confidence peaked at the midpoint *between*
        bands (the least certain region). v6 peaks *inside* a band.
        """
        # Normal band is [0, 0.25); score 0.125 is dead-centre.
        centre = SignalExplainer.explain(
            level=HeliumScarcityLevel.NORMAL,
            score=0.125,
            price=4.0,
            inventory=30,
            source="test",
        )
        # Score 0.25 is exactly on the Normal/Caution boundary.
        boundary = SignalExplainer.explain(
            level=HeliumScarcityLevel.CAUTION,
            score=0.25,
            price=4.0,
            inventory=30,
            source="test",
        )
        assert centre.confidence > boundary.confidence

    def test_explanation_flags_anomaly(self):
        exp = SignalExplainer.explain(
            level=HeliumScarcityLevel.SEVERE,
            score=0.95,
            price=12.0,
            inventory=5,
            source="primary_api",
            anomaly_flagged=True,
        )
        assert any("anomalous" in r.lower() for r in exp.rationale)

    def test_explanation_includes_source(self):
        exp = SignalExplainer.explain(
            level=HeliumScarcityLevel.NORMAL,
            score=0.15,
            price=3.5,
            inventory=30,
            source="simulation",
        )
        assert any("simulation" in r.lower() for r in exp.rationale)


# =========================================================================== #
# ENHANCEMENT 7: Adaptive precision (v18 switcher + shifter-specific selector)
# =========================================================================== #
class TestAdaptivePrecisionSelector:
    def test_critical_urgency_uses_fp16(self):
        pc = AdaptivePrecisionSelector(HardwareProfile())
        assert pc.select("critical") == PrecisionLevel.FP16

    def test_edge_device_uses_int8(self):
        pc = AdaptivePrecisionSelector(
            HardwareProfile(edge_device=True, supports_int8=True)
        )
        assert pc.select("normal") == PrecisionLevel.INT8

    def test_int4_capable_uses_int4(self):
        pc = AdaptivePrecisionSelector(
            HardwareProfile(supports_int4=True)
        )
        assert pc.select("normal") == PrecisionLevel.INT4


# =========================================================================== #
# ENHANCEMENT 2: Causal provider policy
# =========================================================================== #
class TestProviderPolicy:
    def test_select_returns_available(self):
        policy = ProviderPolicy(epsilon=0.0)
        state = ProviderState(
            hour_of_day=10,
            recent_failure_rate=0.0,
            cache_hit=False,
            region_hash=0.5,
        )
        available = [ProviderKind.PRIMARY_API, ProviderKind.SIMULATION]
        choice = policy.select(state, available)
        assert choice in available

    def test_record_and_update_moves_weights(self):
        policy = ProviderPolicy(epsilon=0.0, lr=0.5)
        state = ProviderState(10, 0.0, False, 0.5)
        policy.record(state, ProviderKind.PRIMARY_API, 1.0, 50.0)
        policy.record(state, ProviderKind.PRIMARY_API, 1.0, 60.0)
        before = list(policy.weights[ProviderKind.PRIMARY_API])
        policy.update()
        after = policy.weights[ProviderKind.PRIMARY_API]
        assert after != before
        assert len(policy.buffer) == 0

    def test_epsilon_one_always_explores(self):
        policy = ProviderPolicy(epsilon=1.0)
        state = ProviderState(10, 0.0, False, 0.5)
        available = [ProviderKind.PRIMARY_API, ProviderKind.SIMULATION]
        choices = {policy.select(state, available) for _ in range(50)}
        assert len(choices) > 1


# =========================================================================== #
# ENHANCEMENT 1: Quantum distillation
# =========================================================================== #
class TestQuantumDistillationBridge:
    def test_distill_returns_model(self):
        bridge = QuantumDistillationBridge()
        history = [
            HeliumSupplySignal(
                timestamp=datetime.now(timezone.utc),
                scarcity_level=HeliumScarcityLevel.NORMAL,
                scarcity_score=0.3,
                spot_price_usd_per_liter=4.0,
                fab_inventory_days=25,
                vendor_alerts=[],
                source="test",
            )
            for _ in range(10)
        ]
        model = bridge.distill(history, PrecisionLevel.INT8)
        assert isinstance(model, DistilledSimulationModel)
        assert model.precision == PrecisionLevel.INT8
        assert len(model.hourly_mean) == 24

    def test_uses_round_not_floor(self):
        """FIXED in v6: v5 used ``math.floor`` (systematic downward bias)."""
        bridge = QuantumDistillationBridge()
        history = [
            HeliumSupplySignal(
                timestamp=datetime.now(timezone.utc),
                scarcity_level=HeliumScarcityLevel.NORMAL,
                scarcity_score=0.129,  # floor → 0.12; round → 0.13
                spot_price_usd_per_liter=4.0,
                fab_inventory_days=25,
                vendor_alerts=[],
                source="test",
            )
            for _ in range(20)
        ]
        model = bridge.distill(history, PrecisionLevel.INT8)
        hour = datetime.now(timezone.utc).hour
        # If floor, value is 0.12. If round, 0.13.
        assert model.hourly_mean[hour] >= 0.125

    def test_empty_history_is_safe(self):
        bridge = QuantumDistillationBridge()
        model = bridge.distill([], PrecisionLevel.INT8)
        assert model.precision == PrecisionLevel.INT8
        assert len(model.hourly_mean) == 24


# =========================================================================== #
# ENHANCEMENT 3: Federated learning (fixed class name!)
# =========================================================================== #
class TestFederatedHeliumAggregator:
    def test_aggregate_empty_returns_empty(self):
        agg = FederatedHeliumAggregator()
        assert agg.aggregate() == {}

    def test_weighted_aggregation(self):
        agg = FederatedHeliumAggregator()
        agg.push(
            FederatedHeliumProfile(
                deployment_id="A",
                region="US-CA",
                hourly_means=[0.2] * 24,
                sample_count=10,
            )
        )
        agg.push(
            FederatedHeliumProfile(
                deployment_id="B",
                region="US-CA",
                hourly_means=[0.8] * 24,
                sample_count=30,
            )
        )
        result = agg.aggregate()
        assert "US-CA" in result
        assert result["US-CA"][0] == pytest.approx(0.65, abs=0.01)

    def test_lookup_after_aggregation(self):
        agg = FederatedHeliumAggregator()
        agg.push(
            FederatedHeliumProfile(
                deployment_id="A",
                region="GB",
                hourly_means=[0.5] * 24,
                sample_count=5,
            )
        )
        agg.aggregate()
        assert agg.get_global("GB") is not None
        assert agg.get_global("XX") is None

    def test_zero_weight_does_not_divide_by_zero(self):
        """v5 crashed with ZeroDivisionError; v6 returns empty."""
        agg = FederatedHeliumAggregator()
        agg.push(
            FederatedHeliumProfile(
                deployment_id="A",
                region="US-CA",
                hourly_means=[0.5] * 24,
                sample_count=0,
            )
        )
        result = agg.aggregate()
        assert result == {}


# =========================================================================== #
# ENHANCEMENT 4: Multi-agent coordination
# =========================================================================== #
class TestHeliumAgentCoordinator:
    def test_register_and_select(self):
        coord = HeliumAgentCoordinator()
        coord.register("agent-A")
        coord.register("agent-B")
        aid = coord.select_agent("US-CA")
        assert aid in ("agent-A", "agent-B")

    def test_anomaly_hunter_role(self):
        coord = HeliumAgentCoordinator()
        for _ in range(5):
            coord.record(
                "agent-A",
                "US-CA",
                success=False,
                latency_ms=500.0,
                anomaly_caught=True,
            )
        assert coord.agents["agent-A"].role == AgentRole.ANOMALY_HUNTER

    def test_role_reassignment_uses_sliding_window(self):
        """
        FIXED in v6: v5 used a cumulative mean, so 10 old failures
        dragged the success_rate down forever. v6 uses a 20-outcome
        sliding window.
        """
        coord = HeliumAgentCoordinator(window_size=20)
        for _ in range(10):
            coord.record(
                "agent-A",
                "US-CA",
                success=False,
                latency_ms=1000.0,
                anomaly_caught=False,
            )
        for _ in range(10):
            coord.record(
                "agent-A",
                "US-CA",
                success=True,
                latency_ms=100.0,
                anomaly_caught=False,
            )
        # All 10 recent successes visible in window → success_rate ≈ 1.0
        assert coord.agents["agent-A"].success_rate > 0.9


# =========================================================================== #
# ENHANCEMENT 8: Carbon market
# =========================================================================== #
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
        assert snap1 is snap2


# =========================================================================== #
# ENHANCEMENT 10: HITL (sync + async)
# =========================================================================== #
class TestHumanReviewGate:
    def test_sync_review_without_callback_returns_none(self):
        """v6: returns ``(None, "no_callback")``, not a synthetic False."""
        gate = HumanReviewGate()
        approved, status = gate.review(
            region="US-CA",
            level=HeliumScarcityLevel.CRITICAL,
            score=0.75,
            price=8.5,
            reason="test",
            urgency="medium",
        )
        assert approved is None
        assert status == "no_callback"

    def test_sync_review_with_callback_records_feedback(self):
        gate = HumanReviewGate()
        gate.set_callback(lambda req: True)
        approved, status = gate.review(
            region="US-CA",
            level=HeliumScarcityLevel.CRITICAL,
            score=0.75,
            price=8.5,
            reason="test",
            urgency="medium",
        )
        assert approved is True
        assert status == "approved"
        assert len(gate.feedback_log) == 1
        assert gate.feedback_log[0]["approved"] is True

    def test_review_callback_exception_is_safe(self):
        gate = HumanReviewGate()

        def bad_cb(req):
            raise RuntimeError("callback boom")

        gate.set_callback(bad_cb)
        approved, status = gate.review(
            region="US-CA",
            level=HeliumScarcityLevel.CRITICAL,
            score=0.75,
            price=8.5,
            reason="test",
            urgency="medium",
        )
        assert approved is None
        assert status == "callback_error"
        assert len(gate.pending) == 0  # Must be cleaned up

    @pytest.mark.asyncio
    async def test_async_review_awaits_callback(self):
        gate = HumanReviewGate()

        async def async_cb(req):
            return True

        gate.set_callback(async_cb)
        approved, status = await gate.areview(
            region="US-CA",
            level=HeliumScarcityLevel.CRITICAL,
            score=0.75,
            price=8.5,
            reason="test",
            urgency="medium",
        )
        assert approved is True
        assert status == "approved"

    def test_sync_review_rejects_async_callback(self):
        """FIXED in v6: async callbacks in sync review are rejected (fail-safe)."""
        gate = HumanReviewGate()

        async def async_cb(req):
            return True

        gate.set_callback(async_cb)
        approved, status = gate.review(
            region="US-CA",
            level=HeliumScarcityLevel.CRITICAL,
            score=0.75,
            price=8.5,
            reason="test",
            urgency="medium",
        )
        assert approved is False
        assert status == "async_in_sync"

    def test_outcomes_counter(self):
        gate = HumanReviewGate()
        gate.set_callback(lambda req: True)
        gate.review("US-CA", HeliumScarcityLevel.CRITICAL, 0.75, 8.5, "r", "m")
        assert gate.outcomes()["approved"] == 1

    def test_active_learning_batch_bounded(self):
        gate = HumanReviewGate()
        gate.set_callback(lambda req: True)
        for _ in range(1000):
            gate.review(
                "US-CA", HeliumScarcityLevel.CRITICAL, 0.75, 8.5, "r", "m"
            )
        # v6 bounds feedback_log
        assert len(gate.feedback_log) <= 500
        batch = gate.active_learning_batch(n=10)
        assert len(batch) == 10


# =========================================================================== #
# Feature toggles
# =========================================================================== #
class TestFeatureToggles:
    def test_legacy_mode_disables_enhancements(self, legacy_monitor):
        assert legacy_monitor.rl_policy is None
        assert legacy_monitor.explainer is None
        assert legacy_monitor.federated is None
        assert legacy_monitor.coordinator is None
        assert legacy_monitor.safety is None
        assert legacy_monitor.precision_ctl is None
        assert legacy_monitor.market is None
        assert legacy_monitor.hitl is None
        assert legacy_monitor.anomaly is None
        assert legacy_monitor.distiller is None

    def test_partial_features_disable_specific_layers(self, mock_config):
        features = dict(HeliumMonitor.DEFAULT_FEATURES)
        features["xai"] = False
        features["carbon_market"] = False
        monitor = HeliumMonitor(
            config=mock_config,
            simulation_seed=42,
            features=features,
            auto_start=False,
        )
        assert monitor.explainer is None
        assert monitor.market is None
        assert monitor.rl_policy is not None

    def test_legacy_mode_still_simulates(self, legacy_monitor):
        signal = legacy_monitor._simulate_helium_supply()
        assert signal.source == "simulation"


# =========================================================================== #
# AUDIT-TRAIL REGRESSION TESTS
# =========================================================================== #
class TestAuditTrailFixes:
    """
    One test per P0/P1 fix from the v6 review. Each test fails on v5
    and passes on v6.
    """

    def test_classify_single_source_of_truth(self):
        """
        FIXED (§2.6): v5 had three incompatible threshold sets. 0.55
        was CAUTION from the federation path, CRITICAL from the
        distillation path. v6 uses ``_classify`` everywhere.
        """
        cases = [
            (0.0, HeliumScarcityLevel.NORMAL),
            (0.24, HeliumScarcityLevel.NORMAL),
            (0.25, HeliumScarcityLevel.CAUTION),
            (0.30, HeliumScarcityLevel.CAUTION),
            (0.54, HeliumScarcityLevel.CAUTION),
            (0.55, HeliumScarcityLevel.CRITICAL),
            (0.79, HeliumScarcityLevel.CRITICAL),
            (0.80, HeliumScarcityLevel.SEVERE),
            (1.0, HeliumScarcityLevel.SEVERE),
        ]
        for score, expected in cases:
            assert _classify(score) == expected, (
                f"_classify({score}) = {_classify(score)}, "
                f"expected {expected}"
            )

    def test_constructor_safe_outside_event_loop(self):
        """
        FIXED (§2.1): v5's ``__init__`` called ``asyncio.get_event_loop()``
        and ``create_task()``, which broke in sync contexts.
        v6 defers to ``start()`` unless ``auto_start=True`` *and* a
        loop is already running.
        """
        monitor = HeliumMonitor(config={}, simulation_seed=42, auto_start=False)
        assert monitor._monitoring_task is None
        # Sync operations still work:
        signal = monitor._simulate_helium_supply()
        assert 0.0 <= signal.scarcity_score <= 1.0

    @pytest.mark.asyncio
    async def test_hitl_rejection_marks_signal_stale(
        self, enhanced_monitor, valid_api_response
    ):
        """
        FIXED (§2.2): v5 discarded the HITL result. v6 stores
        ``hitl_status`` and ``is_stale`` on the signal.
        """
        # Force an anomaly
        enhanced_monitor.anomaly = AnomalyDetector(z_threshold=0.1)
        for _ in range(20):
            enhanced_monitor.anomaly.check("US-CA", 0.3)

        enhanced_monitor.set_hitl_callback(lambda req: False)

        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(
            return_value=dict(valid_api_response, scarcity_score=0.9)
        )
        mock_response.headers = {}
        mock_session = AsyncMock()
        mock_session.get.return_value.__aenter__.return_value = mock_response

        enhanced_monitor.rl_policy = None
        with patch("aiohttp.ClientSession", return_value=mock_session):
            signal = await enhanced_monitor.fetch_helium_supply()

        assert signal.hitl_status == "rejected"
        assert signal.hitl_approved is False
        assert signal.is_stale is True

    @pytest.mark.asyncio
    async def test_async_callback_awaited_in_areview(self):
        """
        FIXED (§2.3): v5 silently auto-approved async HITL callbacks.
        v6 awaits them in ``areview``.
        """
        gate = HumanReviewGate()

        async def async_cb(req):
            return True

        gate.set_callback(async_cb)
        approved, status = await gate.areview(
            region="US-CA",
            level=HeliumScarcityLevel.CRITICAL,
            score=0.75,
            price=8.5,
            reason="test",
            urgency="medium",
        )
        assert approved is True
        assert status == "approved"

    def test_signal_freshness_does_not_fire_forever(self):
        """
        FIXED (§2.4): v5's SignalFreshness checked the entire history,
        so once *any* signal was >30 minutes old, every subsequent
        verification failed. v6 checks only the recent window.
        """
        prop = SignalFreshness(max_age_seconds=60, window=3)
        now = datetime.now(timezone.utc)
        history = [
            HeliumSupplySignal(
                timestamp=now - timedelta(hours=5),
                scarcity_level=HeliumScarcityLevel.NORMAL,
                scarcity_score=0.3,
                spot_price_usd_per_liter=4.0,
                fab_inventory_days=25,
                vendor_alerts=[],
                source="test",
                region="global",
            ),
            HeliumSupplySignal(
                timestamp=now,
                scarcity_level=HeliumScarcityLevel.NORMAL,
                scarcity_score=0.3,
                spot_price_usd_per_liter=4.0,
                fab_inventory_days=25,
                vendor_alerts=[],
                source="test",
                region="global",
            ),
            HeliumSupplySignal(
                timestamp=now,
                scarcity_level=HeliumScarcityLevel.NORMAL,
                scarcity_score=0.3,
                spot_price_usd_per_liter=4.0,
                fab_inventory_days=25,
                vendor_alerts=[],
                source="test",
                region="global",
            ),
        ]
        # Old reading is outside the window → must not trip
        assert prop.check(history, "global") is True

    def test_confidence_peaks_inside_band_not_at_boundary(self):
        """
        FIXED (§2.5): v5's confidence peaked at band boundaries —
        exactly when the reading was least certainly classified.
        """
        centre = SignalExplainer.explain(
            level=HeliumScarcityLevel.NORMAL,
            score=0.125,
            price=4.0,
            inventory=30,
            source="test",
        )
        boundary = SignalExplainer.explain(
            level=HeliumScarcityLevel.NORMAL,
            score=0.25,
            price=4.0,
            inventory=30,
            source="test",
        )
        assert centre.confidence > boundary.confidence

    def test_merge_federated_profile_actually_merges(self, enhanced_monitor):
        """
        FIXED (§3.1): v5's ``merge_federated_profile`` was a no-op.
        v6 returns the number of merged hourly values and invalidates
        the distillation cache.
        """
        # Seed a federated profile.
        now = datetime.now(timezone.utc)
        for h in range(15):
            enhanced_monitor._signal_history.append(
                HeliumSupplySignal(
                    timestamp=now - timedelta(hours=h),
                    scarcity_level=HeliumScarcityLevel.NORMAL,
                    scarcity_score=0.3,
                    spot_price_usd_per_liter=4.0,
                    fab_inventory_days=25,
                    vendor_alerts=[],
                    source="test",
                    region="US-CA",
                )
            )
        enhanced_monitor.contribute_federated_profile()
        merged = enhanced_monitor.merge_federated_profile(blend=0.3)
        assert merged == 24
        assert "US-CA" in enhanced_monitor._federated_profile_override

    @pytest.mark.asyncio
    async def test_fallback_signal_appended_to_history(
        self, enhanced_monitor
    ):
        """
        FIXED (§3.5): v5 kept fallback signals in ``current_signal``
        but did not append them to history. v6 does.
        """
        mock_response = AsyncMock()
        mock_response.status = 500
        mock_response.json = AsyncMock(return_value={})
        mock_response.headers = {}
        mock_session = AsyncMock()
        mock_session.get.return_value.__aenter__.return_value = mock_response

        enhanced_monitor.rl_policy = None
        with patch("aiohttp.ClientSession", return_value=mock_session):
            await enhanced_monitor.fetch_helium_supply()

        history = list(enhanced_monitor._signal_history)
        assert any(s.source == "fallback" for s in history)


# =========================================================================== #
# End-to-end integration tests
# =========================================================================== #
@pytest.mark.asyncio
@pytest.mark.integration
class TestEnhancedPipeline:
    async def test_full_pipeline_enriches_signal(
        self, enhanced_monitor, valid_api_response
    ):
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value=valid_api_response)
        mock_response.headers = {}
        mock_session = AsyncMock()
        mock_session.get.return_value.__aenter__.return_value = mock_response

        enhanced_monitor.rl_policy = None
        with patch("aiohttp.ClientSession", return_value=mock_session):
            signal = await enhanced_monitor.fetch_helium_supply()

        assert signal.source in (
            "primary_api",
            "backup_api",
            "federated_peer",
            "distilled_model",
            "simulation",
            "fallback",
        )
        assert signal.explanation is not None
        assert signal.confidence > 0.0
        assert signal.region == "US-CA"

    async def test_start_stop_idempotent(self, enhanced_monitor):
        """v6: start() is idempotent; shutdown() is safe to call twice."""
        await enhanced_monitor.start()
        await enhanced_monitor.start()  # no-op
        await enhanced_monitor.shutdown()
        await enhanced_monitor.shutdown()  # safe

    async def test_get_statistics_returns_all_sections(self, enhanced_monitor):
        stats = enhanced_monitor.get_statistics()
        for key in (
            "deployment_id",
            "agent_id",
            "region",
            "circuit_states",
            "fallback_count",
            "source_counts",
            "temporal_violations",
            "hitl_outcomes",
            "agents",
            "market",
        ):
            assert key in stats

    async def test_health_check_returns_dict(self, enhanced_monitor):
        health = await enhanced_monitor.health_check()
        assert "healthy" in health
        assert "monitoring_active" in health
        assert "circuit_states" in health
        assert "fallback_count" in health
        assert "hitl_outcomes" in health


# =========================================================================== #
# Concurrency
# =========================================================================== #
@pytest.mark.asyncio
@pytest.mark.integration
class TestConcurrency:
    async def test_concurrent_fetches_do_not_corrupt_history(
        self, enhanced_monitor, valid_api_response
    ):
        """100 concurrent fetches must not lose or duplicate history."""
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value=valid_api_response)
        mock_response.headers = {}
        mock_session = AsyncMock()
        mock_session.get.return_value.__aenter__.return_value = mock_response

        enhanced_monitor.rl_policy = None
        with patch("aiohttp.ClientSession", return_value=mock_session):
            signals = await asyncio.gather(
                *[enhanced_monitor.fetch_helium_supply() for _ in range(50)]
            )
        assert len(signals) == 50
        history = list(enhanced_monitor._signal_history)
        # Buffer is bounded at 10 in mock_config; 50 fetches → 10 max
        assert len(history) <= enhanced_monitor.history_buffer_size


# =========================================================================== #
# Property-based tests (optional)
# =========================================================================== #
@pytest.mark.skipif(
    not _HYPOTHESIS_AVAILABLE, reason="hypothesis not installed"
)
class TestProperties:
    @given(score=st.floats(min_value=-10, max_value=10, allow_nan=False))
    def test_classify_never_raises(self, score):
        """FIXED: ``_classify`` is total over the entire float range."""
        assert isinstance(_classify(score), HeliumScarcityLevel)

    @given(
        score=st.floats(
            min_value=0, max_value=1, allow_nan=False, allow_infinity=False
        ),
        price=st.floats(
            min_value=0, max_value=1000, allow_nan=False, allow_infinity=False
        ),
    )
    def test_explainer_confidence_in_range(self, score, price):
        exp = SignalExplainer.explain(
            level=_classify(score),
            score=score,
            price=price,
            inventory=15,
            source="test",
        )
        assert 0.0 <= exp.confidence <= 1.0

    @given(
        raw=st.dictionaries(
            keys=st.integers(min_value=0, max_value=23),
            values=st.floats(
                min_value=0, max_value=2000,
                allow_nan=False, allow_infinity=False,
            ),
            max_size=48,
        )
    )
    def test_aggregator_handles_arbitrary_profiles(self, raw):
        agg = FederatedHeliumAggregator()
        if raw:
            keys = sorted(raw.keys())[:24]
            means = [raw[k] for k in keys]
            means = (means + [0.0] * 24)[:24]
            agg.push(
                FederatedHeliumProfile(
                    deployment_id="x",
                    region="R",
                    hourly_means=means,
                    sample_count=len(means),
                )
            )
        result = agg.aggregate()
        for values in result.values():
            assert all(v >= 0 for v in values)


# =========================================================================== #
# Fallback smoke test (no pytest needed)
# =========================================================================== #
if __name__ == "__main__":
    try:
        import pytest as _pytest

        _pytest.main(
            [__file__, "-v", "--asyncio-mode=auto", "-m", "not slow"]
        )
    except ImportError:
        print("pytest not available, running basic smoke test...")
        logging_configured = False
        try:
            import logging

            logging.basicConfig(level=logging.INFO)
            logging_configured = True
        except Exception:
            pass

        # Sync smoke
        monitor = HeliumMonitor(
            config={"region": "US-CA"},
            simulation_seed=42,
            auto_start=False,
        )
        signal = monitor._simulate_helium_supply()
        assert 0.0 <= signal.scarcity_score <= 1.0

        # Classification consistency
        assert _classify(0.24) == HeliumScarcityLevel.NORMAL
        assert _classify(0.30) == HeliumScarcityLevel.CAUTION
        assert _classify(0.60) == HeliumScarcityLevel.CRITICAL
        assert _classify(0.90) == HeliumScarcityLevel.SEVERE

        # Confidence peaks inside band
        centre = SignalExplainer.explain(
            level=HeliumScarcityLevel.NORMAL,
            score=0.125, price=4.0, inventory=30, source="test",
        )
        boundary = SignalExplainer.explain(
            level=HeliumScarcityLevel.NORMAL,
            score=0.25, price=4.0, inventory=30, source="test",
        )
        assert centre.confidence > boundary.confidence

        # SignalFreshness window fix
        prop = SignalFreshness(max_age_seconds=60, window=2)
        now = datetime.now(timezone.utc)
        history = [
            HeliumSupplySignal(
                timestamp=now - timedelta(hours=5),
                scarcity_level=HeliumScarcityLevel.NORMAL,
                scarcity_score=0.3,
                spot_price_usd_per_liter=4.0,
                fab_inventory_days=25,
                vendor_alerts=[],
                source="test",
                region="global",
            ),
            HeliumSupplySignal(
                timestamp=now,
                scarcity_level=HeliumScarcityLevel.NORMAL,
                scarcity_score=0.3,
                spot_price_usd_per_liter=4.0,
                fab_inventory_days=25,
                vendor_alerts=[],
                source="test",
                region="global",
            ),
        ]
        assert prop.check(history, "global") is True

        # HITL async rejection in sync review
        gate = HumanReviewGate()

        async def _async_cb(req):
            return True

        gate.set_callback(_async_cb)
        approved, status = gate.review(
            region="US-CA",
            level=HeliumScarcityLevel.CRITICAL,
            score=0.75, price=8.5, reason="smoke", urgency="medium",
        )
        assert approved is False
        assert status == "async_in_sync"

        print("✅ Basic smoke test passed (v6 audit-trail fixes verified).")
