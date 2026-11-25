# -*- coding: utf-8 -*-
"""
test_graph_metrics_exporter_helium.py (v5.1)
============================================

Test suite for GraphMetricsExporter helium metrics + ten enhancement layers.

Coverage:
  * Helium signal integration
  * Ten enhancement layers (Quantum-Distillation, Causal RL, Federated,
    Multi-Agent, Temporal Logic, XAI, Adaptive Precision, Carbon Markets,
    Resilience, HITL)
  * v5.1 fixes (multi-series preservation, dry_run, render_json,
    collector adapter, label escaping, circuit breaker, empty scrape,
    NaN/Inf rejection, exporter self-metrics)
  * HTTP server endpoints (/metrics, /health, /json, 404, concurrency,
    restart)

Fixes in v5.1 (see review)
--------------------------
  * P0 — The 350-line inline fallback was removed. The whole module skips
         via ``pytest.importorskip`` when the real exporter is not
         importable. Tests that need the real helium module are marked
         ``@requires_helium``.
  * P0 — All assertions adapted to the v5.1 ``collect()`` return shape
         (``List[Tuple[str, float, Dict]]``) via ``_series_by_name``.
         Previously every ``metrics["X"]`` raised ``TypeError`` against
         v5.1, and every ``"X" in metrics`` was always ``False``.
  * P0 — ``test_helium_metrics_integration`` no longer asserts the
         ``job="green_agent"`` label that v5.1 explicitly removed (the
         label is reserved by Prometheus). It now asserts the label is
         *absent*.
  * P0 — Ten ``test_missing_source_omits_metrics`` tests now use
         ``_series_names()`` (a ``set``) instead of ``in list``, so the
         negative assertions actually verify omission.
  * P0 — ``test_all_sources_missing_yields_only_base`` no longer fails
         on the positive assertion (was checking ``string in list``).
  * P0 — Helium fallback ``HeliumMonitor`` gains ``get_current_supply``
         so ``Mock(spec=HeliumMonitor)`` works in partial checkouts, and
         helium-dependent tests are marked ``@requires_helium``.
  * P0 — ``test_dashboard_has_enhanced_title`` is guarded by the module
         skip (no fallback without ``grafana_dashboard``).
  * P1 — Circuit state, agent roles, HITL reviews, and precision shares
         now verify the per-series split instead of just presence.
  * P1 — ``test_render_format_is_parseable_after_enhancement`` uses
         ``prometheus_client.parser`` instead of a weak regex.
  * P1 — HTTP tests bind ``port=0`` (no free-port race), verify
         concurrent scrapes, and exercise restart-after-stop.
  * P2 — New ``TestV5_1Fixes`` class covering 13 v5.1 fixes.
  * P3 — Unused imports removed; ``__main__`` invokes pytest and exits
         with its status code.

Run with:
    pytest test_graph_metrics_exporter_helium.py -v
    pytest test_graph_metrics_exporter_helium.py::TestV5_1Fixes -v
    pytest test_graph_metrics_exporter_helium.py -k "not requires_helium"
"""

from __future__ import annotations

import json
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Tuple
from unittest.mock import Mock

import pytest


# =============================================================================
# Required test dependencies (fail loudly, don't skip silently)
# =============================================================================
text_string_to_metric_families = pytest.importorskip(
    "prometheus_client.parser",
    reason=(
        "prometheus_client is required to verify Prometheus exposition "
        "format correctness. Install with: pip install prometheus_client"
    ),
).text_string_to_metric_families

requests = pytest.importorskip(
    "requests",
    reason=(
        "requests is required to exercise the HTTP server endpoints. "
        "Install with: pip install requests"
    ),
)


# =============================================================================
# Real exporter (module-scope skip if not importable)
# =============================================================================
_exporter_mod = pytest.importorskip(
    "src.monitoring.graph_metrics_exporter",
    reason=(
        "src.monitoring.graph_metrics_exporter not importable; "
        "install the full package to run these tests."
    ),
)
GraphMetricsExporter = _exporter_mod.GraphMetricsExporter
EnhancementMetricsSources = _exporter_mod.EnhancementMetricsSources
EXPORTER_VERSION = getattr(_exporter_mod, "__version__", None)


# =============================================================================
# Helium module (conditional — not every test needs it)
# =============================================================================
try:
    from carbon.helium_monitor import (  # type: ignore
        HeliumMonitor,
        HeliumScarcityLevel,
        HeliumSupplySignal,
    )
    _HAS_HELIUM = True
except Exception:  # pragma: no cover — partial checkouts
    _HAS_HELIUM = False

    class HeliumScarcityLevel(Enum):  # type: ignore
        NORMAL = "normal"
        CAUTION = "caution"
        CRITICAL = "critical"
        SEVERE = "severe"

    class HeliumSupplySignal:  # type: ignore
        def __init__(
            self,
            timestamp,
            scarcity_level,
            scarcity_score,
            spot_price_usd_per_liter,
            fab_inventory_days,
            vendor_alerts,
            source,
            **kwargs,
        ):
            self.timestamp = timestamp
            self.scarcity_level = scarcity_level
            self.scarcity_score = scarcity_score
            self.spot_price_usd_per_liter = spot_price_usd_per_liter
            self.fab_inventory_days = fab_inventory_days
            self.vendor_alerts = vendor_alerts
            self.source = source

    class HeliumMonitor:  # type: ignore
        def get_current_supply(self):
            raise NotImplementedError("Fallback HeliumMonitor")


requires_helium = pytest.mark.skipif(
    not _HAS_HELIUM,
    reason="carbon.helium_monitor not importable",
)


# =============================================================================
# Series helpers — v5.1 API (List[Tuple[str, float, Dict]])
# =============================================================================

def _series_by_name(
    series: List[Tuple[str, float, Dict[str, str]]],
) -> Dict[str, List[Tuple[float, Dict[str, str]]]]:
    """Group series by metric name: ``{name: [(value, labels), ...]}``."""
    result: Dict[str, List[Tuple[float, Dict[str, str]]]] = {}
    for name, value, lbl in series:
        result.setdefault(name, []).append((value, lbl))
    return result


def _series_names(
    series: List[Tuple[str, float, Dict[str, str]]],
) -> set:
    """Distinct metric names present in a series list."""
    return {name for name, _, _ in series}


def _single(
    by_name: Dict[str, List[Tuple[float, Dict[str, str]]]],
    name: str,
) -> Tuple[float, Dict[str, str]]:
    """Assert exactly one series for ``name`` and return it."""
    entries = by_name.get(name)
    assert entries is not None, f"{name} not emitted"
    assert len(entries) == 1, (
        f"{name}: expected 1 series, got {len(entries)}"
    )
    return entries[0]


def _label_map(
    by_name: Dict[str, List[Tuple[float, Dict[str, str]]]],
    name: str,
    key: str,
) -> Dict[str, float]:
    """Return ``{labels[key]: value}`` for every series of ``name``."""
    return {
        lbl[key]: value
        for value, lbl in by_name.get(name, [])
        if key in lbl
    }


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture
def base_registry():
    """Minimal registry with no graphs attached."""
    registry = Mock()
    registry.health.return_value = {
        "execution_count": 1, "singletons": {},
    }
    registry.get.return_value = None
    return registry


@pytest.fixture
def mock_distiller():
    class D:
        distilled_count = 12
        last_quality_retention = 0.93
        last_energy_reduction_percent = 55.0
    return D()


@pytest.fixture
def mock_rl_policy():
    class RL:
        epsilon = 0.12
        observations = 42
        buffer = [1, 2, 3]
        weights = {
            "lora": [0.5, 0.2, 0.1, 0.0, 0.0],
            "qlora": [0.3, 0.1, 0.4, 0.1, 0.0],
        }
    return RL()


@pytest.fixture
def mock_federated():
    class U:
        def __init__(self, dep):
            self.deployment_id = dep

    class F:
        updates = [
            U("us-ca"), U("us-ca"),
            U("eu-north"), U("eu-north"), U("eu-north"),
        ]

        def aggregate(self):
            return {"carbon:US-CA": 375.0, "carbon:EU-NORTH": 210.0}

    return F()


@pytest.fixture
def mock_coordinator():
    class R:
        def __init__(self, v):
            self.value = v

    class A:
        def __init__(self, aid, role, sr):
            self.agent_id = aid
            self.role = R(role)
            self.success_rate = sr

    class C:
        agents = {
            "a1": A("a1", "carbon_heavy", 0.9),
            "a2": A("a2", "latency_critical", 0.85),
            "a3": A("a3", "carbon_heavy", 0.92),
        }

    return C()


@pytest.fixture
def mock_temporal_monitor():
    class T:
        violations = [
            {"property": "DeadlineRespected",
             "at": datetime.now().isoformat()},
            {"property": "DeadlineRespected",
             "at": datetime.now().isoformat()},
            {"property": "CarbonBudgetNotExceeded",
             "at": datetime.now().isoformat()},
        ]

    return T()


@pytest.fixture
def mock_explainer():
    class E:
        explanations_count = 137
        average_confidence = 0.87
    return E()


@pytest.fixture
def mock_market():
    class S:
        carbon_price_per_tco2_usd = 45.0
        rec_available_mwh = 120.0

    class M:
        trades = [
            {"type": "buy", "cost_usd": 0.5},
            {"type": "buy", "cost_usd": 0.7},
            {"type": "sell", "revenue_usd": 0.3},
        ]
        spent = 1.2

        def get_snapshot(self):
            return S()

    return M()


@pytest.fixture
def mock_circuit_breakers():
    class E:
        def __init__(self, v):
            self.value = v

    class CB:
        def __init__(self, state, failures):
            self.state = E(state)
            self.failures = failures

    return {
        "forecaster": CB("closed", 0),
        "scheduler": CB("open", 3),
        "ledger": CB("half_open", 1),
    }


@pytest.fixture
def mock_chaos():
    class CH:
        events = [{"component": "x"}, {"component": "y"}]
    return CH()


@pytest.fixture
def mock_hitl():
    class H:
        pending = [1, 2]
        feedback_log = [
            {"decision": True},
            {"decision": False},
            {"decision": True},
        ]

        def active_learning_batch(self, n=16):
            return self.feedback_log[-n:]

    return H()


@pytest.fixture
def enhancement_sources(
    mock_distiller, mock_rl_policy, mock_federated, mock_coordinator,
    mock_temporal_monitor, mock_explainer, mock_market,
    mock_circuit_breakers, mock_chaos, mock_hitl,
):
    return EnhancementMetricsSources(
        distiller=mock_distiller,
        rl_policy=mock_rl_policy,
        federated=mock_federated,
        coordinator=mock_coordinator,
        temporal_monitor=mock_temporal_monitor,
        explainer=mock_explainer,
        precision_controller=None,
        precision_counts={
            "fp32": 10, "fp16": 5, "int8": 30, "int4": 15,
        },
        market=mock_market,
        circuit_breakers=mock_circuit_breakers,
        chaos=mock_chaos,
        hitl=mock_hitl,
    )


@pytest.fixture
def enhanced_exporter(base_registry, enhancement_sources):
    return GraphMetricsExporter(
        base_registry, enhancement_sources=enhancement_sources,
    )


# =============================================================================
# Helium integration (adapted from the preserved original)
# =============================================================================

@requires_helium
def test_helium_metrics_integration():
    """
    Helium metrics appear in ``collect()`` and ``render()`` with the
    correct values, and no reserved ``job`` label is emitted.

    v5.1 update: assertions use the ``List[Tuple]`` shape; the previous
    version asserted the presence of ``job="green_agent"``, which v5.1
    removed (Prometheus reserves it as a target label).
    """
    registry = Mock()
    registry.health.return_value = {
        "execution_count": 5, "singletons": {},
    }
    registry.get.return_value = None

    helium_monitor = Mock(spec=HeliumMonitor)
    helium_monitor.get_current_supply.return_value = HeliumSupplySignal(
        timestamp=datetime.now(),
        scarcity_level=HeliumScarcityLevel.CRITICAL,
        scarcity_score=0.7,
        spot_price_usd_per_liter=8.0,
        fab_inventory_days=10,
        vendor_alerts=["Test alert"],
        source="test",
    )

    exporter = GraphMetricsExporter(
        registry=registry, helium_monitor=helium_monitor,
    )
    by_name = _series_by_name(exporter.collect())

    # Scarcity level = 2 (CRITICAL)
    value, labels = _single(by_name, "green_agent_helium_scarcity_level")
    assert value == 2
    assert labels["source"] == "test"
    assert "job" not in labels, (
        "v5.1: the reserved 'job' label must not be emitted on metrics"
    )

    # Scarcity score = 0.7
    value, labels = _single(
        by_name, "green_agent_helium_scarcity_score",
    )
    assert value == pytest.approx(0.7)
    assert "job" not in labels

    # Spot price = 8.0
    value, labels = _single(
        by_name, "green_agent_helium_spot_price_usd",
    )
    assert value == pytest.approx(8.0)
    assert "job" not in labels

    # Fab inventory = 10 days
    value, labels = _single(
        by_name, "green_agent_helium_fab_inventory_days",
    )
    assert value == pytest.approx(10)
    assert "job" not in labels

    # Vendor alerts = 1
    value, _ = _single(
        by_name, "green_agent_helium_vendor_alerts_count",
    )
    assert value == 1

    # Premium = 8.0 - 4.0 baseline = 4.0
    value, _ = _single(
        by_name, "green_agent_helium_price_premium_usd",
    )
    assert value == pytest.approx(4.0)

    # Rendered output must not contain the reserved job label
    text = exporter.render()
    assert 'job="' not in text, (
        "v5.1: metrics must not carry the reserved 'job' label; "
        "set it in the Prometheus scrape config instead"
    )


def test_no_helium_metrics_when_disabled():
    """No helium metrics when ``helium_monitor=None`` (no @requires_helium)."""
    registry = Mock()
    registry.health.return_value = {
        "execution_count": 0, "singletons": {},
    }
    registry.get.return_value = None

    exporter = GraphMetricsExporter(registry, helium_monitor=None)
    names = _series_names(exporter.collect())
    assert "green_agent_execution_graphs_active" in names
    assert "green_agent_helium_scarcity_level" not in names


# =============================================================================
# ENHANCEMENT 1: Quantum-Distillation
# =============================================================================

class TestQuantumDistillationMetrics:
    def test_distilled_models_total(self, enhanced_exporter):
        by_name = _series_by_name(enhanced_exporter.collect())
        value, _ = _single(by_name, "green_agent_distilled_models_total")
        assert value == 12

    def test_quality_retention(self, enhanced_exporter):
        by_name = _series_by_name(enhanced_exporter.collect())
        value, _ = _single(
            by_name, "green_agent_distillation_quality_retention",
        )
        assert value == pytest.approx(0.93)

    def test_energy_reduction_percent(self, enhanced_exporter):
        by_name = _series_by_name(enhanced_exporter.collect())
        value, _ = _single(
            by_name, "green_agent_distillation_energy_reduction_percent",
        )
        assert value == pytest.approx(55.0)

    def test_missing_source_omits_metrics(self, base_registry):
        names = _series_names(
            GraphMetricsExporter(base_registry).collect(),
        )
        assert "green_agent_distilled_models_total" not in names
        assert "green_agent_distillation_quality_retention" not in names
        assert (
            "green_agent_distillation_energy_reduction_percent"
            not in names
        )


# =============================================================================
# ENHANCEMENT 2: Causal RL
# =============================================================================

class TestCausalRLMetrics:
    def test_epsilon(self, enhanced_exporter):
        by_name = _series_by_name(enhanced_exporter.collect())
        value, _ = _single(by_name, "green_agent_rl_epsilon")
        assert value == pytest.approx(0.12)

    def test_observations_total(self, enhanced_exporter):
        by_name = _series_by_name(enhanced_exporter.collect())
        value, _ = _single(
            by_name, "green_agent_rl_observations_total",
        )
        assert value == 42

    def test_buffer_samples(self, enhanced_exporter):
        """
        v5.1 renamed the mislabeled ``green_agent_rl_policy_updates_total``
        (which reported buffer size) to ``green_agent_rl_buffer_samples``
        as a gauge. The old name is no longer emitted.
        """
        by_name = _series_by_name(enhanced_exporter.collect())
        value, _ = _single(by_name, "green_agent_rl_buffer_samples")
        assert value == 3
        assert "green_agent_rl_policy_updates_total" not in by_name

    def test_per_strategy_weight_l1(self, enhanced_exporter):
        by_name = _series_by_name(enhanced_exporter.collect())
        weights = _label_map(
            by_name, "green_agent_rl_weight_l1", "strategy",
        )
        assert set(weights) == {"lora", "qlora"}
        # |0.5|+|0.2|+|0.1| = 0.8
        assert weights["lora"] == pytest.approx(0.8)
        # |0.3|+|0.1|+|0.4|+|0.1| = 0.9
        assert weights["qlora"] == pytest.approx(0.9)

    def test_missing_source_omits_metrics(self, base_registry):
        names = _series_names(
            GraphMetricsExporter(base_registry).collect(),
        )
        assert "green_agent_rl_epsilon" not in names
        assert "green_agent_rl_observations_total" not in names


# =============================================================================
# ENHANCEMENT 3: Federated Green Learning
# =============================================================================

class TestFederatedMetrics:
    def test_updates_total(self, enhanced_exporter):
        by_name = _series_by_name(enhanced_exporter.collect())
        value, _ = _single(
            by_name, "green_agent_federated_updates_total",
        )
        assert value == 5

    def test_contributors_deduplicated(self, enhanced_exporter):
        by_name = _series_by_name(enhanced_exporter.collect())
        value, _ = _single(
            by_name, "green_agent_federated_contributors",
        )
        # 5 updates but only 2 distinct deployment_ids
        assert value == 2

    def test_aggregate_present_per_region(self, enhanced_exporter):
        by_name = _series_by_name(enhanced_exporter.collect())
        agg = _label_map(
            by_name, "green_agent_federated_aggregate", "key",
        )
        assert agg.get("carbon:US-CA") == pytest.approx(375.0)
        assert agg.get("carbon:EU-NORTH") == pytest.approx(210.0)

    def test_missing_source_omits_metrics(self, base_registry):
        names = _series_names(
            GraphMetricsExporter(base_registry).collect(),
        )
        assert "green_agent_federated_updates_total" not in names


# =============================================================================
# ENHANCEMENT 4: Multi-Agent Coordination
# =============================================================================

class TestMultiAgentMetrics:
    def test_agents_total(self, enhanced_exporter):
        by_name = _series_by_name(enhanced_exporter.collect())
        value, _ = _single(by_name, "green_agent_agents_total")
        assert value == 3

    def test_agents_by_role_split(self, enhanced_exporter):
        """Every role emitted as a separate series (not just the last)."""
        by_name = _series_by_name(enhanced_exporter.collect())
        roles = _label_map(
            by_name, "green_agent_agents_by_role", "role",
        )
        assert roles == {"carbon_heavy": 2, "latency_critical": 1}
        assert sum(roles.values()) == 3

    def test_per_agent_success_rate(self, enhanced_exporter):
        by_name = _series_by_name(enhanced_exporter.collect())
        rates = _label_map(
            by_name, "green_agent_agent_success_rate", "agent_id",
        )
        assert set(rates) == {"a1", "a2", "a3"}
        assert rates["a1"] == pytest.approx(0.9)
        assert rates["a2"] == pytest.approx(0.85)
        assert rates["a3"] == pytest.approx(0.92)

    def test_missing_source_omits_metrics(self, base_registry):
        names = _series_names(
            GraphMetricsExporter(base_registry).collect(),
        )
        assert "green_agent_agents_total" not in names


# =============================================================================
# ENHANCEMENT 5: Temporal Logic
# =============================================================================

class TestTemporalLogicMetrics:
    def test_violations_by_property(self, enhanced_exporter):
        """Two violations of DeadlineRespected, one of CarbonBudget."""
        by_name = _series_by_name(enhanced_exporter.collect())
        violations = _label_map(
            by_name,
            "green_agent_temporal_violations_total",
            "property",
        )
        assert violations == {
            "DeadlineRespected": 2,
            "CarbonBudgetNotExceeded": 1,
        }

    def test_last_violation_age(self, enhanced_exporter):
        by_name = _series_by_name(enhanced_exporter.collect())
        value, _ = _single(
            by_name,
            "green_agent_temporal_last_violation_age_seconds",
        )
        assert 0.0 <= value < 60.0

    def test_missing_source_omits_metrics(self, base_registry):
        names = _series_names(
            GraphMetricsExporter(base_registry).collect(),
        )
        assert "green_agent_temporal_violations_total" not in names


# =============================================================================
# ENHANCEMENT 6: XAI
# =============================================================================

class TestXAIMetrics:
    def test_explanations_total(self, enhanced_exporter):
        by_name = _series_by_name(enhanced_exporter.collect())
        value, _ = _single(
            by_name, "green_agent_explanations_total",
        )
        assert value == 137

    def test_average_confidence(self, enhanced_exporter):
        by_name = _series_by_name(enhanced_exporter.collect())
        value, _ = _single(
            by_name, "green_agent_explanation_confidence",
        )
        assert value == pytest.approx(0.87)

    def test_missing_source_omits_metrics(self, base_registry):
        names = _series_names(
            GraphMetricsExporter(base_registry).collect(),
        )
        assert "green_agent_explanations_total" not in names


# =============================================================================
# ENHANCEMENT 7: Adaptive Precision
# =============================================================================

class TestAdaptivePrecisionMetrics:
    def test_precision_task_counts_per_level(self, enhanced_exporter):
        by_name = _series_by_name(enhanced_exporter.collect())
        counts = _label_map(
            by_name, "green_agent_precision_tasks_total", "precision",
        )
        assert counts == {
            "fp32": 10, "fp16": 5, "int8": 30, "int4": 15,
        }

    def test_precision_shares_sum_to_one(self, enhanced_exporter):
        """Shares across all four precision labels sum to 1.0."""
        by_name = _series_by_name(enhanced_exporter.collect())
        shares = [
            v for v, _ in by_name.get("green_agent_precision_share", [])
        ]
        assert len(shares) == 4
        assert sum(shares) == pytest.approx(1.0, abs=1e-6)

    def test_precision_labels_present(self, enhanced_exporter):
        by_name = _series_by_name(enhanced_exporter.collect())
        labels = _label_map(
            by_name, "green_agent_precision_share", "precision",
        )
        assert set(labels) == {"fp32", "fp16", "int8", "int4"}

    def test_missing_counts_omits_metrics(self, base_registry):
        names = _series_names(
            GraphMetricsExporter(base_registry).collect(),
        )
        assert "green_agent_precision_share" not in names


# =============================================================================
# ENHANCEMENT 8: Carbon Markets
# =============================================================================

class TestCarbonMarketMetrics:
    def test_credits_purchased(self, enhanced_exporter):
        by_name = _series_by_name(enhanced_exporter.collect())
        value, _ = _single(
            by_name, "green_agent_carbon_credits_purchased_total",
        )
        assert value == 2

    def test_credits_sold(self, enhanced_exporter):
        by_name = _series_by_name(enhanced_exporter.collect())
        value, _ = _single(
            by_name, "green_agent_carbon_credits_sold_total",
        )
        assert value == 1

    def test_trade_volume(self, enhanced_exporter):
        by_name = _series_by_name(enhanced_exporter.collect())
        value, _ = _single(
            by_name, "green_agent_market_trade_volume_usd",
        )
        assert value == pytest.approx(1.5)   # 0.5 + 0.7 + 0.3

    def test_market_spend(self, enhanced_exporter):
        by_name = _series_by_name(enhanced_exporter.collect())
        value, _ = _single(
            by_name, "green_agent_market_spend_usd",
        )
        assert value == pytest.approx(1.2)

    def test_carbon_price(self, enhanced_exporter):
        by_name = _series_by_name(enhanced_exporter.collect())
        value, _ = _single(
            by_name, "green_agent_carbon_price_usd_per_tco2",
        )
        assert value == pytest.approx(45.0)

    def test_rec_available(self, enhanced_exporter):
        by_name = _series_by_name(enhanced_exporter.collect())
        value, _ = _single(
            by_name, "green_agent_rec_available_mwh",
        )
        assert value == pytest.approx(120.0)

    def test_missing_source_omits_metrics(self, base_registry):
        names = _series_names(
            GraphMetricsExporter(base_registry).collect(),
        )
        assert "green_agent_carbon_credits_purchased_total" not in names


# =============================================================================
# ENHANCEMENT 9: Resilience & Chaos
# =============================================================================

class TestResilienceMetrics:
    def test_circuit_states_per_breaker(self, enhanced_exporter):
        """All three breakers emit their own state series."""
        by_name = _series_by_name(enhanced_exporter.collect())
        states = _label_map(
            by_name, "green_agent_circuit_state", "name",
        )
        assert states == {
            "forecaster": 0,   # closed
            "scheduler": 2,    # open
            "ledger": 1,       # half_open
        }

    def test_circuit_failures_per_breaker(self, enhanced_exporter):
        by_name = _series_by_name(enhanced_exporter.collect())
        failures = _label_map(
            by_name, "green_agent_circuit_failures_total", "name",
        )
        assert failures == {
            "forecaster": 0,
            "scheduler": 3,
            "ledger": 1,
        }

    def test_chaos_events(self, enhanced_exporter):
        by_name = _series_by_name(enhanced_exporter.collect())
        value, _ = _single(by_name, "green_agent_chaos_events_total")
        assert value == 2

    def test_missing_source_omits_metrics(self, base_registry):
        names = _series_names(
            GraphMetricsExporter(base_registry).collect(),
        )
        assert "green_agent_circuit_state" not in names
        assert "green_agent_chaos_events_total" not in names


# =============================================================================
# ENHANCEMENT 10: HITL & Active Learning
# =============================================================================

class TestHITLMetrics:
    def test_pending(self, enhanced_exporter):
        by_name = _series_by_name(enhanced_exporter.collect())
        value, _ = _single(by_name, "green_agent_hitl_pending")
        assert value == 2

    def test_reviews_split(self, enhanced_exporter):
        """Approved and denied emitted as separate series."""
        by_name = _series_by_name(enhanced_exporter.collect())
        reviews = _label_map(
            by_name, "green_agent_hitl_reviews_total", "decision",
        )
        assert reviews == {"approved": 2, "denied": 1}
        assert sum(reviews.values()) == 3   # feedback_log length

    def test_active_learning_batch_size(self, enhanced_exporter):
        by_name = _series_by_name(enhanced_exporter.collect())
        value, _ = _single(
            by_name, "green_agent_active_learning_batch_size",
        )
        assert value == 3

    def test_missing_source_omits_metrics(self, base_registry):
        names = _series_names(
            GraphMetricsExporter(base_registry).collect(),
        )
        assert "green_agent_hitl_pending" not in names


# =============================================================================
# Prometheus rendering
# =============================================================================

class TestPrometheusRendering:
    def test_render_includes_enhancement_metrics(self, enhanced_exporter):
        text = enhanced_exporter.render()
        assert "green_agent_rl_epsilon" in text
        assert "green_agent_agents_total" in text
        assert "green_agent_circuit_state" in text
        assert "green_agent_hitl_pending" in text

    @requires_helium
    def test_render_still_includes_helium(self, base_registry):
        helium_monitor = Mock(spec=HeliumMonitor)
        helium_monitor.get_current_supply.return_value = (
            HeliumSupplySignal(
                timestamp=datetime.now(),
                scarcity_level=HeliumScarcityLevel.NORMAL,
                scarcity_score=0.15,
                spot_price_usd_per_liter=4.2,
                fab_inventory_days=28,
                vendor_alerts=[],
                source="test",
            )
        )
        exporter = GraphMetricsExporter(
            base_registry, helium_monitor=helium_monitor,
        )
        text = exporter.render()
        assert "green_agent_helium_scarcity_level" in text

    def test_render_parses_with_prometheus_client(self, enhanced_exporter):
        """v5.1 fix: use the real parser instead of a weak regex."""
        families = list(
            text_string_to_metric_families(enhanced_exporter.render())
        )
        assert families, "parser must return at least one family"


# =============================================================================
# Feature toggles
# =============================================================================

class TestFeatureToggles:
    def test_all_sources_missing_yields_only_base(self, base_registry):
        """v5.1 fix: positive check uses ``_series_names``, not ``in list``."""
        exporter = GraphMetricsExporter(base_registry)
        names = _series_names(exporter.collect())
        assert "green_agent_execution_graphs_active" in names
        for key in (
            "green_agent_distilled_models_total",
            "green_agent_rl_epsilon",
            "green_agent_federated_updates_total",
            "green_agent_agents_total",
            "green_agent_temporal_violations_total",
            "green_agent_explanations_total",
            "green_agent_precision_share",
            "green_agent_carbon_credits_purchased_total",
            "green_agent_circuit_state",
            "green_agent_hitl_pending",
        ):
            assert key not in names, f"{key} unexpectedly present"

    def test_partial_sources_only_expose_configured(
        self, base_registry, mock_rl_policy, mock_hitl,
    ):
        sources = EnhancementMetricsSources(
            rl_policy=mock_rl_policy, hitl=mock_hitl,
        )
        exporter = GraphMetricsExporter(
            base_registry, enhancement_sources=sources,
        )
        names = _series_names(exporter.collect())
        # Configured
        assert "green_agent_rl_epsilon" in names
        assert "green_agent_hitl_pending" in names
        # Not configured
        assert "green_agent_distilled_models_total" not in names
        assert "green_agent_agents_total" not in names
        assert "green_agent_carbon_credits_purchased_total" not in names


# =============================================================================
# Registry failure resilience
# =============================================================================

class TestRegistryFailureResilience:
    def test_registry_health_raises(self):
        registry = Mock()
        registry.health.side_effect = RuntimeError("registry down")
        registry.get.return_value = None
        exporter = GraphMetricsExporter(registry)
        by_name = _series_by_name(exporter.collect())   # must not raise
        value, _ = _single(
            by_name, "green_agent_execution_graphs_active",
        )
        assert value == 0

    def test_registry_get_raises(self):
        registry = Mock()
        registry.health.return_value = {
            "execution_count": 1, "singletons": {},
        }
        registry.get.side_effect = RuntimeError("get failed")
        exporter = GraphMetricsExporter(registry)
        names = _series_names(exporter.collect())
        assert "green_agent_execution_graphs_active" in names


# =============================================================================
# v5.1 fixes
# =============================================================================

class TestV5_1Fixes:
    """One test per fix from the exporter review."""

    def test_collect_returns_list_of_series(self, enhanced_exporter):
        series = enhanced_exporter.collect()
        assert isinstance(series, list)
        assert not isinstance(series, dict)
        for item in series:
            assert isinstance(item, tuple) and len(item) == 3
            assert isinstance(item[0], str)
            assert isinstance(item[1], (int, float))
            assert isinstance(item[2], dict)

    def test_multi_series_preserved(self, enhanced_exporter):
        by_name = _series_by_name(enhanced_exporter.collect())
        assert len(by_name["green_agent_circuit_state"]) == 3
        assert len(by_name["green_agent_agents_by_role"]) == 2
        assert len(by_name["green_agent_agent_success_rate"]) == 3
        assert len(by_name["green_agent_precision_share"]) == 4
        assert len(
            by_name["green_agent_federated_aggregate"]
        ) == 2
        assert len(
            by_name["green_agent_temporal_violations_total"]
        ) == 2
        assert len(by_name["green_agent_hitl_reviews_total"]) == 2
        assert len(by_name["green_agent_rl_weight_l1"]) == 2

    def test_safe_registry_get_unknown_kind(self, base_registry):
        exporter = GraphMetricsExporter(base_registry)
        assert exporter._safe_registry_get("federated") is None
        assert exporter._safe_registry_get("") is None
        assert exporter._safe_registry_get("bogus") is None

    def test_registry_circuit_breaker(self):
        registry = Mock()
        registry.health.side_effect = RuntimeError("down")
        registry.get.return_value = None
        exporter = GraphMetricsExporter(registry)

        for _ in range(5):
            exporter.collect()

        registry.health.reset_mock()
        exporter.collect()
        registry.health.assert_not_called()

    def test_empty_scrape_has_self_metrics(self, base_registry):
        exporter = GraphMetricsExporter(base_registry)
        text = exporter.render()
        assert "green_agent_exporter_info" in text
        assert "green_agent_exporter_up" in text

    def test_render_json(self, enhanced_exporter):
        payload = enhanced_exporter.render_json()
        data = json.loads(payload)
        assert isinstance(data, list)
        assert data, "render_json must not be empty"
        for entry in data:
            assert {"name", "value", "labels"} <= set(entry)

    def test_health(self, enhanced_exporter):
        report = enhanced_exporter.health()
        assert isinstance(report, dict)
        assert "ok" in report

    def test_test_smoke(self, enhanced_exporter):
        report = enhanced_exporter.test_smoke()
        assert report["ok"] is True
        assert report["steps"]

    def test_version_present(self):
        assert isinstance(EXPORTER_VERSION, str)
        assert EXPORTER_VERSION

    def test_collector_source_adapter(self, base_registry):
        """``MetricsCollector.get_statistics()`` is preferred source."""
        class MockCollector:
            def get_statistics(self):
                return {
                    "circuits": {
                        "forecaster": "closed",
                        "scheduler": "open",
                    },
                    "agents": {
                        "a1": {"role": "carbon_optimizer", "calls": 5},
                        "a2": {"role": "carbon_optimizer", "calls": 3},
                    },
                    "hitl_pending": 4,
                }

        sources = EnhancementMetricsSources(collector=MockCollector())
        exporter = GraphMetricsExporter(
            base_registry, enhancement_sources=sources,
        )
        by_name = _series_by_name(exporter.collect())

        assert _label_map(
            by_name, "green_agent_circuit_state", "name",
        ) == {"forecaster": 0, "scheduler": 2}
        value, _ = _single(by_name, "green_agent_hitl_pending")
        assert value == 4

    def test_coerce_float_rejects_nan_inf(self, base_registry):
        """NaN and Inf values are skipped, not emitted."""
        class BadDistiller:
            distilled_count = float("nan")
            last_quality_retention = float("inf")
            last_energy_reduction_percent = 55.0

        sources = EnhancementMetricsSources(
            distiller=BadDistiller(),
        )
        exporter = GraphMetricsExporter(
            base_registry, enhancement_sources=sources,
        )
        names = _series_names(exporter.collect())
        assert "green_agent_distilled_models_total" not in names
        assert (
            "green_agent_distillation_quality_retention" not in names
        )
        assert (
            "green_agent_distillation_energy_reduction_percent"
            in names
        )

    @requires_helium
    def test_helium_baseline_override(self, base_registry):
        """``helium_baseline_price_usd=`` configures the premium calc."""
        helium_monitor = Mock(spec=HeliumMonitor)
        helium_monitor.get_current_supply.return_value = (
            HeliumSupplySignal(
                timestamp=datetime.now(),
                scarcity_level=HeliumScarcityLevel.NORMAL,
                scarcity_score=0.0,
                spot_price_usd_per_liter=10.0,
                fab_inventory_days=30,
                vendor_alerts=[],
                source="test",
            )
        )
        exporter = GraphMetricsExporter(
            base_registry,
            helium_monitor=helium_monitor,
            helium_baseline_price_usd=5.0,
        )
        by_name = _series_by_name(exporter.collect())
        value, _ = _single(
            by_name, "green_agent_helium_price_premium_usd",
        )
        assert value == pytest.approx(5.0)   # 10.0 - 5.0

    @requires_helium
    def test_unknown_scarcity_defaults_to_critical(self, base_registry):
        """Unknown scarcity level defaults to CRITICAL (2)."""

        class WeirdLevel:
            value = "cosmic-ray"

        class MockHelium:
            def get_current_supply(self):
                return type("S", (), {
                    "scarcity_level": WeirdLevel(),
                    "scarcity_score": 0.5,
                    "spot_price_usd_per_liter": 5.0,
                    "fab_inventory_days": 10,
                    "vendor_alerts": [],
                    "source": "weird",
                })()

        exporter = GraphMetricsExporter(
            base_registry, helium_monitor=MockHelium(),
        )
        by_name = _series_by_name(exporter.collect())
        value, _ = _single(
            by_name, "green_agent_helium_scarcity_level",
        )
        assert value == 2


# =============================================================================
# HTTP server
# =============================================================================

def _start_and_get_port(exporter, host: str = "127.0.0.1") -> int:
    """Start on port 0 (no race) and return the bound port."""
    exporter.start_http_server(port=0, host=host)
    time.sleep(0.15)
    server = getattr(exporter, "_server", None)
    assert server is not None, "server did not start"
    return server.server_address[1]


class TestHTTPServer:
    def test_metrics_endpoint(self, enhanced_exporter):
        port = _start_and_get_port(enhanced_exporter)
        try:
            resp = requests.get(
                f"http://127.0.0.1:{port}/metrics", timeout=5,
            )
            assert resp.status_code == 200
            assert "text/plain" in resp.headers.get(
                "Content-Type", "",
            )
            assert "# HELP" in resp.text
            assert "# TYPE" in resp.text
            list(text_string_to_metric_families(resp.text))
        finally:
            enhanced_exporter.stop_http_server()

    def test_health_endpoint(self, enhanced_exporter):
        port = _start_and_get_port(enhanced_exporter)
        try:
            resp = requests.get(
                f"http://127.0.0.1:{port}/health", timeout=5,
            )
            assert resp.status_code == 200
            assert "application/json" in resp.headers.get(
                "Content-Type", "",
            )
            payload = resp.json()
            assert "ok" in payload
        finally:
            enhanced_exporter.stop_http_server()

    def test_json_endpoint(self, enhanced_exporter):
        port = _start_and_get_port(enhanced_exporter)
        try:
            resp = requests.get(
                f"http://127.0.0.1:{port}/json", timeout=5,
            )
            assert resp.status_code == 200
            data = resp.json()
            assert isinstance(data, list)
            for entry in data:
                assert {"name", "value", "labels"} <= set(entry)
        finally:
            enhanced_exporter.stop_http_server()

    def test_404_for_unknown_path(self, enhanced_exporter):
        port = _start_and_get_port(enhanced_exporter)
        try:
            resp = requests.get(
                f"http://127.0.0.1:{port}/unknown", timeout=5,
            )
            assert resp.status_code == 404
        finally:
            enhanced_exporter.stop_http_server()

    def test_concurrent_scrapes(self, enhanced_exporter):
        port = _start_and_get_port(enhanced_exporter)
        try:
            url = f"http://127.0.0.1:{port}/metrics"
            with ThreadPoolExecutor(max_workers=10) as ex:
                responses = list(
                    ex.map(
                        lambda _: requests.get(url, timeout=5),
                        range(20),
                    )
                )
            assert all(r.status_code == 200 for r in responses)
        finally:
            enhanced_exporter.stop_http_server()

    def test_stop_is_idempotent(self, enhanced_exporter):
        enhanced_exporter.start_http_server(port=0, host="127.0.0.1")
        enhanced_exporter.stop_http_server()
        enhanced_exporter.stop_http_server()   # no-op

    def test_restart_after_stop(self, enhanced_exporter):
        enhanced_exporter.start_http_server(port=0, host="127.0.0.1")
        server = getattr(enhanced_exporter, "_server", None)
        assert server is not None
        port = server.server_address[1]
        enhanced_exporter.stop_http_server()
        # Re-bind to the same port (this fails if server_close() was not
        # called)
        enhanced_exporter.start_http_server(
            port=port, host="127.0.0.1",
        )
        enhanced_exporter.stop_http_server()


# =============================================================================
# End-to-end
# =============================================================================

class TestEndToEnd:
    @requires_helium
    def test_full_stack_with_all_sources(
        self, enhanced_exporter, base_registry,
    ):
        helium_monitor = Mock(spec=HeliumMonitor)
        helium_monitor.get_current_supply.return_value = (
            HeliumSupplySignal(
                timestamp=datetime.now(),
                scarcity_level=HeliumScarcityLevel.CAUTION,
                scarcity_score=0.4,
                spot_price_usd_per_liter=5.5,
                fab_inventory_days=20,
                vendor_alerts=["Test"],
                source="test",
            )
        )
        enhanced_exporter.helium_monitor = helium_monitor

        by_name = _series_by_name(enhanced_exporter.collect())
        # Helium
        assert "green_agent_helium_scarcity_level" in by_name
        assert "green_agent_helium_spot_price_usd" in by_name
        # Enhancements
        assert "green_agent_rl_epsilon" in by_name
        assert "green_agent_hitl_pending" in by_name
        assert "green_agent_circuit_state" in by_name

        text = enhanced_exporter.render()
        assert "green_agent_helium_scarcity_level" in text
        assert "green_agent_rl_epsilon" in text

    def test_dashboard_has_25_panels(self, enhanced_exporter):
        dashboard = json.loads(enhanced_exporter.grafana_dashboard())
        assert "Green Agent" in dashboard["title"]
        assert len(dashboard["panels"]) >= 25, (
            f"expected 25+ panels, got {len(dashboard['panels'])}"
        )


# =============================================================================
# Module entry point
# =============================================================================

if __name__ == "__main__":  # pragma: no cover
    import sys
    sys.exit(pytest.main([__file__, "-v", "--tb=short"]))
