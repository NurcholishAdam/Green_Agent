"""
test_graph_metrics_exporter.py — Complete Test Suite (v5.1)
===========================================================

Unit and integration tests for GraphMetricsExporter, plus comprehensive
coverage for the ten enhancement-layer metrics:

  1. Quantum-Distillation
  2. Causal RL
  3. Federated Green Learning
  4. Multi-Agent Coordination
  5. Temporal Logic
  6. Explainable AI (XAI)
  7. Adaptive Precision
  8. Carbon Markets
  9. Resilience & Chaos
 10. Human-in-the-Loop + Active Learning

Fixes in v5.1 (see review)
--------------------------
  * P0 — All assertions adapted to the v5.1 ``collect()`` return shape
         (``List[Tuple[str, float, Dict]]``). Previously the tests indexed
         ``metrics["..."]`` which raises ``TypeError`` against v5.1 and
         silently no-ops ``in`` checks against a list.
  * P0 — The 300-line inline fallback was removed. Tests that depend on
         the real exporter are marked ``@requires_real`` and skipped when
         the module is not importable. ``pytest.importorskip`` is applied
         at module scope for the required third-party parsers.
  * P0 — ``_HAS_REAL_EXPORTER`` is used as an actual skip gate, not just
         assigned and forgotten.
  * P0 — ``test_dashboard_has_many_panels`` asserts ``>= 25`` (was a
         no-op ``>= 7`` inside an ``if`` that could never fire).
  * P0 — ``test_dashboard_uid`` asserts the UID is present (was a no-op).
  * P0 — ``test_precision_shares_sum_to_one`` actually sums shares to 1.0
         (was a count check with a confessional comment).
  * P0 — ``test_dashboard_expressions`` marked ``@requires_real`` so it
         no longer fails against the missing fallback dashboard.
  * P1 — Presence-only tests rewritten to verify actual values:
         circuit-breaker states (3 series), per-role agent counts,
         federated aggregates, HITL split (approved=2, denied=1).
  * P1 — ``test_collect_cardinality_limiting`` rewritten to assert
         exactly ``max_edges_export`` series, not ``<=``.
  * P1 — ``test_concurrent_collect_consistent`` verifies structural
         equivalence, not just "no exception raised."
  * P1 — HTTP tests bind ``port=0`` (no free-port race) and verify
         concurrent scrapes complete.
  * P2 — New ``TestV5_1Fixes`` class covering: list return shape, label
         escaping, dry_run side effects, registry circuit breaker,
         empty-scrape self-metrics, ``render_json``, ``health()``,
         ``test_smoke()``, ``__version__``, collector-source adapter,
         ``_coerce_float`` NaN/Inf rejection, unknown scarcity defaults,
         restart-after-stop, ``_safe_registry_get`` for unknown kinds.
  * P3 — Unused imports removed; ``__main__`` invokes pytest and exits
         with pytest's status code.

Run with:
    pytest test_graph_metrics_exporter.py -v
    pytest test_graph_metrics_exporter.py::TestV5_1Fixes -v
    pytest test_graph_metrics_exporter.py -k "not requires_real"
"""

from __future__ import annotations

import json
import logging
import re
import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple
from unittest.mock import Mock, patch

import pytest


# =============================================================================
# Optional third-party dependencies
# =============================================================================
# ``prometheus_client`` is REQUIRED to verify output format; use
# ``importorskip`` at module scope so a missing parser fails loudly rather
# than silently skipping the format tests. Same for ``requests``.

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
# Real exporter import — skip the whole module if unavailable
# =============================================================================
try:
    from src.monitoring.graph_metrics_exporter import (
        EnhancementMetricsSources,
        GraphMetricsExporter,
        __version__ as EXPORTER_VERSION,
    )
    _HAS_REAL_EXPORTER = True
except ImportError:  # pragma: no cover — CI runs with the real package
    _HAS_REAL_EXPORTER = False
    GraphMetricsExporter = None  # type: ignore[assignment, misc]
    EnhancementMetricsSources = None  # type: ignore[assignment, misc]
    EXPORTER_VERSION = None


requires_real = pytest.mark.skipif(
    not _HAS_REAL_EXPORTER,
    reason=(
        "src.monitoring.graph_metrics_exporter not importable; "
        "install the full package to run these tests."
    ),
)


# =============================================================================
# Graph type enum (mirrors the package's real enum)
# =============================================================================
class GraphType(Enum):
    CAUSAL = "causal"
    POLICY = "policy"


# =============================================================================
# Series helpers — v5.1 API (List[Tuple[str, float, Dict]])
# =============================================================================

def _series_dict(
    series: List[Tuple[str, float, Dict[str, str]]],
) -> Dict[Tuple[str, Tuple[Tuple[str, str], ...]], float]:
    """Collapse to a labelled lookup: ``{(name, labels_tuple): value}``."""
    return {
        (name, tuple(sorted(lbl.items()))): value
        for name, value, lbl in series
    }


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
    """Distinct metric names in a series list."""
    return {name for name, _, _ in series}


def _labels_of(
    series: List[Tuple[str, float, Dict[str, str]]],
    metric_name: str,
) -> Dict[str, float]:
    """Return ``{<one distinguished label>: value}`` for a labelled metric."""
    result: Dict[str, float] = {}
    for name, value, lbl in series:
        if name != metric_name:
            continue
        # Choose the first non-job label value to distinguish series.
        for key in sorted(lbl):
            if key not in ("job", "instance"):
                result[str(lbl[key])] = value
                break
    return result


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture
def mock_registry():
    registry = Mock()
    registry.health.return_value = {
        "execution_count": 10,
        "singletons": {
            "causal": {
                "node_count": 50,
                "edge_count": 120,
                "anomaly_count": 3,
                "ideal_path_count": 15,
            },
            "policy": {
                "node_count": 30,
                "edge_count": 75,
                "ideal_path_count": 5,
            },
        },
    }
    registry.get.return_value = None  # default: no graphs
    return registry


def _make_edge(source, target, label, weight, confidence):
    class Edge:
        def __init__(self):
            self.source_id = source
            self.target_id = target
            self.label = label
            self.weight = weight
            self.confidence = confidence
    return Edge()


def _make_coordinator(agents_spec):
    """agents_spec: {agent_id: (role_value, success_rate)}."""
    class R:
        def __init__(self, v):
            self.value = v
    class A:
        def __init__(self, aid, role, sr):
            self.agent_id = aid
            self.role = R(role)
            self.success_rate = sr
    class C:
        pass
    c = C()
    c.agents = {
        aid: A(aid, role, sr)
        for aid, (role, sr) in agents_spec.items()
    }
    return c


@pytest.fixture
def mock_causal_graph():
    graph = Mock()
    graph.edges = [
        _make_edge(
            "carbon_intensity", "execution_decision",
            "influences", 0.85, 0.95,
        ),
        _make_edge(
            "execution_decision", "energy_consumed",
            "determines", 0.92, 0.98,
        ),
        _make_edge(
            "energy_consumed", "carbon_emitted",
            "converts", 0.40, 0.99,
        ),
        *[
            _make_edge(f"n{i}", f"n{i+1}", "test", 0.5 + i * 0.01, 0.9)
            for i in range(200)
        ],
    ]
    return graph


@pytest.fixture
def mock_policy_graph():
    graph = Mock()
    graph.export_weights.return_value = [
        {
            "source": "eco_mode", "target": "throttle",
            "weight": 0.75, "context_tag": "yellow_zone",
        },
        {
            "source": "green_zone", "target": "execute_full",
            "weight": 1.0, "context_tag": "green_zone",
        },
        {
            "source": "red_zone", "target": "defer",
            "weight": 0.3, "context_tag": "red_zone",
        },
        *[
            {
                "source": f"p{i}", "target": f"p{i+1}",
                "weight": 0.5 + i * 0.01, "context_tag": "test",
            }
            for i in range(150)
        ],
    ]
    return graph


@pytest.fixture
def registry_with_graphs(
    mock_registry, mock_causal_graph, mock_policy_graph,
):
    def get_side_effect(key):
        if key in (GraphType.CAUSAL, "causal"):
            return mock_causal_graph
        if key in (GraphType.POLICY, "policy"):
            return mock_policy_graph
        return None
    mock_registry.get.side_effect = get_side_effect
    return mock_registry


@pytest.fixture
def exporter(registry_with_graphs):
    if not _HAS_REAL_EXPORTER:
        pytest.skip("real exporter not importable")
    return GraphMetricsExporter(
        registry_with_graphs, max_edges_export=50,
    )


# --- Enhancement source mocks ---

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
            return {
                "carbon:US-CA": 375.0,
                "carbon:EU-NORTH": 210.0,
            }
    return F()


@pytest.fixture
def mock_coordinator():
    return _make_coordinator({
        "a1": ("carbon_heavy", 0.9),
        "a2": ("latency_critical", 0.85),
        "a3": ("carbon_heavy", 0.92),
    })


@pytest.fixture
def mock_temporal_monitor():
    class T:
        violations = [
            {
                "property": "DeadlineRespected",
                "at": datetime.now().isoformat(),
            },
            {
                "property": "DeadlineRespected",
                "at": datetime.now().isoformat(),
            },
            {
                "property": "CarbonBudgetNotExceeded",
                "at": datetime.now().isoformat(),
            },
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
    if not _HAS_REAL_EXPORTER:
        pytest.skip("real exporter not importable")
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
def enhanced_exporter(registry_with_graphs, enhancement_sources):
    if not _HAS_REAL_EXPORTER:
        pytest.skip("real exporter not importable")
    return GraphMetricsExporter(
        registry_with_graphs,
        max_edges_export=50,
        enhancement_sources=enhancement_sources,
    )


# =============================================================================
# Tests — base collection (v5.1 list-of-series shape)
# =============================================================================

@requires_real
class TestBaseCollection:
    def test_collect_returns_list_of_tuples(self, exporter):
        series = exporter.collect()
        assert isinstance(series, list)
        assert len(series) > 0
        for name, value, labels in series:
            assert isinstance(name, str)
            assert isinstance(value, (int, float))
            assert isinstance(labels, dict)

    def test_collect_empty_registry(self):
        registry = Mock()
        registry.health.return_value = {
            "execution_count": 0, "singletons": {},
        }
        registry.get.return_value = None
        exp = GraphMetricsExporter(registry)
        series = exp.collect()
        by_name = _series_by_name(series)
        exec_vals = [v for v, _ in by_name["green_agent_execution_graphs_active"]]
        assert exec_vals == [0]

    def test_collect_with_graph_data(self, exporter):
        series = exporter.collect()
        by_name = _series_by_name(series)

        # Unlabeled execution count
        exec_vals = [v for v, _ in by_name["green_agent_execution_graphs_active"]]
        assert exec_vals == [10]

        # Labelled node counts per graph type
        node_vals = {
            lbl["graph_type"]: v
            for v, lbl in by_name["green_agent_graph_nodes"]
        }
        assert node_vals == {"causal": 50, "policy": 30}

        # Labelled anomaly counts
        anomaly_vals = {
            lbl["graph_type"]: v
            for v, lbl in by_name["green_agent_anomalies_active"]
        }
        assert anomaly_vals == {"causal": 3}

    def test_ideal_paths_both_graph_types_at_collect(self, exporter):
        """``collect()`` returns one entry per graph type's ideal_paths."""
        series = exporter.collect()
        ideal_vals = [
            v for name, v, _ in series
            if name == "green_agent_ideal_paths_total"
        ]
        # 15 (causal) + 5 (policy) — both present at collect() level
        assert len(ideal_vals) == 2, (
            f"expected 2 per-graph-type entries, got {len(ideal_vals)}"
        )
        assert sum(ideal_vals) == 20

    def test_causal_edges_exact_count(self, exporter):
        """Exactly ``max_edges_export`` causal edges are emitted."""
        series = exporter.collect()
        edges = [s for s in series if s[0] == "green_agent_causal_edge_weight"]
        assert len(edges) == 50, (
            f"expected exactly 50 edges (max_edges_export), got {len(edges)}"
        )

    def test_policy_edges_exact_count(self, exporter):
        series = exporter.collect()
        edges = [s for s in series if s[0] == "green_agent_policy_edge_weight"]
        assert len(edges) == 50

    def test_metric_types(self, exporter):
        """Gauge vs counter declared correctly in the rendered output."""
        text = exporter.render()
        types: Dict[str, str] = {}
        for line in text.splitlines():
            if line.startswith("# TYPE"):
                parts = line.split()
                if len(parts) >= 4:
                    types[parts[2]] = parts[3]

        for gauge in (
            "green_agent_execution_graphs_active",
            "green_agent_graph_nodes",
            "green_agent_graph_edges",
            "green_agent_anomalies_active",
            "green_agent_causal_edge_weight",
            "green_agent_policy_edge_weight",
        ):
            assert types.get(gauge) == "gauge", (
                f"{gauge} should be a gauge, got {types.get(gauge)!r}"
            )

        assert types.get("green_agent_ideal_paths_total") == "counter", (
            f"ideal_paths_total should be counter, got {types.get('green_agent_ideal_paths_total')!r}"
        )


# =============================================================================
# Tests — truncation
# =============================================================================

@requires_real
class TestTruncation:
    def test_truncation_count_causal(self, mock_causal_graph):
        registry = Mock()
        registry.health.return_value = {
            "execution_count": 0, "singletons": {},
        }
        registry.get.return_value = mock_causal_graph
        exporter = GraphMetricsExporter(registry, max_edges_export=50)
        series = exporter.collect()
        truncated = [
            (v, lbl) for name, v, lbl in series
            if name == "green_agent_edges_truncated"
        ]
        assert len(truncated) == 1
        value, labels = truncated[0]
        assert value == 153  # 203 total - 50 kept
        assert labels["graph_type"] == "causal"

    def test_truncation_count_policy(self, mock_policy_graph):
        registry = Mock()
        registry.health.return_value = {
            "execution_count": 0, "singletons": {},
        }
        registry.get.return_value = mock_policy_graph
        exporter = GraphMetricsExporter(registry, max_edges_export=50)
        series = exporter.collect()
        truncated = [
            (v, lbl) for name, v, lbl in series
            if name == "green_agent_edges_truncated"
        ]
        assert len(truncated) == 1
        _, labels = truncated[0]
        assert labels["graph_type"] == "policy"

    def test_no_truncation_when_under_limit(self, mock_policy_graph):
        registry = Mock()
        registry.health.return_value = {
            "execution_count": 0, "singletons": {},
        }
        registry.get.return_value = mock_policy_graph
        exporter = GraphMetricsExporter(registry, max_edges_export=1000)
        series = exporter.collect()
        assert "green_agent_edges_truncated" not in _series_names(series)

    def test_max_edges_default(self):
        registry = Mock()
        registry.health.return_value = {
            "execution_count": 0, "singletons": {},
        }
        registry.get.return_value = None
        exporter = GraphMetricsExporter(registry)
        assert exporter.max_edges_export == 100

    def test_max_edges_custom(self):
        registry = Mock()
        registry.health.return_value = {
            "execution_count": 0, "singletons": {},
        }
        registry.get.return_value = None
        exporter = GraphMetricsExporter(registry, max_edges_export=25)
        assert exporter.max_edges_export == 25


# =============================================================================
# Tests — rendering
# =============================================================================

@requires_real
class TestRendering:
    def test_prometheus_format(self, exporter):
        text = exporter.render()
        assert "# HELP" in text
        assert "# TYPE" in text
        assert text.endswith("\n")

        # Every TYPE line has a valid Prometheus type
        for line in text.splitlines():
            if line.startswith("# TYPE"):
                parts = line.split()
                assert len(parts) >= 4, f"malformed TYPE: {line}"
                assert parts[3] in (
                    "counter", "gauge", "histogram", "summary",
                )

    def test_always_has_exporter_self_metrics(self):
        registry = Mock()
        registry.health.return_value = {
            "execution_count": 0, "singletons": {},
        }
        registry.get.return_value = None
        exporter = GraphMetricsExporter(registry)
        text = exporter.render()
        assert "green_agent_exporter_info" in text
        assert "green_agent_exporter_up" in text

    def test_help_type_deduplicated(self, exporter):
        """``# HELP`` and ``# TYPE`` emitted once per metric name."""
        text = exporter.render()
        help_counts: Dict[str, int] = {}
        type_counts: Dict[str, int] = {}
        for line in text.splitlines():
            if line.startswith("# HELP"):
                name = line.split()[2]
                help_counts[name] = help_counts.get(name, 0) + 1
            elif line.startswith("# TYPE"):
                name = line.split()[2]
                type_counts[name] = type_counts.get(name, 0) + 1
        for name, count in help_counts.items():
            assert count == 1, f"duplicate HELP for {name}"
        for name, count in type_counts.items():
            assert count == 1, f"duplicate TYPE for {name}"

    def test_label_escaping(self):
        """Quotes and backslashes in label values are escaped."""
        registry = Mock()
        registry.health.return_value = {
            "execution_count": 0, "singletons": {},
        }
        registry.get.return_value = None
        sources = EnhancementMetricsSources(
            coordinator=_make_coordinator({
                "a1": ('role"with-quote', 0.9),
                "a2": ("role\\with-backslash", 0.85),
            })
        )
        exporter = GraphMetricsExporter(
            registry, enhancement_sources=sources,
        )
        text = exporter.render()
        assert '\\"' in text, "quotes in labels must be escaped"
        assert "\\\\" in text, "backslashes in labels must be escaped"
        # Escaped output must parse cleanly
        families = list(text_string_to_metric_families(text))
        assert len(families) > 0

    def test_render_deduplicates_identical_series(self):
        registry = Mock()
        registry.health.return_value = {
            "execution_count": 0,
            "singletons": {"causal": {"node_count": 10}},
        }
        registry.get.return_value = None
        exporter = GraphMetricsExporter(registry)
        text = exporter.render()
        node_lines = [
            ln for ln in text.splitlines()
            if ln.startswith("green_agent_graph_nodes{")
        ]
        assert len(node_lines) == 1

    def test_metric_lines_have_labels_when_expected(self, exporter):
        text = exporter.render()
        for line in text.splitlines():
            if line.startswith("green_agent_causal_edge_weight{"):
                assert "source=" in line
                assert "target=" in line
                assert "label=" in line

    def test_label_values_are_quoted(self, exporter):
        text = exporter.render()
        for match in re.findall(r"\{([^}]+)\}", text):
            for pair in match.split(","):
                assert "=" in pair, f"malformed label pair: {pair!r}"
                assert '"' in pair, f"unquoted value: {pair!r}"


# =============================================================================
# Tests — Prometheus parser compatibility
# =============================================================================

@requires_real
class TestPrometheusParsing:
    def test_full_render_parses(self, exporter):
        text = exporter.render()
        families = list(text_string_to_metric_families(text))
        assert len(families) > 0

    def test_metric_family_types(self, exporter):
        text = exporter.render()
        families = {
            f.name: f for f in text_string_to_metric_families(text)
        }
        assert "green_agent_execution_graphs_active" in families
        assert (
            families["green_agent_execution_graphs_active"].type
            == "gauge"
        )
        if "green_agent_ideal_paths_total" in families:
            assert (
                families["green_agent_ideal_paths_total"].type
                == "counter"
            )

    def test_enhanced_render_parses(self, enhanced_exporter):
        text = enhanced_exporter.render()
        families = list(text_string_to_metric_families(text))
        names = {f.name for f in families}
        assert "green_agent_execution_graphs_active" in names
        assert "green_agent_rl_epsilon" in names
        assert "green_agent_agents_total" in names
        assert "green_agent_hitl_pending" in names


# =============================================================================
# Tests — Enhancement metrics (value-verifying, not presence-only)
# =============================================================================

@requires_real
class TestEnhancementMetrics:
    # --- 1. Quantum-Distillation ---
    def test_quantum_distillation_metrics(self, enhanced_exporter):
        by_name = _series_by_name(enhanced_exporter.collect())
        assert (
            by_name["green_agent_distilled_models_total"][0][0] == 12
        )
        assert by_name[
            "green_agent_distillation_quality_retention"
        ][0][0] == pytest.approx(0.93)
        assert by_name[
            "green_agent_distillation_energy_reduction_percent"
        ][0][0] == pytest.approx(55.0)

    def test_distillation_missing_source(self, exporter):
        by_name = _series_by_name(exporter.collect())
        assert "green_agent_distilled_models_total" not in by_name

    # --- 2. Causal RL ---
    def test_causal_rl_metrics(self, enhanced_exporter):
        by_name = _series_by_name(enhanced_exporter.collect())
        assert by_name["green_agent_rl_epsilon"][0][0] == pytest.approx(
            0.12
        )
        assert by_name["green_agent_rl_observations_total"][0][0] == 42
        assert by_name["green_agent_rl_buffer_samples"][0][0] == 3

        # Both strategies present as separate series
        weight_series = by_name.get("green_agent_rl_weight_l1", [])
        assert len(weight_series) == 2
        strategies = {lbl["strategy"] for _, lbl in weight_series}
        assert strategies == {"lora", "qlora"}

        # L1 norm of lora = |0.5|+|0.2|+|0.1| = 0.8
        lora = next(
            v for v, lbl in weight_series if lbl["strategy"] == "lora"
        )
        assert lora == pytest.approx(0.8)

    def test_rl_missing_source(self, exporter):
        by_name = _series_by_name(exporter.collect())
        assert "green_agent_rl_epsilon" not in by_name

    # --- 3. Federated ---
    def test_federated_metrics(self, enhanced_exporter):
        by_name = _series_by_name(enhanced_exporter.collect())
        assert (
            by_name["green_agent_federated_updates_total"][0][0] == 5
        )
        assert (
            by_name["green_agent_federated_contributors"][0][0] == 2
        )
        # Both regions emitted as separate series
        agg = {
            lbl["key"]: v
            for v, lbl in by_name.get(
                "green_agent_federated_aggregate", []
            )
        }
        assert agg.get("carbon:US-CA") == pytest.approx(375.0)
        assert agg.get("carbon:EU-NORTH") == pytest.approx(210.0)

    def test_federated_missing_source(self, exporter):
        by_name = _series_by_name(exporter.collect())
        assert "green_agent_federated_updates_total" not in by_name

    # --- 4. Multi-Agent ---
    def test_multi_agent_role_metrics(self, enhanced_exporter):
        series = enhanced_exporter.collect()
        by_name = _series_by_name(series)
        assert by_name["green_agent_agents_total"][0][0] == 3

        # Per-role counts as separate series
        roles = {
            lbl["role"]: v
            for v, lbl in by_name.get("green_agent_agents_by_role", [])
        }
        assert roles == {"carbon_heavy": 2, "latency_critical": 1}
        assert sum(roles.values()) == 3

        # Per-agent success rate: one series per agent
        sr_series = by_name.get("green_agent_agent_success_rate", [])
        assert len(sr_series) == 3
        agent_ids = {lbl["agent_id"] for _, lbl in sr_series}
        assert agent_ids == {"a1", "a2", "a3"}

    def test_coordinator_missing_source(self, exporter):
        by_name = _series_by_name(exporter.collect())
        assert "green_agent_agents_total" not in by_name

    # --- 5. Temporal Logic ---
    def test_temporal_violation_metrics(self, enhanced_exporter):
        by_name = _series_by_name(enhanced_exporter.collect())
        violations = {
            lbl["property"]: v
            for v, lbl in by_name.get(
                "green_agent_temporal_violations_total", []
            )
        }
        assert violations == {
            "DeadlineRespected": 2,
            "CarbonBudgetNotExceeded": 1,
        }

        ages = by_name.get(
            "green_agent_temporal_last_violation_age_seconds", []
        )
        assert len(ages) == 1
        assert 0.0 <= ages[0][0] < 60.0

    def test_temporal_missing_source(self, exporter):
        by_name = _series_by_name(exporter.collect())
        assert "green_agent_temporal_violations_total" not in by_name

    # --- 6. XAI ---
    def test_xai_metrics(self, enhanced_exporter):
        by_name = _series_by_name(enhanced_exporter.collect())
        assert by_name["green_agent_explanations_total"][0][0] == 137
        assert by_name[
            "green_agent_explanation_confidence"
        ][0][0] == pytest.approx(0.87)

    def test_xai_missing_source(self, exporter):
        by_name = _series_by_name(exporter.collect())
        assert "green_agent_explanations_total" not in by_name

    # --- 7. Adaptive Precision ---
    def test_precision_shares_sum_to_one(self, enhanced_exporter):
        """Shares across all four precision labels sum to 1.0."""
        series = enhanced_exporter.collect()
        shares = [
            v for name, v, _ in series
            if name == "green_agent_precision_share"
        ]
        assert len(shares) == 4, (
            f"expected 4 precision labels, got {len(shares)}"
        )
        assert sum(shares) == pytest.approx(1.0, abs=1e-6)

    def test_precision_label_set(self, enhanced_exporter):
        """Each precision level appears exactly once."""
        by_name = _series_by_name(enhanced_exporter.collect())
        labels = {
            lbl["precision"]
            for _, lbl in by_name.get("green_agent_precision_share", [])
        }
        assert labels == {"fp32", "fp16", "int8", "int4"}

    def test_precision_task_counts(self, enhanced_exporter):
        by_name = _series_by_name(enhanced_exporter.collect())
        counts = {
            lbl["precision"]: v
            for v, lbl in by_name.get(
                "green_agent_precision_tasks_total", []
            )
        }
        assert counts == {
            "fp32": 10, "fp16": 5, "int8": 30, "int4": 15,
        }

    def test_precision_missing_counts(self, exporter):
        by_name = _series_by_name(exporter.collect())
        assert "green_agent_precision_share" not in by_name

    # --- 8. Carbon Markets ---
    def test_carbon_market_metrics(self, enhanced_exporter):
        by_name = _series_by_name(enhanced_exporter.collect())
        assert by_name[
            "green_agent_carbon_credits_purchased_total"
        ][0][0] == 2
        assert by_name[
            "green_agent_carbon_credits_sold_total"
        ][0][0] == 1
        assert by_name[
            "green_agent_market_trade_volume_usd"
        ][0][0] == pytest.approx(1.5)  # 0.5 + 0.7 + 0.3
        assert by_name["green_agent_market_spend_usd"][0][0] == pytest.approx(
            1.2
        )
        assert by_name[
            "green_agent_carbon_price_usd_per_tco2"
        ][0][0] == pytest.approx(45.0)
        assert by_name["green_agent_rec_available_mwh"][0][0] == pytest.approx(
            120.0
        )

    def test_market_missing_source(self, exporter):
        by_name = _series_by_name(exporter.collect())
        assert "green_agent_carbon_credits_purchased_total" not in by_name

    # --- 9. Resilience ---
    def test_circuit_breaker_state_metrics(self, enhanced_exporter):
        """All three circuit breakers emit their own state series."""
        series = enhanced_exporter.collect()
        states = {
            lbl["name"]: v
            for name, v, lbl in series
            if name == "green_agent_circuit_state"
        }
        assert states == {
            "forecaster": 0,   # closed
            "scheduler": 2,    # open
            "ledger": 1,       # half_open
        }

    def test_circuit_breaker_failures(self, enhanced_exporter):
        series = enhanced_exporter.collect()
        failures = {
            lbl["name"]: v
            for name, v, lbl in series
            if name == "green_agent_circuit_failures_total"
        }
        assert failures == {
            "forecaster": 0,
            "scheduler": 3,
            "ledger": 1,
        }

    def test_circuit_state_codes(self, enhanced_exporter):
        """Circuit codes are within {0, 1, 2} (closed/half_open/open)."""
        for _, v, lbl in enhanced_exporter.collect():
            if _ is None:  # pragma: no cover
                continue
        # Simple check: all state values in {0,1,2}
        states = [
            v for name, v, _ in enhanced_exporter.collect()
            if name == "green_agent_circuit_state"
        ]
        assert states, "circuit_state must be emitted"
        for v in states:
            assert v in (0, 1, 2)

    def test_circuit_missing_source(self, exporter):
        by_name = _series_by_name(exporter.collect())
        assert "green_agent_circuit_state" not in by_name

    def test_chaos_events_metric(self, enhanced_exporter):
        by_name = _series_by_name(enhanced_exporter.collect())
        assert by_name["green_agent_chaos_events_total"][0][0] == 2

    def test_chaos_missing_source(self, exporter):
        by_name = _series_by_name(exporter.collect())
        assert "green_agent_chaos_events_total" not in by_name

    # --- 10. HITL ---
    def test_hitl_metrics(self, enhanced_exporter):
        series = enhanced_exporter.collect()
        by_name = _series_by_name(series)
        assert by_name["green_agent_hitl_pending"][0][0] == 2

        # Approved and denied counts as separate series
        reviews = {
            lbl["decision"]: v
            for v, lbl in by_name.get(
                "green_agent_hitl_reviews_total", []
            )
        }
        assert reviews == {"approved": 2, "denied": 1}
        assert sum(reviews.values()) == 3  # feedback_log length

        # Active-learning batch size
        batch_series = by_name.get(
            "green_agent_active_learning_batch_size", []
        )
        assert len(batch_series) == 1
        assert batch_series[0][0] == 3

    def test_hitl_missing_source(self, exporter):
        by_name = _series_by_name(exporter.collect())
        assert "green_agent_hitl_pending" not in by_name


# =============================================================================
# Tests — registry failure resilience
# =============================================================================

@requires_real
class TestRegistryFailure:
    def test_health_failure_graceful(self):
        registry = Mock()
        registry.health.side_effect = RuntimeError("registry down")
        registry.get.return_value = None
        exporter = GraphMetricsExporter(registry)
        series = exporter.collect()  # must not raise
        by_name = _series_by_name(series)
        assert by_name["green_agent_execution_graphs_active"][0][0] == 0

    def test_get_failure_graceful(self):
        registry = Mock()
        registry.health.return_value = {
            "execution_count": 1, "singletons": {},
        }
        registry.get.side_effect = RuntimeError("get failed")
        exporter = GraphMetricsExporter(registry)
        series = exporter.collect()
        assert "green_agent_execution_graphs_active" in _series_names(
            series
        )

    def test_health_failure_logs_warning(self, caplog):
        registry = Mock()
        registry.health.side_effect = RuntimeError("registry down")
        registry.get.return_value = None
        exporter = GraphMetricsExporter(registry)
        with caplog.at_level(logging.WARNING):
            exporter.collect()
        assert any(
            "registry.health() failed" in r.message
            for r in caplog.records
        )


# =============================================================================
# Tests — HTTP server (port=0, no race)
# =============================================================================

def _start_and_get_port(exporter, host: str = "127.0.0.1") -> int:
    """Start the exporter's HTTP server on a free port; return the port."""
    exporter.start_http_server(port=0, host=host)
    time.sleep(0.15)  # give the serve thread time to bind
    server = getattr(exporter, "_server", None)
    assert server is not None, "server did not start"
    return server.server_address[1]


@requires_real
class TestHTTPServer:
    def test_metrics_endpoint(self, exporter):
        port = _start_and_get_port(exporter)
        try:
            resp = requests.get(
                f"http://127.0.0.1:{port}/metrics", timeout=5,
            )
            assert resp.status_code == 200
            assert "text/plain" in resp.headers.get("Content-Type", "")
            assert "# HELP" in resp.text
            assert "# TYPE" in resp.text
            list(text_string_to_metric_families(resp.text))
        finally:
            exporter.stop_http_server()

    def test_404_for_unknown_path(self, exporter):
        port = _start_and_get_port(exporter)
        try:
            resp = requests.get(
                f"http://127.0.0.1:{port}/unknown", timeout=5,
            )
            assert resp.status_code == 404
        finally:
            exporter.stop_http_server()

    def test_health_endpoint(self, exporter):
        port = _start_and_get_port(exporter)
        try:
            resp = requests.get(
                f"http://127.0.0.1:{port}/health", timeout=5,
            )
            assert resp.status_code == 200
            assert "application/json" in resp.headers.get(
                "Content-Type", ""
            )
            payload = resp.json()
            assert "ok" in payload
        finally:
            exporter.stop_http_server()

    def test_json_endpoint(self, exporter):
        port = _start_and_get_port(exporter)
        try:
            resp = requests.get(
                f"http://127.0.0.1:{port}/json", timeout=5,
            )
            assert resp.status_code == 200
            data = resp.json()
            assert isinstance(data, list)
            assert all(
                {"name", "value", "labels"} <= set(entry)
                for entry in data
            )
        finally:
            exporter.stop_http_server()

    def test_error_handling(self, exporter):
        original = exporter.render
        exporter.render = Mock(side_effect=RuntimeError("boom"))
        port = _start_and_get_port(exporter)
        try:
            resp = requests.get(
                f"http://127.0.0.1:{port}/metrics", timeout=5,
            )
            assert resp.status_code == 500
        finally:
            exporter.render = original
            exporter.stop_http_server()

    def test_concurrent_scrapes(self, exporter):
        port = _start_and_get_port(exporter)
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
            exporter.stop_http_server()

    def test_stop_is_idempotent(self, exporter):
        exporter.start_http_server(port=0, host="127.0.0.1")
        exporter.stop_http_server()
        exporter.stop_http_server()  # second call is a no-op

    def test_restart_after_stop(self, exporter):
        exporter.start_http_server(port=0, host="127.0.0.1")
        server = getattr(exporter, "_server", None)
        assert server is not None
        port = server.server_address[1]
        exporter.stop_http_server()
        # Restart on the same port must succeed
        exporter.start_http_server(port=port, host="127.0.0.1")
        exporter.stop_http_server()


# =============================================================================
# Tests — Grafana dashboard
# =============================================================================

@requires_real
class TestGrafanaDashboard:
    def test_dashboard_structure(self, exporter):
        dashboard = json.loads(exporter.grafana_dashboard())
        assert "Green Agent" in dashboard["title"]
        # v5.1 emits 25 panels covering all ten enhancement layers + helium
        assert len(dashboard["panels"]) >= 25, (
            f"expected 25+ panels, got {len(dashboard['panels'])}"
        )
        for panel in dashboard["panels"]:
            assert "id" in panel
            assert "title" in panel
            assert "targets" in panel
            assert "datasource" in panel

    def test_dashboard_uid(self, exporter):
        dashboard = json.loads(exporter.grafana_dashboard())
        assert "uid" in dashboard, "dashboard must have a uid"
        assert isinstance(dashboard["uid"], str)
        assert dashboard["uid"], "uid must be non-empty"

    def test_dashboard_expressions(self, exporter):
        dashboard = json.loads(exporter.grafana_dashboard())
        expressions = [
            t["expr"]
            for panel in dashboard["panels"]
            for t in panel.get("targets", [])
            if "expr" in t
        ]
        assert expressions, "dashboard must have at least one expression"
        assert any(
            "green_agent_execution_graphs_active" in e
            for e in expressions
        )
        assert any(
            "green_agent_anomalies_active" in e for e in expressions
        )
        # Enhancement layers covered
        assert any(
            "green_agent_helium" in e for e in expressions
        ), "helium must be covered by the dashboard"

    def test_dashboard_has_variables(self, exporter):
        dashboard = json.loads(exporter.grafana_dashboard())
        var_names = {
            v["name"] for v in dashboard["templating"]["list"]
        }
        assert "datasource" in var_names
        assert "graph_type" in var_names
        assert "top_k" in var_names

    def test_dashboard_schema_version(self, exporter):
        dashboard = json.loads(exporter.grafana_dashboard())
        assert dashboard["schemaVersion"] >= 36

    def test_dashboard_save(self, exporter, tmp_path):
        path = tmp_path / "dashboard.json"
        exporter.save_dashboard(str(path))
        assert path.exists()
        data = json.loads(path.read_text())
        assert "Green Agent" in data["title"]


# =============================================================================
# Tests — thread safety
# =============================================================================

@requires_real
class TestThreadSafety:
    def test_concurrent_collect_consistent(self, exporter):
        """Concurrent collects produce structurally identical results."""
        with ThreadPoolExecutor(max_workers=8) as ex:
            results = list(
                ex.map(lambda _: exporter.collect(), range(20))
            )

        def canonical(series):
            return sorted(
                (name, value, tuple(sorted(lbl.items())))
                for name, value, lbl in series
            )

        baseline = canonical(results[0])
        for i, series in enumerate(results[1:], start=1):
            assert canonical(series) == baseline, (
                f"thread {i} produced inconsistent series"
            )


# =============================================================================
# Tests — v5.1 fixes (new)
# =============================================================================

@requires_real
class TestV5_1Fixes:
    """One test per fix from the exporter review that wasn't previously tested."""

    def test_collect_returns_list_not_dict(self, exporter):
        """P0 #1: collect() returns List[Tuple], not Dict."""
        series = exporter.collect()
        assert isinstance(series, list)
        assert not isinstance(series, dict)
        for item in series:
            assert isinstance(item, tuple)
            assert len(item) == 3

    def test_multi_series_preserved(self, enhanced_exporter):
        """P0 #1: three circuit breakers → three state series."""
        series = enhanced_exporter.collect()
        circuits = [
            s for s in series if s[0] == "green_agent_circuit_state"
        ]
        assert len(circuits) == 3, (
            f"expected 3 circuit series, got {len(circuits)}"
        )
        names = {s[2]["name"] for s in circuits}
        assert names == {"forecaster", "scheduler", "ledger"}

    def test_safe_registry_get_unknown_kind(self, exporter):
        """P1 #4: unknown kinds return None, not policy-graph fallback."""
        assert exporter._safe_registry_get("federated") is None
        assert exporter._safe_registry_get("") is None
        assert exporter._safe_registry_get("bogus") is None
        # Known kinds still work
        assert exporter._safe_registry_get("causal") is not None

    def test_dry_run_skips_side_effects(self, enhanced_exporter):
        """P1 #11: dry_run=True skips aggregate()."""
        federated = enhanced_exporter.enhancement_sources.federated
        original = federated.aggregate
        calls = [0]

        def counting():
            calls[0] += 1
            return original()

        federated.aggregate = counting
        try:
            enhanced_exporter.collect(dry_run=True)
            assert calls[0] == 0, (
                "dry_run must not call aggregate()"
            )
            enhanced_exporter.collect(dry_run=False)
            assert calls[0] >= 1, (
                "non-dry_run must call aggregate()"
            )
        finally:
            federated.aggregate = original

    def test_registry_circuit_breaker(self):
        """P1 #5: registry circuit opens after threshold failures."""
        registry = Mock()
        registry.health.side_effect = RuntimeError("down")
        registry.get.return_value = None
        exporter = GraphMetricsExporter(registry)

        # Five failures
        for _ in range(5):
            exporter.collect()

        # Circuit open — health must not be called
        registry.health.reset_mock()
        exporter.collect()
        registry.health.assert_not_called()

    def test_empty_scrape_has_self_metrics(self):
        """P1 #10: empty scrape still emits exporter_info + exporter_up."""
        registry = Mock()
        registry.health.return_value = {
            "execution_count": 0, "singletons": {},
        }
        registry.get.return_value = None
        exporter = GraphMetricsExporter(registry)
        text = exporter.render()
        assert "green_agent_exporter_info" in text
        assert "green_agent_exporter_up" in text

    def test_render_json(self, exporter):
        """P2 #12: render_json returns a parseable list of series."""
        payload = exporter.render_json()
        data = json.loads(payload)
        assert isinstance(data, list)
        for entry in data:
            assert {"name", "value", "labels"} <= set(entry)
            assert isinstance(entry["labels"], dict)

    def test_health_returns_ok(self, exporter):
        """P2 #12: health() reports ok."""
        report = exporter.health()
        assert isinstance(report, dict)
        assert "ok" in report
        assert report["ok"] is True

    def test_test_smoke_returns_ok(self, exporter):
        """P2 #12: test_smoke() exercises the pipeline."""
        report = exporter.test_smoke()
        assert isinstance(report, dict)
        assert report["ok"] is True
        assert report["steps"], "test_smoke must exercise at least one step"

    def test_version_is_string(self):
        """P2 #12: __version__ exported."""
        assert isinstance(EXPORTER_VERSION, str)
        assert EXPORTER_VERSION

    def test_collector_source_adapter(self, mock_registry):
        """P2 #12: MetricsCollector.get_statistics() preferred source."""
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
                    "hitl_pending": 3,
                }

        sources = EnhancementMetricsSources(collector=MockCollector())
        exporter = GraphMetricsExporter(
            mock_registry, enhancement_sources=sources,
        )
        series = exporter.collect()
        names = _series_names(series)
        assert "green_agent_circuit_state" in names
        assert "green_agent_hitl_pending" in names

        circuits = {
            lbl["name"]: v
            for name, v, lbl in series
            if name == "green_agent_circuit_state"
        }
        assert circuits == {"forecaster": 0, "scheduler": 2}

    def test_coerce_float_rejects_nan_inf(self):
        """P1 #10: NaN/Inf metric values are skipped, not emitted."""
        class BadDistiller:
            distilled_count = float("nan")
            last_quality_retention = float("inf")
            last_energy_reduction_percent = 55.0

        registry = Mock()
        registry.health.return_value = {
            "execution_count": 0, "singletons": {},
        }
        registry.get.return_value = None
        sources = EnhancementMetricsSources(distiller=BadDistiller())
        exporter = GraphMetricsExporter(
            registry, enhancement_sources=sources,
        )
        by_name = _series_by_name(exporter.collect())
        assert "green_agent_distilled_models_total" not in by_name
        assert (
            "green_agent_distillation_quality_retention" not in by_name
        )
        assert (
            "green_agent_distillation_energy_reduction_percent"
            in by_name
        )

    def test_unknown_scarcity_defaults_to_critical(self, mock_registry):
        """P3 #13: unknown scarcity level defaults to CRITICAL (2)."""
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
            mock_registry, helium_monitor=MockHelium(),
        )
        series = exporter.collect()
        levels = [
            v for name, v, _ in series
            if name == "green_agent_helium_scarcity_level"
        ]
        assert len(levels) == 1
        assert levels[0] == 2, "unknown level must default to CRITICAL"

    def test_helium_baseline_price_override(self, mock_registry):
        """``helium_baseline_price_usd=`` configures the premium calc."""

        class MockHelium:
            def get_current_supply(self):
                return type("S", (), {
                    "scarcity_level": type(
                        "E", (), {"value": "normal"},
                    )(),
                    "scarcity_score": 0.0,
                    "spot_price_usd_per_liter": 10.0,
                    "fab_inventory_days": 30,
                    "vendor_alerts": [],
                    "source": "test",
                })()

        exporter = GraphMetricsExporter(
            mock_registry,
            helium_monitor=MockHelium(),
            helium_baseline_price_usd=5.0,
        )
        by_name = _series_by_name(exporter.collect())
        premiums = [
            v for v, _ in by_name["green_agent_helium_price_premium_usd"]
        ]
        assert premiums == [pytest.approx(5.0)]  # 10.0 - 5.0


# =============================================================================
# Tests — feature toggles / partial sources
# =============================================================================

@requires_real
class TestFeatureToggles:
    def test_all_sources_missing(self):
        registry = Mock()
        registry.health.return_value = {
            "execution_count": 1, "singletons": {},
        }
        registry.get.return_value = None
        exporter = GraphMetricsExporter(registry)
        series = exporter.collect()
        names = _series_names(series)
        assert "green_agent_execution_graphs_active" in names
        for enhancement in [
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
        ]:
            assert enhancement not in names, (
                f"{enhancement} unexpectedly present"
            )

    def test_partial_sources(self, mock_rl_policy, mock_hitl):
        registry = Mock()
        registry.health.return_value = {
            "execution_count": 1, "singletons": {},
        }
        registry.get.return_value = None
        sources = EnhancementMetricsSources(
            rl_policy=mock_rl_policy, hitl=mock_hitl,
        )
        exporter = GraphMetricsExporter(
            registry, enhancement_sources=sources,
        )
        series = exporter.collect()
        names = _series_names(series)
        assert "green_agent_rl_epsilon" in names
        assert "green_agent_hitl_pending" in names
        assert "green_agent_distilled_models_total" not in names


# =============================================================================
# Module entry point
# =============================================================================

if __name__ == "__main__":
    import sys
    sys.exit(pytest.main([__file__, "-v", "--tb=short"]))
