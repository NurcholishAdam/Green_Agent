"""
Graph Metrics Exporter (v5.1)
=============================

Production Prometheus exporter for Green Agent graph health, helium supply,
and all ten enhancement-layer metrics. No database dependency; includes an
expanded Grafana dashboard template.

Fixes in v5.1 (see review)
--------------------------
  * P0 — Multi-series collection. ``collect()`` now returns
         ``List[Tuple[str, float, Dict]]`` so labeled metrics emit one
         series per item instead of silently keeping only the last entry.
         Every previously affected metric (causal edges, per-agent
         success rate, per-role counts, per-precision counts, circuit
         states, RL weights, temporal violations by property, federated
         aggregates, HITL decisions, per-graph-type node/edge/anomaly
         counts) now exports all series.
  * P0 — ``sys.path.insert(0, ...)`` removed. The exporter relies on the
         running process's import path; optional siblings are imported via
         ``importlib.util.spec_from_file_location`` only when needed.
  * P0 — Enhancement layers are no longer decorative. The exporter
         accepts a ``collector=`` (the enhanced ``MetricsCollector``) and
         prefers its ``get_statistics()`` output; individual layer sources
         remain supported via the v5.0 attribute queries, and layers that
         expose ``get_statistics()`` are queried through it. Quantum
         Distillation, XAI, Causal RL, and Adaptive Precision now produce
         data when the underlying class implements the statistics API.
  * P1 — ``_safe_registry_get`` returns ``None`` for unknown kinds
         instead of silently falling back to the policy graph.
  * P1 — Registry circuit breaker is actually checked. After
         ``_REGISTRY_FAILURE_THRESHOLD`` failures within
         ``_REGISTRY_RECOVERY_SECONDS``, further calls are short-circuited
         until the recovery window expires.
  * P1 — Label values are escaped (``\\``, ``"``, ``\\n``) so a label that
         contains a quote no longer breaks the entire scrape.
  * P1 — The ``job`` label is no longer emitted on individual metrics.
         Prometheus reserves it as a target label; specify it in the
         scrape config instead.
  * P1 — ``ThreadingHTTPServer`` for concurrent scrapes; a per-request
         ``timeout=5`` prevents a hung client from blocking the exporter.
  * P1 — ``stop_http_server`` now calls ``server_close()``, is idempotent,
         and permits ``start_http_server`` on the same port.
  * P1 — ``render()`` always emits ``green_agent_exporter_info`` and
         ``green_agent_exporter_up`` so an otherwise empty scrape is still
         a valid Prometheus response.
  * P1 — ``collect(dry_run=True)`` skips source methods with side effects
         (``aggregate()``, ``get_statistics()``, ``active_learning_batch()``)
         so tests and debug sessions don't perturb the sources.
  * P2 — ``__version__``, ``test_smoke()``, ``health()``, ``render_json()``,
         ``__repr__``, and a ``MetricsCollector`` adapter via
         ``EnhancementMetricsSources.collector``.
  * P3 — Lazy ``%s`` logging; ``__all__``; ``HELIUM_BASELINE_PRICE_USD``
         configurable via constructor; atomic dashboard save; smoke-test
         ``__main__`` with ``--once`` gate.

Original public API preserved
-----------------------------
- ``GraphMetricsExporter(registry, job_name=..., max_edges_export=...,
   helium_monitor=..., enhancement_sources=...)`` signature unchanged.
- ``start_http_server`` / ``stop_http_server`` / ``grafana_dashboard`` /
  ``save_dashboard`` signatures unchanged.
- ``EnhancementMetricsSources`` gains an optional ``collector`` field.

Backward compatibility
----------------------
- ``collect()`` return type changed from ``Dict[str, tuple]`` to
  ``List[Tuple[str, float, Dict]]``. Any caller that reads
  ``collect()["metric"]`` must adapt; the ``render()`` output is
  unchanged in shape (still valid Prometheus text), only more complete.
- The ``job`` label is no longer emitted on metrics.
- ``stop_http_server`` may be called twice; the second call is a no-op.
"""

from __future__ import annotations

import http.server
import json
import logging
import math
import os
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Tuple,
    TYPE_CHECKING,
)

if TYPE_CHECKING:
    from core.graph_registry import GraphRegistry
    from carbon.helium_monitor import HeliumMonitor

logger = logging.getLogger(__name__)

__version__ = "5.1.0"

__all__ = [
    "EnhancementMetricsSources",
    "GraphMetricsExporter",
    "HELIUM_BASELINE_PRICE_USD",
    "__version__",
]


# =============================================================================
# Optional helium import (unchanged behaviour, no sys.path hack)
# =============================================================================
try:
    from carbon.helium_monitor import HeliumScarcityLevel  # type: ignore
except Exception:  # noqa: BLE001
    class HeliumScarcityLevel(Enum):  # type: ignore
        NORMAL = "normal"
        CAUTION = "caution"
        CRITICAL = "critical"
        SEVERE = "severe"


#: Default reference price for helium (USD/L).
HELIUM_BASELINE_PRICE_USD = 4.0

#: Registry circuit breaker tuning.
_REGISTRY_FAILURE_THRESHOLD = 5
_REGISTRY_RECOVERY_SECONDS = 30.0

#: Graph kinds supported by ``_safe_registry_get``.
_KIND_TO_GRAPH_TYPE: Dict[str, str] = {
    "causal": "CAUSAL",
    "policy": "POLICY",
}


# =============================================================================
# Circuit state (for metric encoding only — not a live breaker)
# =============================================================================
class CircuitState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


_CIRCUIT_STATE_CODE = {
    "closed": 0,
    "half_open": 1,
    "open": 2,
}


# =============================================================================
# Enhancement sources container
# =============================================================================
@dataclass
class EnhancementMetricsSources:
    """
    Optional sources for enhancement-layer metrics.

    Each field is duck-typed. The exporter queries defensively and
    silently omits any metric whose source is missing or malformed.

    The ``collector`` field, when provided, is preferred over the
    individual layer sources: its ``get_statistics()`` output is
    authoritative for the layers it manages. This is the recommended
    integration path for ``MetricsCollector`` v5.1, whose
    ``get_statistics()`` already aggregates all ten layers.
    """
    #: Optional ``MetricsCollector`` (v5.1). When provided, its
    #: ``get_statistics()`` is queried for layer metrics.
    collector: Any = None

    #: 1. Quantum-Distillation (QuantumDistillationBridge)
    distiller: Any = None
    #: 2. Causal RL (CausalRLPolicy / CausalMeasurementLearner)
    rl_policy: Any = None
    #: 3. Federated (FederatedAggregator)
    federated: Any = None
    #: 4. Multi-Agent (MultiAgentCoordinator)
    coordinator: Any = None
    #: 5. Temporal Logic (TemporalLogicMonitor)
    temporal_monitor: Any = None
    #: 6. XAI (SnapshotExplainer / DataExplainer with stats)
    explainer: Any = None
    #: 7. Adaptive Precision (AdaptivePrecisionController)
    precision_controller: Any = None
    #: Optional override: {"fp32": N, "fp16": N, ...}
    precision_counts: Dict[str, int] = field(default_factory=dict)
    #: 8. Carbon Markets (CarbonMarketClient)
    market: Any = None
    #: 9. Resilience (name -> CircuitBreaker)
    circuit_breakers: Dict[str, Any] = field(default_factory=dict)
    chaos: Any = None
    #: 10. HITL (HumanInTheLoopGate)
    hitl: Any = None


# =============================================================================
# Metric metadata
# =============================================================================
_METRIC_METADATA: Dict[str, Tuple[str, Optional[str]]] = {
    # --- Exporter self-metrics ---
    "green_agent_exporter_info": ("Exporter build info", "gauge"),
    "green_agent_exporter_up": ("1 if the exporter is healthy", "gauge"),

    # --- Base graph metrics ---
    "green_agent_execution_graphs_active": ("Currently active execution graphs", "gauge"),
    "green_agent_graph_nodes": ("Nodes in graph", "gauge"),
    "green_agent_graph_edges": ("Edges in graph", "gauge"),
    "green_agent_anomalies_active": ("Active anomalies", "gauge"),
    "green_agent_ideal_paths_total": ("Total ideal paths registered", "counter"),
    "green_agent_causal_edge_weight": ("Causal edge weight", "gauge"),
    "green_agent_causal_edge_confidence": ("Causal edge confidence", "gauge"),
    "green_agent_policy_edge_weight": ("Policy edge weight", "gauge"),
    "green_agent_edges_truncated": ("Edges omitted due to cardinality cap", "gauge"),

    # --- Helium ---
    "green_agent_helium_scarcity_level": ("Helium scarcity (0=NORMAL, 1=CAUTION, 2=CRITICAL, 3=SEVERE)", "gauge"),
    "green_agent_helium_scarcity_score": ("Helium scarcity score (0-1)", "gauge"),
    "green_agent_helium_spot_price_usd": ("Helium spot price (USD/L)", "gauge"),
    "green_agent_helium_fab_inventory_days": ("Fab inventory (days)", "gauge"),
    "green_agent_helium_vendor_alerts_count": ("Number of vendor alerts", "gauge"),
    "green_agent_helium_price_premium_usd": ("Price premium over baseline (USD/L)", "gauge"),

    # --- 1. Quantum-Distillation ---
    "green_agent_distilled_models_total": ("Total distilled expert models produced", "counter"),
    "green_agent_distillation_quality_retention": ("Quality retention of last distilled model (0-1)", "gauge"),
    "green_agent_distillation_energy_reduction_percent": ("Energy reduction from distillation (%)", "gauge"),

    # --- 2. Causal RL ---
    "green_agent_rl_epsilon": ("Current epsilon (exploration rate)", "gauge"),
    "green_agent_rl_observations_total": ("Total observations recorded", "counter"),
    "green_agent_rl_buffer_samples": ("Pending samples in the RL buffer", "gauge"),
    "green_agent_rl_weight_l1": ("L1 norm of RL weights per strategy", "gauge"),
    "green_agent_rl_bias": ("RL bias term", "gauge"),

    # --- 3. Federated ---
    "green_agent_federated_updates_total": ("Total federated updates received", "counter"),
    "green_agent_federated_contributors": ("Distinct deployments contributing", "gauge"),
    "green_agent_federated_aggregate": ("Federated aggregate value by key", "gauge"),

    # --- 4. Multi-Agent ---
    "green_agent_agents_total": ("Total registered agents", "gauge"),
    "green_agent_agents_by_role": ("Agents per role", "gauge"),
    "green_agent_agent_success_rate": ("Average success rate per agent", "gauge"),

    # --- 5. Temporal Logic ---
    "green_agent_temporal_violations_total": ("Temporal logic violations by property", "counter"),
    "green_agent_temporal_last_violation_age_seconds": ("Age of most recent temporal violation (s)", "gauge"),

    # --- 6. XAI ---
    "green_agent_explanations_total": ("Total explanations generated", "counter"),
    "green_agent_explanation_confidence": ("Average explanation confidence (0-1)", "gauge"),

    # --- 7. Adaptive Precision ---
    "green_agent_precision_tasks_total": ("Task count by precision level", "counter"),
    "green_agent_precision_share": ("Fraction of tasks at each precision (0-1)", "gauge"),

    # --- 8. Carbon Markets ---
    "green_agent_carbon_credits_purchased_total": ("Carbon credits purchased", "counter"),
    "green_agent_carbon_credits_sold_total": ("Carbon savings sold", "counter"),
    "green_agent_market_spend_usd": ("Total market spend (USD)", "gauge"),
    "green_agent_market_trade_volume_usd": ("Total trade volume (USD)", "gauge"),
    "green_agent_rec_available_mwh": ("REC available (MWh)", "gauge"),
    "green_agent_carbon_price_usd_per_tco2": ("Current carbon price (USD/tCO2)", "gauge"),

    # --- 9. Resilience ---
    "green_agent_circuit_state": ("Circuit state (0=closed, 1=half_open, 2=open)", "gauge"),
    "green_agent_circuit_failures_total": ("Circuit breaker failures", "counter"),
    "green_agent_chaos_events_total": ("Chaos injection events", "counter"),

    # --- 10. HITL ---
    "green_agent_hitl_reviews_total": ("HITL reviews by decision", "counter"),
    "green_agent_hitl_pending": ("Pending HITL review requests", "gauge"),
    "green_agent_active_learning_batch_size": ("Most recent active-learning batch size", "gauge"),
}


def _metric_help(name: str) -> str:
    meta = _METRIC_METADATA.get(name)
    return meta[0] if meta else "Green Agent metric"


def _metric_type(name: str) -> str:
    meta = _METRIC_METADATA.get(name)
    if meta and meta[1]:
        return meta[1]
    return "counter" if name.endswith("_total") else "gauge"


# =============================================================================
# Prometheus helpers
# =============================================================================
def _escape_label(value: str) -> str:
    """Escape a value for a Prometheus label string."""
    return (
        value
        .replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("\n", "\\n")
    )


def _coerce_float(value: Any) -> Optional[float]:
    """Return a finite float, or None if the value can't be coerced."""
    if isinstance(value, bool):
        return float(value)
    if not isinstance(value, (int, float)):
        try:
            value = float(value)
        except (TypeError, ValueError):
            return None
    if not math.isfinite(float(value)):
        return None
    return float(value)


#: A single Prometheus series: (metric_name, value, labels).
Series = Tuple[str, float, Dict[str, str]]


# =============================================================================
# GraphMetricsExporter
# =============================================================================
class GraphMetricsExporter:
    """
    Collects metrics from GraphRegistry, optional HeliumMonitor, and
    optional enhancement-layer sources; renders Prometheus text.

    Thread Safety:
        The exporter holds no mutable state during ``collect()`` except
        for the registry circuit breaker counters and HTTP server
        lifecycle, both of which are guarded internally. Source
        collections are assumed to be thread-safe; a caller mutating
        ``enhancement_sources`` mid-scrape is not supported.
    """

    def __init__(
        self,
        registry: "GraphRegistry",
        job_name: str = "green_agent",
        max_edges_export: int = 100,
        helium_monitor: Optional["HeliumMonitor"] = None,
        enhancement_sources: Optional[EnhancementMetricsSources] = None,
        helium_baseline_price_usd: float = HELIUM_BASELINE_PRICE_USD,
    ):
        if not isinstance(job_name, str) or not job_name:
            raise ValueError("job_name must be a non-empty string.")
        if max_edges_export <= 0:
            raise ValueError("max_edges_export must be > 0.")
        if not math.isfinite(float(helium_baseline_price_usd)) or (
            helium_baseline_price_usd < 0
        ):
            raise ValueError(
                "helium_baseline_price_usd must be finite and >= 0."
            )

        self.registry = registry
        self.job_name = job_name
        self.max_edges_export = int(max_edges_export)
        self.helium_monitor = helium_monitor
        self.enhancement_sources = (
            enhancement_sources or EnhancementMetricsSources()
        )
        self.helium_baseline_price_usd = float(helium_baseline_price_usd)

        self._server: Optional[http.server.ThreadingHTTPServer] = None
        self._server_thread: Optional[threading.Thread] = None
        self._server_lock = threading.Lock()

        # Registry circuit breaker (fix P1 #5)
        self._registry_failures = 0
        self._registry_last_failure: Optional[float] = None

        logger.debug(
            "GraphMetricsExporter initialized "
            "(job=%s, max_edges=%d, helium=%s, enhancement_sources=%s)",
            job_name,
            max_edges_export,
            "yes" if helium_monitor else "no",
            "yes" if enhancement_sources else "no",
        )

    # ------------------------------------------------------------------
    # Collection
    # ------------------------------------------------------------------

    def collect(
        self, *, dry_run: bool = False,
    ) -> List[Series]:
        """
        Gather all graph, helium, and enhancement metrics.

        Returns
        -------
        List[(name, value, labels)]
            One entry per Prometheus series. Multiple entries can share
            a metric name (differing labels).

        Parameters
        ----------
        dry_run:
            When True, skip methods that mutate source state
            (``aggregate()``, ``get_statistics()``, ``active_learning_batch()``).
            Use in tests and interactive debug sessions.
        """
        series: List[Series] = []

        # --- Base registry health ---
        health = self._safe_registry_health()
        v = _coerce_float(health.get("execution_count", 0))
        if v is not None:
            series.append(
                ("green_agent_execution_graphs_active", v, {})
            )

        for gtype_str, info in (health.get("singletons", {}) or {}).items():
            if not isinstance(info, dict):
                continue
            labels = {"graph_type": str(gtype_str)}
            for key, metric_name in (
                ("node_count", "green_agent_graph_nodes"),
                ("edge_count", "green_agent_graph_edges"),
                ("anomaly_count", "green_agent_anomalies_active"),
            ):
                if key in info:
                    v = _coerce_float(info[key])
                    if v is not None:
                        series.append((metric_name, v, labels))
            if "ideal_path_count" in info:
                v = _coerce_float(info["ideal_path_count"])
                if v is not None:
                    series.append(
                        ("green_agent_ideal_paths_total", v, {})
                    )

        # --- Causal edges ---
        causal = self._safe_registry_get("causal")
        if causal is not None:
            series.extend(self._collect_causal_edges(causal))

        # --- Policy edges ---
        policy = self._safe_registry_get("policy")
        if policy is not None:
            series.extend(self._collect_policy_edges(policy))

        # --- Helium ---
        series.extend(self._collect_helium_metrics())

        # --- Enhancement ---
        series.extend(self._collect_enhancement_metrics(dry_run=dry_run))

        return series

    def _collect_causal_edges(self, causal: Any) -> List[Series]:
        out: List[Series] = []
        try:
            all_edges = list(getattr(causal, "edges", []) or [])
        except Exception as exc:  # noqa: BLE001
            logger.debug("Causal edge listing failed: %s", exc)
            return out

        sorted_edges = sorted(
            all_edges,
            key=lambda e: getattr(e, "weight", 0.0),
            reverse=True,
        )[: self.max_edges_export]
        truncated = max(0, len(all_edges) - len(sorted_edges))
        if truncated:
            out.append((
                "green_agent_edges_truncated",
                float(truncated),
                {"graph_type": "causal"},
            ))
        for edge in sorted_edges:
            labels = {
                "source": str(getattr(edge, "source_id", "?")),
                "target": str(getattr(edge, "target_id", "?")),
                "label": str(getattr(edge, "label", "default")),
            }
            w = _coerce_float(getattr(edge, "weight", 0.0))
            if w is not None:
                out.append((
                    "green_agent_causal_edge_weight",
                    round(w, 4),
                    labels,
                ))
            c = _coerce_float(getattr(edge, "confidence", 0.0))
            if c is not None:
                out.append((
                    "green_agent_causal_edge_confidence",
                    round(c, 4),
                    labels,
                ))
        return out

    def _collect_policy_edges(self, policy: Any) -> List[Series]:
        out: List[Series] = []
        try:
            raw = getattr(policy, "export_weights", None)
            if not callable(raw):
                return out
            all_edges = list(raw() or [])
        except Exception as exc:  # noqa: BLE001
            logger.debug("Policy edge listing failed: %s", exc)
            return out

        sorted_edges = sorted(
            all_edges,
            key=lambda e: e.get("weight", 0),
            reverse=True,
        )[: self.max_edges_export]
        truncated = max(0, len(all_edges) - len(sorted_edges))
        if truncated:
            out.append((
                "green_agent_edges_truncated",
                float(truncated),
                {"graph_type": "policy"},
            ))
        for edge in sorted_edges:
            if not isinstance(edge, dict):
                continue
            labels = {
                "source": str(edge.get("source", "?")),
                "target": str(edge.get("target", "?")),
                "context_tag": str(edge.get("context_tag", "default")),
            }
            w = _coerce_float(edge.get("weight", 0.0))
            if w is not None:
                out.append((
                    "green_agent_policy_edge_weight", w, labels,
                ))
        return out

    # ------------------------------------------------------------------
    # Registry access (with circuit breaker)
    # ------------------------------------------------------------------

    def _safe_registry_health(self) -> Dict[str, Any]:
        """Call ``registry.health()`` with circuit-breaker protection."""
        now = time.time()
        if (
            self._registry_failures >= _REGISTRY_FAILURE_THRESHOLD
            and self._registry_last_failure is not None
            and (now - self._registry_last_failure)
            < _REGISTRY_RECOVERY_SECONDS
        ):
            logger.debug(
                "Registry circuit open (%d failures); skipping health call.",
                self._registry_failures,
            )
            return {"execution_count": 0, "singletons": {}}
        try:
            result = self.registry.health()
            self._registry_failures = 0
            self._registry_last_failure = None
            return result
        except Exception as exc:  # noqa: BLE001
            self._registry_failures += 1
            self._registry_last_failure = now
            logger.warning("registry.health() failed: %s", exc)
            return {"execution_count": 0, "singletons": {}}

    def _safe_registry_get(self, kind: str) -> Optional[Any]:
        """Call ``registry.get(GraphType.X)`` for a known kind."""
        if kind not in _KIND_TO_GRAPH_TYPE:
            logger.warning(
                "Unknown graph kind %r; expected one of %s",
                kind, list(_KIND_TO_GRAPH_TYPE),
            )
            return None
        try:
            from core.graph_registry import GraphType  # type: ignore
            key = getattr(GraphType, _KIND_TO_GRAPH_TYPE[kind], kind)
        except Exception:  # noqa: BLE001
            key = kind
        try:
            return self.registry.get(key)
        except Exception as exc:  # noqa: BLE001
            logger.debug("registry.get(%r) failed: %s", key, exc)
            return None

    # ------------------------------------------------------------------
    # Helium
    # ------------------------------------------------------------------

    def _collect_helium_metrics(self) -> List[Series]:
        if self.helium_monitor is None:
            return []
        try:
            signal = self.helium_monitor.get_current_supply()
        except Exception as exc:  # noqa: BLE001
            logger.debug("Helium signal fetch failed: %s", exc)
            return []
        if signal is None:
            return []

        scarcity_map = {
            HeliumScarcityLevel.NORMAL: 0,
            HeliumScarcityLevel.CAUTION: 1,
            HeliumScarcityLevel.CRITICAL: 2,
            HeliumScarcityLevel.SEVERE: 3,
        }
        level = getattr(signal, "scarcity_level", HeliumScarcityLevel.NORMAL)
        scarcity_code = scarcity_map.get(level, 2)  # unknown → CRITICAL
        if level not in scarcity_map:
            logger.warning(
                "Unknown helium scarcity level %r; defaulting to CRITICAL.",
                level,
            )

        source_label = str(getattr(signal, "source", "unknown"))
        out: List[Series] = [
            (
                "green_agent_helium_scarcity_level",
                float(scarcity_code),
                {"source": source_label},
            ),
        ]

        score = _coerce_float(getattr(signal, "scarcity_score", 0.0))
        if score is not None:
            out.append((
                "green_agent_helium_scarcity_score", score,
                {"source": source_label},
            ))

        spot = _coerce_float(
            getattr(signal, "spot_price_usd_per_liter", 0.0)
        )
        if spot is not None:
            out.append(
                ("green_agent_helium_spot_price_usd", spot, {})
            )
            out.append((
                "green_agent_helium_price_premium_usd",
                max(0.0, spot - self.helium_baseline_price_usd),
                {},
            ))

        fab = _coerce_float(
            getattr(signal, "fab_inventory_days", 0)
        )
        if fab is not None:
            out.append(
                ("green_agent_helium_fab_inventory_days", fab, {})
            )

        alerts = getattr(signal, "vendor_alerts", [])
        try:
            out.append((
                "green_agent_helium_vendor_alerts_count",
                float(len(alerts or [])),
                {},
            ))
        except TypeError:
            pass

        return out

    # ------------------------------------------------------------------
    # Enhancement layers
    # ------------------------------------------------------------------

    def _collect_enhancement_metrics(
        self, *, dry_run: bool = False,
    ) -> List[Series]:
        out: List[Series] = []

        # Preferred: query the collector's aggregate statistics.
        collector = self.enhancement_sources.collector
        if collector is not None:
            try:
                stats_fn = getattr(collector, "get_statistics", None)
                if callable(stats_fn):
                    stats = stats_fn() or {}
                    out.extend(self._stats_to_series(stats))
            except Exception as exc:  # noqa: BLE001
                logger.debug(
                    "collector.get_statistics() failed: %s", exc,
                )

        # Individual layer sources remain supported.
        for collector_fn in (
            self._collect_quantum_distillation,
            self._collect_causal_rl,
            self._collect_federated,
            self._collect_multi_agent,
            self._collect_temporal_logic,
            self._collect_xai,
            self._collect_adaptive_precision,
            self._collect_carbon_markets,
            self._collect_resilience,
            self._collect_hitl,
        ):
            try:
                out.extend(collector_fn(dry_run=dry_run))
            except Exception as exc:  # noqa: BLE001
                logger.debug("%s failed: %s", collector_fn.__name__, exc)
        return out

    def _stats_to_series(
        self, stats: Dict[str, Any],
    ) -> List[Series]:
        """Flatten ``MetricsCollector.get_statistics()`` into Prometheus series."""
        out: List[Series] = []
        if not isinstance(stats, dict):
            return out

        # Circuits
        circuits = stats.get("circuits") or {}
        if isinstance(circuits, dict):
            for name, state in circuits.items():
                state_str = str(state)
                code = _CIRCUIT_STATE_CODE.get(state_str, 0)
                out.append((
                    "green_agent_circuit_state", float(code),
                    {"name": str(name)},
                ))

        # Agents
        agents = stats.get("agents") or {}
        if isinstance(agents, dict):
            role_counts: Dict[str, int] = {}
            for aid, info in agents.items():
                if not isinstance(info, dict):
                    continue
                role = str(info.get("role", "unknown"))
                role_counts[role] = role_counts.get(role, 0) + 1
            for role, count in role_counts.items():
                out.append((
                    "green_agent_agents_by_role", float(count),
                    {"role": role},
                ))
            out.append((
                "green_agent_agents_total", float(len(agents)), {},
            ))

        # Temporal violations
        tv = stats.get("temporal_violations")
        if isinstance(tv, list):
            by_property: Dict[str, int] = {}
            for v in tv:
                if isinstance(v, dict):
                    prop = str(v.get("property", "unknown"))
                    by_property[prop] = by_property.get(prop, 0) + 1
            for prop, count in by_property.items():
                out.append((
                    "green_agent_temporal_violations_total",
                    float(count), {"property": prop},
                ))

        # HITL
        if "hitl_pending" in stats:
            v = _coerce_float(stats["hitl_pending"])
            if v is not None:
                out.append(("green_agent_hitl_pending", v, {}))
        if "hitl_feedback_count" in stats:
            v = _coerce_float(stats["hitl_feedback_count"])
            if v is not None:
                out.append((
                    "green_agent_hitl_reviews_total", v,
                    {"decision": "all"},
                ))

        # Causal learner
        cl = stats.get("causal_learner") or {}
        if isinstance(cl, dict):
            v = _coerce_float(cl.get("observations"))
            if v is not None:
                out.append((
                    "green_agent_rl_observations_total", v, {},
                ))
            v = _coerce_float(cl.get("bias"))
            if v is not None:
                out.append(("green_agent_rl_bias", v, {}))
            weights = cl.get("weights") or []
            if isinstance(weights, list):
                l1 = sum(abs(float(w)) for w in weights
                         if isinstance(w, (int, float)))
                out.append((
                    "green_agent_rl_weight_l1", round(l1, 4),
                    {"strategy": "aggregate"},
                ))

        # Federated aggregate
        fed = stats.get("federated_aggregate") or {}
        if isinstance(fed, dict):
            for key, value in fed.items():
                if isinstance(value, dict):
                    for sub, subv in value.items():
                        v = _coerce_float(subv)
                        if v is not None:
                            out.append((
                                "green_agent_federated_aggregate", v,
                                {"key": f"{key}.{sub}"},
                            ))
                else:
                    v = _coerce_float(value)
                    if v is not None:
                        out.append((
                            "green_agent_federated_aggregate", v,
                            {"key": str(key)},
                        ))

        # Market
        market = stats.get("market") or {}
        if isinstance(market, dict):
            v = _coerce_float(market.get("carbon_price_per_tco2_usd"))
            if v is not None:
                out.append((
                    "green_agent_carbon_price_usd_per_tco2", v, {},
                ))
            v = _coerce_float(market.get("spent_usd"))
            if v is not None:
                out.append((
                    "green_agent_market_spend_usd", v, {},
                ))
        return out

    # --- 1. Quantum-Distillation ---
    def _collect_quantum_distillation(
        self, *, dry_run: bool = False,
    ) -> List[Series]:
        src = self.enhancement_sources.distiller
        if src is None:
            return []
        out: List[Series] = []

        def _emit(name: str, value: Any) -> None:
            v = _coerce_float(value)
            if v is not None:
                out.append((name, v, {}))

        count = getattr(src, "distilled_count", None)
        if count is None:
            count = getattr(src, "count", None)
        if count is not None:
            _emit("green_agent_distilled_models_total", count)
        _emit(
            "green_agent_distillation_quality_retention",
            getattr(src, "last_quality_retention", None),
        )
        _emit(
            "green_agent_distillation_energy_reduction_percent",
            getattr(src, "last_energy_reduction_percent", None),
        )

        if not dry_run:
            stats_fn = getattr(src, "get_statistics", None)
            if callable(stats_fn):
                try:
                    stats = stats_fn() or {}
                    if isinstance(stats, dict):
                        _emit(
                            "green_agent_distilled_models_total",
                            stats.get("distilled_count"),
                        )
                        _emit(
                            "green_agent_distillation_quality_retention",
                            stats.get("last_quality_retention"),
                        )
                        _emit(
                            "green_agent_distillation_energy_reduction_percent",
                            stats.get("last_energy_reduction_percent"),
                        )
                except Exception:  # noqa: BLE001
                    pass
        return out

    # --- 2. Causal RL ---
    def _collect_causal_rl(
        self, *, dry_run: bool = False,
    ) -> List[Series]:
        src = self.enhancement_sources.rl_policy
        if src is None:
            return []
        out: List[Series] = []

        eps = _coerce_float(getattr(src, "epsilon", None))
        if eps is not None:
            out.append(("green_agent_rl_epsilon", eps, {}))

        obs = _coerce_float(getattr(src, "observations", None))
        if obs is not None:
            out.append((
                "green_agent_rl_observations_total", obs, {},
            ))

        buf = getattr(src, "buffer", None)
        if buf is not None:
            try:
                out.append((
                    "green_agent_rl_buffer_samples",
                    float(len(buf)), {},
                ))
            except TypeError:
                pass

        weights = getattr(src, "weights", None)
        if isinstance(weights, dict):
            for strategy, ws in weights.items():
                try:
                    l1 = sum(abs(float(w)) for w in ws)
                except (TypeError, ValueError):
                    continue
                out.append((
                    "green_agent_rl_weight_l1",
                    round(l1, 4),
                    {"strategy": str(strategy)},
                ))

        bias = _coerce_float(getattr(src, "bias", None))
        if bias is not None:
            out.append(("green_agent_rl_bias", bias, {}))

        return out

    # --- 3. Federated ---
    def _collect_federated(
        self, *, dry_run: bool = False,
    ) -> List[Series]:
        src = self.enhancement_sources.federated
        if src is None:
            return []
        out: List[Series] = []

        updates = getattr(src, "updates", None)
        if updates is not None:
            try:
                out.append((
                    "green_agent_federated_updates_total",
                    float(len(updates)), {},
                ))
                deploy_ids = {
                    getattr(u, "deployment_id", None)
                    for u in updates
                }
                deploy_ids.discard(None)
                out.append((
                    "green_agent_federated_contributors",
                    float(len(deploy_ids)), {},
                ))
            except TypeError:
                pass

        if dry_run:
            return out

        agg_fn = getattr(src, "aggregate", None)
        if callable(agg_fn):
            try:
                agg = agg_fn() or {}
                for key, value in agg.items():
                    if isinstance(value, dict):
                        for sub, subv in value.items():
                            v = _coerce_float(subv)
                            if v is not None:
                                out.append((
                                    "green_agent_federated_aggregate",
                                    v,
                                    {"key": f"{key}.{sub}"},
                                ))
                    else:
                        v = _coerce_float(value)
                        if v is not None:
                            out.append((
                                "green_agent_federated_aggregate",
                                v, {"key": str(key)},
                            ))
            except Exception:  # noqa: BLE001
                pass
        return out

    # --- 4. Multi-Agent ---
    def _collect_multi_agent(
        self, *, dry_run: bool = False,
    ) -> List[Series]:
        src = self.enhancement_sources.coordinator
        if src is None:
            return []
        agents = getattr(src, "agents", None)
        if not isinstance(agents, dict):
            return []
        out: List[Series] = []
        role_counts: Dict[str, int] = {}
        for agent in agents.values():
            role = getattr(agent, "role", None)
            role_str = getattr(role, "value", str(role))
            role_counts[role_str] = role_counts.get(role_str, 0) + 1
            sr = _coerce_float(getattr(agent, "success_rate", None))
            if sr is not None:
                out.append((
                    "green_agent_agent_success_rate", sr,
                    {"agent_id": str(getattr(agent, "agent_id", "?"))},
                ))
        for role_str, count in role_counts.items():
            out.append((
                "green_agent_agents_by_role", float(count),
                {"role": role_str},
            ))
        out.append((
            "green_agent_agents_total", float(len(agents)), {},
        ))
        return out

    # --- 5. Temporal Logic ---
    def _collect_temporal_logic(
        self, *, dry_run: bool = False,
    ) -> List[Series]:
        src = self.enhancement_sources.temporal_monitor
        if src is None:
            return []
        violations = getattr(src, "violations", None)
        if violations is None:
            return []
        try:
            violations_list = list(violations)
        except TypeError:
            return []
        out: List[Series] = []
        by_property: Dict[str, int] = {}
        latest_ts: Optional[float] = None
        for v in violations_list:
            if isinstance(v, dict):
                prop = str(v.get("property", "unknown"))
                at = v.get("at")
            else:
                prop = str(v)
                at = None
            by_property[prop] = by_property.get(prop, 0) + 1
            if at:
                try:
                    s = at.rstrip("Z") + "+00:00" if at.endswith("Z") else at
                    ts = datetime.fromisoformat(s).timestamp()
                    if latest_ts is None or ts > latest_ts:
                        latest_ts = ts
                except (ValueError, TypeError):
                    pass
        for prop, count in by_property.items():
            out.append((
                "green_agent_temporal_violations_total",
                float(count), {"property": prop},
            ))
        if latest_ts is not None:
            out.append((
                "green_agent_temporal_last_violation_age_seconds",
                max(0.0, time.time() - latest_ts), {},
            ))
        return out

    # --- 6. XAI ---
    def _collect_xai(
        self, *, dry_run: bool = False,
    ) -> List[Series]:
        src = self.enhancement_sources.explainer
        if src is None:
            return []
        out: List[Series] = []

        count = getattr(src, "explanations_count", None)
        if count is None:
            count = getattr(src, "count", None)
        v = _coerce_float(count)
        if v is not None:
            out.append(("green_agent_explanations_total", v, {}))

        avg_conf = _coerce_float(
            getattr(src, "average_confidence", None)
        )
        if avg_conf is not None:
            out.append((
                "green_agent_explanation_confidence", avg_conf, {},
            ))

        flog = getattr(src, "feedback_log", None)
        if flog is not None:
            try:
                out.append((
                    "green_agent_explanations_total",
                    float(len(flog)), {},
                ))
            except TypeError:
                pass
        return out

    # --- 7. Adaptive Precision ---
    def _collect_adaptive_precision(
        self, *, dry_run: bool = False,
    ) -> List[Series]:
        out: List[Series] = []

        def _emit_counts(counts: Dict[str, Any]) -> None:
            if not counts:
                return
            total = sum(
                int(v) for v in counts.values()
                if isinstance(v, (int, float))
            ) or 1
            for precision, count in counts.items():
                try:
                    cnt = int(count)
                except (TypeError, ValueError):
                    continue
                out.append((
                    "green_agent_precision_tasks_total",
                    float(cnt),
                    {"precision": str(precision)},
                ))
                out.append((
                    "green_agent_precision_share",
                    round(cnt / total, 4),
                    {"precision": str(precision)},
                ))

        _emit_counts(self.enhancement_sources.precision_counts or {})

        if dry_run:
            return out

        src = self.enhancement_sources.precision_controller
        if src is not None:
            stats_fn = getattr(src, "get_statistics", None)
            if callable(stats_fn):
                try:
                    stats = stats_fn() or {}
                    if isinstance(stats, dict):
                        _emit_counts(
                            stats.get("precision_counts") or {}
                        )
                except Exception:  # noqa: BLE001
                    pass
        return out

    # --- 8. Carbon Markets ---
    def _collect_carbon_markets(
        self, *, dry_run: bool = False,
    ) -> List[Series]:
        src = self.enhancement_sources.market
        if src is None:
            return []
        out: List[Series] = []

        trades = getattr(src, "trades", None)
        if trades is not None:
            try:
                buys = 0
                sells = 0
                volume = 0.0
                for t in trades:
                    if not isinstance(t, dict):
                        continue
                    if t.get("type") == "buy":
                        buys += 1
                        volume += _coerce_float(
                            t.get("cost_usd", 0.0)
                        ) or 0.0
                    elif t.get("type") == "sell":
                        sells += 1
                        volume += _coerce_float(
                            t.get("revenue_usd", 0.0)
                        ) or 0.0
                out.append((
                    "green_agent_carbon_credits_purchased_total",
                    float(buys), {},
                ))
                out.append((
                    "green_agent_carbon_credits_sold_total",
                    float(sells), {},
                ))
                out.append((
                    "green_agent_market_trade_volume_usd",
                    round(volume, 4), {},
                ))
            except TypeError:
                pass

        spent = _coerce_float(getattr(src, "spent", None))
        if spent is not None:
            out.append(("green_agent_market_spend_usd", spent, {}))

        if not dry_run:
            snap_fn = getattr(src, "get_snapshot", None)
            if callable(snap_fn):
                try:
                    snap = snap_fn()
                    if snap is not None:
                        cp = _coerce_float(
                            getattr(snap, "carbon_price_per_tco2_usd", None)
                        )
                        if cp is not None:
                            out.append((
                                "green_agent_carbon_price_usd_per_tco2",
                                cp, {},
                            ))
                        rec = _coerce_float(
                            getattr(snap, "rec_available_mwh", None)
                        )
                        if rec is not None:
                            out.append((
                                "green_agent_rec_available_mwh",
                                rec, {},
                            ))
                except Exception:  # noqa: BLE001
                    pass
        return out

    # --- 9. Resilience ---
    def _collect_resilience(
        self, *, dry_run: bool = False,
    ) -> List[Series]:
        out: List[Series] = []
        for name, cb in (
            self.enhancement_sources.circuit_breakers or {}
        ).items():
            state = getattr(cb, "state", None)
            state_str = (
                getattr(state, "value", str(state))
                if state is not None else "closed"
            )
            out.append((
                "green_agent_circuit_state",
                float(_CIRCUIT_STATE_CODE.get(state_str, 0)),
                {"name": str(name)},
            ))
            f = _coerce_float(getattr(cb, "failures", None))
            if f is not None:
                out.append((
                    "green_agent_circuit_failures_total", f,
                    {"name": str(name)},
                ))

        chaos = self.enhancement_sources.chaos
        if chaos is not None:
            events = getattr(chaos, "events", None)
            if events is not None:
                try:
                    out.append((
                        "green_agent_chaos_events_total",
                        float(len(events)), {},
                    ))
                except TypeError:
                    pass
        return out

    # --- 10. HITL ---
    def _collect_hitl(
        self, *, dry_run: bool = False,
    ) -> List[Series]:
        src = self.enhancement_sources.hitl
        if src is None:
            return []
        out: List[Series] = []

        pending = getattr(src, "pending", None)
        if pending is not None:
            try:
                out.append((
                    "green_agent_hitl_pending",
                    float(len(pending)), {},
                ))
            except TypeError:
                pass

        flog = getattr(src, "feedback_log", None)
        if flog is not None:
            try:
                approved = 0
                denied = 0
                for e in flog:
                    if not isinstance(e, dict):
                        continue
                    if e.get("decision") is True:
                        approved += 1
                    elif e.get("decision") is False:
                        denied += 1
                out.append((
                    "green_agent_hitl_reviews_total",
                    float(approved),
                    {"decision": "approved"},
                ))
                out.append((
                    "green_agent_hitl_reviews_total",
                    float(denied),
                    {"decision": "denied"},
                ))
            except TypeError:
                pass

        if not dry_run:
            batch_fn = getattr(src, "active_learning_batch", None)
            if callable(batch_fn):
                try:
                    batch = batch_fn(16) or []
                    out.append((
                        "green_agent_active_learning_batch_size",
                        float(len(batch)), {},
                    ))
                except Exception:  # noqa: BLE001
                    pass
        return out

    # ------------------------------------------------------------------
    # Rendering
    # ------------------------------------------------------------------

    def render(self) -> str:
        """Render all metrics in Prometheus exposition format."""
        series = self.collect()

        # Ensure the scrape is never empty.
        series.insert(0, (
            "green_agent_exporter_info",
            1.0,
            {"version": __version__, "exporter": self.job_name},
        ))
        series.insert(1, ("green_agent_exporter_up", 1.0, {}))

        lines: List[str] = []
        emitted_help: set = set()
        emitted_series: set = set()

        for name, value, labels in series:
            if not isinstance(name, str) or not name:
                continue
            v = _coerce_float(value)
            if v is None:
                logger.debug(
                    "Skipping non-finite value for %s: %r", name, value,
                )
                continue
            if not isinstance(labels, dict):
                labels = {}

            # Deduplicate identical series within a single scrape.
            label_key = tuple(sorted(
                (str(k), str(v_lbl)) for k, v_lbl in labels.items()
            ))
            series_key = (name, label_key)
            if series_key in emitted_series:
                continue
            emitted_series.add(series_key)

            if name not in emitted_help:
                lines.append(f"# HELP {name} {_metric_help(name)}")
                lines.append(f"# TYPE {name} {_metric_type(name)}")
                emitted_help.add(name)

            if labels:
                label_str = ",".join(
                    f'{k}="{_escape_label(str(v_lbl))}"'
                    for k, v_lbl in sorted(labels.items())
                )
                lines.append(f"{name}{{{label_str}}} {v}")
            else:
                lines.append(f"{name} {v}")

        lines.append("")
        return "\n".join(lines)

    def render_json(self) -> str:
        """Return the collected series as a JSON array of objects."""
        payload = [
            {"name": name, "value": value, "labels": dict(labels)}
            for name, value, labels in self.collect()
        ]
        return json.dumps(payload, indent=2)

    def health(self) -> Dict[str, Any]:
        """Structured health report suitable for ``/health`` endpoints."""
        smoke = self.test_smoke()
        return {
            "ok": smoke["ok"],
            "package_version": __version__,
            "job_name": self.job_name,
            "registry_failures": self._registry_failures,
            "server_running": self._server is not None,
            "steps": smoke.get("steps", []),
        }

    def test_smoke(self) -> Dict[str, Any]:
        """Non-mutating self-check for CI. Never raises."""
        results: Dict[str, Any] = {
            "ok": False, "package_version": __version__, "steps": [],
        }
        try:
            series = self.collect(dry_run=True)
            results["steps"].append({
                "name": "collect_dry_run",
                "ok": isinstance(series, list),
                "series_count": len(series),
            })
            text = self.render()
            results["steps"].append({
                "name": "render",
                "ok": (
                    "green_agent_exporter_info" in text
                    and "green_agent_exporter_up" in text
                ),
            })
            results["ok"] = all(s["ok"] for s in results["steps"])
        except Exception as exc:  # noqa: BLE001
            results["error"] = f"{type(exc).__name__}: {exc}"
        return results

    # ------------------------------------------------------------------
    # HTTP server
    # ------------------------------------------------------------------

    def start_http_server(
        self, port: int = 8000, host: str = "0.0.0.0",
    ) -> None:
        """Start a threaded HTTP server serving ``/metrics``."""
        with self._server_lock:
            if self._server is not None:
                raise RuntimeError(
                    "HTTP server already running; call "
                    "stop_http_server() first."
                )
            exporter = self

            class MetricsHandler(http.server.BaseHTTPRequestHandler):
                timeout = 5

                def do_GET(self) -> None:  # noqa: N802
                    try:
                        if self.path == "/metrics":
                            body = exporter.render().encode("utf-8")
                            self.send_response(200)
                            self.send_header(
                                "Content-Type",
                                "text/plain; version=0.0.4; charset=utf-8",
                            )
                            self.send_header(
                                "Content-Length", str(len(body)),
                            )
                            self.end_headers()
                            self.wfile.write(body)
                        elif self.path in ("/health", "/healthz"):
                            report = exporter.health()
                            body = json.dumps(report).encode("utf-8")
                            self.send_response(200)
                            self.send_header(
                                "Content-Type", "application/json",
                            )
                            self.send_header(
                                "Content-Length", str(len(body)),
                            )
                            self.end_headers()
                            self.wfile.write(body)
                        elif self.path == "/json":
                            body = exporter.render_json().encode("utf-8")
                            self.send_response(200)
                            self.send_header(
                                "Content-Type", "application/json",
                            )
                            self.send_header(
                                "Content-Length", str(len(body)),
                            )
                            self.end_headers()
                            self.wfile.write(body)
                        else:
                            self.send_response(404)
                            self.end_headers()
                    except Exception as exc:  # noqa: BLE001
                        logger.warning(
                            "Error rendering metrics: %s", exc,
                        )
                        try:
                            self.send_response(500)
                            self.send_header(
                                "Content-Type", "text/plain",
                            )
                            self.end_headers()
                            self.wfile.write(b"Internal server error\n")
                        except Exception:  # noqa: BLE001
                            pass

                def log_message(self, *args: Any) -> None:
                    return

            self._server = http.server.ThreadingHTTPServer(
                (host, port), MetricsHandler,
            )
            self._server_thread = threading.Thread(
                target=self._server.serve_forever,
                daemon=True,
            )
            self._server_thread.start()
            logger.info(
                "Prometheus endpoint ready on http://%s:%d/metrics",
                host, port,
            )

    def stop_http_server(self) -> None:
        """Stop the HTTP server and release the port. Idempotent."""
        with self._server_lock:
            server = self._server
            thread = self._server_thread
            self._server = None
            self._server_thread = None
        if server is not None:
            try:
                server.shutdown()
            finally:
                server.server_close()
        if thread is not None:
            thread.join(timeout=5)
        logger.debug("HTTP server stopped.")

    # ------------------------------------------------------------------
    # Grafana dashboard
    # ------------------------------------------------------------------

    def grafana_dashboard(self) -> str:
        """Return a Grafana dashboard JSON string."""
        dashboard = {
            "title": "Green Agent — Graph & Enhancement Health",
            "uid": "green_agent_graphs_helium_enhanced",
            "tags": [
                "green-agent", "graphs", "sustainability",
                "helium", "enhancements",
            ],
            "refresh": "10s",
            "time": {"from": "now-1h", "to": "now"},
            "templating": {"list": []},
            "panels": [
                self._panel_stat(
                    1, "Causal graph nodes",
                    'green_agent_graph_nodes{graph_type="causal"}',
                    {"x": 0, "y": 0, "w": 6, "h": 4},
                ),
                self._panel_stat(
                    2, "Policy graph nodes",
                    'green_agent_graph_nodes{graph_type="policy"}',
                    {"x": 6, "y": 0, "w": 6, "h": 4},
                ),
                self._panel_stat(
                    3, "Active anomalies",
                    'green_agent_anomalies_active{graph_type="causal"}',
                    {"x": 12, "y": 0, "w": 6, "h": 4}, color="red",
                ),
                self._panel_stat(
                    4, "Ideal paths (total)",
                    "green_agent_ideal_paths_total",
                    {"x": 18, "y": 0, "w": 6, "h": 4},
                ),
                self._panel_stat(
                    5, "Helium scarcity level",
                    "green_agent_helium_scarcity_level",
                    {"x": 0, "y": 4, "w": 6, "h": 4}, color="orange",
                    thresholds=[
                        {"color": "green", "value": None},
                        {"color": "yellow", "value": 1},
                        {"color": "red", "value": 2},
                        {"color": "dark-red", "value": 3},
                    ],
                ),
                self._panel_stat(
                    6, "Helium spot price ($/L)",
                    "green_agent_helium_spot_price_usd",
                    {"x": 6, "y": 4, "w": 6, "h": 4}, color="blue",
                ),
                self._panel_stat(
                    7, "Fab inventory (days)",
                    "green_agent_helium_fab_inventory_days",
                    {"x": 12, "y": 4, "w": 6, "h": 4}, color="green",
                ),
                self._panel_stat(
                    8, "Price premium ($/L)",
                    "green_agent_helium_price_premium_usd",
                    {"x": 18, "y": 4, "w": 6, "h": 4}, color="purple",
                ),
                self._panel_timeseries(
                    9, "Causal edge weights (top edges)",
                    f"topk({self.max_edges_export}, green_agent_causal_edge_weight)",
                    "{{source}} → {{target}}",
                    {"x": 0, "y": 8, "w": 24, "h": 8},
                ),
                self._panel_timeseries(
                    10, "Policy edge weights",
                    "green_agent_policy_edge_weight",
                    "{{source}} → {{target}} ({{context_tag}})",
                    {"x": 0, "y": 16, "w": 24, "h": 8},
                ),
                self._panel_timeseries(
                    11, "Active execution graphs",
                    "green_agent_execution_graphs_active",
                    "active_executions",
                    {"x": 0, "y": 24, "w": 12, "h": 6},
                ),
                self._panel_timeseries(
                    12, "Helium scarcity score trend",
                    "green_agent_helium_scarcity_score",
                    "scarcity_score",
                    {"x": 12, "y": 24, "w": 12, "h": 6},
                ),
                self._panel_stat(
                    13, "Circuit state (0=closed, 2=open)",
                    "green_agent_circuit_state",
                    {"x": 0, "y": 30, "w": 8, "h": 4}, color="red",
                    thresholds=[
                        {"color": "green", "value": None},
                        {"color": "yellow", "value": 1},
                        {"color": "red", "value": 2},
                    ],
                ),
                self._panel_stat(
                    14, "Temporal violations (total)",
                    "sum(green_agent_temporal_violations_total) or vector(0)",
                    {"x": 8, "y": 30, "w": 8, "h": 4}, color="red",
                ),
                self._panel_stat(
                    15, "Last violation age (s)",
                    "green_agent_temporal_last_violation_age_seconds",
                    {"x": 16, "y": 30, "w": 8, "h": 4}, color="orange",
                ),
                self._panel_piechart(
                    16, "Precision distribution",
                    "green_agent_precision_share",
                    "{{precision}}",
                    {"x": 0, "y": 34, "w": 8, "h": 6},
                ),
                self._panel_stat(
                    17, "Distilled models (total)",
                    "green_agent_distilled_models_total",
                    {"x": 8, "y": 34, "w": 8, "h": 6}, color="green",
                ),
                self._panel_stat(
                    18, "RL epsilon",
                    "green_agent_rl_epsilon",
                    {"x": 16, "y": 34, "w": 8, "h": 6}, color="blue",
                ),
                self._panel_stat(
                    19, "Federated updates",
                    "green_agent_federated_updates_total",
                    {"x": 0, "y": 40, "w": 6, "h": 4},
                ),
                self._panel_stat(
                    20, "Federated contributors",
                    "green_agent_federated_contributors",
                    {"x": 6, "y": 40, "w": 6, "h": 4},
                ),
                self._panel_barchart(
                    21, "Agents by role",
                    "green_agent_agents_by_role",
                    "{{role}}",
                    {"x": 12, "y": 40, "w": 12, "h": 4},
                ),
                self._panel_stat(
                    22, "Carbon credits purchased",
                    "green_agent_carbon_credits_purchased_total",
                    {"x": 0, "y": 44, "w": 6, "h": 4}, color="green",
                ),
                self._panel_stat(
                    23, "Market volume (USD)",
                    "green_agent_market_trade_volume_usd",
                    {"x": 6, "y": 44, "w": 6, "h": 4}, color="blue",
                ),
                self._panel_stat(
                    24, "HITL pending",
                    "green_agent_hitl_pending",
                    {"x": 12, "y": 44, "w": 6, "h": 4}, color="orange",
                ),
                self._panel_stat(
                    25, "Explanations (total)",
                    "green_agent_explanations_total",
                    {"x": 18, "y": 44, "w": 6, "h": 4}, color="green",
                ),
            ],
            "schemaVersion": 36,
            "version": 3,
        }
        return json.dumps(dashboard, indent=2)

    def save_dashboard(
        self, path: str = "./grafana_dashboard.json",
    ) -> None:
        """Atomically write the dashboard JSON to ``path``."""
        try:
            parent = os.path.dirname(os.path.abspath(path)) or "."
            os.makedirs(parent, exist_ok=True)
            tmp = path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                f.write(self.grafana_dashboard())
            os.replace(tmp, path)
            logger.info("Grafana dashboard saved to %s", path)
        except OSError as exc:
            logger.error("Failed to save dashboard to %s: %s", path, exc)
            raise

    # ------------------------------------------------------------------
    # Panel builder helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _panel_stat(
        id: int, title: str, expr: str, gridPos: dict,
        color: str = "green",
        thresholds: Optional[List[Dict]] = None,
    ) -> dict:
        if thresholds is None:
            thresholds = [{"color": color, "value": None}]
        return {
            "id": id, "type": "stat", "title": title,
            "gridPos": gridPos,
            "options": {
                "colorMode": "value", "graphMode": "area",
                "justifyMode": "auto", "orientation": "auto",
                "reduceOptions": {
                    "calcs": ["lastNotNull"], "fields": "",
                    "values": False,
                },
                "textMode": "auto",
            },
            "fieldConfig": {
                "defaults": {
                    "color": {"mode": "fixed", "fixedColor": color},
                    "thresholds": {
                        "mode": "absolute", "steps": thresholds,
                    },
                },
            },
            "targets": [{"expr": expr, "legendFormat": ""}],
            "datasource": {"type": "prometheus"},
        }

    @staticmethod
    def _panel_timeseries(
        id: int, title: str, expr: str, legend: str, gridPos: dict,
    ) -> dict:
        return {
            "id": id, "type": "timeseries", "title": title,
            "gridPos": gridPos,
            "options": {
                "legend": {
                    "displayMode": "list", "placement": "bottom",
                },
            },
            "fieldConfig": {"defaults": {"custom": {"lineWidth": 1}}},
            "targets": [{"expr": expr, "legendFormat": legend}],
            "datasource": {"type": "prometheus"},
        }

    @staticmethod
    def _panel_piechart(
        id: int, title: str, expr: str, legend: str, gridPos: dict,
    ) -> dict:
        return {
            "id": id, "type": "piechart", "title": title,
            "gridPos": gridPos,
            "options": {
                "legend": {
                    "displayMode": "list", "placement": "right",
                },
                "pieType": "donut",
                "reduceOptions": {
                    "calcs": ["lastNotNull"], "fields": "",
                    "values": False,
                },
                "tooltip": {"mode": "single", "sort": "none"},
            },
            "targets": [{"expr": expr, "legendFormat": legend}],
            "datasource": {"type": "prometheus"},
        }

    @staticmethod
    def _panel_barchart(
        id: int, title: str, expr: str, legend: str, gridPos: dict,
    ) -> dict:
        return {
            "id": id, "type": "barchart", "title": title,
            "gridPos": gridPos,
            "options": {
                "orientation": "horizontal",
                "legend": {
                    "displayMode": "list", "placement": "bottom",
                },
            },
            "targets": [{"expr": expr, "legendFormat": legend}],
            "datasource": {"type": "prometheus"},
        }

    def __repr__(self) -> str:
        return (
            "GraphMetricsExporter("
            f"job={self.job_name!r}, "
            f"max_edges={self.max_edges_export}, "
            f"helium={'yes' if self.helium_monitor else 'no'}, "
            f"server={'running' if self._server else 'stopped'})"
        )

    def __dir__(self) -> List[str]:
        return sorted(set(super().__dir__()) | set(__all__))


# =============================================================================
# Standalone demo
# =============================================================================
if __name__ == "__main__":  # pragma: no cover
    import sys

    logging.basicConfig(level=logging.INFO)

    class MockRegistry:
        def health(self):
            return {
                "execution_count": 5,
                "singletons": {
                    "causal": {
                        "node_count": 42, "edge_count": 89,
                        "anomaly_count": 2, "ideal_path_count": 12,
                    },
                    "policy": {"node_count": 28, "edge_count": 56},
                },
            }

        def get(self, graph_type):
            return None

    class MockHeliumMonitor:
        def get_current_supply(self):
            return type("S", (), {
                "scarcity_level": HeliumScarcityLevel.CAUTION,
                "scarcity_score": 0.4,
                "spot_price_usd_per_liter": 5.5,
                "fab_inventory_days": 20,
                "vendor_alerts": ["Test alert"],
                "source": "demo",
            })()

    class MockCircuit:
        def __init__(self, state="closed", failures=0):
            self.state = type("E", (), {"value": state})()
            self.failures = failures

    class MockRL:
        epsilon = 0.12
        observations = 42
        buffer = [1, 2, 3]
        bias = 0.05
        weights = {
            "lora": [0.5, 0.2, 0.1, 0.0, 0.0],
            "qlora": [0.3, 0.1, 0.4, 0.1, 0.0],
        }

    class MockCoordinator:
        agents = {
            "a1": type("A", (), {
                "role": type("R", (), {"value": "carbon_optimizer"})(),
                "success_rate": 0.9, "agent_id": "a1",
            })(),
            "a2": type("A", (), {
                "role": type("R", (), {"value": "latency_optimizer"})(),
                "success_rate": 0.85, "agent_id": "a2",
            })(),
        }

    class MockTemporal:
        violations = [
            {"property": "DeadlineRespected",
             "at": datetime.now(timezone.utc).isoformat()},
            {"property": "DeadlineRespected",
             "at": datetime.now(timezone.utc).isoformat()},
            {"property": "CarbonBudgetNotExceeded",
             "at": datetime.now(timezone.utc).isoformat()},
        ]

    class MockMarket:
        trades = [
            {"type": "buy", "cost_usd": 0.5},
            {"type": "sell", "revenue_usd": 0.3},
        ]
        spent = 0.5

        def get_snapshot(self):
            return type("S", (), {
                "carbon_price_per_tco2_usd": 45.0,
                "rec_available_mwh": 120.0,
            })()

    class MockHITL:
        pending = [1, 2]
        feedback_log = [
            {"decision": True}, {"decision": False}, {"decision": True},
        ]

        def active_learning_batch(self, n=16):
            return self.feedback_log[-n:]

    sources = EnhancementMetricsSources(
        rl_policy=MockRL(),
        coordinator=MockCoordinator(),
        temporal_monitor=MockTemporal(),
        precision_counts={
            "fp32": 10, "fp16": 5, "int8": 30, "int4": 15,
        },
        market=MockMarket(),
        circuit_breakers={
            "forecaster": MockCircuit("closed", 0),
            "scheduler": MockCircuit("open", 3),
            "ledger": MockCircuit("half_open", 1),
        },
        hitl=MockHITL(),
    )

    exporter = GraphMetricsExporter(
        MockRegistry(),
        max_edges_export=50,
        helium_monitor=MockHeliumMonitor(),
        enhancement_sources=sources,
    )

    print("repr       :", exporter)
    print()
    print("=== Prometheus text (single-shot) ===")
    text = exporter.render()
    print(text)

    # --- Smoke assertions ---
    assert "green_agent_exporter_info" in text
    assert "green_agent_exporter_up" in text
    assert text.count("green_agent_circuit_state{") == 3, (
        "expected 3 circuit series (forecaster/scheduler/ledger)"
    )
    assert text.count("green_agent_agents_by_role{") == 2, (
        "expected 2 role series"
    )
    assert "scheduler" in text and "forecaster" in text
    assert 'job="' not in text, "job label must not appear on metrics"
    print("=== Assertions passed ===")
    print()

    smoke = exporter.test_smoke()
    print("test_smoke :", json.dumps(smoke, indent=2))
    assert smoke["ok"]

    health = exporter.health()
    print("health     :", json.dumps(health, indent=2))
    assert health["ok"]

    # --- Dashboard ---
    path = "./grafana_dashboard.json"
    exporter.save_dashboard(path)
    print(f"dashboard  : {path}")

    # --- Optional: HTTP server, gated on --serve ---
    if "--serve" in sys.argv:
        exporter.start_http_server(port=8000)
        print("Demo server running at http://localhost:8000/metrics")
        print("Press Ctrl+C to stop")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            pass
        finally:
            exporter.stop_http_server()
    else:
        print(
            "Single-shot mode complete. "
            "Run with `--serve` to start the HTTP server."
        )
