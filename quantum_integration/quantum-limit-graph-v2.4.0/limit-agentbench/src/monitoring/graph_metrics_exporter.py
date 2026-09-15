"""
graph_metrics_exporter.py — Production Metrics Exporter for Green Agent (Enhanced)
==================================================================================

Exposes Green Agent graph health, helium supply, and all ten enhancement-layer
metrics in Prometheus text format. Works with any Prometheus scrape config;
no database dependency. Includes an expanded Grafana dashboard template.

File Location: src/monitoring/graph_metrics_exporter.py

Two modes:
1. HTTP server mode (production):
       exporter = GraphMetricsExporter(registry, enhancement_sources=sources)
       exporter.start_http_server(port=8000)
   Prometheus scrapes http://localhost:8000/metrics on its interval.

2. Single-shot mode (CI / testing):
       text = exporter.render()
       dashboard = exporter.grafana_dashboard()

Grafana dashboard:
    exporter.save_dashboard("./grafana_dashboard.json")
Import via Grafana UI: Dashboards → Import → Upload JSON file.

Pre-configured panels:
  • Graph node/edge counts (causal, policy)
  • Anomaly rate over time
  • Helium scarcity level and price
  • Causal/policy edge weight heatmaps
  • Circuit breaker state per subsystem
  • Temporal logic violation counters
  • Precision distribution (FP32/FP16/INT8/INT4/Quantum)
  • Causal RL epsilon and policy-update counters
  • Federated rounds and contributor gauge
  • Multi-agent role distribution
  • Carbon market credits / REC availability
  • HITL review counters and pending queue
  • Distilled models and quality retention
"""

from __future__ import annotations

import http.server
import json
import threading
import time
import sys
import os
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Optional, Dict, Any, List, Tuple, TYPE_CHECKING

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Type checking imports to avoid circular dependencies
if TYPE_CHECKING:
    from core.graph_registry import GraphRegistry, GraphType
    from core.causal_graph import CausalGraph, Edge
    from core.policy_graph import PolicyGraph
    from carbon.helium_monitor import HeliumMonitor

logger = logging.getLogger(__name__)


# =============================================================================
# Module-level resilience: optional helium import
# =============================================================================

try:
    from carbon.helium_monitor import HeliumScarcityLevel  # type: ignore
except Exception:
    class HeliumScarcityLevel(Enum):  # type: ignore
        NORMAL = "normal"
        CAUTION = "caution"
        CRITICAL = "critical"
        SEVERE = "severe"


HELIUM_BASELINE_PRICE_USD = 4.0


# =============================================================================
# Circuit breaker state (mirrors enhanced modules; used for metric encoding)
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

    All fields are duck-typed: the exporter queries them defensively and
    silently omits any metric whose source is missing or malformed.
    """
    # 1. Quantum-Distillation (e.g., QuantumDistillationBridge)
    distiller: Any = None

    # 2. Causal RL (e.g., CausalRLPolicy)
    rl_policy: Any = None

    # 3. Federated (e.g., FederatedAggregator)
    federated: Any = None

    # 4. Multi-Agent (e.g., MultiAgentCoordinator)
    coordinator: Any = None

    # 5. Temporal Logic (e.g., TemporalLogicMonitor)
    temporal_monitor: Any = None

    # 6. XAI (e.g., DataExplainer or stats tracker)
    explainer: Any = None

    # 7. Adaptive Precision (e.g., AdaptivePrecisionController)
    precision_controller: Any = None
    # Optional: {"fp32": N, "fp16": N, "int8": N, "int4": N, "quantum_distilled": N}
    precision_counts: Dict[str, int] = field(default_factory=dict)

    # 8. Carbon Markets (e.g., CarbonMarketClient)
    market: Any = None

    # 9. Resilience (name -> CircuitBreaker)
    circuit_breakers: Dict[str, Any] = field(default_factory=dict)
    chaos: Any = None

    # 10. HITL (e.g., HumanInTheLoopGate)
    hitl: Any = None


# =============================================================================
# Per-metric HELP metadata (extended)
# =============================================================================

_METRIC_METADATA: Dict[str, Tuple[str, Optional[str]]] = {
    # --- Base graph metrics ---
    "green_agent_execution_graphs_active": ("Number of currently active execution graphs", "gauge"),
    "green_agent_graph_nodes": ("Number of nodes in graph", "gauge"),
    "green_agent_graph_edges": ("Number of edges in graph", "gauge"),
    "green_agent_anomalies_active": ("Number of active anomalies", "gauge"),
    "green_agent_ideal_paths_total": ("Total ideal paths registered", "counter"),
    "green_agent_causal_edge_weight": ("Causal edge weight", "gauge"),
    "green_agent_causal_edge_confidence": ("Causal edge confidence", "gauge"),
    "green_agent_policy_edge_weight": ("Policy edge weight", "gauge"),
    "green_agent_edges_truncated": ("Edges omitted due to cardinality cap", "gauge"),

    # --- Helium metrics ---
    "green_agent_helium_scarcity_level": ("Helium scarcity (0=NORMAL, 1=CAUTION, 2=CRITICAL, 3=SEVERE)", "gauge"),
    "green_agent_helium_scarcity_score": ("Helium scarcity score (0-1)", "gauge"),
    "green_agent_helium_spot_price_usd": ("Helium spot price (USD/L)", "gauge"),
    "green_agent_helium_fab_inventory_days": ("Fab inventory (days)", "gauge"),
    "green_agent_helium_vendor_alerts_count": ("Number of vendor alerts", "gauge"),
    "green_agent_helium_price_premium_usd": ("Price premium over $4/L baseline", "gauge"),

    # --- 1. Quantum-Distillation ---
    "green_agent_distilled_models_total": ("Total distilled expert models produced", "counter"),
    "green_agent_distillation_quality_retention": ("Quality retention of last distilled model (0-1)", "gauge"),
    "green_agent_distillation_energy_reduction_percent": ("Energy reduction from distillation (%)", "gauge"),

    # --- 2. Causal RL ---
    "green_agent_rl_epsilon": ("Current epsilon (exploration rate)", "gauge"),
    "green_agent_rl_policy_updates_total": ("Total policy updates", "counter"),
    "green_agent_rl_observations_total": ("Total observations recorded", "counter"),
    "green_agent_rl_reward_mean": ("Mean reward per strategy", "gauge"),
    "green_agent_rl_weight_l1": ("L1 norm of RL weights", "gauge"),

    # --- 3. Federated ---
    "green_agent_federated_rounds_total": ("Federated aggregation rounds completed", "counter"),
    "green_agent_federated_contributors": ("Distinct deployments contributing to federated aggregate", "gauge"),
    "green_agent_federated_updates_total": ("Total federated updates received", "counter"),
    "green_agent_federated_aggregate": ("Federated aggregate value by key", "gauge"),

    # --- 4. Multi-Agent ---
    "green_agent_agents_by_role": ("Number of agents per role", "gauge"),
    "green_agent_agents_total": ("Total registered agents", "gauge"),
    "green_agent_agent_success_rate": ("Average success rate per agent", "gauge"),
    "green_agent_role_reassignments_total": ("Total role reassignments", "counter"),

    # --- 5. Temporal Logic ---
    "green_agent_temporal_violations_total": ("Total temporal logic violations by property", "counter"),
    "green_agent_temporal_last_violation_age_seconds": ("Age of most recent temporal violation (seconds)", "gauge"),

    # --- 6. XAI ---
    "green_agent_explanations_total": ("Total explanations generated", "counter"),
    "green_agent_explanation_confidence": ("Average explanation confidence (0-1)", "gauge"),
    "green_agent_explanation_rationale_length": ("Average rationale length", "gauge"),

    # --- 7. Adaptive Precision ---
    "green_agent_precision_tasks_total": ("Task count by precision level", "counter"),
    "green_agent_precision_share": ("Fraction of tasks at each precision (0-1)", "gauge"),

    # --- 8. Carbon Markets ---
    "green_agent_carbon_credits_purchased_total": ("Total carbon credits purchased", "counter"),
    "green_agent_carbon_credits_sold_total": ("Total carbon savings sold", "counter"),
    "green_agent_market_spend_usd": ("Total market spend (USD)", "gauge"),
    "green_agent_market_trade_volume_usd": ("Total market trade volume (USD)", "gauge"),
    "green_agent_rec_available_mwh": ("REC available (MWh)", "gauge"),
    "green_agent_carbon_price_usd_per_tco2": ("Current carbon price (USD/tCO2)", "gauge"),

    # --- 9. Resilience ---
    "green_agent_circuit_state": ("Circuit breaker state (0=closed, 1=half_open, 2=open)", "gauge"),
    "green_agent_circuit_failures_total": ("Total circuit breaker failures", "counter"),
    "green_agent_chaos_events_total": ("Total chaos injection events", "counter"),

    # --- 10. HITL ---
    "green_agent_hitl_reviews_total": ("Total HITL reviews by decision", "counter"),
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
# GraphMetricsExporter
# =============================================================================

class GraphMetricsExporter:
    """
    Collects metrics from GraphRegistry, optional HeliumMonitor, and optional
    enhancement-layer sources; renders Prometheus text format.

    Thread Safety:
        Assumes GraphRegistry and graph objects are immutable after
        registration. If concurrent modification is possible, wrap collect()
        with a read lock.
    """

    def __init__(
        self,
        registry: 'GraphRegistry',
        job_name: str = "green_agent",
        max_edges_export: int = 100,
        helium_monitor: Optional['HeliumMonitor'] = None,
        enhancement_sources: Optional[EnhancementMetricsSources] = None,
    ):
        """
        Args:
            registry: GraphRegistry instance for accessing graph data.
            job_name: Prometheus job label for this exporter.
            max_edges_export: Max edges to export per graph type.
            helium_monitor: Optional HeliumMonitor instance.
            enhancement_sources: Optional EnhancementMetricsSources with
                references to enhancement-layer objects. Missing sources
                cause the corresponding metrics to be silently omitted.
        """
        self.registry = registry
        self.job_name = job_name
        self.max_edges_export = max_edges_export
        self.helium_monitor = helium_monitor
        self.enhancement_sources = enhancement_sources or EnhancementMetricsSources()

        self._server: Optional[http.server.HTTPServer] = None
        self._server_thread: Optional[threading.Thread] = None

        # Circuit breaker for registry access
        self._registry_failures = 0
        self._registry_last_failure: Optional[float] = None

        logger.info(
            f"GraphMetricsExporter initialized "
            f"(job={job_name}, max_edges={max_edges_export}, "
            f"helium={'yes' if helium_monitor else 'no'}, "
            f"enhancement_sources={'yes' if enhancement_sources else 'no'})"
        )

    # ------------------------------------------------------------------
    # Metric collection
    # ------------------------------------------------------------------

    def collect(self) -> Dict[str, tuple]:
        """
        Gather all graph, helium, and enhancement metrics.

        Returns:
            dict: {metric_name: (value, {label_key: label_value, ...})}
        """
        metrics: Dict[str, tuple] = {}

        # --- Base registry health (with defensive failure handling) ---
        health = self._safe_registry_health()
        metrics["green_agent_execution_graphs_active"] = (
            health.get("execution_count", 0), {}
        )

        for gtype_str, info in health.get("singletons", {}).items():
            labels = {"graph_type": gtype_str}
            if "node_count" in info:
                metrics["green_agent_graph_nodes"] = (info["node_count"], labels)
            if "edge_count" in info:
                metrics["green_agent_graph_edges"] = (info["edge_count"], labels)
            if "anomaly_count" in info:
                metrics["green_agent_anomalies_active"] = (info["anomaly_count"], labels)
            if "ideal_path_count" in info:
                metrics["green_agent_ideal_paths_total"] = (info["ideal_path_count"], {})

        # --- Causal graph edges ---
        causal: Optional['CausalGraph'] = self._safe_registry_get("causal")
        if causal is not None:
            try:
                all_edges = list(getattr(causal, "edges", []) or [])
                sorted_edges = sorted(
                    all_edges, key=lambda e: getattr(e, "weight", 0.0), reverse=True
                )[: self.max_edges_export]
                truncated = max(0, len(all_edges) - len(sorted_edges))
                if truncated:
                    metrics["green_agent_edges_truncated"] = (truncated, {"graph_type": "causal"})
                for edge in sorted_edges:
                    edge_labels = {
                        "source": getattr(edge, "source_id", "?"),
                        "target": getattr(edge, "target_id", "?"),
                        "label": getattr(edge, "label", "default"),
                    }
                    metrics["green_agent_causal_edge_weight"] = (
                        round(getattr(edge, "weight", 0.0), 4), edge_labels
                    )
                    metrics["green_agent_causal_edge_confidence"] = (
                        round(getattr(edge, "confidence", 0.0), 4), edge_labels
                    )
            except Exception as e:
                logger.debug(f"Causal edge export failed: {e}")

        # --- Policy graph edges ---
        policy: Optional['PolicyGraph'] = self._safe_registry_get("policy")
        if policy is not None:
            try:
                all_edges = list(policy.export_weights() or [])
                sorted_edges = sorted(
                    all_edges, key=lambda e: e.get("weight", 0), reverse=True
                )[: self.max_edges_export]
                truncated = max(0, len(all_edges) - len(sorted_edges))
                if truncated:
                    metrics["green_agent_edges_truncated"] = (truncated, {"graph_type": "policy"})
                for edge_dict in sorted_edges:
                    plabels = {
                        "source": edge_dict.get("source", "?"),
                        "target": edge_dict.get("target", "?"),
                        "context_tag": edge_dict.get("context_tag", "default"),
                    }
                    metrics["green_agent_policy_edge_weight"] = (
                        edge_dict.get("weight", 0.0), plabels
                    )
            except Exception as e:
                logger.debug(f"Policy edge export failed: {e}")

        # --- Helium metrics ---
        metrics.update(self._collect_helium_metrics())

        # --- Enhancement metrics ---
        metrics.update(self._collect_enhancement_metrics())

        return metrics

    def _safe_registry_health(self) -> Dict[str, Any]:
        """Call registry.health() with defensive error handling."""
        try:
            return self.registry.health()
        except Exception as e:
            self._registry_failures += 1
            self._registry_last_failure = time.time()
            logger.warning(f"registry.health() failed: {e}")
            return {"execution_count": 0, "singletons": {}}

    def _safe_registry_get(self, kind: str) -> Optional[Any]:
        """Call registry.get(GraphType.X) with defensive fallback."""
        try:
            # Try enum first, then string
            try:
                from core.graph_registry import GraphType  # type: ignore
                key = GraphType.CAUSAL if kind == "causal" else GraphType.POLICY
            except Exception:
                key = kind
            return self.registry.get(key)
        except Exception:
            return None

    # ------------------------------------------------------------------
    # Helium metrics
    # ------------------------------------------------------------------

    def _collect_helium_metrics(self) -> Dict[str, tuple]:
        metrics: Dict[str, tuple] = {}
        if self.helium_monitor is None:
            return metrics
        try:
            signal = self.helium_monitor.get_current_supply()
        except Exception as e:
            logger.debug(f"Helium signal fetch failed: {e}")
            return metrics
        if signal is None:
            return metrics

        scarcity_numeric = {
            HeliumScarcityLevel.NORMAL: 0,
            HeliumScarcityLevel.CAUTION: 1,
            HeliumScarcityLevel.CRITICAL: 2,
            HeliumScarcityLevel.SEVERE: 3,
        }
        source_label = getattr(signal, "source", "unknown")
        level = getattr(signal, "scarcity_level", HeliumScarcityLevel.NORMAL)

        metrics["green_agent_helium_scarcity_level"] = (
            scarcity_numeric.get(level, 0),
            {"source": source_label, "job": self.job_name},
        )
        metrics["green_agent_helium_scarcity_score"] = (
            float(getattr(signal, "scarcity_score", 0.0)),
            {"source": source_label, "job": self.job_name},
        )
        metrics["green_agent_helium_spot_price_usd"] = (
            float(getattr(signal, "spot_price_usd_per_liter", 0.0)),
            {"job": self.job_name},
        )
        metrics["green_agent_helium_fab_inventory_days"] = (
            int(getattr(signal, "fab_inventory_days", 0)),
            {"job": self.job_name},
        )
        metrics["green_agent_helium_vendor_alerts_count"] = (
            len(getattr(signal, "vendor_alerts", []) or []),
            {"job": self.job_name},
        )
        premium = max(
            0.0,
            float(getattr(signal, "spot_price_usd_per_liter", 0.0))
            - HELIUM_BASELINE_PRICE_USD,
        )
        metrics["green_agent_helium_price_premium_usd"] = (
            premium, {"job": self.job_name},
        )
        return metrics

    # ------------------------------------------------------------------
    # Enhancement metrics
    # ------------------------------------------------------------------

    def _collect_enhancement_metrics(self) -> Dict[str, tuple]:
        """Aggregate enhancement-layer metrics from all sources."""
        metrics: Dict[str, tuple] = {}
        for collector in (
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
                metrics.update(collector())
            except Exception as e:
                logger.debug(f"{collector.__name__} failed: {e}")
        return metrics

    # --- 1. Quantum-Distillation ---
    def _collect_quantum_distillation(self) -> Dict[str, tuple]:
        src = self.enhancement_sources.distiller
        if src is None:
            return {}
        m: Dict[str, tuple] = {}
        count = getattr(src, "distilled_count", None) or getattr(src, "count", None)
        if count is not None:
            m["green_agent_distilled_models_total"] = (int(count), {})
        retention = getattr(src, "last_quality_retention", None)
        if retention is not None:
            m["green_agent_distillation_quality_retention"] = (float(retention), {})
        energy = getattr(src, "last_energy_reduction_percent", None)
        if energy is not None:
            m["green_agent_distillation_energy_reduction_percent"] = (float(energy), {})
        # Optional statistics dict
        stats_fn = getattr(src, "get_statistics", None)
        if callable(stats_fn):
            try:
                stats = stats_fn() or {}
                if "distilled_models_total" in stats:
                    m["green_agent_distilled_models_total"] = (
                        int(stats["distilled_models_total"]), {}
                    )
                if "quality_retention" in stats:
                    m["green_agent_distillation_quality_retention"] = (
                        float(stats["quality_retention"]), {}
                    )
            except Exception:
                pass
        return m

    # --- 2. Causal RL ---
    def _collect_causal_rl(self) -> Dict[str, tuple]:
        src = self.enhancement_sources.rl_policy
        if src is None:
            return {}
        m: Dict[str, tuple] = {}
        eps = getattr(src, "epsilon", None)
        if eps is not None:
            m["green_agent_rl_epsilon"] = (float(eps), {})
        obs = getattr(src, "observations", None)
        if obs is not None:
            m["green_agent_rl_observations_total"] = (int(obs), {})
        buf = getattr(src, "buffer", None)
        # Buffer is a deque of tuples; count as proxy for updates
        try:
            if buf is not None:
                m["green_agent_rl_policy_updates_total"] = (len(buf), {})
        except Exception:
            pass
        # Per-strategy reward mean + L1 weight norm
        weights = getattr(src, "weights", None)
        if isinstance(weights, dict):
            for strategy, ws in weights.items():
                try:
                    l1 = sum(abs(float(w)) for w in ws)
                    m["green_agent_rl_weight_l1"] = (
                        round(l1, 4), {"strategy": str(strategy)}
                    )
                except Exception:
                    continue
        return m

    # --- 3. Federated ---
    def _collect_federated(self) -> Dict[str, tuple]:
        src = self.enhancement_sources.federated
        if src is None:
            return {}
        m: Dict[str, tuple] = {}
        updates = getattr(src, "updates", None)
        if updates is not None:
            try:
                m["green_agent_federated_updates_total"] = (len(updates), {})
                # Contributors = distinct deployment_ids
                deploy_ids = {
                    getattr(u, "deployment_id", None) for u in updates
                }
                deploy_ids.discard(None)
                m["green_agent_federated_contributors"] = (len(deploy_ids), {})
            except Exception:
                pass
        # Optional aggregate() call
        agg_fn = getattr(src, "aggregate", None)
        if callable(agg_fn):
            try:
                agg = agg_fn() or {}
                for key, value in agg.items():
                    if isinstance(value, (int, float)):
                        m["green_agent_federated_aggregate"] = (
                            float(value), {"key": str(key)}
                        )
                    elif isinstance(value, dict):
                        for sub, subv in value.items():
                            if isinstance(subv, (int, float)):
                                m["green_agent_federated_aggregate"] = (
                                    float(subv),
                                    {"key": f"{key}.{sub}"},
                                )
            except Exception:
                pass
        return m

    # --- 4. Multi-Agent ---
    def _collect_multi_agent(self) -> Dict[str, tuple]:
        src = self.enhancement_sources.coordinator
        if src is None:
            return {}
        m: Dict[str, tuple] = {}
        agents = getattr(src, "agents", None)
        if isinstance(agents, dict):
            role_counts: Dict[str, int] = {}
            for agent in agents.values():
                role = getattr(agent, "role", None)
                role_str = getattr(role, "value", str(role))
                role_counts[role_str] = role_counts.get(role_str, 0) + 1
                sr = getattr(agent, "success_rate", None)
                if sr is not None:
                    m["green_agent_agent_success_rate"] = (
                        float(sr),
                        {"agent_id": str(getattr(agent, "agent_id", "?"))},
                    )
            for role_str, count in role_counts.items():
                m["green_agent_agents_by_role"] = (count, {"role": role_str})
            m["green_agent_agents_total"] = (len(agents), {})
        return m

    # --- 5. Temporal Logic ---
    def _collect_temporal_logic(self) -> Dict[str, tuple]:
        src = self.enhancement_sources.temporal_monitor
        if src is None:
            return {}
        m: Dict[str, tuple] = {}
        violations = getattr(src, "violations", None)
        if violations is None:
            return m
        try:
            violations_list = list(violations)
        except Exception:
            return m
        # Count by property
        by_property: Dict[str, int] = {}
        latest_ts: Optional[float] = None
        for v in violations_list:
            prop = v.get("property", "unknown") if isinstance(v, dict) else str(v)
            by_property[prop] = by_property.get(prop, 0) + 1
            at = v.get("at") if isinstance(v, dict) else None
            if at:
                try:
                    ts = datetime.fromisoformat(at).timestamp()
                    if latest_ts is None or ts > latest_ts:
                        latest_ts = ts
                except Exception:
                    pass
        for prop, count in by_property.items():
            m["green_agent_temporal_violations_total"] = (count, {"property": prop})
        if latest_ts is not None:
            m["green_agent_temporal_last_violation_age_seconds"] = (
                max(0.0, time.time() - latest_ts), {}
            )
        return m

    # --- 6. XAI ---
    def _collect_xai(self) -> Dict[str, tuple]:
        src = self.enhancement_sources.explainer
        if src is None:
            return {}
        m: Dict[str, tuple] = {}
        # Optional counters
        count = getattr(src, "explanations_count", None) or getattr(src, "count", None)
        if count is not None:
            m["green_agent_explanations_total"] = (int(count), {})
        avg_conf = getattr(src, "average_confidence", None)
        if avg_conf is not None:
            m["green_agent_explanation_confidence"] = (float(avg_conf), {})
        # Optional feedback log (like HITL) — count entries
        flog = getattr(src, "feedback_log", None)
        if flog is not None:
            try:
                m["green_agent_explanations_total"] = (len(flog), {})
            except Exception:
                pass
        return m

    # --- 7. Adaptive Precision ---
    def _collect_adaptive_precision(self) -> Dict[str, tuple]:
        m: Dict[str, tuple] = {}
        counts = self.enhancement_sources.precision_counts or {}
        if counts:
            total = sum(int(v) for v in counts.values()) or 1
            for precision, count in counts.items():
                cnt = int(count)
                m["green_agent_precision_tasks_total"] = (
                    cnt, {"precision": str(precision)}
                )
                m["green_agent_precision_share"] = (
                    round(cnt / total, 4), {"precision": str(precision)}
                )
        src = self.enhancement_sources.precision_controller
        if src is not None:
            stats_fn = getattr(src, "get_statistics", None)
            if callable(stats_fn):
                try:
                    stats = stats_fn() or {}
                    counts = stats.get("precision_counts") or {}
                    if counts:
                        total = sum(int(v) for v in counts.values()) or 1
                        for precision, count in counts.items():
                            cnt = int(count)
                            m["green_agent_precision_tasks_total"] = (
                                cnt, {"precision": str(precision)}
                            )
                            m["green_agent_precision_share"] = (
                                round(cnt / total, 4), {"precision": str(precision)}
                            )
                except Exception:
                    pass
        return m

    # --- 8. Carbon Markets ---
    def _collect_carbon_markets(self) -> Dict[str, tuple]:
        src = self.enhancement_sources.market
        if src is None:
            return {}
        m: Dict[str, tuple] = {}
        trades = getattr(src, "trades", None)
        if trades is not None:
            buys = 0
            sells = 0
            volume = 0.0
            for t in trades:
                if not isinstance(t, dict):
                    continue
                if t.get("type") == "buy":
                    buys += 1
                    volume += float(t.get("cost_usd", 0.0) or 0.0)
                elif t.get("type") == "sell":
                    sells += 1
                    volume += float(t.get("revenue_usd", 0.0) or 0.0)
            m["green_agent_carbon_credits_purchased_total"] = (buys, {})
            m["green_agent_carbon_credits_sold_total"] = (sells, {})
            m["green_agent_market_trade_volume_usd"] = (round(volume, 4), {})
        spent = getattr(src, "spent", None)
        if spent is not None:
            m["green_agent_market_spend_usd"] = (float(spent), {})
        # Optional snapshot
        snap_fn = getattr(src, "get_snapshot", None)
        if callable(snap_fn):
            try:
                snap = snap_fn()
                if snap is not None:
                    cp = getattr(snap, "carbon_price_per_tco2_usd", None)
                    if cp is not None:
                        m["green_agent_carbon_price_usd_per_tco2"] = (float(cp), {})
                    rec = getattr(snap, "rec_available_mwh", None)
                    if rec is not None:
                        m["green_agent_rec_available_mwh"] = (float(rec), {})
            except Exception:
                pass
        return m

    # --- 9. Resilience ---
    def _collect_resilience(self) -> Dict[str, tuple]:
        m: Dict[str, tuple] = {}
        for name, cb in (self.enhancement_sources.circuit_breakers or {}).items():
            state = getattr(cb, "state", None)
            state_str = getattr(state, "value", str(state)) if state else "closed"
            m["green_agent_circuit_state"] = (
                _CIRCUIT_STATE_CODE.get(state_str, 0), {"name": str(name)}
            )
            failures = getattr(cb, "failures", None)
            if failures is not None:
                m["green_agent_circuit_failures_total"] = (
                    int(failures), {"name": str(name)}
                )
        chaos = self.enhancement_sources.chaos
        if chaos is not None:
            events = getattr(chaos, "events", None)
            if events is not None:
                try:
                    m["green_agent_chaos_events_total"] = (len(events), {})
                except Exception:
                    pass
        return m

    # --- 10. HITL ---
    def _collect_hitl(self) -> Dict[str, tuple]:
        src = self.enhancement_sources.hitl
        if src is None:
            return {}
        m: Dict[str, tuple] = {}
        pending = getattr(src, "pending", None)
        if pending is not None:
            try:
                m["green_agent_hitl_pending"] = (len(pending), {})
            except Exception:
                pass
        flog = getattr(src, "feedback_log", None)
        if flog is not None:
            try:
                approved = sum(1 for e in flog if e.get("decision") is True)
                denied = sum(1 for e in flog if e.get("decision") is False)
                m["green_agent_hitl_reviews_total"] = (approved, {"decision": "approved"})
                m["green_agent_hitl_reviews_total"] = (denied, {"decision": "denied"})
            except Exception:
                pass
        # Active learning batch size (if exposed)
        batch_fn = getattr(src, "active_learning_batch", None)
        if callable(batch_fn):
            try:
                batch = batch_fn(16) or []
                m["green_agent_active_learning_batch_size"] = (len(batch), {})
            except Exception:
                pass
        return m

    # ------------------------------------------------------------------
    # Prometheus text format renderer
    # ------------------------------------------------------------------

    def render(self) -> str:
        """
        Render all metrics in Prometheus exposition format.

        Uses per-metric HELP metadata when available; falls back to a
        generic string. Type is inferred from _METRIC_METADATA or the
        `_total` naming convention.
        """
        metrics = self.collect()
        lines: List[str] = []
        seen_names: set[str] = set()

        for name, (value, labels) in metrics.items():
            base = name.split("{")[0]
            if base not in seen_names:
                lines.append(f"# HELP {base} {_metric_help(base)}")
                lines.append(f"# TYPE {base} {_metric_type(base)}")
                seen_names.add(base)

            if labels:
                label_str = ",".join(
                    f'{k}="{v}"' for k, v in sorted(labels.items())
                )
                lines.append(f'{base}{{{label_str}}} {value}')
            else:
                lines.append(f"{base} {value}")

        lines.append("")
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # HTTP server
    # ------------------------------------------------------------------

    def start_http_server(self, port: int = 8000, host: str = "0.0.0.0"):
        """Start a daemon-threaded HTTP server serving /metrics."""
        exporter = self

        class MetricsHandler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                try:
                    if self.path == "/metrics":
                        body = exporter.render().encode()
                        self.send_response(200)
                        self.send_header(
                            "Content-Type",
                            "text/plain; version=0.0.4; charset=utf-8",
                        )
                        self.send_header("Content-Length", str(len(body)))
                        self.end_headers()
                        self.wfile.write(body)
                    elif self.path in ("/health", "/healthz"):
                        self.send_response(200)
                        self.send_header("Content-Type", "text/plain")
                        self.end_headers()
                        self.wfile.write(b"ok\n")
                    else:
                        self.send_response(404)
                        self.end_headers()
                except Exception as e:
                    print(
                        f"[GraphMetricsExporter] Error rendering metrics: {e}",
                        file=sys.stderr,
                    )
                    try:
                        self.send_response(500)
                        self.send_header("Content-Type", "text/plain")
                        self.end_headers()
                        self.wfile.write(b"Internal server error\n")
                    except Exception:
                        pass

            def log_message(self, *args):
                pass

        self._server = http.server.HTTPServer((host, port), MetricsHandler)
        self._server_thread = threading.Thread(
            target=self._server.serve_forever, daemon=True,
        )
        self._server_thread.start()
        print(f"[GraphMetricsExporter] Prometheus endpoint: http://{host}:{port}/metrics")

    def stop_http_server(self):
        if self._server:
            self._server.shutdown()
            if self._server_thread:
                self._server_thread.join(timeout=5)

    # ------------------------------------------------------------------
    # Grafana dashboard
    # ------------------------------------------------------------------

    def grafana_dashboard(self) -> str:
        """
        Returns a Grafana dashboard JSON string with panels for base metrics,
        helium, and all ten enhancement layers.
        """
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
                # --- Row 1: Base graph counts ---
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

                # --- Row 2: Helium ---
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

                # --- Row 3: Causal & policy edges ---
                self._panel_timeseries(
                    9, "Causal edge weights (top edges)",
                    f'topk({self.max_edges_export}, green_agent_causal_edge_weight)',
                    "{{source}} → {{target}}",
                    {"x": 0, "y": 8, "w": 24, "h": 8},
                ),
                self._panel_timeseries(
                    10, "Policy edge weights",
                    "green_agent_policy_edge_weight",
                    "{{source}} → {{target}} ({{context_tag}})",
                    {"x": 0, "y": 16, "w": 24, "h": 8},
                ),

                # --- Row 4: Base execution & helium trend ---
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

                # --- Row 5: Circuit breakers & temporal violations ---
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

                # --- Row 6: Precision distribution ---
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

                # --- Row 7: Federated & multi-agent ---
                self._panel_stat(
                    19, "Federated rounds",
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

                # --- Row 8: Carbon markets & HITL ---
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
            "version": 2,
        }
        return json.dumps(dashboard, indent=2)

    def save_dashboard(self, path: str = "./grafana_dashboard.json"):
        with open(path, "w") as f:
            f.write(self.grafana_dashboard())
        print(f"[GraphMetricsExporter] Grafana dashboard saved → {path}")

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
                    "calcs": ["lastNotNull"], "fields": "", "values": False,
                },
                "textMode": "auto",
            },
            "fieldConfig": {
                "defaults": {
                    "color": {"mode": "fixed", "fixedColor": color},
                    "thresholds": {"mode": "absolute", "steps": thresholds},
                }
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
                "legend": {"displayMode": "list", "placement": "bottom"}
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
                "legend": {"displayMode": "list", "placement": "right"},
                "pieType": "donut",
                "reduceOptions": {
                    "calcs": ["lastNotNull"], "fields": "", "values": False,
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
                "legend": {"displayMode": "list", "placement": "bottom"},
            },
            "targets": [{"expr": expr, "legendFormat": legend}],
            "datasource": {"type": "prometheus"},
        }


# ---------------------------------------------------------------------------
# Standalone demo
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("[GraphMetricsExporter] Starting in demo mode...")

    # --- Mock registry ---
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

    # --- Mock helium monitor ---
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

    # --- Mock enhancement sources ---
    class MockCircuit:
        def __init__(self, state="closed", failures=0):
            self.state = type("E", (), {"value": state})()
            self.failures = failures

    class MockRL:
        epsilon = 0.12
        observations = 42
        buffer = [1, 2, 3]
        weights = {
            "lora": [0.5, 0.2, 0.1, 0.0, 0.0],
            "qlora": [0.3, 0.1, 0.4, 0.1, 0.0],
        }

    class MockCoordinator:
        agents = {
            "a1": type("A", (), {
                "role": type("R", (), {"value": "carbon_heavy"})(),
                "success_rate": 0.9, "agent_id": "a1",
            })(),
            "a2": type("A", (), {
                "role": type("R", (), {"value": "latency_critical"})(),
                "success_rate": 0.85, "agent_id": "a2",
            })(),
        }

    class MockTemporal:
        violations = [
            {"property": "DeadlineRespected", "at": datetime.now().isoformat()},
            {"property": "DeadlineRespected", "at": datetime.now().isoformat()},
            {"property": "CarbonBudgetNotExceeded", "at": datetime.now().isoformat()},
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
            {"decision": True}, {"decision": False}, {"decision": True}
        ]
        def active_learning_batch(self, n=16):
            return self.feedback_log[-n:]

    sources = EnhancementMetricsSources(
        distiller=None,
        rl_policy=MockRL(),
        federated=None,
        coordinator=MockCoordinator(),
        temporal_monitor=MockTemporal(),
        explainer=None,
        precision_counts={"fp32": 10, "fp16": 5, "int8": 30, "int4": 15},
        market=MockMarket(),
        circuit_breakers={
            "forecaster": MockCircuit("closed", 0),
            "scheduler": MockCircuit("open", 3),
            "ledger": MockCircuit("half_open", 1),
        },
        chaos=None,
        hitl=MockHITL(),
    )

    exporter = GraphMetricsExporter(
        MockRegistry(),
        max_edges_export=50,
        helium_monitor=MockHeliumMonitor(),
        enhancement_sources=sources,
    )

    # Single-shot render
    print("\n=== Prometheus text (single-shot) ===")
    print(exporter.render())

    # HTTP server
    exporter.start_http_server(port=8000)
    print("Demo server running at http://localhost:8000/metrics")
    print("Press Ctrl+C to stop")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        exporter.stop_http_server()
        print("\nDemo server stopped")
