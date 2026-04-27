"""
graph_metrics_exporter.py — Production Metrics Exporter for Green Agent
========================================================================

Exposes Green Agent graph health and helium supply metrics in Prometheus text format.
Works with any Prometheus scrape configuration; no database dependency.
Includes a static Grafana dashboard JSON template (Layer 11).

File Location: src/monitoring/graph_metrics_exporter.py

Two modes:
1. HTTP server mode (production):
       exporter = GraphMetricsExporter(registry)
       exporter.start_http_server(port=8000)
   Prometheus scrapes http://localhost:8000/metrics on its interval.

2. Single-shot mode (CI / testing):
       text = exporter.render()      # returns Prometheus text format
       dashboard = exporter.grafana_dashboard()  # returns JSON string

Grafana dashboard:
    exporter.save_dashboard("./grafana_dashboard.json")
Import via Grafana UI: Dashboards → Import → Upload JSON file.

Pre-configured panels:
  • Graph node/edge counts (causal, policy)
  • Anomaly rate over time
  • Helium scarcity level and price
  • Causal/policy edge weight heatmaps
"""

import http.server
import json
import threading
import time
import sys
import os
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List, TYPE_CHECKING

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Type checking imports to avoid circular dependencies
if TYPE_CHECKING:
    from core.graph_registry import GraphRegistry, GraphType
    from core.causal_graph import CausalGraph, Edge
    from core.policy_graph import PolicyGraph
    from carbon.helium_monitor import HeliumMonitor

import logging
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Metrics collector
# ---------------------------------------------------------------------------

class GraphMetricsExporter:
    """
    Collects metrics from GraphRegistry and renders Prometheus text format.
    
    Supports:
    - Graph metrics (nodes, edges, anomalies)
    - Causal graph edge weights and confidence
    - Policy graph edge weights
    - Helium supply chain metrics (if HeliumMonitor configured)
    
    Thread Safety:
    - This class assumes GraphRegistry and graph objects are immutable after
      registration, or that read operations are thread-safe.
    - If concurrent modification is possible, wrap collect() with a read lock.
    """

    def __init__(
        self, 
        registry: 'GraphRegistry', 
        job_name: str = "green_agent",
        max_edges_export: int = 100,
        helium_monitor: Optional['HeliumMonitor'] = None
    ):
        """
        Initialize metrics exporter
        
        Args:
            registry: GraphRegistry instance for accessing graph data
            job_name: Prometheus job label for this exporter
            max_edges_export: Max edges to export per graph type (prevent cardinality explosion)
            helium_monitor: Optional HeliumMonitor instance for helium metrics
        """
        self.registry = registry
        self.job_name = job_name
        self.max_edges_export = max_edges_export
        self.helium_monitor = helium_monitor  # Optional helium metrics source
        
        self._server: Optional[http.server.HTTPServer] = None
        self._server_thread: Optional[threading.Thread] = None
        
        logger.info(f"GraphMetricsExporter initialized (job={job_name}, max_edges={max_edges_export})")

    # ------------------------------------------------------------------
    # Metric collection
    # ------------------------------------------------------------------

    def collect(self) -> Dict[str, tuple]:
        """
        Gather all graph and helium metrics into a flat dict.
        
        Keys are Prometheus metric names; values are (value, labels) tuples.
        
        Returns:
            dict: {metric_name: (value, {label_key: label_value, ...})}
        """
        health = self.registry.health()
        metrics: Dict[str, tuple] = {}

        # Execution metric: active execution graphs (gauge, not counter)
        metrics["green_agent_execution_graphs_active"] = (
            health.get("execution_count", 0), {}
        )

        # Graph-level metrics (nodes, edges, anomalies) by graph type
        for gtype_str, info in health.get("singletons", {}).items():
            labels = {"graph_type": gtype_str}

            if "node_count" in info:
                metrics["green_agent_graph_nodes"] = (
                    info["node_count"], labels
                )
            if "edge_count" in info:
                metrics["green_agent_graph_edges"] = (
                    info["edge_count"], labels
                )
            if "anomaly_count" in info:
                metrics["green_agent_anomalies_active"] = (
                    info["anomaly_count"], labels
                )
            if "ideal_path_count" in info:
                # Cumulative count → use _total suffix with counter type
                metrics["green_agent_ideal_paths_total"] = (
                    info["ideal_path_count"], {}
                )

        # Causal graph edge weights (limited by max_edges_export to control cardinality)
        causal: Optional['CausalGraph'] = self.registry.get(GraphType.CAUSAL)
        if causal:
            # Sort by weight descending, take top-K to limit cardinality
            sorted_edges = sorted(
                causal.edges, 
                key=lambda e: e.weight, 
                reverse=True
            )[:self.max_edges_export]
            
            for edge in sorted_edges:
                edge_labels = {
                    "source": edge.source_id,
                    "target": edge.target_id,
                    "label": edge.label,
                }
                metrics["green_agent_causal_edge_weight"] = (
                    round(edge.weight, 4), edge_labels
                )
                metrics["green_agent_causal_edge_confidence"] = (
                    round(edge.confidence, 4), edge_labels
                )

        # Policy graph edge weights (limited by max_edges_export)
        policy: Optional['PolicyGraph'] = self.registry.get(GraphType.POLICY)
        if policy:
            all_edges = policy.export_weights()
            sorted_edges = sorted(
                all_edges, 
                key=lambda e: e.get("weight", 0), 
                reverse=True
            )[:self.max_edges_export]
            
            for edge_dict in sorted_edges:
                plabels = {
                    "source": edge_dict["source"],
                    "target": edge_dict["target"],
                    "context_tag": edge_dict.get("context_tag", "default"),
                }
                metrics["green_agent_policy_edge_weight"] = (
                    edge_dict["weight"], plabels
                )

        # ✅ NEW: Helium supply chain metrics (if monitor configured)
        helium_metrics = self._collect_helium_metrics()
        metrics.update(helium_metrics)

        return metrics

    def _collect_helium_metrics(self) -> Dict[str, tuple]:
        """
        Collect helium-specific metrics for Prometheus exposition
        
        Returns:
            Dict mapping metric names to (value, labels) tuples
        """
        metrics = {}
        
        if self.helium_monitor is None:
            return metrics  # No helium monitor configured
        
        # Get current helium signal
        signal = self.helium_monitor.get_current_supply()
        if signal is None:
            return metrics  # No signal available yet
        
        # Map scarcity level enum to numeric gauge (0=NORMAL, 1=CAUTION, 2=CRITICAL, 3=SEVERE)
        from carbon.helium_monitor import HeliumScarcityLevel
        scarcity_numeric = {
            HeliumScarcityLevel.NORMAL: 0,
            HeliumScarcityLevel.CAUTION: 1,
            HeliumScarcityLevel.CRITICAL: 2,
            HeliumScarcityLevel.SEVERE: 3,
        }
        
        # Scarcity level gauge
        metrics["green_agent_helium_scarcity_level"] = (
            scarcity_numeric[signal.scarcity_level],
            {"source": signal.source, "job": self.job_name}
        )
        
        # Scarcity score (0.0 to 1.0)
        metrics["green_agent_helium_scarcity_score"] = (
            signal.scarcity_score,
            {"source": signal.source, "job": self.job_name}
        )
        
        # Spot price in USD per liter
        metrics["green_agent_helium_spot_price_usd"] = (
            signal.spot_price_usd_per_liter,
            {"job": self.job_name}
        )
        
        # Fabrication facility inventory days
        metrics["green_agent_helium_fab_inventory_days"] = (
            signal.fab_inventory_days,
            {"job": self.job_name}
        )
        
        # Count of active vendor alerts
        metrics["green_agent_helium_vendor_alerts_count"] = (
            len(signal.vendor_alerts),
            {"job": self.job_name}
        )
        
        # Price premium over baseline ($4.0/L)
        baseline_price = 4.0
        premium = max(0.0, signal.spot_price_usd_per_liter - baseline_price)
        metrics["green_agent_helium_price_premium_usd"] = (
            premium,
            {"job": self.job_name}
        )
        
        return metrics

    # ------------------------------------------------------------------
    # Prometheus text format renderer
    # ------------------------------------------------------------------

    def render(self) -> str:
        """
        Render all metrics in Prometheus exposition format.
        
        Each distinct metric name gets one # HELP and one # TYPE line.
        Metric type is inferred from name convention:
        - *_total → counter
        - others → gauge
        
        Returns:
            str: Prometheus text format metrics string
        """
        metrics = self.collect()
        lines: List[str] = []
        seen_names: set[str] = set()

        for name, (value, labels) in metrics.items():
            # Deduplicate HELP/TYPE headers per unique metric base name
            base = name.split("{")[0]
            if base not in seen_names:
                lines.append(f"# HELP {base} Green Agent graph metric")
                # Infer metric type from naming convention
                if name.endswith("_total"):
                    lines.append(f"# TYPE {base} counter")
                else:
                    lines.append(f"# TYPE {base} gauge")
                seen_names.add(base)

            # Format labels as {key="value", ...}
            if labels:
                label_str = ",".join(
                    f'{k}="{v}"' for k, v in sorted(labels.items())
                )
                lines.append(f'{base}{{{label_str}}} {value}')
            else:
                lines.append(f"{base} {value}")

        lines.append("")   # Prometheus requires trailing newline
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # HTTP server (production mode)
    # ------------------------------------------------------------------

    def start_http_server(self, port: int = 8000, host: str = "0.0.0.0"):
        """
        Start a lightweight HTTP server that serves /metrics on demand.
        
        Runs in a daemon thread — stops automatically when the process exits.
        
        Args:
            port: Port to listen on (default: 8000)
            host: Host to bind to (default: 0.0.0.0)
        """
        exporter = self

        class MetricsHandler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                try:  # ✅ Error handling to prevent server crash
                    if self.path == "/metrics":
                        body = exporter.render().encode()
                        self.send_response(200)
                        self.send_header(
                            "Content-Type",
                            "text/plain; version=0.0.4; charset=utf-8"
                        )
                        self.send_header("Content-Length", str(len(body)))
                        self.end_headers()
                        self.wfile.write(body)
                    else:
                        self.send_response(404)
                        self.end_headers()
                except Exception as e:
                    # Log error but don't crash server
                    print(
                        f"[GraphMetricsExporter] Error rendering metrics: {e}",
                        file=sys.stderr
                    )
                    self.send_response(500)
                    self.send_header("Content-Type", "text/plain")
                    self.end_headers()
                    self.wfile.write(b"Internal server error\n")

            def log_message(self, *args):
                pass  # suppress access log noise

        self._server = http.server.HTTPServer((host, port), MetricsHandler)
        self._server_thread = threading.Thread(
            target=self._server.serve_forever, daemon=True
        )
        self._server_thread.start()
        print(f"[GraphMetricsExporter] Prometheus endpoint: http://{host}:{port}/metrics")

    def stop_http_server(self):
        """Stop the HTTP server gracefully."""
        if self._server:
            self._server.shutdown()
            if self._server_thread:
                self._server_thread.join(timeout=5)

    # ------------------------------------------------------------------
    # Grafana dashboard template
    # ------------------------------------------------------------------

    def grafana_dashboard(self) -> str:
        """
        Returns a Grafana dashboard JSON string.
        
        Import via Grafana UI: Dashboards → Import → Upload JSON file.
        
        Pre-configured panels:
          Row 1: Graph node counts (causal, policy)
          Row 2: Active anomaly count | Ideal paths registered
          Row 3: Helium scarcity level | Spot price
          Row 4: Causal edge weight heatmap
          Row 5: Policy edge weight time series
          Row 6: Execution graphs active
        """
        dashboard = {
            "title": "Green Agent — Graph & Helium Health",
            "uid": "green_agent_graphs_helium",
            "tags": ["green-agent", "graphs", "sustainability", "helium"],
            "refresh": "10s",
            "time": {"from": "now-1h", "to": "now"},
            "templating": {"list": []},
            "panels": [
                # Row 1: Graph node counts
                self._panel_stat(
                    id=1, title="Causal graph nodes",
                    expr='green_agent_graph_nodes{graph_type="causal"}',
                    gridPos={"x": 0, "y": 0, "w": 6, "h": 4},
                ),
                self._panel_stat(
                    id=2, title="Policy graph nodes",
                    expr='green_agent_graph_nodes{graph_type="policy"}',
                    gridPos={"x": 6, "y": 0, "w": 6, "h": 4},
                ),
                # Row 2: Anomalies and ideal paths
                self._panel_stat(
                    id=3, title="Active anomalies",
                    expr='green_agent_anomalies_active{graph_type="causal"}',
                    color="red",
                    gridPos={"x": 12, "y": 0, "w": 6, "h": 4},
                ),
                self._panel_stat(
                    id=4, title="Ideal paths (total)",
                    expr="green_agent_ideal_paths_total",
                    gridPos={"x": 18, "y": 0, "w": 6, "h": 4},
                ),
                # Row 3: Helium metrics
                self._panel_stat(
                    id=5, title="Helium scarcity level",
                    expr='green_agent_helium_scarcity_level',
                    color="orange",
                    thresholds=[
                        {"color": "green", "value": None},
                        {"color": "yellow", "value": 1},
                        {"color": "red", "value": 2},
                        {"color": "dark-red", "value": 3},
                    ],
                    gridPos={"x": 0, "y": 4, "w": 6, "h": 4},
                ),
                self._panel_stat(
                    id=6, title="Helium spot price ($/L)",
                    expr="green_agent_helium_spot_price_usd",
                    color="blue",
                    gridPos={"x": 6, "y": 4, "w": 6, "h": 4},
                ),
                self._panel_stat(
                    id=7, title="Fab inventory (days)",
                    expr="green_agent_helium_fab_inventory_days",
                    color="green",
                    thresholds=[
                        {"color": "red", "value": None},
                        {"color": "yellow", "value": 10},
                        {"color": "green", "value": 20},
                    ],
                    gridPos={"x": 12, "y": 4, "w": 6, "h": 4},
                ),
                self._panel_stat(
                    id=8, title="Price premium ($/L)",
                    expr="green_agent_helium_price_premium_usd",
                    color="purple",
                    gridPos={"x": 18, "y": 4, "w": 6, "h": 4},
                ),
                # Row 4: Causal edge weights
                self._panel_timeseries(
                    id=9,
                    title="Causal edge weights (top edges)",
                    expr=f'topk({self.max_edges_export}, green_agent_causal_edge_weight)',
                    legend="{{source}} → {{target}}",
                    gridPos={"x": 0, "y": 8, "w": 24, "h": 8},
                ),
                # Row 5: Policy edge weights
                self._panel_timeseries(
                    id=10,
                    title="Policy edge weights",
                    expr="green_agent_policy_edge_weight",
                    legend="{{source}} → {{target}} ({{context_tag}})",
                    gridPos={"x": 0, "y": 16, "w": 24, "h": 8},
                ),
                # Row 6: Execution graphs
                self._panel_timeseries(
                    id=11,
                    title="Active execution graphs",
                    expr="green_agent_execution_graphs_active",
                    legend="active_executions",
                    gridPos={"x": 0, "y": 24, "w": 12, "h": 6},
                ),
                # Row 6: Helium scarcity trend
                self._panel_timeseries(
                    id=12,
                    title="Helium scarcity score trend",
                    expr="green_agent_helium_scarcity_score",
                    legend="scarcity_score",
                    gridPos={"x": 12, "y": 24, "w": 12, "h": 6},
                ),
            ],
            "schemaVersion": 36,
            "version": 1,
        }
        return json.dumps(dashboard, indent=2)

    def save_dashboard(self, path: str = "./grafana_dashboard.json"):
        """Save Grafana dashboard JSON to file."""
        with open(path, "w") as f:
            f.write(self.grafana_dashboard())
        print(f"[GraphMetricsExporter] Grafana dashboard saved → {path}")

    # ------------------------------------------------------------------
    # Panel builder helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _panel_stat(
        id: int, 
        title: str, 
        expr: str,
        gridPos: dict, 
        color: str = "green",
        thresholds: Optional[List[Dict]] = None
    ) -> dict:
        """Create a Grafana stat panel."""
        if thresholds is None:
            thresholds = [
                {"color": color, "value": None},
            ]
        
        return {
            "id": id, 
            "type": "stat", 
            "title": title,
            "gridPos": gridPos,
            "options": {
                "colorMode": "value",
                "graphMode": "area",
                "justifyMode": "auto",
                "orientation": "auto",
                "reduceOptions": {
                    "calcs": ["lastNotNull"],
                    "fields": "",
                    "values": False
                },
                "textMode": "auto"
            },
            "fieldConfig": {
                "defaults": {
                    "color": {"mode": "fixed", "fixedColor": color},
                    "thresholds": {
                        "mode": "absolute",
                        "steps": thresholds,
                    },
                }
            },
            "targets": [{"expr": expr, "legendFormat": ""}],
            "datasource": {"type": "prometheus"},
        }

    @staticmethod
    def _panel_timeseries(
        id: int, 
        title: str, 
        expr: str,
        legend: str, 
        gridPos: dict
    ) -> dict:
        """Create a Grafana timeseries panel."""
        return {
            "id": id, 
            "type": "timeseries", 
            "title": title,
            "gridPos": gridPos,
            "options": {
                "legend": {"displayMode": "list", "placement": "bottom"}
            },
            "fieldConfig": {
                "defaults": {"custom": {"lineWidth": 1}}
            },
            "targets": [{"expr": expr, "legendFormat": legend}],
            "datasource": {"type": "prometheus"},
        }


# ---------------------------------------------------------------------------
# Standalone execution for testing
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # Demo mode: create mock registry and start server
    print("[GraphMetricsExporter] Starting in demo mode...")
    
    # Create a minimal mock registry for demo
    class MockRegistry:
        def health(self):
            return {
                "execution_count": 5,
                "singletons": {
                    "causal": {
                        "node_count": 42,
                        "edge_count": 89,
                        "anomaly_count": 2,
                        "ideal_path_count": 12,
                    },
                    "policy": {
                        "node_count": 28,
                        "edge_count": 56,
                    }
                }
            }
        
        def get(self, graph_type):
            return None  # Return None for demo
    
    # Create mock helium monitor for demo
    class MockHeliumMonitor:
        def get_current_supply(self):
            from carbon.helium_monitor import HeliumScarcityLevel, HeliumSupplySignal
            return HeliumSupplySignal(
                timestamp=datetime.now(),
                scarcity_level=HeliumScarcityLevel.CAUTION,
                scarcity_score=0.4,
                spot_price_usd_per_liter=5.5,
                fab_inventory_days=20,
                vendor_alerts=['Test alert'],
                source='demo'
            )
    
    registry = MockRegistry()
    helium_monitor = MockHeliumMonitor()
    exporter = GraphMetricsExporter(
        registry, 
        max_edges_export=50,
        helium_monitor=helium_monitor
    )
    
    # Start HTTP server
    exporter.start_http_server(port=8000)
    
    print("Demo server running at http://localhost:8000/metrics")
    print("Press Ctrl+C to stop")
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        exporter.stop_http_server()
        print("\nDemo server stopped")
