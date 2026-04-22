"""
graph_metrics_exporter.py — Fixed Version (Metric Type + Cardinality)
=====================================================================
Exposes Green Agent graph health metrics in Prometheus text format.
Works with any Prometheus scrape configuration; no database dependency.
Includes a static Grafana dashboard JSON template (Layer 11).

Changes in this version:
- ✅ Fixed metric type: green_agent_execution_graphs_active (gauge) instead of _total
- ✅ Added max_edges_export config to prevent cardinality explosion
- ✅ Added error handling in HTTP handler
- ✅ Removed unused ts variable
- ✅ Added thread-safety documentation

Two modes
----------
1. HTTP server mode (production):
       exporter = GraphMetricsExporter(registry)
       exporter.start_http_server(port=8000)
   Prometheus scrapes http://localhost:8000/metrics on its interval.

2. Single-shot mode (CI / testing):
       text = exporter.render()      # returns Prometheus text format
       dashboard = exporter.grafana_dashboard()  # returns JSON string

Grafana dashboard
-----------------
    exporter.save_dashboard("./grafana_dashboard.json")
Import that file into Grafana via Dashboards → Import → Upload JSON file.
Pre-configured panels:
  • Graph node/edge counts (causal, policy)
  • Anomaly rate over time
  • Execution count
  • Learned edge weight heatmap data
"""

import http.server
import json
import threading
import time
import sys
import os
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.graph_registry import GraphRegistry, GraphType
from core.causal_graph import CausalGraph, Edge
from core.policy_graph import PolicyGraph


# ---------------------------------------------------------------------------
# Metrics collector
# ---------------------------------------------------------------------------

class GraphMetricsExporter:
    """
    Collects metrics from GraphRegistry and renders Prometheus text format.
    
    Thread Safety:
    - This class assumes GraphRegistry and graph objects are immutable after
      registration, or that read operations are thread-safe.
    - If concurrent modification is possible, wrap collect() with a read lock.
    """

    def __init__(
        self, 
        registry: GraphRegistry, 
        job_name: str = "green_agent",
        max_edges_export: int = 100  # ✅ NEW: Prevent cardinality explosion
    ):
        self.registry = registry
        self.job_name = job_name
        self.max_edges_export = max_edges_export  # ✅ NEW: Configurable limit
        self._server: Optional[http.server.HTTPServer] = None
        self._server_thread: Optional[threading.Thread] = None

    # ------------------------------------------------------------------
    # Metric collection
    # ------------------------------------------------------------------

    def collect(self) -> Dict[str, tuple]:
        """
        Gather all graph metrics into a flat dict.
        Keys are Prometheus metric names; values are (value, labels) tuples.
        
        Returns:
            dict: {metric_name: (value, {label_key: label_value, ...})}
        """
        health = self.registry.health()
        metrics: Dict[str, tuple] = {}

        # ✅ FIX 1: Use _active suffix for gauge (not _total which implies counter)
        # Execution count: number of graphs currently being executed (can go up/down)
        metrics["green_agent_execution_graphs_active"] = (
            health.get("execution_count", 0), {}
        )

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
                # This is a cumulative count → use _total suffix with counter type
                metrics["green_agent_ideal_paths_total"] = (
                    info["ideal_path_count"], {}
                )

        # ✅ FIX 2: Limit edge exports to prevent cardinality explosion
        # Causal graph edge weights
        causal: Optional[CausalGraph] = self.registry.get(GraphType.CAUSAL)
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

        # Policy graph edge weights
        policy: Optional[PolicyGraph] = self.registry.get(GraphType.POLICY)
        if policy:
            # Use export_weights() abstraction from policy graph
            all_edges = policy.export_weights()
            # Limit to top-K by weight to control cardinality
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

        return metrics

    # ------------------------------------------------------------------
    # Prometheus text format renderer
    # ------------------------------------------------------------------

    def render(self) -> str:
        """
        Render all metrics in Prometheus exposition format.
        Each distinct metric name gets one # HELP and one # TYPE line.
        
        Returns:
            str: Prometheus text format metrics
        """
        metrics = self.collect()
        lines: List[str] = []
        seen_names: set[str] = set()

        for name, (value, labels) in metrics.items():
            # Deduplicate HELP/TYPE headers
            base = name.split("{")[0]
            if base not in seen_names:
                lines.append(f"# HELP {base} Green Agent graph metric")
                # ✅ FIX 1: Use correct metric type based on name convention
                if name.endswith("_total"):
                    lines.append(f"# TYPE {base} counter")
                else:
                    lines.append(f"# TYPE {base} gauge")
                seen_names.add(base)

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
        """
        exporter = self

        class MetricsHandler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                try:  # ✅ FIX 3: Error handling to prevent server crash
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
        Import via Dashboards → Import → Upload JSON file.

        Pre-configured panels:
          Row 1: Graph node counts (causal, policy)
          Row 2: Active anomaly count  |  Ideal paths registered
          Row 3: Causal edge weight heatmap
          Row 4: Policy edge weight time series
        """
        dashboard = {
            "title": "Green Agent — Graph Health",
            "uid": "green_agent_graphs",
            "tags": ["green-agent", "graphs", "sustainability"],
            "refresh": "10s",
            "time": {"from": "now-1h", "to": "now"},
            "templating": {"list": []},
            "panels": [
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
                self._panel_timeseries(
                    id=5,
                    title="Causal edge weights (top edges)",
                    expr=f'topk({self.max_edges_export}, green_agent_causal_edge_weight)',
                    legend="{{source}} → {{target}}",
                    gridPos={"x": 0, "y": 4, "w": 24, "h": 8},
                ),
                self._panel_timeseries(
                    id=6,
                    title="Policy edge weights",
                    expr="green_agent_policy_edge_weight",
                    legend="{{source}} → {{target}}",
                    gridPos={"x": 0, "y": 12, "w": 24, "h": 8},
                ),
                self._panel_timeseries(
                    id=7,
                    title="Active execution graphs",
                    expr="green_agent_execution_graphs_active",
                    legend="active_executions",
                    gridPos={"x": 0, "y": 20, "w": 12, "h": 6},
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
        color: str = "green"
    ) -> dict:
        """Create a Grafana stat panel."""
        return {
            "id": id, 
            "type": "stat", 
            "title": title,
            "gridPos": gridPos,
            "options": {"reduceOptions": {"calcs": ["lastNotNull"]}},
            "fieldConfig": {
                "defaults": {
                    "color": {"mode": "fixed", "fixedColor": color},
                    "thresholds": {
                        "mode": "absolute",
                        "steps": [{"color": color, "value": None}],
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
    
    registry = MockRegistry()
    exporter = GraphMetricsExporter(registry, max_edges_export=50)
    
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
