"""
test_graph_metrics_exporter.py — Complete Test Suite
====================================================
Unit and integration tests for GraphMetricsExporter.

Run with:
    pytest test_graph_metrics_exporter.py -v
    pytest test_graph_metrics_exporter.py::test_prometheus_parser_compatibility -v
"""

import pytest
import json
import time
import threading
import requests
from unittest.mock import Mock, MagicMock, patch
from prometheus_client.parser import text_string_to_metric_families

# Import the module under test
from src.monitoring.graph_metrics_exporter import GraphMetricsExporter
from core.graph_registry import GraphRegistry, GraphType
from core.causal_graph import CausalGraph, Edge
from core.policy_graph import PolicyGraph


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_registry():
    """Create a mock GraphRegistry for testing."""
    registry = Mock(spec=GraphRegistry)
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
            }
        }
    }
    return registry


@pytest.fixture
def mock_causal_graph():
    """Create a mock CausalGraph with test edges."""
    graph = Mock(spec=CausalGraph)
    graph.edges = [
        Edge("carbon_intensity", "execution_decision", "influences", 
             weight=0.85, confidence=0.95),
        Edge("execution_decision", "energy_consumed", "determines", 
             weight=0.92, confidence=0.98),
        Edge("energy_consumed", "carbon_emitted", "converts", 
             weight=0.40, confidence=0.99),
        # Add more edges to test cardinality limiting
        *[Edge(f"node_{i}", f"node_{i+1}", "test", weight=0.5 + i*0.01, confidence=0.9) 
          for i in range(200)],  # 200 edges to test max_edges_export
    ]
    return graph


@pytest.fixture
def mock_policy_graph():
    """Create a mock PolicyGraph with test edges."""
    graph = Mock(spec=PolicyGraph)
    graph.export_weights.return_value = [
        {"source": "eco_mode", "target": "throttle", "weight": 0.75, "context_tag": "yellow_zone"},
        {"source": "green_zone", "target": "execute_full", "weight": 1.0, "context_tag": "green_zone"},
        {"source": "red_zone", "target": "defer", "weight": 0.3, "context_tag": "red_zone"},
        # Add more edges to test cardinality limiting
        *[{"source": f"p{i}", "target": f"p{i+1}", "weight": 0.5 + i*0.01, "context_tag": "test"} 
          for i in range(150)],  # 150 edges to test max_edges_export
    ]
    return graph


@pytest.fixture
def exporter(mock_registry, mock_causal_graph, mock_policy_graph):
    """Create a GraphMetricsExporter with mocked dependencies."""
    # Configure registry.get() to return mock graphs
    def get_side_effect(graph_type):
        if graph_type == GraphType.CAUSAL:
            return mock_causal_graph
        elif graph_type == GraphType.POLICY:
            return mock_policy_graph
        return None
    
    mock_registry.get.side_effect = get_side_effect
    
    return GraphMetricsExporter(mock_registry, max_edges_export=50)


# ---------------------------------------------------------------------------
# Unit Tests: collect()
# ---------------------------------------------------------------------------

def test_collect_empty_registry():
    """Test collection with empty registry returns expected base metrics."""
    registry = Mock(spec=GraphRegistry)
    registry.health.return_value = {"execution_count": 0, "singletons": {}}
    
    exporter = GraphMetricsExporter(registry)
    metrics = exporter.collect()
    
    # Should have at least the execution metric
    assert "green_agent_execution_graphs_active" in metrics
    assert metrics["green_agent_execution_graphs_active"][0] == 0


def test_collect_with_graph_data(exporter, mock_registry):
    """Test collection includes graph-specific metrics."""
    metrics = exporter.collect()
    
    # Execution metric (fixed: _active not _total)
    assert "green_agent_execution_graphs_active" in metrics
    assert metrics["green_agent_execution_graphs_active"][0] == 10
    
    # Graph node/edge counts with labels
    assert "green_agent_graph_nodes" in metrics
    assert metrics["green_agent_graph_nodes"][1] == {"graph_type": "causal"}
    
    # Anomaly count
    assert "green_agent_anomalies_active" in metrics
    assert metrics["green_agent_anomalies_active"][0] == 3
    
    # Ideal paths (cumulative → _total suffix)
    assert "green_agent_ideal_paths_total" in metrics


def test_collect_cardinality_limiting(exporter, mock_causal_graph):
    """Test that edge exports are limited by max_edges_export."""
    # Registry returns causal graph with 200+ edges
    mock_registry = Mock(spec=GraphRegistry)
    mock_registry.health.return_value = {"execution_count": 0, "singletons": {}}
    mock_registry.get.return_value = mock_causal_graph
    
    # Test with max_edges_export=10
    exporter_limited = GraphMetricsExporter(mock_registry, max_edges_export=10)
    metrics = exporter_limited.collect()
    
    # Count causal edge weight metrics
    edge_metrics = [k for k in metrics.keys() if "causal_edge_weight" in k]
    
    # Should be limited to max_edges_export (10)
    assert len(edge_metrics) <= 10
    
    # Test with higher limit
    exporter_unlimited = GraphMetricsExporter(mock_registry, max_edges_export=200)
    metrics_unlimited = exporter_unlimited.collect()
    edge_metrics_unlimited = [k for k in metrics_unlimited.keys() if "causal_edge_weight" in k]
    
    # Should include more edges (up to 200)
    assert len(edge_metrics_unlimited) > len(edge_metrics)


def test_collect_metric_types(exporter):
    """Test that metric names match their intended types."""
    metrics = exporter.collect()
    
    # Gauges (can go up/down)
    gauge_metrics = [
        "green_agent_execution_graphs_active",
        "green_agent_graph_nodes",
        "green_agent_graph_edges",
        "green_agent_anomalies_active",
        "green_agent_causal_edge_weight",
        "green_agent_policy_edge_weight",
    ]
    for metric in gauge_metrics:
        if metric in metrics:
            # Should not end with _total (reserved for counters)
            assert not metric.endswith("_total"), f"{metric} should be gauge, not counter"
    
    # Counters (monotonically increasing)
    counter_metrics = [
        "green_agent_ideal_paths_total",
    ]
    for metric in counter_metrics:
        if metric in metrics:
            # Should end with _total
            assert metric.endswith("_total"), f"{metric} should be counter"


# ---------------------------------------------------------------------------
# Unit Tests: render()
# ---------------------------------------------------------------------------

def test_render_prometheus_format(exporter):
    """Test that render() outputs valid Prometheus exposition format."""
    text = exporter.render()
    
    # Check required Prometheus format elements
    assert "# HELP" in text
    assert "# TYPE" in text
    assert text.endswith("\n")  # Trailing newline required
    
    # Check metric type declarations match names
    lines = text.strip().split("\n")
    for i, line in enumerate(lines):
        if line.startswith("# TYPE"):
            parts = line.split()
            if len(parts) >= 3:
                metric_name = parts[1]
                metric_type = parts[2]
                
                # Verify type matches naming convention
                if metric_name.endswith("_total"):
                    assert metric_type == "counter", f"{metric_name} should be counter"
                else:
                    assert metric_type == "gauge", f"{metric_name} should be gauge"


def test_render_deduplication(exporter):
    """Test that HELP/TYPE headers are not duplicated for same metric name."""
    text = exporter.render()
    lines = text.strip().split("\n")
    
    # Count HELP/TYPE occurrences per metric base name
    help_counts = {}
    type_counts = {}
    
    for line in lines:
        if line.startswith("# HELP"):
            metric_name = line.split()[1]
            help_counts[metric_name] = help_counts.get(metric_name, 0) + 1
        elif line.startswith("# TYPE"):
            metric_name = line.split()[1]
            type_counts[metric_name] = type_counts.get(metric_name, 0) + 1
    
    # Each metric should have exactly one HELP and one TYPE
    for metric in help_counts:
        assert help_counts[metric] == 1, f"Duplicate HELP for {metric}"
    for metric in type_counts:
        assert type_counts[metric] == 1, f"Duplicate TYPE for {metric}"


def test_render_label_format(exporter):
    """Test that labels are formatted correctly in Prometheus style."""
    text = exporter.render()
    
    # Check for properly formatted labels: {key="value", ...}
    import re
    label_pattern = r'\{[^}]+\}'
    matches = re.findall(label_pattern, text)
    
    for match in matches:
        # Labels should be key="value" pairs, comma-separated
        assert '=' in match, f"Invalid label format: {match}"
        assert '"' in match, f"Label values should be quoted: {match}"


# ---------------------------------------------------------------------------
# Integration Tests: Prometheus Parser Compatibility
# ---------------------------------------------------------------------------

def test_prometheus_parser_compatibility(exporter):
    """Verify that output can be parsed by prometheus_client parser."""
    text = exporter.render()
    
    # This will raise if format is invalid
    try:
        families = list(text_string_to_metric_families(text))
        assert len(families) > 0, "No metrics parsed"
    except Exception as e:
        pytest.fail(f"Prometheus parser failed: {e}\nOutput:\n{text}")


def test_prometheus_metric_families(exporter):
    """Test that parsed metrics have expected structure."""
    text = exporter.render()
    families = list(text_string_to_metric_families(text))
    
    # Should have at least execution metric
    family_names = [f.name for f in families]
    assert "green_agent_execution_graphs_active" in family_names
    
    # Check that gauge metrics have correct type
    for family in families:
        if family.name == "green_agent_execution_graphs_active":
            assert family.type == "gauge"
        elif family.name == "green_agent_ideal_paths_total":
            assert family.type == "counter"


# ---------------------------------------------------------------------------
# Integration Tests: HTTP Server
# ---------------------------------------------------------------------------

def test_http_server_metrics_endpoint(exporter):
    """Test that HTTP server serves /metrics correctly."""
    # Start server on random port to avoid conflicts
    import socket
    with socket.socket() as s:
        s.bind(('', 0))
        s.listen(1)
        port = s.getsockname()[1]
    
    exporter.start_http_server(port=port, host="127.0.0.1")
    time.sleep(0.5)  # Let server start
    
    try:
        # Scrape metrics
        response = requests.get(f"http://127.0.0.1:{port}/metrics", timeout=5)
        
        assert response.status_code == 200
        assert "text/plain" in response.headers.get("Content-Type", "")
        assert "# HELP" in response.text
        assert "# TYPE" in response.text
        
        # Verify Prometheus parser accepts it
        list(text_string_to_metric_families(response.text))
        
    finally:
        exporter.stop_http_server()


def test_http_server_404_for_unknown_path(exporter):
    """Test that HTTP server returns 404 for non-/metrics paths."""
    import socket
    with socket.socket() as s:
        s.bind(('', 0))
        s.listen(1)
        port = s.getsockname()[1]
    
    exporter.start_http_server(port=port, host="127.0.0.1")
    time.sleep(0.5)
    
    try:
        response = requests.get(f"http://127.0.0.1:{port}/unknown", timeout=5)
        assert response.status_code == 404
    finally:
        exporter.stop_http_server()


def test_http_server_error_handling(exporter):
    """Test that HTTP server handles render() exceptions gracefully."""
    # Mock render() to raise an exception
    original_render = exporter.render
    exporter.render = Mock(side_effect=Exception("Test error"))
    
    import socket
    with socket.socket() as s:
        s.bind(('', 0))
        s.listen(1)
        port = s.getsockname()[1]
    
    exporter.start_http_server(port=port, host="127.0.0.1")
    time.sleep(0.5)
    
    try:
        # Should return 500, not crash
        response = requests.get(f"http://127.0.0.1:{port}/metrics", timeout=5)
        assert response.status_code == 500
    finally:
        exporter.render = original_render
        exporter.stop_http_server()


# ---------------------------------------------------------------------------
# Grafana Dashboard Tests
# ---------------------------------------------------------------------------

def test_grafana_dashboard_structure(exporter):
    """Test that dashboard JSON is valid and has expected panels."""
    dashboard_json = exporter.grafana_dashboard()
    
    # Parse JSON
    dashboard = json.loads(dashboard_json)
    
    # Check required fields
    assert dashboard["title"] == "Green Agent — Graph Health"
    assert "panels" in dashboard
    assert len(dashboard["panels"]) >= 7  # Expected minimum panels
    
    # Check panel structure
    for panel in dashboard["panels"]:
        assert "id" in panel
        assert "title" in panel
        assert "targets" in panel
        assert "datasource" in panel
        assert panel["datasource"]["type"] == "prometheus"


def test_grafana_dashboard_expressions(exporter):
    """Test that dashboard uses correct metric expressions."""
    dashboard = json.loads(exporter.grafana_dashboard())
    
    # Collect all expressions from panels
    expressions = []
    for panel in dashboard["panels"]:
        for target in panel.get("targets", []):
            if "expr" in target:
                expressions.append(target["expr"])
    
    # Should reference fixed metric names
    assert any("green_agent_execution_graphs_active" in expr for expr in expressions)
    assert any("green_agent_anomalies_active" in expr for expr in expressions)
    
    # Should use topk() for edge metrics to limit cardinality in dashboard
    edge_exprs = [e for e in expressions if "edge_weight" in e]
    assert any("topk(" in expr for expr in edge_exprs), "Edge metrics should use topk()"


def test_save_dashboard_file(exporter, tmp_path):
    """Test that save_dashboard() writes valid JSON file."""
    output_path = tmp_path / "dashboard.json"
    
    exporter.save_dashboard(str(output_path))
    
    # Verify file exists and contains valid JSON
    assert output_path.exists()
    
    with open(output_path) as f:
        dashboard = json.load(f)
    
    assert dashboard["title"] == "Green Agent — Graph Health"


# ---------------------------------------------------------------------------
# Thread Safety Tests (Basic)
# ---------------------------------------------------------------------------

def test_collect_thread_safety_basic(exporter):
    """Basic test that collect() can be called from multiple threads."""
    results = []
    errors = []
    
    def collect_from_thread():
        try:
            result = exporter.collect()
            results.append(result)
        except Exception as e:
            errors.append(e)
    
    # Start multiple threads calling collect()
    threads = [threading.Thread(target=collect_from_thread) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    
    # Should have no errors and all results should be dicts
    assert len(errors) == 0, f"Errors during concurrent collect: {errors}"
    assert len(results) == 10
    assert all(isinstance(r, dict) for r in results)


# ---------------------------------------------------------------------------
# Configuration Tests
# ---------------------------------------------------------------------------

def test_max_edges_export_default():
    """Test that max_edges_export has sensible default."""
    registry = Mock(spec=GraphRegistry)
    exporter = GraphMetricsExporter(registry)
    
    # Default should be 100 (reasonable for most use cases)
    assert exporter.max_edges_export == 100


def test_max_edges_export_custom():
    """Test that max_edges_export can be customized."""
    registry = Mock(spec=GraphRegistry)
    exporter = GraphMetricsExporter(registry, max_edges_export=25)
    
    assert exporter.max_edges_export == 25


# ---------------------------------------------------------------------------
# Main entry point for running tests directly
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # Run with pytest if available, otherwise basic smoke test
    try:
        import pytest
        pytest.main([__file__, "-v"])
    except ImportError:
        print("pytest not available, running basic smoke test...")
        
        # Basic smoke test
        registry = Mock(spec=GraphRegistry)
        registry.health.return_value = {"execution_count": 1, "singletons": {}}
        
        exporter = GraphMetricsExporter(registry)
        text = exporter.render()
        
        assert "# HELP" in text
        assert "# TYPE" in text
        print("✅ Basic smoke test passed")
